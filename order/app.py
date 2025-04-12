import json
import logging
import os
import atexit
import random
import uuid
import asyncio
import time

import aio_pika

from aiohttp import web
from aio_pika import connect_robust, Message, DeliveryMode, RobustConnection, RobustChannel, RobustExchange
from msgspec import msgpack, Struct, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError
from typing import Optional, Tuple, Dict, Any, Union, List

import global_idempotency
from common.rpc_client import RpcClient
from common.amqp_worker import AMQPWorker
import global_idempotency.helper
# from redis import Sentinel

# import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort
import warnings

import redis.asyncio as redis
from redis.asyncio.sentinel import Sentinel 
# from redis import exceptions as redisexceptions
import redis
from global_idempotency.helper import check_idempotency_key, IdempotencyStoreConnectionError

warnings.filterwarnings("ignore", message=".*Python 2.7.*")

SERVICE_NAME = "order"

# TODO create config file for this
DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
SAGA_STATE_TTL = 864000 * 3
LOCK_TIMEOUT = 30000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask("order-service")


sentinel_async = Sentinel([
    (os.environ['REDIS_SENTINEL_1'], 26379),
    (os.environ['REDIS_SENTINEL_2'], 26379),
    (os.environ['REDIS_SENTINEL_3'], 26379)],
    socket_timeout=10, #TODO check if it can be lowered
    socket_connect_timeout=5,
    socket_keepalive=True,
    password=os.environ['REDIS_PASSWORD']
)


db_master = sentinel_async.master_for('saga-master', decode_responses=False)


db_slave = sentinel_async.slave_for('order-master', socket_timeout=1, decode_responses=False)
db_master_saga = sentinel_async.master_for('saga-master', socket_timeout=1, decode_responses=False)
db_slave_saga = sentinel_async.slave_for('saga-master', socket_timeout=1, decode_responses=False)
SERVICE_ID = str(uuid.uuid4())
ORCHESTRATOR_ID = str(uuid.uuid4())
AMQP_URL = os.environ.get('AMQP_URL', 'amqp://user:password@rabbitmq:5672/')
worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="order_queue",
)
OUTBOX_KEY="outbox:order"

async def enqueue_outbox_message(routing_key: str, payload:dict):
    message={
        "routing_key":routing_key,
        "payload":payload,
        "timestamp":time.time()
    }
    try:
        await db_master_saga.rpush(OUTBOX_KEY,json.dumps(message))
        logger.info(f"[Order Orchestrator] Enqueued message to outbox: {message}")
    except Exception as e:
        logger.error(f"Error while enqueueing message to outbox: {str(e)}")

async def update_saga_and_enqueue(saga_id, status,  routing_key: str|None, payload:dict|None, details=None,):
#retry maybe todo too with max attempts?

    try:
        saga_key = f"saga:{saga_id}"
        saga_json = await db_slave_saga.get(saga_key)
        if saga_json:
            saga_data = json.loads(saga_json)
        else:
            # Create a new saga record if not found
            saga_data = {
                "saga_id": saga_id,
                "status": status,
                "steps": [],
                "created_at": time.time()
            }

        saga_data["status"] = status
        saga_data["last_updated"] = time.time()

        if details:
            saga_data["details"] = {**saga_data.get("details", {}), **details}

        # Append a step entry
        saga_data["steps"].append({
            "status": status,
            "timestamp": time.time(),
            "details": details
        })

        outbox_msg_json = None
        if payload and routing_key:
            message={
                "routing_key":routing_key,
                "payload":payload,
                "timestamp":time.time()
            }
            outbox_msg_json = json.dumps(message)

        async with db_master.pipeline(transaction=True) as pipe:
            await pipe.watch(saga_key)

            await pipe.set(saga_key, json.dumps(saga_data))
            if outbox_msg_json:
                await pipe.rpush(OUTBOX_KEY,outbox_msg_json)
            
        result =  await pipe.execute()
        
        logger.info(f"[Order Orchestrator Atomic] Saga {saga_id} updated -> {status}. Enqueued message to outbox")
        return True
        
    except redis.RedisError as e:
        logger.error (f"Error redis update saga state {saga_id} to {status}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error updating saga state {saga_id} to {status}: {str(e)}")

def close_db_connection():
    logger.info("Closing DB connections")
    db_master.close()
    db_slave.close()
    db_master_saga.close()
    db_slave_saga.close()

atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


async def get_order_from_db(order_id: str) -> Tuple[Union[OrderValue, Dict], int]: 
    try:
        # get serialized data
        # entry = await db_slave.get(order_id)
        entry: bytes | None = await db_master.get(order_id) # temporary for testing TODO for change
        if not entry:
            logger.warning(f"Order: {order_id} not found in the database")
            return {"error": f"Order: {order_id} not found!"}, 404
        try:
            user_entry = msgpack.decode(entry, type=OrderValue)
            print(f"--- ORDER: get_order_from_db - Deserialized Order Type={type(user_entry)}, Value={user_entry} ---")
            return user_entry, 200
        except MsgspecDecodeError as e:
            logger.error(f"Failed to decode msgpack {order_id} from the database:{str(e)}")
            return {"error": f"Failed to decode msgpack {order_id} from the database:{str(e)}"}, 500
        
    except redis.RedisError as e:
        logger.error(f"Database Error while getting order: {order_id} from the database:{str(e)}")
        return {"error": DB_ERROR_STR}, 500
        
    # deserialize data if it exists else return null
    except Exception as e:
        logger.error(f"Error while getting order: {order_id} from the database:{str(e)}")
        return {"error": DB_ERROR_STR}, 400

@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2 * item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        await db_master.mset(kv_pairs)
    except redis.RedisError:
        return {"error": DB_ERROR_STR}, 400
    return {"msg": "Batch init for orders successful"}, 200





async def get_saga_state(saga_id: str) -> str:
    try:
        saga_key = f"saga:{saga_id}"
        saga_json = await db_slave_saga.get(saga_key)
        if saga_json:
            return json.loads(saga_json)
        logger.warning(f"[Order Orchestrator] Saga {saga_id} not found.")
        return None
    except Exception as e:
        logger.error(f"Error getting saga state: {str(e)}")
        return None




# Initialize RpcClient for the order service
rpc_client = RpcClient()


async def connect_rpc_client():
    # Connect the RpcClient to the AMQP server
    await rpc_client.connect(os.environ["AMQP_URL"])


@worker.register
async def create_order(data, message):
   
    # user_id = data.get("user_id")
    # in some cases data was the entire message
    user_id = data.get("user_id")
    if not user_id or not isinstance(user_id, str):
        logger.error(f"create_order called without a valid string user_id. Data received: {data}")
        return {"error": "Missing or invalid user_id provided"}, 400
    key = str(uuid.uuid4())
    
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        print(f"--- ORDER SVC: create_order - Attempting to set order {key} for user {user_id}", flush=True)
        await db_master.set(key, value)

        await update_saga_and_enqueue(saga_id=key, status="INITIATED", routing_key=None, payload=None)
        return {"order_id": key}, 200
    
    except redis.RedisError:
        logger.error(f"Redis error creating order {key} for user {user_id}: {e}")
        return {"error": "Redis error"}, 500
    except Exception as e:
        logger.error(f"Unexpected error creating order {key} for user {user_id}: {e}")
        return {"error": "internal error"}, 500
    



@worker.register
async def find_order(data, message):
    order_id = data.get("order_id")
    print(f"--- ORDER SVC: find_order - Looking for order_id: {order_id}", flush=True)
    order_entry, status_code = await get_order_from_db(order_id)
    if status_code != 200:
        print(f"--- ORDER SVC: find_order - Failed to find order {order_id}", flush=True) 
        return order_entry, status_code # Return the error dict and status
    return { # Return the success dict and status
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost
    }, 200 # Return 200 for success

@worker.register
async def process_checkout_request(data, message):
    """
        Receives 'order.checkout' events to begin the saga.
        Tries to reserve stock first, then waits for success/failure events.
        """
    try:
        order_id = data.get('order_id')
        saga_id = str(uuid.uuid4())  
        attempt_id = str(message.delivery_tag) if message else str(uuid.uuid4())
        logger.info(f"[Order Orchestrator] Received checkout request for order {order_id}, . Saga ID: {saga_id}")

        idempotency_key = global_idempotency.helper.generate_idempotency_key(SERVICE_NAME, "checkout", data.get("user_id"), order_id, attempt_id) 
        # idempotency_key = global_idempotency.helper.generate_idempotency_key(SERVICE_NAME,data.get("user_id"),order_id,saga_id)
        try:
            if check_idempotency_key(idempotency_key): #TODO should be async
            # if await check_idempotency_key(idempotency_key): #TODO should be async
               app.logger.info(f"[IDEMPOTENCY] Duplicate add stock for {idempotency_key}")
               return
        except IdempotencyStoreConnectionError as e:
            app.logger.error(str(e))
            return {"msg": "Redis error during idempotency check"}, 500
        response_data = {"status": "success", "step": "checkout"}
        global_idempotency.helper.store_idempotent_result(idempotency_key,response_data)

        await update_saga_and_enqueue(
            saga_id,
            status="SAGA_STARTED",
            routing_key=None,
            payload=None,
        )
        

        print(f"--- ORDER SVC: process_checkout_request - Attempting to find order {order_id}", flush=True) 
        order_data = await rpc_client.call(queue="order_queue",
                                           action="find_order",
                                           payload={"order_id": order_id})
        print(f"--- ORDER SVC: process_checkout_request - Successfully found order {order_id}", flush=True)

        #-- deadlock check
        # order_data = None
        # if isinstance(order_data, tuple) and len(order_data) == 2:
        #     if order_data[1] == 200:
        #         order_data = order_data[0]
        #     else:
        #         logger.error(f"Internal find_order returned error status {order_data[1]}: {order_data[0]}")
        #         raise Exception(f"Internal find_order failed with status {order_data[1]}")
        # elif isinstance(order_data, dict) and "error" in order_data:
        #     logger.error(f"Internal find_order returned error dict: {order_data}")
        #     raise Exception(f"Internal find_order failed: {order_data.get('error')}")
        # elif isinstance(order_data, dict): 
        #     order_data = order_data
        # else:
        #     logger.error(f"Unexpected response type from internal find_order: {type(order_data)} - {order_data}")
        #     raise Exception(f"Unexpected response from internal find_order")

        # if not order_data or 'items' not in order_data or 'user_id' not in order_data:
        #     logger.error(f"Internal find_order did not return valid order data: {order_data}")
        #     raise Exception("Failed to retrieve valid order details internally")

        # logger.info(f"--- ORDER SVC: process_checkout_request - Proceeding after internal find_order. Data: {order_data}") 

        #-- deadlock check DEADLOCK

        # Lock order to prevent concurrent processing
        # order_lock_value = acquire_write_lock(order_id)
        #potential deadlock if the same service instance is called ://
        # logs show wacquire_write_lock:
        #    INFO:common.rpc_client:--- RPC CLIENT CALL: Message published for CorrID b4a0c813-2701-4341-b211-ea0a81ab247e. Waiting for future...
        # but process_checkout_request instance stops and never logs receiving lock result

        order_lock_value = await rpc_client.call(queue="order_queue",
                                                 action="acquire_write_lock",
                                                 payload={"order_id": order_id})
        

        # update_saga_state(saga_id, "ORDER_LOCK_REQUESTED", order_data)

        await update_saga_and_enqueue(
            saga_id=saga_id,
            status="ORDER_LOCK_REQUESTED",
            details=order_data,
            routing_key=None,
            payload=None,
        )

        
        if order_lock_value is None:
            raise Exception("Order is already being processed")
        logger.info(f"[Order Orchestrator] Lock acquired {order_lock_value} for order {order_id}")

        await update_saga_and_enqueue(saga_id=saga_id,
                                status="ORDER_LOCKED",
                                details={"order_id": order_id, "lock_value": order_lock_value},
                                routing_key=None,
                                payload=None)

        logger.info(f"[Order Orchestrator] Sending reserve stock message for saga={saga_id}, order={order_id}")
        for item_id, quantity in order_data["items"]:
            payload = {
                "item_id": item_id,
                "quantity": quantity
            }
            worker.send_message(payload=payload, queue="stock_queue", correlation_id=saga_id,
                                action="reserve_stock", reply_to="orchestrator_queue",
                                callback_action="process_stock_completed")

        return

    except Exception as e:
        logger.error(f"Error in process_checkout_request: {str(e)}")
        if 'saga_id' in locals():
            await update_saga_and_enqueue(
                saga_id=saga_id,
                status="SAGA_FAILED_INITIALIZATION",
                details={"error": str(e)},
                routing_key=None,
                payload=None,
            )

            #outbox instead of enqueue
            await enqueue_outbox_message("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })

@worker.register
async def process_stock_completed(data, message):
    """
          Receives 'stock.reservation_completed' event after Stock Service reserves items.
          Then we proceed to request payment.
          """
    try:
        saga_id = data.get('saga_id')
        order_id = data.get('order_id')
        
        idempotency_key = global_idempotency.helper.generate_idempotency_key(SERVICE_NAME, 
                                                                             "stock", order_id, saga_id,
                                                                             "process_stock_completed")

        # if await check_idempotency_key(idempotency_key):
        if check_idempotency_key(idempotency_key): #TODO make async
            logger.info(f"[IDEMPOTENCY] Skipping duplicate stock step for {idempotency_key}")
            return

        logger.info(f"[Order Orchestrator] Stock reservation completed for saga={saga_id}, order={order_id}")
        # update_saga_state(saga_id, "STOCK_RESERVATION_COMPLETED")

        await update_saga_and_enqueue(
            saga_id=saga_id,
            status="STOCK_RESERVATION_COMPLETED",
            details=None,
            routing_key=None,
        )

        # Retrieve saga state to get user_id, total_cost, etc.
        saga_data = await get_saga_state(saga_id)
        if not saga_data:
            raise Exception(f"Saga {saga_id} not found")

        order_data = saga_data.get("details", {})
        user_id = order_data.get("user_id")
        total_cost = order_data.get("total_cost")

        if not user_id or not total_cost:
            raise Exception("Missing user_id or total_cost in saga details")

        logger.info(f"[Order Orchestrator] Sending remove_credit request for saga={saga_id}, order={order_id}")
        payload = {
            "user_id": user_id,
            "amount": total_cost
        }

        await update_saga_and_enqueue(
            saga_id=saga_id,
            status="PAYMENT_INITIATED",
            details=None,
            routing_key="payment_queue",
            payload=payload,
        )
        # TODO: Probably only the last message should have a callback_action???
        worker.send_message(payload=payload, queue="payment_queue", correlation_id=saga_id,
                            action="remove_credit", reply_to="orchestrator_queue",
                            callback_action="process_payment_completed")
        response_data = {"status": "success", "step": "stock reservation completed"}
        global_idempotency.helper.store_idempotent_result(idempotency_key,response_data)
        return

    except Exception as e:
        logger.error(f"Error in process_stock_completed: {str(e)}")
        if 'saga_id' in locals():

            #TODO: check updated
            #combines the operations
            await update_saga_and_enqueue(
                saga_id = saga_id,
                status = "PAYMENT_INITIATION_FAILED",
                details={"error": str(e)},
                routing_key = "stock.compensate",
                payload= {"saga_id": saga_id, "order_id": order_id}
            )

            await enqueue_outbox_message("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })



@worker.register
async def process_payment_completed(data, message):
    """
      Receives 'payment.completed' after the Payment Service finishes charging.
      Then the order can be finalized.
      """
    try:
        saga_id = data.get("saga_id")
        order_id = data.get("order_id")
        idempotency_key = global_idempotency.helper.generate_idempotency_key(SERVICE_NAME, "payment", order_id, saga_id,
                                                                             "process_payment_completed")

        # if await check_idempotency_key(idempotency_key):
        if check_idempotency_key(idempotency_key): #TODO make async
            logger.info(f"[IDEMPOTENCY] Skipping duplicate payment step for {idempotency_key}")
            return {"msg": "Skipping duplicate payment step "}, 200
        

        logger.info(f"[Order Orchestrator] Payment completed for saga={saga_id}, order={order_id}")
        await update_saga_and_enqueue(
            saga_id=saga_id,
            status="PAYMENT_COMPLETED",
            details=None,
            routing_key=None,
            payload=None,
        )

        order_data = await rpc_client.call(queue="order_queue",action="find_order",payload={"order_id": order_id})

        for item_id, quantity in order_data["items"]:
            payload = {
                "item_id": item_id,
                "quantity": quantity
            }
            worker.send_message(payload=payload, queue="stock_queue", correlation_id=saga_id, action="remove_stock")
        logger.info(f"[Order Orchestrator] Stock removed successfully for saga={saga_id}, order={order_id}")
        await update_saga_and_enqueue(
            saga_id,
            "STOCK_REMOVED",
            routing_key=None,
            payload=None,
        )

        saga_data = await get_saga_state(saga_id)
        if not saga_data:
            raise Exception(f"Saga {saga_id} not found")

        # Get the lock_value from the saga state
        lock_value = saga_data["details"].get("lock_value")
        if not lock_value:
            raise Exception(f"Lock value not found for saga {saga_id}")

        # Release the order lock
        # if release_write_lock(order_id, lock_value):
        

        order_released = await rpc_client.call(
            queue="order_queue",
            action="release_write_lock",
            payload={"order_id": order_id, "lock_value": lock_value}
        )
        if order_released:
            await update_saga_and_enqueue(
                saga_id,
                "ORDER_LOCK_RELEASED",
                routing_key=None,
                payload=None,
            )
        else:
            raise Exception(f"Failed to release lock for order {order_id}")

        await update_saga_and_enqueue(
            saga_id,
            "ORDER_FINALIZED",
            routing_key=None,
            payload=None,
        )
        await update_saga_and_enqueue(
            saga_id,
            "SAGA_COMPLETED",
            details={"completed_at": time.time()},
            routing_key=None,
            payload=None,
        )
        payload = {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "success"
        }
        worker.send_message(payload=payload, queue="order_queue", correlation_id=saga_id,
                            action="checkout_completed", reply_to="gateway_queue")
        response_data = {"status": "success", "step": "payment completed"}
        response_data=global_idempotency.helper.store_idempotent_result(idempotency_key, response_data)
        return

    except Exception as e:
        logger.error(f"Error in process_payment_completed: {str(e)}")
        if 'saga_id' in locals():
            
            #TODO: check updated
            #combines the operations
            success = await update_saga_and_enqueue(
                saga_id = saga_id,
                status = "ORDER_FINALIZATION_FAILED",
                details={"error": str(e)},
                routing_key = "payment.reverse",
                payload= {"saga_id": saga_id, "order_id": order_id}
            )

            if success:
                await enqueue_outbox_message("stock.compensate", {
                    "saga_id": saga_id,
                    "order_id": order_id
                })
                await enqueue_outbox_message("order.checkout_failed", {
                    "saga_id": saga_id,
                    "order_id": order_id,
                    "status": "failed",
                    "error": str(e)
                })
        
            else:
                logger.critical(f"Failed to enqueue outbox messages for saga {saga_id}")
                



async def process_failure_events(data, message):
    """
    Any service can publish a failure event: e.g. 'stock.reservation_failed' or 'payment.failed'.
    The orchestrator listens, updates the saga state, and triggers compensation if needed.
    """

    #TODO refactor for saga update and enqueue
    try:
        saga_id = data.get("saga_id")
        order_id = data.get("order_id")
        # TODO: Extract error message from the event
        error = data.get('error', 'Unknown error')
        # routing_key = data.routing_key

        #using gettatr to default to UNKOWN
        routing_key = getattr(message, 'routing_key', "UNKNOWN")

        logger.info(f"[Order Orchestrator] Failure event {routing_key} for saga={saga_id}: {error}")
        
        # update_saga_state(saga_id, f"FAILURE_EVENT_{routing_key.upper()}", {"error": error})

        saga_data = await get_saga_state(saga_id)
        if not saga_data:
            logger.error(f"[Order Orchestrator] Saga {saga_id} not found.")
            return {"msg": "Saga not found"}, 404
        
        #check whether saga already failed or completed
        current_status = saga_data.get("status")
        if current_status in ["SAGA_COMPLETED", "SAGA_FAILED", "SAGA_FAILED_RECOVERY"]:
            logger.warning(f"[Order Orchestrator] Saga {saga_id} already terminated ({current_status}). Ignoring failure event {routing_key}.")
            return {"msg": "Saga already terminated"}, 200

        # Check which steps were completed
        steps_completed = [step["status"] for step in saga_data.get("steps", [])]
        compensations = []

        # If payment was completed but we got a failure from somewhere else, reverse it
        if "PAYMENT_COMPLETED" in steps_completed and routing_key:
            compensations.append(("payment.reverse", {"saga_id": saga_id, "order_id": order_id}))

        # If stock was reserved but we have a new failure (not from stock reservation itself), roll it back
        if "STOCK_RESERVATION_COMPLETED" in steps_completed and routing_key:
            compensations.append(("stock.compensate", {"saga_id": saga_id, "order_id": order_id}))


        #final failure
        compensations.append(("order.checkout_failed", {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "failed",
            "error": error
        }))

        status = "SAGA_FAILED"
        details = {
            "error": error,
            "compensations_initiated": [c[0] for c in compensations if c[0] != "order.checkout_failed"],
            "failed_at": time.time()
        }
        routing_key = None
        payload = None
        remaining_msgs = []

        if compensations:
            # Gather routing keys and payload for first message
            routing_key = compensations[0][0]
            payload = compensations[0][1]
            remaining_msgs = compensations[1:]
        
        success = await update_saga_and_enqueue(
            saga_id=saga_id,
            status=status,
            details=details,
            routing_key=routing_key,
            payload=payload,
        )
        if success:
            logger.info(f"[Order Orchestrator] Successfully updated saga {saga_id} to {status}")
            for comp_key, payload in compensations:
                await enqueue_outbox_message(comp_key, payload)
            
        else:
            logger.critical(f"[Order Orchestrator] Failed to update saga {saga_id} to {status}")
            await message.nack(requeue=True)

    except Exception as e:
        logger.error(f"Error in process_failure_events: {str(e)}")
        try:
            await message.nack(requeue=True)
        except Exception as e:
            logger.error(f"Error nacking message: {str(e)}")


# ----------------------------------------------------------------------------
# Idempotency Helper
# ----------------------------------------------------------------------------

async def execute_idempotent_operation(key: str, message, handler_function, *args, **kwargs):
    try:
        stored_result = check_idempotency_key(key) #TODO make async
        if stored_result:
            logger.info(f"Idempotency hit for key {key}. Acking.")
            return {"msg": "Idempotency hit"}, 200
    except Exception as e:
        logger.error(f"Error checking idempotency key: {str(e)}")
        await message.nack(requeue=True)
        return {"msg": "Error checking idempotency key"}, 500

    # if idompentcy check is successful, call the handler function
    try:
        await handler_function(message.correlation_id, message.type, message.body, *args, **kwargs)
    except Exception:
        logger.exception(f"Handler {handler_function.__name__} failed for key {key}")


# ----------------------------------------------------------------------------
# Saga State Helpers
# ----------------------------------------------------------------------------

#TODO can be made async aiohttp??
async def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        return {"error": "Request error"}, 400
    else:
        return response


#TODO should be made atomic, easily done through @idempotent when we merge with stock/payment idempotent branch

@worker.register
async def add_item(data, message):
    order_id = data.get("order_id")
    item_id = data.get("item_id")
    quantity = data.get("quantity")
    stock_response = None

    idempotency_key = f"idempotent:{SERVICE_NAME}:add_item:{order_id}:{item_id}:{quantity}" 

    logger.info(f"--- ORDER SVC: add_item - Idempotency key: {idempotency_key}")
    
    if order_id is None or item_id is None or quantity is None:
        logger.error(f"Missing required fields (order_id, item_id, quantity) in add_item request.")
        return {"error": "Missing required fields"}, 400


    try:
        key_exists = global_idempotency.helper.check_idempotency_key(idempotency_key)
        if key_exists:
            logger.info(f"[IDEMPOTENCY] Duplicate add_item request for key {idempotency_key}. Skipping.")
            return {"msg": f"Item: {item_id} potentially already added to order (idempotency) {order_id}"}, 200
    except redis.RedisError as e:
        logger.error(f"Idempotency check failed for add_item {idempotency_key}: {e}")
        return {"error": "Idempotency check failed"}, 503
    
    except Exception as e:
        logger.exception(f"Unexpected error checking idempotency key {idempotency_key}: {e}")
        return {"error": "Unexpected error checking idempotency key"}, 500

    # load the order

    try:
        order_entry, status_code = await get_order_from_db(order_id)
        if status_code != 200:
            logger.error(f"--- ORDER SVC: add_item - Order entry: {order_entry}")
            return {"error": f"Order: {order_id} not found!"}, 404
        if not isinstance(order_entry, OrderValue):
            logger.error(f"--- ORDER SVC: add_item - Order entry: {order_entry}")
            return {"error": f"Order: {order_id}, invalid format!"}, 500
        
    except Exception as e:
        logger.error(f"Unexecpted rror getting order entry for order {order_id}: {e}")
        await message.nack(requeue=False)
        return {"error": "Unexpected error getting order entry"}, 500
    
    #call stock service
    try:
        payload = {
            "item_id": item_id #should be consistent with stock
        }

        logger.info(f"--- ORDER SVC: add_item - Stock service payload: {payload}")

        stock_response = await rpc_client.call(queue="stock_queue", action="find_item", payload=payload)
        logger.info(f"--- ORDER SVC: add_item - Stock service response: {stock_response}")

        stock_data = stock_response.get('data')
        if not isinstance(stock_data, dict):
            logger.error(f"--- ORDER SVC: add_item - Stock service returned invalid data type: {type(stock_data)}")
            return {"error": f"Item data not found, item: {item_id}"}, 404
        
        item_price = stock_data.get("price")
        if item_price is None:
            logger.error(f"--- Invalid or missing price for item {item_id}")
            return {"error": f"Item: {item_id} price not found!"}, 404
    
    except Exception as e:
        logger.error(f"Unexpected error getting stock data for item {item_id}: {e}")
        return {"error": "Unexpected error getting stock data"}, 503
    
    #update and save order

    try:
        if not isinstance(order_entry, OrderValue):
            logger.error(f"--- ORDER SVC: add_item - Order entry: {order_entry}")
            return {"error": f"Order: {order_id}, invalid format!"}, 500
        
        order_entry.items.append((item_id, int(quantity)))
        order_entry.total_cost += int(quantity) * item_price

        await db_master.set(order_id, msgpack.encode(order_entry))
        await update_saga_and_enqueue(
            saga_id=order_id,
            status="ITEM_ADDED",
            routing_key=None,
            payload=None,
        )
        #enqueue outbox??
        try:
            global_idempotency.helper.store_idempotent_result(idempotency_key, {"status": "item_added"})
            logger.info(f"[IDEMPOTENCY] Stored result for add_item key {idempotency_key}")
        except Exception as ie:
            logger.error(f"Failed to store idempotency key {idempotency_key} after add_item: {ie}")
            # operation succeeded-> however idempotency might fail on retry if key wasn't stored.
        
        return ({"msg": f"Item: {item_id} added to order: {order_id}, total cost updated to: {order_entry.total_cost}"}, 200)
    except redis.RedisError:
        await message.nack(requeue=False)
        return {"error": DB_ERROR_STR}, 400
    except Exception as e:
        await message.nack(requeue=False)
        return {"error": f"Unexpected error adding item to order: {e}"}, 500


# Helper methods locks
async def acquire_write_lock(order_id: str, lock_timeout: int = 10000) -> str:
    """
    Acquire a write lock on an order to prevent multiple writes.
    :param order_id: The ID of the order to lock.
    :param lock_timeout: The lock timeout in milliseconds (default: 10 seconds).
    :return: True if the lock was acquired, False otherwise.
    """
    lock_key = f"write_lock:order:{order_id}"
    lock_value = str(uuid.uuid4())  # Unique value to identify the lock owner
    try:
        # Try to acquire the lock using Redis SET with NX and PX options
        is_locked = await db_master.set(lock_key, lock_value, nx=True, px=lock_timeout)
        if is_locked:
            logger.info(f"Lock acquired for order: {order_id}")
            await db_master.hmset(
                f"lock_meta:{lock_key}",
                {
                    "service_id": SERVICE_ID,
                    "ttl": SAGA_STATE_TTL,
                    "lock_timeout": lock_timeout,
                    "lock_value": lock_value
                }
            )
        
            #TODO, check if correct? lock timeout wasnt impelemented??
            await db_master.expire(f"lock_meta:{lock_key}", int(lock_timeout / 1000) + 60)
            return lock_value
    
        else:
            logger.info(f"Lock already acquired for order: {order_id}")
            return None
    except Exception as e:
        logger.error(f"Error while acquiring lock for order: {order_id}: {str(e)}")
        return None


async def release_write_lock(order_id: str, lock_value: str) -> bool:
    """
    Release the write lock for a specific order.
    :param order_id: The ID of the order to unlock.
    :param lock_value: The unique value of the lock owner.
    :return: True if the lock was released, False otherwise.
    """
    lock_key = f"write_lock:order:{order_id}"
    meta_key = f"lock_meta:{lock_key}"
    try:

        # Use a Lua script to ensure atomicity
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
        redis.call("DEL", KEYS[1])
        redis.call("DEL", KEYS[2])
        return 1
        else
          return 0
        end
        """
        result = await db_master.eval(lua_script, 1, lock_key, lock_value)
        if result == 1:
            logger.info(f"Lock released for order: {order_id}")
            return True
        else:
            logger.warning(f"Failed to release lock for order {order_id}-lock value mismatch")
            return False  # Returns True if the lock was released, False otherwise
    except Exception as e:
        logger.error(f"Error while releasing lock for order: {order_id}: {str(e)}")
        return False


# Forcibly release abandoned locks
# was unused
async def force_release_locks(order_id: str) -> bool:
    lock_key = f"write_lock:order:{order_id}"
    meta_key = f"lock_meta:{lock_key}"
    try:
        meta = await db_master.hgetall(meta_key)
        if meta:
            acquired_at = float(meta.get("acquired_at", 0))
            timeout = int(meta.get("lock_timeout", LOCK_TIMEOUT))
            if time.time() - (acquired_at + (timeout / 1000) * 2):
                db_master.delete(lock_key)
                db_master.delete(meta_key)
                logger.warning(f"Force released lock for order: {order_id}")
                return True
        return False
    except Exception as e:
        logger.error(f"Error while releasing lock for order: {order_id}: {str(e)}")
        return False





# ----------------------------------------------------------------------------
# Saga State Helpers
# ----------------------------------------------------------------------------

async def publish_event(routing_key, payload):
    """
    Publishes a message to the “saga_events” exchange with the given routing_key.
    """
    try:
        connection = await connect_robust(AMQP_URL)
        channel = await connection.channel()

        # Declare exchange of type "topic" (or direct/fanout as needed)
        exchange = await channel.declare_exchange("saga_events", "topic", durable=True)

        message = Message(
            body=json.dumps(payload).encode(),
            delivery_mode=DeliveryMode.PERSISTENT
        )

        await exchange.publish(message, routing_key=routing_key)
        logger.info(f"[Order Orchestrator] Published event {routing_key} => {payload}")

        await connection.close()
    except Exception as e:
        logger.error(f"Error publishing event: {str(e)}")


# ----------------------------------------------------------------------------
# Leadership (Optional) and Recovery
# ----------------------------------------------------------------------------
async def maintain_leadership():
    """
    Optional: For high availability, tries to maintain “leadership” so only one
    instance runs the saga orchestration at a time. If not needed, remove.
    """
    while True:
        try:
            # Attempt to acquire leadership
            is_leader = await db_master_saga.set(
                "leader:order-orchestrator",
                ORCHESTRATOR_ID,
                nx=True,
                ex=30  # expires in 30 seconds
            )
            if is_leader:
                logger.info("[Order Orchestrator] Acquired leadership.")
            else:
                # If already the leader, refresh the TTL
                if await db_master_saga.get("leader:order-orchestrator") == ORCHESTRATOR_ID:
                    await db_master_saga.expire("leader:order-orchestrator", 30)

            # Heartbeat
            await db_master_saga.setex(f"heartbeat:{ORCHESTRATOR_ID}", 30, "alive")
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"Leadership maintenance error: {str(e)}")
            await asyncio.sleep(5)


async def recover_in_progress_sagas():
    """
    If there are sagas stuck in a “processing” state for too long,
    mark them as failed or attempt compensation.
    """
    try:
        list_of_keys = await db_master_saga.keys("saga:*")
        print(f"--- ORDER SVC: recover_in_progress_sagas - List of keys: {list_of_keys}")
        for key in list_of_keys:
            saga_json = await db_slave_saga.get(key)
            if not saga_json:
                continue

            saga_data = json.loads(saga_json)
            status = saga_data.get("status")
            last_updated = saga_data.get("last_updated", 0)

            # Check for sagas that appear 'stuck'
            if status not in ["SAGA_COMPLETED", "SAGA_FAILED"]:
                if time.time() - last_updated > 60:  # 60s threshold
                    saga_id = saga_data.get("saga_id")
                    order_id = saga_data.get("details", {}).get("order_id")
                    logger.warning(f"[Order Orchestrator] Found stuck saga={saga_id}, forcing fail.")

                    steps_completed = [step["status"] for step in saga_data.get("steps", [])]

                    comp_routing_key = None
                    payload = {}
                    other_compensations = []
                    
                    if "PAYMENT_COMPLETED" in steps_completed:
                        comp_routing_key = "payment.reverse"
                        payload = {
                            "saga_id": saga_id,
                            "order_id": order_id
                        }
                        if "STOCK_RESERVATION_COMPLETED" in steps_completed:
                            other_compensations.append(("stock.compensate", {
                                "saga_id": saga_id,
                                "order_id": order_id
                            }))
                    
                    elif "STOCK_RESERVATION_COMPLETED" in steps_completed:
                        comp_routing_key = "stock.compensate"
                        payload = {
                            "saga_id": saga_id,
                            "order_id": order_id
                        }
                    
                    success = await update_saga_and_enqueue(
                        saga_id = saga_id,
                        status = "SAGA_FAILED_RECOVERY",
                        details={"error": "Timeout recovery triggered"},
                        routing_key = comp_routing_key,
                        payload= payload, #potentially none
                    )

                    if success:
                        for key, payload in other_compensations:
                            await enqueue_outbox_message(key, payload)
                        
                        await enqueue_outbox_message("order.checkout_failed", {
                            "saga_id": saga_id,
                            "order_id": order_id,
                            "status": "failed",
                            "error": "Timeout recovery triggered"
                        })
                    else:
                        logger.critical(f"Failed to enqueue outbox messages for saga {saga_id}")
                    
    except Exception as e:
        print(f"!!! ERROR INSIDE recover_in_progress_sagas: {e}", flush=True)
        logger.error(f"Error recovering sagas: {str(e)}")


# ----------------------------------------------------------------------------
# RabbitMQ Consumer Setup
# ----------------------------------------------------------------------------
async def consume_messages():
    """
    Connect to RabbitMQ, declare exchange/queues, and consume relevant events.
    """
    while True:
        try:
            connection = await connect_robust(AMQP_URL)
            channel = await connection.channel()

            # Declare exchange
            exchange = await channel.declare_exchange("saga_events", "topic", durable=True)

            # Declare & bind relevant queues
            checkout_queue = await channel.declare_queue("order_checkout_requests", durable=True)
            await checkout_queue.bind(exchange, routing_key="order.checkout")

            stock_complete_queue = await channel.declare_queue("stock_complete_events", durable=True)
            await stock_complete_queue.bind(exchange, routing_key="stock.reservation_completed")

            payment_complete_queue = await channel.declare_queue("payment_complete_events", durable=True)
            await payment_complete_queue.bind(exchange, routing_key="payment.completed")

            failure_queue = await channel.declare_queue("saga_failure_events", durable=True)
            await failure_queue.bind(exchange, routing_key="stock.reservation_failed")
            await failure_queue.bind(exchange, routing_key="payment.failed")

            # Start consumers
            logger.info("[Order Orchestrator] Consuming messages...")
            await checkout_queue.consume(process_checkout_request)
            await stock_complete_queue.consume(process_stock_completed)
            await payment_complete_queue.consume(process_payment_completed)
            await failure_queue.consume(process_failure_events)

            # Keep the consumer alive
            await asyncio.Future()

        except Exception as e:
            logger.error(f"Error in consume_messages: {str(e)}")
            await asyncio.sleep(5)

async def outbox_poller():
    #TODO
    pass

# ----------------------------------------------------------------------------
# Optional Health Check
# ----------------------------------------------------------------------------
async def health_check_server():
    """
    Runs a simple HTTP server on port 8000 to respond with 'OK' for readiness checks.
    """

    async def health_handler(request):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/health", health_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=8000)
    await site.start()
    logger.info("[Order Orchestrator] Health check server started on :8000")


async def main():
    await connect_rpc_client()
    asyncio.create_task(health_check_server())
    asyncio.create_task(consume_messages())
    asyncio.create_task(maintain_leadership())
    asyncio.create_task(recover_in_progress_sagas())
    asyncio.create_task(outbox_poller())
    await worker.start()

if __name__ == '__main__':
   asyncio.run(main())

