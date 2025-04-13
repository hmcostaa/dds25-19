import json
import logging
import os
import atexit
import random
import uuid
import asyncio
import time

from aiohttp import web
from aio_pika import connect_robust, Message, DeliveryMode
from msgspec import  DecodeError as MsgspecDecodeError
from typing import  Tuple, Dict,  Union
from common.rpc_client import RpcClient
from common.amqp_worker import AMQPWorker

import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort
import warnings

import redis.asyncio as redis
from redis.asyncio.sentinel import Sentinel
import redis

from global_idempotency.idempotency_decorator import idempotent

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
    (os.environ['REDIS_SENTINEL_3'], 26379)], #TODO 26379 for evey sentinel now
    socket_timeout=10, #TODO check if it can be lowered
    socket_connect_timeout=5,
    socket_keepalive=True,
    password=os.environ['REDIS_PASSWORD']
)


db_master = sentinel_async.master_for('order-master', decode_responses=False)
db_slave = sentinel_async.slave_for('order-master', socket_timeout=10, decode_responses=False)
db_master_saga = sentinel_async.master_for('saga-master', socket_timeout=10, decode_responses=False)
db_slave_saga = sentinel_async.slave_for('saga-master', socket_timeout=10, decode_responses=False)

#changed.. now saga master is used for idempotency as centralized client
# Read connection details from environment variables

idempotency_redis_db = int(os.environ.get('IDEMPOTENCY_REDIS_DB', 0)) 
idempotency_redis_client = sentinel_async.master_for(
    "saga-master",
    decode_responses=False,
    db=idempotency_redis_db
)

SERVICE_ID = str(uuid.uuid4())
ORCHESTRATOR_ID = str(uuid.uuid4())
AMQP_URL = os.environ.get('AMQP_URL', 'amqp://user:password@rabbitmq:5672/')
worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="order_queue",
)
OUTBOX_KEY="outbox:order"

saga_futures={}
pubsub_clients={}
listener_tasks={}

async def cleanup_pubsub_resource():
    logger.info("Cleaning up Redis PubSub resources.")
    for saga_id, task in listener_tasks.items():
        if not task.done():
            task.cancel()
    for saga_id, pubsub in pubsub_clients.items():
        await pubsub.close()
    saga_futures.clear()
    pubsub_clients.clear()
    listener_tasks.clear()



async def start_pubsub_listener(saga_id: str)->asyncio.Task:
    notification_channel=f"saga_completion:{saga_id}"
    pubsub = db_master_saga.pubsub()
    pubsub_clients[saga_id] = pubsub

    async def listen_for_completion():
        try:
            await pubsub.subscribe(notification_channel)
            logger.info(f"[Pubsub] Subscribed to {notification_channel}")
            while not saga_futures[saga_id].done():
                try:
                  message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                  if message and message['type'] == 'message':
                        data = json.loads(message['data']) if isinstance(message['data'], str) else message['data']
                        logger.info(f"[PubSub] Received notification for saga {saga_id}: {data}")
                        completion_data = json.loads(data) if isinstance(data, str) else data
                        if saga_id in saga_futures and not saga_futures[saga_id].done():
                               if completion_data.get("success", False):
                                   saga_futures[saga_id].set_result({"success": True})
                               else:
                                 error_msg = completion_data.get("error", "Unknown error")
                                 saga_futures[saga_id].set_result({"success": False, "error": error_msg})
                        break
                  current_saga=await get_saga_state(saga_id)
                  if current_saga:
                      status = current_saga.get("status")
                      if status in["SAGA_COMPLETED", "ORDER_FINALIZED"]:
                          if saga_id in saga_futures and not saga_futures[saga_id].done():
                              saga_futures[saga_id].set_result({"success": True})
                              break
                      elif status in ["SAGA_FAILED", "SAGA_FAILED_INITIALIZATION",
                                      "SAGA_FAILED_RECOVERY", "ORDER_FINALIZATION_FAILED","SAGA_FAILED_ORDER_NOT_FOUND"]:
                          error_details=current_saga.get("details",{}).get("error","Unknown saga failure")
                          if saga_id in saga_futures and not saga_futures[saga_id].done():
                              saga_futures[saga_id].set_result({"success": False, "error": error_details})
                              break
                  await asyncio.sleep(0.1)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"[PubSub] Error while processing messages: {e}")
                    await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"[PubSub] Listener error for {saga_id}: {e}")
            if saga_id in saga_futures and not saga_futures[saga_id].done():
                saga_futures[saga_id].set_exception(e)
        finally:
            try:
                await pubsub.unsubscribe(notification_channel)
                logger.info(f"[PubSub] Unsubscribed from {notification_channel}")
                if saga_id in pubsub_clients:
                    await pubsub_clients[saga_id].close()
                    del pubsub_clients[saga_id]
                if saga_id in listener_tasks:
                    del listener_tasks[saga_id]
            except Exception as e:
                logger.error(f"[PubSub] Cleanup error for {saga_id}: {e}")
    task=asyncio.create_task(listen_for_completion())
    listener_tasks[saga_id] = task
    return task

async def handle_final_saga_state(saga_id, request_idempotency_key, final_status, result_payload, status_code):
    logger.info(f"[Order Orchestrator] Handling final saga state for saga {saga_id}")

    # verify tests assumes immediate correct response instead of partial 202
    # same for test microservices when it gives 'error' input and therefore fails
    # so this has to be made sync
    # could potentially lock-> wait check until final status= COMPLETED-> release lock
    # pre-checks?? however would not guarantee consistent state return, but is faster
    # adjust test? elaborate ? or show two version for performance difference
    # would be nice, but time constraint
    saga_data=await get_saga_state(saga_id)
    if saga_data:
        current_status=saga_data.get("status")
        if current_status=="SAGA_COMPLETED":
            logger.info(f"[Order Orchestrator] Saga {saga_id} already completed successfully")
            return result_payload, status_code
        elif current_status in ["SAGA_FAILED","SAGA_FAILED_INITIALIZATION","SAGA_FAILED_ORDER_NOT_FOUND",
                                "SAGA_FAILED_RECOVERY","ORDER_FINALIZATION_FAILED"]:
            error_details=saga_data.get("details",{}).get("error","Unknown saga failure")
            return {"error":error_details},400
    saga_futures[saga_id]=asyncio.Future()
    await start_pubsub_listener(saga_id)
    try:
        await asyncio.wait_for(saga_futures[saga_id],timeout=15)
        saga_result=saga_futures[saga_id].result()
        logger.info(f"[Order Orchestrator] Saga {saga_id} completed with result: {saga_result}")
        if saga_result.get("success",False):
            return result_payload, status_code

        else:
            return {"error": saga_result.get("error", "Unknown saga failure")}, 400
    except asyncio.TimeoutError:
        logger.warning(f"[Order Orchestrator] Timed out waiting for saga {saga_id}")
        return result_payload, status_code
    finally:
        # Clean up resources
        if saga_id in saga_futures:
            del saga_futures[saga_id]

        if saga_id in listener_tasks and not listener_tasks[saga_id].done():
            listener_tasks[saga_id].cancel()

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
        logger.error(f"Error while enqueueing message to outbox: {str(e)}", exc_info=True)

async def update_saga_and_enqueue(saga_id, status,  routing_key: str|None, payload:dict|None, details=None,max_attempts: int=5,backoff_ms:int=100):
#retry maybe todo too with max attempts?
    saga_key = f"saga:{saga_id}"
    notification_channel = f"saga_completion:{saga_id}"
    for attempt in range(max_attempts):
       try:
           async with db_master_saga.pipeline(transaction=True) as pipe:
               await pipe.watch(saga_key)
               saga_json = await pipe.get(saga_key)
               if saga_json:
                   if isinstance(saga_json, bytes):
                       saga_data = saga_json.decode("utf-8")
                   saga_data = json.loads(saga_data)
               else:
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
                saga_data["steps"].append({
                "status": status,
                "timestamp": time.time(),
                "details": details
            })
               outbox_msg = None
               if payload and routing_key:
                 outbox_msg={
                    "routing_key":routing_key,
                    "payload":payload,
                    "timestamp":time.time()
                 }
                 outbox_msg_json = json.dumps(outbox_msg)
               pipe.multi()
               await pipe.set(saga_key, json.dumps(saga_data),ex=SAGA_STATE_TTL)
               if outbox_msg:
                   await pipe.rpush(OUTBOX_KEY,outbox_msg_json)
               is_success_terminal = status in ["SAGA_COMPLETED", "ORDER_FINALIZED"]
               is_failure_terminal = status in ["SAGA_FAILED", "SAGA_FAILED_INITIALIZATION",
                                                "SAGA_FAILED_RECOVERY", "ORDER_FINALIZATION_FAILED"]
               if is_success_terminal:
                   await pipe.publish(notification_channel, json.dumps({"success":True}))
               elif is_failure_terminal:
                   error_msg = details.get("error", "Unknown error") if details else "Unknown error"
                   await pipe.publish(notification_channel, json.dumps({"success": False, "error": error_msg}))
               results=await pipe.execute()
               logger.info(f"[Order Orchestrator] Saga {saga_id} updated -> {status}")
               if (is_success_terminal or is_failure_terminal) and saga_id in saga_futures:
                   future=saga_futures[saga_id]
                   if not future.done():
                       if is_success_terminal:
                           future.set_result({"success":True})
                       else:
                         error_msg = details.get("error", "Unknown error") if details else "Unknown error"
                         future.set_result({"success": False, "error": error_msg})
               logger.info(
                   f"[Order Orchestrator Atomic] Saga {saga_id} updated -> {status}. Enqueued message to outbox")
               return True
       except redis.exceptions.WatchError:
           logger.warning(f"[SAGA UPDATE] Retry {attempt+1}/{max_attempts}. Saga {saga_id} failed to update. WatchError on {saga_id}")
       except redis.exceptions.RedisError as e:
           logger.error(f"[SAGA UPDATE] Redis error on attempt {attempt+1}: {e}")
           logger.error (f"Error redis update saga state {saga_id} to {status}: {str(e)}")
           await asyncio.sleep(backoff_ms/1000)
           backoff_ms *=2
       except Exception as e:
        logger.error(f"Error updating saga state {saga_id} to {status}: {str(e)}", exc_info=True)
    logger.critical("f[SAGA UPDATE] Failed after {max_attempts} attempts for {saga_id}.")
    return False

async def close_db_connection():
    await db_master.close()
    await db_slave.close()
    await db_master_saga.close()
    await db_slave_saga.close()
    await cleanup_pubsub_resource()
    await idempotency_redis_client.close()




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
            if isinstance(saga_json, bytes):
                saga_json = saga_json.decode('utf-8')
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
@idempotent('create_order', idempotency_redis_client, SERVICE_NAME)
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
        # return {"order_id": key}, 200
        return await handle_final_saga_state(
            saga_id=key,
            request_idempotency_key=data.get("idempotency_key", ""),
            final_status="INITIATED",
            result_payload={"order_id": key},
            status_code=200
        )
    
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
@idempotent('process_checkout_request', idempotency_redis_client, SERVICE_NAME)
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

        await update_saga_and_enqueue(
            saga_id,
            status="SAGA_STARTED",
            routing_key=None,
            payload=None,
        )

        order_result, status_code = await get_order_from_db(order_id)

        if status_code != 200:
            logger.error(f"Failed to retrieve order {order_id} from DB for checkout. Status code: {status_code}")
            await update_saga_and_enqueue(
                saga_id,
                status="SAGA_FAILED_ORDER_NOT_FOUND",
                details={"error": f"Failed to retrieve order {order_id} from DB for checkout. Status code: {status_code}"},
                routing_key=None,
                payload=None,
            )
            return order_result if isinstance(order_result, dict) else {"error": f"Failed to retrieve order {order_id} from DB for checkout. Status code: {status_code}"}

        order_data = {
           "order_id": order_id,
           "paid": order_result.paid,
           "items": order_result.items,
           "user_id": order_result.user_id, 
           "total_cost": order_result.total_cost
       }

        order_lock_value = await acquire_write_lock(order_id)


        #-- hopefully no DEADLOCK, using local lock instead of rpc call to itself


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

        items_to_be_reserved = order_data.get("items", [])
        if not items_to_be_reserved:
            logger.error(f"[Order Orchestrator] No items to be fetched for order {order_id}")
            raise Exception(f"No items to be fetched or reserved for order {order_id}")
        
        logger.info(f"[Order Orchestrator] Sending reserve stock message for saga={saga_id}, order={order_id}")
        request_idempotency_key=data.get("idempotency_key","")
        for item_id, quantity in items_to_be_reserved:
            stock_idempotency_key = f"{request_idempotency_key}:{saga_id}:{item_id}:{quantity}"
            payload = {
                "saga_id": saga_id,
                "order_id": order_id,
                "item_id": item_id,
                "amount": quantity,
                "idempotency_key": stock_idempotency_key,

            }
            await worker.send_message(payload=payload, queue="stock_queue", correlation_id=saga_id,
                                action="remove_stock", reply_to="orchestrator_queue",
                                callback_action="process_stock_completed")

        return await handle_final_saga_state(
            saga_id=saga_id,
            request_idempotency_key=data.get("idempotency_key",""),
            final_status="SAGA_STARTED",
            result_payload={"status": "success", "step": "stock reservation initiated"},
            status_code=200
        )

    except Exception as e:
        logger.error(f"Error in process_checkout_request: {str(e)}")
        saga_id = locals().get("saga_id")
        if saga_id:
            # Mark the saga as failed
            # update_saga_state(saga_id, "SAGA_FAILED_INITIALIZATION", {"error": str(e)})

           try:
               success= await update_saga_and_enqueue(
                   saga_id=saga_id,
                   status="SAGA_FAILED_INITIALIZATION",
                   details={"error": str(e)},
                   routing_key=None,
                   payload=None,
               )
               if not success:
                   logger.critical(f"[Saga] Failed to update {saga_id} with error: {str(e)}")
                   raise Exception("Saga update failed - will be sent to DLQ")
           except Exception as e:
                logger.exception(f"Saga initialization failed: {str(e)}", exc_info=True)
        
        return {"error": str(e)}, 500

@worker.register
@idempotent('process_stock_completed', idempotency_redis_client, SERVICE_NAME)
async def process_stock_completed(data, message):
    """
          Receives 'stock.reservation_completed' event after Stock Service reserves items.
          Then we proceed to request payment.
          """
    try:
        saga_id = data.get('saga_id')
        order_id = data.get('order_id')
        
        if not saga_id:
            logger.error(f"no saga_id in process_stock_completed")
            return {"error": "no saga_id in process_stock_completed"}, 400
        
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
        payment_idempotency_key = f"{saga_id}:payment:{user_id}:{total_cost}"

        payload = {
            "user_id": user_id,
            "amount": total_cost,
            "idempotency_key": payment_idempotency_key
        }

        await update_saga_and_enqueue(
            saga_id=saga_id,
            status="PAYMENT_INITIATED",
            details=None,
            routing_key="payment_queue",
            payload=payload,
        )
        # # TODO: Probably only the last message should have a callback_action???
        await worker.send_message(payload=payload, queue="payment_queue", correlation_id=saga_id,
                            action="remove_credit", reply_to="orchestrator_queue",
                            callback_action="process_payment_completed")
        # response_data = {"status": "success", "step": "stock reservation completed"}
        # global_idempotency.helper.store_idempotent_result(idempotency_key,response_data)
        return {"status": "success", "step": "stock reservation completed"}, 200

    except Exception as e:
        logger.error(f"Error in process_stock_completed: {str(e)}")
        if 'saga_id' in locals():
            #TODO: check updated
            try:
                success = await update_saga_and_enqueue(
                    saga_id=saga_id,
                    status="PAYMENT_INITIATION_FAILED",
                    details={"error": str(e)},
                    routing_key="stock.compensate",
                    payload={"saga_id": saga_id, "order_id": order_id}
                )
                if not success:
                    logger.critical(f"[Saga] Could not enqueue compensation for {saga_id} with error: {str(e)}")
                    raise Exception(f"Saga {saga_id} and enqueue compensation failed - will be sent to DLQ: {str(e)}")
            except Exception as e:
               logger.exception(f"Error in process_stock_completed: {str(e)}", exc_info=True)


@worker.register
@idempotent('process_payment_completed', idempotency_redis_client, SERVICE_NAME)
async def process_payment_completed(data, message):
    """
      Receives 'payment.completed' after the Payment Service finishes charging.
      Then the order can be finalized.
      """
    try:
        saga_id = data.get("saga_id")
        order_id = data.get("order_id")

        logger.info(f"[Order Orchestrator] Payment completed for saga={saga_id}, order={order_id}")
        await update_saga_and_enqueue(
            saga_id=saga_id,
            status="PAYMENT_COMPLETED",
            details=None,
            routing_key=None,
            payload=None,
        )

        # order_data = await rpc_client.call(queue="order_queue",action="find_order",payload={"order_id": order_id})

        saga_data = await get_saga_state(saga_id)
        if not saga_data:
            raise Exception(f"Saga {saga_id} not found")
        
        item_details = saga_data.get("details", {})
        items_to_be_removed = item_details.get("items", [])
        lock_value = item_details.get("lock_value")

        if not items_to_be_removed:
            logger.error(f"[Order Orchestrator] No items to be removed for order {order_id}")
            raise Exception(f"No items to be removed for order {order_id}")
        
        for item_id, quantity in items_to_be_removed:
            stock_remove_idempotency_key = f"{saga_id}:stock_remove:{item_id}:{quantity}"
            payload = {
                "item_id": item_id,
                "amount": quantity,
                "idempotency_key": stock_remove_idempotency_key
            }
            await worker.send_message(payload=payload, queue="stock_queue", correlation_id=saga_id, action="remove_stock")
        
        logger.info(f"[Order Orchestrator] Stock removed successfully for saga={saga_id}, order={order_id}")
        await update_saga_and_enqueue(
            saga_id,
            "STOCK_REMOVED",
            routing_key=None,
            payload=None,
        )

        order_released = await release_write_lock(order_id, lock_value)
        if order_released:
            await update_saga_and_enqueue(
                saga_id,
                "ORDER_LOCK_RELEASED",
                routing_key=None,
                payload=None,
            )
        else:
            logger.error(f"Failed to release lock for order {order_id} with lock value {lock_value}")
            await update_saga_and_enqueue(
                saga_id,
                "ORDER_LOCK_RELEASE_FAILED",
                details={"error": f"Failed to release lock for order {order_id} with value {lock_value}"},
                routing_key=None,
                payload=None,
            )

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
        
        response_data = {"status": "success", "step": "payment completed"}
        return response_data, 200

    except Exception as e:
        logger.error(f"Error in process_payment_completed: {str(e)}")
        if 'saga_id' in locals():
            #TODO: check updated
            try:
                success = await update_saga_and_enqueue(
                    saga_id=saga_id,
                    status="ORDER_FINALIZATION_FAILED",
                    details={"error": str(e)},
                    routing_key="payment.reverse",
                    payload={"saga_id": saga_id, "order_id": order_id}
                )
                if not success:
                    logger.critical(f"Could not update saga {saga_id} for order {order_id} during failure handling")
                    raise Exception(f"Saga failure handler failed-message will go to DLQ")
                
                steps_completed = [steps["status"] for steps in saga_data.get("steps", [])]
                order_details = saga_data.get("details", {}) if saga_data else {}

                if "STOCK_RESERVATION_COMPLETED" in steps_completed:
                    await enqueue_outbox_message("stock.compensate", {
                            "saga_id": saga_id,
                            "order_id": order_details.get("order_id",order_id)
                        })

                # Enqueue final failure event
                await enqueue_outbox_message("order.checkout_failed", {
                        "saga_id": saga_id,
                        "order_id": order_details.get("order_id",order_id),
                        "status": "failed",
                        "error": f"Failed during order finalization: {str(e)}"
                    })
            except Exception as e:
                logger.exception(f"[SAGA] Critical error: {str(e)}", exec_info=True)
                raise
        
        return {"error": f"Failed to update payment for order {order_id}"}, 500

@idempotent('process_failure_events', idempotency_redis_client, SERVICE_NAME)
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


# #TODO potentially blocking and synchronous
# def send_post_request(url: str):
#     try:
#         response = requests.post(url)
#     except requests.exceptions.RequestException:
#         return {"error": "Request error"}, 400
#     else:
#         return response


#TODO should be made atomic, easily done through @idempotent when we merge with stock/payment idempotent branch

@worker.register
@idempotent("add_item",idempotency_redis_client,SERVICE_NAME)
async def add_item(data, message):
    order_id = data.get("order_id")
    item_id = data.get("item_id")
    quantity = data.get("quantity")
    stock_response = None

    if not order_id or not item_id or not quantity:
        return {"error": "Missing required fields (order_id,item_id, quantity)"}, 400
        # load the order

    #call stock service

    payload = {
        "item_id": item_id #should be consistent with stock
    }
    try:
        logger.info(f"--- ORDER SVC: add_item - Stock service payload: {payload}")

        stock_response = await rpc_client.call(queue="stock_queue", action="find_item", payload=payload)
        logger.info(f"--- ORDER SVC: add_item - Stock service response: {stock_response}")
    except Exception as e:
        logger.error(f"Unexpected error getting stock data for item {item_id}: {e}")
        return {"error": "Unexpected error getting stock data"}, 503

    stock_data = stock_response.get('data')
    if not isinstance(stock_data, dict):
        logger.error(f"--- ORDER SVC: add_item - Stock service returned invalid data type: {type(stock_data)}")
        return {"error": f"Item data not found, item: {item_id}"}, 404
    
    item_price = stock_data.get("price")
    item_stock = stock_data.get("stock")
    if item_price is None:
        logger.error(f"--- Invalid or missing price for item {item_id}")
        return {"error": f"Item: {item_id} price not found!"}, 404
    try:
        if item_stock < quantity:
            return {"error": f"Item {item_id} is out of stock. Requested {quantity}, available {item_stock}."}, 400
    except(ValueError, TypeError):
        return {"error": "Invalid quantity specified"}, 400
    
    def update_func(order: OrderValue) -> OrderValue:
        order.items.append((item_id, quantity))
        order.total_cost+=quantity*item_price
        return order

    error_msg = None
    try:
        updated_order,error_msg= await atomic_update_order(order_id, update_func=update_func)
        if error_msg:
            logging.error(f"Failed to update order {order_id}: {error_msg}")
            logging.debug(error_msg)
            if "not_found" in error_msg.lower():
                status_code=404
            else:
                status_code=500
            return ({"added": False, "error": error_msg}, status_code)
        elif updated_order:
            logging.debug(f"Updated order {order_id}: {updated_order}")
            await update_saga_and_enqueue(
                saga_id=order_id,
                status="ITEM_ADDED",
                routing_key=None,
                payload=None,
                details={"item_id": item_id, "quantity": quantity}
            )
            # return {"data": {"added": True, "order_id": order_id, "new_total_cost": updated_order.total_cost}}, 200
            return await handle_final_saga_state(
                saga_id=order_id,
                request_idempotency_key=data.get("idempotency_key", ""),
                final_status="ITEM_ADDED",
                result_payload={
                    "data": {"added": True, "order_id": order_id, "new_total_cost": updated_order.total_cost}},
                status_code=200
            )
        else:
            return ({"added": False, "error": "Internal processing error"}, 500)
    except ValueError as e:
        error_msg=str(e)
        return ({"added": False, "error": error_msg}, 400)
    except Exception as e:
        logging.exception("internal error")
        return ({"added": False, "error": error_msg}, 400)
    
    #update and save order
#made async TODO
async def atomic_update_order(order_id, update_func):
    max_retries = 5
    base_backoff = 50
    for attempt in range(max_retries):
        try:
            async with db_master.pipeline(transaction=True) as pipe:
                await pipe.watch(order_id)
                entry= await pipe.get(order_id)
                if not entry:
                    await pipe.unwatch()
                    logging.warning("Atomic update: order: %s", order_id)
                    return None, f"Order {order_id} not found"
                
                try:
                    order_val = msgpack.Decoder(OrderValue).decode(entry)
                    await pipe.unwatch()
                except MsgspecDecodeError:
                    await pipe.unwatch()
                    logging.error("Atomic update: Decode error for order %s", order_id)
                    return None, "Internal data format error"
                
                try:
                    updated_order = update_func(order_val)
                    
                except ValueError as e:
                    await pipe.unwatch()
                    raise e
                except Exception as e:
                    await pipe.unwatch()
                    return None, f"Unexpected error in atomic_update_order for order {order_id}: {e}"
                pipe.multi()
                pipe.set(order_id, msgpack.Encoder().encode(updated_order))
                results = await pipe.execute()

                return updated_order, None 
        except redis.WatchError:
            # key was modified between watch and execute, retry backoff
            backoff_multiplier = (2 ** attempt) * (1 + random.random() * 0.1)
            backoff = base_backoff * backoff_multiplier
            await asyncio.sleep(backoff / 1000)
            continue  # loop again

        except redis.RedisError:
            return None, DB_ERROR_STR
        except ValueError:
            raise

        except Exception as e:
            logging.exception(f"Unexpected error in atomic_update_order for order {order_id}: {e}")
            return None, "Internal data error"

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
            await db_master.hset(
                f"lock_meta:{lock_key}", mapping={
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
        result = await db_master.eval(lua_script, 2, lock_key, lock_value)
        #i think this should be 2 instead of 1, my lue experience is limited
        #TODO check
        if result == 1:
            logger.info(f"Lock released for order: {order_id}")
            return True
        else:
            current_lock_value = await db_master.get(lock_key)
            if current_lock_value == lock_value:
                logger.warning(f"still has a value for lock {lock_key} ")
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
                await db_master.delete(lock_key)
                await db_master.delete(meta_key)
                logger.warning(f"Force released lock for order: {order_id}")
                return True
        return False
    except Exception as e:
        logger.error(f"Error while releasing lock for order: {order_id}: {str(e)}")
        return False

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
        logger.error(f"Error publishing event: {str(e)}", exc_info=True)


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
        logger.error(f"Error recovering sagas: {str(e)}", exec_info=True)


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
            logger.error(f"Error in consume_messages: {str(e)}", exc_info=True)
            await asyncio.sleep(5)

async def outbox_poller():
    while True:
        try:
            #blpop instead of loop
            #TODO what if message loss between LOP and succesfull AMQP publish???
            #Added brpoplush to prevent the issue
            raw= await db_master_saga.brpoplpush(OUTBOX_KEY, f"{OUTBOX_KEY}:processing",timeout=5)
            if not raw:
                await asyncio.sleep(1)
                continue
            message=json.loads(raw)
            routing_key=message.get("routing_key")
            payload=message.get("payload")

            await publish_event(routing_key=routing_key,payload=payload)
            await db_master_saga.lrem(f"{OUTBOX_KEY}:{routing_key}", 0,json.dumps(message))
            logger.info(f"[Outbox] Published event {routing_key} => {payload}")
        except Exception as e:
            logger.error(f"Error publishing event: {str(e)}", exc_info=True)
            await asyncio.sleep(5)

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
   try:
    await connect_rpc_client()
    asyncio.create_task(health_check_server())
    asyncio.create_task(consume_messages())
    asyncio.create_task(maintain_leadership())
    asyncio.create_task(recover_in_progress_sagas())
    asyncio.create_task(outbox_poller())

    await worker.start()
   finally:
       await close_db_connection()

if __name__ == '__main__':
   asyncio.run(main())

