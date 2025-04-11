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

import global_idempotency
from common.rpc_client import RpcClient
from common.amqp_worker import AMQPWorker
from redis import Sentinel

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort
import warnings

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

sentinel = Sentinel([
    (os.environ['REDIS_SENTINEL_1'], 26379),
    (os.environ['REDIS_SENTINEL_2'], 26380),
    (os.environ['REDIS_SENTINEL_3'], 26381)], socket_timeout=0.1, password=os.environ['REDIS_PASSWORD'])

db_master = sentinel.master_for('order-master', socket_timeout=0.1, decode_responses=True)
db_slave = sentinel.slave_for('order-master', socket_timeout=0.1, decode_responses=True)
db_master_saga = sentinel.master_for('saga-master', socket_timeout=0.1, decode_responses=True)
db_slave_saga = sentinel.slave_for('saga-master', socket_timeout=0.1, decode_responses=True)
# Service id for this service
SERVICE_ID = str(uuid.uuid4())
ORCHESTRATOR_ID = str(uuid.uuid4())
AMQP_URL = os.environ.get('AMQP_URL', 'amqp://user:password@rabbitmq:5672/')
worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="order_queue",
)
OUTBOX_KEY="outbox:order"

def enqueue_outbox_message(routing_key: str, payload:dict):
    message={
        "routing_key":routing_key,
        "payload":payload,
        "timestamp":time.time()
    }
    try:
        db_master_saga.rpush(OUTBOX_KEY,json.dumps(message))
        logger.info(f"[Order Orchestrator] Enqueued message to outbox: {message}")
    except Exception as e:
        logger.error(f"Error while enqueueing message to outbox: {str(e)}")


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


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry = db_slave.get(order_id)
        if not entry:
            logger.warning(f"Order: {order_id} not found in the database")
            return None
        entry_bytes = entry.encode() if isinstance(entry, str) else entry
        return msgpack.decode(entry_bytes, type=OrderValue)
    except redis.exceptions.RedisError as e:
        logger.error(f"Database Error while getting order: {order_id} from the database:{str(e)}")
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    except Exception as e:
        logger.error(f"Error while getting order: {order_id} from the database:{str(e)}")
        return abort(400, DB_ERROR_STR)


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
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
        db_master.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


def update_saga_state(saga_id, status, details=None):
    saga_key = f"saga:{saga_id}"
    try:

        saga_json = db_slave_saga.get(saga_key)

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

        db_master_saga.set(saga_key, json.dumps(saga_data))
        logger.info(f"[Order Orchestrator] Saga {saga_id} updated -> {status}")
    except Exception as e:
        logger.error(f"Error updating saga state {saga_id} to {status.value}: {str(e)}")


def get_saga_state(saga_id: str) -> str:
    try:
        saga_key = f"saga:{saga_id}"
        saga_json = db_slave_saga.get(saga_key)
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
async def create_order(data):
    key = str(uuid.uuid4())
    user_id = data.get("user_id")
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db_master.set(key, value)
        update_saga_state(key, "INITIATED")
    except redis.exceptions.RedisError:
        return DB_ERROR_STR, 400
    return {"order_id": key}, 200


@worker.register
async def find_order(data):
    order_id = data.get("order_id")
    order_entry: OrderValue = get_order_from_db(order_id)
    return {
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost
    }


@worker.register
async def process_checkout_request(data, message):
    """
        Receives 'order.checkout' events to begin the saga.
        Tries to reserve stock first, then waits for success/failure events.
        """
    try:
        order_id = data.get('order_id')
        saga_id = str(uuid.uuid4())  # Create a new Saga ID for this checkout uuid 4 for randomness
        attempt_id = str(message.delivery_tag) if message else str(uuid.uuid4())
        logger.info(f"[Order Orchestrator] Received checkout request for order {order_id}, . Saga ID: {saga_id}")

        idempotency_key = global_idempotency.helper.generate_idempotency_key(SERVICE_NAME,data.get("user_id"),order_id,saga_id)
        try:
            if check_idempotency_key(idempotency_key):
               app.logger.info(f"[IDEMPOTENCY] Duplicate add stock for {idempotency_key}")
               return
        except IdempotencyStoreConnectionError as e:
            app.logger.error(str(e))
            return {"msg": "Redis error during idempotency check"}, 500
        response_data = {"status": "success", "step": "checkout"}
        global_idempotency.helper.store_idempotent_result(idempotency_key,response_data)
        # Initialize saga state
        update_saga_state(saga_id, "SAGA_STARTED", {
            "order_id": order_id,
            "initiated_at": time.time()
        })

        # order_data = await find_order(order_id)
        # Prepare the payload for the stock service
        order_data = await rpc_client.call({
            "type": "find_order",
            "data": {
                "order_id": order_id
            }
        }, "order_queue")

        # Lock order to prevent concurrent processing
        # order_lock_value = acquire_write_lock(order_id)
        order_lock_value = await rpc_client.call({
            "type": "acquire_write_lock",
            "data": {
                "order_id": order_id
            }
        }, "order_queue")

        update_saga_state(saga_id, "ORDER_LOCK_REQUESTED", order_data)
        if order_lock_value is None:
            raise Exception("Order is already being processed")
        logger.info(f"[Order Orchestrator] Lock acquired {order_lock_value} for order {order_id}")
        update_saga_state(saga_id, "ORDER_LOCKED", {
            "order_id": order_id,
            "lock_value": order_lock_value
        })

        # Step 1: Publish stock reserve so the Stock Service can reserve items
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
            # Mark the saga as failed
            update_saga_state(saga_id, "SAGA_FAILED_INITIALIZATION", {"error": str(e)})
            # Notify order of failure
            await publish_event("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })


@worker.register
async def process_stock_completed(data):
    """
          Receives 'stock.reservation_completed' event after Stock Service reserves items.
          Then we proceed to request payment.
          """
    try:
        saga_id = data.get('saga_id')
        order_id = data.get('order_id')
        idempotency_key = global_idempotency.helper.generate_idempotency_key(SERVICE_NAME, order_id, saga_id,
                                                                             "process_stock_completed")

        if check_idempotency_key(idempotency_key):
            logger.info(f"[IDEMPOTENCY] Skipping duplicate stock step for {idempotency_key}")
            return

        logger.info(f"[Order Orchestrator] Stock reservation completed for saga={saga_id}, order={order_id}")
        update_saga_state(saga_id, "STOCK_RESERVATION_COMPLETED")

        # Retrieve saga state to get user_id, total_cost, etc.
        saga_data = get_saga_state(saga_id)
        if not saga_data:
            raise Exception(f"Saga {saga_id} not found")

        order_data = saga_data.get("details", {})
        user_id = order_data.get("user_id")
        total_cost = order_data.get("total_cost")

        if not user_id or not total_cost:
            raise Exception("Missing user_id or total_cost in saga details")

        # Step 2: Publish "payment.request" for Payment Service
        logger.info(f"[Order Orchestrator] Sending remove_credit request for saga={saga_id}, order={order_id}")
        payload = {
            "user_id": user_id,
            "amount": total_cost
        }
        update_saga_state(saga_id, "PAYMENT_INITIATED")
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
            update_saga_state(saga_id, "PAYMENT_INITIATION_FAILED", {"error": str(e)})
            # Request stock compensation since we can’t proceed
            # await publish_event("stock.compensate", {
            #     "saga_id": saga_id,
            #     "order_id": order_id
            # })
            enqueue_outbox_message("stock.compensate", {
                "saga_id": saga_id,
                "order_id": order_id
            })
            # Notify order of failure
            # await publish_event("order.checkout_failed", {
            #     "saga_id": saga_id,
            #     "order_id": order_id,
            #     "status": "failed",
            #     "error": str(e)
            # })
            enqueue_outbox_message("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })


# @worker.register
# async def rollback_stock(order_id: str):
#     order = get_order_from_db(order_id)
#     items = order.items
#     for item_id, quantity in items:
#         payload = {
#             "type": "unlock_stock",
#             "data": {
#                 "item_id": item_id,
#                 "qunatity": quantity
#             }
#         }
#         await rpc_client.call(payload, "stock_queue")
#     update_saga_state(order_id, "STOCK_UNLOCKED")
#
#
# @worker.register
# async def rollback_payment(order_id: str, user_id: str, amount: int):
#     payload = {
#         "type": "add_credit",
#         "data": {
#             "user_id": user_id,
#             "amount": amount,
#             "order_id": order_id
#         }
#     }
#     await rpc_client.call(payload, "payment_queue")
#     update_saga_state(order_id, "PAYMENT_REVERSED")


@worker.register
async def process_payment_completed(data):
    """
      Receives 'payment.completed' after the Payment Service finishes charging.
      Then the order can be finalized.
      """
    try:
        saga_id = data.get("saga_id")
        order_id = data.get("order_id")
        idempotency_key = global_idempotency.helper.generate_idempotency_key(SERVICE_NAME, order_id, saga_id,
                                                                             "process_payment_completed")

        if check_idempotency_key(idempotency_key):
            logger.info(f"[IDEMPOTENCY] Skipping duplicate payment step for {idempotency_key}")
            return

        logger.info(f"[Order Orchestrator] Payment completed for saga={saga_id}, order={order_id}")
        update_saga_state(saga_id, "PAYMENT_COMPLETED")

        # Step 3: Finalize the order in your Order Service by removing the previusly reserved items
        # order_data = await find_order(order_id)
        order_data = await rpc_client.call({
            "type": "find_order",
            "data": {
                "order_id": order_id
            }
        }, "order_queue")
        for item_id, quantity in order_data["items"]:
            payload = {
                "item_id": item_id,
                "quantity": quantity
            }
            worker.send_message(payload=payload, queue="stock_queue", correlation_id=saga_id, action="remove_stock")
        logger.info(f"[Order Orchestrator] Stock removed successfully for saga={saga_id}, order={order_id}")
        update_saga_state(saga_id, "STOCK_REMOVED")

        # Retrieve saga state to get user_id, total_cost, etc.
        saga_data = get_saga_state(saga_id)
        if not saga_data:
            raise Exception(f"Saga {saga_id} not found")

        # Get the lock_value from the saga state
        lock_value = saga_data["details"].get("lock_value")
        if not lock_value:
            raise Exception(f"Lock value not found for saga {saga_id}")

        # Release the order lock
        # if release_write_lock(order_id, lock_value):
        order_released = await rpc_client.call({
            "type": "release_write_lock",
            "data": {
                "order_id": order_id,
                "lock_value": lock_value
            }
        }, "order_queue")
        if order_released:
            update_saga_state(saga_id, "ORDER_LOCK_RELEASED")
        else:
            raise Exception(f"Failed to release lock for order {order_id}")

        update_saga_state(saga_id, "ORDER_FINALIZED")
        update_saga_state(saga_id, "SAGA_COMPLETED", {"completed_at": time.time()})

        # Notify success
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
            update_saga_state(saga_id, "ORDER_FINALIZATION_FAILED", {"error": str(e)})
            # # Compensate payment and stock
            # await publish_event("payment.reverse", {
            #     "saga_id": saga_id,
            #     "order_id": order_id
            # })
            enqueue_outbox_message("payment.reverse",{
                "saga_id": saga_id,
                "order_id": order_id
            })
            # await publish_event("stock.compensate", {
            #     "saga_id": saga_id,
            #     "order_id": order_id
            # })
            enqueue_outbox_message("stock.compensate", {
                "saga_id": saga_id,
                "order_id": order_id
            })
            # # Notify failure
            # await publish_event("order.checkout_failed", {
            #     "saga_id": saga_id,
            #     "order_id": order_id,
            #     "status": "failed",
            #     "error": str(e)
            # })
            enqueue_outbox_message("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })


async def process_failure_events(data):
    """
    Any service can publish a failure event: e.g. 'stock.reservation_failed' or 'payment.failed'.
    The orchestrator listens, updates the saga state, and triggers compensation if needed.
    """
    try:
        saga_id = data.get("saga_id")
        order_id = data.get("order_id")
        # TODO: Extract error message from the event
        error = data.get('error', 'Unknown error')
        routing_key = data.routing_key

        logger.info(f"[Order Orchestrator] Failure event {routing_key} for saga={saga_id}: {error}")
        update_saga_state(saga_id, f"FAILURE_EVENT_{routing_key.upper()}", {"error": error})

        saga_data = get_saga_state(saga_id)
        if not saga_data:
            logger.error(f"[Order Orchestrator] Saga {saga_id} not found.")
            # await message.ack()
            return

        # Check which steps were completed
        steps_completed = [step["status"] for step in saga_data.get("steps", [])]
        compensations = []

        # If payment was completed but we got a failure from somewhere else, reverse it
        if "PAYMENT_COMPLETED" in steps_completed and routing_key != "payment.failed":
            compensations.append(("payment.reverse", {"saga_id": saga_id, "order_id": order_id}))

        # If stock was reserved but we have a new failure (not from stock reservation itself), roll it back
        if "STOCK_RESERVATION_COMPLETED" in steps_completed and routing_key != "stock.reservation_failed":
            compensations.append(("stock.compensate", {"saga_id": saga_id, "order_id": order_id}))

        # Publish compensation events in reverse order
        for comp_key, payload in compensations:
            await publish_event(comp_key, payload)

        update_saga_state(saga_id, "SAGA_FAILED", {
            "error": error,
            "compensations_initiated": [c[0] for c in compensations],
            "failed_at": time.time()
        })

        # Notify that checkout failed
        await publish_event("order.checkout_failed", {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "failed",
            "error": error
        })
        enqueue_outbox_message("order.checkout_failed",{
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "failed",
            "error": error
        })

    except Exception as e:
        logger.error(f"Error in process_failure_events: {str(e)}")


# ----------------------------------------------------------------------------
# Idempotency Helper
# ----------------------------------------------------------------------------

async def execute_idempotent_operation(key: str, message, handler_function, *args, **kwargs):
    try:
        stored_result = check_idempotency_key(key)
        if stored_result:
            logger.info(f"Idempotency hit for key {key}. Acking.")
            await message.ack()
            return
    except Exception as e:
        logger.error(f"Error checking idempotency key: {str(e)}")
        await message.nack(requeue=True)
        return

    # if idompentcy check is successful, call the handler function
    try:
        await handler_function(message.correlation_id, message.type, message.body, *args, **kwargs)
    except Exception:
        logger.exception(f"Handler {handler_function.__name__} failed for key {key}")


# ----------------------------------------------------------------------------
# Saga State Helpers
# ----------------------------------------------------------------------------

def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response



@worker.register
async def add_item(data):
    order_id = data.get("order_id")
    item_id = data.get("item_id")
    quantity = data.get("quantity")
    order_entry: OrderValue = get_order_from_db(order_id)

    # Prepare the payload for the stock service
    payload = {
        "type": "find_item",  # Message type for the stock service
        "data": {
            "item_id": item_id
        }
    }

    stock_response = await rpc_client.call(payload, "stock_queue")

    # Check if the stock service returned an error
    if not stock_response or "error" in stock_response:
        abort(400, f"Item: {item_id} does not exist or stock service error!")

    # Extract item details from the stock service response
    item_price = stock_response.get("price")
    if item_price is None:
        abort(400, f"Item: {item_id} price not found!")

    # Update the order with the new item
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_price

    # Save the updated order back to the database
    try:
        db_master.set(order_id, msgpack.encode(order_entry))
        update_saga_state(order_id, "ITEM_ADDED")
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    return {"msg": f"Item: {item_id} added to order: {order_id}, total cost updated to: {order_entry.total_cost}"}


# Helper methods locks
def acquire_write_lock(order_id: str, lock_timeout: int = 10000) -> str:
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
        is_locked = db_master.set(lock_key, lock_value, nx=True, px=lock_timeout)
        if is_locked:
            logger.info(f"Lock acquired for order: {order_id}")
            db_master.hmset(
                f"lock_meta:{lock_key}",
                {
                    "service_id": SERVICE_ID,
                    "ttl": SAGA_STATE_TTL,
                    "lock_timeout": lock_timeout,
                    "lock_value": lock_value
                }
            )
            return lock_value
        else:
            logger.info(f"Lock already acquired for order: {order_id}")
            return None
    except Exception as e:
        logger.error(f"Error while acquiring lock for order: {order_id}: {str(e)}")
        return None


def release_write_lock(order_id: str, lock_value: str) -> bool:
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
        result = db_master.eval(lua_script, 1, lock_key, lock_value)
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
def force_release_locks(order_id: str) -> bool:
    lock_key = f"write_lock:order:{order_id}"
    meta_key = f"lock_meta:{lock_key}"
    try:
        meta = db_master.hgetall(meta_key)
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
            is_leader = db_master_saga.set(
                "leader:order-orchestrator",
                ORCHESTRATOR_ID,
                nx=True,
                ex=30  # expires in 30 seconds
            )
            if is_leader:
                logger.info("[Order Orchestrator] Acquired leadership.")
            else:
                # If already the leader, refresh the TTL
                if db_master_saga.get("leader:order-orchestrator") == ORCHESTRATOR_ID:
                    db_master_saga.expire("leader:order-orchestrator", 30)

            # Heartbeat
            db_master_saga.setex(f"heartbeat:{ORCHESTRATOR_ID}", 30, "alive")
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
        for key in db_master_saga.keys("saga:*"):
            saga_json = db_slave_saga.get(key)
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

                    update_saga_state(saga_id, "SAGA_FAILED_RECOVERY", {
                        "error": "Timeout recovery triggered"
                    })

                    # Trigger compensation if needed
                    steps_completed = [step["status"] for step in saga_data.get("steps", [])]
                    if "PAYMENT_COMPLETED" in steps_completed:
                        # await publish_event("payment.reverse", {
                        #     "saga_id": saga_id,
                        #     "order_id": order_id
                        # })
                        enqueue_outbox_message("payment.reverse",{
                            "saga_id": saga_id,
                            "order_id": order_id
                        })
                    if "STOCK_RESERVATION_COMPLETED" in steps_completed:
                        # await publish_event("stock.compensate", {
                        #     "saga_id": saga_id,
                        #     "order_id": order_id
                        # })
                        enqueue_outbox_message("stock.compensate",{
                            "saga_id": saga_id,
                            "order_id": order_id
                        })
                    # Notify that it failed
                    await publish_event("order.checkout_failed", {
                        "saga_id": saga_id,
                        "order_id": order_id,
                        "status": "failed",
                        "error": "Timeout recovery triggered"
                    })
                    enqueue_outbox_message("order.checkout_failed",{
                        "saga_id": saga_id,
                        "order_id": order_id,
                        "status": "failed",
                        "error": "Timeout recovery triggered"
                    })

    except Exception as e:
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
    while True:
        try:
            raw=db_master_saga.loop(OUTBOX_KEY)
            if not raw:
                await asyncio.sleep(1)
                continue
            message=json.loads(raw)
            routing_key=message.get("routing_key")
            payload=message.get("payload")

            connection = await connect_robust(AMQP_URL)
            channel = await connection.channel()
            exchange = await channel.declare_exchange("saga_events", "topic", durable=True)
            amqp_message = Message(
                body=json.dumps(payload).encode(),
                delivery_mode=DeliveryMode.PERSISTENT
            )
            await exchange.publish(amqp_message, routing_key=routing_key)
            logger.info(f"[Outbox] Published event {routing_key} => {payload}")
            await connection.close()
        except Exception as e:
            logger.error(f"Error publishing event: {str(e)}")
            await asyncio.sleep(5)


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

