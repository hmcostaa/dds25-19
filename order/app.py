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
from saga_states import SagaStateEnum, TERMINAL_SAGA_STATES

import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort
import warnings
from redis.exceptions import WatchError, RedisError
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


async def close_db_connection():
    await db_master.close()
    await db_slave.close()
    await db_master_saga.close()
    await db_slave_saga.close()
    # await cleanup_pubsub_resource()
    await idempotency_redis_client.close()

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

REPLY_QUEUE_NAME = "orchestrator_reply_queue"
orchestrator_reply_worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name=REPLY_QUEUE_NAME,
)

OUTBOX_KEY="outbox:order"

saga_futures={}
pubsub_clients={}
listener_tasks={}

#--------SAGA STATE MANAGEMENT HELPERS ---------

async def get_saga_state(saga_id: str) -> dict | None:
    saga_key = f"saga:{saga_id}"
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

# async def _update_saga_redis_state(
#         pipe: redis.asyncio.client.Pipeline,
#         saga_id: str,
#         new_state: str,
#         details_to_add: dict = None):

#     saga_key = f"saga:{saga_id}"
#     saga_bytes = await pipe.get(saga_key)
#     if saga_bytes:
#         try: saga_data = json.loads(saga_bytes.decode('utf-8'))
#         except (json.JSONDecodeError, UnicodeDecodeError):
#             saga_data = {"saga_id": saga_id, "history": [], "details": {}}
#     else: 
#         saga_data = {"saga_id": saga_id, "history": [], "details": {}}
#     saga_data["current_state"] = new_state
#     saga_data["last_updated"] = time.time()
#     history_entry = {"state": new_state, "timestamp": time.time()}
#     if details_to_add:
#         history_entry["details"] = details_to_add
#         if "details" not in saga_data: saga_data["details"] = {}
#         saga_data["details"].update(details_to_add)
#     if "history" not in saga_data or not isinstance(saga_data["history"], list): saga_data["history"] = []
#     saga_data["history"].append(history_entry)
#     await pipe.set(saga_key, json.dumps(saga_data), ex=SAGA_STATE_TTL)
#     logger.info(f"Queued state update for saga {saga_id} to {new_state}")

def _update_saga_redis_state(saga_id: str, current_saga_bytes: bytes | None, new_state: str, details_to_add: dict | None) -> str:
    """Prepares the JSON string for the updated saga state."""
    saga_data: Dict = {}
    if current_saga_bytes:
        try:
            saga_data = json.loads(current_saga_bytes.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(f"Failed to decode existing saga data for {saga_id}, starting fresh.")
            saga_data = {"saga_id": saga_id, "history": [], "details": {}}
    else:
        # No existing data, start fresh
        saga_data = {"saga_id": saga_id, "history": [], "details": {}}

    saga_data["current_state"] = new_state
    saga_data["last_updated"] = time.time()

    history_entry = {"state": new_state, "timestamp": time.time()}
    if details_to_add:
        history_entry["details"] = details_to_add
        if "details" not in saga_data: saga_data["details"] = {}
        saga_data["details"].update(details_to_add) 

    if "history" not in saga_data or not isinstance(saga_data.get("history"), list):
         saga_data["history"] = []
    saga_data["history"].append(history_entry)
    # --- End Update ---

    return json.dumps(saga_data)

 #From update_saga_and_enqueue
async def enqueue_outbox_message(pipe: redis.asyncio.client.Pipeline, routing_key: str, payload: dict):
        if payload is not None and routing_key is not None:
            message={
                "routing_key":routing_key,
                "payload":payload,
                "timestamp":time.time()
            }
            await pipe.rpush(OUTBOX_KEY,json.dumps(message))
            logger.info(f"[Order Orchestrator] Enqueued message to outbox: {message}")

a


#------NEW WAITING FUNCTION (POLLING)--------

async def wait_for_final_saga_state(saga_id: str, timeout: int = 30000) -> dict:
    start_time = time.time()
    logger.info(f"[wait_for_final_saga_state] Waiting for saga {saga_id} to complete")
    while time.time() - start_time < timeout:
        try:
            saga_data = await get_saga_state(saga_id)
            if saga_data:
                current_state = saga_data.get("current_state")
                logger.debug(f"[wait_for_final_saga_state] Saga {saga_id}, state: {current_state}")
                if current_state in TERMINAL_SAGA_STATES:
                    logger.info(f"[wait_for_final_saga_state] Saga {saga_id} terminal state: {current_state}")
                    # return {"status": "success", "data": saga_data.get("details", {})}
                # e`lse:
                    logger.warning(f"[wait_for_final_saga_state] Saga {saga_id} failed with state: {current_state}")
                    is_success = current_state in {SagaStateEnum.SAGA_COMPLETED, SagaStateEnum.ORDER_FINALIZED}
                    if is_success:
                        return {"status": "success", "data": saga_data.get("details", {})}
                    else:
                        error = saga_data.get("details", {}).get("error", f"Failed: {current_state}")
                        return {"status": "failure", "error": error}
            else: logger.debug(f"[wait_robust] Saga {saga_id} not found yet.")
        except RedisError as e: logger.warning(f"[wait_robust] Redis error polling: {e}")
        except Exception as e:
            logger.exception(f"[wait_robust] Unexpected error polling: {e}")
            return {"status": "error", "error": "Internal polling error"}
        await asyncio.sleep(0.2)
    logger.warning(f"[wait_robust] Timed out waiting for {saga_id}.")
    final_saga_data = await get_saga_state(saga_id)
    if final_saga_data:
        final_state = final_saga_data.get("current_state")
        if final_state in TERMINAL_SAGA_STATES:
            is_success = final_state in {SagaStateEnum.SAGA_COMPLETED, SagaStateEnum.ORDER_FINALIZED}
            logger.info(f"[wait_robust] State {final_state} found after timeout check.")
            if is_success: return {"status": "success", "data": final_saga_data.get("details", {})}
            else: return {"status": "failure", "error": final_saga_data.get("details", {}).get("error", f"Failed: {final_state}")}
    return {"status": "timeout", "error": "Saga processing timed out"}

async def update_saga_state_and_enqueue_multiple(
    saga_id: str, new_state: str,
    messages_to_enqueue: list[tuple[str, dict]] | None = None, 
    details: dict = None, max_attempts: int = 5, backoff_ms: int = 100
):
    saga_key = f"saga:{saga_id}"
    for attempt in range(max_attempts):
        try:
            async with db_master_saga.pipeline(transaction=True) as pipe:
                await pipe.watch(saga_key)
                await _update_saga_redis_state(pipe, saga_id, new_state, details)
                pipe.multi()
                if messages_to_enqueue:
                    for routing_key, payload in messages_to_enqueue:
                        if isinstance(payload, dict):
                             await enqueue_outbox_message(pipe, routing_key, payload) 
                        else:
                             logger.error(f"Invalid payload type for outbox message in saga {saga_id}: {type(payload)}")
                results = await pipe.execute()

                try:
                    await db_master_saga.wait(1, 1000) 
                except Exception as wait_e:
                     logger.warning(f"Redis WAIT command failed after state update for saga {saga_id}: {wait_e}")

                logger.info(f"[atomic_multi_enqueue] Saga {saga_id} updated -> {new_state}, enqueued {len(messages_to_enqueue or [])} msgs.")
                return True # Success

        except WatchError:
             logger.warning(f"[atomic_multi_enqueue] WatchError attempt {attempt+1}/{max_attempts} for {saga_id}. Retrying...")
             if attempt == max_attempts - 1: break
             await asyncio.sleep((backoff_ms / 1000) * (2 ** attempt) * (1 + random.random() * 0.1))
             continue
        except RedisError as e:
             logger.error(f"[atomic_multi_enqueue] Redis error updating saga {saga_id}: {e}")
             return False 
        except Exception as e:
             logger.exception(f"[atomic_multi_enqueue] Unexpected error updating saga {saga_id}: {e}")
             return False 
    logger.critical(f"[atomic_multi_enqueue] Saga update failed definitively for {saga_id} after {max_attempts} attempts.")
    return False

async def update_saga_and_enqueue(
        saga_id:str,
        new_state:str,
        routing_key: str|None,
        payload:dict|None,
        details=None,
        max_attempts: int=5,
        backoff_ms:int=100):

    saga_key = f"saga:{saga_id}"
    # notification_channel = f"saga_completion:{saga_id}" # PubSub notification removed for simplicity in this version
    logger.debug(f"[SAGA UPDATE {saga_id}] Starting update to state '{new_state}' with payload for routing key '{routing_key}'") # Added start log

    for attempt in range(max_attempts):
        logger.debug(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}/{max_attempts} for state '{new_state}'") 
        try:
           async with db_master_saga.pipeline(transaction=True) as pipe:
                logger.debug(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: About to WATCH key '{saga_key}'")
                await pipe.watch(saga_key)
                logger.debug(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: WATCH command issued for key '{saga_key}'")

                update_saga_data = await _update_saga_redis_state(pipe, saga_id, new_state, details)

                logger.debug(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: State update prepared in pipeline. Preparing MULTI.") # Before MULTI
                pipe.multi() # Start MULTI block - commands after this are queued

                await _update_saga_redis_state(pipe, saga_id, new_state, details) # Re-queue SET

                if payload is not None and routing_key is not None:
                    logger.debug(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: Queueing outbox message for '{routing_key}'")
                    await enqueue_outbox_message(pipe, routing_key, payload)
                else:
                     logger.debug(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: No outbox message to queue.")

                logger.debug(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: About to EXEC pipeline.") # Before EXEC
                results = await pipe.execute()
                logger.debug(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: EXEC successful. Results: {results}") # After EXEC success

                #     logger.warning(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: Redis WAIT command failed/timed out: {wait_e}")

                logger.info(f"[update_saga_and_enqueue] Saga {saga_id} updated -> {new_state} on attempt -> {attempt+1}")
                return True # Success
        except WatchError:
            current_time_ms = int(time.time() * 1000)
            logger.warning(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1} FAILED: WatchError at {current_time_ms}. Key '{saga_key}' likely modified concurrently.")
            if attempt == max_attempts - 1:
                logger.error(f"[SAGA UPDATE {saga_id}] WatchError persisted after {max_attempts} attempts. Aborting update.")
                break 
            # Backoff logic
            backoff_duration = (backoff_ms / 1000) * (2 ** attempt) * (1 + random.random() * 0.1)
            logger.info(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: WatchError - backing off for {backoff_duration:.3f}s before retry.")
            await asyncio.sleep(backoff_duration)
            continue 
        except RedisError as e:
            logger.error(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: Redis error during transaction: {e}", exc_info=True)
            break 
        except Exception as e:
            logger.error(f"[SAGA UPDATE {saga_id}] Attempt {attempt+1}: Unexpected error during transaction: {e}", exc_info=True)
            break 

    logger.critical(f"[update_saga_and_enqueue] Failed to update saga {saga_id} to {new_state} after {max_attempts} attempts.")
    return False

#-------CORE ORDER UTILITIES AND NECESSITIES--------
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
            order_entry = msgpack.decode(entry, type=OrderValue)
            print(f"--- ORDER: get_order_from_db - Deserialized Order Type={type(order_entry)}, Value={order_entry} ---")
            return order_entry, 200
        except MsgspecDecodeError as e:
            logger.error(f"Failed to decode msgpack {order_id} from the database:{str(e)}")
            return {"error": f"Failed to decode msgpack {order_id} from the database:{str(e)}"}, 500
    except redis.RedisError as e:
        logger.error(f"Database Error while getting order: {order_id} from the database:{str(e)}")
        return {"error": DB_ERROR_STR}, 500
    # deserialize data if it exists else return null
    except Exception as e:
        logger.error(f"Error while getting order: {order_id} from the database:{str(e)}")
        return {"error": DB_ERROR_STR}, 500

  #update and save order
#made async TODO
async def atomic_update_order(order_id, update_func):
    max_retries = 5
    base_backoff = 50
    for attempt in range(max_retries):
        try:
            async with db_master.pipeline(transaction=True) as pipe:
                key = f"{order_id}"
                await pipe.watch(order_id)
                entry= await pipe.get(order_id)
                if not entry:
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
                pipe.set(key, msgpack.encode(updated_order))
                await pipe.execute()
                await db_master.wait(1,1000)
                return updated_order, None 
        except redis.WatchError:
            # key was modified between watch and execute, retry backoff
            backoff_multiplier = (2 ** attempt) * (1 + random.random() * 0.1)
            backoff = base_backoff * backoff_multiplier
            await asyncio.sleep(backoff / 1000)
            continue  # loop again

        except redis.RedisError:
            return None, f"Redis error updating order {order_id}"
        except Exception as e:
            logging.exception(f"Unexpected error in atomic_update_order for order {order_id}: {e}")
            return None, "Internal data error"

#------SAGA ACTIONS--------

async def action_initiate_payment(saga_id: str, saga_details: dict):
    user_id = saga_details.get("user_id"); total_cost = saga_details.get("total_cost")
    if user_id is None or total_cost is None: return  
    key = f"saga:{saga_id}:step:payment"
    payload = {"user_id": user_id, "amount": total_cost, "saga_id": saga_id, "idempotency_key": key}
    await update_saga_and_enqueue(
        saga_id, 
        SagaStateEnum.PAYMENT_PENDING,
        "payment_queue", 
        payload,
        {"payment_idem_key": key})
        

async def action_initiate_stock_reservation(saga_id: str, saga_details: dict):
    items = saga_details.get("items", [])
    if not items: return 
    pending_items = {item_id: qty for item_id, qty in items}
    next_item_id, next_quantity = list(pending_items.items())[0]
    key = f"saga:{saga_id}:step:stock:{next_item_id}"
    payload = {"item_id": next_item_id, "amount": next_quantity, "saga_id": saga_id, "idempotency_key": key}
    state_details = {"reserving_item": next_item_id, "pending_items": pending_items}
    await update_saga_and_enqueue(
        saga_id, 
        SagaStateEnum.STOCK_PENDING,
        "stock_queue", 
        payload,
        state_details)
    

async def action_initiate_payment_compensation(saga_id: str, saga_details: dict):
    user_id = saga_details.get("user_id"); amount = saga_details.get("total_cost")
    if user_id is None or amount is None: return
    comp_key = f"saga:{saga_id}:compensate:payment"
    compensation_payload = {"user_id": user_id, "amount": amount, "saga_id": saga_id, "idempotency_key": comp_key}
    await update_saga_and_enqueue(
        saga_id, SagaStateEnum.COMPENSATING_PAYMENT, "payment_queue", compensation_payload, {"action": "compensate_payment"}
    )

# async def action_initiate_stock_compensation(saga_id: str, saga_details: dict):
#     items_id = saga_details.get("item_id"); amount = saga_details.get("amount")
#     if items_id is None or amount is None: return
#     compensation_payload = {"item_id": items_id, "amount": amount}
    
#     await update_saga_and_enqueue(
#         saga_id, SagaStateEnum.COMPENSATING_STOCK, "stock_queue", compensation_payload, {"action": "compensate_stock"}
#     )

#instead of function above 
def prepare_stock_compensation_messages(saga_id: str, saga_details: dict) -> list[tuple[str, dict]]:
    messages = []
    successful_items = saga_details.get("successful_stock_items", {}) 

    if successful_items:
        logger.info(f"[Prepare Stock Comp] Preparing compensation for items: {list(successful_items.keys())} in saga {saga_id}")
        for item_id, quantity in successful_items.items():
            if quantity is None or not isinstance(quantity, int) or quantity <= 0:
                 logger.warning(f"Skipping compensation for item {item_id} in saga {saga_id} due to invalid quantity: {quantity}")
                 continue
            comp_key = f"saga:{saga_id}:compensate:stock:{item_id}"
            comp_payload = {
                "item_id": item_id,
                "amount": quantity, 
                "saga_id": saga_id,
                "idempotency_key": comp_key
            }
            messages.append(("stock_queue", comp_payload))
    else:
        logger.info(f"[Prepare Stock Comp] No successful stock items found to compensate for saga {saga_id}")

    return messages


# async def cleanup_pubsub_resource():
#     logger.info("Cleaning up Redis PubSub resources.")
#     for saga_id, task in listener_tasks.items():
#         if not task.done():
#             task.cancel()
#     for saga_id, pubsub in pubsub_clients.items():
#         await pubsub.close()
#     saga_futures.clear()
#     pubsub_clients.clear()
#     listener_tasks.clear()




# async def start_pubsub_listener(saga_id: str)->asyncio.Task:
#     notification_channel=f"saga_completion:{saga_id}"
#     pubsub = db_master_saga.pubsub()
#     pubsub_clients[saga_id] = pubsub

#     async def listen_for_completion():
#         try:
#             await pubsub.subscribe(notification_channel)
#             logger.info(f"[Pubsub] Subscribed to {notification_channel}")
#             while not saga_futures[saga_id].done():
#                 try:
#                   message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
#                   if message and message['type'] == 'message':
#                         data = json.loads(message['data']) if isinstance(message['data'], str) else message['data']
#                         logger.info(f"[PubSub] Received notification for saga {saga_id}: {data}")
#                         completion_data = json.loads(data) if isinstance(data, str) else data
#                         if saga_id in saga_futures and not saga_futures[saga_id].done():
#                                if completion_data.get("success", False):
#                                    saga_futures[saga_id].set_result({"success": True})
#                                else:
#                                  error_msg = completion_data.get("error", "Unknown error")
#                                  saga_futures[saga_id].set_result({"success": False, "error": error_msg})
#                         break
#                   current_saga=await get_saga_state(saga_id)
#                   if current_saga:
#                       status = current_saga.get("status")
#                       if status in["SAGA_COMPLETED", "ORDER_FINALIZED"]:
#                           if saga_id in saga_futures and not saga_futures[saga_id].done():
#                               saga_futures[saga_id].set_result({"success": True})
#                               break
#                       elif status in ["SAGA_FAILED", "SAGA_FAILED_INITIALIZATION",
#                                       "SAGA_FAILED_RECOVERY", "ORDER_FINALIZATION_FAILED","SAGA_FAILED_ORDER_NOT_FOUND"]:
#                           error_details=current_saga.get("details",{}).get("error","Unknown saga failure")
#                           if saga_id in saga_futures and not saga_futures[saga_id].done():
#                               saga_futures[saga_id].set_result({"success": False, "error": error_details})
#                               break
#                   await asyncio.sleep(0.1)
#                 except asyncio.TimeoutError:
#                     continue
#                 except Exception as e:
#                     logger.error(f"[PubSub] Error while processing messages: {e}")
#                     await asyncio.sleep(0.5)
#         except Exception as e:
#             logger.error(f"[PubSub] Listener error for {saga_id}: {e}")
#             if saga_id in saga_futures and not saga_futures[saga_id].done():
#                 saga_futures[saga_id].set_exception(e)
#         finally:
#             try:
#                 await pubsub.unsubscribe(notification_channel)
#                 logger.info(f"[PubSub] Unsubscribed from {notification_channel}")
#                 if saga_id in pubsub_clients:
#                     await pubsub_clients[saga_id].close()
#                     del pubsub_clients[saga_id]
#                 if saga_id in listener_tasks:
#                     del listener_tasks[saga_id]
#             except Exception as e:
#                 logger.error(f"[PubSub] Cleanup error for {saga_id}: {e}")
#     task=asyncio.create_task(listen_for_completion())
#     listener_tasks[saga_id] = task
#     return task

# async def handle_final_saga_state(saga_id, request_idempotency_key, final_status, result_payload, status_code):
#     logger.info(f"[Order Orchestrator] Handling final saga state for saga {saga_id}")

#     # verify tests assumes immediate correct response instead of partial 202
#     # same for test microservices when it gives 'error' input and therefore fails
#     # so this has to be made sync
#     # could potentially lock-> wait check until final status= COMPLETED-> release lock
#     # pre-checks?? however would not guarantee consistent state return, but is faster
#     # adjust test? elaborate ? or show two version for performance difference
#     # would be nice, but time constraint
#     saga_data=await get_saga_state(saga_id)
#     if saga_data:
#         current_status=saga_data.get("status")
#         if current_status=="SAGA_COMPLETED":
#             logger.info(f"[Order Orchestrator] Saga {saga_id} already completed successfully")
#             return result_payload, status_code
#         elif current_status in ["SAGA_FAILED","SAGA_FAILED_INITIALIZATION","SAGA_FAILED_ORDER_NOT_FOUND",
#                                 "SAGA_FAILED_RECOVERY","ORDER_FINALIZATION_FAILED","STOCK_RESERVATION_FAILED"]:
#             error_details=saga_data.get("details",{}).get("error","Unknown saga failure")
#             return {"error":error_details},400
#     saga_futures[saga_id]=asyncio.Future()
#     await start_pubsub_listener(saga_id)
#     try:
#         await asyncio.wait_for(saga_futures[saga_id],timeout=2)
#         saga_result=saga_futures[saga_id].result()
#         logger.info(f"[Order Orchestrator] Saga {saga_id} completed with result: {saga_result}")
#         if saga_result.get("success",False):
#             return result_payload, status_code

#         else:
#             return {"error": saga_result.get("error", "Unknown saga failure")}, 400
#     except asyncio.TimeoutError:
#         logger.warning(f"[Order Orchestrator] Timed out waiting for saga {saga_id}")
#         latest_saga_data=await get_saga_state(saga_id)
#         order_id=None
#         if result_payload and 'order_id' in result_payload:
#             order_id=result_payload['order_id']
#         elif latest_saga_data and 'details' in latest_saga_data and'order_id' in latest_saga_data['details']:
#             order_id=latest_saga_data['details']['order_id']
#         if latest_saga_data:
#             latest_status = latest_saga_data.get("status")
#             logger.info(f"Latest saga state for saga {saga_id}: {latest_status}")
#             if latest_status in ["SAGA_FAILED", "SAGA_FAILED_INITIALIZATION", "SAGA_FAILED_ORDER_NOT_FOUND",
#                                  "SAGA_FAILED_RECOVERY", "ORDER_FINALIZATION_FAILED", "STOCK_RESERVATION_FAILED"]:
#                 error_details = latest_saga_data.get("details", {}).get("error", "Unknown saga failure")
#                 response={"error":error_details}
#                 if order_id:
#                     response["order_id"] = order_id
#                 return response, 400

#             # If we can't determine failure, return a timeout error
#         response= {"warning": "Operation is taking longer than expected. Please check the status later."}
#         if order_id:
#             response["order_id"] = order_id
#         return response,200
#     finally:
#         # Clean up resources
#         if saga_id in saga_futures:
#             del saga_futures[saga_id]

#         if saga_id in listener_tasks and not listener_tasks[saga_id].done():
#             listener_tasks[saga_id].cancel()





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









# Initialize RpcClient for the order service
rpc_client = RpcClient()


async def connect_rpc_client():
    # Connect the RpcClient to the AMQP server
    await rpc_client.connect(os.environ["AMQP_URL"])

#-------AMQP WORKER--------

@worker.register
@idempotent('create_order', idempotency_redis_client, SERVICE_NAME)
async def create_order(data, message):
    user_id = data.get("user_id")
    if not user_id or not isinstance(user_id, str):
        logger.error(f"create_order called without a valid string user_id. Data received: {data}")
        return {"error": "Missing or invalid user_id provided"}, 400
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        logger.info(f"[Create Order] Attempting to set order {key} for user {user_id}")
        await db_master.set(key, value)
        await update_saga_and_enqueue(saga_id=key, new_state="INITIATED", routing_key=None, payload=None)
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
@idempotent("add_item",idempotency_redis_client,SERVICE_NAME)
async def add_item(data, message):
    order_id = data.get("order_id")
    item_id = data.get("item_id")
    quantity = data.get("quantity")
    stock_response = None

    if not order_id or not item_id or not quantity:
        return {"error": "Missing required fields (order_id,item_id, quantity)"}, 400
    payload = {"item_id": item_id}
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
                new_state=SagaStateEnum.ORDER_MODIFIED,
                routing_key=None,
                payload=None,
                details={"item_id": item_id, "quantity": quantity}
            )
            return {"data": {"added": True, "order_id": order_id, "new_total_cost": updated_order.total_cost}}, 200

        else:
            return ({"added": False, "error": "Internal processing error"}, 500)
    except ValueError as e:
        error_msg=str(e)
        return ({"added": False, "error": error_msg}, 400)
    except Exception as e:
        logging.exception("internal error")
        return ({"added": False, "error": error_msg}, 400)

@worker.register
@idempotent('process_checkout_request', idempotency_redis_client, SERVICE_NAME)
async def process_checkout_request(data, message):
    """
    Receives 'order.checkout' events to begin the saga.
    Tries to reserve stock first, then waits for success/failure events.
    """
    # ----- INITIATION PHASE -----
    order_id = data.get('order_id')
    if not order_id: return {"error": "Missing order_id"}, 400
    saga_id = order_id
    logger.info(f"[Checkout] Received checkout request for order {order_id}. Saga ID: {saga_id}")
    initiate_saga = False
    saga_details = {}

    try:
        existing_saga_data = await get_saga_state(saga_id)
        if existing_saga_data:
            state = existing_saga_data.get("current_state")
            saga_details = existing_saga_data.get("details", {})
            logger.info(f"[Checkout] Found existing saga {saga_id} in state: {state}")

            if state in TERMINAL_SAGA_STATES:
                logger.warning(f"[Checkout] Saga {saga_id} already terminal ({state}).")
                success = state in {SagaStateEnum.SAGA_COMPLETED, SagaStateEnum.ORDER_FINALIZED}
                error = existing_saga_data.get("details", {}).get("error", "Previously failed")
                return ({"status": "success", "message": "Previously completed"} if success else {"error": error}), (200 if success else 400)
            else:
                logger.info(f"[Checkout] Saga {saga_id} in progress ({state}). Waiting...")
                saga_details = existing_saga_data.get("details", {}) 
        
        #SAGA does not exist yet
        # else:
        else:
            logger.info(f"[Checkout] Initiating saga {saga_id}...")
            
            
            order_result, status_code = await get_order_from_db(order_id)
            if status_code != 200:
                logger.error(f"Failed to retrieve order {order_id} from DB for checkout. Status code: {status_code}")
                return order_result if isinstance(order_result, dict) else {
                    "error": f"Failed to retrieve order {order_id} from DB for checkout."}, status_code

            # Extract order details, now orderdata=saga_details
            saga_details = {
                "order_id": order_id,
                "paid": order_result.paid,
                "items": order_result.items,
                "user_id": order_result.user_id,
                "total_cost": order_result.total_cost
            }
            
            lock_value = await acquire_write_lock(order_id)
            if lock_value is None: return {"error": "Order locked, try again later"}, 409 
            saga_details["lock_value"] = lock_value
            
            #SAGA create initiate to STARTED
            started = await update_saga_and_enqueue(saga_id, SagaStateEnum.STARTED, details=saga_details, max_attempts=5, routing_key=None, payload=None)
            if not started:
                if lock_value: await release_write_lock(order_id, lock_value) 
                return {"error": "Failed initialization"}, 500
            initiate_saga = True

    except Exception as init_err:
        logger.exception(f"[Checkout] Initiation error {saga_id}: {init_err}")
        return {"error": "Internal initiation error"}, 500

    #SAGA initate payment
    if initiate_saga:
        try:
            logger.info(f"[Checkout] Triggering async payment for {saga_id}")
            asyncio.create_task(action_initiate_payment(saga_id, saga_details))
        except Exception as task_err:
            logger.exception(f"[Checkout] Error creating task {saga_id}: {task_err}")
            await update_saga_and_enqueue(saga_id, SagaStateEnum.FAILED, details={"error": "Task creation failed"})
            if saga_details.get("lock_value"): await release_write_lock(order_id, saga_details["lock_value"]) # Release lock
            return {"error": "Failed background start"}, 500

    # --- Wait and Respond ---
    logger.info(f"[Checkout] Waiting for final status saga {saga_id}...")
    final_result = await wait_for_final_saga_state(saga_id, timeout=30)

    status_map = {"success": 200,
                  "failure": 400, 
                  "timeout": 504, 
                    "error": 500}
    resp_status = status_map.get(final_result["status"], 500)
    resp_body = final_result.get("data") if final_result["status"] == "success" else {"error": final_result.get("error")}

    # Release lock if saga failed/timed out addddnd we still hold it, kinda best effort :)
    if resp_status >= 400:
         current_saga_details = (await get_saga_state(saga_id) or {}).get("details", {})
         lock_val = current_saga_details.get("lock_value")
         if lock_val:
             released = await release_write_lock(order_id, lock_val)
             if released: logger.info(f"Released lock for failed/timed-out saga {saga_id}")
             else: logger.warning(f"Could not release lock for failed/timed-out saga {saga_id}")


    return resp_body, resp_status

#_----------- WORKER REPLY ----------

@orchestrator_reply_worker.register
async def handle_stock_reply(data:dict, message):
    """
          Receives 'stock.reservation_completed' event after Stock Service reserves items.
          Then we proceed to request payment.
          """
    saga_id = data.get('saga_id')
    item_id = data.get('item_id')
    
    if not saga_id or not item_id:
        logger.error(f"no saga_id or item_id in process_stock_completed")
        await message.ack()
        return
    
    logger.info(f"[Order Orchestrator] Received stock reply for saga={saga_id}, item={item_id}")
    try:
        saga_data = await get_saga_state(saga_id)
        if not saga_data:
            logger.warning(f"Saga {saga_id} not found")
            await message.ack()
            return
        
        current_state = saga_data.get("current_state")
        saga_details = saga_data.get("details", {})
        reserving_item = saga_details.get("reserving_item")

        #Check Idempotency
        if current_state != SagaStateEnum.STOCK_PENDING:
            logger.warning(f"Ignored stock reply for saga {saga_id} with state {current_state}")
            await message.ack()
            return
        if reserving_item != item_id:
            logger.warning(f"Ignored stock reply for saga {saga_id} with item {item_id}")
            await message.ack()
            return
        
        #Process reply
        stock_success = data.get("status_code") == 200 and "error" not in data
        pending_items = saga_details.get("pending_items", {})
        quantity_processed = pending_items.get(item_id, None) #
        if quantity_processed is None:
             logger.error(f"Could not determine quantity for successful item {item_id} in saga {saga_id}")

        if stock_success and quantity_processed is not None:
            logger.info(f"Stock step successful for item {item_id}, quantity {quantity_processed}, saga {saga_id}.")

            successful_items = saga_details.get("successful_stock_items", {}) 
            successful_items[item_id] = quantity_processed

            details_update_success = await update_saga_and_enqueue(
                saga_id=saga_id,
                new_state=SagaStateEnum.STOCK_PENDING, 
                details={"successful_stock_items": successful_items} 
            )
            if not details_update_success:
                 logger.critical(f"CRITICAL: Failed to record successful stock item {item_id} for saga {saga_id}. Manual review needed.")
                 await update_saga_and_enqueue(saga_id,
                                                SagaStateEnum.FAILED,
                                                details={"error": "Internal state error recording stock success"})
                 await message.nack(requeue=False); return # Nack to DLQ

            saga_details["successful_stock_items"] = successful_items

            if item_id in pending_items:
                del pending_items[item_id] 

            if not pending_items:
                logger.info(f"All stock steps completed for saga {saga_id}. Finalizing order.")
                saga_updated_stock = await update_saga_and_enqueue(
                    saga_id, SagaStateEnum.STOCK_COMPLETED,
                    details={"successful_stock_items": successful_items,
                             "all_stock_reserved": True}
                )

                if not saga_updated_stock:
                    logger.error(f"[handle_stock_reply] Failed to update saga {saga_id} to {SagaStateEnum.STOCK_COMPLETED}")
                    await message.nack(requeue=False); return
                
                order_id = saga_details.get("order_id")
                lock_value = saga_details.get("lock_value")
                try:
                    _ , update_err = await atomic_update_order(order_id, lambda o: setattr(o, 'paid', True) or o)
                    if update_err:
                        raise Exception(f"Failed to mark order {order_id} as paid: {update_err}")
                    
                    if lock_value:
                        released = await release_write_lock(order_id, lock_value)
                        if not released:
                            logger.warning(f"Failed to release lock for order {order_id} with lock value {lock_value}")
                            #not sure what to do here? after lock failure -> fail saga?
                        await update_saga_and_enqueue(
                            saga_id=saga_id,
                            new_state=SagaStateEnum.ORDER_LOCK_RELEASED if released else SagaStateEnum.ORDER_LOCK_RELEASE_FAILED
                        )
                    else:
                        logger.warning(f"Failed to release lock for order {order_id} with value {lock_value}")
                    
                    #SAGA COMPLETED
                    logger.info(f"[Order Orchestrator] Saga {saga_id} completed")
                    await update_saga_and_enqueue(
                        saga_id=saga_id,
                        new_state=SagaStateEnum.SAGA_COMPLETED,
                    )
                except Exception as finalization_error:
                    logger.exception("[handle_stock_reply] Order finalization failed for saga=%s, order=%s: %s",
                                    saga_id, order_id, finalization_error, exc_info=True)
                    await update_saga_and_enqueue(
                        saga_id=saga_id,
                        new_state=SagaStateEnum.ORDER_FINALIZATION_FAILED, 
                        details={"error": f"Order finalization failed after successful steps: {str(finalization_error)}"},
                        routing_key=None, 
                        payload=None
                    )
                    logger.warning(f"[handle_stock_reply] Saga {saga_id} moved to {SagaStateEnum.ORDER_FINALIZATION_FAILED}. Compensation NOT triggered as core steps succeeded.")

            # MORE ITEMS PENDING, MOST LIKELY STOCK
            else:
                logger.info(f"[Order Orchestrator] More items pending for saga={saga_id}, order={order_id}")
                next_item_id, next_quantity = list(pending_items.items())[0]
                next_key = f"{saga_id}:stock:{next_item_id}"
                next_payload = {
                    "item_id": next_item_id,
                    "amount": next_quantity,
                    "saga_id": saga_id,
                    "idempotency_key": next_key
                }
                next_details = {"reserving_item": next_item_id, "pending_items": pending_items}
                await update_saga_and_enqueue(
                    saga_id=saga_id,
                    new_state=SagaStateEnum.STOCK_PENDING,
                    routing_key="stock_queue",
                    details=next_details,
                    payload=next_payload
                )

        #STOCK STEP FAILED
        else:
            error_msg = data.get("error", f"Stock failed for item {item_id}")
            logger.warning(f"[Order Orchestrator] Stock step failed for saga={saga_id}, order={order_id}")
            await update_saga_and_enqueue(
                saga_id=saga_id,
                new_state=SagaStateEnum.STOCK_FAILED,
                details={"error": error_msg,
                         "failed_item": item_id},
                routing_key=None,
                payload=None
            )
            #if potenially payment need
            # asyncio.create_task(action_initiate_payment_compensation(saga_id, saga_details))
            # successful_items_to_compensate = saga_details.get("successful_stock_items", {})
            # if successful_items_to_compensate: 
            #     logger.info(f"Triggering stock compensation for items {list(successful_items_to_compensate.keys())}...")
            #     asyncio.create_task(action_initiate_stock_compensation(saga_id, saga_details, successful_items_to_compensate))
            # else:
            #     logger.info("No successful stock items found to compensate.")
            await update_saga_and_enqueue(
                saga_id=saga_id,
                new_state=SagaStateEnum.STOCK_FAILED,
                details={"error": error_msg},
            )
            #time to releas lock
            lock_value = saga_details.get("lock_value")
            order_id = saga_details.get("order_id")
            if lock_value and order_id:
                await release_write_lock(order_id, lock_value)
        
        await message.ack()
    
    except Exception as e:
        logger.exception(f"Unexpected error in stock reply: {str(e)} saga_id: {saga_id}")
        try:
            await message.nack(requeue=True)
        except Exception as e:
            logger.error(f"Error nacking message: {str(e)}", exc_info=True)


@orchestrator_reply_worker.register
async def handle_payment_reply(data:dict, message):
    """
          Receives 'payment.reservation_completed' event after payment Service reserves items.
          Then we proceed to request payment.
          """
    saga_id = data.get('saga_id')
    
    if not saga_id:
        logger.error(f"no saga_id or item_id in process_payment_completed")
        await message.ack()
        return
    
    logger.info(f"[Order Orchestrator] Received payment reply for saga={saga_id} data={data}")
    try:
        saga_data = await get_saga_state(saga_id)
        if not saga_data:
            logger.warning(f"Saga {saga_id} not found")
            await message.ack()
            return
        
        current_state = saga_data.get("current_state")
        saga_details = saga_data.get("details", {})

        #Check Idempotency
        if current_state != SagaStateEnum.PAYMENT_PENDING:
            logger.warning(f"Ignored stock reply for saga {saga_id} with state {current_state}")
            await message.ack()
            return
        
        #Process reply
        status_code = data.get("status_code")
        if status_code== 200 and "error" not in data:
            payment_success = status_code

        if payment_success:
            logger.info(f"[Order Orchestrator] payment step succeeded for saga={saga_id}")

            state_updated = await update_saga_and_enqueue(
                saga_id=saga_id,
                new_state=SagaStateEnum.PAYMENT_COMPLETED,
                details={"payment_reply": data.get("data")},
            )
            if state_updated:
                asyncio.create_task(action_initiate_stock_reservation(saga_id, saga_details))
            else:
                logger.critical(f"FAILED critical failed to update saga {saga_id} to {SagaStateEnum.PAYMENT_COMPLETED}")
                await update_saga_and_enqueue(
                    saga_id=saga_id,
                    new_state=SagaStateEnum.SAGA_FAILED,
                    details={"error": "Internal error"},
                )
        #payment failed
        else:
            error_msg = data.get("error", f"Payment failed by service")
            logger.warning(f"[Order Orchestrator] Payment step failed for saga={saga_id}: {error_msg}")
            await update_saga_and_enqueue(
                saga_id=saga_id,
                new_state=SagaStateEnum.PAYMENT_FAILED,
                details={"error": error_msg,"payment_reply": data},
            )
            await update_saga_and_enqueue(
                saga_id=saga_id,
                new_state=SagaStateEnum.SAGA_FAILED,
                details={"error": f"SAGA FAILED: Payment failed for saga {saga_id}"},
            )

        await message.ack()
    except:
        logger.exception(f"Unexpected error in payment reply: {str(e)} saga_id: {saga_id}")
        try:
            await message.nack(requeue=True) #dlq-->
        except Exception as e:
            logger.error(f"Error nacking message: {str(e)}", exc_info=True)
    

@orchestrator_reply_worker.register
async def process_failure_events(data, message):
    """
    Any service can publish a failure event: e.g. 'stock.reservation_failed' or 'payment.failed'.
    The orchestrator listens, updates the saga state, and triggers compensation if needed.
    """
    
    saga_id = data.get("saga_id")
    order_id = data.get("order_id")
    error_msg = data.get("error", "Unknown failure event received")
    failure_source = getattr(message, 'routing_key', "UNKNOWN_FAILURE_SOURCE")

    if not saga_id:
        logger.error(f"Failure event received without saga_id: {data}")
        await message.ack(); return

    logger.warning(f"Processing failure event for saga {saga_id} from {failure_source}: {error_msg}")

    try:
        saga_data = await get_saga_state(saga_id)
        if not saga_data:
            logger.warning(f"Saga {saga_id} not found for failure event. Ignoring.")
            await message.ack(); return

        current_state = saga_data.get("current_state") 
        saga_details = saga_data.get("details", {})

        # Idempotency  
        if current_state in TERMINAL_SAGA_STATES or "COMPENSATING" in current_state:
            logger.warning(f"Saga {saga_id} already terminal/compensating ({current_state}). Ignoring failure event.")
            await message.ack(); return

        # Check which steps were completed
        # steps_completed = [step["status"] for step in saga_data.get("steps", [])]
        compensations = [] #to enqueue
        saga_history = saga_data.get("history", [])

        #did payment complete?
        payment_completed = any(step.get("state") == SagaStateEnum.PAYMENT_COMPLETED for step in saga_history)

        # {item_id: quantity}
        successful_stock_items = saga_details.get("successful_stock_items", {}) 

        # compensating payment 
        if payment_completed:
            user_id = saga_details.get("user_id")
            total_cost = saga_details.get("total_cost")
            if user_id and total_cost is not None:
                comp_key = f"saga:{saga_id}:compensate:payment" 
                comp_payload = {
                    "user_id": user_id, "amount": total_cost, "saga_id": saga_id,
                    "idempotency_key": comp_key
                }
                compensations.append(("payment_queue", comp_payload))
                logger.info(f"Queueing payment compensation for saga {saga_id}")
            else:
                logger.error(f"Cannot compensate payment for saga {saga_id}: missing user_id or total_cost.")
                
                # potentially worrisome

        # compensate stock 
        # if successful_stock_items:
        #     logger.info(f"Queueing stock compensation for items: {list(successful_stock_items.keys())} in saga {saga_id}")
        #     for item_id, quantity in successful_stock_items.items():
        #         comp_key = f"saga:{saga_id}:compensate:stock:{item_id}"
        #         comp_payload = {
        #             "item_id": item_id, "amount": quantity, "saga_id": saga_id,
        #             "idempotency_key": comp_key
        #         }
        #         compensations.append(("stock_queue", comp_payload))
        # elif any(s["state"] == SagaStateEnum.STOCK_COMPLETED for s in saga_history):
        #     logger.warning(f"Stock compensation needed for {saga_id}, but specific items not tracked. Cannot compensate precisely.")
        stock_compensations = prepare_stock_compensation_messages(saga_id, saga_details)
        compensations.extend(stock_compensations)

        failure_details = {
            "error": f"Failure reported by {failure_source}: {error_msg}",
            "original_failure_data": data,
            "compensations_triggered": [c[0] for c in compensations]
        }

        logger.info(f"Updating saga {saga_id} to FAILED and enqueueing {len(compensations)} compensations.")
        success = await update_saga_and_enqueue_multiple(
            saga_id=saga_id,
            new_state=SagaStateEnum.SAGA_FAILED,
            messages_to_enqueue=compensations,
            details=failure_details
        )

        if not success:
            logger.critical(f"CRITICAL: Failed atomic update/enqueue during failure processing for saga {saga_id}.")
            await message.nack(requeue=False); return

        lock_value = saga_details.get("lock_value")
        current_order_id = saga_details.get("order_id", order_id) 
        if lock_value and current_order_id:
            released = await release_write_lock(current_order_id, lock_value)
            if released: logger.info(f"Released lock for failed saga {saga_id}")
            else: logger.warning(f"Could not release lock for failed saga {saga_id}")

        await message.ack()

    except Exception as e:
        logger.exception(f"Unexpected error processing failure event for saga {saga_id}: {e}")
        try: await message.nack(requeue=False) # DLQ
        except Exception as nack_err: logger.error(f"Failed to NACK failure event message for {saga_id}: {nack_err}")

  
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
            await db_master.expire(f"lock_meta:{lock_key}", int(lock_timeout / 1000) + 10)
            return lock_value
    
        else:
            logger.info(f"Lock already acquired for order: {order_id}")
            return None
    except Exception as e:
        logger.error(f"Error while acquiring lock for order: {order_id}: {str(e)}")
        return None


async def release_write_lock(order_id: str, lock_value: str) -> bool:
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
        result = await db_master.eval(lua_script, 2, lock_key, meta_key,lock_value)
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

async def publish_event(routing_key, payload,message_id=None):
    connection=None
    try:
        connection = await connect_robust(AMQP_URL)
        channel = await connection.channel()

        exchange = await channel.declare_exchange("saga_events", "topic", durable=True)

        headers = {}
        if payload.get("callback_action"):
            headers["callback_action"] = payload.pop("callback_action")

        reply_to = None
        if payload.get("reply_to"):
            reply_to = payload.pop("reply_to")

        message = Message(
            body=json.dumps(payload).encode(),
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message_id or str(uuid.uuid4()),
            headers=headers,
            reply_to=reply_to
        )
        await exchange.publish(message, routing_key=routing_key)
        logger.info(f"[Order Orchestrator] Published event {routing_key} => {payload}")

        await connection.close()
        return True
    except Exception as e:
        logger.error(f"Error publishing event: {str(e)}", exc_info=True)
        if connection:
            try:
                await connection.close()
            except:
                pass
        return False


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
            is_leader = await db_master_saga.set(
                "leader:order-orchestrator",
                ORCHESTRATOR_ID,
                nx=True,
                ex=30  
            )
            if is_leader:
                logger.info("[Order Orchestrator] Acquired leadership.")
            else:
                if await db_master_saga.get("leader:order-orchestrator") == ORCHESTRATOR_ID:
                    await db_master_saga.expire("leader:order-orchestrator", 30)

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
    logger.info("[Recovery] Starting check for stuck sagas...")
    processed_count = 0
    failed_recovery_count = 0
    try:
        cursor = '0'
        while True:
            cursor, keys = await db_master_saga.scan(cursor=cursor, match='saga:*', count=100)
            logger.debug(f"[Recovery] Scanned keys: {len(keys)}")
            for saga_key_bytes in keys:
                saga_key = saga_key_bytes.decode('utf-8')
                saga_id = saga_key.split(':')[-1]

                try:
                    saga_data = await get_saga_state(saga_id) 
                    if not saga_data:
                        logger.warning(f"[Recovery] Saga data disappeared for key {saga_key}. Skipping.")
                        continue

                    current_state = saga_data.get("current_state")
                    last_updated = saga_data.get("last_updated") 

                    if current_state not in TERMINAL_SAGA_STATES and last_updated:
                        if time.time() - last_updated > 60:
                            logger.warning(f"[Recovery] Found stuck saga {saga_id} in state {current_state}. Last updated: {last_updated}. Forcing failure.")
                            processed_count += 1
                            saga_details = saga_data.get("details", {})
                            order_id = saga_details.get("order_id")

                            compensations_to_enqueue = []
                            saga_history = saga_data.get("history", [])

                            payment_completed = any(step.get("state") == SagaStateEnum.PAYMENT_COMPLETED for step in saga_history)
                            if payment_completed:
                                user_id = saga_details.get("user_id")
                                total_cost = saga_details.get("total_cost")
                                if user_id and total_cost is not None:
                                    comp_key = f"saga:{saga_id}:compensate:payment:recovery" 
                                    comp_payload = {"user_id": user_id, "amount": total_cost, "saga_id": saga_id, "idempotency_key": comp_key}
                                    compensations_to_enqueue.append(("payment_queue", comp_payload)) 
                                else: logger.error(f"[Recovery] Cannot prepare payment compensation for {saga_id}: missing details.")

                            stock_compensations = prepare_stock_compensation_messages(saga_id, saga_details)
                            compensations_to_enqueue.extend(stock_compensations)

                            failure_details = {
                                "error": f"Saga failed due to timeout recovery (stuck in state {current_state})",
                                "original_state": current_state,
                                "compensations_triggered": [c[0] for c in compensations_to_enqueue]
                            }
                            logger.info(f"[Recovery] Updating saga {saga_id} to FAILED and enqueueing {len(compensations_to_enqueue)} compensations.")
                            success = await update_saga_state_and_enqueue_multiple(
                                saga_id=saga_id,
                                new_state=SagaStateEnum.SAGA_FAILED, 
                                messages_to_enqueue=compensations_to_enqueue,
                                details=failure_details
                            )

                            if not success:
                                failed_recovery_count += 1
                                logger.critical(f"[Recovery] CRITICAL: Failed atomic update/enqueue during recovery for stuck saga {saga_id}.")
                            else:
                                lock_value = saga_details.get("lock_value")
                                if lock_value and order_id:
                                    released = await release_write_lock(order_id, lock_value)
                                    if released: logger.info(f"[Recovery] Released lock for failed saga {saga_id}")
                                    else: logger.warning(f"[Recovery] Could not release lock for failed saga {saga_id}")

                except Exception as e:
                    logger.exception(f"[Recovery] Error processing saga key {saga_key}: {e}")
                    failed_recovery_count += 1 

            if cursor == 0:
                break 

    except Exception as e:
        logger.exception(f"[Recovery] Error during saga recovery scan: {e}")

    logger.info(f"[Recovery] Finished check. Processed {processed_count} stuck sagas. Failed to recover {failed_recovery_count} sagas.")

# ----------------------------------------------------------------------------
# RabbitMQ Consumer Setup
# ----------------------------------------------------------------------------
# async def process_callback_messages(message):
#     try:
#         callback_action = message.headers.get('callback_action')
#         if not callback_action:
#             logger.warning(f"Received message without callback_action: {message}")
#             await message.ack()
#             return

#         # Get the payload
#         body = message.body
#         if isinstance(body, bytes):
#             body = body.decode('utf-8')

#         data = json.loads(body)
#         logger.info(f"Received callback message: action={callback_action}, data={data}")
#         if callback_action == 'process_stock_completed':
#             await process_stock_completed(data, message)
#         elif callback_action == 'process_payment_completed':
#             await process_payment_completed(data, message)
#         else:
#             logger.warning(f"Unknown callback_action: {callback_action}")
#             await message.ack()
#     except Exception as e:
#         logger.error(f"Error processing callback message: {e}", exc_info=True)
#         await message.ack()  # Ack to avoid requeue loops
# async def consume_messages():
#     """
#     Connect to RabbitMQ, declare exchange/queues, and consume relevant events.
#     """
#     while True:
#         try:
#             connection = await connect_robust(AMQP_URL)
#             channel = await connection.channel()

#             # Declare exchange
#             exchange = await channel.declare_exchange("saga_events", "topic", durable=True)
#             orchestrator_queue = await channel.declare_queue("orchestrator_queue", durable=True)
#             await orchestrator_queue.bind(exchange, routing_key="orchestrator.#")
#             # Declare & bind relevant queues
#             checkout_queue = await channel.declare_queue("order_checkout_requests", durable=True)
#             await checkout_queue.bind(exchange, routing_key="order.checkout")

#             stock_complete_queue = await channel.declare_queue("stock_complete_events", durable=True)
#             await stock_complete_queue.bind(exchange, routing_key="stock.reservation_completed")

#             payment_complete_queue = await channel.declare_queue("payment_complete_events", durable=True)
#             await payment_complete_queue.bind(exchange, routing_key="payment.completed")

#             failure_queue = await channel.declare_queue("saga_failure_events", durable=True)
#             await failure_queue.bind(exchange, routing_key="stock.reservation_failed")
#             await failure_queue.bind(exchange, routing_key="payment.failed")

#             # Start consumers
#             logger.info("[Order Orchestrator] Consuming messages...")
#             await orchestrator_queue.consume(process_callback_messages)
#             await checkout_queue.consume(process_checkout_request)
#             await stock_complete_queue.consume(process_stock_completed)
#             await payment_complete_queue.consume(process_payment_completed)
#             await failure_queue.consume(process_failure_events)

#             # Keep the consumer alive
#             await asyncio.Future()

#         except Exception as e:
#             logger.error(f"Error in consume_messages: {str(e)}", exc_info=True)
#             await asyncio.sleep(5)

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
            if "idempotency_key" not in payload and message.get("idempotency_key"):
                payload["idempotency_key"] = message.get("idempotency_key")
            if message.get("callback_action"):
                payload["callback_action"] = message.get("callback_action")
            if message.get("reply_to"):
                payload["reply_to"] = message.get("reply_to")
            message_id = message.get("message_id", str(uuid.uuid4()))
            success=await publish_event(routing_key=routing_key,payload=payload,message_id=message_id)
            if success:
                await db_master_saga.lrem(f"{OUTBOX_KEY}:processing", 0, raw)
                logger.info(f"[Outbox] Published event {routing_key} => {payload}")
            else:
                await asyncio.sleep(0.5)
                await db_master_saga.lrem(f"{OUTBOX_KEY}:processing", 0, raw)
                await db_master_saga.lpush(OUTBOX_KEY, raw)
                logger.warning(f"[Outbox] Failed to publish event, returning to queue: {routing_key}")
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
    # asyncio.create_task(consume_messages())
    asyncio.create_task(maintain_leadership())
    asyncio.create_task(recover_in_progress_sagas())
    asyncio.create_task(outbox_poller())
    asyncio.create_task(worker.start()),                      # Starts main request worker
    asyncio.create_task(orchestrator_reply_worker.start())

    await worker.start()
   finally:
       await close_db_connection()

if __name__ == '__main__':
   asyncio.run(main())

