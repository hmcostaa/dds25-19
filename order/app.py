import functools
import json
import logging
import os
import atexit
import random
import uuid
import asyncio
import time
import weakref

import redis.exceptions as redis_exceptions
import logging as logging_module
from aiohttp import web
from aio_pika import connect_robust, Message, DeliveryMode
from msgspec import DecodeError as MsgspecDecodeError
from typing import Tuple, Dict, Union

from tenacity import wait_exponential, stop_after_attempt, retry_if_exception_type, retry, wait_fixed, before_log

from common.rpc_client import RpcClient, logger
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

warnings.filterwarnings(
    "ignore",
    message="unclosed <redis.asyncio.connection.Connection",
    category=ResourceWarning
)


SERVICE_NAME = "order"

# TODO create config file for this
DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
SAGA_STATE_TTL = 864000 * 3
LOCK_TIMEOUT = 10000

logging.basicConfig(level=logging.INFO)
logging = logging.getLogger("order-service")

RETRYABLE_REDIS_EXCEPTIONS = (
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    redis.exceptions.BusyLoadingError,
    redis.exceptions.ReadOnlyError,
    redis.exceptions.WatchError,
   redis.exceptions.ReadOnlyError,
    redis.exceptions.AuthenticationError,
    redis.exceptions.ResponseError,
)

app = Flask("order-service")
_active_redis_clients = set()
@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def wait_for_master(sentinel, service_name):
    try:
        client = sentinel.master_for(service_name, decode_responses=False)
        await asyncio.wait_for(client.ping(), timeout=8.0)
        _active_redis_clients.add(client)
        return client
    except Exception as e:
        logging.error(f"Failed to connect to master for {service_name}: {str(e)}")
        await handle_redis_connection_failure(e)
        raise
@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def wait_for_slave(sentinel, service_name):
    try:
        client = sentinel.slave_for(service_name, decode_responses=False)
        await asyncio.wait_for(client.ping(), timeout=5.0)
        _active_redis_clients.add(client)
        return client
    except Exception as e:
        logging.error(f"Failed to connect to slave for {service_name}: {str(e)}")
        await handle_redis_connection_failure(e)
        raise

db_master_saga_cached = None
db_master_order_cached = None
db_slave_order_cached=None
db_slave_saga_cached=None
async def handle_redis_connection_failure(e):
    global db_master_saga_cached, db_master_order_cached, db_slave_saga_cached,db_slave_order_cached
    logging.warning(f"Redis connection failure detected: {str(e)} - Resetting connections")
    db_master_saga_cached = None
    db_master_order_cached = None
    db_slave_order_cached = None
    db_slave_saga_cached = None

async def get_db_master_saga():
    global db_master_saga_cached
    try:
        if db_master_saga_cached is not None:
            try:
                await asyncio.wait_for(db_master_saga_cached.ping(), timeout=2.0)
            except (redis.RedisError, asyncio.TimeoutError):
                logging.warning("Saga master connection unhealthy - resetting")
                db_master_saga_cached = None
        if db_master_saga_cached is None:
            db_master_saga_cached = await wait_for_master(sentinel_async, 'saga-master')
            await db_master_saga_cached.ping()
        return db_master_saga_cached
    except redis.RedisError as e:
        logging.error(f"Redis error getting master: {e}")
        db_master_saga_cached=None
        await handle_redis_connection_failure(e)
        raise

async def get_db_master_order():
    global db_master_order_cached
    try:
        if db_master_order_cached is not None:
            try:
                await asyncio.wait_for(db_master_order_cached.ping(), timeout=2.0)
            except (redis.RedisError, asyncio.TimeoutError):
                logging.warning("Order master connection unhealthy - resetting")
                db_master_order_cached = None
        if db_master_order_cached is None:
            db_master_order_cached = await wait_for_master(sentinel_async, 'order-master')
            await db_master_order_cached.ping()
        return db_master_order_cached
    except redis.RedisError as e:
        logging.error(f"Redis error getting master: {e}")
        db_master_order_cached = None
        await handle_redis_connection_failure(e)
        raise
async def get_db_slave_order():
    global db_slave_order_cached
    try:
        if db_slave_order_cached is not None:
            try:
                await asyncio.wait_for(db_slave_order_cached.ping(), timeout=2.0)
            except (redis.RedisError, asyncio.TimeoutError):
                logging.warning("Order master connection unhealthy - resetting")
                db_slave_order_cached = None
        if db_slave_order_cached is None:
            db_slave_order_cached = await wait_for_slave(sentinel_async, 'order-master')
            await db_slave_order_cached.ping()
        return db_slave_order_cached
    except redis.RedisError as e:
        logging.error(f"Redis error getting master: {e}")
        db_slave_order_cached = None
        await handle_redis_connection_failure(e)
        raise
async def get_db_slave_saga():
    global db_slave_saga_cached
    try:
        if db_slave_saga_cached is not None:
            try:
                await asyncio.wait_for(db_slave_saga_cached.ping(), timeout=2.0)
            except (redis.RedisError, asyncio.TimeoutError):
                logging.warning("Order master connection unhealthy - resetting")
                db_slave_saga_cached = None
        if db_slave_saga_cached is None:
            db_slave_saga_cached = await wait_for_slave(sentinel_async, 'saga-master')
            await db_slave_saga_cached.ping()
        return db_slave_saga_cached
    except redis.RedisError as e:
        logging.error(f"Redis error getting slave: {e}")
        db_slave_saga_cached = None
        await handle_redis_connection_failure(e)
        raise

sentinel_async = Sentinel([
    (os.environ['REDIS_SENTINEL_1'], 26379),
    (os.environ['REDIS_SENTINEL_2'], 26379),
    (os.environ['REDIS_SENTINEL_3'], 26379)],  # TODO 26379 for evey sentinel now
    socket_timeout=10,  # TODO check if this is the right value, potentially lower it
    socket_connect_timeout=10,
    socket_keepalive=True,
    password=os.environ['REDIS_PASSWORD'],
    retry_on_timeout=True,
    decode_responses=False
)

@retry(
    wait=wait_exponential(multiplier=0.1, min=0.1, max=2),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def initialize_redis():
    global db_master, db_slave,db_master_saga,db_slave_saga
    global sentinel_async
    # sentinel_async = Sentinel(
    #     [('redis-sentinel-1', 26379), ('redis-sentinel-2', 26379), ('redis-sentinel-3', 26379)],
    #     socket_timeout=10,
    #     decode_responses=False,
    #     password="redis",
    # )
    db_master = await wait_for_master(sentinel_async, 'order-master')
    db_slave = sentinel_async.slave_for('order-master', decode_responses=False)
    db_master_saga = await wait_for_master(sentinel_async,'saga-master')
    db_slave_saga = sentinel_async.slave_for('saga-master',decode_responses=False)
    logging.info("Connected to Redis Sentinel.")



# changed.. now saga master is used for idempotency as centralized client
# Read connection details from environment variables

# idempotency_redis_db = int(os.environ.get('IDEMPOTENCY_REDIS_DB', 0))
# idempotency_redis_client = sentinel_async.master_for(
#     "saga-master",
#     decode_responses=False,
#     db=idempotency_redis_db
# )
# Add at module level
redis_circuit_open = False
last_failure_time = 0


async def redis_circuit_breaker(func):
    async def wrapper(*args, **kwargs):
        global redis_circuit_open, last_failure_time
        current_time = time.time()

        if redis_circuit_open and current_time - last_failure_time > 10:
            redis_circuit_open = False  # Reset after 10 seconds

        if redis_circuit_open:
            raise redis.RedisError("Circuit breaker open")

        try:
            return await func(*args, **kwargs)
        except redis.RedisError as e:
            last_failure_time = current_time
            redis_circuit_open = True
            raise

    return wrapper
async def warmup_redis_connections():
    for _ in range(3):  # Create a small pool of ready connections
        try:
            client = await wait_for_master(sentinel_async, 'saga-master')
            await client.ping()
            client = await wait_for_master(sentinel_async, 'order-master')
            await client.ping()
        except Exception as e:
            logging.warning(f"Connection warmup error (non-fatal): {e}")
    logging.info("Redis connection pool pre-warmed")
_idempotency_redis_client = None

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=0.2, min=0.2, max=2),
    retry=retry_if_exception_type(redis.exceptions.RedisError),
    reraise=True
)
def get_idempotency_redis_client():
    global _idempotency_redis_client
    if _idempotency_redis_client is None:
        _idempotency_redis_client = sentinel_async.master_for(
            "saga-master",
            decode_responses=False,
            db=int(os.environ.get('IDEMPOTENCY_REDIS_DB', 0))
        )
    return _idempotency_redis_client

async def close_idempotency_redis_client():
    global _idempotency_redis_client
    client = _idempotency_redis_client
    if client:
        try:
            if hasattr(client, "close"):
                await client.close()
                logging.info("Closed idempotency Redis client")
        except Exception as e:
            logging.warning(f"Error closing Redis client: {e}")
        _idempotency_redis_client = None


SERVICE_ID = str(uuid.uuid4())
ORCHESTRATOR_ID = str(uuid.uuid4())
AMQP_URL = os.environ.get('AMQP_URL', 'amqp://user:password@rabbitmq:5672/')
worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="order_queue",
)
OUTBOX_KEY = "outbox:order"

saga_futures = {}
pubsub_clients = {}
listener_tasks = {}


async def cleanup_pubsub_resource():
    logging.info("Cleaning up Redis PubSub resources.")
    for saga_id, task in listener_tasks.items():
        if not task.done():
            task.cancel()
    for saga_id, pubsub in pubsub_clients.items():
        await pubsub.close()
    saga_futures.clear()
    pubsub_clients.clear()
    listener_tasks.clear()


original_handlers = {}


def register_original(name=None):
    def decorator(func):
        func_name = name or func.__name__
        original_handlers[func_name] = func

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)

        return wrapper

    return decorator

@retry(wait=wait_fixed(0.2), stop=stop_after_attempt(5), retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS))
async def safe_get_message(pubsub):
    return await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)


async def start_pubsub_listener(saga_id: str) -> asyncio.Task:
    notification_channel = f"saga_completion:{saga_id}"

    client_saga= await get_db_master_saga()
    pubsub = client_saga.pubsub()
    pubsub_clients[saga_id] = pubsub
    async def listen_for_completion():
        try:
            await pubsub.subscribe(notification_channel)
            logging.info(f"[Pubsub] Subscribed to {notification_channel}")
            while not saga_futures[saga_id].done():
                try:
                    message = await safe_get_message(pubsub)
                    if message:
                        # Check message is a dict before trying to access keys
                        if not isinstance(message, dict):
                            logging.warning(f"[PubSub] Received non-dict message type: {type(message)}")
                            await asyncio.sleep(0.1)
                            continue

                        if message.get('type') == 'message':
                            message_data = message.get('data')
                            if isinstance(message_data, bytes):
                                try:
                                    data = json.loads(message_data.decode('utf-8'))
                                except (UnicodeDecodeError, json.JSONDecodeError):
                                    data = message_data
                            elif isinstance(message_data, str):
                                try:
                                    data = json.loads(message_data)
                                except json.JSONDecodeError:
                                    data = message_data
                            else:
                                data = message_data
                        logging.info(f"[PubSub] Received notification for saga {saga_id}: {data}")
                        completion_data = json.loads(data) if isinstance(data, str) else data
                        if saga_id in saga_futures and not saga_futures[saga_id].done():
                            if completion_data.get("success", False):
                                saga_futures[saga_id].set_result({"success": True})
                            else:
                                error_msg = completion_data.get("error", "Unknown error")
                                saga_futures[saga_id].set_result({"success": False, "error": error_msg})
                        break
                    current_saga = await get_saga_state(saga_id)
                    if current_saga:
                        status = current_saga.get("status")
                        if status in ["SAGA_COMPLETED", "ORDER_FINALIZED"]:
                            if saga_id in saga_futures and not saga_futures[saga_id].done():
                                saga_futures[saga_id].set_result({"success": True})
                                break
                        elif status in ["SAGA_FAILED", "SAGA_FAILED_INITIALIZATION",
                                        "SAGA_FAILED_RECOVERY", "ORDER_FINALIZATION_FAILED",
                                        "SAGA_FAILED_ORDER_NOT_FOUND"]:
                            error_details = current_saga.get("details", {}).get("error", "Unknown saga failure")
                            if saga_id in saga_futures and not saga_futures[saga_id].done():
                                saga_futures[saga_id].set_result({"success": False, "error": error_details})
                                break
                    await asyncio.sleep(0.1)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logging.error(f"[PubSub] Error while processing messages: {e}")
                    await asyncio.sleep(0.5)
        except Exception as e:
            logging.error(f"[PubSub] Listener error for {saga_id}: {e}")
            if saga_id in saga_futures and not saga_futures[saga_id].done():
                saga_futures[saga_id].set_exception(e)
        finally:
            try:
                await pubsub.unsubscribe(notification_channel)
                logging.info(f"[PubSub] Unsubscribed from {notification_channel}")
                if saga_id in pubsub_clients:
                    await pubsub_clients[saga_id].close()
                    del pubsub_clients[saga_id]
                if saga_id in listener_tasks:
                    del listener_tasks[saga_id]
            except Exception as e:
                logging.error(f"[PubSub] Cleanup error for {saga_id}: {e}")

    task = asyncio.create_task(listen_for_completion())
    listener_tasks[saga_id] = task
    return task


async def handle_final_saga_state(saga_id, request_idempotency_key, final_status, result_payload, status_code):
    logging.info(f"[Order Orchestrator] Handling final saga state for saga {saga_id}")

    # verify tests assumes immediate correct response instead of partial 202
    # same for test microservices when it gives 'error' input and therefore fails
    # so this has to be made sync
    # could potentially lock-> wait check until final status= COMPLETED-> release lock
    # pre-checks?? however would not guarantee consistent state return, but is faster
    # adjust test? elaborate ? or show two version for performance difference
    # would be nice, but time constraint
    saga_data = await get_saga_state(saga_id)
    if saga_data:
        current_status = saga_data.get("status")
        if current_status == "SAGA_COMPLETED":
            logging.info(f"[Order Orchestrator] Saga {saga_id} already completed successfully")
            return result_payload, status_code
        elif current_status in ["SAGA_FAILED", "SAGA_FAILED_INITIALIZATION", "SAGA_FAILED_ORDER_NOT_FOUND",
                                "SAGA_FAILED_RECOVERY", "ORDER_FINALIZATION_FAILED", "STOCK_RESERVATION_FAILED"]:
            error_details = saga_data.get("details", {}).get("error", "Unknown saga failure")
            return {"error": error_details}, 400
    saga_futures[saga_id] = asyncio.Future()
    await start_pubsub_listener(saga_id)
    try:
        await asyncio.wait_for(saga_futures[saga_id], timeout=30)
        saga_result = saga_futures[saga_id].result()
        logging.info(f"[Order Orchestrator] Saga {saga_id} completed with result: {saga_result}")

        if saga_result.get("success", False):
            order_data = saga_data.get("details", {})
            user_id = order_data.get("user_id")
            logging.debug(f"[ORCHESTRATOR] Order data: user_id={user_id}, order_details={order_data}")
            payment_verified = False
            for attempt in range(5):

                current_saga = await get_saga_state(saga_id)
                logging.debug(f"[ORCHESTRATOR] Current saga state: {current_saga}")

                if current_saga and current_saga.get("status") in ["PAYMENT_COMPLETED", "ORDER_FINALIZED",
                                                                   "SAGA_COMPLETED"]:
                    payment_verified = True
                    break
                if user_id:
                    try:
                        payment_response = await rpc_client.call(
                            queue="payment_queue",
                            action="find_user",
                            payload={"user_id": user_id}
                        )
                        if payment_response and 'data' in payment_response:
                            logging.info(
                                f"[Order Orchestrator] Found user data in payment verification: {payment_response.get('data')}")
                    except Exception as e:
                        logging.error(f"[ORCHESTRATOR] Error checking payment status for user_id={user_id}: {str(e)}",
                                      exc_info=True)

            if payment_verified:
                logging.debug(f"[ORCHESTRATOR] Returning success payload={result_payload}, status_code={status_code}")
                await enqueue_outbox_message("handle_commit", {
                    "saga_id": saga_id,
                    "order_id": order_data.get("order_id")
                })
                return result_payload, status_code
            else:
                logging.error(f"[ORCHESTRATOR] Payment verification failed after 5 attempts for saga {saga_id}")
                return {"error": "Payment processing incomplete"}, 400

        else:
            logging.error(f"[ORCHESTRATOR] Saga {saga_id} failed, triggering failure event.")

            order_data = saga_data.get("details", {})
            order_id = order_data.get("order_id", "unknown_order")

            failure_payload = {
                "saga_id": saga_id,
                "order_id": order_id,
                "error": saga_result.get("error", "Unknown saga failure")
            }

            # Publish failure event for process_failure_events
            await enqueue_outbox_message("process_failure_events", failure_payload)
            await enqueue_outbox_message("handle_rollback", {
                "saga_id": saga_id,
                "order_id": order_id,
                "error": saga_result.get("error", "Unknown saga failure")
            })

            return {"error": saga_result.get("error", "Unknown saga failure")}, 400
    except asyncio.TimeoutError:
        logging.warning(f"[Order Orchestrator] Timed out waiting for saga {saga_id}")
        latest_saga_data = await get_saga_state(saga_id)
        order_id = None
        if result_payload and 'order_id' in result_payload:
            order_id = result_payload['order_id']
        elif latest_saga_data and 'details' in latest_saga_data and 'order_id' in latest_saga_data['details']:
            order_id = latest_saga_data['details']['order_id']
        if latest_saga_data:
            latest_status = latest_saga_data.get("status")
            logging.info(f"Latest saga state for saga {saga_id}: {latest_status}")

            if latest_status in ["SAGA_FAILED", "SAGA_FAILED_INITIALIZATION", "SAGA_FAILED_ORDER_NOT_FOUND",
                                 "SAGA_FAILED_RECOVERY", "ORDER_FINALIZATION_FAILED", "STOCK_RESERVATION_FAILED"]:
                error_details = latest_saga_data.get("details", {}).get("error", "Unknown saga failure")
                logging.error(f"[ORCHESTRATOR] Saga {saga_id} failed due to timeout/error: {error_details}")

                failure_payload = {
                    "saga_id": saga_id,
                    "order_id": order_id if order_id else "unknown_order",
                    "error": error_details
                }

                # Publish failure event for process_failure_events
                await enqueue_outbox_message("process_failure_events", failure_payload)
                await enqueue_outbox_message("saga.rollback", {
                    "saga_id": saga_id,
                    "order_id": order_id,
                    "error": error_details
                })
                response = {"error": error_details}
                if order_id:
                    response["order_id"] = order_id
                return response, 400

        response = {"warning": "Operation is taking longer than expected. Please check the status later."}
        if order_id:
            response["order_id"] = order_id
        return response, 200
    except redis_exceptions.TimeoutError as e:
        # logging.error(f"[Order Orchestrator] Redis read timeout for saga {saga_id}: {e}")
        #
        # saga_data = await get_saga_state(saga_id)
        # order_id = None
        # if saga_data:
        #     order_id = saga_data.get("details", {}).get("order_id")
        #
        # failure_payload = {
        #     "saga_id": saga_id,
        #     "order_id": order_id or "unknown_order",
        #     "error": "Redis read timeout during saga completion"
        # }
        #
        # # Publish failure and rollback
        # await enqueue_outbox_message("process_failure_events", failure_payload)
        # await enqueue_outbox_message("saga.rollback", failure_payload)

        return {"error": "Redis timeout during saga processing. Please try again later."}, 500
    finally:
        if saga_id in saga_futures:
            del saga_futures[saga_id]

        if saga_id in listener_tasks and not listener_tasks[saga_id].done():
            listener_tasks[saga_id].cancel()

@retry(
    wait=wait_exponential(multiplier=0.1, min=0.1, max=2),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def enqueue_outbox_message(routing_key: str, payload: dict):
    message = {
        "routing_key": routing_key,
        "payload": payload,
        "timestamp": time.time()
    }
    try:
        client_saga= await get_db_master_saga()
        await client_saga.rpush(OUTBOX_KEY, json.dumps(message))
        logging.info(f"[Order Orchestrator] Enqueued message to outbox: {message}")
    except Exception as e:
        logging.error(f"Error while enqueueing message to outbox: {str(e)}", exc_info=True)


@retry(
    wait=wait_exponential(multiplier=0.1, min=0.1, max=2),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def update_saga_and_enqueue(saga_id, status, routing_key: str | None, payload: dict | None, details=None,
                                  max_attempts: int = 5, backoff_ms: int = 100):
    # retry maybe todo too with max attempts?
    saga_key = f"saga:{saga_id}"
    notification_channel = f"saga_completion:{saga_id}"
    try:
        client_saga = await get_db_master_saga()
        async with client_saga.pipeline(transaction=True) as pipe:
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
                outbox_msg = {
                    "routing_key": routing_key,
                    "payload": payload,
                    "timestamp": time.time()
                }
                outbox_msg_json = json.dumps(outbox_msg)
            pipe.multi()
            await pipe.set(saga_key, json.dumps(saga_data), ex=SAGA_STATE_TTL)
            if outbox_msg:
                await pipe.rpush(OUTBOX_KEY, outbox_msg_json)
            is_success_terminal = status in ["SAGA_COMPLETED", "ORDER_FINALIZED"]
            is_failure_terminal = status in ["SAGA_FAILED", "SAGA_FAILED_INITIALIZATION",
                                             "SAGA_FAILED_RECOVERY", "ORDER_FINALIZATION_FAILED"]
            if is_success_terminal:
                await pipe.publish(notification_channel, json.dumps({"success": True}))
            elif is_failure_terminal:
                error_msg = details.get("error", "Unknown error") if details else "Unknown error"
                await pipe.publish(notification_channel, json.dumps({"success": False, "error": error_msg}))
            results = await pipe.execute()
            logging.info(f"[Order Orchestrator] Saga {saga_id} updated -> {status}")
            if (is_success_terminal or is_failure_terminal) and saga_id in saga_futures:
                future = saga_futures[saga_id]
                if not future.done():
                    if is_success_terminal:
                        future.set_result({"success": True})
                    else:
                        error_msg = details.get("error", "Unknown error") if details else "Unknown error"
                        future.set_result({"success": False, "error": error_msg})
            logging.info(
                f"[Order Orchestrator Atomic] Saga {saga_id} updated -> {status}. Enqueued message to outbox")
            return True
    except redis.exceptions.WatchError:
        logging.warning(
            f"[SAGA UPDATE] Retry. Saga {saga_id} failed to update. WatchError on {saga_id}")
    except redis.exceptions.RedisError as e:
        logging.error(f"[SAGA UPDATE] Redis error : {e}")
        logging.error(f"Error redis update saga state {saga_id} to {status}: {str(e)}")
        await asyncio.sleep(backoff_ms / 1000)
        backoff_ms *= 2
    except Exception as e:
        logging.error(f"Error updating saga state {saga_id} to {status}: {str(e)}", exc_info=True)

    logging.critical(f"[SAGA UPDATE] Failed after {max_attempts} attempts for {saga_id}.")
    raise redis.exceptions.WatchError(f"Failed to update saga {saga_id} after retries")

async def close_db_connection():
    await db_master.close()
    await db_slave.close()
    await db_master_saga.close()
    await db_slave_saga.close()
    await close_all_redis_clients()
    await cleanup_pubsub_resource()

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

@retry(
    wait=wait_exponential(multiplier=0.05, max=1),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def get_order_from_db(order_id: str) -> Tuple[Union[OrderValue, Dict], int]:
    try:
        # get serialized data
        # entry = await db_slave.get(order_id)
        client = await get_db_slave_order()
        entry = await client.get(order_id)  # temporary for testing TODO for change
        if not entry:
            logging.warning(f"Order: {order_id} not found in the database")
            return {"error": f"Order: {order_id} not found!"}, 404
        try:
            user_entry = msgpack.decode(entry, type=OrderValue)
            print(f"--- ORDER: get_order_from_db - Deserialized Order Type={type(user_entry)}, Value={user_entry} ---")
            return user_entry, 200
        except MsgspecDecodeError as e:
            logging.error(f"Failed to decode msgpack {order_id} from the database:{str(e)}")
            return {"error": f"Failed to decode msgpack {order_id} from the database:{str(e)}"}, 500

    except redis.RedisError as e:
        logging.error(f"Database Error while getting order: {order_id} from the database:{str(e)}")
        return {"error": DB_ERROR_STR}, 500

    # deserialize data if it exists else return null
    except Exception as e:
        logging.error(f"Error while getting order: {order_id} from the database:{str(e)}")
        return {"error": DB_ERROR_STR}, 400


@worker.register
async def batch_init_orders(data: dict, message):
    n = int(data['n_orders'])
    n_items = int(data['n_items'])
    n_users = int(data['n_users'])
    item_price = int(data['item_price'])

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
        client=await get_db_master_order()
        await client.mset(kv_pairs)
    except redis.RedisError:
        return {"error": DB_ERROR_STR}, 400
    return {"msg": "Batch init for orders successful"}, 200

@retry(
    wait=wait_exponential(multiplier=0.05, max=1),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def get_saga_state(saga_id: str) -> str:
    try:
        saga_key = f"saga:{saga_id}"
        client=await get_db_slave_saga()
        saga_json = await asyncio.wait_for(client.get(saga_key),timeout=5.0)
        if saga_json:
            if isinstance(saga_json, bytes):
                saga_json = saga_json.decode('utf-8')
            return json.loads(saga_json)
        logging.warning(f"[Order Orchestrator] Saga {saga_id} not found.")
        return None
    except Exception as e:
        logging.error(f"Error getting saga state: {str(e)}")
        return None


# Initialize RpcClient for the order service
rpc_client = RpcClient()


async def connect_rpc_client():
    # Connect the RpcClient to the AMQP server
    await rpc_client.connect(os.environ["AMQP_URL"])

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
@worker.register
@idempotent('create_order', get_idempotency_redis_client(), SERVICE_NAME, False)
async def create_order(data, message):
    # user_id = data.get("user_id")
    # in some cases data was the entire message
    user_id = data.get("user_id")
    if not user_id or not isinstance(user_id, str):
        logging.error(f"create_order called without a valid string user_id. Data received: {data}")
        return {"error": "Missing or invalid user_id provided"}, 400
    key = str(uuid.uuid4())

    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        print(f"--- ORDER SVC: create_order - Attempting to set order {key} for user {user_id}", flush=True)
        client=await get_db_master_order()
        await client.set(key, value)

        await update_saga_and_enqueue(saga_id=key, status="INITIATED", routing_key=None, payload=None)
        return {"order_id": key}, 200


    except redis.RedisError as e:
        logging.error(f"Redis error creating order {key} for user {user_id}: {e}")
        return {"error": "Redis error"}, 500
    except Exception as e:
        logging.error(f"Unexpected error creating order {key} for user {user_id}: {e}")
        return {"error": "internal error"}, 500


@worker.register
async def find_order(data, message):
    order_id = data.get("order_id")
    print(f"--- ORDER SVC: find_order - Looking for order_id: {order_id}", flush=True)
    order_entry, status_code = await get_order_from_db(order_id)
    if status_code != 200:
        print(f"--- ORDER SVC: find_order - Failed to find order {order_id}", flush=True)
        return order_entry, status_code  # Return the error dict and status
    return {  # Return the success dict and status
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost
    }, 200  # Return 200 for success


@worker.register
@idempotent('process_checkout_request', get_idempotency_redis_client(), SERVICE_NAME, False)
async def process_checkout_request(data, message):
    try:
        order_id = data.get('order_id')
        saga_id = str(uuid.uuid4())
        attempt_id = str(message.delivery_tag) if message else str(uuid.uuid4())
        logging.info(f"[Order Orchestrator] Received checkout request for order {order_id}. Saga ID: {saga_id}")
        order_result, status_code = await get_order_from_db(order_id)
        if status_code != 200:
            logging.error(f"Failed to retrieve order {order_id} from DB for checkout. Status code: {status_code}")
            return order_result if isinstance(order_result, dict) else {
                "error": f"Failed to retrieve order {order_id} from DB for checkout."}, status_code
        order_data = {
            "order_id": order_id,
            "paid": order_result.paid,
            "items": order_result.items,
            "user_id": order_result.user_id,
            "total_cost": order_result.total_cost
        }
        items_to_be_reserved = order_data.get("items", [])
        if not items_to_be_reserved:
            logging.error(f"[Order Orchestrator] No items to be fetched for order {order_id}")
            return {"error": f"No items found in order {order_id}"}, 400
        for item_id, amount in items_to_be_reserved:
            try:
                stock_check_payload = {"item_id": item_id,"timestamp": time.time()}
                stock_response = await rpc_client.call(queue="stock_queue", action="find_item",
                                                       payload=stock_check_payload)
                logging.info(f"[STOCK RESPONSE] Found stock response: {stock_response}")
                stock_data = stock_response.get('data', {})
                logging.info(f"[STOCK DATA] Found stock data: {stock_data}")
                if not stock_data:
                    logging.error(f"[Order Orchestrator] Item {item_id} not found when checking stock")
                    return {"error": f"Item {item_id} not found"}, 404

                available_stock = stock_data.get("stock", 0)
                if available_stock < amount:
                    logging.error(
                        f"[Order Orchestrator] Insufficient stock for item {item_id}. Requested: {amount}, Available: {available_stock}")
                    return {
                        "error": f"Insufficient stock for item {item_id}. Requested: {amount}, Available: {available_stock}"}, 400
            except Exception as e:
                logging.error(f"[Order Orchestrator] Error checking stock for item {item_id}: {str(e)}")
                return {"error": f"Error checking stock availability: {str(e)}"}, 500

        user_id = order_data.get("user_id")
        total_cost = order_data.get("total_cost")

        try:
            user_response = await rpc_client.call(queue="payment_queue", action="find_user",
                                                  payload={"user_id": user_id})
            user_data = user_response.get('data', {})
            logging.info(f"[Payment Response] Found user data: {user_data}")
            if not user_data:
                logging.error(f"[Order Orchestrator] User {user_id} not found when checking credit")
                return {"error": f"User {user_id} not found"}, 404
            available_credit = user_data.get("credit", 0)
            if available_credit < total_cost:
                logging.error(
                    f"[Order Orchestrator] Insufficient credit for user {user_id}. Required: {total_cost}, Available: {available_credit}")
                return {"error": f"Insufficient credit. Required: {total_cost}, Available: {available_credit}"}, 400
        except Exception as e:
            logging.error(f"[Order Orchestrator] Error checking credit for user {user_id}: {str(e)}")

        await update_saga_and_enqueue(
            saga_id,
            status="SAGA_STARTED",
            details=order_data,
            routing_key=None,
            payload=None,
        )

        order_lock_value = await acquire_write_lock(order_id)
        if order_lock_value is None:
            raise Exception("Order is already being processed")

        logging.info(f"[Order Orchestrator] Lock acquired {order_lock_value} for order {order_id}")
        updated_details = {
            **order_data,
            "lock_value": order_lock_value
        }

        await update_saga_and_enqueue(saga_id=saga_id,
                                      status="ORDER_LOCKED",
                                      details=updated_details,
                                      routing_key=None,
                                      payload=None)

        logging.info(f"[Order Orchestrator] Sending reserve stock message for saga={saga_id}, order={order_id}")
        request_idempotency_key = data.get("idempotency_key", "")
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
            await update_saga_and_enqueue(
                saga_id=saga_id,
                status="STOCK_RESERVATION_REQUESTED",
                details=None,
                routing_key=None,
                payload=None
            )

        return await handle_final_saga_state(
            saga_id=saga_id,
            request_idempotency_key=data.get("idempotency_key", ""),
            final_status="SAGA_STARTED",
            result_payload={"status": "success", "step": "stock reservation initiated"},
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error in process_checkout_request: {str(e)}")
        saga_id = locals().get("saga_id")
        if saga_id:
            try:
                success = await update_saga_and_enqueue(
                    saga_id=saga_id,
                    status="SAGA_FAILED_INITIALIZATION",
                    details={"error": str(e)},
                    routing_key=None,
                    payload=None,
                )
                if not success:
                    logging.critical(f"[Saga] Failed to update {saga_id} with error: {str(e)}")
                    raise Exception("Saga update failed - will be sent to DLQ")
            except Exception as e:
                logging.exception(f"Saga initialization failed: {str(e)}", exc_info=True)

        return {"error": str(e)}, 500


@worker.register
@idempotent('process_stock_completed', get_idempotency_redis_client(), SERVICE_NAME, False)
@register_original('process_stock_completed')
async def process_stock_completed(data, message):
    logging.info(f"[STOCK COMPLETED] Data received: {data}, message: {message}")
    try:
        logging.info(f"[Order Orchestrator] Processing stock completion: {data}")
        saga_id = data.get('saga_id')
        if not saga_id and message:
            if hasattr(message, 'correlation_id') and message.correlation_id:
                saga_id = message.correlation_id
                logging.info(f"[Order Orchestrator] Using correlation_id as saga_id: {saga_id}")
            elif hasattr(message, 'headers') and message.headers and 'saga_id' in message.headers:
                saga_id = message.headers['saga_id']
                logging.info(f"[Order Orchestrator] Using saga_id from headers: {saga_id}")

        order_id = data.get('order_id')
        if not order_id and 'data' in data and isinstance(data['data'], dict):
            order_id = data['data'].get('order_id')

        if 'error' in data:
            error_msg = data['error']
            logging.error(f"Stock reservation failed with error: {error_msg}")

            if not saga_id or not order_id:
                logging.error(f"[Order Orchestrator] Missing saga_id or order_id in error response")
                return {"error": "Missing saga_id or order_id in error response"}, 400

            await update_saga_and_enqueue(
                saga_id=saga_id,
                status="STOCK_RESERVATION_FAILED",
                details={"error": error_msg},
                routing_key=None,
                payload=None
            )

            try:
                completion_channel = f"saga_completion:{saga_id}"
                client_saga = await get_db_master_saga()
                await client_saga.publish(completion_channel, json.dumps({"success": False, "error": error_msg}))
                logging.info(f"[Order Orchestrator] Published stock failure notification for saga {saga_id}")
            except Exception as e:
                logging.error(f"[Order Orchestrator] Failed to publish stock failure notification: {str(e)}")

            return {"status": "failed", "error": error_msg}, 400

        if not saga_id:
            logging.error("[Order Orchestrator] Missing saga_id in process_stock_completed")
            if message:
                await message.ack()
            return {"error": "Missing saga_id in process_stock_completed"}, 400

        if not order_id:
            logging.error("[Order Orchestrator] Missing order_id in process_stock_completed")
            if message:
                await message.ack()
            return {"error": "Missing order_id in process_stock_completed"}, 400

        logging.info(f"[Order Orchestrator] Stock reservation completed for saga={saga_id}, order={order_id}")

        await update_saga_and_enqueue(
            saga_id=saga_id,
            status="STOCK_RESERVATION_COMPLETED",
            details=None,
            routing_key=None,
            payload=None
        )

        # Retrieve saga state for payment details
        saga_data = await get_saga_state(saga_id)
        if not saga_data:
            logging.error(f"[Order Orchestrator] Saga {saga_id} not found")
            if message:
                await message.ack()
            return {"error": f"Saga {saga_id} not found"}, 404
        order_data = saga_data.get("details", {})
        user_id = order_data.get("user_id")
        total_cost = order_data.get("total_cost")

        if not user_id or not total_cost:
            logging.error("[Order Orchestrator] Missing user_id or total_cost in saga details")
            if message:
                await message.ack()
            return {"error": "Missing user_id or total_cost in saga details"}, 400
        logging.info(f"[Order Orchestrator] Sending remove_credit request for saga={saga_id}, order={order_id}")
        payment_idempotency_key = f"{saga_id}:payment:{user_id}:{total_cost}"

        payment_payload = {
            "saga_id": saga_id,
            "order_id": order_id,
            "user_id": user_id,
            "amount": total_cost,
            "idempotency_key": payment_idempotency_key,
            "callback_action": "process_payment_completed"
        }

        await update_saga_and_enqueue(
            saga_id=saga_id,
            status="PAYMENT_INITIATED",
            details={"payment_requested_at": time.time()},
            routing_key="payment_queue",
            payload=payment_payload,
        )
        await worker.send_message(
            payload=payment_payload,
            queue="payment_queue",
            correlation_id=saga_id,
            action="remove_credit",
            reply_to="orchestrator_queue",
            callback_action="process_payment_completed"
        )
        await asyncio.sleep(0.1)

        logging.info(f"[Order Orchestrator] Payment request sent for saga={saga_id}, order={order_id}")
        if message and hasattr(message, 'ack'):
            await message.ack()

        return {"status": "success", "step": "stock reservation completed"}, 200

    except Exception as e:
        logging.error(f"[Order Orchestrator] Error in process_stock_completed: {str(e)}", exc_info=True)
        if message and hasattr(message, 'ack'):
            await message.ack()
        if 'saga_id' in locals() and 'order_id' in locals():
            try:
                await update_saga_and_enqueue(
                    saga_id=saga_id,
                    status="PAYMENT_INITIATION_FAILED",
                    details={"error": str(e)},
                    routing_key="stock.compensate",
                    payload={"saga_id": saga_id, "order_id": order_id}
                )
                completion_channel = f"saga_completion:{saga_id}"
                client_saga = await get_db_master_saga()
                await client_saga.publish(completion_channel, json.dumps({"success": False, "error": str(e)}))

            except Exception as inner_e:
                logging.error(f"[Order Orchestrator] Error updating saga state: {str(inner_e)}")

        return {"error": f"Error processing stock completion: {str(e)}"}, 500


@worker.register
@idempotent('process_payment_completed', get_idempotency_redis_client(), SERVICE_NAME, False)
@register_original('process_payment_completed')
async def process_payment_completed(data, message):
    try:
        saga_id = data.get("saga_id")
        order_id = data.get("order_id")
        if not saga_id and 'data' in data and isinstance(data['data'], dict):
            saga_id = data['data'].get('saga_id')
        if not order_id and 'data' in data and isinstance(data['data'], dict):
            order_id = data['data'].get('order_id')
        if not saga_id and message and hasattr(message, 'correlation_id'):
            saga_id = message.correlation_id

        logging.info(
            f"[Order Orchestrator] Processing payment completion: saga={saga_id}, order={order_id}, data={data}")
        if not saga_id or not order_id:
            logging.error(f"[Order Orchestrator] Missing saga_id or order_id in payment completion: {data}")
            if message:
                await message.ack()
            return {"error": "Missing saga_id or order_id in payment completion"}, 400
        await update_saga_and_enqueue(
            saga_id=saga_id,
            status="PAYMENT_COMPLETED",
            details=None,
            routing_key=None,
            payload=None,
        )
        try:
            completion_channel = f"saga_completion:{saga_id}"
            client_saga = await get_db_master_saga()
            await client_saga.publish(completion_channel, json.dumps({"success": True}))
            logging.info(f"[Order Orchestrator] Published payment completion notification for saga {saga_id}")
        except Exception as e:
            logging.error(f"[Order Orchestrator] Failed to publish payment completion notification: {str(e)}")

        saga_data = await get_saga_state(saga_id)
        if not saga_data:
            logging.error(f"[Order Orchestrator] Saga {saga_id} not found")
            if message:
                await message.ack()
            return {"error": f"Saga {saga_id} not found"}, 404
        item_details = saga_data.get("details", {})
        items_to_be_removed = item_details.get("items", item_details.get("reserved_items", []))
        lock_value = item_details.get("lock_value")

        if not lock_value:
            logging.warning(
                f"[Order Orchestrator] No lock value found for order {order_id}, attempting to proceed anyway")

        if not items_to_be_removed:
            logging.error(f"[Order Orchestrator] No items to be removed for order {order_id}")
            if message:
                await message.ack()
            return {"error": f"No items to be removed for order {order_id}"}, 400
        # for item_id, quantity in items_to_be_removed:
        #     stock_remove_idempotency_key = f"{saga_id}:stock_remove:{item_id}:{quantity}"
        #     payload = {
        #         "item_id": item_id,
        #         "amount": quantity,
        #         "idempotency_key": stock_remove_idempotency_key
        #     }
        #     logging.info(f"[Order Orchestrator] Sending stock remove message for item {item_id}, quantity {quantity}")
        #     await worker.send_message(payload=payload, queue="stock_queue", correlation_id=saga_id, reply_to="orchestrator_queue",
        #                               action="remove_stock")

        logging.info(f"[Order Orchestrator] Stock removed successfully for saga={saga_id}, order={order_id}")
        await update_saga_and_enqueue(
            saga_id,
            "STOCK_REMOVED",
            routing_key=None,
            payload=None,
        )
        try:
            current_order, status = await get_order_from_db(order_id)
            if status != 200:
                logging.error(f"[Order Orchestrator] Failed to get order {order_id} for payment completion")
                if message:
                    await message.ack()
                return {"error": f"Failed to get order {order_id}"}, 404

            def update_order_paid(order):
                order.paid = True
                return order

            updated_order, error_msg = await atomic_update_order(order_id, update_func=update_order_paid)
            if error_msg:
                logging.error(f"[Order Orchestrator] Failed to mark order {order_id} as paid: {error_msg}")
                if message:
                    await message.ack()
                return {"error": f"Failed to mark order as paid: {error_msg}"}, 500

            logging.info(f"[Order Orchestrator] Successfully marked order {order_id} as paid")
        except Exception as e:
            logging.error(f"[Order Orchestrator] Error updating order payment status: {str(e)}")
        order_released = await release_write_lock(order_id, lock_value)
        if order_released:
            await update_saga_and_enqueue(
                saga_id,
                "ORDER_LOCK_RELEASED",
                routing_key=None,
                payload=None,
            )
        else:
            logging.error(f"Failed to release lock for order {order_id} with lock value {lock_value}")
            force_released = await force_release_locks(order_id)
            if force_released:
                await update_saga_and_enqueue(
                    saga_id,
                    "ORDER_LOCK_RELEASE_FAILED",
                    details={"error": f"Failed to release lock for order {order_id} with value {lock_value}"},
                    routing_key=None,
                    payload=None
                )
            else:
                logging.error(f"Both normal and force release failed for order {order_id}")

            completion_channel = f"saga_completion:{saga_id}"
            try:
                client_saga = await get_db_master_saga()
                await client_saga.publish(completion_channel, json.dumps({"success": True}))
                logging.info(f"[Order Orchestrator] Published completion notification for saga {saga_id}")
            except Exception as e:
                logging.error(f"[Order Orchestrator] Failed to publish completion: {str(e)}")

        await update_saga_and_enqueue(
            saga_id,
            "ORDER_FINALIZED",
            routing_key=None,
            payload=None
        )
        await update_saga_and_enqueue(
            saga_id,
            "SAGA_COMPLETED",
            details={"completed_at": time.time()},
            routing_key=None,
            payload=None
        )
        try:
            completion_channel = f"saga_completion:{saga_id}"
            client_saga = await get_db_master_saga()
            await client_saga.publish(completion_channel, json.dumps({"success": True}))
            logging.info(f"[Order Orchestrator] Published final completion notification for saga {saga_id}")
        except Exception as e:
            logging.error(f"[Order Orchestrator] Failed to publish final completion notification: {str(e)}")

        if message and hasattr(message, 'ack'):
            await message.ack()
        return {"status": "success", "step": "payment completed"}, 200

    except Exception as e:
        logging.error(f"[Order Orchestrator] Error in process_payment_completed: {str(e)}", exc_info=True)
        if message and hasattr(message, 'ack'):
            await message.ack()
        if 'saga_id' in locals():
            try:
                order_id = locals().get('order_id')
                if not order_id:
                    saga_data = await get_saga_state(saga_id)
                    if saga_data:
                        order_id = saga_data.get("details", {}).get("order_id")

                if order_id:
                    await update_saga_and_enqueue(
                        saga_id=saga_id,
                        status="ORDER_FINALIZATION_FAILED",
                        details={"error": str(e)},
                        routing_key="payment.reverse",
                        payload={"saga_id": saga_id, "order_id": order_id}
                    )
                    saga_data = await get_saga_state(saga_id)
                    if saga_data:
                        steps_completed = [steps["status"] for steps in saga_data.get("steps", [])]
                        if "STOCK_RESERVATION_COMPLETED" in steps_completed:
                            await enqueue_outbox_message("stock.compensate", {
                                "saga_id": saga_id,
                                "order_id": order_id
                            })
                        await enqueue_outbox_message("order.checkout_failed", {
                            "saga_id": saga_id,
                            "order_id": order_id,
                            "status": "failed",
                            "error": f"Failed during order finalization: {str(e)}"
                        })
                    completion_channel = f"saga_completion:{saga_id}"
                    client_saga = await get_db_master_saga()
                    await client_saga.publish(completion_channel, json.dumps({"success": False, "error": str(e)}))
            except Exception as inner_e:
                logging.error(f"[Order Orchestrator] Error during failure handling: {str(inner_e)}", exc_info=True)

        return {"error": f"Failed to process payment completion: {str(e)}"}, 500


@worker.register
@idempotent('process_failure_events', get_idempotency_redis_client(), SERVICE_NAME, False)
async def process_failure_events(data, message):
    # TODO refactor for saga update and enqueue
    try:
        saga_id = data.get("saga_id")
        order_id = data.get("order_id")
        # TODO: Extract error message from the event
        error = data.get('error', 'Unknown error')
        # routing_key = data.routing_key

        # using gettatr to default to UNKOWN
        routing_key = getattr(message, 'routing_key', "UNKNOWN")

        logging.info(f"[Order Orchestrator] Failure event {routing_key} for saga={saga_id}: {error}")

        # update_saga_state(saga_id, f"FAILURE_EVENT_{routing_key.upper()}", {"error": error})

        saga_data = await get_saga_state(saga_id)
        if saga_data.get("status") in ["SAGA_COMPLETED", "ORDER_FINALIZED"]:
            logging.warning(f"[Order Orchestrator] Saga {saga_id} completed. Ignoring late failure.")
            return {"msg": "Saga already completed. Skipping failure logic."}, 200
        lock_value = saga_data["details"].get("lock_value")
        if lock_value:
            await release_write_lock(order_id, lock_value)
        if not saga_data:
            logging.error(f"[Order Orchestrator] Saga {saga_id} not found.")
            return {"msg": "Saga not found"}, 404

        # check whether saga already failed or completed
        current_status = saga_data.get("status")
        if current_status in ["SAGA_COMPLETED", "SAGA_FAILED", "SAGA_FAILED_RECOVERY"]:
            logging.warning(
                f"[Order Orchestrator] Saga {saga_id} already terminated ({current_status}). Ignoring failure event {routing_key}.")
            return {"msg": "Saga already terminated"}, 200
        # Ensure we don't process compensation more than once
        compensation_statuses = [step["status"] for step in saga_data.get("steps", []) if
                                 "COMPENSATION" in step["status"]]
        if compensation_statuses:
            logging.warning(f"[Order Orchestrator] Compensation already processed for saga {saga_id}. Skipping.")
            return {"msg": "Compensation already initiated"}, 200

        # Check which steps were completed
        steps_completed = [step["status"] for step in saga_data.get("steps", [])]
        compensations = []

        if "PAYMENT_COMPLETED" in steps_completed and routing_key:
            compensation_idempotency_key = f"{saga_id}:compensation:payment:{time.time()}"
            compensations.append(("payment.reverse", {"saga_id": saga_id, "order_id": order_id,
                                                      "idempotency_key": compensation_idempotency_key}))

        # If stock was reserved but we have a new failure (not from stock reservation itself), roll it back
        if "STOCK_RESERVATION_COMPLETED" in steps_completed and routing_key:
            stock_compensation_key = f"{saga_id}:stock_compensation:{order_id}:{time.time()}"
            reserved_items = saga_data.get("details", {}).get("items", [])

            if reserved_items:
                for item_id, quantity in reserved_items:
                    item_comp_key = f"{saga_id}:stock_compensation:{item_id}:{quantity}"
                    compensations.append(("stock.add", {
                        "saga_id": saga_id,
                        "item_id": item_id,
                        "amount": quantity,
                        "idempotency_key": item_comp_key
                    }))
            else:
                stock_comp_key = f"{saga_id}:stock_compensation:{order_id}"
                compensations.append(("stock.compensate", {
                    "saga_id": saga_id,
                    "order_id": order_id,
                    "idempotency_key": stock_comp_key
                }))
            compensations.append(("stock.compensate", {"saga_id": saga_id, "order_id": order_id,
                                                       "idempotency_key": stock_compensation_key}))

        # final failure
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
            logging.info(f"[Order Orchestrator] Successfully updated saga {saga_id} to {status}")
            for comp_key, payload in compensations:
                await enqueue_outbox_message(comp_key, payload)

        else:
            logging.critical(f"[Order Orchestrator] Failed to update saga {saga_id} to {status}")
            await message.nack(requeue=True)

    except Exception as e:
        logging.error(f"Error in process_failure_events: {str(e)}")
        try:
            await message.nack(requeue=True)
        except Exception as e:
            logging.error(f"Error nacking message: {str(e)}")


# #TODO potentially blocking and synchronous
# def send_post_request(url: str):
#     try:
#         response = requests.post(url)
#     except requests.exceptions.RequestException:
#         return {"error": "Request error"}, 400
#     else:
#         return response
@worker.register
async def handle_commit(data, message):
    logging.info(f"[SAGA COMMIT] Saga {data['saga_id']} committed successfully for order {data.get('order_id')}.")


@worker.register
async def handle_rollback(data, message):
    logging.warning(
        f"[SAGA ROLLBACK] Saga {data['saga_id']} rolled back for order {data.get('order_id')}. Error: {data.get('error')}")


# TODO should be made atomic, easily done through @idempotent when we merge with stock/payment idempotent branch

@worker.register
@idempotent("add_item", get_idempotency_redis_client(), SERVICE_NAME, False)
async def add_item(data, message):
    order_id = data.get("order_id")
    item_id = data.get("item_id")
    quantity = data.get("quantity")
    stock_response = None

    if not order_id or not item_id or not quantity:
        return {"error": "Missing required fields (order_id,item_id, quantity)"}, 400
        # load the order

    # call stock service

    payload = {
        "item_id": item_id  # should be consistent with stock
    }
    try:
        logging.info(f"--- ORDER SVC: add_item - Stock service payload: {payload}")

        stock_response = await rpc_client.call(queue="stock_queue", action="find_item", payload=payload)
        logging.info(f"--- ORDER SVC: add_item - Stock service response: {stock_response}")
    except Exception as e:
        logging.error(f"Unexpected error getting stock data for item {item_id}: {e}")
        return {"error": "Unexpected error getting stock data"}, 503

    stock_data = stock_response.get('data')
    if not isinstance(stock_data, dict):
        logging.error(f"--- ORDER SVC: add_item - Stock service returned invalid data type: {type(stock_data)}")
        return {"error": f"Item data not found, item: {item_id}"}, 404

    item_price = stock_data.get("price")
    item_stock = stock_data.get("stock")
    if item_price is None:
        logging.error(f"--- Invalid or missing price for item {item_id}")
        return {"error": f"Item: {item_id} price not found!"}, 404
    try:
        if item_stock < quantity:
            return {"error": f"Item {item_id} is out of stock. Requested {quantity}, available {item_stock}."}, 400
    except(ValueError, TypeError):
        return {"error": "Invalid quantity specified"}, 400

    def update_func(order: OrderValue) -> OrderValue:
        order.items.append((item_id, quantity))
        order.total_cost += quantity * item_price
        return order

    error_msg = None
    try:
        updated_order, error_msg = await atomic_update_order(order_id, update_func=update_func)
        if error_msg:
            logging.error(f"Failed to update order {order_id}: {error_msg}")
            logging.debug(error_msg)
            if "not_found" in error_msg.lower():
                status_code = 404
            else:
                status_code = 500
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
            return {"data": {"added": True, "order_id": order_id, "new_total_cost": updated_order.total_cost}}, 200

        else:
            return ({"added": False, "error": "Internal processing error"}, 500)
    except ValueError as e:
        error_msg = str(e)
        return ({"added": False, "error": error_msg}, 400)
    except Exception as e:
        logging.exception("internal error")
        return ({"added": False, "error": error_msg}, 400)

    # update and save order


# made async TODO
@retry(
    wait=wait_exponential(multiplier=0.1, min=0.05, max=2),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def atomic_update_order(order_id, update_func):
    max_retries = 10
    base_backoff = 0.1
    for attempt in range(max_retries):
        try:
            client = await get_db_master_order()
            async with client.pipeline(transaction=True) as pipe:
                await pipe.watch(order_id)
                entry = await pipe.get(order_id)
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
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def acquire_write_lock(order_id: str, lock_timeout: int = 10000) -> str:
    client=await get_db_master_order()
    lock_key = f"write_lock:order:{order_id}"
    lock_value = str(uuid.uuid4())  # Unique value to identify the lock owner
    try:
        # Try to acquire the lock using Redis SET with NX and PX options
        is_locked = await client.set(lock_key, lock_value, nx=True, px=lock_timeout)
        if not is_locked:
            return None
        if is_locked:
            logging.info(f"Lock acquired for order: {order_id}")
            await client.hset(
                f"lock_meta:{lock_key}",
                mapping={
                    "service_id": SERVICE_ID,
                    "ttl": SAGA_STATE_TTL,
                    "lock_timeout": lock_timeout,
                    "lock_value": lock_value,
                    "acquired_at": time.time()
                }
            )

            # TODO, check if correct? lock timeout wasnt impelemented??
            # await db_master.expire(f"lock_meta:{lock_key}", int(lock_timeout / 1000) + 10)
            return lock_value

        else:
            logging.info(f"Lock already acquired for order: {order_id}")
            return None
    except Exception as e:
        logging.error(f"Error while acquiring lock for order: {order_id}: {str(e)}")
        return None


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def release_write_lock(order_id: str, lock_value: str) -> bool:
    lock_key = f"write_lock:order:{order_id}"
    meta_key = f"lock_meta:{lock_key}"
    logging.info(f"[LOCK]Releasing lock for order: {order_id}")

    client = await get_db_master_order()
    try:
        # Check if lock exists first
        actual = await client.get(lock_key)

        # Lock is already gone - consider this a success
        if actual is None:
            logging.info(f"[LOCK] Lock for order {order_id} already released")
            return True

        # Use Lua script for atomic check-and-delete
        lua_script = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
               return redis.call("del", KEYS[1])
            else
               return 0
            end
            """

        result = await client.eval(lua_script, 1, lock_key, lock_value)

        if result == 1:
            logging.info(f"[LOCK] Released lock for order {order_id}")
            return True
        else:
            actual_decoded = actual.decode("utf-8") if isinstance(actual, bytes) else actual
            logging.warning(
                f"[LOCK] Lock value mismatch for {order_id}. Expected: {lock_value}, Found: {actual_decoded}")
            return False
    except Exception as e:
        logging.error(f"[LOCK] Error releasing lock for {order_id}: {e}")
        return False
    #     db_master= await get_db_master_order()
    #     result = await db_master.eval(lua_script, 2, lock_key, meta_key, lock_value)
    #     logging.info(f"[LOCk]Releasing lock for order: {order_id} result: {result}")
    #     # i think this should be 2 instead of 1, my lue experience is limited
    #     # TODO check
    #     if result == 1:
    #         logging.info(f"Lock released for order: {order_id}")
    #         return True
    #     else:
    #         db_master = await get_db_master_order()
    #         current_lock_value = await db_master.get(lock_key)
    #         logging.info(f"Lock released for order: {order_id}: {current_lock_value}")
    #         logging.info(f"[LOCK]Lock value for order: {order_id}: {lock_value}")
    #         if current_lock_value == lock_value:
    #             logging.warning(f"still has a value for lock {lock_key} ")
    #         else:
    #             logging.warning(f"Failed to release lock for order {order_id}-lock value mismatch")
    #         return False  # Returns True if the lock was released, False otherwise
    #
    # except redis.exceptions.RedisError as e:
    #         logging.error(f"[Lock Release] Redis error releasing lock for {order_id}: {e}")
    #         raise
    #
    # except Exception as e:
    #     logging.error(f"[Lock Release] Unexpected error releasing lock for {order_id}: {e}")
    #     return False


# Forcibly release abandoned locks
# was unused
async def force_release_locks(order_id: str) -> bool:
    lock_key = f"write_lock:order:{order_id}"
    meta_key = f"lock_meta:{lock_key}"
    try:
        client = await get_db_master_order()
        meta = await client.hgetall(meta_key)
        if meta:
            acquired_at = float(meta.get("acquired_at", 0) or 0)
            timeout = int(meta.get("lock_timeout", LOCK_TIMEOUT))
            if time.time() > acquired_at + (timeout / 1000) * 2:
                client = await get_db_master_order()
                await client.delete(lock_key)
                await client.delete(meta_key)
                logging.warning(f"Force released lock for order: {order_id}")
                return True
        return False
    except Exception as e:
        logging.error(f"Error while releasing lock for order: {order_id}: {str(e)}")
        return False

@retry(
    wait=wait_fixed(0.5),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True,
    before=before_log(logging,logging_module.WARNING)
)
async def safe_brpoplpush(redis_conn, source, destination, timeout=5):
    return await redis_conn.brpoplpush(source, destination, timeout=timeout)


async def publish_event(routing_key, payload, message_id=None):
    connection = None
    try:
        connection = await connect_robust(AMQP_URL)
        channel = await connection.channel()

        # Declare exchange of type "topic" (or direct/fanout as needed)
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
        logging.info(f"[Order Orchestrator] Published event {routing_key} => {payload}")

        await connection.close()
        return True
    except Exception as e:
        logging.error(f"Error publishing event: {str(e)}", exc_info=True)
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
            # Attempt to acquire leadership
            client_saga = await get_db_master_saga()
            is_leader = await client_saga.set(
                "leader:order-orchestrator",
                ORCHESTRATOR_ID,
                nx=True,
                ex=30  # expires in 30 seconds
            )
            if is_leader:
                logging.info("[Order Orchestrator] Acquired leadership.")
            else:
                # If already the leader, refresh the TTL
                client_saga = await get_db_master_saga()
                if await client_saga.get("leader:order-orchestrator") == ORCHESTRATOR_ID:
                    await client_saga.expire("leader:order-orchestrator", 30)

            # Heartbeat
            client_saga = await get_db_master_saga()
            await client_saga.setex(f"heartbeat:{ORCHESTRATOR_ID}", 30, "alive")
            await asyncio.sleep(10)
        except Exception as e:
            logging.error(f"Leadership maintenance error: {str(e)}")
            await asyncio.sleep(5)


async def recover_in_progress_sagas():
    """
    If there are sagas stuck in a “processing” state for too long,
    mark them as failed or attempt compensation.
    """
    try:
        client_saga = await get_db_master_saga()
        list_of_keys = await client_saga.keys("saga:*")
        print(f"--- ORDER SVC: recover_in_progress_sagas - List of keys: {list_of_keys}")
        for key in list_of_keys:
            client_saga=await get_db_slave_saga()
            saga_json = await client_saga.get(key)
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
                    if order_id:
                        await force_release_locks(order_id)
                        logging.warning(f"[Order Recovery] Force released lock for stuck saga order: {order_id}")

                    logging.warning(f"[Order Orchestrator] Found stuck saga={saga_id}, forcing fail.")

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
                            recovery_key = f"{saga_id}:recovery:stock:{order_id}"
                            other_compensations.append(("stock.compensate", {
                                "saga_id": saga_id,
                                "order_id": order_id,
                                "idempotency_key": recovery_key
                            }))

                    elif "STOCK_RESERVATION_COMPLETED" in steps_completed:
                        comp_routing_key = "stock.compensate"
                        stock_comp_key = f"{saga_id}:payment_failure:stock:{order_id}"
                        payload = {
                            "saga_id": saga_id,
                            "order_id": order_id,
                            "idempotency_key": stock_comp_key
                        }

                    success = await update_saga_and_enqueue(
                        saga_id=saga_id,
                        status="SAGA_FAILED_RECOVERY",
                        details={"error": "Timeout recovery triggered"},
                        routing_key=comp_routing_key,
                        payload=payload,  # potentially none
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
                        logging.critical(f"Failed to enqueue outbox messages for saga {saga_id}")

    except Exception as e:
        print(f"!!! ERROR INSIDE recover_in_progress_sagas: {e}", flush=True)
        logging.error(f"Error recovering sagas: {str(e)}", exc_info=True)


async def cleanup_stale_locks():
    while True:
        try:
            pattern = "write_lock:order:*"
            client=await get_db_master_order()
            lock_keys = await client.keys(pattern)

            for lock_key in lock_keys:
                meta_key = f"lock_meta:{lock_key}"
                meta = await client.hgetall(meta_key)

                if meta and "lock_timeout" in meta:
                    lock_timeout = int(meta["lock_timeout"])
                    # If lock has been around too long, force release it
                    await client.delete(lock_key)
                    await client.delete(meta_key)
                    logging.warning(f"Cleaned up stale lock: {lock_key}")

            await asyncio.sleep(30)  # Check every 30 seconds
        except Exception as e:
            logging.error(f"Error in cleanup_stale_locks: {str(e)}")
            await asyncio.sleep(60)  # Wait longer after an error


async def periodic_lock_cleanup():
    while True:
        try:
            client=await get_db_master_order()
            lock_keys = await client.keys("write_lock:order:*")

            for key in lock_keys:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                order_id = key_str.split(":")[-1]
                meta_key = f"lock_meta:{key_str}"
                meta = await client.hgetall(meta_key)

                if meta:
                    acquired_raw = meta.get(b"acquired_at") or meta.get("acquired_at")
                    timeout_raw = meta.get(b"lock_timeout") or meta.get("lock_timeout")

                    try:
                        acquired_at = float(acquired_raw.decode() if isinstance(acquired_raw, bytes) else acquired_raw)
                        lock_timeout = int(timeout_raw.decode() if isinstance(timeout_raw, bytes) else timeout_raw)
                        # If lock is older than 2x the timeout, force release it
                        if time.time() - acquired_at > (lock_timeout / 1000) * 2:
                            await force_release_locks(order_id)
                            logging.warning(f"[Lock Cleanup] Released stale lock for order {order_id}")
                    except Exception as e:
                        logging.warning(f"[Lock Cleanup] Skipped bad metadata for {order_id}: {e}")

            await asyncio.sleep(30)
        except Exception as e:
            logging.error(f"Error in periodic lock cleanup: {str(e)}")
            await asyncio.sleep(60)


# ----------------------------------------------------------------------------
# RabbitMQ Consumer Setup
# ----------------------------------------------------------------------------

async def process_callback_messages(message):
    service_type = None
    try:
        correlation_id = getattr(message, 'correlation_id', None)
        logging.info(f"[Order Orchestrator] Received callback message with correlation_id: {correlation_id}")
        callback_action = None

        if hasattr(message, 'headers') and message.headers:
            callback_action = message.headers.get('callback_action')
            if callback_action:
                logging.info(f"[Order Orchestrator] Found callback_action in headers: {callback_action}")
        if not callback_action:
            try:
                body = message.body
                if isinstance(body, bytes):
                    body = body.decode('utf-8')
                if isinstance(body, str):
                    try:
                        data = json.loads(body)
                    except json.JSONDecodeError:
                        data = {'data': body}
                else:
                    data = body
                if 'callback_action' in data:
                    callback_action = data['callback_action']
                elif 'data' in data and isinstance(data['data'], dict):
                    if 'callback_action' in data['data']:
                        callback_action = data['data']['callback_action']
                service_type = None
                if hasattr(message, 'routing_key'):
                    routing_key = message.routing_key
                    if 'stock' in routing_key.lower():
                        service_type = 'stock'
                    elif 'payment' in routing_key.lower():
                        service_type = 'payment'
                if correlation_id and not callback_action:
                    saga_data = await get_saga_state(correlation_id)
                    if saga_data:
                        current_status = saga_data.get("status")
                        logging.info(f"[Order Orchestrator] Current status for saga {correlation_id}: {current_status}")

                        if "STOCK_RESERVATION_REQUESTED" in current_status:
                            callback_action = 'process_stock_completed'
                            logging.info(
                                f"[Order Orchestrator] Inferred callback_action from saga status: {callback_action}")
                        elif "PAYMENT_INITIATED" in current_status:
                            callback_action = 'process_payment_completed'
                            logging.info(
                                f"[Order Orchestrator] Inferred callback_action from saga status: {callback_action}")

            except Exception as e:
                logging.error(f"[Order Orchestrator] Error parsing message body: {e}")
        if not callback_action and service_type:
            if service_type == 'stock':
                callback_action = 'process_stock_completed'
                logging.info(f"[Order Orchestrator] Inferred callback_action from service type: {callback_action}")
            elif service_type == 'payment':
                callback_action = 'process_payment_completed'
                logging.info(f"[Order Orchestrator] Inferred callback_action from service type: {callback_action}")

        if not callback_action and hasattr(message, 'reply_to') and message.reply_to:
            if 'stock' in str(message.reply_to).lower():
                callback_action = 'process_stock_completed'
                logging.info(f"[Order Orchestrator] Inferred callback_action from reply_to: {callback_action}")
            elif 'payment' in str(message.reply_to).lower():
                callback_action = 'process_payment_completed'
                logging.info(f"[Order Orchestrator] Inferred callback_action from reply_to: {callback_action}")
        if callback_action:
            body = message.body
            logging.info(f"[CALLBACK] Inferred callback_action body: {body}")
            if isinstance(body, bytes):
                body = body.decode('utf-8')
            data = json.loads(body)
            payload_data = data
            if 'data' in data and isinstance(data['data'], dict):
                payload_data = data['data']

            logging.info(f"[Order Orchestrator] Processing message body data structure: {type(data)}")
            logging.info(
                f"[Order Orchestrator] Message data keys: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
            if callback_action == 'process_stock_completed':
                try:
                    logging.info(f"[CALLBACK ACTION] Using function registry approach")
                    await message.ack()
                    original_handler = original_handlers.get('process_stock_completed')
                    logging.info(f"[CALLBACK ACTION] Original handler registered: {original_handler}")
                    if original_handler:
                        await original_handler(payload_data, None)
                    else:
                        logging.error("[Order Orchestrator] Original handler not found in registry")

                except Exception as e:
                    logging.error(f"[Order Orchestrator] Error in stock completion: {e}", exc_info=True)
            elif callback_action == 'process_payment_completed':
                try:
                    logging.info(f"[CALLBACK ACTION] Using function registry approach")
                    await message.ack()
                    original_handler = original_handlers.get('process_payment_completed')
                    if original_handler:
                        await original_handler(payload_data, None)
                    else:
                        logging.error("[Order Orchestrator] Original handler for process_payment_completed not found")
                except Exception as e:
                    logging.error(f"[Order Orchestrator] Error in payment completion handler: {e}", exc_info=True)
            else:
                logging.warning(f"[Order Orchestrator] Unknown callback_action: {callback_action}")
                await message.ack()
        else:
            logging.warning(f"[Order Orchestrator] Unable to determine callback_action for message: {message}")
            debug_info = {
                "correlation_id": correlation_id,
                "routing_key": getattr(message, 'routing_key', None),
                "headers": getattr(message, 'headers', None),
                "reply_to": getattr(message, 'reply_to', None),
            }

            try:
                body = message.body
                if isinstance(body, bytes):
                    body = body.decode('utf-8')
                debug_info["body"] = json.loads(body)
            except:
                debug_info["body"] = "Could not decode body"

            logging.warning(f"[Order Orchestrator] Message debug info: {debug_info}")
            await message.ack()

    except Exception as e:
        logging.error(f"[Order Orchestrator] Error processing callback message: {e}", exc_info=True)
        if message and hasattr(message, 'ack'):
            await message.ack()


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
            orchestrator_queue = await channel.declare_queue("orchestrator_queue", durable=True)
            await orchestrator_queue.bind(exchange, routing_key="orchestrator.#")
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

            logging.info("[Order Orchestrator] Consuming messages...")
            await orchestrator_queue.consume(process_callback_messages)
            await checkout_queue.consume(process_checkout_request)
            await stock_complete_queue.consume(process_stock_completed)
            await payment_complete_queue.consume(process_payment_completed)
            await failure_queue.consume(process_failure_events)

            await asyncio.Future()

        except Exception as e:
            logging.error(f"Error in consume_messages: {str(e)}", exc_info=True)
            await asyncio.sleep(5)


@worker.register
async def get_saga_state_rpc(data, message):
    """
    RPC-callable version of get_saga_state that other services can invoke
    """
    saga_id = data.get("saga_id")
    if not saga_id:
        return {"error": "Missing saga_id parameter"}, 400

    try:
        saga_data = await get_saga_state(saga_id)
        return saga_data, 200
    except Exception as e:
        logging.error(f"Error in get_saga_state_rpc: {str(e)}", exc_info=True)
        return {"error": str(e)}, 500


async def outbox_poller():
    while True:
        try:
            # blpop instead of loop
            # TODO what if message loss between LOP and succesfull AMQP publish???
            # Added brpoplush to prevent the issue
            client_saga =await get_db_master_saga()
            raw = await client_saga.brpoplpush(OUTBOX_KEY, f"{OUTBOX_KEY}:processing", timeout=5)
            if raw is None:
                await asyncio.sleep(1)
                continue
            try:
                message = json.loads(raw)
            except Exception as e:
                logging.error(f"[Outbox] Failed to decode message: {e}")
                await client_saga.lrem(f"{OUTBOX_KEY}:processing", 0, raw)
                continue
            routing_key = message.get("routing_key")
            payload = message.get("payload")
            if "idempotency_key" not in payload and message.get("idempotency_key"):
                payload["idempotency_key"] = message.get("idempotency_key")
            if message.get("callback_action"):
                payload["callback_action"] = message.get("callback_action")
            if message.get("reply_to"):
                payload["reply_to"] = message.get("reply_to")
            message_id = message.get("message_id", str(uuid.uuid4()))
            success = await publish_event(routing_key, payload, message_id)
            if success:
                client_saga = await get_db_master_saga()
                await client_saga.lrem(f"{OUTBOX_KEY}:processing", 0, raw)
                logging.info(f"[Outbox] Published event {routing_key} => {payload}")
            else:
                await asyncio.sleep(0.5)
                client_saga = await get_db_master_saga()
                await client_saga.lrem(f"{OUTBOX_KEY}:processing", 0, raw)
                await client_saga.lpush(OUTBOX_KEY, raw)
                logging.warning(f"[Outbox] Failed to publish event, returning to queue: {routing_key}")
        except Exception as e:
            logging.error(f"Error publishing event: {str(e)}", exc_info=True)
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
    logging.info("[Order Orchestrator] Health check server started on :8000")
@worker.register
async def health_check(data, message):
    return {"status": "ok"}, 200

async def close_all_redis_clients():
    global _active_redis_clients
    for client in _active_redis_clients:
        try:
            await client.aclose()
        except Exception as e:
            logging.warning(f"Failed to close Redis client: {e}")
    _active_redis_clients.clear()

@worker.register
async def reset_redis_connections(data, message):
    """Force reset all Redis master connections"""
    global db_master_saga_cached, db_master_order_cached
    db_master_saga_cached = None
    db_master_order_cached = None
    logging.info("Redis connections reset for failover testing")
    return {"status": "Redis connections reset"}, 200

async def main():
    try:
        await warmup_redis_connections()
        await initialize_redis()
        await connect_rpc_client()
        asyncio.create_task(health_check_server())
        asyncio.create_task(consume_messages())
        asyncio.create_task(maintain_leadership())
        asyncio.create_task(recover_in_progress_sagas())
        asyncio.create_task(outbox_poller())
        asyncio.create_task(periodic_lock_cleanup())
        await worker.start()
    finally:
        await close_idempotency_redis_client()
        await close_db_connection()


if __name__ == '__main__':
    asyncio.run(main())
