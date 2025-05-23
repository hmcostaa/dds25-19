import logging
import os
import atexit
import sys
import sys
import uuid
import asyncio
import redis.asyncio as redis
from quart import app
from redis.asyncio.sentinel import Sentinel
from flask import Flask, jsonify, abort, Response
from typing import Optional, Tuple, Any, Union, Tuple, Dict
from redis.exceptions import WatchError, RedisError

import random
import time
import logging
import redis.exceptions
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
RETRYABLE_REDIS_EXCEPTIONS = (
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    redis.exceptions.BusyLoadingError,
    redis.exceptions.ReadOnlyError,
    redis.exceptions.WatchError
)

from global_idempotency.idempotency_decorator import idempotent

from msgspec import msgpack, Struct, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s"
)

logging = logging.getLogger("payment-service")
SERVICE_NAME = "payment"

IdempotencyResultTuple = Tuple[Dict[str, Any], int]

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from common.amqp_worker import AMQPWorker

worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="payment_queue",
)

DB_ERROR_STR = "DB error"
db_master_payment_cached = None
db_slave_payment_cached=None
_active_redis_clients=set()
@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def wait_for_master(sentinel, service_name):
    client = sentinel.master_for(service_name, decode_responses=False)
    await asyncio.wait_for(client.ping(), timeout=8.0)
    _active_redis_clients.add(client)
    return client
async def close_all_redis_clients():
    global _active_redis_clients
    for client in _active_redis_clients:
        try:
            await client.aclose()
        except Exception as e:
            logging.warning(f"Failed to close Redis client: {e}")
    _active_redis_clients.clear()
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
async def get_db_master_payment():
    global db_master_payment_cached
    try:
        if db_master_payment_cached is not None:
            try:
                await asyncio.wait_for(db_master_payment_cached.ping(), timeout=2.0)
            except (redis.RedisError, asyncio.TimeoutError):
                logging.warning("Payment master connection unhealthy - resetting")
                db_master_payment_cached = None
        if db_master_payment_cached is None:
            db_master_payment_cached = await wait_for_master(sentinel_async, 'payment-master')
            await db_master_payment_cached.ping()
        return db_master_payment_cached
    except redis.RedisError as e:
        logging.error(f"Redis error getting master: {e}")
        db_master_payment_cached = None
        await handle_redis_connection_failure(e)
        raise
async def get_db_slave_payment():
    global db_slave_payment_cached
    try:
        if db_slave_payment_cached is not None:
            try:
                await asyncio.wait_for(db_slave_payment_cached.ping(), timeout=2.0)
            except (redis.RedisError, asyncio.TimeoutError):
                logging.warning("Payment master connection unhealthy - resetting")
                db_slave_payment_cached = None
        if db_slave_payment_cached is None:
            db_slave_payment_cached = await wait_for_slave(sentinel_async, 'payment-master')
            await db_slave_payment_cached.ping()
        return db_slave_payment_cached
    except redis.RedisError as e:
        logging.error(f"Redis error getting master: {e}")
        db_slave_payment_cached = None
        await handle_redis_connection_failure(e)
        raise


async def handle_redis_connection_failure(e):
    global db_master_payment_cached,db_slave_payment_cached
    logging.warning(f"Redis connection failure detected: {str(e)} - Resetting connections")
    db_master_payment_cached = None
    db_slave_payment_cached=None

sentinel_async = Sentinel([
    (os.environ['REDIS_SENTINEL_1'], 26379),
    (os.environ['REDIS_SENTINEL_2'], 26379),
    (os.environ['REDIS_SENTINEL_3'], 26379)],
    socket_timeout=10,  # TODO check if this is the right value, potentially lower it
    socket_connect_timeout=10,
    socket_keepalive=True,
    password=os.environ['REDIS_PASSWORD'],
    retry_on_timeout=True,
    decode_responses=False
)

# db_master = sentinel_async.master_for('payment-master', decode_responses=False)
# db_slave = sentinel_async.slave_for('payment-master', decode_responses=False)

async def initialize_redis():
    global db_master, db_slave
    # sentinel_async = Sentinel(
    #     [('redis-sentinel-1', 26379), ('redis-sentinel-2', 26379), ('redis-sentinel-3', 26379)],
    #     socket_timeout=0.1,
    #     decode_responses=False,
    #     password="redis",
    # )
    global sentinel_async
    db_master = await wait_for_master(sentinel_async, 'payment-master')
    db_slave = sentinel_async.slave_for('payment-master', decode_responses=False)
    logging.info("Connected to Redis Sentinel.")



# changed.. now saga master is used for idempotency as centralized client
# Read connection details from environment variables

# idempotency_redis_db = int(os.environ.get('IDEMPOTENCY_REDIS_DB', 0))
# idempotency_redis_client = sentinel_async.master_for(
#     "saga-master",
#     decode_responses=False,
#     db=idempotency_redis_db
# )
#
# logging.info("Connected to idempotency (Payment) Redis.")


async def close_db_connection():
    await db_master.close()
    await db_slave.close()
    await close_all_redis_clients()
    # await idempotency_redis_client.close()


class UserValue(Struct):
    credit: int

async def warmup_redis_connections():
    for _ in range(3):  # Create a small pool of ready connections
        try:
            client = await wait_for_master(sentinel_async, 'payment-master')
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

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def get_user_from_db(user_id: str) -> Tuple[Union[UserValue, Dict], int]:
    print(f"--- PAYMENT: get_user_from_db: ENTERED for user_id={user_id}")
    try:
        client = await get_db_slave_payment()
        entry = await client.get(user_id)
        # entry: Optional[bytes] = await db_slave.get(user_id)
    except redis.exceptions.RedisError as redis_err:
        logging.error(f"Redis GET ERROR: {redis_err}")
        return {"error": DB_ERROR_STR}, 500
    if entry is None:
        logging.error(f"User {user_id} NOT FOUND")
        return {"error": f"User: {user_id} not found!"}, 404
    try:
        user_entry: UserValue = msgpack.decode(entry, type=UserValue)
        print(f"--- PAYMENT: get_user_from_db: Decode SUCCESS")
        return user_entry, 200
    except (MsgspecDecodeError, TypeError) as decode_err:
        print(f"--- PAYMENT: get_user_from_db: Decode FAILED: {decode_err}")
        return {"error": "Internal data format error"}, 500


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def atomic_update_user(user_id: str, update_func):
    # retry? backoff/?
    # for attempts i tn range(max_retries):
    max_retries = 10
    base_backoff = 0.1

    # async with db.pipeline(transaction=True) as pipe: # async pipeline, should probably be used TODO

    # pipe = db_master.pipeline(transaction=True)
    try:
        client= await get_db_master_payment()
        # db_master = await wait_for_master(sentinel_async, 'payment-master')
        async with client.pipeline(transaction=True) as pipe:
            # idempotency check?
            # if ?
            # max retry attempts
            # watche exec pipeline

            # await pipe.watch(user_id)
            await pipe.watch(user_id)

            # bytes/?
            entry = await pipe.get(user_id)  # noawait

            # entry = await pipe.get(user_id)
            if entry is None:
                await pipe.unwatch()  # quick reset before returning
                logging.warning("Atomic update: User: %s", user_id)
                return None, f"User {user_id} not found"

            # disistuingish between leader and follower??
            # db_master.pipeline()?
            try:
                user_val: UserValue = msgpack.Decoder(UserValue).decode(entry)
                await pipe.unwatch()
            except MsgspecDecodeError:
                await pipe.unwatch()
                logging.error("Atomic update: Decode error for user %s", user_id)
                return None, "Internal data format error"

            try:
                updated_user = update_func(user_val)
                # synchronous
                # error?
            except ValueError as e:
                await pipe.unwatch()
                raise e
            except Exception as e:
                await pipe.unwatch()
                return None, f"Unexpected error in atomic_update_user for user {user_id}: {e}"

            pipe.multi()
            pipe.set(user_id, msgpack.Encoder().encode(updated_user))

            results = await pipe.execute()  # execute if no other client modified user_id (pipe.watch)
            return updated_user, None  # succesfull


    except WatchError:
        # key was modified between watch and execute, retry backoff
        # backoff?? TODO
        logging.warning(f"WatchError: key was modified between watch and execute, retry backoff")
        # backoff_multiplier = (2 ** attempt) * (1 + random.random() * 0.1)
        # backoff = base_backoff * backoff_multiplier
        raise e

        # await asyncio.sleep(backoff / 1000)
    except redis.RedisError:
        return None, DB_ERROR_STR

    except Exception as e:
        logging.exception(f"Unexpected error in atomic_update_user for user {user_id}: {e}")
        return None, "Internal data error"


    return None, f"Failed to update user, retries nr??."


# For testing, had endpoint "user_id" couldnt be found using POSTMAN
def require_user_id(data):
    uid = data.get("user_id")
    if not uid:
        return None, ({"error": "Missing 'user_id' in payload"}, 400)
    return str(uid).strip()


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
@worker.register
@idempotent('create_user', get_idempotency_redis_client(), SERVICE_NAME, False)  # not sure if idempotent or not TODO
async def create_user(data, message):
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        client= await get_db_master_payment()
        await client.set(key, value)
        return {'user_id': key}, 200
    except redis.exceptions.RedisError:
        return {"error": DB_ERROR_STR}, 400
    except Exception as e:
        logging.exception("Error creating user: %s", e)
        return {"error": f"Error creating user: {e}"}, 400


@worker.register
async def batch_init_users(data, message):
    n = int(data['n'])
    starting_money = int(data['starting_money'])
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        client= await get_db_master_payment()
        await client.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return {"error": DB_ERROR_STR}, 400
    return {"msg": "Batch init for users successful"}, 200

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
@worker.register
async def find_user(data, message):
    try:
        user_id = data.get("user_id")
        user_entry, status_code = await get_user_from_db(user_id)
        if status_code != 200:
            return {"error": f"Error retrieving user from DB for id {user_id}: {status_code}"}, status_code

        user_entry2 = user_entry  # to avoid status code error
        response = {"user_id": user_id, "credit": user_entry2.credit}
        logging.debug(f"Found user {user_id} with credit {user_entry2.credit}")
        return response, 200
    except redis.RedisError as e:
        logging.error(f"Redis error in find_user: {e}")
        return {"error": "Database temporarily unavailable"}, 500
    except Exception as e:
        logging.exception("Error retrieving user from DB for id %s: %s", user_id, e)
        return {"error": f"Error retrieving user from DB for id {user_id}: {e}"}, 500

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
@worker.register
@idempotent('add_funds', get_idempotency_redis_client(), SERVICE_NAME, False)
async def add_funds(data, message) -> IdempotencyResultTuple:
    user_id = require_user_id(data)
    amount = int(data.get("amount", 0))

    if amount <= 0:
        return {"error": "Transaction amount must be positive"}, 400

    def updater(user: UserValue) -> UserValue:
        user.credit += amount
        return user

    updated_user, error_msg = await atomic_update_user(user_id, updater)

    if error_msg:
        logging.error(f"Failed to add funds for user {user_id}: {error_msg}")
        return {"error": f"Failed to add funds: {error_msg}"}, 500

    if updated_user:
        return {"done": True, "credit": updated_user.credit}, 200
    else:
        return {"error": "Failed to update user, unknown reason"}, 500

@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
@worker.register
@idempotent('pay', get_idempotency_redis_client(), SERVICE_NAME, False)
async def pay(data, message) -> IdempotencyResultTuple:
    user_id = data.get("user_id")
    amount = data.get("amount")

    if not all([user_id, amount is not None]):
        logging.error("Missing required fields (user_id amount)")
        return {"error": "Missing required fields"}, 400
    try:
        amount_int = int(amount)
        if amount_int <= 0:
            return {"error": "Transaction amount must be positive"}, 400
    except (ValueError, TypeError):
        logging.error("Unexpected error, amount:.", amount)
        return {"error": "Invalid amount?"}, 400

    def updater(user: UserValue) -> UserValue:
        if user.credit < int(amount):
            raise ValueError(f"User {user_id} has insufficient credit")
        user.credit -= int(amount)
        return user

    updated_user = None
    error_msg = None
    response_tuple = None

    try:
        logging.info(f"--- PAYMENT PAY HANDLER: Calling atomic_update_user for user {user_id}")
        updated_user, error_msg = await atomic_update_user(user_id, updater)

        if updated_user:
            response_tuple = ({"paid": True, "credit": updated_user.credit}, 200)
        elif error_msg:
            status_code = 400 if "insufficient credit" in error_msg.lower() else (
                404 if "not found" in error_msg.lower() else 500)
            response_tuple = ({"paid": False, "error": error_msg}, status_code)
        else:
            response_tuple = ({"paid": False, "error": "Internal processing error"}, 500)

        logging.info(f"--- PAYMENT PAY HANDLER: Returning response tuple: {response_tuple}")
        return response_tuple

    except ValueError as e:
        response_tuple = ({"paid": False, "error": str(e)}, 400 if "insufficient credit" in str(e).lower() else 400)
        logging.error(f"--- PAYMENT PAY HANDLER: Caught ValueError: {e}, returning: {response_tuple}")
        return response_tuple
    except Exception as e:
        response_tuple = ({"paid": False, "internal error": str(e)}, 500)
        logging.exception(f"--- PAYMENT PAY HANDLER: Caught Exception: {e}, returning: {response_tuple}")
        return response_tuple


@retry(
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
@worker.register
@idempotent('remove_credit', get_idempotency_redis_client(), SERVICE_NAME, False)
async def remove_credit(data, message) -> IdempotencyResultTuple:
    logging.info(f"[PAYMENT] Starting remove_credit operation")

    user_id = data.get("user_id")
    raw_amount = data.get("amount")
    saga_id = data.get("saga_id")
    order_id = data.get("order_id")
    callback_action = data.get("callback_action")

    correlation_id = getattr(message, "correlation_id", "unknown")
    logging.info(
        f"[PAYMENT] Processing remove_credit: user_id={user_id}, amount={raw_amount}, saga_id={saga_id}, order_id={order_id}")
    logging.debug(f"[PAYMENT] Correlation ID: {correlation_id}, Callback action: {callback_action}")
    logging.debug(f"[PAYMENT] Full request data: {data}")

    try:
        logging.debug(f"[PAYMENT] Converting amount '{raw_amount}' to integer")
        amount = int(raw_amount)
        logging.debug(f"[PAYMENT] Amount conversion successful: {amount}")
    except (ValueError, TypeError) as e:
        error_msg = f"Invalid amount '{raw_amount}'"
        logging.error(f"[PAYMENT] {error_msg}: {str(e)}")
        logging.debug(f"[PAYMENT] Amount type: {type(raw_amount)}")
        return {"error": error_msg}, 400

    if not user_id:
        logging.error("[PAYMENT] Missing required field: user_id")
        return {"error": "Invalid request (missing user_id or amount <= 0)"}, 400

    if amount <= 0:
        logging.error(f"[PAYMENT] Invalid amount: {amount} <= 0")
        return {"error": "Invalid request (missing user_id or amount <= 0)"}, 400

    logging.info(f"[PAYMENT] Validation passed for user_id={user_id}, amount={amount}")

    def updater(user: UserValue) -> UserValue:
        logging.debug(f"[PAYMENT] User before update: user_id={user_id}, credit={user.credit}")
        if user.credit < amount:
            logging.warning(f"[PAYMENT] Insufficient funds: user_id={user_id}, credit={user.credit}, amount={amount}")
            raise ValueError("Insufficient funds")

        old_credit = user.credit
        user.credit -= amount
        logging.info(
            f"[PAYMENT] Credit updated: user_id={user_id}, old_credit={old_credit}, new_credit={user.credit}, amount_deducted={amount}")
        return user

    logging.debug(f"[PAYMENT] Calling atomic_update_user for user_id={user_id}")
    start_time = time.time()
    updated_user, error_msg = await atomic_update_user(user_id, updater)
    execution_time = time.time() - start_time
    logging.debug(f"[PAYMENT] atomic_update_user completed in {execution_time:.3f}s")

    if error_msg:
        logging.error(f"[PAYMENT] Failed to update user: {error_msg}")

        if saga_id and order_id:
            logging.info(f"[PAYMENT] Preparing failure notification for saga_id={saga_id}, order_id={order_id}")
            try:
                reply_to_queue = getattr(message, "reply_to", "orchestrator_queue")
                if not reply_to_queue:
                    reply_to_queue = "orchestrator_queue"
                    logging.warning(f"[PAYMENT] Reply to queue not provided, using default: {reply_to_queue}")
                else:
                    logging.info(f"[PAYMENT] Reply to queue provided: {reply_to_queue}")

                logging.debug(f"[PAYMENT] Constructing failure payload for saga_id={saga_id}")
                failure_payload = {
                    "saga_id": saga_id,
                    "order_id": order_id,
                    "error": error_msg,
                    "success": False,
                    "callback_action": callback_action or "process_payment_completed"
                }
                logging.debug(f"[PAYMENT] Failure payload: {failure_payload}")

                logging.info(f"[PAYMENT] Sending failure message to queue={reply_to_queue}, correlation_id={saga_id}")
                await worker.send_message(
                    payload=failure_payload,
                    queue=reply_to_queue,
                    correlation_id=saga_id,
                    action="process_payment_completed",
                    callback_action="process_payment_completed",
                    reply_to=reply_to_queue
                )
                logging.info(f"[PAYMENT] Sent payment failure notification for saga {saga_id}")
            except Exception as e:
                logging.error(f"[PAYMENT] Failed to send failure notification: {str(e)}", exc_info=True)

        logging.debug(f"[PAYMENT] Returning error response with status code 400")
        return ({"error": error_msg}, 400)

    if updated_user:
        logging.info(f"[PAYMENT] Successfully updated user_id={user_id}, new_credit={updated_user.credit}")

        if saga_id and order_id and getattr(message, "reply_to", None):
            try:
                reply_to_queue = message.reply_to
                logging.info(f"[PAYMENT] Attempting direct success reply to {reply_to_queue} for saga {saga_id}")

                direct_success_payload = {
                    "saga_id": saga_id,
                    "order_id": order_id,
                    "success": True,
                    "credit": updated_user.credit,
                    "callback_action": callback_action or "process_payment_completed"
                }
                logging.debug(f"[PAYMENT] Direct success payload: {direct_success_payload}")

                logging.info(
                    f"[PAYMENT] Sending direct success message to queue={reply_to_queue}, correlation_id={saga_id}")
                await worker.send_message(
                    payload=direct_success_payload,
                    queue=reply_to_queue,
                    correlation_id=saga_id,
                    action="process_payment_completed",
                    callback_action="process_payment_completed",
                    reply_to=None
                )
                logging.info(f"[PAYMENT] Sent direct payment completion notification for saga {saga_id}")
            except Exception as e:
                logging.error(f"[PAYMENT] Failed to send direct success notification: {str(e)}", exc_info=True)

        logging.debug(f"[PAYMENT] Constructing success response")
        success_data = {
            "message": f"Removed {amount} from user {user_id}",
            "credit": updated_user.credit,
            "saga_id": saga_id,
            "order_id": order_id,
            "callback_action": callback_action or "process_payment_completed",
            "success": True
        }
        logging.info(f"[PAYMENT] Operation completed successfully for user_id={user_id}, saga_id={saga_id}")
        return (success_data, 200)
    else:
        logging.error(f"[PAYMENT] Failed to update user {user_id}, unknown reason")
        error_data = {"error": "Failed to update user, unknown reason"}
        if saga_id:
            error_data["saga_id"] = saga_id
            logging.debug(f"[PAYMENT] Added saga_id={saga_id} to error response")
        if order_id:
            error_data["order_id"] = order_id
            logging.debug(f"[PAYMENT] Added order_id={order_id} to error response")

        logging.debug(f"[PAYMENT] Returning error response with status code 500: {error_data}")
        return (error_data, 500)


@worker.register
@idempotent('compensate', get_idempotency_redis_client(), SERVICE_NAME, False)
async def compensate(data, message):
    try:
        user_id = data.get("user_id")
        amount = data.get("amount")
        saga_id = data.get("saga_id")
        order_id = data.get("order_id")

        if not all([user_id, amount is not None]):
            logging.error("Missing user_id or amount in cancel_payment request.")
            return {"error": "Missing required fields"}, 400

        if int(amount) <= 0:
            return {"error": "Transaction amount must be positive"}, 400

    except (ValueError, TypeError):
        return {"error": "Invalid amount specified"}, 400

    def updater(user: UserValue) -> UserValue:
        user.credit += amount
        return user

    try:
        updated_user, error_msg = await atomic_update_user(user_id, updater)
        if error_msg:
            logging.error(f"Failed to cancel payment (refund) for user {user_id}: {error_msg}")
            status_code = 404 if "not found" in error_msg.lower() else 500
            return {"error": f"Failed to cancel payment: {error_msg}"}, status_code

        if updated_user:
            if saga_id and order_id:
                try:
                    await worker.send_message(
                        payload={
                            "saga_id": saga_id,
                            "order_id": order_id,
                            "success": True,
                            "message": f"Successfully refunded {amount} to user {user_id}",
                            "callback_action": "process_compensation_completed"
                        },
                        queue="orchestrator_queue",
                        correlation_id=saga_id,
                        action="process_compensation_completed",
                        callback_action="process_compensation_completed"
                    )
                    logging.info(f"[Payment] Sent compensation completion notification for saga {saga_id}")
                except Exception as e:
                    logging.error(f"[Payment] Failed to send compensation notification: {e}")

            return {"refunded": True, "credit": updated_user.credit}, 200
        else:
            logging.error(f"Cancel payment failed for user {user_id}, unknown reason .")
            return {"error": "Failed cancellation, unknown reason"}, 500
    except Exception as e:
        logging.exception("Error canceling payment for user %s: %s", user_id, e)
        return {"error": f"Error canceling payment for user {user_id}: {e}"}, 400

@worker.register
async def health_check(data, message):
    return {"status": "ok"}, 200
async def main():
    try:
        await warmup_redis_connections()
        await initialize_redis()
        await worker.start()
        await close_all_redis_clients()
    finally:
        await close_idempotency_redis_client()
        await close_db_connection()


if __name__ == '__main__':
    logging.info("Starting payment service AMQP worker...")
    asyncio.run(main())
