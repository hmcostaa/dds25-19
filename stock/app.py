import logging
import os
import atexit
import time
import uuid
import asyncio
import random
from typing import Tuple, Dict, Any, Union 
from common.amqp_worker import AMQPWorker
from msgspec import msgpack, Struct, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError
from flask import Flask, jsonify, abort, Response
import redis.asyncio as redis
from redis.asyncio.sentinel import Sentinel
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
import redis.exceptions 
from redis.exceptions import WatchError, RedisError
from global_idempotency.idempotency_decorator import idempotent

DB_ERROR_STR = "DB error"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

logger = logging.getLogger("stock-service")

SERVICE_NAME = "stock"

worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="stock_queue",
)
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

sentinel_async = Sentinel([
    (os.environ['REDIS_SENTINEL_1'], 26379),
    (os.environ['REDIS_SENTINEL_2'], 26379),
    (os.environ['REDIS_SENTINEL_3'], 26379)],
    socket_timeout=10, # TODO check if this is the right value, potentially lower it
    socket_connect_timeout=10,
    socket_keepalive=True,
    password=os.environ['REDIS_PASSWORD'],
    retry_on_timeout=True,
    decode_responses=False
)
db_master_stock_cached=None
db_slave_stock_cached=None
# db_master=sentinel_async.master_for('stock-master',  decode_responses=False)
# db_slave=sentinel_async.slave_for('stock-master',  decode_responses=False)
async def initialize_redis():
    global db_master, db_slave
    # sentinel_async = Sentinel(
    #     [('redis-sentinel-1', 26379), ('redis-sentinel-2', 26379), ('redis-sentinel-3', 26379)],
    #     socket_timeout=0.1,
    #     decode_responses=False,
    #     password="redis",
    # )
    global sentinel_async
    db_master = await wait_for_master(sentinel_async, 'stock-master')
    db_slave = sentinel_async.slave_for('stock-master', decode_responses=False)
    logging.info("Connected to Redis Sentinel.")

async def get_db_master_stock():
    global db_master_stock_cached
    try:
        if db_master_stock_cached is not None:
            try:
                await asyncio.wait_for(db_master_stock_cached.ping(), timeout=2.0)
            except (redis.RedisError, asyncio.TimeoutError):
                logging.warning("Stock master connection unhealthy - resetting")
                db_master_stock_cached = None
        if db_master_stock_cached is None:
            db_master_stock_cached = await wait_for_master(sentinel_async, 'stock-master')
            await db_master_stock_cached.ping()
        return db_master_stock_cached
    except redis.RedisError as e:
        logging.error(f"Redis error getting master: {e}")
        db_master_stock_cached = None
        await handle_redis_connection_failure(e)
        raise
async def get_db_slave_stock():
    global db_slave_stock_cached
    try:
        if db_slave_stock_cached is not None:
            try:
                await asyncio.wait_for(db_slave_stock_cached.ping(), timeout=2.0)
            except (redis.RedisError, asyncio.TimeoutError):
                logging.warning("Stock master connection unhealthy - resetting")
                db_slave_stock_cached = None
        if db_slave_stock_cached is None:
            db_slave_stock_cached = await wait_for_slave(sentinel_async, 'stock-master')
            await db_slave_stock_cached.ping()
        return db_slave_stock_cached
    except redis.RedisError as e:
        logging.error(f"Redis error getting master: {e}")
        db_slave_stock_cached = None
        await handle_redis_connection_failure(e)
        raise
@retry(
    stop=stop_after_attempt(15),
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
#changed.. now saga master is used for idempotency as centralized client
# Read connection details from environment variables
#
# idempotency_redis_db = int(os.environ.get('IDEMPOTENCY_REDIS_DB', 0))
# idempotency_redis_client = sentinel_async.master_for(
#     "saga-master",
#     decode_responses=False,
#     db=idempotency_redis_db
# )
#
# logger.info("Connected to idempotency (Stock) Redis.")
async def warmup_redis_connections():
    for _ in range(3):  # Create a small pool of ready connections
        try:
            client = await wait_for_master(sentinel_async, 'stock-master')
            await client.ping()
        except Exception as e:
            logging.warning(f"Connection warmup error (non-fatal): {e}")
    logging.info("Redis connection pool pre-warmed")
_idempotency_redis_client = None

@retry(
    stop=stop_after_attempt(15),
    wait=wait_exponential(multiplier=0.2, min=0.2, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
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

async def close_db_connection():
    await db_master.close()
    await db_slave.close()
    await close_all_redis_clients()
    # await idempotency_redis_client.close()

RETRYABLE_REDIS_EXCEPTIONS = (
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    redis.exceptions.BusyLoadingError,
    redis.exceptions.ReadOnlyError,
    redis.exceptions.WatchError
)
async def handle_redis_connection_failure(e):
    global db_master_stock_cached,db_slave_stock_cached
    logging.warning(f"Redis connection failure detected: {str(e)} - Resetting connections")
    db_master_stock_cached = None
    db_slave_stock_cached=None


async def close_all_redis_clients():
    global _active_redis_clients
    for client in _active_redis_clients:
        try:
            await client.aclose()
        except Exception as e:
            logging.warning(f"Failed to close Redis client: {e}")
    _active_redis_clients.clear()
class StockValue(Struct):
    stock: int
    price: int
@retry(
    stop=stop_after_attempt(15),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=2),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def get_item_from_db(item_id: str) -> Tuple[Union[StockValue, str], int]:
    # get serialized data
    logger.info(f"--- STOCK: get_item_from_db - Getting item_id={item_id} from DB ---")
    try:
        client = await get_db_slave_stock()
        entry = await client.get(item_id)
    except redis.RedisError:
        return DB_ERROR_STR, 400
    logger.info(f"--- STOCK: get_item_from_db - Decoded entry type={type(entry)}, value='{str(entry)[:100]}...' ---")
    # deserialize data if it exists else return null
    if entry is None:
         return {"error": f"Item: {item_id} not found!"}, 404
    try:
         logger.info(f"--- STOCK: get_item_from_db - Attempting msgpack.decode...")
         item_entry: StockValue = msgpack.decode(entry, type=StockValue)
         logger.info(f"--- STOCK: get_item_from_db - Decode SUCCESS")
         return item_entry, 200
    except (MsgspecDecodeError, TypeError) as decode_err:
         logger.error(f"Decode error for item {item_id}: {decode_err}")
         return {"error": "Internal data format error"}, 500
    except Exception as e:
         logger.error(f"Unexpected error processing item {item_id}: {e}", exc_info=True)
         return {"error": "Unexpected processing error"}, 500

@retry(
    stop=stop_after_attempt(15),
    wait=wait_exponential(multiplier=0.25, min=0.25, max=4),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def call_atomic_update_with_retry(item_id: str, updater_func):
    logger.debug(f"[_call_atomic_update_with_retry] Attempting atomic_update_item for {item_id}")
    updated_item, error_msg = await atomic_update_item(item_id, updater_func)

    if error_msg:
        if "not found" in error_msg.lower() or "internal data format error" in error_msg.lower():
            logger.warning(f"[_call_atomic_update_with_retry] Non-retryable: {error_msg}")
            raise ValueError(error_msg)
        else:
            logger.error(f"[_call_atomic_update_with_retry] Raising retryable error for message: {error_msg}")
            raise redis.exceptions.ConnectionError(f"Atomic update failed internally: {error_msg}")

    if not updated_item:
        logger.error("[_call_atomic_update_with_retry] Atomic update returned None, None unexpectedly.")
        raise redis.exceptions.ConnectionError("Unknown atomic_update_item failure")

    logger.debug(f"[_call_atomic_update_with_retry] Atomic update successful for {item_id}")
    return updated_item

@retry(
    stop=stop_after_attempt(15),
    wait=wait_exponential(multiplier=0.25, min=0.25, max=4),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
async def atomic_update_item(item_id: str, update_func):
    # max_retries = 5
    # base_backoff = 50
    #
    #
        # await pipe = db_master.pipeline(transaction=True)
    try:
            client=await get_db_master_stock()
            # client = await wait_for_master(sentinel_async, 'stock-master')
            async with client.pipeline(transaction=True) as pipe:
            
                await pipe.watch(item_id)

                entry = await pipe.get(item_id) #noawait

                if entry is None:
                        await pipe.unwatch()# quick reset before returning
                        logger.warning("Atomic update: item: %s", item_id)
                        return None, f"item {item_id} not found"
                
                try:
                    item_val: StockValue = msgpack.Decoder(StockValue).decode(entry)
                    await pipe.unwatch()
                except MsgspecDecodeError:
                    # pipe.re
                    logger.error("Atomic update: Decode error for item %s", item_id)
                    return None, "Internal data format error"
                
                try:
                    updated_item = update_func(item_val)
                
                except ValueError as e:
                    await pipe.unwatch()
                    raise e
                except Exception as e:
                    await pipe.unwatch()
                    return None, f"Unexpected error in atomic_update_item for item {item_id}: {e}"
                
                pipe.multi()
                pipe.set(item_id, msgpack.Encoder().encode(updated_item))

                results = await pipe.execute() #execute if no other client modified item_id (pipe.watch)
                return updated_item, None #succesfull
    except redis.exceptions.WatchError as e:
        raise e
    except redis.RedisError:
            return None, DB_ERROR_STR
    except ValueError:
            raise
        
    except Exception as e:
            logger.exception(f"Unexpected error in atomic_update_item for item {item_id}: {e}")
            return None, "Internal data error"
            
    return None, f"Failed to update item, retries nr??."

@retry(
    stop=stop_after_attempt(15),
    wait=wait_exponential(multiplier=0.2, min=0.1, max=15),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
@worker.register
@idempotent('create_item', get_idempotency_redis_client(), SERVICE_NAME) #not sure if idempotent or not TODO
async def create_item(data, message):
    price = data.get("price")
    try:
        key = str(uuid.uuid4())
        logger.debug(f"Item: {key} created")
        item = StockValue(stock=0, price=int(price))
        value = msgpack.encode(item)
        client=await get_db_master_stock()
        await client.set(key, value)
        return {'item_id': key, 'stock': item.stock, 'price': item.price}, 201
    
    except redis.RedisError as re:
        return {'error': f"Redis Error: {re}"}, 500
    except Exception as e:
        return {'error': f"Unexpected error: {str(e)}"}, 500

@worker.register
async def batch_init_stock(data, message):
    n = int(data.get("n"))
    starting_stock = int(data.get("starting_stock"))
    item_price = int(data.get("item_price"))
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
        for i in range(n)
    }
    try:
        client=await get_db_master_stock()
        await client.mset(kv_pairs)
    except redis.RedisError as re:
        return {'error': str(re)}, 500
    except Exception as e:
        return {'error': f"Unexpected error: {str(e)}"}, 500

    return {'msg': f"{kv_pairs.keys()}"}, 200


@worker.register
async def find_item(data, message):
    print(f"--- STOCK: find_item - Received data={data} ---")
    item_id = data.get("item_id")
    item_entry, response_code = await get_item_from_db(item_id)
    if response_code != 200:
        return item_entry, response_code

    return {"stock": item_entry.stock,
            "price": item_entry.price } , 200

@worker.register
@idempotent('add_stock', get_idempotency_redis_client(), SERVICE_NAME)
async def add_stock(data, message):
    item_id = data.get("item_id")
    amount = data.get("amount")
    # idempotency_key = data.get("idempotency_key")
    if not all([item_id, amount is not None]):
        return {"error": "Missing required fields (item_id, amount)"}, 400
    
    try:
        amount_int = int(amount)
        if amount_int <= 0:
            return {"error": "Transaction amount must be positive"}, 400
    except (ValueError, TypeError):
        logger.error("Unexpected error, amount:.", amount)
        return {"error": "Invalid amount?"}, 400
    
    
    def updater(item: StockValue) -> StockValue:
        item.stock += amount_int
        return item
    
    updated_item = None
    error_msg = None

    try:
        updated_item = await call_atomic_update_with_retry(item_id, updater)
    
        logger.info(f"updated_item: {updated_item}")
        return ({"added": True, "stock": updated_item.stock}, 200)
    
    except RetryError as e:
        logger.error(f"[add_stock] Operation failed after multiple retries for item {item_id}: {e}")
        return ({"added": False, "error": f"Service temporarily unavailable after retries: {getattr(e, 'cause', e)}"}, 503)

        
    except ValueError as e:
        error_msg = str(e)
        return ({"paid": False, "value error": error_msg}, 400)
    except Exception as e:
        logger.exception("internal error")
        return ({"paid": False, "internal error": error_msg}, 400)

@retry(
    stop=stop_after_attempt(15),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(RETRYABLE_REDIS_EXCEPTIONS),
    reraise=True
)
@worker.register
@idempotent('remove_stock', get_idempotency_redis_client(), SERVICE_NAME)
async def remove_stock(data, message):
    item_id = data.get("item_id")
    amount = data.get("amount")
    saga_id = data.get("saga_id")
    order_id = data.get("order_id")
    callback_action = data.get("callback_action")
    debug_info = {
        "item_id": item_id,
        "amount": amount,
        "saga_id": saga_id,
        "order_id": order_id,
        "callback_action": callback_action,
    }

    if message:
        if hasattr(message, 'reply_to'):
            debug_info["message_reply_to"] = message.reply_to
        if hasattr(message, 'headers'):
            debug_info["message_headers"] = message.headers
        if hasattr(message, 'correlation_id'):
            debug_info["message_correlation_id"] = message.correlation_id

    logger.info(f"[Stock Debug Info] remove_stock called with: {debug_info}")

    if not all([item_id, amount is not None]):
        error_response = {"error": "Missing required fields (item_id, amount)"}
        if saga_id:
            error_response["saga_id"] = saga_id
            error_response["order_id"] = order_id
            error_response["callback_action"] = callback_action or "process_stock_completed"
        status_code = 400
        logging.warning(f"[remove_stock:RETURN] Returning error1 tuple: ({error_response}, {status_code})") 
        return error_response, status_code

    try:
        amount_int = int(amount)
        if amount_int <= 0:
            error_response = {"error": "Transaction amount must be positive"}
            if saga_id:
                error_response["saga_id"] = saga_id
                error_response["order_id"] = order_id
                error_response["callback_action"] = callback_action or "process_stock_completed"
            status_code = 400
            logging.warning(f"[remove_stock:RETURN] Returning error2 tuple: ({error_response}, {status_code})")
            return error_response, status_code
    except (ValueError, TypeError):
        error_response = {"error": "Invalid amount specified"}
        if saga_id:
            error_response["saga_id"] = saga_id
            error_response["order_id"] = order_id
            error_response["callback_action"] = callback_action or "process_stock_completed"
        status_code = 400
        logging.warning(f"[remove_stock:RETURN] Returning error1 tuple: ({error_response}, {status_code})")
        return error_response, status_code

    def updater(item: StockValue) -> StockValue:
        if item.stock < amount_int:
            raise ValueError(
                f"Insufficient stock for item {item_id} (available: {item.stock}, requested: {amount_int})")
        item.stock -= amount_int
        return item

    try:
        updated_item = await call_atomic_update_with_retry(item_id, updater)
        success_response = {
            "removed": True,
            "stock": updated_item.stock,
            "saga_id": saga_id,
            "order_id": order_id,
            "callback_action": "process_stock_completed"  # Always include this
        }
        if saga_id and message and hasattr(message, 'reply_to') and message.reply_to:
            logging.info(f"[Stock] Sending success callback to {message.reply_to} for saga {saga_id}")
            try:
                reply_to = message.reply_to
                for attempt in range(3):
                    logger.info(f"[Stock] Attempt #{attempt+1}")
                    logger.info(f"[Stock] Sending success callback to {reply_to} for saga {saga_id}")
                    logger.info(f"[Stock] Success response: {success_response}")
                    logger.info(f"[Stock] Correlation ID: {saga_id}")
                    try:
                        await worker.send_message(
                            payload=success_response,
                            queue=reply_to,
                            correlation_id=saga_id,
                            action="process_stock_completed",
                            callback_action="process_stock_completed",
                            reply_to=None
                        )
                        logger.info(f"[Stock] Success callback sent to {reply_to} (attempt {attempt + 1})")
                    except Exception as e:
                        logger.error(f"[Stock] Error sending callback (attempt {attempt + 1}): {e}")
                        await asyncio.sleep(0.1)
                        if attempt == 2:
                            raise
                        await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"[Stock] Failed to send callback after all attempts: {e}", exc_info=True)

        status_code = 200
        logging.warning(f"[remove_stock:RETURN] Returning success1 tuple: ({success_response}, {status_code})")
        return success_response, status_code
     

    except RetryError as e:
        logger.error(f"[remove_stock] Operation failed after multiple retries for item {item_id}: {e}")
        error_response = {"error": f"Service temporarily unavailable after retries: {getattr(e, 'cause', e)}"}
        status_code = 503
        if saga_id and message and hasattr(message, 'reply_to') and message.reply_to:
             error_response["saga_id"] = saga_id
             error_response["order_id"] = order_id
             error_response["callback_action"] = callback_action or "process_stock_completed"
             try:
                  logger.info(f"[Stock] Sending final failure (RetryError) callback to {message.reply_to}")
                  await worker.send_message(payload=error_response, queue=message.reply_to, correlation_id=saga_id, action="process_stock_completed", callback_action="process_stock_completed", reply_to=None)
             except Exception as cb_e:
                  logger.error(f"[Stock] Failed to send final failure callback: {cb_e}")
        return error_response, status_code
    
    except ValueError as e:

        error_message = str(e)
        error_response = {"error": error_message}
        if saga_id:
            error_response["saga_id"] = saga_id
            error_response["order_id"] = order_id
            error_response["callback_action"] = "process_stock_completed"


            if message and hasattr(message, 'reply_to') and message.reply_to:
                try:
                    logger.info(f"[Stock] Sending insufficient stock callback to {message.reply_to}")
                    await worker.send_message(
                        payload=error_response,
                        queue=message.reply_to,
                        correlation_id=saga_id,
                        action="process_stock_completed",
                        callback_action="process_stock_completed",
                        reply_to=None
                    )
                except Exception as e:
                    logger.error(f"[Stock] Failed to send insufficient stock callback: {e}")

        status_code = 400
        logging.warning(f"[remove_stock:RETURN] Returning error2 tuple: ({error_response}, {status_code})")
        return error_response, status_code

    except Exception as e:
        logger.exception(f"[Stock] Unexpected error in remove_stock: {e}")
        error_response = {"error": f"Unexpected error: {str(e)}"}
        if saga_id:
            error_response["saga_id"] = saga_id
            error_response["order_id"] = order_id
            error_response["callback_action"] = "process_stock_completed"
        status_code = 500
        logging.warning(f"[remove_stock:RETURN] Returning error3 tuple: ({error_response}, {status_code})")
        return error_response, status_code
@worker.register
async def health_check(data, message):

    return {"status": "ok"}, 200
async def main():
    try:
        await warmup_redis_connections()
        await initialize_redis()
        await worker.start()
    finally:
        await close_idempotency_redis_client()
        await close_db_connection()
if __name__ == '__main__':
    asyncio.run(main())
