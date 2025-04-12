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

sentinel_async = Sentinel([
    (os.environ['REDIS_SENTINEL_1'], 26379),
    (os.environ['REDIS_SENTINEL_2'], 26379),
    (os.environ['REDIS_SENTINEL_3'], 26379)],
    socket_timeout=10, # TODO check if this is the right value, potentially lower it
    socket_connect_timeout=5,
    socket_keepalive=True,
    password=os.environ['REDIS_PASSWORD'],
    decode_responses=False
)

db_master=sentinel_async.master_for('stock-master',  decode_responses=False)
db_slave=sentinel_async.slave_for('stock-master',  decode_responses=False)

#no slave since idempotency is critical to payment service
idempotency_db_conn = db_master 

logger.info("Connected to idempotency (Stock) Redis.")


def close_db_connection():
    db_master.close()
    db_slave.close()
    idempotency_db_conn.close()

atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int

async def get_item_from_db(item_id: str) -> Tuple[Union[StockValue, str], int]:
    # get serialized data
    print(f"--- STOCK: get_item_from_db - Getting item_id={item_id} from DB ---")
    logger.info(f"--- STOCK: get_item_from_db - Getting item_id={item_id} from DB ---")
    try:
        entry: bytes = await db_slave.get(item_id)
    except redis.exceptions.RedisError:
        return DB_ERROR_STR, 400
    print(f"--- STOCK: get_item_from_db - Decoded entry type={type(entry)}, value='{str(entry)[:100]}...' ---")
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


async def atomic_update_item(item_id: str, update_func):
    max_retries = 5
    base_backoff = 50 

    for attempt in range(max_retries):
        # await pipe = db_master.pipeline(transaction=True)
        try:
            async with db_master.pipeline(transaction=True) as pipe:
            
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
                
                
        except WatchError:
            #key was modified between watch and execute, retry backoff 
            backoff_multiplier = (2 ** attempt) * (1 + random.random() * 0.1)
            backoff = base_backoff * backoff_multiplier
            
            await asyncio.sleep(backoff / 1000)
            continue #loop again

        except redis.RedisError:
            return None, DB_ERROR_STR
        except ValueError:
            raise
        
        except Exception as e:
            logger.exception(f"Unexpected error in atomic_update_item for item {item_id}: {e}")
            return None, "Internal data error"
            
    return None, f"Failed to update item, retries nr??."

@worker.register
async def create_item(data, message):
    price = data.get("price")
    try:
        key = str(uuid.uuid4())
        logger.debug(f"Item: {key} created")
        value = msgpack.encode(StockValue(stock=0, price=int(price)))
        await db_master.set(key, value)
        return {'item_id': key}, 200
    
    except redis.exceptions.RedisError as re:
        return {'error': f"Redis Error: {re}"}, 500
    except Exception as e:
        return {'error': f"Unexpected error: {str(e)}"}, 500

@worker.register
async def batch_init_stock(data, message):
    n = int(data.get("n"))
    starting_stock = int(data.get("starting_stock"))
    item_price = int(data.get("item_price"))
    kv_pairs: dict[str, bytes] = {f"{str(uuid.uuid4())}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        await db_master.mset(kv_pairs)
    except redis.exceptions.RedisError as re:
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
@idempotent('add_stock', idempotency_db_conn, SERVICE_NAME)
async def add_stock(data, message):
    item_id = data.get("item_id")
    amount = data.get("amount")

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
        updated_item, error_msg = await atomic_update_item(item_id, updater)
    
        if error_msg:
            logger.error(f"'add_stock' logic failed: {error_msg}")
            logger.debug(f"error_msg: {error_msg}")
            if "not found" in error_msg.lower(): 
                status_code = 404
            else: 
                status_code = 500
            return ({"added": False, "error": error_msg}, status_code)
            
        elif updated_item:
            logger.info(f"Item: {item_id} stock updated to: {updated_item.stock}")
            return ({"added": True, "stock": updated_item.stock}, 200)

        else:
            #fallback
            return ({"added": False, "error": "Internal processing error"}, 500)
        
    except ValueError as e:
        error_msg = str(e)
        return ({"paid": False, "value error": error_msg}, 400)
    except Exception as e:
        logger.exception("internal error")
        return ({"paid": False, "internal error": error_msg}, 400)

@worker.register
@idempotent('remove_stock', idempotency_db_conn, SERVICE_NAME)
async def remove_stock(data, message):
    item_id = data.get("item_id")
    amount = data.get("amount")

    if not all([item_id, amount is not None]):
        logger.error("Missing item_id or amount in remove_stock request.")
        return {"error": "Missing required fields"}, 400
        
    try:
        if int(amount) <= 0:
            return {"error": "Transaction amount must be positive"}, 400
        
    except (ValueError, TypeError):
        return {"error": "Invalid amount specified"}, 400
    
    def updater(item: StockValue) -> StockValue:
        if item.stock < int(amount):
            raise ValueError(f"Error, Insufficient stock for item {item_id} (available: {item.stock}, requested: {int(amount)})")
        item.stock -= int(amount)
        return item
    
    try:
        updated_item, error_msg = await atomic_update_item(item_id, updater)
        if error_msg:
            logger.error(f"Failed to cancel stock (refund) for user {item_id}: {error_msg}")
            status_code = 404 if "not found" in error_msg.lower() else 500
            return {"error": f"Failed to cancel stock: {error_msg}"}, status_code
        if updated_item:
            return {"removed": True, "stock": updated_item.stock}, 200
        else:
            logger.error(f"Cancel stock failed for user {item_id}, unknown reason .")
            return {"error": "Failed stock cancellation, unknown reason"}, 500
    except ValueError as e:
        logger.warning(f"Error:Insufficient stock for user {item_id}: {e}")
        return {"error": f"Error:Insufficient stock for user {item_id}: {e}"}, 400
    except Exception as e:
        logger.exception("Error canceling stock for user %s: %s", item_id, e)
        return {"error": f"Error canceling stock for user {item_id}: {e}"}, 500
        
if __name__ == '__main__':
    asyncio.run(worker.start())
