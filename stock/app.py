import logging
import os
import atexit
import uuid
import asyncio
import random
from common.amqp_worker import AMQPWorker
import redis
from redis.exceptions import WatchError, RedisError
from msgspec import msgpack, Struct, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError
from flask import Flask, jsonify, abort, Response
from redis import Sentinel

from global_idempotency.idempotency_decorator import idempotent

DB_ERROR_STR = "DB error"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s"
)

logging = logging.getLogger("stock-service")

# from global_idempotency.app import (
#         IdempotencyStoreConnectionError,
#         IdempotencyDataError,
#         check_idempotent_operation,
#         store_idempotent_result,
#         IdempotencyResultTuple
#     )

SERVICE_NAME = "stock"

worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="stock_queue",
)


sentinel= Sentinel([
    (os.environ['REDIS_SENTINEL_1'],26379),
    (os.environ['REDIS_SENTINEL_2'],26380),
    (os.environ['REDIS_SENTINEL_3'],26381)], socket_timeout=0.1, password= os.environ['REDIS_PASSWORD'])

db_master=sentinel.master_for('stock-master', socket_timeout=0.1, decode_responses=False)
db_slave=sentinel.slave_for('stock-master', socket_timeout=0.1, decode_responses=False)

#no slave since idempotency is critical to payment service
idempotency_db_conn = db_master 

logging.info("Connected to idempotency (Stock) Redis.")


def close_db_connection():
    db_master.close()
    db_slave.close()
    idempotency_db_conn.close()

atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db_slave.get(item_id)
    except redis.exceptions.RedisError:
        return DB_ERROR_STR, 400
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        return f"Item: {item_id} not found!", 400
    return entry, 200

async def atomic_update_item(item_id: str, update_func):
    max_retries = 5
    base_backoff = 50 

    for attempt in range(max_retries):
        pipe = db_master.pipeline(transaction=True)
        try:
            pipe.watch(item_id)

            entry = pipe.get(item_id) #noawait

            if entry is None:
                    pipe.reset() # quick reset before returning
                    logging.warning("Atomic update: item: %s", item_id)
                    return None, f"item {item_id} not found"
            
            try:
                item_val: StockValue = msgpack.Decoder(StockValue).decode(entry)
            except MsgspecDecodeError:
                pipe.reset()
                logging.error("Atomic update: Decode error for item %s", item_id)
                return None, "Internal data format error"
            
            try:
                updated_item = update_func(item_val)
                pipe.multi()
                pipe.set(item_id, msgpack.Encoder().encode(updated_item))

                pipe.execute() #execute if no other client modified item_id (pipe.watch)
                return updated_item, None #succesfull
            except ValueError as e:
                pipe.reset()
                raise e
                
        except WatchError:
            #key was modified between watch and execute, retry backoff 
            backoff_multiplier = (2 ** attempt) * (1 + random.random() * 0.1)
            backoff = base_backoff * backoff_multiplier
            
            await asyncio.sleep(backoff / 1000)
            continue #loop again

        except redis.RedisError:
            return None, DB_ERROR_STR
        
        except Exception as e:
            logging.exception(f"Unexpected error in atomic_update_item for item {item_id}: {e}")
            return None, "Internal data error"
            
    return None, f"Failed to update item, retries nr??."

@worker.register
async def create_item(data):
    price = data.get("price")
    try:
        key = str(uuid.uuid4())
        logging.debug(f"Item: {key} created")
        value = msgpack.encode(StockValue(stock=0, price=int(price)))
        db_master.set(key, value)
        return {'item_id': key}, 200
    
    except redis.exceptions.RedisError as re:
        return str(re), 400
    except Exception as e:
        # Catch any other unexpected exceptions to prevent silent failures
        return f"Unexpected error: {str(e)}", 401

@worker.register
async def batch_init_stock(data):
    n = int(data.get("n"))
    starting_stock = int(data.get("starting_stock"))
    item_price = int(data.get("item_price"))
    kv_pairs: dict[str, bytes] = {f"{str(uuid.uuid4())}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db_master.mset(kv_pairs)
    except redis.exceptions.RedisError as re:
        return str(re), 400
    except Exception as e:
        # Catch any other unexpected exceptions to prevent silent failures
        return f"Unexpected error: {str(e)}", 401

    return {'msg': f"{kv_pairs.keys()}"}, 200


@worker.register
async def find_item(data):
    item_id = data.get("item_id")
    item_entry, response_code = get_item_from_db(item_id)

    if response_code == 400:
        return item_entry, 400

    return {"stock": item_entry.stock,
            "price": item_entry.price } , 200

@worker.register
@idempotent('add_stock', idempotency_db_conn, SERVICE_NAME)
async def add_stock(data):
    item_id = data.get("item_id")
    amount = data.get("amount")
    order_id = data.get("order_id")
    attempt_id = data.get("attempt_id")

    if not all([item_id, amount is not None]):
        return {"error": "Missing required fields (item_id, amount)"}, 400
    
    try:
        amount_int = int(amount)
        if amount_int <= 0:
            return {"error": "Transaction amount must be positive"}, 400
    except (ValueError, TypeError):
        logging.error("Unexpected error, amount:.", amount)
        return {"error": "Invalid amount?"}, 400
    
    
    def updater(item: StockValue) -> StockValue:
        item.stock += amount_int
        return item
    
    updated_item = None
    error_msg = None

    try:
        updated_item, error_msg = await atomic_update_item(item_id, updater)
    
        if error_msg:
            logging.error(f"'add_stock' logic failed: {error_msg}")
            logging.debug(f"error_msg: {error_msg}")
            if "not found" in error_msg.lower(): 
                status_code = 404
            else: 
                status_code = 500
            return ({"added": False, "error": error_msg}, status_code)
            
        elif updated_item:
            logging.info(f"Item: {item_id} stock updated to: {updated_item.stock}")
            return ({"added": True, "stock": updated_item.stock}, 200)

        else:
            #fallback
            return ({"added": False, "error": "Internal processing error"}, 500)
        
    except ValueError as e:
        error_msg = str(e)
        return ({"paid": False, "value error": error_msg}, 400)
    except Exception as e:
        logging.exception("internal error")
        return ({"paid": False, "internal error": error_msg}, 400)

@worker.register
@idempotent('remove_stock', idempotency_db_conn, SERVICE_NAME)
async def remove_stock(data):
    item_id = data.get("item_id")
    amount = data.get("amount")

    if not all([item_id, amount is not None]):
        logging.error("Missing item_id or amount in remove_stock request.")
        return {"error": "Missing required fields"}, 400
        
    try:
        if int(amount) <= 0:
            return {"error": "Transaction amount must be positive"}, 400
        
    except (ValueError, TypeError):
        return {"error": "Invalid amount specified"}, 400
    
    def updater(item: StockValue) -> StockValue:
        if item.stock < int(amount):
            # Raise ValueError for insufficient stock
            raise ValueError(f"Insufficient stock for item {item_id} (available: {item.stock}, requested: {int(amount)})")
        item.stock -= int(amount)
        return item
    
    updated_item = None
    error_msg = None

    try:
        updated_item, error_msg = await atomic_update_item(item_id, updater)
        if error_msg:
            logging.error(f"Failed to cancel stock (refund) for user {item_id}: {error_msg}")
            status_code = 404 if "not found" in error_msg.lower() else 500
            return {"error": f"Failed to cancel stock: {error_msg}"}, status_code
        if updated_item:
            return {"refunded": True, "credit": updated_item.credit}, 200
        else:
            logging.error(f"Cancel stock failed for user {item_id}, unknown reason .")
            return {"error": "Failed stock cancellation, unknown reason"}, 500
    except Exception as e:
        logging.exception("Error canceling stock for user %s: %s", item_id, e)
        return {"error": f"Error canceling stock for user {item_id}: {e}"}, 400
        
if __name__ == '__main__':
    asyncio.run(worker.start())
