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


DB_ERROR_STR = "DB error"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s"
)

logging = logging.getLogger("stock-service")

from global_idempotency.app import (
        IdempotencyStoreConnectionError,
        IdempotencyDataError,
        check_idempotent_operation,
        store_idempotent_result,
        IdempotencyResultTuple
    )

SERVICE_NAME = "stock"

worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="stock_queue",
)


sentinel= Sentinel([
    (os.environ['REDIS_SENTINEL_1'],26379),
    (os.environ['REDIS_SENTINEL_2'],26380),
    (os.environ['REDIS_SENTINEL_3'],26381)], socket_timeout=0.1, password= os.environ['REDIS_PASSWORD'])

db_master=sentinel.master_for('stock-master', socket_timeout=0.1, decode_responses=True)
db_slave=sentinel.slave_for('stock-master', socket_timeout=0.1, decode_responses=True)

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
    
    if order_id and attempt_id:
        idempotency_key = f"idempotent:{SERVICE_NAME}:add:{item_id}:{order_id}:{attempt_id}"
        idempotency_utf8 = idempotency_key.encode('utf-8')

        try:
            stored = check_idempotent_operation(idempotency_utf8, idempotency_db_conn) #awaitt
            if stored is not None:
                try:
                    if stored is not None:
                        return stored
                except MsgspecDecodeError as e:
                    logging.error("keeps on happening... %s: %s", idempotency_key, e)  
                    return {"error": "idempotency data error"}, 500
        except IdempotencyStoreConnectionError as e:
            return {"error": "Idempotency service unavailable"}, 503
        except IdempotencyDataError as e: 
            return {"error": "Idempotency data error"}, 500
        except Exception as e:
            logging.error("keeps on happening... %s: %s", idempotency_key, e)
            return {"error": "Idempotency service error"}, 500 

        def updater(item: StockValue) -> StockValue:
            item.stock += amount_int
            return item
        
        updated_item = None
        error_msg = None
        res = None

        try:
            updated_item, error_msg = await atomic_update_item(item_id, updater)
        except ValueError as e:
            error_msg = str(e)
            logging.warning(f"'pay' logic failed: {e}")
        except Exception as e:
            logging.exception("internal error")
            error_msg = str(e)

        if error_msg:
            logging.error(f"'add_stock' logic failed: {error_msg}")
            logging.debug(f"error_msg: {error_msg}")
            if "not found" in error_msg.lower(): 
                status_code = 404
            else: 
                status_code = 500
            res = ({"added": False, "error": error_msg}, status_code)
            
            if order_id and attempt_id:
                try:
                    store_idempotent_result(idempotency_key, res, idempotency_db_conn)
                except Exception as e:
                    logging.error(f"failure to store failure result ={idempotency_key}: {e}")
                return res
            
        elif updated_item:
            logging.info(f"Item: {item_id} stock updated to: {updated_item.stock}")
            res = ({"added": True, "stock": updated_item.stock}, 200)

            if order_id and attempt_id:
                try:
                    was_set = store_idempotent_result(idempotency_key, res, idempotency_db_conn)
                    if not was_set:
                        # Race condition
                        logging.warning(f"Idempotency key {idempotency_key} already existed or failed to set (race condition likely lost).")
                except (IdempotencyDataError, IdempotencyStoreConnectionError) as e:
                    logging.critical(f"FAILED TO STORE idempotency result for key {idempotency_key}: {e}")
                except Exception as e:
                    logging.critical("FAILED TO STORE ::", idempotency_key, e)
                    # prevent auto-ACK. Manual intervention |0|
            return res
        else:
            #fallback
            res = ({"added": False, "error": "Internal processing error"}, 500)
            # Store result for idempotency if we have order_id and attempt_id
            if order_id and attempt_id:
                try:
                    store_idempotent_result(idempotency_key, res, idempotency_db_conn)
                except Exception as e:
                    logging.error(f"Failed to store idempotency result, unknown {idempotency_key}: {e}")
            return res

@worker.register
async def remove_stock(data):
    item_id = data.get("item_id")
    amount = data.get("amount")

    def stock_value_change2(item: StockValue) -> StockValue:
        if item.stock - int(amount) < 0:
            return None, 400
        item.stock -= int(amount)
        return item, 200
    response, response_code = atomic_update_item(item_id,stock_value_change2)
    if response_code == 400:
        return response, 400

    updated_item, response_code = get_item_from_db(item_id)
    if response_code == 400:
        return DB_ERROR_STR, 400
    return {
            "UpdatedStock": updated_item.stock
            }, 200

if __name__ == '__main__':
    asyncio.run(worker.start())
