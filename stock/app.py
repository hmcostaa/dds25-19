import logging
import os
import atexit
import time
import uuid
import asyncio
import redis
from common.amqp_worker import AMQPWorker
from global_idempotency import helper

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from redis import Sentinel

from global_idempotency.helper import check_idempotency_key, store_idempotent_result, IdempotencyStoreConnectionError

DB_ERROR_STR = "DB error"

app = Flask("stock-service")
SERVICE_NAME = "stock-service"

sentinel= Sentinel([
    (os.environ['REDIS_SENTINEL_1'],26379),
    (os.environ['REDIS_SENTINEL_2'],26380),
    (os.environ['REDIS_SENTINEL_3'],26381)], socket_timeout=0.1, password= os.environ['REDIS_PASSWORD'])

db_master=sentinel.master_for('stock-master', socket_timeout=0.1, decode_responses=True)
db_slave=sentinel.slave_for('stock-master', socket_timeout=0.1, decode_responses=True)

def close_db_connection():
    db_master.close()
    db_slave.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


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

def atomic_update_item(item_id: str, update_func):
    while True:
        try:
            pipe = db_master.pipeline(transaction=True)  # Create Redis pipeline

            pipe.watch(item_id)  # Watch the item_id for changes
            entry = pipe.get(item_id)  # Retrieve the current value for item_id

            item_val: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None

            # If the item value is None, abort and return an error message
            if item_val is None:
                pipe.reset()  # Reset the pipeline
                f"Item: {item_id} not found!", 400  # Return 400 error with message

            updated_item, response_code = update_func(item_val)
            if response_code == 400:
                pipe.reset()  # Reset the pipeline
                return "update function failure", 400

            pipe.multi()  # Start a transaction block

            # Serialize and set the updated item value in Redis
            pipe.set(item_id, msgpack.encode(updated_item))

            # Execute the transaction if no one else modified the item_id (based on pipe.watch)
            pipe.execute()

        except redis.WatchError:
            continue  # Retry the entire operation

        except redis.RedisError as re:
            # Catch any other Redis-related errors and abort with a 400 status
            return str(re) , 400

        except Exception as e:
            # Catch any other unexpected exceptions to prevent silent failures
            return f"Unexpected error: {str(e)}", 401
        else:
            return "Atomic update successful", 200

worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="stock_queue",
)

@worker.register
async def create_item(data):
    price = data.get("price")
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db_master.set(key, value)
    except redis.exceptions.RedisError as re:
        return str(re), 400
    except Exception as e:
        # Catch any other unexpected exceptions to prevent silent failures
        return f"Unexpected error: {str(e)}", 401
    idempotency_key = helper.generate_idempotency_key(SERVICE_NAME, data.get("user_id"), data.get("order_id"),
                                                      data.get("attempt_id"))
    try:
        if check_idempotency_key(idempotency_key):
            app.logger.info(f"[IDEMPOTENCY] Duplicate batch init stock for {idempotency_key}")
            return {"msg": "ALREADY_PROCESSED"}, 200
    except IdempotencyStoreConnectionError as e:
        app.logger.error(str(e))
        return{"msg": "Redis error during idempotency check"}, 500
    response_data = {"status": "success", "step": "item created"}
    store_idempotent_result(idempotency_key,response_data)

    return {'item_id': key}, 200


@worker.register
async def batch_init_stock(data):
    n = int(data.get("n"))
    starting_stock = int(data.get("starting_stock"))
    item_price = int(data.get("item_price"))
    kv_pairs: dict[str, bytes] = {f"{str(uuid.uuid4())}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    idempotency_key = helper.generate_idempotency_key(SERVICE_NAME, data.get("user_id"), data.get("order_id"),
                                                      data.get("attempt_id"))

    try:
        db_master.mset(kv_pairs)
    except redis.exceptions.RedisError as re:
        return str(re), 400
    except Exception as e:
        # Catch any other unexpected exceptions to prevent silent failures
        return f"Unexpected error: {str(e)}", 401
    try:
        if check_idempotency_key(idempotency_key):
            app.logger.info(f"[IDEMPOTENCY] Duplicate batch init stock for {idempotency_key}")
            return {"msg": "ALREADY_PROCESSED"}, 200
    except IdempotencyStoreConnectionError as e:
        app.logger.error(str(e))
        return {"msg": "Redis error during idempotency check"}, 500
    response_data = {"status": "success", "step": "batch init stock added"}
    store_idempotent_result(idempotency_key, response_data)

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
    idempotency_key = helper.generate_idempotency_key(SERVICE_NAME, data.get("user_id"), data.get("order_id"),
                                                      data.get("attempt_id"))
    try:
        if check_idempotency_key(idempotency_key):
            app.logger.info(f"[IDEMPOTENCY] Duplicate add stock for {idempotency_key}")
            return {"msg": "ALREADY_PROCESSED"}, 200
    except IdempotencyStoreConnectionError as e:
        app.logger.error(str(e))
        return{"msg": "Redis error during idempotency check"}, 500
    def stock_value_change1(item: StockValue) -> StockValue:
        item.stock += int(amount)
        return item, 200
    response, response_code = atomic_update_item(item_id,stock_value_change1)
    if response_code != 200:
        return response, 401
    response_data = {"status": "success", "step": "stock added"}
    store_idempotent_result(idempotency_key,response_data)
    updated_item, response_code = get_item_from_db(item_id)
    if response_code != 200:
        return updated_item, 402

    return {
            "UpdatedStock": updated_item.stock
            }, 200


@worker.register
async def remove_stock(data):
    item_id = data.get("item_id")
    amount = data.get("amount")
    idempotency_key=helper.generate_idempotency_key(SERVICE_NAME,data.get("user_id"),data.get("order_id"),data.get("attempt_id"))
    try:
        if check_idempotency_key(idempotency_key):
            app.logger.info(f"[IDEMPOTENCY] Duplicate remove stock for {idempotency_key}")
            return {"msg": "ALREADY_PROCESSED"}, 200
    except IdempotencyStoreConnectionError as e:
        app.logger.error(str(e))
        return{"msg": "Redis error during idempotency check"}, 500
    def stock_value_change2(item: StockValue) -> StockValue:
        if item.stock - int(amount) < 0:
            return None, 400
        item.stock -= int(amount)
        return item, 200
    response, response_code = atomic_update_item(item_id,stock_value_change2)
    if response_code == 400:
        return response, 400
    response_data = {"status": "success", "step": "stock removed"}
    store_idempotent_result(idempotency_key, response_data)
    updated_item, response_code = get_item_from_db(item_id)
    if response_code == 400:
        return DB_ERROR_STR, 400
    return {
            "UpdatedStock": updated_item.stock
            }, 200

if __name__ == '__main__':
    asyncio.run(worker.start())