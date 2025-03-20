import logging
import os
import atexit
import uuid

import redis
from common.amqp_worker import AMQPWorker

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="stock_queue",
)

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry

def atomic_update_item(item_id: str, update_func):
    while True:
        try:
            pipe = db.pipeline(transaction=True)  # Create Redis pipeline

            pipe.watch(item_id)  # Watch the item_id for changes
            entry = pipe.get(item_id)  # Retrieve the current value for item_id

            item_val: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None

            # If the item value is None, abort and return an error message
            if item_val is None:
                pipe.reset()  # Reset the pipeline
                abort(400, f"Item: {item_val} not found!")  # Return 400 error with message

            updated_item = update_func(item_val)

            pipe.multi()  # Start a transaction block

            # Serialize and set the updated item value in Redis
            pipe.set(item_val, msgpack.encode(updated_item))

            # Execute the transaction if no one else modified the item_id (based on pipe.watch)
            pipe.execute()

            break  # Exit the loop if the transaction is successful

        except redis.WatchError:
            continue  # Retry the entire operation

        except redis.RedisError:
            # Catch any other Redis-related errors and abort with a 400 status
            return DB_ERROR_STR, 400

        except Exception as e:
            # Catch any other unexpected exceptions to prevent silent failures
            return f"Unexpected error: {str(e)}", 401


@worker.register
async def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return DB_ERROR_STR, 400
    except Exception as e:
        # Catch any other unexpected exceptions to prevent silent failures
        return f"Unexpected error: {str(e)}", 401

    return {'item_id': key}, 200


@worker.register
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return DB_ERROR_STR, 400
    except Exception as e:
        # Catch any other unexpected exceptions to prevent silent failures
        return f"Unexpected error: {str(e)}", 401


    return {'msg': "Batch init for stock successful"}, 200


@worker.register
async def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return {"stock": item_entry.stock,
            "price": item_entry.price } , 200

@worker.register
async def add_stock(item_id: str, amount: int):
    async def stock_value_change(item: StockValue) -> StockValue:
        item.stock += int(amount)
        return item
    atomic_update_item(item_id,stock_value_change)
    updated_item = get_item_from_db(item_id)
    return {"Added": True, 
            "UpdatedStock": updated_item.stock
            }, 200


@worker.register
async def remove_stock(item_id: str, amount: int):
    async def stock_value_change(item: StockValue) -> StockValue:
        if item.stock - int(amount) < 0:
            return f"{item_id} has insufficient stock!", 400
        item.stock -= int(amount)
        return item
    atomic_update_item(item_id,stock_value_change)
    updated_item = get_item_from_db(item_id)
    return {"Removed": True, 
            "UpdatedStock": updated_item.stock
            }, 200

if __name__ == '__main__':
    asyncio.run(worker.start())

