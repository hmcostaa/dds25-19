import logging
import os
import atexit
import sys
import uuid
import asyncio

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s"
)


sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from common.amqp_worker import AMQPWorker

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="payment_queue",
)

DB_ERROR_STR = "DB error"

#pytest --maxfail=1 --disable-warnings -q

app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry

def atomic_update_user(user_id: str, update_func):
    while True:
        try:
            pipe = db.pipeline(transaction=True) #create redis ppipeline
            pipe.watch(user_id)
            entry = pipe.get(user_id)
            user_val: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
            if user_val is None:
                pipe.reset()
                abort(400, f"User: {user_id} not found!")
            updated_user = update_func(user_val)
            pipe.multi()
            pipe.set(user_id, msgpack.encode(updated_user))
            pipe.execute() #execute if no other client modified user_id (pipe.watch)
            break
        except redis.WatchError:
            continue 
        except redis.RedisError:
            abort(400, DB_ERROR_STR)


# For testing, had endpoint "user_id" couldnt be found using POSTMAN
def require_user_id(data):
    uid = data.get("user_id")
    if not uid:
        abort(400, "Missing 'user_id' in payload")
    return str(uid).strip("{}")

# @app.post('/create_user')
# def create_user():
@worker.register
async def create_user(data):
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return {"error": DB_ERROR_STR}, 400
    return {'user_id': key}, 200

# @app.post('/batch_init/<n>/<starting_money>')
# def batch_init_users(n: int, starting_money: int):
@worker.register
async def batch_init_users(data):
    n = int(data['n'])
    starting_money = int(data['starting_money'])
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return {"error": DB_ERROR_STR}, 400
    return {"msg": "Batch init for users successful"}, 200

def require_user_id(data):
    uid = data.get("user_id")
    if not uid:
        abort(400, "Missing 'user_id' in payload")
    return str(uid).strip() 

# @app.get('/find_user/<user_id>')
# def find_user(user_id: str):
@worker.register
async def find_user(data):
    logging.debug("find_user invoked with payload: %s", data)
    
    user_id =  require_user_id(data)
    # user_id = data.get("user_id")
    if not user_id:
        logging.error("Missing 'user_id' in payload: %s", data)
        abort(400, "Missing 'user_id' in payload")
    logging.debug("Extracted user_id: %s", user_id)
    
    try:
        user_entry: UserValue = get_user_from_db(user_id)
        logging.debug("Decoded user_entry: %s", user_entry)
    except Exception as e:
        logging.exception("Error retrieving user from DB for id %s: %s", user_id, e)
        abort(400, f"Error retrieving user: {e}")
    
    response = {"user_id": user_id, "credit": user_entry.credit}
    logging.debug("Sending response: %s", response)
    return response, 200

# @app.post('/add_funds/<user_id>/<amount>')
# def add_credit(user_id: str, amount: int):
@worker.register
async def add_funds(data):
    user_id =  require_user_id(data)
    def updater(user: UserValue) -> UserValue:
        user.credit += int(data["amount"])
        return user
    # atomic_update_user(data["user_id"], updater)
    atomic_update_user(user_id, updater)
    updated_user = get_user_from_db(user_id)
    # return Response(f"User: {user_id} credit updated to: {updated_user.credit}", status=200)
    return {"done": True, "credit": updated_user.credit}, 200

# @app.post('/pay/<user_id>/<amount>')
# def remove_credit(user_id: str, amount: int):
@worker.register
async def pay(data):
    user_id =  require_user_id(data)
    def updater(user: UserValue) -> UserValue:
        if user.credit < int(data["amount"]):
            abort(400, f"User: {data['user_id']} has insufficient credit!")
        user.credit -= int(data["amount"])
        return user
    atomic_update_user(user_id, updater)
    updated_user = get_user_from_db(user_id)
    # return Response(f"User: {user_id} credit updated to: {updated_user.credit}", status=200)
    return {"paid": True, "credit": updated_user.credit}, 200

# @app.post('/cancel/<user_id>/<amount>')
# def cancel_payment(user_id: str, amount: int):
@worker.register
async def cancel_payment(data):
    user_id =  require_user_id(data)
    def updater(user: UserValue) -> UserValue:
        user.credit += int(data["amount"])
        return user
    atomic_update_user(user_id, updater)
    updated_user = get_user_from_db(user_id)
    # return Response(f"User: {user_id} credit updated to: {updated_user.credit}", status=200)
    return {"refunded": True, "credit": updated_user.credit}, 200

if __name__ == '__main__':
    asyncio.run(worker.start())
