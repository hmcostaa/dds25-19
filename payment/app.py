import os
import atexit
import asyncio

from redis import Sentinel

from common.amqp_worker import AMQPWorker

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"


app = Flask("payment-service")


sentinel= Sentinel([
    (os.environ['REDIS_SENTINEL_1'],26379),
    (os.environ['REDIS_SENTINEL_2'],26380),
    (os.environ['REDIS_SENTINEL_3'],26381)], socket_timeout=0.1, password= os.environ['REDIS_PASSWORD'])

db_master=sentinel.master_for('payment-master', socket_timeout=0.1, decode_responses=True)
db_slave=sentinel.slave_for('payment-master', socket_timeout=0.1, decode_responses=True)

def close_db_connection():
    db_master.close()
    db_slave.close()

atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db_slave.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


# @app.post('/create_user')
# def create_user():
#     key = str(uuid.uuid4())
#     value = msgpack.encode(UserValue(credit=0))
#     try:
#         db.set(key, value)
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db_master.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


# @app.get('/find_user/<user_id>')
# def find_user(user_id: str):
#     user_entry: UserValue = get_user_from_db(user_id)
#     return jsonify(
#         {
#             "user_id": user_id,
#             "credit": user_entry.credit
#         }
#     )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db_master.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db_master.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="payment_queue",
)


@worker.register
async def find_user(data):
    user_id = data.get("user_id")
    response = {
        "user_id": user_id,
        "credit": 100
    }
    return response, 200


@worker.register
async def create_user(data):
    response = {
        "user_id": "12345"  # Example response
    }
    return "blah", 418


if __name__ == '__main__':
    asyncio.run(worker.start())
