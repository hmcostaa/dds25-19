import logging
import os
import atexit
import uuid
from collections import defaultdict
import asyncio

from redis import Sentinel

import common.amqp_worker

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

sentinel= Sentinel([
    (os.environ['REDIS_SENTINEL_1'],26379),
    (os.environ['REDIS_SENTINEL_2'],26380),
    (os.environ['REDIS_SENTINEL_3'],26381)], socket_timeout=0.1, password= os.environ['REDIS_PASSWORD'])

db_master=sentinel.master_for('order-master', socket_timeout=0.1, decoder_responses=True)
db_slave=sentinel.slave_for('order-master', socket_timeout=0.1, decoder_responses=True)

def close_db_connection():
    db_master.close()
    db_slave.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db_slave.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry



# @app.post('/create/<user_id>')
# def create_order(user_id: str):
#     key = str(uuid.uuid4())
#     value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
#     try:
#         db.set(key, value)
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return jsonify({'order_id': key})


# @app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
# def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

#     n = int(n)
#     n_items = int(n_items)
#     n_users = int(n_users)
#     item_price = int(item_price)

#     def generate_entry() -> OrderValue:
#         user_id = random.randint(0, n_users - 1)
#         item1_id = random.randint(0, n_items - 1)
#         item2_id = random.randint(0, n_items - 1)
#         value = OrderValue(paid=False,
#                            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
#                            user_id=f"{user_id}",
#                            total_cost=2*item_price)
#         return value


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


# def send_get_request(url: str):
#     try:
#         response = requests.get(url)
#     except requests.exceptions.RequestException:
#         abort(400, REQ_ERROR_STR)
#     else:
#         return response


# @app.post('/addItem/<order_id>/<item_id>/<quantity>')
# def add_item(order_id: str, item_id: str, quantity: int):
#     order_entry: OrderValue = get_order_from_db(order_id)
#     # item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
#     if item_reply.status_code != 200:
#         # Request failed because item does not exist
#         abort(400, f"Item: {item_id} does not exist!")
#     item_json: dict = item_reply.json()
#     order_entry.items.append((item_id, int(quantity)))
#     order_entry.total_cost += int(quantity) * item_json["price"]
#     try:
#         db.set(order_id, msgpack.encode(order_entry))
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
#                     status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    pipe = db_master.pipeline()
    try:
        pipe.watch(order_id)
        pipe.multi()
        for item_id, quantity in items_quantities.items():
           stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
           if stock_reply.status_code != 200:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            abort(400, f'Out of stock on item_id: {item_id}')
            removed_items.append((item_id, quantity))
           payment_response= send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
           if payment_response.status_code != 200:
               rollback_stock(removed_items)
               pipe.reset()
               abort(400, "User out of credit")
           order_entry.paid = True
           pipe.set(order_id, msgpack.encode(order_entry))
           pipe.execute()
    except redis.WatchError:
        abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return jsonify({"msg": "Checkout successful"})

worker = common.amqp_worker.AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="order_queue",
)

@worker.register
async def create_order(data):
    key = str(uuid.uuid4())
    user_id = data.get("user_id")
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db_master.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return {"order_id": key}



if __name__ == '__main__':
    asyncio.run(worker.start())
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
