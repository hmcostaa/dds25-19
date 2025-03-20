import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import asyncio
from common.rpc_client import RpcClient
from common.amqp_worker import AMQPWorker

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from stock.app import db_master

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2 * item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


# @app.get('/find/<order_id>')
# def find_order(order_id: str):
#     order_entry: OrderValue = get_order_from_db(order_id)
#     return jsonify(
#         {
#             "order_id": order_id,
#             "paid": order_entry.paid,
#             "items": order_entry.items,
#             "user_id": order_entry.user_id,
#             "total_cost": order_entry.total_cost
#         }
#     )


# def send_post_request(url: str):
#     try:
#         response = requests.post(url)
#     except requests.exceptions.RequestException:
#         abort(400, REQ_ERROR_STR)
#     else:
#         return response


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


# def rollback_stock(removed_items: list[tuple[str, int]]):
#     for item_id, quantity in removed_items:
#         send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


# @app.post('/checkout/<order_id>')
# def checkout(order_id: str):
#     app.logger.debug(f"Checking out {order_id}")
#     order_entry: OrderValue = get_order_from_db(order_id)
#     # get the quantity per item
#     items_quantities: dict[str, int] = defaultdict(int)
#     for item_id, quantity in order_entry.items:
#         items_quantities[item_id] += quantity
#     # The removed items will contain the items that we already have successfully subtracted stock from
#     # for rollback purposes.
#     removed_items: list[tuple[str, int]] = []
#     for item_id, quantity in items_quantities.items():
#         stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
#         if stock_reply.status_code != 200:
#             # If one item does not have enough stock we need to rollback
#             rollback_stock(removed_items)
#             abort(400, f'Out of stock on item_id: {item_id}')
#         removed_items.append((item_id, quantity))
#     user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
#     if user_reply.status_code != 200:
#         # If the user does not have enough credit we need to rollback all the item stock subtractions
#         rollback_stock(removed_items)
#         abort(400, "User out of credit")
#     order_entry.paid = True
#     try:
#         db.set(order_id, msgpack.encode(order_entry))
#     except redis.exceptions.RedisError:
#         return abort(400, DB_ERROR_STR)
#     app.logger.debug("Checkout successful")
#     return Response("Checkout successful", status=200)

worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="order_queue",
)

# Initialize RpcClient for the order service
rpc_client = RpcClient()


@worker.before_start
async def connect_rpc_client():
    # Connect the RpcClient to the AMQP server
    await rpc_client.connect(os.environ["AMQP_URL"])


@worker.register
async def create_order(data):
    user_id = data.get("user_id")
    key = str(uuid.uuid4())
    user_id = data.get("user_id")
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db_master.set(key, value)
    except redis.exceptions.RedisError:
        return DB_ERROR_STR, 400
    return {"order_id": key}, 200


@worker.register
async def find_order(data):
    order_id = data.get("order_id")
    order_entry: OrderValue = get_order_from_db(order_id)
    return {
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost
    }


@worker.register
async def find_order(data):
    order_id = data.get("order_id")
    order_entry: OrderValue = get_order_from_db(order_id)
    return {
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost
    }


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response

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
        payment_response = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
        if payment_response.status_code != 200:
          rollback_stock(removed_items)
          pipe.reset()
          abort(400, "Payment failed or user out of credit")
        order_entry.paid = True
        pipe.set(order_id, msgpack.encode(order_entry))
        pipe.execute()
    except redis.WatchError:
        abort(400, DB_ERROR_STR)
        app.logger.debug("Checkout successful")
    return jsonify({"msg": "Checkout successful"})


@worker.register
async def add_item(data):
    # order_id: str, item_id: str, quantity: int
    order_id = data.get("order_id")
    item_id = data.get("item_id")
    quantity = data.get("quantity")
    order_entry: OrderValue = get_order_from_db(order_id)

    # Prepare the payload for the stock service
    payload = {
        "type": "find_item",  # Message type for the stock service
        "data": {
            "item_id": item_id
        }
    }

    stock_response = await rpc_client.call(payload, "stock_queue")

    # Check if the stock service returned an error
    if not stock_response or "error" in stock_response:
        abort(400, f"Item: {item_id} does not exist or stock service error!")

    # Extract item details from the stock service response
    item_price = stock_response.get("price")
    if item_price is None:
        abort(400, f"Item: {item_id} price not found!")

    # Update the order with the new item
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_price

    # Save the updated order back to the database
    try:
        db_master.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    return {"msg": f"Item: {item_id} added to order: {order_id}, total cost updated to: {order_entry.total_cost}"}


if __name__ == '__main__':
    asyncio.run(worker.start())
