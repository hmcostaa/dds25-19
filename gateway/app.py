import os
from quart import Quart
from rpc_client import RpcClient
import logging

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s"
)


app = Quart(__name__)
rpc_client = RpcClient()


@app.before_serving
async def startup():
    await rpc_client.connect(os.environ["AMQP_URL"])


######## Order Service Routes ########


@app.route("/orders/create_order/<user_id>", methods=["POST"])
async def create_order(user_id):
    response, code = await rpc_client.call(queue="order_queue",
                                           action="create_order",
                                           payload={"user_id": user_id})
    return response, code


@app.route("/payment/find_user/<user_id>", methods=["GET"])
async def find_user(user_id):
    logging.info("Gateway received find_user request for user_id: %s", user_id)
    payload = {"user_id": user_id}
    logging.debug("Sending payload to RPC client: %s", payload)
    response, code = await rpc_client.call(
        queue="payment_queue",
        action="find_user",
        payload=payload
    )
    logging.info("Gateway received response: %s with code %s", response, code)
    return response, code


@app.route("/orders/addItem/<order_id>/<item_id>/<quantity>", methods=["POST"])
async def add_order_item(order_id, item_id, quantity):
    payload = {
        "order_id": order_id,
        "item_id": item_id,
        "quantity": quantity
    }
    response, code = await rpc_client.call(queue="order_queue",
                                           action="add_item",
                                           payload=payload)
    return response, code


@app.route("/orders/checkout/<order_id>", methods=["POST"])
async def checkout(order_id):
    response, code = await rpc_client.call(queue="order_queue",
                                           action="checkout",
                                           payload={"order_id": order_id})
    return response, code


######## Stock Service Routes ########


@app.route("/stock/find/<item_id>", methods=["GET"])
async def find_item(item_id):
    response, code = await rpc_client.call(queue="stock_queue",
                                           action="find_item",
                                           payload={"item_id": item_id})
    return response, code


@app.route("/stock/subtract/<item_id>/<amount>", methods=["POST"])
async def subtract_item(item_id, amount):
    payload = {
        "item_id": item_id,
        "amount": amount
    }
    response, code = await rpc_client.call(queue="stock_queue",
                                           action="subtract_item",
                                           payload=payload)
    return response, code


@app.route("/stock/add/<item_id>/<amount>", methods=["POST"])
async def add_stock_item(item_id, amount):
    payload = {
        "item_id": item_id,
        "amount": amount
    }
    response, code = await rpc_client.call(queue="stock_queue",
                                           action="add_item",
                                           payload=payload)
    return response, code


@app.route("/stock/item/create/<price>", methods=["POST"])
async def create_item(price):
    response, code = await rpc_client.call(queue="stock_queue",
                                           action="create_item",
                                           payload={"price": price})
    return response, code


######## Payment Service Routes ########


@app.route("/payment/pay/<user_id>/<amount>", methods=["POST"])
async def pay(user_id, amount):
    payload = {
        "user_id": user_id,
        "amount": amount
    }
    response, code = await rpc_client.call(queue="payment_queue",
                                           action="pay",
                                           payload=payload)
    return response, code


@app.route("/payment/add_funds/<user_id>/<amount>", methods=["POST"])
async def add_funds(user_id, amount):
    payload = {
        "user_id": user_id,
        "amount": amount
    }
    response, code = await rpc_client.call(queue="payment_queue",
                                           action="add_funds",
                                           payload=payload)
    return response, code


@app.route("/payment/create_user", methods=["POST"])
async def create_user():
    response, code = await rpc_client.call(queue="payment_queue",
                                           action="create_user")
    return response, code



if __name__ == "__main__":
    app.run()
