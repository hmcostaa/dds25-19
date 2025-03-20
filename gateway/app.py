import os
from quart import Quart
from rpc_client import RpcClient

app = Quart(__name__)
rpc_client = RpcClient()


@app.before_serving
async def startup():
    await rpc_client.connect(os.environ["AMQP_URL"])


######## Order Service Routes ########


@app.route("/orders/create_order/<user_id>", methods=["POST"])
async def create_order(user_id):
    payload = {
        "type": "create_order",
        "data": {
            "user_id": user_id
        }
    }
    response = await rpc_client.call(payload, "order_queue")
    return response

@app.route("/order/find_order/<order_id>")
async def find_order(order_id):
    payload = {
        "type": "find_order",
        "data": {
            "order_id": order_id
        }
    }
    response = await rpc_client.call(payload, "order_queue")
    return response

@app.route("/order/add_item/<order_id>/<item_id>/<quantity>")
async def add_item(order_id, item_id, quantity):
    payload = {
        "type": "add_item",
        "data": {
            "order_id": order_id,
            "item_id": item_id,
            "quantity": quantity
        }
    }
    response = await rpc_client.call(payload, "order_queue")
    return response

# The following routes are used to interact with the stock service

@app.route("/stock/find_item/<item_id>")
async def find_item(item_id):
    payload = {
        "type": "find_item",
        "data": {
            "item_id": item_id
        }
    }
    response = await rpc_client.call(payload, "stock_queue")
    return response

@app.route("/stock/create_item/<price>")
async def create_item(price):
    payload = {
        "type": "create_item",
        "data": {
            "price": price
        }
    }
    response = await rpc_client.call(payload, "stock_queue")
    return response

@app.route("/stock/add_stock/<item_id>/<amount>")
async def add_stock(item_id, amount):
    payload = {
        "type": "add_stock",
        "data": {
            "item_id": item_id,
            "amount": amount
        }
    }
    response = await rpc_client.call(payload, "stock_queue")
    return response

# The following routes are used to interact with the payment service
@app.route("/payment/create_user")
async def create_user():
    payload = {
        "type": "create_user",
        "data": {}
    }
    response = await rpc_client.call(payload, "payment_queue")
    return response

@app.route("/stock/create_item/<price>")
async def create_item(price):
    payload = {
        "type": "create_item",
        "data": {
            "price": price
        }
    }
    response = await rpc_client.call(payload, "stock_queue")
    return response

@app.route("/payment/find_user/<user_id>")
async def find_user(user_id):
    payload = {
        "type": "find_user",
        "data": {
            "user_id": user_id
        }
    }
    response = await rpc_client.call(payload, "payment_queue")
    return response

# The following routes are used to interact with the payment service
@app.route("/payment/create_user")
>>>>>>> Stashed changes
async def create_user():
    response, code = await rpc_client.call(queue="payment_queue",
                                           action="create_user")
    return response, code


@app.route("/payment/find_user/<user_id>", methods=["GET"])
async def find_user(user_id):
    response, code = await rpc_client.call(queue="payment_queue",
                                           action="find_user",
                                           payload={"user_id": user_id})
    return response, code

if __name__ == "__main__":
    app.run()
