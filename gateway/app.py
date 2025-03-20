import os
from quart import Quart
from rpc_client import RpcClient

app = Quart(__name__)
rpc_client = RpcClient()


@app.before_serving
async def startup():
    await rpc_client.connect(os.environ["AMQP_URL"])

# The following routes are used to interact with the order service
@app.route("/order/create_order/<user_id>")
async def create_order(user_id):
    payload = {
        "type": "create_order",
        "data": {
            "user_id": user_id
        }
    }
    response = await rpc_client.call(payload, "order_queue")
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

if __name__ == "__main__":
    app.run()
