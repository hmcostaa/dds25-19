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
@app.route("/payment/create_user", methods=["POST"])
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
