import os
from quart import Quart
from rpc_client import RpcClient

app = Quart(__name__)
rpc_client = RpcClient()


@app.before_serving
async def startup():
    await rpc_client.connect(os.environ["AMQP_URL"])


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
