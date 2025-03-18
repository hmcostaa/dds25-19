import os
from quart import Quart
from rpc_client import RpcClient

app = Quart(__name__)
rpc_client = RpcClient()


@app.before_serving
async def startup():
    await rpc_client.connect(os.environ["AMQP_URL"])


@app.route("/payment/find_user/<user_id>")
async def find_user(user_id):
    response = await rpc_client.call(user_id, "payment_queue")
    return response


if __name__ == "__main__":
    app.run()
