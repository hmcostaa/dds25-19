import os
from quart import Quart, jsonify
from common.rpc_client import RpcClient

import logging

app = Quart(__name__)
rpc_client = RpcClient()


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@app.before_serving
async def startup():
    await rpc_client.connect(os.environ["AMQP_URL"])

#other handle rpc response since messages werent propagated succesfully through the gateway (not coherent with test)
async def handle_rpc_response(rpc_result):
    if isinstance(rpc_result, dict):
        if "status_code" in rpc_result and isinstance(rpc_result["status_code"], int):
            status_code = rpc_result["status_code"]
            # data or error
            if "data" in rpc_result:
                response_data = rpc_result["data"]
            elif "error" in rpc_result:
                # need to keep the error message in the response body for the client
                response_data = {"error": rpc_result["error"]}
            else:
                 response_data = {"error": "Worker response missing data/error field"}
                 status_code = 500

            logger.info(f"Gateway received structured response: Status={status_code}, Data/Error={response_data}")
            return response_data, status_code
        else:
            logger.error(f"Gateway received improperly structured response from worker: {rpc_result}")
            return {"error": "Internal Server Error - Malformed worker response"}, 500
    else:
        logger.error(f"Gateway received unexpected response format: {type(rpc_result)} - {rpc_result}")
        return {"error": "Internal Server Error - Invalid response format from worker"}, 500

######## Order Service Routes ########


@app.route("/orders/create/<user_id>", methods=["POST"])
async def create_order(user_id):
    # now consistent payload structure
    payload = {
        "user_id": user_id
    }
    result = await rpc_client.call("order_queue", "create_order", payload)
    return await handle_rpc_response(result)

@app.route("/orders/find/<order_id>")
async def find_order(order_id):
    payload = {
        "order_id": order_id
    }
    result = await rpc_client.call("order_queue","find_order",payload )
    return await handle_rpc_response(result)

@app.route("/orders/addItem/<order_id>/<item_id>/<quantity>", methods=["POST"]) 
async def add_item(order_id, item_id, quantity):
    payload = {
        "order_id": order_id,
        "item_id": item_id,
        "quantity": int(quantity)
    }
    result = await rpc_client.call("order_queue","add_item",payload )
    return await handle_rpc_response(result)

@app.route("/orders/checkout/<order_id>", methods=["POST"])
async def checkout_order(order_id): 
    payload = {
        "order_id": order_id
    }
    result = await rpc_client.call("order_queue", "process_checkout_request", payload)
    return await handle_rpc_response(result)


######## Stock Service Routes ########

@app.route("/stock/item/create/<price>", methods=["POST"])
async def create_item(price):
    payload = {"price": int(price)}
    result = await rpc_client.call(queue="stock_queue", action="create_item", payload=payload)
    return await handle_rpc_response(result)

@app.route("/stock/find/<item_id>", methods=["GET"])
async def find_item(item_id):
    print(f"--- GATEWAY: Entering handle_find_item for item_id={item_id} ---", flush=True)
    payload = {"item_id": item_id}
    result = await rpc_client.call(queue="stock_queue", action="find_item", payload=payload)
    return await handle_rpc_response(result)
    
@app.route("/stock/add/<item_id>/<amount>", methods=["POST"])
async def add_stock_item(item_id, amount):
    payload = {
        "item_id": item_id,
        "amount": amount
    }
    result = await rpc_client.call(queue="stock_queue",
                                           action="add_stock",
                                           payload=payload)
    return await handle_rpc_response(result)

@app.route("/stock/subtract/<item_id>/<amount>", methods=["POST"])
async def subtract_stock(item_id, amount):
    payload = {
        "item_id": item_id,
        "amount": int(amount) 
    }
    result = await rpc_client.call(queue="stock_queue", action="remove_stock", payload=payload)
    return await handle_rpc_response(result)

@app.route("/stock/batch/<n>/<starting_stock>/<item_price>", methods=["POST"])
async def batch_init(n,starting_stock,item_price):
    payload = {
        "n": n,
        "starting_stock" : starting_stock,
        "item_price" : item_price
    }
    result = await rpc_client.call(queue="stock_queue",
                                           action="batch_init_stock",
                                           payload=payload)
    return await handle_rpc_response(result)

######## Payment Service Routes ########

@app.route("/payment/add_funds/<user_id>/<amount>", methods=["POST"])
async def add_credit_to_user(user_id, amount):
    payload = {
        "user_id": user_id,
        "amount": float(amount) 
    }
    result = await rpc_client.call(queue="payment_queue", action="add_funds", payload=payload)
    return await handle_rpc_response(result)


@app.route("/payment/pay/<user_id>/<amount>", methods=["POST"])
async def payment_pay(user_id, amount): 
    payload = {
        "user_id": user_id,
        "amount": float(amount) 
    }
    result = await rpc_client.call(queue="payment_queue", action="pay", payload=payload)
    return await handle_rpc_response(result)

@app.route(("/payment/create_user"), methods=["POST"])
async def create_user():
    result = await rpc_client.call(queue="payment_queue",
                                           action="create_user")
    return await handle_rpc_response(result)

@app.route("/payment/find_user/<user_id>", methods=["GET"])
async def find_user(user_id):
    result = await rpc_client.call(queue="payment_queue",
                                           action="find_user",
                                           payload={"user_id": user_id})
    return await handle_rpc_response(result)

if __name__ == "__main__":
    app.run()
