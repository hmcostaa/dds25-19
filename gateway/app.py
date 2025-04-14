import os
from quart import Quart, jsonify, request
from common.rpc_client import RpcClient
import uuid

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
            return jsonify(response_data), status_code
        else:
            logger.error(f"Gateway received improperly structured response from worker: {rpc_result}")
            return jsonify({"error": "Internal Server Error - Malformed worker response"}), 500
    else:
        logger.error(f"Gateway received unexpected response format: {type(rpc_result)} - {rpc_result}")
        return jsonify({"error": "Internal Server Error - Invalid response format from worker"}), 500

# payload key
def ensure_idempotency_key() -> str:
    idempotency_key = request.headers.get("Idempotency-Key")
    if not idempotency_key:
        idempotency_key = str(uuid.uuid4())
        logger.debug(f"Generated Idempotency-Key: {idempotency_key}")
    else:
        logger.debug(f"Using provided Idempotency-Key: {idempotency_key}")
    return idempotency_key

######## Order Service Routes ########


@app.route("/orders/create/<user_id>", methods=["POST"])
async def create_order(user_id):
    idempotency_key = ensure_idempotency_key()
    payload = {
        "user_id": user_id,
        "idempotency_key": idempotency_key
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
    idempotency_key = ensure_idempotency_key()
    payload = {
        "order_id": order_id,
        "item_id": item_id,
        "quantity": int(quantity),
        "idempotency_key": idempotency_key
    }
    result = await rpc_client.call("order_queue","add_item",payload )
    return await handle_rpc_response(result)

@app.route("/orders/checkout/<order_id>", methods=["POST"])
async def checkout_order(order_id):
    idempotency_key = ensure_idempotency_key()
    payload = {
        "order_id": order_id,
        "idempotency_key": idempotency_key
    }
    logger.info(f"Gateway: Sending checkout RPC for order {order_id}")
    result = await rpc_client.call("order_queue", "process_checkout_request", payload)
    logger.info(f"Gateway: Received RPC result for order {order_id}: {result}") 
    response_tuple = await handle_rpc_response(result)
    logger.info(f"Gateway: Sending HTTP response for order {order_id}: {response_tuple}") 
    return response_tuple


######## Stock Service Routes ########

@app.route("/stock/item/create/<price>", methods=["POST"])
async def create_item(price):
    idempotency_key = ensure_idempotency_key()
    payload = {"price": int(price), "idempotency_key": idempotency_key}
    
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
    idempotency_key = ensure_idempotency_key()
    payload = {
        "item_id": item_id,
        "amount": amount,
        "idempotency_key": idempotency_key
    }
    result = await rpc_client.call(queue="stock_queue",
                                           action="add_stock",
                                           payload=payload)
    return await handle_rpc_response(result)

@app.route("/stock/subtract/<item_id>/<amount>", methods=["POST"])
async def subtract_stock(item_id, amount):
    idempotency_key = ensure_idempotency_key()
    payload = {
        "item_id": item_id,
        "amount": int(amount),
        "idempotency_key": idempotency_key
    }
    result = await rpc_client.call(queue="stock_queue", action="remove_stock", payload=payload)
    return await handle_rpc_response(result)

@app.route("/stock/batch_init/<n>/<starting_stock>/<item_price>", methods=["POST"])
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
    idempotency_key = ensure_idempotency_key()
    if not idempotency_key:
        idempotency_key = str(uuid.uuid4())
        logger.debug(f"No idempotency key found in request headers for add_funds. Generated key: {idempotency_key}")
    payload = {
        "user_id": user_id,
        "amount": float(amount),
        "idempotency_key": idempotency_key
    }
    result = await rpc_client.call(queue="payment_queue", action="add_funds", payload=payload)
    return await handle_rpc_response(result)


@app.route("/payment/pay/<user_id>/<amount>", methods=["POST"])
async def payment_pay(user_id, amount): 
    idempotency_key = ensure_idempotency_key()
    payload = {
        "user_id": user_id,
        "amount": float(amount),
        "idempotency_key": idempotency_key
    }
    result = await rpc_client.call(queue="payment_queue", action="pay", payload=payload)
    return await handle_rpc_response(result)

@app.route(("/payment/create_user"), methods=["POST"])
async def create_user():
    idempotency_key = ensure_idempotency_key()
    result = await rpc_client.call(queue="payment_queue",
                                    action="create_user",
                                    payload={"idempotency_key": idempotency_key}
                                    )
    return await handle_rpc_response(result)

@app.route("/payment/find_user/<user_id>", methods=["GET"])
async def find_user(user_id):
    result = await rpc_client.call(queue="payment_queue",
                                           action="find_user",
                                           payload={"user_id": user_id})
    return await handle_rpc_response(result)

@app.route("/payment/batch_init/<num_users>/<start_credit>", methods=["POST"])
async def payment_batch_init(num_users, start_credit):
    try:
        payload = {
            "n": int(num_users), 
            "starting_money": int(start_credit) 
        }
        logger.debug(f"Gateway: Calling payment_queue RPC action 'batch_init_users' with payload: {payload}")

        result = await rpc_client.call(queue="payment_queue",
                                       action="batch_init_users",
                                       payload=payload)

        logger.debug(f"Gateway: Received RPC result for batch_init_users: {result}")
        return await handle_rpc_response(result)

    except ValueError:
        logger.error(f"Gateway: Invalid non-integer parameters received: num_users='{num_users}', start_credit='{start_credit}'")
        return {"error": "Invalid parameters: num_users and start_credit must be integers."}, 400
    except Exception as e:
        logger.exception(f"Gateway: Error processing /payment/batch_init: {e}")
        return {"error": "Internal Server Error during batch initialization"}, 500

# @app.route("/stock/batch/<n>/<starting_stock>/<item_price>", methods=["POST"])
# async def batch_init(n, starting_stock, item_price):
#     logger.info(f"Gateway: Entered /stock/batch/ handler with n={n}, stock={starting_stock}, price={item_price}")
#     try:
#         # Convert parameters to integers
#         payload = {
#             "n": int(n),
#             "starting_stock": int(starting_stock),
#             "item_price": int(item_price)
#         }

#         logger.info(f"Gateway: Prepared payload for stock_batch_init: {payload}")
#         logger.info("Gateway: About to call RPC for batch_init_stock...")

#         result = await rpc_client.call(queue="stock_queue",
#                                        action="batch_init_stock",
#                                        payload=payload)

#         logger.info(f"Gateway: RPC call for batch_init_stock returned. Result: {result}")
#         return await handle_rpc_response(result)

#     except ValueError:
#         logger.exception(f"Gateway: Unhandled exception in /stock/batch/ handler: {e}")
#         return {"error": "Internal Server Error during stock batch initialization"}, 500
#     except Exception as e:
#         logger.exception(f"Gateway: Unhandled exception in /stock/batch/ handler: {e}")
#         return {"error": "Internal Server Error during stock batch initialization"}, 500
    
@app.route("/orders/batch_init/<num_orders>/<num_items>/<num_users>/<item_price>", methods=["POST"])
async def order_batch_init(num_orders, num_items, num_users, item_price):
    try:    
        payload = {
            "n_orders": int(num_orders),
            "n_items": int(num_items),
            "n_users": int(num_users),
            "item_price": int(item_price)
        }
        logger.debug(f"Gateway: Calling order_queue RPC action 'batch_init_orders' with payload: {payload}")

        result = await rpc_client.call(queue="order_queue",
                                       action="batch_init_orders", 
                                       payload=payload)

        logger.debug(f"Gateway: Received RPC result for batch_init_orders: {result}")
        return await handle_rpc_response(result)

    except ValueError:
        logger.error(f"Gateway: Invalid non-integer parameters received in order batch init")
        return {"error": "Invalid parameters: URL path parameters must be integers."}, 400
    except Exception as e:
        logger.exception(f"Gateway: Error processing /orders/batch_init: {e}")
        return {"error": "Internal Server Error during order batch initialization"}, 500

if __name__ == "__main__":
    app.run()
