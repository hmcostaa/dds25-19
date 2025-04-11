import os
from quart import Quart, jsonify
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
    response = await rpc_client.call("order_queue", "create_order", payload)
    return response

@app.route("/order/find_order/<order_id>")
async def find_order(order_id):
    payload = {
        "type": "find_order",
        "data": {
            "order_id": order_id
        }
    }
    response = await rpc_client.call("order_queue","find_order",payload )
    return response

@app.route("/order/add_item/<order_id>/<item_id>/<quantity>")
async def add_item(order_id, item_id, quantity):
    payload = {
        "type": "add_item",
        "data": {
            "order_id": order_id,
            "item_id": item_id,
            "quantity": int (quantity)
        }
    }
    response = await rpc_client.call("order_queue","add_item",payload )
    return response
@app.route("/orders/checkout/<order_id>", methods=["POST"])
async def checkout(order_id):
    payload = {
        "type": "process_checkout_request",
        "data": { "order_id": order_id }
    }
    response = await rpc_client.call("order_queue", "process_checkout_request", payload)
    return response

@app.route("/orders/status/<order_id>", methods=["GET"])
async def saga_status(order_id):
    payload = {
        "type": "get_saga_state",
        "data": { "order_id": order_id }
    }
    response = await rpc_client.call("order_queue", "get_saga_state", payload)
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
    response, code = await rpc_client.call(queue="stock_queue",
                                           action="remove_stock",
                                           payload=payload)
    return response, code


@app.route("/stock/add/<item_id>/<amount>", methods=["POST"])
async def add_stock_item(item_id, amount):
    payload = {
        "item_id": item_id,
        "amount": amount
    }
    response = await rpc_client.call(payload, "stock_queue")
    return response
    response, code = await rpc_client.call(queue="stock_queue",
                                           action="add_stock",
                                           payload=payload)
    return response, code

@app.route("/stock/create_item/<price>")
async def create_item(price):
    payload = {"price": price}
    try:
        # Corrected call using positional arguments: payload, queue
        response_data = await rpc_client.call(payload, "stock_queue")

        # Check if response_data is actually what you expect (e.g., a dictionary)
        # The stock service returns {'item_id': key} on success
        if isinstance(response_data, dict) and 'item_id' in response_data:
             # Return the successful JSON response with a 200 OK status
            return jsonify(response_data), 200
        else:
             # Handle cases where RPC succeeded but returned unexpected data
            app.logger.error(f"RPC call for create_item returned unexpected data: {response_data}")
            return jsonify({"error": "Received unexpected data from stock service"}), 500

    except Exception as e:
        # Catch potential errors during the RPC call
        # (e.g., if RpcClient raised an error due to non-JSON reply from stock service on error)
        app.logger.error(f"RPC call failed for create_item: {e}")
        # Return an error JSON response
        return jsonify({"error": "RPC call to stock service failed"}), 500

@app.route("/stock/batch/<n>/<starting_stock>/<item_price>", methods=["POST"])
async def batch_init(n,starting_stock,item_price):
    payload = {
        "n": n,
        "starting_stock" : starting_stock,
        "item_price" : item_price
    }
    response, code = await rpc_client.call(queue="stock_queue",
                                           action="batch_init_stock",
                                           payload=payload)
    return response, code

######## Payment Service Routes ########


@app.route("/payment/pay/<user_id>/<amount>", methods=["POST"])
async def pay(user_id, amount):
    payload = {
        "type": "create_item",
        "data": {
            "price": price
        }
    }
    response, code = await rpc_client.call(queue="payment_queue",
                                           action="pay",
                                           payload=payload)
    return response, code


@app.route("/stock/add_stock/<item_id>/<amount>")
async def add_stock(item_id, amount):
    payload = {
        "type": "add_stock",
        "data": {
            "item_id": item_id,
            "amount": amount
        }
    }
    response = await rpc_client.call_stock("find_item", {"item_id":item_id})
    return response

# The following routes are used to interact with the payment service
@app.route(("/payment/create_user"), methods=["POST"])
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
