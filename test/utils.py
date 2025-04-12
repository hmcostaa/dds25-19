import requests
from typing import Tuple, Dict, Any, Union 

ORDER_URL = STOCK_URL = PAYMENT_URL = "http://localhost:8080"

ApiResponse = Tuple[Dict[str, Any], int]

def _process_response(response: requests.Response) -> ApiResponse:
    print(f"--- UTILS: _process_response - Status={response.status_code}, Raw Text='{response.text[:200]}...' ---")
    try:
        data = response.json()
        print(f"--- UTILS: _process_response - JSON Decoded Data Type={type(data)}, Value={data} ---")
        return data, response.status_code
    except requests.exceptions.JSONDecodeError:
        print(f"--- UTILS: _process_response - JSONDecodeError ---")
        return {"error": "Failed to decode JSON", "details": response.text}, response.status_code

def create_user() -> ApiResponse: 
    print(f"--- UTILS: create_user - Sending POST to {PAYMENT_URL}/payment/create_user ---")
    response = requests.post(f"{PAYMENT_URL}/payment/create_user")
    print(f"--- UTILS: create_user - Received response object ---")
    return _process_response(response)

########################################################################################################################
#   STOCK MICROSERVICE FUNCTIONS
########################################################################################################################
def create_item(price: int) -> ApiResponse:
    response = requests.post(f"{STOCK_URL}/stock/item/create/{price}")
    return _process_response(response) 

def find_item(item_id: str) -> ApiResponse:
    response = requests.get(f"{STOCK_URL}/stock/find/{item_id}")
    return _process_response(response) 

def add_stock(item_id: str, amount: int) -> ApiResponse: 
    response = requests.post(f"{STOCK_URL}/stock/add/{item_id}/{amount}")
    return _process_response(response) 

def subtract_stock(item_id: str, amount: int) -> ApiResponse:
    response = requests.post(f"{STOCK_URL}/stock/subtract/{item_id}/{amount}")
    return _process_response(response) 


########################################################################################################################
#   PAYMENT MICROSERVICE FUNCTIONS
########################################################################################################################
def payment_pay(user_id: str, amount: int) -> ApiResponse: 
    response = requests.post(f"{PAYMENT_URL}/payment/pay/{user_id}/{amount}")
    return _process_response(response) 

def create_user() -> ApiResponse: 
    response = requests.post(f"{PAYMENT_URL}/payment/create_user")
    return _process_response(response) 

def find_user(user_id: str) -> ApiResponse: 
    response = requests.get(f"{PAYMENT_URL}/payment/find_user/{user_id}")
    return _process_response(response)

def add_credit_to_user(user_id: str, amount: float) -> ApiResponse: 
    response = requests.post(f"{PAYMENT_URL}/payment/add_funds/{user_id}/{amount}")
    return _process_response(response) 


########################################################################################################################
#   ORDER MICROSERVICE FUNCTIONS
########################################################################################################################
def create_order(user_id: str) -> ApiResponse: 
    response = requests.post(f"{ORDER_URL}/orders/create/{user_id}")
    return _process_response(response) 

def add_item_to_order(order_id: str, item_id: str, quantity: int) -> ApiResponse: 
    response = requests.post(f"{ORDER_URL}/orders/addItem/{order_id}/{item_id}/{quantity}")
    return _process_response(response) 

def find_order(order_id: str) -> ApiResponse: 
    response = requests.get(f"{ORDER_URL}/orders/find/{order_id}")
    return _process_response(response) 

def checkout_order(order_id: str) -> requests.Response:
    return requests.post(f"{ORDER_URL}/orders/checkout/{order_id}")


########################################################################################################################
#   STATUS CHECKS
########################################################################################################################
def status_code_is_success(status_code: int) -> bool:
    print(f"--- UTILS: status_code_is_success - Status Code={status_code} ---")
    return 200 <= status_code < 300

def status_code_is_failure(status_code: int) -> bool:
    return 400 <= status_code < 500
