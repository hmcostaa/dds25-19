import pytest
import asyncio
import json
import uuid 
import random
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch, MagicMock
import logging 

from app import (
    db_master, worker, UserValue  
)

class MockAMQPClient:
    def __init__(self, worker_obj): 
        self.worker = worker_obj

    async def call_worker(self, function_name, data):
        try:
            result, status_code = await self.worker.callbacks[function_name](data)
            logging.info(f"Mock Call: {function_name}({data}) -> Status {status_code}, Result {result}")
            return result, status_code
        except Exception as e:
            logging.exception(f"!!! Exception caught by MockAMQPClient calling '{function_name}' !!!")
            return {"error": str(e)}, 500

@pytest.fixture
def amqp_client():
    print("\n--- Verifying worker.callbacks ---")
    print(f"Callback keys found: {list(worker.callbacks.keys())}")
    print("---------------------------------\n")
    return MockAMQPClient(worker) 

@pytest.fixture(scope="function")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="function", autouse=True)
def flush_db():
    db_master.flushall()  

@pytest.mark.asyncio
async def test_successful_payment(amqp_client):
    """
    Test a basic successful payment scenario
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]

    result_add, status_add = await amqp_client.call_worker('add_funds', {"user_id": user_id, "amount": 100})
    assert status_add == 200
    assert result_add.get("done") is True

    attempt_id_pay = str(uuid.uuid4())
    order_id_pay = f"order-{attempt_id_pay}"
    result_pay, status_pay = await amqp_client.call_worker(
        'pay',
        {
            "user_id": user_id,
            "amount": 50,
            "attempt_id": attempt_id_pay,
            "order_id": order_id_pay
        }
    )
    assert status_pay == 200
    assert result_pay.get("paid") is True

    result_find, status_find = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status_find == 200
    assert result_find.get("credit") == 50

@pytest.mark.asyncio
async def test_payment_insufficient_funds(amqp_client):
    """
    Test payment when user has insufficient funds
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]

    attempt_id_pay = str(uuid.uuid4())
    order_id_pay = f"order-{attempt_id_pay}"
    result_pay, status_pay = await amqp_client.call_worker(
        'pay',
        {
            "user_id": user_id,
            "amount": 50,
            "attempt_id": attempt_id_pay,
            "order_id": order_id_pay
        }
    )
    assert status_pay == 400 
    assert "error" in result_pay
    assert "insufficient credit" in str(result_pay.get("error", "")).lower()

@pytest.mark.asyncio
async def test_payment_exact_balance(amqp_client):
    """
    Test payment using exact available balance
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]

    amount = 100
    result_add, status_add = await amqp_client.call_worker('add_funds', {"user_id": user_id, "amount": amount})
    assert status_add == 200
    assert result_add.get("done") is True

    attempt_id_pay = str(uuid.uuid4())
    order_id_pay = f"order-{attempt_id_pay}"
    result_pay, status_pay = await amqp_client.call_worker(
        'pay',
        {
            "user_id": user_id,
            "amount": amount,
            "attempt_id": attempt_id_pay,
            "order_id": order_id_pay
        }
    )
    assert status_pay == 200
    assert result_pay.get("paid") is True

    result_find, status_find = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status_find == 200
    assert result_find.get("credit") == 0

@pytest.mark.asyncio
async def test_payment_non_existent_user(amqp_client):
    """
    Test payment for a non-existent user
    """
    non_existent_id = str(uuid.uuid4())

    attempt_id_pay = str(uuid.uuid4())
    order_id_pay = f"order-{attempt_id_pay}"
    result_pay, status_pay = await amqp_client.call_worker(
        'pay',
        {
            "user_id": non_existent_id,
            "amount": 50,
            "attempt_id": attempt_id_pay,
            "order_id": order_id_pay
        }
    )
    assert status_pay == 404 
    assert "error" in result_pay
    assert "not found" in str(result_pay.get("error", "")).lower()

@pytest.mark.asyncio
async def test_payment_negative_amount(amqp_client):
    """
    Test payment with a negative amount
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]

    result_add, status_add = await amqp_client.call_worker('add_funds', {"user_id": user_id, "amount": 100})
    assert status_add == 200
    assert result_add.get("done") is True

    attempt_id_pay = str(uuid.uuid4())
    order_id_pay = f"order-{attempt_id_pay}"
    result_pay, status_pay = await amqp_client.call_worker(
        'pay',
        {
            "user_id": user_id,
            "amount": -50,
            "attempt_id": attempt_id_pay,
            "order_id": order_id_pay
        }
    )
    assert status_pay == 400 
    assert "error" in result_pay
    assert "transaction amount must be positive" in str(result_pay.get("error", "")).lower() 


@pytest.mark.asyncio
async def test_payment_idempotency(amqp_client):
    """
    Test that pay operation is idempotent.
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]
    result_add, status_add = await amqp_client.call_worker('add_funds', {"user_id": user_id, "amount": 100})
    assert status_add == 200

    amount_to_pay = 30
    attempt_id = str(uuid.uuid4())
    order_id = f"order-idem-{attempt_id}"
    payload = {
        "user_id": user_id,
        "amount": amount_to_pay,
        "attempt_id": attempt_id,
        "order_id": order_id
    }

    result1, status1 = await amqp_client.call_worker('pay', payload)
    assert status1 == 200
    assert result1.get("paid") is True
    assert result1.get("credit") == 100 - amount_to_pay 

    result_find1, status_find1 = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status_find1 == 200
    assert result_find1.get("credit") == 100 - amount_to_pay

    result2, status2 = await amqp_client.call_worker('pay', payload)
    assert status2 == 200
    assert result2.get("paid") is True
    assert result2.get("credit") == 100 - amount_to_pay 

    result_find2, status_find2 = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status_find2 == 200
    assert result_find2.get("credit") == 100 - amount_to_pay 


@pytest.mark.asyncio
async def test_cancel_payment_successful(amqp_client):
    """
    Test successful cancellation (refund).
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]
    result_add, status_add = await amqp_client.call_worker('add_funds', {"user_id": user_id, "amount": 50})
    assert status_add == 200

    amount_to_cancel = 30
    result_cancel, status_cancel = await amqp_client.call_worker(
        'cancel_payment',
        {"user_id": user_id, "amount": amount_to_cancel}
    )
    assert status_cancel == 200
    assert result_cancel.get("refunded") is True
    assert result_cancel.get("credit") == 50 + amount_to_cancel 

    result_find, status_find = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status_find == 200
    assert result_find.get("credit") == 50 + amount_to_cancel


@pytest.mark.asyncio
async def test_cancel_payment_non_existent_user(amqp_client):
    """
    Test cancelling payment for a non-existent user.
    """
    non_existent_id = str(uuid.uuid4())
    result_cancel, status_cancel = await amqp_client.call_worker(
        'cancel_payment',
        {"user_id": non_existent_id, "amount": 30}
    )
    assert status_cancel == 404 
    assert "error" in result_cancel
    assert "not found" in str(result_cancel.get("error", "")).lower()


@pytest.mark.asyncio
async def test_cancel_payment_invalid_amount(amqp_client):
    """
    Test cancelling payment with zero or negative amount.
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]

    result_cancel0, status_cancel0 = await amqp_client.call_worker(
        'cancel_payment',
        {"user_id": user_id, "amount": 0}
    )
    assert status_cancel0 == 400
    assert "error" in result_cancel0
    assert "must be positive" in str(result_cancel0.get("error", "")).lower()

    result_cancel_neg, status_cancel_neg = await amqp_client.call_worker(
        'cancel_payment',
        {"user_id": user_id, "amount": -30}
    )
    assert status_cancel_neg == 400
    assert "error" in result_cancel_neg
    assert "must be positive" in str(result_cancel_neg.get("error", "")).lower()


@pytest.mark.asyncio
async def test_concurrent_add_funds(amqp_client):
    """
    Test concurrent fund additions to ensure thread-safety
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]

    async def add_concurrent_funds(amount):
        return await amqp_client.call_worker('add_funds', {
            "user_id": user_id, 
            "amount": amount
        })
    
    fund_amounts = [random.randint(10, 50) for _ in range(10)]
    tasks = [add_concurrent_funds(amount) for amount in fund_amounts]
    
    results = await asyncio.gather(*tasks)
    
    successful_additions = [res[0].get('done') for res in results]
    assert all(successful_additions), "Not all fund additions were successful"
    
    result_find, status_find = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status_find == 200
    assert result_find.get("credit") == sum(fund_amounts)

@pytest.mark.asyncio
async def test_concurrent_payments(amqp_client):
    """
    Test concurrent payment attempts to verify race condition handling
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]
    
    initial_funds = 1000
    result_add, status_add = await amqp_client.call_worker('add_funds', {
        "user_id": user_id, 
        "amount": initial_funds
    })
    assert status_add == 200
    
    async def make_concurrent_payment(amount):
        attempt_id = str(uuid.uuid4())
        order_id = f"order-{attempt_id}"
        return await amqp_client.call_worker('pay', {
            "user_id": user_id,
            "amount": amount,
            "attempt_id": attempt_id,
            "order_id": order_id
        })
    
    payment_amounts = [50, 100, 75, 25, 60, 90, 40, 200, 150, 120]
    
    # Track which payments should succeed
    successful_payments = []
    total_paid = 0
    for amount in payment_amounts:
        if total_paid + amount <= initial_funds:
            successful_payments.append(amount)
            total_paid += amount
    
    tasks = [make_concurrent_payment(amount) for amount in payment_amounts]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    result_find, status_find = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status_find == 200
    assert result_find.get("credit") == initial_funds - total_paid

@pytest.mark.asyncio
async def test_batch_user_initialization(amqp_client):
    """
    Test batch user initialization with predefined starting funds
    """
    num_users = 10
    starting_money = 500
    result_batch, status_batch = await amqp_client.call_worker('batch_init_users', {
        "n": num_users,
        "starting_money": starting_money
    })
    
    assert status_batch == 200
    assert result_batch.get("msg") == "Batch init for users successful"
    
    for user_id in range(num_users):
        result_find, status_find = await amqp_client.call_worker('find_user', {"user_id": str(user_id)})
        assert status_find == 200
        assert result_find.get("credit") == starting_money

@pytest.mark.asyncio
async def test_multiple_order_payment_idempotency(amqp_client):
    """
    Test idempotency across multiple orders for the same user
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]
    
    result_add, status_add = await amqp_client.call_worker('add_funds', {
        "user_id": user_id, 
        "amount": 500
    })
    assert status_add == 200
    
    orders = [
        {"amount": 50, "order_id": "order-1"},
        {"amount": 75, "order_id": "order-2"},
        {"amount": 100, "order_id": "order-3"}
    ]
    
    for order in orders:
        attempt_id1 = str(uuid.uuid4())
        result1, status1 = await amqp_client.call_worker('pay', {
            "user_id": user_id,
            "amount": order["amount"],
            "attempt_id": attempt_id1,
            "order_id": order["order_id"]
        })
        assert status1 == 200
        assert result1.get("paid") is True
        
        result2, status2 = await amqp_client.call_worker('pay', {
            "user_id": user_id,
            "amount": order["amount"],
            "attempt_id": attempt_id1,
            "order_id": order["order_id"]
        })
        assert status2 == 200
        assert result2.get("paid") is True
        
        result_find, status_find = await amqp_client.call_worker('find_user', {"user_id": user_id})
        assert status_find == 200
    
    result_final, status_final = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status_final == 200
    assert result_final.get("credit") == 500 - sum(order["amount"] for order in orders)

@pytest.mark.asyncio
async def test_large_transaction_amounts(amqp_client):
    """
    Test handling of extremely large transaction amounts
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]
    
    large_amount = 1_000_000_000  
    result_add, status_add = await amqp_client.call_worker('add_funds', {
        "user_id": user_id, 
        "amount": large_amount
    })
    assert status_add == 200
    
    result_find1, status_find1 = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status_find1 == 200
    assert result_find1.get("credit") == large_amount
    
    attempt_id = str(uuid.uuid4())
    order_id = f"order-large-{attempt_id}"
    result_pay, status_pay = await amqp_client.call_worker('pay', {
        "user_id": user_id,
        "amount": large_amount,
        "attempt_id": attempt_id,
        "order_id": order_id
    })
    assert status_pay == 200
    assert result_pay.get("paid") is True
    
    result_find2, status_find2 = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status_find2 == 200
    assert result_find2.get("credit") == 0

@pytest.mark.asyncio
async def test_overlapping_payment_attempts(amqp_client):
    """
    Test handling of near-simultaneous payment attempts
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]
    
    initial_funds = 500
    result_add, status_add = await amqp_client.call_worker('add_funds', {
        "user_id": user_id, 
        "amount": initial_funds
    })
    assert status_add == 200
    
    payment_sequence = [
        {"amount": 250, "expected": 250},   
        {"amount": 250, "expected": 0},     
        {"amount": 50, "expected": "error"} 
    ]
    
    for payment in payment_sequence:
        attempt_id = str(uuid.uuid4())
        order_id = f"order-overlap-{attempt_id}"
        
        result_pay, status_pay = await amqp_client.call_worker('pay', {
            "user_id": user_id,
            "amount": payment["amount"],
            "attempt_id": attempt_id,
            "order_id": order_id
        })
        
        if payment["expected"] == "error":
            assert status_pay == 400
            assert "error" in result_pay
            assert "insufficient credit" in str(result_pay.get("error", "")).lower()
        else:
            assert status_pay == 200
            assert result_pay.get("paid") is True
            
            result_find, status_find = await amqp_client.call_worker('find_user', {"user_id": user_id})
            assert status_find == 200
            assert result_find.get("credit") == payment["expected"]

@pytest.mark.asyncio
async def test_maximum_concurrent_users(amqp_client):
    """
    Test creating and managing a large number of concurrent users
    """
    num_users = 100
    
    async def create_user_with_funds():
        # Create user
        result_create, status_create = await amqp_client.call_worker('create_user', {})
        assert status_create == 200
        user_id = result_create["user_id"]
        
        result_add, status_add = await amqp_client.call_worker('add_funds', {
            "user_id": user_id, 
            "amount": 1000
        })
        assert status_add == 200
        
        return user_id
    
    user_ids = await asyncio.gather(*[create_user_with_funds() for _ in range(num_users)])
    
    for user_id in user_ids:
        result_find, status_find = await amqp_client.call_worker('find_user', {"user_id": user_id})
        assert status_find == 200
        assert result_find.get("credit") == 1000

@pytest.mark.asyncio
async def test_repeated_cancel_payment(amqp_client):
    """
    Test cancelling payments multiple times
    """
    result_create, status_create = await amqp_client.call_worker('create_user', {})
    assert status_create == 200
    user_id = result_create["user_id"]
    
    initial_funds = 500
    result_add, status_add = await amqp_client.call_worker('add_funds', {
        "user_id": user_id, 
        "amount": initial_funds
    })
    assert status_add == 200
    
    attempt_id = str(uuid.uuid4())
    order_id = f"order-{attempt_id}"
    result_pay, status_pay = await amqp_client.call_worker('pay', {
        "user_id": user_id,
        "amount": 200,
        "attempt_id": attempt_id,
        "order_id": order_id
    })
    assert status_pay == 200
    assert result_pay.get("paid") is True
    
    for _ in range(3):
        result_cancel, status_cancel = await amqp_client.call_worker('cancel_payment', {
            "user_id": user_id, 
            "amount": 200
        })
        
        assert status_cancel == 200
        assert result_cancel.get("refunded") is True
        
        result_find, status_find = await amqp_client.call_worker('find_user', {"user_id": user_id})
        assert status_find == 200
        assert result_find.get("credit") == initial_funds
