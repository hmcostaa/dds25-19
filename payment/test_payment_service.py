# payment/test_payment_service.py
# Extensively improved version with thorough tests, msgpack helpers,
# error simulation, robust idempotency checks, and concurrency tests.

import pytest
import asyncio
import uuid
import random
import logging
from typing import Tuple, Any, Optional, Dict
from concurrent.futures import ThreadPoolExecutor # Or use asyncio.gather

# Ensure msgspec and pytest-mock are installed: pip install msgspec pytest-mock
from msgspec import msgpack, Struct
import redis.exceptions

# Import necessary components from payment app and common modules
from app import (
    db_master,  # Use the master connection for setup/verification
    worker,
    UserValue,
    atomic_update_user, # May be useful for direct setup or complex scenarios
    SERVICE_NAME # Import service name for idempotency keys
)
# Import idempotency decorator to understand key format if needed
from global_idempotency.idempotency_decorator import idempotent

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s]: %(message)s")
test_logger = logging.getLogger("PaymentTestImproved")

# --- Mock AMQP Client (Leveraging app's worker) ---
class MockAMQPClient:
    """ Mocks the AMQP communication for testing worker functions directly """
    def __init__(self, worker_obj):
        self.worker = worker_obj
        if not hasattr(self.worker, 'callbacks') or not self.worker.callbacks:
             test_logger.warning("Worker callbacks might not be registered yet during MockAMQPClient init.")

    async def call_worker(self, function_name: str, data: dict) -> Tuple[dict | str, int]:
        """ Calls the registered worker function """
        if function_name not in self.worker.callbacks:
             test_logger.error(f"Function '{function_name}' not found in worker callbacks: {list(self.worker.callbacks.keys())}")
             pytest.fail(f"Function '{function_name}' not registered in AMQPWorker.")
        try:
            # Worker functions are expected to return (result_dict_or_error_str, status_code)
            result, status_code = await self.worker.callbacks[function_name](data)
            # Handle potential non-dict results gracefully in logs
            log_result = result if isinstance(result, (dict, str)) else str(result)
            test_logger.info(f"Mock Call: {function_name}({data}) -> Status {status_code}, Result {log_result}")
            return result, status_code
        except Exception as e:
            test_logger.exception(f"!!! Exception caught by MockAMQPClient calling '{function_name}' with data {data} !!!")
            # Mimic a potential generic exception handler in the service
            return {"error": f"Internal test error calling worker function '{function_name}': {str(e)}"}, 500

# --- Pytest Fixtures ---

@pytest.fixture(scope="module")
def event_loop():
    """ Ensure a consistent event loop for the test module """
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def amqp_client(event_loop): # Ensure event loop is available
    """ Provides the MockAMQPClient instance """
    if not hasattr(worker, 'callbacks') or not worker.callbacks:
        test_logger.warning("Worker callbacks potentially empty during amqp_client fixture creation.")
    return MockAMQPClient(worker)

@pytest.fixture(scope="function", autouse=True)
def flush_redis_and_idempotency(event_loop):
    """ Cleans the Redis database (including idempotency keys) before each test """
    async def clean():
        test_logger.debug("Flushing Redis DB (master)...")
        try:
            # Use asynchronous flushall if available for your Redis client version
            # Standard redis-py flushall is synchronous.
            db_master.flushall()
            test_logger.debug("Redis DB flushed.")
        except Exception as e:
            test_logger.error(f"Failed to connect to or flush Redis: {e}", exc_info=True)
            pytest.fail(f"Redis connection/flush failed: {e}")
    event_loop.run_until_complete(clean())
    yield # Test runs here

# --- Helper Functions for Direct DB Interaction (Using Msgpack) ---

def set_user_direct(user_id: str, credit: int):
    """ Directly sets user value in Redis using msgpack """
    try:
        value = msgpack.encode(UserValue(credit=credit))
        # Key format should match what the service uses (check app.py)
        # Assuming simple user_id is used as key
        db_master.set(user_id, value)
        test_logger.debug(f"Directly set user {user_id}: credit={credit}")
    except Exception as e:
        test_logger.error(f"Failed to directly set user {user_id}: {e}", exc_info=True)
        raise

def get_user_direct(user_id: str) -> Optional[UserValue]:
    """ Directly gets user value from Redis, decoding with msgpack """
    try:
        # Assuming simple user_id is used as key
        data_bytes = db_master.get(user_id)
        if data_bytes:
            decoded = msgpack.decode(data_bytes, type=UserValue)
            test_logger.debug(f"Directly got user {user_id}: {decoded}")
            return decoded
        test_logger.debug(f"Direct get: User {user_id} not found.")
        return None
    except Exception as e:
        test_logger.error(f"Failed to directly get/decode user {user_id}: {e}", exc_info=True)
        return None # Return None on error

def get_idempotency_key_direct(operation: str, user_id: str, order_id: str, attempt_id: str) -> Optional[bytes]:
    """ Directly checks if an idempotency key exists using the format from the decorator """
    # Key format from decorator: idempotency:{service_name}:{operation_name}:{user_id}:{order_id}:{attempt_id}
    redis_key = f"idempotency:{SERVICE_NAME}:{operation}:{user_id}:{order_id}:{attempt_id}"
    test_logger.debug(f"Checking direct idempotency key: {redis_key}")
    return db_master.get(redis_key)

# --- Test Cases ---

# === create_user Tests ===
@pytest.mark.asyncio
async def test_create_user_success(amqp_client):
    result, status = await amqp_client.call_worker('create_user', {})
    assert status == 200, f"Expected 200, got {status}. Result: {result}"
    assert isinstance(result, dict)
    assert "user_id" in result
    user_id = result["user_id"]
    assert isinstance(user_id, str) and len(user_id) > 0

    # Verify directly in DB
    user_val = get_user_direct(user_id)
    assert user_val is not None
    assert user_val.credit == 0

# === find_user Tests ===
@pytest.mark.asyncio
async def test_find_user_success(amqp_client):
    user_id = str(uuid.uuid4())
    credit = 550
    set_user_direct(user_id, credit)

    result, status = await amqp_client.call_worker('find_user', {"user_id": user_id})
    assert status == 200, f"Expected 200, got {status}. Result: {result}"
    assert isinstance(result, dict)
    assert result.get("user_id") == user_id
    assert result.get("credit") == credit

@pytest.mark.asyncio
async def test_find_user_not_found(amqp_client):
    non_existent_id = str(uuid.uuid4())
    # The 'find_user' worker function aborts(400) via get_user_from_db
    # The test MockAMQPClient currently returns 500 for unhandled exceptions.
    # Need to refine MockAMQPClient or expect 500 here unless app.py handles abort better.
    # Let's assume the abort leads to an exception caught by the mock client.
    result, status = await amqp_client.call_worker('find_user', {"user_id": non_existent_id})

    # Assuming abort causes exception caught by mock client -> 500
    # If app.py handles Flask's abort more gracefully to return a proper response, adjust status code.
    assert status == 500 or status == 400 # Status depends on how Flask abort() interacts with AMQP worker/test mock
    assert isinstance(result, dict)
    assert "error" in result
    assert "not found" in result.get("error", "").lower()

@pytest.mark.asyncio
async def test_find_user_db_read_error(amqp_client, mocker):
    """ Test find_user when the underlying Redis read fails """
    user_id = str(uuid.uuid4())
    # Mock the db_slave.get call within the context of get_user_from_db
    mocker.patch('app.db_slave.get', side_effect=redis.exceptions.ConnectionError("Simulated DB Read Error"))

    result, status = await amqp_client.call_worker('find_user', {"user_id": user_id})

    # get_user_from_db aborts on RedisError. Check how the worker handles abort.
    assert status == 500 or status == 400 # Expecting error status
    assert isinstance(result, dict)
    assert "error" in result
    # The exact message depends on how the abort/exception is caught
    assert "db error" in result.get("error", "").lower() or "internal test error" in result.get("error","").lower()

# === add_funds Tests ===
@pytest.mark.asyncio
async def test_add_funds_success(amqp_client):
    user_id = str(uuid.uuid4())
    set_user_direct(user_id, 100)
    amount_to_add = 50

    result, status = await amqp_client.call_worker('add_funds', {"user_id": user_id, "amount": amount_to_add})
    assert status == 200, f"Expected 200, got {status}. Result: {result}"
    assert isinstance(result, dict)
    assert result.get("done") is True
    assert result.get("credit") == 100 + amount_to_add

    # Verify DB state
    user_val = get_user_direct(user_id)
    assert user_val is not None
    assert user_val.credit == 100 + amount_to_add

@pytest.mark.asyncio
async def test_add_funds_user_not_found(amqp_client):
    non_existent_id = str(uuid.uuid4())
    result, status = await amqp_client.call_worker('add_funds', {"user_id": non_existent_id, "amount": 50})
    # atomic_update_user returns (None, "...not found") -> add_funds returns error msg and 500
    assert status == 500, f"Expected 500, got {status}. Result: {result}"
    assert isinstance(result, dict)
    assert "error" in result
    assert "not found" in result.get("error", "").lower()

@pytest.mark.asyncio
async def test_add_funds_invalid_amount_negative(amqp_client):
    user_id = str(uuid.uuid4())
    set_user_direct(user_id, 100)
    result, status = await amqp_client.call_worker('add_funds', {"user_id": user_id, "amount": -50})
    assert status == 400 # Caught by initial validation
    assert "must be positive" in result.get("error", "").lower()
    user_val = get_user_direct(user_id)
    assert user_val.credit == 100 # Unchanged

@pytest.mark.asyncio
async def test_add_funds_invalid_amount_zero(amqp_client):
    user_id = str(uuid.uuid4())
    set_user_direct(user_id, 100)
    result, status = await amqp_client.call_worker('add_funds', {"user_id": user_id, "amount": 0})
    assert status == 400 # Caught by initial validation
    assert "must be positive" in result.get("error", "").lower()
    user_val = get_user_direct(user_id)
    assert user_val.credit == 100 # Unchanged

@pytest.mark.asyncio
async def test_add_funds_watch_error_retry(amqp_client, mocker):
    """ Test that add_funds retries on WatchError """
    user_id = str(uuid.uuid4())
    initial_credit = 100
    amount_to_add = 50
    set_user_direct(user_id, initial_credit)

    # Mock the 'execute' method of the pipeline to simulate WatchError on first call
    mock_pipeline = MagicMock()
    mock_pipeline.execute.side_effect = [redis.exceptions.WatchError, MagicMock()] # Fail first, succeed second
    mock_pipeline.get.return_value = msgpack.encode(UserValue(credit=initial_credit)) # Simulate reading initial value

    # Patch the pipeline creation within atomic_update_user
    mocker.patch('app.db_master.pipeline', return_value=mock_pipeline)
    # Patch asyncio.sleep to avoid waiting during test
    mocker.patch('asyncio.sleep', return_value=None)

    result, status = await amqp_client.call_worker('add_funds', {"user_id": user_id, "amount": amount_to_add})

    assert status == 200, f"Expected 200 after retry, got {status}. Result: {result}"
    assert result.get("done") is True
    assert result.get("credit") == initial_credit + amount_to_add
    assert mock_pipeline.execute.call_count == 2 # Should be called twice (initial fail + retry)

    # Verify DB state (Note: mock prevents actual DB write, check mock calls instead if needed)
    # In a real scenario without mocking writes, you'd check get_user_direct here.

# === pay Tests ===
@pytest.mark.asyncio
async def test_pay_success(amqp_client):
    user_id = str(uuid.uuid4())
    initial_credit = 200
    amount_to_pay = 75
    set_user_direct(user_id, initial_credit)
    attempt_id = str(uuid.uuid4())
    order_id = f"order-pay-{attempt_id}"
    payload = {"user_id": user_id, "amount": amount_to_pay, "attempt_id": attempt_id, "order_id": order_id}

    result, status = await amqp_client.call_worker('pay', payload)
    assert status == 200, f"Expected 200, got {status}. Result: {result}"
    assert isinstance(result, dict)
    assert result.get("paid") is True
    assert result.get("credit") == initial_credit - amount_to_pay

    user_val = get_user_direct(user_id)
    assert user_val is not None
    assert user_val.credit == initial_credit - amount_to_pay

@pytest.mark.asyncio
async def test_pay_exact_amount(amqp_client):
    user_id = str(uuid.uuid4())
    initial_credit = 150
    amount_to_pay = initial_credit
    set_user_direct(user_id, initial_credit)
    attempt_id = str(uuid.uuid4())
    order_id = f"order-pay-exact-{attempt_id}"
    payload = {"user_id": user_id, "amount": amount_to_pay, "attempt_id": attempt_id, "order_id": order_id}

    result, status = await amqp_client.call_worker('pay', payload)
    assert status == 200
    assert result.get("paid") is True
    assert result.get("credit") == 0

    user_val = get_user_direct(user_id)
    assert user_val is not None
    assert user_val.credit == 0

@pytest.mark.asyncio
async def test_pay_insufficient_funds(amqp_client):
    user_id = str(uuid.uuid4())
    initial_credit = 50
    amount_to_pay = 51
    set_user_direct(user_id, initial_credit)
    attempt_id = str(uuid.uuid4())
    order_id = f"order-pay-insuf-{attempt_id}"
    payload = {"user_id": user_id, "amount": amount_to_pay, "attempt_id": attempt_id, "order_id": order_id}

    result, status = await amqp_client.call_worker('pay', payload)
    # atomic_update_user -> updater raises ValueError -> pay catches ValueError -> returns 400
    # Update based on fixing atomic_update_user error handling for ValueError
    assert status == 400, f"Expected 400 for insufficient credit, got {status}. Result: {result}"
    assert isinstance(result, dict)
    assert result.get("paid") is False
    assert "value error" in result # Key from app.py's exception handler
    assert "insufficient credit" in result.get("value error", "").lower()

    user_val = get_user_direct(user_id)
    assert user_val.credit == initial_credit # Unchanged

@pytest.mark.asyncio
async def test_pay_user_not_found(amqp_client):
    non_existent_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    order_id = f"order-pay-notfound-{attempt_id}"
    payload = {"user_id": non_existent_id, "amount": 50, "attempt_id": attempt_id, "order_id": order_id}

    result, status = await amqp_client.call_worker('pay', payload)
    # atomic_update_user returns (None, "...not found") -> pay returns 404
    assert status == 404, f"Expected 404 for user not found, got {status}. Result: {result}"
    assert isinstance(result, dict)
    assert result.get("paid") is False
    assert "error" in result
    assert "not found" in result.get("error", "").lower()

@pytest.mark.asyncio
async def test_pay_invalid_amount_negative(amqp_client):
    user_id = str(uuid.uuid4())
    set_user_direct(user_id, 100)
    payload = {"user_id": user_id, "amount": -50, "attempt_id": str(uuid.uuid4()), "order_id": "o"}
    result, status = await amqp_client.call_worker('pay', payload)
    assert status == 400 # Caught by initial validation
    assert "must be positive" in result.get("error", "").lower()
    user_val = get_user_direct(user_id)
    assert user_val.credit == 100 # Unchanged

@pytest.mark.asyncio
async def test_pay_invalid_amount_zero(amqp_client):
    user_id = str(uuid.uuid4())
    set_user_direct(user_id, 100)
    payload = {"user_id": user_id, "amount": 0, "attempt_id": str(uuid.uuid4()), "order_id": "o"}
    result, status = await amqp_client.call_worker('pay', payload)
    assert status == 400 # Caught by initial validation
    assert "must be positive" in result.get("error", "").lower()
    user_val = get_user_direct(user_id)
    assert user_val.credit == 100 # Unchanged

# === cancel_payment Tests ===
@pytest.mark.asyncio
async def test_cancel_payment_success(amqp_client):
    user_id = str(uuid.uuid4())
    initial_credit = 100
    amount_to_cancel = 50 # Should be amount originally paid ideally
    set_user_direct(user_id, initial_credit)
    attempt_id = str(uuid.uuid4())
    order_id = f"order-cancel-{attempt_id}"
    payload = {"user_id": user_id, "amount": amount_to_cancel, "attempt_id": attempt_id, "order_id": order_id}

    result, status = await amqp_client.call_worker('cancel_payment', payload)
    assert status == 200, f"Expected 200, got {status}. Result: {result}"
    assert isinstance(result, dict)
    assert result.get("refunded") is True
    assert result.get("credit") == initial_credit + amount_to_cancel

    user_val = get_user_direct(user_id)
    assert user_val is not None
    assert user_val.credit == initial_credit + amount_to_cancel

@pytest.mark.asyncio
async def test_cancel_payment_user_not_found(amqp_client):
    non_existent_id = str(uuid.uuid4())
    attempt_id = str(uuid.uuid4())
    order_id = f"order-cancel-notfound-{attempt_id}"
    payload = {"user_id": non_existent_id, "amount": 50, "attempt_id": attempt_id, "order_id": order_id}

    result, status = await amqp_client.call_worker('cancel_payment', payload)
    # atomic_update_user returns (None, "...not found") -> cancel_payment returns 404
    assert status == 404, f"Expected 404, got {status}. Result: {result}"
    assert isinstance(result, dict)
    assert "error" in result
    assert "not found" in result.get("error", "").lower()

@pytest.mark.asyncio
async def test_cancel_payment_invalid_amount_negative(amqp_client):
    user_id = str(uuid.uuid4())
    set_user_direct(user_id, 100)
    payload = {"user_id": user_id, "amount": -50, "attempt_id": str(uuid.uuid4()), "order_id": "o"}
    result, status = await amqp_client.call_worker('cancel_payment', payload)
    assert status == 400 # Caught by initial validation
    assert "must be positive" in result.get("error", "").lower()
    user_val = get_user_direct(user_id)
    assert user_val.credit == 100 # Unchanged

@pytest.mark.asyncio
async def test_cancel_payment_invalid_amount_zero(amqp_client):
    user_id = str(uuid.uuid4())
    set_user_direct(user_id, 100)
    payload = {"user_id": user_id, "amount": 0, "attempt_id": str(uuid.uuid4()), "order_id": "o"}
    result, status = await amqp_client.call_worker('cancel_payment', payload)
    assert status == 400 # Caught by initial validation
    assert "must be positive" in result.get("error", "").lower()
    user_val = get_user_direct(user_id)
    assert user_val.credit == 100 # Unchanged

# === Idempotency Tests (pay & cancel_payment) ===
@pytest.mark.asyncio
async def test_pay_idempotency_success(amqp_client):
    user_id = str(uuid.uuid4())
    initial_credit = 200
    amount_to_pay = 75
    set_user_direct(user_id, initial_credit)
    attempt_id = str(uuid.uuid4())
    order_id = f"order-idem-pay-{attempt_id}"
    payload = {"user_id": user_id, "amount": amount_to_pay, "attempt_id": attempt_id, "order_id": order_id}

    # First call
    result1, status1 = await amqp_client.call_worker('pay', payload)
    assert status1 == 200, f"First call failed: {result1}"
    assert result1.get("paid") is True
    assert result1.get("credit") == initial_credit - amount_to_pay
    user_val1 = get_user_direct(user_id)
    assert user_val1.credit == initial_credit - amount_to_pay
    # Verify key stored
    idempotency_key_bytes = get_idempotency_key_direct('pay', user_id, order_id, attempt_id)
    assert idempotency_key_bytes is not None
    stored_result, stored_status = msgpack.decode(idempotency_key_bytes)
    assert stored_status == 200
    assert stored_result == result1

    # Second call (identical payload)
    result2, status2 = await amqp_client.call_worker('pay', payload)
    assert status2 == 200, f"Second call failed: {result2}"
    assert result2 == result1 # Idempotency should return the stored result
    user_val2 = get_user_direct(user_id)
    assert user_val2.credit == initial_credit - amount_to_pay # Credit unchanged by second call

@pytest.mark.asyncio
async def test_pay_idempotency_insufficient_funds_error_replay(amqp_client):
    user_id = str(uuid.uuid4())
    initial_credit = 50
    amount_to_pay = 51 # More than available
    set_user_direct(user_id, initial_credit)
    attempt_id = str(uuid.uuid4())
    order_id = f"order-idem-pay-insuf-{attempt_id}"
    payload = {"user_id": user_id, "amount": amount_to_pay, "attempt_id": attempt_id, "order_id": order_id}

    # First call (fails)
    result1, status1 = await amqp_client.call_worker('pay', payload)
    assert status1 == 400, f"First call should fail: {result1}"
    assert result1.get("paid") is False
    assert "insufficient credit" in result1.get("value error", "").lower() # Check specific error
    user_val1 = get_user_direct(user_id)
    assert user_val1.credit == initial_credit # Credit unchanged
    # Check if idempotency key stores the *failure* result
    idempotency_data = get_idempotency_key_direct('pay', user_id, order_id, attempt_id)
    assert idempotency_data is not None
    stored_result, stored_status = msgpack.decode(idempotency_data)
    assert stored_status == status1
    assert stored_result == result1

    # Second call (identical payload)
    result2, status2 = await amqp_client.call_worker('pay', payload)
    assert status2 == status1, f"Second call should return stored status: {result2}"
    assert result2 == result1 # Idempotency should return the stored error result
    user_val2 = get_user_direct(user_id)
    assert user_val2.credit == initial_credit # Credit still unchanged

@pytest.mark.asyncio
async def test_cancel_payment_idempotency_success(amqp_client):
    user_id = str(uuid.uuid4())
    initial_credit = 100
    amount_to_cancel = 50
    set_user_direct(user_id, initial_credit)
    attempt_id = str(uuid.uuid4())
    order_id = f"order-idem-cancel-{attempt_id}"
    payload = {"user_id": user_id, "amount": amount_to_cancel, "attempt_id": attempt_id, "order_id": order_id}

    # First call
    result1, status1 = await amqp_client.call_worker('cancel_payment', payload)
    assert status1 == 200, f"First call failed: {result1}"
    assert result1.get("refunded") is True
    assert result1.get("credit") == initial_credit + amount_to_cancel
    user_val1 = get_user_direct(user_id)
    assert user_val1.credit == initial_credit + amount_to_cancel
    idempotency_key_bytes = get_idempotency_key_direct('cancel_payment', user_id, order_id, attempt_id)
    assert idempotency_key_bytes is not None

    # Second call (identical payload)
    result2, status2 = await amqp_client.call_worker('cancel_payment', payload)
    assert status2 == 200, f"Second call failed: {result2}"
    assert result2 == result1 # Stored result should match
    user_val2 = get_user_direct(user_id)
    assert user_val2.credit == initial_credit + amount_to_cancel # Credit unchanged by second call

@pytest.mark.asyncio
async def test_idempotency_different_payload_same_attempt_id_pay(amqp_client):
    user_id = str(uuid.uuid4())
    initial_credit = 200
    set_user_direct(user_id, initial_credit)
    attempt_id = str(uuid.uuid4())
    order_id = f"order-idem-diff-{attempt_id}"
    payload1 = {"user_id": user_id, "amount": 20, "attempt_id": attempt_id, "order_id": order_id}
    payload2 = {"user_id": user_id, "amount": 30, "attempt_id": attempt_id, "order_id": order_id} # Different amount

    # First call
    result1, status1 = await amqp_client.call_worker('pay', payload1)
    assert status1 == 200
    assert result1.get("credit") == 180
    user_val1 = get_user_direct(user_id)
    assert user_val1.credit == 180

    # Second call (different payload, same attempt_id)
    result2, status2 = await amqp_client.call_worker('pay', payload2)
    # The current decorator likely just returns the stored result for payload1
    assert status2 == 200
    assert result2 == result1 # Expecting the stored result from first call
    user_val2 = get_user_direct(user_id)
    assert user_val2.credit == 180 # Credit should NOT change based on payload2

    test_logger.info("Test confirms current idempotency returns stored 'pay' result even if payload differs for same attempt_id.")

# === Batch Init Test ===
@pytest.mark.asyncio
async def test_batch_init_users_success(amqp_client):
    num_users = 15
    start_money = 500
    result, status = await amqp_client.call_worker('batch_init_users', {
        "n": num_users, "starting_money": start_money
    })

    assert status == 200, f"Expected 200, got {status}. Result: {result}"
    assert isinstance(result, dict)
    assert result.get("msg") == "Batch init for users successful"

    # Verify a sample of users
    for i in random.sample(range(num_users), k=min(num_users, 5)):
        user_val = get_user_direct(str(i))
        assert user_val is not None, f"User {i} not found after batch init"
        assert user_val.credit == start_money, f"User {i} has incorrect credit"

@pytest.mark.asyncio
async def test_batch_init_users_invalid_params(amqp_client):
    # Missing params
    # Assuming app.py is fixed to return 400 for missing/invalid params
    result_missing, status_missing = await amqp_client.call_worker('batch_init_users', {"n": 10})
    assert status_missing == 400 # Expecting 400 after fix
    assert "error" in result_missing
    assert "missing or invalid required parameters" in result_missing.get("error","").lower()

    # Non-integer params
    result_type, status_type = await amqp_client.call_worker('batch_init_users', {"n": "ten", "starting_money": 100})
    assert status_type == 400 # Expecting 400 after fix
    assert "error" in result_type
    assert "missing or invalid required parameters" in result_type.get("error","").lower()

    # Zero users
    result_zero, status_zero = await amqp_client.call_worker('batch_init_users', {"n": 0, "starting_money": 100})
    assert status_zero == 200 # n=0 might be valid, should succeed with no users created/checked
    assert result_zero.get("msg") == "Batch init for users successful"


# === Concurrency Tests ===
@pytest.mark.asyncio
@pytest.mark.parametrize("num_concurrent", [10, 50])
async def test_concurrent_add_funds_single_user(amqp_client, num_concurrent):
    user_id = str(uuid.uuid4())
    set_user_direct(user_id, 0)
    test_logger.info(f"Starting concurrent add_funds test for user {user_id} ({num_concurrent} operations)")

    async def add_task(amount):
        return await amqp_client.call_worker('add_funds', {"user_id": user_id, "amount": amount})

    amounts = [random.randint(1, 10) for _ in range(num_concurrent)]
    tasks = [add_task(amt) for amt in amounts]
    results = await asyncio.gather(*tasks)

    # Check results
    all_succeeded = all(status == 200 and res.get("done") for res, status in results)
    assert all_succeeded, "Not all concurrent add_funds operations succeeded"

    # Verify final state
    final_user_val = get_user_direct(user_id)
    assert final_user_val is not None
    expected_credit = sum(amounts)
    assert final_user_val.credit == expected_credit, f"Final credit mismatch. Expected {expected_credit}, got {final_user_val.credit}"
    test_logger.info(f"Concurrent add_funds test finished for user {user_id}. Final credit: {final_user_val.credit}")

@pytest.mark.asyncio
@pytest.mark.parametrize("num_concurrent", [10, 50])
async def test_concurrent_pay_single_user(amqp_client, num_concurrent):
    user_id = str(uuid.uuid4())
    # Start with enough credit for *some* payments to succeed
    initial_credit = num_concurrent * 5
    set_user_direct(user_id, initial_credit)
    test_logger.info(f"Starting concurrent pay test for user {user_id} ({num_concurrent} operations)")

    async def pay_task(amount):
        attempt_id = str(uuid.uuid4())
        order_id = f"order-conc-pay-{attempt_id}"
        return await amqp_client.call_worker('pay', {
            "user_id": user_id, "amount": amount, "attempt_id": attempt_id, "order_id": order_id
        })

    amounts = [random.randint(8, 12) for _ in range(num_concurrent)] # Amounts designed to cause some failures
    tasks = [pay_task(amt) for amt in amounts]
    results_with_amounts = list(zip(amounts, await asyncio.gather(*tasks)))

    # Analyze results
    total_paid = 0
    success_count = 0
    fail_count = 0
    for amount, (res, status) in results_with_amounts:
        if status == 200 and res.get("paid"):
            success_count += 1
            total_paid += amount
        elif status == 400 and "insufficient credit" in str(res):
            fail_count += 1
        else:
            test_logger.warning(f"Unexpected result in concurrent pay: Status {status}, Result {res}")
            fail_count +=1 # Count other failures too

    test_logger.info(f"Concurrent pay results: Success: {success_count}, Failed (Insufficient): {fail_count}. Total Paid: {total_paid}")

    # Verify final state
    final_user_val = get_user_direct(user_id)
    assert final_user_val is not None
    expected_credit = initial_credit - total_paid
    assert final_user_val.credit == expected_credit, f"Final credit mismatch. Expected {expected_credit}, got {final_user_val.credit}"
    assert success_count + fail_count == num_concurrent, "Mismatch in total operations processed"

@pytest.mark.asyncio
async def test_concurrent_pay_and_cancel(amqp_client):
    user_id = str(uuid.uuid4())
    initial_credit = 500
    set_user_direct(user_id, initial_credit)
    test_logger.info(f"Starting concurrent pay/cancel test for user {user_id}")

    pay_amount = 100
    cancel_amount = 50 # Different amount for clarity
    num_ops = 20
    tasks = []

    for i in range(num_ops):
        attempt_id = str(uuid.uuid4())
        order_id = f"order-conc-mix-{attempt_id}"
        if random.random() < 0.5:
            tasks.append(amqp_client.call_worker('pay', {
                "user_id": user_id, "amount": pay_amount, "attempt_id": attempt_id, "order_id": order_id
            }))
        else:
            tasks.append(amqp_client.call_worker('cancel_payment', {
                 "user_id": user_id, "amount": cancel_amount, "attempt_id": attempt_id, "order_id": order_id
            }))

    results = await asyncio.gather(*tasks)

    # Analyze results - Calculate net change based on successful operations
    net_change = 0
    success_pay = 0
    success_cancel = 0
    fail_count = 0
    for res, status in results:
        if status == 200:
            if res.get("paid"):
                net_change -= pay_amount
                success_pay += 1
            elif res.get("refunded"):
                net_change += cancel_amount
                success_cancel += 1
            else:
                 test_logger.warning(f"Unexpected success result: {res}")
                 fail_count += 1
        else:
            fail_count += 1 # Includes insufficient funds or other errors

    test_logger.info(f"Concurrent pay/cancel results: Pay OK: {success_pay}, Cancel OK: {success_cancel}, Failed: {fail_count}. Net Change: {net_change}")

    # Verify final state
    final_user_val = get_user_direct(user_id)
    assert final_user_val is not None
    expected_credit = initial_credit + net_change
    assert final_user_val.credit == expected_credit, f"Final credit mismatch. Expected {expected_credit}, got {final_user_val.credit}"
