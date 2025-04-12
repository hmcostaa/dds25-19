import pytest
import asyncio
import uuid
import os
import logging
import random
import time
from typing import Dict, Any, Tuple, Optional

# Assuming your service code (app.py) is adjacent or in PYTHONPATH
# Adjust import paths if necessary
from stock.app import (
    worker, StockValue, SERVICE_NAME,
    db_master as service_db_master, # Use the actual connection used by the service
    idempotency_db_conn as service_idempotency_db_conn,
    StockValue,
    MsgspecDecodeError
)
from msgspec import msgpack

# --- Test Configuration ---
# Use separate test Redis connections if possible, otherwise use service connections carefully
# For simplicity here, we assume direct use/flushing of service DBs IS INTENDED FOR THESE TESTS
# WARNING: This will clear data used by the running service if pointed to the same DB!
test_db_master = service_db_master
test_idempotency_db_conn = service_idempotency_db_conn

# --- Logging Setup ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-5s %(name)s:%(filename)s:%(lineno)d - %(message)s"
)
log = logging.getLogger("StockTestImproved")

# --- Test Helper Functions ---

def set_stock_direct(item_id: str, stock: int, price: int):
    """Directly sets a stock item value in Redis for test setup."""
    try:
        value = msgpack.encode(StockValue(stock=stock, price=price))
        test_db_master.set(item_id, value)
        log.debug(f"Directly set stock for {item_id}: stock={stock}, price={price}")
    except Exception as e:
        log.error(f"Failed to directly set stock for {item_id}: {e}")
        raise

def get_stock_direct(item_id: str) -> Optional[StockValue]:
    """Directly retrieves and decodes a stock item from Redis for verification."""
    try:
        entry_bytes = test_db_master.get(item_id)
        if entry_bytes:
            decoded_entry = msgpack.decode(entry_bytes, type=StockValue)
            log.debug(f"Directly got stock for {item_id}: {decoded_entry}")
            return decoded_entry
        log.debug(f"Directly got stock for {item_id}: Not Found (None)")
        return None
    except MsgspecDecodeError:
        log.error(f"Direct get decode error for item {item_id}: {entry_bytes}")
        return None # Or raise an error, depending on desired test behavior
    except Exception as e:
        log.error(f"Failed to directly get stock for {item_id}: {e}")
        raise

def get_idempotency_key_direct(func_name: str, item_id: Optional[str], order_id: Optional[str], attempt_id: str):
    """
    Directly retrieves the raw idempotency data from Redis.
    Constructs the key based on the decorator's likely logic.
    """
    # Key format observed in decorator logs: idempotency:{service}:{func}:{item_id}:{order_id}:{attempt_id}
    # Handle cases where item_id or order_id might not be part of the key for some functions (adjust if needed)
    key_parts = ["idempotency", SERVICE_NAME, func_name]
    if item_id: key_parts.append(item_id)
    if order_id: key_parts.append(order_id)
    key_parts.append(attempt_id)
    key = ":".join(key_parts)

    log.debug(f"Test helper looking for idempotency key: {key}")
    try:
        data = test_idempotency_db_conn.get(key)
        if data:
            log.debug(f"Test helper found data for key {key}: {data}")
            # You might need to decode this data depending on how the decorator stores it
            # For now, just return the raw bytes/string to check existence
            return data
        else:
            log.debug(f"Test helper found NO data for key {key}")
            return None
    except Exception as e:
        log.error(f"Test helper error getting idempotency key {key}: {e}")
        return None


# --- Mock AMQP Client ---
class MockAMQPClient:
    """Simulates AMQP calls by directly invoking worker callbacks."""
    def __init__(self, worker_instance):
        self.worker = worker_instance

    async def call_worker(self, function_name: str, data: Dict[str, Any]) -> Tuple[Any, int]:
        if function_name not in self.worker.callbacks:
            log.error(f"Function '{function_name}' not registered in worker.")
            return {"error": f"Function '{function_name}' not found"}, 404
        try:
            log.info(f"Mock Call: {function_name}({data})")
            # Directly await the callback coroutine
            result, status_code = await self.worker.callbacks[function_name](data)
            log.info(f"Mock Call: {function_name}(...) -> Status {status_code}, Result {result}")
            return result, status_code
        except Exception as e:
            # Catch exceptions happening *within* the service function if not handled there
            log.error(f"!!! Exception caught by MockAMQPClient calling '{function_name}' with data {data} !!!", exc_info=True)
            return {"error": f"Unhandled exception in worker: {e}"}, 500

# --- Pytest Fixtures ---

@pytest.fixture(scope="module")
def event_loop():
    """Overrides pytest default loop."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="class")
def amqp_client(event_loop):
    """Provides a mock AMQP client instance for the test class."""
    # Ensure the worker has loaded its callbacks if that happens dynamically
    # If callbacks are registered at import time, this is usually fine
    log.info("Setting up MockAMQPClient")
    client = MockAMQPClient(worker)
    return client

@pytest.fixture(autouse=True, scope="function")
async def manage_redis_state():
    """Cleans Redis before each test function."""
    log.debug("Flushing Redis DB (master)...")
    # Be VERY careful with flushdb in production environments!
    test_db_master.flushdb()
    # Flush idempotency DB too if it's separate and needs cleaning
    if test_idempotency_db_conn != test_db_master:
         log.debug("Flushing Redis DB (idempotency)...")
         test_idempotency_db_conn.flushdb()
    log.debug("Redis DB flushed.")
    yield # Run the test
    # No cleanup needed after as we flush before the *next* test


# --- Test Class ---
@pytest.mark.usefixtures("manage_redis_state")
class TestStockService:

    @pytest.mark.asyncio
    async def test_create_item_success(self, amqp_client):
        price = 100
        payload = {"price": price}
        result, status = await amqp_client.call_worker('create_item', payload)

        assert status == 200
        assert "item_id" in result
        item_id = result["item_id"]

        # Verify directly in DB
        stock_val = get_stock_direct(item_id)
        assert stock_val is not None
        assert stock_val.stock == 0 # Initial stock
        assert stock_val.price == price

    @pytest.mark.asyncio
    async def test_find_item_success(self, amqp_client):
        item_id = str(uuid.uuid4())
        initial_stock = 50
        price = 75
        set_stock_direct(item_id, initial_stock, price)

        payload = {"item_id": item_id}
        result, status = await amqp_client.call_worker('find_item', payload)

        assert status == 200
        assert result == {"stock": initial_stock, "price": price}

    @pytest.mark.asyncio
    async def test_find_item_not_found(self, amqp_client):
        item_id = str(uuid.uuid4()) # Does not exist
        payload = {"item_id": item_id}
        result, status = await amqp_client.call_worker('find_item', payload)

        assert status == 400 # Should be 400 or 404 based on get_item_from_db logic
        assert "not found" in result.lower() # Check error message

    @pytest.mark.asyncio
    async def test_add_stock_success(self, amqp_client):
        item_id = str(uuid.uuid4())
        initial_stock = 100
        price = 50
        set_stock_direct(item_id, initial_stock, price)
        amount_to_add = 25
        attempt_id = str(uuid.uuid4())
        order_id = f"order-add-{attempt_id}"
        payload = {"item_id": item_id, "amount": amount_to_add, "attempt_id": attempt_id, "order_id": order_id}

        result, status = await amqp_client.call_worker('add_stock', payload)

        assert status == 200, f"Expected status 200, got {status}. Result: {result}"
        assert isinstance(result, dict)
        assert result.get("added") is True
        assert result.get("stock") == initial_stock + amount_to_add

        # Verify directly in DB
        stock_val = get_stock_direct(item_id)
        assert stock_val is not None
        assert stock_val.stock == initial_stock + amount_to_add

    @pytest.mark.asyncio
    async def test_add_stock_negative_amount(self, amqp_client):
        item_id = str(uuid.uuid4())
        set_stock_direct(item_id, 100, 50)
        attempt_id = str(uuid.uuid4())
        order_id = f"order-add-neg-{attempt_id}"
        payload = {"item_id": item_id, "amount": -10, "attempt_id": attempt_id, "order_id": order_id}
        result, status = await amqp_client.call_worker('add_stock', payload)
        assert status == 400
        assert "must be positive" in result.get("error", "").lower()

    @pytest.mark.asyncio
    async def test_add_stock_item_not_found(self, amqp_client):
        item_id = str(uuid.uuid4()) # Does not exist
        attempt_id = str(uuid.uuid4())
        order_id = f"order-add-404-{attempt_id}"
        payload = {"item_id": item_id, "amount": 10, "attempt_id": attempt_id, "order_id": order_id}
        result, status = await amqp_client.call_worker('add_stock', payload)
        assert status == 404 # atomic_update_item should return "not found", leading to 404
        assert "not found" in result.get("error", "").lower()

    @pytest.mark.asyncio
    async def test_remove_stock_success(self, amqp_client):
        item_id = str(uuid.uuid4())
        initial_stock = 100
        price = 50
        set_stock_direct(item_id, initial_stock, price)
        amount_to_remove = 30
        attempt_id = str(uuid.uuid4())
        order_id = f"order-rem-{attempt_id}"
        payload = {"item_id": item_id, "amount": amount_to_remove, "attempt_id": attempt_id, "order_id": order_id}

        result, status = await amqp_client.call_worker('remove_stock', payload)

        assert status == 200, f"Expected status 200, got {status}. Result: {result}"
        assert isinstance(result, dict)
        # --- ASSERT CORRECTED RETURN VALUE ---
        assert result.get("removed") is True # Check 'removed' key
        assert result.get("stock") == initial_stock - amount_to_remove # Check 'stock' key

        # Verify directly in DB
        stock_val = get_stock_direct(item_id)
        assert stock_val is not None
        assert stock_val.stock == initial_stock - amount_to_remove

    @pytest.mark.asyncio
    async def test_remove_stock_exact_amount(self, amqp_client):
        item_id = str(uuid.uuid4())
        initial_stock = 42
        price = 50
        set_stock_direct(item_id, initial_stock, price)
        amount_to_remove = initial_stock
        attempt_id = str(uuid.uuid4())
        order_id = f"order-rem-exact-{attempt_id}"
        payload = {"item_id": item_id, "amount": amount_to_remove, "attempt_id": attempt_id, "order_id": order_id}

        result, status = await amqp_client.call_worker('remove_stock', payload)

        assert status == 200
        assert result.get("removed") is True
        assert result.get("stock") == 0

        # Verify directly in DB
        stock_val = get_stock_direct(item_id)
        assert stock_val is not None
        assert stock_val.stock == 0

    @pytest.mark.asyncio
    async def test_remove_stock_insufficient(self, amqp_client):
        item_id = str(uuid.uuid4())
        initial_stock = 10
        price = 50
        set_stock_direct(item_id, initial_stock, price)
        amount_to_remove = 11 # More than available
        attempt_id = str(uuid.uuid4())
        order_id = f"order-rem-insuf-{attempt_id}"
        payload = {"item_id": item_id, "amount": amount_to_remove, "attempt_id": attempt_id, "order_id": order_id}

        result, status = await amqp_client.call_worker('remove_stock', payload)

        assert status == 400, f"Expected status 400 for insufficient stock, got {status}. Result: {result}"
        assert "insufficient stock" in result.get("error", "").lower()

        # Verify stock unchanged in DB
        stock_val = get_stock_direct(item_id)
        assert stock_val is not None
        assert stock_val.stock == initial_stock

    @pytest.mark.asyncio
    async def test_remove_stock_negative_amount(self, amqp_client):
        item_id = str(uuid.uuid4())
        set_stock_direct(item_id, 100, 50)
        attempt_id = str(uuid.uuid4())
        order_id = f"order-rem-neg-{attempt_id}"
        payload = {"item_id": item_id, "amount": -10, "attempt_id": attempt_id, "order_id": order_id}
        result, status = await amqp_client.call_worker('remove_stock', payload)
        assert status == 400
        assert "must be positive" in result.get("error", "").lower()

    @pytest.mark.asyncio
    async def test_remove_stock_item_not_found(self, amqp_client):
        item_id = str(uuid.uuid4()) # Does not exist
        attempt_id = str(uuid.uuid4())
        order_id = f"order-rem-404-{attempt_id}"
        payload = {"item_id": item_id, "amount": 10, "attempt_id": attempt_id, "order_id": order_id}
        result, status = await amqp_client.call_worker('remove_stock', payload)
        assert status == 404 # atomic_update_item returns "not found", service returns 404
        assert "not found" in result.get("error", "").lower()

    # --- Batch Init Tests ---
    @pytest.mark.asyncio
    async def test_batch_init_stock_success(self, amqp_client):
        n = 5
        starting_stock = 50
        item_price = 25
        payload = {"n": n, "starting_stock": starting_stock, "item_price": item_price}
        result, status = await amqp_client.call_worker('batch_init_stock', payload)

        assert status == 200
        assert "msg" in result
        # Extract keys from the message - brittle, depends on exact msg format
        try:
             # Assuming msg format is like "{'msg': "dict_keys(['uuid1', 'uuid2', ...])"}"
             keys_str = result['msg'].split("(['")[1].split("'])")[0]
             created_keys = keys_str.split("', '")
             assert len(created_keys) == n
             log.info(f"Batch init created {len(created_keys)} keys.")

             # Verify one item
             item_id_to_check = created_keys[0]
             stock_val = get_stock_direct(item_id_to_check)
             assert stock_val is not None
             assert stock_val.stock == starting_stock
             assert stock_val.price == item_price
        except Exception as e:
             pytest.fail(f"Could not parse keys from result message: {result.get('msg')}, error: {e}")

    @pytest.mark.asyncio
    async def test_batch_init_stock_invalid_params(self, amqp_client):
        # Missing params
        payload_missing = {"n": 10, "starting_stock": 50} # Missing item_price
        result, status = await amqp_client.call_worker('batch_init_stock', payload_missing)
        log.info(f"Test 'missing params': Status={status}, Result={result}")
        assert status == 400 # Expect 400 for missing params
        assert "missing required parameters" in result.get("error", "").lower()

        # Invalid type for n
        payload_invalid_type = {"n": "abc", "starting_stock": 50, "item_price": 100}
        result, status = await amqp_client.call_worker('batch_init_stock', payload_invalid_type)
        log.info(f"Test 'invalid type': Status={status}, Result={result}")
        assert status == 400 # Expect 400 for invalid type
        assert "invalid input parameters" in result.get("error", "").lower()

        # Invalid value for n
        payload_invalid_value = {"n": 0, "starting_stock": 50, "item_price": 100}
        result, status = await amqp_client.call_worker('batch_init_stock', payload_invalid_value)
        log.info(f"Test 'invalid value': Status={status}, Result={result}")
        assert status == 400 # Expect 400 for invalid value (n <= 0)
        assert "must be positive" in result.get("error", "").lower()


    # --- Idempotency Tests ---

    @pytest.mark.asyncio
    async def test_remove_stock_idempotency_success(self, amqp_client):
        item_id = str(uuid.uuid4())
        initial_stock = 200
        price = 50
        set_stock_direct(item_id, initial_stock, price)
        amount_to_remove = 75
        attempt_id = str(uuid.uuid4())
        order_id = f"order-idem-rem-{attempt_id}"
        payload = {"item_id": item_id, "amount": amount_to_remove, "attempt_id": attempt_id, "order_id": order_id}
        expected_stock = initial_stock - amount_to_remove

        # First call
        result1, status1 = await amqp_client.call_worker('remove_stock', payload)
        assert status1 == 200, f"First call failed: {result1}"
        assert result1.get("removed") is True # Correct key
        assert result1.get("stock") == expected_stock
        stock_val1 = get_stock_direct(item_id)
        assert stock_val1.stock == expected_stock
        # Check idempotency key stored using the corrected helper
        idem_key_data1 = get_idempotency_key_direct('remove_stock', item_id, order_id, attempt_id)
        assert idem_key_data1 is not None, "Idempotency key was not stored after first successful call"


        # Second call (should be idempotent)
        log.info("--- Making second idempotent call (remove_stock success) ---")
        result2, status2 = await amqp_client.call_worker('remove_stock', payload)
        assert status2 == 200, f"Second call did not return 200: {result2}"
        assert result2 == result1, "Second call result did not match first call result"

        # Verify stock unchanged by second call
        stock_val2 = get_stock_direct(item_id)
        assert stock_val2.stock == expected_stock

    @pytest.mark.asyncio
    async def test_remove_stock_idempotency_insufficient_funds_error(self, amqp_client):
        item_id = str(uuid.uuid4())
        initial_stock = 10
        price = 50
        set_stock_direct(item_id, initial_stock, price)
        amount_to_remove = 11 # More than available
        attempt_id = str(uuid.uuid4())
        order_id = f"order-idem-rem-insuf-{attempt_id}"
        payload = {"item_id": item_id, "amount": amount_to_remove, "attempt_id": attempt_id, "order_id": order_id}

        # First call (fails with 400)
        result1, status1 = await amqp_client.call_worker('remove_stock', payload)
        assert status1 == 400, f"First call should fail with 400: Status={status1}, Result={result1}"
        assert "insufficient stock" in result1.get("error", "").lower()
        stock_val1 = get_stock_direct(item_id)
        assert stock_val1.stock == initial_stock # Stock unchanged

        # Check if idempotency key stores the *failure* result
        idem_key_data1 = get_idempotency_key_direct('remove_stock', item_id, order_id, attempt_id)
        assert idem_key_data1 is not None, "Idempotency key was not stored after first failed call (400)"

        # Second call (should be idempotent, return the same 400 error)
        log.info("--- Making second idempotent call (remove_stock 400) ---")
        result2, status2 = await amqp_client.call_worker('remove_stock', payload)
        assert status2 == 400, f"Second call did not return 400: {result2}"
        assert result2 == result1, "Second call result did not match first call result (400)"

        # Verify stock still unchanged
        stock_val2 = get_stock_direct(item_id)
        assert stock_val2.stock == initial_stock

    @pytest.mark.asyncio
    async def test_add_stock_idempotency_success(self, amqp_client):
        item_id = str(uuid.uuid4())
        initial_stock = 100
        price = 60
        set_stock_direct(item_id, initial_stock, price)
        amount_to_add = 40
        attempt_id = str(uuid.uuid4())
        order_id = f"order-idem-add-{attempt_id}"
        payload = {"item_id": item_id, "amount": amount_to_add, "attempt_id": attempt_id, "order_id": order_id}
        expected_stock = initial_stock + amount_to_add

        # First call
        result1, status1 = await amqp_client.call_worker('add_stock', payload)
        assert status1 == 200, f"First call failed: {result1}"
        assert result1.get("added") is True
        assert result1.get("stock") == expected_stock
        stock_val1 = get_stock_direct(item_id)
        assert stock_val1.stock == expected_stock
        # Check idempotency key stored using the corrected helper
        idem_key_data1 = get_idempotency_key_direct('add_stock', item_id, order_id, attempt_id)
        assert idem_key_data1 is not None, "Idempotency key was not stored after first successful call"

        # Second call
        log.info("--- Making second idempotent call (add_stock success) ---")
        result2, status2 = await amqp_client.call_worker('add_stock', payload)
        assert status2 == 200
        assert result2 == result1

        # Verify stock unchanged by second call
        stock_val2 = get_stock_direct(item_id)
        assert stock_val2.stock == expected_stock


    @pytest.mark.asyncio
    async def test_idempotency_different_payload_same_attempt_id(self, amqp_client):
        item_id = str(uuid.uuid4())
        initial_stock = 100
        price = 10
        set_stock_direct(item_id, initial_stock, price)
        attempt_id = str(uuid.uuid4())
        order_id = f"order-idem-diff-{attempt_id}" # Assume order_id is constant for the attempt
        payload1 = {"item_id": item_id, "amount": 20, "attempt_id": attempt_id, "order_id": order_id}
        payload2 = {"item_id": item_id, "amount": 30, "attempt_id": attempt_id, "order_id": order_id} # Different amount
        expected_stock_after_first = initial_stock - int(payload1['amount'])

        # First call (remove 20)
        log.info("--- Making first call (remove 20) ---")
        result1, status1 = await amqp_client.call_worker('remove_stock', payload1)
        assert status1 == 200
        assert result1.get("removed") is True
        assert result1.get("stock") == expected_stock_after_first # Should be 80
        stock_val1 = get_stock_direct(item_id)
        assert stock_val1.stock == expected_stock_after_first

        # Check idempotency key stored
        idem_key_data1 = get_idempotency_key_direct('remove_stock', item_id, order_id, attempt_id)
        assert idem_key_data1 is not None, "Idempotency key was not stored after first successful call"

        # Second call (attempt remove 30 with same attempt_id)
        log.info("--- Making second call with different payload (remove 30) ---")
        result2, status2 = await amqp_client.call_worker('remove_stock', payload2)
        assert status2 == 200, "Second call (idempotent) should return original status (200)"
        assert result2 == result1, "Second call (idempotent) should return original result"

        # Verify DB stock is still based on the FIRST call
        stock_val_final = get_stock_direct(item_id)
        assert stock_val_final.stock == expected_stock_after_first # Should still be 80

    # --- Concurrency Test ---
    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_items, ops_per_item", [(5, 20)]) # (10, 50) maybe add more later
    async def test_concurrent_updates_multiple_items(self, amqp_client, num_items, ops_per_item):
        """Tests concurrent add/remove operations on multiple items using Redis WATCH."""
        log.info(f"Starting concurrent add/remove test for {num_items} items, {ops_per_item} ops each.")
        item_ids = [str(uuid.uuid4()) for _ in range(num_items)]
        initial_stock = 40 # Start with enough stock for removals
        item_price = 25
        expected_stock = {}

        # Initialize items
        for item_id in item_ids:
            set_stock_direct(item_id, initial_stock, item_price)
            expected_stock[item_id] = initial_stock

        tasks = []
        total_ops = num_items * ops_per_item
        op_results = {"success": 0, "failure": 0, "errors": []}

        async def run_operation(item_id, op_index):
            """Wrapper to run a single add or remove operation."""
            # Use unique attempt/order IDs for each distinct operation
            op_type = random.choice(["add_stock", "remove_stock"])
            amount = random.randint(1, 5)
            attempt_id = str(uuid.uuid4())
            # Include item identifier and op index in order_id for uniqueness if needed
            short_item_id = item_id[:4]
            order_id = f"conc-multi-{short_item_id}-{op_index}-{attempt_id}"

            payload = {"item_id": item_id, "amount": amount, "attempt_id": attempt_id, "order_id": order_id}

            try:
                result, status = await amqp_client.call_worker(op_type, payload)
                if (op_type == "add_stock" and status == 200 and result.get("added")) or \
                   (op_type == "remove_stock" and status == 200 and result.get("removed")):
                    return ("success", op_type, amount)
                elif op_type == "remove_stock" and status == 400 and "insufficient" in result.get("error", ""):
                     # Count insufficient stock as a specific type of 'failure' or just ignore for final count check
                     log.debug(f"Operation {op_type} on {item_id} failed (insufficient stock) - expected in concurrent tests.")
                     return ("insufficient", op_type, amount)
                else:
                    log.warning(f"Operation {op_type} on {item_id} failed unexpectedly: Status={status}, Result={result}")
                    return ("failure", op_type, amount, f"Status {status}, Result {result}")
            except Exception as e:
                 log.error(f"Exception during operation {op_type} on {item_id}: {e}", exc_info=True)
                 return ("exception", op_type, amount, str(e))


        # Create tasks
        for item_id in item_ids:
            for i in range(ops_per_item):
                tasks.append(asyncio.create_task(run_operation(item_id, i)))

        # Run tasks concurrently
        results = await asyncio.gather(*tasks)

        # Process results and update expected stock *sequentially* based on successful ops
        success_count = 0
        failure_count = 0
        for result_tuple in results:
            status = result_tuple[0]
            op_type = result_tuple[1]
            amount = result_tuple[2]
            # Find which item this result belongs to (requires parsing order_id or modifying return)
            # For simplicity, we won't track expected value perfectly here, just count success/failure.
            # A better approach would be to pass item_id back in the result tuple.
            if status == "success":
                success_count += 1
                # We cannot reliably update expected_stock here without knowing the item_id per result
            elif status == "insufficient":
                pass # Expected failure type, don't count as hard failure
            else:
                failure_count += 1
                error_detail = result_tuple[3] if len(result_tuple) > 3 else "Unknown error"
                op_results["errors"].append(f"{op_type} failed: {error_detail}")


        log.info(f"Multi-item concurrency ({total_ops} ops): Success: {success_count}, Failed: {failure_count}")
        if op_results["errors"]:
             log.warning("Concurrency test errors:\n" + "\n".join(op_results["errors"]))

        # Assert that there were no unexpected failures (status != 200 and != 400 insufficient)
        assert failure_count == 0, f"There were {failure_count} unexpected failures during concurrent updates."

        # Verify final stock counts directly (Cannot compare against calculated expected easily without item_id in results)
        # We can at least check they are non-negative
        log.info("Verifying final stock counts...")
        all_items_ok = True
        for item_id in item_ids:
             final_stock_val = get_stock_direct(item_id)
             if final_stock_val:
                  log.info(f"Final stock for item {item_id}: {final_stock_val.stock}")
                  if final_stock_val.stock < 0:
                      log.error(f"Item {item_id} has negative stock: {final_stock_val.stock}")
                      all_items_ok = False
             else:
                  log.error(f"Could not retrieve final stock for item {item_id}")
                  all_items_ok = False

        assert all_items_ok, "Some items ended with negative stock or could not be retrieved."
