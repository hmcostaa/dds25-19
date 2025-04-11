import logging
import os
import atexit
import sys
import sys
import uuid
import asyncio
from redis import Sentinel
from redis import Sentinel
import redis

from flask import Flask, jsonify, abort, Response
from typing import Optional, Tuple, Any
from redis.exceptions import WatchError, RedisError
import random
import logging

from global_idempotency.idempotency_decorator import idempotent

from msgspec import msgpack, Struct, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError
# from global_idempotency.app import (
#         IdempotencyStoreConnectionError,
#         IdempotencyDataError,
#         check_idempotent_operation,
#         store_idempotent_result,
#         IdempotencyResultTuple
#     )

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s"
)

logging = logging.getLogger("payment-service")
SERVICE_NAME = "payment"


sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from common.amqp_worker import AMQPWorker


worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="payment_queue",
)

DB_ERROR_STR = "DB error"

#pytest --maxfail=1 --disable-warnings -q


sentinel = Sentinel([
    (os.environ.get('REDIS_SENTINEL_1', 'localhost'), 26379),
    (os.environ.get('REDIS_SENTINEL_2', 'localhost'), 26380),
    (os.environ.get('REDIS_SENTINEL_3', 'localhost'), 26381)
], socket_timeout=0.1, password=os.environ.get('REDIS_PASSWORD', None))

db_master = sentinel.master_for('payment-master', socket_timeout=0.1, decode_responses=False)
db_slave = sentinel.slave_for('payment-master', socket_timeout=0.1, decode_responses=False)

logging.info("Connected to Redis Sentinel.")

#no slave since idempotency is critical to payment service
idempotency_db_conn = db_master 

logging.info("Connected to idempotency (Payment) Redis.")


def close_db_connection():
    db_master.close()
    db_slave.close()
    idempotency_db_conn.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int

def get_user_from_db(user_id: str) -> Optional[UserValue]:
    try:
        # get serialized data
        entry: Optional[bytes] = db_slave.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: Optional[UserValue] = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry

async def atomic_update_user(user_id: str, update_func):
    #retry? backoff/?
    #for attempts i tn range(max_retries):
    
    max_retries = 5
    base_backoff = 50 

    # async with db.pipeline(transaction=True) as pipe: # async pipeline, should probably be used TODO
    for attempt in range(max_retries):
        pipe = db_master.pipeline(transaction=True)
        try:
            #idempotency check?
            # if ?
            # max retry attempts
            #watche exec pipeline

            # await pipe.watch(user_id)
            pipe.watch(user_id)

            #bytes/?
            entry = pipe.get(user_id) #noawait

            # entry = await pipe.get(user_id)
            if entry is None:
                    pipe.reset() # quick reset before returning
                    logging.warning("Atomic update: User: %s", user_id)
                    return None, f"User {user_id} not found"
            
            #disistuingish between leader and follower??
            #db_master.pipeline()?
            try:
                user_val: UserValue = msgpack.Decoder(UserValue).decode(entry)
            except MsgspecDecodeError:
                pipe.reset()
                logging.error("Atomic update: Decode error for user %s", user_id)
                return None, "Internal data format error"
            
            try:
                updated_user = update_func(user_val)
                #synchronous
                #error?
                pipe.multi()
                pipe.set(user_id, msgpack.Encoder().encode(updated_user))

                pipe.execute() #execute if no other client modified user_id (pipe.watch)
                return updated_user, None #succesfull
            except ValueError as e:
                pipe.reset()
                raise e
                
        except WatchError:
            #key was modified between watch and execute, retry backoff 
            #backoff?? TODO
            backoff_multiplier = (2 ** attempt) * (1 + random.random() * 0.1)
            backoff = base_backoff * backoff_multiplier
            
            await asyncio.sleep(backoff / 1000)
            continue #loop again

        # except redis.WatchError:
        #     continue 
        except redis.RedisError:
            return None, DB_ERROR_STR
        
        except Exception as e:
            logging.exception(f"Unexpected error in atomic_update_user for user {user_id}: {e}")
            return None, "Internal data error"
            
    return None, f"Failed to update user, retries nr??."

# For testing, had endpoint "user_id" couldnt be found using POSTMAN
def require_user_id(data):
    uid = data.get("user_id")
    if not uid:
        return None, ({"error": "Missing 'user_id' in payload"}, 400)
    return str(uid).strip()

@worker.register
async def create_user(data):
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db_master.set(key, value)
        return {'user_id': key}, 200
    except redis.exceptions.RedisError:
        return {"error": DB_ERROR_STR}, 400
    except Exception as e:
        logging.exception("Error creating user: %s", e)
        return {"error": f"Error creating user: {e}"}, 400
    
@worker.register
async def batch_init_users(data):
    n = int(data['n'])
    starting_money = int(data['starting_money'])
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db_master.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return {"error": DB_ERROR_STR}, 400
    return {"msg": "Batch init for users successful"}, 200


@worker.register
async def find_user(data):
    try:
        user_id = require_user_id(data)
        user_entry = get_user_from_db(user_id)
        # if error:
        #     return error
        
        response = {"user_id": user_id, "credit": user_entry.credit}
        logging.debug(f"Found user {user_id} with credit {user_entry.credit}")
        return response, 200
    except Exception as e:
        logging.exception("Error retrieving user from DB for id %s: %s", user_id, e)
        abort(400, f"Error retrieving user: {e}")

@worker.register
async def add_funds(data):
    user_id =  require_user_id(data)
    amount = int(data.get("amount", 0))
    
    if amount <= 0:
        return {"error": "Transaction amount must be positive"}, 400

    def updater(user: UserValue) -> UserValue:
        user.credit += amount
        return user
    
    try:
        # atomic_update_user(data["user_id"], updater)
        updated_user, error_msg = await atomic_update_user(user_id, updater)

        if error_msg:
            logging.error(f"Failed to add funds for user {user_id}: {error_msg}")
            return {"error": f"Failed to add funds: {error_msg}"}, 500 

        if updated_user:
            return {"done": True, "credit": updated_user.credit}, 200
        else:
            return {"error": "Failed to update user, unknown reason"}, 500
    
    except (ValueError, TypeError):
        return {"error": "Invalid amount specified"}, 400
    except Exception as e:
        logging.exception(f"Unexpected error adding funds for user {user_id}: {e}") 
        return {"error": "Unexpected error, add funds"}, 500

@worker.register
@idempotent('pay', idempotency_db_conn, SERVICE_NAME)
async def pay(data):
    user_id = data.get("user_id")
    order_id = data.get("order_id")
    amount = int(data.get("amount", 0))
    attempt_id = data.get("attempt_id")

    if not all([user_id, order_id, attempt_id, amount is not None]):
        logging.error("Missing required fields (user_id, order_id, amount, attempt_id)")
        return {"error": "Missing required fields"}, 400
    try:
        amount_int = int(amount)
        if amount_int <= 0:
            return {"error": "Transaction amount must be positive"}, 400
    except (ValueError, TypeError):
        logging.error("Unexpected error, amount:.", amount)
        return {"error": "Invalid amount?"}, 400

    def updater(user: UserValue) -> UserValue:
        if user.credit < int(amount):
            raise ValueError(f"User {user_id} has insufficient credit")
        user.credit -= int(amount)
        return user

    # updated_user, error_msg, res_tuple = None, None, None
    updated_user = None
    error_msg = None

    try: 
        updated_user, error_msg = await atomic_update_user(user_id, updater) 

        if error_msg:
            logging.error(f"'pay' logic failed: {error_msg}")
            logging.debug(f"error_msg: {error_msg}")
            if "insufficient credit" in error_msg.lower():
                status_code = 400 
            elif "not found" in error_msg.lower(): 
                status_code = 404
            else: 
                status_code = 500
            return ({"paid": False, "error": error_msg}, status_code)
            
        elif updated_user:
            logging.info(f" 'pay' logic succeeded: New credit: {updated_user.credit}")
            return ({"paid": True, "credit": updated_user.credit}, 200)
        else:
            #fallback
            return ({"paid": False, "error": "Internal processing error"}, 500)
    
    except ValueError as e:
        error_msg = str(e)
        return ({"paid": False, "value error": error_msg}, 400)
    except Exception as e:
        logging.exception("internal error")
        return ({"paid": False, "internal error": error_msg}, 400)
    
@worker.register
@idempotent('cancel_payment', idempotency_db_conn, SERVICE_NAME)
async def cancel_payment(data):
    try:
        user_id = data.get("user_id")
        order_id = data.get("order_id") 
        attempt_id = data.get("attempt_id")
        amount = data.get("amount")

        if not all([user_id, amount is not None]):
            logging.error("Missing user_id or amount in cancel_payment request.")
            return {"error": "Missing required fields"}, 400
        
        if int(amount) <= 0:
            return {"error": "Transaction amount must be positive"}, 400
        
    except (ValueError, TypeError):
        return {"error": "Invalid amount specified"}, 400
    
    def updater(user: UserValue) -> UserValue:
        user.credit += amount
        return user
    
    try:
        updated_user, error_msg = await atomic_update_user(user_id, updater)
        if error_msg:
            logging.error(f"Failed to cancel payment (refund) for user {user_id}: {error_msg}")
            status_code = 404 if "not found" in error_msg.lower() else 500
            return {"error": f"Failed to cancel payment: {error_msg}"}, status_code
        if updated_user:
            return {"refunded": True, "credit": updated_user.credit}, 200
        else:
            logging.error(f"Cancel payment failed for user {user_id}, unknown reason .")
            return {"error": "Failed cancellation, unknown reason"}, 500
    except Exception as e:
        logging.exception("Error canceling payment for user %s: %s", user_id, e)
        return {"error": f"Error canceling payment for user {user_id}: {e}"}, 400
    
if __name__ == '__main__':
    logging.info("Starting payment service AMQP worker...")
    asyncio.run(worker.start())

