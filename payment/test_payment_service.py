import pytest
from app import app, db
import msgpack
from msgspec import Struct


#pytest --maxfail=1 --disable-warnings -q

class UserValue(Struct):
    credit: int

@pytest.fixture(autouse=True)
def flush_db():
    db.flushall()

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_create_user(client):
    response = client.post('/create_user')
    assert response.status_code == 200
    data = response.get_json()
    assert "user_id" in data

def test_find_user_and_add_funds(client):
    # create user
    response = client.post('/create_user')
    user_id = response.get_json()["user_id"]
    
    # check initial credit=0
    response = client.get(f'/find_user/{user_id}')
    data = response.get_json()
    assert data["credit"] == 0
    
    # add 100 funds
    response = client.post(f'/add_funds/{user_id}/100')
    assert response.status_code == 200
    data = response.get_json()
    assert data["done"] is True
    
    # check if 100 funds are added
    response = client.get(f'/find_user/{user_id}')
    data = response.get_json()
    assert data["credit"] == 100

def test_successful_payment(client):
    # create user + add 100 funds
    response = client.post('/create_user')
    user_id = response.get_json()["user_id"]
    client.post(f'/add_funds/{user_id}/100')
    
    # pay 50
    response = client.post(f'/pay/{user_id}/50')
    assert response.status_code == 200
    data = response.get_json()
    assert data["paid"] is True
    
    # check final, should be 50
    response = client.get(f'/find_user/{user_id}')
    data = response.get_json()
    assert data["credit"] == 50

def test_payment_insufficient_funds(client):
    # create user with 0 funds
    response = client.post('/create_user')
    user_id = response.get_json()["user_id"]
    
    # should give 400 status code for insufficient funds
    response = client.post(f'/pay/{user_id}/50')
    assert response.status_code == 400

def test_cancel_payment(client):
    # create user, add funds, process payment
    response = client.post('/create_user')
    user_id = response.get_json()["user_id"]
    client.post(f'/add_funds/{user_id}/100')
    
    # pay 50
    response = client.post(f'/pay/{user_id}/50')
    assert response.status_code == 200
    
    # check credit is now 50
    response = client.get(f'/find_user/{user_id}')
    data = response.get_json()
    assert data["credit"] == 50
    
    # cancel payment, should refund 50
    response = client.post(f'/cancel/{user_id}/50')
    assert response.status_code == 200
    data = response.get_json()
    assert data["refunded"] is True
    
    # check final, should be 50
    response = client.get(f'/find_user/{user_id}')
    data = response.get_json()
    assert data["credit"] == 100
