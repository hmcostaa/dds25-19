import unittest
import requests
import utils as tu

#Tests are adjusted to work with [ {dict}, code ] from worker

class TestMicroservices(unittest.TestCase):

    def test_stock(self):
        # Test /stock/item/create/<price>
        item_data, item_status = tu.create_item(10)
        self.assertTrue(tu.status_code_is_success(item_status))
        print(f"--- TEST: TestMicroservices - item_data={item_data} ---")
        print(f"--- TEST: TestMicroservices - item_data[0]={item_data}, item_data[1]={item_data} ---")
        self.assertIn('item_id', item_data)
        item_id: str = item_data['item_id']

        # Test /stock/find/<item_id>
        item_data, item_status = tu.find_item(item_id)
        self.assertTrue(tu.status_code_is_success(item_status))
        self.assertEqual(item_data['price'], 10)
        self.assertEqual(item_data['stock'], 0)

        # Test /stock/add/<item_id>/<number>
        _, add_stock_status = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_status))

        item_data, item_status = tu.find_item(item_id)
        self.assertTrue(tu.status_code_is_success(item_status))
        stock_after_add: int = item_data['stock'] 
        self.assertEqual(stock_after_add, 50)

        # Test /stock/subtract/<item_id>/<number>
        _, over_subtract_stock_status = tu.subtract_stock(item_id, 200)
        self.assertTrue(tu.status_code_is_failure(over_subtract_stock_status))

        _, subtract_stock_status = tu.subtract_stock(item_id, 15)
        self.assertTrue(tu.status_code_is_success(subtract_stock_status))

        item_data, item_status = tu.find_item(item_id)
        self.assertTrue(tu.status_code_is_success(item_status))
        stock_after_subtract: int = item_data['stock']
        self.assertEqual(stock_after_subtract, 35)

    def test_payment(self):
        # Test /payment/pay/<user_id>/<order_id>
        user_data, user_status = tu.create_user()
        self.assertTrue(tu.status_code_is_success(user_status))
        print(f"\n--- TEST_PAYMENT: Before assertIn for user_data ---")
        print(f"user_data type: {type(user_data)}")
        print(f"user_data value: {user_data}")
        print(f"user_status type: {type(user_status)}")
        print(f"user_status value: {user_status}\n")
        self.assertIn('user_id', user_data)
        user_id: str = user_data['user_id']

        # Test /payment/add_funds/<user_id>/<amount> - Kept original comment structure
        _, add_credit_status = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(add_credit_status))

        # Verify credit
        user_data, user_status = tu.find_user(user_id)
        self.assertTrue(tu.status_code_is_success(user_status))
        # Assumes find_user returns dict directly. If it returns [dict, code], need user_data
        self.assertEqual(user_data['credit'], 15)

        # add item to the stock service
        item_data, item_status = tu.create_item(5)
        self.assertTrue(tu.status_code_is_success(item_status))
        self.assertIn('item_id', item_data)
        item_id: str = item_data['item_id']

        _, add_stock_status = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_status))

        # create order in the order service and add item to the order
        order_data, order_status = tu.create_order(user_id)
        # Check status first - if this fails, order_data might not be as expected
        self.assertTrue(tu.status_code_is_success(order_status))
        self.assertIn('order_id', order_data)
        order_id: str = order_data['order_id']

        _, add_item_status = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_status))
        _, add_item_status = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_status))
        _, add_item_status = tu.add_item_to_order(order_id, item_id, 1) 
        self.assertTrue(tu.status_code_is_success(add_item_status))

        # Perform payment (cost should be 15)
        _, payment_status = tu.payment_pay(user_id, 15)
        self.assertTrue(tu.status_code_is_success(payment_status))

        user_data, user_status = tu.find_user(user_id)
        self.assertTrue(tu.status_code_is_success(user_status))
        # Assumes find_user returns dict directly. If it returns [dict, code], need user_data
        credit_after_payment: int = user_data['credit']
        self.assertEqual(credit_after_payment, 0)

    def test_order(self):
        # Test /payment/pay/<user_id>/<order_id> - Kept original comment structure
        user_data, user_status = tu.create_user()
        self.assertTrue(tu.status_code_is_success(user_status))
        self.assertIn('user_id', user_data)
        user_id: str = user_data['user_id']

        # create order in the order service and add item to the order
        order_data, order_status = tu.create_order(user_id)
        # Check status first - test failed here previously
        self.assertTrue(tu.status_code_is_success(order_status))
        self.assertIn('order_id', order_data)
        order_id: str = order_data['order_id']
        print(f"--- TEST: test_order - Created order_id: {order_id}")

        # add item to the stock service
        item1_data, item1_status = tu.create_item(5)
        self.assertTrue(tu.status_code_is_success(item1_status))
        self.assertIn('item_id', item1_data)
        item_id1: str = item1_data['item_id']
        _, add_stock_status = tu.add_stock(item_id1, 15)
        self.assertTrue(tu.status_code_is_success(add_stock_status))

        # add item to the stock service
        item2_data, item2_status = tu.create_item(5)
        self.assertTrue(tu.status_code_is_success(item2_status))
        self.assertIn('item_id', item2_data)
        item_id2: str = item2_data['item_id']
        _, add_stock_status = tu.add_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_stock_status))

        _, add_item_status = tu.add_item_to_order(order_id, item_id1, 1)
        self.assertTrue(tu.status_code_is_success(add_item_status))
        _, add_item_status = tu.add_item_to_order(order_id, item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_item_status)) 

        # Find order and check cost
        order_data, order_status = tu.find_order(order_id)
        self.assertTrue(tu.status_code_is_success(order_status))
        # Assumes find_order returns dict directly. If it returns [dict, code], need order_data
        self.assertEqual(order_data['total_cost'], 10)

        # Make item2 out of stock
        _, subtract_stock_status = tu.subtract_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(subtract_stock_status))

        # Add insufficient credit
        _, add_credit_status = tu.add_credit_to_user(user_id, 5)
        self.assertTrue(tu.status_code_is_success(add_credit_status))

        # Attempt checkout (should fail: needs 10 credit, has 5; OR item2 has 0 stock)
        print(f"--- TEST: test_order - Attempting first checkout for order_id: {order_id}") 
        checkout_response = tu.checkout_order(order_id) # Returns Response object
        print(f"--- TEST: test_order - First checkout response status: {checkout_response.status_code}") 
        print(f"--- TEST: test_order - First checkout response text: {checkout_response.text}") 
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))

        item1_data, item1_status = tu.find_item(item_id1)
        self.assertTrue(tu.status_code_is_success(item1_status))
        # Assumes find_item returns dict directly.
        self.assertEqual(item1_data['stock'], 15)

        # Add stock and credit for success
        _, add_stock_status = tu.add_stock(item_id2, 15) 
        self.assertTrue(tu.status_code_is_success(add_stock_status))
        _, add_credit_status = tu.add_credit_to_user(user_id, 10) 
        self.assertTrue(tu.status_code_is_success(add_credit_status))

        # Verify credit and stock before checkout
        user_data, user_status = tu.find_user(user_id)
        self.assertTrue(tu.status_code_is_success(user_status))
        # Assumes find_user returns dict directly.
        self.assertEqual(user_data['credit'], 15)
        item1_data, item1_status = tu.find_item(item_id1)
        self.assertTrue(tu.status_code_is_success(item1_status))
        # Assumes find_item returns dict directly.
        self.assertEqual(item1_data['stock'], 15)
        item2_data, item2_status = tu.find_item(item_id2)
        self.assertTrue(tu.status_code_is_success(item2_status))
        # Assumes find_item returns dict directly.
        self.assertEqual(item2_data['stock'], 15)

        # Checkout should succeed
        print(f"--- TEST: test_order - Attempting second checkout for order_id: {order_id}") 
        checkout_response = tu.checkout_order(order_id) 
        print(f"--- TEST: test_order - Second checkout response status: {checkout_response.status_code}")
        print(f"--- TEST: test_order - Second checkout response text: {checkout_response.text}") 
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        item1_data, item1_status = tu.find_item(item_id1)
        self.assertTrue(tu.status_code_is_success(item1_status))
         # Assumes find_item returns dict directly.
        self.assertEqual(item1_data['stock'], 14)

        item2_data, item2_status = tu.find_item(item_id2)
        self.assertTrue(tu.status_code_is_success(item2_status))
         # Assumes find_item returns dict directly.
        self.assertEqual(item2_data['stock'], 14)

        user_data, user_status = tu.find_user(user_id)
        self.assertTrue(tu.status_code_is_success(user_status))
         # Assumes find_user returns dict directly.
        self.assertEqual(user_data['credit'], 5)

if __name__ == '__main__':
    unittest.main()
