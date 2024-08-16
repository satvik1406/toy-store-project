import http.client
import json
import random
from time import perf_counter
import numpy as np

# Constants and Setup
SERVER_HOST = '10.0.0.32' #Servers IP address
SERVER_PORT = 8080
PRODUCTS_ENDPOINT = "/products"
ORDER_ENDPOINT = "/orders"
# A longer list to check LRU cache functionality as the cache is only for 10 toys
product_list = ['fox', 'whale', 'tux', 'python','chess','jumprope','bicycle','lego','barbie','guitar','monopoly','frisbee','twister','hotwheels']
P_ORDER_CHANCE = 0.7  # Probability of placing an order if quantity > 0
orders_made = []

def query_product(product_name, conn):
    conn.request("GET", f"{PRODUCTS_ENDPOINT}/{product_name}")
    response = conn.getresponse()
    data = response.read()
    try:
        # First decode the response to a dictionary
        product_info = json.loads(data.decode("utf-8"))
        # If the response is double-encoded, decode it again
        if isinstance(product_info, str):
            product_info = json.loads(product_info)
        return product_info
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON, error: {e}")
        return None


def place_order(product_name, quantity, conn):
    order = {"name": product_name, "quantity": quantity}
    payload = json.dumps(order)
    headers = {'Content-Type': 'application/json'}
    conn.request("POST", ORDER_ENDPOINT, body=payload, headers=headers)
    response = conn.getresponse()
    data = response.read()
    return json.loads(data.decode("utf-8"))

def query_order(order_id, conn):
    conn.request("GET", f"{ORDER_ENDPOINT}/{order_id}")
    response = conn.getresponse()
    data = response.read()
    return json.loads(data.decode("utf-8"))

# Client's behavior for each iteration
response_times_buy = []
response_times_query = []
response_times_fetch_order = []

for _ in range(500): # Let's run 10 iterations for this example
    # Reinitialize connection at the start of each iteration
    conn = http.client.HTTPConnection(SERVER_HOST, SERVER_PORT)
    product_name = random.choice(product_list)
    print(f"Query Request for toy {product_name} made")
    
    start_time_query = perf_counter()
    product_info = query_product(product_name, conn)
    response_time_query = perf_counter() - start_time_query
    response_times_query.append(response_time_query)

    if 'data' in product_info and product_info['data']['quantity'] > 0:
        # Decide to place an order based on probability P_ORDER_CHANCE
        print(f"Buy Request for toy {product_name} will be made")
        if random.random() < P_ORDER_CHANCE:
            # Assuming to order only 1 quantity for simplicity
            start_time_buy = perf_counter()
            order_response = place_order(product_name, 1, conn)
            response_time_buy = perf_counter() - start_time_buy
            response_times_buy.append(response_time_buy)
            orders_made.append({"number": order_response['data']['order_number'],"name":product_name,"quantity":1})
            print(f"Order placed for {product_name}: {order_response}")
        else:
            # Condition not met, so close the connection
            print(f"Decided not to place an order for {product_name}. Closing connection.")
            conn.close()
    else:
        print(f"{product_name} not available or not found")
        conn.close()

# Calculate and print mean response time
mean_response_time_query = np.mean(response_times_query)
print(f"Mean response time for Query Requests: {mean_response_time_query}")
mean_response_time_buy = np.mean(response_times_buy)
print(f"Mean response time for Buy Requests: {mean_response_time_buy}")

# Verify each order made
for order in orders_made:
    order_id = order['number']
    order_name = order['name']
    order_quantity = order['quantity']

    start_time_order = perf_counter()
    retrieved_order = query_order(order_id, conn)
    response_time_order = perf_counter() - start_time_order
    response_times_fetch_order.append(response_time_order)
    
    retrieved_order = retrieved_order['data']
    print(retrieved_order['number'], retrieved_order['name'], retrieved_order['quantity'])
    if int(retrieved_order['number']) == order_id and retrieved_order['name'] == order_name and int(retrieved_order['quantity']) == order_quantity:
        print(f"Order {order_id} verified successfully.")
    else:
        print(f"Order {order_id} verification failed. Expected {order}, got {retrieved_order}")

mean_response_time_order = np.mean(response_times_fetch_order)
print(f"Mean response time for Order Requests: {mean_response_time_order}")
conn.close()  # Ensure connection is closed after all operations
