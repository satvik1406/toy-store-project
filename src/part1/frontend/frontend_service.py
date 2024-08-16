from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import json
import requests
from lru_cache import LRUcache
import socket
import time
from threading import Thread

# Define the hostnames and ports for the frontend, catalog, and order services.
FRONTEND_HOST = ''
CATALOG_HOST = 'localhost'
ORDER_HOST = 'localhost'
FRONTEND_PORT = 8080
CATALOG_PORT = 8081
ORDER_PORT = 8083
ORDER_REPLICAS = [(0, "8000"), (1, "8001"), (2, "8002")]

# Inherits from both HTTPServer and ThreadingMixIn to enable handling requests in separate threads.
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True  # Threads will shut down cleanly upon termination of the main program.

# Custom HTTP request handler.
class CustomHandler(BaseHTTPRequestHandler):
    # Handler for GET requests.
    def do_GET(self):
        # Check if the path starts with '/product' indicating a product lookup.
        if self.path.startswith('/products'):
            toy_name = self.path.split('/products/')[-1]  # Extract the toy name from the URL.
            cached_response = memory_cache.get(toy_name.lower())
            if cached_response != -1:
                print('Found in cache', cached_response)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')  # Set the content type of the response.
                self.end_headers()
                self.wfile.write(json.dumps(cached_response).encode())
            else:
                print('cache miss for toy ',toy_name)
                url = f'http://{CATALOG_HOST}:{CATALOG_PORT}/product/{toy_name}'  # Construct URL to query the catalog service.
                try:
                    response = requests.get(url)  # Send GET request to the catalog service.
                    memory_cache.put(toy_name.lower(), response.content.decode())
                    self.send_response(response.status_code)  # Forward the status code from the catalog service.
                    self.send_header('Content-type', 'application/json')  # Set the content type of the response.
                    self.end_headers()
                    self.wfile.write(response.content)  # Send the content received from the catalog service.
                except Exception as e:
                    self.send_error(500, f'Internal Server Error: {e}')  # Send a 500 error if something goes wrong.
        #Endpoint to get order details for a particular order
        elif self.path.startswith('/orders'):
            global ORDER_PORT, ORDER_NODE
            if CustomHandler.is_port_open(ORDER_HOST, ORDER_PORT) == 0:
                leader = CustomHandler.elect_leader()
                if leader != 0:
                    ORDER_PORT, ORDER_NODE = leader[1], leader[0]
                else:
                    print("No Order Service Node available")
            
            order_number = self.path.split('/orders/')[-1]  # Extract the order number from the URL.
            url = f'http://{ORDER_HOST}:{ORDER_PORT}/orders/{order_number}?node={ORDER_NODE}'  # Construct URL to query the order service.
            try:
                response = requests.get(url)  # Send GET request to the order service.
                self.send_response(response.status_code)  # Forward the status code from the order service.
                self.send_header('Content-type', 'application/json')  # Set the content type of the response.
                self.end_headers()
                self.wfile.write(response.content)  # Send the content received from the catalog service.
            except Exception as e:
                self.send_error(500, f'Internal Server Error: {e}')  # Send a 500 error if something goes wrong.
        
        else:
            self.send_error(404, "File not found.")  # Send a 404 error for any other endpoints.

    # Handler for POST requests to place an order.
    def do_POST(self):
        # Check if the path starts with '/order' indicating an order request.
        if self.path.startswith('/orders'):
            global ORDER_PORT
            try:
                if CustomHandler.is_port_open(ORDER_HOST, ORDER_PORT) == 0:
                    leader = CustomHandler.elect_leader()
                    if leader!=0:
                        ORDER_PORT, _ = leader[1], leader[0]
                    else:
                        print("No Order Service Node available")

                content_length = int(self.headers['Content-Length'])  # Get the size of the data.
                post_data = self.rfile.read(content_length)  # Read the request body.
                request_body = json.loads(post_data.decode('utf-8'))  # Parse the JSON data.
                request_body['node'] = ORDER_NODE  # Example: setting node as 'Node1'.
                url = f'http://{ORDER_HOST}:{ORDER_PORT}/order'  # Construct URL to call the order service.
                response = requests.post(url, json=request_body)  # Send POST request to the order service with the JSON data.
                self.send_response(response.status_code)  # Forward the status code from the order service.
                self.send_header('Content-type', 'application/json')  # Set the content type of the response.
                self.end_headers()
                self.wfile.write(response.content)  # Send the content received from the order service.
            except Exception as e:
                self.send_error(500, f'Internal Server Error: {e}')  # Send a 500 error if something goes wrong.

        # Check if the request is for cache invalidation
        elif self.path.startswith('/invalidate_cache'):
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            toy_name = data['toy_name']
            # Invalidate the cache
            cached_response = memory_cache.get(toy_name.lower())
            if cached_response != -1:
                memory_cache.delete(toy_name)
                print(f"Cache invalidated for {toy_name}")
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            return
        else:
            self.send_error(404, "File not found.")  # Send a 404 error for any other endpoints.

    def is_port_open(ip, port):        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((ip, int(port)))
        sock.close()
        return result == 0
    
    #Function to  elect a leader
    def elect_leader():
        # Sort replicas based on priority, descending
        sorted_replicas = sorted(ORDER_REPLICAS, key=lambda r: r[0], reverse=True)
        leader = 0
        for priority, port in sorted_replicas:
            print(f"Checking port {port} on host {ORDER_HOST}")
            if CustomHandler.is_port_open(ORDER_HOST, port):
                leader = (priority, port)
                break
        return leader
    
    #Function to check if the leader is active or not in every 5 second interval
    def check_leader():
        global ORDER_PORT, ORDER_NODE

        while True:
            # Ping the current leader node
            if not CustomHandler.is_port_open(ORDER_HOST, int(ORDER_PORT)):
                # If the current leader is down, elect a new leader
                print(f"Leader node {ORDER_NODE} is down, electing new leader...")
                new_leader = CustomHandler.elect_leader()
                if new_leader != 0:
                    ORDER_PORT = new_leader[1]
                    ORDER_NODE = new_leader[0]
                    print(f"New leader node is {ORDER_NODE} on port {ORDER_PORT}")
                else:
                    print("No Order Service Node available")

            # Wait for some time before checking again
            time.sleep(5)

# Main function to start the server.
if __name__ == "__main__":
    memory_cache = LRUcache(10)
    leader = CustomHandler.elect_leader()
    if leader != 0:
        ORDER_PORT, ORDER_NODE= leader[1], leader[0]
        print(f"Order Service running at port {ORDER_PORT}")
    else:
        print("No Order Service Node available")

    task_thread = Thread(target=CustomHandler.check_leader)
    task_thread.daemon = True  # This makes the thread exit when the main program exits
    task_thread.start()

    # Create and start the threaded HTTP server.
    httpd = ThreadedHTTPServer(("", FRONTEND_PORT), CustomHandler)
    print(f"Server running on port {FRONTEND_PORT}")
    try:
        httpd.serve_forever()  # Serve until interrupted.
    except KeyboardInterrupt:
        httpd.server_close()  # Cleanly close the server on interruption (e.g., Ctrl+C).
        print("Server stopped.")