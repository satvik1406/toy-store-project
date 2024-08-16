# Import necessary libraries for creating an HTTP server and handling threading.
from http.server import HTTPServer, SimpleHTTPRequestHandler
import http.client
from socketserver import ThreadingMixIn
import time
import csv
import json
import threading
import requests  # Used for making HTTP requests to other microservices.
import sys


# Configuration for the server and file paths.
ORDER_HOST = 'localhost'  # Hostname for the order service.
CATALOG_HOST = 'localhost'  # Hostname for the catalog service.
CATALOG_PORT = 8081  # Port number for the catalog service.
ORDER_PORT = 8083  # Port number for the order service.
ORDER_FILENAME = "orderLog_"  # Filename for storing order logs.
ORDER_REPLICAS = [(0, "8000"), (1, "8001"), (2, "8002")]

# Custom class for managing read-write locks using threading conditions.
class ReadWriteLock:
    def __init__(self):
        self.read_ready = threading.Condition(threading.Lock())
        self.readers = 0  # Counter for the number of read locks.
        self.writer = False  # Boolean flag to indicate if a write lock is held.

    # Method definitions for acquiring and releasing read and write locks.
    def acquire_read(self):
        # Acquire a read lock.
        with self.read_ready:
            while self.writer:
                # Wait if a write operation is in progress.
                self.read_ready.wait()
            self.readers += 1

    def release_read(self):
        # Release a read lock.
        with self.read_ready:
            self.readers -= 1
            if self.readers == 0:
                # Notify all waiting threads if no more readers.
                self.read_ready.notify_all()

    def acquire_write(self):
        # Acquire a write lock.
        with self.read_ready:
            while self.writer or self.readers > 0:
                # Wait if another write operation or read operations are in progress.
                self.read_ready.wait()
            self.writer = True

    def release_write(self):
        # Release a write lock.
        with self.read_ready:
            self.writer = False
            self.read_ready.notify_all()

# Helper function to create error messages in JSON format.
def get_message(sta_code, message):
    return {
        "error": {
            "code": sta_code,
            "message": message
        }
    }

# Reads existing orders from the CSV file and returns them as a list.
def getOrderFileContent():
    try:
        with open(ORDER_FILENAME, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            rows = list(csvreader)
    except FileNotFoundError:
        rows = []  # Return an empty list if the file does not exist.
    return rows

# Returns the file length of the orderLog csv.
def get_file_length(ORDER_FILENAME):
    # Load the catalog from disk, if it exists
    rows = list()
    try:
        with open(ORDER_FILENAME, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            rows = list(csvreader)
        csvfile.close()
    except FileNotFoundError:
        pass
    return len(rows)

# Custom request handler for processing POST requests related to orders.
class OrderRequestHandler(SimpleHTTPRequestHandler):
    def get_file_row(self,filename, order_number):
        # Read the file to get and return the last order id
        val = []
        with open(filename, 'r') as source_file:
            source_reader = csv.reader(source_file)
            rows = list(source_reader)
            for row in rows:
                if int(order_number) == int(row[0]):
                    val = row
            source_file.close()
            return val

 
    def do_POST(self):
        #Endpoint to place an order  
        if self.path.startswith("/order"):
            self.server.lock.acquire_write()  # Acquire write lock before processing the order.
            try:
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                request_body = json.loads(post_data.decode('utf-8'))
                
                toy_name = request_body['name']
                quantity = request_body['quantity']
                node = int(request_body['node'])
                
                # Make a request to the catalog service to check if enough toys are available.
                url = f'http://{CATALOG_HOST}:{CATALOG_PORT}/product/{toy_name}'
                response = requests.get(url)
                
                # If the toy is available, proceed to update the catalog and add the order.
                if response.status_code == 200:
                    url = f'http://{CATALOG_HOST}:{CATALOG_PORT}/catalogUpdate'
                    response = requests.post(url, json=request_body)
                    
                    # If the catalog update is successful, add the order to the order list and log it.
                    if response.status_code == 201:
                        ORDER_FILENAME = OrderRequestHandler._getFileName(node)
                        order_id = get_file_length(ORDER_FILENAME)
                        new_row = [order_id, toy_name, quantity, node]
                        self.MaintainConsistency(new_row,ORDER_FILENAME)
                            
                        # Send a successful response back with the order number.
                        self.send_response(201)
                        self.send_header("Content-type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"data": {"order_number": order_id}}).encode())
                    else:
                        # If catalog update fails, send the error response from the catalog service.
                        self.send_response(response.status_code)
                        self.send_header("Content-type", "text/plain")
                        self.end_headers()
                        self.wfile.write(response.content)
                else:
                    # If the toy is not found in the catalog, send the error response from the catalog service.
                    self.send_response(response.status_code)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(response.content)
            finally:
                self.server.lock.release_write()  # Release the write lock.
        #Endpoint for the followers replicas to sync their database 
        elif self.path.startswith("/consistency"):
                # Handle data consistency for follower nodes
                content_len = int(self.headers.get('content-length'))
                post_body = self.rfile.read(content_len)
                data = json.loads(post_body)
                # Get filename for the server
                fileName=OrderRequestHandler._getFileName(data['current_node'])
                new_row = [data['orderId'], data['name'], data['quantity']]
                # Write order to the server file
                OrderRequestHandler.WriteToFile(new_row,fileName)

                self.send_response(201)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write("Added to file".encode())
        else:
            # If the request path does not start with "/order", send a 400 Bad Request error.
            self.send_response(400)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(json.dumps(get_message(400, "Invalid order")).encode())

    def do_GET(self):
        #Endpoint to get the order details specific to a particular order number. 
        if self.path.startswith("/orders"):
            self.server.lock.acquire_read()  # Acquire read lock before getting the order.
            try:
                params = self.path.split("/orders/")[-1]
                params = params.split('?')
                order_number = int(params[0])
                node = int(params[1].split('=')[1])
                ORDER_FILENAME = OrderRequestHandler._getFileName(node)
                order_row = self.get_file_row(ORDER_FILENAME, order_number)
                if len(order_row) == 0:
                    self.send_response(404)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": {"code": 404, "message": "Order not found"}}).encode())
                else:
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")  # Ensure correct content type
                    self.end_headers()
                    self.wfile.write(json.dumps({"data": {"number": order_row[0], "name": order_row[1], "quantity": order_row[2]}}).encode())
            finally:
                self.server.lock.release_read()
        else:
            self.send_response(400)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(json.dumps(get_message(400, "Invalid order number")).encode())



    def WriteToFile(new_row,ORDER_FILENAME):
        # Persist the order list to disk
        with open(ORDER_FILENAME, 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(new_row)
            csvfile.close()
    
    def _getFileName(node):
        # Form file names depending on the running server
        return ORDER_FILENAME+str(node)+".csv"

    #Function to maintain consistency of orders among the replicas
    def MaintainConsistency(self, new_row, ORDER_FILENAME):
        # POST data to follwer order servers to replicate the current order in their database
        
        for node in ORDER_REPLICAS:
            if int(node[0])!= int(new_row[3]):
                data={
                    "orderId": new_row[0],
                    "name": new_row[1],
                    "quantity": new_row[2],
                    "node": new_row[3],
                    "current_node": node[0]
                }
                headers = {'Content-type': 'application/json'}
                try:
                    connection = http.client.HTTPConnection(ORDER_HOST,int(node[1]))
                    connection.request('POST', '/consistency', json.dumps(data), headers)
                    response = connection.getresponse()
                    if response.status != 201:
                            print("Failed to replicate data to node", node, "Status:", response.status)                        
                except Exception as e:
                    print("Error sending data to node", node, ":", str(e))
            else:
                data = [new_row[0],new_row[1],new_row[2]]
                 # Write the current order details to the database of the leader node
                OrderRequestHandler.WriteToFile(data,ORDER_FILENAME)

   #Function to retrieve missing orders and synchronize them amongst all the replicas
    def retrieve_missing_orders(curr_node):
        curr_node_file_name = OrderRequestHandler._getFileName(curr_node)
        curr_node_file_length = get_file_length(curr_node_file_name)
        for node in ORDER_REPLICAS:
            node_file_name = OrderRequestHandler._getFileName(node[0])
            node_file_length = get_file_length(node_file_name)
            if ORDER_PORT != int(node[1]) and node_file_length > curr_node_file_length:
                # Open both CSV files
                print(f"Node file name {node_file_name}")
                print(f"Curr file name {curr_node_file_name}")
                with open(node_file_name, 'r') as source_file, open(curr_node_file_name, 'r') as target_file:
                    # Create CSV readers for both files
                    source_reader = csv.reader(source_file)
                    target_reader = csv.reader(target_file)

                    # Find missing rows
                    missing_rows = [row for row in source_reader if row not in target_reader]

                    # Close both files
                    source_file.close()
                    target_file.close()

                with open(curr_node_file_name, 'a', newline='') as target_file:
                    # Create CSV writer for the target file
                    target_writer = csv.writer(target_file)

                    # Append missing rows to the target file
                    target_writer.writerows(missing_rows)

                    # Close file
                    target_file.close()

# Multi-threaded HTTP server capable of handling requests in separate threads.
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
        self.order_list = getOrderFileContent()  # Load existing orders from the file.
        self.lock = ReadWriteLock()  # Initialize the read-write lock for managing concurrency.
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)

# Main function to start the server.
if __name__ == '__main__':
    node=int(sys.argv[1])
    ORDER_PORT=int(ORDER_REPLICAS[node][1])
    server = ThreadedHTTPServer(("", ORDER_PORT), OrderRequestHandler)

    OrderRequestHandler.retrieve_missing_orders(node)

    print(time.asctime(), 'Server UP - %s:%s' % (ORDER_HOST, ORDER_PORT))
    try:
        server.serve_forever()  # Serve until interrupted.
    except KeyboardInterrupt:
        # Gracefully shutdown the server on interrupt.
        server.server_close()
        print(time.asctime(), 'Server DOWN - %s:%s' % (ORDER_HOST, ORDER_PORT))
