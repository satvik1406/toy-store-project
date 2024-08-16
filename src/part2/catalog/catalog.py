from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
import time
import csv
import json
import threading
import requests

# Configuration for the server
HOST_NAME = 'localhost'
CATALOG_PORT = 8081
CATALOG_FILENAME = "catalogStock.csv"
FRONTEND_HOST = 'localhost'
FRONTEND_PORT = 8080

# Function to read the catalog data from a CSV file
def getFileContent():
    catalog_data = {}
    rows = []
    try:
        with open(CATALOG_FILENAME, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            rows = list(csvreader)
            # Populate catalog_data dictionary with toy names as keys
            for line in rows:
                key = str(line[0]).lower()
                catalog_data[key] = {'name': key, 'price': float(line[1]), 'quantity': int(line[2])}
    except FileNotFoundError:
        rows = []
        print("File not found")
        catalog_data = {}
    return rows, catalog_data

# Function to construct an error message in JSON format
def get_error_message(sta_code, message):
    return {
        "error": {
            "code": sta_code,
            "message": message
        }
    }

# Function to construct a response message in JSON format
def get_response_message(sta_code, message):
    return {
        "code": sta_code,
        "message": message
    }

# Function to notify the front-end service to invalidate the cache
def notify_frontend_invalidation(toy_name):
    # URL to the front-end service cache invalidation endpoint
    url = f'http://{FRONTEND_HOST}:{FRONTEND_PORT}/invalidate_cache'
    try:
        # Send the invalidation request
        requests.post(url, json={"toy_name": toy_name})
    except Exception as e:
        print(f"Error notifying front-end: {e}")

# Function to periodically check every 10 seconds the quantity of toy, and restock if its 0.
def periodic_check_and_restock(catalog_data, lock, rows):
    while True:
        lock.acquire_write()  # Acquire write lock before modifying the catalog
        for toy, details in catalog_data.items():
            if details['quantity'] == 0:
                #Updating in memory 
                details['quantity'] = 100
                #Invalidating stale data for this toy in cache after restocking 
                notify_frontend_invalidation(toy)
                for row in rows:
                    if toy == str(row[0]).lower():
                        row[2] = str(100)
                # Modify csv file with updated quantity
                with open(CATALOG_FILENAME, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerows(rows)
                print(f'Restocked {toy} to 100 units.')
        lock.release_write()
        time.sleep(10)  # Pause for 10 seconds before the next check

# A class to implement read-write locks for thread synchronization
class ReadWriteLock:
    def __init__(self):
        self.read_ready = threading.Condition(threading.Lock())
        self.readers = 0
        self.writer = False

    def acquire_read(self):
        with self.read_ready:
            while self.writer:
                self.read_ready.wait()
            self.readers += 1

    def release_read(self):
        with self.read_ready:
            self.readers -= 1
            if self.readers == 0:
                self.read_ready.notify_all()

    def acquire_write(self):
        with self.read_ready:
            while self.writer or self.readers > 0:
                self.read_ready.wait()
            self.writer = True

    def release_write(self):
        with self.read_ready:
            self.writer = False
            self.read_ready.notify_all()

# Custom HTTP request handler
class CatalogRequestHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        # Acquire read lock before reading the catalog
        self.server.lock.acquire_read()
        try:
            # Processing GET request for information
            if self.path.startswith("/product"):
                toyName = self.path.split("/product/")[-1].lower()
                catalogStock = self.server.catalogStock
                if toyName in catalogStock:
                    # Product found, sending product information as JSON
                    self.send_response(200)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    message = {"data": catalogStock[toyName]}
                    self.wfile.write(json.dumps(message).encode())
                else:
                    # Product not found, sending error message
                    self.send_response(404)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(get_error_message(404, "product not found")).encode())
            else:
                # If the request is not for a product, use default GET handler
                super().do_GET()
        finally:
            # Release read lock after processing the request
            self.server.lock.release_read()

    def do_POST(self):
        # Acquire write lock before modifying the catalog
        self.server.lock.acquire_write()
        try:
            # Processing POST request for updating the catalog (e.g., placing an order)
            if self.path.startswith("/catalogUpdate"):
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                request_body = json.loads(post_data.decode('utf-8'))
                toy_name = request_body['name'].lower()
                quantity = request_body['quantity']
                # Attempt to update catalog with the new order
                catalogStock = self.server.catalogStock
                rows = self.server.rows
                if toy_name in catalogStock and catalogStock[toy_name]['quantity'] >= quantity:
                    for row in rows:
                        if toy_name == str(row[0]).lower():
                            row[2] = str(int(row[2]) - quantity)
                            # Modify in memory
                            catalogStock[toy_name]['quantity'] -= quantity
                    # Modify csv file with updated quantity
                    with open(CATALOG_FILENAME, 'w', newline='') as csvfile:
                        csvwriter = csv.writer(csvfile)
                        csvwriter.writerows(rows)
                    # Update successful, sending success message
                    notify_frontend_invalidation(toy_name)
                    self.send_response(201)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(get_response_message(201, "Order placed successfully")).encode())
                else:
                    # Update failed (invalid order), sending error message
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(get_error_message(400, "Invalid order")).encode())
            else:
                # If the request is not recognized, send 404 Not Found
                self.send_error(404, "Not Found")
        finally:
            # Release write lock after processing the request
            self.server.lock.release_write()

# Server setup using threading mixin for handling requests in separate threads
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
        self.rows, self.catalogStock = getFileContent()  # Load catalog data
        self.lock = ReadWriteLock()  # Initialize the read-write lock
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)

# Main function to start the server
if __name__ == '__main__':
    server = ThreadedHTTPServer((HOST_NAME, CATALOG_PORT), CatalogRequestHandler)
    print(time.asctime(), 'Server UP - %s:%s' % (HOST_NAME, CATALOG_PORT))

    # Starting the periodic restocking thread
    restocking_thread = threading.Thread(target=periodic_check_and_restock, args=(server.catalogStock, server.lock, server.rows))
    restocking_thread.daemon = True  # Mark as a daemon so it will be killed when the main thread exits
    restocking_thread.start()
    try:
        server.serve_forever()  # Serve until interrupted
    except KeyboardInterrupt:
        server.server_close()  # Clean shutdown on interrupt
        print(time.asctime(), 'Server DOWN - %s:%s' % (HOST_NAME, CATALOG_PORT))
