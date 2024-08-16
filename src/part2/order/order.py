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
LOG_FILENAME = "raftLog_"

# Configuration
NODE_ID = -1  # This node's ID
NODES = {0: '8000', 1: '8001', 2: '8002'}  # All nodes in the system
TERM = 0
log = []
commit_index = 0
last_applied = 0
last_log_index = -1
commit_log = {}

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
def getFileContent(fileName):
    try:
        with open(fileName, 'r') as csvfile:
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
        """ Handles POST requests for orders and RAFT communications. """
        global TERM, next_index
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        request_body = json.loads(post_data.decode('utf-8'))
        #Endpoint to place an order  
        if self.path.startswith("/order"):
            self.server.lock.acquire_write()  # Acquire write lock before processing the order.
            try:
                self.process_order_request(request_body)
            finally:
                self.server.lock.release_write()  # Release the write lock.

        elif self.path.startswith("/append_entries"):
            self.handle_append_entries(request_body)

        elif self.path.startswith("/commit_entries"):
            self.handle_commit_entries(request_body)
        
        elif self.path.startswith("/rollback"):
            self.handle_rollback(request_body)

    def do_GET(self):
        """ Handles GET requests for retrieving order details. """
        if self.path.startswith("/orders"):
            self.server.lock.acquire_read()  # Acquire read lock before getting the order.
            try:
                params = self.path.split("/orders/")[-1]
                params = params.split('?')
                order_number = int(params[0])
                node = int(params[1].split('=')[1])
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

    def process_order_request(self, request_body):
        """ We initialise the order requirements, append the entry to leaders log and propogate ut """
        global log, last_log_index, next_index, NODES, NODE_ID, TERM
        TERM = request_body['term']
        NODE_ID = request_body['node']
        
        url = f'http://{CATALOG_HOST}:{CATALOG_PORT}/product/{request_body['name']}'
        response = requests.get(url)
        if response.status_code != 200:
            self.send_response(response.status_code)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(response.content)

        log = getFileContent(LOG_FILENAME)  # Read the current log from the CSV file
        next_index = len(log)  # Next log index is the length of the log + 1
        if next_index != -1 : last_log_index = next_index - 1

        entry = {
            'index': next_index,
            'term': TERM,
            'body': json.dumps(request_body) 
        }

        self.append_to_log([entry])
        log = getFileContent(LOG_FILENAME)
        self.replicate_log()

    def replicate_log(self):
        """ Propogating the  append requests and basing a decision on the success and resynchronising the follower nodes if necessary"""
        global commit_index, commit_log, next_index, last_log_index, log, NODES, NODE_ID, TERM
        last_log_term = -1
        if last_log_index != -1: last_log_term = log[-1][1]
        entry_to_replicate = log[-1]
        acks = 1  # Start with 1 because the leader counts as an ack
        majority = len(NODES) // 2 + 1  # Majority needed to commit
        last_good_indices = {}

        # Initial replication of log entries
        for node_id, port in NODES.items():
            if node_id != NODE_ID:
                url = f'http://localhost:{port}/append_entries'
                payload = {
                    'term': TERM,
                    'leaderId': NODE_ID,
                    'prevLogIndex': last_log_index,
                    'prevLogTerm': last_log_term,
                    'entry': entry_to_replicate,
                }
                try:
                    response = requests.post(url, json=payload)
                    if response.json().get('success'):
                        commit_log[node_id] = next_index
                        acks += 1
                    else:
                    # Capture the last known good index if available
                        last_good_indices[node_id] = (response.json().get('lastGoodIndex'),response.json().get('lastGoodTerm'))
                        if self.initiate_resynchronization(node_id, port, last_good_indices[node_id]):
                            acks += 1
                        else:
                            continue
                except requests.RequestException:
                    continue

        if acks >= majority:
            commit_index = next_index  # Commit the entry
            request_body = json.loads(log[commit_index][2])
            self.handle_new_order(request_body)  # Apply the log entry to the state machine
            self.notify_followers_to_commit(commit_index)
        else:
            commit_index = next_index - 1 
            self.rollback_log(commit_index)
            self.send_response(400)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(get_message(400, "Failed to execute the order")).encode())

    def initiate_resynchronization(self, node_id, port, last_good_index_and_term):
        """ Resynchronising the follower nodes with latest data as that of the leader """
        global log, commit_log
        last_good_index, last_good_term = last_good_index_and_term

        while last_good_index >= 0 and last_good_index < len(log) and log[last_good_index][1] != last_good_term:
            last_good_index, last_good_term = self.rollback_log(node_id, port, last_good_index)

        start_index = last_good_index + 1

        logs_to_send = log[start_index:] if start_index < len(log) else []

        for entry in logs_to_send:
            prev_index = int(entry[0])-1
            prev_term = log[prev_index][1] if int(prev_index) >= 0 else None
            url = f'http://localhost:{port}/append_entries'
            payload = {
                'term': TERM,
                'leaderId': NODE_ID,
                'prevLogIndex': prev_index,
                'prevLogTerm': prev_term,
                'entry': entry,
            }
            try:
                response = requests.post(url, json=payload)
                if not response.json().get('success'):
                    print(f"Not able to sync logs for node {node_id}.")
                    return False
            except requests.RequestException as e:
                print(f"Network error in communicating with node {node_id}: {str(e)}")
                return False
        commit_log[node_id] = start_index+1
        return True

    def request_rollback(self, node_id, port, rollback_to_index):
        """ Handle Rollback of orders and propogating to the other nodes """
        url = f'http://localhost:{port}/rollback'
        payload = {'rollback_index': rollback_to_index}
        try:
            response = requests.post(url, json=payload)
            if response.json().get('success'):
                print(f"Node {node_id} successfully rolled back to index {rollback_to_index}.")
                return (response.json().get('lastGoodIndex'),response.json().get('lastGoodTerm'))
            else:
                print(f"Failed to rollback node {node_id} to index {rollback_to_index}.")
                return (-1, -1)
        except requests.RequestException as e:
            print(f"Error communicating with node {node_id} during rollback: {str(e)}")

    def rollback_log(self, index):
        global NODES, NODE_ID
        self.truncate_log(index)
        for node_id, port in NODES.items():
            if node_id != NODE_ID:
                self.request_rollback(node_id, port, index)
    
    def notify_followers_to_commit(self, commit_index):
        """ Notifying the follower nodes to commit the changes """
        global TERM, NODES, NODE_ID, commit_log
        for node_id, port in NODES.items():
            if node_id != NODE_ID:
                index_to_commit = commit_log.get(node_id, commit_index)
                url = f'http://localhost:{port}/commit_entries'
                payload = {
                    'term': TERM,
                    'commit_index': index_to_commit
                }
                try:
                    response = requests.post(url, json=payload, timeout=5)
                    if response.status_code == 200 or response.status_code == 201:
                        print(f"Node {node_id} acknowledged commit.")
                    else:
                        print(f"Node {node_id} failed to commit, HTTP status {response.status_code}.")
                except requests.exceptions.RequestException as e:
                    print(f"Failed to contact node {node_id}: {str(e)}")

    def handle_append_entries(self,data):
        """ Handles Logic to check if the follower is in sync with the leader or not and give out a decision """
        global commit_index, TERM

        log = getFileContent(LOG_FILENAME)
        if log != []: TERM = log[-1][1]

        response = {'term': TERM, 'success': False}
        if int(data['term']) < int(TERM):
            self.send_response(200)
            self.wfile.write(json.dumps(response).encode())
            return

        # Check if log matches up to prevLogIndex and prevLogTerm
        prev_log_index = int(data['prevLogIndex'])
        prev_log_term = data['prevLogTerm']

        if prev_log_index == -1 or (prev_log_index <= len(log) and log[prev_log_index][1] == prev_log_term):
            new_entry = data['entry'] 
            if data['entry']: 
                new_entry = {
                    'term': new_entry[0],
                    'index': new_entry[1],
                    'body': new_entry[2] 
                }
            else: None
            if new_entry:
                self.append_to_log([new_entry])  # Append the new entry to the log CSV
                log.append(new_entry)  # Update in-memory log
                response['success'] = True
        else:
            lastGoodIndex = len(log) - 1 if log else -1
            response['lastGoodIndex'] = lastGoodIndex
            response['lastGoodTerm'] = log[lastGoodIndex][1] if log else -1

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

    def handle_commit_entries(self, request_body):
        """ Handle Commiting entries on the follower nodes """
        global log
        log = getFileContent(LOG_FILENAME)
        commit_index = request_body['commit_index']
        for index in range(commit_index, len(log)):
            try:
                entry = log[index]
                request_body = json.loads(entry[2])  # Assuming the third element is the data to be committed
                self.handle_new_order(request_body)  # Process each log entry
            except Exception as e:
                print(f"Error processing log entry at index {index}: {str(e)}")
                break  # Break on error to prevent further misapplication

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'success': True}).encode())
        
    def handle_rollback(self, request_body):
        """ Handle Rollback entries on the follower nodes """
        global log
        rollback_index = request_body['rollback_index']
        self.truncate_log(rollback_index)
        lastGoodIndex = len(log) - 1 if log else -1
        response = {
            'success': True,
            'lastGoodIndex':lastGoodIndex,
            'lastGoodTerm':log[lastGoodIndex][1] if log else -1
        }
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

    def read_log(self):
        log = []
        with open(LOG_FILENAME, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                row[0] = int(row[0])
                row[1] = int(row[1])
                log.append(row)

        return log

    def append_to_log(self, entries):
        fieldnames = entries[0].keys() if entries else ['index','term', 'body']
        with open(LOG_FILENAME, mode='a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writerows(entries)

    def truncate_log(self, new_size):
        # Load the existing log from file
        log = getFileContent(LOG_FILENAME)
        # Truncate the log to the new size
        truncated_log = log[:new_size]

        # Open the file in write mode to overwrite existing data
        with open(LOG_FILENAME, mode='w', newline='') as file:
            writer = csv.writer(file)
            
            # Only write rows if there are any entries in the truncated log
            for entry in truncated_log:
                writer.writerow(entry)

            # If the truncated log is empty, ensure the file is also empty
            if not truncated_log:
                file.truncate()

    def handle_new_order(self, request_body):        
        toy_name = request_body['name']
        quantity = request_body['quantity']
        node = int(request_body['node'])

        url = f'http://{CATALOG_HOST}:{CATALOG_PORT}/catalogUpdate'
        response = requests.post(url, json=request_body)
        
        # If the catalog update is successful, add the order to the order list and log it.
        if response.status_code == 201:
            order_id = get_file_length(ORDER_FILENAME)
            new_row = [order_id, toy_name, quantity, node]
            OrderRequestHandler.WriteToFile(new_row,ORDER_FILENAME)
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

    def WriteToFile(new_row,ORDER_FILENAME):
        # Persist the order list to disk
        with open(ORDER_FILENAME, 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(new_row)
            csvfile.close()
    
    def _getFileName(node):
        # Form file names depending on the running server
        return ORDER_FILENAME+str(node)+".csv"
    
    def _getLogFileName(node):
        # Form file names depending on the running server
        return LOG_FILENAME+str(node)+".csv"

# Multi-threaded HTTP server capable of handling requests in separate threads.
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
        self.order_list = getFileContent(ORDER_FILENAME)  # Load existing orders from the file.
        self.lock = ReadWriteLock()  # Initialize the read-write lock for managing concurrency.
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)

# Main function to start the server.
if __name__ == '__main__':
    NODE_ID=int(sys.argv[1])
    ORDER_PORT=int(NODES[NODE_ID])
    print('order port is ', ORDER_PORT)
    ORDER_FILENAME = OrderRequestHandler._getFileName(NODE_ID)
    LOG_FILENAME = OrderRequestHandler._getLogFileName(NODE_ID)
    server = ThreadedHTTPServer(("", ORDER_PORT), OrderRequestHandler)

    print(time.asctime(), 'Server UP - %s:%s' % (ORDER_HOST, ORDER_PORT))
    try:
        server.serve_forever()  # Serve until interrupted.
    except KeyboardInterrupt:
        # Gracefully shutdown the server on interrupt.
        server.server_close()
        print(time.asctime(), 'Server DOWN - %s:%s' % (ORDER_HOST, ORDER_PORT))
