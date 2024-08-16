import requests
import json

def test_toy_lookup_error():
    url = 'http://localhost:8080/products/Panda'
    response = requests.get(url)
    response_dict = json.loads(response.content.decode())  
    expected_dict = '{"error": {"code": 404, "message": "product not found"}}'
    assert response_dict == expected_dict

def test_toy_lookup_success():
  url = 'http://localhost:8080/products/whale'
  response = requests.get(url)
  expected_response_prefix = '"{"data": {"name": "whale"'
  actual_response = response.content.decode().replace('\\', '')
  assert actual_response.startswith(expected_response_prefix)

def test_place_order_error_wrong_quantity():
  url = "http://localhost:8080/orders"
  payload = json.dumps({
    "name": "tux",
    "quantity": 1000000
  })
  headers = {
    'Content-Type': 'application/json'
  }

  response = requests.post(url, headers=headers, data=payload)
  print(response.content)
  expected_response = b'{"error": {"code": 400, "message": "Invalid order"}}'
  assert response.content == expected_response

def test_place_order_error_wrong_toyname():
  url = "http://localhost:8080/orders"
  payload = json.dumps({
    "name": "NonExistentToy",
    "quantity": 10
  })
  headers = {
    'Content-Type': 'application/json'
  }

  response = requests.post(url, headers=headers, data=payload)
  expected_response = b'{"error": {"code": 404, "message": "product not found"}}'
  assert response.content == expected_response

def test_place_order_success():
  url = "http://localhost:8080/orders"
  payload = json.dumps({
    "name": "fox",
    "quantity": 2 
  })
  headers = {
    'Content-Type': 'application/json'
  }

  response = requests.post(url, headers=headers, data=payload)
  expected_response_prefix = b'{"data": {"order_number"'
  assert response.content.startswith(expected_response_prefix)


def test_get_order_success():
  url = "http://localhost:8080/orders/1"

  response = requests.get(url)
  expected_response_prefix = b'{"data": {"number": "1"'
  assert response.content.startswith(expected_response_prefix)

def test_get_order_invalid():
  url = "http://localhost:8080/orders/100000"

  response = requests.get(url)
  expected_response = b'{"error": {"code": 404, "message": "Order not found"}}'
  assert response.content == expected_response
