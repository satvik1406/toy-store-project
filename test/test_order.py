import requests
import json

def test_order_success():
    url = "http://localhost:8002/order"

    payload = json.dumps({
        "name": "tux",
        "quantity": 10,
        "node" : 2
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    expected_response_prefix = b'{"data": {"order_number":'
    assert response.content.startswith(expected_response_prefix)

def test_order_wrong_toy():
    url = "http://localhost:8002/order"

    payload = json.dumps({
        "name": "Panda",
        "quantity": 10,
        "node" : 2
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    expected_response = b'{"error": {"code": 404, "message": "product not found"}}'
    assert response.content == expected_response

def test_order_wrong_quantity():
    url = "http://localhost:8002/order"

    payload = json.dumps({
        "name": "whale",
        "quantity": 1000000,
        "node" : 2
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    expected_response = b'{"error": {"code": 400, "message": "Invalid order"}}'
    assert response.content == expected_response

def test_get_order_success():
  url = "http://localhost:8002/orders/1?node=2"

  response = requests.get(url)
  expected_response_prefix = b'{"data": {"number": "1"'
  assert response.content.startswith(expected_response_prefix)

def test_get_order_invalid():
  url = "http://localhost:8002/orders/100000?node=2"

  response = requests.get(url)
  expected_response = b'{"error": {"code": 404, "message": "Order not found"}}'
  assert response.content == expected_response
