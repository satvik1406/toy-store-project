import requests
import json

def test_gettoy_success():
    url = 'http://localhost:8081/product/Whale'
    response = requests.get(url)
    expected_response_prefix = b'{"data": {"name": "whale", "price":'
    assert response.content.startswith(expected_response_prefix)

def test_gettoy_error():
    url = 'http://localhost:8081/product/UnknownToy'
    response = requests.get(url)
    expected_response = b'{"error": {"code": 404, "message": "product not found"}}'
    assert response.content == expected_response

def test_order_success():
    url = "http://localhost:8081/catalogUpdate"

    payload = json.dumps({
        "name": "tux",
        "quantity": 5
    })
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, headers=headers, data=payload)
    expected = b'{"code": 201, "message": "Order placed successfully"}'
    assert response.content == expected

def test_order_error_wrong_quantity():
    url = "http://localhost:8081/catalogUpdate"

    payload = json.dumps({
        "name": "Whale",
        "quantity": 1000000
    })
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, headers=headers, data=payload)
    expected = b'{"error": {"code": 400, "message": "Invalid order"}}'
    assert response.content == expected

def test_order_error_wrong_product():
    url = "http://localhost:8081/catalogUpdate"

    payload = json.dumps({
        "name": "Panda",
        "quantity": 5
    })
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, headers=headers, data=payload)
    expected = b'{"error": {"code": 400, "message": "Invalid order"}}'
    assert response.content == expected
