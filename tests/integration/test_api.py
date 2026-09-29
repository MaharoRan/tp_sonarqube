import json
import os
import time
import uuid

import httpx
import pytest

RUN_INTEGRATION = os.getenv("RUN_INTEGRATION_TESTS", "false").lower() == "true"
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=true."
)



@pytest.fixture(scope="module")
def client():
    with httpx.Client(base_url=API_BASE_URL, timeout=20.0) as api_client:
        yield api_client


def test_health(client):
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["status"] == "UP"


# TODO étudiants :
# Ajouter les tests d'intégration API -> Kafka.
def test_create_order_publishes_event_to_kafka(client):
    from kafka import KafkaConsumer

    topic = os.getenv("KAFKA_TOPIC", "sales.orders")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=f"test-api-{uuid.uuid4().hex}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    order_data = {
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": 2,
        "unit_price": 50.0,
    }

    try:
        consumer.subscribe([topic])
        consumer.poll(timeout_ms=1000)
        response = client.post("/api/orders", json=order_data)
        assert response.status_code == 201
        expected_order_id = response.json()["order_id"]

        deadline = time.monotonic() + 15
        received_event = None
        while time.monotonic() < deadline and received_event is None:
            for records in consumer.poll(timeout_ms=1000).values():
                for record in records:
                    if record.value.get("order_id") == expected_order_id:
                        received_event = record.value
                        break
                if received_event is not None:
                    break

        assert received_event is not None
        assert received_event["customer_id"] == order_data["customer_id"]
        assert received_event["product_id"] == order_data["product_id"]
        assert received_event["quantity"] == order_data["quantity"]
        assert received_event["unit_price"] == order_data["unit_price"]
        assert received_event["total_amount"] == 100.0
    finally:
        consumer.close()


def test_create_order(client):
    order_data = {
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": 2,
        "unit_price": 50.0,
    }
    response = client.post("/api/orders", json=order_data)
    assert response.status_code == 201
    order_event = response.json()
    assert order_event["customer_id"] == order_data["customer_id"]
    assert order_event["product_id"] == order_data["product_id"]
    assert order_event["quantity"] == order_data["quantity"]
    assert order_event["unit_price"] == order_data["unit_price"]
    assert "order_id" in order_event
    assert "total_amount" in order_event
    assert "timestamp" in order_event

def test_create_order_invalid_product(client):
    order_data = {
        "customer_id": "C001",
        "product_id": "INVALID_PRODUCT",
        "quantity": 2,
        "unit_price": 50.0,
        }
    response = client.post("/api/orders", json=order_data)
    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown product"

def test_create_order_invalid_quantity(client):
    order_data = {
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": -1,
        "unit_price": 50.0,
    }
    response = client.post("/api/orders", json=order_data)
    assert response.status_code == 422
    assert "quantity" in response.json()["detail"][0]["loc"]


