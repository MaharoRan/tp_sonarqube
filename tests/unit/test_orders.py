import pytest
from app.main import Order, build_order_event


def test_order_total():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=2,
        unit_price=50.0,
    )
    event = build_order_event(order)
    assert event["total_amount"] == 100.0


def test_order_contains_order_id():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=1,
        unit_price=10.0,
    )
    event = build_order_event(order)
    assert event["order_id"].startswith("ORD-")


def test_quantity_must_be_positive():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=0,
            unit_price=10.0,
        )


# TODO étudiants :
# Ajouter des tests pour les autres règles métier.
def test_quantity_can_be_null():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=None,
            unit_price=10.0,
        )


def test_quantity_must_not_be_negative():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=-1,
            unit_price=10.0,
        )


def test_positive_quantity_is_valid():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=1,
        unit_price=10.0,
    )
    assert order.quantity == 1

def test_unit_price_must_be_positive():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=1,
            unit_price=0.0,
        )


def test_positive_unit_price_is_valid():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=1,
        unit_price=10.0,
    )
    assert order.unit_price == 10.0

def test_customer_id_min_length():
    with pytest.raises(Exception):
        Order(
            customer_id="C",
            product_id="P001",
            quantity=1,
            unit_price=10.0,
        )

def test_product_id_min_length():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P",
            quantity=1,
            unit_price=10.0,
        )

def test_quantity_max_value():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=1001,
            unit_price=10.0,
        )

def test_unit_price_max_value():
    with pytest.raises(Exception):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=1,
            unit_price=110000.0,
        )


def test_order_event_is_constructed_correctly():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=2,
        unit_price=50.0,
    )

    event = build_order_event(order)

    assert event["customer_id"] == "C001"
    assert event["product_id"] == "P001"
    assert event["quantity"] == 2
    assert event["unit_price"] == 50.0
    assert event["total_amount"] == 100.0
    assert event["order_id"].startswith("ORD-")
    assert event["timestamp"]


def test_health_endpoint():
    from fastapi.testclient import TestClient
    from app.main import app

    response = TestClient(app).get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "UP", "service": "sales-api"}


def test_products_endpoint():
    from fastapi.testclient import TestClient
    from app.main import PRODUCTS, app

    response = TestClient(app).get("/api/products")

    assert response.status_code == 200
    assert response.json() == PRODUCTS


class _SuccessfulFuture:
    def get(self, timeout):
        return self


class _FailingFuture:
    def get(self, timeout):
        raise RuntimeError("Kafka unavailable")


class _FakeProducer:
    def __init__(self, future):
        self.future = future
        self.sent = []
        self.flushed = False
        self.closed = False

    def send(self, topic, value):
        self.sent.append((topic, value))
        return self.future

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


def test_create_order_publishes_and_closes_producer(monkeypatch):
    from fastapi.testclient import TestClient
    from app import main

    producer = _FakeProducer(_SuccessfulFuture())
    monkeypatch.setattr(main, "create_kafka_producer", lambda: producer)

    response = TestClient(main.app).post(
        "/api/orders",
        json={
            "customer_id": "C001",
            "product_id": "P001",
            "quantity": 2,
            "unit_price": 50.0,
        },
    )

    assert response.status_code == 201
    assert producer.sent[0][0] == main.KAFKA_TOPIC
    assert producer.sent[0][1] == response.json()
    assert producer.flushed is True
    assert producer.closed is True


def test_create_order_closes_producer_when_publish_fails(monkeypatch):
    from fastapi.testclient import TestClient
    from app import main

    producer = _FakeProducer(_FailingFuture())
    monkeypatch.setattr(main, "create_kafka_producer", lambda: producer)

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        TestClient(main.app).post(
            "/api/orders",
            json={
                "customer_id": "C001",
                "product_id": "P001",
                "quantity": 1,
                "unit_price": 10.0,
            },
        )

    assert producer.closed is True


def test_create_order_rejects_unknown_product():
    from fastapi.testclient import TestClient
    from app.main import app

    response = TestClient(app).post(
        "/api/orders",
        json={
            "customer_id": "C001",
            "product_id": "UNKNOWN",
            "quantity": 1,
            "unit_price": 10.0,
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown product"


def test_create_kafka_producer_configuration(monkeypatch):
    import sys
    import types
    from app import main

    captured = {}

    class ConfiguredProducer:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    fake_kafka = types.ModuleType("kafka")
    fake_kafka.KafkaProducer = ConfiguredProducer
    monkeypatch.setitem(sys.modules, "kafka", fake_kafka)

    main.create_kafka_producer()

    assert captured["bootstrap_servers"] == main.KAFKA_BOOTSTRAP_SERVERS
    assert captured["retries"] == 5
    assert captured["value_serializer"]({"ok": True}) == b'{"ok": true}'


