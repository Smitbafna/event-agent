"""Tests for EventAgent models."""

import json

from eventagent.models import Correlation, Event, EventType


def test_correlation_model():
    """Test Correlation model with optional fields."""
    # Test with all fields
    corr = Correlation(order_id="123", customer_id="456", payment_id="789")
    assert corr.order_id == "123"
    assert corr.customer_id == "456"
    assert corr.payment_id == "789"
    
    # Test model_dump returns only non-None values
    dumped = corr.model_dump()
    assert dumped == {"order_id": "123", "customer_id": "456", "payment_id": "789"}


def test_correlation_partial():
    """Test Correlation model with partial fields."""
    corr = Correlation(order_id="8472")
    dumped = corr.model_dump()
    assert dumped == {"order_id": "8472"}
    # model_dump removes None values, so customer_id won't be in the dict
    assert "customer_id" not in dumped


def test_event_creation():
    """Test basic Event creation."""
    event = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    )
    
    assert event.event_type == "order.created"
    assert event.source == "order-service"
    assert event.data == {"amount": 1000}
    assert event.event_id.startswith("evt_")


def test_event_factory_method():
    """Test Event.create factory method."""
    event = Event.create(
        event_type="order.created",
        source="order-service",
        data={"amount": 1000, "currency": "USD"},
        correlation=Correlation(order_id="8472"),
    )
    
    assert event.event_type == "order.created"
    assert event.source == "order-service"
    assert event.data == {"amount": 1000, "currency": "USD"}
    assert event.correlation.order_id == "8472"


def test_event_json_serialization():
    """Test event can be serialized to JSON."""
    event = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    )
    
    # Test to_json_dict
    json_dict = event.to_json_dict()
    
    assert "event_id" in json_dict
    assert json_dict["event_type"] == "order.created"
    assert json_dict["source"] == "order-service"
    assert json_dict["correlation"]["order_id"] == "8472"
    assert json_dict["data"]["amount"] == 1000


def test_event_to_json_dict():
    """Test to_json_dict method returns proper dictionary."""
    event = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472", customer_id="abc"),
        data={"amount": 1000},
    )
    
    result = event.to_json_dict()
    
    assert result["event_id"].startswith("evt_")
    assert result["event_type"] == "order.created"
    assert result["source"] == "order-service"
    assert isinstance(result["timestamp"], str)
    assert result["correlation"] == {"order_id": "8472", "customer_id": "abc"}
    assert result["data"] == {"amount": 1000}


def test_event_validation():
    """Test Event validates JSON correctly."""
    json_data = {
        "event_id": "evt_test123",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {"order_id": "8472"},
        "data": {"amount": 1000},
    }
    
    event = Event.model_validate_json(json.dumps(json_data))
    
    assert event.event_id == "evt_test123"
    assert event.event_type == "order.created"
    assert event.source == "order-service"


def test_event_with_dict_correlation():
    """Test Event accepts dict for correlation as well as Correlation model."""
    event = Event(
        event_type="order.created",
        source="order-service",
        correlation={"order_id": "8472", "custom_key": "custom_value"},
        data={"amount": 1000},
    )
    
    assert event.correlation["order_id"] == "8472"
    assert event.correlation["custom_key"] == "custom_value"


def test_event_type_enum():
    """Test EventType enum values."""
    assert EventType.ORDER_CREATED.value == "order.created"
    assert EventType.ORDER_CANCELLED.value == "order.cancelled"
    assert EventType.PAYMENT_INITIATED.value == "payment.initiated"
    assert EventType.PAYMENT_FAILED.value == "payment.failed"
    assert EventType.PAYMENT_RETRY_SCHEDULED.value == "payment.retry_scheduled"


def test_standard_event_envelope():
    """Test that Event follows the standard envelope structure.
    
    The expected structure:
    {
        "event_id": "evt_123",
        "event_type": "order.created",
        "timestamp": "...",
        "source": "order-service",
        "correlation": {
            "order_id": "8472"
        },
        "data": {}
    }
    """
    event = Event(
        event_type="order.created",
        source="order-service",
        correlation={"order_id": "8472"},
        data={"amount": 1000},
    )
    
    json_dict = event.to_json_dict()
    
    # Verify all required fields present
    assert "event_id" in json_dict
    assert "event_type" in json_dict
    assert "timestamp" in json_dict
    assert "source" in json_dict
    assert "correlation" in json_dict
    assert "data" in json_dict
    
    # Verify field types
    assert isinstance(json_dict["event_id"], str)
    assert json_dict["event_id"].startswith("evt_")
    assert isinstance(json_dict["event_type"], str)
    assert isinstance(json_dict["timestamp"], str)
    assert isinstance(json_dict["source"], str)
    assert isinstance(json_dict["correlation"], dict)
    assert isinstance(json_dict["data"], dict)
