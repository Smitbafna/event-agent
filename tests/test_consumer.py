"""Tests for EventAgent consumer."""

import asyncio
import json
import tempfile
from unittest.mock import AsyncMock, MagicMock

from eventagent.consumer import EventConsumer
from eventagent.models import Correlation, Event, EventType
from eventagent.storage import SQLiteEventStore


def test_consumer_initialization():
    """Test EventConsumer can be initialized."""
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    assert consumer.nc == nc
    assert consumer.js == js
    assert consumer.storage == storage
    assert consumer.handlers == {}
    assert consumer._running is False
    
    storage.close()


def test_register_handler():
    """Test handler registration."""
    nc = MagicMock()
    js = MagicMock()
    consumer = EventConsumer(nc, js)
    
    async def handler(event: Event):
        pass
    
    consumer.register_handler("order.created", handler)
    
    assert "order.created" in consumer.handlers
    assert handler in consumer.handlers["order.created"]
    
    # Register another handler for same type
    async def handler2(event: Event):
        pass
    
    consumer.register_handler("order.created", handler2)
    
    assert len(consumer.handlers["order.created"]) == 2


def test_process_event_success():
    """Test successful event processing."""
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    # Create a mock message
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_test123",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {"order_id": "8472"},
        "data": {"amount": 1000},
    }).encode()
    msg.ack = AsyncMock()
    
    # Process the event
    asyncio.run(consumer.process_event(msg))
    
    # Verify ack was called
    msg.ack.assert_called_once()
    
    # Verify event was stored
    events = storage.get_events(event_type="order.created")
    assert len(events) == 1
    assert events[0]["event_id"] == "evt_test123"
    assert events[0]["correlation_key"] == "order_id"
    assert events[0]["correlation_value"] == "8472"
    assert events[0]["payload"] == '{"amount": 1000}'
    
    storage.close()


def test_process_event_with_handlers():
    """Test event processing calls registered handlers."""
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    handler_called = []
    
    async def handler(event: Event):
        handler_called.append(event.event_type)
    
    consumer.register_handler("order.created", handler)
    
    # Create a mock message
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_test456",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {},
        "data": {"amount": 500},
    }).encode()
    msg.ack = AsyncMock()
    
    asyncio.run(consumer.process_event(msg))
    
    # Verify handler was called
    assert "order.created" in handler_called
    
    storage.close()


def test_process_event_invalid_json():
    """Test event processing with invalid JSON."""
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    # Create a mock message with invalid data
    msg = AsyncMock()
    msg.data = b"not valid json"
    msg.nak = AsyncMock()
    
    asyncio.run(consumer.process_event(msg))
    
    # Verify nak was called (message rejected)
    msg.nak.assert_called_once()
    
    storage.close()


def test_process_event_invalid_event():
    """Test event processing with invalid event data."""
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    # Create a mock message missing required fields
    msg = AsyncMock()
    msg.data = json.dumps({"not": "an event"}).encode()
    msg.nak = AsyncMock()
    
    asyncio.run(consumer.process_event(msg))
    
    # Verify nak was called
    msg.nak.assert_called_once()
    
    storage.close()


def test_consumer_without_storage():
    """Test consumer works without storage."""
    nc = MagicMock()
    js = MagicMock()
    consumer = EventConsumer(nc, js, None)
    
    # Create a mock message
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_test789",
        "event_type": "order.cancelled",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {},
        "data": {},
    }).encode()
    msg.ack = AsyncMock()
    
    # Should not raise error
    asyncio.run(consumer.process_event(msg))
    
    msg.ack.assert_called_once()


def test_handler_error_does_not_stop_processing():
    """Test that handler errors don't prevent event ack."""
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    async def failing_handler(event: Event):
        raise ValueError("Handler failed!")
    
    consumer.register_handler("payment.failed", failing_handler)
    
    # Create a mock message
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_test999",
        "event_type": "payment.failed",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "payment-service",
        "correlation": {"payment_id": "pay_123"},
        "data": {"error": "insufficient funds"},
    }).encode()
    msg.ack = AsyncMock()
    
    # Process should not raise
    asyncio.run(consumer.process_event(msg))
    
    # Ack should still be called
    msg.ack.assert_called_once()
    
    storage.close()


def test_get_events_by_correlation():
    """Test retrieving events by correlation key and value."""
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    # Store multiple events with same order_id
    event1 = Event(
        event_type=EventType.ORDER_CREATED.value,
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    )
    event2 = Event(
        event_type=EventType.PAYMENT_INITIATED.value,
        source="payment-service",
        correlation=Correlation(order_id="8472", payment_id="pay_123"),
        data={"amount": 1000},
    )
    event3 = Event(
        event_type=EventType.PAYMENT_FAILED.value,
        source="payment-service",
        correlation=Correlation(order_id="9999"),
        data={"error": "test"},
    )
    
    storage.store_event(event1)
    storage.store_event(event2)
    storage.store_event(event3)
    
    # Query by order_id
    events = storage.get_events_by_correlation("order_id", "8472")
    assert len(events) == 2
    
    # Should be ordered by timestamp ASC
    assert events[0]["event_type"] == "order.created"
    assert events[1]["event_type"] == "payment.initiated"
    
    storage.close()


def test_store_event_extracts_correlation():
    """Test that store_event properly extracts correlation key/value."""
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    event = Event(
        event_type=EventType.ORDER_CREATED.value,
        source="order-service",
        correlation=Correlation(order_id="8472", customer_id="cust_123"),
        data={"amount": 1000},
    )
    
    storage.store_event(event)
    
    events = storage.get_events(limit=10)
    assert len(events) == 1
    
    # The first key in correlation should be extracted
    assert events[0]["correlation_key"] in ["order_id", "customer_id"]
    assert events[0]["correlation_value"] is not None
    
    storage.close()


def test_process_payment_initiated_event():
    """Test successful processing of payment.initiated event."""
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_pay_init",
        "event_type": "payment.initiated",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "payment-service",
        "correlation": {"order_id": "8472", "payment_id": "pay_abc123"},
        "data": {"amount": 1000},
    }).encode()
    msg.ack = AsyncMock()
    
    asyncio.run(consumer.process_event(msg))
    
    msg.ack.assert_called_once()
    
    events = storage.get_events(event_type="payment.initiated")
    assert len(events) == 1
    assert events[0]["event_id"] == "evt_pay_init"
    
    storage.close()


def test_process_payment_succeeded_event():
    """Test successful processing of payment.succeeded event."""
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_pay_succ",
        "event_type": "payment.succeeded",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "payment-service",
        "correlation": {"order_id": "8472", "payment_id": "pay_abc123"},
        "data": {"amount": 1000, "transaction_id": "txn_xyz"},
    }).encode()
    msg.ack = AsyncMock()
    
    asyncio.run(consumer.process_event(msg))
    
    msg.ack.assert_called_once()
    
    events = storage.get_events(event_type="payment.succeeded")
    assert len(events) == 1
    assert events[0]["event_id"] == "evt_pay_succ"
    
    storage.close()


def test_milestone_1b_payment_flow():
    """Test the Milestone 1B event flow: order.created -> payment.initiated -> payment.succeeded.
    
    This test verifies that all three events can be processed and stored in sequence,
    maintaining proper correlation linking.
    """
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    # Step 1: Process order.created
    msg1 = AsyncMock()
    msg1.data = json.dumps({
        "event_id": "evt_order_1",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {"order_id": "8472"},
        "data": {"amount": 1000},
    }).encode()
    msg1.ack = AsyncMock()
    
    asyncio.run(consumer.process_event(msg1))
    msg1.ack.assert_called_once()
    
    # Step 2: Process payment.initiated
    msg2 = AsyncMock()
    msg2.data = json.dumps({
        "event_id": "evt_pay_init_1",
        "event_type": "payment.initiated",
        "timestamp": "2026-07-19T10:01:00Z",
        "source": "payment-service",
        "correlation": {"order_id": "8472", "payment_id": "pay_abc"},
        "data": {"amount": 1000},
    }).encode()
    msg2.ack = AsyncMock()
    
    asyncio.run(consumer.process_event(msg2))
    msg2.ack.assert_called_once()
    
    # Step 3: Process payment.succeeded
    msg3 = AsyncMock()
    msg3.data = json.dumps({
        "event_id": "evt_pay_succ_1",
        "event_type": "payment.succeeded",
        "timestamp": "2026-07-19T10:02:00Z",
        "source": "payment-service",
        "correlation": {"order_id": "8472", "payment_id": "pay_abc"},
        "data": {"amount": 1000, "transaction_id": "txn_xyz"},
    }).encode()
    msg3.ack = AsyncMock()
    
    asyncio.run(consumer.process_event(msg3))
    msg3.ack.assert_called_once()
    
    # Verify all events are stored and can be queried by correlation
    events = storage.get_events_by_correlation("order_id", "8472")
    assert len(events) == 3
    
    # Verify order: order.created -> payment.initiated -> payment.succeeded
    assert events[0]["event_type"] == "order.created"
    assert events[1]["event_type"] == "payment.initiated"
    assert events[2]["event_type"] == "payment.succeeded"
    
    storage.close()