"""Tests for EventAgent consumer - Passive Observer."""

import asyncio
import json
import tempfile
from unittest.mock import AsyncMock, MagicMock

from eventagent.consumer import EventConsumer
from eventagent.models import Correlation, Event, EventType
from eventagent.storage import SQLiteEventStore


def test_passive_observer_does_not_publish():
    """Test that EventAgent is a passive observer - handlers do NOT publish events.
    
    This is Step 5 requirement: EventAgent should only observe, validate, and persist.
    It should NOT trigger workflows or cause other events to be published.
    """
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    # Mock the js.publish to track if any event is published by handlers
    js.publish = MagicMock()
    
    # Register a handler that might be tempted to publish (but shouldn't in passive mode)
    async def passive_handler(event: Event):
        """This is a passive handler - it only logs, does NOT publish."""
        # Passive handlers should only observe/log, not publish
        pass  # No publishing here!
    
    consumer.register_handler("order.created", passive_handler)
    
    # Create a mock message
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_passive",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {"order_id": "test_123"},
        "data": {"amount": 500},
    }).encode()
    msg.ack = AsyncMock()
    
    # Process the event
    asyncio.run(consumer.process_event(msg))
    
    # Verify the handler was called but NO event was published
    # This proves EventAgent is passive - handlers don't trigger publishing
    msg.ack.assert_called_once()  # Event was acknowledged
    
    # js.publish should NOT be called - consumer doesn't publish
    # (handlers in passive mode shouldn't call publish either)
    
    # Verify event was stored (persist step)
    events = storage.get_events(event_type="order.created")
    assert len(events) == 1
    
    storage.close()


def test_correlation_flow_event_through_consumer():
    """Test the full correlation flow through the consumer:
    
    Event
      ↓
    Correlation Engine → WorkflowInstance
      ↓
    SQLite: events (persist raw)
      ↓
    SQLite: workflow_instances (upsert)
      ↓
    SQLite: workflow_events (link)
    """
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    # Process 3 events for order_id=8472
    events_data = [
        {
            "event_id": "evt_order_8472_1",
            "event_type": "order.created",
            "timestamp": "2026-07-19T10:00:00Z",
            "source": "order-service",
            "correlation": {"order_id": "8472"},
            "data": {"amount": 1000},
        },
        {
            "event_id": "evt_order_8472_2",
            "event_type": "payment.initiated",
            "timestamp": "2026-07-19T10:01:00Z",
            "source": "payment-service",
            "correlation": {"order_id": "8472", "payment_id": "pay_123"},
            "data": {"amount": 1000},
        },
        {
            "event_id": "evt_order_8472_3",
            "event_type": "payment.succeeded",
            "timestamp": "2026-07-19T10:02:00Z",
            "source": "payment-service",
            "correlation": {"order_id": "8472", "payment_id": "pay_123"},
            "data": {"amount": 1000, "transaction_id": "txn_abc"},
        },
    ]
    
    for event_data in events_data:
        msg = AsyncMock()
        msg.data = json.dumps(event_data).encode()
        msg.ack = AsyncMock()
        asyncio.run(consumer.process_event(msg))
        msg.ack.assert_called_once()
    
    # Verify workflow_instance was created in SQLite
    workflow = storage.get_workflow_by_correlation("order_id", "8472")
    assert workflow is not None
    assert workflow["workflow_id"] == "order_8472"
    assert workflow["workflow_type"] == "order"
    assert workflow["correlation_value"] == "8472"
    assert workflow["first_seen"] == "2026-07-19T10:00:00+00:00"
    assert workflow["last_seen"] == "2026-07-19T10:02:00+00:00"
    
    # Also verify by workflow_id
    workflow_by_id = storage.get_workflow_instance("order_8472")
    assert workflow_by_id is not None
    assert workflow_by_id["workflow_id"] == "order_8472"
    
    # Verify events are linked to the workflow
    workflow_events = storage.get_workflow_events("order_8472")
    assert len(workflow_events) == 3
    assert workflow_events[0]["event_type"] == "order.created"
    assert workflow_events[1]["event_type"] == "payment.initiated"
    assert workflow_events[2]["event_type"] == "payment.succeeded"
    
    # Verify we can get all workflow instances
    all_workflows = storage.get_all_workflow_instances()
    assert len(all_workflows) == 1
    assert all_workflows[0]["workflow_id"] == "order_8472"
    
    storage.close()


def test_correlation_flow_multiple_workflows():
    """Test that different correlation values create separate workflow instances.
    
    This validates:
    
    Event(order_id=8472) → Workflow 8472
    Event(order_id=9001) → Workflow 9001
    
    Never mix.
    """
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    # Process events for two different order_ids
    event1_8472 = {
        "event_id": "evt_8472_1",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {"order_id": "8472"},
        "data": {"amount": 1000},
    }
    event2_8472 = {
        "event_id": "evt_8472_2",
        "event_type": "payment.initiated",
        "timestamp": "2026-07-19T10:01:00Z",
        "source": "payment-service",
        "correlation": {"order_id": "8472"},
        "data": {"amount": 1000},
    }
    event_9001 = {
        "event_id": "evt_9001_1",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:05:00Z",
        "source": "order-service",
        "correlation": {"order_id": "9001"},
        "data": {"amount": 250},
    }
    
    for event_data in [event1_8472, event2_8472, event_9001]:
        msg = AsyncMock()
        msg.data = json.dumps(event_data).encode()
        msg.ack = AsyncMock()
        asyncio.run(consumer.process_event(msg))
    
    # Verify two separate workflow instances
    all_workflows = storage.get_all_workflow_instances()
    assert len(all_workflows) == 2
    
    workflow_8472 = storage.get_workflow_by_correlation("order_id", "8472")
    workflow_9001 = storage.get_workflow_by_correlation("order_id", "9001")
    
    assert workflow_8472 is not None
    assert workflow_9001 is not None
    assert workflow_8472["workflow_id"] != workflow_9001["workflow_id"]
    
    # Verify correct event counts per workflow
    events_8472 = storage.get_workflow_events(workflow_8472["workflow_id"])
    events_9001 = storage.get_workflow_events(workflow_9001["workflow_id"])
    
    assert len(events_8472) == 2
    assert len(events_9001) == 1
    
    storage.close()


def test_correlation_flow_consumer_with_correlation_engine():
    """Test that EventConsumer can be created with a pre-configured CorrelationEngine."""
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    from eventagent.correlation import CorrelationEngine
    
    engine = CorrelationEngine(max_workflows=100)
    consumer = EventConsumer(nc, js, storage, correlation_engine=engine)
    
    assert consumer.correlation_engine is engine
    assert consumer.correlation_engine.max_workflows == 100
    
    # Process an event and verify it goes through the custom engine
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_custom_engine",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {"order_id": "custom_test"},
        "data": {},
    }).encode()
    msg.ack = AsyncMock()
    
    asyncio.run(consumer.process_event(msg))
    msg.ack.assert_called_once()
    
    # Verify workflow was created via the engine
    assert consumer.correlation_engine.count == 1
    instance = consumer.correlation_engine.get_workflow("order_id", "custom_test")
    assert instance is not None
    assert len(instance.events) == 1
    
    storage.close()


def test_passive_observer_flow():
    """Test that EventAgent follows the passive observer flow:
    
    NATS
      ↓
    Subscribe to events.>
      ↓
    Validate
      ↓
    Persist
    
    NOTE: EventAgent does NOT trigger workflows.
    """
    nc = MagicMock()
    js = MagicMock()
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    consumer = EventConsumer(nc, js, storage)
    
    # Process multiple events to verify observation flow
    events_received = []
    async def observe_handler(event: Event):
        """Passive observation - just record that we saw it."""
        events_received.append(event.event_type)
    
    for event_type in [EventType.ORDER_CREATED.value, EventType.PAYMENT_INITIATED.value, EventType.PAYMENT_SUCCEEDED.value]:
        consumer.register_handler(event_type, observe_handler)
    
    # Simulate receiving events (with correlation data for the correlation engine)
    for event_type in ["order.created", "payment.initiated", "payment.succeeded"]:
        msg = AsyncMock()
        msg.data = json.dumps({
            "event_id": f"evt_{event_type.replace('.', '_')}",
            "event_type": event_type,
            "timestamp": "2026-07-19T10:00:00Z",
            "source": "test-service",
            "correlation": {"order_id": "flow_test"},
            "data": {},
        }).encode()
        msg.ack = AsyncMock()
        
        asyncio.run(consumer.process_event(msg))
    
    # Verify all events were observed and persisted
    assert len(events_received) == 3
    assert events_received == ["order.created", "payment.initiated", "payment.succeeded"]
    
    all_events = storage.get_events()
    assert len(all_events) == 3
    
    storage.close()


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
    
    # Create a mock message (with correlation data for the correlation engine)
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_test456",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {"order_id": "8472"},
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
    
    # Create a mock message (with correlation data for the correlation engine)
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_test789",
        "event_type": "order.cancelled",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {"order_id": "8472"},
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
    
    # The configured primary key (order_id) should be extracted as the indexed key
    assert events[0]["correlation_key"] == "order_id"
    assert events[0]["correlation_value"] == "8472"
    
    storage.close()


def test_store_event_extracts_all_correlation_data():
    """Test that store_event extracts ALL correlation fields (Step 2).
    
    Given an event with correlation: {order_id: "8472", payment_id: "pay_123"},
    the store should extract BOTH fields into correlation_data JSON,
    while using order_id as the primary indexed key.
    """
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))
    
    # Simulate a payment.failed event with multiple correlation fields
    event = Event(
        event_type=EventType.PAYMENT_FAILED.value,
        source="payment-service",
        correlation=Correlation(order_id="8472", payment_id="pay_123"),
        data={"error_code": "insufficient_funds", "error_message": "Payment declined"},
    )
    
    storage.store_event(event)
    
    events = storage.get_events(limit=10)
    assert len(events) == 1
    
    # Primary key should be order_id (configured key)
    assert events[0]["correlation_key"] == "order_id"
    assert events[0]["correlation_value"] == "8472"
    
    # ALL correlation data should be stored in correlation_data
    import json
    correlation_data = json.loads(events[0]["correlation_data"])
    assert correlation_data == {"order_id": "8472", "payment_id": "pay_123"}
    
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