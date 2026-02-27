"""Tests for EventAgent models."""

import json

from eventagent.models import Correlation, Event, EventType, UncorrelatedEvent


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
    assert isinstance(result["received_at"], str)
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
    assert EventType.PAYMENT_SUCCEEDED.value == "payment.succeeded"
    assert EventType.PAYMENT_FAILED.value == "payment.failed"
    assert EventType.PAYMENT_RETRY_SCHEDULED.value == "payment.retry_scheduled"


def test_workflow_instance_from_events():
    """Test building a WorkflowInstance from related events."""
    from datetime import datetime, timezone, timedelta
    from eventagent.models import WorkflowInstance

    t1 = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 7, 19, 10, 0, 5, tzinfo=timezone.utc)
    t3 = datetime(2026, 7, 19, 10, 0, 10, tzinfo=timezone.utc)

    events = [
        Event(
            event_id="evt_001",
            event_type="order.created",
            timestamp=t1,
            source="order-service",
            correlation=Correlation(order_id="8472"),
            data={"amount": 1000},
        ),
        Event(
            event_id="evt_002",
            event_type="payment.initiated",
            timestamp=t2,
            source="payment-service",
            correlation=Correlation(order_id="8472", payment_id="pay_123"),
            data={"amount": 1000},
        ),
        Event(
            event_id="evt_003",
            event_type="payment.succeeded",
            timestamp=t3,
            source="payment-service",
            correlation=Correlation(order_id="8472", payment_id="pay_123"),
            data={"amount": 1000},
        ),
    ]

    instance = WorkflowInstance.from_events(events, correlation_key="order_id")

    assert instance.workflow_id == "order_8472"
    assert instance.workflow_type == "order"
    assert instance.correlation_key == "order_id"
    assert instance.correlation_value == "8472"
    assert len(instance.events) == 3
    assert instance.first_seen == t1
    assert instance.last_seen == t3
    assert instance.events[0].event_type == "order.created"
    assert instance.events[1].event_type == "payment.initiated"
    assert instance.events[2].event_type == "payment.succeeded"


def test_workflow_instance_from_single_event():
    """Test building a WorkflowInstance from a single event."""
    from datetime import datetime, timezone
    from eventagent.models import WorkflowInstance

    ts = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
    event = Event(
        event_type="order.created",
        timestamp=ts,
        source="order-service",
        correlation=Correlation(order_id="42"),
        data={"amount": 500},
    )

    instance = WorkflowInstance.from_events([event], correlation_key="order_id")

    assert instance.workflow_id == "order_42"
    assert instance.workflow_type == "order"
    assert instance.correlation_value == "42"
    assert len(instance.events) == 1
    assert instance.first_seen == ts
    assert instance.last_seen == ts


def test_workflow_instance_from_events_with_dict_correlation():
    """Test building WorkflowInstance from events with dict correlation."""
    from datetime import datetime, timezone
    from eventagent.models import WorkflowInstance

    ts = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
    event = Event(
        event_type="order.created",
        timestamp=ts,
        source="order-service",
        correlation={"order_id": "9999", "custom_field": "custom_value"},
        data={"amount": 200},
    )

    instance = WorkflowInstance.from_events([event], correlation_key="order_id")

    assert instance.workflow_id == "order_9999"
    assert instance.correlation_value == "9999"
    assert instance.workflow_type == "order"


def test_workflow_instance_model_dump():
    """Test WorkflowInstance serialization."""
    from datetime import datetime, timezone
    from eventagent.models import WorkflowInstance

    ts = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
    event = Event(
        event_type="order.created",
        timestamp=ts,
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    )

    instance = WorkflowInstance.from_events([event], correlation_key="order_id")
    dumped = instance.model_dump()

    assert dumped["workflow_id"] == "order_8472"
    assert dumped["workflow_type"] == "order"
    assert dumped["correlation_key"] == "order_id"
    assert dumped["correlation_value"] == "8472"
    assert len(dumped["events"]) == 1
    assert dumped["events"][0]["event_type"] == "order.created"
    assert "first_seen" in dumped
    assert "last_seen" in dumped


def test_workflow_instance_from_empty_events_raises():
    """Test that from_events raises ValueError for empty list."""
    import pytest
    from eventagent.models import WorkflowInstance

    with pytest.raises(ValueError, match="Cannot build WorkflowInstance from empty events list"):
        WorkflowInstance.from_events([], correlation_key="order_id")


def test_workflow_instance_out_of_order_events():
    """Test that events are sorted by timestamp in the instance."""
    from datetime import datetime, timezone, timedelta
    from eventagent.models import WorkflowInstance

    t1 = datetime(2026, 7, 19, 10, 0, 10, tzinfo=timezone.utc)
    t2 = datetime(2026, 7, 19, 10, 0, 5, tzinfo=timezone.utc)
    t3 = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)

    events = [
        Event(
            event_id="evt_003",
            event_type="order.created",
            timestamp=t1,
            source="order-service",
            correlation=Correlation(order_id="55"),
            data={},
        ),
        Event(
            event_id="evt_002",
            event_type="payment.initiated",
            timestamp=t2,
            source="payment-service",
            correlation=Correlation(order_id="55"),
            data={},
        ),
        Event(
            event_id="evt_001",
            event_type="payment.succeeded",
            timestamp=t3,
            source="payment-service",
            correlation=Correlation(order_id="55"),
            data={},
        ),
    ]

    instance = WorkflowInstance.from_events(events, correlation_key="order_id")

    # Events should be sorted earliest to latest
    assert instance.events[0].event_id == "evt_001"
    assert instance.events[1].event_id == "evt_002"
    assert instance.events[2].event_id == "evt_003"
    assert instance.first_seen == t3
    assert instance.last_seen == t1


def test_workflow_instance_event_type_without_dot():
    """Test workflow_type derivation when event type has no dot."""
    from datetime import datetime, timezone
    from eventagent.models import WorkflowInstance

    ts = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
    event = Event(
        event_type="custom_event",
        timestamp=ts,
        source="custom-service",
        correlation=Correlation(order_id="1"),
        data={},
    )

    instance = WorkflowInstance.from_events([event], correlation_key="order_id")
    assert instance.workflow_type == "custom_event"
    assert instance.workflow_id == "custom_event_1"


def test_workflow_instance_payment_flow():
    """Test a payment workflow instance derived from events."""
    from datetime import datetime, timezone
    from eventagent.models import WorkflowInstance

    t1 = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 7, 19, 10, 0, 5, tzinfo=timezone.utc)

    events = [
        Event(
            event_type="payment.initiated",
            timestamp=t1,
            source="payment-service",
            correlation=Correlation(payment_id="pay_456"),
            data={"amount": 500},
        ),
        Event(
            event_type="payment.failed",
            timestamp=t2,
            source="payment-service",
            correlation=Correlation(payment_id="pay_456"),
            data={"error": "insufficient_funds"},
        ),
    ]

    instance = WorkflowInstance.from_events(events, correlation_key="payment_id")

    assert instance.workflow_id == "payment_pay_456"
    assert instance.workflow_type == "payment"
    assert instance.correlation_value == "pay_456"
    assert len(instance.events) == 2
    assert instance.events[0].event_type == "payment.initiated"
    assert instance.events[1].event_type == "payment.failed"


def test_workflow_instance_constructed_directly():
    """Test WorkflowInstance can be constructed directly (not just via from_events)."""
    from datetime import datetime, timezone
    from eventagent.models import WorkflowInstance

    ts = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
    instance = WorkflowInstance(
        workflow_id="order_8472",
        workflow_type="order",
        correlation_key="order_id",
        correlation_value="8472",
        events=[],
        first_seen=ts,
        last_seen=ts,
    )

    assert instance.workflow_id == "order_8472"
    assert instance.workflow_type == "order"
    assert len(instance.events) == 0
    assert instance.first_seen == ts
    assert instance.last_seen == ts


def test_workflow_instance_construct_without_events():
    """Test WorkflowInstance with no events (empty workflow)."""
    from eventagent.models import WorkflowInstance

    instance = WorkflowInstance(
        workflow_id="order_123",
        workflow_type="order",
        correlation_key="order_id",
        correlation_value="123",
    )

    assert instance.events == []
    assert instance.first_seen is None
    assert instance.last_seen is None


def test_event_has_received_at():
    """Test that events have received_at distinct from timestamp."""
    from datetime import datetime, timezone, timedelta

    event_time = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
    receive_time = datetime(2026, 7, 19, 10, 0, 5, tzinfo=timezone.utc)

    event = Event(
        event_type="order.created",
        timestamp=event_time,
        received_at=receive_time,
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    )

    # timestamp and received_at should be distinct
    assert event.timestamp == event_time
    assert event.received_at == receive_time
    assert event.timestamp != event.received_at

    # Both should be included in serialization
    json_dict = event.to_json_dict()
    assert json_dict["timestamp"] == "2026-07-19T10:00:00+00:00"
    assert json_dict["received_at"] == "2026-07-19T10:00:05+00:00"


def test_event_received_at_defaults_to_now():
    """Test that received_at defaults to current time if not provided."""
    from datetime import datetime, timezone

    before = datetime.now(timezone.utc)
    event = Event(
        event_type="order.created",
        source="order-service",
        data={},
    )
    after = datetime.now(timezone.utc)

    # received_at should be set automatically
    assert event.received_at is not None
    assert before <= event.received_at <= after


def test_event_validation_preserves_received_at():
    """Test that JSON validation preserves received_at."""
    import json

    json_data = {
        "event_id": "evt_received",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:00:00Z",
        "received_at": "2026-07-19T10:00:05Z",
        "source": "order-service",
        "correlation": {"order_id": "8472"},
        "data": {},
    }

    event = Event.model_validate_json(json.dumps(json_data))

    assert event.received_at is not None
    assert event.received_at.isoformat().startswith("2026-07-19T10:00:05")


def test_workflow_instance_out_of_order_events_by_timestamp_not_received_at():
    """Test that workflow timeline sorts by event.timestamp, not received_at.
    
    Events may arrive out of order in distributed systems.
    The workflow timeline should reflect when events happened,
    not when EventAgent observed them.
    
    Scenario:
        Event A: timestamp=10:00:00, received_at=10:00:05 (arrived late)
        Event B: timestamp=10:00:01, received_at=10:00:02 (arrived on time)
        Event C: timestamp=10:00:02, received_at=10:00:01 (arrived early)
    
    Correct timeline (sorted by timestamp):
        Event A @ 10:00:00
        Event B @ 10:00:01
        Event C @ 10:00:02
    
    NOT by received_at:
        Event C @ received 10:00:01
        Event B @ received 10:00:02
        Event A @ received 10:00:05
    """
    from datetime import datetime, timezone, timedelta
    from eventagent.models import WorkflowInstance

    t1 = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)   # Event A happened
    t2 = datetime(2026, 7, 19, 10, 0, 1, tzinfo=timezone.utc)   # Event B happened
    t3 = datetime(2026, 7, 19, 10, 0, 2, tzinfo=timezone.utc)   # Event C happened

    r1 = datetime(2026, 7, 19, 10, 0, 5, tzinfo=timezone.utc)   # Event A received (late)
    r2 = datetime(2026, 7, 19, 10, 0, 2, tzinfo=timezone.utc)   # Event B received
    r3 = datetime(2026, 7, 19, 10, 0, 1, tzinfo=timezone.utc)   # Event C received (early)

    events = [
        Event(
            event_id="evt_A",
            event_type="order.created",
            timestamp=t1,
            received_at=r1,
            source="order-service",
            correlation=Correlation(order_id="8472"),
            data={},
        ),
        Event(
            event_id="evt_B",
            event_type="payment.initiated",
            timestamp=t2,
            received_at=r2,
            source="payment-service",
            correlation=Correlation(order_id="8472"),
            data={},
        ),
        Event(
            event_id="evt_C",
            event_type="payment.succeeded",
            timestamp=t3,
            received_at=r3,
            source="payment-service",
            correlation=Correlation(order_id="8472"),
            data={},
        ),
    ]

    instance = WorkflowInstance.from_events(events, correlation_key="order_id")

    # Timeline should be sorted by timestamp (event time), not received_at
    assert instance.events[0].event_id == "evt_A"  # timestamp 10:00:00
    assert instance.events[1].event_id == "evt_B"  # timestamp 10:00:01
    assert instance.events[2].event_id == "evt_C"  # timestamp 10:00:02

    # first_seen/last_seen are based on event timestamps
    assert instance.first_seen == t1
    assert instance.last_seen == t3

    # Verify received_at values are preserved on each event
    assert instance.events[0].received_at == r1
    assert instance.events[1].received_at == r2
    assert instance.events[2].received_at == r3


def test_payment_succeeded_event():
    """Test PaymentSucceededEvent model."""
    from eventagent.models import PaymentSucceededEvent
    
    event = PaymentSucceededEvent(
        order_id="8472",
        payment_id="pay_123",
        amount=1000.0,
        transaction_id="txn_456",
    )
    
    assert event.order_id == "8472"
    assert event.payment_id == "pay_123"
    assert event.amount == 1000.0
    assert event.transaction_id == "txn_456"


def test_payment_succeeded_event_without_transaction():
    """Test PaymentSucceededEvent without optional transaction_id."""
    from eventagent.models import PaymentSucceededEvent
    
    event = PaymentSucceededEvent(
        order_id="8472",
        payment_id="pay_123",
        amount=500.0,
    )
    
    assert event.order_id == "8472"
    assert event.payment_id == "pay_123"
    assert event.amount == 500.0
    assert event.transaction_id is None


def test_standard_event_envelope():
    """Test that Event follows the standard envelope structure.
    
    The expected structure:
    {
        "event_id": "evt_123",
        "event_type": "order.created",
        "timestamp": "...",
        "received_at": "...",
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
    assert "received_at" in json_dict
    assert "source" in json_dict
    assert "correlation" in json_dict
    assert "data" in json_dict
    
    # Verify field types
    assert isinstance(json_dict["event_id"], str)
    assert json_dict["event_id"].startswith("evt_")
    assert isinstance(json_dict["event_type"], str)
    assert isinstance(json_dict["timestamp"], str)
    assert isinstance(json_dict["received_at"], str)
    assert isinstance(json_dict["source"], str)
    assert isinstance(json_dict["correlation"], dict)
    assert isinstance(json_dict["data"], dict)


def test_uncorrelated_event_creation():
    """Test UncorrelatedEvent model creation."""
    event = UncorrelatedEvent(
        event_id="evt_123",
        event_type="payment.failed",
        source="payment-service",
        correlation_data={},
        data={"error_code": "insufficient_funds"},
        reason="Missing required correlation key: order_id",
    )
    
    assert event.event_id == "evt_123"
    assert event.event_type == "payment.failed"
    assert event.source == "payment-service"
    assert event.reason == "Missing required correlation key: order_id"
    assert event.resolved is False


def test_uncorrelated_event_to_json():
    """Test UncorrelatedEvent serialization."""
    event = UncorrelatedEvent(
        event_id="evt_123",
        event_type="payment.failed",
        source="payment-service",
        correlation_data={},
        data={"error": "failed"},
        reason="Missing required correlation key: order_id",
    )
    
    json_dict = event.to_json_dict()
    
    assert json_dict["event_id"] == "evt_123"
    assert json_dict["event_type"] == "payment.failed"
    assert json_dict["source"] == "payment-service"
    assert json_dict["reason"] == "Missing required correlation key: order_id"
    assert json_dict["resolved"] is False
    assert "timestamp" in json_dict
    assert "received_at" in json_dict


def test_uncorrelated_event_resolved_status():
    """Test UncorrelatedEvent resolved status can be changed."""
    event = UncorrelatedEvent(
        event_id="evt_123",
        event_type="payment.failed",
        source="payment-service",
        correlation_data={},
        data={},
        reason="Missing required correlation key: order_id",
        resolved=False,
    )
    
    assert event.resolved is False
    
    # Mark as resolved
    event.resolved = True
    assert event.resolved is True