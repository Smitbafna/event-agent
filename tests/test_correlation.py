"""Tests for the CorrelationEngine."""

from datetime import datetime, timezone

from eventagent.models import Correlation, Event, UncorrelatedEvent, WorkflowInstance
from eventagent.correlation import CorrelationEngine


def test_process_event_creates_new_workflow():
    """Test that processing an event creates a new workflow instance."""
    engine = CorrelationEngine()
    event = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    )

    instance = engine.process_event(event)

    assert instance.workflow_id == "order_8472"
    assert instance.workflow_type == "order"
    assert instance.correlation_key == "order_id"
    assert instance.correlation_value == "8472"
    assert len(instance.events) == 1
    assert instance.events[0] is event


def test_process_event_attaches_to_existing_workflow():
    """Test that related events are grouped into the same workflow."""
    engine = CorrelationEngine()
    event1 = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    )
    event2 = Event(
        event_type="payment.initiated",
        source="payment-service",
        correlation=Correlation(order_id="8472", payment_id="pay_123"),
        data={"amount": 1000},
    )

    engine.process_event(event1)
    instance = engine.process_event(event2)

    assert instance.workflow_id == "order_8472"
    assert len(instance.events) == 2
    assert instance.events[0] is event1
    assert instance.events[1] is event2


def test_different_correlation_values_are_separate_workflows():
    """Test that events with different correlation values never mix."""
    engine = CorrelationEngine()

    event1 = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    )
    event2 = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="9001"),
        data={"amount": 500},
    )

    instance1 = engine.process_event(event1)
    instance2 = engine.process_event(event2)

    # Should be two separate workflows
    assert instance1.workflow_id == "order_8472"
    assert instance2.workflow_id == "order_9001"
    assert instance1 is not instance2
    assert len(engine.get_all_workflows()) == 2

    # Verify get_workflow returns correct instances
    assert engine.get_workflow("order_id", "8472") is instance1
    assert engine.get_workflow("order_id", "9001") is instance2
    assert engine.get_workflow("order_id", "9999") is None


def test_process_event_with_dict_correlation():
    """Test that events with dict correlation work with the engine."""
    engine = CorrelationEngine()
    event = Event(
        event_type="order.created",
        source="order-service",
        correlation={"order_id": "5555", "custom_key": "custom_value"},
        data={"amount": 1000},
    )

    instance = engine.process_event(event)

    assert instance.workflow_id == "order_5555"
    assert instance.correlation_value == "5555"
    assert len(instance.events) == 1


def test_process_event_without_correlation_returns_uncorrelatable():
    """Test that events without correlation data are stored as UncorrelatedEvent."""
    engine = CorrelationEngine()
    event = Event(
        event_type="payment.failed",
        source="payment-service",
        correlation={},
        data={"error_code": "insufficient_funds"},
    )

    result = engine.process_event(event)

    # Should return an UncorrelatedEvent, not raise an exception
    assert isinstance(result, UncorrelatedEvent)
    assert result.event_type == "payment.failed"
    assert result.source == "payment-service"
    # model_dump() filters None values, so empty correlation returns empty dict
    assert result.reason == "Missing required correlation data (empty correlation object)"
    assert result.resolved is False
    
    # Verify it's tracked in the uncorrelated list
    assert engine.uncorrelated_count == 1
    uncorrelatables = engine.get_uncorrelated_events()
    assert len(uncorrelatables) == 1
    assert uncorrelatables[0].event_id == event.event_id


def test_process_event_with_none_order_id_returns_uncorrelatable():
    """Test that events with explicit None order_id are stored as UncorrelatedEvent.
    
    Note: Correlation.model_dump() filters out None values, so Correlation(order_id=None)
    behaves the same as Correlation() - both result in empty correlation_data.
    """
    engine = CorrelationEngine()
    event = Event(
        event_type="payment.failed",
        source="payment-service",
        correlation=Correlation(order_id=None),  # order_id is explicitly None
        data={"error_code": "insufficient_funds"},
    )

    result = engine.process_event(event)

    # Should return an UncorrelatedEvent with appropriate reason
    # model_dump() filters None values, so this is treated as empty correlation
    assert isinstance(result, UncorrelatedEvent)
    assert result.reason == "Missing required correlation data (empty correlation object)"
    assert engine.uncorrelated_count == 1


def test_process_event_with_fallback_correlation_key():
    """Test that events without the configured key fall back to any available key."""
    engine = CorrelationEngine()
    event = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(customer_id="123"),  # order_id is None, falls back to customer_id
        data={"amount": 1000},
    )

    instance = engine.process_event(event)

    assert instance.correlation_key == "customer_id"
    assert instance.correlation_value == "123"
    assert len(instance.events) == 1


def test_process_event_with_all_none_correlation_returns_uncorrelatable():
    """Test that events with all-None correlation fields are stored as UncorrelatedEvent."""
    engine = CorrelationEngine()
    event = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(),  # all fields None -> empty model_dump
        data={"amount": 1000},
    )

    result = engine.process_event(event)

    # Should return an UncorrelatedEvent, not raise an exception
    assert isinstance(result, UncorrelatedEvent)
    assert result.reason == "Missing required correlation data (empty correlation object)"
    assert engine.uncorrelated_count == 1


def test_uncorrelatable_events_not_in_workflows():
    """Test that uncorrelatable events are not mixed with valid workflows."""
    engine = CorrelationEngine()
    
    # Process a valid event first
    event1 = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    )
    instance1 = engine.process_event(event1)
    
    # Process an uncorrelatable event
    event2 = Event(
        event_type="payment.failed",
        source="payment-service",
        correlation={},
        data={"error": "failed"},
    )
    uncorrelatable = engine.process_event(event2)
    
    # Verify workflow count is still 1
    assert engine.count == 1
    assert engine.uncorrelated_count == 1
    
    # Verify the uncorrelatable is not in any workflow
    assert isinstance(uncorrelatable, UncorrelatedEvent)
    assert engine.get_workflow("order_id", "8472") is instance1


def test_mark_uncorrelatable_as_resolved():
    """Test marking an uncorrelatable event as resolved and moving it to a workflow."""
    engine = CorrelationEngine()
    
    # Process an uncorrelatable event
    event = Event(
        event_type="payment.failed",
        source="payment-service",
        correlation={},
        data={"error": "failed"},
    )
    uncorrelatable = engine.process_event(event)
    
    assert engine.uncorrelated_count == 1
    
    # Later, resolve it with proper correlation
    result = engine.mark_resolved(uncorrelatable.event_id, "order_id", "12345")
    
    assert result is True
    assert uncorrelatable.resolved is True
    
    # Verify it's now in the workflow
    workflow = engine.get_workflow("order_id", "12345")
    assert workflow is not None
    assert len(workflow.events) == 1
    assert workflow.events[0].event_id == uncorrelatable.event_id


def test_mark_uncorrelatable_nonexistent_returns_false():
    """Test that marking a nonexistent event as resolved returns False."""
    engine = CorrelationEngine()
    
    result = engine.mark_resolved("evt_nonexistent", "order_id", "12345")
    
    assert result is False


def test_clear_removes_uncorrelatable_events():
    """Test that clear removes both workflows and uncorrelated events."""
    engine = CorrelationEngine()
    
    # Add a valid workflow
    event1 = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={},
    )
    engine.process_event(event1)
    
    # Add an uncorrelatable event
    event2 = Event(
        event_type="payment.failed",
        source="payment-service",
        correlation={},
        data={},
    )
    engine.process_event(event2)
    
    assert engine.count == 1
    assert engine.uncorrelated_count == 1
    
    engine.clear()
    
    assert engine.count == 0
    assert engine.uncorrelated_count == 0
    assert engine.get_uncorrelated_events() == []


def test_get_all_workflows_returns_sorted_by_first_seen():
    """Test that get_all_workflows returns instances sorted by first_seen timestamp."""
    engine = CorrelationEngine()

    # Create events with different timestamps
    t1 = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 7, 19, 10, 0, 5, tzinfo=timezone.utc)

    # Process 9001 first with later timestamp
    event2 = Event(
        event_type="order.created",
        timestamp=t2,
        source="order-service",
        correlation=Correlation(order_id="9001"),
        data={},
    )
    engine.process_event(event2)

    # Process 8472 second with earlier timestamp
    event1 = Event(
        event_type="order.created",
        timestamp=t1,
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={},
    )
    engine.process_event(event1)

    workflows = engine.get_all_workflows()
    # Sorted by first_seen (timestamp) ascending
    assert workflows[0].correlation_value == "8472"  # t1 - earlier
    assert workflows[1].correlation_value == "9001"  # t2 - later
    assert len(workflows) == 2


def test_update_timestamps_on_event_attach():
    """Test that first_seen and last_seen are updated correctly."""
    engine = CorrelationEngine()

    t1 = datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 7, 19, 10, 0, 10, tzinfo=timezone.utc)
    t3 = datetime(2026, 7, 19, 10, 0, 5, tzinfo=timezone.utc)  # middle

    event1 = Event(
        event_type="order.created",
        timestamp=t1,
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={},
    )
    event2 = Event(
        event_type="payment.initiated",
        timestamp=t2,
        source="payment-service",
        correlation=Correlation(order_id="8472"),
        data={},
    )
    event3 = Event(
        event_type="payment.failed",
        timestamp=t3,
        source="payment-service",
        correlation=Correlation(order_id="8472"),
        data={},
    )

    instance = engine.process_event(event1)
    assert instance.first_seen == t1
    assert instance.last_seen == t1

    # Later event extends last_seen
    instance = engine.process_event(event2)
    assert instance.first_seen == t1
    assert instance.last_seen == t2

    # Middle event doesn't change first or last
    instance = engine.process_event(event3)
    assert instance.first_seen == t1
    assert instance.last_seen == t2


def test_workflow_count_property():
    """Test the count property reflects number of workflows."""
    engine = CorrelationEngine()
    assert engine.count == 0

    engine.process_event(Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={},
    ))
    assert engine.count == 1

    engine.process_event(Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="9001"),
        data={},
    ))
    assert engine.count == 2


def test_remove_workflow():
    """Test removing a workflow instance."""
    engine = CorrelationEngine()
    engine.process_event(Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={},
    ))
    assert engine.count == 1

    assert engine.remove_workflow("order_id", "8472") is True
    assert engine.count == 0

    # Removing again returns False
    assert engine.remove_workflow("order_id", "8472") is False


def test_max_workflows_eviction():
    """Test that oldest workflows are evicted when max is exceeded."""
    engine = CorrelationEngine(max_workflows=2)

    engine.process_event(Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="1"),
        data={},
    ))
    engine.process_event(Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="2"),
        data={},
    ))
    assert engine.count == 2

    # Adding a third should evict the oldest (id=1)
    engine.process_event(Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="3"),
        data={},
    ))
    assert engine.count == 2
    assert engine.get_workflow("order_id", "1") is None
    assert engine.get_workflow("order_id", "2") is not None
    assert engine.get_workflow("order_id", "3") is not None


def test_three_event_workflow():
    """Test the full example from the requirements: order.created -> payment.initiated -> payment.failed."""
    engine = CorrelationEngine()

    events = [
        Event(
            event_type="order.created",
            source="order-service",
            correlation=Correlation(order_id="8472"),
            data={"amount": 1000},
        ),
        Event(
            event_type="payment.initiated",
            source="payment-service",
            correlation=Correlation(order_id="8472", payment_id="pay_123"),
            data={"amount": 1000},
        ),
        Event(
            event_type="payment.failed",
            source="payment-service",
            correlation=Correlation(order_id="8472", payment_id="pay_123"),
            data={"error_code": "insufficient_funds", "error_message": "Insufficient funds"},
        ),
    ]

    for event in events:
        engine.process_event(event)

    instance = engine.get_workflow("order_id", "8472")
    assert instance is not None
    assert len(instance.events) == 3
    assert [e.event_type for e in instance.events] == [
        "order.created",
        "payment.initiated",
        "payment.failed",
    ]


def test_demo_flow_from_requirement():
    """Reproduce the exact example from the task description.
    
    For order_id=8472:
    
        Workflow 8472
        ├── order.created
        ├── payment.initiated
        └── payment.failed
    
    For order_id=9001:
    
        Workflow 9001
        └── order.created
    
    Never mix order_id=8472 with order_id=9001.
    """
    engine = CorrelationEngine()

    # Workflow for 8472 (3 events)
    engine.process_event(Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    ))
    engine.process_event(Event(
        event_type="payment.initiated",
        source="payment-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000},
    ))
    engine.process_event(Event(
        event_type="payment.failed",
        source="payment-service",
        correlation=Correlation(order_id="8472"),
        data={"error": "card_declined"},
    ))

    # Workflow for 9001 (1 event)
    engine.process_event(Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="9001"),
        data={"amount": 250},
    ))

    # Verify 8472 workflow has 3 events
    workflow_8472 = engine.get_workflow("order_id", "8472")
    assert workflow_8472 is not None
    assert len(workflow_8472.events) == 3
    assert workflow_8472.events[0].event_type == "order.created"
    assert workflow_8472.events[1].event_type == "payment.initiated"
    assert workflow_8472.events[2].event_type == "payment.failed"

    # Verify 9001 workflow has 1 event
    workflow_9001 = engine.get_workflow("order_id", "9001")
    assert workflow_9001 is not None
    assert len(workflow_9001.events) == 1
    assert workflow_9001.events[0].event_type == "order.created"

    # Verify they are different instances and don't mix
    assert workflow_8472 is not workflow_9001
    assert workflow_8472.correlation_value == "8472"
    assert workflow_9001.correlation_value == "9001"

    # Verify no cross-contamination
    assert all(e.correlation.order_id == "8472" for e in workflow_8472.events)
    assert all(e.correlation.order_id == "9001" for e in workflow_9001.events)


def test_workflow_id_is_unique():
    """Test that different correlation keys produce different workflow IDs."""
    engine = CorrelationEngine()

    event = Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="8472", payment_id="pay_456"),
        data={},
    )
    instance = engine.process_event(event)

    # With config order_id being primary, the workflow should use order_id
    assert instance.workflow_id == "order_8472"
    assert instance.correlation_key == "order_id"
    assert instance.correlation_value == "8472"


def test_interleaved_workflows():
    """Test that interleaved events are correctly separated by correlation.
    
    This is the most important test for Milestone 2.
    
    Two orders are processed concurrently with interleaved event arrival:
    
    A: order.created(order_id=A)
    B: order.created(order_id=B)
    B: payment.initiated(order_id=B)
    A: payment.initiated(order_id=A)
    B: payment.succeeded(order_id=B)
    A: payment.failed(order_id=A)
    
    Expected result:
    
    Workflow A
    ├── order.created
    ├── payment.initiated
    └── payment.failed
    
    Workflow B
    ├── order.created
    ├── payment.initiated
    └── payment.succeeded
    
    Despite interleaved arrival, each workflow must contain only its own events.
    """
    engine = CorrelationEngine()

    # Process events in interleaved order
    engine.process_event(Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="A"),
        data={"amount": 500},
    ))

    engine.process_event(Event(
        event_type="order.created",
        source="order-service",
        correlation=Correlation(order_id="B"),
        data={"amount": 750},
    ))

    engine.process_event(Event(
        event_type="payment.initiated",
        source="payment-service",
        correlation=Correlation(order_id="B"),
        data={"amount": 750},
    ))

    engine.process_event(Event(
        event_type="payment.initiated",
        source="payment-service",
        correlation=Correlation(order_id="A"),
        data={"amount": 500},
    ))

    engine.process_event(Event(
        event_type="payment.succeeded",
        source="payment-service",
        correlation=Correlation(order_id="B", payment_id="pay_B"),
        data={"amount": 750},
    ))

    engine.process_event(Event(
        event_type="payment.failed",
        source="payment-service",
        correlation=Correlation(order_id="A", payment_id="pay_A"),
        data={"error": "declined"},
    ))

    # Verify Workflow A
    workflow_a = engine.get_workflow("order_id", "A")
    assert workflow_a is not None
    assert workflow_a.workflow_id == "order_A"
    assert len(workflow_a.events) == 3, "Workflow A should have 3 events"
    assert workflow_a.events[0].event_type == "order.created"
    assert workflow_a.events[1].event_type == "payment.initiated"
    assert workflow_a.events[2].event_type == "payment.failed"

    # Verify no cross-contamination in A
    assert all(e.correlation.order_id == "A" for e in workflow_a.events), \
        "Workflow A should only contain events with order_id=A"

    # Verify Workflow B
    workflow_b = engine.get_workflow("order_id", "B")
    assert workflow_b is not None
    assert workflow_b.workflow_id == "order_B"
    assert len(workflow_b.events) == 3, "Workflow B should have 3 events"
    assert workflow_b.events[0].event_type == "order.created"
    assert workflow_b.events[1].event_type == "payment.initiated"
    assert workflow_b.events[2].event_type == "payment.succeeded"

    # Verify no cross-contamination in B
    assert all(e.correlation.order_id == "B" for e in workflow_b.events), \
        "Workflow B should only contain events with order_id=B"

    # Verify total workflows tracked
    all_workflows = engine.get_all_workflows()
    assert len(all_workflows) == 2