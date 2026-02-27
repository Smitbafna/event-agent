"""Tests for workflow query methods."""

import tempfile
from datetime import datetime, timezone

from eventagent.models import Correlation, Event
from eventagent.storage import SQLiteEventStore


def test_get_workflow_summary():
    """Test getting workflow summary with event count."""
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))

    from eventagent.correlation import CorrelationEngine
    engine = CorrelationEngine()

    # Create and store events for order_8472
    for et, ts in [
        ("order.created", datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)),
        ("payment.initiated", datetime(2026, 7, 19, 10, 0, 5, tzinfo=timezone.utc)),
        ("payment.failed", datetime(2026, 7, 19, 10, 0, 10, tzinfo=timezone.utc)),
    ]:
        event = Event(
            event_type=et,
            timestamp=ts,
            source="test-service",
            correlation=Correlation(order_id="8472"),
            data={"amount": 1000},
        )
        storage.store_event_and_correlate(event, engine.process_event(event))

    summary = storage.get_workflow_summary("order_8472")
    assert summary is not None
    assert summary["workflow_id"] == "order_8472"
    assert summary["event_count"] == 3
    assert summary["last_event_type"] == "payment.failed"

    storage.close()


def test_get_all_workflow_summaries():
    """Test listing all workflows with summaries."""
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))

    from eventagent.correlation import CorrelationEngine
    engine = CorrelationEngine()

    # Workflow 8472 - 3 events
    for et, ts in [
        ("order.created", datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc)),
        ("payment.initiated", datetime(2026, 7, 19, 10, 0, 5, tzinfo=timezone.utc)),
        ("payment.failed", datetime(2026, 7, 19, 10, 0, 10, tzinfo=timezone.utc)),
    ]:
        event = Event(
            event_type=et,
            timestamp=ts,
            source="test-service",
            correlation=Correlation(order_id="8472"),
            data={},
        )
        storage.store_event_and_correlate(event, engine.process_event(event))

    # Workflow 9001 - 1 event
    event = Event(
        event_type="order.created",
        timestamp=datetime(2026, 7, 19, 10, 5, 0, tzinfo=timezone.utc),
        source="test-service",
        correlation=Correlation(order_id="9001"),
        data={},
    )
    storage.store_event_and_correlate(event, engine.process_event(event))

    summaries = storage.get_all_workflow_summaries()
    assert len(summaries) == 2

    wf_8472 = next((w for w in summaries if w["workflow_id"] == "order_8472"), None)
    wf_9001 = next((w for w in summaries if w["workflow_id"] == "order_9001"), None)

    assert wf_8472 is not None
    assert wf_8472["event_count"] == 3
    assert wf_8472["last_event_type"] == "payment.failed"

    assert wf_9001 is not None
    assert wf_9001["event_count"] == 1
    assert wf_9001["last_event_type"] == "order.created"

    storage.close()


def test_workflow_summary_not_found():
    """Test workflow summary for non-existent workflow."""
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))

    summary = storage.get_workflow_summary("nonexistent_workflow")
    assert summary is None

    storage.close()


def test_workflow_events_query():
    """Test getting all events for a workflow."""
    storage = SQLiteEventStore(tempfile.mktemp(suffix=".db"))

    from eventagent.correlation import CorrelationEngine
    engine = CorrelationEngine()

    events = [
        Event(
            event_type="order.created",
            timestamp=datetime(2026, 7, 19, 10, 0, 0, tzinfo=timezone.utc),
            source="order-service",
            correlation=Correlation(order_id="8472"),
            data={"amount": 1000},
        ),
        Event(
            event_type="payment.initiated",
            timestamp=datetime(2026, 7, 19, 10, 0, 5, tzinfo=timezone.utc),
            source="payment-service",
            correlation=Correlation(order_id="8472"),
            data={"amount": 1000},
        ),
    ]

    for event in events:
        storage.store_event_and_correlate(event, engine.process_event(event))

    workflow_events = storage.get_workflow_events("order_8472")
    assert len(workflow_events) == 2
    assert workflow_events[0]["event_type"] == "order.created"
    assert workflow_events[1]["event_type"] == "payment.initiated"

    storage.close()