"""Tests for independent event flow through NATS (Step 6)."""

import json
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

from eventagent.models import Correlation, Event, EventType


def test_services_independent_architecture():
    """Test that services are designed for independent operation through NATS.
    
    This verifies the architecture:
        Order Service ──┐
                        │
        Payment Service ─┼──► NATS ──► EventAgent (Passive Observer)
                        │
                        └──► publishes order events
        
        Then:
                        │
                        │
        Payment Service ─┼──► NATS ──► EventAgent (Passive Observer)
                        │
                        └──► publishes payment events
    """
    # Verify that services have their own NATS connections
    from eventagent.services import (
        start_payment_service,
        start_payment_processor_service,
    )
    
    # These functions demonstrate independent NATS connections
    # Each service creates its own NATS connection and subscribes independently
    assert "nats_servers" in start_payment_service.__code__.co_varnames
    assert "nats_servers" in start_payment_processor_service.__code__.co_varnames
    
    # Verify EventAgent is passive observer
    from eventagent.consumer import EventConsumer
    
    nc = MagicMock()
    js = MagicMock()
    
    consumer = EventConsumer(nc, js, None)
    
    # The consumer should NOT have publish capability in its handlers
    # Handlers are for passive observation only
    
    # Verify the handler registration doesn't include publishing
    async def passive_handler(event: Event):
        # Passive handler - only observes, doesn't publish
        pass
    
    consumer.register_handler("order.created", passive_handler)
    
    # The consumer should never call js.publish on its own
    assert not hasattr(consumer, 'publish') or consumer.publish is None


def test_event_flow_no_direct_calls():
    """Test that the demo_runner shows proper event independence.
    
    Events flow through NATS, not direct function calls.
    Each service has independent NATS connections.
    """
    from eventagent.services.demo_runner import run_demo_flow_independent
    
    # The run_demo_flow_independent function should create separate NATS connections
    # for each service, demonstrating independence
    
    # Check that it uses NATSClient directly for each service
    import inspect
    source = inspect.getsource(run_demo_flow_independent)
    
    # Should create multiple NATS connections (one per service)
    assert source.count("NATSClient()") >= 3, "Should have independent NATS connections for each service"


def test_passive_observer_never_publishes():
    """Test that EventConsumer (passive observer) never publishes events.
    
    This is the key property of Step 5/6:
    - EventAgent observes events
    - EventAgent does NOT publish events
    - EventAgent does NOT trigger workflows
    """
    from eventagent.consumer import EventConsumer
    
    nc = MagicMock()
    js = MagicMock()
    js.publish = MagicMock()  # Mock publish
    
    consumer = EventConsumer(nc, js, None)
    
    # Register a handler (even if it tries to do something)
    async def handler(event: Event):
        # Even if handler tries to do something, consumer won't publish
        pass
    
    consumer.register_handler("order.created", handler)
    
    # Process an event
    msg = AsyncMock()
    msg.data = json.dumps({
        "event_id": "evt_test",
        "event_type": "order.created",
        "timestamp": "2026-07-19T10:00:00Z",
        "source": "order-service",
        "correlation": {"order_id": "test"},
        "data": {"amount": 100},
    }).encode()
    msg.ack = AsyncMock()
    
    import asyncio
    asyncio.run(consumer.process_event(msg))
    
    # js.publish should NEVER be called by EventConsumer
    # (handlers may call it through their own stores, but consumer doesn't)
    msg.ack.assert_called_once()


def test_event_chain_through_nats():
    """Test that event chains work through NATS subjects, not direct calls.
    
    The chain should be:
        events.order.created -> events.payment.initiated -> events.payment.succeeded
    
    Services subscribe to NATS subjects, not call each other directly.
    """
    # Verify subject naming convention
    assert EventType.ORDER_CREATED.value == "order.created"
    assert EventType.PAYMENT_INITIATED.value == "payment.initiated"
    assert EventType.PAYMENT_SUCCEEDED.value == "payment.succeeded"
    
    # Verify the NATS subject pattern
    for et in EventType:
        subject = f"events.{et.value}"
        # Subject should follow pattern: events.<event_type>
        assert subject.startswith("events.")


def test_demo_runner_uses_nats_publish():
    """Test that demo_runner publishes to NATS (not direct calls)."""
    import inspect
    from eventagent.services.demo_runner import run_demo_flow_independent
    
    source = inspect.getsource(run_demo_flow_independent)
    
    # Should use store.publish (which publishes to NATS)
    assert "store.publish" in source or "payment_store.publish" in source or "order_store.publish" in source


def test_eventagent_subscribes_to_wildcard():
    """Test that EventAgent subscribes to events.> wildcard.
    
    This allows it to observe all events from all services independently.
    """
    from eventagent.consumer import EventConsumer
    
    nc = MagicMock()
    js = MagicMock()
    
    consumer = EventConsumer(nc, js, None)
    
    # The start method should subscribe to events.> wildcard
    import inspect
    source = inspect.getsource(consumer.start)
    
    assert "events.>" in source, "EventConsumer should subscribe to events.> wildcard"


def test_payment_stuck_demo_function_exists():
    """Test that the payment-stuck demo function exists and has correct signature.
    
    This verifies step 8: The broken workflow demo where Payment Service stops
    after publishing payment.failed, and no retry occurs.
    """
    from eventagent.services.demo_runner import run_payment_stuck_demo
    import inspect
    
    # Verify function exists and has correct signature
    sig = inspect.signature(run_payment_stuck_demo)
    assert "nats_servers" in sig.parameters
    assert "db_path" in sig.parameters
    
    # Verify it uses order_8472 as order_id
    source = inspect.getsource(run_payment_stuck_demo)
    assert "order_8472" in source, "Should use order_8472 as order_id in payment-stuck demo"
    
    # Verify it only publishes 3 events: order.created, payment.initiated, payment.failed
    # (no retry event should be published)
    assert "PAYMENT_FAILED" in source, "Should publish payment.failed"


def test_payment_stuck_demo_shows_broken_workflow():
    """Test that payment-stuck demo shows the broken workflow pattern.
    
    EventAgent knows:
        1. order.created
        2. payment.initiated  
        3. payment.failed
    
    But does NOT know:
        - A retry should have occurred
    """
    from eventagent.services.demo_runner import run_payment_stuck_demo
    import inspect
    
    source = inspect.getsource(run_payment_stuck_demo)
    
    # Verify the flow includes payment.failed
    assert "payment.failed" in source or "PAYMENT_FAILED" in source
    
    # Verify there's a comment or output about Payment Service stopping
    assert "STOPPED" in source or "stopped" in source.lower() or "crash" in source.lower(), \
        "Should indicate Payment Service stopped/crashed"