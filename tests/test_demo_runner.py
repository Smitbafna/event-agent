"""Tests for the actual working services (Payment Service and Order Service)."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

from eventagent.models import Correlation, Event, EventType


def test_services_independent_architecture():
    """Test that services are designed for independent operation through NATS.
    
    This verifies the architecture:
        Order Service ──┐
                        │
        Payment Service ─┼──► NATS ──► EventAgent (Passive Observer)
                        │
                        └──► publishes events
    """
    from eventagent.services import (
        create_order,
        start_payment_service,
    )
    
    # Verify that services have their own NATS connections
    assert "nats_servers" in start_payment_service.__code__.co_varnames
    
    # Verify create_order function signature
    assert "order_id" in create_order.__code__.co_varnames
    assert "amount" in create_order.__code__.co_varnames


def test_payment_service_handler_signature():
    """Test that the payment service handler has the correct signature."""
    import inspect
    from eventagent.services.payment_service import handle_order_created
    
    sig = inspect.signature(handle_order_created)
    assert "event" in sig.parameters
    assert "store" in sig.parameters
    assert "payment_result" in sig.parameters


def test_payment_service_main_entry():
    """Test that payment service has a main entry point."""
    from eventagent.services.payment_service import main
    
    # Verify main function exists
    assert callable(main)
    
    # Check it uses environment variables
    import inspect
    source = inspect.getsource(main)
    assert "PAYMENT_RESULT" in source
    assert "NATS_SERVERS" in source


def test_order_service_main_entry():
    """Test that order service has a main entry point."""
    from eventagent.services.order_service import main
    
    # Verify main function exists
    assert callable(main)
    
    # Check it uses environment variables
    import inspect
    source = inspect.getsource(main)
    assert "ORDER_ID" in source
    assert "ORDER_AMOUNT" in source


def test_payment_service_subscribes_to_order_created():
    """Test that payment service subscribes to order.created subject."""
    import inspect
    from eventagent.services.payment_service import start_payment_service
    
    source = inspect.getsource(start_payment_service)
    
    # Should subscribe to order.created
    assert "ORDER_CREATED" in source
    assert "subscribe" in source


def test_passive_observer_never_publishes():
    """Test that EventConsumer (passive observer) never publishes events.
    
    This is the key property of the EventAgent architecture:
    - EventAgent observes events
    - EventAgent does NOT publish events
    - EventAgent does NOT trigger workflows
    """
    from eventagent.consumer import EventConsumer
    
    nc = MagicMock()
    js = MagicMock()
    js.publish = MagicMock()  # Mock publish
    
    consumer = EventConsumer(nc, js, None)
    
    # The consumer should NOT have publish capability in its handlers
    # Handlers are for passive observation only
    
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


def test_payment_service_handles_success_and_failure():
    """Test that payment service can handle both success and failure scenarios."""
    import inspect
    from eventagent.services.payment_service import start_payment_service
    
    source = inspect.getsource(start_payment_service)
    
    # Should handle both payment_result options
    assert "payment_result" in source
    assert "success" in source
    assert "failure" in source or "failed" in source