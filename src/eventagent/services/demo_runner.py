"""Demo runner for showcasing event flow through NATS."""

import asyncio
from typing import Any

from nats.aio.client import Client as NATSClient
from nats.js.api import ConsumerConfig, StreamConfig

from ..models import Correlation, Event, EventType
from ..store import NATSEventStore


async def run_demo_flow_independent(nats_servers: list[str], payment_result: str = "success", db_path: str | None = None) -> None:
    """Run a complete demo showing event flow.
    
    FLOW:
        1. Payment Service subscribes to order.created
        2. EventAgent starts and subscribes to events.>
        3. Order Service publishes order.created to NATS
        4. Payment Service (subscribed) receives it and publishes payment.initiated to NATS
        5. Payment Service (same connection) publishes payment.succeeded/failed
        6. EventAgent (subscribed to events.>) observes ALL events
        
    All communication happens through NATS - NO direct function calls between services!
    """
    from ..consumer import EventConsumer
    from ..storage import SQLiteEventStore, get_storage
    
    # Storage for observing events
    storage = SQLiteEventStore(db_path) if db_path else get_storage()
    
    # ============================================================
    # STEP 1: Set up Payment Service subscriber (subscribes to order.created)
    # ============================================================
    print("\n[bold green]═══ Payment Service Subscriber Started ═══[/bold green]")
    
    payment_nc = NATSClient()
    await payment_nc.connect(servers=nats_servers)
    payment_js = payment_nc.jetstream()
    
    payment_store = NATSEventStore(payment_nc, payment_js)
    await payment_store.initialize()
    
    # Track events for sequencing
    events_received: list[str] = []
    order_id_for_payment: str | None = None
    
    # This demonstrates: Payment Service subscribes INDEPENDENTLY to NATS
    async def payment_service_handler(msg):
        """Handler that runs in Payment Service process - reacts to order.created."""
        nonlocal order_id_for_payment
        
        data = msg.data.decode()
        event = Event.model_validate_json(data)
        
        print(f"[PaymentService] Received {event.event_type}")
        events_received.append(f"payment-service received: {event.event_type}")
        
        # Extract order_id from correlation
        if isinstance(event.correlation, Correlation):
            order_id_for_payment = event.correlation.order_id
        elif isinstance(event.correlation, dict):
            order_id_for_payment = event.correlation.get("order_id")
        
        if order_id_for_payment:
            # Publish payment.initiated to NATS (INDEPENDENTLY!)
            payment_event = Event(
                event_type=EventType.PAYMENT_INITIATED.value,
                source="payment-service",
                correlation=Correlation(order_id=order_id_for_payment),
                data={"amount": event.data.get("amount", 0.0)},
            )
            
            subject = await payment_store.publish(payment_event)
            print(f"[PaymentService] Published {payment_event.event_type}")
            
            # Publish payment result to NATS
            if payment_result == "success":
                result_event = Event(
                    event_type=EventType.PAYMENT_SUCCEEDED.value,
                    source="payment-service",
                    correlation=Correlation(order_id=order_id_for_payment),
                    data={
                        "order_id": order_id_for_payment,
                        "amount": event.data.get("amount", 0.0),
                        "transaction_id": f"txn_{order_id_for_payment}",
                    },
                )
                result_type = "succeeded"
            else:
                result_event = Event(
                    event_type=EventType.PAYMENT_FAILED.value,
                    source="payment-service",
                    correlation=Correlation(order_id=order_id_for_payment),
                    data={
                        "order_id": order_id_for_payment,
                        "error_code": "insufficient_funds",
                        "error_message": "Payment declined",
                    },
                )
                result_type = "failed"
            
            subject = await payment_store.publish(result_event)
            print(f"[PaymentService] Published payment.{result_type}")
        
        await msg.ack()
    
    # Subscribe to order.created
    order_created_subject = f"events.{EventType.ORDER_CREATED.value}"
    await payment_js.add_consumer(
        "EVENTS",
        ConsumerConfig(
            name="payment-service-consumer",
            filter_subjects=[order_created_subject],
        ),
    )
    await payment_js.subscribe(order_created_subject, cb=payment_service_handler, durable="payment-service-consumer")
    
    # ============================================================
    # STEP 2: Set up EventAgent consumer (passive observer)
    # ============================================================
    print("\n[bold magenta]═══ EventAgent Consumer Started ═══[/bold magenta]")
    
    # EventAgent has its OWN NATS connection
    agent_nc = NATSClient()
    await agent_nc.connect(servers=nats_servers)
    agent_js = agent_nc.jetstream()
    
    agent = EventConsumer(agent_nc, agent_js, storage)
    
    # Passive handlers - just log/observe, DO NOT trigger workflows
    async def observe_order(event: Event):
        """Handler that stores order events."""
        print(f"[EventAgent] Stored {event.event_type}")
    
    async def observe_payment(event: Event):
        """Handler that stores payment events."""
        print(f"[EventAgent] Stored {event.event_type}")
    
    agent.register_handler(EventType.ORDER_CREATED.value, observe_order)
    agent.register_handler(EventType.PAYMENT_INITIATED.value, observe_payment)
    agent.register_handler(EventType.PAYMENT_SUCCEEDED.value, observe_payment)
    agent.register_handler(EventType.PAYMENT_FAILED.value, observe_payment)
    
    await agent.start()
    
    # Give time for EventAgent to be ready
    await asyncio.sleep(0.5)
    
    # ============================================================
    # STEP 3: Order Service publishes order.created
    # ============================================================
    print("\n[bold cyan]═══ Order Service Publisher ═══[/bold cyan]")
    
    order_nc = NATSClient()
    await order_nc.connect(servers=nats_servers)
    order_js = order_nc.jetstream()
    
    order_store = NATSEventStore(order_nc, order_js)
    await order_store.initialize()
    
    # Create and publish order.created event
    order_event = Event(
        event_type=EventType.ORDER_CREATED.value,
        source="order-service",
        correlation=Correlation(order_id="8472"),
        data={"amount": 1000.0, "currency": "USD"},
    )
    
    subject = await order_store.publish(order_event)
    print(f"[OrderService] Published order.created")
    
    # Give NATS time to deliver all events through the flow
    await asyncio.sleep(1.0)
    
    # ============================================================
    # Summary
    # ============================================================
    print("\n[bold blue]═══ DEMO SUMMARY ═══[/bold blue]")
    print(f"Total events in storage: {len(storage.get_events())}")
    for event in storage.get_events():
        print(f"  - {event['event_type']}")
    
    # Cleanup
    await agent.stop()
    await order_nc.close()
    await payment_nc.close()
    await agent_nc.close()
    storage.close()


async def run_payment_stuck_demo(nats_servers: list[str], db_path: str | None = None) -> None:
    """Run the payment-stuck demo showing a broken workflow.
    
    FLOW:
        1. Payment Service subscribes to order.created
        2. EventAgent starts and subscribes to events.>
        3. Order Service publishes order.created to NATS
        4. Payment Service receives it and publishes payment.initiated to NATS
        5. Payment Service publishes payment.failed to NATS
        6. ❌ Payment Service STOPS (simulating a crash/failure)
        7. EventAgent only observes the 3 events - NO retry occurred
        
    This demonstrates that EventAgent can detect the broken workflow:
    - It stores: order.created, payment.initiated, payment.failed
    - It knows these events occurred
    - But it does NOT know a retry should have occurred
    - The next milestone is to detect this anomaly
    
    All communication happens through NATS - NO direct function calls between services!
    """
    from ..consumer import EventConsumer
    from ..storage import SQLiteEventStore, get_storage
    
    # Storage for observing events
    storage = SQLiteEventStore(db_path) if db_path else get_storage()
    
    # ============================================================
    # STEP 1: Set up Payment Service subscriber (subscribes to order.created)
    # ============================================================
    print("\n[bold green]═══ Payment Service Subscriber Started ═══[/bold green]")
    
    payment_nc = NATSClient()
    await payment_nc.connect(servers=nats_servers)
    payment_js = payment_nc.jetstream()
    
    payment_store = NATSEventStore(payment_nc, payment_js)
    await payment_store.initialize()
    
    # Track events for sequencing
    order_id_for_payment: str | None = None
    
    # Payment Service handler that STOPS after publishing payment.failed
    async def payment_service_handler(msg):
        """Handler that runs in Payment Service process - reacts to order.created."""
        nonlocal order_id_for_payment
        
        data = msg.data.decode()
        event = Event.model_validate_json(data)
        
        print(f"[PaymentService] Received {event.event_type}")
        
        # Extract order_id from correlation
        if isinstance(event.correlation, Correlation):
            order_id_for_payment = event.correlation.order_id
        elif isinstance(event.correlation, dict):
            order_id_for_payment = event.correlation.get("order_id")
        
        if order_id_for_payment:
            # Publish payment.initiated to NATS (INDEPENDENTLY!)
            payment_event = Event(
                event_type=EventType.PAYMENT_INITIATED.value,
                source="payment-service",
                correlation=Correlation(order_id=order_id_for_payment),
                data={"amount": event.data.get("amount", 0.0)},
            )
            
            subject = await payment_store.publish(payment_event)
            print(f"[PaymentService] Published {payment_event.event_type}")
            
            # Publish payment.failed to NATS
            result_event = Event(
                event_type=EventType.PAYMENT_FAILED.value,
                source="payment-service",
                correlation=Correlation(order_id=order_id_for_payment),
                data={
                    "order_id": order_id_for_payment,
                    "error_code": "insufficient_funds",
                    "error_message": "Payment declined",
                },
            )
            
            subject = await payment_store.publish(result_event)
            print(f"[PaymentService] Published payment.failed")
            
            # ❌ SIMULATE SERVICE CRASH - Stop processing after payment.failed
            print("[PaymentService] ❌ Payment Service STOPPED (simulating crash)")
        
        await msg.ack()
    
    # Subscribe to order.created
    order_created_subject = f"events.{EventType.ORDER_CREATED.value}"
    await payment_js.add_consumer(
        "EVENTS",
        ConsumerConfig(
            name="payment-service-consumer-stuck",
            filter_subjects=[order_created_subject],
        ),
    )
    await payment_js.subscribe(order_created_subject, cb=payment_service_handler, durable="payment-service-consumer-stuck")
    
    # ============================================================
    # STEP 2: Set up EventAgent consumer (passive observer)
    # ============================================================
    print("\n[bold magenta]═══ EventAgent Consumer Started ═══[/bold magenta]")
    
    # EventAgent has its OWN NATS connection
    agent_nc = NATSClient()
    await agent_nc.connect(servers=nats_servers)
    agent_js = agent_nc.jetstream()
    
    agent = EventConsumer(agent_nc, agent_js, storage)
    
    # Passive handlers - just log/observe, DO NOT trigger workflows
    async def observe_order(event: Event):
        """Handler that stores order events."""
        print(f"[EventAgent] Stored {event.event_type}")
    
    async def observe_payment(event: Event):
        """Handler that stores payment events."""
        print(f"[EventAgent] Stored {event.event_type}")
    
    agent.register_handler(EventType.ORDER_CREATED.value, observe_order)
    agent.register_handler(EventType.PAYMENT_INITIATED.value, observe_payment)
    agent.register_handler(EventType.PAYMENT_SUCCEEDED.value, observe_payment)
    agent.register_handler(EventType.PAYMENT_FAILED.value, observe_payment)
    agent.register_handler(EventType.PAYMENT_RETRY_SCHEDULED.value, observe_payment)
    
    await agent.start()
    
    # Give time for EventAgent to be ready
    await asyncio.sleep(0.5)
    
    # ============================================================
    # STEP 3: Order Service publishes order.created
    # ============================================================
    print("\n[bold cyan]═══ Order Service Publisher ══╗[/bold cyan]")
    
    order_nc = NATSClient()
    await order_nc.connect(servers=nats_servers)
    order_js = order_nc.jetstream()
    
    order_store = NATSEventStore(order_nc, order_js)
    await order_store.initialize()
    
    # Create and publish order.created event with order_8472
    order_event = Event(
        event_type=EventType.ORDER_CREATED.value,
        source="order-service",
        correlation=Correlation(order_id="order_8472"),
        data={"amount": 1000.0, "currency": "USD"},
    )
    
    subject = await order_store.publish(order_event)
    print(f"[OrderService] Published order.created (order_id=order_8472)")
    
    # Give NATS time to deliver all events through the flow
    await asyncio.sleep(1.0)
    
    # ============================================================
    # Summary - EventAgent knows what happened, but not what SHOULD have happened
    # ============================================================
    print("\n[bold blue]═══ DEMO SUMMARY ═══[/bold blue]")
    print(f"Total events in storage: {len(storage.get_events())}")
    for event in storage.get_events():
        print(f"  - {event['event_type']}")
    
    print("\n[bold yellow]═══ WORKFLOW ANALYSIS ═══[/bold yellow]")
    print("EventAgent knows these events occurred:")
    for event in storage.get_events():
        print(f"  ✓ {event['event_type']}")
    
    print("\nBut EventAgent does NOT know:")
    print("  ✗ A retry should have occurred after payment.failed")
    print("  ✗ The Payment Service stopped unexpectedly")
    print("\nNext milestone: Detect this broken workflow!")
    
    # Cleanup
    await agent.stop()
    await order_nc.close()
    await payment_nc.close()
    await agent_nc.close()
    storage.close()


# Keep old function for backward compatibility
async def run_demo_flow(nats_servers: list[str], payment_result: str = "success", db_path: str | None = None) -> None:
    """Legacy function - redirects to run_demo_flow_independent."""
    await run_demo_flow_independent(nats_servers, payment_result, db_path)