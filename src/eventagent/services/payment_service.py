"""Payment Service for handling payment events.

NOTE: This is a SEPARATE SERVICE that operates independently.
It publishes payment events to NATS. EventAgent PASSIVELY OBSERVES
these events but does NOT trigger or call this service.

Architecture:
    Order Service ──┐
                    │
    Payment Service ─┼──► NATS ──► EventAgent (Passive Observer)
                    │              observes but doesn't trigger
                    └──► publishes payment events

Payment Service responsibilities:
    - Subscribes to specific events (order.created) to trigger its OWN workflows
    - Publishes payment events (payment.initiated, payment.succeeded, payment.failed)
    - Operates independently of EventAgent
"""

import asyncio
from typing import Any
from uuid import uuid4

from nats.aio.client import Client as NATSClient
from nats.js.api import ConsumerConfig, StreamConfig

from ..models import Correlation, Event, EventType


async def handle_order_created(event: Event, store) -> None:
    """Handle order.created event and publish payment.initiated.
    
    This handler receives order.created events and triggers payment processing
    by publishing a payment.initiated event.
    
    The correlation (order_id) is preserved from the incoming event to the outgoing event.
    
    Args:
        event: The order.created event received from NATS
        store: The event store for publishing the response event
    """
    # Extract order_id from correlation to preserve it
    order_id = None
    if isinstance(event.correlation, Correlation):
        order_id = event.correlation.order_id
    elif isinstance(event.correlation, dict) and "order_id" in event.correlation:
        order_id = event.correlation["order_id"]
    
    if not order_id:
        print(f"[red]Warning: order.created event missing order_id in correlation[/red]")
        return
    
    # Generate a payment_id
    payment_id = f"payment_{uuid4().hex[:8]}"
    
    # Extract amount from event data
    amount = event.data.get("amount", 0.0)
    
    # Create payment.initiated event with preserved correlation
    payment_event = Event(
        event_type=EventType.PAYMENT_INITIATED.value,
        source="payment-service",
        correlation=Correlation(order_id=order_id),
        data={
            "payment_id": payment_id,
            "amount": amount,
        },
    )
    
    subject = await store.publish(payment_event)
    print(f"Published payment.initiated event to {subject}")
    print(f"Event: {payment_event.model_dump_json(indent=2)}")


async def handle_payment_initiated(event: Event, store: Any, payment_result: str = "success") -> None:
    """Handle payment.initiated event and publish payment.succeeded or payment.failed.
    
    This handler receives payment.initiated events and simulates a payment result.
    
    Flow:
        payment.initiated
              ↓
        payment.succeeded (if payment_result == "success")
        OR
        payment.failed (if payment_result == "failure")
    
    Args:
        event: The payment.initiated event received from NATS
        store: The event store for publishing the response event
        payment_result: Either "success" or "failure" to simulate the payment outcome
    """
    # Extract data from the payment.initiated event
    order_id = None
    payment_id = None
    amount = 0.0
    
    if isinstance(event.correlation, Correlation):
        order_id = event.correlation.order_id
    elif isinstance(event.correlation, dict) and "order_id" in event.correlation:
        order_id = event.correlation["order_id"]
    
    if isinstance(event.correlation, Correlation) and event.correlation.payment_id:
        payment_id = event.correlation.payment_id
    elif isinstance(event.correlation, dict) and "payment_id" in event.correlation:
        payment_id = event.correlation["payment_id"]
    
    payment_id = payment_id or event.data.get("payment_id", f"payment_{uuid4().hex[:8]}")
    amount = event.data.get("amount", 0.0)
    
    if payment_result == "success":
        # Publish payment.succeeded event
        transaction_id = f"txn_{uuid4().hex[:8]}"
        
        result_event = Event(
            event_type=EventType.PAYMENT_SUCCEEDED.value,
            source="payment-service",
            correlation=Correlation(order_id=order_id, payment_id=payment_id),
            data={
                "order_id": order_id,
                "payment_id": payment_id,
                "amount": amount,
                "transaction_id": transaction_id,
            },
        )
        
        subject = await store.publish(result_event)
        print(f"[green]Published payment.succeeded event to {subject}[/green]")
        print(f"Event: {result_event.model_dump_json(indent=2)}")
    
    else:
        # Publish payment.failed event
        result_event = Event(
            event_type=EventType.PAYMENT_FAILED.value,
            source="payment-service",
            correlation=Correlation(order_id=order_id, payment_id=payment_id),
            data={
                "order_id": order_id,
                "payment_id": payment_id,
                "error_code": "insufficient_funds",
                "error_message": "Payment declined due to insufficient funds",
            },
        )
        
        subject = await store.publish(result_event)
        print(f"[red]Published payment.failed event to {subject}[/red]")
        print(f"Event: {result_event.model_dump_json(indent=2)}")


async def start_payment_service(nats_servers: list[str] | None = None) -> None:
    """Start the payment service and subscribe to order.created events.
    
    Flow:
        events.order.created
              ↓
        Payment Service receives it
              ↓
        payment.initiated
    
    The correlation (order_id) is preserved across the event chain.
    """
    nc = NATSClient()
    js = nc.jetstream()
    
    connection_servers = nats_servers or ["nats://localhost:4222"]
    await nc.connect(servers=connection_servers)
    
    try:
        # Ensure the events stream exists
        try:
            await js.add_stream(
                StreamConfig(
                    name="EVENTS",
                    subjects=["events.>"],
                )
            )
        except Exception:
            # Stream may already exist, ignore
            pass
        
        # Create a mock store for publishing
        from ..store import NATSEventStore
        store = NATSEventStore(nc, js)
        
        # Subscribe to order.created events
        subject = f"events.{EventType.ORDER_CREATED.value}"
        
        async def message_handler(msg):
            """Process incoming order.created message."""
            try:
                # Decode JSON from NATS message
                data = msg.data.decode()
                
                # Validate Pydantic Event
                event = Event.model_validate_json(data)
                
                # Handle the event
                await handle_order_created(event, store)
                
                # Ack the message
                await msg.ack()
            except Exception as e:
                print(f"[red]Error processing event: {e}[/red]")
                try:
                    await msg.nak()
                except Exception:
                    pass
        
        # Create or update consumer
        await js.add_consumer(
            "EVENTS",
            ConsumerConfig(
                name="eventagent-payment-service",
                filter_subjects=[subject],
            ),
        )
        
        # Subscribe to the subject
        await js.subscribe(
            subject,
            cb=message_handler,
            durable="eventagent-payment-service",
        )
        
        print(f"[green]Payment Service started, listening for order.created events...[/green]")
        
        # Keep the service running
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        print("[yellow]Payment Service shutting down...[/yellow]")
    finally:
        await nc.close()


async def start_payment_processor_service(nats_servers: list[str] | None = None, payment_result: str = "success") -> None:
    """Start the payment processor service that handles payment.initiated events.
    
    Flow:
        events.payment.initiated
              ↓
        Payment Processor receives it
              ↓
        payment.succeeded OR payment.failed (based on payment_result flag)
    
    Args:
        nats_servers: Optional list of NATS server URLs
        payment_result: Either "success" or "failure" to simulate the payment outcome
    """
    nc = NATSClient()
    js = nc.jetstream()
    
    connection_servers = nats_servers or ["nats://localhost:4222"]
    await nc.connect(servers=connection_servers)
    
    try:
        # Ensure the events stream exists
        try:
            await js.add_stream(
                StreamConfig(
                    name="EVENTS",
                    subjects=["events.>"],
                )
            )
        except Exception:
            # Stream may already exist, ignore
            pass
        
        # Create a mock store for publishing
        from ..store import NATSEventStore
        store = NATSEventStore(nc, js)
        
        # Subscribe to payment.initiated events
        subject = f"events.{EventType.PAYMENT_INITIATED.value}"
        
        async def message_handler(msg):
            """Process incoming payment.initiated message."""
            try:
                # Decode JSON from NATS message
                data = msg.data.decode()
                
                # Validate Pydantic Event
                event = Event.model_validate_json(data)
                
                # Handle the event and simulate payment result
                await handle_payment_initiated(event, store, payment_result)
                
                # Ack the message
                await msg.ack()
            except Exception as e:
                print(f"[red]Error processing event: {e}[/red]")
                try:
                    await msg.nak()
                except Exception:
                    pass
        
        # Create or update consumer
        await js.add_consumer(
            "EVENTS",
            ConsumerConfig(
                name="eventagent-payment-processor",
                filter_subjects=[subject],
            ),
        )
        
        # Subscribe to the subject
        await js.subscribe(
            subject,
            cb=message_handler,
            durable="eventagent-payment-processor",
        )
        
        result_text = "succeeded" if payment_result == "success" else "failed"
        print(f"[green]Payment Processor Service started, will publish payment.{result_text} events...[/green]")
        
        # Keep the service running
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        print("[yellow]Payment Processor Service shutting down...[/yellow]")
    finally:
        await nc.close()


async def init_payment(nats_servers: list[str] | None = None) -> None:
    """Initialize payment processing by subscribing to order events.
    
    This is the main entry point for the payment service.
    It subscribes to events.order.created and triggers payment.initiated events.
    
    Args:
        nats_servers: Optional list of NATS server URLs
    """
    await start_payment_service(nats_servers)


async def init_payment_processor(nats_servers: list[str] | None = None, payment_result: str = "success") -> None:
    """Initialize payment processing by handling payment.initiated events.
    
    This is the main entry point for the payment processor service.
    It subscribes to events.payment.initiated and publishes payment.succeeded/failed events.
    
    Args:
        nats_servers: Optional list of NATS server URLs
        payment_result: Either "success" or "failure" to simulate the payment outcome
    """
    await start_payment_processor_service(nats_servers, payment_result)


def main() -> None:
    """Main entry point for the payment service."""
    asyncio.run(start_payment_service())


if __name__ == "__main__":
    main()