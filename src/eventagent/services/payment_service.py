"""Payment Service for handling payment events."""

import asyncio
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


async def init_payment(nats_servers: list[str] | None = None) -> None:
    """Initialize payment processing by subscribing to order events.
    
    This is the main entry point for the payment service.
    It subscribes to events.order.created and triggers payment.initiated events.
    
    Args:
        nats_servers: Optional list of NATS server URLs
    """
    await start_payment_service(nats_servers)


def main() -> None:
    """Main entry point for the payment service."""
    asyncio.run(start_payment_service())


if __name__ == "__main__":
    main()