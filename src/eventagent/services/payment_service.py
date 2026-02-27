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
import os
from typing import Any
from uuid import uuid4

from nats.aio.client import Client as NATSClient
from nats.js.api import ConsumerConfig, StreamConfig

from ..models import Correlation, Event, EventType
from ..store import NATSEventStore


def _normalize_servers(servers: list[str] | None) -> list[str]:
    """Normalize server URLs to include nats:// prefix if missing."""
    if servers is None:
        return ["nats://localhost:4222"]
    normalized = []
    for server in servers:
        if not server.startswith(("nats://", "tls://", "ws://", "wss://")):
            normalized.append(f"nats://{server}")
        else:
            normalized.append(server)
    return normalized


async def handle_order_created(event: Event, store: NATSEventStore, payment_result: str = "success") -> None:
    """Handle order.created event and trigger payment processing.
    
    Flow:
        order.created
              ↓
        payment.initiated
              ↓
        payment.succeeded (if payment_result == "success")
        OR
        payment.failed (if payment_result == "failure")
    
    The correlation (order_id) is preserved from the incoming event to the outgoing event.
    
    Args:
        event: The order.created event received from NATS
        store: The event store for publishing the response events
        payment_result: Either "success" or "failure" to simulate the payment outcome
    """
    # Extract order_id from correlation to preserve it
    order_id = None
    if isinstance(event.correlation, Correlation):
        order_id = event.correlation.order_id
    elif isinstance(event.correlation, dict) and "order_id" in event.correlation:
        order_id = event.correlation["order_id"]
    
    if not order_id:
        print("[red]Warning: order.created event missing order_id in correlation[/red]")
        return
    
    # Generate a payment_id
    payment_id = f"payment_{uuid4().hex[:8]}"
    
    # Extract amount from event data
    amount = event.data.get("amount", 0.0)
    
    # Publish payment.initiated event with preserved correlation
    payment_event = Event(
        event_type=EventType.PAYMENT_INITIATED.value,
        source="payment-service",
        correlation=Correlation(order_id=order_id, payment_id=payment_id),
        data={
            "payment_id": payment_id,
            "amount": amount,
        },
    )
    
    subject = await store.publish(payment_event)
    print(f"[PaymentService] Published payment.initiated to {subject}")
    print(f"[PaymentService] Event: {payment_event.model_dump_json(indent=2)}")
    
    # Immediately process payment and publish result
    if payment_result == "success":
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
        print(f"[PaymentService] Published payment.succeeded to {subject}")
        print(f"[PaymentService] Event: {result_event.model_dump_json(indent=2)}")
    
    else:
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
        print(f"[PaymentService] Published payment.failed to {subject}")
        print(f"[PaymentService] Event: {result_event.model_dump_json(indent=2)}")


async def start_payment_service(
    nats_servers: list[str] | None = None,
    payment_result: str = "success",
) -> None:
    """Start the payment service and subscribe to order.created events.
    
    This is a COMPLETE payment service that:
        1. Subscribes to order.created events
        2. Publishes payment.initiated events
        3. Publishes payment.succeeded OR payment.failed events
    
    Flow:
        events.order.created
              ↓
        Payment Service receives it
              ↓
        payment.initiated
              ↓
        payment.succeeded OR payment.failed (based on payment_result flag)
    
    The correlation (order_id) is preserved across the event chain.
    
    Args:
        nats_servers: Optional list of NATS server URLs
        payment_result: Either "success" or "failure" to simulate the payment outcome
    """
    nc = NATSClient()
    
    servers = _normalize_servers(nats_servers)
    await nc.connect(servers=servers)
    js = nc.jetstream()
    
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
        
        # Create store for publishing
        store = NATSEventStore(nc, js)
        await store.initialize()
        
        # Subscribe to order.created events
        subject = f"events.{EventType.ORDER_CREATED.value}"
        
        async def message_handler(msg):
            """Process incoming order.created message."""
            try:
                # Decode JSON from NATS message
                data = msg.data.decode()
                
                # Validate Pydantic Event
                event = Event.model_validate_json(data)
                
                print(f"[PaymentService] Received {event.event_type}")
                
                # Handle the event and process payment
                await handle_order_created(event, store, payment_result)
                
                # Ack the message
                await msg.ack()
            except Exception as e:
                print(f"[PaymentService] Error processing event: {e}")
                try:
                    await msg.nak()
                except Exception:
                    pass
        
        # Subscribe directly - NATS will create consumer automatically
        await js.subscribe(subject, cb=message_handler)
        
        result_text = "succeeded" if payment_result == "success" else "failed"
        print(f"[PaymentService] Started, listening for order.created events...")
        print(f"[PaymentService] Will publish payment.{result_text} events automatically")
        
        # Keep the service running
        while True:
            await asyncio.sleep(1)
            
    except asyncio.CancelledError:
        print("[PaymentService] Shutting down...")
    finally:
        await nc.close()


def main() -> None:
    """Main entry point for the payment service."""
    payment_result = os.environ.get("PAYMENT_RESULT", "success")
    nats_servers = os.environ.get("NATS_SERVERS", "localhost:4222")
    
    asyncio.run(start_payment_service(
        nats_servers=nats_servers.split(","),
        payment_result=payment_result,
    ))


if __name__ == "__main__":
    main()