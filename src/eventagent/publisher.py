"""Event publisher for simulated services."""

import asyncio
from uuid import uuid4

from .models import Correlation, Event, EventType
from .store import create_event_store


async def publish_order_created(
    order_id: str = "8472",
    amount: float = 1000.0,
    nats_servers: list[str] | None = None,
) -> str:
    """Create and publish an order.created event.
    
    Args:
        order_id: The order ID to include in correlation
        amount: The order amount
        nats_servers: Optional list of NATS server URLs
    
    Returns:
        The NATS subject the event was published to
    """
    # Create the event
    event = Event(
        event_type=EventType.ORDER_CREATED.value,
        source="order-service",
        correlation=Correlation(order_id=order_id),
        data={"amount": amount},
    )
    
    # Connect to NATS and publish
    store = await create_event_store(nats_servers)
    
    try:
        subject = await store.publish(event)
        print(f"Published event to {subject}")
        print(f"Event: {event.model_dump_json(indent=2)}")
        return subject
    finally:
        await store.nc.close()


async def publish_payment_initiated(
    order_id: str = "8472",
    payment_id: str = "pay_123",
    amount: float = 1000.0,
    nats_servers: list[str] | None = None,
) -> str:
    """Create and publish a payment.initiated event.
    
    Args:
        order_id: The order ID to include in correlation
        payment_id: The payment ID
        amount: The payment amount
        nats_servers: Optional list of NATS server URLs
    
    Returns:
        The NATS subject the event was published to
    """
    event = Event(
        event_type=EventType.PAYMENT_INITIATED.value,
        source="payment-service",
        correlation=Correlation(order_id=order_id, payment_id=payment_id),
        data={"amount": amount},
    )
    
    store = await create_event_store(nats_servers)
    
    try:
        subject = await store.publish(event)
        print(f"Published event to {subject}")
        print(f"Event: {event.model_dump_json(indent=2)}")
        return subject
    finally:
        await store.nc.close()


async def publish_payment_succeeded(
    order_id: str = "8472",
    payment_id: str = "pay_123",
    amount: float = 1000.0,
    transaction_id: str | None = None,
    nats_servers: list[str] | None = None,
) -> str:
    """Create and publish a payment.succeeded event.
    
    Args:
        order_id: The order ID to include in correlation
        payment_id: The payment ID
        amount: The payment amount
        transaction_id: Optional transaction ID from payment provider
        nats_servers: Optional list of NATS server URLs
    
    Returns:
        The NATS subject the event was published to
    """
    event = Event(
        event_type=EventType.PAYMENT_SUCCEEDED.value,
        source="payment-service",
        correlation=Correlation(order_id=order_id, payment_id=payment_id),
        data={"amount": amount, "transaction_id": transaction_id} if transaction_id else {"amount": amount},
    )
    
    store = await create_event_store(nats_servers)
    
    try:
        subject = await store.publish(event)
        print(f"Published event to {subject}")
        print(f"Event: {event.model_dump_json(indent=2)}")
        return subject
    finally:
        await store.nc.close()


async def run_payment_flow_demo(
    order_id: str = "8472",
    amount: float = 1000.0,
    nats_servers: list[str] | None = None,
) -> None:
    """Run a demo of the payment flow: order.created -> payment.initiated -> payment.succeeded.
    
    This demonstrates the Milestone 1B event flow.
    """
    print("=== Payment Flow Demo ===")
    print()
    
    # Step 1: Publish order.created
    print("Step 1: Publishing order.created event")
    await publish_order_created(order_id=order_id, amount=amount, nats_servers=nats_servers)
    print()
    
    # Step 2: Publish payment.initiated
    payment_id = f"pay_{uuid4().hex[:8]}"
    print(f"Step 2: Publishing payment.initiated event (payment_id={payment_id})")
    await publish_payment_initiated(order_id=order_id, payment_id=payment_id, amount=amount, nats_servers=nats_servers)
    print()
    
    # Step 3: Publish payment.succeeded
    transaction_id = f"txn_{uuid4().hex[:8]}"
    print(f"Step 3: Publishing payment.succeeded event (transaction_id={transaction_id})")
    await publish_payment_succeeded(
        order_id=order_id,
        payment_id=payment_id,
        amount=amount,
        transaction_id=transaction_id,
        nats_servers=nats_servers,
    )
    print()
    print("=== Payment Flow Complete ===")


def main() -> None:
    """Main entry point for the publisher."""
    asyncio.run(publish_order_created())


if __name__ == "__main__":
    main()