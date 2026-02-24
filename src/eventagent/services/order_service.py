"""Order Service for creating and publishing order events."""

import asyncio

from ..models import Correlation, Event, EventType
from ..store import create_event_store


async def create_order(
    order_id: str,
    amount: float,
    currency: str = "USD",
    nats_servers: list[str] | None = None,
) -> str:
    """Create and publish an order.created event.
    
    Args:
        order_id: Unique identifier for the order
        amount: Order amount
        currency: Currency code (default: USD)
        nats_servers: Optional list of NATS server URLs
    
    Returns:
        The NATS subject the event was published to
    """
    event = Event(
        event_type=EventType.ORDER_CREATED.value,
        source="order-service",
        correlation=Correlation(order_id=order_id),
        data={"amount": amount, "currency": currency},
    )

    store = await create_event_store(nats_servers)

    try:
        subject = await store.publish(event)
        print(f"Published order.created event to {subject}")
        print(f"Event: {event.model_dump_json(indent=2)}")
        return subject
    finally:
        await store.nc.close()


async def process_order(
    order_id: str,
    amount: float,
    currency: str = "USD",
    nats_servers: list[str] | None = None,
) -> str:
    """Process a new order by creating and publishing the order.created event.
    
    This is the main entry point for order creation. It publishes to NATS
    at events.order.created, which allows other services (like Payment Service)
    to subscribe and react to the event.
    
    Args:
        order_id: Unique identifier for the order
        amount: Order amount
        currency: Currency code (default: USD)
        nats_servers: Optional list of NATS server URLs
    
    Returns:
        The NATS subject the event was published to
    """
    return await create_order(order_id, amount, currency, nats_servers)


def main() -> None:
    """Main entry point for the order service."""
    asyncio.run(create_order(order_id="order_8472", amount=1000.0, currency="INR"))


if __name__ == "__main__":
    main()