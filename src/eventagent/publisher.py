"""Event publisher for simulated Order Service."""

import asyncio

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


def main() -> None:
    """Main entry point for the publisher."""
    asyncio.run(publish_order_created())


if __name__ == "__main__":
    main()