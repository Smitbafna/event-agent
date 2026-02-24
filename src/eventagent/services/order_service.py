"""Order Service for creating and publishing order events.

NOTE: This is a SEPARATE SERVICE that operates independently.
It publishes order events to NATS. EventAgent PASSIVELY OBSERVES
these events but does NOT trigger or call this service.

Architecture:
    Order Service ──┐
                    │
    Payment Service ─┼──► NATS ──► EventAgent (Passive Observer)
                    │
                    └──► publishes order events
    
Order Service responsibilities:
    - Publishes order events (order.created, order.cancelled) to NATS
    - Operates independently - called directly, not triggered by EventAgent
    - EventAgent only observes what this service publishes
"""

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


async def start_order_service(
    nats_servers: list[str] | None = None,
    order_id: str = "order_8472",
    amount: float = 1000.0,
    currency: str = "USD",
) -> None:
    """Start the order service and publish an order event.
    
    This is the entry point for running the order service as an independent process.
    
    Architecture:
        Order Service ──┐
                        │
        Payment Service ─┼──► NATS
                        │
                        └──► publishes order events
    """
    subject = await create_order(order_id, amount, currency, nats_servers)
    print(f"[green]Order Service completed: published to {subject}[/green]")


def main() -> None:
    """Main entry point for the order service."""
    asyncio.run(start_order_service())


if __name__ == "__main__":
    main()