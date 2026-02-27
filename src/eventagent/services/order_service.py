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
import os

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
        print(f"[OrderService] Published order.created to {subject}")
        print(f"[OrderService] Event: {event.model_dump_json(indent=2)}")
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


async def create_order_with_retry(
    order_id: str,
    amount: float,
    currency: str = "USD",
    nats_servers: list[str] | None = None,
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> str:
    """Create an order with automatic retry on failure.
    
    This method attempts to publish the order event and will retry on failure.
    Useful for offline-first scenarios where initial connection may fail.
    
    Args:
        order_id: Unique identifier for the order
        amount: Order amount
        currency: Currency code (default: USD)
        nats_servers: Optional list of NATS server URLs
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
    
    Returns:
        The NATS subject the event was published to
    """
    servers = nats_servers
    last_error: Exception | None = None
    
    for attempt in range(max_retries):
        try:
            store = await create_event_store(servers)
            
            event = Event(
                event_type=EventType.ORDER_CREATED.value,
                source="order-service",
                correlation=Correlation(order_id=order_id),
                data={"amount": amount, "currency": currency},
            )
            
            subject = await store.publish(event)
            print(f"[OrderService] Published order.created (attempt {attempt + 1})")
            print(f"[OrderService] Event: {event.model_dump_json(indent=2)}")
            
            await store.nc.close()
            return subject
            
        except Exception as e:
            last_error = e
            print(f"[OrderService] Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
    
    raise last_error or Exception("Failed to publish order after all retries")


def main() -> None:
    """Main entry point for the order service.
    
    Environment variables:
        ORDER_ID: Order ID to create (default: order_8472)
        ORDER_AMOUNT: Order amount (default: 1000.0)
        ORDER_CURRENCY: Currency (default: USD)
        NATS_SERVERS: NATS servers (default: localhost:4222)
    """
    order_id = os.environ.get("ORDER_ID", "order_8472")
    amount = float(os.environ.get("ORDER_AMOUNT", "1000.0"))
    currency = os.environ.get("ORDER_CURRENCY", "USD")
    nats_servers = os.environ.get("NATS_SERVERS", "localhost:4222")
    
    asyncio.run(create_order(order_id, amount, currency, nats_servers.split(",")))


if __name__ == "__main__":
    main()