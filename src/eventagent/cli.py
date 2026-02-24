"""CLI interface for EventAgent - Passive Observer.

Architecture:
    Order Service ──┐
                    │
    Payment Service ─┼──► NATS
                    │
                    └──► EventAgent (Passive Observer)

EventAgent Flow:
    NATS
      ↓
    Subscribe to events.>
      ↓
    Validate
      ↓
    Persist to SQLite

EventAgent is a PASSIVE OBSERVER. It subscribes to events published by services
and persists them. It does NOT trigger workflows or publish new events.
"""

import asyncio
import json

import typer
from nats.aio.client import Client as NATSClient
from rich.console import Console

from .consumer import EventConsumer
from .models import Event, Correlation, EventType
from .storage import SQLiteEventStore, get_storage
from .store import create_event_store

app = typer.Typer()
console = Console()


async def _run_agent(servers: str, db_path: str | None):
    """Core async logic for running the agent (passive observer mode).
    
    EventAgent observes events published by services:
        - Order Service publishes: events.order.created, events.order.cancelled
        - Payment Service publishes: events.payment.initiated, events.payment.succeeded, etc.
    
    EventAgent:
        - Validates each event
        - Persists to SQLite
        - Does NOT trigger workflows
    """
    # Connect to NATS
    nc = NATSClient()
    await nc.connect(servers=servers.split(","))
    js = nc.jetstream()
    
    # Initialize SQLite storage
    storage = get_storage(db_path)
    
    # Create consumer with NATS connection and storage
    consumer = EventConsumer(nc, js, storage)
    
    # Register PASSIVE handlers - only for logging/observation, NOT workflow triggering
    # These handlers only print to console - they do NOT publish events
    
    async def log_order_event(event: Event):
        """Passive observer: log order event."""
        if event.event_type == EventType.ORDER_CREATED.value:
            console.print(f"[green]order.created:[/green] {event.data}")
        else:
            console.print(f"[red]order.cancelled:[/red] {event.data}")
    
    async def log_payment_event(event: Event):
        """Passive observer: log payment events."""
        console.print(f"[blue]{event.event_type}:[/] {event.data}")
    
    # Register handlers for observation only (logging)
    consumer.register_handler(EventType.ORDER_CREATED.value, log_order_event)
    consumer.register_handler(EventType.ORDER_CANCELLED.value, log_order_event)
    consumer.register_handler(EventType.PAYMENT_INITIATED.value, log_payment_event)
    consumer.register_handler(EventType.PAYMENT_SUCCEEDED.value, log_payment_event)
    consumer.register_handler(EventType.PAYMENT_FAILED.value, log_payment_event)
    consumer.register_handler(EventType.PAYMENT_RETRY_SCHEDULED.value, log_payment_event)
    
    console.print(f"Connected to NATS: {servers}")
    console.print("Subscribed to: events.>")
    console.print("[bold yellow]EventAgent is running as PASSIVE OBSERVER[/bold yellow]")
    console.print("Events will be validated and persisted to SQLite")
    console.print("No workflows will be triggered")
    
    # Start consuming events with wildcard subscription
    await consumer.start()
    
    try:
        # Keep running
        while consumer._running:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()
        await nc.close()
        storage.close()


@app.command()
def run(
    servers: str = typer.Option(
        "localhost:4222",
        "--servers",
        "-s",
        help="NATS server URL(s), comma-separated",
    ),
    db_path: str = typer.Option(
        "",
        "--db-path",
        "-d",
        help="SQLite database path (default: ~/.eventagent/events.db)",
    )
) -> None:
    """Start EventAgent and listen for events.
    
    Subscribes to: events.>
    
    This captures:
        - events.order.created
        - events.payment.failed
        - events.order.cancelled
    """
    
    try:
        asyncio.run(_run_agent(servers, db_path if db_path else None))
    except KeyboardInterrupt:
        console.print("\n[yellow]Shutting down EventAgent...[/yellow]")


# Keep listen as an alias for run
@app.command()
def listen(
    servers: str = typer.Option(
        "localhost:4222",
        "--servers",
        "-s",
        help="NATS server URL(s), comma-separated",
    ),
    db_path: str = typer.Option(
        "",
        "--db-path",
        "-d",
        help="SQLite database path (default: ~/.eventagent/events.db)",
    )
) -> None:
    """Start EventAgent and listen for events (alias for 'run').
    
    Subscribes to: events.>
    
    This captures:
        - events.order.created
        - events.payment.failed
        - events.order.cancelled
    """
    
    try:
        asyncio.run(_run_agent(servers, db_path if db_path else None))
    except KeyboardInterrupt:
        console.print("\n[yellow]Shutting down EventAgent...[/yellow]")


@app.command("publish-test")
def publish_test(
    servers: str = typer.Option(
        "localhost:4222",
        "--servers",
        "-s",
        help="NATS server URL(s), comma-separated",
    )
) -> None:
    """Publish a test order.created event to NATS."""
    
    async def run_async():
        store = await create_event_store(servers.split(","))
        
        # Create and publish a test event
        event = Event(
            event_type=EventType.ORDER_CREATED.value,
            source="order-service",
            correlation=Correlation(order_id="8472"),
            data={"amount": 1000},
        )
        
        subject = await store.publish(event)
        console.print(f"Published test event to {subject}")
        
        # Close connection
        await store.nc.close()
    
    asyncio.run(run_async())


@app.command()
def publish(
    event_type: str,
    payload: str = typer.Option(
        "{}",
        "--payload",
        "-p",
        help="JSON payload for the event",
    ),
    source: str = typer.Option(
        "cli",
        "--source",
        help="Event source identifier",
    ),
    correlation: str = typer.Option(
        "{}",
        "--correlation",
        "-c",
        help="JSON correlation data (e.g., {\"order_id\": \"123\"})",
    ),
    servers: str = typer.Option(
        "localhost:4222",
        "--servers",
        "-s",
        help="NATS server URL(s), comma-separated",
    )
) -> None:
    """Publish an event to NATS."""
    
    async def run_async():
        store = await create_event_store(servers.split(","))
        
        # Parse the event type
        try:
            et = EventType(event_type)
        except ValueError:
            console.print(f"[red]Invalid event type: {event_type}[/red]")
            console.print(f"[yellow]Valid types: {[e.value for e in EventType]}[/yellow]")
            raise typer.Exit(1)
        
        # Parse payload and correlation
        payload_dict = json.loads(payload)
        correlation_dict = json.loads(correlation) if correlation.strip() else {}
        
        # Create and publish event
        event = Event(
            event_type=et.value,  # Pass the string value
            data=payload_dict,
            source=source,
            correlation=correlation_dict,
        )
        
        subject = await store.publish(event)
        console.print(f"[green]Published event to {subject}[/green]")
        
        # Close connection
        await store.nc.close()
    
    asyncio.run(run_async())


@app.command()
def status(
    servers: str = typer.Option(
        "localhost:4222",
        "--servers",
        "-s",
        help="NATS server URL(s), comma-separated",
    )
) -> None:
    """Check EventAgent connection status."""
    
    async def run_async():
        store = await create_event_store(servers.split(","))
        console.print("[green]Connected to NATS server[/green]")
        console.print(f"Stream: {store.stream_name}")
    
    asyncio.run(run_async())


@app.command("events")
def events(
    order_id: str = typer.Option(
        "",
        "--order-id",
        help="Filter by order_id correlation",
    ),
    customer_id: str = typer.Option(
        "",
        "--customer-id",
        help="Filter by customer_id correlation",
    ),
    payment_id: str = typer.Option(
        "",
        "--payment-id",
        help="Filter by payment_id correlation",
    ),
    event_type: str = typer.Option(
        "",
        "--type",
        "-t",
        help="Filter by event type (optional)",
    ),
    limit: int = typer.Option(
        100,
        "--limit",
        "-l",
        help="Maximum number of events to retrieve",
    ),
    db_path: str = typer.Option(
        "",
        "--db-path",
        "-d",
        help="SQLite database path (default: ~/.eventagent/events.db)",
    )
) -> None:
    """Query events from SQLite storage.
    
    Can filter by correlation keys like --order-id or --customer-id.
    
    Example:
        eventagent events --order-id 8472
    
    Output format:
        10:00:00  order.created
    """
    storage = get_storage(db_path if db_path else None)
    
    # Determine correlation filter
    events_result = []
    if order_id:
        events_result = storage.get_events_by_correlation("order_id", order_id)
    elif customer_id:
        events_result = storage.get_events_by_correlation("customer_id", customer_id)
    elif payment_id:
        events_result = storage.get_events_by_correlation("payment_id", payment_id)
    else:
        events_result = storage.get_events(event_type=event_type if event_type else None, limit=limit)
    
    for event in events_result:
        # Parse timestamp and extract time
        timestamp = event["timestamp"]
        try:
            # ISO format: 2026-07-19T10:00:00+00:00 or 2026-07-19T10:00:00Z
            time_part = timestamp.split("T")[1][:8]  # Get HH:MM:SS
        except (IndexError, AttributeError):
            time_part = timestamp
        
        console.print(f"{time_part}  {event['event_type']}")


@app.command()
def list_events(
    event_type: str = typer.Option(
        "",
        "--type",
        "-t",
        help="Filter by event type (optional)",
    ),
    limit: int = typer.Option(
        100,
        "--limit",
        "-l",
        help="Maximum number of events to retrieve",
    ),
    db_path: str = typer.Option(
        "",
        "--db-path",
        "-d",
        help="SQLite database path (default: ~/.eventagent/events.db)",
    )
) -> None:
    """List events from SQLite storage (detailed view)."""
    
    storage = get_storage(db_path if db_path else None)
    events_result = storage.get_events(event_type=event_type if event_type else None, limit=limit)
    
    for event in events_result:
        console.print(f"[cyan]{event['event_type']}[/cyan] - {event['event_id']}")
        console.print(f"  Source: {event['source']}")
        console.print(f"  Timestamp: {event['timestamp']}")
        console.print(f"  Correlation Key: {event.get('correlation_key')}")
        console.print(f"  Correlation Value: {event.get('correlation_value')}")
        console.print(f"  Payload: {event.get('payload')}")
        console.print()


@app.command()
def demo(
    payment_result: str = typer.Option(
        "success",
        "--payment-result",
        "-p",
        help="Payment result to simulate: 'success' or 'failure'",
    ),
    scenario: str = typer.Option(
        "",
        "--scenario",
        help="Demo scenario to run: 'payment-stuck' for broken workflow demo",
    ),
    servers: str = typer.Option(
        "localhost:4222",
        "--servers",
        "-s",
        help="NATS server URL(s), comma-separated",
    ),
    db_path: str = typer.Option(
        "",
        "--db-path",
        "-d",
        help="SQLite database path (default: ~/.eventagent/events.db)",
    )
) -> None:
    """Run the EventAgent demo.
    
    Flow:
        1. Payment Service subscriber starts (listens for order.created)
        2. EventAgent consumer starts (listens for events.>)
        3. Order Service publishes order.created
        4. Payment Service receives order.created and publishes payment.initiated
        5. Payment Service publishes payment.succeeded or payment.failed
        6. EventAgent stores all events
    
    Expected output:
        [OrderService] Published order.created
        [PaymentService] Received order.created
        [PaymentService] Published payment.initiated
        [PaymentService] Published payment.succeeded
        [EventAgent] Stored order.created
        [EventAgent] Stored payment.initiated
        [EventAgent] Stored payment.succeeded
    
    Scenarios:
        --scenario payment-stuck  Shows a broken workflow where payment fails and retry never occurs
    """
    async def run_async():
        from eventagent.services import run_demo_flow_independent, run_payment_stuck_demo
        
        if scenario == "payment-stuck":
            await run_payment_stuck_demo(
                nats_servers=servers.split(","),
                db_path=db_path if db_path else None
            )
        else:
            await run_demo_flow_independent(
                nats_servers=servers.split(","),
                payment_result=payment_result,
                db_path=db_path if db_path else None
            )
    
    try:
        asyncio.run(run_async())
    except KeyboardInterrupt:
        console.print("\n[yellow]Demo interrupted[/yellow]")


@app.command("demo-independent")
def demo_independent(
    payment_result: str = typer.Option(
        "success",
        "--payment-result",
        "-p",
        help="Payment result to simulate: 'success' or 'failure'",
    ),
    servers: str = typer.Option(
        "localhost:4222",
        "--servers",
        "-s",
        help="NATS server URL(s), comma-separated",
    ),
    db_path: str = typer.Option(
        "",
        "--db-path",
        "-d",
        help="SQLite database path (default: ~/.eventagent/events.db)",
    )
) -> None:
    """Run a demo showing INDEPENDENT event flow through NATS.
    
    This demonstrates that services communicate through events, NOT direct function calls:
    
        Order Service ──┐
                        │
        Payment Service ─┼──► NATS ──► EventAgent (Passive Observer)
                        │
                        └──► publishes order events
        
        Then:
                        │
                        │
        Payment Service ─┼──► NATS ──► EventAgent (Passive Observer)
                        │
                        └──► publishes payment events
    
    Each service has its OWN NATS connection and operates independently.
    EventAgent passively observes all events without triggering anything.
    
    Example:
        eventagent demo-independent --payment-result success
        eventagent demo-independent --payment-result failure
    """
    async def run_async():
        from eventagent.services import run_demo_flow
        
        await run_demo_flow(
            nats_servers=servers.split(","),
            payment_result=payment_result,
            db_path=db_path if db_path else None
        )
    
    try:
        asyncio.run(run_async())
    except KeyboardInterrupt:
        console.print("\n[yellow]Demo interrupted[/yellow]")


if __name__ == "__main__":
    app()