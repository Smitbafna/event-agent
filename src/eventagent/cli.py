"""CLI interface for EventAgent."""

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
    """Core async logic for running the agent."""
    # Connect to NATS
    nc = NATSClient()
    await nc.connect(servers=servers.split(","))
    js = nc.jetstream()
    
    # Initialize SQLite storage
    storage = get_storage(db_path)
    
    # Create consumer with NATS connection and storage
    consumer = EventConsumer(nc, js, storage)
    
    # Register handlers for each known event type
    async def handle_order_created(event: Event):
        console.print(f"[green]Order created:[/green] {event.data}")
    
    async def handle_order_cancelled(event: Event):
        console.print(f"[red]Order cancelled:[/red] {event.data}")
    
    async def handle_payment_initiated(event: Event):
        console.print(f"[blue]Payment initiated:[/blue] {event.data}")
    
    async def handle_payment_failed(event: Event):
        console.print(f"[red]Payment failed:[/red] {event.data}")
    
    async def handle_payment_retry_scheduled(event: Event):
        console.print(f"[yellow]Payment retry scheduled:[/yellow] {event.data}")
    
    consumer.register_handler(EventType.ORDER_CREATED.value, handle_order_created)
    consumer.register_handler(EventType.ORDER_CANCELLED.value, handle_order_cancelled)
    consumer.register_handler(EventType.PAYMENT_INITIATED.value, handle_payment_initiated)
    consumer.register_handler(EventType.PAYMENT_FAILED.value, handle_payment_failed)
    consumer.register_handler(EventType.PAYMENT_RETRY_SCHEDULED.value, handle_payment_retry_scheduled)
    
    console.print(f"Connected to NATS: {servers}")
    console.print("Subscribed to: events.>")
    console.print("EventAgent is running...")
    
    # Start consuming events with wildcard subscription
    await consumer.start()
    
    # Keep running
    while True:
        await asyncio.sleep(1)


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
    """List events from SQLite storage."""
    
    storage = get_storage(db_path if db_path else None)
    events = storage.get_events(event_type=event_type if event_type else None, limit=limit)
    
    for event in events:
        console.print(f"[cyan]{event['event_type']}[/cyan] - {event['event_id']}")
        console.print(f"  Source: {event['source']}")
        console.print(f"  Timestamp: {event['timestamp']}")
        console.print(f"  Correlation: {event['correlation']}")
        console.print(f"  Data: {event['data']}")
        console.print()


if __name__ == "__main__":
    app()