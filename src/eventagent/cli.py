"""CLI interface for EventAgent."""

import asyncio
import json

import typer
from rich.console import Console

from .consumer import EventConsumer
from .models import Event, EventType
from .store import create_event_store

app = typer.Typer()
console = Console()


@app.command()
def listen(
    servers: str = typer.Option(
        "nats://localhost:4222",
        "--servers",
        "-s",
        help="NATS server URL(s), comma-separated",
    )
) -> None:
    """Start EventAgent and listen for events."""
    
    async def run():
        store = await create_event_store(servers.split(","))
        consumer = EventConsumer(store)
        
        # Register handlers for each event type
        async def handle_order_created(event: Event):
            console.print(f"[green]Order created:[/green] {event.payload}")
        
        async def handle_payment_initiated(event: Event):
            console.print(f"[blue]Payment initiated:[/blue] {event.payload}")
        
        async def handle_payment_failed(event: Event):
            console.print(f"[red]Payment failed:[/red] {event.payload}")
        
        async def handle_payment_retry_scheduled(event: Event):
            console.print(f"[yellow]Payment retry scheduled:[/yellow] {event.payload}")
        
        consumer.register_handler(EventType.ORDER_CREATED, handle_order_created)
        consumer.register_handler(EventType.PAYMENT_INITIATED, handle_payment_initiated)
        consumer.register_handler(EventType.PAYMENT_FAILED, handle_payment_failed)
        consumer.register_handler(EventType.PAYMENT_RETRY_SCHEDULED, handle_payment_retry_scheduled)
        
        console.print("[bold]EventAgent started. Listening for events...[/bold]")
        await consumer.start()
        
        # Keep running
        while True:
            await asyncio.sleep(1)
    
    asyncio.run(run())


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
    servers: str = typer.Option(
        "nats://localhost:4222",
        "--servers",
        "-s",
        help="NATS server URL(s), comma-separated",
    )
) -> None:
    """Publish an event to NATS."""
    
    async def run():
        store = await create_event_store(servers.split(","))
        
        # Parse the event type
        try:
            et = EventType(event_type)
        except ValueError:
            console.print(f"[red]Invalid event type: {event_type}[/red]")
            console.print(f"[yellow]Valid types: {[e.value for e in EventType]}[/yellow]")
            raise typer.Exit(1)
        
        # Parse payload
        payload_dict = json.loads(payload)
        
        # Create and publish event
        event = Event(
            event_type=et,
            payload=payload_dict,
            source=source,
        )
        
        subject = await store.publish(event)
        console.print(f"[green]Published event to {subject}[/green]")
    
    asyncio.run(run())


@app.command()
def status(
    servers: str = typer.Option(
        "nats://localhost:4222",
        "--servers",
        "-s",
        help="NATS server URL(s), comma-separated",
    )
) -> None:
    """Check EventAgent connection status."""
    
    async def run():
        store = await create_event_store(servers.split(","))
        console.print("[green]Connected to NATS server[/green]")
        console.print(f"Stream: {store.stream_name}")
    
    asyncio.run(run())


if __name__ == "__main__":
    app()