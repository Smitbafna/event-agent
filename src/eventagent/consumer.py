"""Event consumer for EventAgent."""

import asyncio
from typing import Any, Callable

from nats.aio.client import Client as NATSClient
from nats.js.api import ConsumerConfig, StreamConfig

from .models import Event
from .storage import SQLiteEventStore


class EventConsumer:
    """Consumes events from NATS and processes them.
    
    Flow:
        NATS
          ↓
        Receive message
          ↓
        Parse JSON
          ↓
        Validate Event
          ↓
        Persist Event
    """
    
    def __init__(self, nc: NATSClient, js: Any, storage: SQLiteEventStore | None = None):
        self.nc = nc
        self.js = js
        self.storage = storage
        self.handlers: dict[str, list[Callable]] = {}
        self._subscription: Any = None
        self._running = False
    
    def register_handler(self, event_type: str, handler: Callable) -> None:
        """Register a handler for an event type (wildcard like 'order.created')."""
        if event_type not in self.handlers:
            self.handlers[event_type] = []
        self.handlers[event_type].append(handler)
    
    async def process_event(self, msg) -> None:
        """Process an incoming event message.
        
        Flow:
            NATS
              ↓
            receive message
              ↓
            decode JSON
              ↓
            validate Pydantic Event
              ↓
            store in SQLite
        """
        try:
            # Decode JSON from NATS message
            data = msg.data.decode()
            
            # Validate Pydantic Event
            event = Event.model_validate_json(data)
            
            # Persist Event to SQLite
            if self.storage:
                self.storage.store_event(event)
            
            # Call registered handlers for this event type
            handlers = self.handlers.get(event.event_type, [])
            
            for handler in handlers:
                try:
                    await handler(event)
                except Exception as e:
                    print(f"[red]Error in handler for {event.event_type}: {e}[/red]")
            
            # Ack the message
            await msg.ack()
        except Exception as e:
            print(f"[red]Error processing event: {e}[/red]")
            # Nack the message on error so it can be redelivered
            try:
                await msg.nak()
            except Exception:
                pass
    
    async def start(self) -> None:
        """Start consuming events using wildcard subscription.
        
        Subscribes to: events.>
        
        This captures:
            - events.order.created
            - events.payment.failed
            - events.order.cancelled
            - any other events.> prefixed subjects
        """
        self._running = True
        
        # Ensure the events stream exists
        try:
            await self.js.add_stream(
                StreamConfig(
                    name="EVENTS",
                    subjects=["events.>"],
                )
            )
        except Exception:
            # Stream may already exist, ignore
            pass
        
        # Create a durable consumer for the wildcard
        try:
            await self.js.add_consumer(
                "EVENTS",
                ConsumerConfig(
                    name="eventagent-consumer",
                    filter_subjects=["events.>"],
                ),
            )
        except Exception:
            # Consumer may already exist, ignore
            pass
        
        # Subscribe to the wildcard subject
        self._subscription = await self.js.subscribe(
            "events.>",
            cb=self.process_event,
            durable="eventagent-consumer",
        )
    
    async def stop(self) -> None:
        """Stop consuming events and clean up."""
        self._running = False
        if self._subscription:
            try:
                await self._subscription.unsubscribe()
            except Exception:
                pass


async def create_consumer(nc: NATSClient, js: Any, storage: SQLiteEventStore | None = None) -> EventConsumer:
    """Create an EventConsumer with NATS connection."""
    return EventConsumer(nc, js, storage)