"""Event consumer for EventAgent."""

import asyncio
from typing import Callable

from .models import Event, EventType
from .store import EventStore


class EventConsumer:
    """Consumes events from NATS and processes them."""
    
    def __init__(self, store: EventStore):
        self.store = store
        self.handlers: dict[EventType, list[Callable]] = {}
    
    def register_handler(self, event_type: EventType, handler: Callable) -> None:
        """Register a handler for an event type."""
        if event_type not in self.handlers:
            self.handlers[event_type] = []
        self.handlers[event_type].append(handler)
    
    async def process_event(self, msg) -> None:
        """Process an incoming event message."""
        try:
            data = msg.data.decode()
            event = Event.model_validate_json(data)
            
            # Find handlers for this event type
            handlers = self.handlers.get(event.event_type, [])
            
            for handler in handlers:
                try:
                    await handler(event)
                except Exception as e:
                    print(f"Error in handler for {event.event_type}: {e}")
            
            # Ack the message
            await msg.ack()
        except Exception as e:
            print(f"Error processing event: {e}")
    
    async def start(self) -> None:
        """Start consuming events for all registered handlers."""
        for event_type in self.handlers:
            await self.store.subscribe(event_type, self.process_event)