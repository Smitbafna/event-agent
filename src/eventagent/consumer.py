"""Event consumer for EventAgent - Passive Observer."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable

from nats.aio.client import Client as NATSClient
from nats.js.api import ConsumerConfig, StreamConfig

from .correlation import CorrelationEngine
from .models import Event, UncorrelatedEvent
from .storage import SQLiteEventStore


class EventConsumer:
    """Consumes events from NATS and processes them as a passive observer.
    
    Flow:
        NATS
          ↓
        Subscribe to events.>
          ↓
        Receive message
          ↓
        Validate Event
          ↓
        Persist Raw Event to SQLite
          ↓
        Correlation Engine
          ↓
        Update Workflow Instance
          ↓
        Persist Workflow to SQLite
          ↓
        Call handlers (passive observation only)
    
    NOTE: EventAgent is a PASSIVE OBSERVER. It does NOT trigger workflows.
    It only observes, validates, persists, and correlates events.
    Handlers registered here are for passive purposes only (logging, monitoring,
    metrics) and should NOT publish new events.
    """
    
    def __init__(
        self,
        nc: NATSClient,
        js: Any,
        storage: SQLiteEventStore | None = None,
        correlation_engine: CorrelationEngine | None = None,
    ):
        self.nc = nc
        self.js = js
        self.storage = storage
        self.correlation_engine = correlation_engine or CorrelationEngine()
        self.handlers: dict[str, list[Callable]] = {}
        self._subscription: Any = None
        self._running = False
    
    def register_handler(self, event_type: str, handler: Callable) -> None:
        """Register a handler for an event type (wildcard like 'order.created').
        
        WARNING: Handlers are intended for passive observation only (logging, metrics).
        They should NOT trigger workflows by publishing new events.
        EventAgent's role is to observe and persist - not to orchestrate.
        """
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
            persist raw Event to SQLite
              ↓
            Correlation Engine → update WorkflowInstance
              ↓
            persist workflow to SQLite
              ↓
            call registered handlers (passive observation only)
        
        NOTE: This is a passive observer. Handlers are called for observation
        purposes only and should not trigger workflows.
        """
        try:
            # Decode JSON from NATS message
            data = msg.data.decode()
            
            # Validate Pydantic Event
            event = Event.model_validate_json(data)
            
            # Stamp received_at at observation time (when EventAgent received it)
            # This is distinct from event.timestamp (when the producer says it happened)
            # In distributed systems, events may arrive late or out of order,
            # so tracking both timestamps is essential for timeline reconstruction.
            event.received_at = datetime.now(timezone.utc)
            
            # Run correlation engine to find/update the workflow instance
            # This happens before persistence so we have the workflow_id
            result = self.correlation_engine.process_event(event)
            
            # Persist event, upsert workflow instance, and link them together
            if self.storage:
                # If uncorrelatable, store it in the uncorrelated_events table
                # Otherwise, store the event with its workflow instance
                if isinstance(result, UncorrelatedEvent):
                    self.storage.store_event_and_correlate(result, None)
                else:
                    self.storage.store_event_and_correlate(event, result)
            
            # Call registered handlers for this event type (passive observation only)
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
        
        NOTE: EventAgent is a passive observer. It subscribes to observe events
        published by services (Order Service, Payment Service) and persists them.
        It does NOT trigger any workflows.
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
        
        # Subscribe to the wildcard subject (consumer will be auto-created)
        self._subscription = await self.js.subscribe(
            "events.>",
            cb=self.process_event,
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