"""Event store for EventAgent using NATS JetStream."""

from abc import ABC, abstractmethod
from typing import Any

from nats.aio.client import Client as NATSClient
from nats.js.api import ConsumerConfig, StreamConfig

from .models import Event


class EventStore(ABC):
    """Abstract event store interface."""
    
    @abstractmethod
    async def publish(self, event: Event) -> str:
        """Publish an event to the store."""
        pass
    
    @abstractmethod
    async def subscribe(self, event_type: str, callback):
        """Subscribe to events of a specific type."""
        pass


class NATSEventStore:
    """NATS JetStream based event store implementation."""
    
    def __init__(self, nc: NATSClient, js: Any):
        self.nc = nc
        self.js = js
        self.stream_name = "EVENTS"
    
    async def initialize(self) -> None:
        """Initialize the stream for events."""
        from nats.js.errors import BadRequestError
        
        try:
            await self.js.add_stream(
                StreamConfig(
                    name=self.stream_name,
                    subjects=["events.>"],
                )
            )
        except BadRequestError:
            # Stream already exists, ignore
            pass
    
    async def publish(self, event: Event) -> str:
        """Publish an event to NATS JetStream."""
        subject = f"events.{event.event_type}"
        await self.js.publish(subject, event.model_dump_json().encode())
        return subject
    
    async def subscribe(self, event_type: str, callback):
        """Subscribe to events of a specific type."""
        subject = f"events.{event_type}"
        
        # Create or update consumer
        await self.js.add_consumer(
            self.stream_name,
            ConsumerConfig(
                name=f"eventagent-{event_type.replace('.', '-')}",
                filter_subjects=[subject],
            ),
        )
        
        # Subscribe to the subject
        async def message_handler(msg):
            await callback(msg)
        
        await self.js.subscribe(subject, cb=message_handler, durable=f"eventagent-{event_type}")


async def create_event_store(servers: list[str] | None = None) -> NATSEventStore:
    """Create and connect an event store."""
    nc = NATSClient()
    js = nc.jetstream()
    
    # Normalize server URLs - add nats:// prefix if missing
    default_servers = ["nats://localhost:4222"]
    if servers:
        connection_servers = []
        for server in servers:
            if not server.startswith(("nats://", "tls://", "ws://", "wss://")):
                connection_servers.append(f"nats://{server}")
            else:
                connection_servers.append(server)
    else:
        connection_servers = default_servers
    await nc.connect(servers=connection_servers)
    
    store = NATSEventStore(nc, js)
    await store.initialize()
    
    return store