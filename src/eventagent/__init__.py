"""EventAgent - Event-driven agent for distributed systems."""

__version__ = "0.1.0"

from .consumer import EventConsumer
from .models import (
    Correlation,
    Event,
    EventType,
    OrderCreatedEvent,
    PaymentFailedEvent,
    PaymentInitiatedEvent,
    PaymentRetryScheduledEvent,
    PaymentSucceededEvent,
)
from .storage import SQLiteEventStore, get_storage

__all__ = [
    "Event",
    "EventType",
    "Correlation",
    "OrderCreatedEvent",
    "PaymentInitiatedEvent",
    "PaymentSucceededEvent",
    "PaymentFailedEvent",
    "PaymentRetryScheduledEvent",
    "EventConsumer",
    "SQLiteEventStore",
    "get_storage",
]