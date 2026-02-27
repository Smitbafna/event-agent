"""EventAgent - Event-driven agent for distributed systems."""

__version__ = "0.1.0"

from .config import Config, CorrelationConfig, default_config
from .consumer import EventConsumer
from .correlation import CorrelationEngine
from .models import (
    Correlation,
    Event,
    EventType,
    OrderCreatedEvent,
    PaymentFailedEvent,
    PaymentInitiatedEvent,
    PaymentRetryScheduledEvent,
    PaymentSucceededEvent,
    UncorrelatedEvent,
    WorkflowInstance,
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
    "UncorrelatedEvent",
    "WorkflowInstance",
    "EventConsumer",
    "CorrelationEngine",
    "SQLiteEventStore",
    "get_storage",
    "Config",
    "CorrelationConfig",
    "default_config",
]