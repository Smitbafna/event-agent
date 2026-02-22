"""EventAgent - Event-driven agent for distributed systems."""

__version__ = "0.1.0"

from .consumer import EventConsumer
from .models import Correlation, Event, EventType
from .storage import SQLiteEventStore, get_storage

__all__ = [
    "Event",
    "EventType", 
    "Correlation",
    "EventConsumer",
    "SQLiteEventStore",
    "get_storage",
]