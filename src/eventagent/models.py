"""Event models for EventAgent."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator
from pydantic.json import pydantic_encoder


class EventType(str, Enum):
    """Event types for the distributed system."""
    
    ORDER_CREATED = "order.created"
    ORDER_CANCELLED = "order.cancelled"
    PAYMENT_INITIATED = "payment.initiated"
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"
    PAYMENT_RETRY_SCHEDULED = "payment.retry_scheduled"


class Correlation(BaseModel):
    """Correlation data to link related events together.
    
    This allows constructing event chains where all events for a given
    correlation key (e.g., order_id) can be linked together:
    
        order_id = 8472
            ↓
        order.created
        payment.initiated
        payment.succeeded
    """
    
    model_config = {"extra": "allow"}
    
    order_id: str | None = None
    customer_id: str | None = None
    payment_id: str | None = None
    
    def __getitem__(self, key: str) -> Any:
        """Allow dict-like access for compatibility."""
        return getattr(self, key)
    
    def __contains__(self, key: str) -> bool:
        """Allow 'in' operator for compatibility."""
        try:
            getattr(self, key)
            return True
        except AttributeError:
            return False
    
    def model_dump(self) -> dict[str, Any]:
        """Return only non-None values."""
        return {k: v for k, v in super().model_dump().items() if v is not None}


class Event(BaseModel):
    """Base event model with common structure.
    
    Every event in the system follows this structure:
    
        {
            "event_id": "evt_123",
            "event_type": "order.created",
            "timestamp": "2026-07-19T10:00:00Z",   # when the event happened
            "received_at": "2026-07-19T10:00:05Z",  # when EventAgent observed it
            "source": "order-service",
            "correlation": {
                "order_id": "8472"
            },
            "data": {
                "amount": 1000
            }
        }
    
    In distributed systems, events may arrive late or out of order.
    The distinction between:
        timestamp    → when the event happened (event producer's time)
        received_at  → when EventAgent observed it (consumer's time)
    
    This matters for out-of-order detection and timeline reconstruction.
    """
    
    event_id: str = Field(default_factory=lambda: f"evt_{uuid4().hex[:8]}")
    event_type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str
    correlation: Correlation | dict[str, Any] = Field(default_factory=lambda: {})
    data: dict[str, Any] = Field(default_factory=lambda: {})
    
    @model_validator(mode='before')
    @classmethod
    def _validate_correlation(cls, data: Any) -> Any:
        """Convert correlation dict to Correlation model if needed."""
        if isinstance(data, dict) and 'correlation' in data:
            if isinstance(data['correlation'], dict):
                data['correlation'] = Correlation(**data['correlation'])
        return data
    
    def to_json_dict(self) -> dict[str, Any]:
        """Convert event to a JSON-serializable dictionary."""
        result = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "received_at": self.received_at.isoformat(),
            "source": self.source,
            "data": self.data,
        }
        # Handle correlation serialization
        if self.correlation:
            if isinstance(self.correlation, Correlation):
                result['correlation'] = self.correlation.model_dump()
            else:
                result['correlation'] = self.correlation
        return result
    
    @classmethod
    def create(
        cls,
        event_type: str,
        source: str,
        data: dict[str, Any] | None = None,
        correlation: Correlation | dict[str, Any] | None = None,
    ) -> "Event":
        """Factory method to create an event with standard envelope.
        
        Args:
            event_type: The type of event (e.g., "order.created")
            source: The source service publishing the event
            data: Optional event payload data
            correlation: Optional correlation data to link related events
        """
        return cls(
            event_type=event_type,
            source=source,
            data=data or {},
            correlation=correlation or {},
        )


class OrderCreatedEvent(BaseModel):
    """Order created event payload."""
    
    order_id: str
    customer_id: str
    amount: float
    currency: str = "USD"


class PaymentInitiatedEvent(BaseModel):
    """Payment initiated event payload."""
    
    order_id: str
    payment_id: str
    amount: float


class PaymentSucceededEvent(BaseModel):
    """Payment succeeded event payload."""
    
    order_id: str
    payment_id: str
    amount: float
    transaction_id: str | None = None


class PaymentFailedEvent(BaseModel):
    """Payment failed event payload."""
    
    order_id: str
    payment_id: str
    error_code: str
    error_message: str


class PaymentRetryScheduledEvent(BaseModel):
    """Payment retry scheduled event payload."""
    
    order_id: str
    payment_id: str
    retry_at: datetime
    attempt: int


class WorkflowInstance(BaseModel):
    """A logical view over existing events grouped by correlation.
    
    A WorkflowInstance is not a new event. It is a derived view that
    collects all related events (e.g., order.created, payment.initiated,
    payment.failed) for a given correlation key/value pair.
    
    The workflow timeline is ordered by event.timestamp (when events
    happened), not by received_at. This ensures the correct sequence even
    if events arrive out of order.
    
    Example:
        Events (received out of order):
            payment.initiated  (timestamp: 10:00:05, received_at: 10:00:00)
            order.created      (timestamp: 10:00:00, received_at: 10:00:05)
            payment.succeeded  (timestamp: 10:00:10, received_at: 10:00:10)
        
        Workflow timeline (sorted by timestamp):
            order.created      @ 10:00:00
            payment.initiated  @ 10:00:05
            payment.succeeded  @ 10:00:10
    """
    
    workflow_id: str
    workflow_type: str
    correlation_key: str
    correlation_value: str
    events: list[Event] = Field(default_factory=list)
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    
    def model_dump(self, **kwargs) -> dict[str, Any]:
        """Serialize to a JSON-serializable dictionary."""
        result: dict[str, Any] = {
            "workflow_id": self.workflow_id,
            "workflow_type": self.workflow_type,
            "correlation_key": self.correlation_key,
            "correlation_value": self.correlation_value,
            "events": [e.to_json_dict() for e in self.events],
        }
        if self.first_seen:
            result["first_seen"] = self.first_seen.isoformat()
        if self.last_seen:
            result["last_seen"] = self.last_seen.isoformat()
        return result
    
    @classmethod
    def from_events(cls, events: list[Event], correlation_key: str) -> "WorkflowInstance":
        """Build a WorkflowInstance from a list of related events.
        
        Events are sorted by their timestamp (event time), not by
        received_at. This ensures the correct timeline sequence even
        when events arrive out of order from the distributed system.
        
        Args:
            events: List of related events (may be in any order).
            correlation_key: The correlation key used to group these events
                             (e.g., "order_id").
        
        Returns:
            A populated WorkflowInstance with derived fields computed
            from the events.
        """
        if not events:
            raise ValueError("Cannot build WorkflowInstance from empty events list")
        
        # Derive correlation_value from the first event's correlation data
        first_event = events[0]
        correlation_value = ""
        if isinstance(first_event.correlation, Correlation):
            correlation_value = getattr(first_event.correlation, correlation_key, "")
        elif isinstance(first_event.correlation, dict):
            correlation_value = first_event.correlation.get(correlation_key, "")
        
        # Derive workflow_type from the domain part of the first event type
        # e.g., "order.created" -> "order"
        workflow_type = first_event.event_type.split(".")[0] if "." in first_event.event_type else first_event.event_type
        
        # Build workflow_id from workflow_type and correlation_value
        workflow_id = f"{workflow_type}_{correlation_value}"
        
        # Sort events by timestamp (event time) for correct timeline
        sorted_events = sorted(events, key=lambda e: e.timestamp)
        
        # Determine first_seen and last_seen from event timestamps
        timestamps = [e.timestamp for e in sorted_events if e.timestamp]
        first_seen = min(timestamps) if timestamps else None
        last_seen = max(timestamps) if timestamps else None
        
        return cls(
            workflow_id=workflow_id,
            workflow_type=workflow_type,
            correlation_key=correlation_key,
            correlation_value=str(correlation_value),
            events=sorted_events,
            first_seen=first_seen,
            last_seen=last_seen,
        )


class UncorrelatedEvent(BaseModel):
    """An event that arrived without required correlation data.
    
    These events cannot be attached to any workflow and are stored
    separately for later investigation by AI agents or operators.
    
    Example:
        {
            "event_type": "payment.failed",
            "event_id": "evt_123",
            "correlation": {}
        }
        
        Would be stored as:
        UncorrelatedEvent(
            event_id="evt_123",
            event_type="payment.failed",
            reason="Missing required correlation key: order_id"
        )
    """
    
    event_id: str
    event_type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str
    data: dict[str, Any] = Field(default_factory=dict)
    correlation_data: dict[str, Any] = Field(default_factory=dict)
    reason: str  # Why this event is uncorrelated
    resolved: bool = False  # Whether this event has been later correlated
    
    def to_json_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "received_at": self.received_at.isoformat(),
            "source": self.source,
            "data": self.data,
            "correlation_data": self.correlation_data,
            "reason": self.reason,
            "resolved": self.resolved,
        }