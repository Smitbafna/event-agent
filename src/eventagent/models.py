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
        payment.failed
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
            "timestamp": "2026-07-19T10:00:00Z",
            "source": "order-service",
            "correlation": {
                "order_id": "8472"
            },
            "data": {
                "amount": 1000
            }
        }
    """
    
    event_id: str = Field(default_factory=lambda: f"evt_{uuid4().hex[:8]}")
    event_type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
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