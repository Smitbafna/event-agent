"""Event models for EventAgent."""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Event types for the distributed system."""
    
    ORDER_CREATED = "order.created"
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
    
    order_id: str | None = None
    customer_id: str | None = None
    payment_id: str | None = None
    
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
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str
    correlation: Correlation | dict[str, Any] = Field(default_factory=dict)
    data: dict[str, Any] = Field(default_factory=dict)


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