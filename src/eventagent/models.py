"""Event models for EventAgent."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Event types for the distributed system."""
    
    ORDER_CREATED = "order.created"
    PAYMENT_INITIATED = "payment.initiated"
    PAYMENT_FAILED = "payment.failed"
    PAYMENT_RETRY_SCHEDULED = "payment.retry_scheduled"


class Event(BaseModel):
    """Base event model."""
    
    event_type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str
    id: str | None = None


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