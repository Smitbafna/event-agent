"""Configuration for EventAgent correlation keys."""

from typing import Literal

from pydantic import BaseModel


class CorrelationConfig(BaseModel):
    """Configuration for a correlation key.
    
    Defines which correlation key to use for linking events together.
    """
    
    key: Literal["order_id", "customer_id", "payment_id"] | None = None


class Config(BaseModel):
    """EventAgent configuration.
    
    Supports multiple correlation keys for linking events together:
    - order_id: For order-related workflows
    - customer_id: For customer-related workflows  
    - payment_id: For payment-related workflows
    """
    
    order_id: bool = True
    customer_id: bool = True
    payment_id: bool = True
    
    @property
    def correlation_keys(self) -> list[str]:
        """Return list of configured correlation keys in priority order."""
        keys = []
        if self.order_id:
            keys.append("order_id")
        if self.customer_id:
            keys.append("customer_id")
        if self.payment_id:
            keys.append("payment_id")
        return keys


# Default configuration
default_config = Config()