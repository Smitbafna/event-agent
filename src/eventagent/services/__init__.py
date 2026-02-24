"""Services package for EventAgent.

ARCHITECTURE: Services are EVENT PUBLISHERS, not consumers of EventAgent.

    Order Service ──┐
                    │
    Payment Service ─┼──► NATS ──► EventAgent (Passive Observer)
                    │
                    └──► publishes events

Then:

    Payment Service ──┐
                      │
    Payment Processor ─┼──► NATS ──► EventAgent (Passive Observer)
                      │
                      └──► publishes payment events

EventAgent:
    - Subscribes to events.> on NATS
    - Validates events
    - Persists events to SQLite
    - Does NOT trigger workflows

Services:
    - Publish events to NATS (Order Service: order.created)
    - Subscribe to specific events to trigger THEIR OWN workflows (Payment Service: subscribes to order.created)
    - The Payment Service and Order Service are independent actors that publish events
    - EventAgent does NOT call these services - it only observes what they publish

EVENT INDEPENDENCE PRINCIPLE:
    Services communicate through events, NOT direct function calls.
    Each service has its OWN NATS connection and operates independently.
"""

from .demo_runner import run_demo_flow, run_demo_flow_independent, run_payment_stuck_demo
from .order_service import create_order, process_order
from .payment_service import (
    handle_order_created,
    handle_payment_initiated,
    init_payment,
    init_payment_processor,
    start_payment_processor_service,
    start_payment_service,
)

__all__ = [
    "run_demo_flow",
    "run_demo_flow_independent",
    "run_payment_stuck_demo",
    "create_order",
    "process_order",
    "handle_order_created",
    "handle_payment_initiated",
    "init_payment",
    "init_payment_processor",
    "start_payment_processor_service",
    "start_payment_service",
]