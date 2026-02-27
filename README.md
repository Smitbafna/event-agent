# EventAgent

EventAgent is a passive event observer that subscribes to NATS JetStream events, validates them, persists them to SQLite, and correlates related events into workflow instances.

## Architecture

```
Order Service ──┐
                │
Payment Service ─┼──► NATS ──► EventAgent (Passive Observer)
                │
                └──► publishes events
```

EventAgent observes events published by services (Order Service, Payment Service) and persists them. It does NOT trigger workflows or publish new events.

## Features

- **Passive Observation**: Subscribes to `events.>` on NATS, validates and persists events to SQLite
- **Event Correlation**: Groups related events into workflow instances by correlation key (e.g., `order_id`)
- **Out-of-Order Detection**: Distinguishes between `timestamp` (event time) and `received_at` (observation time)
- **CLI Interface**: Rich CLI for querying events and workflows

## Installation

```bash
pip install eventagent
```

Or with `uv`:

```bash
uv pip install eventagent
```

## Quick Start

```bash
# Install the package
pip install eventagent

# Start NATS server (requires Docker)
docker run -d --name nats -p 4222:4222 nats:latest -js -sd /data

# Run EventAgent (passive observer)
python -m eventagent run --servers localhost:4222

# In another terminal, publish an event
python -m eventagent order --order-id order_123 --amount 100.0
```


## CLI Commands

```bash
# Start passive observer
eventagent run --servers nats://localhost:4222

# Publish an order event
eventagent order --order-id order_123 --amount 100.0

# Publish a custom event
eventagent publish order.created --payload '{"amount": 500}' --correlation '{"order_id": "123"}'

# Start payment service (listens for orders)
eventagent payment-service --payment-result success

# Query events
eventagent events --order-id 8472
eventagent events --type order.created

# View workflow timeline
eventagent workflow order_8472

# List all workflows
eventagent workflows
```

## Environment Variables

- `NATS_SERVERS`: NATS server URLs (comma-separated) - default: `localhost:4222`
- `PAYMENT_RESULT`: For payment service, either `success` or `failure`
- `ORDER_ID`: Order ID for order service CLI
- `ORDER_AMOUNT`: Order amount for order service CLI

## Event Flow Example

```
User A (Device 1)            NATS                   EventAgent (Server)
     │                        │                          │
     └── publish order.created ──►                        │
     │                        │                            │
User B (Device 2)             │                            │
     │                        │                            │
     └── publish payment.initiated ──►                     │
     │                        │                            │
     │                        └───► observe & persist    │
     │                        │                          │
User C (Device 3)             │                            │
     │                        │                            │
     └── publish payment.succeeded ──►                     │
     │                        │                            │
     │                        └───► observe & persist    │
     │                        │                          │
     │         ┌──────────────┴──────────────┐            │
     │         │  EventAgent correlates all  │            │
     │         │  events into a single       │            │
     │         │  workflow (order_id)        │            │
     │         └─────────────────────────────┘            │
```

Multiple users on different devices can publish events. EventAgent correlates them by the `order_id` field.