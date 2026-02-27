# EventAgent - Offline-First Collaborative Editor

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
- **Offline-First**: Supports offline event publishing with retry

## Quick Start

### Prerequisites

- Python 3.12+
- NATS Server (JetStream enabled)
- uv (recommended) or pip

### Local Development

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest tests/ -v

# Start NATS server (requires Docker)
docker run -d --name nats -p 4222:4222 nats:latest -js -sd /data

# Run EventAgent (passive observer)
uv run python -m eventagent run --servers localhost:4222

# In another terminal, publish an event
uv run python -m eventagent order --order-id order_123 --amount 100.0
```

### Using Docker (Development)

```bash
# Start all services
docker compose -f docker-compose.dev.yml up -d

# View logs
docker compose -f docker-compose.dev.yml logs -f

# Publish test order
docker compose -f docker-compose.dev.yml exec eventagent order --order-id order_123

# View events
docker compose -f docker-compose.dev.yml exec eventagent events --order-id order_123
```

### Production Deployment

```bash
# Start production environment
docker compose -f docker-compose.prod.yml up -d

# Or with docker directly
docker build -t eventagent .
docker run -d --name eventagent-agent \
  -e NATS_SERVERS=nats.yourdomain.com:4222 \
  -v eventagent-data:/data \
  eventagent
```

## Multi-User / Multi-Device Interaction

When deployed to a server, other users on different devices can interact with EventAgent in two ways:

### 1. Publishing Events from Other Devices (via NATS)

Any device with network access to the NATS server and the EventAgent CLI installed can publish events:

```bash
# On another machine, install eventagent
git clone https://github.com/your-org/eventagent.git
cd eventagent
uv sync

# Publish an order event to the remote server
uv run python -m eventagent order \
  --servers nats://your-server-ip:4222 \
  --order-id order_from_device_456 \
  --amount 250.0

# Publish a custom event
uv run python -m eventagent publish order.created \
  --servers nats://your-server-ip:4222 \
  --payload '{"amount": 500, "items": ["widget"]}' \
  --correlation '{"order_id": "remote_789"}'

# Start a payment service from another machine
uv run python -m eventagent payment-service \
  --servers nats://your-server-ip:4222 \
  --payment-result success
```

**Important**: The NATS port `4222` must be accessible from external devices. In `docker-compose.prod.yml`, the port is already exposed:
```yaml
ports:
  - "4222:4222"
```

For production, consider:
- Using a firewall to restrict NATS access to trusted IPs
- Enabling NATS authentication (see [NATS Auth docs](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro))
- Placing NATS behind a reverse proxy or VPN for secure external access

### 2. Querying Events & Workflows (via SSH or Server Shell)

The SQLite database is stored locally on the server. To query events and workflows, you need shell access to the server:

```bash
# SSH into the server, then query events
ssh user@your-server

# Using docker exec
docker exec eventagent-agent python -m eventagent events --order-id order_123

# List all workflows
docker exec eventagent-agent python -m eventagent workflows

# View a specific workflow timeline
docker exec eventagent-agent python -m eventagent workflow order_123
```

### 3. Built-in Demo Runner (Simulates Multiple Services)

For testing multi-device interaction locally, use the demo runner:

```bash
# Start EventAgent observer
docker compose -f docker-compose.prod.yml up -d

# In another terminal, run the demo which simulates
# order + payment services publishing events
docker exec eventagent-agent python -m eventagent demo
```

### 4. Extending for HTTP API Access (Advanced)

If you need remote users to query events without SSH access, you can add an HTTP API. A minimal FastAPI example:

```python
# api.py - Add this to the project
from fastapi import FastAPI, Query
import sqlite3

app = FastAPI()

@app.get("/events")
def get_events(order_id: str = Query(None)):
    conn = sqlite3.connect("/data/events.db")
    cursor = conn.cursor()
    if order_id:
        cursor.execute(
            "SELECT * FROM events WHERE json_extract(data, '$.order_id') = ?",
            (order_id,)
        )
    else:
        cursor.execute("SELECT * FROM events")
    rows = cursor.fetchall()
    conn.close()
    return {"events": rows}

@app.get("/workflows")
def get_workflows():
    conn = sqlite3.connect("/data/events.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM workflows")
    rows = cursor.fetchall()
    conn.close()
    return {"workflows": rows}
```

Then add a separate container in `docker-compose.prod.yml`:
```yaml
  api:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
    ports:
      - "8000:8000"
    volumes:
      - eventagent-data:/data
    depends_on:
      - eventagent
```

## Deployment Options

### 1. Docker (Recommended)

Build and run with Docker:

```bash
# Build the image
docker build -t eventagent:latest .

# Run the container
docker run -d \
  --name eventagent-agent \
  -e NATS_SERVERS=nats://your-nats-server:4222 \
  -v eventagent-data:/data \
  eventagent:latest run

# With custom database path
docker run -d \
  --name eventagent-agent \
  -e NATS_SERVERS=nats://your-nats-server:4222 \
  -v /path/to/data:/data \
  eventagent:latest run --db-path /data/events.db
```

### 2. Heroku

Create a `Procfile`:
```
worker: python -m eventagent run --servers $NATS_SERVERS
```

Deploy:
```bash
heroku create your-app-name
heroku stack:set container
heroku config:set NATS_SERVERS=nats://your-nats-server:4222
git push heroku main
```

### 3. AWS ECS / Fargate

Create a task definition using the Dockerfile:
- Container: `eventagent:latest`
- Environment: `NATS_SERVERS=nats://your-nats-server:4222`
- Mount: `/data` volume for SQLite persistence
- Expose NATS port `4222` for external event publishing

### 4. Google Cloud Run

Cloud Run can run EventAgent as a service:
```bash
gcloud run deploy eventagent --image gcr.io/PROJECT/eventagent:latest \
  --set-env-vars NATS_SERVERS=nats://your-nats-server:4222 \
  --memory 512Mi
```

### 5. Kubernetes

Create a deployment manifest:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: eventagent
spec:
  replicas: 1
  selector:
    matchLabels:
      app: eventagent
  template:
    metadata:
      labels:
        app: eventagent
    spec:
      containers:
      - name: eventagent
        image: eventagent:latest
        args: ["run", "--servers", "nats://nats:4222"]
        env:
        - name: NATS_SERVERS
          value: "nats://nats:4222"
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: eventagent-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: eventagent
spec:
  selector:
    app: eventagent
  ports:
  - port: 4222
    targetPort: 4222
---
apiVersion: v1
kind: Service
metadata:
  name: nats
spec:
  selector:
    app: nats
  ports:
  - name: nats
    port: 4222
    targetPort: 4222
```

### 6. DigitalOcean / Linode / VPS

On any VPS, you can run directly:

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and run
git clone https://github.com/your/repo.git
cd eventagent
uv sync
uv run python -m eventagent run --servers nats://localhost:4222
```

Other users can then publish events from their machines by pointing to the VPS IP:
```bash
uv run python -m eventagent order \
  --servers nats://your-vps-ip:4222 \
  --order-id multi_user_001
```

### 7. Systemd Service (Linux)

Create `/etc/systemd/system/eventagent.service`:

```ini
[Unit]
Description=EventAgent - Event Observer
After=network.target

[Service]
Type=simple
User=eventagent
WorkingDirectory=/opt/eventagent
Environment=NATS_SERVERS=nats://localhost:4222
ExecStart=/opt/eventagent/.venv/bin/python -m eventagent run
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
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

## Configuration

