"""SQLite storage for EventAgent."""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import default_config
from .models import Correlation, Event, UncorrelatedEvent, WorkflowInstance


DB_PATH = Path.home() / ".eventagent" / "events.db"


class SQLiteEventStore:
    """SQLite-based event storage.
    
    Manages three tables:
        - events: raw event storage
        - workflow_instances: derived workflow views
        - workflow_events: links between events and workflow instances
        - uncorrelated_events: events with missing correlation data
    
    The correlation flow is:
        Event
          ↓
        SQLite: events (persist raw)
          ↓
        SQLite: workflow_instances (upsert)
          ↓
        SQLite: workflow_events (link)
    """
    
    def __init__(self, db_path: str | None = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._initialize_schema()
    
    def _initialize_schema(self) -> None:
        """Create the events, workflow_instances, workflow_events, and uncorrelated_events tables."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                received_at TEXT,
                source TEXT NOT NULL,
                correlation_key TEXT,
                correlation_value TEXT,
                correlation_data TEXT,
                payload TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Migration: Add received_at column if it doesn't exist (for existing databases)
        try:
            self.conn.execute("ALTER TABLE events ADD COLUMN received_at TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_type ON events(event_type)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp ON events(timestamp)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_correlation_key ON events(correlation_key)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_correlation_value ON events(correlation_value)
        """)
        
        # Workflow instances table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS workflow_instances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id TEXT NOT NULL UNIQUE,
                workflow_type TEXT NOT NULL,
                correlation_key TEXT NOT NULL,
                correlation_value TEXT NOT NULL,
                first_seen TEXT,
                last_seen TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_workflow_type ON workflow_instances(workflow_type)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_workflow_correlation ON workflow_instances(correlation_key, correlation_value)
        """)
        
        # Workflow events association table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS workflow_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id TEXT NOT NULL,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (workflow_id) REFERENCES workflow_instances(workflow_id),
                FOREIGN KEY (event_id) REFERENCES events(event_id)
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_workflow_events_workflow ON workflow_events(workflow_id)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_workflow_events_event ON workflow_events(event_id)
        """)
        
        # Uncorrelated events table - for events lacking required correlation
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS uncorrelated_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                received_at TEXT,
                source TEXT NOT NULL,
                correlation_data TEXT,
                payload TEXT,
                reason TEXT NOT NULL,
                resolved INTEGER DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_uncorrelated_event_type ON uncorrelated_events(event_type)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_uncorrelated_resolved ON uncorrelated_events(resolved)
        """)
        self.conn.commit()
    
    def store_event(self, event: Event) -> None:
        """Store an event in SQLite.
        
        This is the 'Persist Event' step in the EventAgent consumer flow.
        Extracts ALL correlation key/value pairs from the event's correlation data.
        The primary correlation key (order_id) is stored in correlation_key/correlation_value
        for indexed lookups, while ALL correlation fields are stored in correlation_data.
        """
        correlation_data_dict: dict[str, Any] = {}
        if isinstance(event.correlation, Correlation):
            correlation_data_dict = event.correlation.model_dump()
        elif isinstance(event.correlation, dict):
            correlation_data_dict = event.correlation
        else:
            correlation_data_dict = {}
        
        # Extract correlation key/value for indexed lookups
        correlation_key = None
        correlation_value = None
        
        if correlation_data_dict:
            configured_keys = default_config.correlation_keys
            for key in configured_keys:
                if key in correlation_data_dict and correlation_data_dict[key] is not None:
                    correlation_key = key
                    correlation_value = str(correlation_data_dict[key])
                    break
            
            # Fall back to first key if no configured key found
            if correlation_key is None:
                first_key = next(iter(correlation_data_dict.keys()), None)
                if first_key:
                    correlation_key = first_key
                    correlation_value = str(correlation_data_dict[first_key])
        
        received_at_str = event.received_at.isoformat() if event.received_at else None
        
        self.conn.execute(
            """
            INSERT OR IGNORE INTO events (event_id, event_type, timestamp, received_at, source, correlation_key, correlation_value, correlation_data, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.event_type,
                event.timestamp.isoformat(),
                received_at_str,
                event.source,
                correlation_key,
                correlation_value,
                json.dumps(correlation_data_dict) if correlation_data_dict else None,
                json.dumps(event.data),
            )
        )
        self.conn.commit()
    
    def store_uncorrelated_event(self, event: UncorrelatedEvent) -> None:
        """Store an uncorrelatable event in SQLite.
        
        These events cannot be attached to any workflow and are stored separately
        for later investigation by AI agents or operators.
        """
        received_at_str = event.received_at.isoformat() if event.received_at else None
        
        self.conn.execute(
            """
            INSERT INTO uncorrelated_events (event_id, event_type, timestamp, received_at, source, correlation_data, payload, reason, resolved)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.event_type,
                event.timestamp.isoformat(),
                received_at_str,
                event.source,
                json.dumps(event.correlation_data) if event.correlation_data else None,
                json.dumps(event.data),
                event.reason,
                1 if event.resolved else 0,
            )
        )
        self.conn.commit()
    
    def upsert_workflow_instance(self, instance: WorkflowInstance) -> None:
        """Insert or update a workflow instance.
        
        This is the 'Persist/Update Workflow' step. Creates the workflow
        instance row if it doesn't exist, or updates its timestamps.
        """
        first_seen_str = instance.first_seen.isoformat() if instance.first_seen else None
        last_seen_str = instance.last_seen.isoformat() if instance.last_seen else None
        
        self.conn.execute(
            """
            INSERT INTO workflow_instances (workflow_id, workflow_type, correlation_key, correlation_value, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(workflow_id) DO UPDATE SET
                first_seen = COALESCE(?,
                    CASE WHEN ? < first_seen THEN ? ELSE first_seen END
                ),
                last_seen = COALESCE(?,
                    CASE WHEN ? > last_seen THEN ? ELSE last_seen END
                )
            """,
            (
                instance.workflow_id,
                instance.workflow_type,
                instance.correlation_key,
                instance.correlation_value,
                first_seen_str,
                last_seen_str,
                first_seen_str,
                first_seen_str,
                first_seen_str,
                last_seen_str,
                last_seen_str,
                last_seen_str,
            )
        )
        self.conn.commit()
    
    def link_event_to_workflow(self, workflow_id: str, event: Event) -> None:
        """Associate an event with a workflow instance.
        
        Inserts a row into workflow_events to link the event to its workflow.
        Uses INSERT OR IGNORE so re-processing is idempotent.
        """
        self.conn.execute(
            """
            INSERT OR IGNORE INTO workflow_events (workflow_id, event_id, event_type, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (
                workflow_id,
                event.event_id,
                event.event_type,
                event.timestamp.isoformat(),
            )
        )
        self.conn.commit()
    
    def store_event_and_correlate(
        self, event: Event | UncorrelatedEvent, workflow_instance: WorkflowInstance | None = None
    ) -> None:
        """Combined operation: store event + upsert workflow + link.
        
        If the event is an UncorrelatedEvent, it is stored in the uncorrelated_events
        table without creating a workflow instance.
        
        This matches the correlation flow:
            Event
              ↓
            SQLite: events (persist raw)
              ↓
            SQLite: workflow_instances (upsert)
              ↓
            SQLite: workflow_events (link)
        """
        if isinstance(event, UncorrelatedEvent):
            self.store_uncorrelated_event(event)
            return
        
        self.store_event(event)
        if workflow_instance:
            self.upsert_workflow_instance(workflow_instance)
            self.link_event_to_workflow(workflow_instance.workflow_id, event)
    
    def get_events(self, event_type: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """Retrieve events from SQLite, optionally filtered by type."""
        query = "SELECT event_id, event_type, timestamp, source, correlation_key, correlation_value, correlation_data, payload FROM events"
        params: tuple = ()
        
        if event_type:
            query += " WHERE event_type = ?"
            params = (event_type,)
        
        query += f" ORDER BY timestamp DESC LIMIT {limit}"
        
        cursor = self.conn.execute(query, params)
        rows = cursor.fetchall()
        
        return [
            {
                "event_id": row[0],
                "event_type": row[1],
                "timestamp": row[2],
                "source": row[3],
                "correlation_key": row[4],
                "correlation_value": row[5],
                "correlation_data": row[6],
                "payload": row[7],
            }
            for row in rows
        ]
    
    def get_events_by_correlation(self, correlation_key: str, correlation_value: str) -> list[dict[str, Any]]:
        """Retrieve events filtered by correlation key and value."""
        cursor = self.conn.execute(
            "SELECT event_id, event_type, timestamp, source, correlation_key, correlation_value, correlation_data, payload FROM events WHERE correlation_key = ? AND correlation_value = ? ORDER BY timestamp ASC",
            (correlation_key, correlation_value)
        )
        rows = cursor.fetchall()
        
        return [
            {
                "event_id": row[0],
                "event_type": row[1],
                "timestamp": row[2],
                "source": row[3],
                "correlation_key": row[4],
                "correlation_value": row[5],
                "correlation_data": row[6],
                "payload": row[7],
            }
            for row in rows
        ]
    
    def get_workflow_instance(self, workflow_id: str) -> dict[str, Any] | None:
        """Retrieve a workflow instance by its workflow_id."""
        cursor = self.conn.execute(
            "SELECT workflow_id, workflow_type, correlation_key, correlation_value, first_seen, last_seen FROM workflow_instances WHERE workflow_id = ?",
            (workflow_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return {
            "workflow_id": row[0],
            "workflow_type": row[1],
            "correlation_key": row[2],
            "correlation_value": row[3],
            "first_seen": row[4],
            "last_seen": row[5],
        }
    
    def get_workflow_by_correlation(self, correlation_key: str, correlation_value: str) -> dict[str, Any] | None:
        """Retrieve a workflow instance by its correlation key/value."""
        cursor = self.conn.execute(
            "SELECT workflow_id, workflow_type, correlation_key, correlation_value, first_seen, last_seen FROM workflow_instances WHERE correlation_key = ? AND correlation_value = ?",
            (correlation_key, correlation_value)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return {
            "workflow_id": row[0],
            "workflow_type": row[1],
            "correlation_key": row[2],
            "correlation_value": row[3],
            "first_seen": row[4],
            "last_seen": row[5],
        }
    
    def get_workflow_events(self, workflow_id: str) -> list[dict[str, Any]]:
        """Retrieve all events linked to a workflow."""
        cursor = self.conn.execute(
            """
            SELECT we.event_id, we.event_type, we.timestamp, e.source, e.correlation_data, e.payload
            FROM workflow_events we
            LEFT JOIN events e ON we.event_id = e.event_id
            WHERE we.workflow_id = ?
            ORDER BY we.timestamp ASC
            """,
            (workflow_id,)
        )
        rows = cursor.fetchall()
        
        return [
            {
                "event_id": row[0],
                "event_type": row[1],
                "timestamp": row[2],
                "source": row[3],
                "correlation_data": row[4],
                "payload": row[5],
            }
            for row in rows
        ]
    
    def get_all_workflow_instances(self, limit: int = 100) -> list[dict[str, Any]]:
        """Retrieve all workflow instances."""
        cursor = self.conn.execute(
            "SELECT workflow_id, workflow_type, correlation_key, correlation_value, first_seen, last_seen FROM workflow_instances ORDER BY first_seen ASC LIMIT ?",
            (limit,)
        )
        return [
            {
                "workflow_id": row[0],
                "workflow_type": row[1],
                "correlation_key": row[2],
                "correlation_value": row[3],
                "first_seen": row[4],
                "last_seen": row[5],
            }
            for row in cursor.fetchall()
        ]
    
    def get_workflow_summary(self, workflow_id: str) -> dict[str, Any] | None:
        """Get workflow summary with event count and latest event type.
        
        Returns:
            Dict with workflow info, event_count, and last_event_type,
            or None if workflow not found.
        """
        # Get workflow instance
        workflow = self.get_workflow_instance(workflow_id)
        if workflow is None:
            return None
        
        # Get event count and latest event for this workflow
        cursor = self.conn.execute(
            """
            SELECT COUNT(*) as event_count,
                   (SELECT event_type FROM workflow_events
                    WHERE workflow_id = ?
                    ORDER BY timestamp DESC LIMIT 1) as last_event_type
            FROM workflow_events
            WHERE workflow_id = ?
            """,
            (workflow_id, workflow_id)
        )
        row = cursor.fetchone()
        
        workflow["event_count"] = row[0]
        workflow["last_event_type"] = row[1]
        return workflow
    
    def get_all_workflow_summaries(self, limit: int = 100) -> list[dict[str, Any]]:
        """Retrieve all workflow instances with event counts and latest event."""
        cursor = self.conn.execute(
            """
            SELECT 
                wi.workflow_id,
                wi.workflow_type,
                wi.correlation_key,
                wi.correlation_value,
                wi.first_seen,
                wi.last_seen,
                COALESCE((
                    SELECT we.event_type FROM workflow_events we
                    WHERE we.workflow_id = wi.workflow_id
                    ORDER BY we.timestamp DESC LIMIT 1
                ), '') as last_event_type,
                COALESCE((
                    SELECT COUNT(*) FROM workflow_events we
                    WHERE we.workflow_id = wi.workflow_id
                ), 0) as event_count
            FROM workflow_instances wi
            ORDER BY wi.last_seen DESC
            LIMIT ?
            """,
            (limit,)
        )
        return [
            {
                "workflow_id": row[0],
                "workflow_type": row[1],
                "correlation_key": row[2],
                "correlation_value": row[3],
                "first_seen": row[4],
                "last_seen": row[5],
                "last_event_type": row[6],
                "event_count": row[7],
            }
            for row in cursor.fetchall()
        ]
    
    def get_uncorrelated_events(self, limit: int = 100) -> list[dict[str, Any]]:
        """Retrieve all uncorrelatable events.
        
        These are events that were received but could not be attached to any workflow
        because they lack required correlation data (e.g., missing order_id).
        
        Returns:
            A list of uncorrelatable event records.
        """
        cursor = self.conn.execute(
            """
            SELECT event_id, event_type, timestamp, received_at, source, correlation_data, payload, reason, resolved
            FROM uncorrelated_events
            ORDER BY received_at DESC
            LIMIT ?
            """,
            (limit,)
        )
        return [
            {
                "event_id": row[0],
                "event_type": row[1],
                "timestamp": row[2],
                "received_at": row[3],
                "source": row[4],
                "correlation_data": row[5],
                "payload": row[6],
                "reason": row[7],
                "resolved": bool(row[8]),
            }
            for row in cursor.fetchall()
        ]
    
    def mark_uncorrelated_resolved(self, event_id: str) -> bool:
        """Mark an uncorrelated event as resolved.
        
        This is used when an AI agent or operator later determines the missing
        correlation information for an uncorrelatable event.
        
        Args:
            event_id: The event to mark as resolved.
        
        Returns:
            True if an event was found and marked, False otherwise.
        """
        cursor = self.conn.execute(
            "UPDATE uncorrelated_events SET resolved = 1 WHERE event_id = ?",
            (event_id,)
        )
        self.conn.commit()
        return cursor.rowcount > 0
    
    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()


# Global storage instance
_storage: SQLiteEventStore | None = None


def get_storage(db_path: str | None = None) -> SQLiteEventStore:
    """Get or create the global storage instance."""
    global _storage
    if _storage is None:
        _storage = SQLiteEventStore(db_path)
    return _storage