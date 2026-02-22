"""SQLite storage for EventAgent."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import Event


DB_PATH = Path.home() / ".eventagent" / "events.db"


class SQLiteEventStore:
    """SQLite-based event storage."""
    
    def __init__(self, db_path: str | None = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._initialize_schema()
    
    def _initialize_schema(self) -> None:
        """Create the events table if it doesn't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                source TEXT NOT NULL,
                correlation TEXT,
                data TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_type ON events(event_type)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp ON events(timestamp)
        """)
        self.conn.commit()
    
    def store_event(self, event: Event) -> None:
        """Store an event in SQLite."""
        # Print event receipt info
        print(f"Received event: {event.event_type}")
        
        # Print correlation info if available
        if event.correlation:
            corr_data = event.correlation.model_dump() if hasattr(event.correlation, 'model_dump') else event.correlation
            if corr_data:
                # Format as key=value pairs
                corr_pairs = ", ".join(f"{k}={v}" for k, v in corr_data.items())
                print(f"Correlation: {corr_pairs}")
        
        # Print stored event
        print(f"Stored event: {event.event_id}")
        
        self.conn.execute(
            """
            INSERT OR IGNORE INTO events (event_id, event_type, timestamp, source, correlation, data)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.event_type,  # event_type is already a string
                event.timestamp.isoformat(),
                event.source,
                str(event.correlation.model_dump() if hasattr(event.correlation, 'model_dump') else event.correlation),
                str(event.data),
            )
        )
        self.conn.commit()
    
    def get_events(self, event_type: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """Retrieve events from SQLite, optionally filtered by type."""
        query = "SELECT event_id, event_type, timestamp, source, correlation, data FROM events"
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
                "correlation": row[4],
                "data": row[5],
            }
            for row in rows
        ]
    
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