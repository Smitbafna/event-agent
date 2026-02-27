"""Correlation engine that groups events into WorkflowInstances.

The correlation engine is the bridge between raw events and derived workflow views.
When an event arrives, the engine:

    1. Extracts the correlation key(s) from the event's correlation data
    2. Finds or creates the WorkflowInstance for that correlation value
    3. Attaches the event to the workflow instance

Flow:
    Event
      ↓
    Extract correlation key/value
      ↓
    Find or create WorkflowInstance
      ↓
    Attach event to workflow

Example:
    For order_id=8472:

    Workflow 8472
    ├── order.created
    ├── payment.initiated
    └── payment.failed

    For order_id=9001 (separate workflow):

    Workflow 9001
    └── order.created

    Never mix correlation values across different workflows.
"""

import json
from collections import OrderedDict
from typing import Any

from .config import default_config
from .models import Correlation, Event, UncorrelatedEvent, WorkflowInstance


class CorrelationEngine:
    """Groups events into WorkflowInstances by correlation key/value.
    
    Maintains an in-memory map of workflow instances, each keyed by
    a composite key of (correlation_key, correlation_value). This
    ensures that events with different correlation values never mix.
    
    Example state after processing three events for order_id=8472
    and one event for order_id=9001:
    
        workflows = {
            ("order_id", "8472"): WorkflowInstance(order_id=8472) [3 events],
            ("order_id", "9001"): WorkflowInstance(order_id=9001) [1 event],
        }
    """
    
    def __init__(self, max_workflows: int = 1000):
        self._workflows: OrderedDict[tuple[str, str], WorkflowInstance] = OrderedDict()
        self._uncorrelated_events: list[UncorrelatedEvent] = []
        self.max_workflows = max_workflows
    
    def process_event(self, event: Event) -> WorkflowInstance | UncorrelatedEvent:
        """Process an event: extract correlation, find/create workflow, attach event.
        
        If the event has no recognizable correlation data, it is stored as an
        UncorrelatedEvent instead of raising an exception. This allows the system
        to track incomplete workflows for later investigation.
        
        Args:
            event: The incoming event to process.
        
        Returns:
            The WorkflowInstance this event was attached to, or an UncorrelatedEvent
            if the event has no recognizable correlation data.
        """
        correlation_key, correlation_value = self._extract_correlation(event)
        
        if not correlation_key or not correlation_value:
            # Store as uncorrelated event instead of raising
            reason = self._build_missing_correlation_reason(event)
            uncorrelatable = UncorrelatedEvent(
                event_id=event.event_id,
                event_type=event.event_type,
                timestamp=event.timestamp,
                received_at=event.received_at,
                source=event.source,
                data=event.data,
                correlation_data=self._get_correlation_dict(event.correlation),
                reason=reason,
            )
            self._uncorrelated_events.append(uncorrelatable)
            return uncorrelatable
        
        # Build the composite key
        workflow_key = (correlation_key, str(correlation_value))
        
        # Find or create the workflow instance
        instance = self._workflows.get(workflow_key)
        if instance is None:
            instance = WorkflowInstance(
                workflow_id=f"{self._derive_workflow_type(event)}_{correlation_value}",
                workflow_type=self._derive_workflow_type(event),
                correlation_key=correlation_key,
                correlation_value=str(correlation_value),
                events=[],
            )
            self._workflows[workflow_key] = instance
            self._evict_if_needed()
        
        # Attach the event to the workflow
        instance.events.append(event)
        
        # Update first_seen and last_seen
        if instance.first_seen is None or event.timestamp < instance.first_seen:
            instance.first_seen = event.timestamp
        if instance.last_seen is None or event.timestamp > instance.last_seen:
            instance.last_seen = event.timestamp
        
        return instance
    
    def get_workflow(
        self, correlation_key: str, correlation_value: str
    ) -> WorkflowInstance | None:
        """Retrieve a workflow instance by its correlation key/value.
        
        Args:
            correlation_key: The correlation key (e.g., "order_id").
            correlation_value: The correlation value (e.g., "8472").
        
        Returns:
            The WorkflowInstance if found, None otherwise.
        """
        return self._workflows.get((correlation_key, str(correlation_value)))
    
    def get_all_workflows(self) -> list[WorkflowInstance]:
        """Return all tracked workflow instances.
        
        Returns:
            A list of all WorkflowInstances, ordered by first_seen.
        """
        return sorted(
            self._workflows.values(),
            key=lambda w: w.first_seen or w.events[0].timestamp if w.events else None,
        )
    
    def get_uncorrelated_events(self) -> list[UncorrelatedEvent]:
        """Return all uncorrelatable events.
        
        These events have been received but cannot be attached to any workflow
        because they lack required correlation data (e.g., missing order_id).
        
        Returns:
            A list of all UncorrelatedEvents, ordered by received_at.
        """
        return sorted(
            self._uncorrelated_events,
            key=lambda e: e.received_at,
        )
    
    def mark_resolved(self, event_id: str, correlation_key: str, correlation_value: str) -> bool:
        """Mark an uncorrelated event as resolved when correlation is provided.
        
        This is used when an AI agent or operator later determines the missing
        correlation information for an uncorrelatable event.
        
        Args:
            event_id: The event to mark as resolved.
            correlation_key: The correlation key to use.
            correlation_value: The correlation value to use.
        
        Returns:
            True if an event was found and marked, False otherwise.
        """
        for uncorrelatable in self._uncorrelated_events:
            if uncorrelatable.event_id == event_id:
                uncorrelatable.resolved = True
                # Move the event to its proper workflow
                event = Event(
                    event_id=uncorrelatable.event_id,
                    event_type=uncorrelatable.event_type,
                    timestamp=uncorrelatable.timestamp,
                    received_at=uncorrelatable.received_at,
                    source=uncorrelatable.source,
                    correlation={correlation_key: correlation_value},
                    data=uncorrelatable.data,
                )
                workflow_key = (correlation_key, str(correlation_value))
                instance = self._workflows.get(workflow_key)
                if instance is None:
                    instance = WorkflowInstance(
                        workflow_id=f"{self._derive_workflow_type(event)}_{correlation_value}",
                        workflow_type=self._derive_workflow_type(event),
                        correlation_key=correlation_key,
                        correlation_value=str(correlation_value),
                        events=[],
                    )
                    self._workflows[workflow_key] = instance
                instance.events.append(event)
                return True
        return False
    
    def remove_workflow(self, correlation_key: str, correlation_value: str) -> bool:
        """Remove a workflow instance from tracking.
        
        Args:
            correlation_key: The correlation key.
            correlation_value: The correlation value.
        
        Returns:
            True if a workflow was removed, False otherwise.
        """
        workflow_key = (correlation_key, str(correlation_value))
        if workflow_key in self._workflows:
            del self._workflows[workflow_key]
            return True
        return False
    
    def clear(self) -> None:
        """Remove all tracked workflow instances and uncorrelated events."""
        self._workflows.clear()
        self._uncorrelated_events.clear()
    
    @property
    def count(self) -> int:
        """Number of tracked workflow instances."""
        return len(self._workflows)
    
    @property
    def uncorrelated_count(self) -> int:
        """Number of uncorrelatable events."""
        return len(self._uncorrelated_events)
    
    def load_from_storage(
        self, storage: Any, correlation_key: str, correlation_value: str
    ) -> WorkflowInstance | None:
        """Load events from storage and build a workflow instance.
        
        This reconstructs a WorkflowInstance from persisted events, e.g.,
        when the application starts up and needs to restore state.
        
        Args:
            storage: An object with a get_events_by_correlation method
                     (e.g., SQLiteEventStore).
            correlation_key: The correlation key to query.
            correlation_value: The correlation value to query.
        
        Returns:
            A reconstructed WorkflowInstance, or None if no events found.
        """
        rows = storage.get_events_by_correlation(correlation_key, correlation_value)
        if not rows:
            return None
        
        events = []
        for row in rows:
            event = Event(
                event_id=row["event_id"],
                event_type=row["event_type"],
                timestamp=row["timestamp"],
                source=row["source"],
                correlation=Correlation(**json.loads(row.get("correlation_data", "{}"))),
                data=json.loads(row.get("payload", "{}")),
            )
            events.append(event)
        
        return WorkflowInstance.from_events(events, correlation_key=correlation_key)
    
    def _extract_correlation(
        self, event: Event
    ) -> tuple[str | None, str | None]:
        """Extract the primary correlation key and value from an event.
        
        Checks configured keys first (e.g., "order_id"), falls back to
        the first available key in the correlation data.
        
        Args:
            event: The event to extract correlation from.
        
        Returns:
            A tuple of (correlation_key, correlation_value) or (None, None).
        """
        correlation_data: dict[str, Any] = {}
        if isinstance(event.correlation, Correlation):
            correlation_data = event.correlation.model_dump()
        elif isinstance(event.correlation, dict):
            correlation_data = event.correlation
        
        if not correlation_data:
            return None, None
        
        # Check configured correlation keys first (e.g., order_id, payment_id)
        configured_keys = default_config.correlation_keys
        for key in configured_keys:
            if key in correlation_data and correlation_data[key] is not None:
                return key, str(correlation_data[key])
        
        # Fall back to first available key
        first_key = next(iter(correlation_data.keys()), None)
        if first_key:
            return first_key, str(correlation_data[first_key])
        
        return None, None
    
    def _build_missing_correlation_reason(self, event: Event) -> str:
        """Build a human-readable reason for why an event is uncorrelatable.
        
        Args:
            event: The event with missing correlation.
        
        Returns:
            A string describing why the event cannot be correlated.
        """
        correlation_data = self._get_correlation_dict(event.correlation)
        
        if not correlation_data:
            return "Missing required correlation data (empty correlation object)"
        
        # Check if any configured keys are present but None
        configured_keys = default_config.correlation_keys
        for key in configured_keys:
            if key in correlation_data and correlation_data[key] is None:
                return f"Missing required correlation key: {key}"
        
        # Check fallback keys
        for key, value in correlation_data.items():
            if value is None:
                return f"Missing required correlation key: {key}"
        
        return f"Missing required correlation key: {', '.join(configured_keys)}"
    
    def _get_correlation_dict(self, correlation: Correlation | dict[str, Any]) -> dict[str, Any]:
        """Get correlation data as a dictionary.
        
        Args:
            correlation: The correlation object or dict.
        
        Returns:
            A dictionary of correlation key/value pairs.
        """
        if isinstance(correlation, Correlation):
            return correlation.model_dump()
        elif isinstance(correlation, dict):
            return correlation
        return {}
    
    def _derive_workflow_type(self, event: Event) -> str:
        """Derive the workflow type from the event type.
        
        e.g., "order.created" -> "order"
        """
        return event.event_type.split(".")[0] if "." in event.event_type else event.event_type
    
    def _evict_if_needed(self) -> None:
        """Evict the oldest workflow if we've exceeded the max count."""
        while len(self._workflows) > self.max_workflows:
            self._workflows.popitem(last=False)