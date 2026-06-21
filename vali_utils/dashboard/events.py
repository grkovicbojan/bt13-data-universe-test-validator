"""
Evaluation event bus for real-time dashboard streaming.

The MinerEvaluator publishes events here; the dashboard SSE endpoint
broadcasts them to connected browser clients.
"""

import json
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional
import threading as _threading


@dataclass
class EventSubscriber:
    """Per-SSE-client cursor so no published event is skipped."""

    notify: _threading.Event = field(default_factory=_threading.Event)
    cursor: int = 0


@dataclass
class EvaluationEvent:
    """A single evaluation lifecycle event."""

    event_type: str
    uid: int
    hotkey: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    data: Dict[str, Any] = field(default_factory=dict)


class EvaluationEventBus:
    """Thread-safe ring buffer with subscriber notification for SSE."""

    MAX_EVENTS = 500

    def __init__(self):
        self._lock = threading.RLock()
        self._events: Deque[EvaluationEvent] = deque(maxlen=self.MAX_EVENTS)
        self._subscribers: List[EventSubscriber] = []

    def publish(
        self,
        event_type: str,
        uid: int,
        hotkey: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> EvaluationEvent:
        event = EvaluationEvent(
            event_type=event_type,
            uid=uid,
            hotkey=hotkey,
            data=data or {},
        )
        with self._lock:
            self._events.append(event)
            for sub in self._subscribers:
                sub.notify.set()
        return event

    def get_recent(self, limit: int = 100) -> List[Dict]:
        with self._lock:
            events = list(self._events)[-limit:]
        return [asdict(e) for e in events]

    def clear(self) -> int:
        """Remove all buffered events. Returns count removed."""
        with self._lock:
            count = len(self._events)
            self._events.clear()
            for sub in self._subscribers:
                sub.cursor = 0
                sub.notify.clear()
            return count

    def subscribe(self) -> EventSubscriber:
        """Register an SSE subscriber starting at the current event tail."""
        with self._lock:
            sub = EventSubscriber(cursor=len(self._events))
            self._subscribers.append(sub)
            return sub

    def sync_subscriber(self, subscriber: EventSubscriber) -> None:
        """Advance subscriber cursor to the tail (after bootstrap replay)."""
        with self._lock:
            subscriber.cursor = len(self._events)
            subscriber.notify.clear()

    def drain_subscriber(self, subscriber: EventSubscriber) -> List[Dict]:
        """Return all events published since the subscriber's last drain."""
        with self._lock:
            events = list(self._events)
            start = min(subscriber.cursor, len(events))
            subscriber.cursor = len(events)
            subscriber.notify.clear()
            return [asdict(e) for e in events[start:]]

    def wait_for_event(
        self, subscriber: EventSubscriber, timeout: float = 30.0
    ) -> bool:
        return subscriber.notify.wait(timeout=timeout)

    def to_sse(self, event: EvaluationEvent) -> str:
        payload = json.dumps(asdict(event), default=str)
        return f"data: {payload}\n\n"


# Module-level singleton.
_event_bus: Optional[EvaluationEventBus] = None


def get_event_bus() -> EvaluationEventBus:
    global _event_bus
    if _event_bus is None:
        _event_bus = EvaluationEventBus()
    return _event_bus
