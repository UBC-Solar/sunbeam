import threading
import time
from collections.abc import Callable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import Event, EventStatus, Sample

# How stale an event's count may be while a worker is writing it
REFRESH_INTERVAL_S = 5.0


class SampleCounts:
    """
    How many samples each event has, for the dashboard's events table.

    Counting a big event's rows isn't free, and the dashboard asks every few seconds, so each count is kept until it
    can have changed: an event being written (ONGOING) is recounted at most every REFRESH_INTERVAL_S, and any other
    event only when its status changes (a run starting or finishing).
    """

    def __init__(self, monotonic: Callable[[], float] = time.monotonic):
        self._monotonic = monotonic
        self._lock = threading.Lock()
        # event id -> (status when counted, count, when counted)
        self._counts: dict[int, tuple[EventStatus, int, float]] = {}

    def count(self, db: Session, event: Event) -> int:
        with self._lock:
            cached = self._counts.get(event.id)
            now = self._monotonic()

            if cached is not None:
                status, count, counted_at = cached
                fresh = status == EventStatus.ONGOING and now - counted_at < REFRESH_INTERVAL_S
                if status == event.status and (status != EventStatus.ONGOING or fresh):
                    return count

            count = db.scalar(select(func.count()).select_from(Sample).where(Sample.event_id == event.id)) or 0
            self._counts[event.id] = (event.status, count, now)
            return count
