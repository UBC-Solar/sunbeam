import threading
import uuid
from typing import Optional


class MetricsCache:
    """
    Holds the most recent timing snapshot reported by each active worker.

    This is intentionally in-memory only: metrics are a live/ephemeral view of
    a running worker, not a historical record. Restarting the server or
    a worker reaching a terminal state simply drops the entry.
    """

    _lock = threading.Lock()
    _store: dict[uuid.UUID, dict] = {}

    @classmethod
    def set(cls, worker_id: uuid.UUID, payload: dict) -> None:
        with cls._lock:
            cls._store[worker_id] = payload

    @classmethod
    def get(cls, worker_id: uuid.UUID) -> Optional[dict]:
        with cls._lock:
            return cls._store.get(worker_id)

    @classmethod
    def clear(cls, worker_id: uuid.UUID) -> None:
        with cls._lock:
            cls._store.pop(worker_id, None)