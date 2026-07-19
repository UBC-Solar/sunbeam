import queue
import threading
import time
from typing import TYPE_CHECKING

from state.frame import FrameView

# Typing-only: importing pipeline.protocols at runtime would be circular,
# since pipeline/__init__ imports the executor, which imports this module.
if TYPE_CHECKING:
    from pipeline.protocols import BatchFrameWriter

_STOP = object()


class QueuedEventWriter:
    """
    Decorates a BatchFrameWriter with a bounded queue and a background thread
    that batches frames, so the scheduler thread never blocks on the database.
    """

    def __init__(self, event_writer: "BatchFrameWriter", batch_size: int = 1000, flush_interval_s: float = 0.1):
        self._event_writer = event_writer
        self._queue: queue.Queue = queue.Queue(maxsize=10_000)
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._closed = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def write_frame(self, frame: FrameView):
        # Fast path for scheduler thread
        self._queue.put(frame)

    def close(self, timeout_s: float = 5.0):
        """
        Flush anything still queued, stop the background thread, and close the
        underlying writer. Safe to call more than once.
        """
        if self._closed:
            return
        self._closed = True

        self._queue.put(_STOP)
        self._thread.join(timeout=timeout_s)
        self._event_writer.close()

    def _run(self):
        pending: list[FrameView] = []
        last_flush = time.monotonic()

        while True:
            timeout = max(0.0, self._flush_interval_s - (time.monotonic() - last_flush))

            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                item = None

            if item is _STOP:
                if pending:
                    self._event_writer.write_frames(pending)
                return

            if item is None:
                if pending:
                    self._event_writer.write_frames(pending)
                    pending = []
                    last_flush = time.monotonic()
                continue

            pending.append(item)

            if len(pending) >= self._batch_size:
                self._event_writer.write_frames(pending)
                pending = []
                last_flush = time.monotonic()