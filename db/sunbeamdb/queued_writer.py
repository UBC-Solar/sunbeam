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

    Frames are never dropped: if the database cannot keep up and the queue
    fills, write_frame blocks the caller. stats() exposes queue depth, the
    high-water mark, and flush timings so that pressure is visible long before
    it reaches that point.
    """

    def __init__(self, event_writer: "BatchFrameWriter", batch_size: int = 1000, flush_interval_s: float = 0.1):
        self._event_writer = event_writer
        self._queue: queue.Queue = queue.Queue(maxsize=10_000)
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._closed = False

        self._frames_enqueued = 0
        self._frames_written = 0
        self._batches_flushed = 0
        self._queue_high_water = 0
        self._flush_ns_total = 0
        self._flush_ns_max = 0

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def write_frame(self, frame: FrameView):
        # Fast path for scheduler thread
        self._queue.put(frame)

        self._frames_enqueued += 1
        depth = self._queue.qsize()
        if depth > self._queue_high_water:
            self._queue_high_water = depth

    def stats(self) -> dict:
        batches = self._batches_flushed
        return {
            "queue_depth": self._queue.qsize(),
            "queue_capacity": self._queue.maxsize,
            "queue_high_water": self._queue_high_water,
            "frames_enqueued": self._frames_enqueued,
            "frames_written": self._frames_written,
            "batches_flushed": batches,
            "avg_flush_ms": (self._flush_ns_total / batches / 1e6) if batches else 0.0,
            "max_flush_ms": self._flush_ns_max / 1e6,
        }

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

    def _flush(self, pending: list[FrameView]) -> None:
        start_ns = time.monotonic_ns()
        self._event_writer.write_frames(pending)
        elapsed_ns = time.monotonic_ns() - start_ns

        self._frames_written += len(pending)
        self._batches_flushed += 1
        self._flush_ns_total += elapsed_ns
        if elapsed_ns > self._flush_ns_max:
            self._flush_ns_max = elapsed_ns

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
                    self._flush(pending)
                return

            if item is None:
                if pending:
                    self._flush(pending)
                    pending = []
                    last_flush = time.monotonic()
                continue

            pending.append(item)

            if len(pending) >= self._batch_size:
                self._flush(pending)
                pending = []
                last_flush = time.monotonic()
