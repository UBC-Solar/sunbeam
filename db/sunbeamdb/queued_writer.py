import queue
import threading
import time
from typing import TYPE_CHECKING

from state.frame import FrameView

# Typing-only: importing pipeline.protocols at runtime would be circular,
# since pipeline/__init__ imports the executor, which imports this module.
if TYPE_CHECKING:
    from pipeline.protocols import BatchFrameWriter


class _Stop:
    """ Queue sentinel telling the writer thread to flush and exit. """


_STOP = _Stop()


class WriterThreadError(RuntimeError):
    """ Raised on the caller's thread when the background writer thread has died. """


class QueuedEventWriter:
    """
    Decorates a BatchFrameWriter with a bounded queue and a background thread
    that batches frames, so the scheduler thread never blocks on the database.

    Frames are never dropped: if the database cannot keep up and the queue
    fills, write_frame blocks the caller. stats() exposes queue depth, the
    high-water mark, and flush timings so that pressure is visible long before
    it reaches that point. If the writer thread dies, the next write_frame or
    close raises WriterThreadError instead.
    """

    def __init__(self, event_writer: "BatchFrameWriter", batch_size: int = 1000, flush_interval_s: float = 0.1):
        self._event_writer = event_writer
        self._queue: queue.Queue[FrameView | _Stop] = queue.Queue(maxsize=10_000)
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._error: BaseException | None = None
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
        self._put(frame)

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

    def close(self, timeout_s: float | None = None):
        """
        Flush anything still queued, stop the background thread, and close the
        underlying writer. Safe to call more than once.

        :param float | None timeout_s: How long to wait for the flush; None (the default) waits until every
            frame is written, since a replay can hand over a whole event's worth of data at the end
        :raises WriterThreadError: The writer thread died, so some frames were not written
        """
        if self._closed:
            return
        self._closed = True

        if self._thread.is_alive():
            self._put(_STOP)
            self._thread.join(timeout=timeout_s)

        self._raise_if_failed()
        self._event_writer.close()

    def _put(self, item: FrameView | _Stop):
        # A plain put() would block forever on a full queue if the writer thread has died, so keep checking on it
        while True:
            self._raise_if_failed()
            try:
                self._queue.put(item, timeout=1.0)
                return
            except queue.Full:
                continue

    def _raise_if_failed(self):
        if self._error is not None:
            raise WriterThreadError("Database writer thread failed; queued frames were not written") from self._error

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
        try:
            self._write_until_stopped()
        except BaseException as e:
            self._error = e
            raise

    def _write_until_stopped(self):
        pending: list[FrameView] = []
        last_flush = time.monotonic()

        while True:
            timeout = max(0.0, self._flush_interval_s - (time.monotonic() - last_flush))

            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                item = None

            if isinstance(item, _Stop):
                if pending:
                    self._flush(pending)
                return

            if item is None:
                if pending:
                    self._flush(pending)
                    pending = []

                # Reset even when nothing was flushed, otherwise the timeout stays at 0 and this loop busy-spins
                last_flush = time.monotonic()
                continue

            pending.append(item)

            if len(pending) >= self._batch_size:
                self._flush(pending)
                pending = []
                last_flush = time.monotonic()
