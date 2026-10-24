import queue
import threading
import time

from db.sunbeamdb.writer import EventWriter
from state.frame import FrameView


class _Stop:
    """ Queue sentinel telling the writer thread to flush and exit. """


_STOP = _Stop()


class WriterThreadError(RuntimeError):
    """ Raised on the caller's thread when the background writer thread has died. """


class QueuedEventWriter:
    def __init__(self, event_writer: EventWriter, batch_size: int = 1000, flush_interval_s: float = 0.1):
        self._event_writer = event_writer
        self._queue: queue.Queue[FrameView | _Stop] = queue.Queue(maxsize=10_000)
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._error: BaseException | None = None
        self._closed = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def write_frame(self, frame: FrameView):
        # Fast path for scheduler thread
        self._put(frame)

    def close(self):
        """ Flushes every queued frame to the database and stops the writer thread. Blocks until done.

        :raises WriterThreadError: The writer thread died, so some frames were not written
        """
        if self._closed:
            return
        self._closed = True

        if self._thread.is_alive():
            self._put(_STOP)
            self._thread.join()

        self._raise_if_failed()

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
                frame = self._queue.get(timeout=timeout)
            except queue.Empty:
                frame = None

            if isinstance(frame, _Stop):
                if pending:
                    self._event_writer.write_frames(pending)
                return

            if frame is None:
                if pending:
                    self._event_writer.write_frames(pending)
                    pending.clear()

                # Reset even when nothing was flushed, otherwise the timeout stays at 0 and this loop busy-spins
                last_flush = time.monotonic()
                continue

            pending.append(frame)

            if len(pending) >= self._batch_size:
                self._event_writer.write_frames(pending)
                pending.clear()
                last_flush = time.monotonic()
