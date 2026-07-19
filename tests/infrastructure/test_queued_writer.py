import threading

from db.sunbeamdb.queued_writer import QueuedEventWriter
from tests.infrastructure.conftest import wait_until


class FakeBatchWriter:
    """Records every batch handed to write_frames."""

    def __init__(self):
        self.batches = []
        self.closed = False
        self._got_batch = threading.Event()

    def write_frame(self, frame):
        self.write_frames([frame])

    def write_frames(self, frames):
        self.batches.append(list(frames))
        self._got_batch.set()

    def close(self):
        self.closed = True

    def wait_for_batch(self, timeout_s: float = 2.0) -> bool:
        return self._got_batch.wait(timeout_s)


class TestQueuedEventWriter:
    def test_flushes_when_batch_size_reached(self):
        inner = FakeBatchWriter()
        writer = QueuedEventWriter(inner, batch_size=3, flush_interval_s=60.0)

        try:
            for i in range(3):
                writer.write_frame(f"frame-{i}")

            assert inner.wait_for_batch()
            assert inner.batches[0] == ["frame-0", "frame-1", "frame-2"]
        finally:
            writer.close()

    def test_flushes_partial_batch_after_interval(self):
        inner = FakeBatchWriter()
        writer = QueuedEventWriter(inner, batch_size=1000, flush_interval_s=0.05)

        try:
            writer.write_frame("only-frame")

            assert inner.wait_for_batch()
            assert inner.batches == [["only-frame"]]
        finally:
            writer.close()

    def test_close_flushes_pending_and_closes_inner_writer(self):
        inner = FakeBatchWriter()
        writer = QueuedEventWriter(inner, batch_size=1000, flush_interval_s=60.0)

        writer.write_frame("pending-1")
        writer.write_frame("pending-2")
        writer.close()

        assert inner.batches == [["pending-1", "pending-2"]]
        assert inner.closed
        assert not writer._thread.is_alive()

    def test_close_is_idempotent(self):
        inner = FakeBatchWriter()
        writer = QueuedEventWriter(inner, batch_size=10, flush_interval_s=0.05)

        writer.close()
        writer.close()

        assert inner.closed

    def test_frames_never_lost_across_many_batches(self):
        inner = FakeBatchWriter()
        writer = QueuedEventWriter(inner, batch_size=7, flush_interval_s=0.01)

        total = 100
        for i in range(total):
            writer.write_frame(i)
        writer.close()

        flushed = [frame for batch in inner.batches for frame in batch]
        assert flushed == list(range(total))

    def test_no_empty_flush_on_idle_interval(self):
        inner = FakeBatchWriter()
        writer = QueuedEventWriter(inner, batch_size=10, flush_interval_s=0.01)

        # Give the flush loop a few idle intervals with nothing queued.
        assert not inner.wait_for_batch(timeout_s=0.1)
        writer.close()

        assert inner.batches == []
