import queue
import threading
import time
from sqlalchemy import insert
from sqlalchemy.orm import Session
from db.sunbeamdb.writer import EventWriter
from state.frame import FrameView
from db.sunbeamdb.models import AlignedSample


class QueuedEventWriter:
    def __init__(self, event_writer: EventWriter, batch_size: int = 1000, flush_interval_s: float = 0.1):
        self._event_writer = event_writer
        self._queue: queue.Queue[FrameView | None] = queue.Queue(maxsize=10_000)
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def write_frame(self, frame: FrameView):
        # Fast path for scheduler thread
        self._queue.put(frame)

    def _run(self):
        pending: list[FrameView] = []
        last_flush = time.monotonic()

        while True:
            timeout = max(0.0, self._flush_interval_s - (time.monotonic() - last_flush))

            try:
                frame = self._queue.get(timeout=timeout)
            except queue.Empty:
                frame = None

            if frame is None:
                if pending:
                    self._flush(pending)
                    pending.clear()
                    last_flush = time.monotonic()
                continue

            pending.append(frame)

            if len(pending) >= self._batch_size:
                self._flush(pending)
                pending.clear()
                last_flush = time.monotonic()

    def _flush(self, frames: list[FrameView]):
        rows = []

        for frame in frames:
            for signal, value in frame:
                if isinstance(value, float):
                    rows.append({
                        "event_id": self._event_writer._event_id,
                        "ts": frame.timestamp,
                        "signal_id": self._event_writer._signal_names_to_id[signal],
                        "value_f64": value,
                    })

        if not rows:
            return

        with Session(self._event_writer._engine) as session:
            session.execute(insert(AlignedSample), rows)
            session.commit()