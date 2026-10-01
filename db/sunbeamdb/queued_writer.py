import queue
import threading
import time

import numpy as np
import pandas as pd
from data_tools.collections import TimeSeries
from sqlalchemy import insert
from sqlalchemy.orm import Session

from db.sunbeamdb.models import AlignedSample
from db.sunbeamdb.writer import EventWriter
from state.frame import FrameView


class _Stop:
    """ Queue sentinel telling the writer thread to flush and exit. """


_STOP = _Stop()


class QueuedEventWriter:
    def __init__(self, event_writer: EventWriter, batch_size: int = 1000, flush_interval_s: float = 0.1):
        self._event_writer = event_writer
        self._queue: queue.Queue[FrameView | _Stop] = queue.Queue(maxsize=10_000)
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def write_frame(self, frame: FrameView):
        # Fast path for scheduler thread
        self._queue.put(frame)

    def close(self):
        """ Flushes every queued frame to the database and stops the writer thread. Blocks until done. """
        if not self._thread.is_alive():  # Writer already died; putting could block forever on a full queue
            return
        self._queue.put(_STOP)
        self._thread.join()

    def _run(self):
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
                    self._flush(pending)
                return

            if frame is None:
                if pending:
                    self._flush(pending)
                    pending.clear()

                # Reset even when nothing was flushed, otherwise the timeout stays at 0 and this loop busy-spins
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
                if isinstance(value, TimeSeries):
                    self._copy_series(signal, value)

                elif isinstance(value, float):
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

    def _copy_series(self, signal: str, series: TimeSeries):
        """ Bulk-loads every sample of a TimeSeries with COPY.

        SQLAlchemy has no COPY construct, and its bulk insert() is ~2x slower here and holds the GIL long
        enough to noticeably slow the compute thread, so this drops to psycopg's COPY on a SQLAlchemy-managed
        connection: engine.begin() still owns the transaction (commit/rollback) and returning it to the pool.
        """
        event_id = self._event_writer._event_id
        signal_id = self._event_writer._signal_names_to_id[signal]
        timestamps = pd.to_datetime(series.unix_x_axis, unit="s", utc=True).to_pydatetime()
        values = np.asarray(series, dtype=float).tolist()

        table = AlignedSample.__table__
        columns = ", ".join(column.name for column in (table.c.event_id, table.c.signal_id, table.c.ts, table.c.value_f64))

        with self._event_writer._engine.begin() as connection:
            with connection.connection.driver_connection.cursor() as cursor, cursor.copy(
                f"COPY {AlignedSample.__tablename__} ({columns}) FROM STDIN"
            ) as copy:
                for ts, value in zip(timestamps, values, strict=True):
                    copy.write_row((event_id, signal_id, ts, value))