import numpy as np
import pandas as pd
from data_tools.collections import TimeSeries
from sqlalchemy import Engine, delete, insert, select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import AlignedSample, Event, EventStatus, Signal
from state.frame import FrameView


class EventWriter:
    def __init__(self, event_name: str, engine: Engine, reprocess: bool = False):
        self._engine = engine
        self._event_name = event_name
        self._signal_names_to_id: dict[str, int] = {}

        self._event_id = None
        self._read_event_id()
        self._read_signal_ids()

        with Session(self._engine) as session:
            event = session.execute(select(Event).where(Event.name == self._event_name)).scalar_one_or_none()

            # Bulk delete directly on the database, so reprocessing doesn't duplicate samples
            if reprocess:
                session.execute(delete(AlignedSample).where(AlignedSample.event_id == self._event_id))

            event.status = EventStatus.ONGOING
            session.commit()

            self._event_name = event_name

    def write_frame(self, frame: FrameView):
        self.write_frames([frame])

    def write_frames(self, frames: list[FrameView]):
        """ Writes scalar values with one bulk insert, and every TimeSeries value with its own COPY. """
        rows = []

        for frame in frames:
            for signal, value in frame:
                if isinstance(value, TimeSeries):
                    self.write_series(signal, value)
                else:
                    rows.append({
                        "event_id": self._event_id,
                        "ts": frame.timestamp,
                        "signal_id": self._signal_names_to_id[signal],
                        "value_f64": float(value),
                    })

        if not rows:
            return

        with Session(self._engine) as session:
            session.execute(insert(AlignedSample), rows)
            session.commit()

    def write_series(self, signal: str, series: TimeSeries):
        """ Bulk-loads every sample of a TimeSeries with COPY.

        SQLAlchemy has no COPY construct, and its bulk insert() is ~2x slower here and holds the GIL long
        enough to noticeably slow the compute thread, so this drops to psycopg's COPY on a SQLAlchemy-managed
        connection: engine.begin() still owns the transaction (commit/rollback) and returning it to the pool.
        """
        signal_id = self._signal_names_to_id[signal]
        timestamps = pd.to_datetime(series.unix_x_axis, unit="s", utc=True).to_pydatetime()
        values = np.asarray(series, dtype=float).tolist()

        with self._engine.begin() as connection:
            with connection.connection.driver_connection.cursor() as cursor, cursor.copy(
                f"COPY {AlignedSample.__tablename__} (event_id, signal_id, ts, value_f64) FROM STDIN"
            ) as copy:
                for ts, value in zip(timestamps, values, strict=True):
                    copy.write_row((self._event_id, signal_id, ts, value))

    def __del__(self):
        self.close()

    def close(self):
        with Session(self._engine) as session:
            event = session.execute(select(Event).where(Event.name == self._event_name)).scalar_one_or_none()

            event.status = EventStatus.PROCESSED
            session.commit()

    def _read_event_id(self):
        with Session(self._engine) as session:
            event = session.execute(select(Event).where(Event.name == self._event_name)).scalar_one_or_none()
            self._event_id = event.id

    def _read_signal_ids(self):
        with Session(self._engine) as session:
            signals = session.execute(select(Signal).where(Signal.event_id == self._event_id)).scalars()

            for signal in signals:
                self._signal_names_to_id[signal.name] = signal.id
