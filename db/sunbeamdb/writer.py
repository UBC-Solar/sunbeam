import numpy as np
import pandas as pd
from data_tools.collections import TimeSeries
from sqlalchemy import Engine, delete, insert, select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import Event, EventStatus, Sample, Signal
from state.frame import FrameView


class EventWriter:
    def __init__(self, event_name: str, engine: Engine, reprocess: bool = False):
        """
        Marks the event ONGOING; close() marks it PROCESSED.

        :param reprocess: Delete the event's existing samples first, so rewriting them doesn't collide
        """
        self._engine = engine
        self._event_name = event_name

        with Session(self._engine) as session:
            self._event_id = self._get_event(session).id
            self._signal_names_to_id = {
                signal.name: signal.id
                for signal in session.scalars(select(Signal).where(Signal.event_id == self._event_id))
            }

            if reprocess:
                session.execute(delete(Sample).where(Sample.event_id == self._event_id))

            self._get_event(session).status = EventStatus.ONGOING
            session.commit()

    def close(self):
        with Session(self._engine) as session:
            self._get_event(session).status = EventStatus.PROCESSED
            session.commit()

    def _get_event(self, session: Session) -> Event:
        event = session.execute(select(Event).where(Event.name == self._event_name)).scalar_one_or_none()

        if event is None:
            raise ValueError(f"Event {self._event_name!r} does not exist in the database.")

        return event

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
            session.execute(insert(Sample), rows)
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
            driver_connection = connection.connection.driver_connection
            assert driver_connection is not None, "COPY needs a live psycopg connection"

            with driver_connection.cursor() as cursor, cursor.copy(
                f"COPY {Sample.__tablename__} (event_id, signal_id, ts, value_f64) FROM STDIN"
            ) as copy:
                for ts, value in zip(timestamps, values, strict=True):
                    copy.write_row((self._event_id, signal_id, ts, value))
