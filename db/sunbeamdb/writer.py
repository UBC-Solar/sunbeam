from collections.abc import Iterator

import numpy as np
import pandas as pd
from data_tools.collections import TimeSeries
from sqlalchemy import Engine, delete, insert, select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import Event, EventStatus, Sample, Signal
from state.frame import FrameView

# The sample columns every row gives, in this order. Taken from the model, so renaming one fails here, not in the
# database.
SAMPLE_COLUMNS = (
    Sample.__table__.c.event_id,
    Sample.__table__.c.signal_id,
    Sample.__table__.c.ts,
    Sample.__table__.c.value_f64,
)

# Postgres's bulk-loading command: "read rows for these columns from the client" (see EventWriter)
COPY_SAMPLES = f"COPY {Sample.__tablename__} ({', '.join(column.name for column in SAMPLE_COLUMNS)}) FROM STDIN"

SampleRow = tuple[int, int, object, float]  # event_id, signal_id, ts (a datetime), value_f64


class EventWriter:
    """
    Writes one event's samples: what the worker's stages (and, in a replay, its ingress) produce.

    Samples are loaded with Postgres's COPY rather than INSERT. Postgres executes an INSERT one row at a time, even
    when SQLAlchemy sends thousands at once, and that is the limit on how fast a replay can run (about 20k rows/s,
    whatever the batching). COPY streams the rows into the table and is 3-4 times faster. SQLAlchemy has no COPY
    construct, so _write_rows uses the psycopg connection underneath SQLAlchemy's for that one statement; everything
    else here is ordinary SQLAlchemy.
    """

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
        """ Writes every value in the frames, in one transaction. """
        if frames:
            self._write_rows(self._rows(frames))

    def _rows(self, frames: list[FrameView]) -> Iterator[SampleRow]:
        """
        One row per value: a plain value is the sample at its frame's timestamp, while a TimeSeries (a replay's
        fetched telemetry) holds a whole signal's samples, each at its own timestamp. Generated lazily, since a
        TimeSeries can hold millions.
        """
        for frame in frames:
            for signal, value in frame:
                signal_id = self._signal_names_to_id[signal]

                if isinstance(value, TimeSeries):
                    timestamps = pd.to_datetime(value.unix_x_axis, unit="s", utc=True).to_pydatetime()
                    values = np.asarray(value, dtype=float).tolist()
                    for ts, sample in zip(timestamps, values, strict=True):
                        yield self._event_id, signal_id, ts, sample
                else:
                    yield self._event_id, signal_id, frame.timestamp, float(value)

    def _write_rows(self, rows: Iterator[SampleRow]) -> None:
        """
        Insert the rows into sample with COPY, in one transaction.

        `engine.begin()` works as for any query: it commits when the block ends (or rolls back if it raises) and
        returns the connection to the pool. Inside it, `connection.connection.driver_connection` is the psycopg
        connection SQLAlchemy is using, and psycopg's `cursor.copy()` sends each row given to `write_row`.

        The default tests run on SQLite, which has no COPY; there, the rows go in with an ordinary bulk insert().
        """
        with self._engine.begin() as connection:
            if connection.dialect.name != "postgresql":
                columns = [column.name for column in SAMPLE_COLUMNS]
                values = [dict(zip(columns, row)) for row in rows]
                if values:
                    connection.execute(insert(Sample), values)
                return

            psycopg_connection = connection.connection.driver_connection
            assert psycopg_connection is not None, "COPY needs an open psycopg connection"

            with psycopg_connection.cursor() as cursor, cursor.copy(COPY_SAMPLES) as copy:
                for row in rows:
                    copy.write_row(row)
