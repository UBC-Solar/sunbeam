from sqlalchemy import select, insert, Engine
from sqlalchemy.orm import Session
from db.sunbeamdb.models import Event, EventStatus, Signal, AlignedSample
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

            # if event.status == EventStatus.PROCESSED or event.status == EventStatus.ONGOING:
            #     if event.status == EventStatus.PROCESSED and reprocess:
            #         warnings.warn("Reprocessing...")
            #         # Probably wipe it here
            #     else:
            #         raise ValueError("Event has already been processed!")

            event.status = EventStatus.ONGOING
            session.commit()

            self._event_name = event_name

    def write_frame(self, frame: FrameView):
        self.write_frames([frame])

    def write_frames(self, frames: list[FrameView]):
        rows = [
            {
                "event_id": self._event_id,
                "ts": frame.timestamp,
                "signal_id": self._signal_names_to_id[signal],
                "value_f64": value,
            }
            for frame in frames
            for signal, value in frame
        ]

        if not rows:
            return

        with Session(self._engine) as session:
            session.execute(insert(AlignedSample), rows)
            session.commit()

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