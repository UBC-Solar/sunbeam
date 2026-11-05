from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from db.sunbeamdb.models import Event
from db.sunbeamdb.seed_data import collect_signal_metadata_for_event, get_or_create_signal


class SignalManager:
    @staticmethod
    def sync_signals(engine: Engine) -> None:
        with Session(engine) as session:
            for event in session.scalars(select(Event)).all():
                for signal_data in collect_signal_metadata_for_event(event):
                    get_or_create_signal(session, **signal_data)
            session.commit()
