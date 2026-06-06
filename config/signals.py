from sqlalchemy import select, Engine
from sqlalchemy.orm import Session
from db.sunbeamdb.models import Event, Signal
from db.sunbeamdb.seed_data import collect_signal_metadata_for_event, get_or_create_signal


class SignalManager:
    @staticmethod
    def sync_signals(engine: Engine):
        with Session(engine) as session:
            events = session.execute(select(Event)).scalars().all()
            for event in events:
                SignalManager.sync_signals_for_event(event, session)

    @staticmethod
    def sync_signals_for_event(event: Event, session: Session):
        signals = collect_signal_metadata_for_event(event)

        created_signals: list[Signal] = []

        for signal_data in signals:
            signal = get_or_create_signal(session, **signal_data)
            created_signals.append(signal)

        session.commit()
