from datetime import datetime, timedelta, timezone

import pytest

from db.sunbeamdb.models import AlignedSample
from server.services.data_service import (
    get_signal,
    list_signals,
    query_samples,
    resolve_time_window,
    stream_batches,
)

NOW = datetime(2026, 7, 20, 12, 0, 0, tzinfo=timezone.utc)
BASE = datetime(2026, 7, 20, 11, 0, 0, tzinfo=timezone.utc)


class TestResolveTimeWindow:
    def test_between_two_instants(self):
        start, end = resolve_time_window(BASE, NOW, None)
        assert (start, end) == (BASE, NOW)

    def test_since_start_until_now(self):
        start, end = resolve_time_window(BASE, None, None, now=NOW)
        assert (start, end) == (BASE, NOW)

    def test_last_seconds(self):
        start, end = resolve_time_window(None, None, 30.0, now=NOW)
        assert end == NOW
        assert start == NOW - timedelta(seconds=30)

    def test_no_mode_selected_rejected(self):
        with pytest.raises(ValueError, match="either start"):
            resolve_time_window(None, None, None)

    def test_both_modes_rejected(self):
        with pytest.raises(ValueError, match="cannot be combined"):
            resolve_time_window(BASE, None, 30.0)

    def test_negative_last_seconds_rejected(self):
        with pytest.raises(ValueError, match="positive"):
            resolve_time_window(None, None, -5.0)

    def test_naive_start_rejected(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            resolve_time_window(BASE.replace(tzinfo=None), None, None)

    def test_naive_end_rejected(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            resolve_time_window(BASE, NOW.replace(tzinfo=None), None)

    def test_inverted_window_rejected(self):
        with pytest.raises(ValueError, match="before end"):
            resolve_time_window(NOW, BASE, None)


def seed_samples(session_factory, seeded_event, *, count=10, signal="speed", start=BASE):
    """count samples, 1 s apart, values 0..count-1."""
    signal_id = seeded_event.signal_ids[signal]
    with session_factory() as session:
        for i in range(count):
            session.add(
                AlignedSample(
                    event_id=seeded_event.event_id,
                    ts=start + timedelta(seconds=i),
                    signal_id=signal_id,
                    value_f64=float(i),
                )
            )
        session.commit()


@pytest.fixture
def db(session_factory):
    session = session_factory()
    yield session
    session.close()


class TestSignalLookup:
    def test_get_signal_resolves_by_names(self, db, seeded_event):
        signal = get_signal(db, seeded_event.event_name, "speed")
        assert signal.id == seeded_event.signal_ids["speed"]

    def test_unknown_event_raises(self, db, seeded_event):
        with pytest.raises(LookupError, match="No event named"):
            get_signal(db, "nope", "speed")

    def test_unknown_signal_raises(self, db, seeded_event):
        with pytest.raises(LookupError, match="no signal named"):
            get_signal(db, seeded_event.event_name, "nope")

    def test_list_signals(self, db, seeded_event):
        signals = list_signals(db, seeded_event.event_name)
        assert [signal.name for signal in signals] == ["power", "speed"]


class TestQuerySamples:
    def test_window_is_half_open(self, db, session_factory, seeded_event):
        seed_samples(session_factory, seeded_event)
        signal = get_signal(db, seeded_event.event_name, "speed")

        # [BASE+2s, BASE+5s): samples at +2, +3, +4 - start in, end out.
        timestamps, values, truncated = query_samples(
            db, signal, BASE + timedelta(seconds=2), BASE + timedelta(seconds=5)
        )

        assert values == [2.0, 3.0, 4.0]
        assert truncated is False
        assert timestamps == sorted(timestamps)

    def test_empty_window(self, db, session_factory, seeded_event):
        seed_samples(session_factory, seeded_event)
        signal = get_signal(db, seeded_event.event_name, "speed")

        timestamps, values, truncated = query_samples(
            db, signal, BASE + timedelta(hours=5), BASE + timedelta(hours=6)
        )

        assert (timestamps, values, truncated) == ([], [], False)

    def test_truncation_keeps_most_recent(self, db, session_factory, seeded_event):
        seed_samples(session_factory, seeded_event, count=10)
        signal = get_signal(db, seeded_event.event_name, "speed")

        timestamps, values, truncated = query_samples(
            db, signal, BASE, BASE + timedelta(seconds=60), limit=4
        )

        assert truncated is True
        assert values == [6.0, 7.0, 8.0, 9.0]

    def test_other_signals_excluded(self, db, session_factory, seeded_event):
        seed_samples(session_factory, seeded_event, signal="speed")
        seed_samples(session_factory, seeded_event, signal="power")
        signal = get_signal(db, seeded_event.event_name, "speed")

        timestamps, values, _ = query_samples(
            db, signal, BASE, BASE + timedelta(seconds=60)
        )

        assert len(values) == 10


class TestStreamBatches:
    def test_meta_first_then_batches_with_advancing_cursor(
        self, session_factory, seeded_event
    ):
        seed_samples(session_factory, seeded_event, count=3)

        batches = stream_batches(
            session_factory,
            event_name=seeded_event.event_name,
            signal_names=["speed", "power"],
            since_us=int(BASE.timestamp() * 1_000_000) - 1,
            max_batches=1,
            sleep=lambda s: None,
        )

        kind, meta, start_cursor = next(batches)
        assert kind == "meta"
        assert meta["speed"]["frequency"] == 10.0
        assert meta["speed"]["unit"] == "unit"
        assert meta["power"]["signal_id"] == seeded_event.signal_ids["power"]

        kind, payload, cursor = next(batches)
        assert kind == "data"
        assert payload["speed"]["values"] == [0.0, 1.0, 2.0]
        assert payload["power"]["values"] == []
        assert cursor > start_cursor

        with pytest.raises(StopIteration):
            next(batches)

    def test_cursor_prevents_duplicates_across_polls(
        self, session_factory, seeded_event
    ):
        seed_samples(session_factory, seeded_event, count=2)

        received = []

        def insert_between_polls(_):
            # Runs after each poll: simulate the worker landing a new sample.
            if len(received) == 1:
                seed_samples(
                    session_factory,
                    seeded_event,
                    count=1,
                    start=BASE + timedelta(seconds=100),
                )

        batches = stream_batches(
            session_factory,
            event_name=seeded_event.event_name,
            signal_names=["speed"],
            since_us=int(BASE.timestamp() * 1_000_000) - 1,
            max_batches=3,
            sleep=insert_between_polls,
        )

        next(batches)  # meta
        for kind, payload, _ in batches:
            if kind == "data":
                received.append(payload["speed"]["values"])

        assert received == [[0.0, 1.0], [0.0]]

    def test_quiet_stream_emits_keepalive(self, session_factory, seeded_event):
        batches = stream_batches(
            session_factory,
            event_name=seeded_event.event_name,
            signal_names=["speed"],
            keepalive_interval_s=0.0,
            max_batches=2,
            sleep=lambda s: None,
        )

        kinds = [kind for kind, _, _ in batches]

        assert kinds == ["meta", "keepalive", "keepalive"]

    def test_quiet_poll_yields_idle_inside_keepalive_interval(
        self, session_factory, seeded_event
    ):
        # Every poll must yield SOMETHING - a silent internal loop would make
        # the stream uninterruptible between keepalives.
        batches = stream_batches(
            session_factory,
            event_name=seeded_event.event_name,
            signal_names=["speed"],
            keepalive_interval_s=1000.0,
            max_batches=2,
            sleep=lambda s: None,
        )

        kinds = [kind for kind, _, _ in batches]

        assert kinds == ["meta", "idle", "idle"]

    def test_unknown_signal_raises_before_any_yield(self, session_factory, seeded_event):
        batches = stream_batches(
            session_factory,
            event_name=seeded_event.event_name,
            signal_names=["nope"],
        )

        with pytest.raises(LookupError, match="no signal named"):
            next(batches)
