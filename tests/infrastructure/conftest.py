import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from config.context import Context, ServiceType
from db.sunbeamdb.models import Base, Event, EventStatus, Signal, Vehicle
from orchestration.control import WorkerControl


TEST_CONFIG = {
    "main": {"default_config": "test"},
    "worker": {
        "sunbeamdb": {
            "test": {
                "database_host_ip": "localhost",
                "database_port": 5432,
                "database_name": "sunbeam_test",
                "database_username": "test",
                "database_password": "test",
            }
        },
        "telemetrydb": {
            "test": {
                "database_url": "http://localhost:8086",
                "bucket": "test-bucket",
                "organization": "test-org",
                "token": "test-token",
                "debug": True,
                "debug_time": "2026-07-01T00:00:00",
            }
        },
        "sunbeam-server": {
            "test": {
                "server_url": "localhost",
                "server_port": 8000,
                "worker_network": None,
            }
        },
    },
}


@pytest.fixture
def test_context():
    """A Context singleton configured from an in-memory dict, torn down after."""
    Context._instance = None
    ctx = Context.from_config(TEST_CONFIG, ServiceType.Worker, "test")
    yield ctx
    Context._instance = None


class FakeClock:
    """
    Deterministic monotonic clock: sleep() advances time instantly instead of
    blocking, so scheduler tests run in microseconds of wall time.
    """

    def __init__(self, start_ns: int = 0):
        self._now_ns = start_ns
        self._lock = threading.Lock()

    def monotonic_ns(self) -> int:
        with self._lock:
            return self._now_ns

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self._now_ns += int(seconds * 1_000_000_000)


@pytest.fixture
def fake_clock():
    return FakeClock()


class FakePipeline:
    """Satisfies RunnablePipeline; records every run timestamp."""

    def __init__(self, name: str, frequency: float, frames_per_run: int = 1,
                 fail_after: Optional[int] = None):
        self.name = name
        self.frequency = frequency
        self.frames_per_run = frames_per_run
        self.fail_after = fail_after
        self.run_timestamps = []

    @property
    def run_count(self) -> int:
        return len(self.run_timestamps)

    def run(self, state, timestamp):
        self.run_timestamps.append(timestamp)

        if self.fail_after is not None and self.run_count > self.fail_after:
            raise RuntimeError(f"{self.name} failed")

        for i in range(self.frames_per_run):
            yield (self.name, self.run_count, i)


class RecordingWriter:
    """FrameWriter that records frames; can trigger a callback per write."""

    def __init__(self, on_write=None):
        self.frames = []
        self.closed = False
        self._on_write = on_write

    def write_frame(self, frame):
        self.frames.append(frame)
        if self._on_write is not None:
            self._on_write(len(self.frames))

    def close(self):
        self.closed = True


class FakeControl(WorkerControl):
    def __init__(self):
        self.started = False
        self.stopped = False
        self.stages = []
        self.messages = []
        self.completions = []
        self._stop = threading.Event()

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def should_stop(self) -> bool:
        return self._stop.is_set()

    def request_stop(self, reason=None):
        self._stop.set()
        if reason is not None:
            self.messages.append(reason)

    def set_stage(self, stage):
        self.stages.append(stage)

    def set_message(self, message):
        self.messages.append(message)

    def heartbeat_now(self, *, status: str = "running"):
        pass

    def report_metrics(self, payload):
        pass

    def complete(self, *, success, message=None):
        self.completions.append((success, message))


class FakeOutputManager:
    def __init__(self):
        self.ticks = 0
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, tb):
        self.exited = True
        return False

    def on_tick(self):
        self.ticks += 1


def wait_until(predicate, timeout_s: float = 2.0, interval_s: float = 0.005) -> bool:
    """Poll predicate until true or timeout; returns whether it became true."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return predicate()


def naive_utcnow() -> datetime:
    """
    Current UTC time without tzinfo. SQLite has no timezone-aware storage, so
    DB-backed tests keep every datetime naive to make arithmetic against
    stored values valid. (Postgres-marked tests can exercise aware datetimes.)
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture
def engine():
    """
    In-memory SQLite engine with the full schema. StaticPool shares the single
    in-memory database across connections and threads (e.g. the
    QueuedEventWriter flush thread).
    """
    engine = create_engine(
        "sqlite+pysqlite://",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


@dataclass
class SeededEvent:
    event_id: int
    event_name: str
    vehicle_id: int
    signal_ids: dict[str, int]


def seed_basic_event(engine) -> SeededEvent:
    """Insert a vehicle, an unprocessed event, and two signals ('speed', 'power')."""
    with Session(engine) as session:
        vehicle = Vehicle(name="TestVehicle", description="test")
        session.add(vehicle)
        session.flush()

        event = Event(
            name="test-event",
            vehicle_id=vehicle.id,
            starts_at=datetime(2026, 7, 1, 12, 0, 0),
            ends_at=None,
            pipeline_edition="v3_0",
            status=EventStatus.UNPROCESSED,
            description="test event",
        )
        session.add(event)
        session.flush()

        signal_ids = {}
        for name in ("speed", "power"):
            signal = Signal(
                name=name,
                unit="unit",
                source="derived",
                frequency=10.0,
                event_id=event.id,
            )
            session.add(signal)
            session.flush()
            signal_ids[name] = signal.id

        session.commit()

        return SeededEvent(
            event_id=event.id,
            event_name=event.name,
            vehicle_id=vehicle.id,
            signal_ids=signal_ids,
        )


@pytest.fixture
def seeded_event(engine) -> SeededEvent:
    return seed_basic_event(engine)


@pytest.fixture
def api_client(session_factory):
    """
    TestClient over create_app with the SQLite session and a fake Docker
    client injected. Imports lazily so environments without the server test
    deps can still collect this conftest.
    """
    pytest.importorskip("fastapi")
    pytest.importorskip("docker")

    from fastapi.testclient import TestClient

    from server.deps import get_db, get_db_session_factory
    from server.main import create_app
    from server.routes.workers import get_worker_service
    from server.services.worker_service import WorkerService
    from tests.infrastructure.test_worker_service import FakeDockerClient

    app = create_app(lifespan=None)

    def override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    service = WorkerService(FakeDockerClient(), worker_network=None)

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_db_session_factory] = lambda: session_factory
    app.dependency_overrides[get_worker_service] = lambda: service

    with TestClient(app) as client:
        yield client
