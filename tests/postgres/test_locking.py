"""
The SELECT ... FOR UPDATE handshake between WorkerService.heartbeat and
WatchdogService._resolve. Both re-read the WorkerRun row under a row lock and
back off if the other side already committed a terminal status — semantics
SQLite cannot verify because it has no row locks (with_for_update is a no-op
there). These tests hold a real lock on one connection and prove the other
side blocks, then backs off.
"""
import threading
from datetime import datetime, timezone

import pytest

pytest.importorskip("docker")

from db.sunbeamdb.models import WorkerRun, WorkerStatus
from server.services.watchdog_service import WatchdogService
from server.services.worker_service import WorkerService
from tests.infrastructure.conftest import wait_until
from tests.infrastructure.test_worker_service import FakeDockerClient

pytestmark = pytest.mark.postgres

BLOCK_CONFIRM_S = 0.5
THREAD_DEADLINE_S = 10.0


@pytest.fixture
def worker_id(pg_session_factory, pg_seeded_event):
    with pg_session_factory() as session:
        worker = WorkerRun(
            event_id=pg_seeded_event.event_id,
            pipeline_edition="v3_0",
            image_tag="sunbeam-worker:v3_0",
            status=WorkerStatus.RUNNING,
        )
        session.add(worker)
        session.commit()
        return worker.id


def run_in_thread(target) -> threading.Thread:
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread


class TestHeartbeatBacksOffAfterWatchdogWins:
    def test_blocked_heartbeat_sees_terminal_status_and_backs_off(
        self, pg_session_factory, worker_id
    ):
        service = WorkerService(FakeDockerClient(), worker_network=None)
        result = {}

        def heartbeat():
            with pg_session_factory() as db:
                worker = service.heartbeat(
                    db,
                    worker_id=worker_id,
                    status=WorkerStatus.RUNNING,
                    current_stage="compute",
                    status_message="still here",
                    host=None,
                )
                result["status"] = worker.status
                result["heartbeat_at"] = worker.last_heartbeat_at

        # Take the same row lock _resolve takes, emulating a watchdog
        # resolution in flight.
        locker = pg_session_factory()
        try:
            locked = locker.get(WorkerRun, worker_id, with_for_update=True)

            thread = run_in_thread(heartbeat)

            # The heartbeat must be stuck on the row lock. If with_for_update
            # were removed from heartbeat, it would complete here and this
            # assertion would fail.
            thread.join(BLOCK_CONFIRM_S)
            assert thread.is_alive(), (
                "heartbeat did not block on the row lock - FOR UPDATE is not "
                "being taken"
            )

            # The watchdog wins: commit a terminal resolution, releasing the lock.
            locked.status = WorkerStatus.LOST
            locked.status_message = "Worker stopped heartbeating."
            locked.stopped_at = datetime.now(timezone.utc)
            locker.commit()
        finally:
            locker.close()

        thread.join(THREAD_DEADLINE_S)
        assert not thread.is_alive()

        # The heartbeat saw the committed terminal status and refused to
        # resurrect the worker.
        assert result["status"] == WorkerStatus.LOST
        assert result["heartbeat_at"] is None

        with pg_session_factory() as session:
            final = session.get(WorkerRun, worker_id)
            assert final.status == WorkerStatus.LOST
            assert final.last_heartbeat_at is None
            assert final.status_message == "Worker stopped heartbeating."


class TestResolveBacksOffAfterHeartbeatSideWins:
    def test_blocked_resolve_sees_terminal_status_and_backs_off(
        self, pg_session_factory, worker_id
    ):
        watchdog = WatchdogService(
            docker_client=FakeDockerClient(),
            session_factory=pg_session_factory,
        )

        # The stale snapshot _resolve receives from its unlocked sweep read.
        with pg_session_factory() as session:
            stale = session.get(WorkerRun, worker_id)
            session.expunge(stale)

        done = threading.Event()

        def resolve():
            with pg_session_factory() as db:
                watchdog._resolve(
                    db, stale, WorkerStatus.LOST, "Worker stopped heartbeating."
                )
            done.set()

        locker = pg_session_factory()
        try:
            locked = locker.get(WorkerRun, worker_id, with_for_update=True)

            thread = run_in_thread(resolve)

            thread.join(BLOCK_CONFIRM_S)
            assert thread.is_alive(), (
                "_resolve did not block on the row lock - FOR UPDATE is not "
                "being taken"
            )

            # The other side wins while _resolve is blocked: the worker
            # reports completion and commits first.
            locked.status = WorkerStatus.COMPLETED
            locked.status_message = "Pipeline completed."
            locked.stopped_at = datetime.now(timezone.utc)
            locker.commit()
        finally:
            locker.close()

        assert wait_until(done.is_set, timeout_s=THREAD_DEADLINE_S)

        # _resolve re-checked under the lock and backed off: the completion
        # was not clobbered with LOST.
        with pg_session_factory() as session:
            final = session.get(WorkerRun, worker_id)
            assert final.status == WorkerStatus.COMPLETED
            assert final.status_message == "Pipeline completed."
