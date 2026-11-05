import logging
import threading
from datetime import datetime

import docker
from docker.errors import DockerException

from db.sunbeamdb.models import TERMINAL_WORKER_STATUSES, WorkerKind, WorkerRun, WorkerStatus
from db.timeutil import utcnow
from server.db import get_session_factory
from server.services.metrics_cache import MetricsCache

logger = logging.getLogger("sunbeam.server.watchdog")

NON_TERMINAL_STATUSES = [
    status for status in WorkerStatus if status not in TERMINAL_WORKER_STATUSES
]

HEARTBEATING_STATUSES = {
    WorkerStatus.RUNNING,
    WorkerStatus.STOP_REQUESTED,
    WorkerStatus.STOPPING,
}


class WatchdogService:
    """
    Reconciles WorkerRun rows against reality: the server is the source
    of truth for a run's lifecycle, not the worker process, which may be dead,
    hung, or unreachable. Runs on its own thread, independent of request
    handling.
    """

    def __init__(
        self,
        *,
        docker_client=None,
        session_factory=None,
        poll_interval_s: float = 5.0,
        heartbeat_timeout_s: float = 10.0,
        startup_grace_s: float = 60.0,
        stop_grace_s: float = 30.0,
        build_timeout_s: float = 20 * 60.0,
    ) -> None:
        self._docker = docker_client if docker_client is not None else docker.from_env()
        # A zero-arg callable returning a Session; resolved lazily so tests
        # never touch the real database configuration.
        self._session_factory = session_factory
        self._poll_interval_s = poll_interval_s
        self._heartbeat_timeout_s = heartbeat_timeout_s
        self._startup_grace_s = startup_grace_s
        self._stop_grace_s = stop_grace_s
        self._build_timeout_s = build_timeout_s

        self._shutdown_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="sunbeam-server-watchdog",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._shutdown_event.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                self._sweep()
            except Exception:
                logger.exception("Watchdog sweep failed")

            self._shutdown_event.wait(self._poll_interval_s)

    def _make_session(self):
        factory = self._session_factory or get_session_factory()
        return factory()

    def _sweep(self) -> None:
        db = self._make_session()
        try:
            workers = (
                db.query(WorkerRun)
                .filter(WorkerRun.status.in_(NON_TERMINAL_STATUSES))
                .all()
            )

            for worker in workers:
                try:
                    self._check_worker(db, worker)
                except DockerException:
                    logger.exception(
                        "Docker error while checking worker %s", worker.id
                    )
        finally:
            db.close()

    def _resolve(self, db, worker: WorkerRun, status: WorkerStatus, reason: str) -> None:
        # worker was read without a lock, so it may be stale by now (e.g. a
        # heartbeat could have landed concurrently). Re-fetch under
        # SELECT ... FOR UPDATE and re-check terminal status immediately
        # before writing, so this can't clobber a concurrent resolution.
        # WorkerService.heartbeat takes the same lock for the same reason.
        locked = db.get(WorkerRun, worker.id, with_for_update=True)
        if locked is None or locked.status in TERMINAL_WORKER_STATUSES:
            return

        logger.warning("Worker %s -> %s: %s", locked.id, status.value, reason)

        locked.status = status
        locked.status_message = reason
        locked.failure_reason = reason if status != WorkerStatus.COMPLETED else None
        locked.stopped_at = utcnow()

        db.commit()
        MetricsCache.clear(locked.id)

    def _check_worker(self, db, worker: WorkerRun) -> None:
        now = utcnow()

        def older_than(since: datetime | None, limit_s: float) -> bool:
            return (now - (since or worker.created_at)).total_seconds() > limit_s

        # A container worker's container is the best evidence of how it's doing. A self-registered (external)
        # worker has none: there is nothing to inspect and nothing to kill, so it's supervised by the timing rules
        # below alone.
        container = None
        if worker.kind == WorkerKind.CONTAINER:
            if worker.container_id is None:
                self._check_unlaunched(db, worker, older_than)
                return

            try:
                container = self._docker.containers.get(worker.container_id)
            except docker.errors.NotFound:
                self._resolve(db, worker, WorkerStatus.LOST, "Container no longer exists.")
                return

            if container.status in ("exited", "dead"):
                exit_code = container.attrs.get("State", {}).get("ExitCode")
                if exit_code == 0:
                    reason = "Container exited without reporting completion."
                else:
                    reason = f"Container exited unexpectedly (exit code {exit_code})."
                self._resolve(db, worker, WorkerStatus.FAILED, reason)
                return

        if worker.status in (WorkerStatus.STOP_REQUESTED, WorkerStatus.STOPPING):
            if older_than(worker.stop_requested_at, self._stop_grace_s):
                if container is None:
                    self._resolve(
                        db, worker, WorkerStatus.LOST,
                        "External worker did not stop within the grace period (no container to kill).",
                    )
                    return

                logger.warning("Worker %s exceeded stop grace period, killing container", worker.id)
                try:
                    container.kill()
                except DockerException:
                    logger.exception("Failed to kill container for worker %s", worker.id)
                self._resolve(
                    db, worker, WorkerStatus.CANCELLED, "Forcibly stopped after exceeding stop grace period.",
                )
                return

        if worker.status in HEARTBEATING_STATUSES:
            if older_than(worker.last_heartbeat_at, self._heartbeat_timeout_s):
                self._resolve(db, worker, WorkerStatus.LOST, "Worker stopped heartbeating.")
            return

        if worker.status in (WorkerStatus.REQUESTED, WorkerStatus.STARTING):
            # started_at is stamped when the launch begins, which for a git build is long after created_at
            if older_than(worker.started_at, self._startup_grace_s):
                self._resolve(db, worker, WorkerStatus.LOST, "Worker never became healthy.")

    def _check_unlaunched(self, db, worker: WorkerRun, older_than) -> None:
        """ A container worker that has no container yet: building its image, or about to launch. """
        if worker.stop_requested:
            # The image build checks for this before launching anything
            self._resolve(db, worker, WorkerStatus.CANCELLED, "Stopped before its container launched.")
        elif worker.status == WorkerStatus.BUILDING:
            if older_than(worker.created_at, self._build_timeout_s):
                self._resolve(db, worker, WorkerStatus.FAILED, "Worker image build timed out.")
        elif older_than(worker.started_at, self._startup_grace_s):
            self._resolve(db, worker, WorkerStatus.LOST, "Worker never launched a container.")
