import logging
import socket
import threading
import uuid
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from typing import Any, Optional

import docker
from docker.errors import DockerException
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from config.context import Context, TelemetryDB
from db.sunbeamdb.models import TERMINAL_WORKER_STATUSES, Event, WorkerKind, WorkerRun, WorkerStatus
from db.telemetrydb.connection_check import event_window
from server.services.connection_service import ConnectionService, ConnectionTestFailed
from server.services.image_builder import ImageBuildFailed, WorkerImageBuilder
from server.services.metrics_cache import MetricsCache
from stage.stage_library import StageLibrary

logger = logging.getLogger("sunbeam.server")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


_UNSET: Any = object()


def _run_in_thread(target: Callable[[], None]) -> None:
    threading.Thread(target=target, name="sunbeam-worker-build", daemon=True).start()


class WorkerService:
    def __init__(
        self,
        docker_client=None,
        *,
        library: StageLibrary | None = None,
        worker_network: str | None = _UNSET,
        connections: ConnectionService | None = None,
        image_builder: WorkerImageBuilder | None = None,
        session_factory: Callable[[], Session] | sessionmaker | None = None,
        run_in_background: Callable[[Callable[[], None]], None] = _run_in_thread,
        worker_database_url: str | None = None,
        worker_server_url: str | None = None,
    ) -> None:
        """
        :param session_factory: Opens the session a git build uses once it finishes, off the request's thread;
            resolved lazily from server.db so tests never touch the real database configuration
        :param run_in_background: Runs a git build; tests pass one that runs it inline
        """
        self._docker = docker_client if docker_client is not None else docker.from_env()
        self._library = library or StageLibrary()
        # None is a valid override (no network), so only fall back to the
        # configured Context when the caller passed nothing at all.
        self._worker_network = Context().worker_network if worker_network is _UNSET else worker_network
        self._connections = connections or ConnectionService()
        self._image_builder = image_builder or WorkerImageBuilder(self._docker)
        self._session_factory = session_factory
        self._run_in_background = run_in_background
        self._worker_database_url = worker_database_url
        self._worker_server_url = worker_server_url

    def _make_session(self) -> Session:
        if self._session_factory is None:
            from server.db import get_session_factory
            self._session_factory = get_session_factory()
        return self._session_factory()

    def _validate_pipeline_edition(self, pipeline_edition: str) -> None:
        editions = list(self._library.pipeline_editions)
        if pipeline_edition not in editions:
            raise ValueError(
                f"Unknown pipeline edition {pipeline_edition!r}. "
                f"Known editions: {editions}"
            )

    def _worker_environment(self, *, worker: WorkerRun, event_name: str, telemetry_db: TelemetryDB) -> dict:
        """ Everything a launched worker needs to know; it reads nothing else to find its connections. """
        context = Context()
        return {
            "PYTHONUNBUFFERED": "1",
            "SUNBEAM_WORKER_RUN_ID": str(worker.id),
            "SUNBEAM_EVENT_NAME": event_name,
            "SUNBEAM_PIPELINE_EDITION": worker.pipeline_edition,
            "SUNBEAM_REPROCESS": "true" if worker.reprocess else "false",
            "SUNBEAM_DATABASE_URL": self._worker_database_url or context.worker_database_url,
            "SUNBEAM_SERVER_URL": self._worker_server_url or context.worker_server_url,
            **telemetry_db.as_env(),
        }

    def launch_worker(
        self,
        db: Session,
        *,
        event_id: int,
        pipeline_edition: str,
        influx_connection_id: int | None = None,
        source_ref: str | None = None,
        reprocess: bool = False,
    ) -> WorkerRun:
        """
        Test the connection against the event, then start a worker container. From a git ref, the run is
        BUILDING until its image is built in the background (unless that commit was built before).

        :raises ValueError: Unknown event, edition, connection, or git ref
        :raises ConnectionTestFailed: The connection can't reach InfluxDB or sees no data for the event
        """
        event = db.get(Event, event_id)
        if event is None:
            raise ValueError(f"No event exists with id={event_id}")
        event_name = event.name
        source_ref = (source_ref or "").strip() or None

        # A ref may well have editions this copy of the code doesn't; its own build will say if it lacks one
        if source_ref is None:
            self._validate_pipeline_edition(pipeline_edition)

        connection = self._connections.resolve(db, influx_connection_id)

        # Fail fast: nothing is created for a connection that can't see the event's data
        result = self._connections.check_saved(db, connection, event_window(event.starts_at, event.ends_at))
        if not result.ok:
            raise ConnectionTestFailed(connection, result)

        source_sha = None
        if source_ref is not None:
            source_sha = self._image_builder.resolve_ref(source_ref)
            image_tag = self._image_builder.git_tag(pipeline_edition, source_sha)
            needs_build = not self._image_builder.has_image(image_tag)
        else:
            image_tag = self._image_builder.local_tag(pipeline_edition)
            needs_build = False

        worker = WorkerRun(
            event_id=event_id,
            pipeline_edition=pipeline_edition,
            image_tag=image_tag,
            status=WorkerStatus.BUILDING if needs_build else WorkerStatus.REQUESTED,
            influx_connection_id=connection.id,
            influx_connection_name=connection.name,
            source_ref=source_ref,
            source_sha=source_sha,
            # The worker wipes a replayed event regardless; record what will actually happen
            reprocess=reprocess or event.is_past(),
        )
        if needs_build:
            worker.status_message = f"Building {image_tag} from {source_ref} ({source_sha[:12]})."  # type: ignore[index]

        db.add(worker)
        db.commit()
        db.refresh(worker)

        # Captured now, so the build thread never has to re-read the connection (which may change meanwhile)
        environment = self._worker_environment(
            worker=worker,
            event_name=event_name,
            telemetry_db=TelemetryDB(
                database_url=connection.url,
                bucket=connection.bucket,
                organization=connection.organization,
                token=connection.token,
            ),
        )

        if needs_build:
            worker_id = worker.id
            self._run_in_background(lambda: self._build_then_start(worker_id, environment))
            return worker

        return self._start_container(db, worker, environment)

    def _build_then_start(self, worker_id: uuid.UUID, environment: dict) -> None:
        db = self._make_session()
        try:
            worker = db.get(WorkerRun, worker_id)
            if worker is None:
                return

            try:
                self._image_builder.build_git(worker.pipeline_edition, worker.source_sha)  # type: ignore[arg-type]
            except (ImageBuildFailed, DockerException) as exc:
                db.refresh(worker)
                if worker.status not in TERMINAL_WORKER_STATUSES:
                    worker.status = WorkerStatus.FAILED
                    worker.failure_reason = f"Worker image build failed: {exc}"
                    worker.status_message = "Worker image build failed."
                    worker.stopped_at = utcnow()
                    db.commit()
                logger.error("Failed to build image for worker %s: %s", worker_id, exc)
                return

            # Stopped (or cancelled by the watchdog) while it was building
            db.refresh(worker)
            if worker.status in TERMINAL_WORKER_STATUSES:
                return
            if worker.stop_requested:
                worker.status = WorkerStatus.CANCELLED
                worker.status_message = "Stopped before its container launched."
                worker.stopped_at = utcnow()
                db.commit()
                return

            self._start_container(db, worker, environment)
        except Exception:
            logger.exception("Building worker %s failed unexpectedly", worker_id)
        finally:
            db.close()

    def _start_container(self, db: Session, worker: WorkerRun, environment: dict) -> WorkerRun:
        container_name = f"sunbeam-worker-{str(worker.id)[:8]}"

        worker.status = WorkerStatus.STARTING
        worker.status_message = None
        worker.container_name = container_name
        # The watchdog's startup grace counts from here, not from created_at (which is before any git build)
        worker.started_at = utcnow()
        db.commit()
        db.refresh(worker)

        try:
            container = self._docker.containers.run(
                image=worker.image_tag,
                name=container_name,
                detach=True,
                environment=environment,
                network=self._worker_network,
                labels={
                    "sunbeam.kind": "worker",
                    "sunbeam.worker_run_id": str(worker.id),
                    "sunbeam.event_id": str(worker.event_id),
                    "sunbeam.pipeline_edition": worker.pipeline_edition,
                },
            )
        except DockerException as exc:
            worker.status = WorkerStatus.FAILED
            worker.failure_reason = f"Failed to launch Docker container: {exc}"
            worker.stopped_at = utcnow()
            db.commit()
            db.refresh(worker)

            logger.error(
                "Failed to launch worker %s for event %s (%s): %s",
                worker.id, worker.event_id, worker.pipeline_edition, exc,
            )

            return worker

        worker.container_id = container.id
        worker.host = socket.gethostname()
        worker.started_at = utcnow()

        db.commit()
        db.refresh(worker)

        logger.info(
            "Launched worker %s for event %s (%s, %s) as container %s",
            worker.id, worker.event_id, worker.pipeline_edition, worker.influx_connection_name, container_name,
        )

        return worker

    def register_worker(
        self,
        db: Session,
        *,
        event_name: str,
        pipeline_edition: str,
        host: str | None,
        influx_connection_name: str | None = None,
        reprocess: bool = False,
    ) -> WorkerRun:
        """
        Register an externally launched worker (e.g. sunbeam.py run by hand).
        The server issues the WorkerRun ID; the worker is supervised via
        heartbeats only, since there is no container to inspect or kill.
        """
        event = db.scalar(select(Event).where(Event.name == event_name))
        if event is None:
            raise ValueError(f"No event exists with name {event_name!r}")

        self._validate_pipeline_edition(pipeline_edition)

        worker = WorkerRun(
            event_id=event.id,
            pipeline_edition=pipeline_edition,
            image_tag=None,
            kind=WorkerKind.EXTERNAL,
            status=WorkerStatus.STARTING,
            host=host,
            started_at=utcnow(),
            # Reported by the worker, which was given its own connection; recorded as-is for display
            influx_connection_name=influx_connection_name,
            reprocess=reprocess or event.is_past(),
        )

        db.add(worker)
        db.commit()
        db.refresh(worker)

        logger.info(
            "Registered external worker %s for event %r (%s) on host %s",
            worker.id, event_name, pipeline_edition, host,
        )

        return worker

    def request_stop(self, db: Session, worker_id: uuid.UUID) -> WorkerRun | None:
        worker = db.get(WorkerRun, worker_id)
        if worker is None:
            return None

        if worker.status in TERMINAL_WORKER_STATUSES:
            return worker

        worker.stop_requested = True
        worker.stop_requested_at = utcnow()
        worker.status = WorkerStatus.STOP_REQUESTED
        worker.status_message = "Stop requested by server."

        db.commit()
        db.refresh(worker)

        logger.info("Stop requested for worker %s", worker_id)

        return worker

    def heartbeat(
        self,
        db: Session,
        *,
        worker_id: uuid.UUID,
        status: WorkerStatus,
        current_stage: str | None,
        status_message: str | None,
        host: str | None,
    ) -> WorkerRun | None:
        # Locked so this can't race the watchdog's own read-modify-write on
        # the same row (see WatchdogService._resolve): whichever of the two
        # commits first wins, and the other sees the committed terminal
        # status on its own (post-lock) re-check and backs off.
        worker = db.get(WorkerRun, worker_id, with_for_update=True)
        if worker is None:
            return None

        if worker.status in TERMINAL_WORKER_STATUSES:
            return worker

        if worker.stop_requested:
            worker.status = WorkerStatus.STOP_REQUESTED
        else:
            worker.status = status

        worker.current_stage = current_stage
        worker.status_message = status_message
        worker.last_heartbeat_at = utcnow()

        if host is not None:
            worker.host = host

        db.commit()
        db.refresh(worker)

        return worker

    def permission_for(
        self,
        db: Session,
        worker_id: uuid.UUID,
    ) -> tuple[bool, str | None, bool]:
        worker = db.get(WorkerRun, worker_id)
        if worker is None:
            return False, "Unknown worker.", True

        if worker.stop_requested:
            return False, "Stop requested.", True

        if worker.status in TERMINAL_WORKER_STATUSES:
            return False, f"Worker is terminal: {worker.status.value}.", True

        return True, None, False

    def complete(
        self,
        db: Session,
        *,
        worker_id: uuid.UUID,
        success: bool,
        message: str | None,
    ) -> WorkerRun | None:
        worker = db.get(WorkerRun, worker_id)
        if worker is None:
            return None

        worker.status = WorkerStatus.COMPLETED if success else WorkerStatus.FAILED
        worker.status_message = message
        worker.failure_reason = None if success else message
        worker.stopped_at = utcnow()

        db.commit()
        db.refresh(worker)

        MetricsCache.clear(worker.id)

        logger.info(
            "Worker %s reported %s: %s",
            worker_id, "success" if success else "failure", message,
        )

        return worker

    def record_metrics(
        self,
        db: Session,
        *,
        worker_id: uuid.UUID,
        payload: dict,
    ) -> bool:
        worker = db.get(WorkerRun, worker_id)
        if worker is None or worker.status in TERMINAL_WORKER_STATUSES:
            return False

        MetricsCache.set(worker_id, {**payload, "reported_at": utcnow().isoformat()})
        return True

    def get_metrics(self, worker_id: uuid.UUID) -> Optional[dict]:
        return MetricsCache.get(worker_id)

    def _get_container(self, db: Session, worker_id: uuid.UUID):
        worker = db.get(WorkerRun, worker_id)
        if worker is None or worker.container_id is None:
            return None

        try:
            return self._docker.containers.get(worker.container_id)
        except DockerException:
            return None

    def get_logs(
        self,
        db: Session,
        worker_id: uuid.UUID,
        *,
        tail: int = 500,
    ) -> Optional[list[str]]:
        container = self._get_container(db, worker_id)
        if container is None:
            return None

        raw = container.logs(tail=tail, timestamps=True)
        return raw.decode("utf-8", errors="replace").splitlines()

    def stream_logs(
        self,
        db: Session,
        worker_id: uuid.UUID,
        *,
        tail: int = 200,
    ) -> Optional[Iterator[bytes]]:
        container = self._get_container(db, worker_id)
        if container is None:
            return None

        return container.logs(stream=True, follow=True, tail=tail, timestamps=True)
