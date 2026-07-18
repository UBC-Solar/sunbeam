import logging
import os
import socket
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Optional

import docker
from docker.errors import DockerException
from sqlalchemy.orm import Session

from db.sunbeamdb.models import TERMINAL_WORKER_STATUSES, Event, WorkerRun, WorkerStatus
from orchestration.services.metrics_cache import MetricsCache
from stage.stage_library import StageLibrary

logger = logging.getLogger("sunbeam.orchestrator")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkerService:
    def __init__(self) -> None:
        self._docker = docker.from_env()
        self._library = StageLibrary()

        self._worker_network = os.environ.get("SUNBEAM_WORKER_NETWORK")
        self._server_url = os.environ.get(
            "SUNBEAM_WORKER_SERVER_URL",
            "http://orchestrator:8000",
        )

    def _validate_pipeline_edition(self, pipeline_edition: str) -> None:
        editions = list(self._library.pipeline_editions)
        if pipeline_edition not in editions:
            raise ValueError(
                f"Unknown pipeline edition {pipeline_edition!r}. "
                f"Known editions: {editions}"
            )

    def _image_tag_for(self, pipeline_edition: str) -> str:
        return f"sunbeam-worker:{pipeline_edition}"

    def launch_worker(
        self,
        db: Session,
        *,
        event_id: int,
        pipeline_edition: str,
    ) -> WorkerRun:
        event = db.get(Event, event_id)
        event_name = event.name
        if event is None:
            raise ValueError(f"No event exists with id={event_id}")

        self._validate_pipeline_edition(pipeline_edition)

        image_tag = self._image_tag_for(pipeline_edition)

        worker = WorkerRun(
            event_id=event_id,
            pipeline_edition=pipeline_edition,
            image_tag=image_tag,
            status=WorkerStatus.REQUESTED,
        )

        db.add(worker)
        db.commit()
        db.refresh(worker)

        container_name = f"sunbeam-worker-{str(worker.id)[:8]}"

        worker.status = WorkerStatus.STARTING
        worker.container_name = container_name
        db.commit()
        db.refresh(worker)

        environment = {
            "PYTHONUNBUFFERED": "1",
            "SUNBEAM_WORKER_RUN_ID": str(worker.id),
            "SUNBEAM_EVENT_NAME": event_name,
            "SUNBEAM_PIPELINE_EDITION": pipeline_edition,
            "SUNBEAM_ORCHESTRATOR_URL": self._server_url,
            "SUNBEAM_CONFIGURATION_PROFILE": "debug",
        }

        try:
            container = self._docker.containers.run(
                image=image_tag,
                name=container_name,
                detach=True,
                environment=environment,
                network=self._worker_network,
                labels={
                    "sunbeam.kind": "worker",
                    "sunbeam.worker_run_id": str(worker.id),
                    "sunbeam.event_id": str(event_id),
                    "sunbeam.pipeline_edition": pipeline_edition,
                },
            )
        except DockerException as exc:
            worker.status = WorkerStatus.FAILED
            worker.failure_reason = f"Failed to launch Docker container: {exc}"
            worker.stopped_at = utcnow()
            db.commit()
            db.refresh(worker)

            logger.error(
                "Failed to launch worker %s for event %r (%s): %s",
                worker.id, event_name, pipeline_edition, exc,
            )

            return worker

        worker.container_id = container.id
        worker.host = socket.gethostname()
        worker.started_at = utcnow()
        worker.status = WorkerStatus.STARTING

        db.commit()
        db.refresh(worker)

        logger.info(
            "Launched worker %s for event %r (%s) as container %s",
            worker.id, event_name, pipeline_edition, container_name,
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
        worker.status_message = "Stop requested by orchestrator."

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
        worker = db.get(WorkerRun, worker_id)
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
