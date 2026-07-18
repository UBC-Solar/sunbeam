from dataclasses import dataclass
from typing import Optional
from config.context import Context

import requests
import socket
import uuid
import os


@dataclass
class WorkerPermission:
    allowed: bool
    reason: Optional[str]


class OrchestratorClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        worker_run_id: Optional[str | uuid.UUID] = None,
    ) -> None:
        self._base_url = Context().sunbeam_broker.build_url()

        raw_worker_id = worker_run_id or os.environ.get("SUNBEAM_WORKER_RUN_ID")

        if not self._base_url:
            raise ValueError("Missing SUNBEAM_ORCHESTRATOR_URL")

        if raw_worker_id is None:
            raise ValueError("Missing SUNBEAM_WORKER_RUN_ID")

        self._worker_run_id = str(raw_worker_id)

    def heartbeat(
        self,
        *,
        status: str = "running",
        current_stage: Optional[str] = None,
        status_message: Optional[str] = None,
    ) -> None:
        response = requests.post(
            f"{self._base_url}/workers/{self._worker_run_id}/heartbeat",
            json={
                "status": status,
                "current_stage": current_stage,
                "status_message": status_message,
                "host": socket.gethostname(),
            },
            timeout=3,
        )
        response.raise_for_status()

    def permission(self) -> WorkerPermission:
        response = requests.get(
            f"{self._base_url}/workers/{self._worker_run_id}/permission",
            timeout=3,
        )
        response.raise_for_status()

        data = response.json()

        return WorkerPermission(
            allowed=data["allowed"],
            # stop_requested=data["stop_requested"],
            reason=data.get("reason"),
        )

    def report_metrics(self, payload: dict) -> None:
        response = requests.post(
            f"{self._base_url}/workers/{self._worker_run_id}/metrics",
            json=payload,
            timeout=3,
        )
        response.raise_for_status()

    def complete(self, *, success: bool, message: Optional[str] = None) -> None:
        response = requests.post(
            f"{self._base_url}/workers/{self._worker_run_id}/complete",
            json={
                "success": success,
                "message": message,
            },
            timeout=3,
        )
        response.raise_for_status()