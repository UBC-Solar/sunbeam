import os
import socket
import uuid
from dataclasses import dataclass
from typing import Optional

import requests

from config.context import Context


@dataclass
class WorkerPermission:
    allowed: bool
    reason: Optional[str]


def _post(url: str, payload: dict, timeout: float = 3) -> requests.Response:
    response = requests.post(url, json=payload, timeout=timeout)
    response.raise_for_status()
    return response


class OrchestratorClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        worker_run_id: Optional[str | uuid.UUID] = None,
    ) -> None:
        self._base_url = base_url or Context().sunbeam_server.build_url()

        raw_worker_id = worker_run_id or os.environ.get("SUNBEAM_WORKER_RUN_ID")
        if raw_worker_id is None:
            raise ValueError("Missing SUNBEAM_WORKER_RUN_ID")

        self.worker_run_id = str(raw_worker_id)

    @property
    def _worker_url(self) -> str:
        return f"{self._base_url}/workers/{self.worker_run_id}"

    @classmethod
    def register(
        cls,
        *,
        event_name: str,
        pipeline_edition: str,
        base_url: Optional[str] = None,
        influx_connection_name: Optional[str] = None,
        reprocess: bool = False,
    ) -> "OrchestratorClient":
        """
        Register this process as an external worker: the server creates the
        WorkerRun and issues its ID, and the returned client speaks for it.
        """
        resolved_base_url = base_url or Context().sunbeam_server.build_url()

        response = _post(
            f"{resolved_base_url}/workers/register",
            {
                "event_name": event_name,
                "pipeline_edition": pipeline_edition,
                "host": socket.gethostname(),
                "influx_connection_name": influx_connection_name,
                "reprocess": reprocess,
            },
            timeout=5,
        )

        return cls(base_url=resolved_base_url, worker_run_id=response.json()["id"])

    def heartbeat(
        self,
        *,
        status: str = "running",
        status_message: Optional[str] = None,
    ) -> None:
        _post(f"{self._worker_url}/heartbeat", {
            "status": status,
            "status_message": status_message,
            "host": socket.gethostname(),
        })

    def permission(self) -> WorkerPermission:
        response = requests.get(f"{self._worker_url}/permission", timeout=3)
        response.raise_for_status()
        data = response.json()
        return WorkerPermission(allowed=data["allowed"], reason=data.get("reason"))

    def report_metrics(self, payload: dict) -> None:
        _post(f"{self._worker_url}/metrics", payload)

    def complete(self, *, success: bool, message: Optional[str] = None) -> None:
        _post(f"{self._worker_url}/complete", {"success": success, "message": message})
