import requests

from orchestration.client import OrchestratorClient, WorkerPermission
from typing import Optional
from abc import ABC
import logging
import threading
import time

logger = logging.getLogger("sunbeam.worker.control")


class WorkerControl(ABC):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def should_stop(self) -> bool: ...
    def request_stop(self, reason: Optional[str] = None) -> None: ...
    def set_stage(self, stage: Optional[str]) -> None: ...
    def set_message(self, message: Optional[str]) -> None: ...
    def heartbeat_now(self, *, status: str = "running") -> None: ...
    def report_metrics(self, payload: dict) -> None: ...
    def complete(self, *, success: bool, message: Optional[str] = None) -> None: ...

class ServerlessWorkerControl(WorkerControl):
    """
    Used when running locally without the server.
    """

    def __init__(self) -> None:
        self._stop_requested = threading.Event()

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def should_stop(self) -> bool:
        return self._stop_requested.is_set()

    def request_stop(self, reason: Optional[str] = None) -> None:
        self._stop_requested.set()

    def set_stage(self, stage: Optional[str]) -> None:
        pass

    def set_message(self, message: Optional[str]) -> None:
        pass

    def heartbeat_now(self, *, status: str = "running") -> None:
        pass

    def report_metrics(self, payload: dict) -> None:
        pass

    def complete(self, *, success: bool, message: Optional[str] = None) -> None:
        pass


class OrchestratedWorkerControl(WorkerControl):
    def __init__(
        self,
        client: OrchestratorClient,
        *,
        heartbeat_interval_s: float = 1.0,
        permission_interval_s: float = 1.0,
        poll_interval_s: float = 0.1,
    ) -> None:
        self._client = client
        self._heartbeat_interval_s = heartbeat_interval_s
        self._permission_interval_s = permission_interval_s
        self._poll_interval_s = poll_interval_s

        self._stop_event = threading.Event()
        self._shutdown_event = threading.Event()

        self._current_stage: Optional[str] = None
        self._status_message: Optional[str] = None
        self._lock = threading.Lock()

        self._thread = threading.Thread(
            target=self._run_control_loop,
            name="sunbeam-worker-control",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._shutdown_event.set()
        self._thread.join(timeout=5)

    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    def request_stop(self, reason: Optional[str] = None) -> None:
        """
        Stop initiated locally by the worker itself (e.g. a crashed scheduler),
        as opposed to a stop the server requested via the permission poll.
        """
        self._stop_event.set()

        if reason is not None:
            with self._lock:
                self._status_message = reason

    def set_stage(self, stage: Optional[str]) -> None:
        with self._lock:
            self._current_stage = stage

    def set_message(self, message: Optional[str]) -> None:
        with self._lock:
            self._status_message = message

    def heartbeat_now(self, *, status: str = "running") -> None:
        stage, message = self._snapshot()
        self._client.heartbeat(
            status=status,
            current_stage=stage,
            status_message=message,
        )

    def report_metrics(self, payload: dict) -> None:
        try:
            self._client.report_metrics(payload)
        except requests.exceptions.RequestException:
            logger.warning("Failed to report metrics to server", exc_info=True)

    def complete(self, *, success: bool, message: Optional[str] = None) -> None:
        try:
            self._client.complete(success=success, message=message)
        except requests.exceptions.RequestException:
            logger.warning("Failed to report completion to server", exc_info=True)

    def _snapshot(self) -> tuple[Optional[str], Optional[str]]:
        with self._lock:
            return self._current_stage, self._status_message

    def _run_control_loop(self) -> None:
        last_heartbeat = 0.0
        last_permission = 0.0

        while not self._shutdown_event.is_set():
            now = time.monotonic()

            try:
                if not self._stop_event.is_set() and now - last_permission >= self._permission_interval_s:
                    try:
                        permission = self._client.permission()
                    except requests.exceptions.ConnectionError:
                        permission = WorkerPermission(
                            reason="Failed to connect to server.",
                            allowed=False,
                        )

                    last_permission = now

                    if not permission.allowed:
                        self._stop_event.set()

                        with self._lock:
                            self._status_message = (
                                permission.reason or "Stop requested by server."
                            )

                if now - last_heartbeat >= self._heartbeat_interval_s:
                    status = "stopping" if self.should_stop() else "running"
                    if not self._stop_event.is_set():
                        self.heartbeat_now(status=status)
                        last_heartbeat = now

            except Exception:
                # Important design choice:
                # Do NOT automatically kill the worker if the server is briefly unreachable.
                # Just keep trying. You can make this stricter later.
                logger.exception("Worker control loop error")

            time.sleep(self._poll_interval_s)