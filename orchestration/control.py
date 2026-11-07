import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Optional

import requests

from orchestration.client import OrchestratorClient, WorkerPermission

logger = logging.getLogger("sunbeam.worker.control")


# The server not answering at all (restarting, or gone), rather than answering with an error
UNREACHABLE = (requests.exceptions.ConnectionError, requests.exceptions.Timeout)


class WorkerControl(ABC):
    # Whether a server is supervising this worker (and receiving its reports)
    is_orchestrated: bool = False

    @abstractmethod
    def start(self) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...

    @abstractmethod
    def should_stop(self) -> bool: ...

    @abstractmethod
    def request_stop(self) -> None: ...

    @abstractmethod

    @abstractmethod
    def set_message(self, message: Optional[str]) -> None: ...

    @abstractmethod
    def report_metrics(self, payload: dict) -> None: ...

    @abstractmethod
    def complete(self, *, success: bool, message: Optional[str] = None) -> None: ...


class ServerlessWorkerControl(WorkerControl):
    """ Used when running locally without the server. """

    def __init__(self) -> None:
        self._stop_requested = threading.Event()

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def should_stop(self) -> bool:
        return self._stop_requested.is_set()

    def request_stop(self) -> None:
        self._stop_requested.set()

    def set_message(self, message: Optional[str]) -> None:
        pass

    def report_metrics(self, payload: dict) -> None:
        pass

    def complete(self, *, success: bool, message: Optional[str] = None) -> None:
        pass


class OrchestratedWorkerControl(WorkerControl):
    """
    Heartbeats to the server and polls it for permission to keep running, on a background thread. Heartbeats
    continue (as "stopping") after a stop, until stop() is called, so the server can tell a worker that is
    winding down from one that has died.
    """
    is_orchestrated = True

    def __init__(
        self,
        client: OrchestratorClient,
        *,
        heartbeat_interval_s: float = 1.0,
        permission_interval_s: float = 1.0,
        poll_interval_s: float = 0.1,
        max_unreachable_polls: int = 60,
    ) -> None:
        """
        :param max_unreachable_polls: Consecutive permission polls that can fail to reach the server before the
            worker stops itself: about a minute. A server restart (several seconds) shouldn't kill a run; a server
            that's gone for good should.
        """
        self._client = client
        self._heartbeat_interval_s = heartbeat_interval_s
        self._permission_interval_s = permission_interval_s
        self._poll_interval_s = poll_interval_s
        self._max_unreachable_polls = max_unreachable_polls

        self._stop_event = threading.Event()
        self._shutdown_event = threading.Event()

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

    def request_stop(self) -> None:
        """ A stop decided by the worker itself (e.g. a crashed scheduler), rather than by the server. """
        self._stop_event.set()

    def set_message(self, message: Optional[str]) -> None:
        with self._lock:
            self._status_message = message

    def report_metrics(self, payload: dict) -> None:
        try:
            self._client.report_metrics(payload)
        except UNREACHABLE:
            pass  # Reported by the permission poll; the next report will go through once the server is back
        except requests.exceptions.RequestException:
            logger.warning("Failed to report metrics to server", exc_info=True)

    def complete(self, *, success: bool, message: Optional[str] = None) -> None:
        try:
            self._client.complete(success=success, message=message)
        except UNREACHABLE:
            logger.warning("Couldn't report completion: the server is unreachable")
        except requests.exceptions.RequestException:
            logger.warning("Failed to report completion to server", exc_info=True)

    def _heartbeat(self) -> None:
        with self._lock:
            message = self._status_message
        try:
            self._client.heartbeat(status="stopping" if self.should_stop() else "running", status_message=message)
        except UNREACHABLE:
            pass  # The permission poll reports (and counts) the outage; one warning per poll is enough

    def _check_permission(self, unreachable_polls: int) -> int:
        """ Poll the server once, stopping if it says to (or has been unreachable for too long).

        :return int: The number of consecutive polls that have failed to reach the server
        """
        try:
            permission = self._client.permission()
        except UNREACHABLE:
            unreachable_polls += 1
            logger.warning("Could not reach the server (%d in a row)", unreachable_polls)
            if unreachable_polls < self._max_unreachable_polls:
                return unreachable_polls
            permission = WorkerPermission(allowed=False, reason="Lost contact with the server.")
        else:
            unreachable_polls = 0

        if not permission.allowed:
            self._stop_event.set()
            self.set_message(permission.reason or "Stop requested by server.")

        return unreachable_polls

    def _run_control_loop(self) -> None:
        last_heartbeat = 0.0
        last_permission = 0.0
        unreachable_polls = 0

        while not self._shutdown_event.is_set():
            now = time.monotonic()

            # Errors are logged and retried: a briefly unreachable server must not kill the worker on its own
            try:
                if not self.should_stop() and now - last_permission >= self._permission_interval_s:
                    last_permission = now
                    unreachable_polls = self._check_permission(unreachable_polls)

                if now - last_heartbeat >= self._heartbeat_interval_s:
                    last_heartbeat = now
                    self._heartbeat()

            except Exception:
                logger.exception("Worker control loop error")

            time.sleep(self._poll_interval_s)
