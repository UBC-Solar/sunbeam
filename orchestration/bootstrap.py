"""
How a worker process obtains its WorkerControl at startup, depending on how it
was launched: by the server (run ID in the environment), by hand against a
running server (self-registration), or fully serverless.
"""
import logging
import os

import requests

from config.context import Context
from orchestration.client import OrchestratorClient
from orchestration.control import (
    OrchestratedWorkerControl,
    ServerlessWorkerControl,
    WorkerControl,
)

logger = logging.getLogger("sunbeam.worker")


def build_control(serverless: bool, event_name: str) -> WorkerControl:
    if serverless:
        return ServerlessWorkerControl()

    if "SUNBEAM_WORKER_RUN_ID" in os.environ:
        # Launched by the server: it already created our WorkerRun.
        return OrchestratedWorkerControl(OrchestratorClient())

    # Launched by hand: register with the server to obtain a run ID.
    from config import EventManager

    pipeline_edition = EventManager().get_event_pipeline_edition(event_name)

    try:
        client = OrchestratorClient.register(
            event_name=event_name,
            pipeline_edition=pipeline_edition,
        )
    except requests.exceptions.ConnectionError as exc:
        raise SystemExit(
            f"Could not register with the Sunbeam server at "
            f"{Context().sunbeam_broker.build_url()}: {exc}\n"
            f"Is the server running? To run without a server, pass --serverless."
        ) from exc

    logger.info("Registered with server as worker %s", client._worker_run_id)

    return OrchestratedWorkerControl(client)