from orchestration.control import WorkerControl, ServerlessWorkerControl, OrchestratedWorkerControl
from orchestration.client import OrchestratorClient
from config.context import Context, ServiceType
from pipeline.preflight import run_preflight_checks
from sqlalchemy import create_engine
from pipeline import Executor
import argparse
import logging
import tomllib
import config
import os

logger = logging.getLogger("sunbeam.worker")


class Sunbeam:
    def __init__(self):
        database_url = Context().sunbeam_db.build_url()
        self._engine = create_engine(database_url, echo=False)

    def __del__(self):
        self._engine.dispose()

    def run(
        self,
        event_name,
        reprocess: bool = False,
        control = None,
    ):
        resolved_control = control or ServerlessWorkerControl()
        is_orchestrated = isinstance(resolved_control, OrchestratedWorkerControl)

        results = run_preflight_checks(self._engine, check_orchestrator=is_orchestrated)
        failures = [r for r in results if not r.ok]

        if failures:
            message = "Preflight failed: " + "; ".join(f"{r.name}: {r.detail}" for r in failures)
            logger.error(message)

            try:
                resolved_control.complete(success=False, message=message)
            except Exception:
                logger.warning(
                    "Failed to report preflight failure to orchestrator", exc_info=True,
                )

            raise SystemExit(1)

        executor = Executor(
            event_name,
            self._engine,
            reprocess=reprocess,
            control=resolved_control,
        )
        executor.run()


def build_control(serverless: bool) -> WorkerControl:
    if not serverless:
        return OrchestratedWorkerControl(OrchestratorClient())
    else:
        return ServerlessWorkerControl()


def parse_args():
    parser = argparse.ArgumentParser(description="Run Sunbeam pipeline executor.")

    parser.add_argument(
        "--event_name",
        nargs="?",
        default=os.environ.get("SUNBEAM_EVENT_NAME", "realtime"),
        help="Event name to run. Defaults to SUNBEAM_EVENT_NAME or 'realtime'.",
    )

    parser.add_argument(
        "--serverless",
        action="store_true",
        default=os.environ.get("SUNBEAM_SERVERLESS", "false").lower() == "false",
        help="Run in serverless mode.",
    )

    parser.add_argument(
        "--reprocess",
        action="store_true",
        default=os.environ.get("SUNBEAM_REPROCESS", "false").lower() == "true",
        help="Reprocess an existing event.",
    )

    parser.add_argument(
        "--configuration",
        default=os.environ.get("SUNBEAM_CONFIGURATION_PROFILE", None),
        help="The configuration profile to use.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    args = parse_args()

    configuration_profile = args.configuration.lower() if args.configuration else None

    is_worker = "SUNBEAM_WORKER_RUN_ID" in os.environ

    if is_worker:
        is_serverless = False
    else:
        is_serverless = args.serverless

    with open(config.CONTEXT_PATH, "rb") as f:
        config_dict = tomllib.load(f)

        service_type = ServiceType.Worker if is_worker else ServiceType.Client
        Context.from_config(config_dict, service_type, configuration_profile)

    logger.info(
        "Resolved worker configuration: %s",
        {
            "service_type": service_type.value,
            "is_serverless": is_serverless,
            "worker_run_id": os.environ.get("SUNBEAM_WORKER_RUN_ID"),
            "pipeline_edition": os.environ.get("SUNBEAM_PIPELINE_EDITION"),
            "event_name": args.event_name,
            "reprocess": args.reprocess,
            **Context().describe(),
        },
    )

    if "event_name" in args:
        event_name = args.event_name
    else:
        event_name = os.environ.get("SUNBEAM_EVENT_NAME", None)

    if event_name is None:
        raise RuntimeError("Event name not set.")

    control = build_control(is_serverless)

    sunbeam = Sunbeam()
    sunbeam.run(
        args.event_name,
        reprocess=args.reprocess,
        control=control,
    )
