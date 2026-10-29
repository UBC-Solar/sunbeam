from orchestration.bootstrap import build_control
from orchestration.control import ServerlessWorkerControl, OrchestratedWorkerControl
from config import EventManager
from config.context import INFLUX_ENV, Context
from db.telemetrydb.connection_check import event_window
from pipeline.preflight import run_preflight_checks
from sqlalchemy import create_engine
from pipeline import Executor
import argparse
import logging
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

        event = EventManager().get_event(event_name)
        results = run_preflight_checks(
            self._engine,
            check_server=is_orchestrated,
            window=event_window(event.starts_at, event.ends_at),
        )
        failures = [r for r in results if not r.ok]

        if failures:
            message = "Preflight failed: " + "; ".join(f"{r.name}: {r.detail}" for r in failures)
            logger.error(message)

            try:
                resolved_control.complete(success=False, message=message)
            except Exception:
                logger.warning(
                    "Failed to report preflight failure to server", exc_info=True,
                )

            raise SystemExit(1)

        executor = Executor.from_event(
            event_name,
            self._engine,
            reprocess=reprocess,
            control=resolved_control,
        )
        executor.run()


_CONNECTION_FLAGS = {
    "influx-url": INFLUX_ENV["database_url"],
    "influx-org": INFLUX_ENV["organization"],
    "influx-bucket": INFLUX_ENV["bucket"],
    "influx-token": INFLUX_ENV["token"],
    "database-url": "SUNBEAM_DATABASE_URL",
    "server-url": "SUNBEAM_SERVER_URL",
}


def apply_connection_flags(args: argparse.Namespace) -> None:
    """ Context reads only the environment, so flags given on the command line become variables. """
    for flag, env in _CONNECTION_FLAGS.items():
        value = getattr(args, flag.replace("-", "_"))
        if value:
            os.environ[env] = value


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
        # Note: orchestrated is the default. A hand-run worker registers
        # itself with the server; --serverless (or SUNBEAM_SERVERLESS=true)
        # opts out entirely.
        default=os.environ.get("SUNBEAM_SERVERLESS", "false").lower() == "true",
        help="Run without the Sunbeam server (no registration, no heartbeats).",
    )

    parser.add_argument(
        "--reprocess",
        action="store_true",
        default=os.environ.get("SUNBEAM_REPROCESS", "false").lower() == "true",
        help="Reprocess an existing event.",
    )

    # Where to find everything; each defaults to its environment variable (see config/context.py). A worker the
    # server launches gets these set for it, from the connection chosen on the dashboard.
    for flag, env in _CONNECTION_FLAGS.items():
        parser.add_argument(f"--{flag}", default=None, help=f"Overrides ${env}.")

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    args = parse_args()

    apply_connection_flags(args)

    is_worker = "SUNBEAM_WORKER_RUN_ID" in os.environ

    if is_worker:
        is_serverless = False
    else:
        is_serverless = args.serverless

    logger.info(
        "Resolved worker configuration: %s",
        {
            "launched_by_server": is_worker,
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

    control = build_control(is_serverless, args.event_name, reprocess=args.reprocess)

    sunbeam = Sunbeam()
    sunbeam.run(
        args.event_name,
        reprocess=args.reprocess,
        control=control,
    )
