import argparse
import logging
import os

from sqlalchemy import Engine, create_engine

from config.context import INFLUX_ENV, Context
from config.events import EventConfig, load_event
from db.telemetrydb.connection_check import event_window
from orchestration.bootstrap import build_control
from orchestration.control import WorkerControl
from pipeline import Executor
from pipeline.preflight import run_preflight_checks

logger = logging.getLogger("sunbeam.worker")

# Where to find everything; each flag overrides its environment variable (see config/context.py). A worker the server
# launches gets these set for it, from the connection chosen on the dashboard.
_CONNECTION_FLAGS = {
    "influx-url": INFLUX_ENV["database_url"],
    "influx-org": INFLUX_ENV["organization"],
    "influx-bucket": INFLUX_ENV["bucket"],
    "influx-token": INFLUX_ENV["token"],
    "database-url": "SUNBEAM_DATABASE_URL",
    "server-url": "SUNBEAM_SERVER_URL",
}


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "false").lower() == "true"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Sunbeam pipeline executor.")
    parser.add_argument(
        "--event_name",
        default=os.environ.get("SUNBEAM_EVENT_NAME", "realtime"),
        help="Event name to run. Defaults to SUNBEAM_EVENT_NAME or 'realtime'.",
    )
    parser.add_argument(
        "--serverless",
        action="store_true",
        # A hand-run worker registers itself with the server unless this (or SUNBEAM_SERVERLESS=true) opts out
        default=_env_flag("SUNBEAM_SERVERLESS"),
        help="Run without the Sunbeam server (no registration, no heartbeats).",
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        default=_env_flag("SUNBEAM_REPROCESS"),
        help="Reprocess an existing event.",
    )
    for flag, env in _CONNECTION_FLAGS.items():
        parser.add_argument(f"--{flag}", default=None, help=f"Overrides ${env}.")

    return parser.parse_args()


def apply_connection_flags(args: argparse.Namespace) -> None:
    """ Context reads only the environment, so flags given on the command line become variables. """
    for flag, env in _CONNECTION_FLAGS.items():
        value = getattr(args, flag.replace("-", "_"))
        if value:
            os.environ[env] = value


def run(event: EventConfig, control: WorkerControl, engine: Engine, *, reprocess: bool) -> None:
    results = run_preflight_checks(
        engine,
        check_server=control.is_orchestrated,
        window=event_window(event.starts_at, event.ends_at),
    )
    failures = [r for r in results if not r.ok]
    if failures:
        message = "Preflight failed: " + "; ".join(f"{r.name}: {r.detail}" for r in failures)
        logger.error(message)
        control.complete(success=False, message=message)
        raise SystemExit(1)

    Executor.from_event(event, engine, reprocess=reprocess, control=control).run()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    args = parse_args()
    apply_connection_flags(args)

    # A worker the server launched is always orchestrated
    launched_by_server = "SUNBEAM_WORKER_RUN_ID" in os.environ
    serverless = args.serverless and not launched_by_server

    logger.info(
        "Resolved worker configuration: %s",
        {
            "launched_by_server": launched_by_server,
            "is_serverless": serverless,
            "worker_run_id": os.environ.get("SUNBEAM_WORKER_RUN_ID"),
            "event_name": args.event_name,
            "reprocess": args.reprocess,
            **Context().describe(),
        },
    )

    engine = create_engine(Context().sunbeam_db.build_url(), echo=False)
    try:
        event = load_event(engine, args.event_name)
        control = build_control(serverless, event, reprocess=args.reprocess)
        run(event, control, engine, reprocess=args.reprocess)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
