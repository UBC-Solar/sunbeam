from server.routes import data, debug, events, workers, pipeline_editions
from server.services.watchdog_service import WatchdogService
from server.preflight import check_docker, check_postgres
from server.db import get_engine
from server.migrations import upgrade_database
from config import VehicleManager, EventManager, SignalManager
from fastapi.middleware.cors import CORSMiddleware
from config.context import Context, ServiceType
from stage.stage_library import StageLibrary
from sqlalchemy import create_engine, text
from dataclasses import dataclass
from docker.errors import DockerException
from fastapi import FastAPI, HTTPException
from typing import Mapping
import pathlib
import logging
from contextlib import asynccontextmanager
import docker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("sunbeam.server")

_watchdog: WatchdogService | None = None


@dataclass
class PipelineWorker:
    pipeline_edition: str
    docker_image: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    # See https://fastapi.tiangolo.com/advanced/events/#use-case
    global _watchdog

    on_startup()

    _watchdog = WatchdogService()
    _watchdog.start()

    yield

    _watchdog.stop()
    on_shutdown()


PROJECT_ROOT = pathlib.Path(__file__).parent.parent.absolute()


def build_workers() -> Mapping[str, PipelineWorker]:
    library = StageLibrary()
    client = docker.from_env()

    worker_versions = {}
    for pipeline_edition in library.pipeline_editions:
        tag = f"sunbeam-worker:{pipeline_edition}"
        logger.info("Building worker image: %s", tag)

        client.images.build(
            path=str(PROJECT_ROOT),
            dockerfile="pipeline/Dockerfile",
            tag=tag,
            rm=True,
            buildargs={
                "PIPELINE_EDITION": pipeline_edition,
            },
        )

        worker_versions[pipeline_edition] = PipelineWorker(pipeline_edition, tag)
        logger.info("Built worker image: %s", tag)

    return worker_versions


def on_shutdown():
    pass

def on_startup() -> None:
    Context.load(ServiceType.Server)

    logger.info("Resolved server configuration: %s", Context().describe())

    engine = create_engine(Context().sunbeam_db.build_url(), echo=False)

    check_postgres(engine)
    check_docker()

    upgrade_database(engine)

    VehicleManager().sync_vehicles(engine)
    EventManager().sync_events(engine)
    SignalManager.sync_signals(engine)

    engine.dispose()

    build_workers()


def health() -> dict[str, str]:
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"PostgreSQL unavailable: {exc}")

    try:
        client = docker.from_env()
        try:
            client.ping()
        finally:
            client.close()
    except DockerException as exc:
        raise HTTPException(status_code=503, detail=f"Docker unavailable: {exc}")

    return {"status": "ok"}


def create_app(lifespan=lifespan) -> FastAPI:
    """
    Build the FastAPI application. Tests can pass lifespan=None to skip the
    Postgres/Docker startup checks and override dependencies instead.
    """
    app = FastAPI(title="Sunbeam Server", lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.get("/health")(health)

    app.include_router(events.router)
    app.include_router(workers.router)
    app.include_router(pipeline_editions.router)
    app.include_router(data.router)
    app.include_router(debug.router)

    return app


app = create_app()