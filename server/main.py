import logging
from contextlib import asynccontextmanager

import docker
import pydantic
from docker.errors import DockerException
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text

from config import SignalManager, VehicleManager
from config.context import Context
from server.db import get_engine
from server.errors import Conflict
from server.migrations import upgrade_database
from server.preflight import check_docker, check_postgres
from server.routes import connections, data, events, pipeline_editions, workers
from server.services.connection_service import ConnectionTestFailed
from server.services.image_builder import WorkerImageBuilder
from server.services.sample_counts import SampleCounts
from server.services.watchdog_service import WatchdogService
from stage.stage_library import StageLibrary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("sunbeam.server")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # See https://fastapi.tiangolo.com/advanced/events/#use-case
    on_startup()

    watchdog = WatchdogService()
    watchdog.start()
    yield
    watchdog.stop()


def on_startup() -> None:
    logger.info("Resolved server configuration: %s", Context().describe())

    engine = create_engine(Context().sunbeam_db.build_url(), echo=False)
    try:
        check_postgres(engine)
        check_docker()

        upgrade_database(engine)

        VehicleManager().sync_vehicles(engine)
        SignalManager.sync_signals(engine)
    finally:
        engine.dispose()

    # One worker image per pipeline edition (each has its own extra) from this copy of the code; launches from a git
    # ref build their own
    builder = WorkerImageBuilder(docker.from_env())
    for pipeline_edition in StageLibrary().pipeline_editions:
        builder.build_local(pipeline_edition)


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


def _error_handler(status_code: int):
    async def handle(request: Request, exc: Exception) -> JSONResponse:
        # pydantic's ValidationError is a ValueError too, but one raised inside a route is a bug in the server, not
        # a bad request (bad request bodies are rejected before the route runs, as 422s)
        if isinstance(exc, pydantic.ValidationError):
            raise exc
        return JSONResponse(status_code=status_code, content={"detail": str(exc)})
    return handle


def create_app(lifespan=lifespan) -> FastAPI:
    """
    Build the FastAPI application. Tests can pass lifespan=None to skip the
    Postgres/Docker startup checks and override dependencies instead.
    """
    app = FastAPI(title="Sunbeam Server", lifespan=lifespan)
    # Per app rather than global, so each test's app (and database) starts with no cached counts
    app.state.sample_counts = SampleCounts()

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

    # How service exceptions become responses (see server/errors.py); the most specific class wins
    app.add_exception_handler(LookupError, _error_handler(404))
    app.add_exception_handler(Conflict, _error_handler(409))
    app.add_exception_handler(ConnectionTestFailed, _error_handler(422))
    app.add_exception_handler(ValueError, _error_handler(400))

    app.get("/health")(health)

    app.include_router(events.router)
    app.include_router(events.vehicles_router)
    app.include_router(workers.router)
    app.include_router(pipeline_editions.router)
    app.include_router(connections.router)
    app.include_router(data.router)

    return app


app = create_app()
