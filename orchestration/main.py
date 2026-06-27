import tomllib

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine

import config
from config.context import Context, ServiceType
from db import create_schema
from orchestration.routes import events, workers, pipeline_editions
import os


app = FastAPI(title="Sunbeam Orchestrator")


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


@app.on_event("startup")
def startup() -> None:
    with open(config.CONTEXT_PATH, "rb") as f:
        config_dict = tomllib.load(f)
        Context.from_config(config_dict, ServiceType.Broker)

    database_url = Context().sunbeam_db.build_url()
    engine = create_engine(database_url, echo=False)
    create_schema(engine)
    engine.dispose()

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(events.router)
app.include_router(workers.router)
app.include_router(pipeline_editions.router)