# Sunbeam

UBC Solar's telemetry pipeline. It reads raw CAN telemetry from InfluxDB, live or for a past event, runs it through
a graph of **stages** (power, energy, efficiency, position, ...) at their own frequencies, and stores the results in
TimescaleDB. A server launches and supervises the workers that do this, and serves the results over REST and
server-sent events.

## How it works

A **worker** (`sunbeam.py`) runs one event. It reads the event from Postgres, builds a dependency graph from its
stages' declared inputs and outputs, and splits it into same-rate pipelines, each scheduled at its frequency.
Signals no stage produces come from generated **ingress** stages that read InfluxDB. An ongoing event runs in
**realtime**, polling InfluxDB with the wall clock; a past event is **replayed**, fetched whole and processed as fast
as possible. Output goes through a batching writer into the `sample` table.

The **server** (`server/`, FastAPI) launches workers as Docker containers, supervises them with heartbeats and a
watchdog, and serves the data. Workers can also run by hand, reporting to the server or with none at all. The
**dashboard** (`dashboard/`, React) is where events, vehicles and connections are created, and workers launched and watched.

| Document | Covers |
|---|---|
| [`docs/USAGE.md`](docs/USAGE.md) | Running Sunbeam: Docker and the dashboard, the CLI, dependencies, configuration. **Start here.** |
| [`docs/STAGES.md`](docs/STAGES.md) | Stages, how they become pipelines, pipeline editions, and writing a new stage. |
| [`docs/CONTRACTS.md`](docs/CONTRACTS.md) | What each module owns, and what may import what. |
| [`docs/ALEMBIC.md`](docs/ALEMBIC.md) | Changing the database schema. |
| `/docs` on the server | The HTTP API, generated from the code (`http://localhost:8000/docs`). |

## Quick start

```bash
docker compose up --build
```

starts TimescaleDB, the server (`http://localhost:8000`) and the dashboard (`http://localhost:5173`). On the
dashboard, add an event, then launch a worker for it.

To run a worker without Docker:

```bash
uv sync --extra executor --extra v3_0
uv run sunbeam.py --event_name realtime --serverless   # plus the --influx-* settings
```

## Layout

```
config/         The environment (Context), loading a worker's event, creating its signals
state/          State, the latest value of every signal, and the Frame views stages see
stage/          The Stage base class, StageLibrary, stage_registry.toml, and each edition's stages
pipeline/       Graph building, scheduling, the Executor, metrics, worker Dockerfiles
orchestration/  The worker's side of the server protocol
db/             SQLAlchemy models, sample writers, InfluxDB readers
server/         The FastAPI app: routes and services
alembic/        Database migrations
dashboard/      The React/Vite dashboard
tests/          v3_0/ (stage unit tests), infrastructure/ (everything else, on SQLite), postgres/ (needs Docker)
```

## Development

```bash
uv sync --extra server --extra executor --extra v3_0 --all-groups

uv run ruff check .        # lint
uv run mypy .              # type check
uv run pytest              # the default suite: SQLite and fake Docker, a few seconds
uv run pytest -m postgres  # needs Docker: locking, time zones, hypertables, migrations
```

The Postgres tests start a throwaway TimescaleDB container with testcontainers, and cover what SQLite can't: row
locks (`SELECT ... FOR UPDATE`, which heartbeats and the watchdog rely on), time zone round trips, and the
migrations. CI (`.github/workflows/ci.yml`) runs all of these, and type-checks and builds the dashboard, on every
push to `main` and every pull request.

To change the schema, edit `db/sunbeamdb/models.py`, then `uv run alembic revision --autogenerate -m "..."` and read
what it generated. The server applies migrations when it starts. See [`docs/ALEMBIC.md`](docs/ALEMBIC.md).
