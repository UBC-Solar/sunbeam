# Sunbeam

Sunbeam is UBC Solar's telemetry processing pipeline: it ingests raw CAN
telemetry (live, via InfluxDB, or from a historical event), runs it through
a graph of computation **stages** (power, energy, efficiency, position,
...) at their declared frequencies, and persists the derived signals to a
TimescaleDB/Postgres database — while a server orchestrates worker
processes, supervises their lifecycle, and serves both the orchestration
API and a read/stream API for the derived data.

## Architecture, Briefly

A **worker** process (`sunbeam.py`) runs one event's pipeline: it reads
`config/events.toml` to find which stages an event needs, builds a
dependency graph from their declared inputs/outputs
(`pipeline/pipeline_generator.py`), splits it into same-rate subgraphs,
and schedules each subgraph based on its frequency. Stages that consume signals
nothing else produces automatically get their data from generated
**ingress** stages reading InfluxDB. Output frames flow through a batching,
non-blocking writer into `aligned_sample` table in `sunbeamdb`, Sunbeam's PostgreSQL database. A worker can run
standalone (`--serverless`), or report in to the **server** — a FastAPI
app (`server/`) that can launch workers as Docker containers, supervise
them via heartbeats and a watchdog, and serve derived telemetry back out
over REST and Server-Sent Events for dashboards. See
[`docs/CONTRACTS.md`](docs/CONTRACTS.md) for the full module-by-module
breakdown and the rules that keep these pieces independently testable.

## Documentation Map

| Document | Covers |
|---|---|
| [`docs/USAGE.md`](docs/USAGE.md) | Running Sunbeam: Docker + dashboard, the CLI's three operating modes, `uv` dependency groups and why they're split per pipeline edition. **Start here to run something.** |
| [`docs/STAGES.md`](docs/STAGES.md) | What a stage is, how stages become a scheduled pipeline, `stage_registry.toml`, pipeline editions, and the checklist for writing a new stage. **Start here to add computation.** |
| [`docs/API.md`](docs/API.md) | The full server HTTP/SSE API: events, workers, pipeline editions, and the telemetry query + streaming endpoints. |
| [`docs/CONTRACTS.md`](docs/CONTRACTS.md) | The contract each module (`config`, `state`, `stage`, `pipeline`, `orchestration`, `db`, `server`) keeps with the others — what it owns, what it's allowed to assume, what must never import what. |
| [`docs/ALEMBIC.md`](docs/ALEMBIC.md) | Database migrations: the mental model, day-to-day workflow, and this repo's specific setup (auto-migration on server startup, the pre-Alembic transition shim, the lock timeout). |

## Quick Start

The fast path is Docker (see [`docs/USAGE.md`](docs/USAGE.md#mode-1-docker-with-the-server-and-dashboard)
for the full explanation of what each service does):

```bash
docker compose up --build
```

This brings up Postgres/TimescaleDB, the server (`http://localhost:8000`,
docs at `/docs`), and the dashboard (`http://localhost:5173`) — the
dashboard lets you launch a worker against any configured event and watch
it run.

To run a single worker directly, without Docker, see
[`docs/USAGE.md`](docs/USAGE.md#mode-2-the-cli-sunbeampy) — you'll need to
`uv sync` the right extras first:

```bash
uv sync --extra executor --extra v3_0
uv run sunbeam.py --event_name realtime --serverless
```

## Repository Layout

```
config/         TOML config + Context singleton, event/vehicle/signal sync
state/          In-memory State (signal blackboard) and Frame/FrameView
stage/          The Stage base class, StageLibrary, and per-edition stage implementations
pipeline/       Graph building, scheduling, the Executor, timing/metrics
orchestration/  Worker<->server protocol: WorkerControl, OrchestratorClient, bootstrap
db/             SQLAlchemy models, event/aligned-sample writers, InfluxDB readers
server/         The FastAPI app: routes, services, the debug stream viewer
alembic/        Database migration chain (see docs/ALEMBIC.md)
dashboard/      The React/Vite operator dashboard
tests/          v3_0/ (pure stage unit tests), infrastructure/ (everything else, SQLite-backed), postgres/ (marked, needs Docker)
```

### Configuration Files
```
config/*.toml              events.toml, vehicles.toml, context.toml (deployment config)
stage/stage_registry.toml  Stage name -> class registry, per pipeline edition
```

## Development

### Setup

Install [`uv`](https://docs.astral.sh/uv/) if you don't have it, then sync
the extras/groups you need. For full local development (running the
worker, the test suite, and the lint/type tooling):

```bash
uv sync --extra executor --extra v3_0 --group test --group dev
```

See [`docs/USAGE.md`](docs/USAGE.md#dependency-groups-and-why-they-exist)
for what each extra/group is for and why `server`/`executor` and
`v3_0`/`v3_1` are mutually exclusive.

### Linting and Type Checking

```bash
uv run ruff check .        # lint
uv run ruff check --fix .  # lint, auto-fixing what it can
uv run mypy .               # type check
```

Both are configured in `pyproject.toml` (`[tool.ruff]`, `[tool.mypy]`) and
run in CI on every push and PR (`.github/workflows/ci.yml`).

### Running Tests

```bash
uv run pytest              # the default suite: fast, no Docker/Postgres needed
uv run pytest -m postgres  # Postgres-marked tests: locking, timezones, migrations
```

The default suite (`tests/v3_0/` + `tests/infrastructure/`) runs against
SQLite in-memory and fake Docker clients — it's the one you run on every
change, and it's fast (well under a second). It does **not** cover a
handful of things SQLite can't express: real row-level locking (`SELECT
... FOR UPDATE`, which the worker heartbeat / watchdog reconciliation
depends on), timezone-aware datetime round-trips, and the Alembic
migration chain against real TimescaleDB DDL. Those live in `tests/
postgres/`, marked `@pytest.mark.postgres` and excluded by default
(`addopts = "-m 'not postgres'"` in `pyproject.toml`); running them spins
up a throwaway `timescale/timescaledb` container via `testcontainers`, so
they need a Docker daemon but nothing else.

CI (`.github/workflows/ci.yml`) runs all four checks — `ruff`, `mypy`, the
default suite, and the Postgres suite — as separate parallel jobs on every
push to `main` and every pull request.

### Adding a Database Migration

See [`docs/ALEMBIC.md`](docs/ALEMBIC.md) in full, but in short:

```bash
uv run alembic revision --autogenerate -m "describe the change"
# read the generated file in alembic/versions/ before trusting it
uv run alembic upgrade head
```

The server applies pending migrations automatically on startup — you don't
separately deploy a migration step, but you do need to write and commit
the migration file alongside the model change that motivated it.
