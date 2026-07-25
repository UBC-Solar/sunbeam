# Usage

Sunbeam runs in two fundamentally different ways: as a **Docker-orchestrated
system** (a server that launches and supervises worker containers,
with a dashboard on top), or as a **bare CLI process** you run directly with
`uv run sunbeam.py`. This document covers both, plus the dependency-group
mechanics you need to understand to run anything at all.

For what a "worker" is actually doing once it's running, see
[`STAGES.md`](STAGES.md). For the HTTP/SSE surface the server exposes, see
[`API.md`](API.md).

## Dependency Groups, and Why

`pyproject.toml` defines several `uv` **extras**
(`[project.optional-dependencies]`) and **groups** (`[dependency-groups]`).
Understanding which ones exist and why is the difference between `uv sync`
working and mysteriously failing:

| Name | Kind | What it's for |
|---|---|---|
| `server` | extra | Everything the FastAPI server needs: `fastapi`, `docker` (SDK), `sqlalchemy`, `psycopg`, etc. |
| `executor` | extra | Everything a worker process needs to *run a pipeline*: `networkx` (graph building), `sqlalchemy`, `psycopg`, `rich` (terminal output). |
| `v3_0` | extra | Dependencies **specific to the `v3_0` pipeline edition's stages** — currently `influxdb-client` (ingress) plus the `pytest` toolchain for that edition's unit tests. |
| `v3_1` | extra | Same idea, for a future edition. Currently empty — a placeholder showing the pattern. |
| `test` | group | Everything the *full* test suite needs beyond an edition + `executor`: `fastapi`, `docker`, `httpx2`, `testcontainers`. |
| `dev` | group | `ruff`, `mypy`, `alembic` — tooling, not runtime dependencies. |

Two conflict rules in `[tool.uv].conflicts` are load-bearing:

- **`server` and `executor` are mutually exclusive.** The server never runs
  pipeline code, and a worker never imports FastAPI/Docker — so a single
  environment installs exactly one of the two, and `uv` will refuse to
  resolve a lockfile that asks for both at once.
- **`v3_0` and `v3_1` are mutually exclusive.** See below.

### Per-Edition Dependency Groups

This is the one worth internalizing: **a pipeline edition's stage code can
depend on arbitrary third-party packages, and different editions may need
different (even conflicting) versions or entirely different packages.**
`v3_0`'s ingress currently needs `influxdb-client`; a hypothetical `v3_1`
targeting a different telemetry backend might need something else instead,
or a newer major version of a shared dependency that isn't compatible with
what `v3_0` pins. Making each edition its own `uv` extra — rather than
lumping every edition's dependencies into `executor` — means:

- A worker container built for one edition never installs packages another
  edition needs (see `dockerfiles/worker.Dockerfile`'s
  `--extra "${PIPELINE_EDITION}"` build arg — this is *why* the image is
  built per edition, not just per Sunbeam version).
- Adding a new edition's stage dependency can never break another edition's
  install, because `uv sync --extra executor --extra v3_1` never resolves
  `v3_0`'s dependency set at all.
- The `[tool.uv].conflicts` entry marking `v3_0`/`v3_1` mutually exclusive
  is what makes `uv` treat this correctly: it's a hard signal that these
  extras represent alternatives, not additions, so `uv sync --extra v3_0
  --extra v3_1` is rejected outright instead of silently producing an
  environment neither edition was tested against.

When you add a new stage that needs a new package, the dependency goes in
*that stage's edition extra* — see
[`STAGES.md`](STAGES.md#adding-a-new-stage) step 4. It never goes in
`executor` or `server` unless every edition, forever, needs it (rare —
`networkx` is a rare example, since pipeline graph-building is
edition-independent).

### Common `uv sync` Invocations

```bash
# Running the server locally (no worker code)
uv sync --extra server

# Running a worker for the v3_0 edition
uv sync --extra executor --extra v3_0

# Everything: worker + tests + lint/type tooling (what CI runs)
uv sync --extra executor --extra v3_0 --group test --group dev
```

As a reminder, `--extra` is for controlling what parts of the repository you are going to be running and is intended for users. `--group` is for controlling development tools and is intended for developers. In a package, `--extra` are installable by end users while `--group` is not. The difference between them is more theoretical than practical. 

`uv run <script>` and `uv run pytest` automatically use whatever
environment the last `uv sync` produced — you don't re-specify extras on
every command, only when you want to *change* what's installed.

## Mode 1: Docker, with the server and dashboard

This is the normal way to run Sunbeam for anything beyond single-stage
development — a Postgres/TimescaleDB container, the FastAPI server, and the
React dashboard, all launched together:

```bash
docker compose up --build
```

This starts three services (`docker-compose.yaml`):

- **`db`** — `timescale/timescaledb`, exposed on `localhost:5432`.
- **`server`** — the FastAPI app (`server/Dockerfile`, `--extra server`),
  exposed on `localhost:8000`. On startup it runs Alembic migrations (see
  [`ALEMBIC.md`](ALEMBIC.md)), syncs `vehicles.toml`/`events.toml` into the
  database, and **builds one `sunbeam-worker:<edition>` Docker image per
  pipeline edition** found in `stage_registry.toml` (this is why the first
  `docker compose up` takes noticeably longer than subsequent ones — it's
  building N worker images, not just the server).
- **`dashboard`** — a Vite dev server (`dashboard/`), exposed on
  `localhost:5173`, talking to the server at `VITE_API_BASE_URL`
  (`http://localhost:8000` by default).

Once it's up:

1. Open `http://localhost:5173` — the dashboard lists events and lets you
   launch a worker for one (`POST /workers/launch`; see
   [`API.md`](API.md#post-workerslaunch)).
2. The server launches a `sunbeam-worker:<edition>` container for that
   event, mounted onto the same `sunbeam-net` Docker network as the server
   and database (`config.toml`'s `[worker]` block points `sunbeamdb` at
   `db` and `sunbeam-server` at `server` — the in-network hostnames, not
   `localhost`).
3. The worker registers no run ID itself here — the server already created
   its `WorkerRun` row and passes the ID via the `SUNBEAM_WORKER_RUN_ID`
   environment variable when it starts the container (see
   `server/services/worker_service.py`'s `launch_worker`).
4. The dashboard polls `/workers` for status, `/workers/{id}/logs` (or the
   `/logs/stream` SSE endpoint) for output, and can request a stop via
   `POST /workers/{id}/stop`.
5. A `WatchdogService` runs inside the server process, sweeping every few
   seconds to reconcile `WorkerRun` status against actual container state —
   a crashed or unresponsive container gets marked `lost`/`failed`
   automatically even if it never calls back.

### Debug Data Viewer

`http://localhost:8000/debug/viewer` is a minimal, throwaway page for
watching the SSE telemetry stream (`GET /events/{event}/data/stream`) work
live — pick an event and signal, hit Connect, and watch samples arrive as a
worker writes them. It exists as a **worked example of the streaming
client protocol** (see the comment block at the top of
`server/static/stream_viewer.html`) for whoever builds the real dashboard
widget consuming that stream; delete `server/routes/debug.py` and
`server/static/` once that's built.

### Rebuilding

`docker compose up --build` rebuilds all three images. If you've only
changed pipeline/stage code, `docker compose up --build server` alone is
enough to pick it up in the next-launched worker container (worker images
are built lazily by the server on its own startup, from the same source
tree the server container was built from).

## Mode 2: the CLI (`sunbeam.py`)

Running `sunbeam.py` directly runs one worker process **without Docker** —
useful for local pipeline development, debugging a single event, or running
against a server that's already up (Docker or otherwise). Sync the right
extras first (see above), then:

```bash
uv run sunbeam.py --event_name realtime
```

### Arguments

| Flag | Default | Meaning |
|---|---|---|
| `--event_name` | `$SUNBEAM_EVENT_NAME` or `"realtime"` | Which event (from `events.toml`) to run. |
| `--serverless` | off | Run with no server at all — see below. |
| `--reprocess` | `$SUNBEAM_REPROCESS` or `false` | Passed through to `EventWriter`; intended for reprocessing an already-`PROCESSED` event (see the commented-out guard in `db/sunbeamdb/writer.py` — this is a known incomplete feature, not a footgun you'll hit by accident). |
| `--configuration` | `$SUNBEAM_CONFIGURATION_PROFILE` | `debug` or `production` — which `context.toml` block to load. Defaults to whatever `context.toml`'s `[main].default_config` says. |

Every flag also has an environment-variable fallback, because that's how
the Docker worker container is actually configured (see
`server/services/worker_service.py`'s `launch_worker`, which sets
`SUNBEAM_EVENT_NAME`, `SUNBEAM_PIPELINE_EDITION`, etc. as container env
vars rather than CLI args).

### The three ways a CLI worker can run

`orchestration/bootstrap.py`'s `build_control` picks one of three
`WorkerControl` implementations based on how the process was invoked:

**1. Serverless** — `--serverless`, or launched with
`SUNBEAM_SERVERLESS=true`. No server contact at all: no registration, no
heartbeats, no permission polling, no completion reporting. This is
`ServerlessWorkerControl` — `should_stop()` always returns `False` (it can
still be told to stop locally via `request_stop()`, e.g. by
`Executor.signal_completion()`), every other method is a no-op. Good for
quick local runs where you don't care about dashboard visibility.

```bash
uv run sunbeam.py --event_name FSGP_2024_Day_1 --serverless
```

**2. Server-launched (container)** — how Docker-launched workers run
(Mode 1 above). `SUNBEAM_WORKER_RUN_ID` is already set in the environment
(the server put it there), so `build_control` skips registration entirely
and goes straight to `OrchestratedWorkerControl(OrchestratorClient())` —
heartbeats, permission polling (a server-side stop request is honored
within one polling interval), completion/failure reporting, and periodic
metrics (timing + writer-queue stats) all flow to the server.

**3. Server-registered (external)** — running `sunbeam.py` **by hand**,
without `--serverless`, while a server is reachable. No
`SUNBEAM_WORKER_RUN_ID` exists yet, so the worker calls
`POST /workers/register` itself, the **server issues the run ID** (the
server always owns `WorkerRun` row creation — see
[`CONTRACTS.md`](CONTRACTS.md#server)), and the worker proceeds exactly
like a container worker from that point on: heartbeats, dashboard
visibility, remote stop, metrics. The one difference is supervision: since
there's no container to inspect or kill, `WatchdogService` tracks this
worker (`WorkerKind.EXTERNAL`) via heartbeat timeout and stop-grace alone —
a stop request that's ignored past the grace period marks it `lost` rather
than force-killing it (there's nothing to kill).

```bash
# server already running (Docker or otherwise), reachable at
# config.toml's [client.sunbeam-server] address
uv run sunbeam.py --event_name realtime
```

If no server is reachable in this mode, the worker exits immediately with
a message telling you to either start the server or pass `--serverless` —
it never silently falls back to serverless.

### Preflight Checks

Before doing anything else, both `Sunbeam.run()` (CLI) and worker container
startup run `pipeline/preflight.py`'s checks: can it reach the Sunbeam
Postgres database, can it reach InfluxDB (skipped gracefully if
`influxdb-client` isn't installed in this edition's image — see the
dependency-group discussion above), and — for orchestrated modes only —
can it reach the server's `/health` endpoint. A failed preflight reports
failure to the server (if reachable) and exits with a nonzero status before
ever building a pipeline; look here first if a worker won't start.

## Configuration Profiles (`debug` vs `production`)

`config/context.toml` has three top-level blocks — `[client]`, `[worker]`,
`[server]` — one per `ServiceType`, each with a `debug` and `production`
sub-block. Which `ServiceType` loads depends on how the process starts
(`sunbeam.py` picks `Worker` if `SUNBEAM_WORKER_RUN_ID` is set, else
`Client`; `server/main.py` always loads `Server`); which profile
(`debug`/`production`) loads is `--configuration`/`SUNBEAM_CONFIGURATION_PROFILE`,
defaulting to `context.toml`'s `[main].default_config`. This is what lets
the same `context.toml` describe both a `docker compose` deployment (where
`worker.sunbeam-server` points at the Docker-network hostname `server`) and
a bare CLI run against `localhost`, without code changes.
