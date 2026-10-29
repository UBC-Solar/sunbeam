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
  `localhost:5173`. The dashboard calls the API at a relative `/api` path and
  the dev server proxies it to the `server` container
  (`SUNBEAM_API_PROXY_TARGET`, `http://localhost:8000` when running
  `npm run dev` outside compose), so it works unchanged when opened from
  another machine, e.g. `http://<ec2-public-ip>:5173`.

Once it's up:

1. Open `http://localhost:5173` — the dashboard lists events (marked
   *Realtime* or *Replay*) and lets you launch a worker for one. **Launch
   worker…** opens a settings dialog (the worker always runs the event's own
   pipeline edition): the InfluxDB connection the worker reads from
   (defaulting to the default connection — see [Connections](#connections)),
   where its code comes from (the server's working copy, or a git ref — see
   [Building workers from git](#building-workers-from-git)), and whether to
   reprocess (always on, and locked, for replays). See
   [`API.md`](API.md#post-workerslaunch).
2. The server **tests the connection against the event first** — it must
   reach InfluxDB, find the bucket, and see at least one point in the
   event's window — and refuses the launch (HTTP 422, shown in the dialog)
   if it can't. Then it launches a `sunbeam-worker:<edition>` container for
   that event on the same `sunbeam-net` Docker network as the server and
   database, handing it every connection it needs as environment variables
   (`SUNBEAM_INFLUX_*`, plus the server's own `SUNBEAM_DATABASE_URL` and
   `SUNBEAM_SERVER_URL=http://server:8000` — the in-network hostnames, not
   `localhost`). The worker reads no config file or database to find them.
3. The worker registers no run ID itself here — the server already created
   its `WorkerRun` row and passes the ID via the `SUNBEAM_WORKER_RUN_ID`
   environment variable when it starts the container (see
   `server/services/worker_service.py`'s `launch_worker`).
4. The dashboard polls `/workers` for status, `/workers/{id}/metrics` for
   live timing (and a replay's progress — see
   [Realtime vs replay runs](#realtime-vs-replay-worker-runs)),
   `/workers/{id}/logs` (or the `/logs/stream` SSE endpoint) for output, and
   can request a stop via `POST /workers/{id}/stop`.
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
tree the server container was built from). To run code you haven't
rebuilt the server with, launch from a git ref instead (see
[Building workers from git](#building-workers-from-git)).

Local builds work offline as long as Docker's layer cache already has their
steps: the `apt-get` and `uv sync` layers only rerun (and need the network)
when `pipeline/Dockerfile`, `pyproject.toml`, or `uv.lock` change.

### Building workers from git

Choosing **Git ref** in the launch dialog (or `source_ref` in
`POST /workers/launch`) builds the worker from a branch, tag, or full commit
SHA of `https://github.com/UBC-Solar/sunbeam` instead of the server's copy of
the code. The server resolves the ref to a commit (`git ls-remote`), then
builds `pipeline/Dockerfile.git`, which clones that commit itself, into
`sunbeam-worker:<edition>-<sha12>`. The run shows as **Building image**
until the build finishes (the watchdog allows 20 minutes); a later launch
of the same commit reuses the image and starts at once, while a branch
that has moved builds its new commit.

Notes:

- Short SHAs can't be resolved remotely; give the full 40 characters.
- The worker runs the event's edition, which must be one the server itself
  knows; editions are added rarely enough that any ref is assumed to have
  it too. (One that doesn't fails its build.)
- Commits from before server-managed connections (they read
  `config/context.toml`) can't be launched this way; their build fails with
  a message saying so.
- Git builds need the server host to reach GitHub.

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
| `--reprocess` | `$SUNBEAM_REPROCESS` or `false` | Delete the event's existing `aligned_sample` rows before writing. Replays always do this regardless (see below), so this only matters for realtime runs, which otherwise append to whatever an earlier run wrote. |
| `--influx-url`, `--influx-org`, `--influx-bucket`, `--influx-token` | `$SUNBEAM_INFLUX_URL`, `_ORG`, `_BUCKET`, `_TOKEN` | The InfluxDB to read telemetry from. Required (as flags or variables); there are no defaults. |
| `--database-url` | `$SUNBEAM_DATABASE_URL` | The Sunbeam Postgres to write to. Defaults to the `docker compose` database on `localhost:5432`. |
| `--server-url` | `$SUNBEAM_SERVER_URL` | The Sunbeam server to register with. Defaults to `http://localhost:8000`. |

Every flag also has an environment-variable fallback, because that's how
the Docker worker container is actually configured (see
`server/services/worker_service.py`'s `launch_worker`, which sets
`SUNBEAM_EVENT_NAME`, `SUNBEAM_PIPELINE_EDITION`, etc. as container env
vars rather than CLI args).

### Realtime vs Replay worker runs

`Executor.from_event` picks one of two modes from the event's entry in
`events.toml`:

- **Realtime** — events with no `ends_at` (like `realtime`), or one still in
  the future. Ingress polls InfluxDB for the latest values on a schedule (one
  `Ingress` stage and thread per signal frequency), and every pipeline runs
  in step with the wall clock until the worker is stopped.
- **Replay** — events whose `ends_at` has passed (like `FSGP_2024_Day_1`).
  The worker fetches every needed signal over the whole event window from
  InfluxDB up front (`Offline_Ingress`, shown as one spinner per signal),
  trims the window to the range every signal actually has data for (with a
  warning if that's noticeably shorter than the event), then runs the
  compute pipelines over that window as fast as possible, never sleeping.
  Reaching the end of the window completes the worker successfully. A
  replay always deletes the event's earlier `aligned_sample` rows first,
  since it rewrites the whole event, and would otherwise collide with an
  earlier run's samples on the `(event, signal, ts)` primary key.

Both modes write the same derived signals to `aligned_sample`; a replay
also writes the raw ingress signals it fetched.

What you see depends on the mode and on whether output is a terminal:

| Run | Terminal shows | Reported to the server |
|---|---|---|
| Realtime, `--serverless` | A live timing table: per-pipeline/per-stage latency, idle/busy split | Nothing |
| Realtime, with a server | `timing idle=...` log lines every 5 s | Timing metrics every 5 s |
| Replay in a terminal | A progress bar: simulated clock vs. the end of the window, speed (× realtime), elapsed, ETA, writer queue | Timing + replay progress every 0.5 s (if there's a server) |
| Replay without a terminal (e.g. a container) | `replay 42.1% ...` and `timing ...` log lines every 5 s | Timing + replay progress every 0.5 s |

The dashboard's worker drawer shows a replay's progress as its own card
above the timing charts, and the worker's status message names the phase
it's in (fetching telemetry, replaying, waiting for database writes).

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
# $SUNBEAM_SERVER_URL (default http://localhost:8000)
uv run sunbeam.py --event_name realtime
```

If no server is reachable in this mode, the worker exits immediately with
a message telling you to either start the server or pass `--serverless` —
it never silently falls back to serverless.

### Preflight Checks

Before doing anything else, both `Sunbeam.run()` (CLI) and worker container
startup run `pipeline/preflight.py`'s checks: can it reach the Sunbeam
Postgres database, can it reach InfluxDB, find its bucket, and **see data
in the event's window** (the whole event for a replay, start-to-now for a
live one; nothing to check yet for one that hasn't started), using the same
check the server runs before a launch (`db/telemetrydb/connection_check.py`;
skipped gracefully if `influxdb-client` isn't installed in this edition's
image — see the dependency-group discussion above), and — for orchestrated
modes only —
can it reach the server's `/health` endpoint. A failed preflight reports
failure to the server (if reachable) and exits with a nonzero status before
ever building a pipeline; look here first if a worker won't start.

## Connections

Where everything lives is read from the environment (`config/context.py`)
— there is no config file.

- **InfluxDB** connections are rows the dashboard manages (**InfluxDB
  connections** card: add, edit, delete, make default, and **Test**, either
  just the server and bucket or for data in a chosen event). Editing a URL or
  token takes effect on the next launch, with no rebuild. A test result is
  recorded on the connection, and editing its settings clears it. Migration
  `c7e2a91f5d3b` seeds the two hosts `context.toml` used to
  hold: `UBC Influx (CAN_log)` (the default) and `Production (CAN_prod)`.
- **Postgres** is not a connection: the server and every worker it launches
  share one database, `SUNBEAM_DATABASE_URL`. `docker-compose.yaml` sets it
  to the compose `db` service; override it in the shell or a `.env` file next
  to `docker-compose.yaml`, then `docker compose up -d server`. Outside
  compose it defaults to `localhost:5432`.

| Variable | Read by | Default |
|---|---|---|
| `SUNBEAM_DATABASE_URL` | server, alembic, workers | `postgresql+psycopg://telemetry:telemetry@localhost:5432/telemetry` (compose: `…@db:5432/…`) |
| `SUNBEAM_INFLUX_URL` / `_ORG` / `_BUCKET` / `_TOKEN` | workers | none — set by the server at launch, or by hand for a CLI run |
| `SUNBEAM_SERVER_URL` | workers | `http://localhost:8000` |
| `SUNBEAM_WORKER_SERVER_URL` | server | `http://server:8000` — what launched workers get as `SUNBEAM_SERVER_URL` |
| `SUNBEAM_WORKER_DATABASE_URL` | server | the server's own `SUNBEAM_DATABASE_URL` — set it when workers reach the database by another name |
| `SUNBEAM_WORKER_NETWORK` | server | `sunbeam-net`; empty for none |

Everything here is reachable only over the team's Tailscale network, which
is why tokens are stored and returned in plain text.
