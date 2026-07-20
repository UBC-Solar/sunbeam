# Module contracts

Sunbeam is split into modules with narrow, deliberate responsibilities. This
document describes what each one is allowed to assume about the others — the
contracts that keep the pipeline, the server, and the database loosely
coupled enough to test independently (see the test suite in `tests/`, which
exists largely *because* these boundaries hold).

If you're about to import across a boundary this document says shouldn't
exist, stop and re-read the relevant section — it's usually a sign the thing
you're building belongs somewhere else, or that the contract needs to change
on purpose (in which case, update this file in the same PR).

## How the modules fit together

The two diagrams below answer two different questions, and are easy to
conflate if drawn as one picture (an earlier version of this document did
exactly that): **which Python modules import which** (a compile-time,
static property of the source tree) versus **which OS processes talk to
which, over what protocol, once everything is running** (a runtime
property — and the two don't line up one-to-one, since e.g. `pipeline` and
`orchestration` are both linked into the *same* worker process, while
`orchestration`'s HTTP calls cross into a completely different process
running `server`).

### 1. Import dependency graph (compile time)

Every solid arrow below (`►`) is a real `import` — it points from the
importing module to the module it imports. Nothing in this graph points
"back up"; that acyclic-ness is what tests like
`tests/infrastructure/test_pipeline_generator.py` (pure Python, no server,
no worker plumbing) rely on. The one **dotted** arrow (`╌╌►`) is
deliberately not an import at all — see the note underneath.

```
Foundation chain — each of these is a single, direct import:

    stage ─────────────► state
    db ────────────────► state
    config ────────────► db
    orchestration ─────► config

The two modules nothing else imports (each becomes an entrypoint - see
diagram 2), fanning out to everything they pull in:

    pipeline ──┬──► config
               ├──► db
               ├──► state
               ├──► stage
               └──► orchestration

    server ────┬──► config
               ├──► db
               └──► stage
               ╎
               ╎  HTTP at runtime only - NOT a Python import,
               ╎  see diagram 2 below
               ╌╌► orchestration
```

That dotted line is the answer to a question worth asking explicitly:
**does `server` know about `orchestration`?** Only at runtime, over HTTP —
`server` never has an `import orchestration` anywhere in its source.
`orchestration.OrchestratorClient` is the code a *worker* process runs to
call `server`'s `/workers/*` routes; `server/routes/workers.py` and
`server/services/worker_service.py` are the code that answers those calls
from the other side. They implement the same protocol without ever
sharing a Python dependency — which is exactly why `server`'s test suite
(`tests/infrastructure/test_worker_service.py`,
`test_watchdog_service.py`) never imports `orchestration` either, and why
a worker container's image never installs `server`'s dependencies (the
`broker`/`executor` `uv` extras are mutually exclusive — see
[`USAGE.md`](USAGE.md#dependency-groups-and-why-they-exist)). The same
logic is why `server` has no edge to `pipeline` or `state` at all: the
broker never runs a pipeline, so it has nothing to import from either.

`pipeline` and `server` are the two modules nothing else imports — they're
each assembled into one of the two entrypoints described in the next
diagram, never imported *by* another Sunbeam module. `pipeline` never
imports `server`, and (as above) `server` never imports `pipeline`,
`state`, or `orchestration` — the two processes only ever meet over HTTP.

### 2. Runtime topology (who talks to whom, once running)

```
                                                  ┌──────────────────────┐
                                                  │  Dashboard / other   │
                                                  │  HTTP clients        │
                                                  └──────────┬───────────┘
                                                             │  HTTP + SSE: /events, /workers,
                                                             │  /events/{name}/data (query + stream)
                                                             ▼
┌──────────────────┐     HTTP: register,          ┌───────────────────────┐  Docker API: launch,
│     Worker       │     heartbeat,               │       Broker          │  inspect logs, kill    ┌────────────────┐
│  (sunbeam.py,    │◄─── permission, ──────────►  │   (server/, one       │◄──────────────────────►│  Docker daemon │
│  one process     │     complete, metrics        │    FastAPI process)   │                        └────────────────┘
│  per running     │     (skipped entirely        └─────────┬─────────────┘ 
│  event)          │     in --serverless mode)              │
└───┬───────────┬──┘                                        │ reads/writes Vehicle, Event,
    │           │                                           │ Signal, WorkerRun
    │           │  writes aligned_sample rows;              ▼
    │           │  reads for preflight checks    ┌───────────────────────────┐
    │           └───────────────────────────────►│  Postgres / TimescaleDB   │
    │                                            └───────────────────────────┘
    │  reads raw CAN
    ▼
┌────────────────────────┐
│      InfluxDB          │
│  (via Tailscale VPN)   │
└────────────────────────┘
```

Two relationships worth spelling out because they don't fit neatly as a
single arrow above: when a worker is launched from the dashboard rather
than run by hand, the "Worker" box is a **container the Docker daemon is
running on the Broker's behalf** — the "Docker API" arrow is how the
Broker *creates, inspects, and kills* that container, not a separate
process with its own connections; the worker process inside still talks to
Postgres/InfluxDB and to the Broker exactly as drawn. And the Broker and
every Worker connect to Postgres **directly and independently** — the
Broker is not a proxy for worker writes; `aligned_sample` rows never pass
through the Broker process at all. See
[`USAGE.md`](USAGE.md#the-three-ways-a-cli-worker-can-run) for the three
ways a worker can come to exist (dashboard-launched container,
by-hand-and-registered, or fully `--serverless` with no Broker contact),
and [`API.md`](API.md#workers) for the HTTP calls behind the "HTTP:
register, heartbeat, ..." arrow above, narrated as one lifecycle in the
`orchestration` section just below.

`sunbeam.py` (worker entrypoint) and `server/main.py` (broker entrypoint)
are the two processes that assemble these modules; nothing in the import
graph above imports either of them.

## `config`

**Owns:** loading `*.toml` files into typed Python objects, and the
process-wide `Context` singleton that answers "what database/broker am I
configured to talk to right now."

**Provides:**
- `Context` (`config/context.py`) — a singleton loaded once per process via
  `Context.load(ServiceType.{Client,Worker,Broker}, configuration_type=None)`.
  `ServiceType` selects which `[client]` / `[worker]` / `[broker]` block of
  `context.toml` to read; `configuration_type` selects `debug` vs
  `production` within that block (defaults to `context.toml`'s
  `[main].default_config`). After the first `.load()`, later calls with
  `configuration_type=None` return the already-configured singleton — so any
  module can call `Context.load(...)` defensively without re-parsing TOML or
  clobbering an explicit override made earlier by the entrypoint.
- `EventManager`, `VehicleManager`, `SignalManager` (`config/events.py`,
  `vehicles.py`, `signals.py`) — read `events.toml` / `vehicles.toml` and
  `sync_*` them into the database (upsert by name; never delete). Also the
  read side: `EventManager.get_stages_for_event`,
  `get_event_pipeline_edition`, `get_event_date`.

**Contract:**
- `Context` is a singleton **by design** — every module that needs
  configuration calls `Context()` (after something has loaded it) rather
  than having config threaded through every constructor. This is the one
  deliberate global in the codebase; don't add a second one.
- Tests must never let `Context.load()` touch the real `context.toml`.
  Reset `Context._instance = None` and call `Context.from_config(dict, ...)`
  with an in-memory config instead (see `tests/infrastructure/conftest.py`'s
  `test_context` fixture).
- Nothing in `config` imports `pipeline`, `stage`, `server`, or
  `orchestration`. It may import `db` (to sync into it).

## `db`

**Owns:** the SQLAlchemy schema (`db/sunbeamdb/models.py`), schema creation
(`db/sunbeamdb/init_db.py`, now superseded by Alembic — see
[`ALEMBIC.md`](ALEMBIC.md)), and the two ways data gets written:
`EventWriter`/`QueuedEventWriter` (aligned samples, one row per
`(event, signal, timestamp)`) and `telemetrydb/` (reading raw CAN data back
out of InfluxDB during ingress).

**Tables** (`db/sunbeamdb/models.py`; see [`ALEMBIC.md`](ALEMBIC.md) for how
the schema evolves):

| Table | Holds | Relates to |
|---|---|---|
| `vehicle` | One row per physical car (currently just `Brightside`), synced from `config/vehicles.toml`. | Parent of `event`. |
| `event` | One row per race/session/debug run (`FSGP_2024_Day_1`, `realtime`, ...), synced from `config/events.toml`. Carries `status` (`unprocessed` → `ongoing` while a worker's `EventWriter` is open → `processed` once it closes, success or failure) and `pipeline_edition` (which stage code applies — see [`STAGES.md`](STAGES.md#versioning-stages-what-v3_0-vs-v3_1-means)). | Belongs to a `vehicle`; parent of `signal`, `worker_run`, and (via `event_id`) every `raw_sample`/`aligned_sample` row. |
| `signal` | One row per `(event, CanonicalName)` pair actually relevant to that event, with `unit`/`frequency`/`source` metadata — populated by `SignalManager.sync_signals` from the localization tables, not hand-edited. | Belongs to an `event`; referenced by `signal_id` from both sample tables. |
| `worker_run` | One row per worker process, ever — the orchestration/supervision record described in the paragraph below and in full in [`API.md`](API.md#workers). `kind` distinguishes a Docker-launched worker from a self-registered one (see [`USAGE.md`](USAGE.md#the-three-ways-a-cli-worker-can-run)). | Belongs to an `event`. |
| `raw_sample` | Unaligned, as-ingested telemetry — one row per `(event, signal, timestamp)` as it arrived, with ingest metadata (`source_message_type`, `source_sequence`). Schema exists and is migrated/hypertabled, but **nothing in this repo currently writes to it** — no code constructs a `RawSample` today; treat it as reserved for a future raw-ingestion path rather than an active part of the pipeline. | TimescaleDB hypertable, chunked on `ts`. |
| `aligned_sample` | **The actual pipeline output** — one row per `(event, signal, timestamp)` for every value a stage or ingress produced, written by every running worker via `EventWriter`/`QueuedEventWriter`. This is what the telemetry query and stream API (`server/routes/data.py`) reads back out. | TimescaleDB hypertable, chunked on `ts`, indexed on `(event_id, signal_id, ts DESC)` and `(event_id, ts DESC)` — exactly the access patterns `server/services/data_service.py` queries. |

**Contract:**
- `db.sunbeamdb.models` is the single source of truth for the schema. Both
  the server and every worker import it; nothing hand-maintains a second
  copy.
- `EventWriter` and `QueuedEventWriter` implement the `FrameWriter` /
  `BatchFrameWriter` protocols from `pipeline/protocols.py` — the pipeline
  layer depends on that protocol, not on these concrete classes, so a test
  double can stand in without touching SQLAlchemy at all (see
  `RecordingWriter` in `tests/infrastructure/conftest.py`).
- `db` never imports `pipeline`, `stage`, or `server`. `pipeline/protocols.py`
  intentionally has no import of `db` either — see
  `db/sunbeamdb/queued_writer.py`'s `TYPE_CHECKING`-guarded import for why
  (a real import there is circular: `pipeline/__init__` → `executor` →
  `queued_writer` → `pipeline.protocols` → back to `pipeline`).

## `state`

**Owns:** `State`, the in-memory "current value of every signal" blackboard
a running worker uses to pass values between pipelines, and `Frame`/
`FrameView`, the read/write and read-only views a single stage sees.

**Contract:**
- `State` is intentionally dumb: a dict of `signal -> latest value`, guarded
  by one `RLock`, with no notion of stages, pipelines, or the database. It
  is the *only* channel through which independently-scheduled pipelines
  (which may run on different rate-group threads) exchange data — see
  `pipeline/pipeline.py`'s `Pipeline.run`, which reads a `Frame` from
  `State` before a stage runs and writes the stage's outputs back
  immediately after.
- A stage never sees `State` directly — only the `Frame`/`FrameView` for its
  own declared `inputs`/`outputs`. This is what makes a stage testable with
  a bare `FrameView` and no `State` at all (see `tests/v3_0/test_*.py`).
- `state` has zero imports from any other Sunbeam module. Keep it that way —
  it's the lowest layer everything else builds on.

## `stage`

**Owns:** the `Stage` abstract base class (the computation contract every
pipeline step implements) and `StageLibrary`, which resolves stage names
(from `events.toml` and `stage_registry.toml`) to concrete classes.

**Contract:**
- Every `Stage` subclass declares `stage_name`, `inputs`, `outputs`,
  `frequency` as **class-level** attributes (enforced by
  `Stage.__init_subclass__` — a subclass missing any of them fails at
  *import* time, not at first use). `Ingress` is the one exception,
  overriding these as properties since its identity varies per instance
  (one `Ingress` object per frequency bin) — see
  [`STAGES.md`](STAGES.md) for the full contract and how to write a new one.
- A stage's `run(input_frame) -> Frame` must be a pure function of its
  input frame and its own instance state (e.g. `IntegratedPackPower`
  accumulating `total_energy` across ticks is fine; reaching out to the
  database or `Context()` mid-`run` is not — ingress stages are the sole,
  deliberate exception, since fetching from InfluxDB *is* their job).
- `stage` does not import `pipeline`. `pipeline` imports `stage` (a `Stage`
  is a node in the graph `pipeline_generator.py` builds), never the reverse.

## `pipeline`

**Owns:** turning a flat list of `Stage` instances into a scheduled,
running system: `PipelineGenerator` (graph construction, splitting into
same-rate subgraphs, generating ingress stages for unproduced signals),
`Pipeline` (topological execution of one subgraph), `Scheduler` (a
frequency-based min-heap runner, clock-injectable for tests), and
`Executor` (the top-level object that owns the compute scheduler, one
ingress scheduler *per ingress pipeline* running on its own thread, the
writer, and the `WorkerControl`).

**Contract:**
- `pipeline/protocols.py` defines the seams other modules implement against:
  `RunnablePipeline` (what `Scheduler` needs — `frequency`, `name`, `run()`),
  `SchedulerObserver` (what `TimingStats` implements), `FrameWriter` /
  `BatchFrameWriter` (what `db.sunbeamdb` implements). Depend on these
  protocols, not on `Pipeline` or `EventWriter` concretely, wherever you
  want a fake in tests.
- `Executor` is constructed two ways: `Executor.from_event(event_name,
  engine, ...)` does the full production wiring (reads `events.toml`,
  builds the stage graph, opens a `QueuedEventWriter`); the plain
  `Executor(pipelines, ingress_pipelines, writer, control, ...)`
  constructor takes everything pre-built, which is what
  `tests/infrastructure/test_executor.py` uses with fakes for every
  argument. If you need to test executor *behavior* (stop/crash/completion
  handling), use the second form — don't reach for `from_event`.
- `pipeline` imports `config` (`pipeline_generator.py` reads `Context` for
  the InfluxDB connection details used to build ingress stages, and
  `EventManager` via `from_event`), `state`, `stage`, `db.sunbeamdb` (for
  the concrete writer types used by `from_event`), and `orchestration`
  (for `WorkerControl`). It does not import `server`.

## `orchestration`

**Owns:** the worker-to-server protocol: `WorkerControl` (the abstract
interface `Executor` drives — `should_stop`, `set_stage`, `complete`,
etc.), its two/three implementations (`ServerlessWorkerControl`,
`OrchestratedWorkerControl`), `OrchestratorClient` (the HTTP client that
talks to the broker's `/workers/*` routes), and `bootstrap.build_control`
(decides which `WorkerControl` a worker process should use — see
[`USAGE.md`](USAGE.md) for the three resulting modes).

**The control loop, concretely.** Once `Executor.run()` starts an
`OrchestratedWorkerControl`, it spawns one background thread
(`_run_control_loop`) that owns all contact with the broker for the rest
of the worker's life — the scheduler threads never block on network I/O,
they only ever check `control.should_stop()`, a plain in-memory flag. While
that flag is unset, each iteration of the control thread (it wakes up
roughly every 100ms) does two things on independent ~1-second timers:
**asks permission** — polls `GET /workers/{id}/permission`; a `false`
response (the broker requested a stop, or the worker is already in a
terminal state) sets the local stop flag and stores the broker's `reason`
as the worker's status message, and the *same thing happens* if the broker
is simply unreachable (a connection error is treated as "not allowed to
continue," never as "assume everything's fine") — and **sends a
heartbeat** — `POST /workers/{id}/heartbeat`, status `"running"`, carrying
whatever `current_stage` the executor last reported via
`control.set_stage(...)` (called every time a pipeline produces output).
The instant the stop flag flips — from a denied permission poll, or from a
purely **local** stop such as a crashed ingress pipeline calling
`request_stop()` directly, which never round-trips through the broker at
all — both the permission poll and the heartbeat stop firing entirely; the
worker simply goes quiet on this channel from the broker's point of view
until it reports terminal status. Separately, and on a much slower cadence
(every ~5 seconds, driven by the scheduler's `on_tick` callback rather than
the control thread), the executor's output manager assembles a **metrics**
snapshot — per-pipeline/per-stage timing, idle/busy percentages, writer
queue depth — and calls `control.report_metrics(...)`, `POST`ing it to
`/workers/{id}/metrics` (see [`API.md`](API.md#get-workersworker_idmetrics)
for the exact payload — it's the same JSON structure behind the periodic
`timing idle=...%` log line). Finally, when the executor's scheduler loop
exits for any reason, `Executor.run()` calls `control.complete(success=,
message=)` exactly once, `POST`ing to `/workers/{id}/complete`, before
tearing down the control thread — this is the one call in the whole loop
that always fires regardless of *why* the worker is stopping, and it's
what the broker actually uses to mark the run terminal (a worker that goes
quiet without ever calling `complete` is instead caught later by
`WatchdogService`'s heartbeat-timeout sweep — see
[`API.md`](API.md#workers) and `server/services/watchdog_service.py`).

**Contract:**
- `WorkerControl` is the *only* thing `Executor` knows about orchestration.
  It never imports `OrchestratorClient` or `requests` directly. This is why
  `tests/infrastructure/test_executor.py` can drive full executor lifecycle
  tests with a `FakeControl` that has no network code at all.
- `OrchestratedWorkerControl` polls `/workers/{id}/permission` and posts
  heartbeats on its own background thread — `Executor` calls
  `control.should_stop()` from the scheduler loop and never blocks on
  network I/O itself.
- `orchestration` imports `config` (for `Context`) only. It does not import
  `pipeline`, `stage`, `state`, or `server` — the server is a peer speaking
  the same HTTP protocol from the other side, in `server/routes/workers.py`
  and `server/services/worker_service.py`, not a shared dependency.

## `server`

**Owns:** the FastAPI broker process — the `WorkerRun`/`Event`/`Signal`
tables' authoritative writer, Docker container lifecycle for launched
workers, the `WatchdogService` that reconciles worker state against
container reality, and the read-only telemetry query/stream API
(`server/routes/data.py`; see [`API.md`](API.md)).

**Contract:**
- Routes are thin: they parse/validate input, call a `server/services/*`
  function, and translate results/exceptions to HTTP status codes. Business
  logic lives in `services/`, not in `routes/`, specifically so services are
  testable via `TestClient`-free unit tests against a SQLite session (see
  `tests/infrastructure/test_worker_service.py`,
  `test_watchdog_service.py`, `test_data_service.py`).
- Every service takes its `Session`/`Engine`/Docker client as a constructor
  or call argument rather than importing a module-level singleton — this is
  what lets `WorkerService(fake_docker_client)` and
  `WatchdogService(session_factory=...)` run against SQLite in-process,
  with no real Postgres or Docker daemon, in the default test suite. See
  `server/db.py`'s lazy `get_engine()`/`get_session_factory()` — nothing
  resolves a database connection at import time.
- `server` is the only module allowed to import `docker` (the Python SDK)
  and to construct engines from `Context().sunbeam_db`. Workers never talk
  to Docker.
- `server` imports `stage` (only `StageLibrary`, to discover pipeline
  editions and validate them — see `PipelineService`) and `db`. It does
  *not* import `pipeline`, `state`, or `orchestration` — the broker never
  runs a pipeline itself, only launches and supervises worker processes
  that do.

## Cross-cutting rules

- **Timezone-aware datetimes only**, everywhere data crosses a module
  boundary or hits the database. `db.sunbeamdb.models` uses
  `DateTime(timezone=True)` throughout; `server/services/data_service.py`'s
  `resolve_time_window` rejects naive input outright rather than guessing a
  timezone. SQLite (used in the default test suite) silently drops tzinfo —
  that's why timezone-sensitive behavior has its own Postgres-marked tests
  under `tests/postgres/` (see [`ALEMBIC.md`](ALEMBIC.md) and the main
  README's testing section).
- **Dependency injection over module-level singletons**, except for
  `Context`, which is the one deliberate exception (see above). If you're
  adding a new service/manager, give it a constructor argument for
  anything that talks to the outside world (DB session, Docker client,
  HTTP client, clock) even if production always passes the same default —
  that argument is what a test replaces with a fake.
- **A module's `tests/infrastructure/` coverage roughly mirrors this
  document.** If you add a new cross-module contract, add or extend a test
  file alongside it; if you're not sure where a new module's tests belong,
  the answer is usually "wherever this document would describe its
  contract."
