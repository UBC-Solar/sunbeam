# Module Contracts

What each module owns and may assume about the others. These boundaries are what let most of the test suite run
without a server, Docker or Postgres. If you need an import this says shouldn't exist, the code probably belongs
elsewhere; if the contract really should change, change this file in the same PR.

## Imports

```
state            imports nothing from sunbeam
db ────────────► state
stage ─────────► state, db         (ingress stages only, for the InfluxDB queriers)
config ────────► db
orchestration ─► config

pipeline ──────► config, db, state, stage, orchestration     (the worker: sunbeam.py)
server ────────► config, db, stage                           (the server: server/main.py)
```

Nothing imports `pipeline` or `server`; each is assembled into one of the two processes. The worker and server meet
only over HTTP: `orchestration.OrchestratorClient` calls the `/workers/*` routes that `server/routes/workers.py`
answers, and neither imports the other. So the server's tests never import `orchestration`, and a worker image never
installs the `server` extra.

## Processes

```
 Dashboard ── HTTP + SSE ──► Server ◄── Docker API ──► Docker daemon
                              ▲   │                       │ runs
                     register,│   │ events, workers,      ▼
          heartbeat, permission,  │ connections        Worker ── reads raw CAN ──► InfluxDB
               complete, metrics  ▼                       │
                              Postgres ◄── samples ───────┘
```

A launched worker is a container the server starts, inspects and kills through Docker; inside, it talks to
Postgres, InfluxDB and the server like any other worker. The server and workers each connect to Postgres directly:
samples never pass through the server. See [`USAGE.md`](USAGE.md#the-three-ways-a-cli-worker-can-run) for the three
ways a worker can run, including with no server at all.

## `config`

**Owns** reading the environment and `config/vehicles.toml`.

- `Context` (`config/context.py`) reads connection details from the environment each time a property is accessed:
  `sunbeam_db`, `telemetry_db` (`SUNBEAM_INFLUX_*`, raising if one is unset), `sunbeam_server`, and the server's
  settings for the workers it launches. `TelemetryDB.as_env()` turns a connection back into those variables.
- `VehicleManager` and `SignalManager` sync vehicles and each event's localized signals into the database at
  server startup (create by name, never delete); `add_signals` does the same for one event when it's created or
  edited.
- `load_event(engine, name)` (`config/events.py`) is how a worker reads its event, which exists only in the
  database. Its `EventConfig.is_past()` decides between replay and realtime.

**Contract**
- The environment is the only source of connection details: code calls `Context()` instead of having them passed
  down, and a worker never looks up its InfluxDB connection (the server resolves the dashboard's choice and passes
  it in). This is the codebase's one deliberate global; don't add another. Tests set the variables with
  `monkeypatch`.
- `config` imports only `db`.

## `db`

**Owns** the schema (`db/sunbeamdb/models.py`, built by [Alembic](ALEMBIC.md)), time helpers (`db/timeutil.py`),
writing samples, and reading InfluxDB (`db/telemetrydb/`: `RealtimeIngressQuerier` for the latest values,
`OfflineIngressQuerier` for a whole window, and the connection check the server and worker preflight share).

`EventWriter` bulk-inserts a frame's values, and loads a replay's `TimeSeries` with `COPY`; with `reprocess=True`
it first deletes the event's samples. `QueuedEventWriter` puts a bounded queue and flush thread in front of it:
frames are never dropped (a full queue blocks), a dead flush thread makes the next write or `close()` raise
`WriterThreadError`, and `close()` waits for everything queued.

| Table | Holds |
|---|---|
| `vehicle` | One row per car, from `config/vehicles.toml`. |
| `event` | One per race or session, created in the dashboard. `stages` are the edition's stages it runs. `status` goes `unprocessed` → `ongoing` while a worker writes it → `processed` when that worker's writes finish, whether or not it succeeded (that's its `worker_run`'s status). At most one active run per event. |
| `signal` | One per signal the event's date has a localization for, with unit, frequency and source. |
| `worker_run` | One per worker, ever: what the server supervises. `kind` is `container` (launched) or `external` (registered itself). |
| `connection` | InfluxDB connections a worker can be launched with. |
| `sample` | The output: one row per `(event, signal, ts)`, for every value ingress or a stage produced. |

**`sample` is a TimescaleDB hypertable**, split into one-day chunks on `ts`, and indexed on
`(event_id, signal_id, ts DESC)` and `(event_id, ts DESC)`, the shapes `server/services/data_service.py` queries.
Writes only touch the current chunk's indexes, and a windowed read only visits the chunks it overlaps, so both stay
fast as years of data accumulate. Code reads and writes it as an ordinary table. What that obliges:

- Its primary key is `(event_id, signal_id, ts)` rather than an `id`, because TimescaleDB requires every unique
  constraint on a hypertable to include the partitioning column.
- The database must be the `timescale/timescaledb` image; on plain Postgres the baseline migration fails.
- The chunk interval suits today's volume. Revisit it if the signal count, rates, or concurrent events grow by an
  order of magnitude.
- SQLite, which the default tests use, has none of this. Hypertable behavior is tested in `tests/postgres/`.

**Contract**
- `models.py` is the one definition of the schema; the server and workers both import it.
- The writers implement `pipeline/protocols.py`'s `FrameWriter`/`BatchFrameWriter`, and the pipeline depends on
  those protocols, so tests use a `RecordingWriter` instead.
- `db` imports only `state`. (`queued_writer.py` imports `pipeline.protocols` for type checking only; a real import
  would be circular.)

## `state`

**Owns** `State`, the latest value of every signal, and `Frame`/`FrameView`, a stage's view of it.

- `State` is a dict behind one lock, with no idea of stages or the database. It's the only way pipelines on
  different threads exchange values: `Pipeline.run` reads a frame from it before each stage and writes the outputs
  back after.
- A stage sees only the frame for its own inputs and outputs, so it can be tested with a bare `FrameView`.
- `state` imports nothing from sunbeam.

## `stage`

**Owns** the `Stage` base class and `StageLibrary`, which resolves an edition's stage names to classes through
`stage/stage_registry.toml`. See [`STAGES.md`](STAGES.md).

- A stage declares `stage_name`, `inputs`, `outputs` and `frequency` on the class (or inherits them), checked when
  the class is defined. Ingress stages override some as properties, since each instance differs.
- `run(frame)` depends only on the frame and the stage's own state; ingress is the only stage that does I/O.
- `stage` imports `state`, and the ingress stages import `db.telemetrydb`. Nothing else in `stage` imports `db`.

## `pipeline`

**Owns** turning stages into a running worker: `pipeline_generator.py` builds the graph, adds ingress for signals
nothing produces, and splits it into same-rate `Pipeline`s; the schedulers run them (`RealtimeScheduler` with the
wall clock, `ReplayScheduler` through a window as fast as it can, both with an injectable clock); `output.py` shows
progress and reports metrics; `Executor` owns all of it.

**Contract**
- `pipeline/protocols.py` holds the seams: `RunnablePipeline`, `SchedulerObserver`, `FrameWriter`,
  `BatchFrameWriter`. Depend on those wherever a test wants a fake.
- `Executor.from_event(event, engine, ...)` does the production wiring. The plain constructor takes everything
  built, which is how `tests/infrastructure/test_executor.py` tests stop, crash and completion handling with fakes.
- `pipeline` never imports `server`.

## `orchestration`

**Owns** the worker's side of the server protocol: `WorkerControl` (what `Executor` drives), its implementations
(`ServerlessWorkerControl`, `OrchestratedWorkerControl`), `OrchestratorClient` (HTTP), and `build_control`, which
picks one.

**The control loop.** `OrchestratedWorkerControl` runs one background thread that does all the talking to the
server; schedulers only check `should_stop()`, an in-memory flag. About every second the thread polls
`/permission` and sends a heartbeat with the current stage. A denied poll sets the flag; so does about ten failed
polls in a row, so a blip doesn't end a run but a vanished server does. A local stop (a crashed ingress pipeline)
sets it too. Once stopping, heartbeats continue as `stopping` until the worker finishes, so the watchdog can tell
winding down from dead. Separately, the scheduler's tick sends metrics (every 5 s realtime, 0.5 s replay). When the
scheduler loop exits for any reason, `Executor.run()` waits for the writes to finish, then calls `complete()` exactly
once. A worker that dies without it is caught by the watchdog's heartbeat timeout.

**Contract**
- `WorkerControl` is all `Executor` knows of orchestration, so executor tests use a `FakeControl` with no network.
- `orchestration` imports only `config`.

## `server`

**Owns** the FastAPI process: the authoritative writer of `event`, `signal`, `worker_run` and `connection`, Docker
container lifecycle, the `WatchdogService` that reconciles runs with their containers, and the telemetry query and
stream routes. Its API reference is `/docs` on a running server.

**Contract**
- Routes parse input, call a function in `server/services/`, and return. Services raise `LookupError` (404),
  `Conflict` (409) or `ValueError` (400), mapped once in `server/main.py`.
- Services take their session, engine or Docker client as arguments, and nothing connects at import time
  (`server/db.py` is lazy), so the services run against SQLite and fake Docker in the default tests.
- Only `server` imports `docker`. It imports `stage` only for `StageLibrary`.

## Across modules

- **Timezone-aware datetimes only**, wherever data crosses a module or reaches the database. SQLite drops time
  zones, so that behavior is tested in `tests/postgres/`.
- **Pass in anything that talks to the outside world** (session, Docker client, HTTP client, clock) as an argument,
  even if production always passes the same one: that's what a test replaces. `Context` is the one exception.
