# Improvement Plan

A review of the whole repository, written from the point of view of a senior engineer who wants
**less code, fewer moving parts, and failures that are loud instead of silent**. Nothing here adds
features; most items delete code. Each item says where the code is, what is wrong, and what to do.

Baseline when this was written (branch `connections`, 2026-11-03):

- `pytest`: **2 failing**, 415 passing (see 0.1)
- `ruff check .`: **1 error** (`pipeline/pipeline.py:67`, F841, from the same commented-out code as 0.1)
- `mypy .`: clean

Suggested order: section 0, then 1, then 2 (mostly deletion, low risk), then 3 and 4 (structural).

## Status (2026-11-05, uncommitted)

Implemented and passing `pytest` (431 tests), `ruff` and `mypy`: everything below **except**:

- **1.6 (stale realtime values): not done.** The right max-age depends on how late live telemetry lands in
  InfluxDB, which can't be measured while it's down. A guessed threshold could fail every realtime run.
- **3.2 (split the Executor): partly done.** It's still one class (the tests construct it directly), but `run()` now
  has a single cleanup-then-report path, `signal_completion`/`_completed` are gone, and the output-manager matrix is a
  short `_output_managers()`.
- **4 (trim docs): not done.** Docs were updated to match the code, not trimmed.
- **3.5: `MetricsCache` left as is.** It's a small, clear class-level cache, and tests reach into it.

Reversed after review (2026-11-05):

- **Per-edition extras and images kept.** Editions will need conflicting dependencies once they're implemented, so
  the `v3_0`/`v3_1`/`v4_0` extras, the `[tool.uv]` `v3_0`/`v3_1` conflict, and one `sunbeam-worker:<edition>` image per
  edition stay. (The `server`/`executor` conflict is still gone, so one environment can run the whole test suite.)
- **Ingress stays per edition** (`stage/v3_0/ingress.py`). Instead of forking files, an edition can reuse another's
  stage with `from = "<edition>"` in `stage_registry.toml`, or subclass it (`Stage` now accepts inherited
  `stage_name`/`inputs`/`outputs`/`frequency`). `v4_0` does both: `Offline_Ingress` is `from = "v3_0"`, and its
  `Ingress` subclasses v3_0's with `data_grace_s = 1000`.
- **`event.status` kept** (no migration), for a future dashboard view of which events have been run. The writer
  sets it as before, minus `EventWriter.__del__`.
- **`/debug/viewer` deleted**: the stream's real client lives outside this repo.

**Not verified, needs Docker/InfluxDB:**

- `pytest -m postgres`: its fixtures now build the schema through Alembic.
- A realtime and a replay run against InfluxDB.
- `docker compose up --build`, plus a launch from a git ref.

---

## 0. CI is red on this branch

### 0.1 Two failing tests

- `TestNotReadyGrace::test_persistent_not_ready_raises_after_grace` fails because the `raise` in
  `Pipeline._handle_not_ready` (`pipeline/pipeline.py:68`) is commented out. See 1.4 for the fix.
- `TestBuildControl::test_hand_run_worker_registers_itself` fails because it reads the **real**
  `config/events.toml`. The `realtime` event was switched to `v4_0`, and the test expects `v3_0`.
  Tests that depend on the live config files break every time an event is edited. Point
  `EVENTS_PATH`/`VEHICLES_PATH` at a fixture file in tests (monkeypatch the constant, or have
  `EventManager` take a path).

---

## 1. Robustness: real bugs and silent failures

### 1.1 Any connection blip stops the worker
`orchestration/control.py:168` treats a single `ConnectionError` on the permission poll as
"server said stop". The comment at `:192` says the opposite was intended. Pick one on purpose.
Recommended: stop only after N consecutive failures (e.g. 10 polls ≈ 10 s), and delete the
contradicting comment.

### 1.2 Heartbeats stop as soon as a stop is requested
In `orchestration/control.py:185`, `status = "stopping" if self.should_stop() else "running"` can
never be `"stopping"`, because the next line skips the heartbeat when the stop event is set. The
watchdog still expects heartbeats in `STOP_REQUESTED`/`STOPPING` (`HEARTBEATING_STATUSES`), so a
shutdown that takes more than `heartbeat_timeout_s` (10 s, for example while flushing the writer
queue) is marked `LOST`.
**Fix:** keep heartbeating while stopping, with `status="stopping"`.

### 1.3 `complete()` can overwrite a terminal status
`WorkerService.complete` (`server/services/worker_service.py:386`) doesn't lock the row or check
`TERMINAL_WORKER_STATUSES`, unlike `heartbeat()` and the watchdog's `_resolve()`. A worker the
watchdog already marked `LOST`/`CANCELLED` can turn back into `COMPLETED`/`FAILED`. Use the same
`with_for_update=True` + terminal check.

### 1.4 Two competing "missing data" mechanisms, one disabled and one effectively off
- `Pipeline._handle_not_ready`: its raise is commented out, so it just logs at debug level.
- `Ingress.MissingDataTracker`: raises after `data_grace_s`. v4_0 sets this to **1000 s**
  (commit `4feca8b`), which looks like a workaround for it firing.

Keep **one** mechanism. The Ingress tracker has the better error message and sits where the data
actually goes missing. Delete `_handle_not_ready` and its grace bookkeeping (and the test that
covers it), then decide deliberately what the v4_0 grace should be. If v4_0 telemetry really has
long gaps, make the grace a per-edition setting next to the registry, not a forked file (see 2.1).

### 1.5 Realtime ingress hides misconfiguration
`db/telemetrydb/realtime_ingress.py:55` catches *every* exception (bad token, wrong URL, DNS
failure) and turns it into "no data". With the 1000 s grace, a broken connection looks like a
quiet car for about 16 minutes. At minimum, log the exception, rate-limited (e.g. first
occurrence plus every 30 s). Better: catch only timeout/connection errors, and let auth errors
(401/403/404) raise immediately.

### 1.6 Realtime ingress discards the sample's own timestamp
`RealtimeIngressQuerier` returns `{"time", "value"}`, but `Ingress.run` uses only `value`, and the
lookback is 10 s (`LOOKBACK`). A value up to 10 s old is written as if it were measured "now", and
a 1 Hz CAN signal polled at 10 Hz is written ten times with ten different timestamps. Stale data
is indistinguishable from fresh data.
**Fix (small):** treat a point older than ~2 polling periods as missing, so it feeds
`MissingDataTracker` instead of the database.

### 1.7 A stage that doesn't write an output silently repeats the old value
`State.from_frame` (`state/state.py:52`) swallows `KeyError` when a stage omits one of its declared
outputs. Downstream stages keep reading the previous tick's value, and it gets written to the DB.
A stage that doesn't produce what it declares is a bug: raise. Also rename the method to
`update(frame, signals) -> None`. It mutates `self` and returns it, so
`state = state.from_frame(...)` in `Pipeline.run` reads like a copy, which it isn't.

### 1.8 Success is reported before the data is written (realtime)
`Executor.run` calls `_report_outcome()` (`pipeline/executor.py:364`) and then closes the writer in
`finally` (`:382`). In realtime mode, the worker reports `COMPLETED` and *then* flushes the queue.
If the flush fails, the server already shows success. Replay closes the writer first (`:294`),
which is the correct order. Also, a `WriterThreadError` raised from the `finally` block hides
whatever exception was already propagating.
**Fix:** close the writer before reporting in both modes, and report the outcome once, at the end.

### 1.9 Nothing stops two workers running the same event
`launch_worker` and `register_worker` don't check for an existing non-terminal run of the event.
Two replays of one event each wipe the samples and COPY the same `(event_id, signal_id, ts)` keys,
so one writer thread dies on a primary-key violation. Two live runs write duplicate keys.
**Fix:** in both methods, refuse (409) if a non-terminal `WorkerRun` exists for `event_id`. For a
DB-level guarantee, add a partial unique index on `worker_run(event_id)` covering the non-terminal
statuses.

### 1.10 Edits to `events.toml` never reach the database
`EventManager.sync_events` uses `get_or_create_event`, so changing an existing event's `ends_at`,
`starts_at` or `pipeline_edition` does nothing to its DB row. The worker reads the TOML, and the
server reads the DB (for `is_past`, the connection-check window, and edition validation). They can
disagree about whether an event is a replay and which window to check.
**Fix:** upsert. Related: a worker built from a git ref uses *that commit's* `events.toml`.
Upserting doesn't fix that; it's worth a sentence in USAGE.md, or have the server pass the event's
window/edition to the worker in its environment.

### 1.11 `__del__` methods do I/O
- `EventWriter.__del__` → `close()` opens a DB session and marks the event `PROCESSED` during
  garbage collection or interpreter shutdown.
- `Sunbeam.__del__` disposes the engine.

Both are already closed explicitly. Delete both `__del__`s.

### 1.12 `Event.status` is misleading
`EventWriter` sets `ONGOING` on open and `PROCESSED` on close, **even when the run failed**
(`db/sunbeamdb/writer.py:28,95`). With two runs (see 1.9), the last writer to close wins. The
real state of an event is its latest `WorkerRun`. Either derive status from that, or drop the
column. Both are less code than keeping it correct.

### 1.13 Compose startup race
`docker-compose.yaml` gives `db` a healthcheck, but `server` uses a plain `depends_on: [db]` with
no `condition: service_healthy` and no `restart:` policy. The server's preflight calls
`SystemExit(1)` if Postgres isn't up yet, so a cold `docker compose up` can leave the server dead.
Add `condition: service_healthy`. Also, the named volume `telemetry_db_data` is declared but
unused (the db uses a bind mount).

### 1.14 Watchdog exit-code semantics are inverted
`watchdog_service.py:159`: a container that exits **0** without reporting is `FAILED`, while one
that exits **non-zero** is `LOST`. `LOST` should mean "we don't know what happened". A non-zero
exit is a known failure, so it should be `FAILED`, with the exit code in the message.

---

## 2. Delete: dead, duplicate, or vestigial code

| What | Where | Why it can go |
|---|---|---|
| v4_0 `ingress.py`, `offline_ingress.py`, `pack_power.py` | `stage/v4_0/` | Byte-identical to v3_0, apart from the grace default. Point the `v4_0` registry entries at the v3_0 modules, or move the ingress stages to `stage/common/`: they're infrastructure, not edition computation. About 175 lines. |
| `describe_subgraphs`, `topo_order_for_subgraph` | `pipeline/pipeline_generator.py` | Unused. |
| `cross_rate_edges` + `remove_cross_rate_edges` + rate sanity check | `pipeline/pipeline_generator.py` | Build the graph with only same-rate edges; the "one rate per component" check can't fail by construction. |
| `PipelineService` | `server/services/pipeline_service.py` | Duplicates `StageLibrary.pipeline_editions`, `WorkerService._validate_pipeline_edition` and `WorkerImageBuilder.local_tag`. The route becomes one line. |
| `TimingStats._make_table_unlocked`, `make_table`, `reset`, `late_ns` | `pipeline/timing.py` | The table recomputes the metrics payload; the rest is never read or called. See 3.1. |
| `stop_on_error` parameter and its silent `except` | `pipeline/scheduler.py` | Every caller passes `True`. |
| `Executor.is_stopped`, `signal_completion`/`_completed` | `pipeline/executor.py` | `is_stopped` is unused; completion is only ever signalled by the replay path itself, so it can just return. |
| `heartbeat_now`, `request_stop(reason)` on `WorkerControl` | `orchestration/control.py` | Internal detail / never passed a reason. |
| Commented-out `stop_requested`, `WorkerPermissionResponse.stop_requested` | `orchestration/client.py`, `server/schemas.py` | The client never reads it. |
| `dispose_engine` | `server/db.py` | Unused. |
| `PipelineWorker`, `build_workers`' return value, `on_shutdown` | `server/main.py` | The mapping is built and thrown away; `on_shutdown` is `pass`. |
| `seed_data.main()`, `DATABASE_URL` | `db/sunbeamdb/seed_data.py` | Creates a hardcoded "Bench Test 2026-03-31" event, and reads `DATABASE_URL`, not `SUNBEAM_DATABASE_URL`. |
| `__main__` blocks | `realtime_ingress.py`, `offline_ingress.py`, `init_db.py` | Dev scratch. Move anything worth keeping to `scripts/`. |
| `/debug/viewer` + `server/static/stream_viewer.html` | `server/routes/debug.py` | Self-described as temporary until the dashboard consumes the stream. |
| `created_signals` | `config/signals.py` | Unused list. |
| `pyzmq`, `coolname` | `pyproject.toml` `server` extra | Never imported. |
| Pre-Alembic stamp shim + `create_schema` + their tests | `server/migrations.py`, `db/sunbeamdb/init_db.py`, `tests/postgres/` | The docstring says to delete it once every deployment is stamped. If that's true now (it probably is), delete it. |
| Unused named volume | `docker-compose.yaml` | See 1.13. |

### 2.1 Drop the dependency-extras conflicts
`[tool.uv].conflicts` declares `server`/`executor` and `v3_0`/`v3_1` as mutually exclusive. **They
aren't.** Re-locking without the `conflicts` block resolves every package to a single version (checked
in a scratch copy: 80 packages, no duplicates). The conflicts only cause:

- `--all-extras` failing, and the `uv sync --extra executor --extra v3_0 --group test --group dev`
  incantation
- `fastapi`/`docker`/`alembic` duplicated into the `test`/`dev` groups to work around the conflict
- a two-step `uv sync` in `pipeline/Dockerfile`, and a section of USAGE.md and README explaining it
- `v3_1`, an empty extra referenced only by the conflict and the docs

Simplest end state: shared deps in `[project].dependencies`, plus `server` (fastapi, uvicorn,
docker, alembic) and `worker` (networkx, rich) extras. (scipy, which `EnergyVOLExtrapolated` imports, currently only arrives
transitively through data_tools; declare it explicitly while you're there). No per-edition extras until an
edition actually needs a dependency the others can't have. Then `uv sync --all-extras --all-groups`
is the dev setup.

---

## 3. Simplify: structural changes that remove concepts

### 3.1 Metrics: one snapshot, many sinks
There are currently five output-manager classes (`RichOutputManager`, `RichReplayOutputManager`,
`LoggingOutputManager`, `CombinedOutputManager`, plus the `_resolve_output_manager` matrix). They
exist because the Rich table and the server reporter **both** call `*_and_reset()` on the same
`TimingStats`, so they "can never both run" (realtime has a comment saying so).

Instead, make one periodic reporter take **one** snapshot per interval (`metrics_payload_and_reset`)
and hand it to every sink:

- `control.report_metrics(payload)` (a no-op when serverless)
- a log line (when not interactive)
- a Rich table rendered *from the payload* (when interactive and realtime)

The replay progress bar stays as its own small thing. This deletes `CombinedOutputManager`, the
duplicated table math in `TimingStats`, the "never both" rule, and most of
`_resolve_output_manager`. A hand-run, server-registered worker in a terminal also gets the live table, which
today it can't.

### 3.2 Split the Executor's two modes
`Executor` is one class with `if replay_window` branches, a crash flag shared across threads,
`_completed`, and an output-manager matrix. Realtime and replay share very little: the state, the
writer, the control, and "report the outcome". Two plain functions, `run_realtime(...)` and
`run_replay(...)`, plus one `finish(control, writer, error)` that does 1.8 correctly, would be
shorter and easier to follow than the class.

### 3.3 `sunbeam.py` → one `main()`
- `if "event_name" in args` is always true, so the `None` check after it is dead.
- The `SUNBEAM_WORKER_RUN_ID` check is done here and again in `build_control`.
- `EventManager().get_event()` is called three times per run (here, `Executor.from_event`,
  `build_control`).
- `Sunbeam` is a class with one method and a `__del__`.

Load the event once, pass the `EventConfig` down, and the file is about 60 lines.

### 3.4 Errors map to HTTP status in one place
Every route repeats `try/except ValueError → 400/409`, `LookupError → 404`, or
`if x is None: raise HTTPException(404)`. Have services raise `LookupError` for not-found and
`ValueError` for bad input, and register two exception handlers in `create_app`. That deletes
most of the route-level boilerplate in `workers.py`, `connections.py` and `data.py`, and makes the
mapping consistent (today the same "no such event" gives 400 in one route and 404 in another).

### 3.5 Lazy singletons with `functools.cache`
`get_worker_service`, `get_connection_service`, `server/db.py`'s `_engine`/`_session_factory`,
and `MetricsCache`'s class-level store are all hand-rolled `global` singletons. Each can become
`@functools.cache def get_x(): return X()`. FastAPI's `dependency_overrides` still works for tests.

### 3.6 Merge the watchdog's two check paths
`_check_worker` and `_check_external_worker` repeat the stop-grace, heartbeat-timeout and
startup-grace rules with small differences (one uses `started_at or created_at`, the other only
`created_at`). Do the container-only checks first (container gone, exited, kill after grace),
then run **one** shared set of timing rules for both kinds.

### 3.7 One time helper module
There are three `utcnow()`s (worker_service, watchdog, connection_service). The "SQLite dropped the
tzinfo, assume UTC" fix exists in `models.Event.is_past`, `connection_check._as_utc`, and
`data_service._epoch_ms`/`_epoch_us`. `is_past` exists on both `EventConfig` and `Event`. Put
`utcnow`, `as_utc` and `is_past(ends_at)` in one small module and use them everywhere.

### 3.8 Small cleanups
- `routes/workers.py`: `active_only` hand-lists the non-terminal statuses. Use
  `~WorkerRun.status.in_(TERMINAL_WORKER_STATUSES)`, so a new status can't be forgotten.
- `isinstance(control, OrchestratedWorkerControl)` in `sunbeam.py` and `Executor` defeats the
  `WorkerControl` interface. An `is_orchestrated` attribute is enough (and after 3.1, the executor
  doesn't need to know).
- `StageLibrary.__init__` has a `try/except KeyError` that can't fire (`stage_library.py:19`), and
  a mutable `set_pipeline_edition`. Make it a constructor taking an optional edition.
- `OrchestratorClient`: replace the four near-identical POSTs with a `_post(path, json)` helper.
- `EventManager.sync_events` re-parses raw dicts instead of using `EventConfig.from_raw`.
- `_start_container` sets `started_at` twice.
- `server/Dockerfile`: installs uv with pip and installs `build-essential`, and `uv run` doesn't
  pass `--no-sync`. Match `pipeline/Dockerfile` (uv base image, `--no-sync`).
- The dashboard polls `/workers?active_only=false` every 3 s, which returns *every run ever*. Add
  a `limit` (e.g. 50 most recent).

---

## 4. Process and docs

- **Fix CI before merging this branch** (section 0). The CI workflow already runs ruff, mypy and
  pytest, so the failures above would block a PR to `main`.
- **Tests shouldn't read real config.** Beyond 0.1, grep tests for `EVENTS_PATH`/`events.toml`
  usage and give them fixtures.
- **Docs are ~1,800 lines and already drifting.** `v3_1` is described as if it exists; CONTRACTS.md
  and API.md restate code that changes often. Point API.md at FastAPI's generated `/docs`, keep only
  the SSE protocol notes, and trim CONTRACTS.md to the import rules plus a paragraph per module.
  Docs nobody updates are worse than no docs.

---

## 5. Leave alone

These are appropriately sized and well done:

- `QueuedEventWriter` (bounded queue, never drops, surfaces thread death)
- The COPY write path and its rationale
- The heap-based scheduler and its monotonic/wall-clock mapping
- `connection_check` (one check shared by the server and worker preflight)
- The dependency injection used for tests (fakes for docker, git, the clock, the checker)
- `data_service`'s cursor-based stream (microsecond cursors, idle yields so disconnects are noticed)
- The Alembic setup with a lock timeout
