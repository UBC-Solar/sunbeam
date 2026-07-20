# API reference

The broker (`server/main.py`, FastAPI) exposes four route groups:
`/events`, `/workers`, `/pipeline-editions`, and the telemetry data API
(`/events/{event_name}/signals*`, also mounted under `/events`). All
request/response bodies are JSON (Pydantic models in `server/schemas.py`)
except the two SSE endpoints, which are `text/event-stream`.

Base URL is whatever `context.toml`'s `[client].sunbeam-broker` resolves
to for your environment — `http://localhost:8000` for a local
`docker compose up` or CLI run against the default profile. FastAPI's
interactive docs are also always available at `/docs` (Swagger) and
`/redoc` on a running server — this document is the narrative companion,
not a replacement.

For how a worker uses the `/workers/*` endpoints against itself, see
[`USAGE.md`](USAGE.md#the-three-ways-a-cli-worker-can-run). For the schema
these endpoints read from, see `db/sunbeamdb/models.py`.

## Conventions

- **Events are identified by name** in the telemetry data API (`event_name`
  path segment) and by integer `event_id` in the worker-launch API — this
  mirrors what each caller already knows: a human/dashboard picking an
  event to query knows its name; `WorkerRun.event_id` is a foreign key, so
  the launch flow uses the ID directly. `Event.name` is a unique column
  (`db/sunbeamdb/models.py`), so name-based lookup is always unambiguous.
- **Signals are identified by name** (`CanonicalName` string, e.g.
  `"VehicleSpeed"`), scoped to an event — the same signal name can exist
  under multiple events with different `Signal.id`s.
- **Timestamps in request bodies/params must be timezone-aware** ISO 8601
  (e.g. `2026-07-20T12:00:00Z` or `...+00:00`). Naive timestamps are
  rejected with `422`, not silently assumed UTC.
- **Timestamps in the data API's responses** are epoch **milliseconds**
  (integers) for `timestamps` arrays — chosen because that's what
  JS `Date`/charting libraries consume natively without a parse step.
  Everything else (worker records, window echo) is ISO 8601 via Pydantic's
  default `datetime` serialization.
- **Errors** are FastAPI's standard `{"detail": "..."}` shape, with status
  codes used consistently: `404` for "the named/IDed thing doesn't exist",
  `400` for "the request referenced a real thing but with an invalid
  combination" (e.g. an unknown pipeline edition), `422` for "the request
  itself is malformed" (validation errors, including the data API's window
  parameter rules).

---

## Events

### `GET /events`

List all events, newest-starting first.

```json
[
  {
    "id": 2, "name": "realtime", "vehicle_id": 1,
    "starts_at": "2026-05-30T01:00:00Z", "ends_at": null,
    "pipeline_edition": "v3_0", "status": "ongoing",
    "description": "Ongoing"
  }
]
```

`status` is one of `unprocessed`, `ongoing`, `processed` (`EventStatus`) —
set to `ongoing` when a worker opens an `EventWriter` for the event, and
`processed` when that writer closes (executor shutdown, for any reason,
including failure — see the note in `USAGE.md` about `--reprocess`).

### `GET /events/{event_id}`

Same shape as one element above. `404` if `event_id` doesn't exist.

---

## Workers

Worker rows (`WorkerRun`) are created two ways — see
[`USAGE.md`](USAGE.md#the-three-ways-a-cli-worker-can-run) for the
distinction between `kind: "container"` and `kind: "external"`. The
individual endpoints below (`/heartbeat`, `/permission`, `/metrics`,
`/complete`) are documented one at a time; for the narrative of how a
running worker actually calls them — the permission-poll/heartbeat loop,
what happens the instant a stop is requested, and when `/complete` fires —
see [`CONTRACTS.md`'s orchestration section](CONTRACTS.md#orchestration).

### `WorkerRunRead` shape

Every worker endpoint below returns (or lists) this shape:

```json
{
  "id": "b3f1...-uuid", "event_id": 2, "pipeline_edition": "v3_0",
  "image_tag": "sunbeam-worker:v3_0", "status": "running", "kind": "container",
  "host": "a1b2c3d4e5f6", "container_id": "a1b2c3...", "container_name": "sunbeam-worker-b3f1a2c4",
  "current_stage": "Array", "status_message": null, "stop_requested": false,
  "created_at": "2026-07-20T12:00:00Z", "started_at": "2026-07-20T12:00:01Z",
  "last_heartbeat_at": "2026-07-20T12:05:32Z", "stopped_at": null,
  "failure_reason": null
}
```

`status` is one of `requested`, `starting`, `running`, `stop_requested`,
`stopping`, `completed`, `failed`, `lost`, `cancelled` (`WorkerStatus`); the
first five are non-terminal, the last four are terminal
(`TERMINAL_WORKER_STATUSES`). `image_tag`/`container_id`/`container_name`
are `null` for `kind: "external"` workers (nothing to launch or inspect —
see `USAGE.md`).

### `GET /workers?active_only=<bool>`

List workers, newest-created first. `active_only=true` filters to
non-terminal statuses only.

### `POST /workers/launch`

Server-launches a Docker container. This is what the dashboard calls.

```json
// request
{"event_id": 2, "pipeline_edition": "v3_0"}
```

Creates a `WorkerRun` (`kind: "container"`, `status: "requested"` then
immediately `"starting"`), runs
`sunbeam-worker:{pipeline_edition}`, and passes `SUNBEAM_WORKER_RUN_ID` /
`SUNBEAM_EVENT_NAME` / `SUNBEAM_PIPELINE_EDITION` as container environment.
Returns the `WorkerRunRead`. `400` if `pipeline_edition` isn't a known
edition or `event_id` doesn't exist. If the Docker launch itself fails
(daemon unreachable, image missing, ...), the row is still created but
immediately marked `status: "failed"` with `failure_reason` set — the
response is `200`, not an error; check `status` in the body.

### `POST /workers/register`

Self-registers an externally-run worker (`uv run sunbeam.py` by hand — see
[`USAGE.md`](USAGE.md#the-three-ways-a-cli-worker-can-run)). The **server** issues
the run ID; this is the only way a `kind: "external"` `WorkerRun` gets
created.

```json
// request
{"event_name": "realtime", "pipeline_edition": "v3_0", "host": "joshuas-laptop"}
```

Returns a `WorkerRunRead` with `status: "starting"`, `kind: "external"`,
`image_tag: null`. `400` for an unknown event name or pipeline edition.

### `POST /workers/{worker_id}/heartbeat`

Called periodically by the worker's `OrchestratedWorkerControl` (roughly
every second). Not something you call manually in normal use.

```json
// request
{"status": "running", "current_stage": "Array", "status_message": null, "host": "a1b2c3d4e5f6"}
```

If a stop was already requested server-side (see below), the stored status
is forced to `stop_requested` regardless of what the worker reports — the
server is the source of truth for the target lifecycle state. A heartbeat
against an already-terminal worker is accepted but ignored (returns the
unchanged row) — this is the losing side of the row-lock race the watchdog
also participates in; see [`CONTRACTS.md`](CONTRACTS.md) and
`server/services/watchdog_service.py`. `404` for an unknown `worker_id`.

### `GET /workers/{worker_id}/permission`

Polled periodically by `OrchestratedWorkerControl` to learn whether it
should keep running.

```json
{"allowed": true, "reason": null, "stop_requested": false}
```

`allowed: false` (with `stop_requested: true`) once a stop has been
requested or the worker has reached a terminal status; the worker's control
loop treats `allowed: false` as "stop now."

### `POST /workers/{worker_id}/stop`

Requests a graceful stop — sets `stop_requested`, `status: stop_requested`.
The worker notices on its next permission poll (roughly one second later)
and winds down; if it doesn't within the watchdog's stop-grace window, the
watchdog force-kills the container (container workers) or marks it `lost`
(external workers, which have nothing to kill). `404` for unknown
`worker_id`; a no-op (still `200`) if the worker is already terminal.

### `POST /workers/{worker_id}/complete`

Called by the worker itself when it finishes (success or failure) —
`Executor.run()`'s completion path. Not normally called by external
clients.

```json
{"success": true, "message": "Pipeline completed."}
```

Sets `status` to `completed` or `failed`, `stopped_at` to now,
`failure_reason` to `message` iff `success: false`, and clears any cached
metrics for the worker. `404` for unknown `worker_id`.

### `POST /workers/{worker_id}/metrics` → `204`

Periodic timing/writer-queue snapshot from a running worker (see
`WorkerMetricsReport` below). `404` if the worker doesn't exist or is
already terminal — a worker's final metrics push right before exit may
race its own `/complete` call and lose; this is expected and harmless.

### `GET /workers/{worker_id}/metrics`

The most recent metrics snapshot for a worker — purely in-memory
(`MetricsCache`), cleared on completion or server restart; `404` if none
have been reported yet.

```json
{
  "idle_pct": 85.2, "busy_pct": 14.8, "writer_ms": 3.16,
  "pipelines": [
    {
      "name": "Array", "total_ms": 12.4, "avg_ms": 0.07, "ticks": 178,
      "late_now_ms": 0.0, "late_max_ms": 1.2,
      "stages": [{"name": "Array", "total_ms": 12.4, "avg_ms": 0.07, "max_ms": 0.3, "calls": 178}]
    }
  ],
  "writer_queue": {
    "queue_depth": 1, "queue_capacity": 10000, "queue_high_water": 6,
    "frames_enqueued": 8912, "frames_written": 8911, "batches_flushed": 214,
    "avg_flush_ms": 7.82, "max_flush_ms": 41.0
  },
  "reported_at": "2026-07-20T12:05:35Z"
}
```

`writer_queue` is `null` until the worker's first periodic report after
its writer has flushed at least once. See the worker log line this same
payload drives: `timing idle=85.2% busy=14.8% writer=3.16ms queue=1/10000
(hwm 6) flush_avg=7.82ms ...`.

### `GET /workers/{worker_id}/logs?tail=<int>`

Last `tail` (default 500) lines of the container's stdout/stderr, with
Docker timestamps. `404` if the worker has no container (external workers,
or a container worker that failed before launching one) or the container
is gone.

### `GET /workers/{worker_id}/logs/stream`

SSE tail of the same log stream (`event: message`-free — plain
`data: <line>\n\n` per line, no `event:`/`id:` framing, unlike the data
stream below). Same `404` conditions as above.

---

## Pipeline editions

### `GET /pipeline-editions`

```json
["v3_0"]
```

Every top-level table key in `stage/stage_registry.toml` — see
[`STAGES.md`](STAGES.md#pipeline-editions-and-stage_registrytoml).

---

## Telemetry data

Two distinct endpoints for two distinct use cases — **don't reach for one
where the other fits**: the query endpoint is for post-mortem analysis over
an arbitrary window (opens a connection, returns a bounded response,
closes); the stream endpoint is for a live dashboard tailing recent data
across many signals (opens one long-lived connection, gets pushed batches
as they land). See the worked client in
`server/static/stream_viewer.html` and the discussion in
[`USAGE.md`](USAGE.md#debug-data-viewer).

### `GET /events/{event_name}/signals`

List every signal recorded for an event, with metadata.

```json
[
  {"name": "VehicleSpeed", "unit": "m/s", "frequency": 5.0, "source": "MDI"},
  {"name": "PackCurrent", "unit": "A", "frequency": 5.0, "source": "ECU"}
]
```

`404` for an unknown event name.

### `GET /events/{event_name}/signals/{signal_name}/data`

Windowed, single-signal query — **for post-mortem analysis**. Exactly one
of three window modes must be selected via query parameters:

| Mode | Parameters | Window |
|---|---|---|
| Between | `start`, `end` | `[start, end)` |
| Since | `start` only | `[start, now)` |
| Trailing | `last_seconds` | `[now − last_seconds, now)` |

`start`/`end` are timezone-aware ISO 8601 datetimes; `last_seconds` is a
positive number of seconds. Combining `last_seconds` with `start`/`end`, or
providing none of them, is a `422`. So is a naive datetime, or `start >=
end`.

`limit` (query param, default and max `360000` — ten hours of a 10 Hz
signal) bounds the response size. A window with more than `limit` samples
returns the **most recent** `limit` of them, with `truncated: true` — never
an error; narrow the window or raise `limit` (up to the max) if you need
more.

```
GET /events/realtime/signals/VehicleSpeed/data?last_seconds=600
```

```json
{
  "event_name": "realtime", "signal": "VehicleSpeed", "unit": "m/s", "frequency": 5.0,
  "start": "2026-07-20T11:50:00Z", "end": "2026-07-20T12:00:00Z",
  "count": 2841, "truncated": false,
  "timestamps": [1752968400123, 1752968400323, "..."],
  "values": [12.5, 12.6, "..."]
}
```

`404` for an unknown event or signal name.

### `GET /events/{event_name}/data/stream`

Multiplexed **Server-Sent Events** stream — **for a live dashboard**, one
connection carrying any number of signals. This is the endpoint the
`/debug/viewer` page demonstrates.

**Query parameters:**

| Param | Default | Meaning |
|---|---|---|
| `signals` | required | Comma-separated signal names, e.g. `VehicleSpeed,PackCurrent`. |
| `since` | none (tail from now) | Resume cursor, **epoch microseconds** — see resume behavior below. |
| `poll_interval_s` | `0.5` | How often the server polls the database for new rows (`0.1`–`5.0`). |

**Response**: `text/event-stream`. Three message kinds:

```
event: meta
data: {"VehicleSpeed": {"signal_id": 14, "unit": "m/s", "frequency": 5.0}, "PackCurrent": {...}}

event: data
id: 1752968412558123
data: {"VehicleSpeed": {"timestamps": [1752968412558], "values": [13.1]}, "PackCurrent": {"timestamps": [], "values": []}}

: keepalive
```

- **`meta`** — sent exactly once, immediately, before any data: per-signal
  `signal_id`/`unit`/`frequency`. If any requested signal doesn't exist for
  the event, the whole request fails with `404` **before** the stream
  opens — you never get a mid-stream error for this.
- **`data`** — one message per poll that found new rows, columnar per
  signal (`timestamps`/`values` arrays; a signal with nothing new in that
  poll gets empty arrays, not an absent key — every signal you subscribed
  to is always present). Carries `id: <cursor>`, the epoch-microsecond
  timestamp of the newest row in that batch.
- **`: keepalive`** — an SSE comment line (no `event:`), sent during quiet
  stretches so intermediary proxies don't time out an idle connection.
  Ignored by `EventSource` automatically.

**Resuming without gaps or duplicates:** every `data` message's `id:`
becomes the cursor for the *next* poll (strictly-greater-than comparison —
a sample at exactly the cursor timestamp is never redelivered). Reconnect
handling is automatic if you use the browser's native `EventSource`: on any
disconnect it retries on its own and sends the last received `id:` back as
an HTTP `Last-Event-ID` header, which the server honors over the `since`
query parameter. You do not need to track the cursor yourself or write any
reconnection logic — this is the entire point of using SSE here (see
`server/static/stream_viewer.html`'s `source.onerror` handler, which does
nothing but update a status label).

**Backfill + live handoff pattern**, for a dashboard widget coming online:
query `GET .../data?last_seconds=600` first, note the largest timestamp
received (convert ms → µs, i.e. `× 1000`), then open the stream with
`since=<that value>`. The query endpoint's half-open `[start, end)` window
and the stream's strictly-greater-than cursor comparison compose exactly —
no gap, no duplicate, at the seam.

`422` if `signals` is empty/missing or resolves to zero names after
trimming. `404` if the event or any named signal doesn't exist.
