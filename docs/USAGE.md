# Usage

Sunbeam runs either as a Docker system (a server that launches and supervises worker containers, with a dashboard)
or as a single worker you run with `uv run sunbeam.py`. For what a worker does, see [`STAGES.md`](STAGES.md); for
the server's API, open `/docs` on a running server (`http://localhost:8000/docs`).

## Dependencies

`pyproject.toml` has a few shared dependencies plus these extras and groups:

| Name | Kind | For |
|---|---|---|
| `server` | extra | The server: FastAPI, uvicorn, the Docker SDK, Alembic. |
| `executor` | extra | A worker: networkx, rich, scipy. |
| `v3_0`, `v3_1`, `v4_0`, ... | extra | One pipeline edition's stages, beyond `executor`. |
| `test` | group | pytest, httpx2, testcontainers. |
| `dev` | group | ruff, mypy. |

Each edition has its own extra because editions may need conflicting versions of a package. Each gets its own worker
image (`sunbeam-worker:<edition>`, built with `--extra executor --extra <edition>`), and editions that can't share an
environment are listed in `[tool.uv].conflicts`. That's also why `--all-extras` fails: name the extras.

```bash
uv sync --extra server --extra executor --extra v3_0 --all-groups   # everything, for v3_0 (what CI runs)
uv sync --extra server                                              # the server alone
uv sync --extra executor --extra v3_0                               # a v3_0 worker
```

`uv run` uses whatever the last `uv sync` installed; a bare `uv sync` removes the extras.

## Docker, with the server and dashboard

```bash
docker compose up --build
```

starts three services (`docker-compose.yaml`):

- **`db`**: TimescaleDB on `localhost:5432`.
- **`server`**: the FastAPI app on `localhost:8000`. On startup it migrates the database ([`ALEMBIC.md`](ALEMBIC.md)),
  syncs `vehicles.toml`, and builds a `sunbeam-worker:<edition>` image for each edition in `stage_registry.toml`
  (which is why the first start is slow).
- **`dashboard`**: a Vite dev server on `localhost:5173`. It calls the API at `/api`, which the dev server proxies
  to the server (`SUNBEAM_API_PROXY_TARGET`), so it works from other machines on the Tailnet too.

On the dashboard:

1. **Add event…** creates an event: its dates (UTC; no end means it's ongoing), vehicle, pipeline edition, and the
   edition's stages to run (pick from the server's, or add any by name for a git ref). Click an event's row to edit or delete it (not while a worker is running it). The
   Status column shows whether it has run, and how many samples it has.
2. **Launch worker…** picks the InfluxDB connection (see [Connections](#connections)), where the code comes from
   (the server's copy, or a [git ref](#building-workers-from-git)), and whether to reprocess (always on for replays).
3. The server tests the connection over the event's window first, and refuses the launch if it can't reach InfluxDB,
   find the bucket, or see any data. It also refuses while the event already has a worker. Then it starts the
   container on the `sunbeam-net` network with everything it needs in its environment: the run ID, the event, the
   InfluxDB connection, and the database and server URLs.
4. The worker drawer shows status, timing, a replay's progress, and logs, and can stop the worker.
5. The server's watchdog checks runs against their containers every few seconds, so a crashed or silent worker is
   marked `lost` or `failed` even if it never reports back.

### Rebuilding

`docker compose up --build server` picks up pipeline and stage changes: the server rebuilds the worker images from
its own copy of the code when it starts. To run code the server wasn't built with, launch from a git ref. The
dashboard bakes its code in too: `docker compose up --build -d dashboard`.

Builds work offline once Docker's cache has their layers: `apt-get` and `uv sync` only rerun when
`pipeline/Dockerfile`, `pyproject.toml` or `uv.lock` change.

### Building workers from git

Choosing **Git ref** at launch builds the worker from a branch, tag, or full commit SHA of
`https://github.com/UBC-Solar/sunbeam`. The server resolves the ref to a commit, then builds
`pipeline/Dockerfile.git` (which fetches that commit) on top of the edition's local image, so `uv sync` only installs
what the commit's lock changes. The image is `sunbeam-worker:<edition>-<sha12>`; the run shows **Building image**
until it's done (the watchdog allows 20 minutes), and later launches of the same commit reuse it.

The usual reason is trying out a new stage: add it to the event by name (**Add stage** in its dialog), then launch
from the branch that has it. The dashboard marks stages the server's copy lacks, and such an event can only be
launched from a git ref. A worker whose code lacks a stage fails before touching the event, and the run's failure
names the stage.

- Short SHAs can't be resolved remotely: give all 40 characters.
- The worker runs the event's edition, which the commit must also have.
- The server host needs to reach GitHub.

## The CLI (`sunbeam.py`)

Runs one worker without Docker: for developing stages, debugging an event, or running against a server that's
already up.

```bash
uv run sunbeam.py --event_name realtime
```

| Flag | Default | Meaning |
|---|---|---|
| `--event_name` | `$SUNBEAM_EVENT_NAME` or `realtime` | The event to run, as named in the dashboard. |
| `--serverless` | off | Run without a server (see below). |
| `--reprocess` | `$SUNBEAM_REPROCESS` or off | Delete the event's samples first. Replays always do; a realtime run otherwise appends. |
| `--influx-url`, `--influx-org`, `--influx-bucket`, `--influx-token` | `$SUNBEAM_INFLUX_*` | The InfluxDB to read. Required. |
| `--database-url` | `$SUNBEAM_DATABASE_URL` | The Postgres to write to; defaults to the compose database on `localhost:5432`. |
| `--server-url` | `$SUNBEAM_SERVER_URL` | The server to register with; defaults to `http://localhost:8000`. |

Every flag falls back to an environment variable because that's how the server configures a worker container.

### Realtime vs replay worker runs

The event's end decides:

- **Realtime**: no end, or one in the future. Ingress polls InfluxDB for the latest values (one `Ingress` stage and
  thread per signal frequency), and the pipelines run in step with the wall clock until the worker is stopped.
- **Replay**: the end has passed. The worker fetches every signal it needs over the whole event up front, trims the
  window to where every signal has data (warning if that's much shorter), then runs the pipelines over it as fast as
  it can, and completes at the end. A replay always deletes the event's samples first, since it rewrites them all,
  and also writes the raw signals it fetched.

| Run | Terminal shows | Reports to the server |
|---|---|---|
| Realtime in a terminal | A live timing table | Metrics every 1 s |
| Realtime in a container | `timing idle=...` lines every 5 s | Metrics every 5 s |
| Replay in a terminal | A progress bar: simulated time, speed, ETA, writer queue | Metrics and progress every 0.5 s |
| Replay in a container | `replay 42.1% ...` and `timing ...` lines every 5 s | Metrics and progress every 0.5 s |

### The three ways a CLI worker can run

`orchestration/bootstrap.py`'s `build_control` picks one:

1. **Serverless** (`--serverless` or `SUNBEAM_SERVERLESS=true`): no contact with a server at all.
2. **Launched by the server**: `SUNBEAM_WORKER_RUN_ID` is set, so the worker skips registering and reports to the
   server: heartbeats, stop requests, metrics, completion.
3. **Registered**: run by hand without `--serverless`. The worker asks the server for a run ID
   (`POST /workers/register`) and from then on behaves like a launched one, appearing on the dashboard. Having no
   container, it's supervised by heartbeats alone: one that ignores a stop is marked `lost`. With no server
   reachable it exits and says to start one or pass `--serverless`; it never falls back silently.

### Preflight checks

Before building a pipeline, a worker checks that it can reach Postgres, that InfluxDB is reachable with its bucket
and has data in the event's window (the same check the server runs at launch), and, unless serverless, that the
server's `/health` answers. A failure is reported to the server if possible, and the worker exits nonzero. Look here
first when a worker won't start.

## Connections

Where everything lives comes from the environment (`config/context.py`); there's no config file.

- **InfluxDB** connections are managed on the dashboard: add, edit, delete, make default, and **Test** (the server
  and bucket, or data in a chosen event). Edits apply at the next launch. A fresh database starts with
  `UBC Influx (CAN_log)` (the default) and `Production (CAN_prod)`.
- **Postgres** is shared by the server and every worker it launches: `SUNBEAM_DATABASE_URL`. Compose points it at
  the `db` service; override it in the shell or a `.env` file beside `docker-compose.yaml`.

| Variable | Read by | Default |
|---|---|---|
| `SUNBEAM_DATABASE_URL` | server, alembic, workers | `postgresql+psycopg://telemetry:telemetry@localhost:5432/telemetry` (compose: `@db`) |
| `SUNBEAM_INFLUX_URL` / `_ORG` / `_BUCKET` / `_TOKEN` | workers | none: set by the server, or by hand for a CLI run |
| `SUNBEAM_SERVER_URL` | workers | `http://localhost:8000` |
| `SUNBEAM_WORKER_SERVER_URL` | server | `http://server:8000`: what launched workers get as `SUNBEAM_SERVER_URL` |
| `SUNBEAM_WORKER_DATABASE_URL` | server | the server's own database URL, for when workers reach it by another name |
| `SUNBEAM_WORKER_NETWORK` | server | `sunbeam-net`; empty for none |

Everything is reachable only inside the team's Tailnet, which is why there's no authentication and tokens are stored
and shown in plain text.
