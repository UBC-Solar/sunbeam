# Alembic Quick Guide

Sunbeam uses [Alembic](https://alembic.sqlalchemy.org/) to version the
Postgres/TimescaleDB schema. This is a practical how-to for this repo
specifically — the mental model, the day-to-day workflow, and the two
non-obvious things this codebase does with it (a pre-Alembic-database
transition shim, and a lock timeout so a blocked migration fails loudly
instead of hanging server startup).

## Introduction

The schema's history is a chain of migration files in
`alembic/versions/`. Each file has a random ID, an `upgrade()` and a
`downgrade()`, and a pointer to its parent (`down_revision`). Every
database carries a one-row bookkeeping table, `alembic_version`, recording
which link of the chain it's currently at. `alembic upgrade head` means
"walk the chain from wherever this database is to the newest revision,
executing each `upgrade()` along the way."

Right now the chain has two links:

1. `f3f709edb0e7` — baseline: all six tables
   (`vehicle`, `event`, `signal`, `worker_run`, `raw_sample`,
   `aligned_sample`), the TimescaleDB extension, both hypertables, and the
   query indexes — a frozen snapshot of what the old `create_schema()`
   function used to build by hand.
2. `6044f7bdb7d9` — adds `worker_run.kind` (`WorkerKind`, for
   self-registered workers — see [`USAGE.md`](USAGE.md)) and makes
   `worker_run.image_tag` nullable.

Run `uv run alembic history --verbose` to see the current chain.

## Connecting

`alembic/env.py` resolves the database URL in this order:

1. `config.attributes["sunbeam_database_url"]` — set programmatically by
   `server/migrations.py` when the server runs migrations on its own
   startup (see below); you won't set this by hand.
2. the `SUNBEAM_DATABASE_URL` environment variable.
3. `context.toml`'s `[client].sunbeamdb` (or `[worker]`, depending on
   `ServiceType` — see [`USAGE.md`](USAGE.md#configuration-profiles-debug-vs-production)).

So against your normal local `docker compose` Postgres, plain
`uv run alembic <command>` just works via `context.toml`. Point at a
different database explicitly with:

```bash
export SUNBEAM_DATABASE_URL="postgresql+psycopg://user:pass@host:5432/dbname"
uv run alembic upgrade head
```

## Automatic Migrations

`server/main.py`'s `on_startup` calls `server/migrations.py`'s
`upgrade_database(engine)` — the server always brings its database to
`head` before serving requests. You do not need to manually run
`alembic upgrade head` against a database the server manages; it happens
every time the server (re)starts, and is a no-op when already at `head`.

### Alembic Transition

If you have a database from before this repo used Alembic (i.e. one built
by the old `create_schema()`), it has the full baseline schema but no
`alembic_version` table — Alembic doesn't know it's already there.
`upgrade_database` detects this (schema present, `alembic_version` absent)
and **stamps** the database at the baseline revision — recording "you're
already here" **without re-running any DDL** — before running `upgrade
head` normally. You'll see this once per such database, in the server log:

```
Pre-Alembic schema detected; stamping baseline revision f3f709edb0e7.
```

After that, the database has its `alembic_version` row like any other, and
the shim never fires again for it. If you ever need to do this by hand
against a database the server doesn't manage:

```bash
uv run alembic stamp f3f709edb0e7
uv run alembic upgrade head
```

### Migration Failures

Every migration connection sets Postgres's `lock_timeout` to 10 seconds
before running (`alembic/env.py`, overridable via
`upgrade_database(engine, lock_timeout=...)`). `lock_timeout` only bounds
*waiting for a lock*, not statement execution — a legitimately long data
migration runs to completion normally. But if some other session (most
often an "idle in transaction" connection) is holding a lock on a table
Alembic needs to `ALTER`, the migration — and therefore server startup —
fails after 10 seconds with a message telling you to inspect
`pg_blocking_pids(pid)` and terminate the offending session, rather than
hanging indefinitely.

## Changing the Schema

Say you add a column to `Event` in `db/sunbeamdb/models.py`:

```bash
# 1. Make sure your target DB is at head (autogenerate diffs models vs. the live DB)
uv run alembic upgrade head

# 2. Generate a draft migration from the diff
uv run alembic revision --autogenerate -m "add lap_number to event"

# 3. READ the generated file in alembic/versions/  <- not optional, see below

# 4. Apply it
uv run alembic upgrade head
```

### Step 3 is not optional

Autogenerate is a heuristic diff, not a compiler, and it has specific,
recurring blind spots — this repo's own migration #2 hit two of them on
its first draft:

- **Renames** look like *drop column + add column* to autogenerate, which
  destroys data. Hand-edit to `op.alter_column(..., new_column_name=...)`.
- **Postgres enum types**: `ADD COLUMN` of an `Enum` type does not
  implicitly create the enum type on Postgres — you need an explicit
  `sa.Enum(...).create(op.get_bind(), checkfirst=True)` call (see
  migration `6044f7bdb7d9` for the pattern), and the matching `.drop(...)`
  in `downgrade()`. Adding a *value* to an existing enum isn't detected by
  autogenerate at all — hand-write
  `op.execute("ALTER TYPE workerstatus ADD VALUE 'paused'")`.
- **Anything TimescaleDB-specific** — new hypertables, chunk intervals,
  compression policies, or (as happened on migration #2) autogenerate
  proposing to **drop** the baseline's hand-written hypertable indexes
  because they don't exist in the SQLAlchemy model metadata. Check every
  generated `op.drop_index(...)` against the baseline migration before
  trusting it.
- Server defaults and some constraint changes are hit-or-miss; skim the
  whole diff, not just the parts you expected.

Simple column/table/index additions that don't touch any of the above,
autogenerate gets right essentially every time.

## Commands Quick Guide

```bash
uv run alembic current            # what revision is this DB at?
uv run alembic history --verbose  # the full chain, newest first
uv run alembic upgrade head       # apply everything pending
uv run alembic downgrade -1       # step back one migration
uv run alembic upgrade --sql head # print the SQL, execute nothing - review before prod
uv run alembic heads              # should print exactly ONE id (see below)
```

## Data Migrations

Migrations can move data, not just DDL:

```python
def upgrade():
    op.add_column("event", sa.Column("lap_number", sa.Integer(), nullable=True))
    op.execute("UPDATE event SET lap_number = 0 WHERE lap_number IS NULL")
```

Keep these idempotent where practical. Anything touching `raw_sample` or
`aligned_sample` (the hypertables — potentially millions of rows) deserves
extra care: prefer batched raw SQL, and think about how long the migration
holds locks given the `lock_timeout` behavior above.

## Merging

Migration files are code: committed, reviewed alongside the model change
that motivated them, and never edited after being applied anywhere that
matters — if a migration turns out wrong, write a new one to fix it rather
than rewriting history.

If two branches each add a migration off the same parent revision, merging
both gives you **two heads**, and `upgrade head` refuses to run
ambiguously. `uv run alembic heads` printing more than one ID is the tell;
fix it with:

```bash
uv run alembic merge heads -m "merge"
```

which creates an empty migration with two parents, restoring a single
linear (well, DAG-with-one-sink) chain.

## Testing

`tests/postgres/test_migrations.py` (Postgres-marked — see the
[README](../README.md#running-the-tests)) keeps the chain honest on every
CI run: a fresh database upgraded to `head` must produce the full schema
including hypertables, `downgrade base` must remove it cleanly, and
`upgrade_database`'s pre-Alembic stamping shim and lock-timeout behavior
each have a dedicated test (the latter by having a second connection hold
an `ACCESS EXCLUSIVE` lock and asserting the migration fails within the
timeout rather than hanging). If you add a migration with any of the sharp
edges above, add a matching assertion here.
