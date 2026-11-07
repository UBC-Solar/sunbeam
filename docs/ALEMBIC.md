# Migrations

[Alembic](https://alembic.sqlalchemy.org/) versions the Postgres/TimescaleDB schema. Migrations live in
`alembic/versions/`, starting from `0001_baseline.py`, the 2.0.0 schema. Each database records the revision it's at
in its `alembic_version` table.

## The server migrates itself

On startup the server runs `alembic upgrade head` against its database (`server/migrations.py`), so a database the
server manages never needs migrating by hand. Commit the migration with the model change and restart the server.

Every migration waits at most 10 s for a lock. If another session holds one (usually a connection left idle in a
transaction), startup fails with a message to find it with `pg_blocking_pids(pid)` and end it, rather than hanging.

## Changing the schema

```bash
# Change db/sunbeamdb/models.py, then, with the database at head:
uv run alembic revision --autogenerate -m "add lap_number to event"
# Read the generated file before trusting it (see below), then:
uv run alembic upgrade head
```

`uv run alembic <command>` uses `SUNBEAM_DATABASE_URL`, which defaults to the `docker compose` database on
`localhost:5432`.

Autogenerate diffs the models against the database, and gets simple additions right. Check its draft for:

- **Renames**, which it sees as a drop and an add, losing the data. Use `op.alter_column(..., new_column_name=...)`.
- **New Postgres enum values**, which it doesn't notice. Write `op.execute("ALTER TYPE workerstatus ADD VALUE 'x'")`.
  A new enum *column* needs its type created first: `sa.Enum(..., name=...).create(op.get_bind(), checkfirst=True)`.
- **TimescaleDB**: hypertables, chunk intervals and the hand-made `sample` indexes aren't in the models.
  `alembic/env.py` keeps autogenerate from proposing to drop indexes the models don't declare.
- **Anything touching `sample`**, which has millions of rows: think about how long it holds locks.

Don't edit a migration once it has run anywhere that matters; write another. If two branches each add one,
`alembic heads` prints two revisions and `upgrade` refuses to run: `uv run alembic merge heads -m "merge"`.

## Useful commands

```bash
uv run alembic current            # this database's revision
uv run alembic history            # the chain
uv run alembic upgrade --sql head # print the SQL without running it
uv run alembic downgrade -1
uv run alembic check              # fails if the models and migrations disagree
```

## Tests

`tests/postgres/test_migrations.py` upgrades a fresh TimescaleDB to head, checks it matches the models
(`alembic check`) and has the hypertable, downgrades it to nothing, and checks that a blocked migration fails rather
than hangs.
