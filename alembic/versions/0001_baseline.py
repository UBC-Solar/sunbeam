"""baseline: the 2.0.0 schema

Revision ID: 0001
Revises:
Create Date: 2026-11-05

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

BigIntegerPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

eventstatus = sa.Enum("UNPROCESSED", "ONGOING", "PROCESSED", name="eventstatus")
workerstatus = sa.Enum(
    "REQUESTED", "BUILDING", "STARTING", "RUNNING", "STOP_REQUESTED", "STOPPING",
    "COMPLETED", "FAILED", "LOST", "CANCELLED",
    name="workerstatus",
)
workerkind = sa.Enum("CONTAINER", "EXTERNAL", name="workerkind")
connectionkind = sa.Enum("INFLUXDB", name="connectionkind")

# So a fresh install can launch a worker straight away. Both hosts are only reachable over the team's Tailnet.
DEFAULT_CONNECTIONS = [
    {
        "name": "UBC Influx (CAN_log)",
        "kind": "INFLUXDB",
        "url": "http://influxdb.telemetry.ubcsolar.com",
        "organization": "8a0b66d77a331e96",
        "bucket": "CAN_log",
        "token": "s4Z9_S6_O09kDzYn1KZcs7LVoCA2cVK9_ObY44vR4xMh-wYLSWBkypS0S0ZHQgBvEV2A5LgvQ1IKr8byHes2LA==",
        "is_default": True,
    },
    {
        "name": "Production (CAN_prod)",
        "kind": "INFLUXDB",
        "url": "http://100.104.117.40:8086",
        "organization": "43a959219b6b3ba0",
        "bucket": "CAN_prod",
        "token": "D3tQw8g8vcgUoOZLlKKdmfT-LWfgsIF2rj5nf0kOdinRpNXotAftMTQCKR24o0yYjfk_K2eLhgYGWu_DsxY1eA==",
        "is_default": False,
    },
]


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    op.create_table(
        "vehicle",
        sa.Column("id", BigIntegerPK, autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "event",
        sa.Column("id", BigIntegerPK, autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("vehicle_id", BigIntegerPK, nullable=False),
        sa.Column("starts_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ends_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pipeline_edition", sa.String(length=32), nullable=False),
        sa.Column("stages", sa.JSON(), nullable=False),
        sa.Column("status", eventstatus, nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["vehicle_id"], ["vehicle.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "signal",
        sa.Column("id", BigIntegerPK, autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("unit", sa.String(length=32), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("frequency", sa.Float(), nullable=True),
        sa.Column("event_id", BigIntegerPK, nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["event_id"], ["event.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id", "name", name="uq_signal_event_name"),
    )
    connection = op.create_table(
        "connection",
        sa.Column("id", BigIntegerPK, autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("kind", connectionkind, nullable=False),
        sa.Column("url", sa.String(length=1024), nullable=False),
        sa.Column("organization", sa.String(length=255), nullable=False),
        sa.Column("bucket", sa.String(length=255), nullable=False),
        sa.Column("token", sa.Text(), nullable=False),
        sa.Column("is_default", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_tested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_test_ok", sa.Boolean(), nullable=True),
        sa.Column("last_test_detail", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "worker_run",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_id", BigIntegerPK, nullable=False),
        sa.Column("pipeline_edition", sa.String(length=32), nullable=False),
        sa.Column("image_tag", sa.String(length=255), nullable=True),
        sa.Column("status", workerstatus, nullable=False),
        sa.Column("kind", workerkind, server_default="CONTAINER", nullable=False),
        sa.Column("host", sa.String(length=255), nullable=True),
        sa.Column("container_id", sa.String(length=255), nullable=True),
        sa.Column("container_name", sa.String(length=255), nullable=True),
        sa.Column("current_stage", sa.String(length=255), nullable=True),
        sa.Column("status_message", sa.Text(), nullable=True),
        sa.Column("stop_requested", sa.Boolean(), nullable=False),
        sa.Column("influx_connection_id", BigIntegerPK, nullable=True),
        sa.Column("influx_connection_name", sa.String(length=1024), nullable=True),
        sa.Column("source_ref", sa.String(length=255), nullable=True),
        sa.Column("source_sha", sa.String(length=40), nullable=True),
        sa.Column("reprocess", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("stop_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["event_id"], ["event.id"]),
        sa.ForeignKeyConstraint(["influx_connection_id"], ["connection.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "sample",
        sa.Column("event_id", BigIntegerPK, nullable=False),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("signal_id", BigIntegerPK, nullable=False),
        sa.Column("value_f64", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["event_id"], ["event.id"]),
        sa.ForeignKeyConstraint(["signal_id"], ["signal.id"]),
        sa.PrimaryKeyConstraint("event_id", "signal_id", "ts", name="pk_sample"),
    )

    # TimescaleDB adds its own sample_ts_idx; these serve the per-signal and per-event range queries. They aren't in
    # the models, and alembic/env.py keeps autogenerate from proposing to drop them.
    op.execute("SELECT create_hypertable('sample', 'ts', chunk_time_interval => INTERVAL '1 day')")
    op.execute("CREATE INDEX sample_event_signal_ts_idx ON sample (event_id, signal_id, ts DESC)")
    op.execute("CREATE INDEX sample_event_ts_idx ON sample (event_id, ts DESC)")

    op.bulk_insert(connection, DEFAULT_CONNECTIONS)


def downgrade() -> None:
    for table in ("sample", "worker_run", "connection", "signal", "event", "vehicle"):
        op.drop_table(table)
    for enum in (connectionkind, workerkind, workerstatus, eventstatus):
        enum.drop(op.get_bind())
