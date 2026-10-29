"""influx connections and worker sources

Replaces context.toml's debug/production profiles with a connection table the
dashboard edits, records which connection (and which git ref) each worker run
used, and adds the BUILDING worker status for runs built from a git ref.

Revision ID: c7e2a91f5d3b
Revises: a3c1e7d2b9f4
Create Date: 2026-10-28 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c7e2a91f5d3b'
down_revision: Union[str, Sequence[str], None] = 'a3c1e7d2b9f4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


connectionkind = sa.Enum('INFLUXDB', name='connectionkind')

# The InfluxDB blocks of the context.toml this replaces, so existing deployments keep working as they did. Its
# old debug profile was the default. (Both hosts are only reachable over the team's Tailscale network.)
SEED_CONNECTIONS = [
    {
        "name": "UBC Influx (CAN_log)",
        "url": "http://influxdb.telemetry.ubcsolar.com",
        "organization": "8a0b66d77a331e96",
        "bucket": "CAN_log",
        "token": "s4Z9_S6_O09kDzYn1KZcs7LVoCA2cVK9_ObY44vR4xMh-wYLSWBkypS0S0ZHQgBvEV2A5LgvQ1IKr8byHes2LA==",
        "is_default": True,
        "profile": "debug",
    },
    {
        "name": "Production (CAN_prod)",
        "url": "http://100.104.117.40:8086",
        "organization": "43a959219b6b3ba0",
        "bucket": "CAN_prod",
        "token": "D3tQw8g8vcgUoOZLlKKdmfT-LWfgsIF2rj5nf0kOdinRpNXotAftMTQCKR24o0yYjfk_K2eLhgYGWu_DsxY1eA==",
        "is_default": False,
        "profile": "production",
    },
]


def upgrade() -> None:
    """Upgrade schema."""
    connection = op.create_table(
        'connection',
        sa.Column('id', sa.BigInteger().with_variant(sa.Integer(), 'sqlite'), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('kind', connectionkind, nullable=False),
        sa.Column('url', sa.String(length=1024), nullable=False),
        sa.Column('organization', sa.String(length=255), nullable=False),
        sa.Column('bucket', sa.String(length=255), nullable=False),
        sa.Column('token', sa.Text(), nullable=False),
        sa.Column('is_default', sa.Boolean(), server_default='false', nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('last_tested_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_test_ok', sa.Boolean(), nullable=True),
        sa.Column('last_test_detail', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )

    op.bulk_insert(connection, [
        {key: value for key, value in seed.items() if key != "profile"} | {"kind": "INFLUXDB"}
        for seed in SEED_CONNECTIONS
    ])

    op.add_column('worker_run', sa.Column('influx_connection_id', sa.BigInteger(), nullable=True))
    op.add_column('worker_run', sa.Column('influx_connection_name', sa.String(length=1024), nullable=True))
    op.add_column('worker_run', sa.Column('source_ref', sa.String(length=255), nullable=True))
    op.add_column('worker_run', sa.Column('source_sha', sa.String(length=40), nullable=True))
    op.create_foreign_key(
        'fk_worker_run_influx_connection', 'worker_run', 'connection',
        ['influx_connection_id'], ['id'], ondelete='SET NULL',
    )

    # Earlier runs recorded a profile name; point them at the connection that profile became
    for seed in SEED_CONNECTIONS:
        op.execute(
            sa.text(
                "UPDATE worker_run SET influx_connection_name = :name, "
                "influx_connection_id = (SELECT id FROM connection WHERE name = :name) "
                "WHERE configuration_profile = :profile"
            ).bindparams(name=seed["name"], profile=seed["profile"])
        )
    op.execute("UPDATE worker_run SET influx_connection_name = configuration_profile "
               "WHERE influx_connection_name IS NULL AND configuration_profile IS NOT NULL")
    op.drop_column('worker_run', 'configuration_profile')

    # The value can't be used until this transaction commits, which nothing here needs to
    op.execute("ALTER TYPE workerstatus ADD VALUE IF NOT EXISTS 'BUILDING' AFTER 'REQUESTED'")


def downgrade() -> None:
    """Downgrade schema."""
    # Postgres can't drop an enum value; leave BUILDING in the type but make sure no row uses it
    op.execute("UPDATE worker_run SET status = 'FAILED' WHERE status = 'BUILDING'")

    op.add_column('worker_run', sa.Column('configuration_profile', sa.String(length=64), nullable=True))
    for seed in SEED_CONNECTIONS:
        op.execute(
            sa.text(
                "UPDATE worker_run SET configuration_profile = :profile WHERE influx_connection_name = :name"
            ).bindparams(name=seed["name"], profile=seed["profile"])
        )

    op.drop_constraint('fk_worker_run_influx_connection', 'worker_run', type_='foreignkey')
    op.drop_column('worker_run', 'source_sha')
    op.drop_column('worker_run', 'source_ref')
    op.drop_column('worker_run', 'influx_connection_name')
    op.drop_column('worker_run', 'influx_connection_id')

    op.drop_table('connection')
    connectionkind.drop(op.get_bind(), checkfirst=True)
