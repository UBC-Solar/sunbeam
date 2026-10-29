"""drop raw_sample; rename aligned_sample to sample

Nothing ever wrote raw_sample, and with it gone aligned_sample is the only
sample table, so it becomes plain `sample`. Its constraints and indexes
(including the ones Postgres and TimescaleDB named after the table) are
renamed to match.

Revision ID: d4b8e1f06a92
Revises: c7e2a91f5d3b
Create Date: 2026-10-29 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd4b8e1f06a92'
down_revision: Union[str, Sequence[str], None] = 'c7e2a91f5d3b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _rename_table(old: str, new: str) -> None:
    """
    Rename a table, its primary key, and every constraint and index whose name starts with the old table name:
    the foreign keys Postgres named, and the ts index create_hypertable made, as well as our own query indexes.
    Renaming a hypertable is a plain ALTER TABLE; its chunks follow.
    """
    op.execute(f"ALTER TABLE {old} RENAME TO {new}")
    # Ours, named pk_<table>; renaming it renames its index too
    op.execute(f"ALTER TABLE {new} RENAME CONSTRAINT pk_{old} TO pk_{new}")
    op.execute(f"""
        DO $$
        DECLARE item record;
        BEGIN
            FOR item IN
                SELECT conname AS name FROM pg_constraint
                WHERE conrelid = '{new}'::regclass AND conname LIKE '{old}%'
            LOOP
                EXECUTE format('ALTER TABLE {new} RENAME CONSTRAINT %I TO %I',
                               item.name, '{new}' || substr(item.name, length('{old}') + 1));
            END LOOP;
            -- Constraint renames carry their backing indexes along; this catches the rest
            FOR item IN
                SELECT indexname AS name FROM pg_indexes
                WHERE schemaname = current_schema() AND tablename = '{new}' AND indexname LIKE '{old}%'
            LOOP
                EXECUTE format('ALTER INDEX %I RENAME TO %I',
                               item.name, '{new}' || substr(item.name, length('{old}') + 1));
            END LOOP;
        END $$;
    """)


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_table('raw_sample')
    _rename_table('aligned_sample', 'sample')


def downgrade() -> None:
    """Downgrade schema."""
    _rename_table('sample', 'aligned_sample')

    # As the baseline created it (empty: nothing ever wrote to it)
    op.create_table(
        'raw_sample',
        sa.Column('event_id', sa.BigInteger(), nullable=False),
        sa.Column('ts', sa.DateTime(timezone=True), nullable=False),
        sa.Column('signal_id', sa.BigInteger(), nullable=False),
        sa.Column('value_f64', sa.Float(), nullable=False),
        sa.Column('ingest_ts', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('source_message_type', sa.String(length=255), nullable=True),
        sa.Column('source_sequence', sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(['event_id'], ['event.id'], ),
        sa.ForeignKeyConstraint(['signal_id'], ['signal.id'], ),
        sa.PrimaryKeyConstraint('event_id', 'signal_id', 'ts', name='pk_raw_sample'),
    )
    op.execute(
        """
        SELECT create_hypertable(
            'raw_sample',
            'ts',
            chunk_time_interval => INTERVAL '1 day',
            if_not_exists => TRUE
        );
        """
    )
