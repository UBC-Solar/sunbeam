"""event.stages: events are created in the dashboard, not events.toml

Events used to be defined in config/events.toml, and the worker read its stage
list from there. Now the database is the only place an event is defined, so it
carries its stages too. The two events that existed in events.toml get theirs.

Revision ID: e5a2c8f17b30
Revises: d4b8e1f06a92
Create Date: 2026-11-05 16:00:00.000000

"""
import json
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5a2c8f17b30'
down_revision: Union[str, Sequence[str], None] = 'd4b8e1f06a92'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# What config/events.toml said when it was removed
STAGES_FROM_EVENTS_TOML = {
    "FSGP_2024_Day_1": ["MotorPower", "Efficiency"],
    "realtime": ["PackPower"],
}


def upgrade() -> None:
    op.add_column("event", sa.Column("stages", sa.JSON(), nullable=False, server_default="[]"))

    for name, stages in STAGES_FROM_EVENTS_TOML.items():
        op.execute(
            sa.text("UPDATE event SET stages = CAST(:stages AS json) WHERE name = :name")
            .bindparams(stages=json.dumps(stages), name=name)
        )


def downgrade() -> None:
    op.drop_column("event", "stages")
