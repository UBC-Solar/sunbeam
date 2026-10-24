"""worker_run configuration profile and reprocess

Revision ID: a3c1e7d2b9f4
Revises: 6044f7bdb7d9
Create Date: 2026-10-24 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a3c1e7d2b9f4'
down_revision: Union[str, Sequence[str], None] = '6044f7bdb7d9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'worker_run',
        sa.Column('configuration_profile', sa.String(length=64), nullable=True),
    )
    op.add_column(
        'worker_run',
        sa.Column('reprocess', sa.Boolean(), server_default='false', nullable=False),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('worker_run', 'reprocess')
    op.drop_column('worker_run', 'configuration_profile')
