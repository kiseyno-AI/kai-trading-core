"""macro context observations

Revision ID: 20260620_0052
Revises: 20260516_0051
Create Date: 2026-06-20 14:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260620_0052'
down_revision = '20260516_0051'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'macro_context_observations',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('source', sa.String(length=64), nullable=False),
        sa.Column('series_id', sa.String(length=128), nullable=False),
        sa.Column('series_name', sa.String(length=255), nullable=True),
        sa.Column('country_code', sa.String(length=16), nullable=True),
        sa.Column('observation_period', sa.String(length=32), nullable=False),
        sa.Column('observation_date', sa.Date(), nullable=False),
        sa.Column('value', sa.Numeric(24, 8), nullable=False),
        sa.Column('unit', sa.String(length=64), nullable=True),
        sa.Column('frequency', sa.String(length=32), nullable=True),
        sa.Column('fetched_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('source_status', sa.String(length=64), nullable=False),
        sa.Column('status_detail', sa.Text(), nullable=True),
        sa.Column('payload', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'source',
            'series_id',
            'observation_period',
            name='uq_macro_context_observations_source_series_period',
        ),
    )
    op.create_index(
        'ix_macro_context_observations_source_series_date',
        'macro_context_observations',
        ['source', 'series_id', 'observation_date'],
        unique=False,
    )
    op.create_index(
        'ix_macro_context_observations_fetched_at',
        'macro_context_observations',
        ['fetched_at'],
        unique=False,
    )
    op.create_index(
        'ix_macro_context_observations_source_status',
        'macro_context_observations',
        ['source_status'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_macro_context_observations_source_status', table_name='macro_context_observations')
    op.drop_index('ix_macro_context_observations_fetched_at', table_name='macro_context_observations')
    op.drop_index('ix_macro_context_observations_source_series_date', table_name='macro_context_observations')
    op.drop_table('macro_context_observations')
