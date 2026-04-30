"""market snapshot cache

Revision ID: 20260430_0041
Revises: 20260429_0040
Create Date: 2026-04-30 11:40:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260430_0041'
down_revision = '20260429_0040'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'current_market_snapshot_cache'):
        return

    op.create_table(
        'current_market_snapshot_cache',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('cache_key', sa.String(length=64), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=True),
        sa.Column('snapshot_status', sa.String(length=32), nullable=True),
        sa.Column('stale_reason', sa.String(length=64), nullable=True),
        sa.Column('as_of', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_refresh_attempted_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_refresh_succeeded_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_refresh_status', sa.String(length=32), nullable=False),
        sa.Column('last_refresh_error', sa.Text(), nullable=True),
        sa.Column('refresh_trigger', sa.String(length=64), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('cache_key', name='uq_current_market_snapshot_cache_key'),
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table_exists(inspector, 'current_market_snapshot_cache'):
        op.drop_table('current_market_snapshot_cache')
