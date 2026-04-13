"""add dart fundamentals refresh runs

Revision ID: 20260414_0006
Revises: 20260408_0005
Create Date: 2026-04-14 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260414_0006'
down_revision = '20260408_0005'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'dart_fundamentals_refresh_runs'):
        return

    op.create_table(
        'dart_fundamentals_refresh_runs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('trigger', sa.String(length=64), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('fs_div', sa.String(length=8), nullable=False),
        sa.Column('force', sa.Boolean(), nullable=False),
        sa.Column('symbol_limit', sa.Integer(), nullable=True),
        sa.Column('symbol_offset', sa.Integer(), nullable=False),
        sa.Column('symbols_total', sa.Integer(), nullable=False),
        sa.Column('symbols_checked', sa.Integer(), nullable=False),
        sa.Column('symbols_missing', sa.Integer(), nullable=False),
        sa.Column('symbols_stale', sa.Integer(), nullable=False),
        sa.Column('symbols_fresh', sa.Integer(), nullable=False),
        sa.Column('symbols_refreshed', sa.Integer(), nullable=False),
        sa.Column('symbols_failed', sa.Integer(), nullable=False),
        sa.Column('events_detected', sa.Integer(), nullable=False),
        sa.Column('events_captured', sa.Integer(), nullable=False),
        sa.Column('events_failed', sa.Integer(), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('run_metadata', sa.JSON(), nullable=True),
        sa.Column('started_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        op.f('ix_dart_fundamentals_refresh_runs_market_date'),
        'dart_fundamentals_refresh_runs',
        ['market_date'],
        unique=False,
    )
    op.create_index(
        op.f('ix_dart_fundamentals_refresh_runs_status'),
        'dart_fundamentals_refresh_runs',
        ['status'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f('ix_dart_fundamentals_refresh_runs_status'), table_name='dart_fundamentals_refresh_runs')
    op.drop_index(op.f('ix_dart_fundamentals_refresh_runs_market_date'), table_name='dart_fundamentals_refresh_runs')
    op.drop_table('dart_fundamentals_refresh_runs')
