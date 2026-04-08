"""add daily technical snapshots

Revision ID: 20260408_0004
Revises: 20260407_0003
Create Date: 2026-04-08 11:45:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260408_0004'
down_revision = '20260407_0003'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'daily_technical_snapshots'):
        return

    op.create_table(
        'daily_technical_snapshots',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('eod_run_id', sa.Integer(), nullable=True),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('close_price', sa.BigInteger(), nullable=False),
        sa.Column('volume', sa.BigInteger(), nullable=False),
        sa.Column('trend_state', sa.String(length=32), nullable=False),
        sa.Column('ma_alignment', sa.String(length=32), nullable=False),
        sa.Column('breakout_20d', sa.Boolean(), nullable=False),
        sa.Column('breakdown_20d', sa.Boolean(), nullable=False),
        sa.Column('return_5d', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('return_20d', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('volume_ratio_20d', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('atr_14', sa.Numeric(precision=14, scale=4), nullable=True),
        sa.Column('atr_pct_14', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('signal_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['eod_run_id'], ['eod_collection_runs.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_technical_snapshots_symbol_market_date'),
    )
    op.create_index(
        op.f('ix_daily_technical_snapshots_symbol_id'),
        'daily_technical_snapshots',
        ['symbol_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_daily_technical_snapshots_eod_run_id'),
        'daily_technical_snapshots',
        ['eod_run_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_daily_technical_snapshots_market_date'),
        'daily_technical_snapshots',
        ['market_date'],
        unique=False,
    )
    op.create_index(
        op.f('ix_daily_technical_snapshots_trend_state'),
        'daily_technical_snapshots',
        ['trend_state'],
        unique=False,
    )
    op.create_index(
        op.f('ix_daily_technical_snapshots_ma_alignment'),
        'daily_technical_snapshots',
        ['ma_alignment'],
        unique=False,
    )
    op.create_index(
        op.f('ix_daily_technical_snapshots_breakout_20d'),
        'daily_technical_snapshots',
        ['breakout_20d'],
        unique=False,
    )
    op.create_index(
        op.f('ix_daily_technical_snapshots_breakdown_20d'),
        'daily_technical_snapshots',
        ['breakdown_20d'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f('ix_daily_technical_snapshots_breakdown_20d'), table_name='daily_technical_snapshots')
    op.drop_index(op.f('ix_daily_technical_snapshots_breakout_20d'), table_name='daily_technical_snapshots')
    op.drop_index(op.f('ix_daily_technical_snapshots_ma_alignment'), table_name='daily_technical_snapshots')
    op.drop_index(op.f('ix_daily_technical_snapshots_trend_state'), table_name='daily_technical_snapshots')
    op.drop_index(op.f('ix_daily_technical_snapshots_market_date'), table_name='daily_technical_snapshots')
    op.drop_index(op.f('ix_daily_technical_snapshots_eod_run_id'), table_name='daily_technical_snapshots')
    op.drop_index(op.f('ix_daily_technical_snapshots_symbol_id'), table_name='daily_technical_snapshots')
    op.drop_table('daily_technical_snapshots')
