"""add daily trending ranks

Revision ID: 20260414_0007
Revises: 20260414_0006
Create Date: 2026-04-14 17:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260414_0007'
down_revision = '20260414_0006'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'daily_trending_ranks'):
        return

    op.create_table(
        'daily_trending_ranks',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('eod_run_id', sa.Integer(), nullable=True),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('period', sa.String(length=8), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('metric_name', sa.String(length=64), nullable=False),
        sa.Column('metric_value', sa.Numeric(precision=14, scale=4), nullable=False),
        sa.Column('trend_direction', sa.String(length=16), nullable=False),
        sa.Column('absolute_rank', sa.Integer(), nullable=False),
        sa.Column('up_rank', sa.Integer(), nullable=True),
        sa.Column('down_rank', sa.Integer(), nullable=True),
        sa.Column('price_payload', sa.JSON(), nullable=True),
        sa.Column('technical_payload', sa.JSON(), nullable=True),
        sa.Column('action_summary_payload', sa.JSON(), nullable=True),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['eod_run_id'], ['eod_collection_runs.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'symbol_id',
            'market_date',
            'period',
            name='uq_daily_trending_ranks_symbol_market_date_period',
        ),
    )
    op.create_index(op.f('ix_daily_trending_ranks_symbol_id'), 'daily_trending_ranks', ['symbol_id'], unique=False)
    op.create_index(op.f('ix_daily_trending_ranks_eod_run_id'), 'daily_trending_ranks', ['eod_run_id'], unique=False)
    op.create_index(op.f('ix_daily_trending_ranks_market_date'), 'daily_trending_ranks', ['market_date'], unique=False)
    op.create_index(op.f('ix_daily_trending_ranks_period'), 'daily_trending_ranks', ['period'], unique=False)
    op.create_index(
        op.f('ix_daily_trending_ranks_trend_direction'),
        'daily_trending_ranks',
        ['trend_direction'],
        unique=False,
    )
    op.create_index(
        op.f('ix_daily_trending_ranks_absolute_rank'),
        'daily_trending_ranks',
        ['absolute_rank'],
        unique=False,
    )
    op.create_index(op.f('ix_daily_trending_ranks_up_rank'), 'daily_trending_ranks', ['up_rank'], unique=False)
    op.create_index(op.f('ix_daily_trending_ranks_down_rank'), 'daily_trending_ranks', ['down_rank'], unique=False)
    op.create_index(
        'ix_daily_trending_ranks_market_period_absolute',
        'daily_trending_ranks',
        ['market_date', 'period', 'absolute_rank'],
        unique=False,
    )
    op.create_index(
        'ix_daily_trending_ranks_market_period_up',
        'daily_trending_ranks',
        ['market_date', 'period', 'up_rank'],
        unique=False,
    )
    op.create_index(
        'ix_daily_trending_ranks_market_period_down',
        'daily_trending_ranks',
        ['market_date', 'period', 'down_rank'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_daily_trending_ranks_market_period_down', table_name='daily_trending_ranks')
    op.drop_index('ix_daily_trending_ranks_market_period_up', table_name='daily_trending_ranks')
    op.drop_index('ix_daily_trending_ranks_market_period_absolute', table_name='daily_trending_ranks')
    op.drop_index(op.f('ix_daily_trending_ranks_down_rank'), table_name='daily_trending_ranks')
    op.drop_index(op.f('ix_daily_trending_ranks_up_rank'), table_name='daily_trending_ranks')
    op.drop_index(op.f('ix_daily_trending_ranks_absolute_rank'), table_name='daily_trending_ranks')
    op.drop_index(op.f('ix_daily_trending_ranks_trend_direction'), table_name='daily_trending_ranks')
    op.drop_index(op.f('ix_daily_trending_ranks_period'), table_name='daily_trending_ranks')
    op.drop_index(op.f('ix_daily_trending_ranks_market_date'), table_name='daily_trending_ranks')
    op.drop_index(op.f('ix_daily_trending_ranks_eod_run_id'), table_name='daily_trending_ranks')
    op.drop_index(op.f('ix_daily_trending_ranks_symbol_id'), table_name='daily_trending_ranks')
    op.drop_table('daily_trending_ranks')
