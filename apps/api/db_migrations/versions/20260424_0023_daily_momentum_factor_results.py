"""add daily momentum factor results

Revision ID: 20260424_0023
Revises: 20260424_0022
Create Date: 2026-04-24 17:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260424_0023'
down_revision = '20260424_0022'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_momentum_factor_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('close_price', sa.BigInteger(), nullable=False),
        sa.Column('ma_60', sa.Numeric(18, 6), nullable=True),
        sa.Column('ma_120', sa.Numeric(18, 6), nullable=True),
        sa.Column('momentum_12_1', sa.Numeric(18, 6), nullable=True),
        sa.Column('return_5d', sa.Numeric(18, 6), nullable=True),
        sa.Column('has_sufficient_history', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_price_above_ma120', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_ma60_above_ma120', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_recent_overheated', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('momentum_rank', sa.Integer(), nullable=True),
        sa.Column('momentum_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('is_momentum_eligible', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('factor_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_momentum_factor_results_symbol_market_date'),
    )
    op.create_index('ix_daily_momentum_factor_results_market_date', 'daily_momentum_factor_results', ['market_date'])
    op.create_index('ix_daily_momentum_factor_results_symbol_id', 'daily_momentum_factor_results', ['symbol_id'])
    op.create_index(
        'ix_daily_momentum_factor_results_market_rank',
        'daily_momentum_factor_results',
        ['market_date', 'momentum_rank'],
    )
    op.create_index(
        'ix_daily_momentum_factor_results_market_percentile',
        'daily_momentum_factor_results',
        ['market_date', 'momentum_percentile'],
    )
    op.create_index(
        'ix_daily_momentum_factor_results_market_eligible',
        'daily_momentum_factor_results',
        ['market_date', 'is_momentum_eligible'],
    )


def downgrade() -> None:
    op.drop_index('ix_daily_momentum_factor_results_market_eligible', table_name='daily_momentum_factor_results')
    op.drop_index('ix_daily_momentum_factor_results_market_percentile', table_name='daily_momentum_factor_results')
    op.drop_index('ix_daily_momentum_factor_results_market_rank', table_name='daily_momentum_factor_results')
    op.drop_index('ix_daily_momentum_factor_results_symbol_id', table_name='daily_momentum_factor_results')
    op.drop_index('ix_daily_momentum_factor_results_market_date', table_name='daily_momentum_factor_results')
    op.drop_table('daily_momentum_factor_results')
