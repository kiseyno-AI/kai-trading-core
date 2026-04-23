"""add daily low risk factor results

Revision ID: 20260424_0022
Revises: 20260422_0021
Create Date: 2026-04-24 11:40:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260424_0022'
down_revision = '20260422_0021'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_low_risk_factor_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('volatility_60d', sa.Numeric(12, 6), nullable=True),
        sa.Column('volatility_120d', sa.Numeric(12, 6), nullable=True),
        sa.Column('max_drawdown_120d', sa.Numeric(12, 6), nullable=True),
        sa.Column('avg_trading_value_20d', sa.Numeric(24, 4), nullable=True),
        sa.Column('volatility_60d_rank', sa.Integer(), nullable=True),
        sa.Column('volatility_120d_rank', sa.Integer(), nullable=True),
        sa.Column('max_drawdown_120d_rank', sa.Integer(), nullable=True),
        sa.Column('liquidity_rank', sa.Integer(), nullable=True),
        sa.Column('volatility_60d_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('volatility_120d_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('max_drawdown_120d_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('liquidity_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('passes_liquidity_floor', sa.Boolean(), nullable=True),
        sa.Column('low_risk_score', sa.Numeric(12, 6), nullable=True),
        sa.Column(
            'low_risk_classification',
            sa.String(length=32),
            nullable=False,
            server_default='insufficient_history',
        ),
        sa.Column('is_low_risk_eligible', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('history_days_available', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('factor_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_low_risk_factor_results_symbol_market_date'),
    )
    op.create_index('ix_daily_low_risk_factor_results_market_date', 'daily_low_risk_factor_results', ['market_date'])
    op.create_index('ix_daily_low_risk_factor_results_symbol_id', 'daily_low_risk_factor_results', ['symbol_id'])
    op.create_index(
        'ix_daily_low_risk_factor_results_market_eligible',
        'daily_low_risk_factor_results',
        ['market_date', 'is_low_risk_eligible'],
    )
    op.create_index(
        'ix_daily_low_risk_factor_results_market_score',
        'daily_low_risk_factor_results',
        ['market_date', 'low_risk_score'],
    )
    op.create_index(
        'ix_daily_low_risk_factor_results_low_risk_classification',
        'daily_low_risk_factor_results',
        ['low_risk_classification'],
    )
    op.create_index(
        'ix_daily_low_risk_factor_results_is_low_risk_eligible',
        'daily_low_risk_factor_results',
        ['is_low_risk_eligible'],
    )
    op.create_index(
        'ix_daily_low_risk_factor_results_liquidity_rank',
        'daily_low_risk_factor_results',
        ['liquidity_rank'],
    )
    op.create_index(
        'ix_daily_low_risk_factor_results_max_drawdown_120d_rank',
        'daily_low_risk_factor_results',
        ['max_drawdown_120d_rank'],
    )
    op.create_index(
        'ix_daily_low_risk_factor_results_volatility_120d_rank',
        'daily_low_risk_factor_results',
        ['volatility_120d_rank'],
    )
    op.create_index(
        'ix_daily_low_risk_factor_results_volatility_60d_rank',
        'daily_low_risk_factor_results',
        ['volatility_60d_rank'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_daily_low_risk_factor_results_volatility_60d_rank',
        table_name='daily_low_risk_factor_results',
    )
    op.drop_index(
        'ix_daily_low_risk_factor_results_volatility_120d_rank',
        table_name='daily_low_risk_factor_results',
    )
    op.drop_index(
        'ix_daily_low_risk_factor_results_max_drawdown_120d_rank',
        table_name='daily_low_risk_factor_results',
    )
    op.drop_index(
        'ix_daily_low_risk_factor_results_liquidity_rank',
        table_name='daily_low_risk_factor_results',
    )
    op.drop_index(
        'ix_daily_low_risk_factor_results_is_low_risk_eligible',
        table_name='daily_low_risk_factor_results',
    )
    op.drop_index(
        'ix_daily_low_risk_factor_results_low_risk_classification',
        table_name='daily_low_risk_factor_results',
    )
    op.drop_index(
        'ix_daily_low_risk_factor_results_market_score',
        table_name='daily_low_risk_factor_results',
    )
    op.drop_index(
        'ix_daily_low_risk_factor_results_market_eligible',
        table_name='daily_low_risk_factor_results',
    )
    op.drop_index(
        'ix_daily_low_risk_factor_results_symbol_id',
        table_name='daily_low_risk_factor_results',
    )
    op.drop_index(
        'ix_daily_low_risk_factor_results_market_date',
        table_name='daily_low_risk_factor_results',
    )
    op.drop_table('daily_low_risk_factor_results')
