"""add daily quality factor results

Revision ID: 20260429_0039
Revises: 20260429_0038
Create Date: 2026-04-29 22:45:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260429_0039'
down_revision = '20260429_0038'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_quality_factor_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('roe', sa.Numeric(18, 6), nullable=True),
        sa.Column('roa', sa.Numeric(18, 6), nullable=True),
        sa.Column('gross_profit_to_assets', sa.Numeric(18, 6), nullable=True),
        sa.Column('operating_cash_flow_to_net_income', sa.Numeric(18, 6), nullable=True),
        sa.Column('debt_ratio', sa.Numeric(18, 6), nullable=True),
        sa.Column('accruals_ratio', sa.Numeric(18, 6), nullable=True),
        sa.Column('roe_status', sa.String(length=48), nullable=False, server_default='missing_metric'),
        sa.Column('roa_status', sa.String(length=48), nullable=False, server_default='missing_metric'),
        sa.Column('gross_profit_to_assets_status', sa.String(length=48), nullable=False, server_default='missing_metric'),
        sa.Column(
            'operating_cash_flow_to_net_income_status',
            sa.String(length=48),
            nullable=False,
            server_default='missing_metric',
        ),
        sa.Column('debt_ratio_status', sa.String(length=48), nullable=False, server_default='missing_metric'),
        sa.Column('accruals_ratio_status', sa.String(length=48), nullable=False, server_default='missing_metric'),
        sa.Column('roe_rank', sa.Integer(), nullable=True),
        sa.Column('roa_rank', sa.Integer(), nullable=True),
        sa.Column('gross_profit_to_assets_rank', sa.Integer(), nullable=True),
        sa.Column('operating_cash_flow_to_net_income_rank', sa.Integer(), nullable=True),
        sa.Column('debt_ratio_rank', sa.Integer(), nullable=True),
        sa.Column('accruals_ratio_rank', sa.Integer(), nullable=True),
        sa.Column('roe_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('roa_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('gross_profit_to_assets_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('operating_cash_flow_to_net_income_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('debt_ratio_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('accruals_ratio_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('quality_score', sa.Numeric(12, 6), nullable=True),
        sa.Column('quality_rank', sa.Integer(), nullable=True),
        sa.Column('quality_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('components_possible', sa.Integer(), nullable=False, server_default='6'),
        sa.Column('components_used', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('is_quality_score_complete', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            'quality_status',
            sa.String(length=48),
            nullable=False,
            server_default='insufficient_components',
        ),
        sa.Column('factor_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_quality_factor_results_symbol_market_date',
        ),
    )
    op.create_index(
        'ix_daily_quality_factor_results_market_date',
        'daily_quality_factor_results',
        ['market_date'],
    )
    op.create_index(
        'ix_daily_quality_factor_results_symbol_id',
        'daily_quality_factor_results',
        ['symbol_id'],
    )
    op.create_index(
        'ix_daily_quality_factor_results_market_rank',
        'daily_quality_factor_results',
        ['market_date', 'quality_rank'],
    )
    op.create_index(
        'ix_daily_quality_factor_results_market_score',
        'daily_quality_factor_results',
        ['market_date', 'quality_score'],
    )
    op.create_index(
        'ix_daily_quality_factor_results_market_status',
        'daily_quality_factor_results',
        ['market_date', 'quality_status'],
    )
    op.create_index(
        'ix_daily_quality_factor_results_quality_rank',
        'daily_quality_factor_results',
        ['quality_rank'],
    )
    op.create_index(
        'ix_daily_quality_factor_results_quality_status',
        'daily_quality_factor_results',
        ['quality_status'],
    )
    op.create_index(
        'ix_daily_quality_factor_results_is_quality_score_complete',
        'daily_quality_factor_results',
        ['is_quality_score_complete'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_daily_quality_factor_results_is_quality_score_complete',
        table_name='daily_quality_factor_results',
    )
    op.drop_index(
        'ix_daily_quality_factor_results_quality_status',
        table_name='daily_quality_factor_results',
    )
    op.drop_index(
        'ix_daily_quality_factor_results_quality_rank',
        table_name='daily_quality_factor_results',
    )
    op.drop_index(
        'ix_daily_quality_factor_results_market_status',
        table_name='daily_quality_factor_results',
    )
    op.drop_index(
        'ix_daily_quality_factor_results_market_score',
        table_name='daily_quality_factor_results',
    )
    op.drop_index(
        'ix_daily_quality_factor_results_market_rank',
        table_name='daily_quality_factor_results',
    )
    op.drop_index(
        'ix_daily_quality_factor_results_symbol_id',
        table_name='daily_quality_factor_results',
    )
    op.drop_index(
        'ix_daily_quality_factor_results_market_date',
        table_name='daily_quality_factor_results',
    )
    op.drop_table('daily_quality_factor_results')
