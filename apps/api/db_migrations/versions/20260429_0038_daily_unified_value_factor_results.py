"""add daily unified value factor results

Revision ID: 20260429_0038
Revises: 20260429_0037
Create Date: 2026-04-29 20:15:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260429_0038'
down_revision = '20260429_0037'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_unified_value_factor_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('per', sa.Numeric(18, 6), nullable=True),
        sa.Column('pbr', sa.Numeric(18, 6), nullable=True),
        sa.Column('dividend_yield', sa.Numeric(18, 6), nullable=True),
        sa.Column('fcf_yield', sa.Numeric(18, 6), nullable=True),
        sa.Column('ev_to_ebitda', sa.Numeric(18, 6), nullable=True),
        sa.Column('per_status', sa.String(length=48), nullable=False, server_default='missing'),
        sa.Column('pbr_status', sa.String(length=48), nullable=False, server_default='missing'),
        sa.Column('dividend_yield_status', sa.String(length=48), nullable=False, server_default='missing'),
        sa.Column('fcf_yield_status', sa.String(length=48), nullable=False, server_default='missing_metric'),
        sa.Column('ev_to_ebitda_status', sa.String(length=48), nullable=False, server_default='missing_metric'),
        sa.Column('per_rank', sa.Integer(), nullable=True),
        sa.Column('pbr_rank', sa.Integer(), nullable=True),
        sa.Column('dividend_yield_rank', sa.Integer(), nullable=True),
        sa.Column('fcf_yield_rank', sa.Integer(), nullable=True),
        sa.Column('ev_to_ebitda_rank', sa.Integer(), nullable=True),
        sa.Column('per_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('pbr_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('dividend_yield_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('fcf_yield_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('ev_to_ebitda_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('unified_value_score', sa.Numeric(12, 6), nullable=True),
        sa.Column('unified_value_rank', sa.Integer(), nullable=True),
        sa.Column('unified_value_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('components_used', sa.Integer(), nullable=False, server_default='0'),
        sa.Column(
            'unified_value_status',
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
            name='uq_daily_unified_value_factor_results_symbol_market_date',
        ),
    )
    op.create_index(
        'ix_daily_unified_value_factor_results_market_date',
        'daily_unified_value_factor_results',
        ['market_date'],
    )
    op.create_index(
        'ix_daily_unified_value_factor_results_symbol_id',
        'daily_unified_value_factor_results',
        ['symbol_id'],
    )
    op.create_index(
        'ix_daily_unified_value_factor_results_market_rank',
        'daily_unified_value_factor_results',
        ['market_date', 'unified_value_rank'],
    )
    op.create_index(
        'ix_daily_unified_value_factor_results_market_score',
        'daily_unified_value_factor_results',
        ['market_date', 'unified_value_score'],
    )
    op.create_index(
        'ix_daily_unified_value_factor_results_market_status',
        'daily_unified_value_factor_results',
        ['market_date', 'unified_value_status'],
    )
    op.create_index(
        'ix_daily_unified_value_factor_results_per_status',
        'daily_unified_value_factor_results',
        ['per_status'],
    )
    op.create_index(
        'ix_daily_unified_value_factor_results_pbr_status',
        'daily_unified_value_factor_results',
        ['pbr_status'],
    )
    op.create_index(
        'ix_daily_unified_value_factor_results_dividend_yield_status',
        'daily_unified_value_factor_results',
        ['dividend_yield_status'],
    )
    op.create_index(
        'ix_daily_unified_value_factor_results_fcf_yield_status',
        'daily_unified_value_factor_results',
        ['fcf_yield_status'],
    )
    op.create_index(
        'ix_daily_unified_value_factor_results_ev_to_ebitda_status',
        'daily_unified_value_factor_results',
        ['ev_to_ebitda_status'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_daily_unified_value_factor_results_ev_to_ebitda_status',
        table_name='daily_unified_value_factor_results',
    )
    op.drop_index(
        'ix_daily_unified_value_factor_results_fcf_yield_status',
        table_name='daily_unified_value_factor_results',
    )
    op.drop_index(
        'ix_daily_unified_value_factor_results_dividend_yield_status',
        table_name='daily_unified_value_factor_results',
    )
    op.drop_index(
        'ix_daily_unified_value_factor_results_pbr_status',
        table_name='daily_unified_value_factor_results',
    )
    op.drop_index(
        'ix_daily_unified_value_factor_results_per_status',
        table_name='daily_unified_value_factor_results',
    )
    op.drop_index(
        'ix_daily_unified_value_factor_results_market_status',
        table_name='daily_unified_value_factor_results',
    )
    op.drop_index(
        'ix_daily_unified_value_factor_results_market_score',
        table_name='daily_unified_value_factor_results',
    )
    op.drop_index(
        'ix_daily_unified_value_factor_results_market_rank',
        table_name='daily_unified_value_factor_results',
    )
    op.drop_index(
        'ix_daily_unified_value_factor_results_symbol_id',
        table_name='daily_unified_value_factor_results',
    )
    op.drop_index(
        'ix_daily_unified_value_factor_results_market_date',
        table_name='daily_unified_value_factor_results',
    )
    op.drop_table('daily_unified_value_factor_results')
