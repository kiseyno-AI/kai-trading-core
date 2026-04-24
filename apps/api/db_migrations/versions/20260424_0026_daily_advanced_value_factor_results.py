"""add daily advanced value factor results

Revision ID: 20260424_0026
Revises: 20260424_0025
Create Date: 2026-04-24 18:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260424_0026'
down_revision = '20260424_0025'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_advanced_value_factor_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('market_cap_proxy', sa.Numeric(24, 4), nullable=True),
        sa.Column('enterprise_value', sa.Numeric(24, 4), nullable=True),
        sa.Column('ebitda', sa.Numeric(24, 4), nullable=True),
        sa.Column('free_cash_flow', sa.Numeric(24, 4), nullable=True),
        sa.Column('fcf_yield', sa.Numeric(18, 6), nullable=True),
        sa.Column('ev_to_ebitda', sa.Numeric(18, 6), nullable=True),
        sa.Column('fcf_yield_status', sa.String(length=48), nullable=False, server_default='missing_metric'),
        sa.Column('ev_to_ebitda_status', sa.String(length=48), nullable=False, server_default='missing_metric'),
        sa.Column('fcf_yield_valid_for_scoring', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('ev_to_ebitda_valid_for_scoring', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('fcf_yield_rank', sa.Integer(), nullable=True),
        sa.Column('ev_to_ebitda_rank', sa.Integer(), nullable=True),
        sa.Column('fcf_yield_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('ev_to_ebitda_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('advanced_value_score', sa.Numeric(12, 6), nullable=True),
        sa.Column('advanced_value_rank', sa.Integer(), nullable=True),
        sa.Column('advanced_value_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('advanced_value_components_used', sa.Integer(), nullable=False, server_default='0'),
        sa.Column(
            'advanced_value_classification',
            sa.String(length=48),
            nullable=False,
            server_default='insufficient_components',
        ),
        sa.Column('is_advanced_value_eligible', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('factor_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'symbol_id',
            'market_date',
            name='uq_daily_advanced_value_factor_results_symbol_market_date',
        ),
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_market_date',
        'daily_advanced_value_factor_results',
        ['market_date'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_symbol_id',
        'daily_advanced_value_factor_results',
        ['symbol_id'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_market_rank',
        'daily_advanced_value_factor_results',
        ['market_date', 'advanced_value_rank'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_market_score',
        'daily_advanced_value_factor_results',
        ['market_date', 'advanced_value_score'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_market_eligible',
        'daily_advanced_value_factor_results',
        ['market_date', 'is_advanced_value_eligible'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_market_classification',
        'daily_advanced_value_factor_results',
        ['market_date', 'advanced_value_classification'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_fcf_yield_status',
        'daily_advanced_value_factor_results',
        ['fcf_yield_status'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_ev_to_ebitda_status',
        'daily_advanced_value_factor_results',
        ['ev_to_ebitda_status'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_fcf_yield_valid',
        'daily_advanced_value_factor_results',
        ['fcf_yield_valid_for_scoring'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_ev_to_ebitda_valid',
        'daily_advanced_value_factor_results',
        ['ev_to_ebitda_valid_for_scoring'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_fcf_yield_rank',
        'daily_advanced_value_factor_results',
        ['fcf_yield_rank'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_ev_to_ebitda_rank',
        'daily_advanced_value_factor_results',
        ['ev_to_ebitda_rank'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_advanced_value_rank',
        'daily_advanced_value_factor_results',
        ['advanced_value_rank'],
    )
    op.create_index(
        'ix_daily_advanced_value_factor_results_is_advanced_value_eligible',
        'daily_advanced_value_factor_results',
        ['is_advanced_value_eligible'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_daily_advanced_value_factor_results_is_advanced_value_eligible',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_advanced_value_rank',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_ev_to_ebitda_rank',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_fcf_yield_rank',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_ev_to_ebitda_valid',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_fcf_yield_valid',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_ev_to_ebitda_status',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_fcf_yield_status',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_market_classification',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_market_eligible',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_market_score',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_market_rank',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_symbol_id',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_index(
        'ix_daily_advanced_value_factor_results_market_date',
        table_name='daily_advanced_value_factor_results',
    )
    op.drop_table('daily_advanced_value_factor_results')
