"""add daily composite factor results

Revision ID: 20260424_0027
Revises: 20260424_0026
Create Date: 2026-04-24 20:20:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260424_0027'
down_revision = '20260424_0026'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_composite_factor_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('momentum_input', sa.Numeric(12, 6), nullable=True),
        sa.Column('value_input', sa.Numeric(12, 6), nullable=True),
        sa.Column('advanced_value_input', sa.Numeric(12, 6), nullable=True),
        sa.Column('low_risk_input', sa.Numeric(12, 6), nullable=True),
        sa.Column('low_beta_input', sa.Numeric(12, 6), nullable=True),
        sa.Column('momentum_contribution', sa.Numeric(12, 6), nullable=True),
        sa.Column('value_contribution', sa.Numeric(12, 6), nullable=True),
        sa.Column('advanced_value_contribution', sa.Numeric(12, 6), nullable=True),
        sa.Column('low_risk_contribution', sa.Numeric(12, 6), nullable=True),
        sa.Column('low_beta_contribution', sa.Numeric(12, 6), nullable=True),
        sa.Column('valid_factor_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('available_base_weight', sa.Numeric(12, 6), nullable=False, server_default='0'),
        sa.Column('has_required_inputs', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_hard_excluded', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('composite_score', sa.Numeric(12, 6), nullable=True),
        sa.Column('composite_rank', sa.Integer(), nullable=True),
        sa.Column('composite_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column(
            'composite_classification',
            sa.String(length=48),
            nullable=False,
            server_default='insufficient_inputs',
        ),
        sa.Column('is_composite_eligible', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('factor_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_composite_factor_results_symbol_market_date'),
    )
    op.create_index(
        'ix_daily_composite_factor_results_market_date',
        'daily_composite_factor_results',
        ['market_date'],
    )
    op.create_index(
        'ix_daily_composite_factor_results_symbol_id',
        'daily_composite_factor_results',
        ['symbol_id'],
    )
    op.create_index(
        'ix_daily_composite_factor_results_market_rank',
        'daily_composite_factor_results',
        ['market_date', 'composite_rank'],
    )
    op.create_index(
        'ix_daily_composite_factor_results_market_score',
        'daily_composite_factor_results',
        ['market_date', 'composite_score'],
    )
    op.create_index(
        'ix_daily_composite_factor_results_market_eligible',
        'daily_composite_factor_results',
        ['market_date', 'is_composite_eligible'],
    )
    op.create_index(
        'ix_daily_composite_factor_results_market_classification',
        'daily_composite_factor_results',
        ['market_date', 'composite_classification'],
    )
    op.create_index(
        'ix_daily_composite_factor_results_composite_rank',
        'daily_composite_factor_results',
        ['composite_rank'],
    )
    op.create_index(
        'ix_daily_composite_factor_results_is_composite_eligible',
        'daily_composite_factor_results',
        ['is_composite_eligible'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_daily_composite_factor_results_is_composite_eligible',
        table_name='daily_composite_factor_results',
    )
    op.drop_index(
        'ix_daily_composite_factor_results_composite_rank',
        table_name='daily_composite_factor_results',
    )
    op.drop_index(
        'ix_daily_composite_factor_results_market_classification',
        table_name='daily_composite_factor_results',
    )
    op.drop_index(
        'ix_daily_composite_factor_results_market_eligible',
        table_name='daily_composite_factor_results',
    )
    op.drop_index(
        'ix_daily_composite_factor_results_market_score',
        table_name='daily_composite_factor_results',
    )
    op.drop_index(
        'ix_daily_composite_factor_results_market_rank',
        table_name='daily_composite_factor_results',
    )
    op.drop_index(
        'ix_daily_composite_factor_results_symbol_id',
        table_name='daily_composite_factor_results',
    )
    op.drop_index(
        'ix_daily_composite_factor_results_market_date',
        table_name='daily_composite_factor_results',
    )
    op.drop_table('daily_composite_factor_results')
