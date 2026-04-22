"""add daily piotroski filter results

Revision ID: 20260422_0020
Revises: 20260422_0019
Create Date: 2026-04-22 15:20:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260422_0020'
down_revision = '20260422_0019'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_piotroski_filter_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('pbr', sa.Numeric(18, 6), nullable=True),
        sa.Column('pbr_percentile_rank', sa.Numeric(12, 6), nullable=True),
        sa.Column('is_low_pbr', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('has_profitability_issue', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('has_capital_impairment', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('has_audit_issue', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('piotroski_score', sa.Integer(), nullable=True),
        sa.Column('passes_piotroski_score', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('momentum_percentile_rank', sa.Numeric(12, 6), nullable=True),
        sa.Column('is_bottom_momentum', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('passed_filter', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('filter_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_piotroski_filter_results_symbol_market_date'),
    )
    op.create_index('ix_daily_piotroski_filter_results_market_date', 'daily_piotroski_filter_results', ['market_date'])
    op.create_index('ix_daily_piotroski_filter_results_symbol_id', 'daily_piotroski_filter_results', ['symbol_id'])
    op.create_index('ix_daily_piotroski_filter_results_market_passed', 'daily_piotroski_filter_results', ['market_date', 'passed_filter'])
    op.create_index('ix_daily_piotroski_filter_results_market_pbr_rank', 'daily_piotroski_filter_results', ['market_date', 'pbr_percentile_rank'])
    op.create_index('ix_daily_piotroski_filter_results_market_score', 'daily_piotroski_filter_results', ['market_date', 'piotroski_score'])


def downgrade() -> None:
    op.drop_index('ix_daily_piotroski_filter_results_market_score', table_name='daily_piotroski_filter_results')
    op.drop_index('ix_daily_piotroski_filter_results_market_pbr_rank', table_name='daily_piotroski_filter_results')
    op.drop_index('ix_daily_piotroski_filter_results_market_passed', table_name='daily_piotroski_filter_results')
    op.drop_index('ix_daily_piotroski_filter_results_symbol_id', table_name='daily_piotroski_filter_results')
    op.drop_index('ix_daily_piotroski_filter_results_market_date', table_name='daily_piotroski_filter_results')
    op.drop_table('daily_piotroski_filter_results')
