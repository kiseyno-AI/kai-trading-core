"""add daily value factor results

Revision ID: 20260424_0024
Revises: 20260424_0023
Create Date: 2026-04-24 13:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260424_0024'
down_revision = '20260424_0023'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_value_factor_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('per', sa.Numeric(18, 6), nullable=True),
        sa.Column('pbr', sa.Numeric(18, 6), nullable=True),
        sa.Column('dividend_yield', sa.Numeric(18, 6), nullable=True),
        sa.Column('per_status', sa.String(length=32), nullable=False, server_default='missing'),
        sa.Column('per_rank', sa.Integer(), nullable=True),
        sa.Column('pbr_rank', sa.Integer(), nullable=True),
        sa.Column('dividend_yield_rank', sa.Integer(), nullable=True),
        sa.Column('per_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('pbr_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('dividend_yield_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('value_score', sa.Numeric(12, 6), nullable=True),
        sa.Column('value_rank', sa.Integer(), nullable=True),
        sa.Column('value_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('value_status', sa.String(length=32), nullable=False, server_default='missing_metric'),
        sa.Column('factor_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_value_factor_results_symbol_market_date'),
    )
    op.create_index('ix_daily_value_factor_results_market_date', 'daily_value_factor_results', ['market_date'])
    op.create_index('ix_daily_value_factor_results_symbol_id', 'daily_value_factor_results', ['symbol_id'])
    op.create_index('ix_daily_value_factor_results_market_rank', 'daily_value_factor_results', ['market_date', 'value_rank'])
    op.create_index('ix_daily_value_factor_results_market_score', 'daily_value_factor_results', ['market_date', 'value_score'])
    op.create_index('ix_daily_value_factor_results_market_status', 'daily_value_factor_results', ['market_date', 'value_status'])
    op.create_index('ix_daily_value_factor_results_per_status', 'daily_value_factor_results', ['per_status'])
    op.create_index('ix_daily_value_factor_results_per_rank', 'daily_value_factor_results', ['per_rank'])
    op.create_index('ix_daily_value_factor_results_pbr_rank', 'daily_value_factor_results', ['pbr_rank'])
    op.create_index('ix_daily_value_factor_results_dividend_yield_rank', 'daily_value_factor_results', ['dividend_yield_rank'])
    op.create_index('ix_daily_value_factor_results_value_rank', 'daily_value_factor_results', ['value_rank'])


def downgrade() -> None:
    op.drop_index('ix_daily_value_factor_results_value_rank', table_name='daily_value_factor_results')
    op.drop_index('ix_daily_value_factor_results_dividend_yield_rank', table_name='daily_value_factor_results')
    op.drop_index('ix_daily_value_factor_results_pbr_rank', table_name='daily_value_factor_results')
    op.drop_index('ix_daily_value_factor_results_per_rank', table_name='daily_value_factor_results')
    op.drop_index('ix_daily_value_factor_results_per_status', table_name='daily_value_factor_results')
    op.drop_index('ix_daily_value_factor_results_market_status', table_name='daily_value_factor_results')
    op.drop_index('ix_daily_value_factor_results_market_score', table_name='daily_value_factor_results')
    op.drop_index('ix_daily_value_factor_results_market_rank', table_name='daily_value_factor_results')
    op.drop_index('ix_daily_value_factor_results_symbol_id', table_name='daily_value_factor_results')
    op.drop_index('ix_daily_value_factor_results_market_date', table_name='daily_value_factor_results')
    op.drop_table('daily_value_factor_results')
