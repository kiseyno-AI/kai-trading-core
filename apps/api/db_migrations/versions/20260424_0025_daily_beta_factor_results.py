"""add daily beta factor results

Revision ID: 20260424_0025
Revises: 20260424_0024
Create Date: 2026-04-24 20:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260424_0025'
down_revision = '20260424_0024'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_beta_factor_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('benchmark_id', sa.String(length=32), nullable=False, server_default='KRX_EQW'),
        sa.Column('benchmark_market', sa.String(length=32), nullable=False, server_default='KRX'),
        sa.Column('beta_60d', sa.Numeric(12, 6), nullable=True),
        sa.Column('beta_120d', sa.Numeric(12, 6), nullable=True),
        sa.Column('beta_60d_rank', sa.Integer(), nullable=True),
        sa.Column('beta_120d_rank', sa.Integer(), nullable=True),
        sa.Column('beta_60d_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('beta_120d_percentile', sa.Numeric(12, 6), nullable=True),
        sa.Column('low_beta_score', sa.Numeric(12, 6), nullable=True),
        sa.Column(
            'low_beta_classification',
            sa.String(length=32),
            nullable=False,
            server_default='insufficient_history',
        ),
        sa.Column('is_low_beta_eligible', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('history_days_available', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('factor_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_beta_factor_results_symbol_market_date'),
    )
    op.create_index('ix_daily_beta_factor_results_market_date', 'daily_beta_factor_results', ['market_date'])
    op.create_index('ix_daily_beta_factor_results_symbol_id', 'daily_beta_factor_results', ['symbol_id'])
    op.create_index(
        'ix_daily_beta_factor_results_market_eligible',
        'daily_beta_factor_results',
        ['market_date', 'is_low_beta_eligible'],
    )
    op.create_index(
        'ix_daily_beta_factor_results_market_score',
        'daily_beta_factor_results',
        ['market_date', 'low_beta_score'],
    )
    op.create_index(
        'ix_daily_beta_factor_results_low_beta_classification',
        'daily_beta_factor_results',
        ['low_beta_classification'],
    )
    op.create_index(
        'ix_daily_beta_factor_results_is_low_beta_eligible',
        'daily_beta_factor_results',
        ['is_low_beta_eligible'],
    )
    op.create_index(
        'ix_daily_beta_factor_results_beta_60d_rank',
        'daily_beta_factor_results',
        ['beta_60d_rank'],
    )
    op.create_index(
        'ix_daily_beta_factor_results_beta_120d_rank',
        'daily_beta_factor_results',
        ['beta_120d_rank'],
    )


def downgrade() -> None:
    op.drop_index('ix_daily_beta_factor_results_beta_120d_rank', table_name='daily_beta_factor_results')
    op.drop_index('ix_daily_beta_factor_results_beta_60d_rank', table_name='daily_beta_factor_results')
    op.drop_index('ix_daily_beta_factor_results_is_low_beta_eligible', table_name='daily_beta_factor_results')
    op.drop_index(
        'ix_daily_beta_factor_results_low_beta_classification',
        table_name='daily_beta_factor_results',
    )
    op.drop_index('ix_daily_beta_factor_results_market_score', table_name='daily_beta_factor_results')
    op.drop_index('ix_daily_beta_factor_results_market_eligible', table_name='daily_beta_factor_results')
    op.drop_index('ix_daily_beta_factor_results_symbol_id', table_name='daily_beta_factor_results')
    op.drop_index('ix_daily_beta_factor_results_market_date', table_name='daily_beta_factor_results')
    op.drop_table('daily_beta_factor_results')
