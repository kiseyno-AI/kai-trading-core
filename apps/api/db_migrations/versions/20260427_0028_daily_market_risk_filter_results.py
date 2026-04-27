"""add daily market risk filter results

Revision ID: 20260427_0028
Revises: 20260424_0027
Create Date: 2026-04-27 09:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260427_0028'
down_revision = '20260424_0027'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'daily_market_risk_filter_results',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('eod_run_id', sa.Integer(), nullable=True),
        sa.Column('market_date', sa.Date(), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('risk_state', sa.String(length=16), nullable=False, server_default='pass'),
        sa.Column('passed_filter', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('watchlist_flagged', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('market_warning_code', sa.String(length=8), nullable=True),
        sa.Column('market_warning_name', sa.String(length=64), nullable=True),
        sa.Column('is_management_item', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_investment_caution', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_short_overheated', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('short_overheat_class_code', sa.String(length=8), nullable=True),
        sa.Column('is_short_sale_overheated', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('vi_class_code', sa.String(length=8), nullable=True),
        sa.Column('is_vi_triggered', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_dishonest_disclosure', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('is_trade_halted', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('filter_payload', sa.JSON(), nullable=False),
        sa.Column('captured_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['eod_run_id'], ['eod_collection_runs.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol_id', 'market_date', name='uq_daily_market_risk_filter_results_symbol_market_date'),
    )
    op.create_index(
        'ix_daily_market_risk_filter_results_eod_run_id',
        'daily_market_risk_filter_results',
        ['eod_run_id'],
    )
    op.create_index(
        'ix_daily_market_risk_filter_results_market_date',
        'daily_market_risk_filter_results',
        ['market_date'],
    )
    op.create_index(
        'ix_daily_market_risk_filter_results_market_passed',
        'daily_market_risk_filter_results',
        ['market_date', 'passed_filter'],
    )
    op.create_index(
        'ix_daily_market_risk_filter_results_market_state',
        'daily_market_risk_filter_results',
        ['market_date', 'risk_state'],
    )
    op.create_index(
        'ix_daily_market_risk_filter_results_market_watchlist',
        'daily_market_risk_filter_results',
        ['market_date', 'watchlist_flagged'],
    )
    op.create_index(
        'ix_daily_market_risk_filter_results_passed_filter',
        'daily_market_risk_filter_results',
        ['passed_filter'],
    )
    op.create_index(
        'ix_daily_market_risk_filter_results_risk_state',
        'daily_market_risk_filter_results',
        ['risk_state'],
    )
    op.create_index(
        'ix_daily_market_risk_filter_results_symbol_id',
        'daily_market_risk_filter_results',
        ['symbol_id'],
    )
    op.create_index(
        'ix_daily_market_risk_filter_results_watchlist_flagged',
        'daily_market_risk_filter_results',
        ['watchlist_flagged'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_daily_market_risk_filter_results_watchlist_flagged',
        table_name='daily_market_risk_filter_results',
    )
    op.drop_index(
        'ix_daily_market_risk_filter_results_symbol_id',
        table_name='daily_market_risk_filter_results',
    )
    op.drop_index(
        'ix_daily_market_risk_filter_results_risk_state',
        table_name='daily_market_risk_filter_results',
    )
    op.drop_index(
        'ix_daily_market_risk_filter_results_passed_filter',
        table_name='daily_market_risk_filter_results',
    )
    op.drop_index(
        'ix_daily_market_risk_filter_results_market_watchlist',
        table_name='daily_market_risk_filter_results',
    )
    op.drop_index(
        'ix_daily_market_risk_filter_results_market_state',
        table_name='daily_market_risk_filter_results',
    )
    op.drop_index(
        'ix_daily_market_risk_filter_results_market_passed',
        table_name='daily_market_risk_filter_results',
    )
    op.drop_index(
        'ix_daily_market_risk_filter_results_market_date',
        table_name='daily_market_risk_filter_results',
    )
    op.drop_index(
        'ix_daily_market_risk_filter_results_eod_run_id',
        table_name='daily_market_risk_filter_results',
    )
    op.drop_table('daily_market_risk_filter_results')
