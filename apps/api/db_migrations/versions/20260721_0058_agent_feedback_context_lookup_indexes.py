"""add agent feedback context lookup indexes

Revision ID: 20260721_0058
Revises: 20260721_0057
Create Date: 2026-07-21 23:05:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260721_0058'
down_revision = '20260721_0057'
branch_labels = None
depends_on = None


RECENT_ROW_INDEXES = {
    'daily_signal_features': 'ix_daily_signal_features_symbol_recent',
    'daily_value_factor_results': 'ix_daily_value_factor_results_symbol_recent',
    'daily_unified_value_factor_results': 'ix_daily_unified_value_factor_results_symbol_recent',
    'daily_quality_factor_results': 'ix_daily_quality_factor_results_symbol_recent',
    'daily_momentum_factor_results': 'ix_daily_momentum_factor_results_symbol_recent',
    'daily_advanced_value_factor_results': 'ix_daily_advanced_value_factor_results_symbol_recent',
    'daily_low_risk_factor_results': 'ix_daily_low_risk_factor_results_symbol_recent',
    'daily_beta_factor_results': 'ix_daily_beta_factor_results_symbol_recent',
    'daily_composite_factor_results': 'ix_daily_composite_factor_results_symbol_recent',
}
PLAN_INDEX_NAME = 'ix_signal_decider_plan_active_symbol_lookup'
PLAN_TABLE_NAME = 'signal_decider_execution_plan_items'


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return any(index['name'] == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table_name, index_name in RECENT_ROW_INDEXES.items():
        if not _table_exists(inspector, table_name) or _index_exists(inspector, table_name, index_name):
            continue
        op.create_index(
            index_name,
            table_name,
            ['symbol_id', 'market_date', 'captured_at', 'id'],
            unique=False,
        )
    if _table_exists(inspector, PLAN_TABLE_NAME) and not _index_exists(inspector, PLAN_TABLE_NAME, PLAN_INDEX_NAME):
        op.create_index(
            PLAN_INDEX_NAME,
            PLAN_TABLE_NAME,
            ['symbol', 'target_trade_date', 'execution_status', 'id'],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table_exists(inspector, PLAN_TABLE_NAME) and _index_exists(inspector, PLAN_TABLE_NAME, PLAN_INDEX_NAME):
        op.drop_index(PLAN_INDEX_NAME, table_name=PLAN_TABLE_NAME)
    for table_name, index_name in reversed(RECENT_ROW_INDEXES.items()):
        if _table_exists(inspector, table_name) and _index_exists(inspector, table_name, index_name):
            op.drop_index(index_name, table_name=table_name)
