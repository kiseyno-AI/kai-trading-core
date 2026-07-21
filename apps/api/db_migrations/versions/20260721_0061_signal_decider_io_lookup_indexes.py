"""add signal decider IO lookup indexes

Revision ID: 20260721_0061
Revises: 20260721_0060
Create Date: 2026-07-22 05:15:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260721_0061'
down_revision = '20260721_0060'
branch_labels = None
depends_on = None


INDEXES = (
    (
        'external_news_items',
        'ix_external_news_items_symbol_collected_published',
        ['symbol_id', 'collected_at', 'published_at'],
    ),
    (
        'signal_decider_decision_snapshots',
        'ix_signal_decider_snapshots_market_agent_symbol_created',
        ['market_date', 'agent_name', 'symbol', 'created_at', 'id'],
    ),
    (
        'signal_decider_execution_plan_items',
        'ix_signal_decider_plan_source_date_lookup',
        ['execution_window', 'target_trade_date', 'source_market_date', 'id'],
    ),
)


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return any(index['name'] == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table_name, index_name, columns in INDEXES:
        if not _table_exists(inspector, table_name) or _index_exists(inspector, table_name, index_name):
            continue
        op.create_index(index_name, table_name, columns, unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table_name, index_name, _columns in reversed(INDEXES):
        if _table_exists(inspector, table_name) and _index_exists(inspector, table_name, index_name):
            op.drop_index(index_name, table_name=table_name)
