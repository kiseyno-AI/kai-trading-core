"""add agent feedback reuse lookup indexes

Revision ID: 20260721_0059
Revises: 20260721_0058
Create Date: 2026-07-21 23:25:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260721_0059'
down_revision = '20260721_0058'
branch_labels = None
depends_on = None


INDEXES = {
    'agent_context_feedback': 'ix_agent_context_feedback_symbol_agent_recent',
    'agent_context_feedback_failures': 'ix_agent_context_feedback_failures_symbol_agent_recent',
}


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return any(index['name'] == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table_name, index_name in INDEXES.items():
        if not _table_exists(inspector, table_name) or _index_exists(inspector, table_name, index_name):
            continue
        op.create_index(
            index_name,
            table_name,
            ['symbol', 'agent_name', 'market_date', 'received_at', 'id'],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table_name, index_name in reversed(INDEXES.items()):
        if _table_exists(inspector, table_name) and _index_exists(inspector, table_name, index_name):
            op.drop_index(index_name, table_name=table_name)
