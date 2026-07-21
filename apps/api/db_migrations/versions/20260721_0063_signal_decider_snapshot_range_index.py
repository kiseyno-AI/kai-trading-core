"""add signal decider snapshot range index

Revision ID: 20260721_0063
Revises: 20260721_0062
Create Date: 2026-07-22 06:55:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260721_0063'
down_revision = '20260721_0062'
branch_labels = None
depends_on = None


TABLE_NAME = 'signal_decider_decision_snapshots'
INDEX_NAME = 'ix_signal_decider_snapshots_agent_market_created'


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return any(index['name'] == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, TABLE_NAME) or _index_exists(inspector, TABLE_NAME, INDEX_NAME):
        return
    op.create_index(
        INDEX_NAME,
        TABLE_NAME,
        [
            'agent_name',
            sa.text('market_date DESC'),
            sa.text('created_at DESC'),
            sa.text('id DESC'),
        ],
        unique=False,
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table_exists(inspector, TABLE_NAME) and _index_exists(inspector, TABLE_NAME, INDEX_NAME):
        op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
