"""add DART disclosure fingerprint lookup index

Revision ID: 20260721_0060
Revises: 20260721_0059
Create Date: 2026-07-21 23:40:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260721_0060'
down_revision = '20260721_0059'
branch_labels = None
depends_on = None


INDEX_NAME = 'ix_dart_disclosure_events_stock_receipt_updated'
TABLE_NAME = 'dart_disclosure_events'


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return any(index['name'] == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, TABLE_NAME) or _index_exists(inspector, TABLE_NAME, INDEX_NAME):
        return
    op.create_index(INDEX_NAME, TABLE_NAME, ['stock_code', 'receipt_date', 'updated_at'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table_exists(inspector, TABLE_NAME) and _index_exists(inspector, TABLE_NAME, INDEX_NAME):
        op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
