"""add DART failed attempt lookup index

Revision ID: 20260721_0064
Revises: 20260721_0063
Create Date: 2026-07-22 07:15:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260721_0064'
down_revision = '20260721_0063'
branch_labels = None
depends_on = None


TABLE_NAME = 'dart_disclosure_events'
INDEX_NAME = 'ix_dart_disclosure_events_failed_attempt_stock'


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
        ['fundamentals_last_attempt_at', 'stock_code'],
        unique=False,
        postgresql_where=sa.text(
            "is_financial_report IS TRUE "
            "AND stock_code IS NOT NULL "
            "AND fundamentals_capture_status = 'failed'"
        ),
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table_exists(inspector, TABLE_NAME) and _index_exists(inspector, TABLE_NAME, INDEX_NAME):
        op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
