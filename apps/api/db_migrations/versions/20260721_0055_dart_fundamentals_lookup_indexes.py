"""add DART fundamentals lookup indexes

Revision ID: 20260721_0055
Revises: 20260721_0054
Create Date: 2026-07-21 17:36:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260721_0055'
down_revision = '20260721_0054'
branch_labels = None
depends_on = None


INDEXES = (
    (
        'ix_dart_disclosure_events_financial_status_symbol_receipt',
        'dart_disclosure_events',
        [
            'is_financial_report',
            'fundamentals_capture_status',
            'stock_code',
            'receipt_date',
            'id',
        ],
    ),
    (
        'ix_dart_fundamentals_snapshots_symbol_fs_year_report',
        'dart_fundamentals_snapshots',
        [
            'stock_code',
            'fs_div',
            'business_year',
            'report_code',
            'id',
        ],
    ),
)


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return any(index['name'] == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for index_name, table_name, columns in INDEXES:
        if not _table_exists(inspector, table_name) or _index_exists(inspector, table_name, index_name):
            continue
        op.create_index(index_name, table_name, columns, unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for index_name, table_name, _columns in reversed(INDEXES):
        if _table_exists(inspector, table_name) and _index_exists(inspector, table_name, index_name):
            op.drop_index(index_name, table_name=table_name)
