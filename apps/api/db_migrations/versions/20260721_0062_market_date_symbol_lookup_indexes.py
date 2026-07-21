"""add market-date symbol lookup indexes

Revision ID: 20260721_0062
Revises: 20260721_0061
Create Date: 2026-07-22 05:40:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260721_0062'
down_revision = '20260721_0061'
branch_labels = None
depends_on = None


INDEXES = (
    (
        'daily_price_bars',
        'ix_daily_price_bars_trade_date_symbol',
        ['trade_date', 'symbol_id'],
    ),
    (
        'daily_market_risk_filter_results',
        'ix_daily_market_risk_filter_results_market_symbol',
        ['market_date', 'symbol_id'],
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
