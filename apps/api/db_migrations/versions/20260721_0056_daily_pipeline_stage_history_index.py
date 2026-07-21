"""add daily pipeline stage history lookup index

Revision ID: 20260721_0056
Revises: 20260721_0055
Create Date: 2026-07-21 18:45:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260721_0056'
down_revision = '20260721_0055'
branch_labels = None
depends_on = None


INDEX_NAME = 'ix_daily_pipeline_stage_runs_market_stage_updated'
TABLE_NAME = 'daily_pipeline_stage_runs'


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return any(index['name'] == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, TABLE_NAME) or _index_exists(inspector, TABLE_NAME, INDEX_NAME):
        return
    op.create_index(INDEX_NAME, TABLE_NAME, ['market_date', 'stage_id', 'updated_at', 'id'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table_exists(inspector, TABLE_NAME) and _index_exists(inspector, TABLE_NAME, INDEX_NAME):
        op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
