"""add missing-data category payload to agent feedback

Revision ID: 20260507_0046
Revises: 20260506_0045
Create Date: 2026-05-07 10:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260507_0046'
down_revision = '20260506_0045'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {column['name'] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_name = 'agent_context_feedback'
    if not _table_exists(inspector, table_name):
        return
    if 'missing_data_categories_payload' not in _column_names(inspector, table_name):
        op.add_column(table_name, sa.Column('missing_data_categories_payload', sa.JSON(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_name = 'agent_context_feedback'
    if not _table_exists(inspector, table_name):
        return
    if 'missing_data_categories_payload' in _column_names(inspector, table_name):
        op.drop_column(table_name, 'missing_data_categories_payload')
