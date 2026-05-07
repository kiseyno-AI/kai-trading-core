"""add persisted pykrx enrichment rows

Revision ID: 20260507_0047
Revises: 20260507_0046
Create Date: 2026-05-07 15:30:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260507_0047'
down_revision = '20260507_0046'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {index['name'] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_name = 'daily_pykrx_enrichment_rows'
    if not _table_exists(inspector, table_name):
        op.create_table(
            table_name,
            sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column('symbol_id', sa.Integer(), sa.ForeignKey('symbols.id', ondelete='CASCADE'), nullable=True),
            sa.Column('market_date', sa.Date(), nullable=False),
            sa.Column('market', sa.String(length=32), nullable=False, server_default='KRX'),
            sa.Column('category', sa.String(length=64), nullable=False),
            sa.Column('scope_type', sa.String(length=32), nullable=False),
            sa.Column('scope_key', sa.String(length=64), nullable=False),
            sa.Column('source', sa.String(length=32), nullable=False, server_default='pykrx'),
            sa.Column('payload', sa.JSON(), nullable=False),
            sa.Column('captured_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint(
                'market_date',
                'category',
                'scope_type',
                'scope_key',
                'source',
                name='uq_daily_pykrx_enrichment_rows_scope',
            ),
        )

    inspector = sa.inspect(bind)
    existing_indexes = _index_names(inspector, table_name)
    if 'ix_daily_pykrx_enrichment_rows_market_category' not in existing_indexes:
        op.create_index(
            'ix_daily_pykrx_enrichment_rows_market_category',
            table_name,
            ['market_date', 'category'],
            unique=False,
        )
    if 'ix_daily_pykrx_enrichment_rows_symbol_market_category' not in existing_indexes:
        op.create_index(
            'ix_daily_pykrx_enrichment_rows_symbol_market_category',
            table_name,
            ['symbol_id', 'market_date', 'category'],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_name = 'daily_pykrx_enrichment_rows'
    if not _table_exists(inspector, table_name):
        return

    existing_indexes = _index_names(inspector, table_name)
    if 'ix_daily_pykrx_enrichment_rows_symbol_market_category' in existing_indexes:
        op.drop_index('ix_daily_pykrx_enrichment_rows_symbol_market_category', table_name=table_name)
    if 'ix_daily_pykrx_enrichment_rows_market_category' in existing_indexes:
        op.drop_index('ix_daily_pykrx_enrichment_rows_market_category', table_name=table_name)
    op.drop_table(table_name)
