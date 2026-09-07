"""preserve point-in-time external-news symbol associations

Revision ID: 20260906_0078
Revises: 20260906_0077
Create Date: 2026-09-06 16:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260906_0078'
down_revision = '20260906_0077'
branch_labels = None
depends_on = None

TABLE_NAME = 'external_news_item_symbols'
INDEX_NAME = 'ix_external_news_item_symbols_symbol_associated'


def upgrade() -> None:
    op.create_table(
        TABLE_NAME,
        sa.Column('external_news_item_id', sa.Integer(), nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column(
            'associated_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ['external_news_item_id'],
            ['external_news_items.id'],
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint(
            'external_news_item_id',
            'symbol_id',
            name='pk_external_news_item_symbols',
        ),
    )
    op.create_index(
        INDEX_NAME,
        TABLE_NAME,
        ['symbol_id', 'associated_at', 'external_news_item_id'],
    )

    external_news_items = sa.table(
        'external_news_items',
        sa.column('id', sa.Integer()),
        sa.column('symbol_id', sa.Integer()),
        sa.column('collected_at', sa.DateTime(timezone=True)),
    )
    associations = sa.table(
        TABLE_NAME,
        sa.column('external_news_item_id', sa.Integer()),
        sa.column('symbol_id', sa.Integer()),
        sa.column('associated_at', sa.DateTime(timezone=True)),
    )
    op.get_bind().execute(
        sa.insert(associations).from_select(
            ['external_news_item_id', 'symbol_id', 'associated_at'],
            sa.select(
                external_news_items.c.id,
                external_news_items.c.symbol_id,
                external_news_items.c.collected_at,
            ),
        )
    )


def downgrade() -> None:
    op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
    op.drop_table(TABLE_NAME)
