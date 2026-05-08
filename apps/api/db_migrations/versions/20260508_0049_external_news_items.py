"""add external news item persistence

Revision ID: 20260508_0049
Revises: 20260507_0048
Create Date: 2026-05-08 08:50:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260508_0049'
down_revision = '20260507_0048'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'external_news_items',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol_id', sa.Integer(), nullable=False),
        sa.Column('provider', sa.String(length=64), nullable=False),
        sa.Column('provider_item_id', sa.String(length=128), nullable=True),
        sa.Column('source', sa.String(length=64), nullable=False),
        sa.Column('title', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('url', sa.String(length=2048), nullable=False),
        sa.Column('source_url', sa.String(length=2048), nullable=True),
        sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('collected_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('provider', 'provider_item_id', name='uq_external_news_items_provider_item_id'),
        sa.UniqueConstraint('provider', 'url', name='uq_external_news_items_provider_url'),
    )
    op.create_index('ix_external_news_items_collected_at', 'external_news_items', ['collected_at'])
    op.create_index('ix_external_news_items_provider_collected', 'external_news_items', ['provider', 'collected_at'])
    op.create_index('ix_external_news_items_published_at', 'external_news_items', ['published_at'])
    op.create_index('ix_external_news_items_symbol_id', 'external_news_items', ['symbol_id'])
    op.create_index('ix_external_news_items_symbol_published', 'external_news_items', ['symbol_id', 'published_at'])


def downgrade() -> None:
    op.drop_index('ix_external_news_items_symbol_published', table_name='external_news_items')
    op.drop_index('ix_external_news_items_symbol_id', table_name='external_news_items')
    op.drop_index('ix_external_news_items_published_at', table_name='external_news_items')
    op.drop_index('ix_external_news_items_provider_collected', table_name='external_news_items')
    op.drop_index('ix_external_news_items_collected_at', table_name='external_news_items')
    op.drop_table('external_news_items')
