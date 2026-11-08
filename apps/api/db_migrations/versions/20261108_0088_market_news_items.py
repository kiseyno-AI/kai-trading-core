"""market and business-line news topics

Revision ID: 20261108_0088
Revises: 20261027_0087
Create Date: 2026-11-08 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20261108_0088'
down_revision = '20261027_0087'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'market_news_items',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('topic_key', sa.String(length=64), nullable=False),
        sa.Column('region', sa.String(length=16), nullable=False),
        sa.Column('provider', sa.String(length=64), nullable=False),
        sa.Column('url', sa.Text(), nullable=False),
        sa.Column('url_hash', sa.String(length=64), nullable=False),
        sa.Column('title', sa.Text(), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('published_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('first_seen_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint('topic_key', 'url_hash', name='uq_market_news_items_topic_url'),
    )
    op.create_index('ix_market_news_items_topic_published', 'market_news_items', ['topic_key', 'published_at'])
    op.create_index('ix_market_news_items_published_at', 'market_news_items', ['published_at'])
    op.create_index('ix_market_news_items_first_seen_at', 'market_news_items', ['first_seen_at'])


def downgrade() -> None:
    op.drop_table('market_news_items')
