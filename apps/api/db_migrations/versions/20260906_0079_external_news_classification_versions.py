"""store external-news classifications as append-only versions

Revision ID: 20260906_0079
Revises: 20260906_0078
Create Date: 2026-09-06 17:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260906_0079'
down_revision = '20260906_0078'
branch_labels = None
depends_on = None

TABLE_NAME = 'external_news_item_classifications'
UNIQUE_NAME = 'uq_external_news_item_classifications_item_version'
INDEX_NAME = 'ix_external_news_item_classifications_item_classified'


def upgrade() -> None:
    op.create_table(
        TABLE_NAME,
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('external_news_item_id', sa.Integer(), nullable=False),
        sa.Column('classifier_version', sa.String(length=64), nullable=False),
        sa.Column(
            'classified_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column('classification_payload', sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ['external_news_item_id'],
            ['external_news_items.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'external_news_item_id',
            'classifier_version',
            name=UNIQUE_NAME,
        ),
    )
    op.create_index(
        INDEX_NAME,
        TABLE_NAME,
        ['external_news_item_id', 'classified_at', 'id'],
    )


def downgrade() -> None:
    op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
    op.drop_table(TABLE_NAME)
