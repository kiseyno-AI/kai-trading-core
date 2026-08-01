"""Add external-news status count lookup index."""

from alembic import op


revision = '20260729_0066'
down_revision = '20260729_0065'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        'CREATE INDEX IF NOT EXISTS ix_external_news_items_collected_symbol '
        'ON external_news_items (collected_at, symbol_id)'
    )


def downgrade() -> None:
    op.execute('DROP INDEX IF EXISTS ix_external_news_items_collected_symbol')
