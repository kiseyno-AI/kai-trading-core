"""Add DART fundamentals freshness lookup index."""

from alembic import op


revision = '20260729_0065'
down_revision = '20260721_0064'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        'CREATE INDEX IF NOT EXISTS ix_dart_fundamentals_snapshots_fs_updated_stock '
        'ON dart_fundamentals_snapshots (fs_div, updated_at, stock_code)'
    )


def downgrade() -> None:
    op.execute(
        'DROP INDEX IF EXISTS ix_dart_fundamentals_snapshots_fs_updated_stock'
    )
