"""Add covering index for daily technical snapshot status counts."""

from alembic import op


revision = '20260729_0067'
down_revision = '20260729_0066'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute('DROP INDEX CONCURRENTLY IF EXISTS ix_daily_technical_snapshots_eod_run_captured_id')
        op.execute('SET statement_timeout = 0')
        op.execute('SET lock_timeout = 0')
        op.execute(
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_daily_technical_snapshots_eod_run_captured_id '
            'ON daily_technical_snapshots (eod_run_id, captured_at DESC, id)'
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute('DROP INDEX CONCURRENTLY IF EXISTS ix_daily_technical_snapshots_eod_run_captured_id')
