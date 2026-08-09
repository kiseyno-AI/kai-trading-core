"""Add latest EOD collection run lookup index."""

from alembic import op


revision = '20260809_0068'
down_revision = '20260729_0067'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute('SET statement_timeout = 0')
        op.execute('SET lock_timeout = 0')
        op.execute(
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_eod_collection_runs_market_trade_source_started_id '
            'ON eod_collection_runs (market, trade_date, source, started_at DESC, id DESC)'
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            'DROP INDEX CONCURRENTLY IF EXISTS ix_eod_collection_runs_market_trade_source_started_id'
        )
