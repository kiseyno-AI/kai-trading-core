"""Preserve each US-equity screening stage independently."""

from alembic import op


revision = '20260819_0074'
down_revision = '20260819_0073'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        'uq_us_equity_screening_symbol_date',
        'us_equity_screening_snapshots',
        type_='unique',
    )
    op.create_unique_constraint(
        'uq_us_equity_screening_symbol_date_stage',
        'us_equity_screening_snapshots',
        ['symbol_id', 'market_date', 'stage'],
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM us_equity_screening_snapshots AS newer
        USING us_equity_screening_snapshots AS older
        WHERE newer.symbol_id = older.symbol_id
          AND newer.market_date = older.market_date
          AND (
            newer.updated_at < older.updated_at
            OR (newer.updated_at = older.updated_at AND newer.id < older.id)
          )
        """
    )
    op.drop_constraint(
        'uq_us_equity_screening_symbol_date_stage',
        'us_equity_screening_snapshots',
        type_='unique',
    )
    op.create_unique_constraint(
        'uq_us_equity_screening_symbol_date',
        'us_equity_screening_snapshots',
        ['symbol_id', 'market_date'],
    )
