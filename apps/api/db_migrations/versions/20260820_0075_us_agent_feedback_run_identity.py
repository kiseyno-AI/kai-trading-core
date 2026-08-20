"""Preserve immutable US agent feedback history across reruns."""

from alembic import op


revision = '20260820_0075'
down_revision = '20260819_0074'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        'uq_us_agent_feedback_date_symbol_agent',
        'us_agent_feedback',
        type_='unique',
    )
    op.create_unique_constraint(
        'uq_us_agent_feedback_run_date_symbol_agent',
        'us_agent_feedback',
        ['run_id', 'market_date', 'symbol_id', 'agent_name'],
    )


def downgrade() -> None:
    op.execute(
        """
        DELETE FROM us_agent_feedback AS older
        USING us_agent_feedback AS newer
        WHERE older.market_date = newer.market_date
          AND older.symbol_id = newer.symbol_id
          AND older.agent_name = newer.agent_name
          AND (
            older.updated_at < newer.updated_at
            OR (older.updated_at = newer.updated_at AND older.id < newer.id)
          )
        """
    )
    op.drop_constraint(
        'uq_us_agent_feedback_run_date_symbol_agent',
        'us_agent_feedback',
        type_='unique',
    )
    op.create_unique_constraint(
        'uq_us_agent_feedback_date_symbol_agent',
        'us_agent_feedback',
        ['market_date', 'symbol_id', 'agent_name'],
    )
