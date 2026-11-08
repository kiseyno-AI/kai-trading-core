"""allow independent WAIT rechecks per decision agent

Downgrade is intentionally refused after a symbol/date has durable events for
multiple agents.  Collapsing those rows would destroy execution evidence, so
operators must retain this revision and apply a forward fix instead.

Revision ID: 20261025_0085
Revises: 20261021_0084
Create Date: 2026-10-25 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20261025_0085'
down_revision = '20261021_0084'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        'uq_signal_decider_wait_recheck_target_symbol',
        'signal_decider_wait_recheck_events',
        type_='unique',
    )
    op.create_unique_constraint(
        'uq_signal_decider_wait_recheck_target_symbol_agent',
        'signal_decider_wait_recheck_events',
        ['target_trade_date', 'symbol', 'agent_name'],
    )


def downgrade() -> None:
    dual_lane_row = op.get_bind().execute(
        sa.text(
            """
            SELECT target_trade_date, symbol, COUNT(*) AS lane_count
            FROM signal_decider_wait_recheck_events
            GROUP BY target_trade_date, symbol
            HAVING COUNT(DISTINCT agent_name) > 1
            LIMIT 1
            """
        )
    ).first()
    if dual_lane_row is not None:
        raise RuntimeError(
            '20261025_0085 downgrade is forward-only after dual-lane WAIT '
            'recheck evidence exists; do not delete evidence rows, apply a '
            'forward migration instead'
        )
    op.drop_constraint(
        'uq_signal_decider_wait_recheck_target_symbol_agent',
        'signal_decider_wait_recheck_events',
        type_='unique',
    )
    op.create_unique_constraint(
        'uq_signal_decider_wait_recheck_target_symbol',
        'signal_decider_wait_recheck_events',
        ['target_trade_date', 'symbol'],
    )
