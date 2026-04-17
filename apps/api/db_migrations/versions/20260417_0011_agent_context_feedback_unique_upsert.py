"""enforce unique feedback rows per market-date symbol agent

Revision ID: 20260417_0011
Revises: 20260415_0009
Create Date: 2026-04-17 11:05:00
"""

from alembic import op


revision = '20260417_0011'
down_revision = '20260415_0009'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DELETE FROM agent_context_feedback a
        USING agent_context_feedback b
        WHERE a.id < b.id
          AND a.market_date = b.market_date
          AND a.symbol = b.symbol
          AND a.agent_name = b.agent_name
        """
    )
    op.create_unique_constraint(
        'uq_agent_context_feedback_market_symbol_agent',
        'agent_context_feedback',
        ['market_date', 'symbol', 'agent_name'],
    )


def downgrade() -> None:
    op.drop_constraint(
        'uq_agent_context_feedback_market_symbol_agent',
        'agent_context_feedback',
        type_='unique',
    )
