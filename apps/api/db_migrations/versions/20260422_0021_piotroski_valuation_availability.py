"""add piotroski valuation availability fields

Revision ID: 20260422_0021
Revises: 20260422_0020
Create Date: 2026-04-22 22:31:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260422_0021'
down_revision = '20260422_0020'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'daily_piotroski_filter_results',
        sa.Column('valuation_available', sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.add_column(
        'daily_piotroski_filter_results',
        sa.Column('valuation_source_status', sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('daily_piotroski_filter_results', 'valuation_source_status')
    op.drop_column('daily_piotroski_filter_results', 'valuation_available')
