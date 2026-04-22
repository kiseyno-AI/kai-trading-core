"""add pykrx fundamentals columns to daily price bars

Revision ID: 20260422_0017
Revises: 20260421_0016
Create Date: 2026-04-22 07:58:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260422_0017'
down_revision = '20260421_0016'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('daily_price_bars', sa.Column('per', sa.Numeric(18, 6), nullable=True))
    op.add_column('daily_price_bars', sa.Column('pbr', sa.Numeric(18, 6), nullable=True))
    op.add_column('daily_price_bars', sa.Column('bps', sa.Numeric(18, 6), nullable=True))
    op.add_column('daily_price_bars', sa.Column('eps', sa.Numeric(18, 6), nullable=True))
    op.add_column('daily_price_bars', sa.Column('dividend_yield', sa.Numeric(18, 6), nullable=True))
    op.add_column('daily_price_bars', sa.Column('dps', sa.Numeric(18, 6), nullable=True))


def downgrade() -> None:
    op.drop_column('daily_price_bars', 'dps')
    op.drop_column('daily_price_bars', 'dividend_yield')
    op.drop_column('daily_price_bars', 'eps')
    op.drop_column('daily_price_bars', 'bps')
    op.drop_column('daily_price_bars', 'pbr')
    op.drop_column('daily_price_bars', 'per')
