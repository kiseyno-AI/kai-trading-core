"""Store SEC operating cash flow used by the US fundamental gate."""

from alembic import op
import sqlalchemy as sa


revision = '20260819_0071'
down_revision = '20260819_0070'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'us_equity_fundamental_snapshots',
        sa.Column('operating_cash_flow_usd', sa.Numeric(precision=28, scale=6), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('us_equity_fundamental_snapshots', 'operating_cash_flow_usd')
