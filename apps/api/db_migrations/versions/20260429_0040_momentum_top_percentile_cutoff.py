"""add explicit momentum top-percentile cutoff fields

Revision ID: 20260429_0040
Revises: 20260429_0039
Create Date: 2026-04-29 23:50:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260429_0040'
down_revision = '20260429_0039'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'daily_momentum_factor_results',
        sa.Column(
            'momentum_percentile_threshold',
            sa.Numeric(12, 6),
            nullable=False,
            server_default='0.8',
        ),
    )
    op.add_column(
        'daily_momentum_factor_results',
        sa.Column(
            'passes_momentum_top_percentile',
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.execute(
        """
        UPDATE daily_momentum_factor_results
        SET momentum_percentile_threshold = 0.8,
            passes_momentum_top_percentile = CASE
                WHEN momentum_percentile IS NOT NULL AND momentum_percentile >= 0.8 THEN TRUE
                ELSE FALSE
            END
        """
    )
    op.create_index(
        'ix_daily_momentum_factor_results_market_top_percentile',
        'daily_momentum_factor_results',
        ['market_date', 'passes_momentum_top_percentile'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_daily_momentum_factor_results_market_top_percentile',
        table_name='daily_momentum_factor_results',
    )
    op.drop_column('daily_momentum_factor_results', 'passes_momentum_top_percentile')
    op.drop_column('daily_momentum_factor_results', 'momentum_percentile_threshold')
