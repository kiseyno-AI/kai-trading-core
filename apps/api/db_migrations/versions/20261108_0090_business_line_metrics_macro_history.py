"""business-line daily metrics and macro series release history

Revision ID: 20261108_0090
Revises: 20261108_0089
Create Date: 2026-11-08 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20261108_0090'
down_revision = '20261108_0089'
branch_labels = None
depends_on = None


def _timestamps() -> list[sa.Column]:
    return [
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    ]


def upgrade() -> None:
    op.create_table(
        'macro_series_history',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('series_id', sa.String(length=64), nullable=False),
        sa.Column('observation_date', sa.Date(), nullable=False),
        sa.Column('value', sa.Numeric(24, 8), nullable=False),
        sa.Column('released_on', sa.Date(), nullable=False),
        sa.Column('release_basis', sa.String(length=32), nullable=False),
        sa.Column('fetched_at', sa.DateTime(timezone=True), nullable=False),
        *_timestamps(),
        sa.UniqueConstraint('source', 'series_id', 'observation_date', name='uq_macro_series_history_obs'),
    )
    op.create_index('ix_macro_series_history_series_released', 'macro_series_history', ['series_id', 'released_on'])
    op.create_table(
        'business_line_daily_metrics',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('trade_date', sa.Date(), nullable=False),
        sa.Column('business_line', sa.String(length=64), nullable=False),
        sa.Column('symbols', sa.Integer(), nullable=False),
        sa.Column('ew_return', sa.Numeric(18, 10), nullable=False),
        sa.Column('market_ew_return', sa.Numeric(18, 10), nullable=False),
        sa.Column('excess_return', sa.Numeric(18, 10), nullable=False),
        sa.Column('advancers_ratio', sa.Numeric(10, 6), nullable=False),
        sa.Column('turnover', sa.Numeric(24, 2), nullable=True),
        sa.Column('turnover_share', sa.Numeric(10, 6), nullable=True),
        sa.Column('classification_basis', sa.String(length=32), nullable=False),
        *_timestamps(),
        sa.UniqueConstraint('trade_date', 'business_line', name='uq_business_line_daily_metrics_day_line'),
    )
    op.create_index('ix_business_line_daily_metrics_line_day', 'business_line_daily_metrics', ['business_line', 'trade_date'])


def downgrade() -> None:
    op.drop_table('business_line_daily_metrics')
    op.drop_table('macro_series_history')
