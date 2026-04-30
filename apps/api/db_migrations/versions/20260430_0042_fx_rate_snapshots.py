"""fx rate snapshots

Revision ID: 20260430_0042
Revises: 20260430_0041
Create Date: 2026-04-30 14:55:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260430_0042'
down_revision = '20260430_0041'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if _table_exists(inspector, 'current_fx_rate_snapshots'):
        return

    op.create_table(
        'current_fx_rate_snapshots',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('provider', sa.String(length=32), nullable=False),
        sa.Column('currency_pair', sa.String(length=32), nullable=False),
        sa.Column('base_currency', sa.String(length=16), nullable=False),
        sa.Column('quote_currency', sa.String(length=16), nullable=False),
        sa.Column('exchange_rate', sa.Numeric(precision=20, scale=6), nullable=False),
        sa.Column('requested_date', sa.Date(), nullable=False),
        sa.Column('effective_date', sa.Date(), nullable=False),
        sa.Column('fallback_applied', sa.Boolean(), nullable=False),
        sa.Column('snapshot_status', sa.String(length=32), nullable=False),
        sa.Column('stale_reason', sa.String(length=64), nullable=True),
        sa.Column('detail', sa.Text(), nullable=True),
        sa.Column('payload', sa.JSON(), nullable=True),
        sa.Column('captured_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('provider', 'currency_pair', name='uq_current_fx_rate_snapshots_provider_pair'),
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table_exists(inspector, 'current_fx_rate_snapshots'):
        op.drop_table('current_fx_rate_snapshots')
