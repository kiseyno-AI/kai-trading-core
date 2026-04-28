"""add KIS order reconciliation state columns

Revision ID: 20260428_0031
Revises: 20260428_0030
Create Date: 2026-04-28 18:40:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260428_0031'
down_revision = '20260428_0030'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('broker_orders', sa.Column('last_reconciled_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('broker_orders', sa.Column('last_reconcile_status', sa.String(length=32), nullable=True))
    op.add_column('broker_orders', sa.Column('last_reconcile_trigger', sa.String(length=32), nullable=True))
    op.add_column('broker_orders', sa.Column('last_reconcile_payload', sa.JSON(), nullable=True))
    op.create_index(
        'ix_broker_orders_last_reconciled_at',
        'broker_orders',
        ['last_reconciled_at'],
    )
    op.create_index(
        'ix_broker_orders_account_reconcile_status',
        'broker_orders',
        ['account_reference', 'last_reconcile_status', 'last_reconciled_at'],
    )


def downgrade() -> None:
    op.drop_index('ix_broker_orders_account_reconcile_status', table_name='broker_orders')
    op.drop_index('ix_broker_orders_last_reconciled_at', table_name='broker_orders')
    op.drop_column('broker_orders', 'last_reconcile_payload')
    op.drop_column('broker_orders', 'last_reconcile_trigger')
    op.drop_column('broker_orders', 'last_reconcile_status')
    op.drop_column('broker_orders', 'last_reconciled_at')
