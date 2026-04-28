"""add KIS order persistence tables

Revision ID: 20260428_0030
Revises: 20260427_0029
Create Date: 2026-04-28 15:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = '20260428_0030'
down_revision = '20260427_0029'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'order_requests',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('broker_name', sa.String(length=32), nullable=False, server_default='kis'),
        sa.Column('client_order_id', sa.String(length=128), nullable=False),
        sa.Column('source', sa.String(length=32), nullable=False, server_default='manual'),
        sa.Column('account_reference', sa.String(length=128), nullable=False),
        sa.Column('symbol', sa.String(length=32), nullable=False),
        sa.Column('side', sa.String(length=16), nullable=False),
        sa.Column('order_type', sa.String(length=32), nullable=False),
        sa.Column('requested_quantity', sa.Numeric(20, 8), nullable=False),
        sa.Column('requested_price', sa.Numeric(20, 8), nullable=True),
        sa.Column('time_in_force', sa.String(length=32), nullable=True),
        sa.Column('strategy_context_payload', sa.JSON(), nullable=True),
        sa.Column('request_payload', sa.JSON(), nullable=False),
        sa.Column('validation_status', sa.String(length=32), nullable=False, server_default='pending'),
        sa.Column('validation_payload', sa.JSON(), nullable=True),
        sa.Column('submission_status', sa.String(length=32), nullable=False, server_default='pending_submit'),
        sa.Column('submission_payload', sa.JSON(), nullable=True),
        sa.Column('submission_attempts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_submission_error', sa.Text(), nullable=True),
        sa.Column('submitted_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'broker_name',
            'account_reference',
            'client_order_id',
            name='uq_order_requests_broker_account_client_order_id',
        ),
    )
    op.create_index(
        'ix_order_requests_account_submission_created',
        'order_requests',
        ['account_reference', 'submission_status', 'created_at'],
    )
    op.create_index(
        'ix_order_requests_account_validation_created',
        'order_requests',
        ['account_reference', 'validation_status', 'created_at'],
    )
    op.create_index(
        'ix_order_requests_symbol_created',
        'order_requests',
        ['symbol', 'created_at'],
    )

    op.create_table(
        'broker_orders',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('order_request_id', sa.Integer(), nullable=True),
        sa.Column('broker_name', sa.String(length=32), nullable=False, server_default='kis'),
        sa.Column('broker_order_id', sa.String(length=128), nullable=True),
        sa.Column('client_order_id', sa.String(length=128), nullable=False),
        sa.Column('account_reference', sa.String(length=128), nullable=False),
        sa.Column('symbol', sa.String(length=32), nullable=False),
        sa.Column('side', sa.String(length=16), nullable=False),
        sa.Column('order_type', sa.String(length=32), nullable=False),
        sa.Column('requested_quantity', sa.Numeric(20, 8), nullable=False),
        sa.Column('requested_price', sa.Numeric(20, 8), nullable=True),
        sa.Column('accepted_quantity', sa.Numeric(20, 8), nullable=True),
        sa.Column('accepted_price', sa.Numeric(20, 8), nullable=True),
        sa.Column('filled_quantity', sa.Numeric(20, 8), nullable=False, server_default='0'),
        sa.Column('remaining_quantity', sa.Numeric(20, 8), nullable=True),
        sa.Column('average_fill_price', sa.Numeric(20, 8), nullable=True),
        sa.Column('time_in_force', sa.String(length=32), nullable=True),
        sa.Column('current_status', sa.String(length=32), nullable=False, server_default='pending_submit'),
        sa.Column('status_reason', sa.Text(), nullable=True),
        sa.Column('first_submitted_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_broker_update_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_fill_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('raw_latest_payload', sa.JSON(), nullable=True),
        sa.Column('snapshot_payload', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['order_request_id'], ['order_requests.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'broker_name',
            'account_reference',
            'client_order_id',
            name='uq_broker_orders_broker_account_client_order_id',
        ),
        sa.UniqueConstraint(
            'broker_name',
            'account_reference',
            'broker_order_id',
            name='uq_broker_orders_broker_account_broker_order_id',
        ),
    )
    op.create_index(
        'ix_broker_orders_account_status_updated',
        'broker_orders',
        ['account_reference', 'current_status', 'updated_at'],
    )
    op.create_index(
        'ix_broker_orders_symbol_status_updated',
        'broker_orders',
        ['symbol', 'current_status', 'updated_at'],
    )
    op.create_index(
        'ix_broker_orders_account_updated',
        'broker_orders',
        ['account_reference', 'updated_at'],
    )
    op.create_index(
        'ix_broker_orders_last_broker_update_at',
        'broker_orders',
        ['last_broker_update_at'],
    )
    op.create_index(
        'ix_broker_orders_order_request_id',
        'broker_orders',
        ['order_request_id'],
    )

    op.create_table(
        'broker_order_events',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('broker_order_row_id', sa.Integer(), nullable=False),
        sa.Column('order_request_id', sa.Integer(), nullable=True),
        sa.Column('broker_name', sa.String(length=32), nullable=False, server_default='kis'),
        sa.Column('broker_order_id', sa.String(length=128), nullable=True),
        sa.Column('client_order_id', sa.String(length=128), nullable=False),
        sa.Column('account_reference', sa.String(length=128), nullable=False),
        sa.Column('symbol', sa.String(length=32), nullable=False),
        sa.Column('event_type', sa.String(length=64), nullable=False),
        sa.Column('event_sequence', sa.String(length=64), nullable=True),
        sa.Column('broker_event_id', sa.String(length=128), nullable=True),
        sa.Column('resulting_status', sa.String(length=32), nullable=True),
        sa.Column('event_occurred_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('quantity_delta', sa.Numeric(20, 8), nullable=True),
        sa.Column('cumulative_filled_quantity', sa.Numeric(20, 8), nullable=True),
        sa.Column('fill_price', sa.Numeric(20, 8), nullable=True),
        sa.Column('source', sa.String(length=32), nullable=False),
        sa.Column('event_dedup_key', sa.String(length=255), nullable=False),
        sa.Column('raw_payload', sa.JSON(), nullable=False),
        sa.Column('normalized_payload', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['broker_order_row_id'], ['broker_orders.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['order_request_id'], ['order_requests.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'broker_order_row_id',
            'event_dedup_key',
            name='uq_broker_order_events_broker_order_dedup_key',
        ),
    )
    op.create_index(
        'ix_broker_order_events_order_timeline',
        'broker_order_events',
        ['broker_order_row_id', 'event_occurred_at', 'id'],
    )
    op.create_index(
        'ix_broker_order_events_account_recency',
        'broker_order_events',
        ['account_reference', 'event_occurred_at'],
    )
    op.create_index(
        'ix_broker_order_events_symbol_recency',
        'broker_order_events',
        ['symbol', 'event_occurred_at'],
    )
    op.create_index(
        'ix_broker_order_events_client_order_recency',
        'broker_order_events',
        ['client_order_id', 'event_occurred_at'],
    )
    op.create_index(
        'ix_broker_order_events_broker_order_recency',
        'broker_order_events',
        ['broker_order_id', 'event_occurred_at'],
    )
    op.create_index(
        'ix_broker_order_events_status_recency',
        'broker_order_events',
        ['resulting_status', 'event_occurred_at'],
    )
    op.create_index(
        'ix_broker_order_events_order_request_id',
        'broker_order_events',
        ['order_request_id'],
    )


def downgrade() -> None:
    op.drop_index('ix_broker_order_events_order_request_id', table_name='broker_order_events')
    op.drop_index('ix_broker_order_events_status_recency', table_name='broker_order_events')
    op.drop_index('ix_broker_order_events_broker_order_recency', table_name='broker_order_events')
    op.drop_index('ix_broker_order_events_client_order_recency', table_name='broker_order_events')
    op.drop_index('ix_broker_order_events_symbol_recency', table_name='broker_order_events')
    op.drop_index('ix_broker_order_events_account_recency', table_name='broker_order_events')
    op.drop_index('ix_broker_order_events_order_timeline', table_name='broker_order_events')
    op.drop_table('broker_order_events')

    op.drop_index('ix_broker_orders_order_request_id', table_name='broker_orders')
    op.drop_index('ix_broker_orders_last_broker_update_at', table_name='broker_orders')
    op.drop_index('ix_broker_orders_account_updated', table_name='broker_orders')
    op.drop_index('ix_broker_orders_symbol_status_updated', table_name='broker_orders')
    op.drop_index('ix_broker_orders_account_status_updated', table_name='broker_orders')
    op.drop_table('broker_orders')

    op.drop_index('ix_order_requests_symbol_created', table_name='order_requests')
    op.drop_index('ix_order_requests_account_validation_created', table_name='order_requests')
    op.drop_index('ix_order_requests_account_submission_created', table_name='order_requests')
    op.drop_table('order_requests')
