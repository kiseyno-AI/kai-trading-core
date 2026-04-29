"""add durable signal decider execution plan items

Revision ID: 20260429_0036
Revises: 20260428_0035
Create Date: 2026-04-29 09:20:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260429_0036'
down_revision = '20260428_0035'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {index['name'] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'signal_decider_execution_plan_items'):
        op.create_table(
            'signal_decider_execution_plan_items',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('execution_window', sa.String(length=32), nullable=False, server_default='next_open'),
            sa.Column('decision_snapshot_id', sa.Integer(), nullable=False),
            sa.Column('order_request_id', sa.Integer(), nullable=True),
            sa.Column('broker_order_row_id', sa.Integer(), nullable=True),
            sa.Column('broker_name', sa.String(length=32), nullable=False, server_default='kis'),
            sa.Column('target_trade_date', sa.Date(), nullable=False),
            sa.Column('source_market_date', sa.Date(), nullable=False),
            sa.Column('account_reference', sa.String(length=128), nullable=False),
            sa.Column('account_label', sa.String(length=64), nullable=True),
            sa.Column('symbol', sa.String(length=32), nullable=False),
            sa.Column('policy_version', sa.String(length=64), nullable=False),
            sa.Column('planned_side', sa.String(length=16), nullable=False, server_default='buy'),
            sa.Column('planned_order_type', sa.String(length=32), nullable=False, server_default='market'),
            sa.Column('planned_time_in_force', sa.String(length=32), nullable=False, server_default='day'),
            sa.Column('allocation_budget_krw', sa.Numeric(20, 2), nullable=False),
            sa.Column('estimated_reference_price', sa.BigInteger(), nullable=True),
            sa.Column('planned_quantity', sa.Numeric(20, 8), nullable=True),
            sa.Column('estimated_notional_krw', sa.Numeric(20, 2), nullable=True),
            sa.Column('eligibility_status', sa.String(length=32), nullable=False, server_default='blocked'),
            sa.Column('execution_status', sa.String(length=32), nullable=False, server_default='planned'),
            sa.Column('client_order_id', sa.String(length=128), nullable=True),
            sa.Column('validation_payload', sa.JSON(), nullable=True),
            sa.Column('traceability_payload', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
            sa.Column('submission_payload', sa.JSON(), nullable=True),
            sa.Column('first_ready_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('last_evaluated_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('submitted_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.ForeignKeyConstraint(
                ['decision_snapshot_id'],
                ['signal_decider_decision_snapshots.id'],
                ondelete='CASCADE',
            ),
            sa.ForeignKeyConstraint(
                ['order_request_id'],
                ['order_requests.id'],
                ondelete='SET NULL',
            ),
            sa.ForeignKeyConstraint(
                ['broker_order_row_id'],
                ['broker_orders.id'],
                ondelete='SET NULL',
            ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint(
                'execution_window',
                'target_trade_date',
                'account_reference',
                'symbol',
                name='uq_signal_decider_execution_plan_items_scope',
            ),
        )

    inspector = sa.inspect(bind)
    existing_indexes = _index_names(inspector, 'signal_decider_execution_plan_items')
    indexes = (
        (
            'ix_signal_decider_execution_plan_items_target_status',
            ['target_trade_date', 'eligibility_status', 'execution_status'],
        ),
        (
            'ix_signal_decider_execution_plan_items_target_account',
            ['target_trade_date', 'account_reference'],
        ),
        (
            'ix_signal_decider_execution_plan_items_snapshot',
            ['decision_snapshot_id'],
        ),
        (
            'ix_signal_decider_execution_plan_items_order_request',
            ['order_request_id'],
        ),
        (
            'ix_signal_decider_execution_plan_items_broker_order',
            ['broker_order_row_id'],
        ),
    )
    for index_name, columns in indexes:
        if index_name not in existing_indexes:
            op.create_index(index_name, 'signal_decider_execution_plan_items', columns, unique=False)
    for column_name in (
        'execution_window',
        'decision_snapshot_id',
        'target_trade_date',
        'source_market_date',
        'account_reference',
        'symbol',
        'policy_version',
        'eligibility_status',
        'execution_status',
        'client_order_id',
        'last_evaluated_at',
        'submitted_at',
    ):
        default_name = op.f(f'ix_signal_decider_execution_plan_items_{column_name}')
        if default_name not in existing_indexes:
            op.create_index(default_name, 'signal_decider_execution_plan_items', [column_name], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, 'signal_decider_execution_plan_items'):
        return
    op.drop_table('signal_decider_execution_plan_items')
