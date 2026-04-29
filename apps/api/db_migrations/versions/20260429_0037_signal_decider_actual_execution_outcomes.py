"""add actual signal decider execution outcomes

Revision ID: 20260429_0037
Revises: 20260429_0036
Create Date: 2026-04-29 14:05:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260429_0037'
down_revision = '20260429_0036'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {index['name'] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _table_exists(inspector, 'signal_decider_actual_execution_outcomes'):
        op.create_table(
            'signal_decider_actual_execution_outcomes',
            sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
            sa.Column('decision_snapshot_id', sa.Integer(), nullable=False),
            sa.Column('entry_plan_item_id', sa.Integer(), nullable=False),
            sa.Column('exit_plan_item_id', sa.Integer(), nullable=True),
            sa.Column('entry_order_request_id', sa.Integer(), nullable=True),
            sa.Column('entry_broker_order_row_id', sa.Integer(), nullable=True),
            sa.Column('exit_order_request_id', sa.Integer(), nullable=True),
            sa.Column('exit_broker_order_row_id', sa.Integer(), nullable=True),
            sa.Column('broker_name', sa.String(length=32), nullable=False, server_default='kis'),
            sa.Column('market_date', sa.Date(), nullable=False),
            sa.Column('source_market_date', sa.Date(), nullable=False),
            sa.Column('target_trade_date', sa.Date(), nullable=False),
            sa.Column('account_reference', sa.String(length=128), nullable=False),
            sa.Column('symbol', sa.String(length=32), nullable=False),
            sa.Column('policy_version', sa.String(length=64), nullable=False),
            sa.Column('entry_side', sa.String(length=16), nullable=False, server_default='buy'),
            sa.Column('exit_side', sa.String(length=16), nullable=False, server_default='sell'),
            sa.Column('lifecycle_status', sa.String(length=32), nullable=False, server_default='planned'),
            sa.Column('exit_reason', sa.String(length=64), nullable=True),
            sa.Column('entry_submitted_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('entry_filled_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('exit_submitted_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('exit_filled_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('entry_filled_quantity', sa.Numeric(20, 8), nullable=True),
            sa.Column('exit_filled_quantity', sa.Numeric(20, 8), nullable=True),
            sa.Column('closed_quantity', sa.Numeric(20, 8), nullable=True),
            sa.Column('open_quantity', sa.Numeric(20, 8), nullable=True),
            sa.Column('entry_average_fill_price', sa.Numeric(20, 8), nullable=True),
            sa.Column('exit_average_fill_price', sa.Numeric(20, 8), nullable=True),
            sa.Column('entry_notional_krw', sa.Numeric(20, 2), nullable=True),
            sa.Column('exit_notional_krw', sa.Numeric(20, 2), nullable=True),
            sa.Column('realized_pnl_krw', sa.Numeric(20, 2), nullable=True),
            sa.Column('realized_return_pct', sa.Numeric(12, 6), nullable=True),
            sa.Column('diagnostics_payload', sa.JSON(), nullable=True),
            sa.Column('computed_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
            sa.ForeignKeyConstraint(
                ['decision_snapshot_id'],
                ['signal_decider_decision_snapshots.id'],
                ondelete='CASCADE',
            ),
            sa.ForeignKeyConstraint(
                ['entry_plan_item_id'],
                ['signal_decider_execution_plan_items.id'],
                ondelete='CASCADE',
            ),
            sa.ForeignKeyConstraint(
                ['exit_plan_item_id'],
                ['signal_decider_execution_plan_items.id'],
                ondelete='SET NULL',
            ),
            sa.ForeignKeyConstraint(
                ['entry_order_request_id'],
                ['order_requests.id'],
                ondelete='SET NULL',
            ),
            sa.ForeignKeyConstraint(
                ['entry_broker_order_row_id'],
                ['broker_orders.id'],
                ondelete='SET NULL',
            ),
            sa.ForeignKeyConstraint(
                ['exit_order_request_id'],
                ['order_requests.id'],
                ondelete='SET NULL',
            ),
            sa.ForeignKeyConstraint(
                ['exit_broker_order_row_id'],
                ['broker_orders.id'],
                ondelete='SET NULL',
            ),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint(
                'entry_plan_item_id',
                name='uq_sdao_entry_plan',
            ),
        )

    inspector = sa.inspect(bind)
    existing_indexes = _index_names(inspector, 'signal_decider_actual_execution_outcomes')
    indexes = (
        (
            'ix_sdao_mkt_sym_status',
            ['market_date', 'symbol', 'lifecycle_status'],
        ),
        (
            'ix_sdao_acct_target_status',
            ['account_reference', 'target_trade_date', 'lifecycle_status'],
        ),
        (
            'ix_sdao_snapshot',
            ['decision_snapshot_id'],
        ),
        (
            'ix_sdao_symbol',
            ['symbol'],
        ),
        (
            'ix_sdao_policy',
            ['policy_version'],
        ),
        (
            'ix_sdao_lifecycle',
            ['lifecycle_status'],
        ),
        (
            'ix_sdao_exit_reason',
            ['exit_reason'],
        ),
        (
            'ix_sdao_exit_plan',
            ['exit_plan_item_id'],
        ),
        (
            'ix_sdao_entry_order_req',
            ['entry_order_request_id'],
        ),
        (
            'ix_sdao_entry_broker_order',
            ['entry_broker_order_row_id'],
        ),
        (
            'ix_sdao_exit_order_req',
            ['exit_order_request_id'],
        ),
        (
            'ix_sdao_exit_broker_order',
            ['exit_broker_order_row_id'],
        ),
    )
    for index_name, columns in indexes:
        if index_name not in existing_indexes:
            op.create_index(index_name, 'signal_decider_actual_execution_outcomes', columns, unique=False)
            existing_indexes.add(index_name)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, 'signal_decider_actual_execution_outcomes'):
        return
    op.drop_table('signal_decider_actual_execution_outcomes')
