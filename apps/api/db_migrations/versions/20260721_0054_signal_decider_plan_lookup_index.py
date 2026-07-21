"""add signal decider plan lookup indexes

Revision ID: 20260721_0054
Revises: 20260704_0053
Create Date: 2026-07-21 17:12:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260721_0054'
down_revision = '20260704_0053'
branch_labels = None
depends_on = None


INDEX_NAME = 'ix_signal_decider_plan_next_open_lookup'
TABLE_NAME = 'signal_decider_execution_plan_items'

INDEXES = (
    (
        INDEX_NAME,
        [
            'execution_window',
            'target_trade_date',
            'account_reference',
            'decision_agent_name',
            'symbol',
            'id',
        ],
        None,
    ),
    (
        'ix_signal_decider_plan_next_open_status_lookup',
        [
            'execution_window',
            'target_trade_date',
            'eligibility_status',
            'execution_status',
            'account_reference',
            'decision_agent_name',
            'symbol',
        ],
        None,
    ),
    (
        'ix_signal_decider_plan_source_block_lookup',
        [
            'execution_window',
            'target_trade_date',
            'source_market_date',
            'execution_status',
            'decision_agent_name',
        ],
        None,
    ),
    (
        'ix_signal_decider_plan_history_submission_lookup',
        [
            'execution_window',
            'decision_agent_name',
            'target_trade_date',
            'submitted_at',
            'id',
        ],
        sa.text('submission_payload is not null'),
    ),
    (
        'ix_signal_decider_plan_account_discovery_lookup',
        [
            'execution_window',
            'decision_agent_name',
            'target_trade_date',
            'submitted_at',
            'id',
        ],
        None,
    ),
    (
        'ix_signal_decider_plan_linked_source_lookup',
        [
            'execution_window',
            'decision_agent_name',
            'account_reference',
            'symbol',
            'planned_side',
            'target_trade_date',
            'submitted_at',
            'id',
        ],
        None,
    ),
    (
        'ix_signal_decider_plan_intraday_watch_lookup',
        [
            'execution_window',
            'target_trade_date',
            'account_reference',
            'symbol',
            'id',
        ],
        None,
    ),
)

def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return any(index['name'] == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, TABLE_NAME):
        return
    for index_name, columns, where_clause in INDEXES:
        if _index_exists(inspector, TABLE_NAME, index_name):
            continue
        kwargs = {'postgresql_where': where_clause} if where_clause is not None else {}
        op.create_index(index_name, TABLE_NAME, columns, unique=False, **kwargs)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, TABLE_NAME):
        return
    for index_name, _, _ in reversed(INDEXES):
        if _index_exists(inspector, TABLE_NAME, index_name):
            op.drop_index(index_name, table_name=TABLE_NAME)
