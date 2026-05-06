"""add decision agent identity to exec plan rows

Revision ID: 20260506_0045
Revises: 20260506_0044
Create Date: 2026-05-06 22:15:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260506_0045'
down_revision = '20260506_0044'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {column['name'] for column in inspector.get_columns(table_name)}


def _index_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {index['name'] for index in inspector.get_indexes(table_name)}


def _unique_names(inspector: sa.Inspector, table_name: str) -> set[str]:
    return {constraint['name'] for constraint in inspector.get_unique_constraints(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_name = 'signal_decider_execution_plan_items'
    if not _table_exists(inspector, table_name):
        return

    existing_columns = _column_names(inspector, table_name)
    if 'decision_agent_name' not in existing_columns:
        op.add_column(
            table_name,
            sa.Column('decision_agent_name', sa.String(length=64), nullable=True, server_default='signal-decider'),
        )
        metadata = sa.MetaData()
        plan_table = sa.Table(table_name, metadata, autoload_with=bind)
        snapshot_table = sa.Table('signal_decider_decision_snapshots', metadata, autoload_with=bind)
        bind.execute(
            plan_table.update().values(
                decision_agent_name=sa.select(snapshot_table.c.agent_name)
                .where(snapshot_table.c.id == plan_table.c.decision_snapshot_id)
                .scalar_subquery()
            )
        )
        bind.execute(
            plan_table.update()
            .where(plan_table.c.decision_agent_name.is_(None))
            .values(decision_agent_name='signal-decider')
        )
        op.alter_column(
            table_name,
            'decision_agent_name',
            existing_type=sa.String(length=64),
            nullable=False,
            server_default='signal-decider',
        )

    inspector = sa.inspect(bind)
    unique_names = _unique_names(inspector, table_name)
    if 'uq_signal_decider_execution_plan_items_scope' in unique_names:
        op.drop_constraint('uq_signal_decider_execution_plan_items_scope', table_name, type_='unique')
    op.create_unique_constraint(
        'uq_signal_decider_execution_plan_items_scope',
        table_name,
        ['execution_window', 'target_trade_date', 'account_reference', 'symbol', 'decision_agent_name'],
    )

    inspector = sa.inspect(bind)
    existing_indexes = _index_names(inspector, table_name)
    decision_agent_index = op.f('ix_signal_decider_execution_plan_items_decision_agent_name')
    if decision_agent_index not in existing_indexes:
        op.create_index(decision_agent_index, table_name, ['decision_agent_name'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_name = 'signal_decider_execution_plan_items'
    if not _table_exists(inspector, table_name):
        return

    existing_indexes = _index_names(inspector, table_name)
    decision_agent_index = op.f('ix_signal_decider_execution_plan_items_decision_agent_name')
    if decision_agent_index in existing_indexes:
        op.drop_index(decision_agent_index, table_name=table_name)

    unique_names = _unique_names(inspector, table_name)
    if 'uq_signal_decider_execution_plan_items_scope' in unique_names:
        op.drop_constraint('uq_signal_decider_execution_plan_items_scope', table_name, type_='unique')
    op.create_unique_constraint(
        'uq_signal_decider_execution_plan_items_scope',
        table_name,
        ['execution_window', 'target_trade_date', 'account_reference', 'symbol'],
    )

    existing_columns = _column_names(inspector, table_name)
    if 'decision_agent_name' in existing_columns:
        op.drop_column(table_name, 'decision_agent_name')
