"""preserve snapshot-owned signal decider rows when feedback is deleted

Revision ID: 20260428_0034
Revises: 20260428_0033
Create Date: 2026-04-28 23:10:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260428_0034'
down_revision = '20260428_0033'
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _feedback_fk(inspector: sa.Inspector, table_name: str) -> dict | None:
    for foreign_key in inspector.get_foreign_keys(table_name):
        if (
            foreign_key.get('referred_table') == 'agent_context_feedback'
            and foreign_key.get('constrained_columns') == ['feedback_id']
        ):
            return foreign_key
    return None


def _null_feedback_count(bind, table_name: str) -> int:
    table = sa.table(table_name, sa.column('feedback_id', sa.Integer()))
    return bind.execute(
        sa.select(sa.func.count()).select_from(table).where(table.c.feedback_id.is_(None))
    ).scalar_one()


def _ensure_feedback_fk(table_name: str, *, constraint_name: str, ondelete: str) -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, table_name):
        return
    foreign_key = _feedback_fk(inspector, table_name)
    current_ondelete = ((foreign_key or {}).get('options') or {}).get('ondelete')
    if foreign_key and current_ondelete != ondelete and foreign_key.get('name'):
        op.drop_constraint(foreign_key['name'], table_name, type_='foreignkey')
        foreign_key = None
    if foreign_key is None:
        op.create_foreign_key(
            constraint_name,
            table_name,
            'agent_context_feedback',
            ['feedback_id'],
            ['id'],
            ondelete=ondelete,
        )


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    for table_name, constraint_name in (
        ('signal_decider_realized_outcomes', 'fk_signal_decider_realized_outcomes_feedback_id'),
        ('signal_decider_evaluations', 'fk_signal_decider_evaluations_feedback_id'),
    ):
        if not _table_exists(inspector, table_name):
            continue
        op.alter_column(table_name, 'feedback_id', existing_type=sa.Integer(), nullable=True)
        _ensure_feedback_fk(table_name, constraint_name=constraint_name, ondelete='SET NULL')
        inspector = sa.inspect(bind)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    for table_name, constraint_name in (
        ('signal_decider_realized_outcomes', 'fk_signal_decider_realized_outcomes_feedback_id'),
        ('signal_decider_evaluations', 'fk_signal_decider_evaluations_feedback_id'),
    ):
        if not _table_exists(inspector, table_name):
            continue
        foreign_key = _feedback_fk(inspector, table_name)
        current_ondelete = ((foreign_key or {}).get('options') or {}).get('ondelete')
        if foreign_key and current_ondelete != 'CASCADE' and foreign_key.get('name'):
            op.drop_constraint(foreign_key['name'], table_name, type_='foreignkey')
            foreign_key = None
        if foreign_key is None:
            op.create_foreign_key(
                constraint_name,
                table_name,
                'agent_context_feedback',
                ['feedback_id'],
                ['id'],
                ondelete='CASCADE',
            )
        if _null_feedback_count(bind, table_name) == 0:
            op.alter_column(table_name, 'feedback_id', existing_type=sa.Integer(), nullable=False)
        inspector = sa.inspect(bind)
