"""align signal-decider policy references with the policy registry

Revision ID: 20260907_0080
Revises: 20260906_0079
Create Date: 2026-09-07 10:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260907_0080'
down_revision = '20260906_0079'
branch_labels = None
depends_on = None

REGISTRY_POLICY_VERSION_LENGTH = 128
LEGACY_POLICY_VERSION_LENGTH = 64
POLICY_VERSION_COLUMNS = (
    ('signal_decider_decision_snapshots', 'policy_version'),
    ('signal_decider_realized_outcomes', 'policy_version'),
    ('signal_decider_evaluations', 'policy_version'),
    ('signal_decider_calendar_review_items', 'policy_version'),
    ('signal_decider_policy_candidates', 'base_policy_version'),
    ('signal_decider_execution_plan_items', 'policy_version'),
    ('signal_decider_actual_execution_outcomes', 'policy_version'),
)


def _column(inspector: sa.Inspector, table_name: str, column_name: str) -> dict:
    if table_name not in inspector.get_table_names():
        raise RuntimeError(f'required signal-decider table is missing: {table_name}')
    for column in inspector.get_columns(table_name):
        if column['name'] == column_name:
            return column
    raise RuntimeError(f'required signal-decider column is missing: {table_name}.{column_name}')


def _alter_policy_version_columns(*, source_length: int, target_length: int) -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table_name, column_name in POLICY_VERSION_COLUMNS:
        column = _column(inspector, table_name, column_name)
        current_length = getattr(column['type'], 'length', None)
        if current_length == target_length:
            continue
        if current_length != source_length:
            raise RuntimeError(
                f'unexpected length for {table_name}.{column_name}: '
                f'expected {source_length}, found {current_length}'
            )
        op.alter_column(
            table_name,
            column_name,
            existing_type=sa.String(length=source_length),
            type_=sa.String(length=target_length),
            existing_nullable=bool(column['nullable']),
        )


def _ensure_legacy_width_is_safe() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table_name, column_name in POLICY_VERSION_COLUMNS:
        _column(inspector, table_name, column_name)
        table = sa.table(table_name, sa.column(column_name, sa.String()))
        longest_value = bind.execute(
            sa.select(sa.func.max(sa.func.char_length(table.c[column_name])))
        ).scalar_one_or_none()
        if longest_value is not None and longest_value > LEGACY_POLICY_VERSION_LENGTH:
            raise RuntimeError(
                f'cannot narrow {table_name}.{column_name} to '
                f'{LEGACY_POLICY_VERSION_LENGTH}: existing value length is {longest_value}'
            )


def upgrade() -> None:
    _alter_policy_version_columns(
        source_length=LEGACY_POLICY_VERSION_LENGTH,
        target_length=REGISTRY_POLICY_VERSION_LENGTH,
    )


def downgrade() -> None:
    _ensure_legacy_width_is_safe()
    _alter_policy_version_columns(
        source_length=REGISTRY_POLICY_VERSION_LENGTH,
        target_length=LEGACY_POLICY_VERSION_LENGTH,
    )
