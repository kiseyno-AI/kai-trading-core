"""enforce one active signal-decider policy

Revision ID: 20260906_0076
Revises: 20260820_0075
Create Date: 2026-09-06 14:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260906_0076'
down_revision = '20260820_0075'
branch_labels = None
depends_on = None

TABLE_NAME = 'signal_decider_policy_versions'
INDEX_NAME = 'uq_signal_decider_policy_versions_one_active'


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return any(index.get('name') == index_name for index in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _table_exists(inspector, TABLE_NAME):
        return

    policy_versions = sa.Table(TABLE_NAME, sa.MetaData(), autoload_with=bind)
    active_rows = list(
        bind.execute(
            sa.select(policy_versions.c.id, policy_versions.c.policy_version)
            .where(policy_versions.c.status == 'active')
            .order_by(policy_versions.c.id.asc())
        ).all()
    )
    if len(active_rows) > 1:
        active_versions = ', '.join(
            f'{int(row.id)}:{row.policy_version}' for row in active_rows
        )
        raise RuntimeError(
            'cannot enforce a single active signal-decider policy while multiple active '
            'rows exist; resolve the policy registry conflict before retrying the migration '
            f'(active rows: {active_versions})'
        )

    inspector = sa.inspect(bind)
    if not _index_exists(inspector, TABLE_NAME, INDEX_NAME):
        op.create_index(
            INDEX_NAME,
            TABLE_NAME,
            ['status'],
            unique=True,
            postgresql_where=sa.text("status = 'active'"),
            sqlite_where=sa.text("status = 'active'"),
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table_exists(inspector, TABLE_NAME) and _index_exists(
        inspector,
        TABLE_NAME,
        INDEX_NAME,
    ):
        op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
