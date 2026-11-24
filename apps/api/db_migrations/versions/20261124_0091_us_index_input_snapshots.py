"""preserve immutable US index input snapshots

Revision ID: 20261124_0091
Revises: 20261108_0090
Create Date: 2026-11-24 00:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20261124_0091'
down_revision = '20261108_0090'
branch_labels = None
depends_on = None

TABLE_NAME = 'us_index_input_snapshots'
INDEX_NAME = 'ix_us_index_input_snapshots_index_session_collected'
IMMUTABLE_FUNCTION_NAME = 'reject_us_index_input_snapshot_mutation'
IMMUTABLE_TRIGGER_NAME = 'trg_us_index_input_snapshots_immutable'
IMMUTABLE_TRUNCATE_TRIGGER_NAME = 'trg_us_index_input_snapshots_truncate_immutable'


def _install_postgresql_immutable_guard() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        return
    op.execute(sa.text(f'''
        CREATE FUNCTION {IMMUTABLE_FUNCTION_NAME}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            RAISE EXCEPTION 'rows in {TABLE_NAME} are immutable';
        END;
        $function$
    '''))
    op.execute(sa.text(f'''
        CREATE TRIGGER {IMMUTABLE_TRIGGER_NAME}
        BEFORE UPDATE OR DELETE ON {TABLE_NAME}
        FOR EACH ROW
        EXECUTE FUNCTION {IMMUTABLE_FUNCTION_NAME}()
    '''))
    op.execute(sa.text(f'''
        CREATE TRIGGER {IMMUTABLE_TRUNCATE_TRIGGER_NAME}
        BEFORE TRUNCATE ON {TABLE_NAME}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {IMMUTABLE_FUNCTION_NAME}()
    '''))


def _drop_postgresql_immutable_guard() -> None:
    if op.get_bind().dialect.name != 'postgresql':
        return
    op.execute(sa.text(f'DROP TRIGGER IF EXISTS {IMMUTABLE_TRUNCATE_TRIGGER_NAME} ON {TABLE_NAME}'))
    op.execute(sa.text(f'DROP TRIGGER IF EXISTS {IMMUTABLE_TRIGGER_NAME} ON {TABLE_NAME}'))
    op.execute(sa.text(f'DROP FUNCTION IF EXISTS {IMMUTABLE_FUNCTION_NAME}()'))


def upgrade() -> None:
    op.create_table(
        TABLE_NAME,
        sa.Column('snapshot_id', sa.String(length=64), primary_key=True),
        sa.Column('index_code', sa.String(length=16), nullable=False),
        sa.Column('session_date', sa.Date(), nullable=False),
        sa.Column('as_of', sa.DateTime(timezone=True), nullable=False),
        sa.Column('collected_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('snapshot_json', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            'collected_at >= as_of', name='ck_us_index_input_snapshots_capture_order',
        ),
    )
    op.create_index(INDEX_NAME, TABLE_NAME, ['index_code', 'session_date', 'collected_at'])
    _install_postgresql_immutable_guard()


def downgrade() -> None:
    _drop_postgresql_immutable_guard()
    op.drop_index(INDEX_NAME, table_name=TABLE_NAME)
    op.drop_table(TABLE_NAME)
