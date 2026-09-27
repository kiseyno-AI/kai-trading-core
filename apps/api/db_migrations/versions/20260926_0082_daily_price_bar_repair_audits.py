"""preserve immutable evidence for KIS final-bar repairs

Revision ID: 20260926_0082
Revises: 20260911_0081
Create Date: 2026-09-26 19:00:00
"""

from alembic import op
import sqlalchemy as sa


revision = '20260926_0082'
down_revision = '20260911_0081'
branch_labels = None
depends_on = None


BATCH_TABLE_NAME = 'daily_price_bar_repair_batches'
TABLE_NAME = 'daily_price_bar_repair_audits'
IMMUTABLE_FUNCTION_NAME = 'reject_daily_price_bar_repair_audit_mutation'
IMMUTABLE_TRIGGER_NAME = 'trg_daily_price_bar_repair_audits_immutable'
IMMUTABLE_TRUNCATE_TRIGGER_NAME = (
    'trg_daily_price_bar_repair_audits_truncate_immutable'
)
PRICE_BAR_ORIGIN_FK_NAME = (
    'fk_daily_price_bars_repair_origin_eod_run_id_eod_run'
)


def _install_postgresql_immutable_guard() -> None:
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        return
    op.execute(
        sa.text(
            f'''
            CREATE FUNCTION {IMMUTABLE_FUNCTION_NAME}()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $function$
            BEGIN
                RAISE EXCEPTION
                    'rows in {TABLE_NAME} are immutable';
            END;
            $function$
            '''
        )
    )
    op.execute(
        sa.text(
            f'''
            CREATE TRIGGER {IMMUTABLE_TRUNCATE_TRIGGER_NAME}
            BEFORE TRUNCATE ON {TABLE_NAME}
            FOR EACH STATEMENT
            EXECUTE FUNCTION {IMMUTABLE_FUNCTION_NAME}()
            '''
        )
    )
    op.execute(
        sa.text(
            f'''
            CREATE TRIGGER {IMMUTABLE_TRIGGER_NAME}
            BEFORE UPDATE OR DELETE ON {TABLE_NAME}
            FOR EACH ROW
            EXECUTE FUNCTION {IMMUTABLE_FUNCTION_NAME}()
            '''
        )
    )


def _drop_postgresql_immutable_guard() -> None:
    bind = op.get_bind()
    if bind.dialect.name != 'postgresql':
        return
    op.execute(
        sa.text(
            f'DROP TRIGGER IF EXISTS {IMMUTABLE_TRUNCATE_TRIGGER_NAME} '
            f'ON {TABLE_NAME}'
        )
    )
    op.execute(
        sa.text(
            f'DROP TRIGGER IF EXISTS {IMMUTABLE_TRIGGER_NAME} ON {TABLE_NAME}'
        )
    )
    op.execute(
        sa.text(f'DROP FUNCTION IF EXISTS {IMMUTABLE_FUNCTION_NAME}()')
    )


def upgrade() -> None:
    with op.batch_alter_table('daily_price_bars') as batch_op:
        batch_op.add_column(
            sa.Column('repair_origin_eod_run_id', sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column('repair_origin_source', sa.String(length=32), nullable=True)
        )
        batch_op.create_foreign_key(
            PRICE_BAR_ORIGIN_FK_NAME,
            'eod_collection_runs',
            ['repair_origin_eod_run_id'],
            ['id'],
            ondelete='SET NULL',
        )
        batch_op.create_index(
            'ix_daily_price_bars_repair_origin_eod_run_id',
            ['repair_origin_eod_run_id'],
            unique=False,
            postgresql_where=sa.text(
                'repair_origin_eod_run_id IS NOT NULL'
            ),
        )

    op.create_table(
        BATCH_TABLE_NAME,
        sa.Column('repair_batch_id', sa.String(length=64), nullable=False),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('affected_symbols', sa.JSON(), nullable=False),
        sa.Column('affected_trade_dates', sa.JSON(), nullable=False),
        sa.Column('required_rebuild_trade_dates', sa.JSON(), nullable=False),
        sa.Column(
            'required_evaluation_market_date_from',
            sa.Date(),
            nullable=False,
        ),
        sa.Column(
            'required_evaluation_market_date_to',
            sa.Date(),
            nullable=False,
        ),
        sa.Column(
            'evaluation_rebuilt_at',
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column('evaluation_rebuild_metadata', sa.JSON(), nullable=True),
        sa.Column('factor_rebuild_metadata', sa.JSON(), nullable=True),
        sa.Column('repaired_row_count', sa.Integer(), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('verification_metadata', sa.JSON(), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'completed')",
            name='ck_daily_price_bar_repair_batches_status',
        ),
        sa.CheckConstraint(
            'repaired_row_count > 0',
            name='ck_daily_price_bar_repair_batches_positive_row_count',
        ),
        sa.PrimaryKeyConstraint('repair_batch_id'),
    )
    op.create_index(
        'ix_daily_price_bar_repair_batches_status_created',
        BATCH_TABLE_NAME,
        ['status', 'created_at'],
        unique=False,
    )

    op.create_table(
        TABLE_NAME,
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('daily_price_bar_id', sa.Integer(), nullable=False),
        sa.Column('repair_batch_id', sa.String(length=64), nullable=False),
        sa.Column('symbol', sa.String(length=32), nullable=False),
        sa.Column('trade_date', sa.Date(), nullable=False),
        sa.Column('provider', sa.String(length=32), nullable=False),
        sa.Column('provider_market_code', sa.String(length=8), nullable=False),
        sa.Column('provider_adjusted_price', sa.String(length=8), nullable=False),
        sa.Column('provider_price_mode', sa.String(length=16), nullable=False),
        sa.Column('old_open_price', sa.BigInteger(), nullable=False),
        sa.Column('old_high_price', sa.BigInteger(), nullable=False),
        sa.Column('old_low_price', sa.BigInteger(), nullable=False),
        sa.Column('old_close_price', sa.BigInteger(), nullable=False),
        sa.Column('old_volume', sa.BigInteger(), nullable=False),
        sa.Column('old_turnover', sa.BigInteger(), nullable=False),
        sa.Column('new_open_price', sa.BigInteger(), nullable=False),
        sa.Column('new_high_price', sa.BigInteger(), nullable=False),
        sa.Column('new_low_price', sa.BigInteger(), nullable=False),
        sa.Column('new_close_price', sa.BigInteger(), nullable=False),
        sa.Column('new_volume', sa.BigInteger(), nullable=False),
        sa.Column('new_turnover', sa.BigInteger(), nullable=False),
        sa.Column('original_source', sa.String(length=32), nullable=False),
        sa.Column(
            'original_captured_at',
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column('original_eod_run_id', sa.Integer(), nullable=True),
        sa.Column('root_source', sa.String(length=32), nullable=True),
        sa.Column('root_eod_run_id', sa.Integer(), nullable=True),
        sa.Column(
            'repaired_at',
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "provider = 'kis' AND provider_market_code = 'J' "
            "AND provider_adjusted_price = '1' "
            "AND provider_price_mode = 'original'",
            name='ck_daily_price_bar_repair_audits_kis_original',
        ),
        sa.CheckConstraint(
            'old_open_price >= 0 AND old_high_price >= 0 '
            'AND old_low_price >= 0 AND old_close_price >= 0 '
            'AND old_volume >= 0 AND old_turnover >= 0 '
            'AND new_open_price >= 0 AND new_high_price >= 0 '
            'AND new_low_price >= 0 AND new_close_price >= 0 '
            'AND new_volume >= 0 AND new_turnover >= 0',
            name='ck_daily_price_bar_repair_audits_nonnegative',
        ),
        sa.CheckConstraint(
            '(old_open_price, old_high_price, old_low_price, old_close_price, '
            'old_volume, old_turnover) <> '
            '(new_open_price, new_high_price, new_low_price, new_close_price, '
            'new_volume, new_turnover)',
            name='ck_daily_price_bar_repair_audits_changed',
        ),
        sa.ForeignKeyConstraint(
            ['daily_price_bar_id'],
            ['daily_price_bars.id'],
            ondelete='RESTRICT',
        ),
        sa.ForeignKeyConstraint(
            ['repair_batch_id'],
            [f'{BATCH_TABLE_NAME}.repair_batch_id'],
            ondelete='RESTRICT',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'repair_batch_id',
            'daily_price_bar_id',
            name='uq_daily_price_bar_repair_audits_batch_bar',
        ),
    )
    op.create_index(
        'ix_daily_price_bar_repair_audits_trade_symbol',
        TABLE_NAME,
        ['trade_date', 'symbol'],
        unique=False,
    )
    op.create_index(
        'ix_daily_price_bar_repair_audits_bar_repaired',
        TABLE_NAME,
        ['daily_price_bar_id', 'repaired_at'],
        unique=False,
    )
    _install_postgresql_immutable_guard()


def downgrade() -> None:
    _drop_postgresql_immutable_guard()
    op.drop_index(
        'ix_daily_price_bar_repair_audits_bar_repaired',
        table_name=TABLE_NAME,
    )
    op.drop_index(
        'ix_daily_price_bar_repair_audits_trade_symbol',
        table_name=TABLE_NAME,
    )
    op.drop_table(TABLE_NAME)
    op.drop_index(
        'ix_daily_price_bar_repair_batches_status_created',
        table_name=BATCH_TABLE_NAME,
    )
    op.drop_table(BATCH_TABLE_NAME)
    with op.batch_alter_table('daily_price_bars') as batch_op:
        batch_op.drop_index(
            'ix_daily_price_bars_repair_origin_eod_run_id'
        )
        batch_op.drop_constraint(
            PRICE_BAR_ORIGIN_FK_NAME,
            type_='foreignkey',
        )
        batch_op.drop_column('repair_origin_source')
        batch_op.drop_column('repair_origin_eod_run_id')
