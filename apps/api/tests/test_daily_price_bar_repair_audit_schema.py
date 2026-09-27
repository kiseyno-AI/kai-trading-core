from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError
import pytest

from app.db import engine
from app.db_models import (
    DailyPriceBar,
    DailyPriceBarRepairAudit,
    DailyPriceBarRepairBatch,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / 'db_migrations'
    / 'versions'
    / '20260926_0082_daily_price_bar_repair_audits.py'
)


def _migration():
    spec = importlib.util.spec_from_file_location(MIGRATION_PATH.stem, MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_daily_price_bar_repair_audit_model_contract() -> None:
    table = DailyPriceBarRepairAudit.__table__
    assert tuple(table.c.keys()) == (
        'id',
        'daily_price_bar_id',
        'repair_batch_id',
        'symbol',
        'trade_date',
        'provider',
        'provider_market_code',
        'provider_adjusted_price',
        'provider_price_mode',
        'old_open_price',
        'old_high_price',
        'old_low_price',
        'old_close_price',
        'old_volume',
        'old_turnover',
        'new_open_price',
        'new_high_price',
        'new_low_price',
        'new_close_price',
        'new_volume',
        'new_turnover',
        'original_source',
        'original_captured_at',
        'original_eod_run_id',
        'root_source',
        'root_eod_run_id',
        'repaired_at',
    )

    unique_constraints = {
        constraint.name: tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, sa.UniqueConstraint)
    }
    assert unique_constraints['uq_daily_price_bar_repair_audits_batch_bar'] == (
        'repair_batch_id',
        'daily_price_bar_id',
    )
    assert {index.name for index in table.indexes} == {
        'ix_daily_price_bar_repair_audits_bar_repaired',
        'ix_daily_price_bar_repair_audits_trade_symbol',
    }
    foreign_key = next(iter(table.c.daily_price_bar_id.foreign_keys))
    assert foreign_key.target_fullname == 'daily_price_bars.id'
    assert foreign_key.ondelete == 'RESTRICT'
    batch_foreign_key = next(iter(table.c.repair_batch_id.foreign_keys))
    assert (
        batch_foreign_key.target_fullname
        == 'daily_price_bar_repair_batches.repair_batch_id'
    )
    assert batch_foreign_key.ondelete == 'RESTRICT'
    assert 'updated_at' not in table.c


def test_daily_price_bar_repair_batch_and_origin_model_contract() -> None:
    price_bar_table = DailyPriceBar.__table__
    assert price_bar_table.c.repair_origin_source.nullable
    assert price_bar_table.c.repair_origin_eod_run_id.nullable
    origin_foreign_key = next(
        iter(price_bar_table.c.repair_origin_eod_run_id.foreign_keys)
    )
    assert origin_foreign_key.target_fullname == 'eod_collection_runs.id'
    assert origin_foreign_key.ondelete == 'SET NULL'
    assert origin_foreign_key.name == (
        'fk_daily_price_bars_repair_origin_eod_run_id_eod_run'
    )
    origin_index = next(
        index
        for index in price_bar_table.indexes
        if index.name == 'ix_daily_price_bars_repair_origin_eod_run_id'
    )
    assert str(
        origin_index.dialect_options['postgresql']['where']
    ) == 'repair_origin_eod_run_id IS NOT NULL'

    batch_table = DailyPriceBarRepairBatch.__table__
    assert tuple(batch_table.c.keys()) == (
        'repair_batch_id',
        'status',
        'affected_symbols',
        'affected_trade_dates',
        'required_rebuild_trade_dates',
        'required_evaluation_market_date_from',
        'required_evaluation_market_date_to',
        'evaluation_rebuilt_at',
        'evaluation_rebuild_metadata',
        'factor_rebuild_metadata',
        'repaired_row_count',
        'created_at',
        'completed_at',
        'verification_metadata',
    )
    assert tuple(column.name for column in batch_table.primary_key.columns) == (
        'repair_batch_id',
    )
    assert {index.name for index in batch_table.indexes} == {
        'ix_daily_price_bar_repair_batches_status_created'
    }
    check_names = {
        constraint.name
        for constraint in batch_table.constraints
        if isinstance(constraint, sa.CheckConstraint)
    }
    assert check_names == {
        'ck_daily_price_bar_repair_batches_positive_row_count',
        'ck_daily_price_bar_repair_batches_status',
    }


def test_daily_price_bar_repair_audit_migration_upgrade_and_downgrade(
    monkeypatch,
) -> None:
    migration = _migration()
    engine = sa.create_engine('sqlite+pysqlite:///:memory:')

    with engine.begin() as connection:
        metadata = sa.MetaData()
        sa.Table(
            'eod_collection_runs',
            metadata,
            sa.Column('id', sa.Integer(), primary_key=True),
        )
        sa.Table(
            'daily_price_bars',
            metadata,
            sa.Column('id', sa.Integer(), primary_key=True),
        )
        metadata.create_all(connection)

        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(migration, 'op', operations)
        migration.upgrade()

        inspector = sa.inspect(connection)
        assert {
            migration.BATCH_TABLE_NAME,
            migration.TABLE_NAME,
        }.issubset(inspector.get_table_names())
        assert {
            'repair_origin_eod_run_id',
            'repair_origin_source',
        }.issubset(
            {
                column['name']
                for column in inspector.get_columns('daily_price_bars')
            }
        )
        assert 'ix_daily_price_bars_repair_origin_eod_run_id' in {
            index['name']
            for index in inspector.get_indexes('daily_price_bars')
        }
        origin_foreign_key = next(
            foreign_key
            for foreign_key in inspector.get_foreign_keys('daily_price_bars')
            if foreign_key['constrained_columns'] == [
                'repair_origin_eod_run_id'
            ]
        )
        assert origin_foreign_key['referred_table'] == 'eod_collection_runs'
        assert origin_foreign_key['referred_columns'] == ['id']
        assert origin_foreign_key['options']['ondelete'] == 'SET NULL'

        assert {
            'repair_batch_id',
            'status',
            'affected_symbols',
            'affected_trade_dates',
            'required_rebuild_trade_dates',
            'required_evaluation_market_date_from',
            'required_evaluation_market_date_to',
            'evaluation_rebuilt_at',
            'evaluation_rebuild_metadata',
            'factor_rebuild_metadata',
            'repaired_row_count',
            'created_at',
            'completed_at',
            'verification_metadata',
        } == {
            column['name']
            for column in inspector.get_columns(migration.BATCH_TABLE_NAME)
        }
        assert {
            'ck_daily_price_bar_repair_batches_positive_row_count',
            'ck_daily_price_bar_repair_batches_status',
        } == {
            constraint['name']
            for constraint in inspector.get_check_constraints(
                migration.BATCH_TABLE_NAME
            )
        }
        assert {
            'ix_daily_price_bar_repair_batches_status_created'
        } == {
            index['name']
            for index in inspector.get_indexes(migration.BATCH_TABLE_NAME)
        }

        assert {
            'daily_price_bar_id',
            'repair_batch_id',
            'symbol',
            'trade_date',
            'provider',
            'provider_market_code',
            'provider_adjusted_price',
            'provider_price_mode',
            'old_open_price',
            'old_turnover',
            'new_open_price',
            'new_turnover',
            'original_source',
            'original_captured_at',
            'original_eod_run_id',
            'root_source',
            'root_eod_run_id',
            'repaired_at',
        }.issubset(
            {
                column['name']
                for column in inspector.get_columns(migration.TABLE_NAME)
            }
        )
        assert any(
            constraint['name']
            == 'uq_daily_price_bar_repair_audits_batch_bar'
            for constraint in inspector.get_unique_constraints(migration.TABLE_NAME)
        )
        assert {
            'ck_daily_price_bar_repair_audits_changed',
            'ck_daily_price_bar_repair_audits_kis_original',
            'ck_daily_price_bar_repair_audits_nonnegative',
        }.issubset(
            {
                constraint['name']
                for constraint in inspector.get_check_constraints(
                    migration.TABLE_NAME
                )
            }
        )
        assert {
            'ix_daily_price_bar_repair_audits_bar_repaired',
            'ix_daily_price_bar_repair_audits_trade_symbol',
        } == {
            index['name'] for index in inspector.get_indexes(migration.TABLE_NAME)
        }
        audit_foreign_keys = {
            tuple(foreign_key['constrained_columns']): foreign_key
            for foreign_key in inspector.get_foreign_keys(migration.TABLE_NAME)
        }
        assert audit_foreign_keys[('repair_batch_id',)]['referred_table'] == (
            migration.BATCH_TABLE_NAME
        )
        assert (
            audit_foreign_keys[('repair_batch_id',)]['options']['ondelete']
            == 'RESTRICT'
        )

        migration.downgrade()
        downgraded_inspector = sa.inspect(connection)
        assert migration.TABLE_NAME not in downgraded_inspector.get_table_names()
        assert (
            migration.BATCH_TABLE_NAME
            not in downgraded_inspector.get_table_names()
        )
        assert {
            'repair_origin_eod_run_id',
            'repair_origin_source',
        }.isdisjoint(
            {
                column['name']
                for column in downgraded_inspector.get_columns(
                    'daily_price_bars'
                )
            }
        )

    engine.dispose()


def test_postgresql_guard_rejects_update_delete_and_truncate(monkeypatch) -> None:
    migration = _migration()
    executed: list[str] = []

    class _Dialect:
        name = 'postgresql'

    class _Bind:
        dialect = _Dialect()

    class _Operations:
        @staticmethod
        def get_bind():
            return _Bind()

        @staticmethod
        def execute(statement) -> None:
            executed.append(str(statement))

    monkeypatch.setattr(migration, 'op', _Operations())
    migration._install_postgresql_immutable_guard()

    guard_ddl = '\n'.join(executed)
    assert 'BEFORE UPDATE OR DELETE' in guard_ddl
    assert 'BEFORE TRUNCATE' in guard_ddl
    assert migration.IMMUTABLE_TRUNCATE_TRIGGER_NAME in guard_ddl
    assert migration.TABLE_NAME in guard_ddl
    assert migration.IMMUTABLE_FUNCTION_NAME in guard_ddl


def test_postgresql_guard_actually_rejects_truncate() -> None:
    if engine.dialect.name != 'postgresql':
        pytest.skip('PostgreSQL trigger behavior requires PostgreSQL')

    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            with pytest.raises(DBAPIError, match='immutable'):
                connection.execute(
                    sa.text('TRUNCATE TABLE daily_price_bar_repair_audits')
                )
        finally:
            transaction.rollback()
