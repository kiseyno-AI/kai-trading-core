from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest


migration = importlib.import_module(
    'db_migrations.versions.20261027_0087_core_schema_drift_repair'
)


def test_portfolio_not_null_guard_allows_only_zero_null_rows() -> None:
    inspector = MagicMock()
    inspector.get_columns.return_value = [
        {'name': 'as_of', 'nullable': True},
    ]
    bind = MagicMock()
    bind.execute.return_value.first.return_value = None

    assert migration._portfolio_as_of_requires_not_null(bind, inspector) is True

    bind.execute.return_value.first.return_value = (17,)
    with pytest.raises(RuntimeError, match='while NULL rows exist'):
        migration._portfolio_as_of_requires_not_null(bind, inspector)


def test_advanced_value_index_rename_is_non_destructive_and_fail_closed() -> None:
    legacy_name, target_name, column_name = (
        migration.ADVANCED_VALUE_INDEX_RENAMES[0]
    )
    inspector = MagicMock()
    inspector.get_indexes.return_value = [
        {
            'name': legacy_name,
            'column_names': [column_name],
            'unique': False,
        }
    ]
    assert migration._advanced_value_renames(inspector) == (
        (legacy_name, target_name),
    )

    inspector.get_indexes.return_value.append(
        {
            'name': target_name,
            'column_names': [column_name],
            'unique': False,
        }
    )
    with pytest.raises(RuntimeError, match='both legacy and canonical'):
        migration._advanced_value_renames(inspector)


def test_upgrade_uses_additive_concurrent_index_repairs() -> None:
    bind = MagicMock()
    bind.dialect.name = 'postgresql'
    bind.execute.return_value.first.return_value = None

    inspector = MagicMock()
    required_tables = {
        table_name
        for _index_name, table_name, _ddl in migration.PRESERVED_INDEX_DDLS
    }
    required_tables.update(
        {
            migration.PORTFOLIO_TABLE_NAME,
            migration.DESIRED_TECHNICAL_INDEX_DDL[1],
            'symbols',
        }
    )
    inspector.get_table_names.return_value = sorted(required_tables)
    inspector.get_columns.return_value = [
        {'name': 'as_of', 'nullable': False},
    ]
    inspector.get_indexes.return_value = [
        {
            'name': target_name,
            'column_names': [column_name],
            'unique': False,
        }
        for _legacy_name, target_name, column_name
        in migration.ADVANCED_VALUE_INDEX_RENAMES
    ]
    context = MagicMock()

    with (
        patch.object(migration.op, 'get_bind', return_value=bind),
        patch.object(migration.sa, 'inspect', return_value=inspector),
        patch.object(migration.op, 'create_table') as create_table,
        patch.object(migration.op, 'alter_column') as alter_column,
        patch.object(migration.op, 'execute') as execute,
        patch.object(migration.op, 'get_context', return_value=context),
    ):
        migration.upgrade()

    create_table.assert_called_once()
    alter_column.assert_not_called()
    executed_sql = [str(call.args[0]) for call in execute.call_args_list]
    create_index_sql = [
        statement
        for statement in executed_sql
        if statement.startswith('CREATE INDEX')
    ]
    assert len(create_index_sql) == 41
    assert all(
        statement.startswith('CREATE INDEX CONCURRENTLY IF NOT EXISTS ')
        for statement in create_index_sql
    )
    assert not any('DROP INDEX' in statement for statement in executed_sql)
    context.autocommit_block.assert_called_once_with()


def test_invalid_target_index_and_downgrade_fail_closed() -> None:
    bind = MagicMock()
    bind.execute.return_value.first.return_value = (
        'ix_daily_technical_snapshots_eod_run_captured_id',
        False,
        True,
    )
    with pytest.raises(RuntimeError, match='invalid or not-ready'):
        migration._assert_no_invalid_target_indexes(bind)

    with pytest.raises(RuntimeError, match='forward-only schema repair'):
        migration.downgrade()
