from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / 'db_migrations'
    / 'versions'
    / '20260911_0081_core_exec_buy_authority_outbox.py'
)


def _migration():
    spec = importlib.util.spec_from_file_location(MIGRATION_PATH.stem, MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_core_exec_buy_authority_outbox_upgrade_and_downgrade(monkeypatch) -> None:
    migration = _migration()
    engine = sa.create_engine('sqlite+pysqlite:///:memory:')

    with engine.begin() as connection:
        operations = Operations(MigrationContext.configure(connection))
        monkeypatch.setattr(migration, 'op', operations)
        migration.upgrade()

        inspector = sa.inspect(connection)
        assert {
            'core_exec_buy_authority_states',
            'core_exec_buy_authority_outbox',
        }.issubset(inspector.get_table_names())
        assert {
            column['name']
            for column in inspector.get_columns('core_exec_buy_authority_states')
        } >= {
            'scope_key',
            'provider',
            'market',
            'currency',
            'runtime_mode',
            'account_reference',
            'latest_observation_id',
            'latest_delivery_sequence',
            'delivery_status',
            'remote_fence_active',
        }
        assert any(
            constraint['name'] == 'uq_core_exec_buy_authority_states_scope'
            for constraint in inspector.get_unique_constraints(
                'core_exec_buy_authority_states'
            )
        )
        assert any(
            constraint['name']
            == 'uq_core_exec_buy_authority_outbox_scope_sequence'
            for constraint in inspector.get_unique_constraints(
                'core_exec_buy_authority_outbox'
            )
        )
        assert any(
            constraint['name'] == 'ck_core_exec_buy_authority_states_krx_scope'
            for constraint in inspector.get_check_constraints(
                'core_exec_buy_authority_states'
            )
        )
        assert any(
            index['name'] == 'ix_core_exec_buy_authority_outbox_pending'
            for index in inspector.get_indexes('core_exec_buy_authority_outbox')
        )

        migration.downgrade()
        remaining = sa.inspect(connection).get_table_names()
        assert 'core_exec_buy_authority_states' not in remaining
        assert 'core_exec_buy_authority_outbox' not in remaining

    engine.dispose()
