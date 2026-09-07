from datetime import date, datetime, timezone
import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
from sqlalchemy import create_engine

from app.db_models import AgentContextFeedback, Base, SignalDeciderDecisionSnapshot
from app.internal.deploy_hygiene import RUNTIME_SCHEMA_UNIQUE_CHECKS


MIGRATION_DIR = Path(__file__).resolve().parents[1] / 'db_migrations' / 'versions'


def _load_migration(filename: str):
    path = MIGRATION_DIR / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'could not load migration {path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SignalDeciderIntegrityMigrationTests(unittest.TestCase):
    def test_postgresql_policy_version_columns_accept_registry_width(self):
        if not os.environ.get('DATABASE_URL', '').startswith('postgresql'):
            self.skipTest('requires an explicitly configured PostgreSQL test database')

        from app.db import engine as application_engine

        expected_columns = {
            ('signal_decider_decision_snapshots', 'policy_version'),
            ('signal_decider_realized_outcomes', 'policy_version'),
            ('signal_decider_evaluations', 'policy_version'),
            ('signal_decider_calendar_review_items', 'policy_version'),
            ('signal_decider_policy_versions', 'policy_version'),
            ('signal_decider_policy_versions', 'parent_policy_version'),
            ('signal_decider_policy_versions', 'source_candidate_version'),
            ('signal_decider_policy_candidates', 'candidate_version'),
            ('signal_decider_policy_candidates', 'base_policy_version'),
            ('signal_decider_execution_plan_items', 'policy_version'),
            ('signal_decider_actual_execution_outcomes', 'policy_version'),
        }
        columns = sa.table(
            'columns',
            sa.column('table_schema', sa.String()),
            sa.column('table_name', sa.String()),
            sa.column('column_name', sa.String()),
            sa.column('character_maximum_length', sa.Integer()),
            schema='information_schema',
        )

        with application_engine.connect() as connection:
            rows = connection.execute(
                sa.select(
                    columns.c.table_name,
                    columns.c.column_name,
                    columns.c.character_maximum_length,
                ).where(
                    columns.c.table_schema == sa.func.current_schema(),
                    columns.c.table_name.like('signal_decider_%'),
                    columns.c.column_name.in_(
                        {
                            'policy_version',
                            'parent_policy_version',
                            'source_candidate_version',
                            'candidate_version',
                            'base_policy_version',
                        }
                    ),
                )
            ).all()

        reflected_lengths = {
            (row.table_name, row.column_name): row.character_maximum_length
            for row in rows
        }
        self.assertTrue(expected_columns.issubset(reflected_lengths))
        self.assertEqual(
            {key: reflected_lengths[key] for key in expected_columns},
            {key: 128 for key in expected_columns},
        )

    def test_single_active_policy_migration_fails_closed_on_existing_conflict(self):
        migration = _load_migration('20260906_0076_single_active_signal_decider_policy.py')
        engine = create_engine('sqlite+pysqlite:///:memory:')
        metadata = sa.MetaData()
        policies = sa.Table(
            migration.TABLE_NAME,
            metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('policy_version', sa.String(64), nullable=False),
            sa.Column('status', sa.String(32), nullable=False),
        )
        metadata.create_all(engine)

        with engine.begin() as connection:
            connection.execute(
                policies.insert(),
                [
                    {'id': 1, 'policy_version': 'signal_decider_policy_v1', 'status': 'active'},
                    {'id': 2, 'policy_version': 'signal_decider_policy_v2', 'status': 'active'},
                ],
            )
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, 'op', operations):
                with self.assertRaisesRegex(RuntimeError, 'resolve the policy registry conflict'):
                    migration.upgrade()

        index_names = {index['name'] for index in sa.inspect(engine).get_indexes(migration.TABLE_NAME)}
        self.assertNotIn(migration.INDEX_NAME, index_names)
        with engine.connect() as connection:
            statuses = connection.execute(
                sa.select(policies.c.status).order_by(policies.c.id.asc())
            ).scalars().all()
        self.assertEqual(statuses, ['active', 'active'])
        engine.dispose()

    def test_append_only_migration_preserves_duplicate_null_feedback_history(self):
        migration = _load_migration('20260906_0077_append_only_signal_decider_snapshots.py')
        engine = create_engine('sqlite+pysqlite:///:memory:')
        metadata = sa.MetaData()
        snapshots = sa.Table(
            migration.TABLE_NAME,
            metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('feedback_id', sa.Integer, nullable=True),
            sa.Column('market_date', sa.Date, nullable=False),
            sa.Column('symbol', sa.String(32), nullable=False),
            sa.Column('agent_name', sa.String(64), nullable=False),
            sa.Column('decision_run_id', sa.String(128), nullable=True),
            sa.Column('feedback_received_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('thesis_summary', sa.Text, nullable=False),
            sa.Column('request_payload', sa.JSON, nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        )
        sa.Index(
            migration.LEGACY_UNIQUE_INDEX_NAME,
            snapshots.c.feedback_id,
            unique=True,
            sqlite_where=sa.text('feedback_id IS NOT NULL'),
        )
        metadata.create_all(engine)
        common = {
            'feedback_id': None,
            'market_date': date(2026, 4, 23),
            'symbol': '005930',
            'agent_name': 'signal-decider',
            'decision_run_id': 'same-run',
            'thesis_summary': 'same content',
            'request_payload': {'same': True},
        }

        with engine.begin() as connection:
            connection.execute(
                snapshots.insert(),
                [
                    {
                        'id': 1,
                        **common,
                        'feedback_received_at': datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
                        'created_at': datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
                        'updated_at': datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
                    },
                    {
                        'id': 2,
                        **common,
                        'feedback_received_at': datetime(2026, 4, 23, 7, 0, tzinfo=timezone.utc),
                        'created_at': datetime(2026, 4, 23, 7, 0, tzinfo=timezone.utc),
                        'updated_at': datetime(2026, 4, 23, 7, 0, tzinfo=timezone.utc),
                    },
                ],
            )
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, 'op', operations):
                migration.upgrade()

        reflected = sa.Table(migration.TABLE_NAME, sa.MetaData(), autoload_with=engine)
        with engine.connect() as connection:
            rows = connection.execute(
                sa.select(reflected.c.id, reflected.c.content_fingerprint).order_by(
                    reflected.c.id.asc()
                )
            ).all()
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(row.content_fingerprint is not None for row in rows), 1)
        index_names = {index['name'] for index in sa.inspect(engine).get_indexes(migration.TABLE_NAME)}
        self.assertIn(migration.FINGERPRINT_UNIQUE_INDEX_NAME, index_names)
        self.assertIn(migration.FEEDBACK_INDEX_NAME, index_names)
        self.assertNotIn(migration.LEGACY_UNIQUE_INDEX_NAME, index_names)
        engine.dispose()

    def test_fresh_schema_and_deploy_hygiene_use_migration_index_name(self):
        migration = _load_migration('20260906_0077_append_only_signal_decider_snapshots.py')
        engine = create_engine('sqlite+pysqlite:///:memory:')
        Base.metadata.create_all(
            engine,
            tables=[
                AgentContextFeedback.__table__,
                SignalDeciderDecisionSnapshot.__table__,
            ],
        )

        index_names = {
            index['name']
            for index in sa.inspect(engine).get_indexes(
                SignalDeciderDecisionSnapshot.__tablename__
            )
        }
        self.assertIn(migration.FINGERPRINT_UNIQUE_INDEX_NAME, index_names)
        self.assertNotIn(migration.LEGACY_UNIQUE_INDEX_NAME, index_names)
        hygiene_check = next(
            check
            for check in RUNTIME_SCHEMA_UNIQUE_CHECKS
            if check['table'] == SignalDeciderDecisionSnapshot.__tablename__
            and check['columns'] == ('content_fingerprint',)
        )
        self.assertEqual(
            hygiene_check['required_name'],
            migration.FINGERPRINT_UNIQUE_INDEX_NAME,
        )
        Base.metadata.drop_all(
            engine,
            tables=[
                SignalDeciderDecisionSnapshot.__table__,
                AgentContextFeedback.__table__,
            ],
        )
        engine.dispose()


if __name__ == '__main__':
    unittest.main()
