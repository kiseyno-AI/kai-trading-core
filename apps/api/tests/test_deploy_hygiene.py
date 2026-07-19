from __future__ import annotations

from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import httpx

from app.internal import deploy_hygiene

READY_MARKET_SNAPSHOT_RUNTIME = {
    'ready': True,
    'global_kis_environment': 'demo',
    'effective_kis_environment': 'real',
    'effective_kis_base_url': 'https://openapi.koreainvestment.com:9443',
    'uses_real_domain_override': True,
    'market_snapshot_kis_use_real': True,
    'kis_app_key_configured': True,
    'kis_app_secret_configured': True,
    'koreaexim_api_key_configured': True,
    'blocking_reasons': [],
}

BLOCKED_MARKET_SNAPSHOT_RUNTIME = {
    **READY_MARKET_SNAPSHOT_RUNTIME,
    'ready': False,
    'blocking_reasons': ['Set KOREAEXIM_API_KEY in apps/api/.env before deploying market snapshot.'],
    'koreaexim_api_key_configured': False,
}

READY_PORTFOLIO_SUMMARY_RUNTIME = {
    'ready': True,
    'app_env': 'production',
    'kis_environment': 'real',
    'kis_base_url': 'https://openapi.koreainvestment.com:9443',
    'kis_use_mock': False,
    'kis_app_key_configured': True,
    'kis_app_secret_configured': True,
    'portfolio_account_references_configured': True,
    'portfolio_account_references_valid': True,
    'configured_account_count': 2,
    'blocking_reasons': [],
}

BLOCKED_PORTFOLIO_SUMMARY_RUNTIME = {
    **READY_PORTFOLIO_SUMMARY_RUNTIME,
    'ready': False,
    'kis_environment': 'demo',
    'kis_use_mock': True,
    'configured_account_count': 0,
    'portfolio_account_references_configured': False,
    'portfolio_account_references_valid': False,
    'blocking_reasons': [
        'No portfolio accounts are configured. Set PORTFOLIO_ACCOUNT_REFERENCES to one or more KIS:<account_number>-<product_code> values.',
        'KIS_USE_MOCK must be false in production so portfolio summary uses live KIS account inquiries.',
    ],
}

READY_SCHEMA_DRIFT_REPORT = {
    'status': deploy_hygiene.OK_STATUS,
    'ready': True,
    'summary': 'Runtime-critical schema objects are present.',
    'database': {
        'connected': True,
        'url': 'postgresql://test:***@localhost/test',
        'error': None,
        'existing_table_count': 1,
    },
    'blocking_reasons': [],
    'warnings': [],
    'recommended_actions': [],
    'checks': [],
}

BLOCKED_SCHEMA_DRIFT_REPORT = {
    **READY_SCHEMA_DRIFT_REPORT,
    'status': deploy_hygiene.FAIL_STATUS,
    'ready': False,
    'summary': 'Runtime-critical schema drift was detected.',
    'blocking_reasons': ['Missing required table daily_signal_features.'],
    'recommended_actions': ['Repair the missing runtime schema object.'],
}


def _ready_schema_inspector(
    *,
    missing_tables: set[str] | None = None,
    missing_unique_names: set[str] | None = None,
    missing_foreign_key_checks: set[str] | None = None,
) -> Mock:
    missing_tables = missing_tables or set()
    missing_unique_names = missing_unique_names or set()
    missing_foreign_key_checks = missing_foreign_key_checks or set()

    required_tables = set(deploy_hygiene._runtime_schema_required_tables()) - missing_tables
    unique_constraints_by_table: dict[str, list[dict]] = {table: [] for table in required_tables}
    unique_indexes_by_table: dict[str, list[dict]] = {table: [] for table in required_tables}
    foreign_keys_by_table: dict[str, list[dict]] = {table: [] for table in required_tables}

    for spec in deploy_hygiene.RUNTIME_SCHEMA_UNIQUE_CHECKS:
        table = spec['table']
        if table not in required_tables:
            continue
        required_name = spec.get('required_name')
        if required_name and required_name in missing_unique_names:
            continue
        if not required_name and spec['name'] in missing_unique_names:
            continue

        entry = {'name': required_name or f"uq_test_{spec['name']}", 'column_names': list(spec['columns'])}
        if required_name:
            unique_constraints_by_table[table].append(entry)
        else:
            unique_indexes_by_table[table].append({**entry, 'unique': True})

    for spec in deploy_hygiene.RUNTIME_SCHEMA_FOREIGN_KEY_CHECKS:
        table = spec['table']
        if table not in required_tables or spec['referred_table'] not in required_tables:
            continue
        if spec['name'] in missing_foreign_key_checks:
            continue
        foreign_keys_by_table[table].append(
            {
                'name': f"fk_test_{spec['name']}",
                'constrained_columns': list(spec['columns']),
                'referred_table': spec['referred_table'],
                'referred_columns': list(spec['referred_columns']),
            }
        )

    inspector = Mock()
    inspector.get_table_names.return_value = sorted(required_tables)
    inspector.get_pk_constraint.side_effect = lambda table: {
        'name': f'pk_{table}',
        'constrained_columns': ['id'],
    }
    inspector.get_unique_constraints.side_effect = lambda table: unique_constraints_by_table.get(table, [])
    inspector.get_indexes.side_effect = lambda table: unique_indexes_by_table.get(table, [])
    inspector.get_foreign_keys.side_effect = lambda table: foreign_keys_by_table.get(table, [])
    return inspector


class DeployHygieneTests(unittest.TestCase):
    def test_parse_create_table_metadata_distinguishes_guarded_and_unguarded_revisions(self) -> None:
        versions_dir = Path(__file__).resolve().parent.parent / 'db_migrations' / 'versions'
        guarded_tables, guarded = deploy_hygiene._parse_create_table_metadata(
            versions_dir / '20260405_0001_initial_market_schema.py'
        )
        unguarded_tables, unguarded = deploy_hygiene._parse_create_table_metadata(
            versions_dir / '20260419_0012_agent_feedback_session_cleanup.py'
        )

        self.assertIn('symbols', guarded_tables)
        self.assertTrue(guarded)
        self.assertEqual(unguarded_tables, ['agent_feedback_session_cleanup_failures'])
        self.assertFalse(unguarded)

    def test_collect_schema_drift_preflight_blocks_missing_runtime_table(self) -> None:
        connection = object()
        inspector = _ready_schema_inspector(missing_tables={'daily_signal_features'})

        connect_context = Mock()
        connect_context.__enter__ = Mock(return_value=connection)
        connect_context.__exit__ = Mock(return_value=False)

        with (
            patch('app.internal.deploy_hygiene.engine.connect', return_value=connect_context),
            patch('app.internal.deploy_hygiene.sqlalchemy_inspect', return_value=inspector),
        ):
            report, exit_code = deploy_hygiene.collect_schema_drift_preflight()

        self.assertEqual(exit_code, 1)
        self.assertFalse(report['ready'])
        self.assertEqual(report['status'], deploy_hygiene.FAIL_STATUS)
        self.assertIn('Missing required table daily_signal_features.', report['blocking_reasons'])
        missing_table_check = next(check for check in report['checks'] if check['name'] == 'daily_signal_features_table')
        self.assertFalse(missing_table_check['ready'])
        self.assertEqual(missing_table_check['kind'], 'table')

    def test_collect_schema_drift_preflight_blocks_missing_unique_and_foreign_key(self) -> None:
        connection = object()
        inspector = _ready_schema_inspector(
            missing_unique_names={'uq_daily_action_summaries_symbol_market_date'},
            missing_foreign_key_checks={'daily_signal_features_symbol_id_fk'},
        )

        connect_context = Mock()
        connect_context.__enter__ = Mock(return_value=connection)
        connect_context.__exit__ = Mock(return_value=False)

        with (
            patch('app.internal.deploy_hygiene.engine.connect', return_value=connect_context),
            patch('app.internal.deploy_hygiene.sqlalchemy_inspect', return_value=inspector),
        ):
            report, exit_code = deploy_hygiene.collect_schema_drift_preflight()

        self.assertEqual(exit_code, 1)
        self.assertFalse(report['ready'])
        self.assertIn(
            'Missing required unique constraint/index uq_daily_action_summaries_symbol_market_date '
            'on daily_action_summaries(symbol_id, market_date).',
            report['blocking_reasons'],
        )
        self.assertIn(
            'Missing foreign key from daily_signal_features(symbol_id) to symbols(id).',
            report['blocking_reasons'],
        )
        failed_check_names = {check['name'] for check in report['checks'] if not check['ready']}
        self.assertIn('daily_action_summaries_symbol_market_date_unique', failed_check_names)
        self.assertIn('daily_signal_features_symbol_id_fk', failed_check_names)

    def test_collect_migration_preflight_flags_likely_duplicate_table_drift(self) -> None:
        connection = object()
        inspector = Mock()
        inspector.get_table_names.return_value = ['agent_feedback_session_cleanup_failures']

        migration_context = Mock()
        migration_context.get_current_heads.return_value = ('20260417_0011',)

        connect_context = Mock()
        connect_context.__enter__ = Mock(return_value=connection)
        connect_context.__exit__ = Mock(return_value=False)

        revision_catalog = [
            {
                'revision': '20260417_0011',
                'migration': '20260417_0011_some_existing_revision.py',
                'create_tables': [],
                'duplicate_table_guarded': False,
            },
            {
                'revision': '20260419_0012',
                'migration': '20260419_0012_agent_feedback_session_cleanup.py',
                'create_tables': ['agent_feedback_session_cleanup_failures'],
                'duplicate_table_guarded': False,
            },
            {
                'revision': '20260419_0013',
                'migration': '20260419_0013_agent_context_feedback_failures.py',
                'create_tables': ['agent_context_feedback_failures'],
                'duplicate_table_guarded': False,
            },
        ]

        with (
            patch('app.internal.deploy_hygiene._load_revision_catalog', return_value=(('20260419_0013',), revision_catalog)),
            patch('app.internal.deploy_hygiene.engine.connect', return_value=connect_context),
            patch('app.internal.deploy_hygiene.sqlalchemy_inspect', return_value=inspector),
            patch('app.internal.deploy_hygiene.MigrationContext.configure', return_value=migration_context),
            patch('app.internal.deploy_hygiene._inspect_runtime_schema_drift', return_value=READY_SCHEMA_DRIFT_REPORT),
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=READY_MARKET_SNAPSHOT_RUNTIME),
            patch('app.internal.deploy_hygiene.portfolio_summary_service.runtime_status', return_value=READY_PORTFOLIO_SUMMARY_RUNTIME),
        ):
            report, exit_code = deploy_hygiene.collect_migration_preflight()

        self.assertEqual(exit_code, 2)
        self.assertEqual(report['status'], deploy_hygiene.FAIL_STATUS)
        self.assertEqual(report['drift']['status'], 'likely_duplicate_table_drift')
        self.assertEqual(report['drift']['findings'][0]['tables'], ['agent_feedback_session_cleanup_failures'])
        self.assertFalse(report['safe_to_continue'])

    def test_collect_migration_preflight_warns_when_pending_revisions_are_ordinary(self) -> None:
        connection = object()
        inspector = Mock()
        inspector.get_table_names.return_value = ['symbols']

        migration_context = Mock()
        migration_context.get_current_heads.return_value = tuple()

        connect_context = Mock()
        connect_context.__enter__ = Mock(return_value=connection)
        connect_context.__exit__ = Mock(return_value=False)

        revision_catalog = [
            {
                'revision': '20260405_0001',
                'migration': '20260405_0001_initial_market_schema.py',
                'create_tables': ['symbols'],
                'duplicate_table_guarded': True,
            },
            {
                'revision': '20260407_0002',
                'migration': '20260407_0002_dart_disclosure_sync.py',
                'create_tables': ['dart_disclosure_sync_runs'],
                'duplicate_table_guarded': True,
            },
        ]

        with (
            patch('app.internal.deploy_hygiene._load_revision_catalog', return_value=(('20260407_0002',), revision_catalog)),
            patch('app.internal.deploy_hygiene.engine.connect', return_value=connect_context),
            patch('app.internal.deploy_hygiene.sqlalchemy_inspect', return_value=inspector),
            patch('app.internal.deploy_hygiene.MigrationContext.configure', return_value=migration_context),
            patch('app.internal.deploy_hygiene._inspect_runtime_schema_drift', return_value=READY_SCHEMA_DRIFT_REPORT),
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=READY_MARKET_SNAPSHOT_RUNTIME),
            patch('app.internal.deploy_hygiene.portfolio_summary_service.runtime_status', return_value=READY_PORTFOLIO_SUMMARY_RUNTIME),
        ):
            report, exit_code = deploy_hygiene.collect_migration_preflight()

        self.assertEqual(exit_code, 0)
        self.assertEqual(report['status'], deploy_hygiene.WARN_STATUS)
        self.assertEqual(report['drift']['status'], 'none')
        self.assertTrue(report['safe_to_continue'])

    def test_collect_migration_preflight_reports_schema_drift_without_blocking_pre_upgrade(self) -> None:
        connection = object()
        inspector = Mock()
        inspector.get_table_names.return_value = ['symbols']

        migration_context = Mock()
        migration_context.get_current_heads.return_value = ('20260405_0001',)

        connect_context = Mock()
        connect_context.__enter__ = Mock(return_value=connection)
        connect_context.__exit__ = Mock(return_value=False)

        revision_catalog = [
            {
                'revision': '20260405_0001',
                'migration': '20260405_0001_initial_market_schema.py',
                'create_tables': ['symbols'],
                'duplicate_table_guarded': True,
            },
            {
                'revision': '20260407_0002',
                'migration': '20260407_0002_dart_disclosure_sync.py',
                'create_tables': ['dart_disclosure_sync_runs'],
                'duplicate_table_guarded': True,
            },
        ]

        with (
            patch('app.internal.deploy_hygiene._load_revision_catalog', return_value=(('20260407_0002',), revision_catalog)),
            patch('app.internal.deploy_hygiene.engine.connect', return_value=connect_context),
            patch('app.internal.deploy_hygiene.sqlalchemy_inspect', return_value=inspector),
            patch('app.internal.deploy_hygiene.MigrationContext.configure', return_value=migration_context),
            patch('app.internal.deploy_hygiene._inspect_runtime_schema_drift', return_value=BLOCKED_SCHEMA_DRIFT_REPORT),
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=READY_MARKET_SNAPSHOT_RUNTIME),
            patch('app.internal.deploy_hygiene.portfolio_summary_service.runtime_status', return_value=READY_PORTFOLIO_SUMMARY_RUNTIME),
        ):
            report, exit_code = deploy_hygiene.collect_migration_preflight()

        self.assertEqual(exit_code, 0)
        self.assertEqual(report['status'], deploy_hygiene.WARN_STATUS)
        self.assertFalse(report['schema_drift']['ready'])
        self.assertFalse(report['schema_drift']['enforced'])
        self.assertTrue(report['safe_to_continue'])

    def test_collect_migration_preflight_blocks_schema_drift_at_head(self) -> None:
        connection = object()
        inspector = Mock()
        inspector.get_table_names.return_value = ['symbols']

        migration_context = Mock()
        migration_context.get_current_heads.return_value = ('20260407_0002',)

        connect_context = Mock()
        connect_context.__enter__ = Mock(return_value=connection)
        connect_context.__exit__ = Mock(return_value=False)

        revision_catalog = [
            {
                'revision': '20260405_0001',
                'migration': '20260405_0001_initial_market_schema.py',
                'create_tables': ['symbols'],
                'duplicate_table_guarded': True,
            },
            {
                'revision': '20260407_0002',
                'migration': '20260407_0002_dart_disclosure_sync.py',
                'create_tables': ['dart_disclosure_sync_runs'],
                'duplicate_table_guarded': True,
            },
        ]

        with (
            patch('app.internal.deploy_hygiene._load_revision_catalog', return_value=(('20260407_0002',), revision_catalog)),
            patch('app.internal.deploy_hygiene.engine.connect', return_value=connect_context),
            patch('app.internal.deploy_hygiene.sqlalchemy_inspect', return_value=inspector),
            patch('app.internal.deploy_hygiene.MigrationContext.configure', return_value=migration_context),
            patch('app.internal.deploy_hygiene._inspect_runtime_schema_drift', return_value=BLOCKED_SCHEMA_DRIFT_REPORT),
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=READY_MARKET_SNAPSHOT_RUNTIME),
            patch('app.internal.deploy_hygiene.portfolio_summary_service.runtime_status', return_value=READY_PORTFOLIO_SUMMARY_RUNTIME),
        ):
            report, exit_code = deploy_hygiene.collect_migration_preflight()

        self.assertEqual(exit_code, 1)
        self.assertEqual(report['status'], deploy_hygiene.FAIL_STATUS)
        self.assertFalse(report['schema_drift']['ready'])
        self.assertTrue(report['schema_drift']['enforced'])
        self.assertFalse(report['safe_to_continue'])

    def test_collect_migration_preflight_require_head_fails_when_pending_remain(self) -> None:
        connection = object()
        inspector = Mock()
        inspector.get_table_names.return_value = []

        migration_context = Mock()
        migration_context.get_current_heads.return_value = ('20260417_0011',)

        connect_context = Mock()
        connect_context.__enter__ = Mock(return_value=connection)
        connect_context.__exit__ = Mock(return_value=False)

        revision_catalog = [
            {
                'revision': '20260417_0011',
                'migration': '20260417_0011_some_existing_revision.py',
                'create_tables': [],
                'duplicate_table_guarded': False,
            },
            {
                'revision': '20260419_0012',
                'migration': '20260419_0012_agent_feedback_session_cleanup.py',
                'create_tables': ['agent_feedback_session_cleanup_failures'],
                'duplicate_table_guarded': False,
            },
        ]

        with (
            patch('app.internal.deploy_hygiene._load_revision_catalog', return_value=(('20260419_0012',), revision_catalog)),
            patch('app.internal.deploy_hygiene.engine.connect', return_value=connect_context),
            patch('app.internal.deploy_hygiene.sqlalchemy_inspect', return_value=inspector),
            patch('app.internal.deploy_hygiene.MigrationContext.configure', return_value=migration_context),
            patch('app.internal.deploy_hygiene._inspect_runtime_schema_drift', return_value=READY_SCHEMA_DRIFT_REPORT),
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=READY_MARKET_SNAPSHOT_RUNTIME),
            patch('app.internal.deploy_hygiene.portfolio_summary_service.runtime_status', return_value=READY_PORTFOLIO_SUMMARY_RUNTIME),
        ):
            report, exit_code = deploy_hygiene.collect_migration_preflight(require_head=True)

        self.assertEqual(exit_code, 1)
        self.assertEqual(report['status'], deploy_hygiene.FAIL_STATUS)
        self.assertEqual(report['drift']['status'], 'none')
        self.assertFalse(report['safe_to_continue'])

    def test_collect_migration_preflight_fails_when_market_snapshot_runtime_is_not_ready(self) -> None:
        connection = object()
        inspector = Mock()
        inspector.get_table_names.return_value = ['symbols']

        migration_context = Mock()
        migration_context.get_current_heads.return_value = ('20260407_0002',)

        connect_context = Mock()
        connect_context.__enter__ = Mock(return_value=connection)
        connect_context.__exit__ = Mock(return_value=False)

        revision_catalog = [
            {
                'revision': '20260405_0001',
                'migration': '20260405_0001_initial_market_schema.py',
                'create_tables': ['symbols'],
                'duplicate_table_guarded': True,
            },
            {
                'revision': '20260407_0002',
                'migration': '20260407_0002_dart_disclosure_sync.py',
                'create_tables': ['dart_disclosure_sync_runs'],
                'duplicate_table_guarded': True,
            },
        ]

        with (
            patch('app.internal.deploy_hygiene._load_revision_catalog', return_value=(('20260407_0002',), revision_catalog)),
            patch('app.internal.deploy_hygiene.engine.connect', return_value=connect_context),
            patch('app.internal.deploy_hygiene.sqlalchemy_inspect', return_value=inspector),
            patch('app.internal.deploy_hygiene.MigrationContext.configure', return_value=migration_context),
            patch('app.internal.deploy_hygiene._inspect_runtime_schema_drift', return_value=READY_SCHEMA_DRIFT_REPORT),
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=BLOCKED_MARKET_SNAPSHOT_RUNTIME),
            patch('app.internal.deploy_hygiene.portfolio_summary_service.runtime_status', return_value=READY_PORTFOLIO_SUMMARY_RUNTIME),
        ):
            report, exit_code = deploy_hygiene.collect_migration_preflight()

        self.assertEqual(exit_code, 1)
        self.assertEqual(report['status'], deploy_hygiene.FAIL_STATUS)
        self.assertFalse(report['safe_to_continue'])
        self.assertEqual(report['runtime_checks']['market_snapshot']['blocking_reasons'], BLOCKED_MARKET_SNAPSHOT_RUNTIME['blocking_reasons'])

    def test_collect_migration_preflight_fails_when_portfolio_summary_runtime_is_not_ready(self) -> None:
        connection = object()
        inspector = Mock()
        inspector.get_table_names.return_value = ['symbols']

        migration_context = Mock()
        migration_context.get_current_heads.return_value = ('20260407_0002',)

        connect_context = Mock()
        connect_context.__enter__ = Mock(return_value=connection)
        connect_context.__exit__ = Mock(return_value=False)

        revision_catalog = [
            {
                'revision': '20260405_0001',
                'migration': '20260405_0001_initial_market_schema.py',
                'create_tables': ['symbols'],
                'duplicate_table_guarded': True,
            },
            {
                'revision': '20260407_0002',
                'migration': '20260407_0002_dart_disclosure_sync.py',
                'create_tables': ['dart_disclosure_sync_runs'],
                'duplicate_table_guarded': True,
            },
        ]

        with (
            patch('app.internal.deploy_hygiene._load_revision_catalog', return_value=(('20260407_0002',), revision_catalog)),
            patch('app.internal.deploy_hygiene.engine.connect', return_value=connect_context),
            patch('app.internal.deploy_hygiene.sqlalchemy_inspect', return_value=inspector),
            patch('app.internal.deploy_hygiene.MigrationContext.configure', return_value=migration_context),
            patch('app.internal.deploy_hygiene._inspect_runtime_schema_drift', return_value=READY_SCHEMA_DRIFT_REPORT),
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=READY_MARKET_SNAPSHOT_RUNTIME),
            patch('app.internal.deploy_hygiene.portfolio_summary_service.runtime_status', return_value=BLOCKED_PORTFOLIO_SUMMARY_RUNTIME),
        ):
            report, exit_code = deploy_hygiene.collect_migration_preflight()

        self.assertEqual(exit_code, 1)
        self.assertEqual(report['status'], deploy_hygiene.FAIL_STATUS)
        self.assertFalse(report['safe_to_continue'])
        self.assertEqual(
            report['runtime_checks']['portfolio_summary']['blocking_reasons'],
            BLOCKED_PORTFOLIO_SUMMARY_RUNTIME['blocking_reasons'],
        )

    def test_collect_deploy_smoke_check_passes_when_health_and_freshness_respond(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/health':
                return httpx.Response(
                    200,
                    json={'status': 'ok', 'database': {'connected': True}},
                    request=request,
                )
            if request.url.path == '/eod/freshness':
                return httpx.Response(
                    200,
                    json={
                        'today': {'trade_date': '2026-04-25'},
                        'latest': {'trade_date': '2026-04-24', 'updated_at': '2026-04-25T00:00:00+09:00'},
                    },
                    request=request,
                )
            if request.url.path == '/market/snapshot':
                return httpx.Response(
                    200,
                    json={
                        'as_of': '2026-04-29T10:05:12+09:00',
                        'markets': {
                            'kospi': {'value': 2642.07},
                            'kosdaq': {'value': 857.6},
                        },
                        'usd_krw': {'rate': 1444.5},
                    },
                    request=request,
                )
            if request.url.path == '/portfolio/summary':
                return httpx.Response(
                    200,
                    json={
                        'sync': {
                            'status': 'synced',
                            'message': None,
                        },
                        'accounts': [{'id': 'KIS:12345678-01'}],
                    },
                    request=request,
                )
            raise AssertionError(f'unexpected path: {request.url.path}')

        report, exit_code = deploy_hygiene.collect_deploy_smoke_check(
            base_url='http://testserver',
            transport=httpx.MockTransport(handler),
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(report['status'], deploy_hygiene.OK_STATUS)
        self.assertTrue(report['checks']['health']['ok'])
        self.assertTrue(report['checks']['eod_freshness']['ok'])
        self.assertTrue(report['checks']['market_snapshot']['ok'])
        self.assertTrue(report['checks']['portfolio_summary']['ok'])

    def test_collect_deploy_smoke_check_accepts_partial_market_snapshot_when_kis_data_is_present(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/health':
                return httpx.Response(
                    200,
                    json={'status': 'ok', 'database': {'connected': True}},
                    request=request,
                )
            if request.url.path == '/eod/freshness':
                return httpx.Response(
                    200,
                    json={
                        'today': {'trade_date': '2026-04-30'},
                        'latest': {'trade_date': '2026-04-29', 'updated_at': '2026-04-30T00:00:00+09:00'},
                    },
                    request=request,
                )
            if request.url.path == '/market/snapshot':
                return httpx.Response(
                    200,
                    json={
                        'as_of': '2026-04-30T13:15:00+09:00',
                        'status': 'partial',
                        'missing_fields': ['usd_krw.rate', 'usd_krw.effective_date'],
                        'stale_reason': 'koreaexim_quota_exhausted',
                        'markets': {
                            'kospi': {'value': 2642.07},
                            'kosdaq': {'value': 857.6},
                        },
                        'usd_krw': {
                            'rate': None,
                            'status': 'missing',
                            'source': 'unavailable',
                            'stale_reason': 'koreaexim_quota_exhausted',
                        },
                    },
                    request=request,
                )
            if request.url.path == '/portfolio/summary':
                return httpx.Response(
                    200,
                    json={
                        'sync': {
                            'status': 'synced',
                            'message': None,
                        },
                        'accounts': [{'id': 'KIS:12345678-01'}],
                    },
                    request=request,
                )
            raise AssertionError(f'unexpected path: {request.url.path}')

        report, exit_code = deploy_hygiene.collect_deploy_smoke_check(
            base_url='http://testserver',
            transport=httpx.MockTransport(handler),
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(report['checks']['market_snapshot']['snapshot_status'], 'partial')
        self.assertTrue(report['checks']['market_snapshot']['ok'])
        self.assertEqual(report['checks']['market_snapshot']['stale_reason'], 'koreaexim_quota_exhausted')
        self.assertEqual(
            report['checks']['market_snapshot']['missing_fields'],
            ['usd_krw.rate', 'usd_krw.effective_date'],
        )

    def test_collect_deploy_smoke_check_fails_when_health_is_degraded(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == '/health':
                return httpx.Response(
                    200,
                    json={'status': 'degraded', 'database': {'connected': False}},
                    request=request,
                )
            if request.url.path == '/eod/freshness':
                return httpx.Response(
                    200,
                    json={'today': {'trade_date': '2026-04-25'}, 'latest': {}},
                    request=request,
                )
            if request.url.path == '/market/snapshot':
                return httpx.Response(
                    503,
                    json={'detail': 'Market snapshot runtime is not ready.'},
                    request=request,
                )
            if request.url.path == '/portfolio/summary':
                return httpx.Response(
                    200,
                    json={
                        'sync': {
                            'status': 'not_configured',
                            'message': 'No portfolio accounts are configured.',
                        },
                        'accounts': [],
                    },
                    request=request,
                )
            raise AssertionError(f'unexpected path: {request.url.path}')

        report, exit_code = deploy_hygiene.collect_deploy_smoke_check(
            base_url='http://testserver',
            transport=httpx.MockTransport(handler),
        )

        self.assertEqual(exit_code, 1)
        self.assertEqual(report['status'], deploy_hygiene.FAIL_STATUS)
        self.assertFalse(report['checks']['health']['ok'])
        self.assertFalse(report['checks']['market_snapshot']['ok'])
        self.assertFalse(report['checks']['portfolio_summary']['ok'])
