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
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=READY_MARKET_SNAPSHOT_RUNTIME),
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
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=READY_MARKET_SNAPSHOT_RUNTIME),
        ):
            report, exit_code = deploy_hygiene.collect_migration_preflight()

        self.assertEqual(exit_code, 0)
        self.assertEqual(report['status'], deploy_hygiene.WARN_STATUS)
        self.assertEqual(report['drift']['status'], 'none')
        self.assertTrue(report['safe_to_continue'])

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
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=READY_MARKET_SNAPSHOT_RUNTIME),
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
            patch('app.internal.deploy_hygiene.market_snapshot_service.runtime_status', return_value=BLOCKED_MARKET_SNAPSHOT_RUNTIME),
        ):
            report, exit_code = deploy_hygiene.collect_migration_preflight()

        self.assertEqual(exit_code, 1)
        self.assertEqual(report['status'], deploy_hygiene.FAIL_STATUS)
        self.assertFalse(report['safe_to_continue'])
        self.assertEqual(report['runtime_checks']['market_snapshot']['blocking_reasons'], BLOCKED_MARKET_SNAPSHOT_RUNTIME['blocking_reasons'])

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
            raise AssertionError(f'unexpected path: {request.url.path}')

        report, exit_code = deploy_hygiene.collect_deploy_smoke_check(
            base_url='http://testserver',
            transport=httpx.MockTransport(handler),
        )

        self.assertEqual(exit_code, 1)
        self.assertEqual(report['status'], deploy_hygiene.FAIL_STATUS)
        self.assertFalse(report['checks']['health']['ok'])
        self.assertFalse(report['checks']['market_snapshot']['ok'])
