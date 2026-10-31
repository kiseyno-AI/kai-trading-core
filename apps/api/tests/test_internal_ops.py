import io
import json
import unittest
from contextlib import redirect_stdout
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi import FastAPI

from app.core.settings import settings
from app.internal import ops
from app.internal.http_client import InternalAPIClient
from app.main import audit_and_require_upstream_bearer_token
from app.signals.advanced_value_factor import ADVANCED_VALUE_FACTOR_SOURCE
from app.signals.beta_factor import BETA_FACTOR_SOURCE
from app.signals.composite_factor import COMPOSITE_FACTOR_SOURCE
from app.signals.low_risk_factor import LOW_RISK_SOURCE
from app.signals.market_risk_filter import MARKET_RISK_FILTER_SOURCE
from app.signals.market_risk_snapshot import MARKET_RISK_SNAPSHOT_SOURCE
from app.signals.unified_value_factor import UNIFIED_VALUE_FACTOR_SOURCE
from app.signals.quality_factor import QUALITY_FACTOR_SOURCE
from app.signals.value_factor import VALUE_FACTOR_SOURCE


class FakeDailyPipelineRunStateService:
    def start_run(self, **kwargs):
        return {'id': 7002, **kwargs, 'status': 'running', 'stages': {}}

    def start_stage(self, **kwargs):
        return {'id': 1, **kwargs, 'status': 'running'}

    def update_stage(self, **kwargs):
        return {'id': 1, **kwargs}

    def finish_stage(self, **kwargs):
        return {'id': 1, **kwargs}

    def finish_run(self, **kwargs):
        return {'id': kwargs['pipeline_run_id'], **kwargs}


class InternalAPIClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_attaches_bearer_token_for_internal_http_calls(self) -> None:
        original_token = settings.upstream_bearer_token
        settings.upstream_bearer_token = 'internal-secret'

        app = FastAPI()
        app.middleware('http')(audit_and_require_upstream_bearer_token)

        @app.post('/eod/pykrx/daily/run-if-due')
        async def run_if_due() -> dict:
            return {'ok': True}

        transport = httpx.ASGITransport(app=app)
        client = InternalAPIClient(
            base_url='http://testserver',
            bearer_token=settings.upstream_bearer_token,
            transport=transport,
        )

        try:
            response = await client.run_pykrx_daily_if_due(market_date='2026-04-08', force=True)
        finally:
            settings.upstream_bearer_token = original_token

        self.assertEqual(response, {'ok': True})

    async def test_client_preserves_query_params_for_daily_run(self) -> None:
        captured_request: httpx.Request | None = None

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_request
            captured_request = request
            return httpx.Response(200, json={'triggered': True}, request=request)

        transport = httpx.MockTransport(handler)
        client = InternalAPIClient(
            base_url='http://127.0.0.1:8000',
            bearer_token='internal-secret',
            transport=transport,
        )

        response = await client.run_pykrx_daily_if_due(
            market_date='2026-04-08',
            markets='KOSPI,KOSDAQ',
            force=True,
            refresh_universe=False,
        )

        self.assertEqual(response['triggered'], True)
        assert captured_request is not None
        self.assertEqual(captured_request.headers['authorization'], 'Bearer internal-secret')
        self.assertEqual(captured_request.url.params['market_date'], '2026-04-08')
        self.assertEqual(captured_request.url.params['markets'], 'KOSPI,KOSDAQ')
        self.assertEqual(captured_request.url.params['force'], 'true')
        self.assertEqual(captured_request.url.params['refresh_universe'], 'false')


class InternalOpsTests(unittest.TestCase):
    @staticmethod
    def _authoritative_mapping(
        *,
        resolved: list[dict] | None = None,
        unsupported_stock_codes: list[str] | None = None,
        conflicting_mappings: list[dict] | None = None,
    ) -> dict:
        return {
            'resolved': resolved or [],
            'unsupported_stock_codes': unsupported_stock_codes or [],
            'conflicting_mappings': conflicting_mappings or [],
            'mapping_authority': {
                'source': 'dart_corp_code_zip',
                'source_fetched_at': '2026-10-30T10:00:00+00:00',
                'authoritative': True,
            },
        }

    @staticmethod
    def _complete_issuance_coverage(
        *,
        stock_code: str,
        corp_code: str,
        window_start: str = '2025-10-30',
        window_end: str = '2026-10-30',
        status: str = 'complete',
    ) -> dict:
        return {
            'corp_code': corp_code,
            'stock_code': stock_code,
            'payload': {
                'evidence_coverage': {
                    'schema_version': (
                        ops.SHARE_ISSUANCE_NEGATIVE_COVERAGE_SCHEMA
                    ),
                    'rule_version': ops.SHARE_ISSUANCE_RULE_VERSION,
                    'source': 'dart_disclosure_events',
                    'corp_code': corp_code,
                    'status': status,
                    'matched_event_count': 0,
                    'pagination_completed': True,
                    'authoritative_as_of_cutoff': True,
                    'authority_policy_version': (
                        ops.SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION
                    ),
                    'late_arrival_reconciliation': (
                        'next_scan_one_day_overlap_v1'
                    ),
                    'scan_completed_at': (
                        '2026-10-30T10:00:00+00:00'
                    ),
                    'window_start': window_start,
                    'window_end': window_end,
                }
            },
        }

    def test_main_schema_drift_preflight_emits_json_and_exit_code(self) -> None:
        payload = {
            'ready': False,
            'blocking_reasons': ['Missing required table daily_signal_features.'],
            'warnings': [],
            'recommended_actions': ['Inspect schema constraints.'],
        }
        stdout = io.StringIO()
        with (
            patch('app.internal.ops.collect_schema_drift_preflight', return_value=(payload, 1)) as collect_preflight,
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(['schema-drift-preflight'])

        collect_preflight.assert_called_once_with()
        self.assertEqual(exit_code, 1)
        self.assertEqual(json.loads(stdout.getvalue()), payload)

    def test_run_pykrx_daily_if_due_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch('app.internal.ops.PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due', return_value={'triggered': True}) as run_daily,
        ):
            result = ops.run_pykrx_daily_if_due(market_date='2026-04-08', force=True)

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'triggered': True})
        self.assertEqual(run_daily.call_args.kwargs['market_date'], '2026-04-08')
        self.assertEqual(run_daily.call_args.kwargs['trigger'], 'internal_ops_cli')
        self.assertEqual(run_daily.call_args.kwargs['force'], True)

    def test_run_dart_fundamentals_daily_if_due_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.dart_fundamentals_capture_service.run_daily_incremental_refresh',
                new=AsyncMock(return_value={'triggered': True, 'run_id': 15}),
            ) as run_daily,
        ):
            result = ops.run_dart_fundamentals_daily_if_due(
                market_date='2026-04-14',
                symbol_limit=5,
                capture_limit=20,
                force=True,
                fs_div='CFS',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'triggered': True, 'run_id': 15})
        self.assertEqual(run_daily.call_args.kwargs['market_date'], '2026-04-14')
        self.assertIsNone(run_daily.call_args.kwargs['symbols'])
        self.assertEqual(run_daily.call_args.kwargs['symbol_limit'], 5)
        self.assertEqual(run_daily.call_args.kwargs['capture_limit'], 20)
        self.assertEqual(run_daily.call_args.kwargs['trigger'], 'internal_ops_cli')
        self.assertEqual(run_daily.call_args.kwargs['force'], True)

    def test_run_dart_fundamentals_daily_if_due_passes_exact_symbol_scope(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.dart_fundamentals_capture_service.run_daily_incremental_refresh',
                new=AsyncMock(
                    return_value={
                        'status': 'partial',
                        'selected_stock_codes': ['005930', '000660'],
                        'successful_stock_codes': ['005930'],
                        'retry_debt_stock_codes': ['000660'],
                    }
                ),
            ) as run_daily,
        ):
            result = ops.run_dart_fundamentals_daily_if_due(
                market_date='2026-10-30',
                symbols='005930, 000660,005930',
                capture_limit=20,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result['selected_stock_codes'], ['005930', '000660'])
        self.assertEqual(result['retry_debt_stock_codes'], ['000660'])
        self.assertEqual(
            run_daily.call_args.kwargs['symbols'],
            ['005930', '000660'],
        )
        self.assertIsNone(run_daily.call_args.kwargs['symbol_limit'])
        self.assertEqual(run_daily.call_args.kwargs['symbol_offset'], 0)

    def test_run_dart_fundamentals_daily_if_due_rejects_ambiguous_symbol_scope(self) -> None:
        for pagination in (
            {'symbol_limit': 1},
            {'symbol_offset': 1},
        ):
            with self.subTest(pagination=pagination):
                with (
                    patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
                    self.assertRaisesRegex(
                        ValueError,
                        '--symbols cannot be combined',
                    ),
                ):
                    ops.run_dart_fundamentals_daily_if_due(
                        symbols='005930',
                        **pagination,
                    )

                bootstrap_runtime.assert_not_called()

    def test_backfill_dart_audit_signals_uses_bounded_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.dart_signals_service.backfill_missing_annual_audit_signals',
                new=AsyncMock(return_value={'status': 'completed', 'audit_signals_captured': 2}),
            ) as backfill,
        ):
            result = ops.backfill_dart_audit_signals(
                market_date='2026-04-30',
                limit=2,
                after_event_id=40,
                concurrency=3,
                dry_run=False,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result['audit_signals_captured'], 2)
        self.assertEqual(backfill.call_args.kwargs['market_date'], date(2026, 4, 30))
        self.assertEqual(backfill.call_args.kwargs['limit'], 2)
        self.assertEqual(backfill.call_args.kwargs['after_event_id'], 40)
        self.assertEqual(backfill.call_args.kwargs['concurrency'], 3)
        self.assertFalse(backfill.call_args.kwargs['dry_run'])

    def test_backfill_dart_share_issuance_evidence_dry_run_freezes_sorted_manifest_and_cursor(self) -> None:
        active_symbols = [
            {'symbol': '005930'},
            {'symbol': '000660'},
            {'symbol': '035420'},
        ]
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch.object(
                ops.dart_fundamentals_capture_service,
                'active_symbol_loader',
                return_value=active_symbols,
            ) as load_active_symbols,
            patch.object(
                ops.dart_fundamentals_capture_service.client,
                'lookup_corp_codes_for_stocks',
                new=AsyncMock(),
            ) as lookup_corp_codes,
            patch.object(
                ops.dart_disclosure_sync_service,
                'sync_disclosures_for_corporations',
                new=AsyncMock(),
            ) as sync_disclosures,
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'create_sync_run',
            ) as create_audit_run,
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'complete_sync_run',
            ) as complete_audit_run,
        ):
            result = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                limit=2,
                after_stock_code='000660',
                dry_run=True,
            )
            load_active_symbols.return_value = list(reversed(active_symbols))
            reordered = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                limit=2,
                after_stock_code='000660',
                dry_run=True,
            )

        self.assertEqual(bootstrap_runtime.call_count, 2)
        lookup_corp_codes.assert_not_awaited()
        sync_disclosures.assert_not_awaited()
        create_audit_run.assert_not_called()
        complete_audit_run.assert_not_called()
        self.assertEqual(result['status'], 'dry_run')
        self.assertEqual(result['window_start'], '2025-10-30')
        self.assertEqual(result['window_end'], '2026-10-30')
        self.assertEqual(
            result['requested_stock_codes'],
            ['000660', '005930', '035420'],
        )
        self.assertEqual(result['selected_stock_codes'], ['005930', '035420'])
        self.assertEqual(result['next_after_stock_code'], '035420')
        self.assertFalse(result['has_more'])
        self.assertEqual(result['manifest_hash'], reordered['manifest_hash'])
        self.assertEqual(
            result['manifest']['requested_stock_codes'],
            result['requested_stock_codes'],
        )
        self.assertEqual(
            result['policy_version'],
            ops.DART_SHARE_ISSUANCE_OPS_POLICY_VERSION,
        )
        self.assertTrue(
            result['invariants']['manifest_stock_codes_sorted_unique']
        )

    def test_backfill_dart_share_issuance_evidence_applies_exact_market_date_window(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch.object(
                ops.dart_fundamentals_capture_service.client,
                'lookup_corp_codes_for_stocks',
                new=AsyncMock(
                    return_value=self._authoritative_mapping(
                        resolved=[
                            {
                                'stock_code': stock_code,
                                'corp_code': f'corp-{stock_code}',
                                'corp_name': stock_code,
                            }
                            for stock_code in ('000660', '005930')
                        ],
                    )
                ),
            ) as lookup_corp_codes,
            patch.object(
                ops.dart_disclosure_sync_service,
                'sync_disclosures_for_corporations',
                new=AsyncMock(
                    return_value={
                        'status': 'completed',
                        'corporations_processed': 2,
                        'dilution_positive_events': 1,
                        'dilution_negative_coverage_rows': 1,
                    }
                ),
            ) as sync_disclosures,
            patch.object(
                ops.dart_signals_service.repository,
                'list_current_share_issuance_coverage',
                return_value=[],
            ),
            patch.object(
                ops.dart_signals_service.repository,
                'list_latest_share_issuance_positive_event_dates',
                return_value={
                    'corp-000660': date(2026, 10, 29),
                    'corp-005930': date(2026, 10, 30),
                },
            ),
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'create_sync_run',
                return_value=901,
            ) as create_audit_run,
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'complete_sync_run',
            ) as complete_audit_run,
        ):
            reviewed = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                limit=2,
                stock_codes='005930,000660',
                dry_run=True,
            )
            result = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                limit=2,
                stock_codes='000660,005930',
                expected_manifest_hash=reviewed['manifest_hash'],
                dry_run=False,
            )

        self.assertEqual(bootstrap_runtime.call_count, 2)
        self.assertEqual(lookup_corp_codes.await_count, 2)
        for call in lookup_corp_codes.await_args_list:
            self.assertEqual(
                call.kwargs['stock_codes'],
                ['000660', '005930'],
            )
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['resolved_corporation_count'], 2)
        self.assertEqual(
            result['operator_buckets']['completed']['stock_codes'],
            ['000660', '005930'],
        )
        self.assertTrue(
            result['invariants']['selected_stock_codes_partition_complete']
        )
        self.assertEqual(
            sync_disclosures.await_args.kwargs['start_date'],
            date(2025, 10, 30),
        )
        self.assertEqual(
            sync_disclosures.await_args.kwargs['end_date'],
            date(2026, 10, 30),
        )
        self.assertEqual(
            sync_disclosures.await_args.kwargs['trigger'],
            ops.DART_SHARE_ISSUANCE_BACKFILL_TRIGGER,
        )
        self.assertLessEqual(
            len(sync_disclosures.await_args.kwargs['trigger']),
            32,
        )
        self.assertTrue(result['full_manifest_audit'])
        self.assertTrue(
            result['aggregate_audit']['aggregate_evidence_complete']
        )
        self.assertEqual(
            result['aggregate_audit']['classification_counts'],
            {
                'completed_positive': 2,
                'completed_negative': 0,
                'terminal_not_applicable': 0,
                'retry_debt': 0,
            },
        )
        self.assertLessEqual(
            len(create_audit_run.call_args.kwargs['trigger']),
            32,
        )
        complete_audit_run.assert_called_once()

    def test_backfill_dart_share_issuance_evidence_separates_terminal_and_retryable_mapping(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime'),
            patch.object(
                ops.dart_fundamentals_capture_service.client,
                'lookup_corp_codes_for_stocks',
                new=AsyncMock(
                    return_value=self._authoritative_mapping(
                        resolved=[
                            {
                                'stock_code': '005930',
                                'corp_code': 'corp-005930',
                                'corp_name': '005930',
                            }
                        ],
                        unsupported_stock_codes=['000660'],
                        conflicting_mappings=[
                            {
                                'stock_code': '035420',
                                'corp_codes': [],
                                'reason': 'temporary lookup failure',
                            }
                        ],
                    )
                ),
            ),
            patch.object(
                ops.dart_disclosure_sync_service,
                'sync_disclosures_for_corporations',
                new=AsyncMock(return_value={'status': 'completed'}),
            ) as sync_disclosures,
            patch.object(
                ops.dart_signals_service.repository,
                'list_current_share_issuance_coverage',
                return_value=[],
            ),
            patch.object(
                ops.dart_signals_service.repository,
                'list_latest_share_issuance_positive_event_dates',
                return_value={'corp-005930': date(2026, 10, 30)},
            ),
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'create_sync_run',
                return_value=902,
            ),
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'complete_sync_run',
            ),
        ):
            reviewed = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                limit=3,
                stock_codes='005930,000660,035420',
                dry_run=True,
            )
            result = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                limit=3,
                stock_codes='005930,000660,035420',
                expected_manifest_hash=reviewed['manifest_hash'],
                dry_run=False,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['resolved_corporation_count'], 1)
        self.assertEqual(result['lookup_error_count'], 1)
        self.assertEqual(
            result['terminal_not_applicable_stock_codes'],
            ['000660'],
        )
        self.assertFalse(result['full_manifest_audit'])
        self.assertTrue(result['durable_full_manifest_audit_complete'])
        self.assertFalse(
            result['aggregate_audit']['aggregate_evidence_complete']
        )
        self.assertEqual(
            result['aggregate_audit']['classification_stock_codes'][
                'retry_debt'
            ],
            ['035420'],
        )
        self.assertEqual(result['retryable_unmapped_stock_codes'], ['035420'])
        self.assertEqual(result['retry_debt_stock_codes'], ['035420'])
        self.assertEqual(
            result['operator_buckets']['terminal_not_applicable']['count'],
            1,
        )
        self.assertEqual(
            result['operator_buckets']['retryable_unmapped']['count'],
            1,
        )
        self.assertTrue(
            result['invariants'][
                'terminal_not_applicable_excluded_from_retry_debt'
            ]
        )
        self.assertEqual(
            [item['stock_code'] for item in sync_disclosures.await_args.kwargs['corporations']],
            ['005930'],
        )

    def test_backfill_dart_share_issuance_evidence_treats_all_terminal_mappings_as_completed_audit(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime'),
            patch.object(
                ops.dart_fundamentals_capture_service.client,
                'lookup_corp_codes_for_stocks',
                new=AsyncMock(
                    return_value=self._authoritative_mapping(
                        unsupported_stock_codes=['000660'],
                    )
                ),
            ),
            patch.object(
                ops.dart_disclosure_sync_service,
                'sync_disclosures_for_corporations',
                new=AsyncMock(),
            ) as sync_disclosures,
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'create_sync_run',
                return_value=903,
            ),
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'complete_sync_run',
            ),
        ):
            reviewed = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                stock_codes='000660',
                dry_run=True,
            )
            result = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                stock_codes='000660',
                expected_manifest_hash=reviewed['manifest_hash'],
                dry_run=False,
            )

        sync_disclosures.assert_not_awaited()
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['retry_debt_stock_codes'], [])
        self.assertEqual(
            result['terminal_not_applicable_stock_codes'],
            ['000660'],
        )
        self.assertTrue(result['full_manifest_audit'])
        self.assertTrue(
            result['aggregate_audit']['aggregate_evidence_complete']
        )

    def test_backfill_dart_share_issuance_final_page_builds_durable_exact_aggregate_audit(self) -> None:
        stock_codes = [f'{index:06d}' for index in range(1, 8)]

        async def lookup_mapping(*, stock_codes: list[str]) -> dict:
            resolved = [
                {
                    'stock_code': stock_code,
                    'corp_code': f'corp-{stock_code}',
                    'corp_name': stock_code,
                }
                for stock_code in stock_codes
                if stock_code not in {'000003', '000007'}
            ]
            return self._authoritative_mapping(
                resolved=resolved,
                unsupported_stock_codes=(
                    ['000003'] if '000003' in stock_codes else []
                ),
                conflicting_mappings=(
                    [
                        {
                            'stock_code': '000007',
                            'reason': 'conflicting_corp_codes',
                        }
                    ]
                    if '000007' in stock_codes
                    else []
                ),
            )

        valid_negative = self._complete_issuance_coverage(
            stock_code='000002',
            corp_code='corp-000002',
        )
        stale_negative = self._complete_issuance_coverage(
            stock_code='000004',
            corp_code='corp-000004',
            window_end='2026-10-29',
        )
        segment_negative = self._complete_issuance_coverage(
            stock_code='000005',
            corp_code='corp-000005',
            status='segment',
        )
        malformed_negative = self._complete_issuance_coverage(
            stock_code='000006',
            corp_code='corp-000006',
        )
        malformed_negative['payload']['evidence_coverage'][
            'authority_policy_version'
        ] = 'obsolete-policy'

        with (
            patch('app.internal.ops.bootstrap_runtime'),
            patch.object(
                ops.dart_fundamentals_capture_service.client,
                'lookup_corp_codes_for_stocks',
                new=AsyncMock(side_effect=lookup_mapping),
            ) as lookup_corp_codes,
            patch.object(
                ops.dart_disclosure_sync_service,
                'sync_disclosures_for_corporations',
                new=AsyncMock(return_value={'status': 'completed'}),
            ),
            patch.object(
                ops.dart_signals_service.repository,
                'list_current_share_issuance_coverage',
                return_value=[
                    valid_negative,
                    stale_negative,
                    segment_negative,
                    malformed_negative,
                ],
            ),
            patch.object(
                ops.dart_signals_service.repository,
                'list_latest_share_issuance_positive_event_dates',
                return_value={'corp-000001': date(2026, 10, 30)},
            ),
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'create_sync_run',
                return_value=904,
            ) as create_audit_run,
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'complete_sync_run',
            ) as complete_audit_run,
        ):
            reviewed = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                limit=4,
                stock_codes=','.join(stock_codes),
                dry_run=True,
            )
            first_page = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                limit=4,
                stock_codes=','.join(stock_codes),
                expected_manifest_hash=reviewed['manifest_hash'],
                dry_run=False,
            )
            self.assertTrue(first_page['has_more'])
            self.assertIsNone(first_page['aggregate_audit'])
            create_audit_run.assert_not_called()

            final_page = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                limit=4,
                stock_codes=','.join(stock_codes),
                after_stock_code=first_page['next_after_stock_code'],
                expected_manifest_hash=reviewed['manifest_hash'],
                dry_run=False,
            )

        self.assertEqual(lookup_corp_codes.await_count, 3)
        self.assertEqual(
            lookup_corp_codes.await_args_list[-1].kwargs['stock_codes'],
            stock_codes,
        )
        aggregate = final_page['aggregate_audit']
        self.assertEqual(final_page['status'], 'partial')
        self.assertFalse(final_page['full_manifest_audit'])
        self.assertTrue(
            final_page['durable_full_manifest_audit_complete']
        )
        self.assertTrue(aggregate['durable_audit_complete'])
        self.assertFalse(aggregate['aggregate_evidence_complete'])
        self.assertEqual(
            aggregate['classification_stock_codes'],
            {
                'completed_positive': ['000001'],
                'completed_negative': ['000002'],
                'terminal_not_applicable': ['000003'],
                'retry_debt': ['000004', '000005', '000006', '000007'],
            },
        )
        self.assertEqual(
            [
                item['stock_code']
                for item in aggregate['outcome_manifest']['outcomes']
            ],
            stock_codes,
        )
        self.assertEqual(
            aggregate['outcome_manifest_sha256'],
            ops._canonical_json_sha256(aggregate['outcome_manifest']),
        )
        self.assertEqual(
            create_audit_run.call_args.kwargs['trigger'],
            ops.DART_SHARE_ISSUANCE_AUDIT_TRIGGER,
        )
        self.assertLessEqual(
            len(create_audit_run.call_args.kwargs['trigger']),
            32,
        )
        complete_audit_run.assert_called_once()
        completed_metadata = complete_audit_run.call_args.kwargs[
            'run_metadata'
        ]['aggregate_audit']
        self.assertEqual(
            completed_metadata['outcome_manifest_sha256'],
            aggregate['outcome_manifest_sha256'],
        )
        self.assertTrue(completed_metadata['durable_audit_complete'])

    def test_backfill_dart_share_issuance_aggregate_audit_persistence_failure_is_fail_closed(self) -> None:
        mapping = self._authoritative_mapping(
            resolved=[
                {
                    'stock_code': '005930',
                    'corp_code': 'corp-005930',
                    'corp_name': 'Samsung',
                }
            ]
        )
        with (
            patch('app.internal.ops.bootstrap_runtime'),
            patch.object(
                ops.dart_fundamentals_capture_service.client,
                'lookup_corp_codes_for_stocks',
                new=AsyncMock(return_value=mapping),
            ),
            patch.object(
                ops.dart_disclosure_sync_service,
                'sync_disclosures_for_corporations',
                new=AsyncMock(return_value={'status': 'completed'}),
            ),
            patch.object(
                ops.dart_signals_service.repository,
                'list_current_share_issuance_coverage',
                return_value=[],
            ),
            patch.object(
                ops.dart_signals_service.repository,
                'list_latest_share_issuance_positive_event_dates',
                return_value={'corp-005930': date(2026, 10, 30)},
            ),
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'create_sync_run',
                return_value=905,
            ) as create_audit_run,
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'complete_sync_run',
                side_effect=RuntimeError('audit database unavailable'),
            ) as complete_audit_run,
        ):
            reviewed = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                stock_codes='005930',
                dry_run=True,
            )
            result = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                stock_codes='005930',
                expected_manifest_hash=reviewed['manifest_hash'],
                dry_run=False,
            )

        self.assertEqual(result['status'], 'failed')
        self.assertFalse(result['full_manifest_audit'])
        self.assertFalse(
            result['aggregate_audit']['aggregate_evidence_complete']
        )
        self.assertFalse(
            result['aggregate_audit']['durable_audit_complete']
        )
        self.assertEqual(
            result['aggregate_audit']['persistence_status'],
            'failed',
        )
        self.assertIn(
            'audit database unavailable',
            result['aggregate_audit']['persistence_error'],
        )
        create_audit_run.assert_called_once()
        complete_audit_run.assert_called_once()
        self.assertEqual(result['aggregate_audit']['audit_run_id'], 905)

    def test_full_universe_audit_uses_dedicated_activation_trigger(self) -> None:
        aggregate = {
            'window_start': '2025-10-30',
            'window_end': '2026-10-30',
            'requested_count': 1,
            'audit_scope': (
                ops.DART_SHARE_ISSUANCE_AUDIT_SCOPE_GLOBAL_ACTIVATION
            ),
            'aggregate_evidence_ready': True,
        }
        with (
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'create_sync_run',
                return_value=906,
            ) as create_audit_run,
            patch.object(
                ops.dart_disclosure_sync_service.repository,
                'complete_sync_run',
            ) as complete_audit_run,
        ):
            result = ops._persist_dart_share_issuance_aggregate_audit(
                aggregate
            )

        self.assertEqual(
            create_audit_run.call_args.kwargs['trigger'],
            ops.DART_SHARE_ISSUANCE_ACTIVATION_AUDIT_TRIGGER,
        )
        self.assertTrue(result['durable_audit_complete'])
        self.assertEqual(result['audit_run_id'], 906)
        complete_audit_run.assert_called_once()

    def test_full_universe_review_manifest_binds_activation_scope(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime'),
            patch.object(
                ops.dart_fundamentals_capture_service,
                'active_symbol_loader',
                return_value=[
                    {'symbol': '005930'},
                    {'symbol': '000660'},
                ],
            ),
        ):
            result = ops.backfill_dart_share_issuance_evidence(
                market_date='2026-10-30',
                dry_run=True,
            )

        self.assertEqual(
            result['manifest_source'],
            ops.DART_SHARE_ISSUANCE_MANIFEST_SOURCE_ACTIVE_UNIVERSE,
        )
        self.assertEqual(
            result['audit_scope'],
            ops.DART_SHARE_ISSUANCE_AUDIT_SCOPE_GLOBAL_ACTIVATION,
        )
        self.assertEqual(
            result['manifest']['manifest_source'],
            result['manifest_source'],
        )
        self.assertEqual(
            result['manifest']['audit_scope'], result['audit_scope']
        )
        self.assertEqual(
            result['manifest_hash'],
            ops._canonical_json_sha256(
                {
                    key: value
                    for key, value in result['manifest'].items()
                    if key not in {'hash_algorithm', 'manifest_hash'}
                }
            ),
        )

    def test_backfill_dart_share_issuance_evidence_requires_exact_reviewed_manifest_before_provider_calls(self) -> None:
        cases = (
            (None, 'expected_manifest_hash_required', None),
            ('0' * 64, 'expected_manifest_hash_mismatch', False),
        )
        for expected_hash, expected_reason, expected_invariant in cases:
            with self.subTest(reason=expected_reason):
                with (
                    patch('app.internal.ops.bootstrap_runtime'),
                    patch.object(
                        ops.dart_fundamentals_capture_service.client,
                        'lookup_corp_codes_for_stocks',
                        new=AsyncMock(),
                    ) as lookup_corp_codes,
                    patch.object(
                        ops.dart_disclosure_sync_service,
                        'sync_disclosures_for_corporations',
                        new=AsyncMock(),
                    ) as sync_disclosures,
                ):
                    result = ops.backfill_dart_share_issuance_evidence(
                        market_date='2026-10-30',
                        stock_codes='000660,005930',
                        expected_manifest_hash=expected_hash,
                        dry_run=False,
                    )

                lookup_corp_codes.assert_not_awaited()
                sync_disclosures.assert_not_awaited()
                self.assertEqual(result['status'], 'blocked')
                self.assertEqual(result['reason'], expected_reason)
                self.assertIs(
                    result['invariants']['manifest_hash_matches_expected'],
                    expected_invariant,
                )

    def test_cli_dart_audit_signals_backfill_outputs_resume_metadata(self) -> None:
        payload = {
            'status': 'dry_run',
            'candidate_count': 2,
            'next_after_event_id': 42,
            'has_more_after_cursor': True,
        }
        stdout = io.StringIO()
        with (
            patch(
                'app.internal.ops.backfill_dart_audit_signals',
                return_value=payload,
            ) as backfill,
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(
                [
                    'dart-audit-signals-backfill',
                    '--market-date',
                    '2026-04-30',
                    '--limit',
                    '2',
                    '--after-event-id',
                    '40',
                    '--concurrency',
                    '3',
                    '--dry-run',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), payload)
        backfill.assert_called_once_with(
            market_date='2026-04-30',
            limit=2,
            after_event_id=40,
            concurrency=3,
            dry_run=True,
        )

    def test_cli_dart_audit_signals_backfill_defaults_to_dry_run(self) -> None:
        payload = {'status': 'dry_run', 'candidate_count': 0}
        with patch(
            'app.internal.ops.backfill_dart_audit_signals',
            return_value=payload,
        ) as backfill:
            exit_code = ops.main(
                [
                    'dart-audit-signals-backfill',
                    '--market-date',
                    '2026-04-30',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertTrue(backfill.call_args.kwargs['dry_run'])
        self.assertEqual(backfill.call_args.kwargs['concurrency'], 4)

    def test_cli_dart_audit_signals_backfill_rejects_invalid_concurrency(self) -> None:
        for concurrency in ('0', '9'):
            with self.subTest(concurrency=concurrency):
                with self.assertRaises(SystemExit):
                    ops._build_parser().parse_args(
                        [
                            'dart-audit-signals-backfill',
                            '--market-date',
                            '2026-04-30',
                            '--concurrency',
                            concurrency,
                        ]
                    )

    def test_cli_dart_audit_signals_backfill_requires_apply_for_writes(self) -> None:
        payload = {'status': 'completed', 'audit_signals_captured': 1}
        with patch(
            'app.internal.ops.backfill_dart_audit_signals',
            return_value=payload,
        ) as backfill:
            exit_code = ops.main(
                [
                    'dart-audit-signals-backfill',
                    '--market-date',
                    '2026-04-30',
                    '--apply',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertFalse(backfill.call_args.kwargs['dry_run'])

    def test_cli_dart_audit_signals_backfill_requires_market_date(self) -> None:
        with self.assertRaises(SystemExit):
            ops._build_parser().parse_args(['dart-audit-signals-backfill'])

    def test_cli_dart_audit_signals_backfill_returns_nonzero_for_partial_or_gaps(self) -> None:
        for status in ('partial', 'completed_with_gaps'):
            with self.subTest(status=status):
                stdout = io.StringIO()
                with (
                    patch(
                        'app.internal.ops.backfill_dart_audit_signals',
                        return_value={'status': status},
                    ),
                    redirect_stdout(stdout),
                ):
                    exit_code = ops.main(
                        [
                            'dart-audit-signals-backfill',
                            '--market-date',
                            '2026-04-30',
                        ]
                    )

                self.assertEqual(exit_code, 2)
                self.assertEqual(json.loads(stdout.getvalue()), {'status': status})

    def test_cli_dart_share_issuance_evidence_backfill_defaults_to_dry_run(self) -> None:
        payload = {'status': 'dry_run', 'selected_count': 2}
        stdout = io.StringIO()
        with (
            patch(
                'app.internal.ops.backfill_dart_share_issuance_evidence',
                return_value=payload,
            ) as backfill,
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(
                [
                    'dart-share-issuance-evidence-backfill',
                    '--market-date',
                    '2026-10-30',
                    '--limit',
                    '2',
                    '--stock-codes',
                    '005930,000660',
                    '--after-stock-code',
                    '000660',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), payload)
        backfill.assert_called_once_with(
            market_date='2026-10-30',
            limit=2,
            stock_codes='005930,000660',
            after_stock_code='000660',
            expected_manifest_hash=None,
            dry_run=True,
        )

    def test_cli_dart_share_issuance_evidence_backfill_returns_nonzero_for_retry_debt(self) -> None:
        stdout = io.StringIO()
        with (
            patch(
                'app.internal.ops.backfill_dart_share_issuance_evidence',
                return_value={'status': 'partial', 'retry_debt_stock_codes': ['000660']},
            ),
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(
                [
                    'dart-share-issuance-evidence-backfill',
                    '--market-date',
                    '2026-10-30',
                    '--apply',
                    '--expected-manifest-hash',
                    'a' * 64,
                ]
            )

        self.assertEqual(exit_code, 2)

    def test_run_next_open_execution_if_due_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.should_delegate_execution',
                return_value=False,
            ),
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.should_manage_delegated_authority',
                return_value=False,
            ),
            patch(
                'app.internal.ops.signal_decider_next_open_execution_service.run_market_open_execution_if_due',
                new=AsyncMock(return_value={'summary': {'triggered': True, 'selected_count': 1}}),
            ) as run_execution,
        ):
            result = ops.run_next_open_execution_if_due(
                target_trade_date='2026-04-30',
                dry_run=False,
                force=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result['summary']['selected_count'], 1)
        self.assertEqual(run_execution.call_args.kwargs['target_trade_date'], date(2026, 4, 30))
        self.assertEqual(run_execution.call_args.kwargs['dry_run'], False)
        self.assertEqual(run_execution.call_args.kwargs['trigger'], 'internal_ops_cli')
        self.assertEqual(run_execution.call_args.kwargs['force'], True)

    def test_run_next_open_execution_if_due_scopes_current_run_to_one_resolved_trade_date(self) -> None:
        resolved_target_date = date(2026, 4, 30)
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.signal_decider_next_open_execution_service._resolve_target_trade_date',
                return_value=resolved_target_date,
            ) as resolve_target_date,
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.should_delegate_execution',
                return_value=False,
            ),
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.should_manage_delegated_authority',
                return_value=False,
            ) as should_manage,
            patch(
                'app.internal.ops.signal_decider_next_open_execution_service.run_market_open_execution_if_due',
                new=AsyncMock(return_value={'summary': {'triggered': True, 'selected_count': 1}}),
            ) as run_execution,
        ):
            result = ops.run_next_open_execution_if_due(
                target_trade_date=None,
                dry_run=False,
                force=False,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result['summary']['selected_count'], 1)
        resolve_target_date.assert_called_once_with(None)
        should_manage.assert_called_once_with(target_trade_date=resolved_target_date)
        self.assertEqual(run_execution.call_args.kwargs['target_trade_date'], resolved_target_date)

    def test_run_next_open_execution_if_due_dispatches_managed_authority_for_resolved_trade_date(self) -> None:
        resolved_target_date = date(2026, 4, 30)
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.signal_decider_next_open_execution_service._resolve_target_trade_date',
                return_value=resolved_target_date,
            ) as resolve_target_date,
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.should_delegate_execution',
                return_value=False,
            ),
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.should_manage_delegated_authority',
                return_value=True,
            ) as should_manage,
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.dispatch_next_open_plan',
                new=AsyncMock(return_value={'summary': {'delegated_to_exec': True, 'submitted_count': 1}}),
            ) as dispatch_exec,
        ):
            result = ops.run_next_open_execution_if_due(
                target_trade_date=None,
                dry_run=False,
                force=False,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertTrue(result['summary']['delegated_to_exec'])
        resolve_target_date.assert_called_once_with(None)
        should_manage.assert_called_once_with(target_trade_date=resolved_target_date)
        self.assertEqual(dispatch_exec.call_args.kwargs['target_trade_date'], resolved_target_date)

    def test_run_intraday_auto_sell_if_due_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.should_delegate_execution',
                return_value=False,
            ),
            patch(
                'app.internal.ops.signal_decider_intraday_auto_sell_service.run_intraday_auto_sell_if_due',
                new=AsyncMock(return_value={'summary': {'triggered': True, 'selected_count': 1}}),
            ) as run_execution,
        ):
            result = ops.run_intraday_auto_sell_if_due(
                account_reference='KIS:12345678-01',
                symbol='005930',
                dry_run=False,
                force=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result['summary']['selected_count'], 1)
        self.assertEqual(run_execution.call_args.kwargs['account_reference'], 'KIS:12345678-01')
        self.assertEqual(run_execution.call_args.kwargs['symbol'], '005930')
        self.assertEqual(run_execution.call_args.kwargs['dry_run'], False)
        self.assertEqual(run_execution.call_args.kwargs['trigger'], 'internal_ops_cli')
        self.assertEqual(run_execution.call_args.kwargs['force'], True)

    def test_run_next_open_execution_if_due_delegates_to_exec_when_enabled(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch('app.internal.ops.signal_decider_exec_integration_service.should_delegate_execution', return_value=True),
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.dispatch_next_open_plan',
                new=AsyncMock(return_value={'summary': {'delegated_to_exec': True, 'submitted_count': 1}}),
            ) as dispatch_exec,
        ):
            result = ops.run_next_open_execution_if_due(
                target_trade_date='2026-04-30',
                dry_run=False,
                force=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertTrue(result['summary']['delegated_to_exec'])
        self.assertEqual(dispatch_exec.call_args.kwargs['target_trade_date'], date(2026, 4, 30))
        self.assertTrue(dispatch_exec.call_args.kwargs['refresh_plan'])
        self.assertEqual(dispatch_exec.call_args.kwargs['dry_run'], False)
        self.assertEqual(dispatch_exec.call_args.kwargs['trigger'], 'internal_ops_cli')
        self.assertEqual(dispatch_exec.call_args.kwargs['force'], True)

    def test_run_intraday_auto_sell_if_due_delegates_to_exec_when_enabled(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch('app.internal.ops.signal_decider_exec_integration_service.should_delegate_execution', return_value=True),
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.list_intraday_exec_watch',
                new=AsyncMock(return_value={'summary': {'delegated_to_exec': True, 'items_total': 1}}),
            ) as delegated_watch,
        ):
            result = ops.run_intraday_auto_sell_if_due(
                account_reference='KIS:12345678-01',
                symbol='005930',
                dry_run=False,
                force=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertTrue(result['summary']['delegated_to_exec'])
        self.assertEqual(delegated_watch.call_args.kwargs['account_reference'], 'KIS:12345678-01')
        self.assertEqual(delegated_watch.call_args.kwargs['symbol'], '005930')
        self.assertTrue(delegated_watch.call_args.kwargs['refresh'])
        self.assertEqual(delegated_watch.call_args.kwargs['trigger'], 'internal_ops_cli')

    def test_replay_decider_candidate_rules_uses_direct_service_call(self) -> None:
        payload = {'schema_version': 'decider_replay_v1', 'summary': {'baseline_trade_count': 1}}
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch('app.internal.ops.decider_replay_service.replay', return_value=payload) as replay,
        ):
            result = ops.replay_decider_candidate_rules(
                market_date_from='2026-06-15',
                market_date_to='2026-06-19',
                agent_name='short-term-decider',
                rule_config_json='{"rules":[]}',
                symbols='005930,000660',
                policy_versions='signal_decider_policy_v1,signal_decider_policy_v2',
                fee_bps='1',
                slippage_bps='2',
                notional_krw='1000000',
                horizon_days=5,
                top_examples_limit=3,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, payload)
        self.assertEqual(replay.call_args.kwargs['market_date_from'], '2026-06-15')
        self.assertEqual(replay.call_args.kwargs['market_date_to'], '2026-06-19')
        self.assertEqual(replay.call_args.kwargs['agent_name'], 'short-term-decider')
        self.assertEqual(replay.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(
            replay.call_args.kwargs['policy_versions'],
            ['signal_decider_policy_v1', 'signal_decider_policy_v2'],
        )
        self.assertEqual(replay.call_args.kwargs['fee_bps'], '1')
        self.assertEqual(replay.call_args.kwargs['slippage_bps'], '2')
        self.assertEqual(replay.call_args.kwargs['top_examples_limit'], 3)

    def test_replay_short_term_wait_conflict_gate_uses_direct_service_call(self) -> None:
        payload = {
            'schema_version': 'short_term_wait_conflict_gate_replay_v1',
            'summary': {'baseline_trade_count': 2},
        }
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch('app.internal.ops.decider_replay_service.replay_wait_conflict_gate', return_value=payload) as replay,
        ):
            result = ops.replay_short_term_wait_conflict_gate(
                market_date_from='2026-06-29',
                market_date_to='2026-07-03',
                symbols='005960,013580',
                fee_bps='1',
                slippage_bps='2',
                notional_krw='1000000',
                gate_mode='hard_block',
                probe_quantity=3,
                top_examples_limit=4,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, payload)
        self.assertEqual(replay.call_args.kwargs['market_date_from'], '2026-06-29')
        self.assertEqual(replay.call_args.kwargs['market_date_to'], '2026-07-03')
        self.assertEqual(replay.call_args.kwargs['completed_weeks'], 4)
        self.assertIsNone(replay.call_args.kwargs['as_of_date_kst'])
        self.assertEqual(replay.call_args.kwargs['symbols'], ['005960', '013580'])
        self.assertEqual(replay.call_args.kwargs['fee_bps'], '1')
        self.assertEqual(replay.call_args.kwargs['slippage_bps'], '2')
        self.assertEqual(replay.call_args.kwargs['gate_mode'], 'hard_block')
        self.assertEqual(replay.call_args.kwargs['probe_quantity'], 3)
        self.assertEqual(replay.call_args.kwargs['top_examples_limit'], 4)

    def test_replay_short_term_wait_conflict_gate_supports_reproducible_rolling_window(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime'),
            patch(
                'app.internal.ops.decider_replay_service.replay_wait_conflict_gate',
                return_value={'schema_version': 'short_term_wait_conflict_gate_replay_v1'},
            ) as replay,
        ):
            ops.replay_short_term_wait_conflict_gate(
                completed_weeks=6,
                as_of_date_kst='2026-09-06',
                gate_mode='downsize_probe',
                probe_quantity=7,
            )

        self.assertIsNone(replay.call_args.kwargs['market_date_from'])
        self.assertIsNone(replay.call_args.kwargs['market_date_to'])
        self.assertEqual(replay.call_args.kwargs['completed_weeks'], 6)
        self.assertEqual(replay.call_args.kwargs['as_of_date_kst'], '2026-09-06')
        self.assertEqual(replay.call_args.kwargs['gate_mode'], 'downsize_probe')
        self.assertEqual(replay.call_args.kwargs['probe_quantity'], 7)

    def test_cli_wait_conflict_replay_forwards_requested_probe_quantity(self) -> None:
        payload = {'schema_version': 'short_term_wait_conflict_gate_replay_v1'}
        stdout = io.StringIO()
        with (
            patch(
                'app.internal.ops.replay_short_term_wait_conflict_gate',
                return_value=payload,
            ) as replay,
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(
                [
                    'agent-short-term-wait-conflict-replay',
                    '--gate-mode',
                    'downsize_probe',
                    '--probe-quantity',
                    '9',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), payload)
        self.assertEqual(replay.call_args.kwargs['gate_mode'], 'downsize_probe')
        self.assertEqual(replay.call_args.kwargs['probe_quantity'], 9)

    def test_compute_trending_ranks_uses_direct_store_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ) as compute_ranks,
        ):
            result = ops.compute_trending_ranks(market_date='2026-04-10', periods='1d,20d')

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'trending_ranks_persisted': 3})
        self.assertEqual(compute_ranks.call_args.kwargs['market_date'], '2026-04-10')
        self.assertEqual(compute_ranks.call_args.kwargs['periods'], ['1d', '20d'])
        self.assertEqual(compute_ranks.call_args.kwargs['source'], 'internal_ops_cli')

    def test_collect_kis_daily_signal_features_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_signal_feature_service.refresh_for_market_date',
                new=AsyncMock(return_value={'features_persisted': 2}),
            ) as collect_features,
        ):
            result = ops.collect_kis_daily_signal_features(
                market_date='2026-04-14',
                sources='volume-power,overtime-volume',
                max_pages=2,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'features_persisted': 2})
        self.assertEqual(collect_features.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(collect_features.call_args.kwargs['sources'], ['volume-power', 'overtime-volume'])
        self.assertEqual(collect_features.call_args.kwargs['max_pages'], 2)

    def test_collect_kis_intraday_snapshots_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.kis_intraday_snapshot_service.collect_snapshots',
                new=AsyncMock(return_value={'snapshots_persisted': 1}),
            ) as collect_snapshots,
        ):
            result = ops.collect_kis_intraday_snapshots(
                symbols='005930,000660',
                market_code='J',
                max_ranking_pages=1,
                max_news_items=2,
                include_news=False,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'snapshots_persisted': 1})
        self.assertEqual(collect_snapshots.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(collect_snapshots.call_args.kwargs['market_code'], 'J')
        self.assertEqual(collect_snapshots.call_args.kwargs['max_ranking_pages'], 1)
        self.assertEqual(collect_snapshots.call_args.kwargs['max_news_items'], 2)
        self.assertEqual(collect_snapshots.call_args.kwargs['include_news'], False)

    def test_collect_daily_piotroski_filter_results_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_piotroski_filter_service.rebuild_for_market_date',
                return_value={'symbols_evaluated': 2},
            ) as collect_filter,
        ):
            result = ops.collect_daily_piotroski_filter_results(market_date='2026-04-14')

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'symbols_evaluated': 2})
        self.assertEqual(collect_filter.call_args.kwargs['market_date'], '2026-04-14')

    def test_list_daily_pipeline_stages_uses_stage_service(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_pipeline_stage_service.describe',
                return_value={'stage_order': ['price_collection']},
            ) as describe_stages,
        ):
            result = ops.list_daily_pipeline_stages()

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'stage_order': ['price_collection']})
        describe_stages.assert_called_once_with()

    def test_run_daily_pipeline_stages_uses_stage_service(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_pipeline_stage_service.run',
                return_value={'resolved_stages': ['technical_snapshots']},
            ) as run_stages,
        ):
            result = ops.run_daily_pipeline_stages(
                market_date='2026-04-14',
                stages='technical_snapshots,signal_decider',
                symbols='005930,000660',
                markets='KOSPI,KOSDAQ',
                force=True,
                fs_div='CFS',
                symbol_limit=25,
                symbol_offset=5,
                capture_limit=100,
                retry_cycles=2,
                feedback_max_cycles=4,
                feedback_outer_batch_size=12,
                reuse_existing_feedback=False,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'resolved_stages': ['technical_snapshots']})
        self.assertEqual(run_stages.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(run_stages.call_args.kwargs['stages'], ['technical_snapshots', 'signal_decider'])
        self.assertEqual(run_stages.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(run_stages.call_args.kwargs['markets'], ['KOSPI', 'KOSDAQ'])
        self.assertEqual(run_stages.call_args.kwargs['feedback_max_cycles'], 4)
        self.assertFalse(run_stages.call_args.kwargs['reuse_existing_feedback'])

    def test_compute_signal_decider_evaluations_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                return_value={'ready': True, 'status': 'ready'},
            ) as inspect_prerequisites,
            patch(
                'app.internal.ops.signal_decider_loop_service.compute_recent_evaluations',
                return_value={'summary': {'evaluations_upserted': 4}},
            ) as compute_evaluations,
        ):
            result = ops.compute_signal_decider_evaluations(
                market_date='2026-04-30',
                lookback_days=21,
                symbols='005930,000660',
                horizons='1,5,10',
                evaluation_version='signal_decider_eval_v1',
                force_recompute=True,
            )

        bootstrap_runtime.assert_called_once_with()
        inspect_prerequisites.assert_called_once_with(
            market_date='2026-04-30',
            symbols=['005930', '000660'],
        )
        self.assertEqual(result, {'summary': {'evaluations_upserted': 4}})
        self.assertEqual(compute_evaluations.call_args.kwargs['as_of_market_date'], '2026-04-30')
        self.assertEqual(compute_evaluations.call_args.kwargs['lookback_days'], 21)
        self.assertEqual(compute_evaluations.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_evaluations.call_args.kwargs['horizons'], [1, 5, 10])
        self.assertEqual(compute_evaluations.call_args.kwargs['evaluation_version'], 'signal_decider_eval_v1')
        self.assertTrue(compute_evaluations.call_args.kwargs['force_recompute'])

    def test_refresh_signal_decider_calendar_reviews_uses_dedicated_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.signal_decider_calendar_review_service.refresh_recent_reviews',
                return_value={'summary': {'review_rows_upserted': 8}},
            ) as refresh_reviews,
        ):
            result = ops.refresh_signal_decider_calendar_reviews(
                market_date='2026-04-30',
                lookback_days=14,
                symbols='005930,000660',
                horizons='1,5,10',
                primary_horizons='3,5',
                evaluation_version='signal_decider_eval_v1',
                force_recompute=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'summary': {'review_rows_upserted': 8}})
        self.assertEqual(refresh_reviews.call_args.kwargs['as_of_market_date'], '2026-04-30')
        self.assertEqual(refresh_reviews.call_args.kwargs['lookback_days'], 14)
        self.assertEqual(refresh_reviews.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(refresh_reviews.call_args.kwargs['horizons'], [1, 5, 10])
        self.assertEqual(refresh_reviews.call_args.kwargs['primary_horizons'], [3, 5])
        self.assertEqual(refresh_reviews.call_args.kwargs['evaluation_version'], 'signal_decider_eval_v1')
        self.assertTrue(refresh_reviews.call_args.kwargs['force_recompute'])

    def test_repair_signal_decider_evaluation_coverage_uses_maintenance_service(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.signal_decider_evaluation_maintenance_service.repair_coverage_range',
                return_value={'status': 'completed'},
            ) as repair_coverage,
        ):
            result = ops.repair_signal_decider_evaluation_coverage(
                market_date_from='2026-04-01',
                market_date_to='2026-04-30',
                symbols='005930,000660',
                horizons='1,3,5',
                primary_horizons='5',
                evaluation_version='signal_decider_eval_v1',
                force_recompute=True,
                refresh_calendar=False,
                fail_on_mature_gaps=False,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'status': 'completed'})
        self.assertEqual(repair_coverage.call_args.kwargs['market_date_from'], '2026-04-01')
        self.assertEqual(repair_coverage.call_args.kwargs['market_date_to'], '2026-04-30')
        self.assertEqual(repair_coverage.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(repair_coverage.call_args.kwargs['horizons'], [1, 3, 5])
        self.assertEqual(repair_coverage.call_args.kwargs['primary_horizons'], [5])
        self.assertEqual(repair_coverage.call_args.kwargs['evaluation_version'], 'signal_decider_eval_v1')
        self.assertTrue(repair_coverage.call_args.kwargs['force_recompute'])
        self.assertFalse(repair_coverage.call_args.kwargs['refresh_calendar'])
        self.assertFalse(repair_coverage.call_args.kwargs['fail_on_mature_gaps'])

    def test_run_agent_feedback_retry_cycles_passes_explicit_symbols_to_retry_service(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.agent_feedback_retry_service.run_retry_cycles',
                return_value={'completed': True, 'final_pending': 0},
            ) as run_retry_cycles,
        ):
            result = ops.run_agent_feedback_retry_cycles(
                market_date='2026-04-14',
                symbols='005930,000660',
                roles='technical-summarizer',
                feedback_scope_mode='broad_feedback_universe',
                max_cycles=2,
                outer_batch_size=10,
                reuse_existing=False,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'completed': True, 'final_pending': 0})
        config = run_retry_cycles.call_args.kwargs['config']
        self.assertEqual(config.market_date.isoformat(), '2026-04-14')
        self.assertEqual(config.symbols, ('005930', '000660'))
        self.assertEqual(config.roles, ('technical-summarizer',))
        self.assertEqual(config.feedback_scope_mode, 'broad_feedback_universe')
        self.assertEqual(config.max_cycles, 2)
        self.assertEqual(config.outer_batch_size, 10)
        self.assertFalse(config.reuse_existing)

    def test_get_agent_feedback_data_gap_summary_passes_filters_to_report_service(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.agent_context_missing_data_report_service.get_summary',
                return_value={'summary': {'feedback_rows_matched': 2}},
            ) as get_summary,
        ):
            result = ops.get_agent_feedback_data_gap_summary(
                market_date_from='2026-05-05',
                market_date_to='2026-05-06',
                symbols='005930,000660',
                agent_name='technical-summarizer',
                categories='factor_score_coverage,liquidity_turnover_slippage',
                persist_missing_categories=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'summary': {'feedback_rows_matched': 2}})
        self.assertEqual(get_summary.call_args.kwargs['market_date_from'], '2026-05-05')
        self.assertEqual(get_summary.call_args.kwargs['market_date_to'], '2026-05-06')
        self.assertEqual(get_summary.call_args.kwargs['symbols'], ('005930', '000660'))
        self.assertEqual(
            get_summary.call_args.kwargs['categories'],
            ('factor_score_coverage', 'liquidity_turnover_slippage'),
        )
        self.assertTrue(get_summary.call_args.kwargs['persist_missing_categories'])

    def test_cli_agent_feedback_data_gap_quality_report_alias_outputs_json(self) -> None:
        stdout = io.StringIO()
        with (
            patch(
                'app.internal.ops.get_agent_feedback_data_gap_summary',
                return_value={'summary': {'duplicate_note_rate': 0.25}},
            ) as get_summary,
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(
                [
                    'agent-feedback-data-gap-quality-report',
                    '--market-date-from',
                    '2026-05-05',
                    '--market-date-to',
                    '2026-05-06',
                    '--agent-name',
                    'technical-summarizer',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {'summary': {'duplicate_note_rate': 0.25}})
        self.assertEqual(get_summary.call_args.kwargs['market_date_from'], '2026-05-05')
        self.assertEqual(get_summary.call_args.kwargs['market_date_to'], '2026-05-06')
        self.assertEqual(get_summary.call_args.kwargs['agent_name'], 'technical-summarizer')

    def test_compute_market_risk_filters_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_market_risk_filter_service.rebuild_for_market_date',
                new=AsyncMock(return_value={'results_persisted': 2}),
            ) as compute_market_risk,
            patch(
                'app.eod.daily_pipeline_tracking.daily_pipeline_run_state_service',
                FakeDailyPipelineRunStateService(),
            ),
            patch(
                'app.eod.daily_pipeline_tracking.get_market_state',
                return_value=SimpleNamespace(
                    server_time='2026-04-14T10:00:00+09:00'
                ),
            ),
        ):
            result = ops.compute_market_risk_filters(
                market_date='2026-04-14',
                symbols='005930,000660',
                source=MARKET_RISK_FILTER_SOURCE,
                market_code='UN',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result['results_persisted'], 2)
        self.assertEqual(result['pipeline_run_id'], 7002)
        self.assertEqual(compute_market_risk.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(compute_market_risk.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_market_risk.call_args.kwargs['source'], MARKET_RISK_FILTER_SOURCE)
        self.assertEqual(compute_market_risk.call_args.kwargs['market_code'], 'UN')
        self.assertIn('progress_callback', compute_market_risk.call_args.kwargs)

    def test_collect_market_risk_snapshots_uses_snapshot_service(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_market_risk_snapshot_service.collect_for_market_date',
                new=AsyncMock(return_value={'snapshots_persisted': 2}),
            ) as collect_snapshots,
            patch(
                'app.internal.ops.get_market_state',
                return_value=SimpleNamespace(
                    server_time='2026-04-14T10:00:00+09:00'
                ),
            ),
        ):
            result = ops.collect_market_risk_snapshots(
                market_date='2026-04-14',
                symbols='005930,000660',
                source=MARKET_RISK_SNAPSHOT_SOURCE,
                market_code='UN',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'snapshots_persisted': 2})
        self.assertEqual(collect_snapshots.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(collect_snapshots.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(collect_snapshots.call_args.kwargs['source'], MARKET_RISK_SNAPSHOT_SOURCE)
        self.assertEqual(collect_snapshots.call_args.kwargs['market_code'], 'UN')

    def test_prior_date_market_risk_cli_commands_safe_skip_kis_writes(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime'),
            patch(
                'app.internal.ops.daily_market_risk_snapshot_service.'
                'collect_for_market_date',
                new=AsyncMock(),
            ) as collect_snapshots,
            patch(
                'app.internal.ops.daily_market_risk_filter_service.'
                'rebuild_for_market_date',
                new=AsyncMock(),
            ) as compute_filters,
            patch(
                'app.internal.ops.get_market_state',
                return_value=SimpleNamespace(
                    server_time='2026-09-14T10:00:00+09:00'
                ),
            ),
            patch(
                'app.eod.daily_pipeline_tracking.get_market_state',
                return_value=SimpleNamespace(
                    server_time='2026-09-14T10:00:00+09:00'
                ),
            ),
        ):
            snapshot_result = ops.collect_market_risk_snapshots(
                market_date='2026-09-11',
                symbols='005930',
            )
            filter_result = ops.compute_market_risk_filters(
                market_date='2026-09-11',
                symbols='005930',
            )

        self.assertTrue(snapshot_result['safe_skip'])
        self.assertTrue(filter_result['safe_skip'])
        collect_snapshots.assert_not_awaited()
        compute_filters.assert_not_awaited()

    def test_compute_low_risk_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_low_risk_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 1},
            ) as compute_low_risk,
        ):
            result = ops.compute_low_risk_factors(
                market_date='2026-04-23',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 1})
        self.assertEqual(compute_low_risk.call_args.kwargs['market_date'], '2026-04-23')
        self.assertEqual(compute_low_risk.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_low_risk.call_args.kwargs['source'], LOW_RISK_SOURCE)

    def test_compute_beta_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_beta_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 1},
            ) as compute_beta,
        ):
            result = ops.compute_beta_factors(
                market_date='2026-04-23',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 1})
        self.assertEqual(compute_beta.call_args.kwargs['market_date'], '2026-04-23')
        self.assertEqual(compute_beta.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_beta.call_args.kwargs['source'], BETA_FACTOR_SOURCE)

    def test_compute_momentum_factors_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_momentum_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_momentum,
        ):
            result = ops.compute_momentum_factors(
                market_date='2026-04-14',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_momentum.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(compute_momentum.call_args.kwargs['symbols'], ['005930', '000660'])

    def test_compute_value_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_value,
        ):
            result = ops.compute_value_factors(
                market_date='2026-04-24',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_value.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_value.call_args.kwargs['source'], VALUE_FACTOR_SOURCE)

    def test_compute_value_factors_preserves_empty_symbol_subset(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 0},
            ) as compute_value,
        ):
            result = ops.compute_value_factors(
                market_date='2026-04-24',
                symbols=',',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 0})
        self.assertEqual(compute_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_value.call_args.kwargs['symbols'], [])
        self.assertEqual(compute_value.call_args.kwargs['source'], VALUE_FACTOR_SOURCE)

    def test_compute_advanced_value_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_advanced_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_advanced_value,
        ):
            result = ops.compute_advanced_value_factors(
                market_date='2026-04-24',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_advanced_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_advanced_value.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_advanced_value.call_args.kwargs['source'], ADVANCED_VALUE_FACTOR_SOURCE)

    def test_compute_advanced_value_factors_preserves_empty_symbol_subset(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_advanced_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 0},
            ) as compute_advanced_value,
        ):
            result = ops.compute_advanced_value_factors(
                market_date='2026-04-24',
                symbols=',',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 0})
        self.assertEqual(compute_advanced_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_advanced_value.call_args.kwargs['symbols'], [])
        self.assertEqual(compute_advanced_value.call_args.kwargs['source'], ADVANCED_VALUE_FACTOR_SOURCE)

    def test_compute_unified_value_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_unified_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_unified_value,
        ):
            result = ops.compute_unified_value_factors(
                market_date='2026-04-24',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_unified_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_unified_value.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_unified_value.call_args.kwargs['source'], UNIFIED_VALUE_FACTOR_SOURCE)

    def test_compute_unified_value_factors_preserves_empty_symbol_subset(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_unified_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 0},
            ) as compute_unified_value,
        ):
            result = ops.compute_unified_value_factors(
                market_date='2026-04-24',
                symbols=',',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 0})
        self.assertEqual(compute_unified_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_unified_value.call_args.kwargs['symbols'], [])
        self.assertEqual(compute_unified_value.call_args.kwargs['source'], UNIFIED_VALUE_FACTOR_SOURCE)

    def test_compute_quality_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_quality_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_quality,
        ):
            result = ops.compute_quality_factors(
                market_date='2026-04-24',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_quality.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_quality.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_quality.call_args.kwargs['source'], QUALITY_FACTOR_SOURCE)

    def test_compute_quality_factors_preserves_empty_symbol_subset(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_quality_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 0},
            ) as compute_quality,
        ):
            result = ops.compute_quality_factors(
                market_date='2026-04-24',
                symbols=',',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 0})
        self.assertEqual(compute_quality.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_quality.call_args.kwargs['symbols'], [])
        self.assertEqual(compute_quality.call_args.kwargs['source'], QUALITY_FACTOR_SOURCE)

    def test_compute_composite_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_composite_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_composite,
        ):
            result = ops.compute_composite_factors(
                market_date='2026-04-24',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_composite.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_composite.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_composite.call_args.kwargs['source'], COMPOSITE_FACTOR_SOURCE)

    def test_compute_composite_factors_preserves_empty_symbol_subset(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_composite_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 0},
            ) as compute_composite,
        ):
            result = ops.compute_composite_factors(
                market_date='2026-04-24',
                symbols=',',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 0})
        self.assertEqual(compute_composite.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_composite.call_args.kwargs['symbols'], [])
        self.assertEqual(compute_composite.call_args.kwargs['source'], COMPOSITE_FACTOR_SOURCE)

    def test_cli_quotes_tracked_outputs_json(self) -> None:
        stdout = io.StringIO()
        with (
            patch('app.internal.ops.get_tracked_quotes', new=AsyncMock(return_value={'items': [{'symbol': '005930'}]})),
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(['quotes-tracked'])

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {'items': [{'symbol': '005930'}]})

    def test_cli_market_status_outputs_json(self) -> None:
        stdout = io.StringIO()
        with (
            patch('app.internal.ops.get_market_status', return_value={'state': 'CLOSED'}),
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(['market-status'])

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {'state': 'CLOSED'})

    def test_cli_dart_fundamentals_daily_run_passes_symbols(self) -> None:
        payload = {
            'status': 'partial',
            'selected_stock_codes': ['005930', '000660'],
            'successful_stock_codes': ['005930'],
            'retry_debt_stock_codes': ['000660'],
        }
        stdout = io.StringIO()
        with (
            patch(
                'app.internal.ops.run_dart_fundamentals_daily_if_due',
                return_value=payload,
            ) as run_daily,
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(
                [
                    'dart-fundamentals-daily-run-if-due',
                    '--market-date',
                    '2026-10-30',
                    '--symbols',
                    '005930,000660',
                    '--capture-limit',
                    '20',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), payload)
        self.assertEqual(run_daily.call_args.kwargs['symbols'], '005930,000660')
        self.assertIsNone(run_daily.call_args.kwargs['symbol_limit'])
        self.assertEqual(run_daily.call_args.kwargs['symbol_offset'], 0)

    def test_cli_signal_decider_evaluations_outputs_json(self) -> None:
        stdout = io.StringIO()
        with (
            patch(
                'app.internal.ops.compute_signal_decider_evaluations',
                return_value={'summary': {'evaluations_upserted': 6}},
            ),
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(
                [
                    'agent-signal-decider-evaluations-compute',
                    '--market-date',
                    '2026-04-30',
                    '--symbols',
                    '005930,000660',
                    '--horizons',
                    '1,5',
                    '--force-recompute',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {'summary': {'evaluations_upserted': 6}})

    def test_cli_signal_decider_calendar_refresh_outputs_json(self) -> None:
        stdout = io.StringIO()
        with (
            patch(
                'app.internal.ops.refresh_signal_decider_calendar_reviews',
                return_value={'summary': {'review_rows_upserted': 12}},
            ),
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(
                [
                    'agent-signal-decider-calendar-refresh',
                    '--market-date',
                    '2026-04-30',
                    '--symbols',
                    '005930,000660',
                    '--horizons',
                    '1,5',
                    '--primary-horizons',
                    '3,5',
                    '--force-recompute',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {'summary': {'review_rows_upserted': 12}})

    def test_cli_signal_decider_evaluations_repair_outputs_json(self) -> None:
        stdout = io.StringIO()
        with (
            patch(
                'app.internal.ops.repair_signal_decider_evaluation_coverage',
                return_value={'status': 'completed', 'summary': {'computed_count': 12}},
            ),
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(
                [
                    'agent-signal-decider-evaluations-repair',
                    '--market-date-from',
                    '2026-04-01',
                    '--market-date-to',
                    '2026-04-30',
                    '--symbols',
                    '005930,000660',
                    '--horizons',
                    '1,3,5',
                    '--primary-horizons',
                    '5',
                    '--refresh-calendar',
                    'false',
                    '--fail-on-mature-gaps',
                    'false',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {'status': 'completed', 'summary': {'computed_count': 12}},
        )
