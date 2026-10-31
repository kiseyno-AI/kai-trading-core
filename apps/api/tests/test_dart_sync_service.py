import asyncio
import unittest
from datetime import date
from unittest.mock import Mock, patch

from sqlalchemy.exc import OperationalError, ProgrammingError

from app.core.settings import settings
from app.dart.signals_service import DartSignalsService
from app.dart.sync_service import DartDisclosureSyncService, classify_filing
from app.integrations.dart.client import DartAPIError


class FakeSyncClient:
    def __init__(self):
        self.calls = []

    async def fetch_disclosure_list(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs['page_no'] == 1:
            return {
                'status': '000',
                'total_count': '3',
                'page_no': '1',
                'page_count': '2',
                'list': [
                    {
                        'rcept_no': '20260407000003',
                        'corp_code': '00126380',
                        'corp_name': '삼성전자',
                        'stock_code': '005930',
                        'corp_cls': 'Y',
                        'report_nm': '[기재정정] 사업보고서 (2025.12)',
                        'flr_nm': '삼성전자',
                        'rcept_dt': '20260407',
                        'rm': '',
                    },
                    {
                        'rcept_no': '20260407000002',
                        'corp_code': '00126380',
                        'corp_name': '삼성전자',
                        'stock_code': '005930',
                        'corp_cls': 'Y',
                        'report_nm': '감사보고서 제출',
                        'flr_nm': '삼성전자',
                        'rcept_dt': '20260407',
                        'rm': '',
                    },
                ],
            }
        return {
            'status': '000',
            'total_count': '3',
            'page_no': '2',
            'page_count': '2',
            'list': [
                {
                    'rcept_no': '20260407000001',
                    'corp_code': '00126380',
                    'corp_name': '삼성전자',
                    'stock_code': '005930',
                    'corp_cls': 'Y',
                    'report_nm': '반기보고서 (2025.06)',
                    'flr_nm': '삼성전자',
                    'rcept_dt': '20260407',
                    'rm': '정',
                }
            ],
        }


class FakeSyncRepository:
    def __init__(self):
        self.created_runs = []
        self.completed_runs = []
        self.upserted_batches = []

    def create_sync_run(self, **kwargs):
        self.created_runs.append(kwargs)
        return 41

    def complete_sync_run(self, run_id, **kwargs):
        self.completed_runs.append((run_id, kwargs))

    def upsert_events(self, items, *, run_id):
        self.upserted_batches.append((run_id, items))
        return {
            'seen': len(items),
            'inserted': len(items),
            'updated': 0,
            'financial_report_events': sum(1 for item in items if item['is_financial_report']),
        }

    def get_latest_sync_run(self):
        return {'id': 41, 'status': 'completed'}

    def list_sync_runs(self, *, limit=10):
        return [{'id': 41, 'status': 'completed'}][:limit]

    def get_summary(self):
        return {'total_events': 3, 'financial_report_events': 2, 'latest_receipt_date': '2026-04-07'}

    def list_events(self, *, limit=50, financial_only=False):
        return [{'rcept_no': '20260407000003', 'is_financial_report': True}][:limit]


class SinglePageSyncClient:
    def __init__(self, items):
        self.items = list(items)
        self.calls = []

    async def fetch_disclosure_list(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'status': '000',
            'total_count': str(len(self.items)),
            'page_no': '1',
            'page_count': str(len(self.items)),
            'list': self.items,
        }


class RecordingSignalsRepository:
    def __init__(self, previous_scan_evidence=None, latest_positive_date=None):
        self.previous_scan_evidence = list(previous_scan_evidence or [])
        self.latest_positive_date = latest_positive_date

    def list_recent_share_issuance_scan_evidence(self, **_kwargs):
        return list(self.previous_scan_evidence)

    def get_latest_share_issuance_positive_event_date(self, **_kwargs):
        return self.latest_positive_date


class RecordingSignalsService(DartSignalsService):
    def __init__(self, *, previous_scan_evidence=None, latest_positive_date=None):
        super().__init__(
            client=object(),
            repository=RecordingSignalsRepository(
                previous_scan_evidence,
                latest_positive_date,
            ),
        )
        self.stored_dilution_rows = []

    def _store_dilution_signal_rows(self, evidence_rows):
        for evidence_row in evidence_rows:
            receipt_number = evidence_row['event']['rcept_no']
            self.stored_dilution_rows = [
                existing
                for existing in self.stored_dilution_rows
                if existing['event']['rcept_no'] != receipt_number
            ]
            self.stored_dilution_rows.append(evidence_row)


class DartDisclosureSyncServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        patcher = patch(
            'app.dart.sync_service._persist_share_issuance_scan_evidence',
            return_value={
                'positive_event_count': 0,
                'negative_coverage_count': 0,
                'rows_upserted': 0,
            },
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        positive_patcher = patch(
            'app.dart.sync_service._persist_share_issuance_positive_events',
            return_value={
                'positive_event_count': 0,
                'rows_upserted': 0,
            },
        )
        positive_patcher.start()
        self.addCleanup(positive_patcher.stop)

    async def test_global_share_issuance_rejects_empty_manifest_without_provider_call(self):
        client = SinglePageSyncClient([])
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)

        result = await service.sync_global_share_issuance_evidence(
            corporations=[],
            end_date=date(2026, 10, 30),
        )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['scan_status'], 'not_started')
        self.assertEqual(client.calls, [])
        self.assertEqual(repository.completed_runs[-1][1]['status'], 'failed')

    async def test_global_share_issuance_slices_long_window_and_dedupes_corporations(self):
        client = SinglePageSyncClient([])
        repository = FakeSyncRepository()
        positive_persister = Mock(
            return_value={'positive_event_count': 0, 'rows_upserted': 0}
        )
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
            dilution_positive_persister=positive_persister,
        )
        with (
            patch(
                'app.dart.signals_service.dart_signals_service.plan_global_share_issuance_scan',
                return_value={
                    'window_start': date(2026, 6, 1),
                    'window_end': date(2026, 10, 30),
                    'corporation_count': 1,
                    'baseline_required_count': 0,
                    'baseline_required_corp_codes': [],
                    'late_arrival_reconciliation': 'next_scan_one_day_overlap_v1',
                },
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.persist_global_share_issuance_scan_evidence',
                return_value={
                    'pagination_completed': True,
                    'corporation_count': 1,
                    'positive_event_count': 0,
                    'negative_coverage_count': 1,
                    'negative_segment_count': 0,
                    'rows_upserted': 2,
                    'coverage_ready_corp_codes': ['00126380'],
                    'baseline_debt_corp_codes': [],
                    'coverage_status_by_corp': {
                        '00126380': 'complete_negative'
                    },
                },
            ),
        ):
            result = await service.sync_global_share_issuance_evidence(
                corporations=[
                    {'corp_code': '00126380', 'stock_code': '005930'},
                    {'corp_code': '00126380', 'stock_code': '005935'},
                ],
                end_date=date(2026, 10, 30),
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(
            [(call['bgn_de'], call['end_de']) for call in client.calls],
            [('20260601', '20260829'), ('20260830', '20261030')],
        )
        self.assertEqual(
            result['coverage_ready_stock_codes'],
            ['005930', '005935'],
        )
        self.assertEqual(
            repository.created_runs[-1]['run_metadata']['requested_corporations'],
            1,
        )

    async def test_global_share_issuance_reuses_only_exact_completed_ready_scope(self):
        client = SinglePageSyncClient([])
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)
        corporations = [
            {'corp_code': '00126380', 'stock_code': '005930'},
        ]
        plan = {
            'window_start': date(2026, 10, 30),
            'window_end': date(2026, 10, 30),
            'corporation_count': 1,
            'baseline_required_count': 0,
            'baseline_required_corp_codes': [],
            'late_arrival_reconciliation': 'next_scan_one_day_overlap_v1',
        }
        evidence = {
            'pagination_completed': True,
            'corporation_count': 1,
            'positive_event_count': 0,
            'negative_coverage_count': 1,
            'negative_segment_count': 0,
            'rows_upserted': 2,
            'coverage_ready_corp_codes': ['00126380'],
            'baseline_debt_corp_codes': [],
            'coverage_status_by_corp': {
                '00126380': 'complete_negative',
            },
        }

        with (
            patch(
                'app.dart.signals_service.dart_signals_service.plan_global_share_issuance_scan',
                return_value=plan,
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.persist_global_share_issuance_scan_evidence',
                return_value=evidence,
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.repository.list_current_share_issuance_coverage',
                return_value=[],
            ) as current_coverage,
            patch(
                'app.dart.signals_service.dart_signals_service.repository.list_latest_share_issuance_positive_event_dates',
                return_value={},
            ),
            patch(
                'app.dart.signals_service.dart_signals_service._validated_share_issuance_coverage',
                return_value={
                    'status': 'complete',
                    'window_start': '2025-10-30',
                    'window_end': '2026-10-30',
                },
            ) as validated_coverage,
        ):
            first = await service.sync_global_share_issuance_evidence(
                corporations=corporations,
                end_date=date(2026, 10, 30),
            )
            prior_metadata = repository.completed_runs[-1][1][
                'run_metadata'
            ]
            repository.list_sync_runs = lambda *, limit=50: [
                {
                    'id': first['run_id'],
                    'status': 'completed',
                    'window_start': first['window_start'],
                    'window_end': first['window_end'],
                    'pages_fetched': first['pages_fetched'],
                    'events_seen': first['events_seen'],
                    'run_metadata': prior_metadata,
                }
            ][:limit]
            current_coverage.return_value = [
                {'corp_code': '00126380', 'payload': {}}
            ]

            second = await service.sync_global_share_issuance_evidence(
                corporations=corporations,
                end_date=date(2026, 10, 30),
            )

            legacy_metadata = {
                **prior_metadata,
                'manifest_version': 'dart_share_issuance_manifest_v1',
            }
            repository.list_sync_runs = lambda *, limit=50: [
                {
                    'id': first['run_id'],
                    'status': 'completed',
                    'window_start': first['window_start'],
                    'window_end': first['window_end'],
                    'pages_fetched': first['pages_fetched'],
                    'events_seen': first['events_seen'],
                    'run_metadata': legacy_metadata,
                }
            ][:limit]
            third = await service.sync_global_share_issuance_evidence(
                corporations=corporations,
                end_date=date(2026, 10, 30),
            )

            repository.list_sync_runs = lambda *, limit=50: [
                {
                    'id': first['run_id'],
                    'status': 'completed',
                    'window_start': first['window_start'],
                    'window_end': first['window_end'],
                    'pages_fetched': first['pages_fetched'],
                    'events_seen': first['events_seen'],
                    'run_metadata': prior_metadata,
                }
            ][:limit]
            validated_coverage.return_value = {
                'status': 'complete',
                'window_start': '2026-10-29',
                'window_end': '2026-10-30',
            }
            fourth = await service.sync_global_share_issuance_evidence(
                corporations=corporations,
                end_date=date(2026, 10, 30),
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(second['status'], 'completed')
        self.assertTrue(second['reused'])
        self.assertEqual(
            second['reason'],
            'exact_completed_global_scan_reused',
        )
        self.assertEqual(third['status'], 'completed')
        self.assertNotIn('reused', third)
        self.assertEqual(fourth['status'], 'completed')
        self.assertNotIn('reused', fourth)
        self.assertEqual(len(client.calls), 3)
        self.assertEqual(len(repository.created_runs), 3)

    async def test_global_share_issuance_keeps_baseline_debt_partial(self):
        client = SinglePageSyncClient([])
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)
        with (
            patch(
                'app.dart.signals_service.dart_signals_service.plan_global_share_issuance_scan',
                return_value={
                    'window_start': date(2026, 10, 30),
                    'window_end': date(2026, 10, 30),
                    'corporation_count': 1,
                    'baseline_required_count': 1,
                    'baseline_required_corp_codes': ['00126380'],
                    'late_arrival_reconciliation': 'next_scan_one_day_overlap_v1',
                },
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.persist_global_share_issuance_scan_evidence',
                return_value={
                    'pagination_completed': True,
                    'corporation_count': 1,
                    'positive_event_count': 0,
                    'negative_coverage_count': 0,
                    'negative_segment_count': 1,
                    'rows_upserted': 2,
                    'coverage_ready_corp_codes': [],
                    'baseline_debt_corp_codes': ['00126380'],
                    'coverage_status_by_corp': {
                        '00126380': 'negative_segment'
                    },
                },
            ),
        ):
            result = await service.sync_global_share_issuance_evidence(
                corporations=[
                    {'corp_code': '00126380', 'stock_code': '005930'}
                ],
                end_date=date(2026, 10, 30),
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['scan_status'], 'completed')
        self.assertEqual(result['coverage_ready_stock_codes'], [])
        self.assertEqual(result['retry_debt_stock_codes'], ['005930'])

    async def test_global_share_issuance_quarantines_only_malformed_corporation(self):
        client = SinglePageSyncClient(
            [
                {
                    'rcept_no': '20261030000001',
                    'corp_code': '00126380',
                    'corp_name': '삼성전자',
                    'stock_code': '005930',
                    'report_nm': '사업보고서',
                    'rcept_dt': '20261030',
                },
                {
                    'rcept_no': '20261030000002',
                    'corp_code': '00999999',
                    'corp_name': '불완전회사',
                    'stock_code': '999999',
                    'report_nm': '',
                    'rcept_dt': '20261030',
                },
            ]
        )
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
            dilution_positive_persister=Mock(
                return_value={
                    'positive_event_count': 0,
                    'rows_upserted': 0,
                }
            ),
        )
        with (
            patch(
                'app.dart.signals_service.dart_signals_service.plan_global_share_issuance_scan',
                return_value={
                    'window_start': date(2026, 10, 30),
                    'window_end': date(2026, 10, 30),
                    'corporation_count': 2,
                    'baseline_required_count': 0,
                    'baseline_required_corp_codes': [],
                    'late_arrival_reconciliation': (
                        'next_scan_one_day_overlap_v1'
                    ),
                },
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.persist_global_share_issuance_scan_evidence',
                return_value={
                    'pagination_completed': True,
                    'corporation_count': 1,
                    'positive_event_count': 0,
                    'negative_coverage_count': 1,
                    'negative_segment_count': 0,
                    'rows_upserted': 2,
                    'coverage_ready_corp_codes': ['00126380'],
                    'baseline_debt_corp_codes': [],
                    'failed_corp_codes': [],
                    'failed_corp_reasons': {},
                    'coverage_status_by_corp': {
                        '00126380': 'complete_negative'
                    },
                },
            ) as evidence_persister,
        ):
            result = await service.sync_global_share_issuance_evidence(
                corporations=[
                    {'corp_code': '00126380', 'stock_code': '005930'},
                    {'corp_code': '00999999', 'stock_code': '999999'},
                ],
                end_date=date(2026, 10, 30),
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['coverage_ready_stock_codes'], ['005930'])
        self.assertEqual(result['retry_debt_stock_codes'], ['999999'])
        self.assertEqual(result['failed_corp_codes'], ['00999999'])
        self.assertIn('missing report_nm', result['failed_corp_reasons']['00999999'])
        persisted_corporations = evidence_persister.call_args.kwargs[
            'corporations'
        ]
        self.assertEqual(
            [item['corp_code'] for item in persisted_corporations],
            ['00126380'],
        )

    async def test_global_share_issuance_page_failure_preserves_positive_and_blocks_negative(self):
        class PageTwoFailureClient:
            def __init__(self):
                self.calls = []

            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs)
                if kwargs['page_no'] == 2:
                    raise TimeoutError('page two unavailable')
                return {
                    'status': '000',
                    'total_count': '2',
                    'page_no': '1',
                    'page_count': '1',
                    'list': [
                        {
                            'rcept_no': '20261030000001',
                            'corp_code': '00126380',
                            'corp_name': '삼성전자',
                            'stock_code': '005930',
                            'report_nm': '유상증자결정',
                            'rcept_dt': '20261030',
                        }
                    ],
                }

        client = PageTwoFailureClient()
        repository = FakeSyncRepository()
        positive_persister = Mock(
            return_value={'positive_event_count': 1, 'rows_upserted': 1}
        )
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
            dilution_positive_persister=positive_persister,
        )
        with (
            patch(
                'app.dart.signals_service.dart_signals_service.plan_global_share_issuance_scan',
                return_value={
                    'window_start': date(2026, 10, 30),
                    'window_end': date(2026, 10, 30),
                    'corporation_count': 1,
                    'baseline_required_count': 1,
                    'baseline_required_corp_codes': ['00126380'],
                    'late_arrival_reconciliation': 'next_scan_one_day_overlap_v1',
                },
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.persist_global_share_issuance_scan_evidence'
            ) as negative_persister,
            patch(
                'app.dart.signals_service.dart_signals_service.persist_global_share_issuance_incomplete_attempts',
                return_value=1,
            ) as incomplete_persister,
        ):
            result = await service.sync_global_share_issuance_evidence(
                corporations=[
                    {'corp_code': '00126380', 'stock_code': '005930'}
                ],
                end_date=date(2026, 10, 30),
            )

        self.assertEqual(result['status'], 'failed')
        positive_persister.assert_called_once()
        negative_persister.assert_not_called()
        incomplete_persister.assert_called_once()

    async def test_global_share_issuance_failure_recovers_persisted_positive_authority(self):
        class FirstPageFailureClient:
            def __init__(self):
                self.calls = []

            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs)
                raise TimeoutError('provider unavailable before first page')

        client = FirstPageFailureClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)
        with (
            patch(
                'app.dart.signals_service.dart_signals_service.plan_global_share_issuance_scan',
                return_value={
                    'window_start': date(2026, 10, 30),
                    'window_end': date(2026, 10, 30),
                    'corporation_count': 1,
                    'baseline_required_count': 1,
                    'baseline_required_corp_codes': ['00126380'],
                    'late_arrival_reconciliation': (
                        'next_scan_one_day_overlap_v1'
                    ),
                },
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.repository.list_current_share_issuance_coverage',
                return_value=[],
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.repository.list_latest_share_issuance_positive_event_dates',
                return_value={'00126380': date(2026, 10, 30)},
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.persist_global_share_issuance_incomplete_attempts',
                return_value=1,
            ),
        ):
            result = await service.sync_global_share_issuance_evidence(
                corporations=[
                    {'corp_code': '00126380', 'stock_code': '005930'}
                ],
                end_date=date(2026, 10, 30),
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(
            result['decision_ready_positive_stock_codes'],
            ['005930'],
        )
        self.assertEqual(result['retry_debt_stock_codes'], [])
        self.assertEqual(result['failed_stock_codes'], ['005930'])

    async def test_global_share_issuance_failure_uses_full_positive_authority_window(self):
        class FirstPageFailureClient:
            async def fetch_disclosure_list(self, **kwargs):
                raise TimeoutError('provider unavailable before first page')

        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(
            client=FirstPageFailureClient(),
            repository=repository,
        )
        with (
            patch(
                'app.dart.signals_service.dart_signals_service.plan_global_share_issuance_scan',
                return_value={
                    'window_start': date(2026, 10, 30),
                    'window_end': date(2026, 10, 30),
                    'corporation_count': 2,
                    'baseline_required_count': 2,
                    'baseline_required_corp_codes': [
                        '00126380',
                        '00164779',
                    ],
                    'late_arrival_reconciliation': (
                        'next_scan_one_day_overlap_v1'
                    ),
                },
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.repository.list_current_share_issuance_coverage',
                return_value=[],
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.repository.list_latest_share_issuance_positive_event_dates',
                return_value={'00126380': date(2026, 9, 30)},
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.persist_global_share_issuance_incomplete_attempts',
                return_value=2,
            ),
        ):
            result = await service.sync_global_share_issuance_evidence(
                corporations=[
                    {'corp_code': '00126380', 'stock_code': '005930'},
                    {'corp_code': '00164779', 'stock_code': '000660'},
                    {'corp_code': '', 'stock_code': '999999'},
                ],
                end_date=date(2026, 10, 30),
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(
            result['decision_ready_positive_stock_codes'],
            ['005930'],
        )
        self.assertEqual(
            result['retry_debt_stock_codes'],
            ['000660', '999999'],
        )
        self.assertEqual(
            result['failed_stock_codes'],
            ['000660', '005930', '999999'],
        )

    async def test_global_share_issuance_stops_on_systemic_positive_write_failure(self):
        client = SinglePageSyncClient(
            [
                {
                    'rcept_no': '20261030000001',
                    'corp_code': '00126380',
                    'corp_name': '삼성전자',
                    'stock_code': '005930',
                    'report_nm': '유상증자결정',
                    'rcept_dt': '20261030',
                },
                {
                    'rcept_no': '20261030000002',
                    'corp_code': '00164779',
                    'corp_name': 'SK하이닉스',
                    'stock_code': '000660',
                    'report_nm': '전환사채권발행결정',
                    'rcept_dt': '20261030',
                },
            ]
        )
        repository = FakeSyncRepository()
        outage = OperationalError(
            'insert positive dilution evidence',
            {},
            ConnectionError('database connection reset'),
            connection_invalidated=True,
        )
        positive_persister = Mock(side_effect=outage)
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
            dilution_positive_persister=positive_persister,
        )
        with (
            patch(
                'app.dart.signals_service.dart_signals_service.plan_global_share_issuance_scan',
                return_value={
                    'window_start': date(2026, 10, 30),
                    'window_end': date(2026, 10, 30),
                    'corporation_count': 2,
                    'baseline_required_count': 2,
                    'baseline_required_corp_codes': [
                        '00126380',
                        '00164779',
                    ],
                    'late_arrival_reconciliation': (
                        'next_scan_one_day_overlap_v1'
                    ),
                },
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.repository.list_current_share_issuance_coverage',
                return_value=[],
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.repository.list_latest_share_issuance_positive_event_dates',
                return_value={},
            ),
            patch(
                'app.dart.signals_service.dart_signals_service.persist_global_share_issuance_incomplete_attempts',
                return_value=2,
            ) as incomplete_persister,
        ):
            result = await service.sync_global_share_issuance_evidence(
                corporations=[
                    {'corp_code': '00126380', 'stock_code': '005930'},
                    {'corp_code': '00164779', 'stock_code': '000660'},
                ],
                end_date=date(2026, 10, 30),
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['scan_status'], 'failed')
        self.assertEqual(
            result['retry_debt_stock_codes'],
            ['000660', '005930'],
        )
        positive_persister.assert_called_once()
        incomplete_persister.assert_called_once()

    async def test_sync_recent_disclosures_paginates_and_classifies_financial_reports(self):
        client = FakeSyncClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)
        original_page_count = settings.dart_sync_page_count
        settings.dart_sync_page_count = 2
        try:
            with patch('app.dart.sync_service.log_fetch_event') as log_fetch_event:
                result = await service.sync_recent_disclosures(days=3, trigger='test')
        finally:
            settings.dart_sync_page_count = original_page_count

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['run_id'], 41)
        self.assertEqual(result['pages_fetched'], 2)
        self.assertEqual(result['events_seen'], 3)
        self.assertEqual(result['events_inserted'], 2)
        self.assertEqual(result['financial_report_events'], 2)
        self.assertEqual(client.calls[0]['last_reprt_at'], 'N')
        self.assertEqual(repository.created_runs[0]['trigger'], 'test')
        first_batch = repository.upserted_batches[0][1]
        self.assertTrue(first_batch[0]['is_financial_report'])
        self.assertEqual(first_batch[0]['filing_category'], 'annual_report')
        self.assertEqual(len(first_batch), 1)
        self.assertEqual(repository.upserted_batches[1][1][0]['filing_category'], 'semiannual_report')
        self.assertEqual(repository.completed_runs[-1][1]['status'], 'completed')

    async def test_sync_disclosures_for_corporations_only_persists_financial_reports(self):
        client = FakeSyncClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)
        original_page_count = settings.dart_sync_page_count
        settings.dart_sync_page_count = 2
        try:
            result = await service.sync_disclosures_for_corporations(
                corporations=[{'corp_code': '00126380', 'stock_code': '005930'}],
                start_date=date(2025, 1, 1),
                end_date=date(2026, 4, 7),
                trigger='active_test',
                scope='targeted_active_universe',
                root_stock_code_manifest=['005930', '000660'],
            )
        finally:
            settings.dart_sync_page_count = original_page_count

        self.assertEqual(result['corporations_requested'], 1)
        self.assertEqual(result['scope'], 'targeted_active_universe')
        self.assertEqual(result['corporations_processed'], 1)
        self.assertEqual(result['concurrency'], 1)
        self.assertEqual(
            repository.created_runs[-1]['run_metadata']['concurrency'],
            1,
        )
        self.assertEqual(
            repository.created_runs[-1]['run_metadata']['scope'],
            'targeted_active_universe',
        )
        self.assertEqual(
            repository.created_runs[-1]['run_metadata'][
                'root_stock_code_manifest'
            ],
            ['005930', '000660'],
        )
        self.assertEqual(
            result['root_stock_code_manifest'],
            ['005930', '000660'],
        )
        self.assertEqual(result['financial_report_events'], 2)
        self.assertEqual(sum(len(batch) for _, batch in repository.upserted_batches), 2)

    async def test_targeted_corp_scan_rejects_blank_stock_code_before_provider_call(self):
        client = FakeSyncClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
        )

        with self.assertRaisesRegex(
            ValueError,
            'contains an invalid stock code',
        ):
            await service.sync_disclosures_for_corporations(
                corporations=[
                    {'corp_code': '00126380', 'stock_code': ''},
                ],
                start_date=date(2025, 1, 1),
                end_date=date(2026, 4, 7),
                trigger='active_test',
                scope='targeted_active_universe',
                root_stock_code_manifest=['005930'],
            )

        self.assertEqual(client.calls, [])
        self.assertEqual(repository.created_runs, [])

    async def test_targeted_corp_scan_rejects_empty_corp_code_before_provider_call(self):
        client = FakeSyncClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
        )

        with self.assertRaisesRegex(
            ValueError,
            'contains an empty corp code',
        ):
            await service.sync_disclosures_for_corporations(
                corporations=[
                    {'corp_code': '', 'stock_code': '005930'},
                ],
                start_date=date(2025, 1, 1),
                end_date=date(2026, 4, 7),
                trigger='active_test',
                scope='targeted_active_universe',
                root_stock_code_manifest=['005930'],
            )

        self.assertEqual(client.calls, [])
        self.assertEqual(repository.created_runs, [])

    async def test_targeted_corp_scan_rejects_invalid_stock_code_shapes(self):
        invalid_stock_codes = ('ABC123', '1234567', '１２３４５６')
        for stock_code in invalid_stock_codes:
            with self.subTest(stock_code=stock_code):
                client = FakeSyncClient()
                repository = FakeSyncRepository()
                service = DartDisclosureSyncService(
                    client=client,
                    repository=repository,
                )

                with self.assertRaisesRegex(
                    ValueError,
                    'invalid stock code',
                ):
                    await service.sync_disclosures_for_corporations(
                        corporations=[
                            {
                                'corp_code': '00126380',
                                'stock_code': stock_code,
                            },
                        ],
                        start_date=date(2025, 1, 1),
                        end_date=date(2026, 4, 7),
                        trigger='active_test',
                        scope='targeted_active_universe',
                        root_stock_code_manifest=[stock_code],
                    )

                self.assertEqual(client.calls, [])
                self.assertEqual(repository.created_runs, [])

    async def test_targeted_corp_scan_normalizes_short_ascii_stock_code(self):
        client = FakeSyncClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {'corp_code': '00126380', 'stock_code': '5930'},
            ],
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
            scope='targeted_active_universe',
            root_stock_code_manifest=['5930'],
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            result['root_stock_code_manifest'],
            ['005930'],
        )
        self.assertEqual(
            repository.created_runs[-1]['run_metadata'][
                'requested_manifest'
            ][0]['stock_code'],
            '005930',
        )

    async def test_complete_corp_scan_persists_positive_share_issuance_event(self):
        client = SinglePageSyncClient(
            [
                {
                    'rcept_no': '20260410000001',
                    'corp_code': '00126380',
                    'corp_name': '삼성전자',
                    'stock_code': '005930',
                    'corp_cls': 'Y',
                    'report_nm': '유상증자 결정',
                    'flr_nm': '삼성전자',
                    'rcept_dt': '20260410',
                    'rm': '',
                }
            ]
        )
        repository = FakeSyncRepository()
        signals_service = RecordingSignalsService()
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
            dilution_evidence_persister=(
                signals_service.persist_share_issuance_scan_evidence
            ),
            dilution_positive_persister=(
                signals_service.persist_share_issuance_positive_events
            ),
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                }
            ],
            start_date=date(2025, 4, 14),
            end_date=date(2026, 4, 14),
            trigger='active_test',
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['dilution_positive_events'], 1)
        self.assertEqual(result['dilution_negative_coverage_rows'], 0)
        self.assertEqual(result['financial_report_events'], 0)
        self.assertEqual(result['dilution_negative_segment_rows'], 1)
        self.assertEqual(len(signals_service.stored_dilution_rows), 3)
        stored = next(
            row
            for row in signals_service.stored_dilution_rows
            if row['dilution_signal']['has_dilution_signal']
        )
        self.assertEqual(stored['event']['rcept_no'], '20260410000001')
        self.assertTrue(stored['dilution_signal']['has_dilution_signal'])
        self.assertEqual(
            stored['dilution_signal']['payload']['source'],
            'dart_disclosure_events',
        )
        self.assertTrue(
            stored['dilution_signal']['payload']['pagination_completed']
        )

    async def test_positive_event_on_window_end_emits_no_negative_segment(self):
        signals_service = RecordingSignalsService()
        service = DartDisclosureSyncService(
            client=SinglePageSyncClient(
                [
                    {
                        'rcept_no': '20260414000001',
                        'corp_code': '00126380',
                        'corp_name': '삼성전자',
                        'stock_code': '005930',
                        'report_nm': '유상증자 결정',
                        'rcept_dt': '20260414',
                    }
                ]
            ),
            repository=FakeSyncRepository(),
            dilution_evidence_persister=(
                signals_service.persist_share_issuance_scan_evidence
            ),
            dilution_positive_persister=(
                signals_service.persist_share_issuance_positive_events
            ),
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                }
            ],
            start_date=date(2026, 4, 14),
            end_date=date(2026, 4, 14),
        )

        self.assertEqual(result['dilution_positive_events'], 1)
        self.assertEqual(result['dilution_negative_coverage_rows'], 0)
        self.assertEqual(result['dilution_negative_segment_rows'], 0)
        self.assertEqual(len(signals_service.stored_dilution_rows), 1)
        self.assertTrue(
            signals_service.stored_dilution_rows[0]['dilution_signal'][
                'has_dilution_signal'
            ]
        )
    async def test_complete_365_day_corp_scan_persists_negative_coverage(self):
        client = SinglePageSyncClient(
            [
                {
                    'rcept_no': '20260410000002',
                    'corp_code': '00126380',
                    'corp_name': '삼성전자',
                    'stock_code': '005930',
                    'corp_cls': 'Y',
                    'report_nm': '정기주주총회 결과',
                    'flr_nm': '삼성전자',
                    'rcept_dt': '20260410',
                    'rm': '',
                }
            ]
        )
        signals_service = RecordingSignalsService()
        service = DartDisclosureSyncService(
            client=client,
            repository=FakeSyncRepository(),
            dilution_evidence_persister=(
                signals_service.persist_share_issuance_scan_evidence
            ),
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                }
            ],
            start_date=date(2025, 4, 14),
            end_date=date(2026, 4, 14),
            trigger='active_test',
        )

        self.assertEqual(result['dilution_positive_events'], 0)
        self.assertEqual(result['dilution_negative_coverage_rows'], 1)
        self.assertEqual(len(signals_service.stored_dilution_rows), 2)
        stored = next(
            row
            for row in signals_service.stored_dilution_rows
            if str(row['event']['rcept_no']).startswith('DILCOV-')
        )
        self.assertFalse(stored['dilution_signal']['has_dilution_signal'])
        coverage = stored['dilution_signal']['payload']['evidence_coverage']
        self.assertEqual(coverage['schema_version'], 'dart_share_issuance_coverage_v1')
        self.assertEqual(coverage['status'], 'complete')
        self.assertEqual(coverage['source'], 'dart_disclosure_events')
        self.assertEqual(coverage['matched_event_count'], 0)
        self.assertEqual(coverage['window_start'], '2025-04-14')
        self.assertEqual(coverage['window_end'], '2026-04-14')
        self.assertLessEqual(len(stored['event']['rcept_no']), 32)

    async def test_corp_scan_rejects_total_count_drift_before_negative_coverage(self):
        class CountDriftClient:
            async def fetch_disclosure_list(self, **kwargs):
                if kwargs['page_no'] == 1:
                    return {
                        'status': '000',
                        'total_count': '2',
                        'page_no': '1',
                        'page_count': '1',
                        'list': [
                            {
                                'rcept_no': '20260414000001',
                                'corp_code': '00126380',
                                'report_nm': '정기주주총회 결과',
                                'rcept_dt': '20260414',
                            }
                        ],
                    }
                return {
                    'status': '000',
                    'total_count': '1',
                    'page_no': '2',
                    'page_count': '1',
                    'list': [],
                }

        persister = Mock()
        service = DartDisclosureSyncService(
            client=CountDriftClient(),
            repository=FakeSyncRepository(),
            dilution_evidence_persister=persister,
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                }
            ],
            start_date=date(2025, 4, 14),
            end_date=date(2026, 4, 14),
        )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['corporations_failed'], 1)
        self.assertIn('total_count changed', result['corporation_errors'][0]['error'])
        persister.assert_called_once()
        self.assertFalse(
            persister.call_args.kwargs['pagination_completed']
        )

    async def test_corp_scan_rejects_duplicate_receipts_across_pages(self):
        class DuplicatePageClient:
            async def fetch_disclosure_list(self, **kwargs):
                return {
                    'status': '000',
                    'total_count': '2',
                    'page_no': str(kwargs['page_no']),
                    'page_count': '1',
                    'list': [
                        {
                            'rcept_no': '20260414000001',
                            'corp_code': '00126380',
                            'report_nm': '정기주주총회 결과',
                            'rcept_dt': '20260414',
                        }
                    ],
                }

        persister = Mock()
        service = DartDisclosureSyncService(
            client=DuplicatePageClient(),
            repository=FakeSyncRepository(),
            dilution_evidence_persister=persister,
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                }
            ],
            start_date=date(2025, 4, 14),
            end_date=date(2026, 4, 14),
        )

        self.assertEqual(result['status'], 'failed')
        self.assertIn('duplicate rcept_no', result['corporation_errors'][0]['error'])
        persister.assert_called_once()
        self.assertFalse(
            persister.call_args.kwargs['pagination_completed']
        )

    async def test_corp_scan_rejects_short_successful_page(self):
        class ShortPageClient:
            async def fetch_disclosure_list(self, **kwargs):
                return {
                    'status': '000',
                    'total_count': '3',
                    'page_no': '1',
                    'page_count': '2',
                    'list': [
                        {
                            'rcept_no': '20260414000001',
                            'corp_code': '00126380',
                            'report_nm': '정기주주총회 결과',
                            'rcept_dt': '20260414',
                        }
                    ],
                }

        persister = Mock()
        service = DartDisclosureSyncService(
            client=ShortPageClient(),
            repository=FakeSyncRepository(),
            dilution_evidence_persister=persister,
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                }
            ],
            start_date=date(2025, 4, 14),
            end_date=date(2026, 4, 14),
        )

        self.assertEqual(result['status'], 'failed')
        self.assertIn('item_count mismatch', result['corporation_errors'][0]['error'])
        persister.assert_called_once()
        self.assertFalse(
            persister.call_args.kwargs['pagination_completed']
        )

    async def test_corp_scan_isolates_pagination_integrity_failure(self):
        class MixedIntegrityClient:
            async def fetch_disclosure_list(self, **kwargs):
                if kwargs['corp_code'] == 'bad':
                    return {
                        'status': '000',
                        'total_count': '2',
                        'page_no': '1',
                        'page_count': '2',
                        'list': [],
                    }
                return {
                    'status': '000',
                    'total_count': '0',
                    'page_no': '1',
                    'page_count': '0',
                    'list': [],
                }

        persister = Mock(
            return_value={
                'positive_event_count': 0,
                'negative_coverage_count': 1,
                'rows_upserted': 1,
            }
        )
        service = DartDisclosureSyncService(
            client=MixedIntegrityClient(),
            repository=FakeSyncRepository(),
            dilution_evidence_persister=persister,
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {'corp_code': 'bad', 'stock_code': '000001'},
                {'corp_code': 'good', 'stock_code': '000002'},
            ],
            start_date=date(2025, 4, 14),
            end_date=date(2026, 4, 14),
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['failed_stock_codes'], ['000001'])
        self.assertEqual(result['corporations_processed'], 1)
        self.assertEqual(persister.call_count, 2)
        self.assertEqual(
            persister.call_args_list[-1].kwargs['corporation']['stock_code'],
            '000002',
        )
        self.assertFalse(
            persister.call_args_list[0].kwargs['pagination_completed']
        )

    async def test_short_corp_scan_persists_non_authoritative_negative_segment(self):
        signals_service = RecordingSignalsService()
        service = DartDisclosureSyncService(
            client=SinglePageSyncClient([]),
            repository=FakeSyncRepository(),
            dilution_evidence_persister=(
                signals_service.persist_share_issuance_scan_evidence
            ),
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                }
            ],
            start_date=date(2025, 4, 15),
            end_date=date(2026, 4, 14),
            trigger='active_test',
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['dilution_negative_coverage_rows'], 0)
        self.assertEqual(result['dilution_negative_segment_rows'], 1)
        self.assertEqual(len(signals_service.stored_dilution_rows), 2)
        stored = next(
            row
            for row in signals_service.stored_dilution_rows
            if str(row['event']['rcept_no']).startswith('DILCOV-')
        )
        self.assertFalse(stored['dilution_signal']['has_dilution_signal'])
        coverage = stored['dilution_signal']['payload']['evidence_coverage']
        self.assertEqual(coverage['status'], 'segment')
        self.assertEqual(coverage['window_start'], '2025-04-15')
        self.assertEqual(coverage['window_end'], '2026-04-14')

    async def test_contiguous_short_scan_promotes_prior_segment_to_complete_coverage(self):
        signals_service = RecordingSignalsService(
            previous_scan_evidence=[
                {
                    'id': 17,
                    'receipt_date': date(2026, 4, 13),
                    'payload': {
                        'evidence_coverage': {
                            'schema_version': 'dart_share_issuance_coverage_v1',
                            'rule_version': 'dart_share_issuance_rules_v1',
                            'status': 'segment',
                            'source': 'dart_disclosure_events',
                            'corp_code': '00126380',
                            'window_start': '2025-04-14',
                            'window_end': '2026-04-13',
                            'matched_event_count': 0,
                            'pagination_completed': True,
                            'scan_completed_at': (
                                '2026-04-13T08:00:00+00:00'
                            ),
                            'authority_policy_version': (
                                'krx_post_close_snapshot_overlap_v1'
                            ),
                            'authoritative_as_of_cutoff': True,
                            'late_arrival_reconciliation': (
                                'next_scan_one_day_overlap_v1'
                            ),
                        }
                    },
                }
            ]
        )
        service = DartDisclosureSyncService(
            client=SinglePageSyncClient([]),
            repository=FakeSyncRepository(),
            dilution_evidence_persister=(
                signals_service.persist_share_issuance_scan_evidence
            ),
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                }
            ],
            start_date=date(2026, 4, 14),
            end_date=date(2026, 4, 14),
        )

        self.assertEqual(result['dilution_negative_coverage_rows'], 1)
        self.assertEqual(result['dilution_negative_segment_rows'], 0)
        coverage = signals_service.stored_dilution_rows[0][
            'dilution_signal'
        ]['payload']['evidence_coverage']
        self.assertEqual(coverage['status'], 'complete')
        self.assertEqual(coverage['window_start'], '2025-04-14')
        self.assertEqual(coverage['window_end'], '2026-04-14')
        self.assertEqual(coverage['continued_from_evidence_id'], 17)

    async def test_noncontiguous_short_scan_does_not_promote_prior_segment(self):
        signals_service = RecordingSignalsService(
            previous_scan_evidence=[
                {
                    'id': 18,
                    'receipt_date': date(2026, 4, 12),
                    'payload': {
                        'evidence_coverage': {
                            'schema_version': 'dart_share_issuance_coverage_v1',
                            'rule_version': 'dart_share_issuance_rules_v1',
                            'status': 'segment',
                            'source': 'dart_disclosure_events',
                            'corp_code': '00126380',
                            'window_start': '2025-04-13',
                            'window_end': '2026-04-12',
                            'matched_event_count': 0,
                            'pagination_completed': True,
                        }
                    },
                }
            ]
        )
        service = DartDisclosureSyncService(
            client=SinglePageSyncClient([]),
            repository=FakeSyncRepository(),
            dilution_evidence_persister=(
                signals_service.persist_share_issuance_scan_evidence
            ),
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                }
            ],
            start_date=date(2026, 4, 14),
            end_date=date(2026, 4, 14),
        )

        self.assertEqual(result['dilution_negative_coverage_rows'], 0)
        self.assertEqual(result['dilution_negative_segment_rows'], 1)
        coverage = signals_service.stored_dilution_rows[0][
            'dilution_signal'
        ]['payload']['evidence_coverage']
        self.assertEqual(coverage['status'], 'segment')
        self.assertEqual(coverage['window_start'], '2026-04-14')
        self.assertNotIn('continued_from_evidence_id', coverage)

    async def test_failed_corp_scan_does_not_persist_share_issuance_coverage(self):
        class FailingClient:
            def __init__(self):
                self.calls = []

            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs)
                if kwargs['page_no'] == 1:
                    return {
                        'status': '000',
                        'total_count': '2',
                        'page_no': '1',
                        'page_count': '1',
                        'list': [
                            {
                                'rcept_no': '20260410000003',
                                'corp_code': '00126380',
                                'corp_name': '삼성전자',
                                'stock_code': '005930',
                                'corp_cls': 'Y',
                                'report_nm': '정기주주총회 결과',
                                'flr_nm': '삼성전자',
                                'rcept_dt': '20260410',
                                'rm': '',
                            }
                        ],
                    }
                raise DartAPIError('DART upstream unavailable')

        persister = Mock(
            return_value={
                'positive_event_count': 0,
                'negative_coverage_count': 1,
                'rows_upserted': 1,
            }
        )
        service = DartDisclosureSyncService(
            client=FailingClient(),
            repository=FakeSyncRepository(),
            dilution_evidence_persister=persister,
        )

        original_page_count = settings.dart_sync_page_count
        settings.dart_sync_page_count = 1
        try:
            result = await service.sync_disclosures_for_corporations(
                corporations=[
                    {'corp_code': '00126380', 'stock_code': '005930'}
                ],
                start_date=date(2025, 4, 14),
                end_date=date(2026, 4, 14),
                trigger='active_test',
            )
        finally:
            settings.dart_sync_page_count = original_page_count

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['pages_fetched'], 1)
        self.assertEqual(result['dilution_negative_coverage_rows'], 0)
        persister.assert_called_once()
        self.assertFalse(
            persister.call_args.kwargs['pagination_completed']
        )

    async def test_page_one_positive_event_survives_page_two_failure(self):
        class PositiveThenFailingClient:
            async def fetch_disclosure_list(self, **kwargs):
                if kwargs['page_no'] == 1:
                    return {
                        'status': '000',
                        'total_count': '2',
                        'page_no': '1',
                        'page_count': '1',
                        'list': [
                            {
                                'rcept_no': '20260410000009',
                                'corp_code': '00126380',
                                'corp_name': '삼성전자',
                                'stock_code': '005930',
                                'report_nm': '유상증자 결정',
                                'rcept_dt': '20260410',
                            }
                        ],
                    }
                raise DartAPIError('DART page two unavailable')

        signals_service = RecordingSignalsService()
        service = DartDisclosureSyncService(
            client=PositiveThenFailingClient(),
            repository=FakeSyncRepository(),
            dilution_evidence_persister=(
                signals_service.persist_share_issuance_scan_evidence
            ),
            dilution_positive_persister=(
                signals_service.persist_share_issuance_positive_events
            ),
        )
        original_page_count = settings.dart_sync_page_count
        settings.dart_sync_page_count = 1
        try:
            result = await service.sync_disclosures_for_corporations(
                corporations=[
                    {
                        'corp_code': '00126380',
                        'stock_code': '005930',
                        'corp_name': '삼성전자',
                    }
                ],
                start_date=date(2025, 4, 14),
                end_date=date(2026, 4, 14),
                trigger='active_test',
            )
        finally:
            settings.dart_sync_page_count = original_page_count

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['dilution_positive_events'], 1)
        self.assertEqual(result['dilution_negative_coverage_rows'], 0)
        positive_rows = [
            row
            for row in signals_service.stored_dilution_rows
            if row['dilution_signal']['has_dilution_signal']
        ]
        self.assertEqual(len(positive_rows), 1)
        self.assertEqual(
            positive_rows[0]['event']['rcept_no'],
            '20260410000009',
        )
        self.assertFalse(
            positive_rows[0]['dilution_signal']['payload'][
                'pagination_completed'
            ]
        )

    async def test_sync_disclosures_for_corporations_reports_upstream_failures_as_retryable_run_failure(self):
        class FailingClient(FakeSyncClient):
            async def fetch_disclosure_list(self, **kwargs):
                raise DartAPIError('DART upstream unavailable')

        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=FailingClient(), repository=repository)

        result = await service.sync_disclosures_for_corporations(
            corporations=[{'corp_code': '00126380', 'stock_code': '005930'}],
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
        )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['corporations_failed'], 1)
        self.assertIn('unavailable', result['corporation_errors'][0]['error'])
        self.assertEqual(repository.completed_runs[-1][1]['status'], 'failed')

    async def test_sync_disclosures_for_corporations_marks_mixed_company_failures_partial(self):
        class PartiallyFailingClient(FakeSyncClient):
            async def fetch_disclosure_list(self, **kwargs):
                if kwargs['corp_code'] == 'broken-corp':
                    raise RuntimeError('company lookup failed')
                return await super().fetch_disclosure_list(**kwargs)

        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=PartiallyFailingClient(), repository=repository)

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {'corp_code': '00126380', 'stock_code': '005930'},
                {'corp_code': 'broken-corp', 'stock_code': '999999'},
            ],
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['corporations_processed'], 1)
        self.assertEqual(result['corporations_failed'], 1)
        self.assertEqual(result['corporation_errors'][0]['stock_code'], '999999')
        self.assertEqual(result['failed_stock_codes'], ['999999'])
        self.assertEqual(repository.completed_runs[-1][1]['status'], 'partial')

    async def test_sync_disclosures_for_corporations_marks_all_company_failures_failed(self):
        class FailingCompanyClient(FakeSyncClient):
            async def fetch_disclosure_list(self, **kwargs):
                raise RuntimeError(f"company lookup failed: {kwargs['corp_code']}")

        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=FailingCompanyClient(), repository=repository)

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {'corp_code': 'broken-a', 'stock_code': '999998'},
                {'corp_code': 'broken-b', 'stock_code': '999999'},
            ],
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
        )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['corporations_processed'], 0)
        self.assertEqual(result['corporations_failed'], 2)
        self.assertEqual(repository.completed_runs[-1][1]['status'], 'failed')

    async def test_sync_disclosures_for_corporations_isolates_one_company_upstream_failure(self):
        class PartiallyUnavailableClient(FakeSyncClient):
            async def fetch_disclosure_list(self, **kwargs):
                if kwargs['corp_code'] == 'unavailable-corp':
                    raise DartAPIError('DART company unavailable')
                return await super().fetch_disclosure_list(**kwargs)

        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=PartiallyUnavailableClient(), repository=repository)

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {'corp_code': '00126380', 'stock_code': '005930'},
                {'corp_code': 'unavailable-corp', 'stock_code': '999999'},
            ],
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['corporations_processed'], 1)
        self.assertEqual(result['corporations_failed'], 1)
        self.assertEqual(repository.completed_runs[-1][1]['status'], 'partial')

    async def test_sync_disclosures_for_corporations_continues_after_one_intermittent_provider_failure(self):
        class IntermittentlyUnavailableClient(FakeSyncClient):
            async def fetch_disclosure_list(self, **kwargs):
                if kwargs['corp_code'] == 'unavailable-corp':
                    raise DartAPIError('DART company unavailable')
                return await super().fetch_disclosure_list(**kwargs)

        client = IntermittentlyUnavailableClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {'corp_code': 'unavailable-corp', 'stock_code': '999999'},
                {'corp_code': '00126380', 'stock_code': '005930'},
            ],
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['corporations_attempted'], 2)
        self.assertEqual(result['corporations_processed'], 1)
        self.assertEqual(result['corporations_failed'], 1)
        self.assertEqual(result['corporations_unattempted'], 0)
        self.assertEqual(result['failed_stock_codes'], ['999999'])
        self.assertEqual(result['unattempted_stock_codes'], [])

    async def test_sync_disclosures_for_corporations_stops_after_three_consecutive_provider_failures(self):
        class SystemicallyUnavailableClient(FakeSyncClient):
            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs)
                raise DartAPIError('DART provider unavailable')

        client = SystemicallyUnavailableClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)
        corporations = [
            {'corp_code': f'broken-{index}', 'stock_code': f'{index:06d}'}
            for index in range(1, 6)
        ]

        result = await service.sync_disclosures_for_corporations(
            corporations=corporations,
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
        )

        self.assertEqual(len(client.calls), 3)
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['corporations_attempted'], 3)
        self.assertEqual(result['corporations_failed'], 3)
        self.assertEqual(result['corporations_unattempted'], 2)
        self.assertEqual(result['failed_stock_codes'], ['000001', '000002', '000003'])
        self.assertEqual(result['unattempted_stock_codes'], ['000004', '000005'])
        self.assertEqual(
            result['incomplete_stock_codes'],
            ['000001', '000002', '000003', '000004', '000005'],
        )
        completed = repository.completed_runs[-1][1]
        self.assertEqual(completed['status'], 'failed')
        self.assertEqual(completed['run_metadata']['circuit_breaker']['threshold'], 3)
        self.assertTrue(completed['run_metadata']['circuit_breaker']['tripped'])

    async def test_sync_disclosures_for_corporations_stops_after_systemic_persistence_failure(self):
        class EmptyClient:
            def __init__(self):
                self.calls = []

            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs)
                return {
                    'status': '000',
                    'total_count': '0',
                    'page_no': '1',
                    'page_count': '100',
                    'list': [],
                }

        client = EmptyClient()
        repository = FakeSyncRepository()
        outage = OperationalError(
            'persist dilution authority',
            {},
            ConnectionError('database connection reset'),
            connection_invalidated=True,
        )
        positive_persister = Mock(side_effect=outage)
        incomplete_persister = Mock()
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
            dilution_positive_persister=positive_persister,
            dilution_evidence_persister=incomplete_persister,
        )
        corporations = [
            {'corp_code': f'corp-{index}', 'stock_code': f'{index:06d}'}
            for index in range(1, 4)
        ]

        result = await service.sync_disclosures_for_corporations(
            corporations=corporations,
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
        )

        self.assertEqual(len(client.calls), 1)
        positive_persister.assert_called_once()
        incomplete_persister.assert_not_called()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['corporations_attempted'], 1)
        self.assertEqual(result['corporations_failed'], 1)
        self.assertEqual(result['corporations_unattempted'], 2)
        self.assertEqual(result['failed_stock_codes'], ['000001'])
        self.assertEqual(
            result['unattempted_stock_codes'],
            ['000002', '000003'],
        )
        self.assertTrue(result['circuit_breaker']['tripped'])

    async def test_sync_disclosures_for_corporations_stops_after_schema_persistence_failure(self):
        class EmptyClient:
            def __init__(self):
                self.calls = []

            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs)
                return {
                    'status': '000',
                    'total_count': '0',
                    'page_no': '1',
                    'page_count': '100',
                    'list': [],
                }

        client = EmptyClient()
        repository = FakeSyncRepository()
        schema_failure = ProgrammingError(
            'persist dilution authority',
            {},
            Exception('relation does not exist'),
        )
        positive_persister = Mock(side_effect=schema_failure)
        incomplete_persister = Mock()
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
            dilution_positive_persister=positive_persister,
            dilution_evidence_persister=incomplete_persister,
        )
        corporations = [
            {'corp_code': f'corp-{index}', 'stock_code': f'{index:06d}'}
            for index in range(1, 4)
        ]

        result = await service.sync_disclosures_for_corporations(
            corporations=corporations,
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
        )

        self.assertEqual(len(client.calls), 1)
        positive_persister.assert_called_once()
        incomplete_persister.assert_not_called()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['corporations_attempted'], 1)
        self.assertEqual(result['corporations_failed'], 1)
        self.assertEqual(result['corporations_unattempted'], 2)
        self.assertEqual(result['failed_stock_codes'], ['000001'])
        self.assertEqual(
            result['unattempted_stock_codes'],
            ['000002', '000003'],
        )
        self.assertTrue(result['circuit_breaker']['tripped'])

    async def test_sync_disclosures_for_corporations_does_not_trip_breaker_for_corp_specific_errors(self):
        class CorpSpecificFailureClient(FakeSyncClient):
            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs)
                if kwargs['corp_code'].startswith('broken-'):
                    raise DartAPIError(
                        'DART request failed [014]: File does not exist'
                    )
                return {
                    'status': '000',
                    'total_count': '0',
                    'page_no': '1',
                    'page_count': '0',
                    'list': [],
                }

        client = CorpSpecificFailureClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {'corp_code': 'broken-1', 'stock_code': '000001'},
                {'corp_code': 'broken-2', 'stock_code': '000002'},
                {'corp_code': 'broken-3', 'stock_code': '000003'},
                {'corp_code': 'healthy', 'stock_code': '005930'},
            ],
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
        )

        self.assertEqual(len(client.calls), 4)
        self.assertEqual(result['corporations_processed'], 1)
        self.assertEqual(result['corporations_failed'], 3)
        self.assertEqual(result['corporations_unattempted'], 0)
        self.assertFalse(result['circuit_breaker']['tripped'])

    async def test_sync_disclosures_for_corporations_trips_breaker_for_transport_timeouts(self):
        class TimeoutClient(FakeSyncClient):
            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs)
                raise TimeoutError('DART request timed out')

        client = TimeoutClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)
        corporations = [
            {'corp_code': f'corp-{index}', 'stock_code': f'{index:06d}'}
            for index in range(1, 5)
        ]

        result = await service.sync_disclosures_for_corporations(
            corporations=corporations,
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
        )

        self.assertEqual(len(client.calls), 3)
        self.assertTrue(result['circuit_breaker']['tripped'])
        self.assertEqual(result['corporations_unattempted'], 1)

    async def test_sync_disclosures_for_corporations_bounds_concurrency_in_ordered_waves(self):
        class ConcurrentClient:
            def __init__(self):
                self.active = 0
                self.max_active = 0
                self.calls = []

            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs['corp_code'])
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                try:
                    await asyncio.sleep(0)
                    return {
                        'status': '000',
                        'total_count': '0',
                        'page_no': '1',
                        'page_count': '100',
                        'list': [],
                    }
                finally:
                    self.active -= 1

        client = ConcurrentClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)
        corporations = [
            {'corp_code': f'corp-{index}', 'stock_code': f'{index:06d}'}
            for index in range(1, 6)
        ]

        result = await service.sync_disclosures_for_corporations(
            corporations=corporations,
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
            concurrency=3,
        )

        self.assertEqual(client.max_active, 3)
        self.assertEqual(client.calls, [f'corp-{index}' for index in range(1, 6)])
        self.assertEqual(result['concurrency'], 3)
        self.assertEqual(result['corporations_attempted'], 5)
        self.assertEqual(result['corporations_processed'], 5)
        self.assertEqual(
            repository.created_runs[-1]['run_metadata']['concurrency'],
            3,
        )
        self.assertEqual(
            repository.completed_runs[-1][1]['run_metadata']['concurrency'],
            3,
        )

    async def test_sync_disclosures_for_corporations_accounts_out_of_order_results_once_in_input_order(self):
        class OutOfOrderClient:
            def __init__(self):
                self.completion_order = []

            async def fetch_disclosure_list(self, **kwargs):
                index = int(kwargs['corp_code'].split('-')[-1])
                await asyncio.sleep((5 - index) * 0.001)
                self.completion_order.append(kwargs['corp_code'])
                if index in {2, 4}:
                    raise DartAPIError(
                        'DART request failed [014]: File does not exist'
                    )
                return {
                    'status': '000',
                    'total_count': '0',
                    'page_no': '1',
                    'page_count': '100',
                    'list': [],
                }

        client = OutOfOrderClient()
        service = DartDisclosureSyncService(
            client=client,
            repository=FakeSyncRepository(),
        )
        corporations = [
            {'corp_code': f'corp-{index}', 'stock_code': f'{index:06d}'}
            for index in range(1, 5)
        ]

        result = await service.sync_disclosures_for_corporations(
            corporations=corporations,
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
            concurrency=4,
        )

        self.assertNotEqual(
            client.completion_order,
            [f'corp-{index}' for index in range(1, 5)],
        )
        self.assertEqual(result['corporations_attempted'], 4)
        self.assertEqual(result['corporations_processed'], 2)
        self.assertEqual(result['corporations_failed'], 2)
        self.assertEqual(
            [item['stock_code'] for item in result['corporation_errors']],
            ['000002', '000004'],
        )
        self.assertEqual(result['failed_stock_codes'], ['000002', '000004'])
        self.assertEqual(result['successful_stock_codes'], ['000001', '000003'])

    async def test_sync_disclosures_for_corporations_finishes_breaker_wave_before_stopping(self):
        class SystemicallyUnavailableClient:
            def __init__(self):
                self.calls = []

            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs['corp_code'])
                await asyncio.sleep(0)
                if kwargs['corp_code'] != 'corp-4':
                    raise DartAPIError('DART provider unavailable')
                return {
                    'status': '000',
                    'total_count': '0',
                    'page_no': '1',
                    'page_count': '100',
                    'list': [],
                }

        client = SystemicallyUnavailableClient()
        service = DartDisclosureSyncService(
            client=client,
            repository=FakeSyncRepository(),
        )
        corporations = [
            {'corp_code': f'corp-{index}', 'stock_code': f'{index:06d}'}
            for index in range(1, 8)
        ]

        result = await service.sync_disclosures_for_corporations(
            corporations=corporations,
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
            concurrency=4,
        )

        self.assertEqual(client.calls, [f'corp-{index}' for index in range(1, 5)])
        self.assertEqual(result['corporations_attempted'], 4)
        self.assertEqual(result['corporations_processed'], 1)
        self.assertEqual(result['corporations_failed'], 3)
        self.assertEqual(result['corporations_unattempted'], 3)
        self.assertEqual(
            result['failed_stock_codes'],
            ['000001', '000002', '000003'],
        )
        self.assertEqual(
            result['unattempted_stock_codes'],
            ['000005', '000006', '000007'],
        )
        self.assertTrue(result['circuit_breaker']['tripped'])
        self.assertEqual(
            result['circuit_breaker']['consecutive_provider_failures'],
            3,
        )

    async def test_sync_disclosures_for_corporations_finishes_persistence_failure_wave_before_stopping(self):
        class EmptyClient:
            def __init__(self):
                self.calls = []

            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs['corp_code'])
                await asyncio.sleep(0)
                return {
                    'status': '000',
                    'total_count': '0',
                    'page_no': '1',
                    'page_count': '100',
                    'list': [],
                }

        outage = OperationalError(
            'persist dilution authority',
            {},
            ConnectionError('database connection reset'),
            connection_invalidated=True,
        )
        persisted_corp_codes = []

        def persist_positive(**kwargs):
            persisted_corp_codes.append(kwargs['corporation']['corp_code'])
            if kwargs['corporation']['corp_code'] == 'corp-2':
                raise outage
            return {'positive_event_count': 0, 'rows_upserted': 0}

        client = EmptyClient()
        service = DartDisclosureSyncService(
            client=client,
            repository=FakeSyncRepository(),
            dilution_positive_persister=persist_positive,
        )
        corporations = [
            {'corp_code': f'corp-{index}', 'stock_code': f'{index:06d}'}
            for index in range(1, 6)
        ]

        result = await service.sync_disclosures_for_corporations(
            corporations=corporations,
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
            concurrency=3,
        )

        self.assertEqual(client.calls, ['corp-1', 'corp-2', 'corp-3'])
        self.assertEqual(result['corporations_attempted'], 3)
        self.assertEqual(result['corporations_processed'], 1)
        self.assertEqual(result['corporations_failed'], 2)
        self.assertEqual(result['failed_stock_codes'], ['000002', '000003'])
        self.assertEqual(set(persisted_corp_codes), {'corp-1', 'corp-2'})
        self.assertIn(
            'skipped persistence after systemic persistence failure',
            result['corporation_errors'][1]['error'],
        )
        self.assertEqual(result['unattempted_stock_codes'], ['000004', '000005'])
        self.assertTrue(result['circuit_breaker']['tripped'])

    async def test_incomplete_marker_persistence_failure_stops_after_current_wave(self):
        class LocallyFailingClient:
            def __init__(self):
                self.calls = []

            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append(kwargs['corp_code'])
                await asyncio.sleep(0)
                if kwargs['corp_code'] == 'corp-1':
                    raise DartAPIError(
                        'DART request failed [014]: File does not exist'
                    )
                return {
                    'status': '000',
                    'total_count': '0',
                    'page_no': '1',
                    'page_count': '100',
                    'list': [],
                }

        marker_outage = OperationalError(
            'persist incomplete marker',
            {},
            ConnectionError('database connection reset'),
            connection_invalidated=True,
        )

        def persist_evidence(**kwargs):
            if not kwargs['pagination_completed']:
                raise marker_outage
            return {
                'negative_coverage_count': 0,
                'negative_segment_count': 0,
                'rows_upserted': 0,
            }

        client = LocallyFailingClient()
        service = DartDisclosureSyncService(
            client=client,
            repository=FakeSyncRepository(),
            dilution_evidence_persister=persist_evidence,
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {'corp_code': f'corp-{index}', 'stock_code': f'{index:06d}'}
                for index in range(1, 4)
            ],
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
            concurrency=2,
        )

        self.assertEqual(client.calls, ['corp-1', 'corp-2'])
        self.assertEqual(result['corporations_attempted'], 2)
        self.assertEqual(result['corporations_failed'], 2)
        self.assertEqual(result['unattempted_stock_codes'], ['000003'])
        self.assertEqual(len(result['incomplete_marker_errors']), 1)
        self.assertEqual(
            result['circuit_breaker']['reason'],
            'systemic_persistence_failure',
        )

    async def test_systemic_fetch_failure_is_not_masked_by_local_page_persistence_error(self):
        class PartialThenTimeoutClient:
            def __init__(self):
                self.calls = []

            async def fetch_disclosure_list(self, **kwargs):
                self.calls.append((kwargs['corp_code'], kwargs['page_no']))
                await asyncio.sleep(0)
                if kwargs['page_no'] == 2:
                    raise TimeoutError('DART provider timed out')
                index = int(kwargs['corp_code'].split('-')[-1])
                return {
                    'status': '000',
                    'total_count': '2',
                    'page_no': '1',
                    'page_count': '1',
                    'list': [
                        {
                            'rcept_no': f'20260410{index:06d}',
                            'corp_code': kwargs['corp_code'],
                            'corp_name': kwargs['corp_code'],
                            'stock_code': f'{index:06d}',
                            'report_nm': '유상증자 결정',
                            'rcept_dt': '20260410',
                        }
                    ],
                }

        client = PartialThenTimeoutClient()
        service = DartDisclosureSyncService(
            client=client,
            repository=FakeSyncRepository(),
            dilution_positive_persister=Mock(
                side_effect=ValueError('local row rejected')
            ),
        )

        result = await service.sync_disclosures_for_corporations(
            corporations=[
                {'corp_code': f'corp-{index}', 'stock_code': f'{index:06d}'}
                for index in range(1, 7)
            ],
            start_date=date(2025, 1, 1),
            end_date=date(2026, 4, 7),
            trigger='active_test',
            concurrency=3,
        )

        self.assertEqual(result['corporations_attempted'], 3)
        self.assertEqual(result['corporations_failed'], 3)
        self.assertEqual(
            result['unattempted_stock_codes'],
            ['000004', '000005', '000006'],
        )
        self.assertTrue(result['circuit_breaker']['tripped'])
        self.assertEqual(
            result['circuit_breaker']['reason'],
            'consecutive_systemic_provider_failures',
        )

    async def test_sync_disclosures_for_corporations_rejects_invalid_concurrency(self):
        client = FakeSyncClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(
            client=client,
            repository=repository,
        )

        for concurrency in (0, 9):
            with self.subTest(concurrency=concurrency):
                with self.assertRaisesRegex(
                    ValueError,
                    'concurrency must be between 1 and 8',
                ):
                    await service.sync_disclosures_for_corporations(
                        corporations=[
                            {'corp_code': '00126380', 'stock_code': '005930'}
                        ],
                        start_date=date(2025, 1, 1),
                        end_date=date(2026, 4, 7),
                        concurrency=concurrency,
                    )

        self.assertEqual(client.calls, [])
        self.assertEqual(repository.created_runs, [])

    async def test_inspect_sync_returns_repository_views(self):
        service = DartDisclosureSyncService(client=FakeSyncClient(), repository=FakeSyncRepository())

        result = service.inspect_sync(limit=5, events_limit=2, financial_only=True)

        self.assertEqual(result['latest_run']['id'], 41)
        self.assertEqual(result['summary']['financial_report_events'], 2)
        self.assertEqual(len(result['events']), 1)


class FilingClassificationTests(unittest.TestCase):
    def test_classify_filing_recognizes_periodic_financial_reports(self):
        self.assertEqual(classify_filing('[기재정정] 사업보고서 (2025.12)')['filing_category'], 'annual_report')
        self.assertEqual(classify_filing('반기보고서 (2025.06)')['filing_category'], 'semiannual_report')
        self.assertEqual(classify_filing('분기보고서 (2025.03)')['filing_category'], 'quarterly_report')

    def test_classify_filing_leaves_non_periodic_disclosures_as_other(self):
        result = classify_filing('감사보고서 제출')

        self.assertEqual(result['filing_category'], 'other')
        self.assertFalse(result['is_financial_report'])


if __name__ == '__main__':
    unittest.main()
