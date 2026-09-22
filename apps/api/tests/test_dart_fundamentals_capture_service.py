import unittest
import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

from app.agent_context.prerequisite_service import FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
from app.dart.fundamentals_capture_service import (
    DartFundamentalsCaptureService,
    _bounded_missing_probe_codes,
    build_row_payloads,
    infer_fundamentals_request,
)


def _feedback_prerequisites(*, status: str = 'ready', missing_stage_ids: list[str] | None = None) -> dict:
    missing_stage_ids = list(missing_stage_ids or [])
    ready = status == 'ready'
    return {
        'market_date': '2026-04-14',
        'fs_div': 'CFS',
        'status': status,
        'ready': ready,
        'required_stage_ids': list(FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER),
        'ready_stage_ids': list(FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER) if ready else [],
        'waiting_stage_ids': missing_stage_ids if status == 'waiting' else [],
        'blocked_stage_ids': missing_stage_ids if status == 'blocked' else [],
        'missing_prerequisites': [
            {
                'stage': stage_id,
                'label': stage_id.replace('_', ' '),
                'status': 'running' if status == 'waiting' else 'partial',
                'gate_status': status,
                'current_activity': f'{status} {stage_id}',
                'details': {},
            }
            for stage_id in missing_stage_ids
        ],
        'stages': {},
    }


def test_bounded_missing_probe_rotates_by_batch_without_daily_starvation() -> None:
    candidates = [f'{index:06d}' for index in range(300)]

    first = _bounded_missing_probe_codes(
        candidates,
        probe_limit=100,
        market_date=date(2026, 4, 14),
    )
    same_day = _bounded_missing_probe_codes(
        candidates,
        probe_limit=100,
        market_date=date(2026, 4, 14),
    )
    next_day = _bounded_missing_probe_codes(
        candidates,
        probe_limit=100,
        market_date=date(2026, 4, 15),
    )

    assert first == same_day
    assert len(first) == 100
    assert set(first).isdisjoint(next_day)


class FakeCaptureClient:
    def __init__(self):
        self.calls = []
        self.overview_calls = []
        self.lookup_calls = []

    async def fetch_company_overview(self, corp_code):
        self.overview_calls.append(corp_code)
        if corp_code == 'quarterly-non-calendar':
            return {'acc_mt': '03'}
        return {'acc_mt': '12'}

    async def fetch_financial_statements(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs['corp_code'] == 'fail-me':
            raise RuntimeError('upstream timeout')
        if kwargs['corp_code'] == 'no-data':
            return {
                'status': '013',
                'message': 'No data',
                'list': [],
            }
        return {
            'status': '000',
            'message': 'OK',
            'list': [
                {
                    'rcept_no': '20260407000003',
                    'reprt_code': kwargs['reprt_code'],
                    'bsns_year': kwargs['bsns_year'],
                    'sj_div': 'IS',
                    'sj_nm': '손익계산서',
                    'account_id': 'ifrs-full_Revenue',
                    'account_nm': '매출액',
                    'thstrm_nm': '당기',
                    'thstrm_amount': '100,000',
                    'frmtrm_nm': '전기',
                    'frmtrm_amount': '90,000',
                    'ord': '1',
                    'currency': 'KRW',
                }
            ],
        }

    async def lookup_corp_code(self, *, stock_code=None, corp_code=None, corp_name=None):
        self.lookup_calls.append({'stock_code': stock_code, 'corp_code': corp_code, 'corp_name': corp_name})
        if stock_code == '999999':
            raise LookupError('Unknown stock_code: 999999')
        return {
            'corp_code': f'corp-{stock_code}',
            'corp_name': f'Corp {stock_code}',
            'stock_code': stock_code,
            'modify_date': '20260407',
        }


class FakeCaptureRepository:
    def __init__(self):
        self.created_runs = []
        self.completed_runs = []
        self.progress_updates = []
        self.stale_run_failures = []
        self.created_refresh_runs = []
        self.completed_refresh_runs = []
        self.refresh_progress_updates = []
        self.stale_refresh_run_failures = []
        self.latest_refresh_run = None
        self.symbol_states = {}
        self.successes = []
        self.failures = []
        self.terminals = []
        self.snapshots = []
        self.candidate_call_kwargs = []

    def create_capture_run(self, **kwargs):
        self.created_runs.append(kwargs)
        return 88

    def complete_capture_run(self, run_id, **kwargs):
        self.completed_runs.append((run_id, kwargs))

    def update_capture_run_progress(self, run_id, **kwargs):
        self.progress_updates.append((run_id, kwargs))

    def fail_stale_capture_runs(self, *, stale_before, error_message):
        self.stale_run_failures.append((stale_before, error_message))
        return []

    def create_refresh_run(self, **kwargs):
        self.created_refresh_runs.append(kwargs)
        return 188

    def complete_refresh_run(self, run_id, **kwargs):
        self.completed_refresh_runs.append((run_id, kwargs))

    def update_refresh_run_progress(self, run_id, **kwargs):
        self.refresh_progress_updates.append((run_id, kwargs))

    def fail_stale_refresh_runs(self, *, stale_before, error_message):
        self.stale_refresh_run_failures.append((stale_before, error_message))
        return []

    def get_latest_refresh_run(self, **kwargs):
        return self.latest_refresh_run

    def list_refresh_runs(self, **kwargs):
        return [self.latest_refresh_run] if self.latest_refresh_run else []

    def list_symbol_fundamentals_states(self, *, stock_codes, fs_div):
        return {
            stock_code: self.symbol_states.get(
                stock_code,
                {
                    'stock_code': stock_code,
                    'has_fundamentals': False,
                    'latest_fundamentals_receipt_date': None,
                    'latest_snapshot': None,
                },
            )
            for stock_code in stock_codes
        }

    def list_capture_candidates(
        self,
        *,
        limit=20,
        force=False,
        stock_codes=None,
        receipt_date_from=None,
        receipt_date_to=None,
        event_ids=None,
        oldest_first=False,
    ):
        self.candidate_call_kwargs.append(
            {
                'limit': limit,
                'force': force,
                'stock_codes': stock_codes,
                'receipt_date_from': receipt_date_from,
                'receipt_date_to': receipt_date_to,
                'event_ids': event_ids,
                'oldest_first': oldest_first,
            }
        )
        events = [
            {
                'id': 1,
                'rcept_no': '20260407000003',
                'corp_code': '00126380',
                'corp_name': '삼성전자',
                'stock_code': '005930',
                'report_nm': '사업보고서 (2025.12)',
                'filing_category': 'annual_report',
                'fundamentals_capture_status': 'pending',
            },
            {
                'id': 2,
                'rcept_no': '20260407000004',
                'corp_code': 'fail-me',
                'corp_name': '실패회사',
                'stock_code': '000001',
                'report_nm': '반기보고서 (2025.06)',
                'filing_category': 'semiannual_report',
                'fundamentals_capture_status': 'failed',
            },
            {
                'id': 3,
                'rcept_no': '20260407000005',
                'corp_code': 'quarterly-non-calendar',
                'corp_name': '비캘린더법인',
                'stock_code': '000002',
                'report_nm': '분기보고서 (2025.12)',
                'filing_category': 'quarterly_report',
                'fundamentals_capture_status': 'pending',
            },
        ]
        if receipt_date_from is not None or receipt_date_to is not None or event_ids is not None:
            events.extend(
                [
                    {
                        'id': 4,
                        'rcept_no': '20260410000001',
                        'corp_code': 'corp-005930',
                        'corp_name': '삼성전자',
                        'stock_code': '005930',
                        'report_nm': '분기보고서 (2026.03)',
                        'filing_category': 'quarterly_report',
                        'fundamentals_capture_status': 'pending',
                        'receipt_date': '2026-04-10',
                    },
                    {
                        'id': 5,
                        'rcept_no': '20250320000001',
                        'corp_code': 'corp-123456',
                        'corp_name': 'Missing Corp',
                        'stock_code': '123456',
                        'report_nm': '사업보고서 (2024.12)',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'pending',
                        'receipt_date': '2025-03-20',
                    },
                    {
                        'id': 6,
                        'rcept_no': '20260301000001',
                        'corp_code': 'corp-005930',
                        'corp_name': '삼성전자',
                        'stock_code': '005930',
                        'report_nm': '사업보고서 (2025.12)',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'failed',
                        'receipt_date': '2026-03-01',
                    },
                    {
                        'id': 7,
                        'rcept_no': '20260412000001',
                        'corp_code': 'corp-222222',
                        'corp_name': 'Retry Corp',
                        'stock_code': '222222',
                        'report_nm': '사업보고서 (2025.12)',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'failed',
                        'receipt_date': '2026-04-12',
                    },
                    {
                        'id': 8,
                        'rcept_no': '20260413000001',
                        'corp_code': 'no-data',
                        'corp_name': 'No Data Corp',
                        'stock_code': '333333',
                        'report_nm': '사업보고서 (2025.12)',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'pending',
                        'receipt_date': '2026-04-13',
                    },
                    {
                        'id': 9,
                        'rcept_no': '20260414000001',
                        'corp_code': 'corp-444444',
                        'corp_name': 'Bad Mapping Corp',
                        'stock_code': '444444',
                        'report_nm': 'Unparseable Report',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'pending',
                        'receipt_date': '2026-04-14',
                    },
                    {
                        'id': 10,
                        'rcept_no': '20260409000001',
                        'corp_code': 'corp-555555',
                        'corp_name': 'Terminal Baseline Corp',
                        'stock_code': '555555',
                        'report_nm': '사업보고서 (2025.12)',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'failed',
                        'receipt_date': '2026-04-09',
                    },
                    {
                        'id': 11,
                        'rcept_no': '20260412000002',
                        'corp_code': 'corp-555555',
                        'corp_name': 'Terminal Baseline Corp',
                        'stock_code': '555555',
                        'report_nm': '사업보고서 (2025.12)',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'failed',
                        'receipt_date': '2026-04-12',
                    },
                ]
            )
        if stock_codes:
            events = [event for event in events if event['stock_code'] in stock_codes]
        if receipt_date_from is not None:
            events = [
                event
                for event in events
                if event.get('receipt_date') and date.fromisoformat(event['receipt_date']) >= receipt_date_from
            ]
        if receipt_date_to is not None:
            events = [
                event
                for event in events
                if event.get('receipt_date') and date.fromisoformat(event['receipt_date']) <= receipt_date_to
            ]
        if event_ids is not None:
            events = [event for event in events if event['id'] in event_ids]
        return events[:limit] if limit is not None else events

    def replace_snapshot_dataset(self, **kwargs):
        self.snapshots.append(kwargs)
        return 101

    def mark_event_capture_success(self, event_id):
        self.successes.append(event_id)

    def mark_event_capture_failed(self, event_id, *, error_message):
        self.failures.append((event_id, error_message))

    def mark_event_capture_terminal(self, event_id, *, capture_status, error_message):
        self.terminals.append((event_id, capture_status, error_message))

    def get_latest_capture_run(self):
        return {'id': 88, 'status': 'completed'}

    def list_capture_runs(self, *, limit=10):
        return [{'id': 88, 'status': 'completed'}][:limit]

    def get_capture_summary(self):
        return {'financial_event_capture_status_counts': {'captured': 1, 'failed': 1}}

    def list_financial_events(self, *, limit=25, capture_status=None):
        events = [
            {'id': 1, 'rcept_no': '20260407000003', 'fundamentals_capture_status': 'captured'},
            {'id': 2, 'rcept_no': '20260407000004', 'fundamentals_capture_status': 'failed'},
        ]
        if capture_status:
            events = [event for event in events if event['fundamentals_capture_status'] == capture_status]
        return events[:limit]


class DartFundamentalsCaptureServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dart_signals_patcher = patch(
            'app.dart.fundamentals_capture_service.dart_signals_service.capture_signals_for_event',
            new_callable=AsyncMock,
            return_value={
                'rcept_no': '20260407000003',
                'audit_signal': {
                    'opinion_code': 'unqualified',
                    'opinion_label': '적정',
                    'has_audit_issue': False,
                },
                'dilution_signal': None,
            },
        )
        self.mock_capture_dart_signals = self.dart_signals_patcher.start()
        self.addCleanup(self.dart_signals_patcher.stop)
        self.piotroski_filter_patcher = patch(
            'app.dart.fundamentals_capture_service.daily_piotroski_filter_service.rebuild_for_market_date',
            return_value={
                'market_date': '2026-04-14',
                'source': 'strategy_piotroski_v1',
                'symbols_evaluated': 3,
                'symbols_passed': 1,
                'symbols_not_passed': 2,
                'results_persisted': 3,
                'results_failed': 0,
                'failed_symbols': [],
            },
        )
        self.mock_piotroski_filter = self.piotroski_filter_patcher.start()
        self.addCleanup(self.piotroski_filter_patcher.stop)

    async def test_capture_annual_fundamentals_captures_audit_signal_before_marking_success(self):
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=repository,
        )

        result = await service.capture_recent_fundamentals(
            limit=1,
            force=False,
            trigger='test',
            fs_div='CFS',
        )

        self.mock_capture_dart_signals.assert_awaited_once_with(
            rcept_no='20260407000003',
        )
        self.assertEqual(repository.successes, [1])
        self.assertEqual(result['results'][0]['audit_signal_status'], 'captured')

    async def test_capture_annual_fundamentals_keeps_event_retryable_when_audit_evidence_is_missing(self):
        self.mock_capture_dart_signals.return_value = {
            'rcept_no': '20260407000003',
            'audit_signal': None,
            'dilution_signal': None,
        }
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=repository,
        )

        result = await service.capture_recent_fundamentals(
            limit=1,
            force=False,
            trigger='test',
            fs_div='CFS',
        )

        self.assertEqual(result['events_captured'], 0)
        self.assertEqual(result['events_failed'], 1)
        self.assertEqual(repository.successes, [])
        self.assertEqual(repository.failures[0][0], 1)
        self.assertIn('audit evidence', repository.failures[0][1])
        self.assertEqual(repository.snapshots, [])
        self.assertEqual(result['results'][0]['status'], 'failed')

    async def test_capture_recent_fundamentals_persists_snapshots_and_marks_event_status(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(client=client, repository=repository)

        with patch('app.dart.fundamentals_capture_service.log_fetch_event') as log_fetch_event:
            result = await service.capture_recent_fundamentals(limit=3, force=False, trigger='test', fs_div='CFS')

        self.assertEqual(result['run_id'], 88)
        self.assertEqual(result['events_seen'], 3)
        self.assertEqual(result['events_captured'], 2)
        self.assertEqual(result['events_failed'], 1)
        self.assertEqual(client.calls[0]['reprt_code'], '11011')
        self.assertEqual(client.calls[1]['reprt_code'], '11012')
        self.assertEqual(client.calls[2]['reprt_code'], '11014')
        self.assertEqual(repository.successes, [1, 3])
        self.assertEqual(repository.failures[0][0], 2)
        self.assertEqual(repository.snapshots[0]['snapshot_payload']['business_year'], 2025)
        self.assertEqual(len(repository.snapshots), 2)
        self.assertEqual(repository.snapshots[1]['snapshot_payload']['report_code'], '11014')
        self.mock_capture_dart_signals.assert_awaited_once_with(
            rcept_no='20260407000003',
        )
        self.assertEqual(repository.completed_runs[-1][1]['status'], 'completed')
        self.assertGreaterEqual(len(repository.progress_updates), 4)
        self.assertTrue(
            any(
                call.kwargs.get('operation') == 'capture_fundamentals_event'
                and call.kwargs.get('status') == 'failed'
                and call.kwargs.get('event_id') == 2
                for call in log_fetch_event.call_args_list
            )
        )

    async def test_capture_recent_fundamentals_filters_to_requested_stock_codes(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(client=client, repository=repository)

        result = await service.capture_recent_fundamentals(limit=None, stock_codes=['005930'], fs_div='CFS')

        self.assertEqual(result['events_seen'], 1)
        self.assertEqual(repository.successes, [1])

    async def test_capture_recent_fundamentals_times_out_event_and_marks_failure(self):
        class SlowCaptureClient(FakeCaptureClient):
            async def fetch_company_overview(self, corp_code):
                await asyncio.sleep(0.02)
                return await super().fetch_company_overview(corp_code)

        client = SlowCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(client=client, repository=repository)
        service.event_timeout_seconds = 0.001

        result = await service.capture_recent_fundamentals(limit=1, force=False, trigger='test', fs_div='CFS')

        self.assertEqual(result['events_seen'], 1)
        self.assertEqual(result['events_failed'], 1)
        self.assertEqual(repository.failures[0][0], 1)
        self.assertIn('timed out', repository.failures[0][1])

    async def test_capture_recent_fundamentals_marks_dart_no_data_terminal(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(client=client, repository=repository)

        result = await service.capture_recent_fundamentals(
            limit=1,
            event_ids=[8],
            force=False,
            trigger='test',
            fs_div='CFS',
        )

        self.assertEqual(result['events_seen'], 1)
        self.assertEqual(result['events_captured'], 0)
        self.assertEqual(result['events_failed'], 0)
        self.assertEqual(result['events_skipped'], 1)
        self.assertEqual(result['results'][0]['status'], 'no_data')
        self.assertEqual(repository.terminals[0][0], 8)
        self.assertEqual(repository.terminals[0][1], 'no_data')
        self.assertEqual(repository.failures, [])
        self.assertEqual(repository.snapshots, [])
        self.assertEqual(result['operator_buckets']['terminal_no_data']['event_count'], 1)

    async def test_capture_recent_fundamentals_falls_back_to_ofs_when_cfs_has_no_data(self):
        class StandaloneOnlyClient(FakeCaptureClient):
            async def fetch_financial_statements(self, **kwargs):
                if kwargs['corp_code'] == 'no-data' and kwargs['fs_div'] == 'CFS':
                    self.calls.append(kwargs)
                    return {'status': '013', 'message': 'No consolidated statements', 'list': []}
                if kwargs['corp_code'] == 'no-data' and kwargs['fs_div'] == 'OFS':
                    original_corp_code = kwargs['corp_code']
                    kwargs = {**kwargs, 'corp_code': 'standalone-data'}
                    payload = await super().fetch_financial_statements(**kwargs)
                    self.calls[-1] = {**self.calls[-1], 'corp_code': original_corp_code}
                    return payload
                return await super().fetch_financial_statements(**kwargs)

        client = StandaloneOnlyClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(client=client, repository=repository)

        result = await service.capture_recent_fundamentals(
            limit=1,
            event_ids=[8],
            force=False,
            trigger='test',
            fs_div='CFS',
        )

        self.assertEqual(result['events_captured'], 1)
        self.assertEqual(result['events_skipped'], 0)
        self.assertEqual(result['results'][0]['status'], 'captured')
        self.assertEqual(result['results'][0]['fs_div'], 'OFS')
        self.assertEqual([call['fs_div'] for call in client.calls], ['CFS', 'OFS'])
        snapshot = repository.snapshots[0]['snapshot_payload']
        self.assertEqual(snapshot['fs_div'], 'OFS')
        self.assertEqual(snapshot['statement_type'], 'separate')
        self.assertEqual(snapshot['source_status'], '000')
        self.assertEqual(snapshot['request_payload']['requested_fs_div'], 'CFS')
        self.assertEqual(snapshot['request_payload']['fallback_from_status'], '013')
        self.assertEqual(
            snapshot['request_payload']['fallback_from_message'],
            'No consolidated statements',
        )
        self.assertEqual(repository.successes, [8])
        self.assertEqual(repository.terminals, [])

    async def test_capture_recent_fundamentals_keeps_failed_ofs_fallback_retryable(self):
        class FailingStandaloneFallbackClient(FakeCaptureClient):
            async def fetch_financial_statements(self, **kwargs):
                self.calls.append(kwargs)
                if kwargs['fs_div'] == 'CFS':
                    return {'status': '013', 'message': 'No consolidated statements', 'list': []}
                return {'status': '020', 'message': 'Temporary request limit', 'list': []}

        client = FailingStandaloneFallbackClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(client=client, repository=repository)

        result = await service.capture_recent_fundamentals(
            limit=1,
            event_ids=[8],
            force=False,
            trigger='test',
            fs_div='CFS',
        )

        self.assertEqual(result['events_captured'], 0)
        self.assertEqual(result['events_failed'], 1)
        self.assertEqual(result['events_skipped'], 0)
        self.assertEqual(result['results'][0]['status'], 'failed')
        self.assertEqual([call['fs_div'] for call in client.calls], ['CFS', 'OFS'])
        self.assertEqual(repository.snapshots, [])
        self.assertEqual(repository.successes, [])
        self.assertEqual(repository.terminals, [])
        self.assertEqual(repository.failures[0][0], 8)
        self.assertIn('CFS [013]', repository.failures[0][1])
        self.assertIn('OFS [020]', repository.failures[0][1])

    async def test_capture_recent_fundamentals_does_not_mark_empty_success_payload_captured(self):
        class EmptySuccessClient(FakeCaptureClient):
            async def fetch_financial_statements(self, **kwargs):
                self.calls.append(kwargs)
                return {'status': '000', 'message': 'OK', 'list': []}

        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(client=EmptySuccessClient(), repository=repository)

        result = await service.capture_recent_fundamentals(
            limit=1,
            event_ids=[8],
            force=False,
            trigger='test',
            fs_div='CFS',
        )

        self.assertEqual(result['events_captured'], 0)
        self.assertEqual(result['events_failed'], 1)
        self.assertEqual(result['results'][0]['status'], 'failed')
        self.assertIn('no statement rows', result['results'][0]['error'])
        self.assertEqual(repository.snapshots, [])
        self.assertEqual(repository.successes, [])
        self.assertEqual(repository.failures[0][0], 8)

    async def test_capture_recent_fundamentals_marks_mapping_failure_terminal(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(client=client, repository=repository)

        result = await service.capture_recent_fundamentals(
            limit=1,
            event_ids=[9],
            force=False,
            trigger='test',
            fs_div='CFS',
        )

        self.assertEqual(result['events_seen'], 1)
        self.assertEqual(result['events_failed'], 0)
        self.assertEqual(result['events_skipped'], 1)
        self.assertEqual(result['results'][0]['status'], 'mapping_failed')
        self.assertEqual(repository.terminals[0][1], 'mapping_failed')
        self.assertIn('Unable to infer report period', repository.terminals[0][2])
        self.assertEqual(repository.failures, [])
        self.assertEqual(result['operator_buckets']['terminal_mapping_failed']['event_count'], 1)

    async def test_capture_recent_fundamentals_fails_stale_runs_before_new_run(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(client=client, repository=repository)

        with patch.object(repository, 'fail_stale_capture_runs', return_value=[41]) as fail_stale_capture_runs:
            await service.capture_recent_fundamentals(limit=1, force=False, trigger='test', fs_div='CFS')

        fail_stale_capture_runs.assert_called_once()

    async def test_inspect_capture_status_returns_repository_views(self):
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=FakeCaptureRepository(),
        )

        result = service.inspect_capture_status(runs_limit=5, events_limit=1, capture_status='failed')

        self.assertEqual(result['latest_run']['id'], 88)
        self.assertEqual(result['summary']['financial_event_capture_status_counts']['captured'], 1)
        self.assertEqual(result['events'][0]['fundamentals_capture_status'], 'failed')

    async def test_inspect_daily_refresh_status_fails_stale_runs_before_returning(self):
        repository = FakeCaptureRepository()
        repository.latest_refresh_run = {
            'id': 144,
            'market_date': '2026-04-14',
            'status': 'failed',
            'error_message': 'Marked failed after exceeding the stale running threshold before a daily refresh status read',
        }
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=repository,
        )

        with patch.object(repository, 'fail_stale_refresh_runs', return_value=[144]) as fail_stale_refresh_runs:
            result = service.inspect_daily_refresh_status(
                market_date=date(2026, 4, 14),
                fs_div='CFS',
            )

        fail_stale_refresh_runs.assert_called_once()
        self.assertEqual(result['latest_run']['id'], 144)
        self.assertEqual(result['latest_run']['status'], 'failed')

    async def test_inspect_daily_refresh_status_returns_zero_updated_entries_summary(self):
        repository = FakeCaptureRepository()
        repository.latest_refresh_run = {
            'id': 145,
            'market_date': '2026-04-14',
            'status': 'completed',
            'symbols_refreshed': 0,
            'run_metadata': {'updated_entries': []},
        }
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=repository,
        )

        result = service.inspect_daily_refresh_status(
            market_date=date(2026, 4, 14),
            fs_div='CFS',
        )

        self.assertEqual(result['latest_run']['updated_entries'], [])
        self.assertEqual(result['latest_run']['updated_entries_preview'], [])
        self.assertEqual(
            result['latest_run']['updated_entries_summary'],
            {
                'updated_count': 0,
                'visible_preview_limit': 3,
                'visible_preview_count': 0,
                'has_more': False,
                'remaining_count': 0,
                'full_list_available': True,
            },
        )

    async def test_inspect_daily_refresh_status_normalizes_single_updated_entry(self):
        repository = FakeCaptureRepository()
        repository.latest_refresh_run = {
            'id': 146,
            'market_date': '2026-04-14',
            'status': 'completed',
            'symbols_refreshed': 1,
            'run_metadata': {
                'updated_entries': [
                    {
                        'symbol': '005930',
                        'company_name': '삼성전자',
                        'update_category': 'financial_statement_refresh',
                    }
                ]
            },
        }
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=repository,
        )

        result = service.inspect_daily_refresh_status(
            market_date=date(2026, 4, 14),
            fs_div='CFS',
        )

        self.assertEqual(
            result['latest_run']['updated_entries'][0],
            {
                'symbol': '005930',
                'stock_code': '005930',
                'company_name': '삼성전자',
                'update_category': 'financial_statement_refresh',
                'category_label': 'Financial statement refresh',
            },
        )
        self.assertEqual(result['latest_run']['updated_entries_preview'], result['latest_run']['updated_entries'])
        self.assertEqual(result['latest_run']['updated_entries_summary']['updated_count'], 1)

    async def test_inspect_daily_refresh_status_exposes_preview_and_more_counts_for_many_entries(self):
        repository = FakeCaptureRepository()
        repository.latest_refresh_run = {
            'id': 147,
            'market_date': '2026-04-14',
            'status': 'completed',
            'symbols_refreshed': 4,
            'run_metadata': {
                'updated_entries': [
                    {
                        'symbol': '005930',
                        'company_name': '삼성전자',
                        'update_category': 'financial_statement_refresh',
                    },
                    {
                        'symbol': '000660',
                        'company_name': 'SK하이닉스',
                        'update_category': 'financial_statement_refresh',
                    },
                    {
                        'symbol': '035420',
                        'company_name': 'NAVER',
                        'update_category': 'initial_fundamentals_capture',
                    },
                    {
                        'symbol': '051910',
                        'company_name': 'LG화학',
                        'update_category': 'financial_statement_refresh',
                    },
                ]
            },
        }
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=repository,
        )

        result = service.inspect_daily_refresh_status(
            market_date=date(2026, 4, 14),
            fs_div='CFS',
        )

        self.assertEqual(len(result['latest_run']['updated_entries']), 4)
        self.assertEqual(len(result['latest_run']['updated_entries_preview']), 3)
        self.assertTrue(result['latest_run']['updated_entries_summary']['has_more'])
        self.assertEqual(result['latest_run']['updated_entries_summary']['remaining_count'], 1)
        self.assertTrue(result['latest_run']['updated_entries_summary']['full_list_available'])

    async def test_backfill_current_universe_uses_2015_floor_and_selected_active_symbols(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [
                {'symbol': '005930'},
                {'symbol': '000001'},
                {'symbol': '000002'},
            ],
            daily_price_bar_bounds_loader=lambda: (date(2007, 6, 5), date(2026, 4, 7)),
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'completed',
                'financial_report_events': 4,
            },
        ):
            result = await service.backfill_current_universe_fundamentals(
                symbol_limit=2,
                symbol_offset=1,
                capture_limit=10,
                fs_div='CFS',
            )

        self.assertEqual(result['effective_start_date'], '2015-01-01')
        self.assertEqual(result['symbols_selected'], 2)
        self.assertEqual(result['symbols_resolved'], 2)
        self.assertEqual(result['sync']['run_id'], 77)

    async def test_backfill_current_universe_marks_capture_limit_debt_partial_until_scope_is_drained(self):
        class DrainingCaptureRepository(FakeCaptureRepository):
            def __init__(self):
                super().__init__()
                self.events = [
                    {
                        'id': 101,
                        'rcept_no': '20260406000001',
                        'corp_code': 'corp-005930',
                        'corp_name': '삼성전자',
                        'stock_code': '005930',
                        'report_nm': '사업보고서 (2025.12)',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'pending',
                        'receipt_date': '2026-04-06',
                    },
                    {
                        'id': 102,
                        'rcept_no': '20260407000001',
                        'corp_code': 'corp-005930',
                        'corp_name': '삼성전자',
                        'stock_code': '005930',
                        'report_nm': '사업보고서 (2025.12)',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'pending',
                        'receipt_date': '2026-04-07',
                    },
                    {
                        'id': 103,
                        'rcept_no': '20260405000001',
                        'corp_code': 'corp-005930',
                        'corp_name': '삼성전자',
                        'stock_code': '005930',
                        'report_nm': '사업보고서 (2025.12)',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'no_data',
                        'receipt_date': '2026-04-05',
                    },
                ]

            def list_capture_candidates(
                self,
                *,
                limit=20,
                force=False,
                stock_codes=None,
                receipt_date_from=None,
                receipt_date_to=None,
                event_ids=None,
                oldest_first=False,
            ):
                self.candidate_call_kwargs.append(
                    {
                        'limit': limit,
                        'force': force,
                        'stock_codes': stock_codes,
                        'receipt_date_from': receipt_date_from,
                        'receipt_date_to': receipt_date_to,
                        'event_ids': event_ids,
                        'oldest_first': oldest_first,
                    }
                )
                events = list(self.events)
                if not force:
                    events = [
                        event
                        for event in events
                        if event['fundamentals_capture_status'] in {'pending', 'failed'}
                    ]
                if stock_codes:
                    events = [event for event in events if event['stock_code'] in stock_codes]
                if receipt_date_from is not None:
                    events = [
                        event
                        for event in events
                        if date.fromisoformat(event['receipt_date']) >= receipt_date_from
                    ]
                if receipt_date_to is not None:
                    events = [
                        event
                        for event in events
                        if date.fromisoformat(event['receipt_date']) <= receipt_date_to
                    ]
                if event_ids is not None:
                    events = [event for event in events if event['id'] in event_ids]
                return events if limit is None else events[:limit]

            def mark_event_capture_success(self, event_id):
                super().mark_event_capture_success(event_id)
                next(event for event in self.events if event['id'] == event_id)[
                    'fundamentals_capture_status'
                ] = 'captured'

        repository = DrainingCaptureRepository()
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}],
            daily_price_bar_bounds_loader=lambda: (date(2015, 1, 1), date(2026, 4, 7)),
        )
        sync_result = {
            'run_id': 77,
            'status': 'completed',
            'corporations_processed': 1,
            'corporations_failed': 0,
            'failed_stock_codes': [],
            'unattempted_stock_codes': [],
            'financial_report_events': 2,
        }

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value=sync_result,
        ):
            first = await service.backfill_current_universe_fundamentals(
                symbol_limit=1,
                capture_limit=1,
                fs_div='CFS',
            )
            drained = await service.backfill_current_universe_fundamentals(
                symbol_limit=1,
                capture_limit=1,
                fs_div='CFS',
            )

        self.assertEqual(first['status'], 'partial')
        self.assertEqual(first['remaining_retryable_events'], 1)
        self.assertEqual(first['capture']['remaining_retryable_events'], 1)
        self.assertEqual(first['operator_buckets']['remaining_retryable_events']['count'], 1)
        self.assertEqual(drained['status'], 'completed')
        self.assertEqual(drained['remaining_retryable_events'], 0)
        self.assertEqual(drained['capture']['remaining_retryable_events'], 0)
        self.assertEqual(repository.successes, [101, 102])
        self.assertEqual(
            next(event for event in repository.events if event['id'] == 103)[
                'fundamentals_capture_status'
            ],
            'no_data',
        )

    async def test_backfill_current_universe_force_drains_scope_without_repeating_limited_candidate(self):
        class ForceCaptureRepository(FakeCaptureRepository):
            def list_capture_candidates(
                self,
                *,
                limit=20,
                force=False,
                stock_codes=None,
                receipt_date_from=None,
                receipt_date_to=None,
                event_ids=None,
                oldest_first=False,
            ):
                self.candidate_call_kwargs.append(
                    {
                        'limit': limit,
                        'force': force,
                        'stock_codes': stock_codes,
                        'receipt_date_from': receipt_date_from,
                        'receipt_date_to': receipt_date_to,
                        'event_ids': event_ids,
                        'oldest_first': oldest_first,
                    }
                )
                if not force:
                    return []
                events = [
                    {
                        'id': event_id,
                        'rcept_no': f'2026040700000{event_id}',
                        'corp_code': 'corp-005930',
                        'corp_name': '삼성전자',
                        'stock_code': '005930',
                        'report_nm': '사업보고서 (2025.12)',
                        'filing_category': 'annual_report',
                        'fundamentals_capture_status': 'captured',
                        'receipt_date': '2026-04-07',
                    }
                    for event_id in (1, 2)
                ]
                if stock_codes:
                    events = [event for event in events if event['stock_code'] in stock_codes]
                if event_ids is not None:
                    events = [event for event in events if event['id'] in event_ids]
                return events if limit is None else events[:limit]

        repository = ForceCaptureRepository()
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}],
            daily_price_bar_bounds_loader=lambda: (date(2015, 1, 1), date(2026, 4, 7)),
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'completed',
                'corporations_processed': 1,
                'corporations_failed': 0,
                'failed_stock_codes': [],
                'unattempted_stock_codes': [],
                'financial_report_events': 2,
            },
        ):
            result = await service.backfill_current_universe_fundamentals(
                symbol_limit=1,
                capture_limit=1,
                force=True,
                fs_div='CFS',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['capture']['events_seen'], 2)
        self.assertEqual(result['remaining_retryable_events'], 0)
        self.assertEqual(repository.successes, [1, 2])
        self.assertEqual([call['limit'] for call in repository.candidate_call_kwargs], [None, None])
        self.assertEqual([call['force'] for call in repository.candidate_call_kwargs], [True, False])

    async def test_backfill_current_universe_reports_partial_and_failed_stock_codes_from_child_outcomes(self):
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=FakeCaptureRepository(),
            active_symbol_loader=lambda: [
                {'symbol': '005930'},
                {'symbol': '000660'},
                {'symbol': '035420'},
            ],
            daily_price_bar_bounds_loader=lambda: (date(2015, 1, 1), date(2026, 4, 7)),
        )
        capture_result = {
            'run_id': 88,
            'status': 'completed',
            'events_seen': 2,
            'events_captured': 1,
            'events_failed': 1,
            'events_skipped': 0,
            'results': [
                {'stock_code': '005930', 'status': 'captured'},
                {'stock_code': '000660', 'status': 'failed', 'error': 'temporary failure'},
            ],
        }

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'partial',
                'corporations_processed': 2,
                'corporations_failed': 1,
                'corporations_unattempted': 0,
                'failed_stock_codes': ['035420'],
                'unattempted_stock_codes': [],
                'financial_report_events': 2,
            },
        ), patch.object(
            service,
            'capture_recent_fundamentals',
            return_value=capture_result,
        ):
            result = await service.backfill_current_universe_fundamentals(
                symbol_limit=3,
                capture_limit=10,
                fs_div='CFS',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['failed_stock_codes'], ['000660', '035420'])

    async def test_backfill_current_universe_reports_failed_when_sync_circuit_breaker_leaves_symbols_unattempted(self):
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=FakeCaptureRepository(),
            active_symbol_loader=lambda: [
                {'symbol': '005930'},
                {'symbol': '000660'},
            ],
            daily_price_bar_bounds_loader=lambda: (date(2015, 1, 1), date(2026, 4, 7)),
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'failed',
                'corporations_processed': 0,
                'corporations_failed': 1,
                'corporations_unattempted': 1,
                'failed_stock_codes': ['005930'],
                'unattempted_stock_codes': ['000660'],
                'financial_report_events': 0,
            },
        ), patch.object(
            service,
            'capture_recent_fundamentals',
            return_value={
                'run_id': 88,
                'status': 'completed',
                'events_seen': 0,
                'events_captured': 0,
                'events_failed': 0,
                'events_skipped': 0,
                'results': [],
            },
        ):
            result = await service.backfill_current_universe_fundamentals(
                symbol_limit=2,
                capture_limit=10,
                fs_div='CFS',
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['failed_stock_codes'], ['000660', '005930'])

    async def test_backfill_current_universe_rejects_reversed_effective_date_window(self):
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=FakeCaptureRepository(),
            active_symbol_loader=lambda: [{'symbol': '005930'}],
            daily_price_bar_bounds_loader=lambda: (date(2007, 6, 5), date(2014, 12, 31)),
        )

        with self.assertRaisesRegex(ValueError, 'effective_start_date'):
            await service.backfill_current_universe_fundamentals(symbol_limit=1, capture_limit=10, fs_div='CFS')

    async def test_daily_incremental_refresh_syncs_only_missing_or_newer_symbol_windows(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '005930': {
                'stock_code': '005930',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': 100},
            },
            '000660': {
                'stock_code': '000660',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-14',
                'latest_snapshot': {'id': 101},
            },
            '123456': {
                'stock_code': '123456',
                'has_fundamentals': False,
                'latest_fundamentals_receipt_date': None,
                'latest_snapshot': None,
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [
                {'symbol': '005930'},
                {'symbol': '000660'},
                {'symbol': '123456'},
            ],
        )

        sync_calls = []

        async def fake_sync(**kwargs):
            sync_calls.append(kwargs)
            return {
                'run_id': len(sync_calls),
                'status': 'completed',
                'financial_report_events': len(kwargs['corporations']),
            }

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            side_effect=fake_sync,
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_initial_backfill_enabled',
            True,
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_incremental_lookback_days',
            5000,
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['symbols_selected'], 3)
        self.assertEqual(result['symbols_missing'], 1)
        self.assertEqual(result['symbols_stale'], 1)
        self.assertEqual(result['symbols_fresh'], 1)
        self.assertEqual(result['events_detected'], 2)
        self.assertEqual(result['events_captured'], 2)
        self.assertEqual(result['piotroski_filter']['symbols_evaluated'], 3)
        self.assertIsNone(result['piotroski_filter_error'])
        self.assertEqual(repository.successes, [4, 5])
        self.assertEqual([call['start_date'] for call in sync_calls], [date(2015, 1, 1), date(2026, 4, 2)])
        self.assertEqual(repository.candidate_call_kwargs[-1]['event_ids'], [4, 5])
        self.assertEqual(repository.completed_refresh_runs[-1][1]['status'], 'completed')
        self.assertEqual(
            repository.completed_refresh_runs[-1][1]['run_metadata']['updated_entries'],
            [
                {
                    'symbol': '005930',
                    'stock_code': '005930',
                    'company_name': '삼성전자',
                    'update_category': 'financial_statement_refresh',
                    'category_label': 'Financial statement refresh',
                },
                {
                    'symbol': '123456',
                    'stock_code': '123456',
                    'company_name': 'Missing Corp',
                    'update_category': 'initial_fundamentals_capture',
                    'category_label': 'Initial fundamentals capture',
                },
            ],
        )
        self.mock_piotroski_filter.assert_called_once_with(market_date=date(2026, 4, 14))

    async def test_daily_incremental_refresh_boundedly_probes_missing_symbols_in_recent_window(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [
                {'symbol': '111111'},
                {'symbol': '222222'},
                {'symbol': '333333'},
            ],
        )
        sync_calls = []

        async def fake_sync(**kwargs):
            sync_calls.append(kwargs)
            return {
                'run_id': len(sync_calls),
                'status': 'completed',
                'financial_report_events': 0,
                'corporations_processed': len(kwargs['corporations']),
                'corporations_failed': 0,
                'corporation_errors': [],
            }

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            side_effect=fake_sync,
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_initial_backfill_enabled',
            False,
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_missing_symbol_probe_limit',
            1,
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_incremental_lookback_days',
            45,
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertEqual(sum(len(call['corporations']) for call in sync_calls), 1)
        self.assertEqual(sync_calls[0]['start_date'], date(2026, 2, 28))
        metadata = repository.completed_refresh_runs[-1][1]['run_metadata']
        self.assertEqual(metadata['bounded_missing_symbol_probe_limit'], 1)
        self.assertEqual(metadata['bounded_missing_symbol_probe_count'], 1)
        deferred = [
            item
            for item in metadata['symbol_results_sample']
            if item['state'] == 'missing_recent_probe_deferred'
        ]
        self.assertEqual(len(deferred), 2)
        self.assertEqual(result['status'], 'completed')

    async def test_daily_incremental_refresh_retries_failed_event_when_baseline_is_missing(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '222222': {
                'stock_code': '222222',
                'has_fundamentals': False,
                'latest_fundamentals_receipt_date': None,
                'latest_snapshot': None,
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '222222'}],
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'completed',
                'financial_report_events': 1,
            },
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_initial_backfill_enabled',
            True,
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_incremental_lookback_days',
            5000,
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['symbols_missing'], 1)
        self.assertEqual(result['events_detected'], 1)
        self.assertEqual(result['events_captured'], 1)
        self.assertEqual(repository.candidate_call_kwargs[-1]['event_ids'], [7])
        self.assertEqual(repository.successes, [7])

    async def test_daily_incremental_refresh_propagates_company_sync_failures_as_retryable_partial(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '005930': {
                'stock_code': '005930',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': 100},
            },
            '000660': {
                'stock_code': '000660',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': 101},
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}, {'symbol': '000660'}],
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'partial',
                'financial_report_events': 1,
                'corporations_processed': 1,
                'corporations_failed': 1,
                'corporation_errors': [],
                'failed_stock_codes': ['000660'],
            },
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['symbols_failed'], 1)
        self.assertIn('sync_failures', result['partial_reasons'])
        self.assertEqual(result['samples']['failed_symbols'], ['000660'])
        self.assertEqual(repository.completed_refresh_runs[-1][1]['status'], 'partial')
        symbol_results = repository.completed_refresh_runs[-1][1]['run_metadata']['symbol_results_sample']
        failed_symbol = next(item for item in symbol_results if item['stock_code'] == '000660')
        self.assertEqual(failed_symbol['state'], 'sync_failed')
        self.assertTrue(repository.candidate_call_kwargs)
        self.assertNotIn('000660', repository.candidate_call_kwargs[0]['stock_codes'])

    async def test_daily_incremental_refresh_marks_failed_when_all_sync_groups_fail(self):
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            stock_code: {
                'stock_code': stock_code,
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': index},
            }
            for index, stock_code in enumerate(('005930', '000660'), start=1)
        }
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}, {'symbol': '000660'}],
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'failed',
                'financial_report_events': 0,
                'corporations_processed': 0,
                'corporations_failed': 2,
                'corporations_unattempted': 0,
                'failed_stock_codes': ['005930', '000660'],
                'unattempted_stock_codes': [],
                'corporation_errors': [],
            },
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['symbols_failed'], 2)
        self.assertEqual(result['events_captured'], 0)
        self.assertEqual(repository.candidate_call_kwargs, [])
        self.assertEqual(repository.completed_refresh_runs[-1][1]['status'], 'failed')
        states = {
            item['stock_code']: item['state']
            for item in repository.completed_refresh_runs[-1][1]['run_metadata']['symbol_results_sample']
        }
        self.assertEqual(states, {'000660': 'sync_failed', '005930': 'sync_failed'})

    async def test_daily_incremental_refresh_marks_partial_when_piotroski_followup_fails(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '005930': {
                'stock_code': '005930',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': 100},
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}],
        )
        self.mock_piotroski_filter.side_effect = RuntimeError('piotroski table unavailable')

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'completed',
                'financial_report_events': 1,
            },
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertIn('piotroski_filter_followup_failed', result['partial_reasons'])
        self.assertIsNone(result['piotroski_filter'])
        self.assertIn('piotroski table unavailable', result['piotroski_filter_error'])

    async def test_daily_incremental_refresh_skips_feedback_followup_when_piotroski_followup_is_incomplete(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '005930': {
                'stock_code': '005930',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': 100},
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}],
        )
        self.mock_piotroski_filter.return_value = {
            'market_date': '2026-04-14',
            'source': 'strategy_piotroski_v1',
            'symbols_evaluated': 1,
            'symbols_passed': 1,
            'symbols_not_passed': 0,
            'results_persisted': 0,
            'results_failed': 1,
            'failed_symbols': ['005930'],
        }

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'completed',
                'financial_report_events': 1,
            },
        ):
            with patch.object(
                __import__('app.dart.fundamentals_capture_service', fromlist=['settings']).settings,
                'openclaw_feedback_trigger_after_fundamentals_refresh',
                True,
            ):
                with patch(
                    'app.dart.fundamentals_capture_service.agent_feedback_retry_service.run_retry_cycles',
                ) as run_retry_cycles:
                    result = await service.run_daily_incremental_refresh(
                        market_date=date(2026, 4, 14),
                        capture_limit=10,
                        fs_div='CFS',
                        trigger='test_daily',
                    )

        run_retry_cycles.assert_not_called()
        self.assertEqual(result['feedback_followup']['reason'], 'piotroski_filter_followup_incomplete')

    async def test_daily_incremental_refresh_triggers_feedback_followup_after_clean_piotroski_followup(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '005930': {
                'stock_code': '005930',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': 100},
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}],
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'completed',
                'financial_report_events': 1,
            },
        ):
            with patch.object(
                __import__('app.dart.fundamentals_capture_service', fromlist=['settings']).settings,
                'openclaw_feedback_trigger_after_fundamentals_refresh',
                True,
            ):
                with (
                    patch(
                        'app.dart.fundamentals_capture_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                        side_effect=lambda **_: _feedback_prerequisites(),
                    ),
                    patch(
                        'app.dart.fundamentals_capture_service.agent_feedback_retry_service.run_retry_cycles',
                        return_value={'completed': True, 'final_pending': 0},
                    ) as run_retry_cycles,
                ):
                    result = await service.run_daily_incremental_refresh(
                        market_date=date(2026, 4, 14),
                        capture_limit=10,
                        fs_div='CFS',
                        trigger='test_daily',
                    )

        run_retry_cycles.assert_called_once()
        self.assertEqual(run_retry_cycles.call_args.kwargs['config'].market_date, date(2026, 4, 14))
        self.assertEqual(
            run_retry_cycles.call_args.kwargs['config'].roles,
            (
                'eod-summarizer',
                'technical-summarizer',
                'fundamental-summarizer',
                'short-term-decider',
                'signal-decider',
            ),
        )
        self.assertEqual(result['feedback_followup'], {'completed': True, 'final_pending': 0})

    async def test_daily_incremental_refresh_gates_feedback_followup_until_factor_prerequisites_finish(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '005930': {
                'stock_code': '005930',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': 100},
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}],
        )

        with (
            patch(
                'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
                return_value={
                    'run_id': 77,
                    'status': 'completed',
                    'financial_report_events': 1,
                },
            ),
            patch.object(
                __import__('app.dart.fundamentals_capture_service', fromlist=['settings']).settings,
                'openclaw_feedback_trigger_after_fundamentals_refresh',
                True,
            ),
            patch(
                'app.dart.fundamentals_capture_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                side_effect=lambda **_: _feedback_prerequisites(
                    status='waiting',
                    missing_stage_ids=['quality_factors', 'composite_factors'],
                ),
            ),
            patch(
                'app.dart.fundamentals_capture_service.agent_feedback_retry_service.run_retry_cycles',
            ) as run_retry_cycles,
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        run_retry_cycles.assert_not_called()
        self.assertEqual(result['feedback_followup']['reason'], 'feedback_prerequisites_not_ready')
        self.assertEqual(result['feedback_followup']['status'], 'waiting')

    async def test_daily_incremental_refresh_can_disable_hidden_followups_for_stage_orchestration(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '005930': {
                'stock_code': '005930',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': 100},
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}],
        )

        with (
            patch(
                'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
                return_value={
                    'run_id': 77,
                    'status': 'completed',
                    'financial_report_events': 1,
                },
            ),
            patch(
                'app.dart.fundamentals_capture_service.daily_piotroski_filter_service.rebuild_for_market_date',
            ) as piotroski_followup,
            patch(
                'app.dart.fundamentals_capture_service.agent_feedback_retry_service.run_retry_cycles',
            ) as feedback_followup,
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
                run_piotroski_followup=False,
                run_feedback_followup=False,
            )

        piotroski_followup.assert_not_called()
        feedback_followup.assert_not_called()
        self.assertEqual(result['piotroski_filter']['reason'], 'followup_disabled')
        self.assertEqual(result['feedback_followup']['reason'], 'followup_disabled')

    async def test_daily_incremental_refresh_filters_candidates_by_symbol_resolved_baseline(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '123456': {
                'stock_code': '123456',
                'has_fundamentals': False,
                'latest_fundamentals_receipt_date': None,
                'latest_resolved_fundamentals_receipt_date': None,
                'latest_snapshot': None,
            },
            '555555': {
                'stock_code': '555555',
                'has_fundamentals': False,
                'latest_fundamentals_receipt_date': None,
                'latest_resolved_fundamentals_receipt_date': '2026-04-10',
                'latest_snapshot': None,
                'latest_terminal_fundamentals_event': {
                    'fundamentals_capture_status': 'no_data',
                    'receipt_date': '2026-04-10',
                },
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [
                {'symbol': '123456'},
                {'symbol': '555555'},
            ],
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'completed',
                'financial_report_events': 2,
            },
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_initial_backfill_enabled',
            True,
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_incremental_lookback_days',
            5000,
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['events_detected'], 2)
        self.assertEqual(repository.candidate_call_kwargs[-1]['event_ids'], [5, 11])
        self.assertEqual(repository.successes, [5, 11])
        self.assertNotIn(10, repository.successes)

    async def test_daily_incremental_refresh_heartbeats_during_sync_and_capture_work(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '005930': {
                'stock_code': '005930',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': 100},
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}],
        )

        sync_active = False
        capture_active = False
        sync_heartbeat_seen = asyncio.Event()
        capture_heartbeat_seen = asyncio.Event()
        original_update_refresh_run_progress = repository.update_refresh_run_progress

        def recording_update_refresh_run_progress(run_id, **kwargs):
            metadata = kwargs.get('run_metadata') or {}
            if sync_active and metadata.get('stage') == 'syncing_disclosures' and metadata.get('last_progress_at'):
                sync_heartbeat_seen.set()
            if capture_active and metadata.get('stage') == 'capturing_fundamentals' and metadata.get('last_progress_at'):
                capture_heartbeat_seen.set()
            original_update_refresh_run_progress(run_id, **kwargs)

        repository.update_refresh_run_progress = recording_update_refresh_run_progress

        async def fake_sync(**kwargs):
            nonlocal sync_active
            sync_active = True
            try:
                await asyncio.wait_for(sync_heartbeat_seen.wait(), timeout=1)
            finally:
                sync_active = False
            return {
                'run_id': 77,
                'status': 'completed',
                'financial_report_events': len(kwargs['corporations']),
            }

        async def fake_capture_recent_fundamentals(**kwargs):
            nonlocal capture_active
            capture_active = True
            try:
                await asyncio.wait_for(capture_heartbeat_seen.wait(), timeout=1)
            finally:
                capture_active = False
            return {
                'run_id': 88,
                'status': 'completed',
                'fs_div': kwargs['fs_div'],
                'events_seen': 1,
                'events_captured': 1,
                'events_failed': 0,
                'events_skipped': 0,
                'results': [
                    {
                        'event_id': 4,
                        'stock_code': '005930',
                        'status': 'captured',
                    }
                ],
            }

        service.capture_recent_fundamentals = fake_capture_recent_fundamentals

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            side_effect=fake_sync,
        ), patch('app.dart.fundamentals_capture_service.DART_FUNDAMENTALS_REFRESH_HEARTBEAT_SECONDS', 0.001):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertTrue(result['triggered'])
        self.assertTrue(sync_heartbeat_seen.is_set())
        self.assertTrue(capture_heartbeat_seen.is_set())
        progress_stages = [
            (update[1].get('run_metadata') or {}).get('stage')
            for update in repository.refresh_progress_updates
        ]
        self.assertIn('syncing_disclosures', progress_stages)
        self.assertIn('capturing_fundamentals', progress_stages)

    async def test_daily_incremental_refresh_skips_latest_terminal_no_data_baseline(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '333333': {
                'stock_code': '333333',
                'has_fundamentals': False,
                'latest_fundamentals_receipt_date': None,
                'latest_resolved_fundamentals_receipt_date': '2026-04-14',
                'latest_snapshot': None,
                'latest_terminal_fundamentals_event': {
                    'fundamentals_capture_status': 'no_data',
                    'receipt_date': '2026-04-14',
                },
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '333333'}],
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            side_effect=AssertionError('terminal no-data symbols should not be synced again'),
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertEqual(client.lookup_calls, [])
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['sync']['run_count'], 0)
        self.assertEqual(result['capture']['reason'], 'no_stale_or_missing_events')
        self.assertEqual(result['operator_buckets']['terminal_no_data']['symbol_count'], 1)
        symbol_sample = repository.completed_refresh_runs[-1][1]['run_metadata']['symbol_results_sample'][0]
        self.assertEqual(symbol_sample['state'], 'terminal_no_data')

    async def test_daily_incremental_refresh_marks_retryable_lookup_failure_as_failed(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '999999'}],
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            side_effect=AssertionError('lookup failures should not reach sync'),
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_initial_backfill_enabled',
            True,
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_incremental_lookback_days',
            5000,
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['symbols_failed'], 1)
        self.assertIn('symbol_failures', result['partial_reasons'])
        self.assertEqual(result['operator_buckets']['lookup_or_mapping_errors']['count'], 1)
        self.assertEqual(result['samples']['lookup_errors'][0]['stock_code'], '999999')
        self.assertIn('Unknown stock_code', result['samples']['lookup_errors'][0]['error'])
        completed_run = repository.completed_refresh_runs[-1][1]
        self.assertEqual(completed_run['status'], 'failed')
        self.assertEqual(completed_run['symbols_failed'], 1)

    async def test_daily_incremental_refresh_tolerates_terminal_mapping_failure(self):
        class UnsupportedCaptureClient(FakeCaptureClient):
            async def has_supported_stock_code(self, stock_code):
                return False

        client = UnsupportedCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '888888'}],
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            side_effect=AssertionError('terminal mapping failures should not reach sync'),
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_initial_backfill_enabled',
            True,
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_incremental_lookback_days',
            5000,
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['symbols_failed'], 0)
        self.assertEqual(result['partial_reasons'], [])
        self.assertEqual(result['operator_buckets']['lookup_or_mapping_errors']['count'], 0)
        self.assertEqual(result['operator_buckets']['terminal_mapping_failed']['symbol_count'], 1)
        completed_run = repository.completed_refresh_runs[-1][1]
        self.assertEqual(completed_run['status'], 'completed')
        self.assertEqual(completed_run['symbols_failed'], 0)

    async def test_daily_incremental_refresh_is_partial_when_one_lookup_fails(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        repository.symbol_states = {
            '005930': {
                'stock_code': '005930',
                'has_fundamentals': True,
                'latest_fundamentals_receipt_date': '2026-04-01',
                'latest_snapshot': {'id': 100},
            },
        }
        service = DartFundamentalsCaptureService(
            client=client,
            repository=repository,
            active_symbol_loader=lambda: [
                {'symbol': '005930'},
                {'symbol': '999999'},
            ],
        )

        with patch(
            'app.dart.sync_service.dart_disclosure_sync_service.sync_disclosures_for_corporations',
            return_value={
                'run_id': 77,
                'status': 'completed',
                'financial_report_events': 1,
            },
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_initial_backfill_enabled',
            True,
        ), patch(
            'app.dart.fundamentals_capture_service.settings.dart_fundamentals_daily_incremental_lookback_days',
            5000,
        ):
            result = await service.run_daily_incremental_refresh(
                market_date=date(2026, 4, 14),
                capture_limit=10,
                fs_div='CFS',
                trigger='test_daily',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['symbols_failed'], 1)
        self.assertIn('symbol_failures', result['partial_reasons'])
        self.assertEqual(result['samples']['failed_symbols'], ['999999'])
        completed_run = repository.completed_refresh_runs[-1][1]
        self.assertEqual(completed_run['status'], 'partial')
        self.assertEqual(completed_run['symbols_failed'], 1)

    async def test_daily_incremental_refresh_skips_completed_market_date_without_force(self):
        repository = FakeCaptureRepository()
        repository.latest_refresh_run = {
            'id': 188,
            'market_date': '2026-04-14',
            'status': 'completed',
            'fs_div': 'CFS',
        }
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=repository,
            active_symbol_loader=lambda: [{'symbol': '005930'}],
        )

        result = await service.run_daily_incremental_refresh(market_date=date(2026, 4, 14), fs_div='CFS')

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'already_completed')
        self.assertEqual(repository.created_refresh_runs, [])


class InferFundamentalsRequestTests(unittest.TestCase):
    def test_infer_fundamentals_request_maps_annual_and_quarterly_periods(self):
        annual = infer_fundamentals_request(
            {'report_nm': '사업보고서 (2025.12)', 'filing_category': 'annual_report'},
            fs_div='CFS',
        )
        quarter = infer_fundamentals_request(
            {'report_nm': '분기보고서 (2025.09)', 'filing_category': 'quarterly_report'},
            fs_div='OFS',
            fiscal_year_end_month=12,
        )
        non_calendar_quarter = infer_fundamentals_request(
            {'report_nm': '분기보고서 (2025.12)', 'filing_category': 'quarterly_report'},
            fs_div='CFS',
            fiscal_year_end_month=3,
        )
        non_calendar_first_quarter = infer_fundamentals_request(
            {'report_nm': '분기보고서 (2025.06)', 'filing_category': 'quarterly_report'},
            fs_div='CFS',
            fiscal_year_end_month=3,
        )

        self.assertEqual(annual['report_code'], '11011')
        self.assertEqual(annual['business_year'], 2025)
        self.assertEqual(quarter['report_code'], '11014')
        self.assertEqual(quarter['statement_type'], 'separate')
        self.assertEqual(non_calendar_quarter['report_code'], '11014')
        self.assertEqual(non_calendar_first_quarter['report_code'], '11013')

    def test_infer_fundamentals_request_rejects_quarterly_requests_without_fiscal_calendar(self):
        with self.assertRaisesRegex(ValueError, 'requires a fiscal year-end month'):
            infer_fundamentals_request(
                {'report_nm': '분기보고서 (2025.06)', 'filing_category': 'quarterly_report'},
                fs_div='CFS',
            )

    def test_infer_fundamentals_request_uses_fiscal_year_end_for_direct_backfill_period_end(self):
        annual_backfill = infer_fundamentals_request(
            {'report_nm': 'direct_backfill_2025_11011', 'filing_category': 'direct_backfill'},
            fs_div='CFS',
            fiscal_year_end_month=3,
        )
        half_backfill = infer_fundamentals_request(
            {'report_nm': 'direct_backfill_2025_11012', 'filing_category': 'direct_backfill'},
            fs_div='CFS',
            fiscal_year_end_month=3,
        )

        self.assertEqual(annual_backfill['period_end_month'], 3)
        self.assertEqual(half_backfill['period_end_month'], 9)


class BuildRowPayloadsTests(unittest.TestCase):
    def test_build_row_payloads_disambiguates_same_account_rows_with_distinct_details(self):
        items = [
            {
                'statement_type': 'BS',
                'statement_name': '재무상태표',
                'account_id': 'ifrs-full_Assets',
                'account_name': '자산총계',
                'account_detail': '연결재무제표',
                'current_period_name': '당기',
                'prior_period_name': '전기',
                'prior_quarter_name': None,
                'pre_prior_period_name': None,
                'order': 1,
                'receipt_number': '20260407000003',
                'report_code': '11011',
                'business_year': '2025',
                'current_amount': 100,
                'current_cumulative_amount': None,
                'prior_amount': 90,
                'prior_quarter_amount': None,
                'prior_cumulative_amount': None,
                'pre_prior_amount': None,
                'currency': 'KRW',
                'raw': {'ord': '1'},
            },
            {
                'statement_type': 'BS',
                'statement_name': '재무상태표',
                'account_id': 'ifrs-full_Assets',
                'account_name': '자산총계',
                'account_detail': '주석 상세',
                'current_period_name': '당기',
                'prior_period_name': '전기',
                'prior_quarter_name': None,
                'pre_prior_period_name': None,
                'order': 1,
                'receipt_number': '20260407000003',
                'report_code': '11011',
                'business_year': '2025',
                'current_amount': 100,
                'current_cumulative_amount': None,
                'prior_amount': 90,
                'prior_quarter_amount': None,
                'prior_cumulative_amount': None,
                'pre_prior_amount': None,
                'currency': 'KRW',
                'raw': {'ord': '1'},
            },
            {
                'statement_type': 'BS',
                'statement_name': '재무상태표',
                'account_id': 'ifrs-full_Assets',
                'account_name': '자산총계',
                'account_detail': '주석 상세',
                'current_period_name': '당기',
                'prior_period_name': '전기',
                'prior_quarter_name': None,
                'pre_prior_period_name': None,
                'order': 1,
                'receipt_number': '20260407000003',
                'report_code': '11011',
                'business_year': '2025',
                'current_amount': 100,
                'current_cumulative_amount': None,
                'prior_amount': 90,
                'prior_quarter_amount': None,
                'prior_cumulative_amount': None,
                'pre_prior_amount': None,
                'currency': 'KRW',
                'raw': {'ord': '1'},
            },
        ]

        payloads = build_row_payloads(items)

        self.assertEqual(len(payloads), 3)
        self.assertEqual(len({payload['row_key'] for payload in payloads}), 3)
        self.assertNotEqual(payloads[0]['row_key'], payloads[1]['row_key'])
        self.assertTrue(payloads[2]['row_key'].endswith(':2'))


if __name__ == '__main__':
    unittest.main()
