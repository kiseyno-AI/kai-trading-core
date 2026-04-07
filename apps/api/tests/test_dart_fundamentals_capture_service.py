import unittest

from app.dart.fundamentals_capture_service import DartFundamentalsCaptureService, infer_fundamentals_request


class FakeCaptureClient:
    def __init__(self):
        self.calls = []

    async def fetch_financial_statements(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs['corp_code'] == 'fail-me':
            raise RuntimeError('upstream timeout')
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


class FakeCaptureRepository:
    def __init__(self):
        self.created_runs = []
        self.completed_runs = []
        self.successes = []
        self.failures = []
        self.snapshots = []

    def create_capture_run(self, **kwargs):
        self.created_runs.append(kwargs)
        return 88

    def complete_capture_run(self, run_id, **kwargs):
        self.completed_runs.append((run_id, kwargs))

    def list_capture_candidates(self, *, limit=20, force=False):
        return [
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
        ][:limit]

    def replace_snapshot_dataset(self, **kwargs):
        self.snapshots.append(kwargs)
        return 101

    def mark_event_capture_success(self, event_id):
        self.successes.append(event_id)

    def mark_event_capture_failed(self, event_id, *, error_message):
        self.failures.append((event_id, error_message))

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
    async def test_capture_recent_fundamentals_persists_snapshots_and_marks_event_status(self):
        client = FakeCaptureClient()
        repository = FakeCaptureRepository()
        service = DartFundamentalsCaptureService(client=client, repository=repository)

        result = await service.capture_recent_fundamentals(limit=2, force=False, trigger='test', fs_div='CFS')

        self.assertEqual(result['run_id'], 88)
        self.assertEqual(result['events_seen'], 2)
        self.assertEqual(result['events_captured'], 1)
        self.assertEqual(result['events_failed'], 1)
        self.assertEqual(client.calls[0]['reprt_code'], '11011')
        self.assertEqual(client.calls[1]['reprt_code'], '11012')
        self.assertEqual(repository.successes, [1])
        self.assertEqual(repository.failures[0][0], 2)
        self.assertEqual(repository.snapshots[0]['snapshot_payload']['business_year'], 2025)
        self.assertEqual(repository.completed_runs[-1][1]['status'], 'completed')

    async def test_inspect_capture_status_returns_repository_views(self):
        service = DartFundamentalsCaptureService(
            client=FakeCaptureClient(),
            repository=FakeCaptureRepository(),
        )

        result = service.inspect_capture_status(runs_limit=5, events_limit=1, capture_status='failed')

        self.assertEqual(result['latest_run']['id'], 88)
        self.assertEqual(result['summary']['financial_event_capture_status_counts']['captured'], 1)
        self.assertEqual(result['events'][0]['fundamentals_capture_status'], 'failed')


class InferFundamentalsRequestTests(unittest.TestCase):
    def test_infer_fundamentals_request_maps_annual_and_quarterly_periods(self):
        annual = infer_fundamentals_request(
            {'report_nm': '사업보고서 (2025.12)', 'filing_category': 'annual_report'},
            fs_div='CFS',
        )
        quarter = infer_fundamentals_request(
            {'report_nm': '분기보고서 (2025.09)', 'filing_category': 'quarterly_report'},
            fs_div='OFS',
        )

        self.assertEqual(annual['report_code'], '11011')
        self.assertEqual(annual['business_year'], 2025)
        self.assertEqual(quarter['report_code'], '11014')
        self.assertEqual(quarter['statement_type'], 'separate')

    def test_infer_fundamentals_request_rejects_unknown_quarter_window(self):
        with self.assertRaisesRegex(ValueError, 'Unsupported quarterly period month'):
            infer_fundamentals_request(
                {'report_nm': '분기보고서 (2025.06)', 'filing_category': 'quarterly_report'},
                fs_div='CFS',
            )


if __name__ == '__main__':
    unittest.main()
