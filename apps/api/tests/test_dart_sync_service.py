import unittest

from app.core.settings import settings
from app.dart.sync_service import DartDisclosureSyncService, classify_filing


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


class DartDisclosureSyncServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_recent_disclosures_paginates_and_classifies_financial_reports(self):
        client = FakeSyncClient()
        repository = FakeSyncRepository()
        service = DartDisclosureSyncService(client=client, repository=repository)
        original_page_count = settings.dart_sync_page_count
        settings.dart_sync_page_count = 2
        try:
            result = await service.sync_recent_disclosures(days=3, trigger='test')
        finally:
            settings.dart_sync_page_count = original_page_count

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['run_id'], 41)
        self.assertEqual(result['pages_fetched'], 2)
        self.assertEqual(result['events_seen'], 3)
        self.assertEqual(result['events_inserted'], 3)
        self.assertEqual(result['financial_report_events'], 2)
        self.assertEqual(client.calls[0]['last_reprt_at'], 'N')
        self.assertEqual(repository.created_runs[0]['trigger'], 'test')
        first_batch = repository.upserted_batches[0][1]
        self.assertTrue(first_batch[0]['is_financial_report'])
        self.assertEqual(first_batch[0]['filing_category'], 'annual_report')
        self.assertFalse(first_batch[1]['is_financial_report'])
        self.assertEqual(first_batch[1]['filing_category'], 'other')
        self.assertEqual(repository.completed_runs[-1][1]['status'], 'completed')

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
