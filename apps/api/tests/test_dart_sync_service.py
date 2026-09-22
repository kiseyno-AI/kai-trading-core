import unittest
from datetime import date
from unittest.mock import patch

from app.core.settings import settings
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


class DartDisclosureSyncServiceTests(unittest.IsolatedAsyncioTestCase):
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
            )
        finally:
            settings.dart_sync_page_count = original_page_count

        self.assertEqual(result['corporations_requested'], 1)
        self.assertEqual(result['corporations_processed'], 1)
        self.assertEqual(result['financial_report_events'], 2)
        self.assertEqual(sum(len(batch) for _, batch in repository.upserted_batches), 2)

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
