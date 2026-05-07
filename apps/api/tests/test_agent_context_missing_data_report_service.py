from datetime import date
import unittest

from app.agent_context.missing_data_report_service import AgentContextMissingDataReportService
from app.agent_context.missing_data_taxonomy import build_missing_data_categories


class FakeMissingDataReportRepository:
    def __init__(self, rows):
        self.rows = list(rows)
        self.list_args = None
        self.persisted_rows = None

    def list_feedback_rows(self, **kwargs):
        self.list_args = kwargs
        return list(self.rows)

    def update_missing_data_categories(self, *, rows):
        self.persisted_rows = list(rows)
        return len(rows)


class AgentContextMissingDataReportServiceTests(unittest.TestCase):
    def test_summary_dedupes_duplicate_notes_within_one_feedback_row(self):
        repository = FakeMissingDataReportRepository(
            [
                {
                    'id': 1,
                    'market_date': date(2026, 5, 5),
                    'symbol': '005930',
                    'agent_name': 'signal-decider',
                    'feedback_payload': {'role': 'signal-decider'},
                    'normalized_feedback_payload': {'role': 'signal-decider'},
                    'data_gaps_payload': {'items': ['Need factor score coverage.']},
                    'improvement_notes_payload': [
                        'Need factor score coverage.',
                        'Need factor score coverage.',
                        '시장/업종 상대 비교 필요',
                    ],
                    'missing_data_categories_payload': None,
                }
            ]
        )
        service = AgentContextMissingDataReportService(repository=repository)

        result = service.get_summary(market_date='2026-05-05')

        self.assertEqual(repository.list_args['market_date_from'], date(2026, 5, 5))
        self.assertEqual(repository.list_args['market_date_to'], date(2026, 5, 5))
        self.assertEqual(result['summary']['feedback_rows_matched'], 1)
        self.assertEqual(result['summary']['unique_note_count'], 2)
        self.assertEqual(result['summary']['matched_note_count'], 2)
        factor_row = next(item for item in result['categories'] if item['category'] == 'factor_score_coverage')
        market_row = next(item for item in result['categories'] if item['category'] == 'market_sector_peer_relative')
        self.assertEqual(factor_row['counts']['feedback_rows'], 1)
        self.assertEqual(factor_row['counts']['note_mentions'], 1)
        self.assertEqual(factor_row['example_notes'], ['Need factor score coverage.'])
        self.assertEqual(market_row['counts']['note_mentions'], 1)

    def test_summary_includes_date_breakdown_and_category_trends(self):
        repository = FakeMissingDataReportRepository(
            [
                {
                    'id': 1,
                    'market_date': date(2026, 5, 5),
                    'symbol': '005930',
                    'agent_name': 'signal-decider',
                    'feedback_payload': {'role': 'signal-decider'},
                    'normalized_feedback_payload': {'role': 'signal-decider'},
                    'data_gaps_payload': None,
                    'improvement_notes_payload': ['Need factor score coverage.'],
                    'missing_data_categories_payload': None,
                },
                {
                    'id': 2,
                    'market_date': date(2026, 5, 6),
                    'symbol': '000660',
                    'agent_name': 'technical-summarizer',
                    'feedback_payload': {'role': 'technical-summarizer'},
                    'normalized_feedback_payload': {'role': 'technical-summarizer'},
                    'data_gaps_payload': None,
                    'improvement_notes_payload': [
                        '거래대금과 슬리피지 근거 필요',
                        '시가 0이라 갭 신뢰도 낮음',
                    ],
                    'missing_data_categories_payload': None,
                },
            ]
        )
        service = AgentContextMissingDataReportService(repository=repository)

        result = service.get_summary(market_date_from='2026-05-05', market_date_to='2026-05-06')

        self.assertEqual(result['summary']['feedback_rows_scanned'], 2)
        self.assertEqual(result['summary']['feedback_rows_matched'], 2)
        self.assertEqual(result['summary']['distinct_symbols_matched'], 2)
        self.assertEqual(result['date_breakdown'][0]['market_date'], '2026-05-05')
        self.assertEqual(result['date_breakdown'][0]['category_counts']['factor_score_coverage']['feedback_rows'], 1)
        self.assertEqual(result['date_breakdown'][1]['category_counts']['liquidity_turnover_slippage']['note_mentions'], 1)
        self.assertEqual(result['date_breakdown'][1]['category_counts']['price_bar_quality_session_state']['note_mentions'], 1)
        liquidity_row = next(item for item in result['categories'] if item['category'] == 'liquidity_turnover_slippage')
        self.assertEqual(liquidity_row['date_breakdown'][0]['market_date'], '2026-05-06')
        self.assertEqual(liquidity_row['date_breakdown'][0]['feedback_rows'], 1)

    def test_summary_exposes_category_mapping_and_can_persist_backfill(self):
        raw_categories = build_missing_data_categories(
            data_gaps_payload=None,
            improvement_notes_payload=['컨센서스 가이던스 필요', '투자자별 수급 필요'],
        )
        repository = FakeMissingDataReportRepository(
            [
                {
                    'id': 7,
                    'market_date': date(2026, 5, 6),
                    'symbol': '035420',
                    'agent_name': 'fundamental-summarizer',
                    'feedback_payload': {'role': 'fundamental-summarizer'},
                    'normalized_feedback_payload': {'role': 'fundamental-summarizer'},
                    'data_gaps_payload': None,
                    'improvement_notes_payload': ['컨센서스 가이던스 필요', '투자자별 수급 필요'],
                    'missing_data_categories_payload': None,
                }
            ]
        )
        service = AgentContextMissingDataReportService(repository=repository)

        result = service.get_summary(
            market_date='2026-05-06',
            categories=['investor_flows_short_selling', 'consensus_guidance'],
            persist_missing_categories=True,
        )

        self.assertEqual(result['summary']['persisted_updates'], 1)
        self.assertEqual(repository.persisted_rows, [{'id': 7, 'missing_data_categories_payload': raw_categories}])
        investor_row = next(item for item in result['categories'] if item['category'] == 'investor_flows_short_selling')
        consensus_row = next(item for item in result['categories'] if item['category'] == 'consensus_guidance')
        self.assertEqual(investor_row['status'], 'external_data_required')
        self.assertIn('KIS investor flow endpoints', investor_row['external_providers'])
        self.assertEqual(consensus_row['status'], 'external_data_required')
        self.assertEqual(consensus_row['counts']['roles'], 1)
