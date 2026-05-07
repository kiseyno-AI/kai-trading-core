import json
from datetime import date
from pathlib import Path
import unittest

from app.agent_context.missing_data_report_service import AgentContextMissingDataReportService
from app.agent_context.missing_data_taxonomy import build_missing_data_categories


FIXTURE_PATH = Path(__file__).with_name('fixtures') / 'agent_feedback_data_gap_quality_rows.json'


def _load_quality_report_fixture_rows(name: str) -> list[dict]:
    payload = json.loads(FIXTURE_PATH.read_text())
    rows = []
    for item in payload[name]:
        row = dict(item)
        row['market_date'] = date.fromisoformat(row['market_date'])
        rows.append(row)
    return rows


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

    def test_summary_counts_unmatched_notes_on_partially_matched_rows(self):
        repository = FakeMissingDataReportRepository(
            [
                {
                    'id': 2,
                    'market_date': date(2026, 5, 5),
                    'symbol': '005930',
                    'agent_name': 'signal-decider',
                    'feedback_payload': {'role': 'signal-decider'},
                    'normalized_feedback_payload': {'role': 'signal-decider'},
                    'data_gaps_payload': None,
                    'improvement_notes_payload': ['Need factor score coverage.', '추가 메모 필요'],
                    'missing_data_categories_payload': None,
                }
            ]
        )
        service = AgentContextMissingDataReportService(repository=repository)

        result = service.get_summary(market_date='2026-05-05')

        self.assertEqual(result['summary']['feedback_rows_matched'], 1)
        self.assertEqual(result['summary']['matched_note_count'], 1)
        self.assertEqual(result['summary']['unmatched_note_count'], 1)
        self.assertEqual(
            result['uncategorized_examples'],
            [
                {
                    'market_date': '2026-05-05',
                    'symbol': '005930',
                    'agent_name': 'signal-decider',
                    'role': 'signal-decider',
                    'note': '추가 메모 필요',
                }
            ],
        )

    def test_summary_counts_filtered_out_notes_as_unmatched_examples(self):
        repository = FakeMissingDataReportRepository(
            [
                {
                    'id': 3,
                    'market_date': date(2026, 5, 5),
                    'symbol': '005930',
                    'agent_name': 'signal-decider',
                    'feedback_payload': {'role': 'signal-decider'},
                    'normalized_feedback_payload': {'role': 'signal-decider'},
                    'data_gaps_payload': None,
                    'improvement_notes_payload': ['Need factor score coverage.', '시장/업종 상대 비교 필요'],
                    'missing_data_categories_payload': None,
                }
            ]
        )
        service = AgentContextMissingDataReportService(repository=repository)

        result = service.get_summary(
            market_date='2026-05-05',
            categories=['factor_score_coverage'],
        )

        self.assertEqual(result['summary']['feedback_rows_matched'], 1)
        self.assertEqual(result['summary']['matched_note_count'], 1)
        self.assertEqual(result['summary']['unmatched_note_count'], 1)
        self.assertEqual([item['category'] for item in result['categories']], ['factor_score_coverage'])
        self.assertEqual(
            result['uncategorized_examples'],
            [
                {
                    'market_date': '2026-05-05',
                    'symbol': '005930',
                    'agent_name': 'signal-decider',
                    'role': 'signal-decider',
                    'note': '시장/업종 상대 비교 필요',
                }
            ],
        )

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
        self.assertEqual(investor_row['status'], 'partially_available')
        self.assertIn('PyKRX EOD investor flow and short-selling statistics', investor_row['external_providers'])
        self.assertEqual(consensus_row['status'], 'external_data_required')
        self.assertEqual(consensus_row['counts']['roles'], 1)

    def test_summary_aggregates_context_issue_type_counts_when_metadata_is_present(self):
        repository = FakeMissingDataReportRepository(
            [
                {
                    'id': 9,
                    'market_date': date(2026, 5, 6),
                    'symbol': '005930',
                    'agent_name': 'technical-summarizer',
                    'feedback_payload': {'role': 'technical-summarizer'},
                    'normalized_feedback_payload': {'role': 'technical-summarizer'},
                    'data_gaps_payload': None,
                    'improvement_notes_payload': ['Need factor score coverage.'],
                    'missing_data_categories_payload': None,
                    'context_payload': {
                        'symbol_day_context': {
                            'context_provenance': {
                                'factor_rows': {
                                    'unified_value': {
                                        'issue_types': ['available_but_old'],
                                        'freshness_status': 'available_but_old',
                                        'provenance_status': 'derived_from_provider_data',
                                        'source_tables': ['daily_unified_value_factor_results'],
                                    }
                                },
                                'signal_features': {
                                    'issue_types': ['derived_or_fallback'],
                                    'freshness_status': 'fresh',
                                    'provenance_status': 'provider_plus_fallback',
                                    'source_tables': ['daily_signal_features'],
                                },
                            }
                        }
                    },
                }
            ]
        )
        service = AgentContextMissingDataReportService(repository=repository)

        result = service.get_summary(market_date='2026-05-06')

        self.assertEqual(result['summary']['context_issue_type_counts']['available_but_old'], 1)
        self.assertEqual(result['summary']['context_issue_type_counts']['derived_or_fallback'], 1)
        factor_row = next(item for item in result['categories'] if item['category'] == 'factor_score_coverage')
        self.assertEqual(factor_row['context_issue_type_counts']['available_but_old'], 1)
        self.assertEqual(factor_row['context_issue_type_counts']['derived_or_fallback'], 1)

    def test_quality_report_regression_fixture_tracks_duplicate_metrics(self):
        repository = FakeMissingDataReportRepository(_load_quality_report_fixture_rows('duplicate_focus'))
        service = AgentContextMissingDataReportService(repository=repository)

        result = service.get_summary(market_date='2026-05-05')

        self.assertEqual(result['summary']['raw_note_mentions'], 5)
        self.assertEqual(result['summary']['duplicate_note_mentions'], 3)
        self.assertEqual(result['summary']['feedback_rows_with_duplicate_notes'], 1)
        self.assertEqual(result['summary']['duplicate_note_rate'], 0.6)
        self.assertEqual(result['quality']['note_metrics']['duplicate_note_rate'], 0.6)
        self.assertEqual(
            result['quality']['classification_metrics']['context_issue_type_note_mentions'],
            {
                'available_but_old': 1,
                'derived_or_fallback': 2,
                'stale': 1,
            },
        )
        self.assertEqual(
            result['quality']['resolution_metrics']['requires_external_data']['categories'],
            ['liquidity_turnover_slippage'],
        )
        self.assertEqual(
            result['quality']['resolution_metrics']['resolved_by_existing_data']['categories'],
            ['factor_score_coverage'],
        )
        signal_features = next(
            item
            for item in result['quality']['context_block_coverage']
            if item['block'] == 'signal_features'
        )
        self.assertEqual(signal_features['requested_symbol_coverage']['average_coverage_ratio'], 1.0)

    def test_quality_report_regression_fixture_builds_range_and_category_trends(self):
        repository = FakeMissingDataReportRepository(_load_quality_report_fixture_rows('range_focus'))
        service = AgentContextMissingDataReportService(repository=repository)

        result = service.get_summary(market_date_from='2026-05-05', market_date_to='2026-05-06')

        comparison = result['quality']['range_comparison']
        self.assertEqual(comparison['before_market_date'], '2026-05-05')
        self.assertEqual(comparison['after_market_date'], '2026-05-06')
        self.assertEqual(comparison['matched_note_delta'], -1)
        self.assertEqual(comparison['direction'], 'improving')
        factor_row = next(item for item in result['categories'] if item['category'] == 'factor_score_coverage')
        liquidity_row = next(item for item in result['categories'] if item['category'] == 'liquidity_turnover_slippage')
        self.assertEqual(factor_row['trend']['direction'], 'improving')
        self.assertEqual(factor_row['trend']['note_mentions_delta'], -1)
        self.assertEqual(liquidity_row['trend']['direction'], 'worsening')
        self.assertEqual(liquidity_row['trend']['note_mentions_delta'], 1)
        self.assertEqual(result['date_breakdown'][0]['matched_note_count'], 3)
        self.assertEqual(result['date_breakdown'][1]['matched_note_count'], 2)
