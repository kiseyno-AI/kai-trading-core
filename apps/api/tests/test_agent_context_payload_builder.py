import unittest
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context.payload_builder import (
    AgentPayloadBuilder,
    PayloadWindows,
    _build_signal_participation_context,
)


class FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class SequentialFakeSession:
    def __init__(self, results):
        self._results = list(results)

    def execute(self, statement):
        if not self._results:
            raise AssertionError(f'unexpected query: {statement}')
        return FakeExecuteResult(self._results.pop(0))


class AgentPayloadBuilderTests(unittest.TestCase):
    def test_build_signal_participation_context_uses_full_row_source_counts_and_sorted_leaders(self):
        rows = [
            {
                'symbol': '005930',
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'signal_count': 3,
                'volume_power_rank': 2,
                'volume_power': 145.2,
                'overtime_volume_rank': 4,
                'overtime_volume': 120000,
                'overtime_fluctuation_rank': 7,
                'overtime_fluctuation_rate': 2.4,
                'feature_payload': {
                    'source_counts': {
                        'volume-power': 50,
                        'overtime-volume': 35,
                        'overtime-fluctuation': 25,
                    }
                },
            },
            {
                'symbol': '000660',
                'name_ko': 'SK하이닉스',
                'market': 'KOSPI',
                'signal_count': 2,
                'volume_power_rank': 1,
                'volume_power': 151.5,
                'overtime_volume_rank': None,
                'overtime_volume': None,
                'overtime_fluctuation_rank': 3,
                'overtime_fluctuation_rate': 3.9,
                'feature_payload': {},
            },
            {
                'symbol': '035420',
                'name_ko': 'NAVER',
                'market': 'KOSPI',
                'signal_count': 1,
                'volume_power_rank': None,
                'volume_power': None,
                'overtime_volume_rank': 1,
                'overtime_volume': 99000,
                'overtime_fluctuation_rank': None,
                'overtime_fluctuation_rate': None,
                'feature_payload': {},
            },
        ]
        run = SimpleNamespace(symbols_total=120, symbols_succeeded=100)

        result = _build_signal_participation_context(rows, limit=2, run=run)

        self.assertEqual(result['captured_symbol_count'], 3)
        self.assertEqual(result['coverage_ratio_vs_run_total'], 0.025)
        self.assertEqual(result['coverage_ratio_vs_run_succeeded'], 0.03)
        self.assertEqual(result['signal_count_distribution'], {'1': 1, '2': 1, '3': 1})
        self.assertEqual(
            result['source_counts'],
            {
                'volume-power': 2,
                'overtime-volume': 2,
                'overtime-fluctuation': 2,
            },
        )
        self.assertEqual(result['leaders_by_source']['volume-power'][0]['symbol'], '000660')
        self.assertEqual(result['leaders_by_source']['overtime-volume'][0]['symbol'], '035420')
        self.assertEqual(result['leaders_by_source']['overtime-fluctuation'][0]['symbol'], '000660')

    def test_build_signal_participation_context_is_order_independent_when_payload_source_counts_are_mixed(self):
        rows = [
            {
                'symbol': '005930',
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'signal_count': 2,
                'volume_power_rank': 2,
                'volume_power': 145.2,
                'overtime_volume_rank': None,
                'overtime_volume': None,
                'overtime_fluctuation_rank': 3,
                'overtime_fluctuation_rate': 2.4,
                'feature_payload': {'source_counts': {'volume-power': 50}},
            },
            {
                'symbol': '000660',
                'name_ko': 'SK하이닉스',
                'market': 'KOSPI',
                'signal_count': 2,
                'volume_power_rank': 1,
                'volume_power': 151.5,
                'overtime_volume_rank': 2,
                'overtime_volume': 88000,
                'overtime_fluctuation_rank': None,
                'overtime_fluctuation_rate': None,
                'feature_payload': {'source_counts': {'overtime-volume': 35}},
            },
            {
                'symbol': '035420',
                'name_ko': 'NAVER',
                'market': 'KOSPI',
                'signal_count': 2,
                'volume_power_rank': None,
                'volume_power': None,
                'overtime_volume_rank': 1,
                'overtime_volume': 99000,
                'overtime_fluctuation_rank': 1,
                'overtime_fluctuation_rate': 3.1,
                'feature_payload': {},
            },
        ]
        run = SimpleNamespace(symbols_total=120, symbols_succeeded=100)

        result = _build_signal_participation_context(rows, limit=2, run=run)
        reversed_result = _build_signal_participation_context(list(reversed(rows)), limit=2, run=run)

        expected_source_counts = {
            'volume-power': 2,
            'overtime-volume': 2,
            'overtime-fluctuation': 2,
        }
        self.assertEqual(result['source_counts'], expected_source_counts)
        self.assertEqual(reversed_result['source_counts'], expected_source_counts)
        self.assertEqual(
            result['source_coverage_ratio_vs_run_succeeded'],
            {
                'volume-power': 0.02,
                'overtime-volume': 0.02,
                'overtime-fluctuation': 0.02,
            },
        )
        self.assertEqual(
            reversed_result['leaders_by_source']['overtime-volume'][0]['symbol'],
            '035420',
        )

    def test_build_market_payload_includes_participation_context(self):
        builder = AgentPayloadBuilder(
            windows=PayloadWindows(
                market_breadth_days=2,
                top_n=2,
            )
        )
        run = SimpleNamespace(
            trade_date=date.fromisoformat('2026-04-15'),
            symbols_total=120,
            symbols_succeeded=100,
        )
        fake_session = SequentialFakeSession(
            [
                [
                    (
                        SimpleNamespace(
                            period='1d',
                            absolute_rank=1,
                            trend_direction='up',
                            metric_name='return_1d',
                            metric_value=4.2,
                            price_payload={'close_price': 71500},
                            technical_payload={'trend_state': 'uptrend'},
                            action_summary_payload={'action_label': 'watch_long'},
                        ),
                        SimpleNamespace(symbol='005930', name_ko='삼성전자', market='KOSPI'),
                    )
                ],
                [
                    (
                        SimpleNamespace(
                            signal_count=3,
                            volume_power_rank=2,
                            volume_power=145.2,
                            overtime_volume_rank=4,
                            overtime_volume=120000,
                            overtime_fluctuation_rank=7,
                            overtime_fluctuation_rate=2.4,
                            feature_payload={
                                'source_counts': {
                                    'volume-power': 50,
                                    'overtime-volume': 35,
                                    'overtime-fluctuation': 25,
                                }
                            },
                        ),
                        SimpleNamespace(symbol='005930', name_ko='삼성전자', market='KOSPI'),
                    ),
                    (
                        SimpleNamespace(
                            signal_count=2,
                            volume_power_rank=1,
                            volume_power=151.5,
                            overtime_volume_rank=None,
                            overtime_volume=None,
                            overtime_fluctuation_rank=3,
                            overtime_fluctuation_rate=3.9,
                            feature_payload={},
                        ),
                        SimpleNamespace(symbol='000660', name_ko='SK하이닉스', market='KOSPI'),
                    ),
                ],
                [
                    (date.fromisoformat('2026-04-14'), 100, 1000000000, 5000000),
                    (date.fromisoformat('2026-04-15'), 101, 1100000000, 5500000),
                ],
            ]
        )

        @contextmanager
        def fake_session_scope():
            yield fake_session

        persisted_context = {
            'context_payload': {
                'captured_symbol_count': 2,
                'signal_count_distribution': {'2': 1, '3': 1},
                'source_counts': {
                    'volume-power': 2,
                    'overtime-volume': 1,
                    'overtime-fluctuation': 2,
                },
                'leaders_by_source': {
                    'volume-power': [
                        {
                            'symbol': '000660',
                            'name_ko': 'SK하이닉스',
                            'market': 'KOSPI',
                            'rank': 1,
                            'metric_name': 'volume_power',
                            'metric_value': 151.5,
                            'signal_count': 2,
                        },
                        {
                            'symbol': '005930',
                            'name_ko': '삼성전자',
                            'market': 'KOSPI',
                            'rank': 2,
                            'metric_name': 'volume_power',
                            'metric_value': 145.2,
                            'signal_count': 3,
                        },
                    ],
                    'overtime-volume': [
                        {
                            'symbol': '005930',
                            'name_ko': '삼성전자',
                            'market': 'KOSPI',
                            'rank': 4,
                            'metric_name': 'overtime_volume',
                            'metric_value': 120000,
                            'signal_count': 3,
                        }
                    ],
                    'overtime-fluctuation': [
                        {
                            'symbol': '000660',
                            'name_ko': 'SK하이닉스',
                            'market': 'KOSPI',
                            'rank': 3,
                            'metric_name': 'overtime_fluctuation_rate',
                            'metric_value': 3.9,
                            'signal_count': 2,
                        },
                        {
                            'symbol': '005930',
                            'name_ko': '삼성전자',
                            'market': 'KOSPI',
                            'rank': 7,
                            'metric_name': 'overtime_fluctuation_rate',
                            'metric_value': 2.4,
                            'signal_count': 3,
                        },
                    ],
                },
            }
        }
        persisted_history = [
            {
                'market_date': '2026-04-14',
                'source': 'kis_ranking',
                'captured_at': '2026-04-14T15:00:00+09:00',
                'context_payload': {
                    'captured_symbol_count': 1,
                    'signal_count_distribution': {'1': 1},
                    'source_counts': {
                        'volume-power': 1,
                        'overtime-volume': 0,
                        'overtime-fluctuation': 1,
                    },
                    'leaders_by_source': {
                        'volume-power': [
                            {
                                'symbol': '005930',
                                'name_ko': '삼성전자',
                                'market': 'KOSPI',
                                'rank': 1,
                                'metric_name': 'volume_power',
                                'metric_value': 140.5,
                                'signal_count': 1,
                            }
                        ],
                        'overtime-volume': [],
                        'overtime-fluctuation': [
                            {
                                'symbol': '005930',
                                'name_ko': '삼성전자',
                                'market': 'KOSPI',
                                'rank': 2,
                                'metric_name': 'overtime_fluctuation_rate',
                                'metric_value': 1.1,
                                'signal_count': 1,
                            }
                        ],
                    },
                },
            },
            {
                'market_date': '2026-04-15',
                'source': 'kis_ranking',
                'captured_at': '2026-04-15T15:00:00+09:00',
                'context_payload': persisted_context['context_payload'],
            },
        ]

        with patch('app.agent_context.payload_builder.session_scope', fake_session_scope):
            with patch(
                'app.agent_context.payload_builder.load_daily_market_participation_context',
                return_value=persisted_context,
            ):
                with patch(
                    'app.agent_context.payload_builder.load_daily_market_participation_context_history',
                    return_value=persisted_history,
                ):
                    payload = builder._build_market_payload(run=run)

        self.assertIn('participation_context', payload)
        self.assertIn('participation_context_history', payload)
        self.assertEqual(payload['signal_feature_leaders'][0]['symbol'], '005930')
        self.assertEqual(payload['participation_context']['captured_symbol_count'], 2)
        self.assertEqual(
            payload['participation_context']['source_counts'],
            {
                'volume-power': 2,
                'overtime-volume': 1,
                'overtime-fluctuation': 2,
            },
        )
        self.assertEqual(
            payload['participation_context']['leaders_by_source']['volume-power'][0]['symbol'],
            '000660',
        )
        self.assertEqual(len(payload['participation_context_history']), 2)
        self.assertEqual(payload['participation_context_history'][0]['market_date'], '2026-04-14')
        self.assertEqual(
            payload['participation_context_history'][1]['participation_context']['captured_symbol_count'],
            2,
        )


if __name__ == '__main__':
    unittest.main()
