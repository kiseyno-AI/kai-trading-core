import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from app.agent_context.decision_input_quality import build_decision_input_quality_envelope
from app.core.settings import Settings, settings


def _quality_context(
    *,
    turnover_status: str = 'available',
    trading_value_source: str = 'raw',
    trading_value_krw: int | None = 8_000_000_000,
    liquidity_status: str = 'available',
    intraday_status: str = 'available',
    news_status: str = 'empty',
) -> dict:
    return {
        'status': 'partial',
        'blocks': {
            'liquidity_turnover': {
                'status': liquidity_status,
                'turnover': {
                    'status': turnover_status,
                    'trading_value_source': trading_value_source,
                    'trading_value_krw': trading_value_krw,
                    'raw_trading_value_krw': trading_value_krw if trading_value_source == 'raw' else None,
                    'estimated_trading_value_krw': trading_value_krw if trading_value_source == 'estimated' else None,
                    'freshness': {
                        'freshness_status': 'fresh',
                        'provenance_status': 'provider_sourced' if trading_value_source == 'raw' else 'fallback_from_owned_data',
                    },
                },
                'liquidity_ranking': {
                    'status': 'available',
                    'avg_trading_value_20d': 7_000_000_000,
                    'freshness': {'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
                },
                'decision_guidance': {
                    'category': 'liquidity_turnover_slippage',
                    'decision_tier': 'core_execution',
                },
            },
            'intraday_realtime': {
                'status': intraday_status,
                'quote': {
                    'status': intraday_status,
                    'freshness': {'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
                },
                'decision_guidance': {
                    'category': 'after_hours_intraday_realtime',
                    'decision_tier': 'core_execution',
                },
            },
            'technical_evidence': {
                'status': 'available',
                'decision_guidance': {
                    'category': 'technical_evidence_support_resistance',
                    'decision_tier': 'supporting_context',
                },
            },
            'disclosures_news': {
                'status': news_status,
                'empty_news_is_neutral': news_status == 'empty',
                'decision_guidance': {
                    'category': 'disclosures_news_events',
                    'decision_tier': 'auxiliary_context',
                    'empty_is_neutral_by_default': True,
                },
            },
        },
    }


def _verified_executable_quote_snapshot(
    *,
    spread_bps: float = 17.5,
    market_date: str = '2026-04-29',
    quote_freshness_timestamp: str = '2026-04-29T09:57:00+09:00',
) -> dict:
    return {
        'status': 'available',
        'snapshot_type': 'current_quote_cache',
        'market_date': market_date,
        'quote_freshness_status': 'fresh',
        'quote_freshness_timestamp': quote_freshness_timestamp,
        'as_of_market_date': market_date,
        'current_price': 40_000,
        'bid_price': 39_965,
        'ask_price': 40_035,
        'spread_points': 70,
        'spread_bps': spread_bps,
        'spread_source_context': 'quote_context',
        'spread_component_source_context': {
            'current_price': 'quote_context',
            'bid_price': 'quote_context',
            'ask_price': 'quote_context',
            'spread_points': 'quote_context',
            'spread_bps': 'quote_context',
        },
        'freshness': {
            'quote_context': {
                'freshness_status': 'fresh',
                'provenance_status': 'provider_sourced',
                'as_of_market_date': market_date,
                'as_of_timestamp': quote_freshness_timestamp,
                'source_tables': ['current_quote_cache', 'quote_snapshots'],
                'source_provider': 'kis',
            },
        },
        'source': {'quote_context': 'kis'},
    }


def _envelope_for_quote(quote: dict, *, market_date: str = '2026-04-29') -> dict:
    with patch(
        'app.agent_context.decision_input_quality._utcnow',
        create=True,
        return_value=datetime(2026, 4, 29, 0, 57, 20, tzinfo=timezone.utc),
    ):
        return build_decision_input_quality_envelope(
            symbol='005930',
            market_date=market_date,
            decider_data_quality_context=_quality_context(),
            pre_decider_enrichment_context={
                'quote_orderbook_snapshot': quote,
                'trading_value': {
                    'status': 'available',
                    'source': 'raw',
                    'raw_trading_value_krw': 8_000_000_000,
                },
            },
        )


class DecisionInputQualityTests(unittest.TestCase):
    def test_realtime_price_staleness_setting_rejects_nonfinite_and_unsafe_bounds(
        self,
    ) -> None:
        for invalid in (float('nan'), float('inf'), float('-inf'), 0, -1, 301):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                Settings(
                    _env_file=None,
                    kis_realtime_price_stale_after_seconds=invalid,
                )

        self.assertEqual(
            Settings(
                _env_file=None,
                kis_realtime_price_stale_after_seconds=300,
            ).kis_realtime_price_stale_after_seconds,
            300,
        )

    def test_execution_slippage_accepts_verified_executable_quote_provenance(self) -> None:
        result = _envelope_for_quote(_verified_executable_quote_snapshot())

        feasibility = result['execution_feasibility']
        self.assertEqual(feasibility['expected_slippage_bps'], 17.5)
        self.assertEqual(feasibility['expected_slippage_basis'], 'quote_spread_bps')

    def test_execution_slippage_fails_closed_if_staleness_setting_is_mutated_nonfinite(
        self,
    ) -> None:
        with patch.object(settings, 'kis_realtime_price_stale_after_seconds', float('inf')):
            feasibility = _envelope_for_quote(
                _verified_executable_quote_snapshot()
            )['execution_feasibility']

        self.assertIsNone(feasibility['expected_slippage_bps'])
        self.assertEqual(feasibility['expected_slippage_status'], 'invalid')
        self.assertEqual(
            feasibility['expected_slippage_reason'],
            'quote_staleness_setting_invalid',
        )

    def test_execution_slippage_accepts_live_snapshot_with_verified_quote_spread_source(self) -> None:
        result = _envelope_for_quote(
            {**_verified_executable_quote_snapshot(), 'snapshot_type': 'live_intraday'}
        )

        feasibility = result['execution_feasibility']
        self.assertEqual(feasibility['expected_slippage_bps'], 17.5)
        self.assertEqual(feasibility['expected_slippage_basis'], 'quote_spread_bps')

    def test_execution_slippage_rejects_numeric_spread_without_exact_executable_provenance(self) -> None:
        valid = _verified_executable_quote_snapshot()
        adversarial_quotes = {
            'bare_numeric_spread': {'status': 'available', 'spread_bps': 17.5},
            'snapshot_type_missing': {key: value for key, value in valid.items() if key != 'snapshot_type'},
            'quote_status_partial': {**valid, 'status': 'partial'},
            'freshness_status_stale': {**valid, 'quote_freshness_status': 'stale'},
            'timestamp_missing_timezone': {
                **valid,
                'quote_freshness_timestamp': '2026-04-29T09:57:00',
            },
            'timestamp_date_mismatch': {
                **valid,
                'quote_freshness_timestamp': '2026-04-28T09:57:00+09:00',
            },
            'timestamp_exchange_date_mismatch': {
                **valid,
                'quote_freshness_timestamp': '2026-04-29T23:00:00-04:00',
                'freshness': {
                    'quote_context': {
                        **valid['freshness']['quote_context'],
                        'as_of_timestamp': '2026-04-29T23:00:00-04:00',
                    },
                },
            },
            'as_of_market_date_mismatch': {**valid, 'as_of_market_date': '2026-04-28'},
            'provider_source_missing': {**valid, 'source': {}},
            'provider_source_unrecognized': {**valid, 'source': {'quote_context': 'candidate_payload'}},
            'spread_source_context_missing': {
                key: value for key, value in valid.items() if key != 'spread_source_context'
            },
            'provider_provenance_missing': {**valid, 'freshness': {}},
            'provider_provenance_fallback': {
                **valid,
                'freshness': {
                    'quote_context': {
                        **valid['freshness']['quote_context'],
                        'provenance_status': 'provider_plus_fallback',
                    },
                },
            },
        }

        for case_name, quote in adversarial_quotes.items():
            with self.subTest(case_name=case_name):
                result = _envelope_for_quote(quote)

                feasibility = result['execution_feasibility']
                self.assertIsNone(feasibility['expected_slippage_bps'])
                self.assertEqual(feasibility['expected_slippage_basis'], 'unavailable')
                self.assertIn(feasibility['expected_slippage_status'], {'invalid', 'unavailable'})
                self.assertNotEqual(feasibility['expected_slippage_reason'], 'current_quote_spread')

    def test_execution_slippage_rejects_future_executable_quote_timestamp(self) -> None:
        result = _envelope_for_quote(
            _verified_executable_quote_snapshot(
                market_date='2999-04-29',
                quote_freshness_timestamp='2999-04-29T09:57:00+09:00',
            ),
            market_date='2999-04-29',
        )

        feasibility = result['execution_feasibility']
        self.assertIsNone(feasibility['expected_slippage_bps'])
        self.assertEqual(feasibility['expected_slippage_status'], 'invalid')
        self.assertEqual(feasibility['expected_slippage_reason'], 'quote_timestamp_in_future')

    def test_execution_slippage_rejects_quote_older_than_realtime_stale_setting(self) -> None:
        observed_at = '2026-04-29T09:56:49+09:00'
        quote = _verified_executable_quote_snapshot(quote_freshness_timestamp=observed_at)

        with patch.object(settings, 'kis_realtime_price_stale_after_seconds', 30.0):
            feasibility = _envelope_for_quote(quote)['execution_feasibility']

        self.assertIsNone(feasibility['expected_slippage_bps'])
        self.assertEqual(feasibility['expected_slippage_status'], 'unavailable')
        self.assertEqual(feasibility['expected_slippage_reason'], 'quote_timestamp_stale')

    def test_execution_slippage_requires_exact_source_tables_and_coherent_positive_book(self) -> None:
        valid = _verified_executable_quote_snapshot()
        adversarial_quotes = {
            'extra_source_table': {
                **valid,
                'freshness': {
                    'quote_context': {
                        **valid['freshness']['quote_context'],
                        'source_tables': ['current_quote_cache', 'quote_snapshots', 'daily_prices'],
                    }
                },
            },
            'zero_spread': {
                **valid,
                'bid_price': 40_000,
                'ask_price': 40_000,
                'spread_points': 0,
                'spread_bps': 0,
            },
            'crossed_book': {**valid, 'bid_price': 40_050, 'ask_price': 40_000},
            'spread_points_mismatch': {**valid, 'spread_points': 69},
            'spread_bps_mismatch': {**valid, 'spread_bps': 10},
            'extreme_unbalanced_book': {
                **valid,
                'current_price': '1e-999999',
                'bid_price': 1,
                'ask_price': 2,
                'spread_points': 1,
                'spread_bps': 1,
            },
            'mixed_component_provenance': {
                **valid,
                'spread_component_source_context': {
                    **valid['spread_component_source_context'],
                    'current_price': 'intraday_execution_context',
                },
            },
        }

        for case_name, quote in adversarial_quotes.items():
            with self.subTest(case_name=case_name):
                feasibility = _envelope_for_quote(quote)['execution_feasibility']

                self.assertIsNone(feasibility['expected_slippage_bps'])
                self.assertEqual(feasibility['expected_slippage_status'], 'invalid')
                self.assertNotEqual(feasibility['expected_slippage_reason'], 'current_quote_spread')

    def test_execution_slippage_does_not_invent_numeric_cost_from_liquidity_tier(self) -> None:
        result = build_decision_input_quality_envelope(
            symbol='005930',
            market_date='2026-04-29',
            decider_data_quality_context=_quality_context(),
            pre_decider_enrichment_context={
                'quote_orderbook_snapshot': {'status': 'available'},
                'trading_value': {
                    'status': 'available',
                    'source': 'raw',
                    'raw_trading_value_krw': 8_000_000_000,
                    'trading_value_krw': 8_000_000_000,
                },
            },
        )

        feasibility = result['execution_feasibility']
        self.assertEqual(feasibility['liquidity_tier'], 'medium')
        self.assertIsNone(feasibility['expected_slippage_bps'])
        self.assertEqual(feasibility['expected_slippage_basis'], 'unavailable')
        self.assertEqual(feasibility['expected_slippage_status'], 'missing')
        self.assertEqual(feasibility['expected_slippage_reason'], 'quote_spread_missing')

    def test_execution_slippage_normalizes_nonfinite_and_negative_spreads_to_invalid(self) -> None:
        for spread_bps in (float('nan'), float('inf'), float('-inf'), '1e999999', -1):
            with self.subTest(spread_bps=spread_bps):
                result = build_decision_input_quality_envelope(
                    symbol='005930',
                    market_date='2026-04-29',
                    decider_data_quality_context=_quality_context(),
                    pre_decider_enrichment_context={
                        'quote_orderbook_snapshot': {
                            'status': 'available',
                            'spread_bps': spread_bps,
                        },
                        'trading_value': {
                            'status': 'available',
                            'source': 'raw',
                            'raw_trading_value_krw': 8_000_000_000,
                        },
                    },
                )

                feasibility = result['execution_feasibility']
                self.assertIsNone(feasibility['expected_slippage_bps'])
                self.assertEqual(feasibility['expected_slippage_basis'], 'unavailable')
                self.assertEqual(feasibility['expected_slippage_status'], 'invalid')
                self.assertEqual(
                    feasibility['expected_slippage_reason'],
                    'quote_spread_nonfinite_or_negative',
                )

    def test_completed_eod_spread_is_reference_only_not_execution_cost_evidence(self) -> None:
        result = build_decision_input_quality_envelope(
            symbol='005930',
            market_date='2026-04-29',
            decider_data_quality_context=_quality_context(intraday_status='not_applicable'),
            pre_decider_enrichment_context={
                'market_data_phase_contract': {
                    'schema_version': 'market_data_phase_contract_v1',
                    'recommendation_basis': 'latest_completed_eod',
                    'realtime_quote_orderbook_required': False,
                    'eod_reference_price_accepted': True,
                    'not_applicable_realtime_is_expected': True,
                },
                'quote_orderbook_snapshot': {
                    'status': 'stale_eod',
                    'snapshot_type': 'eod_stale',
                    'quote_freshness_status': 'stale',
                    'recommendation_basis': 'latest_completed_eod',
                    'spread_bps': 17.5,
                },
                'trading_value': {
                    'status': 'available',
                    'source': 'raw',
                    'raw_trading_value_krw': 8_000_000_000,
                },
            },
        )

        feasibility = result['execution_feasibility']
        self.assertIsNone(feasibility['expected_slippage_bps'])
        self.assertEqual(feasibility['expected_slippage_basis'], 'unavailable')
        self.assertEqual(feasibility['expected_slippage_status'], 'unavailable')
        self.assertEqual(feasibility['expected_slippage_reason'], 'quote_spread_not_executable_eod')

    def test_stale_fallback_quote_and_missing_turnover_are_explained_as_blocks_or_penalties(self) -> None:
        result = build_decision_input_quality_envelope(
            symbol='005930',
            market_date='2026-04-29',
            decider_data_quality_context=_quality_context(
                turnover_status='missing_price_or_volume',
                trading_value_source='missing',
                trading_value_krw=None,
                liquidity_status='unavailable',
                intraday_status='stale',
            ),
            pre_decider_enrichment_context={
                'quote_orderbook_snapshot': {
                    'status': 'stale_eod',
                    'snapshot_type': 'eod_stale',
                    'quote_freshness_status': 'stale',
                },
                'trading_value': {
                    'status': 'unavailable',
                    'source': 'missing',
                },
            },
        )

        final = result['final_judgment']
        self.assertEqual(final['decision_band'], 'block')
        self.assertIn('hard-blocked execution-quality inputs', final['recommended_agent_question'])
        self.assertFalse(result['quality_provenance']['data_policy']['paid_api_dependency'])
        self.assertLessEqual(final['buy_quality_score'], 35)
        block_codes = {block['code'] for block in final['hard_blocks']}
        penalty_codes = {penalty['code'] for penalty in final['quality_penalties']}
        self.assertIn('liquidity_turnover_hard_block', block_codes)
        self.assertIn('quote_stale_or_fallback', penalty_codes)
        self.assertIn('source_turnover_missing', block_codes)

    def test_nonpositive_turnover_is_hard_blocked_without_negative_sizing_hints(self) -> None:
        for trading_value_source in ('raw', 'estimated'):
            with self.subTest(trading_value_source=trading_value_source):
                result = build_decision_input_quality_envelope(
                    symbol='005930',
                    market_date='2026-04-29',
                    decider_data_quality_context=_quality_context(
                        trading_value_source=trading_value_source,
                        trading_value_krw=-1_000_000_000,
                    ),
                    pre_decider_enrichment_context={
                        'quote_orderbook_snapshot': {
                            'status': 'available',
                            'spread_bps': 20,
                        },
                        'trading_value': {
                            'status': 'available',
                            'source': trading_value_source,
                            'trading_value_krw': -1_000_000_000,
                            (
                                'raw_trading_value_krw'
                                if trading_value_source == 'raw'
                                else 'estimated_trading_value_krw'
                            ): -1_000_000_000,
                        },
                    },
                    allocation_budget_krw='1000000',
                    estimated_reference_price='50000',
                )

                feasibility = result['execution_feasibility']
                self.assertIsNone(feasibility['max_safe_notional_krw'])
                self.assertEqual(
                    feasibility['sizing_hints'],
                    [
                        {
                            'code': 'max_safe_notional_unavailable',
                            'reason': 'No valid positive trading value was available for sizing hints.',
                        }
                    ],
                )
                hard_block_codes = {
                    block['code'] for block in result['final_judgment']['hard_blocks']
                }
                self.assertIn('source_turnover_invalid', hard_block_codes)
                self.assertEqual(result['final_judgment']['decision_band'], 'block')

    def test_weak_liquidity_adds_safe_notional_sizing_hint(self) -> None:
        result = build_decision_input_quality_envelope(
            symbol='000660',
            market_date='2026-04-29',
            decider_data_quality_context=_quality_context(
                trading_value_source='raw',
                trading_value_krw=250_000_000,
            ),
            pre_decider_enrichment_context={
                'quote_orderbook_snapshot': {'status': 'available', 'spread_bps': 85},
                'trading_value': {
                    'status': 'available',
                    'source': 'raw',
                    'raw_trading_value_krw': 250_000_000,
                    'trading_value_krw': 250_000_000,
                },
            },
            allocation_budget_krw='1500000',
            estimated_reference_price='50000',
            planned_notional_krw='1500000',
        )

        feasibility = result['execution_feasibility']
        self.assertEqual(feasibility['liquidity_tier'], 'very_weak')
        self.assertEqual(feasibility['max_safe_notional_krw'], 1_250_000)
        self.assertTrue(feasibility['sizing_hints'][0]['budget_exceeds_safe_notional'])
        self.assertEqual(feasibility['sizing_hints'][0]['max_safe_quantity'], 25)

    def test_eod_phase_accepts_eod_reference_price_without_live_quote_penalty(self) -> None:
        result = build_decision_input_quality_envelope(
            symbol='005930',
            market_date='2026-04-29',
            decider_data_quality_context=_quality_context(intraday_status='not_applicable'),
            pre_decider_enrichment_context={
                'market_data_phase_contract': {
                    'recommendation_basis': 'latest_completed_eod',
                    'quote_data_expected': False,
                    'intraday_data_expected': False,
                    'realtime_quote_orderbook_required': False,
                    'eod_reference_price_accepted': True,
                    'not_applicable_realtime_is_expected': True,
                },
                'quote_orderbook_snapshot': {
                    'status': 'stale_eod',
                    'snapshot_type': 'eod_stale',
                    'quote_freshness_status': 'stale',
                    'recommendation_basis': 'latest_completed_eod',
                },
                'trading_value': {
                    'status': 'available',
                    'source': 'raw',
                    'raw_trading_value_krw': 8_000_000_000,
                    'trading_value_krw': 8_000_000_000,
                },
            },
        )

        penalty_codes = {penalty['code'] for penalty in result['final_judgment']['quality_penalties']}
        self.assertNotIn('quote_stale_or_fallback', penalty_codes)
        self.assertFalse(result['execution_feasibility']['blocking_reasons'])
        self.assertEqual(
            result['execution_feasibility']['inputs']['market_data_phase_contract']['recommendation_basis'],
            'latest_completed_eod',
        )

    def test_missing_news_is_neutral_secondary_context(self) -> None:
        result = build_decision_input_quality_envelope(
            symbol='005930',
            market_date='2026-04-29',
            decider_data_quality_context=_quality_context(news_status='empty'),
            pre_decider_enrichment_context={
                'quote_orderbook_snapshot': {'status': 'available', 'spread_bps': 15},
                'trading_value': {
                    'status': 'available',
                    'source': 'raw',
                    'raw_trading_value_krw': 8_000_000_000,
                    'trading_value_krw': 8_000_000_000,
                },
            },
        )

        news_block = result['data_quality']['blocks']['disclosures_news']
        self.assertEqual(news_block['decision_effect'], 'neutral_secondary_context')
        self.assertEqual(news_block['confidence_penalty'], 0)
        self.assertNotIn('disclosures_news', result['final_judgment']['missing_context'])
        self.assertEqual(
            result['final_judgment']['secondary_context'][0]['decision_effect'],
            'neutral_secondary_context',
        )
        self.assertEqual(result['quality_provenance']['data_policy']['source_policy'], 'free_current_sources_only')


if __name__ == '__main__':
    unittest.main()
