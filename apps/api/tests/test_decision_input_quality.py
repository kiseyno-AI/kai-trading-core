import unittest

from app.agent_context.decision_input_quality import build_decision_input_quality_envelope


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


class DecisionInputQualityTests(unittest.TestCase):
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
        self.assertEqual(final['decision_band'], 'blocked')
        self.assertLessEqual(final['buy_quality_score'], 35)
        block_codes = {block['code'] for block in final['hard_blocks']}
        penalty_codes = {penalty['code'] for penalty in final['quality_penalties']}
        self.assertIn('liquidity_turnover_hard_block', block_codes)
        self.assertIn('quote_stale_or_fallback', penalty_codes)
        self.assertIn('source_turnover_missing', block_codes)

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


if __name__ == '__main__':
    unittest.main()
