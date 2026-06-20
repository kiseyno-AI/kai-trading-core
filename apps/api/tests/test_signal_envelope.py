from __future__ import annotations

from datetime import date
import unittest

from app.agent_context.signal_envelope import (
    SIGNAL_ENVELOPE_SCHEMA_VERSION,
    build_decider_signal_envelopes,
    build_news_signal_envelope,
    build_symbol_signal_envelopes,
)


class SignalEnvelopeTests(unittest.TestCase):
    def test_positive_technical_and_factor_signals_normalize_to_positive_values(self) -> None:
        envelopes = build_decider_signal_envelopes(
            symbol='005930',
            market_date=date(2026, 4, 14),
            factor_summary={
                'requested_market_date': '2026-04-14',
                'factor_scores': {
                    'momentum': {
                        'status': 'available',
                        'source_status': 'scored',
                        'score': 0.91,
                        'components_used': 3,
                        'components_possible': 3,
                        'as_of_market_date': '2026-04-14',
                    }
                },
            },
            upstream_summaries={
                'technical-summarizer': {
                    'preferred_action': 'buy',
                    'confidence_score_10': 8,
                    'conviction_score_10': 7,
                    'summary': 'Bullish breakout with constructive volume confirmation.',
                    'supporting_evidence': ['20-day breakout', 'Volume expanded'],
                }
            },
            upstream_traceability={
                'technical-summarizer': {
                    'feedback_id': 77,
                    'run_id': 'tech-run-77',
                    'source': 'persisted_feedback',
                }
            },
        )

        by_source = {item['source_name']: item for item in envelopes}
        technical = by_source['technical-summarizer']
        momentum = by_source['factor:momentum']

        self.assertEqual(technical['schema_version'], SIGNAL_ENVELOPE_SCHEMA_VERSION)
        self.assertEqual(technical['source_type'], 'agent')
        self.assertGreater(technical['value'], 0)
        self.assertEqual(technical['confidence'], 0.75)
        self.assertEqual(technical['evidence_count'], 2)
        self.assertEqual(technical['raw_ref']['feedback_id'], 77)
        self.assertGreater(momentum['value'], 0.8)
        self.assertEqual(momentum['confidence'], 1.0)
        self.assertEqual(momentum['raw_ref']['path'], 'factor_summary.factor_scores.momentum')

    def test_negative_risk_news_signal_normalizes_to_negative_value(self) -> None:
        envelope = build_news_signal_envelope(
            symbol='005930',
            market_date='2026-04-14',
            recent_external_news={
                'status': 'available',
                'classification_summary': {'max_materiality_score': 0.82},
                'material_items': [
                    {
                        'title': 'Samsung faces recall risk',
                        'sentiment': 'negative',
                        'materiality': 'material',
                        'materiality_score': 0.82,
                        'event_type': 'recall',
                        'access_status': 'headline_only',
                        'url': 'https://example.test/news/1',
                    }
                ],
            },
        )

        self.assertEqual(envelope['source_name'], 'news:external')
        self.assertEqual(envelope['source_type'], 'news')
        self.assertLess(envelope['value'], 0)
        self.assertEqual(envelope['confidence'], 0.82)
        self.assertIn('material_news', envelope['risk_flags'])
        self.assertIn('negative_news_sentiment', envelope['risk_flags'])
        self.assertIn('headline_or_limited_text', envelope['risk_flags'])
        self.assertEqual(envelope['evidence_count'], 1)

    def test_missing_low_quality_sources_are_explicit_low_confidence_envelopes(self) -> None:
        envelopes = build_symbol_signal_envelopes(
            symbol='000660',
            market_date='2026-04-14',
            factor_summary={
                'requested_market_date': '2026-04-14',
                'factor_scores': {
                    'value': {'status': 'missing_source_row'},
                },
                'signal_features': {
                    'status': 'missing_source_row',
                },
            },
            recent_external_news={
                'status': 'empty',
                'reason': 'no_recent_external_news_found',
                'items': [],
            },
        )

        by_source = {item['source_name']: item for item in envelopes}
        value = by_source['factor:value']
        kis_ranking = by_source['factor:kis_ranking']
        news = by_source['news:external']

        self.assertEqual(value['value'], 0.0)
        self.assertEqual(value['confidence'], 0.0)
        self.assertIn('missing_source_row', value['risk_flags'])
        self.assertIn('low_quality_or_missing_source', value['risk_flags'])
        self.assertEqual(kis_ranking['confidence'], 0.0)
        self.assertEqual(kis_ranking['evidence_refs'], [])
        self.assertEqual(news['value'], 0.0)
        self.assertEqual(news['confidence'], 0.1)
        self.assertIn('no_recent_external_news_found', news['risk_flags'])

    def test_multiple_sources_for_one_symbol_date_are_kept_compact_and_distinct(self) -> None:
        envelopes = build_decider_signal_envelopes(
            symbol='005930',
            market_date='2026-04-14',
            factor_summary={
                'requested_market_date': '2026-04-14',
                'factor_scores': {
                    'quality': {'status': 'available', 'score': 0.74, 'components_used': 5, 'components_possible': 6},
                    'low_risk': {'status': 'not_eligible', 'source_status': 'watchlist'},
                },
                'signal_features': {'status': 'available', 'signal_count': 2, 'row_kind': 'direct_plus_fallback'},
            },
            upstream_summaries={
                'technical-summarizer': {'preferred_action': 'buy', 'supporting_evidence': ['breakout']},
                'risk:market': {'summary': 'Bearish risk from broad market deterioration.', 'risk_flags': ['market_risk']},
            },
            upstream_traceability={},
        )

        sources = [item['source_name'] for item in envelopes]

        self.assertEqual(len(sources), len(set(sources)))
        self.assertIn('technical-summarizer', sources)
        self.assertIn('risk:market', sources)
        self.assertIn('factor:quality', sources)
        self.assertIn('factor:low_risk', sources)
        self.assertIn('factor:kis_ranking', sources)
        self.assertTrue(all(item['symbol'] == '005930' for item in envelopes))
        self.assertTrue(all(item['market_date'] == '2026-04-14' for item in envelopes))
        self.assertLess(next(item for item in envelopes if item['source_name'] == 'risk:market')['value'], 0)


if __name__ == '__main__':
    unittest.main()
