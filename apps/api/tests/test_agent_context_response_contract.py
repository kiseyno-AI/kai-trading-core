import json
import unittest

from app.agent_context.response_contract import normalize_agent_response


class AgentContextResponseContractTests(unittest.TestCase):
    def test_normalize_signal_decider_response_preserves_decision_support_fields_and_traceability(self):
        output_text = json.dumps(
            {
                'role': 'signal-decider',
                'agent_name': 'signal-decider',
                'symbol': '005930',
                'market_date': '2026-04-23',
                'preferred_action': 'buy',
                'next_day_open_action': 'buy',
                'entry_price_basis': 'next_open',
                'target_price': 91200,
                'expected_return_pct': 6.4,
                'stop_loss_price': 82900,
                'suggested_holding_period': '3-10 trading days',
                'confidence_score_10': 7,
                'conviction_score_10': 6,
                'thesis_summary': 'Momentum and fundamentals align for a tactical long setup.',
                'supporting_evidence': ['Breadth improved into the close.', 'Breakout level held above support.'],
                'decision_risks': ['Leadership is still narrow.'],
                'invalidation_conditions': ['Gap below 82900 at the next open.'],
                'data_quality_warnings': ['Signal-feature coverage was partial.'],
                'specialist_disagreement': ['Fundamental view was constructive but not emphatic.'],
                'improvement_notes': ['Add premarket gap context.'],
            }
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=output_text,
            raw_response={'id': 'resp-1'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertEqual(result['next_day_open_action'], 'buy')
        self.assertEqual(result['preferred_action'], 'buy')
        self.assertEqual(result['entry_price_basis'], 'next_open')
        self.assertEqual(result['target_price'], 91200.0)
        self.assertEqual(result['expected_return_pct'], 6.4)
        self.assertEqual(result['stop_loss_price'], 82900.0)
        self.assertEqual(result['confidence_score_10'], 7.0)
        self.assertEqual(result['conviction_score_10'], 6.0)
        self.assertEqual(result['confidence'], 0.7)
        self.assertEqual(result['summary'], result['thesis_summary'])
        self.assertEqual(result['traceability']['agent_name'], 'signal-decider')
        self.assertEqual(result['traceability']['symbol'], '005930')
        self.assertEqual(result['traceability']['market_date'], '2026-04-23')
        self.assertIn('Signal-feature coverage was partial.', result['risk_flags'])
        self.assertEqual(
            result['details']['decision_support']['supporting_evidence'],
            ['Breadth improved into the close.', 'Breakout level held above support.'],
        )

    def test_normalize_signal_decider_response_falls_back_conservatively_for_unstructured_output(self):
        result = normalize_agent_response(
            role='signal-decider',
            output_text='buy this immediately',
            raw_response={'id': 'resp-2'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertEqual(result['next_day_open_action'], 'wait')
        self.assertEqual(result['preferred_action'], 'wait')
        self.assertIn('unstructured_response', result['risk_flags'])
        self.assertEqual(result['data_quality_warnings'], ['unstructured_response'])
        self.assertEqual(result['traceability']['symbol'], '005930')
        self.assertEqual(result['traceability']['market_date'], '2026-04-23')
        self.assertIn('Return the full structured decision-support contract in JSON.', result['improvement_notes'])


if __name__ == '__main__':
    unittest.main()
