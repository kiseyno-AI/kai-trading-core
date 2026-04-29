import json
import unittest

from app.agent_context.response_contract import build_contract_prompt_suffix, normalize_agent_response


class AgentContextResponseContractTests(unittest.TestCase):
    def test_signal_decider_contract_makes_exact_symbol_and_self_check_mandatory(self):
        suffix = build_contract_prompt_suffix(role='signal-decider')

        self.assertIn('symbol is mandatory', suffix)
        self.assertIn('exactly equal the assigned/requested symbol string', suffix)
        self.assertIn('traceability.symbol must exactly match symbol', suffix)
        self.assertIn('wait or reject', suffix)
        self.assertIn('self-check', suffix)
        self.assertIn('JSON is parseable', suffix)
        self.assertIn('Immediately before sending, validate the final raw JSON text again', suffix)
        self.assertIn('Operator-facing free-text fields must be written in Korean', suffix)
        self.assertIn(
            'Do not translate or localize machine-readable enum fields such as preferred_action, next_day_open_action, or entry_price_basis.',
            suffix,
        )
        self.assertIn('next_day_open_action and preferred_action must be one of buy, wait, reject exactly as written.', suffix)

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

    def test_normalize_signal_decider_response_keeps_action_tokens_machine_readable(self):
        output_text = json.dumps(
            {
                'role': 'signal-decider',
                'agent_name': 'signal-decider',
                'symbol': '005930',
                'market_date': '2026-04-23',
                'preferred_action': 'buy',
                'next_day_open_action': 'buy',
                'entry_price_basis': 'next_open',
                'thesis_summary': '다음 거래일 시가 대응은 매수 우위로 판단됩니다.',
                'supporting_evidence': ['단기 추세와 수급 흐름이 동시에 개선됐습니다.'],
                'decision_risks': ['장 초반 변동성이 다시 확대될 수 있습니다.'],
                'invalidation_conditions': ['시가 직후 지지선 이탈이 확인되면 전제가 훼손됩니다.'],
                'data_quality_warnings': ['일부 보조 신호는 장 마감 기준으로만 확인됐습니다.'],
                'specialist_disagreement': ['펀더멘털 해석은 중립에 가까웠습니다.'],
                'improvement_notes': ['장전 갭 방향 데이터가 추가되면 더 엄밀해집니다.'],
            }
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=output_text,
            raw_response={'id': 'resp-3'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertEqual(result['preferred_action'], 'buy')
        self.assertEqual(result['next_day_open_action'], 'buy')
        self.assertEqual(result['entry_price_basis'], 'next_open')
        self.assertEqual(result['thesis_summary'], '다음 거래일 시가 대응은 매수 우위로 판단됩니다.')


if __name__ == '__main__':
    unittest.main()
