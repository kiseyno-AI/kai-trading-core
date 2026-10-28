import json
from pathlib import Path
import unittest

from app.agent_context.logging import build_normalized_feedback_snapshot
from app.agent_context.response_contract import (
    AGENT_ROLE_CONTRACT_VERSION,
    build_agent_contract_provenance,
    build_contract_prompt_suffix,
    normalize_agent_response,
)


def _complete_signal_decider_feedback(**overrides):
    payload = {
        'role': 'signal-decider',
        'agent_name': 'signal-decider',
        'symbol': '005930',
        'market_date': '2026-04-23',
        'preferred_action': 'buy',
        'next_day_open_action': 'buy',
        'wait_reason': None,
        'entry_price_basis': 'next_open',
        'trigger_price': None,
        'chase_limit_price': None,
        'target_price': 91200,
        'expected_return_pct': 6.4,
        'stop_loss_price': 82900,
        'suggested_holding_period': '3-10 trading days',
        'confidence_score_10': 7,
        'conviction_score_10': 6,
        'thesis_summary': '모멘텀과 실행 여건이 매수 후보를 지지합니다.',
        'trigger_conditions': [],
        'supporting_evidence': ['상대강도와 거래량이 함께 개선됐습니다.'],
        'decision_risks': ['장 초반 변동성이 확대될 수 있습니다.'],
        'decision_delta': {},
        'invalidation_conditions': ['손절 가격 이탈 시 전제가 훼손됩니다.'],
        'data_quality_warnings': [],
        'specialist_disagreement': [],
        'traceability': {'symbol': '005930'},
        'summary': '모멘텀과 실행 여건이 매수 후보를 지지합니다.',
        'confidence': 0.7,
        'risk_flags': [],
        'details': {'decision_support': {'decision_support_only': True}},
        'improvement_notes': [],
    }
    payload.update(overrides)
    return payload


def _complete_short_term_decider_feedback(**overrides):
    payload = {
        'role': 'short-term-decider',
        'agent_name': 'short-term-decider',
        'symbol': '005930',
        'market_date': '2026-04-23',
        'tactical_action': 'scalp_buy_candidate',
        'preferred_action': 'scalp_buy_candidate',
        'wait_reason': None,
        'trigger_price': 85000,
        'chase_limit_price': 86000,
        'target_price': 89000,
        'stop_loss_price': 83000,
        'expected_holding_period': '1-3 trading days',
        'confidence_score_10': 7,
        'conviction_score_10': 6,
        'thesis_summary': '거래량을 동반한 돌파 후보입니다.',
        'trigger_conditions': ['85000원 돌파와 거래량 증가가 함께 확인됩니다.'],
        'supporting_evidence': ['시장 대비 상대강도가 개선됐습니다.'],
        'tactical_risks': ['갭 상승 시 손익비가 악화될 수 있습니다.'],
        'decision_delta': {},
        'invalidation_conditions': ['83000원 이탈 시 전제가 훼손됩니다.'],
        'data_quality_warnings': [],
        'traceability': {'symbol': '005930'},
        'summary': '거래량을 동반한 돌파 후보입니다.',
        'confidence': 0.7,
        'risk_flags': [],
        'details': {'tactical_decision_support': {'decision_support_only': True}},
        'improvement_notes': [],
    }
    payload.update(overrides)
    return payload


class AgentContextResponseContractTests(unittest.TestCase):
    def test_signal_wait_preserves_monitoring_levels_and_synthesizes_buy_upgrade(self):
        payload = _complete_signal_decider_feedback(
            preferred_action='wait',
            next_day_open_action='wait',
            wait_reason={
                'code': 'extended_entry_price',
                'summary': '71,000원 회복 확인이 필요합니다.',
            },
            entry_price_basis='trigger_limit',
            trigger_price=71_000,
            chase_limit_price=71_500,
            target_price=74_000,
            stop_loss_price=69_500,
            expected_return_pct=None,
            trigger_conditions=['71,000원 회복과 거래량 회복을 확인합니다.'],
            decision_delta={},
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(payload, ensure_ascii=False),
            raw_response=payload,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertTrue(result['response_contract_valid'])
        self.assertEqual(result['trigger_price'], 71_000.0)
        self.assertEqual(result['chase_limit_price'], 71_500.0)
        self.assertEqual(
            result['trigger_conditions'],
            ['71,000원 회복과 거래량 회복을 확인합니다.'],
        )
        self.assertEqual(
            result['decision_delta']['upgrade_trigger'],
            {
                'target_action': 'buy',
                'summary': '71,000원 회복과 거래량 회복을 확인합니다.',
                'conditions': ['71,000원 회복과 거래량 회복을 확인합니다.'],
                'trigger_price': 71_000,
                'chase_limit_price': 71_500,
            },
        )

    def test_deciders_reject_noncanonical_batch_envelopes_in_single_response_normalization(self):
        cases = (
            (
                'signal-decider',
                {'items': [{'symbol': '005930', 'feedback': _complete_signal_decider_feedback()}]},
                'next_day_open_action',
            ),
            (
                'short-term-decider',
                {
                    'batch': {
                        'items': [
                            {
                                'symbol': '005930',
                                'feedback': _complete_short_term_decider_feedback(),
                            }
                        ]
                    }
                },
                'tactical_action',
            ),
        )

        for role, payload, action_field in cases:
            with self.subTest(role=role):
                result = normalize_agent_response(
                    role=role,
                    output_text=json.dumps(payload, ensure_ascii=False),
                    raw_response=payload,
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name=role,
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertIn(
                    'response_structure_mismatch:batch_envelope',
                    result['response_schema_violation_flags'],
                )
                self.assertEqual(result[action_field], 'reject')

    def test_structured_specialist_response_rejects_missing_mandatory_role(self):
        output_text = json.dumps(
            {
                'summary': '기술적 돌파 구조가 유지되고 있습니다.',
                'confidence': 0.7,
                'risk_flags': [],
                'details': {'support': 70000},
                'improvement_notes': [],
            }
        )

        result = normalize_agent_response(
            role='technical-summarizer',
            output_text=output_text,
            raw_response={'id': 'resp-specialist-role-missing'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='technical-summarizer',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['role'], 'technical-summarizer')
        self.assertEqual(result['structural_mismatch_flags'], ['role_missing'])
        self.assertEqual(result['response_schema_violation_flags'], ['required_field_missing:role'])

    def test_unstructured_specialist_fallback_is_never_contract_valid(self):
        result = normalize_agent_response(
            role='technical-summarizer',
            output_text='돌파 구조는 유지 중입니다.',
            raw_response={'id': 'resp-specialist-unstructured'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='technical-summarizer',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertIn('unstructured_response', result['risk_flags'])
        self.assertEqual(result['response_schema_violation_flags'], ['unstructured_response'])

    def test_core_generated_structured_fallback_is_never_contract_valid(self):
        cases = (
            (
                'technical-summarizer',
                {
                    'role': 'technical-summarizer',
                    'summary': 'Core fallback',
                    'confidence': 0.0,
                    'risk_flags': ['agent_json_contract_violation'],
                    'details': {'fallback_generated_by_core': True},
                    'improvement_notes': [],
                },
            ),
            (
                'signal-decider',
                _complete_signal_decider_feedback(
                    preferred_action='wait',
                    next_day_open_action='wait',
                    wait_reason={
                        'code': 'core_execution_evidence_gap',
                        'summary': '구조화 응답을 신뢰할 수 없습니다.',
                    },
                    target_price=None,
                    expected_return_pct=None,
                    stop_loss_price=None,
                    suggested_holding_period=None,
                    details={'fallback_generated_by_core': True},
                ),
            ),
            (
                'short-term-decider',
                _complete_short_term_decider_feedback(
                    tactical_action='wait',
                    preferred_action='wait',
                    wait_reason={
                        'code': 'core_execution_evidence_gap',
                        'summary': '구조화 응답을 신뢰할 수 없습니다.',
                    },
                    trigger_price=None,
                    chase_limit_price=None,
                    target_price=None,
                    stop_loss_price=None,
                    expected_holding_period=None,
                    details={'fallback_generated_by_core': True},
                ),
            ),
        )

        for role, payload in cases:
            with self.subTest(role=role):
                result = normalize_agent_response(
                    role=role,
                    output_text=json.dumps(payload, ensure_ascii=False),
                    raw_response=payload,
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name=role,
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertIn('core_generated_fallback', result['response_schema_violation_flags'])
                if role == 'signal-decider':
                    self.assertEqual(result['next_day_open_action'], 'reject')
                elif role == 'short-term-decider':
                    self.assertEqual(result['tactical_action'], 'reject')

    def test_structured_specialist_response_requires_every_contract_field(self):
        result = normalize_agent_response(
            role='technical-summarizer',
            output_text=json.dumps({'role': 'technical-summarizer'}),
            raw_response={'id': 'resp-specialist-incomplete'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='technical-summarizer',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(
            result['response_schema_violation_flags'],
            [
                'required_field_missing:summary',
                'required_field_missing:confidence',
                'required_field_missing:risk_flags',
                'required_field_missing:details',
                'required_field_missing:improvement_notes',
            ],
        )

    def test_structured_specialist_response_rejects_invalid_field_types_and_values(self):
        output_text = json.dumps(
            {
                'role': 'technical-summarizer',
                'summary': '',
                'confidence': 1.5,
                'risk_flags': ['valid', 7],
                'details': [],
                'improvement_notes': 'none',
            }
        )

        result = normalize_agent_response(
            role='technical-summarizer',
            output_text=output_text,
            raw_response={'id': 'resp-specialist-invalid-types'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='technical-summarizer',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(
            result['response_schema_violation_flags'],
            [
                'required_field_invalid_value:summary',
                'required_field_invalid_value:confidence',
                'required_field_invalid_type:risk_flags',
                'required_field_invalid_type:details',
                'required_field_invalid_type:improvement_notes',
            ],
        )

    def test_structured_specialist_non_finite_confidence_is_rejected_and_normalized_to_none(self):
        for value in (float('nan'), float('inf'), float('-inf')):
            with self.subTest(value=value):
                payload = {
                    'role': 'technical-summarizer',
                    'summary': '기술적 구조를 요약합니다.',
                    'confidence': value,
                    'risk_flags': [],
                    'details': {},
                    'improvement_notes': [],
                }

                result = normalize_agent_response(
                    role='technical-summarizer',
                    output_text=json.dumps(payload),
                    raw_response=payload,
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name='technical-summarizer',
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertIn(
                    'required_field_invalid_value:confidence',
                    result['response_schema_violation_flags'],
                )
                self.assertIsNone(result['confidence'])

    def test_nested_non_finite_numbers_are_rejected_and_do_not_escape_normalized_or_audit_payloads(self):
        payload = _complete_signal_decider_feedback(
            traceability={'symbol': '005930', 'nested_score': float('inf')},
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(payload),
            raw_response=payload,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['preferred_action'], 'reject')
        self.assertIn(
            'non_finite_number:traceability.nested_score',
            result['response_schema_violation_flags'],
        )
        self.assertIsNone(result['traceability']['nested_score'])
        json.dumps(result, allow_nan=False)
        json.dumps(build_normalized_feedback_snapshot(result), allow_nan=False)

    def test_structured_specialist_response_accepts_complete_typed_contract(self):
        output_text = json.dumps(
            {
                'role': 'technical-summarizer',
                'summary': '기술적 돌파 구조가 유지되고 있습니다.',
                'confidence': 0.7,
                'risk_flags': [],
                'details': {'support': 70000},
                'improvement_notes': [],
            }
        )

        result = normalize_agent_response(
            role='technical-summarizer',
            output_text=output_text,
            raw_response={'id': 'resp-specialist-valid'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='technical-summarizer',
        )

        self.assertTrue(result['response_contract_valid'])
        self.assertEqual(result['response_schema_violation_flags'], [])

    def test_signal_decider_rejects_incomplete_actionable_schema(self):
        output_text = json.dumps(
            {
                'role': 'signal-decider',
                'agent_name': 'signal-decider',
                'symbol': '005930',
                'market_date': '2026-04-23',
                'traceability': {'symbol': '005930'},
                'preferred_action': 'buy',
                'next_day_open_action': 'buy',
                'thesis_summary': '필수 가격과 근거가 없는 매수 응답입니다.',
            }
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=output_text,
            raw_response={'id': 'resp-signal-incomplete'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['next_day_open_action'], 'reject')
        self.assertIn('required_field_missing:target_price', result['response_schema_violation_flags'])
        self.assertIn('required_field_missing:supporting_evidence', result['response_schema_violation_flags'])
        self.assertIn('required_field_missing:details', result['response_schema_violation_flags'])

    def test_short_term_decider_rejects_incomplete_actionable_schema(self):
        output_text = json.dumps(
            {
                'role': 'short-term-decider',
                'agent_name': 'short-term-decider',
                'symbol': '005930',
                'market_date': '2026-04-23',
                'traceability': {'symbol': '005930'},
                'tactical_action': 'scalp_buy_candidate',
                'preferred_action': 'scalp_buy_candidate',
                'thesis_summary': '필수 가격과 근거가 없는 단기 매수 응답입니다.',
            }
        )

        result = normalize_agent_response(
            role='short-term-decider',
            output_text=output_text,
            raw_response={'id': 'resp-short-incomplete'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='short-term-decider',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['tactical_action'], 'reject')
        self.assertIn('required_field_missing:trigger_price', result['response_schema_violation_flags'])
        self.assertIn('required_field_missing:trigger_conditions', result['response_schema_violation_flags'])
        self.assertIn('required_field_missing:details', result['response_schema_violation_flags'])

    def test_decider_actionable_candidates_require_positive_coherent_levels_and_evidence(self):
        cases = (
            (
                'signal-decider',
                _complete_signal_decider_feedback(
                    target_price=82000,
                    stop_loss_price=83000,
                    supporting_evidence=[],
                    invalidation_conditions=[],
                ),
                {'action_requirement_invalid:price_order', 'action_requirement_empty:supporting_evidence'},
            ),
            (
                'short-term-decider',
                _complete_short_term_decider_feedback(
                    tactical_action='scalp_buy_candidate',
                    preferred_action='scalp_buy_candidate',
                    trigger_price=85000,
                    chase_limit_price=84000,
                    target_price=83000,
                    stop_loss_price=86000,
                    trigger_conditions=[],
                ),
                {'action_requirement_invalid:price_order', 'action_requirement_empty:trigger_conditions'},
            ),
            (
                'short-term-decider',
                _complete_short_term_decider_feedback(
                    tactical_action='watch_for_trigger',
                    preferred_action='watch_for_trigger',
                    wait_reason={
                        'code': 'unsuitable_execution_timing',
                        'summary': '돌파 확인 전입니다.',
                    },
                    target_price=None,
                ),
                {'action_requirement_invalid:target_price'},
            ),
        )

        for role, payload, expected_flags in cases:
            with self.subTest(role=role, action=payload['preferred_action']):
                result = normalize_agent_response(
                    role=role,
                    output_text=json.dumps(payload),
                    raw_response={'id': f"resp-action-safety-{role}-{payload['preferred_action']}"},
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name=role,
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertEqual(result['preferred_action'], 'reject')
                self.assertTrue(expected_flags.issubset(set(result['response_schema_violation_flags'])))

    def test_decider_numeric_contract_rejects_non_finite_values(self):
        cases = (
            (
                'signal-decider',
                _complete_signal_decider_feedback(expected_return_pct=float('nan')),
                'required_field_invalid_value:expected_return_pct',
            ),
            (
                'short-term-decider',
                _complete_short_term_decider_feedback(target_price=float('inf')),
                'required_field_invalid_value:target_price',
            ),
        )

        for role, payload, expected_flag in cases:
            with self.subTest(role=role):
                result = normalize_agent_response(
                    role=role,
                    output_text=json.dumps(payload),
                    raw_response={'id': f'resp-non-finite-{role}'},
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name=role,
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertEqual(result['preferred_action'], 'reject')
                self.assertIn(expected_flag, result['response_schema_violation_flags'])

    def test_numeric_contract_rejects_huge_integer_and_decimal_string_without_raising(self):
        huge_integer = 10**400
        cases = (
            (
                _complete_signal_decider_feedback(expected_return_pct=huge_integer),
                'expected_return_pct',
                'required_field_invalid_value:expected_return_pct',
            ),
            (
                _complete_signal_decider_feedback(target_price='9' * 1000),
                'target_price',
                'required_field_invalid_type:target_price',
            ),
        )

        for payload, field, expected_flag in cases:
            with self.subTest(field=field):
                result = normalize_agent_response(
                    role='signal-decider',
                    output_text=json.dumps(payload),
                    raw_response={'id': f'resp-extreme-number-{field}'},
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name='signal-decider',
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertEqual(result['preferred_action'], 'reject')
                self.assertIsNone(result[field])
                self.assertIn(expected_flag, result['response_schema_violation_flags'])

    def test_wait_and_reject_decisions_allow_declared_nullable_numeric_fields(self):
        signal_wait = _complete_signal_decider_feedback(
            preferred_action='wait',
            next_day_open_action='wait',
            wait_reason={
                'code': 'stale_quote_orderbook',
                'summary': '호가 신선도가 부족합니다.',
            },
            target_price=None,
            expected_return_pct=None,
            stop_loss_price=None,
            confidence_score_10=None,
            conviction_score_10=None,
            confidence=None,
        )
        short_reject = _complete_short_term_decider_feedback(
            tactical_action='reject',
            preferred_action='reject',
            trigger_price=None,
            chase_limit_price=None,
            target_price=None,
            stop_loss_price=None,
            confidence_score_10=None,
            conviction_score_10=None,
            confidence=None,
        )

        for role, payload in (
            ('signal-decider', signal_wait),
            ('short-term-decider', short_reject),
        ):
            with self.subTest(role=role):
                result = normalize_agent_response(
                    role=role,
                    output_text=json.dumps(payload),
                    raw_response={'id': f'resp-nullable-{role}'},
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name=role,
                )

                self.assertTrue(result['response_contract_valid'])
                self.assertEqual(result['response_schema_violation_flags'], [])

    def test_signal_wait_distinguishes_non_monitorable_null_levels_from_incomplete_trigger_contract(self):
        non_monitorable_wait = _complete_signal_decider_feedback(
            preferred_action='wait',
            next_day_open_action='wait',
            wait_reason={
                'code': 'unattractive_risk_reward',
                'summary': '확인된 저항까지의 보상이 하방 위험보다 작습니다.',
            },
            entry_price_basis='next_open',
            trigger_price=None,
            chase_limit_price=None,
            target_price=89_000,
            expected_return_pct=None,
            stop_loss_price=83_000,
            trigger_conditions=[],
        )

        valid_result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(non_monitorable_wait, ensure_ascii=False),
            raw_response=non_monitorable_wait,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertTrue(valid_result['response_contract_valid'])
        self.assertEqual(valid_result['response_schema_violation_flags'], [])
        self.assertIsNone(valid_result['trigger_price'])
        self.assertIsNone(valid_result['chase_limit_price'])

        incomplete_monitorable_wait = {
            **non_monitorable_wait,
            'entry_price_basis': 'trigger_limit',
        }
        invalid_result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(incomplete_monitorable_wait, ensure_ascii=False),
            raw_response=incomplete_monitorable_wait,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(invalid_result['response_contract_valid'])
        self.assertIn(
            'action_requirement_invalid:trigger_price',
            invalid_result['response_schema_violation_flags'],
        )
        self.assertIn(
            'action_requirement_invalid:chase_limit_price',
            invalid_result['response_schema_violation_flags'],
        )

        unsupported_basis_wait = {
            **non_monitorable_wait,
            'entry_price_basis': 'bogus_basis',
        }
        unsupported_basis_result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(unsupported_basis_wait, ensure_ascii=False),
            raw_response=unsupported_basis_wait,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(unsupported_basis_result['response_contract_valid'])
        self.assertIn(
            'required_field_invalid_value:entry_price_basis',
            unsupported_basis_result['response_schema_violation_flags'],
        )

        uppercase_basis_wait = {
            **non_monitorable_wait,
            'entry_price_basis': 'NEXT_OPEN',
        }
        uppercase_basis_result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(uppercase_basis_wait, ensure_ascii=False),
            raw_response=uppercase_basis_wait,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(uppercase_basis_result['response_contract_valid'])
        self.assertIn(
            'required_field_invalid_value:entry_price_basis',
            uppercase_basis_result['response_schema_violation_flags'],
        )

        padded_basis_wait = {
            **non_monitorable_wait,
            'entry_price_basis': ' next_open ',
        }
        padded_basis_result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(padded_basis_wait, ensure_ascii=False),
            raw_response=padded_basis_wait,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(padded_basis_result['response_contract_valid'])
        self.assertIn(
            'required_field_invalid_value:entry_price_basis',
            padded_basis_result['response_schema_violation_flags'],
        )

        mismatched_basis_wait = {
            **non_monitorable_wait,
            'trigger_price': 85_000,
            'chase_limit_price': 86_000,
            'target_price': 90_000,
            'stop_loss_price': 83_000,
            'trigger_conditions': ['85,000원 회복을 확인합니다.'],
        }
        mismatched_basis_result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(mismatched_basis_wait, ensure_ascii=False),
            raw_response=mismatched_basis_wait,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(mismatched_basis_result['response_contract_valid'])
        self.assertIn(
            'action_requirement_invalid:entry_price_basis',
            mismatched_basis_result['response_schema_violation_flags'],
        )

        reject_with_levels = {
            **mismatched_basis_wait,
            'preferred_action': 'reject',
            'next_day_open_action': 'reject',
            'wait_reason': None,
        }
        reject_with_levels_result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(reject_with_levels, ensure_ascii=False),
            raw_response=reject_with_levels,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )
        self.assertFalse(reject_with_levels_result['response_contract_valid'])
        self.assertIn(
            'action_requirement_invalid:entry_price_basis',
            reject_with_levels_result['response_schema_violation_flags'],
        )

        reject_with_empty_trigger_limit = {
            **non_monitorable_wait,
            'preferred_action': 'reject',
            'next_day_open_action': 'reject',
            'wait_reason': None,
            'entry_price_basis': 'trigger_limit',
        }
        reject_with_empty_trigger_limit_result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(
                reject_with_empty_trigger_limit,
                ensure_ascii=False,
            ),
            raw_response=reject_with_empty_trigger_limit,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )
        self.assertFalse(
            reject_with_empty_trigger_limit_result[
                'response_contract_valid'
            ]
        )
        self.assertIn(
            'action_requirement_invalid:entry_price_basis',
            reject_with_empty_trigger_limit_result[
                'response_schema_violation_flags'
            ],
        )

    def test_signal_trigger_limit_buy_rejects_return_unsupported_by_chase_limit(self):
        coherent_buy = _complete_signal_decider_feedback(
            entry_price_basis='trigger_limit',
            trigger_price=100,
            chase_limit_price=110,
            target_price=120,
            stop_loss_price=90,
            expected_return_pct=9.0,
            trigger_conditions=['100원 회복과 거래량 증가를 확인합니다.'],
        )

        coherent_result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(coherent_buy, ensure_ascii=False),
            raw_response=coherent_buy,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertTrue(coherent_result['response_contract_valid'])
        self.assertEqual(coherent_result['response_schema_violation_flags'], [])

        contradictory_buy = {
            **coherent_buy,
            'expected_return_pct': 99,
        }
        contradictory_result = normalize_agent_response(
            role='signal-decider',
            output_text=json.dumps(contradictory_buy, ensure_ascii=False),
            raw_response=contradictory_buy,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(contradictory_result['response_contract_valid'])
        self.assertIn(
            'action_requirement_invalid:expected_return_pct',
            contradictory_result['response_schema_violation_flags'],
        )

    def test_decider_wait_actions_reject_unknown_wait_reason_codes(self):
        cases = (
            (
                'signal-decider',
                _complete_signal_decider_feedback(
                    preferred_action='wait',
                    next_day_open_action='wait',
                    wait_reason={
                        'code': 'invented_wait_reason',
                        'summary': '구체적인 확인 조건이 충족되지 않았습니다.',
                    },
                    target_price=None,
                    expected_return_pct=None,
                    stop_loss_price=None,
                ),
            ),
            (
                'short-term-decider',
                _complete_short_term_decider_feedback(
                    tactical_action='wait',
                    preferred_action='wait',
                    wait_reason={
                        'code': 'invented_wait_reason',
                        'summary': '구체적인 확인 조건이 충족되지 않았습니다.',
                    },
                    trigger_price=None,
                    chase_limit_price=None,
                    target_price=None,
                    stop_loss_price=None,
                ),
            ),
        )

        for role, payload in cases:
            with self.subTest(role=role):
                result = normalize_agent_response(
                    role=role,
                    output_text=json.dumps(payload),
                    raw_response={'id': f'resp-unknown-wait-code-{role}'},
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name=role,
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertEqual(result['preferred_action'], 'reject')
                self.assertIn(
                    'required_field_invalid_value:wait_reason',
                    result['response_schema_violation_flags'],
                )

    def test_deciders_reject_contradictory_action_aliases(self):
        cases = (
            (
                'signal-decider',
                _complete_signal_decider_feedback(
                    preferred_action='reject',
                    next_day_open_action='buy',
                ),
            ),
            (
                'short-term-decider',
                _complete_short_term_decider_feedback(
                    tactical_action='scalp_buy_candidate',
                    preferred_action='reject',
                ),
            ),
        )

        for role, payload in cases:
            with self.subTest(role=role):
                result = normalize_agent_response(
                    role=role,
                    output_text=json.dumps(payload),
                    raw_response={'id': f'resp-action-mismatch-{role}'},
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name=role,
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertEqual(result['preferred_action'], 'reject')
                self.assertIn('action_mismatch', result['response_schema_violation_flags'])

    def test_deciders_require_exact_action_tokens_and_raw_alias_equality(self):
        cases = (
            (
                'signal-decider',
                _complete_signal_decider_feedback(
                    preferred_action='BUY',
                    next_day_open_action='buy',
                ),
                'required_field_invalid_value:preferred_action',
            ),
            (
                'short-term-decider',
                _complete_short_term_decider_feedback(
                    tactical_action='watch_for_trigger',
                    preferred_action='watch_for_trigger ',
                    wait_reason={
                        'code': 'unsuitable_execution_timing',
                        'summary': '트리거 확인 전입니다.',
                    },
                ),
                'required_field_invalid_value:preferred_action',
            ),
        )

        for role, payload, invalid_token_flag in cases:
            with self.subTest(role=role):
                result = normalize_agent_response(
                    role=role,
                    output_text=json.dumps(payload),
                    raw_response={'id': f'resp-exact-actions-{role}'},
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name=role,
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertEqual(result['preferred_action'], 'reject')
                self.assertIn(invalid_token_flag, result['response_schema_violation_flags'])
                self.assertIn('action_mismatch', result['response_schema_violation_flags'])

    def test_role_contract_provenance_is_stable_and_derived_from_canonical_prompt_contract(self):
        provenance = build_agent_contract_provenance(role='signal-decider')

        self.assertEqual(provenance['contract_version'], AGENT_ROLE_CONTRACT_VERSION)
        self.assertEqual(provenance['role'], 'signal-decider')
        self.assertRegex(provenance['contract_sha256'], r'^sha256:[0-9a-f]{64}$')
        self.assertEqual(provenance['allowed_actions'], ['buy', 'wait', 'reject'])
        self.assertEqual(
            provenance['identity_fields'],
            ['role', 'agent_name', 'symbol', 'market_date', 'traceability.symbol'],
        )

    def test_decider_contract_docs_identify_canonical_source_version_and_non_actionable_mismatch_policy(self):
        repo_root = Path(__file__).resolve().parents[3]
        signal_doc = (repo_root / 'docs' / 'SIGNAL_DECIDER_AGENT_CONTRACT.md').read_text()
        short_term_doc = (repo_root / 'docs' / 'SHORT_TERM_DECIDER_AGENT_CONTRACT.md').read_text()

        for document in (signal_doc, short_term_doc):
            self.assertIn(f'Contract version: `{AGENT_ROLE_CONTRACT_VERSION}`', document)
            self.assertIn('`apps/api/app/agent_context/response_contract.py`', document)
            self.assertIn('role/symbol/market_date mismatch', document)
            self.assertIn('hard-failed and remains non-actionable', document)
            self.assertIn('missing response symbol', document)
            self.assertIn('traceability.symbol mismatch', document)
            self.assertIn('missing role, agent_name, market_date, traceability object, or traceability.symbol', document)
        self.assertIn('`buy`, `wait`, `reject`', signal_doc)
        self.assertIn('`watch_for_trigger` is a monitor candidate', short_term_doc)
        self.assertIn('not buy-eligible or planned', short_term_doc)

    def test_signal_decider_contract_makes_exact_symbol_and_self_check_mandatory(self):
        suffix = build_contract_prompt_suffix(role='signal-decider')

        self.assertIn('symbol is mandatory', suffix)
        self.assertIn('role is mandatory', suffix)
        self.assertIn('agent_name is mandatory', suffix)
        self.assertIn('market_date is mandatory', suffix)
        self.assertIn('traceability must be an object', suffix)
        self.assertIn('exactly equal the assigned/requested symbol string', suffix)
        self.assertIn('traceability.symbol must exactly match symbol', suffix)
        self.assertIn('wait or reject', suffix)
        self.assertIn('self-check', suffix)
        self.assertIn('JSON is parseable', suffix)
        self.assertIn('Immediately before sending, validate the final raw JSON text again', suffix)
        self.assertIn('Operator-facing free-text fields must be written in Korean', suffix)
        self.assertIn('Empty recent news means no captured recent news and is neutral', suffix)
        self.assertIn('Do not forecast price reaction from news alone', suffix)
        self.assertIn('Do not summarize headline-only, paywalled, or fetch-failed news as if the full article text was read', suffix)
        self.assertIn('Do not classify a symbol as untradeable solely because auxiliary news or financial detail is absent.', suffix)
        self.assertIn('If only auxiliary context is absent, a buy decision may still be valid', suffix)
        self.assertIn('wait_reason must be an object with code and summary when preferred_action/next_day_open_action is wait; otherwise use null.', suffix)
        self.assertIn('stale_quote_orderbook', suffix)
        self.assertIn('Do not use vague wait text such as insufficient information, need more data, or wait for more confirmation', suffix)
        self.assertIn(
            'Do not translate or localize machine-readable enum fields such as preferred_action, next_day_open_action, or entry_price_basis.',
            suffix,
        )
        self.assertIn('next_day_open_action and preferred_action must be one of buy, wait, reject exactly as written.', suffix)
        self.assertIn(
            'For BUY, expected_return_pct must be a positive percentage consistent with target_price and the stated entry_price_basis',
            suffix,
        )
        self.assertIn(
            'for WAIT or REJECT it may be null when no supportable return can be calculated',
            suffix,
        )
        self.assertNotIn('conservative net return', suffix)
        self.assertIn('Core recomputes a conservative slippage-adjusted pre-screen return', suffix)
        self.assertIn('Exec applies its final full-cost check in dispatch mode', suffix)
        self.assertIn('independent swing, position, and long-horizon lane', suffix)
        self.assertIn('trigger_price, chase_limit_price', suffix)
        self.assertIn('trigger_conditions', suffix)
        self.assertIn(
            'For a non-monitorable wait, use entry_price_basis=next_open as a non-executable schema placeholder',
            suffix,
        )
        self.assertIn(
            'Do not use entry_price_basis=trigger_limit unless trigger_price, chase_limit_price, target_price, stop_loss_price, and trigger_conditions are all complete',
            suffix,
        )

    def test_signal_decider_runtime_contract_matches_non_monitorable_wait_level_semantics(self):
        repo_root = Path(__file__).resolve().parents[3]
        runtime_contract = (
            repo_root
            / 'runtime_contracts'
            / 'openclaw'
            / 'v1'
            / 'signal-decider'
            / 'AGENTS.md'
        ).read_text()

        self.assertIn(
            'For a non-monitorable `wait`, use `entry_price_basis=next_open` as a non-executable schema placeholder',
            runtime_contract,
        )
        self.assertIn(
            'Never use `entry_price_basis=trigger_limit` unless `trigger_price`, `chase_limit_price`, `target_price`, `stop_loss_price`, and `trigger_conditions` are all complete',
            runtime_contract,
        )

    def test_fundamental_summarizer_contract_accepts_market_specific_dart_or_sec_inputs(self):
        suffix = build_contract_prompt_suffix(role='fundamental-summarizer')

        self.assertIn('DART', suffix)
        self.assertIn('SEC', suffix)
        self.assertIn('market and source metadata', suffix)
        self.assertIn('Do not assume every payload is a DART payload', suffix)

    def test_summarizer_contracts_describe_nested_feedback_without_top_level_conflict(self):
        for role in ('technical-summarizer', 'fundamental-summarizer'):
            with self.subTest(role=role):
                suffix = build_contract_prompt_suffix(role=role)

                self.assertIn(
                    f'For every {role} feedback object, required keys are:',
                    suffix,
                )
                self.assertNotIn('Required top-level keys:', suffix)

    def test_short_term_decider_contract_and_normalizer_preserve_tactical_fields(self):
        suffix = build_contract_prompt_suffix(role='short-term-decider')
        self.assertIn('scalp_buy_candidate, watch_for_trigger, avoid_chase, wait, reject', suffix)
        self.assertIn('trigger_price, chase_limit_price', suffix)
        self.assertIn('agent_name is mandatory and must equal short-term-decider', suffix)
        self.assertIn('Empty recent news means no captured recent news and is neutral', suffix)
        self.assertIn('Do not forecast price reaction from news alone', suffix)
        self.assertIn('Do not summarize headline-only, paywalled, or fetch-failed news as if the full article text was read', suffix)
        self.assertIn('If only auxiliary context is absent, a structured scalp_buy_candidate or watch_for_trigger decision may still be valid', suffix)
        self.assertIn('wait_reason must be an object with code and summary when tactical_action/preferred_action is watch_for_trigger, avoid_chase, or wait; otherwise use null.', suffix)
        self.assertNotIn('1-3 trading days', suffix)
        self.assertNotIn('1-2 trading days', suffix)
        self.assertIn('normally be 2-3 trading days', suffix)
        self.assertIn('not a minimum holding lock', suffix)
        self.assertIn('maximum remains 3 trading sessions', suffix)

        output_text = json.dumps(
            _complete_short_term_decider_feedback(
                market_date='2026-05-06',
                tactical_action='avoid_chase',
                preferred_action='avoid_chase',
                wait_reason={
                    'code': 'extended_entry_price',
                    'summary': '추격 구간이라 진입 가격 메리트가 부족합니다.',
                },
                trigger_price=81200,
                chase_limit_price=82400,
                target_price=84600,
                stop_loss_price=79200,
                expected_holding_period='1-2 trading days',
                confidence_score_10=6,
                conviction_score_10=5,
                thesis_summary='단기 탄력은 있으나 추격 리스크가 큽니다.',
                trigger_conditions=['81200원 재돌파와 거래량 동반'],
                supporting_evidence=['수급 개선'],
                tactical_risks=['갭 상승 시 손익비 악화'],
                invalidation_conditions=['79200원 이탈'],
                data_quality_warnings=['마켓 컨텍스트가 stale입니다.'],
                summary='단기 탄력은 있으나 추격 리스크가 큽니다.',
                confidence=0.6,
                improvement_notes=['실시간 호가 잔량이 있으면 판단이 개선됩니다.'],
            )
        )

        result = normalize_agent_response(
            role='short-term-decider',
            output_text=output_text,
            raw_response={'id': 'resp-short-1'},
            market_date='2026-05-06',
            symbol='005930',
            agent_name='short-term-decider',
        )

        self.assertEqual(result['tactical_action'], 'avoid_chase')
        self.assertEqual(result['preferred_action'], 'avoid_chase')
        self.assertEqual(result['trigger_price'], 81200.0)
        self.assertEqual(result['chase_limit_price'], 82400.0)
        self.assertEqual(result['target_price'], 84600.0)
        self.assertEqual(result['stop_loss_price'], 79200.0)
        self.assertEqual(result['wait_reason']['code'], 'extended_entry_price')
        self.assertEqual(result['confidence'], 0.6)
        self.assertEqual(result['traceability']['agent_name'], 'short-term-decider')
        self.assertEqual(result['traceability']['symbol'], '005930')
        self.assertIn('마켓 컨텍스트가 stale입니다.', result['risk_flags'])
        self.assertEqual(
            result['details']['tactical_decision_support']['trigger_conditions'],
            ['81200원 재돌파와 거래량 동반'],
        )

    def test_short_term_decider_normalizer_rejects_batch_envelope_after_diagnostic_extraction(self):
        output_text = json.dumps(
            {
                'batch': {
                    'items': [
                        _complete_short_term_decider_feedback(
                            market_date='2026-05-06',
                            tactical_action='watch_for_trigger',
                            preferred_action='watch_for_trigger',
                            wait_reason={
                                'code': 'stale_quote_orderbook',
                                'summary': '마켓 컨텍스트가 오래됐습니다.',
                            },
                            confidence=0.7,
                            thesis_summary='저항대 근처라 확인이 필요합니다.',
                            summary='저항대 근처라 확인이 필요합니다.',
                            data_quality_warnings=['마켓 컨텍스트가 오래됐습니다.'],
                            risk_flags=['stale_market_context'],
                        )
                    ]
                }
            }
        )

        result = normalize_agent_response(
            role='short-term-decider',
            output_text=output_text,
            raw_response={'id': 'resp-short-batch'},
            market_date='2026-05-06',
            symbol='005930',
            agent_name='short-term-decider',
        )

        self.assertEqual(result['role'], 'short-term-decider')
        self.assertEqual(result['agent_name'], 'short-term-decider')
        self.assertEqual(result['symbol'], '005930')
        self.assertEqual(result['market_date'], '2026-05-06')
        self.assertEqual(result['tactical_action'], 'reject')
        self.assertEqual(result['preferred_action'], 'reject')
        self.assertFalse(result['response_contract_valid'])
        self.assertIn(
            'response_structure_mismatch:batch_envelope',
            result['response_schema_violation_flags'],
        )
        self.assertEqual(result['confidence'], 0.7)
        self.assertIn('stale_market_context', result['risk_flags'])
        self.assertIn('마켓 컨텍스트가 오래됐습니다.', result['risk_flags'])
        self.assertEqual(result['traceability']['symbol'], '005930')
        self.assertTrue(result['details']['tactical_decision_support']['decision_support_only'])

    def test_normalize_signal_decider_response_preserves_decision_support_fields_and_traceability(self):
        output_text = json.dumps(
            _complete_signal_decider_feedback(
                target_price=91200,
                expected_return_pct=6.4,
                stop_loss_price=82900,
                suggested_holding_period='3-10 trading days',
                confidence_score_10=7,
                conviction_score_10=6,
                thesis_summary='Momentum and fundamentals align for a tactical long setup.',
                supporting_evidence=['Breadth improved into the close.', 'Breakout level held above support.'],
                decision_risks=['Leadership is still narrow.'],
                invalidation_conditions=['Gap below 82900 at the next open.'],
                data_quality_warnings=['Signal-feature coverage was partial.'],
                specialist_disagreement=['Fundamental view was constructive but not emphatic.'],
                summary='Momentum and fundamentals align for a tactical long setup.',
                improvement_notes=['Add premarket gap context.'],
            )
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

    def test_signal_decider_identity_mismatch_is_rejected_instead_of_remaining_buy_actionable(self):
        output_text = json.dumps(
            {
                'role': 'short-term-decider',
                'agent_name': 'signal-decider',
                'symbol': '000660',
                'market_date': '2026-04-22',
                'traceability': {'symbol': '005930'},
                'preferred_action': 'buy',
                'next_day_open_action': 'buy',
                'thesis_summary': '잘못 귀속된 매수 응답입니다.',
                'supporting_evidence': ['모멘텀이 강합니다.'],
            }
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=output_text,
            raw_response={'id': 'resp-mismatched-signal'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['preferred_action'], 'reject')
        self.assertEqual(result['next_day_open_action'], 'reject')
        self.assertEqual(
            result['structural_mismatch_flags'],
            ['role_mismatch', 'symbol_mismatch', 'market_date_mismatch'],
        )
        self.assertEqual(result['traceability']['symbol'], '005930')
        self.assertEqual(result['traceability']['reported_symbol'], '000660')
        self.assertEqual(result['details']['decision_support']['next_day_open_action'], 'reject')

    def test_deciders_reject_actionable_responses_with_missing_required_identity_fields(self):
        cases = (
            ('signal-decider', {'preferred_action': 'buy', 'next_day_open_action': 'buy'}),
            (
                'short-term-decider',
                {
                    'preferred_action': 'scalp_buy_candidate',
                    'tactical_action': 'scalp_buy_candidate',
                },
            ),
        )

        for role, action_fields in cases:
            with self.subTest(role=role):
                output_text = json.dumps(
                    {
                        **action_fields,
                        'thesis_summary': '필수 응답 식별자가 누락된 매수 후보입니다.',
                    }
                )

                result = normalize_agent_response(
                    role=role,
                    output_text=output_text,
                    raw_response={'id': f'resp-missing-required-{role}'},
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name=role,
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertEqual(result['preferred_action'], 'reject')
                if role == 'signal-decider':
                    self.assertEqual(result['next_day_open_action'], 'reject')
                else:
                    self.assertEqual(result['tactical_action'], 'reject')
                self.assertEqual(
                    result['structural_mismatch_flags'],
                    [
                        'role_missing',
                        'agent_name_missing',
                        'symbol_missing',
                        'market_date_missing',
                        'traceability_missing',
                        'traceability_symbol_missing',
                    ],
                )

    def test_deciders_reject_mismatched_agent_name(self):
        for role in ('signal-decider', 'short-term-decider'):
            with self.subTest(role=role):
                action_fields = (
                    {'preferred_action': 'buy', 'next_day_open_action': 'buy'}
                    if role == 'signal-decider'
                    else {
                        'preferred_action': 'scalp_buy_candidate',
                        'tactical_action': 'scalp_buy_candidate',
                    }
                )
                output_text = json.dumps(
                    {
                        'role': role,
                        'agent_name': 'wrong-agent',
                        'symbol': '005930',
                        'market_date': '2026-04-23',
                        'traceability': {'symbol': '005930'},
                        **action_fields,
                        'thesis_summary': '에이전트 이름이 잘못된 매수 후보입니다.',
                    }
                )

                result = normalize_agent_response(
                    role=role,
                    output_text=output_text,
                    raw_response={'id': f'resp-agent-mismatch-{role}'},
                    market_date='2026-04-23',
                    symbol='005930',
                    agent_name=role,
                )

                self.assertFalse(result['response_contract_valid'])
                self.assertEqual(result['preferred_action'], 'reject')
                self.assertEqual(result['structural_mismatch_flags'], ['agent_name_mismatch'])
                self.assertEqual(result['traceability']['reported_agent_name'], 'wrong-agent')

    def test_deciders_reject_whitespace_variants_in_raw_identity_fields(self):
        role_payloads = (
            ('signal-decider', _complete_signal_decider_feedback, 'next_day_open_action'),
            ('short-term-decider', _complete_short_term_decider_feedback, 'tactical_action'),
        )
        identity_cases = (
            ('role', 'role_mismatch'),
            ('agent_name', 'agent_name_mismatch'),
            ('symbol', 'symbol_mismatch'),
            ('market_date', 'market_date_mismatch'),
            ('traceability.symbol', 'traceability_symbol_mismatch'),
        )

        for role, payload_factory, action_field in role_payloads:
            canonical_values = {
                'role': role,
                'agent_name': role,
                'symbol': '005930',
                'market_date': '2026-04-23',
                'traceability.symbol': '005930',
            }
            for field, expected_flag in identity_cases:
                for position, raw_value in (
                    ('leading', f" {canonical_values[field]}"),
                    ('trailing', f"{canonical_values[field]} "),
                ):
                    with self.subTest(role=role, field=field, position=position):
                        payload = payload_factory()
                        if field == 'traceability.symbol':
                            payload['traceability'] = {'symbol': raw_value}
                        else:
                            payload[field] = raw_value

                        result = normalize_agent_response(
                            role=role,
                            output_text=json.dumps(payload),
                            raw_response=payload,
                            market_date='2026-04-23',
                            symbol='005930',
                            agent_name=role,
                        )

                        self.assertFalse(result['response_contract_valid'])
                        self.assertEqual(result[action_field], 'reject')
                        self.assertEqual(result['structural_mismatch_flags'], [expected_flag])

    def test_signal_decider_normalizer_rejects_duplicate_action_key(self):
        feedback_json = json.dumps(_complete_signal_decider_feedback())
        duplicate_action_json = '{"preferred_action":"wait",' + feedback_json[1:]

        result = normalize_agent_response(
            role='signal-decider',
            output_text=duplicate_action_json,
            raw_response={'output_text': duplicate_action_json},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['next_day_open_action'], 'reject')
        self.assertIn('json_duplicate_key', result['response_schema_violation_flags'])

    def test_generic_specialist_rejects_whitespace_variant_role(self):
        payload = {
            'role': ' technical-summarizer ',
            'summary': '돌파 구조가 유지되고 있습니다.',
            'confidence': 0.7,
            'risk_flags': [],
            'details': {},
            'improvement_notes': [],
        }

        result = normalize_agent_response(
            role='technical-summarizer',
            output_text=json.dumps(payload),
            raw_response=payload,
            market_date='2026-04-23',
            symbol='005930',
            agent_name='technical-summarizer',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['structural_mismatch_flags'], ['role_mismatch'])

    def test_signal_decider_missing_symbol_and_mismatched_traceability_symbol_are_rejected(self):
        output_text = json.dumps(
            {
                'role': 'signal-decider',
                'agent_name': 'signal-decider',
                'market_date': '2026-04-23',
                'preferred_action': 'buy',
                'next_day_open_action': 'buy',
                'thesis_summary': '응답 본문 심볼이 누락된 매수 응답입니다.',
                'traceability': {'symbol': '000660'},
            }
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=output_text,
            raw_response={'id': 'resp-missing-signal-symbol'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['preferred_action'], 'reject')
        self.assertEqual(result['next_day_open_action'], 'reject')
        self.assertEqual(
            result['structural_mismatch_flags'],
            ['symbol_missing', 'traceability_symbol_mismatch'],
        )
        self.assertEqual(result['traceability']['symbol'], '005930')
        self.assertEqual(result['traceability']['reported_traceability_symbol'], '000660')

    def test_short_term_decider_traceability_symbol_mismatch_is_rejected(self):
        output_text = json.dumps(
            {
                'role': 'short-term-decider',
                'agent_name': 'short-term-decider',
                'symbol': '005930',
                'market_date': '2026-05-06',
                'tactical_action': 'scalp_buy_candidate',
                'trigger_price': 81200,
                'chase_limit_price': 82400,
                'target_price': 84600,
                'stop_loss_price': 79200,
                'thesis_summary': 'traceability가 다른 심볼을 가리키는 후보 응답입니다.',
                'traceability': {'symbol': '000660'},
            }
        )

        result = normalize_agent_response(
            role='short-term-decider',
            output_text=output_text,
            raw_response={'id': 'resp-mismatched-short-trace'},
            market_date='2026-05-06',
            symbol='005930',
            agent_name='short-term-decider',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['tactical_action'], 'reject')
        self.assertEqual(result['preferred_action'], 'reject')
        self.assertEqual(result['structural_mismatch_flags'], ['traceability_symbol_mismatch'])
        self.assertEqual(result['traceability']['symbol'], '005930')
        self.assertEqual(result['traceability']['reported_traceability_symbol'], '000660')

    def test_short_term_decider_identity_mismatch_is_rejected_instead_of_remaining_candidate(self):
        output_text = json.dumps(
            {
                'role': 'signal-decider',
                'agent_name': 'short-term-decider',
                'symbol': '005930',
                'market_date': '2026-05-06',
                'traceability': {'symbol': '005930'},
                'tactical_action': 'scalp_buy_candidate',
                'trigger_price': 81200,
                'chase_limit_price': 82400,
                'target_price': 84600,
                'stop_loss_price': 79200,
                'thesis_summary': '역할이 뒤바뀐 단기 후보 응답입니다.',
            }
        )

        result = normalize_agent_response(
            role='short-term-decider',
            output_text=output_text,
            raw_response={'id': 'resp-mismatched-short'},
            market_date='2026-05-06',
            symbol='005930',
            agent_name='short-term-decider',
        )

        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['tactical_action'], 'reject')
        self.assertEqual(result['preferred_action'], 'reject')
        self.assertEqual(result['structural_mismatch_flags'], ['role_mismatch'])
        self.assertEqual(result['traceability']['reported_role'], 'signal-decider')
        self.assertEqual(result['details']['tactical_decision_support']['tactical_action'], 'reject')

    def test_normalize_signal_decider_response_infers_specific_wait_reason_from_core_blocker(self):
        output_text = json.dumps(
            _complete_signal_decider_feedback(
                preferred_action='wait',
                next_day_open_action='wait',
                wait_reason={
                    'code': 'stale_quote_orderbook',
                    'summary': 'Realtime orderbook freshness is stale.',
                },
                target_price=None,
                expected_return_pct=None,
                stop_loss_price=None,
                thesis_summary='Need more data before entry.',
                summary='Need more data before entry.',
                supporting_evidence=['Momentum is still constructive.'],
                decision_risks=['Bid/ask spread is stale into the open.'],
                invalidation_conditions=['Skip the setup if the stale book persists.'],
                data_quality_warnings=[
                    'No recent news/disclosures were captured.',
                    'Realtime orderbook freshness is stale.',
                ],
                specialist_disagreement=[],
                improvement_notes=['Premarket depth would help.'],
            )
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=output_text,
            raw_response={'id': 'resp-wait-1'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertEqual(result['wait_reason']['code'], 'stale_quote_orderbook')
        self.assertNotIn('news', result['wait_reason']['summary'].lower())
        self.assertEqual(result['details']['decision_support']['wait_reason']['code'], 'stale_quote_orderbook')

    def test_normalize_signal_decider_response_ignores_positive_liquidity_support_when_trend_is_actual_blocker(self):
        output_text = json.dumps(
            _complete_signal_decider_feedback(
                preferred_action='wait',
                next_day_open_action='wait',
                wait_reason={
                    'code': 'weak_trend_relative_strength',
                    'summary': 'Weak trend remains the main blocker.',
                },
                target_price=None,
                expected_return_pct=None,
                stop_loss_price=None,
                thesis_summary='Need more confirmation before entry.',
                summary='Need more confirmation before entry.',
                supporting_evidence=['Liquidity is strong enough for execution.'],
                decision_risks=['Weak trend remains the main blocker.'],
                invalidation_conditions=['Skip the setup if trend confirmation keeps fading.'],
                data_quality_warnings=[],
                specialist_disagreement=[],
                improvement_notes=[],
            )
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=output_text,
            raw_response={'id': 'resp-wait-strong-liquidity'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertEqual(result['wait_reason']['code'], 'weak_trend_relative_strength')
        self.assertEqual(result['wait_reason']['summary'], 'Weak trend remains the main blocker.')
        self.assertNotEqual(result['wait_reason']['code'], 'insufficient_liquidity')

    def test_normalize_signal_decider_response_keeps_negative_liquidity_gaps_mapped_to_liquidity_wait_reason(self):
        output_text = json.dumps(
            _complete_signal_decider_feedback(
                preferred_action='wait',
                next_day_open_action='wait',
                wait_reason={
                    'code': 'insufficient_liquidity',
                    'summary': 'Turnover is too low and spread is too wide for reliable execution.',
                },
                target_price=None,
                expected_return_pct=None,
                stop_loss_price=None,
                thesis_summary='Need more confirmation before entry.',
                summary='Need more confirmation before entry.',
                supporting_evidence=['Price is still above support.'],
                decision_risks=['Turnover is too low and spread is too wide for reliable execution.'],
                invalidation_conditions=['Skip the setup if execution quality does not improve.'],
                data_quality_warnings=[],
                specialist_disagreement=[],
                improvement_notes=[],
            )
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=output_text,
            raw_response={'id': 'resp-wait-liquidity-gap'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertEqual(result['wait_reason']['code'], 'insufficient_liquidity')
        self.assertEqual(
            result['wait_reason']['summary'],
            'Turnover is too low and spread is too wide for reliable execution.',
        )

    def test_normalize_signal_decider_response_flags_generic_wait_reason_without_specific_blocker(self):
        output_text = json.dumps(
            _complete_signal_decider_feedback(
                preferred_action='wait',
                next_day_open_action='wait',
                wait_reason=None,
                target_price=None,
                expected_return_pct=None,
                stop_loss_price=None,
                thesis_summary='Need more data before entry.',
                summary='Need more data before entry.',
                supporting_evidence=['Momentum is still constructive.'],
                decision_risks=['Wait for more confirmation.'],
                invalidation_conditions=['No clear invalidation was supplied.'],
                data_quality_warnings=['No recent news/disclosures were captured.'],
                specialist_disagreement=[],
                improvement_notes=[],
            )
        )

        result = normalize_agent_response(
            role='signal-decider',
            output_text=output_text,
            raw_response={'id': 'resp-wait-2'},
            market_date='2026-04-23',
            symbol='005930',
            agent_name='signal-decider',
        )

        self.assertIsNone(result['wait_reason'])
        self.assertIn('wait_reason_missing_for_wait_action', result['risk_flags'])
        self.assertIn('generic_wait_reason_missing_specific_blocker', result['risk_flags'])
        self.assertIn(
            'action_requirement_invalid:wait_reason',
            result['response_schema_violation_flags'],
        )
        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(result['next_day_open_action'], 'reject')
        self.assertIn(
            'Wait decisions must include a concrete wait_reason code and blocker summary.',
            result['improvement_notes'],
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

        self.assertEqual(result['next_day_open_action'], 'reject')
        self.assertEqual(result['preferred_action'], 'reject')
        self.assertFalse(result['response_contract_valid'])
        self.assertEqual(
            result['structural_mismatch_flags'],
            [
                'role_missing',
                'agent_name_missing',
                'symbol_missing',
                'market_date_missing',
                'traceability_missing',
                'traceability_symbol_missing',
            ],
        )
        self.assertIn('unstructured_response', result['risk_flags'])
        self.assertEqual(result['data_quality_warnings'], ['unstructured_response'])
        self.assertEqual(result['traceability']['symbol'], '005930')
        self.assertEqual(result['traceability']['market_date'], '2026-04-23')
        self.assertIn('Return the full structured decision-support contract in JSON.', result['improvement_notes'])

    def test_normalize_signal_decider_response_keeps_action_tokens_machine_readable(self):
        output_text = json.dumps(
            _complete_signal_decider_feedback(
                thesis_summary='다음 거래일 시가 대응은 매수 우위로 판단됩니다.',
                summary='다음 거래일 시가 대응은 매수 우위로 판단됩니다.',
                supporting_evidence=['단기 추세와 수급 흐름이 동시에 개선됐습니다.'],
                decision_risks=['장 초반 변동성이 다시 확대될 수 있습니다.'],
                invalidation_conditions=['시가 직후 지지선 이탈이 확인되면 전제가 훼손됩니다.'],
                data_quality_warnings=['일부 보조 신호는 장 마감 기준으로만 확인됐습니다.'],
                specialist_disagreement=['펀더멘털 해석은 중립에 가까웠습니다.'],
                improvement_notes=['장전 갭 방향 데이터가 추가되면 더 엄밀해집니다.'],
            )
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
