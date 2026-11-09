from copy import deepcopy
from datetime import date
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from app.agent_context import orchestrator as orchestrator_module
from app.agent_context.orchestrator import AgentFeedbackOrchestrator
from app.agent_context.orchestrator_helpers import (
    attach_timesfm_forecast_evidence,
    compressed_batch_item_for_prompt,
    trim_batch_item_for_prompt,
)
from app.forecast_context.service import unavailable_evidence


MARKET_DATE = date(2026, 11, 9)
CUTOFF = '2026-11-09T12:00:00+00:00'
DECIDERS = ('signal-decider', 'short-term-decider')


def source_item(symbol='005930'):
    return {
        'symbol': symbol,
        'market_date': MARKET_DATE.isoformat(),
        'price_context': {
            'trade_date': MARKET_DATE.isoformat(),
            'close_price': 100,
        },
    }


def forecast(symbol='005930'):
    return {
        'schema_version': 'timesfm_forecast_evidence_v1',
        'status': 'available',
        'symbol': symbol,
        'market_date': MARKET_DATE.isoformat(),
        'input_end_date': MARKET_DATE.isoformat(),
        'model_id': 'google/timesfm-3.0-pytorch',
        'model_revision': 'test-fixed-revision',
        'input_fingerprint': 'test-input-fingerprint',
        'evidence_fingerprint': 'test-evidence-fingerprint',
        'price_basis': 'stored_daily_close',
        'horizon_unit': 'trading_bars',
        'calibration_status': 'not_evaluated',
        'anchor_close': 100,
        'horizons': [
            {
                'trading_bars': 1,
                'price_q10': 98,
                'price_q50': 101,
                'price_q90': 104,
                'return_q10_pct': -2,
                'median_return_pct': 1,
                'return_q90_pct': 4,
            },
        ],
    }


class TimesFMDeciderEvidenceTests(unittest.TestCase):
    def prepare(self, role, *, items=None, evidence=None, reuse=None, error=None):
        service = AgentFeedbackOrchestrator()
        items = items or [source_item()]
        failures = []
        with (
            patch.object(service, '_build_decider_policy_context', return_value={}),
            patch.object(service, '_build_upstream_feedback_bundle', return_value=({}, {}, [])),
            patch.object(orchestrator_module.runtime_feedback_state_store, 'get_run', return_value=None),
            patch.object(orchestrator_module.signal_decider_market_context_builder, 'build', return_value={}),
            patch.object(
                orchestrator_module.timesfm_forecast_service,
                'build_for_symbols',
                return_value=evidence or {},
                side_effect=error,
            ) as build,
        ):
            result = service._prepare_role_dispatch_items(
                role=role,
                market_date=MARKET_DATE,
                policy_context_as_of_date=MARKET_DATE,
                request_run_id='timesfm-integration-test',
                symbol_context_items=items,
                source_context_scope_symbols=[item['symbol'] for item in items],
                eligibility_scope_symbols=[item['symbol'] for item in items],
                fs_div='CFS',
                disclosures_limit=5,
                source_knowledge_cutoff_at=CUTOFF,
                upstream_summary_run_id=None,
                reuse_decisions=reuse or {},
                previous_decision_context_decisions={},
                upstream_reuse_decisions={},
                all_failures=failures,
                buffer_failure=lambda *args, **kwargs: None,
            )
        return result, build, failures

    def test_both_deciders_receive_forecast_without_changing_source_generations(self):
        for role in DECIDERS:
            with self.subTest(role=role):
                original = source_item()
                untouched = deepcopy(original)
                baseline, _, _ = self.prepare(role, items=[original])
                payloads, build, failures = self.prepare(
                    role, items=[original], evidence={'005930': forecast()}
                )
                self.assertFalse(failures)
                build.assert_called_once_with(
                    market_date=MARKET_DATE,
                    symbols=['005930'],
                    knowledge_cutoff_at=CUTOFF,
                )
                payload = payloads[0]
                self.assertEqual(payload['forecast_evidence']['timesfm3'], forecast())
                self.assertEqual(
                    payload['forecast_evidence_generation']['evidence_fingerprint'],
                    forecast()['evidence_fingerprint'],
                )
                self.assertEqual(original, untouched)
                self.assertNotIn('forecast_evidence', payload['symbol_day_context'])
                for generation in ('decision_input_generation', 'durable_decision_input_generation'):
                    self.assertEqual(payload[generation], baseline[0][generation])
                for render in (trim_batch_item_for_prompt, compressed_batch_item_for_prompt):
                    rendered = render(payload)
                    self.assertEqual(rendered['forecast_evidence'], payload['forecast_evidence'])
                    self.assertNotIn('forecast_evidence_generation', rendered)
                    guidance = rendered['decision_support_constraints']['forecast_evidence_guidance']
                    self.assertIn('median', ' '.join(guidance['interpretation']))
                    self.assertIn('not an expected return', ' '.join(guidance['interpretation']))
                    self.assertIn('Unavailable forecasts are neutral', guidance['missing_evidence_policy'])

    def test_forecast_batch_excludes_reused_and_stale_price_symbols(self):
        stale = source_item('051910')
        stale['price_context']['trade_date'] = '2026-11-06'
        items = [source_item(), source_item('000660'), source_item('035420'), stale]
        payloads, build, failures = self.prepare(
            'signal-decider',
            items=items,
            evidence={'005930': forecast(), '000660': forecast('000660')},
            reuse={'035420': {'signal-decider': SimpleNamespace(should_dispatch=False)}},
        )
        self.assertEqual(build.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(build.call_count, 1)
        self.assertEqual([item['symbol'] for item in payloads], ['005930', '000660'])
        self.assertEqual([item['symbol'] for item in failures], ['051910'])

    def test_summarizer_does_not_call_forecasting(self):
        payloads, build, failures = self.prepare('technical-summarizer')
        build.assert_not_called()
        self.assertFalse(failures)
        self.assertNotIn('forecast_evidence', payloads[0])

    def test_forecast_anchor_mismatch_is_unavailable_without_affecting_other_symbols(self):
        for mismatch in ({'anchor_close': 99}, {'input_end_date': '2026-11-06'}):
            with self.subTest(mismatch=mismatch):
                evidence = {'005930': {**forecast(), **mismatch}, '000660': forecast('000660')}
                original = deepcopy(evidence)
                payloads, _, failures = self.prepare(
                    'signal-decider',
                    items=[source_item(), source_item('000660')],
                    evidence=evidence,
                )
                self.assertFalse(failures)
                unavailable = payloads[0]['forecast_evidence']['timesfm3']
                self.assertEqual(unavailable['status'], 'unavailable')
                self.assertEqual(unavailable['reason'], 'forecast_anchor_mismatch')
                self.assertEqual(payloads[1]['forecast_evidence']['timesfm3'], original['000660'])
                self.assertEqual(evidence, original)

    def test_disabled_and_failed_forecasting_preserve_decider_dispatch(self):
        for role in DECIDERS:
            with self.subTest(role=role):
                payloads, _, failures = self.prepare(role)
                self.assertFalse(failures)
                self.assertNotIn('forecast_evidence', payloads[0])
                self.assertNotIn('forecast_evidence_guidance', payloads[0]['decision_support_constraints'])
                payloads, _, failures = self.prepare(role, error=RuntimeError('unavailable model'))
                self.assertFalse(failures)
                self.assertEqual(payloads[0]['forecast_evidence']['timesfm3']['status'], 'unavailable')

    def test_validation_is_independent_for_each_decider_lane(self):
        source_items = [
            {'role': role, 'symbol': '005930', 'forecast_evidence': {'timesfm3': forecast()}}
            for role in DECIDERS
        ]
        with patch.object(
            orchestrator_module.timesfm_forecast_service,
            'validate_evidence',
            side_effect=[{'005930': 'input_changed'}, {}],
        ) as validate:
            failures = AgentFeedbackOrchestrator._validate_forecast_evidence(
                market_date=MARKET_DATE, source_items=source_items, knowledge_cutoff_at=CUTOFF
            )
        self.assertEqual(validate.call_count, 2)
        self.assertEqual(failures, {('signal-decider', '005930'): 'input_changed'})

    def test_unavailable_evidence_is_validated_and_remains_neutral(self):
        source_items = [
            {'role': DECIDERS[0], 'symbol': '005930'},
            {
                'role': DECIDERS[1],
                'symbol': '005930',
                'forecast_evidence': {
                    'timesfm3': unavailable_evidence('005930', MARKET_DATE, 'timeout')
                },
            },
        ]
        with patch.object(
            orchestrator_module.timesfm_forecast_service, 'validate_evidence', return_value={}
        ) as validate:
            failures = AgentFeedbackOrchestrator._validate_forecast_evidence(
                market_date=MARKET_DATE, source_items=source_items, knowledge_cutoff_at=CUTOFF
            )
        validate.assert_called_once_with(
            market_date=MARKET_DATE,
            evidence_by_symbol={'005930': source_items[1]['forecast_evidence']['timesfm3']},
            knowledge_cutoff_at=CUTOFF,
        )
        self.assertEqual(failures, {})

    def test_attached_forecast_cannot_disappear_as_if_it_were_a_legacy_request(self):
        source_items = [
            {
                'role': DECIDERS[0],
                'symbol': '005930',
                'forecast_evidence_generation': {
                    'schema_version': 'forecast_evidence_generation_v1',
                    'evidence_fingerprint': 'test-evidence-fingerprint',
                },
            },
            {'role': DECIDERS[1], 'symbol': '005930'},
        ]
        with patch.object(orchestrator_module.timesfm_forecast_service, 'validate_evidence') as validate:
            failures = AgentFeedbackOrchestrator._validate_forecast_evidence(
                market_date=MARKET_DATE, source_items=source_items, knowledge_cutoff_at=CUTOFF
            )
        validate.assert_not_called()
        self.assertEqual(failures, {('signal-decider', '005930'): 'missing_forecast_evidence'})

    def test_malformed_evidence_and_failed_validation_are_not_silently_trusted(self):
        malformed = {'role': DECIDERS[0], 'symbol': '005930', 'forecast_evidence': {'timesfm3': None}}
        available = {'role': DECIDERS[1], 'symbol': '000660', 'forecast_evidence': {'timesfm3': forecast('000660')}}
        with patch.object(
            orchestrator_module.timesfm_forecast_service, 'validate_evidence', side_effect=RuntimeError('DB unavailable')
        ):
            failures = AgentFeedbackOrchestrator._validate_forecast_evidence(
                market_date=MARKET_DATE, source_items=[malformed, available], knowledge_cutoff_at=CUTOFF
            )
        self.assertEqual(failures[('signal-decider', '005930')], 'malformed_forecast_evidence')
        self.assertEqual(failures[('short-term-decider', '000660')], 'forecast_validation_unavailable')

    def test_replaced_forecast_must_match_its_saved_generation(self):
        payload = attach_timesfm_forecast_evidence(
            {'role': DECIDERS[0], 'symbol': '005930'}, forecast()
        )
        payload['forecast_evidence']['timesfm3']['evidence_fingerprint'] = 'replacement'
        with patch.object(orchestrator_module.timesfm_forecast_service, 'validate_evidence') as validate:
            failures = AgentFeedbackOrchestrator._validate_forecast_evidence(
                market_date=MARKET_DATE, source_items=[payload], knowledge_cutoff_at=CUTOFF
            )
        validate.assert_not_called()
        self.assertEqual(
            failures, {('signal-decider', '005930'): 'forecast_evidence_generation_mismatch'}
        )

    def test_forecast_guard_participates_in_normal_and_durable_revalidation(self):
        service = AgentFeedbackOrchestrator()
        original = source_item()
        payload = service._build_signal_decider_payload(
            item=original,
            market_date=MARKET_DATE,
            request_run_id='timesfm-revalidation-test',
            market_context={},
            upstream_summaries={},
            upstream_traceability={},
            signal_decider_policy={},
            source_knowledge_cutoff_at=CUTOFF,
        )
        payload = attach_timesfm_forecast_evidence(payload, forecast())
        for durable in (False, True):
            for invalid in ({}, {'005930': 'forecast_input_changed'}):
                with (
                    self.subTest(durable=durable, invalid=bool(invalid)),
                    patch.object(orchestrator_module.agent_context_read_service, 'get_symbol_day_context', return_value={'items': [original]}),
                    patch.object(orchestrator_module, '_split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics', return_value=(['005930'], [], {'records_by_symbol': {}})),
                    patch.object(orchestrator_module.runtime_feedback_state_store, 'get_run', return_value=None),
                    patch.object(orchestrator_module.agent_context_reuse_service, 'build_latest_decisions', return_value={}),
                    patch.object(service, '_build_decider_policy_context', return_value={}),
                    patch.object(orchestrator_module, 'UPSTREAM_SUMMARY_ROLES', ()),
                    patch.object(orchestrator_module.timesfm_forecast_service, 'validate_evidence', return_value=invalid),
                    patch.object(orchestrator_module.timesfm_forecast_service, 'build_for_symbols') as infer,
                ):
                    failures = service._revalidate_decider_dispatch_inputs(
                        market_date=MARKET_DATE,
                        request_run_id='timesfm-revalidation-test',
                        source_items=[payload],
                        source_context_scope_symbols=['005930'],
                        eligibility_scope_symbols=['005930'],
                        fs_div='CFS',
                        disclosures_limit=5,
                        revalidate_market_context=False,
                        durable_source_generation=durable,
                        source_knowledge_cutoff_at=CUTOFF,
                    )
                infer.assert_not_called()
                if invalid:
                    reasons = failures[('signal-decider', '005930')]['reasons']
                    self.assertEqual(reasons, [{'code': 'forecast_evidence_invalid', 'model': 'timesfm3', 'reason': 'forecast_input_changed'}])
                else:
                    self.assertEqual(failures, {})


if __name__ == '__main__':
    unittest.main()
