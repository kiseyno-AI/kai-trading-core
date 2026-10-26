import json
import unittest
from datetime import date
from unittest.mock import patch

from app.agent_context.orchestrator import OpenClawAgentClient
from app.agent_context.orchestrator_helpers import correlate_decoded_batch_response
from app.us_equities.agent_analysis_service import US_AGENT_ROLE_CAPS, USAgentAnalysisService


def _candidate(index: int) -> dict:
    symbol = f'U{index:05d}'
    return {
        'id': index + 1,
        'symbol': symbol,
        'name_en': symbol,
        'market': 'NASDAQ',
        'exchange_code': 'NASDAQ',
        'regulatory_id': str(index).zfill(10),
        'collection_run_id': 77,
        'rank': index + 1,
        'composite_score': '0.9',
        'metrics_payload': {
            'price': {
                'latest_price': '10.25',
                'history_days': 252,
                'median_dollar_volume_20d': '25000000',
                'median_volume_20d': '2000000',
                'volatility_60d': '0.32',
                'max_drawdown_120d': '-0.18',
                'momentum_6m': '0.21',
                'momentum_12m': '0.34',
                'return_5d': '0.04',
            },
            'fundamental': {
                'latest_periodic_filing_date': '2026-07-31',
                'ttm_revenue': '1000',
                'ttm_net_income': '120',
                'ttm_operating_cash_flow': '150',
                'total_assets': '2400',
                'total_liabilities': '900',
                'shares_outstanding_growth_yoy': '0.015',
            },
        },
        'decision_payload': {'sec_accession_number': 'test'},
    }


class FakeDispatcher:
    @staticmethod
    def _feedback(*, spec, market_date, item):
        common = {
            'role': spec.role,
            'agent_name': spec.agent_name,
            'symbol': item['symbol'],
            'market_date': market_date.isoformat(),
            'traceability': {'symbol': item['symbol']},
            'summary': f"{item['symbol']} summary",
            'confidence': 0.8,
            'risk_flags': [],
            'details': {},
            'improvement_notes': [],
        }
        if spec.role == 'signal-decider':
            return {
                **common,
                'preferred_action': 'buy',
                'next_day_open_action': 'buy',
                'wait_reason': None,
                'entry_price_basis': 'next_open',
                'trigger_price': None,
                'chase_limit_price': None,
                'target_price': 10.90,
                'expected_return_pct': 6.34,
                'stop_loss_price': 9.80,
                'suggested_holding_period': '3-10 trading days',
                'confidence_score_10': 8,
                'conviction_score_10': 7,
                'thesis_summary': f"{item['symbol']} summary",
                'trigger_conditions': [],
                'supporting_evidence': ['추세와 유동성이 매수 후보를 지지합니다.'],
                'decision_risks': ['장 초반 변동성이 확대될 수 있습니다.'],
                'decision_delta': {},
                'invalidation_conditions': ['9.80달러 이탈 시 전제가 훼손됩니다.'],
                'data_quality_warnings': [],
                'specialist_disagreement': [],
            }
        if spec.role == 'short-term-decider':
            return {
                **common,
                'tactical_action': 'watch_for_trigger',
                'preferred_action': 'watch_for_trigger',
                'wait_reason': {
                    'code': 'unsuitable_execution_timing',
                    'summary': '10.30달러 돌파와 거래량 확인 전입니다.',
                },
                'trigger_price': 10.30,
                'chase_limit_price': 10.50,
                'target_price': 10.90,
                'stop_loss_price': 9.80,
                'expected_holding_period': '2-3 trading days',
                'confidence_score_10': 8,
                'conviction_score_10': 7,
                'thesis_summary': f"{item['symbol']} summary",
                'trigger_conditions': ['10.30달러 돌파와 거래량 증가를 확인합니다.'],
                'supporting_evidence': ['단기 상대강도가 개선됐습니다.'],
                'tactical_risks': ['갭 상승 시 손익비가 악화될 수 있습니다.'],
                'decision_delta': {},
                'invalidation_conditions': ['9.80달러 이탈 시 전제가 훼손됩니다.'],
                'data_quality_warnings': [],
            }
        return common

    def dispatch_batch(self, *, spec, market_date, batch_items):
        decoded = {'items': [
            {
                'symbol': item['symbol'],
                'feedback': self._feedback(spec=spec, market_date=market_date, item=item),
            }
            for item in batch_items
        ]}
        decoded, response_correlation = correlate_decoded_batch_response(
            decoded=decoded,
            expected_symbols=[item['symbol'] for item in batch_items],
        )
        return {
            'decoded': decoded,
            'response_correlation': response_correlation,
        }


class MissingFundamentalDispatcher(FakeDispatcher):
    def dispatch_batch(self, *, spec, market_date, batch_items):
        result = super().dispatch_batch(spec=spec, market_date=market_date, batch_items=batch_items)
        if spec.role == 'fundamental-summarizer':
            result['decoded']['items'] = []
        return result


class PromptCapturingDispatcher(FakeDispatcher):
    def __init__(self):
        self.client = OpenClawAgentClient()
        self.messages = []

    def dispatch_batch(self, *, spec, market_date, batch_items):
        self.messages.append((
            spec.role,
            self.client._build_batch_message(
                spec=spec,
                market_date=market_date,
                batch_items=batch_items,
                request_marker='us-test-marker',
            ),
        ))
        return super().dispatch_batch(spec=spec, market_date=market_date, batch_items=batch_items)


class SessionCapturingDispatcher(FakeDispatcher):
    def __init__(self):
        self.calls = []

    def dispatch_batch(self, *, spec, market_date, batch_items):
        self.calls.append({
            'role': spec.role,
            'batch_items': [dict(item) for item in batch_items],
        })
        return super().dispatch_batch(spec=spec, market_date=market_date, batch_items=batch_items)


class InnerIdentityMismatchDispatcher(FakeDispatcher):
    def dispatch_batch(self, *, spec, market_date, batch_items):
        result = super().dispatch_batch(spec=spec, market_date=market_date, batch_items=batch_items)
        result['decoded']['items'][0]['feedback'].update(
            {
                'symbol': 'MSFT',
                'market_date': '2026-08-18',
            }
        )
        return result


class AttributionMismatchDispatcher(FakeDispatcher):
    def dispatch_batch(self, *, spec, market_date, batch_items):
        result = super().dispatch_batch(spec=spec, market_date=market_date, batch_items=batch_items)
        result['response_correlation'] = {
            'attribution_status': 'mismatch',
            'failure_class': 'batch_response_attribution_mismatch',
            'duplicate_symbols': [batch_items[0]['symbol']],
        }
        return result


class MissingFeedbackDispatcher(FakeDispatcher):
    def dispatch_batch(self, *, spec, market_date, batch_items):
        result = super().dispatch_batch(
            spec=spec,
            market_date=market_date,
            batch_items=batch_items,
        )
        result['decoded']['items'][0]['feedback'] = None
        return result


class RecordingSessionCleanup:
    def __init__(self, *, error: str | None = None):
        self.calls = []
        self.error = error

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise RuntimeError(self.error)
        return {'status': 'deleted', 'session_key': kwargs['session_key']}


class USAgentAnalysisServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.default_cleanup_patcher = patch(
            'app.us_equities.agent_analysis_service.agent_feedback_orchestrator._cleanup_session_key',
            return_value={'status': 'deleted'},
        )
        self.default_cleanup_patcher.start()
        self.addCleanup(self.default_cleanup_patcher.stop)

    def test_ten_thousand_candidates_are_deterministically_bounded_by_role_caps(self):
        rows = [_candidate(index) for index in range(10_000)]
        service = USAgentAnalysisService(candidate_loader=lambda **kwargs: rows)

        prepared = service.prepare(market_date=date(2026, 8, 19), collection_run_id=77)

        self.assertEqual(prepared['counts'], US_AGENT_ROLE_CAPS)
        self.assertEqual(prepared['role_scopes']['signal-decider'][0], 'U00000')
        self.assertEqual(prepared['role_scopes']['signal-decider'][-1], 'U00079')
        context = prepared['contexts']['U00000']['symbol_day_context']
        self.assertEqual((context['market'], context['currency'], context['calendar_timezone']), (
            'US', 'USD', 'America/New_York'
        ))
        self.assertNotIn('KRX', str(context))

    def test_dispatches_and_normalizes_selected_role_without_krx_store(self):
        rows = [_candidate(0), _candidate(1)]
        service = USAgentAnalysisService(
            candidate_loader=lambda **kwargs: rows,
            dispatcher_getter=lambda: FakeDispatcher(),
        )

        result = service.dispatch(
            market_date=date(2026, 8, 19),
            collection_run_id=77,
            roles=['technical-summarizer'],
            persist=False,
            batch_size=2,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['completed'], 2)
        self.assertEqual(result['failed'], 0)

    def test_inner_identity_mismatch_is_rejected_before_us_feedback_persistence(self):
        service = USAgentAnalysisService(
            candidate_loader=lambda **kwargs: [_candidate(0)],
            dispatcher_getter=lambda: InnerIdentityMismatchDispatcher(),
        )

        with patch('app.us_equities.agent_analysis_service.upsert_us_agent_feedback') as persist_feedback:
            result = service.dispatch(
                market_date=date(2026, 8, 19),
                collection_run_id=77,
                roles=['technical-summarizer'],
                persist=True,
            )

        persist_feedback.assert_not_called()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['completed'], 0)
        self.assertEqual(result['failed'], 1)
        self.assertEqual(result['failures'][0]['error'], 'response_identity_mismatch:symbol_mismatch,market_date_mismatch')

    def test_batch_attribution_mismatch_is_rejected_before_us_feedback_persistence(self):
        service = USAgentAnalysisService(
            candidate_loader=lambda **kwargs: [_candidate(0)],
            dispatcher_getter=lambda: AttributionMismatchDispatcher(),
        )

        with patch('app.us_equities.agent_analysis_service.upsert_us_agent_feedback') as persist_feedback:
            result = service.dispatch(
                market_date=date(2026, 8, 19),
                collection_run_id=77,
                roles=['technical-summarizer'],
                persist=True,
            )

        persist_feedback.assert_not_called()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['completed'], 0)
        self.assertEqual(result['failed'], 1)
        self.assertEqual(result['failures'][0]['error'], 'batch_response_attribution_mismatch')

    def test_missing_feedback_object_rejects_entire_us_batch_before_persistence(self):
        service = USAgentAnalysisService(
            candidate_loader=lambda **kwargs: [_candidate(0), _candidate(1)],
            dispatcher_getter=lambda: MissingFeedbackDispatcher(),
        )

        with patch('app.us_equities.agent_analysis_service.upsert_us_agent_feedback') as persist_feedback:
            result = service.dispatch(
                market_date=date(2026, 8, 19),
                collection_run_id=77,
                roles=['technical-summarizer'],
                persist=True,
                batch_size=2,
            )

        persist_feedback.assert_not_called()
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['completed'], 0)
        self.assertEqual(result['failed'], 2)
        self.assertEqual(
            {failure['error'] for failure in result['failures']},
            {'response_contract_invalid:feedback_missing'},
        )

    def test_us_batches_clean_fresh_session_once_on_success_and_contract_failure(self):
        cases = (
            ('success', FakeDispatcher(), 'completed'),
            ('contract_failure', MissingFeedbackDispatcher(), 'failed'),
        )

        for label, dispatcher, expected_status in cases:
            with self.subTest(label=label):
                cleanup = RecordingSessionCleanup()
                service = USAgentAnalysisService(
                    candidate_loader=lambda **kwargs: [_candidate(0), _candidate(1)],
                    dispatcher_getter=lambda dispatcher=dispatcher: dispatcher,
                    session_cleanup=cleanup,
                )

                result = service.dispatch(
                    market_date=date(2026, 8, 19),
                    collection_run_id=77,
                    roles=['technical-summarizer'],
                    persist=False,
                    batch_size=2,
                )

                self.assertEqual(result['status'], expected_status)
                self.assertEqual(len(cleanup.calls), 1)
                self.assertEqual(
                    cleanup.calls[0]['session_key'],
                    result['session_cleanup'][0]['session_key'],
                )

    def test_us_cleanup_failure_is_audited_without_overwriting_dispatch_result(self):
        cleanup = RecordingSessionCleanup(error='cleanup audit store unavailable')
        service = USAgentAnalysisService(
            candidate_loader=lambda **kwargs: [_candidate(0)],
            dispatcher_getter=lambda: FakeDispatcher(),
            session_cleanup=cleanup,
        )

        result = service.dispatch(
            market_date=date(2026, 8, 19),
            collection_run_id=77,
            roles=['technical-summarizer'],
            persist=False,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['completed'], 1)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(len(cleanup.calls), 1)
        self.assertEqual(result['session_cleanup'][0]['status'], 'deferred')
        self.assertEqual(
            result['session_cleanup'][0]['reason'],
            'fresh_batch_session_cleanup_failed',
        )
        self.assertEqual(
            result['session_cleanup'][0]['error'],
            'cleanup audit store unavailable',
        )

    def test_same_date_dispatches_use_distinct_fresh_per_batch_session_keys(self):
        dispatcher = SessionCapturingDispatcher()
        service = USAgentAnalysisService(
            candidate_loader=lambda **kwargs: [_candidate(0), _candidate(1)],
            dispatcher_getter=lambda: dispatcher,
        )

        first = service.dispatch(
            market_date=date(2026, 8, 19),
            collection_run_id=77,
            roles=['technical-summarizer'],
            persist=False,
            batch_size=1,
        )
        second = service.dispatch(
            market_date=date(2026, 8, 19),
            collection_run_id=77,
            roles=['technical-summarizer'],
            persist=False,
            batch_size=1,
        )

        first_calls, second_calls = dispatcher.calls[:2], dispatcher.calls[2:]
        first_keys = {call['batch_items'][0]['session_key'] for call in first_calls}
        second_keys = {call['batch_items'][0]['session_key'] for call in second_calls}
        self.assertTrue(first_keys.isdisjoint(second_keys))
        self.assertEqual(len(first_keys), 2)
        self.assertEqual(len(second_keys), 2)
        for result, calls in ((first, first_calls), (second, second_calls)):
            for batch_index, call in enumerate(calls, start=1):
                item = call['batch_items'][0]
                self.assertEqual(item['session_reuse_mode'], 'fresh_per_batch')
                self.assertIn(result['run_id'], item['session_key'])
                self.assertIn(f'/batch-{batch_index:03d}/', item['session_key'])
                self.assertRegex(item['session_key'], r'/[0-9a-f]{10}$')

    def test_decider_request_expands_required_upstream_roles(self):
        service = USAgentAnalysisService(candidate_loader=lambda **kwargs: [_candidate(0)])

        prepared = service.prepare(
            market_date=date(2026, 8, 19),
            collection_run_id=77,
            roles=['signal-decider'],
        )

        self.assertEqual(list(prepared['role_scopes']), [
            'eod-summarizer',
            'technical-summarizer',
            'fundamental-summarizer',
            'signal-decider',
        ])

    def test_decider_is_blocked_when_upstream_feedback_is_missing(self):
        service = USAgentAnalysisService(
            candidate_loader=lambda **kwargs: [_candidate(0)],
            dispatcher_getter=lambda: MissingFundamentalDispatcher(),
        )

        result = service.dispatch(
            market_date=date(2026, 8, 19),
            collection_run_id=77,
            roles=['signal-decider'],
            persist=False,
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['completed'], 2)
        self.assertEqual(result['failed'], 2)
        self.assertTrue(any(
            item['role'] == 'signal-decider'
            and item['error'] == 'missing_upstream_feedback:fundamental-summarizer'
            for item in result['failures']
        ))

    def test_final_prompts_preserve_canonical_us_metrics_for_every_agent_role(self):
        dispatcher = PromptCapturingDispatcher()
        service = USAgentAnalysisService(
            candidate_loader=lambda **kwargs: [_candidate(0), _candidate(1)],
            dispatcher_getter=lambda: dispatcher,
        )

        result = service.dispatch(
            market_date=date(2026, 8, 19),
            collection_run_id=77,
            roles=['signal-decider', 'short-term-decider'],
            persist=False,
            batch_size=2,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            {role for role, _ in dispatcher.messages},
            {
                'eod-summarizer',
                'technical-summarizer',
                'fundamental-summarizer',
                'short-term-decider',
                'signal-decider',
            },
        )
        for role, message in dispatcher.messages:
            for krw_scaled_example in ('10300', '10500', '10900', '9900', '9800'):
                self.assertNotIn(krw_scaled_example, message, role)
            if role == 'signal-decider':
                self.assertNotIn('"wait_reason": {"code": "stale_quote_orderbook"', message)
                self.assertIn('"wait_reason": {"code": "insufficient_risk_reward"', message)
            rendered_items = json.loads(message.rsplit('Assigned batch payload:\n', 1)[1])
            self.assertEqual(len(rendered_items), 2, role)
            context = rendered_items[0]['symbol_day_context']
            self.assertEqual(context['price_context']['close_price'], '10.25', role)
            self.assertEqual(context['technical_snapshot']['return_5d'], '0.04', role)
            self.assertEqual(context['technical_snapshot']['momentum_12m'], '0.34', role)
            self.assertEqual(context['technical_snapshot']['volatility_60d'], '0.32', role)
            self.assertEqual(context['technical_snapshot']['max_drawdown_120d'], '-0.18', role)
            self.assertEqual(
                context['liquidity_context']['turnover']['median_dollar_volume_20d'],
                '25000000',
                role,
            )
            highlights = context['fundamentals']['latest_snapshot']['highlights']
            self.assertEqual(highlights['revenue'], '1000', role)
            self.assertEqual(highlights['net_income'], '120', role)
            self.assertEqual(highlights['operating_cash_flow'], '150', role)
            self.assertEqual(highlights['total_assets'], '2400', role)
            self.assertEqual(highlights['total_liabilities'], '900', role)
            self.assertEqual(highlights['shares_outstanding_growth_yoy'], '0.015', role)
            self.assertEqual(context['context_provenance']['collection_run_id'], 77, role)
            phase_contract = context['market_data_phase_contract']
            self.assertEqual(phase_contract['strategy_phase'], 'post_close_next_open_planning', role)
            self.assertIn('prior_session_adjusted_eod_price_history', phase_contract['authoritative_inputs'])
            self.assertFalse(phase_contract['missing_intraday_is_automatic_wait_blocker'], role)
            intraday_quality = context['decider_data_quality_context']['blocks']['intraday_realtime']
            self.assertEqual(intraday_quality['status'], 'not_applicable', role)
            self.assertFalse(intraday_quality['decision_guidance']['blocking_by_default'], role)

    def test_dispatch_reports_durable_progress_after_each_bounded_batch(self):
        progress = []
        service = USAgentAnalysisService(
            candidate_loader=lambda **kwargs: [_candidate(0), _candidate(1)],
            dispatcher_getter=lambda: FakeDispatcher(),
        )

        result = service.dispatch(
            market_date=date(2026, 8, 19),
            collection_run_id=77,
            roles=['technical-summarizer'],
            persist=False,
            batch_size=1,
            progress_callback=progress.append,
        )

        self.assertEqual(result['total_batches'], 2)
        self.assertEqual([item['completed_batches'] for item in progress], [1, 2])
        self.assertEqual(progress[-1]['completed'], 2)


if __name__ == '__main__':
    unittest.main()
