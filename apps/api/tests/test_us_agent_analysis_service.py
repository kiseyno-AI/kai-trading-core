import json
import unittest
from datetime import date

from app.agent_context.orchestrator import OpenClawAgentClient
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
    def dispatch_batch(self, *, spec, market_date, batch_items):
        return {'decoded': {'items': [
            {
                'symbol': item['symbol'],
                'feedback': {
                    'role': spec.role,
                    'summary': f"{item['symbol']} summary",
                    'confidence': 0.8,
                    'risk_flags': [],
                    'details': {},
                    'improvement_notes': [],
                },
            }
            for item in batch_items
        ]}}


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


class USAgentAnalysisServiceTests(unittest.TestCase):
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
