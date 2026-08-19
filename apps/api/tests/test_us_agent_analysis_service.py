import unittest
from datetime import date

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
        'rank': index + 1,
        'composite_score': '0.9',
        'metrics_payload': {'price': {'latest_price': '10.25'}, 'fundamental': {'ttm_revenue': '1000'}},
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


class USAgentAnalysisServiceTests(unittest.TestCase):
    def test_ten_thousand_candidates_are_deterministically_bounded_by_role_caps(self):
        rows = [_candidate(index) for index in range(10_000)]
        service = USAgentAnalysisService(candidate_loader=lambda **kwargs: rows)

        prepared = service.prepare(market_date=date(2026, 8, 19))

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


if __name__ == '__main__':
    unittest.main()
