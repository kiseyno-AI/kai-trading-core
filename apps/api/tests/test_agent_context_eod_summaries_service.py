from datetime import date
import unittest

from app.agent_context.eod_summaries_service import AgentContextEodSummariesService


class FakeAgentContextEodSummariesStore:
    def __init__(self):
        self.list_args = None
        self.get_args = None

    def list_for_market_date(self, *, market_date):
        self.list_args = {'market_date': market_date}
        return [
            {
                'symbol': '005930',
                'market_date': market_date.isoformat(),
                'agent_name': 'eod-summarizer',
                'summary': 'Breadth improved into the close.',
                'risk_flags': ['narrow_leadership'],
            },
            {
                'symbol': '000660',
                'market_date': market_date.isoformat(),
                'agent_name': 'eod-summarizer',
                'summary': 'Momentum held up despite weaker breadth.',
                'risk_flags': [],
            },
        ]

    def get_for_symbol(self, *, market_date, symbol):
        self.get_args = {'market_date': market_date, 'symbol': symbol}
        if symbol == '005930':
            return {
                'symbol': symbol,
                'market_date': market_date.isoformat(),
                'agent_name': 'eod-summarizer',
                'summary': 'Breadth improved into the close.',
                'risk_flags': ['narrow_leadership'],
            }
        return None


class AgentContextEodSummariesServiceTests(unittest.TestCase):
    def test_inspect_returns_all_items_for_market_date(self):
        store = FakeAgentContextEodSummariesStore()
        service = AgentContextEodSummariesService(store=store)

        result = service.inspect(market_date='2026-04-21')

        self.assertEqual(store.list_args, {'market_date': date(2026, 4, 21)})
        self.assertEqual(result['market_date'], '2026-04-21')
        self.assertEqual(result['agent_name'], 'eod-summarizer')
        self.assertEqual(result['summary']['entries'], 2)
        self.assertEqual(result['summary']['matched_symbols'], ['005930', '000660'])
        self.assertEqual(result['summary']['risk_flagged_count'], 1)
        self.assertEqual(result['items'][0]['symbol'], '005930')

    def test_inspect_returns_single_item_for_requested_symbol(self):
        store = FakeAgentContextEodSummariesStore()
        service = AgentContextEodSummariesService(store=store)

        result = service.inspect(market_date='2026-04-21', symbol=' 005930 ')

        self.assertEqual(
            store.get_args,
            {'market_date': date(2026, 4, 21), 'symbol': '005930'},
        )
        self.assertEqual(result['summary']['requested_symbol'], '005930')
        self.assertEqual(result['summary']['missing_symbols'], [])
        self.assertEqual(result['item']['summary'], 'Breadth improved into the close.')

    def test_inspect_reports_missing_requested_symbol(self):
        store = FakeAgentContextEodSummariesStore()
        service = AgentContextEodSummariesService(store=store)

        result = service.inspect(market_date='2026-04-21', symbol='035420')

        self.assertEqual(result['summary']['entries'], 0)
        self.assertEqual(result['summary']['missing_symbols'], ['035420'])
        self.assertIsNone(result['item'])
