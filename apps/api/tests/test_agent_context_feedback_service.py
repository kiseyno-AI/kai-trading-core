from datetime import date
import unittest

from app.agent_context.feedback_service import (
    AgentContextFeedbackService,
    _dedupe_feedback_items_for_upsert,
)


class FakeFeedbackRepository:
    def __init__(self):
        self.created = None
        self.create_calls = 0
        self.bulk_created = None
        self.list_args = None

    def create_feedback(self, **kwargs):
        self.create_calls += 1
        self.created = kwargs
        return {
            'id': 11,
            'market_date': kwargs['market_date'].isoformat(),
            'symbol': kwargs['symbol'],
            'agent': {
                'name': kwargs['agent_name'],
                'version': kwargs['agent_version'],
                'run_id': kwargs['run_id'],
            },
            'summary': kwargs['summary_payload'],
            'feedback': kwargs['feedback_payload'],
            'context': kwargs['context_payload'],
            'received_at': '2026-04-14T15:00:00+00:00',
        }

    def bulk_upsert_feedback(self, *, items, chunk_size=100):
        self.bulk_created = list(items)
        return [
            {
                'id': index,
                'market_date': item['market_date'].isoformat(),
                'symbol': item['symbol'],
                'agent': {
                    'name': item['agent_name'],
                    'version': item['agent_version'],
                    'run_id': item['run_id'],
                },
                'summary': item['summary_payload'],
                'feedback': item['feedback_payload'],
                'context': item['context_payload'],
                'received_at': '2026-04-14T15:00:00+00:00',
            }
            for index, item in enumerate(self.bulk_created, start=1)
        ]

    def list_feedback(self, **kwargs):
        self.list_args = kwargs
        return [
            {
                'id': 11,
                'market_date': kwargs['market_date'].isoformat(),
                'symbol': kwargs['symbols'][0],
                'agent': {'name': kwargs['agent_name'], 'version': None, 'run_id': 'run-123'},
                'summary': {'headline': 'watch breakout'},
                'feedback': {'confidence': 0.72},
                'context': None,
                'received_at': '2026-04-14T15:00:00+00:00',
            }
        ]


class AgentContextFeedbackServiceTests(unittest.TestCase):
    def test_dedupe_feedback_items_for_upsert_keeps_last_duplicate_per_conflict_key(self):
        items = [
            {'market_date': date(2026, 4, 14), 'symbol': '005930', 'agent_name': 'openclaw'},
            {'market_date': date(2026, 4, 14), 'symbol': '000660', 'agent_name': 'openclaw'},
            {'market_date': date(2026, 4, 14), 'symbol': '005930', 'agent_name': 'openclaw'},
            {'market_date': date(2026, 4, 14), 'symbol': '005930', 'agent_name': 'technical-summarizer'},
        ]

        deduped = _dedupe_feedback_items_for_upsert(items)

        self.assertEqual(
            [(item['symbol'], item['agent_name'], item.get('run_id')) for item in deduped],
            [
                ('000660', 'openclaw', None),
                ('005930', 'openclaw', None),
                ('005930', 'technical-summarizer', None),
            ],
        )

    def test_dedupe_feedback_items_for_upsert_leaves_unique_items_unchanged(self):
        items = [
            {'market_date': date(2026, 4, 14), 'symbol': '005930', 'agent_name': 'openclaw'},
            {'market_date': date(2026, 4, 14), 'symbol': '000660', 'agent_name': 'openclaw'},
            {'market_date': date(2026, 4, 14), 'symbol': '035420', 'agent_name': 'openclaw'},
        ]

        deduped = _dedupe_feedback_items_for_upsert(items)

        self.assertEqual([item['symbol'] for item in deduped], ['005930', '000660', '035420'])

    def test_submit_feedback_normalizes_and_persists_json_payload(self):
        repository = FakeFeedbackRepository()
        service = AgentContextFeedbackService(repository=repository)

        result = service.submit_feedback(
            market_date='2026-04-14',
            symbol=' 005930 ',
            agent_name=' OpenClaw ',
            agent_version=' daily-v1 ',
            run_id=' run-123 ',
            summary={'headline': 'watch breakout'},
            feedback={'confidence': 0.72},
            context={'source_route': '/agent-context/symbol-day'},
        )

        self.assertTrue(result['accepted'])
        self.assertEqual(repository.created['market_date'].isoformat(), '2026-04-14')
        self.assertEqual(repository.created['symbol'], '005930')
        self.assertEqual(repository.created['agent_name'], 'openclaw')
        self.assertEqual(repository.created['agent_version'], 'daily-v1')
        self.assertEqual(repository.created['run_id'], 'run-123')
        self.assertEqual(repository.created['summary_payload']['headline'], 'watch breakout')
        self.assertEqual(result['summary']['summary_keys'], ['headline'])
        self.assertEqual(result['summary']['feedback_keys'], ['confidence'])

    def test_submit_feedback_requires_summary_or_feedback(self):
        service = AgentContextFeedbackService(repository=FakeFeedbackRepository())

        with self.assertRaisesRegex(ValueError, 'Either summary or feedback must be provided'):
            service.submit_feedback(market_date='2026-04-14', symbol='005930', summary={}, feedback={})

    def test_bulk_submit_feedback_normalizes_and_uses_bulk_upsert(self):
        repository = FakeFeedbackRepository()
        service = AgentContextFeedbackService(repository=repository)

        result = service.bulk_submit_feedback(
            items=[
                {
                    'market_date': '2026-04-14',
                    'symbol': ' 005930 ',
                    'agent_name': ' OpenClaw ',
                    'agent_version': ' daily-v1 ',
                    'run_id': ' run-123 ',
                    'summary': {'headline': 'watch breakout'},
                    'feedback': {'confidence': 0.72},
                    'context': {'source_route': '/agent-context/symbol-day'},
                },
                {
                    'market_date': '2026-04-14',
                    'symbol': '000660',
                    'agent_name': 'openclaw',
                    'feedback': {'confidence': 0.55},
                },
            ]
        )

        self.assertEqual(repository.create_calls, 0)
        self.assertEqual(len(repository.bulk_created), 2)
        self.assertEqual(repository.bulk_created[0]['market_date'].isoformat(), '2026-04-14')
        self.assertEqual(repository.bulk_created[0]['symbol'], '005930')
        self.assertEqual(repository.bulk_created[0]['agent_name'], 'openclaw')
        self.assertEqual(repository.bulk_created[0]['agent_version'], 'daily-v1')
        self.assertEqual(repository.bulk_created[0]['run_id'], 'run-123')
        self.assertEqual(repository.bulk_created[0]['summary_payload']['headline'], 'watch breakout')
        self.assertEqual(repository.bulk_created[1]['summary_payload'], {})
        self.assertEqual(repository.bulk_created[1]['feedback_payload']['confidence'], 0.55)
        self.assertTrue(result['accepted'])
        self.assertEqual(result['count'], 2)
        self.assertEqual([item['symbol'] for item in result['items']], ['005930', '000660'])

    def test_list_feedback_normalizes_filters(self):
        repository = FakeFeedbackRepository()
        service = AgentContextFeedbackService(repository=repository)

        result = service.list_feedback(
            market_date='2026-04-14',
            symbols=['005930,000660', '005930'],
            agent_name=' OpenClaw ',
            limit=10,
        )

        self.assertEqual(repository.list_args['market_date'].isoformat(), '2026-04-14')
        self.assertEqual(repository.list_args['symbols'], ['005930', '000660'])
        self.assertEqual(repository.list_args['agent_name'], 'openclaw')
        self.assertEqual(repository.list_args['limit'], 10)
        self.assertEqual(result['request']['symbols'], ['005930', '000660'])
        self.assertEqual(result['summary']['count'], 1)
