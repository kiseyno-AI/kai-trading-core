import unittest

from app.agent_context.feedback_service import AgentContextFeedbackService


class FakeFeedbackRepository:
    def __init__(self):
        self.created = None
        self.list_args = None

    def create_feedback(self, **kwargs):
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
