import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context import orchestrator as orchestrator_module
from app.agent_context.orchestrator import AgentFeedbackOrchestrator


def _batch_result(symbol: str) -> dict:
    return {
        'decoded': {
            'items': [
                {
                    'symbol': symbol,
                    'feedback': {
                        'role': 'eod-summarizer',
                        'summary': f'{symbol} summary',
                        'confidence': 0.7,
                        'risk_flags': [],
                        'details': {},
                        'improvement_notes': [],
                    },
                }
            ]
        },
        'attempt': 1,
        'started_at': '2026-04-14T01:00:00+00:00',
        'completed_at': '2026-04-14T01:01:00+00:00',
        'session_ref': {'session_key': 'feedback/eod-summarizer/slot-01'},
    }


class FakeFuture:
    def __init__(self, *, result, cancel_result):
        self._result = result
        self.cancel_result = cancel_result
        self.cancel_calls = 0

    def cancel(self):
        self.cancel_calls += 1
        return self.cancel_result

    def result(self):
        return self._result


class FakeThreadPoolExecutor:
    def __init__(self, futures):
        self.futures = list(futures)
        self.submissions = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def submit(self, fn, **kwargs):
        self.submissions.append(kwargs)
        return self.futures[len(self.submissions) - 1]


class FakeAgentContextReadService:
    def get_symbol_day_context(self, **kwargs):
        return {
            'items': [
                {'symbol': '005930'},
                {'symbol': '000660'},
            ]
        }


class FakeOpenClawClient:
    def dispatch_batch(self, **kwargs):
        raise AssertionError('fake executor should not call dispatch_batch')


class DynamicFakeAgentContextReadService:
    def get_symbol_day_context(self, **kwargs):
        return {'items': [{'symbol': symbol} for symbol in kwargs['symbols']]}


class RecordingAgentContextReadService:
    def __init__(self):
        self.calls = []

    def get_symbol_day_context(self, **kwargs):
        self.calls.append(kwargs)
        return {'items': [{'symbol': symbol} for symbol in kwargs['symbols']]}


class RecordingBatchClient:
    def __init__(self):
        self.calls = []
        self._lock = threading.Lock()

    def dispatch_batch(self, **kwargs):
        with self._lock:
            self.calls.append(kwargs)
        spec = kwargs['spec']
        return {
            'decoded': {
                'items': [
                    {
                        'symbol': item['symbol'],
                        'feedback': {
                            'role': spec.role,
                            'summary': f"{item['symbol']} {spec.role} summary",
                            'confidence': 0.7,
                            'risk_flags': [],
                            'details': {},
                            'improvement_notes': [],
                        },
                    }
                    for item in kwargs['batch_items']
                ]
            },
            'attempt': 1,
            'started_at': '2026-04-14T01:00:00+00:00',
            'completed_at': '2026-04-14T01:01:00+00:00',
            'session_ref': {'session_key': kwargs['batch_items'][0]['session_key']},
        }


class FakeFeedbackService:
    def __init__(self):
        self.bulk_items = []
        self.submitted = []

    def bulk_submit_feedback(self, *, items):
        self.bulk_items = list(items)
        return {'accepted': True, 'count': len(self.bulk_items), 'items': self.bulk_items}

    def submit_feedback(self, **kwargs):
        self.submitted.append(kwargs)
        return {
            'accepted': True,
            'item': {
                'id': len(self.submitted),
                'agent_name': kwargs['agent_name'],
                'run_id': kwargs['run_id'],
            },
        }


class FakeSessionCleanupService:
    def __init__(self):
        self.targets = []

    def delete_now(self, *, target):
        self.targets.append(target)
        return {'status': 'deleted', 'session_key': target.session_key}


def _reuse_decision(should_dispatch: bool):
    return SimpleNamespace(should_dispatch=should_dispatch)


class AgentFeedbackOrchestratorTests(unittest.TestCase):
    def test_reuse_existing_skips_reusable_symbol_role_pairs(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        read_service = RecordingAgentContextReadService()
        reuse_service = SimpleNamespace(
            build_latest_decisions=lambda **kwargs: {
                '005930': {'eod-summarizer': _reuse_decision(False)},
                '000660': {'eod-summarizer': _reuse_decision(True)},
            }
        )
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=20)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', read_service):
                with patch.object(orchestrator_module, 'agent_context_reuse_service', reuse_service):
                    with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                        with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                            result = service.dispatch_for_symbols(
                                market_date='2026-04-14',
                                symbols=['005930', '000660'],
                                roles=['eod-summarizer'],
                                reuse_existing=True,
                            )

        dispatched_symbols = [
            item['symbol']
            for call in client.calls
            for item in call['batch_items']
        ]
        self.assertEqual(read_service.calls[0]['symbols'], ['000660'])
        self.assertEqual(dispatched_symbols, ['000660'])
        self.assertEqual([item['symbol'] for item in feedback_service.bulk_items], ['000660'])
        self.assertEqual([target.symbol for target in cleanup_service.targets], ['000660'])
        self.assertEqual(result['summary']['completed'], 1)

    def test_buffered_symbol_dispatch_cleans_each_batch_session_once(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=2, openclaw_agent_parallel_subtasks=20)

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                        result = service.dispatch_for_symbols(
                            market_date='2026-04-14',
                            symbols=['005930', '000660'],
                            roles=['eod-summarizer'],
                            reuse_existing=False,
                        )

        self.assertEqual([item['symbol'] for item in feedback_service.bulk_items], ['005930', '000660'])
        self.assertEqual(len(cleanup_service.targets), 1)
        self.assertEqual(cleanup_service.targets[0].session_key, client.calls[0]['batch_items'][0]['session_key'])
        self.assertEqual(result['summary']['flushed_to_db'], 2)

    def test_symbol_dispatch_uses_configured_parallel_subtasks(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        fake_settings = SimpleNamespace(openclaw_agent_symbol_chunk_size=1, openclaw_agent_parallel_subtasks=3)
        symbols = ['000001', '000002', '000003', '000004', '000005', '000006']

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
                with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                    with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                        result = service.dispatch_for_symbols(
                            market_date='2026-04-14',
                            symbols=symbols,
                            roles=['eod-summarizer'],
                            reuse_existing=False,
                        )

        slots = sorted(call['batch_items'][0]['session_slot'] for call in client.calls)
        self.assertEqual(slots, [1, 1, 2, 2, 3, 3])
        self.assertEqual(result['request']['parallel_subtasks'], 3)
        self.assertEqual(result['request']['role_parallel_subtasks'], 3)
        self.assertEqual(result['summary']['completed'], 6)

    def test_subagent_dispatch_uses_requested_per_role_parallelism(self):
        service = AgentFeedbackOrchestrator()
        client = RecordingBatchClient()
        service.client = client
        feedback_service = FakeFeedbackService()
        cleanup_service = FakeSessionCleanupService()
        symbols = ['000001', '000002', '000003', '000004', '000005', '000006']

        with patch.object(orchestrator_module, 'agent_context_read_service', DynamicFakeAgentContextReadService()):
            with patch.object(orchestrator_module, 'agent_context_feedback_service', feedback_service):
                with patch.object(orchestrator_module, 'agent_feedback_session_cleanup_service', cleanup_service):
                    result = service.dispatch_for_symbols_via_subagents(
                        market_date='2026-04-14',
                        symbols=symbols,
                        roles=['eod-summarizer'],
                        per_role_parallelism=3,
                    )

        slots = sorted(
            call['batch_items'][0]['session_slot']
            for call in client.calls
        )
        self.assertEqual(slots, [1, 2, 3])
        self.assertEqual(result['request']['per_role_parallelism'], 3)
        self.assertEqual(result['request']['total_parallel_subtasks'], 3)
        self.assertEqual(result['summary']['completed'], 6)

    def test_timed_out_running_future_keeps_slot_until_it_completes(self):
        first_future = FakeFuture(result=_batch_result('005930'), cancel_result=False)
        second_future = FakeFuture(result=_batch_result('000660'), cancel_result=True)
        fake_executor = FakeThreadPoolExecutor([first_future, second_future])
        wait_calls = []

        def fake_wait(active_futures, *, timeout, return_when):
            wait_calls.append(list(active_futures))
            if len(wait_calls) == 1:
                return set(), set(active_futures)
            if len(wait_calls) == 2:
                self.assertEqual(list(active_futures), [first_future])
                return {first_future}, set()
            if len(wait_calls) == 3:
                self.assertEqual(list(active_futures), [second_future])
                return {second_future}, set()
            raise AssertionError('unexpected wait call')

        stored_symbols = []

        def fake_store_feedback(**kwargs):
            stored_symbols.append(kwargs['payload']['symbol'])
            return {'id': len(stored_symbols), 'agent_name': kwargs['agent_name'], 'run_id': kwargs['run_id']}

        service = AgentFeedbackOrchestrator()
        service.client = FakeOpenClawClient()
        service._store_feedback = fake_store_feedback

        fake_settings = SimpleNamespace(
            openclaw_agent_symbol_chunk_size=1,
            openclaw_agent_parallel_subtasks=1,
            openclaw_agent_timeout_seconds=1,
            openclaw_agent_retry_backoff_seconds=0,
            openclaw_agent_max_retries=0,
        )
        monotonic_values = iter([0, 32, 33])

        with patch.object(orchestrator_module, 'settings', fake_settings):
            with patch.object(orchestrator_module, 'agent_context_read_service', FakeAgentContextReadService()):
                with patch('app.agent_context.orchestrator.time.monotonic', side_effect=lambda: next(monotonic_values)):
                    with patch(
                        'app.agent_context.orchestrator.concurrent.futures.ThreadPoolExecutor',
                        side_effect=lambda max_workers: fake_executor,
                    ):
                        with patch('app.agent_context.orchestrator.concurrent.futures.wait', side_effect=fake_wait):
                            result = service.dispatch_for_symbols(
                                market_date='2026-04-14',
                                symbols=['005930', '000660'],
                                roles=['eod-summarizer'],
                                reuse_existing=False,
                            )

        self.assertEqual(first_future.cancel_calls, 1)
        self.assertEqual(
            [submission['batch_items'][0]['symbol'] for submission in fake_executor.submissions],
            ['005930', '000660'],
        )
        self.assertEqual(stored_symbols, ['005930', '000660'])
        self.assertEqual(result['summary']['failed'], 0)


if __name__ == '__main__':
    unittest.main()
