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


class AgentFeedbackOrchestratorTests(unittest.TestCase):
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
            openclaw_agent_subtask_timeout_seconds=1,
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
