import threading
import unittest
from datetime import date
from unittest.mock import patch

from app.agent_context.orchestrator import AgentFeedbackOrchestrator
from app.core.settings import settings


class FakeAgentContextReadService:
    def get_symbol_day_context(self, **kwargs):
        return {
            'items': [
                {
                    'symbol': symbol,
                    'market_date': kwargs['market_date'].isoformat(),
                }
                for symbol in kwargs['symbols']
            ],
        }


class SlotTrackingBatchClient:
    def __init__(self):
        self.lock = threading.Lock()
        self.active_by_session_key = {}
        self.overlaps = []
        self.started = []
        self.slot_two_followup_started = threading.Event()

    def dispatch_batch(self, *, spec, market_date, batch_items):
        session_key = str(batch_items[0]['session_key'])
        symbols = tuple(str(item['symbol']) for item in batch_items)
        with self.lock:
            self.started.append((session_key, symbols))
            active_count = self.active_by_session_key.get(session_key, 0) + 1
            self.active_by_session_key[session_key] = active_count
            if active_count > 1:
                self.overlaps.append((session_key, symbols))

        try:
            if session_key.endswith('/slot-02') and symbols == ('000004',):
                self.slot_two_followup_started.set()
            if session_key.endswith('/slot-01') and symbols == ('000001',):
                self.slot_two_followup_started.wait(timeout=0.5)
            return {
                'ok': True,
                'attempt': 1,
                'started_at': f'{market_date.isoformat()}T00:00:00+00:00',
                'completed_at': f'{market_date.isoformat()}T00:00:01+00:00',
                'session_ref': {'session_key': session_key},
                'decoded': {
                    'items': [
                        {
                            'symbol': symbol,
                            'feedback': {
                                'role': spec.role,
                                'summary': f'{symbol} ok',
                                'confidence': 0.5,
                                'risk_flags': [],
                                'details': {},
                                'improvement_notes': [],
                            },
                        }
                        for symbol in symbols
                    ],
                },
            }
        finally:
            with self.lock:
                self.active_by_session_key[session_key] -= 1


class AgentFeedbackOrchestratorTests(unittest.TestCase):
    def test_symbol_batch_refill_preserves_session_slot_affinity(self):
        orchestrator = AgentFeedbackOrchestrator()
        fake_client = SlotTrackingBatchClient()
        orchestrator.client = fake_client
        stored = []

        def fake_store_feedback(**kwargs):
            stored.append(kwargs)
            return {
                'id': len(stored),
                'agent_name': kwargs['agent_name'],
                'run_id': kwargs['run_id'],
            }

        orchestrator._store_feedback = fake_store_feedback

        with patch.object(settings, 'openclaw_agent_symbol_chunk_size', 1), patch.object(
            settings,
            'openclaw_agent_parallel_subtasks',
            2,
        ), patch(
            'app.agent_context.orchestrator.agent_context_read_service',
            FakeAgentContextReadService(),
        ):
            result = orchestrator.dispatch_for_symbols(
                market_date=date(2026, 4, 16),
                symbols=['000001', '000002', '000003', '000004'],
                roles=['eod-summarizer'],
                reuse_existing=False,
            )

        slot_one_key = 'feedback/eod-summarizer/slot-01'
        slot_two_key = 'feedback/eod-summarizer/slot-02'
        with fake_client.lock:
            started = list(fake_client.started)
            overlaps = list(fake_client.overlaps)

        self.assertEqual(result['summary']['status'], 'completed')
        self.assertEqual(result['summary']['completed'], 4)
        self.assertEqual(overlaps, [])
        self.assertLess(
            started.index((slot_two_key, ('000004',))),
            started.index((slot_one_key, ('000003',))),
        )


if __name__ == '__main__':
    unittest.main()
