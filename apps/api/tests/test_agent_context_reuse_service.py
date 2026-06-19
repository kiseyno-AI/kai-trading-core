import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context.orchestrator_helpers import build_upstream_feedback_bundle
from app.agent_context.reuse_service import (
    FUNDAMENTAL_SUMMARY_REUSE_REASON,
    AgentContextReuseService,
    _extract_fingerprint_payload,
)
from app.agent_context.retry_service import AgentFeedbackRetryService


def _decision(*, should_dispatch: bool, reason: str, fingerprint: str = 'sha256:abc'):
    return SimpleNamespace(
        should_dispatch=should_dispatch,
        reason=reason,
        latest_feedback_id=7,
        latest_feedback_received_at='2026-04-15T01:00:00+00:00',
        latest_source_updated_at=None,
        latest_feedback={
            'id': 7,
            'market_date': '2026-04-15',
            'run_id': 'prior-run',
            'feedback_payload': {'summary': 'reusable fundamentals'},
            'received_at': '2026-04-15T01:00:00+00:00',
        },
        input_fingerprint=fingerprint,
        stored_input_fingerprint=fingerprint,
        input_fingerprint_payload={'fingerprint': fingerprint},
        stored_input_fingerprint_payload={'fingerprint': fingerprint},
        reuse_policy_version='fundamental_summary_reuse_policy_v1',
    )


class FundamentalReuseServiceTests(unittest.TestCase):
    def test_fundamental_reuse_decision_accepts_matching_fingerprint_across_dates(self):
        should_dispatch, reason = AgentContextReuseService._decide_fundamental_reuse(
            force_recompute=False,
            has_retryable_feedback_gap=False,
            input_fingerprint='sha256:same',
            stored_input_fingerprint='sha256:same',
            latest_source_updated_at=None,
            feedback_received_at=datetime(2026, 4, 15, 1, 0, tzinfo=timezone.utc),
        )

        self.assertFalse(should_dispatch)
        self.assertEqual(reason, FUNDAMENTAL_SUMMARY_REUSE_REASON)

    def test_fundamental_reuse_decision_dispatches_when_fingerprint_changed(self):
        should_dispatch, reason = AgentContextReuseService._decide_fundamental_reuse(
            force_recompute=False,
            has_retryable_feedback_gap=False,
            input_fingerprint='sha256:new',
            stored_input_fingerprint='sha256:old',
            latest_source_updated_at=None,
            feedback_received_at=datetime(2026, 4, 15, 1, 0, tzinfo=timezone.utc),
        )

        self.assertTrue(should_dispatch)
        self.assertEqual(reason, 'fundamental_input_fingerprint_changed')

    def test_extract_fingerprint_payload_reads_context_reuse_metadata(self):
        payload = _extract_fingerprint_payload(
            {
                'reuse_metadata': {
                    'input_fingerprint': {
                        'schema_version': 'fundamental_summary_input_fingerprint_v1',
                        'fingerprint': 'sha256:stored',
                    }
                }
            }
        )

        self.assertEqual(payload['fingerprint'], 'sha256:stored')

    def test_retry_snapshot_treats_reused_fundamental_as_completed_boundary(self):
        service = AgentFeedbackRetryService()
        decisions = {
            '005930': {
                'fundamental-summarizer': _decision(
                    should_dispatch=False,
                    reason=FUNDAMENTAL_SUMMARY_REUSE_REASON,
                )
            }
        }

        with patch.object(
            service,
            '_load_same_day_terminal_symbols',
            return_value=({}, {}),
        ):
            with patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value=decisions,
            ):
                snapshot = service._build_role_sequence_snapshot(
                    market_date=__import__('datetime').date.fromisoformat('2026-04-16'),
                    roles=('fundamental-summarizer',),
                    eligible_symbols=('005930',),
                )

        role_state = snapshot.role_states['fundamental-summarizer']
        self.assertEqual(role_state.completed_symbols, ('005930',))
        self.assertEqual(role_state.reused_symbols, ('005930',))
        self.assertEqual(role_state.boundary_pending_symbols, ())
        self.assertEqual(role_state.retry_pending_symbols, ())

    def test_decider_upstream_bundle_accepts_fingerprint_reused_prior_fundamental_only(self):
        upstream_summaries, upstream_traceability, missing_roles = build_upstream_feedback_bundle(
            symbol='005930',
            market_date=__import__('datetime').date.fromisoformat('2026-04-16'),
            buffered_lookup={},
            upstream_reuse_decisions={
                '005930': {
                    'fundamental-summarizer': _decision(
                        should_dispatch=False,
                        reason=FUNDAMENTAL_SUMMARY_REUSE_REASON,
                    ),
                    'technical-summarizer': _decision(
                        should_dispatch=False,
                        reason='reuse_latest_feedback',
                    ),
                }
            },
            upstream_summary_roles=('fundamental-summarizer', 'technical-summarizer'),
        )

        self.assertEqual(upstream_summaries['fundamental-summarizer']['summary'], 'reusable fundamentals')
        self.assertEqual(
            upstream_traceability['fundamental-summarizer']['source'],
            'persisted_feedback_fingerprint_reuse',
        )
        self.assertIn('technical-summarizer', missing_roles)


if __name__ == '__main__':
    unittest.main()
