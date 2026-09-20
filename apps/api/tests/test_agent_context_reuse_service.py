import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context.orchestrator_helpers import build_upstream_feedback_bundle
from app.agent_context.reuse_service import (
    FUNDAMENTAL_SUMMARY_REUSE_REASON,
    AgentContextReuseService,
    build_fundamental_input_fingerprint_payload_from_context,
    _extract_fingerprint_payload,
    _failure_supersedes_feedback,
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


class _EmptyScalars:
    def all(self):
        return []

    def first(self):
        return None


class _ReuseExecuteResult:
    def all(self):
        return [SimpleNamespace(id=101, symbol='005930')]

    def scalars(self):
        return _EmptyScalars()

    def scalar_one(self):
        return None


class _ReuseSession:
    def execute(self, _statement):
        return _ReuseExecuteResult()


class _TimestampResult:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value


class _TimestampSession:
    def __init__(self, value):
        self.value = value
        self.calls = 0

    def execute(self, _statement):
        self.calls += 1
        return _TimestampResult(self.value)


class _RowsResult:
    def __init__(self, rows=()):
        self._rows = list(rows)

    def all(self):
        return list(self._rows)

    def scalars(self):
        return _EmptyScalars()


class _CutoffReuseSession:
    def __init__(
        self,
        knowledge_cutoff_at='2026-04-16T06:30:00+00:00',
    ):
        feedback = SimpleNamespace(
            id=7,
            market_date=__import__('datetime').date.fromisoformat('2026-04-16'),
            symbol='CUTOFFTEST',
            agent_name='eod-summarizer',
            run_id='cutoff-reuse-run',
            summary_payload={},
            feedback_payload={'summary': 'reusable'},
            context_payload={},
            received_at=datetime(2026, 4, 16, 6, 32, tzinfo=timezone.utc),
        )
        run = SimpleNamespace(
            id=11,
            run_id='cutoff-reuse-run',
            started_at=datetime(2026, 4, 16, 6, 31, tzinfo=timezone.utc),
            request_payload=(
                {'knowledge_cutoff_at': knowledge_cutoff_at}
                if knowledge_cutoff_at is not None
                else {}
            ),
        )
        self._results = iter(
            (
                _RowsResult(),
                _RowsResult([feedback]),
                _RowsResult(),
                _RowsResult([run]),
            )
        )

    def execute(self, _statement):
        return next(self._results)


@contextmanager
def _reuse_session_scope():
    yield _ReuseSession()


@contextmanager
def _cutoff_reuse_session_scope():
    yield _CutoffReuseSession()


@contextmanager
def _missing_cutoff_reuse_session_scope():
    yield _CutoffReuseSession(knowledge_cutoff_at=None)


class FundamentalReuseServiceTests(unittest.TestCase):
    def test_context_fingerprint_hashes_visible_context_not_filter_literals(self):
        def build_context(
            *,
            fs_div: str,
            disclosures: list[dict[str, str]],
        ) -> dict[str, object]:
            return {
                'symbol': '005930',
                'fundamentals': {
                    'latest_snapshot': {
                        'statement': {'fs_div': fs_div},
                        'highlights': {
                            'revenue': {'current_amount': 100}
                        },
                    },
                    'history_summary': {'entries': 1},
                },
                'recent_disclosures': {'items': disclosures},
                'recent_dart_events': {
                    'status': 'available',
                    'items': disclosures,
                },
                'valuation_context': {'status': 'available'},
            }
        context = build_context(
            fs_div='CFS',
            disclosures=[{'rcept_no': '202604140001'}],
        )
        base_kwargs = {
            'market_date': '2026-04-14',
            'symbol': '005930',
            'context': context,
            'knowledge_cutoff_at': '2026-04-14T06:30:00+00:00',
        }

        cfs = build_fundamental_input_fingerprint_payload_from_context(
            **base_kwargs,
            fs_div='cfs',
            disclosures_limit=3,
        )
        ofs = build_fundamental_input_fingerprint_payload_from_context(
            **base_kwargs,
            fs_div='OFS',
            disclosures_limit=3,
        )
        wider_disclosure_window = (
            build_fundamental_input_fingerprint_payload_from_context(
                **base_kwargs,
                fs_div='CFS',
                disclosures_limit=5,
            )
        )
        later_cutoff = build_fundamental_input_fingerprint_payload_from_context(
            **{
                **base_kwargs,
                'knowledge_cutoff_at': '2026-04-14T07:30:00+00:00',
            },
            fs_div='CFS',
            disclosures_limit=3,
        )
        changed_fs_context = build_fundamental_input_fingerprint_payload_from_context(
            **{
                **base_kwargs,
                'context': build_context(
                    fs_div='OFS',
                    disclosures=[{'rcept_no': '202604140001'}],
                ),
            },
            fs_div='OFS',
            disclosures_limit=3,
        )
        changed_disclosure_context = (
            build_fundamental_input_fingerprint_payload_from_context(
                **{
                    **base_kwargs,
                    'context': build_context(
                        fs_div='CFS',
                        disclosures=[
                            {'rcept_no': '202604140001'},
                            {'rcept_no': '202604130001'},
                        ],
                    ),
                },
                fs_div='CFS',
                disclosures_limit=5,
            )
        )

        self.assertEqual(
            cfs['source_filters'],
            {
                'fs_div': 'CFS',
                'knowledge_cutoff_at': '2026-04-14T06:30:00+00:00',
                'disclosures_limit': 3,
            },
        )
        self.assertEqual(cfs['fingerprint'], ofs['fingerprint'])
        self.assertEqual(
            cfs['fingerprint'],
            wider_disclosure_window['fingerprint'],
        )
        self.assertEqual(cfs['fingerprint'], later_cutoff['fingerprint'])
        self.assertNotEqual(
            cfs['source_filters'],
            later_cutoff['source_filters'],
        )
        self.assertNotEqual(
            cfs['fingerprint'],
            changed_fs_context['fingerprint'],
        )
        self.assertNotEqual(
            cfs['fingerprint'],
            changed_disclosure_context['fingerprint'],
        )

    def test_reuse_decision_uses_materialized_fingerprint_without_db_rebuild(self):
        service = AgentContextReuseService()
        payload = {
            'schema_version': 'fundamental_summary_input_fingerprint_v2',
            'reuse_policy_version': 'fundamental_summary_reuse_policy_v2',
            'fingerprint': 'sha256:materialized',
            'input': {'symbol': '005930'},
        }

        with patch(
            'app.agent_context.reuse_service.session_scope',
            _reuse_session_scope,
        ):
            with patch.object(
                service,
                '_build_fundamental_input_fingerprint_payload',
                side_effect=AssertionError('unexpected DB fingerprint rebuild'),
            ) as db_rebuild:
                decision = service.build_latest_decisions(
                    market_date='2026-04-16',
                    symbols=['005930'],
                    roles=['fundamental-summarizer'],
                    fs_div='OFS',
                    disclosures_limit=2,
                    knowledge_cutoff_at='2026-04-16T06:30:00+00:00',
                    fundamental_input_fingerprint_payloads={
                        '005930': payload
                    },
                )['005930']['fundamental-summarizer']

        self.assertEqual(decision.input_fingerprint_payload, payload)
        self.assertEqual(decision.input_fingerprint, 'sha256:materialized')
        db_rebuild.assert_not_called()

    def test_db_fingerprint_builder_defaults_to_configured_fs_div(self):
        service = AgentContextReuseService()
        payload = {
            'schema_version': 'fundamental_summary_input_fingerprint_v2',
            'reuse_policy_version': 'fundamental_summary_reuse_policy_v2',
            'fingerprint': 'sha256:rebuilt',
            'input': {'symbol': '005930'},
        }

        with patch(
            'app.agent_context.reuse_service.settings',
            SimpleNamespace(dart_fundamentals_default_fs_div='ofs'),
        ):
            with patch(
                'app.agent_context.reuse_service.session_scope',
                _reuse_session_scope,
            ):
                with patch.object(
                    service,
                    '_build_fundamental_input_fingerprint_payload',
                    return_value=payload,
                ) as db_rebuild:
                    service.build_latest_decisions(
                        market_date='2026-04-16',
                        symbols=['005930'],
                        roles=['fundamental-summarizer'],
                    )

        self.assertEqual(db_rebuild.call_args.kwargs['fs_div'], 'OFS')

    def test_feedback_reuse_requires_exact_effective_cutoff_instant(self):
        service = AgentContextReuseService()
        symbol = 'CUTOFFTEST'
        role = 'eod-summarizer'
        market_date = __import__('datetime').date.fromisoformat('2026-04-16')

        with patch(
            'app.agent_context.reuse_service.session_scope',
            _cutoff_reuse_session_scope,
        ):
            equivalent = service.build_latest_decisions(
                market_date=market_date,
                symbols=[symbol],
                roles=[role],
                knowledge_cutoff_at='2026-04-16T15:30:00+09:00',
            )[symbol][role]
        self.assertFalse(equivalent.should_dispatch)
        self.assertEqual(equivalent.reason, 'reuse_latest_feedback')

        for mismatched_cutoff in (
            '2026-04-16T05:30:00+00:00',
            '2026-04-16T07:30:00+00:00',
        ):
            with self.subTest(mismatched_cutoff=mismatched_cutoff):
                with patch(
                    'app.agent_context.reuse_service.session_scope',
                    _cutoff_reuse_session_scope,
                ):
                    mismatch = service.build_latest_decisions(
                        market_date=market_date,
                        symbols=[symbol],
                        roles=[role],
                        knowledge_cutoff_at=mismatched_cutoff,
                    )[symbol][role]
                self.assertTrue(mismatch.should_dispatch)
                self.assertEqual(mismatch.reason, 'knowledge_cutoff_mismatch')

        with patch(
            'app.agent_context.reuse_service.session_scope',
            _missing_cutoff_reuse_session_scope,
        ):
            missing = service.build_latest_decisions(
                market_date=market_date,
                symbols=[symbol],
                roles=[role],
                knowledge_cutoff_at='2026-04-16T06:30:00+00:00',
            )[symbol][role]
        self.assertTrue(missing.should_dispatch)
        self.assertEqual(missing.reason, 'knowledge_cutoff_mismatch')

    def test_newer_run_success_supersedes_late_failure_from_older_run(self):
        earlier = datetime(2026, 4, 16, 5, 0, tzinfo=timezone.utc)
        later = datetime(2026, 4, 16, 6, 0, tzinfo=timezone.utc)
        feedback = SimpleNamespace(
            market_date=earlier.date(),
            run_id='newer-success-run',
            received_at=later,
        )
        failure = SimpleNamespace(
            market_date=earlier.date(),
            run_id='older-failure-run',
            received_at=later.replace(minute=30),
        )

        self.assertFalse(
            _failure_supersedes_feedback(
                feedback=feedback,
                failure=failure,
                run_order_by_id={
                    'older-failure-run': (earlier, 10),
                    'newer-success-run': (later, 20),
                },
            )
        )

    def test_newer_run_failure_supersedes_earlier_success(self):
        earlier = datetime(2026, 4, 16, 5, 0, tzinfo=timezone.utc)
        later = datetime(2026, 4, 16, 6, 0, tzinfo=timezone.utc)
        feedback = SimpleNamespace(
            market_date=earlier.date(),
            run_id='older-success-run',
            received_at=later.replace(minute=30),
        )
        failure = SimpleNamespace(
            market_date=earlier.date(),
            run_id='newer-failure-run',
            received_at=later,
        )

        self.assertTrue(
            _failure_supersedes_feedback(
                feedback=feedback,
                failure=failure,
                run_order_by_id={
                    'older-success-run': (earlier, 10),
                    'newer-failure-run': (later, 20),
                },
            )
        )

    def test_missing_run_lineage_keeps_equal_time_failure_fail_closed(self):
        observed_at = datetime(2026, 4, 16, 5, 0, tzinfo=timezone.utc)
        feedback = SimpleNamespace(
            market_date=observed_at.date(),
            run_id='unknown-success-run',
            received_at=observed_at,
        )
        failure = SimpleNamespace(
            market_date=observed_at.date(),
            run_id='unknown-failure-run',
            received_at=observed_at,
        )

        self.assertTrue(
            _failure_supersedes_feedback(
                feedback=feedback,
                failure=failure,
                run_order_by_id={},
            )
        )

    def test_both_deciders_track_newer_upstream_feedback(self):
        service = AgentContextReuseService()
        updated_at = datetime(2026, 4, 16, 5, 30, tzinfo=timezone.utc)

        for role in ('short-term-decider', 'signal-decider'):
            session = _TimestampSession(updated_at)
            result = service._get_latest_source_updated_at(
                session=session,
                role=role,
                market_date=__import__('datetime').date.fromisoformat(
                    '2026-04-16'
                ),
                symbol='005930',
                symbol_id=101,
            )

            self.assertEqual(result, updated_at)
            self.assertEqual(session.calls, 1)

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

    def test_build_latest_decisions_non_signal_role_does_not_raise_unbound_valid(self):
        service = AgentContextReuseService()

        with patch('app.agent_context.reuse_service.session_scope', _reuse_session_scope):
            decisions = service.build_latest_decisions(
                market_date='2026-04-16',
                symbols=['005930'],
                roles=['eod-summarizer'],
            )

        decision = decisions['005930']['eod-summarizer']
        self.assertTrue(decision.should_dispatch)
        self.assertEqual(decision.reason, 'missing_feedback')
        self.assertIsNone(decision.latest_source_updated_at)

    def test_build_latest_decisions_force_recompute_skips_feedback_lookup(self):
        service = AgentContextReuseService()

        with patch('app.agent_context.reuse_service.session_scope') as session_scope:
            decisions = service.build_latest_decisions(
                market_date='2026-04-16',
                symbols=['005930', '000660'],
                roles=['eod-summarizer', 'fundamental-summarizer'],
                force_recompute=True,
            )

        session_scope.assert_not_called()
        for symbol in ('005930', '000660'):
            for role in ('eod-summarizer', 'fundamental-summarizer'):
                decision = decisions[symbol][role]
                self.assertTrue(decision.should_dispatch)
                self.assertEqual(decision.reason, 'forced_recompute')
                self.assertIsNone(decision.latest_feedback_id)

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
            ) as build_latest_decisions:
                snapshot = service._build_role_sequence_snapshot(
                    market_date=__import__('datetime').date.fromisoformat('2026-04-16'),
                    roles=('fundamental-summarizer',),
                    eligible_symbols=('005930',),
                    knowledge_cutoff_at='2026-04-16T15:30:00+09:00',
                )

        self.assertEqual(
            build_latest_decisions.call_args.kwargs['knowledge_cutoff_at'],
            '2026-04-16T15:30:00+09:00',
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

    def test_decider_upstream_bundle_blocks_same_day_feedback_pending_refresh(self):
        decision = _decision(
            should_dispatch=True,
            reason='fundamental_input_fingerprint_changed',
        )
        decision.latest_feedback['market_date'] = '2026-04-15'

        upstream_summaries, upstream_traceability, missing_roles = build_upstream_feedback_bundle(
            symbol='005930',
            market_date=__import__('datetime').date.fromisoformat('2026-04-15'),
            buffered_lookup={},
            upstream_reuse_decisions={
                '005930': {
                    'fundamental-summarizer': decision,
                }
            },
            upstream_summary_roles=('fundamental-summarizer',),
        )

        self.assertEqual(missing_roles, ['fundamental-summarizer'])
        self.assertNotIn('fundamental-summarizer', upstream_summaries)
        self.assertNotIn('fundamental-summarizer', upstream_traceability)


if __name__ == '__main__':
    unittest.main()
