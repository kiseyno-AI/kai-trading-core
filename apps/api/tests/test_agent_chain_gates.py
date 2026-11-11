"""Dependency rules between the daily agent stages, pinned before and after moving them."""

from __future__ import annotations

import unittest

from app.agent_context.run_modes import FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
from app.eod.agent_chain_gates import (
    empty_agent_scope_followup,
    finished_partial_feeds_downstream,
    summary_partial_dependency_is_proven,
)
from app.eod.daily_pipeline_outcomes import daily_pipeline_outcome_is_satisfied
from app.eod.summary_cutoff_contract import COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION

COMPLETED = ['000660', '005930']


def _outcome(status='partial', run_id='feedback-cycle-1'):
    return {'status': status, 'result': {'run_id': run_id}}


def _resolution(**overrides):
    base = {
        'ready': True,
        'knowledge_cutoff_at': '2026-10-19T13:03:13+00:00',
        'source_run_id': 'feedback-cycle-1',
        'requested_symbols': list(COMPLETED),
        'feedback_scope_mode': FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        'completed_scope_proof': {
            'schema_version': COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION,
            'completed_symbols': list(COMPLETED),
            'admitted_symbols': ['000660', '005930', '035720'],
        },
    }
    base.update(overrides)
    return base


class SummaryPartialDependencyProofTests(unittest.TestCase):
    """Resume-path rule: a partial summary run counts only with an exact lineage proof."""

    def test_partial_with_matching_lineage_is_proven(self):
        self.assertTrue(
            summary_partial_dependency_is_proven(
                outcome=_outcome(), cutoff_resolution=_resolution(), parent_symbols=None
            )
        )

    def test_anything_short_of_the_exact_lineage_is_not(self):
        cases = {
            'completed_status': (_outcome(status='completed'), _resolution()),
            'blocked_status': (_outcome(status='blocked'), _resolution()),
            'not_ready': (_outcome(), _resolution(ready=False)),
            'no_cutoff_time': (_outcome(), _resolution(knowledge_cutoff_at=None)),
            'other_run': (_outcome(run_id='feedback-cycle-2'), _resolution()),
            'no_symbols': (_outcome(), _resolution(requested_symbols=[])),
            'proof_mismatch': (
                _outcome(),
                _resolution(completed_scope_proof={
                    'schema_version': COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION,
                    'completed_symbols': ['000660'],
                    'admitted_symbols': ['000660', '005930'],
                }),
            ),
            'no_result': ({'status': 'partial'}, _resolution()),
        }
        for name, (outcome, resolution) in cases.items():
            with self.subTest(case=name):
                self.assertFalse(
                    summary_partial_dependency_is_proven(
                        outcome=outcome, cutoff_resolution=resolution, parent_symbols=None
                    )
                )



class FinishedPartialRuleTests(unittest.TestCase):
    """First-run rule: the caller resolves the status; only a finished partial run with output passes."""

    def _result(self, **summary):
        base = {'is_live': False, 'completed_symbols': 3, 'pending_symbols': 0, 'in_progress_symbols': 0}
        base.update(summary)
        return {'progress': {'summary': base}}

    def test_only_partial_status_is_considered(self):
        self.assertTrue(finished_partial_feeds_downstream(self._result(), status='partial'))
        for status in ('completed', 'blocked', 'failed', 'skipped'):
            with self.subTest(status=status):
                self.assertFalse(finished_partial_feeds_downstream(self._result(), status=status))

    def test_unfinished_or_empty_runs_do_not_pass(self):
        for summary in ({'is_live': True}, {'pending_symbols': 1}, {'in_progress_symbols': 2},
                        {'completed_symbols': 0}, {'completed_symbols': 'x'}):
            with self.subTest(summary=summary):
                self.assertFalse(finished_partial_feeds_downstream(self._result(**summary), status='partial'))
        self.assertFalse(finished_partial_feeds_downstream({}, status='partial'))


class EmptyAgentScopeTests(unittest.TestCase):
    def test_same_day_empty_candidate_scope_skips_decider_and_plan(self):
        market_date = '2026-11-10'
        summaries = {
            'status': 'skipped', 'reason': 'no_agent_feedback_eligible_symbols',
            'market_date': market_date, 'entry_economics_scope_empty': True,
        }
        decider = empty_agent_scope_followup(summaries, market_date=market_date)
        plan = empty_agent_scope_followup({'result': decider}, market_date=market_date)
        self.assertTrue(daily_pipeline_outcome_is_satisfied(decider))
        self.assertEqual(plan, decider)
        self.assertFalse(plan['triggered'])
        self.assertIsNone(empty_agent_scope_followup(summaries, market_date='2026-11-11'))
        self.assertIsNone(empty_agent_scope_followup({**summaries, 'entry_economics_scope_empty': False}, market_date=market_date))


if __name__ == '__main__':
    unittest.main()
