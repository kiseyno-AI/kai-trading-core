"""A finished partial agent stage with completed symbols still starts the next stage."""

from __future__ import annotations

import unittest

from app.eod.pykrx_pipeline_runner import _agent_followup_feeds_downstream


def _partial(**summary):
    base = {'is_live': False, 'completed_symbols': 101, 'pending_symbols': 0, 'in_progress_symbols': 0}
    base.update(summary)
    return {
        'triggered': True,
        'status': 'partial',
        'market_date': '2026-10-19',
        'progress': {'summary': base},
    }


class AgentFollowupFeedsDownstreamTests(unittest.TestCase):
    def test_finished_partial_with_completed_symbols_feeds_the_next_stage(self):
        # 2026-10-19: 101 of 108 symbols summarized, 7 failed after every retry.
        for stage_id in ('agent_feedback_summaries', 'signal_decider'):
            with self.subTest(stage_id=stage_id):
                self.assertTrue(_agent_followup_feeds_downstream(_partial(), stage_id=stage_id))

    def test_partial_still_running_or_with_nothing_completed_does_not(self):
        cases = {
            'live': _partial(is_live=True),
            'pending': _partial(pending_symbols=3),
            'in_flight': _partial(in_progress_symbols=1),
            'nothing_completed': _partial(completed_symbols=0),
            'no_progress': {'triggered': True, 'status': 'partial', 'market_date': '2026-10-19'},
        }
        for name, result in cases.items():
            with self.subTest(case=name):
                self.assertFalse(_agent_followup_feeds_downstream(result, stage_id='agent_feedback_summaries'))

    def test_blocked_failed_or_skipped_results_never_feed_downstream(self):
        for status in ('blocked', 'failed'):
            with self.subTest(status=status):
                result = {**_partial(), 'status': status}
                self.assertFalse(_agent_followup_feeds_downstream(result, stage_id='agent_feedback_summaries'))
        not_triggered = {**_partial(), 'triggered': False, 'status': 'blocked', 'reason': 'feedback_prerequisites_not_ready'}
        self.assertFalse(_agent_followup_feeds_downstream(not_triggered, stage_id='agent_feedback_summaries'))



class EnrichmentCoverageFloorTests(unittest.TestCase):
    def test_investor_flow_floor_matches_its_structural_coverage(self):
        from app.eod.pykrx_enrichment_coverage_policy import required_category_minimum_coverage_ratio

        # 2,648 of 2,762 symbols on 2026-10-20 must not block the day.
        self.assertLessEqual(required_category_minimum_coverage_ratio('investor_flow_symbol'), 2648 / 2762)
        self.assertEqual(required_category_minimum_coverage_ratio('sector_classification'), 0.99)


if __name__ == '__main__':
    unittest.main()
