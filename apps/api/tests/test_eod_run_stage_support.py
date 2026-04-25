import unittest

from app.eod.run_stage_support import (
    build_stage_counts,
    extract_fundamentals_refresh_metrics,
    normalize_run_status,
    resolve_run_updated_at,
    select_preferred_eod_run,
)


class EODRunStageSupportTests(unittest.TestCase):
    def test_select_preferred_eod_run_skips_historical_backfill_when_daily_row_exists(self):
        historical_run = {
            'id': 10,
            'run_metadata': {'mode': 'historical_backfill'},
        }
        daily_run = {
            'id': 11,
            'run_metadata': {'mode': 'daily_auto'},
        }

        result = select_preferred_eod_run([historical_run, daily_run])

        self.assertEqual(result, daily_run)

    def test_build_stage_counts_defaults_processed_and_remaining_from_total(self):
        counts = build_stage_counts(total='5', succeeded='3', failed='1')

        self.assertEqual(counts.total, 5)
        self.assertEqual(counts.processed, 4)
        self.assertEqual(counts.succeeded, 3)
        self.assertEqual(counts.failed, 1)
        self.assertEqual(counts.remaining, 1)

    def test_extract_fundamentals_refresh_metrics_prefers_selected_symbols_and_combines_success_counts(self):
        run = {
            'symbols_total': 3579,
            'symbols_checked': 120,
            'symbols_fresh': 60,
            'symbols_refreshed': 15,
            'symbols_failed': 2,
            'events_detected': 440,
            'events_captured': 37,
            'events_failed': 1,
            'run_metadata': {
                'symbols_selected': 200,
            },
        }

        metrics = extract_fundamentals_refresh_metrics(run)

        self.assertEqual(metrics.total, 200)
        self.assertEqual(metrics.processed, 120)
        self.assertEqual(metrics.succeeded, 75)
        self.assertEqual(metrics.failed, 2)
        self.assertEqual(metrics.remaining, 80)
        self.assertEqual(metrics.fresh, 60)
        self.assertEqual(metrics.refreshed, 15)
        self.assertEqual(metrics.events_detected, 440)
        self.assertEqual(metrics.events_captured, 37)
        self.assertEqual(metrics.events_failed, 1)

    def test_normalize_run_status_and_resolve_run_updated_at_share_expected_mapping(self):
        run = {
            'started_at': '2026-04-21T09:00:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'last_progress_at': '2026-04-21T09:55:00+00:00',
            },
        }

        self.assertEqual(normalize_run_status('completed_with_errors'), 'partial')
        self.assertEqual(normalize_run_status('skipped'), 'pending')
        self.assertEqual(normalize_run_status('running'), 'running')
        self.assertEqual(resolve_run_updated_at(run), '2026-04-21T09:55:00+00:00')


if __name__ == '__main__':
    unittest.main()
