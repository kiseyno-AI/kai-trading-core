import unittest
from copy import deepcopy
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from app.eod.run_stage_support import (
    admits_same_day_intraday_watch_continuation,
    build_next_open_window_expired_result,
    build_stage_counts,
    extract_fundamentals_refresh_metrics,
    is_exact_auto_pitr_intraday_watch_recovery_request,
    is_recoverable_same_day_intraday_watch_expiration,
    normalize_run_status,
    resolve_run_updated_at,
    select_preferred_eod_run,
)


class EODRunStageSupportTests(unittest.TestCase):
    def test_exact_auto_pitr_intraday_watch_recovery_request_is_narrow(self):
        request = {
            'trigger': 'auto_eod_followup_resume',
            'point_in_time_recovery': True,
            'reused_eod_collection_run_id': 581,
            'force': False,
            'reuse_existing_feedback': True,
            'resume_from_stage': 'signal_decider',
            'resolved_stages': [
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        }

        self.assertTrue(
            is_exact_auto_pitr_intraday_watch_recovery_request(**request)
        )

        invalid_values = {
            'trigger': 'stage_orchestrator',
            'point_in_time_recovery': False,
            'reused_eod_collection_run_id': 0,
            'force': True,
            'reuse_existing_feedback': False,
            'resume_from_stage': 'next_open_execution_plan',
            'resolved_stages': [
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        }
        for field, invalid_value in invalid_values.items():
            with self.subTest(field=field):
                self.assertFalse(
                    is_exact_auto_pitr_intraday_watch_recovery_request(
                        **{**request, field: invalid_value}
                    )
                )

    def test_same_day_open_expiration_is_recoverable_only_with_exact_evidence(
        self,
    ):
        stage_ids = (
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        )

        def expired_stage(stage_id: str) -> dict:
            return {
                'status': 'skipped',
                'processed': 0,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
                'remaining': 0,
                'source_run_ids': {},
                'details': {
                    'result_summary': {
                        'triggered': False,
                        'status': 'skipped',
                        'reason': 'next_open_window_expired',
                        'market_date': '2026-10-26',
                        'target_trade_date': '2026-10-27',
                        'current_market_date': '2026-10-27',
                        'market_state': 'OPEN',
                        'stage': stage_id,
                    }
                },
            }

        pipeline_run = {
            'market_date': '2026-10-26',
            'trigger': 'auto_eod_followup_resume',
            'status': 'skipped',
            'current_stage': 'signal_decider_evaluations',
            'completed_at': '2026-10-27T01:02:03+00:00',
            'error_message': None,
            'run_metadata': {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'point_in_time_recovery': True,
                'reused_eod_collection_run_id': 581,
                'force': False,
                'reuse_existing_feedback': True,
                'allow_expired_analysis_replay': False,
                'same_day_intraday_watch_continuation_admitted': False,
                'resume_from_stage': 'signal_decider',
                'resolved_stages': list(stage_ids),
            },
            'stages': {
                stage_id: expired_stage(stage_id) for stage_id in stage_ids
            },
        }

        self.assertTrue(
            is_recoverable_same_day_intraday_watch_expiration(
                pipeline_run,
                current_market_date=date(2026, 10, 27),
            )
        )

        invalid_runs = []
        wrong_status = deepcopy(pipeline_run)
        wrong_status['status'] = 'completed'
        invalid_runs.append(('status', wrong_status))
        wrong_current_stage = deepcopy(pipeline_run)
        wrong_current_stage['current_stage'] = 'next_open_execution_plan'
        invalid_runs.append(('current_stage', wrong_current_stage))
        unfinished = deepcopy(pipeline_run)
        unfinished['completed_at'] = None
        invalid_runs.append(('completed_at', unfinished))
        errored = deepcopy(pipeline_run)
        errored['error_message'] = 'unexpected failure'
        invalid_runs.append(('error_message', errored))
        wrong_trigger = deepcopy(pipeline_run)
        wrong_trigger['trigger'] = 'stage_orchestrator'
        invalid_runs.append(('trigger', wrong_trigger))
        wrong_entrypoint = deepcopy(pipeline_run)
        wrong_entrypoint['run_metadata']['entrypoint'] = 'forged.entrypoint'
        invalid_runs.append(('entrypoint', wrong_entrypoint))
        missing_lineage = deepcopy(pipeline_run)
        missing_lineage['run_metadata']['reused_eod_collection_run_id'] = 0
        invalid_runs.append(('reused_eod_collection_run_id', missing_lineage))
        forced = deepcopy(pipeline_run)
        forced['run_metadata']['force'] = True
        invalid_runs.append(('force', forced))
        analysis_replay = deepcopy(pipeline_run)
        analysis_replay['run_metadata']['allow_expired_analysis_replay'] = True
        invalid_runs.append(('allow_expired_analysis_replay', analysis_replay))
        already_admitted = deepcopy(pipeline_run)
        already_admitted['run_metadata'][
            'same_day_intraday_watch_continuation_admitted'
        ] = True
        invalid_runs.append(
            ('same_day_intraday_watch_continuation_admitted', already_admitted)
        )
        wrong_suffix = deepcopy(pipeline_run)
        wrong_suffix['run_metadata']['resolved_stages'] = [
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        invalid_runs.append(('resolved_stages', wrong_suffix))
        nonzero_work = deepcopy(pipeline_run)
        nonzero_work['stages']['signal_decider']['processed'] = 1
        invalid_runs.append(('processed', nonzero_work))
        sourced_work = deepcopy(pipeline_run)
        sourced_work['stages']['signal_decider']['source_run_ids'] = {
            'agent_feedback_run_id': 'feedback-run'
        }
        invalid_runs.append(('source_run_ids', sourced_work))
        closed = deepcopy(pipeline_run)
        closed['stages']['signal_decider']['details']['result_summary'][
            'market_state'
        ] = 'CLOSED'
        invalid_runs.append(('market_state', closed))
        future_target = deepcopy(pipeline_run)
        future_target['stages']['signal_decider']['details'][
            'result_summary'
        ]['target_trade_date'] = '2026-10-28'
        invalid_runs.append(('target_trade_date', future_target))

        for field, invalid_run in invalid_runs:
            with self.subTest(field=field):
                self.assertFalse(
                    is_recoverable_same_day_intraday_watch_expiration(
                        invalid_run,
                        current_market_date='2026-10-27',
                    )
                )

    def test_open_prior_date_next_open_window_is_skipped(self):
        market = SimpleNamespace(
            state='OPEN',
            server_time='2026-09-14T10:00:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.normalize_next_open_target_trade_date',
            return_value=date(2026, 9, 14),
        ):
            result = build_next_open_window_expired_result(
                market_date='2026-09-11',
                market=market,
            )

        self.assertEqual(
            result,
            {
                'triggered': False,
                'status': 'skipped',
                'reason': 'next_open_window_expired',
                'market_date': '2026-09-11',
                'target_trade_date': '2026-09-14',
                'current_market_date': '2026-09-14',
                'market_state': 'OPEN',
            },
        )

    def test_existing_recovery_preopen_admission_keeps_only_same_day_open_window_alive(self):
        preopen = SimpleNamespace(
            state='PREOPEN',
            server_time='2026-09-14T08:59:00+09:00',
        )
        opened = SimpleNamespace(
            state='OPEN',
            server_time='2026-09-14T09:01:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.'
            'normalize_next_open_target_trade_date',
            return_value=date(2026, 9, 14),
        ):
            admitted = admits_same_day_intraday_watch_continuation(
                market_date='2026-09-11',
                market=preopen,
                allow_existing_recovery_resume=True,
            )
            without_admission = build_next_open_window_expired_result(
                market_date='2026-09-11',
                market=opened,
            )
            with_admission = build_next_open_window_expired_result(
                market_date='2026-09-11',
                market=opened,
                allow_same_day_intraday_watch_continuation=admitted,
            )

        self.assertTrue(admitted)
        self.assertEqual(
            without_admission['reason'],
            'next_open_window_expired',
        )
        self.assertIsNone(with_admission)

    def test_new_recovery_is_not_admitted_during_live_session(self):
        for state, server_time in (
            ('PREOPEN', '2026-09-14T08:59:00+09:00'),
            ('OPEN', '2026-09-14T09:01:00+09:00'),
        ):
            with self.subTest(state=state):
                market = SimpleNamespace(
                    state=state,
                    server_time=server_time,
                )
                with patch(
                    'app.orders.next_open_execution.'
                    'normalize_next_open_target_trade_date',
                    return_value=date(2026, 9, 14),
                ):
                    admitted = admits_same_day_intraday_watch_continuation(
                        market_date='2026-09-11',
                        market=market,
                    )

                self.assertFalse(admitted)

    def test_same_day_admission_never_extends_past_market_close(self):
        closed = SimpleNamespace(
            state='CLOSED',
            server_time='2026-09-14T15:31:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.'
            'normalize_next_open_target_trade_date',
            return_value=date(2026, 9, 14),
        ):
            result = build_next_open_window_expired_result(
                market_date='2026-09-11',
                market=closed,
                allow_same_day_intraday_watch_continuation=True,
            )

        self.assertEqual(result['reason'], 'next_open_window_expired')

    def test_closed_current_target_next_open_window_is_skipped(self):
        market = SimpleNamespace(
            state='CLOSED',
            server_time='2026-09-14T16:00:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.normalize_next_open_target_trade_date',
            return_value=date(2026, 9, 14),
        ):
            result = build_next_open_window_expired_result(
                market_date='2026-09-11',
                market=market,
            )

        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'next_open_window_expired')

    def test_existing_recovery_preopen_current_target_remains_eligible(self):
        market = SimpleNamespace(
            state='PREOPEN',
            server_time='2026-09-14T08:00:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.normalize_next_open_target_trade_date',
            return_value=date(2026, 9, 14),
        ):
            result = build_next_open_window_expired_result(
                market_date='2026-09-11',
                market=market,
                allow_same_day_intraday_watch_continuation=True,
            )

        self.assertIsNone(result)

    def test_non_open_states_skip_targets_before_current_calendar_date(self):
        for market_state in ('PREOPEN', 'WEEKEND', 'HOLIDAY'):
            with self.subTest(market_state=market_state):
                market = SimpleNamespace(
                    state=market_state,
                    server_time='2026-09-14T08:00:00+09:00',
                )

                with patch(
                    'app.orders.next_open_execution.'
                    'normalize_next_open_target_trade_date',
                    return_value=date(2026, 9, 11),
                ):
                    result = build_next_open_window_expired_result(
                        market_date='2026-09-10',
                        market=market,
                    )

                self.assertEqual(result['status'], 'skipped')
                self.assertEqual(result['reason'], 'next_open_window_expired')
                self.assertEqual(result['market_state'], market_state)

    def test_open_future_target_next_open_window_remains_eligible(self):
        market = SimpleNamespace(
            state='OPEN',
            server_time='2026-09-14T10:00:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.normalize_next_open_target_trade_date',
            return_value=date(2026, 9, 15),
        ):
            result = build_next_open_window_expired_result(
                market_date='2026-09-14',
                market=market,
            )

        self.assertIsNone(result)

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
