from __future__ import annotations

import hashlib
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import patch

from app.eod.auto_pykrx import _scheduled_pipeline_result_status
from app.eod.pykrx_backfill import (
    MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE,
    PykrxBackfillService,
    PykrxSymbol,
)
from app.eod.pykrx_pipeline_runner import (
    AUTO_DAILY_TRACKED_STAGE_ORDER,
    _is_already_completed_followup_result,
)


@contextmanager
def _lock():
    yield True


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        current = cls(2026, 4, 3, 9, 4, tzinfo=timezone.utc)
        return current if tz is not None else current.replace(tzinfo=None)


class AutoPykrxSchedulerResultStatusTests(TestCase):
    def test_already_completed_promotes_only_exact_completed_existing_run(
        self,
    ) -> None:
        self.assertEqual(
            _scheduled_pipeline_result_status(
                {
                    'triggered': False,
                    'reason': 'already_completed',
                    'existing_run': {'status': 'completed'},
                }
            ),
            'completed',
        )

        for existing_status in ('partial', 'completed_with_errors'):
            with self.subTest(existing_status=existing_status):
                self.assertEqual(
                    _scheduled_pipeline_result_status(
                        {
                            'triggered': False,
                            'reason': 'already_completed',
                            'existing_run': {'status': existing_status},
                        }
                    ),
                    'skipped',
                )

    def test_followup_promotes_only_exact_completed_existing_run(self) -> None:
        self.assertTrue(
            _is_already_completed_followup_result(
                {
                    'triggered': False,
                    'reason': 'already_completed',
                    'existing_run': {'status': 'completed'},
                }
            )
        )

        for existing_status in ('partial', 'completed_with_errors'):
            with self.subTest(existing_status=existing_status):
                self.assertFalse(
                    _is_already_completed_followup_result(
                        {
                            'triggered': False,
                            'reason': 'already_completed',
                            'existing_run': {'status': existing_status},
                        }
                    )
                )


class PykrxAutomaticRetryPacingTests(TestCase):
    def test_automatic_price_retry_waits_five_minutes_from_latest_run_activity(
        self,
    ) -> None:
        service = PykrxBackfillService()
        retry_symbol = PykrxSymbol(
            symbol='005930',
            market='KOSPI',
            name_ko='Samsung Electronics',
        )

        for timestamp_fields in (
            {
                'started_at': '2026-04-03T08:55:00+00:00',
                'completed_at': '2026-04-03T09:00:00+00:00',
            },
            {'started_at': '2026-04-03T09:00:00+00:00'},
        ):
            latest_run = {
                'id': 7,
                'status': 'completed_with_errors',
                'symbols_failed': 1,
                'run_metadata': {'failed_symbols': ['005930']},
                **timestamp_fields,
            }
            with (
                self.subTest(timestamp_fields=timestamp_fields),
                patch.object(
                    service,
                    'resolve_latest_trade_date',
                    return_value='2026-04-03',
                ),
                patch(
                    'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                    return_value=_lock(),
                ),
                patch(
                    'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                    return_value=latest_run,
                ),
                patch(
                    'app.eod.pykrx_backfill.count_eod_collection_runs',
                    return_value=1,
                ),
                patch.object(
                    service,
                    '_failed_symbol_codes_for_run',
                    return_value=['005930'],
                ),
                patch.object(
                    service,
                    '_load_retry_symbols_for_run',
                    return_value=[retry_symbol],
                ),
                patch.object(
                    service,
                    '_run_market_data_pipeline',
                ) as run_collection,
                patch('app.eod.pykrx_backfill.datetime', _FixedDateTime),
            ):
                result = service.run_daily_pipeline_if_due(
                    market_date='2026-04-03',
                    trigger='auto_close_scheduler',
                )
                self.assertFalse(result['triggered'])
                self.assertEqual(result['reason'], 'auto_retry_backoff')
                self.assertEqual(result['retry_after_seconds'], 60)
                self.assertEqual(
                    result['auto_retry']['retry_after_seconds'],
                    60,
                )
                run_collection.assert_not_called()

    def test_five_price_attempts_with_two_failures_recovers_safe_downstream_scope(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_total': 4,
            'symbols_succeeded': 2,
            'symbols_failed': 2,
            'started_at': '2026-04-03T07:55:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'requested_symbols': [
                    '005930',
                    '000660',
                    '472220',
                    '121850',
                ],
                'failed_symbols': ['472220', '121850'],
                'unattempted_symbols': [],
                'downstream_retry_symbols': [
                    '005930',
                    '472220',
                    '000660',
                    '121850',
                ],
                'downstream_retry_resume_from_stage': 'pykrx_enrichment',
                'universe_snapshot': {
                    'source': 'pykrx',
                    'latest_trade_date': '2026-04-03',
                    'membership_basis': 'end_date_snapshot',
                    'point_in_time': True,
                    'market_membership': {
                        'schema_version': 'pykrx_market_membership_v1',
                        'membership_basis': 'end_date_snapshot',
                        'symbol_count': 4,
                        'markets': {
                            'KOSPI': {
                                'symbols': [
                                    '000660',
                                    '005930',
                                    '121850',
                                    '472220',
                                ],
                                'symbol_count': 4,
                                'membership_sha256': hashlib.sha256(
                                    b'000660\n005930\n121850\n472220'
                                ).hexdigest(),
                            },
                            'KOSDAQ': {
                                'symbols': [],
                                'symbol_count': 0,
                                'membership_sha256': hashlib.sha256(
                                    b''
                                ).hexdigest(),
                            },
                        },
                    }
                },
            },
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=latest_run,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.'
                'run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 92,
                    'status': 'completed',
                },
            ) as finalize_parent,
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ) as update_eod_run,
            patch.object(
                service,
                '_failed_symbol_codes_for_run',
            ) as load_failed_symbol_codes,
            patch.object(
                service,
                '_load_retry_symbols_for_run',
            ) as load_retry_symbols,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
            patch('app.eod.pykrx_backfill.datetime', _FixedDateTime),
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
                point_in_time_recovery=True,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(
            result['followup_recovery']['symbols'],
            ['005930', '000660'],
        )
        recovery_call = resume_stages.call_args.kwargs
        self.assertEqual(recovery_call['symbols'], ['005930', '000660'])
        self.assertNotIn('price_collection', recovery_call['stages'])
        self.assertIn('next_open_execution_plan', recovery_call['stages'])
        recovery_start_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(
            'pykrx_enrichment'
        )
        self.assertEqual(
            recovery_call['stages'],
            list(
                AUTO_DAILY_TRACKED_STAGE_ORDER[
                    recovery_start_index:
                ]
            ),
        )
        finalize_parent.assert_called_once_with(
            pipeline_run_id=92,
            allow_satisfied_terminal=True,
        )
        persisted_metadata = update_eod_run.call_args.kwargs['run_metadata']
        self.assertEqual(
            persisted_metadata['failed_symbols'],
            ['472220', '121850'],
        )
        self.assertEqual(persisted_metadata['downstream_retry_symbols'], [])
        self.assertEqual(update_eod_run.call_args.kwargs['symbols_failed'], 2)
        self.assertTrue(
            result['followup_recovery']['state_convergence'][
                'price_retry_pending'
            ]
        )
        load_failed_symbol_codes.assert_not_called()
        load_retry_symbols.assert_not_called()
        run_collection.assert_not_called()
