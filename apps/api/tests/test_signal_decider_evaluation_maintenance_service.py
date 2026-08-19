from __future__ import annotations

import unittest
from unittest.mock import patch

from app.agent_context.signal_decider_evaluation_maintenance_service import (
    SignalDeciderEvaluationMaintenanceService,
)


def _prerequisites(*, ready: bool = True, status: str = 'ready') -> dict:
    return {
        'market_date': '2026-05-08',
        'fs_div': 'CFS',
        'status': status,
        'ready': ready,
        'required_stage_ids': [],
        'ready_stage_ids': [],
        'waiting_stage_ids': [] if ready else ['market_risk_filter'],
        'blocked_stage_ids': [],
        'missing_prerequisites': [],
        'stages': {},
    }


class _FakeLoopService:
    def __init__(self, *, recent_result: dict | None = None, range_result: dict | None = None) -> None:
        self.recent_result = recent_result or {'summary': {}}
        self.range_result = range_result or {'summary': {}}
        self.recent_args = None
        self.range_args = None

    def compute_recent_evaluations(self, **kwargs):
        self.recent_args = kwargs
        return self.recent_result

    def compute_evaluations(self, **kwargs):
        self.range_args = kwargs
        return self.range_result


class _FakeCalendarService:
    def __init__(self) -> None:
        self.recent_args = None
        self.range_args = None

    def refresh_recent_reviews(self, **kwargs):
        self.recent_args = kwargs
        return {'summary': {'review_rows_upserted': 5}}

    def refresh_reviews(self, **kwargs):
        self.range_args = kwargs
        return {'summary': {'review_rows_upserted': 7}}


class _FakePrerequisiteService:
    def __init__(self, *, ready: bool = True, status: str = 'ready') -> None:
        self.payload = _prerequisites(ready=ready, status=status)

    def inspect_feedback_prerequisites(self, **kwargs):
        return dict(self.payload)


class SignalDeciderEvaluationMaintenanceServiceTests(unittest.TestCase):
    def test_refresh_recent_coverage_skips_when_prerequisites_are_not_ready(self) -> None:
        loop_service = _FakeLoopService()
        calendar_service = _FakeCalendarService()
        service = SignalDeciderEvaluationMaintenanceService(
            loop_service=loop_service,
            calendar_review_service=calendar_service,
            prerequisite_service=_FakePrerequisiteService(ready=False, status='waiting'),
        )

        result = service.refresh_recent_coverage(
            as_of_market_date='2026-05-08',
            lookback_days=14,
        )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['status'], 'waiting')
        self.assertEqual(result['reason'], 'feedback_prerequisites_not_ready')
        self.assertIsNone(loop_service.recent_args)
        self.assertIsNone(calendar_service.recent_args)

    def test_refresh_recent_coverage_passes_calendar_refresh_summary_without_recompute(self) -> None:
        evaluation_summary = {
            'computed_count': 8,
            'pending_maturity_count': 1,
            'insufficient_price_data_count': 0,
            'not_computed_count': 0,
        }
        loop_service = _FakeLoopService(recent_result={'summary': evaluation_summary})
        calendar_service = _FakeCalendarService()
        service = SignalDeciderEvaluationMaintenanceService(
            loop_service=loop_service,
            calendar_review_service=calendar_service,
            prerequisite_service=_FakePrerequisiteService(),
        )

        result = service.refresh_recent_coverage(
            as_of_market_date='2026-05-08',
            lookback_days=14,
            horizons=[1, 3, 5],
            primary_horizons=[5],
            refresh_calendar=True,
        )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['status'], 'completed')
        self.assertFalse(calendar_service.recent_args['refresh_evaluations'])
        self.assertEqual(calendar_service.recent_args['evaluation_refresh_summary'], evaluation_summary)
        self.assertEqual(result['summary']['calendar_refresh']['review_rows_upserted'], 5)

    def test_refresh_recent_coverage_can_limit_calendar_refresh_to_as_of_date(self) -> None:
        evaluation_summary = {'not_computed_count': 0}
        loop_service = _FakeLoopService(recent_result={'summary': evaluation_summary})
        calendar_service = _FakeCalendarService()
        service = SignalDeciderEvaluationMaintenanceService(
            loop_service=loop_service,
            calendar_review_service=calendar_service,
            prerequisite_service=_FakePrerequisiteService(),
        )

        result = service.refresh_recent_coverage(
            as_of_market_date='2026-05-08',
            lookback_days=30,
            refresh_calendar=True,
            calendar_lookback_days=0,
        )

        self.assertTrue(result['triggered'])
        self.assertIsNone(calendar_service.recent_args)
        self.assertEqual(calendar_service.range_args['market_date_from'].isoformat(), '2026-05-08')
        self.assertEqual(calendar_service.range_args['market_date_to'].isoformat(), '2026-05-08')
        self.assertEqual(calendar_service.range_args['lookback_days'], 0)
        self.assertFalse(calendar_service.range_args['refresh_evaluations'])
        self.assertEqual(result['request']['calendar_lookback_days'], 0)
        self.assertEqual(result['summary']['calendar_refresh']['review_rows_upserted'], 7)

    def test_repair_coverage_range_fails_when_mature_not_computed_remains(self) -> None:
        evaluation_summary = {
            'status': 'failed',
            'computed_count': 8,
            'pending_maturity_count': 1,
            'insufficient_price_data_count': 0,
            'not_computed_count': 2,
        }
        loop_service = _FakeLoopService(range_result={'summary': evaluation_summary})
        calendar_service = _FakeCalendarService()
        service = SignalDeciderEvaluationMaintenanceService(
            loop_service=loop_service,
            calendar_review_service=calendar_service,
            prerequisite_service=_FakePrerequisiteService(),
        )

        result = service.repair_coverage_range(
            market_date_from='2026-04-01',
            market_date_to='2026-04-30',
            horizons=[1, 3, 5],
            primary_horizons=[5],
            fail_on_mature_gaps=True,
        )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['summary']['not_computed_count'], 2)
        self.assertEqual(result['summary']['warning_count'], 1)
        self.assertEqual(result['summary']['calendar_refresh']['review_rows_upserted'], 7)
        self.assertEqual(
            result['warnings'],
            ['2 mature signal-decider horizon(s) remain not_computed after evaluation refresh.'],
        )
        self.assertFalse(calendar_service.range_args['refresh_evaluations'])
        self.assertEqual(calendar_service.range_args['evaluation_refresh_summary'], evaluation_summary)

    def test_refresh_latest_stored_coverage_uses_latest_persisted_trade_date(self) -> None:
        service = SignalDeciderEvaluationMaintenanceService(
            loop_service=_FakeLoopService(),
            calendar_review_service=_FakeCalendarService(),
            prerequisite_service=_FakePrerequisiteService(),
        )

        with (
            patch(
                'app.agent_context.signal_decider_evaluation_maintenance_service.load_latest_available_daily_price_trade_date',
                return_value='2026-05-08',
            ),
            patch.object(service, 'refresh_recent_coverage', return_value={'triggered': True}) as refresh_recent,
        ):
            result = service.refresh_latest_stored_coverage(trigger='startup')

        self.assertEqual(result, {'triggered': True})
        self.assertEqual(refresh_recent.call_args.kwargs['as_of_market_date'], '2026-05-08')
        self.assertEqual(refresh_recent.call_args.kwargs['trigger'], 'startup')
