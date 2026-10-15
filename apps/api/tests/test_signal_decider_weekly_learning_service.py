from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import unittest

from sqlalchemy.exc import IntegrityError

from app.agent_context.signal_decider_weekly_learning_service import (
    SignalDeciderWeeklyLearningService,
)


def _ready_calibration_report() -> dict:
    return {
        'request': {
            'market_date_from': '2026-10-05',
            'market_date_to': '2026-10-11',
            'horizon_days': 5,
            'evaluation_version': 'signal_decider_eval_v1',
            'model_identity_cohort': 'signal-decider|openai/gpt-5.6-sol',
        },
        'review_boundary': {
            'report_version': 'signal_decider_calibration_report_v1',
            'manual_review_only': True,
            'auto_apply_prompt_changes': False,
            'auto_apply_policy_changes': False,
        },
        'summary': {
            'status': 'ready',
            'mature_computed_count': 42,
            'ready_for_operator_review': True,
            'coverage_gap_count': 0,
            'decision_action_counts': {'buy': 8, 'wait': 30, 'reject': 4},
            'grade_label_counts': {'good_buy': 6, 'bad_buy': 2, 'correct_wait': 19},
            'positive_outcome_rate': 0.714286,
            'negative_outcome_rate': 0.285714,
        },
        'group_summaries': {
            'decision_actions': {
                'items': [
                    {
                        'bucket_type': 'decision_actions',
                        'bucket_key': 'decision.action',
                        'bucket_value': 'buy',
                        'sample_count': 8,
                        'positive_rate': 0.75,
                        'negative_rate': 0.25,
                    }
                ]
            }
        },
        'calibration_hints': {
            'prompt_guidance': [
                {
                    'kind': 'prompt_guidance',
                    'title': 'Keep liquidity evidence explicit.',
                    'manual_review_only': True,
                    'auto_apply': False,
                }
            ],
            'scoring_guidance': [],
            'operator_review': [],
        },
        'rule_candidates': {
            'items': [
                {
                    'candidate_id': 'candidate-1',
                    'candidate_type': 'tighten_buy_gate',
                    'predicate': {
                        'schema_version': 'signal_decider_rule_predicate_v1',
                        'all': [
                            {
                                'field': 'decision.action',
                                'operator': 'eq',
                                'value': 'buy',
                            }
                        ],
                    },
                    'reason': 'Observed bad-buy cluster.',
                    'support_sample_count': 4,
                    'evaluated_sample_count': 6,
                    'negative_rate': 0.666667,
                    'activation_status': 'needs_replay',
                    'manual_review_only': True,
                    'active_policy': False,
                    'deterministic_gate_eligible': False,
                }
            ]
        },
    }


class FakeCalibrationService:
    def __init__(self, report: dict | None = None) -> None:
        self.report = deepcopy(report or _ready_calibration_report())
        self.calls: list[dict] = []

    def build_report(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        report = deepcopy(self.report)
        report['request'] = {
            **report.get('request', {}),
            'market_date_from': kwargs['market_date_from'].isoformat(),
            'market_date_to': kwargs['market_date_to'].isoformat(),
        }
        return report


class MaturityAwareCalibrationService(FakeCalibrationService):
    """Simulate pending h5 rows whenever the query reaches recent cohorts."""

    def build_report(self, **kwargs) -> dict:
        report = super().build_report(**kwargs)
        if kwargs['market_date_to'] > date(2026, 9, 20):
            report['summary']['coverage_gap_count'] = 5
            report['summary']['status'] = 'insufficient_sample'
            report['summary']['ready_for_operator_review'] = False
        return report


class FakeWeeklyLearningRepository:
    def __init__(self) -> None:
        self.rows_by_fingerprint: dict[str, dict] = {}
        self.next_id = 1
        self.actual_summary = {
            'status': 'eligible',
            'source_count': 3,
            'model_verified_count': 2,
            'eligible_count': 2,
            'excluded_count': 1,
            'win_count': 1,
            'loss_count': 1,
            'win_rate': 0.5,
            'total_realized_pnl_krw': '12000.00',
            'average_realized_return_pct': 0.4,
            'metric_basis': 'gross_before_transaction_costs',
            'model_identity_cohort': 'signal-decider|openai/gpt-5.6-sol',
            'exclusion_reason_counts': {'actual_lifecycle_not_closed': 1},
        }

    def summarize_actual_execution_outcomes(self, **_kwargs) -> dict:
        return deepcopy(self.actual_summary)

    def get_by_source_fingerprint(self, source_fingerprint: str) -> dict | None:
        row = self.rows_by_fingerprint.get(source_fingerprint)
        return deepcopy(row) if row else None

    def insert_artifact(self, item: dict) -> dict:
        stored = deepcopy(item)
        stored['id'] = self.next_id
        self.next_id += 1
        self.rows_by_fingerprint[stored['source_fingerprint']] = stored
        return deepcopy(stored)

    def list_published_candidates(self, *, limit: int = 100) -> list[dict]:
        rows = [
            row
            for row in self.rows_by_fingerprint.values()
            if row['publication_status'] == 'published'
        ]
        rows.sort(
            key=lambda row: (row['published_at'], row['id']),
            reverse=True,
        )
        return deepcopy(rows[:limit])


class RacingWeeklyLearningRepository(FakeWeeklyLearningRepository):
    def insert_artifact(self, item: dict) -> dict:
        super().insert_artifact(item)
        raise IntegrityError('concurrent source fingerprint', {}, Exception())


class SignalDeciderWeeklyLearningServiceTests(unittest.TestCase):
    def test_horizon_five_publish_uses_mature_prior_rolling_cohorts(self) -> None:
        calibration_service = MaturityAwareCalibrationService()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=calibration_service,
            repository=FakeWeeklyLearningRepository(),
            now=lambda: datetime(2026, 10, 10, 1, 30, tzinfo=timezone.utc),
        )

        result = service.publish(
            week_start=date(2026, 10, 5),
            week_end=date(2026, 10, 9),
            horizon_days=5,
            min_sample_size=30,
        )

        self.assertEqual(len(calibration_service.calls), 1)
        call = calibration_service.calls[0]
        self.assertEqual(call['market_date_to'], date(2026, 9, 20))
        self.assertEqual(call['market_date_from'], date(2026, 6, 22))
        request = result['artifact']['source_report']['request']
        self.assertEqual(request['market_date_from'], '2026-06-22')
        self.assertEqual(request['market_date_to'], '2026-09-20')
        self.assertLess(date.fromisoformat(request['market_date_to']), date(2026, 10, 5))
        self.assertEqual(result['status'], 'published')
        self.assertEqual(
            result['artifact']['training_window']['maturity_buffer_days'],
            15,
        )

    def test_publish_persists_versioned_advisory_context_and_is_idempotent(self) -> None:
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )

        first = service.publish(
            week_start=date(2026, 10, 5),
            week_end=date(2026, 10, 11),
            horizon_days=5,
            evaluation_version='signal_decider_eval_v1',
            min_sample_size=30,
            evidence_manifest={'source': 'weekly-trading-analysis', 'exec_report_sha256': 'a' * 64},
        )
        second = service.publish(
            week_start=date(2026, 10, 5),
            week_end=date(2026, 10, 11),
            horizon_days=5,
            evaluation_version='signal_decider_eval_v1',
            min_sample_size=30,
            evidence_manifest={'source': 'weekly-trading-analysis', 'exec_report_sha256': 'a' * 64},
        )

        self.assertEqual(first['status'], 'published')
        self.assertFalse(first['idempotent_replay'])
        self.assertTrue(second['idempotent_replay'])
        self.assertEqual(first['artifact']['artifact_version'], second['artifact']['artifact_version'])
        self.assertEqual(len(repository.rows_by_fingerprint), 1)
        criteria = first['artifact']['learning_context']['performance_criteria']
        self.assertEqual(criteria['source'], 'published_weekly_learning_artifact')
        self.assertEqual(criteria['actual_execution']['eligible_count'], 2)
        self.assertTrue(criteria['usage_policy']['model_guidance_eligible'])
        self.assertFalse(criteria['usage_policy']['deterministic_gate_eligible'])
        self.assertFalse(criteria['usage_policy']['allow_buy_authorization'])
        self.assertFalse(criteria['usage_policy']['auto_apply_policy_changes'])
        self.assertTrue(criteria['usage_policy']['manual_policy_activation_required'])
        self.assertEqual(len(first['artifact']['content_sha256']), 64)
        self.assertEqual(
            criteria['artifact']['source_fingerprint'],
            first['artifact']['source_fingerprint'],
        )

    def test_publish_idempotency_key_ignores_retry_run_metadata(self) -> None:
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )
        common_manifest = {
            'source': {
                'name': 'weekly-trading-analysis',
                'sha256': 'a' * 64,
            },
            'idempotency_key': 'weekly-learning:2026-10-05:2026-10-11:h5:v1:n30',
        }

        first = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            horizon_days=5,
            evaluation_version='signal_decider_eval_v1',
            min_sample_size=30,
            evidence_manifest={
                **common_manifest,
                'run_id': 'ops-run-first',
                'completed_at': '2026-10-12T01:25:00Z',
            },
        )
        retry = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            horizon_days=5,
            evaluation_version='signal_decider_eval_v1',
            min_sample_size=30,
            evidence_manifest={
                **common_manifest,
                'run_id': 'ops-run-retry',
                'completed_at': '2026-10-12T01:29:00Z',
            },
        )

        self.assertFalse(first['idempotent_replay'])
        self.assertTrue(retry['idempotent_replay'])
        self.assertEqual(len(repository.rows_by_fingerprint), 1)
        self.assertEqual(
            first['artifact']['source_fingerprint'],
            retry['artifact']['source_fingerprint'],
        )
        self.assertEqual(
            first['artifact']['evidence_manifest']['run_id'],
            'ops-run-first',
        )

    def test_corrected_evidence_sha_creates_a_new_immutable_artifact(self) -> None:
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )
        common_manifest = {
            'idempotency_key': 'weekly-learning:2026-10-05:2026-10-11:h5:v1:n30',
        }

        original = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            evidence_manifest={
                **common_manifest,
                'source': {'sha256': 'a' * 64},
            },
        )
        corrected = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            evidence_manifest={
                **common_manifest,
                'source': {'sha256': 'b' * 64},
            },
        )

        self.assertFalse(original['idempotent_replay'])
        self.assertFalse(corrected['idempotent_replay'])
        self.assertEqual(len(repository.rows_by_fingerprint), 2)
        self.assertNotEqual(
            original['artifact']['source_fingerprint'],
            corrected['artifact']['source_fingerprint'],
        )

    def test_ineligible_run_is_audited_but_never_returned_as_latest_published(self) -> None:
        report = _ready_calibration_report()
        report['summary']['mature_computed_count'] = 4
        report['summary']['status'] = 'insufficient_sample'
        report['summary']['ready_for_operator_review'] = False
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(report),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )

        result = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            min_sample_size=30,
        )

        self.assertEqual(result['status'], 'ineligible')
        self.assertFalse(result['artifact']['eligibility']['eligible'])
        self.assertIn('insufficient_verified_evaluation_sample', result['artifact']['eligibility']['reasons'])
        self.assertEqual(service.get_latest_published(), {'available': False, 'artifact': None})

    def test_latest_read_rejects_tampered_persisted_content(self) -> None:
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )
        service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            min_sample_size=30,
        )
        stored = next(iter(repository.rows_by_fingerprint.values()))
        stored['learning_context_payload']['performance_criteria']['sample_count'] = 999

        latest = service.get_latest_published()

        self.assertFalse(latest['available'])
        self.assertEqual(latest['reason'], 'artifact_content_hash_mismatch')

    def test_database_timezone_rendering_does_not_change_artifact_hash(self) -> None:
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )
        service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            min_sample_size=30,
        )
        stored = next(iter(repository.rows_by_fingerprint.values()))
        database_timezone = timezone(timedelta(hours=9))
        stored['generated_at'] = stored['generated_at'].astimezone(
            database_timezone
        )
        stored['published_at'] = stored['published_at'].astimezone(
            database_timezone
        )

        latest = service.get_latest_published(as_of_date='2026-10-13')

        self.assertTrue(latest['available'])
        self.assertEqual(latest['artifact']['generated_at'], '2026-10-12T01:30:00+00:00')
        self.assertEqual(latest['artifact']['published_at'], '2026-10-12T01:30:00+00:00')

    def test_latest_read_rejects_artifact_after_valid_through_date(self) -> None:
        repository = FakeWeeklyLearningRepository()
        clock = {'now': datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc)}
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: clock['now'],
        )
        published = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            min_sample_size=30,
        )
        self.assertEqual(published['artifact']['valid_through'], '2026-10-25')

        clock['now'] = datetime(2026, 10, 26, 0, 1, tzinfo=timezone.utc)
        latest = service.get_latest_published()

        self.assertFalse(latest['available'])
        self.assertTrue(latest['stale'])
        self.assertEqual(latest['reason'], 'artifact_stale')
        self.assertEqual(latest['artifact']['artifact_version'], published['artifact']['artifact_version'])

    def test_backfilled_stale_publish_does_not_shadow_current_valid_artifact(self) -> None:
        repository = FakeWeeklyLearningRepository()
        clock = {'now': datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc)}
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: clock['now'],
        )
        current = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            min_sample_size=30,
        )
        clock['now'] = datetime(2026, 10, 20, 1, 30, tzinfo=timezone.utc)
        backfill = service.publish(
            week_start='2026-09-01',
            week_end='2026-09-05',
            min_sample_size=30,
        )

        latest = service.get_latest_published(as_of_date='2026-10-20')

        self.assertEqual(backfill['status'], 'published')
        self.assertEqual(backfill['artifact']['valid_through'], '2026-09-19')
        self.assertTrue(latest['available'])
        self.assertEqual(
            latest['artifact']['artifact_version'],
            current['artifact']['artifact_version'],
        )

    def test_recent_backfill_does_not_shadow_newer_week_artifact(self) -> None:
        repository = FakeWeeklyLearningRepository()
        clock = {'now': datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc)}
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: clock['now'],
        )
        current = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            min_sample_size=30,
        )
        clock['now'] = datetime(2026, 10, 15, 1, 30, tzinfo=timezone.utc)
        backfill = service.publish(
            week_start='2026-09-28',
            week_end='2026-10-04',
            min_sample_size=30,
        )

        latest = service.get_latest_published(as_of_date='2026-10-16')

        self.assertEqual(backfill['status'], 'published')
        self.assertGreater(
            backfill['artifact']['published_at'],
            current['artifact']['published_at'],
        )
        self.assertTrue(latest['available'])
        self.assertEqual(latest['artifact']['week_end'], '2026-10-11')
        self.assertEqual(
            latest['artifact']['artifact_version'],
            current['artifact']['artifact_version'],
        )

    def test_historical_as_of_never_receives_future_published_learning(self) -> None:
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )
        service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            min_sample_size=30,
        )

        before_publication = service.get_latest_published(
            as_of_date='2026-10-11'
        )
        publication_day = service.get_latest_published(
            as_of_date='2026-10-12'
        )
        next_day = service.get_latest_published(as_of_date='2026-10-13')

        self.assertFalse(before_publication['available'])
        self.assertEqual(
            before_publication['reason'],
            'artifact_not_yet_effective',
        )
        self.assertFalse(publication_day['available'])
        self.assertIsNone(
            service.get_latest_performance_criteria(as_of_date='2026-10-12')
        )
        self.assertTrue(next_day['available'])
        self.assertEqual(next_day['artifact']['effective_from'], '2026-10-13')

    def test_default_latest_read_honors_effective_from(self) -> None:
        repository = FakeWeeklyLearningRepository()
        clock = {'now': datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc)}
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: clock['now'],
        )
        service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            min_sample_size=30,
        )

        publication_day = service.get_latest_published()
        self.assertFalse(publication_day['available'])
        self.assertEqual(
            publication_day['reason'],
            'artifact_not_yet_effective',
        )
        self.assertIsNone(service.get_latest_performance_criteria())

        clock['now'] = datetime(2026, 10, 13, 0, 1, tzinfo=timezone.utc)
        self.assertTrue(service.get_latest_published()['available'])
        self.assertIsNotNone(service.get_latest_performance_criteria())

    def test_concurrent_same_fingerprint_insert_returns_idempotent_replay(self) -> None:
        repository = RacingWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )

        result = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            min_sample_size=30,
            evidence_manifest={
                'idempotency_key': 'weekly:2026-10-05:2026-10-11:h5'
            },
        )

        self.assertTrue(result['idempotent_replay'])
        self.assertEqual(result['status'], 'published')
        self.assertEqual(len(repository.rows_by_fingerprint), 1)

    def test_non_finite_manifest_value_is_rejected(self) -> None:
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=FakeWeeklyLearningRepository(),
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )

        with self.assertRaisesRegex(ValueError, 'finite JSON numbers'):
            service.publish(
                week_start='2026-10-05',
                week_end='2026-10-11',
                evidence_manifest={'invalid_metric': float('nan')},
            )


if __name__ == '__main__':
    unittest.main()
