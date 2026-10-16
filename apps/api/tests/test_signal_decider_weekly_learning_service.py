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


def _learning_quality(
    *,
    mode: str = 'performance_feedback',
    publication_eligible: bool = True,
    ineligible_reasons: list[str] | None = None,
    performance_status: str = 'ready',
    performance_consumer_eligible: bool = True,
    closed_trade_count: int = 1,
    cost_basis_coverage: str = 'complete',
    transaction_cost_coverage: str = 'complete',
    settlement_currency_coverage: str = 'complete',
    fill_decision_lineage_coverage: str = 'complete',
    closed_trade_decision_lineage_missing_count: int = 0,
    failure_status: str = 'no_data',
    failure_consumer_eligible: bool = False,
    execution_failure_count: int = 0,
    terminal_order_outcome_count: int = 0,
    lineage_linked_execution_failure_count: int = 0,
    lineage_missing_execution_failure_count: int = 0,
) -> dict:
    return {
        'schema_version': 'weekly-learning-quality/v1',
        'mode': mode,
        'publication_eligible': publication_eligible,
        'ineligible_reasons': list(ineligible_reasons or []),
        'performance': {
            'status': performance_status,
            'consumer_eligible': performance_consumer_eligible,
            'closed_trade_count': closed_trade_count,
            'cost_basis_coverage': cost_basis_coverage,
            'transaction_cost_coverage': transaction_cost_coverage,
            'settlement_currency_coverage': settlement_currency_coverage,
            'fill_decision_lineage_coverage': (
                fill_decision_lineage_coverage
            ),
            'closed_trade_decision_lineage_missing_count': (
                closed_trade_decision_lineage_missing_count
            ),
        },
        'failure_and_rejection': {
            'status': failure_status,
            'consumer_eligible': failure_consumer_eligible,
            'execution_failure_count': execution_failure_count,
            'terminal_order_outcome_count': terminal_order_outcome_count,
            'lineage_linked_execution_failure_count': (
                lineage_linked_execution_failure_count
            ),
            'lineage_missing_execution_failure_count': (
                lineage_missing_execution_failure_count
            ),
        },
    }


def _evidence_manifest(
    *,
    week_start: str = '2026-10-05',
    week_end: str = '2026-10-11',
    source_sha256: str = 'a' * 64,
    idempotency_key: str | None = None,
    learning_quality: dict | None = None,
) -> dict:
    manifest = {
        'schema_version': 'weekly-learning-evidence-manifest/v1',
        'evidence_schema_version': 'weekly-trade-review-v2',
        'week_start': week_start,
        'week_end': week_end,
        'source': {
            'sha256': source_sha256,
            'runtime_mode': 'live',
            'exec_git_commit': 'b' * 40,
        },
        'validation': {'status': 'valid'},
        'learning_quality': deepcopy(
            learning_quality if learning_quality is not None else _learning_quality()
        ),
    }
    if idempotency_key is not None:
        manifest['idempotency_key'] = idempotency_key
    return manifest


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
            evidence_manifest=_evidence_manifest(week_end='2026-10-09'),
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
            evidence_manifest=_evidence_manifest(),
        )
        second = service.publish(
            week_start=date(2026, 10, 5),
            week_end=date(2026, 10, 11),
            horizon_days=5,
            evaluation_version='signal_decider_eval_v1',
            min_sample_size=30,
            evidence_manifest=_evidence_manifest(),
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

    def test_exec_ineligible_performance_evidence_cannot_publish(self) -> None:
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )
        learning_quality = _learning_quality(
            mode='ineligible_evidence',
            publication_eligible=False,
            ineligible_reasons=['performance_feedback_ineligible'],
            performance_status='blocked',
            performance_consumer_eligible=False,
            closed_trade_count=1,
            transaction_cost_coverage='unknown',
        )
        manifest = _evidence_manifest(
            idempotency_key='weekly-learning-v2:2026-10-05:2026-10-11',
            learning_quality=learning_quality,
        )

        first = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            evidence_manifest={**manifest, 'run_id': 'first'},
        )
        retry = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            evidence_manifest={**manifest, 'run_id': 'retry'},
        )

        self.assertEqual(first['status'], 'ineligible')
        self.assertFalse(first['artifact']['eligibility']['eligible'])
        self.assertIn(
            'performance_feedback_ineligible',
            first['artifact']['eligibility']['reasons'],
        )
        self.assertIsNone(first['artifact']['published_at'])
        self.assertTrue(retry['idempotent_replay'])
        self.assertEqual(retry['status'], 'ineligible')
        self.assertEqual(len(repository.rows_by_fingerprint), 1)
        self.assertEqual(
            service.get_latest_published(),
            {'available': False, 'artifact': None},
        )

    def test_exec_ineligible_failure_evidence_cannot_publish(self) -> None:
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )
        learning_quality = _learning_quality(
            mode='ineligible_evidence',
            publication_eligible=False,
            ineligible_reasons=[
                'failure_and_rejection_feedback_ineligible'
            ],
            performance_status='no_data',
            performance_consumer_eligible=False,
            closed_trade_count=0,
            failure_status='truncated',
            failure_consumer_eligible=False,
            execution_failure_count=2,
            terminal_order_outcome_count=1,
            lineage_linked_execution_failure_count=1,
            lineage_missing_execution_failure_count=1,
        )

        result = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            evidence_manifest=_evidence_manifest(
                learning_quality=learning_quality,
            ),
        )

        self.assertEqual(result['status'], 'ineligible')
        self.assertFalse(result['artifact']['eligibility']['eligible'])
        self.assertIn(
            'failure_and_rejection_feedback_ineligible',
            result['artifact']['eligibility']['reasons'],
        )
        self.assertIsNone(result['artifact']['published_at'])
        self.assertEqual(
            result['artifact']['learning_context']['performance_criteria'][
                'actual_execution'
            ]['reason'],
            'exec_performance_feedback_no_data',
        )
        self.assertEqual(
            service.get_latest_published(),
            {'available': False, 'artifact': None},
        )

    def test_explicit_zero_data_calibration_only_publish_is_labeled(self) -> None:
        repository = FakeWeeklyLearningRepository()
        repository.actual_summary = {
            'status': 'unavailable',
            'source_count': 0,
            'model_verified_count': 0,
            'eligible_count': 0,
            'excluded_count': 0,
            'win_count': 0,
            'loss_count': 0,
            'win_rate': None,
            'total_realized_pnl_krw': '0.00',
            'average_realized_return_pct': None,
            'metric_basis': 'gross_before_transaction_costs',
            'model_identity_cohort': None,
            'exclusion_reason_counts': {},
        }
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )
        calibration_only = _learning_quality(
            mode='calibration_only',
            performance_status='no_data',
            performance_consumer_eligible=False,
            closed_trade_count=0,
        )

        result = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            evidence_manifest=_evidence_manifest(
                learning_quality=calibration_only,
            ),
        )

        self.assertEqual(result['status'], 'published')
        self.assertEqual(
            result['artifact']['eligibility']['publication_mode'],
            'calibration_only',
        )
        learning_context = result['artifact']['learning_context']
        self.assertEqual(learning_context['publication_mode'], 'calibration_only')
        self.assertFalse(learning_context['realized_execution_learning_included'])
        criteria = learning_context['performance_criteria']
        self.assertEqual(criteria['actual_execution']['status'], 'not_included')
        self.assertEqual(
            criteria['actual_execution']['reason'],
            'calibration_only_evidence_mode',
        )
        self.assertFalse(
            criteria['usage_policy']['realized_execution_learning_included']
        )

    def test_missing_learning_quality_contract_is_rejected(self) -> None:
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=FakeWeeklyLearningRepository(),
        )
        manifest = _evidence_manifest()
        del manifest['learning_quality']

        with self.assertRaisesRegex(
            ValueError,
            'evidence_manifest.learning_quality is required',
        ):
            service.publish(
                week_start='2026-10-05',
                week_end='2026-10-11',
                evidence_manifest=manifest,
            )

    def test_malformed_evidence_provenance_is_rejected(self) -> None:
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=FakeWeeklyLearningRepository(),
        )
        cases = (
            (
                'manifest schema',
                lambda manifest: manifest.update({'schema_version': 'legacy'}),
                'evidence_manifest.schema_version',
            ),
            (
                'evidence schema',
                lambda manifest: manifest.update(
                    {'evidence_schema_version': 'weekly-trade-review-v1'}
                ),
                'evidence_manifest.evidence_schema_version',
            ),
            (
                'window',
                lambda manifest: manifest.update({'week_end': '2026-10-10'}),
                'week_start/week_end must match',
            ),
            (
                'sha256',
                lambda manifest: manifest['source'].update(
                    {'sha256': 'A' * 64}
                ),
                'source.sha256 must be 64 lowercase hex',
            ),
            (
                'runtime mode',
                lambda manifest: manifest['source'].update(
                    {'runtime_mode': 'backtest'}
                ),
                'source.runtime_mode must be live or paper',
            ),
            (
                'commit',
                lambda manifest: manifest['source'].update(
                    {'exec_git_commit': 'not-a-commit'}
                ),
                'source.exec_git_commit must be 40 lowercase hex',
            ),
            (
                'validation',
                lambda manifest: manifest['validation'].update(
                    {'status': 'invalid'}
                ),
                'validation.status must be valid',
            ),
        )

        for label, mutate, pattern in cases:
            with self.subTest(label=label):
                manifest = _evidence_manifest()
                mutate(manifest)
                with self.assertRaisesRegex(ValueError, pattern):
                    service.publish(
                        week_start='2026-10-05',
                        week_end='2026-10-11',
                        evidence_manifest=manifest,
                    )

    def test_malformed_learning_quality_count_is_rejected(self) -> None:
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=FakeWeeklyLearningRepository(),
        )
        malformed = _learning_quality()
        malformed['performance']['closed_trade_count'] = True

        with self.assertRaisesRegex(
            ValueError,
            'closed_trade_count must be a non-negative integer',
        ):
            service.publish(
                week_start='2026-10-05',
                week_end='2026-10-11',
                evidence_manifest=_evidence_manifest(
                    learning_quality=malformed,
                ),
            )

    def test_performance_eligibility_requires_complete_quality_coverage(
        self,
    ) -> None:
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=FakeWeeklyLearningRepository(),
        )
        contradictory = _learning_quality(
            cost_basis_coverage='incomplete',
        )

        with self.assertRaisesRegex(
            ValueError,
            'performance status and consumer_eligible disagree with coverage',
        ):
            service.publish(
                week_start='2026-10-05',
                week_end='2026-10-11',
                evidence_manifest=_evidence_manifest(
                    learning_quality=contradictory,
                ),
            )

    def test_failure_eligibility_requires_complete_lineage_counts(self) -> None:
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=FakeWeeklyLearningRepository(),
        )
        contradictory = _learning_quality(
            mode='failure_feedback',
            performance_status='no_data',
            performance_consumer_eligible=False,
            closed_trade_count=0,
            failure_status='complete',
            failure_consumer_eligible=True,
            execution_failure_count=2,
            lineage_linked_execution_failure_count=1,
            lineage_missing_execution_failure_count=1,
        )

        with self.assertRaisesRegex(
            ValueError,
            'failure status and consumer_eligible disagree with lineage',
        ):
            service.publish(
                week_start='2026-10-05',
                week_end='2026-10-11',
                evidence_manifest=_evidence_manifest(
                    learning_quality=contradictory,
                ),
            )

    def test_failure_lineage_counts_must_partition_normalized_failures(
        self,
    ) -> None:
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=FakeWeeklyLearningRepository(),
        )
        contradictory = _learning_quality(
            mode='ineligible_evidence',
            publication_eligible=False,
            ineligible_reasons=[
                'failure_and_rejection_feedback_ineligible'
            ],
            performance_status='no_data',
            performance_consumer_eligible=False,
            closed_trade_count=0,
            failure_status='blocked',
            failure_consumer_eligible=False,
            execution_failure_count=2,
            lineage_linked_execution_failure_count=1,
            lineage_missing_execution_failure_count=0,
        )

        with self.assertRaisesRegex(
            ValueError,
            'failure lineage counts must sum to execution_failure_count',
        ):
            service.publish(
                week_start='2026-10-05',
                week_end='2026-10-11',
                evidence_manifest=_evidence_manifest(
                    learning_quality=contradictory,
                ),
            )

    def test_contradictory_calibration_only_mode_is_rejected(self) -> None:
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=FakeWeeklyLearningRepository(),
        )
        contradictory = _learning_quality(mode='calibration_only')

        with self.assertRaisesRegex(
            ValueError,
            'learning_quality.mode must be performance_feedback',
        ):
            service.publish(
                week_start='2026-10-05',
                week_end='2026-10-11',
                evidence_manifest=_evidence_manifest(
                    learning_quality=contradictory,
                ),
            )

    def test_terminal_outcomes_without_normalized_failures_cannot_be_eligible(
        self,
    ) -> None:
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=FakeWeeklyLearningRepository(),
        )
        contradictory = _learning_quality(
            mode='failure_feedback',
            performance_status='no_data',
            performance_consumer_eligible=False,
            closed_trade_count=0,
            failure_status='complete',
            failure_consumer_eligible=True,
            execution_failure_count=0,
            terminal_order_outcome_count=1,
        )

        with self.assertRaisesRegex(
            ValueError,
            'failure status and consumer_eligible disagree with lineage',
        ):
            service.publish(
                week_start='2026-10-05',
                week_end='2026-10-11',
                evidence_manifest=_evidence_manifest(
                    learning_quality=contradictory,
                ),
            )

    def test_same_evidence_identity_rejects_learning_quality_conflict(self) -> None:
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
        )
        key = 'weekly-learning-v2:2026-10-05:2026-10-11'
        service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            evidence_manifest=_evidence_manifest(idempotency_key=key),
        )
        failure_only = _learning_quality(
            mode='failure_feedback',
            performance_status='no_data',
            performance_consumer_eligible=False,
            closed_trade_count=0,
            failure_status='complete',
            failure_consumer_eligible=True,
            execution_failure_count=1,
            lineage_linked_execution_failure_count=1,
        )

        with self.assertRaisesRegex(
            ValueError,
            'idempotency conflict for learning_quality',
        ):
            service.publish(
                week_start='2026-10-05',
                week_end='2026-10-11',
                evidence_manifest=_evidence_manifest(
                    idempotency_key=key,
                    learning_quality=failure_only,
                ),
            )

    def test_publish_idempotency_key_ignores_retry_run_metadata(self) -> None:
        repository = FakeWeeklyLearningRepository()
        service = SignalDeciderWeeklyLearningService(
            calibration_service=FakeCalibrationService(),
            repository=repository,
            now=lambda: datetime(2026, 10, 12, 1, 30, tzinfo=timezone.utc),
        )
        common_manifest = _evidence_manifest(
            idempotency_key=(
                'weekly-learning:2026-10-05:2026-10-11:h5:v1:n30'
            )
        )

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
        common_manifest = _evidence_manifest(
            idempotency_key=(
                'weekly-learning:2026-10-05:2026-10-11:h5:v1:n30'
            )
        )

        original = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            evidence_manifest={
                **common_manifest,
                'source': {
                    **common_manifest['source'],
                    'sha256': 'a' * 64,
                },
            },
        )
        corrected = service.publish(
            week_start='2026-10-05',
            week_end='2026-10-11',
            evidence_manifest={
                **common_manifest,
                'source': {
                    **common_manifest['source'],
                    'sha256': 'b' * 64,
                },
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
            evidence_manifest=_evidence_manifest(),
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
            evidence_manifest=_evidence_manifest(),
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
            evidence_manifest=_evidence_manifest(),
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
            evidence_manifest=_evidence_manifest(),
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
            evidence_manifest=_evidence_manifest(),
        )
        clock['now'] = datetime(2026, 10, 20, 1, 30, tzinfo=timezone.utc)
        backfill = service.publish(
            week_start='2026-09-01',
            week_end='2026-09-05',
            min_sample_size=30,
            evidence_manifest=_evidence_manifest(
                week_start='2026-09-01',
                week_end='2026-09-05',
            ),
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
            evidence_manifest=_evidence_manifest(),
        )
        clock['now'] = datetime(2026, 10, 15, 1, 30, tzinfo=timezone.utc)
        backfill = service.publish(
            week_start='2026-09-28',
            week_end='2026-10-04',
            min_sample_size=30,
            evidence_manifest=_evidence_manifest(
                week_start='2026-09-28',
                week_end='2026-10-04',
            ),
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
            evidence_manifest=_evidence_manifest(),
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
            evidence_manifest=_evidence_manifest(),
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
            evidence_manifest=_evidence_manifest(
                idempotency_key='weekly:2026-10-05:2026-10-11:h5'
            ),
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
