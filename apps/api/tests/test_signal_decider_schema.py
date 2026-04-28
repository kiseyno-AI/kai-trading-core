from __future__ import annotations

from datetime import date, datetime, timezone
import unittest

from sqlalchemy import create_engine, event, inspect
from sqlalchemy.orm import Session

from app.db_models import (
    AgentContextFeedback,
    Base,
    SignalDeciderDecisionSnapshot,
    SignalDeciderEvaluation,
    SignalDeciderPolicyVersion,
    SignalDeciderRealizedOutcome,
)


def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA foreign_keys=ON')
    cursor.close()


def _feedback_row() -> AgentContextFeedback:
    return AgentContextFeedback(
        market_date=date(2026, 4, 28),
        symbol='005930',
        agent_name='signal-decider',
        agent_version='openclaw-v1',
        run_id='signal-run-1',
        summary_payload={'summary': 'buy setup'},
        feedback_payload={'preferred_action': 'buy'},
        normalized_feedback_payload={'preferred_action': 'buy'},
        context_payload={'signal_decider_trace': {'policy_version': 'signal_decider_policy_v1'}},
        received_at=datetime(2026, 4, 28, 6, 0, tzinfo=timezone.utc),
    )


def _snapshot_row(feedback_id: int) -> SignalDeciderDecisionSnapshot:
    return SignalDeciderDecisionSnapshot(
        feedback_id=feedback_id,
        market_date=date(2026, 4, 28),
        symbol='005930',
        agent_name='signal-decider',
        agent_version='openclaw-v1',
        decision_run_id='signal-run-1',
        feedback_received_at=datetime(2026, 4, 28, 6, 0, tzinfo=timezone.utc),
        policy_version='signal_decider_policy_v1',
        decision_action='buy',
        preferred_action='buy',
        next_day_open_action='buy',
        entry_price_basis='next_open',
        thesis_summary='Breakout continuation with manageable risk.',
        supporting_evidence_payload=['Breadth improved.'],
        decision_risks_payload=['Leadership remained narrow.'],
        invalidation_conditions_payload=['Close back below breakout level.'],
        data_quality_warnings_payload=[],
        specialist_disagreement_payload=[],
        traceability_payload={'agent_name': 'signal-decider'},
        input_feature_snapshot_payload={'technical': {'breakout_20d': True}},
        request_payload={'symbol': '005930'},
        response_payload={'preferred_action': 'buy'},
        response_summary_payload={'summary': 'buy setup'},
    )


def _outcome_row(snapshot_id: int, feedback_id: int) -> SignalDeciderRealizedOutcome:
    return SignalDeciderRealizedOutcome(
        decision_snapshot_id=snapshot_id,
        feedback_id=feedback_id,
        market_date=date(2026, 4, 28),
        symbol='005930',
        policy_version='signal_decider_policy_v1',
        decision_action='buy',
        horizon_days=5,
        outcome_status='complete',
        bars_observed=5,
        entry_price=100,
        exit_price=106,
        close_return_pct=6.0,
        target_hit=True,
        stop_loss_hit=False,
        target_stop_collision=False,
        diagnostics_payload={'source': 'test'},
        computed_at=datetime(2026, 4, 28, 6, 5, tzinfo=timezone.utc),
    )


def _evaluation_row(snapshot_id: int, feedback_id: int, realized_outcome_id: int) -> SignalDeciderEvaluation:
    return SignalDeciderEvaluation(
        decision_snapshot_id=snapshot_id,
        feedback_id=feedback_id,
        realized_outcome_id=realized_outcome_id,
        market_date=date(2026, 4, 28),
        symbol='005930',
        policy_version='signal_decider_policy_v1',
        decision_action='buy',
        horizon_days=5,
        evaluation_version='signal_decider_eval_v1',
        evaluation_status='complete',
        grade_label='good_buy',
        grade_score=0.9,
        missed_opportunity=False,
        rationale_payload={'summary': 'Target was reached.'},
        diagnostics_payload={'source': 'test'},
        evaluated_at=datetime(2026, 4, 28, 6, 10, tzinfo=timezone.utc),
    )


def _policy_version_row() -> SignalDeciderPolicyVersion:
    return SignalDeciderPolicyVersion(
        policy_version='signal_decider_policy_v2',
        status='candidate',
        parent_policy_version='signal_decider_policy_v1',
        source_candidate_version='signal_decider_policy_v2',
        change_summary='Relax wait decisions after repeated breakout evidence.',
        rule_bundle_payload={'schema_version': 'signal_decider_policy_bundle_v1'},
        metadata_payload={'activation_mode': 'manual_only', 'candidate_activation_required': True},
        created_by='system:auto_candidate_generation',
    )


class SignalDeciderSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine('sqlite+pysqlite:///:memory:')
        event.listen(self.engine, 'connect', _enable_sqlite_foreign_keys)
        Base.metadata.create_all(
            self.engine,
            tables=[
                AgentContextFeedback.__table__,
                SignalDeciderDecisionSnapshot.__table__,
                SignalDeciderRealizedOutcome.__table__,
                SignalDeciderEvaluation.__table__,
                SignalDeciderPolicyVersion.__table__,
            ],
        )

    def tearDown(self) -> None:
        Base.metadata.drop_all(
            self.engine,
            tables=[
                SignalDeciderEvaluation.__table__,
                SignalDeciderRealizedOutcome.__table__,
                SignalDeciderDecisionSnapshot.__table__,
                SignalDeciderPolicyVersion.__table__,
                AgentContextFeedback.__table__,
            ],
        )
        self.engine.dispose()

    def test_feedback_foreign_keys_use_set_null_for_snapshot_owned_rows(self) -> None:
        inspector = inspect(self.engine)

        outcome_feedback_fk = next(
            fk
            for fk in inspector.get_foreign_keys('signal_decider_realized_outcomes')
            if fk['constrained_columns'] == ['feedback_id']
        )
        evaluation_feedback_fk = next(
            fk
            for fk in inspector.get_foreign_keys('signal_decider_evaluations')
            if fk['constrained_columns'] == ['feedback_id']
        )

        self.assertEqual((outcome_feedback_fk.get('options') or {}).get('ondelete'), 'SET NULL')
        self.assertEqual((evaluation_feedback_fk.get('options') or {}).get('ondelete'), 'SET NULL')

    def test_deleting_feedback_preserves_snapshot_owned_outcomes_and_evaluations(self) -> None:
        with Session(self.engine) as session:
            feedback = _feedback_row()
            session.add(feedback)
            session.flush()

            snapshot = _snapshot_row(feedback.id)
            session.add(snapshot)
            session.flush()

            outcome = _outcome_row(snapshot.id, feedback.id)
            session.add(outcome)
            session.flush()

            evaluation = _evaluation_row(snapshot.id, feedback.id, outcome.id)
            session.add(evaluation)
            session.commit()

            snapshot_id = snapshot.id
            outcome_id = outcome.id
            evaluation_id = evaluation.id

            session.delete(feedback)
            session.commit()

        with Session(self.engine) as session:
            surviving_snapshot = session.get(SignalDeciderDecisionSnapshot, snapshot_id)
            surviving_outcome = session.get(SignalDeciderRealizedOutcome, outcome_id)
            surviving_evaluation = session.get(SignalDeciderEvaluation, evaluation_id)

            self.assertIsNotNone(surviving_snapshot)
            self.assertIsNotNone(surviving_outcome)
            self.assertIsNotNone(surviving_evaluation)
            self.assertIsNone(surviving_snapshot.feedback_id)
            self.assertIsNone(surviving_outcome.feedback_id)
            self.assertIsNone(surviving_evaluation.feedback_id)
            self.assertEqual(surviving_outcome.decision_snapshot_id, snapshot_id)
            self.assertEqual(surviving_evaluation.decision_snapshot_id, snapshot_id)
            self.assertEqual(surviving_evaluation.realized_outcome_id, outcome_id)

    def test_policy_version_registry_persists_manual_activation_metadata(self) -> None:
        with Session(self.engine) as session:
            policy_version = _policy_version_row()
            session.add(policy_version)
            session.commit()

            persisted = session.query(SignalDeciderPolicyVersion).filter_by(policy_version='signal_decider_policy_v2').one()
            persisted.status = 'active'
            persisted.activated_by = 'human.reviewer'
            persisted.metadata_payload = {
                **persisted.metadata_payload,
                'activation_mode': 'manual_only',
                'candidate_activation_required': True,
            }
            session.commit()

        with Session(self.engine) as session:
            reloaded = session.query(SignalDeciderPolicyVersion).filter_by(policy_version='signal_decider_policy_v2').one()
            self.assertEqual(reloaded.status, 'active')
            self.assertEqual(reloaded.parent_policy_version, 'signal_decider_policy_v1')
            self.assertEqual(reloaded.activated_by, 'human.reviewer')
            self.assertEqual(reloaded.metadata_payload['activation_mode'], 'manual_only')
