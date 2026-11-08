from __future__ import annotations

from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy import select

from app.eod import eod581_feedback_lineage_repair as repair


def _empty_incident(**overrides):
    values = {
        'eod_run': None,
        'pipeline_runs': (),
        'stage_runs': (),
        'feedback_runs': (),
        'direct_children': (),
        'evidence_feedback': (),
        'evidence_snapshots': (),
        'evidence_entries': (),
        'active_eod_ids': (),
        'active_pipeline_ids': (),
        'active_feedback_ids': (),
    }
    values.update(overrides)
    return repair.LockedIncident(**values)


class _FakeSession:
    def __init__(self) -> None:
        self.statements: list[tuple[object, object]] = []
        self.flush_count = 0

    def get_bind(self):
        return SimpleNamespace(dialect=SimpleNamespace(name='postgresql'))

    def execute(self, statement, parameters=None):
        self.statements.append((statement, parameters))
        return SimpleNamespace(scalar_one=lambda: True)

    def flush(self) -> None:
        self.flush_count += 1


class _SequencedResult:
    def __init__(self, value) -> None:
        self.value = value

    def scalar_one_or_none(self):
        return self.value

    def scalars(self):
        return iter(self.value)


class _QueryCaptureSession:
    def __init__(self, outcomes) -> None:
        self.outcomes = list(outcomes)
        self.statements = []

    def execute(self, statement, parameters=None):
        assert parameters is None
        self.statements.append(statement)
        return _SequencedResult(self.outcomes.pop(0))


class _TransactionalFakeSession(_FakeSession):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events
        self.commit_count = 0
        self.rollback_count = 0
        self.close_count = 0

    def flush(self) -> None:
        super().flush()
        self.events.append('flush')

    def commit(self) -> None:
        self.commit_count += 1
        self.events.append('commit')

    def rollback(self) -> None:
        self.rollback_count += 1
        self.events.append('rollback')

    def close(self) -> None:
        self.close_count += 1
        self.events.append('close')


def _session_scope_for(session):
    @contextmanager
    def fake_session_scope(**_kwargs):
        yield session

    return fake_session_scope


def test_manifest_is_exactly_bounded_to_reviewed_one_hop_incident() -> None:
    assert [spec.id for spec in repair.FEEDBACK_RUN_SPECS] == [
        7173,
        7175,
        7176,
        7177,
        7179,
        7180,
        7181,
        7182,
        7183,
        7184,
        7185,
        7186,
        7187,
        7188,
        7189,
        7190,
        7191,
        7193,
        7194,
    ]
    assert repair.PARENT_RUN_IDS == (
        'feedback-cycle-2026-10-27-e60e026357',
        'feedback-cycle-2026-10-27-f0ff7f7141',
        'feedback-cycle-2026-10-27-61b363a8a0',
    )
    assert repair.PLAN['lineage_boundary']['recursive_ancestor_expansion'] is False
    assert repair.PLAN['reuse_evidence'] == {
        'count': 83,
        'sha256': 'fbd2231d84a04aeed42b9a3eb5fe5c1c6f0dcc8576b7710d521a0f8bfa5eacc5',
        'observation_cutoff': '2026-10-27T23:28:35.193940+00:00',
        'observation_cutoff_source': 'feedback_run_7191.started_at',
    }
    assert repair.PLAN['apply_preconditions'] == {
        'core_and_background_workers_offline': True,
        'port_8000_closed': True,
        'active_database_work_absent': True,
        'operator_attestation_required': True,
    }
    assert repair.PLAN_SHA256 == (
        '882fa6428531f64bc282645a7cf0ac3855d426d0b2318198d2c2e0829517ca01'
    )
    assert repair.CLOSURE_SHA256 == (
        'f49e3769fcccdce8a9f1dac9d888eab70aa261ae206a22b90b239a84580a5315'
    )


@pytest.mark.parametrize('value', [False, 0, 1, 'true', None, []])
def test_explicit_false_or_malformed_point_in_time_value_blocks(value) -> None:
    with pytest.raises(repair.RepairBlocked):
        repair._point_in_time_state(  # noqa: SLF001
            {'point_in_time_recovery': value},
            label='test',
        )


def test_active_incident_work_blocks_before_any_row_repair() -> None:
    incident = _empty_incident(active_feedback_ids=(9999,))

    with pytest.raises(repair.RepairBlocked, match='active 2026-10-27 work exists'):
        repair._validate_locked_incident(incident)  # noqa: SLF001


def test_load_locked_incident_locks_the_exact_reviewed_closure_and_active_scope() -> None:
    eod_run = object()
    pipeline_runs = (object(), object(), object())
    stage_runs = (object(), object())
    root_feedback_run = SimpleNamespace(id=7191, summary_payload={})
    feedback_runs = (root_feedback_run,)
    direct_children = (object(),)
    evidence_feedback = (object(), object())
    evidence_snapshots = (object(), object())
    evidence_entries = (
        {'feedback_id': 11, 'decision_snapshot_id': 21},
        {'feedback_id': 12, 'decision_snapshot_id': 22},
    )
    session = _QueryCaptureSession(
        (
            eod_run,
            pipeline_runs,
            stage_runs,
            feedback_runs,
            direct_children,
            evidence_feedback,
            evidence_snapshots,
            (9001,),
            (9002,),
            (9003,),
        )
    )

    with (
        patch.object(
            repair,
            '_extract_evidence_entries',
            return_value=evidence_entries,
        ),
        patch.object(repair, 'EXPECTED_REUSE_EVIDENCE_COUNT', 2),
    ):
        incident = repair._load_locked_incident(session)  # noqa: SLF001

    assert session.outcomes == []
    assert incident == repair.LockedIncident(
        eod_run=eod_run,
        pipeline_runs=pipeline_runs,
        stage_runs=stage_runs,
        feedback_runs=feedback_runs,
        direct_children=direct_children,
        evidence_feedback=evidence_feedback,
        evidence_snapshots=evidence_snapshots,
        evidence_entries=evidence_entries,
        active_eod_ids=(9001,),
        active_pipeline_ids=(9002,),
        active_feedback_ids=(9003,),
    )

    expected_statements = (
        select(repair.EODCollectionRun)
        .where(repair.EODCollectionRun.id == repair.EOD_RUN_ID)
        .with_for_update(),
        select(repair.DailyPipelineRun)
        .where(repair.DailyPipelineRun.id.in_(repair.PIPELINE_RUN_IDS))
        .order_by(repair.DailyPipelineRun.id)
        .with_for_update(),
        select(repair.DailyPipelineStageRun)
        .where(
            repair.DailyPipelineStageRun.pipeline_run_id.in_(
                repair.PIPELINE_RUN_IDS
            )
        )
        .order_by(repair.DailyPipelineStageRun.id)
        .with_for_update(),
        select(repair.AgentContextFeedbackRun)
        .where(
            repair.AgentContextFeedbackRun.id.in_(
                tuple(spec.id for spec in repair.FEEDBACK_RUN_SPECS)
            )
        )
        .order_by(repair.AgentContextFeedbackRun.id)
        .with_for_update(),
        select(repair.AgentContextFeedbackRun)
        .where(
            repair.AgentContextFeedbackRun.request_payload['parent_run_id']
            .as_string()
            .in_(repair.PARENT_RUN_IDS)
        )
        .order_by(repair.AgentContextFeedbackRun.id)
        .with_for_update(),
        select(repair.AgentContextFeedback)
        .where(repair.AgentContextFeedback.id.in_((11, 12)))
        .order_by(repair.AgentContextFeedback.id)
        .with_for_update(),
        select(repair.SignalDeciderDecisionSnapshot)
        .where(repair.SignalDeciderDecisionSnapshot.id.in_((21, 22)))
        .order_by(repair.SignalDeciderDecisionSnapshot.id)
        .with_for_update(),
        select(repair.EODCollectionRun.id)
        .where(
            repair.EODCollectionRun.trade_date == repair.TRADE_DATE,
            repair.EODCollectionRun.status.in_(('queued', 'running')),
        )
        .order_by(repair.EODCollectionRun.id)
        .with_for_update(),
        select(repair.DailyPipelineRun.id)
        .where(
            repair.DailyPipelineRun.market_date == repair.TRADE_DATE,
            repair.DailyPipelineRun.status.in_(('queued', 'running')),
        )
        .order_by(repair.DailyPipelineRun.id)
        .with_for_update(),
        select(repair.AgentContextFeedbackRun.id)
        .where(
            repair.AgentContextFeedbackRun.market_date == repair.TRADE_DATE,
            repair.AgentContextFeedbackRun.status.in_(('queued', 'running')),
        )
        .order_by(repair.AgentContextFeedbackRun.id)
        .with_for_update(),
    )
    assert len(session.statements) == len(expected_statements)
    for query_number, (actual, expected) in enumerate(
        zip(session.statements, expected_statements, strict=True),
        start=1,
    ):
        assert actual.compare(expected), f'locked query {query_number} changed scope'


def test_unexpected_stage_inside_target_pipelines_blocks() -> None:
    expected_rows = tuple(
        SimpleNamespace(
            **{
                **spec.__dict__,
                'market_date': repair.TRADE_DATE,
                'source_run_ids': dict(spec.source_run_ids),
            }
        )
        for spec in repair.STAGE_SPECS
    )
    unexpected = SimpleNamespace(
        id=9999,
        pipeline_run_id=591,
        stage_id='unexpected_stage',
        status='blocked',
        processed=0,
        total=0,
        succeeded=0,
        failed=0,
        remaining=0,
        market_date=repair.TRADE_DATE,
        source_run_ids={},
    )

    with pytest.raises(repair.RepairBlocked, match='stage set changed'):
        repair._validate_stage_runs((*expected_rows, unexpected))  # noqa: SLF001


@pytest.mark.parametrize('active_status', ['queued', 'running'])
def test_active_feedback_candidate_fails_exact_row_validation(active_status) -> None:
    request = {'market_date': '2026-10-27'}
    summary_request = {'market_date': '2026-10-27'}
    spec = repair.FeedbackRunSpec(
        id=1,
        run_id='candidate',
        status='completed',
        symbols_total=1,
        completed_count=1,
        failed_count=0,
        parent_run_id=None,
        request_sha256=repair._sha256(request),  # noqa: SLF001
        summary_request_sha256=repair._sha256(summary_request),  # noqa: SLF001
    )
    row = SimpleNamespace(
        id=1,
        run_id='candidate',
        status=active_status,
        symbols_total=1,
        completed_count=1,
        failed_count=0,
        market_date=repair.TRADE_DATE,
        request_payload=request,
        summary_payload={'request': summary_request},
    )

    with (
        patch.object(repair, 'FEEDBACK_RUN_SPECS', (spec,)),
        pytest.raises(repair.RepairBlocked, match='status changed'),
    ):
        repair._validate_feedback_runs((row,), expect_applied=False)  # noqa: SLF001


def test_partial_point_in_time_state_fails_exact_row_validation() -> None:
    base_request = {'market_date': '2026-10-27'}
    base_summary_request = {'market_date': '2026-10-27'}
    spec = repair.FeedbackRunSpec(
        id=1,
        run_id='candidate',
        status='completed',
        symbols_total=1,
        completed_count=1,
        failed_count=0,
        parent_run_id=None,
        request_sha256=repair._sha256(base_request),  # noqa: SLF001
        summary_request_sha256=repair._sha256(base_summary_request),  # noqa: SLF001
    )
    row = SimpleNamespace(
        id=1,
        run_id='candidate',
        status='completed',
        symbols_total=1,
        completed_count=1,
        failed_count=0,
        market_date=repair.TRADE_DATE,
        request_payload={**base_request, 'point_in_time_recovery': True},
        summary_payload={'request': base_summary_request},
    )

    with (
        patch.object(repair, 'FEEDBACK_RUN_SPECS', (spec,)),
        pytest.raises(repair.RepairBlocked, match='partial repair state'),
    ):
        repair._validate_feedback_runs((row,), expect_applied=True)  # noqa: SLF001


def test_apply_updates_only_two_authority_fields_and_pipeline_attestation() -> None:
    feedback_runs = tuple(
        SimpleNamespace(
            id=spec.id,
            request_payload={'keep': f'request-{spec.id}'},
            summary_payload={
                'request': {'keep': f'summary-request-{spec.id}'},
                'keep_outside_request': spec.id,
            },
        )
        for spec in repair.FEEDBACK_RUN_SPECS
    )
    pipeline_589 = SimpleNamespace(id=589, run_metadata={'keep': 589})
    pipeline_590 = SimpleNamespace(id=590, run_metadata={'keep': 590})
    pipeline_591 = SimpleNamespace(id=591, run_metadata={'keep': 591})
    incident = _empty_incident(
        feedback_runs=feedback_runs,
        pipeline_runs=(pipeline_589, pipeline_590, pipeline_591),
    )
    applied_at = datetime(2026, 10, 28, 1, 2, 3, tzinfo=timezone.utc)

    attestation = repair._apply_locked_repair(  # noqa: SLF001
        incident,
        applied_at=applied_at,
    )

    for row in feedback_runs:
        assert row.request_payload == {
            'keep': f'request-{row.id}',
            'point_in_time_recovery': True,
        }
        assert row.summary_payload == {
            'request': {
                'keep': f'summary-request-{row.id}',
                'point_in_time_recovery': True,
            },
            'keep_outside_request': row.id,
        }
    assert pipeline_589.run_metadata == {'keep': 589}
    assert pipeline_590.run_metadata == {'keep': 590}
    assert pipeline_591.run_metadata['keep'] == 591
    assert pipeline_591.run_metadata[repair.ATTESTATION_KEY] == attestation
    repair._validate_attestation(attestation)  # noqa: SLF001


def test_self_hashed_attestation_rejects_tampering() -> None:
    attestation = repair._build_attestation(  # noqa: SLF001
        applied_at=datetime(2026, 10, 28, tzinfo=timezone.utc)
    )
    attestation['feedback_run_ids'] = [7173]

    with pytest.raises(repair.RepairBlocked, match='digest is invalid'):
        repair._validate_attestation(attestation)  # noqa: SLF001


def _pipeline_rows_with_metadata(metadata_by_id):
    statuses = {589: 'blocked', 590: 'blocked', 591: 'interrupted'}
    stages = {
        589: 'signal_decider_evaluations',
        590: 'signal_decider_evaluations',
        591: 'signal_decider',
    }
    return tuple(
        SimpleNamespace(
            id=pipeline_id,
            market_date=repair.TRADE_DATE,
            trigger='auto_eod_followup_resume',
            status=statuses[pipeline_id],
            current_stage=stages[pipeline_id],
            run_metadata=metadata_by_id[pipeline_id],
        )
        for pipeline_id in repair.PIPELINE_RUN_IDS
    )


def _base_pipeline_metadata():
    return {
        pipeline_id: {
            'entrypoint': 'daily_pipeline_stage_service.run',
            'point_in_time_recovery': True,
            'reused_eod_collection_run_id': repair.EOD_RUN_ID,
            'pipeline': pipeline_id,
        }
        for pipeline_id in repair.PIPELINE_RUN_IDS
    }


def test_attestation_key_is_rejected_on_non_attestation_pipelines() -> None:
    metadata_by_id = _base_pipeline_metadata()
    metadata_by_id[589][repair.ATTESTATION_KEY] = {'unexpected': True}
    expected_hashes = {
        pipeline_id: repair._sha256(metadata_by_id[pipeline_id])  # noqa: SLF001
        for pipeline_id in repair.PIPELINE_RUN_IDS
    }

    with (
        patch.object(repair, 'EXPECTED_PIPELINE_METADATA_SHA256', expected_hashes),
        pytest.raises(repair.RepairBlocked, match='unexpected repair attestation'),
    ):
        repair._validate_pipeline_runs(  # noqa: SLF001
            _pipeline_rows_with_metadata(metadata_by_id)
        )


def test_null_attestation_on_pipeline_591_is_not_treated_as_absent() -> None:
    metadata_by_id = _base_pipeline_metadata()
    expected_hashes = {
        pipeline_id: repair._sha256(metadata_by_id[pipeline_id])  # noqa: SLF001
        for pipeline_id in repair.PIPELINE_RUN_IDS
    }
    metadata_by_id[591][repair.ATTESTATION_KEY] = None

    with (
        patch.object(repair, 'EXPECTED_PIPELINE_METADATA_SHA256', expected_hashes),
        pytest.raises(repair.RepairBlocked, match='attestation must be a JSON object'),
    ):
        repair._validate_pipeline_runs(  # noqa: SLF001
            _pipeline_rows_with_metadata(metadata_by_id)
        )


def test_reuse_evidence_rejects_future_timestamp_after_root_cutoff() -> None:
    future = datetime(2026, 10, 27, 23, 28, 36, tzinfo=timezone.utc)
    root = SimpleNamespace(
        id=7191,
        started_at=repair.EXPECTED_REUSE_OBSERVATION_CUTOFF,
        summary_payload={
            'reused_symbols_by_role': {'signal-decider': ['005930']},
        },
    )
    feedback = SimpleNamespace(
        id=1,
        market_date=repair.TRADE_DATE,
        symbol='005930',
        agent_name='signal-decider',
        run_id='source-run',
        received_at=future,
        created_at=future,
    )
    snapshot = SimpleNamespace(
        id=2,
        market_date=repair.TRADE_DATE,
        symbol='005930',
        agent_name='signal-decider',
        feedback_id=1,
        decision_run_id='source-run',
        content_fingerprint='fingerprint',
        feedback_received_at=future,
        created_at=future,
    )
    entry = {
        'role': 'signal-decider',
        'symbol': '005930',
        'schema_version': 'agent_feedback_reuse_evidence_v1',
        'feedback_id': 1,
        'feedback_run_id': 'source-run',
        'feedback_received_at': future.isoformat(),
        'decision_snapshot_id': 2,
        'decision_snapshot_fingerprint': 'fingerprint',
        'decision_snapshot_observed_at': future.isoformat(),
    }
    incident = _empty_incident(
        feedback_runs=(root,),
        evidence_feedback=(feedback,),
        evidence_snapshots=(snapshot,),
        evidence_entries=(entry,),
    )

    with (
        patch.object(repair, 'EXPECTED_REUSE_EVIDENCE_COUNT', 1),
        pytest.raises(repair.RepairBlocked, match='newer than feedback run 7191'),
    ):
        repair._validate_reuse_evidence(incident)  # noqa: SLF001


def test_reuse_evidence_rejects_drifted_root_observation_cutoff() -> None:
    root = SimpleNamespace(
        id=7191,
        started_at=datetime(2026, 10, 27, 23, 28, 36, tzinfo=timezone.utc),
        summary_payload={},
    )

    with pytest.raises(repair.RepairBlocked, match='observation cutoff changed'):
        repair._validate_reuse_evidence(  # noqa: SLF001
            _empty_incident(feedback_runs=(root,))
        )


def test_reuse_evidence_rejects_snapshot_binding_mismatch() -> None:
    observed_at = datetime(2026, 10, 27, 22, 0, tzinfo=timezone.utc)
    root = SimpleNamespace(
        id=7191,
        started_at=repair.EXPECTED_REUSE_OBSERVATION_CUTOFF,
        summary_payload={
            'reused_symbols_by_role': {'signal-decider': ['005930']},
        },
    )
    feedback = SimpleNamespace(
        id=1,
        market_date=repair.TRADE_DATE,
        symbol='005930',
        agent_name='signal-decider',
        run_id='source-run',
        received_at=observed_at,
        created_at=observed_at,
    )
    snapshot = SimpleNamespace(
        id=2,
        market_date=repair.TRADE_DATE,
        symbol='005930',
        agent_name='signal-decider',
        feedback_id=1,
        decision_run_id='source-run',
        content_fingerprint='different-fingerprint',
        feedback_received_at=observed_at,
        created_at=observed_at,
    )
    entry = {
        'role': 'signal-decider',
        'symbol': '005930',
        'schema_version': 'agent_feedback_reuse_evidence_v1',
        'feedback_id': 1,
        'feedback_run_id': 'source-run',
        'feedback_received_at': observed_at.isoformat(),
        'decision_snapshot_id': 2,
        'decision_snapshot_fingerprint': 'expected-fingerprint',
        'decision_snapshot_observed_at': observed_at.isoformat(),
    }
    incident = _empty_incident(
        feedback_runs=(root,),
        evidence_feedback=(feedback,),
        evidence_snapshots=(snapshot,),
        evidence_entries=(entry,),
    )

    with (
        patch.object(repair, 'EXPECTED_REUSE_EVIDENCE_COUNT', 1),
        pytest.raises(repair.RepairBlocked, match='snapshot binding changed'),
    ):
        repair._validate_reuse_evidence(incident)  # noqa: SLF001


def test_dry_run_holds_bounded_advisory_lock_and_writes_nothing() -> None:
    session = _FakeSession()
    incident = _empty_incident()
    service = repair.EOD581FeedbackLineageRepair()

    with (
        patch.object(repair, 'session_scope', _session_scope_for(session)),
        patch.object(repair, '_load_locked_incident', return_value=incident),
        patch.object(repair, '_validate_locked_incident', return_value=False),
    ):
        result = service.repair()

    assert result['status'] == 'ready'
    assert result['dry_run'] is True
    assert result['applied'] is False
    assert result['plan'] == repair.PLAN
    assert session.flush_count == 0
    assert len(session.statements) == 1
    assert 'pg_try_advisory_xact_lock' in str(session.statements[0][0])
    assert session.statements[0][1] == {
        'source_key': 20_001,
        'trade_key': 20_261_027,
    }


def test_apply_requires_the_reviewed_dry_run_digest_before_database_access() -> None:
    service = repair.EOD581FeedbackLineageRepair()

    with (
        patch.object(repair, 'session_scope') as session_scope,
        pytest.raises(repair.RepairBlocked, match='required with --apply'),
    ):
        service.repair(apply=True)
    session_scope.assert_not_called()

    with (
        patch.object(repair, 'session_scope') as session_scope,
        pytest.raises(repair.RepairBlocked, match='does not match'),
    ):
        service.repair(apply=True, expected_plan_sha256='0' * 64)
    session_scope.assert_not_called()

    with (
        patch.object(repair, 'session_scope') as session_scope,
        pytest.raises(repair.RepairBlocked, match='confirm-core-offline'),
    ):
        service.repair(
            apply=True,
            expected_plan_sha256=repair.PLAN_SHA256,
        )
    session_scope.assert_not_called()


def test_valid_attestation_makes_apply_idempotent_without_flush() -> None:
    session = _FakeSession()
    service = repair.EOD581FeedbackLineageRepair()

    with (
        patch.object(repair, 'session_scope', _session_scope_for(session)),
        patch.object(repair, '_load_locked_incident', return_value=_empty_incident()),
        patch.object(repair, '_validate_locked_incident', return_value=True),
    ):
        result = service.repair(
            apply=True,
            expected_plan_sha256=repair.PLAN_SHA256,
            confirm_core_offline=True,
        )

    assert result['status'] == 'already_applied'
    assert result['applied'] is False
    assert session.flush_count == 0


def test_apply_flushes_then_revalidates_before_the_transaction_commits() -> None:
    events: list[str] = []
    session = _TransactionalFakeSession(events)
    incident = _empty_incident()
    service = repair.EOD581FeedbackLineageRepair()

    def validate(candidate):
        assert candidate is incident
        events.append(f'validate:{session.flush_count}')
        return session.flush_count == 1

    def apply(candidate, *, applied_at):
        assert candidate is incident
        assert applied_at.tzinfo is timezone.utc
        events.append('apply')
        return {'attestation_sha256': 'attestation-digest'}

    with (
        patch('app.db.IsolatedSessionLocal', return_value=session),
        patch.object(repair, '_load_locked_incident', return_value=incident),
        patch.object(repair, '_validate_locked_incident', side_effect=validate),
        patch.object(repair, '_apply_locked_repair', side_effect=apply),
    ):
        result = service.repair(
            apply=True,
            expected_plan_sha256=repair.PLAN_SHA256,
            confirm_core_offline=True,
        )

    assert result['status'] == 'completed'
    assert result['dry_run'] is False
    assert result['applied'] is True
    assert result['attestation_sha256'] == 'attestation-digest'
    assert events == [
        'validate:0',
        'apply',
        'flush',
        'validate:1',
        'commit',
        'close',
    ]
    assert session.commit_count == 1
    assert session.rollback_count == 0


def test_post_apply_verification_exception_rolls_back_and_propagates() -> None:
    events: list[str] = []
    session = _TransactionalFakeSession(events)
    incident = _empty_incident()
    service = repair.EOD581FeedbackLineageRepair()
    verification_error = repair.RepairBlocked('post-flush verification drift')

    def validate(candidate):
        assert candidate is incident
        events.append(f'validate:{session.flush_count}')
        if session.flush_count:
            raise verification_error
        return False

    def apply(candidate, *, applied_at):
        assert candidate is incident
        assert applied_at.tzinfo is timezone.utc
        events.append('apply')
        return {'attestation_sha256': 'unused'}

    with (
        patch('app.db.IsolatedSessionLocal', return_value=session),
        patch.object(repair, '_load_locked_incident', return_value=incident),
        patch.object(repair, '_validate_locked_incident', side_effect=validate),
        patch.object(repair, '_apply_locked_repair', side_effect=apply),
        pytest.raises(repair.RepairBlocked) as exc_info,
    ):
        service.repair(
            apply=True,
            expected_plan_sha256=repair.PLAN_SHA256,
            confirm_core_offline=True,
        )

    assert exc_info.value is verification_error
    assert events == [
        'validate:0',
        'apply',
        'flush',
        'validate:1',
        'rollback',
        'close',
    ]
    assert session.commit_count == 0
    assert session.rollback_count == 1


def test_cli_defaults_to_dry_run() -> None:
    with (
        patch.object(
            repair.eod581_feedback_lineage_repair,
            'repair',
            return_value={'status': 'ready', 'applied': False},
        ) as service,
        redirect_stdout(StringIO()) as stdout,
    ):
        exit_code = repair.main([])

    assert exit_code == 0
    service.assert_called_once_with(
        apply=False,
        expected_plan_sha256=None,
        confirm_core_offline=False,
    )
    assert '"status":"ready"' in stdout.getvalue()


def test_cli_reports_missing_apply_digest_as_blocked_json() -> None:
    with redirect_stderr(StringIO()) as stderr:
        exit_code = repair.main(['--apply'])

    assert exit_code == 2
    assert '"status":"blocked"' in stderr.getvalue()
    assert '--expected-plan-sha256 is required with --apply' in stderr.getvalue()


def test_cli_help_states_offline_apply_preconditions() -> None:
    help_text = ' '.join(repair._build_parser().format_help().split())  # noqa: SLF001

    assert '--confirm-core-offline' in help_text
    assert 'Dry-run may execute while Core is live' in help_text
    assert 'apply requires Core and background workers to be offline' in help_text
    assert 'Core and background workers are stopped' in help_text
    assert 'port 8000 is closed' in help_text
    assert 'active DB work is absent' in help_text
