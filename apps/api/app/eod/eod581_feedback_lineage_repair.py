"""One-time EOD 581 feedback-lineage metadata repair.

Dry-run may execute while Core is live. Apply requires Core and every background
worker to be stopped first, port 8000 to be closed, and active database work to
be absent. ``--confirm-core-offline`` is an operator attestation of those checks;
it does not attempt to stop or inspect processes itself.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import sys
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    AgentContextFeedbackRun,
    DailyPipelineRun,
    DailyPipelineStageRun,
    EODCollectionRun,
    SignalDeciderDecisionSnapshot,
)


INCIDENT_ID = 'eod581_feedback_lineage_20261027'
TRADE_DATE = date(2026, 10, 27)
EOD_RUN_ID = 581
PIPELINE_RUN_IDS = (589, 590, 591)
ATTESTATION_KEY = 'eod581_feedback_lineage_repair_attestation'
ATTESTATION_SCHEMA_VERSION = 'eod581_feedback_lineage_repair_attestation_v1'
PLAN_SCHEMA_VERSION = 'eod581_feedback_lineage_repair_plan_v1'
PIPELINE_ADVISORY_LOCK_KEY = 20_001
PIPELINE_ADVISORY_TRADE_KEY = 20_261_027

EXPECTED_EOD_METADATA_SHA256 = (
    '06907872c9ad5dc4e2f36fb98778cc6310d3f2910d8d30b63c8b4dc4e3989225'
)
EXPECTED_PIPELINE_METADATA_SHA256 = {
    589: '5499269d375bc8da43069db50aea5c323b1f87d213c84e518dcbe33c9047d20d',
    590: '2810f46405b5b60b179c0043ab9ab7d1052765e25400bec8cf937ba38e284f54',
    591: '89b32651745a13188e974007fca6fd960490939cef2041a8d75e09ab555a64d8',
}
EXPECTED_REUSE_EVIDENCE_SHA256 = (
    'fbd2231d84a04aeed42b9a3eb5fe5c1c6f0dcc8576b7710d521a0f8bfa5eacc5'
)
EXPECTED_REUSE_EVIDENCE_COUNT = 83
EXPECTED_REUSE_OBSERVATION_CUTOFF = datetime(
    2026,
    10,
    27,
    23,
    28,
    35,
    193_940,
    tzinfo=timezone.utc,
)


class RepairBlocked(RuntimeError):
    """The frozen incident no longer matches the reviewed repair scope."""


@dataclass(frozen=True)
class FeedbackRunSpec:
    id: int
    run_id: str
    status: str
    symbols_total: int
    completed_count: int
    failed_count: int
    parent_run_id: str | None
    request_sha256: str
    summary_request_sha256: str


@dataclass(frozen=True)
class StageSpec:
    id: int
    pipeline_run_id: int
    stage_id: str
    status: str
    processed: int
    total: int
    succeeded: int
    failed: int
    remaining: int
    source_run_ids: Mapping[str, str]


def _feedback_run_spec(manifest_line: str) -> FeedbackRunSpec:
    fields = manifest_line.split('|')
    if len(fields) != 9:
        raise RuntimeError('invalid frozen feedback-run manifest row')
    (
        row_id,
        run_id,
        status,
        symbols_total,
        completed_count,
        failed_count,
        parent_run_id,
        request_sha256,
        summary_request_sha256,
    ) = fields
    return FeedbackRunSpec(
        id=int(row_id),
        run_id=run_id,
        status=status,
        symbols_total=int(symbols_total),
        completed_count=int(completed_count),
        failed_count=int(failed_count),
        parent_run_id=None if parent_run_id == '-' else parent_run_id,
        request_sha256=request_sha256,
        summary_request_sha256=summary_request_sha256,
    )


# Columns: id, run_id, status, counts, parent_run_id, request hash, summary.request hash.
_FEEDBACK_RUN_MANIFEST = """
7173|feedback-cycle-2026-10-27-e60e026357|partial|160|148|12|-|e621a3e800826fe201390fbf18a041fbc1ccf1b7403902e9d5836b7ad9495577|e621a3e800826fe201390fbf18a041fbc1ccf1b7403902e9d5836b7ad9495577
7175|symbol-run-2026-10-27-3f9397723d|partial|15|1|14|feedback-cycle-2026-10-27-e60e026357|a247c4b87aa43f3ef0c967b580d7348a45f7eb08ade45e568797a38ef337315e|df51f39d93e03162dc4c80b5be28df18510cffabf0764affa3dab6310826001f
7176|symbol-run-2026-10-27-640133abdc|partial|14|2|12|feedback-cycle-2026-10-27-e60e026357|f408cbeb2cec7a75fbac294c3890d4f16fc517e9fbd89988d4b962af1c346c63|1797ffd0f0ea4abc88f9b193e7dcfc5e0379f9f043b9684d6dea5f4a5c2e175c
7177|feedback-cycle-2026-10-27-f0ff7f7141|partial|160|83|77|-|ba87d43780364bd4f1db3489bed32389c7478fd59c2ba850ea9559cb6c70308e|ba87d43780364bd4f1db3489bed32389c7478fd59c2ba850ea9559cb6c70308e
7179|symbol-run-2026-10-27-d12323b154|completed|20|20|0|feedback-cycle-2026-10-27-f0ff7f7141|58d7ddb8415b23a5e52c0213bbfd14e81adfc24092e70f99c691a63064fdc342|9da9c0ffc9d2e9128df6014339c203769052134a080b91f2c500b7061c619d78
7180|symbol-run-2026-10-27-eb89234133|completed|20|20|0|feedback-cycle-2026-10-27-f0ff7f7141|63ab5c8df20d25713823e94667bd4db612a4c7cdc4af5666dc43ee2807ff47f1|c37f95a2bf9dce5998128d34b20c695f16c4cfa814335825b079be196dbda668
7181|symbol-run-2026-10-27-02460d7ed7|completed|20|20|0|feedback-cycle-2026-10-27-f0ff7f7141|4c7f9bc34ef59cb2fd0f5d6c5f0c880310f4649a5af312770ec82ad17f047c4d|3187453016f9fda470df88ce618349c2833febfd5c88d85dabb66482e3607c93
7182|symbol-run-2026-10-27-c3c6ac998c|completed|20|20|0|feedback-cycle-2026-10-27-f0ff7f7141|3013f35f9f924b5c21f0aeda86cb15b04592916886ddcdb3a0cb062191db1cc2|67ffed7a764813900d742aeb057e0c02337e68c2d80bd56064a10e0a9eaf4a23
7183|symbol-run-2026-10-27-bde05b596f|failed|20|0|20|feedback-cycle-2026-10-27-f0ff7f7141|9d6aa366a4ac7473b6ff2475d249a9f3021439c3e8a3a95802262ebadabab3ea|061e5290e0142d818f31aa161a46fb1b11ca6b4d62af83149578c5e79beedd84
7184|symbol-run-2026-10-27-1fd3d87d9c|failed|20|0|20|feedback-cycle-2026-10-27-f0ff7f7141|8e349103abb07e9639cbafead3434668b8321b31d1f3c1bdc567692377e09663|9ae1598b3ddaae8383e472ad9dcbc07f64c35f357451ebc55b48863534341963
7185|symbol-run-2026-10-27-00f76b1b11|failed|20|0|20|feedback-cycle-2026-10-27-f0ff7f7141|64b7c7859776a0c568736c7b81d22391129c5e752450b0d3ef4c52cd51d26f92|3611e829d850a1daee00ec9d65db9e7d148289d6cb3ae87bd274c427a91c8a94
7186|symbol-run-2026-10-27-bab70eb57e|failed|17|0|17|feedback-cycle-2026-10-27-f0ff7f7141|1494bf0d255d2fee3eb94af07342871ab1b9598d6753f8f4d8748903b8f9b0f2|22a755d6b64a9a14912098517ae83363c72b75e38cab544088a0a1a523514fe0
7187|symbol-run-2026-10-27-205e0a22ea|failed|20|0|20|feedback-cycle-2026-10-27-f0ff7f7141|3659aa415ff72eb269702192e0f97e47fb41a2a8d7e0e62289d5e523c204912d|9c9ae2c878a7d4a18ebedce564756cfde83bf8eac019e0c9dbe03e2b356178f8
7188|symbol-run-2026-10-27-70d5948fe2|failed|20|0|20|feedback-cycle-2026-10-27-f0ff7f7141|ed2e111057b98bb26a6ec8e527043b0a4f62775dfe4c0cd9abb01b0f05547aa6|3db43fcddc85339be60d09c4735f57c8e823fa9e093beb44ae3312eb3ceab467
7189|symbol-run-2026-10-27-f42d0cc2eb|failed|20|0|20|feedback-cycle-2026-10-27-f0ff7f7141|d723775fe7900a06bd17300951b9bad961021996fb49504a3ed46b58ecc2263d|ef08cd5fa6e45bc0b7c71984da9eb20697819a1ca7eec47a7d07f92a411d62b2
7190|symbol-run-2026-10-27-ef6aee5691|failed|17|0|17|feedback-cycle-2026-10-27-f0ff7f7141|8d74321104850ec641c9bb34d56807594e693222fa5822e7c710ba32debac77a|c614d749741c26f1b0bdd4331f8422e2c40d0a0198e994c2f22a99c8043f710c
7191|feedback-cycle-2026-10-27-61b363a8a0|abandoned|160|83|77|-|f88aa7ed99927e979a1d633c56f0a3cde966ea2053b5eea955ef48d4347545c2|b3969e0cf10c76cfa26e08c2ed8c9b537a9ea0b78b9ccb0a636659735024d18e
7193|symbol-run-2026-10-27-679790866a|failed|20|0|20|feedback-cycle-2026-10-27-61b363a8a0|8e0359742029f081f739d58be6a59291d7a158c8212d10968f1c37fc22c56c5a|01c49baddceb0a5662a0c0ac2b9b9d08ad5ed62337e2ad3a994c8438bc781b45
7194|symbol-run-2026-10-27-8468703277|abandoned|20|12|8|feedback-cycle-2026-10-27-61b363a8a0|49125a71cdae9cb3547d55d31e9bb22a7df91eb2778dde21ba2b7676345c295b|2df180d54f8ce38e5461c7a1fd63ac8c91ec43dfb8ba04115646e21542fea1f9
""".strip()

FEEDBACK_RUN_SPECS = tuple(
    _feedback_run_spec(line) for line in _FEEDBACK_RUN_MANIFEST.splitlines()
)

STAGE_SPECS = (
    StageSpec(3653, 589, 'signal_decider', 'partial', 160, 160, 148, 12, 0,
              {'run_id': 'feedback-cycle-2026-10-27-e60e026357'}),
    StageSpec(3654, 589, 'next_open_execution_plan', 'blocked', 0, 0, 0, 0, 0, {}),
    StageSpec(3655, 589, 'signal_decider_evaluations', 'blocked', 0, 0, 0, 0, 0, {}),
    StageSpec(3656, 590, 'signal_decider', 'partial', 160, 160, 83, 77, 0,
              {'run_id': 'feedback-cycle-2026-10-27-f0ff7f7141'}),
    StageSpec(3657, 590, 'next_open_execution_plan', 'blocked', 0, 0, 0, 0, 0, {}),
    StageSpec(3658, 590, 'signal_decider_evaluations', 'blocked', 0, 0, 0, 0, 0, {}),
    StageSpec(3659, 591, 'signal_decider', 'interrupted', 103, 160, 83, 20, 57,
              {'agent_feedback_run_id': 'feedback-cycle-2026-10-27-61b363a8a0'}),
)

PARENT_RUN_IDS = tuple(
    spec.run_id for spec in FEEDBACK_RUN_SPECS if spec.parent_run_id is None
)
EXPECTED_DIRECT_CHILD_IDS = tuple(
    spec.id for spec in FEEDBACK_RUN_SPECS if spec.parent_run_id is not None
)


@dataclass
class LockedIncident:
    eod_run: EODCollectionRun | None
    pipeline_runs: tuple[DailyPipelineRun, ...]
    stage_runs: tuple[DailyPipelineStageRun, ...]
    feedback_runs: tuple[AgentContextFeedbackRun, ...]
    direct_children: tuple[AgentContextFeedbackRun, ...]
    evidence_feedback: tuple[AgentContextFeedback, ...]
    evidence_snapshots: tuple[SignalDeciderDecisionSnapshot, ...]
    evidence_entries: tuple[dict[str, Any], ...]
    active_eod_ids: tuple[int, ...]
    active_pipeline_ids: tuple[int, ...]
    active_feedback_ids: tuple[int, ...]


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(',', ':'),
        sort_keys=True,
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode('utf-8')).hexdigest()


def _closure_manifest() -> dict[str, Any]:
    return {
        'schema_version': PLAN_SCHEMA_VERSION,
        'incident_id': INCIDENT_ID,
        'trade_date': TRADE_DATE.isoformat(),
        'eod_collection_run': {
            'id': EOD_RUN_ID,
            'run_metadata_sha256': EXPECTED_EOD_METADATA_SHA256,
        },
        'daily_pipeline_runs': [
            {
                'id': pipeline_id,
                'run_metadata_sha256': EXPECTED_PIPELINE_METADATA_SHA256[pipeline_id],
            }
            for pipeline_id in PIPELINE_RUN_IDS
        ],
        'daily_pipeline_stage_runs': [asdict(spec) for spec in STAGE_SPECS],
        'feedback_runs': [asdict(spec) for spec in FEEDBACK_RUN_SPECS],
        'reuse_evidence': {
            'count': EXPECTED_REUSE_EVIDENCE_COUNT,
            'sha256': EXPECTED_REUSE_EVIDENCE_SHA256,
            'observation_cutoff': EXPECTED_REUSE_OBSERVATION_CUTOFF.isoformat(),
            'observation_cutoff_source': 'feedback_run_7191.started_at',
        },
        'lineage_boundary': {
            'kind': 'reviewed_parents_plus_direct_children_only',
            'parent_run_ids': list(PARENT_RUN_IDS),
            'direct_child_ids': list(EXPECTED_DIRECT_CHILD_IDS),
            'recursive_ancestor_expansion': False,
        },
        'apply_preconditions': {
            'core_and_background_workers_offline': True,
            'port_8000_closed': True,
            'active_database_work_absent': True,
            'operator_attestation_required': True,
        },
        'mutation': {
            'feedback_run_count': len(FEEDBACK_RUN_SPECS),
            'request_payload.point_in_time_recovery': True,
            'summary_payload.request.point_in_time_recovery': True,
            'attestation_pipeline_run_id': 591,
            'other_fields_mutated': False,
        },
    }


CLOSURE_MANIFEST = _closure_manifest()
CLOSURE_SHA256 = _sha256(CLOSURE_MANIFEST)
PLAN = {
    **CLOSURE_MANIFEST,
    'closure_sha256': CLOSURE_SHA256,
}
PLAN_SHA256 = _sha256(PLAN)


def _require_mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RepairBlocked(f'{label} must be a JSON object')
    return value


def _point_in_time_state(container: Mapping[str, Any], *, label: str) -> str:
    if 'point_in_time_recovery' not in container:
        return 'missing'
    value = container['point_in_time_recovery']
    if value is False:
        raise RepairBlocked(f'{label}.point_in_time_recovery is explicitly false')
    if value is not True:
        raise RepairBlocked(f'{label}.point_in_time_recovery is malformed')
    return 'true'


def _sha_without_point_in_time(container: Mapping[str, Any]) -> str:
    normalized = deepcopy(dict(container))
    normalized.pop('point_in_time_recovery', None)
    return _sha256(normalized)


def _extract_evidence_entries(summary_payload: Any) -> tuple[dict[str, Any], ...]:
    summary = _require_mapping(summary_payload, label='feedback run 7191 summary_payload')
    raw_by_role = _require_mapping(
        summary.get('reused_feedback_evidence_by_role'),
        label='feedback run 7191 reused_feedback_evidence_by_role',
    )
    if set(raw_by_role) != {'short-term-decider', 'signal-decider'}:
        raise RepairBlocked('feedback run 7191 reuse evidence roles changed')

    entries: list[dict[str, Any]] = []
    for role in sorted(raw_by_role):
        by_symbol = _require_mapping(
            raw_by_role[role],
            label=f'feedback run 7191 reuse evidence role {role}',
        )
        for symbol in sorted(by_symbol):
            evidence = _require_mapping(
                by_symbol[symbol],
                label=f'feedback run 7191 reuse evidence {role}/{symbol}',
            )
            entries.append({'role': role, 'symbol': symbol, **evidence})
    if len(entries) != EXPECTED_REUSE_EVIDENCE_COUNT:
        raise RepairBlocked('feedback run 7191 reuse evidence count changed')
    if _sha256(entries) != EXPECTED_REUSE_EVIDENCE_SHA256:
        raise RepairBlocked('feedback run 7191 reuse evidence digest changed')
    return tuple(entries)


def _locked_scalars(session: Session, statement: Any) -> tuple[Any, ...]:
    return tuple(session.execute(statement.with_for_update()).scalars())


def _load_locked_incident(session: Session) -> LockedIncident:
    eod_run = session.execute(
        select(EODCollectionRun)
        .where(EODCollectionRun.id == EOD_RUN_ID)
        .with_for_update()
    ).scalar_one_or_none()
    pipeline_runs = _locked_scalars(
        session,
        select(DailyPipelineRun)
        .where(DailyPipelineRun.id.in_(PIPELINE_RUN_IDS))
        .order_by(DailyPipelineRun.id),
    )
    stage_runs = _locked_scalars(
        session,
        select(DailyPipelineStageRun)
        .where(DailyPipelineStageRun.pipeline_run_id.in_(PIPELINE_RUN_IDS))
        .order_by(DailyPipelineStageRun.id),
    )
    feedback_runs = _locked_scalars(
        session,
        select(AgentContextFeedbackRun)
        .where(
            AgentContextFeedbackRun.id.in_(tuple(spec.id for spec in FEEDBACK_RUN_SPECS))
        )
        .order_by(AgentContextFeedbackRun.id),
    )
    direct_children = _locked_scalars(
        session,
        select(AgentContextFeedbackRun)
        .where(
            AgentContextFeedbackRun.request_payload['parent_run_id']
            .as_string()
            .in_(PARENT_RUN_IDS)
        )
        .order_by(AgentContextFeedbackRun.id),
    )

    feedback_by_id = {row.id: row for row in feedback_runs}
    root = feedback_by_id.get(7191)
    if root is None:
        raise RepairBlocked('feedback run 7191 is missing')
    evidence_entries = _extract_evidence_entries(root.summary_payload)
    feedback_ids = tuple(sorted({entry['feedback_id'] for entry in evidence_entries}))
    snapshot_ids = tuple(
        sorted({entry['decision_snapshot_id'] for entry in evidence_entries})
    )
    if len(feedback_ids) != EXPECTED_REUSE_EVIDENCE_COUNT:
        raise RepairBlocked('reuse evidence feedback IDs are not unique')
    if len(snapshot_ids) != EXPECTED_REUSE_EVIDENCE_COUNT:
        raise RepairBlocked('reuse evidence snapshot IDs are not unique')

    evidence_feedback = _locked_scalars(
        session,
        select(AgentContextFeedback)
        .where(AgentContextFeedback.id.in_(feedback_ids))
        .order_by(AgentContextFeedback.id),
    )
    evidence_snapshots = _locked_scalars(
        session,
        select(SignalDeciderDecisionSnapshot)
        .where(SignalDeciderDecisionSnapshot.id.in_(snapshot_ids))
        .order_by(SignalDeciderDecisionSnapshot.id),
    )

    active_eod_ids = tuple(
        session.execute(
            select(EODCollectionRun.id)
            .where(
                EODCollectionRun.trade_date == TRADE_DATE,
                EODCollectionRun.status.in_(('queued', 'running')),
            )
            .order_by(EODCollectionRun.id)
            .with_for_update()
        ).scalars()
    )
    active_pipeline_ids = tuple(
        session.execute(
            select(DailyPipelineRun.id)
            .where(
                DailyPipelineRun.market_date == TRADE_DATE,
                DailyPipelineRun.status.in_(('queued', 'running')),
            )
            .order_by(DailyPipelineRun.id)
            .with_for_update()
        ).scalars()
    )
    active_feedback_ids = tuple(
        session.execute(
            select(AgentContextFeedbackRun.id)
            .where(
                AgentContextFeedbackRun.market_date == TRADE_DATE,
                AgentContextFeedbackRun.status.in_(('queued', 'running')),
            )
            .order_by(AgentContextFeedbackRun.id)
            .with_for_update()
        ).scalars()
    )
    return LockedIncident(
        eod_run=eod_run,
        pipeline_runs=pipeline_runs,
        stage_runs=stage_runs,
        feedback_runs=feedback_runs,
        direct_children=direct_children,
        evidence_feedback=evidence_feedback,
        evidence_snapshots=evidence_snapshots,
        evidence_entries=evidence_entries,
        active_eod_ids=active_eod_ids,
        active_pipeline_ids=active_pipeline_ids,
        active_feedback_ids=active_feedback_ids,
    )


def _validate_eod_run(row: EODCollectionRun | None) -> None:
    if row is None:
        raise RepairBlocked('EOD collection run 581 is missing')
    expected = {
        'id': 581,
        'market': 'KRX',
        'trade_date': TRADE_DATE,
        'source': 'pykrx',
        'trigger': 'auto_close_scheduler',
        'status': 'completed',
        'symbols_total': 2675,
        'symbols_succeeded': 2675,
        'symbols_failed': 0,
        'error_message': None,
    }
    for field, value in expected.items():
        if getattr(row, field) != value:
            raise RepairBlocked(f'EOD collection run 581 {field} changed')
    metadata = _require_mapping(row.run_metadata, label='EOD collection run 581 metadata')
    if metadata.get('mode') != 'daily_auto':
        raise RepairBlocked('EOD collection run 581 mode changed')
    if _sha256(metadata) != EXPECTED_EOD_METADATA_SHA256:
        raise RepairBlocked('EOD collection run 581 metadata digest changed')


def _validate_pipeline_runs(rows: Sequence[DailyPipelineRun]) -> dict[int, DailyPipelineRun]:
    by_id = {row.id: row for row in rows}
    if tuple(sorted(by_id)) != PIPELINE_RUN_IDS:
        raise RepairBlocked('daily pipeline run set changed')
    expected_status = {589: 'blocked', 590: 'blocked', 591: 'interrupted'}
    expected_stage = {
        589: 'signal_decider_evaluations',
        590: 'signal_decider_evaluations',
        591: 'signal_decider',
    }
    for pipeline_id in PIPELINE_RUN_IDS:
        row = by_id[pipeline_id]
        if (
            row.market_date != TRADE_DATE
            or row.trigger != 'auto_eod_followup_resume'
            or row.status != expected_status[pipeline_id]
            or row.current_stage != expected_stage[pipeline_id]
        ):
            raise RepairBlocked(f'daily pipeline run {pipeline_id} identity changed')
        metadata = _require_mapping(
            row.run_metadata,
            label=f'daily pipeline run {pipeline_id} metadata',
        )
        has_attestation = ATTESTATION_KEY in metadata
        if pipeline_id != 591 and has_attestation:
            raise RepairBlocked(
                f'daily pipeline run {pipeline_id} has an unexpected repair attestation'
            )
        metadata_without_attestation = deepcopy(metadata)
        if pipeline_id == 591:
            metadata_without_attestation.pop(ATTESTATION_KEY, None)
        if _sha256(metadata_without_attestation) != EXPECTED_PIPELINE_METADATA_SHA256[pipeline_id]:
            raise RepairBlocked(f'daily pipeline run {pipeline_id} metadata digest changed')
        if (
            metadata.get('entrypoint') != 'daily_pipeline_stage_service.run'
            or metadata.get('point_in_time_recovery') is not True
            or metadata.get('reused_eod_collection_run_id') != EOD_RUN_ID
        ):
            raise RepairBlocked(f'daily pipeline run {pipeline_id} recovery binding changed')
        if pipeline_id == 591 and has_attestation:
            _validate_attestation(metadata[ATTESTATION_KEY])
    return by_id


def _validate_stage_runs(rows: Sequence[DailyPipelineStageRun]) -> None:
    by_id = {row.id: row for row in rows}
    if tuple(sorted(by_id)) != tuple(spec.id for spec in STAGE_SPECS):
        raise RepairBlocked('daily pipeline stage set changed')
    for spec in STAGE_SPECS:
        row = by_id[spec.id]
        for field in (
            'pipeline_run_id',
            'stage_id',
            'status',
            'processed',
            'total',
            'succeeded',
            'failed',
            'remaining',
        ):
            if getattr(row, field) != getattr(spec, field):
                raise RepairBlocked(f'daily pipeline stage {spec.id} {field} changed')
        if row.market_date != TRADE_DATE or dict(row.source_run_ids or {}) != dict(
            spec.source_run_ids
        ):
            raise RepairBlocked(f'daily pipeline stage {spec.id} lineage changed')


def _validate_feedback_runs(
    rows: Sequence[AgentContextFeedbackRun],
    *,
    expect_applied: bool,
) -> None:
    by_id = {row.id: row for row in rows}
    expected_ids = tuple(spec.id for spec in FEEDBACK_RUN_SPECS)
    if tuple(sorted(by_id)) != expected_ids:
        raise RepairBlocked('feedback run set changed')
    for spec in FEEDBACK_RUN_SPECS:
        row = by_id[spec.id]
        for field in (
            'run_id',
            'status',
            'symbols_total',
            'completed_count',
            'failed_count',
        ):
            if getattr(row, field) != getattr(spec, field):
                raise RepairBlocked(f'feedback run {spec.id} {field} changed')
        if row.market_date != TRADE_DATE:
            raise RepairBlocked(f'feedback run {spec.id} market_date changed')

        request = _require_mapping(
            row.request_payload,
            label=f'feedback run {spec.id} request_payload',
        )
        summary = _require_mapping(
            row.summary_payload,
            label=f'feedback run {spec.id} summary_payload',
        )
        summary_request = _require_mapping(
            summary.get('request'),
            label=f'feedback run {spec.id} summary_payload.request',
        )
        request_state = _point_in_time_state(
            request,
            label=f'feedback run {spec.id} request_payload',
        )
        summary_state = _point_in_time_state(
            summary_request,
            label=f'feedback run {spec.id} summary_payload.request',
        )
        expected_state = 'true' if expect_applied else 'missing'
        if request_state != expected_state or summary_state != expected_state:
            raise RepairBlocked(f'feedback run {spec.id} has a partial repair state')
        if _sha_without_point_in_time(request) != spec.request_sha256:
            raise RepairBlocked(f'feedback run {spec.id} request digest changed')
        if _sha_without_point_in_time(summary_request) != spec.summary_request_sha256:
            raise RepairBlocked(f'feedback run {spec.id} summary request digest changed')
        if request.get('parent_run_id') != spec.parent_run_id:
            raise RepairBlocked(f'feedback run {spec.id} parent lineage changed')


def _parse_evidence_datetime(value: Any, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise RepairBlocked(f'{label} must be an ISO timestamp')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise RepairBlocked(f'{label} must be an ISO timestamp') from exc
    if parsed.tzinfo is None:
        raise RepairBlocked(f'{label} must include a timezone')
    return parsed.astimezone(timezone.utc)


def _same_instant(actual: datetime | None, expected: datetime) -> bool:
    return (
        actual is not None
        and actual.tzinfo is not None
        and actual.utcoffset() is not None
        and actual.astimezone(timezone.utc) == expected
    )


def _at_or_before(actual: datetime | None, cutoff: datetime) -> bool:
    return (
        actual is not None
        and actual.tzinfo is not None
        and actual.utcoffset() is not None
        and actual.astimezone(timezone.utc) <= cutoff
    )


def _validate_reuse_evidence(incident: LockedIncident) -> None:
    root = next((row for row in incident.feedback_runs if row.id == 7191), None)
    if root is None:
        raise RepairBlocked('feedback run 7191 is missing')
    if not _same_instant(root.started_at, EXPECTED_REUSE_OBSERVATION_CUTOFF):
        raise RepairBlocked('feedback run 7191 reuse observation cutoff changed')

    feedback_by_id = {row.id: row for row in incident.evidence_feedback}
    snapshot_by_id = {row.id: row for row in incident.evidence_snapshots}
    if len(feedback_by_id) != EXPECTED_REUSE_EVIDENCE_COUNT:
        raise RepairBlocked('reuse evidence feedback row set changed')
    if len(snapshot_by_id) != EXPECTED_REUSE_EVIDENCE_COUNT:
        raise RepairBlocked('reuse evidence decision snapshot set changed')

    root_summary = _require_mapping(root.summary_payload, label='feedback run 7191 summary')
    reused_symbols = _require_mapping(
        root_summary.get('reused_symbols_by_role'),
        label='feedback run 7191 reused_symbols_by_role',
    )
    symbols_from_evidence: dict[str, list[str]] = {}
    for entry in incident.evidence_entries:
        role = entry['role']
        symbol = entry['symbol']
        symbols_from_evidence.setdefault(role, []).append(symbol)
        required_keys = {
            'role',
            'symbol',
            'schema_version',
            'feedback_id',
            'feedback_run_id',
            'feedback_received_at',
            'decision_snapshot_id',
            'decision_snapshot_fingerprint',
            'decision_snapshot_observed_at',
        }
        if set(entry) != required_keys:
            raise RepairBlocked(f'reuse evidence shape changed for {role}/{symbol}')
        if entry['schema_version'] != 'agent_feedback_reuse_evidence_v1':
            raise RepairBlocked(f'reuse evidence version changed for {role}/{symbol}')
        feedback = feedback_by_id.get(entry['feedback_id'])
        snapshot = snapshot_by_id.get(entry['decision_snapshot_id'])
        if feedback is None or snapshot is None:
            raise RepairBlocked(f'reuse evidence row missing for {role}/{symbol}')
        feedback_time = _parse_evidence_datetime(
            entry['feedback_received_at'],
            label=f'reuse evidence feedback time {role}/{symbol}',
        )
        snapshot_time = _parse_evidence_datetime(
            entry['decision_snapshot_observed_at'],
            label=f'reuse evidence snapshot time {role}/{symbol}',
        )
        if not all(
            (
                _at_or_before(feedback_time, EXPECTED_REUSE_OBSERVATION_CUTOFF),
                _at_or_before(snapshot_time, EXPECTED_REUSE_OBSERVATION_CUTOFF),
                _at_or_before(feedback.created_at, EXPECTED_REUSE_OBSERVATION_CUTOFF),
                _at_or_before(snapshot.created_at, EXPECTED_REUSE_OBSERVATION_CUTOFF),
            )
        ):
            raise RepairBlocked(
                f'reuse evidence is newer than feedback run 7191 for {role}/{symbol}'
            )
        if (
            feedback.market_date != TRADE_DATE
            or feedback.symbol != symbol
            or feedback.agent_name != role
            or feedback.run_id != entry['feedback_run_id']
            or not _same_instant(feedback.received_at, feedback_time)
        ):
            raise RepairBlocked(f'reuse feedback binding changed for {role}/{symbol}')
        if (
            snapshot.market_date != TRADE_DATE
            or snapshot.symbol != symbol
            or snapshot.agent_name != role
            or snapshot.feedback_id != feedback.id
            or snapshot.decision_run_id != entry['feedback_run_id']
            or snapshot.content_fingerprint != entry['decision_snapshot_fingerprint']
            or not _same_instant(snapshot.feedback_received_at, snapshot_time)
        ):
            raise RepairBlocked(f'reuse snapshot binding changed for {role}/{symbol}')
    normalized_reused_symbols = {
        role: sorted(symbols) for role, symbols in reused_symbols.items()
    }
    if normalized_reused_symbols != symbols_from_evidence:
        raise RepairBlocked('feedback run 7191 reused symbol authority changed')


def _attestation_body(*, applied_at: datetime) -> dict[str, Any]:
    return {
        'schema_version': ATTESTATION_SCHEMA_VERSION,
        'incident_id': INCIDENT_ID,
        'plan_sha256': PLAN_SHA256,
        'closure_sha256': CLOSURE_SHA256,
        'applied_at': applied_at.astimezone(timezone.utc).isoformat(),
        'eod_collection_run_id': EOD_RUN_ID,
        'daily_pipeline_run_ids': list(PIPELINE_RUN_IDS),
        'feedback_run_ids': [spec.id for spec in FEEDBACK_RUN_SPECS],
        'reuse_evidence_count': EXPECTED_REUSE_EVIDENCE_COUNT,
        'reuse_evidence_sha256': EXPECTED_REUSE_EVIDENCE_SHA256,
        'mutation': {
            'request_payload.point_in_time_recovery': True,
            'summary_payload.request.point_in_time_recovery': True,
        },
    }


def _build_attestation(*, applied_at: datetime) -> dict[str, Any]:
    body = _attestation_body(applied_at=applied_at)
    return {**body, 'attestation_sha256': _sha256(body)}


def _validate_attestation(value: Any) -> None:
    attestation = _require_mapping(value, label='pipeline 591 repair attestation')
    provided_digest = attestation.get('attestation_sha256')
    body = {key: deepcopy(item) for key, item in attestation.items() if key != 'attestation_sha256'}
    if not isinstance(provided_digest, str) or provided_digest != _sha256(body):
        raise RepairBlocked('pipeline 591 repair attestation digest is invalid')
    applied_at = _parse_evidence_datetime(
        body.get('applied_at'),
        label='pipeline 591 repair attestation applied_at',
    )
    if body != _attestation_body(applied_at=applied_at):
        raise RepairBlocked('pipeline 591 repair attestation content is invalid')


def _validate_locked_incident(incident: LockedIncident) -> bool:
    if incident.active_eod_ids or incident.active_pipeline_ids or incident.active_feedback_ids:
        raise RepairBlocked(
            'active 2026-10-27 work exists; refusing incident metadata repair'
        )
    _validate_eod_run(incident.eod_run)
    pipelines = _validate_pipeline_runs(incident.pipeline_runs)
    _validate_stage_runs(incident.stage_runs)
    if tuple(row.id for row in incident.direct_children) != EXPECTED_DIRECT_CHILD_IDS:
        raise RepairBlocked('reviewed one-hop feedback closure changed')

    pipeline_591_metadata = _require_mapping(
        pipelines[591].run_metadata,
        label='daily pipeline run 591 metadata',
    )
    already_applied = ATTESTATION_KEY in pipeline_591_metadata
    _validate_feedback_runs(
        incident.feedback_runs,
        expect_applied=already_applied,
    )
    _validate_reuse_evidence(incident)
    return already_applied


def _apply_locked_repair(
    incident: LockedIncident,
    *,
    applied_at: datetime,
) -> dict[str, Any]:
    for row in incident.feedback_runs:
        request = deepcopy(_require_mapping(row.request_payload, label=f'feedback run {row.id} request'))
        summary = deepcopy(_require_mapping(row.summary_payload, label=f'feedback run {row.id} summary'))
        summary_request = deepcopy(
            _require_mapping(
                summary.get('request'),
                label=f'feedback run {row.id} summary request',
            )
        )
        request['point_in_time_recovery'] = True
        summary_request['point_in_time_recovery'] = True
        summary['request'] = summary_request
        row.request_payload = request
        row.summary_payload = summary

    pipeline_591 = next(row for row in incident.pipeline_runs if row.id == 591)
    metadata = deepcopy(
        _require_mapping(pipeline_591.run_metadata, label='daily pipeline run 591 metadata')
    )
    attestation = _build_attestation(applied_at=applied_at)
    metadata[ATTESTATION_KEY] = attestation
    pipeline_591.run_metadata = metadata
    return attestation


class EOD581FeedbackLineageRepair:
    def repair(
        self,
        *,
        apply: bool = False,
        expected_plan_sha256: str | None = None,
        confirm_core_offline: bool = False,
    ) -> dict[str, Any]:
        if apply:
            expected = str(expected_plan_sha256 or '').strip().lower()
            if not expected:
                raise RepairBlocked('--expected-plan-sha256 is required with --apply')
            if expected != PLAN_SHA256:
                raise RepairBlocked('reviewed plan SHA-256 does not match this repair plan')
            if not confirm_core_offline:
                raise RepairBlocked(
                    '--confirm-core-offline is required with --apply after '
                    'Core/background workers are stopped, port 8000 is closed, '
                    'and active database work is absent'
                )

        with session_scope(
            isolated_connection=True,
            statement_timeout_ms=15_000,
            lock_timeout_ms=2_000,
        ) as session:
            if session.get_bind().dialect.name != 'postgresql':
                raise RepairBlocked('this incident repair requires PostgreSQL')
            lock_acquired = session.execute(
                text('SELECT pg_try_advisory_xact_lock(:source_key, :trade_key)'),
                {
                    'source_key': PIPELINE_ADVISORY_LOCK_KEY,
                    'trade_key': PIPELINE_ADVISORY_TRADE_KEY,
                },
            ).scalar_one()
            if lock_acquired is not True:
                raise RepairBlocked('incident advisory lock is busy')
            incident = _load_locked_incident(session)
            already_applied = _validate_locked_incident(incident)
            result = {
                'incident_id': INCIDENT_ID,
                'plan_sha256': PLAN_SHA256,
                'closure_sha256': CLOSURE_SHA256,
                'feedback_run_count': len(FEEDBACK_RUN_SPECS),
                'reuse_evidence_count': EXPECTED_REUSE_EVIDENCE_COUNT,
                'dry_run': not apply,
                'applied': False,
            }
            if already_applied:
                return {**result, 'status': 'already_applied'}
            if not apply:
                return {**result, 'status': 'ready', 'plan': deepcopy(PLAN)}

            attestation = _apply_locked_repair(
                incident,
                applied_at=datetime.now(timezone.utc),
            )
            session.flush()
            if _validate_locked_incident(incident) is not True:
                raise RepairBlocked('post-apply incident verification failed')
            return {
                **result,
                'status': 'completed',
                'dry_run': False,
                'applied': True,
                'attestation_sha256': attestation['attestation_sha256'],
            }


eod581_feedback_lineage_repair = EOD581FeedbackLineageRepair()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Validate and optionally repair the exact EOD 581 / pipeline '
            '589-591 feedback-run point-in-time lineage incident. Dry-run may '
            'execute while Core is live; apply requires Core and background '
            'workers to be offline.'
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--apply',
        action='store_true',
        help='Apply the reviewed exact-row metadata repair while Core is offline.',
    )
    mode.add_argument(
        '--dry-run',
        action='store_false',
        dest='apply',
        help=(
            'Validate and print the reviewed plan digest without writing; '
            'may run while Core is live (default).'
        ),
    )
    parser.set_defaults(apply=False)
    parser.add_argument(
        '--expected-plan-sha256',
        help='Required with --apply; copy plan_sha256 from a reviewed dry run.',
    )
    parser.add_argument(
        '--confirm-core-offline',
        action='store_true',
        help=(
            'Required with --apply. Attest that Core and background workers '
            'are stopped, port 8000 is closed, and active DB work is absent. '
            'This flag does not perform those shutdown checks.'
        ),
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        result = eod581_feedback_lineage_repair.repair(
            apply=args.apply,
            expected_plan_sha256=args.expected_plan_sha256,
            confirm_core_offline=bool(args.confirm_core_offline),
        )
    except RepairBlocked as exc:
        result = {
            'status': 'blocked',
            'applied': False,
            'error_type': type(exc).__name__,
            'error_message': str(exc),
        }
        print(_canonical_json(result), file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        result = {
            'status': 'failed',
            'applied': False,
            'error_type': type(exc).__name__,
            'error_message': str(exc),
        }
        print(_canonical_json(result), file=sys.stderr)
        return 1

    print(_canonical_json(result))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
