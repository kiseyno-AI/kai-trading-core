from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
import json
from typing import Any, Mapping, Sequence

from app.agent_context.roles import (
    DECISION_SUPPORT_AGENT_NAMES,
    SUMMARIZER_AGENT_NAMES,
)
from app.agent_context.run_modes import (
    FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION,
    FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION,
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)
from app.eod.summary_cutoff_contract import (
    COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION,
    normalize_symbol_codes,
    stage_result_summary,
)


@dataclass(frozen=True)
class StageCounts:
    total: int
    processed: int
    succeeded: int
    failed: int
    remaining: int


@dataclass(frozen=True)
class FundamentalsRefreshMetrics:
    total: int
    processed: int
    succeeded: int
    failed: int
    remaining: int
    fresh: int
    refreshed: int
    events_detected: int
    events_captured: int
    events_failed: int


def build_next_open_window_expired_result(
    *,
    market_date: str,
    market: object,
    allow_same_day_intraday_watch_continuation: bool = False,
    allow_previous_session_preopen_recovery: bool = False,
) -> dict[str, Any] | None:
    """Return an auditable no-op when a recovered plan's window has passed."""

    market_state = str(getattr(market, 'state', '') or '').strip().upper()

    server_time = str(getattr(market, 'server_time', '') or '')
    try:
        current_market_date = date.fromisoformat(server_time[:10])
        source_market_date = date.fromisoformat(market_date)
    except ValueError:
        return None

    from app.orders.next_open_execution import normalize_next_open_target_trade_date

    candidate = source_market_date + timedelta(days=1)
    target_trade_date = normalize_next_open_target_trade_date(
        candidate,
        default=candidate,
        allow_fallback=True,
    )
    target_is_still_actionable = bool(
        (
            market_state == 'PREOPEN'
            and (
                allow_same_day_intraday_watch_continuation
                or allow_previous_session_preopen_recovery
            )
            and target_trade_date >= current_market_date
        )
        or target_trade_date > current_market_date
        or (
            allow_same_day_intraday_watch_continuation
            and market_state == 'OPEN'
            and target_trade_date == current_market_date
        )
    )
    if target_is_still_actionable:
        return None
    return {
        'triggered': False,
        'status': 'skipped',
        'reason': 'next_open_window_expired',
        'market_date': source_market_date.isoformat(),
        'target_trade_date': target_trade_date.isoformat(),
        'current_market_date': current_market_date.isoformat(),
        'market_state': market_state,
    }


EXPIRED_DECISION_REPLAY_STAGE_IDS: frozenset[str] = frozenset(
    {
        'agent_feedback_summaries',
        'signal_decider',
        'signal_decider_evaluations',
    }
)

AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER: tuple[str, ...] = (
    'signal_decider',
    'next_open_execution_plan',
    'signal_decider_evaluations',
)
AUTO_PITR_PREOPEN_RECOVERY_STAGE_ORDERS: tuple[tuple[str, ...], ...] = (
    (
        'agent_feedback_summaries',
        *AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER,
    ),
    AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER,
)
NONBLOCKING_PARTIAL_PROGRESSION_SCHEMA_VERSION = (
    'nonblocking_partial_progression_v1'
)
NONBLOCKING_FEEDBACK_RETRY_DEBT_SCHEMA_VERSION = (
    'nonblocking_feedback_retry_debt_v1'
)
NONBLOCKING_DOWNSTREAM_SCOPE_PROOF_SCHEMA_VERSION = (
    'nonblocking_downstream_scope_v1'
)
SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY = (
    'same_day_intraday_watch_rearm'
)
SAME_DAY_INTRADAY_WATCH_REARM_VERSION = 1
SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT = 8


def nonblocking_partial_progression_digest(
    marker: Mapping[str, Any],
) -> str:
    payload = {
        str(key): value
        for key, value in marker.items()
        if str(key) != 'proof_digest'
    }
    serialized = json.dumps(
        payload,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True,
    )
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()


def validated_nonblocking_downstream_scope_proof(
    stage_id: str,
    proof: object,
) -> dict[str, Any] | None:
    """Return a canonical downstream scope proof or fail closed.

    These small proofs are persisted outside the compact result summary so a
    large plan/evaluation result cannot lose the exact symbol boundary needed
    to validate non-blocking partial progression after a restart.
    """

    if not isinstance(proof, Mapping):
        return None
    proof = dict(proof)
    if proof.get('schema_version') != (
        NONBLOCKING_DOWNSTREAM_SCOPE_PROOF_SCHEMA_VERSION
    ):
        return None

    if stage_id == 'next_open_execution_plan':
        if set(proof) != {
            'schema_version',
            'kind',
            'active_symbols',
            'active_scope',
            'item_count',
            'active_item_count',
            'blocked_item_count',
        } or proof.get('kind') != 'next_open_plan_active_scope':
            return None
        active_symbols = proof.get('active_symbols')
        active_scope = proof.get('active_scope')
        item_count = proof.get('item_count')
        active_item_count = proof.get('active_item_count')
        blocked_item_count = proof.get('blocked_item_count')
        if not (
            isinstance(active_symbols, list)
            and normalize_symbol_codes(active_symbols) == active_symbols
            and isinstance(active_scope, list)
            and all(
                type(value) is int and value >= 0
                for value in (
                    item_count,
                    active_item_count,
                    blocked_item_count,
                )
            )
            and active_item_count + blocked_item_count == item_count
            and len(active_symbols) <= active_item_count
            and bool(active_symbols) == bool(active_item_count)
        ):
            return None
        canonical_active_scope: list[dict[str, str]] = []
        for raw_scope in active_scope:
            if not (
                isinstance(raw_scope, Mapping)
                and set(raw_scope)
                == {'symbol', 'decision_agent_name', 'decision_run_id'}
            ):
                return None
            normalized_symbol = normalize_symbol_codes(
                [raw_scope.get('symbol')]
            )
            decision_agent_name = str(
                raw_scope.get('decision_agent_name') or ''
            ).strip()
            decision_run_id = str(
                raw_scope.get('decision_run_id') or ''
            ).strip()
            if not (
                len(normalized_symbol) == 1
                and decision_agent_name in DECISION_SUPPORT_AGENT_NAMES
                and decision_run_id
            ):
                return None
            canonical_active_scope.append(
                {
                    'symbol': normalized_symbol[0],
                    'decision_agent_name': decision_agent_name,
                    'decision_run_id': decision_run_id,
                }
            )
        canonical_scope_tuples = sorted(
            {
                (
                    scope['symbol'],
                    scope['decision_agent_name'],
                    scope['decision_run_id'],
                )
                for scope in canonical_active_scope
            }
        )
        normalized_scope_payload = [
            {
                'symbol': symbol,
                'decision_agent_name': decision_agent_name,
                'decision_run_id': decision_run_id,
            }
            for symbol, decision_agent_name, decision_run_id
            in canonical_scope_tuples
        ]
        if not (
            active_scope == normalized_scope_payload
            and normalize_symbol_codes(
                [scope['symbol'] for scope in normalized_scope_payload]
            )
            == active_symbols
            and len(active_scope) <= active_item_count
        ):
            return None
        return proof

    if stage_id == 'signal_decider_evaluations':
        if set(proof) != {
            'schema_version',
            'kind',
            'requested_symbols',
            'requested_symbol_count',
        } or proof.get('kind') != 'signal_decider_evaluation_scope':
            return None
        requested_symbols = proof.get('requested_symbols')
        requested_symbol_count = proof.get('requested_symbol_count')
        if not (
            isinstance(requested_symbols, list)
            and requested_symbols
            and normalize_symbol_codes(requested_symbols)
            == requested_symbols
            and type(requested_symbol_count) is int
            and requested_symbol_count == len(requested_symbols)
        ):
            return None
        return proof

    return None


def build_nonblocking_downstream_scope_proof(
    stage_id: str,
    result: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Derive the exact plan/evaluation symbol scope from a full result."""

    if not isinstance(result, Mapping):
        return None

    if stage_id == 'next_open_execution_plan':
        items = result.get('items')
        if not isinstance(items, list):
            return None
        active_scope: list[dict[str, str]] = []
        active_item_count = 0
        blocked_item_count = 0
        for item in items:
            if not isinstance(item, Mapping):
                return None
            normalized_symbol = normalize_symbol_codes(
                [item.get('symbol')]
            )
            if len(normalized_symbol) != 1:
                return None
            eligibility_status = str(
                item.get('eligibility_status') or ''
            ).strip().lower()
            handoff_active = item.get('handoff_active')
            exec_monitoring_active = item.get('exec_monitoring_active')
            if not (
                type(handoff_active) is bool
                and type(exec_monitoring_active) is bool
            ):
                return None
            active = bool(
                eligibility_status in {'eligible', 'monitor_candidate'}
                or handoff_active
                or exec_monitoring_active
            )
            if active:
                decision_agent_name = str(
                    item.get('decision_agent_name')
                    or item.get('agent_name')
                    or ''
                ).strip()
                decision_run_id = str(
                    item.get('decision_run_id') or ''
                ).strip()
                if not (
                    decision_agent_name in DECISION_SUPPORT_AGENT_NAMES
                    and decision_run_id
                ):
                    return None
                active_item_count += 1
                active_scope.append(
                    {
                        'symbol': normalized_symbol[0],
                        'decision_agent_name': decision_agent_name,
                        'decision_run_id': decision_run_id,
                    }
                )
                continue
            if not (
                eligibility_status == 'blocked'
                and not handoff_active
                and not exec_monitoring_active
            ):
                return None
            blocked_item_count += 1
        active_scope_tuples = sorted(
            {
                (
                    scope['symbol'],
                    scope['decision_agent_name'],
                    scope['decision_run_id'],
                )
                for scope in active_scope
            }
        )
        active_scope_payload = [
            {
                'symbol': symbol,
                'decision_agent_name': decision_agent_name,
                'decision_run_id': decision_run_id,
            }
            for symbol, decision_agent_name, decision_run_id
            in active_scope_tuples
        ]
        proof = {
            'schema_version': (
                NONBLOCKING_DOWNSTREAM_SCOPE_PROOF_SCHEMA_VERSION
            ),
            'kind': 'next_open_plan_active_scope',
            'active_symbols': normalize_symbol_codes(
                [scope['symbol'] for scope in active_scope_payload]
            ),
            'active_scope': active_scope_payload,
            'item_count': len(items),
            'active_item_count': active_item_count,
            'blocked_item_count': blocked_item_count,
        }
        return validated_nonblocking_downstream_scope_proof(
            stage_id,
            proof,
        )

    if stage_id == 'signal_decider_evaluations':
        request = result.get('request')
        raw_symbols = (
            request.get('symbols')
            if isinstance(request, Mapping)
            else None
        )
        if not isinstance(raw_symbols, list):
            return None
        requested_symbols = normalize_symbol_codes(raw_symbols)
        if not requested_symbols or requested_symbols != raw_symbols:
            return None
        proof = {
            'schema_version': (
                NONBLOCKING_DOWNSTREAM_SCOPE_PROOF_SCHEMA_VERSION
            ),
            'kind': 'signal_decider_evaluation_scope',
            'requested_symbols': requested_symbols,
            'requested_symbol_count': len(requested_symbols),
        }
        return validated_nonblocking_downstream_scope_proof(
            stage_id,
            proof,
        )

    return None


def validated_nonblocking_partial_progression_marker(
    recovery_result: Mapping[str, Any] | None,
    *,
    expected_eod_collection_run_id: int | None = None,
) -> dict[str, Any] | None:
    """Validate the exact proof that isolated feedback failures are safe.

    The pipeline remains truthfully ``partial``.  This marker only proves that
    every failed role/symbol pair was removed from the cohort that reached the
    plan and evaluation stages, so the remaining retry debt need not freeze
    unrelated symbols.
    """

    if not isinstance(recovery_result, Mapping):
        return None
    pipeline_run = recovery_result.get('pipeline_run')
    stages = recovery_result.get('stages')
    if not isinstance(pipeline_run, Mapping) or not isinstance(stages, Mapping):
        return None
    metadata = pipeline_run.get('run_metadata')
    if not isinstance(metadata, Mapping):
        return None
    marker = metadata.get('nonblocking_partial_progression')
    if not isinstance(marker, Mapping):
        return None
    marker = dict(marker)

    pipeline_run_id = pipeline_run.get('id')
    result_pipeline_run_id = recovery_result.get('pipeline_run_id')
    eod_run_id = metadata.get('reused_eod_collection_run_id')
    resolved_stages = metadata.get('resolved_stages')
    if not (
        marker.get('schema_version')
        == NONBLOCKING_PARTIAL_PROGRESSION_SCHEMA_VERSION
        and marker.get('blocking_status') == 'non_blocking'
        and str(marker.get('proof_digest') or '')
        == nonblocking_partial_progression_digest(marker)
        and type(pipeline_run_id) is int
        and pipeline_run_id > 0
        and result_pipeline_run_id == pipeline_run_id
        and marker.get('pipeline_run_id') == pipeline_run_id
        and type(eod_run_id) is int
        and eod_run_id > 0
        and marker.get('reused_eod_collection_run_id') == eod_run_id
        and (
            expected_eod_collection_run_id is None
            or eod_run_id == expected_eod_collection_run_id
        )
        and str(recovery_result.get('market_date') or '')
        == str(marker.get('market_date') or '')
        == str(pipeline_run.get('market_date') or '')
        and str(recovery_result.get('status') or '').strip().lower()
        == 'partial'
        and str(pipeline_run.get('status') or '').strip().lower()
        == 'partial'
        and pipeline_run.get('trigger') == 'auto_eod_followup_resume'
        and metadata.get('resumable') is False
        and str(pipeline_run.get('completed_at') or '').strip()
        and not str(pipeline_run.get('error_message') or '').strip()
        and isinstance(resolved_stages, list)
        and tuple(resolved_stages)
        in AUTO_PITR_PREOPEN_RECOVERY_STAGE_ORDERS
        and pipeline_run.get('current_stage') == resolved_stages[-1]
        and set(stages) == set(resolved_stages)
    ):
        return None

    def canonical_symbols(value: object) -> list[str] | None:
        if not isinstance(value, list):
            return None
        normalized = normalize_symbol_codes(value)
        if not normalized or normalized != value:
            return None
        return normalized

    def failed_scope(
        payload: Mapping[str, Any],
        *,
        roles: Sequence[str],
    ) -> tuple[dict[str, list[str]], set[str]] | None:
        raw = payload.get('failed_symbols_by_role')
        if not isinstance(raw, Mapping) or set(raw) != set(roles):
            return None
        normalized: dict[str, list[str]] = {}
        failed: set[str] = set()
        for role in roles:
            symbols = raw.get(role)
            if not isinstance(symbols, list):
                return None
            normalized_symbols = normalize_symbol_codes(symbols)
            if normalized_symbols != symbols:
                return None
            normalized[role] = normalized_symbols
            failed.update(normalized_symbols)
        return normalized, failed

    summary = marker.get('summary')
    decider = marker.get('decider')
    downstream = marker.get('downstream')
    completion_authority = marker.get('completion_authority')
    retry_debt = marker.get('retry_debt')
    if not all(
        isinstance(payload, Mapping)
        for payload in (
            summary,
            decider,
            downstream,
            completion_authority,
            retry_debt,
        )
    ):
        return None
    summary_admitted = canonical_symbols(summary.get('admitted_symbols'))
    summary_completed = canonical_symbols(summary.get('completed_symbols'))
    decider_admitted = canonical_symbols(decider.get('admitted_symbols'))
    decider_completed = canonical_symbols(decider.get('completed_symbols'))
    actionable = canonical_symbols(downstream.get('actionable_symbols'))
    plan_scope_proof = validated_nonblocking_downstream_scope_proof(
        'next_open_execution_plan',
        downstream.get('plan_scope_proof'),
    )
    evaluation_scope_proof = validated_nonblocking_downstream_scope_proof(
        'signal_decider_evaluations',
        downstream.get('evaluation_scope_proof'),
    )
    summary_failed_scope = failed_scope(
        summary,
        roles=SUMMARIZER_AGENT_NAMES,
    )
    decider_failed_scope = failed_scope(
        decider,
        roles=DECISION_SUPPORT_AGENT_NAMES,
    )
    if not all(
        value is not None
        for value in (
            summary_admitted,
            summary_completed,
            decider_admitted,
            decider_completed,
            actionable,
            plan_scope_proof,
            evaluation_scope_proof,
            summary_failed_scope,
            decider_failed_scope,
        )
    ):
        return None
    summary_failed = summary_failed_scope[1]
    decider_failed = decider_failed_scope[1]

    raw_decision_identities = completion_authority.get(
        'decision_identities'
    )
    if not (
        set(completion_authority)
        == {
            'schema_version',
            'parent_run_id',
            'parent_summary_run_id',
            'completed_symbols',
            'decision_identities',
            'signal_decider_proof_digest',
            'next_open_execution_plan_proof_digest',
            'signal_decider_evaluations_proof_digest',
            'next_open_manifest_digest',
        }
        and completion_authority.get('schema_version')
        == 'nonblocking_strict_completion_authority_v1'
        and completion_authority.get('parent_run_id')
        == decider.get('source_run_id')
        and completion_authority.get('parent_summary_run_id')
        == decider.get('source_summary_run_id')
        and completion_authority.get('completed_symbols') == actionable
        and isinstance(raw_decision_identities, list)
        and all(
            isinstance(completion_authority.get(field), str)
            and len(str(completion_authority.get(field))) == 64
            for field in (
                'signal_decider_proof_digest',
                'next_open_execution_plan_proof_digest',
                'signal_decider_evaluations_proof_digest',
                'next_open_manifest_digest',
            )
        )
    ):
        return None
    decision_identities: list[dict[str, Any]] = []
    seen_identity_keys: set[tuple[str, str]] = set()
    for raw_identity in raw_decision_identities:
        if not isinstance(raw_identity, Mapping):
            return None
        identity = dict(raw_identity)
        if set(identity) != {
            'symbol',
            'decision_agent_name',
            'decision_snapshot_id',
            'decision_run_id',
        }:
            return None
        normalized_symbol = normalize_symbol_codes(
            [identity.get('symbol')]
        )
        role = str(
            identity.get('decision_agent_name') or ''
        ).strip()
        snapshot_id = identity.get('decision_snapshot_id')
        decision_run_id = str(
            identity.get('decision_run_id') or ''
        ).strip()
        key = (
            normalized_symbol[0] if normalized_symbol else '',
            role,
        )
        if not (
            len(normalized_symbol) == 1
            and identity.get('symbol') == normalized_symbol[0]
            and normalized_symbol[0] in actionable
            and role in DECISION_SUPPORT_AGENT_NAMES
            and type(snapshot_id) is int
            and snapshot_id > 0
            and decision_run_id
            and key not in seen_identity_keys
        ):
            return None
        seen_identity_keys.add(key)
        decision_identities.append(
            {
                'symbol': normalized_symbol[0],
                'decision_agent_name': role,
                'decision_snapshot_id': snapshot_id,
                'decision_run_id': decision_run_id,
            }
        )
    decision_identities.sort(
        key=lambda identity: (
            identity['symbol'],
            identity['decision_agent_name'],
        )
    )
    if not (
        decision_identities == raw_decision_identities
        and seen_identity_keys
        == {
            (symbol, role)
            for symbol in actionable
            for role in DECISION_SUPPORT_AGENT_NAMES
        }
    ):
        return None

    def terminal_counts_are_exact(
        payload: Mapping[str, Any],
        *,
        roles: Sequence[str],
        admitted_symbols: Sequence[str],
        failures_by_role: Mapping[str, Sequence[str]],
    ) -> bool:
        total_items = payload.get('total_items')
        completed_items = payload.get('completed_items')
        failed_items = payload.get('failed_items')
        expected_total = len(admitted_symbols) * len(roles)
        expected_failed = sum(
            len(failures_by_role[role]) for role in roles
        )
        return bool(
            type(total_items) is int
            and type(completed_items) is int
            and type(failed_items) is int
            and total_items == expected_total
            and failed_items == expected_failed
            and completed_items == expected_total - expected_failed
        )

    if not (
        str(summary.get('source_run_id') or '').strip()
        and summary.get('feedback_scope_mode')
        in {
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        }
        and str(decider.get('source_run_id') or '').strip()
        and set(summary_completed)
        == set(summary_admitted) - summary_failed
        and set(decider_admitted) == set(summary_completed)
        and set(decider_completed)
        == set(decider_admitted) - decider_failed
        and actionable == decider_completed
        and (summary_failed or decider_failed)
        and downstream.get('plan_status') == 'completed'
        and downstream.get('evaluation_status') == 'completed'
        and evaluation_scope_proof['requested_symbols'] == actionable
        and set(plan_scope_proof['active_symbols']) <= set(actionable)
        and retry_debt.get('schema_version')
        == NONBLOCKING_FEEDBACK_RETRY_DEBT_SCHEMA_VERSION
        and retry_debt.get('summary_failed_symbols_by_role')
        == summary_failed_scope[0]
        and retry_debt.get('decider_failed_symbols_by_role')
        == decider_failed_scope[0]
        and terminal_counts_are_exact(
            summary,
            roles=SUMMARIZER_AGENT_NAMES,
            admitted_symbols=summary_admitted,
            failures_by_role=summary_failed_scope[0],
        )
        and terminal_counts_are_exact(
            decider,
            roles=DECISION_SUPPORT_AGENT_NAMES,
            admitted_symbols=decider_admitted,
            failures_by_role=decider_failed_scope[0],
        )
    ):
        return None

    raw_cutoff_at = summary.get('knowledge_cutoff_at')
    try:
        cutoff_at = datetime.fromisoformat(str(raw_cutoff_at or ''))
    except ValueError:
        return None
    if cutoff_at.tzinfo is None or cutoff_at.utcoffset() is None:
        return None
    persisted_cutoff = metadata.get('feedback_knowledge_cutoff')
    expected_completed_scope_proof = {
        'schema_version': (
            COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION
        ),
        'completed_symbols': summary_completed,
        'admitted_symbols': summary_admitted,
    }
    expected_terminal_scope_proof = (
        {
            'schema_version': 'terminal_role_success_intersection_v1',
            'failed_symbols_by_role': summary_failed_scope[0],
            'total_items': summary['total_items'],
            'completed_items': summary['completed_items'],
            'failed_items': summary['failed_items'],
        }
        if summary_failed
        else None
    )
    if not (
        isinstance(persisted_cutoff, Mapping)
        and persisted_cutoff.get('ready') is True
        and persisted_cutoff.get('source_run_id')
        == summary.get('source_run_id')
        and persisted_cutoff.get('feedback_scope_mode')
        == summary.get('feedback_scope_mode')
        and persisted_cutoff.get('knowledge_cutoff_at')
        == summary.get('knowledge_cutoff_at')
        and persisted_cutoff.get('requested_symbols')
        == summary_completed
        and persisted_cutoff.get('completed_scope_proof')
        == expected_completed_scope_proof
        and persisted_cutoff.get('terminal_role_scope_proof')
        == expected_terminal_scope_proof
        and decider.get('source_summary_run_id')
        == summary.get('source_run_id')
    ):
        return None

    def exact_stage_source(stage: Mapping[str, Any], source: str) -> bool:
        source_run_ids = (
            stage.get('source_run_ids')
            if isinstance(stage.get('source_run_ids'), Mapping)
            else {}
        )
        result = (
            stage.get('result')
            if isinstance(stage.get('result'), Mapping)
            else stage_result_summary(stage)
        )
        observed = {
            str(value or '').strip()
            for value in (
                source_run_ids.get('agent_feedback_run_id'),
                source_run_ids.get('run_id'),
                result.get('run_id'),
            )
            if str(value or '').strip()
        }
        return observed == {source}

    def terminal_stage_matches_marker(
        stage: Mapping[str, Any],
        *,
        roles: Sequence[str],
        admitted_symbols: list[str],
        completed_symbols: list[str],
        failures_by_role: Mapping[str, list[str]],
        source_run_id: str,
        marker_payload: Mapping[str, Any],
        require_scope_authority: bool,
    ) -> bool:
        if not exact_stage_source(stage, source_run_id):
            return False
        has_uncompacted_result = isinstance(stage.get('result'), Mapping)
        result = (
            stage.get('result')
            if has_uncompacted_result
            else stage_result_summary(stage)
        )
        raw_roles = result.get('roles')
        if not (
            isinstance(raw_roles, list)
            and len(raw_roles) == len(roles)
            and set(raw_roles) == set(roles)
        ):
            return False
        requested_symbols = result.get('requested_symbols')
        if (
            requested_symbols is not None
            and requested_symbols != admitted_symbols
        ):
            return False
        if require_scope_authority:
            scope_authority = result.get('scope_authority')
            if not (
                isinstance(scope_authority, Mapping)
                and scope_authority.get('schema_version')
                == FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION
                and scope_authority.get('kind')
                == (
                    FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION
                )
                and scope_authority.get('source_summary_run_id')
                == summary.get('source_run_id')
                and scope_authority.get('source_feedback_scope_mode')
                == summary.get('feedback_scope_mode')
                and scope_authority.get('decision_feedback_scope_mode')
                == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                and scope_authority.get('knowledge_cutoff_at')
                == summary.get('knowledge_cutoff_at')
                and (
                    not has_uncompacted_result
                    or scope_authority.get('completed_scope_proof')
                    == expected_completed_scope_proof
                )
            ):
                return False

        progress = result.get('progress')
        progress_summary = (
            progress.get('summary')
            if isinstance(progress, Mapping)
            and isinstance(progress.get('summary'), Mapping)
            else None
        )
        if progress_summary is not None:
            # Lazy import avoids the repository -> run-stage-support ->
            # retry-service -> market-data-store repository cycle.
            from app.agent_context.retry_service import (
                resolve_terminal_role_success_intersection,
            )

            exact_resolution = resolve_terminal_role_success_intersection(
                progress_summary=progress_summary,
                roles=roles,
                expected_role_symbol_scopes={
                    role: admitted_symbols for role in roles
                },
            )
            if exact_resolution.get('ready') is True:
                return bool(
                    exact_resolution.get('completed_symbols')
                    == completed_symbols
                    and exact_resolution.get('failed_symbols_by_role')
                    == failures_by_role
                    and exact_resolution.get('total_items')
                    == marker_payload.get('total_items')
                    and exact_resolution.get('completed_items')
                    == marker_payload.get('completed_items')
                    and exact_resolution.get('failed_items')
                    == marker_payload.get('failed_items')
                )

        stage_counts = tuple(
            stage.get(field)
            for field in ('processed', 'total', 'succeeded', 'failed')
        )
        if not all(type(value) is int for value in stage_counts):
            return False
        processed, total, succeeded, failed = stage_counts
        return bool(
            total == marker_payload.get('total_items')
            and succeeded == marker_payload.get('completed_items')
            and failed == marker_payload.get('failed_items')
            and processed == succeeded + failed
        )

    def stage_status(stage_id: str) -> str:
        stage = stages.get(stage_id)
        return (
            str(stage.get('status') or '').strip().lower()
            if isinstance(stage, Mapping)
            else ''
        )

    if not (
        stage_status('signal_decider') in {'completed', 'partial'}
        and stage_status('next_open_execution_plan') == 'completed'
        and stage_status('signal_decider_evaluations') == 'completed'
        and (
            'agent_feedback_summaries' not in resolved_stages
            or stage_status('agent_feedback_summaries')
            in {'completed', 'partial'}
        )
    ):
        return None
    summary_stage = stages.get('agent_feedback_summaries')
    if (
        summary_stage is not None
        and not (
            isinstance(summary_stage, Mapping)
            and terminal_stage_matches_marker(
                summary_stage,
                roles=SUMMARIZER_AGENT_NAMES,
                admitted_symbols=summary_admitted,
                completed_symbols=summary_completed,
                failures_by_role=summary_failed_scope[0],
                source_run_id=str(summary['source_run_id']),
                marker_payload=summary,
                require_scope_authority=False,
            )
        )
    ):
        return None
    signal_stage = stages.get('signal_decider')
    if not (
        isinstance(signal_stage, Mapping)
        and terminal_stage_matches_marker(
            signal_stage,
            roles=DECISION_SUPPORT_AGENT_NAMES,
            admitted_symbols=decider_admitted,
            completed_symbols=decider_completed,
            failures_by_role=decider_failed_scope[0],
            source_run_id=str(decider['source_run_id']),
            marker_payload=decider,
            require_scope_authority=True,
        )
    ):
        return None

    # Lazy import avoids the daily-pipeline -> run-stage-support import cycle.
    from app.eod.daily_pipeline_stages import DailyPipelineStageService

    def validated_completion_scope_proof(
        stage_id: str,
        *,
        parent_scope_proof: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        stage = stages.get(stage_id)
        if not isinstance(stage, Mapping):
            return None
        details = stage.get('details')
        persisted_proof = (
            details.get('completion_scope_proof')
            if isinstance(details, Mapping)
            else None
        )
        validated = (
            DailyPipelineStageService
            .validated_stage_completion_scope_proof(
                stage_id,
                persisted_proof,
            )
        )
        if validated is not None:
            return validated
        result = stage.get('result')
        if not isinstance(result, Mapping):
            return None
        if stage_id == 'signal_decider':
            return (
                DailyPipelineStageService
                ._terminal_decider_progression_proof(
                    dict(stage),
                    expected_decider_symbols=decider_admitted,
                    feedback_cutoff_resolution=dict(persisted_cutoff),
                )
            )
        if stage_id == 'next_open_execution_plan':
            return (
                DailyPipelineStageService
                ._build_next_open_completion_scope_proof(
                    result,
                    parent_scope_proof=parent_scope_proof,
                )
            )
        if stage_id == 'signal_decider_evaluations':
            return (
                DailyPipelineStageService
                ._build_evaluations_completion_scope_proof(
                    result,
                    parent_scope_proof=parent_scope_proof,
                )
            )
        return None

    signal_completion_proof = validated_completion_scope_proof(
        'signal_decider'
    )
    plan_completion_proof = validated_completion_scope_proof(
        'next_open_execution_plan',
        parent_scope_proof=signal_completion_proof,
    )
    evaluation_completion_proof = validated_completion_scope_proof(
        'signal_decider_evaluations',
        parent_scope_proof=signal_completion_proof,
    )
    if not (
        isinstance(signal_completion_proof, Mapping)
        and isinstance(plan_completion_proof, Mapping)
        and isinstance(evaluation_completion_proof, Mapping)
        and signal_completion_proof.get('proof_digest')
        == completion_authority.get('signal_decider_proof_digest')
        and plan_completion_proof.get('proof_digest')
        == completion_authority.get(
            'next_open_execution_plan_proof_digest'
        )
        and evaluation_completion_proof.get('proof_digest')
        == completion_authority.get(
            'signal_decider_evaluations_proof_digest'
        )
        and plan_completion_proof.get('manifest_digest')
        == completion_authority.get('next_open_manifest_digest')
        and signal_completion_proof.get('source_run_id')
        == completion_authority.get('parent_run_id')
        and signal_completion_proof.get('source_summary_run_id')
        == completion_authority.get('parent_summary_run_id')
        and signal_completion_proof.get('completed_symbols')
        == actionable
        and signal_completion_proof.get('decision_identities')
        == decision_identities
        and plan_completion_proof.get('parent_run_id')
        == completion_authority.get('parent_run_id')
        and plan_completion_proof.get('parent_completed_symbols')
        == actionable
        and evaluation_completion_proof.get('parent_run_id')
        == completion_authority.get('parent_run_id')
        and evaluation_completion_proof.get(
            'parent_completed_symbols'
        )
        == actionable
    ):
        return None

    expected_identity_by_key = {
        (
            identity['symbol'],
            identity['decision_agent_name'],
        ): (
            identity['decision_snapshot_id'],
            identity['decision_run_id'],
        )
        for identity in decision_identities
    }
    plan_identity_by_key = {
        (
            entry['symbol'],
            entry['decision_agent_name'],
        ): (
            entry['decision_snapshot_id'],
            entry['decision_run_id'],
        )
        for entry in plan_completion_proof['decision_partition']
    }
    expected_signal_identity_by_symbol = {
        symbol: expected_identity_by_key[(symbol, 'signal-decider')]
        for symbol in actionable
    }
    evaluation_identities_by_symbol: dict[
        str,
        set[tuple[int, str]],
    ] = {}
    for entry in evaluation_completion_proof['decision_horizon_scope']:
        evaluation_identities_by_symbol.setdefault(
            entry['symbol'],
            set(),
        ).add(
            (
                entry['snapshot_id'],
                entry['decision_run_id'],
            )
        )
    if not (
        plan_completion_proof.get('parent_summary_run_id')
        == completion_authority.get('parent_summary_run_id')
        and plan_completion_proof.get('parent_admitted_symbols')
        == decider_admitted
        and plan_identity_by_key == expected_identity_by_key
        and evaluation_completion_proof.get('parent_summary_run_id')
        == completion_authority.get('parent_summary_run_id')
        and evaluation_completion_proof.get('request_symbols')
        == actionable
        and evaluation_completion_proof.get('decision_count')
        == len(actionable)
        and evaluation_identities_by_symbol
        == {
            symbol: {identity}
            for symbol, identity
            in expected_signal_identity_by_symbol.items()
        }
    ):
        return None

    def stage_downstream_scope_proof(
        stage_id: str,
    ) -> dict[str, Any] | None:
        stage = stages.get(stage_id)
        if not isinstance(stage, Mapping):
            return None
        result = stage.get('result')
        if isinstance(result, Mapping):
            return build_nonblocking_downstream_scope_proof(
                stage_id,
                result,
            )
        details = stage.get('details')
        persisted_proof = (
            details.get('nonblocking_scope_proof')
            if isinstance(details, Mapping)
            else None
        )
        return validated_nonblocking_downstream_scope_proof(
            stage_id,
            persisted_proof,
        )

    if not (
        stage_downstream_scope_proof('next_open_execution_plan')
        == plan_scope_proof
        and stage_downstream_scope_proof(
            'signal_decider_evaluations'
        )
        == evaluation_scope_proof
    ):
        return None
    return marker


def pending_same_day_intraday_watch_rearm_marker(
    marker: object,
    *,
    target_trade_date: date | str | None = None,
) -> dict[str, Any] | None:
    """Return a normalized pending marker only when its identity is complete."""

    if not isinstance(marker, Mapping):
        return None
    source_run_id = marker.get('source_false_skip_run_id')
    proof_run_id = marker.get('proof_pipeline_run_id')
    attempt_limit = marker.get('attempt_limit')
    raw_target_trade_date = marker.get('target_trade_date')
    raw_started_at = marker.get('started_at')
    try:
        parsed_target_trade_date = date.fromisoformat(
            str(raw_target_trade_date or '')
        )
        parsed_started_at = datetime.fromisoformat(str(raw_started_at or ''))
    except (TypeError, ValueError):
        return None
    if parsed_started_at.tzinfo is None or parsed_started_at.utcoffset() is None:
        return None
    if target_trade_date is not None:
        try:
            expected_target_trade_date = date.fromisoformat(
                target_trade_date.isoformat()
                if isinstance(target_trade_date, date)
                else str(target_trade_date)
            )
        except (TypeError, ValueError):
            return None
        if parsed_target_trade_date != expected_target_trade_date:
            return None
    if not (
        type(marker.get('version')) is int
        and marker.get('version') == SAME_DAY_INTRADAY_WATCH_REARM_VERSION
        and str(marker.get('status') or '').strip() == 'pending'
        and type(attempt_limit) is int
        and attempt_limit == SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT
        and type(source_run_id) is int
        and type(proof_run_id) is int
        and source_run_id > 0
        and proof_run_id > 0
        and proof_run_id < source_run_id
    ):
        return None
    return dict(marker)


def outcome_contains_reason(
    outcome: object,
    *,
    reason: str,
) -> bool:
    """Find a semantic reason in a nested pipeline result without looping."""

    expected_reason = str(reason or '').strip().lower()
    if not expected_reason:
        return False
    pending: list[object] = [outcome]
    seen: set[int] = set()
    while pending:
        value = pending.pop()
        if id(value) in seen:
            continue
        seen.add(id(value))
        if isinstance(value, Mapping):
            if str(value.get('reason') or '').strip().lower() == expected_reason:
                return True
            pending.extend(value.values())
        elif isinstance(value, (list, tuple)):
            pending.extend(value)
    return False


def is_completed_auto_pitr_intraday_watch_recovery(
    recovery_result: Mapping[str, Any] | None,
    *,
    expected_eod_collection_run_id: int,
) -> bool:
    """Verify terminal evidence before a pending same-day marker is cleared."""

    if not isinstance(recovery_result, Mapping):
        return False
    if outcome_contains_reason(
        recovery_result,
        reason='next_open_window_expired',
    ):
        return False
    pipeline_run = recovery_result.get('pipeline_run')
    stages = recovery_result.get('stages')
    resolved_stages = recovery_result.get('resolved_stages')
    pipeline_run_id = recovery_result.get('pipeline_run_id')
    if not (
        type(pipeline_run_id) is int
        and pipeline_run_id > 0
        and isinstance(pipeline_run, Mapping)
        and pipeline_run.get('id') == pipeline_run_id
        and isinstance(stages, Mapping)
        and isinstance(resolved_stages, (list, tuple))
        and tuple(resolved_stages)
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
        and set(stages) == set(AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER)
    ):
        return False
    metadata = get_run_metadata(pipeline_run)
    if not (
        metadata.get('entrypoint') == 'daily_pipeline_stage_service.run'
        and metadata.get('allow_expired_analysis_replay') is False
        and metadata.get('same_day_intraday_watch_continuation_admitted')
        is True
        and tuple(metadata.get('resolved_stages') or ())
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
        and pipeline_run.get('current_stage')
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[-1]
        and str(pipeline_run.get('status') or '').strip().lower()
        == str(recovery_result.get('status') or '').strip().lower()
        and is_exact_auto_pitr_intraday_watch_recovery_request(
            trigger=str(pipeline_run.get('trigger') or ''),
            point_in_time_recovery=metadata.get('point_in_time_recovery'),
            reused_eod_collection_run_id=metadata.get(
                'reused_eod_collection_run_id'
            ),
            force=metadata.get('force'),
            reuse_existing_feedback=metadata.get('reuse_existing_feedback'),
            resume_from_stage=metadata.get('resume_from_stage'),
            resolved_stages=resolved_stages,
        )
        and metadata.get('reused_eod_collection_run_id')
        == expected_eod_collection_run_id
    ):
        return False

    from app.eod.daily_pipeline_outcomes import (
        daily_pipeline_outcome_is_satisfied,
    )

    return bool(
        daily_pipeline_outcome_is_satisfied(recovery_result)
        and all(
            daily_pipeline_outcome_is_satisfied(stages[stage_id])
            for stage_id in AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
        )
    )


def is_exact_auto_pitr_intraday_watch_recovery_request(
    *,
    trigger: str,
    point_in_time_recovery: bool,
    reused_eod_collection_run_id: object,
    force: bool,
    reuse_existing_feedback: bool,
    resume_from_stage: str | None,
    resolved_stages: Sequence[str],
) -> bool:
    """Recognize only the lineage-bound automatic intraday-watch suffix."""

    exact_stages = bool(
        isinstance(resolved_stages, (list, tuple))
        and tuple(resolved_stages)
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
    )
    return bool(
        trigger == 'auto_eod_followup_resume'
        and point_in_time_recovery is True
        and isinstance(reused_eod_collection_run_id, int)
        and not isinstance(reused_eod_collection_run_id, bool)
        and reused_eod_collection_run_id > 0
        and force is False
        and reuse_existing_feedback is True
        and resume_from_stage
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[0]
        and exact_stages
    )


def admits_previous_session_preopen_auto_recovery(
    *,
    market_date: str,
    market: object,
    trigger: str,
    point_in_time_recovery: bool,
    reused_eod_collection_run_id: object,
    force: bool,
    reuse_existing_feedback: bool,
    resume_from_stage: str | None,
    resolved_stages: Sequence[str],
    provider_admission_fence_owned: bool,
    existing_pipeline_run_id: int | None,
) -> bool:
    """Admit only an exact automatic prior-session suffix before the open.

    This admission is intentionally separate from the same-day intraday-watch
    contract: it lets the scheduler finish yesterday's decision suffix during
    today's PREOPEN, but it expires immediately when the market becomes OPEN.
    """

    market_state = str(getattr(market, 'state', '') or '').strip().upper()
    if not (
        market_state == 'PREOPEN'
        and trigger == 'auto_eod_followup_resume'
        and point_in_time_recovery is True
        and isinstance(reused_eod_collection_run_id, int)
        and not isinstance(reused_eod_collection_run_id, bool)
        and reused_eod_collection_run_id > 0
        and force is False
        and reuse_existing_feedback is True
        and provider_admission_fence_owned is True
        and existing_pipeline_run_id is None
        and isinstance(resolved_stages, (list, tuple))
    ):
        return False
    normalized_stages = tuple(resolved_stages)
    if not (
        normalized_stages in AUTO_PITR_PREOPEN_RECOVERY_STAGE_ORDERS
        and resume_from_stage == normalized_stages[0]
    ):
        return False

    server_time = str(getattr(market, 'server_time', '') or '')
    try:
        current_market_date = date.fromisoformat(server_time[:10])
        source_market_date = date.fromisoformat(market_date)
    except ValueError:
        return False

    from app.orders.next_open_execution import normalize_next_open_target_trade_date

    candidate = source_market_date + timedelta(days=1)
    target_trade_date = normalize_next_open_target_trade_date(
        candidate,
        default=candidate,
        allow_fallback=True,
    )
    return target_trade_date == current_market_date


def is_canonical_same_day_signal_rearm_proof(
    pipeline_run: Mapping[str, Any] | None,
    *,
    expected_eod_collection_run_id: int | None = None,
) -> bool:
    """Validate the one immutable signal proof a same-day marker may name."""

    if not isinstance(pipeline_run, Mapping):
        return False
    metadata = get_run_metadata(pipeline_run)
    stages = pipeline_run.get('stages')
    resolved_stages = metadata.get('resolved_stages')
    if not (
        isinstance(stages, Mapping)
        and set(stages)
        == set(AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER)
        and metadata.get('entrypoint')
        == 'daily_pipeline_stage_service.run'
        and metadata.get('allow_expired_analysis_replay') is False
        and metadata.get(
            'same_day_intraday_watch_continuation_admitted'
        ) is True
        and isinstance(resolved_stages, (list, tuple))
        and is_exact_auto_pitr_intraday_watch_recovery_request(
            trigger=str(pipeline_run.get('trigger') or ''),
            point_in_time_recovery=metadata.get(
                'point_in_time_recovery'
            ),
            reused_eod_collection_run_id=metadata.get(
                'reused_eod_collection_run_id'
            ),
            force=metadata.get('force'),
            reuse_existing_feedback=metadata.get(
                'reuse_existing_feedback'
            ),
            resume_from_stage=metadata.get('resume_from_stage'),
            resolved_stages=resolved_stages,
        )
        and (
            expected_eod_collection_run_id is None
            or metadata.get('reused_eod_collection_run_id')
            == expected_eod_collection_run_id
        )
        and str(pipeline_run.get('status') or '').strip().lower()
        == 'blocked'
        and pipeline_run.get('current_stage')
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[-1]
    ):
        return False

    cutoff = metadata.get('feedback_knowledge_cutoff')
    raw_cutoff_at = (
        cutoff.get('knowledge_cutoff_at')
        if isinstance(cutoff, Mapping)
        else None
    )
    try:
        cutoff_at = (
            raw_cutoff_at
            if isinstance(raw_cutoff_at, datetime)
            else datetime.fromisoformat(str(raw_cutoff_at or ''))
        )
    except (TypeError, ValueError):
        return False
    requested_symbols = (
        cutoff.get('requested_symbols')
        if isinstance(cutoff, Mapping)
        else None
    )
    if not (
        isinstance(cutoff, Mapping)
        and cutoff.get('ready') is True
        and cutoff_at.tzinfo is not None
        and cutoff_at.utcoffset() is not None
        and str(cutoff.get('source_run_id') or '').strip()
        and str(cutoff.get('feedback_scope_mode') or '').strip()
        in {
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        }
        and str(metadata.get('fs_div') or '').strip().upper()
        in {'CFS', 'OFS'}
        and isinstance(requested_symbols, list)
        and bool(requested_symbols)
        and normalize_symbol_codes(requested_symbols) == requested_symbols
    ):
        return False

    signal_stage = stages.get('signal_decider')
    plan_stage = stages.get('next_open_execution_plan')
    evaluation_stage = stages.get('signal_decider_evaluations')
    if not all(
        isinstance(stage, Mapping)
        for stage in (signal_stage, plan_stage, evaluation_stage)
    ):
        return False
    result_summary = stage_result_summary(signal_stage)
    progress = result_summary.get('progress')
    prerequisites = result_summary.get('prerequisites')
    if not isinstance(prerequisites, Mapping) and isinstance(
        progress,
        Mapping,
    ):
        prerequisites = progress.get('prerequisites')
    summary = (
        progress.get('summary')
        if isinstance(progress, Mapping)
        else None
    )
    role_progress = (
        summary.get('role_progress')
        if isinstance(summary, Mapping)
        else None
    )
    failure_groups = (
        summary.get('failure_groups')
        if isinstance(summary, Mapping)
        else None
    )
    source_run_ids = signal_stage.get('source_run_ids')
    result_run_id = str(result_summary.get('run_id') or '').strip()
    source_ids = {
        str(value or '').strip()
        for value in (
            source_run_ids.values()
            if isinstance(source_run_ids, Mapping)
            else ()
        )
        if str(value or '').strip()
    }
    plan_summary = stage_result_summary(plan_stage)
    evaluation_summary = stage_result_summary(evaluation_stage)
    try:
        total = int(signal_stage.get('total') or 0)
        processed = int(signal_stage.get('processed') or 0)
        succeeded = int(signal_stage.get('succeeded') or 0)
        failed = int(signal_stage.get('failed') or 0)
        remaining = int(signal_stage.get('remaining') or 0)
    except (TypeError, ValueError):
        return False
    if not (
        str(signal_stage.get('status') or '').strip().lower()
        == 'partial'
        and total > 0
        and processed == total
        and failed > 0
        and remaining == 0
        and succeeded + failed == processed
        and result_run_id
        and result_run_id in source_ids
        and len(source_ids) == 1
        and isinstance(source_run_ids, Mapping)
        and set(source_run_ids).issubset(
            {'run_id', 'agent_feedback_run_id'}
        )
        and 'openclaw_dispatch_circuit_breaker' not in result_summary
        and str(plan_stage.get('status') or '').strip().lower()
        == 'blocked'
        and str(evaluation_stage.get('status') or '').strip().lower()
        == 'blocked'
        and str(plan_summary.get('reason') or '').strip()
        == 'signal_decider_stage_incomplete'
        and str(evaluation_summary.get('reason') or '').strip()
        == 'declared_dependencies_incomplete'
        and evaluation_summary.get('incomplete_dependencies')
        == ['signal_decider']
        and isinstance(prerequisites, Mapping)
        and prerequisites.get('ready') is True
        and isinstance(summary, Mapping)
        and str(summary.get('run_status') or '').strip().lower()
        in {'partial', 'failed'}
        and isinstance(role_progress, Mapping)
        and bool(role_progress)
        and set(role_progress).issubset(DECISION_SUPPORT_AGENT_NAMES)
        and all(
            isinstance(role, Mapping)
            for role in role_progress.values()
        )
        and isinstance(failure_groups, Mapping)
        and bool(failure_groups)
    ):
        return False
    try:
        failed_items = int(summary.get('failed_items') or 0)
        retry_pending_items = sum(
            int(role.get('retry_pending_symbols') or 0)
            for role in role_progress.values()
        )
        in_progress_items = sum(
            int(role.get('in_progress_symbols') or 0)
            for role in role_progress.values()
        )
        pending_items = sum(
            int(role.get('pending_symbols') or 0)
            for role in role_progress.values()
        )
        retryable_failure_groups = all(
            isinstance(group, Mapping)
            and int(group.get('count') or 0) > 0
            and int(group.get('retryable_count') or 0)
            == int(group.get('count') or 0)
            for group in failure_groups.values()
        )
    except (TypeError, ValueError):
        return False
    return bool(
        failed_items > 0
        and retry_pending_items == failed_items
        and in_progress_items == 0
        and pending_items == 0
        and retryable_failure_groups
    )
def is_recoverable_same_day_intraday_watch_expiration(
    pipeline_run: Mapping[str, Any] | None,
    *,
    current_market_date: date | str,
) -> bool:
    """Keep an exact same-day OPEN false-skip recoverable as durable debt."""

    if not isinstance(pipeline_run, Mapping):
        return False
    metadata = get_run_metadata(pipeline_run)
    if not (
        pipeline_run.get('status') == 'skipped'
        and pipeline_run.get('current_stage')
        == AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[-1]
        and str(pipeline_run.get('completed_at') or '').strip()
        and not str(pipeline_run.get('error_message') or '').strip()
        and metadata.get('entrypoint')
        == 'daily_pipeline_stage_service.run'
        and metadata.get('allow_expired_analysis_replay') is False
        and metadata.get(
            'same_day_intraday_watch_continuation_admitted'
        )
        is False
    ):
        return False
    resolved_stages = metadata.get('resolved_stages')
    if not isinstance(resolved_stages, (list, tuple)):
        return False
    if not is_exact_auto_pitr_intraday_watch_recovery_request(
        trigger=str(pipeline_run.get('trigger') or ''),
        point_in_time_recovery=metadata.get('point_in_time_recovery'),
        reused_eod_collection_run_id=metadata.get(
            'reused_eod_collection_run_id'
        ),
        force=metadata.get('force'),
        reuse_existing_feedback=metadata.get('reuse_existing_feedback'),
        resume_from_stage=metadata.get('resume_from_stage'),
        resolved_stages=resolved_stages,
    ):
        return False

    try:
        normalized_current_market_date = date.fromisoformat(
            current_market_date.isoformat()[:10]
            if isinstance(current_market_date, date)
            else str(current_market_date)
        )
        source_market_date = date.fromisoformat(
            pipeline_run['market_date'].isoformat()[:10]
            if isinstance(pipeline_run.get('market_date'), date)
            else str(pipeline_run.get('market_date') or '')
        )
    except (KeyError, TypeError, ValueError):
        return False
    if source_market_date >= normalized_current_market_date:
        return False

    raw_stages = pipeline_run.get('stages')
    if not isinstance(raw_stages, Mapping) or set(raw_stages) != set(
        AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
    ):
        return False

    for stage_id in AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER:
        stage_row = raw_stages.get(stage_id)
        if not isinstance(stage_row, Mapping):
            return False
        if stage_row.get('status') != 'skipped':
            return False
        if any(
            stage_row.get(field) != 0
            for field in (
                'processed',
                'total',
                'succeeded',
                'failed',
                'remaining',
            )
        ):
            return False
        source_run_ids = stage_row.get('source_run_ids')
        if not isinstance(source_run_ids, Mapping) or source_run_ids:
            return False
        details = stage_row.get('details')
        result_summary = (
            details.get('result_summary')
            if isinstance(details, Mapping)
            else None
        )
        if not isinstance(result_summary, Mapping):
            return False
        if not (
            result_summary.get('triggered') is False
            and result_summary.get('status') == 'skipped'
            and result_summary.get('reason') == 'next_open_window_expired'
            and result_summary.get('market_state') == 'OPEN'
            and result_summary.get('stage') == stage_id
        ):
            return False
        try:
            result_market_date = date.fromisoformat(
                str(result_summary.get('market_date') or '')
            )
            result_target_trade_date = date.fromisoformat(
                str(result_summary.get('target_trade_date') or '')
            )
            result_current_market_date = date.fromisoformat(
                str(result_summary.get('current_market_date') or '')
            )
        except ValueError:
            return False
        if not (
            result_market_date == source_market_date
            and result_target_trade_date == normalized_current_market_date
            and result_current_market_date == normalized_current_market_date
        ):
            return False

    return True


def build_expired_decision_replay_result(
    *,
    stage_id: str,
    market_date: str,
    market: object,
    allow_same_day_intraday_watch_continuation: bool = False,
    allow_previous_session_preopen_recovery: bool = False,
) -> dict[str, Any] | None:
    """Fence decision replay once its next-open action window has passed."""

    if stage_id not in EXPIRED_DECISION_REPLAY_STAGE_IDS:
        return None
    expired_result = build_next_open_window_expired_result(
        market_date=market_date,
        market=market,
        allow_same_day_intraday_watch_continuation=(
            allow_same_day_intraday_watch_continuation
        ),
        allow_previous_session_preopen_recovery=(
            allow_previous_session_preopen_recovery
        ),
    )
    if expired_result is None:
        return None
    return {
        **expired_result,
        'stage': stage_id,
    }


def admits_same_day_intraday_watch_continuation(
    *,
    market_date: str,
    market: object,
    allow_existing_recovery_resume: bool = False,
) -> bool:
    """Admit only a proven existing recovery during its live target session."""

    market_state = str(getattr(market, 'state', '') or '').strip().upper()
    if not allow_existing_recovery_resume or market_state not in {
        'PREOPEN',
        'OPEN',
    }:
        return False
    server_time = str(getattr(market, 'server_time', '') or '')
    try:
        current_market_date = date.fromisoformat(server_time[:10])
        source_market_date = date.fromisoformat(market_date)
    except ValueError:
        return False

    from app.orders.next_open_execution import normalize_next_open_target_trade_date

    candidate = source_market_date + timedelta(days=1)
    target_trade_date = normalize_next_open_target_trade_date(
        candidate,
        default=candidate,
        allow_fallback=True,
    )
    return target_trade_date == current_market_date


def get_run_metadata(run: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(run, Mapping):
        return {}
    metadata = run.get('run_metadata')
    return metadata if isinstance(metadata, dict) else {}


def normalize_run_status(value: str | None) -> str | None:
    if not value:
        return None
    if value == 'completed_with_errors':
        return 'partial'
    if value == 'skipped':
        return 'pending'
    return value


def resolve_run_updated_at(run: Mapping[str, Any] | None) -> Any | None:
    if not isinstance(run, Mapping):
        return None
    metadata = get_run_metadata(run)
    return metadata.get('last_progress_at') or run.get('completed_at') or run.get('started_at')


def select_preferred_eod_run(rows: Sequence[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return next(
        (
            row
            for row in rows
            if get_run_metadata(row).get('mode') != 'historical_backfill'
        ),
        rows[0],
    )


def build_stage_counts(
    *,
    total: Any,
    succeeded: Any,
    failed: Any,
    processed: Any | None = None,
) -> StageCounts:
    total_value = int(total or 0)
    succeeded_value = int(succeeded or 0)
    failed_value = int(failed or 0)
    processed_value = int(processed or 0) if processed is not None else succeeded_value + failed_value
    return StageCounts(
        total=total_value,
        processed=processed_value,
        succeeded=succeeded_value,
        failed=failed_value,
        remaining=max(total_value - processed_value, 0) if total_value else 0,
    )


def extract_next_open_plan_stage_counts(
    result: Mapping[str, Any],
) -> StageCounts:
    """Count every plan-row outcome without treating a safe block as failure."""

    def _as_nonnegative_int(value: Any) -> int:
        try:
            return max(int(value or 0), 0)
        except (TypeError, ValueError):
            return 0

    summary = result.get('summary')
    if not isinstance(summary, Mapping):
        summary = {}
    outcome_count = sum(
        _as_nonnegative_int(summary.get(key))
        for key in (
            'eligible_count',
            'monitor_candidate_count',
            'blocked_count',
        )
    )
    reported_total = _as_nonnegative_int(summary.get('items_total'))
    total = max(reported_total, outcome_count)
    return build_stage_counts(
        total=total,
        succeeded=outcome_count,
        failed=max(total - outcome_count, 0),
    )


def extract_run_stage_counts(
    run: Mapping[str, Any],
    *,
    total_key: str = 'symbols_total',
    succeeded_key: str = 'symbols_succeeded',
    failed_key: str = 'symbols_failed',
    processed_key: str | None = None,
) -> StageCounts:
    processed = run.get(processed_key) if processed_key is not None else None
    return build_stage_counts(
        total=run.get(total_key),
        succeeded=run.get(succeeded_key),
        failed=run.get(failed_key),
        processed=processed,
    )


def extract_fundamentals_refresh_metrics(run: Mapping[str, Any]) -> FundamentalsRefreshMetrics:
    metadata = get_run_metadata(run)
    fresh = int(run.get('symbols_fresh') or 0)
    refreshed = int(run.get('symbols_refreshed') or 0)
    counts = build_stage_counts(
        total=metadata.get('symbols_selected') or run.get('symbols_total'),
        processed=run.get('symbols_checked'),
        succeeded=fresh + refreshed,
        failed=run.get('symbols_failed'),
    )
    return FundamentalsRefreshMetrics(
        total=counts.total,
        processed=counts.processed,
        succeeded=counts.succeeded,
        failed=counts.failed,
        remaining=counts.remaining,
        fresh=fresh,
        refreshed=refreshed,
        events_detected=int(run.get('events_detected') or 0),
        events_captured=int(run.get('events_captured') or 0),
        events_failed=int(run.get('events_failed') or 0),
    )
