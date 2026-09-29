from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from fastapi.encoders import jsonable_encoder
from sqlalchemy import String, column, func, lateral, or_, select, true, values

from app.agent_context.feedback_service import _normalize_market_date, _normalize_symbols
from app.agent_context.orchestrator_helpers import trim_batch_item_for_prompt
from app.agent_context.roles import (
    DECISION_SUPPORT_AGENT_NAMES,
    DEFAULT_AGENT_NAMES,
    SIGNAL_DECIDER_AGENT_NAME,
)
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    AgentContextFeedbackFailure,
    AgentContextFeedbackRun,
    DailyActionSummary,
    DailyPriceBar,
    DailyPykrxEnrichmentRow,
    DailyTechnicalSnapshot,
    DartDisclosureEvent,
    DartFundamentalsHighlight,
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
    SignalDeciderDecisionSnapshot,
    Symbol,
)


FUNDAMENTAL_SUMMARIZER_AGENT_NAME = 'fundamental-summarizer'
FUNDAMENTAL_SUMMARY_INPUT_FINGERPRINT_SCHEMA_VERSION = 'fundamental_summary_input_fingerprint_v2'
FUNDAMENTAL_SUMMARY_PROMPT_VERSION = 'fundamental_summarizer_prompt_v1'
FUNDAMENTAL_SUMMARY_RESPONSE_SCHEMA_VERSION = 'agent_feedback_response_contract_v1'
FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION = 'fundamental_summary_reuse_policy_v2'
FUNDAMENTAL_SUMMARY_REUSE_REASON = 'reuse_unchanged_fundamental_inputs'
FEEDBACK_REUSE_SYMBOL_CHUNK_SIZE = 50
FEEDBACK_REUSE_KEY_CHUNK_SIZE = 15

FUNDAMENTAL_REUSE_STALE_REASONS = {
    'forced_recompute',
    'fundamental_input_fingerprint_changed',
    'fundamental_input_fingerprint_missing',
    'knowledge_cutoff_mismatch',
    'stored_fundamental_input_fingerprint_missing',
    'source_updated',
    'stale_market_feedback',
}


@dataclass(frozen=True)
class FeedbackReuseDecision:
    symbol: str
    agent_name: str
    should_dispatch: bool
    reason: str
    latest_feedback_id: int | None
    latest_feedback_received_at: str | None
    latest_source_updated_at: str | None
    latest_feedback: dict[str, Any] | None
    latest_decision_snapshot_id: int | None = None
    latest_decision_snapshot_fingerprint: str | None = None
    latest_decision_snapshot_observed_at: str | None = None
    input_fingerprint: str | None = None
    stored_input_fingerprint: str | None = None
    input_fingerprint_payload: dict[str, Any] | None = None
    stored_input_fingerprint_payload: dict[str, Any] | None = None
    reuse_policy_version: str | None = None
    reuse_source: str | None = None


@dataclass
class FeedbackReuseStaticInputs:
    """Run-local cache; current-day receipts and source versions stay live."""

    latest_source_updated_at_by_key: dict[
        tuple[str, str],
        datetime | None,
    ] = field(default_factory=dict)
    fundamental_input_fingerprint_payloads: dict[
        str,
        dict[str, Any] | None,
    ] = field(default_factory=dict)
    fundamental_input_versions_by_symbol: dict[
        str,
        tuple[Any, ...],
    ] = field(default_factory=dict)
    prior_feedback_by_key: dict[
        tuple[str, str],
        Any | None,
    ] = field(default_factory=dict)
    prior_failure_by_key: dict[
        tuple[str, str],
        Any | None,
    ] = field(default_factory=dict)


ROLE_TO_AGENT = {
    role: role
    for role in DEFAULT_AGENT_NAMES
}


def _chunked(values: Sequence[str], size: int) -> list[list[str]]:
    normalized = [str(value) for value in values]
    return [normalized[index:index + size] for index in range(0, len(normalized), size)]


def _isoformat(value: Any) -> str | None:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None:
        return None
    return str(value)


def _stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        jsonable_encoder(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
    )
    return 'sha256:' + hashlib.sha256(encoded.encode('utf-8')).hexdigest()


def _normalize_fingerprint_fs_div(fs_div: str | None) -> str | None:
    if fs_div is None:
        return None
    normalized = str(fs_div).strip().upper()
    if not normalized:
        return None
    if normalized not in {'CFS', 'OFS'}:
        raise ValueError('fs_div must be one of CFS or OFS')
    return normalized


def _resolve_fingerprint_db_fs_div(fs_div: str | None) -> str:
    """Match the configured fundamentals default for independent DB reads."""

    return _normalize_fingerprint_fs_div(
        fs_div or settings.dart_fundamentals_default_fs_div
    ) or 'CFS'


def _without_derived_fingerprint_fields(value: Any) -> Any:
    """Remove request-wide freshness annotations from symbol-local lineage."""

    if isinstance(value, Mapping):
        return {
            str(key): _without_derived_fingerprint_fields(item)
            for key, item in value.items()
            if str(key) not in {
                'freshness_provenance',
                'requested_symbol_coverage',
            }
        }
    if isinstance(value, (list, tuple)):
        return [
            _without_derived_fingerprint_fields(item)
            for item in value
        ]
    return value


def build_fundamental_input_fingerprint_payload_from_context(
    *,
    market_date: str | date,
    symbol: str,
    context: Mapping[str, Any],
    fs_div: str | None,
    disclosures_limit: int,
    knowledge_cutoff_at: datetime | str | None,
) -> dict[str, Any]:
    """Hash finalized visible context and retain source filters for audit."""

    normalized_market_date = _normalize_market_date(market_date)
    normalized_symbols = _normalize_symbols([symbol])
    if len(normalized_symbols) != 1:
        raise ValueError('exactly one symbol is required')
    normalized_symbol = normalized_symbols[0]
    context_symbol = str(context.get('symbol') or '').strip().upper()
    if context_symbol and context_symbol != normalized_symbol:
        raise ValueError('context symbol does not match requested symbol')
    normalized_fs_div = _normalize_fingerprint_fs_div(fs_div)
    normalized_cutoff = (
        _normalize_knowledge_cutoff_at(knowledge_cutoff_at)
        if knowledge_cutoff_at is not None
        else None
    )
    normalized_disclosures_limit = int(disclosures_limit)

    visible_context = trim_batch_item_for_prompt(dict(context))

    fundamentals = (
        visible_context.get('fundamentals')
        if isinstance(visible_context.get('fundamentals'), Mapping)
        else {}
    )
    latest_snapshot = (
        fundamentals.get('latest_snapshot')
        if isinstance(fundamentals.get('latest_snapshot'), Mapping)
        else None
    )
    history_summary = (
        fundamentals.get('history_summary')
        if isinstance(fundamentals.get('history_summary'), Mapping)
        else {}
    )
    recent_disclosures = (
        visible_context.get('recent_disclosures')
        if isinstance(visible_context.get('recent_disclosures'), Mapping)
        else {}
    )
    disclosure_items = recent_disclosures.get('items')
    if not isinstance(disclosure_items, (list, tuple)):
        disclosure_items = []
    recent_dart_events = (
        visible_context.get('recent_dart_events')
        if isinstance(visible_context.get('recent_dart_events'), Mapping)
        else {}
    )
    valuation_context = (
        visible_context.get('valuation_context')
        if isinstance(visible_context.get('valuation_context'), Mapping)
        else {}
    )

    source_filters = {
        'fs_div': normalized_fs_div,
        'knowledge_cutoff_at': (
            normalized_cutoff.isoformat()
            if normalized_cutoff is not None
            else None
        ),
        'disclosures_limit': normalized_disclosures_limit,
    }
    fingerprint_input = {
        'schema_version': FUNDAMENTAL_SUMMARY_INPUT_FINGERPRINT_SCHEMA_VERSION,
        'reuse_policy_version': FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION,
        'agent_name': FUNDAMENTAL_SUMMARIZER_AGENT_NAME,
        'prompt_version': FUNDAMENTAL_SUMMARY_PROMPT_VERSION,
        'response_schema_version': FUNDAMENTAL_SUMMARY_RESPONSE_SCHEMA_VERSION,
        'symbol': normalized_symbol,
        'finalized_dispatch_context': {
            'fundamentals': {
                'latest_snapshot': _without_derived_fingerprint_fields(
                    latest_snapshot
                ),
                'history_summary': _without_derived_fingerprint_fields(
                    history_summary
                ),
            },
            'recent_disclosures': _without_derived_fingerprint_fields(
                list(disclosure_items)
            ),
            'recent_dart_events': _without_derived_fingerprint_fields(
                recent_dart_events
            ),
            'valuation_context': _without_derived_fingerprint_fields(
                valuation_context
            ),
        },
    }
    return {
        'schema_version': FUNDAMENTAL_SUMMARY_INPUT_FINGERPRINT_SCHEMA_VERSION,
        'reuse_policy_version': FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION,
        'agent_name': FUNDAMENTAL_SUMMARIZER_AGENT_NAME,
        'prompt_version': FUNDAMENTAL_SUMMARY_PROMPT_VERSION,
        'response_schema_version': FUNDAMENTAL_SUMMARY_RESPONSE_SCHEMA_VERSION,
        'requested_market_date': normalized_market_date.isoformat(),
        'source_filters': source_filters,
        'fingerprint': _stable_hash(fingerprint_input),
        'input': fingerprint_input,
    }


def _extract_fingerprint_payload(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    candidates = (
        payload.get('fundamental_summary_input_fingerprint'),
        payload.get('input_fingerprint'),
        (payload.get('reuse_metadata') or {}).get('input_fingerprint')
        if isinstance(payload.get('reuse_metadata'), dict)
        else None,
    )
    for candidate in candidates:
        if isinstance(candidate, dict):
            fingerprint = str(
                candidate.get('fingerprint')
                or candidate.get('value')
                or candidate.get('hash')
                or ''
            ).strip()
            if fingerprint:
                return candidate
    return None


def _fingerprint_value(payload: dict[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = str(payload.get('fingerprint') or payload.get('value') or payload.get('hash') or '').strip()
    return value or None


def _is_stale_dispatch_reason(reason: str | None) -> bool:
    return str(reason or '') in FUNDAMENTAL_REUSE_STALE_REASONS


def _normalized_evidence_datetime(value: Any) -> datetime:
    if not isinstance(value, datetime):
        return datetime.min.replace(tzinfo=timezone.utc)
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _latest_evidence_rows_by_key(
    rows: Sequence[Any],
) -> dict[tuple[str, str], Any]:
    latest_by_key: dict[tuple[str, str], Any] = {}
    for row in rows:
        key = (str(row.symbol), str(row.agent_name))
        current = latest_by_key.get(key)
        row_order = (
            _normalized_evidence_datetime(getattr(row, 'received_at', None)),
            int(getattr(row, 'id', 0) or 0),
        )
        if current is None:
            latest_by_key[key] = row
            continue
        current_order = (
            _normalized_evidence_datetime(
                getattr(current, 'received_at', None)
            ),
            int(getattr(current, 'id', 0) or 0),
        )
        if row_order > current_order:
            latest_by_key[key] = row
    return latest_by_key


def _normalize_knowledge_cutoff_at(value: datetime | str) -> datetime:
    parsed = value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
    if not isinstance(parsed, datetime):
        raise ValueError('knowledge_cutoff_at must be an ISO-8601 datetime')
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('knowledge_cutoff_at must include a timezone offset')
    return parsed.astimezone(timezone.utc)


def _request_payload_knowledge_cutoff_at(payload: Any) -> datetime | None:
    if not isinstance(payload, dict):
        return None
    for key in ('admission_knowledge_cutoff_at', 'knowledge_cutoff_at'):
        value = payload.get(key)
        if value is None:
            continue
        try:
            return _normalize_knowledge_cutoff_at(value)
        except (TypeError, ValueError):
            return None
    return None


def _failure_supersedes_feedback(
    *,
    feedback: Any,
    failure: Any,
    run_order_by_id: dict[str, tuple[datetime, int, datetime, int]],
) -> bool:
    if failure is None:
        return False
    if feedback is None:
        return True

    feedback_market_date = getattr(feedback, 'market_date', None)
    failure_market_date = getattr(failure, 'market_date', None)
    if isinstance(feedback_market_date, date) and isinstance(
        failure_market_date,
        date,
    ):
        if failure_market_date > feedback_market_date:
            return True
        if failure_market_date < feedback_market_date:
            return False

    feedback_run_id = str(getattr(feedback, 'run_id', None) or '').strip()
    failure_run_id = str(getattr(failure, 'run_id', None) or '').strip()
    if feedback_run_id and failure_run_id and feedback_run_id != failure_run_id:
        feedback_run_order = run_order_by_id.get(feedback_run_id)
        failure_run_order = run_order_by_id.get(failure_run_id)
        if feedback_run_order is not None and failure_run_order is not None:
            return failure_run_order >= feedback_run_order

    feedback_received_at = _normalized_evidence_datetime(
        getattr(feedback, 'received_at', None)
    )
    failure_received_at = _normalized_evidence_datetime(
        getattr(failure, 'received_at', None)
    )
    # A same-run, missing-lineage, or equal-time conflict is ambiguous and must
    # remain retryable instead of allowing a potentially invalid success.
    return failure_received_at >= feedback_received_at


class AgentContextReuseService:
    def build_fundamental_input_fingerprint_payload(
        self,
        *,
        market_date: str | date,
        symbol: str,
        fs_div: str | None = None,
        disclosures_limit: int = 5,
        knowledge_cutoff_at: datetime | str | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols([symbol])
        if len(normalized_symbols) != 1:
            raise ValueError('exactly one symbol is required')
        normalized_symbol = normalized_symbols[0]

        with session_scope() as session:
            symbol_id = session.execute(
                select(Symbol.id).where(Symbol.symbol == normalized_symbol)
            ).scalar_one_or_none()
            return self._build_fundamental_input_fingerprint_payload(
                session=session,
                market_date=normalized_market_date,
                symbol=normalized_symbol,
                symbol_id=int(symbol_id) if symbol_id is not None else None,
                fs_div=_resolve_fingerprint_db_fs_div(fs_div),
                disclosures_limit=int(disclosures_limit),
                knowledge_cutoff_at=(
                    _normalize_knowledge_cutoff_at(knowledge_cutoff_at)
                    if knowledge_cutoff_at is not None
                    else None
                ),
            )

    def build_latest_decisions(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        roles: Sequence[str],
        force_recompute: bool = False,
        knowledge_cutoff_at: datetime | str | None = None,
        fs_div: str | None = None,
        disclosures_limit: int = 5,
        fundamental_input_fingerprint_payloads: Mapping[
            str,
            Mapping[str, Any],
        ] | None = None,
        static_inputs: FeedbackReuseStaticInputs | None = None,
    ) -> dict[str, dict[str, FeedbackReuseDecision]]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        requested_roles = [role for role in roles if role in ROLE_TO_AGENT]
        normalized_knowledge_cutoff_at = (
            _normalize_knowledge_cutoff_at(knowledge_cutoff_at)
            if knowledge_cutoff_at is not None
            else None
        )
        normalized_fs_div = _resolve_fingerprint_db_fs_div(fs_div)
        normalized_disclosures_limit = int(disclosures_limit)

        if not normalized_symbols:
            raise ValueError('symbols is required')
        if not requested_roles:
            raise ValueError('roles is required')

        decisions: dict[str, dict[str, FeedbackReuseDecision]] = {symbol: {} for symbol in normalized_symbols}
        if force_recompute and SIGNAL_DECIDER_AGENT_NAME not in requested_roles:
            for symbol in normalized_symbols:
                for role in requested_roles:
                    agent_name = ROLE_TO_AGENT[role]
                    decisions[symbol][role] = FeedbackReuseDecision(
                        symbol=symbol,
                        agent_name=agent_name,
                        should_dispatch=True,
                        reason='forced_recompute',
                        latest_feedback_id=None,
                        latest_feedback_received_at=None,
                        latest_source_updated_at=None,
                        latest_feedback=None,
                        reuse_policy_version=(
                            FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION
                            if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME
                            else None
                        ),
                    )
            return decisions

        with session_scope() as session:
            symbol_rows = session.execute(
                select(Symbol.id, Symbol.symbol)
                .where(Symbol.symbol.in_(normalized_symbols))
            ).all()
            symbol_id_map = {str(row.symbol): int(row.id) for row in symbol_rows}

            if static_inputs is not None:
                self._refresh_latest_source_updated_at_cache(
                    session=session,
                    market_date=normalized_market_date,
                    symbols=normalized_symbols,
                    roles=requested_roles,
                    symbol_id_map=symbol_id_map,
                    static_inputs=static_inputs,
                )

            latest_feedback_map: dict[tuple[str, str], Any] = {}
            latest_failure_map: dict[tuple[str, str], AgentContextFeedbackFailure] = {}
            if static_inputs is not None:
                (
                    latest_feedback_map,
                    latest_failure_map,
                ) = self._load_latest_feedback_state(
                    session=session,
                    market_date=normalized_market_date,
                    symbols=normalized_symbols,
                    roles=requested_roles,
                    static_inputs=static_inputs,
                )
            else:
                for role in requested_roles:
                    agent_name = ROLE_TO_AGENT[role]
                    for chunk in _chunked(normalized_symbols, FEEDBACK_REUSE_SYMBOL_CHUNK_SIZE):
                        same_day_feedback_rows = session.execute(
                            select(
                                AgentContextFeedback.id,
                                AgentContextFeedback.market_date,
                                AgentContextFeedback.symbol,
                                AgentContextFeedback.agent_name,
                                AgentContextFeedback.run_id,
                                AgentContextFeedback.summary_payload,
                                AgentContextFeedback.feedback_payload,
                                AgentContextFeedback.context_payload,
                                AgentContextFeedback.received_at,
                            )
                            .where(AgentContextFeedback.market_date == normalized_market_date)
                            .where(AgentContextFeedback.symbol.in_(chunk))
                            .where(AgentContextFeedback.agent_name == agent_name)
                            .order_by(
                                AgentContextFeedback.received_at.asc(),
                                AgentContextFeedback.id.asc(),
                            )
                        ).all()
                        for row in same_day_feedback_rows:
                            if not hasattr(row, 'feedback_payload'):
                                continue
                            latest_feedback_map[(row.symbol, agent_name)] = row
                    for symbol in normalized_symbols:
                        if (symbol, agent_name) in latest_feedback_map:
                            continue
                        if role == SIGNAL_DECIDER_AGENT_NAME:
                            continue
                        feedback_rows = session.execute(
                            select(
                                AgentContextFeedback.id,
                                AgentContextFeedback.market_date,
                                AgentContextFeedback.symbol,
                                AgentContextFeedback.agent_name,
                                AgentContextFeedback.run_id,
                                AgentContextFeedback.summary_payload,
                                AgentContextFeedback.feedback_payload,
                                AgentContextFeedback.context_payload,
                                AgentContextFeedback.received_at,
                            )
                            .where(AgentContextFeedback.market_date <= normalized_market_date)
                            .where(AgentContextFeedback.symbol == symbol)
                            .where(AgentContextFeedback.agent_name == agent_name)
                            .order_by(
                                AgentContextFeedback.market_date.desc(),
                                AgentContextFeedback.received_at.desc(),
                                AgentContextFeedback.id.desc(),
                            )
                            .limit(1)
                        ).all()
                        for row in feedback_rows:
                            if not hasattr(row, 'feedback_payload'):
                                continue
                            key = (row.symbol, agent_name)
                            if key not in latest_feedback_map:
                                latest_feedback_map[key] = row
                    for symbol in normalized_symbols:
                        failure_row = session.execute(
                            select(AgentContextFeedbackFailure)
                            .where(AgentContextFeedbackFailure.market_date <= normalized_market_date)
                            .where(AgentContextFeedbackFailure.symbol == symbol)
                            .where(AgentContextFeedbackFailure.agent_name == agent_name)
                            .order_by(
                                AgentContextFeedbackFailure.market_date.desc(),
                                AgentContextFeedbackFailure.received_at.desc(),
                                AgentContextFeedbackFailure.id.desc(),
                            )
                            .limit(1)
                        ).scalars().first()
                        if failure_row is not None:
                            latest_failure_map[(failure_row.symbol, agent_name)] = failure_row

            decision_support_source_by_symbol = (
                self._load_decision_support_source_updated_at(
                    session=session,
                    market_date=normalized_market_date,
                    symbols=normalized_symbols,
                )
                if static_inputs is not None
                and set(requested_roles).intersection(
                    DECISION_SUPPORT_AGENT_NAMES
                )
                else {}
            )

            evidence_run_ids = {
                str(row.run_id or '').strip()
                for row in (
                    *latest_feedback_map.values(),
                    *latest_failure_map.values(),
                )
                if str(getattr(row, 'run_id', None) or '').strip()
            }
            run_order_by_id: dict[
                str,
                tuple[datetime, int, datetime, int],
            ] = {}
            run_knowledge_cutoff_by_id: dict[str, datetime | None] = {}
            if evidence_run_ids:
                run_rows = session.execute(
                    select(
                        AgentContextFeedbackRun.id,
                        AgentContextFeedbackRun.run_id,
                        AgentContextFeedbackRun.started_at,
                        AgentContextFeedbackRun.request_payload,
                    ).where(
                        AgentContextFeedbackRun.run_id.in_(evidence_run_ids)
                    )
                ).all()
                run_metadata_by_id = {
                    str(row.run_id): {
                        'direct_order': (
                            _normalized_evidence_datetime(row.started_at),
                            int(row.id),
                        ),
                        'parent_run_id': str(
                            (
                                row.request_payload.get('parent_run_id')
                                if isinstance(row.request_payload, dict)
                                else None
                            )
                            or ''
                        ).strip(),
                        'knowledge_cutoff_at': (
                            _request_payload_knowledge_cutoff_at(
                                row.request_payload
                            )
                        ),
                    }
                    for row in run_rows
                    if str(row.run_id or '').strip()
                }
                parent_run_ids = {
                    metadata['parent_run_id']
                    for metadata in run_metadata_by_id.values()
                    if metadata['parent_run_id']
                }
                missing_parent_run_ids = parent_run_ids.difference(
                    run_metadata_by_id
                )
                if missing_parent_run_ids:
                    parent_rows = session.execute(
                        select(
                            AgentContextFeedbackRun.id,
                            AgentContextFeedbackRun.run_id,
                            AgentContextFeedbackRun.started_at,
                        ).where(
                            AgentContextFeedbackRun.run_id.in_(
                                missing_parent_run_ids
                            )
                        )
                    ).all()
                    for row in parent_rows:
                        run_id = str(row.run_id or '').strip()
                        if not run_id:
                            continue
                        run_metadata_by_id[run_id] = {
                            'direct_order': (
                                _normalized_evidence_datetime(row.started_at),
                                int(row.id),
                            ),
                            'parent_run_id': '',
                            'knowledge_cutoff_at': None,
                        }
                for run_id in evidence_run_ids:
                    metadata = run_metadata_by_id.get(run_id)
                    if metadata is None:
                        continue
                    run_knowledge_cutoff_by_id[run_id] = metadata[
                        'knowledge_cutoff_at'
                    ]
                    direct_order = metadata['direct_order']
                    parent_run_id = metadata['parent_run_id']
                    if parent_run_id:
                        parent_metadata = run_metadata_by_id.get(parent_run_id)
                        if parent_metadata is None:
                            continue
                        lineage_order = parent_metadata['direct_order']
                    else:
                        lineage_order = direct_order
                    run_order_by_id[run_id] = (
                        lineage_order[0],
                        lineage_order[1],
                        direct_order[0],
                        direct_order[1],
                    )

            latest_snapshot_by_feedback_key: dict[
                tuple[int, str, str, str],
                Any,
            ] = {}
            feedback_ids = {
                int(row.id)
                for row in latest_feedback_map.values()
                if row.id is not None
            }
            if feedback_ids and set(requested_roles).intersection(
                DECISION_SUPPORT_AGENT_NAMES
            ):
                snapshot_rows = session.execute(
                    select(
                        SignalDeciderDecisionSnapshot.id,
                        SignalDeciderDecisionSnapshot.feedback_id,
                        SignalDeciderDecisionSnapshot.content_fingerprint,
                        SignalDeciderDecisionSnapshot.decision_run_id,
                        SignalDeciderDecisionSnapshot.feedback_received_at,
                        SignalDeciderDecisionSnapshot.symbol,
                        SignalDeciderDecisionSnapshot.agent_name,
                    )
                    .where(
                        SignalDeciderDecisionSnapshot.feedback_id.in_(
                            feedback_ids
                        ),
                        SignalDeciderDecisionSnapshot.agent_name.in_(
                            DECISION_SUPPORT_AGENT_NAMES
                        ),
                    )
                    .order_by(
                        SignalDeciderDecisionSnapshot.created_at.desc(),
                        SignalDeciderDecisionSnapshot.id.desc(),
                    )
                ).all()
                for snapshot_row in snapshot_rows:
                    key = (
                        int(snapshot_row.feedback_id),
                        str(snapshot_row.symbol),
                        str(snapshot_row.agent_name),
                        str(snapshot_row.decision_run_id or ''),
                    )
                    latest_snapshot_by_feedback_key.setdefault(key, snapshot_row)

            for symbol in normalized_symbols:
                symbol_id = symbol_id_map.get(symbol)
                for role in requested_roles:
                    agent_name = ROLE_TO_AGENT[role]
                    latest_feedback = latest_feedback_map.get((symbol, agent_name))
                    latest_failure = latest_failure_map.get((symbol, agent_name))

                    source_cache_key = (symbol, role)
                    if role in DECISION_SUPPORT_AGENT_NAMES:
                        latest_source_updated_at = (
                            decision_support_source_by_symbol.get(symbol)
                            if static_inputs is not None
                            else self._get_latest_source_updated_at(
                                session=session,
                                role=role,
                                market_date=normalized_market_date,
                                symbol=symbol,
                                symbol_id=symbol_id,
                            )
                        )
                    elif (
                        static_inputs is not None
                        and source_cache_key
                        in static_inputs.latest_source_updated_at_by_key
                    ):
                        latest_source_updated_at = (
                            static_inputs.latest_source_updated_at_by_key[
                                source_cache_key
                            ]
                        )
                    else:
                        latest_source_updated_at = (
                            self._get_latest_source_updated_at(
                                session=session,
                                role=role,
                                market_date=normalized_market_date,
                                symbol=symbol,
                                symbol_id=symbol_id,
                            )
                        )
                        if static_inputs is not None:
                            static_inputs.latest_source_updated_at_by_key[
                                source_cache_key
                            ] = latest_source_updated_at
                    input_fingerprint_payload = None
                    if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME:
                        if fundamental_input_fingerprint_payloads is not None:
                            supplied_payload = (
                                fundamental_input_fingerprint_payloads.get(
                                    symbol
                                )
                            )
                            if isinstance(supplied_payload, Mapping):
                                input_fingerprint_payload = dict(
                                    supplied_payload
                                )
                        elif (
                            static_inputs is not None
                            and symbol
                            in static_inputs.fundamental_input_fingerprint_payloads
                        ):
                            cached_payload = (
                                static_inputs
                                .fundamental_input_fingerprint_payloads[symbol]
                            )
                            if isinstance(cached_payload, Mapping):
                                input_fingerprint_payload = dict(
                                    cached_payload
                                )
                        else:
                            input_fingerprint_payload = (
                                self._build_fundamental_input_fingerprint_payload(
                                    session=session,
                                    market_date=normalized_market_date,
                                    symbol=symbol,
                                    symbol_id=symbol_id,
                                    fs_div=normalized_fs_div,
                                    disclosures_limit=(
                                        normalized_disclosures_limit
                                    ),
                                    knowledge_cutoff_at=(
                                        normalized_knowledge_cutoff_at
                                    ),
                                )
                            )
                            if static_inputs is not None:
                                static_inputs.fundamental_input_fingerprint_payloads[
                                    symbol
                                ] = input_fingerprint_payload
                    input_fingerprint = _fingerprint_value(input_fingerprint_payload)

                    if latest_feedback is None:
                        decisions[symbol][role] = FeedbackReuseDecision(
                            symbol=symbol,
                            agent_name=agent_name,
                            should_dispatch=True,
                            reason='latest_failure_pending_retry' if latest_failure is not None else 'missing_feedback',
                            latest_feedback_id=None,
                            latest_feedback_received_at=None,
                            latest_source_updated_at=latest_source_updated_at.isoformat() if latest_source_updated_at else None,
                            latest_feedback=None,
                            input_fingerprint=input_fingerprint,
                            input_fingerprint_payload=input_fingerprint_payload,
                            reuse_policy_version=(
                                FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION
                                if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME
                                else None
                            ),
                        )
                        continue

                    feedback_received_at = latest_feedback.received_at
                    latest_decision_snapshot = (
                        latest_snapshot_by_feedback_key.get(
                            (
                                int(latest_feedback.id),
                                symbol,
                                agent_name,
                                str(latest_feedback.run_id or ''),
                            )
                        )
                        if role in DECISION_SUPPORT_AGENT_NAMES
                        else None
                    )
                    has_newer_failure = _failure_supersedes_feedback(
                        feedback=latest_feedback,
                        failure=latest_failure,
                        run_order_by_id=run_order_by_id,
                    )
                    latest_feedback_payload = latest_feedback.feedback_payload if isinstance(latest_feedback.feedback_payload, dict) else {}
                    latest_risk_flags = latest_feedback_payload.get('risk_flags') if isinstance(latest_feedback_payload.get('risk_flags'), list) else []
                    has_retryable_feedback_gap = any(
                        isinstance(flag, str) and flag in {'missing_batch_feedback', 'unstructured_response'}
                        for flag in latest_risk_flags
                    ) or has_newer_failure
                    has_knowledge_cutoff_mismatch = (
                        normalized_knowledge_cutoff_at is not None
                        and run_knowledge_cutoff_by_id.get(
                            str(latest_feedback.run_id or '').strip()
                        )
                        != normalized_knowledge_cutoff_at
                    )
                    feedback_market_date = latest_feedback.market_date if latest_feedback is not None else None
                    is_stale_market_feedback = feedback_market_date is None or feedback_market_date != normalized_market_date
                    stored_input_fingerprint_payload = (
                        _extract_fingerprint_payload(latest_feedback.context_payload)
                        or _extract_fingerprint_payload(latest_feedback.summary_payload)
                    )
                    stored_input_fingerprint = _fingerprint_value(stored_input_fingerprint_payload)
                    has_materialized_fundamental_match = bool(
                        role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME
                        and input_fingerprint
                        and input_fingerprint == stored_input_fingerprint
                        and isinstance(input_fingerprint_payload, Mapping)
                        and isinstance(
                            stored_input_fingerprint_payload,
                            Mapping,
                        )
                        and input_fingerprint_payload.get('schema_version')
                        == FUNDAMENTAL_SUMMARY_INPUT_FINGERPRINT_SCHEMA_VERSION
                        and stored_input_fingerprint_payload.get(
                            'schema_version'
                        )
                        == FUNDAMENTAL_SUMMARY_INPUT_FINGERPRINT_SCHEMA_VERSION
                        and input_fingerprint_payload.get(
                            'reuse_policy_version'
                        )
                        == FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION
                        and stored_input_fingerprint_payload.get(
                            'reuse_policy_version'
                        )
                        == FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION
                    )
                    has_prior_date_materialized_fundamental_match = bool(
                        has_materialized_fundamental_match
                        and isinstance(feedback_market_date, date)
                        and feedback_market_date < normalized_market_date
                    )
                    if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME:
                        should_dispatch, reason = self._decide_fundamental_reuse(
                            force_recompute=force_recompute,
                            has_retryable_feedback_gap=has_retryable_feedback_gap,
                            input_fingerprint=input_fingerprint,
                            stored_input_fingerprint=stored_input_fingerprint,
                            latest_source_updated_at=latest_source_updated_at,
                            feedback_received_at=feedback_received_at,
                            materialized_fingerprint_match=(
                                has_materialized_fundamental_match
                            ),
                        )
                        if has_newer_failure:
                            reason = 'latest_failure_pending_retry'
                    else:
                        should_dispatch = force_recompute or is_stale_market_feedback or has_retryable_feedback_gap or (
                            latest_source_updated_at is not None and feedback_received_at is not None and latest_source_updated_at > feedback_received_at
                        )
                        reason = (
                            'forced_recompute' if force_recompute else (
                                'latest_failure_pending_retry' if has_newer_failure else (
                                    'stale_market_feedback' if is_stale_market_feedback else (
                                        'retryable_feedback_gap' if has_retryable_feedback_gap else ('source_updated' if should_dispatch else 'reuse_latest_feedback')
                                    )
                                )
                            )
                        )
                    if (
                        has_knowledge_cutoff_mismatch
                        and not force_recompute
                        and not has_newer_failure
                        and not has_prior_date_materialized_fundamental_match
                    ):
                        should_dispatch = True
                        reason = 'knowledge_cutoff_mismatch'
                    decisions[symbol][role] = FeedbackReuseDecision(
                        symbol=symbol,
                        agent_name=agent_name,
                        should_dispatch=should_dispatch,
                        reason=reason,
                        latest_feedback_id=latest_feedback.id,
                        latest_feedback_received_at=feedback_received_at.isoformat() if feedback_received_at else None,
                        latest_source_updated_at=latest_source_updated_at.isoformat() if latest_source_updated_at else None,
                        latest_feedback={
                            'id': latest_feedback.id,
                            'market_date': latest_feedback.market_date.isoformat(),
                            'symbol': latest_feedback.symbol,
                            'agent_name': latest_feedback.agent_name,
                            'run_id': latest_feedback.run_id,
                            'summary_payload': latest_feedback.summary_payload,
                            'feedback_payload': latest_feedback.feedback_payload,
                            'context_payload': latest_feedback.context_payload,
                            'received_at': feedback_received_at.isoformat() if feedback_received_at else None,
                            'input_fingerprint': stored_input_fingerprint,
                            'input_fingerprint_payload': stored_input_fingerprint_payload,
                        },
                        latest_decision_snapshot_id=(
                            int(latest_decision_snapshot.id)
                            if latest_decision_snapshot is not None
                            else None
                        ),
                        latest_decision_snapshot_fingerprint=(
                            str(latest_decision_snapshot.content_fingerprint)
                            if latest_decision_snapshot is not None
                            and latest_decision_snapshot.content_fingerprint
                            else None
                        ),
                        latest_decision_snapshot_observed_at=(
                            latest_decision_snapshot.feedback_received_at.isoformat()
                            if latest_decision_snapshot is not None
                            and latest_decision_snapshot.feedback_received_at
                            else None
                        ),
                        input_fingerprint=input_fingerprint,
                        stored_input_fingerprint=stored_input_fingerprint,
                        input_fingerprint_payload=input_fingerprint_payload,
                        stored_input_fingerprint_payload=stored_input_fingerprint_payload,
                        reuse_policy_version=(
                            FUNDAMENTAL_SUMMARY_REUSE_POLICY_VERSION
                            if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME
                            else None
                        ),
                        reuse_source=(
                            'persisted_feedback_fingerprint'
                            if role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME and not should_dispatch
                            else None
                        ),
                    )
        return decisions

    def _load_latest_feedback_state(
        self,
        *,
        session,
        market_date: date,
        symbols: Sequence[str],
        roles: Sequence[str],
        static_inputs: FeedbackReuseStaticInputs,
    ) -> tuple[
        dict[tuple[str, str], Any],
        dict[tuple[str, str], Any],
    ]:
        """Refresh same-day receipts and reuse immutable prior-date lookups."""

        agent_names = tuple(
            dict.fromkeys(ROLE_TO_AGENT[role] for role in roles)
        )
        requested_keys = tuple(
            (symbol, agent_name)
            for symbol in symbols
            for agent_name in agent_names
        )
        same_day_feedback_map: dict[tuple[str, str], Any] = {}
        same_day_failure_map: dict[tuple[str, str], Any] = {}
        for chunk in _chunked(symbols, FEEDBACK_REUSE_SYMBOL_CHUNK_SIZE):
            feedback_rows = session.execute(
                select(
                    AgentContextFeedback.id,
                    AgentContextFeedback.market_date,
                    AgentContextFeedback.symbol,
                    AgentContextFeedback.agent_name,
                    AgentContextFeedback.run_id,
                    AgentContextFeedback.summary_payload,
                    AgentContextFeedback.feedback_payload,
                    AgentContextFeedback.context_payload,
                    AgentContextFeedback.received_at,
                )
                .where(
                    AgentContextFeedback.market_date == market_date,
                    AgentContextFeedback.symbol.in_(chunk),
                    AgentContextFeedback.agent_name.in_(agent_names),
                )
                .order_by(
                    AgentContextFeedback.received_at.asc(),
                    AgentContextFeedback.id.asc(),
                )
            ).all()
            same_day_feedback_map.update(
                _latest_evidence_rows_by_key(feedback_rows)
            )

            failure_rows = session.execute(
                select(
                    AgentContextFeedbackFailure.id,
                    AgentContextFeedbackFailure.market_date,
                    AgentContextFeedbackFailure.symbol,
                    AgentContextFeedbackFailure.agent_name,
                    AgentContextFeedbackFailure.run_id,
                    AgentContextFeedbackFailure.failure_kind,
                    AgentContextFeedbackFailure.retryable,
                    AgentContextFeedbackFailure.summary_payload,
                    AgentContextFeedbackFailure.context_payload,
                    AgentContextFeedbackFailure.received_at,
                )
                .where(
                    AgentContextFeedbackFailure.market_date == market_date,
                    AgentContextFeedbackFailure.symbol.in_(chunk),
                    AgentContextFeedbackFailure.agent_name.in_(agent_names),
                )
                .order_by(
                    AgentContextFeedbackFailure.received_at.asc(),
                    AgentContextFeedbackFailure.id.asc(),
                )
            ).all()
            same_day_failure_map.update(
                _latest_evidence_rows_by_key(failure_rows)
            )

        def load_prior_rows(
            *,
            model,
            keys: Sequence[tuple[str, str]],
            selected_columns: Sequence[Any],
        ) -> list[Any]:
            loaded_rows: list[Any] = []
            for index in range(
                0,
                len(keys),
                FEEDBACK_REUSE_KEY_CHUNK_SIZE,
            ):
                key_chunk = keys[
                    index:index + FEEDBACK_REUSE_KEY_CHUNK_SIZE
                ]
                requested = values(
                    column('requested_symbol', String(32)),
                    column('requested_agent_name', String(64)),
                    name='requested_feedback_keys',
                ).data(key_chunk).alias('requested_feedback_keys')
                latest = lateral(
                    select(model.id.label('row_id'))
                    .select_from(model)
                    .where(
                        model.symbol == requested.c.requested_symbol,
                        model.agent_name
                        == requested.c.requested_agent_name,
                        model.market_date < market_date,
                    )
                    .order_by(
                        model.market_date.desc(),
                        model.received_at.desc(),
                        model.id.desc(),
                    )
                    .limit(1)
                    .correlate(requested)
                ).alias('latest_feedback_row')
                loaded_rows.extend(
                    session.execute(
                        select(*selected_columns)
                        .select_from(requested.join(latest, true()))
                        .join(model, model.id == latest.c.row_id)
                    ).all()
                )
            return loaded_rows

        missing_prior_feedback_keys = tuple(
            key
            for key in requested_keys
            if key[1] != SIGNAL_DECIDER_AGENT_NAME
            and key not in static_inputs.prior_feedback_by_key
        )
        prior_feedback_rows = load_prior_rows(
            model=AgentContextFeedback,
            keys=missing_prior_feedback_keys,
            selected_columns=(
                AgentContextFeedback.id,
                AgentContextFeedback.market_date,
                AgentContextFeedback.symbol,
                AgentContextFeedback.agent_name,
                AgentContextFeedback.run_id,
                AgentContextFeedback.summary_payload,
                AgentContextFeedback.feedback_payload,
                AgentContextFeedback.context_payload,
                AgentContextFeedback.received_at,
            ),
        )
        for key in missing_prior_feedback_keys:
            static_inputs.prior_feedback_by_key[key] = None
        for row in prior_feedback_rows:
            static_inputs.prior_feedback_by_key[
                (row.symbol, row.agent_name)
            ] = row

        missing_prior_failure_keys = tuple(
            key
            for key in requested_keys
            if key not in static_inputs.prior_failure_by_key
        )
        prior_failure_rows = load_prior_rows(
            model=AgentContextFeedbackFailure,
            keys=missing_prior_failure_keys,
            selected_columns=(
                AgentContextFeedbackFailure.id,
                AgentContextFeedbackFailure.market_date,
                AgentContextFeedbackFailure.symbol,
                AgentContextFeedbackFailure.agent_name,
                AgentContextFeedbackFailure.run_id,
                AgentContextFeedbackFailure.failure_kind,
                AgentContextFeedbackFailure.retryable,
                AgentContextFeedbackFailure.summary_payload,
                AgentContextFeedbackFailure.context_payload,
                AgentContextFeedbackFailure.received_at,
            ),
        )
        for key in missing_prior_failure_keys:
            static_inputs.prior_failure_by_key[key] = None
        for row in prior_failure_rows:
            static_inputs.prior_failure_by_key[
                (row.symbol, row.agent_name)
            ] = row

        latest_feedback_map = {
            key: row
            for key, row in static_inputs.prior_feedback_by_key.items()
            if key in requested_keys and row is not None
        }
        latest_feedback_map.update(same_day_feedback_map)
        latest_failure_map = {
            key: row
            for key, row in static_inputs.prior_failure_by_key.items()
            if key in requested_keys and row is not None
        }
        latest_failure_map.update(same_day_failure_map)

        return latest_feedback_map, latest_failure_map

    @staticmethod
    def _load_decision_support_source_updated_at(
        *,
        session,
        market_date: date,
        symbols: Sequence[str],
    ) -> dict[str, datetime]:
        """Refresh the upstream receipt clock used by decider reuse checks."""

        return {
            str(row.symbol): row.latest_at
            for row in session.execute(
                select(
                    AgentContextFeedback.symbol.label('symbol'),
                    func.max(AgentContextFeedback.received_at).label(
                        'latest_at'
                    ),
                )
                .where(
                    AgentContextFeedback.symbol.in_(symbols),
                    AgentContextFeedback.market_date == market_date,
                    AgentContextFeedback.agent_name.in_(
                        (
                            'eod-summarizer',
                            'technical-summarizer',
                            FUNDAMENTAL_SUMMARIZER_AGENT_NAME,
                        )
                    ),
                )
                .group_by(AgentContextFeedback.symbol)
            ).all()
            if row.latest_at is not None
        }

    def _refresh_latest_source_updated_at_cache(
        self,
        *,
        session,
        market_date: date,
        symbols: Sequence[str],
        roles: Sequence[str],
        symbol_id_map: Mapping[str, int],
        static_inputs: FeedbackReuseStaticInputs,
    ) -> None:
        """Refresh source clocks and invalidate only affected fingerprints."""

        (
            current_clocks,
            current_fundamental_versions,
        ) = self._load_latest_source_state(
            session=session,
            market_date=market_date,
            symbols=symbols,
            roles=roles,
            symbol_id_map=symbol_id_map,
        )
        missing = object()
        for key, current_value in current_clocks.items():
            static_inputs.latest_source_updated_at_by_key[key] = (
                current_value
            )
        for symbol, current_version in (
            current_fundamental_versions.items()
        ):
            previous_version = (
                static_inputs.fundamental_input_versions_by_symbol.get(
                    symbol,
                    missing,
                )
            )
            static_inputs.fundamental_input_versions_by_symbol[symbol] = (
                current_version
            )
            if (
                previous_version is not missing
                and previous_version != current_version
            ):
                static_inputs.fundamental_input_fingerprint_payloads.pop(
                    symbol,
                    None,
                )

    def _load_latest_source_state(
        self,
        *,
        session,
        market_date: date,
        symbols: Sequence[str],
        roles: Sequence[str],
        symbol_id_map: Mapping[str, int],
    ) -> tuple[
        dict[tuple[str, str], datetime | None],
        dict[str, tuple[Any, ...]],
    ]:
        """Bulk-load current source clocks without per-symbol queries."""

        requested_keys = {
            (symbol, role)
            for symbol in symbols
            for role in roles
            if role not in DECISION_SUPPORT_AGENT_NAMES
        }
        if not requested_keys:
            return {}, {}

        requested_roles = {role for _, role in requested_keys}
        symbol_ids = tuple(
            dict.fromkeys(
                symbol_id_map[symbol]
                for symbol in symbols
                if symbol in symbol_id_map
            )
        )
        symbol_by_id = {
            symbol_id: symbol
            for symbol, symbol_id in symbol_id_map.items()
        }

        def load_symbol_id_max(
            *,
            symbol_id_column,
            timestamp_column,
            market_date_column,
        ) -> dict[str, datetime]:
            if not symbol_ids:
                return {}
            rows = session.execute(
                select(
                    symbol_id_column.label('symbol_id'),
                    func.max(timestamp_column).label('latest_at'),
                )
                .where(
                    symbol_id_column.in_(symbol_ids),
                    market_date_column == market_date,
                )
                .group_by(symbol_id_column)
            ).all()
            return {
                symbol_by_id[int(row.symbol_id)]: row.latest_at
                for row in rows
                if row.latest_at is not None
                and int(row.symbol_id) in symbol_by_id
            }

        technical_by_symbol: dict[str, datetime] = {}
        action_by_symbol: dict[str, datetime] = {}
        price_by_symbol: dict[str, datetime] = {}
        if requested_roles.intersection(
            {'technical-summarizer', 'eod-summarizer'}
        ):
            technical_by_symbol = load_symbol_id_max(
                symbol_id_column=DailyTechnicalSnapshot.symbol_id,
                timestamp_column=func.greatest(
                    DailyTechnicalSnapshot.captured_at,
                    DailyTechnicalSnapshot.updated_at,
                ),
                market_date_column=DailyTechnicalSnapshot.market_date,
            )
            action_by_symbol = load_symbol_id_max(
                symbol_id_column=DailyActionSummary.symbol_id,
                timestamp_column=func.greatest(
                    DailyActionSummary.captured_at,
                    DailyActionSummary.updated_at,
                ),
                market_date_column=DailyActionSummary.market_date,
            )
        if 'eod-summarizer' in requested_roles:
            price_by_symbol = load_symbol_id_max(
                symbol_id_column=DailyPriceBar.symbol_id,
                timestamp_column=DailyPriceBar.captured_at,
                market_date_column=DailyPriceBar.trade_date,
            )

        disclosure_state_by_symbol: dict[
            str,
            tuple[datetime | None, int],
        ] = {}
        fundamentals_state_by_symbol: dict[
            str,
            tuple[datetime | None, int],
        ] = {}
        valuation_state_by_symbol: dict[
            str,
            tuple[datetime | None, int],
        ] = {}
        if FUNDAMENTAL_SUMMARIZER_AGENT_NAME in requested_roles:
            disclosure_state_by_symbol = {
                str(row.symbol): (
                    row.latest_at,
                    int(row.row_count),
                )
                for row in session.execute(
                    select(
                        DartDisclosureEvent.stock_code.label('symbol'),
                        func.max(DartDisclosureEvent.updated_at).label(
                            'latest_at'
                        ),
                        func.count(DartDisclosureEvent.id).label(
                            'row_count'
                        ),
                    )
                    .where(
                        DartDisclosureEvent.stock_code.in_(symbols),
                        DartDisclosureEvent.receipt_date <= market_date,
                    )
                    .group_by(DartDisclosureEvent.stock_code)
                ).all()
            }
            fundamentals_state_by_symbol = {
                str(row.symbol): (
                    row.latest_at,
                    int(row.row_count),
                )
                for row in session.execute(
                    # replace_snapshot_dataset updates the parent snapshot in
                    # the same transaction before replacing rows/highlights,
                    # so the parent clock covers all fingerprint child data.
                    select(
                        DartFundamentalsSnapshot.stock_code.label('symbol'),
                        func.max(
                            func.greatest(
                                DartFundamentalsSnapshot.captured_at,
                                DartFundamentalsSnapshot.updated_at,
                            )
                        ).label('latest_at'),
                        func.count(DartFundamentalsSnapshot.id).label(
                            'row_count'
                        ),
                    )
                    .where(
                        DartFundamentalsSnapshot.stock_code.in_(symbols),
                    )
                    .group_by(DartFundamentalsSnapshot.stock_code)
                ).all()
            }
            if symbol_ids:
                valuation_state_by_symbol = {
                    symbol_by_id[int(row.symbol_id)]: (
                        row.latest_at,
                        int(row.row_count),
                    )
                    for row in session.execute(
                        select(
                            DailyPykrxEnrichmentRow.symbol_id.label(
                                'symbol_id'
                            ),
                            func.max(
                                func.greatest(
                                    DailyPykrxEnrichmentRow.captured_at,
                                    DailyPykrxEnrichmentRow.updated_at,
                                )
                            ).label('latest_at'),
                            func.count(
                                DailyPykrxEnrichmentRow.id
                            ).label('row_count'),
                        )
                        .where(
                            DailyPykrxEnrichmentRow.symbol_id.in_(
                                symbol_ids
                            ),
                            DailyPykrxEnrichmentRow.market_date
                            == market_date,
                            DailyPykrxEnrichmentRow.category.in_(
                                (
                                    'valuation_snapshot',
                                    'foreign_ownership',
                                )
                            ),
                        )
                        .group_by(DailyPykrxEnrichmentRow.symbol_id)
                    ).all()
                    if int(row.symbol_id) in symbol_by_id
                }

        def latest(*values: datetime | None) -> datetime | None:
            available = [value for value in values if value is not None]
            return max(available) if available else None

        clocks: dict[tuple[str, str], datetime | None] = {}
        for symbol, role in requested_keys:
            if role == 'technical-summarizer':
                value = latest(
                    technical_by_symbol.get(symbol),
                    action_by_symbol.get(symbol),
                )
            elif role == 'eod-summarizer':
                value = latest(
                    price_by_symbol.get(symbol),
                    action_by_symbol.get(symbol),
                    technical_by_symbol.get(symbol),
                )
            elif role == FUNDAMENTAL_SUMMARIZER_AGENT_NAME:
                value = latest(
                    disclosure_state_by_symbol.get(
                        symbol,
                        (None, 0),
                    )[0],
                    fundamentals_state_by_symbol.get(
                        symbol,
                        (None, 0),
                    )[0],
                    valuation_state_by_symbol.get(
                        symbol,
                        (None, 0),
                    )[0],
                )
            else:
                value = None
            clocks[(symbol, role)] = value
        fundamental_versions = (
            {
                symbol: (
                    disclosure_state_by_symbol.get(symbol, (None, 0)),
                    fundamentals_state_by_symbol.get(symbol, (None, 0)),
                    valuation_state_by_symbol.get(symbol, (None, 0)),
                )
                for symbol in symbols
            }
            if FUNDAMENTAL_SUMMARIZER_AGENT_NAME in requested_roles
            else {}
        )
        return clocks, fundamental_versions

    @staticmethod
    def _decide_fundamental_reuse(
        *,
        force_recompute: bool,
        has_retryable_feedback_gap: bool,
        input_fingerprint: str | None,
        stored_input_fingerprint: str | None,
        latest_source_updated_at: datetime | None = None,
        feedback_received_at: datetime | None = None,
        materialized_fingerprint_match: bool = False,
    ) -> tuple[bool, str]:
        if force_recompute:
            return True, 'forced_recompute'
        if has_retryable_feedback_gap:
            return True, 'retryable_feedback_gap'
        if not input_fingerprint:
            return True, 'fundamental_input_fingerprint_missing'
        if not stored_input_fingerprint:
            return True, 'stored_fundamental_input_fingerprint_missing'
        if input_fingerprint != stored_input_fingerprint:
            return True, 'fundamental_input_fingerprint_changed'
        if (
            not materialized_fingerprint_match
            and latest_source_updated_at is not None
            and feedback_received_at is not None
            and latest_source_updated_at > feedback_received_at
        ):
            return True, 'source_updated'
        return False, FUNDAMENTAL_SUMMARY_REUSE_REASON

    def build_latest_feedback_bundle(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        roles: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        requested_roles = list(roles or DEFAULT_AGENT_NAMES)
        decisions = self.build_latest_decisions(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            roles=requested_roles,
        )
        items: list[dict[str, Any]] = []
        for symbol in normalized_symbols:
            role_items = []
            for role in requested_roles:
                decision = decisions.get(symbol, {}).get(role)
                if decision is None:
                    continue
                role_items.append(
                    {
                        'role': role,
                        'agent_name': decision.agent_name,
                        'should_dispatch': decision.should_dispatch,
                        'reason': decision.reason,
                        'latest_feedback_id': decision.latest_feedback_id,
                        'latest_feedback_received_at': decision.latest_feedback_received_at,
                        'latest_source_updated_at': decision.latest_source_updated_at,
                        'input_fingerprint': decision.input_fingerprint,
                        'stored_input_fingerprint': decision.stored_input_fingerprint,
                        'reuse_policy_version': decision.reuse_policy_version,
                        'reuse_source': decision.reuse_source,
                        'stale': _is_stale_dispatch_reason(decision.reason),
                        'latest_feedback': decision.latest_feedback,
                    }
                )
            items.append({'symbol': symbol, 'roles': role_items})
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'roles': requested_roles,
            },
            'items': items,
        }

    def _get_latest_source_updated_at(
        self,
        *,
        session,
        role: str,
        market_date: date,
        symbol: str,
        symbol_id: int | None,
    ) -> datetime | None:
        timestamps: list[datetime | None] = []
        if role == 'technical-summarizer':
            if symbol_id is not None:
                timestamps.append(
                    session.execute(
                        select(func.max(DailyTechnicalSnapshot.captured_at))
                        .where(DailyTechnicalSnapshot.symbol_id == symbol_id)
                        .where(DailyTechnicalSnapshot.market_date == market_date)
                    ).scalar_one()
                )
                timestamps.append(
                    session.execute(
                        select(func.max(DailyActionSummary.captured_at))
                        .where(DailyActionSummary.symbol_id == symbol_id)
                        .where(DailyActionSummary.market_date == market_date)
                    ).scalar_one()
                )
        elif role == 'fundamental-summarizer':
            timestamps.append(
                session.execute(
                    select(func.max(DartDisclosureEvent.updated_at))
                    .where(DartDisclosureEvent.stock_code == symbol)
                    .where(or_(DartDisclosureEvent.receipt_date == market_date, DartDisclosureEvent.receipt_date <= market_date))
                ).scalar_one()
            )
            timestamps.append(
                session.execute(
                    select(func.max(DartFundamentalsSnapshot.captured_at))
                    .join(DartDisclosureEvent, DartDisclosureEvent.id == DartFundamentalsSnapshot.disclosure_event_id)
                    .where(DartFundamentalsSnapshot.stock_code == symbol)
                    .where(DartDisclosureEvent.receipt_date <= market_date)
                ).scalar_one()
            )
        elif role == 'eod-summarizer':
            if symbol_id is not None:
                timestamps.append(
                    session.execute(
                        select(func.max(DailyPriceBar.captured_at))
                        .where(DailyPriceBar.symbol_id == symbol_id)
                        .where(DailyPriceBar.trade_date == market_date)
                    ).scalar_one()
                )
                timestamps.append(
                    session.execute(
                        select(func.max(DailyActionSummary.captured_at))
                        .where(DailyActionSummary.symbol_id == symbol_id)
                        .where(DailyActionSummary.market_date == market_date)
                    ).scalar_one()
                )
                timestamps.append(
                    session.execute(
                        select(func.max(DailyTechnicalSnapshot.captured_at))
                        .where(DailyTechnicalSnapshot.symbol_id == symbol_id)
                        .where(DailyTechnicalSnapshot.market_date == market_date)
                    ).scalar_one()
                )
        elif role in DECISION_SUPPORT_AGENT_NAMES:
            timestamps.append(
                session.execute(
                    select(func.max(AgentContextFeedback.received_at))
                    .where(AgentContextFeedback.symbol == symbol)
                    .where(AgentContextFeedback.market_date == market_date)
                    .where(
                        AgentContextFeedback.agent_name.in_(
                            ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer']
                        )
                    )
                ).scalar_one()
            )
        valid = [item for item in timestamps if item is not None]
        return max(valid) if valid else None

    def _build_fundamental_input_fingerprint_payload(
        self,
        *,
        session,
        market_date: date,
        symbol: str,
        symbol_id: int | None,
        fs_div: str | None,
        disclosures_limit: int,
        knowledge_cutoff_at: datetime | None,
    ) -> dict[str, Any]:
        from app.agent_context.read_service import (
            _build_fundamentals_summary,
            _build_recent_dart_events_context,
            _build_valuation_context,
            _serialize_disclosure,
        )
        from app.dart.fundamentals_store import (
            dart_fundamentals_repository,
        )
        from app.dart.store import _serialize_event

        snapshot_statement = (
            select(DartFundamentalsSnapshot, DartDisclosureEvent)
            .join(
                DartDisclosureEvent,
                DartDisclosureEvent.id
                == DartFundamentalsSnapshot.disclosure_event_id,
            )
            .where(DartFundamentalsSnapshot.stock_code == symbol)
        )
        if fs_div is not None:
            snapshot_statement = snapshot_statement.where(
                DartFundamentalsSnapshot.fs_div == fs_div
            )
        if knowledge_cutoff_at is not None:
            snapshot_statement = snapshot_statement.where(
                DartFundamentalsSnapshot.captured_at
                < knowledge_cutoff_at
            )
        snapshot_pairs = session.execute(
            snapshot_statement.order_by(
                DartFundamentalsSnapshot.stock_code.asc(),
                DartDisclosureEvent.receipt_date.desc(),
                DartFundamentalsSnapshot.business_year.desc(),
                DartFundamentalsSnapshot.report_code.desc(),
                DartFundamentalsSnapshot.id.desc(),
            )
        ).all()
        snapshot_ids = [int(snapshot.id) for snapshot, _ in snapshot_pairs]
        highlights_by_snapshot: dict[int, list[dict[str, Any]]] = (
            defaultdict(list)
        )
        row_count_by_snapshot: dict[int, int] = {}
        if snapshot_ids:
            highlight_rows = session.execute(
                select(DartFundamentalsHighlight)
                .where(
                    DartFundamentalsHighlight.snapshot_id.in_(snapshot_ids)
                )
                .order_by(
                    DartFundamentalsHighlight.snapshot_id.asc(),
                    DartFundamentalsHighlight.metric_key.asc(),
                )
            ).scalars().all()
            for row in highlight_rows:
                highlights_by_snapshot[int(row.snapshot_id)].append(
                    dart_fundamentals_repository._serialize_highlight(row)
                )
            row_count_by_snapshot = {
                int(snapshot_id): int(row_count)
                for snapshot_id, row_count in session.execute(
                    select(
                        DartFundamentalsRow.snapshot_id,
                        func.count(),
                    )
                    .where(
                        DartFundamentalsRow.snapshot_id.in_(snapshot_ids)
                    )
                    .group_by(DartFundamentalsRow.snapshot_id)
                ).all()
            }
        fundamentals_items = [
            {
                **dart_fundamentals_repository._serialize_snapshot(snapshot),
                'disclosure': (
                    dart_fundamentals_repository._serialize_event(event)
                ),
                'highlights': highlights_by_snapshot.get(
                    int(snapshot.id),
                    [],
                ),
                'statement_rows': [],
                'statement_row_count': row_count_by_snapshot.get(
                    int(snapshot.id),
                    0,
                ),
            }
            for snapshot, event in snapshot_pairs
        ]
        fundamentals = _build_fundamentals_summary(
            fundamentals_items,
            market_date=market_date,
        )

        recent_disclosure_statement = (
            select(DartDisclosureEvent)
            .where(DartDisclosureEvent.stock_code == symbol)
            .where(DartDisclosureEvent.receipt_date <= market_date)
        )
        if knowledge_cutoff_at is not None:
            recent_disclosure_statement = (
                recent_disclosure_statement.where(
                    DartDisclosureEvent.created_at < knowledge_cutoff_at
                )
            )
        recent_disclosures = (
            session.execute(
                recent_disclosure_statement.order_by(
                    DartDisclosureEvent.receipt_date.desc(),
                    DartDisclosureEvent.rcept_no.desc(),
                ).limit(disclosures_limit)
            ).scalars().all()
            if disclosures_limit > 0
            else []
        )
        disclosure_payloads = [
            _serialize_disclosure(_serialize_event(row))
            for row in recent_disclosures
        ]
        recent_dart_events = _build_recent_dart_events_context(
            disclosures=disclosure_payloads,
            fundamentals=fundamentals,
            storage_unavailable=False,
        )

        valuation_rows_by_category: dict[str, dict[str, Any]] = {}
        if symbol_id is not None:
            valuation_rows = session.execute(
                select(DailyPykrxEnrichmentRow)
                .where(DailyPykrxEnrichmentRow.symbol_id == symbol_id)
                .where(DailyPykrxEnrichmentRow.market_date == market_date)
                .where(DailyPykrxEnrichmentRow.category.in_(('valuation_snapshot', 'foreign_ownership')))
                .order_by(DailyPykrxEnrichmentRow.category.asc(), DailyPykrxEnrichmentRow.source.asc())
            ).scalars().all()
            valuation_rows_by_category = {
                str(row.category): {
                    'market': row.market,
                    'payload': (
                        row.payload if isinstance(row.payload, dict) else {}
                    ),
                }
                for row in valuation_rows
            }
        valuation_context = _build_valuation_context(
            valuation_row=valuation_rows_by_category.get(
                'valuation_snapshot'
            ),
            foreign_ownership_row=valuation_rows_by_category.get(
                'foreign_ownership'
            ),
        )

        return build_fundamental_input_fingerprint_payload_from_context(
            market_date=market_date,
            symbol=symbol,
            context={
                'symbol': symbol,
                'fundamentals': fundamentals,
                'recent_disclosures': {'items': disclosure_payloads},
                'recent_dart_events': recent_dart_events,
                'valuation_context': valuation_context,
            },
            fs_div=fs_div,
            disclosures_limit=disclosures_limit,
            knowledge_cutoff_at=knowledge_cutoff_at,
        )


agent_context_reuse_service = AgentContextReuseService()
