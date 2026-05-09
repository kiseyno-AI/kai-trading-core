from datetime import date, datetime, timezone
from pathlib import Path
import os
import signal
import subprocess
from typing import Any

import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.agent_context.decider_evaluation_service import decider_post_decision_evaluation_service
from app.agent_context.eod_summaries_service import agent_context_eod_summaries_service
from app.agent_context.eligibility import (
    AGENT_FEEDBACK_ELIGIBILITY_FILTERS,
    AGENT_FEEDBACK_ELIGIBILITY_RULE,
    resolve_agent_feedback_scope,
)
from app.agent_context.feedback_service import agent_context_feedback_service
from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
from app.agent_context.missing_data_report_service import agent_context_missing_data_report_service
from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.agent_context.progress_service import agent_context_progress_service
from app.agent_context.reuse_service import agent_context_reuse_service
from app.agent_context.read_service import agent_context_read_service
from app.agent_context.run_state_service import agent_context_feedback_run_state_service
from app.agent_context.session_cleanup_service import agent_feedback_session_cleanup_service
from app.agent_context.signal_decider_calibration_service import signal_decider_calibration_service
from app.agent_context.signal_decider_calendar_review_service import signal_decider_calendar_review_service
from app.agent_context.signal_decider_loop_service import signal_decider_loop_service
from app.agent_context.retry_service import (
    EXPECTED_ROLES,
    FeedbackRetryCycleConfig,
    agent_feedback_retry_service,
)
from app.agent_context.status_service import agent_context_status_service
from app.agent_context.runtime_state import RuntimeFeedbackRunState, runtime_feedback_state_store
from app.orders.signal_decider_execution_outcomes import signal_decider_execution_outcome_service

import threading
import uuid

router = APIRouter(prefix='/agent-context', tags=['agent-context'])
logger = logging.getLogger(__name__)


class AgentFeedbackRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date: date
    symbol: str = Field(..., min_length=1, max_length=32)
    agent_name: str = Field(default='openclaw', min_length=1, max_length=64)
    agent_version: str | None = Field(default=None, max_length=64)
    run_id: str | None = Field(default=None, max_length=128)
    summary: dict[str, Any] = Field(default_factory=dict)
    feedback: dict[str, Any] = Field(default_factory=dict)
    context: dict[str, Any] | None = None

    @model_validator(mode='after')
    def require_feedback_payload(self) -> 'AgentFeedbackRequest':
        if not self.summary and not self.feedback:
            raise ValueError('Either summary or feedback must be provided')
        return self


class AgentFeedbackDispatchRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    eod_run_id: int = Field(..., ge=1)
    roles: list[str] | None = None


class AgentSymbolFeedbackDispatchRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date: date
    symbols: list[str] = Field(..., min_length=1, max_length=500)
    roles: list[str] | None = None
    fs_div: str | None = None
    disclosures_limit: int = Field(default=5, ge=0, le=20)
    reuse_existing: bool = True
    wait_for_completion: bool = True
    force: bool = False


class AgentFeedbackRetryCyclesRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date: date
    symbols: list[str] | None = Field(default=None, min_length=1, max_length=500)
    roles: list[str] | None = None
    max_cycles: int = Field(default=3, ge=1, le=20)
    outer_batch_size: int = Field(default=20, ge=1, le=200)
    reuse_existing: bool = True
    force: bool = False


class AgentFeedbackFollowupRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date: date
    symbols: list[str] | None = Field(default=None, min_length=1, max_length=500)
    roles: list[str] | None = None
    max_cycles: int = Field(default=3, ge=1, le=20)
    outer_batch_size: int = Field(default=20, ge=1, le=500)
    reuse_existing: bool = True
    force: bool = False


class AgentFeedbackCleanupRunRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    limit: int = Field(default=100, ge=1, le=1000)


class DeciderEvaluationComputeRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date_from: date
    market_date_to: date
    agent_names: list[str] | None = Field(default=None, min_length=1, max_length=20)
    symbols: list[str] | None = Field(default=None, min_length=1, max_length=500)
    window_days: int = Field(default=5, ge=1, le=30)
    force_recompute: bool = False

    @model_validator(mode='after')
    def validate_window(self) -> 'DeciderEvaluationComputeRequest':
        if self.market_date_from > self.market_date_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        return self


class SignalDeciderEvaluationComputeRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date_from: date
    market_date_to: date
    symbols: list[str] | None = Field(default=None, min_length=1, max_length=500)
    horizons: list[int] | None = None
    evaluation_version: str | None = Field(default=None, max_length=64)
    force_recompute: bool = False

    @model_validator(mode='after')
    def validate_window(self) -> 'SignalDeciderEvaluationComputeRequest':
        if self.market_date_from > self.market_date_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        return self


class SignalDeciderPolicyCandidateRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date_from: date
    market_date_to: date
    symbols: list[str] | None = Field(default=None, min_length=1, max_length=500)
    horizons: list[int] | None = None
    primary_horizon_days: int = Field(default=5)
    evaluation_version: str | None = Field(default=None, max_length=64)
    min_sample_size: int = Field(default=3, ge=1, le=1000)
    force_recompute: bool = False

    @model_validator(mode='after')
    def validate_window(self) -> 'SignalDeciderPolicyCandidateRequest':
        if self.market_date_from > self.market_date_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        return self


class SignalDeciderPolicyActivationRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    activated_by: str = Field(..., min_length=1, max_length=128)
    activation_note: str | None = Field(default=None, max_length=500)


class AgentSubagentExperimentRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date: date
    symbols: list[str] = Field(..., min_length=1, max_length=100)
    roles: list[str] | None = None
    fs_div: str | None = None
    disclosures_limit: int = Field(default=5, ge=0, le=20)
    per_role_parallelism: int = Field(default=5, ge=1, le=10)
    wait_for_completion: bool = True
    force: bool = False


def _inspect_feedback_prerequisites(
    *,
    market_date: date,
    fs_div: str | None = None,
    force: bool = False,
) -> dict[str, Any] | None:
    if force:
        return None
    prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
        market_date=market_date,
        fs_div=fs_div,
    )
    return None if prerequisites['ready'] else prerequisites


def _build_feedback_gate_response(
    *,
    request_payload: dict[str, Any],
    prerequisites: dict[str, Any],
) -> dict[str, Any]:
    return {
        'request': request_payload,
        'summary': {
            'status': prerequisites['status'],
            'completed': 0,
            'failed': 0,
            'skipped': 0,
            'flushed_to_db': 0,
            'gated': True,
        },
        'items': [],
        'failures': [],
        'prerequisites': prerequisites,
    }


def _build_evaluation_gate_response(
    *,
    request_payload: dict[str, Any],
    prerequisites: dict[str, Any],
) -> dict[str, Any]:
    return {
        'request': request_payload,
        'summary': {
            'status': prerequisites['status'],
            'gated': True,
            'outcomes_upserted': 0,
            'evaluations_upserted': 0,
        },
        'items': [],
        'prerequisites': prerequisites,
    }


def _to_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _normalize_signal_decider_action_key(value: Any) -> str:
    normalized = str(value or '').strip().lower().replace('-', '_')
    if normalized in {'buy'}:
        return 'buy'
    if normalized in {'wait', 'hold', 'watch', 'watchlist'}:
        return 'wait'
    if normalized in {'reject', 'avoid', 'skip'}:
        return 'reject'
    return 'unknown'


def _normalize_text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for entry in value:
        text = str(entry).strip() if entry is not None else ''
        if text:
            items.append(text)
    return items


def _extract_summary_text(value: Any) -> str | None:
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    if isinstance(value, dict):
        for key in ('summary', 'thesis_summary', 'headline', 'text'):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return None


def _extract_reviewed_count(payload: dict[str, Any] | None) -> int:
    payload = payload or {}
    reviewed_count = payload.get('reviewed_decision_count')
    if reviewed_count is not None:
        return _to_non_negative_int(reviewed_count)
    evaluation_coverage = payload.get('evaluation_coverage')
    if isinstance(evaluation_coverage, dict):
        complete_count = evaluation_coverage.get('complete_count')
        if complete_count is not None:
            return _to_non_negative_int(complete_count)
    review_status_counts = payload.get('review_status_counts')
    if isinstance(review_status_counts, dict):
        if review_status_counts.get('reviewed') is not None:
            return _to_non_negative_int(review_status_counts.get('reviewed'))
        if review_status_counts.get('complete') is not None:
            return _to_non_negative_int(review_status_counts.get('complete'))
    return 0


def _normalize_review_status_counts(payload: dict[str, Any] | None) -> dict[str, int]:
    raw_counts = payload.get('review_status_counts') if isinstance(payload, dict) else None
    if not isinstance(raw_counts, dict):
        return {}
    normalized_counts: dict[str, int] = {}
    for raw_key, raw_value in raw_counts.items():
        normalized_key = str(raw_key or '').strip().lower()
        if not normalized_key:
            continue
        if normalized_key == 'complete':
            normalized_key = 'reviewed'
        normalized_counts[normalized_key] = normalized_counts.get(normalized_key, 0) + _to_non_negative_int(raw_value)
    return dict(sorted(normalized_counts.items()))


def _has_review_rows(payload: dict[str, Any]) -> bool:
    items = payload.get('items')
    if isinstance(items, list) and len(items) > 0:
        return True
    summary = payload.get('summary')
    if isinstance(summary, dict):
        decision_count = summary.get('decision_count', summary.get('count'))
        return _to_non_negative_int(decision_count) > 0
    return False


def _resolve_feedback_review_payload(
    *,
    projection_loader,
    fallback_loader,
) -> tuple[dict[str, Any], dict[str, Any]]:
    projection_payload = projection_loader()
    if _has_review_rows(projection_payload):
        return projection_payload, {
            'status': 'ready',
            'source': 'persisted_review_projection',
            'fallback_used': False,
        }

    fallback_payload = fallback_loader()
    if _has_review_rows(fallback_payload):
        return fallback_payload, {
            'status': 'fallback',
            'source': 'signal_decider_snapshot_fallback',
            'fallback_used': True,
        }

    return projection_payload, {
        'status': 'empty',
        'source': 'persisted_review_projection',
        'fallback_used': False,
    }


def _build_feedback_review_availability(metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        'status': metadata['status'],
        'source': metadata['source'],
        'fallback_used': bool(metadata['fallback_used']),
    }


def _build_feedback_calendar_summary_item(raw_item: dict[str, Any], *, source: str) -> dict[str, Any]:
    decision_count = _to_non_negative_int(raw_item.get('decision_count'))
    reviewed_count = _extract_reviewed_count(raw_item)
    review_status_counts = _normalize_review_status_counts(raw_item)
    action_counts = {'buy': 0, 'wait': 0, 'reject': 0, 'unknown': 0}
    raw_action_counts = raw_item.get('decision_actions')
    if isinstance(raw_action_counts, dict):
        for raw_key, raw_value in raw_action_counts.items():
            action_counts[_normalize_signal_decider_action_key(raw_key)] += _to_non_negative_int(raw_value)

    return {
        'market_date': str(raw_item.get('market_date') or ''),
        'calendar_status': signal_decider_loop_service.build_calendar_day_status(
            decision_count=decision_count,
            reviewed_count=reviewed_count,
        ),
        'decision_count': decision_count,
        'decision_actions': action_counts,
        'review_status_counts': review_status_counts or ({'reviewed': reviewed_count} if reviewed_count > 0 else {}),
        'reviewed_count': reviewed_count,
        'pending_review_count': _to_non_negative_int(raw_item.get('pending_review_count'))
        or max(decision_count - reviewed_count, 0),
        'evaluation_coverage': raw_item.get('evaluation_coverage'),
        'scorecard': raw_item.get('scorecard'),
        'source': source,
    }


def _build_feedback_calendar_response(
    *,
    review_payload: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    request_payload = dict(review_payload.get('request') or {})
    summary_payload = dict(review_payload.get('summary') or {})
    decision_count = _to_non_negative_int(summary_payload.get('decision_count', summary_payload.get('count')))
    reviewed_count = _extract_reviewed_count(summary_payload)
    review_status_counts = _normalize_review_status_counts(summary_payload)
    items = [
        _build_feedback_calendar_summary_item(item, source=metadata['source'])
        for item in list(review_payload.get('items') or [])
        if isinstance(item, dict)
    ]
    return {
        'request': {
            'month': request_payload.get('month'),
            'horizons': list(request_payload.get('horizons') or []),
            'primary_horizon_days': request_payload.get('primary_horizon_days'),
            'evaluation_version': request_payload.get('evaluation_version'),
        },
        'summary': {
            'month': request_payload.get('month'),
            'decision_count': decision_count,
            'reviewed_count': reviewed_count,
            'status': signal_decider_loop_service.build_calendar_day_status(
                decision_count=decision_count,
                reviewed_count=reviewed_count,
            ),
            'pending_review_count': _to_non_negative_int(summary_payload.get('pending_review_count'))
            or max(decision_count - reviewed_count, 0),
            'review_status_counts': review_status_counts or ({'reviewed': reviewed_count} if reviewed_count > 0 else {}),
            'evaluation_coverage': summary_payload.get('evaluation_coverage'),
            'horizon_coverage': summary_payload.get('horizon_coverage'),
            'scorecard': summary_payload.get('scorecard'),
            'source': metadata['source'],
            'request': {
                'month': request_payload.get('month'),
                'decision_count': decision_count,
                'reviewed_count': reviewed_count,
            },
        },
        'items': items,
        'availability': _build_feedback_review_availability(metadata),
        'read_model': review_payload.get('read_model'),
        'taxonomy': review_payload.get('taxonomy'),
    }


def _build_review_realized_results(review_payload: dict[str, Any]) -> dict[str, Any]:
    realized_results: dict[str, Any] = {}
    for horizon_item in list(review_payload.get('outcome_horizons') or review_payload.get('horizons') or []):
        if not isinstance(horizon_item, dict):
            continue
        horizon_days = _to_non_negative_int(horizon_item.get('horizon_days'))
        if horizon_days <= 0:
            continue
        realized_results[f'{horizon_days}d'] = {
            'return_pct': horizon_item.get('close_return_pct'),
            'realized_return_pct': horizon_item.get('close_return_pct'),
            'verdict': horizon_item.get('grade_label'),
            'close_price': horizon_item.get('realized_price') or horizon_item.get('close_price'),
            'status_reason': horizon_item.get('status_reason'),
            'coverage': horizon_item.get('coverage'),
            'notes': (
                horizon_item.get('path_analysis', {}).get('path_explanation')
                if isinstance(horizon_item.get('path_analysis'), dict)
                else None
            ),
        }
    return realized_results


def _build_feedback_review_detail_payload(raw_review: dict[str, Any] | None) -> dict[str, Any] | None:
    if raw_review is None:
        return None
    review_payload = dict(raw_review)
    outcome_horizons = [
        dict(item)
        for item in list(review_payload.get('outcome_horizons') or review_payload.get('horizons') or [])
        if isinstance(item, dict)
    ]
    path_analysis = review_payload.get('path_analysis')
    trigger_sequence = (
        path_analysis.get('trigger_sequence')
        if isinstance(path_analysis, dict)
        else review_payload.get('intraday_path_result')
    )
    review_payload['outcome_horizons'] = outcome_horizons
    review_payload['horizons'] = outcome_horizons
    if trigger_sequence:
        review_payload.setdefault('target_stop_result', trigger_sequence)
        review_payload.setdefault('intraday_path_result', trigger_sequence)
    review_summary = review_payload.get('narrative_text') or review_payload.get('review_conclusion_text')
    if review_summary:
        review_payload.setdefault('review_summary', review_summary)
        review_payload.setdefault('replay_summary', review_summary)
    return review_payload


def _build_feedback_review_decision_payload(raw_item: dict[str, Any]) -> dict[str, Any]:
    raw_decision = dict(raw_item.get('decision') or {})
    raw_review = dict(raw_item.get('review') or {})
    review_payload = _build_feedback_review_detail_payload(raw_review) or {}
    summary_text = _extract_summary_text(raw_item.get('summary'))
    decision_risks = _normalize_text_list(raw_item.get('decision_risks'))
    supporting_evidence = _normalize_text_list(raw_item.get('supporting_evidence'))
    evaluation_payload = {
        'quality_verdict': review_payload.get('quality_verdict'),
        'quality_reason': review_payload.get('quality_reason'),
        'review_conclusion': review_payload.get('review_conclusion'),
        'status_reason': review_payload.get('status_reason'),
        'coverage': review_payload.get('coverage'),
        'target_stop_result': review_payload.get('target_stop_result'),
        'intraday_path_result': review_payload.get('intraday_path_result'),
        'outcome_horizons': review_payload.get('outcome_horizons'),
        'realized_results': _build_review_realized_results(review_payload),
    }
    if not any(
        value not in (None, {}, [])
        for value in evaluation_payload.values()
    ):
        evaluation_payload = {}
    return {
        'role': 'signal-decider',
        'agent_name': 'signal-decider',
        'symbol': raw_item.get('symbol'),
        'market_date': raw_item.get('market_date'),
        'action': raw_decision.get('action'),
        'preferred_action': raw_decision.get('preferred_action') or raw_decision.get('action'),
        'next_day_open_action': raw_decision.get('next_day_open_action') or raw_decision.get('action'),
        'entry_price_basis': raw_decision.get('entry_price_basis'),
        'target_price': raw_decision.get('target_price'),
        'expected_return_pct': raw_decision.get('expected_return_pct'),
        'stop_loss_price': raw_decision.get('stop_loss_price'),
        'suggested_holding_period': raw_decision.get('suggested_holding_period'),
        'confidence_score_10': raw_decision.get('confidence_score_10'),
        'conviction_score_10': raw_decision.get('conviction_score_10'),
        'thesis_summary': summary_text,
        'supporting_evidence': supporting_evidence,
        'risk_flags': decision_risks,
        'decision_risks': decision_risks,
        'decision_delta': raw_item.get('decision_delta'),
        'policy_version': raw_item.get('policy_version'),
        'evaluation': evaluation_payload or None,
    }


def _build_feedback_review_item(raw_item: dict[str, Any], *, source: str) -> dict[str, Any]:
    summary_text = _extract_summary_text(raw_item.get('summary'))
    summary_payload: dict[str, Any] = {}
    if summary_text is not None:
        summary_payload['summary'] = summary_text
    supporting_evidence = _normalize_text_list(raw_item.get('supporting_evidence'))
    if supporting_evidence:
        summary_payload['supporting_evidence'] = supporting_evidence
    decision_risks = _normalize_text_list(raw_item.get('decision_risks'))
    if decision_risks:
        summary_payload['decision_risks'] = decision_risks
    if raw_item.get('policy_version') is not None:
        summary_payload['policy_version'] = raw_item.get('policy_version')
    context_payload = raw_item.get('context') if isinstance(raw_item.get('context'), dict) else None
    run_id = raw_item.get('decision_run_id')
    return {
        'snapshot_id': raw_item.get('snapshot_id'),
        'symbol': raw_item.get('symbol'),
        'market_date': raw_item.get('market_date'),
        'run_id': str(run_id) if run_id is not None else None,
        'captured_at': raw_item.get('received_at') or raw_item.get('created_at'),
        'decision': _build_feedback_review_decision_payload(raw_item),
        'review': _build_feedback_review_detail_payload(dict(raw_item.get('review') or {})),
        'summary': summary_payload or None,
        'context': context_payload,
        'source': source,
    }


def _build_feedback_review_response(
    *,
    review_payload: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    request_payload = dict(review_payload.get('request') or {})
    raw_items = [
        dict(item)
        for item in list(review_payload.get('items') or [])
        if isinstance(item, dict)
    ]
    items = [
        _build_feedback_review_item(item, source=metadata['source'])
        for item in raw_items
    ]
    summary_payload = dict(review_payload.get('summary') or {})
    decision_count = _to_non_negative_int(summary_payload.get('count', len(items)))
    if decision_count <= 0 and items:
        decision_count = len(items)
    reviewed_count = _extract_reviewed_count(summary_payload)
    review_status_counts = _normalize_review_status_counts(summary_payload)
    if reviewed_count <= 0:
        reviewed_count = sum(
            1
            for item in raw_items
            if isinstance(item.get('review'), dict) and str(item.get('review', {}).get('status') or '').strip().lower() == 'complete'
        )
    status = signal_decider_loop_service.build_calendar_day_status(
        decision_count=decision_count,
        reviewed_count=reviewed_count,
    )
    return {
        'request': {
            'market_date': request_payload.get('market_date'),
            'horizons': list(request_payload.get('horizons') or []),
            'primary_horizon_days': request_payload.get('primary_horizon_days'),
            'evaluation_version': request_payload.get('evaluation_version'),
        },
        'summary': {
            'count': decision_count,
            'reviewed_count': reviewed_count,
            'pending_review_count': _to_non_negative_int(summary_payload.get('pending_review_count'))
            or max(decision_count - reviewed_count, 0),
            'review_status_counts': review_status_counts or ({'reviewed': reviewed_count} if reviewed_count > 0 else {}),
            'evaluation_coverage': summary_payload.get('evaluation_coverage'),
            'horizon_coverage': summary_payload.get('horizon_coverage'),
            'insight_counts': summary_payload.get('insight_counts'),
            'path_marker_counts': summary_payload.get('path_marker_counts'),
            'status': status,
            'source': metadata['source'],
        },
        'items': items,
        'availability': _build_feedback_review_availability(metadata),
        'read_model': review_payload.get('read_model'),
        'taxonomy': review_payload.get('taxonomy'),
    }


@router.get('/symbol-day')
async def get_symbol_day_context(
    market_date: date = Query(...),
    symbols: list[str] = Query(...),
    fs_div: str | None = Query(default=None),
    disclosures_limit: int = Query(default=5, ge=0, le=20),
) -> dict:
    try:
        return agent_context_read_service.get_symbol_day_context(
            market_date=market_date,
            symbols=symbols,
            fs_div=fs_div,
            disclosures_limit=disclosures_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent context read failed: {exc}') from exc


@router.post('/feedback', status_code=201)
async def submit_agent_feedback(payload: AgentFeedbackRequest) -> dict:
    try:
        return agent_context_feedback_service.submit_feedback(
            market_date=payload.market_date,
            symbol=payload.symbol,
            agent_name=payload.agent_name,
            agent_version=payload.agent_version,
            run_id=payload.run_id,
            summary=payload.summary,
            feedback=payload.feedback,
            context=payload.context,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback submit failed: {exc}') from exc


@router.post('/feedback/dispatch')
async def dispatch_agent_feedback(payload: AgentFeedbackDispatchRequest) -> dict:
    try:
        return await run_in_threadpool(
            agent_feedback_orchestrator.dispatch_for_eod_run,
            eod_run_id=payload.eod_run_id,
            roles=payload.roles,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback dispatch failed: {exc}') from exc


@router.post('/feedback/dispatch/symbols')
async def dispatch_symbol_agent_feedback(
    payload: AgentSymbolFeedbackDispatchRequest,
    background_tasks: BackgroundTasks,
) -> dict:
    try:
        if payload.wait_for_completion:
            return await run_in_threadpool(
                agent_feedback_orchestrator.dispatch_for_symbols,
                market_date=payload.market_date,
                symbols=payload.symbols,
                roles=payload.roles,
                fs_div=payload.fs_div,
                disclosures_limit=payload.disclosures_limit,
                reuse_existing=payload.reuse_existing,
                force=payload.force,
            )

        requested_roles = list(payload.roles or EXPECTED_ROLES)
        requested_symbols = list(dict.fromkeys(payload.symbols))
        prerequisites = _inspect_feedback_prerequisites(
            market_date=payload.market_date,
            fs_div=payload.fs_div,
            force=payload.force,
        )
        request_payload = {
            'market_date': payload.market_date.isoformat(),
            'symbols': requested_symbols,
            'requested_symbols': requested_symbols,
            'request_run_id': None,
            'roles': requested_roles,
            'fs_div': payload.fs_div,
            'disclosures_limit': payload.disclosures_limit,
            'reuse_existing': payload.reuse_existing,
            'scope': resolve_agent_feedback_scope(requested_symbols=requested_symbols),
            'eligibility_rule': AGENT_FEEDBACK_ELIGIBILITY_RULE,
            'eligibility_filters': list(AGENT_FEEDBACK_ELIGIBILITY_FILTERS),
            'wait_for_completion': False,
        }
        if prerequisites is not None:
            return _build_feedback_gate_response(
                request_payload=request_payload,
                prerequisites=prerequisites,
            )

        request_run_id = f"symbol-run-{payload.market_date.isoformat()}-{uuid.uuid4().hex[:10]}"
        started_at = datetime.now(timezone.utc)
        request_payload = {
            **request_payload,
            'request_run_id': request_run_id,
            'mode': 'role_sequential_runtime_buffered',
        }

        def _bootstrap_placeholder() -> None:
            state = RuntimeFeedbackRunState(
                run_id=request_run_id,
                market_date=payload.market_date,
                requested_symbols=requested_symbols,
                requested_roles=requested_roles,
                role_sequence=requested_roles,
                parallel_per_role=0,
                batch_size=max(1, len(requested_symbols)),
                started_at=started_at,
                current_role=requested_roles[0] if requested_roles else None,
            )
            state.status = 'queued'
            runtime_feedback_state_store.start_run(state)
            agent_context_feedback_run_state_service.start_run(
                run_id=request_run_id,
                market_date=payload.market_date,
                symbols_total=len(requested_symbols),
                request_payload=request_payload,
                summary_payload={
                    'market_date': payload.market_date.isoformat(),
                    'request': request_payload,
                    'lifecycle_stage': 'queued',
                    'current_role': state.current_role,
                    'role_sequence': requested_roles,
                    'progress_summary': state.to_progress_summary(),
                },
                status='queued',
                started_at=started_at,
            )

        def _mark_background_startup_failure(exc: Exception) -> None:
            run_snapshot = agent_context_feedback_run_state_service.get_run(run_id=request_run_id)
            if run_snapshot is not None:
                run_status = str(run_snapshot.get('status') or '').strip().lower()
                if run_snapshot.get('completed_at') is not None or run_status in {'completed', 'partial', 'failed'}:
                    return

            completed_count = int(run_snapshot.get('completed_count') or 0) if isinstance(run_snapshot, dict) else 0
            failed_count = int(run_snapshot.get('failed_count') or 0) if isinstance(run_snapshot, dict) else 0
            final_status = 'partial' if completed_count > 0 or failed_count > 0 else 'failed'
            completed_at = datetime.now(timezone.utc)

            def _update_runtime_state(current: RuntimeFeedbackRunState) -> None:
                current.status = final_status
                current.completed_at = completed_at
                current.last_activity_at = completed_at
                current.cycle_status = final_status

            state = runtime_feedback_state_store.update_run(
                payload.market_date,
                _update_runtime_state,
                run_id=request_run_id,
            )
            progress_summary = state.to_progress_summary() if state is not None else None
            if progress_summary is not None:
                completed_count = int(progress_summary.get('completed_feedback_count') or completed_count)
                failed_count = int(progress_summary.get('failed_items') or failed_count)
                final_status = 'partial' if completed_count > 0 or failed_count > 0 else 'failed'
                if state.status != final_status:
                    state.status = final_status
                progress_summary['run_status'] = final_status
                progress_summary['run_table_status'] = final_status
                progress_summary['is_live'] = False
                progress_summary['run_completed_at'] = completed_at.isoformat()

            agent_context_feedback_run_state_service.finish_run(
                run_id=request_run_id,
                status=final_status,
                completed_count=completed_count,
                failed_count=failed_count,
                summary_payload={
                    'market_date': payload.market_date.isoformat(),
                    'request': request_payload,
                    'lifecycle_stage': 'background_startup_failed',
                    'current_role': requested_roles[0] if requested_roles else None,
                    'role_sequence': requested_roles,
                    'progress_summary': progress_summary,
                    'fatal_error': f'{type(exc).__name__}: {exc}',
                },
            )

        def _run_dispatch() -> None:
            try:
                agent_feedback_orchestrator.dispatch_for_symbols(
                    market_date=payload.market_date,
                    symbols=requested_symbols,
                    roles=requested_roles,
                    fs_div=payload.fs_div,
                    disclosures_limit=payload.disclosures_limit,
                    reuse_existing=payload.reuse_existing,
                    request_run_id=request_run_id,
                    force=payload.force,
                )
            except Exception as exc:  # noqa: BLE001
                try:
                    _mark_background_startup_failure(exc)
                except Exception:  # noqa: BLE001
                    logger.exception(
                        'Agent symbol feedback background startup failure finalization failed',
                        extra={
                            'request_run_id': request_run_id,
                            'market_date': payload.market_date.isoformat(),
                            'requested_roles': requested_roles,
                            'requested_symbols': requested_symbols,
                            'error': str(exc),
                        },
                    )
                else:
                    logger.exception(
                        'Agent symbol feedback background dispatch failed after queue handoff',
                        extra={
                            'request_run_id': request_run_id,
                            'market_date': payload.market_date.isoformat(),
                            'requested_roles': requested_roles,
                            'requested_symbols': requested_symbols,
                            'error': str(exc),
                        },
                    )

        _bootstrap_placeholder()
        background_tasks.add_task(_run_dispatch)
        return {
            'request': {
                'market_date': payload.market_date.isoformat(),
                'symbols': requested_symbols,
                'requested_symbols': requested_symbols,
                'roles': requested_roles,
                'fs_div': payload.fs_div,
                'disclosures_limit': payload.disclosures_limit,
                'reuse_existing': payload.reuse_existing,
                'scope': resolve_agent_feedback_scope(requested_symbols=requested_symbols),
                'eligibility_rule': AGENT_FEEDBACK_ELIGIBILITY_RULE,
                'eligibility_filters': list(AGENT_FEEDBACK_ELIGIBILITY_FILTERS),
                'request_run_id': request_run_id,
                'wait_for_completion': False,
            },
            'summary': {
                'status': 'queued',
                'queued': len(requested_symbols),
            },
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent symbol feedback dispatch failed: {exc}') from exc


@router.post('/feedback/retry-cycles')
async def run_agent_feedback_retry_cycles(payload: AgentFeedbackRetryCyclesRequest) -> dict:
    try:
        config = FeedbackRetryCycleConfig(
            market_date=payload.market_date,
            roles=tuple(payload.roles or EXPECTED_ROLES),
            symbols=tuple(payload.symbols) if payload.symbols is not None else None,
            max_cycles=payload.max_cycles,
            outer_batch_size=payload.outer_batch_size,
            reuse_existing=payload.reuse_existing,
        )
        return await run_in_threadpool(
            agent_feedback_retry_service.run_retry_cycles,
            config=config,
            force=payload.force,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback retry cycles failed: {exc}') from exc


@router.post('/feedback/followup')
async def run_agent_feedback_followup(payload: AgentFeedbackFollowupRequest) -> dict:
    try:
        config = FeedbackRetryCycleConfig(
            market_date=payload.market_date,
            roles=tuple(payload.roles or EXPECTED_ROLES),
            symbols=tuple(payload.symbols) if payload.symbols is not None else None,
            max_cycles=payload.max_cycles,
            outer_batch_size=payload.outer_batch_size,
            reuse_existing=payload.reuse_existing,
        )
        return await run_in_threadpool(
            agent_feedback_retry_service.run_retry_cycles,
            config=config,
            force=payload.force,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback followup failed: {exc}') from exc


@router.post('/feedback/session-cleanup/run')
async def run_agent_feedback_session_cleanup(payload: AgentFeedbackCleanupRunRequest) -> dict:
    try:
        return await run_in_threadpool(
            agent_feedback_session_cleanup_service.run_pending_cleanup,
            limit=payload.limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback session cleanup run failed: {exc}') from exc


@router.get('/feedback/session-cleanup')
async def list_agent_feedback_session_cleanup(
    market_date: date | None = Query(default=None),
    agent_name: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict:
    try:
        return await run_in_threadpool(
            agent_feedback_session_cleanup_service.list_failures,
            market_date=market_date,
            agent_name=agent_name,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback session cleanup list failed: {exc}') from exc


@router.post('/feedback/dispatch/subagent-experiment')
async def dispatch_symbol_agent_feedback_via_subagents(payload: AgentSubagentExperimentRequest, background_tasks: BackgroundTasks) -> dict:
    try:
        prerequisites = _inspect_feedback_prerequisites(
            market_date=payload.market_date,
            fs_div=payload.fs_div,
            force=payload.force,
        )
        if prerequisites is not None:
            return _build_feedback_gate_response(
                request_payload={
                    'market_date': payload.market_date.isoformat(),
                    'symbols': payload.symbols,
                    'requested_symbols': payload.symbols,
                    'request_run_id': None,
                    'roles': payload.roles or EXPECTED_ROLES,
                    'fs_div': payload.fs_div,
                    'disclosures_limit': payload.disclosures_limit,
                    'per_role_parallelism': payload.per_role_parallelism,
                    'mode': 'subagent_experiment',
                    'wait_for_completion': payload.wait_for_completion,
                },
                prerequisites=prerequisites,
            )
        if not payload.wait_for_completion:
            request_run_id = f'subagent-symbol-run-{payload.market_date.isoformat()}-api-{payload.per_role_parallelism}x{len(payload.symbols)}'

            def _run_background_subagent_dispatch() -> None:
                try:
                    result = agent_feedback_orchestrator.dispatch_for_symbols_via_subagents(
                        market_date=payload.market_date,
                        symbols=payload.symbols,
                        roles=payload.roles,
                        fs_div=payload.fs_div,
                        disclosures_limit=payload.disclosures_limit,
                        per_role_parallelism=payload.per_role_parallelism,
                    )
                    logger.info(
                        'Subagent experiment background dispatch finished',
                        extra={
                            'request_run_id_hint': request_run_id,
                            'market_date': payload.market_date.isoformat(),
                            'symbol_count': len(payload.symbols),
                            'roles': payload.roles or EXPECTED_ROLES,
                            'summary': result.get('summary'),
                        },
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        'Subagent experiment background dispatch failed',
                        extra={
                            'request_run_id_hint': request_run_id,
                            'market_date': payload.market_date.isoformat(),
                            'symbol_count': len(payload.symbols),
                            'roles': payload.roles or EXPECTED_ROLES,
                            'error': str(exc),
                        },
                    )

            background_tasks.add_task(_run_background_subagent_dispatch)
            return {
                'request': {
                    'market_date': payload.market_date.isoformat(),
                    'symbols': payload.symbols,
                    'roles': payload.roles or EXPECTED_ROLES,
                    'fs_div': payload.fs_div,
                    'disclosures_limit': payload.disclosures_limit,
                    'per_role_parallelism': payload.per_role_parallelism,
                    'mode': 'subagent_experiment',
                    'wait_for_completion': False,
                    'note': 'Background dispatch has been scheduled. Use DB/progress/session checks to observe completion.',
                    'request_run_id_hint': request_run_id,
                },
                'summary': {
                    'status': 'accepted',
                },
            }
        return await run_in_threadpool(
            agent_feedback_orchestrator.dispatch_for_symbols_via_subagents,
            market_date=payload.market_date,
            symbols=payload.symbols,
            roles=payload.roles,
            fs_div=payload.fs_div,
            disclosures_limit=payload.disclosures_limit,
            per_role_parallelism=payload.per_role_parallelism,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Subagent experiment dispatch failed: {exc}') from exc


@router.get('/feedback/payloads')
async def list_agent_feedback_payloads(
    market_date: date = Query(...),
    symbols: list[str] = Query(...),
    agent_name: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        return agent_context_feedback_service.get_feedback_payloads(
            market_date=market_date,
            symbols=symbols,
            agent_name=agent_name,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback payload read failed: {exc}') from exc


@router.get('/feedback/data-gap-summary')
async def get_agent_feedback_data_gap_summary(
    market_date: date | None = Query(default=None),
    market_date_from: date | None = Query(default=None),
    market_date_to: date | None = Query(default=None),
    symbols: list[str] | None = Query(default=None),
    agent_name: str | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
) -> dict:
    try:
        return agent_context_missing_data_report_service.get_summary(
            market_date=market_date,
            market_date_from=market_date_from,
            market_date_to=market_date_to,
            symbols=symbols,
            agent_name=agent_name,
            categories=categories,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback data-gap summary failed: {exc}') from exc


@router.get('/feedback/requests')
async def list_agent_request_payloads(
    market_date: date = Query(...),
    symbols: list[str] = Query(...),
    agent_name: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        return agent_context_feedback_service.get_request_payloads(
            market_date=market_date,
            symbols=symbols,
            agent_name=agent_name,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent request payload read failed: {exc}') from exc


@router.get('/feedback/status')
async def get_agent_feedback_status(
    market_date: date = Query(...),
    symbols: list[str] = Query(...),
) -> dict:
    try:
        return agent_context_status_service.get_feedback_status(
            market_date=market_date,
            symbols=symbols,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback status read failed: {exc}') from exc


@router.get('/feedback/progress')
async def get_agent_feedback_progress(
    market_date: date = Query(...),
    symbols: list[str] | None = Query(default=None),
    run_id: str | None = Query(default=None),
) -> dict:
    try:
        return agent_context_progress_service.get_feedback_progress(
            market_date=market_date,
            symbols=symbols,
            run_id=run_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback progress read failed: {exc}') from exc


@router.get('/feedback/calendar')
async def get_agent_feedback_calendar_summary(
    month: str = Query(...),
    horizons: list[int] | None = Query(default=None),
    primary_horizon_days: int = Query(default=5),
    evaluation_version: str | None = Query(default=None),
) -> dict:
    try:
        review_payload, metadata = await run_in_threadpool(
            _resolve_feedback_review_payload,
            projection_loader=lambda: signal_decider_calendar_review_service.get_review_calendar(
                month=month,
                horizons=horizons,
                primary_horizon_days=primary_horizon_days,
                evaluation_version=evaluation_version,
            ),
            fallback_loader=lambda: signal_decider_loop_service.get_review_calendar(
                month=month,
                horizons=horizons,
                primary_horizon_days=primary_horizon_days,
                evaluation_version=evaluation_version,
            ),
        )
        return _build_feedback_calendar_response(
            review_payload=review_payload,
            metadata=metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback calendar summary read failed: {exc}') from exc


@router.get('/feedback/review')
async def get_agent_feedback_review_date(
    market_date: date = Query(...),
    horizons: list[int] | None = Query(default=None),
    primary_horizon_days: int = Query(default=5),
    evaluation_version: str | None = Query(default=None),
) -> dict:
    try:
        review_payload, metadata = await run_in_threadpool(
            _resolve_feedback_review_payload,
            projection_loader=lambda: signal_decider_calendar_review_service.get_review_date(
                market_date=market_date,
                horizons=horizons,
                primary_horizon_days=primary_horizon_days,
                evaluation_version=evaluation_version,
            ),
            fallback_loader=lambda: signal_decider_loop_service.get_review_date(
                market_date=market_date,
                horizons=horizons,
                primary_horizon_days=primary_horizon_days,
                evaluation_version=evaluation_version,
            ),
        )
        return _build_feedback_review_response(
            review_payload=review_payload,
            metadata=metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback review read failed: {exc}') from exc


@router.get('/feedback/latest')
async def get_latest_agent_feedback_bundle(
    market_date: date = Query(...),
    symbols: list[str] = Query(...),
    roles: list[str] | None = Query(default=None),
) -> dict:
    try:
        return agent_context_reuse_service.build_latest_feedback_bundle(
            market_date=market_date,
            symbols=symbols,
            roles=roles,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Latest agent feedback read failed: {exc}') from exc


@router.get('/feedback/fundamental-summaries')
async def inspect_fundamental_summarizer_feedback(
    market_date: date = Query(...),
    symbol: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        symbols = [symbol] if symbol is not None else None
        return agent_context_feedback_service.list_feedback(
            market_date=market_date,
            symbols=symbols,
            agent_name='fundamental-summarizer',
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Fundamental summarizer feedback read failed: {exc}') from exc


@router.get('/feedback/technical-summaries')
async def inspect_technical_summarizer_feedback(
    market_date: date = Query(...),
    symbol: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        symbols = [symbol] if symbol is not None else None
        return agent_context_feedback_service.list_feedback(
            market_date=market_date,
            symbols=symbols,
            agent_name='technical-summarizer',
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Technical summarizer feedback read failed: {exc}') from exc


@router.get('/feedback/eod-summaries')
async def inspect_eod_summarizer_feedback(
    market_date: date = Query(...),
    symbol: str | None = Query(default=None),
) -> dict:
    try:
        return agent_context_eod_summaries_service.inspect(
            market_date=market_date,
            symbol=symbol,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'EOD summarizer feedback read failed: {exc}') from exc


@router.get('/feedback')
async def list_agent_feedback(
    market_date: date = Query(...),
    symbols: list[str] | None = Query(default=None),
    agent_name: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        return agent_context_feedback_service.list_feedback(
            market_date=market_date,
            symbols=symbols,
            agent_name=agent_name,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent feedback read failed: {exc}') from exc


@router.post('/decider/evaluations/compute')
async def compute_decider_post_decision_evaluations(payload: DeciderEvaluationComputeRequest) -> dict:
    try:
        prerequisites = _inspect_feedback_prerequisites(
            market_date=payload.market_date_to,
            force=payload.force_recompute,
        )
        if prerequisites is not None:
            request_payload = payload.model_dump() if hasattr(payload, 'model_dump') else payload.dict()
            request_payload['market_date_from'] = payload.market_date_from.isoformat()
            request_payload['market_date_to'] = payload.market_date_to.isoformat()
            return _build_evaluation_gate_response(
                request_payload=request_payload,
                prerequisites=prerequisites,
            )
        return await run_in_threadpool(
            decider_post_decision_evaluation_service.compute_evaluations,
            market_date_from=payload.market_date_from,
            market_date_to=payload.market_date_to,
            agent_names=payload.agent_names,
            symbols=payload.symbols,
            window_days=payload.window_days,
            force_recompute=payload.force_recompute,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Decider evaluation compute failed: {exc}') from exc


@router.get('/decider/evaluations')
async def list_decider_post_decision_evaluations(
    market_date: date | None = Query(default=None),
    market_date_from: date | None = Query(default=None),
    market_date_to: date | None = Query(default=None),
    agent_names: list[str] | None = Query(default=None),
    symbols: list[str] | None = Query(default=None),
    outcome_labels: list[str] | None = Query(default=None),
    window_days: int = Query(default=5, ge=1, le=30),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        return await run_in_threadpool(
            decider_post_decision_evaluation_service.list_evaluations,
            market_date=market_date,
            market_date_from=market_date_from,
            market_date_to=market_date_to,
            agent_names=agent_names,
            symbols=symbols,
            outcome_labels=outcome_labels,
            window_days=window_days,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Decider evaluation read failed: {exc}') from exc


@router.get('/signal-decider/decisions')
async def list_signal_decider_decisions(
    market_date: date | None = Query(default=None),
    market_date_from: date | None = Query(default=None),
    market_date_to: date | None = Query(default=None),
    symbol: str | None = Query(default=None),
    action: str | None = Query(default=None),
    policy_version: str | None = Query(default=None),
    verdict: str | None = Query(default=None),
    has_missed_opportunity: bool | None = Query(default=None),
    horizons: list[int] | None = Query(default=None),
    evaluation_version: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_loop_service.list_decisions,
            market_date=market_date,
            market_date_from=market_date_from,
            market_date_to=market_date_to,
            symbol=symbol,
            action=action,
            policy_version=policy_version,
            verdict=verdict,
            has_missed_opportunity=has_missed_opportunity,
            horizons=horizons,
            evaluation_version=evaluation_version,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider decision read failed: {exc}') from exc


@router.get('/signal-decider/decisions/{snapshot_id}')
async def get_signal_decider_decision_detail(
    snapshot_id: int,
    horizons: list[int] | None = Query(default=None),
    evaluation_version: str | None = Query(default=None),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_loop_service.get_decision_detail,
            snapshot_id=snapshot_id,
            horizons=horizons,
            evaluation_version=evaluation_version,
        )
    except ValueError as exc:
        if 'was not found' in str(exc):
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider decision detail read failed: {exc}') from exc


@router.get('/signal-decider/calendar')
async def get_signal_decider_review_calendar(
    month: str = Query(...),
    horizons: list[int] | None = Query(default=None),
    primary_horizon_days: int = Query(default=5),
    evaluation_version: str | None = Query(default=None),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_calendar_review_service.get_review_calendar,
            month=month,
            horizons=horizons,
            primary_horizon_days=primary_horizon_days,
            evaluation_version=evaluation_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider replay calendar read failed: {exc}') from exc


@router.get('/signal-decider/calendar/maintenance')
async def inspect_signal_decider_review_calendar_maintenance(
    evaluation_version: str | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_calendar_review_service.inspect_maintenance,
            evaluation_version=evaluation_version,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider calendar maintenance inspect failed: {exc}') from exc


@router.get('/signal-decider/calendar/{market_date}')
async def get_signal_decider_review_date(
    market_date: date,
    horizons: list[int] | None = Query(default=None),
    primary_horizon_days: int = Query(default=5),
    evaluation_version: str | None = Query(default=None),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_calendar_review_service.get_review_date,
            market_date=market_date,
            horizons=horizons,
            primary_horizon_days=primary_horizon_days,
            evaluation_version=evaluation_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider replay date review failed: {exc}') from exc


@router.get('/signal-decider/calendar/{market_date}/snapshots/{snapshot_id}')
async def get_signal_decider_review_detail(
    market_date: date,
    snapshot_id: int,
    horizons: list[int] | None = Query(default=None),
    primary_horizon_days: int = Query(default=5),
    evaluation_version: str | None = Query(default=None),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_calendar_review_service.get_review_detail,
            market_date=market_date,
            snapshot_id=snapshot_id,
            horizons=horizons,
            primary_horizon_days=primary_horizon_days,
            evaluation_version=evaluation_version,
        )
    except ValueError as exc:
        if 'was not found' in str(exc) or 'does not belong' in str(exc):
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider replay detail read failed: {exc}') from exc


@router.get('/signal-decider/execution-outcomes')
async def list_signal_decider_execution_outcomes(
    snapshot_id: int | None = Query(default=None),
    market_date: date | None = Query(default=None),
    market_date_from: date | None = Query(default=None),
    market_date_to: date | None = Query(default=None),
    symbol: str | None = Query(default=None),
    account_reference: str | None = Query(default=None),
    policy_version: str | None = Query(default=None),
    lifecycle_status: str | None = Query(default=None),
    exit_reason: str | None = Query(default=None),
    refresh: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_execution_outcome_service.list_outcomes,
            snapshot_id=snapshot_id,
            market_date=market_date,
            market_date_from=market_date_from,
            market_date_to=market_date_to,
            symbol=symbol,
            account_reference=account_reference,
            policy_version=policy_version,
            lifecycle_status=lifecycle_status,
            exit_reason=exit_reason,
            refresh=refresh,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider execution outcome read failed: {exc}') from exc


@router.get('/signal-decider/diagnostics/summary')
async def get_signal_decider_diagnostics_summary(
    market_date: date | None = Query(default=None),
    market_date_from: date | None = Query(default=None),
    market_date_to: date | None = Query(default=None),
    symbols: list[str] | None = Query(default=None),
    action: str | None = Query(default=None),
    policy_version: str | None = Query(default=None),
    horizons: list[int] | None = Query(default=None),
    evaluation_version: str | None = Query(default=None),
    contributor_limit: int = Query(default=20, ge=1, le=100),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_loop_service.get_aggregate_diagnostics,
            market_date=market_date,
            market_date_from=market_date_from,
            market_date_to=market_date_to,
            symbols=symbols,
            action=action,
            policy_version=policy_version,
            horizons=horizons,
            evaluation_version=evaluation_version,
            contributor_limit=contributor_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider diagnostics summary read failed: {exc}') from exc


@router.get('/signal-decider/diagnostics')
async def get_signal_decider_diagnostics(
    market_date: date = Query(...),
    symbol: str | None = Query(default=None),
    evaluation_version: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_loop_service.get_diagnostics,
            market_date=market_date,
            symbol=symbol,
            evaluation_version=evaluation_version,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider diagnostics read failed: {exc}') from exc


@router.post('/signal-decider/evaluations/compute')
async def compute_signal_decider_evaluations(payload: SignalDeciderEvaluationComputeRequest) -> dict:
    try:
        prerequisites = _inspect_feedback_prerequisites(
            market_date=payload.market_date_to,
            force=payload.force_recompute,
        )
        if prerequisites is not None:
            request_payload = payload.model_dump() if hasattr(payload, 'model_dump') else payload.dict()
            request_payload['market_date_from'] = payload.market_date_from.isoformat()
            request_payload['market_date_to'] = payload.market_date_to.isoformat()
            return _build_evaluation_gate_response(
                request_payload=request_payload,
                prerequisites=prerequisites,
            )
        return await run_in_threadpool(
            signal_decider_loop_service.compute_evaluations,
            market_date_from=payload.market_date_from,
            market_date_to=payload.market_date_to,
            symbols=payload.symbols,
            horizons=payload.horizons,
            evaluation_version=payload.evaluation_version,
            force_recompute=payload.force_recompute,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider evaluation compute failed: {exc}') from exc


@router.get('/signal-decider/evaluations')
async def list_signal_decider_evaluations(
    market_date: date = Query(...),
    symbol: str | None = Query(default=None),
    horizons: list[int] | None = Query(default=None),
    evaluation_version: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_loop_service.list_evaluations,
            market_date=market_date,
            symbol=symbol,
            horizons=horizons,
            evaluation_version=evaluation_version,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider evaluation read failed: {exc}') from exc


@router.get('/signal-decider/calibration')
async def get_signal_decider_calibration_report(
    market_date_from: date = Query(...),
    market_date_to: date = Query(...),
    symbols: list[str] | None = Query(default=None),
    policy_version: str | None = Query(default=None),
    horizon_days: int = Query(default=5),
    evaluation_version: str | None = Query(default=None),
    min_sample_size: int = Query(default=3, ge=1, le=1000),
    cluster_limit: int = Query(default=10, ge=1, le=50),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_calibration_service.build_report,
            market_date_from=market_date_from,
            market_date_to=market_date_to,
            symbols=symbols,
            policy_version=policy_version,
            horizon_days=horizon_days,
            evaluation_version=evaluation_version,
            min_sample_size=min_sample_size,
            cluster_limit=cluster_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider calibration report failed: {exc}') from exc


@router.post('/signal-decider/policy-candidates/generate')
async def generate_signal_decider_policy_candidates(payload: SignalDeciderPolicyCandidateRequest) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_loop_service.generate_policy_candidates,
            market_date_from=payload.market_date_from,
            market_date_to=payload.market_date_to,
            symbols=payload.symbols,
            horizons=payload.horizons,
            primary_horizon_days=payload.primary_horizon_days,
            evaluation_version=payload.evaluation_version,
            min_sample_size=payload.min_sample_size,
            force_recompute=payload.force_recompute,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider policy candidate generation failed: {exc}') from exc


@router.post('/signal-decider/decision-policies/candidates/generate')
async def generate_signal_decider_decision_policy_candidates(payload: SignalDeciderPolicyCandidateRequest) -> dict:
    return await generate_signal_decider_policy_candidates(payload)


@router.get('/signal-decider/policy-candidates')
async def list_signal_decider_policy_candidates(
    status: str | None = Query(default=None),
    evaluation_version: str | None = Query(default=None),
    base_policy_version: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_loop_service.list_policy_candidates,
            status=status,
            evaluation_version=evaluation_version,
            base_policy_version=base_policy_version,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider policy candidate read failed: {exc}') from exc


@router.get('/signal-decider/decision-policies/candidates')
async def list_signal_decider_decision_policy_candidates(
    status: str | None = Query(default=None),
    evaluation_version: str | None = Query(default=None),
    base_policy_version: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    return await list_signal_decider_policy_candidates(
        status=status,
        evaluation_version=evaluation_version,
        base_policy_version=base_policy_version,
        limit=limit,
    )


@router.get('/signal-decider/decision-policies')
async def list_signal_decider_decision_policies(
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_loop_service.list_policy_versions,
            status=status,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider decision policy read failed: {exc}') from exc


@router.get('/signal-decider/decision-policies/{policy_version}')
async def get_signal_decider_decision_policy_detail(policy_version: str) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_loop_service.get_policy_version_detail,
            policy_version=policy_version,
        )
    except ValueError as exc:
        if 'was not found' in str(exc):
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider decision policy detail read failed: {exc}') from exc


@router.post('/signal-decider/decision-policies/{policy_version}/activate')
async def activate_signal_decider_decision_policy(
    policy_version: str,
    payload: SignalDeciderPolicyActivationRequest,
) -> dict:
    try:
        return await run_in_threadpool(
            signal_decider_loop_service.activate_policy_version,
            policy_version=policy_version,
            activated_by=payload.activated_by,
            activation_note=payload.activation_note,
        )
    except ValueError as exc:
        if 'was not found' in str(exc):
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Signal-decider decision policy activation failed: {exc}') from exc


class CoreControlRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    action: str = Field(default='kill', pattern=r'^(kill|restart)$')
    signal_name: str = Field(default='SIGKILL', pattern=r'^SIG(?:TERM|KILL)$')
    port: int = Field(default=8000, ge=1, le=65535)


@router.post('/internal/core/control')
async def control_core_api(payload: CoreControlRequest) -> dict:
    try:
        result = subprocess.run(
            ['lsof', '-ti', f':{payload.port}'],
            check=False,
            capture_output=True,
            text=True,
        )
        pid_text = result.stdout.strip()
        pids = []
        if pid_text:
            for piece in pid_text.splitlines():
                piece = piece.strip()
                if not piece:
                    continue
                pids.append(int(piece))
        sig = signal.SIGKILL if payload.signal_name == 'SIGKILL' else signal.SIGTERM
        current_pid = os.getpid()
        target_pids = [pid for pid in pids if pid != current_pid]
        self_targeted = current_pid in pids
        if payload.action == 'restart':
            plist = Path(__file__).resolve().parents[2] / 'com.kiseyno.kai-trading-core-api.plist'
            uid = str(os.getuid())
            subprocess.run(['launchctl', 'bootout', f'gui/{uid}', str(plist)], check=False, capture_output=True, text=True)
            restarted = subprocess.run(['launchctl', 'bootstrap', f'gui/{uid}', str(plist)], check=True, capture_output=True, text=True)
            logger.warning('core api control endpoint invoked', extra={'action': payload.action, 'port': payload.port, 'signal': payload.signal_name, 'killed_pids': target_pids, 'self_targeted': self_targeted})
            return {
                'status': 'restarted',
                'port': payload.port,
                'signal': payload.signal_name,
                'killed_pids': target_pids,
                'self_targeted': self_targeted,
                'launchctl_stdout': restarted.stdout.strip(),
                'launchctl_stderr': restarted.stderr.strip(),
                'plist_path': str(plist),
            }
        killed = []
        for pid in target_pids:
            os.kill(pid, sig)
            killed.append(pid)
        if self_targeted:
            os.kill(current_pid, sig)
        status = 'killed' if (killed or self_targeted) else 'no_process'
        logger.warning('core api control endpoint invoked', extra={'action': payload.action, 'port': payload.port, 'signal': payload.signal_name, 'killed_pids': killed, 'self_targeted': self_targeted})
        return {
            'status': status,
            'port': payload.port,
            'signal': payload.signal_name,
            'pids': killed,
            'self_targeted': self_targeted,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ProcessLookupError as exc:
        raise HTTPException(status_code=404, detail=f'Process already exited: {exc}') from exc
    except subprocess.CalledProcessError as exc:
        raise HTTPException(status_code=500, detail=f'Core api restart failed: {exc.stderr or exc.stdout or exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Core api control failed: {exc}') from exc
