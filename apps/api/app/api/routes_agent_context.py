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

from app.agent_context.eod_summaries_service import agent_context_eod_summaries_service
from app.agent_context.eligibility import (
    AGENT_FEEDBACK_ELIGIBILITY_FILTERS,
    AGENT_FEEDBACK_ELIGIBILITY_RULE,
    resolve_agent_feedback_scope,
)
from app.agent_context.feedback_service import agent_context_feedback_service
from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.agent_context.progress_service import agent_context_progress_service
from app.agent_context.reuse_service import agent_context_reuse_service
from app.agent_context.read_service import agent_context_read_service
from app.agent_context.run_state_service import agent_context_feedback_run_state_service
from app.agent_context.session_cleanup_service import agent_feedback_session_cleanup_service
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


class AgentFeedbackRetryCyclesRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date: date
    symbols: list[str] | None = Field(default=None, min_length=1, max_length=500)
    roles: list[str] | None = None
    max_cycles: int = Field(default=3, ge=1, le=20)
    outer_batch_size: int = Field(default=20, ge=1, le=200)
    reuse_existing: bool = True


class AgentFeedbackFollowupRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date: date
    symbols: list[str] | None = Field(default=None, min_length=1, max_length=500)
    roles: list[str] | None = None
    max_cycles: int = Field(default=3, ge=1, le=20)
    outer_batch_size: int = Field(default=20, ge=1, le=500)
    reuse_existing: bool = True


class AgentFeedbackCleanupRunRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    limit: int = Field(default=100, ge=1, le=1000)


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
            )

        request_run_id = f"symbol-run-{payload.market_date.isoformat()}-{uuid.uuid4().hex[:10]}"
        requested_roles = list(payload.roles or EXPECTED_ROLES)
        requested_symbols = list(dict.fromkeys(payload.symbols))
        started_at = datetime.now(timezone.utc)
        request_payload = {
            'market_date': payload.market_date.isoformat(),
            'symbols': requested_symbols,
            'requested_symbols': requested_symbols,
            'request_run_id': request_run_id,
            'roles': requested_roles,
            'fs_div': payload.fs_div,
            'disclosures_limit': payload.disclosures_limit,
            'reuse_existing': payload.reuse_existing,
            'scope': resolve_agent_feedback_scope(requested_symbols=requested_symbols),
            'eligibility_rule': AGENT_FEEDBACK_ELIGIBILITY_RULE,
            'eligibility_filters': list(AGENT_FEEDBACK_ELIGIBILITY_FILTERS),
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
