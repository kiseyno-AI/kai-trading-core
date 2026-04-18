from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.agent_context.feedback_service import agent_context_feedback_service
from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.agent_context.progress_service import agent_context_progress_service
from app.agent_context.reuse_service import agent_context_reuse_service
from app.agent_context.read_service import agent_context_read_service
from app.agent_context.retry_service import (
    EXPECTED_ROLES,
    FeedbackRetryCycleConfig,
    agent_feedback_retry_service,
)
from app.agent_context.status_service import agent_context_status_service

router = APIRouter(prefix='/agent-context', tags=['agent-context'])


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
    symbols: list[str] = Field(..., min_length=1, max_length=20)
    roles: list[str] | None = None
    fs_div: str | None = None
    disclosures_limit: int = Field(default=5, ge=0, le=20)
    reuse_existing: bool = True


class AgentFeedbackRetryCyclesRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date: date
    roles: list[str] | None = None
    max_cycles: int = Field(default=3, ge=1, le=20)
    outer_batch_size: int = Field(default=20, ge=1, le=200)
    reuse_existing: bool = True


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
async def dispatch_symbol_agent_feedback(payload: AgentSymbolFeedbackDispatchRequest) -> dict:
    try:
        return agent_feedback_orchestrator.dispatch_for_symbols(
            market_date=payload.market_date,
            symbols=payload.symbols,
            roles=payload.roles,
            fs_div=payload.fs_div,
            disclosures_limit=payload.disclosures_limit,
            reuse_existing=payload.reuse_existing,
        )
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
) -> dict:
    try:
        return agent_context_progress_service.get_feedback_progress(
            market_date=market_date,
            symbols=symbols,
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
