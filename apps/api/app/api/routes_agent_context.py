from datetime import date
from pathlib import Path
import os
import signal
import subprocess
from typing import Any

import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.agent_context.feedback_service import agent_context_feedback_service
from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.agent_context.progress_service import agent_context_progress_service
from app.agent_context.reuse_service import agent_context_reuse_service
from app.agent_context.read_service import agent_context_read_service
from app.agent_context.session_cleanup_service import agent_feedback_session_cleanup_service
from app.agent_context.retry_service import (
    EXPECTED_ROLES,
    FeedbackRetryCycleConfig,
    agent_feedback_retry_service,
)
from app.agent_context.status_service import agent_context_status_service

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


class AgentFeedbackFollowupRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    market_date: date
    roles: list[str] | None = None
    max_cycles: int = Field(default=3, ge=1, le=20)
    outer_batch_size: int = Field(default=20, ge=1, le=500)
    reuse_existing: bool = True


class AgentFeedbackCleanupRunRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    limit: int = Field(default=100, ge=1, le=1000)


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


@router.post('/feedback/followup')
async def run_agent_feedback_followup(payload: AgentFeedbackFollowupRequest) -> dict:
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
