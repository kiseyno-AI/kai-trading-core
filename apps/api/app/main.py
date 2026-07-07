import logging
import time
import uuid
from contextlib import asynccontextmanager, suppress
from secrets import compare_digest
import asyncio
from typing import Awaitable, Callable

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes_agent_context import router as agent_context_router
from app.api.routes_crypto import router as crypto_router
from app.api.routes_execution_runtime import router as execution_runtime_router
from app.api.routes_eod import router as eod_router
from app.api.routes_dart import router as dart_router
from app.api.routes_external_news import router as external_news_router
from app.api.routes_kis import router as kis_router
from app.api.routes_macro import router as macro_router
from app.api.routes_fx import router as fx_router
from app.api.routes_favorite_symbols import router as favorite_symbols_router
from app.api.routes_scanner import router as scanner_router
from app.api.routes_market import router as market_router
from app.api.routes_quotes import router as quotes_router
from app.api.routes_symbols import router as symbols_router
from app.api.routes_portfolio import router as portfolio_router
from app.api.routes_trading_accounts import router as trading_accounts_router
from app.api.routes_trading import router as trading_router
from app.agent_context.signal_decider_evaluation_maintenance_service import (
    signal_decider_evaluation_maintenance_service,
)
from app.core.audit_logging import (
    build_api_request_event,
    configure_file_logging,
    log_api_request_event,
    reset_request_id,
    set_request_id,
)
from app.core.settings import settings
from app.db import check_database, init_database
from app.eod.daily_pipeline_state import daily_pipeline_run_state_service
from app.eod.runner import background_runner
from app.market.snapshot import market_snapshot_service
from app.market_data_store import load_tracked_symbols
from app.orders.portfolio_summary_service import portfolio_summary_service
from app.orders.reconciliation import trading_order_reconciliation_service
from app.orders.kis_realtime_order_notices import kis_realtime_order_notice_service
from app.quotes.kis_realtime_prices import kis_realtime_price_service
from app.scanner.state import scanner_state

configure_file_logging()
logger = logging.getLogger(__name__)
STARTUP_MAINTENANCE_TIMEOUT_SECONDS = 30


async def _run_startup_maintenance_step(name: str, factory: Callable[[], Awaitable[object]]) -> None:
    try:
        await asyncio.wait_for(factory(), timeout=STARTUP_MAINTENANCE_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        logger.warning(
            'Startup maintenance step timed out after %ss and will not block API readiness: %s',
            STARTUP_MAINTENANCE_TIMEOUT_SECONDS,
            name,
        )
    except Exception as exc:
        logger.warning('Startup maintenance step failed: %s: %s', name, exc)


async def _run_startup_maintenance() -> None:
    await _run_startup_maintenance_step(
        'trading_order_startup_reconcile',
        lambda: trading_order_reconciliation_service.run_startup_reconcile(),
    )
    await _run_startup_maintenance_step(
        'market_snapshot_startup_refresh',
        lambda: market_snapshot_service.refresh_market_snapshot_if_due(trigger='startup'),
    )
    await _run_startup_maintenance_step(
        'portfolio_summary_startup_refresh',
        lambda: portfolio_summary_service.refresh_portfolio_summary_if_due(trigger='startup', force=True),
    )
    await _run_startup_maintenance_step(
        'signal_decider_evaluation_startup_refresh',
        lambda: asyncio.to_thread(
            signal_decider_evaluation_maintenance_service.refresh_latest_stored_coverage,
            trigger='startup',
        ),
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    startup_maintenance_task: asyncio.Task | None = None
    try:
        await asyncio.to_thread(init_database)
        await asyncio.to_thread(
            daily_pipeline_run_state_service.mark_active_runs_interrupted,
            reason='Core process started while previous daily pipeline run was still marked running.',
        )
        tracked_symbols = await asyncio.to_thread(load_tracked_symbols)
        scanner_state.hydrate(tracked_symbols)
    except Exception as exc:
        logger.warning('Database initialization failed; starting in degraded mode: %s', exc)
    await kis_realtime_order_notice_service.start()
    await kis_realtime_price_service.start()
    if settings.startup_maintenance_enabled:
        startup_maintenance_task = asyncio.create_task(_run_startup_maintenance(), name='startup-maintenance')
    if settings.background_runner_enabled:
        await background_runner.start()
    try:
        yield
    finally:
        if startup_maintenance_task and not startup_maintenance_task.done():
            startup_maintenance_task.cancel()
            with suppress(asyncio.CancelledError):
                await startup_maintenance_task
        await kis_realtime_price_service.stop()
        await kis_realtime_order_notice_service.stop()
        await background_runner.stop()


app = FastAPI(title=settings.app_name, debug=settings.app_debug, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.middleware('http')
async def audit_and_require_upstream_bearer_token(request: Request, call_next):
    request_id = request.headers.get('X-Request-ID') or str(uuid.uuid4())
    request.state.request_id = request_id
    request_token = set_request_id(request_id)
    started_at = time.perf_counter()
    response = None
    error = None

    configured_token = settings.upstream_bearer_token
    try:
        if configured_token and request.method != 'OPTIONS':
            scheme, _, credentials = request.headers.get('Authorization', '').partition(' ')
            if scheme != 'Bearer' or not credentials or not compare_digest(credentials, configured_token):
                response = JSONResponse(
                    status_code=401,
                    content={'detail': 'Unauthorized'},
                    headers={'WWW-Authenticate': 'Bearer'},
                )
            else:
                response = await call_next(request)
        else:
            response = await call_next(request)
        response.headers['X-Request-ID'] = request_id
        return response
    except Exception as exc:
        error = exc
        raise
    finally:
        duration_ms = (time.perf_counter() - started_at) * 1000
        status_code = response.status_code if response is not None else 500
        log_api_request_event(
            build_api_request_event(
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                query_params=request.query_params,
                client_ip=request.client.host if request.client else None,
                user_agent=request.headers.get('user-agent'),
                status_code=status_code,
                duration_ms=duration_ms,
                request_headers=request.headers,
                response_headers=response.headers if response is not None else None,
                error=error,
            )
        )
        reset_request_id(request_token)


app.include_router(kis_router)
app.include_router(macro_router)
app.include_router(fx_router)
app.include_router(scanner_router)
app.include_router(market_router)
app.include_router(quotes_router)
app.include_router(favorite_symbols_router)
app.include_router(symbols_router)
app.include_router(portfolio_router)
app.include_router(trading_router)
app.include_router(trading_accounts_router)
app.include_router(execution_runtime_router)
app.include_router(eod_router)
app.include_router(dart_router)
app.include_router(external_news_router)
app.include_router(agent_context_router)
app.include_router(crypto_router)


@app.get('/health')
async def health() -> dict:
    db_ok, db_error = check_database()
    return {
        'status': 'ok' if db_ok else 'degraded',
        'app': settings.app_name,
        'env': settings.app_env,
        'database': {
            'connected': db_ok,
            'error': db_error,
        },
    }
