import logging
from contextlib import asynccontextmanager
from secrets import compare_digest

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes_eod import router as eod_router
from app.api.routes_kis import router as kis_router
from app.api.routes_macro import router as macro_router
from app.api.routes_fx import router as fx_router
from app.api.routes_scanner import router as scanner_router
from app.api.routes_market import router as market_router
from app.api.routes_quotes import router as quotes_router
from app.api.routes_symbols import router as symbols_router
from app.core.settings import settings
from app.db import check_database, init_database
from app.eod.runner import background_runner
from app.market_data_store import load_tracked_symbols
from app.scanner.state import scanner_state

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        init_database()
        tracked_symbols = load_tracked_symbols()
        if tracked_symbols:
            scanner_state.hydrate(tracked_symbols)
    except Exception as exc:
        logger.warning('Database initialization failed; starting in degraded mode: %s', exc)
    await background_runner.start()
    try:
        yield
    finally:
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
async def require_upstream_bearer_token(request: Request, call_next):
    configured_token = settings.upstream_bearer_token
    if not configured_token or request.method == 'OPTIONS':
        return await call_next(request)

    scheme, _, credentials = request.headers.get('Authorization', '').partition(' ')
    if scheme != 'Bearer' or not credentials or not compare_digest(credentials, configured_token):
        return JSONResponse(
            status_code=401,
            content={'detail': 'Unauthorized'},
            headers={'WWW-Authenticate': 'Bearer'},
        )

    return await call_next(request)


app.include_router(kis_router)
app.include_router(macro_router)
app.include_router(fx_router)
app.include_router(scanner_router)
app.include_router(market_router)
app.include_router(quotes_router)
app.include_router(symbols_router)
app.include_router(eod_router)


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
