from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes_eod import router as eod_router
from app.api.routes_kis import router as kis_router
from app.api.routes_macro import router as macro_router
from app.api.routes_fx import router as fx_router
from app.api.routes_scanner import router as scanner_router
from app.api.routes_market import router as market_router
from app.api.routes_quotes import router as quotes_router
from app.api.routes_symbols import router as symbols_router
from app.core.settings import settings
from app.eod.runner import background_runner


@asynccontextmanager
async def lifespan(_: FastAPI):
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
    return {
        'status': 'ok',
        'app': settings.app_name,
        'env': settings.app_env,
    }
