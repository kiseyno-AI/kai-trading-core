from fastapi import FastAPI

from app.api.routes_kis import router as kis_router
from app.api.routes_macro import router as macro_router
from app.api.routes_fx import router as fx_router
from app.api.routes_scanner import router as scanner_router
from app.core.settings import settings

app = FastAPI(title=settings.app_name, debug=settings.app_debug)
app.include_router(kis_router)
app.include_router(macro_router)
app.include_router(fx_router)
app.include_router(scanner_router)


@app.get('/health')
async def health() -> dict:
    return {
        'status': 'ok',
        'app': settings.app_name,
        'env': settings.app_env,
    }
