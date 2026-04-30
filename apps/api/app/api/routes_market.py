import logging

from fastapi import APIRouter, HTTPException

from app.brokers.kis.client import KISClientError, KISConfigurationError
from app.fx.service import KoreaEximRateNotFoundError
from app.integrations.fx.client import KoreaEximAPIError, KoreaEximConfigurationError
from app.market_data_store import persist_market_status_snapshot
from app.market.clock import get_market_state
from app.market.snapshot import (
    MarketSnapshotConfigurationError,
    MarketSnapshotUnavailableError,
    MarketSnapshotUpstreamError,
    market_snapshot_service,
)
from app.schemas.market import MarketSnapshotResponse

router = APIRouter(prefix='/market', tags=['market'])
logger = logging.getLogger(__name__)


@router.get('/status')
async def market_status() -> dict:
    state = get_market_state()
    try:
        persist_market_status_snapshot(state, source='api')
    except Exception as exc:
        logger.warning('Market status persistence failed: %s', exc)
    return state.__dict__


@router.get('/snapshot', response_model=MarketSnapshotResponse)
async def market_snapshot() -> dict:
    try:
        return await market_snapshot_service.get_market_snapshot()
    except (MarketSnapshotConfigurationError, MarketSnapshotUnavailableError, KISConfigurationError, KoreaEximConfigurationError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except (MarketSnapshotUpstreamError, KISClientError, KoreaEximAPIError, KoreaEximRateNotFoundError) as exc:
        raise HTTPException(status_code=502, detail=f'Market snapshot upstream failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Market snapshot failed: {exc}') from exc
