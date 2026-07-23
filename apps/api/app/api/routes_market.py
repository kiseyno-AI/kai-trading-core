import asyncio
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException

from app.brokers.kis.client import KISClientError, KISConfigurationError
from app.fx.service import KoreaEximRateNotFoundError
from app.integrations.fx.client import KoreaEximAPIError, KoreaEximConfigurationError
from app.market_data_store import persist_market_status_snapshot
from app.market.clock import KST, MarketState, async_get_market_state
from app.market.snapshot import (
    MarketSnapshotConfigurationError,
    MarketSnapshotUnavailableError,
    MarketSnapshotUpstreamError,
    market_snapshot_service,
)
from app.schemas.market import MarketSnapshotResponse

router = APIRouter(prefix='/market', tags=['market'])
logger = logging.getLogger(__name__)
MARKET_STATUS_TIMEOUT_SECONDS = 1.5


@router.get('/status')
async def market_status() -> dict:
    try:
        state = await asyncio.wait_for(async_get_market_state(), timeout=MARKET_STATUS_TIMEOUT_SECONDS)
    except Exception as exc:
        logger.warning('Market status state lookup failed; serving fallback: %s', exc)
        state = _build_market_status_fallback(exc)
        return state.__dict__

    try:
        persist_market_status_snapshot(state, source='api')
    except Exception as exc:
        logger.warning('Market status persistence failed: %s', exc)
    return state.__dict__


def _build_market_status_fallback(exc: Exception) -> MarketState:
    now = datetime.now(KST)
    session_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    session_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    is_weekday = now.weekday() < 5

    if not is_weekday:
        state = 'WEEKEND'
        is_open = False
        market_session = 'weekend'
        session_open_at = None
        session_close_at = None
        latest_valid_trading_date = _previous_weekday(now).date().isoformat()
    elif now < session_open:
        state = 'PREOPEN'
        is_open = False
        market_session = 'closed'
        session_open_at = session_open.isoformat()
        session_close_at = session_close.isoformat()
        latest_valid_trading_date = _previous_weekday(now).date().isoformat()
    elif now <= session_close:
        state = 'OPEN'
        is_open = True
        market_session = 'open'
        session_open_at = session_open.isoformat()
        session_close_at = session_close.isoformat()
        latest_valid_trading_date = now.date().isoformat()
    else:
        state = 'CLOSED'
        is_open = False
        market_session = 'closed'
        session_open_at = session_open.isoformat()
        session_close_at = session_close.isoformat()
        latest_valid_trading_date = now.date().isoformat()

    return MarketState(
        market='KRX',
        state=state,
        is_trading_day=is_weekday,
        is_open=is_open,
        server_time=now.isoformat(),
        session_open_at=session_open_at,
        session_close_at=session_close_at,
        reason='market_status_fallback',
        current_calendar_date=now.date().isoformat(),
        latest_valid_trading_date=latest_valid_trading_date,
        market_session=market_session,
        calendar_source='local_weekday_fallback',
        calendar_source_status='fallback',
        calendar_source_error=str(exc),
    )


def _previous_weekday(now: datetime) -> datetime:
    candidate = now - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


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
