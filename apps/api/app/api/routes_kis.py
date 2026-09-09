import asyncio
from secrets import compare_digest
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query

from app.brokers.kis.provider import kis_client
from app.brokers.kis.schemas import KISTokenResponse
from app.core.settings import settings
from app.market.kis_intraday_snapshots import kis_intraday_snapshot_service
from app.market.snapshot import market_snapshot_service
from app.orders.portfolio_summary_service import portfolio_summary_service
from app.orders.kis_realtime_order_notices import kis_realtime_order_notice_service
from app.quotes.kis_realtime_prices import kis_realtime_price_service

router = APIRouter(prefix='/broker/kis', tags=['broker:kis'])


@router.get('/health')
async def kis_health() -> dict:
    payload = await kis_client.healthcheck()
    payload['realtime_order_notices'] = kis_realtime_order_notice_service.health_snapshot()
    payload['realtime_prices'] = kis_realtime_price_service.health_snapshot()
    payload['market_snapshot_runtime'] = market_snapshot_service.runtime_status()
    payload['portfolio_summary_runtime'] = portfolio_summary_service.runtime_status()
    payload['intraday_snapshot_runtime'] = kis_intraday_snapshot_service.runtime_status()
    return payload


def _require_token_route_authorization(authorization: str | None) -> None:
    configured_token = settings.upstream_bearer_token
    if not configured_token:
        raise HTTPException(
            status_code=503,
            detail='KIS token endpoint is disabled',
        )
    expected = f'Bearer {configured_token}'.encode('utf-8')
    supplied = (authorization or '').encode('utf-8')
    if not compare_digest(supplied, expected):
        raise HTTPException(
            status_code=401,
            detail='Unauthorized',
            headers={'WWW-Authenticate': 'Bearer'},
        )


def _safe_token_metadata(payload: Any) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError('unexpected token result')
    access_token = payload.get('access_token')
    token_type = payload.get('token_type')
    expires_in = payload.get('expires_in')
    cached = payload.get('cached')
    refresh_deferred = payload.get('refresh_deferred', False)
    defer_reason = payload.get('defer_reason')
    if (
        not isinstance(access_token, str)
        or not access_token
        or token_type != 'Bearer'
        or not isinstance(expires_in, int)
        or isinstance(expires_in, bool)
        or expires_in < 0
        or not isinstance(cached, bool)
        or not isinstance(refresh_deferred, bool)
        or (
            defer_reason is not None
            and defer_reason not in {'minimum_issue_interval', 'peer_refresh'}
        )
        or (refresh_deferred != (defer_reason is not None))
    ):
        raise ValueError('unexpected token result')
    return {
        'access_token_present': True,
        'token_type': token_type,
        'expires_in': expires_in,
        'cached': cached,
        'refresh_deferred': refresh_deferred,
        'defer_reason': defer_reason,
    }


@router.post(
    '/token',
    response_model=KISTokenResponse,
    response_model_exclude_none=True,
)
async def issue_kis_token(
    force_refresh: bool = False,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, object]:
    _require_token_route_authorization(authorization)
    try:
        payload = await kis_client.issue_access_token(force_refresh=force_refresh)
        return _safe_token_metadata(payload)
    except Exception:
        raise HTTPException(status_code=502, detail='KIS token issue failed') from None


@router.get('/quote')
async def get_kis_quote(symbol: str, market_code: str = 'J') -> dict:
    try:
        return await kis_client.fetch_domestic_quote(symbol=symbol, market_code=market_code)
    except Exception:
        raise HTTPException(status_code=502, detail='KIS quote failed') from None


@router.post('/intraday-snapshots/collect')
async def collect_kis_intraday_snapshots(
    symbols: str | None = None,
    market_code: str = 'J',
    max_ranking_pages: int = 2,
    max_news_items: int = 5,
    include_news: bool = True,
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await kis_intraday_snapshot_service.collect_snapshots(
            symbols=parsed_symbols,
            market_code=market_code,
            max_ranking_pages=max_ranking_pages,
            max_news_items=max_news_items,
            include_news=include_news,
        )
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail='Invalid KIS intraday snapshot request',
        ) from None
    except Exception:
        raise HTTPException(
            status_code=502,
            detail='KIS intraday snapshot collection failed',
        ) from None


@router.get('/intraday-snapshots')
async def inspect_kis_intraday_snapshots(
    symbols: str | None = None,
    limit: int = Query(default=100, ge=1, le=5000),
) -> dict:
    try:
        parsed_symbols = [item.strip() for item in symbols.split(',') if item.strip()] if symbols else None
        return await asyncio.to_thread(
            kis_intraday_snapshot_service.inspect,
            symbols=parsed_symbols,
            limit=limit,
        )
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail='Invalid KIS intraday snapshot request',
        ) from None
    except Exception:
        raise HTTPException(
            status_code=500,
            detail='KIS intraday snapshot read failed',
        ) from None
