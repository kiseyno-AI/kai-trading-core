from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from app.crypto.market_data import CryptoMarketDataValidationError, crypto_market_data_service
from app.integrations.upbit.client import UpbitQuotationAPIError

router = APIRouter(prefix='/market-data/crypto', tags=['market-data:crypto'])


@router.get('/markets')
async def list_crypto_markets() -> dict:
    try:
        return await crypto_market_data_service.list_markets()
    except UpbitQuotationAPIError as exc:
        raise HTTPException(status_code=502, detail=f'Upbit market data request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Crypto market metadata failed: {exc}') from exc


@router.get('/tickers')
async def get_crypto_tickers(
    markets: str | None = Query(default=None, description='Comma-separated whitelisted Upbit market codes. Defaults to the crypto whitelist.'),
) -> dict:
    try:
        return await crypto_market_data_service.get_tickers(markets=markets)
    except CryptoMarketDataValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except UpbitQuotationAPIError as exc:
        raise HTTPException(status_code=502, detail=f'Upbit market data request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Crypto ticker read failed: {exc}') from exc


@router.get('/candles')
async def get_crypto_candles(
    market: str = Query(..., description='Whitelisted Upbit market code such as KRW-BTC.'),
    interval: Literal['day', 'minute'] = Query(default='day'),
    unit: int = Query(default=1, description='Minute candle unit. Used only when interval=minute.'),
    count: int = Query(default=30, ge=1, le=200),
    to: str | None = Query(default=None, description='Optional Upbit candle end time. Format follows Upbit quotation API.'),
) -> dict:
    try:
        return await crypto_market_data_service.get_candles(
            market=market,
            interval=interval,
            unit=unit,
            count=count,
            to=to,
        )
    except CryptoMarketDataValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except UpbitQuotationAPIError as exc:
        raise HTTPException(status_code=502, detail=f'Upbit market data request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Crypto candle read failed: {exc}') from exc
