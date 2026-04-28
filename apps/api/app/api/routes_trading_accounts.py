from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder

from app.orders.account_inquiry_service import (
    TradingAccountInquiryUpstreamError,
    TradingAccountInquiryValidationError,
    trading_account_inquiry_service,
)

router = APIRouter(prefix='/trading/accounts', tags=['trading:accounts'])


def _raise_account_http_error(exc: Exception) -> None:
    if isinstance(exc, TradingAccountInquiryValidationError):
        raise HTTPException(status_code=400, detail=jsonable_encoder(exc.payload or {'detail': str(exc)})) from exc
    if isinstance(exc, TradingAccountInquiryUpstreamError):
        payload = exc.payload or {'detail': str(exc)}
        status_code = 503 if payload.get('configured') is False or 'configured' in str(exc).lower() else 502
        raise HTTPException(status_code=status_code, detail=jsonable_encoder(payload)) from exc


@router.get('/{account_reference}/summary')
async def get_account_summary(account_reference: str) -> dict[str, Any]:
    try:
        return await trading_account_inquiry_service.get_account_summary(account_reference=account_reference)
    except Exception as exc:
        _raise_account_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading account summary failed: {exc}') from exc


@router.get('/{account_reference}/positions')
async def get_account_positions(
    account_reference: str,
    symbol: str | None = Query(default=None, min_length=1, max_length=32),
    include_zero: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return await trading_account_inquiry_service.get_positions(
            account_reference=account_reference,
            symbol=symbol,
            include_zero=include_zero,
        )
    except Exception as exc:
        _raise_account_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading account positions read failed: {exc}') from exc


@router.get('/{account_reference}/buying-power')
async def get_account_buying_power(
    account_reference: str,
    symbol: str = Query(..., min_length=1, max_length=32),
    price: Decimal = Query(..., gt=0),
    order_type: str = Query(..., min_length=1, max_length=32),
    include_cma: bool = Query(default=False),
    include_overseas: bool = Query(default=False),
    side: str = Query(default='buy', min_length=1, max_length=16),
) -> dict[str, Any]:
    try:
        return await trading_account_inquiry_service.get_buying_power(
            account_reference=account_reference,
            symbol=symbol,
            price=price,
            order_type=order_type,
            include_cma=include_cma,
            include_overseas=include_overseas,
            side=side,
        )
    except Exception as exc:
        _raise_account_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading account buying power read failed: {exc}') from exc
