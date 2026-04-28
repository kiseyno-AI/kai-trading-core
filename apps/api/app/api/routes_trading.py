from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.orders.service import (
    TradingOrderIdempotencyConflictError,
    TradingOrderNotFoundError,
    TradingOrderPendingSubmissionError,
    TradingOrderRejectedError,
    TradingOrderUpstreamError,
    TradingOrderValidationError,
    trading_order_service,
)

router = APIRouter(prefix='/trading/orders', tags=['trading:orders'])


class TradingOrderSubmitRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    client_order_id: str | None = Field(default=None, max_length=128)
    account_reference: str | None = Field(default=None, max_length=128)
    account_number: str = Field(..., min_length=1, max_length=32)
    account_product_code: str = Field(..., min_length=1, max_length=32)
    symbol: str = Field(..., min_length=1, max_length=32)
    quantity: Decimal = Field(..., gt=0)
    price: Decimal | None = None
    order_type: str = Field(default='limit', min_length=1, max_length=32)
    time_in_force: str = Field(default='day', min_length=1, max_length=32)
    exchange_id: str = Field(default='KRX', min_length=1, max_length=16)
    source: str = Field(default='api', min_length=1, max_length=32)
    sell_type: str | None = Field(default=None, max_length=16)
    condition_price: Decimal | None = None
    strategy_context_payload: dict[str, Any] | None = None


class TradingOrderCancelRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    account_reference: str = Field(..., min_length=1, max_length=128)
    quantity: Decimal | None = Field(default=None, gt=0)
    all_remaining: bool = True
    exchange_id: str = Field(default='KRX', min_length=1, max_length=16)


class TradingOrderAmendRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    account_reference: str = Field(..., min_length=1, max_length=128)
    quantity: Decimal | None = Field(default=None, gt=0)
    price: Decimal | None = Field(default=None, ge=0)
    order_type: str | None = Field(default=None, min_length=1, max_length=32)
    exchange_id: str = Field(default='KRX', min_length=1, max_length=16)

    @model_validator(mode='after')
    def require_change(self) -> 'TradingOrderAmendRequest':
        if self.quantity is None and self.price is None and self.order_type is None:
            raise ValueError('At least one of quantity, price, or order_type must be provided')
        return self


def _raise_order_http_error(exc: Exception) -> None:
    if isinstance(exc, TradingOrderValidationError):
        raise HTTPException(status_code=400, detail=jsonable_encoder(exc.payload or {'detail': str(exc)})) from exc
    if isinstance(exc, TradingOrderNotFoundError):
        raise HTTPException(status_code=404, detail=jsonable_encoder(exc.payload or {'detail': str(exc)})) from exc
    if isinstance(exc, (TradingOrderIdempotencyConflictError, TradingOrderRejectedError)):
        raise HTTPException(status_code=409, detail=jsonable_encoder(exc.payload or {'detail': str(exc)})) from exc
    if isinstance(exc, TradingOrderPendingSubmissionError):
        raise HTTPException(status_code=504, detail=jsonable_encoder(exc.payload or {'detail': str(exc)})) from exc
    if isinstance(exc, TradingOrderUpstreamError):
        status_code = 503 if 'configured' in str(exc).lower() else 502
        raise HTTPException(status_code=status_code, detail=jsonable_encoder(exc.payload or {'detail': str(exc)})) from exc


@router.post('/buy')
async def submit_buy_order(payload: TradingOrderSubmitRequest, response: Response) -> dict[str, Any]:
    try:
        result = await trading_order_service.submit_buy_order(**payload.model_dump())
        response.status_code = 201 if result.get('created') else 200
        return result
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order submit failed: {exc}') from exc


@router.post('/sell')
async def submit_sell_order(payload: TradingOrderSubmitRequest, response: Response) -> dict[str, Any]:
    try:
        result = await trading_order_service.submit_sell_order(**payload.model_dump())
        response.status_code = 201 if result.get('created') else 200
        return result
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order submit failed: {exc}') from exc


@router.post('/{client_order_id}/cancel')
async def cancel_order(client_order_id: str, payload: TradingOrderCancelRequest) -> dict[str, Any]:
    try:
        return await trading_order_service.cancel_order(
            client_order_id=client_order_id,
            account_reference=payload.account_reference,
            quantity=payload.quantity,
            all_remaining=payload.all_remaining,
            exchange_id=payload.exchange_id,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order cancel failed: {exc}') from exc


@router.post('/{client_order_id}/amend')
async def amend_order(client_order_id: str, payload: TradingOrderAmendRequest) -> dict[str, Any]:
    try:
        return await trading_order_service.amend_order(
            client_order_id=client_order_id,
            account_reference=payload.account_reference,
            quantity=payload.quantity,
            price=payload.price,
            order_type=payload.order_type,
            exchange_id=payload.exchange_id,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order amend failed: {exc}') from exc


@router.get('/{client_order_id}')
async def get_order(
    client_order_id: str,
    account_reference: str = Query(..., min_length=1, max_length=128),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return await trading_order_service.get_order(
            client_order_id=client_order_id,
            account_reference=account_reference,
            refresh=refresh,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order read failed: {exc}') from exc


@router.get('')
async def list_orders(
    account_reference: str | None = Query(default=None, min_length=1, max_length=128),
    symbol: str | None = Query(default=None, min_length=1, max_length=32),
    status: list[str] | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    try:
        return trading_order_service.list_orders(
            account_reference=account_reference,
            symbol=symbol,
            statuses=status,
            limit=limit,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order list failed: {exc}') from exc
