from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.orders.intraday_auto_sell import signal_decider_intraday_auto_sell_service
from app.orders.monitoring import trading_order_monitoring_service
from app.orders.next_open_execution import signal_decider_next_open_execution_service
from app.orders.reconciliation import trading_order_reconciliation_service
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


class TradingOrdersReconcileRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    account_reference: str | None = Field(default=None, min_length=1, max_length=128)
    symbol: str | None = Field(default=None, min_length=1, max_length=32)
    limit: int = Field(default=100, ge=1, le=500)
    reason: str | None = Field(default=None, max_length=256)
    include_orders: bool = True


class TradingOrderReconcileSingleRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    account_reference: str = Field(..., min_length=1, max_length=128)
    reason: str | None = Field(default=None, max_length=256)


class TradingNextOpenExecutionRunRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    target_trade_date: date | None = None
    dry_run: bool | None = None
    force: bool = False


class TradingIntradayAutoSellRunRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    account_reference: str | None = Field(default=None, min_length=1, max_length=128)
    symbol: str | None = Field(default=None, min_length=1, max_length=32)
    dry_run: bool | None = None
    force: bool = False


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


@router.post('/reconcile')
async def reconcile_orders(payload: TradingOrdersReconcileRequest) -> dict[str, Any]:
    try:
        return await trading_order_reconciliation_service.reconcile_recent_orders(
            account_reference=payload.account_reference,
            symbol=payload.symbol,
            limit=payload.limit,
            trigger='manual',
            reason=payload.reason,
            include_orders=payload.include_orders,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order reconcile failed: {exc}') from exc


@router.get('/inspect')
async def inspect_reconcile_state(
    account_reference: str | None = Query(default=None, min_length=1, max_length=128),
    symbol: str | None = Query(default=None, min_length=1, max_length=32),
    limit: int = Query(default=50, ge=1, le=200),
    stale_after_minutes: int | None = Query(default=None, ge=1, le=1440),
) -> dict[str, Any]:
    try:
        return trading_order_reconciliation_service.inspect_orders(
            account_reference=account_reference,
            symbol=symbol,
            limit=limit,
            stale_after_minutes=stale_after_minutes,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order inspect failed: {exc}') from exc


@router.get('/lifecycle')
async def get_lifecycle_view(
    account_reference: str | None = Query(default=None, min_length=1, max_length=128),
    symbol: str | None = Query(default=None, min_length=1, max_length=32),
    limit: int = Query(default=25, ge=1, le=200),
    stale_after_minutes: int | None = Query(default=None, ge=1, le=1440),
) -> dict[str, Any]:
    try:
        return trading_order_monitoring_service.lifecycle_view(
            account_reference=account_reference,
            symbol=symbol,
            limit=limit,
            stale_after_minutes=stale_after_minutes,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order lifecycle read failed: {exc}') from exc


@router.get('/mismatches')
async def get_order_mismatches(
    account_reference: str | None = Query(default=None, min_length=1, max_length=128),
    symbol: str | None = Query(default=None, min_length=1, max_length=32),
    limit: int = Query(default=50, ge=1, le=200),
    stale_after_minutes: int | None = Query(default=None, ge=1, le=1440),
) -> dict[str, Any]:
    try:
        return trading_order_monitoring_service.mismatches_view(
            account_reference=account_reference,
            symbol=symbol,
            limit=limit,
            stale_after_minutes=stale_after_minutes,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order mismatch read failed: {exc}') from exc


@router.get('/stale-pending')
async def get_stale_pending_orders(
    account_reference: str | None = Query(default=None, min_length=1, max_length=128),
    symbol: str | None = Query(default=None, min_length=1, max_length=32),
    limit: int = Query(default=50, ge=1, le=200),
    stale_after_minutes: int | None = Query(default=None, ge=1, le=1440),
) -> dict[str, Any]:
    try:
        return trading_order_monitoring_service.stale_pending_view(
            account_reference=account_reference,
            symbol=symbol,
            limit=limit,
            stale_after_minutes=stale_after_minutes,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order stale pending read failed: {exc}') from exc


@router.get('/realtime-health')
async def get_realtime_health() -> dict[str, Any]:
    try:
        return trading_order_monitoring_service.realtime_health()
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order realtime health read failed: {exc}') from exc


@router.get('/next-open-execution')
async def list_next_open_execution_plan(
    target_trade_date: date | None = Query(default=None),
    account_reference: str | None = Query(default=None, min_length=1, max_length=128),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return await run_in_threadpool(
            signal_decider_next_open_execution_service.list_plan_items,
            target_trade_date=target_trade_date,
            account_reference=account_reference,
            refresh=refresh,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Next-open execution plan read failed: {exc}') from exc


@router.post('/next-open-execution/run-if-due')
async def run_next_open_execution_if_due(payload: TradingNextOpenExecutionRunRequest) -> dict[str, Any]:
    try:
        return await signal_decider_next_open_execution_service.run_market_open_execution_if_due(
            target_trade_date=payload.target_trade_date,
            dry_run=payload.dry_run,
            force=payload.force,
            trigger='api',
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Next-open execution submit failed: {exc}') from exc


@router.get('/intraday-auto-sell')
async def list_intraday_auto_sell_watch(
    account_reference: str | None = Query(default=None, min_length=1, max_length=128),
    symbol: str | None = Query(default=None, min_length=1, max_length=32),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return await signal_decider_intraday_auto_sell_service.list_watch_items(
            account_reference=account_reference,
            symbol=symbol,
            refresh=refresh,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Intraday auto-sell diagnostics read failed: {exc}') from exc


@router.post('/intraday-auto-sell/run-if-due')
async def run_intraday_auto_sell_if_due(payload: TradingIntradayAutoSellRunRequest) -> dict[str, Any]:
    try:
        return await signal_decider_intraday_auto_sell_service.run_intraday_auto_sell_if_due(
            account_reference=payload.account_reference,
            symbol=payload.symbol,
            dry_run=payload.dry_run,
            force=payload.force,
            trigger='api',
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Intraday auto-sell execution failed: {exc}') from exc


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


@router.post('/{client_order_id}/reconcile')
async def reconcile_order(client_order_id: str, payload: TradingOrderReconcileSingleRequest) -> dict[str, Any]:
    try:
        return await trading_order_reconciliation_service.reconcile_single_order(
            account_reference=payload.account_reference,
            client_order_id=client_order_id,
            trigger='manual_single',
            reason=payload.reason,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order reconcile failed: {exc}') from exc


@router.get('/{client_order_id}/timeline')
async def get_order_timeline(
    client_order_id: str,
    account_reference: str = Query(..., min_length=1, max_length=128),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return await trading_order_monitoring_service.order_timeline_view(
            client_order_id=client_order_id,
            account_reference=account_reference,
            refresh=refresh,
        )
    except Exception as exc:
        _raise_order_http_error(exc)
        raise HTTPException(status_code=500, detail=f'Trading order timeline read failed: {exc}') from exc


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
