from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class QuoteRequest(BaseModel):
    market: str = 'KRX'
    symbol: str


class FXRateResponse(BaseModel):
    source: str
    base_currency: str
    quote_currency: str
    rate: float | None = None
    date: str | None = None
    raw: dict | None = None


class MacroSeriesResponse(BaseModel):
    source: str
    series_id: str
    items: list[dict]


class MarketSnapshotMarket(BaseModel):
    label: str
    index_code: str
    value: float | None = None
    change: float | None = None
    change_percent: float | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    volume: int | None = None
    trading_value: int | None = Field(
        default=None,
        description='Cumulative trading value from KIS `acml_tr_pbmn`, expressed in million KRW.',
    )
    trading_value_unit: Literal['million_krw'] = Field(
        default='million_krw',
        description='Unit for `trading_value`.',
    )
    advancers: int | None = None
    unchanged: int | None = None
    decliners: int | None = None
    foreign_net_quantity: int | None = None
    foreign_net_value: int | None = Field(
        default=None,
        description='Foreign net trading value in KRW, normalized from KIS `frgn_ntby_tr_pbmn`.',
    )
    institution_net_quantity: int | None = None
    institution_net_value: int | None = Field(
        default=None,
        description='Institutional net trading value in KRW, normalized from KIS `orgn_ntby_tr_pbmn`.',
    )
    investor_flow_value_unit: Literal['krw'] = Field(
        default='krw',
        description='Unit for `foreign_net_value` and `institution_net_value`.',
    )


class MarketSnapshotFX(BaseModel):
    pair: str
    rate: float | None = None
    effective_date: str | None = None
    fallback_applied: bool
    status: Literal['ready', 'stale', 'missing'] = 'ready'
    source: Literal['live', 'cache', 'unavailable'] = 'live'
    stale_reason: str | None = None
    detail: str | None = None
    captured_at: datetime | None = None


class MarketSnapshotFreshness(BaseModel):
    served_from_cache: bool = True
    refresh_interval_seconds: int
    last_refresh_attempted_at: datetime | None = None
    last_refresh_succeeded_at: datetime | None = None
    next_refresh_due_at: datetime | None = None
    last_refresh_status: Literal['never', 'success', 'degraded', 'error', 'unknown'] = 'never'
    last_refresh_error: str | None = None


class MarketSnapshotResponse(BaseModel):
    as_of: datetime
    status: Literal['ready', 'partial', 'stale'] = 'ready'
    missing_fields: list[str] = Field(default_factory=list)
    stale_reason: str | None = None
    markets: dict[str, MarketSnapshotMarket]
    usd_krw: MarketSnapshotFX
    freshness: MarketSnapshotFreshness
