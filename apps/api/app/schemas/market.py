from datetime import datetime

from pydantic import BaseModel


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
    trading_value: int | None = None
    advancers: int | None = None
    unchanged: int | None = None
    decliners: int | None = None
    foreign_net_quantity: int | None = None
    foreign_net_value: int | None = None
    institution_net_quantity: int | None = None
    institution_net_value: int | None = None


class MarketSnapshotFX(BaseModel):
    pair: str
    rate: float | None = None
    effective_date: str | None = None
    fallback_applied: bool


class MarketSnapshotResponse(BaseModel):
    as_of: datetime
    markets: dict[str, MarketSnapshotMarket]
    usd_krw: MarketSnapshotFX
