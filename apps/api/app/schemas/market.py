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
