from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from app.core.settings import settings
from app.integrations.upbit.client import UpbitQuotationClient

PROVIDER = 'upbit'
VENUE = 'UPBIT'
ASSET_CLASS = 'crypto'
INSTRUMENT_TYPE = 'spot_crypto_pair'
QUOTE_CURRENCY = 'KRW'
USAGE_SCOPE = 'paper_research_only'
ALLOWED_MINUTE_UNITS = {1, 3, 5, 10, 15, 30, 60, 240}

TickerInterval = Literal['day', 'minute']


class CryptoMarketDataValidationError(ValueError):
    pass


def configured_crypto_markets() -> list[str]:
    markets = [
        item.strip().upper()
        for item in settings.upbit_market_whitelist.split(',')
        if item.strip()
    ]
    return markets or ['KRW-BTC', 'KRW-ETH']


def _split_market(market: str) -> tuple[str, str]:
    quote_currency, separator, base_currency = market.partition('-')
    if not separator or not quote_currency or not base_currency:
        raise CryptoMarketDataValidationError('Upbit market codes must use QUOTE-BASE format, for example KRW-BTC')
    return quote_currency, base_currency


def _to_float(value: Any) -> float | None:
    if value in (None, ''):
        return None
    return float(value)


def _to_int(value: Any) -> int | None:
    if value in (None, ''):
        return None
    return int(value)


def _iso_from_millis(value: Any) -> str | None:
    timestamp_ms = _to_int(value)
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def _source(endpoint: str, docs_url: str) -> dict[str, Any]:
    return {
        'provider': PROVIDER,
        'venue': VENUE,
        'api_family': 'quotation',
        'endpoint': endpoint,
        'docs_url': docs_url,
        'auth_required': False,
        'read_only': True,
        'order_submission_enabled': False,
    }


def _instrument_payload(
    market: str,
    *,
    korean_name: str | None = None,
    english_name: str | None = None,
    market_warning: str | None = None,
) -> dict[str, Any]:
    quote_currency, base_currency = _split_market(market)
    return {
        'market': market,
        'symbol': market,
        'asset_class': ASSET_CLASS,
        'instrument_type': INSTRUMENT_TYPE,
        'provider': PROVIDER,
        'venue': VENUE,
        'quote_currency': quote_currency,
        'base_currency': base_currency,
        'korean_name': korean_name,
        'english_name': english_name,
        'market_warning': market_warning,
        'trading_enabled_in_core': False,
        'usage_scope': USAGE_SCOPE,
    }


class UpbitCryptoMarketDataService:
    def __init__(self, client: UpbitQuotationClient | None = None) -> None:
        self.client = client or UpbitQuotationClient()

    def _whitelist(self) -> list[str]:
        return configured_crypto_markets()

    def _select_markets(self, markets: str | list[str] | None = None) -> list[str]:
        whitelist = self._whitelist()
        if markets is None:
            requested = whitelist
        elif isinstance(markets, str):
            requested = [item.strip().upper() for item in markets.split(',') if item.strip()]
        else:
            requested = [item.strip().upper() for item in markets if item.strip()]

        if not requested:
            raise CryptoMarketDataValidationError('At least one Upbit market must be requested')

        unsupported = [market for market in requested if market not in whitelist]
        if unsupported:
            allowed = ', '.join(whitelist)
            raise CryptoMarketDataValidationError(f'Unsupported crypto market(s): {", ".join(unsupported)}. Allowed: {allowed}')

        return requested

    async def list_markets(self) -> dict[str, Any]:
        whitelist = self._whitelist()
        rows = await self.client.fetch_markets(is_details=True)
        rows_by_market = {str(row.get('market') or '').upper(): row for row in rows if isinstance(row, dict)}
        items = []
        for market in whitelist:
            row = rows_by_market.get(market, {})
            items.append(
                _instrument_payload(
                    market,
                    korean_name=row.get('korean_name'),
                    english_name=row.get('english_name'),
                    market_warning=row.get('market_warning'),
                )
            )

        return {
            'provider': PROVIDER,
            'asset_class': ASSET_CLASS,
            'venue': VENUE,
            'quote_currency': QUOTE_CURRENCY,
            'usage_scope': USAGE_SCOPE,
            'trading_enabled_in_core': False,
            'whitelist': whitelist,
            'count': len(items),
            'source': _source(
                '/v1/market/all',
                'https://docs.upbit.com/kr/reference/list-trading-pairs',
            ),
            'items': items,
        }

    async def get_tickers(self, *, markets: str | list[str] | None = None) -> dict[str, Any]:
        selected_markets = self._select_markets(markets)
        rows = await self.client.fetch_tickers(markets=selected_markets)
        items = [self._normalize_ticker(row) for row in rows]
        items.sort(key=lambda item: selected_markets.index(item['market']) if item['market'] in selected_markets else len(selected_markets))

        return {
            'provider': PROVIDER,
            'asset_class': ASSET_CLASS,
            'venue': VENUE,
            'usage_scope': USAGE_SCOPE,
            'trading_enabled_in_core': False,
            'requested_markets': selected_markets,
            'count': len(items),
            'source': _source(
                '/v1/ticker',
                'https://docs.upbit.com/kr/reference/list-tickers',
            ),
            'items': items,
        }

    async def get_candles(
        self,
        *,
        market: str,
        interval: TickerInterval = 'day',
        unit: int = 1,
        count: int = 30,
        to: str | None = None,
    ) -> dict[str, Any]:
        selected_market = self._select_markets([market])[0]
        if interval == 'minute':
            if unit not in ALLOWED_MINUTE_UNITS:
                allowed_units = ', '.join(str(value) for value in sorted(ALLOWED_MINUTE_UNITS))
                raise CryptoMarketDataValidationError(f'unit must be one of: {allowed_units}')
            rows = await self.client.fetch_minute_candles(market=selected_market, unit=unit, count=count, to=to)
            endpoint = f'/v1/candles/minutes/{unit}'
            docs_url = 'https://docs.upbit.com/kr/reference/list-candles-minutes'
        elif interval == 'day':
            rows = await self.client.fetch_day_candles(market=selected_market, count=count, to=to)
            endpoint = '/v1/candles/days'
            docs_url = 'https://docs.upbit.com/kr/reference/list-candles-days'
        else:
            raise CryptoMarketDataValidationError('interval must be day or minute')

        quote_currency, base_currency = _split_market(selected_market)
        return {
            'provider': PROVIDER,
            'asset_class': ASSET_CLASS,
            'venue': VENUE,
            'usage_scope': USAGE_SCOPE,
            'trading_enabled_in_core': False,
            'market': selected_market,
            'symbol': selected_market,
            'base_currency': base_currency,
            'quote_currency': quote_currency,
            'instrument_type': INSTRUMENT_TYPE,
            'interval': interval,
            'unit': unit if interval == 'minute' else None,
            'requested_count': count,
            'to': to,
            'count': len(rows),
            'source': _source(endpoint, docs_url),
            'items': [self._normalize_candle(row, interval=interval, unit=unit if interval == 'minute' else None) for row in rows],
        }

    @staticmethod
    def _normalize_ticker(row: dict[str, Any]) -> dict[str, Any]:
        market = str(row.get('market') or '').upper()
        quote_currency, _ = _split_market(market)
        return {
            **_instrument_payload(market),
            'currency': quote_currency,
            'price': _to_float(row.get('trade_price')),
            'open': _to_float(row.get('opening_price')),
            'high': _to_float(row.get('high_price')),
            'low': _to_float(row.get('low_price')),
            'previous_close': _to_float(row.get('prev_closing_price')),
            'change': row.get('change'),
            'change_price': _to_float(row.get('change_price')),
            'change_rate': _to_float(row.get('change_rate')),
            'signed_change_price': _to_float(row.get('signed_change_price')),
            'signed_change_rate': _to_float(row.get('signed_change_rate')),
            'trade_volume': _to_float(row.get('trade_volume')),
            'acc_trade_price': _to_float(row.get('acc_trade_price')),
            'acc_trade_price_24h': _to_float(row.get('acc_trade_price_24h')),
            'acc_trade_volume': _to_float(row.get('acc_trade_volume')),
            'acc_trade_volume_24h': _to_float(row.get('acc_trade_volume_24h')),
            'last_trade_at': _iso_from_millis(row.get('trade_timestamp')),
            'captured_at': _iso_from_millis(row.get('timestamp')),
            'provider_timestamp_ms': _to_int(row.get('timestamp')),
            'source': _source(
                '/v1/ticker',
                'https://docs.upbit.com/kr/reference/list-tickers',
            ),
        }

    @staticmethod
    def _normalize_candle(
        row: dict[str, Any],
        *,
        interval: TickerInterval,
        unit: int | None,
    ) -> dict[str, Any]:
        market = str(row.get('market') or '').upper()
        quote_currency, base_currency = _split_market(market)
        return {
            'market': market,
            'symbol': market,
            'asset_class': ASSET_CLASS,
            'instrument_type': INSTRUMENT_TYPE,
            'provider': PROVIDER,
            'venue': VENUE,
            'quote_currency': quote_currency,
            'base_currency': base_currency,
            'interval': interval,
            'unit': unit,
            'candle_date_time_utc': row.get('candle_date_time_utc'),
            'candle_date_time_kst': row.get('candle_date_time_kst'),
            'open': _to_float(row.get('opening_price')),
            'high': _to_float(row.get('high_price')),
            'low': _to_float(row.get('low_price')),
            'close': _to_float(row.get('trade_price')),
            'acc_trade_price': _to_float(row.get('candle_acc_trade_price')),
            'acc_trade_volume': _to_float(row.get('candle_acc_trade_volume')),
            'captured_at': _iso_from_millis(row.get('timestamp')),
            'provider_timestamp_ms': _to_int(row.get('timestamp')),
        }


crypto_market_data_service = UpbitCryptoMarketDataService()
