from __future__ import annotations

from enum import StrEnum


class KISUSExchange(StrEnum):
    NASDAQ = 'NASDAQ'
    NYSE = 'NYSE'
    NYSE_AMERICAN = 'NYSE_AMERICAN'


_PRICE_CODES = {
    KISUSExchange.NASDAQ: 'NAS',
    KISUSExchange.NYSE: 'NYS',
    KISUSExchange.NYSE_AMERICAN: 'AMS',
}
_ORDER_CODES = {
    KISUSExchange.NASDAQ: 'NASD',
    KISUSExchange.NYSE: 'NYSE',
    KISUSExchange.NYSE_AMERICAN: 'AMEX',
}
_PRODUCT_TYPE_CODES = {
    KISUSExchange.NASDAQ: '512',
    KISUSExchange.NYSE: '513',
    KISUSExchange.NYSE_AMERICAN: '529',
}
_ORDER_CODES_REVERSE = {value: key for key, value in _ORDER_CODES.items()}


def _exchange(value: KISUSExchange | str) -> KISUSExchange:
    try:
        return KISUSExchange(str(value).strip().upper())
    except ValueError as exc:
        raise ValueError(f'Unsupported KIS US exchange: {value}') from exc


def to_kis_price_exchange(value: KISUSExchange | str) -> str:
    return _PRICE_CODES[_exchange(value)]


def to_kis_order_exchange(value: KISUSExchange | str) -> str:
    return _ORDER_CODES[_exchange(value)]


def to_kis_product_type_code(value: KISUSExchange | str) -> str:
    return _PRODUCT_TYPE_CODES[_exchange(value)]


def from_kis_order_exchange(value: str) -> KISUSExchange:
    normalized = value.strip().upper()
    try:
        return _ORDER_CODES_REVERSE[normalized]
    except KeyError as exc:
        raise ValueError(f'Unsupported KIS US order exchange code: {value}') from exc
