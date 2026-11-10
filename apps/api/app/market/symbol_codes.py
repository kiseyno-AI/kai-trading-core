from __future__ import annotations

import re
from typing import Any, Sequence


KRX_SHORT_CODE_PATTERN = re.compile(r'[0-9]{4}[0-9A-Z]{2}\Z')
KRX_EQUITY_MARKETS = frozenset({'KRX', 'KOSPI', 'KOSDAQ', 'KONEX'})
_PREFERRED_NAME_SUFFIX = re.compile(
    r'(?<![0-9])(?:[1-9][0-9]*\s*)?우(?:\s*[BC]|선주|\s*\(전환\))?\s*\Z',
    re.IGNORECASE,
)


def is_krx_preferred_share(symbol: Any, name_ko: Any, market: Any) -> bool:
    code = normalize_krx_short_code(symbol)
    return bool(
        code
        and str(market or '').upper() in KRX_EQUITY_MARKETS
        and (not code.isdecimal() or code[-1] in '579')
        and _PREFERRED_NAME_SUFFIX.search(str(name_ko or ''))
    )


def normalize_krx_short_code(value: Any) -> str | None:
    """Return one canonical six-character KRX short code.

    KRX market payloads contain both legacy numeric codes and newer codes with
    an ASCII letter in the final two positions.  Short numeric inputs retain
    the legacy zero-padding behavior; every other shape fails closed.
    """

    raw = str(value or '').strip().upper()
    if not raw:
        return None
    normalized = raw.zfill(6) if raw.isascii() and raw.isdigit() else raw
    if KRX_SHORT_CODE_PATTERN.fullmatch(normalized) is None:
        return None
    return normalized


def is_canonical_krx_short_code(value: Any) -> bool:
    raw = str(value or '').strip()
    return (
        raw == raw.upper()
        and KRX_SHORT_CODE_PATTERN.fullmatch(raw) is not None
    )


def normalize_symbol_text(value: Any) -> str:
    """Trimmed, upper-cased symbol; all-digit KRX codes zero-padded to six.

    Lenient counterpart of ``normalize_krx_short_code`` for scopes that may
    also carry non-KRX symbols: any other shape passes through unchanged and a
    blank value becomes ``''``.
    """

    text = str(value or '').strip().upper()
    return text.zfill(6) if text.isascii() and text.isdigit() else text


def require_symbol_text(value: Any) -> str:
    symbol = normalize_symbol_text(value)
    if not symbol:
        raise ValueError('symbol must not be blank')
    return symbol


def normalize_symbol_scope(symbols: Sequence[Any] | None) -> list[str] | None:
    """Order-preserving unique symbols; ``None`` (no scope) stays ``None``.

    A blank member is an error: an explicit scope must name real symbols.
    """

    if symbols is None:
        return None
    return list(dict.fromkeys(require_symbol_text(symbol) for symbol in symbols))


def split_symbol_list(symbols: Sequence[Any] | str | None) -> list[str]:
    """Symbols from a string or sequence of comma-separated values.

    Blank pieces are skipped, duplicates removed in first-seen order.
    """

    if symbols is None:
        return []
    values = [symbols] if isinstance(symbols, str) else symbols
    pieces = (
        normalize_symbol_text(piece)
        for value in values
        for piece in str(value or '').split(',')
    )
    return list(dict.fromkeys(piece for piece in pieces if piece))
