from __future__ import annotations

import re
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db_models import Symbol


_KRX_EQUITY_SYMBOL_PATTERN = re.compile(r'[0-9]{6}\Z')
_PREFERRED_SYMBOL_LAST_DIGITS = frozenset({'5', '7', '9'})
_PREFERRED_NAME_SUFFIX_PATTERN = re.compile(
    r'(?<![0-9])(?:[123]\s*)?우\s*B?\s*\Z',
    re.IGNORECASE,
)


def resolve_preferred_fundamentals_issuer_symbols(
    session: Session,
    symbols: Sequence[str],
) -> dict[str, str]:
    """Return strict preferred-share to common-share fundamentals aliases.

    The mapping is intentionally based only on current symbol-master records and
    the KRX code convention. It does not infer issuers from similar company
    names.
    """

    requested_symbols = list(dict.fromkeys(str(symbol) for symbol in symbols))
    if not requested_symbols:
        return {}

    requested_rows = session.execute(
        select(
            Symbol.symbol,
            Symbol.name_ko,
            Symbol.market,
            Symbol.is_active,
        ).where(Symbol.symbol.in_(requested_symbols))
    ).mappings().all()

    candidate_by_preferred_symbol: dict[str, str] = {}
    requested_market_by_symbol: dict[str, str] = {}
    for row in requested_rows:
        symbol = str(row['symbol'])
        name_ko = str(row['name_ko'] or '')
        if not bool(row['is_active']):
            continue
        if _KRX_EQUITY_SYMBOL_PATTERN.fullmatch(symbol) is None:
            continue
        if symbol[-1] not in _PREFERRED_SYMBOL_LAST_DIGITS:
            continue
        if _PREFERRED_NAME_SUFFIX_PATTERN.search(name_ko) is None:
            continue
        candidate_by_preferred_symbol[symbol] = f'{symbol[:5]}0'
        requested_market_by_symbol[symbol] = str(row['market'])

    if not candidate_by_preferred_symbol:
        return {}

    issuer_rows = session.execute(
        select(Symbol.symbol, Symbol.market, Symbol.is_active).where(
            Symbol.symbol.in_(list(dict.fromkeys(candidate_by_preferred_symbol.values())))
        )
    ).mappings().all()
    issuer_market_by_symbol = {
        str(row['symbol']): str(row['market'])
        for row in issuer_rows
        if bool(row['is_active'])
    }

    return {
        preferred_symbol: issuer_symbol
        for preferred_symbol, issuer_symbol in candidate_by_preferred_symbol.items()
        if issuer_market_by_symbol.get(issuer_symbol)
        == requested_market_by_symbol[preferred_symbol]
    }
