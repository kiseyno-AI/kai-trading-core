from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import re
from typing import Any

from app.core.numbers import finite_float
from app.market.us_session import CompletedUSSession


SUPPORTED_US_INDEX_CODES = ('SPX', 'COMP', 'SOX')


class USIndexPricesUnavailable(LookupError):
    """A successful KIS response lacks one or both required session prices."""


@dataclass(frozen=True)
class USIndexSessionPrices:
    index_code: str
    session: CompletedUSSession
    price: Decimal
    previous_price: Decimal


def _positive_price(value: Any) -> Decimal:
    number = finite_float(value) if isinstance(value, (str, int, float, Decimal)) else None
    if number is None or number <= 0:
        raise ValueError('KIS index price must be positive finite')
    return Decimal(str(value).replace(',', '').strip())


def validate_kis_us_index_prices(
    payload: Mapping[str, Any], *, index_code: str, session: CompletedUSSession,
) -> USIndexSessionPrices:
    """Validate two session prices without certifying provider publication finality."""
    if not isinstance(index_code, str) or index_code.strip().upper() not in SUPPORTED_US_INDEX_CODES:
        raise ValueError('Unsupported US index_code')
    if not isinstance(payload, Mapping) or payload.get('rt_cd') != '0':
        raise ValueError('KIS response must have rt_cd=0')
    normalized_code = index_code.strip().upper()
    dates = (session.session_date.strftime('%Y%m%d'), session.previous_session_date.strftime('%Y%m%d'))
    rows = payload.get('output2')
    if not isinstance(rows, (list, Mapping)):
        raise ValueError('KIS output2 must be a list or object')
    if not rows:
        raise USIndexPricesUnavailable(f'Missing KIS index prices for {", ".join(dates)}')
    if isinstance(rows, Mapping):
        rows = [rows]
    header = payload.get('output1')
    provider_code = header.get('stck_shrn_iscd') if isinstance(header, Mapping) else None
    if not isinstance(provider_code, str) or provider_code.strip().upper() != normalized_code:
        raise ValueError('KIS index identity mismatch or missing')
    prices: dict[str, Decimal] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError('KIS daily row must be an object')
        date_text = row.get('stck_bsop_date')
        if not isinstance(date_text, str) or not re.fullmatch(r'[0-9]{8}', date_text):
            raise ValueError('KIS daily row has invalid stck_bsop_date')
        try:
            datetime.strptime(date_text, '%Y%m%d')
        except ValueError:
            raise ValueError('KIS daily row has invalid stck_bsop_date') from None
        if date_text not in dates:
            continue
        if date_text in prices:
            raise ValueError(f'KIS duplicate required session {date_text}')
        prices[date_text] = _positive_price(row.get('ovrs_nmix_prpr'))
    missing = [value for value in dates if value not in prices]
    if missing:
        raise USIndexPricesUnavailable(f'Missing KIS index prices for {", ".join(missing)}')
    return USIndexSessionPrices(
        index_code=normalized_code,
        session=session,
        price=prices[dates[0]],
        previous_price=prices[dates[1]],
    )
