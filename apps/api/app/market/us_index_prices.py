from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import re
from typing import Any
from zoneinfo import ZoneInfo

from app.core.numbers import finite_float
from app.market.us_session import CompletedUSSession, US_INDEX_HISTORY_RETURNS, prior_completed_us_sessions


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


def validate_kis_us_index_history(
    payload: Mapping[str, Any], *, index_code: str, session: CompletedUSSession,
) -> tuple[USIndexSessionPrices, ...]:
    """Require 22 dated closes for the current pair and 20 prior return windows."""
    pairs = []
    unavailable = None
    for window in (*prior_completed_us_sessions(session), session):
        try:
            pairs.append(validate_kis_us_index_prices(payload, index_code=index_code, session=window))
        except USIndexPricesUnavailable as error:
            # Missing closes must not hide malformed evidence in another required pair.
            unavailable = error
    if unavailable is not None:
        raise unavailable
    return tuple(pairs[:-1])


def validate_us_index_price_history(
    current: USIndexSessionPrices, history: Sequence[USIndexSessionPrices],
) -> None:
    """Validate frozen history without consulting a calendar, provider or database."""
    if len(history) != US_INDEX_HISTORY_RETURNS:
        raise ValueError('US index history requires exactly 20 prior return windows')
    if not isinstance(current, USIndexSessionPrices) or current.index_code not in SUPPORTED_US_INDEX_CODES:
        raise ValueError('US index history requires a supported current index')
    pairs = (*history, current)
    for prices in pairs:
        if not isinstance(prices, USIndexSessionPrices) or prices.index_code != current.index_code:
            raise ValueError('US index history must contain the same index')
        for value in (prices.price, prices.previous_price):
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError('US index history requires positive finite Decimal closes')
        window = prices.session
        if (
            not isinstance(window, CompletedUSSession)
            or type(window.session_date) is not date or type(window.previous_session_date) is not date
        ):
            raise ValueError('US index history requires dated frozen session windows')
        for close_at in (window.previous_close_at, window.close_at):
            if not isinstance(close_at, datetime) or close_at.utcoffset() is None:
                raise ValueError('US index history requires timezone-aware frozen closes')
        if (
            window.previous_session_date >= window.session_date
            or window.previous_close_at >= window.close_at
            or window.close_at.astimezone(ZoneInfo('America/New_York')).date() != window.session_date
            or window.previous_close_at.astimezone(ZoneInfo('America/New_York')).date() != window.previous_session_date
        ):
            raise ValueError('US index history session window mismatch')
    for older, newer in zip(pairs, pairs[1:]):
        if (
            older.session.session_date != newer.session.previous_session_date
            or older.session.close_at != newer.session.previous_close_at
        ):
            raise ValueError('US index history requires consecutive frozen session windows')
        if older.price != newer.previous_price:
            raise ValueError('US index historical price continuity mismatch')
