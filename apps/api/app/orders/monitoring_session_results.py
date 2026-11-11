"""Read stored KRX monitoring and valuation facts for one trading session."""

from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.agent_context import wait_price_observations
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SIGNAL_DECIDER_AGENT_NAME
from app.db import session_scope
from app.db_models import (
    DailyPriceBar,
    SignalDeciderDecisionSnapshot,
    Symbol,
)
from app.market.clock import KST
from app.market.session import (
    KRX_EXCHANGE_CALENDAR,
    get_krx_exchange_calendar_day,
    latest_krx_exchange_calendar_trading_date,
)
from app.market.symbol_codes import is_canonical_krx_short_code
from app.quotes import monitoring_session_history
from app.quotes.monitoring_session_history import MonitoringLastQuote
from app.quotes.kis_realtime_prices import kis_realtime_price_service


class MonitoringSymbolResult(BaseModel):
    symbol: str
    price_status: Literal['close', 'last_quote', 'unavailable']
    close_price: int | None = None
    close_trade_date: date | None = None
    previous_close_price: int | None = None
    previous_close_trade_date: date | None = None
    last_price: int | float | None = None
    last_price_at: datetime | None = None
    change: int | float | None = None
    change_pct: float | None = None


class HypotheticalMonitoringReturn(BaseModel):
    return_pct: float | None = None
    included_symbol_count: int = 0
    monitored_symbol_count: int = 0


class MonitoringSessionResults(BaseModel):
    market_date: date | None
    previous_trade_date: date | None = None
    items: list[MonitoringSymbolResult]
    hypothetical_return: HypotheticalMonitoringReturn


def _session_dates() -> set[date]:
    root = wait_price_observations.wait_price_observations.root
    return monitoring_session_history.monitoring_session_history.dates() | {
        date.fromisoformat(path.name) for path in root.glob('????-??-??') if path.is_dir()
    }


def read_monitoring_session_results(market_date: date | None = None) -> MonitoringSessionResults:
    now = datetime.now(KST)
    candidates = [market_date] if market_date is not None else sorted(_session_dates(), reverse=True)
    for day in candidates:
        if day > now.date() or not get_krx_exchange_calendar_day(day).is_open:
            continue
        opened_at = KRX_EXCHANGE_CALENDAR.session_open(day.isoformat()).to_pydatetime()
        closed_at = KRX_EXCHANGE_CALENDAR.session_close(day.isoformat()).to_pydatetime()
        if now < opened_at:
            continue
        as_of = min(now, closed_at)
        quotes = {
            symbol: quote if quote is not None and opened_at <= quote.as_of <= as_of else None
            for symbol, quote in monitoring_session_history.monitoring_session_history.read(day).items()
        }
        waits = wait_price_observations.wait_price_observations.history(day, as_of=as_of)
        for row in waits:
            symbol = row['symbol']
            if not is_canonical_krx_short_code(symbol):
                continue
            started_at = datetime.fromisoformat(row['tracking_started_at'])
            touch_at = datetime.fromisoformat(row['price_as_of']) if row.get('price_as_of') else None
            if touch_at is not None and not opened_at <= touch_at <= as_of:
                touch_at = None
            if touch_at is None and not opened_at <= started_at <= as_of:
                continue
            last = quotes.setdefault(symbol, None)
            if touch_at is not None and (last is None or touch_at > last.as_of):
                if (price := _positive_decimal(row.get('observed_price'))) is not None:
                    quotes[symbol] = MonitoringLastQuote(price, touch_at)
        if quotes:
            return _read_session(
                day, quotes=quotes, waits=waits, as_of=as_of, session_closed=now >= closed_at,
            )
    return MonitoringSessionResults(
        market_date=market_date, items=[], hypothetical_return=HypotheticalMonitoringReturn(),
    )


def _read_session(
    market_date: date,
    *,
    quotes: dict[str, MonitoringLastQuote | None],
    waits: list[dict[str, Any]],
    as_of: datetime,
    session_closed: bool,
) -> MonitoringSessionResults:
    previous_date = latest_krx_exchange_calendar_trading_date(market_date - timedelta(days=1))
    dates = {previous_date} if previous_date is not None else set()
    if session_closed:
        dates.add(market_date)
    with session_scope() as session:
        closes = {
            (symbol, trade_date): close_price
            for symbol, trade_date, close_price in session.execute(
                select(Symbol.symbol, DailyPriceBar.trade_date, DailyPriceBar.close_price)
                .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                .where(
                    Symbol.symbol.in_(quotes),
                    DailyPriceBar.trade_date.in_(dates),
                    DailyPriceBar.close_price > 0,
                )
            )
        }
        entries = _verified_wait_entries(
            session, market_date=market_date,
            symbols=set(quotes), waits=waits, as_of=as_of,
        )

    items = []
    evaluation_prices: dict[str, Decimal] = {}
    for symbol, quote in sorted(quotes.items()):
        close = closes.get((symbol, market_date))
        previous = closes.get((symbol, previous_date))
        last_quote = quote if close is None else None
        last_price = last_quote.price if last_quote is not None else None
        evaluation_price = Decimal(close) if close is not None else last_price
        if evaluation_price is not None:
            evaluation_prices[symbol] = evaluation_price
        change = change_pct = None
        if evaluation_price is not None and previous is not None:
            difference = evaluation_price - previous
            change = int(difference) if close is not None else float(difference)
            change_pct = float(difference / previous * 100)
        items.append(MonitoringSymbolResult(
            symbol=symbol,
            price_status='close' if close is not None else 'last_quote' if last_price is not None else 'unavailable',
            close_price=close,
            close_trade_date=market_date if close is not None else None,
            previous_close_price=previous,
            previous_close_trade_date=previous_date if previous is not None else None,
            last_price=float(last_price) if last_price is not None else None,
            last_price_at=last_quote.as_of if last_quote is not None else None,
            change=change,
            change_pct=change_pct,
        ))
    if not session_closed:
        opened_at = KRX_EXCHANGE_CALENDAR.session_open(market_date.isoformat()).to_pydatetime()
        closed_at = KRX_EXCHANGE_CALENDAR.session_close(market_date.isoformat()).to_pydatetime()
        valuation_cutoff = min(datetime.now(KST), closed_at)
        for symbol in entries:
            sample = kis_realtime_price_service.price_fields_for_symbol(symbol)
            if (
                sample.get('price_source') not in {'kis_realtime_ws', 'kis_rest_multi_quote'}
                or sample.get('price_status') != 'available'
                or sample.get('display_price_available') is not True
                or (price := _positive_decimal(sample.get('current_price'))) is None
            ):
                continue
            try:
                sampled_at = datetime.fromisoformat(str(sample.get('price_as_of')))
            except ValueError:
                continue
            if (
                sampled_at.tzinfo is None or sampled_at > valuation_cutoff
                or sampled_at.astimezone(KST).date() != market_date
                or sampled_at < opened_at
            ):
                continue
            recorded = quotes.get(symbol)
            if recorded is None or sampled_at >= recorded.as_of:
                evaluation_prices[symbol] = price
    returns = [
        (evaluation_prices[symbol] / entry - 1) * 100
        for symbol, entry in entries.items() if symbol in evaluation_prices
    ]
    return MonitoringSessionResults(
        market_date=market_date,
        previous_trade_date=previous_date,
        items=items,
        hypothetical_return=HypotheticalMonitoringReturn(
            return_pct=float(sum(returns) / len(returns)) if returns else None,
            included_symbol_count=len(returns),
            monitored_symbol_count=len(quotes),
        ),
    )


def _positive_decimal(value: Any) -> Decimal | None:
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() and number > 0 else None


def _is_wait(value: str | None) -> bool:
    return isinstance(value, str) and value.strip().lower() == 'wait'


def _verified_wait_entries(
    session: Session,
    *,
    market_date: date,
    symbols: set[str],
    waits: list[dict[str, Any]],
    as_of: datetime,
) -> dict[str, Decimal]:
    triggers = {}
    for row in waits:
        try:
            source_date = date.fromisoformat(row['source_market_date'])
        except (TypeError, ValueError):
            continue
        trigger = _positive_decimal(row.get('trigger_price'))
        if row['symbol'] in symbols and source_date < market_date and trigger is not None:
            triggers[(row['decision_snapshot_id'], row['symbol'], row['decision_agent_name'], source_date)] = trigger
    if not triggers:
        return {}
    snapshot = SignalDeciderDecisionSnapshot
    latest = select(
        snapshot.id,
        func.row_number().over(
            partition_by=(snapshot.symbol, snapshot.agent_name),
            order_by=(snapshot.market_date.desc(), snapshot.created_at.desc(), snapshot.id.desc()),
        ).label('rank'),
    ).where(
        snapshot.market_date >= min(key[3] for key in triggers),
        snapshot.market_date < market_date,
        snapshot.symbol.in_(symbols),
        snapshot.agent_name.in_(DECISION_SUPPORT_AGENT_NAMES),
        snapshot.created_at <= as_of,
        func.coalesce(snapshot.feedback_received_at, snapshot.created_at) <= as_of,
    ).subquery()
    rows = session.execute(
        select(
            snapshot.id, snapshot.symbol, snapshot.agent_name, snapshot.market_date, snapshot.next_day_open_action,
            snapshot.response_payload['tactical_action'].as_string().label('tactical_action'),
            snapshot.response_payload['preferred_action'].as_string().label('preferred_action'),
        )
        .join(latest, latest.c.id == snapshot.id)
        .where(latest.c.rank == 1)
    ).all()
    entries = {}
    for row in sorted(rows, key=lambda row: row.agent_name != SIGNAL_DECIDER_AGENT_NAME):
        action = row.next_day_open_action
        if row.agent_name != SIGNAL_DECIDER_AGENT_NAME:
            action = row.tactical_action if row.tactical_action is not None else row.preferred_action
        trigger = triggers.get((row.id, row.symbol, row.agent_name, row.market_date))
        if (
            _is_wait(action) and (row.preferred_action is None or _is_wait(row.preferred_action))
            and trigger is not None
        ):
            entries.setdefault(row.symbol, trigger)
    return entries
