"""Read stored KRX monitoring and valuation facts for one trading session."""

from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.agent_context import wait_price_observations
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SIGNAL_DECIDER_AGENT_NAME
from app.agent_context.wait_paper_ledger import wait_paper_ledger
from app.db import session_scope
from app.db_models import (
    DailyPriceBar,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
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
    method: Literal['legacy_trigger_to_mark_gross'] = 'legacy_trigger_to_mark_gross'
    deprecated: bool = True
    return_pct: float | None = None
    included_symbol_count: int = 0
    monitored_symbol_count: int = 0


class MonitoringSessionResults(BaseModel):
    market_date: date | None
    previous_trade_date: date | None = None
    session_open_at: datetime | None = None
    session_close_at: datetime | None = None
    items: list[MonitoringSymbolResult]
    hypothetical_return: HypotheticalMonitoringReturn
    paper_trades: list[dict[str, Any]] = Field(default_factory=list)
    paper_summary: dict[str, Any] = Field(default_factory=lambda: summarize_wait_paper_trades([]))
    paper_recording_error: str | None = None


def summarize_wait_paper_trades(
    trades: list[dict[str, Any]], *, recording_error: str | None = None,
) -> dict[str, Any]:
    """Keep simulated realized outcomes, open marks and missing evidence separate."""
    def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
        closed = [row for row in rows if row['status'] in {'target_exited', 'stop_exited'}]
        opened = [row for row in rows if row['status'] == 'open'
                  and row.get('coverage') == 'complete'
                  and (row.get('active_tracking') is not False
                       or row.get('coverage_basis') == 'stored_daily_bounds_no_exit')]
        unresolved = sum(row['status'] == 'open' for row in rows) - len(opened)

        def total(items: list[dict[str, Any]], key: str) -> str | None:
            values = [Decimal(str(row[key])) for row in items if row.get(key) is not None]
            return str(sum(values, Decimal(0))) if values else None

        incomplete = sum(
            row.get('coverage') != 'complete' or row['status'] == 'unavailable' or bool(row.get('missing_inputs'))
            for row in rows
        )
        return {
            'trade_count': len(rows),
            'status_counts': dict(Counter(row['status'] for row in rows)),
            'coverage': 'unavailable' if not rows else 'incomplete' if incomplete or recording_error else 'complete',
            'incomplete_count': incomplete,
            'closed_trade_count': len(closed),
            'realized_pnl_count': sum(row.get('realized_net_pnl_krw') is not None for row in closed),
            'realized_gross_pnl_krw': total(closed, 'realized_gross_pnl_krw'),
            'realized_net_pnl_krw': total(closed, 'realized_net_pnl_krw'),
            'realized_cost_krw': total(closed, 'cost_krw'),
            'closed_quantity': total(closed, 'filled_quantity'),
            'open_trade_count': len(opened),
            'unresolved_open_trade_count': unresolved,
            'open_mtm_count': sum(row.get('unrealized_net_pnl_krw') is not None for row in opened),
            'open_quantity': total(opened, 'filled_quantity'),
            'open_unrealized_gross_pnl_krw': total(opened, 'unrealized_gross_pnl_krw'),
            'open_unrealized_net_pnl_krw': total(opened, 'unrealized_net_pnl_krw'),
            'open_estimated_cost_krw': total(opened, 'cost_krw'),
        }

    return {
        'method': 'wait_paper_ledger',
        'overall': summarize(trades),
        'by_role': {role: summarize([row for row in trades if row['decision_agent_name'] == role])
                    for role in DECISION_SUPPORT_AGENT_NAMES},
    }


def _session_dates() -> set[date]:
    root = wait_price_observations.wait_price_observations.root
    return monitoring_session_history.monitoring_session_history.dates() | wait_paper_ledger.dates() | {
        date.fromisoformat(path.name)
        for path in root.glob('????-??-??') if path.is_dir()
    }


def read_monitoring_session_results(market_date: date | None = None) -> MonitoringSessionResults:
    now = datetime.now(KST)
    error = wait_price_observations.wait_price_observations.paper_recording_error
    paper_error = error.split(':', 1)[0] if error else None
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
        paper_trades = wait_paper_ledger.history(day, as_of=as_of, include_reconstruction=True)
        for trade in paper_trades:
            if is_canonical_krx_short_code(trade['symbol']):
                quotes.setdefault(trade['symbol'], None)
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
                day, quotes=quotes, waits=waits, paper_trades=paper_trades, as_of=as_of, session_closed=now >= closed_at,
                opened_at=opened_at, closed_at=closed_at, paper_error=paper_error,
            )
    return MonitoringSessionResults(
        market_date=market_date, items=[], hypothetical_return=HypotheticalMonitoringReturn(),
        paper_recording_error=paper_error,
    )


def _read_session(
    market_date: date,
    *,
    quotes: dict[str, MonitoringLastQuote | None],
    waits: list[dict[str, Any]],
    paper_trades: list[dict[str, Any]],
    as_of: datetime,
    session_closed: bool,
    opened_at: datetime,
    closed_at: datetime,
    paper_error: str | None,
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
        lots = _verified_wait_lots(
            session, market_date=market_date, previous_date=previous_date,
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
    cost = pnl = Decimal(0)
    included = set()
    for symbol, entry, quantity in lots:
        if (price := evaluation_prices.get(symbol)) is not None:
            cost += entry * quantity
            pnl += (price - entry) * quantity
            included.add(symbol)
    return MonitoringSessionResults(
        market_date=market_date,
        previous_trade_date=previous_date,
        session_open_at=opened_at,
        session_close_at=closed_at,
        items=items,
        paper_trades=paper_trades,
        paper_summary=summarize_wait_paper_trades(paper_trades, recording_error=paper_error),
        paper_recording_error=paper_error,
        hypothetical_return=HypotheticalMonitoringReturn(
            return_pct=float(pnl / cost * 100) if cost else None,
            included_symbol_count=len(included),
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


def _verified_wait_lots(
    session: Session,
    *,
    market_date: date,
    previous_date: date | None,
    symbols: set[str],
    waits: list[dict[str, Any]],
    as_of: datetime,
) -> list[tuple[str, Decimal, Decimal]]:
    if previous_date is None:
        return []
    triggers = {
        (row['decision_snapshot_id'], row['symbol'], row['decision_agent_name']): trigger
        for row in waits
        if row['symbol'] in symbols and row['source_market_date'] == previous_date.isoformat()
        and (trigger := _positive_decimal(row.get('trigger_price'))) is not None
    }
    if not triggers:
        return []
    snapshot = SignalDeciderDecisionSnapshot
    plan = SignalDeciderExecutionPlanItem
    latest = select(
        snapshot.id,
        func.row_number().over(
            partition_by=(snapshot.symbol, snapshot.agent_name),
            order_by=(snapshot.created_at.desc(), snapshot.id.desc()),
        ).label('rank'),
    ).where(
        snapshot.market_date == previous_date,
        snapshot.symbol.in_(symbols),
        snapshot.agent_name.in_(DECISION_SUPPORT_AGENT_NAMES),
        snapshot.created_at <= as_of,
        func.coalesce(snapshot.feedback_received_at, snapshot.created_at) <= as_of,
    ).subquery()
    rows = session.execute(
        select(
            plan.symbol, plan.decision_agent_name, plan.decision_snapshot_id, plan.planned_quantity,
            plan.traceability_payload['raw_action'].as_string().label('raw_action'),
            plan.traceability_payload['trigger_price'].as_string().label('trigger_price'),
            snapshot.next_day_open_action,
            snapshot.response_payload['tactical_action'].as_string().label('tactical_action'),
            snapshot.response_payload['preferred_action'].as_string().label('preferred_action'),
        )
        .join(snapshot, snapshot.id == plan.decision_snapshot_id)
        .join(latest, latest.c.id == snapshot.id)
        .where(
            latest.c.rank == 1,
            snapshot.symbol == plan.symbol,
            snapshot.agent_name == plan.decision_agent_name,
            plan.target_trade_date == market_date,
            plan.source_market_date == previous_date,
            plan.execution_window == 'next_open',
            plan.broker_name == 'kis',
            plan.planned_side == 'buy',
            plan.planned_quantity > 0,
            plan.created_at <= as_of,
            func.coalesce(plan.last_evaluated_at, plan.created_at) <= as_of,
        )
    )
    lots = []
    for row in rows:
        action = row.next_day_open_action
        if row.decision_agent_name != SIGNAL_DECIDER_AGENT_NAME:
            action = row.tactical_action if row.tactical_action is not None else row.preferred_action
        trigger = triggers.get((row.decision_snapshot_id, row.symbol, row.decision_agent_name))
        quantity = _positive_decimal(row.planned_quantity)
        if (
            _is_wait(action) and _is_wait(row.raw_action)
            and (row.preferred_action is None or _is_wait(row.preferred_action))
            and trigger is not None and trigger == _positive_decimal(row.trigger_price)
            and quantity is not None
        ):
            lots.append((row.symbol, trigger, quantity))
    return lots
