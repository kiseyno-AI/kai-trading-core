"""Point-in-time evidence for the independent KRX Daily Review."""

from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from math import isfinite
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, load_only

from app.agent_context.decider_replay_service import actual_execution_outcome_exclusion_reason
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
from app.agent_context.wait_price_observations import wait_price_observations
from app.agent_context.wait_paper_ledger import wait_paper_ledger
from app.db import session_scope, session_timeout_budget
from app.db_models import (
    DailyMarketRiskSnapshot, DailyPriceBar, DailyTechnicalSnapshot, DartDisclosureEvent,
    SignalDeciderActualExecutionOutcome, SignalDeciderDecisionSnapshot, Symbol,
)
from app.external_news.store import list_external_news_items
from app.market.clock import KST
from app.market.session import get_krx_exchange_calendar_day
from app.market_data_store import load_current_market_snapshot_context
from app.market_news.service import market_news_service
from app.orders.monitoring_session_results import summarize_wait_paper_trades
from app.side_agents.market_inputs import _num, aggregate_breadth
from app.side_agents.schemas import MARKET_AGENT
from app.side_agents.store import SideAgentStore


MAX_EXAMPLES_PER_ROLE = 6
MAX_MARKET_HEADLINES = 6
RECENT_SESSION_COUNT = 5


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _object(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _fields(value: Any, *keys: str) -> dict:
    source = _object(value)
    return {
        key: item[:240] if isinstance(item, str) else item
        for key in keys if isinstance(item := source.get(key), (str, int, float))
        and (not isinstance(item, float) or isfinite(item))
    }


def _items(value: Any, *keys: str) -> list[dict]:
    return [fields for item in value[:2] if (fields := _fields(item, *keys))] if isinstance(value, list) else []


def _number(value: Any) -> float | None:
    number = _num(value)
    return number if number is not None and isfinite(number) else None


def _has_price_range(value: Any) -> bool:
    prices = _object(value)
    high, low = _number(prices.get('high_price', prices.get('high'))), _number(prices.get('low_price', prices.get('low')))
    return high is not None and low is not None and high >= low > 0


def _role_watches(watches: list[dict], role: str) -> list[dict]:
    return [watch for watch in watches if watch['decision_agent_name'] == role]


def _has_watch_event(watch: dict) -> bool:
    return watch['status'] == 'touched' or bool(watch.get('ask_touch') or watch.get('ask_entry'))


def _watch_counts(watches: list[dict]) -> dict:
    return {
        'observation_count': len(watches),
        'watch_event_count': sum(_has_watch_event(watch) for watch in watches),
        'trade_touch_count': sum(watch['status'] == 'touched' for watch in watches),
        'ask_touch_count': sum(bool(watch.get('ask_touch')) for watch in watches),
        'ask_entry_count': sum(bool(watch.get('ask_entry')) for watch in watches),
    }


def _watch_registration(watches: list[dict], session_open: datetime) -> dict:
    starts = [datetime.fromisoformat(watch['tracking_started_at']).astimezone(KST) for watch in watches]
    return {
        'session_open_at': session_open.isoformat(),
        'first_registered_at': min(starts).isoformat() if starts else None,
        'last_registered_at': max(starts).isoformat() if starts else None,
        'registered_after_open': sum(start > session_open for start in starts),
    }


def _source_status(current: bool, earlier_context: bool) -> str:
    if current:
        return 'available'
    return 'context_only' if earlier_context else 'unavailable'


def _execution_status(entry_fills: int, records: int) -> str:
    if entry_fills:
        return 'available'
    return 'records_only' if records else 'unavailable'


def _recent_sessions(day: date, cutoff: datetime) -> list[dict]:
    history = []
    for offset in range(1, 15):
        prior_day = day - timedelta(days=offset)
        if not get_krx_exchange_calendar_day(prior_day).is_open:
            continue
        watches = wait_price_observations.history(prior_day, as_of=cutoff)
        roles = {role: _watch_counts(_role_watches(watches, role)) for role in DECISION_SUPPORT_AGENT_NAMES}
        history.append({
            'market_date': prior_day.isoformat(),
            'roles': roles,
            'status': 'available' if any(role['watch_event_count'] for role in roles.values()) else 'unavailable',
        })
        if len(history) == RECENT_SESSION_COUNT:
            break
    return history


def _matches_watch(watch: dict, *, role: str, symbol: str, source_day: date) -> bool:
    return (role, symbol, source_day.isoformat()) == (
        watch['decision_agent_name'], watch['symbol'], watch['source_market_date'],
    )


def _stored_execution(db: Session, market_date: date, cutoff: datetime, watches: list[dict]) -> dict[int, list[dict]]:
    by_id = {watch['decision_snapshot_id']: watch for watch in watches}
    if not by_id:
        return {}
    outcome = SignalDeciderActualExecutionOutcome
    snapshot = SignalDeciderDecisionSnapshot
    rows = db.execute(select(outcome, snapshot.agent_name, snapshot.symbol, snapshot.market_date).join(
        snapshot, (snapshot.id == outcome.decision_snapshot_id) & (snapshot.symbol == outcome.symbol)
        & (snapshot.market_date == outcome.source_market_date),
    ).options(load_only(
        outcome.id, outcome.decision_snapshot_id, outcome.lifecycle_status,
        outcome.entry_filled_at, outcome.exit_filled_at, outcome.entry_filled_quantity,
        outcome.exit_filled_quantity, outcome.closed_quantity, outcome.open_quantity,
        outcome.entry_average_fill_price, outcome.exit_average_fill_price,
        outcome.entry_notional_krw, outcome.exit_notional_krw, outcome.realized_pnl_krw,
        outcome.realized_return_pct,
    )).where(
        outcome.decision_snapshot_id.in_(by_id),
        outcome.target_trade_date == market_date,
        outcome.created_at <= cutoff,
        outcome.updated_at <= cutoff,
        outcome.computed_at <= cutoff,
        snapshot.created_at <= cutoff,
        func.coalesce(snapshot.feedback_received_at, snapshot.created_at) <= cutoff,
    ).order_by(outcome.id))
    results: dict[int, list[dict]] = {}
    for row, role, symbol, source_day in rows:
        if not _matches_watch(by_id[row.decision_snapshot_id], role=role, symbol=symbol, source_day=source_day):
            continue
        entry_at = _aware(row.entry_filled_at) if row.entry_filled_at else None
        exit_at = _aware(row.exit_filled_at) if row.exit_filled_at else None
        # The row aggregates fills across the lifecycle, so later fills invalidate its historical state.
        if (entry_at and entry_at > cutoff) or (exit_at and exit_at > cutoff):
            continue
        quantity = _number(row.entry_filled_quantity)
        has_entry = bool(
            row.lifecycle_status != 'dry_run' and entry_at and entry_at.astimezone(KST).date() == market_date
            and quantity is not None and quantity > 0
        )
        has_closed_trade = bool(
            has_entry and entry_at and exit_at and exit_at >= entry_at and actual_execution_outcome_exclusion_reason(row) is None
        )
        realized_return = _number(row.realized_return_pct) if has_closed_trade else None
        results.setdefault(row.decision_snapshot_id, []).append({
            'lifecycle_status': row.lifecycle_status,
            'entry_fill_observed': has_entry,
            'closed_trade_observed': has_closed_trade,
            # The execution ledger stores a fractional return despite its column name.
            'gross_realized_return_pct': round(realized_return * 100, 4) if realized_return is not None else None,
        })
    return results


def _market_breadth(db: Session, market_date: date, cutoff: datetime) -> list[dict]:
    risk = DailyMarketRiskSnapshot
    raw = risk.snapshot_payload['raw']
    rows = db.execute(select(
        raw['rprs_mrkt_kor_name'].as_string().label('market'),
        raw['bstp_kor_isnm'].as_string().label('sector'),
        raw['prdy_ctrt'].as_string().label('ret'),
        raw['acml_tr_pbmn'].as_string().label('turnover'),
        risk.is_investment_caution.label('caution'),
        risk.is_trade_halted.label('halted'),
        risk.is_vi_triggered.label('vi'),
    ).where(risk.market_date == market_date, risk.captured_at <= cutoff, risk.updated_at <= cutoff)).mappings()
    return [{**row, 'ret': _number(row['ret']), 'turnover': _number(row['turnover'])} for row in rows]


def _market_analyst_context(store: SideAgentStore, day: date, cutoff: datetime) -> dict | None:
    record = _object(store.load_result(MARKET_AGENT, day.isoformat()))
    output = _object(record.get('output'))
    try:
        completed_at = datetime.fromisoformat(record.get('completed_at', ''))
    except (TypeError, ValueError):
        return None
    if completed_at.tzinfo is None or completed_at > cutoff or output.get('market_date') != day.isoformat():
        return None
    return {
        'completed_at': completed_at.isoformat(),
        **_fields(output, 'market_date', 'regime', 'posture_hint', 'volatility', 'confidence', 'summary_ko'),
        'trend': _fields(output.get('trend'), 'kospi', 'kosdaq'),
        'drivers': _items(output.get('drivers'), 'factor', 'direction', 'evidence'),
    }


def _timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return parsed.astimezone(KST) if parsed.tzinfo is not None else None


def _dashboard_leg_known(leg: dict, day: date, cached_at: datetime) -> bool:
    observed = _timestamp(leg.get('provider_observed_at'))
    collected = _timestamp(leg.get('collected_at'))
    return bool(observed and collected and observed.date() == day and observed <= collected <= cached_at)


def _dashboard_market_context(day: date, cutoff: datetime) -> dict | None:
    snapshot = _object(load_current_market_snapshot_context())
    cached_at = _timestamp(snapshot.get('cached_at'))
    if cached_at is None or cached_at > cutoff:
        return None

    markets = {}
    available = False
    stored_markets = _object(_object(snapshot.get('payload')).get('markets'))
    provenance_fields = ('status', 'observation_source', 'provider_observed_at', 'collected_at', 'error')
    for key in ('kospi', 'kosdaq'):
        row = _object(stored_markets.get(key))
        index = _object(row.get('index_leg'))
        flow = _object(row.get('flow_leg'))
        legs = {}
        if (
            index.get('status') in ('ready', 'unverified') and index.get('observation_payload_match') is True
            and _dashboard_leg_known(index, day, cached_at) and (_number(row.get('value')) or 0) > 0
        ):
            legs['index'] = {
                **_fields(index, *provenance_fields),
                **_fields(row, 'value', 'change_percent', 'open', 'high', 'low'),
            }
            available |= index['status'] == 'ready'
        flows = _fields(row, 'foreign_net_value', 'institution_net_value')
        if (
            flows and flow.get('status') in ('ready', 'cached') and flow.get('current_session') is True
            and _dashboard_leg_known(flow, day, cached_at)
        ):
            legs['flow'] = {
                **_fields(flow, *provenance_fields, 'source'), **flows,
                **_fields(row, 'investor_flow_value_unit'),
            }
            available = True
        if legs:
            markets[key] = legs
    if not markets:
        return None
    return {
        **_fields(snapshot, 'source', 'snapshot_status', 'cached_at'),
        'status': _source_status(available, True),
        'markets': markets,
    }


def _decision_contexts(db: Session, watches: list[dict], cutoff: datetime) -> dict[int, dict]:
    if not watches:
        return {}
    by_id = {watch['decision_snapshot_id']: watch for watch in watches}
    snapshot = SignalDeciderDecisionSnapshot
    rows = db.execute(select(
        snapshot.id, snapshot.agent_name, snapshot.symbol, snapshot.market_date,
        snapshot.policy_version, snapshot.confidence_score_10, snapshot.thesis_summary,
        snapshot.suggested_holding_period, snapshot.target_price, snapshot.stop_loss_price,
        snapshot.invalidation_conditions_payload, snapshot.request_payload,
    ).where(
        snapshot.id.in_(by_id), snapshot.created_at <= cutoff,
        func.coalesce(snapshot.feedback_received_at, snapshot.created_at) <= cutoff,
    ))
    results = {}
    for row in rows:
        if not _matches_watch(by_id[row.id], role=row.agent_name, symbol=row.symbol, source_day=row.market_date):
            continue
        request = _object(row.request_payload)
        context = _object(request.get('symbol_day_context'))
        fundamentals = _object(context.get('fundamentals'))
        latest = _object(fundamentals.get('latest_snapshot'))
        news = _object(context.get('recent_external_news'))
        dart = _object(context.get('recent_dart_events'))
        factors = _object(_object(request.get('factor_summary')).get('factor_scores'))
        company = _fields(context.get('company'), 'name_ko', 'market', 'instrument_type', 'is_preferred_share')
        highlights = _object(latest.get('highlights'))
        invalidations = row.invalidation_conditions_payload
        results[row.id] = {
            'policy_version': row.policy_version,
            'confidence_score_10': _number(row.confidence_score_10),
            'thesis_summary': row.thesis_summary[:360],
            'suggested_holding_period': row.suggested_holding_period,
            'target_price': row.target_price,
            'stop_loss_price': row.stop_loss_price,
            'invalidation_conditions': [
                condition[:240] for condition in invalidations[:5] if isinstance(condition, str)
            ] if isinstance(invalidations, list) else [],
            'market_regime': _fields(request.get('market_regime_context'), 'market_date', 'status', 'posture', 'market_regime', 'reason_summary'),
            'factor_scores': {
                key: _fields(value, 'score', 'status', 'market_date') for key, value in list(factors.items())[:10]
            },
            'technical': _fields(context.get('technical_snapshot'), 'trend_state', 'ma_alignment', 'breakout_20d', 'return_5d', 'return_20d', 'volume_ratio_20d', 'atr_pct_14'),
            'company': {
                **company,
                'sector': _fields(context.get('sector_peer_context'), 'sector_name').get('sector_name'),
                **_fields(fundamentals.get('history_summary'), 'latest_receipt_date', 'latest_period_end_date'),
                'fundamentals_freshness': _fields(fundamentals.get('freshness_provenance'), 'freshness_status', 'as_of_market_date'),
                'financial_highlights': {
                    key: amounts for key in ('revenue', 'operating_profit', 'net_income', 'operating_cash_flow', 'free_cash_flow')
                    if (amounts := _fields(highlights.get(key), 'current_amount', 'current_cumulative_amount', 'prior_amount', 'currency'))
                    and (amounts.get('current_amount') is not None or amounts.get('current_cumulative_amount') is not None)
                },
                'filings': _items(dart.get('items'), 'report_name', 'report_nm', 'receipt_date', 'filing_category'),
            },
            'news_at_decision': {
                **_fields(news, 'status', 'reason', 'item_count', 'knowledge_cutoff_at'),
                'items': _items(news.get('items'), 'title', 'url', 'published_at', 'materiality', 'event_type', 'access_status'),
            },
        }
    return results


def _session_prices(db: Session, day: date, cutoff: datetime, symbols: list[str]) -> dict[str, dict]:
    if not symbols:
        return {}
    bar = DailyPriceBar
    rows = db.execute(select(
        Symbol.symbol, bar.open_price, bar.high_price, bar.low_price, bar.close_price,
        bar.volume, bar.source, bar.captured_at,
    ).join(Symbol, Symbol.id == bar.symbol_id).where(
        Symbol.symbol.in_(symbols), bar.trade_date == day, bar.captured_at <= cutoff, bar.volume > 0,
        bar.close_price > 0,
    )).mappings()
    return {row['symbol']: {**row, 'captured_at': _aware(row['captured_at']).isoformat()} for row in rows}


def _recent_technicals(db: Session, day: date, cutoff: datetime, symbols: list[str]) -> dict[str, list[dict]]:
    if not symbols:
        return {}
    technical = DailyTechnicalSnapshot
    rows = db.execute(select(
        Symbol.symbol, technical.market_date, technical.captured_at, technical.source,
        technical.trend_state, technical.ma_alignment, technical.breakout_20d, technical.breakdown_20d,
        technical.return_5d, technical.return_20d, technical.volume_ratio_20d, technical.atr_pct_14,
    ).join(Symbol, Symbol.id == technical.symbol_id).where(
        Symbol.symbol.in_(symbols), technical.market_date.between(day - timedelta(days=14), day),
        technical.created_at <= cutoff, technical.captured_at <= cutoff, technical.updated_at <= cutoff,
    ).order_by(Symbol.symbol, technical.market_date.desc())).mappings()
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        if len(by_symbol[row['symbol']]) < 3:
            by_symbol[row['symbol']].append({
                **_fields(dict(row), 'source', 'trend_state', 'ma_alignment', 'breakout_20d', 'breakdown_20d'),
                'market_date': row['market_date'].isoformat(),
                'captured_at': _aware(row['captured_at']).isoformat(),
                **{key: _number(row[key]) for key in ('return_5d', 'return_20d', 'volume_ratio_20d', 'atr_pct_14')},
            })
    return dict(by_symbol)


def _session_filings(db: Session, day: date, cutoff: datetime, symbols: list[str]) -> dict[str, list[dict]]:
    if not symbols:
        return {}
    filing = DartDisclosureEvent
    rows = db.execute(select(
        filing.stock_code, filing.rcept_no, filing.report_nm, filing.filing_category, filing.created_at,
    ).where(
        filing.stock_code.in_(symbols), filing.receipt_date == day,
        filing.created_at <= cutoff, filing.updated_at <= cutoff,
    ).order_by(filing.stock_code, filing.rcept_no.desc()))
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        if len(by_symbol[row.stock_code]) < 2:
            by_symbol[row.stock_code].append({
                'receipt_date': day.isoformat(), 'receipt_number': row.rcept_no,
                'report_name': row.report_nm[:240], 'filing_category': row.filing_category,
                'first_seen_at': _aware(row.created_at).isoformat(),
            })
    return dict(by_symbol)


def _session_news(day: date, cutoff: datetime, symbols: list[str]) -> dict[str, list[dict]]:
    if not symbols:
        return {}
    news_cutoff = min(cutoff, datetime.combine(day + timedelta(days=1), time.min, KST)).astimezone(timezone.utc)
    with session_timeout_budget(statement_timeout_ms=10_000):
        rows = list_external_news_items(
            symbols=symbols, limit=4 * len(symbols),
            published_since=datetime.combine(day, time.min, KST).astimezone(timezone.utc),
            published_before=news_cutoff, collected_before=news_cutoff,
        )
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_symbol[row['symbol']].append(row)
    return {
        symbol: _items(items, 'id', 'title', 'url', 'published_at', 'collected_at', 'access_status', 'materiality', 'event_type')
        for symbol, items in by_symbol.items()
    }


def _market_headlines(day: date, cutoff: datetime) -> list[dict]:
    start = datetime.combine(day, time.min, KST)
    with session_timeout_budget(statement_timeout_ms=10_000):
        rows = market_news_service.list_items(
            topic_keys=['kr_kospi', 'kr_kosdaq', 'kr_foreign_flows'], start=start.astimezone(timezone.utc),
            end=min(cutoff, start + timedelta(days=1)).astimezone(timezone.utc),
            as_of=cutoff.astimezone(timezone.utc), persisted_before=cutoff.astimezone(timezone.utc),
            limit=2 * MAX_MARKET_HEADLINES,
        )
    headlines = {}
    for row in rows:
        headlines.setdefault(row['url'], _fields(row, 'topic_key', 'title', 'url', 'published_at', 'first_seen_at'))
        if len(headlines) == MAX_MARKET_HEADLINES:
            break
    return list(headlines.values())


def load_daily_review_evidence(market_date: date, *, observed_at: datetime, store: SideAgentStore) -> dict[str, Any]:
    """Read stored KRX session evidence. An absent observation does not prove a price was missed."""
    if observed_at.tzinfo is None:
        raise ValueError('Daily Review observed_at must be timezone-aware')
    session_open = datetime.combine(market_date, time(9), KST)
    watches = wait_price_observations.history(market_date, as_of=observed_at)
    watches = [watch for watch in watches if watch['decision_agent_name'] in DECISION_SUPPORT_AGENT_NAMES]
    paper_trades = wait_paper_ledger.history(market_date, as_of=observed_at)
    error = wait_price_observations.paper_recording_error
    paper_error = error.split(':', 1)[0] if error else None
    paper_summary = summarize_wait_paper_trades(paper_trades, recording_error=paper_error)
    roles = {}
    with session_scope(statement_timeout_ms=10_000) as db:
        executions = _stored_execution(db, market_date, observed_at, watches)
        filled_ids = {key for key, rows in executions.items() if any(row['entry_fill_observed'] for row in rows)}
        breadth = _market_breadth(db, market_date, observed_at)
        for role in DECISION_SUPPORT_AGENT_NAMES:
            role_watches = _role_watches(watches, role)
            role_paper = sorted(
                [row for row in paper_trades if row['decision_agent_name'] == role],
                key=lambda row: (row['status'] not in {'target_exited', 'stop_exited'}, row['status'] != 'open',
                                 row['decision_snapshot_id']),
            )
            ordered = sorted(role_watches, key=lambda watch: (
                watch['decision_snapshot_id'] not in filled_ids,
                not bool(watch.get('ask_entry')), not bool(watch.get('ask_touch')),
                watch['status'] != 'touched', watch['decision_snapshot_id'],
            ))
            priority = []
            contrasts = []
            for watch in ordered:
                if watch['decision_snapshot_id'] in filled_ids or _has_watch_event(watch):
                    priority.append(watch)
                else:
                    contrasts.append(watch)
            fill_count = sum(watch['decision_snapshot_id'] in filled_ids for watch in role_watches)
            priority_limit = max(min(fill_count, MAX_EXAMPLES_PER_ROLE), MAX_EXAMPLES_PER_ROLE - min(2, len(contrasts)))
            selected = priority[:priority_limit]
            selected.extend(contrasts[:MAX_EXAMPLES_PER_ROLE - len(selected)])
            role_execution = [outcome for watch in role_watches for outcome in executions.get(watch['decision_snapshot_id'], [])]
            roles[role] = {
                **_watch_counts(role_watches),
                'registration': _watch_registration(role_watches, session_open),
                'actual_entry_fill_count': sum(row['entry_fill_observed'] for row in role_execution),
                'actual_closed_trade_count': sum(row['closed_trade_observed'] for row in role_execution),
                'execution_lifecycle_counts': dict(Counter(row['lifecycle_status'] for row in role_execution)),
                'paper_trades': [
                    {key: value for key, value in row.items() if key != 'events'}
                    for row in role_paper[:MAX_EXAMPLES_PER_ROLE]
                ],
                'paper_summary': paper_summary['by_role'][role],
                'examples': [
                    {**watch, 'recorded_execution': executions.get(watch['decision_snapshot_id'], [])[:3]}
                    for watch in selected
                ],
            }
        examples = [example for role in roles.values() for example in role['examples']]
        contexts = _decision_contexts(db, examples, observed_at)
        example_symbols = sorted({example['symbol'] for example in examples})
        session_prices = _session_prices(db, market_date, observed_at, example_symbols)
        technicals = _recent_technicals(db, market_date, observed_at, example_symbols)
        filings = _session_filings(db, market_date, observed_at, example_symbols)
    session_news = _session_news(market_date, observed_at, example_symbols)
    for example in examples:
        example.update(
            decision_context=contexts.get(example['decision_snapshot_id']),
            session_news=session_news.get(example['symbol'], []),
            session_price=None,
            technical_snapshots=technicals.get(example['symbol'], []),
            session_filings=filings.get(example['symbol'], []),
        )
        if price := session_prices.get(example['symbol']):
            ask = _number(_object(example.get('ask_entry')).get('ask_price'))
            example['session_price'] = {
                **price,
                'confirmed_ask_mark_to_close_pct': round((price['close_price'] / ask - 1) * 100, 3) if ask and ask > 0 else None,
            }
    analyst_context = _market_analyst_context(store, market_date, observed_at)
    dashboard = _dashboard_market_context(market_date, observed_at)
    dashboard_status = dashboard['status'] if dashboard else 'unavailable'
    dashboard_markets = dashboard['markets'] if dashboard else {}
    analyst_is_current = bool(analyst_context and datetime.fromisoformat(analyst_context['completed_at']) >= session_open)
    has_market = bool(breadth or analyst_is_current or dashboard_status == 'available')
    index_legs = [_object(market.get('index')) for market in dashboard_markets.values()]
    market_volatility = any(leg.get('status') == 'ready' and _has_price_range(leg) for leg in index_legs)
    market_headlines = _market_headlines(market_date, observed_at)
    recent_sessions = _recent_sessions(market_date, observed_at)
    for name, role in roles.items():
        role_examples = role['examples']
        role_contexts = [_object(example['decision_context']) for example in role_examples]
        current_technicals = [
            snapshot for example in role_examples for snapshot in example['technical_snapshots']
            if snapshot['market_date'] == market_date.isoformat()
        ]
        role['source_status'] = {
            'watch': 'available' if role['watch_event_count'] else 'unavailable',
            'actual_execution': _execution_status(
                role['actual_entry_fill_count'], sum(role['execution_lifecycle_counts'].values()),
            ),
            'paper_execution': (
                'available' if role['paper_summary']['coverage'] == 'complete'
                else role['paper_summary']['coverage']
            ),
            'market': _source_status(
                has_market or bool(current_technicals) or any(example['session_price'] for example in role_examples),
                bool(dashboard or analyst_context) or any(example['technical_snapshots'] for example in role_examples) or any(
                    _object(context.get('market_regime')).get('posture') in {'normal', 'caution', 'risk_off'}
                    for context in role_contexts
                ),
            ),
            'market_volatility': 'available' if market_volatility or any(
                _has_price_range(example['session_price']) for example in role_examples
            ) or any(snapshot['atr_pct_14'] is not None and snapshot['atr_pct_14'] >= 0
                     for snapshot in current_technicals) else 'unavailable',
            'business': _source_status(
                any(example['session_filings'] for example in role_examples),
                any(company.get('filings') or company.get('financial_highlights')
                    for company in (_object(context.get('company')) for context in role_contexts)),
            ),
            'news': _source_status(
                any(example['session_news'] for example in role_examples),
                any(_object(context.get('news_at_decision')).get('items') for context in role_contexts),
            ),
            'recent_sessions': 'available' if any(
                session['roles'][name]['watch_event_count'] for session in recent_sessions
            ) else 'unavailable',
        }
    watch_event_count = sum(role['watch_event_count'] for role in roles.values())
    execution_records = sum(map(len, executions.values()))
    return {
        'market_date': market_date.isoformat(),
        'observed_at': observed_at.isoformat(),
        'roles': roles,
        'market': {
            'status': _source_status(has_market, bool(dashboard or analyst_context)),
            'breadth': aggregate_breadth(breadth) if breadth else None,
            'analyst_context': analyst_context,
            'dashboard': dashboard,
        },
        'market_headlines': market_headlines,
        'recent_sessions': recent_sessions,
        'coverage': {
            'watch': {
                'status': 'available' if watch_event_count else 'unavailable',
                'tracked_decisions': len(watches),
                'event_bearing_decisions': watch_event_count,
                'unique_symbols': len({watch['symbol'] for watch in watches}),
                'registration': _watch_registration(watches, session_open),
                'registration_by_role': {name: role['registration'] for name, role in roles.items()},
                'scope': 'Registration records the earliest possible monitoring time. The log does not measure feed coverage or a hit rate.',
                'trade_touch': 'first exact trade or crossing between two accepted trades',
                'ask_entry': 'first timestamped ask in the entry band with positive displayed size; not a fill',
            },
            'actual_execution': {
                'status': _execution_status(sum(role['actual_entry_fill_count'] for role in roles.values()), execution_records),
                'scope': 'Stored records linked to the watched decisions for this session. Records without fills describe lifecycle only.',
                'records': execution_records,
            },
            'paper_execution': {
                **paper_summary['overall'],
                'recording_error': paper_error,
                'scope': 'Virtual WAIT ledger only; never broker fills. Realized net outcomes include the frozen cost assumption. '
                         'Open marks are separate, and incomplete paths or missing inputs are unknown rather than zero. '
                         'Summaries cover all rows; paper_trades samples up to six per role with closed and open trades first, '
                         'omitting raw event timelines.',
            },
            'decision_context': {'covered_examples': len(contexts), 'examples': len(examples)},
            'market_risk': {'records': len(breadth), 'source': 'daily_market_risk_snapshots'},
            'market_dashboard': {
                'status': dashboard_status,
                'markets': {name: {leg: item['status'] for leg, item in legs.items()}
                            for name, legs in dashboard_markets.items()},
            },
            'market_headlines': {
                'items': len(market_headlines), 'source': 'market_news_items',
                'scope': 'Market-wide topic headlines. They provide context without linking a catalyst to a watched symbol.',
            },
            'session_price': {'covered_symbols': len(session_prices), 'scope': 'EOD bar marks, no fill or exit inference'},
            'technical_snapshots': {'covered_symbols': len(technicals), 'scope': 'up to three dated snapshots available at cutoff per sampled symbol'},
            'session_filings': {'covered_symbols': len(filings), 'scope': 'same-session DART receipts available at cutoff for sampled symbols'},
            'session_news': {'covered_symbols': len(session_news), 'scope': 'stored news linked before cutoff for sampled symbols'},
        },
    }
