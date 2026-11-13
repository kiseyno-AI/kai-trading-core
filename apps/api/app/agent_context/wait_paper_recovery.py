"""Replay stored WAIT observations without calling providers or changing the database.

Run from apps/api with ``python -B -m app.agent_context.wait_paper_recovery
2026-11-11 2026-11-12``. The default is a write-free preview; --apply writes
only the paper ledger. Sparse prices never prove an uninterrupted exit path.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import fields, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

KST = ZoneInfo('Asia/Seoul')


def _timestamp(value) -> datetime | None:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _positive(value) -> Decimal | None:
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() and number > 0 else None


def load_cohort(root: Path, day: date) -> list[dict]:
    """Use recorded monitoring membership, including independent same-symbol lanes."""
    rows = []
    for path in sorted((root / day.isoformat()).glob('*.watch.json')):
        row = json.loads(path.read_text(encoding='utf-8'))
        if row.get('market_date') != day.isoformat():
            raise ValueError(f'Watch market date mismatch: {path.name}')
        if path.name != f'{row["decision_snapshot_id"]}.watch.json':
            raise ValueError(f'Watch snapshot identity mismatch: {path.name}')
        rows.append(row)
    return sorted(rows, key=lambda row: row['decision_snapshot_id'])


def hydrate_recovery_watches(day: date, cohort: list[dict], *, snapshots: dict, plans: list, fallback_cost_bps=None) -> list:
    """Use a stored quantity only from the time its persisted value is provable."""
    from app.agent_context.wait_price_observations import WaitPriceWatch, watch_from_snapshot, watch_with_paper_plan

    names = {field.name for field in fields(WaitPriceWatch)}
    closed = datetime.combine(day, time(15, 30), KST)
    result = []
    for raw in cohort:
        watch = WaitPriceWatch(**{key: value for key, value in raw.items() if key in names})
        cutoff = _timestamp(raw.get('tracking_started_at'))
        if cutoff is None:
            result.append(watch)
            continue
        snapshot = snapshots.get(watch.decision_snapshot_id)
        current = watch_from_snapshot(snapshot, market_date=day) if snapshot is not None else None
        if current is not None and _timestamp(current.observed_at) <= cutoff:
            if (current.symbol, current.decision_agent_name, current.source_market_date) != (
                watch.symbol, watch.decision_agent_name, watch.source_market_date,
            ):
                raise ValueError('Stored snapshot does not match monitored watch')
            watch = replace(
                watch, target_price=watch.target_price or current.target_price,
                stop_loss_price=watch.stop_loss_price or current.stop_loss_price,
                buy_direction=watch.buy_direction or current.buy_direction,
                chase_limit_price=watch.chase_limit_price or current.chase_limit_price,
            )
        if watch.quantity is None:
            matching = [plan for plan in plans if plan.decision_snapshot_id == watch.decision_snapshot_id]
            latest = watch_with_paper_plan(
                watch, matching, known_at=closed, fallback_cost_bps=fallback_cost_bps,
                allow_reference_quantity=False,
            )
            if _positive(latest.quantity) is not None:
                candidates = {cutoff}
                for plan in matching:
                    initial = ((plan.submission_payload or {}).get('core_quantity_sizing') or {}).get('initial_policy') or {}
                    for value in (plan.created_at, plan.updated_at, initial.get('calculated_at')):
                        instant = _timestamp(value)
                        if instant is not None and cutoff <= instant <= closed:
                            candidates.add(instant)
                for known_at in sorted(candidates):
                    known = watch_with_paper_plan(
                        watch, matching, known_at=known_at, fallback_cost_bps=fallback_cost_bps,
                        allow_reference_quantity=False,
                    )
                    if (known.quantity, known.quantity_source, known.quantity_account_count) == (
                        latest.quantity, latest.quantity_source, latest.quantity_account_count,
                    ):
                        watch = replace(known, observed_at=max(_timestamp(watch.observed_at), known_at).isoformat())
                        break
        result.append(watch)
    return result


def _stored_events(root: Path, day: date, rows: list[dict], prices: list[dict]) -> tuple[list[dict], dict]:
    events = []
    sources = Counter()
    rejected = 0

    def add(row, *, symbol, source, kind='trade', precision='point', snapshot_id=None):
        nonlocal rejected
        as_of = _timestamp(row.get('price_as_of'))
        received = _timestamp(row.get('received_at') or row.get('price_received_at'))
        price = _positive(row.get('price') or row.get('observed_price') or row.get('ask_price'))
        if (
            as_of is None or received is None or price is None
            or as_of.astimezone(KST).date() != day
            or not time(9) <= as_of.astimezone(KST).time() <= time(15, 30)
            or not timedelta(0) <= received - as_of <= timedelta(seconds=30)
        ):
            rejected += 1
            return
        if kind == 'ask' and (
            type(row.get('ask_size')) is not int or row['ask_size'] <= 0
        ):
            rejected += 1
            return
        events.append({
            'symbol': symbol, 'price': price, 'price_as_of': as_of, 'received_at': received,
            'source': source, 'kind': kind, 'size': row.get('ask_size'), 'precision': precision,
            'decision_snapshot_id': snapshot_id,
        })
        sources[source] += 1

    for watch in rows:
        for suffix in ('touch', 'ask-touch', 'ask-entry'):
            path = root / str(day) / f'{watch["decision_snapshot_id"]}.{suffix}.json'
            if not path.is_file():
                continue
            evidence = json.loads(path.read_text(encoding='utf-8'))
            source = f'wait_{suffix}'
            if suffix == 'touch':
                if evidence.get('evidence') not in {'exact', 'crossed'}:
                    rejected += 1
                    continue
                add(evidence, symbol=watch['symbol'], source=source, precision=evidence['evidence'], snapshot_id=watch['decision_snapshot_id'])
                if evidence.get('previous_price_as_of'):
                    # A saved crossing contains two accepted trade points, not the intervening path.
                    add({
                        'price': evidence.get('previous_price'),
                        'price_as_of': evidence['previous_price_as_of'],
                        'received_at': evidence['price_received_at'],
                    }, symbol=watch['symbol'], source='wait_touch_previous', precision='crossing_endpoint')
            else:
                add(evidence, symbol=watch['symbol'], source=source, kind='ask', precision='timestamped_ask', snapshot_id=watch['decision_snapshot_id'])
    symbols = {row['symbol'] for row in rows}
    for row in prices:
        if row.get('symbol') in symbols:
            add(row, symbol=row['symbol'], source=row.get('source', 'stored_intraday_price'))
    # Receipt order preserves what was known; same-packet asks cap the simulated fill.
    events.sort(key=lambda row: (row['received_at'], row['price_as_of'], row['kind'] != 'ask'))
    return events, {
        'coverage': 'incomplete', 'path_gap': True,
        'precision': 'sparse_timestamped_points', 'source_counts': dict(sorted(sources.items())),
        'accepted_event_count': len(events), 'rejected_event_count': rejected,
        'precision_counts': dict(sorted(Counter(row['precision'] for row in events).items())),
        'cohort_files': [f'{day}/{row["decision_snapshot_id"]}.watch.json' for row in rows],
        'limitation': 'Sparse prices cannot establish an exit path. Complete daily bounds may prove no exit through their cutoff; crossed or missing bounds remain unresolved.',
    }


def _daily_no_exit_proof(row: dict, bars: list[dict], cutoff: date, *, now: datetime) -> dict | None:
    """Whole-session interior bounds exclude exits without inventing a price path."""
    from app.market.session import KRX_EXCHANGE_CALENDAR

    if row.get('status') != 'open' or not row.get('entry'):
        return None
    entry_at = _timestamp(row['entry']['price_as_of'])
    stop, target = _positive(row.get('stop_loss_price')), _positive(row.get('target_price'))
    if entry_at is None or stop is None or target is None or entry_at.astimezone(KST).date() > cutoff:
        return None
    sessions = KRX_EXCHANGE_CALENDAR.sessions_in_range(entry_at.astimezone(KST).date(), cutoff)
    covered = []
    for session in sessions:
        day = session.date()
        closed = KRX_EXCHANGE_CALENDAR.session_close(session).to_pydatetime()
        if closed > now:
            return None
        matches = [bar for bar in bars if bar.get('symbol') == row['symbol'] and str(bar.get('trade_date')) == str(day)]
        if len(matches) != 1 or matches[0].get('complete') is not True:
            return None
        bar = matches[0]
        opened, high, low, close = (_positive(bar.get(name)) for name in ('open_price', 'high_price', 'low_price', 'close_price'))
        if (
            any(value is None for value in (opened, high, low, close))
            or not low <= min(opened, close) <= max(opened, close) <= high
            or not stop < low <= high < target
        ):
            return None
        published_at = _timestamp(bar.get('published_at'))
        if published_at is None or not closed <= published_at <= now:
            return None
        covered.append({**bar, 'published_at': published_at.isoformat()})
    if not covered:
        return None
    economic_cutoff = KRX_EXCHANGE_CALENDAR.session_close(sessions[-1]).to_pydatetime()
    return {
        'coverage_basis': 'stored_daily_bounds_no_exit',
        'economic_cutoff': economic_cutoff.isoformat(),
        'published_at': max(_timestamp(bar['published_at']) for bar in covered).isoformat(),
        'retrospective': True, 'reconstruction_created_at': now.isoformat(),
        'bars': covered,
        'price_basis_assumption': 'Stored daily bars and frozen levels use a consistent price basis; this is a paper position, not actual execution.',
    }


def recover_market_date(
    day: date, *, observation_root: Path, ledger_root: Path,
    watches=None, prices=(), daily_bars=(), recovery_cutoff: date | None = None, apply: bool = False,
) -> dict:
    """Preview or apply a conservative replay; existing ledger lots are preserved."""
    from app.agent_context.wait_paper_ledger import WaitPaperLedger
    from app.agent_context.wait_price_observations import WaitPriceWatch
    from app.market.session import KRX_EXCHANGE_CALENDAR

    cutoff = recovery_cutoff or day
    if cutoff < day:
        raise ValueError('Recovery cutoff cannot precede the monitoring session')
    now = datetime.now(timezone.utc)
    cutoff_at = min(now, datetime.combine(cutoff, time(15, 30), KST))

    cohort = load_cohort(observation_root, day)
    ids = {row['decision_snapshot_id'] for row in cohort}
    if watches is None:
        names = {field.name for field in fields(WaitPriceWatch)}
        watches = [WaitPriceWatch(**{key: value for key, value in row.items() if key in names}) for row in cohort]
    watches = [watch for watch in watches if watch.decision_snapshot_id in ids]
    if len(watches) != len(ids) or {watch.decision_snapshot_id for watch in watches} != ids:
        raise ValueError('Every monitored snapshot must have one recovery watch')
    by_id = {row['decision_snapshot_id']: row for row in cohort}
    for watch in watches:
        raw = by_id[watch.decision_snapshot_id]
        if (watch.symbol, watch.decision_agent_name, watch.source_market_date, _positive(watch.trigger_price)) != (
            raw['symbol'], raw['decision_agent_name'], raw['source_market_date'], _positive(raw.get('trigger_price')),
        ):
            raise ValueError('Recovery watch must preserve the original monitored identity and trigger')
    reader = WaitPaperLedger(ledger_root, persist=False)
    protected_ids = {row['decision_snapshot_id'] for row in reader.history(day)}
    existing = {row['decision_snapshot_id']: row for row in reader.history(day, as_of=cutoff_at, include_reconstruction=True)}
    untouched = [watch for watch in watches if watch.decision_snapshot_id not in protected_ids]
    ledger = WaitPaperLedger(ledger_root, persist=apply, scope={(day, watch.decision_snapshot_id) for watch in untouched})
    events, evidence = _stored_events(observation_root, day, cohort, list(prices))
    starts = {}
    for watch in untouched:
        row = by_id[watch.decision_snapshot_id]
        started = _timestamp(row.get('tracking_started_at'))
        known = _timestamp(watch.observed_at)
        if started is not None and known is not None and started.astimezone(KST).date() == day:
            starts[watch.decision_snapshot_id] = max(started, known)
    active = [watch for watch in untouched if watch.decision_snapshot_id in starts]
    recovery_evidence = {}
    for watch in untouched:
        raw = by_id[watch.decision_snapshot_id]
        original_start = _timestamp(raw.get('tracking_started_at'))
        original_known = _timestamp(raw.get('observed_at'))
        conditions = [
            {
                'type': 'confirmed_ask_entry_condition' if event['kind'] == 'ask' else 'trigger_trade_touch',
                'price': str(event['price']), 'price_as_of': event['price_as_of'].isoformat(),
                'received_at': event['received_at'].isoformat(), 'precision': event['precision'],
                'source': event['source'],
            }
            for event in events
            if event['decision_snapshot_id'] == watch.decision_snapshot_id
            and event['source'] in {'wait_touch', 'wait_ask-entry'}
            and original_start is not None and original_known is not None
            and event['price_as_of'] >= max(original_start, original_known)
        ]
        recovery_evidence[watch.decision_snapshot_id] = {
            'original_observed_at': raw['observed_at'],
            'original_tracking_started_at': raw.get('tracking_started_at'),
            'quantity_known_at': starts.get(watch.decision_snapshot_id).isoformat()
            if _positive(watch.quantity) is not None and watch.decision_snapshot_id in starts else None,
            'entry_condition_status': 'observed' if conditions else 'not_observed_in_stored_evidence',
            'observed_entry_conditions': conditions,
            'quantity_status': 'known_stored_quantity' if _positive(watch.quantity) is not None else 'unknown',
            'reference_quantity_policy_applied': False,
            'reconstruction_created_at': datetime.now(timezone.utc).isoformat(),
        }
    if active:
        # Configure each historical start separately so evidence never predates enrollment.
        for start in sorted(set(starts.values())):
            enrolled = [watch for watch in active if starts[watch.decision_snapshot_id] <= start]
            ledger.configure(day, enrolled, now=start)
        for watch in active:
            ledger.mark_path_gap(day, watch.decision_snapshot_id, now=starts[watch.decision_snapshot_id], reason='historical_path_incomplete')
        for event in events:
            if event['kind'] == 'ask':
                ledger.observe_ask(
                    event['symbol'], event['price'], event['size'], price_as_of=event['price_as_of'],
                    received_at=event['received_at'], source=event['source'],
                )
            else:
                observe = ledger.observe_mark if event['source'] == 'monitoring_last_quote' else ledger.observe_trade
                observe(
                    event['symbol'], event['price'], price_as_of=event['price_as_of'],
                    received_at=event['received_at'], source=event['source'],
                )
        bars = list(daily_bars)
        for row in ledger.history(day):
            if row.get('status') != 'open' or not row.get('entry'):
                continue
            for session in KRX_EXCHANGE_CALENDAR.sessions_in_range(day, cutoff):
                proof = _daily_no_exit_proof(row, bars, session.date(), now=now)
                if proof is None:
                    if row.get('coverage_basis') == 'stored_daily_bounds_no_exit':
                        ledger.mark_path_gap(
                            day, row['decision_snapshot_id'],
                            now=KRX_EXCHANGE_CALENDAR.session_open(session).to_pydatetime(),
                            reason='daily_bounds_coverage_incomplete',
                        )
                    break
                ledger.certify_no_exit(
                    day, row['decision_snapshot_id'], price=Decimal(str(proof['bars'][-1]['close_price'])),
                    price_as_of=_timestamp(proof['economic_cutoff']), received_at=_timestamp(proof['published_at']),
                    evidence=proof,
                )
                row = {**row, 'coverage_basis': 'stored_daily_bounds_no_exit'}
        for watch in active:
            metadata = recovery_evidence[watch.decision_snapshot_id]
            conditions = metadata['observed_entry_conditions']
            if not conditions:
                ledger.annotate_recovery(day, watch.decision_snapshot_id, now=starts[watch.decision_snapshot_id], evidence=metadata)
            for index, condition in enumerate(conditions):
                ledger.annotate_recovery(
                    day, watch.decision_snapshot_id,
                    now=max(starts[watch.decision_snapshot_id], _timestamp(condition['received_at'])),
                    evidence={**metadata, 'observed_entry_conditions': conditions[:index + 1]},
                )
    results = {**existing, **{
        row['decision_snapshot_id']: row
        for row in ledger.history(day, as_of=cutoff_at, include_reconstruction=True)
    }}
    items = []
    for row in cohort:
        key = row['decision_snapshot_id']
        item = results.get(key, {
            'decision_snapshot_id': key, 'decision_agent_name': row['decision_agent_name'],
            'symbol': row['symbol'], 'status': 'unknown_time_coverage', 'entry': None, 'exit': None,
            'realized_gross_pnl_krw': None, 'realized_net_pnl_krw': None,
        })
        items.append({
            **item, 'recovery_preserved_existing': key in protected_ids,
            'recovery_evidence': item.get('recovery_evidence') or recovery_evidence.get(key),
            'source_evidence': {
                'watch_file': str(observation_root / str(day) / f'{key}.watch.json'),
                'observation_files': [
                    f'{key}.{suffix}.json' for suffix in ('touch', 'ask-touch', 'ask-entry')
                    if (observation_root / str(day) / f'{key}.{suffix}.json').is_file()
                ],
                'accepted_symbol_event_count': sum(event['symbol'] == row['symbol'] for event in events),
                'coverage': item.get('coverage', 'incomplete'),
            },
        })
    entered = lambda row: row.get('entry') is not None
    exited = lambda row: row.get('exit') is not None
    evidence['coverage'] = 'complete' if items and all(row.get('coverage') == 'complete' for row in items) else 'incomplete'
    evidence['path_gap'] = any(row.get('coverage') != 'complete' for row in items)
    evidence['coverage_basis_counts'] = dict(Counter(row.get('coverage_basis', 'sparse_observations') for row in items))
    return {
        'market_date': str(day), 'recovery_cutoff': str(cutoff), 'dry_run': not apply, 'cohort_count': len(cohort),
        'preserved_existing_count': len(ids & protected_ids),
        'recovered_entry_count': sum(entered(row) and not row['recovery_preserved_existing'] for row in items),
        'recovered_exit_count': sum(exited(row) and not row['recovery_preserved_existing'] for row in items),
        'open_count': sum(entered(row) and not exited(row) for row in items),
        'unknown_exit_count': sum(entered(row) and not exited(row) and row.get('coverage') == 'incomplete' for row in items),
        'missing_quantity_count': sum(_positive(watch.quantity) is None for watch in watches),
        'missing_levels_count': sum(_positive(watch.target_price) is None or _positive(watch.stop_loss_price) is None for watch in watches),
        'missing_time_coverage_count': sum(row.get('coverage') != 'complete' for row in items),
        'missing_tracking_start_count': len(ids - starts.keys() - protected_ids),
        'missing_cost_count': sum(watch.round_trip_cost_bps is None for watch in watches),
        'incomplete_count': sum(row.get('coverage') != 'complete' for row in items),
        'daily_bounds_confirmed_open_count': sum(
            row.get('status') == 'open' and row.get('coverage') == 'complete'
            and row.get('coverage_basis') == 'stored_daily_bounds_no_exit' for row in items
        ),
        'unknown_entry_count': sum(not entered(row) and not exited(row) for row in items),
        'observed_entry_condition_count': sum(
            (row.get('recovery_evidence') or {}).get('entry_condition_status') == 'observed' for row in items
        ),
        'unquantified_entry_condition_count': sum(
            (row.get('recovery_evidence') or {}).get('entry_condition_status') == 'observed'
            and (row.get('recovery_evidence') or {}).get('quantity_status') == 'unknown' for row in items
        ),
        'evidence': evidence, 'items': items,
    }


def load_stored_inputs(day: date, *, observation_root: Path, monitoring_root: Path) -> tuple[list, list[dict]]:
    """Read only the exact cohort's snapshots, plans and same-session point prices."""
    cohort = load_cohort(observation_root, day)
    if not cohort:
        return [], []
    from sqlalchemy import select, text

    from app.core.settings import settings
    from app.db import SessionLocal
    from app.db_models import (
        KISIntradaySnapshot, QuoteSnapshot, SignalDeciderDecisionSnapshot,
        SignalDeciderExecutionPlanItem, Symbol,
    )

    ids = {row['decision_snapshot_id'] for row in cohort}
    symbols = {row['symbol'] for row in cohort}
    opened = datetime.combine(day, time(9), KST)
    closed = datetime.combine(day, time(15, 30), KST)
    prices = []
    with SessionLocal() as session:
        session.execute(text('SET TRANSACTION READ ONLY'))
        snapshots = {row.id: row for row in session.scalars(select(SignalDeciderDecisionSnapshot).where(
            SignalDeciderDecisionSnapshot.id.in_(ids),
        )).all()}
        plans = session.scalars(select(SignalDeciderExecutionPlanItem).where(
            SignalDeciderExecutionPlanItem.decision_snapshot_id.in_(ids),
            SignalDeciderExecutionPlanItem.target_trade_date == day,
            SignalDeciderExecutionPlanItem.broker_name == 'kis',
            SignalDeciderExecutionPlanItem.execution_window == 'next_open',
            SignalDeciderExecutionPlanItem.planned_side == 'buy',
        )).all()
        hydrated = hydrate_recovery_watches(
            day, cohort, snapshots=snapshots, plans=plans,
            fallback_cost_bps=settings.signal_decider_next_open_non_spread_round_trip_cost_bps,
        )
        for symbol, row in session.execute(
            select(Symbol.symbol, QuoteSnapshot).join(Symbol, Symbol.id == QuoteSnapshot.symbol_id).where(
                Symbol.symbol.in_(symbols), QuoteSnapshot.captured_at >= opened,
                QuoteSnapshot.captured_at <= closed + timedelta(seconds=30),
                QuoteSnapshot.is_live.is_(True), QuoteSnapshot.fallback.is_(False),
            )
        ):
            prices.append({
                'symbol': symbol, 'price': row.price, 'price_as_of': row.as_of,
                'received_at': row.captured_at, 'source': 'quote_snapshots',
            })
        for symbol, row in session.execute(
            select(Symbol.symbol, KISIntradaySnapshot)
            .join(Symbol, Symbol.id == KISIntradaySnapshot.symbol_id).where(
                Symbol.symbol.in_(symbols), KISIntradaySnapshot.as_of_market_date == day,
                KISIntradaySnapshot.captured_at >= opened,
                KISIntradaySnapshot.captured_at <= closed + timedelta(seconds=30),
                KISIntradaySnapshot.availability_status.in_(['available', 'partial']),
            )
        ):
            quote = (row.snapshot_payload or {}).get('quote') or {}
            if quote.get('status') == 'available':
                prices.append({
                    'symbol': symbol, 'price': quote.get('price'),
                    'price_as_of': quote.get('as_of_timestamp'),
                    'received_at': row.captured_at, 'source': 'kis_intraday_snapshots',
                })
    monitoring_path = monitoring_root / f'{day}.json'
    if monitoring_path.is_file():
        for symbol, quote in json.loads(monitoring_path.read_text(encoding='utf-8')).items():
            if symbol in symbols and quote:
                prices.append({
                    'symbol': symbol, 'price': quote.get('price'), 'price_as_of': quote.get('as_of'),
                    'received_at': quote.get('as_of'), 'source': 'monitoring_last_quote',
                })
    return hydrated, prices


def load_daily_bounds(day: date, cutoff: date, *, symbols: set[str]) -> list[dict]:
    """Read only EOD bars whose completed collection began after that session."""
    if not symbols:
        return []
    from sqlalchemy import select, text

    from app.db import SessionLocal
    from app.db_models import DailyPriceBar, EODCollectionRun, Symbol
    from app.market.session import KRX_EXCHANGE_CALENDAR

    bars = []
    with SessionLocal() as session:
        session.execute(text('SET TRANSACTION READ ONLY'))
        rows = session.execute(
            select(Symbol.symbol, DailyPriceBar, EODCollectionRun)
            .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
            .outerjoin(EODCollectionRun, EODCollectionRun.id == DailyPriceBar.eod_run_id)
            .where(Symbol.symbol.in_(symbols), DailyPriceBar.trade_date.between(day, cutoff))
        )
        for symbol, bar, run in rows:
            try:
                closed = KRX_EXCHANGE_CALENDAR.session_close(bar.trade_date.isoformat()).to_pydatetime()
            except ValueError:
                continue
            started = _timestamp(run.started_at) if run is not None else None
            completed = _timestamp(run.completed_at) if run is not None else None
            captured = _timestamp(bar.captured_at)
            published = max(completed, captured) if completed is not None and captured is not None else None
            bars.append({
                'id': bar.id, 'symbol': symbol, 'trade_date': bar.trade_date.isoformat(),
                'open_price': bar.open_price, 'high_price': bar.high_price,
                'low_price': bar.low_price, 'close_price': bar.close_price, 'source': bar.source,
                'eod_run_id': bar.eod_run_id,
                'eod_run_completed_at': completed.isoformat() if completed else None,
                'captured_at': captured.isoformat() if captured else None,
                'published_at': published.isoformat() if published else None,
                'complete': bool(
                    run is not None and run.status == 'completed' and run.trade_date == bar.trade_date
                    and started is not None and completed is not None and closed <= started <= completed
                    and captured is not None and captured >= closed
                ),
            })
    return bars


def main(argv: list[str] | None = None) -> None:
    from app.agent_context.evaluation_refresh import default_marker_dir

    root = default_marker_dir() / 'watch-audit'
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('market_dates', type=date.fromisoformat, nargs='+')
    parser.add_argument('--observations-root', type=Path, default=root / 'price-observations')
    parser.add_argument('--monitoring-root', type=Path, default=root / 'monitoring-sessions')
    parser.add_argument('--ledger-root', type=Path, default=root / 'paper-ledger')
    parser.add_argument('--apply', action='store_true', help='Write absent paper lots only; database remains read-only.')
    args = parser.parse_args(argv)
    reports = []
    cutoff = max(args.market_dates)
    if cutoff > datetime.now(KST).date():
        parser.error('Cannot reconstruct a future market date')
    for day in sorted(set(args.market_dates)):
        watches, prices = load_stored_inputs(
            day, observation_root=args.observations_root, monitoring_root=args.monitoring_root,
        )
        bars = load_daily_bounds(day, cutoff, symbols={watch.symbol for watch in watches})
        reports.append(recover_market_date(
            day, observation_root=args.observations_root, ledger_root=args.ledger_root,
            watches=watches, prices=prices, daily_bars=bars, recovery_cutoff=cutoff, apply=args.apply,
        ))
    print(json.dumps({'dry_run': not args.apply, 'market_dates': reports}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
