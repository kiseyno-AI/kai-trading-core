"""Record WAIT trigger observations from KIS trades, without execution authority."""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
import threading
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from app.agent_context.evaluation_refresh import default_marker_dir
from app.market.clock import KST
from app.market.session import get_krx_exchange_calendar_day
from app.market.symbol_codes import is_canonical_krx_short_code

if TYPE_CHECKING:
    from app.db_models import SignalDeciderDecisionSnapshot
    from app.quotes.kis_realtime_prices import KISRealtimePriceTick

logger = logging.getLogger(__name__)
MAX_TRADE_DELAY = timedelta(seconds=30)


@dataclass(frozen=True)
class WaitPriceWatch:
    decision_snapshot_id: int
    decision_agent_name: str
    symbol: str
    source_market_date: str
    observed_at: str
    trigger_price: str


def watch_from_snapshot(
    snapshot: SignalDeciderDecisionSnapshot, *, market_date: date,
) -> WaitPriceWatch | None:
    from app.orders.next_open_execution import (
        _is_krx_next_open_snapshot,
        _short_term_tactical_action,
        _trigger_entry_price_values,
    )

    role = snapshot.agent_name
    action = str(snapshot.next_day_open_action or snapshot.decision_action or '').strip().lower()
    if role == 'short-term-decider':
        if action != 'wait' or _short_term_tactical_action(snapshot) not in {'wait', 'watch_for_trigger'}:
            return None
    elif role != 'signal-decider' or action != 'wait':
        return None
    if (
        not is_canonical_krx_short_code(snapshot.symbol)
        or not _is_krx_next_open_snapshot(snapshot)
        or snapshot.market_date >= market_date
    ):
        return None
    trigger = _trigger_entry_price_values(snapshot)['trigger_price']
    known_at = max(
        value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        for value in (snapshot.created_at, snapshot.feedback_received_at)
        if value is not None
    )
    if trigger is None or not trigger.is_finite() or trigger <= 0 or known_at >= datetime.combine(
        market_date, time(15, 30), KST,
    ):
        return None
    return WaitPriceWatch(
        decision_snapshot_id=int(snapshot.id),
        decision_agent_name=role,
        symbol=snapshot.symbol,
        source_market_date=snapshot.market_date.isoformat(),
        observed_at=known_at.isoformat(),
        trigger_price=str(trigger),
    )


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding='utf-8'))


def _write_once(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.wait-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(name, path)
        except FileExistsError:
            pass
    finally:
        Path(name).unlink()


class WaitPriceObservations:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or default_marker_dir() / 'watch-audit' / 'price-observations'
        self._lock = threading.Lock()
        self._day: date | None = None
        self._watches: dict[str, list[WaitPriceWatch]] = {}
        self._started: dict[int, datetime] = {}
        self._previous: dict[int, tuple[datetime, Decimal]] = {}
        self._touched: set[int] = set()

    def recovery_watches(self, day: date) -> list[WaitPriceWatch]:
        return [
            WaitPriceWatch(**_read(path))
            for path in sorted((self.root / day.isoformat() / 'recovery').glob('*.json'))
        ]

    def enroll_recovery(self, day: date, watches: list[WaitPriceWatch]) -> None:
        if not get_krx_exchange_calendar_day(day).is_open:
            raise ValueError('WAIT observation date must be a KRX session')
        for watch in watches:
            _write_once(
                self.root / day.isoformat() / 'recovery' / f'{watch.decision_snapshot_id}.json',
                asdict(watch),
            )

    def configure(self, day: date, watches: list[WaitPriceWatch], *, now: datetime) -> None:
        with self._lock:
            if self._day is not None and day < self._day:
                return
            same_day = self._day == day
            started_by_id = dict(self._started) if same_day else {}
            touched = set(self._touched) if same_day else set()
            grouped: dict[str, list[WaitPriceWatch]] = defaultdict(list)
            active_ids: set[int] = set()
            for watch in watches:
                snapshot_id = watch.decision_snapshot_id
                if snapshot_id in active_ids:
                    continue
                active_ids.add(snapshot_id)
                grouped[watch.symbol].append(watch)
                if snapshot_id in started_by_id:
                    continue
                path = self.root / day.isoformat() / f'{snapshot_id}.watch.json'
                started = max(now, datetime.fromisoformat(watch.observed_at))
                _write_once(path, {
                    **asdict(watch), 'market_date': day.isoformat(),
                    'tracking_started_at': started.isoformat(), 'status': 'not_observed',
                })
                started_by_id[snapshot_id] = started
                if path.with_name(f'{snapshot_id}.touch.json').is_file():
                    touched.add(snapshot_id)
            self._previous = {
                key: value for key, value in self._previous.items() if same_day and key in active_ids
            }
            self._started = {key: started_by_id[key] for key in active_ids}
            self._touched = touched
            self._day = day
            self._watches = grouped

    def symbols(self, day: date) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._watches) if self._day == day else ()

    def observe(self, tick: KISRealtimePriceTick) -> None:
        with self._lock:
            watches = self._watches.get(tick.symbol)
            if not watches:
                return
            price = tick.current_price
            local = tick.price_as_of.astimezone(KST)
            if (
                price is None or not price.is_finite() or price <= 0 or tick.trade_halt
                or local.date() != self._day
                or not time(9) <= local.time() <= time(15, 30)
                or not timedelta(0) <= tick.received_at - tick.price_as_of <= MAX_TRADE_DELAY
                or tick.raw_fields.get('STCK_CNTG_HOUR') != local.strftime('%H%M%S')
            ):
                return
            for watch in watches:
                snapshot_id = watch.decision_snapshot_id
                if snapshot_id in self._touched or tick.price_as_of < self._started[snapshot_id]:
                    continue
                previous = self._previous.get(snapshot_id)
                if previous is not None and tick.price_as_of < previous[0]:
                    continue
                trigger = Decimal(watch.trigger_price)
                exact = price == trigger
                crossed = previous is not None and min(previous[1], price) < trigger < max(previous[1], price)
                if exact or crossed:
                    try:
                        _write_once(self.root / local.date().isoformat() / f'{snapshot_id}.touch.json', {
                            'status': 'touched', 'evidence': 'exact' if exact else 'crossed',
                            'price_as_of': tick.price_as_of.isoformat(), 'observed_price': str(price),
                            'previous_price': str(previous[1]) if crossed else None,
                            'previous_price_as_of': previous[0].isoformat() if crossed else None,
                        })
                    except OSError:
                        logger.exception('Could not save WAIT price observation for snapshot %s', snapshot_id)
                        continue
                    self._touched.add(snapshot_id)
                self._previous[snapshot_id] = (tick.price_as_of, price)

    def history(self, day: date) -> list[dict]:
        folder = self.root / day.isoformat()
        results = {}
        for watch in self.recovery_watches(day):
            results[watch.decision_snapshot_id] = {
                **asdict(watch), 'market_date': day.isoformat(), 'status': 'enrolled',
            }
        for path in folder.glob('*.watch.json'):
            row = _read(path)
            key = row['decision_snapshot_id']
            touch = folder / f'{key}.touch.json'
            results[key] = {**row, **(_read(touch) if touch.is_file() else {})}
        return [results[key] for key in sorted(results)]


wait_price_observations = WaitPriceObservations()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('market_date', type=date.fromisoformat)
    parser.add_argument('--enroll-recovery', type=int, nargs='+')
    args = parser.parse_args()
    if args.enroll_recovery:
        from sqlalchemy import select, text

        from app.db import SessionLocal
        from app.db_models import SignalDeciderDecisionSnapshot

        ids = set(args.enroll_recovery)
        if args.market_date < datetime.now(KST).date():
            parser.error('Cannot enroll observations for a past session')
        with SessionLocal() as session:
            session.execute(text('SET TRANSACTION READ ONLY'))
            snapshots = session.scalars(select(SignalDeciderDecisionSnapshot).where(
                SignalDeciderDecisionSnapshot.id.in_(ids),
            )).all()
        watches = [
            watch for snapshot in snapshots
            if (watch := watch_from_snapshot(snapshot, market_date=args.market_date)) is not None
        ]
        if {watch.decision_snapshot_id for watch in watches} != ids:
            parser.error('Every snapshot must have a valid KRX WAIT trigger for this date')
        wait_price_observations.enroll_recovery(args.market_date, watches)
    print(json.dumps(wait_price_observations.history(args.market_date), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
