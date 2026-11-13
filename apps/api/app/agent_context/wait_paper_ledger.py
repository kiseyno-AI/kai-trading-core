"""Durable, observation-driven virtual WAIT positions; never submits orders."""

from __future__ import annotations

import copy
import fcntl
import json
import os
import tempfile
import threading
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from app.agent_context.evaluation_refresh import default_marker_dir
from app.market.clock import KST
from app.market.session import get_krx_exchange_calendar_day
from app.market.symbol_codes import is_canonical_krx_short_code

_ACTIVE = {'pending', 'open'}
_FIELDS = (
    'decision_snapshot_id', 'decision_agent_name', 'symbol', 'source_market_date',
    'observed_at', 'trigger_price', 'buy_direction', 'chase_limit_price',
    'target_price', 'stop_loss_price', 'quantity', 'quantity_source', 'round_trip_cost_bps',
    'quantity_account_count', 'cost_source',
    'quantity_basis', 'confidence_score_10',
)


def _number(value) -> Decimal | None:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def _moment(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError('WAIT paper timestamps must include a timezone')
    return result


class WaitPaperLedger:
    def __init__(self, root: Path | None = None, *, persist: bool = True, scope: set[tuple[date, int]] | None = None) -> None:
        self.root = root or default_marker_dir() / 'watch-audit' / 'paper-ledger'
        self.persist = persist
        self._memory: dict[Path, dict] = {}
        self._lock = threading.RLock()
        self._configured: set[Path] = set()
        self._active: dict[Path, dict] = {}
        self._durable_at: dict[Path, datetime] = {}
        self._versions: dict[Path, int] = {}
        self.scope = scope

    def _read(self, path: Path) -> dict | None:
        if path in self._memory:
            return copy.deepcopy(self._memory[path])
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except FileNotFoundError:
            return None

    def _paths(self) -> list[Path]:
        return sorted(path for path in set(self.root.glob('????-??-??/*.json')) | self._memory.keys()
                      if self.scope is None or (date.fromisoformat(path.parent.name), int(path.stem)) in self.scope)

    @contextmanager
    def _locked(self, path: Path):
        with self._lock:
            if not self.persist:
                yield
                return
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.with_suffix('.lock').open('a') as handle:
                fcntl.flock(handle, fcntl.LOCK_EX)
                yield

    def _save(self, path: Path, data: dict) -> None:
        if not self.persist:
            self._memory[path] = copy.deepcopy(data)
        else:
            fd, name = tempfile.mkstemp(prefix='.paper-', dir=path.parent)
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as handle:
                    json.dump(data, handle, ensure_ascii=False)
                    handle.flush()
                    os.fsync(handle.fileno())
                    version = os.fstat(handle.fileno()).st_mtime_ns
                os.replace(name, path)
                self._versions[path] = version
            finally:
                Path(name).unlink(missing_ok=True)
        if data['status'] in _ACTIVE and data.get('active_tracking', True):
            self._active[path] = copy.deepcopy(data)
        else:
            self._active.pop(path, None)
        last = data.get('last_event')
        if last:
            self._durable_at[path] = _moment(last['price_received_at'])

    @staticmethod
    def _event(data: dict, *, received_at: datetime, kind: str, updates: dict, **evidence) -> None:
        data['events'].append({
            'kind': kind, 'price_received_at': received_at.isoformat(),
            **evidence, 'updates': updates,
        })
        data.update(updates)

    def configure(self, day: date, watches, *, now: datetime) -> None:
        with self._lock:
            self._configure(day, watches, now=now)

    def _configure(self, day: date, watches, *, now: datetime) -> None:
        if now.tzinfo is None:
            raise ValueError('WAIT paper timestamps must include a timezone')
        if not get_krx_exchange_calendar_day(day).is_open:
            raise ValueError('WAIT paper date must be a KRX session')
        previous_cohorts = {(row['decision_snapshot_id'], row['market_date'])
                            for old_path in self._paths() if (row := self._read(old_path))}
        for watch in watches:
            key = int(watch.decision_snapshot_id)
            if self.scope is not None and (day, key) not in self.scope:
                continue
            path = self.root / day.isoformat() / f'{key}.json'
            if any(old_key == key and old_day < day.isoformat() for old_key, old_day in previous_cohorts):
                continue
            try:
                existing_version = path.stat().st_mtime_ns
            except FileNotFoundError:
                existing_version = None
            existing = self._read(path)
            if existing is not None and existing['status'] != 'unavailable':
                if existing['status'] in _ACTIVE and existing.get('active_tracking', True):
                    cached = self._active.get(path)
                    if cached and cached['status'] == existing['status']:
                        for field in ('last_event', 'previous_trade'):
                            old, new = cached.get(field), existing.get(field)
                            if old and (not new or _moment(old['price_as_of']) > _moment(new['price_as_of'])):
                                existing[field] = old
                    self._active[path] = existing
                    if path not in self._configured and existing['status'] == 'pending':
                        self._active[path]['previous_trade'] = None
                    if self.persist:
                        self._versions[path] = existing_version
                    if existing['last_event']:
                        self._durable_at.setdefault(path, _moment(existing['last_event']['price_received_at']))
                if path not in self._configured and existing['status'] == 'open':
                    last = existing.get('last_event') or existing['entry']
                    if now > _moment(last['price_received_at']):
                        self.mark_path_gap(day, key, now=now, reason='restart_path_gap')
                drift = [field for field in _FIELDS if field not in {'observed_at'}
                         and str(getattr(watch, field, None)) != str(existing.get(field))]
                if existing['status'] in _ACTIVE and drift and drift != existing.get('input_drift'):
                    with self._locked(path):
                        latest = self._read(path)
                        if latest['status'] in _ACTIVE:
                            self._event(latest, received_at=now, kind='input_drift', updates={'input_drift': drift})
                            self._save(path, latest)
                self._configured.add(path)
                continue
            with self._locked(path):
                prior = self._read(path)
                if prior is not None and prior['status'] != 'unavailable':
                    continue
                fields = {field: getattr(watch, field, None) for field in _FIELDS}
                for field in ('trigger_price', 'target_price', 'stop_loss_price', 'quantity', 'round_trip_cost_bps'):
                    if fields[field] is not None:
                        fields[field] = str(fields[field])
                missing = []
                for field in ('trigger_price', 'target_price', 'stop_loss_price', 'quantity', 'round_trip_cost_bps'):
                    value = _number(fields[field])
                    if value is None or (value < 0 if field == 'round_trip_cost_bps' else value <= 0):
                        missing.append(field)
                quantity = _number(fields['quantity'])
                if quantity is not None and quantity != quantity.to_integral_value():
                    missing.append('integer_quantity')
                if not fields['quantity_source']:
                    missing.append('quantity_source')
                if fields['buy_direction'] not in {'at_or_above', 'at_or_below'}:
                    missing.append('buy_direction')
                if not is_canonical_krx_short_code(fields['symbol']):
                    missing.append('canonical_symbol')
                target, stop, trigger = (_number(fields[name]) for name in ('target_price', 'stop_loss_price', 'trigger_price'))
                if all(value is not None for value in (target, stop, trigger)) and not stop < trigger < target:
                    missing.append('ordered_exit_levels')
                started = max(now, _moment(fields['observed_at']))
                initial = {
                    **fields, 'market_date': day.isoformat(), 'tracking_started_at': started.isoformat(),
                    'status': 'unavailable' if missing else 'pending', 'missing_inputs': missing,
                    'coverage': 'complete', 'coverage_reasons': [], 'exit_status': 'pending',
                    'active_tracking': not missing,
                    'entry': None, 'exit': None, 'filled_quantity': None, 'unfilled_quantity': None,
                    'realized_gross_pnl_krw': None, 'realized_net_pnl_krw': None,
                    'unrealized_gross_pnl_krw': None, 'unrealized_net_pnl_krw': None,
                    'cost_krw': None, 'last_event': None, 'previous_trade': None,
                }
                if prior is not None:
                    if prior['missing_inputs'] == missing:
                        continue
                    self._event(prior, received_at=now, kind='inputs_updated', updates=initial)
                    self._save(path, prior)
                else:
                    self._save(path, {**initial, 'initial': initial, 'events': []})
            self._configured.add(path)
        for path, row in list(self._active.items()):
            if row['status'] == 'pending' and row['market_date'] < day.isoformat():
                self._active.pop(path, None)
        # Earlier sessions' open positions remain part of this observer after rollover.
        for path in self._paths():
            if path in self._configured:
                continue
            row = self._read(path)
            if row and row['status'] == 'open' and row.get('active_tracking', True) and row['market_date'] <= day.isoformat():
                self._active[path] = row
                if self.persist:
                    self._versions[path] = path.stat().st_mtime_ns
                if row['last_event']:
                    self._durable_at[path] = _moment(row['last_event']['price_received_at'])
                last = row.get('last_event') or row['entry']
                if now > _moment(last['price_received_at']):
                    self.mark_path_gap(date.fromisoformat(row['market_date']), row['decision_snapshot_id'], now=now, reason='restart_path_gap')
                self._configured.add(path)

    def mark_path_gap(self, day: date, snapshot_id: int, *, now: datetime, reason: str = 'historical_path_incomplete') -> None:
        path = self.root / day.isoformat() / f'{int(snapshot_id)}.json'
        row = self._read(path)
        if row is None or row['status'] not in _ACTIVE | {'unavailable'} or reason in row['coverage_reasons']:
            return
        with self._locked(path):
            row = self._read(path)
            if row is None or row['status'] not in _ACTIVE | {'unavailable'} or reason in row['coverage_reasons']:
                return
            self._event(row, received_at=now, kind='path_gap', updates={
                'coverage': 'incomplete', 'coverage_reasons': [*row['coverage_reasons'], reason],
                'exit_status': 'unknown_path_gap',
                'unrealized_gross_pnl_krw': None, 'unrealized_net_pnl_krw': None,
                'cost_krw': None,
                'previous_trade': None,
                'active_tracking': row['status'] == 'pending',
            })
            self._save(path, row)

    def annotate_recovery(self, day: date, snapshot_id: int, *, now: datetime, evidence: dict) -> None:
        path = self.root / day.isoformat() / f'{int(snapshot_id)}.json'
        row = self._read(path)
        if row is None or row['status'] not in _ACTIVE | {'unavailable'} or row.get('recovery_evidence') == evidence:
            return
        with self._locked(path):
            row = self._read(path)
            if row is None or row['status'] not in _ACTIVE | {'unavailable'} or row.get('recovery_evidence') == evidence:
                return
            self._event(row, received_at=now, kind='recovery_annotation', updates={'recovery_evidence': copy.deepcopy(evidence)})
            self._save(path, row)

    def symbols(self, day: date) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted({
                row['symbol'] for row in self._active.values()
                if (row['market_date'] == day.isoformat() and row['status'] == 'pending'
                     or row['market_date'] <= day.isoformat() and row['status'] == 'open')
            }))

    def dates(self) -> set[date]:
        days = set()
        for path in self._paths():
            row = self._read(path)
            days.add(date.fromisoformat(row['market_date']))
            days.update(_moment(event['price_as_of']).astimezone(KST).date()
                        for event in row['events'] if event.get('price_as_of'))
        return days

    def observe_trade(self, symbol, price, *, price_as_of, received_at, source='kis_trade', trade_halt=False) -> None:
        if not trade_halt:
            with self._lock:
                self._observe(symbol, price, None, kind='trade', price_as_of=price_as_of, received_at=received_at, source=source)

    def observe_ask(self, symbol, ask, size, *, price_as_of, received_at, source) -> None:
        with self._lock:
            self._observe(symbol, ask, size, kind='ask', price_as_of=price_as_of, received_at=received_at, source=source)

    def observe_bid(self, symbol, bid, size, *, price_as_of, received_at, source) -> None:
        with self._lock:
            self._observe(symbol, bid, size, kind='bid', price_as_of=price_as_of, received_at=received_at, source=source)

    def observe_mark(self, symbol, price, *, price_as_of, received_at, source) -> None:
        """Retain an isolated last quote without inferring entry or an exit."""
        with self._lock:
            self._observe(symbol, price, None, kind='mark', price_as_of=price_as_of, received_at=received_at, source=source)

    def _observe(self, symbol, value, size, *, kind, price_as_of, received_at, source) -> None:
        price = _number(value)
        if (price is None or price <= 0 or price_as_of is None or price_as_of.tzinfo is None
            or received_at.tzinfo is None or not timedelta(0) <= received_at-price_as_of <= timedelta(seconds=30)):
            return
        local = price_as_of.astimezone(KST)
        if not time(9) <= local.time() <= time(15, 30) or not get_krx_exchange_calendar_day(local.date()).is_open:
            return
        if kind not in {'trade', 'mark'} and (not isinstance(size, int) or isinstance(size, bool) or size <= 0):
            return
        candidates = self._active
        if kind == 'mark':
            candidates = {path: row for path in self._paths() if (row := self._read(path)) and row['status'] == 'open'}
        for path, row in list(candidates.items()):
            version = path.stat().st_mtime_ns if self.persist else None
            if self.persist and version != self._versions.get(path):
                latest = self._read(path)
                for field in ('last_event', 'previous_trade'):
                    old, new = row.get(field), latest.get(field)
                    if (row['status'] == latest['status'] and old is not None
                        and (new is None or _moment(old['price_as_of']) > _moment(new['price_as_of']))):
                        latest[field] = old
                row = latest
                self._versions[path] = version
                if row['status'] not in _ACTIVE or kind != 'mark' and not row.get('active_tracking', True):
                    self._active.pop(path, None)
                    continue
                self._active[path] = row
                disk_last = self._read(path)['last_event']
                if disk_last:
                    self._durable_at[path] = _moment(disk_last['price_received_at'])
            if row is None or row['symbol'] != symbol or row['status'] not in _ACTIVE:
                continue
            if row['market_date'] != local.date().isoformat() and row['status'] != 'open':
                continue
            if row['market_date'] > local.date().isoformat() or price_as_of < _moment(row['tracking_started_at']):
                continue
            if row['status'] == 'pending' and kind == 'bid' or row['status'] == 'open' and kind == 'ask':
                continue
            local_row = row
            prior = row['last_event']
            if prior and (price_as_of <= _moment(prior['price_as_of']) or received_at < _moment(prior['price_received_at'])):
                continue
            # Durable marks are bounded; crossing detection still evaluates every accepted trade.
            durable_at = self._durable_at.get(path)
            force_save = kind == 'mark' or durable_at is None or received_at-durable_at >= timedelta(minutes=1)
            if row['status'] == 'open' and row['coverage'] == 'complete':
                force_save = force_save or price >= Decimal(row['target_price']) or price <= Decimal(row['stop_loss_price'])
            if row['status'] == 'pending':
                trigger = Decimal(row['trigger_price'])
                previous = row['previous_trade']
                ask_eligible = kind == 'ask' and (price >= trigger if row['buy_direction'] == 'at_or_above' else price <= trigger)
                chase = _number(row['chase_limit_price'])
                ask_eligible = ask_eligible and chase is not None and price <= chase
                force_save = force_save or ask_eligible or price == trigger or previous is not None and (
                    Decimal(previous['price']) < trigger <= price if row['buy_direction'] == 'at_or_above'
                    else Decimal(previous['price']) > trigger >= price
                )
            if not force_save:
                event = {'kind': kind, 'price': str(price), 'size': size, 'source': source,
                         'price_as_of': price_as_of.isoformat(), 'price_received_at': received_at.isoformat()}
                row['last_event'] = event
                if row['status'] == 'pending' and kind == 'trade':
                    row['previous_trade'] = event
                elif row['status'] == 'open':
                    row.update(self._pnl(row, row['entry'], price, realized=False))
                continue
            with self._locked(path):
                row = self._read(path)
                if row['status'] not in _ACTIVE:
                    self._active.pop(path, None)
                    continue
                if row['status'] == 'pending':
                    old, new = local_row['previous_trade'], row['previous_trade']
                    if old is not None and (new is None or _moment(old['price_as_of']) > _moment(new['price_as_of'])):
                        row['previous_trade'] = old
                last = row['last_event']
                if last and (price_as_of <= _moment(last['price_as_of']) or received_at < _moment(last['price_received_at'])):
                    continue
                event = {
                    'kind': kind, 'price': str(price), 'size': size, 'source': source,
                    'price_as_of': price_as_of.isoformat(), 'price_received_at': received_at.isoformat(),
                }
                updates = {'last_event': event}
                if row['status'] == 'pending':
                    trigger = Decimal(row['trigger_price'])
                    direction = row['buy_direction']
                    previous = row['previous_trade']
                    crossing = previous is not None and (
                        Decimal(previous['price']) < trigger <= price if direction == 'at_or_above'
                        else Decimal(previous['price']) > trigger >= price
                    )
                    eligible = (price == trigger or crossing) if kind == 'trade' else kind == 'ask' and (
                        price >= trigger if direction == 'at_or_above' else price <= trigger
                    )
                    chase = _number(row['chase_limit_price'])
                    eligible = eligible and (price <= chase if chase is not None else kind == 'trade')
                    eligible = eligible and Decimal(row['stop_loss_price']) < price < Decimal(row['target_price'])
                    if kind == 'trade':
                        updates['previous_trade'] = event
                    if eligible:
                        quantity = int(Decimal(row['quantity']))
                        filled = min(quantity, size) if kind == 'ask' else quantity
                        entry = {
                            **event, 'quantity': str(filled), 'requested_quantity': row['quantity'],
                            'execution_model': 'top_ask_simulation' if kind == 'ask' else 'trade_touch_simulation',
                            'liquidity_verified': kind == 'ask', 'evidence': 'directional_ask' if kind == 'ask' else 'exact' if price == trigger else 'crossed',
                            'quantity_source': row['quantity_source'], 'round_trip_cost_bps': row['round_trip_cost_bps'],
                            'quantity_account_count': row['quantity_account_count'], 'cost_source': row['cost_source'],
                            'quantity_basis': row['quantity_basis'], 'confidence_score_10': row['confidence_score_10'],
                        }
                        updates.update(status='open', entry=entry, filled_quantity=str(filled), unfilled_quantity=str(quantity-filled), active_tracking=row['coverage'] == 'complete')
                        updates.update(self._pnl(row, entry, price, realized=False))
                elif kind in {'trade', 'bid', 'mark'}:
                    entry = row['entry']
                    target = price >= Decimal(row['target_price'])
                    stop = price <= Decimal(row['stop_loss_price'])
                    # A missing interval may contain an earlier stop; never manufacture a later exit.
                    can_exit = row['coverage'] == 'complete' and (kind == 'trade' or kind == 'bid' and size >= int(entry['quantity']))
                    if can_exit and (target or stop):
                        reason = 'stop' if stop else 'target'
                        updates.update(status=f'{reason}_exited', exit_status=reason, exit={
                            **event, 'quantity': entry['quantity'], 'liquidity_verified': kind == 'bid',
                            'execution_model': 'top_bid_simulation' if kind == 'bid' else 'trade_touch_simulation',
                        }, active_tracking=False)
                        updates.update(self._pnl(row, entry, price, realized=True))
                    else:
                        updates.update(self._pnl(row, entry, price, realized=False))
                self._event(row, received_at=received_at, kind=kind, updates=updates, **{key: val for key, val in event.items() if key not in {'kind', 'price_received_at'}})
                self._save(path, row)

    @staticmethod
    def _pnl(row: dict, entry: dict, price: Decimal, *, realized: bool) -> dict:
        quantity, entered = Decimal(entry['quantity']), Decimal(entry['price'])
        gross = (price-entered)*quantity
        cost = (entered+price)*quantity*Decimal(row['round_trip_cost_bps'])/Decimal('20000')
        return {
            'cost_krw': str(cost) if row['coverage'] == 'complete' else None,
            'realized_gross_pnl_krw': str(gross) if realized else None,
            'realized_net_pnl_krw': str(gross-cost) if realized else None,
            'unrealized_gross_pnl_krw': None if realized or row['coverage'] != 'complete' else str(gross),
            'unrealized_net_pnl_krw': None if realized or row['coverage'] != 'complete' else str(gross-cost),
        }

    def history(self, day: date, *, as_of: datetime | None = None) -> list[dict]:
        as_of = as_of or datetime.combine(day, time(15, 30), KST)
        results = []
        for path in self._paths():
            if path.parent.name > day.isoformat():
                continue
            data = self._read(path)
            row = copy.deepcopy(data['initial'])
            if as_of is not None and _moment(row['tracking_started_at']) > as_of:
                continue
            events = []
            for event in data['events']:
                if as_of is None or _moment(event['price_received_at']) <= as_of:
                    row.update(event['updates'])
                    events.append({key: value for key, value in event.items() if key != 'updates'})
            if row['market_date'] != day.isoformat():
                if row['entry'] is None or _moment(row['entry']['price_as_of']).astimezone(KST).date() > day:
                    continue
                if row['exit'] is not None and _moment(row['exit']['price_as_of']).astimezone(KST).date() != day:
                    continue
            row['events'] = events
            results.append(row)
        return sorted(results, key=lambda row: row['decision_snapshot_id'])


wait_paper_ledger = WaitPaperLedger()
