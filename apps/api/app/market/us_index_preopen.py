import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo

from app.brokers.kis.client import KISClient
from app.brokers.kis.provider import kis_real_client
from app.core.numbers import finite_float
from app.db import session_scope
from app.internal.exact_repair_manifest import checkpoint_file_lock, load_json_mapping, write_checkpoint
from app.market.session import KRX_EXCHANGE_CALENDAR
from app.market.us_index_collector import collect_us_index_inputs
from app.market.us_index_prices import SUPPORTED_US_INDEX_CODES
from app.market.us_index_snapshot_store import load_us_index_captures
from app.market.us_session import latest_completed_us_session


KST = ZoneInfo('Asia/Seoul')
SCHEMA_VERSION = 'us_index_preopen_v1'
COLLECTION_SLOTS = (time(7), time(7, 5), time(7, 10))
MAX_COLLECTION_SECONDS = 90


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _read_clock(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError('clock must return a timezone-aware datetime')
    return value.astimezone(UTC)


async def _collect_pending(
    *, codes, as_of, deadline, client, session_factory, clock, budget_seconds,
) -> dict[str, Any]:
    started = monotonic()
    indices = []
    result: dict[str, Any] = {'status': 'completed', 'indices': indices}
    for code in codes:
        now = _read_clock(clock)
        if not as_of <= now < deadline:
            result.update(status='expired', reason='outside_window')
            break
        remaining = min((deadline - now).total_seconds(), budget_seconds - (monotonic() - started))
        if remaining <= 0:
            result.update(status='failed', reason='budget_exhausted')
            break
        try:
            single = await asyncio.wait_for(collect_us_index_inputs(
                as_of=as_of, index_codes=(code,), client=client,
                session_factory=session_factory, clock=lambda: _read_clock(clock), deadline=deadline,
            ), timeout=remaining)
        except TimeoutError:
            indices.append({
                'index_code': code, 'status': 'failed', 'stage': 'collect', 'error_type': 'TimeoutError',
            })
            result.update(status='failed', reason='budget_exhausted')
            break
        indices.extend(single['indices'])
    if _read_clock(clock) >= deadline:
        result.update(status='expired', reason='outside_window')
    elif result['status'] == 'completed' and any(item['status'] == 'failed' for item in indices):
        result['status'] = 'failed' if all(item['status'] == 'failed' for item in indices) else 'partial'
    return result


def run_us_index_preopen(
    *, state_dir: str | Path = 'var/us-index-preopen', client: KISClient | None = None,
    session_factory=session_scope, clock: Callable[[], datetime] = _utc_now,
    budget_seconds: float = MAX_COLLECTION_SECONDS,
) -> dict[str, Any]:
    """Collect optional preopen inputs in a standalone process."""
    budget_seconds = finite_float(budget_seconds)
    if budget_seconds is None or not 0 < budget_seconds <= MAX_COLLECTION_SECONDS:
        raise ValueError('budget_seconds must be positive and at most 90')
    now = _read_clock(clock).astimezone(KST)
    if not time(7) <= now.time() < time(7, 12):
        return {'status': 'not_due', 'reason': 'outside_window', 'target_trade_date': now.date().isoformat()}
    as_of = now.replace(hour=7, minute=0, second=0, microsecond=0).astimezone(UTC)
    deadline = as_of + timedelta(minutes=12)
    target_date = now.date().isoformat()
    if not KRX_EXCHANGE_CALENDAR.is_session(target_date):
        return {'status': 'not_due', 'reason': 'krx_closed', 'target_trade_date': target_date}
    if (
        KRX_EXCHANGE_CALENDAR.session_open(target_date).tz_convert(KST).time() != time(9)
        or KRX_EXCHANGE_CALENDAR.session_close(target_date).tz_convert(KST).time() != time(15, 30)
    ):
        return {'status': 'not_due', 'reason': 'special_krx_hours', 'target_trade_date': target_date}
    session = latest_completed_us_session(as_of=as_of)
    previous_krx_day = KRX_EXCHANGE_CALENDAR.previous_session(target_date).date()
    previous_us_session = latest_completed_us_session(
        as_of=datetime.combine(previous_krx_day, time(7), tzinfo=KST),
    )
    if session.session_date <= previous_us_session.session_date:
        return {'status': 'not_due', 'reason': 'no_new_us_session', 'target_trade_date': target_date}
    client = client if client is not None else kis_real_client
    environment = 'mock' if client.use_mock else 'real'
    path = Path(state_dir) / f'{environment}-{target_date}.json'
    state = {
        'schema_version': SCHEMA_VERSION, 'target_trade_date': target_date,
        'as_of': as_of.isoformat(), 'source_environment': environment, 'attempts': {},
    }
    # shortcut: claims coordinate one host/state directory; use DB claims before multi-host scheduling.
    with checkpoint_file_lock(path) as acquired:
        if not acquired:
            return {'status': 'not_due', 'reason': 'busy', 'target_trade_date': target_date}
        if path.exists():
            existing = load_json_mapping(path, label='US index preopen checkpoint')
            if any(existing.get(key) != value for key, value in state.items() if key != 'attempts'):
                raise ValueError('US index preopen checkpoint contract mismatch')
            attempts = existing.get('attempts')
            if not isinstance(attempts, dict) or any(
                key not in ('07:00', '07:05', '07:10') or not isinstance(value, dict)
                for key, value in attempts.items()
            ):
                raise ValueError('US index preopen checkpoint attempts are invalid')
            state = existing
        captures = load_us_index_captures(
            as_of=as_of, collected_by=now, session=session,
            source_environment=environment, session_factory=session_factory,
        )
        now = _read_clock(clock).astimezone(KST)
        if not as_of <= now < deadline:
            return {'status': 'not_due', 'reason': 'outside_window', 'target_trade_date': target_date}
        snapshot_ids = {code: record.snapshot_id for code, record in captures.items()}
        pending = [code for code in SUPPORTED_US_INDEX_CODES if code not in captures]
        if not pending:
            return {
                'status': 'completed', 'data_complete': True, 'target_trade_date': target_date,
                'snapshot_ids': snapshot_ids,
            }
        slot = max(slot for slot in COLLECTION_SLOTS if slot <= now.time()).strftime('%H:%M')
        if slot in state['attempts']:
            return {
                'status': 'not_due', 'reason': 'already_attempted', 'target_trade_date': target_date,
                'slot': slot, 'data_complete': False, 'snapshot_ids': snapshot_ids,
            }
        state['attempts'][slot] = {'started_at': now.isoformat(), 'index_codes': pending}
        write_checkpoint(path, state)
        try:
            result = asyncio.run(_collect_pending(
                codes=pending, as_of=as_of, deadline=deadline, client=client,
                session_factory=session_factory, clock=clock, budget_seconds=budget_seconds,
            ))
        except Exception as exc:
            result = {'status': 'failed', 'stage': 'collect', 'error_type': type(exc).__name__, 'indices': []}
        state['attempts'][slot].update(finished_at=_read_clock(clock).isoformat(), result=result)
        write_checkpoint(path, state)
    snapshot_ids.update({item['index_code']: item['snapshot_id'] for item in result['indices']
                         if item['status'] == 'saved'
                         and datetime.fromisoformat(item['collected_at']) < deadline})
    return {
        'status': result['status'],
        'data_complete': len(snapshot_ids) == len(SUPPORTED_US_INDEX_CODES) and result['status'] != 'expired',
        'target_trade_date': target_date, 'slot': slot, 'snapshot_ids': snapshot_ids,
        **({'reason': result['reason']} if 'reason' in result else {}),
    }
