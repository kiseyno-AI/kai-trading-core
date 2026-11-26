"""Restart-safe, independent scheduling; never enters the daily agent pipeline."""

from collections.abc import Callable
from datetime import UTC, date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from app.internal.exact_repair_manifest import (
    canonical_json_sha256, checkpoint_file_lock, load_json_mapping, write_checkpoint,
)
from app.market.us_index_input_freeze import freeze_us_index_inputs, load_frozen_us_index_inputs
from app.market.us_index_features import calculate_us_index_features
from app.market.us_index_preopen import _read_clock, resolve_us_index_preopen_session, run_us_index_preopen
from app.overnight_rebound.rules import evaluate_us_rebound


KST = ZoneInfo('Asia/Seoul')
STRATEGY_ID = 'krx_us_rebound'


def _folder(state_dir, day, environment):
    if type(day) is not date or environment not in ('real', 'mock'):
        raise ValueError('A session date and real/mock environment are required')
    return Path(state_dir) / 'rebound' / f'{environment}-{day.isoformat()}'


def _load_evaluation(path, day, environment):
    if not path.exists():
        return None
    value = load_json_mapping(path, label='US rebound evaluation')
    body = {key: item for key, item in value.items() if key != 'evaluation_id'}
    if value.get('evaluation_id') != canonical_json_sha256(body) or (
        value.get('schema_version') != 'us_rebound_evaluation_v1'
        or value.get('strategy_id') != STRATEGY_ID or value.get('order_authority') is not False
    ):
        raise ValueError('US rebound evaluation contract/hash mismatch')
    if value.get('target_trade_date') != day.isoformat() or value.get('source_environment') != environment:
        raise ValueError('US rebound evaluation identity mismatch')
    return value


def load_us_rebound_status(
    *, target_trade_date: date, state_dir='var/us-index-preopen', source_environment='real',
) -> dict:
    """Read persisted status without provider calls, collection or execution."""
    folder = _folder(state_dir, target_trade_date, source_environment)
    evaluation = _load_evaluation(folder / 'evaluation.json', target_trade_date, source_environment)
    last_tick = None
    if (folder / 'run.json').exists():
        last_tick = load_json_mapping(folder / 'run.json', label='US rebound run status')
        if (
            last_tick.get('target_trade_date') != target_trade_date.isoformat()
            or last_tick.get('source_environment') != source_environment
            or last_tick.get('strategy_id') != STRATEGY_ID
        ):
            raise ValueError('US rebound run identity mismatch')
    return {
        'strategy_id': STRATEGY_ID, 'target_trade_date': target_trade_date.isoformat(),
        'source_environment': source_environment,
        'status': 'completed' if evaluation is not None else (last_tick or {}).get('status', 'not_started'),
        'evaluation': evaluation, 'last_tick': last_tick,
    }


def run_us_rebound_tick(
    *, mode='disabled', enabled_from: date | None = None, state_dir='var/us-index-preopen',
    source_environment='real', clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    collector=run_us_index_preopen, loader=load_frozen_us_index_inputs, freezer=freeze_us_index_inputs,
) -> dict:
    """Advance only this strategy's debt; disabled/future sessions perform no I/O."""
    if mode not in ('disabled', 'observe'):
        raise ValueError('Unsupported US rebound mode')
    now = _read_clock(clock).astimezone(KST)
    result = {'strategy_id': STRATEGY_ID, 'target_trade_date': now.date().isoformat(), 'mode': mode}
    if mode == 'disabled':
        return {**result, 'status': 'not_due', 'reason': 'disabled'}
    if type(enabled_from) is not date:
        raise ValueError('An explicit enabled_from session is required')
    if now.date() < enabled_from:
        return {**result, 'status': 'not_due', 'reason': 'before_enabled_session'}
    if not time(6, 45) <= now.time() <= time(15, 30):
        return {**result, 'status': 'not_due', 'reason': 'outside_window'}
    folder = _folder(state_dir, now.date(), source_environment)
    path = folder / 'evaluation.json'

    def report(**outcome):
        value = {**result, **outcome}
        write_checkpoint(folder / 'run.json', {
            **{key: value[key] for key in ('strategy_id', 'target_trade_date', 'mode', 'status')},
            'source_environment': source_environment, 'checked_at': _read_clock(clock).astimezone(KST).isoformat(),
            **{key: value[key] for key in ('reason', 'error_type') if key in value},
        })
        return value

    def advance():
        existing = _load_evaluation(path, now.date(), source_environment)
        if existing is not None:
            return report(status='completed', evaluation=existing)
        session, reason = resolve_us_index_preopen_session(now.date())
        if reason is None:
            if now.time() < time(7):
                return report(status='not_due', reason='awaiting_collection')
            if now.time() < time(7, 12):
                collection = collector(state_dir=state_dir, clock=clock, include_history=True)
                if collection.get('data_complete'):
                    if collection.get('source_environment') != source_environment:
                        raise ValueError('US rebound collection environment mismatch')
                    frozen = freezer(
                        target_trade_date=now.date(), snapshot_ids=collection['snapshot_ids'],
                        state_dir=state_dir, source_environment=source_environment,
                        include_history=True, clock=clock,
                    )
                    collection['input_manifest_id'] = frozen.manifest_id if frozen else None
                status = 'failed' if collection.get('status') in ('failed', 'partial') else 'collecting'
                return report(status=status, collection=collection)
            if now.time() < time(7, 15):
                return report(status='not_due', reason='awaiting_evaluation')
            if now.time() >= time(8, 40):
                outcome = {'status': 'UNAVAILABLE', 'reason': 'evaluation_expired', 'input_manifest_id': None}
            else:
                frozen = loader(
                    target_trade_date=now.date(), state_dir=state_dir,
                    source_environment=source_environment, include_history=True,
                )
                if frozen is None:
                    return report(status='unavailable', reason='missing_frozen_input')
                if frozen.target_trade_date != now.date() or frozen.source_environment != source_environment:
                    raise ValueError('US rebound frozen input identity mismatch')
                features = calculate_us_index_features(frozen)
                outcome = {**evaluate_us_rebound(features), 'features': features}
        else:
            outcome = {'status': 'NO_SIGNAL', 'reason': reason, 'input_manifest_id': None}
        completed_at = _read_clock(clock).astimezone(KST)
        if completed_at.date() != now.date() or completed_at < now:
            raise ValueError('US rebound clock moved outside its session')
        if reason is None and completed_at.time() >= time(8, 40):
            outcome = {'status': 'UNAVAILABLE', 'reason': 'evaluation_expired', 'input_manifest_id': None}
        evaluation = {
            **outcome,
            'schema_version': 'us_rebound_evaluation_v1', 'strategy_id': STRATEGY_ID,
            'target_trade_date': now.date().isoformat(), 'source_environment': source_environment,
            'completed_at': completed_at.isoformat(), 'order_authority': False,
        }
        evaluation['evaluation_id'] = canonical_json_sha256(evaluation)
        write_checkpoint(path, evaluation)
        return report(status='completed', evaluation=evaluation)

    with checkpoint_file_lock(path) as acquired:
        if not acquired:
            return {**result, 'status': 'not_due', 'reason': 'busy'}
        try:
            return advance()
        except Exception as exc:
            return report(status='failed', error_type=type(exc).__name__)
