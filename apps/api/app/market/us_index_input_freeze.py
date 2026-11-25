from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import partial
from pathlib import Path
import os
import re
from zoneinfo import ZoneInfo

from app.db import session_scope
from app.internal.exact_repair_manifest import (
    canonical_json_sha256, checkpoint_file_lock, load_json_mapping, write_checkpoint,
)
from app.market.us_index_prices import SUPPORTED_US_INDEX_CODES
from app.market.us_index_snapshot_store import USIndexInputSnapshotRecord, _utc, load_us_index_snapshot


KST = ZoneInfo('Asia/Seoul')


@dataclass(frozen=True)
class USIndexInputFreezeRecord:
    manifest_id: str
    target_trade_date: date
    source_environment: str
    as_of: datetime
    cutoff_at: datetime
    selected_at: datetime
    confirmed_at: datetime
    snapshots: tuple[USIndexInputSnapshotRecord, ...]

    @property
    def snapshot_ids(self) -> dict[str, str]:
        return {record.prices.index_code: record.snapshot_id for record in self.snapshots}


def _folder(state_dir, target_trade_date: date, source_environment: str) -> Path:
    if type(target_trade_date) is not date or source_environment not in ('real', 'mock'):
        raise ValueError('A target date and real/mock source environment are required')
    return Path(state_dir) / 'frozen' / f'{source_environment}-{target_trade_date.isoformat()}'


def _timestamp(value) -> datetime:
    try:
        return _utc(datetime.fromisoformat(value), 'US index manifest timestamp')
    except (TypeError, ValueError):
        raise ValueError('US index manifest timestamps must be timezone-aware ISO strings') from None


def _snapshot_ids(value) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError('snapshot_ids must be a mapping')
    ids = dict(value)
    if set(ids) - set(SUPPORTED_US_INDEX_CODES) or any(
        not isinstance(item, str) or not re.fullmatch(r'[0-9a-f]{64}', item) for item in ids.values()
    ):
        raise ValueError('snapshot_ids must contain supported indices and SHA-256 IDs')
    return ids


def _sync_parent_directories(folder: Path) -> None:
    # Sync the directory entries too; the leaf alone cannot preserve a newly created path.
    directory = folder.resolve()
    while directory.parent != directory:
        parent = directory.parent
        if parent.stat().st_dev != directory.stat().st_dev:
            break
        descriptor = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        directory = parent


def _validate_snapshots(snapshots, as_of, selected_at, environment) -> None:
    for code, snapshot in zip(SUPPORTED_US_INDEX_CODES, snapshots):
        if snapshot is None or (
            snapshot.prices.index_code != code or snapshot.as_of != as_of
            or snapshot.source_environment != environment
            or not as_of <= snapshot.collected_at <= selected_at
            or snapshot.prices.session != snapshots[0].prices.session
        ):
            raise ValueError('US index input snapshot binding mismatch')


def _selection(folder: Path, target_trade_date: date, environment: str, session_factory):
    data = load_json_mapping(folder / 'input.json', label='US index input selection')
    body = {key: value for key, value in data.items() if key != 'manifest_id'}
    if data.get('manifest_id') != canonical_json_sha256(body) or body.get('schema_version') != 'us_index_input_v1':
        raise ValueError('US index input manifest hash/version mismatch')
    if body.get('target_trade_date') != target_trade_date.isoformat() or body.get('source_environment') != environment:
        raise ValueError('US index input manifest identity mismatch')
    ids = _snapshot_ids(body.get('snapshot_ids'))
    if set(ids) != set(SUPPORTED_US_INDEX_CODES):
        raise ValueError('US index input manifest requires all three indices')
    as_of = _timestamp(body['as_of'])
    cutoff = _timestamp(body['cutoff_at'])
    selected = _timestamp(body['selected_at'])
    expected_as_of = datetime.combine(target_trade_date, time(7), tzinfo=KST).astimezone(UTC)
    if as_of != expected_as_of or cutoff != as_of + timedelta(minutes=12) or not as_of <= selected < cutoff:
        raise ValueError('US index input manifest time window mismatch')
    snapshots = tuple(load_us_index_snapshot(ids[code], session_factory=session_factory)
                      for code in SUPPORTED_US_INDEX_CODES)
    _validate_snapshots(snapshots, as_of, selected, environment)
    return data, snapshots


def load_frozen_us_index_inputs(
    *, target_trade_date: date, source_environment: str = 'real',
    state_dir: str | Path = 'var/us-index-preopen', session_factory=session_scope,
) -> USIndexInputFreezeRecord | None:
    """Replay exact sealed IDs without collecting or selecting newer data."""
    folder = _folder(state_dir, target_trade_date, source_environment)
    if not (folder / 'seal.json').exists():
        return None
    seal = load_json_mapping(folder / 'seal.json', label='US index input seal')
    body = {key: value for key, value in seal.items() if key != 'seal_hash'}
    if seal.get('seal_hash') != canonical_json_sha256(body) or body.get('schema_version') != 'us_index_seal_v1':
        raise ValueError('US index input seal hash/version mismatch')
    data, snapshots = _selection(folder, target_trade_date, source_environment, session_factory)
    selected_at = _timestamp(data['selected_at'])
    cutoff = _timestamp(data['cutoff_at'])
    confirmed_at = _timestamp(body['confirmed_at'])
    if body.get('manifest_id') != data['manifest_id'] or not selected_at <= confirmed_at < cutoff:
        raise ValueError('US index input seal binding/time mismatch')
    return USIndexInputFreezeRecord(
        data['manifest_id'], target_trade_date, source_environment,
        _timestamp(data['as_of']), cutoff, selected_at, confirmed_at, snapshots,
    )


def freeze_us_index_inputs(
    *, target_trade_date: date, snapshot_ids: Mapping[str, str], source_environment: str = 'real',
    state_dir: str | Path = 'var/us-index-preopen', session_factory=session_scope,
    clock: Callable[[], datetime] = partial(datetime.now, UTC),
) -> USIndexInputFreezeRecord | None:
    """Seal observed committed IDs before cutoff, or replay the existing selection."""
    folder = _folder(state_dir, target_trade_date, source_environment)
    existing = load_frozen_us_index_inputs(
        target_trade_date=target_trade_date, source_environment=source_environment,
        state_dir=state_dir, session_factory=session_factory,
    )
    if existing is not None:
        return existing
    as_of = datetime.combine(target_trade_date, time(7), tzinfo=KST).astimezone(UTC)
    cutoff = as_of + timedelta(minutes=12)
    if not as_of <= _utc(clock(), 'clock') < cutoff:
        return None
    # Replay must not initialize scheduling calendars or provider clients.
    from app.market.us_index_preopen import resolve_us_index_preopen_session

    session, skip_reason = resolve_us_index_preopen_session(target_trade_date)
    if skip_reason is not None:
        return None
    with checkpoint_file_lock(folder / 'seal.json') as acquired:
        if not acquired:
            return None
        existing = load_frozen_us_index_inputs(
            target_trade_date=target_trade_date, source_environment=source_environment,
            state_dir=state_dir, session_factory=session_factory,
        )
        if existing is not None:
            return existing
        if not as_of <= _utc(clock(), 'clock') < cutoff:
            return None
        if (folder / 'input.json').exists():
            data, snapshots = _selection(folder, target_trade_date, source_environment, session_factory)
            selected_at = _timestamp(data['selected_at'])
        else:
            ids = _snapshot_ids(snapshot_ids)
            if len(ids) != len(SUPPORTED_US_INDEX_CODES):
                return None
            snapshots = tuple(load_us_index_snapshot(ids[code], session_factory=session_factory)
                              for code in SUPPORTED_US_INDEX_CODES)
            selected_at = _utc(clock(), 'clock')
            if not as_of <= selected_at < cutoff:
                return None
            _validate_snapshots(snapshots, as_of, selected_at, source_environment)
            data = {
                'schema_version': 'us_index_input_v1', 'target_trade_date': target_trade_date.isoformat(),
                'source_environment': source_environment, 'as_of': as_of.isoformat(),
                'cutoff_at': cutoff.isoformat(), 'selected_at': selected_at.isoformat(), 'snapshot_ids': ids,
            }
            data['manifest_id'] = canonical_json_sha256(data)
        if snapshots[0].prices.session != session:
            raise ValueError('US index selection calendar window mismatch')
        if not selected_at <= _utc(clock(), 'clock') < cutoff:
            return None
        write_checkpoint(folder / 'input.json', data)
        _sync_parent_directories(folder)
        confirmed_at = _utc(clock(), 'clock')
        if not selected_at <= confirmed_at < cutoff:
            return None
        seal = {
            'schema_version': 'us_index_seal_v1', 'manifest_id': data['manifest_id'],
            'confirmed_at': confirmed_at.isoformat(),
        }
        write_checkpoint(folder / 'seal.json', {**seal, 'seal_hash': canonical_json_sha256(seal)})
        return load_frozen_us_index_inputs(
            target_trade_date=target_trade_date, source_environment=source_environment,
            state_dir=state_dir, session_factory=session_factory,
        )
