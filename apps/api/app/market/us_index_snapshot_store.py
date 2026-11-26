from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
import hashlib
from importlib.metadata import version
import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.db import session_scope
from app.db_models import USIndexInputSnapshot
from app.market.us_index_prices import (
    USIndexSessionPrices, validate_kis_us_index_history, validate_kis_us_index_prices,
    validate_us_index_price_history,
)
from app.market.us_session import CompletedUSSession, latest_completed_us_session


@dataclass(frozen=True)
class USIndexInputSnapshotRecord:
    snapshot_id: str
    prices: USIndexSessionPrices
    as_of: datetime
    collected_at: datetime
    created_at: datetime
    source_environment: str
    source_payload_sha256: str
    source_payload_json: str
    calendar_version: str
    history_prices: tuple[USIndexSessionPrices, ...] = ()


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError):
        raise ValueError('US index snapshot requires a JSON-serializable payload') from None


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f'{name} must be a timezone-aware datetime')
    return value.astimezone(UTC)


def _database_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _decode_prices(data, index_code: str) -> USIndexSessionPrices:
    window = data['session']
    return USIndexSessionPrices(
        index_code=index_code,
        session=CompletedUSSession(
            session_date=date.fromisoformat(window['session_date']),
            close_at=datetime.fromisoformat(window['close_at']),
            previous_session_date=date.fromisoformat(window['previous_session_date']),
            previous_close_at=datetime.fromisoformat(window['previous_close_at']),
        ),
        price=Decimal(data['price']), previous_price=Decimal(data['previous_price']),
    )


def _price_payload(prices: USIndexSessionPrices) -> dict:
    return {
        'session': {key: value.isoformat() for key, value in asdict(prices.session).items()},
        'price': str(prices.price), 'previous_price': str(prices.previous_price),
    }


def _record(row: USIndexInputSnapshot) -> USIndexInputSnapshotRecord:
    if _sha256(row.snapshot_json) != row.snapshot_id:
        raise ValueError('US index snapshot hash mismatch')
    data = json.loads(row.snapshot_json)
    if data['schema_version'] not in (1, 2):
        raise ValueError('Unsupported US index snapshot schema version')
    prices = _decode_prices(data, data['index_code'])
    as_of = datetime.fromisoformat(data['as_of'])
    collected_at = datetime.fromisoformat(data['collected_at'])
    if (
        row.index_code != data['index_code']
        or row.session_date != prices.session.session_date
        or _database_utc(row.as_of) != as_of
        or _database_utc(row.collected_at) != collected_at
    ):
        raise ValueError('US index snapshot metadata mismatch')
    history = ()
    if data['schema_version'] == 2:
        if not isinstance(data.get('history_prices'), list):
            raise ValueError('US index history snapshot requires stored price pairs')
        history = tuple(_decode_prices(item, data['index_code']) for item in data['history_prices'])
        validate_us_index_price_history(prices, history)
        if prices.session.close_at > _utc(as_of, 'as_of') or _utc(collected_at, 'collected_at') < as_of:
            raise ValueError('US index history capture time mismatch')
    return USIndexInputSnapshotRecord(
        snapshot_id=row.snapshot_id,
        prices=prices,
        as_of=as_of,
        collected_at=collected_at,
        created_at=_database_utc(row.created_at),
        source_environment=data['source_environment'],
        source_payload_sha256=data['source_payload_sha256'],
        source_payload_json=data['source_payload_json'],
        calendar_version=data['calendar_version'],
        history_prices=history,
    )


def save_us_index_snapshot(
    payload: Mapping[str, Any], *, index_code: str, as_of: datetime,
    collected_at: datetime, source_environment: str, session_factory=session_scope,
    include_history: bool = False,
) -> USIndexInputSnapshotRecord:
    as_of = _utc(as_of, 'as_of')
    collected_at = _utc(collected_at, 'collected_at')
    if collected_at < as_of:
        raise ValueError('collected_at cannot precede as_of')
    if not isinstance(source_environment, str) or source_environment not in {'real', 'mock'}:
        raise ValueError('source_environment must be real or mock')
    source_json = _canonical_json(dict(payload) if isinstance(payload, Mapping) else payload)
    session = latest_completed_us_session(as_of=as_of)
    parsed = json.loads(source_json)
    history = validate_kis_us_index_history(
        parsed, index_code=index_code, session=session,
    ) if include_history else ()
    prices = validate_kis_us_index_prices(parsed, index_code=index_code, session=session)
    snapshot = {
        'schema_version': 2 if include_history else 1,
        'source': 'kis_overseas_index_daily',
        'source_environment': source_environment,
        'index_code': prices.index_code,
        'as_of': as_of.isoformat(),
        'collected_at': collected_at.isoformat(),
        **_price_payload(prices),
        'source_payload_json': source_json,
        'source_payload_sha256': _sha256(source_json),
        'calendar_version': version('exchange_calendars'),
    }
    if include_history:
        snapshot['history_prices'] = [_price_payload(item) for item in history]
    snapshot_json = _canonical_json(snapshot)
    snapshot_id = _sha256(snapshot_json)
    with session_factory() as db_session:
        insert = sqlite_insert if db_session.get_bind().dialect.name == 'sqlite' else pg_insert
        statement = insert(USIndexInputSnapshot).values(
            snapshot_id=snapshot_id, index_code=prices.index_code,
            session_date=session.session_date, as_of=as_of, collected_at=collected_at,
            snapshot_json=snapshot_json,
        )
        db_session.execute(statement.on_conflict_do_nothing(index_elements=['snapshot_id']))
        row = db_session.get(USIndexInputSnapshot, snapshot_id)
        if row is None:
            raise RuntimeError('US index snapshot was not persisted')
        return _record(row)


def load_us_index_snapshot(
    snapshot_id: str, *, session_factory=session_scope,
) -> USIndexInputSnapshotRecord | None:
    with session_factory() as db_session:
        row = db_session.get(USIndexInputSnapshot, snapshot_id)
        return _record(row) if row is not None else None


def load_us_index_captures(
    *, as_of: datetime, collected_by: datetime, session: CompletedUSSession,
    source_environment: str, session_factory=session_scope, include_history: bool = False,
) -> dict[str, USIndexInputSnapshotRecord]:
    """Recover the first committed capture per index in one exact collection window."""
    as_of = _utc(as_of, 'as_of')
    collected_by = _utc(collected_by, 'collected_by')
    if collected_by < as_of:
        raise ValueError('collected_by cannot precede as_of')
    if source_environment not in ('real', 'mock'):
        raise ValueError('source_environment must be real or mock')
    statement = select(USIndexInputSnapshot).where(
        USIndexInputSnapshot.as_of == as_of,
        USIndexInputSnapshot.session_date == session.session_date,
        USIndexInputSnapshot.collected_at >= as_of,
        USIndexInputSnapshot.collected_at <= collected_by,
    ).order_by(USIndexInputSnapshot.collected_at, USIndexInputSnapshot.snapshot_id)
    captures = {}
    with session_factory() as db_session:
        for row in db_session.scalars(statement):
            record = _record(row)
            if (
                record.source_environment == source_environment and record.prices.session == session
                and (not include_history or record.history_prices)
            ):
                captures.setdefault(record.prices.index_code, record)
    return captures
