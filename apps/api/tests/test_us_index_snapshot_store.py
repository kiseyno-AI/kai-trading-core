from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from threading import Barrier
from zoneinfo import ZoneInfo
from uuid import uuid4

import exchange_calendars
from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
from sqlalchemy import create_engine, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateSchema, DropSchema

from app.market.us_index_snapshot_store import load_us_index_captures, load_us_index_snapshot, save_us_index_snapshot
from app.market import us_index_snapshot_store as snapshot_store_module
from app.db import engine as core_test_engine
from app.db_models import USIndexInputSnapshot


@pytest.fixture
def payload():
    return {
        'rt_cd': '0', 'output1': {'stck_shrn_iscd': 'SPX', 'hts_kor_isnm': 'S&P500 지수'},
        'output2': [
            {'stck_bsop_date': '20261123', 'ovrs_nmix_prpr': '7100.123456789012345678'},
            {'stck_bsop_date': '20261120', 'ovrs_nmix_prpr': '7000.000001'},
        ],
    }


@pytest.fixture
def capture():
    return {
        'index_code': 'SPX', 'as_of': datetime(2026, 11, 24, 16, tzinfo=UTC),
        'collected_at': datetime(2026, 11, 24, 16, 0, 2, tzinfo=UTC),
        'source_environment': 'mock',
    }


def _session_factory(engine):
    @contextmanager
    def factory():
        with Session(engine) as session, session.begin():
            yield session
    return factory


@pytest.fixture
def database(tmp_path):
    url = f'sqlite+pysqlite:///{tmp_path / "index-inputs.sqlite"}'
    engine = create_engine(url)
    USIndexInputSnapshot.__table__.create(engine)
    yield url, engine, _session_factory(engine)
    engine.dispose()


def test_persists_exact_prices_and_capture_times_after_reopening_database(database, payload) -> None:
    url, engine, factory = database
    as_of = datetime(2026, 11, 24, 16, tzinfo=UTC)
    collected_at = datetime(2026, 11, 24, 16, 0, 2, tzinfo=UTC)

    saved = save_us_index_snapshot(
        payload, index_code='SPX', as_of=as_of, collected_at=collected_at,
        source_environment='mock', session_factory=factory,
    )
    engine.dispose()
    reopened = create_engine(url)
    try:
        loaded = load_us_index_snapshot(saved.snapshot_id, session_factory=_session_factory(reopened))
    finally:
        reopened.dispose()

    assert loaded == saved
    assert loaded.prices.price == Decimal('7100.123456789012345678')
    assert loaded.prices.previous_price == Decimal('7000.000001')
    assert loaded.as_of == as_of
    assert loaded.collected_at == collected_at
    assert loaded.created_at.utcoffset().total_seconds() == 0


def test_repeating_the_same_capture_returns_the_original_record(database, payload) -> None:
    _, _, factory = database
    arguments = {
        'index_code': 'SPX', 'as_of': datetime(2026, 11, 24, 16, tzinfo=UTC),
        'collected_at': datetime(2026, 11, 24, 16, 0, 2, tzinfo=UTC),
        'source_environment': 'mock', 'session_factory': factory,
    }

    first = save_us_index_snapshot(payload, **arguments)
    second = save_us_index_snapshot(payload, **arguments)

    assert second == first


def test_capture_recovery_is_scoped_to_clock_environment_and_exact_as_of(database, payload, capture):
    _, _, factory = database
    first = save_us_index_snapshot(payload, **capture, session_factory=factory)
    for changes in (
        {'source_environment': 'real'},
        {'as_of': capture['as_of'] + timedelta(seconds=1)},
        {'collected_at': capture['collected_at'] + timedelta(seconds=5)},
    ):
        save_us_index_snapshot(payload, **(capture | changes), session_factory=factory)

    recovered = load_us_index_captures(
        as_of=capture['as_of'], collected_by=capture['collected_at'] + timedelta(seconds=1),
        session=first.prices.session, source_environment='mock', session_factory=factory,
    )

    assert recovered == {'SPX': first}


def test_capture_is_readable_in_a_new_python_process(database, payload, capture) -> None:
    url, engine, factory = database
    saved = save_us_index_snapshot(payload, **capture, session_factory=factory)
    engine.dispose()
    script = '''
from contextlib import contextmanager
import json
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.market.us_index_snapshot_store import load_us_index_snapshot

engine = create_engine(sys.argv[1])
@contextmanager
def factory():
    with Session(engine) as session:
        yield session
record = load_us_index_snapshot(sys.argv[2], session_factory=factory)
print(json.dumps([record.snapshot_id, str(record.prices.price), record.collected_at.isoformat()]))
engine.dispose()
'''
    result = subprocess.run(
        [sys.executable, '-c', script, url, saved.snapshot_id],
        cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True, text=True,
    )

    assert json.loads(result.stdout) == [
        saved.snapshot_id, '7100.123456789012345678', capture['collected_at'].isoformat(),
    ]


@pytest.mark.parametrize('change', ['response', 'collection_time', 'environment'])
def test_new_capture_keeps_the_previous_snapshot(database, payload, capture, change) -> None:
    _, _, factory = database
    first = save_us_index_snapshot(payload, **capture, session_factory=factory)
    if change == 'response':
        payload['output2'][0]['ovrs_nmix_prpr'] = '7200.000000000000000001'
    elif change == 'collection_time':
        capture['collected_at'] += timedelta(seconds=1)
    else:
        capture['source_environment'] = 'real'

    second = save_us_index_snapshot(payload, **capture, session_factory=factory)

    assert second.snapshot_id != first.snapshot_id
    assert load_us_index_snapshot(first.snapshot_id, session_factory=factory) == first
    assert load_us_index_snapshot(second.snapshot_id, session_factory=factory) == second


def test_concurrent_duplicate_saves_return_one_identity(database, payload, capture) -> None:
    _, _, factory = database
    barrier = Barrier(4)

    def save(_):
        barrier.wait(timeout=5)
        return save_us_index_snapshot(payload, **capture, session_factory=factory)

    with ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(save, range(4)))

    assert all(record == records[0] for record in records)
    assert load_us_index_snapshot(records[0].snapshot_id, session_factory=factory) == records[0]


def test_equivalent_timestamps_and_key_orders_share_an_identity(database, payload, capture) -> None:
    _, _, factory = database
    first = save_us_index_snapshot(payload, **capture, session_factory=factory)
    capture['as_of'] = capture['as_of'].astimezone(ZoneInfo('Asia/Seoul'))
    capture['collected_at'] = capture['collected_at'].astimezone(ZoneInfo('America/New_York'))

    second = save_us_index_snapshot(dict(reversed(list(payload.items()))), **capture, session_factory=factory)

    assert second == first


def test_source_payload_and_frozen_calendar_survive_later_changes(database, payload, capture, monkeypatch) -> None:
    _, _, factory = database
    saved = save_us_index_snapshot(payload, **capture, session_factory=factory)
    assert json.loads(saved.source_payload_json) == payload
    assert hashlib.sha256(saved.source_payload_json.encode('utf-8')).hexdigest() == saved.source_payload_sha256
    payload['output2'][0]['ovrs_nmix_prpr'] = '1'

    def unavailable_calendar(*args, **kwargs):
        raise RuntimeError('calendar unavailable during later read')

    monkeypatch.setattr(exchange_calendars, 'get_calendar', unavailable_calendar)

    assert load_us_index_snapshot(saved.snapshot_id, session_factory=factory) == saved


@pytest.mark.parametrize('field', ['as_of', 'collected_at'])
@pytest.mark.parametrize('value', [None, '2026-11-24T16:00:00Z', datetime(2026, 11, 24, 16)])
def test_invalid_capture_times_do_not_open_the_database(payload, capture, field, value) -> None:
    capture[field] = value

    def forbidden_factory():
        raise AssertionError('database opened for invalid capture')

    with pytest.raises(ValueError, match='timezone-aware'):
        save_us_index_snapshot(payload, **capture, session_factory=forbidden_factory)


def test_reversed_capture_times_do_not_open_the_database(payload, capture) -> None:
    capture['collected_at'] = capture['as_of'] - timedelta(seconds=1)

    def forbidden_factory():
        raise AssertionError('database opened for invalid capture')

    with pytest.raises(ValueError, match='cannot precede'):
        save_us_index_snapshot(payload, **capture, session_factory=forbidden_factory)


@pytest.mark.parametrize('environment', ['', 'production', None, False])
def test_unknown_source_environment_is_rejected_before_storage(payload, capture, environment) -> None:
    capture['source_environment'] = environment

    def forbidden_factory():
        raise AssertionError('database opened for invalid environment')

    with pytest.raises(ValueError, match='source_environment'):
        save_us_index_snapshot(payload, **capture, session_factory=forbidden_factory)


@pytest.mark.parametrize('problem', ['missing', 'invalid', 'non_json'])
def test_unusable_response_is_never_saved(payload, capture, problem) -> None:
    expected_error = ValueError
    if problem == 'missing':
        payload['output2'].pop()
        expected_error = LookupError
    elif problem == 'invalid':
        payload['output2'][0]['ovrs_nmix_prpr'] = '0'
    else:
        payload['unsupported'] = object()

    def forbidden_factory():
        raise AssertionError('database opened for unusable response')

    with pytest.raises(expected_error):
        save_us_index_snapshot(payload, **capture, session_factory=forbidden_factory)


def test_unknown_snapshot_is_not_created_on_read(database) -> None:
    _, _, factory = database

    assert load_us_index_snapshot('0' * 64, session_factory=factory) is None


def test_corrupted_snapshot_is_not_returned_as_valid_data(database, payload, capture) -> None:
    _, _, factory = database
    saved = save_us_index_snapshot(payload, **capture, session_factory=factory)
    with factory() as session:
        session.execute(update(USIndexInputSnapshot).where(
            USIndexInputSnapshot.snapshot_id == saved.snapshot_id,
        ).values(snapshot_json='{}'))

    with pytest.raises(ValueError, match='hash mismatch'):
        load_us_index_snapshot(saved.snapshot_id, session_factory=factory)


@pytest.mark.parametrize('field', ['index_code', 'session_date', 'as_of', 'collected_at'])
def test_changed_indexed_metadata_is_not_returned_as_valid_data(database, payload, capture, field) -> None:
    _, _, factory = database
    saved = save_us_index_snapshot(payload, **capture, session_factory=factory)
    values = {
        'index_code': 'SOX',
        'session_date': saved.prices.session.session_date + timedelta(days=1),
        'as_of': saved.as_of - timedelta(seconds=1),
        'collected_at': saved.collected_at + timedelta(seconds=1),
    }
    with factory() as session:
        session.execute(update(USIndexInputSnapshot).where(
            USIndexInputSnapshot.snapshot_id == saved.snapshot_id,
        ).values({field: values[field]}))

    with pytest.raises(ValueError, match='metadata mismatch'):
        load_us_index_snapshot(saved.snapshot_id, session_factory=factory)


def test_calendar_version_is_part_of_the_snapshot_vintage(database, payload, capture, monkeypatch) -> None:
    _, _, factory = database
    first = save_us_index_snapshot(payload, **capture, session_factory=factory)
    monkeypatch.setattr(snapshot_store_module, 'version', lambda name: 'test-next-calendar-version')

    second = save_us_index_snapshot(payload, **capture, session_factory=factory)

    assert second.snapshot_id != first.snapshot_id
    assert second.calendar_version == 'test-next-calendar-version'
    assert load_us_index_snapshot(first.snapshot_id, session_factory=factory) == first


def test_storage_failure_does_not_become_an_in_memory_success(payload, capture) -> None:
    @contextmanager
    def failed_factory():
        raise OSError('test database unavailable')
        yield

    with pytest.raises(OSError, match='unavailable'):
        save_us_index_snapshot(payload, **capture, session_factory=failed_factory)
    with pytest.raises(OSError, match='unavailable'):
        load_us_index_snapshot('0' * 64, session_factory=failed_factory)


@pytest.fixture
def postgres_database():
    if core_test_engine.dialect.name != 'postgresql':
        pytest.skip('PostgreSQL integration requires PostgreSQL')
    schema = f'us_index_snapshots_test_{uuid4().hex}'
    path = Path(__file__).resolve().parents[1] / 'db_migrations/versions/20261124_0091_us_index_input_snapshots.py'
    spec = importlib.util.spec_from_file_location(path.stem, path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    with core_test_engine.begin() as connection:
        connection.execute(CreateSchema(schema))
        connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()

    @contextmanager
    def factory():
        with core_test_engine.begin() as connection:
            connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
            with Session(bind=connection, join_transaction_mode='create_savepoint') as session, session.begin():
                yield session

    try:
        yield factory
    finally:
        with core_test_engine.begin() as connection:
            connection.execute(DropSchema(schema, cascade=True))


def test_postgresql_round_trip_preserves_hash_and_decimal_text(postgres_database, payload, capture) -> None:
    saved = save_us_index_snapshot(payload, **capture, session_factory=postgres_database)

    assert load_us_index_snapshot(saved.snapshot_id, session_factory=postgres_database) == saved
    assert saved.prices.price == Decimal('7100.123456789012345678')
    assert load_us_index_captures(
        as_of=capture['as_of'], collected_by=capture['collected_at'], session=saved.prices.session,
        source_environment=capture['source_environment'], session_factory=postgres_database,
    ) == {'SPX': saved}


@pytest.mark.parametrize('statement', [
    'UPDATE us_index_input_snapshots SET index_code = \'SOX\' WHERE snapshot_id = :snapshot_id',
    'DELETE FROM us_index_input_snapshots WHERE snapshot_id = :snapshot_id',
    'TRUNCATE TABLE us_index_input_snapshots',
])
def test_postgresql_guards_reject_mutation(postgres_database, payload, capture, statement) -> None:
    saved = save_us_index_snapshot(payload, **capture, session_factory=postgres_database)

    with pytest.raises(DBAPIError, match='immutable'):
        with postgres_database() as session:
            session.execute(text(statement), {'snapshot_id': saved.snapshot_id})

    assert load_us_index_snapshot(saved.snapshot_id, session_factory=postgres_database) == saved


def test_postgresql_concurrent_duplicate_saves_are_idempotent(postgres_database, payload, capture) -> None:
    barrier = Barrier(4)

    def save(_):
        barrier.wait(timeout=10)
        return save_us_index_snapshot(payload, **capture, session_factory=postgres_database)

    with ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(save, range(4)))

    assert all(record == records[0] for record in records)
