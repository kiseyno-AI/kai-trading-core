from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys
from threading import Barrier
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session

from app.db_models import USIndexInputSnapshot
from app.internal.exact_repair_manifest import canonical_json_sha256
from app.market.us_index_input_freeze import freeze_us_index_inputs, load_frozen_us_index_inputs
from app.market.us_index_snapshot_store import save_us_index_snapshot


KST = ZoneInfo('Asia/Seoul')
TARGET_DATE = date(2026, 11, 25)
AS_OF = datetime(2026, 11, 25, 7, tzinfo=KST)


class Clock:
    def __init__(self, minute=10):
        self.value = AS_OF.replace(minute=minute)

    def __call__(self):
        return self.value


@pytest.fixture
def database(tmp_path):
    engine = create_engine(f'sqlite+pysqlite:///{tmp_path / "freeze.sqlite"}')
    USIndexInputSnapshot.__table__.create(engine)

    @contextmanager
    def factory():
        with Session(engine) as session, session.begin():
            yield session

    yield factory
    engine.dispose()


@pytest.fixture
def captures(database):
    records = {}
    for code in ('SPX', 'COMP', 'SOX'):
        records[code] = save_us_index_snapshot(
            {'rt_cd': '0', 'output1': {'stck_shrn_iscd': code}, 'output2': [
                {'stck_bsop_date': '20261124', 'ovrs_nmix_prpr': '7100.123456789012345678'},
                {'stck_bsop_date': '20261123', 'ovrs_nmix_prpr': '7000'},
            ]},
            index_code=code, as_of=AS_OF, collected_at=AS_OF.replace(minute=1),
            source_environment='real', session_factory=database,
        )
    return records


def test_freezes_exact_committed_inputs_with_a_durable_selection_and_seal(tmp_path, database, captures):
    ids = {code: record.snapshot_id for code, record in captures.items()}
    frozen = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE, snapshot_ids=ids, state_dir=tmp_path,
        session_factory=database, clock=Clock(),
    )

    assert frozen.snapshot_ids == ids
    assert frozen.target_trade_date == TARGET_DATE
    assert frozen.selected_at.isoformat() == '2026-11-24T22:10:00+00:00'
    assert frozen.confirmed_at == frozen.selected_at
    assert frozen.snapshots[0].prices.price == Decimal('7100.123456789012345678')
    folder = tmp_path / 'frozen/real-2026-11-25'
    assert json.loads((folder / 'input.json').read_text())['manifest_id'] == frozen.manifest_id
    assert json.loads((folder / 'seal.json').read_text())['manifest_id'] == frozen.manifest_id


def test_first_seal_is_replayed_instead_of_selecting_revised_values(tmp_path, database, captures):
    ids = {code: record.snapshot_id for code, record in captures.items()}
    first = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE, snapshot_ids=ids, state_dir=tmp_path,
        session_factory=database, clock=Clock(),
    )
    folder = tmp_path / 'frozen/real-2026-11-25'
    original_input = (folder / 'input.json').read_bytes()
    original_seal = (folder / 'seal.json').read_bytes()
    revised_payload = json.loads(captures['SPX'].source_payload_json)
    revised_payload['output2'][0]['ovrs_nmix_prpr'] = '7200'
    revised = save_us_index_snapshot(
        revised_payload, index_code='SPX', as_of=AS_OF, collected_at=AS_OF.replace(minute=11),
        source_environment='real', session_factory=database,
    )

    replayed = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE, snapshot_ids=ids | {'SPX': revised.snapshot_id},
        state_dir=tmp_path, session_factory=database, clock=Clock(11),
    )

    assert replayed == first
    assert (folder / 'input.json').read_bytes() == original_input
    assert (folder / 'seal.json').read_bytes() == original_seal


@pytest.mark.parametrize('minute', [12, 13])
def test_backdated_receipts_cannot_create_a_new_freeze_after_cutoff(tmp_path, captures, minute):
    def no_database():
        raise AssertionError('Post-cutoff input selection is forbidden')

    frozen = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE,
        snapshot_ids={code: record.snapshot_id for code, record in captures.items()},
        state_dir=tmp_path, session_factory=no_database, clock=Clock(minute),
    )
    assert frozen is None
    assert not (tmp_path / 'frozen/real-2026-11-25').exists()


def test_selection_must_finish_reading_committed_ids_before_cutoff(tmp_path, database, captures):
    clock = Clock()
    reads = 0

    @contextmanager
    def delayed_read():
        nonlocal reads
        with database() as session:
            yield session
        reads += 1
        if reads == 3:
            clock.value = clock.value.replace(minute=12)

    frozen = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE,
        snapshot_ids={code: record.snapshot_id for code, record in captures.items()},
        state_dir=tmp_path, session_factory=delayed_read, clock=clock,
    )
    assert frozen is None
    assert not (tmp_path / 'frozen/real-2026-11-25/input.json').exists()


def test_manifest_must_be_durable_before_cutoff_to_receive_a_seal(tmp_path, database, captures, monkeypatch):
    clock = Clock()
    replace = os.replace

    def late_manifest(source, destination):
        replace(source, destination)
        if destination.name == 'input.json':
            clock.value = clock.value.replace(minute=12)

    monkeypatch.setattr(os, 'replace', late_manifest)
    frozen = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE,
        snapshot_ids={code: record.snapshot_id for code, record in captures.items()},
        state_dir=tmp_path, session_factory=database, clock=clock,
    )
    assert frozen is None
    assert (tmp_path / 'frozen/real-2026-11-25/input.json').exists()
    assert not (tmp_path / 'frozen/real-2026-11-25/seal.json').exists()
    assert load_frozen_us_index_inputs(target_trade_date=TARGET_DATE, state_dir=tmp_path) is None


@pytest.mark.parametrize('day', [date(2026, 11, 28), date(2026, 1, 2), date(2026, 11, 27)])
def test_calendar_noops_do_not_select_or_publish_inputs(tmp_path, captures, day):
    clock = Clock()
    clock.value = datetime.combine(day, clock.value.timetz())

    def no_database():
        raise AssertionError('Closed/special/no-new-session dates must not select inputs')

    assert freeze_us_index_inputs(
        target_trade_date=day, snapshot_ids={code: record.snapshot_id for code, record in captures.items()},
        state_dir=tmp_path, session_factory=no_database, clock=clock,
    ) is None


def test_only_the_original_sealed_ids_are_read_after_cutoff(tmp_path, database, captures):
    ids = {code: record.snapshot_id for code, record in captures.items()}
    first = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE, snapshot_ids=ids, state_dir=tmp_path,
        session_factory=database, clock=Clock(),
    )
    replayed = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE, snapshot_ids={}, state_dir=tmp_path,
        session_factory=database, clock=Clock(13),
    )
    assert replayed == first
    assert load_frozen_us_index_inputs(
        target_trade_date=TARGET_DATE, state_dir=tmp_path, session_factory=database,
    ) == first


def test_late_seal_publication_keeps_the_proven_pre_cutoff_selection(tmp_path, database, captures, monkeypatch):
    clock = Clock()
    replace = os.replace

    def late_seal(source, destination):
        replace(source, destination)
        if destination.name == 'seal.json':
            clock.value = clock.value.replace(minute=12)

    monkeypatch.setattr(os, 'replace', late_seal)
    frozen = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE,
        snapshot_ids={code: record.snapshot_id for code, record in captures.items()},
        state_dir=tmp_path, session_factory=database, clock=clock,
    )
    assert frozen.confirmed_at < AS_OF.replace(minute=12)
    assert frozen.snapshot_ids == {code: record.snapshot_id for code, record in captures.items()}


@pytest.mark.parametrize('missing', ['SPX', 'COMP', 'SOX'])
def test_missing_index_does_not_publish_a_partial_selection(tmp_path, captures, missing):
    def no_database():
        raise AssertionError('Incomplete ID sets must not query the database')

    assert freeze_us_index_inputs(
        target_trade_date=TARGET_DATE,
        snapshot_ids={code: record.snapshot_id for code, record in captures.items() if code != missing},
        state_dir=tmp_path, session_factory=no_database, clock=Clock(),
    ) is None
    assert not (tmp_path / 'frozen/real-2026-11-25/input.json').exists()


def test_naive_manifest_timestamps_are_rejected_even_with_valid_hashes(tmp_path, database, captures):
    freeze_us_index_inputs(
        target_trade_date=TARGET_DATE,
        snapshot_ids={code: record.snapshot_id for code, record in captures.items()},
        state_dir=tmp_path, session_factory=database, clock=Clock(),
    )
    folder = tmp_path / 'frozen/real-2026-11-25'
    data = json.loads((folder / 'input.json').read_text())
    data.pop('manifest_id')
    data['selected_at'] = '2026-11-24T22:10:00'
    data['manifest_id'] = canonical_json_sha256(data)
    (folder / 'input.json').write_text(json.dumps(data))
    seal = json.loads((folder / 'seal.json').read_text())
    seal.pop('seal_hash')
    seal['manifest_id'] = data['manifest_id']
    seal['seal_hash'] = canonical_json_sha256(seal)
    (folder / 'seal.json').write_text(json.dumps(seal))

    with pytest.raises(ValueError, match='timezone-aware'):
        load_frozen_us_index_inputs(
            target_trade_date=TARGET_DATE, state_dir=tmp_path, session_factory=database,
        )


def test_malformed_stored_ids_fail_before_database_access(tmp_path, database, captures):
    freeze_us_index_inputs(
        target_trade_date=TARGET_DATE,
        snapshot_ids={code: record.snapshot_id for code, record in captures.items()},
        state_dir=tmp_path, session_factory=database, clock=Clock(),
    )
    folder = tmp_path / 'frozen/real-2026-11-25'
    data = json.loads((folder / 'input.json').read_text())
    data.pop('manifest_id')
    data['snapshot_ids']['SPX'] = 123
    data['manifest_id'] = canonical_json_sha256(data)
    (folder / 'input.json').write_text(json.dumps(data))

    def no_database():
        raise AssertionError('Malformed IDs must fail before database access')

    with pytest.raises(ValueError, match='snapshot_ids'):
        load_frozen_us_index_inputs(
            target_trade_date=TARGET_DATE, state_dir=tmp_path, session_factory=no_database,
        )


def test_new_directory_links_are_synced_before_confirmation(tmp_path, database, captures, monkeypatch):
    open_fd, close_fd, sync_fd = os.open, os.close, os.fsync
    descriptors = {}
    synced = set()
    folder = (tmp_path / 'frozen/real-2026-11-25').resolve()
    required = {folder, folder.parent, folder.parent.parent, folder.parent.parent.parent}

    def tracked_open(path, *args, **kwargs):
        descriptor = open_fd(path, *args, **kwargs)
        descriptors[descriptor] = Path(path).resolve()
        return descriptor

    def tracked_sync(descriptor):
        path = descriptors.get(descriptor)
        if path is not None and path.is_dir():
            synced.add(path)
        return sync_fd(descriptor)

    def tracked_close(descriptor):
        descriptors.pop(descriptor, None)
        return close_fd(descriptor)

    monkeypatch.setattr(os, 'open', tracked_open)
    monkeypatch.setattr(os, 'fsync', tracked_sync)
    monkeypatch.setattr(os, 'close', tracked_close)

    def clock():
        if (folder / 'input.json').exists():
            assert required <= synced
        return AS_OF.replace(minute=10)

    frozen = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE,
        snapshot_ids={code: record.snapshot_id for code, record in captures.items()},
        state_dir=tmp_path, session_factory=database, clock=clock,
    )
    assert frozen is not None
    assert required <= synced


@pytest.mark.parametrize('minute', [11, 12])
def test_interrupted_seal_keeps_its_candidate_and_only_resumes_before_cutoff(
    tmp_path, database, captures, monkeypatch, minute,
):
    replace = os.replace

    def fail_seal(source, destination):
        if destination.name == 'seal.json':
            raise OSError('simulated seal failure')
        return replace(source, destination)

    monkeypatch.setattr(os, 'replace', fail_seal)
    ids = {code: record.snapshot_id for code, record in captures.items()}
    with pytest.raises(OSError):
        freeze_us_index_inputs(
            target_trade_date=TARGET_DATE, snapshot_ids=ids, state_dir=tmp_path,
            session_factory=database, clock=Clock(),
        )
    folder = tmp_path / 'frozen/real-2026-11-25'
    candidate = (folder / 'input.json').read_bytes()
    manifest_id = json.loads(candidate)['manifest_id']
    monkeypatch.setattr(os, 'replace', replace)

    def no_database():
        raise AssertionError('A candidate alone cannot establish pre-cutoff availability afterward')

    resumed = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE, snapshot_ids={'SPX': '0' * 64}, state_dir=tmp_path,
        session_factory=database if minute == 11 else no_database, clock=Clock(minute),
    )
    if minute == 11:
        assert resumed.manifest_id == manifest_id
        assert resumed.snapshot_ids == ids
    else:
        assert resumed is None
        assert not (folder / 'seal.json').exists()
    assert (folder / 'input.json').read_bytes() == candidate


@pytest.mark.parametrize('mismatch', ['environment', 'as_of', 'future_receipt', 'index'])
def test_mixed_capture_identity_cannot_be_sealed(tmp_path, database, captures, mismatch):
    original = captures['SPX']
    arguments = {
        'index_code': 'SPX', 'as_of': AS_OF, 'collected_at': AS_OF.replace(minute=1),
        'source_environment': 'real',
    }
    if mismatch == 'environment':
        arguments['source_environment'] = 'mock'
    elif mismatch == 'as_of':
        arguments['as_of'] = AS_OF.replace(minute=1)
    elif mismatch == 'future_receipt':
        arguments['collected_at'] = AS_OF.replace(minute=11)
    replacement = save_us_index_snapshot(
        json.loads(original.source_payload_json), **arguments, session_factory=database,
    )
    ids = {code: record.snapshot_id for code, record in captures.items()}
    ids['SPX'] = captures['COMP'].snapshot_id if mismatch == 'index' else replacement.snapshot_id

    with pytest.raises(ValueError, match='binding'):
        freeze_us_index_inputs(
            target_trade_date=TARGET_DATE, snapshot_ids=ids, state_dir=tmp_path,
            session_factory=database, clock=Clock(),
        )
    assert not (tmp_path / 'frozen/real-2026-11-25/input.json').exists()


def test_missing_linked_snapshot_is_corruption_without_latest_data_fallback(tmp_path, database, captures):
    ids = {code: record.snapshot_id for code, record in captures.items()}
    freeze_us_index_inputs(
        target_trade_date=TARGET_DATE, snapshot_ids=ids, state_dir=tmp_path,
        session_factory=database, clock=Clock(),
    )
    with database() as session:
        session.execute(delete(USIndexInputSnapshot).where(USIndexInputSnapshot.snapshot_id == ids['SPX']))
    with pytest.raises(ValueError, match='binding'):
        load_frozen_us_index_inputs(
            target_trade_date=TARGET_DATE, state_dir=tmp_path, session_factory=database,
        )


def test_read_replay_never_recalculates_trading_calendars(tmp_path, database, captures, monkeypatch):
    import exchange_calendars
    from app.market.session import KRX_EXCHANGE_CALENDAR

    frozen = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE,
        snapshot_ids={code: record.snapshot_id for code, record in captures.items()},
        state_dir=tmp_path, session_factory=database, clock=Clock(),
    )

    def no_calendar(*args, **kwargs):
        raise AssertionError('Replay must use stored session evidence')

    monkeypatch.setattr(exchange_calendars, 'get_calendar', no_calendar)
    monkeypatch.setattr(KRX_EXCHANGE_CALENDAR, 'is_session', no_calendar)
    assert load_frozen_us_index_inputs(
        target_trade_date=TARGET_DATE, state_dir=tmp_path, session_factory=database,
    ) == frozen


@pytest.mark.parametrize('filename', ['input.json', 'seal.json'])
def test_corrupted_sealed_evidence_is_not_replaced_or_reinterpreted(tmp_path, database, captures, filename):
    ids = {code: record.snapshot_id for code, record in captures.items()}
    freeze_us_index_inputs(
        target_trade_date=TARGET_DATE, snapshot_ids=ids, state_dir=tmp_path,
        session_factory=database, clock=Clock(),
    )
    path = tmp_path / 'frozen/real-2026-11-25' / filename
    payload = json.loads(path.read_text())
    payload['schema_version'] = 'unexpected'
    path.write_text(json.dumps(payload))
    corrupted = path.read_bytes()
    with pytest.raises(ValueError):
        freeze_us_index_inputs(
            target_trade_date=TARGET_DATE, snapshot_ids=ids, state_dir=tmp_path,
            session_factory=database, clock=Clock(11),
        )
    assert path.read_bytes() == corrupted


def test_concurrent_different_selections_have_one_winner(tmp_path, database, captures):
    ids = {code: record.snapshot_id for code, record in captures.items()}
    payload = json.loads(captures['SPX'].source_payload_json)
    payload['output2'][0]['ovrs_nmix_prpr'] = '7200'
    revised = save_us_index_snapshot(
        payload, index_code='SPX', as_of=AS_OF, collected_at=AS_OF.replace(minute=2),
        source_environment='real', session_factory=database,
    )
    alternatives = [ids, ids | {'SPX': revised.snapshot_id}]
    barrier = Barrier(2)

    def attempt(selected):
        barrier.wait(timeout=10)
        return freeze_us_index_inputs(
            target_trade_date=TARGET_DATE, snapshot_ids=selected, state_dir=tmp_path,
            session_factory=database, clock=Clock(),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, alternatives))
    winner = load_frozen_us_index_inputs(
        target_trade_date=TARGET_DATE, state_dir=tmp_path, session_factory=database,
    )
    assert winner is not None
    assert winner.snapshot_ids in alternatives
    assert all(record is None or record == winner for record in results)


def test_a_fresh_python_process_replays_stored_ids_and_exact_prices(tmp_path, database, captures):
    frozen = freeze_us_index_inputs(
        target_trade_date=TARGET_DATE,
        snapshot_ids={code: record.snapshot_id for code, record in captures.items()},
        state_dir=tmp_path, session_factory=database, clock=Clock(),
    )
    script = '''
from contextlib import contextmanager
from datetime import date
import json
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from app.market.us_index_input_freeze import load_frozen_us_index_inputs
engine = create_engine(sys.argv[1])
@contextmanager
def factory():
    with Session(engine) as session, session.begin():
        yield session
record = load_frozen_us_index_inputs(
    target_trade_date=date(2026, 11, 25), state_dir=sys.argv[2], session_factory=factory,
)
print(json.dumps({'manifest_id': record.manifest_id, 'snapshot_ids': record.snapshot_ids,
                  'price': str(record.snapshots[0].prices.price)}))
engine.dispose()
'''
    result = subprocess.run(
        [sys.executable, '-c', script, f'sqlite+pysqlite:///{tmp_path / "freeze.sqlite"}', str(tmp_path)],
        cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True, text=True,
    )
    replayed = json.loads(result.stdout)
    assert replayed['manifest_id'] == frozen.manifest_id
    assert replayed['snapshot_ids'] == frozen.snapshot_ids
    assert replayed['price'] == '7100.123456789012345678'
