import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Context, Decimal, localcontext
from functools import partial
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db_models import USIndexInputSnapshot
from app.market.us_index_collector import collect_us_index_inputs
from app.market.us_index_preopen import run_us_index_preopen
from app.market.us_index_input_freeze import freeze_us_index_inputs, load_frozen_us_index_inputs
from app.market.us_index_features import calculate_us_index_features
from app.market.us_index_snapshot_store import load_us_index_snapshot, save_us_index_snapshot


AS_OF = datetime(2026, 11, 25, 7, tzinfo=ZoneInfo('Asia/Seoul'))
DATES = (
    '20261026', '20261027', '20261028', '20261029', '20261030',
    '20261102', '20261103', '20261104', '20261105', '20261106',
    '20261109', '20261110', '20261111', '20261112', '20261113',
    '20261116', '20261117', '20261118', '20261119', '20261120', '20261123', '20261124',
)


def payload(code='SPX', event=3):
    with localcontext(Context(prec=100)):
        levels = [Decimal(100)]
        for change in [-1, 1] * 10 + [event]:
            levels.append(levels[-1] * (1 + Decimal(change) / 100))
    return {
        'rt_cd': '0', 'output1': {'stck_shrn_iscd': code},
        'output2': [
            {'stck_bsop_date': day, 'ovrs_nmix_prpr': str(close)}
            for day, close in reversed(list(zip(DATES, levels)))
        ],
    }


@pytest.fixture
def database(tmp_path):
    engine = create_engine(f'sqlite+pysqlite:///{tmp_path / "history.sqlite"}')
    USIndexInputSnapshot.__table__.create(engine)

    @contextmanager
    def factory():
        with Session(engine) as session, session.begin():
            yield session

    yield factory
    engine.dispose()


def test_a_current_capture_persists_twenty_prior_returns_from_twenty_two_closes(database):
    arguments = {
        'index_code': 'SPX', 'as_of': AS_OF, 'collected_at': AS_OF + timedelta(minutes=1),
        'source_environment': 'real', 'session_factory': database,
    }
    legacy = save_us_index_snapshot(payload(), **arguments)
    saved = save_us_index_snapshot(payload(), **arguments, include_history=True)

    assert len(saved.history_prices) == 20
    assert saved.history_prices[0].session.session_date.isoformat() == '2026-10-27'
    assert saved.history_prices[0].session.previous_session_date.isoformat() == '2026-10-26'
    assert saved.history_prices[-1].session.session_date.isoformat() == '2026-11-23'
    assert saved.history_prices[0].session.close_at.isoformat() == '2026-10-27T20:00:00+00:00'
    assert saved.history_prices[-1].session.close_at.isoformat() == '2026-11-23T21:00:00+00:00'
    assert load_us_index_snapshot(saved.snapshot_id, session_factory=database) == saved
    assert load_us_index_snapshot(legacy.snapshot_id, session_factory=database) == legacy
    assert saved.snapshot_id != legacy.snapshot_id


class FakeKIS:
    use_mock = False

    def __init__(self):
        self.calls = []
        self.responses = {}

    async def fetch_overseas_index_daily_prices(self, **kwargs):
        self.calls.append(kwargs)
        code = kwargs['index_code']
        return self.responses.get(code, payload(code, {'SPX': 3, 'COMP': 4, 'SOX': 5}[code]))


def test_history_collection_widens_each_single_request_without_extra_fetches(database):
    client = FakeKIS()
    result = asyncio.run(collect_us_index_inputs(
        as_of=AS_OF, client=client, session_factory=database,
        clock=lambda: AS_OF + timedelta(minutes=1), include_history=True,
    ))

    assert result['data_complete'] is True
    assert result['input_kind'] == 'history_22'
    assert [call['index_code'] for call in client.calls] == ['SPX', 'COMP', 'SOX']
    assert all(call['start_date'] == '20261026' and call['end_date'] == '20261124' for call in client.calls)
    assert all(len(load_us_index_snapshot(item['snapshot_id'], session_factory=database).history_prices) == 20
               for item in result['indices'])


def test_history_recovery_skips_legacy_captures_but_shares_the_spent_request_slots(database, tmp_path):
    client = FakeKIS()
    now = [AS_OF]
    arguments = {'state_dir': tmp_path, 'client': client, 'session_factory': database, 'clock': lambda: now[0]}
    legacy = run_us_index_preopen(**arguments)
    assert legacy['data_complete'] is True
    assert len(client.calls) == 3

    spent = run_us_index_preopen(**arguments, include_history=True)
    assert spent['reason'] == 'already_attempted'
    assert spent['data_complete'] is False
    assert len(client.calls) == 3

    now[0] += timedelta(minutes=5)
    complete = run_us_index_preopen(**arguments, include_history=True)
    assert complete['data_complete'] is True
    assert complete['input_kind'] == 'history_22'
    assert len(client.calls) == 6
    assert all(complete['snapshot_ids'][code] != legacy['snapshot_ids'][code] for code in ('SPX', 'COMP', 'SOX'))
    now[0] += timedelta(minutes=5)
    assert run_us_index_preopen(**arguments, include_history=True)['snapshot_ids'] == complete['snapshot_ids']
    assert len(client.calls) == 6


def stored_ids(database, *, include_history):
    return {
        code: save_us_index_snapshot(
            payload(code, event), index_code=code, as_of=AS_OF, collected_at=AS_OF + timedelta(minutes=1),
            source_environment='real', session_factory=database, include_history=include_history,
        ).snapshot_id for code, event in (('SPX', 3), ('COMP', 4), ('SOX', 5))
    }


def test_full_history_is_sealed_separately_without_rewriting_legacy_evidence(database, tmp_path):
    arguments = {
        'target_trade_date': AS_OF.date(), 'state_dir': tmp_path,
        'session_factory': database, 'clock': lambda: AS_OF + timedelta(minutes=10),
    }
    legacy = freeze_us_index_inputs(snapshot_ids=stored_ids(database, include_history=False), **arguments)
    original_files = {file: file.read_bytes() for file in (tmp_path / 'frozen').rglob('*.json')}

    history = freeze_us_index_inputs(
        snapshot_ids=stored_ids(database, include_history=True), **arguments, include_history=True,
    )

    assert history.input_kind == 'history_22'
    assert history.manifest_id != legacy.manifest_id
    assert len(history.snapshots[0].history_prices) == 20
    manifest = json.loads((tmp_path / 'frozen-history/real-2026-11-25/input.json').read_text())
    assert manifest['schema_version'] == 'us_index_history_input_v1'
    assert manifest['input_kind'] == 'history_22'
    assert load_frozen_us_index_inputs(
        target_trade_date=AS_OF.date(), state_dir=tmp_path, session_factory=database,
        include_history=True,
    ) == history
    assert load_frozen_us_index_inputs(
        target_trade_date=AS_OF.date(), state_dir=tmp_path, session_factory=database,
    ) == legacy
    assert {file: file.read_bytes() for file in (tmp_path / 'frozen').rglob('*.json')} == original_files


def test_one_current_history_seal_supplies_all_lagged_features_without_fake_past_captures(database, tmp_path):
    ids = stored_ids(database, include_history=True)
    arguments = {
        'target_trade_date': AS_OF.date(), 'snapshot_ids': ids, 'state_dir': tmp_path,
        'session_factory': database, 'clock': lambda: AS_OF + timedelta(minutes=10),
    }
    current = freeze_us_index_inputs(**arguments, include_history=True)

    result = calculate_us_index_features(current)

    assert result['schema_version'] == 'us_index_features_v2'
    assert result['history_source'] == 'current_snapshot_history'
    assert result['history_inputs'] == []
    assert result['input_manifest_id'] == current.manifest_id
    assert result['history_count'] == 20
    assert result['history_unavailable_reason'] is None
    assert result['indices']['SPX']['return_pct'] == '3'
    assert result['indices']['SPX']['acceleration_pp'] == '2'
    assert float(result['indices']['SPX']['shock_z']) == pytest.approx(2.924038303442689)
    assert result['indices']['SPX']['percentile_pct'] == '100'
    assert not (tmp_path / 'frozen').exists()

    legacy_projection = freeze_us_index_inputs(**arguments)
    legacy_result = calculate_us_index_features(legacy_projection)
    assert legacy_result['schema_version'] == 'us_index_features_v1'
    assert legacy_result['history_count'] == 0
    assert legacy_result['indices']['SPX']['shock_z'] is None


def test_manual_history_collection_is_an_explicit_single_request_opt_in(database, capsys):
    from app.internal.us_index_collect import main

    client = FakeKIS()
    assert main(
        ['--indices', 'SPX', '--as-of', AS_OF.isoformat(), '--include-history'],
        collector=partial(collect_us_index_inputs, client=client, session_factory=database,
                          clock=lambda: AS_OF + timedelta(minutes=1)),
    ) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['data_complete'] is True
    assert result['input_kind'] == 'history_22'
    assert len(client.calls) == 1
    assert len(load_us_index_snapshot(result['indices'][0]['snapshot_id'], session_factory=database).history_prices) == 20


def test_history_cli_workflow_collects_seals_and_replays_only_the_selected_contract(database, tmp_path, capsys):
    from app.internal.us_index_preopen import main as preopen_main
    from app.internal.us_index_features import main as features_main

    client = FakeKIS()
    arguments = ['--state-dir', str(tmp_path), '--include-history']
    runner = partial(run_us_index_preopen, client=client, session_factory=database, clock=lambda: AS_OF)
    freezer = partial(freeze_us_index_inputs, session_factory=database, clock=lambda: AS_OF)
    assert preopen_main(arguments, runner=runner, freezer=freezer) == 0
    collected = json.loads(capsys.readouterr().out)
    assert collected['data_complete'] is True
    assert collected['input_freeze']['input_kind'] == 'history_22'
    assert len(client.calls) == 3

    reader = partial(load_frozen_us_index_inputs, session_factory=database)
    query = ['--target-trade-date', AS_OF.date().isoformat(), '--state-dir', str(tmp_path), '--include-history']
    assert features_main(query, loader=reader) == 0
    features = json.loads(capsys.readouterr().out)
    assert features['schema_version'] == 'us_index_features_v2'
    assert features['input_manifest_id'] == collected['input_freeze']['manifest_id']
    assert features['history_count'] == 20
    assert features['indices']['SPX']['return_pct'] == '3'
    assert preopen_main(arguments, runner=runner, freezer=freezer) == 0
    assert json.loads(capsys.readouterr().out)['input_freeze'] == collected['input_freeze']
    assert len(client.calls) == 3


def test_missing_history_is_normal_and_only_the_missing_index_is_retried(database, tmp_path):
    client = FakeKIS()
    partial_payload = payload('COMP', 4)
    partial_payload['output2'].pop()
    client.responses['COMP'] = partial_payload
    now = [AS_OF]
    arguments = {
        'state_dir': tmp_path, 'client': client, 'session_factory': database,
        'clock': lambda: now[0], 'include_history': True,
    }
    first = run_us_index_preopen(**arguments)
    assert first['status'] == 'completed'
    assert first['data_complete'] is False
    assert set(first['snapshot_ids']) == {'SPX', 'SOX'}
    now[0] += timedelta(minutes=5)
    client.responses.clear()
    recovered = run_us_index_preopen(**arguments)
    assert recovered['data_complete'] is True
    assert [call['index_code'] for call in client.calls] == ['SPX', 'COMP', 'SOX', 'COMP']
    assert all(recovered['snapshot_ids'][code] == first['snapshot_ids'][code] for code in ('SPX', 'SOX'))


def test_legacy_and_history_modes_cannot_exceed_the_shared_nine_fetch_budget(database, tmp_path):
    client = FakeKIS()
    for code in ('SPX', 'COMP', 'SOX'):
        response = payload(code)
        response['output2'] = response['output2'][:2]
        client.responses[code] = response
    now = [AS_OF]
    arguments = {'state_dir': tmp_path, 'client': client, 'session_factory': database, 'clock': lambda: now[0]}
    assert run_us_index_preopen(**arguments)['data_complete'] is True
    for minute in (0, 5, 5, 10, 10, 11):
        now[0] = AS_OF + timedelta(minutes=minute)
        assert run_us_index_preopen(**arguments, include_history=True)['data_complete'] is False
    assert len(client.calls) == 9
    state = json.loads((tmp_path / 'real-2026-11-25.json').read_text())
    assert set(state['attempts']) == {'07:00', '07:05', '07:10'}


def test_history_freeze_rejects_a_mixture_of_two_close_and_full_history_captures(database, tmp_path):
    ids = stored_ids(database, include_history=True)
    ids['COMP'] = stored_ids(database, include_history=False)['COMP']
    with pytest.raises(ValueError):
        freeze_us_index_inputs(
            target_trade_date=AS_OF.date(), snapshot_ids=ids, state_dir=tmp_path,
            session_factory=database, clock=lambda: AS_OF + timedelta(minutes=10), include_history=True,
        )
    assert not (tmp_path / 'frozen-history/real-2026-11-25/input.json').exists()


def test_a_late_history_capture_cannot_be_sealed_but_an_existing_seal_replays(database, tmp_path):
    ids = stored_ids(database, include_history=True)

    def no_database():
        raise AssertionError('No new selection after cutoff')

    assert freeze_us_index_inputs(
        target_trade_date=AS_OF.date(), snapshot_ids=ids, state_dir=tmp_path, session_factory=no_database,
        clock=lambda: AS_OF + timedelta(minutes=12), include_history=True,
    ) is None
    original = freeze_us_index_inputs(
        target_trade_date=AS_OF.date(), snapshot_ids=ids, state_dir=tmp_path, session_factory=database,
        clock=lambda: AS_OF + timedelta(minutes=10), include_history=True,
    )
    assert freeze_us_index_inputs(
        target_trade_date=AS_OF.date(), snapshot_ids={}, state_dir=tmp_path, session_factory=database,
        clock=lambda: AS_OF + timedelta(days=3), include_history=True,
    ) == original


def test_history_query_cannot_fall_back_to_a_legacy_seal(database, tmp_path, capsys):
    from app.internal.us_index_features import main

    freeze_us_index_inputs(
        target_trade_date=AS_OF.date(), snapshot_ids=stored_ids(database, include_history=False),
        state_dir=tmp_path, session_factory=database, clock=lambda: AS_OF + timedelta(minutes=10),
    )
    assert main(
        ['--target-trade-date', AS_OF.date().isoformat(), '--state-dir', str(tmp_path), '--include-history'],
        loader=partial(load_frozen_us_index_inputs, session_factory=database),
    ) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'unavailable'


def test_history_and_explicit_old_seals_cannot_be_combined(database, tmp_path, capsys):
    from app.internal.us_index_features import main

    current = freeze_us_index_inputs(
        target_trade_date=AS_OF.date(), snapshot_ids=stored_ids(database, include_history=True),
        state_dir=tmp_path, session_factory=database, clock=lambda: AS_OF + timedelta(minutes=10), include_history=True,
    )
    with pytest.raises(ValueError, match='explicit history'):
        calculate_us_index_features(current, history=(current,))
    with pytest.raises(SystemExit) as error:
        main(['--target-trade-date', AS_OF.date().isoformat(), '--include-history', '--history-date', '2026-11-24'])
    assert error.value.code == 2


def test_stored_incomplete_history_fails_even_when_its_content_hash_is_valid(database):
    saved = save_us_index_snapshot(
        payload(), index_code='SPX', as_of=AS_OF, collected_at=AS_OF + timedelta(minutes=1),
        source_environment='real', session_factory=database, include_history=True,
    )
    with database() as session:
        row = session.get(USIndexInputSnapshot, saved.snapshot_id)
        data = json.loads(row.snapshot_json)
        data['history_prices'].pop()
        row.snapshot_json = json.dumps(data, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        row.snapshot_id = hashlib.sha256(row.snapshot_json.encode()).hexdigest()
        damaged_id = row.snapshot_id
    with pytest.raises(ValueError):
        load_us_index_snapshot(damaged_id, session_factory=database)


def test_a_fresh_history_replay_needs_no_provider_or_calendar(database, tmp_path):
    current = freeze_us_index_inputs(
        target_trade_date=AS_OF.date(), snapshot_ids=stored_ids(database, include_history=True),
        state_dir=tmp_path, session_factory=database, clock=lambda: AS_OF + timedelta(minutes=10),
        include_history=True,
    )
    expected = calculate_us_index_features(current)
    script = '''
from contextlib import contextmanager
from functools import partial
import sys
import exchange_calendars
from exchange_calendars.exchange_calendar_xkrx import XKRXExchangeCalendar
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
def no_calendar(*args, **kwargs):
    raise AssertionError('History replay must use stored session metadata')
exchange_calendars.get_calendar = no_calendar
XKRXExchangeCalendar.__init__ = no_calendar
from app.internal.us_index_features import main
from app.market.us_index_input_freeze import load_frozen_us_index_inputs
assert 'app.market.us_index_preopen' not in sys.modules
assert 'app.brokers.kis.provider' not in sys.modules
engine = create_engine(sys.argv[1])
@contextmanager
def factory():
    with Session(engine) as session, session.begin():
        yield session
code = main(['--target-trade-date', '2026-11-25', '--include-history', '--state-dir', sys.argv[2]],
            loader=partial(load_frozen_us_index_inputs, session_factory=factory))
engine.dispose()
raise SystemExit(code)
'''
    result = subprocess.run(
        [sys.executable, '-c', script, f'sqlite+pysqlite:///{tmp_path / "history.sqlite"}', str(tmp_path)],
        cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True, text=True, timeout=20,
    )
    assert json.loads(result.stdout) == expected


def test_saving_history_does_not_hide_bad_older_data_behind_a_missing_current_close(database):
    response = payload()
    response['output2'] = [row for row in response['output2'] if row['stck_bsop_date'] != '20261124']
    response['output2'][-1]['ovrs_nmix_prpr'] = '-1'
    with pytest.raises(ValueError):
        save_us_index_snapshot(
            response, index_code='SPX', as_of=AS_OF, collected_at=AS_OF + timedelta(minutes=1),
            source_environment='real', session_factory=database, include_history=True,
        )
