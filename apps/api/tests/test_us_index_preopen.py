import asyncio
from contextlib import contextmanager
from datetime import datetime
from functools import partial
import json
import os
import time
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db_models import USIndexInputSnapshot
from app.internal.exact_repair_manifest import checkpoint_file_lock
from app.market.us_index_preopen import run_us_index_preopen
from app.market.us_index_snapshot_store import load_us_index_snapshot


KST = ZoneInfo('Asia/Seoul')


class Clock:
    def __init__(self, hour=7, minute=0):
        self.value = datetime(2026, 11, 25, hour, minute, tzinfo=KST)

    def __call__(self):
        return self.value


class FakeKIS:
    use_mock = False

    def __init__(self):
        self.calls = []
        self.responses = {}

    async def fetch_overseas_index_daily_prices(self, **kwargs):
        self.calls.append(kwargs)
        code = kwargs['index_code']
        return self.responses.get(code, {
            'rt_cd': '0', 'output1': {'stck_shrn_iscd': code},
            'output2': [
                {'stck_bsop_date': kwargs['end_date'], 'ovrs_nmix_prpr': '7100'},
                {'stck_bsop_date': kwargs['start_date'], 'ovrs_nmix_prpr': '7000'},
            ],
        })


@pytest.fixture
def database(tmp_path):
    engine = create_engine(f'sqlite+pysqlite:///{tmp_path / "preopen.sqlite"}')
    USIndexInputSnapshot.__table__.create(engine)

    @contextmanager
    def factory():
        with Session(engine) as session, session.begin():
            yield session

    yield factory
    engine.dispose()


def test_first_slot_collects_and_persists_only_this_krx_sessions_inputs(tmp_path, database):
    client = FakeKIS()
    result = run_us_index_preopen(
        state_dir=tmp_path, client=client, session_factory=database, clock=Clock(),
    )

    assert result['status'] == 'completed'
    assert result['data_complete'] is True
    assert result['target_trade_date'] == '2026-11-25'
    assert result['slot'] == '07:00'
    assert [call['index_code'] for call in client.calls] == ['SPX', 'COMP', 'SOX']
    for code, snapshot_id in result['snapshot_ids'].items():
        saved = load_us_index_snapshot(snapshot_id, session_factory=database)
        assert saved.prices.index_code == code
        assert saved.prices.session.session_date.isoformat() == '2026-11-24'
        assert saved.as_of.isoformat() == '2026-11-24T22:00:00+00:00'
    checkpoint = json.loads((tmp_path / 'real-2026-11-25.json').read_text())
    assert list(checkpoint['attempts']) == ['07:00']


def test_restart_retries_only_the_missing_index_and_retains_saved_ids(tmp_path, database):
    clock = Clock()
    first_client = FakeKIS()
    first_client.responses['COMP'] = {'rt_cd': '0', 'output2': []}
    first = run_us_index_preopen(
        state_dir=tmp_path, client=first_client, session_factory=database, clock=clock,
    )
    assert first['status'] == 'completed'
    assert first['data_complete'] is False

    clock.value = clock.value.replace(minute=5)
    restarted_client = FakeKIS()
    second = run_us_index_preopen(
        state_dir=tmp_path, client=restarted_client, session_factory=database, clock=clock,
    )

    assert [call['index_code'] for call in restarted_client.calls] == ['COMP']
    assert second['status'] == 'completed'
    assert second['data_complete'] is True
    assert second['snapshot_ids']['SPX'] == first['snapshot_ids']['SPX']
    assert second['snapshot_ids']['SOX'] == first['snapshot_ids']['SOX']
    checkpoint = json.loads((tmp_path / 'real-2026-11-25.json').read_text())
    assert list(checkpoint['attempts']) == ['07:00', '07:05']


@pytest.mark.parametrize(('hour', 'minute'), [(6, 59), (7, 12), (7, 30), (8, 0), (15, 0)])
def test_outside_collection_window_touches_neither_database_nor_provider(tmp_path, hour, minute):
    client = FakeKIS()

    def no_database():
        raise AssertionError('No database work outside the collection window')

    result = run_us_index_preopen(
        state_dir=tmp_path, client=client, session_factory=no_database, clock=Clock(hour, minute),
    )

    assert result['status'] == 'not_due'
    assert result['reason'] == 'outside_window'
    assert client.calls == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(('day', 'reason'), [
    ('2026-11-28', 'krx_closed'), ('2026-07-17', 'krx_closed'),
    ('2026-01-02', 'special_krx_hours'), ('2026-11-27', 'no_new_us_session'),
])
def test_calendar_noops_do_not_call_kis_or_create_a_checkpoint(tmp_path, day, reason):
    clock = Clock()
    clock.value = datetime.fromisoformat(f'{day}T07:00:00+09:00')
    client = FakeKIS()

    def no_database():
        raise AssertionError('Calendar no-ops must not open the database')

    result = run_us_index_preopen(
        state_dir=tmp_path, client=client, session_factory=no_database, clock=clock,
    )

    assert result['status'] == 'not_due'
    assert result['reason'] == reason
    assert client.calls == []
    assert list(tmp_path.iterdir()) == []


def test_each_slot_is_claimed_once_even_when_all_prices_are_unavailable(tmp_path, database):
    client = FakeKIS()
    client.responses = {code: {'rt_cd': '0', 'output2': []} for code in ('SPX', 'COMP', 'SOX')}
    clock = Clock()
    for minute in (0, 1, 4, 5, 6, 9, 10, 11):
        clock.value = clock.value.replace(minute=minute)
        result = run_us_index_preopen(
            state_dir=tmp_path, client=client, session_factory=database, clock=clock,
        )
        if minute not in (0, 5, 10):
            assert result['status'] == 'not_due'
            assert result['reason'] == 'already_attempted'

    assert len(client.calls) == 9
    checkpoint = json.loads((tmp_path / 'real-2026-11-25.json').read_text())
    assert list(checkpoint['attempts']) == ['07:00', '07:05', '07:10']


def test_deadline_crossed_during_a_quote_stops_later_quotes_and_excludes_late_capture(tmp_path, database):
    clock = Clock(7, 11)

    class LateKIS(FakeKIS):
        async def fetch_overseas_index_daily_prices(self, **kwargs):
            response = await super().fetch_overseas_index_daily_prices(**kwargs)
            clock.value = clock.value.replace(minute=12)
            return response

    client = LateKIS()
    result = run_us_index_preopen(
        state_dir=tmp_path, client=client, session_factory=database, clock=clock,
    )

    assert len(client.calls) == 1
    assert result['status'] == 'expired'
    assert result['data_complete'] is False
    assert result['snapshot_ids'] == {}


def test_collection_time_budget_stops_a_hanging_quote_without_retry(tmp_path, database):
    class SlowKIS(FakeKIS):
        async def fetch_overseas_index_daily_prices(self, **kwargs):
            self.calls.append(kwargs)
            await asyncio.sleep(1)

    client = SlowKIS()
    result = run_us_index_preopen(
        state_dir=tmp_path, client=client, session_factory=database, clock=Clock(),
        budget_seconds=0.01,
    )

    assert len(client.calls) == 1
    assert result['status'] == 'failed'
    assert result['reason'] == 'budget_exhausted'
    assert result['data_complete'] is False


@pytest.mark.parametrize('attempts', [[], {'unexpected': {}}, {'07:00': 'broken'}])
def test_corrupt_attempt_state_fails_without_resetting_the_request_budget(tmp_path, attempts):
    path = tmp_path / 'real-2026-11-25.json'
    state = {
        'schema_version': 'us_index_preopen_v1', 'target_trade_date': '2026-11-25',
        'as_of': '2026-11-24T22:00:00+00:00', 'source_environment': 'real', 'attempts': attempts,
    }
    path.write_text(json.dumps(state))
    client = FakeKIS()

    def no_database():
        raise AssertionError('Invalid checkpoint must fail before database access')

    with pytest.raises(ValueError, match='attempt'):
        run_us_index_preopen(
            state_dir=tmp_path, client=client, session_factory=no_database, clock=Clock(),
        )
    assert client.calls == []
    assert json.loads(path.read_text()) == state


def test_another_process_lock_prevents_database_and_provider_work(tmp_path):
    client = FakeKIS()

    def no_database():
        raise AssertionError('Busy worker must not query the database')

    with checkpoint_file_lock(tmp_path / 'real-2026-11-25.json') as acquired:
        assert acquired
        result = run_us_index_preopen(
            state_dir=tmp_path, client=client, session_factory=no_database, clock=Clock(),
        )
    assert result['reason'] == 'busy'
    assert client.calls == []


def test_interruption_after_a_committed_capture_resumes_only_exact_debt(tmp_path, database):
    path = tmp_path / 'real-2026-11-25.json'

    class InterruptedKIS(FakeKIS):
        async def fetch_overseas_index_daily_prices(self, **kwargs):
            claim = json.loads(path.read_text())['attempts']['07:00']
            assert claim['index_codes'] == ['SPX', 'COMP', 'SOX']
            if kwargs['index_code'] == 'COMP':
                raise asyncio.CancelledError()
            return await super().fetch_overseas_index_daily_prices(**kwargs)

    clock = Clock()
    with pytest.raises(asyncio.CancelledError):
        run_us_index_preopen(
            state_dir=tmp_path, client=InterruptedKIS(), session_factory=database, clock=clock,
        )
    assert 'result' not in json.loads(path.read_text())['attempts']['07:00']

    restarted_client = FakeKIS()
    clock.value = clock.value.replace(minute=1)
    assert run_us_index_preopen(
        state_dir=tmp_path, client=restarted_client, session_factory=database, clock=clock,
    )['reason'] == 'already_attempted'
    assert restarted_client.calls == []
    clock.value = clock.value.replace(minute=5)
    resumed = run_us_index_preopen(
        state_dir=tmp_path, client=restarted_client, session_factory=database, clock=clock,
    )
    assert resumed['data_complete'] is True
    assert [call['index_code'] for call in restarted_client.calls] == ['COMP', 'SOX']


def test_missed_slots_are_not_replayed_as_a_burst(tmp_path, database):
    client = FakeKIS()
    client.responses = {code: {'rt_cd': '0', 'output2': []} for code in ('SPX', 'COMP', 'SOX')}
    clock = Clock(7, 6)
    for minute in (6, 7, 11):
        clock.value = clock.value.replace(minute=minute)
        run_us_index_preopen(
            state_dir=tmp_path, client=client, session_factory=database, clock=clock,
        )
    assert len(client.calls) == 6
    assert list(json.loads((tmp_path / 'real-2026-11-25.json').read_text())['attempts']) == ['07:05', '07:10']


def test_deadline_is_rechecked_after_database_work_before_claiming_or_fetching(tmp_path, database):
    clock = Clock(7, 11)

    @contextmanager
    def delayed_database():
        with database() as session:
            yield session
        clock.value = clock.value.replace(minute=12)

    client = FakeKIS()
    result = run_us_index_preopen(
        state_dir=tmp_path, client=client, session_factory=delayed_database, clock=clock,
    )
    assert result['reason'] == 'outside_window'
    assert client.calls == []
    assert not (tmp_path / 'real-2026-11-25.json').exists()


def test_database_worker_crossing_deadline_during_executor_shutdown_is_expired(tmp_path, database):
    clock = Clock(7, 11)
    accesses = 0

    @contextmanager
    def slow_commit():
        nonlocal accesses
        accesses += 1
        with database() as session:
            yield session
            if accesses == 2:
                time.sleep(0.3)
                clock.value = clock.value.replace(minute=12)

    client = FakeKIS()
    result = run_us_index_preopen(
        state_dir=tmp_path, client=client, session_factory=slow_commit, clock=clock,
        budget_seconds=0.1,
    )
    assert result['status'] == 'expired'
    assert result['reason'] == 'outside_window'
    assert result['data_complete'] is False
    assert len(client.calls) == 1


def test_checkpoint_write_crossing_cutoff_cannot_report_a_ready_result(tmp_path, database, monkeypatch):
    clock = Clock(7, 11)
    replace = os.replace
    writes = 0

    def delayed_replace(source, destination):
        nonlocal writes
        replace(source, destination)
        writes += 1
        if writes == 2:
            clock.value = clock.value.replace(minute=12)

    monkeypatch.setattr(os, 'replace', delayed_replace)
    result = run_us_index_preopen(
        state_dir=tmp_path, client=FakeKIS(), session_factory=database, clock=clock,
    )
    assert result['status'] == 'expired'
    assert result['data_complete'] is False
    checkpoint = json.loads((tmp_path / 'real-2026-11-25.json').read_text())
    assert checkpoint['attempts']['07:10']['result']['status'] == 'expired'


@pytest.mark.parametrize('value', [None, '2026-11-25T07:00:00', datetime(2026, 11, 25, 7)])
def test_bad_clock_fails_before_any_io(tmp_path, value):
    client = FakeKIS()
    with pytest.raises(ValueError, match='timezone-aware'):
        run_us_index_preopen(state_dir=tmp_path, client=client, clock=lambda: value)
    assert client.calls == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('budget', [True, 0, -1, 91, float('inf'), float('nan')])
def test_bad_budget_fails_before_any_io(tmp_path, budget):
    client = FakeKIS()
    with pytest.raises(ValueError, match='budget_seconds'):
        run_us_index_preopen(state_dir=tmp_path, client=client, budget_seconds=budget, clock=Clock())
    assert client.calls == []
    assert list(tmp_path.iterdir()) == []


def test_host_timezone_does_not_change_the_krx_target_or_frozen_as_of(tmp_path, database):
    clock = Clock()
    clock.value = clock.value.astimezone(ZoneInfo('America/New_York'))
    result = run_us_index_preopen(
        state_dir=tmp_path, client=FakeKIS(), session_factory=database, clock=clock,
    )
    assert result['target_trade_date'] == '2026-11-25'
    assert result['data_complete'] is True


def test_next_krx_session_does_not_reuse_yesterdays_checkpoints_or_captures(tmp_path, database):
    clock = Clock()
    first = run_us_index_preopen(
        state_dir=tmp_path, client=FakeKIS(), session_factory=database, clock=clock,
    )
    clock.value = clock.value.replace(day=26)
    client = FakeKIS()
    second = run_us_index_preopen(
        state_dir=tmp_path, client=client, session_factory=database, clock=clock,
    )
    assert len(client.calls) == 3
    assert second['data_complete'] is True
    assert second['snapshot_ids']['SPX'] != first['snapshot_ids']['SPX']
    assert (tmp_path / 'real-2026-11-25.json').exists()
    assert (tmp_path / 'real-2026-11-26.json').exists()


def test_standalone_cli_runs_actual_scheduler_and_reports_committed_ids(tmp_path, database, capsys):
    from app.internal.us_index_preopen import main

    exit_code = main(
        ['--state-dir', str(tmp_path)],
        runner=partial(run_us_index_preopen, client=FakeKIS(), session_factory=database, clock=Clock()),
    )

    result = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert result['data_complete'] is True
    assert len(result['snapshot_ids']) == 3


def test_cli_reports_database_errors_without_leaking_exception_text(tmp_path, capsys):
    from app.internal.us_index_preopen import main

    client = FakeKIS()

    def broken_database():
        raise RuntimeError('credential=synthetic-secret')

    exit_code = main(
        ['--state-dir', str(tmp_path)],
        runner=partial(run_us_index_preopen, client=client, session_factory=broken_database, clock=Clock()),
    )
    result = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert result == {'status': 'failed', 'stage': 'schedule', 'error_type': 'RuntimeError'}
    assert client.calls == []


@pytest.mark.parametrize('hour', [6, 8])
def test_cli_treats_a_normal_noop_as_success(tmp_path, hour, capsys):
    from app.internal.us_index_preopen import main

    assert main(
        ['--state-dir', str(tmp_path)], runner=partial(run_us_index_preopen, clock=Clock(hour)),
    ) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'not_due'


def test_calendar_bounds_are_a_technical_failure_rather_than_a_closed_market(tmp_path, capsys):
    from app.internal.us_index_preopen import main

    clock = Clock()
    clock.value = datetime(2100, 1, 4, 7, tzinfo=KST)
    client = FakeKIS()
    assert main(
        ['--state-dir', str(tmp_path)], runner=partial(run_us_index_preopen, client=client, clock=clock),
    ) == 1
    assert json.loads(capsys.readouterr().out)['status'] == 'failed'
    assert client.calls == []
