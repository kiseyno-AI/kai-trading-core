from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from decimal import Context, Decimal, Inexact, ROUND_DOWN, localcontext
from functools import partial
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db_models import USIndexInputSnapshot
from app.market.us_index_features import calculate_us_index_features
from app.market.us_index_input_freeze import freeze_us_index_inputs, load_frozen_us_index_inputs
from app.market.us_index_snapshot_store import save_us_index_snapshot
from app.market.us_session import latest_completed_us_session


KST = ZoneInfo('Asia/Seoul')
CODES = ('SPX', 'COMP', 'SOX')


@pytest.fixture
def inputs(tmp_path):
    engine = create_engine(f'sqlite+pysqlite:///{tmp_path / "features.sqlite"}')
    USIndexInputSnapshot.__table__.create(engine)

    @contextmanager
    def database():
        with Session(engine) as session, session.begin():
            yield session

    def capture(day, closes, previous=None):
        as_of = datetime.combine(day, time(7), tzinfo=KST)
        window = latest_completed_us_session(as_of=as_of)
        previous = previous or {code: Decimal(100) for code in CODES}
        records = {
            code: save_us_index_snapshot(
                {'rt_cd': '0', 'output1': {'stck_shrn_iscd': code}, 'output2': [
                    {'stck_bsop_date': window.session_date.strftime('%Y%m%d'),
                     'ovrs_nmix_prpr': str(closes[code])},
                    {'stck_bsop_date': window.previous_session_date.strftime('%Y%m%d'),
                     'ovrs_nmix_prpr': str(previous[code])},
                ]},
                index_code=code, as_of=as_of, collected_at=as_of + timedelta(minutes=1),
                source_environment='real', session_factory=database,
            ) for code in CODES
        }
        frozen = freeze_us_index_inputs(
            target_trade_date=day, snapshot_ids={code: row.snapshot_id for code, row in records.items()},
            state_dir=tmp_path, session_factory=database, clock=lambda: as_of + timedelta(minutes=10),
        )
        assert frozen is not None
        return frozen

    yield capture, database, tmp_path
    engine.dispose()


def test_frozen_closes_produce_daily_returns_and_synchronized_return_spreads(inputs):
    capture, _, _ = inputs
    current = capture(date(2026, 11, 25), {'SPX': Decimal(102), 'COMP': Decimal(104), 'SOX': Decimal(106)})

    result = calculate_us_index_features(current)

    assert {code: result['indices'][code]['return_pct'] for code in CODES} == {
        'SPX': '2', 'COMP': '4', 'SOX': '6',
    }
    assert result['return_spreads_pp'] == {'COMP-SPX': '2', 'SOX-SPX': '4', 'SOX-COMP': '2'}


def test_two_closes_leave_history_metrics_unavailable_instead_of_zero(inputs):
    capture, _, _ = inputs
    current = capture(date(2026, 11, 25), {code: Decimal(100) for code in CODES})

    result = calculate_us_index_features(current)

    assert result['history_count'] == 0
    assert result['history_unavailable_reason'] == 'insufficient_history'
    for item in result['indices'].values():
        assert item['return_pct'] == '0'
        assert item['acceleration_pp'] is None
        assert item['reference_mean_pct'] is None
        assert item['reference_sample_std_pct'] is None
        assert item['shock_z'] is None
        assert item['shock_unavailable_reason'] == 'insufficient_history'
        assert item['percentile_pct'] is None


def test_acceleration_subtracts_the_immediately_preceding_session_return(inputs):
    capture, _, _ = inputs
    prior = capture(date(2026, 11, 24), {code: Decimal(100) for code in CODES})
    current = capture(date(2026, 11, 25), {'SPX': Decimal(102), 'COMP': Decimal(104), 'SOX': Decimal(106)})

    result = calculate_us_index_features(current, history=(prior,))

    assert result['history_count'] == 1
    assert [result['indices'][code]['acceleration_pp'] for code in CODES] == ['2', '4', '6']
    assert result['indices']['SPX']['shock_z'] is None


def _series(inputs, *, baseline=None, event=None):
    capture, _, _ = inputs
    baseline = baseline if baseline is not None else [Decimal(-1), Decimal(1)] * 10
    event = event or {'SPX': Decimal(3), 'COMP': Decimal(4), 'SOX': Decimal(5)}
    day = date(2026, 10, 21)
    previous = {code: Decimal(100) for code in CODES}
    records = []
    with localcontext(Context(prec=100)):
        for rates in [{code: value for code in CODES} for value in baseline] + [event]:
            closes = {code: previous[code] * (1 + rates[code] / 100) for code in CODES}
            records.append(capture(day, closes, previous))
            previous = closes
            day += timedelta(days=1)
            while day.weekday() >= 5:
                day += timedelta(days=1)
    return records[-1], tuple(records[:-1])


def test_shock_uses_twenty_lagged_returns_and_sample_volatility_without_the_event(inputs):
    current, history = _series(inputs)

    result = calculate_us_index_features(current, history=history)

    assert result['history_count'] == 20
    assert result['history_unavailable_reason'] is None
    spx = result['indices']['SPX']
    assert spx['return_pct'] == '3'
    assert spx['acceleration_pp'] == '2'
    assert spx['reference_mean_pct'] == '0'
    assert float(spx['reference_sample_std_pct']) == pytest.approx(1.025978352085154)
    assert float(spx['shock_z']) == pytest.approx(2.924038303442689)
    assert spx['shock_unavailable_reason'] is None
    assert spx['percentile_pct'] == '100'


def test_constant_reference_keeps_a_tie_inclusive_percentile_but_has_no_shock(inputs):
    current, history = _series(
        inputs, baseline=[Decimal(0)] * 20, event={code: Decimal(0) for code in CODES},
    )

    result = calculate_us_index_features(current, history=history)

    for item in result['indices'].values():
        assert item['reference_sample_std_pct'] == '0'
        assert item['shock_z'] is None
        assert item['shock_unavailable_reason'] == 'zero_reference_volatility'
        assert item['percentile_pct'] == '100'


def test_replay_identity_binds_algorithm_and_exact_current_and_history_manifests(inputs):
    capture, _, _ = inputs
    prior = capture(date(2026, 11, 24), {code: Decimal(100) for code in CODES})
    current = capture(date(2026, 11, 25), {code: Decimal(102) for code in CODES})

    first = calculate_us_index_features(current, history=(prior,))
    replayed = calculate_us_index_features(current, history=(prior,))
    changed = calculate_us_index_features(current, history=(replace(prior, manifest_id='0' * 64),))

    assert first == replayed
    assert first['schema_version'] == 'us_index_features_v1'
    assert first['input_manifest_id'] == current.manifest_id
    assert first['history_inputs'] == [{'target_trade_date': '2026-11-24', 'manifest_id': prior.manifest_id}]
    assert first['feature_id'] != changed['feature_id']


def test_percentile_is_a_lag_only_ecdf_with_ties_included(inputs):
    current, history = _series(inputs, event={'SPX': Decimal(-2), 'COMP': Decimal(-1), 'SOX': Decimal(1)})
    result = calculate_us_index_features(current, history=history)
    assert [result['indices'][code]['percentile_pct'] for code in CODES] == ['0', '50', '100']


def test_a_missing_us_session_is_not_replaced_by_an_older_observation(inputs):
    current, history = _series(inputs)
    gap = calculate_us_index_features(current, history=history[:-2] + history[-1:])
    assert gap['history_count'] == 1
    assert gap['history_unavailable_reason'] == 'nonconsecutive_history'
    assert gap['indices']['SPX']['acceleration_pp'] == '2'
    assert gap['indices']['SPX']['shock_z'] is None
    assert gap['indices']['SPX']['percentile_pct'] is None


def test_missing_immediate_predecessor_keeps_acceleration_unavailable(inputs):
    capture, _, _ = inputs
    old = capture(date(2026, 11, 23), {code: Decimal(100) for code in CODES})
    current = capture(date(2026, 11, 25), {code: Decimal(102) for code in CODES})
    result = calculate_us_index_features(current, history=(old,))
    assert result['history_count'] == 0
    assert result['history_unavailable_reason'] == 'nonconsecutive_history'
    assert result['indices']['SPX']['acceleration_pp'] is None
    assert result['indices']['SPX']['return_pct'] == '2'


@pytest.mark.parametrize('problem', ['future', 'duplicate', 'reversed', 'environment', 'too_many'])
def test_ambiguous_or_future_history_is_a_contract_error(inputs, problem):
    capture, _, _ = inputs
    old = capture(date(2026, 11, 23), {code: Decimal(100) for code in CODES})
    prior = capture(date(2026, 11, 24), {code: Decimal(100) for code in CODES})
    current = capture(date(2026, 11, 25), {code: Decimal(102) for code in CODES})
    selections = {
        'future': (current,), 'duplicate': (prior, prior), 'reversed': (prior, old),
        'environment': (replace(prior, source_environment='mock', snapshots=tuple(
            replace(row, source_environment='mock') for row in prior.snapshots
        )),),
        'too_many': (prior,) * 21,
    }
    with pytest.raises(ValueError):
        calculate_us_index_features(current, history=selections[problem])


def test_conflicting_overlapping_closes_are_not_silently_mixed(inputs):
    capture, _, _ = inputs
    prior = capture(date(2026, 11, 24), {code: Decimal(101) for code in CODES})
    current = capture(date(2026, 11, 25), {code: Decimal(102) for code in CODES})
    with pytest.raises(ValueError, match='price continuity'):
        calculate_us_index_features(current, history=(prior,))


@pytest.mark.parametrize('price', [Decimal(0), Decimal(-1), Decimal('NaN'), Decimal('Infinity'), '100'])
def test_invalid_closing_prices_fail_without_a_numeric_result(inputs, price):
    capture, _, _ = inputs
    current = capture(date(2026, 11, 25), {code: Decimal(100) for code in CODES})
    row = current.snapshots[0]
    bad = replace(current, snapshots=(replace(row, prices=replace(row.prices, price=price)), *current.snapshots[1:]))
    with pytest.raises(ValueError, match='finite positive'):
        calculate_us_index_features(bad)


def test_decimal_context_cannot_change_features_or_their_replay_identity(inputs):
    capture, _, _ = inputs
    current = capture(date(2026, 11, 25), {code: Decimal(103) for code in CODES},
                      {code: Decimal(99) for code in CODES})
    expected = calculate_us_index_features(current)
    with localcontext(Context(prec=7, rounding=ROUND_DOWN)) as context:
        context.traps[Inexact] = True
        assert calculate_us_index_features(current) == expected
        assert context.prec == 7


def test_an_exact_tiny_move_is_not_lost_to_ratio_cancellation(inputs):
    capture, _, _ = inputs
    current = capture(date(2026, 11, 25), {
        code: Decimal('100.0000000000000000000000000000000000000001') for code in CODES
    })
    result = calculate_us_index_features(current)
    assert Decimal(result['indices']['SPX']['return_pct']) == Decimal('1e-40')


def test_read_only_cli_replays_explicit_current_and_historical_seals(inputs, capsys):
    from app.internal.us_index_features import main

    capture, database, path = inputs
    prior = capture(date(2026, 11, 24), {code: Decimal(100) for code in CODES})
    current = capture(date(2026, 11, 25), {code: Decimal(102) for code in CODES})

    exit_code = main(
        ['--target-trade-date', '2026-11-25', '--history-date', '2026-11-24', '--state-dir', str(path)],
        loader=partial(load_frozen_us_index_inputs, session_factory=database),
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == calculate_us_index_features(current, history=(prior,))


@pytest.mark.parametrize('problem', ['late', 'before_selection', 'naive'])
def test_an_invalid_confirmation_cannot_be_used_as_frozen_input(inputs, problem):
    capture, _, _ = inputs
    current = capture(date(2026, 11, 25), {code: Decimal(100) for code in CODES})
    confirmed = {
        'late': current.cutoff_at,
        'before_selection': current.selected_at - timedelta(seconds=1),
        'naive': current.confirmed_at.replace(tzinfo=None),
    }[problem]
    with pytest.raises(ValueError):
        calculate_us_index_features(replace(current, confirmed_at=confirmed))


@pytest.mark.parametrize('missing', ['current', 'history', 'mock_environment'])
def test_cli_reports_absent_requested_evidence_as_normal_unavailability(inputs, capsys, missing):
    from app.internal.us_index_features import main

    capture, database, path = inputs
    if missing != 'current':
        capture(date(2026, 11, 25), {code: Decimal(102) for code in CODES})
    arguments = ['--target-trade-date', '2026-11-25', '--state-dir', str(path)]
    if missing == 'history':
        arguments += ['--history-date', '2026-11-24']
    elif missing == 'mock_environment':
        arguments += ['--source-environment', 'mock']
    assert main(arguments, loader=partial(load_frozen_us_index_inputs, session_factory=database)) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['status'] == 'unavailable'
    assert result['reason'] == ('missing_history' if missing == 'history' else 'no_frozen_inputs')


def test_cli_does_not_turn_corrupt_history_into_missing_history(inputs, capsys):
    from app.internal.us_index_features import main

    capture, database, path = inputs
    capture(date(2026, 11, 24), {code: Decimal(100) for code in CODES})
    capture(date(2026, 11, 25), {code: Decimal(102) for code in CODES})
    damaged = path / 'frozen/real-2026-11-24/input.json'
    damaged.write_text('{}')
    assert main(
        ['--target-trade-date', '2026-11-25', '--history-date', '2026-11-24', '--state-dir', str(path)],
        loader=partial(load_frozen_us_index_inputs, session_factory=database),
    ) == 1
    assert json.loads(capsys.readouterr().out) == {'status': 'failed', 'stage': 'features', 'error_type': 'ValueError'}
    assert damaged.read_text() == '{}'


def test_cli_technical_errors_omit_exception_text_and_do_not_collect(tmp_path, capsys):
    from app.internal.us_index_features import main

    def broken_storage(**kwargs):
        raise RuntimeError('credential=synthetic-secret')

    assert main(
        ['--target-trade-date', '2026-11-25', '--state-dir', str(tmp_path)], loader=broken_storage,
    ) == 1
    assert json.loads(capsys.readouterr().out) == {'status': 'failed', 'stage': 'features', 'error_type': 'RuntimeError'}
    assert list(tmp_path.iterdir()) == []


def test_cli_rejects_an_unbounded_history_request_before_reading_storage(capsys):
    from app.internal.us_index_features import main

    def no_storage(**kwargs):
        raise AssertionError('Oversized input is rejected before reading storage')

    with pytest.raises(SystemExit) as error:
        main(['--target-trade-date', '2026-11-25'] + ['--history-date', '2026-11-24'] * 21, loader=no_storage)
    assert error.value.code == 2


def test_later_raw_price_captures_cannot_change_a_sealed_feature_result(inputs, capsys):
    from app.internal.us_index_features import main

    capture, database, path = inputs
    capture(date(2026, 11, 25), {code: Decimal(102) for code in CODES})
    arguments = ['--target-trade-date', '2026-11-25', '--state-dir', str(path)]
    reader = partial(load_frozen_us_index_inputs, session_factory=database)
    assert main(arguments, loader=reader) == 0
    first = json.loads(capsys.readouterr().out)
    original_files = {file: file.read_bytes() for file in path.rglob('*.json')}
    capture(date(2026, 11, 25), {code: Decimal(200) for code in CODES})
    assert main(arguments, loader=reader) == 0
    assert json.loads(capsys.readouterr().out) == first
    assert {file: file.read_bytes() for file in path.rglob('*.json')} == original_files


def test_a_fresh_cli_process_requires_no_calendar_provider_or_current_clock(inputs):
    capture, _, path = inputs
    prior = capture(date(2026, 11, 24), {code: Decimal(100) for code in CODES})
    current = capture(date(2026, 11, 25), {code: Decimal(102) for code in CODES})
    script = '''
from contextlib import contextmanager
from functools import partial
import sys
import exchange_calendars
from exchange_calendars.exchange_calendar_xkrx import XKRXExchangeCalendar
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
def no_calendar(*args, **kwargs):
    raise AssertionError('Features replay stored session evidence only')
XKRXExchangeCalendar.__init__ = no_calendar
exchange_calendars.get_calendar = no_calendar
from app.internal.us_index_features import main
from app.market.us_index_input_freeze import load_frozen_us_index_inputs
assert 'app.market.us_index_preopen' not in sys.modules
assert 'app.brokers.kis.provider' not in sys.modules
engine = create_engine(sys.argv[1])
@contextmanager
def factory():
    with Session(engine) as session, session.begin():
        yield session
code = main(['--target-trade-date', '2026-11-25', '--history-date', '2026-11-24',
             '--state-dir', sys.argv[2]],
            loader=partial(load_frozen_us_index_inputs, session_factory=factory))
engine.dispose()
raise SystemExit(code)
'''
    replay = subprocess.run(
        [sys.executable, '-c', script, f'sqlite+pysqlite:///{path / "features.sqlite"}', str(path)],
        cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True, text=True, timeout=20,
    )
    assert json.loads(replay.stdout) == calculate_us_index_features(current, history=(prior,))


@pytest.mark.parametrize('problem', ['missing_index', 'mixed_session'])
def test_return_spreads_require_a_complete_synchronized_index_set(inputs, problem):
    capture, _, _ = inputs
    current = capture(date(2026, 11, 25), {code: Decimal(100) for code in CODES})
    rows = current.snapshots[:-1]
    if problem == 'mixed_session':
        row = current.snapshots[-1]
        other_session = replace(row.prices.session, session_date=date(2026, 11, 23))
        rows = (*rows, replace(row, prices=replace(row.prices, session=other_session)))
    with pytest.raises(ValueError):
        calculate_us_index_features(replace(current, snapshots=rows))


def test_history_close_timestamps_must_match_the_stored_session_link(inputs):
    capture, _, _ = inputs
    prior = capture(date(2026, 11, 24), {code: Decimal(100) for code in CODES})
    current = capture(date(2026, 11, 25), {code: Decimal(102) for code in CODES})
    changed_session = replace(prior.snapshots[0].prices.session,
                              close_at=prior.snapshots[0].prices.session.close_at + timedelta(hours=1))
    shifted = replace(prior, snapshots=tuple(
        replace(row, prices=replace(row.prices, session=changed_session)) for row in prior.snapshots
    ))
    with pytest.raises(ValueError, match='session window'):
        calculate_us_index_features(current, history=(shifted,))
