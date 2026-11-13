import json
import subprocess
import sys
from dataclasses import replace
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from app.agent_context.wait_paper_recovery import load_cohort, recover_market_date


def recorded_watch(tmp_path, *, key=1):
    from app.agent_context.wait_price_observations import WaitPriceWatch

    folder = tmp_path / 'observations' / '2026-11-11'
    folder.mkdir(parents=True, exist_ok=True)
    raw = {
        'decision_snapshot_id': key, 'decision_agent_name': 'signal-decider', 'symbol': '130580',
        'source_market_date': '2026-11-10', 'observed_at': '2026-11-10T23:00:00+00:00',
        'tracking_started_at': '2026-11-11T00:00:00+00:00',
        'market_date': '2026-11-11', 'trigger_price': '100', 'status': 'not_observed',
    }
    (folder / f'{key}.watch.json').write_text(json.dumps(raw))
    watch = WaitPriceWatch(
        key, 'signal-decider', '130580', '2026-11-10', raw['observed_at'], '100',
        target_price='110', stop_loss_price='90', quantity='3', buy_direction='at_or_below',
        chase_limit_price='103', quantity_source='stored_plan', round_trip_cost_bps='20',
    )
    return folder, watch


def test_cohort_is_exact_session_watch_files_including_both_lanes(tmp_path):
    day = date(2026, 11, 11)
    folder = tmp_path / str(day)
    folder.mkdir()
    for key, agent in [(1, 'signal-decider'), (2, 'short-term-decider')]:
        (folder / f'{key}.watch.json').write_text(json.dumps({
            'decision_snapshot_id': key, 'decision_agent_name': agent, 'symbol': '130580',
            'source_market_date': '2026-11-10', 'observed_at': '2026-11-10T23:00:00+00:00',
            'tracking_started_at': '2026-11-11T00:00:00+00:00',
            'market_date': str(day), 'trigger_price': '100', 'status': 'not_observed',
        }))
    (folder / '99.touch.json').write_text('{}')
    (folder / 'recovery').mkdir()
    (folder / 'recovery' / '99.json').write_text('{}')

    rows = load_cohort(tmp_path, day)

    assert [row['decision_snapshot_id'] for row in rows] == [1, 2]
    assert [row['decision_agent_name'] for row in rows] == ['signal-decider', 'short-term-decider']
    assert load_cohort(tmp_path, date(2026, 11, 12)) == []


def test_preview_does_not_create_ledger_or_mutate_raw_observations(tmp_path):
    from app.agent_context.wait_price_observations import WaitPriceWatch

    day = date(2026, 11, 11)
    folder = tmp_path / 'observations' / str(day)
    folder.mkdir(parents=True)
    watch = {
        'decision_snapshot_id': 1, 'decision_agent_name': 'signal-decider', 'symbol': '130580',
        'source_market_date': '2026-11-10', 'observed_at': '2026-11-10T23:00:00+00:00',
        'tracking_started_at': '2026-11-11T00:00:00+00:00',
        'market_date': str(day), 'trigger_price': '100', 'status': 'not_observed',
    }
    (folder / '1.watch.json').write_text(json.dumps(watch))
    (folder / '1.touch.json').write_text(json.dumps({
        'status': 'touched', 'evidence': 'exact', 'observed_price': '100',
        'price_as_of': '2026-11-11T00:01:00+00:00',
        'price_received_at': '2026-11-11T00:01:01+00:00',
    }))
    hydrated = WaitPriceWatch(
        1, 'signal-decider', '130580', '2026-11-10', watch['observed_at'], '100',
        target_price='110', stop_loss_price='90', quantity='3',
        quantity_source='stored_plan', round_trip_cost_bps='20', buy_direction='at_or_below',
    )
    before = {str(path): path.read_bytes() for path in folder.iterdir()}
    result = recover_market_date(
        day, observation_root=folder.parent, ledger_root=tmp_path / 'ledger',
        watches=[hydrated],
        prices=[{
            'symbol': '130580', 'price': '115',
            'price_as_of': '2026-11-11T06:00:00+00:00',
            'received_at': '2026-11-11T06:00:00+00:00', 'source': 'monitoring_last_quote',
        }],
    )

    assert result['dry_run'] is True
    assert result['cohort_count'] == 1
    assert result['recovered_entry_count'] == 1
    assert result['recovered_exit_count'] == 0
    assert result['unknown_exit_count'] == 1
    assert result['items'][0]['realized_net_pnl_krw'] is None
    assert result['evidence']['coverage'] == 'incomplete'
    assert not (tmp_path / 'ledger').exists()
    assert {str(path): path.read_bytes() for path in folder.iterdir()} == before


def test_timestamped_ask_caps_fill_before_same_timestamp_trade(tmp_path):
    folder, watch = recorded_watch(tmp_path)
    moment = '2026-11-11T00:01:00+00:00'
    (folder / '1.touch.json').write_text(json.dumps({
        'evidence': 'exact', 'observed_price': '100',
        'price_as_of': moment, 'price_received_at': moment,
    }))
    (folder / '1.ask-entry.json').write_text(json.dumps({
        'ask_price': '100', 'ask_size': 1, 'within_chase_limit': True,
        'price_as_of': moment, 'price_received_at': moment,
    }))

    result = recover_market_date(
        date(2026, 11, 11), observation_root=folder.parent,
        ledger_root=tmp_path / 'ledger', watches=[watch],
    )

    assert result['items'][0]['filled_quantity'] == '1'
    assert result['items'][0]['unfilled_quantity'] == '2'
    assert result['items'][0]['entry']['execution_model'] == 'top_ask_simulation'


def test_later_received_ask_does_not_replace_known_earlier_trade(tmp_path):
    folder, watch = recorded_watch(tmp_path)
    moment = '2026-11-11T00:01:00+00:00'
    (folder / '1.touch.json').write_text(json.dumps({
        'evidence': 'exact', 'observed_price': '100',
        'price_as_of': moment, 'price_received_at': moment,
    }))
    (folder / '1.ask-entry.json').write_text(json.dumps({
        'ask_price': '100', 'ask_size': 1, 'within_chase_limit': True,
        'price_as_of': moment, 'price_received_at': '2026-11-11T00:01:01+00:00',
    }))

    result = recover_market_date(
        date(2026, 11, 11), observation_root=folder.parent,
        ledger_root=tmp_path / 'ledger', watches=[watch],
    )

    assert result['items'][0]['filled_quantity'] == '3'
    assert result['items'][0]['entry']['price_received_at'] == moment


def test_last_quote_cannot_create_an_entry_without_observed_entry_sequence(tmp_path):
    folder, watch = recorded_watch(tmp_path)

    result = recover_market_date(
        date(2026, 11, 11), observation_root=folder.parent,
        ledger_root=tmp_path / 'ledger', watches=[watch], prices=[{
            'symbol': '130580', 'price': '100', 'price_as_of': '2026-11-11T06:00:00+00:00',
            'received_at': '2026-11-11T06:00:00+00:00', 'source': 'monitoring_last_quote',
        }],
    )

    assert result['recovered_entry_count'] == 0
    assert result['items'][0]['entry'] is None
    assert result['items'][0]['realized_net_pnl_krw'] is None


def test_apply_is_idempotent_and_preserves_existing_live_lots(tmp_path):
    from app.agent_context.wait_paper_ledger import WaitPaperLedger

    folder, watch = recorded_watch(tmp_path)
    ledger_root = tmp_path / 'ledger'
    ledger = WaitPaperLedger(ledger_root)
    ledger.configure(date(2026, 11, 10), [replace(watch, decision_snapshot_id=99)], now=datetime(2026, 11, 10, tzinfo=timezone.utc))
    ledger.observe_trade('130580', 100, price_as_of=datetime(2026, 11, 10, 0, 1, tzinfo=timezone.utc), received_at=datetime(2026, 11, 10, 0, 1, tzinfo=timezone.utc))
    previous = ledger_root / '2026-11-10' / '99.json'
    original = previous.read_bytes()
    (folder / '1.touch.json').write_text(json.dumps({
        'evidence': 'exact', 'observed_price': '100',
        'price_as_of': '2026-11-11T00:01:00+00:00',
        'price_received_at': '2026-11-11T00:01:01+00:00',
    }))
    args = dict(observation_root=folder.parent, ledger_root=ledger_root, watches=[watch], apply=True)

    first = recover_market_date(date(2026, 11, 11), **args)
    stored = {str(path): path.read_bytes() for path in ledger_root.rglob('*') if path.is_file()}
    second = recover_market_date(date(2026, 11, 11), **args)

    assert first['recovered_entry_count'] == 1
    assert second['preserved_existing_count'] == 1
    assert second['recovered_entry_count'] == 0
    assert previous.read_bytes() == original
    assert {str(path): path.read_bytes() for path in ledger_root.rglob('*') if path.is_file()} == stored


@pytest.mark.parametrize(('field', 'count'), [('quantity', 'missing_quantity_count'), ('target_price', 'missing_levels_count')])
def test_missing_frozen_inputs_stay_visible_without_invented_entry(tmp_path, field, count):
    folder, watch = recorded_watch(tmp_path)
    (folder / '1.touch.json').write_text(json.dumps({
        'evidence': 'exact', 'observed_price': '100',
        'price_as_of': '2026-11-11T00:01:00+00:00',
        'price_received_at': '2026-11-11T00:01:01+00:00',
    }))

    result = recover_market_date(
        date(2026, 11, 11), observation_root=folder.parent,
        ledger_root=tmp_path / 'ledger', watches=[replace(watch, **{field: None})],
    )

    assert result[count] == 1
    assert result['cohort_count'] == 1
    assert result['items'][0]['status'] == 'unavailable'
    assert result['items'][0]['realized_net_pnl_krw'] is None
    assert result['recovered_entry_count'] == 0
    if field == 'quantity':
        assert result['unquantified_entry_condition_count'] == 1
        assert result['items'][0]['recovery_evidence']['observed_entry_conditions'][0]['price'] == '100'


def test_unknown_timestamp_daily_ohlc_and_stale_prices_never_invent_entry(tmp_path):
    folder, watch = recorded_watch(tmp_path)
    (folder / '1.ask-entry.json').write_text(json.dumps({
        'ask_price': '100', 'ask_size': 9, 'price_as_of': None,
        'price_received_at': '2026-11-11T00:01:00+00:00',
    }))
    result = recover_market_date(
        date(2026, 11, 11), observation_root=folder.parent,
        ledger_root=tmp_path / 'ledger', watches=[watch], prices=[
            {'symbol': '130580', 'open': '100', 'low': '85', 'high': '115', 'close': '105'},
            {'symbol': '130580', 'price': '100', 'price_as_of': '2026-11-11T00:01:00+00:00',
             'received_at': '2026-11-11T00:02:00+00:00'},
        ],
    )

    assert result['recovered_entry_count'] == 0
    assert result['recovered_exit_count'] == 0
    assert result['evidence']['rejected_event_count'] == 3


def test_preview_and_imports_cannot_write_files(tmp_path):
    folder, watch = recorded_watch(tmp_path)
    # The input already carries immutable paper parameters, so this preview needs no DB.
    path = folder / '1.watch.json'
    raw = json.loads(path.read_text())
    raw.update({name: getattr(watch, name) for name in (
        'buy_direction', 'chase_limit_price', 'target_price', 'stop_loss_price',
        'quantity', 'quantity_source', 'round_trip_cost_bps',
    )})
    path.write_text(json.dumps(raw))
    script = '''
import json, os, sys
from datetime import date
from pathlib import Path
def deny_writes(event, args):
    if event == 'open':
        mode, flags = args[1], args[2]
        if (isinstance(mode, str) and any(x in mode for x in 'wax+')) or (flags & (os.O_CREAT | os.O_WRONLY | os.O_RDWR)):
            raise AssertionError('Preview attempted file write')
    if event in {'os.mkdir', 'os.remove', 'os.rename', 'os.link', 'os.symlink'}:
        raise AssertionError('Preview attempted filesystem mutation: ' + event)
sys.addaudithook(deny_writes)
from app.agent_context.wait_paper_recovery import recover_market_date
result = recover_market_date(date(2026, 11, 11), observation_root=Path(sys.argv[1]), ledger_root=Path(sys.argv[2]))
assert result['dry_run'] and result['cohort_count'] == 1
print('write-free')
'''

    result = subprocess.run(
        [sys.executable, '-B', '-c', script, str(folder.parent), str(tmp_path / 'ledger')],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'write-free'
    assert not (tmp_path / 'ledger').exists()


@pytest.mark.parametrize(('known_hour', 'known_minute', 'expected_entries'), [(5, 31, 1), (6, 20, 0)])
def test_quantity_known_after_registration_before_late_touch_can_enter(tmp_path, known_hour, known_minute, expected_entries):
    from app.agent_context.wait_paper_recovery import hydrate_recovery_watches

    folder, watch = recorded_watch(tmp_path)
    path = folder / '1.watch.json'
    raw = json.loads(path.read_text())
    raw['tracking_started_at'] = '2026-11-11T02:00:00+00:00'
    path.write_text(json.dumps(raw))
    (folder / '1.touch.json').write_text(json.dumps({
        'evidence': 'exact', 'observed_price': '100',
        'price_as_of': '2026-11-11T06:10:00+00:00',
        'price_received_at': '2026-11-11T06:10:01+00:00',
    }))
    plan = SimpleNamespace(
        decision_snapshot_id=1, symbol='130580', decision_agent_name='signal-decider',
        source_market_date=date(2026, 11, 10), target_trade_date=date(2026, 11, 11),
        broker_name='kis', execution_window='next_open', planned_side='buy', account_reference='test',
        created_at=datetime(2026, 11, 11, 5, 0, tzinfo=timezone.utc),
        updated_at=datetime(2026, 11, 11, known_hour, known_minute, tzinfo=timezone.utc),
        last_evaluated_at=datetime(2026, 11, 11, 5, 30, tzinfo=timezone.utc),
        planned_quantity=3, submission_payload=None,
    )
    raw.update({name: getattr(watch, name) for name in (
        'target_price', 'stop_loss_price', 'buy_direction', 'chase_limit_price',
    )})
    path.write_text(json.dumps(raw))

    watches = hydrate_recovery_watches(
        date(2026, 11, 11), load_cohort(folder.parent, date(2026, 11, 11)),
        snapshots={}, plans=[plan], fallback_cost_bps=20,
    )
    result = recover_market_date(
        date(2026, 11, 11), observation_root=folder.parent,
        ledger_root=tmp_path / 'ledger', watches=watches,
    )

    assert result['recovered_entry_count'] == expected_entries
    if expected_entries:
        assert result['items'][0]['entry']['quantity'] == '3'
    known_at = f'2026-11-11T{known_hour:02d}:{known_minute:02d}:00+00:00'
    assert result['items'][0]['tracking_started_at'] == known_at
    assert result['items'][0]['recovery_evidence']['quantity_known_at'] == known_at
    assert result['items'][0]['recovery_evidence']['entry_condition_status'] == 'observed'


def test_unknown_quantity_condition_visible_at_historical_cutoff_without_later_receipt_leak(tmp_path):
    from app.agent_context.wait_paper_ledger import WaitPaperLedger

    folder, watch = recorded_watch(tmp_path)
    (folder / '1.touch.json').write_text(json.dumps({
        'evidence': 'exact', 'observed_price': '100',
        'price_as_of': '2026-11-11T06:00:00+00:00',
        'price_received_at': '2026-11-11T06:00:01+00:00',
    }))
    (folder / '1.ask-entry.json').write_text(json.dumps({
        'ask_price': '100', 'ask_size': 3, 'within_chase_limit': True,
        'price_as_of': '2026-11-11T06:30:00+00:00',
        'price_received_at': '2026-11-11T06:30:02+00:00',
    }))
    recover_market_date(
        date(2026, 11, 11), observation_root=folder.parent, ledger_root=tmp_path / 'ledger',
        watches=[replace(watch, quantity=None, quantity_source=None)], apply=True,
    )

    at_close = WaitPaperLedger(tmp_path / 'ledger').history(
        date(2026, 11, 11), as_of=datetime(2026, 11, 11, 6, 30, tzinfo=timezone.utc),
    )[0]

    assert at_close['entry'] is None
    assert at_close['filled_quantity'] is None
    assert at_close['realized_net_pnl_krw'] is None
    assert at_close['recovery_evidence']['quantity_status'] == 'unknown'
    assert len(at_close['recovery_evidence']['observed_entry_conditions']) == 1
    assert at_close['recovery_evidence']['observed_entry_conditions'][0]['source'] == 'wait_touch'
