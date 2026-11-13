from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from app.agent_context.wait_paper_ledger import WaitPaperLedger

DAY = date(2026, 11, 10)
OPEN = datetime(2026, 11, 10, tzinfo=timezone.utc)


def watch(key=1, role='signal-decider', **values):
    return SimpleNamespace(**{
        'decision_snapshot_id': key, 'decision_agent_name': role, 'symbol': '130580',
        'source_market_date': '2026-11-09', 'observed_at': (OPEN-timedelta(hours=1)).isoformat(),
        'trigger_price': '100', 'buy_direction': 'at_or_below', 'chase_limit_price': '103',
        'target_price': '110', 'stop_loss_price': '90', 'quantity': '10',
        'quantity_source': 'core_plan', 'round_trip_cost_bps': '50', **values,
    })


def trade(ledger, second, price):
    now = OPEN + timedelta(seconds=second)
    ledger.observe_trade('130580', Decimal(price), price_as_of=now, received_at=now)


def test_both_roles_enter_once_and_realize_target_with_per_leg_cost(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch(), watch(2, 'short-term-decider')], now=OPEN)
    trade(ledger, 1, '100')
    trade(ledger, 2, '110')
    rows = ledger.history(DAY)
    assert [row['status'] for row in rows] == ['target_exited', 'target_exited']
    assert rows[0]['realized_gross_pnl_krw'] == '100'
    assert Decimal(rows[0]['cost_krw']) == Decimal('5.25')
    assert Decimal(rows[0]['realized_net_pnl_krw']) == Decimal('94.75')
    assert rows[0]['unrealized_net_pnl_krw'] is None
    assert rows[0]['entry']['execution_model'] == 'trade_touch_simulation'
    assert rows[0]['entry']['liquidity_verified'] is False
    assert ledger.history(DAY, as_of=OPEN+timedelta(seconds=1))[0]['status'] == 'open'


def test_ask_partial_fill_stop_and_frozen_parameters(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch()], now=OPEN)
    now = OPEN+timedelta(seconds=1)
    ledger.observe_ask('130580', Decimal('99'), 3, price_as_of=now, received_at=now, source='kis_ask')
    ledger.configure(DAY, [watch(quantity='100', round_trip_cost_bps='100')], now=now)
    trade(ledger, 2, '89')
    row = ledger.history(DAY)[0]
    assert row['status'] == 'stop_exited'
    assert row['filled_quantity'] == '3'
    assert row['unfilled_quantity'] == '7'
    assert row['quantity'] == '10'
    assert row['round_trip_cost_bps'] == '50'
    assert Decimal(row['realized_gross_pnl_krw']) == Decimal('-30')
    assert Decimal(row['realized_net_pnl_krw']) == Decimal('-31.41')
    assert row['entry']['liquidity_verified'] is True
    assert 'quantity' in row['input_drift']


def test_hot_path_marks_coalesced_but_exit_immediate_and_terminal_immutable(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch()], now=OPEN)
    trade(ledger, 1, '100')
    for second in range(2, 59):
        trade(ledger, second, '101')
    assert len(ledger.history(DAY)[0]['events']) <= 2
    trade(ledger, 59, '110')
    path = tmp_path / DAY.isoformat() / '1.json'
    before = path.read_bytes()
    for second in range(60, 70):
        trade(ledger, second, '90')
    assert path.read_bytes() == before
    assert ledger.history(DAY)[0]['status'] == 'target_exited'


def test_restart_open_is_unknown_and_asof_does_not_leak_gap(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch()], now=OPEN)
    trade(ledger, 1, '100')
    restarted = WaitPaperLedger(tmp_path)
    restarted.configure(DAY, [], now=OPEN+timedelta(seconds=5))
    trade(restarted, 6, '110')
    row = restarted.history(DAY)[0]
    assert row['status'] == 'open'
    assert row['coverage'] == 'incomplete'
    assert row['exit_status'] == 'unknown_path_gap'
    assert row['realized_net_pnl_krw'] is None
    assert row['unrealized_net_pnl_krw'] is None
    assert row['active_tracking'] is False
    assert restarted.symbols(DAY) == ()
    assert restarted.history(DAY, as_of=OPEN+timedelta(seconds=1))[0]['coverage'] == 'complete'


def test_unavailable_can_acquire_quantity_without_replaying_old_entry(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch(quantity=None)], now=OPEN)
    assert ledger.history(DAY)[0]['missing_inputs'] == ['quantity']
    ledger.configure(DAY, [watch()], now=OPEN+timedelta(seconds=5))
    trade(ledger, 1, '100')
    assert ledger.history(DAY)[0]['status'] == 'pending'
    trade(ledger, 6, '100')
    assert ledger.history(DAY)[0]['status'] == 'open'
    assert ledger.history(DAY, as_of=OPEN+timedelta(seconds=2))[0]['status'] == 'unavailable'


def test_cross_day_carries_entered_only_and_dates_include_exit_session(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch(), watch(2, trigger_price='95')], now=OPEN)
    trade(ledger, 1, '100')
    tomorrow = DAY+timedelta(days=1)
    ledger.configure(tomorrow, [], now=OPEN+timedelta(days=1))
    assert ledger.symbols(tomorrow) == ('130580',)
    next_tick = OPEN+timedelta(days=1, seconds=1)
    ledger.observe_trade('130580', Decimal('111'), price_as_of=next_tick, received_at=next_tick)
    assert ledger.history(DAY)[0]['status'] == 'open'
    carried = ledger.history(tomorrow)
    assert len(carried) == 1
    assert carried[0]['status'] == 'target_exited'
    assert carried[0]['market_date'] == DAY.isoformat()
    assert tomorrow in ledger.dates()
    assert ledger.history(tomorrow+timedelta(days=1)) == []


def test_stale_process_cache_observes_other_process_entry_and_exits(tmp_path):
    first, second = WaitPaperLedger(tmp_path), WaitPaperLedger(tmp_path)
    first.configure(DAY, [watch(stop_loss_price='80')], now=OPEN)
    second.configure(DAY, [watch(stop_loss_price='80')], now=OPEN)
    trade(first, 1, '90')
    trade(second, 2, '100')
    first.configure(DAY, [watch(stop_loss_price='80')], now=OPEN+timedelta(seconds=2))
    trade(first, 3, '110')
    assert first.history(DAY)[0]['status'] == 'target_exited'
    trade(second, 4, '80')
    assert second.history(DAY)[0]['status'] == 'target_exited'


def test_sparse_replay_and_dry_run_never_write_existing_lots(tmp_path):
    live = WaitPaperLedger(tmp_path)
    live.configure(DAY, [watch()], now=OPEN)
    trade(live, 1, '100')
    before = (tmp_path / DAY.isoformat() / '1.json').read_bytes()
    dry = WaitPaperLedger(tmp_path, persist=False, scope={(DAY, 2)})
    dry.configure(DAY, [watch(), watch(2)], now=OPEN)
    dry.mark_path_gap(DAY, 2, now=OPEN)
    trade(dry, 1, '100')
    trade(dry, 2, '110')
    row = dry.history(DAY)[0]
    assert row['decision_snapshot_id'] == 2
    assert row['status'] == 'open'
    assert row['coverage'] == 'incomplete'
    assert row['realized_net_pnl_krw'] is None
    assert (tmp_path / DAY.isoformat() / '1.json').read_bytes() == before
    assert not (tmp_path / DAY.isoformat() / '2.json').exists()


def test_directional_crossing_out_of_order_and_untrusted_ticks(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch(buy_direction='at_or_above', stop_loss_price='80')], now=OPEN)
    trade(ledger, 1, '99')
    trade(ledger, 2, '101')
    trade(ledger, 1, '90')
    row = ledger.history(DAY)[0]
    assert row['entry']['price'] == '101'
    assert row['entry']['evidence'] == 'crossed'
    assert row['status'] == 'open'
    stale = OPEN+timedelta(seconds=3)
    ledger.observe_trade('130580', Decimal('90'), price_as_of=stale, received_at=stale+timedelta(seconds=31))
    ledger.observe_trade('130580', Decimal('NaN'), price_as_of=stale, received_at=stale)
    ledger.observe_trade('130580', Decimal('90'), price_as_of=stale, received_at=stale, trade_halt=True)
    assert ledger.history(DAY)[0]['status'] == 'open'


def test_same_snapshot_never_reenrolls_after_rollover(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch(), watch(2, trigger_price='95')], now=OPEN)
    trade(ledger, 1, '100')
    trade(ledger, 2, '110')
    tomorrow = DAY+timedelta(days=1)
    ledger.configure(tomorrow, [watch(), watch(2, trigger_price='95')], now=OPEN+timedelta(days=1))
    assert ledger.symbols(tomorrow) == ()
    assert ledger.history(tomorrow) == []
    assert not (tmp_path / tomorrow.isoformat() / '1.json').exists()
    assert not (tmp_path / tomorrow.isoformat() / '2.json').exists()


def test_recovery_annotation_preserves_unknown_quantity_and_is_idempotent(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch(quantity=None)], now=OPEN)
    evidence = {'quantity_status': 'unknown', 'observed_conditions': [{'price': '100'}]}
    ledger.annotate_recovery(DAY, 1, now=OPEN+timedelta(seconds=1), evidence=evidence)
    path = tmp_path / DAY.isoformat() / '1.json'
    saved = path.read_bytes()
    ledger.annotate_recovery(DAY, 1, now=OPEN+timedelta(seconds=2), evidence=evidence)
    assert path.read_bytes() == saved
    row = ledger.history(DAY)[0]
    assert row['recovery_evidence'] == evidence
    assert row['status'] == 'unavailable'
    assert row['entry'] is None
    assert row['quantity'] is None
    assert row['realized_net_pnl_krw'] is None
    assert 'recovery_evidence' not in ledger.history(DAY, as_of=OPEN)[0]
    dry = WaitPaperLedger(tmp_path, persist=False)
    dry.annotate_recovery(DAY, 1, now=OPEN+timedelta(seconds=3), evidence={'quantity_status': 'still_unknown'})
    assert dry.history(DAY)[0]['recovery_evidence']['quantity_status'] == 'still_unknown'
    assert path.read_bytes() == saved


def test_bid_depth_must_cover_exit_and_ask_cannot_hide_trade_exit(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch()], now=OPEN)
    trade(ledger, 1, '100')
    now = OPEN+timedelta(seconds=2)
    ledger.observe_bid('130580', Decimal('110'), 1, price_as_of=now, received_at=now, source='kis_bid')
    assert ledger.history(DAY)[0]['status'] == 'open'
    later = OPEN+timedelta(seconds=3)
    ledger.observe_ask('130580', Decimal('120'), 30, price_as_of=later, received_at=later, source='kis_ask')
    ledger.observe_trade('130580', Decimal('111'), price_as_of=later, received_at=later)
    assert ledger.history(DAY)[0]['status'] == 'target_exited'


def test_isolated_last_quote_never_proves_entry_or_exit(tmp_path):
    ledger = WaitPaperLedger(tmp_path)
    ledger.configure(DAY, [watch()], now=OPEN)
    now = OPEN+timedelta(seconds=1)
    ledger.observe_mark('130580', Decimal('100'), price_as_of=now, received_at=now, source='saved_last_quote')
    assert ledger.history(DAY)[0]['entry'] is None
    trade(ledger, 2, '100')
    now = OPEN+timedelta(seconds=3)
    ledger.observe_mark('130580', Decimal('110'), price_as_of=now, received_at=now, source='saved_last_quote')
    assert ledger.history(DAY)[0]['status'] == 'open'
