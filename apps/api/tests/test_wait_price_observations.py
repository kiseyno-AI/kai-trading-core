from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.agent_context import wait_price_observations
from app.agent_context.wait_price_observations import WaitPriceObservations, watch_from_snapshot
from app.quotes.kis_realtime_prices import KIS_REALTIME_PRICE_COLUMNS, KISRealtimePriceService


def test_wait_watch_keeps_snapshot_exit_prices_even_below_execution_confidence():
    watch = watch_from_snapshot(SimpleNamespace(
        id=91, symbol='005930', agent_name='signal-decider',
        market_date=date(2026, 11, 9), decision_action='wait', next_day_open_action='wait',
        feedback_received_at=datetime(2026, 11, 9, 7, tzinfo=timezone.utc),
        created_at=datetime(2026, 11, 9, 7, tzinfo=timezone.utc),
        target_price=Decimal('115'), stop_loss_price=Decimal('90'), confidence_score_10=Decimal('4'),
        request_payload={'symbol_day_context': {'company': {'market': 'KOSPI'}}},
        response_payload={'trigger_price': '100'},
    ), market_date=date(2026, 11, 10))
    assert watch.target_price == '115'
    assert watch.stop_loss_price == '90'
    assert watch.quantity is None


def test_paper_reference_uses_known_frozen_quantity_then_stored_positive_plan_quantity():
    opened = datetime(2026, 11, 10, 0, tzinfo=timezone.utc)
    watch = wait_price_observations.WaitPriceWatch(
        91, 'signal-decider', '005930', '2026-11-09', opened.isoformat(), '100',
    )

    def plan(account, quantity, frozen=None, *, calculated_at=opened - timedelta(minutes=1)):
        return SimpleNamespace(
            decision_snapshot_id=91, decision_agent_name='signal-decider', symbol='005930',
            source_market_date=date(2026, 11, 9), target_trade_date=opened.date(),
            broker_name='kis', execution_window='next_open', planned_side='buy',
            account_reference=account, planned_quantity=quantity,
            created_at=opened - timedelta(hours=1), updated_at=opened - timedelta(minutes=1),
            submission_payload={'core_quantity_sizing': {'initial_policy': {
                'type': 'core_sized', 'target_quantity': frozen,
                'calculated_at': calculated_at.isoformat(),
                'sizing_inputs': {'round_trip_cost_bps': '50'},
            }}},
        )

    hydrated = wait_price_observations.watch_with_paper_plan(
        watch, [plan('a', '99', '3'), plan('b', '2', '0'), plan('c', '0', '0')],
        known_at=opened, fallback_cost_bps='50',
    )
    assert hydrated.quantity == '5'
    assert hydrated.quantity_account_count == 2
    assert hydrated.quantity_source == 'core_frozen_quantity+stored_planned_quantity'
    assert hydrated.round_trip_cost_bps == '50'
    missing = wait_price_observations.watch_with_paper_plan(watch, [plan('a', '0', '0')], known_at=opened)
    assert missing.quantity is None
    assert missing.quantity_source is None
    future = wait_price_observations.watch_with_paper_plan(
        watch, [plan('a', None, '5', calculated_at=opened + timedelta(seconds=1))], known_at=opened,
    )
    assert future.quantity is None


def test_live_wait_paper_reference_sizes_missing_quantity_from_known_budget_and_risk():
    opened = datetime(2026, 11, 10, 0, tzinfo=timezone.utc)
    watch = wait_price_observations.WaitPriceWatch(
        91, 'signal-decider', '005930', '2026-11-09', opened.isoformat(), '105',
        chase_limit_price='110', target_price='120', stop_loss_price='100',
    )
    plan = SimpleNamespace(
        decision_snapshot_id=91, decision_agent_name='signal-decider', symbol='005930',
        source_market_date=date(2026, 11, 9), broker_name='kis', execution_window='next_open',
        planned_side='buy', account_reference='a', allocation_budget_krw='100000', planned_quantity=None,
        created_at=opened - timedelta(hours=1), updated_at=opened - timedelta(minutes=1), submission_payload={},
    )
    historical = wait_price_observations.watch_with_paper_plan(watch, [plan], known_at=opened)
    assert historical.quantity is None
    live = wait_price_observations.watch_with_paper_plan(
        watch, [plan], known_at=opened, fallback_cost_bps='50',
        allow_reference_quantity=True, reference_risk_fraction='0.015',
    )
    assert live.quantity == '150'
    assert live.quantity_source == 'paper_budget_risk_reference_v1'
    assert live.quantity_basis['accounts'] == [{
        'account_index': 1, 'allocation_budget_krw': '100000', 'risk_fraction': '0.015',
        'sizing_entry_price': '110', 'stop_loss_price': '100', 'reference_quantity': '150',
    }]
    assert plan.planned_quantity is None
    plan.allocation_budget_krw = None
    assert wait_price_observations.watch_with_paper_plan(
        watch, [plan], known_at=opened, allow_reference_quantity=True, reference_risk_fraction='0.015',
    ).quantity is None
    plan.allocation_budget_krw = '100000'
    for invalid_watch in (replace(watch, stop_loss_price='105'), replace(watch, chase_limit_price='125')):
        assert wait_price_observations.watch_with_paper_plan(
            invalid_watch, [plan], known_at=opened,
            allow_reference_quantity=True, reference_risk_fraction='0.015',
        ).quantity is None
    plan.account_reference = 'KIS:00000000-00'
    assert wait_price_observations.watch_with_paper_plan(
        watch, [plan], known_at=opened, allow_reference_quantity=True, reference_risk_fraction='0.015',
    ).quantity is None
    plan.account_reference = 'a'
    plan.updated_at = opened + timedelta(seconds=1)
    assert wait_price_observations.watch_with_paper_plan(
        watch, [plan], known_at=opened, allow_reference_quantity=True, reference_risk_fraction='0.015',
    ).quantity is None


def test_paper_observer_receives_prices_after_raw_first_touch_and_after_watch_refresh(tmp_path):
    from unittest.mock import Mock

    opened = datetime(2026, 11, 10, 0, tzinfo=timezone.utc)
    paper = Mock()
    paper.symbols.return_value = ('005930',)
    observer = WaitPriceObservations(tmp_path, paper_ledger=paper)
    watch = wait_price_observations.WaitPriceWatch(
        91, 'signal-decider', '005930', '2026-11-09', opened.isoformat(), '100',
    )
    incomplete = wait_price_observations.WaitPriceWatch(
        92, 'short-term-decider', '000660', '2026-11-09', opened.isoformat(), None,
    )
    observer.configure(opened.date(), [watch, incomplete], now=opened)
    assert paper.configure.call_args.args[1] == [watch, incomplete]
    for second, price in [(1, '100'), (2, '105')]:
        instant = opened + timedelta(seconds=second)
        observer.observe(SimpleNamespace(
            symbol='005930', current_price=Decimal(price), trade_halt=False,
            price_as_of=instant, received_at=instant,
            raw_fields={'STCK_CNTG_HOUR': f'09000{second}'},
        ))
    assert paper.observe_trade.call_count == 2
    raw_touch = (tmp_path / '2026-11-10' / '91.touch.json').read_bytes()
    observer.configure(opened.date(), [], now=opened + timedelta(seconds=3))
    assert observer.symbols(opened.date()) == ('005930',)
    instant = opened + timedelta(seconds=4)
    observer.observe(SimpleNamespace(
        symbol='005930', current_price=Decimal('115'), trade_halt=False,
        price_as_of=instant, received_at=instant, raw_fields={'STCK_CNTG_HOUR': '090004'},
    ))
    assert paper.observe_trade.call_count == 3
    assert (tmp_path / '2026-11-10' / '91.touch.json').read_bytes() == raw_touch
    observer.observe_ask('005930', Decimal('105'), 4, price_as_of=None, received_at=instant, source='sample')
    paper.observe_ask.assert_not_called()
    observer.observe_ask('005930', Decimal('105'), 4, price_as_of=instant, received_at=instant, source='confirmed')
    paper.observe_ask.assert_called_once()


def test_live_trade_packet_prefers_timestamped_ask_entry_and_bid_exit(tmp_path, monkeypatch):
    opened = datetime(2026, 11, 10, 0, tzinfo=timezone.utc)
    observer = WaitPriceObservations(tmp_path)
    observer.configure(opened.date(), [wait_price_observations.WaitPriceWatch(
        91, 'signal-decider', '005930', '2026-11-09', opened.isoformat(), '100',
        buy_direction='at_or_below', chase_limit_price='103', target_price='110', stop_loss_price='90',
        quantity='5', quantity_source='stored_planned_quantity', round_trip_cost_bps='50',
    )], now=opened)
    monkeypatch.setattr(wait_price_observations, 'wait_price_observations', observer)
    prices = KISRealtimePriceService()

    def ingest(second, price, bid, ask, size):
        fields = {
            'MKSC_SHRN_ISCD': '005930', 'STCK_PRPR': price, 'BSOP_DATE': '20261110',
            'STCK_CNTG_HOUR': f'09000{second}', 'BIDP1': bid, 'ASKP1': ask,
            'ASKP_RSQN1': size, 'BIDP_RSQN1': size,
        }
        message = '0|H0STCNT0|001|' + '^'.join(fields.get(key, '') for key in KIS_REALTIME_PRICE_COLUMNS)
        prices.ingest_price_message(message, received_at=opened + timedelta(seconds=second))

    ingest(1, '100', '98', '99', '2')
    entry = observer.paper_ledger.history(opened.date())[0]
    assert entry['entry']['price'] == '99'
    assert entry['filled_quantity'] == '2'
    ingest(2, '109', '111', '112', '5')
    closed = observer.paper_ledger.history(opened.date())[0]
    assert closed['status'] == 'target_exited'
    assert closed['exit']['source'] == 'kis_trade_packet_bid'
    assert closed['realized_gross_pnl_krw'] == '24'


def test_paper_write_failure_does_not_block_price_ingestion_or_raw_touch(tmp_path, monkeypatch):
    from unittest.mock import Mock

    opened = datetime(2026, 11, 10, 0, tzinfo=timezone.utc)
    paper = Mock()
    paper.observe_trade.side_effect = OSError('disk full')
    observer = WaitPriceObservations(tmp_path, paper_ledger=paper)
    observer.configure(opened.date(), [wait_price_observations.WaitPriceWatch(
        91, 'signal-decider', '005930', '2026-11-09', opened.isoformat(), '100',
    )], now=opened)
    monkeypatch.setattr(wait_price_observations, 'wait_price_observations', observer)
    prices = KISRealtimePriceService()
    fields = {
        'MKSC_SHRN_ISCD': '005930', 'STCK_PRPR': '100', 'BSOP_DATE': '20261110',
        'STCK_CNTG_HOUR': '090001',
    }
    message = '0|H0STCNT0|001|' + '^'.join(fields.get(key, '') for key in KIS_REALTIME_PRICE_COLUMNS)
    result = prices.ingest_price_message(message, received_at=opened + timedelta(seconds=1))
    assert result[0]['current_price'] == 100
    assert observer.history(opened.date())[0]['status'] == 'touched'
    assert observer.paper_recording_error == 'OSError: disk full'


def test_open_paper_position_keeps_rest_confirmation_after_raw_ask_entry(tmp_path):
    opened = datetime(2026, 11, 10, 0, tzinfo=timezone.utc)
    observer = WaitPriceObservations(tmp_path)
    observer.configure(opened.date(), [wait_price_observations.WaitPriceWatch(
        91, 'signal-decider', '005930', '2026-11-09', opened.isoformat(), '100',
        buy_direction='at_or_below', chase_limit_price='103', target_price='110', stop_loss_price='90',
        quantity='5', quantity_source='stored_planned_quantity', round_trip_cost_bps='50',
    )], now=opened)
    entry_at = opened + timedelta(seconds=1)
    observer.observe_ask('005930', Decimal('99'), 5, price_as_of=entry_at, received_at=entry_at, source='confirmed')
    sampled_at = opened + timedelta(seconds=2)
    assert observer.observe_ask(
        '005930', Decimal('112'), 5, price_as_of=None, received_at=sampled_at, source='sample',
    ) is True
    observer.observe_bid(
        '005930', Decimal('111'), 5, price_as_of=sampled_at, received_at=sampled_at, source='confirmed',
    )
    assert observer.observe_ask(
        '005930', Decimal('112'), 5, price_as_of=None,
        received_at=opened + timedelta(seconds=3), source='sample',
    ) is False


def test_same_symbol_keeps_independent_first_touch_history(tmp_path, monkeypatch):
    day = date(2026, 11, 10)
    opened = datetime(2026, 11, 10, 0, 0, tzinfo=timezone.utc)
    observer = WaitPriceObservations(tmp_path)

    def watch(snapshot_id, role, trigger):
        return watch_from_snapshot(SimpleNamespace(
            id=snapshot_id, symbol='130580', agent_name=role,
            market_date=date(2026, 11, 9), decision_action='wait',
            next_day_open_action='wait', feedback_received_at=opened - timedelta(hours=1),
            created_at=opened - timedelta(hours=1), target_price=None, stop_loss_price=None,
            confidence_score_10=Decimal('4'),
            request_payload={'symbol_day_context': {'company': {'market': 'KOSPI'}}},
            response_payload={'trigger_price': trigger, 'tactical_action': 'wait'},
        ), market_date=day)

    watches = [watch(1, 'signal-decider', '100'), watch(2, 'short-term-decider', '105')]
    observer.enroll_recovery(day, watches)
    assert [x['status'] for x in observer.history(day)] == ['enrolled', 'enrolled']
    observer.configure(day, observer.recovery_watches(day), now=opened)
    monkeypatch.setattr(wait_price_observations, 'wait_price_observations', observer)
    prices = KISRealtimePriceService()

    def ingest(second, price, *, business_date='20261110'):
        fields = {
            'MKSC_SHRN_ISCD': '130580', 'STCK_PRPR': str(price),
            'BSOP_DATE': business_date, 'STCK_CNTG_HOUR': f'0900{second:02d}',
        }
        message = '0|H0STCNT0|001|' + '^'.join(fields.get(key, '') for key in KIS_REALTIME_PRICE_COLUMNS)
        prices.ingest_price_message(message, received_at=opened + timedelta(seconds=second))

    ingest(1, 100, business_date='20261109')
    ingest(1, 106)
    assert [x['status'] for x in observer.history(day)] == ['not_observed', 'not_observed']
    ingest(2, 105)
    ingest(3, 98)
    ingest(4, 100)

    stored = WaitPriceObservations(tmp_path)
    earlier = stored.history(day, as_of=opened + timedelta(seconds=2))
    assert [x['status'] for x in earlier] == ['not_observed', 'touched']
    history = stored.history(day)
    assert [(x['decision_snapshot_id'], x['status'], x['observed_price'], x['evidence'])
            for x in history] == [(1, 'touched', '98', 'crossed'), (2, 'touched', '105', 'exact')]


def test_ask_touch_and_entry_band_are_independent_of_trade_history(tmp_path):
    opened = datetime(2026, 11, 10, 0, 0, tzinfo=timezone.utc)
    observer = WaitPriceObservations(tmp_path)
    watch = wait_price_observations.WaitPriceWatch(
        1, 'short-term-decider', '130580', '2026-11-09',
        (opened - timedelta(hours=1)).isoformat(), '100',
        buy_direction='at_or_above', chase_limit_price='103',
    )
    observer.configure(opened.date(), [watch], now=opened)
    for second, price, size in [(1, '100', 0), (2, '104', 9), (3, '102', 7)]:
        now = opened + timedelta(seconds=second)
        observer.observe_ask('130580', Decimal(price), size, price_as_of=now, received_at=now, source='test')
    result = observer.history(opened.date())[0]
    assert result['status'] == 'not_observed'
    assert result['ask_touch']['ask_price'] == '104'
    assert result['ask_touch']['ask_size'] == 9
    assert result['ask_touch']['within_chase_limit'] is False
    assert result['ask_entry']['ask_price'] == '102'
    assert result['ask_entry']['ask_size'] == 7
    earlier = observer.history(opened.date(), as_of=opened + timedelta(seconds=2))[0]
    assert earlier['ask_touch']['ask_price'] == '104'
    assert 'ask_entry' not in earlier


@pytest.mark.parametrize(('symbol_count', 'batch_sizes'), [(35, [30, 5]), (100, [30, 30, 30, 10])])
def test_orderbook_sampling_covers_more_than_one_kis_batch(tmp_path, monkeypatch, symbol_count, batch_sizes):
    import asyncio

    from app.quotes import kis_realtime_prices

    async def check():
        now = datetime(2026, 11, 10, 4, 0, tzinfo=timezone.utc)
        sample_clock = now
        observer = WaitPriceObservations(tmp_path)
        symbols = [f'{index:06d}' for index in range(1, symbol_count + 1)]
        observer.configure(now.date(), [
            wait_price_observations.WaitPriceWatch(
                index, 'signal-decider', symbol, '2026-11-09',
                (now - timedelta(hours=5)).isoformat(), '100',
                buy_direction='at_or_below', chase_limit_price='100',
            )
            for index, symbol in enumerate(symbols, 1)
        ], now=now)
        batches = []

        class Client:
            def _environment(self):
                return 'real'

            def websocket_url(self):
                return 'ws://example.test'

            async def fetch_domestic_multi_quotes(self, batch):
                nonlocal sample_clock
                sample_clock = now + timedelta(seconds=len(batches))
                batches.append(batch)
                return [
                    {
                        'inter_shrn_iscd': symbol, 'inter2_askp': '99', 'inter2_bidp': '98',
                        'seln_rsqn': '3', 'shnu_rsqn': '4', 'inter2_prpr': '99',
                        'inter2_prdy_clpr': '100', 'inter2_prdy_vrss': '1',
                        'prdy_vrss_sign': '5', 'prdy_ctrt': '-1.00',
                    }
                    for symbol in batch
                ]

            async def fetch_domestic_orderbook(self, symbol):
                assert symbol == symbols[0]
                return {'output1': {
                    'askp1': '99', 'bidp1': '98', 'askp_rsqn1': '3', 'aspr_acpt_hour': '130000',
                }}

        async def skip_trade_socket():
            pass

        monkeypatch.setattr(wait_price_observations, 'wait_price_observations', observer)
        monkeypatch.setattr(kis_realtime_prices, '_utc_now', lambda: sample_clock)
        monkeypatch.setattr(kis_realtime_prices.settings, 'kis_realtime_price_enabled', True)
        prices = KISRealtimePriceService(client=Client())
        monkeypatch.setattr(prices, '_is_configured', lambda: (True, None))
        monkeypatch.setattr(prices, '_recorded_market_session_is_active', lambda: True)
        monkeypatch.setattr(prices, '_run', skip_trade_socket)
        prices.observe_monitored_symbols(symbols, trigger='test')
        async def wait_for_round():
            while 'wait_orderbook_observation' not in prices.health_snapshot():
                await asyncio.sleep(0.001)

        await prices.start()
        try:
            await asyncio.wait_for(wait_for_round(), 2)
            assert [len(batch) for batch in batches] == batch_sizes
            assert [symbol for batch in batches for symbol in batch] == symbols
            assert prices.health_snapshot()['wait_orderbook_observation']['sampled_count'] == symbol_count
            quotes = prices.monitored_snapshot()['items']
            assert len(quotes) == symbol_count
            last = quotes[-1]
            assert last['current_price'] == 99
            assert last['bid_price'] == 98
            assert last['ask_price'] == 99
            assert last['change'] == -1
            assert last['change_pct'] == -1
            assert last['previous_close_price'] == 100
            assert last['price_source'] == 'kis_rest_multi_quote'
            assert last['price_time_basis'] == 'received_at'
            assert last['price_status'] == 'available'
            assert last['display_price_available'] is True
            assert last['realtime_subscribed'] is False
            assert prices._prices == {}
            assert all(item['status'] == 'not_observed' for item in observer.history(now.date()))
            snapshot = prices.orderbooks_snapshot()
            assert snapshot['passive_quote_symbols'] == symbols
            assert snapshot['passive_quote_expires_at'] == (now + timedelta(seconds=10)).isoformat()
            # The end of the sampling round is later than its first usable batch.
            assert prices.health_snapshot()['wait_orderbook_observation']['last_sample_at'] != now.isoformat()
            prices._record_market_session(SimpleNamespace(
                state='OPEN', session_close_at=(now + timedelta(seconds=6)).isoformat(),
            ), active=True)
            assert prices.orderbooks_snapshot()['passive_quote_expires_at'] == (now + timedelta(seconds=6)).isoformat()
            expired = prices.orderbooks_snapshot(now=now + timedelta(seconds=6))
            assert expired['passive_quote_symbols'] == []
            assert expired['passive_quote_expires_at'] is None
            records = observer.history(now.date())
            assert len(records) == symbol_count
            assert all(row['ask_touch']['ask_size'] == 3 and row['status'] == 'not_observed' for row in records)
            assert all(row['ask_touch']['price_as_of'] is None for row in records)
            assert records[0]['ask_entry']['source'] == 'kis_orderbook_rest_confirmed'
            assert 'ask_entry' not in records[1]
            monkeypatch.setattr(prices, '_recorded_market_session_is_active', lambda: False)
            assert prices.monitored_snapshot()['items'][-1]['display_price_available'] is False
            monkeypatch.setattr(prices, '_recorded_market_session_is_active', lambda: True)
            sample_clock += timedelta(seconds=120)
            expired_price = prices.monitored_snapshot()['items'][-1]
            assert expired_price['display_price_available'] is False
            assert expired_price['current_price'] is None
        finally:
            await prices.stop()

    asyncio.run(check())
