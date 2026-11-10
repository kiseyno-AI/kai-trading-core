from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.agent_context import wait_price_observations
from app.agent_context.wait_price_observations import WaitPriceObservations, watch_from_snapshot
from app.quotes.kis_realtime_prices import KIS_REALTIME_PRICE_COLUMNS, KISRealtimePriceService


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
                batches.append(batch)
                return [
                    {'inter_shrn_iscd': symbol, 'inter2_askp': '99', 'inter2_bidp': '98', 'seln_rsqn': '3'}
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
        monkeypatch.setattr(kis_realtime_prices, '_utc_now', lambda: now)
        monkeypatch.setattr(kis_realtime_prices.settings, 'kis_realtime_price_enabled', True)
        prices = KISRealtimePriceService(client=Client())
        monkeypatch.setattr(prices, '_is_configured', lambda: (True, None))
        monkeypatch.setattr(prices, '_recorded_market_session_is_active', lambda: True)
        monkeypatch.setattr(prices, '_run', skip_trade_socket)
        async def wait_for_round():
            while 'wait_orderbook_observation' not in prices.health_snapshot():
                await asyncio.sleep(0.001)

        await prices.start()
        try:
            await asyncio.wait_for(wait_for_round(), 2)
            assert [len(batch) for batch in batches] == batch_sizes
            assert [symbol for batch in batches for symbol in batch] == symbols
            assert prices.health_snapshot()['wait_orderbook_observation']['sampled_count'] == symbol_count
            snapshot = prices.orderbooks_snapshot()
            assert snapshot['passive_quote_symbols'] == symbols
            assert snapshot['passive_quote_expires_at'] == (now + timedelta(seconds=10)).isoformat()
            expired = prices.orderbooks_snapshot(now=now + timedelta(seconds=11))
            assert expired['passive_quote_symbols'] == []
            assert expired['passive_quote_expires_at'] is None
            records = observer.history(now.date())
            assert len(records) == symbol_count
            assert all(row['ask_touch']['ask_size'] == 3 and row['status'] == 'not_observed' for row in records)
            assert all(row['ask_touch']['price_as_of'] is None for row in records)
            assert records[0]['ask_entry']['source'] == 'kis_orderbook_rest_confirmed'
            assert 'ask_entry' not in records[1]
        finally:
            await prices.stop()

    asyncio.run(check())
