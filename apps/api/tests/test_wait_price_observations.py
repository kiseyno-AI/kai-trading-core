from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

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

    history = WaitPriceObservations(tmp_path).history(day)
    assert [(x['decision_snapshot_id'], x['status'], x['observed_price'], x['evidence'])
            for x in history] == [(1, 'touched', '98', 'crossed'), (2, 'touched', '105', 'exact')]
