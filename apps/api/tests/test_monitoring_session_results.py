from datetime import date, datetime
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.agent_context import wait_price_observations
from app.agent_context.wait_price_observations import WaitPriceObservations, WaitPriceWatch
from app.api.routes_execution_runtime import router
from app.db import session_scope
from app.db_models import DailyPriceBar, SignalDeciderDecisionSnapshot, SignalDeciderExecutionPlanItem, Symbol
from app.orders import monitoring_session_results
from app.quotes import monitoring_session_history
from app.quotes.kis_realtime_prices import parse_realtime_price_fields
from app.quotes.monitoring_session_history import MonitoringLastQuote, MonitoringSessionHistory


RESULTS_PATH = '/execution/runtime/monitoring-session-results'
TRADE_DATE = date(2026, 11, 6)
PREVIOUS_DATE = date(2026, 11, 5)
SYMBOL = '987654'


@pytest.fixture
def tracked_symbol(tmp_path, monkeypatch):
    history = WaitPriceObservations(tmp_path / 'waits')
    history.configure(
        TRADE_DATE,
        [WaitPriceWatch(
            decision_snapshot_id=1,
            decision_agent_name='signal-decider',
            symbol=SYMBOL,
            source_market_date=PREVIOUS_DATE.isoformat(),
            observed_at='2026-11-05T21:00:00+09:00',
            trigger_price='10000',
        )],
        now=datetime.fromisoformat('2026-11-06T09:01:00+09:00'),
    )
    monkeypatch.setattr(wait_price_observations, 'wait_price_observations', history)
    with session_scope() as session:
        row = Symbol(symbol=SYMBOL, market='KOSPI', name_ko='Monitoring fixture')
        session.add(row)
        session.flush()
        symbol_id = row.id
    try:
        yield symbol_id
    finally:
        with session_scope() as session:
            session.execute(delete(SignalDeciderDecisionSnapshot).where(
                SignalDeciderDecisionSnapshot.symbol == SYMBOL,
                SignalDeciderDecisionSnapshot.market_date == PREVIOUS_DATE,
            ))
            session.execute(delete(Symbol).where(Symbol.id == symbol_id))


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(
        monitoring_session_history, 'monitoring_session_history', MonitoringSessionHistory(tmp_path / 'archive'),
    )
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _store_bar(symbol_id: int, trade_date: date, close_price: int) -> None:
    with session_scope() as session:
        session.add(DailyPriceBar(
            symbol_id=symbol_id, trade_date=trade_date, open_price=close_price,
            high_price=close_price, low_price=close_price, close_price=close_price,
            volume=100, turnover=100 * close_price, source='pykrx',
        ))


def test_session_close_compares_exact_krx_trading_dates(client, tracked_symbol):
    _store_bar(tracked_symbol, PREVIOUS_DATE, 10_000)
    _store_bar(tracked_symbol, TRADE_DATE, 10_500)
    _store_bar(tracked_symbol, date(2026, 11, 9), 11_800)

    response = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()})

    assert response.status_code == 200
    result = response.json()
    assert result['market_date'] == TRADE_DATE.isoformat()
    assert result['previous_trade_date'] == PREVIOUS_DATE.isoformat()
    assert result['items'] == [{
        'symbol': SYMBOL,
        'price_status': 'close',
        'close_price': 10_500,
        'close_trade_date': TRADE_DATE.isoformat(),
        'previous_close_price': 10_000,
        'previous_close_trade_date': PREVIOUS_DATE.isoformat(),
        'last_price': None,
        'last_price_at': None,
        'change': 500,
        'change_pct': 5.0,
    }]
    assert result['hypothetical_return'] == {
        'return_pct': None, 'included_symbol_count': 0, 'monitored_symbol_count': 1,
    }


def test_last_quote_survives_restart_and_is_replaced_by_the_daily_close(
    client, tracked_symbol, tmp_path, monkeypatch,
):
    _set_clock(monkeypatch, '2026-11-06T15:31:00+09:00')
    _store_bar(tracked_symbol, PREVIOUS_DATE, 10_000)
    _store_bar(tracked_symbol, date(2026, 11, 9), 15_000)
    archive_dir = tmp_path / 'sessions'
    last_at = datetime.fromisoformat('2026-11-06T15:29:55+09:00')
    MonitoringSessionHistory(archive_dir).record(TRADE_DATE, {
        SYMBOL: MonitoringLastQuote(Decimal('10500'), last_at),
        '987653': None,
    })
    MonitoringSessionHistory(archive_dir).record(TRADE_DATE, {
        SYMBOL: MonitoringLastQuote(Decimal('9900'), last_at.replace(minute=28)),
    })
    monkeypatch.setattr(
        monitoring_session_history, 'monitoring_session_history', MonitoringSessionHistory(archive_dir),
    )

    result = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()}).json()
    by_symbol = {row['symbol']: row for row in result['items']}
    row = by_symbol[SYMBOL]
    assert row['price_status'] == 'last_quote'
    assert row['close_price'] is None
    assert row['close_trade_date'] is None
    assert row['last_price'] == 10_500
    assert datetime.fromisoformat(row['last_price_at']) == last_at
    assert row['change'] == 500 and row['change_pct'] == 5.0
    missing = by_symbol['987653']
    assert missing['price_status'] == 'unavailable'
    assert missing['change'] is None and missing['change_pct'] is None
    assert result['hypothetical_return']['monitored_symbol_count'] == 2

    _set_clock(monkeypatch, '2026-11-06T20:30:00+09:00')
    _store_bar(tracked_symbol, TRADE_DATE, 10_000)
    result = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()}).json()
    row = next(item for item in result['items'] if item['symbol'] == SYMBOL)
    assert row['price_status'] == 'close'
    assert row['close_price'] == 10_000
    assert row['change'] == 0 and row['change_pct'] == 0.0


def test_reads_the_latest_verified_trade_in_existing_wait_history(client, tracked_symbol, monkeypatch):
    _set_clock(monkeypatch, '2026-11-06T15:31:00+09:00')
    _store_bar(tracked_symbol, PREVIOUS_DATE, 9_500)
    archive = monitoring_session_history.monitoring_session_history
    archive.record(TRADE_DATE, {
        SYMBOL: MonitoringLastQuote(Decimal('9900'), datetime.fromisoformat('2026-11-06T10:00:00+09:00')),
    })
    touched_at = datetime.fromisoformat('2026-11-06T14:00:00+09:00')
    tick = parse_realtime_price_fields({
        'MKSC_SHRN_ISCD': SYMBOL, 'STCK_PRPR': '10000', 'BSOP_DATE': '20261106',
        'STCK_CNTG_HOUR': '140000',
    }, received_at=touched_at)
    assert tick is not None
    wait_price_observations.wait_price_observations.observe(tick)

    row = client.get(RESULTS_PATH).json()['items'][0]

    assert row['price_status'] == 'last_quote'
    assert row['last_price'] == 10_000
    assert datetime.fromisoformat(row['last_price_at']) == touched_at
    assert row['change'] == 500


def _store_decision(session, agent: str, *, action: str, trigger: int, decided_at: datetime, market_date=PREVIOUS_DATE):
    snapshot = SignalDeciderDecisionSnapshot(
        market_date=market_date, symbol=SYMBOL, agent_name=agent, thesis_summary='fixture',
        created_at=decided_at, feedback_received_at=decided_at,
        next_day_open_action=action if agent == 'signal-decider' else 'wait',
        supporting_evidence_payload=[], decision_risks_payload=[], invalidation_conditions_payload=[],
        data_quality_warnings_payload=[], specialist_disagreement_payload=[], traceability_payload={},
        input_feature_snapshot_payload={}, request_payload={}, response_summary_payload={},
        response_payload={'tactical_action': action, 'trigger_price': trigger},
    )
    session.add(snapshot)
    session.flush()
    return snapshot


def _store_plan(
    session, agent: str, *, action: str, quantity: int, trigger: int, planned_at: str,
) -> WaitPriceWatch:
    decided_at = datetime.fromisoformat('2026-11-05T21:00:00+09:00')
    evaluated_at = datetime.fromisoformat(planned_at)
    snapshot = _store_decision(session, agent, action=action, trigger=trigger, decided_at=decided_at)
    targets = [TRADE_DATE, date(2026, 11, 9)] if agent == 'signal-decider' else [TRADE_DATE]
    for target_date in targets:
        session.add(SignalDeciderExecutionPlanItem(
            target_trade_date=target_date, source_market_date=PREVIOUS_DATE,
            symbol=SYMBOL, decision_agent_name=agent, decision_snapshot_id=snapshot.id,
            account_reference='fixture-private-account', policy_version='fixture',
            allocation_budget_krw=500_000,
            planned_quantity=quantity if target_date == TRADE_DATE else quantity * 100,
            traceability_payload={'raw_action': 'wait', 'trigger_price': trigger},
            created_at=evaluated_at, last_evaluated_at=evaluated_at,
        ))
    return WaitPriceWatch(
        decision_snapshot_id=snapshot.id, decision_agent_name=agent, symbol=SYMBOL,
        source_market_date=PREVIOUS_DATE.isoformat(), observed_at=decided_at.isoformat(),
        trigger_price=str(trigger),
    )


@pytest.mark.parametrize(('short_action', 'short_quantity', 'short_time', 'return_pct'), [
    ('wait', 1, '08:15:00', 5.0),
    ('wait', 0, '08:15:00', 5.0),
    ('reject', 1, '08:15:00', 5.0),
    ('avoid_chase', 1, '08:15:00', 5.0),
    ('wait', 1, '15:31:00', 5.0),
])
def test_hypothetical_return_uses_long_term_once_regardless_of_execution_quantity(
    client, tracked_symbol, short_action, short_quantity, short_time, return_pct,
):
    _store_bar(tracked_symbol, TRADE_DATE, 10_500)
    with session_scope() as session:
        long_watch = _store_plan(
            session, 'signal-decider', action='wait', quantity=2, trigger=10_000,
            planned_at='2026-11-06T08:15:00+09:00',
        )
        short_watch = _store_plan(
            session, 'short-term-decider', action=short_action, quantity=short_quantity, trigger=8_000,
            planned_at=f'2026-11-06T{short_time}+09:00',
        )
    wait_price_observations.wait_price_observations.configure(
        TRADE_DATE, [long_watch, short_watch], now=datetime.fromisoformat('2026-11-06T09:02:00+09:00'),
    )

    response = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()})

    assert response.status_code == 200
    assert response.json()['hypothetical_return'] == {
        'return_pct': return_pct, 'included_symbol_count': 1, 'monitored_symbol_count': 1,
    }
    assert 'fixture-private-account' not in response.text
    assert 'quantity' not in response.text


@pytest.mark.parametrize(('rejected_at', 'return_pct'), [
    ('2026-11-06T15:29:00+09:00', 31.25),
    ('2026-11-06T15:31:00+09:00', 5.0),
])
def test_newer_reject_only_replaces_wait_if_known_before_close(
    client, tracked_symbol, rejected_at, return_pct,
):
    _store_bar(tracked_symbol, TRADE_DATE, 10_500)
    planned_at = '2026-11-06T08:15:00+09:00'
    with session_scope() as session:
        watches = [
            _store_plan(session, 'signal-decider', action='wait', quantity=2, trigger=10_000, planned_at=planned_at),
            _store_plan(session, 'short-term-decider', action='wait', quantity=1, trigger=8_000, planned_at=planned_at),
        ]
        _store_decision(
            session, 'signal-decider', action='reject', trigger=10_000,
            decided_at=datetime.fromisoformat(rejected_at),
        )
    wait_price_observations.wait_price_observations.configure(
        TRADE_DATE, watches, now=datetime.fromisoformat('2026-11-06T09:02:00+09:00'),
    )

    result = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()}).json()
    assert result['hypothetical_return']['return_pct'] == return_pct


def _set_clock(monkeypatch, timestamp: str) -> None:
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromisoformat(timestamp).astimezone(tz)

    monkeypatch.setattr(monitoring_session_results, 'datetime', FixedDatetime)


@pytest.mark.parametrize('timestamp', [
    '2026-11-07T12:00:00+09:00',
    '2026-11-09T08:50:00+09:00',
])
def test_default_keeps_last_session_until_next_open(client, tracked_symbol, monkeypatch, timestamp):
    _set_clock(monkeypatch, timestamp)
    next_session = date(2026, 11, 9)
    watch = WaitPriceWatch(
        decision_snapshot_id=8, decision_agent_name='signal-decider', symbol='987653',
        source_market_date=TRADE_DATE.isoformat(), observed_at='2026-11-06T21:00:00+09:00',
        trigger_price='1234',
    )
    history = wait_price_observations.wait_price_observations
    history.enroll_recovery(next_session, [watch])
    history.configure(
        next_session, [watch], now=datetime.fromisoformat('2026-11-09T08:45:00+09:00'),
    )

    result = client.get(RESULTS_PATH).json()

    assert result['market_date'] == TRADE_DATE.isoformat()
    assert [row['symbol'] for row in result['items']] == [SYMBOL]


def test_holiday_default_uses_last_session_and_explicit_holiday_is_empty(client, monkeypatch):
    _set_clock(monkeypatch, '2026-07-17T13:00:00+09:00')
    monitoring_session_history.monitoring_session_history.record(date(2026, 7, 16), {SYMBOL: None})

    assert client.get(RESULTS_PATH).json()['market_date'] == '2026-07-16'
    result = client.get(RESULTS_PATH, params={'market_date': '2026-07-17'}).json()
    assert result['market_date'] == '2026-07-17'
    assert result['items'] == []


def test_equal_capital_return_includes_each_wait_symbol_without_execution_plans(client, tracked_symbol, monkeypatch):
    _set_clock(monkeypatch, '2026-11-06T15:31:00+09:00')
    other = '987652'
    with session_scope() as session:
        row = Symbol(symbol=other, market='KOSPI', name_ko='Equal capital fixture')
        session.add(row)
        session.flush()
        other_id = row.id
        first = _store_decision(session, 'signal-decider', action='wait', trigger=100,
            decided_at=datetime.fromisoformat('2026-11-05T21:00:00+09:00'))
        second = _store_decision(session, 'signal-decider', action='wait', trigger=1000,
            decided_at=datetime.fromisoformat('2026-11-05T21:00:00+09:00'))
        second.symbol = other
        watches = [WaitPriceWatch(
            decision_snapshot_id=snapshot.id, decision_agent_name='signal-decider',
            symbol=symbol, source_market_date=PREVIOUS_DATE.isoformat(),
            observed_at='2026-11-05T21:00:00+09:00', trigger_price=str(trigger),
        ) for snapshot, symbol, trigger in [(first, SYMBOL, 100), (second, other, 1000)]]
    try:
        wait_price_observations.wait_price_observations.configure(
            TRADE_DATE, watches, now=datetime.fromisoformat('2026-11-06T09:02:00+09:00'),
        )
        _store_bar(tracked_symbol, TRADE_DATE, 110)
        _store_bar(other_id, TRADE_DATE, 900)
        result = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()}).json()
        # Equal money buys ten times as many of the first stock: +10% and -10% average to zero.
        assert result['hypothetical_return'] == {
            'return_pct': 0.0, 'included_symbol_count': 2, 'monitored_symbol_count': 2,
        }
    finally:
        with session_scope() as session:
            session.execute(delete(SignalDeciderDecisionSnapshot).where(
                SignalDeciderDecisionSnapshot.symbol == other,
                SignalDeciderDecisionSnapshot.market_date == PREVIOUS_DATE,
            ))
            session.execute(delete(Symbol).where(Symbol.id == other_id))


@pytest.mark.parametrize(('sample_time', 'status', 'source', 'expected'), [
    ('2026-11-06T10:00:00+09:00', 'available', 'kis_rest_multi_quote', 10.0),
    ('2026-11-06T10:00:00+09:00', 'available', 'kis_realtime_ws', 10.0),
    ('2026-11-06T10:00:00+09:00', 'stale', 'kis_rest_multi_quote', 1.0),
    ('2026-11-06T10:00:01+09:00', 'available', 'kis_rest_multi_quote', 1.0),
    ('2026-11-06T08:59:00+09:00', 'available', 'kis_rest_multi_quote', 1.0),
    ('2026-11-05T10:00:00+09:00', 'available', 'kis_rest_multi_quote', 1.0),
    ('2026-11-06T10:00:00', 'available', 'kis_rest_multi_quote', 1.0),
    ('2026-11-06T10:00:00+09:00', 'available', 'unknown', 1.0),
])
def test_current_valuation_uses_fresh_display_quotes_without_recording_a_trade(
    client, tracked_symbol, monkeypatch, sample_time, status, source, expected,
):
    _set_clock(monkeypatch, '2026-11-06T10:00:00+09:00')
    with session_scope() as session:
        watch = _store_plan(session, 'signal-decider', action='wait', quantity=0,
            trigger=10000, planned_at='2026-11-06T08:15:00+09:00')
    wait_price_observations.wait_price_observations.configure(
        TRADE_DATE, [watch], now=datetime.fromisoformat('2026-11-06T09:02:00+09:00'),
    )
    archive = monitoring_session_history.monitoring_session_history
    archive.record(TRADE_DATE, {SYMBOL: MonitoringLastQuote(
        Decimal('10100'), datetime.fromisoformat('2026-11-06T09:59:00+09:00'),
    )})
    monkeypatch.setattr(monitoring_session_results.kis_realtime_price_service, 'price_fields_for_symbol',
        lambda symbol: {'current_price': 11000, 'price_as_of': sample_time, 'price_status': status,
                        'price_source': source, 'display_price_available': True})
    before = archive.read(TRADE_DATE)
    response = client.get(RESULTS_PATH)
    result = response.json()
    assert result['hypothetical_return']['return_pct'] == expected
    assert result['hypothetical_return']['included_symbol_count'] == 1
    assert result['items'][0]['last_price'] == 10100
    assert datetime.fromisoformat(result['items'][0]['last_price_at']) == datetime.fromisoformat('2026-11-06T09:59:00+09:00')
    assert archive.read(TRADE_DATE) == before
    _set_clock(monkeypatch, '2026-11-06T15:31:00+09:00')
    assert client.get(RESULTS_PATH).json()['hypothetical_return']['return_pct'] == 1.0
    _set_clock(monkeypatch, '2026-11-09T10:00:00+09:00')
    def later_session_quote(symbol):
        raise AssertionError('Historical valuation must not use the later session display quote')
    monkeypatch.setattr(monitoring_session_results.kis_realtime_price_service,
        'price_fields_for_symbol', later_session_quote)
    archived = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()}).json()
    assert archived['hypothetical_return']['return_pct'] == 1.0


def test_rest_sample_values_a_wait_without_a_plan_or_recorded_trade(client, tracked_symbol, monkeypatch):
    _set_clock(monkeypatch, '2026-11-06T10:00:00+09:00')
    with session_scope() as session:
        snapshot = _store_decision(session, 'signal-decider', action='wait', trigger=10000,
            decided_at=datetime.fromisoformat('2026-11-05T21:00:00+09:00'))
        watch = WaitPriceWatch(snapshot.id, 'signal-decider', SYMBOL, PREVIOUS_DATE.isoformat(),
            '2026-11-05T21:00:00+09:00', '10000')
    wait_price_observations.wait_price_observations.configure(
        TRADE_DATE, [watch], now=datetime.fromisoformat('2026-11-06T09:02:00+09:00'),
    )
    monkeypatch.setattr(monitoring_session_results.kis_realtime_price_service, 'price_fields_for_symbol',
        lambda symbol: {'current_price': 11000, 'price_as_of': '2026-11-06T10:00:00+09:00',
            'price_status': 'available', 'price_source': 'kis_rest_multi_quote', 'display_price_available': True})
    result = client.get(RESULTS_PATH).json()
    assert result['hypothetical_return'] == {
        'return_pct': 10.0, 'included_symbol_count': 1, 'monitored_symbol_count': 1,
    }
    assert result['items'][0]['price_status'] == 'unavailable'
    assert result['items'][0]['last_price_at'] is None
    assert monitoring_session_history.monitoring_session_history.read(TRADE_DATE) == {}


@pytest.mark.parametrize(('newer_source', 'rejected_at', 'expected'), [
    (None, None, 10.0),
    (PREVIOUS_DATE, '2026-11-06T15:29:00+09:00', None),
    (PREVIOUS_DATE, '2026-11-06T15:31:00+09:00', 10.0),
    (TRADE_DATE, '2026-11-06T15:29:00+09:00', 10.0),
    (date(2026, 11, 9), '2026-11-06T15:29:00+09:00', 10.0),
])
def test_carried_wait_survives_until_a_newer_prior_session_decision_replaces_it(
    client, tracked_symbol, monkeypatch, newer_source, rejected_at, expected,
):
    _set_clock(monkeypatch, '2026-11-06T15:31:00+09:00')
    source_date = date(2026, 11, 4)
    with session_scope() as session:
        snapshot = _store_decision(session, 'signal-decider', action='wait', trigger=10000,
            decided_at=datetime.fromisoformat('2026-11-04T21:00:00+09:00'), market_date=source_date)
        watch = WaitPriceWatch(snapshot.id, 'signal-decider', SYMBOL, source_date.isoformat(),
            '2026-11-04T21:00:00+09:00', '10000')
        if newer_source is not None:
            _store_decision(session, 'signal-decider', action='reject', trigger=10000,
                decided_at=datetime.fromisoformat(rejected_at), market_date=newer_source)
    try:
        wait_price_observations.wait_price_observations.configure(
            TRADE_DATE, [watch], now=datetime.fromisoformat('2026-11-06T09:02:00+09:00'),
        )
        _store_bar(tracked_symbol, TRADE_DATE, 11000)
        result = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()}).json()
        assert result['hypothetical_return']['return_pct'] == expected
        assert result['hypothetical_return']['included_symbol_count'] == (0 if expected is None else 1)
    finally:
        with session_scope() as session:
            session.execute(delete(SignalDeciderDecisionSnapshot).where(
                SignalDeciderDecisionSnapshot.symbol == SYMBOL,
                SignalDeciderDecisionSnapshot.market_date.in_([source_date, TRADE_DATE, date(2026, 11, 9)]),
            ))
