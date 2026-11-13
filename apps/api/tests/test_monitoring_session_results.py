from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
import pandas as pd
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
    monkeypatch.setattr(monitoring_session_results, 'wait_paper_ledger', SimpleNamespace(dates=lambda: set(), history=lambda *args, **kwargs: []))
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
        'method': 'legacy_trigger_to_mark_gross', 'deprecated': True,
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


def _store_decision(session, agent: str, *, action: str, trigger: int, decided_at: datetime):
    snapshot = SignalDeciderDecisionSnapshot(
        market_date=PREVIOUS_DATE, symbol=SYMBOL, agent_name=agent, thesis_summary='fixture',
        created_at=decided_at, feedback_received_at=decided_at, next_day_open_action='wait',
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
    ('wait', 1, '08:15:00', 12.5),
    ('wait', 0, '08:15:00', 5.0),
    ('reject', 1, '08:15:00', 5.0),
    ('avoid_chase', 1, '08:15:00', 5.0),
    ('wait', 1, '15:31:00', 5.0),
])
def test_hypothetical_return_uses_only_same_session_wait_plans(
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
        'method': 'legacy_trigger_to_mark_gross', 'deprecated': True,
    }
    assert 'fixture-private-account' not in response.text


@pytest.mark.parametrize(('rejected_at', 'return_pct'), [
    ('2026-11-06T15:29:00+09:00', 5.0),
    ('2026-11-06T15:31:00+09:00', 12.5),
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
            session, 'short-term-decider', action='reject', trigger=8_000,
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


def test_session_boundaries_follow_the_selected_exchange_calendar_not_clock_defaults(
    client, tracked_symbol, monkeypatch,
):
    opened_at = datetime.fromisoformat('2026-11-06T10:00:00+09:00')
    closed_at = datetime.fromisoformat('2026-11-06T16:30:00+09:00')
    class Calendar:
        def session_open(self, value):
            assert value == TRADE_DATE.isoformat()
            return pd.Timestamp(opened_at)
        def session_close(self, value):
            assert value == TRADE_DATE.isoformat()
            return pd.Timestamp(closed_at)
    monkeypatch.setattr(monitoring_session_results, 'KRX_EXCHANGE_CALENDAR', Calendar())
    _set_clock(monkeypatch, '2026-11-07T12:00:00+09:00')
    monitoring_session_history.monitoring_session_history.record(TRADE_DATE, {SYMBOL: None})
    result = client.get(RESULTS_PATH).json()
    assert result['market_date'] == TRADE_DATE.isoformat()
    assert datetime.fromisoformat(result['session_open_at']) == opened_at
    assert datetime.fromisoformat(result['session_close_at']) == closed_at


def test_unrecorded_session_does_not_invent_session_boundaries(client, monkeypatch):
    _set_clock(monkeypatch, '2026-11-07T12:00:00+09:00')
    result = client.get(RESULTS_PATH, params={'market_date': '2026-11-06'}).json()
    assert result['items'] == []
    assert result['session_open_at'] is None
    assert result['session_close_at'] is None


def test_paper_ledger_summary_keeps_net_costs_roles_and_unknowns_separate(client, monkeypatch, tmp_path):
    from types import SimpleNamespace

    _set_clock(monkeypatch, '2026-11-06T15:31:00+09:00')
    rows = [
        {'symbol': SYMBOL, 'decision_agent_name': 'signal-decider', 'status': 'target_exited',
         'coverage': 'complete', 'quantity': '2', 'filled_quantity': '2',
         'realized_gross_pnl_krw': '1000', 'realized_net_pnl_krw': '959', 'cost_krw': '41'},
        {'symbol': SYMBOL, 'decision_agent_name': 'short-term-decider', 'status': 'open',
         'coverage': 'complete', 'quantity': '3', 'filled_quantity': '3',
         'unrealized_gross_pnl_krw': '1500', 'unrealized_net_pnl_krw': '1438.5', 'cost_krw': '61.5'},
        {'symbol': '987653', 'decision_agent_name': 'short-term-decider', 'status': 'unavailable',
         'coverage': 'complete', 'missing_inputs': ['quantity'], 'quantity': None, 'filled_quantity': None,
         'realized_gross_pnl_krw': None, 'realized_net_pnl_krw': None},
        {'symbol': '987652', 'decision_agent_name': 'short-term-decider', 'status': 'open',
         'coverage': 'incomplete', 'active_tracking': False, 'filled_quantity': '5',
         'unrealized_gross_pnl_krw': '2000', 'unrealized_net_pnl_krw': '1900', 'cost_krw': '100'},
    ]
    cutoffs = []
    def history(day, *, as_of):
        assert day == TRADE_DATE
        cutoffs.append(as_of)
        return rows
    monkeypatch.setattr(monitoring_session_results, 'wait_paper_ledger', SimpleNamespace(root=tmp_path, history=history), raising=False)
    # The ledger can exist even when no monitoring archive or legacy WAIT record survived.
    response = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()})

    assert response.status_code == 200
    result = response.json()
    assert result['paper_trades'] == rows
    assert cutoffs == [datetime.fromisoformat('2026-11-06T15:30:00+09:00')]
    overall = result['paper_summary']['overall']
    assert overall['realized_gross_pnl_krw'] == '1000'
    assert overall['realized_net_pnl_krw'] == '959'
    assert overall['realized_cost_krw'] == '41'
    assert overall['closed_quantity'] == '2'
    assert overall['open_unrealized_gross_pnl_krw'] == '1500'
    assert overall['open_unrealized_net_pnl_krw'] == '1438.5'
    assert overall['incomplete_count'] == 2
    assert overall['open_trade_count'] == 1
    assert overall['unresolved_open_trade_count'] == 1
    assert overall['open_quantity'] == '3'
    assert overall['coverage'] == 'incomplete'
    signal = result['paper_summary']['by_role']['signal-decider']
    short = result['paper_summary']['by_role']['short-term-decider']
    assert signal['realized_net_pnl_krw'] == '959'
    assert short['realized_net_pnl_krw'] is None
    assert short['closed_trade_count'] == 0
    assert result['hypothetical_return']['deprecated'] is True
    assert result['hypothetical_return']['method'] == 'legacy_trigger_to_mark_gross'
    monkeypatch.setattr(wait_price_observations.wait_price_observations, 'paper_recording_error',
                        'OSError: /private/ledger/write failed', raising=False)
    degraded = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()}).json()
    assert degraded['paper_recording_error'] == 'OSError'
    assert degraded['paper_summary']['by_role']['signal-decider']['coverage'] == 'incomplete'
    assert '/private/ledger' not in str(degraded)


def test_session_reader_exposes_carried_exit_only_on_its_realization_day(client, monkeypatch, tmp_path):
    from app.agent_context.wait_paper_ledger import WaitPaperLedger

    ledger = WaitPaperLedger(tmp_path / 'paper')
    entered = datetime.fromisoformat('2026-11-06T09:00:01+09:00')
    exited = datetime.fromisoformat('2026-11-09T09:01:00+09:00')
    watch = SimpleNamespace(
        decision_snapshot_id=101, decision_agent_name='signal-decider', symbol=SYMBOL,
        source_market_date=PREVIOUS_DATE.isoformat(), observed_at='2026-11-06T08:00:00+09:00',
        trigger_price='100', target_price='110', stop_loss_price='90', quantity='2',
        quantity_source='core_plan', round_trip_cost_bps='50', buy_direction='at_or_below',
        chase_limit_price='103',
    )
    missing_quantity = SimpleNamespace(**{**vars(watch), 'decision_snapshot_id': 102, 'quantity': None})
    ledger.configure(TRADE_DATE, [watch, missing_quantity], now=entered)
    ledger.observe_trade(SYMBOL, Decimal('100'), price_as_of=entered, received_at=entered)
    ledger.observe_trade(SYMBOL, Decimal('110'), price_as_of=exited, received_at=exited)
    monkeypatch.setattr(monitoring_session_results, 'wait_paper_ledger', ledger)
    _set_clock(monkeypatch, '2026-11-09T16:00:00+09:00')

    cohort = client.get(RESULTS_PATH, params={'market_date': TRADE_DATE.isoformat()}).json()
    assert cohort['paper_trades'][0]['status'] == 'open'
    assert cohort['paper_summary']['overall']['realized_net_pnl_krw'] is None
    assert cohort['paper_summary']['overall']['incomplete_count'] == 1
    realized = client.get(RESULTS_PATH, params={'market_date': '2026-11-09'}).json()
    assert realized['paper_trades'][0]['market_date'] == TRADE_DATE.isoformat()
    assert realized['paper_trades'][0]['status'] == 'target_exited'
    assert Decimal(realized['paper_summary']['overall']['realized_net_pnl_krw']) == Decimal('18.95')
    assert client.get(RESULTS_PATH).json()['market_date'] == '2026-11-09'
