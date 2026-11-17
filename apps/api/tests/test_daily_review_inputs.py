from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.agent_context.wait_price_observations import WaitPriceObservations, WaitPriceWatch
from app.db_models import (
    Base, DailyMarketRiskSnapshot, DailyPriceBar, DailyTechnicalSnapshot, DartDisclosureEvent, ExternalNewsItem,
    ExternalNewsItemClassification, ExternalNewsItemSymbol, MarketNewsItem, SignalDeciderActualExecutionOutcome,
    SignalDeciderDecisionSnapshot, Symbol,
)
from app.external_news import store as news_store
from app.market.clock import KST
from app.market_news import service as market_news_store
from app.side_agents import daily_review_inputs
from app.side_agents.schemas import MARKET_AGENT
from app.side_agents.store import SideAgentStore


@pytest.fixture
def review_db(monkeypatch):
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine, tables=[model.__table__ for model in (
        SignalDeciderActualExecutionOutcome, SignalDeciderDecisionSnapshot, DailyMarketRiskSnapshot, DailyPriceBar,
        DailyTechnicalSnapshot, DartDisclosureEvent, ExternalNewsItem, ExternalNewsItemClassification,
        ExternalNewsItemSymbol, MarketNewsItem, Symbol,
    )])

    @contextmanager
    def test_session_scope(**_):
        with Session(engine) as db:
            yield db

    monkeypatch.setattr(daily_review_inputs, 'session_scope', test_session_scope)
    monkeypatch.setattr(news_store, 'session_scope', test_session_scope)
    monkeypatch.setattr(market_news_store, 'session_scope', test_session_scope)
    monkeypatch.setattr(daily_review_inputs, 'load_current_market_snapshot_context', lambda: None)
    return engine


def test_review_counts_each_role_and_separates_touches_quotes_and_fills(tmp_path, monkeypatch, review_db):
    market_date = date(2026, 11, 10)
    opened = datetime(2026, 11, 10, tzinfo=timezone.utc)
    cutoff = opened + timedelta(hours=7)
    watches = WaitPriceObservations(tmp_path / 'watches')
    watches.configure(market_date, [
        WaitPriceWatch(1, 'signal-decider', '005930', '2026-11-09', opened.isoformat(), '100', 'at_or_below', '103'),
        WaitPriceWatch(2, 'short-term-decider', '005930', '2026-11-09', opened.isoformat(), '100', 'at_or_above', '103'),
    ], now=opened)
    for minute, ask in [(1, '104'), (2, '102')]:
        now = opened + timedelta(minutes=minute)
        watches.observe_ask('005930', Decimal(ask), 10, price_as_of=now, received_at=now, source='test')
    monkeypatch.setattr(daily_review_inputs, 'wait_price_observations', watches)

    store = SideAgentStore(tmp_path / 'results')
    review = daily_review_inputs.load_daily_review_evidence(market_date, observed_at=cutoff, store=store)

    signal = review['roles']['signal-decider']
    short_term = review['roles']['short-term-decider']
    assert signal['observation_count'] == short_term['observation_count'] == 1
    assert signal['watch_event_count'] == 0
    assert signal['source_status']['watch'] == 'unavailable'
    assert short_term['watch_event_count'] == 1
    assert short_term['source_status']['watch'] == 'available'
    assert signal['ask_touch_count'] == signal['ask_entry_count'] == 0
    assert short_term['trade_touch_count'] == 0
    assert short_term['ask_touch_count'] == short_term['ask_entry_count'] == 1
    assert short_term['examples'][0]['ask_touch']['ask_price'] == '104'
    assert short_term['examples'][0]['ask_entry']['ask_price'] == '102'
    assert short_term['examples'][0]['ask_entry']['ask_size'] == 10
    assert short_term['actual_entry_fill_count'] == 0
    assert review['coverage']['watch']['unique_symbols'] == 1
    assert review['coverage']['watch']['tracked_decisions'] == 2
    assert review['coverage']['watch']['event_bearing_decisions'] == 1
    assert review['coverage']['actual_execution']['status'] == 'unavailable'
    assert review['market']['status'] == 'unavailable'


def test_review_reports_when_watches_registered_for_each_role(tmp_path, monkeypatch, review_db):
    day = date(2026, 11, 10)
    first = datetime(2026, 11, 10, 8, 55, tzinfo=KST)
    late = datetime(2026, 11, 10, 13, 39, tzinfo=KST)
    cutoff = datetime(2026, 11, 10, 16, 30, tzinfo=KST)
    watches = WaitPriceObservations(tmp_path / 'watches')
    signal = WaitPriceWatch(1, 'signal-decider', '005930', '2026-11-09', first.isoformat(), '100')
    short_term = WaitPriceWatch(2, 'short-term-decider', '000660', '2026-11-09', first.isoformat(), '100')
    watches.configure(day, [signal], now=first)
    watches.configure(day, [signal, short_term], now=late)
    monkeypatch.setattr(daily_review_inputs, 'wait_price_observations', watches)

    review = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=SideAgentStore(tmp_path))

    assert review['coverage']['watch']['registration'] == {
        'session_open_at': '2026-11-10T09:00:00+09:00',
        'first_registered_at': '2026-11-10T08:55:00+09:00',
        'last_registered_at': '2026-11-10T13:39:00+09:00',
        'registered_after_open': 1,
    }
    assert review['roles']['signal-decider']['registration']['registered_after_open'] == 0
    assert review['roles']['short-term-decider']['registration']['registered_after_open'] == 1
    assert review['coverage']['watch']['registration_by_role']['short-term-decider']['registered_after_open'] == 1
    assert review['coverage']['watch']['event_bearing_decisions'] == 0


def test_review_reserves_contrast_examples_without_changing_watch_counts(tmp_path, monkeypatch, review_db):
    day = date(2026, 11, 10)
    opened = datetime(2026, 11, 10, 9, tzinfo=KST)
    cutoff = datetime(2026, 11, 10, 16, 30, tzinfo=KST)
    watches = WaitPriceObservations(tmp_path / 'watches')
    watches.configure(day, [
        WaitPriceWatch(i, 'signal-decider', '005930', '2026-11-09', opened.isoformat(),
                       '100' if i <= 5 else '90', 'at_or_below')
        for i in range(1, 9)
    ], now=opened)
    watches.observe_ask('005930', Decimal('100'), 10, price_as_of=opened, received_at=opened, source='test')
    monkeypatch.setattr(daily_review_inputs, 'wait_price_observations', watches)

    review = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=SideAgentStore(tmp_path))

    role = review['roles']['signal-decider']
    assert role['observation_count'] == 8
    assert role['watch_event_count'] == 5
    assert len(role['examples']) == 6
    assert sum(not row.get('ask_touch') for row in role['examples']) == 2


def test_review_joins_the_exact_decision_and_excludes_facts_after_the_cutoff(tmp_path, monkeypatch, review_db):
    market_date = date(2026, 11, 10)
    prior_day = date(2026, 11, 9)
    opened = datetime(2026, 11, 10, tzinfo=timezone.utc)
    cutoff = opened + timedelta(hours=7)
    watches = WaitPriceObservations(tmp_path / 'watches')
    watches.configure(prior_day, [
        WaitPriceWatch(9, 'short-term-decider', '005930', '2026-11-06', (opened - timedelta(days=1)).isoformat(), '102'),
    ], now=opened - timedelta(days=1))
    watches.configure(market_date, [
        WaitPriceWatch(1, 'signal-decider', '005930', prior_day.isoformat(), opened.isoformat(), '100'),
        WaitPriceWatch(2, 'short-term-decider', '005930', prior_day.isoformat(), opened.isoformat(), '100'),
        WaitPriceWatch(3, 'short-term-decider', '000660', prior_day.isoformat(), opened.isoformat(), '100'),
    ], now=opened)
    monkeypatch.setattr(daily_review_inputs, 'wait_price_observations', watches)
    with Session(review_db) as db:
        snapshot_data = dict(
            market_date=prior_day, symbol='005930', agent_name='signal-decider', created_at=opened,
            thesis_summary='거래량이 뒷받침된 추세', suggested_holding_period='3일', target_price=110, stop_loss_price=90,
            supporting_evidence_payload=[], decision_risks_payload=[], invalidation_conditions_payload=['거래량 감소'],
            data_quality_warnings_payload=[], specialist_disagreement_payload=[],
            traceability_payload={}, input_feature_snapshot_payload={}, response_payload={}, response_summary_payload={},
            request_payload={
                'factor_summary': {'factor_scores': {'quality': {'score': 7, 'status': 'available'}}},
                'market_regime_context': {'posture': 'caution'},
                'symbol_day_context': {
                    'company': {'name_ko': '회사', 'market': 'KOSPI'},
                    'sector_peer_context': {'sector_name': '반도체'},
                    'fundamentals': {
                        'history_summary': {'latest_receipt_date': '2026-11-06'},
                        'latest_snapshot': {'highlights': {
                            'revenue': {'current_amount': '1500', 'currency': 'KRW', 'prior_amount': '1000'},
                        }},
                        'freshness_provenance': {'freshness_status': 'fresh', 'as_of_market_date': '2026-11-09'},
                    },
                    'recent_external_news': {'status': 'available', 'items': [{'title': '저장된 뉴스'}]},
                },
            },
        )
        db.add_all([SignalDeciderDecisionSnapshot(id=i, **snapshot_data) for i in (1, 2)])
        db.add(SignalDeciderDecisionSnapshot(**{
            **snapshot_data, 'id': 3, 'agent_name': 'short-term-decider', 'symbol': '000660',
        }))
        db.add(SignalDeciderActualExecutionOutcome(
            id=5, decision_snapshot_id=3, entry_plan_item_id=5, market_date=prior_day, source_market_date=prior_day,
            target_trade_date=market_date, account_reference='private-account', symbol='000660', policy_version='v1',
            lifecycle_status='planned', created_at=opened, updated_at=opened, computed_at=opened,
        ))
        for i, snapshot_id, symbol, filled_at in [
            (1, 1, '005930', cutoff - timedelta(minutes=15)),
            (2, 1, '005930', cutoff + timedelta(minutes=15)),
            (3, 1, '000660', cutoff - timedelta(minutes=15)),
            (4, 2, '005930', cutoff - timedelta(minutes=15)),
        ]:
            db.add(SignalDeciderActualExecutionOutcome(
                id=i, decision_snapshot_id=snapshot_id, entry_plan_item_id=i, market_date=prior_day, source_market_date=prior_day,
                target_trade_date=market_date, account_reference='private-account', symbol=symbol, policy_version='v1',
                lifecycle_status='closed', entry_filled_at=opened + timedelta(minutes=1), exit_filled_at=filled_at,
                entry_filled_quantity=2, exit_filled_quantity=2, closed_quantity=2, open_quantity=0,
                entry_average_fill_price=100, exit_average_fill_price=110, entry_notional_krw=200,
                exit_notional_krw=220, realized_pnl_krw=20, realized_return_pct=Decimal('0.10'),
                created_at=opened, updated_at=filled_at, computed_at=filled_at,
            ))
            if i <= 2:
                db.add(DailyMarketRiskSnapshot(
                    symbol_id=i, market_date=market_date, captured_at=filled_at, created_at=filled_at, updated_at=filled_at,
                    snapshot_payload={'raw': {'rprs_mrkt_kor_name': 'KOSPI', 'prdy_ctrt': '1.0' if i == 1 else '-90'}},
                ))
        db.commit()
    store = SideAgentStore(tmp_path / 'results')
    store.write_result(MARKET_AGENT, market_date.isoformat(), {
        'completed_at': (cutoff + timedelta(minutes=1)).isoformat(),
        'output': {'market_date': market_date.isoformat(), 'regime': 'risk_on'},
    })
    review = daily_review_inputs.load_daily_review_evidence(market_date, observed_at=cutoff, store=store)

    signal = review['roles']['signal-decider']
    short_term = review['roles']['short-term-decider']
    assert signal['actual_entry_fill_count'] == signal['actual_closed_trade_count'] == 1
    assert short_term['actual_entry_fill_count'] == 0
    assert short_term['examples'][0]['decision_context'] is None
    assert review['coverage']['actual_execution']['records'] == 2
    assert signal['source_status']['actual_execution'] == 'available'
    assert short_term['source_status']['actual_execution'] == 'records_only'
    assert short_term['execution_lifecycle_counts'] == {'planned': 1}
    assert signal['source_status']['news'] == signal['source_status']['business'] == 'context_only'
    evidence = signal['examples'][0]
    assert evidence['recorded_execution'] == [{
        'lifecycle_status': 'closed', 'entry_fill_observed': True,
        'closed_trade_observed': True, 'gross_realized_return_pct': 10,
    }]
    assert evidence['decision_context']['thesis_summary'] == '거래량이 뒷받침된 추세'
    assert evidence['decision_context']['suggested_holding_period'] == '3일'
    assert evidence['decision_context']['target_price'] == 110
    assert evidence['decision_context']['stop_loss_price'] == 90
    assert evidence['decision_context']['invalidation_conditions'] == ['거래량 감소']
    assert evidence['decision_context']['factor_scores']['quality']['score'] == 7
    assert evidence['decision_context']['company']['sector'] == '반도체'
    assert evidence['decision_context']['company']['latest_receipt_date'] == '2026-11-06'
    assert evidence['decision_context']['company']['financial_highlights']['revenue'] == {
        'current_amount': '1500', 'currency': 'KRW', 'prior_amount': '1000',
    }
    assert evidence['decision_context']['company']['fundamentals_freshness']['freshness_status'] == 'fresh'
    assert evidence['decision_context']['news_at_decision']['items'][0]['title'] == '저장된 뉴스'
    assert evidence['decision_context']['market_regime']['posture'] == 'caution'
    assert review['market']['breadth']['markets']['KOSPI']['median_return_pct'] == 1
    assert review['market']['analyst_context'] is None
    assert review['recent_sessions'][0]['market_date'] == prior_day.isoformat()
    assert review['recent_sessions'][0]['roles']['short-term-decider']['observation_count'] == 1
    assert 'private-account' not in str(review)


def test_review_includes_session_news_and_uses_closing_price_only_as_a_mark(tmp_path, monkeypatch, review_db):
    day = date(2026, 11, 10)
    opened = datetime(2026, 11, 10, tzinfo=timezone.utc)
    cutoff = opened + timedelta(hours=10)
    watches = WaitPriceObservations(tmp_path / 'watches')
    watches.configure(day, [
        WaitPriceWatch(1, 'signal-decider', '005930', '2026-11-09', opened.isoformat(), '100', 'at_or_below', '101'),
    ], now=opened)
    watches.observe_ask('005930', Decimal('100'), 3, price_as_of=opened, received_at=opened, source='test')
    monkeypatch.setattr(daily_review_inputs, 'wait_price_observations', watches)
    with Session(review_db) as db:
        db.add(Symbol(id=1, symbol='005930', name_ko='테스트', market='KOSPI'))
        db.add(DailyPriceBar(
            symbol_id=1, trade_date=day, open_price=101, high_price=112, low_price=98,
            close_price=105, volume=1000, turnover=105000, captured_at=cutoff - timedelta(minutes=1),
        ))
        for i, associated_at in [(1, opened), (2, cutoff + timedelta(minutes=1))]:
            db.add(ExternalNewsItem(
                id=i, symbol_id=1, title=f'공시 {i}', provider='test', source='test',
                url=f'https://example.test/{i}', published_at=opened, collected_at=opened,
            ))
            db.add(ExternalNewsItemSymbol(external_news_item_id=i, symbol_id=1, associated_at=associated_at))
        db.commit()
    review = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=SideAgentStore(tmp_path / 'results'))

    signal = review['roles']['signal-decider']
    evidence = signal['examples'][0]
    assert evidence['session_price']['confirmed_ask_mark_to_close_pct'] == 5
    assert signal['actual_entry_fill_count'] == 0
    assert [item['title'] for item in evidence['session_news']] == ['공시 1']
    assert signal['source_status']['news'] == 'available'
    assert review['roles']['short-term-decider']['source_status']['news'] == 'unavailable'


def test_review_includes_current_filings_and_recent_technicals_known_by_cutoff(tmp_path, monkeypatch, review_db):
    day = date(2026, 11, 10)
    opened = datetime(2026, 11, 10, tzinfo=timezone.utc)
    cutoff = opened + timedelta(hours=10)
    watches = WaitPriceObservations(tmp_path / 'watches')
    watches.configure(day, [
        WaitPriceWatch(1, 'signal-decider', '005930', '2026-11-09', opened.isoformat(), '100'),
    ], now=opened)
    monkeypatch.setattr(daily_review_inputs, 'wait_price_observations', watches)
    with Session(review_db) as db:
        db.add(Symbol(id=1, symbol='005930', name_ko='테스트', market='KOSPI'))
        for snapshot_day, revised_at, atr in [
            (date(2026, 11, 10), cutoff - timedelta(minutes=1), 2.3),
            (date(2026, 11, 9), cutoff + timedelta(minutes=1), 9.5),
            (date(2026, 11, 6), cutoff - timedelta(days=2), 1.8),
        ]:
            db.add(DailyTechnicalSnapshot(
                symbol_id=1, market_date=snapshot_day, close_price=105, volume=1000, signal_payload={},
                created_at=opened - timedelta(days=4), captured_at=opened - timedelta(days=4),
                updated_at=revised_at, atr_pct_14=atr,
            ))
        for i, filing_day, first_seen_at in [
            (1, day, cutoff - timedelta(minutes=1)),
            (2, day, cutoff + timedelta(minutes=1)),
            (3, day - timedelta(days=1), cutoff - timedelta(minutes=1)),
        ]:
            db.add(DartDisclosureEvent(
                rcept_no=str(i), stock_code='005930', corp_code='123', corp_name='회사', report_nm=f'공시 {i}',
                receipt_date=filing_day, created_at=first_seen_at, updated_at=first_seen_at, payload={},
            ))
        db.commit()
    review = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=SideAgentStore(tmp_path / 'results'))

    signal = review['roles']['signal-decider']
    evidence = signal['examples'][0]
    assert [(row['market_date'], row['atr_pct_14']) for row in evidence['technical_snapshots']] == [
        ('2026-11-10', 2.3), ('2026-11-06', 1.8),
    ]
    assert [(row['receipt_date'], row['report_name']) for row in evidence['session_filings']] == [('2026-11-10', '공시 1')]
    assert signal['source_status']['business'] == signal['source_status']['market'] == 'available'
    assert signal['source_status']['market_volatility'] == 'available'

    earlier = daily_review_inputs.load_daily_review_evidence(
        day, observed_at=cutoff - timedelta(minutes=2), store=SideAgentStore(tmp_path / 'results'),
    )
    assert earlier['roles']['signal-decider']['source_status']['market'] == 'context_only'
    assert earlier['roles']['signal-decider']['source_status']['market_volatility'] == 'unavailable'


def test_review_keeps_cached_market_observations_separate_by_trust_and_cutoff(tmp_path, monkeypatch, review_db):
    day = date(2026, 11, 10)
    cutoff = datetime(2026, 11, 10, 16, 30, tzinfo=KST)
    index_at = '2026-11-10T15:32:00+09:00'
    cached_at = '2026-11-10T15:39:00+09:00'
    row = {
        'value': 5239.13, 'change_percent': 4.39, 'high': 5300.92, 'low': 5174.55, 'advancers': 716,
        'foreign_net_value': 370825000000, 'investor_flow_value_unit': 'krw',
        'index_leg': {
            'status': 'unverified', 'provider_observed_at': index_at, 'collected_at': cached_at,
            'observation_payload_match': True, 'error': 'outside regular session',
        },
        'flow_leg': {
            'status': 'cached', 'current_session': True, 'provider_observed_at': '2026-11-10T15:25:00+09:00',
            'collected_at': '2026-11-10T15:25:22+09:00',
        },
    }
    snapshot = {
        'cached_at': cached_at, 'as_of': index_at, 'snapshot_status': 'partial',
        'source': 'kis_market_snapshot_v1', 'payload': {'markets': {'kospi': row}},
    }
    monkeypatch.setattr(daily_review_inputs, 'load_current_market_snapshot_context', lambda: snapshot)
    store = SideAgentStore(tmp_path)

    review = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=store)

    dashboard = review['market']['dashboard']
    assert dashboard['markets']['kospi']['index']['status'] == 'unverified'
    assert dashboard['markets']['kospi']['index']['value'] == 5239.13
    assert 'advancers' not in dashboard['markets']['kospi']['index']
    assert dashboard['markets']['kospi']['flow']['foreign_net_value'] == 370825000000
    assert review['roles']['signal-decider']['source_status']['market'] == 'available'
    assert review['roles']['signal-decider']['source_status']['market_volatility'] == 'unavailable'

    analyst_store = SideAgentStore(tmp_path / 'with-analyst')
    analyst_store.write_result(MARKET_AGENT, day.isoformat(), {
        'completed_at': (cutoff - timedelta(minutes=1)).isoformat(),
        'output': {'market_date': day.isoformat(), 'regime': 'neutral', 'volatility': 'unknown'},
    })
    analyst_review = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=analyst_store)
    assert analyst_review['roles']['signal-decider']['source_status']['market_volatility'] == 'unavailable'

    row['flow_leg']['provider_observed_at'] = '2026-11-09T15:25:00+09:00'
    stale_flow = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=store)
    assert 'flow' not in stale_flow['market']['dashboard']['markets']['kospi']
    assert stale_flow['roles']['signal-decider']['source_status']['market'] == 'context_only'

    earlier = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff - timedelta(hours=1), store=store)
    assert earlier['market']['dashboard'] is None
    assert earlier['roles']['signal-decider']['source_status']['market'] == 'unavailable'


def test_review_includes_only_market_headlines_known_before_the_cutoff(tmp_path, review_db):
    day = date(2026, 11, 10)
    cutoff = datetime(2026, 11, 10, 7, 30, tzinfo=timezone.utc)
    before = cutoff - timedelta(minutes=1)
    after = cutoff + timedelta(minutes=1)
    with Session(review_db) as db:
        for i, (topic, published_at, first_seen_at, created_at, updated_at) in enumerate([
            ('kr_kospi', before, before, before, before), ('kr_kosdaq', before, before, before, before),
            ('kr_kospi', before, after, after, after), ('kr_kospi', after, before, before, before),
            ('line_entertainment', before, before, before, before),
            ('kr_kospi', before, before, after, after), ('kr_kospi', before, before, before, after),
        ]):
            db.add(MarketNewsItem(
                topic_key=topic, region='KR', provider='test', url=f'https://example.test/{i}',
                url_hash=str(i), title=f'뉴스 {i}', published_at=published_at, first_seen_at=first_seen_at,
                created_at=created_at, updated_at=updated_at,
            ))
        db.commit()

    review = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=SideAgentStore(tmp_path))

    assert {item['title'] for item in review['market_headlines']} == {'뉴스 0', '뉴스 1'}
    assert review['coverage']['market_headlines']['items'] == 2
    assert review['roles']['signal-decider']['source_status']['news'] == 'unavailable'
    assert review['roles']['signal-decider']['source_status']['market'] == 'unavailable'


def test_review_consumes_paper_ledger_with_cutoff_without_claiming_actual_fills(tmp_path, monkeypatch, review_db):
    from types import SimpleNamespace

    day = date(2026, 11, 10)
    cutoff = datetime(2026, 11, 10, 16, 30, tzinfo=KST)
    monkeypatch.setattr(daily_review_inputs, 'wait_price_observations', WaitPriceObservations(tmp_path / 'watches'))
    rows = [
        {'decision_snapshot_id': 1, 'symbol': '005930', 'decision_agent_name': 'signal-decider',
         'status': 'stop_exited', 'coverage': 'complete', 'filled_quantity': '2',
         'realized_gross_pnl_krw': '-200', 'realized_net_pnl_krw': '-203.8', 'cost_krw': '3.8',
         'events': [{'kind': 'trade', 'price': '100'}] * 1000},
        {'decision_snapshot_id': 2, 'symbol': '005930', 'decision_agent_name': 'short-term-decider',
         'status': 'open', 'coverage': 'incomplete', 'filled_quantity': '1',
         'unrealized_gross_pnl_krw': None, 'unrealized_net_pnl_krw': None,
         'coverage_reasons': ['unknown_path_gap']},
    ]
    rows.extend({'decision_snapshot_id': key, 'symbol': '005930', 'decision_agent_name': 'signal-decider',
                 'status': 'pending', 'coverage': 'complete'} for key in range(3, 10))
    def history(market_date, *, as_of):
        assert market_date == day
        assert as_of == cutoff
        return rows
    monkeypatch.setattr(daily_review_inputs, 'wait_paper_ledger', SimpleNamespace(history=history), raising=False)

    review = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=SideAgentStore(tmp_path))

    signal = review['roles']['signal-decider']
    short = review['roles']['short-term-decider']
    assert signal['paper_trades'][0] == {key: value for key, value in rows[0].items() if key != 'events'}
    assert len(signal['paper_trades']) == 6
    assert signal['paper_summary']['trade_count'] == 8
    assert signal['paper_summary']['realized_net_pnl_krw'] == '-203.8'
    assert signal['actual_entry_fill_count'] == 0
    assert signal['source_status']['actual_execution'] == 'unavailable'
    assert signal['source_status']['paper_execution'] == 'available'
    assert short['source_status']['paper_execution'] == 'incomplete'
    assert short['paper_summary']['realized_net_pnl_krw'] is None
    assert short['paper_summary']['open_unrealized_net_pnl_krw'] is None
    assert review['coverage']['paper_execution']['incomplete_count'] == 1
    assert review['coverage']['actual_execution']['status'] == 'unavailable'

    monkeypatch.setattr(daily_review_inputs.wait_price_observations, 'paper_recording_error', 'OSError: /private/path', raising=False)
    degraded = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=SideAgentStore(tmp_path))
    assert degraded['coverage']['paper_execution']['recording_error'] == 'OSError'
    assert degraded['roles']['signal-decider']['source_status']['paper_execution'] == 'incomplete'


def test_daily_review_never_uses_later_published_no_exit_reconstruction(tmp_path, monkeypatch, review_db):
    from types import SimpleNamespace
    from app.agent_context.wait_paper_ledger import WaitPaperLedger

    day = date(2026, 11, 10)
    entered = datetime(2026, 11, 10, 9, tzinfo=KST)
    cutoff = datetime(2026, 11, 10, 16, 30, tzinfo=KST)
    close = cutoff.replace(hour=15, minute=30)
    published = cutoff.replace(hour=20, minute=30)
    ledger = WaitPaperLedger(tmp_path / 'paper')
    watch = SimpleNamespace(
        decision_snapshot_id=33922, decision_agent_name='signal-decider', symbol='005930',
        source_market_date='2026-11-09', observed_at=entered.isoformat(),
        trigger_price='14230', target_price='16000', stop_loss_price='13000', quantity='6',
        quantity_source='stored_plan', round_trip_cost_bps='50', buy_direction='at_or_below',
        chase_limit_price='14500',
    )
    ledger.configure(day, [watch], now=entered)
    ledger.observe_trade('005930', Decimal('14230'), price_as_of=entered, received_at=entered)
    ledger.mark_path_gap(day, 33922, now=entered.replace(minute=1))
    ledger.certify_no_exit(day, 33922, price=Decimal('14690'), price_as_of=close,
                           received_at=published, evidence={
                               'retrospective': True, 'economic_cutoff': close.isoformat(),
                               'published_at': published.isoformat(),
                           })
    monkeypatch.setattr(daily_review_inputs, 'wait_paper_ledger', ledger)
    monkeypatch.setattr(daily_review_inputs, 'wait_price_observations', WaitPriceObservations(tmp_path / 'watches'))

    review = daily_review_inputs.load_daily_review_evidence(day, observed_at=cutoff, store=SideAgentStore(tmp_path))

    signal = review['roles']['signal-decider']
    assert signal['source_status']['paper_execution'] == 'incomplete'
    assert signal['paper_summary']['open_trade_count'] == 0
    assert signal['paper_summary']['unresolved_open_trade_count'] == 1
    assert signal['paper_summary']['open_unrealized_net_pnl_krw'] is None
    assert signal['paper_trades'][0].get('no_exit_certificate') is None
    assert signal['actual_entry_fill_count'] == 0
