from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.agent_context.wait_price_observations import WaitPriceObservations, WaitPriceWatch
from app.db_models import (
    Base, DailyMarketRiskSnapshot, DailyPriceBar, ExternalNewsItem, ExternalNewsItemClassification,
    ExternalNewsItemSymbol, SignalDeciderActualExecutionOutcome, SignalDeciderDecisionSnapshot, Symbol,
)
from app.external_news import store as news_store
from app.side_agents import daily_review_inputs
from app.side_agents.schemas import MARKET_AGENT
from app.side_agents.store import SideAgentStore


@pytest.fixture
def review_db(monkeypatch):
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine, tables=[model.__table__ for model in (
        SignalDeciderActualExecutionOutcome, SignalDeciderDecisionSnapshot, DailyMarketRiskSnapshot, DailyPriceBar,
        ExternalNewsItem, ExternalNewsItemClassification, ExternalNewsItemSymbol, Symbol,
    )])

    @contextmanager
    def test_session_scope(**_):
        with Session(engine) as db:
            yield db

    monkeypatch.setattr(daily_review_inputs, 'session_scope', test_session_scope)
    monkeypatch.setattr(news_store, 'session_scope', test_session_scope)
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
    assert signal['ask_touch_count'] == signal['ask_entry_count'] == 0
    assert short_term['trade_touch_count'] == 0
    assert short_term['ask_touch_count'] == short_term['ask_entry_count'] == 1
    assert short_term['examples'][0]['ask_touch']['ask_price'] == '104'
    assert short_term['examples'][0]['ask_entry']['ask_price'] == '102'
    assert short_term['examples'][0]['ask_entry']['ask_size'] == 10
    assert short_term['actual_entry_fill_count'] == 0
    assert review['coverage']['watch']['unique_symbols'] == 1
    assert review['coverage']['actual_execution']['status'] == 'unavailable'
    assert review['market']['status'] == 'unavailable'


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
    ], now=opened)
    monkeypatch.setattr(daily_review_inputs, 'wait_price_observations', watches)
    with Session(review_db) as db:
        db.add(SignalDeciderDecisionSnapshot(
            id=1, market_date=prior_day, symbol='005930', agent_name='signal-decider', created_at=opened,
            thesis_summary='Test', supporting_evidence_payload=[], decision_risks_payload=[],
            invalidation_conditions_payload=[], data_quality_warnings_payload=[], specialist_disagreement_payload=[],
            traceability_payload={}, input_feature_snapshot_payload={}, response_payload={}, response_summary_payload={},
            request_payload={
                'factor_summary': {'factor_scores': {'quality': {'score': 7, 'status': 'available'}}},
                'market_regime_context': {'posture': 'caution'},
                'symbol_day_context': {
                    'company': {'name_ko': '회사', 'market': 'KOSPI'},
                    'sector_peer_context': {'sector_name': '반도체'},
                    'fundamentals': {'history_summary': {'latest_receipt_date': '2026-11-06'}},
                    'recent_external_news': {'status': 'available', 'items': [{'title': '저장된 뉴스'}]},
                },
            },
        ))
        for i, filled_at in [(1, cutoff - timedelta(minutes=15)), (2, cutoff + timedelta(minutes=15))]:
            db.add(SignalDeciderActualExecutionOutcome(
                id=i, decision_snapshot_id=1, entry_plan_item_id=i, market_date=prior_day, source_market_date=prior_day,
                target_trade_date=market_date, account_reference='private-account', symbol='005930', policy_version='v1',
                lifecycle_status='closed', entry_filled_at=opened + timedelta(minutes=1), exit_filled_at=filled_at,
                entry_filled_quantity=2, exit_filled_quantity=2, closed_quantity=2, open_quantity=0,
                entry_average_fill_price=100, exit_average_fill_price=110, entry_notional_krw=200,
                exit_notional_krw=220, realized_pnl_krw=20, realized_return_pct=10,
                created_at=opened, updated_at=filled_at, computed_at=filled_at,
            ))
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
    assert signal['actual_entry_fill_count'] == signal['actual_closed_trade_count'] == 1
    evidence = signal['examples'][0]
    assert evidence['recorded_execution'][0]['gross_realized_pnl_krw'] == 20
    assert evidence['decision_context']['factor_scores']['quality']['score'] == 7
    assert evidence['decision_context']['company']['sector'] == '반도체'
    assert evidence['decision_context']['company']['latest_receipt_date'] == '2026-11-06'
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
