from datetime import date, datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.agent_context.decider_watch_audit import build_decider_watch_audit
from app.db_models import (
    AgentContextFeedbackFailure,
    Base,
    DailyPriceBar,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
    Symbol,
)


def test_audit_separates_observed_watches_from_failed_decisions_and_paper_profit():
    engine = create_engine('sqlite://')
    Base.metadata.create_all(engine, tables=[
        model.__table__ for model in (
            Symbol, DailyPriceBar, SignalDeciderDecisionSnapshot,
            SignalDeciderExecutionPlanItem, AgentContextFeedbackFailure,
        )
    ])
    source_date = date(2026, 11, 9)
    target_date = date(2026, 11, 10)
    latest_date = date(2026, 11, 11)
    preopen = datetime(2026, 11, 9, 23, tzinfo=timezone.utc)
    after_open = datetime(2026, 11, 10, 1, tzinfo=timezone.utc)
    with Session(engine) as db:
        for symbol in ('005930', '000660', '035760', '033560'):
            master = Symbol(symbol=symbol, name_ko=symbol, market='KOSPI')
            db.add(master)
            db.flush()
            db.add_all([
                DailyPriceBar(
                    symbol_id=master.id, trade_date=trade_date,
                    open_price=100, high_price=125, low_price=95,
                    close_price=110, volume=1000, turnover=100000,
                )
                for trade_date in (target_date, latest_date)
            ])
        for symbol, observed_at, stop in (
            ('005930', preopen, 90),
            ('035760', after_open, 90),
            ('033560', preopen, 97),
        ):
            db.add(SignalDeciderDecisionSnapshot(
                market_date=source_date, symbol=symbol, agent_name='signal-decider',
                entry_price_basis='trigger_limit', target_price=120, stop_loss_price=stop,
                thesis_summary='test', supporting_evidence_payload=[], decision_risks_payload=[],
                invalidation_conditions_payload=[], data_quality_warnings_payload=[],
                specialist_disagreement_payload=[], traceability_payload={},
                input_feature_snapshot_payload={}, request_payload={'symbol_day_context': {'market': 'KOSPI'}},
                response_payload={'trigger_price': 105, 'chase_limit_price': 108},
                response_summary_payload={}, created_at=observed_at, feedback_received_at=observed_at,
            ))
        db.flush()
        snapshot = db.query(SignalDeciderDecisionSnapshot).filter_by(symbol='005930').one()
        db.add(SignalDeciderExecutionPlanItem(
            source_market_date=source_date, target_trade_date=target_date,
            decision_snapshot_id=snapshot.id, symbol='005930', policy_version='v1',
            account_reference='private-account', allocation_budget_krw=1000,
            eligibility_status='monitor_candidate', traceability_payload={},
            submission_payload={'exec_dispatch': {'readback': {
                'last_synced_at': after_open.isoformat(),
                'derived_state': {'plan_item_status_counts': {'watch_monitoring_active': 1}},
            }}},
        ))
        db.add(AgentContextFeedbackFailure(
            market_date=source_date, symbol='000660', agent_name='short-term-decider',
            run_id='child-run-643', failure_kind='dependency_missing_upstream',
            summary_payload={'error': 'missing_decider_upstream_summaries:eod-summarizer'},
            received_at=after_open,
        ))
        db.add(Symbol(symbol='AAPL', name_ko='Apple', market='NASDAQ', currency_code='USD'))
        db.add(AgentContextFeedbackFailure(
            market_date=source_date, symbol='AAPL', agent_name='short-term-decider',
            failure_kind='dependency_missing_upstream', summary_payload={},
            received_at=after_open,
        ))
        db.flush()
        report = build_decider_watch_audit(
            db, as_of_market_date=latest_date,
            observed_at=datetime(2026, 11, 11, 14, tzinfo=timezone.utc),
            next_session=lambda day: day + timedelta(days=1),
        )
    items = {item['symbol']: item for item in report['items']}
    assert 'AAPL' not in items
    watched = items['005930']
    assert watched['plans'][0]['exec_monitoring_observed_at'] == after_open.isoformat()
    assert 'private-account' not in str(report)
    assert watched['simulation']['gross_pnl_krw_per_share'] == 15
    assert watched['simulation']['exit_reason'] == 'target'
    assert items['033560']['simulation']['exit_reason'] == 'stop_first_same_bar'
    assert items['033560']['simulation']['gross_pnl_krw_per_share'] == -8
    late = items['035760']
    assert late['simulation']['trade_date'] == latest_date.isoformat()
    assert late['simulation']['original_session_unobservable'] is True
    failed = items['000660']
    assert failed['failure_kind'] == 'dependency_missing_upstream'
    assert failed['simulation']['scenario'] == 'missing_decision_open_to_close'
    assert failed['simulation']['gross_pnl_krw_per_share'] == 10
    assert failed['plans'] == []
