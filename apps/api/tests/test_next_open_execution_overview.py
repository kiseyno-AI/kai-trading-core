from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.api import routes_trading
from app.db_models import SignalDeciderExecutionPlanItem
from app.orders.next_open_execution import SignalDeciderNextOpenExecutionService


def test_overview_returns_small_plan_without_full_evidence_or_broker_reads():
    engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
    SignalDeciderExecutionPlanItem.__table__.create(engine)
    with Session(engine) as session:
        for index in range(60):
            session.add(SignalDeciderExecutionPlanItem(
                decision_snapshot_id=1, source_market_date=date(2026, 11, 9),
                target_trade_date=date(2026, 11, 10), account_reference='private-account',
                symbol=f'{index:06d}', policy_version='test', allocation_budget_krw=1000,
                planned_quantity=2, estimated_reference_price=100,
                eligibility_status='eligible' if index < 20 else 'monitor_candidate' if index < 40 else 'blocked',
                execution_status='planned' if index < 20 else 'awaiting_trigger' if index < 40 else 'blocked',
                traceability_payload={
                    'raw_action': 'wait' if index else None, 'tactical_action': 'watch_for_trigger',
                    'trigger_price': 100, 'chase_limit_price': 105,
                    'target_price': 120, 'stop_loss_price': 90,
                    'response_summary': {'large_private_evidence': 'x' * 100_000},
                },
                validation_payload={'private_validation': 'y' * 50_000},
                submission_payload={'private_submission': 'z' * 10_000},
            ))
        session.commit()

    @contextmanager
    def session_scope():
        with Session(engine) as session:
            yield session

    statements = []
    event.listen(engine, 'before_cursor_execute', lambda connection, cursor, statement, *args: statements.append(statement))
    service = SignalDeciderNextOpenExecutionService()
    app = FastAPI()
    app.include_router(routes_trading.router)
    with (
        patch.object(routes_trading, 'signal_decider_next_open_execution_service', service),
        patch('app.orders.next_open_execution.session_scope', session_scope),
        patch.object(service, '_resolve_target_trade_date', return_value=date(2026, 11, 10)),
        patch('app.orders.next_open_execution._previous_trading_day', return_value=date(2026, 11, 9)),
        patch.object(service, 'list_plan_items', side_effect=AssertionError('full plan read')),
        patch.object(service, 'refresh_plan_items', side_effect=AssertionError('plan mutation')),
        patch('app.orders.next_open_execution.load_order_request', side_effect=AssertionError('broker lookup')),
    ):
        client = TestClient(app)
        response = client.get('/trading/orders/next-open-execution/overview?target_trade_date=2026-11-10')

    assert response.status_code == 200
    body = response.json()
    assert body['summary'] == {
        'target_trade_date': '2026-11-10', 'source_market_date': '2026-11-09',
        'items_total': 60, 'planned_count': 20, 'blocked_count': 20, 'awaiting_trigger_count': 20,
    }
    assert len(body['items']) == 60
    item = next(row for row in body['items'] if row['symbol'] == '000000')
    assert item['trigger_price'] == 100
    assert item['target_price'] == 120
    assert item['stop_loss_price'] == 90
    assert item['raw_action'] == 'watch_for_trigger'
    assert set(item) == {
        'symbol', 'decision_agent_name', 'raw_action',
        'trigger_price', 'chase_limit_price', 'target_price', 'stop_loss_price',
    }
    assert len(response.content) < 100_000
    assert 'private' not in response.text
    assert 'response_summary' not in response.text
    assert len(statements) == 1
    assert all('validation_payload' not in statement for statement in statements)
    assert all('submission_payload' not in statement for statement in statements)
    engine.dispose()


def test_overview_validates_queries_and_redacts_read_failures():
    app = FastAPI()
    app.include_router(routes_trading.router)
    client = TestClient(app)
    assert client.get('/trading/orders/next-open-execution/overview?target_trade_date=bad').status_code == 422
    with patch.object(
        routes_trading.signal_decider_next_open_execution_service,
        'list_plan_overview', side_effect=ValueError('invalid resolved date'),
    ):
        assert client.get('/trading/orders/next-open-execution/overview').status_code == 400
    with patch.object(
        routes_trading.signal_decider_next_open_execution_service,
        'list_plan_overview', side_effect=RuntimeError('private database detail'),
    ):
        response = client.get('/trading/orders/next-open-execution/overview')
    assert response.status_code == 500
    assert response.json() == {'detail': 'Next-open execution overview read failed'}
