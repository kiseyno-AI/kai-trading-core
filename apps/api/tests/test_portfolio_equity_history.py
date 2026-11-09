from contextlib import closing
from datetime import datetime, timezone
import sqlite3

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.api.routes_portfolio import router
from app.db import engine, session_scope
from app.db_models import PortfolioAssetSnapshot
from app.orders.portfolio_equity_history import capture_daily_equity
from scripts.import_observatory_equity import import_history


def test_daily_equity_route_keeps_legacy_baseline_and_requires_complete_post_close_data(tmp_path):
    PortfolioAssetSnapshot.__table__.create(engine, checkfirst=True)
    with session_scope() as session:
        session.execute(delete(PortfolioAssetSnapshot).where(
            PortfolioAssetSnapshot.source == 'portfolio_daily_equity_v1'
        ))

    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    assert client.get('/portfolio/equity-history').json() == {'equity': []}

    def capture(day, hour, first, second, **freshness):
        close = datetime(2026, 11, day, 6, 30, tzinfo=timezone.utc)
        observed = close.replace(hour=hour)
        snapshot = {
            'asOf': observed.isoformat(),
            'freshness': {'isStale': False, **freshness},
            'sync': {'status': 'synced'},
            'accountCoverage': {'status': 'complete'},
            'accounts': [
                {'id': 'private-a', 'summary': {'totalEquity': {'amount': first, 'currency': 'KRW'}}},
                {'id': 'private-b', 'summary': {'totalEquity': {'amount': second, 'currency': 'KRW'}}},
            ],
        }
        return capture_daily_equity(snapshot, close, now=observed.replace(hour=8))

    assert not capture(10, 5, 100, 100)
    assert not capture(10, 7, 100, 100, isStale=True)
    assert not capture(10, 7, 100, None)
    legacy_path = tmp_path / 'legacy.sqlite'
    with closing(sqlite3.connect(legacy_path)) as legacy:
        legacy.execute('CREATE TABLE valuations (date TEXT, currency TEXT, equity REAL)')
        legacy.execute("INSERT INTO valuations VALUES ('2026-11-10', 'KRW', 200)")
        legacy.commit()
    assert import_history(legacy_path) == 1
    assert import_history(legacy_path) == 1
    assert capture(10, 7, 200, 200)
    assert capture(11, 7, 150, 100)
    response = client.get('/portfolio/equity-history')
    assert response.status_code == 200
    assert response.headers['cache-control'] == 'no-store'
    assert response.json() == {'equity': [{'currency': 'KRW', 'points': [
        {'date': '2026-11-10', 'equity': 200, 'returnPct': 0},
        {'date': '2026-11-11', 'equity': 250, 'returnPct': 25},
    ]}]}
