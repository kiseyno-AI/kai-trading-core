from contextlib import closing
from datetime import datetime, timezone
import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.api.routes_portfolio import router
from app.db import engine, session_scope
from app.db_models import PortfolioAssetSnapshot
from app.orders.portfolio_equity_history import capture_daily_equity
from scripts.import_observatory_equity import import_history


@pytest.fixture
def equity_client():
    PortfolioAssetSnapshot.__table__.create(engine, checkfirst=True)
    with session_scope() as session:
        session.execute(delete(PortfolioAssetSnapshot).where(
            PortfolioAssetSnapshot.source == 'portfolio_daily_equity_v1'
        ))

    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        yield client


def test_daily_equity_route_keeps_legacy_baseline_and_requires_complete_post_close_data(tmp_path, equity_client):
    client = equity_client
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



def test_legacy_import_leaves_all_dates_unchanged_on_conflict(tmp_path, equity_client):
    existing_path = tmp_path / 'existing.sqlite'
    incoming_path = tmp_path / 'incoming.sqlite'
    for path, rows in (
        (existing_path, [('2026-11-11', 'KRW', 250)]),
        (incoming_path, [('2026-11-10', 'KRW', 200), ('2026-11-11', 'KRW', 251)]),
    ):
        with closing(sqlite3.connect(path)) as legacy:
            legacy.execute('CREATE TABLE valuations (date TEXT, currency TEXT, equity REAL)')
            legacy.executemany('INSERT INTO valuations VALUES (?, ?, ?)', rows)
            legacy.commit()

    assert import_history(existing_path) == 1
    before = equity_client.get('/portfolio/equity-history').json()
    with pytest.raises(ValueError, match='Legacy valuations do not match'):
        import_history(incoming_path)

    assert equity_client.get('/portfolio/equity-history').json() == before

    with closing(sqlite3.connect(incoming_path)) as legacy:
        legacy.execute("UPDATE valuations SET equity = 250 WHERE date = '2026-11-11'")
        legacy.commit()
    assert import_history(incoming_path) == 2
    assert import_history(incoming_path) == 2
    assert equity_client.get('/portfolio/equity-history').json()['equity'][0]['points'] == [
        {'date': '2026-11-10', 'equity': 200, 'returnPct': 0},
        {'date': '2026-11-11', 'equity': 250, 'returnPct': 25},
    ]
