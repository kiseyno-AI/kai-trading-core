from fastapi.testclient import TestClient

from app.core.settings import settings
from app.main import app


def test_strategy_status_is_authenticated_and_read_only_before_first_tick(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, 'upstream_bearer_token', 'test-rebound-reader')
    monkeypatch.setattr(settings, 'us_rebound_state_dir', str(tmp_path), raising=False)
    monkeypatch.setattr(settings, 'us_rebound_mode', 'observe', raising=False)
    monkeypatch.setattr(settings, 'us_rebound_enabled_from', '2026-11-27')
    client = TestClient(app)
    path = '/strategies/us-rebound/status?target_trade_date=2026-11-27'

    assert client.get(path).status_code == 401
    response = client.get(path, headers={'Authorization': 'Bearer test-rebound-reader'})
    assert response.status_code == 200
    value = response.json()
    assert value['target_trade_date'] == '2026-11-27'
    assert value['mode'] == 'observe'
    assert value['enabled_from'] == '2026-11-27'
    assert value['order_execution_enabled'] is False
    assert value['status'] == 'not_started'
    assert value['evaluation'] is None
    assert list(tmp_path.iterdir()) == []


def test_corrupt_evidence_is_a_safe_status_error_without_resetting_it(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, 'upstream_bearer_token', None)
    monkeypatch.setattr(settings, 'us_rebound_state_dir', str(tmp_path))
    folder = tmp_path / 'rebound/real-2026-11-27'
    folder.mkdir(parents=True)
    path = folder / 'evaluation.json'
    path.write_text('{"private":"must-not-be-exposed"}')
    response = TestClient(app).get('/strategies/us-rebound/status?target_trade_date=2026-11-27')
    assert response.status_code == 503
    assert response.json() == {'detail': {'code': 'us_rebound_status_unavailable', 'error_type': 'ValueError'}}
    assert path.read_text() == '{"private":"must-not-be-exposed"}'
