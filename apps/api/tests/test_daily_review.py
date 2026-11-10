from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.side_agents import daily_review
from app.side_agents.store import SideAgentStore


def install_analyst_contract(tmp_path, monkeypatch):
    monkeypatch.setattr(daily_review.settings, 'openclaw_state_dir', str(tmp_path))
    monkeypatch.setattr(daily_review.settings, 'openclaw_dispatch_min_free_bytes', 0)
    workspace = tmp_path / 'workspace-postmortem-analyst'
    workspace.mkdir()
    (workspace / 'AGENTS.md').write_bytes(
        (daily_review.CONTRACTS / 'postmortem-analyst' / 'AGENTS.md').read_bytes()
    )


def watch_outcomes():
    return {
        'observation_count': 12, 'watch_event_count': 6, 'trade_touch_count': 4, 'ask_touch_count': 5,
        'ask_entry_count': 3, 'actual_entry_fill_count': 1, 'actual_closed_trade_count': 0,
        'source_status': {'watch': 'available'}, 'examples': [{'private': 'evidence'}],
    }


def test_scheduled_review_exposes_only_public_status_and_runs_once(tmp_path, monkeypatch):
    day = date(2026, 11, 10)
    now = datetime(2026, 11, 10, 22, tzinfo=ZoneInfo('Asia/Seoul'))
    store = SideAgentStore(tmp_path)
    monkeypatch.setattr(daily_review.settings, 'side_agents_output_dir', str(tmp_path))
    monkeypatch.setattr(daily_review.settings, 'upstream_bearer_token', 'test-daily-review-token')
    install_analyst_contract(tmp_path, monkeypatch)
    assert TestClient(app).get('/agent-context/daily-review').status_code == 401
    api = TestClient(app, headers={'Authorization': 'Bearer test-daily-review-token'})

    def collect(market_date, *, observed_at, store):
        assert market_date == day
        assert observed_at == now
        stats = watch_outcomes()
        trade_only = {**stats, 'watch_event_count': 1, 'trade_touch_count': 1, 'ask_touch_count': 0, 'ask_entry_count': 0}
        return {'roles': {'signal-decider': stats, 'short-term-decider': trade_only}}

    class Analyst:
        calls = 0

        def call(self, *, agent, task, payload, run_id):
            self.calls += 1
            assert (agent, task, run_id) == ('postmortem-analyst', 'daily_review', f'daily-review-{day}-1')
            run = api.get(f'/agent-context/daily-review?market_date={day}').json()['runs'][0]
            assert (run['status'], run['stage']) == ('running', 'postmortem')
            return '''{
                "schema_version": "daily_review_v1", "market_date": "2026-11-10",
                "summary_ko": "호가와 체결을 구분했습니다.",
                "roles": {
                    "signal-decider": {"assessment_ko": "호가 관찰", "findings": [{"source": "watch", "observation_ko": "호가 잔량 차이", "implication_ko": "진입 가정 점검"}], "focus": "entry_liquidity"},
                    "short-term-decider": {"assessment_ko": "가용성 확인", "findings": [{"source": "watch", "observation_ko": "호가 확인 필요", "implication_ko": "진입 가정 점검"}], "focus": "entry_liquidity"}
                }
            }'''

    analyst = Analyst()
    options = dict(store=store, client=analyst, evidence_loader=collect, is_trading_day=lambda _: True)
    assert daily_review.run_due_daily_review(now=now.replace(hour=15), **options) is None
    assert api.get('/agent-context/daily-review').json() == {'market': 'KRX', 'runs': []}
    assert daily_review.run_due_daily_review(now=now, **options).status == 'ok'
    assert daily_review.run_due_daily_review(now=now, **options).detail == 'already_done'
    assert analyst.calls == 1
    run = api.get(f'/agent-context/daily-review?market_date={day}').json()['runs'][0]
    assert run['status'] == 'completed'
    assert run['candidates']['signal-decider']['status'] == 'ready_for_review'
    assert run['candidates']['short-term-decider']['status'] == 'insufficient_evidence'
    assert 'contract_markdown' not in run['candidates']['signal-decider']
    assert 'evidence' not in run
    assert run['watch_outcomes']['signal-decider']['ask_entry_count'] == 3
    assert run['watch_outcomes']['short-term-decider']['watch_event_count'] == 1
    artifact = store.load_result('daily-review', day.isoformat())
    assert 'displayed offer depth' in artifact['candidate_contracts']['signal-decider']
    assert 'displayed offer depth' not in artifact['candidate_contracts']['short-term-decider']


def test_scheduled_review_rejects_an_outdated_live_analyst_contract(tmp_path, monkeypatch):
    install_analyst_contract(tmp_path, monkeypatch)
    contract = Path(daily_review.settings.openclaw_state_dir) / 'workspace-postmortem-analyst' / 'AGENTS.md'
    contract.write_text('old weekly-only contract')
    store = SideAgentStore(tmp_path)
    now = datetime(2026, 11, 10, 22, tzinfo=ZoneInfo('Asia/Seoul'))

    class NoDispatch:
        def call(self, **kwargs):
            raise AssertionError('an outdated analyst must not be dispatched')

    result = daily_review.run_due_daily_review(
        store=store, client=NoDispatch(), now=now, is_trading_day=lambda _: True,
        evidence_loader=lambda *args, **kwargs: {
            'roles': {role: {'observation_count': 12} for role in daily_review.DECIDER_ROLES},
        },
    )
    assert result.detail == 'postmortem_contract_mismatch'
    record = store.load_result('daily-review', '2026-11-10')
    assert record['status'] == 'retry_wait'
    assert 'candidate_contracts' not in record


def test_failed_attempt_near_window_end_is_terminal(tmp_path, monkeypatch):
    install_analyst_contract(tmp_path, monkeypatch)
    store = SideAgentStore(tmp_path)
    now = datetime(2026, 11, 11, 5, 50, tzinfo=ZoneInfo('Asia/Seoul'))

    def collect(*args, **kwargs):
        raise ValueError('evidence query failed')

    options = dict(store=store, evidence_loader=collect, is_trading_day=lambda _: True)
    assert daily_review.run_due_daily_review(now=now, **options).status == 'failed'
    assert store.load_result('daily-review', '2026-11-10')['status'] == 'failed'
    assert daily_review.run_due_daily_review(now=now + timedelta(minutes=10), **options) is None


def test_interrupted_last_attempt_does_not_stay_running(tmp_path, monkeypatch):
    install_analyst_contract(tmp_path, monkeypatch)
    monkeypatch.setattr(daily_review.settings, 'side_agents_output_dir', str(tmp_path))
    monkeypatch.setattr(daily_review.settings, 'side_agents_max_daily_attempts', 1)
    now = datetime(2026, 11, 10, 22, tzinfo=ZoneInfo('Asia/Seoul'))

    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        daily_review.run_due_daily_review(now=now, evidence_loader=interrupted, is_trading_day=lambda _: True)
    run = TestClient(app).get('/agent-context/daily-review?market_date=2026-11-10').json()['runs'][0]
    assert (run['status'], run['error']) == ('failed', 'interrupted')


def test_storage_admission_blocks_a_review_before_dispatch(tmp_path, monkeypatch):
    install_analyst_contract(tmp_path, monkeypatch)
    monkeypatch.setattr(daily_review.settings, 'openclaw_dispatch_min_free_bytes', 2**63)
    store = SideAgentStore(tmp_path)
    now = datetime(2026, 11, 10, 22, tzinfo=ZoneInfo('Asia/Seoul'))

    def collect(*args, **kwargs):
        raise AssertionError('blocked reviews must not collect evidence or dispatch')

    result = daily_review.run_due_daily_review(now=now, store=store, evidence_loader=collect, is_trading_day=lambda _: True)
    assert result.detail == 'openclaw_storage_admission_blocked'


def test_review_rechecks_storage_admission_after_evidence_collection(tmp_path, monkeypatch):
    install_analyst_contract(tmp_path, monkeypatch)
    store = SideAgentStore(tmp_path)
    now = datetime(2026, 11, 10, 22, tzinfo=ZoneInfo('Asia/Seoul'))

    def collect(*args, **kwargs):
        monkeypatch.setattr(daily_review.settings, 'openclaw_dispatch_min_free_bytes', 2**63)
        return {'roles': {role: watch_outcomes() for role in daily_review.DECIDER_ROLES}}

    class NoDispatch:
        def call(self, **kwargs):
            pytest.fail('storage is no longer admitted')

    result = daily_review.run_due_daily_review(
        now=now, store=store, client=NoDispatch(), evidence_loader=collect, is_trading_day=lambda _: True,
    )
    assert result.detail == 'openclaw_storage_admission_blocked'
