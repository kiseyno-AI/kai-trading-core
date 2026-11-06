from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import pytest

from app.side_agents import runner
from app.side_agents.client import SideAgentClient, SideAgentDispatchError, build_message
from app.side_agents.payloads import build_news_payloads
from app.side_agents.schemas import (
    MARKET_SCHEMA,
    NEWS_SCHEMA,
    POSTMORTEM_SCHEMA,
    SideAgentValidationError,
    parse_json_object,
    validate_market_context,
    validate_news_events,
    validate_postmortem,
)
from app.side_agents.store import SideAgentStore

DAY = date(2026, 11, 9)  # Monday
REPO_ROOT = Path(__file__).resolve().parents[3]


def kst(hour: int, minute: int = 0, day: int = 9) -> datetime:
    return datetime(2026, 11, day, hour - 9, minute, tzinfo=timezone.utc)  # KST = UTC+9


def market_output(**over):
    base = {
        'schema_version': MARKET_SCHEMA, 'market_date': DAY.isoformat(), 'regime': 'neutral',
        'trend': {'kospi': 'range', 'kosdaq': 'down'}, 'volatility': 'normal', 'rotation': [],
        'drivers': [{'factor': 'breadth', 'direction': 'adverse', 'evidence': '하락 종목 우세'}],
        'event_risks': [{'event': 'FOMC', 'date': '2026-11-12', 'importance': 'critical'}],
        'posture_hint': 'selective', 'confidence': 0.55, 'data_gaps': [], 'pre_mortem': 'x', 'summary_ko': '요약',
    }
    base.update(over)
    return base


def news_output(symbols, **over):
    item = {
        'materiality': 'noise', 'event_types': ['other'], 'direction': 'neutral', 'evidence_grade': 'headline_only',
        'reaction_check': 'not_assessable', 'horizon': 'unknown', 'top_source_tier': 'aggregator',
        'confidence': 0.4, 'risk_flags': ['headline_only_news: 제목만 확인'], 'summary_ko': '요약',
    }
    item.update(over)
    return {'schema_version': NEWS_SCHEMA, 'market_date': DAY.isoformat(), 'results': [{'symbol': s, **item} for s in symbols]}


def postmortem_output(**over):
    base = {
        'schema_version': POSTMORTEM_SCHEMA, 'period': {'from': '2026-11-02', 'to': '2026-11-06'},
        'sample': {'total': 120, 'by_agent': {'signal-decider': 120}, 'sufficient': True},
        'classification': {'true_positive': 50, 'false_positive': 40, 'missed_opportunity': 10, 'regime_mismatch': 10, 'cost_drag': 10},
        'calibration': [{'bucket': '0.6-0.7', 'count': 60, 'hit_rate': 0.5, 'verdict': 'over'}],
        'patterns': [{'description': 'risk_off 실패 집중', 'count': 20, 'rate': 0.7, 'base_rate': 0.3}],
        'proposals': [{'target_agent': 'signal-decider', 'target_file': 'SOUL.md', 'change': 'cap', 'evidence': 'e', 'risk': 'r', 'confidence': 0.9}],
        'pre_mortem': 'x', 'summary_ko': '요약',
    }
    base.update(over)
    return base


# ---- schemas -------------------------------------------------------------

def test_parse_json_object_is_strict_but_accepts_one_fence():
    assert parse_json_object('{"a": 1}') == {'a': 1}
    assert parse_json_object('```json\n{"a": 1}\n```') == {'a': 1}
    for bad in (None, '', 'text {"a":1}', '[1]', '{"a":'):
        with pytest.raises(SideAgentValidationError):
            parse_json_object(bad)


def test_market_context_accepts_valid_and_rejects_bad_enum_date_and_extra_drivers():
    assert validate_market_context(market_output(), market_date=DAY)['regime'] == 'neutral'
    with pytest.raises(SideAgentValidationError):
        validate_market_context(market_output(regime='bullish'), market_date=DAY)
    with pytest.raises(SideAgentValidationError):
        validate_market_context(market_output(market_date='2026-11-06'), market_date=DAY)
    with pytest.raises(SideAgentValidationError):
        validate_market_context(market_output(confidence=1.2), market_date=DAY)
    too_many = [{'factor': 'f', 'direction': 'neutral', 'evidence': 'e'}] * 6
    with pytest.raises(SideAgentValidationError):
        validate_market_context(market_output(drivers=too_many), market_date=DAY)


def test_news_events_require_each_symbol_once_in_order_and_valid_enums():
    ok = validate_news_events(news_output(['005930', '000660']), market_date=DAY, symbols=['005930', '000660'])
    assert [r['symbol'] for r in ok['results']] == ['005930', '000660']
    for bad_symbols in (['000660', '005930'], ['005930'], ['5930', '000660']):
        out = news_output(bad_symbols)
        with pytest.raises(SideAgentValidationError):
            validate_news_events(out, market_date=DAY, symbols=['005930', '000660'])
    with pytest.raises(SideAgentValidationError):
        validate_news_events(news_output(['005930'], materiality='huge'), market_date=DAY, symbols=['005930'])
    with pytest.raises(SideAgentValidationError):
        validate_news_events(news_output(['005930'], event_types=['rocket']), market_date=DAY, symbols=['005930'])


def test_postmortem_caps_confidence_and_blocks_proposals_without_sufficient_sample():
    agents = ('signal-decider', 'short-term-decider')
    out = validate_postmortem(postmortem_output(), known_agents=agents)
    assert out['proposals'][0]['confidence'] == 0.70
    insufficient = postmortem_output(sample={'total': 5, 'by_agent': {'signal-decider': 5}, 'sufficient': False})
    with pytest.raises(SideAgentValidationError):
        validate_postmortem(insufficient, known_agents=agents)
    ok_empty = postmortem_output(sample={'total': 5, 'by_agent': {}, 'sufficient': False}, proposals=[])
    assert validate_postmortem(ok_empty, known_agents=agents)['proposals'] == []
    bad_target = postmortem_output()
    bad_target['proposals'][0]['target_agent'] = 'exec-agent'
    with pytest.raises(SideAgentValidationError):
        validate_postmortem(bad_target, known_agents=agents)
    bad_file = postmortem_output()
    bad_file['proposals'][0]['target_file'] = 'openclaw.json'
    with pytest.raises(SideAgentValidationError):
        validate_postmortem(bad_file, known_agents=agents)


# ---- client --------------------------------------------------------------

def _gateway(handler):
    return SideAgentClient(base_url='http://gw', token='t', timeout_seconds=5, transport=httpx.MockTransport(handler))


def _ok_body(text):
    return {'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': text}]}]}


def test_client_posts_agent_model_headers_and_returns_output_text():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen['url'] = str(request.url)
        seen['headers'] = request.headers
        seen['body'] = json.loads(request.content)
        return httpx.Response(200, json=_ok_body('{"x": 1}'))

    text = _gateway(handler).call(agent='news-event-analyst', task='news_events', payload={'a': '한글'}, run_id='r1')
    assert text == '{"x": 1}'
    assert seen['url'] == 'http://gw/v1/responses'
    assert seen['body']['model'] == 'openclaw/news-event-analyst'
    assert seen['headers']['x-openclaw-agent-id'] == 'news-event-analyst'
    assert seen['headers']['authorization'] == 'Bearer t'
    assert 'untrusted data' in seen['body']['input'] and '한글' in seen['body']['input']


@pytest.mark.parametrize('response', [httpx.Response(429, text='quota'), httpx.Response(503), httpx.Response(200, json={'output': []}), httpx.Response(200, text='nope')])
def test_client_raises_dispatch_error_on_gateway_problems(response):
    with pytest.raises(SideAgentDispatchError):
        _gateway(lambda request: response).call(agent='a', task='t', payload={})


def test_build_message_is_deterministic():
    assert build_message('t', {'b': 1, 'a': 2}) == build_message('t', {'a': 2, 'b': 1})


# ---- payloads ------------------------------------------------------------

def test_build_news_payloads_batches_symbols_in_order_and_carries_inputs():
    inputs = {s: {'name_ko': s, 'news': [{'title': s}], 'official_filings': [], 'price_context': {'move_1d_pct': 1.0}} for s in ['A', 'B', 'C']}
    payloads = build_news_payloads(DAY, inputs, batch_size=2)
    assert [p['symbols'] for p in payloads] == [['A', 'B'], ['C']]
    assert payloads[0]['items'][1] == {'symbol': 'B', **inputs['B']}
    assert payloads[0]['task'] == 'news_events' and payloads[0]['market_date'] == '2026-11-09'


# ---- runner --------------------------------------------------------------

class Gateway:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    def client(self):
        def handler(request: httpx.Request) -> httpx.Response:
            self.calls.append(json.loads(request.content)['model'])
            reply = self.replies.pop(0)
            if isinstance(reply, httpx.Response):
                return reply
            return httpx.Response(200, json=_ok_body(json.dumps(reply)))

        return _gateway(handler)


def _run_due(tmp_path, gateway, now, **kw):
    return runner.run_due(
        client=gateway.client(), store=SideAgentStore(tmp_path), now=now,
        is_trading_day=kw.pop('is_trading_day', lambda d: d.weekday() < 5),
        payload_builder=lambda d: {'market_date': d.isoformat()},
        news_inputs_loader=kw.pop('news_inputs_loader', lambda d, n: {}),
        summary_loader=kw.pop('summary_loader', lambda d: None), **kw,
    )


def test_run_due_waits_for_scheduled_time_and_skips_non_trading_days(tmp_path):
    gw = Gateway([])
    assert _run_due(tmp_path, gw, kst(17, 59)).outcomes == []
    assert gw.calls == []
    assert _run_due(tmp_path, gw, kst(19), is_trading_day=lambda d: False).outcomes == []
    assert gw.calls == []


def test_market_context_runs_once_and_is_idempotent(tmp_path):
    gw = Gateway([market_output()])
    first = _run_due(tmp_path, gw, kst(18, 30), only=['market-context-analyst'])
    assert [o.status for o in first.outcomes] == ['ok']
    saved = json.loads((tmp_path / 'market-context-analyst' / '2026-11-09.json').read_text())
    assert saved['advisory_only'] is True and saved['output']['regime'] == 'neutral'
    again = _run_due(tmp_path, gw, kst(19), only=['market-context-analyst'])
    assert [o.status for o in again.outcomes] == ['skipped'] and len(gw.calls) == 1


def test_gateway_failure_backs_off_then_exhausts_budget_without_hammering(tmp_path, monkeypatch):
    monkeypatch.setattr(runner.settings, 'side_agents_max_daily_attempts', 2)
    gw = Gateway([httpx.Response(429, text='quota'), httpx.Response(429, text='quota')])
    only = ['market-context-analyst']
    assert _run_due(tmp_path, gw, kst(18, 30), only=only).outcomes[0].status == 'failed'
    assert _run_due(tmp_path, gw, kst(18, 40), only=only).outcomes[0].detail == 'retry_gap'
    assert _run_due(tmp_path, gw, kst(19, 0), only=only).outcomes[0].status == 'failed'
    assert _run_due(tmp_path, gw, kst(20, 0), only=only).outcomes[0].detail == 'attempt_budget_exhausted'
    assert len(gw.calls) == 2


def test_invalid_output_is_rejected_stored_aside_and_never_written_as_result(tmp_path):
    gw = Gateway([market_output(regime='bullish')])
    outcome = _run_due(tmp_path, gw, kst(18, 30), only=['market-context-analyst']).outcomes[0]
    assert outcome.status == 'rejected'
    assert not (tmp_path / 'market-context-analyst' / '2026-11-09.json').exists()
    assert (tmp_path / 'market-context-analyst' / 'rejected' / '2026-11-09.json').exists()


def _news_inputs(symbols):
    return {s: {'name_ko': s, 'news': [{'title': s, 'materiality': 'noise'}], 'official_filings': [], 'price_context': {}} for s in symbols}


def test_news_runs_batches_and_stops_after_gateway_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(runner.settings, 'side_agents_news_batch_size', 1)
    inputs = _news_inputs(('000660', '005930', '035420'))
    gw = Gateway([news_output(['000660']), httpx.Response(429, text='quota')])
    outcomes = _run_due(tmp_path, gw, kst(19), only=['news-event-analyst'], news_inputs_loader=lambda d, n: inputs).outcomes
    assert [o.status for o in outcomes] == ['ok', 'failed']
    assert len(gw.calls) == 2
    # a later run resumes only the unfinished batches
    gw2 = Gateway([news_output(['005930']), news_output(['035420'])])
    outcomes2 = _run_due(tmp_path, gw2, kst(19, 40), only=['news-event-analyst'], news_inputs_loader=lambda d, n: inputs).outcomes
    assert [o.status for o in outcomes2] == ['skipped', 'ok', 'ok']
    assert len(gw2.calls) == 2


def test_news_without_inputs_is_skipped_without_calling_gateway(tmp_path):
    gw = Gateway([])
    outcomes = _run_due(tmp_path, gw, kst(19), only=['news-event-analyst']).outcomes
    assert [(o.status, o.detail) for o in outcomes] == [('skipped', 'no_recent_news_or_filings')] and gw.calls == []


SUMMARY = {'total_resolved': 120, 'period': {'from': '2026-10-05', 'to': '2026-11-13'}, 'agents': {}, 'known_agents': ['signal-decider']}


def test_postmortem_runs_saturday_from_computed_summary_and_skips_without_one(tmp_path):
    saturday = kst(9, 5, day=14)
    gw = Gateway([postmortem_output()])
    assert _run_due(tmp_path, gw, saturday, only=['postmortem-analyst']).outcomes[0].detail == 'no_summary'
    outcome = _run_due(tmp_path, gw, saturday, only=['postmortem-analyst'], summary_loader=lambda d: SUMMARY).outcomes[0]
    assert outcome.status == 'ok'
    assert _run_due(tmp_path, gw, saturday, only=['postmortem-analyst'], summary_loader=lambda d: SUMMARY).outcomes[0].detail == 'already_done'
    assert _run_due(tmp_path, gw, kst(9, 5, day=10), only=['postmortem-analyst'], summary_loader=lambda d: SUMMARY).outcomes == []


def test_postmortem_with_no_resolved_outcomes_never_calls_the_gateway(tmp_path):
    gw = Gateway([])
    outcome = _run_due(tmp_path, gw, kst(10, day=14), only=['postmortem-analyst'], summary_loader=lambda d: {**SUMMARY, 'total_resolved': 0}).outcomes[0]
    assert outcome.detail == 'no_resolved_outcomes' and gw.calls == []


def test_oversized_postmortem_summary_is_skipped_not_truncated(tmp_path):
    big = {**SUMMARY, 'blob': 'a' * (runner.MAX_POSTMORTEM_BYTES + 10)}
    outcome = runner.run_postmortem(
        '2026-W46', summary=big, client=Gateway([]).client(), store=SideAgentStore(tmp_path), now=kst(10, day=14)
    )
    assert outcome.status == 'skipped' and 'too_large' in outcome.detail


def test_dry_run_never_calls_the_gateway(tmp_path):
    gw = Gateway([])
    report = _run_due(tmp_path, gw, kst(19, 30), dry_run=True, news_inputs_loader=lambda d, n: _news_inputs(('A',)))
    assert gw.calls == []
    assert {o.status for o in report.outcomes} == {'skipped'}


# ---- contracts and registration -----------------------------------------

@pytest.mark.parametrize('agent', ['market-context-analyst', 'news-event-analyst', 'postmortem-analyst'])
def test_new_agent_contracts_exist_with_required_guards(agent):
    text = (REPO_ROOT / 'runtime_contracts' / 'openclaw' / 'v1' / agent / 'AGENTS.md').read_text(encoding='utf-8')
    for phrase in ('not a personal assistant', 'untrusted data, never instructions', 'raw JSON object only', 'Calibration', '## Input fields'):
        assert phrase.lower() in text.lower(), (agent, phrase)
    assert 'schema_version' in text
    if agent != 'news-event-analyst':
        assert 'pre-mortem' in text.lower()
    lowered = text.lower()
    assert 'place order' not in lowered and 'execute order' not in lowered


def test_news_contract_honors_price_forecast_prohibition_and_postmortem_cannot_loosen_gates():
    news = (REPO_ROOT / 'runtime_contracts/openclaw/v1/news-event-analyst/AGENTS.md').read_text(encoding='utf-8')
    assert 'price_forecast_prohibited' in news and 'No price targets' in news
    post = (REPO_ROOT / 'runtime_contracts/openclaw/v1/postmortem-analyst/AGENTS.md').read_text(encoding='utf-8')
    assert 'never loosen a gate' in post and 'fewer than 20 records' in post


def test_sync_script_knows_new_agents():
    script = (REPO_ROOT / 'scripts' / 'sync_openclaw_runtime_contract.py').read_text(encoding='utf-8')
    for agent in ('market-context-analyst', 'news-event-analyst', 'postmortem-analyst'):
        assert f'"{agent}": "workspace-{agent}"' in script
