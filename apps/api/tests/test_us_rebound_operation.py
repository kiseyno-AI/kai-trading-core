import asyncio
from datetime import date, datetime
import json
from types import SimpleNamespace

import httpx
import pytest

from app.overnight_rebound.operation import RuleOperationConfig, run_us_rebound_operation
from app.internal.exact_repair_manifest import canonical_json_sha256
from app.overnight_rebound.evidence import EvidenceResult
from tests.test_us_rebound_execution_contract import evaluation, market, policy
from tests.test_us_rebound_rule_sizing import account
from tests.test_us_rebound_execution_outbox import Remote


NOW = datetime.fromisoformat('2026-11-30T08:20:00+09:00')


@pytest.fixture
def current_api_clock(monkeypatch):
    def set_clock(value):
        class Clock:
            @staticmethod
            def now(tz):
                return value.astimezone(tz)
        monkeypatch.setattr('app.api.routes_us_rebound.datetime', Clock)
    set_clock(NOW)
    return set_clock


def forbidden(**kwargs):
    raise AssertionError('Execution/evidence I/O is forbidden in this scope')


def test_observation_and_disabled_execution_do_no_execution_io_or_create_state(tmp_path):
    for mode, enabled, reason in (('observe', True, 'observation_only'), ('live', False, 'execution_disabled')):
        result = asyncio.run(run_us_rebound_operation(
            mode=mode, config=RuleOperationConfig(enabled=enabled), evaluation=evaluation(),
            state_dir=tmp_path, clock=lambda: NOW, seal_loader=forbidden, adapter_factory=forbidden,
        ))
        assert result['status'] == 'disabled'
        assert result['reason'] == reason
    assert list(tmp_path.iterdir()) == []


def test_live_policy_cannot_enable_execution_without_product_approval(tmp_path):
    from pathlib import Path

    config = configuration(tmp_path)
    path = Path(config.policy_file)
    artifact = json.loads(path.read_text())
    artifact['policy'].update(execution_mode='live', validation_status='validated_live',
        validation_evidence_id='synthetic-validation', validation_evidence_hash='f' * 64)
    path.write_text(json.dumps(artifact))
    result = asyncio.run(run_us_rebound_operation(mode='live', config=config,
        evaluation=current_evaluation(), state_dir=tmp_path, clock=lambda: NOW,
        seal_loader=forbidden, adapter_factory=forbidden, account_target_reader=target_reader))

    assert result['status'] == 'unavailable'
    assert result['reason'] == 'live_execution_not_approved'


def current_evaluation(status='WATCH'):
    value = evaluation()
    value.update(target_trade_date='2026-11-30', completed_at=NOW.replace(hour=7, minute=15).isoformat(), status=status)
    value.pop('evaluation_id')
    value['evaluation_id'] = canonical_json_sha256(value)
    return value


def configuration(tmp_path):
    parameters = policy().model_dump(mode='json')
    parameters.pop('entry_expires_at')
    path = tmp_path / 'policy.json'
    path.write_text(json.dumps({'schema_version': 'us_rebound_execution_policy_v1',
                               'entry_expires_at_kst': '09:10:00', 'policy': parameters}))
    path.chmod(0o600)
    return RuleOperationConfig(enabled=True, account_reference='KIS:12345678-01', policy_file=str(path),
        destination_base_url='http://exec.test', bearer_token='synthetic-exec-token',
        authority_id='explicit-test-authorization', authority_until='2027-01-01T00:00:00+00:00')


def target_reader():
    return [SimpleNamespace(account_reference='KIS:12345678-01')], []


def seal_loader(**kwargs):
    return SimpleNamespace(target_trade_date=date(2026, 11, 30), source_environment='real',
                           input_kind='history_22', manifest_id=current_evaluation()['input_manifest_id'])


class Evidence:
    calls = []
    def __init__(self, **kwargs):
        pass

    async def market(self, *, symbol, session, **kwargs):
        self.calls.append(('market', symbol))
        return EvidenceResult(market(symbol=symbol, prior_close_date=session.previous_trade_date,
                                     observed_at=kwargs['clock']()), 'available')

    async def account(self, *, setup, **kwargs):
        self.calls.append(('account', setup.symbol))
        updates = {name: kwargs['clock']() for name in ('observed_at', 'balance_observed_at', 'order_possible_requested_at',
            'order_possible_observed_at', 'broker_orders_observed_at', 'baseline_observed_at')}
        return EvidenceResult(account(account_reference=setup.account_reference, symbol=setup.symbol,
                                      order_possible_price=setup.chase_limit_price, **updates), 'available')


class WireExec(Remote):
    def __init__(self):
        self.plan, self.activated = None, False
        self.posts, self.activation_posts, self.quantity_updates = [], [], {}
        self.lose_create, self.lose_activation = False, False
        self.requests = []

    def readback(self):
        value = super().readback()
        for item in value['item_states']:
            if item.get('core_activation_state'):
                item['core_activation_state']['authority_generation_fingerprint'] = self.plan['items'][0]['risk_context']['_core_activation']['authority_generation_fingerprint']
        return value

    def __call__(self, request):
        self.requests.append((request.method, request.url.path))
        assert request.headers['Authorization'] == 'Bearer synthetic-exec-token'
        if request.url.path.endswith('/capabilities'):
            return httpx.Response(200, json={
                'capabilities': ['rule_origin_krx_us_rebound_v1'],
                'runtime_binding': {'mode': 'paper', 'provider': 'kis', 'account_reference_matches': True,
                                    'configured_route_fingerprint': 'a' * 32},
                'rule_origin': {'supported': True, 'enabled': True, 'strategy_id': 'krx_us_rebound',
                                'core_sized_only': True, 'explicit_close_exit_at': True},
            })
        if request.method == 'GET':
            return httpx.Response(200, json=[] if self.plan is None else [self.readback()])
        value = json.loads(request.content)
        if request.url.path == '/v4/execution-plans':
            self.plan = value
            self.posts.append(value)
            if self.lose_create:
                raise httpx.ReadTimeout('lost create ACK', request=request)
            return httpx.Response(201, json=self.ack())
        assert request.url.path == '/v4/execution-plans/activations'
        self.activation_posts.append(value)
        self.activated = True
        if self.lose_activation:
            raise httpx.ReadTimeout('lost activation ACK', request=request)
        return httpx.Response(200, json={
            'plan_id': value['plan_id'], 'execution_plan_id': value['execution_plan_id'], 'activated_item_count': 3,
            'contract_ack': {'schema_version': value['schema_version'], 'contract_version': value['contract_version'],
                'capability': value['required_capability'], 'plan_id': value['plan_id'], 'status': 'activated',
                'origin_type': 'rule', 'rule_origin': self.plan['rule_origin'],
                'dispatch_material_fingerprint': value['dispatch_material_fingerprint'],
                'authority_generation_fingerprint': value['authority_generation_fingerprint']},
            'item_acks': [{**item, 'execution_plan_id': value['execution_plan_id'], 'status': 'activated'}
                          for item in value['items']],
        })


def test_fresh_watch_uses_existing_outbox_registration_readback_activation_and_safe_status(tmp_path):
    config = configuration(tmp_path)
    remote = WireExec()
    Evidence.calls = []
    async def tick():
        async with httpx.AsyncClient(base_url=config.destination_base_url,
            headers={'Authorization': 'Bearer synthetic-exec-token'}, transport=httpx.MockTransport(remote)) as client:
            return await run_us_rebound_operation(mode='paper', config=config, evaluation=current_evaluation(),
                state_dir=tmp_path, clock=lambda: NOW, seal_loader=seal_loader,
                adapter_factory=Evidence, client=client, account_target_reader=target_reader)
    result = asyncio.run(tick())
    assert result['status'] == 'active'
    assert len(remote.posts) == len(remote.activation_posts) == 1
    assert len(Evidence.calls) == 4
    public = json.dumps(result)
    assert '12345678' not in public and 'target_quantity' not in public and 'claim_id' not in public
    assert remote.plan['items'][0]['quantity_policy']['target_quantity'] == 19


@pytest.mark.parametrize('lost', ['create', 'activation'])
def test_restart_reconciles_lost_ack_without_new_provider_calls_or_posts(tmp_path, lost):
    config, remote = configuration(tmp_path), WireExec()
    remote.lose_create, remote.lose_activation = lost == 'create', lost == 'activation'
    Evidence.calls = []
    async def tick(factory, loader, now):
        async with httpx.AsyncClient(base_url=config.destination_base_url, headers={'Authorization': 'Bearer synthetic-exec-token'},
                                     transport=httpx.MockTransport(remote)) as client:
            return await run_us_rebound_operation(mode='paper', config=config, evaluation=current_evaluation(), state_dir=tmp_path,
                clock=lambda: now, seal_loader=loader, adapter_factory=factory, client=client, account_target_reader=target_reader)
    assert asyncio.run(tick(Evidence, seal_loader, NOW))['status'] == 'pending'
    calls = list(Evidence.calls)
    result = asyncio.run(tick(forbidden, forbidden, NOW.replace(minute=21)))
    assert result['status'] == ('active' if lost == 'activation' else 'pending')
    assert Evidence.calls == calls
    assert len(remote.posts) == 1
    assert len(remote.activation_posts) == (1 if lost == 'activation' else 0)


def test_minute_poll_does_not_repeat_provider_attempt_and_second_slot_only_refreshes_unprepared(tmp_path):
    config, remote = configuration(tmp_path), WireExec()
    Evidence.calls = []
    async def tick(now, factory=Evidence):
        async with httpx.AsyncClient(base_url=config.destination_base_url, headers={'Authorization': 'Bearer synthetic-exec-token'},
                                     transport=httpx.MockTransport(remote)) as client:
            return await run_us_rebound_operation(mode='paper', config=config, evaluation=current_evaluation(), state_dir=tmp_path,
                clock=lambda: now, seal_loader=seal_loader, adapter_factory=factory, client=client, account_target_reader=target_reader)
    assert asyncio.run(tick(NOW))['status'] == 'active'
    first = list(Evidence.calls)
    requests = list(remote.requests)
    assert asyncio.run(tick(NOW.replace(minute=21), forbidden))['status'] == 'active'
    assert Evidence.calls == first and remote.requests == requests
    asyncio.run(tick(NOW.replace(minute=30)))
    assert Evidence.calls == first + [('market', '091160'), ('account', '091160')]
    asyncio.run(tick(NOW.replace(minute=31), forbidden))
    assert len(Evidence.calls) == 6
    assert len(remote.posts) == len(remote.activation_posts) == 1


@pytest.mark.parametrize('case', ['missing_config', 'missing_policy', 'no_signal', 'no_new_us', 'expired', 'before_slot', 'future_evaluation'])
def test_ineligible_execution_scope_never_enters_evidence_or_transport(tmp_path, case):
    config = configuration(tmp_path)
    value, now = current_evaluation(), NOW
    if case == 'missing_config':
        config = RuleOperationConfig(enabled=True)
    elif case == 'missing_policy':
        from dataclasses import replace
        config = replace(config, policy_file=str(tmp_path / 'absent.json'))
    elif case == 'no_signal':
        value = current_evaluation('NO_SIGNAL')
    elif case == 'no_new_us':
        now = NOW.replace(day=27)
        value.update(target_trade_date='2026-11-27', completed_at=now.replace(hour=7, minute=15).isoformat())
        value.pop('evaluation_id')
        value['evaluation_id'] = canonical_json_sha256(value)
    elif case == 'expired':
        now = NOW.replace(minute=40)
    elif case == 'future_evaluation':
        value['completed_at'] = NOW.replace(minute=21).isoformat()
        value.pop('evaluation_id')
        value['evaluation_id'] = canonical_json_sha256(value)
    else:
        now = NOW.replace(minute=19)
    result = asyncio.run(run_us_rebound_operation(mode='paper', config=config, evaluation=value, state_dir=tmp_path,
        clock=lambda: now, seal_loader=forbidden, adapter_factory=forbidden, account_target_reader=target_reader))
    assert result['status'] in ('unavailable', 'skipped', 'not_due')


def test_api_execution_status_excludes_private_material_even_if_extra_fields_are_present(tmp_path):
    from app.overnight_rebound.operation import load_us_rebound_execution_status
    from app.internal.exact_repair_manifest import write_checkpoint
    path = tmp_path / 'rebound/real-2026-11-30/execution-status.json'
    value = {'strategy_id': 'krx_us_rebound', 'target_trade_date': '2026-11-30', 'source_environment': 'real',
             'mode': 'paper', 'status': 'pending', 'account_reference': 'KIS:private-account',
             'items': [{'symbol': '069500', 'status': 'create_pending', 'target_quantity': 99, 'claim_id': 'private-hash'}]}
    write_checkpoint(path, {**value, 'record_hash': canonical_json_sha256(value)})
    result = load_us_rebound_execution_status(target_trade_date=date(2026, 11, 30), state_dir=tmp_path)
    assert 'private' not in json.dumps(result)
    assert result['items'] == [{'symbol': '069500', 'status': 'create_pending'}]


def test_existing_independent_runner_calls_operation_only_in_explicit_execution_mode(tmp_path):
    from functools import partial
    from app.internal.exact_repair_manifest import write_checkpoint
    from app.overnight_rebound.runner import run_us_rebound_tick
    path = tmp_path / 'rebound/real-2026-11-30/evaluation.json'
    write_checkpoint(path, current_evaluation())
    config, remote = configuration(tmp_path), WireExec()
    client = httpx.AsyncClient(base_url=config.destination_base_url, headers={'Authorization': 'Bearer synthetic-exec-token'},
                               transport=httpx.MockTransport(remote))
    try:
        operation = partial(run_us_rebound_operation, client=client, adapter_factory=Evidence, account_target_reader=target_reader)
        result = run_us_rebound_tick(mode='observe', enabled_from=date(2026, 11, 30), state_dir=tmp_path,
            clock=lambda: NOW, collector=forbidden, loader=forbidden, execution_config=config, execution_runner=forbidden)
        assert result['evaluation']['order_authority'] is False
        assert remote.requests == []
        result = run_us_rebound_tick(mode='paper', enabled_from=date(2026, 11, 30), state_dir=tmp_path,
            clock=lambda: NOW, collector=forbidden, loader=seal_loader, execution_config=config, execution_runner=operation)
        assert result['execution']['status'] == 'active'
        assert len(remote.posts) == len(remote.activation_posts) == 1
    finally:
        asyncio.run(client.aclose())


def test_read_only_status_reports_persisted_authority_without_http_or_private_fields(tmp_path, monkeypatch, current_api_clock):
    from fastapi.testclient import TestClient
    from app.core.settings import settings
    from app.main import app
    from app.internal.exact_repair_manifest import write_checkpoint
    value = {'strategy_id': 'krx_us_rebound', 'target_trade_date': '2026-11-30', 'source_environment': 'real',
        'mode': 'paper', 'status': 'active', 'checked_at': NOW.isoformat(), 'private_claim_hash': 'private',
        'items': [{'symbol': '069500', 'status': 'active', 'last_confirmed_at': NOW.isoformat(),
                   'account_reference': 'KIS:private', 'quantity': 19}]}
    write_checkpoint(tmp_path / 'rebound/real-2026-11-30/execution-status.json',
                     {**value, 'record_hash': canonical_json_sha256(value)})
    monkeypatch.setattr(settings, 'upstream_bearer_token', None)
    monkeypatch.setattr(settings, 'us_rebound_state_dir', str(tmp_path))
    monkeypatch.setattr(settings, 'us_rebound_mode', 'paper')
    monkeypatch.setattr(settings, 'us_rebound_execution_enabled', True)
    response = TestClient(app).get('/strategies/us-rebound/status?target_trade_date=2026-11-30')
    assert response.status_code == 200
    assert response.json()['order_execution_enabled'] is True
    assert response.json()['execution_connection'] == 'rule_v4'
    assert 'private' not in response.text and 'quantity' not in response.text


@pytest.mark.parametrize(('mode', 'enabled'), [('observe', True), ('paper', False), ('live', True), ('paper', 'invalid')])
def test_persisted_activation_does_not_assert_current_permission_after_operator_scope_changes(tmp_path, monkeypatch, current_api_clock, mode, enabled):
    from fastapi.testclient import TestClient
    from app.core.settings import settings
    from app.main import app
    from app.internal.exact_repair_manifest import write_checkpoint
    value = {'strategy_id': 'krx_us_rebound', 'target_trade_date': '2026-11-30', 'source_environment': 'real',
        'mode': 'paper', 'status': 'active', 'checked_at': NOW.isoformat(),
        'items': [{'symbol': '069500', 'status': 'active', 'last_confirmed_at': NOW.isoformat()}]}
    write_checkpoint(tmp_path / 'rebound/real-2026-11-30/execution-status.json',
                     {**value, 'record_hash': canonical_json_sha256(value)})
    monkeypatch.setattr(settings, 'upstream_bearer_token', None)
    monkeypatch.setattr(settings, 'us_rebound_state_dir', str(tmp_path))
    monkeypatch.setattr(settings, 'us_rebound_mode', mode)
    monkeypatch.setattr(settings, 'us_rebound_execution_enabled', enabled)
    response = TestClient(app).get('/strategies/us-rebound/status?target_trade_date=2026-11-30')
    assert response.status_code == 200
    assert response.json()['order_execution_enabled'] is False
    assert response.json()['execution']['status'] == 'active'


def test_fifo_policy_is_rejected_without_waiting_for_a_writer_through_public_operation(tmp_path):
    import os
    from pathlib import Path
    import subprocess
    import sys

    fifo = tmp_path / 'policy.fifo'
    os.mkfifo(fifo, 0o600)
    code = '''
import asyncio, json, sys
from dataclasses import replace
from pathlib import Path
from tests.test_us_rebound_operation import NOW, configuration, current_evaluation
from app.overnight_rebound.operation import run_us_rebound_operation
folder = Path(sys.argv[1])
config = replace(configuration(folder), policy_file=sys.argv[2])
result = asyncio.run(run_us_rebound_operation(mode='paper', config=config,
    evaluation=current_evaluation(), state_dir=folder, clock=lambda: NOW))
print(json.dumps(result))
'''
    with subprocess.Popen([sys.executable, '-c', code, str(tmp_path), str(fifo)],
        cwd=Path(__file__).parents[1], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as child:
        try:
            output, error = child.communicate(timeout=8)
        except subprocess.TimeoutExpired:
            child.kill()
            child.communicate()
            pytest.fail('FIFO policy blocked the operation while waiting for a writer')
    assert child.returncode == 0, error
    result = json.loads(output)
    assert result['status'] == 'unavailable'
    assert result['reason'] == 'execution_policy_or_scope_unavailable'


@pytest.mark.parametrize(('server_now', 'target'), [
    (NOW.replace(month=12, day=1), date(2026, 11, 30)),
    (NOW, date(2026, 12, 1)),
])
def test_historical_and_future_active_cache_remains_visible_without_current_permission(tmp_path, monkeypatch, current_api_clock, server_now, target):
    from fastapi.testclient import TestClient
    from app.core.settings import settings
    from app.main import app
    from app.internal.exact_repair_manifest import write_checkpoint
    current_api_clock(server_now)
    value = {'strategy_id': 'krx_us_rebound', 'target_trade_date': target.isoformat(), 'source_environment': 'real',
        'mode': 'paper', 'status': 'active', 'checked_at': NOW.isoformat(),
        'items': [{'symbol': '069500', 'status': 'active', 'last_confirmed_at': NOW.isoformat()}]}
    write_checkpoint(tmp_path / f'rebound/real-{target.isoformat()}/execution-status.json',
                     {**value, 'record_hash': canonical_json_sha256(value)})
    monkeypatch.setattr(settings, 'upstream_bearer_token', None)
    monkeypatch.setattr(settings, 'us_rebound_state_dir', str(tmp_path))
    monkeypatch.setattr(settings, 'us_rebound_mode', 'paper')
    monkeypatch.setattr(settings, 'us_rebound_execution_enabled', True)
    response = TestClient(app).get(f'/strategies/us-rebound/status?target_trade_date={target.isoformat()}')
    assert response.status_code == 200
    assert response.json()['server_time'].startswith(server_now.date().isoformat())
    assert response.json()['order_execution_enabled'] is False
    assert response.json()['execution']['status'] == 'active'


@pytest.mark.parametrize('expiry', ['08:10:00', '08:25:00', '15:25:00'])
def test_invalid_entry_window_skips_all_new_evidence_calls(tmp_path, expiry):
    config = configuration(tmp_path)
    from pathlib import Path
    path = Path(config.policy_file)
    value = json.loads(path.read_text())
    value['entry_expires_at_kst'] = expiry
    path.write_text(json.dumps(value))
    result = asyncio.run(run_us_rebound_operation(mode='paper', config=config, evaluation=current_evaluation(),
        state_dir=tmp_path, clock=lambda: NOW, seal_loader=seal_loader,
        adapter_factory=forbidden, account_target_reader=target_reader))
    assert result['status'] == 'unavailable'


def test_expired_entry_window_still_reconciles_existing_pending_ack_without_new_evidence(tmp_path):
    config, remote = configuration(tmp_path), WireExec()
    remote.lose_activation = True
    async def tick(now, factory, loader):
        async with httpx.AsyncClient(base_url=config.destination_base_url, headers={'Authorization': 'Bearer synthetic-exec-token'},
                                     transport=httpx.MockTransport(remote)) as client:
            return await run_us_rebound_operation(mode='paper', config=config, evaluation=current_evaluation(),
                state_dir=tmp_path, clock=lambda: now, seal_loader=loader,
                adapter_factory=factory, client=client, account_target_reader=target_reader)
    assert asyncio.run(tick(NOW, Evidence, seal_loader))['status'] == 'pending'
    assert asyncio.run(tick(NOW.replace(hour=9, minute=11), forbidden, forbidden))['status'] == 'active'
    assert len(remote.posts) == len(remote.activation_posts) == 1


@pytest.mark.parametrize('failure,reason', [
    ('missing_policy', 'execution_policy_or_scope_unavailable'),
    ('invalid_policy', 'execution_policy_or_scope_unavailable'),
    ('unreadable_policy', 'execution_policy_or_scope_unavailable'),
    ('missing_config', 'execution_configuration_unavailable'),
    ('changed_scope', 'persisted_execution_scope_changed'),
])
def test_confirmed_families_remain_visible_when_configuration_fails(
    tmp_path, monkeypatch, current_api_clock, failure, reason,
):
    from dataclasses import replace
    from pathlib import Path
    from fastapi.testclient import TestClient
    from app.core.settings import settings
    from app.main import app
    from app.overnight_rebound.operation import load_us_rebound_execution_status

    config, remote = configuration(tmp_path), WireExec()
    async def activate():
        async with httpx.AsyncClient(base_url=config.destination_base_url,
            headers={'Authorization': 'Bearer synthetic-exec-token'}, transport=httpx.MockTransport(remote)) as client:
            return await run_us_rebound_operation(mode='paper', config=config, evaluation=current_evaluation(),
                state_dir=tmp_path, clock=lambda: NOW, seal_loader=seal_loader,
                adapter_factory=Evidence, client=client, account_target_reader=target_reader)
    confirmed = asyncio.run(activate())
    assert confirmed['status'] == 'active'
    assert any(item.get('last_confirmed_at') == NOW.isoformat() for item in confirmed['items'])
    calls, requests = list(Evidence.calls), list(remote.requests)
    if failure == 'missing_policy':
        Path(config.policy_file).unlink()
    elif failure == 'invalid_policy':
        Path(config.policy_file).write_text('{')
    elif failure == 'unreadable_policy':
        config = replace(config, policy_file=str(tmp_path))
    elif failure == 'missing_config':
        config = replace(config, account_reference=None)
    else:
        config = replace(config, authority_id='different-operator-scope')
    later = NOW.replace(minute=21)
    monkeypatch.setattr(httpx.AsyncClient, 'request', forbidden)
    result = asyncio.run(run_us_rebound_operation(mode='paper', config=config, evaluation=current_evaluation(),
        state_dir=tmp_path, clock=lambda: later, seal_loader=forbidden,
        adapter_factory=forbidden, account_target_reader=target_reader))

    assert result['status'] == 'unavailable'
    assert result['reason'] == reason
    assert result['items'] == confirmed['items']
    assert result['checked_at'] == later.isoformat()
    assert Evidence.calls == calls and remote.requests == requests
    assert load_us_rebound_execution_status(target_trade_date=NOW.date(), state_dir=tmp_path) == result
    monkeypatch.setattr(settings, 'us_rebound_state_dir', str(tmp_path))
    monkeypatch.setattr(settings, 'us_rebound_mode', 'paper')
    monkeypatch.setattr(settings, 'us_rebound_execution_enabled', True)
    current_api_clock(later)
    response = TestClient(app).get('/strategies/us-rebound/status?target_trade_date=2026-11-30')
    assert response.status_code == 200
    body = response.json()
    assert body['execution_connection'] == 'rule_v4'
    assert body['order_execution_enabled'] is True
    assert body['execution'] == result
    assert all(private not in json.dumps(body) for private in ('12345678', 'target_quantity', 'claim_id', 'policy.json'))


@pytest.mark.parametrize('corruption', ['json', 'hash', 'scope', 'items'])
def test_configuration_failure_does_not_trust_or_replace_corrupt_execution_status(
    tmp_path, monkeypatch, current_api_clock, corruption,
):
    from fastapi.testclient import TestClient
    from app.core.settings import settings
    from app.main import app
    from app.internal.exact_repair_manifest import write_checkpoint

    value = {'strategy_id': 'krx_us_rebound', 'target_trade_date': NOW.date().isoformat(),
        'source_environment': 'real', 'mode': 'paper', 'status': 'active', 'checked_at': NOW.isoformat(),
        'items': [{'symbol': '069500', 'status': 'active', 'last_confirmed_at': NOW.isoformat()}]}
    if corruption == 'scope':
        value['target_trade_date'] = '2026-11-27'
    elif corruption == 'items':
        value['items'] = ['invalid']
    path = tmp_path / 'rebound/real-2026-11-30/execution-status.json'
    write_checkpoint(path, {**value, 'record_hash': 'wrong' if corruption == 'hash' else canonical_json_sha256(value)})
    if corruption == 'json':
        path.write_text('{')
    previous = path.read_bytes()
    result = asyncio.run(run_us_rebound_operation(mode='paper', config=RuleOperationConfig(enabled=True),
        evaluation=current_evaluation(), state_dir=tmp_path, clock=lambda: NOW,
        seal_loader=forbidden, adapter_factory=forbidden))
    assert result['status'] == 'unavailable'
    assert result['reason'] == 'execution_status_unavailable'
    assert result['items'] == []
    assert path.read_bytes() == previous
    monkeypatch.setattr(settings, 'us_rebound_state_dir', str(tmp_path))
    monkeypatch.setattr(httpx.AsyncClient, 'request', forbidden)
    response = TestClient(app).get('/strategies/us-rebound/status?target_trade_date=2026-11-30')
    assert response.status_code == 503


@pytest.mark.parametrize(('mode', 'status'), [('paper', 'accepted'), ('paper', 'activation_pending'), ('live', 'active')])
def test_configuration_failure_preserves_registered_items_without_rebinding_their_mode(
    tmp_path, monkeypatch, current_api_clock, mode, status,
):
    from fastapi.testclient import TestClient
    from app.core.settings import settings
    from app.main import app
    from app.internal.exact_repair_manifest import write_checkpoint

    value = {'strategy_id': 'krx_us_rebound', 'target_trade_date': NOW.date().isoformat(),
        'source_environment': 'real', 'mode': mode, 'status': 'pending', 'checked_at': NOW.isoformat(),
        'items': [{'symbol': '069500', 'status': status, 'last_confirmed_at': NOW.isoformat()}]}
    path = tmp_path / 'rebound/real-2026-11-30/execution-status.json'
    write_checkpoint(path, {**value, 'record_hash': canonical_json_sha256(value)})
    monkeypatch.setattr(httpx.AsyncClient, 'request', forbidden)
    result = asyncio.run(run_us_rebound_operation(mode='paper', config=RuleOperationConfig(enabled=True),
        evaluation=current_evaluation(), state_dir=tmp_path, clock=lambda: NOW.replace(minute=21),
        seal_loader=forbidden, adapter_factory=forbidden))
    assert result['status'] == 'unavailable'
    assert result['items'] == value['items'] and result['mode'] == mode
    monkeypatch.setattr(settings, 'us_rebound_state_dir', str(tmp_path))
    monkeypatch.setattr(settings, 'us_rebound_mode', 'paper')
    monkeypatch.setattr(settings, 'us_rebound_execution_enabled', True)
    response = TestClient(app).get('/strategies/us-rebound/status?target_trade_date=2026-11-30')
    assert response.status_code == 200
    assert response.json()['execution_connection'] == 'rule_v4'
    assert response.json()['order_execution_enabled'] is False


def test_configuration_failure_does_not_import_another_sessions_execution_facts(tmp_path):
    from app.internal.exact_repair_manifest import write_checkpoint

    value = {'strategy_id': 'krx_us_rebound', 'target_trade_date': '2026-11-27',
        'source_environment': 'real', 'mode': 'paper', 'status': 'active',
        'items': [{'symbol': '069500', 'status': 'active', 'last_confirmed_at': '2026-11-27T08:20:00+09:00'}]}
    path = tmp_path / 'rebound/real-2026-11-27/execution-status.json'
    write_checkpoint(path, {**value, 'record_hash': canonical_json_sha256(value)})
    previous = path.read_bytes()
    result = asyncio.run(run_us_rebound_operation(mode='paper', config=RuleOperationConfig(enabled=True),
        evaluation=current_evaluation(), state_dir=tmp_path, clock=lambda: NOW,
        seal_loader=forbidden, adapter_factory=forbidden))
    assert result['status'] == 'unavailable'
    assert result['reason'] == 'execution_configuration_unavailable'
    assert result['items'] == []
    assert path.read_bytes() == previous

