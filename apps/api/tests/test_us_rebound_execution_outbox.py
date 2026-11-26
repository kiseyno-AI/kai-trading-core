import asyncio
from copy import deepcopy
import json

import httpx
import pytest

from app.overnight_rebound.execution_outbox import RuleExecutionAuthority, RuleExecutionOutbox
from tests.test_us_rebound_execution_contract import NOW, evaluation, market, policy, setup
from tests.test_us_rebound_rule_sizing import account


def authority():
    return RuleExecutionAuthority(
        enabled=True, mode='paper', account_reference='KIS:test-only',
        policy_hash=policy().policy_hash, authority_generation_fingerprint='e' * 64,
        evidence_id='explicit-test-authorization', valid_until=NOW.replace(hour=8, minute=40),
    )


class Remote:
    def __init__(self, outbox, prepared):
        self.outbox, self.prepared = outbox, prepared
        self.plan = None
        self.posts = []
        self.enabled = False
        self.lose_create_ack = True
        self.activation_posts = []
        self.activated = False
        self.lose_activation_ack = True
        self.quantity_updates = {}

    def __call__(self, request):
        if request.url.path.endswith('/capabilities'):
            assert request.headers['X-Core-Account-Reference'] == 'KIS:test-only'
            return httpx.Response(200, json={
                'capabilities': ['rule_origin_krx_us_rebound_v1'],
                'rule_origin': {'supported': True, 'enabled': self.enabled, 'strategy_id': 'krx_us_rebound',
                                'core_sized_only': True, 'explicit_close_exit_at': True},
                'runtime_binding': {'mode': 'paper', 'provider': 'kis',
                                    'configured_route_fingerprint': 'a' * 32, 'account_reference_matches': True},
            })
        if request.method == 'GET':
            return httpx.Response(200, json=[] if self.plan is None else [self.readback()])
        if request.url.path == '/v4/execution-plans/activations':
            assert self.outbox.status(self.prepared)['status'] == 'activation_pending'
            self.activation_posts.append(json.loads(request.content))
            self.activated = True
            if self.lose_activation_ack:
                raise httpx.ReadTimeout('activation response lost', request=request)
            activation = self.activation_posts[-1]
            return httpx.Response(200, json={
                'plan_id': activation['plan_id'], 'execution_plan_id': activation['execution_plan_id'],
                'activated_item_count': 3,
                'contract_ack': {
                    'schema_version': 'execution_handoff_activation_v1', 'contract_version': 'execution_handoff_contract_v4',
                    'capability': 'core_confirmed_activation_v1', 'plan_id': activation['plan_id'],
                    'status': 'activated', 'origin_type': 'rule', 'rule_origin': self.plan['rule_origin'],
                    'dispatch_material_fingerprint': activation['dispatch_material_fingerprint'],
                    'authority_generation_fingerprint': activation['authority_generation_fingerprint'],
                },
                'item_acks': [{**item, 'status': 'activated', 'execution_plan_id': activation['execution_plan_id']}
                              for item in activation['items']],
            })
        assert request.method == 'POST' and request.url.path == '/v4/execution-plans'
        assert self.outbox.status(self.prepared)['status'] == 'create_pending'
        payload = json.loads(request.content)
        self.posts.append(payload)
        self.plan = payload
        if self.lose_create_ack:
            raise httpx.ReadTimeout('accepted response lost', request=request)
        return httpx.Response(201, json=self.ack())

    def ack(self):
        plan = self.plan
        return {
            'plan_id': plan['plan_id'], 'execution_plan_id': 'exec-plan-test',
            'origin_type': 'rule', 'rule_origin': plan['rule_origin'],
            'accepted_item_count': 3, 'rejected_item_count': 0,
            'contract_ack': {
                'schema_version': 'execution_handoff_ack_v4', 'contract_version': 'execution_handoff_contract_v4',
                'capability': 'delegated_final_full_cost_gate_recheck_v1', 'plan_id': plan['plan_id'],
                'status': 'accepted', 'origin_type': 'rule', 'rule_origin': plan['rule_origin'],
                'dispatch_material_fingerprint': plan['metadata']['dispatch_material_fingerprint'],
            },
            'item_acks': [{
                'core_item_key': item['core_item_key'], 'status': 'accepted',
                'execution_plan_id': 'exec-plan-test', 'execution_plan_item_id': f'exec-item-{index}',
                'rule_origin': plan['rule_origin'],
            } for index, item in enumerate(plan['items'])],
        }

    def readback(self):
        contract = deepcopy(self.plan)
        for item in contract['items']:
            item['risk_context']['execution_account_route'] = {
                'version': 'kis-account-route-v1', 'configured_route_fingerprint': 'a' * 32,
            }
        return {
            **self.ack(), 'status': 'accepted' if self.activated else 'activation_pending', 'rule_contract': contract,
            'item_states': [{
                'core_item_key': item['core_item_key'], 'execution_plan_item_id': f'exec-item-{index}',
                'status': 'core_activation_pending' if item['side'] == 'buy' and not self.activated else 'accepted',
                'rule_origin': self.plan['rule_origin'], 'close_exit_at': item['close_exit_at'],
                'decision_agent_name': None, 'decision_snapshot_id': None, 'decision_run_id': None,
                'limit_price': item.get('limit_price'), 'trigger_price': item.get('trigger_price'),
                'core_activation_state': {
                    'status': 'activated' if self.activated else 'pending', 'dispatch_material_fingerprint': self.plan['metadata']['dispatch_material_fingerprint'],
                    'authority_generation_fingerprint': 'e' * 64,
                } if item['side'] == 'buy' else None,
                **({'core_quantity_state': {
                    'target_quantity': item['quantity_policy']['target_quantity'], 'revision': 1,
                    'filled_quantity': 0, 'open_quantity': 0, 'submission_uncertain': False,
                    'entry_closed': False, **self.quantity_updates,
                }} if item['side'] == 'buy' else {}),
            } for index, item in enumerate(self.plan['items'])],
        }


def test_lost_create_ack_restarts_with_exact_readback_without_creating_twice(tmp_path):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    result = outbox.prepare(
        setup=prepared, policy=policy(), account=account(),
        authority_generation_fingerprint='e' * 64, clock=lambda: NOW,
    )
    assert result['status'] == 'prepared'
    remote = Remote(outbox, prepared)

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(remote)) as client:
            first = await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            assert first['status'] == 'create_pending'
            assert first['reason'] == 'remote_outcome_unknown'
            restarted = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
            recovered = await restarted.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            assert recovered['status'] == 'accepted'
            assert recovered['reason'] == 'rule_activation_disabled'

    asyncio.run(run())
    assert len(remote.posts) == 1
    assert 'account_reference' not in result and 'target_quantity' not in result


def test_lost_activation_ack_is_recovered_from_exact_entry_activation_state(tmp_path):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)
    remote.enabled, remote.lose_create_ack = True, False

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(remote)) as client:
            assert (await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW))['status'] == 'accepted'
            first = await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            assert first['status'] == 'activation_pending'
            restarted = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
            assert (await restarted.advance(setup=prepared, authority=None, client=client, clock=lambda: NOW))['status'] == 'active'

    asyncio.run(run())
    assert len(remote.posts) == len(remote.activation_posts) == 1
    assert remote.activation_posts[0]['origin_type'] == 'rule'
    assert all(item['decision_snapshot_id'] is None and item['rule_origin'] == remote.plan['rule_origin']
               for item in remote.activation_posts[0]['items'])


def test_both_etfs_share_one_residual_budget_even_before_any_rpc(tmp_path):
    first = setup().setup
    second = setup(market=market(symbol='091160')).setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    assert outbox.prepare(setup=first, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)['status'] == 'prepared'
    result = outbox.prepare(setup=second, policy=policy(), account=account(symbol='091160'), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    assert result['status'] == 'skipped'
    assert result['reason'] == 'insufficient_residual_capacity'


@pytest.mark.parametrize('change', ['no_authority', 'wrong_mode', 'late_capability', 'wrong_account'])
def test_missing_authority_runtime_mismatch_and_network_crossing_cutoff_cannot_post(tmp_path, change):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)
    current = [NOW]

    def handler(request):
        response = remote(request)
        if request.url.path.endswith('/capabilities'):
            if change == 'late_capability':
                current[0] = NOW.replace(minute=40)
            if change in ('wrong_mode', 'wrong_account'):
                body = response.json()
                body['runtime_binding']['mode' if change == 'wrong_mode' else 'account_reference_matches'] = 'live' if change == 'wrong_mode' else False
                return httpx.Response(200, json=body)
        return response

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            return await outbox.advance(setup=prepared, authority=None if change == 'no_authority' else authority(), client=client, clock=lambda: current[0])

    assert asyncio.run(run())['status'] == 'prepared'
    assert remote.posts == []


@pytest.mark.parametrize('remote_stage', ['absent', 'registered'])
def test_live_validation_and_explicit_authority_cannot_mint_unapproved_live_execution(tmp_path, remote_stage):
    from app.overnight_rebound.execution_contract import build_rule_execution_plan
    from app.overnight_rebound.sizing import size_rule_entry

    explicit = policy(execution_mode='live', validation_status='validated_live',
                      validation_evidence_id='synthetic-validation', validation_evidence_hash='f' * 64)
    prepared = setup(policy=explicit).setup
    evidence = account(runtime_mode='live')
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=explicit, account=evidence,
                   authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)
    remote.enabled = True
    if remote_stage == 'registered':
        grant = size_rule_entry(setup=prepared, policy=explicit, account=evidence, now=NOW)
        remote.plan = build_rule_execution_plan(setup=prepared, sizing=grant,
            authority_generation_fingerprint='e' * 64, requested_at=NOW)

    def handler(request):
        response = remote(request)
        if request.url.path.endswith('/capabilities'):
            value = response.json()
            value['runtime_binding']['mode'] = 'live'
            return httpx.Response(200, json=value)
        return response

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            approved = authority().model_copy(update={'mode': 'live', 'policy_hash': explicit.policy_hash})
            return await outbox.advance(setup=prepared, authority=approved, client=client, clock=lambda: NOW)

    assert asyncio.run(run())['reason'] == 'live_execution_not_approved'
    assert remote.posts == remote.activation_posts == []


def test_readback_price_or_origin_mismatch_preserves_the_claim_for_manual_review(tmp_path):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)

    def handler(request):
        response = remote(request)
        if request.method == 'GET' and request.url.path == '/v4/execution-plans' and remote.plan is not None:
            body = response.json()
            body[0]['item_states'][0]['trigger_price'] = '9999'
            return httpx.Response(200, json=body)
        return response

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            return await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)

    assert asyncio.run(run())['status'] == 'manual_review'
    assert len(remote.posts) == 1
    assert outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='f' * 64, clock=lambda: NOW)['status'] == 'manual_review'


@pytest.mark.parametrize('change', ['valid_until', 'calculated_at', 'cash_per_share'])
def test_changed_grant_with_an_echoed_old_fingerprint_cannot_activate(tmp_path, change):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)
    remote.enabled, remote.lose_create_ack = True, False

    def handler(request):
        response = remote(request)
        if request.method == 'GET' and request.url.path == '/v4/execution-plans' and remote.plan is not None:
            body = response.json()
            grant = body[0]['rule_contract']['items'][0]['quantity_policy']
            if change == 'cash_per_share':
                grant['sizing_inputs'][change] = '1'
            else:
                grant[change] = '2026-11-27T15:10:00+09:00' if change == 'valid_until' else '2026-11-27T07:59:00+09:00'
            return httpx.Response(200, json=body)
        return response

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            return await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)

    assert asyncio.run(run())['status'] == 'manual_review'
    assert remote.activation_posts == []


@pytest.mark.parametrize('change', [
    None, 'policy_hash', 'unknown_risk', 'broker', 'monitoring', 'session_copy',
    'metadata_trigger', 'requested_at', 'account_reference',
])
def test_exact_readback_accepts_only_reviewed_exec_normalization(tmp_path, change):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)
    remote.enabled, remote.lose_create_ack, remote.lose_activation_ack = True, False, False

    def handler(request):
        response = remote(request)
        if request.method != 'GET' or request.url.path != '/v4/execution-plans' or remote.plan is None:
            return response
        body = response.json()
        contract = body[0]['rule_contract']
        contract['metadata']['account_reference'] = None
        for item in contract['items']:
            item.update(generic_action=None, raw_action=None, tactical_action=None)
            item.setdefault('handoff_intent', None)
            item['session_policy']['whole_shares_only'] = None
            risk = item['risk_context']
            risk.update({name: deepcopy(item[name]) for name in (
                'broker', 'close_exit_at', 'currency', 'exchange', 'market', 'rule_origin', 'session_policy',
            )})
            if item['side'] == 'buy':
                risk['handoff_intent'] = item['handoff_intent']
                risk['watch_buy_monitoring'] = {
                    'received_intent': 'intraday_watch_buy', 'trigger_source': 'trigger_price',
                    'trigger_price': '10000.0000', 'chase_limit_price': None, 'direction': 'at_or_below',
                    'tactical_action': None, 'exec_monitoring_active': None,
                }
        risk = contract['items'][0]['risk_context']
        if change == 'policy_hash':
            risk['execution_policy_hash'] = '0' * 64
        elif change == 'unknown_risk':
            risk['unreviewed_authority'] = True
        elif change == 'broker':
            risk['broker'] = 'alpaca'
        elif change == 'monitoring':
            risk['watch_buy_monitoring']['direction'] = 'at_or_above'
        elif change == 'session_copy':
            risk['session_policy']['whole_shares_only'] = True
        elif change == 'metadata_trigger':
            contract['metadata']['trigger'] = 'changed-trigger'
        elif change == 'requested_at':
            contract['requested_at'] = contract['metadata']['requested_at'] = NOW.replace(minute=1).isoformat()
        elif change == 'account_reference':
            contract['metadata']['account_reference'] = 'KIS:other-account'
        return httpx.Response(200, json=body)

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            return await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)

    assert asyncio.run(run())['status'] == ('active' if change is None else 'manual_review')
    assert len(remote.activation_posts) == (1 if change is None else 0)


def test_preparation_rejects_account_evidence_from_another_destination(tmp_path):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    with pytest.raises(ValueError, match='immutable destination'):
        outbox.prepare(setup=prepared, policy=policy(), account=account(execution_destination='http://other.test'),
                       authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    assert outbox.status(prepared)['status'] == 'not_prepared'


def test_prepared_route_binding_survives_restart_and_rejects_the_first_changed_route(tmp_path):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    restarted = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    remote = Remote(restarted, prepared)
    paths = []

    def handler(request):
        paths.append(request.url.path)
        response = remote(request)
        body = response.json()
        body['runtime_binding']['configured_route_fingerprint'] = 'b' * 32
        return httpx.Response(200, json=body)

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            return await restarted.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)

    assert asyncio.run(run())['reason'] == 'rule_runtime_binding_changed'
    assert paths == ['/v4/execution-plans/capabilities']
    assert remote.posts == remote.activation_posts == []


def test_legacy_preparation_without_evidence_route_binding_cannot_gain_authority(tmp_path):
    from app.overnight_rebound.execution_outbox import _load, _save

    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    path = outbox._folder(prepared) / 'prepared.json'
    legacy = _load(path)
    legacy.pop('evidence_runtime_binding')
    _save(path, legacy)
    calls = []

    def handler(request):
        calls.append(request)
        raise AssertionError('unbound preparation must not perform RPC')

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            return await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)

    result = asyncio.run(run())
    assert result['status'] == 'manual_review'
    assert result['reason'] == 'preparation_runtime_binding_unproven'
    assert calls == []


@pytest.mark.parametrize('generated_kind', ['timer', 'stop_repair'])
def test_generated_close_timer_does_not_erase_original_intake_activation_proof(tmp_path, generated_kind):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)

    def handler(request):
        response = remote(request)
        if request.method == 'GET' and request.url.path == '/v4/execution-plans' and remote.plan is not None:
            body = response.json()
            generated = deepcopy(body[0]['item_states'][2])
            generated.update(execution_plan_item_id='generated-monitor-item')
            if generated_kind == 'timer':
                generated['core_item_key'] = f'{prepared.origin.setup_id}:close-timer'
            body[0]['item_states'].append(generated)
            return httpx.Response(200, json=body)
        return response

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            remote.activated = True
            return await outbox.advance(setup=prepared, authority=None, client=client, clock=lambda: NOW)

    assert asyncio.run(run())['status'] == 'active'


def test_generated_repair_cannot_replace_the_original_intake_item_proof(tmp_path):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)

    def handler(request):
        response = remote(request)
        if request.method == 'GET' and request.url.path == '/v4/execution-plans' and remote.plan is not None:
            body = response.json()
            body[0]['item_states'][2]['execution_plan_item_id'] = 'generated-repair-item'
            return httpx.Response(200, json=body)
        return response

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            remote.activated = True
            return await outbox.advance(setup=prepared, authority=None, client=client, clock=lambda: NOW)

    assert asyncio.run(run())['status'] == 'manual_review'
    assert len(remote.posts) == 1
    assert remote.activation_posts == []


@pytest.mark.parametrize('update', [{'submission_uncertain': True}, {'filled_quantity': 1}, {'open_quantity': 1}, {'entry_closed': True}])
def test_pending_activation_never_rewrites_a_family_with_unknown_or_existing_exposure(tmp_path, update):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)
    remote.enabled, remote.lose_create_ack, remote.quantity_updates = True, False, update

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(remote)) as client:
            await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            return await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)

    assert asyncio.run(run())['reason'] == 'entry_exposure_or_uncertainty'
    assert remote.activation_posts == []


def test_successful_activation_ack_confirms_exact_origin_and_ids(tmp_path):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)
    remote.enabled, remote.lose_create_ack, remote.lose_activation_ack = True, False, False

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(remote)) as client:
            await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            return await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)

    assert asyncio.run(run())['status'] == 'active'


def test_stale_prepared_grant_can_reconcile_but_cannot_create_new_authority(tmp_path):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(remote)) as client:
            return await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW.replace(minute=6))

    assert asyncio.run(run())['reason'] == 'preparation_evidence_expired'
    assert remote.posts == []


def test_account_session_lock_spans_rpc_and_blocks_another_symbol(tmp_path):
    prepared = setup().setup
    second = setup(market=market(symbol='091160')).setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)
    attempts = []

    def handler(request):
        if request.url.path.endswith('/capabilities'):
            attempts.append(outbox.prepare(setup=second, policy=policy(), account=account(symbol='091160'), authority_generation_fingerprint='e' * 64, clock=lambda: NOW))
        return remote(request)

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(handler)) as client:
            await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)

    asyncio.run(run())
    assert attempts[0]['status'] == 'busy'
    assert attempts[0]['reason'] == 'account_session_busy'


def test_lost_activation_without_remote_commit_retries_the_identical_pending_request(tmp_path):
    prepared = setup().setup
    outbox = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
    outbox.prepare(setup=prepared, policy=policy(), account=account(), authority_generation_fingerprint='e' * 64, clock=lambda: NOW)
    remote = Remote(outbox, prepared)
    remote.enabled, remote.lose_create_ack = True, False

    async def run():
        async with httpx.AsyncClient(base_url='http://exec.test', transport=httpx.MockTransport(remote)) as client:
            await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            await outbox.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)
            remote.activated = False  # First activation never committed remotely.
            restarted = RuleExecutionOutbox(tmp_path, destination_base_url='http://exec.test')
            await restarted.advance(setup=prepared, authority=authority(), client=client, clock=lambda: NOW)

    asyncio.run(run())
    assert len(remote.activation_posts) == 2
    assert remote.activation_posts[0] == remote.activation_posts[1]

