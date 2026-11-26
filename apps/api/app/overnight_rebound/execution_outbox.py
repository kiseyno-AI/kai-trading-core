"""Private one-host rule debt: immutable preparation, exact retries, no orders."""

from collections.abc import Callable
from copy import deepcopy
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import httpx
from pydantic import AwareDatetime, StrictBool

from app.internal.exact_repair_manifest import (
    canonical_json_sha256, checkpoint_file_lock, load_json_mapping, write_checkpoint,
)
from app.market.us_index_input_freeze import _sync_parent_directories
from app.orders.execution_handoff_contract import (
    execution_plan_dispatch_material_fingerprint, validate_execution_plan_dispatch_ack,
)
from app.overnight_rebound.execution_contract import (
    Digest, RULE_CAPABILITY, RuleExecutionPlan, RuleExecutionPolicy, RuleExecutionSetup,
    Text, _Frozen, _aware, build_rule_execution_plan,
)
from app.overnight_rebound.sizing import RuleAccountEvidence, size_rule_entry


class RuleExecutionAuthority(_Frozen):
    """Separate explicit authorization; no authority is inferred from globals."""

    enabled: StrictBool
    mode: Literal['paper', 'live']
    account_reference: Text
    policy_hash: Digest
    authority_generation_fingerprint: Digest
    evidence_id: Text
    valid_until: AwareDatetime


def _destination(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/'):
        raise ValueError('rule destination must be an explicit HTTP origin without credentials')
    return url.rstrip('/')


def _save(path: Path, value: dict):
    body = {key: item for key, item in value.items() if key != 'record_hash'}
    write_checkpoint(path, {**body, 'record_hash': canonical_json_sha256(body)})
    _sync_parent_directories(path.parent)


def _load(path: Path):
    value = load_json_mapping(path, label='private rule outbox')
    body = {key: item for key, item in value.items() if key != 'record_hash'}
    if value.get('record_hash') != canonical_json_sha256(body):
        raise ValueError('rule outbox integrity mismatch')
    return value


def _number(value):
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError('invalid rule price readback')
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError('invalid rule price readback')
    return number


def _ack(payload: dict, plan: dict) -> dict:
    origin = plan['rule_origin']
    if payload.get('origin_type') != 'rule' or payload.get('rule_origin') != origin:
        raise ValueError('rule ACK origin mismatch')
    stripped = deepcopy(payload)
    stripped.pop('origin_type', None)
    stripped.pop('rule_origin', None)
    header = stripped['contract_ack']
    if header.pop('origin_type', None) != 'rule' or header.pop('rule_origin', None) != origin:
        raise ValueError('rule ACK header origin mismatch')
    for item in stripped['item_acks']:
        if item.pop('rule_origin', None) != origin:
            raise ValueError('rule ACK item origin mismatch')
    # Existing validator remains the authority for unchanged v4 identity/hash rules.
    allowed = {'contract_ack', 'item_acks', 'plan_id', 'execution_plan_id', 'accepted_item_count', 'rejected_item_count'}
    return validate_execution_plan_dispatch_ack(
        {key: value for key, value in stripped.items() if key in allowed},
        plan_id=plan['plan_id'], submitted_core_item_keys=[item['core_item_key'] for item in plan['items']],
        expected_dispatch_material_fingerprint=plan['metadata']['dispatch_material_fingerprint'],
    )


def _readback(payload: dict, plan: dict, state: dict) -> tuple[dict, bool, str | None]:
    links = _ack(payload, plan)
    if state.get('execution_plan_id') not in (None, payload['execution_plan_id']):
        raise ValueError('rule readback plan ID changed')
    if state.get('item_ids') not in (None, {key: item['execution_plan_item_id'] for key, item in links.items()}):
        raise ValueError('rule readback item IDs changed')
    raw = payload.get('rule_contract')
    if not isinstance(raw, dict):
        raise ValueError('rule immutable price readback unavailable')
    contract = RuleExecutionPlan.model_validate(raw)
    if contract.plan_id != plan['plan_id'] or contract.rule_origin.model_dump(mode='json') != plan['rule_origin'] or contract.mode != plan['mode'] or contract.metadata.dispatch_material_fingerprint != plan['metadata']['dispatch_material_fingerprint']:
        raise ValueError('rule persisted contract mismatch')
    actual = {item.core_item_key: item for item in contract.items}
    states = payload.get('item_states')
    if not isinstance(states, list) or len(states) < len(links):
        raise ValueError('rule readback item states incomplete')
    states_by_identity = {(item['core_item_key'], item['execution_plan_item_id']): item for item in states}
    if len({item['execution_plan_item_id'] for item in states}) != len(states) or any(
        (key, item['execution_plan_item_id']) not in states_by_identity for key, item in links.items()
    ):
        raise ValueError('rule readback item state keys mismatch')
    for item in states:
        if item.get('rule_origin') != plan['rule_origin'] or any(item.get(name) is not None for name in ('decision_agent_name', 'decision_snapshot_id', 'decision_run_id')):
            raise ValueError('rule generated item identity mismatch')
    activated = False
    entry_block = None
    for item in plan['items']:
        key = item['core_item_key']
        persisted = actual[key]
        observed = states_by_identity[(key, links[key]['execution_plan_item_id'])]
        if observed.get('execution_plan_item_id') != links[key]['execution_plan_item_id'] or observed.get('rule_origin') != plan['rule_origin'] or any(observed.get(name) is not None for name in ('decision_agent_name', 'decision_snapshot_id', 'decision_run_id')):
            raise ValueError('rule readback item identity mismatch')
        if persisted.source_plan_item_id != key or persisted.symbol != item['symbol'] or persisted.side != item['side'] or persisted.intent != item['intent']:
            raise ValueError('rule immutable item identity mismatch')
        if persisted.close_exit_at != datetime.fromisoformat(item['close_exit_at']) or datetime.fromisoformat(observed['close_exit_at']) != persisted.close_exit_at:
            raise ValueError('rule readback close deadline mismatch')
        for name in ('limit_price', 'trigger_price'):
            if _number(getattr(persisted, name)) != _number(item.get(name)) or _number(observed.get(name)) != _number(item.get(name)):
                raise ValueError('rule immutable price mismatch')
        if persisted.risk_context.get('rule_session') != item['risk_context']['rule_session']:
            raise ValueError('rule readback session evidence mismatch')
        route = persisted.risk_context.get('execution_account_route')
        expected_route = state.get('runtime_binding', {}).get('configured_route_fingerprint')
        if not isinstance(route, dict) or route.get('version') != 'kis-account-route-v1' or route.get('configured_route_fingerprint') != expected_route:
            raise ValueError('rule persisted account route mismatch')
        if item['side'] == 'buy':
            if persisted.quantity_policy.get('type') != 'core_sized' or persisted.quantity_policy.get('revision') != item['quantity_policy']['revision'] or persisted.quantity_policy.get('target_quantity') != item['quantity_policy']['target_quantity']:
                raise ValueError('rule readback quantity revision mismatch')
            activation = observed.get('core_activation_state')
            if not isinstance(activation, dict) or activation.get('dispatch_material_fingerprint') != plan['metadata']['dispatch_material_fingerprint'] or activation.get('authority_generation_fingerprint') != item['risk_context']['_core_activation']['authority_generation_fingerprint']:
                raise ValueError('rule readback activation binding unavailable')
            activated = activation.get('status') == 'activated'
            quantities = observed.get('core_quantity_state')
            if not isinstance(quantities, dict) or any(type(quantities.get(name)) is not int or quantities[name] < 0 for name in (
                'target_quantity', 'revision', 'filled_quantity', 'open_quantity',
            )) or quantities['target_quantity'] != item['quantity_policy']['target_quantity'] or quantities['revision'] != item['quantity_policy']['revision'] or any(type(quantities.get(name)) is not bool for name in ('submission_uncertain', 'entry_closed')):
                raise ValueError('rule quantity state/revision unproven')
            if quantities['submission_uncertain'] or quantities['entry_closed'] or quantities['filled_quantity'] or quantities['open_quantity']:
                entry_block = 'entry_exposure_or_uncertainty'
    return links, activated, entry_block


def _activation_request(plan: dict, state: dict) -> dict:
    return {
        'schema_version': 'execution_handoff_activation_v1',
        'contract_version': 'execution_handoff_contract_v4',
        'required_capability': 'core_confirmed_activation_v1', 'origin_type': 'rule',
        'plan_id': plan['plan_id'], 'execution_plan_id': state['execution_plan_id'],
        'dispatch_material_fingerprint': plan['metadata']['dispatch_material_fingerprint'],
        'authority_generation_fingerprint': plan['items'][0]['risk_context']['_core_activation']['authority_generation_fingerprint'],
        'requested_at': plan['requested_at'],
        'items': [{
            'core_item_key': item['core_item_key'],
            'execution_plan_item_id': state['item_ids'][item['core_item_key']],
            'decision_snapshot_id': None, 'rule_origin': plan['rule_origin'],
        } for item in plan['items']],
    }


def _validate_activation_ack(payload: dict, request: dict, plan: dict):
    expected_header = {
        'schema_version': request['schema_version'], 'contract_version': request['contract_version'],
        'capability': request['required_capability'], 'plan_id': request['plan_id'],
        'status': 'activated', 'origin_type': 'rule', 'rule_origin': plan['rule_origin'],
        'dispatch_material_fingerprint': request['dispatch_material_fingerprint'],
        'authority_generation_fingerprint': request['authority_generation_fingerprint'],
    }
    if payload.get('plan_id') != request['plan_id'] or payload.get('execution_plan_id') != request['execution_plan_id'] or payload.get('contract_ack') != expected_header:
        raise ValueError('rule activation ACK header mismatch')
    acks = payload.get('item_acks')
    if not isinstance(acks, list) or len(acks) != len(request['items']):
        raise ValueError('rule activation ACK item count mismatch')
    expected = {item['core_item_key']: {
        **item, 'status': 'activated', 'execution_plan_id': request['execution_plan_id'],
    } for item in request['items']}
    if {item['core_item_key']: item for item in acks} != expected or payload.get('activated_item_count') != len(expected):
        raise ValueError('rule activation ACK item binding mismatch')


class RuleExecutionOutbox:
    """One private account/session ledger shared by both ETF setup claims.

    Keep this directory through restart/deploy. Unknown and filled claims retain
    their full same-session grant; this module never releases or recreates them.
    """

    def __init__(self, state_dir: str | Path, *, destination_base_url: str):
        self.root = Path(state_dir) / 'rule-execution'
        self.destination = _destination(destination_base_url)

    def _account_folder(self, setup: RuleExecutionSetup) -> Path:
        account_key = canonical_json_sha256({
            'account_reference': setup.account_reference, 'target_trade_date': setup.origin.target_trade_date.isoformat(),
            'strategy_lane': 'krx_us_rebound',
        })
        return self.root / account_key

    def _folder(self, setup: RuleExecutionSetup) -> Path:
        return self._account_folder(setup) / setup.origin.setup_id

    def _mkdir(self, setup):
        for folder in (self.root, self._account_folder(setup), self._folder(setup)):
            folder.mkdir(mode=0o700, parents=True, exist_ok=True)
            folder.chmod(0o700)

    def _prepared(self, setup):
        value = _load(self._folder(setup) / 'prepared.json')
        if value.get('schema_version') != 'us_rebound_execution_outbox_v1' or value.get('destination') != self.destination:
            raise ValueError('rule outbox destination/schema mismatch')
        plan = RuleExecutionPlan.model_validate(value['plan'])
        if plan.rule_origin.setup_id != setup.origin.setup_id or value['setup']['account_reference'] != setup.account_reference:
            raise ValueError('rule outbox claim mismatch')
        if execution_plan_dispatch_material_fingerprint(value['plan']) != plan.metadata.dispatch_material_fingerprint:
            raise ValueError('rule outbox dispatch fingerprint mismatch')
        return value

    def status(self, setup: RuleExecutionSetup) -> dict:
        path = self._folder(setup) / 'state.json'
        if not path.exists():
            prepared_exists = (self._folder(setup) / 'prepared.json').exists()
            return {'status': 'prepared' if prepared_exists else 'not_prepared', 'claim_id': setup.origin.setup_id}
        state = _load(path)
        # Deliberately exclude account, quantities, prices, raw ACKs and errors.
        return {key: state[key] for key in ('status', 'reason', 'claim_id') if key in state}

    def prepare(
        self, *, setup: RuleExecutionSetup, policy: RuleExecutionPolicy,
        account: RuleAccountEvidence | None, authority_generation_fingerprint: str,
        clock: Callable[[], datetime],
    ) -> dict:
        self._mkdir(setup)
        with checkpoint_file_lock(self._account_folder(setup) / 'ledger.json') as acquired:
            if not acquired:
                return {'status': 'busy', 'reason': 'account_session_busy', 'claim_id': setup.origin.setup_id}
            folder = self._folder(setup)
            if (folder / 'prepared.json').exists():
                self._prepared(setup)
                return self.status(setup)
            now = _aware(clock())
            if now.date() != setup.origin.target_trade_date or now.time() >= time(8, 40):
                return {'status': 'skipped', 'reason': 'new_authority_window_expired', 'claim_id': setup.origin.setup_id}
            commitments = Decimal(0)
            risks = Decimal(0)
            for path in self._account_folder(setup).glob('*/prepared.json'):
                existing = _load(path)
                grant = existing['sizing']
                commitments += grant['target_quantity'] * Decimal(grant['cash_per_share'])
                risks += grant['target_quantity'] * Decimal(grant['risk_per_share'])
            if account is not None:
                values = account.model_dump(mode='python')
                for name, amount in (
                    ('other_rule_committed_notional_krw', commitments), ('other_rule_committed_risk_krw', risks),
                    ('strategy_committed_notional_krw', commitments), ('liquidity_committed_notional_krw', commitments),
                ):
                    if values[name] is not None:
                        values[name] += amount
                for name, amount in (('remaining_account_buy_notional_krw', commitments), ('remaining_account_risk_krw', risks)):
                    if values[name] is not None:
                        values[name] = max(values[name] - amount, Decimal(0))
                account = RuleAccountEvidence.model_validate(values)
            sizing = size_rule_entry(setup=setup, policy=policy, account=account, now=now)
            if sizing.reasons:
                return {'status': 'skipped', 'reason': sizing.reasons[0], 'claim_id': setup.origin.setup_id}
            plan = build_rule_execution_plan(
                setup=setup, sizing=sizing, authority_generation_fingerprint=authority_generation_fingerprint, requested_at=now,
            )
            prepared = {
                'schema_version': 'us_rebound_execution_outbox_v1', 'destination': self.destination,
                'setup': setup.model_dump(mode='json'), 'policy': policy.model_dump(mode='json'),
                'plan': plan, 'sizing': sizing.model_dump(mode='json'),
                'evidence_valid_until': (min(
                    setup.evidence_observed_at, account.observed_at, account.balance_observed_at,
                    account.order_possible_requested_at, account.order_possible_observed_at,
                    account.broker_orders_observed_at, account.baseline_observed_at,
                ) + timedelta(seconds=policy.max_evidence_age_seconds)).isoformat(),
            }
            _save(folder / 'prepared.json', prepared)
            _save(folder / 'state.json', {'status': 'prepared', 'claim_id': setup.origin.setup_id})
            return self.status(setup)

    async def advance(
        self, *, setup: RuleExecutionSetup, authority: RuleExecutionAuthority | None,
        client: httpx.AsyncClient, clock: Callable[[], datetime],
    ) -> dict:
        if _destination(str(client.base_url)) != self.destination:
            raise ValueError('rule HTTP client differs from the immutable destination')
        folder = self._folder(setup)
        if not (folder / 'prepared.json').exists():
            return self.status(setup)
        with checkpoint_file_lock(self._account_folder(setup) / 'ledger.json') as acquired:
            if not acquired:
                return {'status': 'busy', 'reason': 'account_session_busy', 'claim_id': setup.origin.setup_id}
            prepared = self._prepared(setup)
            plan = prepared['plan']
            frozen_setup = RuleExecutionSetup.model_validate(prepared['setup'])
            policy = RuleExecutionPolicy.model_validate(prepared['policy'])
            state_path = folder / 'state.json'
            state = _load(state_path) if state_path.exists() else {'status': 'prepared', 'claim_id': setup.origin.setup_id}

            def report(reason=None):
                if reason:
                    state['reason'] = reason
                else:
                    state.pop('reason', None)
                _save(state_path, state)
                return self.status(setup)

            def authorized():
                now = _aware(clock())
                if now.date() != frozen_setup.origin.target_trade_date or now.time() >= time(8, 40) or now >= frozen_setup.entry_expires_at:
                    return 'new_authority_window_expired'
                if authority is None or not authority.enabled:
                    return 'explicit_rule_authority_unavailable'
                generation = plan['items'][0]['risk_context']['_core_activation']['authority_generation_fingerprint']
                if authority.mode != plan['mode'] or authority.account_reference != frozen_setup.account_reference or authority.policy_hash != frozen_setup.policy_hash or authority.authority_generation_fingerprint != generation:
                    return 'rule_authority_binding_mismatch'
                if now >= authority.valid_until:
                    return 'rule_authority_expired'
                if policy.execution_mode == 'live' and policy.validation_status != 'validated_live':
                    return 'policy_not_validated_for_live'
                if now < datetime.fromisoformat(plan['requested_at']) or now > datetime.fromisoformat(prepared['evidence_valid_until']):
                    return 'preparation_evidence_expired'
                return None

            if state['status'] == 'manual_review':
                return self.status(setup)
            try:
                capabilities_response = await client.get('/v4/execution-plans/capabilities', headers={'X-Core-Account-Reference': frozen_setup.account_reference}, follow_redirects=False)
                capabilities_response.raise_for_status()
                capabilities = capabilities_response.json()
                binding = capabilities.get('runtime_binding')
                rule = capabilities.get('rule_origin', {})
                if RULE_CAPABILITY not in capabilities.get('capabilities', []) or rule.get('supported') is not True or rule.get('strategy_id') != 'krx_us_rebound' or rule.get('core_sized_only') is not True or rule.get('explicit_close_exit_at') is not True:
                    return report('rule_capability_unavailable')
                route = binding.get('configured_route_fingerprint') if isinstance(binding, dict) else None
                if not isinstance(binding, dict) or binding.get('mode') != plan['mode'] or binding.get('provider') != 'kis' or binding.get('account_reference_matches') is not True or not isinstance(route, str) or len(route) != 32 or any(c not in '0123456789abcdef' for c in route):
                    return report('rule_runtime_binding_mismatch')
                if state.get('runtime_binding') not in (None, binding):
                    return report('rule_runtime_binding_changed')
                state['runtime_binding'] = binding
                report()
                response = await client.get('/v4/execution-plans', params={'external_plan_id': plan['plan_id']}, follow_redirects=False)
                response.raise_for_status()
                rows = response.json()
                if not isinstance(rows, list) or len(rows) > 1:
                    raise ValueError('rule readback must identify at most one exact plan')
                if rows:
                    links, activated, entry_block = _readback(rows[0], plan, state)
                    state.update(execution_plan_id=rows[0]['execution_plan_id'], item_ids={key: item['execution_plan_item_id'] for key, item in links.items()})
                    state['status'] = 'active' if activated else 'accepted'
                    report()
                    if activated:
                        return self.status(setup)
                    if entry_block:
                        return report(entry_block)
                elif state['status'] in ('accepted', 'activation_pending', 'active'):
                    return report('remote_authority_unresolved')
                reason = authorized()
                if reason:
                    return report(reason)
                if not rows:
                    state['status'] = 'create_pending'
                    report()
                    reason = authorized()
                    if reason:
                        return report(reason)
                    response = await client.post('/v4/execution-plans', json=plan, follow_redirects=False)
                    response.raise_for_status()
                    links = _ack(response.json(), plan)
                    state.update(execution_plan_id=response.json()['execution_plan_id'], item_ids={key: item['execution_plan_item_id'] for key, item in links.items()}, status='accepted')
                    return report('awaiting_exact_readback')
                if rule.get('enabled') is not True:
                    return report('rule_activation_disabled')
                activation = _activation_request(plan, state)
                if state.get('activation_request') not in (None, activation):
                    raise ValueError('pending rule activation request changed')
                state.update(status='activation_pending', activation_request=activation)
                report()
                reason = authorized()
                if reason:
                    return report(reason)
                response = await client.post('/v4/execution-plans/activations', json=state['activation_request'], follow_redirects=False)
                response.raise_for_status()
                _validate_activation_ack(response.json(), state['activation_request'], plan)
                state['status'] = 'active'
                return report()
            except httpx.HTTPError:
                return report('remote_outcome_unknown')
            except (ValueError, KeyError, TypeError, ArithmeticError):
                state['status'] = 'manual_review'
                return report('rule_contract_readback_mismatch')
