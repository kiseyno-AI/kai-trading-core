"""Opt-in independent rule handoff; observation never enters this path."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, time, timedelta
import json
import os
from pathlib import Path
import re
import stat
from time import monotonic
from uuid import uuid4

import httpx

from app.internal.exact_repair_manifest import canonical_json_sha256, checkpoint_file_lock, load_json_mapping, write_checkpoint
from app.market.us_index_input_freeze import _sync_parent_directories, load_frozen_us_index_inputs
from app.market.us_index_preopen import resolve_us_index_preopen_session
from app.overnight_rebound.evidence import RuleEvidenceAdapter, resolve_rule_session
from app.overnight_rebound.execution_contract import (
    ETF_NAMES, KST, RuleExecutionPolicy, RuleExecutionSetup, _aware, build_rule_execution_setup,
)
from app.overnight_rebound.execution_outbox import RuleExecutionAuthority, RuleExecutionOutbox, _destination


EVIDENCE_SLOTS = (time(8, 20), time(8, 30))
MAX_BATCH_SECONDS = 90
MAX_PAGES = 2


@dataclass(frozen=True)
class RuleOperationConfig:
    enabled: bool | str = False
    account_reference: str | None = None
    policy_file: str | None = None
    destination_base_url: str | None = None
    bearer_token: str | None = None
    authority_id: str | None = None
    authority_until: str | None = None

    @classmethod
    def from_settings(cls, settings):
        return cls(**{name: getattr(settings, f'us_rebound_execution_{name}') for name in cls.__dataclass_fields__})


def _folder(state_dir, day, environment):
    if environment not in ('real', 'mock'):
        raise ValueError('unsupported rule evidence environment')
    return Path(state_dir) / 'rebound' / f'{environment}-{day.isoformat()}'


def _write(path, value):
    body = {key: item for key, item in value.items() if key != 'record_hash'}
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    _sync_parent_directories(path.parent)
    write_checkpoint(path, {**body, 'record_hash': canonical_json_sha256(body)})


def _read(path):
    value = load_json_mapping(path, label='US rebound execution state')
    body = {key: item for key, item in value.items() if key != 'record_hash'}
    if value.get('record_hash') != canonical_json_sha256(body):
        raise ValueError('US rebound execution state integrity mismatch')
    return value


def _public(value):
    result = {key: value[key] for key in (
        'strategy_id', 'target_trade_date', 'source_environment', 'mode', 'status', 'reason', 'checked_at',
    ) if key in value}
    result['items'] = [{key: item[key] for key in ('symbol', 'status', 'reason', 'last_confirmed_at') if key in item}
                       for item in value.get('items', []) if item.get('symbol') in ETF_NAMES]
    return result


def load_us_rebound_execution_status(*, target_trade_date, state_dir, source_environment='real'):
    path = _folder(state_dir, target_trade_date, source_environment) / 'execution-status.json'
    if not path.exists():
        return None
    value = _read(path)
    if value.get('strategy_id') != 'krx_us_rebound' or value.get('target_trade_date') != target_trade_date.isoformat() or value.get('source_environment') != source_environment:
        raise ValueError('US rebound execution status scope mismatch')
    if value.get('mode') not in ('paper', 'live') or not isinstance(value.get('items'), list) or any(not isinstance(item, dict) for item in value['items']):
        raise ValueError('US rebound execution status is invalid')
    return _public(value)


def _policy(path, day, mode):
    if not path:
        return None
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(descriptor, 'r', encoding='utf-8') as handle:
        metadata = os.fstat(handle.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077 or metadata.st_uid != os.getuid() or metadata.st_size > 2 * 1024 * 1024:
            raise ValueError('execution policy must be a private owned JSON file')
        def unique(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError('duplicate policy field')
                value[key] = item
            return value
        artifact = json.load(handle, object_pairs_hook=unique)
    if not isinstance(artifact, dict) or set(artifact) != {'schema_version', 'entry_expires_at_kst', 'policy'} or artifact['schema_version'] != 'us_rebound_execution_policy_v1':
        raise ValueError('unsupported execution policy artifact')
    expiry = artifact['entry_expires_at_kst']
    if not isinstance(expiry, str) or re.fullmatch(r'[0-9]{2}:[0-9]{2}:[0-9]{2}', expiry) is None:
        raise ValueError('explicit KST entry expiry time is required')
    if not isinstance(artifact['policy'], dict) or 'entry_expires_at' in artifact['policy']:
        raise ValueError('entry expiry must bind exactly once to the target session')
    policy = RuleExecutionPolicy.model_validate({
        **artifact['policy'], 'entry_expires_at': datetime.combine(day, time.fromisoformat(expiry), tzinfo=KST),
    })
    if policy.execution_mode != mode or mode == 'live' and policy.validation_status != 'validated_live':
        raise ValueError('execution policy mode/validation mismatch')
    return policy, canonical_json_sha256(artifact)


async def run_us_rebound_operation(
    *, mode, config: RuleOperationConfig, evaluation, state_dir, clock,
    seal_loader=None, adapter_factory=None, client=None, account_target_reader=None,
):
    if mode not in ('paper', 'live'):
        return {'status': 'disabled', 'reason': 'observation_only', 'mode': mode}
    if config.enabled is False or str(config.enabled).strip().lower() in ('false', '0', ''):
        return {'status': 'disabled', 'reason': 'execution_disabled', 'mode': mode}
    if mode == 'live':
        return {'status': 'unavailable', 'reason': 'live_execution_not_approved', 'mode': mode}
    now = _aware(clock())
    environment = evaluation.get('source_environment', 'real')
    folder = _folder(state_dir, now.date(), environment)
    public_path, private_path = folder / 'execution-status.json', folder / 'execution-operation.json'

    def report(status, reason=None, items=()):
        value = {'strategy_id': 'krx_us_rebound', 'target_trade_date': now.date().isoformat(),
                 'source_environment': environment, 'mode': mode, 'status': status,
                 'checked_at': _aware(clock()).isoformat(), 'items': list(items),
                 **({'reason': reason} if reason else {})}
        with checkpoint_file_lock(public_path) as acquired:
            if not acquired:
                return _public(value if value['items'] else {**value, 'status': 'busy', 'reason': 'execution_status_busy'})
            try:
                previous = load_us_rebound_execution_status(
                    target_trade_date=now.date(), state_dir=state_dir, source_environment=environment,
                )
            except (OSError, ValueError):
                return _public({**value, 'status': 'unavailable', 'reason': 'execution_status_unavailable', 'items': []})
            if not value['items'] and previous and any(item.get('status') in ('accepted', 'activation_pending', 'active') for item in previous['items']):
                value['items'] = previous['items']
                value['mode'] = previous['mode']
            _write(public_path, _public(value))
            return _public(value)

    if evaluation.get('status') != 'WATCH':
        return report('skipped', 'rule_not_watch')
    if evaluation.get('target_trade_date') != now.date().isoformat():
        return report('skipped', 'evaluation_session_expired')
    body = {key: value for key, value in evaluation.items() if key != 'evaluation_id'}
    if evaluation.get('evaluation_id') != canonical_json_sha256(body) or evaluation.get('order_authority') is not False or evaluation.get('schema_version') != 'us_rebound_evaluation_v1' or evaluation.get('strategy_id') != 'krx_us_rebound' or any(
        not isinstance(evaluation.get(key), str) or re.fullmatch(r'[0-9a-f]{64}', evaluation[key]) is None
        for key in ('evaluation_id', 'input_manifest_id', 'feature_id', 'config_hash')
    ):
        return report('unavailable', 'evaluation_binding_unproven')
    try:
        completed = _aware(datetime.fromisoformat(evaluation['completed_at']))
        if completed > now or completed.date() != now.date() or completed.time() >= time(8, 40):
            return report('unavailable', 'evaluation_binding_unproven')
    except (KeyError, TypeError, ValueError):
        return report('unavailable', 'evaluation_binding_unproven')
    if config.enabled is not True and str(config.enabled).strip().lower() not in ('true', '1'):
        return report('unavailable', 'invalid_execution_flag')
    if not all((config.account_reference, config.policy_file, config.destination_base_url,
                config.bearer_token, config.authority_id, config.authority_until)):
        return report('unavailable', 'execution_configuration_unavailable')
    try:
        destination = _destination(config.destination_base_url)
        operator_until = _aware(datetime.fromisoformat(config.authority_until))
        loaded = _policy(config.policy_file, now.date(), mode)
        if loaded is None:
            return report('unavailable', 'execution_policy_unavailable')
        policy, template_hash = loaded
        from app.overnight_rebound.evidence import _route_from_original_configuration
        _route_from_original_configuration(account_target_reader, config.account_reference)
    except (OSError, ValueError, TypeError):
        return report('unavailable', 'execution_policy_or_scope_unavailable')
    if mode == 'paper' and adapter_factory is None:
        return report('unavailable', 'dedicated_paper_client_unavailable')
    if now.time() < EVIDENCE_SLOTS[0] and not private_path.exists():
        return report('not_due', 'awaiting_evidence_slot')
    if (now.time() >= time(8, 40) or now >= operator_until) and not private_path.exists():
        return report('skipped', 'new_authority_window_expired')
    _, session_reason = resolve_us_index_preopen_session(now.date())
    if session_reason:
        return report('skipped', session_reason)
    session_result = resolve_rule_session(now.date())
    if session_result.value is None:
        return report('unavailable', session_result.reasons[0])
    session = session_result.value
    outbox = RuleExecutionOutbox(state_dir, destination_base_url=destination)
    generation = canonical_json_sha256({
        'schema_version': 'us_rebound_watch_authority_v1', 'target_trade_date': now.date().isoformat(),
        'mode': mode, 'account_reference': config.account_reference, 'template_hash': template_hash,
        'policy_hash': policy.policy_hash, 'evaluation_id': evaluation['evaluation_id'],
        'authority_id': config.authority_id, 'operator_until': operator_until.isoformat(),
    })
    scope = {'target_trade_date': now.date().isoformat(), 'source_environment': environment, 'mode': mode,
             'account_reference': config.account_reference, 'destination': destination,
             'policy_hash': policy.policy_hash, 'evaluation_id': evaluation['evaluation_id'],
             'authority_generation_fingerprint': generation}
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    folder.chmod(0o700)
    with checkpoint_file_lock(private_path) as acquired:
        if not acquired:
            return report('busy', 'operation_busy')
        try:
            if not private_path.exists():
                account_key = canonical_json_sha256({'account_reference': config.account_reference,
                    'target_trade_date': now.date().isoformat(), 'strategy_lane': 'krx_us_rebound'})
                if any((outbox.root / account_key).glob('*/prepared.json')):
                    return report('unavailable', 'durable_outbox_without_operation_state')
            state = _read(private_path) if private_path.exists() else {**scope, 'run_id': str(uuid4()), 'attempts': {}, 'setups': {}}
            if any(state.get(key) != value for key, value in scope.items()):
                return report('unavailable', 'persisted_execution_scope_changed')
            _write(private_path, state)
            started = monotonic()
            def remaining():
                value = MAX_BATCH_SECONDS - (monotonic() - started)
                if value <= 0:
                    raise TimeoutError()
                return value

            async def work(http):
                if _destination(str(http.base_url)) != destination or http.headers.get('Authorization') != f'Bearer {config.bearer_token}':
                    return report('unavailable', 'execution_transport_binding_mismatch')
                current = _aware(clock())
                entry_window_valid = (session.session_open_at < policy.entry_expires_at
                    < session.session_close_at - timedelta(seconds=policy.close_buffer_seconds)
                    and current < policy.entry_expires_at)
                slot = max((item for item in EVIDENCE_SLOTS if item <= current.time()), default=None)
                new_reads = entry_window_valid and slot is not None and current.time() < time(8, 40) and current < operator_until and slot.isoformat() not in state['attempts']
                needs_evidence = any(symbol not in state['setups'] or outbox.status(RuleExecutionSetup.model_validate(state['setups'][symbol]['setup']))['status'] == 'not_prepared' for symbol in ETF_NAMES)
                if new_reads and needs_evidence:
                    state['attempts'][slot.isoformat()] = {'started_at': current.isoformat(),
                        'attempt_id': canonical_json_sha256({**scope, 'slot_kst': slot.isoformat()})}
                    _write(private_path, state)
                    frozen = (seal_loader or load_frozen_us_index_inputs)(
                        target_trade_date=current.date(), state_dir=state_dir, source_environment=environment, include_history=True,
                    )
                    if frozen is None or frozen.target_trade_date != current.date() or frozen.source_environment != environment or frozen.input_kind != 'history_22' or frozen.manifest_id != evaluation.get('input_manifest_id'):
                        return report('unavailable', 'current_history_seal_unavailable')
                    if adapter_factory is None:
                        from app.brokers.kis.provider import kis_real_client
                        adapter = RuleEvidenceAdapter(kis_client=kis_real_client, exec_client=http)
                    else:
                        adapter = adapter_factory(exec_client=http)
                else:
                    new_reads = False
                    adapter = None
                outcomes = []
                for symbol in ETF_NAMES:
                    cached = state['setups'].get(symbol)
                    setup = RuleExecutionSetup.model_validate(cached['setup']) if cached else None
                    old_status = outbox.status(setup)['status'] if setup else 'not_prepared'
                    if old_status == 'not_prepared' and new_reads:
                        market = await adapter.market(symbol=symbol, session=session, mode=mode, clock=clock, max_pages=MAX_PAGES, budget_seconds=remaining())
                        if market.value is None:
                            outcomes.append({'symbol': symbol, 'status': market.status, 'reason': market.reasons[0]})
                            continue
                        built = build_rule_execution_setup(policy=policy, evaluation=evaluation, market=market.value,
                            session=session, account_reference=config.account_reference, run_id=state['run_id'], now=_aware(clock()))
                        if built.setup is None:
                            outcomes.append({'symbol': symbol, 'status': 'skipped', 'reason': built.reasons[0]})
                            continue
                        setup = built.setup
                        state['setups'][symbol] = {'setup': setup.model_dump(mode='json'), 'readback_only': False}
                        _write(private_path, state)
                        evidence = await adapter.account(setup=setup, policy=policy, clock=clock, max_pages=MAX_PAGES, budget_seconds=remaining())
                        if evidence.value is None or evidence.status != 'available':
                            outcomes.append({'symbol': symbol, 'status': evidence.status, 'reason': evidence.reasons[0] if evidence.reasons else 'account_evidence_unavailable'})
                            continue
                        state['setups'][symbol]['evidence_valid_until'] = min(
                            operator_until, datetime.combine(current.date(), time(8, 40), tzinfo=KST),
                            *(stamp + timedelta(seconds=policy.max_evidence_age_seconds) for stamp in (
                                setup.evidence_observed_at, evidence.value.observed_at, evidence.value.balance_observed_at,
                                evidence.value.order_possible_requested_at, evidence.value.order_possible_observed_at,
                                evidence.value.broker_orders_observed_at, evidence.value.baseline_observed_at,
                            )),
                        ).isoformat()
                        _write(private_path, state)
                        prepared = outbox.prepare(setup=setup, policy=policy, account=evidence.value,
                            authority_generation_fingerprint=generation, clock=clock)
                        if prepared['status'] in ('skipped', 'busy'):
                            outcomes.append({'symbol': symbol, **{key: prepared[key] for key in ('status', 'reason') if key in prepared}})
                            continue
                    elif setup is None or old_status == 'not_prepared':
                        outcomes.append({'symbol': symbol, 'status': 'not_due', 'reason':
                            'evidence_attempt_not_due' if entry_window_valid else 'policy_entry_window_invalid'})
                        continue
                    item = state['setups'][symbol]
                    status = outbox.status(setup)
                    if status['status'] == 'active':
                        outcomes.append({'symbol': symbol, 'status': 'active', **({'last_confirmed_at': item['last_confirmed_at']} if 'last_confirmed_at' in item else {})})
                        continue
                    if status['status'] in ('create_pending', 'activation_pending'):
                        item['readback_only'] = True
                        _write(private_path, state)
                    authority = None if item.get('readback_only') or not entry_window_valid else RuleExecutionAuthority(
                        enabled=True, mode=mode, account_reference=config.account_reference, policy_hash=policy.policy_hash,
                        authority_generation_fingerprint=generation, evidence_id=config.authority_id,
                        valid_until=datetime.fromisoformat(item['evidence_valid_until']),
                    )
                    remaining()
                    status = await outbox.advance(setup=setup, authority=authority, client=http, clock=clock)
                    if status['status'] == 'accepted' and status.get('reason') == 'awaiting_exact_readback' and authority is not None:
                        remaining()
                        status = await outbox.advance(setup=setup, authority=authority, client=http, clock=clock)
                    if status['status'] in ('create_pending', 'activation_pending'):
                        item['readback_only'] = True
                    if status['status'] == 'active':
                        item['last_confirmed_at'] = _aware(clock()).isoformat()
                    _write(private_path, state)
                    outcomes.append({'symbol': symbol, **{key: status[key] for key in ('status', 'reason') if key in status},
                                     **({'last_confirmed_at': item['last_confirmed_at']} if 'last_confirmed_at' in item else {})})
                if any(item['status'] == 'active' for item in outcomes):
                    return report('active', items=outcomes)
                if any(item['status'] in ('create_pending', 'activation_pending', 'accepted') for item in outcomes):
                    return report('pending', items=outcomes)
                if any(item['status'] == 'failed' for item in outcomes):
                    return report('failed', 'evidence_failed', outcomes)
                return report('unavailable', 'no_executable_setup' if entry_window_valid else 'policy_entry_window_invalid', outcomes)

            async with asyncio.timeout(MAX_BATCH_SECONDS):
                if client is not None:
                    return await work(client)
                async with httpx.AsyncClient(base_url=destination, headers={'Authorization': f'Bearer {config.bearer_token}'}) as http:
                    return await work(http)
        except TimeoutError:
            return report('pending', 'operation_budget_expired')
        except Exception as exc:
            return report('failed', f'operation_{type(exc).__name__.lower()}')
