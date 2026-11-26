"""Pure, explicit rule preparation; no provider, agent, or broker calls."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
import re
from typing import TYPE_CHECKING, Annotated, Literal
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StrictBool, StrictInt, model_validator

from app.internal.exact_repair_manifest import canonical_json_sha256
from app.orders.execution_handoff_contract import (
    EXECUTION_HANDOFF_CAPABILITY, EXECUTION_HANDOFF_CONTRACT_VERSION,
    EXECUTION_HANDOFF_SOURCE, EXECUTION_PLAN_DISPATCH_MAX_BYTES,
    EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION, ExecutionPlanDispatchItemPayload,
    ExecutionPlanDispatchMetadataPayload, ExecutionPlanDispatchPayload,
    execution_plan_dispatch_material_fingerprint,
)

if TYPE_CHECKING:
    from app.overnight_rebound.sizing import RuleSizingResult


KST = ZoneInfo('Asia/Seoul')
STRATEGY_ID = 'krx_us_rebound'
RULE_CAPABILITY = 'rule_origin_krx_us_rebound_v1'
ETF_NAMES = {'069500': 'KODEX 200', '091160': 'KODEX Semiconductors'}
Positive = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
Nonnegative = Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
Digest = Annotated[str, Field(pattern=r'^[0-9a-f]{64}$')]
Text = Annotated[str, Field(min_length=1, pattern=r'^\S(?:.*\S)?$')]


class _Frozen(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)


class RuleExecutionPolicy(_Frozen):
    """All execution/risk parameters are required, including for paper plans."""

    execution_mode: Literal['paper', 'live']
    validation_status: Literal['paper_only', 'validated_live']
    validation_evidence_id: Text | None
    validation_evidence_hash: Digest | None
    entry_gap_pct: Nonnegative
    chase_gap_pct: Nonnegative
    stop_loss_pct: Annotated[Decimal, Field(gt=0, lt=100, allow_inf_nan=False)]
    target_return_pct: Positive
    round_trip_cost_bps: Nonnegative
    slippage_bps: Nonnegative
    volatility_multiplier: Positive
    loss_fraction: Annotated[Decimal, Field(gt=0, le=Decimal('0.015'), allow_inf_nan=False)]
    max_symbol_fraction: Annotated[Decimal, Field(gt=0, le=Decimal('0.30'), allow_inf_nan=False)]
    max_trade_notional_krw: Positive
    max_strategy_notional_krw: Positive
    max_liquidity_fraction: Annotated[Decimal, Field(gt=0, le=1, allow_inf_nan=False)]
    max_evidence_age_seconds: StrictInt = Field(gt=0)
    entry_expires_at: AwareDatetime
    close_buffer_seconds: StrictInt = Field(gt=0, le=3600)

    @model_validator(mode='after')
    def validate_policy(self):
        if self.entry_gap_pct > self.chase_gap_pct:
            raise ValueError('entry gap must not exceed chase gap')
        if self.validation_status == 'validated_live' and (
            self.validation_evidence_id is None or self.validation_evidence_hash is None
        ):
            raise ValueError('live validation requires explicit evidence identity and hash')
        if self.validation_status == 'paper_only' and (
            self.validation_evidence_id is not None or self.validation_evidence_hash is not None
        ):
            raise ValueError('paper-only policy cannot claim live validation evidence')
        return self

    @property
    def policy_hash(self):
        return canonical_json_sha256(self.model_dump(mode='json'))


class RuleMarketEvidence(_Frozen):
    symbol: Literal['069500', '091160']
    prior_close_date: date
    prior_close: Positive
    volatility_pct: Positive
    daily_value_krw: Positive
    observed_at: AwareDatetime
    source: Text
    evidence_id: Digest
    source_environment: Literal['real', 'mock']
    status: Literal['verified']


class RuleSessionEvidence(_Frozen):
    target_trade_date: date
    previous_trade_date: date
    session_open_at: AwareDatetime
    session_close_at: AwareDatetime
    calendar_status: Literal['authoritative']
    calendar_source: Text = Field(max_length=256)
    special_hours: StrictBool

    @model_validator(mode='after')
    def validate_session(self):
        if not self.previous_trade_date < self.target_trade_date:
            raise ValueError('prior KRX session must precede target session')
        if not self.session_open_at < self.session_close_at or any(
            instant.astimezone(KST).date() != self.target_trade_date
            for instant in (self.session_open_at, self.session_close_at)
        ):
            raise ValueError('invalid authoritative KRX session bounds')
        return self


class RuleOrigin(_Frozen):
    schema_version: Literal['rule_origin.v1']
    strategy_id: Literal['krx_us_rebound']
    strategy_lane: Literal['krx_us_rebound']
    rule_version: Text = Field(max_length=128)
    run_id: Text = Field(max_length=128)
    evaluation_id: Text = Field(max_length=128)
    input_manifest_id: Text = Field(max_length=128)
    config_hash: Digest
    input_hash: Digest
    target_trade_date: date
    setup_id: Digest


class RuleExecutionSetup(_Frozen):
    account_reference: Text
    mode: Literal['paper', 'live']
    symbol: Literal['069500', '091160']
    source_market_date: date
    origin: RuleOrigin
    policy_hash: Digest
    trigger_price: Positive
    chase_limit_price: Positive
    target_price: Positive
    stop_price: Positive
    volatility_pct: Positive
    daily_value_krw: Positive
    evidence_observed_at: AwareDatetime
    entry_expires_at: AwareDatetime
    close_exit_at: AwareDatetime
    session: RuleSessionEvidence


@dataclass(frozen=True)
class RuleSetupResult:
    setup: RuleExecutionSetup | None
    reasons: tuple[str, ...]


def _aware(now: datetime) -> datetime:
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('clock must return an aware datetime')
    return now.astimezone(KST)


def _tick_price(price: Decimal, *, up: bool) -> Decimal:
    tick = Decimal(1) if price < 2000 else Decimal(5)
    result = (price / tick).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR) * tick
    # A rounded crossing must obey the tick at the resulting price too.
    if result >= 2000:
        result = (result / 5).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR) * 5
    return result


def build_rule_execution_setup(
    *, policy: RuleExecutionPolicy, evaluation: dict, market: RuleMarketEvidence | None,
    session: RuleSessionEvidence | None, account_reference: str, run_id: str, now: datetime,
) -> RuleSetupResult:
    """Build one ETF family only from verified evidence and an explicit policy.

    The evaluator's WATCH and order_authority=false remain unchanged. Execution
    authority comes from separately validated policy and the outbox caller.
    """
    now = _aware(now)
    if not isinstance(policy, RuleExecutionPolicy):
        raise ValueError('explicit validated policy is required')
    reasons = []
    if market is None:
        reasons.append('market_evidence_unavailable')
    if session is None:
        reasons.append('calendar_evidence_unavailable')
    body = {key: value for key, value in evaluation.items() if key != 'evaluation_id'}
    if (
        evaluation.get('evaluation_id') != canonical_json_sha256(body)
        or evaluation.get('schema_version') != 'us_rebound_evaluation_v1'
        or evaluation.get('strategy_id') != STRATEGY_ID
        or evaluation.get('order_authority') is not False
    ):
        raise ValueError('immutable rule evaluation binding mismatch')
    try:
        completed = _aware(datetime.fromisoformat(evaluation['completed_at']))
        if completed > now or completed.date().isoformat() != evaluation['target_trade_date'] or completed.time() >= time(8, 40) or any(
            not isinstance(evaluation.get(key), str) or re.fullmatch(r'[0-9a-f]{64}', evaluation[key]) is None
            for key in ('evaluation_id', 'input_manifest_id', 'feature_id', 'config_hash')
        ):
            raise ValueError('invalid rule evaluation time/replay identity')
    except (KeyError, TypeError):
        raise ValueError('invalid rule evaluation time/replay identity') from None
    if evaluation.get('status') != 'WATCH':
        reasons.append('rule_not_watch')
    if policy.execution_mode == 'live' and policy.validation_status != 'validated_live':
        reasons.append('policy_not_validated_for_live')
    if market is None or session is None:
        return RuleSetupResult(None, tuple(reasons))
    if evaluation.get('target_trade_date') != session.target_trade_date.isoformat():
        raise ValueError('evaluation session mismatch')
    if market.prior_close_date != session.previous_trade_date:
        reasons.append('prior_krx_close_unproven')
    if evaluation.get('source_environment') != market.source_environment:
        reasons.append('input_environment_mismatch')
    if policy.execution_mode == 'live' and market.source_environment != 'real':
        reasons.append('live_requires_real_inputs')
    if not timedelta(0) <= now - market.observed_at <= timedelta(seconds=policy.max_evidence_age_seconds):
        reasons.append('market_evidence_stale_or_future')
    if session.special_hours or (
        session.session_open_at.astimezone(KST).time() != time(9)
        or session.session_close_at.astimezone(KST).time() != time(15, 30)
    ):
        reasons.append('special_session_unsupported')
    close_exit = session.session_close_at - timedelta(seconds=policy.close_buffer_seconds)
    if not session.session_open_at < policy.entry_expires_at < close_exit:
        reasons.append('invalid_entry_or_close_deadline')
    if now.date() != session.target_trade_date or now.time() >= time(8, 40):
        reasons.append('new_setup_window_expired')
    trigger = _tick_price(market.prior_close * (1 + policy.entry_gap_pct / 100), up=True)
    chase = _tick_price(market.prior_close * (1 + policy.chase_gap_pct / 100), up=False)
    target = _tick_price(market.prior_close * (1 + policy.target_return_pct / 100), up=False)
    stop = _tick_price(market.prior_close * (1 - policy.stop_loss_pct / 100), up=False)
    if not 0 < stop < trigger <= chase < target:
        reasons.append('invalid_price_geometry_or_exhausted_gap')
    elif target - chase <= chase * (policy.round_trip_cost_bps + policy.slippage_bps) / 10000:
        reasons.append('nonpositive_net_edge')
    if reasons:
        return RuleSetupResult(None, tuple(reasons))
    setup_id = canonical_json_sha256({
        'strategy_id': STRATEGY_ID, 'account_reference': account_reference,
        'target_trade_date': session.target_trade_date.isoformat(), 'symbol': market.symbol,
        'setup': 'rebound_open_v1',
    })
    origin = RuleOrigin(
        schema_version='rule_origin.v1', strategy_id=STRATEGY_ID, strategy_lane=STRATEGY_ID,
        rule_version=evaluation['rule_version'], run_id=run_id, evaluation_id=evaluation['evaluation_id'],
        input_manifest_id=evaluation['input_manifest_id'],
        config_hash=canonical_json_sha256({
            'rule_config_hash': evaluation['config_hash'], 'execution_policy_hash': policy.policy_hash,
        }),
        input_hash=canonical_json_sha256({
            'evaluation_id': evaluation['evaluation_id'], 'feature_id': evaluation['feature_id'],
            'market': market.model_dump(mode='json'), 'session': session.model_dump(mode='json'),
        }), target_trade_date=session.target_trade_date, setup_id=setup_id,
    )
    return RuleSetupResult(RuleExecutionSetup(
        account_reference=account_reference, mode=policy.execution_mode, symbol=market.symbol,
        source_market_date=market.prior_close_date, origin=origin, policy_hash=policy.policy_hash,
        trigger_price=trigger, chase_limit_price=chase, target_price=target, stop_price=stop,
        volatility_pct=market.volatility_pct, daily_value_krw=market.daily_value_krw,
        evidence_observed_at=market.observed_at, entry_expires_at=policy.entry_expires_at,
        close_exit_at=close_exit, session=session,
    ), ())


def rule_price_contract_digest(
    *, entry_limit_price: Decimal | None, entry_trigger_price: Decimal | None,
    target_price: Decimal, stop_price: Decimal, close_exit_at: datetime,
) -> str:
    """Rule-only cross-repository digest; existing agent digest stays unchanged."""
    return canonical_json_sha256({
        **{key: format(value.normalize(), 'f') if value is not None else None for key, value in {
            'entry_limit_price': entry_limit_price, 'entry_trigger_price': entry_trigger_price,
            'target_price': target_price, 'stop_price': stop_price,
        }.items()},
        'close_exit_at': _aware(close_exit_at).astimezone(UTC).isoformat(),
    })


class RuleExecutionPlanItem(ExecutionPlanDispatchItemPayload):
    agent_name: None
    decision_agent_name: None
    decision_snapshot_id: None
    rule_origin: RuleOrigin
    close_exit_at: AwareDatetime


class RuleExecutionPlanMetadata(ExecutionPlanDispatchMetadataPayload):
    agent_name: None
    decision_agent_name: None


class RuleExecutionPlan(ExecutionPlanDispatchPayload):
    """Separate strict DTO so legacy agent bytes and validation remain intact."""

    origin_type: Literal['rule']
    strategy_run_id: None
    rule_origin: RuleOrigin
    items: list[RuleExecutionPlanItem] = Field(min_length=3, max_length=3)
    metadata: RuleExecutionPlanMetadata

    @model_validator(mode='after')
    def validate_rule_family(self):
        if self.rule_origin.target_trade_date.isoformat() != self.market_date:
            raise ValueError('rule origin target session mismatch')
        if len({item.symbol for item in self.items}) != 1 or any(
            item.rule_origin != self.rule_origin or item.close_exit_at != self.items[0].close_exit_at
            for item in self.items
        ):
            raise ValueError('rule family must have one exact origin, symbol and exit deadline')
        sessions = [item.risk_context.get('rule_session') for item in self.items]
        if not isinstance(sessions[0], dict) or any(value != sessions[0] for value in sessions):
            raise ValueError('rule family requires exact authoritative session evidence')
        session = sessions[0]
        if set(session) != {'calendar_status', 'calendar_source', 'session_close_at', 'close_buffer_seconds'}:
            raise ValueError('rule session evidence fields mismatch')
        close = _aware(datetime.fromisoformat(session['session_close_at']))
        buffer = session['close_buffer_seconds']
        if (
            session['calendar_status'] != 'authoritative' or not isinstance(session['calendar_source'], str) or not session['calendar_source'].strip()
            or type(buffer) is not int or not 0 < buffer <= 3600
            or close.date() != self.rule_origin.target_trade_date
            or self.items[0].close_exit_at != close - timedelta(seconds=buffer)
            or self.items[0].close_exit_at.astimezone(KST).time() <= time(9)
        ):
            raise ValueError('rule close deadline does not match authoritative close')
        generations = []
        for item in self.items:
            activation = item.risk_context.get('_core_activation')
            if not isinstance(activation, dict) or set(activation) != {
                'schema_version', 'mode', 'authority_generation_fingerprint',
            } or activation.get('schema_version') != 'core_execution_activation_binding_v1' or activation.get('mode') != 'core_confirmed':
                raise ValueError('rule plan requires exact Core-confirmed activation')
            generation = activation['authority_generation_fingerprint']
            if not isinstance(generation, str) or len(generation) != 64 or any(c not in '0123456789abcdef' for c in generation):
                raise ValueError('rule authority generation must be a SHA-256 digest')
            generations.append(generation)
            if item.risk_context.get('instrument_subtype') != 'etf' or item.symbol not in ETF_NAMES:
                raise ValueError('rule requires a whitelisted unleveraged ETF')
            if any(item.risk_context.get(key) is not None for key in (
                'agent_name', 'decision_agent_name', 'decision_snapshot_id', 'decision_run_id', 'confidence',
            )):
                raise ValueError('rule must not claim agent provenance')
        if len(set(generations)) != 1:
            raise ValueError('rule family authority generation mismatch')
        entry = next(item for item in self.items if item.side == 'buy')
        target = next(item for item in self.items if item.intent == 'target_sell')
        stop = next(item for item in self.items if item.intent == 'stop_sell')
        if entry.quantity_policy.get('type') != 'core_sized':
            raise ValueError('rule BUY quantity must be Core-sized')
        prices = [Decimal(str(value)) for value in (
            stop.trigger_price, entry.trigger_price, entry.limit_price, target.trigger_price,
        )]
        if any(not value.is_finite() or value <= 0 for value in prices) or not prices[0] < prices[1] <= prices[2] < prices[3]:
            raise ValueError('rule price geometry is invalid')
        if any(_tick_price(value, up=False) != value for value in prices):
            raise ValueError('rule prices must obey KRX ETF ticks')
        digest = rule_price_contract_digest(
            entry_limit_price=prices[2], entry_trigger_price=prices[1],
            target_price=prices[3], stop_price=prices[0], close_exit_at=entry.close_exit_at,
        )
        if entry.quantity_policy['sizing_inputs'].get('price_contract_digest') != digest:
            raise ValueError('rule immutable price contract digest mismatch')
        if datetime.fromisoformat(entry.quantity_policy['valid_until']) > entry.close_exit_at:
            raise ValueError('rule quantity grant must expire before its close exit')
        return self


def build_rule_execution_plan(
    *, setup: RuleExecutionSetup, sizing: 'RuleSizingResult',
    authority_generation_fingerprint: str, requested_at: datetime,
) -> dict:
    """Prepare strict v4 bytes; this alone creates no runtime authority."""
    requested_at = _aware(requested_at)
    if sizing.reasons or sizing.target_quantity <= 0:
        raise ValueError('rule execution requires a positive proven sizing grant')
    if sizing.sizing_price != setup.chase_limit_price or sizing.valid_until != setup.entry_expires_at:
        raise ValueError('rule sizing grant does not bind the prepared price/expiry')
    if not sizing.calculated_at <= requested_at < sizing.valid_until:
        raise ValueError('rule sizing grant is not current')
    origin = setup.origin.model_dump(mode='json')
    plan_id = f'rule-rebound:{setup.origin.setup_id}'
    entry_key = f'{plan_id}:entry'
    common = {
        'agent_name': None, 'decision_agent_name': None, 'decision_snapshot_id': None,
        'rule_origin': origin, 'symbol': setup.symbol, 'asset_class': 'stock',
        'market': 'KRX', 'exchange': 'KRX', 'currency': 'KRW',
        'broker': 'kis', 'provider': 'kis', 'venue': 'kis',
        'session_policy': {'session': 'regular', 'timezone': 'Asia/Seoul', 'allow_extended_hours': False},
        'close_exit_at': setup.close_exit_at.isoformat(), 'time_in_force': 'day',
        'risk_context': {
            'instrument_subtype': 'etf', 'execution_policy_hash': setup.policy_hash,
            'rule_session': {
                'calendar_status': 'authoritative', 'calendar_source': setup.session.calendar_source,
                'session_close_at': setup.session.session_close_at.isoformat(),
                'close_buffer_seconds': int((setup.session.session_close_at - setup.close_exit_at).total_seconds()),
            },
            '_core_activation': {
                'schema_version': 'core_execution_activation_binding_v1', 'mode': 'core_confirmed',
                'authority_generation_fingerprint': authority_generation_fingerprint,
            },
        },
    }
    price_digest = rule_price_contract_digest(
        entry_limit_price=setup.chase_limit_price, entry_trigger_price=setup.trigger_price,
        target_price=setup.target_price, stop_price=setup.stop_price, close_exit_at=setup.close_exit_at,
    )
    items = [{
        **common, 'core_item_key': entry_key, 'source_plan_item_id': entry_key,
        'side': 'buy', 'intent': 'intraday_watch_buy', 'handoff_intent': 'intraday_watch_buy',
        'limit_price': str(setup.chase_limit_price), 'trigger_price': str(setup.trigger_price),
        'quantity_policy': {
            'type': 'core_sized', 'target_quantity': sizing.target_quantity, 'revision': 1,
            'calculated_at': sizing.calculated_at.isoformat(), 'valid_until': sizing.valid_until.isoformat(),
            'sizing_inputs': {
                **sizing.model_dump(mode='json', exclude={'target_quantity', 'calculated_at', 'valid_until'}),
                'price_contract_digest': price_digest, 'execution_policy_hash': setup.policy_hash,
            },
        },
    }]
    for label, price in (('target_sell', setup.target_price), ('stop_sell', setup.stop_price)):
        key = f'{plan_id}:{label}'
        items.append({
            **common, 'core_item_key': key, 'source_plan_item_id': key,
            'side': 'sell', 'intent': label, 'trigger_price': str(price), 'limit_price': None,
            'quantity_policy': {'type': 'position_exit_from_entry', 'linked_entry_item_key': entry_key},
        })
    payload = {
        'plan_id': plan_id, 'source': EXECUTION_HANDOFF_SOURCE, 'mode': setup.mode,
        'origin_type': 'rule', 'rule_origin': origin, 'strategy_run_id': None,
        'market_date': setup.origin.target_trade_date.isoformat(), 'requested_at': requested_at.isoformat(),
        'items': items, 'metadata': {
            'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
            'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
            'required_capability': EXECUTION_HANDOFF_CAPABILITY,
            'agent_name': None, 'decision_agent_name': None, 'trigger': 'rule_rebound_prepare',
            'requested_at': requested_at.isoformat(), 'account_reference': setup.account_reference,
            'account_label': None, 'source_market_date': setup.source_market_date.isoformat(),
            'target_trade_date': setup.origin.target_trade_date.isoformat(),
            'dispatch_batch_type': 'initial', 'dispatch_batch_key': None,
            'dispatch_material_fingerprint': '0' * 64,
        },
    }
    validated = RuleExecutionPlan.model_validate(payload).model_dump(mode='json', exclude_unset=True)
    validated['metadata']['dispatch_material_fingerprint'] = execution_plan_dispatch_material_fingerprint(validated)
    model = RuleExecutionPlan.model_validate(validated)
    if len(model.model_dump_json(exclude_unset=True).encode('utf-8')) > EXECUTION_PLAN_DISPATCH_MAX_BYTES:
        raise ValueError('rule plan exceeds execution handoff wire budget')
    return validated
