from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any, Callable

import exchange_calendars as exchange_calendars

from app.core.settings import settings
from app.orders.exec_integration import SignalDeciderExecClient
from app.orders.execution_handoff_contract import (
    EXECUTION_HANDOFF_CAPABILITY,
    EXECUTION_HANDOFF_CONTRACT_VERSION,
    EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
    validate_execution_plan_dispatch_ack,
)
from app.repositories.us_equity_repository import load_us_signal_feedback
from app.us_equities.kis_market_codes import to_kis_order_exchange


STRUCTURAL_AGENT_RISK_FLAGS = {
    'role_mismatch',
    'symbol_mismatch',
    'market_date_mismatch',
}
US_EXECUTION_BLOCK_REASON = 'exec_currency_aware_sizing_capability_unavailable'


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _next_us_session(value: date) -> date:
    candidate = value + timedelta(days=1)
    try:
        session = exchange_calendars.get_calendar('XNYS').date_to_session(candidate, direction='next')
        return session.date()
    except Exception:
        pass
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


class USExecutionPlanService:
    def __init__(
        self,
        *,
        feedback_loader: Callable[..., list[dict]] = load_us_signal_feedback,
        client_factory: Callable[[], SignalDeciderExecClient] = SignalDeciderExecClient,
    ) -> None:
        self._feedback_loader = feedback_loader
        self._client_factory = client_factory

    def build(
        self,
        *,
        source_market_date: date,
        target_trade_date: date | None = None,
        max_candidates: int | None = None,
        agent_run_id: str | None = None,
    ) -> dict[str, Any]:
        if not agent_run_id:
            raise ValueError('agent_run_id is required for US equity execution planning')
        resolved_target = target_trade_date or _next_us_session(source_market_date)
        cap = min(max_candidates or settings.us_equity_exec_max_candidates, 80)
        rows = self._feedback_loader(market_date=source_market_date, limit=80, run_id=agent_run_id)
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, str]] = []
        for row in rows:
            if len(accepted) >= cap:
                rejected.append({'symbol': str(row['symbol']), 'reason': 'candidate_cap'})
                continue
            candidate, reason = self._build_candidate(row, source_market_date=source_market_date)
            if candidate is None:
                rejected.append({'symbol': str(row['symbol']), 'reason': reason or 'invalid_candidate'})
                continue
            accepted.append(candidate)

        items: list[dict[str, Any]] = []
        for candidate in accepted:
            entry_key = f"us:{source_market_date.isoformat()}:{candidate['symbol']}:next_open_buy"
            common = {
                'agent_name': 'signal-decider',
                'decision_agent_name': 'signal-decider',
                'symbol': candidate['broker_symbol'],
                'asset_class': 'us_equity',
                'market': 'US',
                'exchange': candidate['exchange'],
                'currency': 'USD',
                'broker': 'kis',
                'provider': 'kis',
                'venue': 'kis',
                'session_policy': {
                    'session': 'regular',
                    'timezone': 'America/New_York',
                    'allow_extended_hours': False,
                    'whole_shares_only': True,
                },
                'time_in_force': 'day',
                'decision_snapshot_id': str(candidate['feedback_id']),
            }
            risk_context = {
                'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
                'agent_run_id': agent_run_id,
                'source_market_date': source_market_date.isoformat(),
                'canonical_symbol': candidate['symbol'],
                'currency': 'USD',
                'exchange': candidate['exchange'],
                'budget_usd': str(settings.us_equity_exec_per_symbol_budget_usd),
                'reward_risk_ratio': str(candidate['reward_risk_ratio']),
                'agent_feedback': candidate['feedback'],
            }
            items.extend(
                [
                    {
                        **common,
                        'core_item_key': entry_key,
                        'source_plan_item_id': entry_key,
                        'side': 'buy',
                        'intent': 'next_open_buy',
                        'handoff_intent': 'next_open_buy',
                        'raw_action': 'buy',
                        'generic_action': 'buy',
                        'quantity_policy': {
                            'type': 'exec_agent_sized',
                            'max_quantity': candidate['quantity'],
                            'budget_usd': str(settings.us_equity_exec_per_symbol_budget_usd),
                            'estimated_reference_price': str(candidate['limit_price']),
                        'pricing_note': (
                            'Preview only: dispatch remains blocked until Exec enforces '
                            'currency-aware USD sizing and account-risk ceilings.'
                        ),
                        },
                        'limit_price': str(candidate['limit_price']),
                        'trigger_price': None,
                        'risk_context': risk_context,
                    },
                    {
                        **common,
                        'core_item_key': f"us:{source_market_date.isoformat()}:{candidate['symbol']}:target_sell",
                        'source_plan_item_id': f"us:{source_market_date.isoformat()}:{candidate['symbol']}:target_sell",
                        'side': 'sell',
                        'intent': 'target_sell',
                        'quantity_policy': {
                            'type': 'position_exit_from_entry',
                            'linked_entry_item_key': entry_key,
                        },
                        'limit_price': None,
                        'trigger_price': str(candidate['target_price']),
                        'risk_context': risk_context,
                    },
                    {
                        **common,
                        'core_item_key': f"us:{source_market_date.isoformat()}:{candidate['symbol']}:stop_sell",
                        'source_plan_item_id': f"us:{source_market_date.isoformat()}:{candidate['symbol']}:stop_sell",
                        'side': 'sell',
                        'intent': 'stop_sell',
                        'quantity_policy': {
                            'type': 'position_exit_from_entry',
                            'linked_entry_item_key': entry_key,
                        },
                        'limit_price': None,
                        'trigger_price': str(candidate['stop_price']),
                        'risk_context': risk_context,
                    },
                ]
            )

        run_identity = (
            hashlib.sha256(agent_run_id.encode('utf-8')).hexdigest()[:12]
            if agent_run_id
            else 'unreachable'
        )
        plan_id = f"core-us-signal-decider-{resolved_target.strftime('%Y%m%d')}-kis-{run_identity}"
        preview_payload = None
        if items:
            requested_at = datetime.now(timezone.utc).isoformat()
            preview_payload = {
                'plan_id': plan_id,
                'source': 'kai-trading-core',
                'mode': settings.signal_decider_exec_runtime_mode,
                'market_date': resolved_target.isoformat(),
                'strategy_run_id': agent_run_id,
                'requested_at': requested_at,
                'items': items,
                'metadata': {
                    'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
                    'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                    'required_capability': EXECUTION_HANDOFF_CAPABILITY,
                    'agent_name': 'signal-decider',
                    'decision_agent_name': 'signal-decider',
                    'trigger': 'us_equity_signal_decider',
                    'requested_at': requested_at,
                    'account_reference': None,
                    'account_label': None,
                    'source_market_date': source_market_date.isoformat(),
                    'target_trade_date': resolved_target.isoformat(),
                    'dispatch_batch_type': 'initial',
                    'dispatch_batch_key': None,
                },
            }
        return {
            'status': 'preview_only' if items else 'empty',
            'execution_supported': False,
            'execution_block_reason': US_EXECUTION_BLOCK_REASON,
            'source_market_date': source_market_date.isoformat(),
            'target_trade_date': resolved_target.isoformat(),
            'accepted_candidate_count': len(accepted),
            'rejected_candidate_count': len(rejected),
            'rejections': rejected,
            # This proposal deliberately is not exposed as a dispatch payload:
            # the current v4 capability only certifies KIS/KRX/KRW sizing.
            'payload': None,
            'preview_payload': preview_payload,
        }

    async def dispatch(
        self,
        *,
        source_market_date: date,
        target_trade_date: date | None = None,
        max_candidates: int | None = None,
        dry_run: bool = True,
        agent_run_id: str | None = None,
    ) -> dict[str, Any]:
        if not agent_run_id:
            raise ValueError('agent_run_id is required for US equity execution planning')
        result = self.build(
            source_market_date=source_market_date,
            target_trade_date=target_trade_date,
            max_candidates=max_candidates,
            agent_run_id=agent_run_id,
        )
        result['dry_run'] = dry_run
        if dry_run or result['status'] == 'empty':
            return result
        if not result['execution_supported']:
            # Exec's delegated sizing contract is currently KRW/KRX-specific.
            # Keep US plans as inspectable previews until Exec can enforce
            # currency-aware cash, fee, quantity, and account-route ceilings.
            raise ValueError(str(result['execution_block_reason']))
        payload = result['payload']
        if not isinstance(payload, dict):
            raise ValueError('execution_dispatch_payload_unavailable')
        if not settings.us_equity_execution_enabled:
            raise ValueError('US equity execution dispatch is disabled by configuration')
        metadata = payload.get('metadata')
        dispatch_material_fingerprint = (
            metadata.get('dispatch_material_fingerprint')
            if isinstance(metadata, dict)
            else None
        )
        if not isinstance(dispatch_material_fingerprint, str):
            raise ValueError(
                'execution dispatch payload validation error: '
                'dispatch_material_fingerprint is required'
            )
        exec_response = await self._client_factory().post_execution_plan(payload)
        validate_execution_plan_dispatch_ack(
            exec_response,
            plan_id=str(payload['plan_id']),
            submitted_core_item_keys=[
                str(item['core_item_key']) for item in payload['items']
            ],
            expected_dispatch_material_fingerprint=(
                dispatch_material_fingerprint
            ),
        )
        result['exec_response'] = exec_response
        result['status'] = 'dispatched'
        return result

    @staticmethod
    def _build_candidate(
        row: dict,
        *,
        source_market_date: date,
    ) -> tuple[dict[str, Any] | None, str | None]:
        feedback = row.get('normalized_feedback_payload')
        if not isinstance(feedback, dict):
            return None, 'missing_signal_feedback'
        risk_flags = {
            str(value).strip().lower()
            for value in feedback.get('risk_flags') or []
            if str(value).strip()
        }
        structurally_valid = (
            str(feedback.get('role') or '').strip().lower() == 'signal-decider'
            and str(feedback.get('symbol') or '').strip().upper() == str(row.get('symbol') or '').strip().upper()
            and str(feedback.get('market_date') or '').strip() == source_market_date.isoformat()
            and not (risk_flags & STRUCTURAL_AGENT_RISK_FLAGS)
        )
        if not structurally_valid:
            return None, 'agent_structural_mismatch'
        if str(feedback.get('next_day_open_action') or feedback.get('preferred_action') or '').lower() != 'buy':
            return None, 'agent_action_not_buy'
        if feedback.get('data_quality_warnings'):
            return None, 'agent_data_quality_warning'
        confidence = _decimal(feedback.get('confidence_score_10'))
        if confidence is None or confidence < Decimal(str(settings.us_equity_exec_min_confidence_score_10)):
            return None, 'confidence_below_threshold'
        persisted_context = row.get('context_payload') or {}
        symbol_day_context = persisted_context.get('symbol_day_context') or {}
        price_metrics = symbol_day_context.get('price_screen') or (
            (row.get('metrics_payload') or {}).get('price') or {}
        )
        latest_price = _decimal(price_metrics.get('latest_price'))
        target_price = _decimal(feedback.get('target_price'))
        stop_price = _decimal(feedback.get('stop_loss_price'))
        if latest_price is None or target_price is None or stop_price is None:
            return None, 'missing_price_or_risk_levels'
        buffer = Decimal('1') + (settings.us_equity_exec_limit_buffer_pct / Decimal('100'))
        limit_price = (latest_price * buffer).quantize(Decimal('0.01'), rounding=ROUND_UP)
        if not (stop_price < limit_price < target_price):
            return None, 'invalid_price_level_ordering'
        downside = limit_price - stop_price
        reward_risk = (target_price - limit_price) / downside
        if reward_risk < Decimal(str(settings.us_equity_exec_min_reward_risk_ratio)):
            return None, 'reward_risk_below_threshold'
        quantity = int(
            (settings.us_equity_exec_per_symbol_budget_usd / limit_price).to_integral_value(rounding=ROUND_DOWN)
        )
        if quantity <= 0:
            return None, 'budget_below_one_share'
        try:
            exchange = to_kis_order_exchange(str(row.get('exchange_code') or row.get('market') or ''))
        except ValueError:
            return None, 'unsupported_exchange'
        return {
            'feedback_id': int(row['feedback_id']),
            'symbol': str(row['symbol']),
            'broker_symbol': str(row.get('standard_code') or row['symbol']),
            'exchange': exchange,
            'quantity': quantity,
            'limit_price': limit_price,
            'target_price': target_price,
            'stop_price': stop_price,
            'reward_risk_ratio': reward_risk.quantize(Decimal('0.0001')),
            'feedback': feedback,
        }, None


us_execution_plan_service = USExecutionPlanService()
