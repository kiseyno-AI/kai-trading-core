from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any, Callable

import exchange_calendars as exchange_calendars

from app.core.settings import settings
from app.orders.exec_integration import SignalDeciderExecClient
from app.orders.execution_handoff_contract import (
    EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
    serialize_execution_plan_dispatch_payload,
)
from app.repositories.us_equity_repository import load_us_signal_feedback
from app.us_equities.kis_market_codes import to_kis_order_exchange


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
                        'quantity_policy': {'kind': 'shares', 'quantity': candidate['quantity']},
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

        plan_id = f"core-us-signal-decider-{resolved_target.strftime('%Y%m%d')}-kis"
        payload = serialize_execution_plan_dispatch_payload(
            {
                'plan_id': plan_id,
                'source': settings.signal_decider_exec_dispatch_source or 'kai-trading-core',
                'mode': settings.signal_decider_exec_runtime_mode,
                'market_date': resolved_target.isoformat(),
                'strategy_run_id': agent_run_id or f'us-signal-decider-preview:{source_market_date.isoformat()}',
                'requested_at': datetime.now(timezone.utc).isoformat(),
                'items': items,
                'metadata': {
                    'schema_version': EXECUTION_PLAN_DISPATCH_SCHEMA_VERSION,
                    'market': 'US',
                    'currency': 'USD',
                    'source_market_date': source_market_date.isoformat(),
                    'agent_run_id': agent_run_id,
                },
            }
        )
        return {
            'status': 'ready' if items else 'empty',
            'source_market_date': source_market_date.isoformat(),
            'target_trade_date': resolved_target.isoformat(),
            'accepted_candidate_count': len(accepted),
            'rejected_candidate_count': len(rejected),
            'rejections': rejected,
            'payload': payload,
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
        if not dry_run and not agent_run_id:
            raise ValueError('agent_run_id is required for non-preview US equity execution dispatch')
        result = self.build(
            source_market_date=source_market_date,
            target_trade_date=target_trade_date,
            max_candidates=max_candidates,
            agent_run_id=agent_run_id,
        )
        result['dry_run'] = dry_run
        if dry_run or not result['payload']['items']:
            return result
        if not settings.us_equity_execution_enabled:
            raise ValueError('US equity execution dispatch is disabled by configuration')
        result['exec_response'] = await self._client_factory().post_execution_plan(result['payload'])
        result['status'] = 'dispatched'
        return result

    @staticmethod
    def _build_candidate(
        row: dict,
        *,
        source_market_date: date,
    ) -> tuple[dict[str, Any] | None, str | None]:
        del source_market_date
        feedback = row.get('normalized_feedback_payload')
        if not isinstance(feedback, dict):
            return None, 'missing_signal_feedback'
        if str(feedback.get('next_day_open_action') or feedback.get('preferred_action') or '').lower() != 'buy':
            return None, 'agent_action_not_buy'
        if feedback.get('data_quality_warnings'):
            return None, 'agent_data_quality_warning'
        confidence = _decimal(feedback.get('confidence_score_10'))
        if confidence is None or confidence < Decimal(str(settings.us_equity_exec_min_confidence_score_10)):
            return None, 'confidence_below_threshold'
        price_metrics = (row.get('metrics_payload') or {}).get('price') or {}
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
