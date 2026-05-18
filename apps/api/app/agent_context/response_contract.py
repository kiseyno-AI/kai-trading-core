from __future__ import annotations

import json
from datetime import date
from typing import Any

from app.agent_context.decision_delta import build_decision_delta
from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME
from app.agent_context.wait_reason_taxonomy import (
    WAIT_REASON_ISSUE_GENERIC,
    WAIT_REASON_ISSUE_MISSING,
    derive_wait_reason,
    wait_reason_taxonomy_codes,
)


REQUIRED_RESPONSE_KEYS = ('role', 'summary', 'confidence', 'risk_flags', 'details', 'improvement_notes')
SIGNAL_DECIDER_ACTIONS = {'buy', 'wait', 'reject'}
SHORT_TERM_DECIDER_ACTIONS = {'scalp_buy_candidate', 'watch_for_trigger', 'avoid_chase', 'wait', 'reject'}


def _select_feedback_payload(parsed: dict[str, Any] | None, *, symbol: str | None = None) -> dict[str, Any] | None:
    if not isinstance(parsed, dict):
        return None
    items_source = parsed.get('items')
    batch = parsed.get('batch')
    if not isinstance(items_source, list) and isinstance(batch, dict):
        items_source = batch.get('items')
    if not isinstance(items_source, list):
        return parsed

    normalized_symbol = str(symbol or '').strip() or None
    fallback_feedback: dict[str, Any] | None = None
    for item in items_source:
        if not isinstance(item, dict):
            continue
        item_symbol = str(item.get('symbol') or '').strip() or None
        feedback = item.get('feedback') if isinstance(item.get('feedback'), dict) else None
        if feedback is None:
            feedback = {key: value for key, value in item.items() if key != 'feedback'}
        if not isinstance(feedback, dict):
            continue
        if fallback_feedback is None:
            fallback_feedback = feedback
        feedback_symbol = str(feedback.get('symbol') or '').strip() or item_symbol
        if normalized_symbol is None or feedback_symbol == normalized_symbol or item_symbol == normalized_symbol:
            return feedback
    return fallback_feedback


def _normalize_text_list(value: Any) -> list[str]:
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    if not isinstance(value, list):
        return []
    items: list[str] = []
    seen: set[str] = set()
    for item in value:
        if item is None:
            continue
        normalized = item.strip() if isinstance(item, str) else str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        items.append(normalized)
    return items


def _normalize_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _normalize_score_10(value: Any) -> float | None:
    normalized = _normalize_number(value)
    if normalized is None or normalized < 1 or normalized > 10:
        return None
    return float(normalized)


def _normalize_iso_date(value: Any) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return None


def _normalize_signal_decider_action(value: Any) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in SIGNAL_DECIDER_ACTIONS:
            return normalized
    return 'wait'


def _normalize_short_term_decider_action(value: Any) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in SHORT_TERM_DECIDER_ACTIONS:
            return normalized
    return 'wait'


def _build_signal_decider_details(
    *,
    parsed_details: dict[str, Any],
    decision_delta: dict[str, Any],
    next_day_open_action: str,
    wait_reason: dict[str, Any] | None,
    entry_price_basis: str,
    target_price: float | None,
    expected_return_pct: float | None,
    stop_loss_price: float | None,
    suggested_holding_period: str | None,
    confidence_score_10: float | None,
    conviction_score_10: float | None,
    supporting_evidence: list[str],
    decision_risks: list[str],
    invalidation_conditions: list[str],
    data_quality_warnings: list[str],
    specialist_disagreement: list[str],
    traceability: dict[str, Any],
) -> dict[str, Any]:
    return {
        **parsed_details,
        'decision_support': {
            'next_day_open_action': next_day_open_action,
            'wait_reason': wait_reason,
            'decision_delta': decision_delta,
            'entry_price_basis': entry_price_basis,
            'target_price': target_price,
            'expected_return_pct': expected_return_pct,
            'stop_loss_price': stop_loss_price,
            'suggested_holding_period': suggested_holding_period,
            'confidence_score_10': confidence_score_10,
            'conviction_score_10': conviction_score_10,
            'supporting_evidence': supporting_evidence,
            'decision_risks': decision_risks,
            'invalidation_conditions': invalidation_conditions,
            'data_quality_warnings': data_quality_warnings,
            'specialist_disagreement': specialist_disagreement,
            'decision_support_only': True,
        },
        'traceability': traceability,
    }


def _normalize_signal_decider_response(
    *,
    role: str,
    parsed: dict[str, Any] | None,
    output_text: str | None,
    market_date: str | date | None,
    symbol: str | None,
    agent_name: str | None,
) -> dict[str, Any]:
    payload = _select_feedback_payload(parsed, symbol=symbol) or {}
    parsed_details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
    next_day_open_action = _normalize_signal_decider_action(
        payload.get('next_day_open_action') or payload.get('preferred_action')
    )
    entry_price_basis = str(payload.get('entry_price_basis') or 'next_open').strip() or 'next_open'
    target_price = _normalize_number(payload.get('target_price'))
    expected_return_pct = _normalize_number(payload.get('expected_return_pct'))
    stop_loss_price = _normalize_number(payload.get('stop_loss_price'))
    suggested_holding_period = str(payload.get('suggested_holding_period')).strip() if payload.get('suggested_holding_period') is not None else None
    confidence_score_10 = _normalize_score_10(payload.get('confidence_score_10'))
    conviction_score_10 = _normalize_score_10(payload.get('conviction_score_10'))
    thesis_summary = str(payload.get('thesis_summary') or payload.get('summary') or output_text or '').strip()
    supporting_evidence = _normalize_text_list(payload.get('supporting_evidence'))
    decision_risks = _normalize_text_list(payload.get('decision_risks'))
    invalidation_conditions = _normalize_text_list(payload.get('invalidation_conditions'))
    data_quality_warnings = _normalize_text_list(payload.get('data_quality_warnings'))
    specialist_disagreement = _normalize_text_list(payload.get('specialist_disagreement'))
    improvement_notes = _normalize_text_list(payload.get('improvement_notes'))
    risk_flags = _normalize_text_list(payload.get('risk_flags'))
    wait_reason, wait_reason_issues = derive_wait_reason(
        role=role,
        action=next_day_open_action,
        payload=payload,
    )
    if wait_reason_issues:
        risk_flags = list(dict.fromkeys([*risk_flags, *wait_reason_issues]))
        if WAIT_REASON_ISSUE_MISSING in wait_reason_issues or WAIT_REASON_ISSUE_GENERIC in wait_reason_issues:
            improvement_notes = list(
                dict.fromkeys(
                    [
                        *improvement_notes,
                        'Wait decisions must include a concrete wait_reason code and blocker summary.',
                    ]
                )
            )
    reported_role = payload.get('role')
    reported_symbol = str(payload.get('symbol')).strip() if payload.get('symbol') is not None else None
    reported_market_date = _normalize_iso_date(payload.get('market_date'))

    normalized_symbol = (symbol or reported_symbol or '').strip() or None
    normalized_market_date = _normalize_iso_date(market_date) or reported_market_date
    resolved_agent_name = (agent_name or role).strip() or role

    traceability = payload.get('traceability') if isinstance(payload.get('traceability'), dict) else {}
    if parsed is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'unstructured_response']))
        if not data_quality_warnings:
            data_quality_warnings = ['unstructured_response']
        improvement_notes = list(
            dict.fromkeys([*improvement_notes, 'Return the full structured decision-support contract in JSON.'])
        )
        if output_text:
            traceability = {**traceability, 'raw_output_text': output_text}
    if reported_role and reported_role != role:
        traceability = {**traceability, 'reported_role': reported_role}
        risk_flags = list(dict.fromkeys([*risk_flags, 'role_mismatch']))
    if reported_symbol and normalized_symbol and reported_symbol != normalized_symbol:
        traceability = {**traceability, 'reported_symbol': reported_symbol}
        risk_flags = list(dict.fromkeys([*risk_flags, 'symbol_mismatch']))
    if reported_market_date and normalized_market_date and reported_market_date != normalized_market_date:
        traceability = {**traceability, 'reported_market_date': reported_market_date}
        risk_flags = list(dict.fromkeys([*risk_flags, 'market_date_mismatch']))

    traceability = {
        **traceability,
        'agent_name': resolved_agent_name,
        'symbol': normalized_symbol,
        'market_date': normalized_market_date,
    }
    risk_flags = list(dict.fromkeys([*risk_flags, *data_quality_warnings]))

    confidence = payload.get('confidence') if isinstance(payload.get('confidence'), (int, float)) else None
    if confidence is not None and confidence > 1 and confidence <= 10:
        if confidence_score_10 is None:
            confidence_score_10 = float(confidence)
        confidence = round(float(confidence) / 10, 4)
    if confidence is None and confidence_score_10 is not None:
        confidence = round(confidence_score_10 / 10, 4)

    decision_delta = build_decision_delta(
        role=role,
        payload=payload,
        market_date=normalized_market_date,
        symbol=normalized_symbol,
        previous_decision_context=payload.get('previous_decision_context'),
    )

    normalized = {
        'role': role,
        'agent_name': resolved_agent_name,
        'symbol': normalized_symbol,
        'market_date': normalized_market_date,
        'preferred_action': next_day_open_action,
        'next_day_open_action': next_day_open_action,
        'wait_reason': wait_reason,
        'entry_price_basis': entry_price_basis,
        'target_price': target_price,
        'expected_return_pct': expected_return_pct,
        'stop_loss_price': stop_loss_price,
        'suggested_holding_period': suggested_holding_period,
        'confidence_score_10': confidence_score_10,
        'conviction_score_10': conviction_score_10,
        'thesis_summary': thesis_summary,
        'supporting_evidence': supporting_evidence,
        'decision_risks': decision_risks,
        'invalidation_conditions': invalidation_conditions,
        'data_quality_warnings': data_quality_warnings,
        'specialist_disagreement': specialist_disagreement,
        'decision_delta': decision_delta,
        'traceability': traceability,
        'summary': thesis_summary,
        'confidence': confidence,
        'risk_flags': risk_flags,
        'details': _build_signal_decider_details(
            parsed_details=parsed_details,
            decision_delta=decision_delta,
            next_day_open_action=next_day_open_action,
            wait_reason=wait_reason,
            entry_price_basis=entry_price_basis,
            target_price=target_price,
            expected_return_pct=expected_return_pct,
            stop_loss_price=stop_loss_price,
            suggested_holding_period=suggested_holding_period,
            confidence_score_10=confidence_score_10,
            conviction_score_10=conviction_score_10,
            supporting_evidence=supporting_evidence,
            decision_risks=decision_risks,
            invalidation_conditions=invalidation_conditions,
            data_quality_warnings=data_quality_warnings,
            specialist_disagreement=specialist_disagreement,
            traceability=traceability,
        ),
        'improvement_notes': improvement_notes,
    }
    return normalized


def _build_short_term_decider_details(
    *,
    parsed_details: dict[str, Any],
    decision_delta: dict[str, Any],
    tactical_action: str,
    wait_reason: dict[str, Any] | None,
    trigger_price: float | None,
    chase_limit_price: float | None,
    target_price: float | None,
    stop_loss_price: float | None,
    expected_holding_period: str | None,
    confidence_score_10: float | None,
    conviction_score_10: float | None,
    trigger_conditions: list[str],
    supporting_evidence: list[str],
    tactical_risks: list[str],
    invalidation_conditions: list[str],
    data_quality_warnings: list[str],
    traceability: dict[str, Any],
) -> dict[str, Any]:
    return {
        **parsed_details,
        'tactical_decision_support': {
            'tactical_action': tactical_action,
            'wait_reason': wait_reason,
            'decision_delta': decision_delta,
            'trigger_price': trigger_price,
            'chase_limit_price': chase_limit_price,
            'target_price': target_price,
            'stop_loss_price': stop_loss_price,
            'expected_holding_period': expected_holding_period,
            'confidence_score_10': confidence_score_10,
            'conviction_score_10': conviction_score_10,
            'trigger_conditions': trigger_conditions,
            'supporting_evidence': supporting_evidence,
            'tactical_risks': tactical_risks,
            'invalidation_conditions': invalidation_conditions,
            'data_quality_warnings': data_quality_warnings,
            'decision_support_only': True,
        },
        'traceability': traceability,
    }


def _normalize_short_term_decider_response(
    *,
    role: str,
    parsed: dict[str, Any] | None,
    output_text: str | None,
    market_date: str | date | None,
    symbol: str | None,
    agent_name: str | None,
) -> dict[str, Any]:
    payload = _select_feedback_payload(parsed, symbol=symbol) or {}
    parsed_details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
    tactical_action = _normalize_short_term_decider_action(payload.get('tactical_action') or payload.get('preferred_action'))
    trigger_price = _normalize_number(payload.get('trigger_price'))
    chase_limit_price = _normalize_number(payload.get('chase_limit_price'))
    target_price = _normalize_number(payload.get('target_price'))
    stop_loss_price = _normalize_number(payload.get('stop_loss_price'))
    expected_holding_period = str(payload.get('expected_holding_period')).strip() if payload.get('expected_holding_period') is not None else None
    confidence_score_10 = _normalize_score_10(payload.get('confidence_score_10'))
    conviction_score_10 = _normalize_score_10(payload.get('conviction_score_10'))
    thesis_summary = str(payload.get('thesis_summary') or payload.get('summary') or output_text or '').strip()
    trigger_conditions = _normalize_text_list(payload.get('trigger_conditions'))
    supporting_evidence = _normalize_text_list(payload.get('supporting_evidence'))
    tactical_risks = _normalize_text_list(payload.get('tactical_risks') or payload.get('decision_risks'))
    invalidation_conditions = _normalize_text_list(payload.get('invalidation_conditions'))
    data_quality_warnings = _normalize_text_list(payload.get('data_quality_warnings'))
    improvement_notes = _normalize_text_list(payload.get('improvement_notes'))
    risk_flags = _normalize_text_list(payload.get('risk_flags'))
    wait_reason, wait_reason_issues = derive_wait_reason(
        role=role,
        action=tactical_action,
        payload=payload,
    )
    if wait_reason_issues:
        risk_flags = list(dict.fromkeys([*risk_flags, *wait_reason_issues]))
        if WAIT_REASON_ISSUE_MISSING in wait_reason_issues or WAIT_REASON_ISSUE_GENERIC in wait_reason_issues:
            improvement_notes = list(
                dict.fromkeys(
                    [
                        *improvement_notes,
                        'Wait-like tactical actions must include a concrete wait_reason code and blocker summary.',
                    ]
                )
            )
    reported_role = payload.get('role')
    reported_symbol = str(payload.get('symbol')).strip() if payload.get('symbol') is not None else None
    reported_market_date = _normalize_iso_date(payload.get('market_date'))

    normalized_symbol = (symbol or reported_symbol or '').strip() or None
    normalized_market_date = _normalize_iso_date(market_date) or reported_market_date
    resolved_agent_name = (agent_name or role).strip() or role

    traceability = payload.get('traceability') if isinstance(payload.get('traceability'), dict) else {}
    if parsed is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'unstructured_response']))
        if not data_quality_warnings:
            data_quality_warnings = ['unstructured_response']
        improvement_notes = list(
            dict.fromkeys([*improvement_notes, 'Return the full structured short-term decision-support contract in JSON.'])
        )
        if output_text:
            traceability = {**traceability, 'raw_output_text': output_text}
    if reported_role and reported_role != role:
        traceability = {**traceability, 'reported_role': reported_role}
        risk_flags = list(dict.fromkeys([*risk_flags, 'role_mismatch']))
    if reported_symbol and normalized_symbol and reported_symbol != normalized_symbol:
        traceability = {**traceability, 'reported_symbol': reported_symbol}
        risk_flags = list(dict.fromkeys([*risk_flags, 'symbol_mismatch']))
    if reported_market_date and normalized_market_date and reported_market_date != normalized_market_date:
        traceability = {**traceability, 'reported_market_date': reported_market_date}
        risk_flags = list(dict.fromkeys([*risk_flags, 'market_date_mismatch']))

    traceability = {
        **traceability,
        'agent_name': resolved_agent_name,
        'symbol': normalized_symbol,
        'market_date': normalized_market_date,
    }
    risk_flags = list(dict.fromkeys([*risk_flags, *data_quality_warnings]))

    confidence = payload.get('confidence') if isinstance(payload.get('confidence'), (int, float)) else None
    if confidence is not None and confidence > 1 and confidence <= 10:
        if confidence_score_10 is None:
            confidence_score_10 = float(confidence)
        confidence = round(float(confidence) / 10, 4)
    if confidence is None and confidence_score_10 is not None:
        confidence = round(confidence_score_10 / 10, 4)

    decision_delta = build_decision_delta(
        role=role,
        payload=payload,
        market_date=normalized_market_date,
        symbol=normalized_symbol,
        previous_decision_context=payload.get('previous_decision_context'),
    )

    return {
        'role': role,
        'agent_name': resolved_agent_name,
        'symbol': normalized_symbol,
        'market_date': normalized_market_date,
        'tactical_action': tactical_action,
        'preferred_action': tactical_action,
        'wait_reason': wait_reason,
        'trigger_price': trigger_price,
        'chase_limit_price': chase_limit_price,
        'target_price': target_price,
        'stop_loss_price': stop_loss_price,
        'expected_holding_period': expected_holding_period,
        'confidence_score_10': confidence_score_10,
        'conviction_score_10': conviction_score_10,
        'thesis_summary': thesis_summary,
        'trigger_conditions': trigger_conditions,
        'supporting_evidence': supporting_evidence,
        'tactical_risks': tactical_risks,
        'invalidation_conditions': invalidation_conditions,
        'data_quality_warnings': data_quality_warnings,
        'decision_delta': decision_delta,
        'traceability': traceability,
        'summary': thesis_summary,
        'confidence': confidence,
        'risk_flags': risk_flags,
        'details': _build_short_term_decider_details(
            parsed_details=parsed_details,
            decision_delta=decision_delta,
            tactical_action=tactical_action,
            wait_reason=wait_reason,
            trigger_price=trigger_price,
            chase_limit_price=chase_limit_price,
            target_price=target_price,
            stop_loss_price=stop_loss_price,
            expected_holding_period=expected_holding_period,
            confidence_score_10=confidence_score_10,
            conviction_score_10=conviction_score_10,
            trigger_conditions=trigger_conditions,
            supporting_evidence=supporting_evidence,
            tactical_risks=tactical_risks,
            invalidation_conditions=invalidation_conditions,
            data_quality_warnings=data_quality_warnings,
            traceability=traceability,
        ),
        'improvement_notes': improvement_notes,
    }


def build_contract_prompt_suffix(*, role: str | None = None) -> str:
    wait_reason_codes = ', '.join(wait_reason_taxonomy_codes())
    if role == SHORT_TERM_DECIDER_AGENT_NAME:
        return (
            'Return JSON only. Do not wrap in markdown. '
            'For every short-term-decider feedback object, required keys are: role, agent_name, symbol, market_date, tactical_action, preferred_action, '
            'wait_reason, trigger_price, chase_limit_price, target_price, stop_loss_price, expected_holding_period, confidence_score_10, conviction_score_10, '
            'thesis_summary, trigger_conditions, supporting_evidence, tactical_risks, invalidation_conditions, data_quality_warnings, decision_delta, traceability, '
            'summary, confidence, risk_flags, details, improvement_notes. '
            'Operator-facing free-text fields must be written in Korean while keeping the outer response as raw JSON only. '
            'Do not translate or localize machine-readable enum fields such as tactical_action or preferred_action. '
            'role must equal the requested role. agent_name should equal short-term-decider. '
            'symbol is mandatory and must exactly equal the assigned/requested symbol string. '
            'Do not omit, alias, translate, normalize away leading zeroes, reorder, infer, or substitute symbols. '
            'traceability.symbol must exactly match symbol, and both must match the assigned item symbol. '
            'tactical_action and preferred_action must be one of scalp_buy_candidate, watch_for_trigger, avoid_chase, wait, reject exactly as written. '
            'Use scalp_buy_candidate only when momentum, participation, liquidity, and trigger/risk levels are all supportable; otherwise prefer watch_for_trigger, avoid_chase, wait, or reject. '
            f'wait_reason must be an object with code and summary when tactical_action/preferred_action is watch_for_trigger, avoid_chase, or wait; otherwise use null. Valid wait_reason.code values are: {wait_reason_codes}. '
            'wait_reason.summary must be a concise Korean explanation that names the concrete blocker. '
            'Do not use vague wait text such as insufficient information, need more data, or wait for more confirmation unless the same summary explicitly names the concrete blocker. '
            'decision_delta must be an object, not prose, and should summarize prior-vs-current change metadata when previous_decision_context is available in the request. '
            'decision_delta should include previous_market_date, previous_recommendation, previous_recommendation_class, previous_wait_reason, current_recommendation, current_recommendation_class, current_wait_reason, transition, persistence_trend, changed_fields, stable_fields, what_changed, what_stayed_same, and upgrade_trigger. '
            'When a wait-like recommendation repeats, do not copy-paste the earlier explanation. Summarize what changed, what stayed the same, whether the setup is persistent or improving or worsening, and the concrete trigger that upgrades the setup to scalp_buy_candidate or watch_for_trigger. '
            'trigger_price, chase_limit_price, target_price, and stop_loss_price must be included for every item and should be explicit conservative numeric levels whenever the inputs support them; use null rather than fake precision when unsupported and explain why in data_quality_warnings. '
            'expected_holding_period should normally be 1-2 trading days or 1-3 trading days depending on the setup and evidence. '
            'confidence_score_10 and conviction_score_10 must be numbers from 1 to 10 when provided. '
            'thesis_summary must be a concise short-term tactical decision-support explanation in Korean. '
            'trigger_conditions, supporting_evidence, tactical_risks, invalidation_conditions, data_quality_warnings, risk_flags, and improvement_notes must be arrays of short Korean strings. '
            'details must be an object with structured tactical decision-support fields and traceability. '
            'Treat core execution/liquidity inputs such as liquidity, quote freshness, spread/slippage, and execution context as the main blocker domain. '
            'When supplied, use decision_input_quality_context.final_judgment and execution_feasibility as the authoritative buy-quality guardrail. '
            'Treat news/disclosure and detailed quarterly/cash-flow context as auxiliary unless the tactic explicitly depends on them. '
            'Empty recent news means no captured recent news and is neutral unless other risk evidence exists. '
            'Missing detailed quarterly/cash-flow fields should usually reduce confidence only. '
            'Do not classify a symbol as untradeable solely because auxiliary news or financial detail is absent. '
            'Optional news/disclosure or quarterly/cash-flow gaps must not become the primary wait_reason when price, liquidity, trend, or execution blockers already explain the conservative action. '
            'If core execution evidence is missing, stale, weak, conflicting, or chase risk is elevated, still return a complete structured conservative feedback object with avoid_chase, wait, or reject, null unsupported numeric fields, and explicit risk/data-quality warnings. '
            'If only auxiliary context is absent, a structured scalp_buy_candidate or watch_for_trigger decision may still be valid when price, liquidity, trigger, and risk evidence are otherwise strong. '
            'Before finalizing, self-check that the JSON is parseable, all required keys are present, role and agent_name are correct, symbol and traceability.symbol exactly match the assigned symbol, operator-facing free-text fields are in Korean, and arrays are arrays of strings. '
            'Immediately before sending, validate the final raw JSON text again and correct any extra or missing closing braces/brackets so it begins with {, ends with }, and parses successfully.'
        )
    if role == SIGNAL_DECIDER_AGENT_NAME:
        return (
            'Return JSON only. Do not wrap in markdown. '
            'For every signal-decider feedback object, required keys are: role, agent_name, symbol, market_date, preferred_action, next_day_open_action, '
            'wait_reason, entry_price_basis, target_price, expected_return_pct, stop_loss_price, suggested_holding_period, '
            'confidence_score_10, conviction_score_10, thesis_summary, supporting_evidence, decision_risks, '
            'decision_delta, '
            'invalidation_conditions, data_quality_warnings, specialist_disagreement, traceability, summary, confidence, '
            'risk_flags, details, improvement_notes. '
            'Operator-facing free-text fields must be written in Korean while keeping the outer response as raw JSON only. '
            'Do not translate or localize machine-readable enum fields such as preferred_action, next_day_open_action, or entry_price_basis. '
            'role must equal the requested role. '
            'agent_name should equal signal-decider. '
            'symbol is mandatory and must exactly equal the assigned/requested symbol string. '
            'Do not omit, alias, translate, normalize away leading zeroes, reorder, infer, or substitute symbols. '
            'traceability.symbol must exactly match symbol, and both must match the assigned item symbol. '
            'next_day_open_action and preferred_action must be one of buy, wait, reject exactly as written. '
            f'wait_reason must be an object with code and summary when preferred_action/next_day_open_action is wait; otherwise use null. Valid wait_reason.code values are: {wait_reason_codes}. '
            'wait_reason.summary must be a concise Korean explanation that names the concrete blocker. '
            'Do not use vague wait text such as insufficient information, need more data, or wait for more confirmation unless the same summary explicitly names the concrete blocker. '
            'decision_delta must be an object, not prose, and should summarize prior-vs-current change metadata when previous_decision_context is available in the request. '
            'decision_delta should include previous_market_date, previous_recommendation, previous_recommendation_class, previous_wait_reason, current_recommendation, current_recommendation_class, current_wait_reason, transition, persistence_trend, changed_fields, stable_fields, what_changed, what_stayed_same, and upgrade_trigger. '
            'When wait repeats, do not copy-paste the earlier explanation. Summarize what changed, what stayed the same, whether the setup is persistent or improving or worsening, and the concrete trigger that would upgrade the setup to buy. '
            'entry_price_basis should be next_open unless the supplied inputs justify a different explicit basis. '
            'target_price and stop_loss_price must be included for every item and should be explicit conservative numeric levels whenever the inputs support them; if a concrete level is genuinely not supportable, keep the keys and explain the limitation in data_quality_warnings instead of omitting them. '
            'Use null rather than fake precision when a numeric field is not well-supported by the inputs. '
            'confidence_score_10 and conviction_score_10 must be numbers from 1 to 10 when provided. '
            'thesis_summary must be a concise decision-support explanation in Korean. '
            'supporting_evidence, decision_risks, invalidation_conditions, data_quality_warnings, specialist_disagreement, '
            'risk_flags, and improvement_notes must be arrays of short Korean strings. '
            'details must be an object with structured decision-support fields and traceability. '
            'Treat core execution/liquidity inputs such as liquidity, quote freshness, spread/slippage, and execution context as the main blocker domain. '
            'When supplied, use decision_input_quality_context.final_judgment and execution_feasibility as the authoritative buy-quality guardrail. '
            'Treat news/disclosure and detailed quarterly/cash-flow context as auxiliary unless the thesis explicitly depends on them. '
            'Empty recent news means no captured recent news and is neutral unless other risk evidence exists. '
            'Missing detailed quarterly/cash-flow fields should usually reduce confidence only. '
            'Do not classify a symbol as untradeable solely because auxiliary news or financial detail is absent. '
            'Optional news/disclosure or quarterly/cash-flow gaps must not become the primary wait_reason when price, liquidity, trend, execution timing, or volatility blockers already explain the wait. '
            'If core execution evidence is missing, weak, conflicting, or truncated, still return a complete structured conservative feedback object with wait or reject, null unsupported numeric fields, and explicit risk/data-quality warnings. '
            'If only auxiliary context is absent, a buy decision may still be valid when price, liquidity, trend, execution risk, and invalidation evidence are otherwise strong. '
            'Before finalizing, self-check that the JSON is parseable, all required keys are present, role and agent_name are correct, symbol and traceability.symbol exactly match the assigned symbol, operator-facing free-text fields are in Korean, and arrays are arrays of strings. '
            'Immediately before sending, validate the final raw JSON text again and correct any extra or missing closing braces/brackets so it begins with {, ends with }, and parses successfully.'
        )
    return (
        'Return JSON only. Do not wrap in markdown. '
        'Required top-level keys: role, summary, confidence, risk_flags, details, improvement_notes. '
        'role must equal the requested role. '
        'summary must be a concise string. '
        'confidence must be a number between 0 and 1. '
        'risk_flags must be an array of strings. '
        'details must be an object with structured supporting fields relevant to the role. '
        'improvement_notes must be an array of short strings describing missing inputs, ambiguities, desired fields, or algorithm/data improvements that would make the analysis better.'
    )


def normalize_agent_response(
    *,
    role: str,
    output_text: str | None,
    raw_response: dict[str, Any],
    market_date: str | date | None = None,
    symbol: str | None = None,
    agent_name: str | None = None,
) -> dict[str, Any]:
    parsed: dict[str, Any] | None = None
    if output_text:
        try:
            candidate = json.loads(output_text)
            if isinstance(candidate, dict):
                parsed = candidate
        except Exception:  # noqa: BLE001
            parsed = None

    if role == SHORT_TERM_DECIDER_AGENT_NAME:
        return _normalize_short_term_decider_response(
            role=role,
            parsed=parsed,
            output_text=output_text,
            market_date=market_date,
            symbol=symbol,
            agent_name=agent_name,
        )

    if role == SIGNAL_DECIDER_AGENT_NAME:
        return _normalize_signal_decider_response(
            role=role,
            parsed=parsed,
            output_text=output_text,
            market_date=market_date,
            symbol=symbol,
            agent_name=agent_name,
        )

    if parsed is None:
        parsed = {
            'role': role,
            'summary': output_text or '',
            'confidence': None,
            'risk_flags': ['unstructured_response'],
            'details': {
                'raw_output_text': output_text,
            },
            'improvement_notes': ['Return structured JSON with explicit missing-data and improvement notes.'],
        }

    normalized = {
        'role': parsed.get('role') or role,
        'summary': parsed.get('summary') if isinstance(parsed.get('summary'), str) else str(parsed.get('summary') or ''),
        'confidence': parsed.get('confidence') if isinstance(parsed.get('confidence'), (int, float)) else None,
        'risk_flags': parsed.get('risk_flags') if isinstance(parsed.get('risk_flags'), list) else [],
        'details': parsed.get('details') if isinstance(parsed.get('details'), dict) else {},
        'improvement_notes': parsed.get('improvement_notes') if isinstance(parsed.get('improvement_notes'), list) else [],
    }
    if normalized['role'] != role:
        normalized['details'] = {**normalized['details'], 'reported_role': normalized['role']}
        normalized['role'] = role
        normalized['risk_flags'] = list(dict.fromkeys([*normalized['risk_flags'], 'role_mismatch']))
    return normalized
