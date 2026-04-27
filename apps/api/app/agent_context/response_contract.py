from __future__ import annotations

import json
from datetime import date
from typing import Any

from app.agent_context.roles import SIGNAL_DECIDER_AGENT_NAME


REQUIRED_RESPONSE_KEYS = ('role', 'summary', 'confidence', 'risk_flags', 'details', 'improvement_notes')
SIGNAL_DECIDER_ACTIONS = {'buy', 'wait', 'reject'}


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


def _build_signal_decider_details(
    *,
    parsed_details: dict[str, Any],
    next_day_open_action: str,
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
    payload = parsed or {}
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
        if not improvement_notes:
            improvement_notes = ['Return the full structured decision-support contract in JSON.']
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

    normalized = {
        'role': role,
        'agent_name': resolved_agent_name,
        'symbol': normalized_symbol,
        'market_date': normalized_market_date,
        'preferred_action': next_day_open_action,
        'next_day_open_action': next_day_open_action,
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
        'traceability': traceability,
        'summary': thesis_summary,
        'confidence': confidence,
        'risk_flags': risk_flags,
        'details': _build_signal_decider_details(
            parsed_details=parsed_details,
            next_day_open_action=next_day_open_action,
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


def build_contract_prompt_suffix(*, role: str | None = None) -> str:
    if role == SIGNAL_DECIDER_AGENT_NAME:
        return (
            'Return JSON only. Do not wrap in markdown. '
            'For every signal-decider feedback object, required keys are: role, agent_name, symbol, market_date, preferred_action, next_day_open_action, '
            'entry_price_basis, target_price, expected_return_pct, stop_loss_price, suggested_holding_period, '
            'confidence_score_10, conviction_score_10, thesis_summary, supporting_evidence, decision_risks, '
            'invalidation_conditions, data_quality_warnings, specialist_disagreement, traceability, summary, confidence, '
            'risk_flags, details, improvement_notes. '
            'role must equal the requested role. '
            'agent_name should equal signal-decider. '
            'symbol is mandatory and must exactly equal the assigned/requested symbol string. '
            'Do not omit, alias, translate, normalize away leading zeroes, reorder, infer, or substitute symbols. '
            'traceability.symbol must exactly match symbol, and both must match the assigned item symbol. '
            'next_day_open_action and preferred_action must be one of buy, wait, reject. '
            'entry_price_basis should be next_open unless the supplied inputs justify a different explicit basis. '
            'Use null rather than fake precision when a numeric field is not well-supported by the inputs. '
            'confidence_score_10 and conviction_score_10 must be numbers from 1 to 10 when provided. '
            'thesis_summary must be a concise decision-support explanation. '
            'supporting_evidence, decision_risks, invalidation_conditions, data_quality_warnings, specialist_disagreement, '
            'risk_flags, and improvement_notes must be arrays of short strings. '
            'details must be an object with structured decision-support fields and traceability. '
            'If evidence is missing, weak, conflicting, or truncated, still return a complete structured conservative feedback object with wait or reject, null unsupported numeric fields, and explicit risk/data-quality warnings. '
            'Before finalizing, self-check that the JSON is parseable, all required keys are present, role and agent_name are correct, symbol and traceability.symbol exactly match the assigned symbol, and arrays are arrays of strings.'
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
