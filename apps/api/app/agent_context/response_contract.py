from __future__ import annotations

import json
from typing import Any


REQUIRED_RESPONSE_KEYS = ('role', 'summary', 'confidence', 'risk_flags', 'details', 'improvement_notes')


def build_contract_prompt_suffix() -> str:
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


def normalize_agent_response(*, role: str, output_text: str | None, raw_response: dict[str, Any]) -> dict[str, Any]:
    parsed: dict[str, Any] | None = None
    if output_text:
        try:
            candidate = json.loads(output_text)
            if isinstance(candidate, dict):
                parsed = candidate
        except Exception:  # noqa: BLE001
            parsed = None

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
