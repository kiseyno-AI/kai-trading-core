import json

import pytest

from app.agent_context.response_contract import normalize_agent_response


@pytest.mark.parametrize('role', ['eod-summarizer', 'technical-summarizer', 'fundamental-summarizer'])
@pytest.mark.parametrize('invalid_flag', [{'code': 'stale_data'}, ['stale_data']])
@pytest.mark.parametrize('reported_role', [None, 'other-role'])
def test_invalid_risk_flag_items_return_contract_failure(role, invalid_flag, reported_role):
    payload = {
        'role': reported_role or role,
        'summary': 'Stored inputs reviewed.',
        'confidence': 0.7,
        'risk_flags': ['existing_warning', invalid_flag],
        'details': {},
        'improvement_notes': [],
    }

    result = normalize_agent_response(
        role=role,
        output_text=json.dumps(payload),
        raw_response=payload,
        market_date='2026-05-08',
        symbol='005930',
        agent_name=role,
    )

    assert result['response_contract_valid'] is False
    assert 'required_field_invalid_type:risk_flags' in result['response_schema_violation_flags']
    assert 'existing_warning' in result['risk_flags']
    if reported_role:
        assert 'role_mismatch' in result['structural_mismatch_flags']


def test_valid_risk_flags_keep_existing_order_text_and_deduplication():
    payload = {
        'role': 'technical-summarizer',
        'summary': 'Stored inputs reviewed.',
        'confidence': 0.7,
        'risk_flags': ['second_warning', ' first_warning ', 'second_warning'],
        'details': {},
        'improvement_notes': [],
    }

    result = normalize_agent_response(
        role=payload['role'],
        output_text=json.dumps(payload),
        raw_response=payload,
    )

    assert result['response_contract_valid'] is True
    assert result['risk_flags'] == ['second_warning', ' first_warning ']
