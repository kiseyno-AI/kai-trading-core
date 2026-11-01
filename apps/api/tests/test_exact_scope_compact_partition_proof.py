from __future__ import annotations

from copy import deepcopy

from app.eod.daily_pipeline_outcomes import (
    daily_pipeline_outcome_is_satisfied,
)
from app.eod.daily_pipeline_stages import DailyPipelineStageService
from app.eod.market_membership import market_membership_sha256
from app.eod.pykrx_backfill import _followup_exact_result_authority
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome


def _compact_factor_details() -> tuple[list[str], dict]:
    symbols = [f'{index:06d}' for index in range(1, 201)]
    successful = symbols[:-2]
    retry_debt = symbols[-2:-1]
    terminal_not_applicable = symbols[-1:]
    result = {
        'status': 'partial',
        'market_date': '2026-10-30',
        'symbols_total': len(symbols),
        'symbols_succeeded': len(successful),
        'symbols_failed': len(retry_debt),
        'remaining_retryable_events': 0,
        'scope_finalization': {
            'stage': 'momentum_factors',
            'market_date': '2026-10-30',
            'exact_symbol_count': len(symbols),
            'exact_membership_sha256': market_membership_sha256(symbols),
            'successful_symbol_count': len(successful),
            'successful_membership_sha256': market_membership_sha256(
                successful
            ),
            'deleted_stale_row_count': 0,
            'ready': True,
        },
        **build_exact_symbol_scope_outcome(
            manifest_version='factor_scope_outcomes_v1',
            selected_symbols=symbols,
            successful_symbols=successful,
            retry_debt_symbols=retry_debt,
            terminal_not_applicable_symbols=terminal_not_applicable,
        ),
    }
    details = DailyPipelineStageService._build_stage_run_details(
        stage_id='momentum_factors',
        result=result,
        invocation_symbols=symbols,
        market_date='2026-10-30',
        publication_symbols=symbols,
    )
    return symbols, details


def test_compact_proof_rejects_retry_terminal_identity_swap() -> None:
    symbols, details = _compact_factor_details()
    proof = details['exact_scope_outcome_proof']
    assert proof['retry_debt_symbol_count'] == 1
    assert proof['terminal_not_applicable_symbol_count'] == 1
    assert 'selected_stock_codes' not in details['result_summary']

    outcome = {
        'stage': 'momentum_factors',
        'status': 'partial',
        'details': details,
    }
    stage_row = {'status': 'partial', 'details': details}
    assert DailyPipelineStageService._exact_partial_success_scope(
        outcome={**outcome, 'result': {'status': 'partial'}},
        invocation_symbols=None,
        expected_stage_id='momentum_factors',
        expected_market_date='2026-10-30',
        publication_symbols=symbols,
    ) is not None
    assert daily_pipeline_outcome_is_satisfied(outcome)
    assert _followup_exact_result_authority(
        stage_id='momentum_factors',
        stage=stage_row,
    ) is not None

    tampered_details = deepcopy(details)
    tampered_proof = tampered_details['exact_scope_outcome_proof']
    (
        tampered_proof['retry_debt_stock_codes'],
        tampered_proof['terminal_not_applicable_stock_codes'],
    ) = (
        tampered_proof['terminal_not_applicable_stock_codes'],
        tampered_proof['retry_debt_stock_codes'],
    )
    tampered_outcome = {
        'stage': 'momentum_factors',
        'status': 'partial',
        'result': {'status': 'partial'},
        'details': tampered_details,
    }
    tampered_stage_row = {
        'status': 'partial',
        'details': tampered_details,
    }

    assert DailyPipelineStageService._exact_partial_success_scope(
        outcome=tampered_outcome,
        invocation_symbols=None,
        expected_stage_id='momentum_factors',
        expected_market_date='2026-10-30',
        publication_symbols=symbols,
    ) is None
    assert not daily_pipeline_outcome_is_satisfied(tampered_outcome)
    assert _followup_exact_result_authority(
        stage_id='momentum_factors',
        stage=tampered_stage_row,
    ) is None
