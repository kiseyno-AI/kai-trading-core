from __future__ import annotations

import pytest

from app.eod.daily_pipeline_outcomes import _has_exact_scope_outcome_manifest
from app.eod.daily_pipeline_stages import DailyPipelineStageService
from app.eod.market_membership import market_membership_sha256
from app.eod.pipeline_status import EODPipelineStatusService
from app.eod.pykrx_pipeline_runner import (
    _build_pykrx_invocation_scope_proof,
    _validated_exact_piotroski_membership_symbols,
    _validated_exact_scope_outcome,
)
from app.eod.scope_outcomes import (
    build_exact_scope_partition_identity_proof,
    build_exact_symbol_scope_outcome,
    validate_exact_symbol_scope_outcome,
)


ALPHA_CODE = '1234A5'
MALFORMED_CODE = 'ABCDEF'
MARKET_DATE = '2026-10-30'


def _exact_result(symbol: str) -> dict:
    return {
        'status': 'completed',
        'market_date': MARKET_DATE,
        **build_exact_symbol_scope_outcome(
            manifest_version='dart_fundamentals_scope_outcomes_v1',
            selected_symbols=[symbol],
            successful_symbols=[symbol],
            retry_debt_symbols=[],
        ),
    }


def _membership_snapshot(symbol: str) -> dict:
    membership = {
        'schema_version': 'pykrx_market_membership_v1',
        'membership_basis': 'end_date_snapshot',
        'symbol_count': 1,
        'markets': {
            'KOSPI': {
                'symbols': [symbol],
                'symbol_count': 1,
                'membership_sha256': market_membership_sha256([symbol]),
            }
        },
    }
    return {
        'source': 'pykrx',
        'complete': True,
        'latest_trade_date': MARKET_DATE,
        'membership_basis': 'end_date_snapshot',
        'markets': ['KOSPI'],
        'unavailable_markets': [],
        'universe_source': 'pykrx_master',
        'symbol_count': 1,
        'market_membership': membership,
    }


def test_exact_scope_builder_canonicalizes_alpha_and_rejects_malformed() -> None:
    outcome = build_exact_symbol_scope_outcome(
        manifest_version='factor_scope_outcomes_v1',
        selected_symbols=[ALPHA_CODE.lower(), '5930'],
        successful_symbols=[ALPHA_CODE.lower(), '5930'],
        retry_debt_symbols=[],
    )

    assert outcome['selected_stock_codes'] == [ALPHA_CODE, '005930']
    assert validate_exact_symbol_scope_outcome(
        result={'status': 'completed', **outcome},
        invocation_symbols=[ALPHA_CODE, '005930'],
    ) is not None
    with pytest.raises(ValueError, match='canonical KRX short codes'):
        build_exact_symbol_scope_outcome(
            manifest_version='factor_scope_outcomes_v1',
            selected_symbols=[MALFORMED_CODE],
            successful_symbols=[MALFORMED_CODE],
            retry_debt_symbols=[],
        )


def test_raw_scope_validators_reject_internally_consistent_malformed_code() -> None:
    malformed = {
        'status': 'completed',
        'scope_outcome_manifest_version': 'factor_scope_outcomes_v1',
        'selected_stock_codes': [MALFORMED_CODE],
        'successful_stock_codes': [MALFORMED_CODE],
        'retry_debt_stock_codes': [],
        'terminal_not_applicable_stock_codes': [],
        'scope_outcome_manifest': [
            {'stock_code': MALFORMED_CODE, 'outcome': 'success'}
        ],
    }

    assert validate_exact_symbol_scope_outcome(
        result=malformed,
        invocation_symbols=[MALFORMED_CODE],
    ) is None
    assert _validated_exact_scope_outcome(
        result=malformed,
        invocation_symbols=[MALFORMED_CODE],
    ) is None


def test_compact_scope_proof_preserves_alpha_and_rejects_malformed() -> None:
    valid_result = _exact_result(ALPHA_CODE)
    proof = DailyPipelineStageService._build_exact_scope_outcome_proof(
        valid_result,
        stage_id='fundamentals_daily_refresh',
        invocation_symbols=[ALPHA_CODE],
        expected_market_date=MARKET_DATE,
    )

    assert proof is not None
    assert proof['selected_stock_codes'] == [ALPHA_CODE]
    assert _has_exact_scope_outcome_manifest(
        ({'exact_scope_outcome_proof': proof},),
        expected_stage_id='fundamentals_daily_refresh',
        expected_market_date=MARKET_DATE,
    )

    malformed_proof = {
        'schema_version': 'exact_scope_outcome_proof_v2',
        'stage_id': 'fundamentals_daily_refresh',
        'market_date': MARKET_DATE,
        'outcome_status': 'completed',
        'manifest_version': 'dart_fundamentals_scope_outcomes_v1',
        'selected_stock_codes': [MALFORMED_CODE],
        'selected_symbol_count': 1,
        'selected_symbol_sha256': market_membership_sha256(
            [MALFORMED_CODE]
        ),
        'successful_symbol_count': 1,
        'successful_symbol_sha256': market_membership_sha256(
            [MALFORMED_CODE]
        ),
        'retry_debt_stock_codes': [],
        'terminal_not_applicable_stock_codes': [],
        **build_exact_scope_partition_identity_proof(
            retry_debt_symbols=[],
            terminal_not_applicable_symbols=[],
        ),
    }
    assert not _has_exact_scope_outcome_manifest(
        ({'exact_scope_outcome_proof': malformed_proof},),
        expected_stage_id='fundamentals_daily_refresh',
        expected_market_date=MARKET_DATE,
    )


def test_invocation_and_membership_proofs_fail_closed_on_malformed_codes() -> None:
    invocation_proof = _build_pykrx_invocation_scope_proof(
        [ALPHA_CODE.lower(), '5930']
    )
    assert invocation_proof['invocation_scope_stock_codes'] == [
        ALPHA_CODE,
        '005930',
    ]
    assert DailyPipelineStageService._validated_pykrx_invocation_scope(
        invocation_proof
    ) == [ALPHA_CODE, '005930']
    with pytest.raises(ValueError, match='canonical KRX short codes'):
        _build_pykrx_invocation_scope_proof([MALFORMED_CODE])

    assert _validated_exact_piotroski_membership_symbols(
        universe_snapshot=_membership_snapshot(ALPHA_CODE),
        market_date=MARKET_DATE,
        markets=['KOSPI'],
    ) == [ALPHA_CODE]
    assert _validated_exact_piotroski_membership_symbols(
        universe_snapshot=_membership_snapshot(MALFORMED_CODE),
        market_date=MARKET_DATE,
        markets=['KOSPI'],
    ) is None


def test_pipeline_status_symbol_normalization_excludes_malformed_codes() -> None:
    assert EODPipelineStatusService._normalize_symbol_list(
        [ALPHA_CODE.lower(), '5930', MALFORMED_CODE]
    ) == [ALPHA_CODE, '005930']
