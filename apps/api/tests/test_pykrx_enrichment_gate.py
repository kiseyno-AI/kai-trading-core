from app.eod.pykrx_enrichment_gate import build_pykrx_enrichment_preflight
from app.eod.pykrx_pipeline_runner import (
    _build_pykrx_enrichment_symbol_scope_outcome,
    _partition_pykrx_enrichment_category_gaps,
)


def test_pykrx_enrichment_preflight_allows_required_complete_categories():
    result = build_pykrx_enrichment_preflight(
        market_date='2026-04-29',
        coverage={
            'requested_symbol_count': 2,
            'requested_market_count': 1,
            'categories': {
                'sector_classification': {
                    'scope_type': 'symbol',
                    'expected_rows': 2,
                    'present_rows': 2,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'investor_flow_symbol': {
                    'scope_type': 'symbol',
                    'expected_rows': 2,
                    'present_rows': 2,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'short_selling': {
                    'scope_type': 'symbol',
                    'expected_rows': 2,
                    'present_rows': 2,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'valuation_snapshot': {
                    'scope_type': 'symbol',
                    'expected_rows': 2,
                    'present_rows': 2,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
            },
        },
    )

    assert result['status'] == 'ready'
    assert result['allowed'] is True
    assert result['failed_categories'] == []
    assert result['category_row_counts'] == {
        'sector_classification': 2,
        'investor_flow_symbol': 2,
        'short_selling': 2,
        'valuation_snapshot': 2,
    }


def test_pykrx_enrichment_preflight_blocks_zero_required_category_coverage():
    result = build_pykrx_enrichment_preflight(
        market_date='2026-04-29',
        coverage={
            'requested_symbol_count': 2,
            'requested_market_count': 1,
            'categories': {
                'sector_classification': {
                    'scope_type': 'symbol',
                    'expected_rows': 2,
                    'present_rows': 2,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'investor_flow_symbol': {
                    'scope_type': 'symbol',
                    'expected_rows': 2,
                    'present_rows': 0,
                    'coverage_ratio': 0.0,
                    'coverage_status': 'missing',
                },
                'valuation_snapshot': {
                    'scope_type': 'symbol',
                    'expected_rows': 2,
                    'present_rows': 2,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'short_selling': {
                    'scope_type': 'symbol',
                    'expected_rows': 2,
                    'present_rows': 0,
                    'coverage_ratio': 0.0,
                    'coverage_status': 'missing',
                },
            },
        },
    )

    assert result['status'] == 'blocked'
    assert result['allowed'] is False
    assert result['failed_categories'] == ['investor_flow_symbol', 'short_selling']
    assert result['zero_coverage_categories'] == ['investor_flow_symbol', 'short_selling']


def test_pykrx_enrichment_preflight_blocks_low_partial_required_category_coverage():
    result = build_pykrx_enrichment_preflight(
        market_date='2026-04-29',
        coverage={
            'requested_symbol_count': 100,
            'requested_market_count': 1,
            'categories': {
                'sector_classification': {
                    'scope_type': 'symbol',
                    'expected_rows': 100,
                    'present_rows': 100,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'investor_flow_symbol': {
                    'scope_type': 'symbol',
                    'expected_rows': 100,
                    'present_rows': 3,
                    'coverage_ratio': 0.03,
                    'coverage_status': 'partial',
                },
                'valuation_snapshot': {
                    'scope_type': 'symbol',
                    'expected_rows': 100,
                    'present_rows': 100,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'short_selling': {
                    'scope_type': 'symbol',
                    'expected_rows': 100,
                    'present_rows': 2,
                    'coverage_ratio': 0.02,
                    'coverage_status': 'partial',
                },
            },
        },
    )

    assert result['status'] == 'blocked'
    assert result['allowed'] is False
    assert [item['category'] for item in result['blocked_categories']] == [
        'investor_flow_symbol',
        'short_selling',
    ]
    assert result['failed_categories'] == [
        'investor_flow_symbol',
        'short_selling',
    ]
    assert result['warning_categories'] == []


def test_pykrx_enrichment_preflight_waits_when_eligible_universe_is_unavailable():
    result = build_pykrx_enrichment_preflight(
        market_date='2026-09-18',
        coverage={
            'categories': {
                'sector_classification': {
                    'scope_type': 'symbol',
                    'expected_rows': 2,
                    'present_rows': 2,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'investor_flow_symbol': {
                    'scope_type': 'symbol',
                    'expected_rows': None,
                    'present_rows': 0,
                    'coverage_ratio': None,
                    'coverage_status': 'unavailable',
                    'expectation_status': 'unavailable',
                },
                'valuation_snapshot': {
                    'scope_type': 'symbol',
                    'expected_rows': 2,
                    'present_rows': 2,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'short_selling': {
                    'scope_type': 'symbol',
                    'expected_rows': None,
                    'present_rows': 0,
                    'coverage_ratio': None,
                    'coverage_status': 'unavailable',
                    'expectation_status': 'unavailable',
                },
            },
        },
    )

    assert result['status'] == 'waiting'
    assert result['allowed'] is False
    assert result['failed_categories'] == ['investor_flow_symbol', 'short_selling']
    assert {
        item['reason'] for item in result['waiting_categories']
    } == {'coverage_expectation_unavailable'}


def test_pykrx_enrichment_preflight_waits_on_ambiguous_zero_row_expectation():
    result = build_pykrx_enrichment_preflight(
        market_date='2026-09-18',
        coverage={
            'categories': {
                category: {
                    'scope_type': 'symbol',
                    'expected_rows': 0 if category == 'short_selling' else 2,
                    'present_rows': 0 if category == 'short_selling' else 2,
                    'coverage_ratio': None if category == 'short_selling' else 1.0,
                    'coverage_status': 'missing' if category == 'short_selling' else 'complete',
                    'expectation_status': 'available',
                }
                for category in (
                    'sector_classification',
                    'investor_flow_symbol',
                    'short_selling',
                    'valuation_snapshot',
                )
            },
        },
    )

    assert result['status'] == 'waiting'
    assert result['allowed'] is False
    assert result['failed_categories'] == ['short_selling']
    assert result['waiting_categories'][0]['reason'] == 'coverage_expectation_unavailable'


def test_pykrx_enrichment_preflight_uses_row_counts_when_reported_ratio_is_stale():
    result = build_pykrx_enrichment_preflight(
        market_date='2026-04-29',
        coverage={
            'categories': {
                category: {
                    'scope_type': 'symbol',
                    'expected_rows': 100,
                    'present_rows': 3 if category == 'investor_flow_symbol' else 100,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'partial' if category == 'investor_flow_symbol' else 'complete',
                }
                for category in (
                    'sector_classification',
                    'investor_flow_symbol',
                    'short_selling',
                    'valuation_snapshot',
                )
            },
        },
    )

    assert result['status'] == 'blocked'
    assert result['allowed'] is False
    assert result['failed_categories'] == ['investor_flow_symbol']
    assert result['category_gates'][1]['coverage_ratio'] == 0.03


def test_pykrx_enrichment_preflight_allows_partial_coverage_at_minimum_threshold():
    result = build_pykrx_enrichment_preflight(
        market_date='2026-04-29',
        coverage={
            'categories': {
                category: {
                    'scope_type': 'symbol',
                    'expected_rows': 100,
                    'present_rows': 99,
                    'coverage_ratio': 0.99,
                    'coverage_status': 'partial',
                }
                for category in (
                    'sector_classification',
                    'investor_flow_symbol',
                    'short_selling',
                    'valuation_snapshot',
                )
            },
        },
    )

    assert result['status'] == 'ready'
    assert result['allowed'] is True
    assert result['failed_categories'] == []
    assert [item['category'] for item in result['warning_categories']] == [
        'sector_classification',
        'investor_flow_symbol',
        'short_selling',
        'valuation_snapshot',
    ]


def test_pykrx_enrichment_preflight_blocks_shell_rows_without_decision_usable_short_core():
    categories = {
        category: {
            'scope_type': 'symbol',
            'expected_rows': 2767,
            'present_rows': 2767,
            'coverage_ratio': 1.0,
            'coverage_status': 'complete',
            'decision_usable_present_rows': (
                0 if category == 'short_selling' else 2767
            ),
            'decision_usable_ratio': (
                0.0 if category == 'short_selling' else 1.0
            ),
            'decision_usable_status': (
                'missing' if category == 'short_selling' else 'complete'
            ),
            'complete_present_rows': 0 if category == 'short_selling' else 2767,
            'completeness_ratio': 0.0 if category == 'short_selling' else 1.0,
            'completeness_status': 'missing' if category == 'short_selling' else 'complete',
        }
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
    }

    result = build_pykrx_enrichment_preflight(
        market_date='2026-09-18',
        coverage={'categories': categories},
    )

    assert result['status'] == 'blocked'
    assert result['allowed'] is False
    assert result['failed_categories'] == ['short_selling']
    assert result['warning_categories'] == []
    assert result['category_row_counts']['short_selling'] == 2767
    assert result['category_decision_usable_row_counts']['short_selling'] == 0
    assert result['zero_coverage_categories'] == []
    assert result['zero_decision_usable_categories'] == ['short_selling']
    short_gate = result['blocked_categories'][0]
    assert short_gate['reason'] == 'missing_required_pykrx_enrichment_decision_evidence'
    assert short_gate['decision_usable_status'] == 'missing'
    assert short_gate['missing_decision_usable_rows'] == 2767
    assert short_gate['missing_complete_rows'] == 2767


def test_pykrx_enrichment_preflight_accepts_short_core_and_warns_on_optional_balance_gap():
    categories = {
        category: {
            'scope_type': 'symbol',
            'expected_rows': 2767,
            'present_rows': 2767,
            'coverage_ratio': 1.0,
            'coverage_status': 'complete',
            'decision_usable_present_rows': 2767,
            'decision_usable_ratio': 1.0,
            'decision_usable_status': 'complete',
            'complete_present_rows': (
                0 if category == 'short_selling' else 2767
            ),
            'completeness_ratio': (
                0.0 if category == 'short_selling' else 1.0
            ),
            'completeness_status': (
                'missing' if category == 'short_selling' else 'complete'
            ),
        }
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
    }

    result = build_pykrx_enrichment_preflight(
        market_date='2026-09-18',
        coverage={'categories': categories},
    )

    assert result['status'] == 'ready'
    assert result['allowed'] is True
    assert result['failed_categories'] == []
    assert result['zero_decision_usable_categories'] == []
    assert [item['category'] for item in result['warning_categories']] == [
        'short_selling'
    ]
    short_gate = result['warning_categories'][0]
    assert short_gate['reason'] == (
        'required_pykrx_enrichment_optional_components_incomplete'
    )
    assert short_gate['decision_usable_rows'] == 2767
    assert short_gate['complete_present_rows'] == 0


def test_legacy_coverage_falls_back_only_when_decision_usable_fields_are_absent():
    categories = {
        category: {
            'scope_type': 'symbol',
            'expected_rows': 100,
            'present_rows': 100,
            'coverage_ratio': 1.0,
            'coverage_status': 'complete',
            'complete_present_rows': (
                0 if category == 'short_selling' else 100
            ),
            'completeness_ratio': (
                0.0 if category == 'short_selling' else 1.0
            ),
            'completeness_status': (
                'missing' if category == 'short_selling' else 'complete'
            ),
        }
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
    }

    result = build_pykrx_enrichment_preflight(
        market_date='2026-09-18',
        coverage={'categories': categories},
    )

    assert result['status'] == 'ready'
    assert result['allowed'] is True
    assert [item['category'] for item in result['warning_categories']] == [
        'short_selling'
    ]
    short_gate = result['warning_categories'][0]
    assert short_gate['decision_usable_evidence_available'] is False
    assert short_gate['decision_usable_rows'] == 100


def test_valuation_gate_accepts_documented_provider_universe_coverage() -> None:
    categories = {
        category: {
            'scope_type': 'symbol',
            'expected_rows': 100,
            'present_rows': 96 if category == 'valuation_snapshot' else 100,
            'coverage_ratio': 0.96 if category == 'valuation_snapshot' else 1.0,
            'coverage_status': 'partial' if category == 'valuation_snapshot' else 'complete',
        }
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
    }

    result = build_pykrx_enrichment_preflight(
        market_date='2026-09-18',
        coverage={'categories': categories},
    )

    assert result['status'] == 'ready'
    assert result['allowed'] is True
    assert [item['category'] for item in result['warning_categories']] == ['valuation_snapshot']


def test_valuation_gate_applies_documented_floor_to_decision_usable_rows() -> None:
    categories = {
        category: {
            'scope_type': 'symbol',
            'expected_rows': 100,
            'present_rows': 100,
            'coverage_ratio': 1.0,
            'coverage_status': 'complete',
            'decision_usable_present_rows': (
                96 if category == 'valuation_snapshot' else 100
            ),
            'decision_usable_ratio': (
                0.96 if category == 'valuation_snapshot' else 1.0
            ),
            'decision_usable_status': (
                'partial' if category == 'valuation_snapshot' else 'complete'
            ),
            'complete_present_rows': (
                96 if category == 'valuation_snapshot' else 100
            ),
            'completeness_ratio': (
                0.96 if category == 'valuation_snapshot' else 1.0
            ),
            'completeness_status': (
                'partial' if category == 'valuation_snapshot' else 'complete'
            ),
        }
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
    }

    result = build_pykrx_enrichment_preflight(
        market_date='2026-09-18',
        coverage={'categories': categories},
    )

    assert result['status'] == 'ready'
    assert result['allowed'] is True
    assert result['failed_categories'] == []
    assert [item['category'] for item in result['warning_categories']] == [
        'valuation_snapshot'
    ]
    assert result['category_decision_usable_row_counts'][
        'valuation_snapshot'
    ] == 96


def test_runner_advances_short_core_when_only_optional_balance_is_missing() -> None:
    symbols = ['005930', '000660']
    categories = {
        category: {
            'scope_type': 'symbol',
            'expected_rows': 2,
            'present_rows': 2,
            'coverage_ratio': 1.0,
            'coverage_status': 'complete',
            'decision_usable_present_rows': 2,
            'decision_usable_ratio': 1.0,
            'decision_usable_status': 'complete',
            'complete_present_rows': 0 if category == 'short_selling' else 2,
            'completeness_ratio': 0.0 if category == 'short_selling' else 1.0,
            'completeness_status': 'missing' if category == 'short_selling' else 'complete',
            'expected_scope_keys': symbols,
            'decision_usable_scope_keys': symbols,
            'complete_scope_keys': [] if category == 'short_selling' else symbols,
        }
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
    }
    preflight = build_pykrx_enrichment_preflight(
        market_date='2026-10-30',
        coverage={'categories': categories},
    )

    blocking, tolerated = _partition_pykrx_enrichment_category_gaps(
        failed_categories=preflight['failed_categories'],
        preflight=preflight,
    )
    outcome, retry_reasons = _build_pykrx_enrichment_symbol_scope_outcome(
        selected_symbols=symbols,
        coverage_categories=categories,
    )

    assert blocking == []
    assert tolerated == []
    assert outcome['successful_stock_codes'] == symbols
    assert outcome['retry_debt_stock_codes'] == []
    assert retry_reasons == {}


def test_runner_keeps_retry_debt_when_short_core_is_not_decision_usable() -> None:
    symbols = ['005930', '000660']
    categories = {
        category: {
            'scope_type': 'symbol',
            'expected_rows': 2,
            'present_rows': 2,
            'coverage_ratio': 1.0,
            'coverage_status': 'complete',
            'decision_usable_present_rows': (
                0 if category == 'short_selling' else 2
            ),
            'decision_usable_ratio': (
                0.0 if category == 'short_selling' else 1.0
            ),
            'decision_usable_status': (
                'missing' if category == 'short_selling' else 'complete'
            ),
            'complete_present_rows': 0 if category == 'short_selling' else 2,
            'completeness_ratio': 0.0 if category == 'short_selling' else 1.0,
            'completeness_status': 'missing' if category == 'short_selling' else 'complete',
            'expected_scope_keys': symbols,
            'decision_usable_scope_keys': (
                [] if category == 'short_selling' else symbols
            ),
            'complete_scope_keys': [] if category == 'short_selling' else symbols,
        }
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
            'valuation_snapshot',
        )
    }
    preflight = build_pykrx_enrichment_preflight(
        market_date='2026-10-30',
        coverage={'categories': categories},
    )

    blocking, tolerated = _partition_pykrx_enrichment_category_gaps(
        failed_categories=preflight['failed_categories'],
        preflight=preflight,
    )
    outcome, retry_reasons = _build_pykrx_enrichment_symbol_scope_outcome(
        selected_symbols=symbols,
        coverage_categories=categories,
    )

    assert blocking == ['short_selling']
    assert tolerated == []
    assert outcome['successful_stock_codes'] == []
    assert outcome['retry_debt_stock_codes'] == symbols
    assert retry_reasons == {
        symbol: ['short_selling:incomplete_required_coverage']
        for symbol in symbols
    }
