from app.eod.pykrx_enrichment_gate import build_pykrx_enrichment_preflight


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
    ]
