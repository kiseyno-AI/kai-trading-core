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
