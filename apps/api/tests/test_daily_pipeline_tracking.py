from app.eod.daily_pipeline_tracking import _market_risk_filter_coverage_gate


def test_market_risk_filter_coverage_gate_blocks_collapsed_requested_scope() -> None:
    result = {
        'requested_symbols': [f'{index:06d}' for index in range(1, 81)],
        'results_persisted': 1,
        'missing_symbols': [f'{index:06d}' for index in range(2, 81)],
    }

    gate = _market_risk_filter_coverage_gate(result)

    assert gate['gate_status'] == 'blocked'
    assert gate['gate_reason'] == 'market_risk_filter_coverage_below_minimum'
    assert gate['requested_symbol_count'] == 80
    assert gate['results_persisted'] == 1


def test_market_risk_filter_coverage_gate_allows_high_requested_scope_coverage() -> None:
    result = {
        'requested_symbols': [f'{index:06d}' for index in range(1, 81)],
        'results_persisted': 79,
        'missing_symbols': ['000080'],
    }

    gate = _market_risk_filter_coverage_gate(result)

    assert gate['gate_status'] == 'ready'
    assert gate['blocking_status'] == 'non_blocking'
