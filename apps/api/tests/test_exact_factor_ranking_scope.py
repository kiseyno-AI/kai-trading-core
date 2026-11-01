from __future__ import annotations

from unittest.mock import patch

import pytest

from app.signals.advanced_value_factor import DailyAdvancedValueFactorService
from app.signals.beta_factor import DailyBetaFactorService
from app.signals.composite_factor import DailyCompositeFactorService
from app.signals.low_risk_factor import DailyLowRiskFactorService
from app.signals.momentum_factor import DailyMomentumFactorService
from app.signals.piotroski_filter import DailyPiotroskiFilterService
from app.signals.quality_factor import DailyQualityFactorService
from app.signals.unified_value_factor import DailyUnifiedValueFactorService
from app.signals.value_factor import DailyValueFactorService


EXACT_SYMBOLS = ['000001']


@pytest.mark.parametrize(
    ('service', 'module_name', 'bulk_upsert_name', 'patch_load_symbols'),
    [
        (
            DailyValueFactorService(),
            'app.signals.value_factor',
            'upsert_daily_value_factor_results',
            False,
        ),
        (
            DailyLowRiskFactorService(),
            'app.signals.low_risk_factor',
            'upsert_daily_low_risk_factor_results',
            False,
        ),
        (
            DailyBetaFactorService(),
            'app.signals.beta_factor',
            'upsert_daily_beta_factor_results',
            False,
        ),
        (
            DailyAdvancedValueFactorService(),
            'app.signals.advanced_value_factor',
            'upsert_daily_advanced_value_factor_results',
            False,
        ),
        (
            DailyUnifiedValueFactorService(),
            'app.signals.unified_value_factor',
            'upsert_daily_unified_value_factor_results',
            True,
        ),
        (
            DailyQualityFactorService(),
            'app.signals.quality_factor',
            'upsert_daily_quality_factor_results',
            False,
        ),
    ],
)
def test_value_style_services_never_reload_global_scope_when_exact_scope_given(
    service,
    module_name: str,
    bulk_upsert_name: str,
    patch_load_symbols: bool,
) -> None:
    observed_scopes: list[list[str] | None] = []

    def load_inputs(*, market_date, symbols):
        del market_date
        observed_scopes.append(symbols)
        if symbols is None:
            raise AssertionError('global ranking universe must not be loaded')
        return [], [], list(symbols)

    patches = [
        patch.object(service, '_load_inputs', side_effect=load_inputs),
        patch(f'{module_name}.{bulk_upsert_name}', return_value=0),
    ]
    if patch_load_symbols:
        patches.append(
            patch(
                f'{module_name}.load_symbols',
                return_value=[{'symbol': '000001'}],
            )
        )

    entered = []
    try:
        for patcher in patches:
            entered.append(patcher)
            patcher.start()
        service.rebuild_for_market_date(
            market_date='2026-09-23',
            symbols=EXACT_SYMBOLS,
            ranking_symbols=EXACT_SYMBOLS,
        )
    finally:
        for patcher in reversed(entered):
            patcher.stop()

    assert observed_scopes
    assert all(scope == EXACT_SYMBOLS for scope in observed_scopes)


def test_momentum_exact_scope_does_not_load_available_global_symbols() -> None:
    service = DailyMomentumFactorService()
    with (
        patch(
            'app.signals.momentum_factor.list_symbols_with_daily_bar_on_date',
            side_effect=AssertionError('global universe must not be loaded'),
        ),
        patch(
            'app.signals.momentum_factor.load_recent_price_bars_by_symbol',
            return_value={},
        ) as load_histories,
        patch(
            'app.signals.momentum_factor.upsert_daily_momentum_factor_results',
            return_value=0,
        ),
    ):
        service.rebuild_for_market_date(
            market_date='2026-09-23',
            symbols=EXACT_SYMBOLS,
            ranking_symbols=EXACT_SYMBOLS,
        )

    assert load_histories.call_args.kwargs['symbols'] == EXACT_SYMBOLS


def test_piotroski_exact_scope_is_used_as_input_and_ranking_population() -> None:
    service = DailyPiotroskiFilterService()
    observed_scopes: list[list[str] | None] = []

    def load_inputs(_market_date, *, symbols):
        observed_scopes.append(symbols)
        return []

    with patch.object(service, '_load_inputs', side_effect=load_inputs):
        service.rebuild_for_market_date(
            market_date='2026-09-23',
            symbols=EXACT_SYMBOLS,
            ranking_symbols=EXACT_SYMBOLS,
        )

    assert observed_scopes == [EXACT_SYMBOLS]


def test_composite_loads_only_exact_factor_rows_for_ranking() -> None:
    service = DailyCompositeFactorService()
    factor_rows = {
        'momentum': {},
        'value_v1': {},
        'advanced_value': {},
        'low_risk': {},
        'beta': {},
    }
    with (
        patch(
            'app.signals.composite_factor.list_symbols_with_daily_bar_on_date',
            return_value=EXACT_SYMBOLS,
        ),
        patch.object(
            service,
            '_load_factor_rows',
            return_value=factor_rows,
        ) as load_factor_rows,
        patch('app.signals.composite_factor.load_symbols', return_value=[]),
        patch(
            'app.signals.composite_factor.upsert_daily_composite_factor_results',
            return_value=1,
        ),
    ):
        service.rebuild_for_market_date(
            market_date='2026-09-23',
            symbols=EXACT_SYMBOLS,
            ranking_symbols=EXACT_SYMBOLS,
        )

    assert load_factor_rows.call_args.kwargs['symbols'] == EXACT_SYMBOLS
