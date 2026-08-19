from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Callable, Generic, Iterable, TypeVar


@dataclass(frozen=True, slots=True)
class USScreeningPolicy:
    minimum_price: Decimal = Decimal('5')
    minimum_history_days: int = 252
    minimum_median_dollar_volume_20d: Decimal = Decimal('10000000')
    minimum_median_volume_20d: Decimal = Decimal('200000')
    maximum_volatility_60d: Decimal = Decimal('1.00')
    maximum_drawdown_120d: Decimal = Decimal('-0.60')
    maximum_return_5d: Decimal = Decimal('0.25')
    price_candidate_cap: int = 300
    maximum_periodic_filing_age_days: int = 150
    maximum_liabilities_to_assets: Decimal = Decimal('0.85')
    maximum_shares_outstanding_growth_yoy: Decimal = Decimal('0.10')
    fundamental_candidate_cap: int = 150


@dataclass(frozen=True, slots=True)
class USPriceScreenMetrics:
    symbol: str
    latest_price: Decimal | None
    history_days: int
    median_dollar_volume_20d: Decimal | None
    median_volume_20d: Decimal | None
    volatility_60d: Decimal | None
    max_drawdown_120d: Decimal | None
    momentum_6m: Decimal | None
    momentum_12m: Decimal | None
    return_5d: Decimal | None


@dataclass(frozen=True, slots=True)
class USFundamentalScreenMetrics:
    symbol: str
    latest_periodic_filing_date: date | None
    ttm_revenue: Decimal | None
    ttm_operating_cash_flow: Decimal | None
    ttm_net_income: Decimal | None
    total_assets: Decimal | None
    total_liabilities: Decimal | None
    shares_outstanding_growth_yoy: Decimal | None
    has_bankruptcy_or_delinquency_flag: bool


MetricT = TypeVar('MetricT')


@dataclass(frozen=True, slots=True)
class USScreeningResult(Generic[MetricT]):
    selected: tuple[MetricT, ...]
    scores: dict[str, Decimal]
    exclusions: dict[str, tuple[str, ...]]
    eligible_before_cap: int
    cap: int


def _percentile_scores(
    rows: list[MetricT],
    getter: Callable[[MetricT], Decimal | None],
    *,
    higher_is_better: bool,
) -> dict[str, Decimal]:
    available = [row for row in rows if getter(row) is not None]
    if not available:
        return {}
    ordered = sorted(
        available,
        key=lambda row: (
            -(getter(row) or Decimal('0')) if higher_is_better else (getter(row) or Decimal('0')),
            str(getattr(row, 'symbol')),
        ),
    )
    if len(ordered) == 1:
        return {str(getattr(ordered[0], 'symbol')): Decimal('1')}
    denominator = Decimal(len(ordered) - 1)
    result: dict[str, Decimal] = {}
    group_start = 0
    while group_start < len(ordered):
        group_value = getter(ordered[group_start])
        group_end = group_start + 1
        while group_end < len(ordered) and getter(ordered[group_end]) == group_value:
            group_end += 1
        average_rank = (Decimal(group_start) + Decimal(group_end - 1)) / Decimal('2')
        percentile = Decimal('1') - (average_rank / denominator)
        for row in ordered[group_start:group_end]:
            result[str(getattr(row, 'symbol'))] = percentile
        group_start = group_end
    return result


def _combined_scores(rows: list[MetricT], components: tuple[tuple[Callable[[MetricT], Decimal | None], bool], ...]) -> dict[str, Decimal]:
    component_scores = [
        _percentile_scores(rows, getter, higher_is_better=higher_is_better)
        for getter, higher_is_better in components
    ]
    scores: dict[str, Decimal] = {}
    for row in rows:
        symbol = str(getattr(row, 'symbol'))
        values = [scores_by_symbol[symbol] for scores_by_symbol in component_scores if symbol in scores_by_symbol]
        scores[symbol] = sum(values, Decimal('0')) / Decimal(len(values)) if values else Decimal('0')
    return scores


def screen_us_price_candidates(
    metrics: Iterable[USPriceScreenMetrics],
    *,
    policy: USScreeningPolicy | None = None,
) -> USScreeningResult[USPriceScreenMetrics]:
    resolved_policy = policy or USScreeningPolicy()
    eligible: list[USPriceScreenMetrics] = []
    exclusions: dict[str, tuple[str, ...]] = {}
    for item in sorted(metrics, key=lambda value: value.symbol):
        reasons: list[str] = []
        if item.latest_price is None:
            reasons.append('missing_price')
        elif item.latest_price < resolved_policy.minimum_price:
            reasons.append('price_below_floor')
        if item.history_days < resolved_policy.minimum_history_days:
            reasons.append('insufficient_history')
        if item.median_dollar_volume_20d is None:
            reasons.append('missing_dollar_volume')
        elif item.median_dollar_volume_20d < resolved_policy.minimum_median_dollar_volume_20d:
            reasons.append('dollar_volume_below_floor')
        if item.median_volume_20d is None:
            reasons.append('missing_volume')
        elif item.median_volume_20d < resolved_policy.minimum_median_volume_20d:
            reasons.append('volume_below_floor')
        if item.volatility_60d is None:
            reasons.append('missing_volatility')
        elif item.volatility_60d > resolved_policy.maximum_volatility_60d:
            reasons.append('volatility_above_ceiling')
        if item.max_drawdown_120d is None:
            reasons.append('missing_drawdown')
        elif item.max_drawdown_120d < resolved_policy.maximum_drawdown_120d:
            reasons.append('drawdown_below_floor')
        if item.return_5d is None:
            reasons.append('missing_recent_return')
        elif item.return_5d > resolved_policy.maximum_return_5d:
            reasons.append('recent_overheat')
        if item.momentum_6m is None or item.momentum_12m is None:
            reasons.append('missing_momentum')
        if reasons:
            exclusions[item.symbol] = tuple(reasons)
        else:
            eligible.append(item)

    scores = _combined_scores(
        eligible,
        (
            (lambda item: item.median_dollar_volume_20d, True),
            (lambda item: item.volatility_60d, False),
            (lambda item: item.max_drawdown_120d, True),
            (lambda item: item.momentum_6m, True),
            (lambda item: item.momentum_12m, True),
        ),
    )
    ranked = sorted(eligible, key=lambda item: (-scores[item.symbol], item.symbol))
    selected = tuple(ranked[: resolved_policy.price_candidate_cap])
    for item in ranked[resolved_policy.price_candidate_cap :]:
        exclusions[item.symbol] = ('price_candidate_cap',)
    return USScreeningResult(
        selected=selected,
        scores={item.symbol: scores[item.symbol] for item in selected},
        exclusions={symbol: exclusions[symbol] for symbol in sorted(exclusions)},
        eligible_before_cap=len(eligible),
        cap=resolved_policy.price_candidate_cap,
    )


def screen_us_fundamental_candidates(
    metrics: Iterable[USFundamentalScreenMetrics],
    *,
    market_date: date,
    policy: USScreeningPolicy | None = None,
) -> USScreeningResult[USFundamentalScreenMetrics]:
    resolved_policy = policy or USScreeningPolicy()
    eligible: list[USFundamentalScreenMetrics] = []
    exclusions: dict[str, tuple[str, ...]] = {}
    for item in sorted(metrics, key=lambda value: value.symbol):
        reasons: list[str] = []
        if item.latest_periodic_filing_date is None:
            reasons.append('missing_periodic_filing')
        elif (market_date - item.latest_periodic_filing_date).days > resolved_policy.maximum_periodic_filing_age_days:
            reasons.append('stale_periodic_filing')
        if item.ttm_revenue is None:
            reasons.append('missing_revenue')
        elif item.ttm_revenue <= 0:
            reasons.append('non_positive_revenue')
        if item.ttm_operating_cash_flow is None:
            reasons.append('missing_operating_cash_flow')
        elif item.ttm_operating_cash_flow <= 0:
            reasons.append('non_positive_operating_cash_flow')
        if item.total_assets is None or item.total_assets <= 0 or item.total_liabilities is None:
            reasons.append('missing_leverage_inputs')
        elif item.total_liabilities / item.total_assets > resolved_policy.maximum_liabilities_to_assets:
            reasons.append('leverage_above_ceiling')
        if item.shares_outstanding_growth_yoy is None:
            reasons.append('missing_dilution')
        elif item.shares_outstanding_growth_yoy > resolved_policy.maximum_shares_outstanding_growth_yoy:
            reasons.append('dilution_above_ceiling')
        if item.has_bankruptcy_or_delinquency_flag:
            reasons.append('bankruptcy_or_delinquency')
        if reasons:
            exclusions[item.symbol] = tuple(reasons)
        else:
            eligible.append(item)

    def cash_flow_margin(item: USFundamentalScreenMetrics) -> Decimal | None:
        if item.ttm_revenue is None or item.ttm_revenue <= 0 or item.ttm_operating_cash_flow is None:
            return None
        return item.ttm_operating_cash_flow / item.ttm_revenue

    def net_margin(item: USFundamentalScreenMetrics) -> Decimal | None:
        if item.ttm_revenue is None or item.ttm_revenue <= 0 or item.ttm_net_income is None:
            return None
        return item.ttm_net_income / item.ttm_revenue

    def leverage(item: USFundamentalScreenMetrics) -> Decimal | None:
        if item.total_assets is None or item.total_assets <= 0 or item.total_liabilities is None:
            return None
        return item.total_liabilities / item.total_assets

    scores = _combined_scores(
        eligible,
        (
            (cash_flow_margin, True),
            (net_margin, True),
            (leverage, False),
            (lambda item: item.shares_outstanding_growth_yoy, False),
        ),
    )
    ranked = sorted(eligible, key=lambda item: (-scores[item.symbol], item.symbol))
    selected = tuple(ranked[: resolved_policy.fundamental_candidate_cap])
    for item in ranked[resolved_policy.fundamental_candidate_cap :]:
        exclusions[item.symbol] = ('fundamental_candidate_cap',)
    return USScreeningResult(
        selected=selected,
        scores={item.symbol: scores[item.symbol] for item in selected},
        exclusions={symbol: exclusions[symbol] for symbol in sorted(exclusions)},
        eligible_before_cap=len(eligible),
        cap=resolved_policy.fundamental_candidate_cap,
    )
