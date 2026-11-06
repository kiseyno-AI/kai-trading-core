"""Pure functions for judging whether a stock-ranking signal earns money.

No database, no network. Everything takes DataFrames so the same code can score a
deterministic factor, an agent's confidence, or a decision label, and so the rules
can be tested with synthetic data.

Conventions (see docs/SIGNAL_EVALUATION_LAB.md):

* A signal value on date ``D`` may only use information available at the ``D`` close.
* The return it is judged on enters at the **next session's open** and exits at the
  **close of the ``h``-th session after ``D``** (``r{h}``, in percent). Entries on a
  halted/zero-volume day, and bars that are missing on the session calendar, give NaN.
* Rank correlation (IC) is computed within each date; the time series of daily ICs is
  then tested with a Newey-West t that respects the overlap of ``h``-day windows.
* A signal is only called a candidate when it clears a threshold that grows with the
  number of signals tested, is not carried by the overlapping test alone, and keeps the
  same sign in both halves of the sample.
"""

from __future__ import annotations

from statistics import NormalDist
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


# --------------------------------------------------------------------------- returns
def forward_returns(bars: pd.DataFrame, horizons: Sequence[int] = (1, 5, 10)) -> pd.DataFrame:
    """Long frame ``symbol, d, r{h}`` of next-open-to-horizon-close returns (percent).

    ``bars`` needs ``symbol, d, open, close, volume``. Rows are aligned on the union of
    all dates, so ``shift`` always means "sessions", never "this symbol's next row".
    """

    opens = bars.pivot_table(index='d', columns='symbol', values='open', aggfunc='last').sort_index()
    closes = bars.pivot_table(index='d', columns='symbol', values='close', aggfunc='last').reindex(opens.index)
    volumes = bars.pivot_table(index='d', columns='symbol', values='volume', aggfunc='last').reindex(opens.index)
    entry = opens.shift(-1)
    entry_volume = volumes.shift(-1)
    out: dict[str, pd.DataFrame] = {}
    for h in horizons:
        exit_close = closes.shift(-h)
        ret = (exit_close / entry - 1.0) * 100.0
        ret = ret.where((entry > 0) & (entry_volume > 0) & (exit_close > 0))
        out[f'r{h}'] = ret.stack(future_stack=True) if hasattr(ret, 'stack') else ret.stack()
    # Entry-day drift: what the first session does between the open we buy at and its own
    # close. If it is persistently negative, entering later in that session beats the open.
    intraday = (closes.shift(-1) / entry - 1.0) * 100.0
    intraday = intraday.where((entry > 0) & (entry_volume > 0) & (closes.shift(-1) > 0))
    out['intraday1'] = intraday.stack(future_stack=True) if hasattr(intraday, 'stack') else intraday.stack()
    frame = pd.DataFrame(out)
    frame.index.set_names(['d', 'symbol'], inplace=True)
    return frame.reset_index()[['symbol', 'd', *[f'r{h}' for h in horizons], 'intraday1']]


# --------------------------------------------------------------------------- IC
def _per_date(panel: pd.DataFrame, columns: Sequence[str], min_names: int):
    for d, group in panel.groupby('d', sort=True):
        sub = group[list(columns)].dropna()
        if len(sub) >= min_names:
            yield d, sub


def cross_sectional_ic(panel: pd.DataFrame, score: str, ret: str, *, min_names: int = 20) -> pd.Series:
    """Spearman rank correlation of ``score`` with ``ret`` within each date."""

    values: dict[Any, float] = {}
    for d, sub in _per_date(panel, (score, ret), min_names):
        if sub[score].nunique() < 3 or sub[ret].nunique() < 3:
            continue
        corr = sub[score].rank().corr(sub[ret].rank())
        if corr == corr:
            values[d] = float(corr)
    return pd.Series(values, dtype=float).sort_index()


def _residual(y: np.ndarray, x: np.ndarray) -> np.ndarray:
    x_centered = x - x.mean()
    denominator = float((x_centered**2).sum())
    if denominator == 0.0:
        return y - y.mean()
    slope = float(((y - y.mean()) * x_centered).sum()) / denominator
    return (y - y.mean()) - slope * x_centered


def partial_cross_sectional_ic(
    panel: pd.DataFrame, score: str, control: str, ret: str, *, min_names: int = 25
) -> pd.Series:
    """IC of ``score`` with ``ret`` after removing the rank of ``control`` from both."""

    values: dict[Any, float] = {}
    for d, sub in _per_date(panel, (score, control, ret), min_names):
        if sub[score].nunique() < 3 or sub[ret].nunique() < 3 or sub[control].nunique() < 3:
            continue
        x = sub[control].rank().to_numpy(dtype=float)
        a = _residual(sub[score].rank().to_numpy(dtype=float), x)
        b = _residual(sub[ret].rank().to_numpy(dtype=float), x)
        denom = float(np.sqrt((a**2).sum() * (b**2).sum()))
        if denom > 0:
            values[d] = float((a * b).sum() / denom)
    return pd.Series(values, dtype=float).sort_index()


# --------------------------------------------------------------------------- tests
def newey_west_t(series: pd.Series, lag: int) -> float:
    """t-statistic of the mean with a Bartlett-kernel (Newey-West) variance."""

    x = series.dropna().to_numpy(dtype=float)
    n = len(x)
    if n < 3:
        return float('nan')
    u = x - x.mean()
    variance = float((u**2).sum()) / n
    for l in range(1, max(lag, 0) + 1):
        if l >= n:
            break
        gamma = float((u[l:] * u[:-l]).sum()) / n
        variance += 2.0 * (1.0 - l / (lag + 1.0)) * gamma
    variance_of_mean = variance / n * (n / (n - 1.0))
    if variance_of_mean <= 0:
        return float('nan')
    return float(x.mean() / np.sqrt(variance_of_mean))


def summarize_ic(ics: pd.Series, horizon: int) -> dict[str, float]:
    ics = ics.dropna().sort_index()
    n = len(ics)
    if n < 3:
        return {'days': n}
    half = n // 2
    non_overlap = ics.iloc[:: max(horizon, 1)]
    naive = float(ics.mean() / (ics.std(ddof=1) / np.sqrt(n))) if ics.std(ddof=1) > 0 else float('nan')
    return {
        'days': n,
        'mean_ic': float(ics.mean()),
        'std_ic': float(ics.std(ddof=1)),
        't_naive': naive,
        't_hac': newey_west_t(ics, lag=max(horizon - 1, 0)),
        't_nonoverlap': newey_west_t(non_overlap, lag=0),
        'positive_day_share': float((ics > 0).mean()),
        'first_half_ic': float(ics.iloc[:half].mean()),
        'second_half_ic': float(ics.iloc[half:].mean()),
    }


def required_t(n_tests: int, *, alpha: float = 0.05, floor: float = 2.0) -> float:
    """Two-sided Bonferroni threshold for the number of signals that were tried."""

    n = max(int(n_tests), 1)
    return max(floor, NormalDist().inv_cdf(1.0 - alpha / 2.0 / n))


def verdict(
    summary: Mapping[str, float],
    *,
    n_tests: int = 1,
    min_days: int = 40,
    min_abs_ic: float = 0.02,
    min_nonoverlap_t: float = 1.5,
) -> str:
    """Conservative label: only a stable, multiplicity-adjusted effect is a candidate."""

    if summary.get('days', 0) < min_days or 'mean_ic' not in summary:
        return 'insufficient'
    need = required_t(n_tests)
    ic, t_hac, t_no = summary['mean_ic'], summary['t_hac'], summary['t_nonoverlap']
    first, second = summary['first_half_ic'], summary['second_half_ic']
    if ic >= min_abs_ic and t_hac >= need and t_no >= min_nonoverlap_t and first > 0 and second > 0:
        return 'candidate_positive'
    if ic <= -min_abs_ic and t_hac <= -need and t_no <= -min_nonoverlap_t and first < 0 and second < 0:
        return 'candidate_inverse'
    return 'no_evidence'


# --------------------------------------------------------------------------- spreads
def quantile_spread(
    panel: pd.DataFrame, score: str, ret: str, *, quantiles: int = 5, min_names: int = 50
) -> pd.Series:
    """Mean return of the top score quantile minus the bottom one, per date."""

    values: dict[Any, float] = {}
    for d, sub in _per_date(panel, (score, ret), min_names):
        bucket = pd.qcut(sub[score].rank(method='first'), quantiles, labels=False)
        top = sub.loc[bucket == quantiles - 1, ret].mean()
        bottom = sub.loc[bucket == 0, ret].mean()
        values[d] = float(top - bottom)
    return pd.Series(values, dtype=float).sort_index()


def paired_difference(panel: pd.DataFrame, group_col: str, a: str, b: str, ret: str) -> pd.Series:
    """Per-date mean ``ret`` of group ``a`` minus group ``b`` (same pool, same day)."""

    pivot = panel.dropna(subset=[ret]).pivot_table(index='d', columns=group_col, values=ret, aggfunc='mean')
    if a not in pivot or b not in pivot:
        return pd.Series(dtype=float)
    return (pivot[a] - pivot[b]).dropna().sort_index()


# --------------------------------------------------------------------------- simulation
def simulate_topk(
    panel: pd.DataFrame,
    score: str,
    ret: str,
    *,
    k: int,
    step: int,
    roundtrip_cost_bps: float,
    ascending: bool = False,
    min_names: int = 20,
) -> dict[str, float]:
    """Hold the ``k`` best names for ``step`` sessions, repeat, and charge costs.

    Rebalancing only every ``step`` dates keeps the holding windows from overlapping, so
    the period returns are (nearly) independent and compound honestly.
    """

    dates = sorted(panel['d'].dropna().unique())
    cost_pct = roundtrip_cost_bps / 100.0
    rows: list[tuple[float, float, float]] = []
    for d in dates[:: max(step, 1)]:
        sub = panel.loc[panel['d'] == d, [score, ret]].dropna()
        if len(sub) < max(min_names, k):
            continue
        chosen = sub.nsmallest(k, score) if ascending else sub.nlargest(k, score)
        gross = float(chosen[ret].mean())
        rows.append((gross, gross - cost_pct, float(sub[ret].mean())))
    if not rows:
        return {
            'periods': 0, 'mean_gross_pct': 0.0, 'mean_net_pct': 0.0, 'mean_bench_pct': 0.0,
            'mean_excess_pct': 0.0, 'excess_t': float('nan'), 'win_rate_vs_bench': 0.0,
            'total_return_pct': 0.0, 'bench_total_return_pct': 0.0, 'max_drawdown_pct': 0.0,
        }
    gross, net, bench = (np.array(col) for col in zip(*rows))
    excess = net - bench
    equity = np.cumprod(1.0 + net / 100.0)
    drawdown = equity / np.maximum.accumulate(equity) - 1.0
    excess_t = (
        float(excess.mean() / (excess.std(ddof=1) / np.sqrt(len(excess))))
        if len(excess) > 2 and excess.std(ddof=1) > 0
        else float('nan')
    )
    return {
        'periods': len(rows),
        'mean_gross_pct': float(gross.mean()),
        'mean_net_pct': float(net.mean()),
        'mean_bench_pct': float(bench.mean()),
        'mean_excess_pct': float(excess.mean()),
        'excess_t': excess_t,
        'win_rate_vs_bench': float((net > bench).mean()),
        'total_return_pct': float((equity[-1] - 1.0) * 100.0),
        'bench_total_return_pct': float((np.prod(1.0 + bench / 100.0) - 1.0) * 100.0),
        'max_drawdown_pct': float(drawdown.min() * 100.0),
    }
