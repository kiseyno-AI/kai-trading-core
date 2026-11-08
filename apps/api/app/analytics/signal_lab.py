"""Signal lab: score deterministic factors and the agents on the same footing.

Read-only. ``python -m app.analytics.signal_lab`` loads bars, stored factor scores and
agent feedback/decisions, evaluates each signal against next-open-to-horizon-close
returns with the rules in ``signal_evaluation`` and writes a JSON + Markdown report
under ``var/alpha-lab/``. Nothing here places orders or writes to the database.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import text

from app.analytics import signal_evaluation as ev

DEFAULT_HORIZONS = (1, 5, 10)
DEFAULT_START = '2026-04-14'
# KRX round trip for a small retail account: ~1.5 bp commission each way, the 18 bp
# sell-side transaction tax and roughly 15 bp slippage each way. All are assumptions
# and are printed in the report; the cost sensitivity table shows the effect.
DEFAULT_ROUNDTRIP_COST_BPS = 51.0
COST_SENSITIVITY_BPS = (0.0, 30.0, 51.0, 80.0)


@dataclass(frozen=True)
class SignalSpec:
    name: str
    column: str
    family: str  # deterministic | agent | control
    description: str
    coverage: str  # universe | pool


SIGNALS: tuple[SignalSpec, ...] = (
    SignalSpec('low_risk_score', 'low_risk_score', 'deterministic', 'stored low-risk factor score', 'universe'),
    SignalSpec('neg_vol20', 'neg_vol20', 'deterministic', 'minus 20-session realised volatility', 'universe'),
    SignalSpec('momentum_pct', 'momentum_percentile', 'deterministic', 'stored momentum percentile', 'universe'),
    SignalSpec('composite_pct', 'composite_percentile', 'deterministic', 'stored composite percentile', 'universe'),
    SignalSpec('quality_pct', 'quality_percentile', 'deterministic', 'stored quality percentile', 'universe'),
    SignalSpec('reversal_5d', 'neg_tr5', 'deterministic', 'minus trailing 5-session return', 'universe'),
    SignalSpec('trend_20d', 'tr20', 'deterministic', 'trailing 20-session return', 'universe'),
    SignalSpec('placebo_random', 'placebo', 'control', 'seeded random score (false-positive control)', 'universe'),
    SignalSpec('eod_summarizer_conf', 'eod_conf', 'agent', 'EOD summarizer confidence', 'pool'),
    SignalSpec('technical_summarizer_conf', 'tech_conf', 'agent', 'technical summarizer confidence', 'pool'),
    SignalSpec('fundamental_summarizer_conf', 'fund_conf', 'agent', 'fundamental summarizer confidence', 'pool'),
    SignalSpec('signal_decider_conf', 'sig_conf', 'agent', 'signal-decider confidence', 'pool'),
    SignalSpec('signal_decider_conv', 'sig_conv', 'agent', 'signal-decider conviction', 'pool'),
    SignalSpec('signal_decider_expret', 'sig_expret', 'agent', 'signal-decider expected return claim', 'pool'),
    SignalSpec('short_term_conf', 'st_conf', 'agent', 'short-term-decider confidence', 'pool'),
    SignalSpec('short_term_conv', 'st_conv', 'agent', 'short-term-decider conviction', 'pool'),
)


# ------------------------------------------------------------------------ data
def _rows(session: Any, sql: str, **params: Any) -> list[Any]:
    return list(session.execute(text(sql), params).all())


def load_panel(start: str = DEFAULT_START, end: str | None = None, *, horizons: Sequence[int] = DEFAULT_HORIZONS) -> pd.DataFrame:
    """Build the symbol x date panel with forward returns, factor scores and agent scores."""

    from app.db import session_scope

    end_date = end or date.today().isoformat()
    warmup_start = (pd.Timestamp(start) - pd.Timedelta(days=60)).date().isoformat()
    with session_scope(statement_timeout_ms=900_000) as session:
        session.execute(text('SET TRANSACTION READ ONLY'))
        bars = pd.DataFrame(
            _rows(
                session,
                """select s.symbol, b.trade_date as d, b.open_price as open, b.close_price as close, b.volume
                   from daily_price_bars b join symbols s on s.id = b.symbol_id
                   where b.trade_date between :a and :b and s.market in ('KOSPI','KOSDAQ') and s.is_active""",
                a=warmup_start,
                b=end_date,
            ),
            columns=['symbol', 'd', 'open', 'close', 'volume'],
        )
        if bars.empty:
            return pd.DataFrame(columns=['symbol', 'd'])
        factors = {
            'low': _rows(session, 'select s.symbol, r.market_date, r.low_risk_score from daily_low_risk_factor_results r join symbols s on s.id=r.symbol_id where r.market_date between :a and :b', a=start, b=end_date),
            'mom': _rows(session, 'select s.symbol, r.market_date, r.momentum_percentile from daily_momentum_factor_results r join symbols s on s.id=r.symbol_id where r.market_date between :a and :b', a=start, b=end_date),
            'comp': _rows(session, 'select s.symbol, r.market_date, r.composite_percentile from daily_composite_factor_results r join symbols s on s.id=r.symbol_id where r.market_date between :a and :b', a=start, b=end_date),
            'qual': _rows(session, 'select s.symbol, r.market_date, r.quality_percentile from daily_quality_factor_results r join symbols s on s.id=r.symbol_id where r.market_date between :a and :b', a=start, b=end_date),
        }
        summarizers = _rows(
            session,
            """select agent_name, market_date, symbol, (feedback_payload::jsonb->>'confidence')::float, received_at
               from agent_context_feedback
               where agent_name in ('eod-summarizer','technical-summarizer','fundamental-summarizer')
                 and market_date between :a and :b""",
            a=start,
            b=end_date,
        )
        decisions = _rows(
            session,
            # The later of storing, receiving and last updating the row: a value that
            # changed after the open is not what a trader saw at the open.
            """select agent_name, market_date, symbol, decision_action, confidence_score_10, conviction_score_10, expected_return_pct,
                      greatest(created_at, coalesce(feedback_received_at, created_at), updated_at)
               from signal_decider_decision_snapshots where market_date between :a and :b""",
            a=start,
            b=end_date,
        )

    bars['d'] = pd.to_datetime(bars['d'])
    for col in ('open', 'close', 'volume'):
        bars[col] = pd.to_numeric(bars[col], errors='coerce')
    bars = bars.sort_values(['symbol', 'd'])
    bars['ret1'] = bars.groupby('symbol')['close'].pct_change()
    bars['vol20'] = bars.groupby('symbol')['ret1'].transform(lambda x: x.rolling(20, min_periods=15).std()) * 100.0
    bars['neg_vol20'] = -bars['vol20']
    bars['tr5'] = bars.groupby('symbol')['close'].pct_change(5) * 100.0
    bars['neg_tr5'] = -bars['tr5']
    bars['tr20'] = bars.groupby('symbol')['close'].pct_change(20) * 100.0

    panel = forward_panel(bars, horizons)
    panel = panel.merge(bars[['symbol', 'd', 'vol20', 'neg_vol20', 'tr5', 'neg_tr5', 'tr20']], on=['symbol', 'd'], how='left')
    panel = panel[panel['d'] >= pd.Timestamp(start)]

    for key, column in (('low', 'low_risk_score'), ('mom', 'momentum_percentile'), ('comp', 'composite_percentile'), ('qual', 'quality_percentile')):
        frame = pd.DataFrame(factors[key], columns=['symbol', 'd', column])
        frame['d'] = pd.to_datetime(frame['d'])
        frame[column] = pd.to_numeric(frame[column], errors='coerce')
        panel = panel.merge(frame.drop_duplicates(['symbol', 'd']), on=['symbol', 'd'], how='left')

    sessions = bars['d'].unique()
    point_in_time: dict[str, dict[str, int]] = {}
    rename = {'eod-summarizer': 'eod_conf', 'technical-summarizer': 'tech_conf', 'fundamental-summarizer': 'fund_conf'}
    summ = pd.DataFrame(summarizers, columns=['agent', 'd', 'symbol', 'conf', 'known_at'])
    summ['d'] = pd.to_datetime(summ['d'])
    summ['conf'] = pd.to_numeric(summ['conf'], errors='coerce')
    for agent, column in rename.items():
        part, point_in_time[agent] = ev.admit_before_next_open(summ[summ['agent'] == agent], sessions)
        part = part[['symbol', 'd', 'conf']].rename(columns={'conf': column})
        panel = panel.merge(part, on=['symbol', 'd'], how='left')

    dec = pd.DataFrame(decisions, columns=['agent', 'd', 'symbol', 'act', 'conf', 'conv', 'expret', 'known_at'])
    dec['d'] = pd.to_datetime(dec['d'])
    for col in ('conf', 'conv', 'expret'):
        dec[col] = pd.to_numeric(dec[col], errors='coerce')
    for agent, prefix in (('signal-decider', 'sig'), ('short-term-decider', 'st')):
        part, point_in_time[agent] = ev.admit_before_next_open(dec[dec['agent'] == agent], sessions)
        part = part[['symbol', 'd', 'act', 'conf', 'conv', 'expret']].rename(
            columns={'act': f'{prefix}_act', 'conf': f'{prefix}_conf', 'conv': f'{prefix}_conv', 'expret': f'{prefix}_expret'}
        )
        panel = panel.merge(part, on=['symbol', 'd'], how='left')
    for column in ('sig_expret',):
        if column not in panel:
            panel[column] = np.nan
    panel['placebo'] = np.random.default_rng(20260101).normal(size=len(panel))
    panel = panel.reset_index(drop=True)
    panel.attrs['point_in_time'] = point_in_time
    return panel


def forward_panel(bars: pd.DataFrame, horizons: Sequence[int]) -> pd.DataFrame:
    return ev.forward_returns(bars[['symbol', 'd', 'open', 'close', 'volume']], horizons)


# ------------------------------------------------------------------------ evaluation
def evaluate_panel(
    panel: pd.DataFrame,
    *,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
    signals: Sequence[SignalSpec] = SIGNALS,
    k: int = 5,
    roundtrip_cost_bps: float = DEFAULT_ROUNDTRIP_COST_BPS,
    control: str = 'vol20',
) -> dict[str, Any]:
    present = [spec for spec in signals if spec.column in panel and panel[spec.column].notna().any()]
    n_tests = max(len(present) * len(horizons), 1)
    universe_mean = {h: panel.groupby('d')[f'r{h}'].transform('mean') for h in horizons}
    rows: list[dict[str, Any]] = []
    for spec in present:
        min_names = 50 if spec.coverage == 'universe' else 20
        for h in horizons:
            ret = f'r{h}'
            ics = ev.cross_sectional_ic(panel, spec.column, ret, min_names=min_names)
            summary = ev.summarize_ic(ics, horizon=h)
            partial = (
                ev.partial_cross_sectional_ic(panel, spec.column, control, ret, min_names=min_names)
                if control in panel and spec.column != control
                else pd.Series(dtype=float)
            )
            partial_summary = ev.summarize_ic(partial, horizon=h) if len(partial) >= 3 else {}
            row: dict[str, Any] = {
                'signal': spec.name,
                'family': spec.family,
                'coverage': spec.coverage,
                'horizon': h,
                'verdict': ev.verdict(summary, n_tests=n_tests),
                **{key: _round(summary.get(key)) for key in ('days', 'mean_ic', 't_hac', 't_nonoverlap', 'first_half_ic', 'second_half_ic', 'positive_day_share')},
                'partial_ic_vs_vol': _round(partial_summary.get('mean_ic')),
                'partial_t_hac': _round(partial_summary.get('t_hac')),
            }
            if spec.coverage == 'universe':
                spread = ev.quantile_spread(panel, spec.column, ret)
                row['top_minus_bottom_quintile_pct'] = _round(spread.mean() if len(spread) else None)
                sims = {}
                for cost in COST_SENSITIVITY_BPS:
                    sim = ev.simulate_topk(panel, spec.column, ret, k=k, step=h, roundtrip_cost_bps=cost)
                    sims[str(int(cost))] = {key: _round(value) for key, value in sim.items()}
                row['topk_simulation'] = sims
                row['topk_at_assumed_cost'] = {
                    key: _round(value)
                    for key, value in ev.simulate_topk(panel, spec.column, ret, k=k, step=h, roundtrip_cost_bps=roundtrip_cost_bps).items()
                }
            rows.append(row)
    return {
        'point_in_time': dict(panel.attrs.get('point_in_time') or {}),
        'entry_timing': entry_timing(panel),
        'n_tests': n_tests,
        'required_t': _round(ev.required_t(n_tests)),
        'k': k,
        'roundtrip_cost_bps': roundtrip_cost_bps,
        'signals': rows,
        'decisions': decision_scoreboard(panel, horizons),
    }


def entry_timing(panel: pd.DataFrame) -> dict[str, Any]:
    """Does the session we enter in tend to fade after the open we buy at?"""

    if 'intraday1' not in panel:
        return {}
    per_date = panel.groupby('d')['intraday1'].mean().dropna()
    median = panel.groupby('d')['intraday1'].median().dropna()
    by_month = per_date.groupby(per_date.index.strftime('%Y-%m')).mean()
    return {
        'days': int(len(per_date)),
        'mean_open_to_close_pct': _round(per_date.mean()),
        'median_symbol_open_to_close_pct': _round(median.mean()),
        't': _round(ev.newey_west_t(per_date, lag=0)),
        'share_of_days_negative': _round((per_date < 0).mean()),
        'by_month_pct': {month: _round(value) for month, value in by_month.items()},
    }


def decision_scoreboard(panel: pd.DataFrame, horizons: Sequence[int]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for agent, column in (('signal-decider', 'sig_act'), ('short-term-decider', 'st_act')):
        if column not in panel:
            continue
        pool = panel[panel[column].notna()]
        for h in horizons:
            ret = f'r{h}'
            for action in sorted(pool[column].dropna().unique()):
                per_date = pool[pool[column] == action].groupby('d')[ret].mean().dropna()
                universe = panel.groupby('d')[ret].mean()
                excess = (per_date - universe.reindex(per_date.index)).dropna()
                out.append({
                    'agent': agent, 'action': action, 'horizon': h, 'decisions': int((pool[column] == action).sum()),
                    'days': int(len(excess)), 'excess_vs_universe_pct': _round(excess.mean() if len(excess) else None),
                    't_hac': _round(ev.newey_west_t(excess, lag=max(h - 1, 0)) if len(excess) > 3 else None),
                })
            for a, b in (('buy', 'wait'), ('reject', 'wait')):
                diff = ev.paired_difference(pool, column, a, b, ret)
                if len(diff) > 3:
                    out.append({
                        'agent': agent, 'action': f'{a}-minus-{b}', 'horizon': h, 'decisions': int(pool[column].isin([a, b]).sum()),
                        'days': int(len(diff)), 'excess_vs_universe_pct': _round(diff.mean()),
                        't_hac': _round(ev.newey_west_t(diff, lag=max(h - 1, 0))),
                    })
    return out


def _round(value: Any, digits: int = 3) -> Any:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    return None if number != number else round(number, digits)


# ------------------------------------------------------------------------ report
def render_markdown(result: Mapping[str, Any], *, start: str, end: str, universe_days: int) -> str:
    lines = [
        f'# Signal lab report ({start} .. {end})',
        '',
        f'* sessions in sample: {universe_days}; signals x horizons tested: {result["n_tests"]}; '
        f'multiplicity-adjusted |t| threshold: {result["required_t"]}',
        f'* returns: next-session open to the close h sessions after the decision date; top-{result["k"]} simulation '
        f'rebalances every h sessions with an assumed round-trip cost of {result["roundtrip_cost_bps"]:.0f} bp',
        '* the sample is one market regime; a label is a screening result, not proof of a lasting edge',
        '',
        '## Signals (rank IC within date, Newey-West t)',
        '',
        '| signal | family | h | days | IC | t(HAC) | t(non-overlap) | IC 1st / 2nd half | partial IC vs vol | verdict |',
        '|---|---|---|---|---|---|---|---|---|---|',
    ]
    for row in result['signals']:
        lines.append(
            f"| {row['signal']} | {row['family']} | {row['horizon']} | {row['days']} | {row['mean_ic']} | {row['t_hac']} | "
            f"{row['t_nonoverlap']} | {row['first_half_ic']} / {row['second_half_ic']} | {row['partial_ic_vs_vol']} | **{row['verdict']}** |"
        )
    admission = result.get('point_in_time') or {}
    if admission:
        lines += ['', '## Agent rows admitted (stored before the next session open, 09:00 KST)', '',
                  '| agent | rows | admitted | stored at/after the open | no stored time | no next session yet |',
                  '|---|---|---|---|---|---|']
        for agent, c in admission.items():
            lines.append(f"| {agent} | {c['rows']} | {c['admitted']} | {c['late']} | {c['unknown_time']} | {c['no_next_session']} |")
    timing = result.get('entry_timing') or {}
    if timing:
        lines += ['', '## Entry timing: the session we buy in, open to its own close', '',
                  f"* mean across {timing['days']} sessions: {timing['mean_open_to_close_pct']}% (median symbol {timing['median_symbol_open_to_close_pct']}%), "
                  f"t={timing['t']}, negative on {timing['share_of_days_negative']} of days",
                  '* by month: ' + ', '.join(f'{m} {v}%' for m, v in timing['by_month_pct'].items())]
    lines += ['', '## Top-k simulation with costs (universe signals)', '',
              '| signal | h | periods | gross %/period | net % | benchmark % | excess % | t | total % | benchmark total % | max drawdown % |',
              '|---|---|---|---|---|---|---|---|---|---|---|']
    for row in result['signals']:
        sim = row.get('topk_at_assumed_cost')
        if sim and sim.get('periods'):
            lines.append(
                f"| {row['signal']} | {row['horizon']} | {sim['periods']} | {sim['mean_gross_pct']} | {sim['mean_net_pct']} | "
                f"{sim['mean_bench_pct']} | {sim['mean_excess_pct']} | {sim['excess_t']} | {sim['total_return_pct']} | "
                f"{sim['bench_total_return_pct']} | {sim['max_drawdown_pct']} |"
            )
    lines += ['', '## Agent decisions vs the same-day universe', '',
              '| agent | action | h | decisions | days | excess % | t(HAC) |', '|---|---|---|---|---|---|---|']
    for row in result['decisions']:
        lines.append(f"| {row['agent']} | {row['action']} | {row['horizon']} | {row['decisions']} | {row['days']} | {row['excess_vs_universe_pct']} | {row['t_hac']} |")
    return '\n'.join(lines) + '\n'


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Evaluate stock-ranking signals (read-only).')
    parser.add_argument('--start', default=DEFAULT_START)
    parser.add_argument('--end', default=None)
    parser.add_argument('--horizons', default=','.join(str(h) for h in DEFAULT_HORIZONS))
    parser.add_argument('--k', type=int, default=5)
    parser.add_argument('--cost-bps', type=float, default=DEFAULT_ROUNDTRIP_COST_BPS)
    parser.add_argument('--out-dir', default='var/alpha-lab')
    args = parser.parse_args(argv)
    horizons = tuple(int(item) for item in args.horizons.split(',') if item.strip())
    end = args.end or date.today().isoformat()
    panel = load_panel(args.start, end, horizons=horizons)
    if panel.empty:
        print(json.dumps({'status': 'no_data'}))
        return 1
    result = evaluate_panel(panel, horizons=horizons, k=args.k, roundtrip_cost_bps=args.cost_bps)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d-%H%M')
    (out / f'report-{stamp}.json').write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    markdown = render_markdown(result, start=args.start, end=end, universe_days=int(panel['d'].nunique()))
    (out / f'report-{stamp}.md').write_text(markdown)
    print(markdown)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
