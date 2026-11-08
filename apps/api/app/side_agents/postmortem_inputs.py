"""Outcome summary for the postmortem analyst, computed in code from stored decisions and bars.

Entry is the next session's open after the decision date; the exit is the close of the
h-th session counting the entry day. Costs use the lab's 51 bp round-trip assumption.
"""
from __future__ import annotations

import statistics
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Callable, Mapping, Sequence

ROUND_TRIP_COST_PCT = 0.51
HORIZONS = (3, 5)
PRIMARY_HORIZON = {'short-term-decider': 3, 'signal-decider': 5}
REGIME_MEDIAN_THRESHOLD_PCT = -0.5
MIN_SAMPLE = 20
# Same-day decisions share the market move and holding windows overlap, so the number of
# distinct decision dates, not the record count, bounds the effective sample.
MIN_DISTINCT_DATES = 10

DECISIONS_SQL = """
select distinct on (market_date, symbol, agent_name)
       market_date, symbol, agent_name, decision_action, confidence_score_10,
       response_payload->>'tactical_action' as tactical_action,
       response_payload->>'preferred_action' as preferred_action
from signal_decider_decision_snapshots
where market_date between :a and :b
order by market_date, symbol, agent_name, created_at desc
"""
BARS_SQL = """
select s.symbol, b.trade_date, b.open_price, b.close_price
from daily_price_bars b join symbols s on s.id = b.symbol_id
where s.symbol = any(:codes) and b.trade_date between :a and :b
order by s.symbol, b.trade_date
"""
MEDIAN_SQL = """
with r as (
  select b.trade_date d, b.close_price / nullif(lag(b.close_price) over (partition by b.symbol_id order by b.trade_date), 0) - 1 as ret
  from daily_price_bars b where b.trade_date between :a and :b)
select d, percentile_cont(0.5) within group (order by ret) * 100 as med
from r where d >= :c and ret is not null group by d order by d
"""


def normalize_action(agent: str, row: Mapping[str, Any]) -> str | None:
    from app.agent_context.roles import canonical_evaluation_action

    payload = {'tactical_action': row.get('tactical_action'), 'preferred_action': row.get('preferred_action')}
    return canonical_evaluation_action(agent_name=agent, persisted_action=row.get('decision_action'), response_payload=payload)


def realized_return(bars: Sequence[Mapping[str, Any]], decision_date: date, horizon: int) -> float | None:
    """Percent return from the next session's open to the close of the horizon-th session."""
    entry = next((i for i, b in enumerate(bars) if b['trade_date'] > decision_date), None)
    if entry is None or entry + horizon - 1 >= len(bars):
        return None
    open_price, close_price = bars[entry].get('open_price'), bars[entry + horizon - 1].get('close_price')
    if not open_price or not close_price or float(open_price) <= 0:
        return None
    return (float(close_price) / float(open_price) - 1) * 100


def classify(action: str, gross_pct: float, window_market_median: float | None, cost: float) -> str:
    if action == 'buy':
        if gross_pct > cost:
            return 'true_positive'
        if gross_pct > 0:
            return 'cost_drag'
        if window_market_median is not None and window_market_median <= REGIME_MEDIAN_THRESHOLD_PCT:
            return 'regime_mismatch'
        return 'false_positive'
    if gross_pct > 2 * cost:
        return 'missed_opportunity'
    return 'correct_abstention'


def _bucket(score: Any) -> str:
    try:
        value = float(score)
    except (TypeError, ValueError):
        return 'unknown'
    if value <= 5:
        return '<=5'
    if value >= 9:
        return '9-10'
    return str(int(value))


def summarize_outcomes(
    decisions: Sequence[Mapping[str, Any]],
    bars_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    market_medians: Mapping[date, float],
    *,
    cost_pct: float = ROUND_TRIP_COST_PCT,
) -> dict[str, Any]:
    per_agent: dict[str, dict[str, Any]] = {}
    for row in decisions:
        agent = row['agent_name']
        action = normalize_action(agent, row)
        if action is None:
            continue
        horizon = PRIMARY_HORIZON.get(agent, 5)
        bars = bars_by_symbol.get(row['symbol'], [])
        entry_index = next((i for i, b in enumerate(bars) if b['trade_date'] > row['market_date']), None)
        gross = realized_return(bars, row['market_date'], horizon)
        entry = per_agent.setdefault(agent, {'recorded': 0, 'unresolved': 0, 'actions': defaultdict(list), 'classification': defaultdict(int), 'buckets': defaultdict(list), 'all': [], 'dates': set(), 'action_dates': defaultdict(set), 'horizon_sessions': horizon})
        entry['recorded'] += 1
        if gross is None or entry_index is None:
            entry['unresolved'] += 1
            continue
        window = [market_medians[b['trade_date']] for b in bars[entry_index : entry_index + horizon] if b['trade_date'] in market_medians]
        window_median = statistics.fmean(window) if window else None
        label = classify(action, gross, window_median, cost_pct)
        entry['actions'][action].append(gross)
        entry['all'].append(gross)
        entry['dates'].add(row['market_date'])
        entry['action_dates'][action].add(row['market_date'])
        entry['classification'][label] += 1
        if action == 'buy':
            entry['buckets'][_bucket(row.get('confidence_score_10'))].append(gross)
    agents: dict[str, Any] = {}
    for agent, e in sorted(per_agent.items()):
        resolved = sum(len(v) for v in e['actions'].values())
        agents[agent] = {
            'recorded': e['recorded'], 'resolved': resolved, 'unresolved': e['unresolved'],
            'distinct_decision_dates': len(e['dates']),
            'horizon_sessions': e['horizon_sessions'], 'sufficient_sample': resolved >= MIN_SAMPLE and len(e['dates']) >= MIN_DISTINCT_DATES,
            'candidate_baseline': {
                'n': len(e['all']), 'mean_gross_return_pct': round(statistics.fmean(e['all']), 3) if e['all'] else None,
                'share_above_cost': round(sum(x > cost_pct for x in e['all']) / len(e['all']), 4) if e['all'] else None,
            },
            'by_action': {
                a: {
                    'n': len(v), 'distinct_dates': len(e['action_dates'][a]), 'mean_gross_return_pct': round(statistics.fmean(v), 3),
                    'share_positive': round(sum(x > 0 for x in v) / len(v), 4),
                    'share_above_cost': round(sum(x > cost_pct for x in v) / len(v), 4),
                }
                for a, v in sorted(e['actions'].items()) if v
            },
            'classification': {k: e['classification'].get(k, 0) for k in ('true_positive', 'false_positive', 'missed_opportunity', 'regime_mismatch', 'cost_drag', 'correct_abstention')},
            'buy_confidence_buckets': [
                {'bucket': b, 'n': len(v), 'share_above_cost': round(sum(x > cost_pct for x in v) / len(v), 4), 'mean_gross_return_pct': round(statistics.fmean(v), 3)}
                for b, v in sorted(e['buckets'].items())
            ],
        }
    return {
        'cost_round_trip_pct': cost_pct, 'min_sample': MIN_SAMPLE, 'min_distinct_dates': MIN_DISTINCT_DATES,
        'missed_opportunity_threshold_pct': round(2 * cost_pct, 4),
        'independence_note': 'records are not independent: decisions on the same date share the market move and holding windows overlap; treat distinct_decision_dates as the effective sample for any timing or regime claim', 'return_definition': 'next-session open to close of the horizon-th session, percent, before costs',
        'agents': agents,
        'total_resolved': sum(a['resolved'] for a in agents.values()),
    }


def load_summary(today: date, *, weeks: int = 6, session_scope: Callable[..., Any] | None = None) -> dict[str, Any]:
    from sqlalchemy import text

    if session_scope is None:
        from app.db import session_scope as default_scope

        session_scope = default_scope
    start = today - timedelta(weeks=weeks)
    far = today + timedelta(days=1)
    with session_scope() as session:
        session.execute(text("SET LOCAL statement_timeout = '120s'"))
        decisions = [dict(r) for r in session.execute(text(DECISIONS_SQL), {'a': start, 'b': today}).mappings().all()]
        codes = sorted({d['symbol'] for d in decisions})
        bars: dict[str, list[dict[str, Any]]] = defaultdict(list)
        if codes:
            for r in session.execute(text(BARS_SQL), {'codes': codes, 'a': start, 'b': far}).mappings().all():
                bars[r['symbol']].append(dict(r))
        medians = {r['d']: float(r['med']) for r in session.execute(text(MEDIAN_SQL), {'a': start - timedelta(days=7), 'b': far, 'c': start}).mappings().all()}
    summary = summarize_outcomes(decisions, bars, medians)
    summary['period'] = {'from': start.isoformat(), 'to': today.isoformat()}
    summary['known_agents'] = sorted({d['agent_name'] for d in decisions})
    return summary
