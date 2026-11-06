"""Turn a signal-lab report into the findings the agent-improvement loop may act on.

Deterministic on purpose: this decides *whether* a change is allowed and what evidence
backs it, so the AI that later writes the wording has no say in that. A change needs a
verdict that persists over several weekly runs (one noisy week never edits an agent), a
cooldown applies between automated text changes, only the two decider contracts can be
edited, and anything else becomes an issue for the normal code/PR flow.
See docs/AGENT_IMPROVEMENT_LOOP.md.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

TEXT_CHANGE_AGENTS = frozenset({'signal-decider', 'short-term-decider'})
INVERSE_STREAK = 2
NO_SKILL_STREAK = 4
SURVIVING_STREAK = 2
MIN_PARTIAL_IC = 0.02
MIN_PARTIAL_T = 2.0
COOLDOWN_DAYS = 14
SESSIONS_PER_MONTH = 21


def agent_of(signal: str) -> str:
    if signal.startswith('signal_decider'):
        return 'signal-decider'
    if signal.startswith('short_term'):
        return 'short-term-decider'
    for name in ('eod', 'technical', 'fundamental'):
        if signal.startswith(name):
            return f'{name}-summarizer'
    return 'core'


def update_history(history: Sequence[Mapping[str, Any]], report: Mapping[str, Any], *, run_id: str) -> list[dict[str, Any]]:
    verdicts = {f"{row['signal']}|{row['horizon']}": row['verdict'] for row in report.get('signals', [])}
    kept = [dict(entry) for entry in history if entry.get('run_id') != run_id]
    kept.append({'run_id': run_id, 'verdicts': verdicts})
    return kept


def streak(history: Sequence[Mapping[str, Any]], key: str, verdict: str) -> int:
    count = 0
    for entry in reversed(list(history)):
        if entry.get('verdicts', {}).get(key) != verdict:
            break
        count += 1
    return count


def _cooling_down(today: date, last_change_date: date | None, cooldown_days: int) -> bool:
    return last_change_date is not None and (today - last_change_date).days < cooldown_days


def build_findings(
    report: Mapping[str, Any],
    history: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
    today: date,
    last_change_date: date | None = None,
    cooldown_days: int = COOLDOWN_DAYS,
) -> list[dict[str, Any]]:
    full_history = update_history(history, report, run_id=run_id)
    cooling = _cooling_down(today, last_change_date, cooldown_days)
    findings: list[dict[str, Any]] = []

    def add(rule: str, row: Mapping[str, Any], action: str, agent: str, count: int) -> None:
        key = f"{row['signal']}|{row['horizon']}"
        findings.append(
            {
                'id': f'{rule}:{key}',
                'rule': rule,
                'signal': row['signal'],
                'horizon': row['horizon'],
                'agent': agent,
                'action': action,
                'evidence': {
                    'verdict': row['verdict'],
                    'streak': count,
                    'mean_ic': row.get('mean_ic'),
                    't_hac': row.get('t_hac'),
                    'days': row.get('days'),
                    'partial_ic_vs_vol': row.get('partial_ic_vs_vol'),
                    'partial_t_hac': row.get('partial_t_hac'),
                },
            }
        )

    for row in report.get('signals', []):
        key = f"{row['signal']}|{row['horizon']}"
        verdict = row['verdict']
        count = streak(full_history, key, verdict)
        if row.get('family') == 'agent':
            agent = agent_of(row['signal'])
            if verdict == 'candidate_inverse' and count >= INVERSE_STREAK:
                rule = 'persistent_inverse_claim'
            elif verdict == 'no_evidence' and (row.get('mean_ic') or 0) < 0 and count >= NO_SKILL_STREAK:
                rule = 'persistent_no_skill'
            elif verdict == 'candidate_positive' and count >= INVERSE_STREAK:
                add('persistent_agent_skill', row, 'observation', agent, count)
                continue
            else:
                continue
            if agent in TEXT_CHANGE_AGENTS:
                if not cooling:
                    add(rule, row, 'text_change', agent, count)
            else:
                add(rule, row, 'code_issue', agent, count)
        elif row.get('family') == 'deterministic':
            if (
                verdict == 'candidate_positive'
                and count >= SURVIVING_STREAK
                and (row.get('partial_ic_vs_vol') or 0) >= MIN_PARTIAL_IC
                and (row.get('partial_t_hac') or 0) >= MIN_PARTIAL_T
            ):
                add('expose_surviving_signal', row, 'code_issue', 'core', count)
    return findings


# ------------------------------------------------------------------------- economics
def monthly_net_excess_pct(per_period_pct: float, *, horizon: int) -> float:
    return per_period_pct * SESSIONS_PER_MONTH / max(horizon, 1)


def breakeven_capital_krw(monthly_cost_krw: float, monthly_net_excess_pct_value: float) -> int | None:
    """Capital at which a monthly edge exactly pays the monthly agent bill (None if no edge)."""

    if monthly_net_excess_pct_value <= 0:
        return None
    return int(round(monthly_cost_krw / (monthly_net_excess_pct_value / 100.0)))


MIN_ECONOMICS_PERIODS = 20
MIN_ECONOMICS_T = 2.0


def economics(report: Mapping[str, Any], *, monthly_cost_krw: float) -> list[dict[str, Any]]:
    """Monthly edge and break-even capital per simulated signal.

    A handful of non-overlapping periods says almost nothing, and scaling it to a month
    multiplies the noise, so the break-even is only given when there are enough periods and
    the excess return is statistically distinguishable from zero.
    """

    rows = []
    for row in report.get('signals', []):
        sim = row.get('topk_at_assumed_cost') or {}
        periods = int(sim.get('periods') or 0)
        if not periods:
            continue
        monthly = monthly_net_excess_pct(float(sim.get('mean_excess_pct') or 0.0), horizon=int(row['horizon']))
        t = sim.get('excess_t')
        reliable = periods >= MIN_ECONOMICS_PERIODS and t is not None and float(t) >= MIN_ECONOMICS_T
        rows.append(
            {
                'signal': row['signal'],
                'horizon': row['horizon'],
                'verdict': row['verdict'],
                'periods': periods,
                'monthly_net_excess_vs_universe_pct': round(monthly, 2),
                'reliable': reliable,
                'breakeven_capital_krw': breakeven_capital_krw(monthly_cost_krw, monthly) if reliable else None,
            }
        )
    return sorted(rows, key=lambda r: (not r['reliable'], -r['monthly_net_excess_vs_universe_pct']))


# ------------------------------------------------------------------------- CLI
def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Derive improvement findings from a signal-lab report.')
    parser.add_argument('--report', required=True)
    parser.add_argument('--history', required=True, help='jsonl of previous runs (kept private, not in git)')
    parser.add_argument('--run-id', default=datetime.now().strftime('%Y%m%d-%H%M'))
    parser.add_argument('--last-change', default=None, help='YYYY-MM-DD of the last automated text change')
    parser.add_argument('--changelog', default=None, help='agent_rules/CHANGELOG.md; the newest dated line sets the cooldown')
    parser.add_argument('--monthly-cost-krw', type=float, default=500_000)
    parser.add_argument('--out', required=True)
    args = parser.parse_args(argv)
    report = json.loads(Path(args.report).read_text())
    history_path = Path(args.history)
    history = [json.loads(line) for line in history_path.read_text().splitlines() if line.strip()] if history_path.exists() else []
    last_change = date.fromisoformat(args.last_change) if args.last_change else None
    if last_change is None and args.changelog and Path(args.changelog).exists():
        dates = re.findall(r'^- (\d{4}-\d{2}-\d{2}) ', Path(args.changelog).read_text(), flags=re.MULTILINE)
        last_change = max(date.fromisoformat(item) for item in dates) if dates else None
    findings = build_findings(report, history, run_id=args.run_id, today=date.today(), last_change_date=last_change)
    updated = update_history(history, report, run_id=args.run_id)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text('\n'.join(json.dumps(entry) for entry in updated) + '\n')
    payload = {
        'run_id': args.run_id,
        'findings': findings,
        'economics': economics(report, monthly_cost_krw=args.monthly_cost_krw),
        'text_changes_allowed': sum(1 for f in findings if f['action'] == 'text_change'),
    }
    Path(args.out).write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(json.dumps({k: v for k, v in payload.items() if k != 'economics'}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
