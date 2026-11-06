#!/usr/bin/env python3
"""Act on improvement findings: AI-worded contract notes by PR, everything else by issue.

Runs on this Mac via scripts/run_agent_improvement_local.sh (see docs/AGENT_IMPROVEMENT_LOOP.md). Dry-run is the
default: it only prints what it would do. With ``--apply`` it

* asks the Codex CLI to edit ONLY the calibration block of the affected decider contract
  and append a changelog line,
* commits on ``bot/agent-rules-<run>``, runs ``scripts/agent_rules_guard.py`` against
  origin/main and aborts without pushing if it reports anything,
* opens a pull request (the repository's existing workflow then asks Codex to review it)
  and opens/dedupes one issue per code-level finding.

It never merges, never touches execution or risk code, and never sends the database,
the environment or secrets to the model: the prompt only carries the policy text, the
finding ids with verdict/streak, and the current calibration block.
"""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Callable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
BEGIN = '<!-- BEGIN AUTO-CALIBRATION -->'
END = '<!-- END AUTO-CALIBRATION -->'
CONTRACT = 'runtime_contracts/openclaw/v1/{agent}/AGENTS.md'
DECIDERS = ('signal-decider', 'short-term-decider')
RULE_PRIORITY = {'persistent_inverse_claim': 0, 'persistent_no_skill': 1}
COMMIT_PREFIX = '[Agent Rules Bot]'
FOOTER = 'Generated with the agent improvement loop (docs/AGENT_IMPROVEMENT_LOOP.md).'


def split_findings(findings: Sequence[Mapping[str, Any]]):
    text = [f for f in findings if f.get('action') == 'text_change' and f.get('agent') in DECIDERS]
    issues: dict[tuple[str, str], Mapping[str, Any]] = {}
    for f in findings:
        if f.get('action') != 'code_issue':
            continue
        key = (f['rule'], f['signal'])  # one issue per signal; the strongest horizon represents it
        if key not in issues or (f.get('evidence') or {}).get('t_hac', 0) ** 2 > (issues[key].get('evidence') or {}).get('t_hac', 0) ** 2:
            issues[key] = f
    return list(text), list(issues.values())


def cap_text_changes(findings: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """One text change per agent per run, strongest kind of evidence first."""

    best: dict[str, Mapping[str, Any]] = {}
    for finding in sorted(findings, key=lambda f: (RULE_PRIORITY.get(f['rule'], 9), (f.get('evidence') or {}).get('mean_ic') or 0)):
        best.setdefault(finding['agent'], finding)
    return list(best.values())


def last_change_date(changelog_text: str) -> date | None:
    dates = re.findall(r'^- (\d{4}-\d{2}-\d{2}) ', changelog_text, flags=re.MULTILINE)
    return max(date.fromisoformat(item) for item in dates) if dates else None


def build_prompt(policy_text: str, findings: Sequence[Mapping[str, Any]], contract_blocks: Mapping[str, str], *, run_id: str) -> str:
    text_findings, _ = split_findings(findings)
    lines = [
        'You maintain calibration notes for two stock-decision agents. Follow this policy exactly.',
        '',
        policy_text.strip(),
        '',
        '## Task',
        f'Run id: report:{run_id}. Today: {date.today().isoformat()}.',
        'For each target below, edit ONLY the lines between the markers',
        f'`{BEGIN}` and `{END}` in the named file. Write at most 3 plain bullet lines ("- ...", under 300 characters each)',
        'that make the agent more careful or better calibrated. Replace stale bullets instead of piling up new ones.',
        'Then append one line per target to agent_rules/CHANGELOG.md in the form',
        '`- YYYY-MM-DD report:<run id> finding:<finding id> <agent>: <one-sentence summary>`.',
        'Do not touch any other file, any other part of a contract, or the guard script. If no safe, evidence-backed bullet',
        'exists, change nothing.',
        '',
    ]
    for finding in text_findings:
        agent = finding['agent']
        ev = finding.get('evidence') or {}
        lines += [
            f"### Target: {CONTRACT.format(agent=agent)}",
            f"finding:{finding['id']}  rule: {finding['rule']}  signal: {finding['signal']} at {finding['horizon']} session(s)",
            f"evidence: verdict {ev.get('verdict')}, persisted for {ev.get('streak')} consecutive weekly runs",
            'current block:',
            '```',
            contract_blocks.get(agent, '').strip(),
            '```',
            '',
        ]
    return '\n'.join(lines)


def pr_body(findings: Sequence[Mapping[str, Any]], *, run_id: str) -> str:
    lines = [
        '## Automated agent-rule calibration',
        '',
        f'Run `report:{run_id}`. The full report stays on the runner (`var/alpha-lab`); this repository is public, so only',
        'verdicts and persistence are listed here.',
        '',
        '| finding | agent | verdict | persisted |',
        '|---|---|---|---|',
    ]
    for f in findings:
        ev = f.get('evidence') or {}
        lines.append(f"| `{f['id']}` | {f['agent']} | {ev.get('verdict')} | streak {ev.get('streak')} |")
    lines += [
        '',
        'Scope enforced by `scripts/agent_rules_guard.py`: only the AUTO-CALIBRATION block of the decider contracts and the',
        'append-only changelog may change. After merge the live workspace copies must be re-installed with',
        '`scripts/sync_openclaw_runtime_contract.py`.',
        '',
        FOOTER,
    ]
    return '\n'.join(lines)


def issue_for(finding: Mapping[str, Any]) -> tuple[str, str]:
    ev = finding.get('evidence') or {}
    title = f"[auto-finding] {finding['rule']}: {finding['signal']}|{finding['horizon']}"
    body = '\n'.join(
        [
            f"Automated finding `{finding['id']}` for **{finding['agent']}**.",
            '',
            f"* verdict: {ev.get('verdict')}, persisted for {ev.get('streak')} consecutive weekly runs",
            '* this needs a code or payload change, which the loop does not make on its own',
            '* the full numbers are in the runner-side report (`var/alpha-lab`)',
            '',
            FOOTER,
        ]
    )
    return title, body


Runner = Callable[..., Any]


def _default_runner(cmd: Sequence[str], *, input: str | None = None, check: bool = True, cwd: str | None = None):
    return subprocess.run(list(cmd), input=input, text=True, capture_output=True, check=check, cwd=cwd)


def execute(
    *,
    findings: Sequence[Mapping[str, Any]],
    run_id: str,
    apply: bool,
    runner: Runner = _default_runner,
    policy_text: str,
    contract_blocks: Mapping[str, str],
    repo_dir: str | None = None,
) -> dict[str, Any]:
    text_findings, issue_findings = split_findings(findings)
    chosen = cap_text_changes(text_findings)
    result: dict[str, Any] = {
        'mode': 'apply' if apply else 'dry_run',
        'run_id': run_id,
        'would_change': [f['id'] for f in chosen],
        'would_open_issues': [issue_for(f)[0] for f in issue_findings],
    }
    if not apply:
        return result

    result['pull_request'] = None
    if chosen:
        branch = f'bot/agent-rules-{run_id}'
        run = lambda cmd, **kw: runner(cmd, cwd=repo_dir, **kw)  # noqa: E731
        run(['git', 'fetch', 'origin', 'main'])
        run(['git', 'checkout', '-B', branch, 'origin/main'])
        prompt = build_prompt(policy_text, chosen, contract_blocks, run_id=run_id)
        run(['codex', 'exec', '--sandbox', 'workspace-write', '--cd', repo_dir or '.', '-'], input=prompt)
        run(['git', 'add', '-A'])
        staged = run(['git', 'diff', '--cached', '--quiet'], check=False)
        if getattr(staged, 'returncode', 1) == 0:
            result['pull_request'] = 'no_change'
        else:
            run(['git', 'commit', '-m', f"{COMMIT_PREFIX} Calibrate decider notes ({run_id})"])
            guard = run([sys.executable, 'scripts/agent_rules_guard.py', '--base', 'origin/main'], check=False)
            if getattr(guard, 'returncode', 1) != 0:
                result['pull_request'] = 'blocked_by_guard'
                result['guard_output'] = (getattr(guard, 'stdout', '') or '')[-1500:]
                run(['git', 'checkout', 'main'], check=False)
                run(['git', 'branch', '-D', branch], check=False)
            else:
                run(['git', 'push', '-u', 'origin', branch])
                created = run(['gh', 'pr', 'create', '--base', 'main', '--head', branch,
                               '--title', f'Calibrate decider notes ({run_id})', '--body', pr_body(chosen, run_id=run_id)])
                result['pull_request'] = (getattr(created, 'stdout', '') or '').strip() or 'created'
    result['issues'] = []
    for finding in issue_findings:
        title, body = issue_for(finding)
        existing = runner(['gh', 'issue', 'list', '--state', 'open', '--search', f'"{title}" in:title', '--json', 'number'], cwd=repo_dir)
        if (getattr(existing, 'stdout', '') or '').strip() not in ('', '[]'):
            result['issues'].append({'title': title, 'status': 'already_open'})
            continue
        runner(['gh', 'issue', 'create', '--title', title, '--body', body, '--label', 'auto-finding'], cwd=repo_dir)
        result['issues'].append({'title': title, 'status': 'opened'})
    return result


def _read_blocks() -> dict[str, str]:
    blocks: dict[str, str] = {}
    for agent in DECIDERS:
        text = (ROOT / CONTRACT.format(agent=agent)).read_text(encoding='utf-8')
        if text.count(BEGIN) == 1 and text.count(END) == 1:
            blocks[agent] = text.split(BEGIN, 1)[1].split(END, 1)[0]
    return blocks


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--findings', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--apply', action='store_true', help='create the branch/PR/issues; default is a dry run')
    args = parser.parse_args(argv)
    payload = json.loads(Path(args.findings).read_text())
    result = execute(
        findings=payload.get('findings', []),
        run_id=args.run_id,
        apply=args.apply,
        policy_text=(ROOT / 'agent_rules' / 'POLICY.md').read_text(encoding='utf-8'),
        contract_blocks=_read_blocks(),
        repo_dir=str(ROOT),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get('pull_request') != 'blocked_by_guard' else 2


if __name__ == '__main__':
    sys.exit(main())
