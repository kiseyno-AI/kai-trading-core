from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest

PATH = Path(__file__).resolve().parents[3] / 'scripts' / 'agent_improvement_run.py'
spec = importlib.util.spec_from_file_location('agent_improvement_run', PATH)
run = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(run)

TEXT = {
    'id': 'persistent_inverse_claim:signal_decider_expret|1', 'rule': 'persistent_inverse_claim',
    'signal': 'signal_decider_expret', 'horizon': 1, 'agent': 'signal-decider', 'action': 'text_change',
    'evidence': {'verdict': 'candidate_inverse', 'streak': 3, 'mean_ic': -0.1, 't_hac': -5.2, 'days': 77},
}
ISSUE = {
    'id': 'expose_surviving_signal:quality_pct|5', 'rule': 'expose_surviving_signal', 'signal': 'quality_pct',
    'horizon': 5, 'agent': 'core', 'action': 'code_issue',
    'evidence': {'verdict': 'candidate_positive', 'streak': 2, 'mean_ic': 0.07, 't_hac': 6.1, 'partial_ic_vs_vol': 0.041},
}
POLICY = '# Policy\nOnly edit the calibration block.'


class PromptTests(unittest.TestCase):
    def test_prompt_contains_policy_findings_and_the_hard_boundaries(self) -> None:
        prompt = run.build_prompt(POLICY, [TEXT], {'signal-decider': '- existing note'}, run_id='20261108-0900')

        self.assertIn('Only edit the calibration block.', prompt)
        self.assertIn('persistent_inverse_claim:signal_decider_expret|1', prompt)
        self.assertIn('runtime_contracts/openclaw/v1/signal-decider/AGENTS.md', prompt)
        self.assertIn('BEGIN AUTO-CALIBRATION', prompt)
        self.assertIn('- existing note', prompt)
        self.assertIn('report:20261108-0900', prompt)
        self.assertIn('Do not touch any other file', prompt)

    def test_prompt_only_covers_text_change_findings_for_deciders(self) -> None:
        prompt = run.build_prompt(POLICY, [TEXT, ISSUE], {'signal-decider': ''}, run_id='r')
        self.assertNotIn('quality_pct', prompt)

    def test_prompt_never_embeds_secrets_or_environment(self) -> None:
        prompt = run.build_prompt(POLICY, [TEXT], {'signal-decider': ''}, run_id='r')
        for needle in ('DATABASE_URL', 'KIS_APP_SECRET', 'password', '.env'):
            self.assertNotIn(needle, prompt)


class SelectionTests(unittest.TestCase):
    def test_split_findings_by_action(self) -> None:
        text, issues = run.split_findings([TEXT, ISSUE, {**ISSUE, 'action': 'observation', 'id': 'x'}])
        self.assertEqual([f['id'] for f in text], [TEXT['id']])
        self.assertEqual([f['id'] for f in issues], [ISSUE['id']])

    def test_one_issue_per_signal_even_when_several_horizons_qualify(self) -> None:
        weaker = {**ISSUE, 'id': 'expose_surviving_signal:quality_pct|1', 'horizon': 1, 'evidence': {**ISSUE['evidence'], 't_hac': 2.0}}
        _, issues = run.split_findings([weaker, ISSUE])
        self.assertEqual([f['id'] for f in issues], [ISSUE['id']])

    def test_at_most_one_text_change_per_agent_per_run(self) -> None:
        other = {**TEXT, 'id': 'persistent_no_skill:signal_decider_conf|5', 'rule': 'persistent_no_skill'}
        chosen = run.cap_text_changes([TEXT, other])
        self.assertEqual(len(chosen), 1)
        self.assertEqual(chosen[0]['rule'], 'persistent_inverse_claim')  # stronger evidence kind first


class OutputTests(unittest.TestCase):
    def test_pr_body_lists_findings_with_verdicts_but_no_raw_performance_numbers(self) -> None:
        body = run.pr_body([TEXT], run_id='20261108-0900')

        self.assertIn('persistent_inverse_claim:signal_decider_expret|1', body)
        self.assertIn('candidate_inverse', body)
        self.assertIn('streak 3', body)
        self.assertNotIn('-0.1', body)
        self.assertNotIn('-5.2', body)
        self.assertIn('Generated with', body)

    def test_issue_title_is_stable_for_deduplication(self) -> None:
        title, body = run.issue_for(ISSUE)
        self.assertEqual(title, '[auto-finding] expose_surviving_signal: quality_pct|5')
        self.assertIn('candidate_positive', body)
        self.assertNotIn('0.041', body)

    def test_last_change_date_comes_from_the_changelog(self) -> None:
        text = '# Agent rules changelog\n- 2026-10-04 report:a finding:b signal-decider: x\n- 2026-11-08 report:c finding:d signal-decider: y\n'
        self.assertEqual(str(run.last_change_date(text)), '2026-11-08')
        self.assertIsNone(run.last_change_date('# Agent rules changelog\n'))


class DryRunTests(unittest.TestCase):
    def test_dry_run_is_the_default_and_never_calls_external_tools(self) -> None:
        calls: list[list[str]] = []
        result = run.execute(
            findings=[TEXT, ISSUE], run_id='r', apply=False, runner=lambda cmd, **kw: calls.append(cmd),
            policy_text=POLICY, contract_blocks={'signal-decider': ''},
        )
        self.assertEqual(calls, [])
        self.assertEqual(result['mode'], 'dry_run')
        self.assertEqual(len(result['would_change']), 1)
        self.assertEqual(len(result['would_open_issues']), 1)


class _Done:
    def __init__(self, returncode=0, stdout=''):
        self.returncode, self.stdout = returncode, stdout


class ApplyPathTests(unittest.TestCase):
    def _go(self, *, staged_clean=False, guard_rc=0, existing_issue=False):
        calls: list[list[str]] = []

        def runner(cmd, *, input=None, check=True, cwd=None):
            calls.append(list(cmd))
            if cmd[:3] == ['git', 'diff', '--cached']:
                return _Done(0 if staged_clean else 1)
            if cmd[:3] == ['gh', 'issue', 'list']:
                return _Done(0, '[{"number": 4}]' if existing_issue else '[]')
            if cmd[:3] == ['gh', 'pr', 'create']:
                return _Done(0, 'https://github.com/o/r/pull/9\n')
            if cmd[0] == sys.executable and any(part.endswith('agent_rules_guard.py') for part in cmd):
                return _Done(guard_rc, 'GUARD VIOLATION: x' if guard_rc else 'ok')
            return _Done(0)

        result = run.execute(
            findings=[TEXT, ISSUE], run_id='r1', apply=True, runner=runner,
            policy_text=POLICY, contract_blocks={'signal-decider': ''}, repo_dir='/tmp/repo',
        )
        return result, calls

    @staticmethod
    def _verbs(calls):
        return [' '.join(c[:2]) for c in calls]

    def test_success_branches_asks_codex_commits_guards_pushes_and_opens_a_pr_then_an_issue(self) -> None:
        result, calls = self._go()
        verbs = self._verbs(calls)

        self.assertLess(verbs.index('git checkout'), verbs.index('codex exec'))
        self.assertLess(verbs.index('codex exec'), verbs.index('git commit'))
        self.assertLess(verbs.index('git commit'), next(i for i, c in enumerate(calls) if any(p.endswith('agent_rules_guard.py') for p in c)))
        self.assertLess(next(i for i, c in enumerate(calls) if any(p.endswith('agent_rules_guard.py') for p in c)), verbs.index('git push'))
        self.assertIn('gh pr', verbs)
        self.assertEqual(result['pull_request'], 'https://github.com/o/r/pull/9')
        self.assertEqual(result['issues'], [{'title': '[auto-finding] expose_surviving_signal: quality_pct|5', 'status': 'opened'}])
        codex = next(c for c in calls if c[:2] == ['codex', 'exec'])
        self.assertIn('workspace-write', codex)
        self.assertNotIn('danger-full-access', ' '.join(codex))
        # The loop proposes; it never merges (check the command verbs, not the PR text).
        self.assertFalse(any(c[:2] in (['gh', 'pr'],) and len(c) > 2 and c[2] in ('merge', 'ready') for c in calls))
        self.assertFalse(any(c[:2] == ['git', 'merge'] for c in calls))

    def test_a_guard_violation_aborts_without_pushing_or_opening_a_pr(self) -> None:
        result, calls = self._go(guard_rc=1)
        verbs = self._verbs(calls)

        self.assertEqual(result['pull_request'], 'blocked_by_guard')
        self.assertNotIn('git push', verbs)
        self.assertNotIn('gh pr', verbs)
        self.assertIn('git branch', verbs)  # the local bot branch is discarded

    def test_when_codex_changes_nothing_no_commit_or_pr_is_made(self) -> None:
        result, calls = self._go(staged_clean=True)
        self.assertEqual(result['pull_request'], 'no_change')
        self.assertNotIn('git commit', self._verbs(calls))

    def test_an_already_open_issue_is_not_duplicated(self) -> None:
        result, calls = self._go(existing_issue=True)
        self.assertEqual(result['issues'][0]['status'], 'already_open')
        self.assertNotIn('gh issue', [' '.join(c[:2]) for c in calls if c[:3] == ['gh', 'issue', 'create']])


if __name__ == '__main__':
    unittest.main()
