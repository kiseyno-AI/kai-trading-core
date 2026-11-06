from __future__ import annotations

from pathlib import Path
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[3]
LAB = ROOT / '.github' / 'workflows' / 'signal-lab-weekly.yml'
GUARD = ROOT / '.github' / 'workflows' / 'agent-rules-guard.yml'


def load(path: Path) -> dict:
    data = yaml.safe_load(path.read_text())
    data['on'] = data.get('on', data.get(True))  # PyYAML parses a bare `on` key as boolean True
    return data


class LabWorkflowSafetyTests(unittest.TestCase):
    def test_it_cannot_be_triggered_by_a_pull_request_or_a_fork(self) -> None:
        triggers = load(LAB)['on']
        self.assertEqual(set(triggers), {'schedule', 'workflow_dispatch'})

    def test_it_runs_only_on_the_owners_runner_and_only_for_this_repository(self) -> None:
        job = load(LAB)['jobs']['lab']
        self.assertEqual(job['runs-on'], ['self-hosted', 'kai-core'])
        self.assertIn("github.repository == 'kiseyno-AI/kai-trading-core'", job['if'])

    def test_permissions_are_read_only_and_checkout_is_main(self) -> None:
        data = load(LAB)
        self.assertEqual(data['permissions'], {'contents': 'read'})
        checkout = next(step for step in data['jobs']['lab']['steps'] if step.get('uses', '').startswith('actions/checkout'))
        self.assertEqual(checkout['with']['ref'], 'main')

    def test_numbers_never_reach_the_public_actions_log(self) -> None:
        text = LAB.read_text()
        self.assertIn('last-run.log', text)  # lab stdout is redirected to a runner file
        self.assertIn('last-findings.log', text)
        self.assertIn('last-improvement.log', text)
        for forbidden in ('cat "$OUT/report', 'cat "${{ steps.lab.outputs.report', '--markdown'):
            self.assertNotIn(forbidden, text)

    def test_applying_is_opt_in(self) -> None:
        text = LAB.read_text()
        self.assertIn('AGENT_RULES_AUTO_PR', text)
        self.assertIn("default: false", text)
        self.assertNotIn('--apply \\', text.replace('ARGS+=(--apply)', ''))  # no unconditional --apply


class GuardWorkflowTests(unittest.TestCase):
    def test_it_only_judges_automation_branches_and_uses_the_base_branch_guard(self) -> None:
        data = load(GUARD)
        job = data['jobs']['guard']
        self.assertIn("startsWith(github.head_ref, 'bot/agent-rules-')", job['if'])
        self.assertEqual(job['runs-on'], 'ubuntu-latest')
        text = GUARD.read_text()
        self.assertIn('git show "origin/${{ github.base_ref }}:scripts/agent_rules_guard.py"', text)
        self.assertEqual(data['permissions'], {'contents': 'read'})

    def test_it_watches_the_guard_script_itself(self) -> None:
        paths = load(GUARD)['on']['pull_request']['paths']
        self.assertIn('scripts/agent_rules_guard.py', paths)
        self.assertIn('runtime_contracts/**', paths)


if __name__ == '__main__':
    unittest.main()
