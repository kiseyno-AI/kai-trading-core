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


class AnySelfHostedWorkflowTests(unittest.TestCase):
    """The repository is public: code from a stranger's pull request must never reach the owner's Mac.

    This scans EVERY workflow, so a future workflow (or a later edit) that puts a self-hosted
    job behind a pull request, comment, issue or fork trigger fails CI instead of exposing the machine.
    """

    SAFE_TRIGGERS = {'schedule', 'workflow_dispatch'}

    def test_no_self_hosted_job_can_be_reached_by_outsiders(self) -> None:
        offenders = []
        for path in sorted((ROOT / '.github' / 'workflows').glob('*.y*ml')):
            data = load(path)
            uses_self_hosted = any(
                'self-hosted' in (job.get('runs-on') if isinstance(job.get('runs-on'), list) else [job.get('runs-on')])
                for job in (data.get('jobs') or {}).values()
            )
            if not uses_self_hosted:
                continue
            triggers = data['on']
            names = set(triggers) if isinstance(triggers, dict) else ({triggers} if isinstance(triggers, str) else set(triggers))
            if not names <= self.SAFE_TRIGGERS:
                offenders.append((path.name, sorted(names - self.SAFE_TRIGGERS)))
        self.assertEqual(offenders, [])

    def test_the_scan_really_finds_the_lab_workflow(self) -> None:
        names = [p.name for p in (ROOT / '.github' / 'workflows').glob('*.y*ml') if 'self-hosted' in p.read_text()]
        self.assertIn('signal-lab-weekly.yml', names)


if __name__ == '__main__':
    unittest.main()
