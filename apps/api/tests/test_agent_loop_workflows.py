from __future__ import annotations

from pathlib import Path
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[3]
LAB = ROOT / '.github' / 'workflows' / 'signal-lab-weekly.yml'
LOCAL = ROOT / 'scripts' / 'run_agent_improvement_local.sh'
GUARD = ROOT / '.github' / 'workflows' / 'agent-rules-guard.yml'


def load(path: Path) -> dict:
    data = yaml.safe_load(path.read_text())
    data['on'] = data.get('on', data.get(True))  # PyYAML parses a bare `on` key as boolean True
    return data


class LocalLoopSafetyTests(unittest.TestCase):
    """The loop runs on the owner's Mac from Core's background runner, never on GitHub Actions."""

    def test_the_lab_is_not_a_github_workflow_anymore(self) -> None:
        self.assertFalse(LAB.exists())

    def test_applying_is_opt_in(self) -> None:
        text = LOCAL.read_text()
        self.assertIn('APPLY=false', text)
        self.assertIn('AGENT_RULES_AUTO_PR', text)
        self.assertIn('ARGS+=(--apply)', text)
        self.assertNotIn('--apply \\', text)

    def test_numbers_stay_in_local_files(self) -> None:
        text = LOCAL.read_text()
        for log in ('last-run.log', 'last-findings.log', 'last-improvement.log'):
            self.assertIn(log, text)
        self.assertNotIn('cat "$REPORT"', text)

    def test_runs_once_per_week_on_saturday_kst_unless_forced(self) -> None:
        text = LOCAL.read_text()
        self.assertIn('TZ=Asia/Seoul date +%u', text)
        self.assertIn('[ "$DOW" = "6" ] || exit 0', text)
        self.assertIn('.done-$WEEK', text)
        self.assertIn('--force', text)

    def test_no_separate_launchd_job_any_more(self) -> None:
        self.assertFalse((ROOT / 'scripts' / 'install_agent_improvement_launchd.sh').exists())
        self.assertFalse((ROOT / 'scripts' / 'launchd' / 'com.kai.agent-improvement.plist.template').exists())


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

    def test_no_workflow_uses_a_self_hosted_runner_at_all(self) -> None:
        names = [p.name for p in (ROOT / '.github' / 'workflows').glob('*.y*ml') if 'self-hosted' in p.read_text()]
        self.assertEqual(names, [])


if __name__ == '__main__':
    unittest.main()
