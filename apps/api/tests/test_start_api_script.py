from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class StartAPIScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.workdir = Path(self.temp_dir.name)

        source_script = Path(__file__).resolve().parent.parent / 'start-api.sh'
        self.script_path = self.workdir / 'start-api.sh'
        self.script_path.write_text(source_script.read_text(encoding='utf-8'), encoding='utf-8')
        self.script_path.chmod(0o755)

        self.log_path = self.workdir / 'fake-python.log'
        self.wrapper_path = self.workdir / 'fake-python'
        self.wrapper_path.write_text(
            """#!/usr/bin/env bash
set -euo pipefail

printf '%s\\n' "$*" >> "${FAKE_PYTHON_LOG:?}"

if [ "${1:-}" = "-" ]; then
  cat >/dev/null
  exit 0
fi

if [ "${1:-}" = "-m" ] && [ "${2:-}" = "app.internal.deploy_hygiene" ]; then
  exit 0
fi

if [ "${1:-}" = "-m" ] && [ "${2:-}" = "uvicorn" ]; then
  exit 0
fi

if [ "${1:-}" = "-m" ] && [ "${2:-}" = "pip" ]; then
  exit 0
fi

echo "unexpected invocation: $*" >&2
exit 1
""",
            encoding='utf-8',
        )
        self.wrapper_path.chmod(0o755)

    def _run_script(self, *, db_auto_upgrade: str | None) -> list[str]:
        env_path = self.workdir / '.env'
        env_lines = ['APP_ENV=test']
        if db_auto_upgrade is not None:
            env_lines.append(f'DB_AUTO_UPGRADE={db_auto_upgrade}')
        env_path.write_text('\n'.join(env_lines) + '\n', encoding='utf-8')

        env = os.environ.copy()
        env['PYTHON_BIN'] = str(self.wrapper_path)
        env['FAKE_PYTHON_LOG'] = str(self.log_path)
        env['INSTALL_REQUIREMENTS_ON_START'] = 'false'

        result = subprocess.run(
            [str(self.script_path)],
            cwd=self.workdir,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=f'stdout={result.stdout}\nstderr={result.stderr}')
        return self.log_path.read_text(encoding='utf-8').splitlines()

    def test_preflight_runs_for_truthy_db_auto_upgrade_values(self) -> None:
        for value in ('true', 'True', 'TRUE', '1', 'yes', 'on'):
            with self.subTest(value=value):
                log_lines = self._run_script(db_auto_upgrade=value)
                self.assertTrue(
                    any('-m app.internal.deploy_hygiene preflight --format text' in line for line in log_lines),
                    msg=log_lines,
                )
                self.assertTrue(
                    any('-m uvicorn app.main:app' in line for line in log_lines),
                    msg=log_lines,
                )
                self.log_path.unlink()

    def test_preflight_skips_for_falsey_db_auto_upgrade_values(self) -> None:
        for value in ('false', 'False', 'FALSE', '0', 'no', 'off'):
            with self.subTest(value=value):
                log_lines = self._run_script(db_auto_upgrade=value)
                self.assertFalse(
                    any('-m app.internal.deploy_hygiene preflight --format text' in line for line in log_lines),
                    msg=log_lines,
                )
                self.assertTrue(
                    any('-m uvicorn app.main:app' in line for line in log_lines),
                    msg=log_lines,
                )
                self.log_path.unlink()

    def test_preflight_defaults_to_enabled_when_db_auto_upgrade_is_unset(self) -> None:
        log_lines = self._run_script(db_auto_upgrade=None)

        self.assertTrue(
            any('-m app.internal.deploy_hygiene preflight --format text' in line for line in log_lines),
            msg=log_lines,
        )
        self.assertTrue(
            any('-m uvicorn app.main:app' in line for line in log_lines),
            msg=log_lines,
        )
