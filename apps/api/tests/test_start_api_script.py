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

if [ "${1:-}" = "-m" ] && [ "${2:-}" = "app.internal.deploy_hygiene" ]; then
  if [ "${3:-}" = "preflight" ] && [ "${4:-}" = "--format" ] && [ "${5:-}" = "json" ]; then
    printf '%s\\n' "${FAKE_PREFLIGHT_JSON:-{\\"status\\":\\"ok\\",\\"safe_to_continue\\":true,\\"alembic\\":{\\"at_head\\":true}}}"
    exit "${FAKE_PREFLIGHT_JSON_EXIT_CODE:-0}"
  fi

  if [ "${3:-}" = "preflight" ] && [ "${4:-}" = "--require-head" ] && [ "${5:-}" = "--format" ] && [ "${6:-}" = "text" ]; then
    printf '%s\\n' "${FAKE_PREFLIGHT_REQUIRE_HEAD_TEXT:-OK migration preflight: Database is already at the Alembic head revision.}"
    exit "${FAKE_PREFLIGHT_REQUIRE_HEAD_EXIT_CODE:-0}"
  fi

  if [ "${3:-}" = "preflight" ] && [ "${4:-}" = "--format" ] && [ "${5:-}" = "text" ]; then
    printf '%s\\n' "${FAKE_PREFLIGHT_TEXT:-FAIL migration preflight: simulated preflight failure.}"
    exit "${FAKE_PREFLIGHT_TEXT_EXIT_CODE:-1}"
  fi
fi

if [ "${1:-}" = "-m" ] && [ "${2:-}" = "uvicorn" ]; then
  printf 'UVICORN_DB_AUTO_UPGRADE=%s\\n' "${DB_AUTO_UPGRADE:-<unset>}" >> "${FAKE_PYTHON_LOG:?}"
  exit 0
fi

if [ "${1:-}" = "-m" ] && [ "${2:-}" = "alembic" ]; then
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

    def _run_script(
        self,
        *,
        db_auto_upgrade: str | None,
        preflight_json: str | None = None,
        preflight_json_exit_code: int = 0,
        preflight_text_exit_code: int = 1,
        preflight_require_head_exit_code: int = 0,
        expected_returncode: int = 0,
    ) -> list[str]:
        env_path = self.workdir / '.env'
        env_lines = ['APP_ENV=test']
        if db_auto_upgrade is not None:
            env_lines.append(f'DB_AUTO_UPGRADE={db_auto_upgrade}')
        env_path.write_text('\n'.join(env_lines) + '\n', encoding='utf-8')

        env = os.environ.copy()
        env['PYTHON_BIN'] = str(self.wrapper_path)
        env['FAKE_PYTHON_LOG'] = str(self.log_path)
        env['INSTALL_REQUIREMENTS_ON_START'] = 'false'
        if preflight_json is not None:
            env['FAKE_PREFLIGHT_JSON'] = preflight_json
        env['FAKE_PREFLIGHT_JSON_EXIT_CODE'] = str(preflight_json_exit_code)
        env['FAKE_PREFLIGHT_TEXT_EXIT_CODE'] = str(preflight_text_exit_code)
        env['FAKE_PREFLIGHT_REQUIRE_HEAD_EXIT_CODE'] = str(preflight_require_head_exit_code)

        result = subprocess.run(
            [str(self.script_path)],
            cwd=self.workdir,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, expected_returncode, msg=f'stdout={result.stdout}\nstderr={result.stderr}')
        return self.log_path.read_text(encoding='utf-8').splitlines()

    def test_truthy_db_auto_upgrade_values_run_preflight_and_disable_runtime_upgrade_when_at_head(self) -> None:
        for value in ('true', 'True', 'TRUE', '1', 'yes', 'on'):
            with self.subTest(value=value):
                log_lines = self._run_script(
                    db_auto_upgrade=value,
                    preflight_json='{"status":"ok","safe_to_continue":true,"alembic":{"at_head":true}}',
                )
                self.assertTrue(
                    any('-m app.internal.deploy_hygiene preflight --format json' in line for line in log_lines),
                    msg=log_lines,
                )
                self.assertTrue(
                    any('-m app.internal.deploy_hygiene preflight --require-head --format text' in line for line in log_lines),
                    msg=log_lines,
                )
                self.assertFalse(
                    any('-m alembic upgrade head' in line for line in log_lines),
                    msg=log_lines,
                )
                self.assertTrue(
                    any('-m uvicorn app.main:app' in line for line in log_lines),
                    msg=log_lines,
                )
                self.assertIn('UVICORN_DB_AUTO_UPGRADE=false', log_lines, msg=log_lines)
                self.log_path.unlink()

    def test_preflight_runs_alembic_before_uvicorn_when_database_is_behind_head(self) -> None:
        log_lines = self._run_script(
            db_auto_upgrade='true',
            preflight_json='{"status":"warn","safe_to_continue":true,"alembic":{"at_head":false}}',
        )

        self.assertTrue(
            any('-m app.internal.deploy_hygiene preflight --format json' in line for line in log_lines),
            msg=log_lines,
        )
        self.assertTrue(
            any('-m alembic upgrade head' in line for line in log_lines),
            msg=log_lines,
        )
        self.assertTrue(
            any('-m app.internal.deploy_hygiene preflight --require-head --format text' in line for line in log_lines),
            msg=log_lines,
        )
        self.assertIn('UVICORN_DB_AUTO_UPGRADE=false', log_lines, msg=log_lines)

    def test_preflight_skips_for_falsey_db_auto_upgrade_values(self) -> None:
        for value in ('false', 'False', 'FALSE', '0', 'no', 'off'):
            with self.subTest(value=value):
                log_lines = self._run_script(db_auto_upgrade=value)
                self.assertFalse(
                    any('-m app.internal.deploy_hygiene preflight' in line for line in log_lines),
                    msg=log_lines,
                )
                self.assertFalse(
                    any('-m alembic upgrade head' in line for line in log_lines),
                    msg=log_lines,
                )
                self.assertIn(f'UVICORN_DB_AUTO_UPGRADE={value}', log_lines, msg=log_lines)
                self.log_path.unlink()

    def test_preflight_defaults_to_enabled_when_db_auto_upgrade_is_unset(self) -> None:
        log_lines = self._run_script(
            db_auto_upgrade=None,
            preflight_json='{"status":"ok","safe_to_continue":true,"alembic":{"at_head":true}}',
        )

        self.assertTrue(
            any('-m app.internal.deploy_hygiene preflight --format json' in line for line in log_lines),
            msg=log_lines,
        )
        self.assertTrue(
            any('-m uvicorn app.main:app' in line for line in log_lines),
            msg=log_lines,
        )
        self.assertIn('UVICORN_DB_AUTO_UPGRADE=false', log_lines, msg=log_lines)

    def test_unsafe_preflight_stops_before_uvicorn(self) -> None:
        log_lines = self._run_script(
            db_auto_upgrade='true',
            preflight_json_exit_code=1,
            preflight_text_exit_code=1,
            expected_returncode=1,
        )

        self.assertTrue(
            any('-m app.internal.deploy_hygiene preflight --format json' in line for line in log_lines),
            msg=log_lines,
        )
        self.assertTrue(
            any('-m app.internal.deploy_hygiene preflight --format text' in line for line in log_lines),
            msg=log_lines,
        )
        self.assertFalse(
            any('-m alembic upgrade head' in line for line in log_lines),
            msg=log_lines,
        )
        self.assertFalse(
            any('-m uvicorn app.main:app' in line for line in log_lines),
            msg=log_lines,
        )
