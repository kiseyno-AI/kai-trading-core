from __future__ import annotations

import os
from pathlib import Path
import plistlib
import shlex
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

printf 'PYTHON_EXECUTABLE=%s\\n' "$0" >> "${FAKE_PYTHON_LOG:?}"
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
  printf 'UVICORN_APP_DEBUG=%s\\n' "${APP_DEBUG:-<unset>}" >> "${FAKE_PYTHON_LOG:?}"
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

    def test_default_python_runtime_is_312(self) -> None:
        script = self.script_path.read_text(encoding='utf-8')

        self.assertIn('PYTHON_BIN="python3.12"', script)
        self.assertNotIn('PYTHON_BIN="python3.11"', script)

    def test_launchd_uses_the_project_python312_virtualenv(self) -> None:
        plist_path = (
            Path(__file__).resolve().parent.parent
            / 'com.kiseyno.kai-trading-core-api.plist'
        )

        with plist_path.open('rb') as plist_file:
            launchd_config = plistlib.load(plist_file)

        python_bin = launchd_config['EnvironmentVariables']['PYTHON_BIN']
        self.assertTrue(python_bin.endswith('/apps/api/.venv/bin/python'))

    def _run_script(
        self,
        *,
        db_auto_upgrade: str | None,
        preflight_json: str | None = None,
        preflight_json_exit_code: int = 0,
        preflight_text_exit_code: int = 1,
        preflight_require_head_exit_code: int = 0,
        host: str | None = None,
        upstream_bearer_token: str | None = None,
        use_explicit_python: bool = True,
        virtual_env: Path | None = None,
        env_python_bin: Path | None = None,
        env_install_requirements: str | None = None,
        inherited_install_requirements: str | None = 'false',
        expected_returncode: int = 0,
    ) -> list[str]:
        env_path = self.workdir / '.env'
        env_lines = ['APP_ENV=test']
        if db_auto_upgrade is not None:
            env_lines.append(f'DB_AUTO_UPGRADE={db_auto_upgrade}')
        if upstream_bearer_token is not None:
            env_lines.append(f'UPSTREAM_BEARER_TOKEN={shlex.quote(upstream_bearer_token)}')
        if env_python_bin is not None:
            env_lines.append(f'PYTHON_BIN={shlex.quote(str(env_python_bin))}')
        if env_install_requirements is not None:
            env_lines.append(f'INSTALL_REQUIREMENTS_ON_START={shlex.quote(env_install_requirements)}')
        env_path.write_text('\n'.join(env_lines) + '\n', encoding='utf-8')

        env = os.environ.copy()
        # The launcher intentionally honors inherited operator settings. Tests
        # must start from a clean process environment so a developer's shell
        # cannot silently supply auth, host, or migration controls.
        for key in (
            'APP_DEBUG',
            'DB_AUTO_UPGRADE',
            'HOST',
            'UPSTREAM_BEARER_TOKEN',
            'VIRTUAL_ENV',
        ):
            env.pop(key, None)
        if use_explicit_python:
            env['PYTHON_BIN'] = str(self.wrapper_path)
        else:
            env.pop('PYTHON_BIN', None)
        if virtual_env is not None:
            env['VIRTUAL_ENV'] = str(virtual_env)
        env['FAKE_PYTHON_LOG'] = str(self.log_path)
        if inherited_install_requirements is None:
            env.pop('INSTALL_REQUIREMENTS_ON_START', None)
        else:
            env['INSTALL_REQUIREMENTS_ON_START'] = inherited_install_requirements
        if host is not None:
            env['HOST'] = host
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
        if not self.log_path.exists():
            return []
        return self.log_path.read_text(encoding='utf-8').splitlines()

    def test_active_virtualenv_python_is_used_without_an_explicit_override(self) -> None:
        virtual_env = self.workdir / 'active-venv'
        virtualenv_python = virtual_env / 'bin' / 'python'
        virtualenv_python.parent.mkdir(parents=True)
        virtualenv_python.symlink_to(self.wrapper_path)

        log_lines = self._run_script(
            db_auto_upgrade='false',
            use_explicit_python=False,
            virtual_env=virtual_env,
        )

        self.assertIn(f'PYTHON_EXECUTABLE={virtualenv_python}', log_lines)
        self.assertTrue(any('-m uvicorn app.main:app' in line for line in log_lines))

    def test_env_python_is_loaded_before_dependency_probe(self) -> None:
        log_lines = self._run_script(
            db_auto_upgrade='false',
            use_explicit_python=False,
            env_python_bin=self.wrapper_path,
        )

        self.assertIn(f'PYTHON_EXECUTABLE={self.wrapper_path}', log_lines)
        self.assertTrue(any('-m uvicorn app.main:app' in line for line in log_lines))

    def test_inherited_python_override_wins_over_env_file(self) -> None:
        log_lines = self._run_script(
            db_auto_upgrade='false',
            env_python_bin=self.workdir / 'missing-python-from-env',
        )

        self.assertIn(f'PYTHON_EXECUTABLE={self.wrapper_path}', log_lines)
        self.assertTrue(any('-m uvicorn app.main:app' in line for line in log_lines))

    def test_env_install_control_is_loaded_before_bootstrap_decision(self) -> None:
        log_lines = self._run_script(
            db_auto_upgrade='false',
            use_explicit_python=False,
            env_python_bin=self.wrapper_path,
            env_install_requirements='true',
            inherited_install_requirements=None,
        )

        self.assertTrue(any('-m pip install -r requirements.txt' in line for line in log_lines))

    def test_non_loopback_bind_requires_bearer_token(self) -> None:
        log_lines = self._run_script(
            db_auto_upgrade='false',
            host='0.0.0.0',
            expected_returncode=1,
        )

        self.assertFalse(any('-m uvicorn app.main:app' in line for line in log_lines))

    def test_non_loopback_bind_rejects_whitespace_only_bearer_token(self) -> None:
        log_lines = self._run_script(
            db_auto_upgrade='false',
            host='0.0.0.0',
            upstream_bearer_token='   ',
            expected_returncode=1,
        )

        self.assertFalse(any('-m uvicorn app.main:app' in line for line in log_lines))

    def test_non_loopback_bind_with_bearer_token_disables_debug(self) -> None:
        log_lines = self._run_script(
            db_auto_upgrade='false',
            host='0.0.0.0',
            upstream_bearer_token='internal-secret',
        )

        self.assertTrue(any('-m uvicorn app.main:app --host 0.0.0.0' in line for line in log_lines))
        self.assertIn('UVICORN_APP_DEBUG=false', log_lines)

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
