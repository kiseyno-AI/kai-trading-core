import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from app.core.test_database_safety import (
    UnsafeTestDatabaseError,
    assert_single_active_policy_fixture_state,
    assert_safe_test_database_settings,
    is_safe_test_database_url,
)


def test_test_database_guard_rejects_default_production_database_name() -> None:
    unsafe = SimpleNamespace(
        database_url=None,
        postgres_user='kai_trading_app',
        postgres_password='secret',
        postgres_host='localhost',
        postgres_port=5432,
        postgres_db='kai_trading',
    )

    with pytest.raises(UnsafeTestDatabaseError, match='kai_trading'):
        assert_safe_test_database_settings(unsafe)


@pytest.mark.parametrize(
    'database_url',
    [
        'postgresql+psycopg://kai:secret@127.0.0.1:5432/kai_trading_test',
        'postgresql://kai:secret@127.0.0.1:5432/kai_trading_test_worker_01',
        'postgresql+psycopg://kai:secret@127.0.0.1:5432/kai_trading_core_agent_integrity_test',
    ],
)
def test_test_database_guard_accepts_only_core_test_targets(database_url: str) -> None:
    assert is_safe_test_database_url(database_url) is True


@pytest.mark.parametrize(
    'database_url',
    [
        'postgresql+psycopg://kai:secret@127.0.0.1:5432/analytics_test',
        'postgresql://kai:secret@127.0.0.1:5432/test_kai_trading',
        'postgresql://kai:secret@127.0.0.1:5432/kai_exec_test',
        'postgresql://kai:secret@127.0.0.1:5432/kai_trading_test_',
        'postgresql://kai:secret@127.0.0.1:5432/kai_trading_core__test',
        'sqlite+pysqlite:///:memory:',
    ],
)
def test_test_database_guard_rejects_non_core_or_unsupported_test_targets(database_url: str) -> None:
    assert is_safe_test_database_url(database_url) is False


def test_test_database_guard_does_not_treat_test_in_credentials_as_safe() -> None:
    database_url = 'postgresql+psycopg://test_user:test_password@127.0.0.1:5432/kai_trading'

    assert is_safe_test_database_url(database_url) is False


def test_policy_fixture_guard_rejects_an_ambiguous_active_policy_baseline() -> None:
    with pytest.raises(UnsafeTestDatabaseError, match='multiple active signal-decider policies'):
        assert_single_active_policy_fixture_state(['policy-a', 'policy-b'])


def test_policy_fixture_guard_counts_duplicate_version_rows_as_ambiguous() -> None:
    with pytest.raises(UnsafeTestDatabaseError, match='multiple active signal-decider policies'):
        assert_single_active_policy_fixture_state(['policy-a', 'policy-a'])


def test_policy_fixture_guard_accepts_zero_or_one_active_policy() -> None:
    assert_single_active_policy_fixture_state([])
    assert_single_active_policy_fixture_state(['policy-a'])


def test_next_open_mutating_test_module_fails_closed_without_pytest_collection() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    module_path = repo_root / 'apps' / 'api' / 'tests' / 'test_next_open_execution_service.py'
    env = {
        **os.environ,
        'DATABASE_URL': 'postgresql+psycopg://user:secret@127.0.0.1:5432/kai_trading',
        'PYTHONPATH': str(repo_root / 'apps' / 'api'),
    }

    completed = subprocess.run(
        [
            sys.executable,
            '-c',
            'import runpy, sys; runpy.run_path(sys.argv[1])',
            str(module_path),
        ],
        cwd=repo_root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    assert 'Refusing to run tests against a non-Core test database' in completed.stderr
    assert 'kai_trading' in completed.stderr


def test_tests_package_fails_closed_for_unittest_module_imports() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env = {
        **os.environ,
        'DATABASE_URL': 'postgresql+psycopg://user:secret@127.0.0.1:5432/kai_trading',
        'PYTHONPATH': str(repo_root / 'apps' / 'api'),
    }

    completed = subprocess.run(
        [sys.executable, '-c', 'import tests.test_order_store'],
        cwd=repo_root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    assert 'Refusing to run tests against a non-Core test database' in completed.stderr
    assert 'kai_trading' in completed.stderr


def test_direct_db_backed_test_script_fails_closed_before_database_use() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    module_path = repo_root / 'apps' / 'api' / 'tests' / 'test_order_store.py'
    env = {
        **os.environ,
        'DATABASE_URL': 'postgresql+psycopg://user:secret@127.0.0.1:1/kai_trading',
        'PYTHONPATH': str(repo_root / 'apps' / 'api'),
    }

    completed = subprocess.run(
        [sys.executable, str(module_path)],
        cwd=repo_root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode != 0
    assert 'Refusing to run tests against a non-Core test database' in completed.stderr
    assert 'kai_trading' in completed.stderr


def test_unittest_discovery_fails_closed_before_database_use() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env = {
        **os.environ,
        'DATABASE_URL': 'postgresql+psycopg://user:secret@127.0.0.1:1/kai_trading',
        'PYTHONPATH': str(repo_root / 'apps' / 'api'),
    }

    completed = subprocess.run(
        [
            sys.executable,
            '-m',
            'unittest',
            'discover',
            '-s',
            'apps/api/tests',
            '-p',
            'test_order_store.py',
        ],
        cwd=repo_root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode != 0
    assert 'Refusing to run tests against a non-Core test database' in completed.stderr
    assert 'kai_trading' in completed.stderr


def test_normal_application_import_does_not_activate_the_test_runner_gate() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    env = {
        **os.environ,
        'DATABASE_URL': 'postgresql+psycopg://user:secret@127.0.0.1:1/kai_trading',
        'PYTHONPATH': str(repo_root / 'apps' / 'api'),
    }

    completed = subprocess.run(
        [sys.executable, '-c', 'import app.db; print("normal-import-ok")'],
        cwd=repo_root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == 'normal-import-ok'
