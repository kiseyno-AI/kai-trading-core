from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy.engine import make_url


CORE_TEST_DATABASE_NAME = 'kai_trading_test'
CORE_ISOLATED_TEST_DATABASE_PREFIX = 'kai_trading_core_'
CORE_ISOLATED_TEST_DATABASE_SUFFIX = '_test'


class UnsafeTestDatabaseError(RuntimeError):
    """Raised before tests can bind application storage to a non-test database."""


def _effective_database_url(settings: Any) -> str:
    explicit = str(getattr(settings, 'database_url', '') or '').strip()
    if explicit:
        return explicit
    return (
        f"postgresql+psycopg://{getattr(settings, 'postgres_user', '')}:"
        f"{getattr(settings, 'postgres_password', '')}@{getattr(settings, 'postgres_host', '')}:"
        f"{getattr(settings, 'postgres_port', '')}/{getattr(settings, 'postgres_db', '')}"
    )


def is_safe_test_database_url(database_url: str) -> bool:
    try:
        parsed = make_url(database_url)
    except Exception:
        return False
    if parsed.get_backend_name() != 'postgresql':
        return False
    database_name = str(parsed.database or '').strip().lower()
    if database_name == CORE_TEST_DATABASE_NAME:
        return True
    worker_prefix = f'{CORE_TEST_DATABASE_NAME}_'
    if database_name.startswith(worker_prefix) and database_name[len(worker_prefix) :]:
        return True
    if not (
        database_name.startswith(CORE_ISOLATED_TEST_DATABASE_PREFIX)
        and database_name.endswith(CORE_ISOLATED_TEST_DATABASE_SUFFIX)
    ):
        return False
    isolated_scope = database_name[
        len(CORE_ISOLATED_TEST_DATABASE_PREFIX) : -len(CORE_ISOLATED_TEST_DATABASE_SUFFIX)
    ]
    return bool(isolated_scope)


def assert_safe_test_database_settings(settings: Any) -> None:
    database_url = _effective_database_url(settings)
    if is_safe_test_database_url(database_url):
        return
    try:
        database_name = make_url(database_url).database or '<missing>'
    except Exception:
        database_name = '<invalid>'
    raise UnsafeTestDatabaseError(
        'Refusing to run tests against a non-Core test database '
        f'({database_name}). Set DATABASE_URL to the dedicated {CORE_TEST_DATABASE_NAME} database '
        f'or an isolated {CORE_TEST_DATABASE_NAME}_<suffix> or '
        f'{CORE_ISOLATED_TEST_DATABASE_PREFIX}<scope>{CORE_ISOLATED_TEST_DATABASE_SUFFIX} database.'
    )


def assert_single_active_policy_fixture_state(policy_versions: Sequence[str]) -> None:
    normalized = sorted(
        str(version).strip()
        for version in policy_versions
        if str(version).strip()
    )
    if len(normalized) <= 1:
        return
    raise UnsafeTestDatabaseError(
        'Refusing to modify an ambiguous fixture baseline with multiple active signal-decider policies: '
        f'{", ".join(normalized)}.'
    )
