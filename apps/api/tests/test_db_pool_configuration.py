from unittest.mock import patch

from app import db
from app.db_pool_watchdog import DatabasePoolWatchdog


class _DeadlineSession:
    def __init__(self) -> None:
        self.connection_calls = 0
        self.statements = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def connection(self):
        self.connection_calls += 1
        return object()

    def execute(self, statement):
        self.statements.append(str(statement))

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def test_engine_uses_resilient_pool_and_connect_settings() -> None:
    kwargs = db._build_engine_kwargs()

    assert kwargs['pool_pre_ping'] is True
    assert kwargs['pool_recycle'] == db.settings.db_pool_recycle_seconds
    assert kwargs['pool_use_lifo'] == db.settings.db_pool_use_lifo
    assert kwargs['connect_args']['connect_timeout'] == db.settings.db_connect_timeout_seconds


def test_database_health_fails_fast_when_pool_is_exhausted() -> None:
    exhausted = {
        'description': 'test pool exhausted',
        'configured_pool_size': 2,
        'configured_max_overflow': 1,
        'configured_capacity': 3,
        'checked_out': 3,
    }

    with patch('app.db.pool_status', return_value=exhausted):
        connected, error = db.check_database()

    assert connected is False
    assert error is not None
    assert 'pool exhausted' in error


def test_database_health_does_not_stack_connection_attempts() -> None:
    in_progress_lock = db._database_healthcheck_lock
    assert in_progress_lock.acquire(blocking=False) is True
    try:
        connected, error = db.check_database()
    finally:
        in_progress_lock.release()

    assert connected is False
    assert error is not None
    assert 'already in progress' in error


def test_session_scope_recomputes_statement_limits_after_deadline_pool_checkout() -> None:
    session = _DeadlineSession()

    with (
        patch.object(db, 'SessionLocal', return_value=session),
        patch.object(db.time, 'monotonic', return_value=103.0),
    ):
        with db.session_scope(
            statement_timeout_ms=30_000,
            lock_timeout_ms=15_000,
            deadline_monotonic=110.0,
        ):
            pass

    assert session.connection_calls == 1
    assert session.statements == [
        'SET LOCAL statement_timeout = 7000',
        'SET LOCAL lock_timeout = 7000',
    ]
    assert session.committed is True
    assert session.rolled_back is False
    assert session.closed is True


class _WatchdogCursor:
    def __init__(self) -> None:
        self.last_query = ''
        self.cancelled_pid = None

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, query, params=None) -> None:
        self.last_query = str(query)
        if 'pg_cancel_backend' in self.last_query:
            self.cancelled_pid = params[0]

    def fetchone(self):
        if 'count(*)' in self.last_query:
            return (db.settings.db_pool_watchdog_active_connection_threshold,)
        if 'pg_cancel_backend' in self.last_query:
            return (True,)
        raise AssertionError(f'unexpected fetchone query: {self.last_query}')

    def fetchall(self):
        return [(42, 'select * from daily_signal_features where symbol_id = 1')]


class _WatchdogConnection:
    def __init__(self) -> None:
        self.cursor_instance = _WatchdogCursor()

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def cursor(self) -> _WatchdogCursor:
        return self.cursor_instance


def test_watchdog_can_cancel_only_observed_slow_reads_when_enabled() -> None:
    connection = _WatchdogConnection()
    with (
        patch('app.db_pool_watchdog.psycopg.connect', return_value=connection),
        patch.object(db.settings, 'db_pool_watchdog_cancel_enabled', True),
    ):
        sweep = DatabasePoolWatchdog().sweep_once()

    assert sweep.slow_read_pids == [42]
    assert sweep.cancelled_pids == [42]
    assert connection.cursor_instance.cancelled_pid == 42
