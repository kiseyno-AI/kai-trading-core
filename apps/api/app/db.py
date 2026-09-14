from contextlib import contextmanager
import logging
from pathlib import Path
import sys
import threading
import time

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.orm import sessionmaker

from app.core.settings import settings
from app.db_models import Base

logger = logging.getLogger(__name__)
_database_healthcheck_lock = threading.Lock()


def _running_under_database_test_runner() -> bool:
    main_module = sys.modules.get('__main__')
    main_spec_name = str(
        getattr(getattr(main_module, '__spec__', None), 'name', '') or ''
    )
    if main_spec_name in {'unittest.__main__', 'pytest.__main__'}:
        return True
    if 'pytest' in sys.modules:
        return True
    main_file = str(getattr(main_module, '__file__', '') or '').strip()
    if not main_file:
        return False
    main_path = Path(main_file).resolve()
    return main_path.name.startswith('test_') and any(
        parent.name == 'tests' for parent in main_path.parents
    )


if _running_under_database_test_runner():
    from app.core.test_database_safety import assert_safe_test_database_settings

    assert_safe_test_database_settings(settings)


def _build_engine_connect_args() -> dict[str, object]:
    return {
        'client_encoding': 'UTF8',
        'connect_timeout': settings.db_connect_timeout_seconds,
        'options': (
            f'-c statement_timeout={settings.db_statement_timeout_ms} '
            f'-c lock_timeout={settings.db_lock_timeout_ms}'
        ),
    }


def _build_engine_kwargs() -> dict[str, object]:
    return {
        'pool_size': settings.db_pool_size,
        'max_overflow': settings.db_max_overflow,
        'pool_timeout': settings.db_pool_timeout_seconds,
        'pool_recycle': settings.db_pool_recycle_seconds,
        'pool_pre_ping': True,
        'pool_use_lifo': settings.db_pool_use_lifo,
        'future': True,
        'connect_args': _build_engine_connect_args(),
    }


def _build_database_url() -> str:
    if settings.database_url:
        return settings.database_url
    return (
        f"postgresql+psycopg://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
    )


DATABASE_URL = _build_database_url()
engine = create_engine(DATABASE_URL, **_build_engine_kwargs())
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def pool_status() -> dict[str, object]:
    pool = engine.pool
    status: dict[str, object] = {
        'description': pool.status(),
        'configured_pool_size': settings.db_pool_size,
        'configured_max_overflow': settings.db_max_overflow,
        'configured_capacity': settings.db_pool_size + settings.db_max_overflow,
    }
    for key, method_name in (
        ('pool_size', 'size'),
        ('checked_in', 'checkedin'),
        ('checked_out', 'checkedout'),
        ('overflow', 'overflow'),
    ):
        method = getattr(pool, method_name, None)
        if callable(method):
            status[key] = method()
    return status


@event.listens_for(engine, 'checkout')
def _record_connection_checkout(_dbapi_connection, connection_record, _connection_proxy) -> None:
    connection_record.info['kai_checked_out_at'] = time.monotonic()


@event.listens_for(engine, 'checkin')
def _log_long_connection_checkout(_dbapi_connection, connection_record) -> None:
    checked_out_at = connection_record.info.pop('kai_checked_out_at', None)
    if checked_out_at is None:
        return
    elapsed = time.monotonic() - checked_out_at
    if elapsed >= settings.db_long_checkout_warning_seconds:
        logger.warning(
            'Database connection was checked out for %.2fs; pool_status=%s',
            elapsed,
            pool_status(),
        )


@event.listens_for(engine, 'handle_error')
def _log_database_disconnect(exception_context) -> None:
    if exception_context.is_disconnect:
        logger.warning(
            'Database disconnect detected; SQLAlchemy will invalidate the connection; pool_status=%s error=%s',
            pool_status(),
            exception_context.original_exception,
        )


@contextmanager
def session_scope(
    *,
    statement_timeout_ms: int | None = None,
    lock_timeout_ms: int | None = None,
    deadline_monotonic: float | None = None,
):
    session = SessionLocal()
    try:
        if deadline_monotonic is not None:
            # Checkout is intentionally forced before calculating the SQL
            # limits. QueuePool may spend part of the caller's budget waiting
            # for a connection, so a timeout calculated before checkout would
            # let the following statement run past the aggregate deadline.
            session.connection()
            remaining_timeout_ms = int(
                (deadline_monotonic - time.monotonic()) * 1000
            )
            if remaining_timeout_ms <= 0:
                raise TimeoutError('database session deadline exhausted during checkout')
            statement_timeout_ms = min(
                statement_timeout_ms or remaining_timeout_ms,
                remaining_timeout_ms,
            )
            lock_timeout_ms = min(
                lock_timeout_ms or remaining_timeout_ms,
                remaining_timeout_ms,
            )
        if statement_timeout_ms is not None:
            session.execute(text(f'SET LOCAL statement_timeout = {int(statement_timeout_ms)}'))
        if lock_timeout_ms is not None:
            session.execute(text(f'SET LOCAL lock_timeout = {int(lock_timeout_ms)}'))
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_alembic_config() -> Config:
    config = Config(str(Path(__file__).resolve().parent.parent / 'alembic.ini'))
    config.set_main_option('script_location', str(Path(__file__).resolve().parent.parent / 'db_migrations'))
    config.set_main_option('sqlalchemy.url', DATABASE_URL)
    return config


def run_migrations() -> None:
    command.upgrade(get_alembic_config(), 'head')


def init_database() -> None:
    if settings.db_auto_upgrade:
        run_migrations()


def check_database() -> tuple[bool, str | None]:
    status = pool_status()
    if not _database_healthcheck_lock.acquire(blocking=False):
        return False, f'database health check already in progress; pool_status={status}'
    try:
        checked_out = status.get('checked_out')
        capacity = status['configured_capacity']
        if isinstance(checked_out, int) and checked_out >= capacity:
            return False, f'database pool exhausted; pool_status={status}'
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        return True, None
    except SQLAlchemyTimeoutError as exc:
        return False, f'database pool checkout timed out: {exc}; pool_status={pool_status()}'
    except Exception as exc:  # pragma: no cover
        return False, f'{exc}; pool_status={pool_status()}'
    finally:
        _database_healthcheck_lock.release()
