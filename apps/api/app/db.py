from contextlib import contextmanager
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.core.settings import settings
from app.db_models import Base


def _build_database_url() -> str:
    if settings.database_url:
        return settings.database_url
    return (
        f"postgresql+psycopg://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
    )


DATABASE_URL = _build_database_url()
engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


@contextmanager
def session_scope():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _alembic_config() -> Config:
    config = Config(str(Path(__file__).resolve().parent.parent / 'alembic.ini'))
    config.set_main_option('script_location', str(Path(__file__).resolve().parent.parent / 'db_migrations'))
    config.set_main_option('sqlalchemy.url', DATABASE_URL)
    return config


def run_migrations() -> None:
    command.upgrade(_alembic_config(), 'head')


def init_database() -> None:
    if settings.db_auto_upgrade:
        run_migrations()


def check_database() -> tuple[bool, str | None]:
    try:
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        return True, None
    except Exception as exc:  # pragma: no cover
        return False, str(exc)
