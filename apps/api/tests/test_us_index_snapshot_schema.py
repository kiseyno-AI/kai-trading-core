from datetime import UTC, date, datetime, timedelta
import importlib.util
from pathlib import Path

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
from sqlalchemy import MetaData, create_engine, inspect
from sqlalchemy.exc import IntegrityError

from app.db_models import USIndexInputSnapshot


def test_migration_matches_the_model_and_can_be_reversed(tmp_path) -> None:
    path = Path(__file__).resolve().parents[1] / 'db_migrations/versions/20261124_0091_us_index_input_snapshots.py'
    spec = importlib.util.spec_from_file_location(path.stem, path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    engine = create_engine(f'sqlite+pysqlite:///{tmp_path / "schema.sqlite"}')
    metadata = MetaData()
    table = USIndexInputSnapshot.__table__.to_metadata(metadata)
    try:
        with engine.begin() as connection:
            with Operations.context(MigrationContext.configure(connection)):
                migration.upgrade()
            assert compare_metadata(MigrationContext.configure(connection), metadata) == []
            assert inspect(connection).get_indexes(table.name)[0]['column_names'] == [
                'index_code', 'session_date', 'collected_at',
            ]

        with pytest.raises(IntegrityError):
            with engine.begin() as connection:
                now = datetime(2026, 11, 24, 16, tzinfo=UTC)
                connection.execute(table.insert().values(
                    snapshot_id='a' * 64, index_code='SPX', session_date=date(2026, 11, 23),
                    as_of=now, collected_at=now - timedelta(seconds=1), snapshot_json='{}',
                ))

        with engine.begin() as connection:
            with Operations.context(MigrationContext.configure(connection)):
                migration.downgrade()
            assert not inspect(connection).has_table(table.name)
    finally:
        engine.dispose()
