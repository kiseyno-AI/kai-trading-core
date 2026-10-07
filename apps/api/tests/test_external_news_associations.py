from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
import sqlalchemy as sa
from sqlalchemy import create_engine, delete, select
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.orm import Session, sessionmaker

from app.db_models import Base, ExternalNewsItem as StoredExternalNewsItem
from app.db_models import ExternalNewsItemClassification, ExternalNewsItemSymbol, Symbol
from app.eod.pipeline_status import _load_external_news_counts
from app.external_news.provider import ExternalNewsItem
from app.external_news.store import (
    _build_external_news_classification_insert_statement,
    _build_external_news_enrichment_candidate_statement,
    _build_external_news_insert_statement,
    _build_external_news_symbol_insert_statement,
    _find_existing_news_item,
    enrich_recent_external_news_classification,
    list_external_news_items,
    persist_external_news_items,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / 'db_migrations'
    / 'versions'
    / '20260906_0078_external_news_symbol_associations.py'
)
CLASSIFICATION_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / 'db_migrations'
    / 'versions'
    / '20260906_0079_external_news_classification_versions.py'
)


def _load_migration(path: Path = MIGRATION_PATH):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'could not load migration {path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@contextmanager
def _sqlite_session_scope(session_factory: sessionmaker[Session]):
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class ExternalNewsAssociationMigrationTests(unittest.TestCase):
    def test_article_and_association_idempotency_sql_compile_for_sqlite_and_postgresql(self):
        article_values = {
            'symbol_id': 1,
            'provider': 'fake_news',
            'provider_item_id': 'item-1',
            'source': 'fake_news',
            'title': 'title',
            'description': None,
            'url': 'https://example.test/news/1',
            'source_url': None,
            'published_at': None,
            'collected_at': datetime(2026, 5, 8, tzinfo=timezone.utc),
            'payload': {},
        }
        association_values = {
            'external_news_item_id': 1,
            'symbol_id': 1,
            'associated_at': datetime(2026, 5, 8, tzinfo=timezone.utc),
        }
        classification_values = {
            'external_news_item_id': 1,
            'classifier_version': 'rule_based_external_news_contract_v1',
            'classified_at': datetime(2026, 5, 8, tzinfo=timezone.utc),
            'classification_payload': {'classifier': 'rule_based_external_news_contract_v1'},
        }

        for dialect_name, dialect in (
            ('sqlite', sqlite.dialect()),
            ('postgresql', postgresql.dialect()),
        ):
            with self.subTest(dialect=dialect_name):
                article_sql = str(
                    _build_external_news_insert_statement(
                        article_values,
                        dialect_name=dialect_name,
                    ).compile(dialect=dialect)
                )
                association_sql = str(
                    _build_external_news_symbol_insert_statement(
                        association_values,
                        dialect_name=dialect_name,
                    ).compile(dialect=dialect)
                )
                classification_sql = str(
                    _build_external_news_classification_insert_statement(
                        classification_values,
                        dialect_name=dialect_name,
                    ).compile(dialect=dialect)
                )
                self.assertIn('ON CONFLICT DO NOTHING', article_sql)
                self.assertIn('ON CONFLICT', association_sql)
                self.assertIn('DO NOTHING', association_sql)
                self.assertIn('ON CONFLICT', classification_sql)
                self.assertIn('DO NOTHING', classification_sql)

    def test_symbol_scoped_enrichment_query_uses_correlated_exists_without_distinct(self):
        association_start = datetime(2026, 10, 6, 15, 0, tzinfo=timezone.utc)
        association_end = datetime(2026, 10, 7, 15, 0, tzinfo=timezone.utc)

        statement = _build_external_news_enrichment_candidate_statement(
            symbols=['000660'],
            since=datetime(2026, 9, 23, tzinfo=timezone.utc),
            association_start=association_start,
            association_end=association_end,
        )

        compiled = statement.compile(dialect=postgresql.dialect())
        sql = str(compiled)
        self.assertIn('EXISTS (SELECT', sql)
        self.assertIn('external_news_item_symbols.associated_at >=', sql)
        self.assertIn('external_news_item_symbols.associated_at <', sql)
        self.assertIn('symbols.symbol IN', sql)
        self.assertNotIn('SELECT DISTINCT', sql)
        self.assertIn(association_start, compiled.params.values())
        self.assertIn(association_end, compiled.params.values())

    def test_enrichment_query_accepts_empty_frozen_current_day_window(self):
        captured_at_midnight = datetime(2026, 10, 6, 15, 0, tzinfo=timezone.utc)

        statement = _build_external_news_enrichment_candidate_statement(
            symbols=['000660'],
            since=datetime(2026, 9, 23, tzinfo=timezone.utc),
            association_start=captured_at_midnight,
            association_end=captured_at_midnight,
        )

        self.assertNotIn(
            'SELECT DISTINCT',
            str(statement.compile(dialect=postgresql.dialect())),
        )

    def test_sqlite_upgrade_backfills_original_attribution_and_supports_down_up(self):
        migration = _load_migration()
        engine = create_engine('sqlite+pysqlite:///:memory:')
        metadata = sa.MetaData()
        symbols = sa.Table(
            'symbols',
            metadata,
            sa.Column('id', sa.Integer, primary_key=True),
        )
        news = sa.Table(
            'external_news_items',
            metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('symbol_id', sa.Integer, nullable=False),
            sa.Column('collected_at', sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        )
        metadata.create_all(engine)
        collected_at = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)

        with engine.begin() as connection:
            connection.execute(symbols.insert(), [{'id': 1}, {'id': 2}])
            connection.execute(
                news.insert(),
                [
                    {'id': 10, 'symbol_id': 1, 'collected_at': collected_at},
                    {'id': 11, 'symbol_id': 2, 'collected_at': collected_at + timedelta(minutes=1)},
                ],
            )
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, 'op', operations):
                migration.upgrade()

        association = sa.Table(migration.TABLE_NAME, sa.MetaData(), autoload_with=engine)
        with engine.connect() as connection:
            rows = connection.execute(
                select(
                    association.c.external_news_item_id,
                    association.c.symbol_id,
                    association.c.associated_at,
                ).order_by(association.c.external_news_item_id)
            ).all()
        self.assertEqual([(row.external_news_item_id, row.symbol_id) for row in rows], [(10, 1), (11, 2)])
        self.assertEqual(rows[0].associated_at, collected_at.replace(tzinfo=None))
        index_names = {
            item['name']
            for item in sa.inspect(engine).get_indexes(migration.TABLE_NAME)
        }
        self.assertIn(migration.INDEX_NAME, index_names)

        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, 'op', operations):
                migration.downgrade()
        self.assertNotIn(migration.TABLE_NAME, sa.inspect(engine).get_table_names())

        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, 'op', operations):
                migration.upgrade()
        with engine.connect() as connection:
            self.assertEqual(connection.scalar(sa.select(sa.func.count()).select_from(association)), 2)
        engine.dispose()

    def test_sqlite_fresh_orm_schema_matches_migration_key_and_index(self):
        migration = _load_migration()
        engine = create_engine('sqlite+pysqlite:///:memory:')
        Base.metadata.create_all(
            engine,
            tables=[
                Symbol.__table__,
                StoredExternalNewsItem.__table__,
                ExternalNewsItemSymbol.__table__,
                ExternalNewsItemClassification.__table__,
            ],
        )

        inspector = sa.inspect(engine)
        primary_key = inspector.get_pk_constraint(ExternalNewsItemSymbol.__tablename__)
        self.assertEqual(
            primary_key['constrained_columns'],
            ['external_news_item_id', 'symbol_id'],
        )
        self.assertEqual(primary_key['name'], 'pk_external_news_item_symbols')
        index_names = {
            item['name']
            for item in inspector.get_indexes(ExternalNewsItemSymbol.__tablename__)
        }
        self.assertIn(migration.INDEX_NAME, index_names)

        classification_migration = _load_migration(CLASSIFICATION_MIGRATION_PATH)
        unique_constraints = inspector.get_unique_constraints(
            ExternalNewsItemClassification.__tablename__
        )
        self.assertIn(
            classification_migration.UNIQUE_NAME,
            {item['name'] for item in unique_constraints},
        )
        classification_indexes = {
            item['name']
            for item in inspector.get_indexes(
                ExternalNewsItemClassification.__tablename__
            )
        }
        self.assertIn(classification_migration.INDEX_NAME, classification_indexes)
        engine.dispose()

    def test_sqlite_classification_migration_supports_down_up(self):
        migration = _load_migration(CLASSIFICATION_MIGRATION_PATH)
        engine = create_engine('sqlite+pysqlite:///:memory:')
        metadata = sa.MetaData()
        news = sa.Table(
            'external_news_items',
            metadata,
            sa.Column('id', sa.Integer, primary_key=True),
        )
        metadata.create_all(engine)

        with engine.begin() as connection:
            connection.execute(news.insert(), {'id': 10})
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, 'op', operations):
                migration.upgrade()
            classification = sa.Table(
                migration.TABLE_NAME,
                sa.MetaData(),
                autoload_with=connection,
            )
            connection.execute(
                classification.insert(),
                {
                    'external_news_item_id': 10,
                    'classifier_version': 'rule_based_external_news_contract_v1',
                    'classified_at': datetime(2026, 5, 9, tzinfo=timezone.utc),
                    'classification_payload': {'materiality': 'unknown'},
                },
            )

        inspector = sa.inspect(engine)
        self.assertIn(migration.TABLE_NAME, inspector.get_table_names())
        self.assertIn(
            migration.UNIQUE_NAME,
            {
                item['name']
                for item in inspector.get_unique_constraints(migration.TABLE_NAME)
            },
        )
        self.assertIn(
            migration.INDEX_NAME,
            {item['name'] for item in inspector.get_indexes(migration.TABLE_NAME)},
        )

        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, 'op', operations):
                migration.downgrade()
        self.assertNotIn(migration.TABLE_NAME, sa.inspect(engine).get_table_names())

        with engine.begin() as connection:
            operations = Operations(MigrationContext.configure(connection))
            with patch.object(migration, 'op', operations):
                migration.upgrade()
        self.assertIn(migration.TABLE_NAME, sa.inspect(engine).get_table_names())
        engine.dispose()


class ExternalNewsAssociationStoreTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite+pysqlite:///:memory:')
        Base.metadata.create_all(
            self.engine,
            tables=[
                Symbol.__table__,
                StoredExternalNewsItem.__table__,
                ExternalNewsItemSymbol.__table__,
                ExternalNewsItemClassification.__table__,
            ],
        )
        self.session_factory = sessionmaker(bind=self.engine, future=True)
        with self.session_factory.begin() as session:
            session.add_all(
                [
                    Symbol(symbol='005930', name_ko='Samsung', market='KOSPI', source_payload={}),
                    Symbol(symbol='000660', name_ko='SK Hynix', market='KOSPI', source_payload={}),
                ]
            )

    def tearDown(self):
        self.engine.dispose()

    def _session_scope(self):
        return _sqlite_session_scope(self.session_factory)

    @staticmethod
    def _provider_item(*, title: str) -> ExternalNewsItem:
        return ExternalNewsItem(
            provider='fake_news',
            provider_item_id='item-1',
            title=title,
            description='description',
            url='https://example.test/news/1',
            source_url='https://publisher.example.test/news/1',
            published_at=datetime(2026, 5, 8, 5, 0, tzinfo=timezone.utc),
            payload={'provider_payload': 'first'},
        )

    def test_conflict_keeps_article_immutable_and_adds_point_in_time_symbol_association(self):
        first_seen = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)
        second_symbol_seen = first_seen + timedelta(hours=1)
        repeated_second_symbol = second_symbol_seen + timedelta(hours=1)

        with (
            patch('app.external_news.store.session_scope', self._session_scope),
            patch(
                'app.external_news.store._utcnow',
                side_effect=[first_seen, second_symbol_seen, repeated_second_symbol],
            ),
        ):
            first = persist_external_news_items(
                symbol='005930',
                items=[self._provider_item(title='original title')],
            )
            second = persist_external_news_items(
                symbol='000660',
                items=[self._provider_item(title='later title must not replace')],
            )
            repeated = persist_external_news_items(
                symbol='000660',
                items=[self._provider_item(title='another replay')],
            )

            before_second_association = list_external_news_items(
                symbols=['000660'],
                published_since=first_seen - timedelta(days=1),
                published_before=second_symbol_seen + timedelta(days=1),
                collected_before=second_symbol_seen,
            )
            after_second_association = list_external_news_items(
                symbols=['000660'],
                published_since=first_seen - timedelta(days=1),
                published_before=second_symbol_seen + timedelta(days=1),
                collected_before=repeated_second_symbol + timedelta(seconds=1),
            )
            post_cutoff_association = list_external_news_items(
                symbols=['000660'],
                observed_after=first_seen + timedelta(minutes=30),
                published_before=second_symbol_seen + timedelta(days=1),
                collected_before=repeated_second_symbol + timedelta(seconds=1),
            )

        self.assertEqual(first, {'inserted': 1, 'updated': 0, 'skipped': 0})
        self.assertEqual(second, {'inserted': 0, 'updated': 0, 'skipped': 1})
        self.assertEqual(repeated, {'inserted': 0, 'updated': 0, 'skipped': 1})
        self.assertEqual(before_second_association, [])
        self.assertEqual(len(after_second_association), 1)
        self.assertEqual(len(post_cutoff_association), 1)
        self.assertEqual(
            post_cutoff_association[0]['associated_at'],
            second_symbol_seen.replace(tzinfo=None).isoformat(),
        )
        self.assertEqual(after_second_association[0]['symbol'], '000660')
        self.assertEqual(after_second_association[0]['title'], 'original title')

        with self.session_factory() as session:
            article = session.scalars(select(StoredExternalNewsItem)).one()
            associations = session.scalars(
                select(ExternalNewsItemSymbol).order_by(ExternalNewsItemSymbol.symbol_id)
            ).all()
            classifications = session.scalars(
                select(ExternalNewsItemClassification)
            ).all()
        self.assertEqual(article.title, 'original title')
        self.assertEqual(article.collected_at, first_seen.replace(tzinfo=None))
        self.assertEqual(len(associations), 2)
        self.assertEqual(len(classifications), 1)
        second_association = next(
            item for item in associations if item.symbol_id != article.symbol_id
        )
        self.assertEqual(second_association.associated_at, second_symbol_seen.replace(tzinfo=None))

        with patch('app.eod.pipeline_status.session_scope', self._session_scope):
            status_counts = _load_external_news_counts(market_date='2026-05-08')
        self.assertEqual(status_counts['items'], 1)
        self.assertEqual(status_counts['symbols'], 2)
        self.assertEqual(status_counts['contract_classified_items'], 1)

        with (
            patch('app.external_news.store.session_scope', self._session_scope),
            patch(
                'app.external_news.store._utcnow',
                return_value=repeated_second_symbol + timedelta(days=1),
            ),
        ):
            verification = enrich_recent_external_news_classification(
                symbols=['000660'],
                lookback_days=14,
            )
        self.assertEqual(verification['status'], 'completed')
        self.assertEqual(verification['items_updated'], 0)
        self.assertEqual(verification['items_missing_classification'], 0)

    def test_legacy_classification_enrichment_appends_version_without_mutating_observation(self):
        observed_at = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)
        with (
            patch('app.external_news.store.session_scope', self._session_scope),
            patch('app.external_news.store._utcnow', return_value=observed_at),
        ):
            persist_external_news_items(
                symbol='000660',
                items=[self._provider_item(title='immutable legacy observation')],
            )

        legacy_payload = {'provider_payload': 'must remain byte-for-byte equivalent'}
        with self.session_factory.begin() as session:
            article = session.scalars(select(StoredExternalNewsItem)).one()
            article.payload = dict(legacy_payload)
            session.execute(delete(ExternalNewsItemClassification))

        classified_at = observed_at + timedelta(days=1)
        with (
            patch('app.external_news.store.session_scope', self._session_scope),
            patch(
                'app.external_news.store._utcnow',
                return_value=classified_at,
            ),
        ):
            result = enrich_recent_external_news_classification(
                symbols=['000660'],
                lookback_days=14,
            )
            repeated = enrich_recent_external_news_classification(
                symbols=['000660'],
                lookback_days=14,
            )
            before_classification = list_external_news_items(
                symbols=['000660'],
                published_since=observed_at - timedelta(days=1),
                published_before=classified_at,
                collected_before=classified_at,
            )
            after_classification = list_external_news_items(
                symbols=['000660'],
                published_since=observed_at - timedelta(days=1),
                published_before=classified_at + timedelta(seconds=1),
                collected_before=classified_at + timedelta(seconds=1),
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['items_inspected'], 1)
        self.assertEqual(result['items_updated'], 1)
        self.assertEqual(result['items_missing_classification'], 0)
        self.assertEqual(result['mutation_policy'], 'append_only_classification_versions')
        self.assertEqual(repeated['status'], 'completed')
        self.assertEqual(repeated['items_updated'], 0)
        self.assertEqual(before_classification[0]['classification'], {})
        self.assertEqual(
            before_classification[0]['classification_provenance']['source'],
            'unavailable',
        )
        self.assertEqual(
            after_classification[0]['classification']['classifier'],
            'rule_based_external_news_contract_v1',
        )
        self.assertEqual(
            after_classification[0]['classification_provenance']['source'],
            'append_only_sidecar',
        )
        self.assertEqual(
            after_classification[0]['classification_provenance']['classified_at'],
            classified_at.replace(tzinfo=None).isoformat(),
        )
        with self.session_factory() as session:
            stored_payload = session.scalars(select(StoredExternalNewsItem.payload)).one()
            stored_versions = session.scalars(
                select(ExternalNewsItemClassification)
            ).all()
        self.assertEqual(stored_payload, legacy_payload)
        self.assertEqual(len(stored_versions), 1)

    def test_enrichment_includes_old_article_newly_associated_in_market_day_scope(self):
        now = datetime(2026, 10, 8, tzinfo=timezone.utc)
        association_start = datetime(2026, 10, 6, 15, 0, tzinfo=timezone.utc)
        association_end = datetime(2026, 10, 7, 15, 0, tzinfo=timezone.utc)
        old_observation_at = now - timedelta(days=30)
        with self.session_factory.begin() as session:
            samsung = session.scalars(
                select(Symbol).where(Symbol.symbol == '005930')
            ).one()
            hynix = session.scalars(
                select(Symbol).where(Symbol.symbol == '000660')
            ).one()
            naver = Symbol(
                symbol='035420',
                name_ko='Naver',
                market='KOSPI',
                source_payload={},
            )
            session.add(naver)
            session.flush()
            articles = [
                StoredExternalNewsItem(
                    symbol_id=samsung.id,
                    provider='fake_news',
                    provider_item_id=f'old-{label}',
                    source='fake_news',
                    title=f'old article {label}',
                    url=f'https://example.test/news/old-{label}',
                    published_at=old_observation_at,
                    collected_at=old_observation_at,
                    payload={},
                )
                for label in (
                    'new-association',
                    'unrelated',
                    'out-of-window',
                    'cross-symbol-window',
                    'end-boundary',
                )
            ]
            session.add_all(articles)
            session.flush()
            session.add_all(
                [
                    ExternalNewsItemSymbol(
                        external_news_item_id=articles[0].id,
                        symbol_id=hynix.id,
                        associated_at=association_start,
                    ),
                    ExternalNewsItemSymbol(
                        external_news_item_id=articles[0].id,
                        symbol_id=samsung.id,
                        associated_at=association_start + timedelta(hours=2),
                    ),
                    ExternalNewsItemSymbol(
                        external_news_item_id=articles[1].id,
                        symbol_id=naver.id,
                        associated_at=association_start + timedelta(hours=1),
                    ),
                    ExternalNewsItemSymbol(
                        external_news_item_id=articles[2].id,
                        symbol_id=hynix.id,
                        associated_at=association_start - timedelta(microseconds=1),
                    ),
                    ExternalNewsItemSymbol(
                        external_news_item_id=articles[3].id,
                        symbol_id=hynix.id,
                        associated_at=association_start - timedelta(days=1),
                    ),
                    ExternalNewsItemSymbol(
                        external_news_item_id=articles[3].id,
                        symbol_id=naver.id,
                        associated_at=association_start + timedelta(hours=1),
                    ),
                    ExternalNewsItemSymbol(
                        external_news_item_id=articles[4].id,
                        symbol_id=hynix.id,
                        associated_at=association_end,
                    ),
                ]
            )
            eligible_article_id = int(articles[0].id)

        with (
            patch('app.external_news.store.session_scope', self._session_scope),
            patch('app.external_news.store._utcnow', return_value=now),
        ):
            first = enrich_recent_external_news_classification(
                symbols=['000660', '005930'],
                lookback_days=14,
                association_start=association_start,
                association_end=association_end,
            )
            repeated = enrich_recent_external_news_classification(
                symbols=['000660', '005930'],
                lookback_days=14,
                association_start=association_start,
                association_end=association_end,
            )

        with self.session_factory() as session:
            classified_item_ids = session.scalars(
                select(ExternalNewsItemClassification.external_news_item_id)
            ).all()
        self.assertEqual(first['items_inspected'], 1)
        self.assertEqual(first['classification_versions_inserted'], 1)
        self.assertEqual(repeated['items_inspected'], 0)
        self.assertEqual(repeated['classification_versions_inserted'], 0)
        self.assertEqual(classified_item_ids, [eligible_article_id])

    def test_enrichment_prioritizes_market_day_associations_before_recent_articles(self):
        now = datetime(2026, 10, 8, tzinfo=timezone.utc)
        association_start = datetime(2026, 10, 6, 15, 0, tzinfo=timezone.utc)
        association_end = datetime(2026, 10, 7, 15, 0, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            symbol = session.scalars(
                select(Symbol).where(Symbol.symbol == '000660')
            ).one()
            old_association = StoredExternalNewsItem(
                symbol_id=symbol.id,
                provider='fake_news',
                provider_item_id='priority-old-association',
                source='fake_news',
                title='old article associated on market day',
                url='https://example.test/news/priority-old-association',
                published_at=now - timedelta(days=30),
                collected_at=now - timedelta(days=30),
                payload={},
            )
            recent_article = StoredExternalNewsItem(
                symbol_id=symbol.id,
                provider='fake_news',
                provider_item_id='priority-recent-article',
                source='fake_news',
                title='recent article',
                url='https://example.test/news/priority-recent-article',
                published_at=now - timedelta(hours=1),
                collected_at=now - timedelta(hours=1),
                payload={},
            )
            session.add_all([old_association, recent_article])
            session.flush()
            session.add_all(
                [
                    ExternalNewsItemSymbol(
                        external_news_item_id=old_association.id,
                        symbol_id=symbol.id,
                        associated_at=association_start + timedelta(hours=1),
                    ),
                    ExternalNewsItemSymbol(
                        external_news_item_id=recent_article.id,
                        symbol_id=symbol.id,
                        associated_at=association_start - timedelta(days=1),
                    ),
                ]
            )
            old_association_id = int(old_association.id)
            recent_article_id = int(recent_article.id)

        with (
            patch('app.external_news.store.session_scope', self._session_scope),
            patch('app.external_news.store._utcnow', return_value=now),
        ):
            first = enrich_recent_external_news_classification(
                symbols=['000660'],
                lookback_days=14,
                limit=1,
                association_start=association_start,
                association_end=association_end,
            )
            with self.session_factory() as session:
                first_item_ids = session.scalars(
                    select(ExternalNewsItemClassification.external_news_item_id)
                ).all()
            second = enrich_recent_external_news_classification(
                symbols=['000660'],
                lookback_days=14,
                limit=1,
                association_start=association_start,
                association_end=association_end,
            )
            third = enrich_recent_external_news_classification(
                symbols=['000660'],
                lookback_days=14,
                limit=1,
                association_start=association_start,
                association_end=association_end,
            )

        self.assertEqual(first['status'], 'partial')
        self.assertEqual(first_item_ids, [old_association_id])
        self.assertEqual(second['classification_versions_inserted'], 1)
        self.assertEqual(third['items_inspected'], 0)
        with self.session_factory() as session:
            final_item_ids = set(
                session.scalars(
                    select(ExternalNewsItemClassification.external_news_item_id)
                ).all()
            )
        self.assertEqual(final_item_ids, {old_association_id, recent_article_id})

    def test_embedded_legacy_classification_is_not_visible_until_timestamped_sidecar_exists(self):
        observed_at = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)
        classified_at = observed_at + timedelta(days=1)
        with (
            patch('app.external_news.store.session_scope', self._session_scope),
            patch('app.external_news.store._utcnow', return_value=observed_at),
        ):
            persist_external_news_items(
                symbol='005930',
                items=[self._provider_item(title='삼성전자 대규모 공급 계약')],
            )

        with self.session_factory.begin() as session:
            article = session.scalars(select(StoredExternalNewsItem)).one()
            embedded_payload = dict(article.payload)
            self.assertIn('classification', embedded_payload)
            session.execute(delete(ExternalNewsItemClassification))

        with patch('app.external_news.store.session_scope', self._session_scope):
            before_enrichment = list_external_news_items(
                symbols=['005930'],
                published_since=observed_at - timedelta(days=1),
                published_before=classified_at,
                collected_before=classified_at,
            )
        with patch('app.eod.pipeline_status.session_scope', self._session_scope):
            before_counts = _load_external_news_counts(market_date='2026-05-08')
        self.assertEqual(before_enrichment[0]['classification'], {})
        self.assertNotIn('classification', before_enrichment[0]['payload'])
        self.assertEqual(before_counts['contract_classified_items'], 0)
        self.assertEqual(before_counts['legacy_classified_items'], 1)

        with (
            patch('app.external_news.store.session_scope', self._session_scope),
            patch('app.external_news.store._utcnow', return_value=classified_at),
        ):
            result = enrich_recent_external_news_classification(
                symbols=['005930'],
                lookback_days=14,
            )

        with patch('app.external_news.store.session_scope', self._session_scope):
            at_exclusive_cutoff = list_external_news_items(
                symbols=['005930'],
                published_since=observed_at - timedelta(days=1),
                published_before=classified_at,
                collected_before=classified_at,
            )
            after_cutoff = list_external_news_items(
                symbols=['005930'],
                published_since=observed_at - timedelta(days=1),
                published_before=classified_at + timedelta(microseconds=1),
                collected_before=classified_at + timedelta(microseconds=1),
            )
            post_classification = list_external_news_items(
                symbols=['005930'],
                observed_after=classified_at,
                published_before=classified_at + timedelta(microseconds=1),
                collected_before=classified_at + timedelta(microseconds=1),
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['classification_versions_inserted'], 1)
        self.assertEqual(at_exclusive_cutoff[0]['classification'], {})
        self.assertEqual(
            after_cutoff[0]['classification'],
            embedded_payload['classification'],
        )
        self.assertEqual(len(post_classification), 1)
        self.assertEqual(
            after_cutoff[0]['classification_provenance'],
            {
                'source': 'append_only_sidecar',
                'classifier_version': 'rule_based_external_news_contract_v1',
                'classified_at': classified_at.replace(tzinfo=None).isoformat(),
            },
        )
        with patch('app.eod.pipeline_status.session_scope', self._session_scope):
            after_counts = _load_external_news_counts(market_date='2026-05-08')
        self.assertEqual(after_counts['contract_classified_items'], 1)
        self.assertEqual(after_counts['legacy_classified_items'], 0)
        with self.session_factory() as session:
            stored_payload = session.scalars(select(StoredExternalNewsItem.payload)).one()
        self.assertEqual(stored_payload, embedded_payload)

    def test_enrichment_batch_limit_advances_instead_of_rescanning_completed_rows(self):
        observed_at = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            owner = session.scalars(select(Symbol).where(Symbol.symbol == '005930')).one()
            articles = [
                StoredExternalNewsItem(
                    symbol_id=owner.id,
                    provider='fake_news',
                    provider_item_id=f'legacy-{index}',
                    source='fake_news',
                    title=f'legacy article {index}',
                    url=f'https://example.test/news/legacy-{index}',
                    published_at=observed_at + timedelta(minutes=index),
                    collected_at=observed_at + timedelta(minutes=index),
                    payload={},
                )
                for index in range(2)
            ]
            session.add_all(articles)
            session.flush()
            session.add_all(
                [
                    ExternalNewsItemSymbol(
                        external_news_item_id=article.id,
                        symbol_id=owner.id,
                        associated_at=article.collected_at,
                    )
                    for article in articles
                ]
            )

        with (
            patch('app.external_news.store.session_scope', self._session_scope),
            patch(
                'app.external_news.store._utcnow',
                return_value=observed_at + timedelta(days=1),
            ),
        ):
            first = enrich_recent_external_news_classification(limit=1)
            second = enrich_recent_external_news_classification(limit=1)
            third = enrich_recent_external_news_classification(limit=1)

        self.assertEqual(first['status'], 'partial')
        self.assertEqual(first['reason'], 'external_news_classification_batch_limit_reached')
        self.assertEqual(first['classification_versions_inserted'], 1)
        self.assertEqual(second['status'], 'completed')
        self.assertEqual(second['classification_versions_inserted'], 1)
        self.assertEqual(third['status'], 'completed')
        self.assertEqual(third['items_inspected'], 0)
        with self.session_factory() as session:
            version_count = len(
                session.scalars(select(ExternalNewsItemClassification)).all()
            )
        self.assertEqual(version_count, 2)

    def test_point_in_time_read_selects_latest_classification_strictly_before_cutoff(self):
        first_classified_at = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)
        second_classified_at = first_classified_at + timedelta(days=1)
        with (
            patch('app.external_news.store.session_scope', self._session_scope),
            patch('app.external_news.store._utcnow', return_value=first_classified_at),
        ):
            persist_external_news_items(
                symbol='005930',
                items=[self._provider_item(title='original v1 classification')],
            )

        with self.session_factory.begin() as session:
            article = session.scalars(select(StoredExternalNewsItem)).one()
            first_version = session.scalars(
                select(ExternalNewsItemClassification)
            ).one()
            second_payload = dict(first_version.classification_payload)
            second_payload['classifier'] = 'rule_based_external_news_contract_v2'
            second_payload['sentiment'] = 'negative'
            session.add(
                ExternalNewsItemClassification(
                    external_news_item_id=article.id,
                    classifier_version='rule_based_external_news_contract_v2',
                    classified_at=second_classified_at,
                    classification_payload=second_payload,
                )
            )

        def _read(cutoff: datetime) -> dict[str, object]:
            with patch('app.external_news.store.session_scope', self._session_scope):
                rows = list_external_news_items(
                    symbols=['005930'],
                    published_since=first_classified_at - timedelta(days=1),
                    published_before=cutoff,
                    collected_before=cutoff,
                )
            return rows[0]

        between_versions = _read(second_classified_at - timedelta(seconds=1))
        at_second_version = _read(second_classified_at)
        after_second_version = _read(second_classified_at + timedelta(microseconds=1))

        self.assertEqual(
            between_versions['classification']['classifier'],
            'rule_based_external_news_contract_v1',
        )
        self.assertEqual(
            at_second_version['classification']['classifier'],
            'rule_based_external_news_contract_v1',
        )
        self.assertEqual(
            after_second_version['classification']['classifier'],
            'rule_based_external_news_contract_v2',
        )
        self.assertEqual(after_second_version['sentiment'], 'negative')
        self.assertEqual(
            after_second_version['classification_provenance']['classified_at'],
            second_classified_at.replace(tzinfo=None).isoformat(),
        )
        with patch('app.eod.pipeline_status.session_scope', self._session_scope):
            counts = _load_external_news_counts(market_date='2026-05-08')
        self.assertEqual(counts['contract_classified_items'], 1)
        self.assertEqual(counts['legacy_classified_items'], 0)

        with (
            patch('app.eod.pipeline_status.session_scope', self._session_scope),
            patch(
                'app.eod.pipeline_status.CLASSIFIER_VERSION',
                'rule_based_external_news_contract_v3',
            ),
        ):
            version_bump_counts = _load_external_news_counts(market_date='2026-05-08')
        self.assertEqual(version_bump_counts['classifier_version'], 'rule_based_external_news_contract_v3')
        self.assertEqual(version_bump_counts['contract_classified_items'], 0)
        self.assertEqual(version_bump_counts['outdated_classified_items'], 1)
        self.assertEqual(version_bump_counts['legacy_classified_items'], 1)

    def test_conflicting_provider_id_and_url_identity_fails_closed(self):
        first_seen = datetime(2026, 5, 8, 6, 0, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            owner = session.scalars(select(Symbol).where(Symbol.symbol == '005930')).one()
            session.add_all(
                [
                    StoredExternalNewsItem(
                        symbol_id=owner.id,
                        provider='fake_news',
                        provider_item_id='item-1',
                        source='fake_news',
                        title='first',
                        url='https://example.test/news/1',
                        published_at=first_seen,
                        collected_at=first_seen,
                        payload={},
                    ),
                    StoredExternalNewsItem(
                        symbol_id=owner.id,
                        provider='fake_news',
                        provider_item_id='item-2',
                        source='fake_news',
                        title='second',
                        url='https://example.test/news/2',
                        published_at=first_seen,
                        collected_at=first_seen,
                        payload={},
                    ),
                ]
            )

        ambiguous_item = ExternalNewsItem(
            provider='fake_news',
            provider_item_id='item-1',
            title='ambiguous',
            description=None,
            url='https://example.test/news/2',
            source_url=None,
            published_at=first_seen,
            payload={},
        )
        with self.session_factory() as session:
            with self.assertRaisesRegex(RuntimeError, 'different immutable observations'):
                _find_existing_news_item(session, ambiguous_item)


if __name__ == '__main__':
    unittest.main()
