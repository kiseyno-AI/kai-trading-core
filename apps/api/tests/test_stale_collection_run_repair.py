import unittest
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timezone
from unittest.mock import patch

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from app.db_models import (
    DailyActionSummary,
    DailyMarketRiskFilterResult,
    DailyMarketRiskSnapshot,
    DailyPipelineRun,
    DailyPriceBar,
    DailyPriceContext,
    DailyTechnicalSnapshot,
    DailyTrendingRank,
    EODCollectionRun,
    Symbol,
)
from app.eod.stale_collection_run_repair import (
    CHILD_REFERENCE_FIELDS,
    FROZEN_REPAIR_MANIFEST,
    FROZEN_REPAIR_MANIFEST_SHA256,
    PARENT_ROW_STATE_FIELDS,
    REPAIR_MARKER_KEY,
    TARGET_ROW_STATE_FIELDS,
    StaleEODCollectionRunRepair,
    _expected_state,
    _sha256,
)


class StaleEODCollectionRunRepairTests(unittest.TestCase):
    def test_production_manifest_freezes_reviewed_exact_scope(self) -> None:
        self.assertEqual(
            [row['id'] for row in FROZEN_REPAIR_MANIFEST['rows']],
            [139, 360, 361, 362],
        )
        self.assertEqual(
            FROZEN_REPAIR_MANIFEST['expected_global_running_ids'],
            [139, 360, 361, 362],
        )
        self.assertEqual(
            {row['status'] for row in FROZEN_REPAIR_MANIFEST['rows']},
            {'running'},
        )
        self.assertEqual(FROZEN_REPAIR_MANIFEST['target_status'], 'interrupted')
        self.assertEqual(
            [
                parent['id']
                for parent in FROZEN_REPAIR_MANIFEST['contextual_parent_gates']
            ],
            [87, 88, 90],
        )
        self.assertEqual(
            FROZEN_REPAIR_MANIFEST_SHA256,
            '61610cc75a8b84190328b8d55c739e61d904396e0a4cdaca5aa992f729cc84a9',
        )

    def setUp(self) -> None:
        self.engine = create_engine('sqlite+pysqlite:///:memory:')
        for table in (
            EODCollectionRun.__table__,
            DailyPipelineRun.__table__,
            Symbol.__table__,
            DailyPriceBar.__table__,
            DailyPriceContext.__table__,
            DailyTechnicalSnapshot.__table__,
            DailyActionSummary.__table__,
            DailyTrendingRank.__table__,
            DailyMarketRiskSnapshot.__table__,
            DailyMarketRiskFilterResult.__table__,
        ):
            table.create(self.engine)
        self.session_factory = sessionmaker(bind=self.engine, future=True)
        self.manifest = self._manifest()
        self.repair = StaleEODCollectionRunRepair(self.manifest)
        self.lock_calls: list[dict] = []
        self._seed_rows()

    def tearDown(self) -> None:
        self.engine.dispose()

    @staticmethod
    def _child_counts(*, direct_price_rows: int) -> dict[str, int]:
        return {
            label: (
                direct_price_rows
                if label == 'daily_price_bars.eod_run_id'
                else 0
            )
            for label, _column in CHILD_REFERENCE_FIELDS
        }

    def _manifest(self) -> dict:
        manifest = {
            'schema_version': 'test_manifest_v1',
            'incident_id': 'test_stale_eod_collection_runs',
            'target_status': 'interrupted',
            'expected_global_running_ids': [41, 42],
            'rows': [
                {
                    'id': 41,
                    'market': 'KRX',
                    'trade_date': '2026-07-09',
                    'source': 'pykrx',
                    'trigger': 'internal_ops_cli',
                    'status': 'running',
                    'symbols_total': 4,
                    'symbols_succeeded': 2,
                    'symbols_failed': 1,
                    'error_message': None,
                    'started_at': '2026-07-09T10:00:00+00:00',
                    'completed_at': None,
                    'run_metadata': {
                        'mode': 'daily_stage_run',
                        'stage': 'price_collection',
                        'last_progress_at': '2026-07-09T10:05:00+00:00',
                        'current_symbol': '005930',
                        'current_step': 'fetch_symbol_ohlcv',
                        'current_activity': (
                            'Collecting pykrx daily prices for 005930'
                        ),
                        'stage_progress': {
                            'price_collection': {
                                'status': 'running',
                                'current_step': 'fetch_symbol_ohlcv',
                                'current_activity': (
                                    'Collecting pykrx daily prices for 005930'
                                ),
                            }
                        },
                    },
                    'child_reference_counts': self._child_counts(
                        direct_price_rows=1
                    ),
                    'expected_row_sha256': '',
                },
                {
                    'id': 42,
                    'market': 'KRX',
                    'trade_date': '2026-07-10',
                    'source': 'pykrx',
                    'trigger': 'internal_ops_cli',
                    'status': 'running',
                    'symbols_total': 5,
                    'symbols_succeeded': 3,
                    'symbols_failed': 0,
                    'error_message': None,
                    'started_at': '2026-07-10T10:00:00+00:00',
                    'completed_at': None,
                    'run_metadata': {
                        'mode': 'daily_stage_run',
                        'stage': 'price_collection',
                        'last_progress_at': '2026-07-10T10:05:00+00:00',
                        'current_symbol': '000660',
                        'current_step': 'fetch_symbol_ohlcv',
                        'current_activity': (
                            'Collecting pykrx daily prices for 000660'
                        ),
                        'stage_progress': {
                            'price_collection': {
                                'status': 'running',
                                'current_step': 'fetch_symbol_ohlcv',
                                'current_activity': (
                                    'Collecting pykrx daily prices for 000660'
                                ),
                            }
                        },
                    },
                    'child_reference_counts': self._child_counts(
                        direct_price_rows=0
                    ),
                    'expected_row_sha256': '',
                },
            ],
            'contextual_parent_gates': [
                {
                    'id': 31,
                    'market_date': '2026-07-09',
                    'trigger': 'internal_ops_cli',
                    'status': 'interrupted',
                    'current_stage': None,
                    'started_at': '2026-07-09T09:59:00+00:00',
                    'completed_at': '2026-07-09T12:00:00+00:00',
                    'error_message': 'Core process restarted.',
                    'run_metadata': {},
                    'created_at': '2026-07-09T09:58:59+00:00',
                    'updated_at': '2026-07-09T12:00:00+00:00',
                    'expected_row_sha256': '',
                    'corroborates_eod_run_id': 41,
                    'authoritative_link': False,
                }
            ],
            'mutation_contract': {
                'preserve_counters': True,
                'preserve_child_rows': True,
            },
        }
        for row in manifest['rows']:
            row['expected_row_sha256'] = _sha256(
                _expected_state(row, TARGET_ROW_STATE_FIELDS)
            )
        for row in manifest['contextual_parent_gates']:
            row['expected_row_sha256'] = _sha256(
                _expected_state(row, PARENT_ROW_STATE_FIELDS)
            )
        return manifest

    def _seed_rows(self) -> None:
        with self.session_factory.begin() as session:
            session.add(
                DailyPipelineRun(
                    id=31,
                    market_date=date(2026, 7, 9),
                    trigger='internal_ops_cli',
                    status='interrupted',
                    started_at=datetime(2026, 7, 9, 9, 59, tzinfo=timezone.utc),
                    completed_at=datetime(2026, 7, 9, 12, tzinfo=timezone.utc),
                    error_message='Core process restarted.',
                    run_metadata={},
                    created_at=datetime(
                        2026, 7, 9, 9, 58, 59, tzinfo=timezone.utc
                    ),
                    updated_at=datetime(
                        2026, 7, 9, 12, tzinfo=timezone.utc
                    ),
                )
            )
            for expected in self.manifest['rows']:
                metadata = deepcopy(expected['run_metadata'])
                session.add(
                    EODCollectionRun(
                        id=expected['id'],
                        market=expected['market'],
                        trade_date=date.fromisoformat(expected['trade_date']),
                        source=expected['source'],
                        trigger=expected['trigger'],
                        status=expected['status'],
                        symbols_total=expected['symbols_total'],
                        symbols_succeeded=expected['symbols_succeeded'],
                        symbols_failed=expected['symbols_failed'],
                        error_message=expected['error_message'],
                        started_at=datetime.fromisoformat(
                            expected['started_at']
                        ),
                        completed_at=None,
                        run_metadata=metadata,
                    )
                )
            symbol = Symbol(
                symbol='005930',
                name_ko='Samsung Electronics',
                market='KOSPI',
                source_payload={},
            )
            session.add(symbol)
            session.flush()
            session.add(
                DailyPriceBar(
                    symbol_id=symbol.id,
                    eod_run_id=41,
                    trade_date=date(2026, 7, 9),
                    open_price=100,
                    high_price=110,
                    low_price=90,
                    close_price=105,
                    volume=1_000,
                    turnover=105_000,
                    source='pykrx',
                )
            )

    @contextmanager
    def _session_scope(self, **_kwargs):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def _advisory_locks(self, **kwargs):
        self.lock_calls.append(kwargs)
        yield True, None

    def _run(self, *, apply: bool) -> dict:
        with (
            patch(
                'app.eod.stale_collection_run_repair.session_scope',
                self._session_scope,
            ),
            patch(
                'app.eod.stale_collection_run_repair.'
                'advisory_eod_collection_locks',
                self._advisory_locks,
            ),
        ):
            return self.repair.repair(
                apply=apply,
                expected_manifest_sha256=(
                    self.repair.manifest_sha256 if apply else None
                ),
            )

    def test_exact_scope_dry_run_apply_and_idempotent_replay(self) -> None:
        dry_run = self._run(apply=False)

        self.assertEqual(dry_run['status'], 'ready')
        self.assertTrue(dry_run['dry_run'])
        self.assertEqual(
            self.lock_calls[-1],
            {
                'market': 'KRX',
                'trade_dates': ('2026-07-09', '2026-07-10'),
                'source': 'daily_price_bars',
            },
        )
        with self.session_factory() as session:
            self.assertEqual(
                session.scalar(
                    select(func.count())
                    .select_from(EODCollectionRun)
                    .where(EODCollectionRun.status == 'running')
                ),
                2,
            )

        applied = self._run(apply=True)

        self.assertEqual(applied['status'], 'completed')
        self.assertEqual(applied['repaired_run_ids'], [41, 42])
        self.assertTrue(applied['postconditions']['counters_preserved'])
        self.assertTrue(applied['postconditions']['child_references_preserved'])
        with self.session_factory() as session:
            rows = list(
                session.scalars(
                    select(EODCollectionRun).order_by(EODCollectionRun.id)
                )
            )
            bar = session.scalar(select(DailyPriceBar))
            self.assertEqual([row.status for row in rows], ['interrupted'] * 2)
            self.assertEqual(
                [(row.symbols_total, row.symbols_succeeded, row.symbols_failed)
                 for row in rows],
                [(4, 2, 1), (5, 3, 0)],
            )
            self.assertEqual(bar.eod_run_id, 41)
            self.assertEqual(
                rows[0].run_metadata['stage_progress']['price_collection'][
                    'status'
                ],
                'interrupted',
            )
            self.assertEqual(
                rows[0].run_metadata['stage_progress']['price_collection'][
                    'current_step'
                ],
                'stale_run_interrupted',
            )
            self.assertIn(REPAIR_MARKER_KEY, rows[0].run_metadata)
            completed_at = rows[0].completed_at

        replay = self._run(apply=True)

        self.assertEqual(replay['status'], 'already_applied')
        with self.session_factory() as session:
            self.assertEqual(session.get(EODCollectionRun, 41).completed_at, completed_at)

    def test_manifest_mismatch_blocks_every_write(self) -> None:
        with self.session_factory.begin() as session:
            session.get(EODCollectionRun, 42).symbols_succeeded = 4

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['reason'],
            'stale_eod_collection_run_manifest_mismatch',
        )
        with self.session_factory() as session:
            rows = list(
                session.scalars(
                    select(EODCollectionRun).order_by(EODCollectionRun.id)
                )
            )
            self.assertEqual([row.status for row in rows], ['running', 'running'])
            self.assertFalse(
                any(
                    REPAIR_MARKER_KEY in (row.run_metadata or {})
                    for row in rows
                )
            )
            self.assertIsNone(rows[0].completed_at)
            self.assertIsNone(rows[1].completed_at)

    def test_extra_target_metadata_blocks_every_write(self) -> None:
        with self.session_factory.begin() as session:
            row = session.get(EODCollectionRun, 42)
            metadata = dict(row.run_metadata)
            metadata['unexpected_lease_id'] = 'new-worker-lease'
            row.run_metadata = metadata

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertFalse(result['invariants']['row_fields_match'])
        self.assertFalse(result['invariants']['row_hashes_match'])
        self.assertIn(
            'run_metadata.unexpected_lease_id',
            result['row_mismatches'][42],
        )
        with self.session_factory() as session:
            rows = list(
                session.scalars(
                    select(EODCollectionRun).order_by(EODCollectionRun.id)
                )
            )
            self.assertEqual([row.status for row in rows], ['running', 'running'])
            self.assertFalse(
                any(
                    REPAIR_MARKER_KEY in (row.run_metadata or {})
                    for row in rows
                )
            )

    def test_parent_metadata_drift_blocks_every_write(self) -> None:
        with self.session_factory.begin() as session:
            parent = session.get(DailyPipelineRun, 31)
            parent.run_metadata = {'lease_id': 'replacement-lease'}

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertFalse(result['invariants']['parent_fields_match'])
        self.assertFalse(result['invariants']['parent_row_hashes_match'])
        self.assertIn('run_metadata.lease_id', result['parent_mismatches'][31])
        with self.session_factory() as session:
            self.assertEqual(
                list(
                    session.scalars(
                        select(EODCollectionRun.status).order_by(
                            EODCollectionRun.id
                        )
                    )
                ),
                ['running', 'running'],
            )

    def test_parent_current_stage_drift_blocks_every_write(self) -> None:
        with self.session_factory.begin() as session:
            session.get(DailyPipelineRun, 31).current_stage = 'price_collection'

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertFalse(result['invariants']['parent_fields_match'])
        self.assertFalse(result['invariants']['parent_row_hashes_match'])
        self.assertIn('current_stage', result['parent_mismatches'][31])
        with self.session_factory() as session:
            self.assertEqual(
                list(
                    session.scalars(
                        select(EODCollectionRun.status).order_by(
                            EODCollectionRun.id
                        )
                    )
                ),
                ['running', 'running'],
            )

    def test_partial_marker_state_blocks_replay_without_changes(self) -> None:
        self.assertEqual(self._run(apply=True)['status'], 'completed')
        with self.session_factory.begin() as session:
            row = session.get(EODCollectionRun, 42)
            metadata = dict(row.run_metadata)
            metadata.pop(REPAIR_MARKER_KEY)
            row.run_metadata = metadata

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'partial_repair_marker_state')
        with self.session_factory() as session:
            rows = list(
                session.scalars(
                    select(EODCollectionRun).order_by(EODCollectionRun.id)
                )
            )
            self.assertEqual([row.status for row in rows], ['interrupted'] * 2)
            self.assertIn(REPAIR_MARKER_KEY, rows[0].run_metadata)
            self.assertNotIn(REPAIR_MARKER_KEY, rows[1].run_metadata)

    def test_replay_blocks_when_repaired_metadata_has_drifted(self) -> None:
        self.assertEqual(self._run(apply=True)['status'], 'completed')
        with self.session_factory.begin() as session:
            row = session.get(EODCollectionRun, 41)
            metadata = dict(row.run_metadata)
            metadata['unexpected_link_id'] = 999
            row.run_metadata = metadata

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'invalid_repair_marker_state')
        with self.session_factory() as session:
            rows = list(
                session.scalars(
                    select(EODCollectionRun).order_by(EODCollectionRun.id)
                )
            )
            self.assertEqual([row.status for row in rows], ['interrupted'] * 2)
            self.assertEqual(rows[0].run_metadata['unexpected_link_id'], 999)

    def test_apply_requires_reviewed_manifest_hash_before_opening_database(self) -> None:
        with (
            patch(
                'app.eod.stale_collection_run_repair.session_scope'
            ) as session_scope,
            patch(
                'app.eod.stale_collection_run_repair.'
                'advisory_eod_collection_locks'
            ) as advisory_locks,
        ):
            result = self.repair.repair(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'expected_manifest_sha256_required')
        session_scope.assert_not_called()
        advisory_locks.assert_not_called()

    def test_active_price_bar_advisory_lock_blocks_before_row_transaction(self) -> None:
        @contextmanager
        def blocked_locks(**_kwargs):
            yield False, '2026-07-10'

        with (
            patch(
                'app.eod.stale_collection_run_repair.session_scope'
            ) as session_scope,
            patch(
                'app.eod.stale_collection_run_repair.'
                'advisory_eod_collection_locks',
                blocked_locks,
            ),
        ):
            result = self.repair.repair(
                apply=True,
                expected_manifest_sha256=self.repair.manifest_sha256,
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'active_daily_price_bar_advisory_lock')
        self.assertEqual(result['blocked_trade_date'], '2026-07-10')
        session_scope.assert_not_called()


if __name__ == '__main__':
    unittest.main()
