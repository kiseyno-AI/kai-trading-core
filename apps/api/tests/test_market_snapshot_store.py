import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier

from sqlalchemy import delete, func, select

from app.db import engine, session_scope
from app.db_models import CurrentMarketSnapshotCache
from app.market.snapshot_store import MarketSnapshotCacheStore


class MarketSnapshotCacheStoreIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        CurrentMarketSnapshotCache.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.store = MarketSnapshotCacheStore()
        with session_scope() as session:
            session.execute(delete(CurrentMarketSnapshotCache))

    def tearDown(self) -> None:
        with session_scope() as session:
            session.execute(delete(CurrentMarketSnapshotCache))

    def test_save_and_load_round_trip_snapshot_and_refresh_metadata(self) -> None:
        attempted_at = datetime(2026, 4, 30, 1, 15, tzinfo=timezone.utc)
        succeeded_at = datetime(2026, 4, 30, 1, 16, tzinfo=timezone.utc)

        saved = self.store.save(
            payload={
                'as_of': '2026-04-30T10:16:00+09:00',
                'status': 'stale',
                'missing_fields': [],
                'stale_reason': 'koreaexim_quota_exhausted',
                'markets': {'kospi': {'value': 2642.07}, 'kosdaq': {'value': 857.6}},
                'usd_krw': {
                    'pair': 'USD/KRW',
                    'rate': 1444.5,
                    'effective_date': '2026-04-30',
                    'fallback_applied': False,
                    'status': 'stale',
                    'source': 'cache',
                    'stale_reason': 'koreaexim_quota_exhausted',
                    'detail': 'Korea Exim returned result=4',
                    'captured_at': '2026-04-30T10:02:11+09:00',
                },
            },
            last_refresh_attempted_at=attempted_at,
            last_refresh_succeeded_at=succeeded_at,
            last_refresh_status='degraded',
            last_refresh_error='Korea Exim returned result=4',
            refresh_trigger='test',
        )
        loaded = self.store.load()

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(saved.snapshot_status, 'stale')
        self.assertEqual(loaded.snapshot_status, 'stale')
        self.assertEqual(loaded.stale_reason, 'koreaexim_quota_exhausted')
        self.assertEqual(loaded.payload['usd_krw']['source'], 'cache')
        self.assertEqual(loaded.last_refresh_status, 'degraded')
        self.assertEqual(loaded.last_refresh_error, 'Korea Exim returned result=4')
        self.assertEqual(loaded.last_refresh_attempted_at, attempted_at)
        self.assertEqual(loaded.last_refresh_succeeded_at, succeeded_at)
        self.assertEqual(loaded.refresh_trigger, 'test')

    def test_older_failed_refresh_cannot_overwrite_newer_success(self) -> None:
        newer_attempted_at = datetime(2026, 4, 30, 1, 10, tzinfo=timezone.utc)
        older_attempted_at = datetime(2026, 4, 30, 1, 5, tzinfo=timezone.utc)
        newer_payload = {
            'as_of': '2026-04-30T10:10:00+09:00',
            'collected_at': '2026-04-30T10:10:03+09:00',
            'status': 'ready',
            'markets': {'kospi': {'value': 2700.0}},
        }
        self.store.save(
            payload=newer_payload,
            last_refresh_attempted_at=newer_attempted_at,
            last_refresh_succeeded_at=newer_attempted_at,
            last_refresh_status='success',
            last_refresh_error=None,
            refresh_trigger='newer_success',
        )

        saved = self.store.save(
            payload={
                **newer_payload,
                'as_of': '2026-04-30T10:05:00+09:00',
                'collected_at': '2026-04-30T10:05:02+09:00',
                'status': 'stale',
                'stale_reason': 'market_snapshot_kis_upstream_error',
            },
            last_refresh_attempted_at=older_attempted_at,
            last_refresh_succeeded_at=None,
            last_refresh_status='error',
            last_refresh_error='slow older request failed',
            refresh_trigger='older_failure',
        )

        self.assertEqual(saved.payload, newer_payload)
        self.assertEqual(saved.last_refresh_attempted_at, newer_attempted_at)
        self.assertEqual(saved.last_refresh_status, 'success')
        self.assertEqual(saved.refresh_trigger, 'newer_success')

    def test_newer_attempt_with_regressed_provider_observation_keeps_last_good_payload(self) -> None:
        initial_attempted_at = datetime(2026, 4, 30, 1, 10, tzinfo=timezone.utc)
        regressed_attempted_at = datetime(2026, 4, 30, 1, 15, tzinfo=timezone.utc)
        last_good_payload = {
            'as_of': '2026-04-30T10:10:00+09:00',
            'collected_at': '2026-04-30T10:10:03+09:00',
            'status': 'ready',
            'markets': {'kospi': {'value': 2700.0}},
        }
        self.store.save(
            payload=last_good_payload,
            last_refresh_attempted_at=initial_attempted_at,
            last_refresh_succeeded_at=initial_attempted_at,
            last_refresh_status='success',
            last_refresh_error=None,
            refresh_trigger='last_good',
        )

        saved = self.store.save(
            payload={
                **last_good_payload,
                'as_of': '2026-04-30T10:09:00+09:00',
                'collected_at': '2026-04-30T10:15:03+09:00',
                'markets': {'kospi': {'value': 2690.0}},
            },
            last_refresh_attempted_at=regressed_attempted_at,
            last_refresh_succeeded_at=regressed_attempted_at,
            last_refresh_status='success',
            last_refresh_error=None,
            refresh_trigger='regressed_provider_observation',
        )

        self.assertEqual(saved.payload, last_good_payload)
        self.assertEqual(saved.last_refresh_attempted_at, regressed_attempted_at)
        self.assertEqual(saved.last_refresh_succeeded_at, initial_attempted_at)
        self.assertEqual(saved.last_refresh_status, 'error')
        self.assertEqual(saved.last_refresh_error, 'market_snapshot_observation_regressed')

    def test_newer_top_level_observation_cannot_hide_one_market_leg_regression(self) -> None:
        initial_attempted_at = datetime(2026, 4, 30, 1, 10, tzinfo=timezone.utc)
        candidate_attempted_at = datetime(2026, 4, 30, 1, 15, tzinfo=timezone.utc)
        last_good_payload = {
            'as_of': '2026-04-30T10:10:00+09:00',
            'collected_at': '2026-04-30T10:10:03+09:00',
            'status': 'ready',
            'markets': {
                'kospi': {
                    'value': 2700.0,
                    'provider_observed_at': '2026-04-30T10:10:00+09:00',
                },
                'kosdaq': {
                    'value': 870.0,
                    'provider_observed_at': '2026-04-30T10:10:00+09:00',
                },
            },
        }
        self.store.save(
            payload=last_good_payload,
            last_refresh_attempted_at=initial_attempted_at,
            last_refresh_succeeded_at=initial_attempted_at,
            last_refresh_status='success',
            last_refresh_error=None,
            refresh_trigger='last_good_per_market',
        )

        saved = self.store.save(
            payload={
                'as_of': '2026-04-30T10:15:00+09:00',
                'collected_at': '2026-04-30T10:15:03+09:00',
                'status': 'ready',
                'markets': {
                    'kospi': {
                        'value': 2710.0,
                        'provider_observed_at': '2026-04-30T10:15:00+09:00',
                    },
                    'kosdaq': {
                        'value': 865.0,
                        'provider_observed_at': '2026-04-30T10:05:00+09:00',
                    },
                },
            },
            last_refresh_attempted_at=candidate_attempted_at,
            last_refresh_succeeded_at=candidate_attempted_at,
            last_refresh_status='success',
            last_refresh_error=None,
            refresh_trigger='mixed_market_observations',
        )

        self.assertEqual(saved.payload, last_good_payload)
        self.assertEqual(saved.last_refresh_attempted_at, candidate_attempted_at)
        self.assertEqual(saved.last_refresh_succeeded_at, initial_attempted_at)
        self.assertEqual(saved.last_refresh_status, 'error')
        self.assertEqual(saved.last_refresh_error, 'market_snapshot_observation_regressed')

    def test_newer_failed_attempt_marks_equal_last_good_observation_stale(self) -> None:
        last_succeeded_at = datetime(2026, 4, 30, 1, 10, tzinfo=timezone.utc)
        observed_payload = {
            'as_of': '2026-04-30T10:10:00+09:00',
            'collected_at': '2026-04-30T10:10:03+09:00',
            'status': 'ready',
            'markets': {'kospi': {'value': 2700.0}},
        }
        self.store.save(
            payload=observed_payload,
            last_refresh_attempted_at=datetime(2026, 4, 30, 1, 10, tzinfo=timezone.utc),
            last_refresh_succeeded_at=last_succeeded_at,
            last_refresh_status='success',
            last_refresh_error=None,
            refresh_trigger='ready_observation',
        )

        saved = self.store.save(
            payload={
                **observed_payload,
                'status': 'stale',
                'stale_reason': 'market_snapshot_kis_upstream_error',
                'markets': {'kospi': {'value': 2600.0}},
            },
            last_refresh_attempted_at=datetime(2026, 4, 30, 1, 15, tzinfo=timezone.utc),
            last_refresh_succeeded_at=None,
            last_refresh_status='error',
            last_refresh_error='late failure for the same observation',
            refresh_trigger='same_observation_failure',
        )

        self.assertEqual(
            saved.payload,
            {
                **observed_payload,
                'status': 'stale',
                'stale_reason': 'market_snapshot_kis_upstream_error',
            },
        )
        self.assertEqual(saved.snapshot_status, 'stale')
        self.assertEqual(saved.stale_reason, 'market_snapshot_kis_upstream_error')
        self.assertEqual(saved.last_refresh_succeeded_at, last_succeeded_at)
        self.assertEqual(saved.last_refresh_status, 'error')
        self.assertEqual(saved.last_refresh_error, 'late failure for the same observation')
        self.assertEqual(saved.refresh_trigger, 'same_observation_failure')

    def test_failed_attempt_with_regressed_market_observation_is_rejected(self) -> None:
        last_good_payload = {
            'as_of': '2026-04-30T10:10:00+09:00',
            'collected_at': '2026-04-30T10:10:03+09:00',
            'status': 'ready',
            'markets': {
                'kospi': {
                    'value': 2700.0,
                    'provider_observed_at': '2026-04-30T10:10:00+09:00',
                }
            },
        }
        last_succeeded_at = datetime(2026, 4, 30, 1, 10, tzinfo=timezone.utc)
        self.store.save(
            payload=last_good_payload,
            last_refresh_attempted_at=last_succeeded_at,
            last_refresh_succeeded_at=last_succeeded_at,
            last_refresh_status='success',
            last_refresh_error=None,
            refresh_trigger='last_good',
        )

        saved = self.store.save(
            payload={
                **last_good_payload,
                'status': 'stale',
                'stale_reason': 'market_snapshot_kis_upstream_error',
                'markets': {
                    'kospi': {
                        'value': 2690.0,
                        'provider_observed_at': '2026-04-30T10:05:00+09:00',
                    }
                },
            },
            last_refresh_attempted_at=datetime(2026, 4, 30, 1, 15, tzinfo=timezone.utc),
            last_refresh_succeeded_at=last_succeeded_at,
            last_refresh_status='error',
            last_refresh_error='provider returned an older observation',
            refresh_trigger='regressed_failed_attempt',
        )

        self.assertEqual(saved.payload, last_good_payload)
        self.assertEqual(saved.snapshot_status, 'ready')
        self.assertEqual(saved.last_refresh_succeeded_at, last_succeeded_at)
        self.assertEqual(saved.last_refresh_status, 'error')
        self.assertEqual(saved.last_refresh_error, 'market_snapshot_observation_regressed')

    def test_newer_observation_replaces_older_snapshot_even_when_optional_quality_degrades(self) -> None:
        self.store.save(
            payload={
                'as_of': '2026-04-30T10:05:00+09:00',
                'collected_at': '2026-04-30T10:05:03+09:00',
                'status': 'ready',
                'markets': {
                    'kospi': {
                        'value': 2690.0,
                        'provider_observed_at': '2026-04-30T10:05:00+09:00',
                    }
                },
            },
            last_refresh_attempted_at=datetime(2026, 4, 30, 1, 5, tzinfo=timezone.utc),
            last_refresh_succeeded_at=datetime(2026, 4, 30, 1, 5, tzinfo=timezone.utc),
            last_refresh_status='success',
            last_refresh_error=None,
            refresh_trigger='older_ready',
        )
        newer_payload = {
            'as_of': '2026-04-30T10:10:00+09:00',
            'collected_at': '2026-04-30T10:10:03+09:00',
            'status': 'partial',
            'stale_reason': 'koreaexim_quota_exhausted',
            'markets': {
                'kospi': {
                    'value': 2700.0,
                    'provider_observed_at': '2026-04-30T10:10:00+09:00',
                }
            },
        }

        saved = self.store.save(
            payload=newer_payload,
            last_refresh_attempted_at=datetime(2026, 4, 30, 1, 10, tzinfo=timezone.utc),
            last_refresh_succeeded_at=datetime(2026, 4, 30, 1, 10, tzinfo=timezone.utc),
            last_refresh_status='degraded',
            last_refresh_error='optional FX unavailable',
            refresh_trigger='newer_partial',
        )

        self.assertEqual(saved.payload, newer_payload)
        self.assertEqual(saved.last_refresh_status, 'degraded')

    def test_valid_prior_session_observation_replaces_legacy_null_as_of_cache(self) -> None:
        invalid_attempted_at = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
        invalid_payload = {
            'as_of': None,
            'collected_at': '2026-05-01T09:00:00+09:00',
            'status': 'partial',
            'markets': {
                'kospi': {'value': 2700.0, 'provider_observed_at': None},
                'kosdaq': {'value': 870.0, 'provider_observed_at': None},
            },
        }
        with session_scope() as session:
            session.add(
                CurrentMarketSnapshotCache(
                    cache_key='dashboard',
                    payload=invalid_payload,
                    snapshot_status='partial',
                    as_of=None,
                    last_refresh_attempted_at=invalid_attempted_at,
                    last_refresh_succeeded_at=invalid_attempted_at,
                    last_refresh_status='degraded',
                    last_refresh_error='provider timestamp unavailable',
                    refresh_trigger='legacy_preopen_refresh',
                )
            )

        prior_session_payload = {
            'as_of': '2026-04-30T15:30:00+09:00',
            'collected_at': '2026-05-01T09:05:00+09:00',
            'status': 'partial',
            'markets': {
                'kospi': {
                    'value': 2710.0,
                    'provider_observed_at': '2026-04-30T15:30:00+09:00',
                },
                'kosdaq': {
                    'value': 875.0,
                    'provider_observed_at': '2026-04-30T15:30:00+09:00',
                },
            },
        }
        saved = self.store.save(
            payload=prior_session_payload,
            last_refresh_attempted_at=datetime(2026, 5, 1, 0, 5, tzinfo=timezone.utc),
            last_refresh_succeeded_at=datetime(2026, 5, 1, 0, 5, tzinfo=timezone.utc),
            last_refresh_status='degraded',
            last_refresh_error='prior-session observation is not current-session proof',
            refresh_trigger='preopen_refresh',
        )

        self.assertEqual(saved.payload, prior_session_payload)
        self.assertEqual(saved.as_of, datetime(2026, 4, 30, 6, 30, tzinfo=timezone.utc))
        self.assertEqual(saved.refresh_trigger, 'preopen_refresh')

    def test_null_as_of_candidate_is_not_persisted_as_a_snapshot(self) -> None:
        attempted_at = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)

        saved = self.store.save(
            payload={
                'as_of': None,
                'collected_at': '2026-05-01T09:00:00+09:00',
                'status': 'partial',
                'markets': {
                    'kospi': {'value': 2700.0, 'provider_observed_at': None},
                    'kosdaq': {'value': 870.0, 'provider_observed_at': None},
                },
            },
            last_refresh_attempted_at=attempted_at,
            last_refresh_succeeded_at=attempted_at,
            last_refresh_status='degraded',
            last_refresh_error=None,
            refresh_trigger='missing_provider_timestamp',
        )

        self.assertIsNone(saved.payload)
        self.assertIsNone(saved.as_of)
        self.assertEqual(saved.last_refresh_status, 'error')
        self.assertEqual(
            saved.last_refresh_error,
            'market_snapshot_provider_observation_missing',
        )

    def test_refresh_bucket_claim_is_cross_store_and_first_insert_race_safe(self) -> None:
        bucket_started_at = datetime(2026, 4, 30, 0, 5, tzinfo=timezone.utc)
        attempted_at = datetime(2026, 4, 30, 0, 5, 2, tzinfo=timezone.utc)
        barrier = Barrier(2)

        def claim(index: int) -> bool:
            barrier.wait(timeout=5)
            return MarketSnapshotCacheStore().try_claim_refresh_bucket(
                bucket_started_at=bucket_started_at,
                attempted_at=attempted_at,
                refresh_trigger=f'process:{index}',
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(claim, (1, 2)))

        self.assertEqual(sorted(claims), [False, True])
        with session_scope() as session:
            row_count = session.scalar(
                select(func.count()).select_from(CurrentMarketSnapshotCache)
            )
        self.assertEqual(row_count, 1)

        next_bucket_claimed = MarketSnapshotCacheStore().try_claim_refresh_bucket(
            bucket_started_at=datetime(2026, 4, 30, 0, 10, tzinfo=timezone.utc),
            attempted_at=datetime(2026, 4, 30, 0, 10, 1, tzinfo=timezone.utc),
            refresh_trigger='process:next_bucket',
        )
        self.assertTrue(next_bucket_claimed)


if __name__ == '__main__':
    unittest.main()
