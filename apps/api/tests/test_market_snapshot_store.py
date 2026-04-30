import unittest
from datetime import datetime, timezone

from sqlalchemy import delete

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


if __name__ == '__main__':
    unittest.main()
