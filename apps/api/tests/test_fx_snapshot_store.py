import unittest
from datetime import datetime, timezone

from sqlalchemy import delete, func, select

from app.db import engine, session_scope
from app.db_models import CurrentFXRateSnapshot
from app.fx.snapshot_store import FXRateSnapshotStore, KOREAEXIM_PROVIDER, USD_KRW_PAIR


class FXRateSnapshotStoreIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        CurrentFXRateSnapshot.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.store = FXRateSnapshotStore()
        with session_scope() as session:
            session.execute(delete(CurrentFXRateSnapshot))

    def tearDown(self) -> None:
        with session_scope() as session:
            session.execute(delete(CurrentFXRateSnapshot))

    def test_upsert_and_load_latest_usable_snapshot(self) -> None:
        first_captured_at = datetime(2026, 4, 30, 1, 15, tzinfo=timezone.utc)
        second_captured_at = datetime(2026, 4, 30, 2, 5, tzinfo=timezone.utc)

        self.store.upsert_snapshot(
            provider=KOREAEXIM_PROVIDER,
            currency_pair=USD_KRW_PAIR,
            base_currency='USD',
            quote_currency='KRW',
            exchange_rate=1444.5,
            requested_date='20260430',
            effective_date='20260430',
            fallback_applied=False,
            snapshot_status='ready',
            stale_reason=None,
            detail=None,
            payload={'provider': 'koreaexim', 'requested_date': '20260430', 'effective_date': '20260430'},
            captured_at=first_captured_at,
        )
        self.store.upsert_snapshot(
            provider=KOREAEXIM_PROVIDER,
            currency_pair=USD_KRW_PAIR,
            base_currency='USD',
            quote_currency='KRW',
            exchange_rate=1443.2,
            requested_date='20260430',
            effective_date='20260429',
            fallback_applied=True,
            snapshot_status='stale',
            stale_reason='koreaexim_date_fallback_applied',
            detail='Korea Exim date fallback applied',
            payload={'provider': 'koreaexim', 'requested_date': '20260430', 'effective_date': '20260429'},
            captured_at=second_captured_at,
        )
        loaded = self.store.load_latest_usable(
            provider=KOREAEXIM_PROVIDER,
            currency_pair=USD_KRW_PAIR,
        )

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(loaded.currency_pair, 'USD/KRW')
        self.assertEqual(loaded.exchange_rate, 1443.2)
        self.assertEqual(loaded.requested_date.isoformat(), '2026-04-30')
        self.assertEqual(loaded.effective_date.isoformat(), '2026-04-29')
        self.assertTrue(loaded.fallback_applied)
        self.assertEqual(loaded.snapshot_status, 'stale')
        self.assertEqual(loaded.stale_reason, 'koreaexim_date_fallback_applied')
        self.assertEqual(loaded.detail, 'Korea Exim date fallback applied')
        self.assertEqual(loaded.captured_at, second_captured_at)

        with session_scope() as session:
            count = session.execute(select(func.count(CurrentFXRateSnapshot.id))).scalar_one()
        self.assertEqual(count, 1)


if __name__ == '__main__':
    unittest.main()
