from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

from app.eod.kis_valuation_backfill import (
    ValuationTarget,
    _persist_valuation,
    backfill_kis_quote_valuation_for_market_date,
)


class KISValuationPersistenceTests(TestCase):
    def test_persist_updates_only_valuation_metrics(self) -> None:
        statements = []

        class _Session:
            def execute(self, statement):
                statements.append(statement)
                if len(statements) == 1:
                    return SimpleNamespace(
                        scalar_one_or_none=lambda: 73,
                    )
                return SimpleNamespace(rowcount=1)

        @contextmanager
        def fake_session_scope():
            yield _Session()

        with patch(
            'app.eod.kis_valuation_backfill.session_scope',
            new=fake_session_scope,
        ):
            updated = _persist_valuation(
                market_date=date(2026, 9, 18),
                symbol='005930',
                valuation={
                    'per': Decimal('10.1'),
                    'pbr': Decimal('1.2'),
                    'bps': Decimal('50000'),
                    'eps': Decimal('5000'),
                    'dividend_yield': Decimal('2.5'),
                    'dps': Decimal('1750'),
                },
            )

        self.assertEqual(updated, 1)
        update_fields = {
            str(key).rsplit('.', 1)[-1] for key in statements[1]._values
        }
        self.assertEqual(
            update_fields,
            {'per', 'pbr', 'bps', 'eps', 'dividend_yield', 'dps'},
        )
        self.assertNotIn('source', update_fields)
        self.assertNotIn('captured_at', update_fields)
        self.assertNotIn('repair_origin_source', update_fields)
        self.assertNotIn('repair_origin_eod_run_id', update_fields)


class KISValuationBackfillTests(IsolatedAsyncioTestCase):
    async def test_backfill_reloads_targets_then_writes_under_common_lock(
        self,
    ) -> None:
        initial_target = ValuationTarget(
            symbol='005930',
            market='KOSPI',
            close_price=100,
            prior_dps=Decimal('1'),
            prior_dividend_yield=None,
            prior_close_price=100,
        )
        locked_target = ValuationTarget(
            symbol='005930',
            market='KOSPI',
            close_price=200,
            prior_dps=Decimal('4'),
            prior_dividend_yield=None,
            prior_close_price=200,
        )
        lock_calls: list[dict[str, object]] = []

        @contextmanager
        def valuation_lock(**kwargs):
            lock_calls.append(kwargs)
            yield True

        quote_client = SimpleNamespace(
            fetch_domestic_quote=AsyncMock(
                return_value={'output': {'per': '10.1', 'pbr': '1.2'}}
            )
        )
        with (
            patch(
                'app.eod.kis_valuation_backfill.validate_finalized_krx_write_date'
            ) as validate_finality,
            patch(
                'app.eod.kis_valuation_backfill._load_targets',
                side_effect=[[initial_target], [locked_target]],
            ) as load_targets,
            patch(
                'app.eod.kis_valuation_backfill.KISClient',
                return_value=quote_client,
            ),
            patch(
                'app.eod.kis_valuation_backfill.advisory_eod_collection_lock',
                new=valuation_lock,
            ),
            patch(
                'app.eod.kis_valuation_backfill._persist_valuation',
                return_value=1,
            ) as persist_valuation,
        ):
            result = await backfill_kis_quote_valuation_for_market_date(
                market_date='2026-09-18',
                sleep_seconds=0,
            )

        validate_finality.assert_called_once_with(
            date(2026, 9, 18),
            operation='KIS valuation price-bar backfill',
        )
        self.assertEqual(load_targets.call_count, 2)
        self.assertEqual(
            load_targets.call_args_list[1].kwargs['symbols'],
            ('005930',),
        )
        self.assertEqual(
            lock_calls,
            [
                {
                    'market': 'KRX',
                    'trade_date': '2026-09-18',
                    'source': 'daily_price_bars',
                }
            ],
        )
        persisted_valuation = persist_valuation.call_args.kwargs['valuation']
        self.assertEqual(persisted_valuation['dps'], Decimal('4'))
        self.assertEqual(
            persisted_valuation['dividend_yield'],
            Decimal('2.000000'),
        )
        self.assertEqual(result['updated'], 1)
        self.assertEqual(result['failed'], 0)

    async def test_backfill_fails_closed_when_common_lock_is_busy(self) -> None:
        target = ValuationTarget(
            symbol='005930',
            market='KOSPI',
            close_price=100,
            prior_dps=None,
            prior_dividend_yield=None,
            prior_close_price=None,
        )

        @contextmanager
        def busy_lock(**_kwargs):
            yield False

        quote_client = SimpleNamespace(
            fetch_domestic_quote=AsyncMock(
                return_value={'output': {'per': '10.1', 'pbr': '1.2'}}
            )
        )
        with (
            patch(
                'app.eod.kis_valuation_backfill._load_targets',
                return_value=[target],
            ),
            patch(
                'app.eod.kis_valuation_backfill.KISClient',
                return_value=quote_client,
            ),
            patch(
                'app.eod.kis_valuation_backfill.advisory_eod_collection_lock',
                new=busy_lock,
            ),
            patch(
                'app.eod.kis_valuation_backfill._persist_valuation'
            ) as persist_valuation,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'daily price-bar writer lock is already held',
            ):
                await backfill_kis_quote_valuation_for_market_date(
                    market_date='2026-09-18',
                    sleep_seconds=0,
                )

        persist_valuation.assert_not_called()

    async def test_backfill_rejects_unfinalized_date_before_provider_work(
        self,
    ) -> None:
        with (
            patch(
                'app.eod.kis_valuation_backfill.validate_finalized_krx_write_date',
                side_effect=ValueError('not finalized'),
            ),
            patch('app.eod.kis_valuation_backfill._load_targets') as load_targets,
            patch('app.eod.kis_valuation_backfill.KISClient') as client,
        ):
            with self.assertRaisesRegex(ValueError, 'not finalized'):
                await backfill_kis_quote_valuation_for_market_date(
                    market_date='2026-09-18',
                    sleep_seconds=0,
                )

        load_targets.assert_not_called()
        client.assert_not_called()
