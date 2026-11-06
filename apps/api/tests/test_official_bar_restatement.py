from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import engine
from app.db_models import (
    DailyPriceBar,
    DailyPriceBarRepairAudit,
    DailyPriceBarRepairBatch,
    Symbol,
)
from app.eod.kis_final_bar_repair import (
    ProviderFinalBar,
    StoredFinalBar,
    repair_existing_final_bars,
)
from app.eod.official_bar_restatement import (
    OfficialReference,
    make_planner,
    plan_official_restatement,
)

DAY = date(2099, 9, 14)
ONE_ROW = {'KOSPI': 1, 'KOSDAQ': 1}


def stored(symbol='000660', values=(100, 110, 90, 105, 1000, 100_000), source='pykrx') -> StoredFinalBar:
    o, h, l, c, v, t = values
    return StoredFinalBar(
        bar_id=1,
        symbol=symbol,
        trade_date=DAY,
        open_price=o,
        high_price=h,
        low_price=l,
        close_price=c,
        volume=v,
        turnover=t,
        source=source,
    )


def provider(symbol='000660', values=(100, 110, 90, 106, 1200, 120_000)) -> ProviderFinalBar:
    o, h, l, c, v, t = values
    return ProviderFinalBar(
        symbol=symbol,
        trade_date=DAY,
        open_price=o,
        high_price=h,
        low_price=l,
        close_price=c,
        volume=v,
        turnover=t,
    )


def plan(stored_rows, provider_rows, official):
    return plan_official_restatement(
        stored_rows=stored_rows,
        provider_bars={(p.symbol, p.trade_date): p for p in provider_rows},
        official=OfficialReference({(sym, DAY): vals for sym, vals in official.items()}, covered_dates={DAY}),
    )


class PlannerTests(unittest.TestCase):
    def test_updates_when_stored_differs_and_kis_agrees_with_the_official_table(self) -> None:
        result = plan([stored()], [provider()], {'000660': (100, 110, 90, 106, 1200, 120_000)})

        self.assertEqual(result['safe_changed_count'], 1)
        self.assertEqual(result['unsafe_row_count'], 0)
        (old, new) = result['updates'][0]
        self.assertEqual((old.close_price, new.close_price), (105, 106))

    def test_corrects_in_both_directions_unlike_the_monotonic_extension_rule(self) -> None:
        # Close and volume go down, high goes down, low goes up: not a partial-day extension.
        official = (100, 108, 92, 104, 900, 90_000)
        result = plan([stored()], [provider(values=official)], {'000660': official})

        self.assertEqual(result['safe_changed_count'], 1)

    def test_unchanged_when_stored_already_equals_the_official_table_even_if_kis_differs(self) -> None:
        # KIS may carry adjusted history for a corporate action; the stored raw bar is right.
        official = (100, 110, 90, 105, 1000, 100_000)
        result = plan([stored()], [provider(values=(50, 55, 45, 52, 2000, 100_000))], {'000660': official})

        self.assertEqual(result['safe_changed_count'], 0)
        self.assertEqual(result['unsafe_row_count'], 0)
        self.assertEqual(result['unchanged_count'], 1)

    def test_blocks_when_kis_and_the_official_table_disagree(self) -> None:
        result = plan(
            [stored()],
            [provider(values=(100, 110, 90, 107, 1200, 120_000))],
            {'000660': (100, 110, 90, 106, 1200, 120_000)},
        )

        self.assertEqual(result['safe_changed_count'], 0)
        self.assertEqual(result['unsafe_rows'][0]['reason'], 'provider_official_disagree')

    def test_blocks_when_the_official_table_has_no_row_for_a_covered_date(self) -> None:
        result = plan([stored('005935')], [provider('005935')], {'000660': (1, 1, 1, 1, 1, 1)})

        self.assertEqual(result['safe_changed_count'], 0)
        self.assertEqual(result['unsafe_rows'][0]['reason'], 'official_reference_missing')

    def test_blocks_a_date_the_official_reference_does_not_cover(self) -> None:
        result = plan_official_restatement(
            stored_rows=[stored()],
            provider_bars={('000660', DAY): provider()},
            official=OfficialReference({}, covered_dates=set()),
        )

        self.assertEqual(result['unsafe_rows'][0]['reason'], 'official_reference_unavailable')

    def test_blocks_an_implausible_close_change(self) -> None:
        # 105 -> 200 is beyond any KRX daily limit; treat as a basis problem.
        official = (100, 210, 90, 200, 1000, 100_000)
        result = plan([stored()], [provider(values=official)], {'000660': official})

        self.assertEqual(result['safe_changed_count'], 0)
        self.assertEqual(result['unsafe_rows'][0]['reason'], 'implausible_close_change')

    def test_missing_kis_row_is_reported_as_missing_not_unsafe(self) -> None:
        result = plan([stored()], [], {'000660': (100, 110, 90, 106, 1200, 120_000)})

        self.assertEqual(result['missing_provider_row_count'], 1)
        self.assertEqual(result['safe_changed_count'], 0)

    def test_halted_symbol_representation_matches(self) -> None:
        halted = (0, 0, 0, 4200, 0, 0)
        result = plan([stored(values=halted)], [provider(values=halted)], {'000660': halted})

        self.assertEqual(result['unchanged_count'], 1)

    def test_make_planner_binds_the_reference(self) -> None:
        official = (100, 110, 90, 106, 1200, 120_000)
        planner = make_planner(OfficialReference({('000660', DAY): official}, covered_dates={DAY}))

        result = planner(stored_rows=[stored()], provider_bars={('000660', DAY): provider()})

        self.assertEqual(result['safe_changed_count'], 1)


class OfficialReferenceTests(unittest.TestCase):
    def test_loads_from_the_cache_without_fetching(self) -> None:
        import csv
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            for market, code in (('KOSPI', '000660'), ('KOSDAQ', '203650')):
                with open(Path(directory) / f'20990914_{market}.csv', 'w', newline='') as handle:
                    writer = csv.writer(handle)
                    writer.writerow(['code', '시가', '고가', '저가', '종가', '거래량', '거래대금'])
                    writer.writerow([code, 100, 110, 90, 105, 1000, 100000])

            def never(*_args):
                raise AssertionError('must not fetch when the cache covers the date')

            reference = OfficialReference.load([DAY], cache_dir=directory, fetcher=never, minimum_rows=ONE_ROW)

        self.assertIn(DAY, reference.covered_dates)
        self.assertEqual(reference.get('000660', DAY), (100, 110, 90, 105, 1000, 100000))
        self.assertEqual(reference.get('203650', DAY), (100, 110, 90, 105, 1000, 100000))

    def test_fetches_and_caches_when_missing_and_skips_a_date_whose_fetch_fails(self) -> None:
        import tempfile
        from pathlib import Path

        import pandas as pd

        calls: list[tuple[str, str]] = []

        def fetcher(compact_date, market):
            calls.append((compact_date, market))
            if market == 'KOSDAQ':
                raise ConnectionError('krx unavailable')
            return pd.DataFrame(
                {'시가': [100], '고가': [110], '저가': [90], '종가': [105], '거래량': [10], '거래대금': [1050]},
                index=['000660'],
            )

        with tempfile.TemporaryDirectory() as directory:
            reference = OfficialReference.load([DAY], cache_dir=directory, fetcher=fetcher, minimum_rows=ONE_ROW)
            cached = sorted(path.name for path in Path(directory).iterdir())

        # One market failing means the date is not covered: never a half-reference.
        self.assertNotIn(DAY, reference.covered_dates)
        self.assertEqual(cached, [])
        self.assertEqual(len(calls), 2)


class OfficialReferenceFloorTests(unittest.TestCase):
    def test_a_truncated_market_frame_leaves_the_date_uncovered(self) -> None:
        import pandas as pd

        def fetcher(_compact_date, _market):
            return pd.DataFrame(
                {'시가': [1], '고가': [1], '저가': [1], '종가': [1], '거래량': [1], '거래대금': [1]},
                index=['000660'],
            )

        # Default floors (700 KOSPI / 1,200 KOSDAQ) apply: one row per market is not a market.
        reference = OfficialReference.load([DAY], fetcher=fetcher)

        self.assertNotIn(DAY, reference.covered_dates)


class AppliedRestatementTests(unittest.TestCase):
    def test_apply_restates_the_bar_audits_it_and_opens_a_pending_batch(self) -> None:
        symbol_code = uuid4().hex[:6].upper()
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbol = Symbol(
                symbol=symbol_code,
                name_ko='official restatement integration',
                market='KOSPI',
                instrument_type='equity',
                source='protected_symbol_source',
                is_active=True,
            )
            session.add(symbol)
            session.flush()
            bar = DailyPriceBar(
                symbol_id=symbol.id,
                trade_date=DAY,
                open_price=100,
                high_price=110,
                low_price=90,
                close_price=105,
                volume=1000,
                turnover=100_000,
                per=Decimal('10.5'),
                source='pykrx',
            )
            session.add(bar)
            session.flush()
            session.refresh(bar)
            initial = StoredFinalBar(
                bar_id=bar.id,
                symbol=symbol_code,
                trade_date=DAY,
                open_price=100,
                high_price=110,
                low_price=90,
                close_price=105,
                volume=1000,
                turnover=100_000,
                source=bar.source,
                captured_at=bar.captured_at,
                eod_run_id=bar.eod_run_id,
            )
            official = (100, 108, 92, 104, 900, 90_000)  # lower close, high and volume
            planner = make_planner(OfficialReference({(symbol_code, DAY): official}, covered_dates={DAY}))

            @contextmanager
            def rollback_only_session_scope(**_kwargs):
                yield session
                session.flush()

            try:
                with patch('app.eod.kis_final_bar_repair.session_scope', rollback_only_session_scope):
                    result = repair_existing_final_bars(
                        start_date=DAY,
                        end_date=DAY,
                        selected_symbols=[symbol_code],
                        initial_rows=[initial],
                        provider_bars={(symbol_code, DAY): provider(symbol_code, official)},
                        apply=True,
                        planner=planner,
                    )

                self.assertEqual(result['updated_count'], 1)
                session.expire_all()
                persisted = session.execute(select(DailyPriceBar).where(DailyPriceBar.id == bar.id)).scalar_one()
                self.assertEqual(
                    (persisted.open_price, persisted.high_price, persisted.low_price, persisted.close_price, persisted.volume, persisted.turnover),
                    official,
                )
                self.assertEqual(persisted.per, Decimal('10.5'))  # untouched lineage
                audit = session.execute(
                    select(DailyPriceBarRepairAudit).where(DailyPriceBarRepairAudit.daily_price_bar_id == bar.id)
                ).scalar_one()
                self.assertEqual((audit.old_close_price, audit.new_close_price), (105, 104))
                self.assertEqual((audit.old_volume, audit.new_volume), (1000, 900))
                batch = session.execute(
                    select(DailyPriceBarRepairBatch).where(
                        DailyPriceBarRepairBatch.repair_batch_id == result['repair_batch_id']
                    )
                ).scalar_one()
                self.assertEqual(batch.status, 'pending')
            finally:
                session.close()
                transaction.rollback()

    def test_default_planner_still_refuses_the_same_bidirectional_change(self) -> None:
        # Guard against the hook changing default behaviour: without the restatement
        # planner this correction is "unsafe" and nothing is written.
        symbol_code = uuid4().hex[:6].upper()
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbol = Symbol(
                symbol=symbol_code,
                name_ko='default planner guard',
                market='KOSPI',
                instrument_type='equity',
                source='protected_symbol_source',
                is_active=True,
            )
            session.add(symbol)
            session.flush()
            bar = DailyPriceBar(
                symbol_id=symbol.id,
                trade_date=DAY,
                open_price=100,
                high_price=110,
                low_price=90,
                close_price=105,
                volume=1000,
                turnover=100_000,
                source='pykrx',
            )
            session.add(bar)
            session.flush()
            session.refresh(bar)
            initial = StoredFinalBar(
                bar_id=bar.id,
                symbol=symbol_code,
                trade_date=DAY,
                open_price=100,
                high_price=110,
                low_price=90,
                close_price=105,
                volume=1000,
                turnover=100_000,
                source=bar.source,
                captured_at=bar.captured_at,
                eod_run_id=bar.eod_run_id,
            )

            @contextmanager
            def rollback_only_session_scope(**_kwargs):
                yield session
                session.flush()

            try:
                with patch('app.eod.kis_final_bar_repair.session_scope', rollback_only_session_scope):
                    result = repair_existing_final_bars(
                        start_date=DAY,
                        end_date=DAY,
                        selected_symbols=[symbol_code],
                        initial_rows=[initial],
                        provider_bars={(symbol_code, DAY): provider(symbol_code, (100, 108, 92, 104, 900, 90_000))},
                        apply=True,
                    )
                self.assertEqual(result['updated_count'], 0)
                self.assertEqual(result['unsafe_row_count'], 1)
            finally:
                session.close()
                transaction.rollback()


if __name__ == '__main__':
    unittest.main()
