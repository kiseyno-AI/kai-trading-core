from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import event, select
from sqlalchemy.orm import Session

from app.db import engine
from app.db_models import (
    DailyPriceBar,
    DailyPriceBarRepairAudit,
    DailyPriceBarRepairBatch,
    EODCollectionRun,
    Symbol,
)
from app.eod.kis_final_bar_repair import (
    KISFinalBarRepairService,
    ProviderFinalBar,
    StoredFinalBar,
    TargetPage,
    extract_kis_final_bars,
    load_target_page,
    plan_final_bar_repairs,
    repair_existing_final_bars,
)
from app.market.clock import KST


MARKET_DATE = date(2099, 12, 29)


def _after_settle() -> datetime:
    return datetime.combine(MARKET_DATE, time(21, 0), tzinfo=KST)


def _stored(
    *,
    bar_id: int = 1,
    symbol: str = '005930',
    open_price: int = 100,
    high_price: int = 105,
    low_price: int = 95,
    close_price: int = 101,
    volume: int = 100,
    turnover: int = 10_000,
) -> StoredFinalBar:
    return StoredFinalBar(
        bar_id=bar_id,
        symbol=symbol,
        trade_date=MARKET_DATE,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=volume,
        turnover=turnover,
    )


def _provider(
    *,
    symbol: str = '005930',
    open_price: int = 100,
    high_price: int = 110,
    low_price: int = 94,
    close_price: int = 108,
    volume: int = 120,
    turnover: int = 12_500,
) -> ProviderFinalBar:
    return ProviderFinalBar(
        symbol=symbol,
        trade_date=MARKET_DATE,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=volume,
        turnover=turnover,
    )


def _payload(*, symbol: str = '005930') -> dict:
    del symbol
    return {
        'output2': [
            {
                'stck_bsop_date': MARKET_DATE.strftime('%Y%m%d'),
                'stck_oprc': '100',
                'stck_hgpr': '110',
                'stck_lwpr': '94',
                'stck_clpr': '108',
                'acml_vol': '120',
                'acml_tr_pbmn': '12500',
            }
        ]
    }


def _available_lock(**_kwargs):
    @contextmanager
    def lock():
        yield True

    return lock()


class KISFinalBarValidationTests(unittest.TestCase):
    def test_extracts_valid_kis_j_daily_row(self) -> None:
        bars = extract_kis_final_bars(
            _payload(),
            symbol='005930',
            start_date=MARKET_DATE,
            end_date=MARKET_DATE,
        )

        self.assertEqual(bars[MARKET_DATE].values, (100, 110, 94, 108, 120, 12_500))

    def test_safe_extension_can_finalize_high_low_close_and_liquidity(self) -> None:
        stored = _stored()
        provider = _provider()

        result = plan_final_bar_repairs(
            stored_rows=[stored],
            provider_bars={(stored.symbol, stored.trade_date): provider},
        )

        self.assertEqual(result['safe_changed_count'], 1)
        self.assertEqual(result['unsafe_row_count'], 0)

    def test_rejects_open_mismatch_and_nonmonotonic_liquidity(self) -> None:
        stored = _stored()
        providers = (
            _provider(open_price=99),
            _provider(volume=99),
            _provider(turnover=9_999),
        )

        reasons = []
        for provider in providers:
            result = plan_final_bar_repairs(
                stored_rows=[stored],
                provider_bars={(stored.symbol, stored.trade_date): provider},
            )
            reasons.append(result['unsafe_rows'][0]['reason'])

        self.assertEqual(
            reasons,
            [
                'provider_open_mismatch',
                'provider_volume_decreased',
                'provider_turnover_decreased',
            ],
        )

    def test_missing_provider_row_is_reported_without_an_update(self) -> None:
        result = plan_final_bar_repairs(
            stored_rows=[_stored()],
            provider_bars={},
        )

        self.assertEqual(result['missing_provider_row_count'], 1)
        self.assertEqual(result['safe_changed_count'], 0)


class KISFinalBarTargetPageTests(unittest.TestCase):
    def test_page_load_uses_bounded_timeout_and_price_bar_id_lookup(self) -> None:
        generated_codes: set[str] = set()
        while len(generated_codes) < 2:
            generated_codes.add(f'Z{uuid4().hex[:5].upper()}')
        symbol_codes = sorted(generated_codes)
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            captured_statements: list[str] = []

            def capture_statement(
                _connection,
                _cursor,
                statement,
                _parameters,
                _context,
                _executemany,
            ) -> None:
                captured_statements.append(str(statement))

            event.listen(connection, 'before_cursor_execute', capture_statement)
            try:
                symbols = [
                    Symbol(
                        symbol=symbol_code,
                        name_ko=f'KIS page {symbol_code}',
                        market='KOSPI',
                        instrument_type='equity',
                        source='test',
                        is_active=True,
                    )
                    for symbol_code in symbol_codes
                ]
                session.add_all(symbols)
                session.flush()
                session.add_all(
                    [
                        DailyPriceBar(
                            symbol_id=symbol.id,
                            trade_date=MARKET_DATE,
                            open_price=100,
                            high_price=110,
                            low_price=90,
                            close_price=105,
                            volume=100,
                            turnover=10_500,
                            source='test',
                        )
                        for symbol in symbols
                    ]
                )
                session.flush()

                @contextmanager
                def rollback_only_session_scope(**kwargs):
                    self.assertEqual(
                        kwargs,
                        {'statement_timeout_ms': 60_000},
                    )
                    yield session

                with patch(
                    'app.eod.kis_final_bar_repair.session_scope',
                    rollback_only_session_scope,
                ):
                    page = load_target_page(
                        start_date=MARKET_DATE,
                        end_date=MARKET_DATE,
                        after_symbol='YZZZZZ',
                        symbol_limit=1,
                    )

                self.assertEqual(page.symbols, (symbol_codes[0],))
                self.assertTrue(page.has_more)
                self.assertEqual(len(page.rows), 1)
                self.assertEqual(page.rows[0].symbol, symbol_codes[0])
                normalized_sql = [
                    ' '.join(statement.split())
                    for statement in captured_statements
                ]
                self.assertTrue(
                    any(
                        statement.startswith(
                            'SELECT daily_price_bars.id AS bar_id, '
                            'daily_price_bars.symbol_id'
                        )
                        and 'JOIN symbols' not in statement
                        and 'daily_price_bars.symbol_id IN' in statement
                        for statement in normalized_sql
                    )
                )
            finally:
                event.remove(
                    connection,
                    'before_cursor_execute',
                    capture_statement,
                )
                session.close()
                transaction.rollback()


class KISFinalBarRepairServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_dry_run_uses_bounded_kis_contract_and_pagination(self) -> None:
        stored = _stored()
        provider_calls: list[dict] = []
        repair_calls: list[dict] = []

        def page_loader(**kwargs):
            self.assertEqual(kwargs['symbol_limit'], 1)
            return TargetPage(
                symbols=('005930',),
                rows=(stored,),
                has_more=True,
            )

        async def provider_loader(symbol, **kwargs):
            provider_calls.append({'symbol': symbol, **kwargs})
            return _payload()

        def repairer(**kwargs):
            repair_calls.append(kwargs)
            return {
                'status': 'ready',
                'applied': False,
                'target_count': 1,
                'safe_changed_count': 1,
                'updated_count': 0,
            }

        service = KISFinalBarRepairService(
            page_loader=page_loader,
            provider_loader=provider_loader,
            repairer=repairer,
            lock_factory=_available_lock,
            concurrency=1,
        )
        result = await service.repair_page(
            start_date=MARKET_DATE,
            end_date=MARKET_DATE,
            symbol_limit=1,
            apply=False,
        )

        self.assertEqual(result['status'], 'ready')
        self.assertTrue(result['has_more'])
        self.assertEqual(result['next_after_symbol'], '005930')
        self.assertEqual(provider_calls[0]['symbol'], '005930')
        self.assertEqual(provider_calls[0]['start_date'], MARKET_DATE)
        self.assertEqual(provider_calls[0]['end_date'], MARKET_DATE)
        self.assertFalse(repair_calls[0]['apply'])

    async def test_final_page_does_not_advertise_another_cursor(self) -> None:
        async def provider_loader(_symbol, **_kwargs):
            return _payload()

        service = KISFinalBarRepairService(
            page_loader=lambda **_kwargs: TargetPage(
                symbols=('005930',),
                rows=(_stored(),),
                has_more=False,
            ),
            provider_loader=provider_loader,
            repairer=lambda **_kwargs: {
                'status': 'ready',
                'applied': False,
                'updated_count': 0,
            },
            pending_batch_loader=lambda: [],
            concurrency=1,
        )

        result = await service.repair_page(
            start_date=MARKET_DATE,
            end_date=MARKET_DATE,
        )

        self.assertIsNone(result['next_after_symbol'])

    async def test_empty_page_still_surfaces_durable_pending_rebuild(self) -> None:
        service = KISFinalBarRepairService(
            page_loader=lambda **_kwargs: TargetPage(
                symbols=(),
                rows=(),
                has_more=False,
            ),
            pending_batch_loader=lambda: [
                {
                    'repair_batch_id': 'repair-pending-1',
                    'required_rebuild_trade_dates': [
                        MARKET_DATE.isoformat(),
                    ],
                }
            ],
        )

        result = await service.repair_page(
            start_date=MARKET_DATE,
            end_date=MARKET_DATE,
            apply=False,
        )

        self.assertEqual(result['status'], 'rebuild_required')
        self.assertTrue(result['derived_rebuild_required'])
        self.assertEqual(
            result['pending_rebuild_batch_ids'],
            ['repair-pending-1'],
        )

    async def test_blocked_row_drift_retries_the_entire_page(self) -> None:
        async def provider_loader(_symbol, **_kwargs):
            return _payload()

        service = KISFinalBarRepairService(
            page_loader=lambda **_kwargs: TargetPage(
                symbols=('005930',),
                rows=(_stored(),),
                has_more=True,
            ),
            provider_loader=provider_loader,
            repairer=lambda **_kwargs: {
                'status': 'blocked',
                'reason': 'stored_rows_changed_during_provider_fetch',
                'applied': False,
                'updated_count': 0,
            },
            lock_factory=_available_lock,
            pending_batch_loader=lambda: [],
            concurrency=1,
        )

        result = await service.repair_page(
            start_date=MARKET_DATE,
            end_date=MARKET_DATE,
        )

        self.assertTrue(result['retry_required'])
        self.assertEqual(result['retry_symbols'], ['005930'])
        self.assertIsNone(result['next_after_symbol'])

    async def test_provider_retries_three_times_then_reports_partial(self) -> None:
        attempts = 0

        async def provider_loader(_symbol, **_kwargs):
            nonlocal attempts
            attempts += 1
            raise RuntimeError('temporary provider failure')

        service = KISFinalBarRepairService(
            page_loader=lambda **_kwargs: TargetPage(
                symbols=('005930',),
                rows=(_stored(),),
                has_more=False,
            ),
            provider_loader=provider_loader,
            repairer=lambda **_kwargs: {
                'status': 'partial',
                'applied': False,
                'updated_count': 0,
            },
            lock_factory=_available_lock,
            concurrency=1,
        )

        with patch('app.eod.kis_final_bar_repair.asyncio.sleep', return_value=None):
            result = await service.repair_page(
                start_date=MARKET_DATE,
                end_date=MARKET_DATE,
            )

        self.assertEqual(attempts, 3)
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['provider_failed_symbol_count'], 1)
        self.assertTrue(result['retry_required'])
        self.assertEqual(result['retry_symbols'], ['005930'])

    async def test_explicit_retry_symbols_bypass_cursor_pagination(self) -> None:
        page_calls: list[dict] = []

        def page_loader(**kwargs):
            page_calls.append(kwargs)
            return TargetPage(
                symbols=('005930',),
                rows=(_stored(),),
                has_more=False,
            )

        async def provider_loader(_symbol, **_kwargs):
            return _payload()

        service = KISFinalBarRepairService(
            page_loader=page_loader,
            provider_loader=provider_loader,
            repairer=lambda **_kwargs: {
                'status': 'ready',
                'applied': False,
                'safe_changed_trade_dates': [MARKET_DATE.isoformat()],
                'updated_count': 0,
            },
            lock_factory=_available_lock,
            concurrency=1,
        )

        result = await service.repair_page(
            start_date=MARKET_DATE,
            end_date=MARKET_DATE,
            symbols=['005930'],
        )

        self.assertEqual(page_calls[0]['selected_symbols'], ('005930',))
        self.assertEqual(result['explicit_symbols'], ['005930'])
        self.assertIsNone(result['next_after_symbol'])

    async def test_apply_refuses_when_advisory_lock_is_unavailable(self) -> None:
        @contextmanager
        def unavailable_lock(**_kwargs):
            yield False

        service = KISFinalBarRepairService(
            page_loader=lambda **_kwargs: TargetPage(
                symbols=('005930',),
                rows=(_stored(),),
                has_more=False,
            ),
            provider_loader=lambda *_args, **_kwargs: None,
            repairer=lambda **_kwargs: self.fail('repairer must not run'),
            lock_factory=unavailable_lock,
            rebuild_date_loader=lambda **_kwargs: (MARKET_DATE,),
            now_factory=_after_settle,
            concurrency=1,
        )

        async def valid_provider(_symbol, **_kwargs):
            return _payload()

        service.provider_loader = valid_provider
        result = await service.repair_page(
            start_date=MARKET_DATE,
            end_date=MARKET_DATE,
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertFalse(result['applied'])

    async def test_apply_uses_one_multi_date_lock_request(
        self,
    ) -> None:
        earlier_date = MARKET_DATE - timedelta(days=1)
        downstream_date = MARKET_DATE + timedelta(days=1)
        lock_calls: list[dict] = []

        @contextmanager
        def date_locks(**kwargs):
            lock_calls.append(kwargs)
            yield False, MARKET_DATE.isoformat()

        async def provider_loader(_symbol, **_kwargs):
            rows = _payload()['output2']
            return {
                'output2': [
                    {
                        **rows[0],
                        'stck_bsop_date': earlier_date.strftime('%Y%m%d'),
                    },
                    rows[0],
                ]
            }

        service = KISFinalBarRepairService(
            page_loader=lambda **_kwargs: TargetPage(
                symbols=('005930',),
                rows=(
                    replace(_stored(), trade_date=earlier_date),
                    _stored(),
                ),
                has_more=False,
            ),
            provider_loader=provider_loader,
            repairer=lambda **_kwargs: self.fail('repairer must not run'),
            lock_set_factory=date_locks,
            rebuild_date_loader=lambda **_kwargs: (
                earlier_date,
                MARKET_DATE,
                downstream_date,
            ),
            now_factory=_after_settle,
            concurrency=1,
        )

        result = await service.repair_page(
            start_date=earlier_date,
            end_date=MARKET_DATE,
            apply=True,
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['blocked_trade_date'], MARKET_DATE.isoformat())
        self.assertEqual(result['blocked_lock_source'], 'daily_price_bars')
        self.assertEqual(
            lock_calls,
            [
                {
                    'market': 'KRX',
                    'trade_dates': [
                        earlier_date,
                        MARKET_DATE,
                        downstream_date,
                    ],
                    'source': 'daily_price_bars',
                }
            ],
        )

    async def test_apply_reports_required_downstream_rebuild(self) -> None:
        downstream_date = MARKET_DATE + timedelta(days=1)
        repair_calls: list[dict] = []

        async def provider_loader(_symbol, **_kwargs):
            return _payload()

        def repairer(**kwargs):
            repair_calls.append(kwargs)
            return {
                'status': 'completed',
                'applied': True,
                'safe_changed_trade_dates': [MARKET_DATE.isoformat()],
                'required_rebuild_trade_dates': [
                    MARKET_DATE.isoformat(),
                    downstream_date.isoformat(),
                ],
                'updated_count': 1,
            }

        service = KISFinalBarRepairService(
            page_loader=lambda **_kwargs: TargetPage(
                symbols=('005930',),
                rows=(_stored(),),
                has_more=False,
            ),
            provider_loader=provider_loader,
            repairer=repairer,
            lock_factory=_available_lock,
            rebuild_date_loader=lambda **_kwargs: (
                MARKET_DATE,
                downstream_date,
            ),
            now_factory=_after_settle,
            concurrency=1,
        )

        result = await service.repair_page(
            start_date=MARKET_DATE,
            end_date=MARKET_DATE,
            apply=True,
        )

        self.assertEqual(result['status'], 'rebuild_required')
        self.assertTrue(result['derived_rebuild_required'])
        self.assertEqual(
            result['derived_rebuild_trade_dates'],
            [MARKET_DATE.isoformat(), downstream_date.isoformat()],
        )
        self.assertEqual(
            repair_calls[0]['required_rebuild_trade_dates'],
            [MARKET_DATE, downstream_date],
        )
        self.assertIn(
            'rebuild_price_dependent_factors_for_affected_and_downstream_dates',
            result['required_follow_up'],
        )
        self.assertIn(
            'rebuild_daily_trending_ranks',
            result['required_follow_up'],
        )

    async def test_apply_rejects_same_day_before_nxt_settle(self) -> None:
        same_day = date(2026, 9, 23)
        service = KISFinalBarRepairService(
            page_loader=lambda **_kwargs: self.fail('page loader must not run'),
            now_factory=lambda: datetime(2026, 9, 23, 20, 9, tzinfo=KST),
        )

        with self.assertRaisesRegex(ValueError, 'blocked until'):
            await service.repair_page(
                start_date=same_day,
                end_date=same_day,
                apply=True,
            )

    async def test_apply_rejects_future_date(self) -> None:
        service = KISFinalBarRepairService(
            page_loader=lambda **_kwargs: self.fail('page loader must not run'),
            now_factory=lambda: datetime(2026, 9, 23, 21, 0, tzinfo=KST),
        )

        with self.assertRaisesRegex(ValueError, 'future date'):
            await service.repair_page(
                start_date=date(2026, 9, 24),
                end_date=date(2026, 9, 24),
                apply=True,
            )


class KISFinalBarRepositoryPostgresTests(unittest.TestCase):
    def test_apply_updates_values_and_records_truthful_repair_lineage(self) -> None:
        symbol_code = uuid4().hex[:6].upper()
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbol = Symbol(
                symbol=symbol_code,
                name_ko='KIS final bar integration',
                market='KOSPI',
                instrument_type='equity',
                source='protected_symbol_source',
                is_active=True,
            )
            session.add(symbol)
            session.flush()
            bar = DailyPriceBar(
                symbol_id=symbol.id,
                trade_date=MARKET_DATE,
                open_price=100,
                high_price=105,
                low_price=95,
                close_price=101,
                volume=100,
                turnover=10_000,
                per=Decimal('10.5'),
                pbr=Decimal('1.2'),
                source='protected_bar_source',
            )
            session.add(bar)
            session.flush()
            session.refresh(bar)
            initial = StoredFinalBar(
                bar_id=bar.id,
                symbol=symbol_code,
                trade_date=MARKET_DATE,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                close_price=bar.close_price,
                volume=bar.volume,
                turnover=bar.turnover,
                source=bar.source,
                captured_at=bar.captured_at,
                eod_run_id=bar.eod_run_id,
            )
            original_captured_at = bar.captured_at
            original_eod_run_id = bar.eod_run_id
            captured_statements: list[str] = []

            def capture_statement(
                _connection,
                _cursor,
                statement,
                _parameters,
                _context,
                _executemany,
            ) -> None:
                captured_statements.append(str(statement))

            @contextmanager
            def rollback_only_session_scope(**kwargs):
                self.assertEqual(
                    kwargs,
                    {'statement_timeout_ms': 60_000},
                )
                yield session
                session.flush()

            try:
                event.listen(
                    connection,
                    'before_cursor_execute',
                    capture_statement,
                )
                try:
                    with patch(
                        'app.eod.kis_final_bar_repair.session_scope',
                        rollback_only_session_scope,
                    ):
                        result = repair_existing_final_bars(
                            start_date=MARKET_DATE,
                            end_date=MARKET_DATE,
                            selected_symbols=[symbol_code],
                            initial_rows=[initial],
                            provider_bars={
                                (symbol_code, MARKET_DATE): ProviderFinalBar(
                                    symbol=symbol_code,
                                    trade_date=MARKET_DATE,
                                    open_price=100,
                                    high_price=110,
                                    low_price=94,
                                    close_price=108,
                                    volume=120,
                                    turnover=12_500,
                                )
                            },
                            apply=True,
                        )
                finally:
                    event.remove(
                        connection,
                        'before_cursor_execute',
                        capture_statement,
                    )

                self.assertEqual(result['status'], 'completed')
                self.assertEqual(result['updated_count'], 1)
                self.assertEqual(result['audit_count'], 1)
                normalized_sql = [
                    ' '.join(statement.split())
                    for statement in captured_statements
                ]
                self.assertTrue(
                    any(
                        statement.startswith(
                            'SELECT daily_price_bars.id AS bar_id, '
                            'daily_price_bars.symbol_id'
                        )
                        and 'JOIN symbols' not in statement
                        and 'daily_price_bars.symbol_id IN' in statement
                        and 'FOR UPDATE OF daily_price_bars' in statement
                        for statement in normalized_sql
                    )
                )
                session.expire_all()
                persisted = session.execute(
                    select(DailyPriceBar).where(DailyPriceBar.id == bar.id)
                ).scalar_one()
                self.assertEqual(
                    (
                        persisted.open_price,
                        persisted.high_price,
                        persisted.low_price,
                        persisted.close_price,
                        persisted.volume,
                        persisted.turnover,
                    ),
                    (100, 110, 94, 108, 120, 12_500),
                )
                self.assertEqual(
                    (
                        persisted.per,
                        persisted.pbr,
                    ),
                    (Decimal('10.5'), Decimal('1.2')),
                )
                self.assertEqual(persisted.source, 'kis_final_bar_repair')
                self.assertIsNone(persisted.eod_run_id)
                self.assertIsNone(persisted.repair_origin_eod_run_id)
                self.assertEqual(
                    persisted.repair_origin_source,
                    'protected_bar_source',
                )
                self.assertGreater(persisted.captured_at, original_captured_at)
                audit = session.execute(
                    select(DailyPriceBarRepairAudit).where(
                        DailyPriceBarRepairAudit.daily_price_bar_id == bar.id,
                        DailyPriceBarRepairAudit.repair_batch_id
                        == result['repair_batch_id'],
                    )
                ).scalar_one()
                self.assertEqual(audit.symbol, symbol_code)
                self.assertEqual(audit.old_close_price, 101)
                self.assertEqual(audit.new_close_price, 108)
                self.assertEqual(audit.original_source, 'protected_bar_source')
                self.assertEqual(audit.original_captured_at, original_captured_at)
                self.assertEqual(audit.original_eod_run_id, original_eod_run_id)
                self.assertEqual(audit.root_source, 'protected_bar_source')
                self.assertIsNone(audit.root_eod_run_id)
                batch = session.get(
                    DailyPriceBarRepairBatch,
                    result['repair_batch_id'],
                )
                self.assertIsNotNone(batch)
                self.assertEqual(batch.status, 'pending')
                self.assertEqual(batch.repaired_row_count, 1)
                self.assertLessEqual(
                    batch.required_evaluation_market_date_from,
                    MARKET_DATE,
                )
                self.assertEqual(
                    batch.required_evaluation_market_date_to,
                    MARKET_DATE,
                )
                self.assertIsNone(batch.evaluation_rebuilt_at)
            finally:
                session.close()
                transaction.rollback()

    def test_second_repair_preserves_the_original_eod_root_lineage(self) -> None:
        symbol_code = uuid4().hex[:6].upper()
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            eod_run = EODCollectionRun(
                market='KRX',
                trade_date=MARKET_DATE,
                source='pykrx',
                trigger='test',
                status='completed',
                symbols_total=1,
                symbols_succeeded=1,
                symbols_failed=0,
                run_metadata={},
            )
            symbol = Symbol(
                symbol=symbol_code,
                name_ko='KIS repeated final bar repair',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add_all([eod_run, symbol])
            session.flush()
            bar = DailyPriceBar(
                symbol_id=symbol.id,
                eod_run_id=eod_run.id,
                trade_date=MARKET_DATE,
                open_price=100,
                high_price=105,
                low_price=95,
                close_price=101,
                volume=100,
                turnover=10_000,
                source='pykrx',
            )
            session.add(bar)
            session.flush()
            session.refresh(bar)

            @contextmanager
            def rollback_only_session_scope(**_kwargs):
                yield session
                session.flush()

            def stored_from_bar() -> StoredFinalBar:
                session.refresh(bar)
                return StoredFinalBar(
                    bar_id=bar.id,
                    symbol=symbol_code,
                    trade_date=MARKET_DATE,
                    open_price=bar.open_price,
                    high_price=bar.high_price,
                    low_price=bar.low_price,
                    close_price=bar.close_price,
                    volume=bar.volume,
                    turnover=bar.turnover,
                    source=bar.source,
                    captured_at=bar.captured_at,
                    eod_run_id=bar.eod_run_id,
                    repair_origin_eod_run_id=bar.repair_origin_eod_run_id,
                    repair_origin_source=bar.repair_origin_source,
                )

            try:
                with patch(
                    'app.eod.kis_final_bar_repair.session_scope',
                    rollback_only_session_scope,
                ):
                    first = repair_existing_final_bars(
                        start_date=MARKET_DATE,
                        end_date=MARKET_DATE,
                        selected_symbols=[symbol_code],
                        initial_rows=[stored_from_bar()],
                        provider_bars={
                            (symbol_code, MARKET_DATE): _provider(
                                symbol=symbol_code,
                            )
                        },
                        apply=True,
                    )
                    second_initial = stored_from_bar()
                    second = repair_existing_final_bars(
                        start_date=MARKET_DATE,
                        end_date=MARKET_DATE,
                        selected_symbols=[symbol_code],
                        initial_rows=[second_initial],
                        provider_bars={
                            (symbol_code, MARKET_DATE): _provider(
                                symbol=symbol_code,
                                high_price=112,
                                low_price=93,
                                close_price=109,
                                volume=130,
                                turnover=13_500,
                            )
                        },
                        apply=True,
                    )

                session.refresh(bar)
                self.assertEqual(bar.repair_origin_eod_run_id, eod_run.id)
                self.assertEqual(bar.repair_origin_source, 'pykrx')
                second_audit = session.execute(
                    select(DailyPriceBarRepairAudit).where(
                        DailyPriceBarRepairAudit.repair_batch_id
                        == second['repair_batch_id']
                    )
                ).scalar_one()
                self.assertEqual(
                    second_audit.original_source,
                    'kis_final_bar_repair',
                )
                self.assertIsNone(second_audit.original_eod_run_id)
                self.assertEqual(second_audit.root_eod_run_id, eod_run.id)
                self.assertEqual(second_audit.root_source, 'pykrx')
                self.assertNotEqual(
                    first['repair_batch_id'],
                    second['repair_batch_id'],
                )
            finally:
                session.close()
                transaction.rollback()

    def test_row_drift_blocks_without_writing(self) -> None:
        symbol_code = uuid4().hex[:6].upper()
        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)
            symbol = Symbol(
                symbol=symbol_code,
                name_ko='KIS final bar drift',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add(symbol)
            session.flush()
            bar = DailyPriceBar(
                symbol_id=symbol.id,
                trade_date=MARKET_DATE,
                open_price=100,
                high_price=105,
                low_price=95,
                close_price=102,
                volume=101,
                turnover=10_100,
                source='test',
            )
            session.add(bar)
            session.flush()

            @contextmanager
            def rollback_only_session_scope(**_kwargs):
                yield session
                session.flush()

            stale_initial = _stored(bar_id=bar.id, symbol=symbol_code)
            provider = _provider(symbol=symbol_code)
            try:
                with patch(
                    'app.eod.kis_final_bar_repair.session_scope',
                    rollback_only_session_scope,
                ):
                    result = repair_existing_final_bars(
                        start_date=MARKET_DATE,
                        end_date=MARKET_DATE,
                        selected_symbols=[symbol_code],
                        initial_rows=[stale_initial],
                        provider_bars={(symbol_code, MARKET_DATE): provider},
                        apply=True,
                    )
                self.assertEqual(result['status'], 'blocked')
                self.assertEqual(result['reason'], 'stored_rows_changed_during_provider_fetch')
                self.assertEqual(bar.volume, 101)
            finally:
                session.close()
                transaction.rollback()


if __name__ == '__main__':
    unittest.main()
