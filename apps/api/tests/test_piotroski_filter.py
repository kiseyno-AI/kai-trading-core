import unittest
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import create_engine, delete
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db_models import (
    DailyPriceBar,
    DailyTechnicalSnapshot,
    DartAuditSignal,
    DartDilutionSignal,
    DartDisclosureEvent,
    DartFundamentalsHighlight,
    DartFundamentalsSnapshot,
    Symbol,
)
from app.signals.piotroski_filter import (
    LATEST_FALLBACK_POLICY,
    DailyPiotroskiFilterService,
    PiotroskiInputs,
)


def _build_input(
    *,
    symbol: str,
    pbr: float,
    return_20d: float,
    has_audit_issue: bool = False,
) -> PiotroskiInputs:
    return PiotroskiInputs(
        symbol=symbol,
        name_ko=symbol,
        market_date=date(2026, 4, 14),
        pbr=pbr,
        return_20d=return_20d,
        total_equity=100.0,
        net_income=20.0,
        prior_net_income=10.0,
        operating_cash_flow=30.0,
        total_assets=200.0,
        prior_total_assets=180.0,
        debt_ratio=0.3,
        prior_debt_ratio=0.4,
        current_ratio=1.8,
        prior_current_ratio=1.2,
        gross_margin=0.35,
        prior_gross_margin=0.30,
        asset_turnover=1.4,
        prior_asset_turnover=1.2,
        has_audit_issue=has_audit_issue,
        has_dilution_signal=False,
    )


class DailyPiotroskiFilterServiceTests(unittest.TestCase):
    def test_collect_and_persist_stores_passed_and_not_passed_symbols(self):
        service = DailyPiotroskiFilterService()
        inputs = [
            _build_input(symbol='005930', pbr=0.4, return_20d=8.0),
            _build_input(symbol='000660', pbr=2.0, return_20d=-8.0, has_audit_issue=True),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=inputs),
            patch('app.signals.piotroski_filter.upsert_daily_piotroski_filter_result') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date='2026-04-14')

        self.assertEqual(result['symbols_evaluated'], 2)
        self.assertEqual(result['symbols_passed'], 1)
        self.assertEqual(result['symbols_not_passed'], 1)
        self.assertEqual(result['results_persisted'], 2)
        self.assertEqual(result['results_failed'], 0)
        self.assertEqual(upsert.call_count, 2)

        persisted_by_symbol = {call.args[0]['symbol']: call.args[0] for call in upsert.call_args_list}
        self.assertTrue(persisted_by_symbol['005930']['passed_filter'])
        self.assertFalse(persisted_by_symbol['000660']['passed_filter'])
        self.assertTrue(persisted_by_symbol['005930']['passes_piotroski_score'])
        self.assertTrue(persisted_by_symbol['000660']['is_bottom_momentum'])

    def test_collect_and_persist_records_point_in_time_input_provenance(self):
        service = DailyPiotroskiFilterService()
        input_row = _build_input(symbol='005930', pbr=0.4, return_20d=8.0)
        input_row.fundamentals_provenance = {
            2025: {
                'snapshot_id': 25,
                'disclosure_event_id': 125,
                'business_year': 2025,
                'report_code': '11011',
                'fs_div': 'CFS',
                'rcept_no': '202603200001',
                'receipt_date': '2026-03-20',
                'captured_at': '2026-03-20T01:00:00+00:00',
            }
        }
        input_row.audit_provenance = {
            'id': 31,
            'rcept_no': '202604100001',
            'receipt_date': '2026-04-10',
        }
        input_row.dilution_provenance = {
            'id': 41,
            'rcept_no': '202604110001',
            'receipt_date': '2026-04-11',
        }

        with (
            patch.object(service, '_load_inputs', return_value=[input_row]),
            patch('app.signals.piotroski_filter.upsert_daily_piotroski_filter_result') as upsert,
        ):
            service.rebuild_for_market_date(market_date='2026-04-14')

        provenance = upsert.call_args.args[0]['filter_payload']['provenance']
        self.assertEqual(provenance['as_of_market_date'], '2026-04-14')
        self.assertEqual(provenance['universe_policy'], 'exact_market_date_daily_price_bar')
        self.assertEqual(provenance['fundamentals_by_business_year']['2025']['snapshot_id'], 25)
        self.assertEqual(provenance['audit_signal']['id'], 31)
        self.assertEqual(provenance['dilution_signal']['id'], 41)

    def test_collect_and_persist_continues_when_one_symbol_write_fails(self):
        service = DailyPiotroskiFilterService()
        inputs = [
            _build_input(symbol='005930', pbr=0.4, return_20d=8.0),
            _build_input(symbol='000660', pbr=2.0, return_20d=-8.0),
        ]

        def failing_upsert(payload: dict) -> None:
            if payload['symbol'] == '000660':
                raise RuntimeError('write failed')

        with (
            patch.object(service, '_load_inputs', return_value=inputs),
            patch(
                'app.signals.piotroski_filter.upsert_daily_piotroski_filter_result',
                side_effect=failing_upsert,
            ),
        ):
            result = service.rebuild_for_market_date(market_date='2026-04-14')

        self.assertEqual(result['symbols_evaluated'], 2)
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['results_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['000660'])

    def test_inspect_returns_exact_market_date_when_requested_date_is_available(self):
        service = DailyPiotroskiFilterService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-14',
                'passed_filter': True,
            },
            {
                'symbol': '000660',
                'market_date': '2026-04-14',
                'passed_filter': False,
            },
        ]

        with (
            patch(
                'app.signals.piotroski_filter.load_latest_available_daily_piotroski_filter_result_date',
                return_value='2026-04-14',
            ) as latest_loader,
            patch(
                'app.signals.piotroski_filter.load_daily_piotroski_filter_results',
                return_value=stored_items,
            ) as loader,
        ):
            result = service.inspect(market_date='2026-04-14')

        latest_loader.assert_called_once_with(
            market_date=date(2026, 4, 14),
            symbols=None,
        )
        loader.assert_called_once_with(
            market_date='2026-04-14',
            symbols=None,
            limit=None,
        )
        self.assertEqual(result['requested_market_date'], '2026-04-14')
        self.assertEqual(result['resolved_market_date'], '2026-04-14')
        self.assertEqual(result['market_date'], '2026-04-14')
        self.assertFalse(result['latest_mode'])
        self.assertFalse(result['is_latest_fallback'])
        self.assertEqual(result['fallback_policy'], LATEST_FALLBACK_POLICY)
        self.assertEqual(result['summary']['entries'], 2)
        self.assertEqual(result['summary']['passed_count'], 1)
        self.assertEqual(result['summary']['not_passed_count'], 1)
        self.assertEqual(result['items'], stored_items)

    def test_inspect_uses_latest_mode_when_market_date_is_omitted(self):
        service = DailyPiotroskiFilterService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-23',
                'passed_filter': True,
            },
            {
                'symbol': '000660',
                'market_date': '2026-04-23',
                'passed_filter': False,
            },
        ]

        with (
            patch(
                'app.signals.piotroski_filter.load_latest_available_daily_piotroski_filter_result_date',
                return_value='2026-04-23',
            ) as latest_loader,
            patch(
                'app.signals.piotroski_filter.load_daily_piotroski_filter_results',
                return_value=stored_items,
            ) as loader,
        ):
            result = service.inspect()

        latest_loader.assert_called_once_with(
            market_date=None,
            symbols=None,
        )
        loader.assert_called_once_with(
            market_date='2026-04-23',
            symbols=None,
            limit=None,
        )
        self.assertIsNone(result['requested_market_date'])
        self.assertEqual(result['resolved_market_date'], '2026-04-23')
        self.assertEqual(result['market_date'], '2026-04-23')
        self.assertTrue(result['latest_mode'])
        self.assertFalse(result['is_latest_fallback'])
        self.assertEqual(result['summary']['entries'], 2)
        self.assertEqual(result['items'], stored_items)

    def test_inspect_uses_latest_fallback_when_requested_market_date_is_missing(self):
        service = DailyPiotroskiFilterService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-23',
                'passed_filter': True,
            },
            {
                'symbol': '000660',
                'market_date': '2026-04-23',
                'passed_filter': False,
            },
        ]

        with (
            patch(
                'app.signals.piotroski_filter.load_latest_available_daily_piotroski_filter_result_date',
                return_value='2026-04-23',
            ) as latest_loader,
            patch(
                'app.signals.piotroski_filter.load_daily_piotroski_filter_results',
                return_value=stored_items,
            ) as loader,
        ):
            result = service.inspect(market_date='2026-04-24')

        latest_loader.assert_called_once_with(
            market_date=date(2026, 4, 24),
            symbols=None,
        )
        loader.assert_called_once_with(
            market_date='2026-04-23',
            symbols=None,
            limit=None,
        )
        self.assertEqual(result['requested_market_date'], '2026-04-24')
        self.assertEqual(result['resolved_market_date'], '2026-04-23')
        self.assertEqual(result['market_date'], '2026-04-23')
        self.assertFalse(result['latest_mode'])
        self.assertTrue(result['is_latest_fallback'])
        self.assertEqual(result['fallback_policy'], LATEST_FALLBACK_POLICY)
        self.assertEqual(result['items'], stored_items)

    def test_inspect_returns_single_item_when_symbol_is_requested(self):
        service = DailyPiotroskiFilterService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-23',
                'passed_filter': True,
            }
        ]

        with (
            patch(
                'app.signals.piotroski_filter.load_latest_available_daily_piotroski_filter_result_date',
                return_value='2026-04-23',
            ) as latest_loader,
            patch(
                'app.signals.piotroski_filter.load_daily_piotroski_filter_results',
                return_value=stored_items,
            ) as loader,
        ):
            result = service.inspect(symbol='5930', limit=25)

        latest_loader.assert_called_once_with(
            market_date=None,
            symbols=['005930'],
        )
        loader.assert_called_once_with(
            market_date='2026-04-23',
            symbols=['005930'],
            limit=1,
        )
        self.assertTrue(result['latest_mode'])
        self.assertEqual(result['resolved_market_date'], '2026-04-23')
        self.assertEqual(result['summary']['requested_symbol'], '005930')
        self.assertEqual(result['summary']['missing_symbols'], [])
        self.assertEqual(result['item'], stored_items[0])

    def test_inspect_rejects_blank_symbol(self):
        service = DailyPiotroskiFilterService()

        with self.assertRaisesRegex(ValueError, 'symbol must not be blank'):
            service.inspect(market_date='2026-04-14', symbol='   ')


class DailyPiotroskiPointInTimeInputsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.engine = create_engine(
            'sqlite+pysqlite:///:memory:',
            connect_args={'check_same_thread': False},
            poolclass=StaticPool,
        )
        tables = (
            Symbol.__table__,
            DailyPriceBar.__table__,
            DailyTechnicalSnapshot.__table__,
            DartDisclosureEvent.__table__,
            DartFundamentalsSnapshot.__table__,
            DartFundamentalsHighlight.__table__,
            DartAuditSignal.__table__,
            DartDilutionSignal.__table__,
        )
        for table in tables:
            table.create(bind=cls.engine, checkfirst=True)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.engine.dispose()

    def setUp(self) -> None:
        with Session(self.engine) as session:
            for model in (
                DartFundamentalsHighlight,
                DartFundamentalsSnapshot,
                DartDisclosureEvent,
                DartAuditSignal,
                DartDilutionSignal,
                DailyTechnicalSnapshot,
                DailyPriceBar,
                Symbol,
            ):
                session.execute(delete(model))
            session.commit()
        self.service = DailyPiotroskiFilterService()

    @contextmanager
    def _session_scope(self):
        with Session(self.engine) as session:
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise

    def _add_symbol(self, *, symbol: str, is_active: bool) -> int:
        with Session(self.engine) as session:
            row = Symbol(
                symbol=symbol,
                name_ko=symbol,
                market='KOSPI',
                source='test',
                is_active=is_active,
            )
            session.add(row)
            session.commit()
            return row.id

    def _add_price_bar(self, *, symbol_id: int, trade_date: date, pbr: float = 1.0) -> None:
        with Session(self.engine) as session:
            session.add(
                DailyPriceBar(
                    symbol_id=symbol_id,
                    trade_date=trade_date,
                    open_price=100,
                    high_price=110,
                    low_price=90,
                    close_price=105,
                    volume=1_000,
                    turnover=105_000,
                    pbr=pbr,
                    source='test',
                )
            )
            session.commit()

    def _add_fundamentals_snapshot(
        self,
        *,
        symbol: str,
        business_year: int,
        receipt_date: date,
        net_income: int,
    ) -> tuple[int, int, str]:
        suffix = uuid4().hex[:16].upper()
        rcept_no = f'PIT{suffix}'
        with Session(self.engine) as session:
            event = DartDisclosureEvent(
                rcept_no=rcept_no,
                corp_code=f'C{suffix[:12]}',
                corp_name=symbol,
                stock_code=symbol,
                report_nm=f'Annual Report {business_year}',
                filing_category='annual_report',
                is_financial_report=True,
                receipt_date=receipt_date,
                payload={'rcept_no': rcept_no},
                fundamentals_capture_status='captured',
            )
            session.add(event)
            session.flush()
            snapshot = DartFundamentalsSnapshot(
                disclosure_event_id=event.id,
                rcept_no=rcept_no,
                corp_code=event.corp_code,
                stock_code=symbol,
                corp_name=symbol,
                business_year=business_year,
                report_code='11011',
                filing_category='annual_report',
                fs_div='CFS',
                statement_type='financial_statement',
                period_type='annual',
                period_end_month=12,
                currency='KRW',
                request_payload={},
                response_payload={},
            )
            session.add(snapshot)
            session.flush()
            session.add(
                DartFundamentalsHighlight(
                    snapshot_id=snapshot.id,
                    metric_key='net_income',
                    metric_type='flow',
                    current_amount=net_income,
                    currency='KRW',
                )
            )
            snapshot_id = snapshot.id
            event_id = event.id
            session.commit()
        return snapshot_id, event_id, rcept_no

    def _add_audit_signal(
        self,
        *,
        symbol: str,
        receipt_date: date | None,
        has_audit_issue: bool,
    ) -> tuple[int, str]:
        rcept_no = f'PAU{uuid4().hex[:16].upper()}'
        with Session(self.engine) as session:
            row = DartAuditSignal(
                rcept_no=rcept_no,
                corp_code=f'C{uuid4().hex[:12].upper()}',
                stock_code=symbol,
                corp_name=symbol,
                receipt_date=receipt_date,
                has_audit_issue=has_audit_issue,
            )
            session.add(row)
            session.commit()
            return row.id, rcept_no

    def _add_dilution_signal(
        self,
        *,
        symbol: str,
        receipt_date: date | None,
        has_dilution_signal: bool,
    ) -> tuple[int, str]:
        rcept_no = f'PDI{uuid4().hex[:16].upper()}'
        with Session(self.engine) as session:
            row = DartDilutionSignal(
                rcept_no=rcept_no,
                corp_code=f'C{uuid4().hex[:12].upper()}',
                stock_code=symbol,
                corp_name=symbol,
                receipt_date=receipt_date,
                signal_type='share_issuance',
                has_dilution_signal=has_dilution_signal,
            )
            session.add(row)
            session.commit()
            return row.id, rcept_no

    def test_load_inputs_uses_exact_date_price_bar_universe_even_if_symbol_is_inactive(self) -> None:
        market_date = date(2026, 4, 14)
        historical_symbol_id = self._add_symbol(symbol='099999', is_active=False)
        current_only_symbol_id = self._add_symbol(symbol='005930', is_active=True)
        self._add_price_bar(symbol_id=historical_symbol_id, trade_date=market_date, pbr=0.8)
        self._add_price_bar(
            symbol_id=current_only_symbol_id,
            trade_date=date(2026, 4, 15),
            pbr=1.2,
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(
                market_date,
                annual_year=2025,
                prior_annual_year=2024,
            )

        self.assertEqual([item.symbol for item in inputs], ['099999'])
        self.assertEqual(inputs[0].pbr, 0.8)

    def test_load_inputs_selects_latest_annual_snapshot_available_as_of_market_date(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2024,
            receipt_date=date(2025, 3, 20),
            net_income=70,
        )
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 4, 10),
            net_income=100,
        )
        expected_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 4, 10),
            net_income=110,
        )
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 4, 15),
            net_income=999,
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(
                market_date,
                annual_year=2025,
                prior_annual_year=2024,
            )

        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].net_income, 110.0)
        self.assertEqual(inputs[0].prior_net_income, 70.0)
        self.assertEqual(
            inputs[0].fundamentals_provenance[2025]['snapshot_id'],
            expected_snapshot_id,
        )

    def test_load_inputs_selects_latest_dart_signals_available_as_of_market_date(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)

        self._add_audit_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 10),
            has_audit_issue=True,
        )
        expected_audit_id, expected_audit_rcept_no = self._add_audit_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 10),
            has_audit_issue=False,
        )
        self._add_audit_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 15),
            has_audit_issue=True,
        )
        self._add_audit_signal(
            symbol=symbol,
            receipt_date=None,
            has_audit_issue=True,
        )

        self._add_dilution_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 10),
            has_dilution_signal=True,
        )
        expected_dilution_id, expected_dilution_rcept_no = self._add_dilution_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 10),
            has_dilution_signal=False,
        )
        self._add_dilution_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 15),
            has_dilution_signal=True,
        )
        self._add_dilution_signal(
            symbol=symbol,
            receipt_date=None,
            has_dilution_signal=True,
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(
                market_date,
                annual_year=2025,
                prior_annual_year=2024,
            )

        self.assertEqual(len(inputs), 1)
        self.assertFalse(inputs[0].has_audit_issue)
        self.assertFalse(inputs[0].has_dilution_signal)
        self.assertEqual(inputs[0].audit_provenance['id'], expected_audit_id)
        self.assertEqual(inputs[0].audit_provenance['rcept_no'], expected_audit_rcept_no)
        self.assertEqual(inputs[0].dilution_provenance['id'], expected_dilution_id)
        self.assertEqual(
            inputs[0].dilution_provenance['rcept_no'],
            expected_dilution_rcept_no,
        )
