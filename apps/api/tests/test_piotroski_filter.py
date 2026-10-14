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
    DailyPykrxEnrichmentRow,
    DailyTechnicalSnapshot,
    DartAuditSignal,
    DartDilutionSignal,
    DartDisclosureEvent,
    DartFundamentalsHighlight,
    DartFundamentalsSnapshot,
    Symbol,
)
from app.signals.piotroski_filter import (
    CANONICAL_DEBT_RATIO,
    LATEST_FALLBACK_POLICY,
    DailyPiotroskiFilterService,
    PiotroskiInputs,
    _compute_signals,
    _ratio_or_derived,
)


def _build_input(
    *,
    symbol: str,
    pbr: float,
    return_20d: float,
    has_audit_issue: bool | None = False,
    has_dilution_signal: bool | None = None,
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
        has_dilution_signal=has_dilution_signal,
    )


class DailyPiotroskiFilterServiceTests(unittest.TestCase):
    def test_canonical_debt_ratio_does_not_bypass_negative_equity_validation(self):
        result = _ratio_or_derived(
            symbol='005930',
            business_year=2025,
            ratios_by_symbol={
                '005930': {(CANONICAL_DEBT_RATIO, 2025): -2.0},
            },
            highlights_by_symbol={
                '005930': {
                    ('total_liabilities', 2025): 200.0,
                    ('total_equity', 2025): -100.0,
                },
            },
            ratio_metric_keys=(CANONICAL_DEBT_RATIO,),
            numerator_metric_key='total_liabilities',
            denominator_metric_key='total_equity',
            require_positive_denominator=True,
        )

        self.assertIsNone(result)

    def test_incomplete_signals_cannot_pass_on_normalized_single_signal_score(self):
        service = DailyPiotroskiFilterService()
        incomplete = _build_input(symbol='005930', pbr=0.4, return_20d=8.0)
        incomplete.prior_net_income = None
        incomplete.operating_cash_flow = None
        incomplete.prior_total_assets = None
        incomplete.debt_ratio = None
        incomplete.prior_debt_ratio = None
        incomplete.current_ratio = None
        incomplete.prior_current_ratio = None
        incomplete.gross_margin = None
        incomplete.prior_gross_margin = None
        incomplete.asset_turnover = None
        incomplete.prior_asset_turnover = None
        incomplete.has_dilution_signal = None

        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=[
                    incomplete,
                    _build_input(symbol='000660', pbr=2.0, return_20d=-8.0),
                ],
            ),
            patch(
                'app.signals.piotroski_filter.upsert_daily_piotroski_filter_results'
            ) as upsert,
        ):
            service.rebuild_for_market_date(market_date='2026-04-14')

        persisted = {
            payload['symbol']: payload
            for payload in upsert.call_args.args[0]
        }['005930']
        self.assertEqual(persisted['piotroski_score'], 1)
        self.assertEqual(
            persisted['filter_payload']['score_summary']['available_signal_count'],
            1,
        )
        self.assertFalse(persisted['passes_piotroski_score'])
        self.assertFalse(persisted['passed_filter'])

    def test_missing_optional_dilution_event_keeps_raw_score_conservative_without_blocking_core_coverage(self):
        service = DailyPiotroskiFilterService()
        candidate = _build_input(
            symbol='005930',
            pbr=0.4,
            return_20d=8.0,
            has_dilution_signal=None,
        )

        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=[
                    candidate,
                    _build_input(symbol='000660', pbr=2.0, return_20d=-8.0),
                ],
            ),
            patch(
                'app.signals.piotroski_filter.upsert_daily_piotroski_filter_results'
            ) as upsert,
        ):
            service.rebuild_for_market_date(market_date='2026-04-14')

        persisted = {
            payload['symbol']: payload
            for payload in upsert.call_args.args[0]
        }['005930']
        summary = persisted['filter_payload']['score_summary']
        self.assertEqual(persisted['piotroski_score'], 8)
        self.assertEqual(summary['available_signal_count'], 8)
        self.assertEqual(summary['required_financial_signal_count'], 8)
        self.assertEqual(summary['required_financial_signals_available'], 8)
        self.assertEqual(
            summary['missing_optional_signal_names'],
            ['no_new_share_issuance'],
        )
        self.assertTrue(persisted['passes_piotroski_score'])
        self.assertTrue(persisted['passed_filter'])

    def test_missing_audit_evidence_is_unknown_and_fails_closed(self):
        service = DailyPiotroskiFilterService()
        inputs = [
            _build_input(
                symbol='005930',
                pbr=0.4,
                return_20d=8.0,
                has_audit_issue=None,
            ),
            _build_input(symbol='000660', pbr=2.0, return_20d=-8.0),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=inputs),
            patch('app.signals.piotroski_filter.upsert_daily_piotroski_filter_results') as upsert,
        ):
            service.rebuild_for_market_date(market_date='2026-04-14')

        persisted_by_symbol = {
            payload['symbol']: payload for payload in upsert.call_args.args[0]
        }
        missing_audit = persisted_by_symbol['005930']
        self.assertFalse(missing_audit['passed_filter'])
        self.assertTrue(missing_audit['has_audit_issue'])
        self.assertEqual(
            missing_audit['filter_payload']['provenance']['audit_evidence_status'],
            'unknown',
        )
        self.assertEqual(
            missing_audit['filter_payload']['evidence_status']['audit'],
            'unavailable',
        )

    def test_missing_financial_inputs_are_unavailable_not_proven_issues(self):
        service = DailyPiotroskiFilterService()
        incomplete = _build_input(
            symbol='005930',
            pbr=0.4,
            return_20d=8.0,
            has_audit_issue=None,
        )
        incomplete.net_income = None
        incomplete.total_equity = None
        incomplete.operating_cash_flow = None
        incomplete.prior_net_income = None
        incomplete.total_assets = None
        incomplete.prior_total_assets = None

        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=[
                    incomplete,
                    _build_input(symbol='000660', pbr=2.0, return_20d=-8.0),
                ],
            ),
            patch(
                'app.signals.piotroski_filter.upsert_daily_piotroski_filter_results'
            ) as upsert,
        ):
            service.rebuild_for_market_date(market_date='2026-04-14')

        payload = {
            item['symbol']: item for item in upsert.call_args.args[0]
        }['005930']
        evidence = payload['filter_payload']['evidence_status']
        self.assertEqual(evidence['profitability'], 'unavailable')
        self.assertEqual(evidence['capital'], 'unavailable')
        self.assertEqual(evidence['audit'], 'unavailable')
        self.assertEqual(evidence['financial_signal_coverage'], 'unavailable')
        self.assertFalse(payload['passed_filter'])

    def test_collect_and_persist_stores_passed_and_not_passed_symbols(self):
        service = DailyPiotroskiFilterService()
        inputs = [
            _build_input(symbol='005930', pbr=0.4, return_20d=8.0),
            _build_input(symbol='000660', pbr=2.0, return_20d=-8.0, has_audit_issue=True),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=inputs),
            patch('app.signals.piotroski_filter.upsert_daily_piotroski_filter_results') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date='2026-04-14')

        self.assertEqual(result['symbols_evaluated'], 2)
        self.assertEqual(result['symbols_passed'], 1)
        self.assertEqual(result['symbols_not_passed'], 1)
        self.assertEqual(result['results_persisted'], 2)
        self.assertEqual(result['results_failed'], 0)
        upsert.assert_called_once()

        persisted_by_symbol = {
            payload['symbol']: payload for payload in upsert.call_args.args[0]
        }
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
            patch('app.signals.piotroski_filter.upsert_daily_piotroski_filter_results') as upsert,
        ):
            service.rebuild_for_market_date(market_date='2026-04-14')

        provenance = upsert.call_args.args[0][0]['filter_payload']['provenance']
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
                'app.signals.piotroski_filter.upsert_daily_piotroski_filter_results',
                side_effect=RuntimeError('bulk write failed'),
            ),
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
            DailyPykrxEnrichmentRow.__table__,
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
                DailyPykrxEnrichmentRow,
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

    def _add_price_bar(self, *, symbol_id: int, trade_date: date, pbr: float | None = 1.0) -> None:
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

    def _add_valuation_snapshot(
        self,
        *,
        symbol_id: int,
        symbol: str,
        market_date: date,
        pbr: float,
    ) -> None:
        with Session(self.engine) as session:
            session.add(
                DailyPykrxEnrichmentRow(
                    symbol_id=symbol_id,
                    market_date=market_date,
                    market='KOSPI',
                    category='valuation_snapshot',
                    scope_type='symbol',
                    scope_key=symbol,
                    source='pykrx',
                    payload={'pbr': pbr, 'market_cap': 1_000_000},
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
        source_status: str = '000',
        fs_div: str = 'CFS',
        rcept_no: str | None = None,
        disclosure_event_id: int | None = None,
        capture_status: str = 'captured',
    ) -> tuple[int, int, str]:
        suffix = uuid4().hex[:16].upper()
        resolved_rcept_no = rcept_no or f'PIT{suffix}'
        with Session(self.engine) as session:
            if disclosure_event_id is None:
                event = DartDisclosureEvent(
                    rcept_no=resolved_rcept_no,
                    corp_code=f'C{suffix[:12]}',
                    corp_name=symbol,
                    stock_code=symbol,
                    report_nm=f'Annual Report {business_year}',
                    filing_category='annual_report',
                    is_financial_report=True,
                    receipt_date=receipt_date,
                    payload={'rcept_no': resolved_rcept_no},
                    fundamentals_capture_status=capture_status,
                )
                session.add(event)
                session.flush()
            else:
                event = session.get(DartDisclosureEvent, disclosure_event_id)
                if event is None:
                    raise AssertionError(
                        f'Missing test disclosure event {disclosure_event_id}'
                    )
            snapshot = DartFundamentalsSnapshot(
                disclosure_event_id=event.id,
                rcept_no=event.rcept_no,
                corp_code=event.corp_code,
                stock_code=symbol,
                corp_name=symbol,
                business_year=business_year,
                report_code='11011',
                filing_category='annual_report',
                fs_div=fs_div,
                statement_type='financial_statement',
                period_type='annual',
                period_end_month=12,
                currency='KRW',
                source_status=source_status,
                request_payload={},
                response_payload={},
            )
            session.add(snapshot)
            session.flush()
            event_rcept_no = event.rcept_no
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
        return snapshot_id, event_id, event_rcept_no

    def _add_audit_signal(
        self,
        *,
        symbol: str,
        receipt_date: date | None,
        has_audit_issue: bool,
        rcept_no: str | None = None,
    ) -> tuple[int, str]:
        resolved_rcept_no = rcept_no or f'PAU{uuid4().hex[:16].upper()}'
        with Session(self.engine) as session:
            row = DartAuditSignal(
                rcept_no=resolved_rcept_no,
                corp_code=f'C{uuid4().hex[:12].upper()}',
                stock_code=symbol,
                corp_name=symbol,
                receipt_date=receipt_date,
                has_audit_issue=has_audit_issue,
            )
            session.add(row)
            session.commit()
            return row.id, resolved_rcept_no

    def _add_amount_highlights(
        self,
        snapshot_id: int,
        *,
        cumulative_metrics: set[str] | None = None,
        **amounts: int,
    ) -> None:
        cumulative_metrics = cumulative_metrics or set()
        with Session(self.engine) as session:
            for metric_key, amount in amounts.items():
                session.add(
                    DartFundamentalsHighlight(
                        snapshot_id=snapshot_id,
                        metric_key=metric_key,
                        metric_type=(
                            'flow'
                            if metric_key in {'net_income', 'operating_cash_flow', 'gross_profit', 'revenue'}
                            else 'stock'
                        ),
                        current_amount=None if metric_key in cumulative_metrics else amount,
                        current_cumulative_amount=amount if metric_key in cumulative_metrics else None,
                        currency='KRW',
                    )
                )
            session.commit()

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
            inputs = self.service._load_inputs(market_date)

        self.assertEqual([item.symbol for item in inputs], ['099999'])
        self.assertEqual(inputs[0].pbr, 0.8)

    def test_load_inputs_prefers_same_day_valuation_snapshot_pbr(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(
            symbol_id=symbol_id,
            trade_date=market_date,
            pbr=None,
        )
        self._add_valuation_snapshot(
            symbol_id=symbol_id,
            symbol=symbol,
            market_date=market_date,
            pbr=0.75,
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].pbr, 0.75)
        self.assertEqual(inputs[0].valuation_source, 'pykrx_valuation_snapshot')

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
            rcept_no='20260410000001',
        )
        expected_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 4, 10),
            net_income=110,
            rcept_no='20260410000002',
        )
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 4, 15),
            net_income=999,
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].net_income, 110.0)
        self.assertEqual(inputs[0].prior_net_income, 70.0)
        self.assertEqual(
            inputs[0].fundamentals_provenance[2025]['snapshot_id'],
            expected_snapshot_id,
        )

    def test_load_inputs_uses_latest_two_annual_years_actually_available_as_of_market_date(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2022,
            receipt_date=date(2023, 3, 20),
            net_income=40,
        )
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2023,
            receipt_date=date(2024, 3, 20),
            net_income=60,
        )
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2024,
            receipt_date=date(2026, 4, 15),
            net_income=999,
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].annual_year, 2023)
        self.assertEqual(inputs[0].prior_annual_year, 2022)
        self.assertEqual(inputs[0].net_income, 60.0)
        self.assertEqual(inputs[0].prior_net_income, 40.0)
        self.assertEqual(sorted(inputs[0].fundamentals_provenance), [2022, 2023])

    def test_load_inputs_excludes_unsuccessful_fundamentals_snapshots(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        valid_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2023,
            receipt_date=date(2024, 3, 20),
            net_income=60,
            source_status='000',
        )
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2024,
            receipt_date=date(2025, 3, 20),
            net_income=999,
            source_status='013',
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(inputs[0].annual_year, 2023)
        self.assertEqual(inputs[0].net_income, 60.0)
        self.assertEqual(inputs[0].fundamentals_provenance[2023]['snapshot_id'], valid_snapshot_id)
        self.assertNotIn(2024, inputs[0].fundamentals_provenance)

    def test_load_inputs_excludes_snapshot_from_failed_capture_event(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        valid_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2024,
            receipt_date=date(2025, 3, 20),
            net_income=60,
        )
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=999,
            capture_status='failed',
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(inputs[0].annual_year, 2024)
        self.assertEqual(inputs[0].net_income, 60.0)
        self.assertEqual(
            inputs[0].fundamentals_provenance[2024]['snapshot_id'],
            valid_snapshot_id,
        )
        self.assertNotIn(2025, inputs[0].fundamentals_provenance)

    def test_load_inputs_prefers_latest_filing_and_falls_back_to_valid_ofs(self) -> None:
        market_date = date(2026, 4, 14)
        ofs_fallback_symbol = '005930'
        cfs_preferred_symbol = '000660'
        for symbol in (ofs_fallback_symbol, cfs_preferred_symbol):
            symbol_id = self._add_symbol(symbol=symbol, is_active=True)
            self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)

        self._add_fundamentals_snapshot(
            symbol=ofs_fallback_symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=999,
            source_status='013',
            fs_div='CFS',
        )
        fallback_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=ofs_fallback_symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 19),
            net_income=80,
            source_status='000',
            fs_div='OFS',
        )

        self._add_fundamentals_snapshot(
            symbol=cfs_preferred_symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 18),
            net_income=100,
            source_status='000',
            fs_div='CFS',
        )
        corrected_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=cfs_preferred_symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=90,
            source_status='000',
            fs_div='OFS',
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        by_symbol = {item.symbol: item for item in inputs}
        fallback = by_symbol[ofs_fallback_symbol]
        preferred = by_symbol[cfs_preferred_symbol]
        self.assertEqual(fallback.net_income, 80.0)
        self.assertEqual(fallback.fundamentals_provenance[2025]['snapshot_id'], fallback_snapshot_id)
        self.assertEqual(fallback.fundamentals_provenance[2025]['fs_div'], 'OFS')
        self.assertEqual(preferred.net_income, 90.0)
        self.assertEqual(preferred.fundamentals_provenance[2025]['snapshot_id'], corrected_snapshot_id)
        self.assertEqual(preferred.fundamentals_provenance[2025]['fs_div'], 'OFS')

    def test_load_inputs_prefers_cfs_within_the_same_filing(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '035420'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        _, event_id, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=90,
            fs_div='OFS',
            rcept_no='20260320000001',
        )
        cfs_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=100,
            fs_div='CFS',
            disclosure_event_id=event_id,
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(inputs[0].net_income, 100.0)
        self.assertEqual(
            inputs[0].fundamentals_provenance[2025]['snapshot_id'],
            cfs_snapshot_id,
        )
        self.assertEqual(inputs[0].fundamentals_provenance[2025]['fs_div'], 'CFS')

    def test_load_inputs_derives_all_financial_signals_from_canonical_raw_highlights(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        prior_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2024,
            receipt_date=date(2025, 3, 20),
            net_income=10,
        )
        current_snapshot_id, _, current_rcept_no = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        self._add_amount_highlights(
            prior_snapshot_id,
            cumulative_metrics={'operating_cash_flow', 'gross_profit', 'revenue'},
            operating_cash_flow=12,
            total_assets=100,
            total_equity=50,
            total_liabilities=50,
            current_assets=30,
            current_liabilities=20,
            gross_profit=28,
            revenue=70,
        )
        self._add_amount_highlights(
            current_snapshot_id,
            cumulative_metrics={'operating_cash_flow', 'gross_profit', 'revenue'},
            operating_cash_flow=30,
            total_assets=100,
            total_equity=60,
            total_liabilities=40,
            current_assets=50,
            current_liabilities=25,
            gross_profit=40,
            revenue=80,
        )
        self._add_audit_signal(
            symbol=symbol,
            receipt_date=date(2026, 3, 21),
            has_audit_issue=False,
            rcept_no=current_rcept_no,
        )
        self._add_dilution_signal(
            symbol=symbol,
            receipt_date=date(2026, 3, 21),
            has_dilution_signal=False,
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        input_row = inputs[0]
        self.assertAlmostEqual(input_row.debt_ratio, 40 / 60)
        self.assertAlmostEqual(input_row.prior_debt_ratio, 1.0)
        self.assertAlmostEqual(input_row.current_ratio, 2.0)
        self.assertAlmostEqual(input_row.prior_current_ratio, 1.5)
        self.assertAlmostEqual(input_row.gross_margin, 0.5)
        self.assertAlmostEqual(input_row.prior_gross_margin, 0.4)
        self.assertAlmostEqual(input_row.asset_turnover, 0.8)
        self.assertAlmostEqual(input_row.prior_asset_turnover, 0.7)
        _, _, available_signal_count = _compute_signals(input_row)
        self.assertEqual(available_signal_count, 9)

    def test_load_inputs_uses_audit_signal_for_selected_annual_filing_only(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        _, _, selected_rcept_no = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 4, 10),
            net_income=20,
            rcept_no='20260410000002',
        )

        self._add_audit_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 9),
            has_audit_issue=False,
        )
        expected_audit_id, expected_audit_rcept_no = self._add_audit_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 10),
            has_audit_issue=True,
            rcept_no=selected_rcept_no,
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
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertTrue(inputs[0].has_audit_issue)
        self.assertFalse(inputs[0].has_dilution_signal)
        self.assertEqual(inputs[0].audit_provenance['id'], expected_audit_id)
        self.assertEqual(inputs[0].audit_provenance['rcept_no'], expected_audit_rcept_no)
        self.assertEqual(inputs[0].dilution_provenance['id'], expected_dilution_id)
        self.assertEqual(
            inputs[0].dilution_provenance['rcept_no'],
            expected_dilution_rcept_no,
        )

    def test_load_inputs_does_not_reuse_prior_clean_audit_for_newer_filing(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        _, _, prior_rcept_no = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2024,
            receipt_date=date(2025, 3, 20),
            net_income=10,
        )
        _, _, current_rcept_no = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        self._add_audit_signal(
            symbol=symbol,
            receipt_date=date(2025, 3, 20),
            has_audit_issue=False,
            rcept_no=prior_rcept_no,
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(inputs[0].fundamentals_provenance[2025]['rcept_no'], current_rcept_no)
        self.assertIsNone(inputs[0].has_audit_issue)
        self.assertIsNone(inputs[0].audit_provenance)

    def test_load_inputs_represents_missing_audit_signal_as_unknown(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertIsNone(inputs[0].has_audit_issue)
        self.assertIsNone(inputs[0].audit_provenance)
