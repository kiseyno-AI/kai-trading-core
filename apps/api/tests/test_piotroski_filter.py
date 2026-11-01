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
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
    Symbol,
)
from app.eod.daily_pipeline_outcomes import daily_pipeline_outcome_is_satisfied
from app.dart.signals_service import build_authoritative_absence_audit_signal
from app.signals.piotroski_filter import (
    CANONICAL_DEBT_RATIO,
    LATEST_FALLBACK_POLICY,
    PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES,
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


def _complete_negative_audit_diagnostics() -> dict:
    return {
        'version': 'dart_audit_archive_diagnostics_v1',
        'archive_sha256': 'a' * 64,
        'archive_size_bytes': 321,
        'member_count': 1,
        'members': [],
        'decoded_member_count': 1,
        'aud_opn_table_count': 1,
        'audit_report_label_row_count': 1,
        'known_opinion_token_row_count': 1,
        'positive_authority_evidence_count': 0,
        'negative_authority_evidence_count': 1,
        'ambiguous_authority_evidence_count': 0,
        'truncated': False,
    }


def _parsed_audit_signal_fields(opinion_code: str) -> dict:
    opinion_labels = {
        'unqualified': '적정',
        'qualified': '한정',
        'adverse': '부적정',
        'disclaimer': '의견거절',
    }
    opinion_label = opinion_labels[opinion_code]
    source_section = '감사의견'
    return {
        'opinion_label': opinion_label,
        'source_section': source_section,
        'evidence_text': f'{source_section}: {opinion_label}',
        'payload': {
            'parser': 'semantic_audit_table_v1',
            'opinion_cell': opinion_label,
            'row_cells': [source_section, opinion_label],
        },
    }


class DailyPiotroskiFilterServiceTests(unittest.TestCase):
    @staticmethod
    def _remove_all_required_financial_evidence(
        item: PiotroskiInputs,
    ) -> PiotroskiInputs:
        item.net_income = None
        item.prior_net_income = None
        item.operating_cash_flow = None
        item.total_assets = None
        item.prior_total_assets = None
        item.debt_ratio = None
        item.prior_debt_ratio = None
        item.current_ratio = None
        item.prior_current_ratio = None
        item.gross_margin = None
        item.prior_gross_margin = None
        item.asset_turnover = None
        item.prior_asset_turnover = None
        return item

    @staticmethod
    def _make_required_financial_evidence_incomplete(
        item: PiotroskiInputs,
    ) -> PiotroskiInputs:
        item.prior_net_income = None
        item.prior_total_assets = None
        item.prior_debt_ratio = None
        item.prior_current_ratio = None
        item.prior_gross_margin = None
        item.prior_asset_turnover = None
        return item

    def test_zero_full_signal_usable_rows_are_partial_but_nonblocking(self):
        service = DailyPiotroskiFilterService()
        inputs = [
            self._remove_all_required_financial_evidence(
                _build_input(symbol='005930', pbr=0.4, return_20d=8.0)
            ),
            self._remove_all_required_financial_evidence(
                _build_input(symbol='000660', pbr=2.0, return_20d=-8.0)
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=inputs),
            patch(
                'app.signals.piotroski_filter.'
                'upsert_daily_piotroski_filter_results'
            ) as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-04-14',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'piotroski_evidence_unusable')
        self.assertEqual(result['requested_symbol_count'], 2)
        self.assertEqual(result['results_persisted'], 2)
        self.assertEqual(result['full_signal_usable_count'], 0)
        self.assertEqual(result['full_signal_incomplete_count'], 0)
        self.assertEqual(result['full_signal_unusable_count'], 2)
        self.assertEqual(result['full_signal_coverage_ratio'], 0.0)
        self.assertEqual(
            result['required_financial_signal_missing_counts'],
            {
                'positive_roa': 2,
                'positive_operating_cash_flow': 2,
                'improving_roa': 2,
                'accrual_quality': 2,
                'lower_leverage': 2,
                'improving_current_ratio': 2,
                'improving_gross_margin': 2,
                'improving_asset_turnover': 2,
            },
        )
        self.assertEqual(
            result['required_financial_input_missing_counts'],
            {
                'missing_net_income': 2,
                'missing_total_assets': 2,
                'missing_operating_cash_flow': 2,
                'missing_prior_net_income': 2,
                'missing_prior_total_assets': 2,
                'missing_debt_ratio': 2,
                'missing_prior_debt_ratio': 2,
                'missing_current_ratio': 2,
                'missing_prior_current_ratio': 2,
                'missing_gross_margin': 2,
                'missing_prior_gross_margin': 2,
                'missing_asset_turnover': 2,
                'missing_prior_asset_turnover': 2,
            },
        )
        score_summary = upsert.call_args.args[0][0]['filter_payload'][
            'score_summary'
        ]
        self.assertTrue(score_summary['full_signal_coverage_required'])
        self.assertEqual(
            score_summary['full_signal_coverage_status'],
            'unusable',
        )
        self.assertEqual(
            score_summary['missing_required_financial_signal_names'],
            list(PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES),
        )
        self.assertEqual(
            score_summary['missing_required_financial_signal_reasons'][
                'positive_roa'
            ],
            ['missing_net_income', 'missing_total_assets'],
        )
        self.assertTrue(
            daily_pipeline_outcome_is_satisfied(
                {'stage': 'piotroski_filter', **result}
            )
        )

    def test_partial_full_signal_coverage_reports_quality_warning(self):
        service = DailyPiotroskiFilterService()
        inputs = [
            _build_input(symbol='005930', pbr=0.4, return_20d=8.0),
            self._make_required_financial_evidence_incomplete(
                _build_input(symbol='000660', pbr=2.0, return_20d=-8.0)
            ),
            self._remove_all_required_financial_evidence(
                _build_input(symbol='035420', pbr=0.6, return_20d=3.0)
            ),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=inputs),
            patch(
                'app.signals.piotroski_filter.'
                'upsert_daily_piotroski_filter_results'
            ),
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-04-14',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'piotroski_evidence_incomplete')
        self.assertEqual(result['full_signal_usable_count'], 1)
        self.assertEqual(result['full_signal_incomplete_count'], 1)
        self.assertEqual(result['full_signal_unusable_count'], 1)
        self.assertAlmostEqual(result['full_signal_coverage_ratio'], 1 / 3)
        self.assertEqual(
            result['required_financial_signal_missing_counts'],
            {
                'positive_roa': 1,
                'positive_operating_cash_flow': 1,
                'improving_roa': 2,
                'accrual_quality': 1,
                'lower_leverage': 2,
                'improving_current_ratio': 2,
                'improving_gross_margin': 2,
                'improving_asset_turnover': 2,
            },
        )
        self.assertTrue(
            daily_pipeline_outcome_is_satisfied(
                {'stage': 'piotroski_filter', **result}
            )
        )

    def test_all_full_signal_usable_rows_remain_completed(self):
        service = DailyPiotroskiFilterService()
        inputs = [
            _build_input(symbol='005930', pbr=0.4, return_20d=8.0),
            _build_input(symbol='000660', pbr=2.0, return_20d=-8.0),
        ]

        with (
            patch.object(service, '_load_inputs', return_value=inputs),
            patch(
                'app.signals.piotroski_filter.'
                'upsert_daily_piotroski_filter_results'
            ),
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-04-14',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertNotIn('reason', result)
        self.assertEqual(result['full_signal_usable_count'], 2)
        self.assertEqual(result['full_signal_incomplete_count'], 0)
        self.assertEqual(result['full_signal_unusable_count'], 0)
        self.assertEqual(result['full_signal_coverage_ratio'], 1.0)
        self.assertEqual(result['required_financial_signal_missing_counts'], {})
        self.assertEqual(result['required_financial_input_missing_counts'], {})

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

    def test_missing_dilution_evidence_keeps_raw_score_but_fails_closed(self):
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
        self.assertFalse(persisted['passed_filter'])

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
            _build_input(
                symbol='005930',
                pbr=0.4,
                return_20d=8.0,
                has_dilution_signal=False,
            ),
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

    def test_explicit_symbols_emit_exact_scope_outcome_manifest(self):
        service = DailyPiotroskiFilterService()
        inputs = [
            _build_input(symbol='005930', pbr=0.4, return_20d=8.0),
            _build_input(symbol='000660', pbr=2.0, return_20d=-8.0),
            _build_input(symbol='051910', pbr=0.6, return_20d=4.0),
        ]

        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=inputs,
            ) as load_inputs,
            patch(
                'app.signals.piotroski_filter.'
                'upsert_daily_piotroski_filter_results'
            ) as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-04-14',
                symbols=['5930', '000660', '035420', '005930'],
            )

        load_inputs.assert_called_once_with(
            date(2026, 4, 14),
            symbols=None,
        )
        self.assertEqual(
            [payload['symbol'] for payload in upsert.call_args.args[0]],
            ['005930', '000660'],
        )
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'piotroski_evidence_incomplete')
        self.assertEqual(result['requested_symbol_count'], 3)
        self.assertEqual(result['full_signal_usable_count'], 2)
        self.assertEqual(result['full_signal_incomplete_count'], 0)
        self.assertEqual(result['full_signal_unusable_count'], 1)
        self.assertAlmostEqual(result['full_signal_coverage_ratio'], 2 / 3)
        self.assertEqual(
            result['required_financial_signal_missing_counts'],
            {
                signal_name: 1
                for signal_name in PIOTROSKI_REQUIRED_FINANCIAL_SIGNAL_NAMES
            },
        )
        self.assertEqual(
            result['required_financial_input_missing_counts'],
            {'missing_piotroski_input_row': 1},
        )
        self.assertEqual(
            result['scope_outcome_manifest_version'],
            'piotroski_scope_outcomes_v1',
        )
        self.assertEqual(
            result['selected_stock_codes'],
            ['005930', '000660', '035420'],
        )
        self.assertEqual(
            result['successful_stock_codes'],
            ['000660', '005930'],
        )
        self.assertEqual(result['retry_debt_stock_codes'], ['035420'])
        self.assertEqual(
            result['scope_outcome_manifest'],
            [
                {'stock_code': '005930', 'outcome': 'success'},
                {'stock_code': '000660', 'outcome': 'success'},
                {'stock_code': '035420', 'outcome': 'retry_debt'},
            ],
        )

    def test_ranking_symbols_exclude_off_universe_bar_from_ranks_and_persistence(
        self,
    ) -> None:
        service = DailyPiotroskiFilterService()
        inputs = [
            _build_input(symbol='005930', pbr=0.4, return_20d=8.0),
            _build_input(symbol='000660', pbr=2.0, return_20d=-8.0),
            _build_input(symbol='069500', pbr=0.1, return_20d=20.0),
        ]

        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=inputs,
            ) as load_inputs,
            patch(
                'app.signals.piotroski_filter.'
                'upsert_daily_piotroski_filter_results'
            ) as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date='2026-04-14',
                symbols=['005930', '000660'],
                ranking_symbols=['005930', '000660'],
            )

        load_inputs.assert_called_once_with(
            date(2026, 4, 14),
            symbols=['005930', '000660'],
        )
        self.assertEqual(
            [payload['symbol'] for payload in upsert.call_args.args[0]],
            ['005930', '000660'],
        )
        self.assertEqual(result['selected_stock_codes'], ['005930', '000660'])
        self.assertNotIn('069500', result['successful_stock_codes'])

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
            DartFundamentalsRow.__table__,
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
                DartFundamentalsRow,
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

    def _add_symbol(
        self,
        *,
        symbol: str,
        is_active: bool,
        name_ko: str | None = None,
        market: str = 'KOSPI',
    ) -> int:
        with Session(self.engine) as session:
            row = Symbol(
                symbol=symbol,
                name_ko=name_ko or symbol,
                market=market,
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
        pbr: float | None,
        market_cap: int = 1_000_000,
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
                    payload={'pbr': pbr, 'market_cap': market_cap},
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
        total_equity: int | None = None,
        currency: str = 'KRW',
        total_equity_currency: str | None = None,
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
                    corp_code=f'C{symbol}',
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
                currency=currency,
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
                    currency=currency,
                )
            )
            if total_equity is not None:
                session.add(
                    DartFundamentalsHighlight(
                        snapshot_id=snapshot.id,
                        metric_key='total_equity',
                        metric_type='stock',
                        current_amount=total_equity,
                        currency=total_equity_currency or currency,
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
        disclosure_event_id: int | None = None,
        opinion_code: str | None = None,
        payload: dict | None = None,
    ) -> tuple[int, str]:
        resolved_rcept_no = rcept_no or f'PAU{uuid4().hex[:16].upper()}'
        resolved_opinion_code = (
            opinion_code
            if opinion_code is not None
            else ('qualified' if has_audit_issue else 'unqualified')
        )
        parsed_fields = _parsed_audit_signal_fields(
            resolved_opinion_code
        )
        with Session(self.engine) as session:
            row = DartAuditSignal(
                disclosure_event_id=disclosure_event_id,
                rcept_no=resolved_rcept_no,
                corp_code=f'C{uuid4().hex[:12].upper()}',
                stock_code=symbol,
                corp_name=symbol,
                receipt_date=receipt_date,
                opinion_code=resolved_opinion_code,
                opinion_label=parsed_fields['opinion_label'],
                has_audit_issue=has_audit_issue,
                source_section=parsed_fields['source_section'],
                evidence_text=parsed_fields['evidence_text'],
                payload=(
                    payload
                    if payload is not None
                    else parsed_fields['payload']
                ),
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

    def _add_ratio_highlights(
        self,
        snapshot_id: int,
        **ratios: float,
    ) -> None:
        with Session(self.engine) as session:
            for metric_key, ratio_value in ratios.items():
                session.add(
                    DartFundamentalsHighlight(
                        snapshot_id=snapshot_id,
                        metric_key=metric_key,
                        metric_type='ratio',
                        ratio_value=ratio_value,
                    )
                )
            session.commit()

    def _add_dilution_signal(
        self,
        *,
        symbol: str,
        receipt_date: date | None,
        has_dilution_signal: bool,
        payload: dict | None = None,
        row_corp_code: str | None = None,
        coverage_corp_code: str | None = None,
        normalize_negative_coverage: bool = True,
        signal_type: str = 'share_issuance',
    ) -> tuple[int, str]:
        rcept_no = f'PDI{uuid4().hex[:16].upper()}'
        corp_code = row_corp_code or f'C{symbol}'
        resolved_payload = payload
        if (
            not has_dilution_signal
            and normalize_negative_coverage
            and isinstance(payload, dict)
        ):
            coverage = payload.get('evidence_coverage')
            if isinstance(coverage, dict):
                resolved_payload = {
                    **payload,
                    'evidence_coverage': {
                        **coverage,
                        'rule_version': 'dart_share_issuance_rules_v1',
                        'corp_code': coverage_corp_code or corp_code,
                        'pagination_completed': True,
                        'scan_completed_at': '2026-04-14T12:00:00+00:00',
                        'authority_policy_version': 'krx_post_close_snapshot_overlap_v1',
                        'authoritative_as_of_cutoff': True,
                        'late_arrival_reconciliation': 'next_scan_one_day_overlap_v1',
                    },
                }
        with Session(self.engine) as session:
            row = DartDilutionSignal(
                rcept_no=rcept_no,
                corp_code=corp_code,
                stock_code=symbol,
                corp_name=symbol,
                receipt_date=receipt_date,
                signal_type=signal_type,
                has_dilution_signal=has_dilution_signal,
                payload=resolved_payload,
            )
            session.add(row)
            session.commit()
            return row.id, rcept_no

    def _add_statement_rows(
        self,
        snapshot_id: int,
        **amounts: tuple[str, str, int],
    ) -> None:
        with Session(self.engine) as session:
            for metric_key, (statement_type, account_id, amount) in amounts.items():
                session.add(
                    DartFundamentalsRow(
                        snapshot_id=snapshot_id,
                        row_key=f'{metric_key}:{uuid4().hex}',
                        statement_type=statement_type,
                        account_id=account_id,
                        account_name=metric_key,
                        current_amount=amount,
                        row_order=1,
                        currency='KRW',
                        payload={},
                    )
                )
            session.commit()

    def _add_comparative_statement_rows(
        self,
        snapshot_id: int,
        **amounts: tuple[str, str, int, int],
    ) -> None:
        with Session(self.engine) as session:
            for metric_key, (
                statement_type,
                account_id,
                current_amount,
                prior_amount,
            ) in amounts.items():
                is_flow = statement_type in {'IS', 'CIS', 'CF'}
                session.add(
                    DartFundamentalsRow(
                        snapshot_id=snapshot_id,
                        row_key=f'{metric_key}:{uuid4().hex}',
                        statement_type=statement_type,
                        account_id=account_id,
                        account_name=metric_key,
                        current_amount=(
                            None if is_flow else current_amount
                        ),
                        current_cumulative_amount=(
                            current_amount if is_flow else None
                        ),
                        prior_amount=(
                            None if is_flow else prior_amount
                        ),
                        prior_cumulative_amount=(
                            prior_amount if is_flow else None
                        ),
                        row_order=1,
                        currency='KRW',
                        payload={},
                    )
                )
            session.commit()

    def test_load_inputs_reuses_preferred_issuer_metrics_and_signals_with_provenance(
        self,
    ) -> None:
        market_date = date(2026, 4, 14)
        issuer_symbol = '005930'
        preferred_symbol = '005935'
        self._add_symbol(
            symbol=issuer_symbol,
            is_active=True,
            name_ko='삼성전자',
        )
        preferred_symbol_id = self._add_symbol(
            symbol=preferred_symbol,
            is_active=True,
            name_ko='삼성전자우',
        )
        self._add_price_bar(
            symbol_id=preferred_symbol_id,
            trade_date=market_date,
            pbr=0.8,
        )
        prior_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=issuer_symbol,
            business_year=2024,
            receipt_date=date(2025, 3, 20),
            net_income=80,
            total_equity=900,
        )
        current_snapshot_id, _, current_rcept_no = (
            self._add_fundamentals_snapshot(
                symbol=issuer_symbol,
                business_year=2025,
                receipt_date=date(2026, 3, 20),
                net_income=120,
                total_equity=1_000,
            )
        )
        self._add_amount_highlights(
            prior_snapshot_id,
            operating_cash_flow=70,
            total_assets=1_800,
        )
        self._add_amount_highlights(
            current_snapshot_id,
            operating_cash_flow=110,
            total_assets=2_000,
        )
        expected_audit_id, _ = self._add_audit_signal(
            symbol=issuer_symbol,
            receipt_date=date(2026, 3, 20),
            has_audit_issue=False,
            rcept_no=current_rcept_no,
        )
        expected_dilution_id, _ = self._add_dilution_signal(
            symbol=issuer_symbol,
            receipt_date=date(2026, 4, 1),
            has_dilution_signal=True,
        )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(
                market_date,
                symbols=[preferred_symbol],
            )

        self.assertEqual(len(inputs), 1)
        input_row = inputs[0]
        self.assertEqual(input_row.symbol, preferred_symbol)
        self.assertEqual(input_row.net_income, 120.0)
        self.assertEqual(input_row.prior_net_income, 80.0)
        self.assertEqual(input_row.annual_year, 2025)
        self.assertEqual(input_row.prior_annual_year, 2024)
        current_provenance = input_row.fundamentals_provenance[2025]
        self.assertEqual(current_provenance['snapshot_id'], current_snapshot_id)
        self.assertEqual(
            current_provenance['requested_symbol'],
            preferred_symbol,
        )
        self.assertEqual(
            current_provenance['fundamentals_issuer_symbol'],
            issuer_symbol,
        )
        self.assertFalse(input_row.has_audit_issue)
        self.assertEqual(input_row.audit_provenance['id'], expected_audit_id)
        self.assertEqual(
            input_row.audit_provenance['fundamentals_issuer_symbol'],
            issuer_symbol,
        )
        self.assertTrue(input_row.has_dilution_signal)
        self.assertEqual(
            input_row.dilution_provenance['id'],
            expected_dilution_id,
        )
        self.assertEqual(
            input_row.dilution_provenance['fundamentals_issuer_symbol'],
            issuer_symbol,
        )
        self.assertEqual(
            input_row.dilution_provenance['corp_code'],
            f'C{issuer_symbol}',
        )

    def test_load_inputs_prefers_direct_preferred_fundamentals_and_signals(
        self,
    ) -> None:
        market_date = date(2026, 4, 14)
        issuer_symbol = '005930'
        preferred_symbol = '005935'
        self._add_symbol(
            symbol=issuer_symbol,
            is_active=True,
            name_ko='삼성전자',
        )
        preferred_symbol_id = self._add_symbol(
            symbol=preferred_symbol,
            is_active=True,
            name_ko='삼성전자우',
        )
        self._add_price_bar(
            symbol_id=preferred_symbol_id,
            trade_date=market_date,
            pbr=0.8,
        )
        self._add_fundamentals_snapshot(
            symbol=issuer_symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 18),
            net_income=999,
        )
        direct_snapshot_id, _, direct_rcept_no = (
            self._add_fundamentals_snapshot(
                symbol=preferred_symbol,
                business_year=2025,
                receipt_date=date(2026, 3, 20),
                net_income=111,
            )
        )
        expected_audit_id, _ = self._add_audit_signal(
            symbol=preferred_symbol,
            receipt_date=date(2026, 3, 20),
            has_audit_issue=False,
            rcept_no=direct_rcept_no,
        )
        expected_dilution_id, _ = self._add_dilution_signal(
            symbol=preferred_symbol,
            receipt_date=date(2026, 4, 1),
            has_dilution_signal=True,
            row_corp_code=f'C{preferred_symbol}',
        )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(
                market_date,
                symbols=[preferred_symbol],
            )

        self.assertEqual(len(inputs), 1)
        input_row = inputs[0]
        self.assertEqual(input_row.net_income, 111.0)
        direct_provenance = input_row.fundamentals_provenance[2025]
        self.assertEqual(direct_provenance['snapshot_id'], direct_snapshot_id)
        self.assertEqual(
            direct_provenance['fundamentals_issuer_symbol'],
            preferred_symbol,
        )
        self.assertEqual(input_row.audit_provenance['id'], expected_audit_id)
        self.assertEqual(
            input_row.audit_provenance['fundamentals_issuer_symbol'],
            preferred_symbol,
        )
        self.assertEqual(
            input_row.dilution_provenance['id'],
            expected_dilution_id,
        )
        self.assertEqual(
            input_row.dilution_provenance['fundamentals_issuer_symbol'],
            preferred_symbol,
        )

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

    def test_load_inputs_derives_pbr_from_krw_equity_and_same_day_market_cap(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date, pbr=None)
        self._add_valuation_snapshot(
            symbol_id=symbol_id,
            symbol=symbol,
            market_date=market_date,
            pbr=None,
            market_cap=2_000_000,
        )
        snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 4, 10),
            net_income=100,
            total_equity=500_000,
            currency='KRW',
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].pbr, 4.0)
        self.assertEqual(
            inputs[0].valuation_source,
            'dart_annual_equity_market_cap_derived',
        )
        valuation_provenance = inputs[0].valuation_provenance
        self.assertIsNotNone(valuation_provenance)
        equity_fact = valuation_provenance['total_equity_fact']
        self.assertIsInstance(equity_fact['highlight_id'], int)
        self.assertEqual(
            {
                key: value
                for key, value in valuation_provenance.items()
                if key != 'total_equity_fact'
            },
            {
                'source': 'dart_annual_equity_market_cap_derived',
                'formula': 'same_day_market_cap / annual_total_equity',
                'market_cap': 2_000_000.0,
                'total_equity': 500_000.0,
                'currency': 'KRW',
                'business_year': 2025,
                'market_date': market_date.isoformat(),
                'market_cap_source': 'pykrx_valuation_snapshot',
                'total_equity_source': 'dart_annual_fundamentals',
                'fundamentals_snapshot_id': snapshot_id,
            },
        )
        self.assertEqual(
            {
                key: value
                for key, value in equity_fact.items()
                if key != 'highlight_id'
            },
            {
                'source': 'dart_fundamentals_highlight',
                'snapshot_id': snapshot_id,
                'metric_key': 'total_equity',
                'amount_field': 'current_amount',
                'currency': 'KRW',
            },
        )

    def test_load_inputs_keeps_invalid_derived_pbr_inputs_unavailable(
        self,
    ) -> None:
        market_date = date(2026, 4, 14)
        cases = (
            ('005930', 'USD', 500_000, 2_000_000),
            ('000660', 'KRW', None, 2_000_000),
            ('035420', 'KRW', 500_000, 0),
            ('035720', 'KRW', 0, 2_000_000),
        )
        for symbol, currency, total_equity, market_cap in cases:
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
                pbr=None,
                market_cap=market_cap,
            )
            self._add_fundamentals_snapshot(
                symbol=symbol,
                business_year=2025,
                receipt_date=date(2026, 4, 10),
                net_income=100,
                total_equity=total_equity,
                currency=currency,
            )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        by_symbol = {item.symbol: item for item in inputs}
        self.assertEqual(set(by_symbol), {case[0] for case in cases})
        for symbol, _, _, _ in cases:
            self.assertIsNone(by_symbol[symbol].pbr)
            self.assertEqual(by_symbol[symbol].valuation_source, 'unavailable')
            self.assertIsNone(by_symbol[symbol].valuation_provenance)

    def test_load_inputs_binds_derived_pbr_to_total_equity_fact_currency(self) -> None:
        market_date = date(2026, 4, 14)
        cases = (
            ('005930', 'KRW', 'USD'),
            ('000660', 'USD', 'KRW'),
        )
        snapshot_ids: dict[str, int] = {}
        for symbol, snapshot_currency, equity_currency in cases:
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
                pbr=None,
                market_cap=2_000_000,
            )
            snapshot_id, _, _ = self._add_fundamentals_snapshot(
                symbol=symbol,
                business_year=2025,
                receipt_date=date(2026, 4, 10),
                net_income=100,
                total_equity=500_000,
                currency=snapshot_currency,
                total_equity_currency=equity_currency,
            )
            snapshot_ids[symbol] = snapshot_id

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        by_symbol = {item.symbol: item for item in inputs}
        self.assertIsNone(by_symbol['005930'].pbr)
        self.assertEqual(by_symbol['005930'].valuation_source, 'unavailable')
        self.assertIsNone(by_symbol['005930'].valuation_provenance)

        derived = by_symbol['000660']
        self.assertEqual(derived.pbr, 4.0)
        self.assertEqual(
            derived.valuation_source,
            'dart_annual_equity_market_cap_derived',
        )
        equity_fact = derived.valuation_provenance['total_equity_fact']
        self.assertIsInstance(equity_fact['highlight_id'], int)
        self.assertEqual(
            {
                key: value
                for key, value in equity_fact.items()
                if key != 'highlight_id'
            },
            {
                'source': 'dart_fundamentals_highlight',
                'snapshot_id': snapshot_ids['000660'],
                'metric_key': 'total_equity',
                'amount_field': 'current_amount',
                'currency': 'KRW',
            },
        )

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
            payload={
                'evidence_coverage': {
                    'schema_version': 'dart_share_issuance_coverage_v1',
                    'status': 'complete',
                    'source': 'dart_disclosure_events',
                    'window_start': '2025-04-14',
                    'window_end': '2026-04-14',
                    'matched_event_count': 0,
                }
            },
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

    def test_load_inputs_derives_missing_ratio_inputs_from_persisted_statement_rows(self) -> None:
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
        current_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        self._add_statement_rows(
            prior_snapshot_id,
            current_assets=('BS', 'ifrs-full_CurrentAssets', 30),
            current_liabilities=('BS', 'ifrs-full_CurrentLiabilities', 20),
            gross_profit=('IS', 'ifrs-full_GrossProfit', 28),
            revenue=('IS', 'ifrs-full_Revenue', 70),
        )
        self._add_statement_rows(
            current_snapshot_id,
            current_assets=('BS', 'ifrs-full_CurrentAssets', 50),
            current_liabilities=('BS', 'ifrs-full_CurrentLiabilities', 25),
            gross_profit=('IS', 'ifrs-full_GrossProfit', 40),
            revenue=('IS', 'ifrs-full_Revenue', 80),
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        input_row = inputs[0]
        self.assertAlmostEqual(input_row.current_ratio, 2.0)
        self.assertAlmostEqual(input_row.prior_current_ratio, 1.5)
        self.assertAlmostEqual(input_row.gross_margin, 0.5)
        self.assertAlmostEqual(input_row.prior_gross_margin, 0.4)
        self.assertEqual(
            input_row.fundamentals_provenance[2025]['raw_row_fallback_metrics'],
            ['current_assets', 'current_liabilities', 'gross_profit', 'revenue'],
        )
        self.assertEqual(
            input_row.fundamentals_provenance[2024]['raw_row_fallback_metrics'],
            ['current_assets', 'current_liabilities', 'gross_profit', 'revenue'],
        )
        self.assertEqual(
            input_row.fundamentals_provenance[2025]['raw_row_derived_ratios'],
            ['current_ratio', 'gross_margin'],
        )
        self.assertEqual(
            input_row.fundamentals_provenance[2024]['raw_row_derived_ratios'],
            ['current_ratio', 'gross_margin'],
        )

    def test_load_inputs_recovers_prior_ratios_from_latest_annual_comparatives(
        self,
    ) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        current_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
            total_equity=60,
        )
        self._add_amount_highlights(
            current_snapshot_id,
            total_assets=100,
            total_liabilities=40,
        )
        self._add_comparative_statement_rows(
            current_snapshot_id,
            net_income=('IS', 'ifrs-full_ProfitLoss', 20, 10),
            total_assets=('BS', 'ifrs-full_Assets', 100, 100),
            total_equity=('BS', 'ifrs-full_Equity', 60, 50),
            total_liabilities=(
                'BS',
                'ifrs-full_Liabilities',
                40,
                50,
            ),
            current_assets=(
                'BS',
                'ifrs-full_CurrentAssets',
                50,
                30,
            ),
            current_liabilities=(
                'BS',
                'ifrs-full_CurrentLiabilities',
                25,
                20,
            ),
            gross_profit=('IS', 'ifrs-full_GrossProfit', 40, 28),
            revenue=('IS', 'ifrs-full_Revenue', 80, 70),
        )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        input_row = inputs[0]
        self.assertEqual(input_row.annual_year, 2025)
        self.assertEqual(input_row.prior_annual_year, 2024)
        self.assertAlmostEqual(input_row.current_ratio, 2.0)
        self.assertAlmostEqual(input_row.prior_current_ratio, 1.5)
        self.assertAlmostEqual(input_row.gross_margin, 0.5)
        self.assertAlmostEqual(input_row.prior_gross_margin, 0.4)
        self.assertAlmostEqual(input_row.prior_debt_ratio, 1.0)
        self.assertAlmostEqual(input_row.prior_asset_turnover, 0.7)
        self.assertEqual(input_row.prior_net_income, 10.0)
        self.assertEqual(input_row.prior_total_assets, 100.0)

        prior_provenance = input_row.fundamentals_provenance[2024]
        self.assertEqual(
            prior_provenance['source'],
            'dart_latest_annual_filing_comparative',
        )
        self.assertEqual(
            prior_provenance['snapshot_id'],
            current_snapshot_id,
        )
        self.assertEqual(prior_provenance['source_business_year'], 2025)
        current_assets_provenance = prior_provenance[
            'comparative_prior_facts'
        ]['current_assets']
        self.assertEqual(
            current_assets_provenance['amount_field'],
            'prior_amount',
        )
        self.assertIsNotNone(current_assets_provenance['row_id'])
        gross_profit_provenance = prior_provenance[
            'comparative_prior_facts'
        ]['gross_profit']
        self.assertEqual(
            gross_profit_provenance['amount_field'],
            'prior_cumulative_amount',
        )
        self.assertIsNotNone(gross_profit_provenance['row_id'])
        self.assertAlmostEqual(
            prior_provenance['comparative_prior_derived_ratios'][
                'current_ratio'
            ],
            1.5,
        )
        self.assertAlmostEqual(
            prior_provenance['comparative_prior_derived_ratios'][
                'gross_margin'
            ],
            0.4,
        )

    def test_latest_annual_comparative_overrides_conflicting_prior_filing(
        self,
    ) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        prior_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2024,
            receipt_date=date(2025, 3, 20),
            net_income=8,
        )
        self._add_amount_highlights(
            prior_snapshot_id,
            cumulative_metrics={'gross_profit', 'revenue'},
            current_assets=300,
            current_liabilities=100,
            gross_profit=60,
            revenue=100,
        )
        self._add_ratio_highlights(
            prior_snapshot_id,
            current_ratio=3.0,
            gross_margin=0.6,
        )
        current_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        self._add_comparative_statement_rows(
            current_snapshot_id,
            current_assets=(
                'BS',
                'ifrs-full_CurrentAssets',
                50,
                30,
            ),
            current_liabilities=(
                'BS',
                'ifrs-full_CurrentLiabilities',
                25,
                20,
            ),
            gross_profit=('IS', 'ifrs-full_GrossProfit', 40, 28),
            revenue=('IS', 'ifrs-full_Revenue', 80, 70),
        )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        input_row = inputs[0]
        self.assertAlmostEqual(input_row.prior_current_ratio, 1.5)
        self.assertAlmostEqual(input_row.prior_gross_margin, 0.4)
        prior_provenance = input_row.fundamentals_provenance[2024]
        self.assertEqual(
            prior_provenance['comparative_value_precedence'],
            'latest_selected_annual_filing_comparative_'
            'over_prior_filing_current',
        )
        self.assertEqual(
            prior_provenance['superseded_prior_filing_provenance'][
                'snapshot_id'
            ],
            prior_snapshot_id,
        )
        self.assertEqual(
            prior_provenance['snapshot_id'],
            current_snapshot_id,
        )
        self.assertEqual(
            prior_provenance['comparative_prior_facts'][
                'current_assets'
            ]['superseded_prior_filing_value'],
            300.0,
        )

    def test_partial_latest_comparative_invalidates_prior_stored_ratio(
        self,
    ) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        prior_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2024,
            receipt_date=date(2025, 3, 20),
            net_income=8,
        )
        self._add_amount_highlights(
            prior_snapshot_id,
            current_assets=300,
            current_liabilities=100,
        )
        self._add_ratio_highlights(
            prior_snapshot_id,
            current_ratio=3.0,
        )
        current_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        self._add_comparative_statement_rows(
            current_snapshot_id,
            current_assets=(
                'BS',
                'ifrs-full_CurrentAssets',
                50,
                30,
            ),
        )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        input_row = inputs[0]
        self.assertIsNone(input_row.prior_current_ratio)
        prior_provenance = input_row.fundamentals_provenance[2024]
        self.assertEqual(
            prior_provenance['comparative_prior_derived_ratios'],
            {},
        )
        self.assertEqual(
            prior_provenance['invalidated_derived_inputs'][
                'current_ratio'
            ],
            ['current_liabilities'],
        )
        self.assertEqual(
            prior_provenance['comparative_prior_facts'][
                'current_liabilities'
            ]['reason'],
            'latest_comparative_derived_input_incomplete',
        )

    def test_zero_denominator_latest_comparative_invalidates_prior_ratio(
        self,
    ) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        prior_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2024,
            receipt_date=date(2025, 3, 20),
            net_income=8,
        )
        self._add_amount_highlights(
            prior_snapshot_id,
            current_assets=300,
            current_liabilities=100,
        )
        self._add_ratio_highlights(
            prior_snapshot_id,
            current_ratio=3.0,
        )
        current_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        self._add_comparative_statement_rows(
            current_snapshot_id,
            current_assets=(
                'BS',
                'ifrs-full_CurrentAssets',
                50,
                30,
            ),
            current_liabilities=(
                'BS',
                'ifrs-full_CurrentLiabilities',
                25,
                0,
            ),
        )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        input_row = inputs[0]
        self.assertIsNone(input_row.prior_current_ratio)
        prior_provenance = input_row.fundamentals_provenance[2024]
        self.assertEqual(
            prior_provenance['comparative_prior_derived_ratios'],
            {},
        )
        self.assertEqual(
            prior_provenance['invalidated_derived_inputs'][
                'current_ratio'
            ],
            ['current_liabilities'],
        )

    def test_load_inputs_recovers_asset_turnover_when_other_ratios_are_stored(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        self._add_amount_highlights(snapshot_id, total_assets=100)
        self._add_ratio_highlights(
            snapshot_id,
            current_ratio=2.0,
            gross_margin=0.5,
        )
        self._add_statement_rows(
            snapshot_id,
            revenue=('IS', 'ifrs-full_Revenue', 80),
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertAlmostEqual(inputs[0].asset_turnover, 0.8)
        self.assertEqual(
            inputs[0].fundamentals_provenance[2025]['raw_row_fallback_metrics'],
            ['revenue'],
        )

    def test_load_inputs_recovers_total_assets_even_when_asset_turnover_ratio_is_stored(self) -> None:
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
        current_snapshot_id, _, _ = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        for snapshot_id, asset_turnover in (
            (prior_snapshot_id, 0.7),
            (current_snapshot_id, 0.8),
        ):
            self._add_ratio_highlights(
                snapshot_id,
                current_ratio=2.0,
                gross_margin=0.5,
                asset_turnover=asset_turnover,
            )
            self._add_statement_rows(
                snapshot_id,
                total_assets=('BS', 'ifrs-full_Assets', 100),
            )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].total_assets, 100.0)
        self.assertEqual(inputs[0].prior_total_assets, 100.0)
        self.assertAlmostEqual(inputs[0].asset_turnover, 0.8)
        self.assertAlmostEqual(inputs[0].prior_asset_turnover, 0.7)
        self.assertEqual(
            inputs[0].fundamentals_provenance[2025][
                'raw_row_fallback_metrics'
            ],
            ['total_assets'],
        )
        signals, _, _ = _compute_signals(inputs[0])
        self.assertTrue(signals['positive_roa'].available)
        self.assertTrue(signals['improving_roa'].available)

    def test_load_inputs_uses_audit_signal_for_selected_annual_filing_only(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        _, selected_event_id, selected_rcept_no = (
            self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 4, 10),
            net_income=20,
            rcept_no='20260410000002',
            )
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
            disclosure_event_id=selected_event_id,
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
            receipt_date=date(2025, 4, 13),
            has_dilution_signal=True,
        )
        expected_dilution_id, expected_dilution_rcept_no = self._add_dilution_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 10),
            has_dilution_signal=False,
            payload={
                'evidence_coverage': {
                    'schema_version': 'dart_share_issuance_coverage_v1',
                    'status': 'complete',
                    'source': 'dart_disclosure_events',
                    'window_start': '2025-04-14',
                    'window_end': '2026-04-14',
                    'matched_event_count': 0,
                }
            },
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

    def test_load_inputs_ignores_negative_dilution_row_without_complete_coverage(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        self._add_dilution_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 10),
            has_dilution_signal=False,
            payload={'matched_event_count': 0},
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertIsNone(inputs[0].has_dilution_signal)
        self.assertIsNone(inputs[0].dilution_provenance)

    def test_load_inputs_rejects_legacy_negative_coverage_without_authority_fields(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        self._add_dilution_signal(
            symbol=symbol,
            receipt_date=market_date,
            has_dilution_signal=False,
            normalize_negative_coverage=False,
            payload={
                'evidence_coverage': {
                    'schema_version': 'dart_share_issuance_coverage_v1',
                    'status': 'complete',
                    'source': 'dart_disclosure_events',
                    'window_start': '2025-04-14',
                    'window_end': '2026-04-14',
                    'matched_event_count': 0,
                }
            },
        )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertIsNone(inputs[0].has_dilution_signal)
        self.assertIsNone(inputs[0].dilution_provenance)

    def test_load_inputs_rejects_negative_coverage_for_different_corporation(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        self._add_dilution_signal(
            symbol=symbol,
            receipt_date=market_date,
            has_dilution_signal=False,
            coverage_corp_code='COTHER',
            payload={
                'evidence_coverage': {
                    'schema_version': 'dart_share_issuance_coverage_v1',
                    'status': 'complete',
                    'source': 'dart_disclosure_events',
                    'window_start': '2025-04-14',
                    'window_end': '2026-04-14',
                    'matched_event_count': 0,
                }
            },
        )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertIsNone(inputs[0].has_dilution_signal)
        self.assertIsNone(inputs[0].dilution_provenance)

    def test_latest_incomplete_dilution_attempt_invalidates_older_complete_coverage(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        self._add_dilution_signal(
            symbol=symbol,
            receipt_date=date(2026, 4, 13),
            has_dilution_signal=False,
            payload={
                'evidence_coverage': {
                    'schema_version': 'dart_share_issuance_coverage_v1',
                    'status': 'complete',
                    'source': 'dart_disclosure_events',
                    'window_start': '2025-04-14',
                    'window_end': '2026-04-14',
                    'matched_event_count': 0,
                }
            },
        )
        self._add_dilution_signal(
            symbol=symbol,
            receipt_date=market_date,
            has_dilution_signal=False,
            payload={
                'evidence_coverage': {
                    'schema_version': 'dart_share_issuance_coverage_v1',
                    'status': 'incomplete',
                    'source': 'dart_disclosure_events',
                    'window_start': '2026-04-14',
                    'window_end': '2026-04-14',
                    'matched_event_count': 0,
                }
            },
        )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertIsNone(inputs[0].has_dilution_signal)
        self.assertIsNone(inputs[0].dilution_provenance)

    def test_load_inputs_ignores_positive_dilution_event_outside_lookback_window(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        self._add_dilution_signal(
            symbol=symbol,
            receipt_date=date(2025, 4, 13),
            has_dilution_signal=True,
        )

        with patch('app.signals.piotroski_filter.session_scope', self._session_scope):
            inputs = self.service._load_inputs(market_date)

        self.assertEqual(len(inputs), 1)
        self.assertIsNone(inputs[0].has_dilution_signal)
        self.assertIsNone(inputs[0].dilution_provenance)

    def test_load_inputs_recognizes_all_positive_share_issuance_signal_types(self) -> None:
        market_date = date(2026, 4, 14)
        signal_types = (
            'paid_increase',
            'convertible_bond',
            'bond_with_warrant',
            'exchange_bond',
        )
        symbols = ['005930', '000660', '035420', '051910']
        for symbol, signal_type in zip(symbols, signal_types, strict=True):
            symbol_id = self._add_symbol(symbol=symbol, is_active=True)
            self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
            self._add_fundamentals_snapshot(
                symbol=symbol,
                business_year=2025,
                receipt_date=date(2026, 4, 10),
                net_income=20,
            )
            self._add_dilution_signal(
                symbol=symbol,
                receipt_date=date(2026, 4, 10),
                has_dilution_signal=True,
                signal_type=signal_type,
            )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        inputs_by_symbol = {item.symbol: item for item in inputs}
        self.assertEqual(set(inputs_by_symbol), set(symbols))
        self.assertTrue(
            all(
                inputs_by_symbol[symbol].has_dilution_signal is True
                for symbol in symbols
            )
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

    def test_load_inputs_keeps_authoritative_absence_unknown_with_provenance(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        _, event_id, rcept_no = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        sentinel = build_authoritative_absence_audit_signal(
            {
                'id': event_id,
                'rcept_no': rcept_no,
                'receipt_date': '2026-03-20',
            },
            _complete_negative_audit_diagnostics(),
        )
        with Session(self.engine) as session:
            session.add(
                DartAuditSignal(
                    disclosure_event_id=event_id,
                    rcept_no=rcept_no,
                    corp_code=f'C{symbol}',
                    stock_code=symbol,
                    corp_name=symbol,
                    receipt_date=date(2026, 3, 20),
                    opinion_code=None,
                    opinion_label=None,
                    has_audit_issue=False,
                    payload=sentinel.payload,
                )
            )
            session.commit()

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        self.assertIsNone(inputs[0].has_audit_issue)
        self.assertEqual(
            inputs[0].audit_provenance['evidence_status'],
            'authoritative_absence',
        )

    def test_load_inputs_rejects_authoritative_absence_identity_drift(self) -> None:
        market_date = date(2026, 4, 14)
        symbol = '005930'
        symbol_id = self._add_symbol(symbol=symbol, is_active=True)
        self._add_price_bar(symbol_id=symbol_id, trade_date=market_date)
        _, event_id, rcept_no = self._add_fundamentals_snapshot(
            symbol=symbol,
            business_year=2025,
            receipt_date=date(2026, 3, 20),
            net_income=20,
        )
        sentinel = build_authoritative_absence_audit_signal(
            {
                'id': event_id,
                'rcept_no': rcept_no,
                'receipt_date': '2026-03-20',
            },
            _complete_negative_audit_diagnostics(),
        )
        sentinel.payload['receipt_date'] = '2026-03-21'
        with Session(self.engine) as session:
            session.add(
                DartAuditSignal(
                    disclosure_event_id=event_id,
                    rcept_no=rcept_no,
                    corp_code=f'C{symbol}',
                    stock_code=symbol,
                    corp_name=symbol,
                    receipt_date=date(2026, 3, 21),
                    opinion_code=None,
                    opinion_label=None,
                    has_audit_issue=False,
                    payload=sentinel.payload,
                )
            )
            session.commit()

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        self.assertIsNone(inputs[0].has_audit_issue)
        self.assertEqual(
            inputs[0].audit_provenance['evidence_status'],
            'invalid',
        )

    def test_load_inputs_rejects_sentinel_downgrade_and_opinion_drift(
        self,
    ) -> None:
        market_date = date(2026, 4, 14)
        sentinel_symbol = '005930'
        parsed_symbol = '000660'
        for symbol in (sentinel_symbol, parsed_symbol):
            symbol_id = self._add_symbol(symbol=symbol, is_active=True)
            self._add_price_bar(
                symbol_id=symbol_id,
                trade_date=market_date,
            )

        _, sentinel_event_id, sentinel_rcept_no = (
            self._add_fundamentals_snapshot(
                symbol=sentinel_symbol,
                business_year=2025,
                receipt_date=date(2026, 3, 20),
                net_income=20,
            )
        )
        sentinel = build_authoritative_absence_audit_signal(
            {
                'id': sentinel_event_id,
                'rcept_no': sentinel_rcept_no,
                'receipt_date': '2026-03-20',
            },
            _complete_negative_audit_diagnostics(),
        )
        corrupted_payload = dict(sentinel.payload)
        corrupted_payload['schema_version'] = 'tampered'
        corrupted_payload['evidence_semantics'] = 'tampered'
        self._add_audit_signal(
            symbol=sentinel_symbol,
            receipt_date=date(2026, 3, 20),
            has_audit_issue=False,
            rcept_no=sentinel_rcept_no,
            disclosure_event_id=sentinel_event_id,
            opinion_code='unqualified',
            payload=corrupted_payload,
        )

        _, parsed_event_id, parsed_rcept_no = (
            self._add_fundamentals_snapshot(
                symbol=parsed_symbol,
                business_year=2025,
                receipt_date=date(2026, 3, 20),
                net_income=20,
            )
        )
        self._add_audit_signal(
            symbol=parsed_symbol,
            receipt_date=date(2026, 3, 20),
            has_audit_issue=False,
            rcept_no=parsed_rcept_no,
            disclosure_event_id=parsed_event_id,
            opinion_code='qualified',
            payload={'parser': 'semantic_audit_table_v1'},
        )

        with patch(
            'app.signals.piotroski_filter.session_scope',
            self._session_scope,
        ):
            inputs = self.service._load_inputs(market_date)

        by_symbol = {item.symbol: item for item in inputs}
        for symbol in (sentinel_symbol, parsed_symbol):
            with self.subTest(symbol=symbol):
                self.assertIsNone(by_symbol[symbol].has_audit_issue)
                self.assertEqual(
                    by_symbol[symbol].audit_provenance[
                        'evidence_status'
                    ],
                    'invalid',
                )

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
