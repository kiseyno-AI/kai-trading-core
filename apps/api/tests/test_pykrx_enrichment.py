import unittest
from copy import deepcopy
from datetime import date
from unittest.mock import AsyncMock, patch

import pandas as pd

from app.eod.pykrx_enrichment import PykrxEnrichmentService


def _filter_rows(
    rows,
    *,
    market_date,
    categories=None,
    markets=None,
    symbols=None,
):
    filtered = [row for row in rows if row['market_date'] == market_date]
    if categories:
        filtered = [row for row in filtered if row['category'] in categories]
    if markets:
        filtered = [row for row in filtered if row['market'] in markets]
    if symbols:
        normalized_symbols = set(symbols)
        filtered = [row for row in filtered if row.get('symbol') in normalized_symbols]
    return [deepcopy(row) for row in filtered]


class PykrxEnrichmentServiceTests(unittest.TestCase):
    @patch('app.eod.pykrx_enrichment.load_daily_market_relative_contexts')
    @patch('app.eod.pykrx_enrichment.load_symbols')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_market_sector_classifications')
    def test_collect_for_market_date_normalizes_and_persists_selected_categories(
        self,
        get_market_sector_classifications_mock,
        get_shorting_volume_by_ticker_mock,
        get_shorting_value_by_ticker_mock,
        get_shorting_balance_by_ticker_mock,
        load_symbols_mock,
        load_daily_market_relative_contexts_mock,
    ):
        persisted_rows: list[dict] = []
        upsert_calls: list[list[dict]] = []

        load_symbols_mock.return_value = [
            {
                'symbol': '005930',
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'standard_code': 'KR7005930003',
                'instrument_type': 'equity',
            },
            {
                'symbol': '000660',
                'name_ko': 'SK하이닉스',
                'market': 'KOSPI',
                'standard_code': 'KR7000660001',
                'instrument_type': 'equity',
            },
        ]
        load_daily_market_relative_contexts_mock.return_value = [
            {
                'symbol': '005930',
                'market': 'KOSPI',
                'market_date': '2026-04-21',
                'status': 'available',
                'breadth': {
                    'market_symbol_count': 2,
                    'up_count': 1,
                    'down_count': 1,
                    'flat_count': 0,
                    'missing_previous_close_count': 0,
                },
                'relative_strength': {'return_percentile': 0.8},
            }
        ]
        get_market_sector_classifications_mock.return_value = pd.DataFrame(
            {
                '종목명': ['삼성전자', 'SK하이닉스'],
                '업종명': ['반도체', '반도체'],
                '종가': [71000, 192000],
                '대비': [800, 1200],
                '등락률': [1.14, 0.63],
                '시가총액': [420000000000, 180000000000],
            },
            index=['005930', '000660'],
        )
        get_shorting_volume_by_ticker_mock.return_value = pd.DataFrame(
            {
                '공매도': [1000, 500],
                '매수': [20000, 10000],
                '비중': [5.0, 5.0],
            },
            index=['005930', '000660'],
        )
        get_shorting_value_by_ticker_mock.return_value = pd.DataFrame(
            {
                '공매도': [70000000, 40000000],
                '매수': [1400000000, 800000000],
                '비중': [5.0, 5.0],
            },
            index=['005930', '000660'],
        )
        get_shorting_balance_by_ticker_mock.return_value = pd.DataFrame(
            {
                '공매도잔고': [120000, 80000],
                '상장주식수': [5969782550, 728002365],
                '공매도금액': [8400000000, 6400000000],
                '시가총액': [420000000000, 180000000000],
                '비중': [0.45, 0.55],
            },
            index=['005930', '000660'],
        )

        def upsert_side_effect(*, rows):
            upsert_calls.append([deepcopy(row) for row in rows])
            for row in rows:
                key = (row['market_date'], row['category'], row['scope_type'], row['scope_key'], row['source'])
                persisted_rows[:] = [
                    existing
                    for existing in persisted_rows
                    if (
                        existing['market_date'],
                        existing['category'],
                        existing['scope_type'],
                        existing['scope_key'],
                        existing['source'],
                    )
                    != key
                ]
                persisted_rows.append(
                    {
                        **deepcopy(row),
                        'captured_at': '2026-04-21T15:10:00+09:00',
                    }
                )
            return len(rows)

        def load_rows_side_effect(*, market_date, categories=None, markets=None, symbols=None, **kwargs):
            return _filter_rows(
                persisted_rows,
                market_date=market_date,
                categories=categories,
                markets=markets,
                symbols=symbols,
            )

        with (
            patch('app.eod.pykrx_enrichment.upsert_daily_pykrx_enrichment_rows', side_effect=upsert_side_effect),
            patch('app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows', side_effect=load_rows_side_effect),
        ):
            service = PykrxEnrichmentService()
            result = service.collect_for_market_date(
                market_date='2026-04-21',
                markets=['KOSPI'],
                symbols=['005930'],
                categories=['sector_classification', 'market_breadth', 'short_selling'],
            )
            context_inputs = service.load_symbol_day_context_inputs(
                market_date='2026-04-21',
                symbols=['005930'],
                markets=['KOSPI'],
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(len(upsert_calls), 3)
        self.assertEqual(result['categories']['sector_classification']['rows_persisted'], 1)
        self.assertEqual(result['categories']['market_breadth']['rows_persisted'], 1)
        self.assertEqual(result['categories']['short_selling']['rows_persisted'], 1)
        sector_row = next(row for row in persisted_rows if row['category'] == 'sector_classification')
        short_row = next(row for row in persisted_rows if row['category'] == 'short_selling')
        breadth_row = next(row for row in persisted_rows if row['category'] == 'market_breadth')
        self.assertEqual(sector_row['symbol'], '005930')
        self.assertEqual(sector_row['payload']['sector_name'], '반도체')
        self.assertEqual(short_row['payload']['short_volume']['short_volume'], 1000)
        self.assertEqual(short_row['payload']['short_volume']['total_volume'], 20000)
        self.assertEqual(short_row['payload']['short_volume']['short_value'], 70000000)
        self.assertEqual(short_row['payload']['short_volume']['total_value'], 1400000000)
        self.assertEqual(short_row['payload']['short_volume']['short_value_ratio_pct'], 5.0)
        self.assertEqual(short_row['payload']['short_balance']['short_balance_ratio_pct'], 0.45)
        self.assertEqual(breadth_row['payload']['advance_decline_ratio'], 1.0)
        self.assertEqual(
            context_inputs['by_symbol']['005930']['short_selling']['payload']['short_volume']['short_value'],
            70000000,
        )
        self.assertEqual(result['coverage']['categories']['sector_classification']['coverage_status'], 'complete')
        self.assertEqual(result['coverage']['categories']['market_breadth']['coverage_status'], 'complete')
        self.assertEqual(result['coverage']['categories']['short_selling']['present_rows'], 1)

    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    def test_collect_short_selling_rows_keeps_multiindex_value_fallback(
        self,
        get_shorting_volume_by_ticker_mock,
        get_shorting_value_by_ticker_mock,
        get_shorting_balance_by_ticker_mock,
    ):
        get_shorting_volume_by_ticker_mock.return_value = pd.DataFrame(
            {
                ('거래량', '공매도'): [1000],
                ('거래량', '매수'): [20000],
                ('거래량', '비중'): [5.0],
                ('거래대금', '공매도'): [70000000],
                ('거래대금', '매수'): [1400000000],
                ('거래대금', '비중'): [5.0],
            },
            index=['005930'],
        )
        get_shorting_value_by_ticker_mock.return_value = pd.DataFrame(index=['005930'])
        get_shorting_balance_by_ticker_mock.return_value = pd.DataFrame(
            {
                '공매도잔고': [120000],
                '상장주식수': [5969782550],
                '공매도금액': [8400000000],
                '시가총액': [420000000000],
                '비중': [0.45],
            },
            index=['005930'],
        )

        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        rows, details = service._collect_short_selling_rows(
            market_date_iso='2026-04-21',
            market_date_yyyymmdd='20260421',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={
                '005930': {
                    'symbol': '005930',
                    'name_ko': '삼성전자',
                    'market': 'KOSPI',
                    'standard_code': 'KR7005930003',
                }
            },
            cache={},
        )

        self.assertEqual(details['rows_prepared'], 1)
        self.assertEqual(rows[0]['payload']['short_volume']['short_value'], 70000000)
        self.assertEqual(rows[0]['payload']['short_volume']['total_value'], 1400000000)
        self.assertEqual(rows[0]['payload']['short_volume']['short_value_ratio_pct'], 5.0)

    @patch('app.eod.pykrx_enrichment.stock.get_market_fundamental_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_market_cap_by_ticker')
    def test_collect_valuation_snapshot_keeps_market_cap_rows_when_fundamentals_fail(
        self,
        get_market_cap_by_ticker_mock,
        get_market_fundamental_by_ticker_mock,
    ):
        get_market_cap_by_ticker_mock.return_value = pd.DataFrame(
            {
                '종가': [71000],
                '시가총액': [420000000000],
                '거래량': [1000000],
                '거래대금': [71000000000],
                '상장주식수': [5969782550],
            },
            index=['005930'],
        )
        get_market_fundamental_by_ticker_mock.side_effect = KeyError(
            "None of [Index(['BPS', 'PER', 'PBR', 'EPS', 'DIV', 'DPS'], dtype='object')] are in the [columns]"
        )

        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_count = 0
        rows, details = service._collect_valuation_snapshot_rows(
            market_date_iso='2026-06-09',
            market_date_yyyymmdd='20260609',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={
                '005930': {
                    'symbol': '005930',
                    'name_ko': '삼성전자',
                    'market': 'KOSPI',
                    'standard_code': 'KR7005930003',
                }
            },
            cache={},
        )

        self.assertEqual(details['rows_prepared'], 1)
        self.assertEqual(details['errors'][0]['operation'], 'valuation_fundamentals')
        self.assertEqual(rows[0]['symbol'], '005930')
        self.assertEqual(rows[0]['payload']['market_cap'], 420000000000)
        self.assertIsNone(rows[0]['payload']['per'])

    @patch('app.eod.pykrx_enrichment.load_symbols')
    def test_collect_for_market_date_falls_back_for_market_cap_only_valuation_rows(self, load_symbols_mock):
        load_symbols_mock.return_value = [
            {
                'symbol': '005930',
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'standard_code': 'KR7005930003',
                'instrument_type': 'equity',
            }
        ]
        market_cap_only_rows = [
            {
                'market_date': '2026-06-09',
                'market': 'KOSPI',
                'category': 'valuation_snapshot',
                'scope_type': 'symbol',
                'scope_key': '005930',
                'source': 'pykrx',
                'symbol': '005930',
                'name_ko': '삼성전자',
                'standard_code': 'KR7005930003',
                'payload': {
                    'market_cap': 420000000000,
                    'per': None,
                    'pbr': None,
                    'bps': None,
                    'eps': None,
                    'dividend_yield': None,
                    'dps': None,
                },
            }
        ]
        fallback_rows = [
            {
                'market_date': '2026-06-09',
                'market': 'KOSPI',
                'category': 'valuation_snapshot',
                'scope_type': 'symbol',
                'scope_key': '005930',
                'source': 'daily_price_bars_carry_forward',
                'symbol': '005930',
                'skip_symbol_upsert': True,
                'name_ko': '삼성전자',
                'standard_code': 'KR7005930003',
                'payload': {
                    'market_cap': None,
                    'per': 11.4,
                    'pbr': 1.2,
                    'dividend_yield': 2.1,
                    'carried_forward_from_market_date': '2026-06-08',
                },
            }
        ]
        upsert_calls: list[list[dict]] = []

        def upsert_side_effect(*, rows):
            upsert_calls.append([deepcopy(row) for row in rows])
            return len(rows)

        with (
            patch.object(
                PykrxEnrichmentService,
                '_collect_valuation_snapshot_rows',
                return_value=(
                    market_cap_only_rows,
                    {
                        'category': 'valuation_snapshot',
                        'scope_type': 'symbol',
                        'rows_prepared': 1,
                        'errors': [{'market': 'KOSPI', 'operation': 'valuation_fundamentals', 'error': 'missing'}],
                    },
                ),
            ),
            patch.object(
                PykrxEnrichmentService,
                '_collect_fallback_rows',
                return_value=(
                    fallback_rows,
                    {
                        'category': 'valuation_snapshot',
                        'scope_type': 'symbol',
                        'fallback_source': 'daily_price_bars',
                        'source_market_date': '2026-06-08',
                        'rows_prepared': 1,
                    },
                ),
            ) as fallback_mock,
            patch.object(
                PykrxEnrichmentService,
                '_build_coverage_summary',
                return_value={
                    'requested_symbol_count': 1,
                    'requested_market_count': 1,
                    'categories': {
                        'valuation_snapshot': {
                            'coverage_status': 'complete',
                            'present_rows': 1,
                            'expected_rows': 1,
                        }
                    },
                },
            ),
            patch('app.eod.pykrx_enrichment.upsert_daily_pykrx_enrichment_rows', side_effect=upsert_side_effect),
        ):
            result = PykrxEnrichmentService().collect_for_market_date(
                market_date='2026-06-09',
                markets=['KOSPI'],
                symbols=['005930'],
                categories=['valuation_snapshot'],
            )

        fallback_mock.assert_called_once()
        self.assertEqual(len(upsert_calls), 2)
        self.assertEqual(upsert_calls[0][0]['payload']['market_cap'], 420000000000)
        self.assertEqual(upsert_calls[1][0]['payload']['per'], 11.4)
        self.assertEqual(result['status'], 'recovered')
        self.assertEqual(result['categories']['valuation_snapshot']['fallback']['rows_persisted'], 1)

    @patch('app.eod.pykrx_enrichment.load_symbols')
    def test_point_in_time_sector_failure_never_uses_current_symbol_sector(
        self,
        load_symbols_mock,
    ):
        load_symbols_mock.return_value = [
            {
                'symbol': '099999',
                'name_ko': 'Current name',
                'market': 'KOSPI',
                'standard_code': 'CURRENT-CODE',
                'sector_name': 'Current sector',
            }
        ]
        current_sector_rows = [
            {
                'market_date': '2026-09-11',
                'market': 'KOSPI',
                'category': 'sector_classification',
                'scope_type': 'symbol',
                'scope_key': '099999',
                'source': 'daily_price_bars_reconstructed',
                'symbol': '099999',
                'payload': {'sector_name': 'Current sector'},
            }
        ]
        persisted_rows: list[dict] = []

        def upsert_side_effect(*, rows):
            persisted_rows.extend(deepcopy(rows))
            return len(rows)

        with (
            patch.object(
                PykrxEnrichmentService,
                '_collect_sector_classification_rows',
                return_value=(
                    [],
                    {
                        'category': 'sector_classification',
                        'scope_type': 'symbol',
                        'rows_prepared': 0,
                        'errors': [{'market': 'KOSPI', 'error': 'provider down'}],
                    },
                ),
            ),
            patch.object(
                PykrxEnrichmentService,
                '_collect_sector_rows_from_symbols',
                return_value=(
                    current_sector_rows,
                    {
                        'category': 'sector_classification',
                        'scope_type': 'symbol',
                        'fallback_source': 'daily_price_bars',
                        'rows_prepared': 1,
                    },
                ),
            ) as current_sector_fallback,
            patch.object(
                PykrxEnrichmentService,
                '_collect_rows_from_latest_enrichment',
                return_value=(
                    [],
                    {
                        'category': 'sector_classification',
                        'scope_type': 'symbol',
                        'fallback_source': 'latest_pykrx_enrichment',
                        'rows_prepared': 0,
                        'reason': 'no_prior_enrichment_rows',
                    },
                ),
            ) as historical_sector_fallback,
            patch.object(
                PykrxEnrichmentService,
                '_build_coverage_summary',
                return_value={
                    'requested_symbol_count': 1,
                    'requested_market_count': 1,
                    'categories': {},
                },
            ),
            patch(
                'app.eod.pykrx_enrichment.upsert_daily_pykrx_enrichment_rows',
                side_effect=upsert_side_effect,
            ),
            patch(
                'app.eod.pykrx_enrichment._current_krx_calendar_date',
                return_value=date(2026, 9, 14),
            ),
        ):
            result = PykrxEnrichmentService().collect_for_market_date(
                market_date='2026-09-11',
                markets=['KOSPI'],
                symbols=['099999'],
                categories=['sector_classification'],
            )

        current_sector_fallback.assert_not_called()
        historical_sector_fallback.assert_called_once()
        self.assertTrue(result['point_in_time_recovery'])
        self.assertEqual(result['status'], 'partial')
        self.assertFalse(
            any(
                row.get('payload', {}).get('sector_name') == 'Current sector'
                for row in persisted_rows
            )
        )

    @patch('app.eod.pykrx_enrichment.load_symbols')
    def test_inspect_reports_partial_coverage_for_missing_categories(self, load_symbols_mock):
        load_symbols_mock.return_value = [
            {
                'symbol': '005930',
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'standard_code': 'KR7005930003',
                'instrument_type': 'equity',
            },
            {
                'symbol': '000660',
                'name_ko': 'SK하이닉스',
                'market': 'KOSPI',
                'standard_code': 'KR7000660001',
                'instrument_type': 'equity',
            },
        ]
        persisted_rows = [
            {
                'market_date': '2026-04-21',
                'market': 'KOSPI',
                'category': 'sector_classification',
                'scope_type': 'symbol',
                'scope_key': '005930',
                'source': 'pykrx',
                'symbol': '005930',
                'name_ko': '삼성전자',
                'payload': {'sector_name': '반도체'},
                'captured_at': '2026-04-21T15:10:00+09:00',
            }
        ]

        with patch(
            'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
            side_effect=lambda *, market_date, categories=None, markets=None, symbols=None, **kwargs: _filter_rows(
                persisted_rows,
                market_date=market_date,
                categories=categories,
                markets=markets,
                symbols=symbols,
            ),
        ):
            service = PykrxEnrichmentService()
            result = service.inspect(
                market_date='2026-04-21',
                markets=['KOSPI'],
                categories=['sector_classification', 'investor_flow_symbol'],
                limit=10,
            )

        self.assertEqual(result['summary']['rows_returned'], 1)
        self.assertEqual(result['coverage']['categories']['sector_classification']['coverage_status'], 'partial')
        self.assertEqual(result['coverage']['categories']['investor_flow_symbol']['coverage_status'], 'missing')
        self.assertEqual(result['coverage']['categories']['investor_flow_symbol']['expected_rows'], 2)

    @patch('app.eod.pykrx_enrichment.load_symbols')
    def test_sector_classification_coverage_excludes_unsupported_konex_symbols(self, load_symbols_mock):
        load_symbols_mock.return_value = [
            {
                'symbol': '005930',
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'standard_code': 'KR7005930003',
                'instrument_type': 'equity',
            },
            {
                'symbol': '035720',
                'name_ko': '카카오',
                'market': 'KOSDAQ',
                'standard_code': 'KR7035720002',
                'instrument_type': 'equity',
            },
            {
                'symbol': '900001',
                'name_ko': '코넥스테스트',
                'market': 'KONEX',
                'standard_code': 'KR7900001001',
                'instrument_type': 'equity',
            },
        ]
        persisted_rows = [
            {
                'market_date': '2026-04-21',
                'market': 'KOSPI',
                'category': 'sector_classification',
                'scope_type': 'symbol',
                'scope_key': '005930',
                'source': 'pykrx',
                'symbol': '005930',
                'name_ko': '삼성전자',
                'payload': {'sector_name': '반도체'},
                'captured_at': '2026-04-21T15:10:00+09:00',
            },
            {
                'market_date': '2026-04-21',
                'market': 'KOSDAQ',
                'category': 'sector_classification',
                'scope_type': 'symbol',
                'scope_key': '035720',
                'source': 'pykrx',
                'symbol': '035720',
                'name_ko': '카카오',
                'payload': {'sector_name': '서비스업'},
                'captured_at': '2026-04-21T15:10:00+09:00',
            },
        ]

        with patch(
            'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
            side_effect=lambda *, market_date, categories=None, markets=None, symbols=None, **kwargs: _filter_rows(
                persisted_rows,
                market_date=market_date,
                categories=categories,
                markets=markets,
                symbols=symbols,
            ),
        ):
            result = PykrxEnrichmentService().inspect(
                market_date='2026-04-21',
                markets=['KOSPI', 'KOSDAQ', 'KONEX'],
                categories=['sector_classification'],
                limit=10,
            )

        coverage = result['coverage']['categories']['sector_classification']
        self.assertEqual(result['coverage']['requested_symbol_count'], 3)
        self.assertEqual(coverage['expected_rows'], 2)
        self.assertEqual(coverage['present_rows'], 2)
        self.assertEqual(coverage['coverage_ratio'], 1.0)
        self.assertEqual(coverage['coverage_status'], 'complete')

    def test_inspect_uses_latest_eod_run_universe_for_unspecified_symbol_coverage(self):
        persisted_rows = [
            {
                'market_date': '2026-04-21',
                'market': 'KOSPI',
                'category': 'short_selling',
                'scope_type': 'symbol',
                'scope_key': '005930',
                'source': 'pykrx',
                'symbol': '005930',
                'payload': {},
                'captured_at': '2026-04-21T15:10:00+09:00',
            },
            {
                'market_date': '2026-04-21',
                'market': 'KOSPI',
                'category': 'short_selling',
                'scope_type': 'symbol',
                'scope_key': '000660',
                'source': 'pykrx',
                'symbol': '000660',
                'payload': {},
                'captured_at': '2026-04-21T15:10:00+09:00',
            },
        ]
        symbol_rows = [
            {'symbol': '005930', 'name_ko': '삼성전자', 'market': 'KOSPI'},
            {'symbol': '000660', 'name_ko': 'SK하이닉스', 'market': 'KOSPI'},
        ]

        with (
            patch('app.eod.pykrx_enrichment.load_latest_eod_collection_run', return_value={'id': 42}),
            patch('app.eod.pykrx_enrichment.load_eod_run_persisted_symbols', return_value=['005930', '000660']),
            patch('app.eod.pykrx_enrichment.load_symbols', return_value=symbol_rows) as load_symbols_mock,
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                side_effect=lambda *, market_date, categories=None, markets=None, symbols=None, **kwargs: _filter_rows(
                    persisted_rows,
                    market_date=market_date,
                    categories=categories,
                    markets=markets,
                    symbols=symbols,
                ),
            ),
        ):
            result = PykrxEnrichmentService().inspect(
                market_date='2026-04-21',
                markets=['KOSPI', 'KOSDAQ', 'KONEX'],
                categories=['short_selling'],
                limit=10,
            )

        load_symbols_mock.assert_called_once_with(
            markets=['KOSPI', 'KOSDAQ', 'KONEX'],
            symbols=['005930', '000660'],
            limit=2,
        )
        coverage = result['coverage']['categories']['short_selling']
        self.assertEqual(result['coverage']['requested_symbol_count'], 2)
        self.assertEqual(coverage['expected_rows'], 2)
        self.assertEqual(coverage['present_rows'], 2)
        self.assertEqual(coverage['coverage_ratio'], 1.0)


class KisCandidateFallbackTests(unittest.TestCase):
    @patch('app.eod.pykrx_enrichment.kis_real_client.fetch_domestic_investor_trade_daily', new_callable=AsyncMock)
    def test_candidate_fallback_maps_kis_investor_flow(self, fetch_mock):
        fetch_mock.return_value = {
            'output2': [{
                'stck_bsop_date': '20260819',
                'prsn_seln_vol': '10', 'prsn_shnu_vol': '20', 'prsn_ntby_qty': '10',
                'prsn_seln_tr_pbmn': '100', 'prsn_shnu_tr_pbmn': '200', 'prsn_ntby_tr_pbmn': '100',
                'frgn_seln_vol': '30', 'frgn_shnu_vol': '40', 'frgn_ntby_qty': '10',
                'frgn_seln_tr_pbmn': '300', 'frgn_shnu_tr_pbmn': '400', 'frgn_ntby_tr_pbmn': '100',
                'orgn_seln_vol': '50', 'orgn_shnu_vol': '60', 'orgn_ntby_qty': '10',
                'orgn_seln_tr_pbmn': '500', 'orgn_shnu_tr_pbmn': '600', 'orgn_ntby_tr_pbmn': '100',
            }],
        }
        rows, details = PykrxEnrichmentService()._collect_kis_candidate_fallback_rows(
            category='investor_flow_symbol', market_date_iso='2026-08-19',
            market_date_yyyymmdd='20260819', symbols=['004370'],
            symbol_meta={'004370': {'market': 'KOSPI', 'name_ko': '농심'}},
        )
        self.assertEqual(details['rows_prepared'], 1)
        self.assertEqual(rows[0]['source'], 'kis_open_api')
        self.assertEqual(rows[0]['payload']['investor_groups']['외국인']['net_volume'], 10)

    @patch('app.eod.pykrx_enrichment.kis_real_client.fetch_domestic_daily_short_sale', new_callable=AsyncMock)
    def test_candidate_fallback_keeps_unavailable_short_balance_explicit(self, fetch_mock):
        fetch_mock.return_value = {'output2': [{
            'stck_bsop_date': '20260819', 'ssts_cntg_qty': '123', 'stnd_vol_smtn': '1000',
            'ssts_vol_rlim': '12.3', 'ssts_tr_pbmn': '456000',
            'stnd_tr_pbmn_smtn': '5000000', 'ssts_tr_pbmn_rlim': '9.12',
        }]}
        rows, _ = PykrxEnrichmentService()._collect_kis_candidate_fallback_rows(
            category='short_selling', market_date_iso='2026-08-19',
            market_date_yyyymmdd='20260819', symbols=['004370'],
            symbol_meta={'004370': {'market': 'KOSPI', 'name_ko': '농심'}},
        )
        self.assertEqual(rows[0]['payload']['short_volume']['short_volume'], 123)
        self.assertEqual(rows[0]['payload']['missing_fields'], ['short_balance'])


class KisCandidateFallbackAsyncContextTests(unittest.IsolatedAsyncioTestCase):
    @patch('app.eod.pykrx_enrichment.kis_real_client.fetch_domestic_investor_trade_daily', new_callable=AsyncMock)
    async def test_candidate_fallback_is_safe_inside_running_event_loop(self, fetch_mock):
        fetch_mock.return_value = {'output2': [{'stck_bsop_date': '20260819'}]}

        rows, details = PykrxEnrichmentService()._collect_kis_candidate_fallback_rows(
            category='investor_flow_symbol', market_date_iso='2026-08-19',
            market_date_yyyymmdd='20260819', symbols=['004370'],
            symbol_meta={'004370': {'market': 'KOSPI', 'name_ko': '농심'}},
        )

        self.assertEqual(details['rows_prepared'], 1)
        self.assertEqual(rows[0]['source'], 'kis_open_api')


if __name__ == '__main__':
    unittest.main()
