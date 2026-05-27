import unittest
from copy import deepcopy
from unittest.mock import patch

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


if __name__ == '__main__':
    unittest.main()
