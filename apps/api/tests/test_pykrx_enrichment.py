import os
import hashlib
import unittest
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

import pandas as pd

from app.eod.pykrx_enrichment import (
    PykrxEnrichmentService,
    _market_ticker_membership_frame,
    _resolve_short_balance_as_of_date,
)
from app.eod.pykrx_enrichment_gate import build_pykrx_enrichment_preflight
from app.market.session import (
    previous_krx_trading_date as actual_previous_krx_trading_date,
)


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


def _complete_investor_groups() -> dict:
    metrics = {
        'sell_volume': 1,
        'buy_volume': 2,
        'net_volume': 1,
        'sell_value': 10,
        'buy_value': 20,
        'net_value': 10,
    }
    return {
        group: dict(metrics)
        for group in ('개인', '외국인', '기관합계')
    }


def _complete_market_investor_groups() -> list[dict]:
    return [
        {'investor_type': group, **metrics}
        for group, metrics in _complete_investor_groups().items()
    ]


def _universe_reference_payload(symbols: list[str]) -> dict:
    normalized = sorted(set(symbols))
    return {
        'universe_reference_source': 'pykrx_market_ticker_list',
        'universe_reference_count': len(normalized),
        'universe_reference_sha256': hashlib.sha256(
            '\n'.join(normalized).encode('utf-8')
        ).hexdigest(),
    }


def _market_membership_payload(
    symbols_by_market: dict[str, list[str]],
) -> dict:
    all_symbols = [
        symbol
        for market_symbols in symbols_by_market.values()
        for symbol in market_symbols
    ]
    return {
        'schema_version': 'pykrx_market_membership_v1',
        'membership_basis': 'end_date_snapshot',
        'symbol_count': len(all_symbols),
        'markets': {
            market: {
                'symbols': sorted(market_symbols),
                'symbol_count': len(market_symbols),
                'membership_sha256': hashlib.sha256(
                    '\n'.join(sorted(market_symbols)).encode('utf-8')
                ).hexdigest(),
            }
            for market, market_symbols in symbols_by_market.items()
        },
    }


def _exact_membership_run(
    *,
    run_id: int,
    market_date: str,
    symbols_by_market: dict[str, list[str]],
    limited_run: bool = False,
    symbol: str | None = None,
) -> dict:
    markets = list(symbols_by_market)
    requested_symbols = [
        symbol_code
        for market_symbols in symbols_by_market.values()
        for symbol_code in market_symbols
    ]
    return {
        'id': run_id,
        'market': 'KRX',
        'trade_date': market_date,
        'source': 'pykrx',
        'status': 'completed',
        'symbols_total': len(requested_symbols),
        'run_metadata': {
            'mode': 'daily_auto',
            'markets': markets,
            'symbol': symbol,
            'limited_run': limited_run,
            'requested_symbols': requested_symbols,
            'universe_snapshot': {
                'source': 'pykrx',
                'markets': markets,
                'symbol_count': len(requested_symbols),
                'persisted_count': len(requested_symbols),
                'latest_trade_date': market_date,
                'membership_basis': 'end_date_snapshot',
                'complete': True,
                'unavailable_markets': [],
                'point_in_time': False,
                'current_universe_mutated': True,
                'market_membership': _market_membership_payload(
                    symbols_by_market
                ),
            },
        },
    }


class PykrxEnrichmentServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        def previous_weekday(value, **_kwargs):
            candidate = (
                value
                if isinstance(value, date)
                else date.fromisoformat(str(value))
            ) - timedelta(days=1)
            while candidate.weekday() >= 5:
                candidate -= timedelta(days=1)
            return candidate

        previous_trading_date_patcher = patch(
            'app.eod.pykrx_enrichment.previous_krx_trading_date',
            side_effect=previous_weekday,
        )
        self.previous_krx_trading_date_mock = previous_trading_date_patcher.start()
        self.addCleanup(previous_trading_date_patcher.stop)

    @patch('app.eod.pykrx_enrichment.load_symbols')
    def test_valuation_collection_rejects_unfinalized_date_before_provider_work(
        self,
        load_symbols_mock,
    ) -> None:
        service = PykrxEnrichmentService()
        with patch(
            'app.eod.pykrx_enrichment.validate_finalized_krx_write_date',
            side_effect=ValueError('not finalized'),
        ):
            with self.assertRaisesRegex(ValueError, 'not finalized'):
                service.collect_for_market_date(
                    market_date='2026-09-18',
                    markets=['KOSPI'],
                    categories=['valuation_snapshot'],
                )

        load_symbols_mock.assert_not_called()

    def test_large_price_fallback_filter_avoids_massive_sql_in_clause(self):
        captured_scope_kwargs: dict[str, object] = {}
        captured_statements = []
        included_symbol = SimpleNamespace(
            symbol='005930',
            market='KOSPI',
            name_ko='삼성전자',
            standard_code='KR7005930003',
        )
        excluded_symbol = SimpleNamespace(
            symbol='000660',
            market='KOSPI',
            name_ko='SK하이닉스',
            standard_code='KR7000660001',
        )
        bar = SimpleNamespace(
            open_price=1,
            high_price=2,
            low_price=1,
            close_price=2,
            volume=10,
            turnover=20,
            per=None,
            pbr=None,
            bps=None,
            eps=None,
            dividend_yield=None,
            dps=None,
            source='test',
        )

        class _Result:
            @staticmethod
            def all():
                return [(bar, included_symbol), (bar, excluded_symbol)]

        class _Session:
            @staticmethod
            def execute(statement):
                captured_statements.append(statement)
                return _Result()

        @contextmanager
        def fake_session_scope(**kwargs):
            captured_scope_kwargs.update(kwargs)
            yield _Session()

        allowed_symbols = {f'X{index:05d}' for index in range(500)}
        allowed_symbols.add('005930')
        with patch(
            'app.eod.pykrx_enrichment.session_scope',
            new=fake_session_scope,
        ):
            rows = PykrxEnrichmentService()._daily_price_rows_for_fallback(
                market_date_iso='2026-09-18',
                markets=['KOSPI', 'KOSDAQ'],
                symbol_filter=allowed_symbols,
            )

        self.assertEqual([row['symbol'] for row in rows], ['005930'])
        self.assertEqual(
            captured_scope_kwargs,
            {'statement_timeout_ms': 30_000},
        )
        self.assertNotIn('symbols.symbol IN', str(captured_statements[0]))

    def test_fetch_frame_applies_timeout_to_session_requests(self):
        observed_timeouts: list[object] = []

        def fake_request(_session, _method, _url, **kwargs):
            observed_timeouts.append(kwargs.get('timeout'))
            return object()

        def provider_call():
            from pykrx.website.comm import webio as pykrx_webio

            pykrx_webio.requests.Session().get('https://example.test/pykrx')
            return pd.DataFrame({'value': [1]}, index=['005930'])

        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_count = 0
        with (
            patch(
                'app.eod.pykrx_enrichment.settings.eod_pykrx_request_timeout_seconds',
                9,
            ),
            patch('requests.sessions.Session.request', new=fake_request),
        ):
            frame = service._fetch_frame(
                cache={},
                cache_key=('timeout',),
                operation='timeout_probe',
                func=provider_call,
            )

        self.assertFalse(frame.empty)
        self.assertEqual(observed_timeouts, [9])

    @patch('app.eod.pykrx_enrichment.stock.get_market_ticker_list')
    def test_market_ticker_membership_frame_is_nonempty_for_normal_ticker_list(
        self,
        ticker_list_mock,
    ):
        ticker_list_mock.return_value = ['005930', '000660']

        frame = _market_ticker_membership_frame(date='20260918', market='KOSPI')

        self.assertFalse(frame.empty)
        self.assertEqual(frame.index.tolist(), ['005930', '000660'])

    @patch('app.eod.pykrx_enrichment.load_symbols')
    def test_collect_can_use_full_market_coverage_for_pipeline_symbol_batch(
        self,
        load_symbols_mock,
    ):
        load_symbols_mock.return_value = [
            {
                'symbol': '005930',
                'market': 'KOSPI',
                'name_ko': '삼성전자',
                'standard_code': 'KR7005930003',
            },
            {
                'symbol': '000660',
                'market': 'KOSPI',
                'name_ko': 'SK하이닉스',
                'standard_code': 'KR7000660001',
            },
        ]
        coverage = {
            'requested_symbol_count': 1,
            'requested_market_count': 1,
            'categories': {},
        }

        with (
            patch.object(
                PykrxEnrichmentService,
                '_collect_market_breadth_rows',
                return_value=(
                    [],
                    {
                        'category': 'market_breadth',
                        'scope_type': 'market',
                        'rows_prepared': 0,
                        'errors': [],
                    },
                ),
            ),
            patch.object(
                PykrxEnrichmentService,
                '_build_coverage_summary',
                return_value=coverage,
            ) as build_coverage_mock,
            patch(
                'app.eod.pykrx_enrichment.upsert_daily_pykrx_enrichment_rows',
                return_value=0,
            ),
        ):
            PykrxEnrichmentService().collect_for_market_date(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=['005930'],
                categories=['market_breadth'],
                use_full_market_coverage=True,
            )

        self.assertIsNone(build_coverage_mock.call_args.kwargs['symbols'])

    @patch('app.eod.pykrx_enrichment.load_symbols')
    def test_full_market_collection_uses_provider_universe_for_required_valuation(
        self,
        load_symbols_mock,
    ):
        load_symbols_mock.return_value = [
            {'symbol': '005930', 'market': 'KOSPI'},
            {'symbol': '000660', 'market': 'KOSPI'},
        ]

        def coverage_summary(*, market_date, markets, symbols, categories):
            expected_rows = 2 if symbols is None else len(symbols)
            return {
                'market_date': market_date,
                'requested_symbol_count': expected_rows,
                'requested_market_count': len(markets or []),
                'categories': {
                    category: {
                        'scope_type': 'market'
                        if category == 'market_breadth'
                        else 'symbol',
                        'expected_rows': expected_rows,
                        'present_rows': expected_rows,
                        'coverage_status': 'complete',
                    }
                    for category in categories
                },
            }

        with (
            patch.object(
                PykrxEnrichmentService,
                '_collect_market_breadth_rows',
                return_value=(
                    [],
                    {
                        'category': 'market_breadth',
                        'scope_type': 'market',
                        'rows_prepared': 0,
                        'errors': [],
                    },
                ),
            ),
            patch.object(
                PykrxEnrichmentService,
                '_collect_valuation_snapshot_rows',
                return_value=(
                    [],
                    {
                        'category': 'valuation_snapshot',
                        'scope_type': 'symbol',
                        'rows_prepared': 0,
                        'errors': [],
                    },
                ),
            ) as collect_valuation_mock,
            patch.object(
                PykrxEnrichmentService,
                '_collect_foreign_ownership_rows',
                return_value=(
                    [],
                    {
                        'category': 'foreign_ownership',
                        'scope_type': 'symbol',
                        'rows_prepared': 0,
                        'errors': [],
                    },
                ),
            ),
            patch.object(
                PykrxEnrichmentService,
                '_collect_fallback_rows',
                return_value=([], {'status': 'unavailable'}),
            ),
            patch.object(
                PykrxEnrichmentService,
                '_build_coverage_summary',
                side_effect=coverage_summary,
            ) as build_coverage_mock,
            patch(
                'app.eod.pykrx_enrichment.upsert_daily_pykrx_enrichment_rows',
                return_value=0,
            ) as upsert_mock,
        ):
            result = PykrxEnrichmentService().collect_for_market_date(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=['005930'],
                categories=[
                    'market_breadth',
                    'valuation_snapshot',
                    'foreign_ownership',
                ],
                use_full_market_coverage=True,
                daily_price_bar_lock_owned=True,
            )

        self.assertEqual(
            [
                (call.kwargs['symbols'], call.kwargs['categories'])
                for call in build_coverage_mock.call_args_list
            ],
            [
                (None, ['market_breadth', 'valuation_snapshot']),
                (
                    ['005930'],
                    ['foreign_ownership'],
                ),
            ],
        )
        self.assertIsNone(
            collect_valuation_mock.call_args.kwargs['symbol_filter']
        )
        self.assertTrue(upsert_mock.call_args_list)
        self.assertTrue(
            all(
                call.kwargs.get('daily_price_bar_lock_owned') is True
                for call in upsert_mock.call_args_list
            )
        )
        self.assertEqual(
            result['coverage']['categories']['market_breadth'][
                'expected_rows'
            ],
            2,
        )
        self.assertEqual(
            result['coverage']['categories']['valuation_snapshot'][
                'expected_rows'
            ],
            2,
        )
        self.assertEqual(
            result['coverage']['categories']['foreign_ownership'][
                'expected_rows'
            ],
            1,
        )

    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_market_sector_classifications')
    @patch('app.eod.pykrx_enrichment.stock.get_market_ticker_list')
    @patch('app.eod.pykrx_enrichment.load_symbols')
    def test_full_market_coverage_collects_required_provider_universe_beyond_batch(
        self,
        load_symbols_mock,
        ticker_list_mock,
        sector_mock,
        short_volume_mock,
        short_value_mock,
        short_balance_mock,
    ):
        ticker_list_mock.return_value = ['005930', '000660']
        load_symbols_mock.return_value = [
            {
                'symbol': '005930',
                'market': 'KOSPI',
                'name_ko': '삼성전자',
                'standard_code': 'KR7005930003',
            },
            {
                'symbol': '000660',
                'market': 'KOSPI',
                'name_ko': 'SK하이닉스',
                'standard_code': 'KR7000660001',
            },
        ]
        sector_mock.return_value = pd.DataFrame(
            {
                '종목명': ['삼성전자', 'SK하이닉스'],
                '업종명': ['반도체', '반도체'],
                '종가': [70000, 120000],
                '대비': [100, 200],
                '등락률': [0.1, 0.2],
                '시가총액': [1, 2],
            },
            index=['005930', '000660'],
        )
        short_volume_mock.return_value = pd.DataFrame(
            {'공매도': [1, 2], '매수': [10, 20], '비중': [10.0, 10.0]},
            index=['005930', '000660'],
        )
        short_value_mock.return_value = pd.DataFrame(
            {'공매도': [100, 200], '매수': [1000, 2000], '비중': [10.0, 10.0]},
            index=['005930', '000660'],
        )
        short_balance_mock.return_value = pd.DataFrame(
            {
                '공매도잔고': [1, 2],
                '상장주식수': [100, 200],
                '공매도금액': [100, 200],
                '시가총액': [10000, 20000],
                '비중': [1.0, 1.0],
            },
            index=['005930', '000660'],
        )
        persisted_rows: list[dict] = []

        def upsert_side_effect(*, rows, replace_scopes=None):
            for scope in replace_scopes or []:
                persisted_rows[:] = [
                    row
                    for row in persisted_rows
                    if not (
                        row['market_date'] == scope['market_date']
                        and row['market'] == scope['market']
                        and row['category'] == scope['category']
                        and row['source'] == scope['source']
                    )
                ]
            persisted_rows.extend(deepcopy(rows))
            return len(rows)

        def load_rows_side_effect(*, market_date, categories=None, markets=None, symbols=None, **kwargs):
            return _filter_rows(
                persisted_rows,
                market_date=market_date,
                categories=categories,
                markets=markets,
                symbols=symbols,
            )

        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[
                    {'symbol': '005930', 'market': 'KOSPI'},
                    {'symbol': '000660', 'market': 'KOSPI'},
                ],
            ),
            patch(
                'app.eod.pykrx_enrichment.upsert_daily_pykrx_enrichment_rows',
                side_effect=upsert_side_effect,
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                side_effect=load_rows_side_effect,
            ),
        ):
            result = service.collect_for_market_date(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=['005930'],
                categories=['sector_classification', 'short_selling'],
                use_full_market_coverage=True,
            )

        self.assertEqual(result['categories']['sector_classification']['rows_persisted'], 2)
        self.assertEqual(result['categories']['short_selling']['rows_persisted'], 2)
        self.assertEqual(result['coverage']['categories']['sector_classification']['expected_rows'], 2)
        self.assertEqual(result['coverage']['categories']['short_selling']['expected_rows'], 2)
        short_rows = [
            row
            for row in persisted_rows
            if row.get('category') == 'short_selling'
        ]
        self.assertTrue(short_rows)
        self.assertTrue(
            all(row['payload']['universe_market_complete'] for row in short_rows)
        )
        self.assertEqual(
            {
                row['payload']['universe_reference_count']
                for row in short_rows
            },
            {2},
        )
        extra_rows = [row for row in persisted_rows if row.get('symbol') == '000660']
        self.assertTrue(extra_rows)
        self.assertTrue(all(row['name_ko'] == 'SK하이닉스' for row in extra_rows))
        self.assertTrue(all(row['standard_code'] == 'KR7000660001' for row in extra_rows))
        self.assertIsNone(load_symbols_mock.call_args.kwargs['symbols'])

    @patch('app.eod.pykrx_enrichment.load_symbols')
    def test_full_market_short_collection_replaces_konex_provider_scope(
        self,
        load_symbols_mock,
    ):
        load_symbols_mock.return_value = [
            {'symbol': '900001', 'market': 'KONEX', 'name_ko': '테스트'},
        ]
        coverage = {
            'market_date': '2026-09-18',
            'requested_symbol_count': 1,
            'requested_market_count': 1,
            'categories': {
                'short_selling': {
                    'present_rows': 0,
                    'expected_rows': None,
                    'coverage_status': 'unavailable',
                    'expectation_status': 'unavailable',
                },
            },
        }
        service = PykrxEnrichmentService()
        with (
            patch.object(
                service,
                '_collect_short_selling_rows',
                return_value=(
                    [],
                    {
                        'category': 'short_selling',
                        'scope_type': 'symbol',
                        'rows_prepared': 0,
                        'errors': [],
                    },
                ),
            ),
            patch.object(
                service,
                '_build_coverage_summary',
                return_value=coverage,
            ),
            patch(
                'app.eod.pykrx_enrichment.upsert_daily_pykrx_enrichment_rows',
                return_value=1,
            ) as upsert_mock,
        ):
            service.collect_for_market_date(
                market_date='2026-09-18',
                markets=['KONEX'],
                symbols=['900001'],
                categories=['short_selling'],
                use_full_market_coverage=True,
            )

        self.assertEqual(
            upsert_mock.call_args_list[0].kwargs['replace_scopes'],
            [
                {
                    'market_date': '2026-09-18',
                    'market': 'KONEX',
                    'category': 'short_selling',
                    'source': 'pykrx',
                }
            ],
        )

    def test_fetch_frame_preserves_krx_credentials_for_authenticated_provider_requests(self):
        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_count = 0

        def authenticated_provider():
            self.assertEqual(os.environ.get('KRX_ID'), 'test-id')
            self.assertEqual(os.environ.get('KRX_PW'), 'test-password')
            return pd.DataFrame({'value': [1]})

        with patch.dict(
            os.environ,
            {'KRX_ID': 'test-id', 'KRX_PW': 'test-password'},
        ):
            result = service._fetch_frame(
                cache={},
                cache_key=('authenticated-provider',),
                operation='authenticated_provider',
                func=authenticated_provider,
            )

        self.assertEqual(result['value'].tolist(), [1])

    def test_fetch_frame_retries_required_empty_frame(self):
        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_backoff_seconds = 0.0
        service._retry_count = 1
        provider_responses = iter(
            [
                pd.DataFrame(),
                pd.DataFrame({'value': [1]}),
            ]
        )

        result = service._fetch_frame(
            cache={},
            cache_key=('required-provider',),
            operation='required_provider',
            func=lambda: next(provider_responses),
            require_non_empty=True,
        )

        self.assertEqual(result['value'].tolist(), [1])

    def test_fetch_frame_raises_when_required_frames_remain_empty(self):
        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_backoff_seconds = 0.0
        service._retry_count = 1

        with self.assertRaisesRegex(RuntimeError, 'required_provider returned empty DataFrame'):
            service._fetch_frame(
                cache={},
                cache_key=('required-provider',),
                operation='required_provider',
                func=lambda: pd.DataFrame(),
                require_non_empty=True,
            )

    @patch('app.eod.pykrx_enrichment.load_daily_market_relative_contexts')
    @patch('app.eod.pykrx_enrichment.load_symbols')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_market_sector_classifications')
    @patch('app.eod.pykrx_enrichment.stock.get_market_ticker_list')
    def test_collect_for_market_date_normalizes_and_persists_selected_categories(
        self,
        get_market_ticker_list_mock,
        get_market_sector_classifications_mock,
        get_shorting_volume_by_ticker_mock,
        get_shorting_value_by_ticker_mock,
        get_shorting_balance_by_ticker_mock,
        load_symbols_mock,
        load_daily_market_relative_contexts_mock,
    ):
        persisted_rows: list[dict] = []
        upsert_calls: list[list[dict]] = []

        get_market_ticker_list_mock.return_value = ['005930', '000660']

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
        self.assertTrue(sector_row['payload']['universe_market_complete'])
        self.assertEqual(sector_row['payload']['universe_market_row_count'], 2)
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
        self.assertEqual(
            load_daily_market_relative_contexts_mock.call_args.kwargs[
                'statement_timeout_ms'
            ],
            30_000,
        )
        self.assertEqual(
            load_daily_market_relative_contexts_mock.call_args.kwargs['markets'],
            ['KOSPI'],
        )

    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    def test_collect_short_selling_uses_t_plus_2_balance_date_with_provenance(
        self,
        get_shorting_volume_by_ticker_mock,
        get_shorting_value_by_ticker_mock,
        get_shorting_balance_by_ticker_mock,
    ):
        self.previous_krx_trading_date_mock.side_effect = [
            date(2026, 10, 29),
            date(2026, 10, 28),
        ]
        get_shorting_volume_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [1000], '매수': [20000], '비중': [5.0]},
            index=['005930'],
        )
        get_shorting_value_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [70000000], '매수': [1400000000], '비중': [5.0]},
            index=['005930'],
        )
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
            market_date_iso='2026-10-30',
            market_date_yyyymmdd='20261030',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={'005930': {'market': 'KOSPI', 'name_ko': '삼성전자'}},
            cache={},
        )

        self.assertEqual(details['errors'], [])
        self.assertEqual(
            self.previous_krx_trading_date_mock.call_args_list,
            [
                call(date(2026, 10, 30), allow_fallback=True),
                call(date(2026, 10, 29), allow_fallback=True),
            ],
        )
        self.assertEqual(
            get_shorting_volume_by_ticker_mock.call_args.kwargs['date'],
            '20261030',
        )
        self.assertEqual(
            get_shorting_value_by_ticker_mock.call_args.kwargs['date'],
            '20261030',
        )
        self.assertEqual(
            get_shorting_balance_by_ticker_mock.call_args.kwargs['date'],
            '20261028',
        )
        self.assertEqual(
            rows[0]['payload']['short_balance']['as_of_date'],
            '2026-10-28',
        )
        self.assertEqual(
            rows[0]['payload']['short_balance']['availability_lag_trading_days'],
            2,
        )
        self.assertEqual(
            rows[0]['payload']['short_balance']['availability_contract'],
            'krx_t_plus_2',
        )

    def test_short_balance_date_crosses_weekend_with_real_krx_calendar(self) -> None:
        with patch(
            'app.eod.pykrx_enrichment.previous_krx_trading_date',
            side_effect=actual_previous_krx_trading_date,
        ):
            self.assertEqual(
                _resolve_short_balance_as_of_date('2026-10-26'),
                date(2026, 10, 22),
            )

    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    def test_short_balance_calendar_failure_keeps_same_day_trade_data(
        self,
        get_shorting_volume_by_ticker_mock,
        get_shorting_value_by_ticker_mock,
        get_shorting_balance_by_ticker_mock,
    ) -> None:
        self.previous_krx_trading_date_mock.side_effect = [
            date(2026, 10, 29),
            None,
        ]
        get_shorting_volume_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [1000], '매수': [20000], '비중': [5.0]},
            index=['005930'],
        )
        get_shorting_value_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [70000000], '매수': [1400000000], '비중': [5.0]},
            index=['005930'],
        )

        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        rows, details = service._collect_short_selling_rows(
            market_date_iso='2026-10-30',
            market_date_yyyymmdd='20261030',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={'005930': {'market': 'KOSPI', 'name_ko': '삼성전자'}},
            cache={},
        )

        self.assertEqual(rows[0]['payload']['short_volume']['short_volume'], 1000)
        self.assertEqual(rows[0]['payload']['short_volume']['short_value'], 70000000)
        self.assertEqual(rows[0]['payload']['missing_fields'], ['short_balance'])
        self.assertEqual(details['errors'][0]['operation'], 'short_selling_balance')
        self.assertIn(
            'availability date resolution failed',
            details['errors'][0]['error'],
        )
        get_shorting_balance_by_ticker_mock.assert_not_called()

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

    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    def test_collect_short_selling_rows_keeps_trade_data_when_balance_fails(
        self,
        get_shorting_volume_by_ticker_mock,
        get_shorting_value_by_ticker_mock,
        get_shorting_balance_by_ticker_mock,
    ):
        get_shorting_volume_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [1000], '매수': [20000], '비중': [5.0]},
            index=['005930'],
        )
        get_shorting_value_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [70000000], '매수': [1400000000], '비중': [5.0]},
            index=['005930'],
        )
        get_shorting_balance_by_ticker_mock.side_effect = IndexError('no balance rows')

        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_backoff_seconds = 0.0
        service._retry_count = 0
        rows, details = service._collect_short_selling_rows(
            market_date_iso='2026-04-21',
            market_date_yyyymmdd='20260421',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={'005930': {'market': 'KOSPI', 'name_ko': '삼성전자'}},
            cache={},
        )

        self.assertEqual(details['rows_prepared'], 1)
        self.assertEqual(details['errors'][0]['operation'], 'short_selling_balance')
        self.assertEqual(rows[0]['payload']['short_volume']['short_volume'], 1000)
        self.assertEqual(rows[0]['payload']['short_volume']['short_value'], 70000000)
        self.assertEqual(
            get_shorting_balance_by_ticker_mock.call_args.kwargs['date'],
            '20260417',
        )
        self.assertEqual(
            rows[0]['payload']['short_balance']['as_of_date'],
            '2026-04-17',
        )
        self.assertEqual(
            rows[0]['payload']['short_balance']['availability_lag_trading_days'],
            2,
        )
        self.assertEqual(
            rows[0]['payload']['short_balance']['availability_contract'],
            'krx_t_plus_2',
        )
        self.assertIsNone(rows[0]['payload']['short_balance']['short_balance_qty'])
        self.assertEqual(rows[0]['payload']['missing_fields'], ['short_balance'])

    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    def test_collect_short_selling_rows_marks_empty_optional_frames_partial(
        self,
        get_shorting_volume_by_ticker_mock,
        get_shorting_value_by_ticker_mock,
        get_shorting_balance_by_ticker_mock,
    ):
        get_shorting_volume_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [1000], '매수': [20000], '비중': [5.0]},
            index=['005930'],
        )
        get_shorting_value_by_ticker_mock.return_value = pd.DataFrame()
        get_shorting_balance_by_ticker_mock.return_value = pd.DataFrame()

        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_backoff_seconds = 0.0
        service._retry_count = 0
        rows, details = service._collect_short_selling_rows(
            market_date_iso='2026-04-21',
            market_date_yyyymmdd='20260421',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={'005930': {'market': 'KOSPI', 'name_ko': '삼성전자'}},
            cache={},
        )

        self.assertEqual(details['rows_prepared'], 1)
        self.assertEqual(
            [item['operation'] for item in details['errors']],
            ['short_selling_value', 'short_selling_balance'],
        )
        self.assertEqual(
            rows[0]['payload']['missing_fields'],
            ['short_value', 'short_balance'],
        )

    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    def test_collect_short_selling_rejects_nonempty_all_zero_volume_fallback(
        self,
        get_shorting_volume_by_ticker_mock,
        get_shorting_value_by_ticker_mock,
        get_shorting_balance_by_ticker_mock,
    ):
        get_shorting_volume_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [0], '매수': [0], '비중': [0.0]},
            index=['005930'],
        )
        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_backoff_seconds = 0.0
        service._retry_count = 0

        rows, details = service._collect_short_selling_rows(
            market_date_iso='2026-04-21',
            market_date_yyyymmdd='20260421',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={'005930': {'market': 'KOSPI'}},
            cache={},
        )

        self.assertEqual(rows, [])
        self.assertEqual(details['errors'][0]['operation'], 'short_selling_volume')
        get_shorting_value_by_ticker_mock.assert_not_called()
        get_shorting_balance_by_ticker_mock.assert_not_called()

    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    def test_collect_short_selling_marks_nonempty_all_zero_value_fallback_partial(
        self,
        get_shorting_volume_by_ticker_mock,
        get_shorting_value_by_ticker_mock,
        get_shorting_balance_by_ticker_mock,
    ):
        get_shorting_volume_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [1], '매수': [10], '비중': [10.0]},
            index=['005930'],
        )
        get_shorting_value_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [0], '매수': [0], '비중': [0.0]},
            index=['005930'],
        )
        get_shorting_balance_by_ticker_mock.return_value = pd.DataFrame(
            {
                '공매도잔고': [1],
                '상장주식수': [100],
                '공매도금액': [100],
                '시가총액': [10000],
                '비중': [1.0],
            },
            index=['005930'],
        )
        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_backoff_seconds = 0.0
        service._retry_count = 0

        rows, details = service._collect_short_selling_rows(
            market_date_iso='2026-04-21',
            market_date_yyyymmdd='20260421',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={'005930': {'market': 'KOSPI'}},
            cache={},
        )

        self.assertEqual(details['errors'][0]['operation'], 'short_selling_value')
        self.assertEqual(rows[0]['payload']['missing_fields'], ['short_value'])

    @patch('app.eod.pykrx_enrichment.stock.get_shorting_balance_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_value_by_ticker')
    @patch('app.eod.pykrx_enrichment.stock.get_shorting_volume_by_ticker')
    def test_short_rows_are_authorized_by_required_volume_frame_only(
        self,
        get_shorting_volume_by_ticker_mock,
        get_shorting_value_by_ticker_mock,
        get_shorting_balance_by_ticker_mock,
    ):
        get_shorting_volume_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [1000], '매수': [20000], '비중': [5.0]},
            index=['005930'],
        )
        get_shorting_value_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도': [70000000, 1], '매수': [1400000000, 2], '비중': [5.0, 50.0]},
            index=['005930', '999999'],
        )
        get_shorting_balance_by_ticker_mock.return_value = pd.DataFrame(
            {'공매도잔고': [1], '상장주식수': [2], '비중': [50.0]},
            index=['888888'],
        )

        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        rows, details = service._collect_short_selling_rows(
            market_date_iso='2026-04-21',
            market_date_yyyymmdd='20260421',
            markets=['KOSPI'],
            symbol_filter={'005930', '888888', '999999'},
            symbol_meta={},
            cache={},
        )

        self.assertEqual(details['rows_prepared'], 1)
        self.assertEqual([row['symbol'] for row in rows], ['005930'])

    @patch('app.eod.pykrx_enrichment.stock.get_market_net_purchases_of_equities_by_ticker')
    def test_symbol_investor_rows_require_every_investor_frame_for_market(
        self,
        get_investor_flow_mock,
    ):
        def provider_response(*, investor, **kwargs):
            if investor == '외국인':
                return pd.DataFrame()
            return pd.DataFrame(
                {
                    '종목명': ['삼성전자'],
                    '매도거래량': [10],
                    '매수거래량': [20],
                    '순매수거래량': [10],
                    '매도거래대금': [100],
                    '매수거래대금': [200],
                    '순매수거래대금': [100],
                },
                index=['005930'],
            )

        get_investor_flow_mock.side_effect = provider_response
        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_backoff_seconds = 0.0
        service._retry_count = 0

        rows, details = service._collect_symbol_investor_flow_rows(
            market_date_iso='2026-04-21',
            market_date_yyyymmdd='20260421',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={'005930': {'market': 'KOSPI', 'name_ko': '삼성전자'}},
            cache={},
        )

        self.assertEqual(rows, [])
        self.assertEqual(details['errors'][0]['investor'], '외국인')

    @patch('app.eod.pykrx_enrichment.stock.get_market_net_purchases_of_equities_by_ticker')
    def test_symbol_investor_rows_reject_nonempty_all_zero_provider_frames(
        self,
        get_investor_flow_mock,
    ):
        get_investor_flow_mock.return_value = pd.DataFrame(
            {
                '종목명': ['삼성전자'],
                '매도거래량': [0],
                '매수거래량': [0],
                '순매수거래량': [0],
                '매도거래대금': [0],
                '매수거래대금': [0],
                '순매수거래대금': [0],
            },
            index=['005930'],
        )
        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_backoff_seconds = 0.0
        service._retry_count = 0

        rows, details = service._collect_symbol_investor_flow_rows(
            market_date_iso='2026-09-18',
            market_date_yyyymmdd='20260918',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={'005930': {'market': 'KOSPI', 'name_ko': '삼성전자'}},
            cache={},
        )

        self.assertEqual(rows, [])
        self.assertEqual(
            {error['investor'] for error in details['errors']},
            {'개인', '외국인', '기관합계'},
        )

    @patch('app.eod.pykrx_enrichment.stock.get_market_net_purchases_of_equities_by_ticker')
    def test_symbol_investor_rows_accept_zero_sell_when_buy_activity_is_positive(
        self,
        get_investor_flow_mock,
    ):
        get_investor_flow_mock.return_value = pd.DataFrame(
            {
                '종목명': ['삼성전자'],
                '매도거래량': [0],
                '매수거래량': [20],
                '순매수거래량': [20],
                '매도거래대금': [0],
                '매수거래대금': [200],
                '순매수거래대금': [200],
            },
            index=['005930'],
        )
        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_backoff_seconds = 0.0
        service._retry_count = 0

        rows, details = service._collect_symbol_investor_flow_rows(
            market_date_iso='2026-09-18',
            market_date_yyyymmdd='20260918',
            markets=['KOSPI'],
            symbol_filter={'005930'},
            symbol_meta={},
            cache={},
        )

        self.assertEqual(details['errors'], [])
        self.assertEqual([row['symbol'] for row in rows], ['005930'])

    @patch('app.eod.pykrx_enrichment.stock.get_market_trading_volume_by_investor')
    @patch('app.eod.pykrx_enrichment.stock.get_market_trading_value_by_investor')
    def test_market_investor_rows_reject_empty_or_all_zero_provider_frames(
        self,
        value_mock,
        volume_mock,
    ):
        zero_frame = pd.DataFrame(
            {
                '매도': [0, 0, 0],
                '매수': [0, 0, 0],
                '순매수': [0, 0, 0],
            },
            index=['개인', '외국인', '기관합계'],
        )
        value_mock.return_value = zero_frame
        volume_mock.return_value = pd.DataFrame()
        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0
        service._retry_backoff_seconds = 0.0
        service._retry_count = 0

        rows, details = service._collect_market_investor_flow_rows(
            market_date_iso='2026-09-18',
            market_date_yyyymmdd='20260918',
            markets=['KOSPI'],
            cache={},
        )

        self.assertEqual(rows, [])
        self.assertTrue(details['errors'])

    def test_decision_usability_rejects_non_finite_metrics(self):
        from app.eod.pykrx_enrichment import is_decision_usable_pykrx_enrichment_row

        row = {
            'category': 'short_selling',
            'source': 'pykrx',
            'payload': {
                'short_volume': {
                    'short_volume': float('nan'),
                    'total_volume': 10,
                    'short_volume_ratio_pct': 0.0,
                }
            },
        }

        self.assertFalse(is_decision_usable_pykrx_enrichment_row(row))

    def test_coverage_completeness_rejects_partial_short_selling_payload(self):
        from app.eod.pykrx_enrichment import is_complete_pykrx_enrichment_row

        row = {
            'category': 'short_selling',
            'source': 'pykrx',
            'payload': {
                'short_volume': {
                    'short_volume': 10,
                    'total_volume': 100,
                    'short_volume_ratio_pct': 10.0,
                    'short_value': 1000,
                    'total_value': 10000,
                    'short_value_ratio_pct': 10.0,
                },
                'short_balance': {},
            },
        }

        self.assertFalse(is_complete_pykrx_enrichment_row(row))

    def test_coverage_completeness_requires_observed_valuation_market_cap(self):
        from app.eod.pykrx_enrichment import is_complete_pykrx_enrichment_row

        reconstructed = {
            'category': 'valuation_snapshot',
            'source': 'daily_price_bars_reconstructed',
            'payload': {'per': 8.0, 'pbr': 1.1, 'dividend_yield': 2.0, 'market_cap': None},
        }
        observed = {
            'category': 'valuation_snapshot',
            'source': 'pykrx',
            'payload': {'per': None, 'pbr': 1.1, 'dividend_yield': 0.0, 'market_cap': 2_000_000},
        }

        self.assertFalse(is_complete_pykrx_enrichment_row(reconstructed))
        self.assertTrue(is_complete_pykrx_enrichment_row(observed))

    def test_coverage_completeness_rejects_all_zero_valuation_metrics_without_mutation(
        self,
    ):
        from app.eod.pykrx_enrichment import is_complete_pykrx_enrichment_row

        payload = {
            'per': 0.0,
            'pbr': 0.0,
            'dividend_yield': 0.0,
            'market_cap': 2_000_000,
            'provenance': {'provider': 'pykrx', 'observed': True},
        }
        row = {
            'category': 'valuation_snapshot',
            'source': 'pykrx',
            'payload': payload,
        }

        self.assertFalse(is_complete_pykrx_enrichment_row(row))
        self.assertEqual(
            row['payload'],
            {
                'per': 0.0,
                'pbr': 0.0,
                'dividend_yield': 0.0,
                'market_cap': 2_000_000,
                'provenance': {'provider': 'pykrx', 'observed': True},
            },
        )

    def test_short_coverage_exposes_partial_component_completeness(self):
        service = PykrxEnrichmentService()
        row = {
            'category': 'short_selling',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'pykrx',
            'payload': {
                'short_volume': {
                    'short_volume': 10,
                    'total_volume': 100,
                    'short_volume_ratio_pct': 10.0,
                    'short_value': 1000,
                    'total_value': 10000,
                    'short_value_ratio_pct': 10.0,
                },
                'short_balance': {},
            },
        }

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[{'symbol': '005930', 'market': 'KOSPI'}],
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[row],
            ),
        ):
            result = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=['005930'],
                categories=['short_selling'],
            )

        coverage = result['categories']['short_selling']
        self.assertEqual(coverage['present_rows'], 1)
        self.assertEqual(coverage['coverage_status'], 'complete')
        self.assertEqual(coverage['complete_present_rows'], 0)
        self.assertEqual(coverage['completeness_status'], 'missing')
        self.assertEqual(
            coverage['component_coverage'],
            {
                'short_volume': {
                    'present_rows': 1,
                    'missing_rows': 0,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'short_value': {
                    'present_rows': 1,
                    'missing_rows': 0,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                },
                'short_balance': {
                    'present_rows': 0,
                    'missing_rows': 1,
                    'coverage_ratio': 0.0,
                    'coverage_status': 'missing',
                },
            },
        )

    def test_valuation_coverage_exposes_incomplete_observation_without_counting_it_ready(self):
        service = PykrxEnrichmentService()
        row = {
            'category': 'valuation_snapshot',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'daily_price_bars_reconstructed',
            'payload': {
                'per': 8.0,
                'pbr': 1.1,
                'dividend_yield': 2.0,
                'market_cap': None,
            },
        }

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[{'symbol': '005930', 'market': 'KOSPI'}],
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[row],
            ),
        ):
            result = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=['005930'],
                categories=['valuation_snapshot'],
            )

        coverage = result['categories']['valuation_snapshot']
        self.assertEqual(coverage['present_rows'], 0)
        self.assertEqual(coverage['raw_present_rows'], 1)
        self.assertEqual(coverage['complete_present_rows'], 0)
        self.assertEqual(coverage['missing_component_rows'], 1)
        self.assertEqual(coverage['coverage_status'], 'missing')

    @patch('app.eod.pykrx_enrichment.stock.get_market_net_purchases_of_equities_by_ticker')
    def test_symbol_investor_rows_zero_fill_absent_investor_activity(
        self,
        get_investor_flow_mock,
    ):
        def provider_response(*, investor, **kwargs):
            symbols = ['005930'] if investor == '외국인' else ['005930', '000660']
            return pd.DataFrame(
                {
                    '종목명': ['삼성전자' if symbol == '005930' else 'SK하이닉스' for symbol in symbols],
                    '매도거래량': [10] * len(symbols),
                    '매수거래량': [20] * len(symbols),
                    '순매수거래량': [10] * len(symbols),
                    '매도거래대금': [100] * len(symbols),
                    '매수거래대금': [200] * len(symbols),
                    '순매수거래대금': [100] * len(symbols),
                },
                index=symbols,
            )

        get_investor_flow_mock.side_effect = provider_response
        service = PykrxEnrichmentService()
        service._request_pause_seconds = 0.0

        rows, details = service._collect_symbol_investor_flow_rows(
            market_date_iso='2026-04-21',
            market_date_yyyymmdd='20260421',
            markets=['KOSPI'],
            symbol_filter={'005930', '000660'},
            symbol_meta={},
            cache={},
        )

        self.assertEqual(details['errors'], [])
        self.assertEqual([row['symbol'] for row in rows], ['000660', '005930'])
        rows_by_symbol = {row['symbol']: row for row in rows}
        self.assertEqual(
            rows_by_symbol['000660']['payload']['investor_groups']['외국인'],
            {
                'sell_volume': 0,
                'buy_volume': 0,
                'net_volume': 0,
                'sell_value': 0,
                'buy_value': 0,
                'net_value': 0,
            },
        )
        self.assertEqual(
            set(rows_by_symbol['000660']['payload']['investor_groups']),
            {'개인', '외국인', '기관합계'},
        )

    def test_time_sensitive_categories_never_carry_forward_prior_rows(self):
        service = PykrxEnrichmentService()

        with patch.object(service, '_collect_rows_from_latest_enrichment') as latest_rows_mock:
            for category in (
                'sector_classification',
                'investor_flow_market',
                'investor_flow_symbol',
                'short_selling',
            ):
                with self.subTest(category=category):
                    rows, details = service._collect_fallback_rows(
                        category=category,
                        market_date_iso='2026-09-18',
                        markets=['KOSPI', 'KOSDAQ'],
                        symbol_filter={'005930'},
                        symbol_meta={'005930': {'market': 'KOSPI'}},
                        point_in_time_recovery=True,
                    )

                    self.assertEqual(rows, [])
                    self.assertEqual(details['reason'], 'time_sensitive_carry_forward_not_allowed')

        latest_rows_mock.assert_not_called()

    def test_time_sensitive_carry_forward_rows_are_not_decision_inputs(self):
        stale_investor_row = {
            'category': 'investor_flow_symbol',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'pykrx_carry_forward',
            'payload': {
                'carried_forward_from_market_date': '2026-09-17',
                'investor_groups': _complete_investor_groups(),
            },
        }
        fresh_investor_row = {
            'category': 'investor_flow_symbol',
            'market': 'KOSPI',
            'scope_key': '000660',
            'symbol': '000660',
            'source': 'pykrx',
            'payload': {'investor_groups': _complete_investor_groups()},
        }
        kis_investor_row = {
            **fresh_investor_row,
            'source': 'kis_open_api',
            'payload': {
                'investor_groups': {
                    **_complete_investor_groups(),
                    '개인': {
                        **_complete_investor_groups()['개인'],
                        'net_value': -1,
                    },
                }
            },
        }
        stale_market_row = {
            'category': 'investor_flow_market',
            'market': 'KOSPI',
            'scope_key': 'KOSPI',
            'source': 'pykrx_carry_forward',
            'payload': {'carried_forward_from_market_date': '2026-09-17'},
        }
        fresh_market_row = {
            'category': 'investor_flow_market',
            'market': 'KOSPI',
            'scope_key': 'KOSPI',
            'source': 'pykrx',
            'payload': {'investor_groups': _complete_market_investor_groups()},
        }
        stale_sector_row = {
            'category': 'sector_classification',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'pykrx_carry_forward',
            'payload': {'sector_name': 'stale'},
        }

        def load_rows_side_effect(*, categories, **kwargs):
            if 'investor_flow_symbol' in categories:
                return [fresh_investor_row, stale_investor_row, kis_investor_row]
            if 'investor_flow_market' in categories:
                return [fresh_market_row, stale_market_row]
            if categories == ['sector_classification']:
                return [stale_sector_row]
            return []

        with patch(
            'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
            side_effect=load_rows_side_effect,
        ) as load_rows_mock:
            result = PykrxEnrichmentService().load_symbol_day_context_inputs(
                market_date='2026-09-18',
                symbols=['005930', '000660'],
                markets=['KOSPI'],
            )

        self.assertNotIn('investor_flow_symbol', result['by_symbol'].get('005930', {}))
        self.assertEqual(
            result['by_symbol']['000660']['investor_flow_symbol']['source'],
            'pykrx',
        )
        self.assertEqual(
            result['by_market']['KOSPI']['investor_flow_market']['source'],
            'pykrx',
        )
        self.assertEqual(result['sector_rows_by_market'], {})
        self.assertTrue(load_rows_mock.call_args_list)
        self.assertTrue(
            all(call.kwargs['limit'] is None for call in load_rows_mock.call_args_list[:2])
        )

    def test_time_sensitive_carry_forward_rows_do_not_count_toward_coverage(self):
        service = PykrxEnrichmentService()
        stale_row = {
            'category': 'short_selling',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'pykrx_carry_forward',
            'captured_at': '2026-09-18T16:00:00+09:00',
        }

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[{'symbol': '005930', 'market': 'KOSPI'}],
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[stale_row],
            ) as load_rows_mock,
        ):
            result = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=['005930'],
                categories=['short_selling'],
            )

        coverage = result['categories']['short_selling']
        self.assertEqual(coverage['present_rows'], 0)
        self.assertEqual(coverage['coverage_status'], 'missing')
        self.assertEqual(coverage['sources'], [])
        self.assertIsNone(load_rows_mock.call_args.kwargs['limit'])
        self.assertEqual(
            load_rows_mock.call_args.kwargs['statement_timeout_ms'],
            30_000,
        )
        self.assertFalse(load_rows_mock.call_args.kwargs['order_results'])
        self.assertFalse(
            load_rows_mock.call_args.kwargs['include_symbol_metadata']
        )

    def test_time_sensitive_coverage_uses_fresh_stock_eligible_universe(self):
        service = PykrxEnrichmentService()
        active_rows = [
            {'symbol': '005930', 'market': 'KOSPI'},
            {'symbol': '000660', 'market': 'KOSPI'},
            {'symbol': '069500', 'market': 'KOSPI'},
        ]
        enrichment_rows = [
            {
                'category': 'sector_classification',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'universe_market_complete': True,
                    'universe_market_row_count': 2,
                    **_universe_reference_payload(['005930', '000660']),
                    'sector_name': '반도체',
                },
            }
            for symbol in ('005930', '000660')
        ] + [
            {
                'category': 'investor_flow_symbol',
                'market': 'KOSPI',
                'scope_key': '005930',
                'symbol': '005930',
                'source': 'pykrx',
                'payload': {'investor_groups': _complete_investor_groups()},
            }
        ] + [
            {
                'category': 'short_selling',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'short_volume': {
                        'short_volume': 1,
                        'total_volume': 10,
                        'short_volume_ratio_pct': 10.0,
                    }
                },
            }
            for symbol in ('005930', '000660')
        ]
        price_rows = [
            {'symbol': '005930', 'market': 'KOSPI', 'volume': 100},
            {'symbol': '000660', 'market': 'KOSPI', 'volume': 0},
            {'symbol': '069500', 'market': 'KOSPI', 'volume': 1000},
        ]

        with (
            patch.object(service, '_load_coverage_symbol_rows', return_value=active_rows),
            patch.object(service, '_daily_price_rows_for_fallback', return_value=price_rows),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=enrichment_rows,
            ) as load_rows_mock,
        ):
            result = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=None,
                categories=[
                    'sector_classification',
                    'investor_flow_symbol',
                    'short_selling',
                ],
            )

        requested_categories = set(load_rows_mock.call_args.kwargs['categories'])
        self.assertIn('sector_classification', requested_categories)
        self.assertEqual(result['requested_symbol_count'], 3)
        sector = result['categories']['sector_classification']
        self.assertEqual(sector['expected_rows'], 2)
        self.assertEqual(sector['present_rows'], 2)
        self.assertEqual(sector['expected_universe_source'], 'fresh_pykrx_sector_classification')
        investor = result['categories']['investor_flow_symbol']
        self.assertEqual(investor['expected_rows'], 1)
        self.assertEqual(investor['present_rows'], 1)
        self.assertEqual(investor['expected_universe_source'], 'fresh_pykrx_sector_positive_volume')
        short = result['categories']['short_selling']
        self.assertEqual(short['expected_rows'], 2)
        self.assertEqual(short['present_rows'], 2)
        self.assertEqual(short['expected_universe_source'], 'fresh_pykrx_sector_classification')
        preflight = build_pykrx_enrichment_preflight(
            market_date='2026-09-18',
            coverage=result,
            categories=(
                'sector_classification',
                'investor_flow_symbol',
                'short_selling',
            ),
        )
        self.assertTrue(preflight['allowed'])
        self.assertEqual(preflight['status'], 'ready')

    def test_valuation_coverage_uses_exact_eod_membership_not_broader_sector_universe(
        self,
    ):
        service = PykrxEnrichmentService()
        eod_symbols = ['005930', '000660']
        broader_sector_symbols = [*eod_symbols, '069500']
        active_rows = [
            {'symbol': '005930', 'market': 'KOSPI'},
            {'symbol': '000660', 'market': 'KOSPI'},
        ]
        sector_rows = [
            {
                'category': 'sector_classification',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    **_universe_reference_payload(broader_sector_symbols),
                    'sector_name': '반도체',
                },
            }
            for symbol in broader_sector_symbols
        ]
        short_rows = [
            {
                'category': 'short_selling',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'short_volume': {
                        'short_volume': 1,
                        'total_volume': 10,
                        'short_volume_ratio_pct': 10.0,
                    }
                },
            }
            for symbol in broader_sector_symbols
        ]
        valuation_rows = [
            {
                'category': 'valuation_snapshot',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'market_cap': 1_000_000,
                    'per': 8.0,
                    'pbr': 1.0,
                    'dividend_yield': 2.0,
                },
            }
            for symbol in eod_symbols
        ]

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=active_rows,
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[*sector_rows, *short_rows, *valuation_rows],
            ) as load_rows_mock,
        ):
            result = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=None,
                categories=['valuation_snapshot'],
            )

        requested_categories = set(load_rows_mock.call_args.kwargs['categories'])
        self.assertEqual(requested_categories, {'valuation_snapshot'})
        coverage = result['categories']['valuation_snapshot']
        self.assertEqual(coverage['expected_rows'], 2)
        self.assertEqual(coverage['present_rows'], 2)
        self.assertEqual(coverage['expected_scope_keys'], sorted(eod_symbols))
        self.assertEqual(
            coverage['expected_universe_source'],
            'validated_eod_run_market_membership',
        )
        preflight = build_pykrx_enrichment_preflight(
            market_date='2026-09-18',
            coverage=result,
            categories=('valuation_snapshot',),
        )
        self.assertTrue(preflight['allowed'])

    def test_investor_coverage_remains_partial_for_missing_positive_volume_stock(self):
        service = PykrxEnrichmentService()
        sector_rows = [
            {
                'category': 'sector_classification',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'universe_market_complete': True,
                    'universe_market_row_count': 2,
                    **_universe_reference_payload(['005930', '000660']),
                    'sector_name': '반도체',
                },
            }
            for symbol in ('005930', '000660')
        ]
        investor_row = {
            'category': 'investor_flow_symbol',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'pykrx',
            'payload': {'investor_groups': _complete_investor_groups()},
        }
        short_rows = [
            {
                'category': 'short_selling',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'short_volume': {
                        'short_volume': 1,
                        'total_volume': 10,
                        'short_volume_ratio_pct': 10.0,
                    }
                },
            }
            for symbol in ('005930', '000660')
        ]

        with (
            patch.object(service, '_load_coverage_symbol_rows', return_value=sector_rows),
            patch.object(
                service,
                '_daily_price_rows_for_fallback',
                return_value=[
                    {'symbol': '005930', 'market': 'KOSPI', 'volume': 100},
                    {'symbol': '000660', 'market': 'KOSPI', 'volume': 200},
                ],
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[*sector_rows, investor_row, *short_rows],
            ),
        ):
            result = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=None,
                categories=['investor_flow_symbol'],
            )

        coverage = result['categories']['investor_flow_symbol']
        self.assertEqual(coverage['expected_rows'], 2)
        self.assertEqual(coverage['present_rows'], 1)
        self.assertEqual(coverage['coverage_status'], 'partial')

    def test_investor_expectation_uses_active_universe_and_requires_its_prices(self):
        service = PykrxEnrichmentService()
        sector_symbols = ['005930', '000660']
        sector_rows = [
            {
                'category': 'sector_classification',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'universe_market_complete': True,
                    'universe_market_row_count': 2,
                    **_universe_reference_payload(sector_symbols),
                    'sector_name': '반도체',
                },
            }
            for symbol in sector_symbols
        ]
        investor_row = {
            'category': 'investor_flow_symbol',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'pykrx',
            'payload': {'investor_groups': _complete_investor_groups()},
        }
        short_rows = [
            {
                'category': 'short_selling',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'short_volume': {
                        'short_volume': 1,
                        'total_volume': 10,
                        'short_volume_ratio_pct': 10.0,
                    }
                },
            }
            for symbol in sector_symbols
        ]

        cases = (
            (
                'inactive_sector_symbol_excluded',
                [{'symbol': '005930', 'market': 'KOSPI'}],
                'available',
                1,
                'complete',
            ),
            (
                'active_sector_symbol_missing_price',
                [
                    {'symbol': '005930', 'market': 'KOSPI'},
                    {'symbol': '000660', 'market': 'KOSPI'},
                ],
                'unavailable',
                None,
                'unavailable',
            ),
        )
        for (
            case_name,
            active_symbol_rows,
            expectation_status,
            expected_rows,
            coverage_status,
        ) in cases:
            with self.subTest(case=case_name):
                with (
                    patch.object(
                        service,
                        '_load_coverage_symbol_rows',
                        return_value=active_symbol_rows,
                    ),
                    patch.object(
                        service,
                        '_daily_price_rows_for_fallback',
                        return_value=[
                            {
                                'symbol': '005930',
                                'market': 'KOSPI',
                                'volume': 100,
                            },
                        ],
                    ),
                    patch(
                        'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                        return_value=[*sector_rows, investor_row, *short_rows],
                    ),
                ):
                    result = service._build_coverage_summary(
                        market_date='2026-09-18',
                        markets=['KOSPI'],
                        symbols=None,
                        categories=[
                            'sector_classification',
                            'investor_flow_symbol',
                            'short_selling',
                        ],
                    )

                investor = result['categories']['investor_flow_symbol']
                self.assertEqual(
                    investor['expectation_status'], expectation_status
                )
                self.assertEqual(investor['expected_rows'], expected_rows)
                self.assertEqual(investor['coverage_status'], coverage_status)
                if expected_rows is not None:
                    self.assertEqual(investor['present_rows'], expected_rows)

    def test_investor_expectation_waits_when_all_stock_volumes_are_zero(self):
        service = PykrxEnrichmentService()
        sector_rows = [
            {
                'category': 'sector_classification',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    **_universe_reference_payload(['005930', '000660']),
                    'sector_name': '반도체',
                },
            }
            for symbol in ('005930', '000660')
        ]
        short_rows = [
            {
                'category': 'short_selling',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'short_volume': {
                        'short_volume': 1,
                        'total_volume': 10,
                        'short_volume_ratio_pct': 10.0,
                    }
                },
            }
            for symbol in ('005930', '000660')
        ]

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[
                    {'symbol': '005930', 'market': 'KOSPI'},
                    {'symbol': '000660', 'market': 'KOSPI'},
                ],
            ),
            patch.object(
                service,
                '_daily_price_rows_for_fallback',
                return_value=[
                    {'symbol': '005930', 'market': 'KOSPI', 'volume': 0},
                    {'symbol': '000660', 'market': 'KOSPI', 'volume': 0},
                ],
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[*sector_rows, *short_rows],
            ),
        ):
            result = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=None,
                categories=['sector_classification', 'investor_flow_symbol', 'short_selling'],
            )

        investor = result['categories']['investor_flow_symbol']
        self.assertIsNone(investor['expected_rows'])
        self.assertEqual(investor['expectation_status'], 'unavailable')
        self.assertEqual(
            investor['expected_universe_source'],
            'same_day_positive_volume_unavailable',
        )

    def test_fresh_stock_universe_requires_every_requested_market(self):
        service = PykrxEnrichmentService()
        kospi_sector = {
            'category': 'sector_classification',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'pykrx',
            'payload': {
                'universe_market_complete': True,
                'universe_market_row_count': 1,
                **_universe_reference_payload(['005930']),
                'sector_name': '반도체',
            },
        }

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[
                    {'symbol': '005930', 'market': 'KOSPI'},
                    {'symbol': '035720', 'market': 'KOSDAQ'},
                ],
            ),
            patch.object(service, '_daily_price_rows_for_fallback') as price_rows_mock,
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[kospi_sector],
            ),
        ):
            result = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI', 'KOSDAQ'],
                symbols=None,
                categories=['sector_classification', 'investor_flow_symbol', 'short_selling'],
            )

        price_rows_mock.assert_not_called()
        for category in ('sector_classification', 'investor_flow_symbol', 'short_selling'):
            with self.subTest(category=category):
                coverage = result['categories'][category]
                self.assertIsNone(coverage['expected_rows'])
                self.assertEqual(coverage['expectation_status'], 'unavailable')

    def test_mixed_market_short_coverage_requires_konex_membership_proof(self):
        service = PykrxEnrichmentService()
        kospi_sector = {
            'category': 'sector_classification',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'pykrx',
            'payload': {
                **_universe_reference_payload(['005930']),
                'sector_name': '반도체',
            },
        }
        kospi_short = {
            'category': 'short_selling',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'pykrx',
            'payload': {
                'short_volume': {
                    'short_volume': 1,
                    'total_volume': 10,
                    'short_volume_ratio_pct': 10.0,
                },
            },
        }

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[
                    {'symbol': '005930', 'market': 'KOSPI'},
                    {'symbol': '900001', 'market': 'KONEX'},
                ],
            ),
            patch.object(service, '_daily_price_rows_for_fallback', return_value=[]),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[kospi_sector, kospi_short],
            ),
        ):
            coverage = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI', 'KONEX'],
                symbols=None,
                categories=[
                    'sector_classification',
                    'investor_flow_symbol',
                    'short_selling',
                ],
            )

        short_selling = coverage['categories']['short_selling']
        self.assertIsNone(short_selling['expected_rows'])
        self.assertEqual(short_selling['expectation_status'], 'unavailable')
        self.assertEqual(
            short_selling['expected_universe_source'],
            'fresh_pykrx_short_selling_universe_unavailable',
        )

    def test_konex_short_coverage_uses_own_membership_proof(self):
        service = PykrxEnrichmentService()
        konex_short = {
            'category': 'short_selling',
            'market': 'KONEX',
            'scope_key': '900001',
            'symbol': '900001',
            'source': 'pykrx',
            'payload': {
                'universe_market_complete': True,
                'universe_market_row_count': 1,
                **_universe_reference_payload(['900001']),
                'short_volume': {
                    'short_volume': 1,
                    'total_volume': 10,
                    'short_volume_ratio_pct': 10.0,
                },
            },
        }

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[{'symbol': '900001', 'market': 'KONEX'}],
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[konex_short],
            ),
        ):
            coverage = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KONEX'],
                symbols=None,
                categories=['short_selling'],
            )

        short_selling = coverage['categories']['short_selling']
        self.assertEqual(short_selling['expected_rows'], 1)
        self.assertEqual(short_selling['present_rows'], 1)
        self.assertEqual(short_selling['expectation_status'], 'available')
        self.assertEqual(short_selling['coverage_status'], 'complete')
        self.assertEqual(
            short_selling['expected_universe_source'],
            'fresh_pykrx_short_selling_universe',
        )

    def test_konex_partial_retries_cannot_union_certify_short_coverage(self):
        service = PykrxEnrichmentService()
        reference_payload = _universe_reference_payload(['900001', '900002'])
        partial_rows = [
            {
                'category': 'short_selling',
                'market': 'KONEX',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'universe_market_complete': False,
                    'universe_market_row_count': 1,
                    **reference_payload,
                    'short_volume': {
                        'short_volume': 1,
                        'total_volume': 10,
                        'short_volume_ratio_pct': 10.0,
                    },
                },
            }
            for symbol in ('900001', '900002')
        ]

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[
                    {'symbol': '900001', 'market': 'KONEX'},
                    {'symbol': '900002', 'market': 'KONEX'},
                ],
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=partial_rows,
            ),
        ):
            coverage = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KONEX'],
                symbols=None,
                categories=['short_selling'],
            )

        short_selling = coverage['categories']['short_selling']
        self.assertIsNone(short_selling['expected_rows'])
        self.assertEqual(short_selling['expectation_status'], 'unavailable')
        self.assertEqual(short_selling['coverage_status'], 'unavailable')

    def test_konex_only_unsupported_required_coverage_is_not_applicable(self):
        service = PykrxEnrichmentService()
        required_categories = [
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        ]
        unsupported_categories = [
            'sector_classification',
            'investor_flow_symbol',
        ]

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[{'symbol': '900001', 'market': 'KONEX'}],
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[],
            ),
        ):
            coverage = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KONEX'],
                symbols=None,
                categories=required_categories,
            )

        for category in unsupported_categories:
            with self.subTest(category=category):
                payload = coverage['categories'][category]
                self.assertEqual(payload['expected_rows'], 0)
                self.assertEqual(
                    payload['expectation_status'],
                    'not_applicable',
                )
                self.assertEqual(
                    payload['coverage_status'],
                    'not_applicable',
                )

        short_selling = coverage['categories']['short_selling']
        self.assertIsNone(short_selling['expected_rows'])
        self.assertEqual(short_selling['expectation_status'], 'unavailable')
        self.assertEqual(short_selling['coverage_status'], 'unavailable')

        preflight = build_pykrx_enrichment_preflight(
            market_date='2026-09-18',
            coverage=coverage,
            categories=required_categories,
        )
        self.assertEqual(preflight['status'], 'waiting')
        self.assertFalse(preflight['allowed'])
        self.assertEqual(preflight['failed_categories'], ['short_selling'])
        self.assertEqual(
            [
                item['category']
                for item in preflight['not_applicable_categories']
            ],
            unsupported_categories,
        )

    def test_explicit_unsupported_symbols_are_not_applicable(self):
        service = PykrxEnrichmentService()

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[{'symbol': '900001', 'market': 'KONEX'}],
            ),
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[],
            ),
        ):
            coverage = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KONEX'],
                symbols=['900001'],
                categories=['sector_classification'],
            )

        payload = coverage['categories']['sector_classification']
        self.assertEqual(payload['expected_rows'], 0)
        self.assertEqual(payload['expectation_status'], 'not_applicable')
        self.assertEqual(payload['coverage_status'], 'not_applicable')

    def test_truncated_sector_response_cannot_self_certify_complete(self):
        service = PykrxEnrichmentService()
        sector_row = {
            'category': 'sector_classification',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'pykrx',
            'payload': {
                'universe_market_complete': True,
                'universe_market_row_count': 1,
                **_universe_reference_payload(['005930', '000660']),
                'sector_name': '반도체',
            },
        }
        short_rows = [
            {
                'category': 'short_selling',
                'market': 'KOSPI',
                'scope_key': symbol,
                'symbol': symbol,
                'source': 'pykrx',
                'payload': {
                    'short_volume': {
                        'short_volume': 1,
                        'total_volume': 10,
                        'short_volume_ratio_pct': 10.0,
                    }
                },
            }
            for symbol in ('005930',)
        ]

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[
                    {'symbol': '005930', 'market': 'KOSPI'},
                    {'symbol': '000660', 'market': 'KOSPI'},
                ],
            ),
            patch.object(service, '_daily_price_rows_for_fallback') as price_rows_mock,
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[sector_row, *short_rows],
            ),
        ):
            result = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=None,
                categories=['sector_classification', 'investor_flow_symbol', 'short_selling'],
            )

        price_rows_mock.assert_not_called()
        for category in ('sector_classification', 'investor_flow_symbol', 'short_selling'):
            with self.subTest(category=category):
                self.assertEqual(
                    result['categories'][category]['expectation_status'],
                    'unavailable',
                )

    def test_time_sensitive_coverage_waits_without_fresh_stock_universe(self):
        service = PykrxEnrichmentService()
        reconstructed_sector = {
            'category': 'sector_classification',
            'market': 'KOSPI',
            'scope_key': '005930',
            'symbol': '005930',
            'source': 'daily_price_bars_reconstructed',
        }

        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=[{'symbol': '005930', 'market': 'KOSPI'}],
            ),
            patch.object(service, '_daily_price_rows_for_fallback') as price_rows_mock,
            patch(
                'app.eod.pykrx_enrichment.load_daily_pykrx_enrichment_rows',
                return_value=[reconstructed_sector],
            ),
        ):
            result = service._build_coverage_summary(
                market_date='2026-09-18',
                markets=['KOSPI'],
                symbols=None,
                categories=['short_selling'],
            )

        price_rows_mock.assert_not_called()
        coverage = result['categories']['short_selling']
        self.assertIsNone(coverage['expected_rows'])
        self.assertEqual(coverage['coverage_status'], 'unavailable')
        self.assertEqual(coverage['expectation_status'], 'unavailable')

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
        historical_sector_fallback.assert_not_called()
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
                'payload': {
                    **_universe_reference_payload(['005930']),
                    'sector_name': '반도체',
                },
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
                symbols=['005930', '000660'],
                categories=['sector_classification', 'investor_flow_symbol'],
                limit=10,
            )

        self.assertEqual(result['summary']['rows_returned'], 1)
        self.assertEqual(result['coverage']['categories']['sector_classification']['coverage_status'], 'partial')
        self.assertEqual(result['coverage']['categories']['investor_flow_symbol']['coverage_status'], 'missing')
        self.assertEqual(result['coverage']['categories']['investor_flow_symbol']['expected_rows'], 2)

    def test_sector_classification_coverage_excludes_unsupported_konex_symbols(self):
        active_symbol_rows = [
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
                'payload': {
                    **_universe_reference_payload(['005930']),
                    'sector_name': '반도체',
                },
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
                'payload': {
                    **_universe_reference_payload(['035720']),
                    'sector_name': '서비스업',
                },
                'captured_at': '2026-04-21T15:10:00+09:00',
            },
        ] + [
            {
                'market_date': '2026-04-21',
                'market': market,
                'category': 'short_selling',
                'scope_type': 'symbol',
                'scope_key': symbol,
                'source': 'pykrx',
                'symbol': symbol,
                'name_ko': symbol,
                'payload': {
                    'short_volume': {
                        'short_volume': 1,
                        'total_volume': 10,
                        'short_volume_ratio_pct': 10.0,
                    }
                },
                'captured_at': '2026-04-21T15:10:00+09:00',
            }
            for market, symbol in (('KOSPI', '005930'), ('KOSDAQ', '035720'))
        ]

        service = PykrxEnrichmentService()
        with (
            patch.object(
                service,
                '_load_coverage_symbol_rows',
                return_value=active_symbol_rows,
            ),
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
            result = service.inspect(
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

    def test_coverage_uses_prior_exact_membership_after_later_subset_run(self):
        symbol_rows = [
            {'symbol': '005930', 'name_ko': '삼성전자', 'market': 'KOSPI'},
            {'symbol': '000660', 'name_ko': 'SK하이닉스', 'market': 'KOSPI'},
        ]
        subset_run = _exact_membership_run(
            run_id=42,
            market_date='2026-04-21',
            symbols_by_market={'KOSPI': ['005930']},
            limited_run=True,
        )
        full_run = _exact_membership_run(
            run_id=41,
            market_date='2026-04-21',
            symbols_by_market={'KOSPI': ['005930', '000660']},
        )

        with (
            patch(
                'app.eod.pykrx_enrichment.list_eod_collection_runs',
                return_value=[subset_run, full_run],
            ) as list_runs_mock,
            patch('app.eod.pykrx_enrichment.load_symbols', return_value=symbol_rows) as load_symbols_mock,
        ):
            result = PykrxEnrichmentService()._load_coverage_symbol_rows(
                market_date='2026-04-21',
                markets=['KOSPI'],
                symbols=[],
            )

        self.assertEqual(result, symbol_rows)
        load_symbols_mock.assert_called_once_with(
            markets=['KOSPI'],
            symbols=['000660', '005930'],
            limit=2,
        )
        list_runs_mock.assert_called_once_with(
            market='KRX',
            trade_date='2026-04-21',
            source='pykrx',
            statuses=['running', 'completed', 'completed_with_errors'],
            limit=50,
            statement_timeout_ms=30_000,
        )

    def test_coverage_rejects_symbol_and_retry_subsets(self):
        symbol_subset = _exact_membership_run(
            run_id=42,
            market_date='2026-04-21',
            symbols_by_market={'KOSPI': ['005930']},
            symbol='005930',
        )
        retry_subset = _exact_membership_run(
            run_id=43,
            market_date='2026-04-21',
            symbols_by_market={'KOSPI': ['005930', '000660']},
        )
        retry_subset['symbols_total'] = 1
        retry_subset['run_metadata']['requested_symbols'] = ['005930']
        retry_scope_subset = _exact_membership_run(
            run_id=44,
            market_date='2026-04-21',
            symbols_by_market={'KOSPI': ['005930']},
        )
        retry_scope_universe = retry_scope_subset['run_metadata'][
            'universe_snapshot'
        ]
        retry_scope_universe.update(
            {
                'persisted_count': 0,
                'universe_source': 'point_in_time_retry_scope',
                'point_in_time': True,
                'current_universe_mutated': False,
            }
        )

        for case_name, candidate in (
            ('symbol_subset', symbol_subset),
            ('retry_subset', retry_subset),
            ('self_consistent_retry_scope_subset', retry_scope_subset),
        ):
            with self.subTest(case=case_name):
                with (
                    patch(
                        'app.eod.pykrx_enrichment.list_eod_collection_runs',
                        return_value=[candidate],
                    ),
                    patch(
                        'app.eod.pykrx_enrichment.load_symbols'
                    ) as load_symbols_mock,
                    patch.object(
                        PykrxEnrichmentService,
                        '_daily_price_rows_for_fallback',
                    ) as price_fallback_mock,
                ):
                    result = PykrxEnrichmentService()._load_coverage_symbol_rows(
                        market_date='2026-04-21',
                        markets=['KOSPI'],
                        symbols=[],
                    )

                self.assertEqual(result, [])
                load_symbols_mock.assert_not_called()
                price_fallback_mock.assert_not_called()

    def test_coverage_rejects_wrong_loaded_membership_set(self):
        full_run = _exact_membership_run(
            run_id=41,
            market_date='2026-04-21',
            symbols_by_market={'KOSPI': ['005930', '000660']},
        )
        wrong_symbol_rows = [
            {'symbol': '005930', 'name_ko': '삼성전자', 'market': 'KOSPI'},
            {'symbol': '035720', 'name_ko': '카카오', 'market': 'KOSPI'},
        ]
        with (
            patch(
                'app.eod.pykrx_enrichment.list_eod_collection_runs',
                return_value=[full_run],
            ),
            patch(
                'app.eod.pykrx_enrichment.load_symbols',
                return_value=wrong_symbol_rows,
            ),
        ):
            result = PykrxEnrichmentService()._load_coverage_symbol_rows(
                market_date='2026-04-21',
                markets=['KOSPI'],
                symbols=[],
            )

        self.assertEqual(result, [])


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
