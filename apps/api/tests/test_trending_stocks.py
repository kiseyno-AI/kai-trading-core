import unittest
from datetime import date
from unittest.mock import ANY, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_scanner
from app.internal import ops
from app.market_data_store import (
    build_daily_trending_rank_items,
    compute_and_persist_daily_trending_ranks,
    replace_daily_trending_ranks,
)
from app.scanner.trending import TrendingStocksService


class TrendingStocksServiceTests(unittest.TestCase):
    def test_get_trending_stocks_uses_latest_stored_rank_date_and_ranks_rows(self):
        calls = []
        requested_periods = []

        def fake_loader(**kwargs):
            calls.append(kwargs)
            return [
                {
                    'symbol': '005930',
                    'name_ko': 'Samsung Electronics',
                    'market': 'KOSPI',
                    'market_date': kwargs['market_date'],
                    'trend_direction': 'up',
                    'metric': {'period': kwargs['period'], 'name': 'return_5d_pct', 'value': 8.2},
                    'price': None,
                    'technical': {},
                    'action_summary': None,
                },
                {
                    'symbol': '000660',
                    'name_ko': 'SK hynix',
                    'market': 'KOSPI',
                    'market_date': kwargs['market_date'],
                    'trend_direction': 'down',
                    'metric': {'period': kwargs['period'], 'name': 'return_5d_pct', 'value': -6.5},
                    'price': None,
                    'technical': {},
                    'action_summary': None,
                },
            ]

        def latest_day_trending_date(period):
            requested_periods.append(period)
            return '2026-04-10'

        service = TrendingStocksService(
            latest_day_trending_market_date_loader=latest_day_trending_date,
            trending_loader=fake_loader,
        )

        result = service.get_trending_stocks(
            period='5D',
            direction='BOTH',
            limit=10,
            markets=['kospi,kosdaq', 'KOSPI'],
        )

        self.assertEqual(calls[0]['market_date'], '2026-04-10')
        self.assertEqual(calls[0]['period'], '5d')
        self.assertEqual(calls[0]['direction'], 'both')
        self.assertEqual(calls[0]['markets'], ['KOSPI', 'KOSDAQ'])
        self.assertEqual(requested_periods, ['5d'])
        self.assertEqual(result['request']['resolved_market_date'], '2026-04-10')
        self.assertEqual(result['summary']['count'], 2)
        self.assertEqual(result['summary']['up_count'], 1)
        self.assertEqual(result['summary']['down_count'], 1)
        self.assertEqual(result['items'][0]['rank'], 1)
        self.assertEqual(result['items'][1]['rank'], 2)

    def test_get_trending_stocks_returns_empty_when_no_stored_date_exists(self):
        service = TrendingStocksService(
            latest_day_trending_market_date_loader=lambda _: None,
            trending_loader=lambda **_: [{'symbol': 'should-not-load'}],
        )

        result = service.get_trending_stocks(period='20d')

        self.assertEqual(result['request']['resolved_market_date'], None)
        self.assertEqual(result['summary']['count'], 0)
        self.assertEqual(result['summary']['data_available'], False)
        self.assertEqual(result['items'], [])

    def test_get_trending_stocks_rejects_unknown_period(self):
        service = TrendingStocksService()

        with self.assertRaisesRegex(ValueError, 'period must be one of'):
            service.get_trending_stocks(period='1h')

    def test_get_trending_stocks_keeps_action_score_on_action_summary_date(self):
        calls = []

        service = TrendingStocksService(
            latest_day_trending_market_date_loader=lambda _: calls.append('day') or None,
            latest_action_market_date_loader=lambda: '2026-04-09',
            trending_loader=lambda **kwargs: [
                {
                    'symbol': '005930',
                    'name_ko': 'Samsung Electronics',
                    'market': 'KOSPI',
                    'market_date': kwargs['market_date'],
                    'trend_direction': 'up',
                    'metric': {'period': kwargs['period'], 'name': 'signal_score', 'value': 82},
                    'price': None,
                    'technical': {},
                    'action_summary': {},
                }
            ],
        )

        result = service.get_trending_stocks(period='action_score')

        self.assertEqual(calls, [])
        self.assertEqual(result['request']['resolved_market_date'], '2026-04-09')
        self.assertEqual(result['items'][0]['metric']['name'], 'signal_score')


class TrendingRankBuilderTests(unittest.TestCase):
    def test_build_daily_trending_rank_items_assigns_directional_and_absolute_ranks(self):
        ranked = build_daily_trending_rank_items(
            [
                {'symbol': 'BBB', 'metric': {'value': 5.0}, 'trend_direction': 'up'},
                {'symbol': 'AAA', 'metric': {'value': -7.0}, 'trend_direction': 'down'},
                {'symbol': 'CCC', 'metric': {'value': 0.0}, 'trend_direction': 'flat'},
                {'symbol': 'AAA2', 'metric': {'value': 5.0}, 'trend_direction': 'up'},
            ]
        )

        ranks_by_symbol = {item['symbol']: item['stored_ranks'] for item in ranked}

        self.assertEqual(ranks_by_symbol['AAA']['absolute_rank'], 1)
        self.assertEqual(ranks_by_symbol['AAA2']['absolute_rank'], 2)
        self.assertEqual(ranks_by_symbol['BBB']['absolute_rank'], 3)
        self.assertEqual(ranks_by_symbol['CCC']['absolute_rank'], 4)
        self.assertEqual(ranks_by_symbol['AAA']['down_rank'], 1)
        self.assertEqual(ranks_by_symbol['AAA2']['up_rank'], 1)
        self.assertEqual(ranks_by_symbol['BBB']['up_rank'], 2)
        self.assertIsNone(ranks_by_symbol['CCC']['up_rank'])
        self.assertIsNone(ranks_by_symbol['CCC']['down_rank'])


class DailyTrendingRankPersistenceTests(unittest.TestCase):
    def test_scoped_replacement_requires_authoritative_membership_proof(self):
        with patch(
            'app.market_data_store.replace_daily_trending_ranks'
        ) as replace_mock:
            with self.assertRaisesRegex(
                ValueError,
                'authoritative full membership proof',
            ):
                compute_and_persist_daily_trending_ranks(
                    market_date='2026-04-29',
                    periods=['1d'],
                    symbols=['005930'],
                )

        replace_mock.assert_not_called()

    def test_compute_marks_empty_source_period_unavailable_and_partial(self):
        progress_updates: list[dict] = []

        with (
            patch(
                'app.market_data_store.'
                '_load_trending_stocks_from_daily_price_bars',
                return_value=[],
            ),
            patch(
                'app.market_data_store.replace_daily_trending_ranks',
                return_value=0,
            ) as replace_mock,
        ):
            result = compute_and_persist_daily_trending_ranks(
                market_date='2026-04-29',
                periods=['1d'],
                symbols=['005930'],
                authoritative_scope=True,
                progress_callback=progress_updates.append,
            )

        replace_mock.assert_called_once()
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            result['reason'],
            'trending_source_periods_unavailable',
        )
        self.assertEqual(result['periods_persisted'], 0)
        self.assertEqual(result['periods_unavailable'], 1)
        self.assertEqual(result['unavailable_periods'], ['1d'])
        self.assertEqual(result['periods'][0]['status'], 'unavailable')
        self.assertEqual(progress_updates[-1]['status'], 'partial')
        self.assertEqual(progress_updates[-1]['failed'], 1)

    def test_compute_and_persist_rebuilds_ranks_without_loader_limit(self):
        with (
            patch(
                'app.market_data_store._load_trending_stocks_from_daily_price_bars',
                return_value=[],
            ) as daily_loader,
            patch(
                'app.market_data_store._load_trending_stocks_from_snapshots',
                return_value=[],
            ) as snapshot_loader,
            patch(
                'app.market_data_store.replace_daily_trending_ranks',
                return_value=0,
            ),
        ):
            compute_and_persist_daily_trending_ranks(
                market_date='2026-04-29',
                periods=['1d', '5d', '20d'],
                symbols=['005930'],
                authoritative_scope=True,
            )

        self.assertIsNone(daily_loader.call_args.kwargs['limit'])
        self.assertEqual([call.kwargs['limit'] for call in snapshot_loader.call_args_list], [None, None])

    def test_compute_and_persist_propagates_progress_callback_failures(self):
        progress_updates: list[dict] = []

        def _failing_progress_callback(progress: dict) -> None:
            progress_updates.append(progress)
            raise RuntimeError('progress store unavailable')

        with (
            patch(
                'app.market_data_store._load_trending_stocks_from_daily_price_bars',
                return_value=[{'symbol': '005930'}],
            ),
            patch(
                'app.market_data_store._load_trending_stocks_from_snapshots',
                return_value=[{'symbol': '000660'}],
            ),
            patch(
                'app.market_data_store.replace_daily_trending_ranks',
                side_effect=[1, 1],
            ) as replace_mock,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'progress store unavailable',
            ):
                compute_and_persist_daily_trending_ranks(
                    market_date='2026-04-29',
                    periods=['1d', '5d'],
                    symbols=['000660', '005930'],
                    authoritative_scope=True,
                    progress_callback=_failing_progress_callback,
                )

        self.assertEqual(
            [item['status'] for item in progress_updates],
            ['running'],
        )
        replace_mock.assert_not_called()

    def test_replace_daily_trending_ranks_uses_one_rollback_scope_for_insert_failure(self):
        class FakeResult:
            def all(self):
                return [('AAA', 1), ('BBB', 2)]

        class FakeSession:
            def __init__(self):
                self.insert_count = 0
                self.statements = []

            def execute(self, statement):
                statement_name = statement.__class__.__name__
                self.statements.append(statement_name)
                if statement_name == 'Select':
                    return FakeResult()
                if statement_name == 'Insert':
                    self.insert_count += 1
                    if self.insert_count == 2:
                        raise RuntimeError('insert failed')
                return None

        class FakeSessionScope:
            def __init__(self):
                self.session = FakeSession()
                self.committed = False
                self.rolled_back = False

            def __enter__(self):
                return self.session

            def __exit__(self, exc_type, _exc, _tb):
                if exc_type is None:
                    self.committed = True
                else:
                    self.rolled_back = True
                return False

        scopes: list[FakeSessionScope] = []

        def fake_session_scope():
            scope = FakeSessionScope()
            scopes.append(scope)
            return scope

        items = [
            {
                'symbol': 'AAA',
                'trend_direction': 'up',
                'metric': {'name': 'return_5d_pct', 'value': 10.0},
                'price': None,
                'technical': {},
                'action_summary': None,
            },
            {
                'symbol': 'BBB',
                'trend_direction': 'down',
                'metric': {'name': 'return_5d_pct', 'value': -8.0},
                'price': None,
                'technical': {},
                'action_summary': None,
            },
        ]

        with (
            patch('app.market_data_store.session_scope', side_effect=fake_session_scope),
            patch('app.market_data_store.TRENDING_RANK_INSERT_CHUNK_SIZE', 1),
        ):
            with self.assertRaisesRegex(RuntimeError, 'insert failed'):
                replace_daily_trending_ranks(
                    market_date='2026-04-29',
                    period='5d',
                    items=items,
                )

        self.assertEqual(len(scopes), 1)
        self.assertFalse(scopes[0].committed)
        self.assertTrue(scopes[0].rolled_back)
        self.assertEqual(scopes[0].session.statements, ['Delete', 'Select', 'Insert', 'Insert'])


class TrendingRankInternalOpsTests(unittest.TestCase):
    def test_pending_repair_records_completed_trending_outputs(self):
        result_payload = {
            'market_date': '2026-04-29',
            'status': 'completed',
            'periods': [
                {'period': period, 'status': 'completed'}
                for period in ('1d', '5d', '20d')
            ],
        }
        frozen_symbols = ('000660', '005930')

        with (
            patch('app.internal.ops.bootstrap_runtime'),
            patch(
                'app.internal.ops.'
                'resolve_pending_frozen_price_dependent_membership',
                return_value=frozen_symbols,
            ),
            patch(
                'app.internal.ops.'
                'compute_and_persist_daily_trending_ranks',
                return_value=result_payload,
            ) as compute,
            patch(
                'app.internal.ops.record_pending_trending_rebuild'
            ) as record,
        ):
            result = ops.compute_trending_ranks(
                market_date='2026-04-29'
            )

        self.assertEqual(result, result_payload)
        self.assertEqual(
            compute.call_args.kwargs['symbols'],
            list(frozen_symbols),
        )
        self.assertTrue(
            compute.call_args.kwargs['authoritative_scope']
        )
        record.assert_called_once_with(
            market_date='2026-04-29',
            periods=('1d', '5d', '20d'),
            repair_started_at=ANY,
            rebuild_completed_at=ANY,
            exact_symbols=list(frozen_symbols),
        )


class FakeTrendingStocksService:
    def __init__(self):
        self.last_args = None

    def get_trending_stocks(self, **kwargs):
        self.last_args = kwargs
        return {
            'request': {
                'market_date': kwargs['market_date'].isoformat() if kwargs['market_date'] else None,
                'resolved_market_date': '2026-04-10',
                'period': kwargs['period'],
                'direction': kwargs['direction'],
                'limit': kwargs['limit'],
                'markets': kwargs['markets'],
            },
            'summary': {'count': 1},
            'items': [{'rank': 1, 'symbol': '005930'}],
        }


class ErrorTrendingStocksService:
    def get_trending_stocks(self, **kwargs):
        raise ValueError('direction must be one of up, down, or both')


class TrendingStocksRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(routes_scanner.router)

    def test_trending_stocks_route_calls_service(self):
        fake_service = FakeTrendingStocksService()
        original_service = routes_scanner.trending_stocks_service
        routes_scanner.trending_stocks_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/scanner/trending-stocks',
                params=[
                    ('market_date', '2026-04-10'),
                    ('period', '1d'),
                    ('direction', 'down'),
                    ('limit', '5'),
                    ('markets', 'KOSPI'),
                    ('markets', 'KOSDAQ'),
                ],
            )
        finally:
            routes_scanner.trending_stocks_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.last_args['market_date'], date(2026, 4, 10))
        self.assertEqual(fake_service.last_args['period'], '1d')
        self.assertEqual(fake_service.last_args['direction'], 'down')
        self.assertEqual(fake_service.last_args['limit'], 5)
        self.assertEqual(fake_service.last_args['markets'], ['KOSPI', 'KOSDAQ'])
        self.assertEqual(response.json()['items'][0]['symbol'], '005930')

    def test_trending_stocks_route_maps_validation_error_to_400(self):
        original_service = routes_scanner.trending_stocks_service
        routes_scanner.trending_stocks_service = ErrorTrendingStocksService()
        try:
            client = TestClient(self.app)
            response = client.get('/scanner/trending-stocks')
        finally:
            routes_scanner.trending_stocks_service = original_service

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'direction must be one of up, down, or both')
