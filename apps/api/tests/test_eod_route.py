
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_eod


class FakeFreshnessService:
    def read(self):
        return {
            'today': {
                'trade_date': '2026-04-10',
                'price': {
                    'status': 'ready',
                    'symbols_total': 3,
                    'symbols_succeeded': 3,
                    'symbols_failed': 0,
                },
                'technical_analysis': {
                    'status': 'ready',
                    'symbols_total': 3,
                    'symbols_succeeded': 3,
                    'symbols_failed': 0,
                },
                'action_summary': {
                    'status': 'ready',
                    'symbols_total': 3,
                    'symbols_succeeded': 3,
                    'symbols_failed': 0,
                },
                'fundamentals': {
                    'status': 'ready',
                    'symbols_total': 1,
                    'symbols_succeeded': 1,
                    'symbols_failed': 0,
                },
            },
            'latest': {
                'trade_date': '2026-04-10',
                'price_trade_date': '2026-04-10',
                'eod_status': 'completed',
                'updated_at': '2026-04-10T07:31:55+00:00',
                'succeeded': 3,
                'failed': 0,
                'total': 3,
                'summary': {
                    'basis': 'latest_eod_run_symbol_universe',
                    'updated_at': '2026-04-10T07:31:55+00:00',
                    'succeeded': 3,
                    'failed': 0,
                    'total': 3,
                },
                'symbols_total': 3,
                'symbols_succeeded': 3,
                'symbols_failed': 0,
                'technical_market_date': '2026-04-10',
                'action_market_date': '2026-04-10',
            },
        }


class FakeTechnicalSnapshotService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'snapshots_persisted': 2,
            'snapshots_failed': 0,
            'items': [{'symbol': '005930'}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'entries': 1},
            'items': [{'symbol': '005930', 'trend_state': 'uptrend'}],
        }


class FakeActionSummaryService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'action_summaries_persisted': 2,
            'action_summaries_failed': 0,
            'items': [{'symbol': '005930', 'action_label': 'long_breakout'}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'entries': 1},
            'items': [{'symbol': '005930', 'action_label': 'long_breakout'}],
        }


class FakeSignalFeatureService:
    def __init__(self):
        self.collect_args = None
        self.inspect_args = None

    async def refresh_for_market_date(self, **kwargs):
        self.collect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'features_persisted': 2,
            'features_failed': 0,
            'items': [{'symbol': '005930', 'signal_count': 2}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'entries': 1},
            'items': [{'symbol': '005930', 'signal_count': 2}],
        }


class FakeMomentumFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'momentum_rank': 1}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'entries': 1, 'eligible_symbols': 1},
            'items': [{'symbol': '005930', 'momentum_rank': 1, 'is_momentum_eligible': True}],
        }


class FakeValueFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'value_score': 0.86}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'value_factor_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'scored_count': 1,
                    'invalid_per_count': 0,
                    'missing_metric_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'value_score': 0.91,
                    'value_status': 'scored',
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'value_factor_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'scored_count': 1,
                'invalid_per_count': 1,
                'missing_metric_count': 0,
            },
            'items': [
                {'symbol': '005930', 'market_date': market_date, 'value_score': 0.91, 'value_status': 'scored'},
                {'symbol': '000660', 'market_date': market_date, 'value_score': None, 'value_status': 'invalid_per'},
            ],
        }


class FakeAdvancedValueFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'advanced_value_score': 0.84}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'advanced_value_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'eligible_count': 1,
                    'watchlist_count': 0,
                    'ineligible_count': 0,
                    'insufficient_components_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'advanced_value_score': 0.91,
                    'advanced_value_classification': 'eligible',
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'advanced_value_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'eligible_count': 1,
                'watchlist_count': 1,
                'ineligible_count': 0,
                'insufficient_components_count': 0,
            },
            'items': [
                {
                    'symbol': '005930',
                    'market_date': market_date,
                    'advanced_value_score': 0.91,
                    'advanced_value_classification': 'eligible',
                },
                {
                    'symbol': '000660',
                    'market_date': market_date,
                    'advanced_value_score': 0.50,
                    'advanced_value_classification': 'watchlist',
                },
            ],
        }


class FakePiotroskiFilterService:
    def __init__(self):
        self.inspect_args = None

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        resolved_market_date = market_date or '2026-04-23'
        latest_mode = market_date is None
        is_latest_fallback = market_date == '2026-04-24'
        if is_latest_fallback:
            resolved_market_date = '2026-04-23'
        if symbol:
            return {
                'requested_market_date': market_date,
                'resolved_market_date': resolved_market_date,
                'market_date': resolved_market_date,
                'latest_mode': latest_mode,
                'is_latest_fallback': is_latest_fallback,
                'fallback_policy': 'latest_on_or_before_requested_market_date',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'passed_count': 1,
                    'not_passed_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': resolved_market_date,
                    'passed_filter': True,
                    'piotroski_score': 8,
                },
            }
        return {
            'requested_market_date': market_date,
            'resolved_market_date': resolved_market_date,
            'market_date': resolved_market_date,
            'latest_mode': latest_mode,
            'is_latest_fallback': is_latest_fallback,
            'fallback_policy': 'latest_on_or_before_requested_market_date',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'passed_count': 1,
                'not_passed_count': 1,
            },
            'items': [
                {'symbol': '005930', 'market_date': resolved_market_date, 'passed_filter': True, 'piotroski_score': 8},
                {'symbol': '000660', 'market_date': resolved_market_date, 'passed_filter': False, 'piotroski_score': 5},
            ],
        }


class FakeLowRiskFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'low_risk_score': 0.86}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'low_risk_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'eligible_count': 1,
                    'liquidity_floor_pass_count': 1,
                    'insufficient_history_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'low_risk_score': 0.91,
                    'is_low_risk_eligible': True,
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'low_risk_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'eligible_count': 1,
                'liquidity_floor_pass_count': 2,
                'insufficient_history_count': 0,
            },
            'items': [
                {'symbol': '005930', 'market_date': market_date, 'low_risk_score': 0.91, 'is_low_risk_eligible': True},
                {'symbol': '000660', 'market_date': market_date, 'low_risk_score': 0.42, 'is_low_risk_eligible': False},
            ],
        }


class FakeBetaFactorService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def rebuild_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'feature_version': kwargs['source'],
            'results_persisted': 2,
            'results_failed': 0,
            'items': [{'symbol': '005930', 'low_beta_score': 0.84}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        market_date = kwargs['market_date']
        symbol = kwargs.get('symbol')
        if symbol:
            return {
                'market_date': market_date,
                'feature_version': 'beta_v1',
                'summary': {
                    'entries': 1,
                    'requested_symbol': symbol,
                    'matched_symbols': [symbol],
                    'missing_symbols': [],
                    'eligible_count': 1,
                    'watchlist_count': 0,
                    'insufficient_history_count': 0,
                },
                'item': {
                    'symbol': symbol,
                    'market_date': market_date,
                    'benchmark_id': 'KOSPI_EQW',
                    'low_beta_score': 0.91,
                    'is_low_beta_eligible': True,
                },
            }
        return {
            'market_date': market_date,
            'feature_version': 'beta_v1',
            'summary': {
                'entries': 2,
                'requested_symbol': None,
                'matched_symbols': ['005930', '000660'],
                'missing_symbols': [],
                'eligible_count': 1,
                'watchlist_count': 1,
                'insufficient_history_count': 0,
            },
            'items': [
                {'symbol': '005930', 'market_date': market_date, 'low_beta_score': 0.91, 'is_low_beta_eligible': True},
                {'symbol': '000660', 'market_date': market_date, 'low_beta_score': 0.55, 'is_low_beta_eligible': False},
            ],
        }


class FakePipelineStatusService:
    def __init__(self):
        self.read_args = None

    def read(self, **kwargs):
        self.read_args = kwargs
        return {
            'market_date': kwargs.get('market_date') or '2026-04-21',
            'fs_div': kwargs.get('fs_div') or 'CFS',
            'status': 'running',
            'stage_order': ['price_collection', 'technical_snapshots', 'fundamentals_daily_refresh'],
            'stages': {
                'price_collection': {
                    'status': 'completed',
                    'source_run_ids': {'eod_run_id': 101},
                },
                'technical_snapshots': {
                    'status': 'running',
                    'current_activity': 'Computing daily technical snapshots for 000660',
                },
                'fundamentals_daily_refresh': {
                    'status': 'pending',
                    'source_run_ids': {},
                },
            },
        }


class EODRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(routes_eod.router)

    def test_get_eod_freshness_route_returns_service_payload(self):
        fake_service = FakeFreshnessService()
        original_service = routes_eod.eod_freshness_service
        routes_eod.eod_freshness_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/eod/freshness')
        finally:
            routes_eod.eod_freshness_service = original_service

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['today']['trade_date'], '2026-04-10')
        self.assertEqual(payload['today']['price']['status'], 'ready')
        self.assertEqual(payload['today']['technical_analysis']['symbols_succeeded'], 3)
        self.assertEqual(payload['today']['fundamentals']['symbols_total'], 1)
        self.assertEqual(payload['latest']['trade_date'], '2026-04-10')
        self.assertEqual(payload['latest']['updated_at'], '2026-04-10T07:31:55+00:00')
        self.assertEqual(payload['latest']['total'], 3)
        self.assertEqual(payload['latest']['summary']['basis'], 'latest_eod_run_symbol_universe')
        self.assertEqual(payload['latest']['symbols_succeeded'], 3)
        self.assertEqual(payload['latest']['action_market_date'], '2026-04-10')

    def test_get_eod_pipeline_status_route_returns_service_payload(self):
        fake_service = FakePipelineStatusService()
        original_service = routes_eod.eod_pipeline_status_service
        routes_eod.eod_pipeline_status_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/pipeline/status',
                params={'market_date': '2026-04-21', 'fs_div': 'OFS'},
            )
        finally:
            routes_eod.eod_pipeline_status_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.read_args, {'market_date': '2026-04-21', 'fs_div': 'OFS'})
        payload = response.json()
        self.assertEqual(payload['status'], 'running')
        self.assertEqual(payload['stages']['price_collection']['source_run_ids']['eod_run_id'], 101)
        self.assertEqual(
            payload['stages']['technical_snapshots']['current_activity'],
            'Computing daily technical snapshots for 000660',
        )

    def test_compute_technical_snapshots_route_calls_service(self):
        fake_service = FakeTechnicalSnapshotService()
        original_service = routes_eod.daily_technical_snapshot_service
        routes_eod.daily_technical_snapshot_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/technical-snapshots/compute',
                params={'market_date': '2026-04-07', 'symbols': '005930,000660'},
            )
        finally:
            routes_eod.daily_technical_snapshot_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(response.json()['snapshots_persisted'], 2)

    def test_inspect_technical_snapshots_route_calls_service(self):
        fake_service = FakeTechnicalSnapshotService()
        original_service = routes_eod.daily_technical_snapshot_service
        routes_eod.daily_technical_snapshot_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/technical-snapshots',
                params={'market_date': '2026-04-07', 'symbols': '005930', 'limit': 5},
            )
        finally:
            routes_eod.daily_technical_snapshot_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['symbols'], ['005930'])
        self.assertEqual(fake_service.inspect_args['limit'], 5)
        self.assertEqual(response.json()['items'][0]['trend_state'], 'uptrend')

    def test_compute_action_summaries_route_calls_service(self):
        fake_service = FakeActionSummaryService()
        original_service = routes_eod.daily_action_summary_service
        routes_eod.daily_action_summary_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/action-summaries/compute',
                params={'market_date': '2026-04-07', 'symbols': '005930,000660'},
            )
        finally:
            routes_eod.daily_action_summary_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(response.json()['action_summaries_persisted'], 2)

    def test_inspect_action_summaries_route_calls_service(self):
        fake_service = FakeActionSummaryService()
        original_service = routes_eod.daily_action_summary_service
        routes_eod.daily_action_summary_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/action-summaries',
                params={'market_date': '2026-04-07', 'symbols': '005930', 'limit': 5},
            )
        finally:
            routes_eod.daily_action_summary_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['symbols'], ['005930'])
        self.assertEqual(fake_service.inspect_args['limit'], 5)
        self.assertEqual(response.json()['items'][0]['action_label'], 'long_breakout')

    def test_collect_signal_features_route_calls_service(self):
        fake_service = FakeSignalFeatureService()
        original_service = routes_eod.daily_signal_feature_service
        routes_eod.daily_signal_feature_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/signal-features/collect',
                params={
                    'market_date': '2026-04-14',
                    'sources': 'volume-power,overtime-volume',
                    'max_pages': 2,
                },
            )
        finally:
            routes_eod.daily_signal_feature_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.collect_args['sources'], ['volume-power', 'overtime-volume'])
        self.assertEqual(fake_service.collect_args['max_pages'], 2)
        self.assertEqual(response.json()['features_persisted'], 2)

    def test_inspect_signal_features_route_calls_service(self):
        fake_service = FakeSignalFeatureService()
        original_service = routes_eod.daily_signal_feature_service
        routes_eod.daily_signal_feature_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/signal-features',
                params={'market_date': '2026-04-14', 'symbols': '005930', 'limit': 5},
            )
        finally:
            routes_eod.daily_signal_feature_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['symbols'], ['005930'])
        self.assertEqual(fake_service.inspect_args['limit'], 5)
        self.assertEqual(response.json()['items'][0]['signal_count'], 2)

    def test_compute_low_risk_factors_route_calls_service(self):
        fake_service = FakeLowRiskFactorService()
        original_service = routes_eod.daily_low_risk_factor_service
        routes_eod.daily_low_risk_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/low-risk-factors/compute',
                params={
                    'market_date': '2026-04-23',
                    'symbols': '005930,000660',
                    'source': 'low_risk_v1',
                },
            )
        finally:
            routes_eod.daily_low_risk_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-23')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'low_risk_v1')
        self.assertEqual(response.json()['items'][0]['low_risk_score'], 0.86)

    def test_inspect_low_risk_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeLowRiskFactorService()
        original_service = routes_eod.daily_low_risk_factor_service
        routes_eod.daily_low_risk_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/low-risk-factors',
                params={'market_date': '2026-04-23', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_low_risk_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-23')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['low_risk_score'], 0.91)
        self.assertTrue(response.json()['item']['is_low_risk_eligible'])

    def test_compute_beta_factors_route_calls_service(self):
        fake_service = FakeBetaFactorService()
        original_service = routes_eod.daily_beta_factor_service
        routes_eod.daily_beta_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/beta-factors/compute',
                params={
                    'market_date': '2026-04-23',
                    'symbols': '005930,000660',
                    'source': 'beta_v1',
                },
            )
        finally:
            routes_eod.daily_beta_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-23')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'beta_v1')
        self.assertEqual(response.json()['items'][0]['low_beta_score'], 0.84)

    def test_inspect_beta_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeBetaFactorService()
        original_service = routes_eod.daily_beta_factor_service
        routes_eod.daily_beta_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/beta-factors',
                params={'market_date': '2026-04-23', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_beta_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-23')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['benchmark_id'], 'KOSPI_EQW')
        self.assertTrue(response.json()['item']['is_low_beta_eligible'])

    def test_compute_momentum_factors_route_calls_service(self):
        fake_service = FakeMomentumFactorService()
        original_service = routes_eod.daily_momentum_factor_service
        routes_eod.daily_momentum_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/momentum-factors/compute',
                params={'market_date': '2026-04-14', 'symbols': '005930,000660'},
            )
        finally:
            routes_eod.daily_momentum_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(response.json()['results_persisted'], 2)

    def test_inspect_momentum_factors_route_calls_service(self):
        fake_service = FakeMomentumFactorService()
        original_service = routes_eod.daily_momentum_factor_service
        routes_eod.daily_momentum_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/momentum-factors',
                params={'market_date': '2026-04-14', 'symbols': '005930', 'limit': 5},
            )
        finally:
            routes_eod.daily_momentum_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['symbols'], ['005930'])
        self.assertEqual(fake_service.inspect_args['limit'], 5)
        self.assertEqual(response.json()['summary']['eligible_symbols'], 1)
        self.assertEqual(response.json()['items'][0]['momentum_rank'], 1)

    def test_compute_value_factors_route_calls_service(self):
        fake_service = FakeValueFactorService()
        original_service = routes_eod.daily_value_factor_service
        routes_eod.daily_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/value-factors/compute',
                params={
                    'market_date': '2026-04-24',
                    'symbols': '005930,000660',
                    'source': 'value_factor_v1',
                },
            )
        finally:
            routes_eod.daily_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'value_factor_v1')
        self.assertEqual(response.json()['items'][0]['value_score'], 0.86)

    def test_compute_value_factors_route_preserves_empty_symbol_subset(self):
        fake_service = FakeValueFactorService()
        original_service = routes_eod.daily_value_factor_service
        routes_eod.daily_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/value-factors/compute',
                params={
                    'market_date': '2026-04-24',
                    'symbols': ',',
                    'source': 'value_factor_v1',
                },
            )
        finally:
            routes_eod.daily_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.compute_args['symbols'], [])
        self.assertEqual(fake_service.compute_args['source'], 'value_factor_v1')

    def test_inspect_value_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeValueFactorService()
        original_service = routes_eod.daily_value_factor_service
        routes_eod.daily_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/value-factors',
                params={'market_date': '2026-04-24', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['value_score'], 0.91)
        self.assertEqual(response.json()['item']['value_status'], 'scored')

    def test_compute_advanced_value_factors_route_calls_service(self):
        fake_service = FakeAdvancedValueFactorService()
        original_service = routes_eod.daily_advanced_value_factor_service
        routes_eod.daily_advanced_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/advanced-value-factors/compute',
                params={
                    'market_date': '2026-04-24',
                    'symbols': '005930,000660',
                    'source': 'advanced_value_v1',
                },
            )
        finally:
            routes_eod.daily_advanced_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(fake_service.compute_args['source'], 'advanced_value_v1')
        self.assertEqual(response.json()['items'][0]['advanced_value_score'], 0.84)

    def test_inspect_advanced_value_factors_route_returns_single_item_for_symbol(self):
        fake_service = FakeAdvancedValueFactorService()
        original_service = routes_eod.daily_advanced_value_factor_service
        routes_eod.daily_advanced_value_factor_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/advanced-value-factors',
                params={'market_date': '2026-04-24', 'symbol': '005930'},
            )
        finally:
            routes_eod.daily_advanced_value_factor_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['market_date'], '2026-04-24')
        self.assertEqual(fake_service.inspect_args['symbol'], '005930')
        self.assertEqual(response.json()['item']['advanced_value_score'], 0.91)
        self.assertEqual(response.json()['item']['advanced_value_classification'], 'eligible')

    def test_inspect_piotroski_filter_results_route_returns_all_items(self):
        fake_service = FakePiotroskiFilterService()
        original_service = routes_eod.daily_piotroski_filter_service
        routes_eod.daily_piotroski_filter_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/eod/piotroski-filter-results', params={'market_date': '2026-04-14'})
        finally:
            routes_eod.daily_piotroski_filter_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.inspect_args,
            {'market_date': '2026-04-14', 'symbol': None, 'limit': None},
        )
        self.assertEqual(response.json()['requested_market_date'], '2026-04-14')
        self.assertEqual(response.json()['resolved_market_date'], '2026-04-14')
        self.assertFalse(response.json()['latest_mode'])
        self.assertFalse(response.json()['is_latest_fallback'])
        self.assertEqual(response.json()['summary']['entries'], 2)
        self.assertEqual(response.json()['summary']['passed_count'], 1)
        self.assertEqual(response.json()['items'][0]['symbol'], '005930')

    def test_inspect_piotroski_filter_results_route_returns_single_item_for_symbol(self):
        fake_service = FakePiotroskiFilterService()
        original_service = routes_eod.daily_piotroski_filter_service
        routes_eod.daily_piotroski_filter_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/piotroski-filter-results',
                params={'market_date': '2026-04-14', 'symbol': '005930', 'limit': 10},
            )
        finally:
            routes_eod.daily_piotroski_filter_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.inspect_args,
            {'market_date': '2026-04-14', 'symbol': '005930', 'limit': 10},
        )
        self.assertEqual(response.json()['summary']['requested_symbol'], '005930')
        self.assertEqual(response.json()['resolved_market_date'], '2026-04-14')
        self.assertEqual(response.json()['item']['piotroski_score'], 8)

    def test_inspect_piotroski_filter_results_route_allows_latest_mode_without_market_date(self):
        fake_service = FakePiotroskiFilterService()
        original_service = routes_eod.daily_piotroski_filter_service
        routes_eod.daily_piotroski_filter_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/eod/piotroski-filter-results')
        finally:
            routes_eod.daily_piotroski_filter_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            fake_service.inspect_args,
            {'market_date': None, 'symbol': None, 'limit': None},
        )
        self.assertIsNone(response.json()['requested_market_date'])
        self.assertEqual(response.json()['resolved_market_date'], '2026-04-23')
        self.assertTrue(response.json()['latest_mode'])
        self.assertFalse(response.json()['is_latest_fallback'])
        self.assertEqual(response.json()['market_date'], '2026-04-23')
