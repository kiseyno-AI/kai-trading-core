import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI

from app.core.settings import settings
from app.internal import ops
from app.internal.http_client import InternalAPIClient
from app.main import audit_and_require_upstream_bearer_token
from app.signals.low_risk_factor import LOW_RISK_SOURCE
from app.signals.value_factor import VALUE_FACTOR_SOURCE


class InternalAPIClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_attaches_bearer_token_for_internal_http_calls(self) -> None:
        original_token = settings.upstream_bearer_token
        settings.upstream_bearer_token = 'internal-secret'

        app = FastAPI()
        app.middleware('http')(audit_and_require_upstream_bearer_token)

        @app.post('/eod/pykrx/daily/run-if-due')
        async def run_if_due() -> dict:
            return {'ok': True}

        transport = httpx.ASGITransport(app=app)
        client = InternalAPIClient(
            base_url='http://testserver',
            bearer_token=settings.upstream_bearer_token,
            transport=transport,
        )

        try:
            response = await client.run_pykrx_daily_if_due(market_date='2026-04-08', force=True)
        finally:
            settings.upstream_bearer_token = original_token

        self.assertEqual(response, {'ok': True})

    async def test_client_preserves_query_params_for_daily_run(self) -> None:
        captured_request: httpx.Request | None = None

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_request
            captured_request = request
            return httpx.Response(200, json={'triggered': True}, request=request)

        transport = httpx.MockTransport(handler)
        client = InternalAPIClient(
            base_url='http://127.0.0.1:8000',
            bearer_token='internal-secret',
            transport=transport,
        )

        response = await client.run_pykrx_daily_if_due(
            market_date='2026-04-08',
            markets='KOSPI,KOSDAQ',
            force=True,
            refresh_universe=False,
        )

        self.assertEqual(response['triggered'], True)
        assert captured_request is not None
        self.assertEqual(captured_request.headers['authorization'], 'Bearer internal-secret')
        self.assertEqual(captured_request.url.params['market_date'], '2026-04-08')
        self.assertEqual(captured_request.url.params['markets'], 'KOSPI,KOSDAQ')
        self.assertEqual(captured_request.url.params['force'], 'true')
        self.assertEqual(captured_request.url.params['refresh_universe'], 'false')


class InternalOpsTests(unittest.TestCase):
    def test_run_pykrx_daily_if_due_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch('app.internal.ops.PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due', return_value={'triggered': True}) as run_daily,
        ):
            result = ops.run_pykrx_daily_if_due(market_date='2026-04-08', force=True)

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'triggered': True})
        self.assertEqual(run_daily.call_args.kwargs['market_date'], '2026-04-08')
        self.assertEqual(run_daily.call_args.kwargs['trigger'], 'internal_ops_cli')
        self.assertEqual(run_daily.call_args.kwargs['force'], True)

    def test_run_dart_fundamentals_daily_if_due_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.dart_fundamentals_capture_service.run_daily_incremental_refresh',
                new=AsyncMock(return_value={'triggered': True, 'run_id': 15}),
            ) as run_daily,
        ):
            result = ops.run_dart_fundamentals_daily_if_due(
                market_date='2026-04-14',
                symbol_limit=5,
                capture_limit=20,
                force=True,
                fs_div='CFS',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'triggered': True, 'run_id': 15})
        self.assertEqual(run_daily.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(run_daily.call_args.kwargs['symbol_limit'], 5)
        self.assertEqual(run_daily.call_args.kwargs['capture_limit'], 20)
        self.assertEqual(run_daily.call_args.kwargs['trigger'], 'internal_ops_cli')
        self.assertEqual(run_daily.call_args.kwargs['force'], True)

    def test_compute_trending_ranks_uses_direct_store_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ) as compute_ranks,
        ):
            result = ops.compute_trending_ranks(market_date='2026-04-10', periods='1d,20d')

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'trending_ranks_persisted': 3})
        self.assertEqual(compute_ranks.call_args.kwargs['market_date'], '2026-04-10')
        self.assertEqual(compute_ranks.call_args.kwargs['periods'], ['1d', '20d'])
        self.assertEqual(compute_ranks.call_args.kwargs['source'], 'internal_ops_cli')

    def test_collect_kis_daily_signal_features_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_signal_feature_service.refresh_for_market_date',
                new=AsyncMock(return_value={'features_persisted': 2}),
            ) as collect_features,
        ):
            result = ops.collect_kis_daily_signal_features(
                market_date='2026-04-14',
                sources='volume-power,overtime-volume',
                max_pages=2,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'features_persisted': 2})
        self.assertEqual(collect_features.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(collect_features.call_args.kwargs['sources'], ['volume-power', 'overtime-volume'])
        self.assertEqual(collect_features.call_args.kwargs['max_pages'], 2)

    def test_collect_daily_piotroski_filter_results_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_piotroski_filter_service.rebuild_for_market_date',
                return_value={'symbols_evaluated': 2},
            ) as collect_filter,
        ):
            result = ops.collect_daily_piotroski_filter_results(market_date='2026-04-14')

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'symbols_evaluated': 2})
        self.assertEqual(collect_filter.call_args.kwargs['market_date'], '2026-04-14')

    def test_compute_low_risk_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_low_risk_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 1},
            ) as compute_low_risk,
        ):
            result = ops.compute_low_risk_factors(
                market_date='2026-04-23',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 1})
        self.assertEqual(compute_low_risk.call_args.kwargs['market_date'], '2026-04-23')
        self.assertEqual(compute_low_risk.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_low_risk.call_args.kwargs['source'], LOW_RISK_SOURCE)

    def test_compute_momentum_factors_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_momentum_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_momentum,
        ):
            result = ops.compute_momentum_factors(
                market_date='2026-04-14',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_momentum.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(compute_momentum.call_args.kwargs['symbols'], ['005930', '000660'])

    def test_compute_value_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_value,
        ):
            result = ops.compute_value_factors(
                market_date='2026-04-24',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_value.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_value.call_args.kwargs['source'], VALUE_FACTOR_SOURCE)

    def test_compute_value_factors_preserves_empty_symbol_subset(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 0},
            ) as compute_value,
        ):
            result = ops.compute_value_factors(
                market_date='2026-04-24',
                symbols=',',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 0})
        self.assertEqual(compute_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_value.call_args.kwargs['symbols'], [])
        self.assertEqual(compute_value.call_args.kwargs['source'], VALUE_FACTOR_SOURCE)

    def test_cli_quotes_tracked_outputs_json(self) -> None:
        stdout = io.StringIO()
        with (
            patch('app.internal.ops.get_tracked_quotes', new=AsyncMock(return_value={'items': [{'symbol': '005930'}]})),
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(['quotes-tracked'])

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {'items': [{'symbol': '005930'}]})

    def test_cli_market_status_outputs_json(self) -> None:
        stdout = io.StringIO()
        with (
            patch('app.internal.ops.get_market_status', return_value={'state': 'CLOSED'}),
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(['market-status'])

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {'state': 'CLOSED'})
