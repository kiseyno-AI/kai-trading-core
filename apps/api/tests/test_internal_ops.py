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
from app.signals.advanced_value_factor import ADVANCED_VALUE_FACTOR_SOURCE
from app.signals.beta_factor import BETA_FACTOR_SOURCE
from app.signals.composite_factor import COMPOSITE_FACTOR_SOURCE
from app.signals.low_risk_factor import LOW_RISK_SOURCE
from app.signals.market_risk_filter import MARKET_RISK_FILTER_SOURCE
from app.signals.market_risk_snapshot import MARKET_RISK_SNAPSHOT_SOURCE
from app.signals.unified_value_factor import UNIFIED_VALUE_FACTOR_SOURCE
from app.signals.quality_factor import QUALITY_FACTOR_SOURCE
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

    def test_run_next_open_execution_if_due_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.signal_decider_next_open_execution_service.run_market_open_execution_if_due',
                new=AsyncMock(return_value={'summary': {'triggered': True, 'selected_count': 1}}),
            ) as run_execution,
        ):
            result = ops.run_next_open_execution_if_due(
                target_trade_date='2026-04-30',
                dry_run=False,
                force=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result['summary']['selected_count'], 1)
        self.assertEqual(run_execution.call_args.kwargs['target_trade_date'], '2026-04-30')
        self.assertEqual(run_execution.call_args.kwargs['dry_run'], False)
        self.assertEqual(run_execution.call_args.kwargs['trigger'], 'internal_ops_cli')
        self.assertEqual(run_execution.call_args.kwargs['force'], True)

    def test_run_intraday_auto_sell_if_due_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.signal_decider_intraday_auto_sell_service.run_intraday_auto_sell_if_due',
                new=AsyncMock(return_value={'summary': {'triggered': True, 'selected_count': 1}}),
            ) as run_execution,
        ):
            result = ops.run_intraday_auto_sell_if_due(
                account_reference='KIS:12345678-01',
                symbol='005930',
                dry_run=False,
                force=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result['summary']['selected_count'], 1)
        self.assertEqual(run_execution.call_args.kwargs['account_reference'], 'KIS:12345678-01')
        self.assertEqual(run_execution.call_args.kwargs['symbol'], '005930')
        self.assertEqual(run_execution.call_args.kwargs['dry_run'], False)
        self.assertEqual(run_execution.call_args.kwargs['trigger'], 'internal_ops_cli')
        self.assertEqual(run_execution.call_args.kwargs['force'], True)

    def test_run_next_open_execution_if_due_delegates_to_exec_when_enabled(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch('app.internal.ops.signal_decider_exec_integration_service.should_delegate_execution', return_value=True),
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.dispatch_next_open_plan',
                new=AsyncMock(return_value={'summary': {'delegated_to_exec': True, 'submitted_count': 1}}),
            ) as dispatch_exec,
        ):
            result = ops.run_next_open_execution_if_due(
                target_trade_date='2026-04-30',
                dry_run=False,
                force=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertTrue(result['summary']['delegated_to_exec'])
        self.assertEqual(dispatch_exec.call_args.kwargs['target_trade_date'], '2026-04-30')
        self.assertTrue(dispatch_exec.call_args.kwargs['refresh_plan'])
        self.assertEqual(dispatch_exec.call_args.kwargs['dry_run'], False)
        self.assertEqual(dispatch_exec.call_args.kwargs['trigger'], 'internal_ops_cli')
        self.assertEqual(dispatch_exec.call_args.kwargs['force'], True)

    def test_run_intraday_auto_sell_if_due_delegates_to_exec_when_enabled(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch('app.internal.ops.signal_decider_exec_integration_service.should_delegate_execution', return_value=True),
            patch(
                'app.internal.ops.signal_decider_exec_integration_service.list_intraday_exec_watch',
                new=AsyncMock(return_value={'summary': {'delegated_to_exec': True, 'items_total': 1}}),
            ) as delegated_watch,
        ):
            result = ops.run_intraday_auto_sell_if_due(
                account_reference='KIS:12345678-01',
                symbol='005930',
                dry_run=False,
                force=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertTrue(result['summary']['delegated_to_exec'])
        self.assertEqual(delegated_watch.call_args.kwargs['account_reference'], 'KIS:12345678-01')
        self.assertEqual(delegated_watch.call_args.kwargs['symbol'], '005930')
        self.assertTrue(delegated_watch.call_args.kwargs['refresh'])
        self.assertEqual(delegated_watch.call_args.kwargs['trigger'], 'internal_ops_cli')

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

    def test_list_daily_pipeline_stages_uses_stage_service(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_pipeline_stage_service.describe',
                return_value={'stage_order': ['price_collection']},
            ) as describe_stages,
        ):
            result = ops.list_daily_pipeline_stages()

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'stage_order': ['price_collection']})
        describe_stages.assert_called_once_with()

    def test_run_daily_pipeline_stages_uses_stage_service(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_pipeline_stage_service.run',
                return_value={'resolved_stages': ['technical_snapshots']},
            ) as run_stages,
        ):
            result = ops.run_daily_pipeline_stages(
                market_date='2026-04-14',
                stages='technical_snapshots,signal_decider',
                symbols='005930,000660',
                markets='KOSPI,KOSDAQ',
                force=True,
                fs_div='CFS',
                symbol_limit=25,
                symbol_offset=5,
                capture_limit=100,
                retry_cycles=2,
                feedback_max_cycles=4,
                feedback_outer_batch_size=12,
                reuse_existing_feedback=False,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'resolved_stages': ['technical_snapshots']})
        self.assertEqual(run_stages.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(run_stages.call_args.kwargs['stages'], ['technical_snapshots', 'signal_decider'])
        self.assertEqual(run_stages.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(run_stages.call_args.kwargs['markets'], ['KOSPI', 'KOSDAQ'])
        self.assertEqual(run_stages.call_args.kwargs['feedback_max_cycles'], 4)
        self.assertFalse(run_stages.call_args.kwargs['reuse_existing_feedback'])

    def test_compute_signal_decider_evaluations_uses_direct_service_call(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.signal_decider_loop_service.compute_recent_evaluations',
                return_value={'summary': {'evaluations_upserted': 4}},
            ) as compute_evaluations,
        ):
            result = ops.compute_signal_decider_evaluations(
                market_date='2026-04-30',
                lookback_days=21,
                symbols='005930,000660',
                horizons='1,5,10',
                evaluation_version='signal_decider_eval_v1',
                force_recompute=True,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'summary': {'evaluations_upserted': 4}})
        self.assertEqual(compute_evaluations.call_args.kwargs['as_of_market_date'], '2026-04-30')
        self.assertEqual(compute_evaluations.call_args.kwargs['lookback_days'], 21)
        self.assertEqual(compute_evaluations.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_evaluations.call_args.kwargs['horizons'], [1, 5, 10])
        self.assertEqual(compute_evaluations.call_args.kwargs['evaluation_version'], 'signal_decider_eval_v1')
        self.assertTrue(compute_evaluations.call_args.kwargs['force_recompute'])

    def test_run_agent_feedback_retry_cycles_passes_explicit_symbols_to_retry_service(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.agent_feedback_retry_service.run_retry_cycles',
                return_value={'completed': True, 'final_pending': 0},
            ) as run_retry_cycles,
        ):
            result = ops.run_agent_feedback_retry_cycles(
                market_date='2026-04-14',
                symbols='005930,000660',
                roles='technical-summarizer',
                max_cycles=2,
                outer_batch_size=10,
                reuse_existing=False,
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'completed': True, 'final_pending': 0})
        config = run_retry_cycles.call_args.kwargs['config']
        self.assertEqual(config.market_date.isoformat(), '2026-04-14')
        self.assertEqual(config.symbols, ('005930', '000660'))
        self.assertEqual(config.roles, ('technical-summarizer',))
        self.assertEqual(config.max_cycles, 2)
        self.assertEqual(config.outer_batch_size, 10)
        self.assertFalse(config.reuse_existing)

    def test_compute_market_risk_filters_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_market_risk_filter_service.rebuild_for_market_date',
                new=AsyncMock(return_value={'results_persisted': 2}),
            ) as compute_market_risk,
        ):
            result = ops.compute_market_risk_filters(
                market_date='2026-04-14',
                symbols='005930,000660',
                source=MARKET_RISK_FILTER_SOURCE,
                market_code='UN',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_market_risk.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(compute_market_risk.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_market_risk.call_args.kwargs['source'], MARKET_RISK_FILTER_SOURCE)
        self.assertEqual(compute_market_risk.call_args.kwargs['market_code'], 'UN')

    def test_collect_market_risk_snapshots_uses_snapshot_service(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_market_risk_snapshot_service.collect_for_market_date',
                new=AsyncMock(return_value={'snapshots_persisted': 2}),
            ) as collect_snapshots,
        ):
            result = ops.collect_market_risk_snapshots(
                market_date='2026-04-14',
                symbols='005930,000660',
                source=MARKET_RISK_SNAPSHOT_SOURCE,
                market_code='UN',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'snapshots_persisted': 2})
        self.assertEqual(collect_snapshots.call_args.kwargs['market_date'], '2026-04-14')
        self.assertEqual(collect_snapshots.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(collect_snapshots.call_args.kwargs['source'], MARKET_RISK_SNAPSHOT_SOURCE)
        self.assertEqual(collect_snapshots.call_args.kwargs['market_code'], 'UN')

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

    def test_compute_beta_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_beta_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 1},
            ) as compute_beta,
        ):
            result = ops.compute_beta_factors(
                market_date='2026-04-23',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 1})
        self.assertEqual(compute_beta.call_args.kwargs['market_date'], '2026-04-23')
        self.assertEqual(compute_beta.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_beta.call_args.kwargs['source'], BETA_FACTOR_SOURCE)

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

    def test_compute_advanced_value_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_advanced_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_advanced_value,
        ):
            result = ops.compute_advanced_value_factors(
                market_date='2026-04-24',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_advanced_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_advanced_value.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_advanced_value.call_args.kwargs['source'], ADVANCED_VALUE_FACTOR_SOURCE)

    def test_compute_advanced_value_factors_preserves_empty_symbol_subset(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_advanced_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 0},
            ) as compute_advanced_value,
        ):
            result = ops.compute_advanced_value_factors(
                market_date='2026-04-24',
                symbols=',',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 0})
        self.assertEqual(compute_advanced_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_advanced_value.call_args.kwargs['symbols'], [])
        self.assertEqual(compute_advanced_value.call_args.kwargs['source'], ADVANCED_VALUE_FACTOR_SOURCE)

    def test_compute_unified_value_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_unified_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_unified_value,
        ):
            result = ops.compute_unified_value_factors(
                market_date='2026-04-24',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_unified_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_unified_value.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_unified_value.call_args.kwargs['source'], UNIFIED_VALUE_FACTOR_SOURCE)

    def test_compute_unified_value_factors_preserves_empty_symbol_subset(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_unified_value_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 0},
            ) as compute_unified_value,
        ):
            result = ops.compute_unified_value_factors(
                market_date='2026-04-24',
                symbols=',',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 0})
        self.assertEqual(compute_unified_value.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_unified_value.call_args.kwargs['symbols'], [])
        self.assertEqual(compute_unified_value.call_args.kwargs['source'], UNIFIED_VALUE_FACTOR_SOURCE)

    def test_compute_quality_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_quality_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_quality,
        ):
            result = ops.compute_quality_factors(
                market_date='2026-04-24',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_quality.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_quality.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_quality.call_args.kwargs['source'], QUALITY_FACTOR_SOURCE)

    def test_compute_quality_factors_preserves_empty_symbol_subset(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_quality_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 0},
            ) as compute_quality,
        ):
            result = ops.compute_quality_factors(
                market_date='2026-04-24',
                symbols=',',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 0})
        self.assertEqual(compute_quality.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_quality.call_args.kwargs['symbols'], [])
        self.assertEqual(compute_quality.call_args.kwargs['source'], QUALITY_FACTOR_SOURCE)

    def test_compute_composite_factors_uses_canonical_feature_version(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_composite_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 2},
            ) as compute_composite,
        ):
            result = ops.compute_composite_factors(
                market_date='2026-04-24',
                symbols='005930,000660',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 2})
        self.assertEqual(compute_composite.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_composite.call_args.kwargs['symbols'], ['005930', '000660'])
        self.assertEqual(compute_composite.call_args.kwargs['source'], COMPOSITE_FACTOR_SOURCE)

    def test_compute_composite_factors_preserves_empty_symbol_subset(self) -> None:
        with (
            patch('app.internal.ops.bootstrap_runtime') as bootstrap_runtime,
            patch(
                'app.internal.ops.daily_composite_factor_service.rebuild_for_market_date',
                return_value={'results_persisted': 0},
            ) as compute_composite,
        ):
            result = ops.compute_composite_factors(
                market_date='2026-04-24',
                symbols=',',
            )

        bootstrap_runtime.assert_called_once_with()
        self.assertEqual(result, {'results_persisted': 0})
        self.assertEqual(compute_composite.call_args.kwargs['market_date'], '2026-04-24')
        self.assertEqual(compute_composite.call_args.kwargs['symbols'], [])
        self.assertEqual(compute_composite.call_args.kwargs['source'], COMPOSITE_FACTOR_SOURCE)

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

    def test_cli_signal_decider_evaluations_outputs_json(self) -> None:
        stdout = io.StringIO()
        with (
            patch(
                'app.internal.ops.compute_signal_decider_evaluations',
                return_value={'summary': {'evaluations_upserted': 6}},
            ),
            redirect_stdout(stdout),
        ):
            exit_code = ops.main(
                [
                    'agent-signal-decider-evaluations-compute',
                    '--market-date',
                    '2026-04-30',
                    '--symbols',
                    '005930,000660',
                    '--horizons',
                    '1,5',
                    '--force-recompute',
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {'summary': {'evaluations_upserted': 6}})
