from contextlib import ExitStack
from datetime import date
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import AsyncMock, patch

from app.eod.daily_pipeline_stages import DailyPipelineStageService
from app.eod.pykrx_backfill import (
    PykrxBackfillService, PykrxSymbol, _followup_stage_debt_terminal_cohort,
)
from app.eod.pykrx_pipeline_runner import FACTOR_FOLLOWUP_STAGE_ORDER
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome
from tests.test_auto_pykrx import (
    _AnySymbolTurnoverFrame,
    _atomic_exact_scope_proof,
    _point_in_time_universe_snapshot,
    _ready_pykrx_enrichment_result,
)


class PreopenDailyRecoveryTests(TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.market = SimpleNamespace(
            state='PREOPEN', is_open=False, is_trading_day=True,
            server_time='2026-04-06T06:30:00+09:00',
        )
        for module in ('pykrx_pipeline_runner', 'daily_pipeline_stages'):
            self.mock(f'app.eod.{module}.get_market_state', return_value=self.market)
        self.mock('app.eod.pykrx_pipeline_runner.previous_krx_trading_date',
                  return_value=date(2026, 4, 3))
        self.mock('app.eod.pykrx_backfill._current_krx_calendar_date',
                  return_value=date(2026, 4, 6))
        self.mock('app.eod.pykrx_backfill.settings.dart_api_key', 'test-key')
        self.mock('app.eod.pykrx_backfill.settings.kis_app_key', 'test-key')
        self.mock('app.eod.pykrx_backfill.settings.kis_app_secret', 'test-secret')

    def mock(self, target, *args, **kwargs):
        return self.stack.enter_context(patch(target, *args, **kwargs))

    def test_verified_stored_signal_cohort_survives_prior_session_continuation(self):
        successful = {stage: ['005930'] for stage in FACTOR_FOLLOWUP_STAGE_ORDER}
        successful['signal_features'] = ['005930', '005935']
        state = {'successful_by_stage': successful, 'pending_by_stage': {}}
        self.assertEqual(_followup_stage_debt_terminal_cohort(
            state, point_in_time_recovery=True,
        ), ('signal_features', ['005930', '005935']))
        del successful['signal_features']
        self.assertEqual(_followup_stage_debt_terminal_cohort(
            state, point_in_time_recovery=True,
        ), ('factor_intersection', ['005930']))

    def test_first_preopen_run_reaches_native_inputs_and_agents_without_live_rankings(self):
        self._run_first_preopen_pipeline(cross_open=False)

    def test_scheduled_preopen_signal_admission_survives_factor_work_crossing_open(self):
        self._run_first_preopen_pipeline(cross_open=True)

    def test_scheduled_signal_cannot_borrow_preopen_admission_from_retry_metadata(self):
        self._run_first_preopen_pipeline(cross_open=False, initial_open=True)

    def _run_first_preopen_pipeline(self, *, cross_open, initial_open=False):
        service = PykrxBackfillService()
        scope = ['005930']
        calls = []
        if initial_open:
            self.market.state = 'OPEN'
            self.market.is_open = True
            self.market.server_time = '2026-04-06T09:01:00+09:00'

        def completed(stage, result):
            def run(*args, **kwargs):
                calls.append(stage)
                if stage == 'factors' and cross_open:
                    self.market.state = 'OPEN'
                    self.market.is_open = True
                    self.market.server_time = '2026-04-06T09:01:00+09:00'
                return result
            return run

        def atomic(version):
            return {
                'status': 'completed', 'market_date': '2026-04-03',
                'gate_status': 'ready', 'results_persisted': 1, 'results_failed': 0,
                **_atomic_exact_scope_proof(
                    manifest_version=version, selected_symbols=scope,
                ),
            }

        self.stack.enter_context(patch.object(
            service, 'fetch_recent_universe_snapshot',
            return_value=_point_in_time_universe_snapshot(
                [PykrxSymbol('005930', 'KOSPI', '삼성전자')],
                market_date='2026-04-03',
            ),
        ))
        self.stack.enter_context(patch.object(service, 'resolve_latest_trade_date',
                                             return_value='2026-04-03'))
        self.stack.enter_context(patch.object(service, '_map_symbol_bars', return_value=[{
            'trade_date': '2026-04-03', 'open_price': 10, 'high_price': 12,
            'low_price': 9, 'close_price': 11, 'volume': 100, 'turnover': 1100,
        }]))
        create_run = self.mock('app.eod.pykrx_backfill.create_eod_collection_run', return_value=594)
        self.mock('app.eod.pykrx_backfill.update_eod_collection_run_progress')
        self.mock('app.eod.pykrx_backfill.complete_eod_collection_run')
        self.mock('app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                  return_value=SimpleNamespace(empty=False))
        self.mock('app.eod.pykrx_backfill._get_market_ohlcv_by_ticker',
                  return_value=_AnySymbolTurnoverFrame())
        self.mock('app.eod.pykrx_pipeline_runner._finalize_daily_stage_scope',
                  return_value={'ready': True, 'deleted_stale_row_count': 0})
        self.mock('app.eod.pykrx_backfill.persist_daily_price_bars',
                  side_effect=completed('prices', 1))
        self.mock('app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                  return_value={
                      'contexts_persisted': 1, 'contexts_failed': 0,
                      **atomic('price_contexts_scope_outcomes_v1'),
                  })
        self.mock('app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                  side_effect=completed('enrichment', _ready_pykrx_enrichment_result()))
        self.mock('app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                  new=AsyncMock(side_effect=completed('risk', {
                      'results_persisted': 1, 'results_failed': 0,
                      **atomic('market_risk_filter_scope_outcomes_v1'),
                  })))
        fundamentals = self.mock(
            'app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh',
            new=AsyncMock(side_effect=completed('fundamentals', {
                'status': 'completed', 'market_date': '2026-04-03',
                **build_exact_symbol_scope_outcome(
                    manifest_version='dart_fundamentals_scope_outcomes_v1',
                    selected_symbols=scope, successful_symbols=scope,
                    retry_debt_symbols=[],
                ),
            })),
        )
        self.mock('app.eod.pykrx_pipeline_runner._run_daily_piotroski_filter_followup',
                  side_effect=completed('piotroski', atomic('piotroski_scope_outcomes_v1')))
        self.mock('app.eod.pykrx_pipeline_runner._run_daily_factor_followup',
                  side_effect=completed('factors', {
                      'triggered': True, 'status': 'completed', 'results_persisted': 1,
                      'results_failed': 0, 'gate_status': 'ready',
                      'blocking_status': 'non_blocking',
                      **build_exact_symbol_scope_outcome(
                          manifest_version='factor_followup_scope_outcomes_v1',
                          selected_symbols=scope, successful_symbols=scope,
                          retry_debt_symbols=[],
                      ),
                  }))
        stored = self.mock(
            'app.eod.pykrx_backfill.daily_signal_feature_service.rebuild_stored_owned_fallback_for_market_date',
            side_effect=completed('stored_signal', atomic('signal_features_scope_outcomes_v1')),
        )
        live = self.mock('app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                         new=AsyncMock())
        optional = [self.mock(f'app.eod.pykrx_pipeline_runner.{name}') for name in (
            '_run_daily_external_news_followup', '_run_daily_external_news_enrichment_followup',
            '_run_daily_macro_context_followup', '_run_daily_intraday_snapshot_followup',
        )]
        agent_calls = {}
        for name, stage in (
            ('_run_daily_feedback_followup', 'feedback'),
            ('_run_daily_signal_decider_followup', 'decider'),
            ('_run_daily_next_open_execution_plan_followup', 'plan'),
        ):
            agent_calls[stage] = self.mock(
                f'app.eod.pykrx_pipeline_runner.{name}',
                side_effect=completed(stage, {'triggered': True, 'status': 'completed'}),
            )

        result = service._run_market_data_pipeline(
            start_date='20260403', end_date='20260403', markets=('KOSPI',),
            trigger='auto_close_scheduler', refresh_universe=True, mode='daily_auto',
            selected_stages=('price_collection', 'pykrx_enrichment', 'market_risk_filter'),
            chain_fundamentals_followup=True, point_in_time_recovery=True,
            retry_context=({'preopen_stored_signal_recovery_admitted': True}
                           if initial_open else None),
        )

        admitted = create_run.call_args.kwargs['run_metadata'].get(
            'preopen_stored_signal_recovery_admitted', False,
        )
        if initial_open:
            self.assertFalse(admitted)
            stored.assert_not_called()
            live.assert_not_called()
            return
        self.assertTrue(admitted)
        self.assertEqual(result['status'], 'completed', result)
        self.assertEqual(calls, [
            'prices', 'enrichment', 'risk', 'fundamentals', 'piotroski',
            'factors', 'stored_signal', 'feedback', 'decider', 'plan',
        ])
        self.assertEqual(fundamentals.call_args.kwargs['symbols'], scope)
        self.assertFalse(fundamentals.call_args.kwargs['run_piotroski_followup'])
        self.assertFalse(fundamentals.call_args.kwargs['run_feedback_followup'])
        stored.assert_called_once_with(market_date='2026-04-03', exact_symbols=scope)
        for stage in ('feedback', 'decider'):
            self.assertTrue(agent_calls[stage].call_args.kwargs.get('point_in_time_recovery'))
            self.assertEqual(agent_calls[stage].call_args.kwargs.get('symbols'), scope)
        live.assert_not_called()
        for provider in optional:
            provider.assert_not_called()

    def test_explicit_preopen_stages_use_native_fundamentals_and_narrowed_stored_signal(self):
        service = DailyPipelineStageService()
        native = self.mock(
            'app.eod.daily_pipeline_stages.dart_fundamentals_capture_service.run_daily_incremental_refresh',
            new=AsyncMock(return_value={'status': 'completed'}),
        )
        stored = self.mock(
            'app.eod.daily_pipeline_stages.daily_signal_feature_service.rebuild_stored_owned_fallback_for_market_date',
            return_value={'status': 'completed'},
        )
        live = self.mock(
            'app.eod.daily_pipeline_stages.daily_signal_feature_service.refresh_for_market_date',
            new=AsyncMock(),
        )
        download = self.mock('app.eod.pykrx_backfill._get_market_ohlcv_by_date')
        kwargs = {
            'market_date': '2026-04-03', 'symbols': ['005930'],
            'symbol_limit': None, 'symbol_offset': 0, 'capture_limit': 500,
            'retry_cycles': None, 'force': False, 'fs_div': 'CFS',
            'point_in_time_recovery': True,
            'exact_scope_symbols': ['000660', '005930'],
            'markets': ['KOSPI'], 'bypass_prerequisite_gate': False,
            'feedback_max_cycles': 1, 'feedback_outer_batch_size': 1,
            'reuse_existing_feedback': False,
        }
        service._execute_stage('fundamentals_daily_refresh', **kwargs)
        self.assertEqual(native.call_args.kwargs['symbols'], ['005930'])
        self.assertFalse(native.call_args.kwargs['run_piotroski_followup'])
        self.assertFalse(native.call_args.kwargs['run_feedback_followup'])

        for symbols, expected in ((['005930'], ['005930']), ([], []), (None, ['000660', '005930'])):
            with self.subTest(symbols=symbols):
                service._execute_stage('signal_features', **{**kwargs, 'symbols': symbols})
                self.assertEqual(stored.call_args.kwargs['exact_symbols'], expected)
        with self.assertRaisesRegex(ValueError, 'requires exact symbol scope'):
            service._execute_stage('signal_features', **{
                **kwargs, 'symbols': None, 'exact_scope_symbols': None,
            })
        live.assert_not_called()
        download.assert_not_called()

    def test_preopen_admitted_stored_signal_continues_after_open_only(self):
        self.market.state = 'OPEN'
        self.market.is_open = True
        self.market.server_time = '2026-04-06T09:01:00+09:00'
        service = DailyPipelineStageService()
        stored = self.mock(
            'app.eod.daily_pipeline_stages.daily_signal_feature_service.rebuild_stored_owned_fallback_for_market_date',
            return_value={'status': 'completed'},
        )
        live = self.mock(
            'app.eod.daily_pipeline_stages.daily_signal_feature_service.refresh_for_market_date',
            new=AsyncMock(),
        )
        native = self.mock(
            'app.eod.daily_pipeline_stages.dart_fundamentals_capture_service.run_daily_incremental_refresh',
            new=AsyncMock(),
        )
        kwargs = {
            'market_date': '2026-04-03', 'symbols': ['005930'],
            'markets': ['KOSPI'], 'symbol_limit': None, 'symbol_offset': 0,
            'capture_limit': 500, 'retry_cycles': None, 'force': False,
            'fs_div': 'CFS', 'bypass_prerequisite_gate': False,
            'feedback_max_cycles': 1, 'feedback_outer_batch_size': 1,
            'reuse_existing_feedback': False, 'point_in_time_recovery': True,
            'exact_scope_symbols': ['000660', '005930'],
        }

        unadmitted = service._execute_stage('signal_features', **kwargs)
        self.assertTrue(unadmitted['safe_skip'])
        stored.assert_not_called()

        admitted = {**kwargs, 'allow_previous_session_preopen_recovery': True}
        result = service._execute_stage('signal_features', **admitted)
        self.assertEqual(result['status'], 'completed')
        stored.assert_called_once_with(market_date='2026-04-03', exact_symbols=['005930'])
        late_fundamentals = service._execute_stage('fundamentals_daily_refresh', **admitted)
        self.assertTrue(late_fundamentals['safe_skip'])
        native.assert_not_called()
        live.assert_not_called()
