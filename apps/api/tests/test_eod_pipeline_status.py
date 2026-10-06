import unittest
from contextlib import contextmanager
from unittest.mock import call, patch

from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from app.eod.pipeline_status import (
    DEFAULT_FACTOR_STAGE_ORDER,
    DEFAULT_PYKRX_ENRICHMENT_CATEGORIES,
    EODPipelineStatusService,
    _load_daily_pipeline_stage_history,
    _load_latest_feedback_run_summary,
    _load_latest_pipeline_pykrx_run,
    _load_pipeline_pykrx_run_rows,
)


class _FakeMarketState:
    market = 'KRX'
    state = 'CLOSED'
    is_trading_day = True
    is_open = False
    reason = None
    server_time = '2026-04-21T18:15:00+09:00'
    current_calendar_date = '2026-04-21'
    latest_valid_trading_date = '2026-04-21'
    market_session = 'closed'


class _FakeBreakDayMarketState:
    market = 'KRX'
    state = 'HOLIDAY'
    is_trading_day = False
    is_open = False
    reason = 'holiday'
    server_time = '2026-05-05T12:15:00+09:00'
    current_calendar_date = '2026-05-05'
    latest_valid_trading_date = '2026-05-04'
    market_session = 'holiday'


def _full_factor_stage_counts(*, total: int, updated_at: str) -> dict[str, dict]:
    return {
        stage_id: {
            'persisted': total,
            'updated_at': updated_at,
        }
        for stage_id in DEFAULT_FACTOR_STAGE_ORDER
    }


def _full_feedback_prerequisite_stage_counts(*, total: int, updated_at: str) -> dict[str, dict]:
    return {
        'market_risk_filter': {
            'persisted': total,
            'updated_at': updated_at,
        },
        'piotroski_filter': {
            'persisted': total,
            'updated_at': updated_at,
        },
        **_full_factor_stage_counts(total=total, updated_at=updated_at),
    }


def _full_pykrx_enrichment_coverage(*, symbol_count: int, market_count: int, updated_at: str) -> dict[str, dict]:
    return {
        'requested_symbol_count': symbol_count,
        'requested_market_count': market_count,
        'categories': {
            category: {
                'present_rows': symbol_count if category not in {'market_breadth', 'investor_flow_market'} else market_count,
                'expected_rows': symbol_count if category not in {'market_breadth', 'investor_flow_market'} else market_count,
                'coverage_status': 'complete',
                'latest_captured_at': updated_at,
            }
            for category in DEFAULT_PYKRX_ENRICHMENT_CATEGORIES
        },
    }


def _blocked_pykrx_enrichment_coverage(*, expected_symbols: int, market_count: int, updated_at: str) -> dict[str, dict]:
    present_by_category = {
        'sector_classification': int(expected_symbols * 0.62),
        'investor_flow_symbol': int(expected_symbols * 0.60),
        'short_selling': int(expected_symbols * 0.63),
        'valuation_snapshot': int(expected_symbols * 0.61),
        'foreign_ownership': expected_symbols,
    }
    return {
        'requested_symbol_count': expected_symbols,
        'requested_market_count': market_count,
        'categories': {
            category: {
                'present_rows': (
                    market_count
                    if category in {'market_breadth', 'investor_flow_market'}
                    else present_by_category.get(category, expected_symbols)
                ),
                'expected_rows': (
                    market_count
                    if category in {'market_breadth', 'investor_flow_market'}
                    else expected_symbols
                ),
                'coverage_ratio': (
                    1.0
                    if category in {'market_breadth', 'investor_flow_market', 'foreign_ownership'}
                    else round(present_by_category.get(category, expected_symbols) / expected_symbols, 6)
                ),
                'coverage_status': (
                    'complete'
                    if category in {'market_breadth', 'investor_flow_market', 'foreign_ownership'}
                    else 'partial'
                ),
                'latest_captured_at': updated_at,
            }
            for category in DEFAULT_PYKRX_ENRICHMENT_CATEGORIES
        },
    }


def _available_macro_context_counts(*, updated_at: str = '2026-06-20T06:00:00+00:00') -> dict:
    return {
        'persisted': 3,
        'required_series_count': 3,
        'updated_at': updated_at,
        'context_status': 'available',
        'source_statuses': {
            'world_bank': {'status': 'available', 'series_count': 3},
            'fred': {'status': 'disabled_missing_api_key'},
        },
        'macro_regime_summary': {
            'growth_label': 'growth_expansion',
            'inflation_label': 'inflation_stable',
            'llm_generated': False,
        },
        'usage_policy': {
            'contextual_input_only': True,
            'direct_buy_sell_signal': False,
            'llm_generated': False,
        },
    }


def _completed_official_stage(
    *,
    run_id: int,
    stage_id: str,
    market_date: str,
    processed: int = 1,
    total: int = 1,
    updated_at: str = '2026-05-28T08:30:00+00:00',
) -> dict[str, dict]:
    return {
        'run': {
            'id': run_id,
            'market_date': market_date,
            'trigger': 'manual_api',
            'status': 'completed',
            'current_stage': stage_id,
            'started_at': updated_at,
            'updated_at': updated_at,
            'completed_at': updated_at,
            'error_message': None,
            'run_metadata': {'requested_stages': [stage_id], 'resolved_stages': [stage_id]},
        },
        'stage': {
            'stage_id': stage_id,
            'status': 'completed',
            'market_date': market_date,
            'started_at': updated_at,
            'updated_at': updated_at,
            'completed_at': updated_at,
            'processed': processed,
            'total': total,
            'succeeded': processed,
            'failed': 0,
            'remaining': 0,
            'current_step': None,
            'current_activity': None,
            'current_symbol': None,
            'current_period': None,
            'source_run_ids': {},
            'details': {},
        },
    }


def _incomplete_official_stage(
    *,
    run_id: int,
    stage_id: str,
    market_date: str,
    status: str,
    updated_at: str,
    succeeded: int,
    failed: int,
) -> dict[str, dict]:
    processed = succeeded + failed
    return {
        'run': {
            'id': run_id,
            'market_date': market_date,
            'trigger': 'manual_api',
            'status': status,
            'current_stage': stage_id,
            'started_at': updated_at,
            'updated_at': updated_at,
            'completed_at': updated_at,
            'error_message': 'Signal feature collection did not finish cleanly' if status == 'failed' else None,
            'run_metadata': {'requested_stages': [stage_id], 'resolved_stages': [stage_id]},
        },
        'stage': {
            'stage_id': stage_id,
            'status': status,
            'market_date': market_date,
            'started_at': updated_at,
            'updated_at': updated_at,
            'completed_at': updated_at,
            'processed': processed,
            'total': processed,
            'succeeded': succeeded,
            'failed': failed,
            'remaining': 0,
            'current_step': f'{stage_id}_{status}',
            'current_activity': f'{stage_id} finished {status}',
            'current_symbol': None,
            'current_period': None,
            'error_message': 'Signal feature collection did not finish cleanly' if status == 'failed' else None,
            'source_run_ids': {},
            'details': {
                'features_persisted': succeeded,
                'features_failed': failed,
            },
        },
    }


class EODPipelineStatusServiceTests(unittest.TestCase):
    def test_pykrx_stage_counts_not_applicable_categories_as_satisfied(self):
        coverage = _full_pykrx_enrichment_coverage(
            symbol_count=1,
            market_count=1,
            updated_at='2026-09-18T15:40:00+09:00',
        )
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        ):
            coverage['categories'][category] = {
                'scope_type': 'symbol',
                'present_rows': 0,
                'expected_rows': 0,
                'coverage_ratio': None,
                'coverage_status': 'not_applicable',
                'expectation_status': 'not_applicable',
                'latest_captured_at': None,
            }

        stage = EODPipelineStatusService()._build_pykrx_enrichment_stage(
            market_date='2026-09-18',
            coverage=coverage,
        )

        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['gate_status'], 'ready')
        self.assertEqual(stage['details']['missing_categories'], [])
        self.assertEqual(stage['details']['coverage_warning_categories'], [])
        gates = {
            item['category']: item
            for item in stage['details']['decider_category_gates']
        }
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        ):
            with self.subTest(category=category):
                self.assertEqual(
                    gates[category]['gate_status'],
                    'not_applicable',
                )

    def test_pykrx_partial_stage_counts_not_applicable_categories_as_processed(self):
        coverage = _full_pykrx_enrichment_coverage(
            symbol_count=1,
            market_count=1,
            updated_at='2026-09-18T15:40:00+09:00',
        )
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        ):
            coverage['categories'][category] = {
                'scope_type': 'symbol',
                'present_rows': 0,
                'expected_rows': 0,
                'coverage_ratio': None,
                'coverage_status': 'not_applicable',
                'expectation_status': 'not_applicable',
                'latest_captured_at': None,
            }
        coverage['categories']['valuation_snapshot'] = {
            'scope_type': 'symbol',
            'present_rows': 0,
            'expected_rows': 1,
            'coverage_ratio': 0.0,
            'coverage_status': 'missing',
            'expectation_status': 'available',
            'latest_captured_at': None,
        }

        stage = EODPipelineStatusService()._build_pykrx_enrichment_stage(
            market_date='2026-09-18',
            coverage=coverage,
        )

        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['total'], 7)
        self.assertEqual(stage['succeeded'], 6)
        self.assertEqual(stage['failed'], 1)
        self.assertEqual(stage['processed'], 7)
        self.assertEqual(stage['remaining'], 0)

    def test_official_completed_stage_with_failed_items_is_reported_partial(self):
        market_date = '2026-05-28'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='signal_decider',
            market_date=market_date,
            processed=160,
            total=160,
        )
        official_item['stage']['succeeded'] = 60
        official_item['stage']['failed'] = 100
        official_run = {
            **official_item['run'],
            'stages': {'signal_decider': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['signal_decider']
        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['source_status'], 'completed')
        self.assertEqual(stage['failed'], 100)
        self.assertEqual(stage['current_activity'], 'Completed with 100 failed items.')

    def test_completed_next_open_plan_is_blocked_when_decider_counts_are_incomplete(self):
        market_date = '2026-05-28'
        decider_item = _completed_official_stage(
            run_id=386,
            stage_id='signal_decider',
            market_date=market_date,
            processed=160,
            total=160,
        )
        decider_item['stage']['succeeded'] = 60
        decider_item['stage']['failed'] = 100
        plan_item = _completed_official_stage(
            run_id=386,
            stage_id='next_open_execution_plan',
            market_date=market_date,
            processed=0,
            total=0,
        )
        official_run = {
            **decider_item['run'],
            'stages': {
                'signal_decider': decider_item['stage'],
                'next_open_execution_plan': plan_item['stage'],
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['next_open_execution_plan']
        self.assertEqual(stage['status'], 'blocked')
        self.assertEqual(stage['source_status'], 'completed')
        self.assertEqual(stage['details']['status_repair_reason'], 'signal_decider_incomplete')

    def test_official_external_news_results_repair_legacy_zero_counters(self):
        market_date = '2026-05-28'
        collect_item = _completed_official_stage(
            run_id=386,
            stage_id='external_news_collect',
            market_date=market_date,
            processed=0,
            total=201,
        )
        collect_item['stage']['succeeded'] = 0
        collect_item['stage']['details'] = {
            'result_summary': {
                'summary': {
                    'eligible_symbol_count': 201,
                    'collected_symbol_count': 201,
                    'items_inserted': 252,
                }
            }
        }
        enrich_item = _completed_official_stage(
            run_id=386,
            stage_id='external_news_enrich',
            market_date=market_date,
            processed=0,
            total=0,
        )
        enrich_item['stage']['succeeded'] = 0
        enrich_item['stage']['details'] = {
            'result_summary': {
                'items_inspected': 703,
                'items_updated': 703,
                'items_missing_classification': 0,
                'items_remaining_at_least': 0,
                'missing_item_ids': [],
            }
        }
        official_run = {
            **collect_item['run'],
            'stages': {
                'external_news_collect': collect_item['stage'],
                'external_news_enrich': enrich_item['stage'],
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        collect_stage = result['stages']['external_news_collect']
        self.assertEqual(collect_stage['processed'], 201)
        self.assertEqual(collect_stage['total'], 201)
        self.assertEqual(collect_stage['succeeded'], 201)
        self.assertEqual(collect_stage['failed'], 0)
        enrich_stage = result['stages']['external_news_enrich']
        self.assertEqual(enrich_stage['processed'], 703)
        self.assertEqual(enrich_stage['total'], 703)
        self.assertEqual(enrich_stage['succeeded'], 703)
        self.assertEqual(enrich_stage['failed'], 0)

    def test_official_external_news_enrichment_recovers_legacy_lower_bound_semantics(self):
        market_date = '2026-05-28'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='external_news_enrich',
            market_date=market_date,
            processed=703,
            total=704,
        )
        official_item['stage']['succeeded'] = 701
        official_item['stage']['failed'] = 2
        official_item['stage']['remaining'] = 1
        official_item['stage']['details'] = {
            'result_summary': {
                'items_inspected': 703,
                'items_missing_classification': 3,
                'items_remaining_at_least': 1,
                'missing_item_ids': [11, 12],
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'external_news_enrich': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        self.assertEqual(
            result['stages']['external_news_enrich']['details']['count_semantics'],
            {
                'total': 'lower_bound',
                'total_at_least': 704,
                'remaining': 'lower_bound',
                'remaining_at_least': 1,
            },
        )

    def test_official_external_news_explicit_zero_scope_clears_stale_legacy_total(self):
        market_date = '2026-05-28'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='external_news_collect',
            market_date=market_date,
            processed=0,
            total=201,
        )
        official_item['stage']['succeeded'] = 0
        official_item['stage']['details'] = {
            'result_summary': {
                'status': 'disabled',
                'summary': {
                    'requested_symbol_count': 201,
                    'eligible_symbol_count': 0,
                    'collected_symbol_count': 0,
                },
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'external_news_collect': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['external_news_collect']
        self.assertEqual(stage['processed'], 0)
        self.assertEqual(stage['total'], 0)
        self.assertEqual(stage['succeeded'], 0)
        self.assertEqual(stage['failed'], 0)
        self.assertEqual(stage['remaining'], 0)

    def test_official_external_news_failure_counters_are_not_overwritten(self):
        market_date = '2026-05-28'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='external_news_collect',
            market_date=market_date,
            processed=0,
            total=201,
        )
        official_item['stage']['succeeded'] = 0
        official_item['stage']['failed'] = 1
        official_item['stage']['details'] = {
            'result_summary': {
                'summary': {
                    'eligible_symbol_count': 201,
                    'collected_symbol_count': 201,
                }
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'external_news_collect': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            external_news_counts_loader=lambda **_: {},
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['external_news_collect']
        self.assertEqual(stage['processed'], 0)
        self.assertEqual(stage['succeeded'], 0)
        self.assertEqual(stage['failed'], 1)

    def test_pykrx_enrichment_diagnostics_do_not_change_official_stage_status(self):
        market_date = '2026-05-28'
        official_item = _incomplete_official_stage(
            run_id=386,
            stage_id='pykrx_enrichment',
            market_date=market_date,
            status='partial',
            updated_at='2026-05-28T08:45:00+00:00',
            succeeded=2,
            failed=5,
        )
        official_run = {
            **official_item['run'],
            'stages': {'pykrx_enrichment': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=10,
                market_count=2,
                updated_at='2026-05-28T08:40:00+00:00',
            ),
        )

        without_diagnostics = service.read(
            market_date=market_date,
            include_pykrx_enrichment_coverage=False,
        )
        with_diagnostics = service.read(
            market_date=market_date,
            include_pykrx_enrichment_coverage=True,
        )

        self.assertEqual(
            without_diagnostics['stages']['pykrx_enrichment']['status'],
            'partial',
        )
        self.assertEqual(
            with_diagnostics['stages']['pykrx_enrichment']['status'],
            'partial',
        )
        self.assertEqual(
            with_diagnostics['stages']['pykrx_enrichment']['gate_status'],
            'ready',
        )
        self.assertTrue(
            with_diagnostics['stages']['pykrx_enrichment']['details']['categories']
        )
        self.assertEqual(
            with_diagnostics['stages']['pykrx_enrichment']['details'][
                'coverage_diagnostics_source'
            ],
            'stage_specific_fallback',
        )
        self.assertEqual(
            with_diagnostics['stages']['pykrx_enrichment']['details'][
                'coverage_diagnostics_updated_at'
            ],
            '2026-05-28T08:40:00+00:00',
        )

    def test_load_daily_pipeline_stage_history_filters_stage_market_date_for_history_index(self):
        class _CapturedResult:
            def all(self):
                return []

        class _CapturedSession:
            statement = None

            def execute(self, statement):
                self.statement = statement
                return _CapturedResult()

        captured_session = _CapturedSession()

        @contextmanager
        def _session_scope():
            yield captured_session

        with patch('app.eod.pipeline_status.session_scope', _session_scope):
            result = _load_daily_pipeline_stage_history(market_date='2026-05-28')

        self.assertEqual(result, {})
        compiled_sql = str(captured_session.statement.compile(compile_kwargs={'literal_binds': True}))
        where_sql = compiled_sql.split('\nWHERE ', 1)[1].split(' ORDER BY ', 1)[0]
        self.assertIn('daily_pipeline_runs.market_date =', where_sql)
        self.assertIn('daily_pipeline_stage_runs.market_date =', where_sql)
        self.assertIn('daily_pipeline_stage_runs.stage_id IN', where_sql)

    def test_load_latest_pipeline_pykrx_run_is_read_only_for_stale_looking_daily_run(self):
        historical_run = {
            'id': 900,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_backfill_api',
            'status': 'running',
            'symbols_total': 100,
            'symbols_succeeded': 20,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T10:00:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'historical_backfill',
            },
        }
        daily_running_run = {
            'id': 901,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'running',
            'symbols_total': 3,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:00:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'daily_auto',
                'stage': 'technical_snapshots',
            },
        }
        with (
            patch(
                'app.eod.pipeline_status._load_pipeline_pykrx_run_rows',
                return_value=[historical_run, daily_running_run],
            ),
            patch(
                'app.eod.pykrx_backfill.PYKRX_BACKFILL_SERVICE.'
                'reconcile_latest_daily_run_status',
            ) as reconcile_run,
        ):
            result = _load_latest_pipeline_pykrx_run(trade_date='2026-04-21')

        self.assertEqual(result['id'], daily_running_run['id'])
        self.assertEqual(result['status'], 'running')
        reconcile_run.assert_not_called()

    def test_load_pipeline_pykrx_run_rows_filters_historical_runs_in_sql_before_limit(self):
        daily_run = {'id': 901, 'run_metadata': {'mode': 'daily_auto'}}

        with patch(
            'app.eod.pipeline_status.list_eod_collection_runs',
            return_value=[daily_run],
        ) as list_runs:
            result = _load_pipeline_pykrx_run_rows(trade_date='2026-04-21')

        self.assertEqual(result, [daily_run])
        list_runs.assert_called_once_with(
            market='KRX',
            trade_date='2026-04-21',
            source='pykrx',
            excluded_run_metadata_modes=('historical_backfill',),
            limit=10,
        )

    def test_load_pipeline_pykrx_run_rows_falls_back_to_latest_historical_run(self):
        historical_run = {
            'id': 900,
            'run_metadata': {'mode': 'historical_backfill'},
        }

        with patch(
            'app.eod.pipeline_status.list_eod_collection_runs',
            side_effect=[[], [historical_run]],
        ) as list_runs:
            result = _load_pipeline_pykrx_run_rows(trade_date='2026-04-21')

        self.assertEqual(result, [historical_run])
        self.assertEqual(
            list_runs.call_args_list,
            [
                call(
                    market='KRX',
                    trade_date='2026-04-21',
                    source='pykrx',
                    excluded_run_metadata_modes=('historical_backfill',),
                    limit=10,
                ),
                call(
                    market='KRX',
                    trade_date='2026-04-21',
                    source='pykrx',
                    limit=1,
                ),
            ],
        )

    def test_read_returns_pending_when_no_runs_exist(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            pykrx_enrichment_loader=lambda **_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['market_date'], '2026-04-21')
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['total_stages'], 26)
        self.assertEqual(result['completed_stages'], 0)
        self.assertEqual(result['pending_stages'], 26)
        self.assertEqual(result['current_stage'], 'master_refresh')
        self.assertEqual(result['stages']['price_collection']['status'], 'pending')
        self.assertEqual(result['stages']['price_collection']['order'], 3)
        self.assertEqual(result['stages']['price_collection']['group'], 'market_data')
        self.assertEqual(result['stages']['price_collection']['inspect_routes'], ['/eod/pipeline/status', '/eod/pykrx/daily/run-if-due'])
        self.assertEqual(result['stages']['price_collection']['total'], 3579)
        self.assertEqual(result['stages']['trending_ranks']['total'], 3)
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'pending')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'pending')
        self.assertEqual(result['stages']['composite_factors']['status'], 'pending')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['details']['fs_div'], 'CFS')
        self.assertEqual(
            result['stages']['fundamentals_daily_refresh']['details']['inspect_routes'],
            ['/disclosures/dart/fundamentals/daily/inspect', '/eod/pipeline/status'],
        )

    def test_read_prefers_official_running_market_risk_filter_before_output_rows_exist(self):
        official_run = {
            'id': 501,
            'market_date': '2026-04-21',
            'trigger': 'internal_ops_cli_stage',
            'status': 'running',
            'current_stage': 'market_risk_filter',
            'started_at': '2026-04-21T09:10:00+00:00',
            'updated_at': '2026-04-21T09:11:00+00:00',
            'completed_at': None,
            'error_message': None,
            'run_metadata': {'resume_from_stage': 'market_risk_filter'},
            'stages': {
                'market_risk_filter': {
                    'stage_id': 'market_risk_filter',
                    'status': 'running',
                    'market_date': '2026-04-21',
                    'started_at': '2026-04-21T09:10:00+00:00',
                    'updated_at': '2026-04-21T09:11:00+00:00',
                    'completed_at': None,
                    'processed': 7,
                    'total': 20,
                    'succeeded': 7,
                    'failed': 0,
                    'remaining': 13,
                    'current_step': 'fetching_kis_market_risk_snapshots',
                    'current_activity': 'Fetching KIS market-risk snapshot for 005930',
                    'current_symbol': '005930',
                    'current_period': None,
                    'source_run_ids': {},
                    'details': {},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 20,
            fundamentals_refresh_loader=lambda **_: None,
            row_count_stage_counts_loader=lambda **_: {'market_risk_filter': {'persisted': 0}},
            official_run_loader=lambda **_: official_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['total_stages'], 26)
        self.assertEqual(result['running_stages'], 1)
        self.assertEqual(result['current_stage'], 'market_risk_filter')
        self.assertEqual(result['official_run']['id'], 501)
        stage = result['stages']['market_risk_filter']
        self.assertEqual(stage['status'], 'running')
        self.assertEqual(stage['order'], 8)
        self.assertEqual(stage['group'], 'market_data')
        self.assertEqual(stage['details']['progress_source'], 'official_run')
        self.assertEqual(stage['processed'], 7)
        self.assertEqual(stage['total'], 20)
        self.assertEqual(stage['current_symbol'], '005930')
        self.assertEqual(stage['details']['mode'], 'official_daily_pipeline_run_state')

    def test_read_surfaces_interrupted_official_run_as_resumable_stage_state(self):
        official_run = {
            'id': 502,
            'market_date': '2026-04-21',
            'trigger': 'manual_api',
            'status': 'interrupted',
            'current_stage': 'signal_decider',
            'started_at': '2026-04-21T09:10:00+00:00',
            'updated_at': '2026-04-21T09:30:00+00:00',
            'completed_at': '2026-04-21T09:30:00+00:00',
            'error_message': 'Core process started while previous daily pipeline run was still marked running.',
            'run_metadata': {'resumable': True},
            'stages': {
                'signal_decider': {
                    'stage_id': 'signal_decider',
                    'status': 'interrupted',
                    'market_date': '2026-04-21',
                    'started_at': '2026-04-21T09:20:00+00:00',
                    'updated_at': '2026-04-21T09:30:00+00:00',
                    'completed_at': '2026-04-21T09:30:00+00:00',
                    'processed': 3,
                    'total': 10,
                    'succeeded': 3,
                    'failed': 0,
                    'remaining': 7,
                    'current_step': 'core_restart_interrupted',
                    'current_activity': 'Core process started while previous daily pipeline run was still marked running.',
                    'current_symbol': None,
                    'current_period': None,
                    'error_message': 'Core process started while previous daily pipeline run was still marked running.',
                    'source_run_ids': {},
                    'details': {'resumable': True},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['current_stage'], 'signal_decider')
        self.assertEqual(result['current_stage_status'], 'interrupted')
        self.assertEqual(result['stage_status_counts']['interrupted'], 1)
        self.assertEqual(result['stages']['signal_decider']['status'], 'interrupted')
        self.assertTrue(result['stages']['signal_decider']['details']['resumable'])
        self.assertIn('previous daily pipeline run', result['stages']['signal_decider']['current_activity'])

    def test_read_selects_abandoned_official_stage_before_pending_stage(self):
        official_run = {
            'id': 504,
            'market_date': '2026-04-21',
            'trigger': 'manual_api',
            'status': 'abandoned',
            'current_stage': 'signal_decider',
            'started_at': '2026-04-21T09:10:00+00:00',
            'updated_at': '2026-04-21T09:30:00+00:00',
            'completed_at': '2026-04-21T09:30:00+00:00',
            'error_message': 'Previous daily pipeline run was abandoned after restart.',
            'run_metadata': {'resumable': True},
            'stages': {
                'signal_decider': {
                    'stage_id': 'signal_decider',
                    'status': 'abandoned',
                    'market_date': '2026-04-21',
                    'started_at': '2026-04-21T09:20:00+00:00',
                    'updated_at': '2026-04-21T09:30:00+00:00',
                    'completed_at': '2026-04-21T09:30:00+00:00',
                    'processed': 3,
                    'total': 10,
                    'succeeded': 3,
                    'failed': 0,
                    'remaining': 7,
                    'current_step': 'core_restart_abandoned',
                    'current_activity': 'Previous daily pipeline run was abandoned after restart.',
                    'current_symbol': None,
                    'current_period': None,
                    'error_message': 'Previous daily pipeline run was abandoned after restart.',
                    'source_run_ids': {},
                    'details': {'resumable': True},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'abandoned')
        self.assertEqual(result['current_stage'], 'signal_decider')
        self.assertEqual(result['current_stage_status'], 'abandoned')
        self.assertEqual(result['stage_status_counts']['abandoned'], 1)
        self.assertEqual(result['stages']['signal_decider']['status'], 'abandoned')
        self.assertTrue(result['stages']['signal_decider']['details']['resumable'])

    def test_read_shows_downstream_signal_decider_running_with_zero_output_rows(self):
        official_run = {
            'id': 503,
            'market_date': '2026-04-21',
            'trigger': 'manual_api',
            'status': 'running',
            'current_stage': 'signal_decider',
            'started_at': '2026-04-21T09:10:00+00:00',
            'updated_at': '2026-04-21T09:20:00+00:00',
            'completed_at': None,
            'error_message': None,
            'run_metadata': {'requested_stages': ['signal_decider']},
            'stages': {
                'signal_decider': {
                    'stage_id': 'signal_decider',
                    'status': 'running',
                    'market_date': '2026-04-21',
                    'started_at': '2026-04-21T09:20:00+00:00',
                    'updated_at': '2026-04-21T09:20:00+00:00',
                    'completed_at': None,
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 0,
                    'current_step': 'signal_decider_started',
                    'current_activity': 'Running decision support decider dispatch',
                    'current_symbol': None,
                    'current_period': None,
                    'source_run_ids': {},
                    'details': {},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['stages']['signal_decider']['status'], 'running')
        self.assertEqual(result['stages']['signal_decider']['processed'], 0)
        self.assertEqual(
            result['stages']['signal_decider']['current_activity'],
            'Running decision support decider dispatch',
        )

    def test_read_summarizes_normalized_warning_followup_official_stage_as_partial(self):
        market_date = '2026-06-05'
        official_run = {
            'id': 505,
            'market_date': market_date,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'current_stage': 'signal_decider_evaluations',
            'started_at': '2026-06-05T06:41:47+00:00',
            'updated_at': '2026-06-05T07:08:00+00:00',
            'completed_at': '2026-06-05T07:08:00+00:00',
            'error_message': None,
            'run_metadata': {
                'mode': 'daily_auto',
                'stage_statuses': {'signal_decider_evaluations': 'partial'},
            },
            'stages': {
                'signal_decider_evaluations': {
                    'stage_id': 'signal_decider_evaluations',
                    'status': 'partial',
                    'market_date': market_date,
                    'started_at': '2026-06-05T07:05:30+00:00',
                    'updated_at': '2026-06-05T07:08:00+00:00',
                    'completed_at': '2026-06-05T07:08:00+00:00',
                    'processed': 228,
                    'total': 228,
                    'succeeded': 227,
                    'failed': 1,
                    'remaining': 0,
                    'current_step': 'signal_decider_evaluations_partial',
                    'current_activity': 'Completed with errors signal decider evaluations',
                    'current_symbol': None,
                    'current_period': None,
                    'source_run_ids': {'eod_run_id': 311},
                    'details': {
                        'mode': 'auto_eod_pipeline_tracking',
                        'progress_source': 'auto_eod_stage_progress',
                        'result_summary': {
                            'status': 'warning',
                            'summary': {'not_computed_count': 1},
                        },
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 228,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read(market_date=market_date)

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['current_stage'], 'signal_decider_evaluations')
        self.assertEqual(result['current_stage_status'], 'partial')
        self.assertEqual(result['partial_stages'], 1)
        self.assertNotIn('warning', result['stage_status_counts'])
        stage = result['stages']['signal_decider_evaluations']
        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['completed_at'], '2026-06-05T07:08:00+00:00')
        self.assertEqual(stage['details']['progress_source'], 'auto_eod_stage_progress')
        self.assertEqual(
            result['official_run']['run_metadata']['stage_statuses'],
            {'signal_decider_evaluations': 'partial'},
        )

    def test_read_uses_break_day_calendar_date_when_no_official_run_exists(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeBreakDayMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['market_date'], '2026-05-05')
        self.assertEqual(
            result['market'],
            {
                'market': 'KRX',
                'state': 'HOLIDAY',
                'is_trading_day': False,
                'is_open': False,
                'reason': 'holiday',
                'server_time': '2026-05-05T12:15:00+09:00',
                'current_calendar_date': '2026-05-05',
                'latest_valid_trading_date': '2026-05-04',
                'market_session': 'holiday',
            },
        )
        self.assertEqual(result['market_session']['status'], 'holiday')
        self.assertEqual(result['market_session']['latest_valid_trading_date'], '2026-05-04')
        self.assertFalse(result['market_session']['intraday_data_expected'])
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['stages']['price_collection']['market_date'], '2026-05-05')
        self.assertEqual(result['stages']['price_collection']['status'], 'pending')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['market_date'], '2026-05-05')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'pending')

    def test_read_without_market_date_uses_latest_persisted_run_date(self):
        latest_run = {
            'id': 386,
            'market_date': '2026-05-04',
            'trigger': 'manual_daily_api',
            'status': 'partial',
            'current_stage': 'price_collection',
            'started_at': '2026-05-05T09:00:00+00:00',
            'updated_at': '2026-05-05T10:00:00+00:00',
            'completed_at': '2026-05-05T10:00:00+00:00',
            'error_message': None,
            'run_metadata': {},
            'stages': {},
        }
        official_run_dates = []
        pykrx_dates = []

        def _load_official_run(*, market_date):
            official_run_dates.append(market_date)
            return latest_run if market_date is None else None

        def _load_pykrx_run(*, trade_date):
            pykrx_dates.append(trade_date)
            return None

        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeBreakDayMarketState(),
            pykrx_run_loader=_load_pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=_load_official_run,
        )

        result = service.read()

        self.assertEqual(official_run_dates, [None])
        self.assertEqual(pykrx_dates, ['2026-05-04'])
        self.assertEqual(result['market_date'], '2026-05-04')
        self.assertEqual(result['official_run']['id'], 386)

    def test_official_stage_result_summary_is_compacted_on_read(self):
        market_date = '2026-04-21'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='signal_decider_evaluations',
            market_date=market_date,
            processed=200,
            total=200,
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'status': 'completed',
                'summary': {'evaluations_upserted': 200},
                'evaluation_refresh': {
                    'summary': {'evaluations_upserted': 200},
                    'items': [{'symbol': f'{index:06d}'} for index in range(200)],
                },
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'signal_decider_evaluations': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read(market_date=market_date)

        details = result['stages']['signal_decider_evaluations']['details']
        self.assertEqual(
            details['result_summary']['evaluation_refresh'],
            {'summary': {'evaluations_upserted': 200}},
        )
        self.assertEqual(
            details['result_summary_omissions']['evaluation_refresh.items'],
            {'type': 'list', 'item_count': 200},
        )

    def test_full_factor_row_count_does_not_hide_zero_ready_content(self):
        market_date = '2026-09-23'
        official_item = _completed_official_stage(
            run_id=475,
            stage_id='low_risk_factors',
            market_date=market_date,
            processed=2_678,
            total=2_678,
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'results_persisted': 2_678,
                'results_failed': 0,
                'summary': {
                    'requested_symbol_count': 2_678,
                    'matched_symbol_count': 2_678,
                    'insufficient_history_count': 2_678,
                    'ready_count': 0,
                },
            }
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_678,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'low_risk_factors': official_item,
            },
            price_bar_count_loader=lambda **_: 2_678,
            row_count_stage_counts_loader=lambda **_: {
                'low_risk_factors': {
                    'persisted': 2_678,
                    'updated_at': '2026-09-23T12:00:00+00:00',
                },
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['low_risk_factors']
        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['gate_status'], 'blocked')
        self.assertEqual(
            stage['details']['gate_reason'],
            'no_decision_ready_rows',
        )

    def test_fresher_low_risk_rows_replace_stale_zero_ready_history(self):
        market_date = '2026-09-23'
        official_item = _completed_official_stage(
            run_id=475,
            stage_id='low_risk_factors',
            market_date=market_date,
            processed=2_678,
            total=2_678,
            updated_at='2026-09-24T00:00:00+00:00',
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'results_persisted': 2_678,
                'results_failed': 0,
                'summary': {
                    'requested_symbol_count': 2_678,
                    'matched_symbol_count': 2_678,
                    'insufficient_history_count': 2_678,
                    'ready_count': 0,
                },
            }
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_678,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'low_risk_factors': official_item,
            },
            price_bar_count_loader=lambda **_: 2_678,
            row_count_stage_counts_loader=lambda **_: {
                'low_risk_factors': {
                    'persisted': 2_678,
                    'updated_at': '2026-09-27T22:54:27+00:00',
                    'decision_ready_count': 1_587,
                    'insufficient_history_count': 1_091,
                    'latest_decision_ready_at': '2026-09-27T22:54:27+00:00',
                },
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['low_risk_factors']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['gate_status'], 'ready')
        self.assertEqual(stage['details']['decision_ready_count'], 1_587)
        self.assertEqual(stage['details']['insufficient_history_count'], 1_091)

    def test_newer_unready_row_does_not_make_older_ready_rows_fresh(self):
        market_date = '2026-09-23'
        official_item = _completed_official_stage(
            run_id=475,
            stage_id='low_risk_factors',
            market_date=market_date,
            processed=2_678,
            total=2_678,
            updated_at='2026-09-24T00:00:00+00:00',
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'results_persisted': 2_678,
                'results_failed': 0,
                'summary': {
                    'requested_symbol_count': 2_678,
                    'matched_symbol_count': 2_678,
                    'insufficient_history_count': 2_678,
                    'ready_count': 0,
                },
            }
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_678,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'low_risk_factors': official_item,
            },
            price_bar_count_loader=lambda **_: 2_678,
            row_count_stage_counts_loader=lambda **_: {
                'low_risk_factors': {
                    'persisted': 2_678,
                    'updated_at': '2026-09-27T22:54:27+00:00',
                    'decision_ready_count': 1_587,
                    'insufficient_history_count': 1_091,
                    'latest_decision_ready_at': '2026-09-23T22:54:27+00:00',
                },
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['low_risk_factors']
        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['gate_status'], 'blocked')
        self.assertEqual(stage['details']['gate_reason'], 'no_decision_ready_rows')

    def test_fresher_ready_rows_replace_zero_ready_current_official_run(self):
        market_date = '2026-09-23'
        official_item = _completed_official_stage(
            run_id=475,
            stage_id='low_risk_factors',
            market_date=market_date,
            processed=2_678,
            total=2_678,
            updated_at='2026-09-24T00:00:00+00:00',
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'results_persisted': 2_678,
                'results_failed': 0,
                'summary': {
                    'requested_symbol_count': 2_678,
                    'matched_symbol_count': 2_678,
                    'insufficient_history_count': 2_678,
                    'ready_count': 0,
                },
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'low_risk_factors': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_678,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
            official_stage_history_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 2_678,
            row_count_stage_counts_loader=lambda **_: {
                'low_risk_factors': {
                    'persisted': 2_678,
                    'updated_at': '2026-09-27T22:54:27+00:00',
                    'decision_ready_count': 1_587,
                    'insufficient_history_count': 1_091,
                    'latest_decision_ready_at': '2026-09-27T22:54:27+00:00',
                },
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['low_risk_factors']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['gate_status'], 'ready')
        self.assertEqual(stage['details']['decision_ready_count'], 1_587)

    def test_completed_zero_scope_history_is_blocked_by_incomplete_context(self):
        market_date = '2026-09-22'
        signal_decider_item = _completed_official_stage(
            run_id=467,
            stage_id='signal_decider',
            market_date=market_date,
            processed=1,
            total=1,
        )
        official_item = _completed_official_stage(
            run_id=467,
            stage_id='next_open_execution_plan',
            market_date=market_date,
            processed=0,
            total=0,
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'summary': {
                    'items_total': 0,
                    'symbol_count': 0,
                    'agent_context_completeness_status': 'incomplete',
                    'agent_context_completeness': {
                        'status': 'incomplete',
                        'candidate_set_definitive': False,
                        'pending_scope_count': 157,
                    },
                },
                'items': [],
            }
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2_678,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {
                'signal_decider': signal_decider_item,
                'next_open_execution_plan': official_item,
            },
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['next_open_execution_plan']
        self.assertEqual(stage['status'], 'blocked')
        self.assertEqual(
            stage['details']['status_repair_reason'],
            'incomplete_result_summary',
        )

    def test_official_feedback_stage_repairs_counts_and_activity_from_role_progress(self):
        market_date = '2026-04-21'
        official_item = _completed_official_stage(
            run_id=386,
            stage_id='agent_feedback_summaries',
            market_date=market_date,
            processed=239,
            total=239,
        )
        official_item['stage']['details'] = {
            'result_summary': {
                'status': 'completed',
                'progress': {
                    'summary': {
                        'role_progress': {
                            'eod-summarizer': {
                                'status': 'completed',
                                'total_symbols': 80,
                                'processed_symbols': 80,
                                'completed_symbols': 80,
                                'pending_symbols': 0,
                                'failed_items': 0,
                            },
                            'technical-summarizer': {
                                'status': 'completed',
                                'total_symbols': 80,
                                'processed_symbols': 80,
                                'completed_symbols': 80,
                                'pending_symbols': 0,
                                'failed_items': 0,
                            },
                            'fundamental-summarizer': {
                                'status': 'partial',
                                'total_symbols': 80,
                                'processed_symbols': 79,
                                'completed_symbols': 79,
                                'pending_symbols': 1,
                                'failed_items': 0,
                            },
                        }
                    }
                },
            }
        }
        official_run = {
            **official_item['run'],
            'stages': {'agent_feedback_summaries': official_item['stage']},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: official_run,
        )

        result = service.read(market_date=market_date)

        stage = result['stages']['agent_feedback_summaries']
        self.assertEqual(stage['status'], 'partial')
        self.assertEqual(stage['processed'], 239)
        self.assertEqual(stage['total'], 240)
        self.assertEqual(stage['succeeded'], 239)
        self.assertEqual(stage['failed'], 0)
        self.assertEqual(stage['remaining'], 1)
        self.assertEqual(stage['current_activity'], 'Incomplete with 1 item remaining.')

    def test_read_reports_live_stage_progress_for_pykrx_and_fundamentals(self):
        pykrx_run = {
            'id': 101,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'running',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'daily_auto',
                'requested_start_date': '20260421',
                'requested_end_date': '20260421',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
                'stage': 'technical_snapshots',
                'current_step': 'computing_symbol_snapshot',
                'current_activity': 'Computing daily technical snapshots for 000660',
                'current_symbol': '000660',
                'stage_progress': {
                    'price_collection': {
                        'status': 'completed',
                        'started_at': '2026-04-21T09:10:00+00:00',
                        'updated_at': '2026-04-21T09:20:00+00:00',
                        'completed_at': '2026-04-21T09:20:00+00:00',
                        'processed': 3,
                        'total': 3,
                        'succeeded': 3,
                        'failed': 0,
                        'bars_upserted': 3,
                    },
                    'technical_snapshots': {
                        'status': 'running',
                        'started_at': '2026-04-21T09:20:10+00:00',
                        'updated_at': '2026-04-21T09:20:30+00:00',
                        'processed': 1,
                        'total': 3,
                        'succeeded': 1,
                        'failed': 0,
                        'current_symbol': '000660',
                        'current_step': 'computing_symbol_snapshot',
                        'current_activity': 'Computing daily technical snapshots for 000660',
                    },
                },
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 1, 'updated_at': '2026-04-21T09:20:30+00:00'},
            'action_summaries': {'persisted': 0, 'updated_at': None},
            'trending_ranks': {'persisted': 0, 'periods_persisted': 0, 'updated_at': None},
        }
        fundamentals_run = {
            'id': 55,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'running',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3579,
            'symbols_checked': 120,
            'symbols_missing': 10,
            'symbols_stale': 35,
            'symbols_fresh': 60,
            'symbols_refreshed': 15,
            'symbols_failed': 2,
            'events_detected': 440,
            'events_captured': 37,
            'events_failed': 1,
            'error_message': None,
            'started_at': '2026-04-21T09:21:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'symbols_selected': 200,
                'stage': 'capturing_fundamentals',
                'capture_event_count': 75,
                'retry_cycle': 2,
                'retry_cycles_requested': 3,
                'batch_index': 1,
                'batch_count': 2,
                'capture_run_id': 700,
                'capture_run_ids': [699, 700],
                'sync_runs': [{'run_id': 880}, {'run_id': 881}],
                'remaining_candidate_events': 403,
                'last_progress_at': '2026-04-21T09:22:10+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            official_run_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['stages']['price_collection']['status'], 'completed')
        self.assertEqual(result['stages']['price_collection']['processed'], 3)
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'running')
        self.assertEqual(result['stages']['technical_snapshots']['current_symbol'], '000660')
        self.assertEqual(result['stages']['technical_snapshots']['processed'], 1)
        self.assertEqual(result['stages']['action_summaries']['status'], 'pending')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'running')
        self.assertEqual(
            result['stages']['fundamentals_daily_refresh']['current_activity'],
            'Capturing DART fundamentals for 75 events, retry cycle 2/3, batch 1/2',
        )
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['source_run_ids']['capture_run_ids'], [699, 700])
        self.assertNotIn(
            'trigger_after_capture_enabled',
            result['stages']['fundamentals_daily_refresh']['details'],
        )

    def test_read_promotes_active_auto_followup_progress_when_official_rows_are_absent(self):
        market_date = '2026-06-05'
        pykrx_run = {
            'id': 311,
            'market': 'KRX',
            'trade_date': market_date,
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'running',
            'symbols_total': 2806,
            'symbols_succeeded': 2806,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-06-05T06:41:47+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': market_date,
                'markets': ['KOSPI', 'KOSDAQ'],
                'stage': 'signal_decider_evaluations',
                'current_step': 'signal_decider_evaluations_running',
                'current_activity': 'Running signal-decider evaluation follow-up',
                'last_progress_at': '2026-06-05T07:06:00+00:00',
                'stage_progress': {
                    'price_collection': {
                        'status': 'completed',
                        'processed': 2806,
                        'total': 2806,
                        'succeeded': 2806,
                        'failed': 0,
                        'updated_at': '2026-06-05T06:35:00+00:00',
                    },
                    'market_risk_filter': {
                        'status': 'completed',
                        'processed': 2806,
                        'total': 2806,
                        'succeeded': 2806,
                        'failed': 0,
                        'updated_at': '2026-06-05T06:55:00+00:00',
                    },
                    'signal_decider_evaluations': {
                        'status': 'running',
                        'started_at': '2026-06-05T07:05:30+00:00',
                        'updated_at': '2026-06-05T07:06:00+00:00',
                        'current_step': 'signal_decider_evaluations_running',
                        'current_activity': 'Running signal-decider evaluation follow-up',
                        'processed': 14,
                        'total': 228,
                        'succeeded': 14,
                        'failed': 0,
                        'source_run_ids': {'signal_decider_evaluation_run_id': 'recent-coverage'},
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {
                'price_collection': {'persisted': 2806, 'updated_at': '2026-06-05T06:35:00+00:00'},
                'technical_snapshots': {'persisted': 2806, 'updated_at': '2026-06-05T06:45:00+00:00'},
                'action_summaries': {'persisted': 2806, 'updated_at': '2026-06-05T06:50:00+00:00'},
                'trending_ranks': {'persisted': 8418, 'periods_persisted': 3, 'updated_at': '2026-06-05T06:52:00+00:00'},
            },
            live_symbol_count_loader=lambda: 4480,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 2806,
        )

        result = service.read(market_date=market_date)

        self.assertIsNone(result['official_run'])
        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['running_stages'], 1)
        self.assertEqual(result['current_stage'], 'signal_decider_evaluations')
        stage = result['stages']['signal_decider_evaluations']
        self.assertEqual(stage['status'], 'running')
        self.assertEqual(stage['processed'], 14)
        self.assertEqual(stage['total'], 228)
        self.assertEqual(stage['current_activity'], 'Running signal-decider evaluation follow-up')
        self.assertEqual(stage['source_run_ids']['eod_run_id'], 311)
        self.assertEqual(stage['source_run_ids']['signal_decider_evaluation_run_id'], 'recent-coverage')
        self.assertEqual(stage['details']['mode'], 'eod_collection_run_stage_progress')
        self.assertEqual(stage['details']['progress_source'], 'live_run_metadata')
        self.assertEqual(stage['details']['last_progress_at'], '2026-06-05T07:06:00+00:00')
        self.assertEqual(stage['details']['stale_running_threshold_minutes'], 30)

    def test_read_downgrades_failed_pykrx_stage_when_progress_row_was_left_running(self):
        pykrx_run = {
            'id': 202,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'failed',
            'symbols_total': 3,
            'symbols_succeeded': 1,
            'symbols_failed': 2,
            'error_message': 'Marked stale after 30 minutes without completion.',
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'stage': 'technical_snapshots',
                'stage_progress': {
                    'price_collection': {
                        'status': 'completed',
                        'processed': 3,
                        'total': 3,
                        'succeeded': 3,
                        'failed': 0,
                    },
                    'technical_snapshots': {
                        'status': 'running',
                        'processed': 1,
                        'total': 3,
                        'succeeded': 1,
                        'failed': 0,
                        'current_symbol': '000660',
                        'current_step': 'computing_symbol_snapshot',
                        'current_activity': 'Computing daily technical snapshots for 000660',
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'failed')
        self.assertEqual(
            result['stages']['technical_snapshots']['current_activity'],
            'Marked stale after 30 minutes without completion.',
        )

    def test_read_uses_failed_refresh_error_message_instead_of_running_activity(self):
        fundamentals_run = {
            'id': 77,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'failed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3579,
            'symbols_checked': 120,
            'symbols_missing': 10,
            'symbols_stale': 35,
            'symbols_fresh': 60,
            'symbols_refreshed': 15,
            'symbols_failed': 2,
            'events_detected': 440,
            'events_captured': 37,
            'events_failed': 1,
            'error_message': 'Marked failed after exceeding the stale running threshold before a daily refresh status read',
            'started_at': '2026-04-21T09:21:00+00:00',
            'completed_at': '2026-04-21T10:21:00+00:00',
            'run_metadata': {
                'symbols_selected': 200,
                'stage': 'capturing_fundamentals',
                'capture_event_count': 75,
                'retry_cycle': 2,
                'retry_cycles_requested': 3,
                'batch_index': 1,
                'batch_count': 2,
                'capture_run_id': 700,
                'capture_run_ids': [699, 700],
                'sync_runs': [{'run_id': 880}, {'run_id': 881}],
                'remaining_candidate_events': 403,
                'last_progress_at': '2026-04-21T09:22:10+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
        )

        result = service.read()

        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'failed')
        self.assertEqual(
            result['stages']['fundamentals_daily_refresh']['current_activity'],
            'Marked failed after exceeding the stale running threshold before a daily refresh status read',
        )

    def test_read_marks_completed_core_pipeline_partial_when_factor_rows_are_missing(self):
        pykrx_run = {
            'id': 303,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 3, 'updated_at': '2026-04-21T09:35:00+00:00'},
            'action_summaries': {'persisted': 3, 'updated_at': '2026-04-21T09:45:00+00:00'},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 88,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2,
            'symbols_refreshed': 1,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T10:01:00+00:00',
            'completed_at': '2026-04-21T10:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 3,
                'last_progress_at': '2026-04-21T10:05:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'completed')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'pending')
        self.assertEqual(result['stages']['value_factors']['status'], 'pending')
        self.assertEqual(result['stages']['composite_factors']['status'], 'pending')
        self.assertEqual(
            result['stage_order'],
            [
                'master_refresh',
                'pykrx_universe_refresh',
                'price_collection',
                'pykrx_enrichment',
                'technical_snapshots',
                'action_summaries',
                'trending_ranks',
                'market_risk_filter',
                'fundamentals_daily_refresh',
                'piotroski_filter',
                'momentum_factors',
                'value_factors',
                'low_risk_factors',
                'beta_factors',
                'advanced_value_factors',
                'unified_value_factors',
                'quality_factors',
                'composite_factors',
                'signal_features',
                'external_news_collect',
                'external_news_enrich',
                'macro_context_refresh',
                'agent_feedback_summaries',
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )

    def test_read_keeps_blocked_factor_stage_pending_when_stored_rows_exist_before_fundamentals(self):
        pykrx_run = {
            'id': 304,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 3, 'updated_at': '2026-04-21T09:35:00+00:00'},
            'action_summaries': {'persisted': 3, 'updated_at': '2026-04-21T09:45:00+00:00'},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        factor_stage_counts = {
            'momentum_factors': {
                'persisted': 2,
                'updated_at': '2026-04-21T10:10:00+00:00',
            },
            'advanced_value_factors': {
                'persisted': 3,
                'updated_at': '2026-04-21T10:15:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            factor_stage_counts_loader=lambda **_: factor_stage_counts,
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=3,
                market_count=2,
                updated_at='2026-04-21T10:00:00+00:00',
            ),
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'partial')
        self.assertEqual(
            result['stages']['momentum_factors']['current_activity'],
            'Stored 2/3 rows for momentum factor compute',
        )
        self.assertEqual(result['stages']['advanced_value_factors']['status'], 'pending')
        self.assertEqual(
            result['stages']['advanced_value_factors']['current_activity'],
            'Waiting on DART fundamentals daily refresh',
        )
        self.assertEqual(
            result['stages']['advanced_value_factors']['details']['blocking_stages'],
            ['fundamentals_daily_refresh'],
        )
        self.assertEqual(
            result['stages']['advanced_value_factors']['source_status'],
            'stored_rows_present',
        )
        self.assertIsNone(result['stages']['advanced_value_factors']['completed_at'])

    def test_read_keeps_blocked_factor_stage_pending_when_dependency_failed(self):
        pykrx_run = {
            'id': 305,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 3, 'updated_at': '2026-04-21T09:35:00+00:00'},
            'action_summaries': {'persisted': 3, 'updated_at': '2026-04-21T09:45:00+00:00'},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 89,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'failed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_missing': 1,
            'symbols_stale': 1,
            'symbols_fresh': 1,
            'symbols_refreshed': 0,
            'symbols_failed': 1,
            'events_detected': 5,
            'events_captured': 2,
            'events_failed': 1,
            'error_message': 'Fundamentals refresh failed',
            'started_at': '2026-04-21T10:01:00+00:00',
            'completed_at': '2026-04-21T10:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 3,
                'last_progress_at': '2026-04-21T10:05:00+00:00',
            },
        }
        factor_stage_counts = {
            'advanced_value_factors': {
                'persisted': 3,
                'updated_at': '2026-04-21T10:15:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            factor_stage_counts_loader=lambda **_: factor_stage_counts,
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'failed')
        self.assertEqual(result['stages']['advanced_value_factors']['status'], 'pending')
        self.assertEqual(
            result['stages']['advanced_value_factors']['current_activity'],
            'Blocked by failed DART fundamentals daily refresh',
        )
        self.assertEqual(
            result['stages']['advanced_value_factors']['details']['failed_blocking_stages'],
            ['fundamentals_daily_refresh'],
        )
        self.assertIsNone(result['stages']['advanced_value_factors']['completed_at'])

    def test_completed_official_factor_is_not_replaced_by_stale_dependency_wait(self):
        market_date = '2026-09-28'
        updated_at = '2026-09-28T20:00:00+00:00'
        fundamentals_item = _completed_official_stage(
            run_id=480,
            stage_id='fundamentals_daily_refresh',
            market_date=market_date,
            processed=0,
            total=0,
            updated_at=updated_at,
        )
        fundamentals_item['stage']['details'] = {
            'result_summary': {
                'triggered': False,
                'skipped': True,
                'safe_skip': True,
                'reason': 'point_in_time_recovery_current_only_stage',
            }
        }
        affected_stage_ids = (
            'piotroski_filter',
            'advanced_value_factors',
            'unified_value_factors',
            'quality_factors',
            'composite_factors',
        )
        stage_history = {
            'fundamentals_daily_refresh': fundamentals_item,
            **{
                stage_id: _completed_official_stage(
                    run_id=480,
                    stage_id=stage_id,
                    market_date=market_date,
                    processed=3,
                    total=3,
                    updated_at=updated_at,
                )
                for stage_id in affected_stage_ids
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {
                'price_collection': {'persisted': 3, 'updated_at': updated_at},
            },
            live_symbol_count_loader=lambda: 3,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: stage_history,
            price_bar_count_loader=lambda **_: 3,
            row_count_stage_counts_loader=lambda **_: {
                stage_id: {'persisted': 3, 'updated_at': updated_at}
                for stage_id in (
                    'piotroski_filter',
                    *DEFAULT_FACTOR_STAGE_ORDER,
                )
            },
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=3,
                market_count=2,
                updated_at=updated_at,
            ),
        )

        result = service.read(market_date=market_date)

        for stage_id in affected_stage_ids:
            with self.subTest(stage_id=stage_id):
                factor_stage = result['stages'][stage_id]
                self.assertEqual(factor_stage['status'], 'completed')
                self.assertEqual(
                    factor_stage['details']['mode'],
                    'official_daily_pipeline_run_state',
                )
                self.assertEqual(
                    factor_stage['details'].get('blocking_stages', []),
                    [],
                )

    def test_completed_eod_progress_factor_is_not_replaced_by_stale_dependency_wait(self):
        market_date = '2026-09-28'
        updated_at = '2026-09-28T20:00:00+00:00'
        affected_stage_ids = (
            'piotroski_filter',
            'advanced_value_factors',
            'unified_value_factors',
            'quality_factors',
            'composite_factors',
        )
        completed_progress = {
            'status': 'completed',
            'processed': 3,
            'total': 3,
            'succeeded': 3,
            'failed': 0,
            'updated_at': updated_at,
            'result_summary': {'results_persisted': 3, 'results_failed': 0},
        }
        pykrx_run = {
            'id': 552,
            'market': 'KRX',
            'trade_date': market_date,
            'source': 'pykrx',
            'trigger': 'internal_ops_cli',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': updated_at,
            'completed_at': updated_at,
            'run_metadata': {
                'mode': 'daily_auto',
                'last_progress_at': updated_at,
                'stage_progress': {
                    'fundamentals_daily_refresh': {
                        'status': 'completed',
                        'processed': 0,
                        'total': 0,
                        'succeeded': 0,
                        'failed': 0,
                        'updated_at': updated_at,
                        'result_summary': {
                            'triggered': False,
                            'skipped': True,
                            'safe_skip': True,
                            'reason': 'point_in_time_recovery_current_only_stage',
                        },
                    },
                    **{
                        stage_id: dict(completed_progress)
                        for stage_id in affected_stage_ids
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {
                'price_collection': {'persisted': 3, 'updated_at': updated_at},
            },
            live_symbol_count_loader=lambda: 3,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 3,
            row_count_stage_counts_loader=lambda **_: {
                stage_id: {'persisted': 3, 'updated_at': updated_at}
                for stage_id in (
                    'piotroski_filter',
                    *DEFAULT_FACTOR_STAGE_ORDER,
                )
            },
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=3,
                market_count=2,
                updated_at=updated_at,
            ),
        )

        result = service.read(market_date=market_date)

        for stage_id in affected_stage_ids:
            with self.subTest(stage_id=stage_id):
                factor_stage = result['stages'][stage_id]
                self.assertEqual(factor_stage['status'], 'completed')
                self.assertEqual(
                    factor_stage['details']['mode'],
                    'eod_collection_run_stage_progress',
                )
                self.assertEqual(
                    factor_stage['details'].get('blocking_stages', []),
                    [],
                )

    def test_read_prefers_stored_price_bar_count_when_price_stage_metadata_is_stale(self):
        pykrx_run = {
            'id': 306,
            'market': 'KRX',
            'trade_date': '2026-05-06',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2811,
            'symbols_succeeded': 2811,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T06:10:00+00:00',
            'completed_at': '2026-05-06T07:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-05-06',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 2810, 'updated_at': '2026-05-06T06:20:00+00:00'},
            'technical_snapshots': {'persisted': 2810, 'updated_at': '2026-05-06T06:35:00+00:00'},
            'action_summaries': {'persisted': 2810, 'updated_at': '2026-05-06T06:45:00+00:00'},
            'trending_ranks': {'persisted': 8430, 'periods_persisted': 3, 'updated_at': '2026-05-06T06:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 90,
            'market_date': '2026-05-06',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 2810,
            'symbols_checked': 2810,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2750,
            'symbols_refreshed': 60,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T07:01:00+00:00',
            'completed_at': '2026-05-06T07:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 2810,
                'last_progress_at': '2026-05-06T07:05:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            row_count_stage_counts_loader=lambda **_: _full_feedback_prerequisite_stage_counts(
                total=2810,
                updated_at='2026-05-06T07:15:00+00:00',
            ),
            price_bar_count_loader=lambda **_: 2810,
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=2810,
                market_count=2,
                updated_at='2026-05-06T07:15:00+00:00',
            ),
        )

        result = service.read(market_date='2026-05-06')

        self.assertEqual(result['stages']['momentum_factors']['status'], 'completed')
        self.assertEqual(result['stages']['unified_value_factors']['status'], 'completed')
        self.assertEqual(result['stages']['quality_factors']['status'], 'completed')
        self.assertEqual(result['stages']['momentum_factors']['details']['expected_rows'], 2810)
        self.assertEqual(
            result['stages']['momentum_factors']['details']['expected_rows_source'],
            'daily_price_bars',
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_bars_persisted'],
            2810,
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_stage_expected_rows'],
            2810,
        )

    def test_read_prefers_date_scoped_price_bars_over_limited_retry_run_counts_for_factor_progress(self):
        pykrx_run = {
            'id': 307,
            'market': 'KRX',
            'trade_date': '2026-05-06',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 100,
            'symbols_succeeded': 100,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T06:10:00+00:00',
            'completed_at': '2026-05-06T07:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-05-06',
                'requested_start_date': '20260506',
                'requested_end_date': '20260506',
                'limited_run': True,
                'retry': {
                    'retry_of_run_id': 300,
                    'retry_cycle': 2,
                    'symbol_count': 100,
                },
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 100, 'updated_at': '2026-05-06T06:20:00+00:00'},
            'technical_snapshots': {'persisted': 100, 'updated_at': '2026-05-06T06:35:00+00:00'},
            'action_summaries': {'persisted': 100, 'updated_at': '2026-05-06T06:45:00+00:00'},
            'trending_ranks': {'persisted': 300, 'periods_persisted': 3, 'updated_at': '2026-05-06T06:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 91,
            'market_date': '2026-05-06',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 2810,
            'symbols_checked': 2810,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2750,
            'symbols_refreshed': 60,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T07:01:00+00:00',
            'completed_at': '2026-05-06T07:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 2810,
                'last_progress_at': '2026-05-06T07:05:00+00:00',
            },
        }
        factor_stage_counts = {
            'momentum_factors': {
                'persisted': 1000,
                'updated_at': '2026-05-06T07:15:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            factor_stage_counts_loader=lambda **_: factor_stage_counts,
            price_bar_count_loader=lambda **_: 2810,
        )

        result = service.read(market_date='2026-05-06')

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'partial')
        self.assertEqual(result['stages']['momentum_factors']['total'], 2810)
        self.assertEqual(
            result['stages']['momentum_factors']['current_activity'],
            'Stored 1000/2810 rows for momentum factor compute',
        )
        self.assertEqual(result['stages']['momentum_factors']['details']['expected_rows'], 2810)
        self.assertEqual(
            result['stages']['momentum_factors']['details']['expected_rows_source'],
            'daily_price_bars',
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_bars_persisted'],
            2810,
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_stage_expected_rows'],
            2810,
        )

    def test_read_marks_market_risk_gap_non_blocking_when_feedback_running_cleanly(self):
        pykrx_run = {
            'id': 390,
            'market': 'KRX',
            'trade_date': '2026-05-27',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2806,
            'symbols_succeeded': 2806,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-05-27T06:10:00+00:00',
            'completed_at': '2026-05-27T07:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-05-27',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 2806, 'updated_at': '2026-05-27T06:20:00+00:00'},
            'technical_snapshots': {'persisted': 2806, 'updated_at': '2026-05-27T06:35:00+00:00'},
            'action_summaries': {'persisted': 2806, 'updated_at': '2026-05-27T06:45:00+00:00'},
            'trending_ranks': {'persisted': 8418, 'periods_persisted': 3, 'updated_at': '2026-05-27T06:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 3901,
            'market_date': '2026-05-27',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 2806,
            'symbols_checked': 2806,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2800,
            'symbols_refreshed': 6,
            'symbols_failed': 0,
            'events_detected': 0,
            'events_captured': 0,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-05-27T07:01:00+00:00',
            'completed_at': '2026-05-27T07:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 2806,
                'last_progress_at': '2026-05-27T07:05:00+00:00',
            },
        }
        row_counts = _full_feedback_prerequisite_stage_counts(
            total=2806,
            updated_at='2026-05-27T07:15:00+00:00',
        )
        row_counts['market_risk_filter'] = {
            'persisted': 2805,
            'updated_at': '2026-05-27T07:15:00+00:00',
            'missing_count': 1,
            'missing_symbols_sample': ['176750'],
            'missing_symbol_details_sample': [
                {'symbol': '176750', 'name_ko': 'DuChemBio', 'market': 'KOSDAQ'},
            ],
        }
        feedback_run = {
            'source': 'runtime_feedback_state',
            'run_id': 'feedback-cycle-2026-05-27-c21e78a409',
            'status': 'running',
            'requested_symbols': ['005930', '000660'],
            'summary': {
                'run_status': 'running',
                'run_table_status': 'running',
                'current_role': 'fundamental-summarizer',
                'failed_items': 0,
                'completed_feedback_count': 280,
                'expected_feedback_count': 565,
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            row_count_stage_counts_loader=lambda **_: row_counts,
            price_bar_count_loader=lambda **_: 2806,
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=2806,
                market_count=2,
                updated_at='2026-05-27T06:25:00+00:00',
            ),
            feedback_run_loader=lambda **_: feedback_run,
            row_count_scope_gap_loader=lambda **_: {
                'missing_count': 0,
                'missing_symbols_sample': [],
                'missing_symbol_details_sample': [],
            },
        )

        result = service.read(market_date='2026-05-27')

        self.assertEqual(result['status'], 'running')
        market_risk_stage = result['stages']['market_risk_filter']
        self.assertEqual(market_risk_stage['status'], 'partial')
        self.assertEqual(market_risk_stage['gate_status'], 'ready')
        self.assertEqual(market_risk_stage['severity'], 'degraded')
        self.assertEqual(market_risk_stage['blocking_status'], 'non_blocking')
        self.assertEqual(market_risk_stage['details']['missing_count'], 1)
        self.assertEqual(market_risk_stage['details']['missing_symbols_sample'], ['176750'])
        self.assertEqual(
            market_risk_stage['details']['gate_reason'],
            'missing_rows_outside_active_feedback_scope',
        )
        self.assertEqual(market_risk_stage['details']['feedback_scope_gap']['missing_count'], 0)
        feedback_stage = result['stages']['agent_feedback_summaries']
        self.assertEqual(feedback_stage['status'], 'running')
        self.assertEqual(feedback_stage['failed'], 0)
        self.assertEqual(feedback_stage['details']['feedback_failure_status'], 'no_failures')

    def test_read_reconciles_agent_feedback_stage_from_child_progress_when_run_snapshot_is_stale(self):
        roles = [
            'eod-summarizer',
            'technical-summarizer',
            'fundamental-summarizer',
            'short-term-decider',
            'signal-decider',
        ]
        stale_run_snapshot = {
            'run_id': 'symbol-run-2026-06-19-stale',
            'status': 'running',
            'symbols_total': 2,
            'completed_count': 0,
            'failed_count': 0,
            'started_at': '2026-06-19T07:53:00+00:00',
            'completed_at': None,
            'request_payload': {
                'mode': 'role_sequential_runtime_buffered',
                'symbols': ['005930', '000660'],
                'requested_symbols': ['005930', '000660'],
                'roles': roles,
            },
            'summary_payload': {
                'progress_summary': {
                    'run_id': 'symbol-run-2026-06-19-stale',
                    'run_status': 'running',
                    'run_table_status': 'running',
                    'current_role': 'eod-summarizer',
                    'completed_feedback_count': 0,
                    'failed_items': 0,
                    'expected_feedback_count': 10,
                    'latest_feedback_at': '2026-06-19T07:53:00+00:00',
                },
            },
        }
        reconciled_progress = {
            'summary': {
                'run_id': 'symbol-run-2026-06-19-stale',
                'run_status': 'running',
                'run_table_status': 'running',
                'is_live': True,
                'run_started_at': '2026-06-19T07:53:00+00:00',
                'current_role': 'eod-summarizer',
                'completed_roles': [],
                'remaining_roles': roles,
                'total_symbols': 2,
                'processed_symbols': 2,
                'completed_symbols': 0,
                'partial_symbols': 2,
                'in_progress_symbols': 0,
                'pending_symbols': 0,
                'completed_feedback_count': 2,
                'failed_items': 1,
                'expected_feedback_count': 10,
                'latest_feedback_at': '2026-06-19T09:39:03+00:00',
                'latest_failed_feedback_at': '2026-06-19T09:38:00+00:00',
                'failure_groups': {
                    'transport/timeout': {
                        'failure_status': 'transport/timeout',
                        'count': 1,
                        'symbols_sample': ['000660'],
                    },
                },
                'role_progress': {
                    'eod-summarizer': {
                        'role': 'eod-summarizer',
                        'status': 'completed',
                        'total_symbols': 2,
                        'processed_symbols': 2,
                        'completed_symbols': 2,
                        'pending_symbols': 0,
                        'failed_items': 0,
                    },
                    'technical-summarizer': {
                        'role': 'technical-summarizer',
                        'status': 'partial',
                        'total_symbols': 2,
                        'processed_symbols': 1,
                        'completed_symbols': 0,
                        'pending_symbols': 1,
                        'failed_items': 1,
                    },
                    'fundamental-summarizer': {'role': 'fundamental-summarizer', 'status': 'pending', 'total_symbols': 2},
                    'short-term-decider': {'role': 'short-term-decider', 'status': 'pending', 'total_symbols': 2},
                    'signal-decider': {'role': 'signal-decider', 'status': 'pending', 'total_symbols': 2},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            pykrx_enrichment_loader=lambda **_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            official_stage_history_loader=lambda **_: {},
            row_count_stage_counts_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 0,
            signal_feature_counts_loader=lambda **_: {},
            external_news_counts_loader=lambda **_: {},
            signal_decider_counts_loader=lambda **_: {},
            feedback_run_loader=_load_latest_feedback_run_summary,
        )

        with patch(
            'app.agent_context.runtime_state.runtime_feedback_state_store.get_run',
            return_value=None,
        ), patch(
            'app.agent_context.run_state_service.agent_context_feedback_run_state_service.find_latest_run',
            return_value=stale_run_snapshot,
        ) as latest_run_loader, patch(
            'app.agent_context.progress_service.agent_context_progress_service.get_feedback_progress',
            return_value=reconciled_progress,
        ) as progress_loader:
            result = service.read(market_date='2026-06-19')

        self.assertEqual(
            latest_run_loader.call_args.kwargs['request_modes'],
            [DAILY_RETRY_CYCLE_MODE],
        )
        self.assertEqual(
            latest_run_loader.call_args.kwargs['requested_roles'],
            [
                'eod-summarizer',
                'technical-summarizer',
                'fundamental-summarizer',
            ],
        )
        progress_loader.assert_called_once()
        self.assertIsNone(progress_loader.call_args.kwargs.get('symbols'))
        self.assertEqual(
            progress_loader.call_args.kwargs['run_id'],
            'symbol-run-2026-06-19-stale',
        )
        stage = result['stages']['agent_feedback_summaries']
        self.assertEqual(stage['status'], 'running')
        self.assertEqual(stage['updated_at'], '2026-06-19T09:39:03+00:00')
        self.assertEqual(stage['processed'], 3)
        self.assertEqual(stage['total'], 10)
        self.assertEqual(stage['succeeded'], 2)
        self.assertEqual(stage['failed'], 1)
        self.assertEqual(stage['details']['current_active_roles'], ['eod-summarizer'])
        self.assertEqual(stage['details']['target_symbols'], 2)
        self.assertEqual(stage['details']['completed_symbols'], 0)
        self.assertEqual(stage['details']['failed_symbols_count'], 1)
        self.assertEqual(stage['details']['latest_successful_feedback_at'], '2026-06-19T09:39:03+00:00')
        self.assertEqual(stage['details']['latest_failed_feedback_at'], '2026-06-19T09:38:00+00:00')
        self.assertEqual(stage['details']['role_progress']['eod-summarizer']['completed_symbols'], 2)
        self.assertIn('fundamental-summarizer', stage['details']['role_progress'])
        self.assertIn('short-term-decider', stage['details']['role_progress'])
        self.assertIn('signal-decider', stage['details']['role_progress'])

    def test_read_treats_stored_full_coverage_as_completed_after_scoped_retry_run(self):
        pykrx_run = {
            'id': 401,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            'symbols_total': 877,
            'symbols_succeeded': 874,
            'symbols_failed': 3,
            'error_message': 'Failed symbols: 139050,217620,452980',
            'started_at': '2026-04-21T09:50:00+00:00',
            'completed_at': '2026-04-21T09:55:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'stage_progress': {
                    'price_collection': {'status': 'partial', 'processed': 3, 'total': 3, 'succeeded': 0, 'failed': 3},
                    'technical_snapshots': {'status': 'partial', 'processed': 3, 'total': 3, 'succeeded': 0, 'failed': 3},
                    'action_summaries': {'status': 'partial', 'processed': 3, 'total': 3, 'succeeded': 0, 'failed': 3},
                },
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 0, 'updated_at': None},
            'technical_snapshots': {'persisted': 0, 'updated_at': None},
            'action_summaries': {'persisted': 0, 'updated_at': None},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 4458,
            fundamentals_refresh_loader=lambda **_: None,
            official_run_loader=lambda **_: None,
            price_bar_count_loader=lambda **_: 3683,
            technical_snapshot_count_loader=lambda _: 3683,
            action_summary_count_loader=lambda _: 3683,
            row_count_stage_counts_loader=lambda **_: {
                'market_risk_filter': {'persisted': 3683, 'updated_at': '2026-04-21T10:15:00+00:00'},
                'piotroski_filter': {'persisted': 3683, 'updated_at': '2026-04-21T10:15:00+00:00'},
            },
        )

        result = service.read()

        for stage_id in ('price_collection', 'technical_snapshots', 'action_summaries'):
            self.assertEqual(result['stages'][stage_id]['status'], 'completed')
            self.assertEqual(result['stages'][stage_id]['total'], 3683)
            self.assertEqual(result['stages'][stage_id]['succeeded'], 3683)
            self.assertEqual(result['stages'][stage_id]['failed'], 0)
        self.assertEqual(result['stages']['market_risk_filter']['status'], 'completed')
        self.assertEqual(result['stages']['market_risk_filter']['details']['blocking_stages'], [])

    def test_read_maps_completed_pipeline_stages_from_completed_run_and_aggregate_counts(self):
        pykrx_run = {
            'id': 303,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 3, 'updated_at': '2026-04-21T09:35:00+00:00'},
            'action_summaries': {'persisted': 3, 'updated_at': '2026-04-21T09:45:00+00:00'},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 88,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2,
            'symbols_refreshed': 1,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T10:01:00+00:00',
            'completed_at': '2026-04-21T10:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 3,
                'last_progress_at': '2026-04-21T10:05:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            official_run_loader=lambda **_: None,
            row_count_stage_counts_loader=lambda **_: _full_feedback_prerequisite_stage_counts(
                total=3,
                updated_at='2026-04-21T10:15:00+00:00',
            ),
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=3,
                market_count=2,
                updated_at='2026-04-21T10:12:00+00:00',
            ),
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['total_stages'], 26)
        self.assertEqual(result['stage_status_counts']['completed'], 16)
        self.assertEqual(result['skipped_stages'], 2)
        self.assertEqual(result['partial_stages'], 0)
        self.assertEqual(result['pending_stages'], 8)
        self.assertEqual(result['current_stage'], 'signal_features')
        self.assertEqual(result['stages']['master_refresh']['status'], 'skipped')
        self.assertEqual(result['stages']['pykrx_universe_refresh']['status'], 'skipped')
        self.assertEqual(result['stages']['price_collection']['status'], 'completed')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'completed')
        self.assertEqual(result['stages']['action_summaries']['status'], 'completed')
        self.assertEqual(result['stages']['trending_ranks']['status'], 'completed')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'completed')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'completed')
        self.assertEqual(result['stages']['composite_factors']['status'], 'completed')
        self.assertEqual(result['stages']['trending_ranks']['processed'], 3)
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['succeeded'], 3)

    def test_read_uses_persisted_outputs_when_latest_official_run_is_single_stage_resume(self):
        market_date = '2026-05-28'
        pykrx_run = {
            'id': 400,
            'market': 'KRX',
            'trade_date': market_date,
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2806,
            'symbols_succeeded': 2806,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-05-28T06:10:00+00:00',
            'completed_at': '2026-05-28T07:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': market_date,
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 2806, 'updated_at': '2026-05-28T06:20:00+00:00'},
            'technical_snapshots': {'persisted': 2806, 'updated_at': '2026-05-28T06:35:00+00:00'},
            'action_summaries': {'persisted': 2806, 'updated_at': '2026-05-28T06:45:00+00:00'},
            'trending_ranks': {'persisted': 8418, 'periods_persisted': 3, 'updated_at': '2026-05-28T06:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 4001,
            'market_date': market_date,
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 2806,
            'symbols_checked': 2806,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2800,
            'symbols_refreshed': 6,
            'symbols_failed': 0,
            'events_detected': 0,
            'events_captured': 0,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-05-28T07:01:00+00:00',
            'completed_at': '2026-05-28T07:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 2806,
                'last_progress_at': '2026-05-28T07:05:00+00:00',
            },
        }
        official_run = {
            'id': 9,
            'market_date': market_date,
            'trigger': 'manual_api',
            'status': 'completed',
            'current_stage': 'signal_decider_evaluations',
            'started_at': '2026-05-28T09:00:00+00:00',
            'updated_at': '2026-05-28T09:05:00+00:00',
            'completed_at': '2026-05-28T09:05:00+00:00',
            'error_message': None,
            'run_metadata': {
                'requested_stages': ['signal_decider_evaluations'],
                'resolved_stages': ['signal_decider_evaluations'],
            },
            'stages': {
                'signal_decider_evaluations': {
                    'stage_id': 'signal_decider_evaluations',
                    'status': 'completed',
                    'market_date': market_date,
                    'started_at': '2026-05-28T09:00:00+00:00',
                    'updated_at': '2026-05-28T09:05:00+00:00',
                    'completed_at': '2026-05-28T09:05:00+00:00',
                    'processed': 228,
                    'total': 228,
                    'succeeded': 228,
                    'failed': 0,
                    'remaining': 0,
                    'current_step': None,
                    'current_activity': None,
                    'current_symbol': None,
                    'current_period': None,
                    'source_run_ids': {},
                    'details': {},
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 4480,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            row_count_stage_counts_loader=lambda **_: _full_feedback_prerequisite_stage_counts(
                total=2806,
                updated_at='2026-05-28T07:15:00+00:00',
            ),
            price_bar_count_loader=lambda **_: 2806,
            pykrx_enrichment_loader=lambda **_: _blocked_pykrx_enrichment_coverage(
                expected_symbols=4480,
                market_count=3,
                updated_at='2026-05-28T06:25:00+00:00',
            ),
            official_run_loader=lambda **_: official_run,
            official_stage_history_loader=lambda **_: {
                'agent_feedback_summaries': _completed_official_stage(
                    run_id=8,
                    stage_id='agent_feedback_summaries',
                    market_date=market_date,
                    processed=570,
                    total=570,
                ),
                'next_open_execution_plan': _completed_official_stage(
                    run_id=8,
                    stage_id='next_open_execution_plan',
                    market_date=market_date,
                    processed=114,
                    total=114,
                ),
            },
            signal_feature_counts_loader=lambda **_: {
                'persisted': 2823,
                'updated_at': '2026-05-28T06:30:00+00:00',
            },
            external_news_counts_loader=lambda **_: {
                'items': 1091,
                'symbols': 114,
                'classified_items': 1091,
                'updated_at': '2026-05-28T06:40:00+00:00',
            },
            macro_context_counts_loader=lambda **_: _available_macro_context_counts(
                updated_at='2026-05-28T06:50:00+00:00',
            ),
            signal_decider_counts_loader=lambda **_: {
                'feedback_rows': 114,
                'feedback_symbols': 114,
                'decision_snapshots': 228,
                'decision_snapshot_symbols': 114,
                'updated_at': '2026-05-28T08:50:00+00:00',
            },
        )

        result = service.read(market_date=market_date)

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['total_stages'], 26)
        self.assertEqual(result['completed_stages'], 24)
        self.assertEqual(result['skipped_stages'], 2)
        self.assertEqual(result['display_completed_stages'], 26)
        self.assertEqual(result['pending_stages'], 0)
        self.assertEqual(result['partial_stages'], 0)
        self.assertEqual(result['running_stages'], 0)
        self.assertIsNone(result['current_stage'])
        self.assertEqual(result['stages']['master_refresh']['status'], 'skipped')
        self.assertEqual(
            result['stages']['master_refresh']['details']['reason'],
            'preopen_global_stage_not_required_for_resumed_market_date_status',
        )
        self.assertEqual(result['stages']['signal_features']['status'], 'completed')
        self.assertEqual(result['stages']['signal_features']['details']['rows_persisted'], 2823)
        self.assertEqual(result['stages']['external_news_collect']['status'], 'completed')
        self.assertEqual(result['stages']['external_news_collect']['details']['items_persisted'], 1091)
        self.assertEqual(result['stages']['external_news_enrich']['status'], 'completed')
        self.assertEqual(result['stages']['external_news_enrich']['details']['classified_items'], 1091)
        self.assertEqual(result['stages']['macro_context_refresh']['status'], 'completed')
        self.assertEqual(result['stages']['macro_context_refresh']['details']['persisted_series_count'], 3)
        self.assertEqual(result['stages']['signal_decider']['status'], 'completed')
        self.assertEqual(result['stages']['signal_decider']['details']['feedback_rows'], 114)
        self.assertEqual(result['stages']['signal_decider']['details']['decision_snapshots'], 228)
        self.assertEqual(result['stages']['signal_decider_evaluations']['status'], 'completed')
        pykrx_stage = result['stages']['pykrx_enrichment']
        self.assertEqual(pykrx_stage['status'], 'completed')
        self.assertEqual(pykrx_stage['gate_status'], 'blocked')
        self.assertEqual(pykrx_stage['details']['decider_category_gate_status'], 'blocked')
        self.assertEqual(result['gate_summary']['status'], 'blocked')
        self.assertEqual(result['gate_summary']['blocked_stage_ids'], ['pykrx_enrichment'])
        blocking_categories = pykrx_stage['details']['decider_blocking_categories']
        self.assertIn('sector_classification', {item['category'] for item in blocking_categories})
        self.assertEqual(
            blocking_categories[0]['reason'],
            'incomplete_required_pykrx_enrichment_category',
        )

    def test_read_preserves_incomplete_official_history_for_persisted_output_stage(self):
        market_date = '2026-05-28'
        resumed_stage = _completed_official_stage(
            run_id=22,
            stage_id='signal_decider_evaluations',
            market_date=market_date,
            processed=10,
            total=10,
            updated_at='2026-05-28T09:05:00+00:00',
        )['stage']
        latest_resume_run = {
            'id': 22,
            'market_date': market_date,
            'trigger': 'manual_api',
            'status': 'completed',
            'current_stage': 'signal_decider_evaluations',
            'started_at': '2026-05-28T09:00:00+00:00',
            'updated_at': '2026-05-28T09:05:00+00:00',
            'completed_at': '2026-05-28T09:05:00+00:00',
            'error_message': None,
            'run_metadata': {
                'requested_stages': ['signal_decider_evaluations'],
                'resolved_stages': ['signal_decider_evaluations'],
            },
            'stages': {'signal_decider_evaluations': resumed_stage},
        }

        for official_status in ('partial', 'failed'):
            with self.subTest(official_status=official_status):
                service = EODPipelineStatusService(
                    market_state_loader=lambda: _FakeMarketState(),
                    pykrx_run_loader=lambda **_: None,
                    pykrx_stage_counts_loader=lambda *_: {},
                    live_symbol_count_loader=lambda: 10,
                    fundamentals_refresh_loader=lambda **_: None,
                    official_run_loader=lambda **_: latest_resume_run,
                    official_stage_history_loader=lambda **_: {
                        'signal_features': _incomplete_official_stage(
                            run_id=21,
                            stage_id='signal_features',
                            market_date=market_date,
                            status=official_status,
                            updated_at='2026-05-28T08:45:00+00:00',
                            succeeded=3,
                            failed=2,
                        ),
                    },
                    signal_feature_counts_loader=lambda **_: {
                        'persisted': 3,
                        'updated_at': '2026-05-28T08:40:00+00:00',
                    },
                )

                result = service.read(market_date=market_date)

                stage = result['stages']['signal_features']
                self.assertEqual(stage['status'], official_status)
                self.assertEqual(stage['source_status'], official_status)
                self.assertEqual(stage['processed'], 5)
                self.assertEqual(stage['succeeded'], 3)
                self.assertEqual(stage['failed'], 2)
                self.assertEqual(stage['details']['features_persisted'], 3)
                self.assertEqual(stage['details']['features_failed'], 2)
                self.assertTrue(stage['details']['historical_stage_state'])
                self.assertEqual(stage['details']['progress_source'], 'official_stage_history')
                self.assertEqual(result['current_stage'], 'signal_features')
                self.assertEqual(result['stages']['signal_decider_evaluations']['status'], 'completed')

    def test_read_reports_external_news_enrich_pending_when_persisted_items_are_unclassified(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            external_news_counts_loader=lambda **_: {
                'items': 7,
                'symbols': 3,
                'classified_items': 0,
                'updated_at': '2026-05-28T06:40:00+00:00',
            },
        )

        result = service.read(market_date='2026-05-28')

        self.assertEqual(result['stages']['external_news_collect']['status'], 'completed')
        enrich_stage = result['stages']['external_news_enrich']
        self.assertEqual(enrich_stage['status'], 'pending')
        self.assertEqual(enrich_stage['source_status'], 'stored_external_news_unclassified')
        self.assertEqual(enrich_stage['details']['items_persisted'], 7)
        self.assertEqual(enrich_stage['details']['classified_items'], 0)
        self.assertEqual(enrich_stage['details']['reason'], 'external_news_items_need_classification')

    def test_read_reports_external_news_enrich_partial_when_only_some_items_have_contract_classification(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 10,
            fundamentals_refresh_loader=lambda **_: None,
            external_news_counts_loader=lambda **_: {
                'items': 7,
                'symbols': 3,
                'contract_classified_items': 4,
                'legacy_classified_items': 2,
                'updated_at': '2026-05-28T06:40:00+00:00',
            },
        )

        result = service.read(market_date='2026-05-28')

        enrich_stage = result['stages']['external_news_enrich']
        self.assertEqual(enrich_stage['status'], 'partial')
        self.assertEqual(enrich_stage['source_status'], 'stored_external_news_partially_classified')
        self.assertEqual(enrich_stage['processed'], 4)
        self.assertEqual(enrich_stage['total'], 7)
        self.assertEqual(enrich_stage['details']['classified_items'], 4)
        self.assertEqual(enrich_stage['details']['contract_classified_items'], 4)
        self.assertEqual(enrich_stage['details']['legacy_classified_items'], 2)
        self.assertEqual(enrich_stage['details']['classification_coverage_ratio'], 0.571429)

    def test_read_includes_pykrx_enrichment_stage_from_coverage(self):
        coverage = {
            'requested_symbol_count': 2,
            'requested_market_count': 1,
            'categories': {
                category: {
                    'present_rows': 0,
                    'expected_rows': 2 if category not in {'market_breadth', 'investor_flow_market'} else 1,
                    'coverage_status': 'missing',
                    'latest_captured_at': None,
                }
                for category in DEFAULT_PYKRX_ENRICHMENT_CATEGORIES
            },
        }
        coverage['categories']['sector_classification'] = {
            'present_rows': 2,
            'expected_rows': 2,
            'coverage_status': 'complete',
            'latest_captured_at': '2026-04-21T15:10:00+09:00',
        }
        coverage['categories']['market_breadth'] = {
            'present_rows': 1,
            'expected_rows': 1,
            'coverage_status': 'complete',
            'latest_captured_at': '2026-04-21T15:10:00+09:00',
        }

        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            factor_stage_counts_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 0,
            pykrx_enrichment_loader=lambda **_: coverage,
        )

        result = service.read(market_date='2026-04-21', fs_div='CFS')

        self.assertLess(
            result['stage_order'].index('price_collection'),
            result['stage_order'].index('pykrx_enrichment'),
        )
        self.assertEqual(result['stages']['pykrx_enrichment']['status'], 'partial')
        self.assertEqual(result['stages']['pykrx_enrichment']['details']['requested_symbol_count'], 2)
        self.assertEqual(
            result['stages']['pykrx_enrichment']['details']['categories']['sector_classification']['coverage_status'],
            'complete',
        )

    def test_read_exposes_pykrx_enrichment_stage_from_live_run_progress(self):
        pykrx_run = {
            'id': 91,
            'trade_date': '2026-04-21',
            'status': 'completed_with_errors',
            'run_metadata': {
                'stage_progress': {
                    'pykrx_enrichment': {
                        'status': 'failed',
                        'started_at': '2026-04-21T08:00:00+00:00',
                        'updated_at': '2026-04-21T08:02:00+00:00',
                        'current_step': 'pykrx_enrichment_failed',
                        'current_activity': 'PyKRX enrichment collection failed',
                        'processed': 1,
                        'total': len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                        'succeeded': 1,
                        'failed': 3,
                        'category_row_counts': {
                            'sector_classification': 1,
                            'investor_flow_symbol': 0,
                            'short_selling': 0,
                        },
                        'failed_categories': ['investor_flow_symbol', 'short_selling'],
                        'downstream_allowed': False,
                        'downstream_preflight_status': 'blocked',
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            factor_stage_counts_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 0,
            pykrx_enrichment_loader=lambda **_: {},
        )

        result = service.read(market_date='2026-04-21', fs_div='CFS')

        stage = result['stages']['pykrx_enrichment']
        self.assertEqual(stage['status'], 'failed')
        self.assertEqual(stage['source_status'], 'failed')
        self.assertEqual(stage['source_run_ids'], {'eod_run_id': 91})
        self.assertEqual(stage['details']['progress_source'], 'live_run_metadata')
        self.assertFalse(stage['details']['downstream_allowed'])
        self.assertEqual(stage['details']['downstream_preflight_status'], 'blocked')
        self.assertEqual(
            stage['details']['category_row_counts'],
            {
                'sector_classification': 1,
                'investor_flow_symbol': 0,
                'short_selling': 0,
            },
        )

    def test_macro_context_stage_reports_persisted_context_status(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 0,
            fundamentals_refresh_loader=lambda **_: None,
            price_bar_count_loader=lambda **_: 0,
            pykrx_enrichment_loader=lambda **_: {},
            macro_context_counts_loader=lambda **_: {
                'persisted': 3,
                'required_series_count': 3,
                'updated_at': '2026-06-20T06:00:00+00:00',
                'context_status': 'available',
                'source_statuses': {
                    'world_bank': {'status': 'available', 'series_count': 3},
                    'fred': {'status': 'disabled_missing_api_key'},
                },
                'macro_regime_summary': {
                    'growth_label': 'growth_expansion',
                    'inflation_label': 'inflation_stable',
                    'llm_generated': False,
                },
                'usage_policy': {
                    'contextual_input_only': True,
                    'direct_buy_sell_signal': False,
                    'llm_generated': False,
                },
            },
        )

        result = service.read(market_date='2026-06-20', fs_div='CFS')

        stage = result['stages']['macro_context_refresh']
        self.assertEqual(stage['status'], 'completed')
        self.assertEqual(stage['source_status'], 'stored_macro_context_fresh')
        self.assertEqual(stage['succeeded'], 3)
        self.assertEqual(stage['details']['context_status'], 'available')
        self.assertEqual(stage['details']['source_statuses']['fred']['status'], 'disabled_missing_api_key')
        self.assertTrue(stage['details']['usage_policy']['contextual_input_only'])
        self.assertFalse(stage['details']['usage_policy']['direct_buy_sell_signal'])
