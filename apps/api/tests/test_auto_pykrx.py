from __future__ import annotations

import asyncio
import ast
from collections.abc import Sequence
from copy import deepcopy
import hashlib
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

import pandas as pd
from sqlalchemy.exc import OperationalError

from app.agent_context.roles import SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import (
    OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION,
)
from app.core.settings import Settings, settings
from app.eod.auto_pykrx import (
    AutoPykrxEODScheduler,
    RecoveryOnlyScheduleDisposition,
)
from app.eod.pykrx_backfill import (
    AUTO_FOLLOWUP_PROVIDER_OUTAGE_RETRY_INTERVAL_SECONDS,
    AUTO_FOLLOWUP_RECOVERY_TRIGGER,
    FEEDBACK_RETRY_PARENT_CONTRACT_VERSION,
    PYKRX_STAGE_SEQUENCE,
    PykrxBackfillService,
    PykrxSymbol,
    _call_pykrx,
    _feedback_dispatch_rate_limit_backoff,
    _feedback_suffix_has_stale_prerequisite_ordering,
    _is_dart_provider_outage_rearm_candidate,
    _is_retired_decider_cutoff_zero_work_run,
    _is_resumable_same_day_intraday_watch_post_signal_restart,
    _is_transparent_core_restart_signal_suffix_run,
    _is_retryable_feedback_rearm_candidate,
    _retryable_feedback_rearm_config,
    _retryable_feedback_rearm_knowledge_cutoff,
    _retryable_feedback_rearm_max_cycles,
    _is_summary_cutoff_contract_repair_candidate,
    _is_terminal_followup_continuation_candidate,
    _select_summary_cutoff_proof,
    _select_prior_signal_rearm_evidence,
    _signal_features_have_stale_prerequisite_ordering,
)
from app.eod.pykrx_import import (
    pykrx_request_credentials_visible,
    suppress_pykrx_login_env,
)
from app.eod.market_membership import (
    build_market_membership_snapshot,
    validate_market_membership_snapshot,
)
from app.eod.pykrx_pipeline_support import build_daily_retry_context
from app.eod.run_stage_support import (
    AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER,
    SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT,
    SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY,
    SAME_DAY_INTRADAY_WATCH_REARM_VERSION,
    nonblocking_partial_progression_digest,
    pending_same_day_intraday_watch_rearm_marker,
)
from app.eod.summary_cutoff_contract import (
    is_resumable_summary_cutoff_contract_repair,
    summary_cutoff_proof_from_stage,
)
from app.eod.stage_heartbeat import PeriodicStageHeartbeat
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome
from app.eod.pykrx_pipeline_runner import (
    AUTO_DAILY_FOLLOWUP_STAGE_ORDER,
    AUTO_DAILY_TRACKED_STAGE_ORDER,
    DailyPipelineTrackingWriteError,
    HISTORICAL_BACKFILL_STAGE_ORDER,
    _OfficialDailyPipelineTracker,
    _bounded_failure_diagnostics,
    _build_macro_provider_coverage,
    _build_pykrx_enrichment_symbol_scope_outcome,
    _classify_pykrx_enrichment_categories,
    _completion_status_for_gap,
    _gap_policy_details,
    _enrich_bars_with_market_turnover,
    _execute_factor_stage_with_heartbeat,
    _extract_followup_source_run_ids,
    _extract_result_counts,
    _is_satisfied_followup_result,
    _merge_inherited_downstream_scope,
    _normalize_stage_status,
    _partition_pykrx_enrichment_category_gaps,
    _pykrx_component_retry_symbols,
    _resolve_result_status,
    _run_daily_factor_followup,
    _run_daily_piotroski_filter_followup,
    _run_daily_external_news_enrichment_followup,
    _run_daily_feedback_followup,
    _run_daily_intraday_snapshot_followup,
    _run_daily_macro_context_followup,
    _run_daily_next_open_execution_plan_followup,
    _run_daily_signal_decider_evaluation_followup,
    _run_daily_signal_decider_followup,
    _truthful_failed_stage_counts,
    _validated_exact_piotroski_membership_symbols,
    should_skip_point_in_time_current_only_stage,
)
from app.market.clock import KST, MarketState
from app.repositories.eod_run_repository import AutoEODRecoveryTarget


class FeedbackRetryConfigCallsiteTests(TestCase):
    def test_production_callsites_have_audited_recovery_modes(self) -> None:
        api_root = Path(__file__).resolve().parents[1]
        expected = {
            'app/agent_context/weekly_policy_rebind_service.py': [
                '<default_false>'
            ],
            'app/api/routes_agent_context.py': [
                '<default_false>',
                '<default_false>',
            ],
            'app/dart/fundamentals_capture_service.py': [
                '<default_false>',
                '<default_false>'
            ],
            'app/eod/daily_pipeline_stages.py': [
                'point_in_time_recovery'
            ],
            'app/eod/pykrx_pipeline_runner.py': [
                'point_in_time_recovery'
            ],
            'app/internal/ops.py': ['<default_false>'],
            'scripts/run_feedback_retry_cycles.py': ['<default_false>'],
        }
        observed: dict[str, list[str]] = {}

        for source_root in (api_root / 'app', api_root / 'scripts'):
            for source_path in sorted(source_root.rglob('*.py')):
                tree = ast.parse(source_path.read_text(encoding='utf-8'))
                for node in ast.walk(tree):
                    if not (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Name)
                        and node.func.id == 'FeedbackRetryCycleConfig'
                    ):
                        continue
                    recovery_keyword = next(
                        (
                            keyword
                            for keyword in node.keywords
                            if keyword.arg == 'point_in_time_recovery'
                        ),
                        None,
                    )
                    mode = (
                        ast.unparse(recovery_keyword.value)
                        if recovery_keyword is not None
                        else '<default_false>'
                    )
                    relative_path = source_path.relative_to(api_root).as_posix()
                    observed.setdefault(relative_path, []).append(mode)

        self.assertEqual(observed, expected)


class _AnySymbolTurnoverFrame:
    empty = False
    columns = {'거래대금'}

    class _Index:
        def __contains__(self, _symbol: object) -> bool:
            return True

    class _Loc:
        def __getitem__(self, _key: object) -> int:
            return 1_000_000

    index = _Index()
    loc = _Loc()


class PykrxPipelineResultStatusTests(TestCase):
    def setUp(self) -> None:
        self.scope_finalizer_patcher = patch(
            'app.eod.pykrx_pipeline_runner._finalize_daily_stage_scope',
            return_value={'ready': True, 'deleted_stale_row_count': 0},
        )
        self.scope_finalizer = self.scope_finalizer_patcher.start()
        self.addCleanup(self.scope_finalizer_patcher.stop)

    def test_piotroski_followup_excludes_bars_outside_validated_eod_membership(
        self,
    ) -> None:
        membership = build_market_membership_snapshot(
            symbols=[
                SimpleNamespace(symbol='005930', market='KOSPI'),
                SimpleNamespace(symbol='000660', market='KOSPI'),
            ],
            markets=['KOSPI'],
        )
        exact_symbols = _validated_exact_piotroski_membership_symbols(
            universe_snapshot={
                'source': 'pykrx',
                'complete': True,
                'latest_trade_date': '2026-10-30',
                'universe_source': 'pykrx_market_ohlcv',
                'membership_basis': 'end_date_snapshot',
                'markets': ['KOSPI'],
                'symbol_count': 2,
                'unavailable_markets': [],
                'market_membership': membership,
            },
            market_date='2026-10-30',
            markets=['KOSPI'],
        )

        with patch(
            'app.signals.piotroski_filter.daily_piotroski_filter_service.'
            'rebuild_for_market_date',
            return_value={
                'results_persisted': 2,
                'results_failed': 0,
                'scope_finalization': {'ready': True},
            },
        ) as rebuild:
            _run_daily_piotroski_filter_followup(
                market_date='2026-10-30',
                exact_membership_symbols=exact_symbols,
            )

        self.assertEqual(exact_symbols, ['000660', '005930'])
        rebuild.assert_called_once_with(
            market_date='2026-10-30',
            symbols=['000660', '005930'],
            ranking_symbols=['000660', '005930'],
            exact_scope_symbols=['000660', '005930'],
            authoritative_market_scope=None,
        )

    def test_piotroski_followup_fails_closed_without_exact_membership_proof(
        self,
    ) -> None:
        with patch(
            'app.signals.piotroski_filter.daily_piotroski_filter_service.'
            'rebuild_for_market_date',
        ) as rebuild:
            with self.assertRaisesRegex(
                RuntimeError,
                'validated exact EOD membership',
            ):
                _run_daily_piotroski_filter_followup(
                    market_date='2026-10-30',
                    exact_membership_symbols=None,
                )

        rebuild.assert_not_called()

    def test_minor_market_risk_gap_is_completed_and_audited(self) -> None:
        self.assertEqual(
            _completion_status_for_gap(
                failed=7,
                total=2675,
                treat_minor_gap_as_completed=True,
            ),
            'completed',
        )
        details = _gap_policy_details(
            failed=7,
            total=2675,
            treat_minor_gap_as_completed=True,
        )
        self.assertTrue(details['is_minor_missing_gap'])
        self.assertTrue(details['minor_missing_gap_treated_as_success'])

    def test_market_risk_gap_at_threshold_remains_partial(self) -> None:
        self.assertEqual(
            _completion_status_for_gap(
                failed=10,
                total=100,
                treat_minor_gap_as_completed=True,
            ),
            'partial',
        )

    def test_non_market_risk_minor_gap_remains_partial_and_retryable(self) -> None:
        for stage in (
            'price_collection',
            'technical_snapshots',
            'action_summaries',
            'trending_ranks',
            'signal_features',
        ):
            with self.subTest(stage=stage):
                self.assertEqual(
                    _completion_status_for_gap(failed=1, total=100),
                    'partial',
                )
                details = _gap_policy_details(failed=1, total=100)
                self.assertTrue(details['is_minor_missing_gap'])
                self.assertFalse(
                    details['minor_missing_gap_treated_as_success']
                )

    def test_failed_symbol_diagnostics_are_bounded_for_stage_persistence(
        self,
    ) -> None:
        diagnostics = _bounded_failure_diagnostics(
            {
                'failed_symbols': ['027830', '299170'],
                'failed_symbol_errors': [
                    {'symbol': '027830', 'error': 'temporary KIS failure'},
                    {'symbol': '299170', 'error': 'x' * 1200},
                ],
            },
            limit=2,
            error_length_limit=1000,
        )

        self.assertEqual(diagnostics['failed_symbols'], ['027830', '299170'])
        self.assertEqual(
            diagnostics['failed_symbol_errors'][0],
            {'symbol': '027830', 'error': 'temporary KIS failure'},
        )
        self.assertEqual(
            len(diagnostics['failed_symbol_errors'][1]['error']),
            1000,
        )

    def test_preopen_previous_session_market_risk_is_not_safety_skipped(
        self,
    ) -> None:
        market = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-29T08:30:00+09:00',
            session_open_at='2026-09-29T09:00:00+09:00',
            session_close_at='2026-09-29T15:30:00+09:00',
        )

        self.assertFalse(
            should_skip_point_in_time_current_only_stage(
                stage_id='market_risk_filter',
                market_date='2026-09-28',
                market=market,
                previous_trading_date=date(2026, 9, 28),
            )
        )

    def test_preopen_older_market_risk_remains_safety_skipped(self) -> None:
        market = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-29T08:30:00+09:00',
            session_open_at='2026-09-29T09:00:00+09:00',
            session_close_at='2026-09-29T15:30:00+09:00',
        )

        self.assertTrue(
            should_skip_point_in_time_current_only_stage(
                stage_id='market_risk_filter',
                market_date='2026-09-25',
                market=market,
                previous_trading_date=date(2026, 9, 28),
            )
        )

    def test_open_session_previous_market_risk_remains_safety_skipped(
        self,
    ) -> None:
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-29T09:00:01+09:00',
            session_open_at='2026-09-29T09:00:00+09:00',
            session_close_at='2026-09-29T15:30:00+09:00',
        )

        self.assertTrue(
            should_skip_point_in_time_current_only_stage(
                stage_id='market_risk_filter',
                market_date='2026-09-28',
                market=market,
                previous_trading_date=date(2026, 9, 28),
            )
        )

    def test_fast_path_prerequisite_bypass_default_is_disabled(self) -> None:
        self.assertFalse(
            Settings.model_fields[
                'openclaw_feedback_fast_path_bypass_prerequisite_gate'
            ].default
        )

    def test_ordering_repair_includes_optional_context_and_signal_lineage(
        self,
    ) -> None:
        stages = {
            'momentum_factors': {
                'completed_at': '2026-04-03T08:30:00+00:00',
            },
            'signal_features': {
                'started_at': '2026-04-03T08:00:00+00:00',
                'completed_at': '2026-04-03T08:05:00+00:00',
            },
            'macro_context_refresh': {
                'completed_at': '2026-04-03T08:40:00+00:00',
            },
            'agent_feedback_summaries': {
                'started_at': '2026-04-03T08:35:00+00:00',
                'completed_at': '2026-04-03T08:36:00+00:00',
            },
        }

        self.assertTrue(
            _signal_features_have_stale_prerequisite_ordering(
                stage_rows=stages,
                point_in_time_recovery=False,
            )
        )
        self.assertTrue(
            _feedback_suffix_has_stale_prerequisite_ordering(
                stage_rows=stages,
                point_in_time_recovery=False,
            )
        )

    def test_point_in_time_safe_skip_is_terminal_for_pipeline_tracking(
        self,
    ) -> None:
        self.assertEqual(
            _resolve_result_status(
                {
                    'triggered': False,
                    'status': 'skipped',
                    'safe_skip': True,
                    'reason': 'point_in_time_recovery_current_only_stage',
                }
            ),
            'completed',
        )

    def test_completed_payload_with_failed_items_is_partial(self) -> None:
        result = {
            'status': 'completed',
            'completed_count': 60,
            'failed_count': 100,
        }

        self.assertEqual(_resolve_result_status(result), 'partial')

    def test_intraday_followup_preserves_partial_as_unsatisfied(self) -> None:
        partial_result = {
            'status': 'partial',
            'reason': 'shared_source_incomplete',
            'snapshots_persisted': 2,
            'snapshots_failed': 0,
            'retry_required': True,
            'retry_symbols_count': 2,
            'retry_symbols': ['005930', '000660'],
            '_retry_metadata': {
                'failed_symbols': ['005930', '000660'],
            },
        }

        with (
            patch(
                'app.agent_context.eligibility.'
                'load_agent_feedback_eligible_symbols',
                return_value=['005930', '000660'],
            ),
            patch(
                'app.market.kis_intraday_snapshots.'
                'kis_intraday_snapshot_service.collect_snapshots',
                new=AsyncMock(return_value=partial_result),
            ),
        ):
            result = _run_daily_intraday_snapshot_followup(
                market_date='2026-05-07',
                enabled=True,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            _resolve_result_status(
                result,
                stage_id='intraday_snapshots',
            ),
            'partial',
        )
        self.assertFalse(
            _is_satisfied_followup_result(
                result,
                stage_id='intraday_snapshots',
            )
        )

    def test_intraday_followup_normalizes_non_trading_day_to_safe_skip(
        self,
    ) -> None:
        closed_result = {
            'status': 'market_closed',
            'reason': 'non_trading_day',
            'market_state': 'WEEKEND',
            'market_date': '2026-05-09',
            'snapshots_persisted': 0,
        }

        with (
            patch(
                'app.agent_context.eligibility.'
                'load_agent_feedback_eligible_symbols',
                return_value=['005930'],
            ),
            patch(
                'app.market.kis_intraday_snapshots.'
                'kis_intraday_snapshot_service.collect_snapshots',
                new=AsyncMock(return_value=closed_result),
            ),
        ):
            result = _run_daily_intraday_snapshot_followup(
                market_date='2026-05-09',
                enabled=True,
            )

        self.assertFalse(result['triggered'])
        self.assertTrue(result['safe_skip'])
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'non_trading_day')
        self.assertEqual(
            _resolve_result_status(
                result,
                stage_id='intraday_snapshots',
            ),
            'completed',
        )
        self.assertTrue(
            _is_satisfied_followup_result(
                result,
                stage_id='intraday_snapshots',
            )
        )

    def test_official_optional_unavailable_result_is_partial(self) -> None:
        result = {
            'triggered': True,
            'status': 'unavailable',
            'reason': 'external_news_storage_unavailable',
        }

        self.assertEqual(
            _resolve_result_status(
                result,
                stage_id='external_news_enrich',
            ),
            'partial',
        )
        self.assertEqual(
            _resolve_result_status(result, stage_id='price_collection'),
            'unavailable',
        )

    def test_factor_payload_without_ready_content_is_partial(self) -> None:
        result = {
            'status': 'completed',
            'results_persisted': 2_678,
            'results_failed': 0,
            'summary': {
                'requested_symbol_count': 2_678,
                'matched_symbol_count': 2_678,
                'insufficient_history_count': 2_678,
                'ready_count': 0,
            },
        }

        self.assertEqual(_resolve_result_status(result), 'partial')

    def test_zero_scope_payload_with_incomplete_context_is_blocked(self) -> None:
        result = {
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

        self.assertEqual(_resolve_result_status(result), 'blocked')

    def test_factor_followup_stops_before_next_factor_when_stage_owner_is_lost(
        self,
    ) -> None:
        lifecycle: list[tuple[str, str]] = []

        def execute_stage(stage_id, call):
            lifecycle.append((stage_id, 'started'))
            result = call()
            lifecycle.append((stage_id, 'completed'))
            return result, stage_id != 'value_factors'

        def record_stage(stage_id, _stage_result):
            lifecycle.append((stage_id, 'recorded'))

        with (
            patch(
                'app.signals.momentum_factor.'
                'daily_momentum_factor_service.rebuild_for_market_date',
                return_value={
                    'status': 'completed',
                    'results_persisted': 2,
                    'results_failed': 0,
                    'scope_finalization': {'ready': True},
                },
            ) as momentum,
            patch(
                'app.signals.value_factor.'
                'daily_value_factor_service.rebuild_for_market_date',
                return_value={
                    'status': 'completed',
                    'results_persisted': 2,
                    'results_failed': 0,
                    'scope_finalization': {'ready': True},
                },
            ) as value,
            patch(
                'app.signals.low_risk_factor.'
                'daily_low_risk_factor_service.rebuild_for_market_date',
            ) as low_risk,
        ):
            result = _run_daily_factor_followup(
                market_date='2026-04-14',
                exact_membership_symbols=['005930', '000660'],
                stage_executor=execute_stage,
                stage_result_callback=record_stage,
            )

        momentum.assert_called_once_with(
            market_date='2026-04-14',
            symbols=['000660', '005930'],
            ranking_symbols=['000660', '005930'],
            exact_scope_symbols=['000660', '005930'],
            authoritative_market_scope=None,
        )
        value.assert_called_once()
        low_risk.assert_not_called()
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')
        self.assertEqual(
            lifecycle,
            [
                ('momentum_factors', 'started'),
                ('momentum_factors', 'completed'),
                ('momentum_factors', 'recorded'),
                ('value_factors', 'started'),
                ('value_factors', 'completed'),
            ],
        )

    def test_factor_followup_fails_closed_without_exact_membership(self) -> None:
        with patch(
            'app.signals.momentum_factor.'
            'daily_momentum_factor_service.rebuild_for_market_date',
        ) as momentum:
            with self.assertRaisesRegex(
                RuntimeError,
                'requires validated exact EOD membership',
            ):
                _run_daily_factor_followup(
                    market_date='2026-04-14',
                    exact_membership_symbols=None,
                )

        momentum.assert_not_called()

    def test_factor_followup_preserves_partial_service_status(self) -> None:
        def execute_stage(stage_id, _call):
            if stage_id == 'momentum_factors':
                return (
                    {
                        'status': 'partial',
                        'gate_status': 'ready',
                        'results_persisted': 2,
                        'results_failed': 0,
                        'scope_finalization': {'ready': True},
                        **build_exact_symbol_scope_outcome(
                            manifest_version='factor_scope_outcomes_v1',
                            selected_symbols=['005930', '000660'],
                            successful_symbols=['005930', '000660'],
                            retry_debt_symbols=[],
                        ),
                    },
                    True,
                )
            return (
                {
                    'status': 'completed',
                    'results_persisted': 2,
                    'results_failed': 0,
                    'scope_finalization': {'ready': True},
                },
                True,
            )

        result = _run_daily_factor_followup(
            market_date='2026-04-14',
            exact_membership_symbols=['005930', '000660'],
            stage_executor=execute_stage,
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            result['stages']['momentum_factors']['status'],
            'partial',
        )

    def test_factor_followup_advances_exact_success_cohort_and_keeps_debt(
        self,
    ) -> None:
        exact_symbols = [f'{index:06d}' for index in range(1, 101)]
        failed_symbol = exact_symbols[-1]
        successful_symbols = exact_symbols[:-1]
        calls: dict[str, dict] = {}

        def stage_result(stage_id: str, **kwargs):
            calls[stage_id] = kwargs
            selected = list(kwargs['symbols'])
            retry_debt = (
                [failed_symbol] if stage_id == 'momentum_factors' else []
            )
            successful = [
                symbol
                for symbol in selected
                if symbol not in set(retry_debt)
            ]
            return {
                'status': 'partial' if retry_debt else 'completed',
                'gate_status': 'ready',
                'results_persisted': len(successful),
                'results_failed': len(retry_debt),
                'scope_finalization': {'ready': True},
                **build_exact_symbol_scope_outcome(
                    manifest_version='factor_scope_outcomes_v1',
                    selected_symbols=selected,
                    successful_symbols=successful,
                    retry_debt_symbols=retry_debt,
                ),
            }

        patch_targets = {
            'momentum_factors': (
                'app.signals.momentum_factor.'
                'daily_momentum_factor_service.rebuild_for_market_date'
            ),
            'value_factors': (
                'app.signals.value_factor.'
                'daily_value_factor_service.rebuild_for_market_date'
            ),
            'low_risk_factors': (
                'app.signals.low_risk_factor.'
                'daily_low_risk_factor_service.rebuild_for_market_date'
            ),
            'beta_factors': (
                'app.signals.beta_factor.'
                'daily_beta_factor_service.rebuild_for_market_date'
            ),
            'advanced_value_factors': (
                'app.signals.advanced_value_factor.'
                'daily_advanced_value_factor_service.rebuild_for_market_date'
            ),
            'unified_value_factors': (
                'app.signals.unified_value_factor.'
                'daily_unified_value_factor_service.rebuild_for_market_date'
            ),
            'quality_factors': (
                'app.signals.quality_factor.'
                'daily_quality_factor_service.rebuild_for_market_date'
            ),
            'composite_factors': (
                'app.signals.composite_factor.'
                'daily_composite_factor_service.rebuild_for_market_date'
            ),
        }
        with ExitStack() as stack:
            for stage_id, target in patch_targets.items():
                stack.enter_context(
                    patch(
                        target,
                        side_effect=(
                            lambda stage_id=stage_id, **kwargs: stage_result(
                                stage_id,
                                **kwargs,
                            )
                        ),
                    )
                )
            result = _run_daily_factor_followup(
                market_date='2026-10-30',
                exact_membership_symbols=exact_symbols,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['successful_stock_codes'], successful_symbols)
        self.assertEqual(result['retry_debt_stock_codes'], [failed_symbol])
        self.assertEqual(result['_retry_metadata']['failed_symbols'], [failed_symbol])
        for stage_id in tuple(patch_targets)[1:]:
            self.assertEqual(calls[stage_id]['symbols'], successful_symbols)
            self.assertEqual(calls[stage_id]['ranking_symbols'], exact_symbols)

    def test_auto_factor_executor_heartbeats_during_sync_call_and_stops_on_loss(
        self,
    ) -> None:
        heartbeat_seen = threading.Event()
        value_called = False

        def execute_stage(stage_id, call):
            pulse = PeriodicStageHeartbeat(
                lambda: heartbeat_seen.set() or False,
                interval_seconds=0.01,
                thread_name=f'test-auto-{stage_id}-heartbeat',
            )
            with pulse:
                result = call()
            return result, not pulse.ownership_lost

        def momentum_result(**_kwargs):
            self.assertTrue(heartbeat_seen.wait(timeout=0.5))
            return {
                'status': 'completed',
                'results_persisted': 2,
                'results_failed': 0,
            }

        def value_result(**_kwargs):
            nonlocal value_called
            value_called = True
            return {
                'status': 'completed',
                'results_persisted': 2,
                'results_failed': 0,
            }

        with (
            patch(
                'app.signals.momentum_factor.'
                'daily_momentum_factor_service.rebuild_for_market_date',
                side_effect=momentum_result,
            ),
            patch(
                'app.signals.value_factor.'
                'daily_value_factor_service.rebuild_for_market_date',
                side_effect=value_result,
            ),
        ):
            result = _run_daily_factor_followup(
                market_date='2026-04-14',
                exact_membership_symbols=['005930', '000660'],
                stage_executor=execute_stage,
            )

        self.assertTrue(heartbeat_seen.is_set())
        self.assertFalse(value_called)
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')

    def test_factor_executor_rejects_lost_owner_before_starting_stage(
        self,
    ) -> None:
        factor_call = MagicMock()

        result, ownership_retained = _execute_factor_stage_with_heartbeat(
            call=factor_call,
            heartbeat=lambda: False,
            interval_seconds=60.0,
            thread_name='test-auto-preflight-ownership',
        )

        factor_call.assert_not_called()
        self.assertFalse(ownership_retained)
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')

    def test_factor_executor_propagates_heartbeat_tracking_error(self) -> None:
        factor_call = MagicMock()
        tracking_error = DailyPipelineTrackingWriteError(
            'heartbeat checkpoint unavailable'
        )

        with self.assertRaisesRegex(
            DailyPipelineTrackingWriteError,
            'heartbeat checkpoint unavailable',
        ):
            _execute_factor_stage_with_heartbeat(
                call=factor_call,
                heartbeat=MagicMock(side_effect=tracking_error),
                interval_seconds=60.0,
                thread_name='test-auto-tracking-heartbeat',
            )

        factor_call.assert_not_called()

    def test_factor_executor_propagates_background_tracking_error(self) -> None:
        background_heartbeat_seen = threading.Event()
        heartbeat_attempts = 0

        def heartbeat() -> bool:
            nonlocal heartbeat_attempts
            heartbeat_attempts += 1
            if heartbeat_attempts == 1:
                return True
            background_heartbeat_seen.set()
            raise DailyPipelineTrackingWriteError(
                'background heartbeat checkpoint unavailable'
            )

        def factor_call() -> dict:
            self.assertTrue(background_heartbeat_seen.wait(timeout=1.0))
            return {'status': 'completed'}

        with self.assertRaisesRegex(
            DailyPipelineTrackingWriteError,
            'background heartbeat checkpoint unavailable',
        ):
            _execute_factor_stage_with_heartbeat(
                call=factor_call,
                heartbeat=heartbeat,
                interval_seconds=0.01,
                thread_name='test-auto-background-tracking-heartbeat',
            )

    def test_factor_followup_does_not_finalize_after_tracking_failure(
        self,
    ) -> None:
        value_factor = MagicMock()

        with (
            patch(
                'app.signals.value_factor.'
                'daily_value_factor_service.rebuild_for_market_date',
                value_factor,
            ),
            self.assertRaisesRegex(
                DailyPipelineTrackingWriteError,
                'heartbeat checkpoint unavailable',
            ),
        ):
            _run_daily_factor_followup(
                market_date='2026-04-14',
                exact_membership_symbols=['005930'],
                stage_executor=MagicMock(
                    side_effect=DailyPipelineTrackingWriteError(
                        'heartbeat checkpoint unavailable'
                    )
                ),
            )

        self.scope_finalizer.assert_not_called()
        value_factor.assert_not_called()

    def test_factor_followup_does_not_finalize_after_store_failure(
        self,
    ) -> None:
        db_error = OperationalError(
            'INSERT factor',
            {},
            RuntimeError('db unavailable'),
        )

        with self.assertRaises(OperationalError):
            _run_daily_factor_followup(
                market_date='2026-04-14',
                exact_membership_symbols=['005930'],
                stage_executor=MagicMock(side_effect=db_error),
            )

        self.scope_finalizer.assert_not_called()

    def test_piotroski_followup_does_not_finalize_after_store_failure(
        self,
    ) -> None:
        db_error = OperationalError(
            'INSERT piotroski',
            {},
            RuntimeError('db unavailable'),
        )

        with (
            patch(
                'app.signals.piotroski_filter.'
                'daily_piotroski_filter_service.rebuild_for_market_date',
                side_effect=db_error,
            ),
            self.assertRaises(OperationalError),
        ):
            _run_daily_piotroski_filter_followup(
                market_date='2026-04-14',
                exact_membership_symbols=['005930'],
            )

        self.scope_finalizer.assert_not_called()

    def test_factor_stage_exception_stops_suffix_without_empty_publication(
        self,
    ) -> None:
        value_factor = MagicMock()

        with patch(
            'app.signals.value_factor.'
            'daily_value_factor_service.rebuild_for_market_date',
            value_factor,
        ):
            result = _run_daily_factor_followup(
                market_date='2026-04-14',
                exact_membership_symbols=['005930'],
                stage_executor=MagicMock(
                    side_effect=ValueError('stage invariant unavailable')
                ),
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['successful_stock_codes'], [])
        self.assertEqual(result['retry_debt_stock_codes'], ['005930'])
        self.scope_finalizer.assert_not_called()
        value_factor.assert_not_called()

    def test_factor_exception_after_ownership_loss_does_not_start_next_factor(
        self,
    ) -> None:
        heartbeat_seen = threading.Event()
        heartbeat_attempts = 0

        def heartbeat() -> bool:
            nonlocal heartbeat_attempts
            heartbeat_attempts += 1
            if heartbeat_attempts == 1:
                return True
            heartbeat_seen.set()
            return False

        def execute_stage(stage_id, call):
            return _execute_factor_stage_with_heartbeat(
                call=call,
                heartbeat=heartbeat,
                interval_seconds=0.01,
                thread_name=f'test-auto-error-{stage_id}-heartbeat',
            )

        def momentum_failure(**_kwargs):
            self.assertTrue(heartbeat_seen.wait(timeout=0.5))
            raise RuntimeError('factor write failed after ownership loss')

        with (
            patch(
                'app.signals.momentum_factor.'
                'daily_momentum_factor_service.rebuild_for_market_date',
                side_effect=momentum_failure,
            ),
            patch(
                'app.signals.value_factor.'
                'daily_value_factor_service.rebuild_for_market_date',
            ) as value,
        ):
            result = _run_daily_factor_followup(
                market_date='2026-04-14',
                exact_membership_symbols=['005930', '000660'],
                stage_executor=execute_stage,
            )

        value.assert_not_called()
        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')
        self.assertEqual(
            result['stages']['momentum_factors']['status'],
            'interrupted',
        )
        self.assertEqual(
            result['stages']['momentum_factors']['result']['error_type'],
            'RuntimeError',
        )

    def test_enrichment_provider_error_is_recovered_only_when_final_coverage_is_complete(self) -> None:
        classification = _classify_pykrx_enrichment_categories(
            category_results={
                'sector_classification': {'errors': [{'error': 'provider timeout'}]},
                'foreign_ownership': {'errors': []},
            },
            coverage_categories={
                'sector_classification': {'expected_rows': 2, 'present_rows': 2},
                'foreign_ownership': {'expected_rows': 2, 'present_rows': 1},
            },
            preflight_failed_categories=[],
        )

        self.assertEqual(classification['provider_error_categories'], ['sector_classification'])
        self.assertEqual(classification['recovered_categories'], ['sector_classification'])
        self.assertEqual(classification['coverage_gap_categories'], ['foreign_ownership'])
        self.assertEqual(classification['unresolved_categories'], ['foreign_ownership'])

    def test_enrichment_component_retry_debt_is_not_mislabeled_as_recovered(
        self,
    ) -> None:
        classification = _classify_pykrx_enrichment_categories(
            category_results={
                'short_selling': {
                    'errors': [{'error': 'balance unavailable'}],
                    'component_retry_debt': [
                        {
                            'component': 'short_balance',
                            'status': 'retry_debt',
                        }
                    ],
                },
            },
            coverage_categories={
                'short_selling': {'expected_rows': 2, 'present_rows': 2},
            },
            preflight_failed_categories=[],
        )

        self.assertEqual(
            classification['component_retry_debt_categories'],
            ['short_selling'],
        )
        self.assertEqual(classification['recovered_categories'], [])
        self.assertEqual(classification['unresolved_categories'], ['short_selling'])

    def test_component_retry_scope_uses_full_affected_symbols_not_preview(self) -> None:
        requested_symbols = [f'{index:06d}' for index in range(1, 61)]
        result = {
            'component_retry_debt': [
                {
                    'component': 'short_balance',
                    'affected_symbols': requested_symbols,
                    'affected_symbols_preview': requested_symbols[:50],
                }
            ]
        }

        self.assertEqual(
            _pykrx_component_retry_symbols(
                result=result,
                requested_symbols=requested_symbols,
            ),
            requested_symbols,
        )

    def test_component_retry_scope_keeps_legacy_preview_compatibility(self) -> None:
        self.assertEqual(
            _pykrx_component_retry_symbols(
                result={
                    'component_retry_debt': [
                        {
                            'component': 'short_value',
                            'affected_symbols_preview': ['005930'],
                        }
                    ]
                },
                requested_symbols=['000660', '005930'],
            ),
            ['005930'],
        )

    def test_enrichment_exact_scope_advances_only_complete_symbols(self) -> None:
        categories = {
            category: {
                'expected_scope_keys': ['005930', '000660'],
                'complete_scope_keys': ['005930', '000660'],
            }
            for category in (
                'sector_classification',
                'investor_flow_symbol',
                'short_selling',
                'valuation_snapshot',
            )
        }
        categories['short_selling']['complete_scope_keys'] = ['005930']
        outcome, reasons = _build_pykrx_enrichment_symbol_scope_outcome(
            selected_symbols=['005930', '000660'],
            coverage_categories=categories,
        )

        self.assertEqual(outcome['successful_stock_codes'], ['005930'])
        self.assertEqual(outcome['retry_debt_stock_codes'], ['000660'])
        self.assertEqual(
            reasons['000660'],
            ['short_selling:incomplete_required_coverage'],
        )

    def test_enrichment_exact_scope_fails_closed_without_symbol_identity(self) -> None:
        categories = {
            category: {
                'expected_scope_keys': ['005930', '000660'],
                'complete_scope_keys': ['005930', '000660'],
            }
            for category in (
                'sector_classification',
                'investor_flow_symbol',
                'short_selling',
                'valuation_snapshot',
            )
        }
        categories['valuation_snapshot'].pop('complete_scope_keys')
        outcome, reasons = _build_pykrx_enrichment_symbol_scope_outcome(
            selected_symbols=['005930', '000660'],
            coverage_categories=categories,
        )

        self.assertEqual(outcome['successful_stock_codes'], [])
        self.assertEqual(
            outcome['retry_debt_stock_codes'],
            ['005930', '000660'],
        )
        self.assertEqual(
            reasons['005930'],
            ['valuation_snapshot:missing_exact_scope_identity'],
        )

    def test_enrichment_gap_partition_uses_required_category_gate_threshold(self) -> None:
        blocking, tolerated = _partition_pykrx_enrichment_category_gaps(
            failed_categories=['valuation_snapshot'],
            preflight={
                'allowed': True,
                'required_categories': ['valuation_snapshot'],
                'category_gates': [
                    {
                        'category': 'valuation_snapshot',
                        'gate_status': 'ready',
                        'minimum_coverage_ratio': 0.95,
                    }
                ],
            },
        )

        self.assertEqual(blocking, [])
        self.assertEqual(tolerated, ['valuation_snapshot'])

        blocking, tolerated = _partition_pykrx_enrichment_category_gaps(
            failed_categories=['valuation_snapshot'],
            preflight={
                'allowed': False,
                'required_categories': ['valuation_snapshot'],
                'category_gates': [
                    {
                        'category': 'valuation_snapshot',
                        'gate_status': 'blocked',
                        'minimum_coverage_ratio': 0.95,
                    }
                ],
            },
        )

        self.assertEqual(blocking, ['valuation_snapshot'])
        self.assertEqual(tolerated, [])

        blocking, tolerated = _partition_pykrx_enrichment_category_gaps(
            failed_categories=['foreign_ownership'],
            preflight={
                'allowed': True,
                'required_categories': ['valuation_snapshot'],
                'category_gates': [],
            },
        )

        self.assertEqual(blocking, [])
        self.assertEqual(tolerated, ['foreign_ownership'])

        blocking, tolerated = _partition_pykrx_enrichment_category_gaps(
            failed_categories=[
                'valuation_snapshot',
                'investor_flow_symbol',
            ],
            preflight={
                'allowed': False,
                'required_categories': [
                    'valuation_snapshot',
                    'investor_flow_symbol',
                ],
                'category_gates': [
                    {
                        'category': 'valuation_snapshot',
                        'gate_status': 'ready',
                    },
                    {
                        'category': 'investor_flow_symbol',
                        'gate_status': 'blocked',
                    },
                ],
            },
        )

        self.assertEqual(blocking, ['investor_flow_symbol'])
        self.assertEqual(tolerated, ['valuation_snapshot'])

    def test_retry_run_keeps_only_inherited_and_current_downstream_debt(
        self,
    ) -> None:
        inherited, merged = _merge_inherited_downstream_scope(
            requested_symbols=['472220'],
            inherited_symbols=['005930', '472220', '005930'],
            current_price_failure_symbols=[],
            current_failure_symbols=['121850'],
            preserve_deferred_followup_scope=True,
        )

        self.assertEqual(inherited, ['005930'])
        self.assertEqual(merged, ['005930', '121850'])

        inherited, merged = _merge_inherited_downstream_scope(
            requested_symbols=['005930', '000660'],
            inherited_symbols=[],
            current_price_failure_symbols=[],
            current_failure_symbols=['000660'],
            preserve_deferred_followup_scope=True,
        )

        self.assertEqual(inherited, [])
        self.assertEqual(merged, ['000660'])


@contextmanager
def _lock(acquired: bool = True):
    yield acquired


def _ready_pykrx_enrichment_result(
    *,
    market_date: str = '2026-04-03',
    symbols: tuple[str, ...] = ('005930',),
    markets: tuple[str, ...] = ('KOSPI',),
) -> dict:
    categories = {
        'sector_classification': {'rows_persisted': 1, 'errors': []},
        'investor_flow_market': {'rows_persisted': 1, 'errors': []},
        'investor_flow_symbol': {'rows_persisted': 1, 'errors': []},
        'short_selling': {'rows_persisted': 1, 'errors': []},
        'valuation_snapshot': {'rows_persisted': 1, 'errors': []},
        'market_breadth': {'rows_persisted': 1, 'errors': []},
        'foreign_ownership': {'rows_persisted': 1, 'errors': []},
    }
    market_scope_categories = {'market_breadth', 'investor_flow_market'}
    return {
        'market_date': market_date,
        'status': 'completed',
        'categories_requested': list(categories),
        'categories': categories,
        'coverage': {
            'requested_symbol_count': 1,
            'requested_market_count': 1,
            'categories': {
                category: {
                    'scope_type': (
                        'market'
                        if category in market_scope_categories
                        else 'symbol'
                    ),
                    'expected_rows': 1,
                    'present_rows': 1,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                    'expected_scope_keys': list(
                        markets
                        if category in market_scope_categories
                        else symbols
                    ),
                    'decision_usable_scope_keys': list(
                        markets
                        if category in market_scope_categories
                        else symbols
                    ),
                    'complete_scope_keys': list(
                        markets
                        if category in market_scope_categories
                        else symbols
                    ),
                }
                for category in categories
            },
        },
        'summary': {'categories_failed': 0},
    }


def _point_in_time_universe_snapshot(
    symbols: list[PykrxSymbol],
    *,
    market_date: str = '2026-04-03',
) -> dict:
    markets = sorted({item.market.upper() for item in symbols})
    return {
        'symbols': symbols,
        'latest_trade_date': market_date,
        'universe_source': 'pykrx_market_ohlcv',
        'membership_basis': 'end_date_snapshot',
        'membership_scope_warning': 'test end-date snapshot',
        'complete': True,
        'market_status': {market: 'pykrx_market_ohlcv' for market in markets},
        'unavailable_markets': [],
        'market_errors': [],
    }


def _stored_daily_universe_snapshot(
    symbols: list[PykrxSymbol],
    *,
    market_date: str = '2026-04-03',
) -> dict:
    markets = sorted({item.market.upper() for item in symbols})
    return {
        'source': 'pykrx',
        'markets': markets,
        'latest_trade_date': market_date,
        'universe_source': 'pykrx_market_ohlcv',
        'membership_basis': 'end_date_snapshot',
        'complete': True,
        'unavailable_markets': [],
        'point_in_time': False,
        'current_universe_mutated': True,
        'market_membership': build_market_membership_snapshot(
            symbols=symbols,
            markets=markets,
        ),
    }


class NextOpenExecutionPlanFollowupTests(TestCase):
    def test_refreshes_plan_for_day_after_market_date(self) -> None:
        with (
            patch(
                'app.eod.pykrx_pipeline_runner.get_market_state',
                return_value=SimpleNamespace(state='PREOPEN'),
            ),
            patch(
                'app.orders.next_open_execution.signal_decider_next_open_execution_service.refresh_plan_items',
                return_value={'summary': {'items_total': 3}},
            ) as refresh_plan,
        ):
            result = _run_daily_next_open_execution_plan_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertEqual(result['summary']['items_total'], 3)
        refresh_plan.assert_called_once_with(
            target_trade_date='2026-04-04',
            trigger='eod_post_decider',
        )

    def test_prior_date_recovery_does_not_create_expired_open_plan(self) -> None:
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T10:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
        )

        with (
            patch(
                'app.eod.pykrx_pipeline_runner.get_market_state',
                return_value=market,
                create=True,
            ),
            patch(
                'app.orders.next_open_execution.normalize_next_open_target_trade_date',
                return_value=datetime(2026, 9, 14).date(),
            ),
            patch(
                'app.orders.next_open_execution.signal_decider_next_open_execution_service.refresh_plan_items',
            ) as refresh_plan,
        ):
            result = _run_daily_next_open_execution_plan_followup(
                market_date='2026-09-11',
                enabled=True,
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'next_open_window_expired')
        refresh_plan.assert_not_called()

    def test_prior_date_recovery_skips_expired_decision_replay(self) -> None:
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T10:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
        )

        with (
            patch(
                'app.eod.pykrx_pipeline_runner.get_market_state',
                return_value=market,
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 9, 14),
            ),
            patch(
                'app.agent_context.prerequisite_service.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value={'ready': True, 'status': 'ready'},
            ) as inspect_prerequisites,
            patch(
                'app.agent_context.retry_service.'
                'agent_feedback_retry_service.run_retry_cycles',
                return_value={'status': 'completed'},
            ) as run_retry_cycles,
            patch(
                'app.agent_context.'
                'signal_decider_evaluation_maintenance_service.'
                'signal_decider_evaluation_maintenance_service.'
                'refresh_recent_coverage',
                return_value={'status': 'completed'},
            ) as refresh_evaluations,
        ):
            results = (
                _run_daily_feedback_followup(
                    market_date='2026-09-11',
                    enabled=True,
                ),
                _run_daily_signal_decider_followup(
                    market_date='2026-09-11',
                    enabled=True,
                ),
                _run_daily_signal_decider_evaluation_followup(
                    market_date='2026-09-11',
                    enabled=True,
                ),
            )

        self.assertTrue(
            all(result['status'] == 'skipped' for result in results)
        )
        self.assertTrue(
            all(
                result['reason'] == 'next_open_window_expired'
                for result in results
            )
        )
        inspect_prerequisites.assert_not_called()
        run_retry_cycles.assert_not_called()
        refresh_evaluations.assert_not_called()

    def test_decider_recovery_crossing_market_open_finishes_as_expired(
        self,
    ) -> None:
        preopen = SimpleNamespace(
            state='PREOPEN',
            server_time='2026-09-14T08:59:00+09:00',
        )
        opened = SimpleNamespace(
            state='OPEN',
            server_time='2026-09-14T09:01:00+09:00',
        )

        with (
            patch(
                'app.eod.pykrx_pipeline_runner.get_market_state',
                side_effect=[preopen, opened],
            ),
            patch(
                'app.orders.next_open_execution.'
                'normalize_next_open_target_trade_date',
                return_value=date(2026, 9, 14),
            ),
            patch(
                'app.agent_context.prerequisite_service.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value={'ready': True, 'status': 'ready'},
            ),
            patch(
                'app.agent_context.retry_service.'
                'agent_feedback_retry_service.run_retry_cycles',
                return_value={'status': 'completed'},
            ) as run_retry_cycles,
        ):
            result = _run_daily_signal_decider_followup(
                market_date='2026-09-11',
                enabled=True,
            )

        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'next_open_window_expired')
        run_retry_cycles.assert_not_called()


def _completed_factor_followup_result(*, market_date: str = '2026-04-03') -> dict:
    stages = {
        stage_id: {
            'status': 'completed',
            'result': {
                'market_date': market_date,
                'results_persisted': 1,
                'results_failed': 0,
            },
        }
        for stage_id in (
            'momentum_factors',
            'value_factors',
            'low_risk_factors',
            'beta_factors',
            'advanced_value_factors',
            'unified_value_factors',
            'quality_factors',
            'composite_factors',
        )
    }
    return {
        'triggered': True,
        'status': 'completed',
        'market_date': market_date,
        'stages': stages,
    }


def _atomic_exact_scope_proof(
    *,
    manifest_version: str,
    selected_symbols: Sequence[str],
    successful_symbols: Sequence[str] | None = None,
    retry_debt_symbols: Sequence[str] = (),
) -> dict:
    """Build the proof returned by exact-scope atomic stage publishers."""

    selected = list(selected_symbols)
    return {
        'scope_finalization': {'ready': True},
        **build_exact_symbol_scope_outcome(
            manifest_version=manifest_version,
            selected_symbols=selected,
            successful_symbols=(
                selected
                if successful_symbols is None
                else successful_symbols
            ),
            retry_debt_symbols=retry_debt_symbols,
        ),
    }


class _FakeDailyPipelineRunStateService:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.start_runs: list[dict] = []
        self.linked_eod_runs: list[dict] = []
        self.stage_updates: list[dict] = []
        self.stage_heartbeats: list[dict] = []
        self.finish_runs: list[dict] = []

    def start_run(self, **kwargs):
        self.events.append('start_run')
        self.start_runs.append(kwargs)
        return {'id': 501}

    def update_stage(self, **kwargs):
        self.events.append('update_stage')
        self.stage_updates.append(kwargs)
        return kwargs

    def link_eod_run(self, **kwargs):
        self.events.append('link_eod_run')
        self.linked_eod_runs.append(kwargs)
        return kwargs

    def heartbeat_stage(self, **kwargs):
        self.events.append('heartbeat_stage')
        self.stage_heartbeats.append(kwargs)
        return True

    def finish_run(self, **kwargs):
        self.finish_runs.append(kwargs)
        return kwargs


class _FakeDeduplicatingDailyPipelineRunStateService(_FakeDailyPipelineRunStateService):
    def start_run(self, **kwargs):
        self.start_runs.append(kwargs)
        return {
            'id': 744,
            'market_date': kwargs['market_date'],
            'trigger': 'manual_api',
            'status': 'running',
            'deduplicated': True,
            'deduplication_reason': 'overlapping_run_already_active',
            'stages': {'signal_decider': {'status': 'running'}},
        }


class PykrxBackfillServiceTests(TestCase):
    def test_latest_pipeline_summary_proof_fails_closed_before_eod_fallback(
        self,
    ) -> None:
        eod_summary = {
            'status': 'completed',
            'source_run_ids': {
                'agent_feedback_run_id': 'older-summary',
            },
            'result_summary': {
                'status': 'completed',
                'run_id': 'older-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': ['005930'],
            },
        }
        conflicting_latest = {
            'status': 'completed',
            'source_run_ids': {
                'agent_feedback_run_id': 'latest-summary',
            },
            'details': {
                'result_summary': {
                    'run_id': 'different-summary',
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': ['005930'],
                },
            },
        }

        self.assertIsNone(
            _select_summary_cutoff_proof(
                pipeline_stages={
                    'agent_feedback_summaries': conflicting_latest,
                },
                eod_stage_progress={
                    'agent_feedback_summaries': eod_summary,
                },
            )
        )
        self.assertEqual(
            _select_summary_cutoff_proof(
                pipeline_stages={},
                eod_stage_progress={
                    'agent_feedback_summaries': eod_summary,
                },
            ),
            {
                'source_run_id': 'older-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': ['005930'],
            },
        )

    def test_decider_only_summary_proof_uses_validated_run_metadata(self) -> None:
        eod_summary = {
            'status': 'completed',
            'source_run_ids': {
                'agent_feedback_run_id': 'older-summary',
            },
            'result_summary': {
                'status': 'completed',
                'run_id': 'older-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': ['005930'],
            },
        }
        persisted_cutoff = {
            'fs_div': 'CFS',
            'feedback_knowledge_cutoff': {
                'knowledge_cutoff_at': '2026-04-03T08:00:00+00:00',
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'source_run_id': 'pinned-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': ['000660', '005930'],
            },
        }

        self.assertEqual(
            _select_summary_cutoff_proof(
                pipeline_stages={},
                run_metadata=persisted_cutoff,
                eod_stage_progress={
                    'agent_feedback_summaries': eod_summary,
                },
            ),
            {
                'source_run_id': 'pinned-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': None,
                'expected_completed_symbols': ['000660', '005930'],
                'knowledge_cutoff_at': '2026-04-03T08:00:00+00:00',
                'fs_div': 'CFS',
            },
        )

        invalid_cutoff = deepcopy(persisted_cutoff)
        invalid_cutoff['feedback_knowledge_cutoff'][
            'knowledge_cutoff_at'
        ] = 'not-a-timestamp'
        self.assertIsNone(
            _select_summary_cutoff_proof(
                pipeline_stages={},
                run_metadata=invalid_cutoff,
                eod_stage_progress={
                    'agent_feedback_summaries': eod_summary,
                },
            )
        )

    def test_partial_summary_proof_resolves_only_exact_success_intersection(
        self,
    ) -> None:
        cutoff_at = datetime(
            2026,
            4,
            3,
            8,
            0,
            tzinfo=timezone.utc,
        )
        parent_symbols = ['000660', '005930', '035420']
        completed_scope_proof = {
            'schema_version': 'completed_summarizer_intersection_v1',
            'completed_symbols': ['000660'],
            'admitted_symbols': ['000660', '005930'],
        }
        terminal_scope_proof = {
            'schema_version': 'terminal_role_success_intersection_v1',
            'failed_symbols_by_role': {
                'eod-summarizer': [],
                'technical-summarizer': ['005930'],
                'fundamental-summarizer': [],
            },
            'total_items': 6,
            'completed_items': 5,
            'failed_items': 1,
        }
        stage = {
            'status': 'partial',
            'source_run_ids': {'run_id': 'summary-partial'},
            'result_summary': {
                'status': 'partial',
                'run_id': 'summary-partial',
                'feedback_scope_mode': 'decider_scope_fast_path',
            },
        }
        resolution = {
            'ready': True,
            'source_run_id': 'summary-partial',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'knowledge_cutoff_at': cutoff_at,
            'requested_symbols': ['000660'],
            'completed_scope_proof': completed_scope_proof,
            'terminal_role_scope_proof': terminal_scope_proof,
        }

        with patch(
            'app.eod.pykrx_backfill.'
            'resolve_latest_completed_summary_knowledge_cutoff',
            return_value=resolution,
        ) as resolve_cutoff:
            proof = _select_summary_cutoff_proof(
                pipeline_stages={
                    'agent_feedback_summaries': stage,
                },
                eod_stage_progress={},
                market_date='2026-04-03',
                fs_div='CFS',
                parent_symbols=parent_symbols,
            )

        self.assertEqual(
            proof,
            {
                'source_run_id': 'summary-partial',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': None,
                'expected_completed_symbols': ['000660'],
                'knowledge_cutoff_at': cutoff_at.isoformat(),
                'fs_div': 'CFS',
                'completed_scope_proof': completed_scope_proof,
                'terminal_role_scope_proof': terminal_scope_proof,
            },
        )
        resolve_cutoff.assert_called_once_with(
            market_date='2026-04-03',
            feedback_scope_mode='decider_scope_fast_path',
            fs_div='CFS',
            source_run_id='summary-partial',
            requested_symbols=None,
        )

        conflicting = deepcopy(stage)
        conflicting['source_run_ids']['agent_feedback_run_id'] = (
            'different-summary'
        )
        self.assertIsNone(
            _select_summary_cutoff_proof(
                pipeline_stages={
                    'agent_feedback_summaries': conflicting,
                },
                eod_stage_progress={},
                market_date='2026-04-03',
                fs_div='CFS',
                parent_symbols=parent_symbols,
            )
        )

    def test_partial_summary_success_scope_continues_without_failed_retry_scope(
        self,
    ) -> None:
        service = PykrxBackfillService()
        completed_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        completed_scope_proof = {
            'schema_version': 'completed_summarizer_intersection_v1',
            'completed_symbols': ['000660'],
            'admitted_symbols': ['000660', '005930'],
        }
        terminal_scope_proof = {
            'schema_version': 'terminal_role_success_intersection_v1',
            'failed_symbols_by_role': {
                'eod-summarizer': [],
                'technical-summarizer': ['005930'],
                'fundamental-summarizer': [],
            },
            'total_items': 6,
            'completed_items': 5,
            'failed_items': 1,
        }
        summary_proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
            'expected_completed_symbols': ['000660'],
            'knowledge_cutoff_at': '2026-04-03T08:00:00+00:00',
            'fs_div': 'CFS',
            'completed_scope_proof': completed_scope_proof,
            'terminal_role_scope_proof': terminal_scope_proof,
        }
        pipeline_run = {
            'id': 595,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'updated_at': completed_at.isoformat(),
            'completed_at': completed_at.isoformat(),
            'run_metadata': {
                'resolved_stages': [
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'resume_from_stage': 'agent_feedback_summaries',
                'reused_eod_collection_run_id': 582,
                'fs_div': 'CFS',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
            'stages': {
                'agent_feedback_summaries': {
                    'status': 'partial',
                    'source_run_ids': {
                        'agent_feedback_run_id': (
                            'feedback-cycle-summary'
                        ),
                    },
                },
                'signal_decider': {'status': 'blocked'},
                'next_open_execution_plan': {'status': 'blocked'},
                'signal_decider_evaluations': {'status': 'blocked'},
            },
        }
        retry_config = {
            'source_run_id': 'feedback-cycle-summary',
            'knowledge_cutoff_at': '2026-04-03T08:00:00+00:00',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': ['005930'],
            'outer_batch_size': 20,
            'fs_div': 'CFS',
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=6,
            ),
            patch(
                'app.eod.pykrx_backfill._select_summary_cutoff_proof',
                return_value=summary_proof,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                '_is_retryable_feedback_rearm_candidate',
                return_value=True,
            ),
            patch(
                'app.eod.pykrx_backfill._feedback_retry_stage_id',
                return_value='agent_feedback_summaries',
            ),
            patch(
                'app.eod.pykrx_backfill._retryable_feedback_rearm_config',
                return_value=retry_config,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                '_retryable_feedback_rearm_max_cycles',
                return_value=3,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                return_value={
                    'result': None,
                    'symbols': [],
                    'downstream_symbols': [],
                },
            ),
            patch.object(
                service,
                '_resolve_point_in_time_followup_membership',
                return_value=(
                    {'000660', '005930'},
                    ('KOSPI', 'KOSDAQ'),
                ),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'status': 'blocked',
                    'pipeline_run_id': 596,
                },
            ) as resume_followups,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run={
                    'id': 582,
                    'status': 'completed',
                    'symbols_failed': 0,
                    'run_metadata': {},
                },
                markets=('KOSPI',),
                point_in_time_recovery=True,
            )

        self.assertTrue(result['triggered'])
        recovery_kwargs = resume_followups.call_args.kwargs
        self.assertEqual(
            recovery_kwargs['stages'],
            [
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )
        self.assertEqual(
            recovery_kwargs['resume_from_stage'],
            'signal_decider',
        )
        self.assertEqual(
            recovery_kwargs['summary_cutoff_proof'],
            summary_proof,
        )
        self.assertIs(recovery_kwargs['point_in_time_recovery'], True)
        self.assertNotIn('symbols', recovery_kwargs)
        self.assertNotIn(
            'inherited_feedback_retry_config',
            recovery_kwargs,
        )
        self.assertNotIn(
            'inherited_feedback_source_run_id',
            recovery_kwargs,
        )
        self.assertNotIn(
            'inherit_feedback_prerequisite_admission',
            recovery_kwargs,
        )
        self.assertNotIn('feedback_max_cycles', recovery_kwargs)

    def test_market_turnover_enrichment_rejects_invalid_turnover_but_preserves_zero_volume_bar(self) -> None:
        frames = {
            '20260402': pd.DataFrame({'거래대금': [0]}, index=['005930']),
            '20260403': pd.DataFrame({'거래대금': [0]}, index=['005930']),
        }
        coverage: dict[str, int] = {}
        dependencies = SimpleNamespace(
            get_market_ohlcv_by_ticker=lambda trade_date, _market: frames[trade_date],
            log_fetch=lambda *_args, **_kwargs: None,
        )

        result = _enrich_bars_with_market_turnover(
            bars=[
                {'trade_date': '2026-04-02', 'volume': 100, 'turnover': 999},
                {'trade_date': '2026-04-03', 'volume': 0, 'turnover': 0},
            ],
            symbol='005930',
            market='KOSPI',
            dependencies=dependencies,
            market_turnover_cache={},
            turnover_coverage=coverage,
            run_id=12,
        )

        self.assertEqual([bar['turnover'] for bar in result], [0, 0])
        self.assertEqual(
            coverage,
            {
                'total_bars': 2,
                'provider_turnover_bars': 0,
                'zero_volume_bars': 1,
                'invalid_turnover_bars': 1,
                'market_fetch_attempts': 2,
                'market_fetch_failures': 0,
            },
        )

    def test_call_pykrx_applies_timeout_to_session_requests(self) -> None:
        observed_timeouts: list[object] = []

        def fake_request(_session, _method, _url, **kwargs):
            observed_timeouts.append(kwargs.get('timeout'))
            return SimpleNamespace()

        def provider_call():
            from pykrx.website.comm import webio as pykrx_webio

            return pykrx_webio.requests.Session().get(
                'https://example.test/pykrx'
            )

        with (
            patch(
                'app.eod.pykrx_backfill.settings.eod_pykrx_request_timeout_seconds',
                7,
            ),
            patch('requests.sessions.Session.request', new=fake_request),
        ):
            _call_pykrx(provider_call)

        self.assertEqual(observed_timeouts, [7])

    def test_call_pykrx_preserves_explicit_timeout_for_authenticated_post(self) -> None:
        observed_timeouts: list[object] = []

        class FakeSession:
            def post(self, _url, **kwargs):
                observed_timeouts.append(kwargs.get('timeout'))
                return SimpleNamespace()

        fake_krx_session = SimpleNamespace(
            session=FakeSession(),
            get_headers=lambda: {},
        )
        original_get_session = lambda: fake_krx_session
        fake_webio = SimpleNamespace(
            requests=SimpleNamespace(Session=lambda: FakeSession()),
            get_session=original_get_session,
        )

        def provider_call():
            return fake_webio.get_session().session.post(
                'https://example.test/pykrx',
                timeout=3,
            )

        with (
            patch(
                'app.eod.pykrx_backfill.settings.eod_pykrx_request_timeout_seconds',
                7,
            ),
            patch(
                'app.eod.pykrx_import.import_pykrx_webio',
                return_value=fake_webio,
            ),
        ):
            _call_pykrx(provider_call)

        self.assertEqual(observed_timeouts, [3])
        self.assertIs(fake_webio.get_session, original_get_session)

    def test_call_pykrx_applies_timeout_to_module_level_get_and_post(self) -> None:
        observed_timeouts: list[object] = []

        def record_request(_url, **kwargs):
            observed_timeouts.append(kwargs.get('timeout'))
            return SimpleNamespace()

        original_requests = SimpleNamespace(
            Session=lambda: SimpleNamespace(),
            get=record_request,
            post=record_request,
        )
        original_get_session = lambda: None
        fake_webio = SimpleNamespace(
            requests=original_requests,
            get_session=original_get_session,
        )

        def provider_call():
            fake_webio.requests.get('https://example.test/pykrx-get')
            fake_webio.requests.post(
                'https://example.test/pykrx-post',
                timeout=3,
            )

        with (
            patch(
                'app.eod.pykrx_backfill.settings.eod_pykrx_request_timeout_seconds',
                7,
            ),
            patch(
                'app.eod.pykrx_import.import_pykrx_webio',
                return_value=fake_webio,
            ),
        ):
            _call_pykrx(provider_call)

        self.assertEqual(observed_timeouts, [7, 3])
        self.assertIs(fake_webio.requests, original_requests)
        self.assertIs(fake_webio.get_session, original_get_session)

    def test_call_pykrx_supports_legacy_webio_without_get_session(self) -> None:
        observed_timeouts: list[object] = []

        def record_get(_url, **kwargs):
            observed_timeouts.append(kwargs.get('timeout'))
            return SimpleNamespace()

        original_requests = SimpleNamespace(
            Session=lambda: SimpleNamespace(),
            get=record_get,
            post=lambda *_args, **_kwargs: SimpleNamespace(),
        )
        fake_webio = SimpleNamespace(requests=original_requests)

        with (
            patch(
                'app.eod.pykrx_backfill.settings.eod_pykrx_request_timeout_seconds',
                7,
            ),
            patch(
                'app.eod.pykrx_import.import_pykrx_webio',
                return_value=fake_webio,
            ),
        ):
            _call_pykrx(
                lambda: fake_webio.requests.get(
                    'https://example.test/legacy-pykrx'
                )
            )

        self.assertEqual(observed_timeouts, [7])
        self.assertIs(fake_webio.requests, original_requests)
        self.assertFalse(hasattr(fake_webio, 'get_session'))

    def test_pykrx_login_env_suppression_is_serialized_across_threads(self) -> None:
        worker_count = 8
        barrier = threading.Barrier(worker_count)

        def worker() -> None:
            barrier.wait()
            with suppress_pykrx_login_env():
                time.sleep(0.005)

        with patch.dict(
            os.environ,
            {'KRX_ID': 'test-id', 'KRX_PW': 'test-password'},
        ):
            threads = [threading.Thread(target=worker) for _ in range(worker_count)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=2)

            self.assertTrue(all(not thread.is_alive() for thread in threads))
            self.assertEqual(os.environ.get('KRX_ID'), 'test-id')
            self.assertEqual(os.environ.get('KRX_PW'), 'test-password')

    def test_provider_request_waits_for_import_suppression_to_restore_credentials(self) -> None:
        suppression_entered = threading.Event()
        provider_called = threading.Event()
        observed_credentials: list[tuple[str | None, str | None]] = []

        def suppressor() -> None:
            with suppress_pykrx_login_env():
                suppression_entered.set()
                time.sleep(0.03)

        def requester() -> None:
            suppression_entered.wait(timeout=1)
            with pykrx_request_credentials_visible():
                observed_credentials.append(
                    (os.environ.get('KRX_ID'), os.environ.get('KRX_PW'))
                )
                provider_called.set()

        with patch.dict(
            os.environ,
            {'KRX_ID': 'test-id', 'KRX_PW': 'test-password'},
        ):
            suppress_thread = threading.Thread(target=suppressor)
            request_thread = threading.Thread(target=requester)
            suppress_thread.start()
            request_thread.start()
            suppression_entered.wait(timeout=1)
            self.assertFalse(provider_called.wait(timeout=0.005))
            suppress_thread.join(timeout=2)
            request_thread.join(timeout=2)

        self.assertEqual(observed_credentials, [('test-id', 'test-password')])

    def setUp(self) -> None:
        self.current_krx_date_patcher = patch(
            'app.eod.pykrx_backfill._current_krx_calendar_date',
            return_value=date(2026, 4, 3),
        )
        self.current_krx_date_patcher.start()
        self.addCleanup(self.current_krx_date_patcher.stop)
        self.scope_finalizer_patcher = patch(
            'app.eod.pykrx_pipeline_runner._finalize_daily_stage_scope',
            return_value={'ready': True, 'deleted_stale_row_count': 0},
        )
        self.scope_finalizer = self.scope_finalizer_patcher.start()
        self.addCleanup(self.scope_finalizer_patcher.stop)
        self.market_state_patcher = patch(
            'app.eod.pykrx_pipeline_runner.get_market_state',
            return_value=SimpleNamespace(
                state='CLOSED',
                server_time='2026-04-03T16:00:00+09:00',
            ),
        )
        self.market_state_patcher.start()
        self.addCleanup(self.market_state_patcher.stop)
        self.market_turnover_patcher = patch(
            'app.eod.pykrx_backfill._get_market_ohlcv_by_ticker',
            return_value=_AnySymbolTurnoverFrame(),
        )
        self.market_turnover_patcher.start()
        self.addCleanup(self.market_turnover_patcher.stop)
        self.next_open_plan_patcher = patch(
            'app.eod.pykrx_pipeline_runner._run_daily_next_open_execution_plan_followup',
            return_value={
                'triggered': True,
                'status': 'completed',
                'summary': {'items_total': 2, 'eligible_count': 1},
            },
        )
        self.next_open_plan_patcher.start()
        self.addCleanup(self.next_open_plan_patcher.stop)

    def test_daily_signal_decider_evaluation_refreshes_maturing_calendar_window(self) -> None:
        with patch(
            'app.agent_context.signal_decider_evaluation_maintenance_service.'
            'signal_decider_evaluation_maintenance_service.refresh_recent_coverage',
            return_value={'status': 'completed'},
        ) as refresh_recent_coverage:
            result = _run_daily_signal_decider_evaluation_followup(
                market_date='2026-08-19',
                enabled=True,
            )

        self.assertEqual(result, {'status': 'completed'})
        refresh_recent_coverage.assert_called_once_with(
            as_of_market_date='2026-08-19',
            refresh_calendar=True,
            calendar_lookback_days=21,
            trigger='eod_pykrx_followup',
        )

    def test_suppress_pykrx_login_env_temporarily_removes_credentials(self) -> None:
        original_id = os.environ.get('KRX_ID')
        original_pw = os.environ.get('KRX_PW')
        os.environ['KRX_ID'] = 'test-id'
        os.environ['KRX_PW'] = 'test-pw'
        try:
            with suppress_pykrx_login_env():
                self.assertNotIn('KRX_ID', os.environ)
                self.assertNotIn('KRX_PW', os.environ)
            self.assertEqual(os.environ.get('KRX_ID'), 'test-id')
            self.assertEqual(os.environ.get('KRX_PW'), 'test-pw')
        finally:
            if original_id is None:
                os.environ.pop('KRX_ID', None)
            else:
                os.environ['KRX_ID'] = original_id
            if original_pw is None:
                os.environ.pop('KRX_PW', None)
            else:
                os.environ['KRX_PW'] = original_pw

    def test_pykrx_requests_preserve_configured_krx_credentials(self) -> None:
        def authenticated_provider():
            return os.environ.get('KRX_ID'), os.environ.get('KRX_PW')

        with patch.dict(
            os.environ,
            {'KRX_ID': 'test-id', 'KRX_PW': 'test-password'},
        ):
            result = _call_pykrx(authenticated_provider)

        self.assertEqual(result, ('test-id', 'test-password'))

    def test_extract_result_counts_reads_agent_feedback_progress_summary(self) -> None:
        result = {
            'status': 'partial',
            'progress': {
                'summary': {
                    'completed_feedback_count': 60,
                    'failed_items': 0,
                    'expected_feedback_count': 986,
                }
            },
        }

        counts = _extract_result_counts(result)

        self.assertEqual(
            counts,
            {
                'processed': 60,
                'total': 986,
                'succeeded': 60,
                'failed': 0,
            },
        )

    def test_extract_result_counts_reads_external_news_collection_summary(self) -> None:
        counts = _extract_result_counts(
            {
                'status': 'completed',
                'summary': {
                    'requested_symbol_count': 201,
                    'eligible_symbol_count': 201,
                    'collected_symbol_count': 201,
                    'matched_symbol_count': 201,
                },
            },
            stage_id='external_news_collect',
        )

        self.assertEqual(
            counts,
            {
                'processed': 201,
                'total': 201,
                'succeeded': 201,
                'failed': 0,
            },
        )

    def test_extract_result_counts_reads_external_news_enrichment_components(self) -> None:
        counts = _extract_result_counts(
            {
                'status': 'partial',
                'items_inspected': 200,
                'items_missing_classification': 7,
                'items_remaining_at_least': 2,
                'missing_item_ids': [101, 102, 102],
            },
            stage_id='external_news_enrich',
        )

        self.assertEqual(
            counts,
            {
                'processed': 200,
                'total': 202,
                'succeeded': 195,
                'failed': 5,
            },
        )

    def test_extract_result_counts_reads_macro_observation_outcomes(self) -> None:
        counts = _extract_result_counts(
            {
                'status': 'partial',
                'observations_upserted': 2,
                'observations_failed': 1,
                'source_statuses': {
                    'ecos': 'ready',
                    'fred': 'disabled_missing_api_key',
                },
            },
            stage_id='macro_context_refresh',
        )

        self.assertEqual(
            counts,
            {
                'processed': 3,
                'total': 3,
                'succeeded': 2,
                'failed': 1,
            },
        )

    def test_extract_result_counts_uses_signal_feature_result_scope(self) -> None:
        counts = _extract_result_counts(
            {
                'status': 'completed',
                'features_persisted': 2_702,
                'features_failed': 0,
                'items_total': 2_702,
            },
            default_total=2_675,
            stage_id='signal_features',
        )

        self.assertEqual(
            counts,
            {
                'processed': 2_702,
                'total': 2_702,
                'succeeded': 2_702,
                'failed': 0,
            },
        )
        self.assertEqual(
            counts['processed'],
            counts['succeeded'] + counts['failed'],
        )
        self.assertLessEqual(counts['succeeded'], counts['processed'])
        self.assertLessEqual(counts['processed'], counts['total'])

    def test_extract_result_counts_derives_signal_feature_scope_without_total(
        self,
    ) -> None:
        counts = _extract_result_counts(
            {
                'status': 'partial',
                'features_persisted': 2_702,
                'features_failed': 3,
            },
            default_total=2_675,
            stage_id='signal_features',
        )

        self.assertEqual(
            counts,
            {
                'processed': 2_705,
                'total': 2_705,
                'succeeded': 2_702,
                'failed': 3,
            },
        )
        self.assertEqual(
            counts['processed'],
            counts['succeeded'] + counts['failed'],
        )
        self.assertLessEqual(counts['succeeded'], counts['processed'])
        self.assertLessEqual(counts['processed'], counts['total'])

    def test_extract_result_counts_uses_signal_feature_retry_debt_scope(
        self,
    ) -> None:
        selected = ['005930', '000660', '035420']
        counts = _extract_result_counts(
            {
                'status': 'partial',
                'features_persisted': 0,
                'features_failed': 0,
                'items_total': 0,
                **build_exact_symbol_scope_outcome(
                    manifest_version=(
                        'signal_features_scope_outcomes_v1'
                    ),
                    selected_symbols=selected,
                    successful_symbols=[],
                    retry_debt_symbols=selected,
                ),
            },
            stage_id='signal_features',
        )

        self.assertEqual(
            counts,
            {
                'processed': 3,
                'total': 3,
                'succeeded': 0,
                'failed': 3,
            },
        )

    def test_extract_followup_source_run_ids_maps_feedback_result_run_id(
        self,
    ) -> None:
        self.assertEqual(
            _extract_followup_source_run_ids(
                stage_id='agent_feedback_summaries',
                result={
                    'run_id': 'feedback-cycle-summary',
                    'source_run_ids': {'parent_run_id': 'daily-run'},
                },
            ),
            {
                'parent_run_id': 'daily-run',
                'agent_feedback_run_id': 'feedback-cycle-summary',
            },
        )

    def test_extract_result_counts_includes_every_next_open_plan_outcome(self) -> None:
        counts = _extract_result_counts(
            {
                'status': 'completed',
                'summary': {
                    'items_total': 18,
                    'eligible_count': 4,
                    'monitor_candidate_count': 10,
                    'blocked_count': 4,
                },
            }
        )

        self.assertEqual(
            counts,
            {
                'processed': 18,
                'total': 18,
                'succeeded': 18,
                'failed': 0,
            },
        )

    def test_auto_tracker_normalizes_warning_followup_status_to_terminal_partial(self) -> None:
        fake_run_state_service = _FakeDailyPipelineRunStateService()
        log_events: list[dict] = []
        tracker = _OfficialDailyPipelineTracker(
            service=fake_run_state_service,
            log_fetch=lambda operation, status, **kwargs: log_events.append(
                {'operation': operation, 'status': status, **kwargs}
            ),
        )

        tracker.start(
            market_date='2026-04-03',
            trigger='auto_close_scheduler',
            requested_stages=['signal_decider_evaluations'],
            resolved_stages=['signal_decider_evaluations'],
            eod_run_id=12,
            run_metadata={'mode': 'daily_auto'},
        )
        tracker.update_stage(
            stage='signal_decider_evaluations',
            status='warning',
            current_step='signal_decider_evaluations_partial',
            current_activity='Completed signal-decider evaluation follow-up with mature coverage gaps',
            processed=4,
            total=4,
            succeeded=3,
            failed=1,
            details={'result_summary': {'status': 'warning', 'summary': {'not_computed_count': 1}}},
        )
        tracker.finish_run()

        self.assertEqual(_normalize_stage_status('warning'), 'partial')
        self.assertEqual(log_events, [])
        self.assertEqual(fake_run_state_service.stage_updates[-1]['status'], 'partial')
        self.assertEqual(tracker.stage_statuses['signal_decider_evaluations'], 'partial')
        self.assertEqual(fake_run_state_service.finish_runs[-1]['status'], 'partial')
        self.assertEqual(
            fake_run_state_service.finish_runs[-1]['run_metadata']['stage_statuses'],
            {'signal_decider_evaluations': 'partial'},
        )

    def test_auto_tracker_completes_with_optional_context_warning(self) -> None:
        fake_run_state_service = _FakeDailyPipelineRunStateService()
        tracker = _OfficialDailyPipelineTracker(
            service=fake_run_state_service,
            log_fetch=MagicMock(),
        )
        tracker.start(
            market_date='2026-04-03',
            trigger='auto_close_scheduler',
            requested_stages=['macro_context_refresh'],
            resolved_stages=['macro_context_refresh'],
            eod_run_id=12,
            run_metadata={'mode': 'daily_auto'},
        )
        tracker.update_stage(
            stage='macro_context_refresh',
            status='unavailable',
            current_step='macro_context_refresh_unavailable',
            current_activity='Macro context completed with stale cache',
            processed=3,
            total=3,
            succeeded=2,
            failed=1,
            details={
                'result_summary': {
                    'triggered': True,
                    'status': 'unavailable',
                    'remaining_retryable_events': 0,
                },
            },
        )
        tracker.finish_run()

        completion = fake_run_state_service.finish_runs[-1]
        self.assertEqual(tracker.stage_statuses['macro_context_refresh'], 'partial')
        self.assertEqual(
            fake_run_state_service.stage_updates[-1]['details'][
                'status_canonicalized_from'
            ],
            'unavailable',
        )
        self.assertEqual(completion['status'], 'completed')
        self.assertEqual(completion['run_metadata']['quality_status'], 'degraded')
        self.assertEqual(
            completion['run_metadata']['warning_stage_ids'],
            ['macro_context_refresh'],
        )
        self.assertEqual(completion['run_metadata']['warning_count'], 1)

    def test_auto_tracker_heartbeat_write_error_is_fatal(self) -> None:
        state_service = MagicMock()
        state_service.start_run.return_value = {'id': 501}
        state_service.heartbeat_stage.side_effect = RuntimeError(
            'heartbeat storage unavailable'
        )
        tracker = _OfficialDailyPipelineTracker(
            service=state_service,
            log_fetch=MagicMock(),
        )
        tracker.start(
            market_date='2026-04-03',
            trigger='auto_close_scheduler',
            requested_stages=['momentum_factors'],
            resolved_stages=['momentum_factors'],
            eod_run_id=12,
            run_metadata={'mode': 'daily_auto'},
        )

        with self.assertRaisesRegex(
            DailyPipelineTrackingWriteError,
            'heartbeat checkpoint failed for momentum_factors',
        ):
            tracker.heartbeat_stage(stage='momentum_factors')

    def test_failed_stage_counts_do_not_invent_unknown_scope(self) -> None:
        for case_name, raw_counts in (
            ('followup_exception_without_scope', None),
            (
                'factor_failure_with_synthetic_attempt_count',
                {
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 1,
                },
            ),
        ):
            with self.subTest(case_name=case_name):
                counts, scope_known = _truthful_failed_stage_counts(
                    raw_counts
                )

                self.assertFalse(scope_known)
                self.assertEqual(
                    counts,
                    {
                        'processed': 0,
                        'total': 0,
                        'succeeded': 0,
                        'failed': 0,
                    },
                )

    def test_auto_tracker_status_contract_for_pending_skip_and_malformed(
        self,
    ) -> None:
        cases = {
            'all_pending': (
                {'value_factors': 'pending', 'composite_factors': 'pending'},
                'pending',
            ),
            'all_completed': (
                {
                    'value_factors': 'completed',
                    'composite_factors': 'completed',
                },
                'completed',
            ),
            'generic_skip_only': ({'value_factors': 'skipped'}, 'partial'),
            'generic_skip_with_success': (
                {
                    'value_factors': 'completed',
                    'composite_factors': 'skipped',
                },
                'partial',
            ),
            'malformed': ({'value_factors': 'unexpected'}, 'partial'),
        }
        for case_name, (stage_statuses, expected_status) in cases.items():
            with self.subTest(case_name=case_name):
                tracker = _OfficialDailyPipelineTracker(
                    service=None,
                    log_fetch=MagicMock(),
                )
                tracker.stage_statuses = stage_statuses

                self.assertEqual(
                    tracker.resolve_overall_status(),
                    expected_status,
                )

    def test_auto_tracker_only_completes_satisfied_terminal_outcomes(
        self,
    ) -> None:
        safe_skip_tracker = _OfficialDailyPipelineTracker(
            service=None,
            log_fetch=MagicMock(),
        )
        safe_skip_tracker.stage_statuses = {
            'value_factors': 'completed',
            'signal_decider': 'skipped',
        }
        safe_skip_tracker.stage_outcomes = {
            'value_factors': {
                'stage': 'value_factors',
                'status': 'completed',
            },
            'signal_decider': {
                'stage': 'signal_decider',
                'status': 'skipped',
                'safe_skip': True,
            },
        }
        unsatisfied_tracker = _OfficialDailyPipelineTracker(
            service=None,
            log_fetch=MagicMock(),
        )
        unsatisfied_tracker.stage_statuses = {
            'value_factors': 'completed',
        }
        unsatisfied_tracker.stage_outcomes = {
            'value_factors': {
                'stage': 'value_factors',
                'status': 'completed',
                'failed': 1,
            }
        }

        self.assertEqual(
            safe_skip_tracker.resolve_overall_status(),
            'completed',
        )
        self.assertEqual(
            unsatisfied_tracker.resolve_overall_status(),
            'partial',
        )

    def test_auto_tracker_missing_resolved_stage_is_not_completed(self) -> None:
        tracker = _OfficialDailyPipelineTracker(
            service=None,
            log_fetch=MagicMock(),
        )
        tracker.resolved_stage_ids = ('value_factors',)

        self.assertEqual(tracker.resolve_overall_status(), 'partial')

    def test_auto_tracker_persists_mixed_failed_blocked_completed_as_partial(
        self,
    ) -> None:
        fake_run_state_service = _FakeDailyPipelineRunStateService()
        tracker = _OfficialDailyPipelineTracker(
            service=fake_run_state_service,
            log_fetch=MagicMock(),
        )
        tracker.start(
            market_date='2026-04-03',
            trigger='auto_close_scheduler',
            requested_stages=[
                'value_factors',
                'low_risk_factors',
                'composite_factors',
            ],
            resolved_stages=[
                'value_factors',
                'low_risk_factors',
                'composite_factors',
            ],
            eod_run_id=12,
            run_metadata={'mode': 'daily_auto'},
        )
        tracker.stage_statuses = {
            'value_factors': 'failed',
            'low_risk_factors': 'completed',
            'composite_factors': 'blocked',
        }

        tracker.finish_run()

        self.assertEqual(tracker.resolve_overall_status(), 'partial')
        self.assertEqual(
            fake_run_state_service.finish_runs[-1]['status'],
            'partial',
        )

    def test_auto_tracker_persists_failure_without_success_as_failed(
        self,
    ) -> None:
        cases = {
            'failed_only': {'value_factors': 'failed'},
            'failed_and_blocked': {
                'value_factors': 'failed',
                'composite_factors': 'blocked',
            },
        }
        for case_name, stage_statuses in cases.items():
            with self.subTest(case_name=case_name):
                fake_run_state_service = _FakeDailyPipelineRunStateService()
                tracker = _OfficialDailyPipelineTracker(
                    service=fake_run_state_service,
                    log_fetch=MagicMock(),
                )
                tracker.start(
                    market_date='2026-04-03',
                    trigger='auto_close_scheduler',
                    requested_stages=list(stage_statuses),
                    resolved_stages=list(stage_statuses),
                    eod_run_id=12,
                    run_metadata={'mode': 'daily_auto'},
                )
                tracker.stage_statuses = stage_statuses

                tracker.finish_run()

                self.assertEqual(tracker.resolve_overall_status(), 'failed')
                self.assertEqual(
                    fake_run_state_service.finish_runs[-1]['status'],
                    'failed',
                )

    def test_auto_tracker_running_status_precedes_terminal_outcomes(self) -> None:
        tracker = _OfficialDailyPipelineTracker(
            service=None,
            log_fetch=MagicMock(),
        )
        tracker.stage_statuses = {
            'value_factors': 'failed',
            'low_risk_factors': 'running',
            'composite_factors': 'completed',
        }

        self.assertEqual(tracker.resolve_overall_status(), 'running')

    def test_auto_tracker_mixed_terminal_incomplete_with_completed_is_partial(
        self,
    ) -> None:
        for incomplete_status in (
            'blocked',
            'waiting',
            'interrupted',
            'abandoned',
        ):
            with self.subTest(incomplete_status=incomplete_status):
                tracker = _OfficialDailyPipelineTracker(
                    service=None,
                    log_fetch=MagicMock(),
                )
                tracker.stage_statuses = {
                    'value_factors': 'completed',
                    'composite_factors': incomplete_status,
                }

                self.assertEqual(
                    tracker.resolve_overall_status(),
                    'partial',
                )

    def test_auto_tracker_pure_terminal_incomplete_statuses_remain_specific(
        self,
    ) -> None:
        for status in ('blocked', 'waiting', 'interrupted', 'abandoned'):
            with self.subTest(status=status):
                tracker = _OfficialDailyPipelineTracker(
                    service=None,
                    log_fetch=MagicMock(),
                )
                tracker.stage_statuses = {
                    'value_factors': status,
                    'composite_factors': status,
                }

                self.assertEqual(tracker.resolve_overall_status(), status)

    def test_auto_tracker_preserves_feedback_child_run_id_on_final_update(
        self,
    ) -> None:
        state_service = _FakeDailyPipelineRunStateService()
        tracker = _OfficialDailyPipelineTracker(
            service=state_service,
            log_fetch=MagicMock(),
        )
        tracker.start(
            market_date='2026-04-03',
            trigger='auto_close_scheduler',
            requested_stages=['agent_feedback_summaries'],
            resolved_stages=['agent_feedback_summaries'],
            eod_run_id=7,
            run_metadata={'mode': 'daily_auto'},
        )

        tracker.update_stage(
            stage='agent_feedback_summaries',
            status='running',
            current_step='role_started',
            current_activity='Summarizer started',
            source_run_ids={
                'agent_feedback_run_id': 'feedback-cycle-summary'
            },
        )
        tracker.update_stage(
            stage='agent_feedback_summaries',
            status='completed',
            current_step='agent_feedback_summaries_completed',
            current_activity='Completed agent feedback summaries',
        )

        self.assertEqual(
            state_service.stage_updates[-1]['source_run_ids'],
            {
                'eod_run_id': 7,
                'agent_feedback_run_id': 'feedback-cycle-summary',
            },
        )

    def test_auto_tracker_retries_terminal_checkpoint(self) -> None:
        state_service = MagicMock()
        state_service.start_run.return_value = {'id': 501}
        state_service.finish_run.side_effect = [
            RuntimeError('commit response lost'),
            RuntimeError('retry timeout'),
            {'id': 501, 'status': 'completed'},
        ]
        tracker = _OfficialDailyPipelineTracker(
            service=state_service,
            log_fetch=MagicMock(),
        )
        tracker.start(
            market_date='2026-04-03',
            trigger='auto_close_scheduler',
            requested_stages=['price_collection'],
            resolved_stages=['price_collection'],
            eod_run_id=None,
            run_metadata={'mode': 'daily_auto'},
        )

        tracker.finish_run(status='completed')

        self.assertEqual(state_service.finish_run.call_count, 3)

    def test_auto_tracker_failure_cleanup_does_not_mask_primary_error(
        self,
    ) -> None:
        state_service = MagicMock()
        state_service.start_run.return_value = {'id': 501}
        state_service.finish_run.side_effect = RuntimeError(
            'terminal database unavailable'
        )
        tracker = _OfficialDailyPipelineTracker(
            service=state_service,
            log_fetch=MagicMock(),
        )
        tracker.start(
            market_date='2026-04-03',
            trigger='auto_close_scheduler',
            requested_stages=['price_collection'],
            resolved_stages=['price_collection'],
            eod_run_id=None,
            run_metadata={'mode': 'daily_auto'},
        )

        tracker.finish_run_preserving_primary_error(
            error_message='provider failed'
        )

        self.assertEqual(state_service.finish_run.call_count, 3)

    def test_auto_feedback_followups_use_matching_decider_scope(self) -> None:
        from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
        from app.agent_context.run_modes import FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH

        configs = []
        shared_cutoff = datetime(
            2026,
            4,
            3,
            7,
            15,
            tzinfo=timezone.utc,
        )

        def run_retry_cycles(*, config, **_kwargs):
            configs.append(config)
            return {'status': 'completed'}

        with (
            patch(
                'app.agent_context.prerequisite_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                return_value={'ready': True, 'status': 'ready'},
            ),
            patch(
                'app.agent_context.retry_service.agent_feedback_retry_service.run_retry_cycles',
                side_effect=run_retry_cycles,
            ),
        ):
            summary_result = _run_daily_feedback_followup(
                market_date='2026-04-03',
                enabled=True,
                knowledge_cutoff_at=shared_cutoff,
                symbols=['005930'],
            )
            decider_result = _run_daily_signal_decider_followup(
                market_date='2026-04-03',
                enabled=True,
                knowledge_cutoff_at=shared_cutoff,
                symbols=['005930'],
            )

        self.assertEqual(summary_result['status'], 'completed')
        self.assertEqual(decider_result['status'], 'completed')
        self.assertEqual(
            [config.roles for config in configs],
            [SUMMARIZER_AGENT_NAMES, DECISION_SUPPORT_AGENT_NAMES],
        )
        self.assertEqual(
            [config.feedback_scope_mode for config in configs],
            [
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
            ],
        )
        self.assertEqual(
            [config.knowledge_cutoff_at for config in configs],
            [shared_cutoff, shared_cutoff],
        )
        self.assertEqual(
            [config.point_in_time_recovery for config in configs],
            [False, False],
        )
        self.assertEqual(
            [config.symbols for config in configs],
            [('005930',), ('005930',)],
        )

    def test_auto_feedback_followups_propagate_point_in_time_recovery(
        self,
    ) -> None:
        configs = []

        def run_retry_cycles(*, config, **_kwargs):
            configs.append(config)
            return {'status': 'completed'}

        with (
            patch(
                'app.agent_context.prerequisite_service.'
                'agent_feedback_prerequisite_service.'
                'inspect_feedback_prerequisites',
                return_value={'ready': True, 'status': 'ready'},
            ),
            patch(
                'app.agent_context.retry_service.'
                'agent_feedback_retry_service.run_retry_cycles',
                side_effect=run_retry_cycles,
            ),
            patch(
                'app.eod.pykrx_pipeline_runner.'
                'build_expired_decision_replay_result',
                return_value=None,
            ),
        ):
            _run_daily_feedback_followup(
                market_date='2026-04-03',
                enabled=True,
                point_in_time_recovery=True,
            )
            _run_daily_signal_decider_followup(
                market_date='2026-04-03',
                enabled=True,
                point_in_time_recovery=True,
            )

        self.assertEqual(
            [config.point_in_time_recovery for config in configs],
            [True, True],
        )

    def test_auto_followup_order_matches_canonical_news_macro_feedback_dependencies(self) -> None:
        signal_index = AUTO_DAILY_FOLLOWUP_STAGE_ORDER.index('signal_features')

        self.assertEqual(
            AUTO_DAILY_FOLLOWUP_STAGE_ORDER[signal_index : signal_index + 5],
            (
                'signal_features',
                'external_news_collect',
                'external_news_enrich',
                'macro_context_refresh',
                'agent_feedback_summaries',
            ),
        )

    def test_external_news_enrichment_followup_uses_idempotent_classification_stage(self) -> None:
        with patch(
            'app.external_news.service.external_news_service.enrich_recent_classification',
            return_value={'status': 'completed', 'classifications_persisted': 2},
        ) as enrich_recent_classification:
            result = _run_daily_external_news_enrichment_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['market_date'], '2026-04-03')
        enrich_recent_classification.assert_called_once_with(
            market_date='2026-04-03',
            lookback_days=ANY,
            limit=1000,
        )

    def test_macro_context_followup_uses_service_freshness_gate_instead_of_forcing_duplicate_refresh(self) -> None:
        macro_result = {
            'status': 'skipped',
            'reason': 'macro_context_refresh_not_due',
            'context': {'freshness': {'status': 'fresh'}},
            'source_statuses': {
                'world_bank': {'status': 'available'},
                'fred': {
                    'status': 'disabled_missing_api_key',
                    'reason': 'FRED_API_KEY is not configured',
                },
            },
        }
        with patch(
            'app.macro_context.service.macro_context_service.refresh_context',
            return_value=macro_result,
        ) as refresh_context:
            result = _run_daily_macro_context_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertEqual(result['market_date'], '2026-04-03')
        self.assertEqual(result['freshness_gate'], 'fresh')
        self.assertEqual(
            result['provider_coverage'],
            {
                'status': 'partial',
                'provider_count': 2,
                'available_provider_count': 1,
                'configured_provider_count': 0,
                'gap_provider_count': 1,
                'providers': {
                    'world_bank': 'available',
                    'fred': 'disabled',
                },
            },
        )
        refresh_context.assert_called_once_with(force=False, trigger='eod_pykrx_followup')

    def test_macro_provider_coverage_prefers_fresh_context_over_configured_refresh_status(self) -> None:
        macro_result = {
            'status': 'skipped',
            'reason': 'macro_context_refresh_not_due',
            'source_statuses': {
                'world_bank': {'status': 'configured'},
                'fred': {'status': 'disabled_missing_api_key'},
            },
            'context': {
                'freshness': {'status': 'fresh'},
                'source_statuses': {
                    'world_bank': {'status': 'available'},
                    'fred': {'status': 'disabled_missing_api_key'},
                },
            },
        }
        with patch(
            'app.macro_context.service.macro_context_service.refresh_context',
            return_value=macro_result,
        ):
            result = _run_daily_macro_context_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertEqual(result['freshness_gate'], 'fresh')
        self.assertEqual(result['provider_coverage']['status'], 'partial')
        self.assertEqual(
            result['provider_coverage']['providers']['world_bank'],
            'available',
        )

    def test_macro_provider_coverage_does_not_treat_configured_as_observed(self) -> None:
        coverage = _build_macro_provider_coverage(
            {
                'world_bank': {'status': 'configured'},
                'fred': {'status': 'disabled_missing_api_key'},
            }
        )

        self.assertEqual(coverage['status'], 'unknown')
        self.assertEqual(coverage['available_provider_count'], 0)
        self.assertEqual(coverage['configured_provider_count'], 1)

    def test_macro_provider_coverage_treats_missing_credential_as_unavailable(
        self,
    ) -> None:
        coverage = _build_macro_provider_coverage(
            {
                'world_bank': {'status': 'available'},
                'fred': {'status': 'blocked_missing_credential'},
            }
        )

        self.assertEqual(coverage['status'], 'partial')
        self.assertEqual(coverage['gap_provider_count'], 1)
        self.assertEqual(coverage['providers']['fred'], 'unavailable')

    def test_fetch_recent_universe_uses_index_when_market_frame_columns_change(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0
        frame = pd.DataFrame({'현재가': [196500]}, index=['005930'])

        def fake_call(func, *args, **kwargs):
            if func.__name__ == 'get_market_ohlcv_by_ticker':
                return frame
            if func.__name__ == 'get_market_ticker_name':
                return '삼성전자'
            raise AssertionError(f'unexpected pykrx function: {func.__name__}')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-07'),
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=fake_call),
        ):
            result = service.fetch_recent_universe_snapshot(markets=('KOSPI',))

        self.assertEqual(result['latest_trade_date'], '2026-04-07')
        self.assertEqual(result['universe_source'], 'pykrx_market_ohlcv')
        self.assertEqual(
            result['symbols'],
            [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='삼성전자')],
        )

    def test_fetch_recent_universe_falls_back_to_ticker_when_name_lookup_returns_frame(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0
        frame = pd.DataFrame({'현재가': [196500]}, index=['005930'])
        bad_name = pd.DataFrame({'unexpected': ['html']})

        def fake_call(func, *args, **kwargs):
            if func.__name__ == 'get_market_ohlcv_by_ticker':
                return frame
            if func.__name__ == 'get_market_ticker_name':
                return bad_name
            raise AssertionError(f'unexpected pykrx function: {func.__name__}')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-07'),
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=fake_call),
        ):
            result = service.fetch_recent_universe_snapshot(markets=('KOSPI',))

        self.assertEqual(
            result['symbols'],
            [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='005930')],
        )

    def test_resolve_reference_trade_date_falls_back_to_krx_calendar_when_pykrx_probe_fails(self) -> None:
        service = PykrxBackfillService()

        with (
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', side_effect=ConnectionError('naver reset')),
            patch('app.eod.pykrx_backfill.latest_krx_trading_date', return_value=datetime(2026, 6, 9).date()) as latest,
        ):
            result = service.resolve_latest_trade_date(end_date='20260609')

        self.assertEqual(result, '2026-06-09')
        latest.assert_called_once()

    def test_fetch_recent_universe_falls_back_per_market_when_pykrx_market_ohlcv_breaks(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-07'),
            patch(
                'app.eod.pykrx_backfill._call_pykrx',
                side_effect=KeyError("None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"),
            ),
            patch.object(
                service,
                '_fetch_master_fallback_universe',
                return_value=[PykrxSymbol(symbol='000020', market='KOSPI', name_ko='동화약품')],
            ) as fallback_universe,
        ):
            result = service.fetch_recent_universe_snapshot(markets=('KOSPI',))

        self.assertEqual(result['latest_trade_date'], '2026-04-07')
        self.assertEqual(result['universe_source'], 'pykrx_market_ohlcv_with_master_fallback')
        self.assertEqual(
            result['symbols'],
            [PykrxSymbol(symbol='000020', market='KOSPI', name_ko='동화약품')],
        )
        fallback_universe.assert_called_once_with(markets=('KOSPI',), limit=None)

    def test_historical_universe_uses_requested_end_date_and_never_current_master_fallback(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0
        frame = pd.DataFrame({'종가': [5000]}, index=['099999'])

        def fake_call(func, *args, **kwargs):
            if func.__name__ == 'get_market_ohlcv_by_ticker':
                self.assertEqual(args[0], '20200429')
                return frame
            if func.__name__ == 'get_market_ticker_name':
                return '과거상장종목'
            raise AssertionError(f'unexpected pykrx function: {func.__name__}')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2020-04-29') as resolve_date,
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=fake_call),
            patch.object(service, '_fetch_master_fallback_universe') as master_fallback,
        ):
            result = service.fetch_recent_universe_snapshot(
                markets=('KOSPI',),
                end_date='20200430',
                allow_master_fallback=False,
            )

        resolve_date.assert_called_once_with(end_date='20200430')
        master_fallback.assert_not_called()
        self.assertEqual(
            result['symbols'],
            [PykrxSymbol(symbol='099999', market='KOSPI', name_ko='과거상장종목')],
        )
        self.assertEqual(result['membership_basis'], 'end_date_snapshot')

    def test_universe_limit_marks_later_markets_unqueried_without_source_failure(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0
        frame = pd.DataFrame({'종가': [5000]}, index=['099999'])

        def fake_call(func, *args, **kwargs):
            if func.__name__ == 'get_market_ohlcv_by_ticker':
                self.assertEqual(kwargs['market'], 'KOSPI')
                return frame
            if func.__name__ == 'get_market_ticker_name':
                return '표본종목'
            raise AssertionError(f'unexpected pykrx function: {func.__name__}')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2020-04-29'),
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=fake_call),
        ):
            result = service.fetch_recent_universe_snapshot(
                markets=('KOSPI', 'KOSDAQ'),
                end_date='20200430',
                limit=1,
                allow_master_fallback=False,
            )

        self.assertTrue(result['complete'])
        self.assertEqual(result['unavailable_markets'], [])
        self.assertEqual(result['market_status']['KOSDAQ'], 'not_queried_due_to_limit')

    def test_historical_universe_fails_closed_when_point_in_time_source_is_unavailable(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2020-04-29'),
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=ConnectionError('source unavailable')),
            patch.object(service, '_fetch_master_fallback_universe') as master_fallback,
        ):
            result = service.fetch_recent_universe_snapshot(
                markets=('KOSPI',),
                end_date='20200430',
                allow_master_fallback=False,
            )

        master_fallback.assert_not_called()
        self.assertEqual(result['symbols'], [])
        self.assertEqual(result['universe_source'], 'pykrx_market_ohlcv')
        self.assertFalse(result['complete'])
        self.assertEqual(result['unavailable_markets'], ['KOSPI'])

    def test_historical_universe_marks_partial_requested_market_snapshot_incomplete(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0
        frame = pd.DataFrame({'종가': [5000]}, index=['099999'])

        def fake_call(func, *args, **kwargs):
            if func.__name__ == 'get_market_ohlcv_by_ticker':
                if kwargs['market'] == 'KOSPI':
                    return frame
                raise ConnectionError('KOSDAQ source unavailable')
            if func.__name__ == 'get_market_ticker_name':
                return '과거상장종목'
            raise AssertionError(f'unexpected pykrx function: {func.__name__}')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2020-04-29'),
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=fake_call),
        ):
            result = service.fetch_recent_universe_snapshot(
                markets=('KOSPI', 'KOSDAQ'),
                end_date='20200430',
                allow_master_fallback=False,
            )

        self.assertFalse(result['complete'])
        self.assertEqual(result['market_status']['KOSPI'], 'pykrx_market_ohlcv')
        self.assertEqual(result['market_status']['KOSDAQ'], 'unavailable')
        self.assertEqual(result['unavailable_markets'], ['KOSDAQ'])
        self.assertEqual(result['market_errors'][0]['status'], 'fetch_failed')

    def test_historical_pipeline_rejects_partial_requested_market_snapshot(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='099999', market='KOSPI', name_ko='과거상장종목')],
            'latest_trade_date': '2020-04-29',
            'universe_source': 'pykrx_market_ohlcv',
            'membership_basis': 'end_date_snapshot',
            'complete': False,
            'market_status': {
                'KOSPI': 'pykrx_market_ohlcv',
                'KOSDAQ': 'unavailable',
            },
            'unavailable_markets': ['KOSDAQ'],
            'market_errors': [{'market': 'KOSDAQ', 'status': 'fetch_failed'}],
        }

        with (
            patch.object(service, 'fetch_recent_universe_snapshot', return_value=snapshot),
            patch('app.eod.pykrx_backfill.create_eod_collection_run') as create_run,
        ):
            with self.assertRaisesRegex(RuntimeError, 'incomplete.*KOSDAQ'):
                service._run_market_data_pipeline(
                    start_date='20200430',
                    end_date='20200430',
                    markets=('KOSPI', 'KOSDAQ'),
                    trigger='test',
                    refresh_universe=True,
                    mode='historical_backfill',
                )

        create_run.assert_not_called()

    def test_historical_pipeline_forces_point_in_time_universe_when_refresh_is_disabled(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='099999', market='KOSPI', name_ko='과거상장종목')],
            'latest_trade_date': '2020-04-29',
            'universe_source': 'pykrx_market_ohlcv',
            'complete': True,
            'market_status': {'KOSPI': 'pykrx_market_ohlcv'},
            'unavailable_markets': [],
            'market_errors': [],
        }

        with (
            patch.object(service, 'fetch_recent_universe_snapshot', return_value=snapshot) as fetch_snapshot,
            patch.object(service, 'load_active_universe') as load_active_universe,
            patch.object(service, 'find_symbol') as find_symbol,
            patch.object(service, 'resolve_latest_trade_date', return_value='2020-04-29'),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                side_effect=RuntimeError('stop after universe selection'),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, 'stop after universe selection'):
                service._run_market_data_pipeline(
                    start_date='20200430',
                    end_date='20200430',
                    markets=('KOSPI',),
                    trigger='test',
                    refresh_universe=False,
                    mode='historical_backfill',
                    limit=1,
                    symbol='099999',
                )

        fetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20200430',
            allow_master_fallback=False,
        )
        load_active_universe.assert_not_called()
        find_symbol.assert_not_called()

    def test_prior_date_daily_auto_forces_point_in_time_membership_without_current_universe_writes(
        self,
    ) -> None:
        service = PykrxBackfillService()
        snapshot = _point_in_time_universe_snapshot(
            [
                PykrxSymbol(
                    symbol='099999',
                    market='KOSPI',
                    name_ko='Friday listed symbol',
                )
            ],
            market_date='2026-09-11',
        )
        monday_market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T10:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
        )

        with (
            patch(
                'app.eod.pykrx_pipeline_runner.get_market_state',
                return_value=monday_market,
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=snapshot,
            ) as fetch_snapshot,
            patch.object(service, 'load_active_universe') as load_active_universe,
            patch.object(service, '_persist_universe') as persist_universe,
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-09-11',
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                side_effect=RuntimeError('stop after point-in-time admission'),
            ) as create_run,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'stop after point-in-time admission',
            ):
                service._run_market_data_pipeline(
                    start_date='20260911',
                    end_date='20260911',
                    markets=('KOSPI',),
                    trigger='auto_close_scheduler',
                    refresh_universe=True,
                    mode='daily_auto',
                    selected_stages=('price_collection',),
                    chain_fundamentals_followup=False,
                    point_in_time_recovery=True,
                )

        fetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20260911',
            allow_master_fallback=False,
        )
        load_active_universe.assert_not_called()
        persist_universe.assert_not_called()
        metadata = create_run.call_args.kwargs['run_metadata']
        self.assertTrue(metadata['point_in_time_recovery'])
        self.assertFalse(
            metadata['universe_snapshot']['current_universe_mutated']
        )

    def test_prior_date_daily_auto_preserves_prices_and_narrows_partial_followups(
        self,
    ) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(
                symbol=f'{symbol:06d}',
                market='KOSPI',
                name_ko=f'Friday listed symbol {symbol}',
            )
            for symbol in (99997, 99998, 99999)
        ]
        symbol_codes = [item.symbol for item in symbols]
        with (
            patch(
                'app.eod.pykrx_pipeline_runner.get_market_state',
                return_value=SimpleNamespace(
                    state='OPEN',
                    server_time='2026-09-14T10:00:00+09:00',
                ),
            ),
            patch(
                'app.eod.pykrx_pipeline_runner.'
                'should_skip_point_in_time_current_only_stage',
                wraps=should_skip_point_in_time_current_only_stage,
            ) as current_only_stage_policy,
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    symbols,
                    market_date='2026-09-11',
                ),
            ),
            patch.object(service, '_persist_universe') as persist_universe,
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-09-11',
            ),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[
                    {
                        'trade_date': '2026-09-11',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }
                ],
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=12,
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_ticker',
                return_value=_AnySymbolTurnoverFrame(),
            ),
            patch(
                'app.eod.pykrx_backfill.persist_daily_price_bars',
                return_value=1,
            ) as persist_bars,
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ) as update_progress,
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 3,
                    'contexts_failed': 0,
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.'
                'rebuild_for_market_date',
            ) as rebuild_kis_risk,
            patch(
                'app.eod.pykrx_backfill.pykrx_enrichment_service.'
                'collect_for_market_date',
                return_value=_ready_pykrx_enrichment_result(
                    market_date='2026-09-11',
                    symbols=tuple(symbol_codes),
                ),
            ) as collect_enrichment,
            patch(
                'app.eod.pykrx_pipeline_runner.'
                '_run_daily_piotroski_filter_followup',
                return_value={
                    'status': 'partial',
                    'results_persisted': 2,
                    'results_failed': 1,
                    'gate_status': 'ready',
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'piotroski_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                        successful_symbols=symbol_codes[:2],
                        retry_debt_symbols=symbol_codes[2:],
                    ),
                },
            ) as rebuild_piotroski,
            patch(
                'app.eod.pykrx_pipeline_runner._run_daily_factor_followup',
                return_value={
                    'triggered': True,
                    'status': 'partial',
                    'results_persisted': 1,
                    'results_failed': 1,
                    'gate_status': 'ready',
                    'blocking_status': 'non_blocking',
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'factor_followup_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes[:2],
                        successful_symbols=symbol_codes[:1],
                        retry_debt_symbols=symbol_codes[1:2],
                    ),
                },
            ) as rebuild_factors,
            patch(
                'app.eod.pykrx_pipeline_runner.'
                '_run_daily_feedback_followup',
                return_value={
                    'triggered': True,
                    'status': 'completed',
                },
            ) as feedback_followup,
            patch(
                'app.eod.pykrx_pipeline_runner.'
                '_run_daily_signal_decider_followup',
                return_value={
                    'triggered': False,
                    'status': 'skipped',
                    'reason': 'next_open_window_expired',
                },
            ) as signal_decider_followup,
            patch('app.eod.pykrx_backfill.complete_eod_collection_run'),
        ):
            result = service._run_market_data_pipeline(
                start_date='20260911',
                end_date='20260911',
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=True,
                mode='daily_auto',
                selected_stages=(
                    'price_collection',
                    'pykrx_enrichment',
                    'market_risk_filter',
                ),
                chain_fundamentals_followup=True,
                point_in_time_recovery=True,
            )

            partial_factor_call = rebuild_factors.call_args
            partial_feedback_call = feedback_followup.call_args
            partial_signal_decider_call = signal_decider_followup.call_args

            rebuild_piotroski.return_value = {
                'status': 'completed',
                'results_persisted': len(symbol_codes),
                'results_failed': 0,
                'gate_status': 'ready',
                **build_exact_symbol_scope_outcome(
                    manifest_version='piotroski_scope_outcomes_v1',
                    selected_symbols=symbol_codes,
                    successful_symbols=symbol_codes,
                    retry_debt_symbols=(),
                ),
            }
            rebuild_factors.return_value = {
                'triggered': True,
                'status': 'completed',
                'results_persisted': len(symbol_codes),
                'results_failed': 0,
                'gate_status': 'ready',
                'blocking_status': 'non_blocking',
                **build_exact_symbol_scope_outcome(
                    manifest_version=(
                        'factor_followup_scope_outcomes_v1'
                    ),
                    selected_symbols=symbol_codes,
                    successful_symbols=symbol_codes,
                    retry_debt_symbols=(),
                ),
            }
            current_only_stage_policy.reset_mock()
            collect_enrichment.reset_mock()
            rebuild_piotroski.reset_mock()
            rebuild_factors.reset_mock()
            feedback_followup.reset_mock()
            signal_decider_followup.reset_mock()
            persist_bars.reset_mock()

            full_cohort_result = service._run_market_data_pipeline(
                start_date='20260911',
                end_date='20260911',
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=True,
                mode='daily_auto',
                selected_stages=(
                    'price_collection',
                    'pykrx_enrichment',
                    'market_risk_filter',
                ),
                chain_fundamentals_followup=True,
                point_in_time_recovery=True,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertTrue(result['market_risk_filter']['safe_skip'])
        current_only_stage_policy.assert_called_once_with(
            stage_id='market_risk_filter',
            market_date='2026-09-11',
        )
        persist_universe.assert_not_called()
        rebuild_kis_risk.assert_not_called()
        collect_enrichment.assert_called_once_with(
            market_date='2026-09-11',
            markets=('KOSPI',),
            symbols=symbol_codes,
            use_full_market_coverage=True,
            daily_price_bar_lock_owned=True,
            point_in_time_recovery=True,
        )
        rebuild_piotroski.assert_called_once_with(
            market_date='2026-09-11',
            exact_membership_symbols=symbol_codes,
        )
        self.assertEqual(
            partial_factor_call.kwargs,
            {
                'market_date': '2026-09-11',
                'exact_membership_symbols': symbol_codes,
                'admitted_symbols': symbol_codes[:2],
                'stage_executor': ANY,
                'stage_result_callback': ANY,
            },
        )
        rebuild_factors.assert_called_once_with(
            market_date='2026-09-11',
            exact_membership_symbols=symbol_codes,
            stage_executor=ANY,
            stage_result_callback=ANY,
        )
        self.assertTrue(
            partial_feedback_call.kwargs[
                'point_in_time_recovery'
            ]
        )
        self.assertTrue(
            partial_signal_decider_call.kwargs[
                'point_in_time_recovery'
            ]
        )
        self.assertEqual(
            partial_feedback_call.kwargs['symbols'],
            symbol_codes[:1],
        )
        self.assertEqual(
            partial_signal_decider_call.kwargs['symbols'],
            symbol_codes[:1],
        )
        self.assertEqual(full_cohort_result['status'], 'completed')
        self.assertEqual(
            feedback_followup.call_args.kwargs['symbols'],
            symbol_codes,
        )
        self.assertEqual(
            signal_decider_followup.call_args.kwargs['symbols'],
            symbol_codes,
        )
        self.assertEqual(
            result['piotroski_filter_followup']['status'],
            'partial',
        )
        self.assertEqual(
            result['factor_scoring_followup']['status'],
            'partial',
        )
        self.assertFalse(
            persist_bars.call_args.kwargs['update_current_symbol_state']
        )

    def test_production_manual_boundaries_auto_promote_prior_date_recovery(
        self,
    ) -> None:
        service = PykrxBackfillService()

        for trigger in ('manual_daily_api', 'internal_ops_cli'):
            with (
                self.subTest(trigger=trigger),
                patch(
                    'app.eod.pykrx_backfill._current_krx_calendar_date',
                    return_value=date(2026, 9, 14),
                ),
                patch.object(
                    service,
                    'resolve_latest_trade_date',
                    return_value='2026-09-11',
                ),
                patch(
                    'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                    return_value=_lock(),
                ),
                patch(
                    'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                    return_value=None,
                ),
                patch.object(
                    service,
                    '_run_market_data_pipeline',
                    return_value={'status': 'completed'},
                ) as run_pipeline,
            ):
                result = service.run_daily_pipeline_if_due(
                    market_date='2026-09-11',
                    trigger=trigger,
                )

            self.assertTrue(result['triggered'])
            self.assertTrue(
                run_pipeline.call_args.kwargs['point_in_time_recovery']
            )

    def test_pipeline_persists_point_in_time_market_membership_proof(self) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(symbol='005930', market='KOSPI', name_ko='삼성전자'),
            PykrxSymbol(symbol='247540', market='KOSDAQ', name_ko='에코프로비엠'),
        ]

        with (
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    symbols,
                    market_date='2026-04-07',
                ),
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                side_effect=RuntimeError('stop after run admission'),
            ) as create_run,
        ):
            with self.assertRaisesRegex(RuntimeError, 'stop after run admission'):
                service._run_market_data_pipeline(
                    start_date='20260407',
                    end_date='20260407',
                    markets=('KOSPI', 'KOSDAQ'),
                    trigger='test',
                    refresh_universe=True,
                    mode='historical_backfill',
                )

        metadata = create_run.call_args.kwargs['run_metadata']
        membership = metadata['universe_snapshot']['market_membership']
        self.assertEqual(membership['schema_version'], 'pykrx_market_membership_v1')
        self.assertEqual(membership['symbol_count'], 2)
        self.assertEqual(membership['markets']['KOSPI']['symbols'], ['005930'])
        self.assertEqual(membership['markets']['KOSDAQ']['symbols'], ['247540'])
        self.assertEqual(
            membership['markets']['KOSPI']['membership_sha256'],
            hashlib.sha256(b'005930').hexdigest(),
        )

    def test_historical_pipeline_rejects_symbol_override_membership_bypass(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'fetch_recent_universe_snapshot') as fetch_snapshot,
            patch.object(service, 'load_active_universe') as load_active_universe,
        ):
            with self.assertRaisesRegex(
                ValueError,
                'membership must be derived from the requested end date',
            ):
                service._run_market_data_pipeline(
                    start_date='20200401',
                    end_date='20200430',
                    markets=('KOSPI',),
                    trigger='test',
                    refresh_universe=False,
                    mode='historical_backfill',
                    symbols_override=[
                        PykrxSymbol(
                            symbol='005930',
                            market='KOSPI',
                            name_ko='Current master row',
                        )
                    ],
                )

        fetch_snapshot.assert_not_called()
        load_active_universe.assert_not_called()

    def test_historical_pipeline_rejects_multi_day_range_before_io(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'fetch_recent_universe_snapshot') as fetch_snapshot,
            patch('app.eod.pykrx_backfill.create_eod_collection_run') as create_run,
            patch('app.eod.pykrx_backfill.log_fetch_event') as log_fetch,
        ):
            with self.assertRaisesRegex(ValueError, 'one market date per request'):
                service._run_market_data_pipeline(
                    start_date='20200401',
                    end_date='20200430',
                    markets=('KOSPI',),
                    trigger='test',
                    refresh_universe=True,
                    mode='historical_backfill',
                )

        fetch_snapshot.assert_not_called()
        create_run.assert_not_called()
        log_fetch.assert_not_called()

    def test_run_backfill_uses_only_point_in_time_safe_stages_and_disables_followups(self) -> None:
        service = PykrxBackfillService()
        with (
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-05-04') as latest,
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={'status': 'completed'},
            ) as run_pipeline,
        ):
            service.run_backfill()

        latest.assert_called_once_with()
        kwargs = run_pipeline.call_args.kwargs
        self.assertEqual(kwargs['start_date'], '20260504')
        self.assertEqual(kwargs['end_date'], '20260504')
        self.assertEqual(kwargs['selected_stages'], HISTORICAL_BACKFILL_STAGE_ORDER)
        self.assertFalse(kwargs['chain_fundamentals_followup'])

    def test_run_backfill_preserves_explicit_trading_date(self) -> None:
        service = PykrxBackfillService()
        with (
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={'status': 'completed'},
            ) as run_pipeline,
        ):
            service.run_backfill(end_date='20200430')

        kwargs = run_pipeline.call_args.kwargs
        self.assertEqual(kwargs['start_date'], '20200430')
        self.assertEqual(kwargs['end_date'], '20200430')
        self.assertEqual(kwargs['selected_stages'], HISTORICAL_BACKFILL_STAGE_ORDER)
        self.assertFalse(kwargs['chain_fundamentals_followup'])

    def test_historical_pipeline_rejects_current_only_and_invalid_stage_dependencies_before_io(self) -> None:
        service = PykrxBackfillService()
        for stages, message in (
            (('price_collection', 'signal_features'), 'point-in-time-safe stages'),
            (('technical_snapshots',), 'price_collection stage is required'),
            (('price_collection', 'action_summaries'), 'technical_snapshots is required'),
            ((), 'At least one pykrx stage'),
        ):
            with self.subTest(stages=stages), patch.object(
                service,
                'fetch_recent_universe_snapshot',
            ) as fetch_snapshot:
                with self.assertRaisesRegex(ValueError, message):
                    service._run_market_data_pipeline(
                        start_date='20200430',
                        end_date='20200430',
                        markets=('KOSPI',),
                        trigger='test',
                        refresh_universe=True,
                        mode='historical_backfill',
                        selected_stages=stages,
                    )
                fetch_snapshot.assert_not_called()

    def test_historical_pipeline_does_not_find_missing_symbol_in_current_master(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='099999', market='KOSPI', name_ko='과거상장종목')],
            'latest_trade_date': '2020-04-29',
            'universe_source': 'pykrx_market_ohlcv',
            'complete': True,
            'market_status': {'KOSPI': 'pykrx_market_ohlcv'},
            'unavailable_markets': [],
            'market_errors': [],
        }

        with (
            patch.object(service, 'fetch_recent_universe_snapshot', return_value=snapshot),
            patch.object(service, 'find_symbol') as find_symbol,
        ):
            with self.assertRaisesRegex(RuntimeError, 'No active pykrx symbols'):
                service._run_market_data_pipeline(
                    start_date='20200430',
                    end_date='20200430',
                    markets=('KOSPI',),
                    trigger='test',
                    refresh_universe=True,
                    mode='historical_backfill',
                    symbol='005930',
                )

        find_symbol.assert_not_called()

    def test_point_in_time_retry_rehydrates_inactive_symbol_from_membership_evidence(
        self,
    ) -> None:
        service = PykrxBackfillService()
        run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'run_metadata': {
                'markets': ['KOSPI', 'KOSDAQ'],
                'requested_symbols': ['099999', '088888'],
                'universe_snapshot': {
                    'source': 'pykrx',
                    'latest_trade_date': '2026-09-11',
                    'membership_basis': 'end_date_snapshot',
                    'point_in_time': True,
                    'market_membership': {
                        'schema_version': 'pykrx_market_membership_v1',
                        'membership_basis': 'end_date_snapshot',
                        'symbol_count': 2,
                        'markets': {
                            'KOSPI': {
                                'symbols': ['099999'],
                                'symbol_count': 1,
                                'membership_sha256': hashlib.sha256(
                                    b'099999'
                                ).hexdigest(),
                            },
                            'KOSDAQ': {
                                'symbols': ['088888'],
                                'symbol_count': 1,
                                'membership_sha256': hashlib.sha256(
                                    b'088888'
                                ).hexdigest(),
                            },
                        },
                        'symbol_metadata': [
                            {
                                'symbol': '099999',
                                'market': 'KOSPI',
                                'name_ko': 'Friday only listing',
                                'standard_code': 'KR7099990001',
                            },
                            {
                                'symbol': '088888',
                                'market': 'KOSDAQ',
                                'name_ko': 'Other Friday listing',
                                'standard_code': None,
                            },
                        ],
                    }
                },
            },
        }

        with (
            patch('app.eod.pykrx_backfill.load_symbols') as load_current_symbols,
            patch.object(service, 'find_symbol') as find_current_symbol,
        ):
            result = service._load_retry_symbols_for_run(
                run,
                markets=('KOSPI', 'KOSDAQ'),
                symbol_codes=['099999'],
                point_in_time_recovery=True,
            )

        self.assertEqual(
            result,
            [
                PykrxSymbol(
                    symbol='099999',
                    market='KOSPI',
                    name_ko='Friday only listing',
                    standard_code='KR7099990001',
                )
            ],
        )
        load_current_symbols.assert_not_called()
        find_current_symbol.assert_not_called()

    def test_point_in_time_retry_rejects_cross_date_membership_envelope(
        self,
    ) -> None:
        service = PykrxBackfillService()
        run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': ['099999'],
                'universe_snapshot': {
                    'source': 'pykrx',
                    'latest_trade_date': '2026-09-14',
                    'membership_basis': 'end_date_snapshot',
                    'point_in_time': True,
                    'market_membership': {
                        'schema_version': 'pykrx_market_membership_v1',
                        'membership_basis': 'end_date_snapshot',
                        'symbol_count': 1,
                        'markets': {
                            'KOSPI': {
                                'symbols': ['099999'],
                                'symbol_count': 1,
                                'membership_sha256': hashlib.sha256(
                                    b'099999'
                                ).hexdigest(),
                            }
                        },
                        'symbol_metadata': [
                            {
                                'symbol': '099999',
                                'market': 'KOSPI',
                                'name_ko': 'Wrong-date identity',
                            }
                        ],
                    },
                },
            },
        }
        exact_date_symbol = PykrxSymbol(
            symbol='099999',
            market='KOSPI',
            name_ko='Exact-date identity',
        )

        with patch.object(
            service,
            'fetch_recent_universe_snapshot',
            return_value=_point_in_time_universe_snapshot(
                [exact_date_symbol],
                market_date='2026-09-11',
            ),
        ) as refetch_snapshot:
            result = service._load_retry_symbols_for_run(
                run,
                markets=('KOSPI',),
                symbol_codes=['099999'],
                point_in_time_recovery=True,
            )

        self.assertEqual(result, [exact_date_symbol])
        refetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20260911',
            allow_master_fallback=False,
        )

    def test_legacy_retry_recovers_delisted_symbol_from_exact_date_membership(
        self,
    ) -> None:
        service = PykrxBackfillService()
        run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'run_metadata': {
                'markets': ['KOSPI'],
                'universe_snapshot': {
                    'source': 'pykrx',
                    'latest_trade_date': '2026-09-11',
                    'membership_basis': 'end_date_snapshot',
                    'point_in_time': True,
                    'market_membership': {
                        'schema_version': 'pykrx_market_membership_v1',
                        'membership_basis': 'end_date_snapshot',
                        'symbol_count': 2,
                        'markets': {
                            'KOSPI': {
                                'symbols': ['005930', '099999'],
                                'symbol_count': 2,
                                'membership_sha256': hashlib.sha256(
                                    b'005930\n099999'
                                ).hexdigest(),
                            }
                        },
                    },
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ),
            patch.object(
                service,
                'load_active_universe',
                return_value=[
                    PykrxSymbol(
                        symbol='005930',
                        market='KOSPI',
                        name_ko='Current active symbol',
                    )
                ],
            ) as load_active_universe,
        ):
            retry_symbols = service._failed_symbol_codes_for_run(
                run,
                markets=('KOSPI',),
            )

        self.assertEqual(retry_symbols, ['099999'])
        load_active_universe.assert_not_called()

    def test_metadata_less_legacy_retry_refetches_exact_date_membership(
        self,
    ) -> None:
        service = PykrxBackfillService()
        persisted_symbol = PykrxSymbol(
            symbol='005930',
            market='KOSPI',
            name_ko='삼성전자',
        )
        delisted_symbol = PykrxSymbol(
            symbol='099999',
            market='KOSPI',
            name_ko='Friday only listing',
        )
        run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'status': 'failed',
            'symbols_total': 2,
            'symbols_succeeded': 1,
            'symbols_failed': 1,
            'run_metadata': {},
        }

        with (
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    [persisted_symbol, delisted_symbol],
                    market_date='2026-09-11',
                ),
            ) as refetch_snapshot,
            patch.object(service, 'load_active_universe') as load_current,
            patch('app.eod.pykrx_backfill.load_symbols') as load_symbols,
            patch.object(service, 'find_symbol') as find_current,
        ):
            retry_codes = service._failed_symbol_codes_for_run(
                run,
                markets=('KOSPI',),
                point_in_time_recovery=True,
            )
            retry_symbols = service._load_retry_symbols_for_run(
                run,
                markets=('KOSPI',),
                symbol_codes=retry_codes,
                point_in_time_recovery=True,
            )

        self.assertEqual(retry_codes, ['099999'])
        self.assertEqual(retry_symbols, [delisted_symbol])
        refetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20260911',
            allow_master_fallback=False,
        )
        load_current.assert_not_called()
        load_symbols.assert_not_called()
        find_current.assert_not_called()

    def test_point_in_time_price_retry_promotes_inherited_daily_snapshot(
        self,
    ) -> None:
        service = PykrxBackfillService()
        inherited_symbol = PykrxSymbol('005930', 'KOSPI', '삼성전자')
        retry_symbol = PykrxSymbol('472220', 'KOSPI', '가격 재시도 종목')
        run = {
            'id': 8,
            'trade_date': '2026-09-11',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 0,
            'symbols_failed': 1,
            'run_metadata': {
                'mode': 'daily_auto',
                'markets': ['KOSPI'],
                'requested_symbols': ['472220'],
                'failed_symbols': ['472220'],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
                'universe_snapshot': _stored_daily_universe_snapshot(
                    [inherited_symbol, retry_symbol],
                    market_date='2026-09-11',
                ),
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=[],
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                side_effect=RuntimeError('provider unavailable'),
            ) as refetch_snapshot,
        ):
            retry_codes = service._failed_symbol_codes_for_run(
                run,
                markets=('KOSPI',),
                point_in_time_recovery=True,
            )
            retry_symbols = service._load_retry_symbols_for_run(
                run,
                markets=('KOSPI',),
                symbol_codes=retry_codes,
                point_in_time_recovery=True,
            )

        self.assertEqual(retry_codes, ['472220'])
        self.assertTrue(retry_codes.universe_snapshot['point_in_time'])
        self.assertTrue(
            retry_codes.universe_snapshot[
                'promoted_from_frozen_daily_snapshot'
            ]
        )
        self.assertEqual(
            [(item.symbol, item.market) for item in retry_symbols],
            [('472220', 'KOSPI')],
        )
        self.assertEqual(
            retry_symbols.universe_snapshot,
            retry_codes.universe_snapshot,
        )
        refetch_snapshot.assert_not_called()

    def test_point_in_time_price_retry_recovers_snapshot_from_retry_lineage(
        self,
    ) -> None:
        service = PykrxBackfillService()
        inherited_symbol = PykrxSymbol('005930', 'KOSPI', '삼성전자')
        retry_symbol = PykrxSymbol('472220', 'KOSPI', '가격 재시도 종목')
        run = {
            'id': 10,
            'trade_date': '2026-09-11',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 0,
            'symbols_failed': 1,
            'run_metadata': {
                'mode': 'daily_auto',
                'markets': ['KOSPI'],
                'requested_symbols': ['472220'],
                'failed_symbols': ['472220'],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
                'retry': {'retry_of_run_id': 9},
            },
        }
        ancestors = {
            9: {
                'id': 9,
                'trade_date': '2026-09-11',
                'trigger': 'auto_close_scheduler',
                'run_metadata': {
                    'mode': 'daily_auto',
                    'markets': ['KOSPI'],
                    'retry': {'retry_of_run_id': 8},
                },
            },
            8: {
                'id': 8,
                'trade_date': '2026-09-11',
                'trigger': 'auto_close_scheduler',
                'run_metadata': {
                    'mode': 'daily_auto',
                    'markets': ['KOSPI'],
                    'universe_snapshot': _stored_daily_universe_snapshot(
                        [inherited_symbol, retry_symbol],
                        market_date='2026-09-11',
                    ),
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=[],
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_automatic_daily_eod_collection_run_by_id',
                side_effect=lambda **kwargs: ancestors.get(
                    kwargs['eod_run_id']
                ),
            ) as load_ancestor,
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                side_effect=RuntimeError('provider unavailable'),
            ) as refetch_snapshot,
        ):
            retry_codes = service._failed_symbol_codes_for_run(
                run,
                markets=('KOSPI',),
                point_in_time_recovery=True,
            )
            retry_symbols = service._load_retry_symbols_for_run(
                run,
                markets=('KOSPI',),
                symbol_codes=retry_codes,
                point_in_time_recovery=True,
            )

        self.assertEqual(
            [item.kwargs['eod_run_id'] for item in load_ancestor.call_args_list],
            [9, 8],
        )
        self.assertEqual(retry_codes, ['472220'])
        self.assertTrue(retry_codes.universe_snapshot['point_in_time'])
        self.assertTrue(
            retry_codes.universe_snapshot[
                'promoted_from_frozen_daily_snapshot'
            ]
        )
        self.assertEqual(
            [(item.symbol, item.market) for item in retry_symbols],
            [('472220', 'KOSPI')],
        )
        self.assertEqual(
            retry_symbols.universe_snapshot,
            retry_codes.universe_snapshot,
        )
        refetch_snapshot.assert_not_called()

    def test_metadata_less_retry_inherits_persisted_exact_membership_scope(
        self,
    ) -> None:
        service = PykrxBackfillService()
        persisted_symbol = PykrxSymbol('005930', 'KOSPI', '삼성전자')
        retry_symbol = PykrxSymbol('099999', 'KOSPI', 'Friday only listing')
        latest_run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'status': 'failed',
            'symbols_total': 2,
            'symbols_succeeded': 1,
            'symbols_failed': 1,
            'run_metadata': {},
        }

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-09-11'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch('app.eod.pykrx_backfill.load_latest_eod_collection_run', return_value=latest_run),
            patch.object(service, 'reconcile_latest_daily_run_status', return_value=latest_run),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=1),
            patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols', return_value=['005930']),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    [persisted_symbol, retry_symbol],
                    market_date='2026-09-11',
                ),
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={'status': 'completed'},
            ) as run_pipeline,
        ):
            service.run_daily_pipeline_if_due(
                market_date='2026-09-11',
                trigger='auto_close_scheduler',
                point_in_time_recovery=True,
            )

        retry_context = run_pipeline.call_args.kwargs['retry_context']
        self.assertEqual(
            retry_context['inherited_downstream_retry_symbols'],
            ['005930'],
        )
        self.assertEqual(
            retry_context['inherited_downstream_retry_resume_from_stage'],
            'pykrx_enrichment',
        )

    def test_metadata_less_price_complete_scope_converges_then_resumes_followups(
        self,
    ) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol('005930', 'KOSPI', '삼성전자'),
            PykrxSymbol('099999', 'KOSPI', 'Friday only listing'),
        ]
        latest_run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'status': 'failed',
            'symbols_total': 2,
            'symbols_succeeded': 0,
            'symbols_failed': 2,
            'run_metadata': {},
        }
        recovered = {
            'triggered': True,
            'status': 'completed',
            'reason': 'incomplete_followups_resumed',
        }

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-09-11'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch('app.eod.pykrx_backfill.load_latest_eod_collection_run', return_value=latest_run),
            patch.object(service, 'reconcile_latest_daily_run_status', return_value=latest_run),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=1),
            patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols', return_value=['005930', '099999']),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    symbols,
                    market_date='2026-09-11',
                ),
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
            patch('app.eod.pykrx_backfill.create_eod_collection_run') as create_attempt,
            patch.object(
                service,
                '_resume_auto_daily_followups_if_due',
                return_value=recovered,
            ) as resume_followups,
            patch.object(service, '_run_market_data_pipeline') as run_pipeline,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-09-11',
                trigger='auto_close_scheduler',
                point_in_time_recovery=True,
            )

        self.assertEqual(result, recovered)
        complete_run.assert_called_once()
        self.assertEqual(complete_run.call_args.args, (7,))
        self.assertEqual(complete_run.call_args.kwargs['status'], 'completed')
        self.assertEqual(complete_run.call_args.kwargs['symbols_succeeded'], 2)
        resume_followups.assert_called_once()
        create_attempt.assert_not_called()
        run_pipeline.assert_not_called()

    def test_metadata_less_retry_rejects_persisted_symbol_outside_exact_membership(
        self,
    ) -> None:
        service = PykrxBackfillService()
        admitted_symbol = PykrxSymbol('005930', 'KOSPI', '삼성전자')
        run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'status': 'failed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {},
        }

        with (
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930', '088888'],
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    [admitted_symbol],
                    market_date='2026-09-11',
                ),
            ),
        ):
            retry_codes = service._failed_symbol_codes_for_run(
                run,
                markets=('KOSPI',),
                point_in_time_recovery=True,
            )

        self.assertEqual(retry_codes, [])
        self.assertTrue(retry_codes.coverage_mismatch)
        self.assertEqual(retry_codes.unexpected_persisted_symbols, ['088888'])

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-09-11'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch('app.eod.pykrx_backfill.load_latest_eod_collection_run', return_value=run),
            patch.object(service, 'reconcile_latest_daily_run_status', return_value=run),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=1),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930', '088888'],
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    [admitted_symbol],
                    market_date='2026-09-11',
                ),
            ),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=8) as create_attempt,
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_attempt,
            patch.object(service, '_run_market_data_pipeline') as run_pipeline,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-09-11',
                trigger='auto_close_scheduler',
                point_in_time_recovery=True,
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['reason'], 'point_in_time_retry_lineage_mismatch')
        create_attempt.assert_called_once()
        complete_attempt.assert_called_once()
        run_pipeline.assert_not_called()

    def test_unresolvable_legacy_point_in_time_retry_records_bounded_failed_attempt(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'status': 'failed',
            'symbols_total': 1,
            'symbols_succeeded': 0,
            'symbols_failed': 1,
            'completed_at': '2026-09-14T00:00:00+00:00',
            'run_metadata': {
                'requested_symbols': ['099999'],
                'failed_symbols': ['099999'],
                'unattempted_symbols': [],
            },
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-09-11',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=latest_run,
            ),
            patch.object(
                service,
                'reconcile_latest_daily_run_status',
                return_value=latest_run,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=1,
            ),
            patch.object(
                service,
                '_failed_symbol_codes_for_run',
                return_value=['099999'],
            ),
            patch.object(
                service,
                '_load_retry_symbols_for_run',
                return_value=[],
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=8,
            ) as create_attempt,
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_attempt,
            patch.object(service, '_run_market_data_pipeline') as run_pipeline,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-09-11',
                trigger='auto_close_scheduler',
                point_in_time_recovery=True,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(
            result['reason'],
            'point_in_time_retry_membership_unavailable',
        )
        create_attempt.assert_called_once()
        self.assertEqual(
            create_attempt.call_args.kwargs['run_metadata']['auto_retry'][
                'attempt'
            ],
            2,
        )
        complete_attempt.assert_called_once()
        run_pipeline.assert_not_called()

    def test_refetched_retry_membership_proof_survives_second_retry_hop(
        self,
    ) -> None:
        service = PykrxBackfillService()
        persisted_symbol = PykrxSymbol(
            symbol='005930',
            market='KOSPI',
            name_ko='삼성전자',
        )
        failed_symbol = PykrxSymbol(
            symbol='099999',
            market='KOSPI',
            name_ko='Friday only listing',
            standard_code='KR7099990001',
        )
        latest_run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'status': 'failed',
            'symbols_total': 2,
            'symbols_succeeded': 1,
            'symbols_failed': 1,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': ['005930', '099999'],
                'failed_symbols': ['099999'],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
                'universe_snapshot': {
                    'market_membership': {
                        'schema_version': 'pykrx_market_membership_v1',
                        'membership_basis': 'end_date_snapshot',
                        'symbol_count': 1,
                        'markets': {
                            'KOSPI': {
                                'symbols': ['099999'],
                                'symbol_count': 1,
                                'membership_sha256': 'stale-invalid-proof',
                            }
                        },
                    }
                },
            },
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-09-11',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=latest_run,
            ),
            patch.object(
                service,
                'reconcile_latest_daily_run_status',
                return_value=latest_run,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=1,
            ),
            patch.object(
                service,
                '_failed_symbol_codes_for_run',
                return_value=['099999'],
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    [persisted_symbol, failed_symbol],
                    market_date='2026-09-11',
                ),
            ) as refetch_snapshot,
            patch.object(
                service,
                '_resume_auto_daily_followups_if_due',
                return_value=None,
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={'status': 'completed'},
            ) as run_pipeline,
        ):
            service.run_daily_pipeline_if_due(
                market_date='2026-09-11',
                trigger='auto_close_scheduler',
                point_in_time_recovery=True,
            )

        refetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20260911',
            allow_master_fallback=False,
        )
        retry_snapshot = run_pipeline.call_args.kwargs['retry_context'][
            'point_in_time_universe_snapshot'
        ]
        validated = validate_market_membership_snapshot(
            retry_snapshot['market_membership'],
            requested_symbols=['005930', '099999'],
            markets=['KOSPI'],
        )
        self.assertEqual(
            validated,
            {'KOSPI': ['005930', '099999']},
        )

        second_retry_run = {
            'id': 8,
            'trade_date': '2026-09-11',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': ['099999'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
                'universe_snapshot': retry_snapshot,
            },
        }
        with patch.object(
            service,
            'fetch_recent_universe_snapshot',
        ) as second_refetch:
            rehydrated = service._load_retry_symbols_for_run(
                second_retry_run,
                markets=('KOSPI', 'KOSDAQ'),
                symbol_codes=['099999'],
                point_in_time_recovery=True,
            )

        self.assertEqual(rehydrated, [failed_symbol])
        second_refetch.assert_not_called()

        pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'run_metadata': {
                'resolved_stages': ['technical_snapshots'],
                'eod_collection_run_id': 8,
            },
            'stages': {'technical_snapshots': {'status': 'failed'}},
        }
        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
            ) as followup_refetch,
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 92,
                    'status': 'completed',
                },
            ),
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={'updated': True, 'status': 'completed'},
            ),
        ):
            service._resume_auto_daily_followups_if_due(
                market_date='2026-09-11',
                latest_available_trade_date='2026-09-11',
                latest_eod_run=second_retry_run,
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=True,
            )

        followup_refetch.assert_not_called()
        recovery_call = resume_stages.call_args.kwargs
        self.assertEqual(recovery_call['markets'], ('KOSPI',))
        self.assertEqual(recovery_call['symbols'], ['005930'])

    def test_point_in_time_followup_keeps_inherited_scope_after_price_retry_succeeds(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 8,
            'trade_date': '2026-09-11',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI', 'KOSDAQ'],
                'requested_symbols': ['472220'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930', '000660'],
                'universe_snapshot': {
                    'source': 'pykrx',
                    'latest_trade_date': '2026-09-11',
                    'membership_basis': 'end_date_snapshot',
                    'point_in_time': True,
                    'market_membership': {
                        'schema_version': 'pykrx_market_membership_v1',
                        'membership_basis': 'end_date_snapshot',
                        'symbol_count': 3,
                        'markets': {
                            'KOSPI': {
                                'symbols': ['000660', '005930', '472220'],
                                'symbol_count': 3,
                                'membership_sha256': hashlib.sha256(
                                    b'000660\n005930\n472220'
                                ).hexdigest(),
                            },
                            'KOSDAQ': {
                                'symbols': [],
                                'symbol_count': 0,
                                'membership_sha256': hashlib.sha256(
                                    b''
                                ).hexdigest(),
                            },
                        },
                    }
                },
            },
        }
        pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'run_metadata': {
                'resolved_stages': ['technical_snapshots'],
                'eod_collection_run_id': 8,
            },
            'stages': {'technical_snapshots': {'status': 'failed'}},
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 92,
                    'status': 'completed',
                },
            ),
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={'updated': True, 'status': 'completed'},
            ),
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-09-11',
                latest_available_trade_date='2026-09-11',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=True,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            resume_stages.call_args.kwargs['symbols'],
            ['005930', '000660'],
        )

    def test_finalized_nonblocking_partial_suffix_reconciles_without_replay(
        self,
    ) -> None:
        service = PykrxBackfillService()
        marker = {
            'schema_version': 'nonblocking_partial_progression_v1',
            'proof_digest': 'validated-by-shared-contract',
            'retry_debt': {
                'schema_version': 'nonblocking_feedback_retry_debt_v1',
                'items': [
                    {
                        'stage': 'agent_feedback_summaries',
                        'role': 'technical-summarizer',
                        'symbol': '028050',
                    }
                ],
            },
        }
        resolved_stages = [
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        stage_rows = {
            'signal_decider': {'status': 'partial'},
            'next_open_execution_plan': {'status': 'completed'},
            'signal_decider_evaluations': {'status': 'completed'},
        }
        pipeline_run = {
            'id': 596,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'partial',
            'market_date': '2026-10-28',
            'current_stage': 'signal_decider_evaluations',
            'completed_at': '2026-10-28T23:15:00+00:00',
            'run_metadata': {
                'resolved_stages': resolved_stages,
                'reused_eod_collection_run_id': 582,
                'resumable': False,
                'nonblocking_partial_progression': marker,
            },
            'stages': stage_rows,
        }
        latest_eod_run = {
            'id': 582,
            'trade_date': '2026-10-28',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'followup_recovery_required': True,
                'downstream_retry_symbols': ['005930'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'validated_nonblocking_partial_progression_marker',
                return_value=marker,
            ) as validate_marker,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run'
            ) as converge_pipeline,
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run'
            ) as replay_stages,
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={'updated': True, 'status': 'partial'},
            ) as persist_recovery,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-10-28',
                latest_available_trade_date='2026-10-28',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
            )

        self.assertEqual(result['reason'], 'followup_recovery_state_converged')
        validate_marker.assert_called_once_with(
            {
                'status': 'partial',
                'pipeline_run_id': 596,
                'pipeline_run': pipeline_run,
                'market_date': '2026-10-28',
                'resolved_stages': resolved_stages,
                'stages': stage_rows,
            },
            expected_eod_collection_run_id=582,
        )
        converge_pipeline.assert_not_called()
        replay_stages.assert_not_called()
        persist_recovery.assert_called_once()
        self.assertEqual(
            persist_recovery.call_args.kwargs['recovery_result']['stages'],
            stage_rows,
        )

    def test_point_in_time_followup_recovers_membership_from_retry_lineage(
        self,
    ) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol('005930', 'KOSPI', '삼성전자'),
            PykrxSymbol('000660', 'KOSPI', 'SK하이닉스'),
            PykrxSymbol('472220', 'KOSPI', '가격 재시도 종목'),
        ]
        latest_eod_run = {
            'id': 10,
            'trade_date': '2026-09-11',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'mode': 'daily_auto',
                'markets': ['KOSPI'],
                'requested_symbols': ['472220'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930', '000660'],
                'retry': {'retry_of_run_id': 9},
            },
        }
        ancestors = {
            9: {
                'id': 9,
                'trade_date': '2026-09-11',
                'trigger': 'auto_close_scheduler',
                'run_metadata': {
                    'mode': 'daily_auto',
                    'markets': ['KOSPI'],
                    'retry': {'retry_of_run_id': 8},
                },
            },
            8: {
                'id': 8,
                'trade_date': '2026-09-11',
                'trigger': 'auto_close_scheduler',
                'run_metadata': {
                    'mode': 'daily_auto',
                    'markets': ['KOSPI'],
                    'universe_snapshot': _stored_daily_universe_snapshot(
                        symbols,
                        market_date='2026-09-11',
                    ),
                },
            },
        }
        pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'run_metadata': {
                'resolved_stages': ['technical_snapshots'],
                'eod_collection_run_id': 10,
            },
            'stages': {'technical_snapshots': {'status': 'failed'}},
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_automatic_daily_eod_collection_run_by_id',
                side_effect=lambda **kwargs: ancestors.get(
                    kwargs['eod_run_id']
                ),
            ) as load_ancestor,
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                side_effect=RuntimeError('provider unavailable'),
            ) as refetch_snapshot,
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 92,
                    'status': 'completed',
                },
            ),
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={'updated': True, 'status': 'completed'},
            ),
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-09-11',
                latest_available_trade_date='2026-09-11',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI',),
                point_in_time_recovery=True,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            [item.kwargs['eod_run_id'] for item in load_ancestor.call_args_list],
            [9, 8],
        )
        refetch_snapshot.assert_not_called()
        self.assertEqual(
            resume_stages.call_args.kwargs['symbols'],
            ['005930', '000660'],
        )
        self.assertTrue(
            resume_stages.call_args.kwargs['point_in_time_recovery']
        )

    def test_retry_lineage_snapshot_cannot_omit_latest_admitted_scope(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_metadata = {
            'markets': ['KOSPI'],
            'requested_symbols': ['472220'],
            'downstream_retry_symbols': ['005930'],
            'retry': {'retry_of_run_id': 8},
        }
        ancestor = {
            'id': 8,
            'trade_date': '2026-09-11',
            'trigger': 'auto_close_scheduler',
            'run_metadata': {
                'mode': 'daily_auto',
                'markets': ['KOSPI'],
                'universe_snapshot': _stored_daily_universe_snapshot(
                    [PykrxSymbol('005930', 'KOSPI', '삼성전자')],
                    market_date='2026-09-11',
                ),
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.'
                'load_automatic_daily_eod_collection_run_by_id',
                return_value=ancestor,
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                side_effect=RuntimeError('provider unavailable'),
            ) as refetch_snapshot,
        ):
            membership, markets = (
                service._resolve_point_in_time_followup_membership(
                    market_date='2026-09-11',
                    metadata=latest_metadata,
                    markets=('KOSPI',),
                    eod_run_id=9,
                )
            )

        self.assertIsNone(membership)
        self.assertEqual(markets, ('KOSPI',))
        refetch_snapshot.assert_called_once()

    def test_current_orphan_keeps_verified_inherited_scope_after_price_retry(
        self,
    ) -> None:
        service = PykrxBackfillService()
        inherited_symbol = PykrxSymbol(
            symbol='005930',
            market='KOSPI',
            name_ko='삼성전자',
        )
        retried_symbol = PykrxSymbol(
            symbol='099999',
            market='KOSPI',
            name_ko='Retry success',
        )
        downstream_scope = ['005930', '099999']
        latest_eod_run = {
            'id': 8,
            'trade_date': '2026-09-14',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': ['099999'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': downstream_scope,
                'downstream_retry_resume_from_stage': (
                    'pykrx_enrichment'
                ),
                'expected_daily_pipeline_stages': list(
                    AUTO_DAILY_TRACKED_STAGE_ORDER
                ),
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['099999'],
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'list_symbols_with_daily_bars_in_range',
                return_value=downstream_scope,
            ) as load_exact_price_symbols,
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    [inherited_symbol, retried_symbol],
                    market_date='2026-09-14',
                ),
            ) as fetch_snapshot,
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 92,
                    'status': 'completed',
                },
            ),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-09-14',
                latest_available_trade_date='2026-09-14',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI',),
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            resume_stages.call_args.kwargs['symbols'],
            downstream_scope,
        )
        self.assertEqual(
            result['followup_recovery']['symbols'],
            downstream_scope,
        )
        load_exact_price_symbols.assert_called_once_with(
            start_date='2026-09-14',
            end_date='2026-09-14',
        )
        fetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20260914',
            allow_master_fallback=False,
        )
        completion_metadata = complete_eod_run.call_args.kwargs[
            'run_metadata'
        ]
        self.assertEqual(completion_metadata['downstream_retry_symbols'], [])
        self.assertEqual(
            completion_metadata['downstream_recovery_symbols_count'],
            2,
        )

    def test_point_in_time_followup_refetches_exact_scope_using_source_markets(
        self,
    ) -> None:
        service = PykrxBackfillService()
        historical_symbols = [
            PykrxSymbol(
                symbol='005930',
                market='KOSPI',
                name_ko='삼성전자',
            ),
            PykrxSymbol(
                symbol='099999',
                market='KOSPI',
                name_ko='Friday only listing',
            ),
        ]
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': ['005930', '099999'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['099999'],
                'universe_snapshot': {'market_membership': {}},
            },
        }
        pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'run_metadata': {
                'resolved_stages': ['technical_snapshots'],
                'eod_collection_run_id': 7,
            },
            'stages': {'technical_snapshots': {'status': 'failed'}},
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    historical_symbols,
                    market_date='2026-09-11',
                ),
            ) as refetch_snapshot,
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 92,
                    'status': 'completed',
                },
            ),
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={'updated': True, 'status': 'completed'},
            ),
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-09-11',
                latest_available_trade_date='2026-09-11',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=True,
            )

        self.assertEqual(result['status'], 'completed')
        refetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20260911',
            allow_master_fallback=False,
        )
        recovery_call = resume_stages.call_args.kwargs
        self.assertEqual(recovery_call['markets'], ('KOSPI',))
        self.assertEqual(recovery_call['symbols'], ['099999'])
        self.assertTrue(recovery_call['point_in_time_recovery'])

    def test_invalid_point_in_time_followup_scope_records_one_durable_attempt(
        self,
    ) -> None:
        service = PykrxBackfillService()
        persisted_recovery_runs: list[dict] = []
        completed_at = datetime.now(timezone.utc).isoformat()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-09-11',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': ['005930', '099999'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930', '099999'],
                'expected_daily_pipeline_stages': ['technical_snapshots'],
                'stage_progress': {
                    'technical_snapshots': {'status': 'failed'}
                },
                # The digest is internally valid, but this proof omits one
                # admitted symbol and therefore cannot authorize a subset run.
                'universe_snapshot': {
                    'source': 'pykrx',
                    'latest_trade_date': '2026-09-11',
                    'membership_basis': 'end_date_snapshot',
                    'point_in_time': True,
                    'market_membership': {
                        'schema_version': 'pykrx_market_membership_v1',
                        'membership_basis': 'end_date_snapshot',
                        'symbol_count': 1,
                        'markets': {
                            'KOSPI': {
                                'symbols': ['005930'],
                                'symbol_count': 1,
                                'membership_sha256': hashlib.sha256(
                                    b'005930'
                                ).hexdigest(),
                            }
                        },
                    },
                },
            },
        }

        def get_latest_recovery(**_kwargs):
            return persisted_recovery_runs[-1] if persisted_recovery_runs else None

        def finish_failed_recovery(**kwargs):
            persisted = {
                'id': kwargs['pipeline_run_id'],
                'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
                'status': kwargs['status'],
                'completed_at': completed_at,
                'run_metadata': {
                    'resolved_stages': ['technical_snapshots'],
                    'reused_eod_collection_run_id': 7,
                },
                'stages': {},
            }
            persisted_recovery_runs.append(persisted)
            return persisted

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                side_effect=get_latest_recovery,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                side_effect=lambda **_kwargs: len(persisted_recovery_runs),
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'start_run',
                return_value={'id': 92},
            ) as start_recovery,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'finish_run',
                side_effect=finish_failed_recovery,
            ) as finish_recovery,
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                side_effect=RuntimeError('provider unavailable'),
            ) as refetch_snapshot,
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run'
            ) as resume_stages,
        ):
            first = service._resume_auto_daily_followups_if_due(
                market_date='2026-09-11',
                latest_available_trade_date='2026-09-11',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=True,
            )
            second = service._resume_auto_daily_followups_if_due(
                market_date='2026-09-11',
                latest_available_trade_date='2026-09-11',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=True,
            )

        self.assertTrue(first['triggered'])
        self.assertEqual(first['status'], 'failed')
        self.assertEqual(
            first['reason'],
            'point_in_time_recovery_scope_unavailable',
        )
        self.assertEqual(first['followup_recovery']['attempt'], 1)
        self.assertEqual(second['reason'], 'followup_recovery_backoff')
        start_recovery.assert_called_once()
        start_metadata = start_recovery.call_args.kwargs['run_metadata']
        self.assertEqual(start_metadata['reused_eod_collection_run_id'], 7)
        self.assertTrue(start_metadata['point_in_time_recovery'])
        finish_recovery.assert_called_once()
        self.assertEqual(finish_recovery.call_args.kwargs['status'], 'failed')
        refetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20260911',
            allow_master_fallback=False,
        )
        resume_stages.assert_not_called()

    def test_refresh_recent_universe_preserves_snapshot_and_persist_counts(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='삼성전자')],
            'latest_trade_date': '2026-04-07',
            'universe_source': 'pykrx_market_ohlcv',
            'membership_basis': 'end_date_snapshot',
            'membership_scope_warning': 'end-date snapshot only',
            'complete': True,
            'market_status': {'KOSPI': 'pykrx_market_ohlcv'},
            'unavailable_markets': [],
            'market_errors': [],
        }

        with (
            patch.object(service, 'fetch_recent_universe_snapshot', return_value=snapshot) as fetch_snapshot,
            patch.object(service, '_persist_universe', return_value=1) as persist_universe,
        ):
            result = service.refresh_recent_universe(markets=('KOSPI',), deactivate_missing=False)

        self.assertEqual(
            result,
            {
                'source': 'pykrx',
                'markets': ['KOSPI'],
                'symbol_count': 1,
                'persisted_count': 1,
                'latest_trade_date': '2026-04-07',
                'universe_source': 'pykrx_market_ohlcv',
                'membership_basis': 'end_date_snapshot',
                'membership_scope_warning': 'end-date snapshot only',
                'complete': True,
                'market_status': {'KOSPI': 'pykrx_market_ohlcv'},
                'unavailable_markets': [],
                'market_errors': [],
                'deactivate_missing': False,
            },
        )
        fetch_snapshot.assert_called_once_with(markets=('KOSPI',), limit=None)
        persist_universe.assert_called_once_with(snapshot['symbols'], deactivate_missing=False)

    def test_load_active_universe_filters_inactive_symbols_at_query_boundary(self) -> None:
        service = PykrxBackfillService()
        with patch('app.eod.pykrx_backfill.load_symbols', return_value=[]) as load_symbol_rows:
            result = service.load_active_universe(markets=('KOSPI',), limit=25)

        self.assertEqual(result, [])
        load_symbol_rows.assert_called_once_with(
            is_active=True,
            markets=['KOSPI'],
            limit=25,
        )

    def test_refresh_recent_universe_rejects_incomplete_snapshot_without_persisting(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='삼성전자')],
            'latest_trade_date': '2026-04-07',
            'universe_source': 'pykrx_market_ohlcv',
            'complete': False,
            'market_status': {
                'KOSPI': 'pykrx_market_ohlcv',
                'KOSDAQ': 'unavailable',
            },
            'unavailable_markets': ['KOSDAQ'],
            'market_errors': [
                {
                    'market': 'KOSDAQ',
                    'status': 'fetch_failed',
                    'error_type': 'ConnectionError',
                    'error_message': 'source unavailable',
                }
            ],
        }

        with (
            patch.object(service, 'fetch_recent_universe_snapshot', return_value=snapshot),
            patch.object(service, '_persist_universe') as persist_universe,
            patch('app.eod.pykrx_backfill._log_pykrx_fetch') as log_fetch,
        ):
            with self.assertRaisesRegex(RuntimeError, 'incomplete.*KOSDAQ'):
                service.refresh_recent_universe(markets=('KOSPI', 'KOSDAQ'))

        persist_universe.assert_not_called()
        failure_log = log_fetch.call_args_list[-1]
        self.assertEqual(failure_log.args, ('refresh_recent_universe', 'failed'))
        self.assertFalse(failure_log.kwargs['complete'])
        self.assertEqual(failure_log.kwargs['unavailable_markets'], ['KOSDAQ'])
        self.assertEqual(failure_log.kwargs['market_errors'], snapshot['market_errors'])

    def test_run_daily_pipeline_skips_completed_trade_date(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
            ),
            patch.object(service, '_resume_auto_daily_followups_if_due', return_value=None),
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'already_completed')
        run_collection.assert_not_called()

    def test_completed_collection_resumes_only_incomplete_followups(self) -> None:
        service = PykrxBackfillService()
        failed_stage = 'external_news_collect'
        failed_index = AUTO_DAILY_FOLLOWUP_STAGE_ORDER.index(failed_stage)
        stage_rows = {
            stage_id: {
                'status': (
                    'completed'
                    if index < failed_index
                    else 'failed'
                    if index == failed_index
                    else 'blocked'
                )
            }
            for index, stage_id in enumerate(AUTO_DAILY_FOLLOWUP_STAGE_ORDER)
        }
        safe_skipped_stage = AUTO_DAILY_FOLLOWUP_STAGE_ORDER[0]
        self.assertLess(
            AUTO_DAILY_FOLLOWUP_STAGE_ORDER.index(safe_skipped_stage),
            failed_index,
        )
        stage_rows[safe_skipped_stage] = {
            'status': 'skipped',
            'details': {
                'result_summary': {'reason': 'already_completed'},
            },
        }
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_FOLLOWUP_STAGE_ORDER),
                'eod_collection_run_id': 7,
            },
            'stages': stage_rows,
        }
        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_followups,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 92,
                    'status': 'completed',
                },
            ),
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={'updated': True, 'status': 'completed'},
            ),
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(result['followup_recovery']['resume_from_stage'], failed_stage)
        expected_stages = list(AUTO_DAILY_FOLLOWUP_STAGE_ORDER[failed_index:])
        self.assertEqual(result['followup_recovery']['stages'], expected_stages)
        self.assertNotIn('price_collection', expected_stages)
        resume_followups.assert_called_once_with(
            market_date='2026-04-03',
            stages=expected_stages,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            force=True,
            reuse_existing_feedback=False,
            resume_from_stage=failed_stage,
            trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            reused_eod_collection_run_id=7,
            provider_admission_fence_owned=True,
        )
        run_collection.assert_not_called()

    def test_completed_run_replays_feedback_when_prerequisite_finished_late(
        self,
    ) -> None:
        service = PykrxBackfillService()
        prerequisite_finished = '2026-04-03T08:30:00+00:00'
        stage_ids = [
            'pykrx_enrichment',
            'market_risk_filter',
            *AUTO_DAILY_FOLLOWUP_STAGE_ORDER,
        ]
        stage_rows = {
            stage_id: {
                'status': 'completed',
                'started_at': '2026-04-03T07:40:00+00:00',
                'completed_at': '2026-04-03T07:50:00+00:00',
                'updated_at': '2026-04-03T07:50:00+00:00',
            }
            for stage_id in stage_ids
        }
        stage_rows['composite_factors']['completed_at'] = (
            prerequisite_finished
        )
        stage_rows['composite_factors']['updated_at'] = prerequisite_finished
        stage_rows['signal_features'].update(
            {
                'started_at': '2026-04-03T08:31:00+00:00',
                'completed_at': '2026-04-03T08:35:00+00:00',
                'updated_at': '2026-04-03T08:35:00+00:00',
            }
        )
        stage_rows['agent_feedback_summaries'].update(
            {
                'started_at': '2026-04-03T08:00:00+00:00',
                'completed_at': '2026-04-03T08:10:00+00:00',
                'updated_at': '2026-04-03T08:10:00+00:00',
            }
        )
        stage_rows['signal_decider'].update(
            {
                'started_at': '2026-04-03T08:11:00+00:00',
                'completed_at': '2026-04-03T08:15:00+00:00',
                'updated_at': '2026-04-03T08:15:00+00:00',
            }
        )
        pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'updated_at': '2026-04-03T08:30:00+00:00',
            'run_metadata': {
                'resolved_stages': stage_ids,
                'eod_collection_run_id': 7,
            },
            'stages': stage_rows,
        }
        latest_eod_run = {
            'id': 7,
            'status': 'completed',
            'symbols_failed': 0,
            'run_metadata': {'markets': ['KOSPI']},
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 92,
                    'status': 'completed',
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'triggered': True,
                    'status': 'completed',
                    'pipeline_run_id': 92,
                },
            ) as resume_followups,
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={'updated': True, 'status': 'completed'},
            ),
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI',),
            )

        recovery_call = resume_followups.call_args.kwargs
        self.assertEqual(
            recovery_call['resume_from_stage'],
            'agent_feedback_summaries',
        )
        self.assertEqual(
            recovery_call['stages'],
            [
                'agent_feedback_summaries',
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )
        self.assertTrue(recovery_call['force'])
        self.assertFalse(recovery_call['reuse_existing_feedback'])
        self.assertEqual(
            result['followup_recovery']['ordering_repair'],
            'feedback_prerequisite_completed_after_feedback_started',
        )

    def test_completed_run_with_canonical_order_only_converges_state(
        self,
    ) -> None:
        service = PykrxBackfillService()
        stage_ids = [
            'pykrx_enrichment',
            'market_risk_filter',
            *AUTO_DAILY_FOLLOWUP_STAGE_ORDER,
        ]
        stage_rows = {
            stage_id: {
                'status': 'completed',
                'started_at': '2026-04-03T07:40:00+00:00',
                'completed_at': '2026-04-03T07:50:00+00:00',
                'updated_at': '2026-04-03T07:50:00+00:00',
            }
            for stage_id in stage_ids
        }
        stage_rows['signal_features'].update(
            {
                'started_at': '2026-04-03T07:51:00+00:00',
                'completed_at': '2026-04-03T07:55:00+00:00',
                'updated_at': '2026-04-03T07:55:00+00:00',
            }
        )
        stage_rows['agent_feedback_summaries'].update(
            {
                'started_at': '2026-04-03T08:00:00+00:00',
                'completed_at': '2026-04-03T08:10:00+00:00',
                'updated_at': '2026-04-03T08:10:00+00:00',
            }
        )
        stage_rows['signal_decider'].update(
            {
                'started_at': '2026-04-03T08:11:00+00:00',
                'completed_at': '2026-04-03T08:15:00+00:00',
                'updated_at': '2026-04-03T08:15:00+00:00',
            }
        )
        pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'updated_at': '2026-04-03T08:15:00+00:00',
            'run_metadata': {
                'resolved_stages': stage_ids,
                'eod_collection_run_id': 7,
            },
            'stages': stage_rows,
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 91,
                    'status': 'completed',
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run'
            ) as resume_followups,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                    'run_metadata': {'markets': ['KOSPI']},
                },
                markets=('KOSPI',),
            )

        self.assertEqual(result['reason'], 'daily_pipeline_state_converged')
        resume_followups.assert_not_called()

    def test_followup_recovery_includes_piotroski_before_factors_and_signals(
        self,
    ) -> None:
        service = PykrxBackfillService()
        resolved_stages = [
            'fundamentals_daily_refresh',
            'piotroski_filter',
            'momentum_factors',
            'signal_features',
        ]
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': resolved_stages,
                'eod_collection_run_id': 7,
            },
            'stages': {
                stage_id: {
                    'status': 'failed' if index == 0 else 'blocked'
                }
                for index, stage_id in enumerate(resolved_stages)
            },
        }
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'failed_symbols': [],
                'unattempted_symbols': [],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_followups,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        recovery_stages = result['followup_recovery']['stages']
        self.assertEqual(recovery_stages, resolved_stages)
        self.assertEqual(
            resume_followups.call_args.kwargs['stages'],
            resolved_stages,
        )
        self.assertLess(
            recovery_stages.index('piotroski_filter'),
            recovery_stages.index('momentum_factors'),
        )
        self.assertLess(
            recovery_stages.index('piotroski_filter'),
            recovery_stages.index('signal_features'),
        )

    def test_downstream_only_failure_resumes_without_downloading_prices_again(self) -> None:
        service = PykrxBackfillService()
        failed_stage = 'technical_snapshots'
        failed_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(failed_stage)
        stage_rows = {
            stage_id: {
                'status': (
                    'completed'
                    if index < failed_index
                    else 'failed'
                    if index == failed_index
                    else 'completed'
                )
            }
            for index, stage_id in enumerate(AUTO_DAILY_TRACKED_STAGE_ORDER)
        }
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
                'eod_collection_run_id': 7,
            },
            'stages': stage_rows,
        }
        accepted_partial = {
            'status': 'partial',
            'pipeline_run_id': 92,
            'stages': {
                'momentum_factors': {
                    'status': 'partial',
                    'succeeded': 99,
                    'failed': 1,
                    'total': 100,
                    'details': {
                        'result_summary': {
                            'remaining_retryable_events': 0,
                            'results_persisted': 99,
                            'results_failed': 1,
                            **build_exact_symbol_scope_outcome(
                                manifest_version=(
                                    'factor_scope_outcomes_v1'
                                ),
                                selected_symbols=[
                                    f'{index:06d}'
                                    for index in range(1, 101)
                                ],
                                successful_symbols=[
                                    f'{index:06d}'
                                    for index in range(1, 100)
                                ],
                                retry_debt_symbols=['000100'],
                            ),
                        }
                    },
                }
            },
        }
        persistence_order: list[str] = []

        def finalize_parent(**_kwargs) -> dict:
            persistence_order.append('pipeline_parent')
            return {
                'updated': True,
                'pipeline_run_id': 92,
                'status': 'partial',
            }

        def complete_eod(*_args, **_kwargs) -> None:
            persistence_order.append('eod_scope')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'trade_date': '2026-04-03',
                    'status': 'completed_with_errors',
                    'symbols_total': 1,
                    'symbols_succeeded': 1,
                    'symbols_failed': 0,
                    'error_message': 'restart interrupted followups',
                    'run_metadata': {
                        'failed_symbols': [],
                        'unattempted_symbols': [],
                        'downstream_retry_symbols': ['005930'],
                    },
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value=accepted_partial,
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                side_effect=finalize_parent,
            ) as converge_parent,
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run',
                side_effect=complete_eod,
            ) as complete_eod_run,
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ) as count_persisted_symbols,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        recovery_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(
            'pykrx_enrichment'
        )
        expected_stages = list(AUTO_DAILY_TRACKED_STAGE_ORDER[recovery_index:])
        self.assertTrue(result['triggered'])
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(
            result['followup_recovery']['resume_from_stage'],
            'pykrx_enrichment',
        )
        self.assertEqual(result['followup_recovery']['stages'], expected_stages)
        self.assertEqual(result['followup_recovery']['symbols'], [])
        self.assertNotIn('price_collection', expected_stages)
        resume_stages.assert_called_once_with(
            market_date='2026-04-03',
            stages=expected_stages,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            force=True,
            reuse_existing_feedback=False,
            resume_from_stage='pykrx_enrichment',
            trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            reused_eod_collection_run_id=7,
            provider_admission_fence_owned=True,
        )
        run_collection.assert_not_called()
        converge_parent.assert_called_once_with(
            pipeline_run_id=92,
            allow_satisfied_terminal=True,
        )
        count_persisted_symbols.assert_called_once_with(
            7,
            trade_date='2026-04-03',
        )
        complete_eod_run.assert_called_once()
        completion = complete_eod_run.call_args
        self.assertEqual(completion.args, (7,))
        self.assertEqual(completion.kwargs['status'], 'completed')
        self.assertEqual(completion.kwargs['symbols_succeeded'], 1)
        self.assertEqual(completion.kwargs['symbols_failed'], 0)
        self.assertIsNone(completion.kwargs['error_message'])
        self.assertEqual(
            completion.kwargs['run_metadata']['downstream_retry_symbols'],
            [],
        )
        self.assertEqual(
            persistence_order,
            ['pipeline_parent', 'eod_scope'],
        )

    def test_no_parent_downstream_scope_recovers_in_admitted_price_scope(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_stages,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(
            result['followup_recovery']['resume_from_stage'],
            'pykrx_enrichment',
        )
        self.assertEqual(result['followup_recovery']['symbols'], ['005930'])
        recovery_call = resume_stages.call_args.kwargs
        self.assertNotIn('price_collection', recovery_call['stages'])
        self.assertIn('momentum_factors', recovery_call['stages'])
        self.assertEqual(recovery_call['symbols'], ['005930'])
        self.assertEqual(recovery_call['reused_eod_collection_run_id'], 7)

    def test_no_parent_factor_recovery_defers_until_price_retry_completes(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            'symbols_total': 2,
            'symbols_succeeded': 1,
            'symbols_failed': 1,
            'run_metadata': {
                'requested_symbols': ['005930', '472220'],
                'failed_symbols': ['472220'],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ) as count_recovery_runs,
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_stages,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertIsNone(result)
        count_recovery_runs.assert_not_called()
        resume_stages.assert_not_called()

    def test_failed_followup_recovers_when_parent_and_symbol_scope_are_missing(
        self,
    ) -> None:
        service = PykrxBackfillService()
        failed_stage = 'signal_decider'
        failed_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(failed_stage)
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930', '000660'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': [],
                'stage_progress': {
                    stage_id: {
                        'status': (
                            'completed'
                            if index < failed_index
                            else 'failed'
                            if index == failed_index
                            else 'blocked'
                        )
                    }
                    for index, stage_id in enumerate(
                        AUTO_DAILY_TRACKED_STAGE_ORDER
                    )
                },
            },
        }
        latest_eod_run['run_metadata']['stage_progress'][
            'agent_feedback_summaries'
        ] = {
            'status': 'completed',
            'source_run_ids': {
                'eod_run_id': 7,
                'agent_feedback_run_id': 'feedback-cycle-summary',
            },
            'result_summary': {
                'status': 'completed',
                'run_id': 'feedback-cycle-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': None,
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=2,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930', '000660'],
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_stages,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertIsNotNone(result)
        self.assertEqual(
            result['followup_recovery']['resume_from_stage'],
            failed_stage,
        )
        self.assertEqual(
            result['followup_recovery']['symbols'],
            ['005930', '000660'],
        )
        recovery_call = resume_stages.call_args.kwargs
        self.assertEqual(
            recovery_call['stages'],
            list(AUTO_DAILY_TRACKED_STAGE_ORDER[failed_index:]),
        )
        self.assertEqual(
            recovery_call['symbols'],
            ['005930', '000660'],
        )
        self.assertEqual(
            recovery_call['summary_cutoff_proof'],
            {
                'source_run_id': 'feedback-cycle-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': None,
            },
        )

    def test_recovery_resumes_from_unsafe_skipped_stage(self) -> None:
        service = PykrxBackfillService()
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'blocked',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': [
                    'fundamentals_daily_refresh',
                    'momentum_factors',
                ],
                'eod_collection_run_id': 7,
            },
            'stages': {
                'fundamentals_daily_refresh': {
                    'status': 'skipped',
                    'details': {
                        'result_summary': {
                            'reason': 'dart_api_key_not_configured',
                        },
                    },
                },
                'momentum_factors': {'status': 'blocked'},
            },
        }
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': [],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_stages,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertEqual(
            result['followup_recovery']['resume_from_stage'],
            'fundamentals_daily_refresh',
        )
        self.assertEqual(
            resume_stages.call_args.kwargs['stages'],
            ['fundamentals_daily_refresh', 'momentum_factors'],
        )

    def test_orphan_followup_recovery_audits_prices_without_downstream_scope(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930', '000660'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': [],
                'expected_daily_pipeline_stages': list(
                    AUTO_DAILY_TRACKED_STAGE_ORDER
                ),
                'stage_progress': {
                    stage_id: {'status': 'completed'}
                    for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=2,
            ) as count_persisted_symbols,
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930', '035420'],
            ) as load_persisted_symbols,
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ) as update_eod_run,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertEqual(
            result['reason'],
            'followup_recovery_price_scope_inconsistent',
        )
        resume_stages.assert_not_called()
        count_persisted_symbols.assert_called_once_with(
            7,
            trade_date='2026-04-03',
        )
        load_persisted_symbols.assert_called_once_with(
            7,
            trade_date='2026-04-03',
        )
        complete_eod_run.assert_called_once()
        completion = complete_eod_run.call_args
        self.assertEqual(completion.kwargs['status'], 'completed_with_errors')
        self.assertEqual(completion.kwargs['symbols_succeeded'], 1)
        self.assertEqual(completion.kwargs['symbols_failed'], 1)
        self.assertEqual(
            completion.kwargs['run_metadata']['failed_symbols'],
            ['000660'],
        )
        self.assertEqual(
            completion.kwargs['run_metadata'][
                'unexpected_persisted_symbols'
            ],
            ['035420'],
        )
        self.assertEqual(
            result['followup_recovery']['state_convergence']['status'],
            'completed_with_errors',
        )
        self.assertTrue(
            result['followup_recovery']['state_convergence'][
                'price_retry_pending'
            ]
        )
        update_eod_run.assert_not_called()

    def test_metadata_less_orphan_rebuilds_exact_price_scope_before_followups(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'expected_daily_pipeline_stages': [
                    'technical_snapshots'
                ],
                'stage_progress': {
                    'technical_snapshots': {'status': 'completed'}
                },
            },
        }
        exact_symbols = [
            PykrxSymbol(
                symbol='005930',
                market='KOSPI',
                name_ko='삼성전자',
            )
        ]

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    exact_symbols,
                    market_date='2026-04-03',
                ),
            ) as fetch_snapshot,
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_stages,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
            )

        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        fetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20260403',
            allow_master_fallback=False,
        )
        self.assertEqual(
            resume_stages.call_args.kwargs['symbols'],
            ['005930'],
        )

    def test_orphan_with_extra_price_row_resumes_only_admitted_symbols(
        self,
    ) -> None:
        service = PykrxBackfillService()
        admitted_symbols = ['005930', '000660']
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': admitted_symbols,
                'failed_symbols': [],
                'unattempted_symbols': [],
                'expected_daily_pipeline_stages': [
                    'technical_snapshots'
                ],
                'stage_progress': {
                    'technical_snapshots': {'status': 'completed'}
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=3,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=[*admitted_symbols, '035420'],
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
            )

        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(
            resume_stages.call_args.kwargs['symbols'],
            admitted_symbols,
        )
        complete_eod_run.assert_not_called()

    def test_metadata_less_orphan_scope_failure_is_durable_and_backed_off(
        self,
    ) -> None:
        service = PykrxBackfillService()
        persisted_recovery_runs: list[dict] = []
        completed_at = datetime.now(timezone.utc).isoformat()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'expected_daily_pipeline_stages': [
                    'technical_snapshots'
                ],
                'stage_progress': {
                    'technical_snapshots': {'status': 'completed'}
                },
            },
        }

        def get_latest_recovery(**_kwargs):
            return (
                persisted_recovery_runs[-1]
                if persisted_recovery_runs
                else None
            )

        def finish_failed_recovery(**kwargs):
            persisted = {
                'id': kwargs['pipeline_run_id'],
                'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
                'status': kwargs['status'],
                'completed_at': completed_at,
                'run_metadata': {
                    'resolved_stages': ['technical_snapshots'],
                    'reused_eod_collection_run_id': 7,
                },
                'stages': {
                    'technical_snapshots': {'status': 'failed'}
                },
            }
            persisted_recovery_runs.append(persisted)
            return persisted

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                side_effect=get_latest_recovery,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                side_effect=lambda **_kwargs: len(
                    persisted_recovery_runs
                ),
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'start_run',
                return_value={'id': 92},
            ) as start_recovery,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'finish_run',
                side_effect=finish_failed_recovery,
            ) as finish_recovery,
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                side_effect=RuntimeError('provider unavailable'),
            ) as fetch_snapshot,
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run'
            ) as resume_stages,
        ):
            first = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
            )
            second = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
            )

        self.assertTrue(first['triggered'])
        self.assertEqual(first['status'], 'failed')
        self.assertEqual(
            first['reason'],
            'followup_recovery_price_scope_unavailable',
        )
        self.assertEqual(first['followup_recovery']['attempt'], 1)
        self.assertEqual(second['reason'], 'followup_recovery_backoff')
        start_recovery.assert_called_once()
        self.assertFalse(
            start_recovery.call_args.kwargs['run_metadata'][
                'point_in_time_recovery'
            ]
        )
        finish_recovery.assert_called_once()
        fetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20260403',
            allow_master_fallback=False,
        )
        resume_stages.assert_not_called()

    def test_missing_first_followup_checkpoint_uses_durable_expected_stages(
        self,
    ) -> None:
        service = PykrxBackfillService()
        first_followup_stage = AUTO_DAILY_FOLLOWUP_STAGE_ORDER[0]
        first_followup_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(
            first_followup_stage
        )
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930', '000660'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': [],
                'expected_daily_pipeline_stages': list(
                    AUTO_DAILY_TRACKED_STAGE_ORDER
                ),
                'stage_progress': {
                    stage_id: {'status': 'completed'}
                    for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER[
                        :first_followup_index
                    ]
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=2,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930', '000660'],
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_stages,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertIsNotNone(result)
        self.assertEqual(
            result['followup_recovery']['resume_from_stage'],
            first_followup_stage,
        )
        recovery_call = resume_stages.call_args.kwargs
        self.assertEqual(
            recovery_call['stages'],
            list(AUTO_DAILY_TRACKED_STAGE_ORDER[first_followup_index:]),
        )
        self.assertEqual(
            recovery_call['symbols'],
            ['005930', '000660'],
        )

    def test_new_eod_scope_does_not_reuse_old_recovery_parent_evidence(
        self,
    ) -> None:
        service = PykrxBackfillService()
        old_recovery_parent = {
            'id': 91,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'completed',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
                'reused_eod_collection_run_id': 6,
            },
            'stages': {
                stage_id: {'status': 'completed'}
                for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
            },
        }
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['000660'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['000660'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=old_recovery_parent,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['000660'],
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch.object(
                service,
                '_persist_successful_followup_recovery',
            ) as converge_eod,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(result['followup_recovery']['symbols'], ['000660'])
        self.assertEqual(
            result['followup_recovery']['resumed_from_pipeline_run_id'],
            None,
        )
        resume_stages.assert_called_once()
        self.assertEqual(
            resume_stages.call_args.kwargs['symbols'],
            ['000660'],
        )
        converge_eod.assert_not_called()

    def test_price_retry_precedes_existing_downstream_recovery(self) -> None:
        service = PykrxBackfillService()
        failed_stage = 'technical_snapshots'
        failed_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(failed_stage)
        stage_rows = {
            stage_id: {
                'status': (
                    'completed'
                    if index < failed_index
                    else 'failed'
                    if index == failed_index
                    else 'blocked'
                )
            }
            for index, stage_id in enumerate(AUTO_DAILY_TRACKED_STAGE_ORDER)
        }
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
                'eod_collection_run_id': 7,
            },
            'stages': stage_rows,
        }
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_total': 2,
            'symbols_succeeded': 1,
            'symbols_failed': 1,
            'error_message': 'one price failed',
            'run_metadata': {
                'failed_symbols': ['472220'],
                # Never send a price-missing symbol into stages that require
                # a same-day bar, even if legacy metadata overlaps the scopes.
                'downstream_retry_symbols': ['005930', '472220'],
            },
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=latest_eod_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ) as update_eod_run,
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=1,
            ),
            patch.object(
                service,
                '_load_retry_symbols_for_run',
                return_value=[
                    PykrxSymbol(
                        symbol='472220',
                        market='KOSDAQ',
                        name_ko='test',
                        standard_code=None,
                    )
                ],
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 8,
                    'source': 'pykrx',
                    'status': 'completed_with_errors',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        resume_stages.assert_not_called()
        run_collection.assert_called_once()
        retry_call = run_collection.call_args.kwargs
        self.assertEqual(
            [item.symbol for item in retry_call['symbols_override']],
            ['472220'],
        )
        self.assertEqual(
            retry_call['retry_context'][
                'inherited_downstream_retry_symbols'
            ],
            ['005930'],
        )
        update_eod_run.assert_not_called()

    def test_completed_followup_stages_converge_interrupted_parent_on_next_poll(
        self,
    ) -> None:
        service = PykrxBackfillService()
        stage_rows = {
            stage_id: {'status': 'completed'}
            for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
        }
        stage_rows['pykrx_enrichment'] = {
            'status': 'completed',
            'processed': 4,
            'total': 4,
            'succeeded': 3,
            'failed': 1,
            'remaining': 0,
            'completed_at': '2026-04-03T07:59:00+00:00',
            'error_message': None,
            'details': {
                'result_summary': {
                    'status': 'completed',
                    'point_in_time_recovery': True,
                    'categories_requested': [
                        'sector_classification',
                        'investor_flow_symbol',
                        'short_selling',
                        'valuation_snapshot',
                    ],
                    'categories': {
                        'sector_classification': {
                            'status': 'completed',
                            'rows_persisted': 10,
                            'errors': [],
                        },
                        'investor_flow_symbol': {
                            'status': 'completed',
                            'rows_persisted': 10,
                            'errors': [],
                        },
                        'short_selling': {
                            'status': 'partial',
                            'rows_persisted': 10,
                            'errors': ['empty provider frame'],
                        },
                        'valuation_snapshot': {
                            'status': 'completed',
                            'rows_persisted': 10,
                            'errors': [],
                        },
                    },
                    'downstream_preflight': {
                        'allowed': True,
                        'status': 'ready',
                        'required_categories': [
                            'sector_classification',
                            'investor_flow_symbol',
                            'short_selling',
                            'valuation_snapshot',
                        ],
                        'category_row_counts': {
                            'sector_classification': 10,
                            'investor_flow_symbol': 10,
                            'short_selling': 10,
                            'valuation_snapshot': 10,
                        },
                        'failed_categories': [],
                        'blocked_categories': [],
                        'waiting_categories': [],
                        'zero_coverage_categories': [],
                    },
                }
            },
        }
        latest_pipeline_run = {
            'id': 92,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'interrupted',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
                'reused_eod_collection_run_id': 7,
                'terminal_continuation_count': 1,
                'terminal_continuation_version': 1,
                'terminal_continuation_reason': (
                    'summary_knowledge_cutoff_repair'
                ),
                'terminal_continuation_claimed_at': (
                    '2026-04-03T07:30:00+00:00'
                ),
                'terminal_continuation_evidence': {},
                'resume_from_stage': 'agent_feedback_summaries',
            },
            'stages': stage_rows,
        }
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'error_message': 'restart interrupted followups',
            'run_metadata': {
                'requested_symbols': ['005930'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
            },
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=latest_eod_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 92,
                    'status': 'completed',
                },
            ) as converge_pipeline_run,
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ) as count_persisted_symbols,
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ) as load_persisted_symbols,
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run'
            ) as resume_stages,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'followup_recovery_state_converged')
        count_persisted_symbols.assert_called_once_with(
            7,
            trade_date='2026-04-03',
        )
        load_persisted_symbols.assert_called_once_with(
            7,
            trade_date='2026-04-03',
        )
        complete_eod_run.assert_called_once()
        converge_pipeline_run.assert_called_once_with(
            pipeline_run_id=92,
            allow_satisfied_terminal=True,
        )
        self.assertTrue(
            result['followup_recovery']['state_convergence'][
                'pipeline_run'
            ]['updated']
        )
        resume_stages.assert_not_called()
        run_collection.assert_not_called()

    def test_primary_parent_converges_after_final_stage_finish_crash(
        self,
    ) -> None:
        service = PykrxBackfillService()
        stage_rows = {
            stage_id: {'status': 'completed'}
            for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
        }
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'interrupted',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
                'eod_collection_run_id': 7,
            },
            'stages': stage_rows,
        }
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': [],
            },
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=latest_eod_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 91,
                    'status': 'completed',
                },
            ) as converge_pipeline_run,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'daily_pipeline_state_converged')
        converge_pipeline_run.assert_called_once_with(pipeline_run_id=91)
        self.assertTrue(
            result['followup_recovery']['state_convergence'][
                'pipeline_run'
            ]['updated']
        )
        run_collection.assert_not_called()

    def test_legacy_downstream_scope_restarts_from_enrichment(
        self,
    ) -> None:
        service = PykrxBackfillService()
        stage_rows = {
            stage_id: {'status': 'completed'}
            for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
        }
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
                'eod_collection_run_id': 7,
            },
            'stages': stage_rows,
        }
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run'),
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertIsNotNone(result)
        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(
            result['followup_recovery']['resume_from_stage'],
            'pykrx_enrichment',
        )
        self.assertEqual(result['followup_recovery']['symbols'], [])
        resume_stages.assert_called_once()
        self.assertEqual(
            resume_stages.call_args.kwargs['resume_from_stage'],
            'pykrx_enrichment',
        )
        self.assertNotIn('symbols', resume_stages.call_args.kwargs)

    def test_legacy_scope_recovers_enrichment_before_a_later_failure(self) -> None:
        service = PykrxBackfillService()
        stage_rows = {
            stage_id: {'status': 'completed'}
            for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
        }
        stage_rows['action_summaries'] = {'status': 'partial'}
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
                'eod_collection_run_id': 7,
            },
            'stages': stage_rows,
        }
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_stages,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertIsNotNone(result)
        self.assertEqual(
            result['followup_recovery']['resume_from_stage'],
            'pykrx_enrichment',
        )
        self.assertNotIn('symbols', resume_stages.call_args.kwargs)
        self.assertIn(
            'action_summaries',
            resume_stages.call_args.kwargs['stages'],
        )

    def test_inherited_enrichment_gap_resumes_before_retry_completed_stages(
        self,
    ) -> None:
        service = PykrxBackfillService()
        stage_rows = {
            stage_id: {'status': 'completed'}
            for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
        }
        latest_pipeline_run = {
            'id': 92,
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
                'eod_collection_run_id': 8,
            },
            'stages': stage_rows,
        }
        latest_eod_run = {
            'id': 8,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            # This retry collected only its one missing price.  The inherited
            # cohort came from a prior full-scope enrichment partial, so the
            # retry's own completed enrichment row is not proof for it.
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['472220'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'inherited_downstream_retry_symbols': ['005930'],
                'downstream_retry_symbols': ['005930'],
                'downstream_retry_resume_from_stage': 'pykrx_enrichment',
                'retry': {
                    'retry_of_run_id': 7,
                    'inherited_downstream_retry_symbols': ['005930'],
                    'inherited_downstream_retry_resume_from_stage': (
                        'pykrx_enrichment'
                    ),
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 93},
            ) as resume_stages,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertIsNotNone(result)
        self.assertEqual(
            result['followup_recovery']['resume_from_stage'],
            'pykrx_enrichment',
        )
        recovery_call = resume_stages.call_args.kwargs
        self.assertNotIn('symbols', recovery_call)
        self.assertEqual(
            recovery_call['stages'],
            list(
                AUTO_DAILY_TRACKED_STAGE_ORDER[
                    AUTO_DAILY_TRACKED_STAGE_ORDER.index('pykrx_enrichment'):
                ]
            ),
        )

    def test_partial_followup_recovery_keeps_downstream_scope_retryable(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ) as update_eod_run,
        ):
            result = service._persist_successful_followup_recovery(
                latest_eod_run=latest_eod_run,
                recovery_result={'status': 'partial', 'pipeline_run_id': 92},
            )

        self.assertEqual(
            result,
            {'updated': False, 'reason': 'recovery_not_completed'},
        )
        self.assertEqual(
            latest_eod_run['run_metadata']['downstream_retry_symbols'],
            ['005930'],
        )
        complete_eod_run.assert_not_called()
        update_eod_run.assert_not_called()

    def test_weak_nonblocking_marker_cannot_complete_pykrx_recovery(self) -> None:
        service = PykrxBackfillService()
        marker = {
            'schema_version': 'nonblocking_partial_progression_v1',
            'market_date': '2026-10-28',
            'pipeline_run_id': 596,
            'reused_eod_collection_run_id': 582,
            'summary': {
                'completed_symbols': ['005930'],
            },
            'decider': {
                'completed_symbols': ['005930'],
            },
            'retry_debt': {
                'schema_version': 'nonblocking_feedback_retry_debt_v1',
                'items': [],
            },
        }
        marker['proof_digest'] = (
            nonblocking_partial_progression_digest(marker)
        )
        stage_rows = {
            'signal_decider': {'status': 'partial'},
            'next_open_execution_plan': {'status': 'completed'},
            'signal_decider_evaluations': {'status': 'completed'},
        }
        resolved_stages = list(stage_rows)
        pipeline_run = {
            'id': 596,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'partial',
            'market_date': '2026-10-28',
            'current_stage': 'signal_decider_evaluations',
            'run_metadata': {
                'resolved_stages': resolved_stages,
                'reused_eod_collection_run_id': 582,
                'nonblocking_partial_progression': marker,
            },
            'stages': stage_rows,
        }
        latest_eod_run = {
            'id': 582,
            'trade_date': '2026-10-28',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'downstream_retry_symbols': ['005930'],
            },
        }
        recovery_result = {
            'status': 'partial',
            'pipeline_run_id': 596,
            'pipeline_run': pipeline_run,
            'market_date': '2026-10-28',
            'resolved_stages': resolved_stages,
            'stages': stage_rows,
        }

        with patch(
            'app.eod.pykrx_backfill.complete_eod_collection_run'
        ) as complete_eod_run:
            result = service._persist_successful_followup_recovery(
                latest_eod_run=latest_eod_run,
                recovery_result=recovery_result,
            )

        self.assertEqual(
            result,
            {'updated': False, 'reason': 'recovery_not_completed'},
        )
        complete_eod_run.assert_not_called()

    def test_validated_nonblocking_partial_recovery_preserves_retry_debt(
        self,
    ) -> None:
        service = PykrxBackfillService()
        marker = {
            'schema_version': 'nonblocking_partial_progression_v1',
            'proof_digest': 'validated-by-shared-contract',
            'retry_debt': {
                'schema_version': 'nonblocking_feedback_retry_debt_v1',
                'items': [
                    {
                        'stage': 'agent_feedback_summaries',
                        'role': 'technical-summarizer',
                        'symbol': '028050',
                    }
                ],
            },
        }
        latest_eod_run = {
            'id': 582,
            'trade_date': '2026-10-28',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
                'downstream_retry_symbols_count': 1,
                'followup_recovery_required': True,
            },
        }
        recovery_result = {'status': 'partial', 'pipeline_run_id': 596}

        with (
            patch(
                'app.eod.pykrx_backfill.'
                'validated_nonblocking_partial_progression_marker',
                return_value=marker,
            ) as validate_marker,
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
        ):
            result = service._persist_successful_followup_recovery(
                latest_eod_run=latest_eod_run,
                recovery_result=recovery_result,
            )

        validate_marker.assert_called_once_with(
            recovery_result,
            expected_eod_collection_run_id=582,
        )
        self.assertEqual(result['status'], 'completed')
        metadata = complete_eod_run.call_args.kwargs['run_metadata']
        self.assertEqual(metadata['nonblocking_partial_progression'], marker)
        self.assertEqual(
            metadata['nonblocking_feedback_retry_debt'],
            marker['retry_debt'],
        )
        self.assertFalse(metadata['followup_recovery_required'])
        self.assertEqual(metadata['downstream_retry_symbols'], [])

    def test_successful_followup_recovery_clears_downstream_resume_provenance(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
                'downstream_retry_symbols_count': 1,
                'downstream_retry_resume_from_stage': 'pykrx_enrichment',
                'inherited_downstream_retry_symbols': ['005930'],
                'inherited_downstream_retry_symbols_count': 1,
                'inherited_downstream_retry_resume_from_stage': (
                    'pykrx_enrichment'
                ),
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
        ):
            result = service._persist_successful_followup_recovery(
                latest_eod_run=latest_eod_run,
                recovery_result={'status': 'completed', 'pipeline_run_id': 92},
            )

        self.assertEqual(result['status'], 'completed')
        completed_metadata = complete_eod_run.call_args.kwargs['run_metadata']
        self.assertEqual(completed_metadata['downstream_retry_symbols'], [])
        self.assertEqual(
            completed_metadata['inherited_downstream_retry_symbols'],
            [],
        )
        self.assertNotIn(
            'downstream_retry_resume_from_stage',
            completed_metadata,
        )
        self.assertNotIn(
            'inherited_downstream_retry_resume_from_stage',
            completed_metadata,
        )

    def test_successful_followup_recovery_clears_orphan_required_flag(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': [],
                'downstream_retry_symbols_count': 0,
                'followup_recovery_required': True,
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
        ):
            result = service._persist_successful_followup_recovery(
                latest_eod_run=latest_eod_run,
                recovery_result={'status': 'completed', 'pipeline_run_id': 92},
            )

        self.assertEqual(result['status'], 'completed')
        completion = complete_eod_run.call_args.kwargs
        self.assertEqual(completion['status'], 'completed')
        self.assertFalse(
            completion['run_metadata']['followup_recovery_required']
        )

    def test_followup_recovery_does_not_promote_incomplete_price_coverage(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'error_message': 'restart interrupted followups',
            'run_metadata': {
                'requested_symbols': ['005930', '000660'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ) as count_persisted_symbols,
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ) as load_persisted_symbols,
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ) as update_eod_run,
        ):
            result = service._persist_successful_followup_recovery(
                latest_eod_run=latest_eod_run,
                recovery_result={'status': 'completed', 'pipeline_run_id': 92},
            )

        count_persisted_symbols.assert_called_once_with(
            7,
            trade_date='2026-04-03',
        )
        load_persisted_symbols.assert_called_once_with(
            7,
            trade_date='2026-04-03',
        )
        complete_eod_run.assert_called_once()
        completion = complete_eod_run.call_args
        self.assertEqual(completion.kwargs['status'], 'completed_with_errors')
        self.assertEqual(completion.kwargs['symbols_succeeded'], 1)
        self.assertEqual(completion.kwargs['symbols_failed'], 1)
        self.assertEqual(
            completion.kwargs['run_metadata']['failed_symbols'],
            ['000660'],
        )
        update_eod_run.assert_not_called()
        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['persisted_symbol_count'], 1)
        self.assertTrue(result['price_retry_pending'])
        retryable_state = {
            'status': completion.kwargs['status'],
            'symbols_failed': completion.kwargs['symbols_failed'],
            'run_metadata': completion.kwargs['run_metadata'],
        }
        self.assertTrue(service._is_retryable_daily_run(retryable_state))
        self.assertEqual(
            service._metadata_retry_symbol_codes(retryable_state),
            ['000660'],
        )

    def test_followup_recovery_surfaces_persisted_coverage_above_admission(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=2,
            ),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ) as update_eod_run,
        ):
            result = service._persist_successful_followup_recovery(
                latest_eod_run=latest_eod_run,
                recovery_result={'status': 'completed', 'pipeline_run_id': 92},
            )

        completion = complete_eod_run.call_args
        self.assertEqual(completion.kwargs['status'], 'completed_with_errors')
        self.assertEqual(completion.kwargs['symbols_succeeded'], 1)
        self.assertEqual(completion.kwargs['symbols_failed'], 0)
        self.assertTrue(
            completion.kwargs['run_metadata'][
                'collection_coverage_inconsistent'
            ]
        )
        self.assertEqual(result['coverage_excess_count'], 1)
        self.assertFalse(result['price_retry_pending'])
        update_eod_run.assert_not_called()

    def test_followup_recovery_rejects_equal_count_symbol_substitution(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930', '000660'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': ['005930'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=2,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930', '035420'],
            ),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_eod_run,
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ) as update_eod_run,
        ):
            result = service._persist_successful_followup_recovery(
                latest_eod_run=latest_eod_run,
                recovery_result={'status': 'completed', 'pipeline_run_id': 92},
            )

        completion = complete_eod_run.call_args
        self.assertEqual(completion.kwargs['status'], 'completed_with_errors')
        self.assertEqual(completion.kwargs['symbols_succeeded'], 1)
        self.assertEqual(completion.kwargs['symbols_failed'], 1)
        self.assertEqual(
            completion.kwargs['run_metadata']['failed_symbols'],
            ['000660'],
        )
        self.assertEqual(
            completion.kwargs['run_metadata'][
                'unexpected_persisted_symbols'
            ],
            ['035420'],
        )
        self.assertTrue(result['price_retry_pending'])
        self.assertEqual(result['unexpected_symbol_count'], 1)
        update_eod_run.assert_not_called()

    def test_price_retry_continues_after_followup_recovery_limit(self) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'status': 'completed_with_errors',
            'symbols_failed': 1,
            'run_metadata': {
                'failed_symbols': ['472220'],
                'downstream_retry_symbols': ['005930'],
                'downstream_retry_resume_from_stage': 'pykrx_enrichment',
            },
        }
        retry_symbol = PykrxSymbol(
            symbol='472220',
            market='KOSDAQ',
            name_ko='test',
            standard_code=None,
        )

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=latest_eod_run,
            ),
            patch.object(
                service,
                '_resume_auto_daily_followups_if_due',
                return_value={
                    'triggered': False,
                    'status': 'skipped',
                    'reason': 'followup_recovery_limit_reached',
                },
            ) as resume_followups,
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=1,
            ),
            patch.object(
                service,
                '_load_retry_symbols_for_run',
                return_value=[retry_symbol],
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={'status': 'completed', 'run_id': 8},
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        resume_followups.assert_called_once()
        run_collection.assert_called_once()
        self.assertEqual(
            run_collection.call_args.kwargs['symbols_override'],
            [retry_symbol],
        )
        self.assertFalse(run_collection.call_args.kwargs['refresh_universe'])
        self.assertEqual(
            run_collection.call_args.kwargs['retry_context'][
                'inherited_downstream_retry_symbols'
            ],
            ['005930'],
        )
        self.assertEqual(
            run_collection.call_args.kwargs['retry_context'][
                'inherited_downstream_retry_resume_from_stage'
            ],
            'pykrx_enrichment',
        )

    def test_mixed_symbol_partial_and_incomplete_followup_resumes_full_universe_without_prices(self) -> None:
        service = PykrxBackfillService()
        failed_stage = 'technical_snapshots'
        failed_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(failed_stage)
        stage_rows = {
            stage_id: {'status': 'completed'}
            for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
        }
        stage_rows[failed_stage] = {'status': 'partial'}
        stage_rows['momentum_factors'] = {'status': 'failed'}
        stage_rows.pop('signal_decider')
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
                'eod_collection_run_id': 7,
            },
            'stages': stage_rows,
        }

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_failed': 0,
                    'run_metadata': {
                        'failed_symbols': [],
                        'downstream_retry_symbols': ['005930'],
                    },
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 92,
                    'status': 'completed',
                },
            ),
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={'updated': True, 'status': 'completed'},
            ),
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        recovery_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(
            'pykrx_enrichment'
        )
        expected_stages = list(AUTO_DAILY_TRACKED_STAGE_ORDER[recovery_index:])
        self.assertTrue(result['triggered'])
        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(
            result['followup_recovery']['resume_from_stage'],
            'pykrx_enrichment',
        )
        self.assertEqual(result['followup_recovery']['stages'], expected_stages)
        self.assertEqual(result['followup_recovery']['symbols'], [])
        self.assertNotIn('price_collection', expected_stages)
        resume_stages.assert_called_once_with(
            market_date='2026-04-03',
            stages=expected_stages,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            force=True,
            reuse_existing_feedback=False,
            resume_from_stage='pykrx_enrichment',
            trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            reused_eod_collection_run_id=7,
            provider_admission_fence_owned=True,
        )
        run_collection.assert_not_called()

    def test_followup_recovery_respects_retry_backoff(self) -> None:
        service = PykrxBackfillService()
        latest_pipeline_run = {
            'id': 92,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'failed',
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'completed_at': datetime.now(timezone.utc).isoformat(),
            'run_metadata': {
                'resolved_stages': ['signal_decider'],
                'reused_eod_collection_run_id': 7,
            },
            'stages': {'signal_decider': {'status': 'failed'}},
        }

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={'id': 7, 'status': 'completed', 'symbols_failed': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=1,
            ),
            patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run') as resume_followups,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'followup_recovery_backoff')
        self.assertGreater(result['followup_recovery']['retry_after_seconds'], 0)
        resume_followups.assert_not_called()
        run_collection.assert_not_called()

    def test_followup_recovery_stops_after_bounded_attempts(self) -> None:
        service = PykrxBackfillService()
        latest_pipeline_run = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'failed',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': ['signal_decider'],
                'reused_eod_collection_run_id': 7,
            },
            'stages': {'signal_decider': {'status': 'failed'}},
        }

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={'id': 7, 'status': 'completed', 'symbols_failed': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=5,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ) as load_repair_checkpoint,
            patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run') as resume_followups,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'followup_recovery_limit_reached')
        self.assertEqual(result['followup_recovery']['attempts'], 5)
        load_repair_checkpoint.assert_not_called()
        resume_followups.assert_not_called()
        run_collection.assert_not_called()

    def test_retryable_feedback_failure_rearms_with_exact_parent_contract(
        self,
    ) -> None:
        service = PykrxBackfillService()
        now = datetime.now(timezone.utc)
        latest_pipeline_run = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'updated_at': now.isoformat(),
            'completed_at': now.isoformat(),
            'run_metadata': {
                'resolved_stages': [
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'reused_eod_collection_run_id': 7,
            },
            'stages': {
                'agent_feedback_summaries': {
                    'status': 'partial',
                    'details': {
                        'result_summary': {
                            'run_id': 'feedback-cycle-test',
                            'prerequisites': {'ready': True},
                            'progress': {
                                'summary': {
                                    'run_status': 'partial',
                                    'failed_items': 1,
                                    'completed_cycles': 2,
                                    'cycle_number': 2,
                                    'role_progress': {
                                        'eod-summarizer': {
                                            'retry_pending_symbols': 1,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 0,
                                        }
                                    },
                                }
                            }
                        }
                    },
                },
                'signal_decider': {'status': 'blocked'},
                'next_open_execution_plan': {'status': 'blocked'},
                'signal_decider_evaluations': {'status': 'blocked'},
            },
        }
        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(latest_pipeline_run)
        )
        self.assertEqual(
            _retryable_feedback_rearm_max_cycles(latest_pipeline_run),
            3,
        )
        persisted_shape_pipeline_run = deepcopy(latest_pipeline_run)
        persisted_result = persisted_shape_pipeline_run['stages'][
            'agent_feedback_summaries'
        ]['details']['result_summary']
        persisted_result['progress']['prerequisites'] = (
            persisted_result.pop('prerequisites')
        )
        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(
                persisted_shape_pipeline_run
            )
        )
        parent_cutoff = datetime(
            2026,
            4,
            3,
            8,
            0,
            tzinfo=timezone.utc,
        )
        parent_symbols = ['005930', '000660']
        parent_role_symbol_scopes = {
            role: list(parent_symbols)
            for role in SUMMARIZER_AGENT_NAMES
        }
        parent_feedback_run = {
            'run_id': 'feedback-cycle-test',
            'market_date': '2026-04-03',
            'status': 'partial',
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'roles': list(SUMMARIZER_AGENT_NAMES),
                'requested_symbols': list(parent_symbols),
                'symbols': list(parent_symbols),
                'role_symbol_scopes': parent_role_symbol_scopes,
                'feedback_scope_mode': 'decider_scope_fast_path',
                'knowledge_cutoff_at': parent_cutoff.isoformat(),
                'admission_knowledge_cutoff_at': (
                    parent_cutoff.isoformat()
                ),
                'fs_div': 'OFS',
                'policy_context_as_of_date': '2026-04-06',
                'outer_batch_size': 17,
            },
        }
        with patch(
            'app.eod.pykrx_backfill.'
            'agent_context_feedback_run_state_service.get_run',
            return_value=parent_feedback_run,
        ) as get_feedback_run:
            rearm_config = _retryable_feedback_rearm_config(
                latest_pipeline_run,
                market_date='2026-04-03',
            )
            self.assertIsNotNone(rearm_config)
            self.assertEqual(
                rearm_config['feedback_scope_mode'],
                'decider_scope_fast_path',
            )
            self.assertEqual(
                rearm_config['requested_symbols'],
                parent_symbols,
            )
            self.assertEqual(
                rearm_config['role_symbol_scopes'],
                parent_role_symbol_scopes,
            )
            self.assertEqual(rearm_config['fs_div'], 'OFS')
            self.assertEqual(
                rearm_config['policy_context_as_of_date'],
                date(2026, 4, 6),
            )
            self.assertEqual(
                _retryable_feedback_rearm_knowledge_cutoff(
                    latest_pipeline_run,
                    market_date='2026-04-03',
                ),
                datetime(2026, 4, 3, 8, 0, tzinfo=timezone.utc),
            )
        self.assertEqual(get_feedback_run.call_count, 2)
        get_feedback_run.assert_called_with(run_id='feedback-cycle-test')
        failed_pipeline_run = deepcopy(latest_pipeline_run)
        failed_pipeline_run['stages']['agent_feedback_summaries'][
            'status'
        ] = 'failed'
        failed_pipeline_run['stages']['agent_feedback_summaries'][
            'details'
        ]['result_summary']['progress']['summary'][
            'run_status'
        ] = 'failed'
        failed_summary = failed_pipeline_run['stages'][
            'agent_feedback_summaries'
        ]['details']['result_summary']['progress']['summary']
        failed_summary['completed_cycles'] = 0
        failed_summary['cycle_number'] = 1
        failed_parent_run = deepcopy(parent_feedback_run)
        failed_parent_run['status'] = 'failed'
        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(failed_pipeline_run)
        )
        self.assertEqual(
            _retryable_feedback_rearm_max_cycles(failed_pipeline_run),
            1,
        )
        with patch(
            'app.eod.pykrx_backfill.'
            'agent_context_feedback_run_state_service.get_run',
            return_value=failed_parent_run,
        ):
            self.assertIsNotNone(
                _retryable_feedback_rearm_config(
                    failed_pipeline_run,
                    market_date='2026-04-03',
                )
            )
        latest_pipeline_run = failed_pipeline_run
        parent_feedback_run = failed_parent_run
        latest_pipeline_run['stages']['agent_feedback_summaries'][
            'details'
        ]['result_summary']['prerequisites']['ready'] = False
        latest_pipeline_run['run_metadata'][
            'inherited_feedback_prerequisite_admission'
        ] = True
        latest_pipeline_run['run_metadata'][
            'inherited_feedback_knowledge_cutoff_at'
        ] = '2026-04-03T08:00:00+00:00'
        latest_pipeline_run['run_metadata'][
            'inherited_feedback_source_run_id'
        ] = 'feedback-cycle-test'
        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(latest_pipeline_run)
        )

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'agent_context_feedback_run_state_service.get_run',
                return_value=parent_feedback_run,
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run'
            ) as resume_followups,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'followup_recovery_backoff')
        self.assertTrue(
            result['followup_recovery']['retryable_feedback_rearm']
        )
        self.assertTrue(result['followup_recovery']['retry_scheduled'])
        self.assertEqual(result['followup_recovery']['max_attempts'], 8)
        resume_followups.assert_not_called()
        run_collection.assert_not_called()

        latest_pipeline_run['updated_at'] = (
            now - timedelta(minutes=10)
        ).isoformat()
        latest_pipeline_run['completed_at'] = (
            now - timedelta(minutes=10)
        ).isoformat()
        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'agent_context_feedback_run_state_service.get_run',
                return_value=parent_feedback_run,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                return_value={
                    'result': None,
                    'symbols': [],
                    'downstream_symbols': [],
                },
            ),
            patch.object(
                service,
                '_resolve_point_in_time_followup_membership',
                return_value=({'005930'}, ('KOSPI',)),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'status': 'partial',
                    'pipeline_run_id': 97,
                },
            ) as rearmed_followups,
        ):
            rearmed = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=False,
            )

        self.assertTrue(rearmed['triggered'])
        self.assertTrue(
            rearmed['followup_recovery']['retryable_feedback_rearm']
        )
        self.assertNotIn(
            'inherit_feedback_prerequisite_admission',
            rearmed_followups.call_args.kwargs,
        )
        self.assertEqual(
            rearmed_followups.call_args.kwargs[
                'inherited_feedback_knowledge_cutoff_at'
            ],
            datetime(2026, 4, 3, 8, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            rearmed_followups.call_args.kwargs[
                'inherited_feedback_source_run_id'
            ],
            'feedback-cycle-test',
        )
        self.assertEqual(
            rearmed_followups.call_args.kwargs['feedback_max_cycles'],
            1,
        )
        self.assertEqual(
            rearmed_followups.call_args.kwargs['symbols'],
            parent_symbols,
        )
        self.assertEqual(
            rearmed_followups.call_args.kwargs['fs_div'],
            'OFS',
        )
        self.assertEqual(
            rearmed_followups.call_args.kwargs[
                'feedback_outer_batch_size'
            ],
            17,
        )
        inherited_config = rearmed_followups.call_args.kwargs[
            'inherited_feedback_retry_config'
        ]
        self.assertEqual(
            inherited_config['feedback_scope_mode'],
            'decider_scope_fast_path',
        )
        self.assertEqual(
            inherited_config['requested_symbols'],
            parent_symbols,
        )
        self.assertEqual(
            inherited_config['role_symbol_scopes'],
            parent_role_symbol_scopes,
        )
        self.assertEqual(
            inherited_config['knowledge_cutoff_at'],
            parent_cutoff,
        )
        self.assertEqual(
            inherited_config['policy_context_as_of_date'],
            date(2026, 4, 6),
        )
        self.assertIsNone(inherited_config['scope_authority'])

        failed_exact_retry_run = {
            'id': 97,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'failed',
            'updated_at': (
                now - timedelta(minutes=10)
            ).isoformat(),
            'completed_at': (
                now - timedelta(minutes=10)
            ).isoformat(),
            'run_metadata': {
                'resolved_stages': [
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'reused_eod_collection_run_id': 7,
                'symbols': list(parent_symbols),
                'fs_div': 'OFS',
                'feedback_max_cycles': 1,
                'feedback_outer_batch_size': 17,
                'inherited_feedback_prerequisite_admission': False,
                'inherited_feedback_knowledge_cutoff_at': (
                    parent_cutoff.isoformat()
                ),
                'inherited_feedback_source_run_id': (
                    'feedback-cycle-test'
                ),
                'feedback_retry_parent_contract': {
                    'schema_version': (
                        FEEDBACK_RETRY_PARENT_CONTRACT_VERSION
                    ),
                    'source_run_id': 'feedback-cycle-test',
                    'knowledge_cutoff_at': parent_cutoff.isoformat(),
                    'feedback_scope_mode': (
                        'decider_scope_fast_path'
                    ),
                },
            },
            'stages': {
                'agent_feedback_summaries': {
                    'status': 'failed',
                    'details': {'error_message': 'transport failure'},
                },
                'signal_decider': {'status': 'blocked'},
                'next_open_execution_plan': {'status': 'blocked'},
                'signal_decider_evaluations': {'status': 'blocked'},
            },
        }
        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(
                failed_exact_retry_run
            )
        )
        self.assertEqual(
            _retryable_feedback_rearm_max_cycles(
                failed_exact_retry_run
            ),
            1,
        )
        missing_stage_exact_retry_run = deepcopy(
            failed_exact_retry_run
        )
        missing_stage_exact_retry_run['stages'] = {}
        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(
                missing_stage_exact_retry_run
            )
        )
        blocked_exact_retry_run = deepcopy(failed_exact_retry_run)
        blocked_exact_retry_run['stages'][
            'agent_feedback_summaries'
        ]['status'] = 'blocked'
        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(
                blocked_exact_retry_run
            )
        )
        advanced_exact_retry_run = deepcopy(failed_exact_retry_run)
        advanced_exact_retry_run['stages'][
            'agent_feedback_summaries'
        ] = {
            'status': 'partial',
            'details': {
                'result_summary': {
                    'run_id': 'feedback-cycle-child',
                }
            },
        }
        child_feedback_run = deepcopy(parent_feedback_run)
        child_feedback_run['run_id'] = 'feedback-cycle-child'
        child_feedback_run['status'] = 'partial'
        child_feedback_run['request_payload'][
            'resumed_from_run_id'
        ] = 'feedback-cycle-test'
        with patch(
            'app.eod.pykrx_backfill.'
            'agent_context_feedback_run_state_service.get_run',
            return_value=child_feedback_run,
        ):
            advanced_config = _retryable_feedback_rearm_config(
                advanced_exact_retry_run,
                market_date='2026-04-03',
            )
        self.assertIsNotNone(advanced_config)
        self.assertEqual(
            advanced_config['source_run_id'],
            'feedback-cycle-child',
        )
        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=missing_stage_exact_retry_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=2,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'agent_context_feedback_run_state_service.get_run',
                return_value=parent_feedback_run,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                return_value={
                    'result': None,
                    'symbols': [],
                    'downstream_symbols': [],
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'status': 'partial',
                    'pipeline_run_id': 98,
                },
            ) as second_rearmed_followups,
        ):
            second_rearmed = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=False,
            )

        self.assertTrue(second_rearmed['triggered'])
        second_kwargs = second_rearmed_followups.call_args.kwargs
        self.assertEqual(second_kwargs['symbols'], parent_symbols)
        self.assertEqual(second_kwargs['fs_div'], 'OFS')
        self.assertEqual(second_kwargs['feedback_outer_batch_size'], 17)
        self.assertEqual(second_kwargs['feedback_max_cycles'], 1)
        self.assertNotIn(
            'inherit_feedback_prerequisite_admission',
            second_kwargs,
        )
        second_config = second_kwargs[
            'inherited_feedback_retry_config'
        ]
        self.assertEqual(
            second_config['source_run_id'],
            'feedback-cycle-test',
        )
        self.assertEqual(
            second_config['feedback_scope_mode'],
            'decider_scope_fast_path',
        )
        self.assertEqual(
            second_config['requested_symbols'],
            parent_symbols,
        )
        self.assertEqual(
            second_config['role_symbol_scopes'],
            parent_role_symbol_scopes,
        )
        self.assertEqual(
            second_config['knowledge_cutoff_at'],
            parent_cutoff,
        )
        self.assertEqual(
            second_config['policy_context_as_of_date'],
            date(2026, 4, 6),
        )

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=8,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run'
            ) as capped_resume_followups,
            patch.object(
                service,
                '_run_market_data_pipeline',
            ) as capped_run_collection,
        ):
            capped = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(capped['triggered'])
        self.assertEqual(capped['reason'], 'followup_recovery_limit_reached')
        self.assertEqual(capped['followup_recovery']['max_attempts'], 8)
        capped_resume_followups.assert_not_called()
        capped_run_collection.assert_not_called()

    def test_shared_openclaw_outage_rearms_only_with_versioned_exact_marker(
        self,
    ) -> None:
        pipeline_run = {
            'status': 'blocked',
            'run_metadata': {},
            'stages': {
                'agent_feedback_summaries': {
                    'status': 'failed',
                    'details': {
                        'result_summary': {
                            'run_id': 'feedback-cycle-shared-outage',
                            'prerequisites': {'ready': True},
                            'openclaw_dispatch_circuit_breaker': {
                                'schema_version': (
                                    OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION
                                ),
                                'failure_kind': (
                                    'openclaw_provider_rate_limited'
                                ),
                                'failure_status': 'provider/rate-limit',
                                'retryable': True,
                                'source': (
                                    'child_dispatch_circuit_breaker'
                                ),
                            },
                            'progress': {
                                'summary': {
                                    'run_status': 'failed',
                                    'failed_items': 0,
                                    'completed_cycles': 0,
                                    'cycle_number': 1,
                                    'role_progress': {
                                        'eod-summarizer': {
                                            'retry_pending_symbols': 2,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 2,
                                        }
                                    },
                                }
                            },
                        }
                    },
                }
            },
        }

        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(pipeline_run)
        )
        self.assertEqual(
            _retryable_feedback_rearm_max_cycles(pipeline_run),
            1,
        )

        parent_marker_run = deepcopy(pipeline_run)
        parent_marker_run['run_metadata'][
            'feedback_retry_parent_contract'
        ] = {'schema_version': 'feedback_retry_parent_contract_v1'}
        parent_role = parent_marker_run['stages'][
            'agent_feedback_summaries'
        ]['details']['result_summary']['progress']['summary'][
            'role_progress'
        ]['eod-summarizer']
        parent_role['in_progress_symbols'] = 1
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(parent_marker_run)
        )
        parent_role['in_progress_symbols'] = 0
        parent_role['retry_pending_symbols'] = 0
        parent_role['pending_symbols'] = 0
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(parent_marker_run)
        )

        for field, value in (
            ('schema_version', 'unknown'),
            ('failure_kind', 'dispatch_failed'),
            ('retryable', False),
            ('source', 'item_scoped_failure'),
        ):
            malformed = deepcopy(pipeline_run)
            malformed['stages']['agent_feedback_summaries'][
                'details'
            ]['result_summary']['openclaw_dispatch_circuit_breaker'][
                field
            ] = value
            self.assertFalse(
                _is_retryable_feedback_rearm_candidate(malformed),
                field,
            )

        without_marker = deepcopy(pipeline_run)
        del without_marker['stages']['agent_feedback_summaries'][
            'details'
        ]['result_summary']['openclaw_dispatch_circuit_breaker']
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(without_marker)
        )

    def test_completed_feedback_prefix_normalizes_to_signal_decider_retry_suffix(
        self,
    ) -> None:
        service = PykrxBackfillService()
        completed_at = datetime.now(timezone.utc) - timedelta(minutes=20)
        pipeline_run = {
            'id': 584,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'updated_at': completed_at.isoformat(),
            'completed_at': completed_at.isoformat(),
            'run_metadata': {
                'resolved_stages': [
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'resume_from_stage': 'agent_feedback_summaries',
                'reused_eod_collection_run_id': 7,
                'fs_div': 'CFS',
                'feedback_knowledge_cutoff': {
                    'knowledge_cutoff_at': (
                        '2026-10-20T08:00:00+00:00'
                    ),
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': ['005930'],
                },
            },
            'stages': {
                'agent_feedback_summaries': {'status': 'completed'},
                'signal_decider': {
                    'status': 'partial',
                    'details': {
                        'result_summary': {
                            'run_id': 'decider-cycle-run-584',
                            'prerequisites': {'ready': True},
                            'openclaw_dispatch_circuit_breaker': {
                                'schema_version': (
                                    OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION
                                ),
                                'failure_kind': (
                                    'openclaw_provider_rate_limited'
                                ),
                                'failure_status': 'provider/rate-limit',
                                'role': 'short-term-decider',
                                'retryable': True,
                                'source': 'child_dispatch_circuit_breaker',
                            },
                            'progress': {
                                'summary': {
                                    'run_status': 'partial',
                                    'failed_items': 20,
                                    'completed_cycles': 1,
                                    'cycle_number': 2,
                                    'role_progress': {
                                        'short-term-decider': {
                                            'retry_pending_symbols': 80,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 60,
                                        },
                                        'signal-decider': {
                                            'retry_pending_symbols': 80,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 80,
                                        }
                                    },
                                }
                            },
                        }
                    },
                },
                'next_open_execution_plan': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'reason': 'signal_decider_stage_incomplete',
                        }
                    },
                },
                'signal_decider_evaluations': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'reason': 'declared_dependencies_incomplete',
                            'incomplete_dependencies': ['signal_decider'],
                        }
                    },
                },
            },
        }

        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(pipeline_run)
        )

        incomplete_prefix = deepcopy(pipeline_run)
        incomplete_prefix['stages']['agent_feedback_summaries'][
            'status'
        ] = 'partial'
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(incomplete_prefix)
        )

        invalid_cutoff = deepcopy(pipeline_run)
        invalid_cutoff['run_metadata']['feedback_knowledge_cutoff'][
            'knowledge_cutoff_at'
        ] = '2026-10-20T08:00:00'
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(invalid_cutoff)
        )

        with (
            patch(
                'app.eod.pykrx_backfill.'
                'daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'daily_pipeline_run_state_service.count_runs',
                return_value=6,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                return_value={
                    'result': None,
                    'symbols': [],
                    'downstream_symbols': [],
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'status': 'partial',
                    'pipeline_run_id': 585,
                },
            ) as resume_followups,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-10-20',
                latest_available_trade_date='2026-10-20',
                latest_eod_run={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                    'run_metadata': {},
                },
                markets=('KOSPI',),
            )

        self.assertTrue(result['triggered'], result)
        recovery_kwargs = resume_followups.call_args.kwargs
        self.assertEqual(
            recovery_kwargs['stages'],
            [
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            ],
        )
        self.assertEqual(
            recovery_kwargs['resume_from_stage'],
            'signal_decider',
        )
        self.assertNotIn(
            'agent_feedback_summaries',
            recovery_kwargs['stages'],
        )

    def test_signal_decider_dispatch_circuit_rearms_two_consecutive_retries(
        self,
    ) -> None:
        service = PykrxBackfillService()
        completed_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        summary_proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': ['005930'],
        }

        def decider_retry_run(run_id: int, decider_run_id: str) -> dict:
            return {
                'id': run_id,
                'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
                'status': 'blocked',
                'updated_at': completed_at.isoformat(),
                'completed_at': completed_at.isoformat(),
                'run_metadata': {
                    'resolved_stages': [
                        'signal_decider',
                        'next_open_execution_plan',
                        'signal_decider_evaluations',
                    ],
                    'resume_from_stage': 'signal_decider',
                    'reused_eod_collection_run_id': 7,
                    'fs_div': 'CFS',
                    'feedback_knowledge_cutoff': {
                        'knowledge_cutoff_at': (
                            '2026-04-03T08:00:00+00:00'
                        ),
                        'ready': True,
                        'reason': 'latest_completed_summary_cutoff',
                        **summary_proof,
                    },
                },
                'stages': {
                    'signal_decider': {
                        'status': 'failed',
                        'details': {
                            'result_summary': {
                                'run_id': decider_run_id,
                                'prerequisites': {'ready': True},
                                'openclaw_dispatch_circuit_breaker': {
                                    'schema_version': (
                                        OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION
                                    ),
                                    'failure_kind': (
                                        'openclaw_transport_timeout'
                                    ),
                                    'failure_status': 'transport/timeout',
                                    'role': 'signal-decider',
                                    'retryable': True,
                                    'source': (
                                        'child_dispatch_circuit_breaker'
                                    ),
                                },
                                'progress': {
                                    'summary': {
                                        'run_status': 'failed',
                                        'failed_items': 1,
                                        'completed_cycles': 0,
                                        'cycle_number': 1,
                                        'role_progress': {
                                            'signal-decider': {
                                                'retry_pending_symbols': 1,
                                                'in_progress_symbols': 0,
                                                'pending_symbols': 0,
                                            }
                                        },
                                    }
                                },
                            }
                        },
                    },
                    'next_open_execution_plan': {
                        'status': 'blocked',
                        'details': {
                            'result_summary': {
                                'reason': (
                                    'signal_decider_stage_incomplete'
                                ),
                            }
                        },
                    },
                    'signal_decider_evaluations': {
                        'status': 'blocked',
                        'details': {
                            'result_summary': {
                                'reason': (
                                    'declared_dependencies_incomplete'
                                ),
                                'incomplete_dependencies': [
                                    'signal_decider'
                                ],
                            }
                        },
                    },
                },
            }

        latest_eod_run = {
            'id': 7,
            'status': 'completed',
            'symbols_failed': 0,
            'run_metadata': {},
        }
        first_parent = decider_retry_run(96, 'decider-cycle-one')
        second_parent = decider_retry_run(97, 'decider-cycle-two')

        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(first_parent)
        )
        self.assertEqual(
            _retryable_feedback_rearm_max_cycles(first_parent),
            1,
        )

        for parent, attempt_count in (
            (first_parent, 5),
            (second_parent, 6),
        ):
            with (
                self.subTest(attempt_count=attempt_count),
                patch(
                    'app.eod.pykrx_backfill.'
                    'daily_pipeline_run_state_service.'
                    'get_latest_run_for_context',
                    return_value=parent,
                ),
                patch(
                    'app.eod.pykrx_backfill.'
                    'daily_pipeline_run_state_service.count_runs',
                    return_value=attempt_count,
                ),
                patch.object(
                    service,
                    '_preflight_untracked_followup_price_scope',
                    return_value={
                        'result': None,
                        'symbols': [],
                        'downstream_symbols': [],
                    },
                ),
                patch(
                    'app.eod.daily_pipeline_stages.'
                    'daily_pipeline_stage_service.run',
                    return_value={
                        'status': 'partial',
                        'pipeline_run_id': parent['id'] + 1,
                    },
                ) as resume_followups,
            ):
                result = service._resume_auto_daily_followups_if_due(
                    market_date='2026-04-03',
                    latest_available_trade_date='2026-04-03',
                    latest_eod_run=latest_eod_run,
                    markets=('KOSPI',),
                )

            self.assertTrue(result['triggered'])
            self.assertTrue(
                result['followup_recovery'][
                    'retryable_feedback_rearm'
                ]
            )
            recovery_kwargs = resume_followups.call_args.kwargs
            self.assertEqual(
                recovery_kwargs['summary_cutoff_proof'],
                {
                    **summary_proof,
                    'requested_symbols': None,
                    'expected_completed_symbols': ['005930'],
                    'knowledge_cutoff_at': (
                        '2026-04-03T08:00:00+00:00'
                    ),
                    'fs_div': 'CFS',
                },
            )
            self.assertEqual(recovery_kwargs['feedback_max_cycles'], 1)
            self.assertEqual(recovery_kwargs['fs_div'], 'CFS')
            self.assertNotIn(
                'inherited_feedback_retry_config',
                recovery_kwargs,
            )

        malformed = decider_retry_run(98, 'decider-cycle-invalid')
        malformed['run_metadata']['feedback_knowledge_cutoff'][
            'requested_symbols'
        ] = ['5930']
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(malformed)
        )

        naive_cutoff = decider_retry_run(99, 'decider-cycle-naive')
        naive_cutoff['run_metadata']['feedback_knowledge_cutoff'][
            'knowledge_cutoff_at'
        ] = '2026-04-03T08:00:00'
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(naive_cutoff)
        )

        unsupported_mode = decider_retry_run(
            100,
            'decider-cycle-mode',
        )
        unsupported_mode['run_metadata']['feedback_knowledge_cutoff'][
            'feedback_scope_mode'
        ] = 'unknown_scope'
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(unsupported_mode)
        )

        wrong_role = decider_retry_run(101, 'decider-cycle-role')
        wrong_role_summary = wrong_role['stages']['signal_decider'][
            'details'
        ]['result_summary']
        wrong_role_summary['openclaw_dispatch_circuit_breaker'][
            'role'
        ] = 'eod-summarizer'
        wrong_role_summary['progress']['summary']['role_progress'] = {
            'eod-summarizer': {
                'retry_pending_symbols': 1,
                'in_progress_symbols': 0,
                'pending_symbols': 0,
            }
        }
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(wrong_role)
        )

        completed_stage = decider_retry_run(
            102,
            'decider-cycle-completed',
        )
        completed_stage['stages']['signal_decider'][
            'status'
        ] = 'completed'
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(completed_stage)
        )

        rate_limited = decider_retry_run(
            103,
            'decider-cycle-rate-limited',
        )
        rate_limit_marker = rate_limited['stages']['signal_decider'][
            'details'
        ]['result_summary']['openclaw_dispatch_circuit_breaker']
        rate_limit_marker['failure_kind'] = (
            'openclaw_provider_rate_limited'
        )
        rate_limit_marker['failure_status'] = 'provider/rate-limit'
        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(rate_limited)
        )

    def test_signal_decider_terminal_retry_only_rearms_through_exact_eighth_attempt(
        self,
    ) -> None:
        service = PykrxBackfillService()
        completed_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        latest_pipeline_run = {
            'id': 581,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'updated_at': completed_at.isoformat(),
            'completed_at': completed_at.isoformat(),
            'run_metadata': {
                'resolved_stages': [
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'resume_from_stage': 'signal_decider',
                'reused_eod_collection_run_id': 7,
                'fs_div': 'CFS',
                'feedback_knowledge_cutoff': {
                    'knowledge_cutoff_at': (
                        '2026-10-20T08:00:00+00:00'
                    ),
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': ['005930'],
                },
            },
            'stages': {
                'signal_decider': {
                    'status': 'partial',
                    'details': {
                        'result_summary': {
                            'progress': {
                                'prerequisites': {'ready': True},
                                'summary': {
                                    'run_status': 'partial',
                                    'failed_items': 20,
                                    'completed_cycles': 1,
                                    'cycle_number': 2,
                                    'failure_groups': {
                                        'dependency/upstream-changed-during-dispatch': {
                                            # Persisted failure groups can be
                                            # narrower than aggregate retry
                                            # counters.
                                            'count': 7,
                                            'retryable_count': 7,
                                            'failure_kinds': [
                                                'dependency_upstream_changed_during_dispatch'
                                            ],
                                        }
                                    },
                                    'role_progress': {
                                        'short-term-decider': {
                                            'status': 'partial',
                                            'total_symbols': 80,
                                            'processed_symbols': 80,
                                            'completed_symbols': 60,
                                            'failed_items': 20,
                                            'retry_pending_symbols': 20,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 0,
                                        },
                                        'signal-decider': {
                                            'status': 'completed',
                                            'total_symbols': 80,
                                            'processed_symbols': 80,
                                            'completed_symbols': 80,
                                            'failed_items': 0,
                                            'retry_pending_symbols': 0,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 0,
                                        }
                                    },
                                }
                            },
                        }
                    },
                },
                'next_open_execution_plan': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'reason': 'signal_decider_stage_incomplete',
                        }
                    },
                },
                'signal_decider_evaluations': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'reason': 'declared_dependencies_incomplete',
                            'incomplete_dependencies': [
                                'signal_decider'
                            ],
                        }
                    },
                },
            },
        }

        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(latest_pipeline_run)
        )
        self.assertEqual(
            _retryable_feedback_rearm_max_cycles(latest_pipeline_run),
            2,
        )

        without_visible_failure_groups = deepcopy(latest_pipeline_run)
        del without_visible_failure_groups['stages']['signal_decider'][
            'details'
        ]['result_summary']['progress']['summary']['failure_groups']
        self.assertTrue(
            _is_retryable_feedback_rearm_candidate(
                without_visible_failure_groups
            )
        )

        nonretryable = deepcopy(latest_pipeline_run)
        failure_group = nonretryable['stages']['signal_decider'][
            'details'
        ]['result_summary']['progress']['summary']['failure_groups'][
            'dependency/upstream-changed-during-dispatch'
        ]
        failure_group['retryable_count'] = 6
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(nonretryable)
        )

        failed_without_marker = deepcopy(latest_pipeline_run)
        failed_without_marker['stages']['signal_decider'][
            'status'
        ] = 'failed'
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(
                failed_without_marker
            )
        )

        invalid_cutoff = deepcopy(latest_pipeline_run)
        invalid_cutoff['run_metadata']['feedback_knowledge_cutoff'][
            'knowledge_cutoff_at'
        ] = '2026-10-20T08:00:00'
        self.assertFalse(
            _is_retryable_feedback_rearm_candidate(invalid_cutoff)
        )

        latest_eod_run = {
            'id': 7,
            'status': 'completed',
            'symbols_failed': 0,
            'run_metadata': {},
        }
        for recovery_attempts in (6, 7):
            with (
                self.subTest(recovery_attempts=recovery_attempts),
                patch(
                    'app.eod.pykrx_backfill.'
                    'daily_pipeline_run_state_service.'
                    'get_latest_run_for_context',
                    return_value=latest_pipeline_run,
                ),
                patch(
                    'app.eod.pykrx_backfill.'
                    'daily_pipeline_run_state_service.count_runs',
                    return_value=recovery_attempts,
                ),
                patch.object(
                    service,
                    '_preflight_untracked_followup_price_scope',
                    return_value={
                        'result': None,
                        'symbols': [],
                        'downstream_symbols': [],
                    },
                ),
                patch(
                    'app.eod.daily_pipeline_stages.'
                    'daily_pipeline_stage_service.run',
                    return_value={
                        'status': 'partial',
                        'pipeline_run_id': 582,
                    },
                ) as resume_followups,
            ):
                rearmed = service._resume_auto_daily_followups_if_due(
                    market_date='2026-10-20',
                    latest_available_trade_date='2026-10-20',
                    latest_eod_run=latest_eod_run,
                    markets=('KOSPI',),
                )

            self.assertTrue(rearmed['triggered'])
            self.assertEqual(
                rearmed['followup_recovery']['attempt'],
                recovery_attempts + 1,
            )
            self.assertEqual(
                rearmed['followup_recovery']['max_attempts'],
                8,
            )
            self.assertTrue(
                rearmed['followup_recovery'][
                    'retryable_feedback_rearm'
                ]
            )
            recovery_kwargs = resume_followups.call_args.kwargs
            self.assertEqual(recovery_kwargs['feedback_max_cycles'], 2)
            self.assertEqual(recovery_kwargs['fs_div'], 'CFS')
            self.assertEqual(
                recovery_kwargs['summary_cutoff_proof'],
                {
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': None,
                    'expected_completed_symbols': ['005930'],
                    'knowledge_cutoff_at': (
                        '2026-10-20T08:00:00+00:00'
                    ),
                    'fs_div': 'CFS',
                },
            )
            self.assertNotIn('symbols', recovery_kwargs)
            self.assertNotIn(
                'inherited_feedback_retry_config',
                recovery_kwargs,
            )

        latest_eod_run['run_metadata'].update(
            {
                'followup_recovery_required': True,
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY: {
                    'version': SAME_DAY_INTRADAY_WATCH_REARM_VERSION,
                    'status': 'pending',
                    'source_false_skip_run_id': 580,
                    'proof_pipeline_run_id': 577,
                    'target_trade_date': '2026-10-20',
                    'attempt_limit': (
                        SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT
                    ),
                    'started_at': completed_at.isoformat(),
                },
            }
        )
        with (
            patch(
                'app.eod.pykrx_backfill.'
                'daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'daily_pipeline_run_state_service.count_runs',
                return_value=8,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run'
            ) as capped_resume_followups,
            patch(
                'app.eod.pykrx_backfill.'
                'update_eod_collection_run_progress'
            ) as persist_terminal_block,
        ):
            capped = service._resume_auto_daily_followups_if_due(
                market_date='2026-10-20',
                latest_available_trade_date='2026-10-20',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI',),
            )

        self.assertFalse(capped['triggered'])
        self.assertEqual(capped['reason'], 'followup_recovery_limit_reached')
        self.assertEqual(capped['followup_recovery']['attempts'], 8)
        self.assertEqual(capped['followup_recovery']['max_attempts'], 8)
        self.assertTrue(
            capped['followup_recovery'][
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
            ]['updated']
        )
        blocked_metadata = persist_terminal_block.call_args.kwargs[
            'run_metadata'
        ]
        self.assertEqual(
            blocked_metadata[SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY][
                'status'
            ],
            'blocked',
        )
        self.assertFalse(blocked_metadata['followup_recovery_required'])
        capped_resume_followups.assert_not_called()

    def test_retired_zero_work_chain_rearms_from_contiguous_prior_signal_evidence(
        self,
    ) -> None:
        completed_at = datetime.now(timezone.utc) - timedelta(minutes=10)

        def zero_work_stage(reason: str, **extra: object) -> dict:
            return {
                'status': 'blocked',
                'processed': 0,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
                'remaining': 0,
                'source_run_ids': {},
                'details': {
                    'result_summary': {
                        'triggered': False,
                        'status': 'blocked',
                        'reason': reason,
                        **extra,
                    }
                },
            }

        zero_signal = zero_work_stage(
            'summary_knowledge_cutoff_unavailable',
            knowledge_cutoff_resolution={
                'ready': False,
                'reason': 'summary_cutoff_proof_unavailable',
                'knowledge_cutoff_at': None,
                'source_run_id': None,
                'feedback_scope_mode': None,
                'requested_symbols': None,
            },
        )
        zero_stages = {
            'signal_decider': zero_signal,
            'next_open_execution_plan': zero_work_stage(
                'signal_decider_stage_incomplete',
                upstream_status='blocked',
            ),
            'signal_decider_evaluations': zero_work_stage(
                'declared_dependencies_incomplete',
                incomplete_dependencies=['signal_decider'],
            ),
        }
        current_run = {
            'id': 580,
            'market_date': '2026-10-23',
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'current_stage': 'signal_decider_evaluations',
            'started_at': (
                completed_at - timedelta(minutes=1)
            ).isoformat(),
            'updated_at': completed_at.isoformat(),
            'completed_at': completed_at.isoformat(),
            'run_metadata': {
                'resolved_stages': [
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'resume_from_stage': 'signal_decider',
                'reused_eod_collection_run_id': 579,
                'fs_div': 'CFS',
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'point_in_time_recovery': False,
                'feedback_knowledge_cutoff': {
                    'ready': False,
                    'reason': 'summary_cutoff_proof_unavailable',
                    'knowledge_cutoff_at': None,
                    'source_run_id': None,
                    'feedback_scope_mode': None,
                    'requested_symbols': None,
                },
            },
            'stages': zero_stages,
        }
        prior_run = deepcopy(current_run)
        prior_run['id'] = 577
        prior_run['run_metadata']['feedback_knowledge_cutoff'] = {
            'ready': True,
            'reason': 'latest_completed_summary_cutoff',
            'knowledge_cutoff_at': '2026-10-23T12:16:52+00:00',
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': ['005930'],
        }
        prior_run['stages']['signal_decider'] = {
            'status': 'partial',
            'processed': 160,
            'total': 160,
            'succeeded': 147,
            'failed': 13,
            'remaining': 0,
            'source_run_ids': {
                'agent_feedback_run_id': 'decider-cycle-prior',
            },
            'details': {
                'result_summary': {
                    'run_id': 'decider-cycle-prior',
                    'prerequisites': {'ready': True},
                    'progress': {
                        'summary': {
                            'run_status': 'partial',
                            'failed_items': 13,
                            'completed_cycles': 0,
                            'cycle_number': 1,
                            'failure_groups': {
                                'openclaw_transport_error': {
                                    'count': 13,
                                    'retryable_count': 13,
                                }
                            },
                            'role_progress': {
                                'short-term-decider': {
                                    'retry_pending_symbols': 13,
                                    'in_progress_symbols': 0,
                                    'pending_symbols': 0,
                                },
                                'signal-decider': {
                                    'retry_pending_symbols': 0,
                                    'in_progress_symbols': 0,
                                    'pending_symbols': 0,
                                },
                            },
                        }
                    },
                }
            },
        }
        intervening_579 = deepcopy(current_run)
        intervening_579['id'] = 579
        intervening_578 = deepcopy(current_run)
        intervening_578['id'] = 578
        older_full_run = {
            'id': 576,
            'market_date': '2026-10-23',
            'run_metadata': {
                'reused_eod_collection_run_id': 579,
            },
            'stages': {},
        }
        prior_chain = [
            intervening_579,
            intervening_578,
            prior_run,
            older_full_run,
        ]

        self.assertTrue(
            _is_retired_decider_cutoff_zero_work_run(current_run)
        )
        selected = _select_prior_signal_rearm_evidence(
            current_pipeline_run=current_run,
            prior_pipeline_runs=prior_chain,
        )
        self.assertIsNotNone(selected)
        self.assertEqual(selected[0]['id'], 577)
        expected_proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
            'expected_completed_symbols': ['005930'],
            'knowledge_cutoff_at': '2026-10-23T12:16:52+00:00',
            'fs_div': 'CFS',
        }
        self.assertEqual(selected[1], expected_proof)

        expired_current = deepcopy(current_run)
        expired_current.update(
            {
                'status': 'skipped',
                'current_stage': 'signal_decider_evaluations',
                'completed_at': completed_at.isoformat(),
                'error_message': None,
            }
        )
        expired_current['run_metadata'].update(
            {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'point_in_time_recovery': True,
                'force': False,
                'reuse_existing_feedback': True,
                'allow_expired_analysis_replay': False,
                'same_day_intraday_watch_continuation_admitted': False,
                'feedback_knowledge_cutoff': {
                    'ready': False,
                    'reason': 'summary_cutoff_proof_unavailable',
                    'knowledge_cutoff_at': None,
                    'source_run_id': None,
                    'feedback_scope_mode': None,
                    'requested_symbols': None,
                },
            }
        )

        def expired_stage(stage_id: str) -> dict:
            return {
                'status': 'skipped',
                'processed': 0,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
                'remaining': 0,
                'source_run_ids': {},
                'details': {
                    'result_summary': {
                        'triggered': False,
                        'status': 'skipped',
                        'reason': 'next_open_window_expired',
                        'market_date': '2026-10-23',
                        'target_trade_date': '2026-10-24',
                        'current_market_date': '2026-10-24',
                        'market_state': 'OPEN',
                        'stage': stage_id,
                    }
                },
            }

        expired_current['stages'] = {
            stage_id: expired_stage(stage_id)
            for stage_id in (
                'signal_decider',
                'next_open_execution_plan',
                'signal_decider_evaluations',
            )
        }
        interrupted_prior = {
            'id': 579,
            'market_date': '2026-10-23',
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'interrupted',
            'current_stage': 'signal_decider',
            'completed_at': completed_at.isoformat(),
            'run_metadata': {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'resolved_stages': [
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'resume_from_stage': 'signal_decider',
                'reused_eod_collection_run_id': 579,
                'fs_div': 'CFS',
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'point_in_time_recovery': True,
                'force': False,
                'reuse_existing_feedback': True,
                'allow_expired_analysis_replay': False,
                'same_day_intraday_watch_continuation_admitted': True,
                'resumable': True,
                'interrupted_at': completed_at.isoformat(),
                'interrupted_reason': 'Core restarted during recovery.',
            },
            'stages': {
                'signal_decider': {
                    'status': 'interrupted',
                    'current_step': 'core_restart_interrupted',
                    'completed_at': completed_at.isoformat(),
                    'error_message': 'Core restarted during recovery.',
                    'source_run_ids': {
                        'agent_feedback_run_id': 'interrupted-cycle'
                    },
                    'details': {
                        'resumable': True,
                        'interrupted_reason': (
                            'Core restarted during recovery.'
                        ),
                    },
                }
            },
        }
        proof_prior = deepcopy(prior_run)
        proof_prior['run_metadata'].update(
            {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'point_in_time_recovery': True,
                'force': False,
                'reuse_existing_feedback': True,
                'allow_expired_analysis_replay': False,
                'same_day_intraday_watch_continuation_admitted': True,
            }
        )

        self.assertTrue(
            _is_transparent_core_restart_signal_suffix_run(
                interrupted_prior
            )
        )
        interrupted_with_own_proof = deepcopy(interrupted_prior)
        interrupted_with_own_proof['run_metadata'][
            'feedback_knowledge_cutoff'
        ] = deepcopy(
            prior_run['run_metadata']['feedback_knowledge_cutoff']
        )
        self.assertFalse(
            _is_transparent_core_restart_signal_suffix_run(
                interrupted_with_own_proof
            )
        )
        recovered_proof = _select_prior_signal_rearm_evidence(
            current_pipeline_run=expired_current,
            prior_pipeline_runs=[interrupted_prior, proof_prior],
            current_market_date='2026-10-24',
        )
        self.assertIsNotNone(recovered_proof)
        self.assertEqual(recovered_proof[0]['id'], 577)
        self.assertEqual(recovered_proof[1], expected_proof)

        false_skip_eod_run = {
            'id': 579,
            'status': 'completed',
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'followup_recovery_required': False,
            },
        }
        service = PykrxBackfillService()

        def resume_after_marker(**_kwargs: object) -> dict:
            marker = false_skip_eod_run['run_metadata'][
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
            ]
            self.assertEqual(marker['status'], 'pending')
            self.assertEqual(marker['source_false_skip_run_id'], 580)
            self.assertEqual(marker['proof_pipeline_run_id'], 577)
            self.assertTrue(
                false_skip_eod_run['run_metadata'][
                    'followup_recovery_required'
                ]
            )
            return {'status': 'partial', 'pipeline_run_id': 581}

        with (
            patch(
                'app.eod.pykrx_backfill._current_krx_calendar_date',
                return_value=date(2026, 10, 24),
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=expired_current,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=5,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'list_prior_followup_runs_for_lineage',
                return_value=[interrupted_prior, proof_prior],
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                return_value={
                    'result': None,
                    'symbols': [],
                    'downstream_symbols': [],
                },
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'update_eod_collection_run_progress'
            ) as persist_rearm,
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                side_effect=resume_after_marker,
            ) as resume_false_skip,
        ):
            false_skip_rearm = service._resume_auto_daily_followups_if_due(
                market_date='2026-10-23',
                latest_available_trade_date='2026-10-23',
                latest_eod_run=false_skip_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertTrue(false_skip_rearm['triggered'], false_skip_rearm)
        persist_rearm.assert_called_once()
        resume_false_skip.assert_called_once()
        persisted_metadata = persist_rearm.call_args.kwargs['run_metadata']
        self.assertTrue(persisted_metadata['followup_recovery_required'])
        self.assertEqual(
            persisted_metadata[SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY][
                'attempt_limit'
            ],
            8,
        )

        for field, value in (
            ('knowledge_cutoff_at', '2026-10-23T12:16:52+00:00'),
            ('source_run_id', 'unexpected'),
        ):
            malformed_current = deepcopy(current_run)
            malformed_current['run_metadata']['feedback_knowledge_cutoff'][
                field
            ] = value
            self.assertFalse(
                _is_retired_decider_cutoff_zero_work_run(
                    malformed_current
                )
            )
        for field, value in (
            ('run_id', 'unexpected-run'),
            (
                'openclaw_dispatch_circuit_breaker',
                {'retryable': True},
            ),
        ):
            malformed_current = deepcopy(current_run)
            malformed_current['stages']['signal_decider']['details'][
                'result_summary'
            ][field] = value
            self.assertFalse(
                _is_retired_decider_cutoff_zero_work_run(
                    malformed_current
                )
            )

        for mutate in (
            lambda row: row['run_metadata'].__setitem__(
                'reused_eod_collection_run_id',
                580,
            ),
            lambda row: row.__setitem__('market_date', '2026-10-22'),
            lambda row: row['run_metadata'].__setitem__('fs_div', 'OFS'),
            lambda row: row['stages']['signal_decider']['details'][
                'result_summary'
            ].__setitem__('run_id', 'intervening-explicit-run'),
        ):
            invalid_intervening = deepcopy(intervening_579)
            mutate(invalid_intervening)
            self.assertIsNone(
                _select_prior_signal_rearm_evidence(
                    current_pipeline_run=current_run,
                    prior_pipeline_runs=[
                        invalid_intervening,
                        intervening_578,
                        prior_run,
                    ],
                )
            )

        latest_eod_run = {
            'id': 579,
            'status': 'completed',
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'stage_progress': {
                    'agent_feedback_summaries': {
                        'status': 'partial',
                    }
                },
            },
        }
        service = PykrxBackfillService()
        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=current_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=5,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'list_prior_followup_runs_for_lineage',
                return_value=prior_chain,
            ) as list_prior,
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                return_value={
                    'result': None,
                    'symbols': [],
                    'downstream_symbols': [],
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'status': 'partial',
                    'pipeline_run_id': 581,
                },
            ) as resume_followups,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-10-23',
                latest_available_trade_date='2026-10-23',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['followup_recovery']['attempt'], 6)
        self.assertEqual(
            result['followup_recovery'][
                'feedback_rearm_evidence_pipeline_run_id'
            ],
            577,
        )
        list_prior.assert_called_once_with(
            market_date='2026-10-23',
            reused_eod_collection_run_id=579,
            before_pipeline_run_id=580,
        )
        recovery_kwargs = resume_followups.call_args.kwargs
        self.assertEqual(
            recovery_kwargs['summary_cutoff_proof'],
            expected_proof,
        )
        self.assertEqual(recovery_kwargs['feedback_max_cycles'], 1)
        self.assertEqual(recovery_kwargs['fs_div'], 'CFS')
        self.assertNotIn('symbols', recovery_kwargs)
        self.assertNotIn(
            'summary_cutoff_contract_repair',
            recovery_kwargs,
        )

        for attempt_count in (4, 6):
            with (
                self.subTest(attempt_count=attempt_count),
                patch(
                    'app.eod.pykrx_backfill.'
                    'daily_pipeline_run_state_service.'
                    'get_latest_run_for_context',
                    return_value=current_run,
                ),
                patch(
                    'app.eod.pykrx_backfill.'
                    'daily_pipeline_run_state_service.count_runs',
                    return_value=attempt_count,
                ),
                patch(
                    'app.eod.pykrx_backfill.'
                    'daily_pipeline_run_state_service.'
                    'list_prior_followup_runs_for_lineage',
                ) as bounded_prior_lookup,
                patch(
                    'app.eod.pykrx_backfill.'
                    'load_latest_pykrx_enrichment_repair_checkpoint',
                    return_value=None,
                ),
                patch.object(
                    service,
                    '_preflight_untracked_followup_price_scope',
                    return_value={
                        'result': None,
                        'symbols': [],
                        'downstream_symbols': [],
                    },
                ),
                patch(
                    'app.eod.daily_pipeline_stages.'
                    'daily_pipeline_stage_service.run',
                    return_value={
                        'status': 'blocked',
                        'pipeline_run_id': 581,
                    },
                ) as bounded_resume,
            ):
                bounded = service._resume_auto_daily_followups_if_due(
                    market_date='2026-10-23',
                    latest_available_trade_date='2026-10-23',
                    latest_eod_run=latest_eod_run,
                    markets=('KOSPI', 'KOSDAQ', 'KONEX'),
                )
            bounded_prior_lookup.assert_not_called()
            if attempt_count == 4:
                bounded_resume.assert_called_once()
                self.assertNotIn(
                    'summary_cutoff_proof',
                    bounded_resume.call_args.kwargs,
                )
            else:
                bounded_resume.assert_not_called()
                self.assertEqual(
                    bounded['reason'],
                    'followup_recovery_limit_reached',
                )

    def _assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
        self,
        *,
        restart_checkpoint: str,
        attempt_count: int = 6,
        shared_circuit_proof: bool = False,
        noncanonical_proof: str | None = None,
        proof_started_after_source: bool = False,
        expiry_stage_id: str | None = None,
        closed_before_call: bool = False,
    ) -> None:
        completed_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        stage_ids = AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
        expected_proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
            'expected_completed_symbols': ['005930'],
            'knowledge_cutoff_at': '2026-10-27T07:55:00+00:00',
            'fs_div': 'CFS',
        }

        def expired_stage(stage_id: str) -> dict:
            return {
                'status': 'skipped',
                'processed': 0,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
                'remaining': 0,
                'source_run_ids': {},
                'details': {
                    'result_summary': {
                        'triggered': False,
                        'status': 'skipped',
                        'reason': 'next_open_window_expired',
                        'market_date': '2026-10-27',
                        'target_trade_date': '2026-10-28',
                        'current_market_date': '2026-10-28',
                        'market_state': 'OPEN',
                        'stage': stage_id,
                    }
                },
            }

        false_skip = {
            'id': 592,
            'market_date': '2026-10-27',
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'skipped',
            'current_stage': stage_ids[-1],
            'started_at': (
                completed_at - timedelta(minutes=1)
            ).isoformat(),
            'completed_at': completed_at.isoformat(),
            'error_message': None,
            'run_metadata': {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'resolved_stages': list(stage_ids),
                'resume_from_stage': stage_ids[0],
                'reused_eod_collection_run_id': 581,
                'fs_div': 'CFS',
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'point_in_time_recovery': True,
                'force': False,
                'reuse_existing_feedback': True,
                'allow_expired_analysis_replay': False,
                'same_day_intraday_watch_continuation_admitted': False,
            },
            'stages': {
                stage_id: expired_stage(stage_id) for stage_id in stage_ids
            },
        }
        proof_run = {
            'id': 590,
            'market_date': '2026-10-27',
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'current_stage': stage_ids[-1],
            'started_at': (
                completed_at - timedelta(minutes=6)
            ).isoformat(),
            'completed_at': (
                completed_at - timedelta(minutes=5)
            ).isoformat(),
            'run_metadata': {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'resolved_stages': list(stage_ids),
                'resume_from_stage': stage_ids[0],
                'reused_eod_collection_run_id': 581,
                'fs_div': 'CFS',
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'point_in_time_recovery': True,
                'force': False,
                'reuse_existing_feedback': True,
                'allow_expired_analysis_replay': False,
                'same_day_intraday_watch_continuation_admitted': True,
                'feedback_knowledge_cutoff': {
                    'ready': True,
                    'reason': 'latest_completed_summary_cutoff',
                    'knowledge_cutoff_at': (
                        expected_proof['knowledge_cutoff_at']
                    ),
                    'source_run_id': expected_proof['source_run_id'],
                    'feedback_scope_mode': (
                        expected_proof['feedback_scope_mode']
                    ),
                    'requested_symbols': ['005930'],
                },
            },
            'stages': {
                'signal_decider': {
                    'status': 'partial',
                    'processed': 160,
                    'total': 160,
                    'succeeded': 147,
                    'failed': 13,
                    'remaining': 0,
                    'source_run_ids': {
                        'agent_feedback_run_id': 'decider-cycle-prior'
                    },
                    'details': {
                        'result_summary': {
                            'run_id': 'decider-cycle-prior',
                            'prerequisites': {'ready': True},
                            'progress': {
                                'summary': {
                                    'run_status': 'partial',
                                    'failed_items': 13,
                                    'completed_cycles': 0,
                                    'cycle_number': 1,
                                    'failure_groups': {
                                        'openclaw_transport_error': {
                                            'count': 13,
                                            'retryable_count': 13,
                                        }
                                    },
                                    'role_progress': {
                                        'short-term-decider': {
                                            'retry_pending_symbols': 13,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 0,
                                        },
                                        'signal-decider': {
                                            'retry_pending_symbols': 0,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 0,
                                        },
                                    },
                                }
                            },
                        }
                    },
                },
                'next_open_execution_plan': {
                    'status': 'blocked',
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 0,
                    'source_run_ids': {},
                    'details': {
                        'result_summary': {
                            'reason': 'signal_decider_stage_incomplete'
                        }
                    },
                },
                'signal_decider_evaluations': {
                    'status': 'blocked',
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 0,
                    'source_run_ids': {},
                    'details': {
                        'result_summary': {
                            'reason': 'declared_dependencies_incomplete',
                            'incomplete_dependencies': ['signal_decider'],
                        }
                    },
                },
            },
        }
        restart_reason = 'Core restarted during recovery.'
        current_restart = {
            'id': 593,
            'market_date': '2026-10-27',
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'interrupted',
            'current_stage': 'signal_decider',
            'started_at': (
                completed_at + timedelta(minutes=1)
            ).isoformat(),
            'completed_at': (
                completed_at + timedelta(minutes=2)
            ).isoformat(),
            'run_metadata': {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'resolved_stages': list(stage_ids),
                'resume_from_stage': stage_ids[0],
                'reused_eod_collection_run_id': 581,
                'fs_div': 'CFS',
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'point_in_time_recovery': True,
                'force': False,
                'reuse_existing_feedback': True,
                'allow_expired_analysis_replay': False,
                'same_day_intraday_watch_continuation_admitted': True,
                'resumable': True,
                'interrupted_at': (
                    completed_at + timedelta(minutes=2)
                ).isoformat(),
                'interrupted_reason': restart_reason,
            },
            'stages': {
                'signal_decider': {
                    'status': 'interrupted',
                    'current_step': 'core_restart_interrupted',
                    'completed_at': (
                        completed_at + timedelta(minutes=2)
                    ).isoformat(),
                    'error_message': restart_reason,
                    'source_run_ids': {
                        'agent_feedback_run_id': 'interrupted-cycle'
                    },
                    'details': {
                        'resumable': True,
                        'interrupted_reason': restart_reason,
                    },
                }
            },
        }
        if shared_circuit_proof:
            proof_run['stages']['signal_decider']['details'][
                'result_summary'
            ]['openclaw_dispatch_circuit_breaker'] = {
                'schema_version': (
                    OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION
                ),
                'failure_kind': 'openclaw_transport_error',
                'failure_status': 'transport/error',
                'role': 'short-term-decider',
                'retryable': True,
                'source': 'child_dispatch_circuit_breaker',
            }
        if noncanonical_proof == 'empty_failure_groups':
            proof_run['stages']['signal_decider']['details'][
                'result_summary'
            ]['progress']['summary']['failure_groups'] = {}
        elif noncanonical_proof == 'source_run_id_mismatch':
            proof_run['stages']['signal_decider']['source_run_ids'] = {
                'agent_feedback_run_id': 'different-decider-cycle'
            }
        elif noncanonical_proof is not None:
            self.fail(f'unknown noncanonical proof: {noncanonical_proof}')
        if proof_started_after_source:
            proof_run['started_at'] = (
                completed_at + timedelta(minutes=1)
            ).isoformat()
        if restart_checkpoint == 'immediately_after_start_run':
            current_restart['current_stage'] = None
            current_restart['stages'] = {}
        elif restart_checkpoint == 'after_cutoff_persisted':
            current_restart['run_metadata']['feedback_knowledge_cutoff'] = {
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': expected_proof[
                    'knowledge_cutoff_at'
                ],
                'source_run_id': expected_proof['source_run_id'],
                'feedback_scope_mode': expected_proof[
                    'feedback_scope_mode'
                ],
                'requested_symbols': ['005930'],
            }
        elif restart_checkpoint in {
            'after_signal_completed_before_plan',
            'during_plan',
            'during_evaluation',
        }:
            current_restart['run_metadata']['feedback_knowledge_cutoff'] = {
                'ready': True,
                'reason': 'latest_completed_summary_cutoff',
                'knowledge_cutoff_at': expected_proof[
                    'knowledge_cutoff_at'
                ],
                'source_run_id': expected_proof['source_run_id'],
                'feedback_scope_mode': expected_proof[
                    'feedback_scope_mode'
                ],
                'requested_symbols': ['005930'],
            }
            current_restart['current_stage'] = 'signal_decider'
            current_restart['stages'] = {
                'signal_decider': {
                    'status': 'completed',
                    'processed': 160,
                    'total': 160,
                    'succeeded': 160,
                    'failed': 0,
                    'remaining': 0,
                    'current_step': 'signal_decider_completed',
                    'completed_at': (
                        completed_at + timedelta(minutes=1)
                    ).isoformat(),
                    'error_message': None,
                    'source_run_ids': {
                        'agent_feedback_run_id': 'decider-cycle-completed'
                    },
                    'details': {
                        'result_summary': {'status': 'completed'}
                    },
                }
            }
            if restart_checkpoint in {'during_plan', 'during_evaluation'}:
                current_restart['current_stage'] = (
                    'next_open_execution_plan'
                    if restart_checkpoint == 'during_plan'
                    else 'signal_decider_evaluations'
                )
                current_restart['stages']['next_open_execution_plan'] = {
                    'status': (
                        'interrupted'
                        if restart_checkpoint == 'during_plan'
                        else 'completed'
                    ),
                    'processed': 1 if restart_checkpoint == 'during_evaluation' else 0,
                    'total': 1 if restart_checkpoint == 'during_evaluation' else 0,
                    'succeeded': 1 if restart_checkpoint == 'during_evaluation' else 0,
                    'failed': 0,
                    'remaining': 0,
                    'current_step': (
                        'core_restart_interrupted'
                        if restart_checkpoint == 'during_plan'
                        else 'next_open_execution_plan_completed'
                    ),
                    'completed_at': (
                        completed_at + timedelta(minutes=2)
                    ).isoformat(),
                    'error_message': (
                        restart_reason
                        if restart_checkpoint == 'during_plan'
                        else None
                    ),
                    'source_run_ids': (
                        {}
                        if restart_checkpoint == 'during_plan'
                        else {'plan_id': 77}
                    ),
                    'details': (
                        {
                            'resumable': True,
                            'interrupted_reason': restart_reason,
                        }
                        if restart_checkpoint == 'during_plan'
                        else {'result_summary': {'status': 'completed'}}
                    ),
                }
            if restart_checkpoint == 'during_evaluation':
                current_restart['stages'][
                    'signal_decider_evaluations'
                ] = {
                    'status': 'interrupted',
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 0,
                    'current_step': 'core_restart_interrupted',
                    'completed_at': (
                        completed_at + timedelta(minutes=2)
                    ).isoformat(),
                    'error_message': restart_reason,
                    'source_run_ids': {},
                    'details': {
                        'resumable': True,
                        'interrupted_reason': restart_reason,
                    },
                }
        elif restart_checkpoint != 'after_signal_stage_started':
            self.fail(f'unknown restart checkpoint: {restart_checkpoint}')

        post_signal_restart = restart_checkpoint in {
            'after_signal_completed_before_plan',
            'during_plan',
            'during_evaluation',
        }
        self.assertEqual(
            _is_resumable_same_day_intraday_watch_post_signal_restart(
                current_restart
            ),
            post_signal_restart,
        )
        self.assertEqual(
            _is_transparent_core_restart_signal_suffix_run(
                current_restart,
                allow_pending_marker_restart=True,
            ),
            not post_signal_restart,
        )
        if (
            restart_checkpoint != 'after_signal_stage_started'
            and not post_signal_restart
        ):
            self.assertFalse(
                _is_transparent_core_restart_signal_suffix_run(
                    current_restart
                )
            )
        latest_eod_run = {
            'id': 581,
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'followup_recovery_required': True,
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY: {
                    'version': SAME_DAY_INTRADAY_WATCH_REARM_VERSION,
                    'status': 'pending',
                    'source_false_skip_run_id': 592,
                    'proof_pipeline_run_id': 590,
                    'target_trade_date': '2026-10-28',
                    'attempt_limit': (
                        SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT
                    ),
                    'started_at': (
                        completed_at + timedelta(seconds=1)
                    ).isoformat(),
                },
            },
        }
        service = PykrxBackfillService()
        recovery_response = {
            'status': 'partial',
            'pipeline_run_id': 594,
        }
        if expiry_stage_id is not None:
            recovery_response = {
                'status': 'skipped',
                'pipeline_run_id': 594,
                'stages': {
                    expiry_stage_id: {
                        'status': 'skipped',
                        'details': {
                            'result_summary': {
                                'reason': 'next_open_window_expired',
                                'stage': expiry_stage_id,
                            }
                        },
                    }
                },
            }
        recovery_side_effect = (
            ValueError(
                'same-day intraday-watch rearm resume is outside its '
                'target PREOPEN/OPEN session'
            )
            if closed_before_call
            else None
        )

        with (
            patch(
                'app.eod.pykrx_backfill._current_krx_calendar_date',
                return_value=date(2026, 10, 28),
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=current_restart,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=attempt_count,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'list_prior_followup_runs_for_lineage',
                return_value=[false_skip, proof_run],
            ) as list_prior,
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch.object(
                service,
                '_resolve_point_in_time_followup_membership',
                return_value=(
                    ['005930'],
                    ('KOSPI', 'KOSDAQ', 'KONEX'),
                ),
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                return_value={
                    'verified': True,
                    'symbols': [],
                    'downstream_symbols': [],
                },
            ) as preflight_price_scope,
            patch(
                'app.eod.pykrx_backfill.'
                'update_eod_collection_run_progress'
            ) as persist_terminal_block,
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value=recovery_response,
                side_effect=recovery_side_effect,
            ) as resume_suffix,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-10-27',
                latest_available_trade_date='2026-10-27',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        if attempt_count > SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT:
            self.assertFalse(result['triggered'])
            list_prior.assert_not_called()
            preflight_price_scope.assert_not_called()
            resume_suffix.assert_not_called()
            self.assertEqual(result['reason'], 'followup_recovery_limit_reached')
            return
        list_prior.assert_called_once()
        if proof_started_after_source:
            self.assertFalse(result['triggered'])
            self.assertEqual(
                result['reason'], 'followup_recovery_limit_reached'
            )
            preflight_price_scope.assert_not_called()
            resume_suffix.assert_not_called()
            return
        if shared_circuit_proof or noncanonical_proof is not None:
            self.assertFalse(result['triggered'])
            self.assertEqual(
                result['reason'], 'followup_rearm_proof_not_canonical'
            )
            preflight_price_scope.assert_not_called()
            resume_suffix.assert_not_called()
            return
        if closed_before_call:
            self.assertFalse(result['triggered'])
        else:
            self.assertTrue(result['triggered'])
        if post_signal_restart:
            preflight_price_scope.assert_not_called()
        else:
            preflight_price_scope.assert_called_once()
        resume_suffix.assert_called_once()
        recovery_kwargs = resume_suffix.call_args.kwargs
        self.assertEqual(recovery_kwargs['stages'], list(stage_ids))
        self.assertEqual(recovery_kwargs['resume_from_stage'], stage_ids[0])
        self.assertEqual(
            recovery_kwargs['trigger'], AUTO_FOLLOWUP_RECOVERY_TRIGGER
        )
        self.assertEqual(recovery_kwargs['reused_eod_collection_run_id'], 581)
        self.assertTrue(recovery_kwargs['point_in_time_recovery'])
        self.assertFalse(recovery_kwargs['force'])
        self.assertTrue(recovery_kwargs['reuse_existing_feedback'])
        self.assertEqual(
            recovery_kwargs['summary_cutoff_proof'], expected_proof
        )
        if expiry_stage_id is not None or closed_before_call:
            persist_terminal_block.assert_called_once()
            marker = latest_eod_run['run_metadata'][
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
            ]
            self.assertEqual(marker['status'], 'blocked')
            self.assertEqual(
                marker['blocked_reason'], 'next_open_window_expired'
            )
            self.assertFalse(
                latest_eod_run['run_metadata'][
                    'followup_recovery_required'
                ]
            )
            self.assertTrue(
                result['followup_recovery'][
                    'same_day_intraday_watch_rearm_terminal_expiry'
                ]
            )
            self.assertIsNone(
                pending_same_day_intraday_watch_rearm_marker(
                    marker,
                    target_trade_date=date(2026, 10, 29),
                )
            )
            return
        persist_terminal_block.assert_not_called()
        self.assertNotIn('agent_feedback_summaries', recovery_kwargs['stages'])
        self.assertNotIn('symbols', recovery_kwargs)
        if post_signal_restart:
            self.assertEqual(recovery_kwargs['existing_pipeline_run_id'], 593)
            self.assertTrue(
                recovery_kwargs['same_day_intraday_watch_rearm_resume']
            )
            self.assertEqual(
                result['followup_recovery']['attempt'], attempt_count
            )
            self.assertEqual(
                result['followup_recovery']['max_attempts'],
                SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT,
            )
        else:
            self.assertNotIn('existing_pipeline_run_id', recovery_kwargs)

    def test_pending_rearm_retries_exact_signal_suffix_after_core_restart(
        self,
    ) -> None:
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='after_signal_stage_started',
        )

    def test_pending_rearm_retries_after_restart_immediately_after_start_run(
        self,
    ) -> None:
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='immediately_after_start_run',
        )

    def test_pending_rearm_retries_after_cutoff_persisted_signal_restart(
        self,
    ) -> None:
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='after_cutoff_persisted',
        )

    def test_pending_rearm_resumes_after_signal_before_plan_without_replay(
        self,
    ) -> None:
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='after_signal_completed_before_plan',
        )

    def test_pending_rearm_resumes_interrupted_plan_without_signal_replay(
        self,
    ) -> None:
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='during_plan',
        )

    def test_pending_rearm_resumes_interrupted_evaluation_without_replay(
        self,
    ) -> None:
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='during_evaluation',
        )

    def test_pending_rearm_post_signal_allows_attempt_eight_not_nine(
        self,
    ) -> None:
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='after_signal_completed_before_plan',
            attempt_count=8,
        )
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='after_signal_completed_before_plan',
            attempt_count=9,
        )

    def test_same_day_marker_rejects_shared_dispatch_circuit_proof(
        self,
    ) -> None:
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='after_signal_stage_started',
            shared_circuit_proof=True,
        )

    def test_same_day_marker_writer_rejects_noncanonical_signal_proof(
        self,
    ) -> None:
        for noncanonical_proof in (
            'empty_failure_groups',
            'source_run_id_mismatch',
        ):
            with self.subTest(noncanonical_proof=noncanonical_proof):
                self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
                    restart_checkpoint='after_signal_stage_started',
                    noncanonical_proof=noncanonical_proof,
                )

    def test_same_day_marker_writer_rejects_late_started_proof(self) -> None:
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='after_signal_stage_started',
            proof_started_after_source=True,
        )

    def test_pending_rearm_terminal_blocks_expiry_at_each_suffix_stage(
        self,
    ) -> None:
        for restart_checkpoint, expiry_stage_id in (
            ('after_signal_stage_started', 'signal_decider'),
            (
                'after_signal_completed_before_plan',
                'next_open_execution_plan',
            ),
            ('during_evaluation', 'signal_decider_evaluations'),
        ):
            with self.subTest(expiry_stage_id=expiry_stage_id):
                self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
                    restart_checkpoint=restart_checkpoint,
                    expiry_stage_id=expiry_stage_id,
                )

    def test_pending_rearm_terminal_blocks_closed_before_in_place_resume(
        self,
    ) -> None:
        self._assert_pending_rearm_retries_exact_signal_suffix_after_core_restart(
            restart_checkpoint='after_signal_completed_before_plan',
            closed_before_call=True,
        )

    def test_pending_rearm_rejects_conflicting_intermediate_restart_cutoff(
        self,
    ) -> None:
        completed_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        stage_ids = AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
        exact_metadata = {
            'entrypoint': 'daily_pipeline_stage_service.run',
            'resolved_stages': list(stage_ids),
            'resume_from_stage': stage_ids[0],
            'reused_eod_collection_run_id': 581,
            'fs_div': 'CFS',
            'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
            'point_in_time_recovery': True,
            'force': False,
            'reuse_existing_feedback': True,
            'allow_expired_analysis_replay': False,
            'same_day_intraday_watch_continuation_admitted': True,
        }
        proof_cutoff = {
            'ready': True,
            'reason': 'latest_completed_summary_cutoff',
            'knowledge_cutoff_at': '2026-10-27T07:55:00+00:00',
            'source_run_id': 'feedback-cycle-proof',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': ['005930'],
        }
        conflicting_cutoff = {
            'ready': True,
            'reason': 'latest_completed_summary_cutoff',
            'knowledge_cutoff_at': '2026-10-27T07:56:00+00:00',
            'source_run_id': 'feedback-cycle-conflict',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': ['000660'],
        }

        proof_run = {
            'id': 590,
            'market_date': '2026-10-27',
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'current_stage': stage_ids[-1],
            'completed_at': (
                completed_at - timedelta(minutes=5)
            ).isoformat(),
            'run_metadata': {
                **exact_metadata,
                'feedback_knowledge_cutoff': proof_cutoff,
            },
            'stages': {
                'signal_decider': {
                    'status': 'partial',
                    'processed': 1,
                    'total': 1,
                    'succeeded': 0,
                    'failed': 1,
                    'remaining': 0,
                    'source_run_ids': {
                        'agent_feedback_run_id': 'feedback-cycle-proof'
                    },
                    'details': {
                        'result_summary': {
                            'run_id': 'feedback-cycle-proof',
                            'prerequisites': {'ready': True},
                            'progress': {
                                'summary': {
                                    'run_status': 'partial',
                                    'failed_items': 1,
                                    'failure_groups': {
                                        'openclaw_transport_error': {
                                            'count': 1,
                                            'retryable_count': 1,
                                        }
                                    },
                                    'role_progress': {
                                        'signal-decider': {
                                            'retry_pending_symbols': 1,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 0,
                                        }
                                    },
                                }
                            },
                        }
                    },
                },
                'next_open_execution_plan': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'reason': 'signal_decider_stage_incomplete'
                        }
                    },
                },
                'signal_decider_evaluations': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'reason': 'declared_dependencies_incomplete',
                            'incomplete_dependencies': ['signal_decider'],
                        }
                    },
                },
            },
        }

        def expired_stage(stage_id: str) -> dict:
            return {
                'status': 'skipped',
                'processed': 0,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
                'remaining': 0,
                'source_run_ids': {},
                'details': {
                    'result_summary': {
                        'triggered': False,
                        'status': 'skipped',
                        'reason': 'next_open_window_expired',
                        'market_date': '2026-10-27',
                        'target_trade_date': '2026-10-28',
                        'current_market_date': '2026-10-28',
                        'market_state': 'OPEN',
                        'stage': stage_id,
                    }
                },
            }

        false_skip = {
            'id': 592,
            'market_date': '2026-10-27',
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'skipped',
            'current_stage': stage_ids[-1],
            'completed_at': completed_at.isoformat(),
            'error_message': None,
            'run_metadata': {
                **exact_metadata,
                'same_day_intraday_watch_continuation_admitted': False,
            },
            'stages': {
                stage_id: expired_stage(stage_id) for stage_id in stage_ids
            },
        }
        restart_reason = 'Core restarted during recovery.'
        intermediate_restart = {
            'id': 593,
            'market_date': '2026-10-27',
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'interrupted',
            'current_stage': 'signal_decider',
            'completed_at': (
                completed_at + timedelta(minutes=1)
            ).isoformat(),
            'run_metadata': {
                **exact_metadata,
                'feedback_knowledge_cutoff': conflicting_cutoff,
                'resumable': True,
                'interrupted_at': (
                    completed_at + timedelta(minutes=1)
                ).isoformat(),
                'interrupted_reason': restart_reason,
            },
            'stages': {
                'signal_decider': {
                    'status': 'interrupted',
                    'processed': 0,
                    'total': 1,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 1,
                    'current_step': 'core_restart_interrupted',
                    'completed_at': (
                        completed_at + timedelta(minutes=1)
                    ).isoformat(),
                    'error_message': restart_reason,
                    'source_run_ids': {},
                    'details': {
                        'resumable': True,
                        'interrupted_reason': restart_reason,
                    },
                }
            },
        }
        current_restart = {
            'id': 594,
            'market_date': '2026-10-27',
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'interrupted',
            'current_stage': None,
            'completed_at': (
                completed_at + timedelta(minutes=2)
            ).isoformat(),
            'run_metadata': {
                **exact_metadata,
                'resumable': True,
                'interrupted_at': (
                    completed_at + timedelta(minutes=2)
                ).isoformat(),
                'interrupted_reason': restart_reason,
            },
            'stages': {},
        }
        prior_runs = [intermediate_restart, false_skip, proof_run]
        latest_eod_run = {
            'id': 581,
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI', 'KOSDAQ', 'KONEX'],
                'followup_recovery_required': True,
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY: {
                    'version': SAME_DAY_INTRADAY_WATCH_REARM_VERSION,
                    'status': 'pending',
                    'source_false_skip_run_id': 592,
                    'proof_pipeline_run_id': 590,
                    'target_trade_date': '2026-10-28',
                    'attempt_limit': (
                        SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT
                    ),
                    'started_at': (
                        completed_at + timedelta(seconds=1)
                    ).isoformat(),
                },
            },
        }

        selected = _select_prior_signal_rearm_evidence(
            current_pipeline_run=current_restart,
            prior_pipeline_runs=prior_runs,
            current_market_date=date(2026, 10, 28),
            allow_transparent_current=True,
        )
        service = PykrxBackfillService()
        with (
            patch(
                'app.eod.pykrx_backfill._current_krx_calendar_date',
                return_value=date(2026, 10, 28),
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=current_restart,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=7,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'list_prior_followup_runs_for_lineage',
                return_value=prior_runs,
            ) as list_prior,
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                return_value={
                    'verified': True,
                    'symbols': [],
                    'downstream_symbols': [],
                },
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'update_eod_collection_run_progress'
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'status': 'partial',
                    'pipeline_run_id': 595,
                },
            ) as resume_suffix,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-10-27',
                latest_available_trade_date='2026-10-27',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        list_prior.assert_called_once()
        self.assertEqual(
            {
                'prior_proof_rejected': selected is None,
                'recovery_triggered': result['triggered'],
                'recovery_call_count': resume_suffix.call_count,
            },
            {
                'prior_proof_rejected': True,
                'recovery_triggered': False,
                'recovery_call_count': 0,
            },
        )

    def test_pending_same_day_rearm_is_not_completed_by_zero_work_expiration(
        self,
    ) -> None:
        service = PykrxBackfillService()
        marker = {
            'version': SAME_DAY_INTRADAY_WATCH_REARM_VERSION,
            'status': 'pending',
            'source_false_skip_run_id': 592,
            'proof_pipeline_run_id': 590,
            'target_trade_date': '2026-10-28',
            'attempt_limit': SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT,
            'started_at': '2026-10-28T01:45:00+00:00',
        }
        eod_run = {
            'id': 581,
            'status': 'completed',
            'symbols_total': 0,
            'symbols_succeeded': 0,
            'symbols_failed': 0,
            'trade_date': '2026-10-27',
            'run_metadata': {
                'followup_recovery_required': True,
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY: marker,
            },
        }

        def expired_stage(stage_id: str) -> dict:
            return {
                'status': 'skipped',
                'processed': 0,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
                'remaining': 0,
                'source_run_ids': {},
                'details': {
                    'result_summary': {
                        'triggered': False,
                        'status': 'skipped',
                        'reason': 'next_open_window_expired',
                        'market_date': '2026-10-27',
                        'target_trade_date': '2026-10-28',
                        'current_market_date': '2026-10-28',
                        'market_state': 'OPEN',
                        'stage': stage_id,
                    }
                },
            }

        recovery_result = {
            'status': 'skipped',
            'reason': 'next_open_window_expired',
            'pipeline_run_id': 593,
            'stages': {
                stage_id: expired_stage(stage_id)
                for stage_id in AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
            },
        }

        with patch(
            'app.eod.pykrx_backfill.update_eod_collection_run_progress'
        ) as persist_progress:
            result = service._persist_successful_followup_recovery(
                latest_eod_run=eod_run,
                recovery_result=recovery_result,
            )

        self.assertFalse(result['updated'])
        self.assertEqual(
            eod_run['run_metadata'][
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
            ]['status'],
            'pending',
        )
        self.assertTrue(
            eod_run['run_metadata']['followup_recovery_required']
        )
        persist_progress.assert_not_called()

    def test_stale_pending_rearm_is_blocked_without_replaying_decisions(
        self,
    ) -> None:
        service = PykrxBackfillService()
        eod_run = {
            'id': 581,
            'status': 'completed',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'trade_date': '2026-10-27',
            'run_metadata': {
                'markets': ['KOSPI', 'KOSDAQ'],
                'followup_recovery_required': True,
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY: {
                    'version': SAME_DAY_INTRADAY_WATCH_REARM_VERSION,
                    'status': 'pending',
                    'source_false_skip_run_id': 592,
                    'proof_pipeline_run_id': 590,
                    'target_trade_date': '2026-10-28',
                    'attempt_limit': (
                        SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT
                    ),
                    'started_at': '2026-10-28T01:45:00+00:00',
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill._current_krx_calendar_date',
                return_value=date(2026, 10, 29),
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context'
            ) as load_pipeline_run,
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run'
            ) as run_stages,
            patch(
                'app.eod.pykrx_backfill.'
                'update_eod_collection_run_progress'
            ) as persist_terminal_block,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-10-27',
                latest_available_trade_date='2026-10-27',
                latest_eod_run=eod_run,
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=True,
            )

        self.assertEqual(result['reason'], 'next_open_window_expired')
        self.assertEqual(
            result['followup_recovery']['blocker'],
            'same_day_intraday_watch_rearm_terminal_expiry',
        )
        marker = eod_run['run_metadata'][
            SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
        ]
        self.assertEqual(marker['status'], 'blocked')
        self.assertEqual(marker['blocked_reason'], 'next_open_window_expired')
        self.assertFalse(
            eod_run['run_metadata']['followup_recovery_required']
        )
        persist_terminal_block.assert_called_once()
        load_pipeline_run.assert_not_called()
        run_stages.assert_not_called()

    def test_same_day_intraday_watch_rearm_marker_lifecycle_is_durable(
        self,
    ) -> None:
        service = PykrxBackfillService()
        eod_run = {
            'id': 581,
            'status': 'completed',
            'symbols_total': 0,
            'symbols_succeeded': 0,
            'symbols_failed': 0,
            'trade_date': '2026-10-27',
            'run_metadata': {},
        }

        with patch(
            'app.eod.pykrx_backfill.update_eod_collection_run_progress'
        ) as persist_progress:
            first = service._persist_same_day_intraday_watch_rearm_pending(
                latest_eod_run=eod_run,
                source_false_skip_run_id=592,
                proof_pipeline_run_id=590,
                target_trade_date=date(2026, 10, 28),
            )
            started_at = eod_run['run_metadata'][
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
            ]['started_at']
            second = service._persist_same_day_intraday_watch_rearm_pending(
                latest_eod_run=eod_run,
                source_false_skip_run_id=592,
                proof_pipeline_run_id=590,
                target_trade_date=date(2026, 10, 28),
            )

        self.assertTrue(first['updated'])
        self.assertFalse(second['updated'])
        self.assertEqual(second['reason'], 'rearm_marker_already_pending')
        self.assertEqual(
            eod_run['run_metadata'][
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
            ][
                'started_at'
            ],
            started_at,
        )
        self.assertTrue(eod_run['run_metadata']['followup_recovery_required'])
        self.assertEqual(persist_progress.call_count, 2)

        with patch(
            'app.eod.pykrx_backfill.update_eod_collection_run_progress'
        ) as persist_block:
            blocked = service._block_same_day_intraday_watch_rearm(
                latest_eod_run=eod_run,
                reason='followup_recovery_limit_reached',
            )

        self.assertTrue(blocked['updated'])
        self.assertEqual(
            eod_run['run_metadata'][
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
            ][
                'status'
            ],
            'blocked',
        )
        self.assertFalse(eod_run['run_metadata']['followup_recovery_required'])
        persist_block.assert_called_once()

        completion_run = deepcopy(eod_run)
        completion_run['run_metadata'][
            SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
        ] = {
            **completion_run['run_metadata'][
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
            ],
            'status': 'pending',
        }
        completion_run['run_metadata']['followup_recovery_required'] = True
        completion_result = {
            'status': 'completed',
            'pipeline_run_id': 593,
            'resolved_stages': list(
                AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
            ),
            'pipeline_run': {
                'id': 593,
                'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
                'status': 'completed',
                'current_stage': (
                    AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[-1]
                ),
                'run_metadata': {
                    'entrypoint': 'daily_pipeline_stage_service.run',
                    'resolved_stages': list(
                        AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
                    ),
                    'resume_from_stage': (
                        AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[0]
                    ),
                    'reused_eod_collection_run_id': 581,
                    'point_in_time_recovery': True,
                    'force': False,
                    'reuse_existing_feedback': True,
                    'allow_expired_analysis_replay': False,
                    'same_day_intraday_watch_continuation_admitted': True,
                },
            },
            'stages': {
                stage_id: {'status': 'completed'}
                for stage_id in (
                    AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
                )
            },
        }
        with patch(
            'app.eod.pykrx_backfill.update_eod_collection_run_progress'
        ) as persist_completion:
            completed = service._persist_successful_followup_recovery(
                latest_eod_run=completion_run,
                recovery_result=completion_result,
            )

        self.assertTrue(completed['updated'])
        completed_metadata = persist_completion.call_args.kwargs[
            'run_metadata'
        ]
        completed_marker = completed_metadata[
            SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
        ]
        self.assertEqual(completed_marker['status'], 'completed')
        self.assertEqual(completed_marker['completion_pipeline_run_id'], 593)
        self.assertFalse(completed_metadata['followup_recovery_required'])
        persist_completion.assert_called_once()

    def test_feedback_provider_rate_limit_waits_until_retry_deadline(
        self,
    ) -> None:
        service = PykrxBackfillService()
        now = datetime.now(timezone.utc)
        retry_not_before = now + timedelta(minutes=10)
        completed_at = now - timedelta(minutes=10)
        pipeline_run = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'updated_at': completed_at.isoformat(),
            'completed_at': completed_at.isoformat(),
            'run_metadata': {
                'resolved_stages': [
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'reused_eod_collection_run_id': 7,
            },
            'stages': {
                'agent_feedback_summaries': {
                    'status': 'failed',
                    'details': {
                        'result_summary': {
                            'run_id': 'feedback-cycle-rate-limited',
                            'prerequisites': {'ready': True},
                            'openclaw_dispatch_circuit_breaker': {
                                'schema_version': (
                                    OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION
                                ),
                                'failure_kind': (
                                    'openclaw_provider_rate_limited'
                                ),
                                'failure_status': 'provider/rate-limit',
                                'retryable': True,
                                'source': 'child_dispatch_circuit_breaker',
                                'retry_after_seconds': 1200,
                                'retry_not_before': (
                                    retry_not_before.isoformat()
                                ),
                                'retry_after_source': (
                                    'retry-after-seconds'
                                ),
                            },
                            'progress': {
                                'summary': {
                                    'run_status': 'failed',
                                    'failed_items': 1,
                                    'completed_cycles': 0,
                                    'cycle_number': 1,
                                    'role_progress': {
                                        'eod-summarizer': {
                                            'retry_pending_symbols': 1,
                                            'in_progress_symbols': 0,
                                            'pending_symbols': 0,
                                        }
                                    },
                                }
                            },
                        }
                    },
                },
                'signal_decider': {'status': 'blocked'},
                'next_open_execution_plan': {'status': 'blocked'},
                'signal_decider_evaluations': {'status': 'blocked'},
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=1,
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run'
            ) as resume_followups,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=False,
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(
            result['reason'],
            'followup_feedback_provider_backoff',
        )
        recovery = result['followup_recovery']
        self.assertTrue(recovery['retry_scheduled'])
        self.assertTrue(recovery['retryable_feedback_rearm'])
        self.assertEqual(recovery['provider'], 'openclaw')
        self.assertEqual(recovery['provider_status'], 'rate_limited')
        self.assertGreater(recovery['retry_after_seconds'], 0)
        self.assertLessEqual(recovery['retry_after_seconds'], 600)
        self.assertEqual(
            recovery['retry_not_before'],
            retry_not_before.isoformat(),
        )
        self.assertEqual(
            recovery['retry_after_source'],
            'retry-after-seconds',
        )
        resume_followups.assert_not_called()

    def test_feedback_provider_backoff_caps_untrusted_deadline_from_fixed_anchor(
        self,
    ) -> None:
        completed_at = datetime(2026, 4, 1, 8, tzinfo=timezone.utc)
        now = completed_at + timedelta(days=8)
        marker = {
            'schema_version': (
                OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION
            ),
            'failure_kind': 'openclaw_provider_rate_limited',
            'failure_status': 'provider/rate-limit',
            'retryable': True,
            'source': 'child_dispatch_circuit_breaker',
            'retry_after_seconds': float('inf'),
            'retry_not_before': '2099-01-01T00:00:00+00:00',
            'retry_after_source': 'retry-after-seconds',
        }

        backoff = _feedback_dispatch_rate_limit_backoff(
            marker=marker,
            pipeline_run={'completed_at': completed_at.isoformat()},
            now=now,
        )

        self.assertIsNotNone(backoff)
        self.assertFalse(backoff['active'])
        self.assertEqual(backoff['retry_after_seconds'], 0)
        self.assertEqual(
            backoff['retry_not_before'],
            (completed_at + timedelta(days=7)).isoformat(),
        )

    def test_feedback_dispatch_circuit_never_falls_back_to_broad_recovery(
        self,
    ) -> None:
        service = PykrxBackfillService()
        completed_at = datetime.now(timezone.utc) - timedelta(minutes=10)
        base_marker = {
            'schema_version': (
                OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION
            ),
            'failure_kind': 'openclaw_plugin_state_capacity_exhausted',
            'failure_status': 'capacity/exhausted',
            'retryable': False,
            'source': 'child_dispatch_circuit_breaker',
        }

        for marker, blocker in (
            (base_marker, 'non_retryable_dispatch_circuit_breaker'),
            (
                {
                    **base_marker,
                    'schema_version': 'future_v2',
                    'failure_kind': 'openclaw_transport_error',
                    'retryable': True,
                },
                'invalid_dispatch_circuit_breaker',
            ),
        ):
            pipeline_run = {
                'id': 96,
                'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
                'status': 'failed',
                'updated_at': completed_at.isoformat(),
                'completed_at': completed_at.isoformat(),
                'run_metadata': {
                    'resolved_stages': [
                        'agent_feedback_summaries',
                        'signal_decider',
                        'next_open_execution_plan',
                        'signal_decider_evaluations',
                    ],
                    'reused_eod_collection_run_id': 7,
                },
                'stages': {
                    'agent_feedback_summaries': {
                        'status': 'failed',
                        'details': {
                            'result_summary': {
                                'run_id': 'feedback-cycle-circuit-blocked',
                                'prerequisites': {'ready': True},
                                'openclaw_dispatch_circuit_breaker': marker,
                                'progress': {
                                    'summary': {
                                        'run_status': 'failed',
                                        'failed_items': 1,
                                        'role_progress': {
                                            'eod-summarizer': {
                                                'retry_pending_symbols': 1,
                                                'in_progress_symbols': 0,
                                                'pending_symbols': 0,
                                            }
                                        },
                                    }
                                },
                            }
                        },
                    },
                    'signal_decider': {'status': 'blocked'},
                    'next_open_execution_plan': {'status': 'blocked'},
                    'signal_decider_evaluations': {'status': 'blocked'},
                },
            }
            with (
                self.subTest(marker=marker),
                patch(
                    'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                    'get_latest_run_for_context',
                    return_value=pipeline_run,
                ),
                patch(
                    'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                    'count_runs',
                    return_value=1,
                ),
                patch(
                    'app.eod.daily_pipeline_stages.'
                    'daily_pipeline_stage_service.run'
                ) as resume_followups,
            ):
                result = service._resume_auto_daily_followups_if_due(
                    market_date='2026-04-03',
                    latest_available_trade_date='2026-04-03',
                    latest_eod_run={
                        'id': 7,
                        'status': 'completed',
                        'symbols_failed': 0,
                    },
                    markets=('KOSPI', 'KOSDAQ'),
                    point_in_time_recovery=False,
                )

            self.assertFalse(result['triggered'])
            self.assertEqual(
                result['reason'],
                'followup_feedback_dispatch_circuit_blocked',
            )
            self.assertEqual(
                result['followup_recovery']['blocker'],
                blocker,
            )
            resume_followups.assert_not_called()

        no_marker_run = deepcopy(pipeline_run)
        no_marker_summary = no_marker_run['stages'][
            'agent_feedback_summaries'
        ]['details']['result_summary']
        del no_marker_summary['openclaw_dispatch_circuit_breaker']
        no_marker_summary['progress']['summary']['failed_items'] = 0
        no_marker_role = no_marker_summary['progress']['summary'][
            'role_progress'
        ]['eod-summarizer']
        no_marker_role['retry_pending_symbols'] = 2
        no_marker_role['pending_symbols'] = 2
        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=no_marker_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=1,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                return_value={
                    'result': None,
                    'symbols': [],
                    'downstream_symbols': [],
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'status': 'partial',
                    'pipeline_run_id': 97,
                },
            ) as legacy_resume,
        ):
            legacy_result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=False,
            )

        self.assertTrue(legacy_result['triggered'])
        self.assertEqual(
            legacy_result['reason'],
            'incomplete_followups_resumed',
        )
        self.assertNotIn('symbols', legacy_resume.call_args.kwargs)
        self.assertNotIn(
            'inherited_feedback_retry_config',
            legacy_resume.call_args.kwargs,
        )

    def test_dart_provider_maintenance_waits_then_rearms_after_recovery_limit(
        self,
    ) -> None:
        service = PykrxBackfillService()

        def provider_outage_run(*, completed_at: datetime) -> dict:
            return {
                'id': 96,
                'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
                'status': 'blocked',
                'updated_at': completed_at.isoformat(),
                'completed_at': completed_at.isoformat(),
                'run_metadata': {
                    'resolved_stages': ['fundamentals_daily_refresh'],
                    'reused_eod_collection_run_id': 7,
                },
                'stages': {
                    'fundamentals_daily_refresh': {
                        'status': 'partial',
                        'details': {
                            'result_summary': {
                                'samples': {
                                    'lookup_errors': [
                                        {
                                            'error_type': 'DartAPIError',
                                            'error': (
                                                'DART request failed [800]: '
                                                'Maintenance'
                                            ),
                                        }
                                    ]
                                }
                            }
                        },
                    }
                },
            }

        latest_eod_run = {
            'id': 7,
            'status': 'completed',
            'symbols_failed': 0,
            'run_metadata': {'markets': ['KOSPI', 'KOSDAQ']},
        }
        recent_run = provider_outage_run(
            completed_at=datetime.now(timezone.utc) - timedelta(minutes=10)
        )
        self.assertTrue(_is_dart_provider_outage_rearm_candidate(recent_run))

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=recent_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=5,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run'
            ) as resume_followups,
        ):
            waiting = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
            )

        self.assertEqual(
            waiting['reason'],
            'followup_recovery_provider_backoff',
        )
        self.assertTrue(waiting['followup_recovery']['retry_scheduled'])
        self.assertGreater(
            waiting['followup_recovery']['retry_after_seconds'],
            0,
        )
        resume_followups.assert_not_called()

        expired_run = provider_outage_run(
            completed_at=(
                datetime.now(timezone.utc)
                - timedelta(
                    seconds=(
                        AUTO_FOLLOWUP_PROVIDER_OUTAGE_RETRY_INTERVAL_SECONDS
                        + 60
                    )
                )
            )
        )
        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=expired_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=5,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                return_value={
                    'result': None,
                    'symbols': [],
                    'downstream_symbols': [],
                },
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'status': 'partial',
                    'pipeline_run_id': 97,
                },
            ) as resume_followups,
        ):
            resumed = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
            )

        self.assertTrue(resumed['triggered'])
        self.assertEqual(resumed['followup_recovery']['attempt'], 6)
        self.assertTrue(
            resumed['followup_recovery']['provider_outage_rearm']
        )
        resume_followups.assert_called_once()

    def test_cutoff_contract_block_gets_one_repair_attempt_after_limit(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_pipeline_run = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'current_stage': 'signal_decider_evaluations',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': [
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'reused_eod_collection_run_id': 7,
                'resume_from_stage': 'signal_decider',
                'feedback_knowledge_cutoff': {
                    'ready': False,
                    'reason': 'summary_run_missing',
                    'source_run_id': None,
                    'feedback_scope_mode': 'broad_feedback_universe',
                },
                'stage_statuses': {
                    'signal_decider': 'blocked',
                    'next_open_execution_plan': 'blocked',
                    'signal_decider_evaluations': 'blocked',
                },
            },
            'stages': {
                'signal_decider': {
                    'status': 'blocked',
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 0,
                    'source_run_ids': {},
                    'details': {
                        'result_summary': {
                            'triggered': False,
                            'status': 'blocked',
                            'reason': (
                                'summary_knowledge_cutoff_unavailable'
                            ),
                            'knowledge_cutoff_resolution': {
                                'ready': False,
                                'reason': 'summary_run_missing',
                                'source_run_id': None,
                                'feedback_scope_mode': (
                                    'broad_feedback_universe'
                                ),
                            },
                        }
                    },
                },
                'next_open_execution_plan': {
                    'status': 'blocked',
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 0,
                    'source_run_ids': {},
                    'details': {
                        'result_summary': {
                            'triggered': False,
                            'status': 'blocked',
                            'reason': 'signal_decider_stage_incomplete',
                            'upstream_status': 'blocked',
                        }
                    },
                },
                'signal_decider_evaluations': {
                    'status': 'blocked',
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 0,
                    'source_run_ids': {},
                    'details': {
                        'result_summary': {
                            'triggered': False,
                            'status': 'blocked',
                            'reason': 'declared_dependencies_incomplete',
                            'incomplete_dependencies': ['signal_decider'],
                        }
                    },
                },
            },
        }
        latest_eod_run = {
            'id': 7,
            'status': 'completed',
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': ['005930'],
                'stage_progress': {
                    'agent_feedback_summaries': {
                        'status': 'completed',
                        'source_run_ids': {
                            'eod_run_id': 7,
                            'agent_feedback_run_id': (
                                'feedback-cycle-summary'
                            ),
                        },
                        'result_summary': {
                            'status': 'completed',
                            'run_id': 'feedback-cycle-summary',
                            'feedback_scope_mode': (
                                'decider_scope_fast_path'
                            ),
                            'requested_symbols': None,
                        },
                    }
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=5,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=None,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                side_effect=AssertionError(
                    'cutoff repair must reuse the proven summary scope'
                ),
            ),
            patch.object(
                service,
                '_resolve_point_in_time_followup_membership',
                side_effect=AssertionError(
                    'cutoff repair must not rebuild historical membership'
                ),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'status': 'blocked',
                    'pipeline_run_id': 97,
                },
            ) as resume_followups,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI',),
                point_in_time_recovery=True,
            )

        self.assertTrue(result['triggered'])
        self.assertTrue(
            result['followup_recovery']['cutoff_contract_repair']
        )
        self.assertEqual(result['followup_recovery']['attempt'], 6)
        self.assertEqual(result['followup_recovery']['max_attempts'], 6)
        resume_followups.assert_called_once()
        recovery_call = resume_followups.call_args.kwargs
        self.assertEqual(recovery_call['resume_from_stage'], 'signal_decider')
        self.assertTrue(
            recovery_call['summary_cutoff_contract_repair']
        )
        self.assertTrue(recovery_call['point_in_time_recovery'])
        self.assertEqual(
            recovery_call['summary_cutoff_proof'],
            {
                'source_run_id': 'feedback-cycle-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': None,
            },
        )

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=6,
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
            ) as second_resume,
        ):
            second = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI',),
            )

        self.assertFalse(second['triggered'])
        self.assertEqual(second['reason'], 'followup_recovery_limit_reached')
        self.assertEqual(second['followup_recovery']['max_attempts'], 6)
        second_resume.assert_not_called()

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=8,
            ),
        ):
            over_counted = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI',),
            )

        self.assertFalse(over_counted['triggered'])
        self.assertEqual(
            over_counted['reason'],
            'followup_recovery_limit_reached',
        )
        self.assertEqual(
            over_counted['followup_recovery']['max_attempts'],
            6,
        )

        proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
        }
        processed_work = deepcopy(latest_pipeline_run)
        processed_work['stages']['signal_decider']['processed'] = 1
        self.assertFalse(
            _is_summary_cutoff_contract_repair_candidate(
                pipeline_run=processed_work,
                summary_cutoff_proof=proof,
                price_retry_symbols=set(),
            )
        )
        missing_count = deepcopy(latest_pipeline_run)
        missing_count['stages']['signal_decider'].pop('processed')
        self.assertFalse(
            _is_summary_cutoff_contract_repair_candidate(
                pipeline_run=missing_count,
                summary_cutoff_proof=proof,
                price_retry_symbols=set(),
            )
        )
        missing_sources = deepcopy(latest_pipeline_run)
        missing_sources['stages']['signal_decider'].pop(
            'source_run_ids'
        )
        self.assertFalse(
            _is_summary_cutoff_contract_repair_candidate(
                pipeline_run=missing_sources,
                summary_cutoff_proof=proof,
                price_retry_symbols=set(),
            )
        )
        mismatched_status_evidence = deepcopy(latest_pipeline_run)
        mismatched_status_evidence['run_metadata']['stage_statuses'][
            'signal_decider'
        ] = 'failed'
        self.assertFalse(
            _is_summary_cutoff_contract_repair_candidate(
                pipeline_run=mismatched_status_evidence,
                summary_cutoff_proof=proof,
                price_retry_symbols=set(),
            )
        )
        incomplete_summary = deepcopy(latest_pipeline_run)
        incomplete_summary['run_metadata']['feedback_knowledge_cutoff'][
            'reason'
        ] = 'summary_run_incomplete'
        self.assertFalse(
            _is_summary_cutoff_contract_repair_candidate(
                pipeline_run=incomplete_summary,
                summary_cutoff_proof=proof,
                price_retry_symbols=set(),
            )
        )
        self.assertFalse(
            _is_summary_cutoff_contract_repair_candidate(
                pipeline_run=latest_pipeline_run,
                summary_cutoff_proof={
                    **proof,
                    'feedback_scope_mode': 'broad_feedback_universe',
                },
                price_retry_symbols=set(),
            )
        )
        self.assertFalse(
            _is_summary_cutoff_contract_repair_candidate(
                pipeline_run=latest_pipeline_run,
                summary_cutoff_proof=proof,
                price_retry_symbols={'005930'},
            )
        )

    def test_resumable_cutoff_contract_repair_rejects_gapped_stage_set(
        self,
    ) -> None:
        stage_ids = [
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        interrupted_repair = {
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'interrupted',
            'current_stage': 'signal_decider',
            'run_metadata': {
                'resolved_stages': stage_ids,
                'summary_cutoff_contract_repair_version': 1,
                'summary_cutoff_contract_repair_proof': {
                    'source_run_id': 'feedback-cycle-summary',
                    'feedback_scope_mode': 'decider_scope_fast_path',
                    'requested_symbols': None,
                },
            },
            'stages': {
                stage_id: {
                    'status': 'pending',
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 0,
                    'source_run_ids': {},
                    'details': {},
                }
                for stage_id in (stage_ids[0], stage_ids[2])
            },
        }

        self.assertFalse(
            is_resumable_summary_cutoff_contract_repair(
                interrupted_repair
            )
        )
        misaligned_current_stage = deepcopy(interrupted_repair)
        misaligned_current_stage['stages'] = {
            stage_ids[0]: interrupted_repair['stages'][stage_ids[0]]
        }
        misaligned_current_stage['current_stage'] = stage_ids[2]
        self.assertFalse(
            is_resumable_summary_cutoff_contract_repair(
                misaligned_current_stage
            )
        )
        missing_first_stage = deepcopy(interrupted_repair)
        missing_first_stage['stages'] = {}
        missing_first_stage['current_stage'] = stage_ids[0]
        self.assertFalse(
            is_resumable_summary_cutoff_contract_repair(
                missing_first_stage
            )
        )
        unreachable_two_stage_prefix = deepcopy(interrupted_repair)
        unreachable_two_stage_prefix['stages'] = {
            stage_id: {
                'status': 'pending',
                'processed': 0,
                'total': 0,
                'succeeded': 0,
                'failed': 0,
                'remaining': 0,
                'source_run_ids': {},
                'details': {},
            }
            for stage_id in stage_ids[:2]
        }
        unreachable_two_stage_prefix['current_stage'] = stage_ids[1]
        self.assertFalse(
            is_resumable_summary_cutoff_contract_repair(
                unreachable_two_stage_prefix
            )
        )

    def test_summary_cutoff_proof_rejects_noncanonical_persisted_scope(
        self,
    ) -> None:
        stage = {
            'status': 'completed',
            'source_run_ids': {
                'agent_feedback_run_id': 'feedback-cycle-summary'
            },
            'result_summary': {
                'status': 'completed',
                'run_id': 'feedback-cycle-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': ['005930'],
            },
        }

        self.assertEqual(
            summary_cutoff_proof_from_stage(stage),
            {
                'source_run_id': 'feedback-cycle-summary',
                'feedback_scope_mode': 'decider_scope_fast_path',
                'requested_symbols': ['005930'],
            },
        )
        for malformed_scope in (
            [],
            ['005930', '005930'],
            ['5930'],
        ):
            malformed_stage = deepcopy(stage)
            malformed_stage['result_summary'][
                'requested_symbols'
            ] = malformed_scope
            self.assertIsNone(
                summary_cutoff_proof_from_stage(malformed_stage)
            )

        incomplete_summary = deepcopy(stage)
        incomplete_summary['result_summary']['status'] = 'partial'
        self.assertIsNone(
            summary_cutoff_proof_from_stage(incomplete_summary)
        )

    def test_interrupted_cutoff_contract_repair_resumes_sixth_run_in_place(
        self,
    ) -> None:
        service = PykrxBackfillService()
        proof = {
            'source_run_id': 'feedback-cycle-summary',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'requested_symbols': None,
        }
        stage_ids = [
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        interrupted_repair = {
            'id': 97,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'interrupted',
            'current_stage': 'signal_decider',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'resolved_stages': stage_ids,
                'reused_eod_collection_run_id': 7,
                'resume_from_stage': 'signal_decider',
                'symbols': None,
                'markets': ['KOSPI'],
                'force': False,
                'fs_div': 'CFS',
                'symbol_limit': None,
                'symbol_offset': 0,
                'capture_limit': 500,
                'retry_cycles': None,
                'feedback_max_cycles': 3,
                'feedback_outer_batch_size': 20,
                'reuse_existing_feedback': True,
                'bypass_prerequisite_gate': False,
                'point_in_time_recovery': True,
                'summary_cutoff_contract_repair_version': 1,
                'summary_cutoff_contract_repair_proof': proof,
            },
            'stages': {
                stage_id: {
                    'status': (
                        'interrupted'
                        if stage_id == 'signal_decider'
                        else 'pending'
                    ),
                    'processed': 0,
                    'total': 0,
                    'succeeded': 0,
                    'failed': 0,
                    'remaining': 0,
                    'source_run_ids': {},
                    'details': {},
                }
                for stage_id in stage_ids[:1]
            },
        }
        interrupted_repair['stages']['signal_decider'].update(
            {'total': 1, 'remaining': 1}
        )
        latest_eod_run = {
            'id': 7,
            'status': 'completed',
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': ['005930'],
                'stage_progress': {
                    'agent_feedback_summaries': {
                        'status': 'completed',
                        'source_run_ids': {
                            'agent_feedback_run_id': (
                                'feedback-cycle-summary'
                            ),
                        },
                        'result_summary': {
                            'status': 'completed',
                            'run_id': 'feedback-cycle-summary',
                            'feedback_scope_mode': (
                                'decider_scope_fast_path'
                            ),
                            'requested_symbols': None,
                        },
                    },
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=interrupted_repair,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=6,
            ),
            patch.object(
                service,
                '_preflight_untracked_followup_price_scope',
                side_effect=AssertionError(
                    'in-place repair resume must reuse persisted scope'
                ),
            ),
            patch.object(
                service,
                '_resolve_point_in_time_followup_membership',
                side_effect=AssertionError(
                    'repair resume must not rebuild historical membership'
                ),
            ),
            patch(
                'app.eod.daily_pipeline_stages.'
                'daily_pipeline_stage_service.run',
                return_value={
                    'status': 'completed',
                    'pipeline_run_id': 97,
                },
            ) as resume_followups,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 97,
                    'status': 'completed',
                },
            ),
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={'updated': True},
            ),
        ):
            interrupted_repair['completed_at'] = datetime.now(
                timezone.utc
            ).isoformat()
            backed_off = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI',),
                point_in_time_recovery=True,
            )
            self.assertEqual(backed_off['reason'], 'followup_recovery_backoff')
            self.assertEqual(
                backed_off['followup_recovery']['max_attempts'],
                6,
            )
            resume_followups.assert_not_called()
            interrupted_repair['completed_at'] = (
                '2026-04-03T08:00:00+00:00'
            )
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI',),
                point_in_time_recovery=True,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['followup_recovery']['attempt'], 6)
        self.assertEqual(result['followup_recovery']['max_attempts'], 6)
        self.assertTrue(
            result['followup_recovery'][
                'cutoff_contract_repair_resumed_in_place'
            ]
        )
        recovery_call = resume_followups.call_args.kwargs
        self.assertEqual(recovery_call['existing_pipeline_run_id'], 97)
        self.assertTrue(
            recovery_call['summary_cutoff_contract_repair']
        )
        self.assertTrue(recovery_call['point_in_time_recovery'])
        self.assertEqual(recovery_call['summary_cutoff_proof'], proof)

    def test_terminal_continuation_prefix_uses_shared_outcome_semantics(
        self,
    ) -> None:
        def pipeline_run(
            prefix_stage_id: str,
            prefix_outcome: dict[str, object],
        ) -> dict[str, object]:
            stages = {
                prefix_stage_id: prefix_outcome,
                'agent_feedback_summaries': {'status': 'completed'},
                'signal_decider': {
                    'status': 'failed',
                    'details': {
                        'result_summary': {
                            'status': 'failed',
                            'progress': {
                                'summary': {
                                    'failed_items': 1,
                                    'failure_groups': {
                                        'dependency/missing-upstream': {
                                            'failure_status': (
                                                'dependency/missing-upstream'
                                            ),
                                            'failure_category': 'dependency',
                                            'count': 1,
                                            'retryable_count': 1,
                                            'failure_kinds': [
                                                'dependency_missing_upstream'
                                            ],
                                        }
                                    },
                                }
                            },
                        }
                    },
                },
                'next_open_execution_plan': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'triggered': False,
                            'status': 'blocked',
                            'reason': 'signal_decider_stage_incomplete',
                            'upstream_status': 'failed',
                        }
                    },
                },
                'signal_decider_evaluations': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'triggered': False,
                            'status': 'blocked',
                            'reason': 'declared_dependencies_incomplete',
                            'incomplete_dependencies': ['signal_decider'],
                        }
                    },
                },
            }
            return {
                'id': 96,
                'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
                'status': 'blocked',
                'current_stage': 'signal_decider_evaluations',
                'run_metadata': {
                    'entrypoint': 'daily_pipeline_stage_service.run',
                    'resolved_stages': [
                        prefix_stage_id,
                        'agent_feedback_summaries',
                        'signal_decider',
                        'next_open_execution_plan',
                        'signal_decider_evaluations',
                    ],
                    'symbols': ['005930'],
                    'markets': ['KOSPI'],
                    'force': False,
                    'fs_div': 'CFS',
                    'symbol_limit': None,
                    'symbol_offset': 0,
                    'capture_limit': 500,
                    'retry_cycles': None,
                    'feedback_max_cycles': 3,
                    'feedback_outer_batch_size': 20,
                    'reuse_existing_feedback': True,
                    'bypass_prerequisite_gate': False,
                    'point_in_time_recovery': True,
                    'reused_eod_collection_run_id': 7,
                    'stage_statuses': {
                        stage_id: str(stage['status'])
                        for stage_id, stage in stages.items()
                    },
                },
                'stages': stages,
            }

        cases = (
            (
                'clean completed prefix',
                'pykrx_enrichment',
                {'status': 'completed'},
                True,
            ),
            (
                'high-coverage partial prefix without identity proof',
                'technical_snapshots',
                {
                    'status': 'partial',
                    'succeeded': 99,
                    'failed': 1,
                    'total': 100,
                },
                False,
            ),
            (
                'allowlisted skipped prefix',
                'external_news_collect',
                {'status': 'skipped', 'reason': 'already_completed'},
                True,
            ),
            (
                'completed prefix with failure evidence',
                'pykrx_enrichment',
                {'status': 'completed', 'failed': 1},
                False,
            ),
            (
                'completed enrichment with a nonfatal provider warning',
                'pykrx_enrichment',
                {
                    'status': 'completed',
                    'processed': 4,
                    'total': 4,
                    'succeeded': 3,
                    'failed': 1,
                    'remaining': 0,
                    'completed_at': '2026-04-03T08:00:00+00:00',
                    'details': {
                        'result_summary': {
                            'status': 'completed',
                            'point_in_time_recovery': True,
                            'categories_requested': [
                                'sector_classification',
                                'investor_flow_symbol',
                                'short_selling',
                                'valuation_snapshot',
                            ],
                            'categories': {
                                'sector_classification': {
                                    'status': 'completed',
                                    'rows_persisted': 10,
                                    'errors': [],
                                },
                                'investor_flow_symbol': {
                                    'status': 'completed',
                                    'rows_persisted': 10,
                                    'errors': [],
                                },
                                'short_selling': {
                                    'status': 'partial',
                                    'rows_persisted': 10,
                                    'errors': [
                                        {
                                            'operation': (
                                                'short_selling_balance'
                                            ),
                                            'error': 'empty provider frame',
                                        }
                                    ],
                                },
                                'valuation_snapshot': {
                                    'status': 'completed',
                                    'rows_persisted': 10,
                                    'errors': [],
                                },
                            },
                            'downstream_preflight': {
                                'allowed': True,
                                'status': 'ready',
                                'required_categories': [
                                    'sector_classification',
                                    'investor_flow_symbol',
                                    'short_selling',
                                    'valuation_snapshot',
                                ],
                                'category_row_counts': {
                                    'sector_classification': 10,
                                    'investor_flow_symbol': 10,
                                    'short_selling': 10,
                                    'valuation_snapshot': 10,
                                },
                                'failed_categories': [],
                                'blocked_categories': [],
                                'waiting_categories': [],
                                'zero_coverage_categories': [],
                            },
                        }
                    },
                },
                True,
            ),
        )

        for label, prefix_stage_id, prefix_outcome, expected in cases:
            with self.subTest(label=label):
                self.assertEqual(
                    _is_terminal_followup_continuation_candidate(
                        pipeline_run(prefix_stage_id, prefix_outcome)
                    ),
                    expected,
                )

    def test_sixth_missing_upstream_failure_continues_same_pipeline_row_from_summaries(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'status': 'completed',
            'symbols_failed': 0,
            'run_metadata': {'failed_symbols': []},
        }
        latest_pipeline_run = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            # Mixed completed/failed/blocked stages are persisted as partial by
            # the shared overall-status lattice.
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'resolved_stages': [
                    'pykrx_enrichment',
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'symbols': ['005930', '000660'],
                'markets': ['KOSPI'],
                'force': False,
                'fs_div': 'CFS',
                'symbol_limit': None,
                'symbol_offset': 0,
                'capture_limit': 500,
                'retry_cycles': None,
                'feedback_max_cycles': 3,
                'feedback_outer_batch_size': 20,
                'reuse_existing_feedback': True,
                'bypass_prerequisite_gate': False,
                'point_in_time_recovery': True,
                'reused_eod_collection_run_id': 7,
                'stage_statuses': {
                    'pykrx_enrichment': 'completed',
                    'agent_feedback_summaries': 'completed',
                    'signal_decider': 'failed',
                    'next_open_execution_plan': 'blocked',
                    'signal_decider_evaluations': 'blocked',
                },
            },
            'stages': {
                'pykrx_enrichment': {
                    'status': 'completed',
                },
                'agent_feedback_summaries': {
                    'status': 'completed',
                    'processed': 2,
                    'total': 2,
                    'succeeded': 2,
                    'failed': 0,
                    'details': {
                        'result_summary': {
                            'status': 'completed',
                            'run_id': 'feedback-cycle-summary',
                        },
                    },
                },
                'signal_decider': {
                    'status': 'failed',
                    'details': {
                        'result_summary': {
                            'status': 'failed',
                            'progress': {
                                'summary': {
                                    'failed_items': 4,
                                    'failure_groups': {
                                        'dependency/missing-upstream': {
                                            'failure_status': (
                                                'dependency/missing-upstream'
                                            ),
                                            'failure_category': 'dependency',
                                            'count': 4,
                                            'retryable_count': 4,
                                            'failure_kinds': [
                                                'dependency_missing_upstream'
                                            ],
                                        }
                                    },
                                }
                            },
                        }
                    },
                },
                'next_open_execution_plan': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'triggered': False,
                            'status': 'blocked',
                            'reason': 'signal_decider_stage_incomplete',
                            'upstream_status': 'failed',
                        }
                    },
                },
                'signal_decider_evaluations': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'triggered': False,
                            'status': 'blocked',
                            'reason': 'declared_dependencies_incomplete',
                            'incomplete_dependencies': ['signal_decider'],
                        }
                    },
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=6,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={
                    'triggered': True,
                    'status': 'completed',
                    'pipeline_run_id': 96,
                },
            ) as resume_followups,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': False,
                    'pipeline_run_id': 96,
                    'status': 'completed',
                    'reason': 'already_non_resumable',
                },
            ),
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={'updated': True, 'status': 'completed'},
            ),
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ'),
            )

        expected_stages = [
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        self.assertTrue(result['triggered'])
        self.assertEqual(
            result['reason'],
            'terminal_followup_continuation_resumed',
        )
        self.assertEqual(result['pipeline_run_id'], 96)
        self.assertTrue(
            result['followup_recovery']['terminal_continuation_in_place']
        )
        self.assertEqual(result['followup_recovery']['attempt'], 6)
        self.assertNotIn('price_collection', expected_stages)
        resume_followups.assert_called_once_with(
            market_date='2026-04-03',
            stages=expected_stages,
            symbols=['005930', '000660'],
            markets=('KOSPI',),
            force=False,
            reuse_existing_feedback=True,
            resume_from_stage='agent_feedback_summaries',
            trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            reused_eod_collection_run_id=7,
            point_in_time_recovery=True,
            existing_pipeline_run_id=96,
            provider_admission_fence_owned=False,
        )

    def test_terminal_continuation_marker_prevents_second_same_row_resume(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_pipeline_run = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': [
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'symbols': ['005930'],
                'markets': ['KOSPI'],
                'reused_eod_collection_run_id': 7,
                'terminal_continuation_count': 1,
            },
            'stages': {
                'agent_feedback_summaries': {'status': 'completed'},
                'signal_decider': {
                    'status': 'failed',
                    'details': {
                        'result_summary': {
                            'status': 'failed',
                            'progress': {
                                'summary': {
                                    'failed_items': 1,
                                    'failure_groups': {
                                        'dependency/missing-upstream': {
                                            'count': 1,
                                            'retryable_count': 1,
                                            'failure_kinds': [
                                                'dependency_missing_upstream'
                                            ],
                                        }
                                    },
                                }
                            },
                        }
                    },
                },
                'next_open_execution_plan': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'reason': 'signal_decider_stage_incomplete'
                        }
                    },
                },
                'signal_decider_evaluations': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'reason': 'declared_dependencies_incomplete',
                            'incomplete_dependencies': ['signal_decider'],
                        }
                    },
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=6,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run'
            ) as resume_followups,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
                markets=('KOSPI',),
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'followup_recovery_limit_reached')
        resume_followups.assert_not_called()

    def test_claimed_stale_terminal_continuation_is_resumable_in_place(
        self,
    ) -> None:
        original_suffix = {
            'agent_feedback_summaries': {
                'status': 'completed',
                'source_run_ids': {'run_id': 'feedback-cycle-summary'},
            },
            'signal_decider': {
                'status': 'failed',
                'details': {
                    'result_summary': {
                        'status': 'failed',
                        'progress': {
                            'summary': {
                                'failed_items': 1,
                                'failure_groups': {
                                    'dependency/missing-upstream': {
                                        'failure_status': (
                                            'dependency/missing-upstream'
                                        ),
                                        'failure_category': 'dependency',
                                        'count': 1,
                                        'retryable_count': 1,
                                        'failure_kinds': [
                                            'dependency_missing_upstream'
                                        ],
                                    }
                                },
                            }
                        },
                    }
                },
            },
            'next_open_execution_plan': {
                'status': 'blocked',
                'details': {
                    'result_summary': {
                        'triggered': False,
                        'status': 'blocked',
                        'reason': 'signal_decider_stage_incomplete',
                        'upstream_status': 'failed',
                    }
                },
            },
            'signal_decider_evaluations': {
                'status': 'blocked',
                'details': {
                    'result_summary': {
                        'triggered': False,
                        'status': 'blocked',
                        'reason': 'declared_dependencies_incomplete',
                        'incomplete_dependencies': ['signal_decider'],
                    }
                },
            },
        }
        claimed_run = {
            'id': 96,
            'market_date': '2026-04-03',
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'running',
            'current_stage': 'agent_feedback_summaries',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'entrypoint': 'daily_pipeline_stage_service.run',
                'resolved_stages': [
                    'pykrx_enrichment',
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'symbols': ['005930', '000660'],
                'markets': ['KOSPI'],
                'force': False,
                'fs_div': 'CFS',
                'symbol_limit': None,
                'symbol_offset': 0,
                'capture_limit': 500,
                'retry_cycles': None,
                'feedback_max_cycles': 3,
                'feedback_outer_batch_size': 20,
                'reuse_existing_feedback': True,
                'bypass_prerequisite_gate': False,
                'point_in_time_recovery': True,
                'reused_eod_collection_run_id': 7,
                'resume_from_stage': 'agent_feedback_summaries',
                'terminal_continuation_count': 1,
                'terminal_continuation_version': 1,
                'terminal_continuation_claimed_at': (
                    '2026-04-03T08:00:00+00:00'
                ),
                'terminal_continuation_reason': (
                    'summary_knowledge_cutoff_repair'
                ),
                'stage_statuses': {
                    'pykrx_enrichment': 'completed',
                    'agent_feedback_summaries': 'pending',
                    'signal_decider': 'pending',
                    'next_open_execution_plan': 'pending',
                    'signal_decider_evaluations': 'pending',
                },
                'terminal_continuation_evidence': {
                    'parent': {
                        'status': 'blocked',
                        'current_stage': 'signal_decider_evaluations',
                    },
                    'suffix_stages': original_suffix,
                    'lineage': {
                        'reused_eod_collection_run_id': 7,
                        'expected_run_count': 6,
                        'observed_run_count': 6,
                        'ordered_pipeline_run_ids': [91, 92, 93, 94, 95, 96],
                        'target_ordinal': 6,
                        'latest_pipeline_run_id': 96,
                    },
                    'summary_proof': {
                        'ready': True,
                        'source_run_id': 'feedback-cycle-summary',
                        'knowledge_cutoff_at': (
                            '2026-04-03T06:30:00+00:00'
                        ),
                        'feedback_scope_mode': 'decider_scope_fast_path',
                        'requested_symbols': ['005930'],
                        'requested_symbol_count': 1,
                    },
                },
            },
            'stages': {
                'pykrx_enrichment': {'status': 'completed'},
                **{
                    stage_id: {
                        'status': 'pending',
                        'processed': 0,
                        'total': 0,
                        'succeeded': 0,
                        'failed': 0,
                        'remaining': 0,
                        'current_step': 'terminal_continuation_pending',
                        'completed_at': None,
                        'error_message': None,
                        'source_run_ids': {},
                        'details': {},
                    }
                    for stage_id in (
                        'agent_feedback_summaries',
                        'signal_decider',
                        'next_open_execution_plan',
                        'signal_decider_evaluations',
                    )
                },
            },
        }

        self.assertTrue(
            _is_terminal_followup_continuation_candidate(claimed_run)
        )

        malformed_completed_scope_proof = deepcopy(claimed_run)
        malformed_completed_scope_proof['run_metadata'][
            'terminal_continuation_evidence'
        ]['summary_proof']['completed_scope_proof'] = {
            'schema_version': 'completed_summarizer_intersection_v1',
            'completed_symbols': ['005930'],
            'admitted_symbols': ['035420'],
        }
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(
                malformed_completed_scope_proof
            )
        )

        outside_parent_scope = deepcopy(claimed_run)
        outside_parent_scope['run_metadata'][
            'terminal_continuation_evidence'
        ]['summary_proof']['requested_symbols'] = ['035420']
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(
                outside_parent_scope
            )
        )

        duplicated_summary_scope = deepcopy(claimed_run)
        duplicated_summary_scope['run_metadata'][
            'terminal_continuation_evidence'
        ]['summary_proof'].update(
            {
                'requested_symbols': ['005930', '005930'],
                'requested_symbol_count': 2,
            }
        )
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(
                duplicated_summary_scope
            )
        )

        unsupported_summary_scope = deepcopy(claimed_run)
        unsupported_summary_scope['run_metadata'][
            'terminal_continuation_evidence'
        ]['summary_proof']['feedback_scope_mode'] = 'unknown_scope'
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(
                unsupported_summary_scope
            )
        )

        result_lineage_only = deepcopy(claimed_run)
        archived_summary = result_lineage_only['run_metadata'][
            'terminal_continuation_evidence'
        ]['suffix_stages']['agent_feedback_summaries']
        archived_summary['source_run_ids'] = {'eod_run_id': 7}
        archived_summary['details'] = {
            'result_summary': {'run_id': 'feedback-cycle-summary'}
        }
        self.assertTrue(
            _is_terminal_followup_continuation_candidate(
                result_lineage_only
            )
        )

        conflicting_result_lineage = deepcopy(result_lineage_only)
        conflicting_result_lineage['run_metadata'][
            'terminal_continuation_evidence'
        ]['suffix_stages']['agent_feedback_summaries'][
            'source_run_ids'
        ] = {'run_id': 'another-feedback-cycle'}
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(
                conflicting_result_lineage
            )
        )

        duplicate_suffix = deepcopy(claimed_run)
        duplicate_suffix['run_metadata']['resolved_stages'].insert(
            -1,
            'signal_decider',
        )
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(duplicate_suffix)
        )

        misordered_suffix = deepcopy(claimed_run)
        misordered_suffix['run_metadata']['resolved_stages'][-2:] = [
            'signal_decider_evaluations',
            'next_open_execution_plan',
        ]
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(misordered_suffix)
        )

        interrupted_after_summary = deepcopy(claimed_run)
        interrupted_after_summary['status'] = 'interrupted'
        interrupted_after_summary['current_stage'] = 'signal_decider'
        interrupted_after_summary['run_metadata'].update(
            {
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
                'interrupted_at': '2026-04-03T08:01:00+00:00',
            }
        )
        interrupted_after_summary['stages']['agent_feedback_summaries'] = {
            'status': 'completed',
            'processed': 1,
            'total': 1,
            'succeeded': 1,
            'failed': 0,
            'completed_at': '2026-04-03T08:00:30+00:00',
            'error_message': None,
            'source_run_ids': {
                'run_id': 'feedback-cycle-continued'
            },
            'details': {
                'result_summary': {
                    'status': 'completed',
                    'run_id': 'feedback-cycle-continued',
                }
            },
        }
        interrupted_after_summary['stages']['signal_decider'] = {
            'status': 'interrupted',
            'processed': 0,
            'total': 1,
            'succeeded': 0,
            'failed': 0,
            'remaining': 1,
            'current_step': 'core_restart_interrupted',
            'completed_at': '2026-04-03T08:01:00+00:00',
            'error_message': 'Core restarted.',
            'source_run_ids': {},
            'details': {
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
            },
        }
        self.assertTrue(
            _is_terminal_followup_continuation_candidate(
                interrupted_after_summary
            )
        )

        lease_lost_after_summary = deepcopy(interrupted_after_summary)
        lease_loss_error = (
            'feedback retry lease ownership lost for '
            'feedback-cycle-2026-04-03-deadbeef01 at '
            'before_parent_finalization: '
            'heartbeat_refresh_grace_exceeded'
        )
        lease_lost_after_summary['status'] = 'failed'
        lease_lost_after_summary['current_stage'] = 'signal_decider'
        lease_lost_after_summary['completed_at'] = (
            '2026-04-03T08:01:00+00:00'
        )
        lease_lost_after_summary['error_message'] = lease_loss_error
        lease_lost_after_summary['run_metadata'].pop('resumable', None)
        lease_lost_after_summary['run_metadata'].pop(
            'interrupted_reason',
            None,
        )
        lease_lost_after_summary['run_metadata'].pop(
            'interrupted_at',
            None,
        )
        lease_lost_after_summary['stages']['signal_decider'] = {
            'stage_id': 'signal_decider',
            'status': 'failed',
            'processed': 20,
            'total': 160,
            'succeeded': 0,
            'failed': 20,
            'remaining': 140,
            'current_step': 'signal_decider_failed',
            'completed_at': '2026-04-03T08:01:00+00:00',
            'error_message': lease_loss_error,
            'source_run_ids': {
                'agent_feedback_run_id': (
                    'feedback-cycle-2026-04-03-deadbeef01'
                )
            },
            'details': {},
        }
        self.assertTrue(
            _is_terminal_followup_continuation_candidate(
                lease_lost_after_summary
            )
        )

        generic_failure = deepcopy(lease_lost_after_summary)
        generic_failure['error_message'] = 'upstream request failed'
        generic_failure['stages']['signal_decider'][
            'error_message'
        ] = 'upstream request failed'
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(generic_failure)
        )

        mismatched_child = deepcopy(lease_lost_after_summary)
        mismatched_child['stages']['signal_decider']['source_run_ids'] = {
            'agent_feedback_run_id': (
                'feedback-cycle-2026-04-03-cafebabe01'
            )
        }
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(mismatched_child)
        )

        consumed_reclaim = deepcopy(lease_lost_after_summary)
        consumed_reclaim['run_metadata'][
            'terminal_continuation_resume_count'
        ] = 1
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(consumed_reclaim)
        )

        for malformed_resume_count in ('0', 'corrupt', True):
            with self.subTest(
                malformed_resume_count=malformed_resume_count,
            ):
                malformed_reclaim = deepcopy(lease_lost_after_summary)
                malformed_reclaim['run_metadata'][
                    'terminal_continuation_resume_count'
                ] = malformed_resume_count
                self.assertFalse(
                    _is_terminal_followup_continuation_candidate(
                        malformed_reclaim
                    )
                )

        failed_parent_with_pending_stage = deepcopy(
            lease_lost_after_summary
        )
        failed_parent_with_pending_stage['stages']['signal_decider'] = (
            deepcopy(claimed_run['stages']['signal_decider'])
        )
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(
                failed_parent_with_pending_stage
            )
        )

        interrupted_between_stages = deepcopy(interrupted_after_summary)
        interrupted_between_stages['current_stage'] = (
            'agent_feedback_summaries'
        )
        interrupted_between_stages['stages']['signal_decider'] = deepcopy(
            claimed_run['stages']['signal_decider']
        )
        self.assertTrue(
            _is_terminal_followup_continuation_candidate(
                interrupted_between_stages
            )
        )

        noncontiguous_progress = deepcopy(interrupted_after_summary)
        noncontiguous_progress['current_stage'] = (
            'agent_feedback_summaries'
        )
        noncontiguous_progress['stages']['agent_feedback_summaries'] = {
            **interrupted_after_summary['stages']['signal_decider'],
        }
        noncontiguous_progress['stages']['signal_decider'] = {
            'status': 'completed',
            'processed': 1,
            'total': 1,
            'succeeded': 1,
            'failed': 0,
            'completed_at': '2026-04-03T08:00:30+00:00',
            'error_message': None,
            'source_run_ids': {'run_id': 'decider-continued'},
            'details': {'result_summary': {'status': 'completed'}},
        }
        self.assertFalse(
            _is_terminal_followup_continuation_candidate(
                noncontiguous_progress
            )
        )

    def test_sixth_non_dependency_failure_does_not_continue_same_row(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_pipeline_run = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'blocked',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': [
                    'agent_feedback_summaries',
                    'signal_decider',
                    'next_open_execution_plan',
                    'signal_decider_evaluations',
                ],
                'symbols': ['005930'],
                'markets': ['KOSPI'],
                'reused_eod_collection_run_id': 7,
            },
            'stages': {
                'agent_feedback_summaries': {'status': 'completed'},
                'signal_decider': {
                    'status': 'failed',
                    'details': {
                        'result_summary': {
                            'status': 'failed',
                            'progress': {
                                'summary': {
                                    'failed_items': 1,
                                    'failure_groups': {
                                        'transport/timeout': {
                                            'count': 1,
                                            'retryable_count': 1,
                                            'failure_kinds': [
                                                'openclaw_transport_timeout'
                                            ],
                                        }
                                    },
                                }
                            },
                        }
                    },
                },
                'next_open_execution_plan': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'reason': 'signal_decider_stage_incomplete'
                        }
                    },
                },
                'signal_decider_evaluations': {
                    'status': 'blocked',
                    'details': {
                        'result_summary': {
                            'reason': 'declared_dependencies_incomplete',
                            'incomplete_dependencies': ['signal_decider'],
                        }
                    },
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=6,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run'
            ) as resume_followups,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
                markets=('KOSPI',),
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'followup_recovery_limit_reached')
        resume_followups.assert_not_called()

    def test_fresh_ready_enrichment_repair_rearms_one_bounded_followup(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'failed',
            'symbols_total': 3,
            'symbols_succeeded': 2,
            'symbols_failed': 1,
            'run_metadata': {
                'markets': ['KOSPI'],
                'failed_symbols': ['472220'],
                'downstream_retry_symbols': ['005930', '000660'],
                'downstream_retry_resume_from_stage': 'pykrx_enrichment',
            },
        }
        latest_pipeline_run = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'failed',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': [
                    'pykrx_enrichment',
                    'technical_snapshots',
                ],
                'reused_eod_collection_run_id': 7,
            },
            'stages': {},
        }
        repair_checkpoint = {
            'pipeline_run_id': 455,
            'stage_run_id': 900,
            'completed_at': '2026-04-03T09:00:00+00:00',
            'markets': ['KOSPI'],
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=5,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=repair_checkpoint,
            ) as load_repair_checkpoint,
            patch.object(
                service,
                '_resolve_point_in_time_followup_membership',
                return_value=(
                    {'005930', '000660', '472220'},
                    ('KOSPI',),
                ),
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 97},
            ) as resume_followups,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 97,
                    'status': 'completed',
                },
            ),
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={
                    'updated': True,
                    'status': 'failed',
                    'price_retry_pending': True,
                },
            ),
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI',),
                allow_safe_downstream_scope_with_price_failures=True,
                point_in_time_recovery=True,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(result['followup_recovery']['attempt'], 6)
        self.assertEqual(
            result['followup_recovery']['repair_checkpoint_pipeline_run_id'],
            455,
        )
        recovery_call = resume_followups.call_args.kwargs
        self.assertEqual(recovery_call['resume_from_stage'], 'pykrx_enrichment')
        self.assertEqual(
            recovery_call['stages'],
            ['pykrx_enrichment', 'technical_snapshots'],
        )
        self.assertNotIn('price_collection', recovery_call['stages'])
        self.assertEqual(recovery_call['symbols'], ['005930', '000660'])
        self.assertEqual(recovery_call['reused_eod_collection_run_id'], 7)
        self.assertTrue(recovery_call['point_in_time_recovery'])
        load_repair_checkpoint.assert_called_once_with(
            trade_date='2026-04-03',
            markets=('KOSPI',),
            completed_after='2026-04-03T08:00:00+00:00',
        )

    def test_same_day_repair_admission_uses_persisted_lineage_markets(
        self,
    ) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'failed',
            'symbols_total': 3,
            'symbols_succeeded': 2,
            'symbols_failed': 1,
            'run_metadata': {
                'markets': ['KOSPI'],
                'failed_symbols': ['472220'],
                'downstream_retry_symbols': ['005930', '000660'],
                'downstream_retry_resume_from_stage': 'pykrx_enrichment',
            },
        }
        latest_pipeline_run = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'failed',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': [
                    'pykrx_enrichment',
                    'technical_snapshots',
                ],
                'reused_eod_collection_run_id': 7,
            },
            'stages': {},
        }
        repair_checkpoint = {
            'pipeline_run_id': 455,
            'stage_run_id': 900,
            'completed_at': '2026-04-03T09:00:00+00:00',
            'markets': ['KOSPI'],
        }

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=5,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_latest_pykrx_enrichment_repair_checkpoint',
                return_value=repair_checkpoint,
            ) as load_repair_checkpoint,
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 97},
            ) as resume_followups,
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'converge_terminal_stages_run',
                return_value={
                    'updated': True,
                    'pipeline_run_id': 97,
                    'status': 'completed',
                },
            ),
            patch.object(
                service,
                '_persist_successful_followup_recovery',
                return_value={
                    'updated': True,
                    'status': 'failed',
                    'price_retry_pending': True,
                },
            ),
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSDAQ',),
                allow_safe_downstream_scope_with_price_failures=True,
                point_in_time_recovery=False,
            )

        self.assertTrue(result['triggered'])
        load_repair_checkpoint.assert_called_once_with(
            trade_date='2026-04-03',
            markets=('KOSPI',),
            completed_after='2026-04-03T08:00:00+00:00',
        )
        recovery_call = resume_followups.call_args.kwargs
        self.assertEqual(recovery_call['markets'], ('KOSDAQ',))
        self.assertNotIn('point_in_time_recovery', recovery_call)

    def test_old_eod_recovery_attempts_do_not_exhaust_new_eod_lineage(
        self,
    ) -> None:
        service = PykrxBackfillService()
        old_eod_recovery = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'failed',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': ['signal_decider'],
                'reused_eod_collection_run_id': 6,
            },
            'stages': {'signal_decider': {'status': 'failed'}},
        }
        latest_eod_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 1,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930'],
                'failed_symbols': [],
                'unattempted_symbols': [],
                'downstream_retry_symbols': [],
                'stage_progress': {
                    'signal_decider': {'status': 'failed'},
                },
            },
        }

        def count_recovery_attempts(
            *,
            market_date,
            trigger,
            reused_eod_collection_run_id=None,
        ):
            self.assertEqual(market_date, '2026-04-03')
            self.assertEqual(trigger, AUTO_FOLLOWUP_RECOVERY_TRIGGER)
            # Five attempts belong to EOD #6; EOD #7 has its own fresh budget.
            return 0 if reused_eod_collection_run_id == 7 else 5

        with (
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=old_eod_recovery,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                side_effect=count_recovery_attempts,
            ) as count_runs,
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['005930'],
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 97},
            ) as resume_followups,
        ):
            result = service._resume_auto_daily_followups_if_due(
                market_date='2026-04-03',
                latest_available_trade_date='2026-04-03',
                latest_eod_run=latest_eod_run,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )

        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(result['followup_recovery']['attempt'], 1)
        self.assertEqual(
            result['followup_recovery']['resumed_from_pipeline_run_id'],
            None,
        )
        count_runs.assert_called_once_with(
            market_date='2026-04-03',
            trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            reused_eod_collection_run_id=7,
        )
        resume_followups.assert_called_once()

    def test_run_daily_pipeline_uses_scheduled_stage_sequence_with_pykrx_enrichment(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch('app.eod.pykrx_backfill.load_latest_eod_collection_run', return_value=None),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 9,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        kwargs = run_collection.call_args.kwargs
        self.assertEqual(kwargs['selected_stages'], PYKRX_STAGE_SEQUENCE)
        self.assertTrue(kwargs['track_daily_pipeline'])
        self.assertTrue(kwargs['provider_admission_fence_owned'])
        self.assertIn('pykrx_enrichment', kwargs['selected_stages'])
        self.assertLess(
            kwargs['selected_stages'].index('price_collection'),
            kwargs['selected_stages'].index('pykrx_enrichment'),
        )
        self.assertLess(
            kwargs['selected_stages'].index('pykrx_enrichment'),
            kwargs['selected_stages'].index('technical_snapshots'),
        )

    def test_run_daily_pipeline_force_bypasses_stale_running_row(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 8,
                    'status': 'running',
                    'symbols_failed': 0,
                },
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 9,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='manual_daily_api',
                force=True,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['run_id'], 9)
        run_collection.assert_called_once()

    def test_run_daily_pipeline_stops_before_collection_when_admission_is_deduplicated(self) -> None:
        service = PykrxBackfillService()
        run_state_service = _FakeDeduplicatingDailyPipelineRunStateService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(
                service,
                'load_active_universe',
                return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
            ),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch('app.eod.pykrx_backfill.load_latest_eod_collection_run', return_value=None),
            patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service', run_state_service),
            patch('app.eod.pykrx_backfill.create_eod_collection_run') as create_run,
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date') as collect_prices,
            patch(
                'app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh'
            ) as fundamentals_followup,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
                refresh_universe=False,
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'overlapping_run_already_active')
        self.assertEqual(result['pipeline_run_id'], 744)
        create_run.assert_not_called()
        collect_prices.assert_not_called()
        fundamentals_followup.assert_not_called()
        self.assertEqual(run_state_service.stage_updates, [])
        self.assertEqual(run_state_service.finish_runs, [])

    def test_run_daily_pipeline_fails_closed_when_admission_write_fails(
        self,
    ) -> None:
        service = PykrxBackfillService()
        run_state_service = MagicMock()
        run_state_service.start_run.side_effect = RuntimeError(
            'daily state database unavailable'
        )

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch.object(
                service,
                'load_active_universe',
                return_value=[
                    PykrxSymbol(
                        symbol='005930',
                        market='KOSPI',
                        name_ko='Samsung Electronics',
                    )
                ],
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service',
                run_state_service,
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run'
            ) as create_run,
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date'
            ) as collect_prices,
            patch(
                'app.eod.pykrx_backfill.'
                'dart_fundamentals_capture_service.'
                'run_daily_incremental_refresh'
            ) as fundamentals_followup,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'admission checkpoint failed',
            ):
                service.run_daily_pipeline_if_due(
                    market_date='2026-04-03',
                    trigger='auto_close_scheduler',
                    refresh_universe=False,
                )

        create_run.assert_not_called()
        collect_prices.assert_not_called()
        fundamentals_followup.assert_not_called()

    def test_tracking_checkpoint_failure_closes_admitted_rows_before_provider_work(
        self,
    ) -> None:
        for failure_point in ('link_eod_run', 'initial_stage'):
            with self.subTest(failure_point=failure_point):
                service = PykrxBackfillService()
                run_state_service = MagicMock()
                run_state_service.start_run.return_value = {'id': 501}
                run_state_service.finish_run.return_value = {
                    'id': 501,
                    'status': 'failed',
                }
                if failure_point == 'link_eod_run':
                    run_state_service.link_eod_run.side_effect = (
                        RuntimeError('link checkpoint unavailable')
                    )
                else:
                    run_state_service.update_stage.side_effect = (
                        RuntimeError('stage checkpoint unavailable')
                    )

                with (
                    patch.object(
                        service,
                        'resolve_latest_trade_date',
                        return_value='2026-04-03',
                    ),
                    patch.object(
                        service,
                        'load_active_universe',
                        return_value=[
                            PykrxSymbol(
                                symbol='005930',
                                market='KOSPI',
                                name_ko='Samsung Electronics',
                            )
                        ],
                    ),
                    patch(
                        'app.eod.pykrx_backfill.'
                        'daily_pipeline_run_state_service',
                        run_state_service,
                    ),
                    patch(
                        'app.eod.pykrx_backfill.create_eod_collection_run',
                        return_value=12,
                    ),
                    patch(
                        'app.eod.pykrx_backfill.'
                        'complete_eod_collection_run'
                    ) as complete_run,
                    patch(
                        'app.eod.pykrx_backfill._get_market_ohlcv_by_date'
                    ) as collect_prices,
                ):
                    with self.assertRaisesRegex(
                        RuntimeError,
                        'checkpoint unavailable',
                    ):
                        service._run_market_data_pipeline(
                            start_date='20260403',
                            end_date='20260403',
                            markets=('KOSPI',),
                            trigger='auto_close_scheduler',
                            refresh_universe=False,
                            mode='daily_auto',
                            selected_stages=('price_collection',),
                            chain_fundamentals_followup=False,
                            track_daily_pipeline=True,
                        )

                collect_prices.assert_not_called()
                complete_run.assert_called_once()
                self.assertEqual(
                    complete_run.call_args.kwargs['status'],
                    'failed',
                )
                run_state_service.finish_run.assert_called_once()

    def test_grouped_pipeline_owner_is_persisted_and_linked_before_provider_work(
        self,
    ) -> None:
        service = PykrxBackfillService()
        run_state_service = MagicMock()
        run_state_service.link_eod_run.side_effect = RuntimeError(
            'owner link unavailable'
        )

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch.object(
                service,
                'load_active_universe',
                return_value=[
                    PykrxSymbol(
                        symbol='005930',
                        market='KOSPI',
                        name_ko='Samsung Electronics',
                    )
                ],
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service',
                run_state_service,
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=12,
            ) as create_run,
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_run,
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date'
            ) as collect_prices,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'owner link unavailable',
            ):
                service._run_market_data_pipeline(
                    start_date='20260403',
                    end_date='20260403',
                    markets=('KOSPI',),
                    trigger='internal_ops_cli_stage',
                    refresh_universe=False,
                    mode='daily_stage_run',
                    selected_stages=('price_collection',),
                    chain_fundamentals_followup=False,
                    owning_pipeline_run_id=9000,
                )

        self.assertEqual(
            create_run.call_args.kwargs['run_metadata'][
                'daily_pipeline_run_id'
            ],
            9000,
        )
        run_state_service.link_eod_run.assert_called_once_with(
            pipeline_run_id=9000,
            eod_run_id=12,
        )
        run_state_service.start_run.assert_not_called()
        collect_prices.assert_not_called()
        complete_run.assert_called_once()
        self.assertEqual(
            complete_run.call_args.kwargs['status'],
            'failed',
        )

    def test_run_daily_pipeline_marks_stale_running_row_failed_and_retries(self) -> None:
        service = PykrxBackfillService()
        stale_started_at = (datetime.now(timezone.utc) - timedelta(minutes=45)).isoformat()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                side_effect=[
                    {
                        'id': 8,
                        'status': 'running',
                        'symbols_total': 10,
                        'symbols_succeeded': 0,
                        'symbols_failed': 0,
                        'started_at': stale_started_at,
                        'run_metadata': {
                            'markets': ['KOSPI'],
                            'requested_symbols': [
                                '000001',
                                '000002',
                                '000003',
                                '000004',
                                '000005',
                                '000006',
                                '000007',
                                '000008',
                                '000009',
                                '000010',
                            ],
                        },
                    },
                    {
                        'id': 8,
                        'status': 'failed',
                        'symbols_total': 10,
                        'symbols_succeeded': 3,
                        'symbols_failed': 7,
                        'started_at': stale_started_at,
                        'run_metadata': {
                            'markets': ['KOSPI'],
                            'failed_symbols': [
                                '000004',
                                '000005',
                                '000006',
                                '000007',
                                '000008',
                                '000009',
                                '000010',
                            ],
                        },
                    },
                ],
            ),
            patch('app.eod.pykrx_backfill.count_eod_run_persisted_symbols', return_value=3),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['000001', '000002', '000003'],
            ),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=1),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
            patch.object(
                service,
                '_load_retry_symbols_for_run',
                return_value=[
                    PykrxSymbol(
                        symbol=f'{symbol_number:06d}',
                        market='KOSPI',
                        name_ko='Retry Me',
                    )
                    for symbol_number in range(4, 11)
                ],
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 9,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        complete_run.assert_called_once_with(
            8,
            status='failed',
            symbols_succeeded=3,
            symbols_failed=7,
            error_message='Marked stale after 30 minutes without completion.',
            run_metadata=ANY,
        )
        run_collection.assert_called_once()

    def test_stale_failed_run_with_exact_complete_prices_resumes_followups(
        self,
    ) -> None:
        service = PykrxBackfillService()
        stale_started_at = (
            datetime.now(timezone.utc) - timedelta(minutes=45)
        ).isoformat()
        requested_symbols = ['000001', '000002']
        stale_run = {
            'id': 8,
            'trade_date': '2026-04-03',
            'trigger': 'auto_close_scheduler',
            'status': 'running',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'started_at': stale_started_at,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': requested_symbols,
                'failed_symbols': [],
                'unattempted_symbols': [],
                'expected_daily_pipeline_stages': list(
                    AUTO_DAILY_TRACKED_STAGE_ORDER
                ),
                'stage_progress': {
                    'price_collection': {'status': 'completed'},
                    'pykrx_enrichment': {'status': 'completed'},
                    'technical_snapshots': {'status': 'running'},
                },
            },
        }
        failed_run = {
            **stale_run,
            'status': 'failed',
            'run_metadata': {
                **stale_run['run_metadata'],
                'failed_symbols': [],
                'failed_symbols_count': 0,
                'unattempted_symbols': [],
                'unattempted_symbols_count': 0,
                'collection_coverage_checked_at': (
                    '2026-04-03T09:00:00+00:00'
                ),
                'collection_persisted_symbols_count': 2,
                'collection_expected_symbols_count': 2,
                'unexpected_persisted_symbols': [],
                'unexpected_persisted_symbols_count': 0,
                'collection_coverage_inconsistent': False,
                'stage_progress': {
                    'price_collection': {'status': 'completed'},
                    'pykrx_enrichment': {'status': 'completed'},
                    'technical_snapshots': {'status': 'failed'},
                },
            },
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                side_effect=[stale_run, failed_run],
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=2,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=requested_symbols,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=1,
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run'),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'get_latest_run_for_context',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
                'count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'partial', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(
            result['followup_recovery']['resume_from_stage'],
            'technical_snapshots',
        )
        recovery_call = resume_stages.call_args.kwargs
        self.assertIn('momentum_factors', recovery_call['stages'])
        self.assertEqual(recovery_call['symbols'], requested_symbols)
        run_collection.assert_not_called()

    def test_stale_run_reconciles_committed_bar_before_building_retry_scope(
        self,
    ) -> None:
        service = PykrxBackfillService()
        run = {
            'id': 8,
            'trade_date': '2026-04-03',
            'status': 'running',
            'symbols_total': 3,
            'symbols_succeeded': 1,
            'symbols_failed': 2,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': ['000001', '000002', '000003'],
                # 000002 was persisted immediately before the worker died,
                # but its progress checkpoint did not commit.
                'unattempted_symbols': ['000002', '000003'],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=2,
            ) as count_persisted,
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['000001', '000002'],
            ) as load_persisted,
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_run,
        ):
            service._fail_stale_running_run(run)

        count_persisted.assert_called_once_with(
            8,
            trade_date='2026-04-03',
        )
        load_persisted.assert_called_once_with(
            8,
            trade_date='2026-04-03',
        )
        completion = complete_run.call_args
        self.assertEqual(completion.kwargs['symbols_succeeded'], 2)
        self.assertEqual(completion.kwargs['symbols_failed'], 1)
        self.assertEqual(
            completion.kwargs['symbols_succeeded']
            + completion.kwargs['symbols_failed'],
            run['symbols_total'],
        )
        self.assertEqual(
            completion.kwargs['run_metadata']['failed_symbols'],
            ['000003'],
        )
        self.assertEqual(
            completion.kwargs['run_metadata']['unattempted_symbols'],
            [],
        )
        self.assertFalse(
            completion.kwargs['run_metadata'][
                'collection_coverage_inconsistent'
            ]
        )

    def test_stale_run_rejects_equal_count_symbol_substitution(self) -> None:
        service = PykrxBackfillService()
        run = {
            'id': 8,
            'trade_date': '2026-04-03',
            'status': 'running',
            'symbols_total': 2,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'run_metadata': {
                'markets': ['KOSPI'],
                'requested_symbols': ['000001', '000002'],
                'failed_symbols': [],
                'unattempted_symbols': [],
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.count_eod_run_persisted_symbols',
                return_value=2,
            ),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['000001', '000003'],
            ),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_run,
        ):
            service._fail_stale_running_run(run)

        completion = complete_run.call_args
        self.assertEqual(completion.kwargs['symbols_succeeded'], 1)
        self.assertEqual(completion.kwargs['symbols_failed'], 1)
        self.assertEqual(
            completion.kwargs['run_metadata']['failed_symbols'],
            ['000002'],
        )
        self.assertEqual(
            completion.kwargs['run_metadata'][
                'unexpected_persisted_symbols'
            ],
            ['000003'],
        )
        self.assertEqual(
            completion.kwargs['run_metadata'][
                'unexpected_persisted_symbols_count'
            ],
            1,
        )
        self.assertTrue(
            completion.kwargs['run_metadata'][
                'collection_coverage_inconsistent'
            ]
        )

    def test_daily_signal_decider_followup_does_not_structurally_bypass_prerequisites(
        self,
    ) -> None:
        blocked_prerequisites = {
            'ready': False,
            'status': 'blocked',
            'missing_prerequisites': [{'stage': 'piotroski_filter'}],
        }

        with (
            patch(
                'app.agent_context.prerequisite_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                return_value=blocked_prerequisites,
            ),
            patch(
                'app.agent_context.retry_service.agent_feedback_retry_service.run_retry_cycles',
                return_value={'status': 'completed', 'completed': True},
            ) as run_retry_cycles,
            patch('app.core.settings.settings.openclaw_feedback_fast_path_bypass_prerequisite_gate', True),
        ):
            result = _run_daily_signal_decider_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'feedback_prerequisites_not_ready')
        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['prerequisites'], blocked_prerequisites)
        run_retry_cycles.assert_not_called()

    def test_daily_signal_decider_followup_does_not_bypass_or_force_when_prerequisites_are_ready(self) -> None:
        ready_prerequisites = {'ready': True, 'status': 'ready'}

        with (
            patch(
                'app.agent_context.prerequisite_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                return_value=ready_prerequisites,
            ),
            patch(
                'app.agent_context.retry_service.agent_feedback_retry_service.run_retry_cycles',
                return_value={'status': 'completed', 'completed': True},
            ) as run_retry_cycles,
            patch('app.core.settings.settings.openclaw_feedback_fast_path_bypass_prerequisite_gate', True),
        ):
            result = _run_daily_signal_decider_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertTrue(result['triggered'])
        self.assertFalse(result['prerequisite_gate_bypassed'])
        self.assertIsNone(result['prerequisites'])
        self.assertFalse(run_retry_cycles.call_args.kwargs['bypass_prerequisite_gate'])
        self.assertFalse(run_retry_cycles.call_args.kwargs.get('force', False))
        self.assertFalse(run_retry_cycles.call_args.kwargs['config'].force_recompute)

    def test_reconcile_latest_daily_run_status_uses_last_progress_heartbeat(self) -> None:
        service = PykrxBackfillService()
        stale_started_at = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        recent_progress_at = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        running_run = {
            'id': 18,
            'status': 'running',
            'trade_date': '2026-04-03',
            'symbols_total': 10,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'started_at': stale_started_at,
            'run_metadata': {
                'stage': 'technical_snapshots',
                'last_progress_at': recent_progress_at,
                'stage_progress': {
                    'technical_snapshots': {
                        'status': 'running',
                        'current_step': 'computing_symbol_snapshot',
                    },
                },
            },
        }

        with patch(
            'app.eod.pykrx_backfill.load_latest_eod_collection_run',
            return_value=running_run,
        ), patch.object(service, '_fail_stale_running_run') as fail_stale_run:
            result = service.reconcile_latest_daily_run_status(trade_date='2026-04-03')

        self.assertEqual(result, running_run)
        fail_stale_run.assert_not_called()

    def test_reconcile_latest_daily_run_status_marks_stale_stage_progress_failed(self) -> None:
        service = PykrxBackfillService()
        stale_progress_at = (datetime.now(timezone.utc) - timedelta(minutes=45)).isoformat()
        stale_run = {
            'id': 19,
            'status': 'running',
            'trade_date': '2026-04-03',
            'symbols_total': 4,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'started_at': (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            'run_metadata': {
                'stage': 'technical_snapshots',
                'markets': ['KOSPI'],
                'last_progress_at': stale_progress_at,
                'stage_progress': {
                    'price_collection': {
                        'status': 'completed',
                        'processed': 4,
                        'total': 4,
                        'succeeded': 4,
                        'failed': 0,
                    },
                    'technical_snapshots': {
                        'status': 'running',
                        'processed': 1,
                        'total': 4,
                        'succeeded': 1,
                        'failed': 0,
                        'current_step': 'computing_symbol_snapshot',
                        'current_activity': 'Computing daily technical snapshots for 000660',
                        'current_symbol': '000660',
                    },
                },
            },
        }
        failed_run = {
            **stale_run,
            'status': 'failed',
            'symbols_failed': 3,
            'error_message': 'Marked stale after 30 minutes without completion.',
            'run_metadata': {
                **stale_run['run_metadata'],
                'stage_progress': {
                    **stale_run['run_metadata']['stage_progress'],
                    'technical_snapshots': {
                        **stale_run['run_metadata']['stage_progress']['technical_snapshots'],
                        'status': 'failed',
                        'current_step': 'stale_run_failed',
                        'current_activity': 'Marked stale after 30 minutes without completion.',
                    },
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                side_effect=[stale_run, failed_run],
            ),
            patch('app.eod.pykrx_backfill.count_eod_run_persisted_symbols', return_value=1),
            patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols', return_value=['000001']),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service.reconcile_latest_daily_run_status(trade_date='2026-04-03')

        self.assertEqual(result['status'], 'failed')
        complete_run.assert_called_once()
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(
            completed_metadata['stage_progress']['technical_snapshots']['status'],
            'failed',
        )
        self.assertEqual(
            completed_metadata['stage_progress']['technical_snapshots']['current_activity'],
            'Marked stale after 30 minutes without completion.',
        )
        self.assertEqual(completed_metadata['stale_failure_reason'], 'Marked stale after 30 minutes without completion.')

    def test_run_market_data_pipeline_continues_when_progress_update_fails(self) -> None:
        service = PykrxBackfillService()
        symbols = [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')]

        with (
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch('app.eod.pykrx_backfill.log_fetch_event') as log_fetch_event,
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(service, '_map_symbol_bars', return_value=[{
                'trade_date': '2026-04-03',
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 3,
                'turnover': 4,
            }]),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=type('Frame', (), {'empty': False})()),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1) as persist_bars,
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=[],
                    ),
                },
            ) as price_contexts,
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 1,
                    'snapshots_failed': 0,
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=[],
                    ),
                },
            ) as technical_snapshots,
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'action_summaries_persisted': 1,
                    'action_summaries_failed': 0,
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=[],
                    ),
                },
            ) as action_summaries,
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ) as trending_ranks,
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 2, 'features_failed': 0},
            ) as signal_features,
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={'results_persisted': 1, 'results_failed': 0, 'summary': {'passed_count': 1, 'watchlist_count': 0}},
            ) as market_risk_filter,
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress', side_effect=RuntimeError('progress write failed')),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service.run_backfill(
                start_date='20260403',
                end_date='20260403',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['symbols_succeeded'], 1)
        self.assertEqual(result['price_contexts']['contexts_persisted'], 1)
        self.assertEqual(result['technical_snapshots']['snapshots_persisted'], 1)
        self.assertEqual(result['action_summaries']['action_summaries_persisted'], 1)
        self.assertEqual(result['trending_ranks']['reason'], 'stage_not_requested')
        self.assertEqual(result['market_risk_filter']['reason'], 'stage_not_requested')
        self.assertFalse(persist_bars.call_args.kwargs['update_current_symbol_state'])
        price_contexts.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            exact_scope_symbols=['005930'],
            source='pykrx',
            run_id=12,
            include_retry_metadata=True,
            authoritative_market_scope=None,
        )
        technical_snapshots.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            exact_scope_symbols=['005930'],
            source='pykrx',
            run_id=12,
            progress_callback=ANY,
            include_retry_metadata=True,
            authoritative_market_scope=None,
        )
        action_summaries.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            exact_scope_symbols=['005930'],
            source='pykrx',
            run_id=12,
            progress_callback=ANY,
            include_retry_metadata=True,
            authoritative_market_scope=None,
        )
        trending_ranks.assert_not_called()
        signal_features.assert_not_called()
        market_risk_filter.assert_not_called()
        self.assertTrue(
            any(
                call.kwargs.get('operation') == 'update_collection_progress'
                and call.kwargs.get('status') == 'failed'
                for call in log_fetch_event.call_args_list
            )
        )
        complete_run.assert_called_once_with(
            12,
            status='completed',
            symbols_succeeded=1,
            symbols_failed=0,
            error_message=None,
            run_metadata=ANY,
        )

    @patch('app.eod.pykrx_backfill.settings.openclaw_feedback_trigger_after_fundamentals_refresh', False)
    @patch(
        'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_evaluation_followup',
        return_value={'triggered': True, 'summary': {'not_computed_count': 0}},
    )
    def test_run_collection_advances_exact_partial_cohorts_through_decider(
        self,
        evaluation_followup,
    ) -> None:
        service = PykrxBackfillService()
        followup_order: list[str] = []
        symbols = [
            PykrxSymbol(
                symbol=f'{index:06d}',
                market='KOSPI',
                name_ko=f'Symbol {index}',
            )
            for index in range(1, 7)
        ]
        symbol_codes = [item.symbol for item in symbols]

        with ExitStack() as stack:
            stack.enter_context(patch('app.eod.pykrx_backfill.settings.dart_api_key', 'test-dart-key'))
            stack.enter_context(
                patch.object(
                    service,
                    'load_active_universe',
                    return_value=symbols,
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    'fetch_recent_universe_snapshot',
                    return_value=_point_in_time_universe_snapshot(symbols),
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    '_persist_universe',
                    return_value=len(symbols),
                )
            )
            stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
            stack.enter_context(patch.object(service, '_load_symbol_fundamentals', return_value=None))
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[{
                        'trade_date': '2026-04-03',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }],
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.create_eod_collection_run',
                    return_value=12,
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                    side_effect=lambda _start, _end, symbol: SimpleNamespace(
                        empty=symbol == symbol_codes[0]
                    ),
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1))
            stack.enter_context(patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'))
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                    return_value=_ready_pykrx_enrichment_result(),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                    return_value={
                        'contexts_persisted': len(symbols) - 1,
                        'contexts_failed': 1,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'price_contexts_scope_outcomes_v1'
                            ),
                            selected_symbols=symbol_codes,
                            successful_symbols=symbol_codes[1:],
                            retry_debt_symbols=symbol_codes[:1],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                    return_value={
                        'snapshots_persisted': len(symbols) - 1,
                        'snapshots_failed': 1,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'technical_snapshots_scope_outcomes_v1'
                            ),
                            selected_symbols=symbol_codes,
                            successful_symbols=symbol_codes[1:],
                            retry_debt_symbols=symbol_codes[:1],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                    return_value={
                        'action_summaries_persisted': len(symbols) - 2,
                        'action_summaries_failed': 2,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'action_summaries_scope_outcomes_v1'
                            ),
                            selected_symbols=symbol_codes,
                            successful_symbols=symbol_codes[2:],
                            retry_debt_symbols=symbol_codes[:2],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                    return_value={'trending_ranks_persisted': 3},
                )
            )
            signal_feature_refresh = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                    side_effect=lambda **_: followup_order.append('signal_features')
                    or {
                        'status': 'completed',
                        'features_persisted': 1,
                        'features_failed': 0,
                        'sources': [],
                        'source_counts': {},
                        'gate_status': 'ready',
                        **build_exact_symbol_scope_outcome(
                            manifest_version=(
                                'signal_features_scope_outcomes_v1'
                            ),
                            selected_symbols=symbol_codes[2:3],
                            successful_symbols=symbol_codes[2:3],
                            retry_debt_symbols=[],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                    return_value={
                        'matched_symbols': symbol_codes,
                        'results_persisted': len(symbols) - 2,
                        'results_failed': 2,
                        'failed_symbols': [symbol_codes[0], symbol_codes[-1]],
                        'summary': {
                            'passed_count': len(symbols) - 2,
                            'watchlist_count': 0,
                        },
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'market_risk_filter_scope_outcomes_v1'
                            ),
                            selected_symbols=symbol_codes,
                            successful_symbols=symbol_codes[1:-1],
                            retry_debt_symbols=[symbol_codes[0], symbol_codes[-1]],
                        ),
                    },
                )
            )
            fundamentals_refresh = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh',
                    return_value={
                        'status': 'partial',
                        'trigger': 'eod_pykrx_followup',
                        'results_persisted': 2,
                        'results_failed': 1,
                        'gate_status': 'ready',
                        'scope_finalization': {'ready': True},
                        **build_exact_symbol_scope_outcome(
                            manifest_version=(
                                'dart_fundamentals_scope_outcomes_v1'
                            ),
                            selected_symbols=symbol_codes[2:5],
                            successful_symbols=symbol_codes[2:4],
                            retry_debt_symbols=symbol_codes[4:5],
                        ),
                    },
                )
            )
            piotroski_followup = stack.enter_context(
                patch(
                    'app.signals.piotroski_filter.daily_piotroski_filter_service.'
                    'rebuild_for_market_date',
                    side_effect=lambda **_: followup_order.append(
                        'piotroski_filter'
                    )
                    or {
                        'status': 'partial',
                        'results_persisted': 1,
                        'results_failed': 1,
                        'gate_status': 'ready',
                        'scope_finalization': {'ready': True},
                        **build_exact_symbol_scope_outcome(
                            manifest_version=(
                                'piotroski_scope_outcomes_v1'
                            ),
                            selected_symbols=symbol_codes[2:4],
                            successful_symbols=symbol_codes[2:3],
                            retry_debt_symbols=symbol_codes[3:4],
                        ),
                    },
                )
            )
            factor_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_factor_followup',
                    side_effect=lambda **_: followup_order.append('factor_scoring')
                    or {
                        'triggered': True,
                        'status': 'completed',
                        'results_persisted': 1,
                        'results_failed': 0,
                        'gate_status': 'ready',
                        'blocking_status': 'non_blocking',
                        **build_exact_symbol_scope_outcome(
                            manifest_version=(
                                'factor_followup_scope_outcomes_v1'
                            ),
                            selected_symbols=symbol_codes[2:3],
                            successful_symbols=symbol_codes[2:3],
                            retry_debt_symbols=[],
                        ),
                    },
                )
            )
            external_news_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_external_news_followup',
                    side_effect=lambda **_: followup_order.append('external_news_collect')
                    or {'triggered': True, 'status': 'disabled', 'reason': 'naver_search_credentials_missing'},
                )
            )
            external_news_enrichment_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_external_news_enrichment_followup',
                    side_effect=lambda **_: followup_order.append('external_news_enrich')
                    or {'triggered': True, 'status': 'completed', 'classifications_persisted': 0},
                )
            )
            macro_context_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_macro_context_followup',
                    side_effect=lambda **_: followup_order.append('macro_context_refresh')
                    or {'triggered': True, 'status': 'completed', 'freshness_gate': 'fresh'},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner.'
                    '_run_daily_intraday_snapshot_followup',
                    return_value={
                        'triggered': False,
                        'status': 'skipped',
                        'reason': 'followup_disabled',
                    },
                )
            )
            feedback_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_feedback_followup',
                    side_effect=lambda **_: followup_order.append('agent_feedback_summaries')
                    or {'triggered': True, 'status': 'completed'},
                )
            )
            signal_decider_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_followup',
                    return_value={'triggered': True, 'status': 'completed', 'decision_snapshots': 2},
                )
            )
            evaluation_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_evaluation_followup',
                    return_value={'triggered': True, 'status': 'completed', 'summary': {'computed_count': 4}},
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI', 'KOSDAQ'),
                trigger='manual_daily_api',
                refresh_universe=True,
                mode='daily_auto',
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['fundamentals_followup']['status'], 'partial')
        self.assertEqual(
            result['fundamentals_followup']['successful_stock_codes'],
            symbol_codes[2:4],
        )
        self.assertEqual(
            result['fundamentals_followup']['retry_debt_stock_codes'],
            symbol_codes[4:5],
        )
        self.assertEqual(result['piotroski_filter_followup']['status'], 'partial')
        fundamentals_refresh.assert_called_once_with(
            market_date='2026-04-03',
            capture_limit=500,
            trigger='eod_pykrx_followup',
            run_piotroski_followup=False,
            run_feedback_followup=False,
            progress_callback=ANY,
            symbols=symbol_codes[2:5],
        )
        piotroski_followup.assert_called_once_with(
            market_date='2026-04-03',
            symbols=symbol_codes[2:4],
            ranking_symbols=symbol_codes,
            exact_scope_symbols=symbol_codes,
            authoritative_market_scope=None,
        )
        factor_followup.assert_called_once_with(
            market_date='2026-04-03',
            exact_membership_symbols=symbol_codes,
            admitted_symbols=symbol_codes[2:3],
            authoritative_market_scope=ANY,
            stage_executor=ANY,
            stage_result_callback=ANY,
        )
        external_news_followup.assert_called_once_with(market_date='2026-04-03', enabled=True)
        external_news_enrichment_followup.assert_called_once_with(market_date='2026-04-03', enabled=True)
        macro_context_followup.assert_called_once_with(market_date='2026-04-03', enabled=True)
        self.assertEqual(
            followup_order[:6],
            [
                'piotroski_filter',
                'factor_scoring',
                'signal_features',
                'external_news_collect',
                'external_news_enrich',
                'macro_context_refresh',
            ],
        )
        signal_feature_refresh.assert_called_once_with(
            market_date='2026-04-03',
            include_retry_metadata=True,
            exact_symbols=symbol_codes,
            admitted_symbols=symbol_codes[2:3],
        )
        feedback_followup.assert_called_once_with(
            market_date='2026-04-03',
            enabled=False,
            knowledge_cutoff_at=ANY,
            progress_callback=ANY,
            symbols=symbol_codes[2:3],
        )
        signal_decider_followup.assert_called_once_with(
            market_date='2026-04-03',
            enabled=False,
            knowledge_cutoff_at=ANY,
            progress_callback=ANY,
            symbols=symbol_codes[2:3],
        )
        evaluation_followup.assert_called_once_with(market_date='2026-04-03', enabled=True)
        self.assertEqual(
            result['factor_scoring_followup']['successful_stock_codes'],
            symbol_codes[2:3],
        )
        self.assertEqual(
            result['factor_scoring_followup']['retry_debt_stock_codes'],
            [],
        )
        self.assertEqual(
            result['signal_feature_followup']['successful_stock_codes'],
            symbol_codes[2:3],
        )
        self.assertEqual(
            result['signal_feature_followup']['retry_debt_stock_codes'],
            [],
        )
        self.assertEqual(
            result['external_news_followup'],
            {'triggered': True, 'status': 'disabled', 'reason': 'naver_search_credentials_missing'},
        )
        self.assertEqual(
            result['external_news_enrichment_followup'],
            {'triggered': True, 'status': 'completed', 'classifications_persisted': 0},
        )
        self.assertEqual(
            result['macro_context_followup'],
            {'triggered': True, 'status': 'completed', 'freshness_gate': 'fresh'},
        )
        self.assertEqual(
            result['feedback_followup'],
            {'triggered': True, 'status': 'completed'},
        )
        self.assertEqual(
            result['signal_decider_followup'],
            {'triggered': True, 'status': 'completed', 'decision_snapshots': 2},
        )
        self.assertEqual(
            result['signal_decider_evaluation_followup'],
            {'triggered': True, 'status': 'completed', 'summary': {'computed_count': 4}},
        )

    def test_primary_piotroski_partial_blocks_factors_and_signal_features(
        self,
    ) -> None:
        service = PykrxBackfillService()
        symbol = PykrxSymbol(
            symbol='005930',
            market='KOSPI',
            name_ko='Samsung Electronics',
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch('app.eod.pykrx_backfill.settings.dart_api_key', 'test-dart-key')
            )
            stack.enter_context(
                patch.object(
                    service,
                    'resolve_latest_trade_date',
                    return_value='2026-04-03',
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[{
                        'trade_date': '2026-04-03',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }],
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.create_eod_collection_run',
                    return_value=12,
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                    return_value=SimpleNamespace(empty=False),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._enrich_bars_with_market_turnover',
                    side_effect=lambda **kwargs: kwargs['bars'],
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.persist_daily_price_bars',
                    return_value=1,
                )
            )
            stack.enter_context(
                patch('app.eod.pykrx_backfill.update_eod_collection_run_progress')
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.pykrx_enrichment_service.'
                    'collect_for_market_date',
                    return_value=_ready_pykrx_enrichment_result(),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_price_context_service.'
                    'rebuild_for_market_date',
                    return_value={
                        'contexts_persisted': 1,
                        'contexts_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'price_contexts_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_technical_snapshot_service.'
                    'rebuild_for_market_date',
                    return_value={
                        'snapshots_persisted': 1,
                        'snapshots_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'technical_snapshots_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_action_summary_service.'
                    'rebuild_for_market_date',
                    return_value={
                        'action_summaries_persisted': 1,
                        'action_summaries_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'action_summaries_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                    return_value={'trending_ranks_persisted': 1},
                )
            )
            signal_features = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_signal_feature_service.'
                    'refresh_for_market_date',
                    return_value={
                        'features_persisted': 1,
                        'features_failed': 0,
                        'sources': [],
                        'source_counts': {},
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_market_risk_filter_service.'
                    'rebuild_for_market_date',
                    return_value={
                        'results_persisted': 1,
                        'results_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'market_risk_filter_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            fundamentals = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.dart_fundamentals_capture_service.'
                    'run_daily_incremental_refresh',
                    return_value={'status': 'completed'},
                )
            )
            stack.enter_context(
                patch(
                    'app.signals.piotroski_filter.daily_piotroski_filter_service.'
                    'rebuild_for_market_date',
                    return_value={
                        'status': 'partial',
                        'results_persisted': 98,
                        'results_failed': 2,
                        'scope_finalization': {'ready': True},
                    },
                )
            )
            factors = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_factor_followup',
                    return_value=_completed_factor_followup_result(),
                )
            )
            for target in (
                '_run_daily_external_news_followup',
                '_run_daily_external_news_enrichment_followup',
                '_run_daily_macro_context_followup',
                '_run_daily_intraday_snapshot_followup',
                '_run_daily_feedback_followup',
                '_run_daily_signal_decider_followup',
                '_run_daily_signal_decider_evaluation_followup',
            ):
                stack.enter_context(
                    patch(
                        f'app.eod.pykrx_pipeline_runner.{target}',
                        return_value={'status': 'completed'},
                    )
                )
            stack.enter_context(
                patch('app.eod.pykrx_backfill.complete_eod_collection_run')
            )

            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=tuple(
                    stage
                    for stage in PYKRX_STAGE_SEQUENCE
                    if stage != 'trending_ranks'
                ),
                symbols_override=[symbol],
                retry_context={
                    'inherited_universe_snapshot': (
                        _stored_daily_universe_snapshot(
                            [symbol],
                            market_date='2026-04-03',
                        )
                    ),
                },
            )

        self.assertEqual(result['piotroski_filter_followup']['status'], 'partial')
        self.assertEqual(result['factor_scoring_followup']['status'], 'blocked')
        self.assertEqual(
            result['factor_scoring_followup']['reason'],
            'piotroski_filter_incomplete',
        )
        self.assertEqual(result['signal_feature_followup']['status'], 'blocked')
        factors.assert_not_called()
        signal_features.assert_not_called()
        self.assertFalse(
            fundamentals.call_args.kwargs['run_piotroski_followup']
        )

    @patch('app.eod.pykrx_backfill.settings.openclaw_feedback_trigger_after_fundamentals_refresh', False)
    def test_factor_ownership_loss_aborts_entire_auto_followup_chain(self) -> None:
        service = PykrxBackfillService()
        fake_run_state_service = _FakeDailyPipelineRunStateService()
        ownership_lost_result = {
            'triggered': False,
            'reason': 'pipeline_run_ownership_lost',
            'market_date': '2026-04-03',
            'status': 'interrupted',
            'stages': {
                'momentum_factors': {
                    'status': 'interrupted',
                    'result': {
                        'status': 'interrupted',
                        'reason': 'pipeline_run_ownership_lost',
                    },
                },
            },
        }

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.settings.dart_api_key',
                    'test-dart-key',
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_pipeline_run_state_service',
                    fake_run_state_service,
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    'load_active_universe',
                    return_value=[
                        PykrxSymbol(
                            symbol='005930',
                            market='KOSPI',
                            name_ko='Samsung Electronics',
                        )
                    ],
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    'fetch_recent_universe_snapshot',
                    return_value=_point_in_time_universe_snapshot(
                        [
                            PykrxSymbol(
                                symbol='005930',
                                market='KOSPI',
                                name_ko='Samsung Electronics',
                            )
                        ],
                    ),
                )
            )
            stack.enter_context(
                patch.object(service, '_persist_universe', return_value=1)
            )
            stack.enter_context(
                patch.object(
                    service,
                    'resolve_latest_trade_date',
                    return_value='2026-04-03',
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    '_load_symbol_fundamentals',
                    return_value=None,
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[
                        {
                            'trade_date': '2026-04-03',
                            'open_price': 1,
                            'high_price': 2,
                            'low_price': 1,
                            'close_price': 2,
                            'volume': 3,
                            'turnover': 4,
                        }
                    ],
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.create_eod_collection_run',
                    return_value=12,
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                    return_value=SimpleNamespace(empty=False),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.persist_daily_price_bars',
                    return_value=1,
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.update_eod_collection_run_progress'
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.pykrx_enrichment_service.'
                    'collect_for_market_date',
                    return_value=_ready_pykrx_enrichment_result(),
                )
            )
            for target, return_value in (
                (
                    'daily_price_context_service.rebuild_for_market_date',
                    {
                        'contexts_persisted': 1,
                        'contexts_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'price_contexts_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                ),
                (
                    'daily_technical_snapshot_service.rebuild_for_market_date',
                    {
                        'snapshots_persisted': 1,
                        'snapshots_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'technical_snapshots_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                ),
                (
                    'daily_action_summary_service.rebuild_for_market_date',
                    {
                        'action_summaries_persisted': 1,
                        'action_summaries_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'action_summaries_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                ),
                (
                    'compute_and_persist_daily_trending_ranks',
                    {'trending_ranks_persisted': 3},
                ),
                (
                    'daily_signal_feature_service.refresh_for_market_date',
                    {
                        'features_persisted': 2,
                        'features_failed': 0,
                        'sources': [],
                        'source_counts': {},
                    },
                ),
                (
                    'daily_market_risk_filter_service.rebuild_for_market_date',
                    {
                        'results_persisted': 1,
                        'results_failed': 0,
                        'summary': {
                            'passed_count': 1,
                            'watchlist_count': 0,
                        },
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'market_risk_filter_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                ),
            ):
                stack.enter_context(
                    patch(
                        f'app.eod.pykrx_backfill.{target}',
                        return_value=return_value,
                    )
                )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.'
                    'dart_fundamentals_capture_service.'
                    'run_daily_incremental_refresh',
                    return_value={
                        'status': 'completed',
                        'trigger': 'eod_pykrx_followup',
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.signals.piotroski_filter.'
                    'daily_piotroski_filter_service.rebuild_for_market_date',
                    return_value={
                        'status': 'completed',
                        'results_persisted': 1,
                        'results_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'piotroski_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner.'
                    '_run_daily_factor_followup',
                    return_value=ownership_lost_result,
                )
            )
            downstream_calls = {
                target: stack.enter_context(
                    patch(f'app.eod.pykrx_pipeline_runner.{target}')
                )
                for target in (
                    '_run_daily_external_news_followup',
                    '_run_daily_external_news_enrichment_followup',
                    '_run_daily_macro_context_followup',
                    '_run_daily_intraday_snapshot_followup',
                    '_run_daily_feedback_followup',
                    '_run_daily_signal_decider_followup',
                    '_run_daily_next_open_execution_plan_followup',
                    '_run_daily_signal_decider_evaluation_followup',
                )
            }
            stack.enter_context(
                patch('app.eod.pykrx_backfill.complete_eod_collection_run')
            )

            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI', 'KOSDAQ'),
                trigger='auto_close_scheduler',
                refresh_universe=True,
                mode='daily_auto',
                selected_stages=PYKRX_STAGE_SEQUENCE,
                track_daily_pipeline=True,
            )

        self.assertEqual(result['status'], 'interrupted')
        self.assertEqual(result['reason'], 'pipeline_run_ownership_lost')
        self.assertEqual(fake_run_state_service.finish_runs, [])
        for downstream_call in downstream_calls.values():
            downstream_call.assert_not_called()

    @patch('app.eod.pykrx_backfill.settings.openclaw_feedback_trigger_after_fundamentals_refresh', False)
    def test_run_market_data_pipeline_tracks_auto_followups_in_daily_pipeline_state(self) -> None:
        service = PykrxBackfillService()
        fake_run_state_service = _FakeDailyPipelineRunStateService()
        factor_followup_result = _completed_factor_followup_result()
        factor_followup_result['status'] = 'partial'
        factor_followup_result['stages']['momentum_factors'] = {
            'status': 'failed',
            'reason': 'factor_provider_failure',
        }

        with ExitStack() as stack:
            stack.enter_context(patch('app.eod.pykrx_backfill.settings.dart_api_key', 'test-dart-key'))
            stack.enter_context(
                patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service', fake_run_state_service)
            )
            stack.enter_context(
                patch.object(
                    service,
                    'load_active_universe',
                    return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    'fetch_recent_universe_snapshot',
                    return_value=_point_in_time_universe_snapshot(
                        [
                            PykrxSymbol(
                                symbol='005930',
                                market='KOSPI',
                                name_ko='Samsung Electronics',
                            )
                        ],
                    ),
                )
            )
            stack.enter_context(
                patch.object(service, '_persist_universe', return_value=1)
            )
            stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
            stack.enter_context(patch.object(service, '_load_symbol_fundamentals', return_value=None))
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[{
                        'trade_date': '2026-04-03',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }],
                )
            )
            create_eod_run = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.create_eod_collection_run',
                    return_value=12,
                )
            )
            stack.enter_context(
                patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False))
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1))
            stack.enter_context(patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'))
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                    return_value=_ready_pykrx_enrichment_result(),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                    return_value={
                        'contexts_persisted': 1,
                        'contexts_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'price_contexts_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                    return_value={
                        'snapshots_persisted': 1,
                        'snapshots_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'technical_snapshots_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                    return_value={
                        'action_summaries_persisted': 1,
                        'action_summaries_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'action_summaries_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                    return_value={'trending_ranks_persisted': 3},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                    return_value={'features_persisted': 2, 'features_failed': 0, 'sources': [], 'source_counts': {}},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                    return_value={
                        'results_persisted': 1,
                        'results_failed': 0,
                        'summary': {
                            'passed_count': 1,
                            'watchlist_count': 0,
                        },
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'market_risk_filter_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            fundamentals_refresh = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh',
                    return_value={'status': 'completed', 'trigger': 'eod_pykrx_followup'},
                )
            )
            piotroski_followup = stack.enter_context(
                patch(
                    'app.signals.piotroski_filter.daily_piotroski_filter_service.'
                    'rebuild_for_market_date',
                    return_value={
                        'status': 'completed',
                        'results_persisted': 1,
                        'results_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'piotroski_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_factor_followup',
                    return_value=factor_followup_result,
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_external_news_followup',
                    return_value={'triggered': True, 'status': 'completed', 'items_persisted': 3},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner.'
                    '_run_daily_external_news_enrichment_followup',
                    return_value={
                        'triggered': True,
                        'status': 'completed',
                        'classifications_persisted': 3,
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner.'
                    '_run_daily_macro_context_followup',
                    return_value={
                        'triggered': True,
                        'status': 'completed',
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner.'
                    '_run_daily_intraday_snapshot_followup',
                    return_value={
                        'triggered': False,
                        'status': 'skipped',
                        'safe_skip': True,
                        'reason': 'non_trading_day',
                        'market_state': 'WEEKEND',
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_feedback_followup',
                    return_value={'triggered': False, 'reason': 'followup_disabled', 'market_date': '2026-04-03'},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_followup',
                    return_value={'triggered': False, 'reason': 'followup_disabled', 'market_date': '2026-04-03'},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_evaluation_followup',
                    return_value={'triggered': True, 'status': 'completed', 'summary': {'computed_count': 4}},
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))

            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI', 'KOSDAQ'),
                trigger='auto_close_scheduler',
                refresh_universe=True,
                mode='daily_auto',
                selected_stages=PYKRX_STAGE_SEQUENCE,
                track_daily_pipeline=True,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(fake_run_state_service.start_runs[0]['market_date'], '2026-04-03')
        self.assertEqual(
            create_eod_run.call_args.kwargs['run_metadata'][
                'daily_pipeline_run_id'
            ],
            501,
        )
        self.assertEqual(
            create_eod_run.call_args.kwargs['run_metadata'][
                'expected_daily_pipeline_stages'
            ],
            list(AUTO_DAILY_TRACKED_STAGE_ORDER),
        )
        self.assertEqual(
            fake_run_state_service.linked_eod_runs,
            [{'pipeline_run_id': 501, 'eod_run_id': 12}],
        )
        self.assertLess(
            fake_run_state_service.events.index('link_eod_run'),
            fake_run_state_service.events.index('update_stage'),
        )
        self.assertIn('fundamentals_daily_refresh', fake_run_state_service.start_runs[0]['resolved_stages'])
        self.assertIn('momentum_factors', fake_run_state_service.start_runs[0]['resolved_stages'])
        self.assertIn('signal_decider', fake_run_state_service.start_runs[0]['resolved_stages'])
        self.assertIn('signal_decider_evaluations', fake_run_state_service.start_runs[0]['resolved_stages'])
        stage_statuses = {
            (item['stage_id'], item['status'])
            for item in fake_run_state_service.stage_updates
        }
        self.assertIn(('fundamentals_daily_refresh', 'running'), stage_statuses)
        self.assertIn(('fundamentals_daily_refresh', 'completed'), stage_statuses)
        self.assertIn(('piotroski_filter', 'running'), stage_statuses)
        self.assertIn(('piotroski_filter', 'completed'), stage_statuses)
        self.assertIn(('momentum_factors', 'running'), stage_statuses)
        self.assertIn(('momentum_factors', 'failed'), stage_statuses)
        self.assertIn(('signal_features', 'blocked'), stage_statuses)
        self.assertIn(('external_news_collect', 'completed'), stage_statuses)
        self.assertIn(('macro_context_refresh', 'completed'), stage_statuses)
        self.assertIn(('intraday_snapshots', 'running'), stage_statuses)
        self.assertIn(('intraday_snapshots', 'completed'), stage_statuses)
        self.assertIn(('signal_decider', 'blocked'), stage_statuses)
        self.assertIn(('signal_decider_evaluations', 'blocked'), stage_statuses)
        factor_failure_update = next(
            item
            for item in fake_run_state_service.stage_updates
            if item['stage_id'] == 'momentum_factors'
            and item['status'] == 'failed'
        )
        for count_key in ('processed', 'total', 'succeeded', 'failed'):
            self.assertEqual(factor_failure_update[count_key], 0)
        self.assertFalse(
            factor_failure_update['details']['failure_scope_known']
        )
        self.assertTrue(
            factor_failure_update['details']['stage_attempt_failed']
        )
        self.assertEqual(
            factor_failure_update['details']['result_summary']['reason'],
            'factor_provider_failure',
        )
        self.assertEqual(fake_run_state_service.finish_runs[-1]['status'], 'partial')
        intraday_terminal_update = next(
            item
            for item in reversed(fake_run_state_service.stage_updates)
            if item['stage_id'] == 'intraday_snapshots'
            and item['status'] == 'completed'
        )
        self.assertEqual(
            intraday_terminal_update['details']['result_summary']['status'],
            'skipped',
        )
        self.assertTrue(
            intraday_terminal_update['details']['result_summary']['safe_skip']
        )
        self.assertEqual(
            fake_run_state_service.finish_runs[-1]['run_metadata']['eod_collection_run_id'],
            12,
        )
        fundamentals_refresh.assert_called_once()
        self.assertIn('progress_callback', fundamentals_refresh.call_args.kwargs)
        self.assertFalse(
            fundamentals_refresh.call_args.kwargs['run_piotroski_followup']
        )
        piotroski_followup.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            ranking_symbols=['005930'],
            exact_scope_symbols=['005930'],
            authoritative_market_scope=ANY,
        )

    @patch(
        'app.eod.pykrx_backfill.settings.'
        'openclaw_feedback_trigger_after_fundamentals_refresh',
        False,
    )
    def test_followup_admission_checkpoint_failure_stops_all_followups(
        self,
    ) -> None:
        service = PykrxBackfillService()
        fake_run_state_service = _FakeDailyPipelineRunStateService()
        symbol = PykrxSymbol(
            symbol='005930',
            market='KOSPI',
            name_ko='Samsung Electronics',
        )

        def fail_admission_checkpoint(*_args, **kwargs):
            run_metadata = kwargs.get('run_metadata') or {}
            if 'followup_admission_scope_outcome' in run_metadata:
                raise RuntimeError('admission proof database unavailable')

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.settings.dart_api_key',
                    'test-dart-key',
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.'
                    'daily_pipeline_run_state_service',
                    fake_run_state_service,
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    'load_active_universe',
                    return_value=[symbol],
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    'fetch_recent_universe_snapshot',
                    return_value=_point_in_time_universe_snapshot([symbol]),
                )
            )
            stack.enter_context(
                patch.object(service, '_persist_universe', return_value=1)
            )
            stack.enter_context(
                patch.object(
                    service,
                    'resolve_latest_trade_date',
                    return_value='2026-04-03',
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    '_load_symbol_fundamentals',
                    return_value=None,
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[
                        {
                            'trade_date': '2026-04-03',
                            'open_price': 1,
                            'high_price': 2,
                            'low_price': 1,
                            'close_price': 2,
                            'volume': 3,
                            'turnover': 4,
                        }
                    ],
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.create_eod_collection_run',
                    return_value=12,
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                    return_value=SimpleNamespace(empty=False),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.persist_daily_price_bars',
                    return_value=1,
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.'
                    'update_eod_collection_run_progress',
                    side_effect=fail_admission_checkpoint,
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.pykrx_enrichment_service.'
                    'collect_for_market_date',
                    return_value=_ready_pykrx_enrichment_result(),
                )
            )
            for target, return_value in (
                (
                    'daily_price_context_service.rebuild_for_market_date',
                    {
                        'contexts_persisted': 1,
                        'contexts_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'price_contexts_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                ),
                (
                    'daily_technical_snapshot_service.'
                    'rebuild_for_market_date',
                    {
                        'snapshots_persisted': 1,
                        'snapshots_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'technical_snapshots_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                ),
                (
                    'daily_action_summary_service.rebuild_for_market_date',
                    {
                        'action_summaries_persisted': 1,
                        'action_summaries_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'action_summaries_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                ),
                (
                    'compute_and_persist_daily_trending_ranks',
                    {'trending_ranks_persisted': 1},
                ),
                (
                    'daily_market_risk_filter_service.'
                    'rebuild_for_market_date',
                    {
                        'results_persisted': 1,
                        'results_failed': 0,
                        'summary': {
                            'passed_count': 1,
                            'watchlist_count': 0,
                        },
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'market_risk_filter_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                ),
            ):
                stack.enter_context(
                    patch(
                        f'app.eod.pykrx_backfill.{target}',
                        return_value=return_value,
                    )
                )
            fundamentals_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.'
                    'dart_fundamentals_capture_service.'
                    'run_daily_incremental_refresh',
                    return_value={
                        'status': 'completed',
                        'trigger': 'eod_pykrx_followup',
                    },
                )
            )
            piotroski_followup = stack.enter_context(
                patch(
                    'app.signals.piotroski_filter.'
                    'daily_piotroski_filter_service.'
                    'rebuild_for_market_date'
                )
            )
            factor_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner.'
                    '_run_daily_factor_followup'
                )
            )
            signal_feature_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.'
                    'daily_signal_feature_service.'
                    'refresh_for_market_date'
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.'
                    'complete_eod_collection_run'
                )
            )

            with self.assertRaisesRegex(
                DailyPipelineTrackingWriteError,
                'follow-up admission checkpoint',
            ):
                service._run_market_data_pipeline(
                    start_date='20260403',
                    end_date='20260403',
                    markets=('KOSPI', 'KOSDAQ'),
                    trigger='auto_close_scheduler',
                    refresh_universe=True,
                    mode='daily_auto',
                    selected_stages=PYKRX_STAGE_SEQUENCE,
                    track_daily_pipeline=True,
                )

        fundamentals_followup.assert_not_called()
        piotroski_followup.assert_not_called()
        factor_followup.assert_not_called()
        signal_feature_followup.assert_not_called()
        self.assertEqual(
            fake_run_state_service.finish_runs[-1]['status'],
            'failed',
        )
        self.assertIn(
            'follow-up admission checkpoint',
            fake_run_state_service.finish_runs[-1]['error_message'],
        )

    def test_run_collection_fast_fails_when_price_provider_fails_all_symbols(self) -> None:
        service = PykrxBackfillService()
        fake_run_state_service = _FakeDailyPipelineRunStateService()

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    service,
                    'load_active_universe',
                    return_value=[
                        PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics'),
                        PykrxSymbol(symbol='000660', market='KOSPI', name_ko='SK Hynix'),
                    ],
                )
            )
            stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                    side_effect=RuntimeError('PyKRX stock client is unavailable'),
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12))
            stack.enter_context(patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'))
            complete_run = stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))
            fundamentals_refresh = stack.enter_context(
                patch('app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh')
            )
            technical_snapshots = stack.enter_context(
                patch('app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date')
            )
            action_summaries = stack.enter_context(
                patch('app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date')
            )
            market_risk = stack.enter_context(
                patch('app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date')
            )
            stack.enter_context(
                patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service', fake_run_state_service)
            )

            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=PYKRX_STAGE_SEQUENCE,
                track_daily_pipeline=True,
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['error_type'], 'PykrxProviderUnavailable')
        self.assertEqual(result['symbols_succeeded'], 0)
        self.assertEqual(result['symbols_failed'], 2)
        complete_run.assert_called_once()
        self.assertEqual(complete_run.call_args.kwargs['status'], 'failed')
        self.assertIn('provider appears unavailable', complete_run.call_args.kwargs['error_message'])
        fundamentals_refresh.assert_not_called()
        technical_snapshots.assert_not_called()
        action_summaries.assert_not_called()
        market_risk.assert_not_called()
        self.assertEqual(fake_run_state_service.finish_runs[-1]['status'], 'failed')
        price_collection_updates = [
            item
            for item in fake_run_state_service.stage_updates
            if item['stage_id'] == 'price_collection'
        ]
        self.assertEqual(price_collection_updates[-1]['status'], 'failed')
        self.assertEqual(price_collection_updates[-1]['current_step'], 'price_collection_provider_unavailable')

    def test_run_collection_aborts_on_price_persistence_database_failure(self) -> None:
        service = PykrxBackfillService()
        service.symbol_pause_seconds = 0
        symbols = [
            PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics'),
            PykrxSymbol(symbol='000660', market='KOSPI', name_ko='SK Hynix'),
        ]
        database_error = OperationalError(
            'INSERT INTO daily_price_bars',
            {},
            RuntimeError('database connection lost'),
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(service, 'load_active_universe', return_value=symbols)
            )
            stack.enter_context(
                patch.object(
                    service,
                    'resolve_latest_trade_date',
                    return_value='2026-04-03',
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[{
                        'trade_date': '2026-04-03',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }],
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._enrich_bars_with_market_turnover',
                    side_effect=lambda **kwargs: kwargs['bars'],
                )
            )
            price_fetch = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                    return_value=SimpleNamespace(empty=False),
                )
            )
            stack.enter_context(
                patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12)
            )
            persist_bars = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.persist_daily_price_bars',
                    side_effect=database_error,
                )
            )
            stack.enter_context(
                patch('app.eod.pykrx_backfill.update_eod_collection_run_progress')
            )
            complete_run = stack.enter_context(
                patch('app.eod.pykrx_backfill.complete_eod_collection_run')
            )

            with self.assertRaises(OperationalError):
                service._run_market_data_pipeline(
                    start_date='20260403',
                    end_date='20260403',
                    markets=('KOSPI',),
                    trigger='auto_close_scheduler',
                    refresh_universe=False,
                    mode='daily_auto',
                    selected_stages=('price_collection',),
                )

        self.assertEqual(price_fetch.call_count, 1)
        self.assertEqual(persist_bars.call_count, 1)
        complete_run.assert_called_once()
        self.assertEqual(complete_run.call_args.kwargs['status'], 'failed')
        self.assertEqual(complete_run.call_args.kwargs['symbols_succeeded'], 0)
        self.assertEqual(complete_run.call_args.kwargs['symbols_failed'], 0)
        self.assertIn(
            'database connection lost',
            complete_run.call_args.kwargs['error_message'],
        )

    def test_run_collection_aborts_daily_auto_price_loop_after_initial_provider_failures(self) -> None:
        service = PykrxBackfillService()
        fake_run_state_service = _FakeDailyPipelineRunStateService()
        symbols = [
            PykrxSymbol(symbol=str(index).zfill(6), market='KOSPI', name_ko=f'Symbol {index}')
            for index in range(25)
        ]

        with ExitStack() as stack:
            stack.enter_context(patch.object(service, 'load_active_universe', return_value=symbols))
            stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
            price_fetch = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                    side_effect=RuntimeError('PyKRX stock client is unavailable'),
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12))
            update_progress = stack.enter_context(
                patch('app.eod.pykrx_backfill.update_eod_collection_run_progress')
            )
            complete_run = stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))
            fundamentals_refresh = stack.enter_context(
                patch('app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh')
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_pipeline_run_state_service',
                    fake_run_state_service,
                )
            )

            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=PYKRX_STAGE_SEQUENCE,
                track_daily_pipeline=True,
            )

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['symbols_total'], 25)
        self.assertEqual(result['symbols_failed'], 20)
        self.assertEqual(price_fetch.call_count, 20)
        self.assertEqual(complete_run.call_args.kwargs['status'], 'failed')
        self.assertEqual(result['unattempted_symbols'], [f'{index:06d}' for index in range(20, 25)])
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(completed_metadata['requested_symbols'], [item.symbol for item in symbols])
        self.assertEqual(completed_metadata['failed_symbols'], [f'{index:06d}' for index in range(20)])
        self.assertEqual(completed_metadata['unattempted_symbols'], [f'{index:06d}' for index in range(20, 25)])
        latest_progress_metadata = update_progress.call_args.kwargs['run_metadata']
        self.assertEqual(latest_progress_metadata['failed_symbols'], [f'{index:06d}' for index in range(20)])
        self.assertEqual(latest_progress_metadata['unattempted_symbols'], [f'{index:06d}' for index in range(20, 25)])
        fundamentals_refresh.assert_not_called()
        self.assertEqual(fake_run_state_service.finish_runs[-1]['status'], 'failed')

    def test_grouped_daily_stage_run_rebuilds_price_contexts_with_technical_stage(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(
                service,
                'load_active_universe',
                return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch.object(service, '_load_symbol_fundamentals', return_value=None),
            patch.object(service, '_map_symbol_bars', return_value=[{
                'trade_date': '2026-04-03',
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 3,
                'turnover': 4,
            }]),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False)),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    'failed_symbols': [],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=[],
                    ),
                },
            ) as price_contexts,
            patch(
                'app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                return_value=_ready_pykrx_enrichment_result(),
            ) as pykrx_enrichment,
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'snapshots_persisted': 1,
                    'snapshots_failed': 0,
                    'failed_symbols': [],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=[],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'action_summaries_persisted': 1,
                    'action_summaries_failed': 0,
                    'failed_symbols': [],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=[],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 0, 'features_failed': 0, 'sources': [], 'source_counts': {}},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={
                    'results_persisted': 1,
                    'results_failed': 0,
                    'summary': {'passed_count': 1, 'watchlist_count': 0},
                },
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run'),
        ):
            result = service.run_daily_pipeline_stages(
                market_date='2026-04-03',
                stages=tuple(
                    stage
                    for stage in PYKRX_STAGE_SEQUENCE
                    if stage not in {'trending_ranks', 'market_risk_filter'}
                ),
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['pykrx_enrichment']['status'], 'completed')
        self.assertTrue(result['pykrx_enrichment']['downstream_preflight']['allowed'])
        self.assertEqual(
            result['pykrx_enrichment']['downstream_preflight']['category_row_counts'],
            {
                'sector_classification': 1,
                'investor_flow_symbol': 1,
                'short_selling': 1,
                'valuation_snapshot': 1,
            },
        )
        self.assertEqual(result['price_contexts']['contexts_persisted'], 1)
        pykrx_enrichment.assert_called_once_with(
            market_date='2026-04-03',
            markets=('KOSPI',),
            symbols=['005930'],
            use_full_market_coverage=True,
            daily_price_bar_lock_owned=True,
        )
        price_contexts.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            exact_scope_symbols=['005930'],
            source='pykrx',
            run_id=12,
            include_retry_metadata=True,
            authoritative_market_scope=None,
        )

    def test_grouped_enrichment_accepts_gate_ready_valuation_gap_without_retry(self) -> None:
        service = PykrxBackfillService()
        enrichment_result = _ready_pykrx_enrichment_result()
        enrichment_result['status'] = 'partial'
        enrichment_result['categories']['valuation_snapshot']['rows_persisted'] = 96
        for category in (
            'sector_classification',
            'investor_flow_symbol',
            'short_selling',
        ):
            enrichment_result['coverage']['categories'][category].update(
                {
                    'expected_rows': 100,
                    'present_rows': 100,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                    'decision_usable_present_rows': 100,
                    'decision_usable_ratio': 1.0,
                    'decision_usable_status': 'complete',
                }
            )
        enrichment_result['coverage']['categories']['valuation_snapshot'].update(
            {
                'expected_rows': 100,
                'present_rows': 96,
                'coverage_ratio': 0.96,
                'coverage_status': 'partial',
                'decision_usable_present_rows': 96,
                'decision_usable_ratio': 0.96,
                'decision_usable_status': 'partial',
                'complete_present_rows': 96,
                'completeness_ratio': 0.96,
                'completeness_status': 'partial',
            }
        )
        enrichment_result['categories']['foreign_ownership'].update(
            {
                'rows_persisted': 0,
                'errors': [{'error': 'provider unavailable'}],
            }
        )
        enrichment_result['coverage']['categories']['foreign_ownership'].update(
            {
                'expected_rows': 100,
                'present_rows': 0,
                'coverage_ratio': 0.0,
                'coverage_status': 'missing',
            }
        )

        with (
            patch.object(
                service,
                'load_active_universe',
                return_value=[
                    PykrxSymbol(
                        symbol='005930',
                        market='KOSPI',
                        name_ko='Samsung Electronics',
                    )
                ],
            ),
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch.object(service, '_load_symbol_fundamentals', return_value=None),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[{
                    'trade_date': '2026-04-03',
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 2,
                    'volume': 3,
                    'turnover': 4,
                }],
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=12,
            ),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    'failed_symbols': [],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=[],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                return_value=enrichment_result,
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service.run_daily_pipeline_stages(
                market_date='2026-04-03',
                stages=('price_collection', 'pykrx_enrichment'),
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertTrue(result['pykrx_enrichment']['downstream_preflight']['allowed'])
        self.assertEqual(
            result['pykrx_enrichment']['tolerated_failed_categories'],
            ['foreign_ownership', 'valuation_snapshot'],
        )
        self.assertEqual(result['pykrx_enrichment']['blocking_failed_categories'], [])
        self.assertNotIn('_retry_metadata', result['pykrx_enrichment'])
        self.assertIsNone(complete_run.call_args.kwargs['error_message'])

    def test_point_in_time_grouped_stage_rejects_missing_requested_membership(
        self,
    ) -> None:
        service = PykrxBackfillService()
        admitted_symbol = PykrxSymbol(
            symbol='005930',
            market='KOSPI',
            name_ko='Friday member',
        )

        with (
            patch(
                'app.eod.pykrx_backfill._current_krx_calendar_date',
                return_value=date(2026, 9, 14),
            ),
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-09-11',
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    [admitted_symbol],
                    market_date='2026-09-11',
                ),
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
            ) as run_pipeline,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'missing requested symbols: 099999',
            ):
                service.run_daily_pipeline_stages(
                    market_date='2026-09-11',
                    stages=['price_collection'],
                    markets=('KOSPI',),
                    trigger='manual_api',
                    symbols=['005930', '099999'],
                )

        run_pipeline.assert_not_called()

    @patch(
        'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_evaluation_followup',
        return_value={'triggered': True, 'summary': {'not_computed_count': 0}},
    )
    def test_run_collection_skips_fundamentals_followup_when_dart_is_not_configured(self, evaluation_followup) -> None:
        service = PykrxBackfillService()

        with ExitStack() as stack:
            stack.enter_context(patch('app.eod.pykrx_backfill.settings.dart_api_key', ''))
            stack.enter_context(
                patch.object(
                    service,
                    'load_active_universe',
                    return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
                )
            )
            stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
            stack.enter_context(patch.object(service, '_load_symbol_fundamentals', return_value=None))
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[{
                        'trade_date': '2026-04-03',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }],
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12))
            stack.enter_context(
                patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False))
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1))
            stack.enter_context(patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'))
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                    return_value=_ready_pykrx_enrichment_result(),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                    return_value={
                        'contexts_persisted': 1,
                        'contexts_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'price_contexts_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                    return_value={
                        'snapshots_persisted': 1,
                        'snapshots_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'technical_snapshots_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                    return_value={
                        'action_summaries_persisted': 1,
                        'action_summaries_failed': 0,
                        **_atomic_exact_scope_proof(
                            manifest_version=(
                                'action_summaries_scope_outcomes_v1'
                            ),
                            selected_symbols=['005930'],
                        ),
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                    return_value={'trending_ranks_persisted': 3},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                    return_value={'features_persisted': 0, 'features_failed': 0, 'sources': [], 'source_counts': {}},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                    return_value={'results_persisted': 1, 'results_failed': 0, 'summary': {'passed_count': 1, 'watchlist_count': 0}},
                )
            )
            fundamentals_refresh = stack.enter_context(
                patch('app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh')
            )
            evaluation_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_evaluation_followup',
                    return_value={'triggered': True, 'status': 'warning', 'summary': {'not_computed_count': 1}},
                )
            )
            signal_decider_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_followup',
                    return_value={'triggered': True, 'status': 'completed', 'decision_snapshots': 1},
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='manual_daily_api',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=tuple(
                    stage
                    for stage in PYKRX_STAGE_SEQUENCE
                    if stage not in {'trending_ranks', 'market_risk_filter'}
                ),
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            result['fundamentals_followup'],
            {
                'triggered': False,
                'reason': 'dart_api_key_not_configured',
                'market_date': '2026-04-03',
            },
        )
        self.assertEqual(
            result['factor_scoring_followup'],
            {
                'triggered': False,
                'status': 'blocked',
                'reason': 'dart_api_key_not_configured',
                'market_date': '2026-04-03',
            },
        )
        self.assertEqual(
            result['piotroski_filter_followup'],
            {
                'triggered': False,
                'status': 'blocked',
                'reason': 'dart_api_key_not_configured',
                'market_date': '2026-04-03',
                'upstream_status': 'skipped',
                'stage': 'piotroski_filter',
            },
        )
        self.assertIn('external_news_followup', result)
        signal_decider_followup.assert_not_called()
        evaluation_followup.assert_not_called()
        self.assertEqual(
            result['signal_decider_followup'],
            {
                'triggered': False,
                'status': 'blocked',
                'reason': 'factor_scoring_followup_incomplete',
                'market_date': '2026-04-03',
                'upstream_status': 'blocked',
                'stage': 'signal_decider',
            },
        )
        self.assertEqual(
            result['signal_decider_evaluation_followup'],
            {
                'triggered': False,
                'status': 'blocked',
                'reason': 'factor_scoring_followup_incomplete',
                'market_date': '2026-04-03',
                'upstream_status': 'blocked',
                'stage': 'signal_decider_evaluations',
            },
        )
        fundamentals_refresh.assert_not_called()

    def test_run_market_data_pipeline_keeps_retryable_status_when_snapshot_persistence_has_failures(self) -> None:
        service = PykrxBackfillService()
        symbols = [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')]

        with (
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(service, '_map_symbol_bars', return_value=[{
                'trade_date': '2026-04-03',
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 3,
                'turnover': 4,
            }]),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=type('Frame', (), {'empty': False})()),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    'failed_symbols': [],
                    'items': [],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=[],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'snapshots_persisted': 0,
                    'snapshots_failed': 1,
                    'failed_symbols': ['005930'],
                    'items': [],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=[],
                        retry_debt_symbols=['005930'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'action_summaries_persisted': 0,
                    'action_summaries_failed': 1,
                    'failed_symbols': ['005930'],
                    'items': [],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=[],
                        retry_debt_symbols=['005930'],
                    ),
                },
            ) as rebuild_action_summaries,
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 0, 'features_failed': 0, 'failed_symbols': []},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={'results_persisted': 1, 'results_failed': 0, 'summary': {'passed_count': 1, 'watchlist_count': 0}},
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service.run_backfill(
                start_date='20260403',
                end_date='20260403',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        rebuild_action_summaries.assert_called_once()
        self.assertEqual(
            rebuild_action_summaries.call_args.kwargs['symbols'],
            [],
        )
        self.assertEqual(
            rebuild_action_summaries.call_args.kwargs[
                'exact_scope_symbols'
            ],
            ['005930'],
        )
        complete_run.assert_called_once_with(
            12,
            status='completed_with_errors',
            symbols_succeeded=1,
            symbols_failed=0,
            error_message=(
                'Technical snapshot persistence incomplete for symbols: '
                '005930 | Action summary persistence incomplete for '
                'symbols: 005930'
            ),
            run_metadata=ANY,
        )

    def test_run_market_data_pipeline_unions_disjoint_technical_debt(
        self,
    ) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(
                symbol='000001',
                market='KOSPI',
                name_ko='Context failure',
            ),
            PykrxSymbol(
                symbol='000002',
                market='KOSPI',
                name_ko='Snapshot failure',
            ),
        ]

        with (
            patch.object(service, '_persist_universe', return_value=2),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[
                    {
                        'trade_date': '2026-04-03',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }
                ],
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=12,
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch(
                'app.eod.pykrx_backfill.persist_daily_price_bars',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'status': 'partial',
                    'contexts_persisted': 1,
                    'contexts_failed': 1,
                    'failed_symbols': ['000001'],
                    'scope_finalization': {'ready': True},
                    '_retry_metadata': {
                        'failed_symbols': ['000001'],
                    },
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['000001', '000002'],
                        successful_symbols=['000002'],
                        retry_debt_symbols=['000001'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'status': 'partial',
                    'snapshots_persisted': 0,
                    'snapshots_failed': 2,
                    'failed_symbols': ['000001', '000002'],
                    'scope_finalization': {'ready': True},
                    '_retry_metadata': {
                        'failed_symbols': ['000001', '000002'],
                    },
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['000001', '000002'],
                        successful_symbols=[],
                        retry_debt_symbols=['000001', '000002'],
                    ),
                },
            ) as rebuild_technical_snapshots,
            patch(
                'app.eod.pykrx_backfill.'
                'daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'status': 'partial',
                    'action_summaries_persisted': 0,
                    'action_summaries_failed': 2,
                    'failed_symbols': ['000001', '000002'],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=['000001', '000002'],
                        successful_symbols=[],
                        retry_debt_symbols=['000001', '000002'],
                    ),
                },
            ) as rebuild_action_summaries,
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ),
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='test',
                refresh_universe=True,
                mode='daily_auto',
                selected_stages=(
                    'price_collection',
                    'technical_snapshots',
                    'action_summaries',
                ),
                chain_fundamentals_followup=False,
            )

        technical = result['technical_snapshots']
        self.assertEqual(
            technical['scope_outcome_manifest_version'],
            'technical_snapshots_scope_outcomes_v1',
        )
        self.assertEqual(
            technical['selected_stock_codes'],
            ['000001', '000002'],
        )
        self.assertEqual(technical['successful_stock_codes'], [])
        self.assertEqual(
            technical['retry_debt_stock_codes'],
            ['000001', '000002'],
        )
        self.assertEqual(
            technical['scope_outcome_manifest'],
            [
                {'stock_code': '000001', 'outcome': 'retry_debt'},
                {'stock_code': '000002', 'outcome': 'retry_debt'},
            ],
        )
        self.assertEqual(
            result['downstream_retry_symbols'],
            ['000001', '000002'],
        )
        rebuild_technical_snapshots.assert_called_once()
        self.assertEqual(
            rebuild_technical_snapshots.call_args.kwargs['symbols'],
            ['000002'],
        )
        self.assertEqual(
            rebuild_technical_snapshots.call_args.kwargs[
                'exact_scope_symbols'
            ],
            ['000001', '000002'],
        )
        rebuild_action_summaries.assert_called_once()
        self.assertEqual(
            rebuild_action_summaries.call_args.kwargs['symbols'],
            [],
        )
        self.assertEqual(
            rebuild_action_summaries.call_args.kwargs[
                'exact_scope_symbols'
            ],
            ['000001', '000002'],
        )
        self.assertFalse(
            any(
                call.kwargs.get('stage_id')
                in {
                    'price_contexts',
                    'technical_snapshots',
                    'action_summaries',
                }
                for call in self.scope_finalizer.call_args_list
            )
        )

    def test_trending_ranks_preserve_prior_rows_when_technical_scope_has_debt(
        self,
    ) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(
                symbol='000001',
                market='KOSPI',
                name_ko='Technical success',
            ),
            PykrxSymbol(
                symbol='000002',
                market='KOSDAQ',
                name_ko='Technical failure',
            ),
        ]
        symbol_codes = [item.symbol for item in symbols]

        with (
            patch.object(service, '_persist_universe', return_value=2),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[{
                    'trade_date': '2026-04-03',
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 2,
                    'volume': 3,
                    'turnover': 4,
                }],
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=12,
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch(
                'app.eod.pykrx_backfill.persist_daily_price_bars',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'update_eod_collection_run_progress'
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 2,
                    'contexts_failed': 0,
                    'failed_symbols': [],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                        successful_symbols=symbol_codes,
                        retry_debt_symbols=[],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'status': 'partial',
                    'snapshots_persisted': 1,
                    'snapshots_failed': 1,
                    'failed_symbols': ['000002'],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                        successful_symbols=['000001'],
                        retry_debt_symbols=['000002'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'status': 'partial',
                    'action_summaries_persisted': 1,
                    'action_summaries_failed': 1,
                    'failed_symbols': ['000002'],
                    'scope_finalization': {'ready': True},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                        successful_symbols=['000001'],
                        retry_debt_symbols=['000002'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'compute_and_persist_daily_trending_ranks'
            ) as compute_trending_ranks,
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ),
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI', 'KOSDAQ'),
                trigger='test',
                refresh_universe=True,
                mode='daily_auto',
                selected_stages=(
                    'price_collection',
                    'technical_snapshots',
                    'action_summaries',
                    'trending_ranks',
                ),
                chain_fundamentals_followup=False,
            )

        compute_trending_ranks.assert_not_called()
        self.assertEqual(
            result['technical_snapshots']['retry_debt_stock_codes'],
            ['000002'],
        )
        self.assertEqual(
            result['trending_ranks']['trending_ranks_persisted'],
            0,
        )
        self.assertIn(
            'technical retry debt remains: 000002',
            result['trending_ranks']['error'],
        )

    def test_run_market_data_pipeline_marks_partial_when_market_risk_filter_has_symbol_failures(self) -> None:
        service = PykrxBackfillService()
        symbols = [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')]

        with (
            patch('app.eod.pykrx_backfill.settings.dart_api_key', 'test-dart-key'),
            patch.object(service, '_persist_universe', return_value=1),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(service, '_map_symbol_bars', return_value=[{
                'trade_date': '2026-04-03',
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 3,
                'turnover': 4,
            }]),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=type('Frame', (), {'empty': False})()),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    'failed_symbols': [],
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 1,
                    'snapshots_failed': 0,
                    'failed_symbols': [],
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'action_summaries_persisted': 1,
                    'action_summaries_failed': 0,
                    'failed_symbols': [],
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 0, 'features_failed': 0, 'failed_symbols': []},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={
                    'results_persisted': 0,
                    'results_failed': 1,
                    'failed_symbols': ['005930'],
                    'summary': {'passed_count': 0, 'watchlist_count': 0},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'market_risk_filter_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=[],
                        retry_debt_symbols=['005930'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.dart_fundamentals_capture_service.'
                'run_daily_incremental_refresh'
            ) as fundamentals_followup,
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='test',
                refresh_universe=True,
                mode='daily_auto',
                selected_stages=(
                    'price_collection',
                    'technical_snapshots',
                    'action_summaries',
                    'market_risk_filter',
                ),
                chain_fundamentals_followup=True,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['market_risk_filter']['results_failed'], 1)
        complete_run.assert_called_once_with(
            12,
            status='completed_with_errors',
            symbols_succeeded=1,
            symbols_failed=0,
            error_message='Market risk filter persistence incomplete for symbols: 005930',
            run_metadata=ANY,
        )
        fundamentals_followup.assert_not_called()
        for result_key in (
            'fundamentals_followup',
            'factor_scoring_followup',
            'signal_feature_followup',
            'external_news_followup',
            'external_news_enrichment_followup',
            'macro_context_followup',
            'feedback_followup',
            'signal_decider_followup',
            'next_open_execution_plan_followup',
            'signal_decider_evaluation_followup',
        ):
            self.assertEqual(result[result_key]['status'], 'blocked')
            self.assertEqual(
                result[result_key]['reason'],
                'downstream_recovery_pending',
            )

    def test_run_market_data_pipeline_retries_exact_minor_market_risk_gap(self) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(symbol=f'{index:06d}', market='KOSPI', name_ko=f'Symbol {index}')
            for index in range(1, 21)
        ]
        symbol_codes = [item.symbol for item in symbols]

        with (
            patch.object(service, '_persist_universe', return_value=len(symbols)),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(service, '_map_symbol_bars', return_value=[{
                'trade_date': '2026-04-03',
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 3,
                'turnover': 4,
            }]),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False)),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 20,
                    'contexts_failed': 0,
                    'failed_symbols': [],
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 20,
                    'snapshots_failed': 0,
                    'failed_symbols': [],
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'action_summaries_persisted': 20,
                    'action_summaries_failed': 0,
                    'failed_symbols': [],
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 19, 'features_failed': 1, 'failed_symbols': ['000019']},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={
                    'matched_symbols': symbol_codes,
                    'results_persisted': 19,
                    'results_failed': 1,
                    'failed_symbols': ['000020'],
                    'summary': {'passed_count': 19, 'watchlist_count': 0},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'market_risk_filter_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                        successful_symbols=symbol_codes[:-1],
                        retry_debt_symbols=symbol_codes[-1:],
                    ),
                },
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='test',
                refresh_universe=True,
                mode='daily_auto',
                selected_stages=(
                    'price_collection',
                    'technical_snapshots',
                    'action_summaries',
                    'signal_features',
                    'market_risk_filter',
                ),
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['signal_features']['features_failed'], 1)
        self.assertEqual(result['market_risk_filter']['results_failed'], 1)
        self.assertEqual(result['retry_symbols'], ['000019', '000020'])
        self.assertEqual(
            result['downstream_retry_symbols'],
            ['000019', '000020'],
        )
        stage_progress = complete_run.call_args.kwargs['run_metadata']['stage_progress']
        self.assertEqual(stage_progress['signal_features']['status'], 'partial')
        self.assertFalse(
            stage_progress['signal_features'][
                'minor_missing_gap_treated_as_success'
            ]
        )
        self.assertEqual(stage_progress['market_risk_filter']['status'], 'partial')
        self.assertFalse(
            stage_progress['market_risk_filter'][
                'minor_missing_gap_treated_as_success'
            ]
        )
        complete_run.assert_called_once_with(
            12,
            status='completed_with_errors',
            symbols_succeeded=20,
            symbols_failed=0,
            error_message=(
                'Daily signal feature persistence incomplete for symbols: 000019 | '
                'Market risk filter persistence incomplete for symbols: 000020'
            ),
            run_metadata=ANY,
        )

    def test_run_market_data_pipeline_retries_full_failure_scope_beyond_public_preview(self) -> None:
        service = PykrxBackfillService()
        service.symbol_pause_seconds = 0
        symbols = [
            PykrxSymbol(symbol=f'{index:06d}', market='KOSPI', name_ko=f'Symbol {index}')
            for index in range(1, 61)
        ]
        symbol_codes = [item.symbol for item in symbols]

        with (
            patch.object(service, 'load_active_universe', return_value=symbols),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[{
                    'trade_date': '2026-04-03',
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 2,
                    'volume': 3,
                    'turnover': 4,
                }],
            ),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': len(symbols),
                    'contexts_failed': 0,
                    'failed_symbols': [],
                    '_retry_metadata': {'failed_symbols': []},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 0,
                    'snapshots_failed': len(symbols),
                    'failed_symbols': symbol_codes[:50],
                    '_retry_metadata': {'failed_symbols': symbol_codes},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=symbol_codes,
                        successful_symbols=[],
                        retry_debt_symbols=symbol_codes,
                    ),
                },
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='test',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=('price_collection', 'technical_snapshots'),
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['retry_symbols_count'], 60)
        self.assertEqual(result['retry_symbols'], symbol_codes[:50])
        self.assertEqual(result['technical_snapshots']['failed_symbols'], symbol_codes[:50])
        self.assertNotIn('_retry_metadata', result['technical_snapshots'])
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(completed_metadata['failed_symbols'], [])
        self.assertEqual(completed_metadata['failed_symbols_count'], 0)
        self.assertEqual(completed_metadata['downstream_retry_symbols'], symbol_codes)
        self.assertEqual(completed_metadata['downstream_retry_symbols_count'], 60)
        self.assertEqual(result['price_retry_symbols_count'], 0)
        self.assertEqual(result['downstream_retry_symbols_count'], 60)

    def test_run_market_data_pipeline_treats_unhealthy_signal_source_as_retry_debt(self) -> None:
        service = PykrxBackfillService()
        symbol = PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')
        enrichment_result = _ready_pykrx_enrichment_result()
        enrichment_result['status'] = 'recovered'
        enrichment_result['categories']['market_breadth']['errors'] = [
            {'market': 'KOSPI', 'error': 'provider timeout'}
        ]

        with (
            patch.object(service, 'load_active_universe', return_value=[symbol]),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[{
                    'trade_date': '2026-04-03',
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 2,
                    'volume': 3,
                    'turnover': 4,
                }],
            ),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ) as update_progress,
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    'failed_symbols': [],
                    '_retry_metadata': {'failed_symbols': []},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                return_value=enrichment_result,
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'periods_completed': 3, 'periods_failed': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={
                    'status': 'partial',
                    'features_persisted': 0,
                    'features_failed': 0,
                    'failed_symbols': [],
                    'source_errors': {},
                    'incomplete_sources': ['volume-power'],
                    'source_health': {
                        'volume-power': {
                            'status': 'unavailable',
                            'reason': 'all_rows_invalid',
                        },
                    },
                    '_retry_metadata': {'failed_symbols': ['005930']},
                    **build_exact_symbol_scope_outcome(
                        manifest_version=(
                            'signal_features_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=[],
                        retry_debt_symbols=['005930'],
                    ),
                },
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI', 'KOSDAQ'),
                trigger='test',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=(
                    'price_collection',
                    'pykrx_enrichment',
                    'signal_features',
                ),
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['retry_symbols'], ['005930'])
        self.assertEqual(result['pykrx_enrichment']['recovered_categories'], ['market_breadth'])
        self.assertEqual(result['pykrx_enrichment']['unresolved_categories'], [])
        self.assertNotIn('_retry_metadata', result['pykrx_enrichment'])
        self.assertNotIn('_retry_metadata', result['signal_features'])
        self.assertEqual(
            complete_run.call_args.kwargs['error_message'],
            'Daily signal feature sources incomplete: volume-power',
        )
        self.assertEqual(
            complete_run.call_args.kwargs['run_metadata']['downstream_retry_symbols'],
            ['005930'],
        )
        self.assertEqual(
            complete_run.call_args.kwargs['run_metadata']['failed_symbols'],
            [],
        )
        signal_progress = next(
            call.kwargs['run_metadata']['stage_progress'][
                'signal_features'
            ]
            for call in update_progress.call_args_list
            if call.kwargs['run_metadata']['stage_progress'].get(
                'signal_features', {}
            ).get('current_step')
            == 'signal_features_completed'
        )
        self.assertEqual(signal_progress['processed'], 1)
        self.assertEqual(signal_progress['total'], 1)
        self.assertEqual(signal_progress['succeeded'], 0)
        self.assertEqual(signal_progress['failed'], 1)

    def test_enrichment_partial_persists_earliest_downstream_retry_stage(
        self,
    ) -> None:
        service = PykrxBackfillService()
        service.symbol_pause_seconds = 0
        symbol = PykrxSymbol(
            symbol='005930',
            market='KOSPI',
            name_ko='Samsung Electronics',
        )
        enrichment_result = _ready_pykrx_enrichment_result()
        enrichment_result['categories']['short_selling']['rows_persisted'] = 0
        enrichment_result['coverage']['categories']['short_selling'].update(
            {
                'present_rows': 0,
                'coverage_ratio': 0.0,
                'coverage_status': 'missing',
                'decision_usable_scope_keys': [],
                'complete_scope_keys': [],
            }
        )

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[{
                    'trade_date': '2026-04-03',
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 2,
                    'volume': 3,
                    'turnover': 4,
                }],
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=12,
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch(
                'app.eod.pykrx_pipeline_runner._enrich_bars_with_market_turnover',
                side_effect=lambda **kwargs: kwargs['bars'],
            ),
            patch(
                'app.eod.pykrx_backfill.persist_daily_price_bars',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'requested_symbols': 2,
                    'contexts_persisted': 1,
                    'contexts_failed': 1,
                    '_retry_metadata': {'failed_symbols': ['472220']},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930', '472220'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=['472220'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.pykrx_enrichment_service.'
                'collect_for_market_date',
                return_value=enrichment_result,
            ),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                symbols_override=[symbol],
                selected_stages=('price_collection', 'pykrx_enrichment'),
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(
            completed_metadata['downstream_retry_symbols'],
            ['005930'],
        )
        self.assertEqual(
            completed_metadata['downstream_retry_resume_from_stage'],
            'pykrx_enrichment',
        )

    def test_run_market_data_pipeline_preserves_price_collection_counts_in_result(self) -> None:
        service = PykrxBackfillService()
        symbols = [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')]

        with (
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(service, '_load_symbol_fundamentals', return_value=None),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[
                    {
                        'trade_date': '2026-04-02',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    },
                    {
                        'trade_date': '2026-04-03',
                        'open_price': 2,
                        'high_price': 3,
                        'low_price': 2,
                        'close_price': 3,
                        'volume': 5,
                        'turnover': 6,
                    },
                ],
            ),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False)),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_ticker',
                return_value=pd.DataFrame({'거래대금': [6]}, index=['005930']),
            ),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=2),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 1,
                    'snapshots_failed': 0,
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'action_summaries_persisted': 1,
                    'action_summaries_failed': 0,
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                    ),
                },
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 0, 'features_failed': 0, 'sources': [], 'source_counts': {}},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={'results_persisted': 1, 'results_failed': 0, 'summary': {'passed_count': 1, 'watchlist_count': 0}},
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service.run_backfill(
                start_date='20260403',
                end_date='20260403',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['symbols_total'], 1)
        self.assertEqual(result['symbols_succeeded'], 1)
        self.assertEqual(result['symbols_failed'], 0)
        self.assertEqual(result['bars_upserted'], 2)
        self.assertEqual(
            result['turnover_coverage'],
            {
                'status': 'complete',
                'total_bars': 2,
                'eligible_bars': 2,
                'provider_turnover_bars': 2,
                'zero_volume_bars': 0,
                'invalid_turnover_bars': 0,
                'market_fetch_attempts': 2,
                'market_fetch_failures': 0,
                'coverage_ratio': 1.0,
            },
        )
        complete_run.assert_called_once_with(
            12,
            status='completed',
            symbols_succeeded=1,
            symbols_failed=0,
            error_message=None,
            run_metadata=ANY,
        )

    def test_run_market_data_pipeline_marks_and_exposes_invalid_turnover(self) -> None:
        service = PykrxBackfillService()
        service.symbol_pause_seconds = 0
        symbol = PykrxSymbol(
            symbol='005930',
            market='KOSPI',
            name_ko='Samsung Electronics',
        )

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[{
                    'trade_date': '2026-04-03',
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 2,
                    'volume': 3,
                    'turnover': 6,
                }],
            ),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_ticker',
                return_value=pd.DataFrame({'거래대금': [0]}, index=['005930']),
            ),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    '_retry_metadata': {'failed_symbols': []},
                },
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='manual_api_stage',
                refresh_universe=False,
                mode='daily_stage_run',
                symbols_override=[symbol],
                selected_stages=('price_collection',),
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['turnover_coverage']['status'], 'partial')
        self.assertEqual(result['turnover_coverage']['invalid_turnover_bars'], 1)
        self.assertEqual(result['turnover_invalid_symbols'], ['005930'])
        self.assertEqual(result['price_retry_symbols'], [])
        self.assertIn(
            'Turnover coverage incomplete',
            complete_run.call_args.kwargs['error_message'],
        )

    def test_recovery_target_reloads_exact_automatic_lineage_inside_lock(
        self,
    ) -> None:
        service = PykrxBackfillService()
        lock_active = False
        target_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_failed': 1,
            'run_metadata': {'failed_symbols': ['000660']},
        }

        @contextmanager
        def tracked_lock():
            nonlocal lock_active
            lock_active = True
            try:
                yield True
            finally:
                lock_active = False

        def load_exact_target(**_kwargs):
            self.assertTrue(lock_active)
            return target_run

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=tracked_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_exact_automatic_daily_eod_collection_run',
                side_effect=load_exact_target,
                create=True,
            ) as load_exact,
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 8,
                    'trade_date': '2026-04-03',
                    'status': 'completed',
                    'symbols_failed': 0,
                    'run_metadata': {'mode': 'manual'},
                },
            ) as load_latest,
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=1,
            ),
            patch.object(
                service,
                '_failed_symbol_codes_for_run',
                return_value=['000660'],
            ),
            patch.object(
                service,
                '_load_retry_symbols_for_run',
                return_value=[
                    PykrxSymbol(
                        symbol='000660',
                        market='KOSPI',
                        name_ko='SK hynix',
                    )
                ],
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 9,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
                recovery_eod_run_id=7,
            )

        self.assertTrue(result['triggered'])
        load_exact.assert_called_once_with(
            eod_run_id=7,
            trade_date='2026-04-03',
        )
        load_latest.assert_not_called()
        self.assertEqual(
            run_collection.call_args.kwargs['retry_context']['retry_of_run_id'],
            7,
        )

    def test_recovery_target_fails_closed_when_exact_lineage_is_invalid(
        self,
    ) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_exact_automatic_daily_eod_collection_run',
                return_value=None,
            ) as load_exact,
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run'
            ) as load_latest,
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 9,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
                recovery_eod_run_id=7,
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(
            result['reason'],
            'recovery_target_invalid_or_superseded',
        )
        self.assertEqual(
            result['recovery_target'],
            {'trade_date': '2026-04-03', 'eod_run_id': 7},
        )
        load_exact.assert_called_once_with(
            eod_run_id=7,
            trade_date='2026-04-03',
        )
        load_latest.assert_not_called()
        run_collection.assert_not_called()

    def test_fresh_running_recovery_target_does_not_start_duplicate_collection(
        self,
    ) -> None:
        service = PykrxBackfillService()
        fresh_activity_at = datetime.now(timezone.utc).isoformat()
        running_target = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'running',
            'symbols_total': 2,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'started_at': fresh_activity_at,
            'run_metadata': {'last_progress_at': fresh_activity_at},
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_exact_automatic_daily_eod_collection_run',
                return_value=running_target,
            ) as load_exact,
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run'
            ) as load_latest,
            patch.object(
                service,
                '_run_market_data_pipeline',
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
                recovery_eod_run_id=7,
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'already_running')
        self.assertIs(result['existing_run'], running_target)
        load_exact.assert_called_once_with(
            eod_run_id=7,
            trade_date='2026-04-03',
        )
        load_latest.assert_not_called()
        run_collection.assert_not_called()

    def test_stale_recovery_target_reloads_same_exact_lineage_after_reconcile(
        self,
    ) -> None:
        service = PykrxBackfillService()
        stale_running = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'running',
            'symbols_failed': 1,
            'run_metadata': {'failed_symbols': ['000660']},
        }
        reconciled_target = {
            **stale_running,
            'status': 'failed',
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.'
                'load_exact_automatic_daily_eod_collection_run',
                side_effect=[stale_running, reconciled_target],
            ) as load_exact,
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 8,
                    'trade_date': '2026-04-03',
                    'status': 'completed',
                    'symbols_failed': 0,
                    'run_metadata': {'mode': 'manual'},
                },
            ) as load_latest,
            patch.object(
                service,
                '_is_stale_running_run',
                return_value=True,
            ),
            patch.object(service, '_fail_stale_running_run') as fail_stale,
            patch.object(
                service,
                '_resume_auto_daily_followups_if_due',
                return_value=None,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=1,
            ),
            patch.object(
                service,
                '_failed_symbol_codes_for_run',
                return_value=['000660'],
            ),
            patch.object(
                service,
                '_load_retry_symbols_for_run',
                return_value=[
                    PykrxSymbol(
                        symbol='000660',
                        market='KOSPI',
                        name_ko='SK hynix',
                    )
                ],
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 9,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
                recovery_eod_run_id=7,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(
            load_exact.call_args_list,
            [
                call(eod_run_id=7, trade_date='2026-04-03'),
                call(eod_run_id=7, trade_date='2026-04-03'),
            ],
        )
        load_latest.assert_not_called()
        fail_stale.assert_called_once_with(stale_running)
        self.assertEqual(
            run_collection.call_args.kwargs['retry_context']['retry_of_run_id'],
            7,
        )

    def test_reconcile_without_prefetched_run_uses_exact_recovery_target(
        self,
    ) -> None:
        service = PykrxBackfillService()
        exact_target = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_failed': 1,
            'run_metadata': {'failed_symbols': ['000660']},
        }

        with (
            patch(
                'app.eod.pykrx_backfill.'
                'load_exact_automatic_daily_eod_collection_run',
                return_value=exact_target,
            ) as load_exact,
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 8,
                    'trade_date': '2026-04-03',
                    'status': 'completed',
                    'run_metadata': {'mode': 'manual'},
                },
            ) as load_latest,
        ):
            result = service.reconcile_latest_daily_run_status(
                trade_date='2026-04-03',
                recovery_eod_run_id=7,
            )

        self.assertIs(result, exact_target)
        load_exact.assert_called_once_with(
            eod_run_id=7,
            trade_date='2026-04-03',
        )
        load_latest.assert_not_called()

    def test_run_daily_collection_skips_completed_with_errors_without_failed_symbols(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_failed': 0,
                },
            ),
            patch.object(service, '_resume_auto_daily_followups_if_due', return_value=None),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 8,
                    'source': 'pykrx',
                    'status': 'completed',
                    'technical_snapshots': {'snapshots_failed': 0},
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'already_completed_with_errors')
        run_collection.assert_not_called()

    def test_auto_daily_retry_uses_previous_failed_symbols_only(self) -> None:
        service = PykrxBackfillService()
        retry_symbols = [
            PykrxSymbol(symbol='000660', market='KOSPI', name_ko='SK hynix'),
            PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics'),
        ]

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_failed': 2,
                    'run_metadata': {'failed_symbols': ['000660', '005930']},
                },
            ),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=2),
            patch(
                'app.eod.pykrx_backfill.load_symbols',
                return_value=[
                    {
                        'symbol': '000660',
                        'market': 'KOSPI',
                        'name_ko': 'SK hynix',
                        'standard_code': None,
                    },
                    {
                        'symbol': '005930',
                        'market': 'KOSPI',
                        'name_ko': 'Samsung Electronics',
                        'standard_code': None,
                    },
                ],
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 8,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        run_collection.assert_called_once()
        kwargs = run_collection.call_args.kwargs
        self.assertEqual(kwargs['symbols_override'], retry_symbols)
        self.assertFalse(kwargs['refresh_universe'])
        self.assertEqual(
            kwargs['retry_context'],
            {
                'retry_of_run_id': 7,
                'previous_status': 'completed_with_errors',
                'previous_symbols_failed': 2,
                'attempt': 3,
                'max_attempts': 5,
                'symbol_count': 2,
            },
        )

    def test_auto_daily_retry_fails_closed_when_symbol_resolution_is_partial(
        self,
    ) -> None:
        service = PykrxBackfillService()
        resolved_symbol = PykrxSymbol(
            symbol='000660',
            market='KOSPI',
            name_ko='SK hynix',
        )
        latest_run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_failed': 2,
            'run_metadata': {
                'requested_symbols': ['000660', '005930'],
                'failed_symbols': ['000660', '005930'],
            },
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=latest_run,
            ),
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=1,
            ),
            patch.object(
                service,
                '_failed_symbol_codes_for_run',
                return_value=['000660', '005930'],
            ),
            patch.object(
                service,
                '_load_retry_symbols_for_run',
                return_value=[resolved_symbol],
            ) as load_retry_symbols,
            patch.object(
                service,
                '_run_market_data_pipeline',
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(
            result['reason'],
            'retry_symbol_resolution_incomplete',
        )
        self.assertEqual(
            result['auto_retry']['unresolved_symbols'],
            ['005930'],
        )
        self.assertEqual(result['auto_retry']['unresolved_symbol_count'], 1)
        load_retry_symbols.assert_called_once_with(
            latest_run,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            symbol_codes=['000660', '005930'],
        )
        run_collection.assert_not_called()

    def test_retry_scope_unions_attempted_failures_and_unattempted_outage_tail(self) -> None:
        service = PykrxBackfillService()
        run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'failed',
            'run_metadata': {
                'requested_symbols': ['000001', '000002', '000003', '000004'],
                'failed_symbols': ['000001', '000002'],
                'unattempted_symbols': ['000003', '000004'],
            },
        }

        with patch(
            'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
            return_value=[],
        ) as persisted:
            retry_codes = service._failed_symbol_codes_for_run(run, markets=('KOSPI',))

        self.assertEqual(retry_codes, ['000001', '000002', '000003', '000004'])
        persisted.assert_called_once_with(7, trade_date='2026-04-03')

    def test_retry_scope_keeps_invalid_turnover_symbols_even_when_price_bar_persisted(self) -> None:
        service = PykrxBackfillService()
        run = {
            'id': 7,
            'trade_date': '2026-04-03',
            'status': 'completed_with_errors',
            'symbols_failed': 0,
            'run_metadata': {
                'requested_symbols': ['005930'],
                'turnover_invalid_symbols': ['005930'],
            },
        }

        with patch(
            'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
            return_value=['005930'],
        ):
            retry_codes = service._failed_symbol_codes_for_run(
                run,
                markets=('KOSPI',),
            )

        self.assertEqual(retry_codes, ['005930'])

    def test_auto_daily_retries_only_minor_symbol_gap(self) -> None:
        service = PykrxBackfillService()
        retry_symbols = [
            PykrxSymbol(symbol=symbol, market='KOSPI', name_ko=symbol)
            for symbol in ('082640', '489010', '096610', '471050')
        ]

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_total': 3552,
                    'symbols_failed': 4,
                    'run_metadata': {'failed_symbols': ['082640', '489010', '096610', '471050']},
                },
            ),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=1) as count_runs,
            patch.object(service, '_load_retry_symbols_for_run', return_value=retry_symbols),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={'run_id': 8, 'source': 'pykrx', 'status': 'completed'},
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        count_runs.assert_called_once()
        kwargs = run_collection.call_args.kwargs
        self.assertEqual(kwargs['symbols_override'], retry_symbols)
        self.assertFalse(kwargs['refresh_universe'])
        self.assertEqual(kwargs['retry_context']['symbol_count'], 4)

    def test_auto_daily_retry_stops_after_five_auto_runs(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'failed',
                    'symbols_failed': 4,
                    'run_metadata': {'failed_symbols': ['000660', '005930', '035420', '051910']},
                },
            ),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=5),
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'auto_retry_limit_reached')
        self.assertEqual(result['auto_retry']['attempts'], 5)
        self.assertEqual(result['auto_retry']['max_attempts'], 5)
        run_collection.assert_not_called()

    def test_manual_daily_triggers_retry_minor_symbol_gaps_with_full_collection(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                side_effect=[_lock(), _lock()],
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_total': 3552,
                    'symbols_failed': 4,
                    'run_metadata': {'failed_symbols': ['082640', '489010', '096610', '471050']},
                },
            ),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs') as count_runs,
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 8,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            results = [
                service.run_daily_pipeline_if_due(market_date='2026-04-03', trigger=trigger)
                for trigger in ('manual_daily_api', 'internal_ops_cli')
            ]

        self.assertTrue(all(result['triggered'] for result in results))
        count_runs.assert_not_called()
        self.assertEqual(run_collection.call_count, 2)
        for call in run_collection.call_args_list:
            self.assertIsNone(call.kwargs['symbols_override'])
            self.assertIsNone(call.kwargs['retry_context'])
            self.assertTrue(call.kwargs['refresh_universe'])

    def test_run_market_data_pipeline_limits_symbol_stages_to_fresh_price_scope(self) -> None:
        service = PykrxBackfillService()
        service.symbol_pause_seconds = 0
        symbols = [
            PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics'),
            PykrxSymbol(symbol='472220', market='KOSPI', name_ko='Missing Price'),
        ]

        def price_frame(_start: str, _end: str, symbol: str) -> SimpleNamespace:
            return SimpleNamespace(empty=symbol == '472220')

        with (
            patch.object(service, 'load_active_universe', return_value=symbols),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[{
                    'trade_date': '2026-04-03',
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 2,
                    'volume': 3,
                    'turnover': 4,
                }],
            ),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', side_effect=price_frame),
            patch(
                'app.eod.pykrx_pipeline_runner._enrich_bars_with_market_turnover',
                side_effect=lambda **kwargs: kwargs['bars'],
            ),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 2,
                    'contexts_persisted': 1,
                    'contexts_failed': 1,
                    '_retry_metadata': {'failed_symbols': ['472220']},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930', '472220'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=['472220'],
                    ),
                },
            ) as price_contexts,
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 2,
                    'snapshots_persisted': 1,
                    'snapshots_failed': 1,
                    '_retry_metadata': {'failed_symbols': ['472220']},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930', '472220'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=['472220'],
                    ),
                },
            ) as technical_snapshots,
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 2,
                    'action_summaries_persisted': 1,
                    'action_summaries_failed': 1,
                    '_retry_metadata': {'failed_symbols': ['472220']},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930', '472220'],
                        successful_symbols=['005930'],
                        retry_debt_symbols=['472220'],
                    ),
                },
            ) as action_summaries,
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='test',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=('price_collection', 'technical_snapshots', 'action_summaries'),
                chain_fundamentals_followup=False,
            )

        for stage in (price_contexts, technical_snapshots, action_summaries):
            self.assertEqual(stage.call_args.kwargs['symbols'], ['005930'])
            self.assertEqual(
                stage.call_args.kwargs['exact_scope_symbols'],
                ['005930', '472220'],
            )
        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['price_retry_symbols'], ['472220'])
        self.assertEqual(result['downstream_retry_symbols'], [])
        self.assertEqual(result['technical_snapshots']['causally_skipped_symbols'], ['472220'])
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(completed_metadata['failed_symbols'], ['472220'])
        self.assertEqual(completed_metadata['downstream_retry_symbols'], [])

    def test_run_market_data_pipeline_finalizes_empty_symbol_stages_when_no_price_was_collected(self) -> None:
        service = PykrxBackfillService()
        symbols = [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')]

        with (
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch('app.eod.pykrx_backfill.log_fetch_event') as log_fetch_event,
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                side_effect=RuntimeError('pykrx timeout'),
            ),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 0,
                    'contexts_failed': 1,
                    'failed_symbols': ['005930'],
                    '_retry_metadata': {'failed_symbols': ['005930']},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'price_contexts_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=[],
                        retry_debt_symbols=['005930'],
                    ),
                },
            ) as price_contexts,
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 0,
                    'snapshots_failed': 1,
                    'failed_symbols': ['005930'],
                    '_retry_metadata': {'failed_symbols': ['005930']},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'technical_snapshots_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=[],
                        retry_debt_symbols=['005930'],
                    ),
                },
            ) as technical_snapshots,
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'action_summaries_persisted': 0,
                    'action_summaries_failed': 1,
                    'failed_symbols': ['005930'],
                    '_retry_metadata': {'failed_symbols': ['005930']},
                    **_atomic_exact_scope_proof(
                        manifest_version=(
                            'action_summaries_scope_outcomes_v1'
                        ),
                        selected_symbols=['005930'],
                        successful_symbols=[],
                        retry_debt_symbols=['005930'],
                    ),
                },
            ) as action_summaries,
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 0},
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run'),
        ):
            result = service.run_backfill(
                start_date='20260403',
                end_date='20260403',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['failed_symbols'], ['005930'])
        for stage in (price_contexts, technical_snapshots, action_summaries):
            stage.assert_called_once()
            self.assertEqual(stage.call_args.kwargs['symbols'], [])
            self.assertEqual(
                stage.call_args.kwargs['exact_scope_symbols'],
                ['005930'],
            )
        self.assertEqual(
            result['price_contexts']['retry_debt_stock_codes'],
            ['005930'],
        )
        self.assertEqual(
            result['technical_snapshots']['retry_debt_stock_codes'],
            ['005930'],
        )
        self.assertEqual(
            result['action_summaries']['retry_debt_stock_codes'],
            ['005930'],
        )
        self.assertEqual(result['technical_snapshots']['causally_skipped_symbols'], ['005930'])
        self.assertTrue(
            any(
                call.kwargs.get('operation') == 'fetch_symbol_ohlcv'
                and call.kwargs.get('status') == 'failed'
                and call.kwargs.get('symbol') == '005930'
                for call in log_fetch_event.call_args_list
            )
        )

    def test_price_retry_outage_keeps_inherited_downstream_scope(self) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(
                symbol='472220',
                market='KOSDAQ',
                name_ko='retry symbol',
            )
        ]
        universe_snapshot = _stored_daily_universe_snapshot(
            [
                PykrxSymbol('005930', 'KOSPI', 'inherited symbol'),
                symbols[0],
            ]
        )
        retry_context = build_daily_retry_context(
            {
                'id': 7,
                'status': 'completed_with_errors',
                'symbols_total': 2681,
                'symbols_succeeded': 2680,
                'symbols_failed': 1,
                'run_metadata': {
                    'failed_symbols': ['472220'],
                    'downstream_retry_symbols': ['005930'],
                    'universe_snapshot': universe_snapshot,
                },
            },
            attempt=1,
            max_attempts=5,
            symbol_count=1,
        )

        with (
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=12,
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                side_effect=RuntimeError('pykrx timeout'),
            ),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI', 'KOSDAQ'),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                symbols_override=symbols,
                retry_context=retry_context,
                selected_stages=PYKRX_STAGE_SEQUENCE,
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['error_type'], 'PykrxRetryScopeUnresolved')
        self.assertEqual(result['unresolved_symbols'], ['472220'])
        self.assertFalse(result['provider_outage_suspected'])
        self.assertEqual(result['retry_scope_status'], 'partial')
        self.assertEqual(result['retry']['baseline_run_id'], 7)
        self.assertEqual(result['retry']['baseline_symbols_total'], 2681)
        self.assertEqual(result['retry']['baseline_symbols_succeeded'], 2680)
        self.assertEqual(result['retry']['max_attempts'], 5)
        self.assertEqual(
            complete_run.call_args.kwargs['status'],
            'completed_with_errors',
        )
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(
            completed_metadata['stage_progress']['price_collection']['status'],
            'partial',
        )
        self.assertEqual(completed_metadata['unresolved_symbols'], ['472220'])
        self.assertEqual(
            completed_metadata['retry']['baseline_symbols_succeeded'],
            2680,
        )
        self.assertEqual(
            completed_metadata['downstream_retry_symbols'],
            ['005930'],
        )
        self.assertEqual(
            completed_metadata['inherited_downstream_retry_symbols'],
            ['005930'],
        )
        self.assertEqual(
            completed_metadata['universe_snapshot'],
            universe_snapshot,
        )
        self.assertEqual(
            retry_context['inherited_universe_snapshot'],
            universe_snapshot,
        )

    def test_retry_early_abort_keeps_unattempted_tail_unresolved_and_reports_provider_outage(
        self,
    ) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(
                symbol=f'{index:06d}',
                market='KOSPI',
                name_ko=f'Retry Symbol {index}',
            )
            for index in range(25)
        ]
        retry_context = build_daily_retry_context(
            {
                'id': 7,
                'status': 'completed_with_errors',
                'symbols_total': 2681,
                'symbols_succeeded': 2656,
                'symbols_failed': 25,
                'run_metadata': {
                    'failed_symbols': [item.symbol for item in symbols],
                },
            },
            attempt=2,
            max_attempts=5,
            symbol_count=len(symbols),
        )

        with (
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=12,
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                side_effect=TimeoutError('pykrx provider timed out'),
            ) as price_fetch,
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                symbols_override=symbols,
                retry_context=retry_context,
                selected_stages=PYKRX_STAGE_SEQUENCE,
                chain_fundamentals_followup=False,
            )

        attempted = [item.symbol for item in symbols[:20]]
        unattempted = [item.symbol for item in symbols[20:]]
        unresolved = [item.symbol for item in symbols]
        self.assertEqual(price_fetch.call_count, 20)
        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['symbols_failed'], 20)
        self.assertEqual(result['failed_symbols'], attempted)
        self.assertEqual(result['unattempted_symbols'], unattempted)
        self.assertEqual(result['unattempted_symbols_count'], 5)
        self.assertEqual(result['unresolved_symbols'], unresolved)
        self.assertEqual(result['unresolved_symbols_count'], 25)
        self.assertTrue(result['provider_outage_suspected'])
        self.assertEqual(result['retry_scope_status'], 'provider_outage')
        self.assertEqual(
            result['provider_outage_evidence'],
            {
                'early_abort_triggered': True,
                'all_attempted_failures_provider_related': True,
                'attempted_failure_count': 20,
                'provider_failure_count': 20,
                'unattempted_count': 5,
                'failure_types': ['TimeoutError'],
            },
        )
        self.assertIn('20 attempted failures', result['error_message'])
        self.assertIn('5 unattempted', result['error_message'])
        self.assertEqual(result['retry']['baseline_run_id'], 7)
        self.assertEqual(result['retry']['attempt'], 2)

        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(completed_metadata['failed_symbols'], attempted)
        self.assertEqual(completed_metadata['unattempted_symbols'], unattempted)
        self.assertEqual(completed_metadata['unresolved_symbols'], unresolved)
        self.assertEqual(completed_metadata['unresolved_symbols_count'], 25)
        self.assertTrue(completed_metadata['provider_outage_suspected'])
        self.assertEqual(
            completed_metadata['retry_scope_status'],
            'provider_outage',
        )
        self.assertEqual(
            completed_metadata['provider_outage_evidence'],
            result['provider_outage_evidence'],
        )
        self.assertEqual(completed_metadata['retry']['baseline_run_id'], 7)

    def test_retry_exact_abort_threshold_reports_provider_outage_without_unattempted_tail(
        self,
    ) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(
                symbol=f'{index:06d}',
                market='KOSPI',
                name_ko=f'Retry Symbol {index}',
            )
            for index in range(20)
        ]
        retry_context = build_daily_retry_context(
            {
                'id': 7,
                'status': 'completed_with_errors',
                'symbols_total': 2681,
                'symbols_succeeded': 2661,
                'symbols_failed': 20,
                'run_metadata': {
                    'failed_symbols': [item.symbol for item in symbols],
                },
            },
            attempt=2,
            max_attempts=5,
            symbol_count=len(symbols),
        )

        with (
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
            ),
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=13,
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                side_effect=TimeoutError('pykrx provider timed out'),
            ) as price_fetch,
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                symbols_override=symbols,
                retry_context=retry_context,
                selected_stages=PYKRX_STAGE_SEQUENCE,
                chain_fundamentals_followup=False,
            )

        unresolved = [item.symbol for item in symbols]
        self.assertEqual(price_fetch.call_count, 20)
        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['symbols_failed'], 20)
        self.assertEqual(result['unattempted_symbols'], [])
        self.assertEqual(result['unattempted_symbols_count'], 0)
        self.assertEqual(result['unresolved_symbols'], unresolved)
        self.assertEqual(result['unresolved_symbols_count'], 20)
        self.assertTrue(result['provider_outage_suspected'])
        self.assertEqual(result['retry_scope_status'], 'provider_outage')
        self.assertEqual(
            result['provider_outage_evidence'],
            {
                'early_abort_triggered': True,
                'all_attempted_failures_provider_related': True,
                'attempted_failure_count': 20,
                'provider_failure_count': 20,
                'unattempted_count': 0,
                'failure_types': ['TimeoutError'],
            },
        )
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(completed_metadata['unresolved_symbols'], unresolved)
        self.assertTrue(completed_metadata['provider_outage_suspected'])
        self.assertEqual(
            complete_run.call_args.kwargs['status'],
            'completed_with_errors',
        )

    def test_successful_small_price_retry_preserves_inherited_enrichment_stage(
        self,
    ) -> None:
        service = PykrxBackfillService()
        service.symbol_pause_seconds = 0
        retry_symbol = PykrxSymbol(
            symbol='472220',
            market='KOSDAQ',
            name_ko='Retry Symbol',
        )

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[{
                    'trade_date': '2026-04-03',
                    'open_price': 1,
                    'high_price': 2,
                    'low_price': 1,
                    'close_price': 2,
                    'volume': 3,
                    'turnover': 4,
                }],
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                return_value=8,
            ),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch(
                'app.eod.pykrx_pipeline_runner._enrich_bars_with_market_turnover',
                side_effect=lambda **kwargs: kwargs['bars'],
            ),
            patch(
                'app.eod.pykrx_backfill.persist_daily_price_bars',
                return_value=1,
            ),
            patch(
                'app.eod.pykrx_backfill.update_eod_collection_run_progress'
            ),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    '_retry_metadata': {'failed_symbols': []},
                },
            ),
            patch(
                'app.eod.pykrx_backfill.pykrx_enrichment_service.'
                'collect_for_market_date',
                return_value=_ready_pykrx_enrichment_result(
                    symbols=('472220',),
                    markets=('KOSDAQ',),
                ),
            ),
            patch(
                'app.eod.pykrx_backfill.complete_eod_collection_run'
            ) as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSDAQ',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                symbols_override=[retry_symbol],
                retry_context={
                    'retry_of_run_id': 7,
                    'inherited_downstream_retry_symbols': ['005930'],
                    'inherited_downstream_retry_resume_from_stage': (
                        'pykrx_enrichment'
                    ),
                },
                selected_stages=('price_collection', 'pykrx_enrichment'),
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(
            completed_metadata['downstream_retry_symbols'],
            ['005930'],
        )
        self.assertEqual(
            completed_metadata['downstream_retry_resume_from_stage'],
            'pykrx_enrichment',
        )

    def test_small_price_retry_blocks_all_followups_while_inherited_scope_remains(
        self,
    ) -> None:
        service = PykrxBackfillService()
        service.symbol_pause_seconds = 0
        retry_symbol = PykrxSymbol(
            symbol='472220',
            market='KOSDAQ',
            name_ko='Retry Symbol',
        )
        fake_run_state_service = _FakeDailyPipelineRunStateService()

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_pipeline_run_state_service',
                    fake_run_state_service,
                )
            )
            stack.enter_context(
                patch('app.eod.pykrx_backfill.settings.dart_api_key', 'test-dart-key')
            )
            stack.enter_context(
                patch.object(
                    service,
                    'resolve_latest_trade_date',
                    return_value='2026-04-03',
                )
            )
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[{
                        'trade_date': '2026-04-03',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }],
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.create_eod_collection_run',
                    return_value=8,
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                    return_value=SimpleNamespace(empty=False),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._enrich_bars_with_market_turnover',
                    side_effect=lambda **kwargs: kwargs['bars'],
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.persist_daily_price_bars',
                    return_value=1,
                )
            )
            stack.enter_context(
                patch('app.eod.pykrx_backfill.update_eod_collection_run_progress')
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_price_context_service.'
                    'rebuild_for_market_date',
                    return_value={
                        'requested_symbols': 1,
                        'contexts_persisted': 1,
                        'contexts_failed': 0,
                        '_retry_metadata': {'failed_symbols': []},
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.pykrx_enrichment_service.'
                    'collect_for_market_date',
                    return_value=_ready_pykrx_enrichment_result(
                        symbols=('472220',),
                        markets=('KOSDAQ',),
                    ),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_technical_snapshot_service.'
                    'rebuild_for_market_date',
                    return_value={
                        'snapshots_persisted': 1,
                        'snapshots_failed': 0,
                        '_retry_metadata': {'failed_symbols': []},
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_action_summary_service.'
                    'rebuild_for_market_date',
                    return_value={
                        'action_summaries_persisted': 1,
                        'action_summaries_failed': 0,
                        '_retry_metadata': {'failed_symbols': []},
                    },
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                    return_value={'trending_ranks_persisted': 1},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_market_risk_filter_service.'
                    'rebuild_for_market_date',
                    return_value={
                        'matched_symbols': ['472220'],
                        'results_persisted': 1,
                        'results_failed': 0,
                        '_retry_metadata': {'failed_symbols': []},
                    },
                )
            )
            signal_features = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_signal_feature_service.'
                    'refresh_for_market_date',
                    return_value={
                        'features_persisted': 1,
                        'features_failed': 0,
                        'sources': [],
                        'source_counts': {},
                    },
                )
            )
            followup_mocks = [
                stack.enter_context(
                    patch(
                        'app.eod.pykrx_backfill.dart_fundamentals_capture_service.'
                        'run_daily_incremental_refresh',
                        return_value={'status': 'completed'},
                    )
                ),
                stack.enter_context(
                    patch(
                        'app.eod.pykrx_pipeline_runner._run_daily_factor_followup',
                        return_value=_completed_factor_followup_result(),
                    )
                ),
                stack.enter_context(
                    patch(
                        'app.eod.pykrx_pipeline_runner._run_daily_external_news_followup',
                        return_value={'status': 'completed'},
                    )
                ),
                stack.enter_context(
                    patch(
                        'app.eod.pykrx_pipeline_runner.'
                        '_run_daily_external_news_enrichment_followup',
                        return_value={'status': 'completed'},
                    )
                ),
                stack.enter_context(
                    patch(
                        'app.eod.pykrx_pipeline_runner._run_daily_macro_context_followup',
                        return_value={'status': 'completed'},
                    )
                ),
                stack.enter_context(
                    patch(
                        'app.eod.pykrx_pipeline_runner._run_daily_intraday_snapshot_followup',
                        return_value={'status': 'completed'},
                    )
                ),
                stack.enter_context(
                    patch(
                        'app.eod.pykrx_pipeline_runner._run_daily_feedback_followup',
                        return_value={'status': 'completed'},
                    )
                ),
                stack.enter_context(
                    patch(
                        'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_followup',
                        return_value={'status': 'completed'},
                    )
                ),
                stack.enter_context(
                    patch(
                        'app.eod.pykrx_pipeline_runner.'
                        '_run_daily_next_open_execution_plan_followup',
                        return_value={'status': 'completed'},
                    )
                ),
                stack.enter_context(
                    patch(
                        'app.eod.pykrx_pipeline_runner.'
                        '_run_daily_signal_decider_evaluation_followup',
                        return_value={'status': 'completed'},
                    )
                ),
            ]
            complete_run = stack.enter_context(
                patch('app.eod.pykrx_backfill.complete_eod_collection_run')
            )

            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSDAQ',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                symbols_override=[retry_symbol],
                retry_context={
                    'retry_of_run_id': 7,
                    'inherited_downstream_retry_symbols': ['005930'],
                    'inherited_downstream_retry_resume_from_stage': (
                        'pykrx_enrichment'
                    ),
                },
                selected_stages=PYKRX_STAGE_SEQUENCE,
                chain_fundamentals_followup=True,
                track_daily_pipeline=True,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(
            complete_run.call_args.kwargs['run_metadata'][
                'downstream_retry_symbols'
            ],
            ['005930', '472220'],
        )
        signal_features.assert_not_called()
        for followup_mock in followup_mocks:
            followup_mock.assert_not_called()

        result_keys = (
            'fundamentals_followup',
            'piotroski_filter_followup',
            'factor_scoring_followup',
            'signal_feature_followup',
            'signal_features',
            'external_news_followup',
            'external_news_enrichment_followup',
            'macro_context_followup',
            'intraday_snapshot_followup',
            'feedback_followup',
            'signal_decider_followup',
            'next_open_execution_plan_followup',
            'signal_decider_evaluation_followup',
        )
        for result_key in result_keys:
            self.assertEqual(result[result_key]['status'], 'blocked')
            self.assertEqual(
                result[result_key]['reason'],
                'inherited_downstream_recovery_pending',
            )

        latest_stage_updates = {
            update['stage_id']: update
            for update in fake_run_state_service.stage_updates
        }
        for stage_id in AUTO_DAILY_FOLLOWUP_STAGE_ORDER:
            stage_update = latest_stage_updates[stage_id]
            self.assertEqual(stage_update['status'], 'blocked')
            self.assertEqual(
                stage_update['details']['result_summary']['reason'],
                'inherited_downstream_recovery_pending',
            )


class AutoPykrxEODSchedulerTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        async def previous_weekday(value: date, **_kwargs) -> date:
            candidate = value - timedelta(days=1)
            while candidate.weekday() >= 5:
                candidate -= timedelta(days=1)
            return candidate

        recovery_target_patcher = patch(
            'app.eod.auto_pykrx.load_oldest_outstanding_auto_eod_recovery_target',
            return_value=None,
        )
        recovery_target_patcher.start()
        self.addCleanup(recovery_target_patcher.stop)
        calendar_patcher = patch(
            'app.eod.auto_pykrx.async_previous_krx_trading_date',
            new=AsyncMock(side_effect=previous_weekday),
        )
        calendar_patcher.start()
        self.addCleanup(calendar_patcher.stop)

    def test_automatic_market_default_excludes_unreliable_konex_feed(self) -> None:
        field = Settings.model_fields['eod_auto_pykrx_markets']

        self.assertEqual(field.default, 'KOSPI,KOSDAQ')

    def test_same_day_pipeline_default_waits_until_nxt_data_settles(self) -> None:
        field = Settings.model_fields['eod_auto_pykrx_after_close_minutes']

        self.assertEqual(field.default, 280)

    def test_same_day_pipeline_rejects_shorter_finality_window(self) -> None:
        with self.assertRaises(ValueError):
            Settings(eod_auto_pykrx_after_close_minutes=279)

    async def test_schedule_daily_pipeline_if_due_waits_for_close_buffer(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now - timedelta(minutes=279)).replace(second=0, microsecond=0)
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(close_at - timedelta(hours=6, minutes=30)).isoformat(),
            session_close_at=close_at.isoformat(),
        )

        with patch('app.eod.auto_pykrx.async_get_market_state', return_value=market):
            await scheduler.schedule_daily_pipeline_if_due()

        snapshot = scheduler.snapshot()
        self.assertFalse(snapshot['active_task'])
        self.assertEqual(snapshot['last_status'], 'idle')
        self.assertEqual(
            snapshot['last_reason'],
            f'waiting_until:{(close_at + timedelta(minutes=280)).isoformat()}',
        )

    async def test_schedule_daily_pipeline_if_due_starts_background_task_after_close_buffer(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now - timedelta(minutes=281)).replace(second=0, microsecond=0)
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(close_at - timedelta(hours=6, minutes=30)).isoformat(),
            session_close_at=close_at.isoformat(),
        )

        with patch('app.eod.auto_pykrx.async_get_market_state', return_value=market), patch.object(
            scheduler,
            '_run_scheduled_daily_pipeline',
            new=AsyncMock(return_value=None),
        ):
            await scheduler.schedule_daily_pipeline_if_due()
            await scheduler.stop()

        snapshot = scheduler.snapshot()
        self.assertEqual(snapshot['last_status'], 'running')

    async def test_schedule_daily_pipeline_if_due_uses_latest_session_on_weekend(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = MarketState(
            market='KRX',
            state='WEEKEND',
            is_trading_day=False,
            is_open=False,
            server_time='2026-09-13T14:00:00+09:00',
            session_open_at=None,
            session_close_at=None,
            latest_valid_trading_date='2026-09-11',
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            await scheduler.schedule_daily_pipeline_if_due()
            self.assertIsNotNone(scheduler._task)
            await scheduler._task

        run_pipeline.assert_awaited_once_with(
            '2026-09-11',
            markets=('KOSPI', 'KOSDAQ'),
        )
        self.assertEqual(scheduler.snapshot()['last_market_date'], '2026-09-11')

    async def test_weekend_scheduler_does_not_rerun_completed_latest_session(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = MarketState(
            market='KRX',
            state='WEEKEND',
            is_trading_day=False,
            is_open=False,
            server_time='2026-09-13T14:00:00+09:00',
            session_open_at=None,
            session_close_at=None,
            latest_valid_trading_date='2026-09-11',
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.auto_pykrx.load_latest_eod_collection_run',
                return_value={
                    'id': 584,
                    'status': 'completed',
                    'symbols_failed': 0,
                    'run_metadata': {},
                },
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            await scheduler.schedule_daily_pipeline_if_due()

        self.assertIsNone(scheduler._task)
        run_pipeline.assert_not_awaited()
        self.assertEqual(scheduler.snapshot()['last_status'], 'completed')
        self.assertEqual(scheduler.snapshot()['last_reason'], 'already_completed')
        self.assertEqual(scheduler.snapshot()['last_market_date'], '2026-09-11')

    async def test_open_recovery_only_returns_no_debt_without_starting_fresh_daily(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T10:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            disposition = await scheduler.schedule_recovery_if_due(
                expected_market_state='OPEN'
            )

        self.assertEqual(
            disposition,
            RecoveryOnlyScheduleDisposition.NO_DEBT,
        )
        self.assertIsNone(scheduler._task)
        run_pipeline.assert_not_awaited()
        self.assertEqual(
            scheduler.snapshot()['last_reason'],
            'no_older_recovery_debt',
        )

    async def test_open_recovery_only_returns_unknown_when_debt_inspection_fails(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T10:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                side_effect=RuntimeError('recovery database unavailable'),
            ),
        ):
            disposition = await scheduler.schedule_recovery_if_due(
                expected_market_state='OPEN'
            )

        self.assertEqual(
            disposition,
            RecoveryOnlyScheduleDisposition.UNKNOWN,
        )
        self.assertIsNone(scheduler._task)
        self.assertEqual(
            scheduler.snapshot()['last_reason'],
            'recovery_state_unavailable',
        )

    async def test_open_recovery_only_reports_due_blocked_for_pending_debt(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T10:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )
        scheduler._state.preflight_failure_market_date = '2026-09-11'
        scheduler._state.preflight_failure_markets = ('KOSPI', 'KOSDAQ')
        scheduler._state.preflight_retry_not_before = (
            datetime.now(timezone.utc) + timedelta(minutes=5)
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=AutoEODRecoveryTarget(
                    trade_date='2026-09-11',
                    eod_run_id=73,
                ),
            ),
        ):
            disposition = await scheduler.schedule_recovery_if_due(
                expected_market_state='OPEN'
            )

        self.assertEqual(
            disposition,
            RecoveryOnlyScheduleDisposition.DUE_BLOCKED,
        )
        self.assertIsNone(scheduler._task)
        self.assertEqual(
            scheduler.snapshot()['last_reason'],
            'older_recovery_debt_pending',
        )

    async def test_open_recovery_only_reports_active_task_as_due_blocked(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T10:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )
        hold_task = asyncio.create_task(asyncio.Event().wait())
        scheduler._task = hold_task

        try:
            with patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ):
                disposition = (
                    await scheduler.schedule_recovery_if_due(
                        expected_market_state='OPEN'
                    )
                )
        finally:
            await scheduler.stop()

        self.assertEqual(
            disposition,
            RecoveryOnlyScheduleDisposition.DUE_BLOCKED,
        )

    async def test_monday_open_blocks_on_outstanding_friday_recovery_without_starting_pipeline(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-09-14T10:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=AutoEODRecoveryTarget(
                    trade_date='2026-09-11',
                    eod_run_id=73,
                ),
                create=True,
            ) as load_recovery_target,
            patch(
                'app.eod.auto_pykrx.async_previous_krx_trading_date',
                new=AsyncMock(
                    side_effect=[
                        date(2026, 9, 11),
                        date(2026, 9, 10),
                        date(2026, 9, 9),
                        date(2026, 9, 8),
                    ]
                ),
                create=True,
            ) as previous_trading_date,
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            disposition = await scheduler.schedule_recovery_if_due(
                expected_market_state='OPEN'
            )

        load_recovery_target.assert_called_once_with(
            on_or_after='2026-09-08',
            on_or_before='2026-09-14',
        )
        self.assertEqual(
            previous_trading_date.await_args_list,
            [
                call(date(2026, 9, 14), allow_fallback=True),
                call(date(2026, 9, 11), allow_fallback=True),
                call(date(2026, 9, 10), allow_fallback=True),
                call(date(2026, 9, 9), allow_fallback=True),
            ],
        )
        self.assertIsNone(scheduler._task)
        run_pipeline.assert_not_awaited()
        self.assertEqual(scheduler.snapshot()['last_market_date'], '2026-09-11')
        self.assertEqual(
            scheduler.snapshot()['last_reason'],
            'older_recovery_debt_pending',
        )
        self.assertEqual(
            disposition,
            RecoveryOnlyScheduleDisposition.DUE_BLOCKED,
        )

    async def test_preopen_recovery_starts_exact_previous_session_pipeline(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-14T08:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=AutoEODRecoveryTarget(
                    trade_date='2026-09-11',
                    eod_run_id=73,
                ),
            ),
            patch(
                'app.eod.auto_pykrx.PYKRX_BACKFILL_SERVICE.'
                'run_daily_pipeline_if_due',
                return_value={'status': 'completed'},
            ) as run_daily,
            patch(
                'app.eod.auto_pykrx.async_previous_krx_trading_date',
                new=AsyncMock(
                    side_effect=[
                        date(2026, 9, 11),
                        date(2026, 9, 10),
                        date(2026, 9, 9),
                        date(2026, 9, 8),
                        date(2026, 9, 11),
                    ]
                ),
            ),
        ):
            disposition = await scheduler.schedule_recovery_if_due(
                expected_market_state='PREOPEN'
            )
            self.assertIsNotNone(scheduler._task)
            await scheduler._task

        self.assertEqual(
            disposition,
            RecoveryOnlyScheduleDisposition.DUE_STARTED,
        )
        self.assertFalse(scheduler.state.active_task)
        self.assertEqual(scheduler.snapshot()['last_status'], 'completed')
        run_daily.assert_called_once_with(
            market_date='2026-09-11',
            markets=('KOSPI', 'KOSDAQ'),
            trigger='auto_close_scheduler',
            refresh_universe=settings.eod_auto_pykrx_refresh_universe,
            point_in_time_recovery=True,
            recovery_eod_run_id=73,
        )

    async def test_preopen_recovery_keeps_older_than_previous_session_blocked(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-14T08:00:00+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=AutoEODRecoveryTarget(
                    trade_date='2026-09-10',
                    eod_run_id=72,
                ),
            ),
            patch(
                'app.eod.auto_pykrx.async_previous_krx_trading_date',
                new=AsyncMock(
                    side_effect=[
                        date(2026, 9, 11),
                        date(2026, 9, 10),
                        date(2026, 9, 9),
                        date(2026, 9, 8),
                        date(2026, 9, 11),
                    ]
                ),
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            disposition = await scheduler.schedule_recovery_if_due(
                expected_market_state='PREOPEN'
            )

        self.assertEqual(
            disposition,
            RecoveryOnlyScheduleDisposition.DUE_BLOCKED,
        )
        self.assertIsNone(scheduler._task)
        run_pipeline.assert_not_awaited()
        self.assertEqual(
            scheduler.snapshot()['last_reason'],
            'older_recovery_debt_pending',
        )

    async def test_daily_scheduler_never_starts_recovery_during_preopen(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        preopen = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-09-14T08:59:59+09:00',
            session_open_at='2026-09-14T09:00:00+09:00',
            session_close_at='2026-09-14T15:30:00+09:00',
            latest_valid_trading_date='2026-09-11',
        )
        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=preopen,
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=AutoEODRecoveryTarget(
                    trade_date='2026-09-11',
                    eod_run_id=73,
                ),
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            await scheduler.schedule_daily_pipeline_if_due(
                expected_market_state='PREOPEN'
            )

        self.assertIsNone(scheduler._task)
        run_pipeline.assert_not_awaited()
        self.assertEqual(scheduler.snapshot()['last_market_state'], 'PREOPEN')
        self.assertEqual(
            scheduler.snapshot()['last_reason'],
            'market_not_closed:PREOPEN',
        )

    async def test_closed_session_defers_older_recovery_until_same_day_finality_window(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now - timedelta(minutes=30)).replace(second=0, microsecond=0)
        current_market_date = now.date().isoformat()
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(close_at - timedelta(hours=6, minutes=30)).isoformat(),
            session_close_at=close_at.isoformat(),
            latest_valid_trading_date=current_market_date,
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=AutoEODRecoveryTarget(
                    trade_date='2026-09-11',
                    eod_run_id=81,
                ),
                create=True,
            ) as load_recovery_target,
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            await scheduler.schedule_daily_pipeline_if_due()
            self.assertIsNone(scheduler._task)

        load_recovery_target.assert_called_once()
        run_pipeline.assert_not_awaited()
        self.assertEqual(scheduler.snapshot()['last_status'], 'idle')
        self.assertEqual(
            scheduler.snapshot()['last_reason'],
            f'waiting_until:{(close_at + timedelta(minutes=280)).isoformat()}',
        )

    async def test_closed_session_starts_older_recovery_after_same_day_finality_window(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now - timedelta(minutes=281)).replace(
            second=0,
            microsecond=0,
        )
        current_market_date = now.date().isoformat()
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(
                close_at - timedelta(hours=6, minutes=30)
            ).isoformat(),
            session_close_at=close_at.isoformat(),
            latest_valid_trading_date=current_market_date,
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=AutoEODRecoveryTarget(
                    trade_date='2026-09-11',
                    eod_run_id=81,
                ),
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            await scheduler.schedule_daily_pipeline_if_due(
                expected_market_state='CLOSED'
            )
            self.assertIsNotNone(scheduler._task)
            await scheduler._task

        run_pipeline.assert_awaited_once_with(
            '2026-09-11',
            markets=('KOSPI', 'KOSDAQ'),
            point_in_time_recovery=True,
            recovery_eod_run_id=81,
        )

    async def test_closed_scheduler_rechecks_session_before_starting_recovery(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now - timedelta(minutes=281)).replace(
            second=0,
            microsecond=0,
        )
        current_market_date = now.date().isoformat()
        closed = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(
                close_at - timedelta(hours=6, minutes=30)
            ).isoformat(),
            session_close_at=close_at.isoformat(),
            latest_valid_trading_date=current_market_date,
        )
        preopen = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time=(now + timedelta(days=1)).replace(
                hour=8,
                minute=0,
            ).isoformat(),
            session_open_at=(now + timedelta(days=1)).replace(
                hour=9,
                minute=0,
            ).isoformat(),
            session_close_at=(now + timedelta(days=1)).replace(
                hour=15,
                minute=30,
            ).isoformat(),
            latest_valid_trading_date=current_market_date,
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                side_effect=[closed, preopen],
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=AutoEODRecoveryTarget(
                    trade_date='2026-09-11',
                    eod_run_id=81,
                ),
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            await scheduler.schedule_daily_pipeline_if_due(
                expected_market_state='CLOSED'
            )

        self.assertIsNone(scheduler._task)
        run_pipeline.assert_not_awaited()
        self.assertEqual(
            scheduler.snapshot()['last_reason'],
            'market_state_changed:CLOSED->PREOPEN',
        )

    async def test_same_day_recovery_does_not_bypass_finality_window(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now - timedelta(minutes=30)).replace(second=0, microsecond=0)
        current_market_date = now.date().isoformat()
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(close_at - timedelta(hours=6, minutes=30)).isoformat(),
            session_close_at=close_at.isoformat(),
            latest_valid_trading_date=current_market_date,
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch(
                'app.eod.auto_pykrx.'
                'load_oldest_outstanding_auto_eod_recovery_target',
                return_value=AutoEODRecoveryTarget(
                    trade_date=current_market_date,
                    eod_run_id=82,
                ),
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            await scheduler.schedule_daily_pipeline_if_due()

        self.assertIsNone(scheduler._task)
        run_pipeline.assert_not_awaited()
        self.assertEqual(scheduler.snapshot()['last_status'], 'idle')
        self.assertEqual(
            scheduler.snapshot()['last_reason'],
            f'waiting_until:{(close_at + timedelta(minutes=280)).isoformat()}',
        )

    async def test_triggered_failed_pipeline_remains_failed_in_scheduler_status(self) -> None:
        scheduler = AutoPykrxEODScheduler()

        with patch(
            'app.eod.auto_pykrx.PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
            return_value={
                'triggered': True,
                'status': 'failed',
                'reason': 'price_collection_failed',
            },
        ):
            await scheduler._run_scheduled_daily_pipeline(
                '2026-09-11',
                markets=('KOSPI', 'KOSDAQ'),
            )

        self.assertEqual(scheduler.snapshot()['last_status'], 'failed')

    async def test_scheduled_recovery_passes_exact_eod_run_id_to_runtime(
        self,
    ) -> None:
        scheduler = AutoPykrxEODScheduler()

        with patch(
            'app.eod.auto_pykrx.PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
            return_value={
                'triggered': False,
                'status': 'skipped',
                'reason': 'already_completed',
            },
        ) as run_daily:
            await scheduler._run_scheduled_daily_pipeline(
                '2026-09-11',
                markets=('KOSPI', 'KOSDAQ'),
                point_in_time_recovery=True,
                recovery_eod_run_id=73,
            )

        self.assertEqual(run_daily.call_args.kwargs['recovery_eod_run_id'], 73)

    async def test_triggered_completed_with_errors_pipeline_is_partial(self) -> None:
        scheduler = AutoPykrxEODScheduler()

        with patch(
            'app.eod.auto_pykrx.PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
            return_value={
                'triggered': True,
                'status': 'completed_with_errors',
            },
        ):
            await scheduler._run_scheduled_daily_pipeline(
                '2026-09-11',
                markets=('KOSPI', 'KOSDAQ'),
            )

        self.assertEqual(scheduler.snapshot()['last_status'], 'partial')

    async def test_incomplete_universe_preflight_uses_backoff_then_retries(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now - timedelta(minutes=281)).replace(second=0, microsecond=0)
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(close_at - timedelta(hours=6, minutes=30)).isoformat(),
            session_close_at=close_at.isoformat(),
        )
        incomplete_error = RuntimeError(
            'PyKRX universe snapshot is incomplete for requested markets: KONEX.'
        )

        with (
            patch('app.eod.auto_pykrx.async_get_market_state', return_value=market),
            patch(
                'app.eod.auto_pykrx.PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
                side_effect=[
                    incomplete_error,
                    {'triggered': False, 'reason': 'already_completed'},
                ],
            ) as run_daily,
            patch(
                'app.eod.auto_pykrx.settings.eod_auto_pykrx_markets',
                'KOSPI,KOSDAQ,KONEX',
            ),
        ):
            await scheduler.schedule_daily_pipeline_if_due()
            self.assertIsNotNone(scheduler._task)
            await scheduler._task

            await scheduler.schedule_daily_pipeline_if_due()

            self.assertEqual(run_daily.call_count, 1)
            self.assertEqual(scheduler.snapshot()['last_status'], 'blocked')
            self.assertEqual(
                scheduler.snapshot()['last_reason'],
                'incomplete_universe_preflight_backoff',
            )

            scheduler._state.preflight_retry_not_before = (
                datetime.now(timezone.utc) - timedelta(seconds=1)
            )
            await scheduler.schedule_daily_pipeline_if_due()
            self.assertIsNotNone(scheduler._task)
            await scheduler._task

        self.assertEqual(run_daily.call_count, 2)
