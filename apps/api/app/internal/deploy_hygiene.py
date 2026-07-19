from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any, Iterable

import httpx
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.engine.url import make_url

from app.core.settings import settings
from app.db import DATABASE_URL, engine, get_alembic_config
from app.market.snapshot import market_snapshot_service
from app.orders.portfolio_summary_service import portfolio_summary_service

OK_STATUS = 'ok'
WARN_STATUS = 'warn'
FAIL_STATUS = 'fail'

RUNTIME_SCHEMA_TABLE_DESCRIPTIONS: dict[str, str] = {
    'symbols': 'Symbol identity table; symbol writes and downstream FKs depend on symbols.id and symbols.symbol.',
    'tracked_symbols': 'Tracked universe state used by scanner, EOD, quote, and execution workflows.',
    'current_quote_cache': 'Current quote read model updated by quote ingestion and read by execution/runtime dashboards.',
    'daily_price_bars': 'Primary EOD price-bar store; PyKRX/KIS writes use ON CONFLICT by symbol/date.',
    'daily_price_contexts': 'Derived price context store used by agent-context reads.',
    'daily_technical_snapshots': 'Daily technical read model written by the EOD pipeline.',
    'daily_action_summaries': 'Daily action-summary read model written by the EOD pipeline.',
    'daily_signal_features': 'KIS ranking signal feature table used by EOD and decider context.',
    'daily_market_risk_snapshots': 'Daily KIS market risk source snapshots.',
    'daily_market_risk_filter_results': 'Persisted market risk filter results.',
    'daily_piotroski_filter_results': 'Persisted Piotroski filter results.',
    'daily_momentum_factor_results': 'Persisted momentum factor results.',
    'daily_low_risk_factor_results': 'Persisted low-risk factor results.',
    'daily_beta_factor_results': 'Persisted beta factor results.',
    'daily_value_factor_results': 'Persisted value factor results.',
    'daily_advanced_value_factor_results': 'Persisted advanced value factor results.',
    'daily_unified_value_factor_results': 'Persisted unified value factor results.',
    'daily_quality_factor_results': 'Persisted quality factor results.',
    'daily_composite_factor_results': 'Persisted composite factor results.',
    'daily_market_participation_contexts': 'Persisted market participation context written by KIS signal collection.',
    'daily_pykrx_enrichment_rows': 'Persisted PyKRX enrichment rows for sector, investor-flow, and short-selling context.',
    'dart_disclosure_events': 'DART disclosure event store; disclosure sync upserts by receipt number.',
    'dart_fundamentals_snapshots': 'DART fundamentals snapshot store; fundamentals capture upserts by event/period/fs_div.',
    'agent_context_feedback': 'Agent feedback store used by decider snapshot persistence.',
    'agent_context_feedback_failures': 'Agent feedback failure store used by retry/repair workflows.',
    'agent_context_feedback_runs': 'Agent feedback run-state store used by retry/run tracking.',
    'agent_feedback_session_cleanup_failures': 'Cleanup failure retry queue for agent-feedback transcript/session cleanup.',
    'signal_decider_decision_snapshots': 'Signal-decider snapshot store; feedback-linked snapshots use ON CONFLICT.',
    'signal_decider_realized_outcomes': 'Signal-decider realized outcome store written by evaluation refresh.',
    'signal_decider_evaluations': 'Signal-decider evaluation store written by evaluation refresh.',
    'decider_post_decision_evaluations': 'Post-decision evaluation store used by decider feedback quality loops.',
    'signal_decider_calendar_review_items': 'Signal-decider calendar-review read model.',
    'signal_decider_policy_versions': 'Signal-decider policy version store.',
    'signal_decider_policy_candidates': 'Signal-decider policy candidate store.',
    'signal_decider_execution_plan_items': 'Signal-decider next-open/intraday execution plan items.',
    'signal_decider_actual_execution_outcomes': 'Actual execution outcome store linked to execution plan items.',
    'order_requests': 'Normalized trading order request store; duplicate protection uses ON CONFLICT.',
    'broker_orders': 'Current broker order snapshots; execution submit/reconcile writes select and update this parent store first.',
    'broker_order_events': 'Broker order event ledger; event dedupe uses ON CONFLICT.',
}

RUNTIME_SCHEMA_PRIMARY_KEY_CHECKS: tuple[dict[str, Any], ...] = (
    {
        'name': 'symbols_id_primary_key',
        'table': 'symbols',
        'columns': ('id',),
        'description': 'symbols.id must be the primary key so downstream foreign keys and ORM identity are valid.',
    },
    {
        'name': 'broker_orders_id_primary_key',
        'table': 'broker_orders',
        'columns': ('id',),
        'description': 'broker_orders.id must be the primary key so broker order event foreign keys are valid.',
    },
)

RUNTIME_SCHEMA_UNIQUE_CHECKS: tuple[dict[str, Any], ...] = (
    {
        'name': 'symbols_symbol_unique',
        'table': 'symbols',
        'columns': ('symbol',),
        'required_name': None,
        'description': 'Symbol upserts use ON CONFLICT(symbol).',
    },
    {
        'name': 'tracked_symbols_symbol_id_unique',
        'table': 'tracked_symbols',
        'columns': ('symbol_id',),
        'required_name': 'uq_tracked_symbols_symbol_id',
        'description': 'Tracked-symbol replacement depends on one row per symbol.',
    },
    {
        'name': 'current_quote_cache_symbol_id_unique',
        'table': 'current_quote_cache',
        'columns': ('symbol_id',),
        'required_name': 'uq_current_quote_cache_symbol_id',
        'description': 'Current quote cache must have one mutable row per symbol.',
    },
    {
        'name': 'daily_price_bars_symbol_trade_date_unique',
        'table': 'daily_price_bars',
        'columns': ('symbol_id', 'trade_date'),
        'required_name': None,
        'description': 'Daily price-bar persistence uses ON CONFLICT(symbol_id, trade_date).',
    },
    {
        'name': 'daily_price_contexts_symbol_market_date_unique',
        'table': 'daily_price_contexts',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_price_contexts_symbol_market_date',
        'description': 'Daily price-context persistence uses this named conflict target.',
    },
    {
        'name': 'daily_technical_snapshots_symbol_market_date_unique',
        'table': 'daily_technical_snapshots',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_technical_snapshots_symbol_market_date',
        'description': 'Daily technical snapshot persistence uses this named conflict target.',
    },
    {
        'name': 'daily_action_summaries_symbol_market_date_unique',
        'table': 'daily_action_summaries',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_action_summaries_symbol_market_date',
        'description': 'Daily action summary persistence uses this named conflict target.',
    },
    {
        'name': 'daily_signal_features_symbol_market_date_unique',
        'table': 'daily_signal_features',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_signal_features_symbol_market_date',
        'description': 'Daily signal feature persistence uses this named conflict target.',
    },
    {
        'name': 'daily_market_risk_snapshots_symbol_market_date_unique',
        'table': 'daily_market_risk_snapshots',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_market_risk_snapshots_symbol_market_date',
        'description': 'Daily market risk snapshot persistence uses this named conflict target.',
    },
    {
        'name': 'daily_market_risk_filter_results_symbol_market_date_unique',
        'table': 'daily_market_risk_filter_results',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_market_risk_filter_results_symbol_market_date',
        'description': 'Daily market risk filter persistence uses this named conflict target.',
    },
    {
        'name': 'daily_piotroski_filter_results_symbol_market_date_unique',
        'table': 'daily_piotroski_filter_results',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_piotroski_filter_results_symbol_market_date',
        'description': 'Daily Piotroski filter persistence uses this named conflict target.',
    },
    {
        'name': 'daily_momentum_factor_results_symbol_market_date_unique',
        'table': 'daily_momentum_factor_results',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_momentum_factor_results_symbol_market_date',
        'description': 'Daily momentum factor persistence uses this named conflict target.',
    },
    {
        'name': 'daily_low_risk_factor_results_symbol_market_date_unique',
        'table': 'daily_low_risk_factor_results',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_low_risk_factor_results_symbol_market_date',
        'description': 'Daily low-risk factor persistence uses this named conflict target.',
    },
    {
        'name': 'daily_beta_factor_results_symbol_market_date_unique',
        'table': 'daily_beta_factor_results',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_beta_factor_results_symbol_market_date',
        'description': 'Daily beta factor persistence uses this named conflict target.',
    },
    {
        'name': 'daily_value_factor_results_symbol_market_date_unique',
        'table': 'daily_value_factor_results',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_value_factor_results_symbol_market_date',
        'description': 'Daily value factor persistence uses this named conflict target.',
    },
    {
        'name': 'daily_advanced_value_factor_results_symbol_market_date_unique',
        'table': 'daily_advanced_value_factor_results',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_advanced_value_factor_results_symbol_market_date',
        'description': 'Daily advanced-value factor persistence uses this named conflict target.',
    },
    {
        'name': 'daily_unified_value_factor_results_symbol_market_date_unique',
        'table': 'daily_unified_value_factor_results',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_unified_value_factor_results_symbol_market_date',
        'description': 'Daily unified-value factor persistence uses this named conflict target.',
    },
    {
        'name': 'daily_quality_factor_results_symbol_market_date_unique',
        'table': 'daily_quality_factor_results',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_quality_factor_results_symbol_market_date',
        'description': 'Daily quality factor persistence uses this named conflict target.',
    },
    {
        'name': 'daily_composite_factor_results_symbol_market_date_unique',
        'table': 'daily_composite_factor_results',
        'columns': ('symbol_id', 'market_date'),
        'required_name': 'uq_daily_composite_factor_results_symbol_market_date',
        'description': 'Daily composite factor persistence uses this named conflict target.',
    },
    {
        'name': 'daily_market_participation_contexts_market_market_date_unique',
        'table': 'daily_market_participation_contexts',
        'columns': ('market', 'market_date'),
        'required_name': 'uq_daily_market_participation_contexts_market_market_date',
        'description': 'Daily market participation persistence uses this named conflict target.',
    },
    {
        'name': 'daily_pykrx_enrichment_rows_scope_unique',
        'table': 'daily_pykrx_enrichment_rows',
        'columns': ('market_date', 'category', 'scope_type', 'scope_key', 'source'),
        'required_name': 'uq_daily_pykrx_enrichment_rows_scope',
        'description': 'PyKRX enrichment persistence uses this named conflict target.',
    },
    {
        'name': 'dart_disclosure_events_rcept_no_unique',
        'table': 'dart_disclosure_events',
        'columns': ('rcept_no',),
        'required_name': None,
        'description': 'DART disclosure sync uses ON CONFLICT(rcept_no).',
    },
    {
        'name': 'dart_fundamentals_snapshots_event_period_fs_div_unique',
        'table': 'dart_fundamentals_snapshots',
        'columns': ('disclosure_event_id', 'business_year', 'report_code', 'fs_div'),
        'required_name': 'uq_dart_fundamentals_snapshots_event_period_fs_div',
        'description': 'DART fundamentals capture uses this named conflict target.',
    },
    {
        'name': 'agent_context_feedback_market_symbol_agent_unique',
        'table': 'agent_context_feedback',
        'columns': ('market_date', 'symbol', 'agent_name'),
        'required_name': 'uq_agent_context_feedback_market_symbol_agent',
        'description': 'Agent feedback persistence uses this named conflict target.',
    },
    {
        'name': 'agent_context_feedback_failures_market_symbol_agent_unique',
        'table': 'agent_context_feedback_failures',
        'columns': ('market_date', 'symbol', 'agent_name'),
        'required_name': 'uq_agent_context_feedback_failures_market_symbol_agent',
        'description': 'Agent feedback failure persistence uses this named conflict target.',
    },
    {
        'name': 'agent_context_feedback_runs_run_id_unique',
        'table': 'agent_context_feedback_runs',
        'columns': ('run_id',),
        'required_name': None,
        'description': 'Agent feedback run-state persistence uses ON CONFLICT(run_id).',
    },
    {
        'name': 'agent_feedback_session_cleanup_failures_session_key_unique',
        'table': 'agent_feedback_session_cleanup_failures',
        'columns': ('session_key',),
        'required_name': 'uq_agent_feedback_session_cleanup_failures_session_key',
        'description': 'Agent feedback session cleanup retry persistence uses this named conflict target.',
    },
    {
        'name': 'signal_decider_decision_snapshots_feedback_id_unique',
        'table': 'signal_decider_decision_snapshots',
        'columns': ('feedback_id',),
        'required_name': 'uq_signal_decider_decision_snapshots_feedback_id_not_null',
        'description': 'Signal-decider snapshot persistence uses the partial unique feedback_id index.',
    },
    {
        'name': 'signal_decider_realized_outcomes_snapshot_horizon_unique',
        'table': 'signal_decider_realized_outcomes',
        'columns': ('decision_snapshot_id', 'horizon_days'),
        'required_name': 'uq_signal_decider_realized_outcomes_snapshot_horizon',
        'description': 'Signal-decider realized outcome persistence uses this named conflict target.',
    },
    {
        'name': 'signal_decider_evaluations_snapshot_horizon_version_unique',
        'table': 'signal_decider_evaluations',
        'columns': ('decision_snapshot_id', 'horizon_days', 'evaluation_version'),
        'required_name': 'uq_signal_decider_evaluations_snapshot_horizon_version',
        'description': 'Signal-decider evaluation persistence uses this named conflict target.',
    },
    {
        'name': 'decider_post_decision_evaluations_feedback_window_unique',
        'table': 'decider_post_decision_evaluations',
        'columns': ('feedback_id', 'window_days'),
        'required_name': 'uq_decider_post_decision_evaluations_feedback_window',
        'description': 'Post-decision evaluation persistence uses this named conflict target.',
    },
    {
        'name': 'signal_decider_calendar_review_items_snapshot_eval_horizon_unique',
        'table': 'signal_decider_calendar_review_items',
        'columns': ('decision_snapshot_id', 'evaluation_version', 'primary_horizon_days'),
        'required_name': 'uq_sd_cal_review_items_snapshot_eval_horizon',
        'description': 'Signal-decider calendar review persistence uses this named conflict target.',
    },
    {
        'name': 'signal_decider_policy_versions_version_unique',
        'table': 'signal_decider_policy_versions',
        'columns': ('policy_version',),
        'required_name': 'uq_signal_decider_policy_versions_version',
        'description': 'Signal-decider policy version persistence uses this named conflict target.',
    },
    {
        'name': 'signal_decider_policy_candidates_version_unique',
        'table': 'signal_decider_policy_candidates',
        'columns': ('candidate_version',),
        'required_name': 'uq_signal_decider_policy_candidates_version',
        'description': 'Signal-decider policy candidate persistence uses this named conflict target.',
    },
    {
        'name': 'signal_decider_execution_plan_items_scope_unique',
        'table': 'signal_decider_execution_plan_items',
        'columns': ('execution_window', 'target_trade_date', 'account_reference', 'symbol', 'decision_agent_name'),
        'required_name': 'uq_signal_decider_execution_plan_items_scope',
        'description': 'Signal-decider execution plan refresh depends on one row per execution scope.',
    },
    {
        'name': 'signal_decider_actual_execution_outcomes_entry_plan_unique',
        'table': 'signal_decider_actual_execution_outcomes',
        'columns': ('entry_plan_item_id',),
        'required_name': 'uq_sdao_entry_plan',
        'description': 'Actual execution outcomes depend on one outcome per entry plan item.',
    },
    {
        'name': 'order_requests_broker_account_client_order_id_unique',
        'table': 'order_requests',
        'columns': ('broker_name', 'account_reference', 'client_order_id'),
        'required_name': 'uq_order_requests_broker_account_client_order_id',
        'description': 'Trading order request persistence uses this named conflict target.',
    },
    {
        'name': 'broker_orders_broker_account_client_order_id_unique',
        'table': 'broker_orders',
        'columns': ('broker_name', 'account_reference', 'client_order_id'),
        'required_name': 'uq_broker_orders_broker_account_client_order_id',
        'description': 'Broker order upsert/load uses one current snapshot per broker account client order.',
    },
    {
        'name': 'broker_orders_broker_account_broker_order_id_unique',
        'table': 'broker_orders',
        'columns': ('broker_name', 'account_reference', 'broker_order_id'),
        'required_name': 'uq_broker_orders_broker_account_broker_order_id',
        'description': 'Broker order reconciliation uses one current snapshot per broker account broker order id.',
    },
    {
        'name': 'broker_order_events_broker_order_dedup_key_unique',
        'table': 'broker_order_events',
        'columns': ('broker_order_row_id', 'event_dedup_key'),
        'required_name': 'uq_broker_order_events_broker_order_dedup_key',
        'description': 'Broker order event persistence uses this named conflict target.',
    },
)

RUNTIME_SCHEMA_FOREIGN_KEY_CHECKS: tuple[dict[str, Any], ...] = (
    {
        'name': 'tracked_symbols_symbol_id_fk',
        'table': 'tracked_symbols',
        'columns': ('symbol_id',),
        'referred_table': 'symbols',
        'referred_columns': ('id',),
        'description': 'tracked_symbols.symbol_id must reference symbols.id.',
    },
    {
        'name': 'current_quote_cache_symbol_id_fk',
        'table': 'current_quote_cache',
        'columns': ('symbol_id',),
        'referred_table': 'symbols',
        'referred_columns': ('id',),
        'description': 'current_quote_cache.symbol_id must reference symbols.id.',
    },
    {
        'name': 'daily_price_bars_symbol_id_fk',
        'table': 'daily_price_bars',
        'columns': ('symbol_id',),
        'referred_table': 'symbols',
        'referred_columns': ('id',),
        'description': 'daily_price_bars.symbol_id must reference symbols.id.',
    },
    {
        'name': 'daily_signal_features_symbol_id_fk',
        'table': 'daily_signal_features',
        'columns': ('symbol_id',),
        'referred_table': 'symbols',
        'referred_columns': ('id',),
        'description': 'daily_signal_features.symbol_id must reference symbols.id.',
    },
    {
        'name': 'dart_fundamentals_snapshots_disclosure_event_id_fk',
        'table': 'dart_fundamentals_snapshots',
        'columns': ('disclosure_event_id',),
        'referred_table': 'dart_disclosure_events',
        'referred_columns': ('id',),
        'description': 'dart_fundamentals_snapshots.disclosure_event_id must reference dart_disclosure_events.id.',
    },
    {
        'name': 'signal_decider_execution_plan_items_decision_snapshot_id_fk',
        'table': 'signal_decider_execution_plan_items',
        'columns': ('decision_snapshot_id',),
        'referred_table': 'signal_decider_decision_snapshots',
        'referred_columns': ('id',),
        'description': 'Execution plan items must reference persisted decision snapshots.',
    },
    {
        'name': 'signal_decider_actual_execution_outcomes_entry_plan_item_id_fk',
        'table': 'signal_decider_actual_execution_outcomes',
        'columns': ('entry_plan_item_id',),
        'referred_table': 'signal_decider_execution_plan_items',
        'referred_columns': ('id',),
        'description': 'Actual execution outcomes must reference entry execution plan items.',
    },
    {
        'name': 'broker_order_events_broker_order_row_id_fk',
        'table': 'broker_order_events',
        'columns': ('broker_order_row_id',),
        'referred_table': 'broker_orders',
        'referred_columns': ('id',),
        'description': 'Broker order events must reference the parent broker order snapshot.',
    },
)


def _market_snapshot_runtime_actions(runtime_status: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    if runtime_status.get('effective_kis_environment') != 'real':
        actions.append(
            'Enable MARKET_SNAPSHOT_KIS_USE_REAL=true or run with KIS_USE_MOCK=false so /market/snapshot uses the real KIS domain.'
        )
    if not runtime_status.get('kis_app_key_configured') or not runtime_status.get('kis_app_secret_configured'):
        actions.append('Set KIS_APP_KEY and KIS_APP_SECRET in apps/api/.env before deploying market snapshot.')
    if not runtime_status.get('koreaexim_api_key_configured'):
        actions.append('Set KOREAEXIM_API_KEY in apps/api/.env before deploying market snapshot.')
    actions.append(
        'After restart, run the deploy smoke check against /health, /eod/freshness, /market/snapshot, and /portfolio/summary.'
    )
    return actions


def _portfolio_summary_runtime_actions(runtime_status: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    if not runtime_status.get('portfolio_account_references_configured'):
        actions.append(
            'Set PORTFOLIO_ACCOUNT_REFERENCES in apps/api/.env to one or more Label=KIS:<account_number>-<product_code> entries.'
        )
    elif not runtime_status.get('portfolio_account_references_valid'):
        actions.append(
            'Fix PORTFOLIO_ACCOUNT_REFERENCES so each entry uses KIS:<account_number>-<product_code> or Label=KIS:<account_number>-<product_code>.'
        )
    if not runtime_status.get('kis_app_key_configured') or not runtime_status.get('kis_app_secret_configured'):
        actions.append('Set KIS_APP_KEY and KIS_APP_SECRET in apps/api/.env before deploying portfolio summary.')
    if runtime_status.get('app_env', '').lower() == 'production' and runtime_status.get('kis_use_mock'):
        actions.append('Set KIS_USE_MOCK=false in production so portfolio summary uses live KIS account inquiries.')
    actions.append('After restart, verify GET /portfolio/summary returns sync.status other than not_configured.')
    return actions


def _redacted_database_url() -> str:
    try:
        return make_url(DATABASE_URL).render_as_string(hide_password=True)
    except Exception:
        return DATABASE_URL


def _parse_create_table_metadata(path: Path) -> tuple[list[str], bool]:
    source = path.read_text(encoding='utf-8')
    create_tables: list[str] = []

    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return create_tables, False

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        function = node.func
        if not isinstance(function, ast.Attribute) or function.attr != 'create_table' or not node.args:
            continue

        first_arg = node.args[0]
        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
            create_tables.append(first_arg.value)

    duplicate_table_guarded = any(
        marker in source for marker in ('_table_exists(', '.has_table(', 'checkfirst=True')
    )
    return sorted(dict.fromkeys(create_tables)), duplicate_table_guarded


def _load_revision_catalog() -> tuple[tuple[str, ...], list[dict[str, Any]]]:
    script_dir = ScriptDirectory.from_config(get_alembic_config())
    chain = list(script_dir.walk_revisions(base='base', head='heads'))
    chain.reverse()

    catalog: list[dict[str, Any]] = []
    for revision in chain:
        path = Path(revision.path)
        create_tables, duplicate_table_guarded = _parse_create_table_metadata(path)
        catalog.append(
            {
                'revision': revision.revision,
                'migration': path.name,
                'create_tables': create_tables,
                'duplicate_table_guarded': duplicate_table_guarded,
            }
        )

    return tuple(script_dir.get_heads()), catalog


def _runtime_schema_required_tables() -> tuple[str, ...]:
    tables = set(RUNTIME_SCHEMA_TABLE_DESCRIPTIONS)
    for spec in RUNTIME_SCHEMA_PRIMARY_KEY_CHECKS:
        tables.add(spec['table'])
    for spec in RUNTIME_SCHEMA_UNIQUE_CHECKS:
        tables.add(spec['table'])
    for spec in RUNTIME_SCHEMA_FOREIGN_KEY_CHECKS:
        tables.add(spec['table'])
        tables.add(spec['referred_table'])
    return tuple(sorted(tables))


def _normalize_column_names(value: Any) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, str):
        return (value,)
    try:
        return tuple(str(item) for item in value if item is not None)
    except TypeError:
        return tuple()


def _column_sets_match(actual: Iterable[str], expected: Iterable[str]) -> bool:
    return set(actual) == set(expected)


def _format_columns(columns: Iterable[str]) -> str:
    return '(' + ', '.join(columns) + ')'


def _normalize_reflected_entries(raw_entries: Any, *, label: str, table: str) -> tuple[list[dict[str, Any]], list[str]]:
    if raw_entries is None:
        return [], []
    if not isinstance(raw_entries, (list, tuple)):
        return [], [f'{label} reflection for {table} returned {type(raw_entries).__name__}, expected list.']

    entries: list[dict[str, Any]] = []
    warnings: list[str] = []
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict):
            warnings.append(f'{label} reflection for {table} included non-dict entry {type(raw_entry).__name__}.')
            continue
        entries.append(raw_entry)
    return entries, warnings


def _normalize_primary_key(raw_pk: Any, *, table: str) -> tuple[dict[str, Any], list[str]]:
    if raw_pk is None:
        return {'name': None, 'columns': []}, []
    if not isinstance(raw_pk, dict):
        return {'name': None, 'columns': []}, [
            f'Primary-key reflection for {table} returned {type(raw_pk).__name__}, expected dict.'
        ]
    return {
        'name': raw_pk.get('name'),
        'columns': list(_normalize_column_names(raw_pk.get('constrained_columns') or raw_pk.get('column_names'))),
    }, []


def _normalize_unique_constraint(raw_constraint: dict[str, Any]) -> dict[str, Any]:
    return {
        'source': 'unique_constraint',
        'name': raw_constraint.get('name'),
        'columns': list(_normalize_column_names(raw_constraint.get('column_names') or raw_constraint.get('constrained_columns'))),
    }


def _normalize_unique_index(raw_index: dict[str, Any]) -> dict[str, Any] | None:
    if not raw_index.get('unique'):
        return None
    dialect_options = raw_index.get('dialect_options')
    where_clause = None
    if isinstance(dialect_options, dict):
        where_clause = dialect_options.get('postgresql_where') or dialect_options.get('sqlite_where')
    return {
        'source': 'unique_index',
        'name': raw_index.get('name'),
        'columns': list(_normalize_column_names(raw_index.get('column_names') or raw_index.get('expressions'))),
        'where': str(where_clause) if where_clause is not None else None,
    }


def _normalize_foreign_key(raw_key: dict[str, Any]) -> dict[str, Any]:
    return {
        'name': raw_key.get('name'),
        'columns': list(_normalize_column_names(raw_key.get('constrained_columns'))),
        'referred_table': raw_key.get('referred_table'),
        'referred_columns': list(_normalize_column_names(raw_key.get('referred_columns'))),
    }


def _reflect_runtime_table(inspector: Any, table: str) -> tuple[dict[str, Any], list[str]]:
    metadata: dict[str, Any] = {
        'primary_key': {'name': None, 'columns': []},
        'unique_constraints': [],
        'unique_indexes': [],
        'foreign_keys': [],
    }
    warnings: list[str] = []

    try:
        primary_key, pk_warnings = _normalize_primary_key(inspector.get_pk_constraint(table), table=table)
        metadata['primary_key'] = primary_key
        warnings.extend(pk_warnings)
    except Exception as exc:
        warnings.append(f'Could not reflect primary key for {table}: {exc}')

    try:
        raw_constraints, constraint_warnings = _normalize_reflected_entries(
            inspector.get_unique_constraints(table),
            label='Unique-constraint',
            table=table,
        )
        metadata['unique_constraints'] = [_normalize_unique_constraint(item) for item in raw_constraints]
        warnings.extend(constraint_warnings)
    except Exception as exc:
        warnings.append(f'Could not reflect unique constraints for {table}: {exc}')

    try:
        raw_indexes, index_warnings = _normalize_reflected_entries(
            inspector.get_indexes(table),
            label='Index',
            table=table,
        )
        metadata['unique_indexes'] = [
            normalized for item in raw_indexes if (normalized := _normalize_unique_index(item)) is not None
        ]
        warnings.extend(index_warnings)
    except Exception as exc:
        warnings.append(f'Could not reflect indexes for {table}: {exc}')

    try:
        raw_foreign_keys, foreign_key_warnings = _normalize_reflected_entries(
            inspector.get_foreign_keys(table),
            label='Foreign-key',
            table=table,
        )
        metadata['foreign_keys'] = [_normalize_foreign_key(item) for item in raw_foreign_keys]
        warnings.extend(foreign_key_warnings)
    except Exception as exc:
        warnings.append(f'Could not reflect foreign keys for {table}: {exc}')

    return metadata, warnings


def _unique_candidates(table_metadata: dict[str, Any]) -> list[dict[str, Any]]:
    return list(table_metadata.get('unique_constraints') or []) + list(table_metadata.get('unique_indexes') or [])


def _schema_drift_connection_error_report(redacted_database_url: str, error: str) -> dict[str, Any]:
    return {
        'status': FAIL_STATUS,
        'ready': False,
        'summary': 'Runtime schema drift preflight could not inspect the configured database.',
        'database': {
            'connected': False,
            'url': redacted_database_url,
            'error': error,
        },
        'blocking_reasons': ['Database schema metadata could not be inspected.'],
        'warnings': [],
        'recommended_actions': [
            'Verify the configured database is reachable from apps/api and rerun the schema drift preflight.',
        ],
        'checks': [],
    }


def _build_schema_drift_report(
    *,
    redacted_database_url: str,
    existing_tables: Iterable[str],
    checks: list[dict[str, Any]],
    blocking_reasons: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    ready = not blocking_reasons
    status = OK_STATUS if ready and not warnings else WARN_STATUS if ready else FAIL_STATUS
    if ready:
        summary = 'Runtime-critical schema objects are present.'
        recommended_actions = [
            'Continue with the real write-path reproduction before debugging providers if runtime writes still fail.',
        ]
    else:
        summary = 'Runtime-critical schema drift was detected.'
        recommended_actions = [
            'Reproduce the real failing write path against the target database.',
            'Inspect the flagged table, primary key, unique constraint/index, and foreign-key metadata.',
            'Apply or repair the missing migration explicitly; do not mutate production schema automatically from this preflight.',
            'After schema parity is confirmed, rerun schema-drift-preflight before debugging provider payloads or credentials.',
        ]

    return {
        'status': status,
        'ready': ready,
        'summary': summary,
        'database': {
            'connected': True,
            'url': redacted_database_url,
            'error': None,
            'existing_table_count': len(set(existing_tables)),
        },
        'blocking_reasons': list(dict.fromkeys(blocking_reasons)),
        'warnings': list(dict.fromkeys(warnings)),
        'recommended_actions': recommended_actions,
        'checks': checks,
    }


def _inspect_runtime_schema_drift(
    inspector: Any,
    *,
    existing_tables: Iterable[str] | None = None,
    redacted_database_url: str | None = None,
) -> dict[str, Any]:
    database_url = redacted_database_url or _redacted_database_url()
    try:
        reflected_tables = sorted(existing_tables if existing_tables is not None else inspector.get_table_names())
    except Exception as exc:
        return _schema_drift_connection_error_report(database_url, str(exc))

    required_tables = _runtime_schema_required_tables()
    existing_table_set = set(reflected_tables)
    checks: list[dict[str, Any]] = []
    blocking_reasons: list[str] = []
    warnings: list[str] = []
    metadata_by_table: dict[str, dict[str, Any]] = {}

    for table in required_tables:
        exists = table in existing_table_set
        checks.append(
            {
                'name': f'{table}_table',
                'kind': 'table',
                'table': table,
                'ready': exists,
                'expected': {'table': table},
                'actual': {'exists': exists},
                'description': RUNTIME_SCHEMA_TABLE_DESCRIPTIONS.get(table, 'Required runtime table.'),
                'message': 'table exists' if exists else f'missing required table {table}',
            }
        )
        if not exists:
            blocking_reasons.append(f'Missing required table {table}.')
            continue
        metadata_by_table[table], table_warnings = _reflect_runtime_table(inspector, table)
        warnings.extend(table_warnings)

    for spec in RUNTIME_SCHEMA_PRIMARY_KEY_CHECKS:
        table = spec['table']
        expected_columns = tuple(spec['columns'])
        if table not in metadata_by_table:
            continue
        primary_key = metadata_by_table[table]['primary_key']
        ready = _column_sets_match(primary_key.get('columns') or [], expected_columns)
        checks.append(
            {
                'name': spec['name'],
                'kind': 'primary_key',
                'table': table,
                'ready': ready,
                'expected': {'columns': list(expected_columns)},
                'actual': primary_key,
                'description': spec['description'],
                'message': (
                    f'primary key covers {_format_columns(expected_columns)}'
                    if ready
                    else f'missing primary key over {table}{_format_columns(expected_columns)}'
                ),
            }
        )
        if not ready:
            blocking_reasons.append(f'Missing primary key over {table}{_format_columns(expected_columns)}.')

    for spec in RUNTIME_SCHEMA_UNIQUE_CHECKS:
        table = spec['table']
        expected_columns = tuple(spec['columns'])
        required_name = spec.get('required_name')
        if table not in metadata_by_table:
            continue

        candidates = _unique_candidates(metadata_by_table[table])
        column_matches = [candidate for candidate in candidates if _column_sets_match(candidate.get('columns') or [], expected_columns)]
        if required_name:
            matches = [candidate for candidate in column_matches if candidate.get('name') == required_name]
        else:
            matches = column_matches

        ready = bool(matches)
        check = {
            'name': spec['name'],
            'kind': 'unique',
            'table': table,
            'ready': ready,
            'expected': {
                'name': required_name,
                'columns': list(expected_columns),
                'name_required': bool(required_name),
            },
            'actual': {
                'matching_candidates': matches,
                'column_matching_candidates': column_matches,
                'unique_constraints': metadata_by_table[table].get('unique_constraints') or [],
                'unique_indexes': metadata_by_table[table].get('unique_indexes') or [],
            },
            'description': spec['description'],
            'message': (
                f'unique constraint/index present on {table}{_format_columns(expected_columns)}'
                if ready
                else f'missing required unique constraint/index on {table}{_format_columns(expected_columns)}'
            ),
        }
        checks.append(check)
        if not ready:
            if required_name:
                blocking_reasons.append(
                    f'Missing required unique constraint/index {required_name} on {table}{_format_columns(expected_columns)}.'
                )
            else:
                blocking_reasons.append(
                    f'Missing required unique constraint/index on {table}{_format_columns(expected_columns)}.'
                )

    for spec in RUNTIME_SCHEMA_FOREIGN_KEY_CHECKS:
        table = spec['table']
        expected_columns = tuple(spec['columns'])
        expected_referred_columns = tuple(spec['referred_columns'])
        expected_referred_table = spec['referred_table']
        if table not in metadata_by_table:
            continue
        if expected_referred_table not in existing_table_set:
            continue

        foreign_keys = metadata_by_table[table].get('foreign_keys') or []
        matches = [
            foreign_key
            for foreign_key in foreign_keys
            if _column_sets_match(foreign_key.get('columns') or [], expected_columns)
            and foreign_key.get('referred_table') == expected_referred_table
            and _column_sets_match(foreign_key.get('referred_columns') or [], expected_referred_columns)
        ]
        ready = bool(matches)
        checks.append(
            {
                'name': spec['name'],
                'kind': 'foreign_key',
                'table': table,
                'ready': ready,
                'expected': {
                    'columns': list(expected_columns),
                    'referred_table': expected_referred_table,
                    'referred_columns': list(expected_referred_columns),
                },
                'actual': {
                    'matching_candidates': matches,
                    'foreign_keys': foreign_keys,
                },
                'description': spec['description'],
                'message': (
                    f'foreign key present from {table}{_format_columns(expected_columns)} to '
                    f'{expected_referred_table}{_format_columns(expected_referred_columns)}'
                    if ready
                    else f'missing foreign key from {table}{_format_columns(expected_columns)} to '
                    f'{expected_referred_table}{_format_columns(expected_referred_columns)}'
                ),
            }
        )
        if not ready:
            blocking_reasons.append(
                f'Missing foreign key from {table}{_format_columns(expected_columns)} to '
                f'{expected_referred_table}{_format_columns(expected_referred_columns)}.'
            )

    return _build_schema_drift_report(
        redacted_database_url=database_url,
        existing_tables=reflected_tables,
        checks=checks,
        blocking_reasons=blocking_reasons,
        warnings=warnings,
    )


def _build_preflight_report(*, require_head: bool) -> dict[str, Any]:
    redacted_database_url = _redacted_database_url()
    market_snapshot_runtime = market_snapshot_service.runtime_status()
    portfolio_summary_runtime = portfolio_summary_service.runtime_status()

    try:
        head_revisions, revision_catalog = _load_revision_catalog()
        with engine.connect() as connection:
            inspector = sqlalchemy_inspect(connection)
            existing_tables = sorted(inspector.get_table_names())
            current_revisions = tuple(MigrationContext.configure(connection).get_current_heads())
            schema_drift_report = _inspect_runtime_schema_drift(
                inspector,
                existing_tables=existing_tables,
                redacted_database_url=redacted_database_url,
            )
    except Exception as exc:
        return {
            'status': FAIL_STATUS,
            'safe_to_continue': False,
            'require_head': require_head,
            'summary': 'Database migration preflight could not connect to the configured database.',
            'database': {
                'connected': False,
                'url': redacted_database_url,
                'error': str(exc),
            },
            'alembic': {
                'head_revisions': [],
                'current_revisions': [],
                'at_head': False,
                'pending_revision_count': 0,
                'pending_revisions': [],
            },
            'drift': {
                'status': 'unknown',
                'heuristic': 'Skipped because the database connection failed before Alembic state inspection.',
                'findings': [],
            },
            'schema_drift': _schema_drift_connection_error_report(redacted_database_url, str(exc)),
            'recommended_actions': [
                'Verify the configured database is reachable from apps/api and rerun the preflight.',
            ],
            'runtime_checks': {
                'market_snapshot': market_snapshot_runtime,
                'portfolio_summary': portfolio_summary_runtime,
            },
        }

    revision_ids = [item['revision'] for item in revision_catalog]
    head_revisions_list = list(head_revisions)
    current_revisions_list = list(current_revisions)
    at_head = bool(head_revisions) and tuple(current_revisions) == tuple(head_revisions)
    schema_drift_enforced = at_head or require_head
    schema_drift_report = {
        **schema_drift_report,
        'enforced': schema_drift_enforced,
    }

    pending_revisions: list[dict[str, Any]] = []
    drift_findings: list[dict[str, Any]] = []
    status = OK_STATUS
    summary = 'Database is already at the Alembic head revision.'
    recommended_actions = [
        'After restart, run the deploy smoke check against /health, /eod/freshness, /market/snapshot, and /portfolio/summary.',
    ]

    if len(head_revisions) != 1:
        status = FAIL_STATUS
        summary = 'Repository Alembic state has multiple heads; deployment should stop until the revision graph is linearized.'
        recommended_actions = [
            'Resolve the multiple Alembic heads in the repo before deploying this service.',
        ]
    elif len(current_revisions) > 1:
        status = FAIL_STATUS
        summary = 'Database is stamped to multiple Alembic revisions; deployment should stop until the version table is reconciled.'
        recommended_actions = [
            'Inspect the alembic_version table and reconcile it to a single linear revision before restarting the API.',
        ]
    elif current_revisions and current_revisions[0] not in revision_ids:
        status = FAIL_STATUS
        summary = 'Database is stamped to an Alembic revision that does not exist in this repo checkout.'
        recommended_actions = [
            'Verify the deployed code and the database revision history are from the same migration lineage before proceeding.',
        ]
    else:
        if not at_head:
            if current_revisions:
                current_index = revision_ids.index(current_revisions[0])
                pending_revisions = revision_catalog[current_index + 1 :]
            else:
                pending_revisions = revision_catalog

            existing_table_set = set(existing_tables)
            for revision in pending_revisions:
                if revision['duplicate_table_guarded']:
                    continue
                overlapping_tables = sorted(existing_table_set.intersection(revision['create_tables']))
                if overlapping_tables:
                    drift_findings.append(
                        {
                            'revision': revision['revision'],
                            'migration': revision['migration'],
                            'tables': overlapping_tables,
                        }
                    )

            if drift_findings:
                status = FAIL_STATUS
                summary = (
                    'Likely duplicate-table drift detected: pending unguarded create-table revisions already have '
                    'matching live tables.'
                )
                recommended_actions = [
                    'Stop automatic restart or migration retries for this deploy.',
                    'Inspect the flagged tables and confirm the live schema matches the pending migration file, including indexes and constraints.',
                    'Only after schema parity is confirmed, run alembic stamp <matching_revision>; do not stamp blindly.',
                    'If parity does not hold, add a corrective migration instead of dropping tables in place.',
                ]
            elif require_head:
                status = FAIL_STATUS
                summary = 'Database is not at the Alembic head revision.'
                recommended_actions = [
                    'Run alembic upgrade head before restart, then rerun this preflight with --require-head.',
                    'After restart, run the deploy smoke check against /health, /eod/freshness, /market/snapshot, and /portfolio/summary.',
                ]
            else:
                status = WARN_STATUS
                summary = (
                    'Database is behind the Alembic head revision, but no likely duplicate-table drift pattern was detected.'
                )
                recommended_actions = [
                    'If DB_AUTO_UPGRADE=true, startup may continue and let Alembic apply the pending revisions.',
                    'For a gated deploy, prefer alembic upgrade head before restart, then rerun this preflight with --require-head.',
                    'After restart, run the deploy smoke check against /health, /eod/freshness, /market/snapshot, and /portfolio/summary.',
                ]

    if not schema_drift_report['ready']:
        if not schema_drift_enforced and status != FAIL_STATUS:
            recommended_actions.append(
                'Runtime schema drift was reported before the database reached Alembic head; run alembic upgrade head, then rerun preflight --require-head.'
            )
        recommended_actions.extend(schema_drift_report['recommended_actions'])

    if schema_drift_enforced and not schema_drift_report['ready']:
        if status != FAIL_STATUS:
            status = FAIL_STATUS
            summary = 'Runtime schema drift preflight failed.'

    if not market_snapshot_runtime['ready']:
        if status != FAIL_STATUS:
            status = FAIL_STATUS
            summary = 'Market snapshot runtime is not ready for deploy.'
        recommended_actions.extend(_market_snapshot_runtime_actions(market_snapshot_runtime))
    if not portfolio_summary_runtime['ready']:
        if status != FAIL_STATUS:
            status = FAIL_STATUS
            summary = 'Portfolio summary runtime is not ready for deploy.'
        recommended_actions.extend(_portfolio_summary_runtime_actions(portfolio_summary_runtime))

    return {
        'status': status,
        'safe_to_continue': status != FAIL_STATUS,
        'require_head': require_head,
        'summary': summary,
        'database': {
            'connected': True,
            'url': redacted_database_url,
            'error': None,
            'existing_table_count': len(existing_tables),
        },
        'alembic': {
            'head_revisions': head_revisions_list,
            'current_revisions': current_revisions_list,
            'at_head': at_head,
            'pending_revision_count': len(pending_revisions),
            'pending_revisions': pending_revisions,
        },
        'drift': {
            'status': 'likely_duplicate_table_drift' if drift_findings else 'none',
            'heuristic': (
                'Flags tables that already exist even though they are still created by a pending migration file '
                'without an explicit duplicate-table guard.'
            ),
            'findings': drift_findings,
        },
        'schema_drift': schema_drift_report,
        'recommended_actions': list(dict.fromkeys(recommended_actions)),
        'runtime_checks': {
            'market_snapshot': market_snapshot_runtime,
            'portfolio_summary': portfolio_summary_runtime,
        },
    }


def _render_preflight_text(report: dict[str, Any]) -> str:
    lines = [f"{report['status'].upper()} migration preflight: {report['summary']}"]
    lines.append(f"Database: {report['database']['url']}")

    current_revisions = report['alembic']['current_revisions']
    head_revisions = report['alembic']['head_revisions']
    lines.append(f"Current revision(s): {', '.join(current_revisions) if current_revisions else '<unversioned>'}")
    lines.append(f"Head revision(s): {', '.join(head_revisions) if head_revisions else '<unknown>'}")

    pending_revisions = report['alembic']['pending_revisions']
    if pending_revisions:
        lines.append(f"Pending revisions: {report['alembic']['pending_revision_count']}")
        for revision in pending_revisions[:12]:
            table_note = ''
            if revision['create_tables']:
                table_note = f" creates tables [{', '.join(revision['create_tables'])}]"
            guard_note = ' guarded' if revision['duplicate_table_guarded'] else ' unguarded'
            lines.append(f"- {revision['revision']} ({revision['migration']}){table_note}; duplicate-table check:{guard_note}")
        remaining = len(pending_revisions) - 12
        if remaining > 0:
            lines.append(f"- ... {remaining} additional pending revision(s) omitted")

    if report['drift']['findings']:
        lines.append('Likely duplicate-table drift:')
        for finding in report['drift']['findings']:
            tables = ', '.join(finding['tables'])
            lines.append(f"- {finding['revision']} ({finding['migration']}): live table(s) already present -> {tables}")

    schema_drift = report.get('schema_drift')
    if isinstance(schema_drift, dict):
        lines.append(
            'Runtime schema drift: '
            + ('ready' if schema_drift.get('ready') else 'blocked')
            + f" (enforced={schema_drift.get('enforced', True)}, {schema_drift.get('summary')})"
        )
        for reason in (schema_drift.get('blocking_reasons') or [])[:12]:
            lines.append(f"- schema_drift: {reason}")
        remaining_reasons = len(schema_drift.get('blocking_reasons') or []) - 12
        if remaining_reasons > 0:
            lines.append(f"- schema_drift: ... {remaining_reasons} additional blocking reason(s) omitted")
        for warning in (schema_drift.get('warnings') or [])[:6]:
            lines.append(f"- schema_drift warning: {warning}")

    runtime_checks = report.get('runtime_checks') or {}
    market_snapshot = runtime_checks.get('market_snapshot')
    if isinstance(market_snapshot, dict):
        lines.append(
            'Market snapshot runtime: '
            + ('ready' if market_snapshot.get('ready') else 'blocked')
            + f" (effective_kis_environment={market_snapshot.get('effective_kis_environment')}, "
            + f"uses_real_domain_override={market_snapshot.get('uses_real_domain_override')})"
        )
        for reason in market_snapshot.get('blocking_reasons') or []:
            lines.append(f"- market_snapshot: {reason}")
    portfolio_summary = runtime_checks.get('portfolio_summary')
    if isinstance(portfolio_summary, dict):
        lines.append(
            'Portfolio summary runtime: '
            + ('ready' if portfolio_summary.get('ready') else 'blocked')
            + f" (kis_environment={portfolio_summary.get('kis_environment')}, "
            + f"configured_account_count={portfolio_summary.get('configured_account_count')}, "
            + f"kis_use_mock={portfolio_summary.get('kis_use_mock')})"
        )
        for reason in portfolio_summary.get('blocking_reasons') or []:
            lines.append(f"- portfolio_summary: {reason}")

    if report['database'].get('error'):
        lines.append(f"Database error: {report['database']['error']}")

    if report['recommended_actions']:
        lines.append('Recommended actions:')
        for action in report['recommended_actions']:
            lines.append(f"- {action}")

    return '\n'.join(lines)


def collect_schema_drift_preflight() -> tuple[dict[str, Any], int]:
    redacted_database_url = _redacted_database_url()
    try:
        with engine.connect() as connection:
            inspector = sqlalchemy_inspect(connection)
            existing_tables = sorted(inspector.get_table_names())
            report = _inspect_runtime_schema_drift(
                inspector,
                existing_tables=existing_tables,
                redacted_database_url=redacted_database_url,
            )
    except Exception as exc:
        report = _schema_drift_connection_error_report(redacted_database_url, str(exc))
    return report, 0 if report['ready'] else 1


def _render_schema_drift_text(report: dict[str, Any]) -> str:
    lines = [f"{report['status'].upper()} schema drift preflight: {report['summary']}"]
    lines.append(f"Database: {report['database']['url']}")
    lines.append(f"Ready: {report['ready']}")

    checks = report.get('checks') or []
    failed_checks = [check for check in checks if not check.get('ready')]
    lines.append(f"Checks: {len(checks)} total, {len(failed_checks)} blocking")

    if report.get('blocking_reasons'):
        lines.append('Blocking reasons:')
        for reason in report['blocking_reasons']:
            lines.append(f"- {reason}")

    if failed_checks:
        lines.append('Failed check details:')
        for check in failed_checks[:20]:
            expected = check.get('expected') or {}
            expected_name = expected.get('name')
            expected_columns = expected.get('columns')
            expected_detail = ''
            if expected_name:
                expected_detail += f" name={expected_name}"
            if expected_columns:
                expected_detail += f" columns={expected_columns}"
            if expected.get('referred_table'):
                expected_detail += f" references={expected.get('referred_table')}({expected.get('referred_columns')})"
            lines.append(f"- {check.get('kind')} {check.get('table')}.{check.get('name')}:{expected_detail} -> {check.get('message')}")
        remaining = len(failed_checks) - 20
        if remaining > 0:
            lines.append(f"- ... {remaining} additional failed check(s) omitted")

    if report.get('warnings'):
        lines.append('Warnings:')
        for warning in report['warnings']:
            lines.append(f"- {warning}")

    if report.get('recommended_actions'):
        lines.append('Recommended actions:')
        for action in report['recommended_actions']:
            lines.append(f"- {action}")

    return '\n'.join(lines)


def collect_migration_preflight(*, require_head: bool = False) -> tuple[dict[str, Any], int]:
    report = _build_preflight_report(require_head=require_head)
    exit_code = 0 if report['safe_to_continue'] else 1
    if report['drift']['status'] == 'likely_duplicate_table_drift':
        exit_code = 2
    return report, exit_code


def collect_deploy_smoke_check(
    *,
    base_url: str = 'http://127.0.0.1:8000',
    timeout_seconds: float = 10.0,
    transport: httpx.BaseTransport | None = None,
) -> tuple[dict[str, Any], int]:
    headers = {'Accept': 'application/json'}
    if settings.upstream_bearer_token:
        headers['Authorization'] = f'Bearer {settings.upstream_bearer_token}'

    checks: dict[str, Any] = {}
    status = OK_STATUS
    summary = 'Deploy smoke checks passed.'

    with httpx.Client(
        base_url=base_url,
        timeout=timeout_seconds,
        headers=headers,
        transport=transport,
    ) as client:
        for name, path in (
            ('health', '/health'),
            ('eod_freshness', '/eod/freshness'),
            ('market_snapshot', '/market/snapshot'),
            ('portfolio_summary', '/portfolio/summary'),
        ):
            try:
                response = client.get(path)
                payload = response.json()
            except Exception as exc:
                checks[name] = {
                    'ok': False,
                    'path': path,
                    'error': str(exc),
                }
                status = FAIL_STATUS
                summary = 'Deploy smoke checks failed.'
                continue

            if name == 'health':
                app_status = payload.get('status')
                database_connected = bool((payload.get('database') or {}).get('connected'))
                ok = response.status_code == 200 and app_status == 'ok' and database_connected
                checks[name] = {
                    'ok': ok,
                    'path': path,
                    'status_code': response.status_code,
                    'app_status': app_status,
                    'database_connected': database_connected,
                }
            elif name == 'market_snapshot':
                markets = payload.get('markets') if isinstance(payload.get('markets'), dict) else {}
                kospi = markets.get('kospi') if isinstance(markets.get('kospi'), dict) else {}
                kosdaq = markets.get('kosdaq') if isinstance(markets.get('kosdaq'), dict) else {}
                usd_krw = payload.get('usd_krw') if isinstance(payload.get('usd_krw'), dict) else {}
                missing_fields = payload.get('missing_fields') if isinstance(payload.get('missing_fields'), list) else []
                ok = (
                    response.status_code == 200
                    and bool(payload.get('as_of'))
                    and kospi.get('value') is not None
                    and kosdaq.get('value') is not None
                )
                checks[name] = {
                    'ok': ok,
                    'path': path,
                    'status_code': response.status_code,
                    'as_of': payload.get('as_of'),
                    'snapshot_status': payload.get('status'),
                    'usd_krw_rate': usd_krw.get('rate'),
                    'kospi_value': kospi.get('value'),
                    'kosdaq_value': kosdaq.get('value'),
                    'missing_fields': missing_fields,
                    'stale_reason': payload.get('stale_reason'),
                    'detail': payload.get('detail'),
                }
            elif name == 'portfolio_summary':
                sync = payload.get('sync') if isinstance(payload.get('sync'), dict) else {}
                accounts = payload.get('accounts') if isinstance(payload.get('accounts'), list) else []
                sync_status = str(sync.get('status') or '').strip()
                ok = (
                    response.status_code == 200
                    and sync_status in {'synced', 'stale', 'unavailable'}
                    and len(accounts) > 0
                )
                checks[name] = {
                    'ok': ok,
                    'path': path,
                    'status_code': response.status_code,
                    'sync_status': sync_status,
                    'accounts_count': len(accounts),
                    'detail': sync.get('message'),
                }
            else:
                today = payload.get('today') if isinstance(payload.get('today'), dict) else {}
                latest = payload.get('latest') if isinstance(payload.get('latest'), dict) else {}
                ok = response.status_code == 200 and bool(today.get('trade_date'))
                checks[name] = {
                    'ok': ok,
                    'path': path,
                    'status_code': response.status_code,
                    'today_trade_date': today.get('trade_date'),
                    'latest_trade_date': latest.get('trade_date') or latest.get('price_trade_date'),
                    'latest_updated_at': latest.get('updated_at'),
                }

            if not checks[name]['ok']:
                status = FAIL_STATUS
                summary = 'Deploy smoke checks failed.'

    report = {
        'status': status,
        'base_url': base_url,
        'summary': summary,
        'checks': checks,
    }
    return report, 0 if status == OK_STATUS else 1


def _render_smoke_check_text(report: dict[str, Any]) -> str:
    lines = [f"{report['status'].upper()} deploy smoke check: {report['summary']}"]
    lines.append(f"Base URL: {report['base_url']}")
    for name, check in report['checks'].items():
        detail_parts = [f"path={check['path']}"]
        if 'status_code' in check:
            detail_parts.append(f"status_code={check['status_code']}")
        if name == 'health':
            detail_parts.append(f"app_status={check.get('app_status')}")
            detail_parts.append(f"database_connected={check.get('database_connected')}")
        elif name == 'market_snapshot':
            detail_parts.append(f"as_of={check.get('as_of')}")
            detail_parts.append(f"snapshot_status={check.get('snapshot_status')}")
            detail_parts.append(f"usd_krw_rate={check.get('usd_krw_rate')}")
            detail_parts.append(f"kospi_value={check.get('kospi_value')}")
            detail_parts.append(f"kosdaq_value={check.get('kosdaq_value')}")
            if check.get('missing_fields'):
                detail_parts.append(f"missing_fields={check.get('missing_fields')}")
            if check.get('stale_reason'):
                detail_parts.append(f"stale_reason={check.get('stale_reason')}")
        elif name == 'portfolio_summary':
            detail_parts.append(f"sync_status={check.get('sync_status')}")
            detail_parts.append(f"accounts_count={check.get('accounts_count')}")
        else:
            detail_parts.append(f"today_trade_date={check.get('today_trade_date')}")
            detail_parts.append(f"latest_trade_date={check.get('latest_trade_date')}")
        if check.get('detail'):
            detail_parts.append(f"detail={check['detail']}")
        if check.get('error'):
            detail_parts.append(f"error={check['error']}")
        lines.append(f"- {name}: {'ok' if check['ok'] else 'fail'} ({', '.join(detail_parts)})")
    return '\n'.join(lines)


def _emit_report(report: dict[str, Any], *, output_format: str, renderer) -> None:
    if output_format == 'text':
        print(renderer(report))
        return
    print(json.dumps(report, ensure_ascii=False))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Deployment hygiene helpers for kai-trading-core.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    preflight_parser = subparsers.add_parser(
        'preflight',
        help='Inspect Alembic current/head state and flag likely duplicate-table drift before deploy or restart.',
    )
    preflight_parser.add_argument('--require-head', action='store_true')
    preflight_parser.add_argument('--format', choices=('json', 'text'), default='json')

    schema_drift_parser = subparsers.add_parser(
        'schema-drift-preflight',
        help='Verify runtime-critical tables, primary keys, unique constraints/indexes, and foreign keys.',
    )
    schema_drift_parser.add_argument('--format', choices=('json', 'text'), default='json')

    smoke_parser = subparsers.add_parser(
        'smoke-check',
        help='Hit /health, /eod/freshness, and /market/snapshot after restart and fail if the service does not look healthy.',
    )
    smoke_parser.add_argument('--base-url', default='http://127.0.0.1:8000')
    smoke_parser.add_argument('--timeout-seconds', type=float, default=10.0)
    smoke_parser.add_argument('--format', choices=('json', 'text'), default='json')
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == 'preflight':
        report, exit_code = collect_migration_preflight(require_head=args.require_head)
        _emit_report(report, output_format=args.format, renderer=_render_preflight_text)
        return exit_code

    if args.command == 'schema-drift-preflight':
        report, exit_code = collect_schema_drift_preflight()
        _emit_report(report, output_format=args.format, renderer=_render_schema_drift_text)
        return exit_code

    if args.command == 'smoke-check':
        report, exit_code = collect_deploy_smoke_check(
            base_url=args.base_url,
            timeout_seconds=args.timeout_seconds,
        )
        _emit_report(report, output_format=args.format, renderer=_render_smoke_check_text)
        return exit_code

    parser.error(f'Unsupported command: {args.command}')


if __name__ == '__main__':
    raise SystemExit(main())
