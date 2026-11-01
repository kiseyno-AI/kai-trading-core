from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Mapping, Sequence

from app.eod.daily_pipeline_outcomes import (
    daily_pipeline_nonblocking_warning_stage_ids,
    daily_pipeline_outcome_is_satisfied,
    daily_pipeline_result_has_no_ready_content,
    daily_pipeline_warning_metadata,
    daily_pipeline_zero_scope_gate_status,
    normalize_daily_pipeline_stage_status,
)
from app.eod.pykrx_enrichment import DEFAULT_PYKRX_ENRICHMENT_CATEGORIES
from app.eod.pykrx_enrichment_gate import (
    REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
    build_pykrx_enrichment_preflight,
)
from app.eod.market_membership import (
    build_market_membership_snapshot,
    market_membership_sha256,
    validate_market_membership_snapshot,
)
from app.eod.pykrx_pipeline_support import (
    append_stage_error,
    build_stage_failure_result,
    make_stage_progress_callback,
)
from app.eod.run_stage_support import (
    build_expired_decision_replay_result,
    build_next_open_window_expired_result,
    extract_next_open_plan_stage_counts,
)
from app.eod.scope_outcomes import (
    EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS,
    build_exact_symbol_scope_outcome,
)
from app.eod.stage_heartbeat import PeriodicStageHeartbeat
from app.market.clock import get_market_state
from app.market.session import previous_krx_trading_date
from app.market_data_store import (
    build_daily_derived_market_scope_authority,
)
from app.signals.market_risk_filter import MARKET_RISK_FILTER_SOURCE

logger = logging.getLogger(__name__)

PYKRX_DAILY_STAGE_ORDER: tuple[str, ...] = (
    'price_collection',
    'technical_snapshots',
    'action_summaries',
    'trending_ranks',
    'signal_features',
    'market_risk_filter',
)

# Historical execution is intentionally narrower than the current-day pipeline.
# The remaining daily stages either depend on today's active universe or call
# current-only KIS endpoints, so allowing them to stamp a past market date would
# introduce survivorship or look-ahead bias into replay inputs.
HISTORICAL_BACKFILL_STAGE_ORDER: tuple[str, ...] = (
    'price_collection',
    'technical_snapshots',
    'action_summaries',
)

FACTOR_FOLLOWUP_STAGE_ORDER: tuple[str, ...] = (
    'momentum_factors',
    'value_factors',
    'low_risk_factors',
    'beta_factors',
    'advanced_value_factors',
    'unified_value_factors',
    'quality_factors',
    'composite_factors',
)
AUTO_DAILY_FOLLOWUP_STAGE_ORDER: tuple[str, ...] = (
    'fundamentals_daily_refresh',
    'piotroski_filter',
    *FACTOR_FOLLOWUP_STAGE_ORDER,
    'signal_features',
    'external_news_collect',
    'external_news_enrich',
    'macro_context_refresh',
    'agent_feedback_summaries',
    'signal_decider',
    'next_open_execution_plan',
    'signal_decider_evaluations',
)
AUTO_DAILY_TRACKED_STAGE_ORDER: tuple[str, ...] = (
    'price_collection',
    'pykrx_enrichment',
    'technical_snapshots',
    'action_summaries',
    'trending_ranks',
    'market_risk_filter',
    *AUTO_DAILY_FOLLOWUP_STAGE_ORDER,
)
MAX_NON_BLOCKING_STAGE_MISSING_RATIO = 0.10
PYKRX_PROVIDER_OUTAGE_EARLY_ABORT_FAILURES = 20
FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS = 60.0
DAILY_PIPELINE_TRACKER_FINISH_ATTEMPTS = 3
POINT_IN_TIME_RECOVERY_CURRENT_ONLY_STAGE_IDS: frozenset[str] = frozenset(
    {
        'master_refresh',
        'pykrx_universe_refresh',
        'signal_features',
        'market_risk_snapshots',
        'market_risk_filter',
        'fundamentals_daily_refresh',
        'external_news_collect',
        'external_news_enrich',
        'macro_context_refresh',
        'intraday_snapshots',
    }
)
POINT_IN_TIME_RECOVERY_BOUNDARY_TRIGGERS: frozenset[str] = frozenset(
    {
        'auto_close',
        'auto_close_scheduler',
        'auto_eod_followup_resume',
        'internal_ops_cli',
        'internal_ops_cli_stage',
        'manual_api',
        'manual_api_stage',
        'manual_daily_api',
    }
)
PREOPEN_PREVIOUS_SESSION_CURRENT_ONLY_STAGE_IDS: frozenset[str] = frozenset(
    {
        'market_risk_snapshots',
        'market_risk_filter',
    }
)

_PYKRX_PROVIDER_FAILURE_MESSAGE_MARKERS: tuple[str, ...] = (
    'client is unavailable',
    'connection aborted',
    'connection refused',
    'connection reset',
    'provider unavailable',
    'read timed out',
    'remote disconnected',
    'service unavailable',
    'temporarily unavailable',
    'timed out',
    'timeout',
    'too many requests',
)


def _is_pykrx_provider_failure(exc: BaseException) -> bool:
    """Identify transport/provider failures without treating bad symbol data as outage."""

    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (TimeoutError, ConnectionError)):
            return True
        error_type = type(current).__name__.lower()
        message = str(current).lower()
        if any(token in error_type for token in ('timeout', 'connection', 'transport')):
            return True
        if any(marker in message for marker in _PYKRX_PROVIDER_FAILURE_MESSAGE_MARKERS):
            return True
        current = current.__cause__ or current.__context__
    return False


def _parse_market_date(value: str | date) -> date:
    normalized = str(value).strip()
    if len(normalized) == 8 and normalized.isdigit():
        return datetime.strptime(normalized, '%Y%m%d').date()
    return date.fromisoformat(normalized[:10])


def should_skip_point_in_time_current_only_stage(
    *,
    stage_id: str,
    market_date: str | date,
    market: Any | None = None,
    current_market_date: str | date | None = None,
    previous_trading_date: str | date | None = None,
) -> bool:
    """Allow only the immediately previous KRX session before today's open."""

    if stage_id not in POINT_IN_TIME_RECOVERY_CURRENT_ONLY_STAGE_IDS:
        return False
    if stage_id not in PREOPEN_PREVIOUS_SESSION_CURRENT_ONLY_STAGE_IDS:
        return True

    resolved_market = market if market is not None else get_market_state()
    if (
        str(getattr(resolved_market, 'state', '') or '').upper() != 'PREOPEN'
        or getattr(resolved_market, 'is_trading_day', None) is False
        or getattr(resolved_market, 'is_open', None) is True
    ):
        return True

    if current_market_date is None:
        current_market_date = (
            getattr(resolved_market, 'current_calendar_date', None)
            or str(getattr(resolved_market, 'server_time', ''))[:10]
        )
    current_date = _parse_market_date(current_market_date)
    target_date = _parse_market_date(market_date)
    if target_date >= current_date:
        return True

    resolved_previous_trading_date = previous_trading_date
    if resolved_previous_trading_date is None:
        try:
            resolved_previous_trading_date = previous_krx_trading_date(
                current_date,
                allow_fallback=True,
            )
        except Exception:
            return True
    if resolved_previous_trading_date is None:
        return True
    return target_date != _parse_market_date(resolved_previous_trading_date)


def resolve_point_in_time_recovery(
    *,
    market_date: str,
    requested: bool = False,
    recovery_context: bool = False,
    market: Any | None = None,
    current_market_date: date | str | None = None,
) -> bool:
    """Fail closed when a daily boundary is asked to write a prior KRX date."""

    if requested:
        return True
    if not recovery_context:
        return False
    if current_market_date is None:
        resolved_market = market if market is not None else get_market_state()
        current_market_date = str(resolved_market.server_time)[:10]
    else:
        current_market_date = str(current_market_date)[:10]
    return date.fromisoformat(market_date) < date.fromisoformat(
        current_market_date
    )


def _build_point_in_time_current_only_skip(
    *,
    stage_id: str,
    market_date: str,
) -> dict[str, Any]:
    return {
        'triggered': False,
        'skipped': True,
        'safe_skip': True,
        'status': 'skipped',
        'reason': 'point_in_time_recovery_current_only_stage',
        'market_date': market_date,
        'stage': stage_id,
    }


@dataclass(frozen=True)
class PykrxPipelineDependencies:
    log_fetch: Callable[..., None]
    create_eod_collection_run: Callable[..., int]
    update_eod_collection_run_progress: Callable[..., None]
    complete_eod_collection_run: Callable[..., None]
    persist_daily_price_bars: Callable[..., int]
    get_market_ohlcv_by_date: Callable[..., Any]
    get_market_ohlcv_by_ticker: Callable[..., Any] | None
    price_context_service: Any
    technical_snapshot_service: Any
    action_summary_service: Any
    compute_trending_ranks: Callable[..., dict]
    pykrx_enrichment_service: Any
    signal_feature_service: Any
    market_risk_filter_service: Any
    dart_fundamentals_capture_service: Any
    settings: Any
    normalize_yyyymmdd: Callable[..., str]
    update_run_stage_metadata: Callable[..., dict]
    daily_pipeline_run_state_service: Any | None = None


def _normalize_stage_status(value: Any) -> str:
    status = str(value or '').strip().lower()
    if status == 'ok':
        return 'completed'
    if status in {'completed_with_errors', 'warning'}:
        return 'partial'
    if status in {'disabled', 'not_configured'}:
        return 'skipped'
    return status or 'completed'


def _status_activity_prefix(status: str) -> str:
    if status == 'completed':
        return 'Completed'
    if status == 'partial':
        return 'Completed with errors'
    if status == 'failed':
        return 'Failed'
    if status == 'waiting':
        return 'Waiting on'
    if status == 'blocked':
        return 'Blocked'
    if status == 'skipped':
        return 'Skipped'
    return status.replace('_', ' ').capitalize()


def _missing_ratio(*, failed: int, total: int) -> float | None:
    if total <= 0:
        return None
    return max(failed, 0) / total


def _merge_inherited_downstream_scope(
    *,
    requested_symbols: Sequence[str],
    inherited_symbols: Sequence[str],
    current_price_failure_symbols: Sequence[str],
    current_failure_symbols: Sequence[str],
    preserve_deferred_followup_scope: bool,
) -> tuple[list[str], list[str]]:
    requested_symbol_set = set(requested_symbols)
    pending_inherited_symbols = [
        symbol
        for symbol in dict.fromkeys(inherited_symbols)
        if symbol not in requested_symbol_set
    ]
    if not pending_inherited_symbols and not current_failure_symbols:
        return [], []

    # Current downstream collectors already expand shared/global failures to
    # the whole invocation in their retry metadata.  Broadening every exact
    # per-symbol failure to all newly collected symbols here would erase the
    # durable success partition and turn a 1/N persistence error into N/N
    # retry debt.  Keep only inherited debt and identities that actually
    # failed in this attempt; admission still fails closed later when a stage
    # lacks an exhaustive exact-scope manifest.
    _ = current_price_failure_symbols, preserve_deferred_followup_scope
    return pending_inherited_symbols, list(
        dict.fromkeys(
            [
                *pending_inherited_symbols,
                *current_failure_symbols,
            ]
        )
    )


def _is_non_blocking_gap(*, failed: int, total: int) -> bool:
    ratio = _missing_ratio(failed=failed, total=total)
    return failed > 0 and ratio is not None and ratio < MAX_NON_BLOCKING_STAGE_MISSING_RATIO


def _completion_status_for_gap(
    *,
    failed: int,
    total: int,
    treat_minor_gap_as_completed: bool = False,
) -> str:
    if failed <= 0:
        return 'completed'
    if treat_minor_gap_as_completed and _is_non_blocking_gap(
        failed=failed,
        total=total,
    ):
        return 'completed'
    return 'partial'


def _gap_policy_details(
    *,
    failed: int,
    total: int,
    treat_minor_gap_as_completed: bool = False,
) -> dict[str, Any]:
    ratio = _missing_ratio(failed=failed, total=total)
    is_minor_missing_gap = _is_non_blocking_gap(failed=failed, total=total)
    return {
        'max_non_blocking_missing_ratio': MAX_NON_BLOCKING_STAGE_MISSING_RATIO,
        'missing_ratio': round(ratio, 6) if ratio is not None else None,
        'is_minor_missing_gap': is_minor_missing_gap,
        'minor_missing_gap_treated_as_success': (
            treat_minor_gap_as_completed and is_minor_missing_gap
        ),
    }


def _bounded_failure_diagnostics(
    result: dict[str, Any],
    *,
    limit: int = 50,
    error_length_limit: int = 1000,
) -> dict[str, Any]:
    failed_symbols = [
        str(symbol)
        for symbol in (result.get('failed_symbols') or [])
        if str(symbol).strip()
    ][:limit]
    failed_symbol_errors: list[dict[str, str]] = []
    for item in result.get('failed_symbol_errors') or []:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get('symbol') or '').strip()
        error = str(item.get('error') or '').strip()
        if not symbol and not error:
            continue
        failed_symbol_errors.append(
            {
                'symbol': symbol,
                'error': error[:error_length_limit],
            }
        )
        if len(failed_symbol_errors) >= limit:
            break
    return {
        'failed_symbols': failed_symbols,
        'failed_symbol_errors': failed_symbol_errors,
    }


def _classify_pykrx_enrichment_categories(
    *,
    category_results: dict[str, Any],
    coverage_categories: dict[str, Any],
    preflight_failed_categories: Sequence[str],
) -> dict[str, list[str]]:
    provider_error_categories = sorted(
        category
        for category, details in category_results.items()
        if isinstance(details, dict) and details.get('errors')
    )
    coverage_gap_categories = sorted(
        category
        for category, coverage_payload in coverage_categories.items()
        if isinstance(coverage_payload, dict)
        and int(coverage_payload.get('expected_rows') or 0)
        > int(coverage_payload.get('present_rows') or 0)
    )
    component_retry_debt_categories = sorted(
        category
        for category, details in category_results.items()
        if isinstance(details, dict)
        and details.get('component_retry_debt')
    )
    unresolved_categories = sorted(
        {
            *coverage_gap_categories,
            *preflight_failed_categories,
            *component_retry_debt_categories,
        }
    )
    recovered_categories = sorted(
        set(provider_error_categories) - set(unresolved_categories)
    )
    return {
        'provider_error_categories': provider_error_categories,
        'coverage_gap_categories': coverage_gap_categories,
        'component_retry_debt_categories': (
            component_retry_debt_categories
        ),
        'recovered_categories': recovered_categories,
        'unresolved_categories': unresolved_categories,
    }


def _pykrx_component_retry_symbols(
    *,
    result: dict[str, Any],
    requested_symbols: Sequence[str],
) -> list[str]:
    affected = {
        str(symbol).strip()
        for debt in result.get('component_retry_debt') or []
        if isinstance(debt, dict)
        for symbol in (
            debt.get('affected_symbols')
            or debt.get('affected_symbols_preview')
            or []
        )
        if str(symbol).strip()
    }
    return [
        str(symbol).strip()
        for symbol in requested_symbols
        if str(symbol).strip() in affected
    ]


def _partition_pykrx_enrichment_category_gaps(
    *,
    failed_categories: Sequence[str],
    preflight: dict[str, Any],
) -> tuple[list[str], list[str]]:
    required_categories = {
        str(category)
        for category in preflight.get('required_categories') or []
    }
    category_gates = {
        str(item.get('category')): item
        for item in preflight.get('category_gates') or []
        if isinstance(item, dict) and item.get('category')
    }
    blocking: list[str] = []
    tolerated: list[str] = []
    for category in failed_categories:
        if category not in required_categories:
            tolerated.append(category)
            continue
        gate = category_gates.get(category)
        if gate is None:
            blocking.append(category)
            continue
        if gate.get('gate_status') in {'ready', 'not_applicable'}:
            tolerated.append(category)
        else:
            blocking.append(category)
    return blocking, tolerated


def _resolve_requested_stages(
    *,
    mode: str,
    selected_stages: Sequence[str] | None,
) -> tuple[str, ...]:
    historical_backfill = mode == 'historical_backfill'
    defaults = HISTORICAL_BACKFILL_STAGE_ORDER if historical_backfill else PYKRX_DAILY_STAGE_ORDER
    raw_stages = defaults if selected_stages is None else selected_stages
    requested = tuple(
        dict.fromkeys(
            str(stage_id or '').strip()
            for stage_id in raw_stages
            if str(stage_id or '').strip()
        )
    )
    if not requested:
        raise ValueError('At least one pykrx stage must be requested.')
    if historical_backfill:
        unsupported = sorted(set(requested) - set(HISTORICAL_BACKFILL_STAGE_ORDER))
        if unsupported:
            raise ValueError(
                'historical_backfill stages are restricted to point-in-time-safe stages: '
                f'{",".join(HISTORICAL_BACKFILL_STAGE_ORDER)}; unsupported={",".join(unsupported)}'
            )
    if 'price_collection' not in requested:
        raise ValueError('price_collection stage is required for pykrx pipeline execution')
    if 'action_summaries' in requested and 'technical_snapshots' not in requested:
        raise ValueError('technical_snapshots is required when action_summaries is requested')
    return requested


def _consume_retry_failed_symbols(result: dict[str, Any]) -> list[str]:
    """Read full internal retry scope while keeping public failure previews bounded."""

    retry_metadata = result.pop('_retry_metadata', None)
    raw_symbols = (
        retry_metadata.get('failed_symbols')
        if isinstance(retry_metadata, dict)
        else result.get('failed_symbols')
    )
    return list(
        dict.fromkeys(
            str(symbol).strip()
            for symbol in (raw_symbols or [])
            if str(symbol).strip()
        )
    )


def _peek_retry_failed_symbols(result: dict[str, Any]) -> list[str]:
    """Read an exact retry scope without consuming its internal metadata."""

    retry_metadata = result.get('_retry_metadata')
    raw_symbols = (
        retry_metadata.get('failed_symbols')
        if isinstance(retry_metadata, dict)
        else result.get('failed_symbols')
    )
    return list(
        dict.fromkeys(
            str(symbol).strip()
            for symbol in (raw_symbols or [])
            if str(symbol).strip()
        )
    )


def _build_exact_scope_outcome(
    *,
    selected_symbols: Sequence[str],
    successful_symbols: Sequence[str],
    retry_debt_symbols: Sequence[str],
    manifest_version: str = 'pykrx_enrichment_scope_outcomes_v1',
) -> dict[str, Any]:
    return build_exact_symbol_scope_outcome(
        manifest_version=manifest_version,
        selected_symbols=selected_symbols,
        successful_symbols=successful_symbols,
        retry_debt_symbols=retry_debt_symbols,
    )


_EXACT_SCOPE_OUTCOME_KEYS = (
    'scope_outcome_manifest_version',
    'selected_stock_codes',
    'successful_stock_codes',
    'retry_debt_stock_codes',
    'terminal_not_applicable_stock_codes',
    'scope_outcome_manifest',
)


def _validated_exact_scope_outcome(
    *,
    result: Mapping[str, Any],
    invocation_symbols: Sequence[str],
    status: str | None = None,
) -> dict[str, Any] | None:
    """Validate a per-symbol outcome partition against its invocation.

    A partial result is safe to narrow only when every invoked symbol has one
    unambiguous terminal outcome.  This deliberately validates the persisted
    manifest instead of inferring success from counts or stale same-day rows.
    """

    normalized_status = str(
        status if status is not None else result.get('status') or ''
    ).strip().lower()
    if normalized_status not in {'completed', 'partial'}:
        return None
    version = str(
        result.get('scope_outcome_manifest_version') or ''
    ).strip()
    if version not in EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS:
        return None

    def normalize_list(key: str, *, optional: bool = False) -> list[str] | None:
        raw = result.get(key)
        if optional and raw is None:
            return []
        if not isinstance(raw, list):
            return None
        normalized = [
            (
                str(value or '').strip().zfill(6)
                if str(value or '').strip().isdigit()
                else str(value or '').strip().upper()
            )
            for value in raw
        ]
        if any(not value for value in normalized):
            return None
        if len(normalized) != len(set(normalized)):
            return None
        return normalized

    selected = normalize_list('selected_stock_codes')
    successful = normalize_list('successful_stock_codes')
    retry_debt = normalize_list('retry_debt_stock_codes')
    terminal_not_applicable = normalize_list(
        'terminal_not_applicable_stock_codes',
        optional=True,
    )
    if (
        selected is None
        or successful is None
        or retry_debt is None
        or terminal_not_applicable is None
    ):
        return None

    normalized_invocation = [
        (
            str(value or '').strip().zfill(6)
            if str(value or '').strip().isdigit()
            else str(value or '').strip().upper()
        )
        for value in invocation_symbols
    ]
    if (
        any(not value for value in normalized_invocation)
        or len(normalized_invocation) != len(set(normalized_invocation))
        or set(selected) != set(normalized_invocation)
    ):
        return None

    selected_set = set(selected)
    successful_set = set(successful)
    retry_debt_set = set(retry_debt)
    terminal_set = set(terminal_not_applicable)
    if (
        successful_set & retry_debt_set
        or successful_set & terminal_set
        or retry_debt_set & terminal_set
        or selected_set
        != successful_set | retry_debt_set | terminal_set
        or (normalized_status == 'completed' and retry_debt_set)
    ):
        return None

    raw_manifest = result.get('scope_outcome_manifest')
    if not isinstance(raw_manifest, list):
        return None
    manifest_outcomes: dict[str, str] = {}
    for item in raw_manifest:
        if not isinstance(item, Mapping):
            return None
        raw_stock_code = str(item.get('stock_code') or '').strip()
        stock_code = (
            raw_stock_code.zfill(6)
            if raw_stock_code.isdigit()
            else raw_stock_code.upper()
        )
        outcome = str(item.get('outcome') or '').strip()
        if (
            not stock_code
            or stock_code in manifest_outcomes
            or outcome
            not in {'success', 'retry_debt', 'terminal_not_applicable'}
        ):
            return None
        manifest_outcomes[stock_code] = outcome
    expected_manifest_outcomes = {
        **{stock_code: 'success' for stock_code in successful_set},
        **{stock_code: 'retry_debt' for stock_code in retry_debt_set},
        **{
            stock_code: 'terminal_not_applicable'
            for stock_code in terminal_set
        },
    }
    if manifest_outcomes != expected_manifest_outcomes:
        return None
    return {
        'manifest_version': version,
        'selected_stock_codes': selected,
        'successful_stock_codes': successful,
        'retry_debt_stock_codes': retry_debt,
        'terminal_not_applicable_stock_codes': terminal_not_applicable,
    }


def _clear_exact_scope_outcome(result: dict[str, Any]) -> None:
    """Remove a child-stage partition before rebuilding a combined scope."""

    for key in _EXACT_SCOPE_OUTCOME_KEYS:
        result.pop(key, None)


def _build_pykrx_invocation_scope_proof(
    symbols: Sequence[str],
) -> dict[str, Any]:
    normalized = [
        str(value or '').strip().zfill(6)
        if str(value or '').strip().isdigit()
        else str(value or '').strip()
        for value in symbols
    ]
    if (
        not normalized
        or any(not value for value in normalized)
        or len(normalized) != len(set(normalized))
    ):
        raise ValueError('PyKRX invocation scope must be nonempty and unique')
    canonical = json.dumps(
        normalized,
        ensure_ascii=False,
        separators=(',', ':'),
    )
    return {
        'invocation_scope_manifest_version': (
            'pykrx_daily_invocation_scope_v1'
        ),
        'invocation_scope_stock_codes': normalized,
        'invocation_scope_count': len(normalized),
        'invocation_scope_sha256': hashlib.sha256(
            canonical.encode('utf-8')
        ).hexdigest(),
    }


def _build_pykrx_enrichment_symbol_scope_outcome(
    *,
    selected_symbols: Sequence[str],
    coverage_categories: dict[str, Any],
    required_categories: Sequence[str] = (
        REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES
    ),
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """Partition enrichment scope using exact per-symbol coverage proof."""

    selected = list(
        dict.fromkeys(
            str(value).strip()
            for value in selected_symbols
            if str(value).strip()
        )
    )
    selected_set = set(selected)
    retry_reasons: dict[str, list[str]] = {symbol: [] for symbol in selected}

    for raw_category in required_categories:
        category = str(raw_category).strip()
        payload = coverage_categories.get(category)
        if not isinstance(payload, dict):
            for symbol in selected:
                retry_reasons[symbol].append(
                    f'{category}:missing_exact_coverage_payload'
                )
            continue

        raw_expected = payload.get('expected_scope_keys')
        # Exact retry debt follows the same decision-usable contract as the
        # required-category gate.  Full component completeness remains
        # operator-visible, but an unavailable optional short-balance
        # component must not deadlock otherwise usable short-volume evidence.
        # Persisted legacy snapshots predate the decision-usable manifest and
        # may safely fall back to their stricter complete manifest.
        raw_decision_usable = (
            payload.get('decision_usable_scope_keys')
            if 'decision_usable_scope_keys' in payload
            else payload.get('complete_scope_keys')
        )
        if not isinstance(raw_expected, list) or not isinstance(
            raw_decision_usable, list
        ):
            for symbol in selected:
                retry_reasons[symbol].append(
                    f'{category}:missing_exact_scope_identity'
                )
            continue

        expected = {
            str(value).strip()
            for value in raw_expected
            if str(value).strip()
        }
        decision_usable = {
            str(value).strip()
            for value in raw_decision_usable
            if str(value).strip()
        }
        if len(expected) != len(raw_expected) or len(decision_usable) != len(
            raw_decision_usable
        ):
            for symbol in selected:
                retry_reasons[symbol].append(
                    f'{category}:malformed_exact_scope_identity'
                )
            continue
        if not decision_usable <= expected:
            for symbol in selected:
                retry_reasons[symbol].append(
                    f'{category}:non_exhaustive_exact_scope_identity'
                )
            continue

        # A category may legitimately exclude a symbol from its exact expected
        # universe (for example, a zero-volume investor-flow scope).  Once a
        # symbol is expected, decision-usable coverage is mandatory.
        for symbol in selected_set & expected:
            if symbol not in decision_usable:
                retry_reasons[symbol].append(
                    f'{category}:incomplete_required_coverage'
                )

    retry_debt_symbols = [
        symbol for symbol in selected if retry_reasons[symbol]
    ]
    successful_symbols = [
        symbol for symbol in selected if not retry_reasons[symbol]
    ]
    return (
        _build_exact_scope_outcome(
            selected_symbols=selected,
            successful_symbols=successful_symbols,
            retry_debt_symbols=retry_debt_symbols,
        ),
        {
            symbol: reasons
            for symbol, reasons in retry_reasons.items()
            if reasons
        },
    )


def _resolve_result_status(
    result: dict[str, Any],
    *,
    stage_id: str | None = None,
) -> str:
    if result.get('safe_skip') is True:
        return 'completed'
    if result.get('triggered') is False:
        if _is_already_completed_followup_result(result):
            return 'completed'
        status = _normalize_stage_status(result.get('status'))
        return status if status in {'waiting', 'blocked', 'failed', 'partial'} else 'skipped'
    status = _normalize_stage_status(result.get('status'))
    zero_scope_gate_status = daily_pipeline_zero_scope_gate_status(result)
    if zero_scope_gate_status:
        return zero_scope_gate_status
    status = normalize_daily_pipeline_stage_status(
        stage_id=stage_id,
        status=status,
        outcome=result,
    )
    if status != 'completed':
        return status
    if daily_pipeline_result_has_no_ready_content(result):
        return 'partial'
    failed_keys = (
        'symbols_failed',
        'events_failed',
        'results_failed',
        'contexts_failed',
        'features_failed',
        'snapshots_failed',
        'action_summaries_failed',
        'failed_count',
        'errors_count',
    )
    if any(int(result.get(key) or 0) > 0 for key in failed_keys):
        return 'partial'
    if result.get('error') or result.get('error_message'):
        return 'failed'
    return 'completed'


def _is_already_completed_followup_result(result: dict[str, Any]) -> bool:
    if result.get('triggered') is not False:
        return False
    if str(result.get('reason') or '').strip().lower() != 'already_completed':
        return False
    existing_run = (
        result.get('existing_run')
        if isinstance(result.get('existing_run'), dict)
        else {}
    )
    existing_status = str(existing_run.get('status') or '').strip().lower()
    return existing_status == 'completed'


def _is_satisfied_followup_result(
    result: dict[str, Any],
    *,
    stage_id: str | None = None,
) -> bool:
    outcome = dict(result)
    outcome['status'] = _resolve_result_status(
        result,
        stage_id=stage_id,
    )
    if stage_id:
        outcome['stage'] = stage_id
    return daily_pipeline_outcome_is_satisfied(outcome)


def _extract_raw_result_counts(
    result: dict[str, Any],
    *,
    default_total: int = 0,
    stage_id: str | None = None,
) -> dict[str, int]:
    def _int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    if stage_id == 'fundamentals_daily_refresh':
        succeeded = _int(result.get('events_captured'))
        failed = _int(result.get('events_failed'))
        total = _int(result.get('events_detected')) or succeeded + failed
        return {
            'processed': succeeded + failed,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
        }

    if stage_id == 'macro_context_refresh':
        succeeded = _int(result.get('observations_upserted'))
        failed = _int(result.get('observations_failed'))
        total = succeeded + failed
        return {
            'processed': total,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
        }

    if stage_id == 'external_news_collect':
        summary = result.get('summary') if isinstance(result.get('summary'), dict) else {}
        total_key = (
            'eligible_symbol_count'
            if 'eligible_symbol_count' in summary
            else 'requested_symbol_count'
        )
        total = _int(summary.get(total_key))
        collected = _int(
            summary.get('collected_symbol_count')
            if 'collected_symbol_count' in summary
            else summary.get('matched_symbol_count')
        )
        # Stale/no-result provider responses are successful collection work.
        # Keep freshness coverage in the summary, but do not turn it into a
        # terminal progress counter or processed will no longer reconcile to
        # succeeded + failed.
        succeeded = collected
        failed = _int(summary.get('failed_symbol_count')) + _int(
            summary.get('skipped_symbol_count')
        )
        return {
            'processed': collected + failed,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
        }

    if stage_id == 'external_news_enrich':
        inspected = _int(result.get('items_inspected'))
        missing_item_ids = result.get('missing_item_ids')
        remaining = _int(result.get('items_remaining_at_least'))
        failed_preview_count = (
            len(set(missing_item_ids))
            if isinstance(missing_item_ids, list)
            else 0
        )
        failed = max(
            failed_preview_count,
            _int(result.get('items_missing_classification')) - remaining,
        )
        return {
            'processed': inspected,
            'total': inspected + remaining,
            'succeeded': max(inspected - failed, 0),
            'failed': min(failed, inspected),
        }

    if stage_id == 'signal_features':
        selected_scope = result.get('selected_stock_codes')
        if isinstance(selected_scope, list):
            scope_status = str(result.get('status') or '').strip().lower()
            if scope_status not in {'completed', 'partial'}:
                scope_status = (
                    'partial'
                    if result.get('retry_debt_stock_codes')
                    else 'completed'
                )
            partition = _validated_exact_scope_outcome(
                result=result,
                invocation_symbols=selected_scope,
                status=scope_status,
            )
            if partition is not None:
                succeeded = len(partition['successful_stock_codes']) + len(
                    partition['terminal_not_applicable_stock_codes']
                )
                failed = len(partition['retry_debt_stock_codes'])
                total = len(partition['selected_stock_codes'])
                return {
                    'processed': succeeded + failed,
                    'total': total,
                    'succeeded': succeeded,
                    'failed': failed,
                }
        succeeded = _int(result.get('features_persisted'))
        failed = _int(result.get('features_failed'))
        processed = succeeded + failed
        reported_total = (
            _int(result.get('items_total'))
            if 'items_total' in result
            else processed
        )
        total = max(reported_total, processed)
        return {
            'processed': processed,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
        }

    count_pairs = (
        ('symbols_refreshed', 'symbols_failed'),
        ('events_captured', 'events_failed'),
        ('results_persisted', 'results_failed'),
        ('features_persisted', 'features_failed'),
        ('snapshots_persisted', 'snapshots_failed'),
        ('action_summaries_persisted', 'action_summaries_failed'),
        ('evaluations_upserted', 'failed_count'),
        ('outcomes_upserted', 'failed_count'),
        ('completed_count', 'failed_count'),
    )
    for succeeded_key, failed_key in count_pairs:
        if succeeded_key not in result and failed_key not in result:
            continue
        succeeded = _int(result.get(succeeded_key))
        failed = _int(result.get(failed_key))
        total = _int(result.get('total') or result.get('symbols_total') or result.get('requested_count'))
        if total <= 0:
            total = default_total or succeeded + failed
        return {
            'processed': min(succeeded + failed, total) if total else succeeded + failed,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
        }

    summary = result.get('summary') if isinstance(result.get('summary'), dict) else {}
    if 'items_total' in summary:
        counts = extract_next_open_plan_stage_counts(result)
        return {
            'processed': counts.processed,
            'total': counts.total,
            'succeeded': counts.succeeded,
            'failed': counts.failed,
        }
    if summary:
        succeeded = _int(
            summary.get('completed_count')
            or summary.get('computed_count')
            or summary.get('evaluated_count')
            or summary.get('passed_count')
        )
        failed = _int(summary.get('failed_count'))
        total = _int(summary.get('total') or summary.get('matched_symbol_count') or succeeded + failed)
        return {
            'processed': min(succeeded + failed, total) if total else succeeded + failed,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
        }

    progress = result.get('progress') if isinstance(result.get('progress'), dict) else {}
    progress_summary = progress.get('summary') if isinstance(progress.get('summary'), dict) else {}
    if progress_summary:
        succeeded = _int(
            progress_summary.get('completed_feedback_count')
            or progress_summary.get('completed_symbols')
            or progress_summary.get('processed_symbols')
        )
        failed = _int(progress_summary.get('failed_items') or progress_summary.get('failed_symbols'))
        total = _int(
            progress_summary.get('expected_feedback_count')
            or progress_summary.get('total_expected_feedback')
            or progress_summary.get('total_symbols')
        )
        return {
            'processed': min(succeeded + failed, total) if total else succeeded + failed,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
        }

    succeeded = _int(
        result.get('persisted_count')
        or result.get('trending_ranks_persisted')
        or result.get('items_persisted')
        or result.get('classified_items')
    )
    total = default_total or succeeded
    return {
        'processed': min(succeeded, total) if total else succeeded,
        'total': total,
        'succeeded': succeeded,
        'failed': 0,
    }


def _extract_result_counts(
    result: dict[str, Any],
    *,
    default_total: int = 0,
    stage_id: str | None = None,
) -> dict[str, int]:
    raw = _extract_raw_result_counts(
        result,
        default_total=default_total,
        stage_id=stage_id,
    )
    succeeded = max(int(raw.get('succeeded') or 0), 0)
    failed = max(int(raw.get('failed') or 0), 0)
    processed = max(
        int(raw.get('processed') or 0),
        succeeded + failed,
        0,
    )
    total = max(int(raw.get('total') or 0), processed, 0)
    if _normalize_stage_status(result.get('status')) == 'completed':
        processed = total
    return {
        'processed': processed,
        'total': total,
        'succeeded': succeeded,
        'failed': failed,
    }


def _compact_result_summary(result: dict[str, Any]) -> dict[str, Any]:
    omitted_keys = {'items', 'rows', 'payloads', 'results'}
    return {
        key: value
        for key, value in result.items()
        if key not in omitted_keys and not key.endswith('_payload')
    }


def _extract_followup_source_run_ids(
    *,
    stage_id: str,
    result: dict[str, Any],
) -> dict[str, Any]:
    source_run_ids = (
        dict(result['source_run_ids'])
        if isinstance(result.get('source_run_ids'), dict)
        else {}
    )
    if (
        stage_id in {'agent_feedback_summaries', 'signal_decider'}
        and result.get('run_id') is not None
    ):
        source_run_ids.setdefault(
            'agent_feedback_run_id',
            result['run_id'],
        )
    return source_run_ids


def _resolve_auto_daily_tracked_stages(
    *,
    selected_stages: Sequence[str] | None,
    chain_fundamentals_followup: bool,
) -> list[str]:
    requested = list(selected_stages or PYKRX_DAILY_STAGE_ORDER)
    if chain_fundamentals_followup:
        requested.extend(AUTO_DAILY_FOLLOWUP_STAGE_ORDER)
    requested_set = {str(stage_id or '').strip() for stage_id in requested if str(stage_id or '').strip()}
    return [stage_id for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER if stage_id in requested_set]


class DailyPipelineTrackingWriteError(RuntimeError):
    """An ownership-critical official pipeline checkpoint was not persisted."""


class _OfficialDailyPipelineTracker:
    def __init__(self, *, service: Any | None, log_fetch: Callable[..., None]) -> None:
        self.service = service
        self.log_fetch = log_fetch
        self.pipeline_run_id: int | None = None
        self.market_date: str | None = None
        self.eod_run_id: int | None = None
        self.stage_statuses: dict[str, str] = {}
        self.stage_outcomes: dict[str, dict[str, Any]] = {}
        self.stage_source_run_ids: dict[str, dict[str, Any]] = {}
        self.enabled = service is not None

    def _log_tracking_failure(self, *, stage: str, exc: Exception, **extra: Any) -> None:
        try:
            self.log_fetch(
                'daily_pipeline_tracking',
                'failed',
                stage=stage,
                run_id=self.eod_run_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
                **extra,
            )
        except Exception as log_exc:  # pragma: no cover - logging must be fail-open
            logger.warning(
                'daily pipeline tracking failure log also failed: %s',
                log_exc,
            )

    def start(
        self,
        *,
        market_date: str,
        trigger: str,
        requested_stages: Sequence[str],
        resolved_stages: Sequence[str],
        eod_run_id: int | None,
        run_metadata: dict[str, Any],
        provider_admission_fence_owned: bool = False,
    ) -> dict[str, Any] | None:
        if not self.enabled or self.service is None:
            return None
        try:
            tracking_metadata = {
                'entrypoint': 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
                **run_metadata,
            }
            if eod_run_id is not None:
                tracking_metadata['eod_collection_run_id'] = eod_run_id
            run = self.service.start_run(
                market_date=market_date,
                trigger=trigger,
                requested_stages=requested_stages,
                resolved_stages=resolved_stages,
                run_metadata=tracking_metadata,
                provider_admission_fence_owned=(
                    provider_admission_fence_owned
                ),
            )
            if run.get('deduplicated'):
                self.enabled = False
                self.log_fetch(
                    'daily_pipeline_tracking',
                    'deduplicated',
                    stage='start_run',
                    run_id=eod_run_id,
                    existing_pipeline_run_id=run.get('id'),
                    reason=run.get('deduplication_reason'),
                )
                return run
            self.pipeline_run_id = int(run['id'])
            self.market_date = market_date
            self.eod_run_id = eod_run_id
            return run
        except Exception as exc:
            logger.warning('daily pipeline tracker start failed for auto EOD run %s: %s', eod_run_id, exc)
            self._log_tracking_failure(
                stage='start_run',
                exc=exc,
            )
            raise DailyPipelineTrackingWriteError(
                f'daily pipeline admission checkpoint failed: {exc}'
            ) from exc

    def update_stage(
        self,
        *,
        stage: str,
        status: str,
        current_step: str,
        current_activity: str,
        processed: int | None = None,
        total: int | None = None,
        succeeded: int | None = None,
        failed: int | None = None,
        current_symbol: str | None = None,
        current_period: str | None = None,
        source_run_ids: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if not self.enabled or self.service is None or self.pipeline_run_id is None or self.market_date is None:
            return
        raw_status = _normalize_stage_status(status)
        merged_source_run_ids = dict(
            self.stage_source_run_ids.get(stage, {})
        )
        if self.eod_run_id is not None:
            merged_source_run_ids['eod_run_id'] = self.eod_run_id
        if source_run_ids:
            merged_source_run_ids.update(source_run_ids)
        self.stage_source_run_ids[stage] = dict(merged_source_run_ids)
        merged_details = {
            'mode': 'auto_eod_pipeline_tracking',
            'progress_source': 'auto_eod_stage_progress',
            **dict(details or {}),
        }
        normalized_status = normalize_daily_pipeline_stage_status(
            stage_id=stage,
            status=raw_status,
            outcome={
                'stage': stage,
                'status': raw_status,
                'processed': processed,
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
                'details': merged_details,
            },
        )
        if normalized_status != raw_status:
            merged_details.setdefault(
                'status_canonicalized_from',
                raw_status,
            )
            merged_details.setdefault(
                'status_canonicalization_reason',
                'terminal_optional_context_unavailable',
            )
        self.stage_statuses[stage] = normalized_status
        self.stage_outcomes[stage] = {
            'stage': stage,
            'status': normalized_status,
            'processed': processed,
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
            'details': merged_details,
        }
        try:
            self.service.update_stage(
                pipeline_run_id=self.pipeline_run_id,
                stage_id=stage,
                market_date=self.market_date,
                status=normalized_status,
                current_step=current_step,
                current_activity=current_activity,
                processed=processed,
                total=total,
                succeeded=succeeded,
                failed=failed,
                current_symbol=current_symbol,
                current_period=current_period,
                source_run_ids=merged_source_run_ids,
                details=merged_details,
            )
        except Exception as exc:
            logger.warning('daily pipeline tracker update failed for run %s stage %s: %s', self.eod_run_id, stage, exc)
            self._log_tracking_failure(
                stage=stage,
                exc=exc,
            )
            raise DailyPipelineTrackingWriteError(
                f'daily pipeline stage checkpoint failed for {stage}: {exc}'
            ) from exc

    def heartbeat_stage(self, *, stage: str) -> bool:
        if (
            not self.enabled
            or self.service is None
            or self.pipeline_run_id is None
            or self.market_date is None
        ):
            return True
        try:
            return bool(
                self.service.heartbeat_stage(
                    pipeline_run_id=self.pipeline_run_id,
                    stage_id=stage,
                    market_date=self.market_date,
                )
            )
        except Exception as exc:  # pragma: no cover - defensive production guard
            logger.warning(
                'daily pipeline heartbeat failed for run %s stage %s: %s',
                self.eod_run_id,
                stage,
                exc,
            )
            self._log_tracking_failure(
                stage=stage,
                exc=exc,
            )
            return True

    def link_eod_run(self, eod_run_id: int) -> None:
        self.eod_run_id = int(eod_run_id)
        if (
            not self.enabled
            or self.service is None
            or self.pipeline_run_id is None
        ):
            return
        try:
            self.service.link_eod_run(
                pipeline_run_id=self.pipeline_run_id,
                eod_run_id=self.eod_run_id,
            )
        except Exception as exc:
            logger.warning(
                'daily pipeline EOD link failed for provider run %s: %s',
                self.eod_run_id,
                exc,
            )
            self._log_tracking_failure(
                stage='link_eod_run',
                exc=exc,
                pipeline_run_id=self.pipeline_run_id,
            )
            raise DailyPipelineTrackingWriteError(
                f'daily pipeline EOD link checkpoint failed: {exc}'
            ) from exc

    def finish_run(self, *, status: str | None = None, error_message: str | None = None) -> None:
        if not self.enabled or self.service is None or self.pipeline_run_id is None:
            return
        final_status = status or self.resolve_overall_status()
        warning_metadata = daily_pipeline_warning_metadata(
            self.stage_outcomes,
        )
        last_error: Exception | None = None
        for attempt in range(1, DAILY_PIPELINE_TRACKER_FINISH_ATTEMPTS + 1):
            try:
                self.service.finish_run(
                    pipeline_run_id=self.pipeline_run_id,
                    status=final_status,
                    error_message=error_message,
                    run_metadata={
                        'stage_statuses': dict(self.stage_statuses),
                        'eod_collection_run_id': self.eod_run_id,
                        **warning_metadata,
                    },
                )
                return
            except Exception as exc:
                last_error = exc
                logger.warning(
                    'daily pipeline tracker finish attempt %s/%s failed for run %s: %s',
                    attempt,
                    DAILY_PIPELINE_TRACKER_FINISH_ATTEMPTS,
                    self.eod_run_id,
                    exc,
                )
        assert last_error is not None
        self._log_tracking_failure(
            stage='finish_run',
            exc=last_error,
            attempts=DAILY_PIPELINE_TRACKER_FINISH_ATTEMPTS,
        )
        raise DailyPipelineTrackingWriteError(
            f'daily pipeline terminal checkpoint failed after '
            f'{DAILY_PIPELINE_TRACKER_FINISH_ATTEMPTS} attempts: {last_error}'
        ) from last_error

    def finish_run_preserving_primary_error(
        self,
        *,
        error_message: str,
    ) -> None:
        """Retry terminal tracking without masking the worker's root error."""

        try:
            self.finish_run(status='failed', error_message=error_message)
        except DailyPipelineTrackingWriteError as exc:
            logger.error(
                'Unable to terminalize official daily pipeline after '
                'worker failure: %s',
                exc,
            )

    def resolve_overall_status(self) -> str:
        warning_stage_ids = set(
            daily_pipeline_nonblocking_warning_stage_ids(
                self.stage_outcomes,
            )
        )
        statuses = {
            status
            for stage_id, status in self.stage_statuses.items()
            if stage_id not in warning_stage_ids
        }
        if not statuses:
            return 'completed'
        if 'failed' in statuses:
            return 'failed'
        if 'blocked' in statuses:
            return 'blocked'
        if 'waiting' in statuses:
            return 'waiting'
        if 'running' in statuses:
            return 'running'
        if 'partial' in statuses:
            return 'partial'
        if statuses <= {'completed', 'skipped'}:
            return 'completed'
        return 'partial'


def _validated_exact_piotroski_membership_symbols(
    *,
    universe_snapshot: dict[str, Any] | None,
    market_date: str,
    markets: Sequence[str],
    limited_run: bool = False,
) -> list[str] | None:
    """Return the exact authoritative EOD membership, or fail closed.

    Piotroski ranks are cross-sectional.  A daily-price row is therefore not
    sufficient proof that its symbol belonged to the official EOD universe.
    Only a complete PyKRX snapshot whose embedded membership manifest validates
    for the same date and markets may define the ranking/persistence scope.
    """

    if limited_run or not isinstance(universe_snapshot, dict):
        return None
    normalized_markets = list(
        dict.fromkeys(str(market or '').strip().upper() for market in markets)
    )
    snapshot_markets = universe_snapshot.get('markets')
    snapshot_unavailable_markets = universe_snapshot.get(
        'unavailable_markets'
    )
    universe_source = str(
        universe_snapshot.get('universe_source') or ''
    ).strip()
    if (
        not normalized_markets
        or any(not market for market in normalized_markets)
        or universe_snapshot.get('source') != 'pykrx'
        or universe_snapshot.get('complete') is not True
        or str(universe_snapshot.get('latest_trade_date') or '') != market_date
        or universe_snapshot.get('membership_basis') != 'end_date_snapshot'
        or not isinstance(snapshot_markets, list)
        or len(snapshot_markets) != len(normalized_markets)
        or set(
            str(market or '').strip().upper() for market in snapshot_markets
        )
        != set(normalized_markets)
        or not isinstance(snapshot_unavailable_markets, list)
        or bool(snapshot_unavailable_markets)
        or not universe_source
        or universe_source == 'point_in_time_retry_scope'
    ):
        return None

    membership = universe_snapshot.get('market_membership')
    if not isinstance(membership, dict):
        return None
    raw_membership_markets = membership.get('markets')
    if not isinstance(raw_membership_markets, dict):
        return None
    requested_symbols: list[str] = []
    for market in normalized_markets:
        entry = raw_membership_markets.get(market)
        if not isinstance(entry, dict) or not isinstance(
            entry.get('symbols'),
            list,
        ):
            return None
        requested_symbols.extend(entry['symbols'])

    validated = validate_market_membership_snapshot(
        membership,
        requested_symbols=requested_symbols,
        markets=normalized_markets,
    )
    if validated is None:
        return None
    exact_symbols = sorted(
        symbol
        for market in normalized_markets
        for symbol in validated.get(market, [])
    )
    if not exact_symbols:
        return None
    outer_symbol_count = universe_snapshot.get('symbol_count')
    if outer_symbol_count is not None and (
        isinstance(outer_symbol_count, bool)
        or outer_symbol_count != len(exact_symbols)
    ):
        return None
    return exact_symbols


def _run_daily_piotroski_filter_followup(
    *,
    market_date: str,
    exact_membership_symbols: Sequence[str] | None,
    admitted_symbols: Sequence[str] | None = None,
    authoritative_market_scope: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    from app.signals.piotroski_filter import daily_piotroski_filter_service

    normalized_symbol_set: set[str] = set()
    for symbol in exact_membership_symbols or []:
        normalized_symbol = str(symbol or '').strip().upper()
        if not normalized_symbol:
            continue
        if normalized_symbol.isdigit():
            normalized_symbol = normalized_symbol.zfill(6)
        normalized_symbol_set.add(normalized_symbol)
    normalized_symbols = sorted(normalized_symbol_set)
    if not normalized_symbols:
        raise RuntimeError(
            'Piotroski follow-up requires validated exact EOD membership proof.'
        )
    normalized_admitted_symbols = (
        sorted(
            {
                str(symbol or '').strip().upper().zfill(6)
                for symbol in admitted_symbols
                if str(symbol or '').strip()
            }
        )
        if admitted_symbols is not None
        else list(normalized_symbols)
    )
    outside_membership = sorted(
        set(normalized_admitted_symbols) - set(normalized_symbols)
    )
    if outside_membership:
        raise RuntimeError(
            'Piotroski admitted scope falls outside exact EOD membership: '
            f'{outside_membership[:20]}'
        )
    try:
        result = daily_piotroski_filter_service.rebuild_for_market_date(
            market_date=market_date,
            symbols=normalized_admitted_symbols,
            ranking_symbols=normalized_symbols,
            exact_scope_symbols=normalized_symbols,
            authoritative_market_scope=(
                authoritative_market_scope
                if set(normalized_admitted_symbols) == set(normalized_symbols)
                else None
            ),
        )
    except Exception:
        _finalize_daily_stage_scope(
            market_date=market_date,
            stage_id='piotroski_filter',
            exact_symbols=normalized_symbols,
            result=None,
            authoritative_market_scope=(
                authoritative_market_scope
                if set(normalized_admitted_symbols) == set(normalized_symbols)
                else None
            ),
        )
        raise
    if not (
        isinstance(result.get('scope_finalization'), Mapping)
        and result['scope_finalization'].get('ready') is True
    ):
        raise RuntimeError(
            'Piotroski atomic exact-scope finalization proof is unavailable.'
        )
    return result


def _successful_symbols_from_stage_result(
    *,
    stage_id: str,
    exact_symbols: Sequence[str],
    result: Mapping[str, Any],
) -> list[str]:
    exact = list(dict.fromkeys(exact_symbols))
    exact_set = set(exact)
    explicit_success = result.get('successful_stock_codes')
    if isinstance(explicit_success, list):
        successful = list(
            dict.fromkeys(
                str(symbol).strip().upper().zfill(6)
                for symbol in explicit_success
                if str(symbol).strip()
            )
        )
    else:
        item_symbols = list(
            dict.fromkeys(
                str(item.get('symbol') or '').strip().upper().zfill(6)
                for item in (result.get('items') or [])
                if isinstance(item, Mapping)
                and str(item.get('symbol') or '').strip()
            )
        )
        persisted_count_key = {
            'price_contexts': 'contexts_persisted',
            'technical_snapshots': 'snapshots_persisted',
            'action_summaries': 'action_summaries_persisted',
        }.get(stage_id, 'results_persisted')
        persisted_count = int(result.get(persisted_count_key) or 0)
        if len(item_symbols) == persisted_count:
            successful = item_symbols
        else:
            failed = {
                str(symbol).strip().upper().zfill(6)
                for symbol in [
                    *(result.get('failed_symbols') or []),
                    *(result.get('missing_symbols') or []),
                ]
                if str(symbol).strip()
            }
            inferred = [symbol for symbol in exact if symbol not in failed]
            if len(inferred) != persisted_count:
                raise RuntimeError(
                    'cannot prove current-attempt persisted scope: '
                    f'stage={stage_id}, persisted={persisted_count}, '
                    f'item_symbols={len(item_symbols)}, inferred={len(inferred)}'
                )
            successful = inferred
    outside = sorted(set(successful) - exact_set)
    if outside:
        raise RuntimeError(
            'stage reported success outside exact membership: '
            f'stage={stage_id}, preview={outside[:20]}'
        )
    persisted_count_key = {
        'price_contexts': 'contexts_persisted',
        'technical_snapshots': 'snapshots_persisted',
        'action_summaries': 'action_summaries_persisted',
    }.get(stage_id, 'results_persisted')
    persisted_count = int(result.get(persisted_count_key) or 0)
    if len(successful) != persisted_count:
        raise RuntimeError(
            'stage success evidence does not match persisted count: '
            f'stage={stage_id}, successful={len(successful)}, '
            f'persisted={persisted_count}'
        )
    return sorted(successful)


def _finalize_daily_stage_scope(
    *,
    market_date: str,
    stage_id: str,
    exact_symbols: Sequence[str],
    result: Mapping[str, Any] | None = None,
    authoritative_market_scope: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    from app.market_data_store import (
        finalize_daily_derived_stage_exact_scope,
    )

    successful_symbols = (
        _successful_symbols_from_stage_result(
            stage_id=stage_id,
            exact_symbols=exact_symbols,
            result=result,
        )
        if result is not None
        else []
    )
    return finalize_daily_derived_stage_exact_scope(
        market_date=market_date,
        stage_id=stage_id,
        exact_symbols=exact_symbols,
        successful_symbols=successful_symbols,
        authoritative_market_scope=authoritative_market_scope,
    )


def _execute_factor_stage_with_heartbeat(
    *,
    call: Callable[[], dict[str, Any]],
    heartbeat: Callable[[], bool],
    interval_seconds: float,
    thread_name: str,
) -> tuple[dict[str, Any], bool]:
    pulse = PeriodicStageHeartbeat(
        heartbeat,
        interval_seconds=interval_seconds,
        thread_name=thread_name,
    )
    if not pulse.check_now():
        return (
            {
                'status': 'interrupted',
                'reason': 'pipeline_run_ownership_lost',
            },
            False,
        )
    try:
        with pulse:
            result = call()
    except Exception as exc:
        if not pulse.ownership_lost:
            raise
        return (
            {
                'status': 'interrupted',
                'reason': 'pipeline_run_ownership_lost',
                'error_type': type(exc).__name__,
                'error_message': str(exc),
            },
            False,
        )
    return result, not pulse.ownership_lost


def _run_daily_factor_followup(
    *,
    market_date: str,
    exact_membership_symbols: Sequence[str] | None,
    admitted_symbols: Sequence[str] | None = None,
    authoritative_market_scope: Mapping[str, Any] | None = None,
    stage_executor: (
        Callable[
            [str, Callable[[], dict[str, Any]]],
            tuple[dict[str, Any], bool],
        ]
        | None
    ) = None,
    stage_result_callback: (
        Callable[[str, dict[str, Any]], None] | None
    ) = None,
) -> dict[str, Any]:
    from app.signals.advanced_value_factor import ADVANCED_VALUE_FACTOR_SOURCE, daily_advanced_value_factor_service
    from app.signals.beta_factor import BETA_FACTOR_SOURCE, daily_beta_factor_service
    from app.signals.composite_factor import COMPOSITE_FACTOR_SOURCE, daily_composite_factor_service
    from app.signals.low_risk_factor import LOW_RISK_SOURCE, daily_low_risk_factor_service
    from app.signals.momentum_factor import daily_momentum_factor_service
    from app.signals.quality_factor import QUALITY_FACTOR_SOURCE, daily_quality_factor_service
    from app.signals.unified_value_factor import UNIFIED_VALUE_FACTOR_SOURCE, daily_unified_value_factor_service
    from app.signals.value_factor import VALUE_FACTOR_SOURCE, daily_value_factor_service

    normalized_symbols = sorted(
        {
            str(symbol or '').strip().upper().zfill(6)
            for symbol in exact_membership_symbols or []
            if str(symbol or '').strip()
        }
    )
    if not normalized_symbols:
        raise RuntimeError(
            'Daily factor follow-up requires validated exact EOD membership '
            'proof.'
        )
    initial_admitted_symbols = (
        sorted(
            {
                str(symbol or '').strip().upper().zfill(6)
                for symbol in admitted_symbols
                if str(symbol or '').strip()
            }
        )
        if admitted_symbols is not None
        else list(normalized_symbols)
    )
    outside_membership = sorted(
        set(initial_admitted_symbols) - set(normalized_symbols)
    )
    if outside_membership:
        raise RuntimeError(
            'Daily factor admitted scope falls outside exact EOD membership: '
            f'{outside_membership[:20]}'
        )

    def build_stage_call(
        stage_id: str,
        stage_symbols: Sequence[str],
    ) -> Callable[[], dict[str, Any]]:
        common = {
            'market_date': market_date,
            'symbols': list(stage_symbols),
            'ranking_symbols': normalized_symbols,
            'exact_scope_symbols': normalized_symbols,
            'authoritative_market_scope': (
                authoritative_market_scope
                if set(stage_symbols) == set(normalized_symbols)
                else None
            ),
        }
        if stage_id == 'momentum_factors':
            return lambda: daily_momentum_factor_service.rebuild_for_market_date(
                **common
            )
        services_and_sources = {
            'value_factors': (
                daily_value_factor_service,
                VALUE_FACTOR_SOURCE,
            ),
            'low_risk_factors': (
                daily_low_risk_factor_service,
                LOW_RISK_SOURCE,
            ),
            'beta_factors': (daily_beta_factor_service, BETA_FACTOR_SOURCE),
            'advanced_value_factors': (
                daily_advanced_value_factor_service,
                ADVANCED_VALUE_FACTOR_SOURCE,
            ),
            'unified_value_factors': (
                daily_unified_value_factor_service,
                UNIFIED_VALUE_FACTOR_SOURCE,
            ),
            'quality_factors': (
                daily_quality_factor_service,
                QUALITY_FACTOR_SOURCE,
            ),
            'composite_factors': (
                daily_composite_factor_service,
                COMPOSITE_FACTOR_SOURCE,
            ),
        }
        service, source = services_and_sources[stage_id]
        return lambda: service.rebuild_for_market_date(
            **common,
            source=source,
        )

    stages: dict[str, dict[str, Any]] = {}
    overall_status = 'completed'
    current_admitted_symbols = list(initial_admitted_symbols)
    for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER:
        stage_invocation_symbols = list(current_admitted_symbols)
        stage_call = build_stage_call(stage_id, stage_invocation_symbols)
        scope_finalized = False
        try:
            if stage_executor is None:
                result = stage_call()
                ownership_retained = True
            else:
                result, ownership_retained = stage_executor(
                    stage_id,
                    stage_call,
                )
            if not ownership_retained:
                stages[stage_id] = {
                    'status': 'interrupted',
                    'result': result,
                }
                return {
                    'triggered': False,
                    'reason': 'pipeline_run_ownership_lost',
                    'market_date': market_date,
                    'status': 'interrupted',
                    'stages': stages,
                }
            if not (
                isinstance(result.get('scope_finalization'), Mapping)
                and result['scope_finalization'].get('ready') is True
            ):
                raise RuntimeError(
                    f'{stage_id} atomic exact-scope finalization proof '
                    'is unavailable'
                )
            scope_finalized = True
            stage_status = _resolve_result_status(
                result,
                stage_id=stage_id,
            )
            exact_partition = _validated_exact_scope_outcome(
                result=result,
                invocation_symbols=stage_invocation_symbols,
                status=stage_status,
            )
            if str(result.get('gate_status') or '').strip().lower() == 'blocked':
                raise RuntimeError(
                    'factor stage coverage gate is blocked: '
                    f'stage={stage_id}'
                )
            if exact_partition is None:
                if stage_status != 'completed':
                    raise RuntimeError(
                        'factor stage partial result lacks an exact current-attempt '
                        f'scope manifest: stage={stage_id}'
                    )
                successful_symbols = _successful_symbols_from_stage_result(
                    stage_id=stage_id,
                    exact_symbols=stage_invocation_symbols,
                    result=result,
                )
            else:
                successful_symbols = exact_partition[
                    'successful_stock_codes'
                ]
            current_admitted_symbols = [
                symbol
                for symbol in stage_invocation_symbols
                if symbol in set(successful_symbols)
            ]
            if stage_status != 'completed':
                overall_status = 'partial'
            stage_result = {
                'status': stage_status,
                'result': result,
            }
            stages[stage_id] = stage_result
            if stage_result_callback is not None:
                stage_result_callback(stage_id, stage_result)
        except Exception as exc:  # pragma: no cover - defensive production guard
            logger.exception('Daily factor follow-up failed for %s on %s', stage_id, market_date)
            overall_status = 'partial'
            current_admitted_symbols = []
            finalization_error: Exception | None = None
            scope_finalization: dict[str, Any] | None = None
            if not scope_finalized:
                try:
                    scope_finalization = _finalize_daily_stage_scope(
                        market_date=market_date,
                        stage_id=stage_id,
                        exact_symbols=normalized_symbols,
                        result=None,
                        authoritative_market_scope=(
                            authoritative_market_scope
                            if set(stage_invocation_symbols)
                            == set(normalized_symbols)
                            else None
                        ),
                    )
                except Exception as cleanup_exc:  # pragma: no cover - defensive
                    finalization_error = cleanup_exc
            stage_result = {
                'status': 'failed',
                'error_type': type(exc).__name__,
                'error_message': str(exc),
                'scope_finalization': scope_finalization,
            }
            if finalization_error is not None:
                stage_result['scope_finalization_error_type'] = type(
                    finalization_error
                ).__name__
                stage_result['scope_finalization_error_message'] = str(
                    finalization_error
                )
            stages[stage_id] = stage_result
            if stage_result_callback is not None:
                stage_result_callback(stage_id, stage_result)
    retry_debt_symbols = [
        symbol
        for symbol in initial_admitted_symbols
        if symbol not in set(current_admitted_symbols)
    ]
    aggregate_scope_outcome = _build_exact_scope_outcome(
        manifest_version='factor_followup_scope_outcomes_v1',
        selected_symbols=initial_admitted_symbols,
        successful_symbols=current_admitted_symbols,
        retry_debt_symbols=retry_debt_symbols,
    )
    if retry_debt_symbols:
        overall_status = 'partial'
    return {
        'triggered': True,
        'market_date': market_date,
        'status': overall_status,
        'membership_symbol_count': len(normalized_symbols),
        'membership_sha256': market_membership_sha256(
            normalized_symbols
        ),
        **aggregate_scope_outcome,
        'gate_status': (
            'ready' if current_admitted_symbols else 'blocked'
        ),
        'blocking_status': (
            'non_blocking'
            if current_admitted_symbols
            else 'blocking'
        ),
        '_retry_metadata': {
            'failed_symbols': retry_debt_symbols,
        },
        'stages': stages,
        '_stage_results_recorded_incrementally': (
            stage_result_callback is not None
        ),
    }


def _run_daily_external_news_followup(*, market_date: str, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {
            'triggered': False,
            'reason': 'followup_disabled',
            'market_date': market_date,
        }

    from app.core.settings import settings
    from app.external_news.service import external_news_service

    result = asyncio.run(
        external_news_service.collect_for_agent_feedback_candidates(
            market_date=market_date,
            display=settings.external_news_default_display,
        )
    )
    return {
        'triggered': True,
        **result,
    }


def _run_daily_external_news_enrichment_followup(*, market_date: str, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {
            'triggered': False,
            'reason': 'followup_disabled',
            'market_date': market_date,
        }

    from app.core.settings import settings
    from app.external_news.service import external_news_service

    result = external_news_service.enrich_recent_classification(
        market_date=market_date,
        lookback_days=settings.external_news_context_lookback_days,
        limit=1000,
    )
    return {
        'triggered': True,
        'market_date': market_date,
        **result,
    }


def _build_macro_provider_coverage(source_statuses: Any) -> dict[str, Any]:
    if not isinstance(source_statuses, dict) or not source_statuses:
        return {
            'status': 'unknown',
            'provider_count': 0,
            'available_provider_count': 0,
            'configured_provider_count': 0,
            'gap_provider_count': 0,
            'providers': {},
        }

    provider_states: dict[str, str] = {}
    for provider, payload in source_statuses.items():
        raw_status = (
            payload.get('status')
            if isinstance(payload, dict)
            else payload
        )
        status = str(raw_status or 'unknown').strip().lower()
        if status in {'available', 'ready', 'completed', 'fresh'}:
            provider_state = 'available'
        elif status == 'configured':
            provider_state = 'configured'
        elif status.startswith('disabled'):
            provider_state = 'disabled'
        elif status in {'partial', 'stale', 'degraded'}:
            provider_state = 'degraded'
        elif status in {
            'unavailable',
            'failed',
            'error',
            'missing',
            'blocked_missing_credential',
        }:
            provider_state = 'unavailable'
        else:
            provider_state = 'unknown'
        provider_states[str(provider)] = provider_state

    available_provider_count = sum(
        state == 'available' for state in provider_states.values()
    )
    gap_provider_count = sum(
        state in {'disabled', 'degraded', 'unavailable'}
        for state in provider_states.values()
    )
    configured_provider_count = sum(
        state == 'configured' for state in provider_states.values()
    )
    if available_provider_count == len(provider_states):
        coverage_status = 'complete'
    elif available_provider_count > 0:
        coverage_status = 'partial'
    elif configured_provider_count > 0:
        coverage_status = 'unknown'
    elif gap_provider_count > 0:
        coverage_status = 'unavailable'
    else:
        coverage_status = 'unknown'
    return {
        'status': coverage_status,
        'provider_count': len(provider_states),
        'available_provider_count': available_provider_count,
        'configured_provider_count': configured_provider_count,
        'gap_provider_count': gap_provider_count,
        'providers': provider_states,
    }


def _run_daily_macro_context_followup(*, market_date: str, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {
            'triggered': False,
            'reason': 'followup_disabled',
            'market_date': market_date,
            'freshness_gate': 'not_checked',
        }

    from app.macro_context.service import macro_context_service

    result = macro_context_service.refresh_context(
        force=False,
        trigger='eod_pykrx_followup',
    )
    context = result.get('context') if isinstance(result.get('context'), dict) else {}
    freshness = context.get('freshness') if isinstance(context.get('freshness'), dict) else {}
    freshness_status = str(freshness.get('status') or context.get('status') or 'unavailable').strip().lower()
    return {
        'triggered': True,
        'market_date': market_date,
        **result,
        'freshness_gate': freshness_status,
        'provider_coverage': _build_macro_provider_coverage(
            context.get('source_statuses')
            if isinstance(context.get('source_statuses'), dict)
            and context.get('source_statuses')
            else result.get('source_statuses')
        ),
    }


def _frame_value(frame: Any, symbol: str, column: str) -> Any:
    if frame is None or getattr(frame, 'empty', False):
        return None
    try:
        if symbol in frame.index and column in frame.columns:
            return frame.loc[symbol, column]
    except Exception:
        pass
    try:
        row = frame.loc[symbol]
        if hasattr(row, 'get'):
            return row.get(column)
    except Exception:
        return None
    return None


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if hasattr(value, 'item'):
            value = value.item()
        converted = int(value)
    except (TypeError, ValueError):
        return None
    return converted if converted > 0 else None


def _summarize_turnover_coverage(counters: Mapping[str, Any]) -> dict[str, Any]:
    total_bars = int(counters.get('total_bars') or 0)
    zero_volume_bars = int(counters.get('zero_volume_bars') or 0)
    eligible_bars = max(total_bars - zero_volume_bars, 0)
    provider_turnover_bars = int(counters.get('provider_turnover_bars') or 0)
    invalid_turnover_bars = int(counters.get('invalid_turnover_bars') or 0)
    market_fetch_failures = int(counters.get('market_fetch_failures') or 0)
    return {
        'status': (
            'partial'
            if invalid_turnover_bars > 0 or market_fetch_failures > 0
            else 'complete'
        ),
        'total_bars': total_bars,
        'eligible_bars': eligible_bars,
        'provider_turnover_bars': provider_turnover_bars,
        'zero_volume_bars': zero_volume_bars,
        'invalid_turnover_bars': invalid_turnover_bars,
        'market_fetch_attempts': int(counters.get('market_fetch_attempts') or 0),
        'market_fetch_failures': market_fetch_failures,
        'coverage_ratio': (
            round(provider_turnover_bars / eligible_bars, 6)
            if eligible_bars
            else 1.0
        ),
    }


def _enrich_bars_with_market_turnover(
    *,
    bars: list[dict[str, Any]],
    symbol: str,
    market: str,
    dependencies: PykrxPipelineDependencies,
    market_turnover_cache: dict[tuple[str, str], Any],
    turnover_coverage: dict[str, int] | None = None,
    run_id: int | None,
) -> list[dict[str, Any]]:
    coverage = turnover_coverage
    if coverage is not None:
        for key in (
            'total_bars',
            'provider_turnover_bars',
            'zero_volume_bars',
            'invalid_turnover_bars',
            'market_fetch_attempts',
            'market_fetch_failures',
        ):
            coverage.setdefault(key, 0)
    getter = dependencies.get_market_ohlcv_by_ticker
    if not bars:
        return bars
    enriched: list[dict[str, Any]] = []
    for bar in bars:
        if coverage is not None:
            coverage['total_bars'] += 1
        trade_date = str(bar.get('trade_date'))
        pykrx_date = trade_date.replace('-', '')
        cache_key = (pykrx_date, market)
        if getter is not None and cache_key not in market_turnover_cache:
            if coverage is not None:
                coverage['market_fetch_attempts'] += 1
            try:
                market_turnover_cache[cache_key] = getter(pykrx_date, market)
            except Exception as exc:
                market_turnover_cache[cache_key] = None
                if coverage is not None:
                    coverage['market_fetch_failures'] += 1
                dependencies.log_fetch(
                    'fetch_market_ohlcv_turnover',
                    'failed',
                    run_id=run_id,
                    market=market,
                    trade_date=trade_date,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
        provider_turnover = _to_int_or_none(_frame_value(market_turnover_cache.get(cache_key), symbol, '거래대금'))
        updated = dict(bar)
        if provider_turnover is not None:
            updated['turnover'] = provider_turnover
            if coverage is not None:
                coverage['provider_turnover_bars'] += 1
            enriched.append(updated)
            continue
        updated['turnover'] = 0
        raw_volume = bar.get('volume')
        try:
            volume = int(raw_volume) if raw_volume is not None else None
        except (TypeError, ValueError):
            volume = None
        if coverage is not None:
            if volume == 0:
                coverage['zero_volume_bars'] += 1
            else:
                coverage['invalid_turnover_bars'] += 1
        enriched.append(updated)
    return enriched


def _run_daily_intraday_snapshot_followup(*, market_date: str, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {'triggered': False, 'reason': 'followup_disabled', 'market_date': market_date}
    from datetime import date

    from app.agent_context.eligibility import load_agent_feedback_eligible_symbols
    from app.core.settings import settings
    from app.market.kis_intraday_snapshots import kis_intraday_snapshot_service

    symbols = load_agent_feedback_eligible_symbols(market_date=date.fromisoformat(market_date))
    if not symbols:
        return {'triggered': False, 'reason': 'no_agent_feedback_eligible_symbols', 'market_date': market_date}
    result = asyncio.run(
        kis_intraday_snapshot_service.collect_snapshots(
            symbols=symbols,
            market_code=settings.kis_intraday_snapshot_market_code,
            max_ranking_pages=settings.kis_intraday_snapshot_max_ranking_pages,
            max_news_items=settings.kis_intraday_snapshot_news_limit,
            include_news=False,
        )
    )
    return {'triggered': True, 'market_date': market_date, **result}


def _run_daily_feedback_followup(
    *,
    market_date: str,
    enabled: bool,
    knowledge_cutoff_at: datetime | str | None = None,
    progress_callback: Callable[..., None] | None = None,
    point_in_time_recovery: bool = False,
    symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    from app.agent_context.roles import SUMMARIZER_AGENT_NAMES
    from app.agent_context.run_modes import FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH

    return _run_daily_agent_feedback_followup(
        stage_id='agent_feedback_summaries',
        market_date=market_date,
        enabled=enabled,
        roles=SUMMARIZER_AGENT_NAMES,
        feedback_scope_mode=FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        knowledge_cutoff_at=knowledge_cutoff_at,
        progress_callback=progress_callback,
        point_in_time_recovery=point_in_time_recovery,
        symbols=symbols,
    )


def _run_daily_signal_decider_followup(
    *,
    market_date: str,
    enabled: bool,
    knowledge_cutoff_at: datetime | str | None = None,
    progress_callback: Callable[..., None] | None = None,
    point_in_time_recovery: bool = False,
    symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
    from app.agent_context.run_modes import FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH

    return _run_daily_agent_feedback_followup(
        stage_id='signal_decider',
        market_date=market_date,
        enabled=enabled,
        roles=DECISION_SUPPORT_AGENT_NAMES,
        feedback_scope_mode=FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        knowledge_cutoff_at=knowledge_cutoff_at,
        progress_callback=progress_callback,
        point_in_time_recovery=point_in_time_recovery,
        symbols=symbols,
    )


def _run_daily_next_open_execution_plan_followup(*, market_date: str, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {
            'triggered': False,
            'reason': 'followup_disabled',
            'market_date': market_date,
        }

    from datetime import date, timedelta

    from app.orders.next_open_execution import signal_decider_next_open_execution_service

    expired_result = build_next_open_window_expired_result(
        market_date=market_date,
        market=get_market_state(),
    )
    if expired_result is not None:
        return expired_result
    target_trade_date = (date.fromisoformat(market_date) + timedelta(days=1)).isoformat()
    return signal_decider_next_open_execution_service.refresh_plan_items(
        target_trade_date=target_trade_date,
        trigger='eod_post_decider',
    )


def _run_daily_agent_feedback_followup(
    *,
    stage_id: str,
    market_date: str,
    enabled: bool,
    roles: tuple[str, ...],
    feedback_scope_mode: str,
    knowledge_cutoff_at: datetime | str | None = None,
    progress_callback: Callable[..., None] | None = None,
    point_in_time_recovery: bool = False,
    symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    if not enabled:
        return {
            'triggered': False,
            'reason': 'followup_disabled',
            'market_date': market_date,
        }

    expired_result = build_expired_decision_replay_result(
        stage_id=stage_id,
        market_date=market_date,
        market=get_market_state(),
    )
    if expired_result is not None:
        return expired_result

    from datetime import date

    from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
    from app.agent_context.retry_service import FeedbackRetryCycleConfig, agent_feedback_retry_service
    from app.core.settings import settings

    prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
        market_date=market_date,
        symbols=(
            list(dict.fromkeys(str(symbol) for symbol in symbols))
            if symbols is not None
            else None
        ),
    )
    prerequisites_ready = bool(prerequisites['ready'])
    if not prerequisites_ready:
        return {
            'triggered': False,
            'reason': 'feedback_prerequisites_not_ready',
            'status': prerequisites['status'],
            'market_date': market_date,
            'prerequisites': prerequisites,
        }

    result = agent_feedback_retry_service.run_retry_cycles(
        config=FeedbackRetryCycleConfig(
            market_date=date.fromisoformat(market_date),
            roles=roles,
            symbols=(
                tuple(dict.fromkeys(str(symbol) for symbol in symbols))
                if symbols is not None
                else None
            ),
            knowledge_cutoff_at=(
                knowledge_cutoff_at or datetime.now(timezone.utc)
            ),
            feedback_scope_mode=feedback_scope_mode,
            max_cycles=settings.openclaw_feedback_retry_max_cycles,
            outer_batch_size=settings.openclaw_feedback_retry_outer_batch_size,
            reuse_existing=True,
            point_in_time_recovery=point_in_time_recovery,
            progress_callback=progress_callback,
        ),
        bypass_prerequisite_gate=False,
    )
    expired_after_completion = build_expired_decision_replay_result(
        stage_id=stage_id,
        market_date=market_date,
        market=get_market_state(),
    )
    if expired_after_completion is not None:
        return {
            **expired_after_completion,
            'completed_after_action_window': True,
        }
    return {
        'triggered': True,
        'prerequisite_gate_bypassed': False,
        'prerequisites': None,
        **result,
    }

def _run_daily_signal_decider_evaluation_followup(*, market_date: str, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {
            'triggered': False,
            'reason': 'followup_disabled',
            'market_date': market_date,
        }

    expired_result = build_expired_decision_replay_result(
        stage_id='signal_decider_evaluations',
        market_date=market_date,
        market=get_market_state(),
    )
    if expired_result is not None:
        return expired_result

    from app.agent_context.signal_decider_evaluation_maintenance_service import (
        signal_decider_evaluation_maintenance_service,
    )

    result = signal_decider_evaluation_maintenance_service.refresh_recent_coverage(
        as_of_market_date=market_date,
        refresh_calendar=True,
        # Cover the longest 10-session outcome horizon plus weekends/holidays
        # without rebuilding the full 30-day evaluation window every day.
        calendar_lookback_days=21,
        trigger='eod_pykrx_followup',
    )
    expired_after_completion = build_expired_decision_replay_result(
        stage_id='signal_decider_evaluations',
        market_date=market_date,
        market=get_market_state(),
    )
    if expired_after_completion is not None:
        return {
            **expired_after_completion,
            'completed_after_action_window': True,
        }
    return result


def run_market_data_pipeline(
    service,
    *,
    dependencies: PykrxPipelineDependencies,
    start_date: str,
    end_date: str,
    markets: Sequence[str],
    trigger: str,
    refresh_universe: bool,
    mode: str,
    limit: int | None = None,
    symbol: str | None = None,
    symbols_override: Sequence[Any] | None = None,
    retry_context: dict | None = None,
    selected_stages: Sequence[str] | None = None,
    chain_fundamentals_followup: bool = True,
    track_daily_pipeline: bool = False,
    point_in_time_recovery: bool = False,
    provider_admission_fence_owned: bool = False,
    owning_pipeline_run_id: int | None = None,
) -> dict:
    feedback_knowledge_cutoff_at: datetime | None = None

    def resolve_feedback_knowledge_cutoff() -> datetime:
        nonlocal feedback_knowledge_cutoff_at
        if feedback_knowledge_cutoff_at is None:
            feedback_knowledge_cutoff_at = datetime.now(timezone.utc)
        return feedback_knowledge_cutoff_at

    normalized_start = dependencies.normalize_yyyymmdd(start_date)
    normalized_end = dependencies.normalize_yyyymmdd(end_date)
    normalized_owning_pipeline_run_id: int | None = None
    if owning_pipeline_run_id is not None:
        try:
            normalized_owning_pipeline_run_id = int(
                owning_pipeline_run_id
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                'owning_pipeline_run_id must be a positive integer'
            ) from exc
        if normalized_owning_pipeline_run_id <= 0:
            raise ValueError(
                'owning_pipeline_run_id must be a positive integer'
            )
        if track_daily_pipeline:
            raise ValueError(
                'owning_pipeline_run_id cannot be combined with '
                'track_daily_pipeline'
            )
    historical_backfill = mode == 'historical_backfill'
    # Production entry points resolve prior-date recovery before reaching this
    # low-level runner. Keeping this boundary explicit avoids making direct
    # fixture/simulation calls depend on the process wall clock.
    point_in_time_recovery = bool(point_in_time_recovery)
    point_in_time_scope = historical_backfill or point_in_time_recovery
    resolved_selected_stages = _resolve_requested_stages(
        mode=mode,
        selected_stages=selected_stages,
    )
    requested_stage_set = set(resolved_selected_stages)
    if mode == 'daily_auto' and chain_fundamentals_followup:
        # Signal features consume same-day factor tables. Treat the requested
        # stage as a chained follow-up so it is written once, after factors,
        # instead of persisting a stale pre-factor snapshot first.
        requested_stage_set.discard('signal_features')
    if historical_backfill and symbols_override is not None:
        raise ValueError(
            'symbols_override is not allowed for historical_backfill; '
            'membership must be derived from the requested end date.'
        )
    if historical_backfill and normalized_start != normalized_end:
        raise ValueError(
            'historical_backfill currently supports one market date per request; '
            'per-date universe membership is required before multi-day ranges are safe.'
        )
    if historical_backfill:
        # Historical requests never enter current-only DART/KIS/decider follow-ups,
        # even if an internal caller accidentally opts in.
        chain_fundamentals_followup = False
    dependencies.log_fetch(
        'run_collection',
        'started',
        mode=mode,
        trigger=trigger,
        start_date=normalized_start,
        end_date=normalized_end,
        markets=list(markets),
        limit=limit,
        symbol=symbol,
        symbols_override_count=len(symbols_override) if symbols_override is not None else None,
        refresh_universe=refresh_universe,
        retry=retry_context,
        selected_stages=list(resolved_selected_stages),
    )

    # Historical runs must always derive membership from the requested end date.
    # ``refresh_universe=False`` is safe for current daily jobs, but loading the
    # active table here would introduce survivorship bias into a backfill.
    effective_refresh_universe = (
        (refresh_universe or point_in_time_scope) and symbols_override is None
    )
    if symbols_override is not None:
        inherited_universe_snapshot = (
            (retry_context or {}).get('point_in_time_universe_snapshot')
            if point_in_time_recovery
            and isinstance(
                (retry_context or {}).get('point_in_time_universe_snapshot'),
                dict,
            )
            else (
                (retry_context or {}).get('inherited_universe_snapshot')
                if isinstance(
                    (retry_context or {}).get('inherited_universe_snapshot'),
                    dict,
                )
                else None
            )
        )
        universe_snapshot = (
            dict(inherited_universe_snapshot)
            if inherited_universe_snapshot is not None
            else None
        )
        symbols = list(symbols_override)
    elif effective_refresh_universe:
        snapshot = service.fetch_recent_universe_snapshot(
            markets=markets,
            # A requested symbol may sort beyond a caller-provided global cap;
            # inspect the complete point-in-time membership before filtering it.
            limit=None if point_in_time_scope and symbol else limit,
            end_date=normalized_end,
            # KIS master files represent the current listing set. Using them for
            # a historical cross-section silently introduces survivorship bias.
            allow_master_fallback=not point_in_time_scope,
        )
        if not snapshot.get('complete', True):
            unavailable_markets = ','.join(snapshot.get('unavailable_markets') or [])
            raise RuntimeError(
                'PyKRX universe snapshot is incomplete for requested markets: '
                f'{unavailable_markets or "unknown"}.'
            )
        symbols = snapshot['symbols']
        historical_snapshot = point_in_time_scope
        deactivate_missing = not historical_snapshot and limit is None and symbol is None
        persisted = (
            0
            if historical_snapshot
            else service._persist_universe(symbols, deactivate_missing=deactivate_missing)
        )
        market_membership = build_market_membership_snapshot(
            symbols=symbols,
            markets=markets,
        )
        if historical_snapshot:
            market_membership['symbol_metadata'] = [
                {
                    'symbol': item.symbol,
                    'market': item.market,
                    'name_ko': item.name_ko,
                    'standard_code': item.standard_code,
                }
                for item in symbols
            ]
        universe_snapshot = {
            'source': 'pykrx',
            'markets': list(markets),
            'symbol_count': len(symbols),
            'persisted_count': persisted,
            'latest_trade_date': snapshot['latest_trade_date'],
            'universe_source': snapshot['universe_source'],
            'membership_basis': snapshot.get('membership_basis', 'end_date_snapshot'),
            'membership_scope_warning': snapshot.get('membership_scope_warning'),
            'complete': snapshot.get('complete', True),
            'market_status': dict(snapshot.get('market_status') or {}),
            'unavailable_markets': list(snapshot.get('unavailable_markets') or []),
            'market_errors': list(snapshot.get('market_errors') or []),
            'deactivate_missing': deactivate_missing,
            'point_in_time': historical_snapshot,
            'current_universe_mutated': not historical_snapshot,
            'forced_for_point_in_time': historical_snapshot and not refresh_universe,
            'market_membership': market_membership,
        }
    else:
        universe_snapshot = None
        symbols = service.load_active_universe(markets=markets, symbol=symbol, limit=limit)

    if symbol:
        symbols = [item for item in symbols if item.symbol == symbol]
        if not symbols and not point_in_time_scope:
            matched = service.find_symbol(symbol, markets=markets)
            if matched is not None:
                symbols = [matched]
    if not symbols:
        raise RuntimeError('No active pykrx symbols are available for backfill.')

    latest_trade_date = service.resolve_latest_trade_date(end_date=normalized_end)
    requested_symbol_codes = [item.symbol for item in symbols]
    exact_piotroski_membership_symbols = (
        _validated_exact_piotroski_membership_symbols(
            universe_snapshot=universe_snapshot,
            market_date=latest_trade_date,
            markets=markets,
            limited_run=limit is not None or symbol is not None,
        )
    )
    daily_derived_market_scope_authority: dict[str, Any] | None = None
    if (
        exact_piotroski_membership_symbols is not None
        and not point_in_time_scope
    ):
        membership_snapshot = (
            universe_snapshot.get('market_membership')
            if isinstance(universe_snapshot, Mapping)
            else None
        )
        if isinstance(membership_snapshot, Mapping):
            daily_derived_market_scope_authority = (
                build_daily_derived_market_scope_authority(
                    market_date=latest_trade_date,
                    exact_symbols=exact_piotroski_membership_symbols,
                    market_membership_snapshot=membership_snapshot,
                )
            )
    invocation_scope_proof = _build_pykrx_invocation_scope_proof(
        requested_symbol_codes
    )
    inherited_downstream_symbols = list(
        dict.fromkeys(
            str(symbol).strip()
            for symbol in (
                (retry_context or {}).get(
                    'inherited_downstream_retry_symbols'
                )
                or []
            )
            if str(symbol).strip()
        )
    )
    inherited_downstream_resume_from_stage = str(
        (retry_context or {}).get(
            'inherited_downstream_retry_resume_from_stage'
        )
        or ''
    ).strip()
    if inherited_downstream_resume_from_stage not in {
        stage_id
        for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
        if stage_id != 'price_collection'
    }:
        inherited_downstream_resume_from_stage = ''
    unattempted_symbols = list(requested_symbol_codes)
    run_metadata = {
        'mode': mode,
        'point_in_time_recovery': point_in_time_recovery,
        'requested_start_date': normalized_start,
        'requested_end_date': normalized_end,
        'latest_available_trade_date': latest_trade_date,
        'markets': list(markets),
        'symbol': symbol,
        'limited_run': limit is not None,
        'universe_refreshed': effective_refresh_universe,
        'universe_snapshot': universe_snapshot,
        # Persist the complete admission scope before the first provider call.
        # If a provider outage triggers the bounded early abort, the scheduler
        # can distinguish never-attempted symbols from attempted failures.
        'requested_symbols': requested_symbol_codes,
        **invocation_scope_proof,
        'failed_symbols': [],
        'failed_symbols_count': 0,
        'unattempted_symbols': unattempted_symbols,
    }
    if normalized_owning_pipeline_run_id is not None:
        run_metadata['daily_pipeline_run_id'] = (
            normalized_owning_pipeline_run_id
        )
    if retry_context is not None:
        run_metadata['retry'] = retry_context
    if inherited_downstream_symbols:
        run_metadata['inherited_downstream_retry_symbols'] = (
            inherited_downstream_symbols
        )
        run_metadata['inherited_downstream_retry_symbols_count'] = len(
            inherited_downstream_symbols
        )
        # Canonical scope must exist from EOD creation onward so every early
        # failure path carries it into the next scheduler attempt.
        run_metadata['downstream_retry_symbols'] = (
            inherited_downstream_symbols
        )
        run_metadata['downstream_retry_symbols_count'] = len(
            inherited_downstream_symbols
        )
        if inherited_downstream_resume_from_stage:
            run_metadata['downstream_retry_resume_from_stage'] = (
                inherited_downstream_resume_from_stage
            )
    first_symbol = symbols[0].symbol if symbols else None
    run_metadata = dependencies.update_run_stage_metadata(
        run_metadata,
        stage='price_collection',
        status='running',
        current_step='fetch_symbol_ohlcv',
        current_activity=(
            f'Collecting pykrx daily prices for {first_symbol}'
            if first_symbol
            else 'Collecting pykrx daily prices'
        ),
        processed=0,
        total=len(symbols),
        succeeded=0,
        failed=0,
        current_symbol=first_symbol,
        extra={'bars_upserted': 0},
    )
    tracked_stages = _resolve_auto_daily_tracked_stages(
        selected_stages=resolved_selected_stages,
        chain_fundamentals_followup=chain_fundamentals_followup,
    )
    # Keep the intended stage contract on the EOD row itself so restart
    # recovery can prove which follow-ups remain after EOD finalization.
    run_metadata['expected_daily_pipeline_stages'] = list(tracked_stages)
    daily_pipeline_tracker = _OfficialDailyPipelineTracker(
        service=dependencies.daily_pipeline_run_state_service if track_daily_pipeline else None,
        log_fetch=dependencies.log_fetch,
    )
    pipeline_admission = daily_pipeline_tracker.start(
        market_date=latest_trade_date,
        trigger=trigger,
        requested_stages=tracked_stages,
        resolved_stages=tracked_stages,
        eod_run_id=None,
        run_metadata={
            'mode': mode,
            'trigger': trigger,
            'markets': list(markets),
            'selected_stages': list(resolved_selected_stages),
            'chain_fundamentals_followup': chain_fundamentals_followup,
            'retry': retry_context,
        },
        provider_admission_fence_owned=provider_admission_fence_owned,
    )
    if pipeline_admission and pipeline_admission.get('deduplicated'):
        return {
            'triggered': False,
            'reason': pipeline_admission.get('deduplication_reason') or 'overlapping_run_already_active',
            'source': 'pykrx',
            'status': 'skipped',
            'selected_stages': list(resolved_selected_stages),
            'markets': list(markets),
            'start_date': normalized_start,
            'requested_end_date': normalized_end,
            'latest_available_trade_date': latest_trade_date,
            'pipeline_run_id': pipeline_admission.get('id'),
            'pipeline_run': pipeline_admission,
        }

    if daily_pipeline_tracker.pipeline_run_id is not None:
        run_metadata['daily_pipeline_run_id'] = (
            daily_pipeline_tracker.pipeline_run_id
        )

    try:
        run_id = dependencies.create_eod_collection_run(
            market='KRX',
            trade_date=latest_trade_date,
            symbols_total=len(symbols),
            trigger=trigger,
            source='pykrx',
            run_metadata=run_metadata,
        )
    except Exception as exc:
        daily_pipeline_tracker.finish_run_preserving_primary_error(
            error_message=str(exc)
        )
        raise

    symbols_succeeded = 0
    symbols_failed = 0
    failed_symbols: list[str] = []
    provider_failure_count = 0
    price_failure_types: set[str] = set()
    early_abort_triggered = False
    bars_upserted = 0

    def terminalize_tracking_failure(
        exc: Exception,
        *,
        current_step: str,
    ) -> None:
        """Best-effort cleanup without masking the critical checkpoint error."""

        nonlocal run_metadata
        run_metadata = dependencies.update_run_stage_metadata(
            run_metadata,
            stage='price_collection',
            status='failed',
            current_step=current_step,
            current_activity='Official daily pipeline tracking failed',
            processed=symbols_succeeded + symbols_failed,
            total=len(symbols),
            succeeded=symbols_succeeded,
            failed=symbols_failed,
            current_symbol=first_symbol,
            extra={
                'tracking_checkpoint_failed': True,
                'error_type': type(exc).__name__,
                'bars_upserted': bars_upserted,
            },
        )
        try:
            dependencies.complete_eod_collection_run(
                run_id,
                status='failed',
                symbols_succeeded=symbols_succeeded,
                symbols_failed=symbols_failed,
                error_message=str(exc),
                run_metadata=run_metadata,
            )
        except Exception as cleanup_exc:  # pragma: no cover - defensive log
            logger.warning(
                'EOD cleanup failed after daily pipeline tracking setup '
                'failure for run %s: %s',
                run_id,
                cleanup_exc,
            )
        try:
            daily_pipeline_tracker.finish_run(
                status='failed',
                error_message=str(exc),
            )
        except Exception as cleanup_exc:  # bounded retry already occurred
            logger.warning(
                'Official pipeline cleanup failed after tracking setup '
                'failure for run %s: %s',
                run_id,
                cleanup_exc,
            )

    def complete_eod_run_or_fail_tracking(**kwargs: Any) -> None:
        try:
            dependencies.complete_eod_collection_run(run_id, **kwargs)
        except Exception as exc:
            daily_pipeline_tracker.finish_run_preserving_primary_error(
                error_message=str(exc)
            )
            raise

    try:
        if normalized_owning_pipeline_run_id is not None:
            dependencies.daily_pipeline_run_state_service.link_eod_run(
                pipeline_run_id=normalized_owning_pipeline_run_id,
                eod_run_id=run_id,
            )
        else:
            daily_pipeline_tracker.link_eod_run(run_id)
    except Exception as exc:
        terminalize_tracking_failure(
            exc,
            current_step='daily_pipeline_link_failed',
        )
        raise

    def persist_stage_progress(
        *,
        stage: str,
        status: str,
        current_step: str,
        current_activity: str,
        processed: int | None = None,
        total: int | None = None,
        succeeded: int | None = None,
        failed: int | None = None,
        current_symbol: str | None = None,
        current_period: str | None = None,
        source_run_ids: dict[str, Any] | None = None,
        extra: dict | None = None,
    ) -> None:
        nonlocal run_metadata
        metadata_extra = dict(extra or {})
        if source_run_ids:
            metadata_extra['source_run_ids'] = dict(source_run_ids)
        run_metadata = dependencies.update_run_stage_metadata(
            run_metadata,
            stage=stage,
            status=status,
            current_step=current_step,
            current_activity=current_activity,
            processed=processed,
            total=total,
            succeeded=succeeded,
            failed=failed,
            current_symbol=current_symbol,
            current_period=current_period,
            extra=metadata_extra,
        )
        try:
            daily_pipeline_tracker.update_stage(
                stage=stage,
                status=status,
                current_step=current_step,
                current_activity=current_activity,
                processed=processed,
                total=total,
                succeeded=succeeded,
                failed=failed,
                current_symbol=current_symbol,
                current_period=current_period,
                source_run_ids=source_run_ids,
                details=metadata_extra,
            )
        except DailyPipelineTrackingWriteError as exc:
            terminalize_tracking_failure(
                exc,
                current_step=f'{stage}_tracking_failed',
            )
            raise
        try:
            dependencies.update_eod_collection_run_progress(
                run_id,
                symbols_succeeded=symbols_succeeded,
                symbols_failed=symbols_failed,
                run_metadata=run_metadata,
            )
        except Exception as exc:
            logger.warning('pykrx progress update failed for run %s during %s: %s', run_id, stage, exc)
            dependencies.log_fetch(
                'update_collection_progress',
                'failed',
                run_id=run_id,
                stage=stage,
                current_step=current_step,
                current_symbol=current_symbol,
                current_period=current_period,
                symbols_succeeded=symbols_succeeded,
                symbols_failed=symbols_failed,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

    persist_stage_progress(
        stage='price_collection',
        status='running',
        current_step='fetch_symbol_ohlcv',
        current_activity=(
            f'Collecting pykrx daily prices for {first_symbol}'
            if first_symbol
            else 'Collecting pykrx daily prices'
        ),
        processed=0,
        total=len(symbols),
        succeeded=0,
        failed=0,
        current_symbol=first_symbol,
        extra={'bars_upserted': 0},
    )

    try:
        market_turnover_cache: dict[tuple[str, str], Any] = {}
        turnover_coverage_counters: dict[str, int] = {}
        turnover_invalid_symbols: list[str] = []
        for index, item in enumerate(symbols):
            symbol_started_at = time.perf_counter()
            try:
                frame = dependencies.get_market_ohlcv_by_date(normalized_start, normalized_end, item.symbol)
                if frame.empty:
                    raise RuntimeError('No OHLCV rows returned.')
                fundamentals_frame = None
                if mode not in {'daily_auto', 'daily_stage_run'}:
                    fundamentals_frame = service._load_symbol_fundamentals(
                        symbol=item.symbol,
                        start_date=normalized_start,
                        end_date=normalized_end,
                    )
                bars = service._map_symbol_bars(frame, fundamentals_frame=fundamentals_frame)
                invalid_turnover_before = int(
                    turnover_coverage_counters.get('invalid_turnover_bars') or 0
                )
                bars = _enrich_bars_with_market_turnover(
                    bars=bars,
                    symbol=item.symbol,
                    market=item.market,
                    dependencies=dependencies,
                    market_turnover_cache=market_turnover_cache,
                    turnover_coverage=turnover_coverage_counters,
                    run_id=run_id,
                )
                if (
                    int(turnover_coverage_counters.get('invalid_turnover_bars') or 0)
                    > invalid_turnover_before
                ):
                    turnover_invalid_symbols.append(item.symbol)
                persisted_count = dependencies.persist_daily_price_bars(
                    item.symbol,
                    bars,
                    market=item.market,
                    source='pykrx',
                    run_id=run_id,
                    name_ko=item.name_ko,
                    standard_code=item.standard_code,
                    update_current_symbol_state=not point_in_time_scope,
                )
                bars_upserted += persisted_count
                symbols_succeeded += 1
                dependencies.log_fetch(
                    'fetch_symbol_ohlcv',
                    'completed',
                    run_id=run_id,
                    symbol=item.symbol,
                    market=item.market,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    bar_count=len(bars),
                    persisted_count=persisted_count,
                    duration_ms=round((time.perf_counter() - symbol_started_at) * 1000, 3),
                )
            except DailyPipelineTrackingWriteError:
                raise
            except Exception as exc:
                symbols_failed += 1
                failed_symbols.append(item.symbol)
                price_failure_types.add(type(exc).__name__)
                if _is_pykrx_provider_failure(exc):
                    provider_failure_count += 1
                logger.warning('pykrx backfill failed for %s: %s', item.symbol, exc)
                dependencies.log_fetch(
                    'fetch_symbol_ohlcv',
                    'failed',
                    run_id=run_id,
                    symbol=item.symbol,
                    market=item.market,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    duration_ms=round((time.perf_counter() - symbol_started_at) * 1000, 3),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                if (
                    mode == 'daily_auto'
                    and symbols_succeeded == 0
                    and symbols_failed >= min(PYKRX_PROVIDER_OUTAGE_EARLY_ABORT_FAILURES, len(symbols))
                ):
                    dependencies.log_fetch(
                        'run_collection',
                        'failed',
                        run_id=run_id,
                        mode=mode,
                        trigger=trigger,
                        start_date=normalized_start,
                        end_date=normalized_end,
                        latest_available_trade_date=latest_trade_date,
                        symbols_total=len(symbols),
                        symbols_succeeded=symbols_succeeded,
                        symbols_failed=symbols_failed,
                        bars_upserted=bars_upserted,
                        error_type='PykrxProviderUnavailable',
                        error_message=(
                            'Aborting daily PyKRX price collection after initial consecutive failures; '
                            'provider appears unavailable.'
                        ),
                    )
                    run_metadata = service._run_metadata_with_failed_symbols(
                        run_metadata,
                        failed_symbols,
                    )
                    unattempted_symbols = requested_symbol_codes[index + 1 :]
                    run_metadata['unattempted_symbols'] = unattempted_symbols
                    run_metadata['unattempted_symbols_count'] = len(
                        unattempted_symbols
                    )
                    early_abort_triggered = True
                    break
            unattempted_symbols = requested_symbol_codes[index + 1 :]
            run_metadata['unattempted_symbols'] = unattempted_symbols
            run_metadata = service._run_metadata_with_failed_symbols(
                run_metadata,
                failed_symbols,
            )
            try:
                next_symbol = symbols[index + 1].symbol if index + 1 < len(symbols) else None
                turnover_coverage = _summarize_turnover_coverage(
                    turnover_coverage_counters
                )
                persist_stage_progress(
                    stage='price_collection',
                    status='running'
                    if next_symbol is not None
                    else (
                        'partial'
                        if turnover_coverage['status'] == 'partial'
                        else _completion_status_for_gap(failed=symbols_failed, total=len(symbols))
                    ),
                    current_step='fetch_symbol_ohlcv' if next_symbol is not None else 'price_collection_completed',
                    current_activity=(
                        f'Collecting pykrx daily prices for {next_symbol}'
                        if next_symbol is not None
                        else 'Completed pykrx daily price collection'
                    ),
                    processed=symbols_succeeded + symbols_failed,
                    total=len(symbols),
                    succeeded=symbols_succeeded,
                    failed=symbols_failed,
                    current_symbol=next_symbol,
                    extra={
                        'bars_upserted': bars_upserted,
                        'turnover_coverage': turnover_coverage,
                        **_gap_policy_details(failed=symbols_failed, total=len(symbols)),
                    },
                )
            except DailyPipelineTrackingWriteError:
                raise
            except Exception as exc:
                logger.warning('pykrx progress update failed for run %s after %s: %s', run_id, item.symbol, exc)
                dependencies.log_fetch(
                    'update_collection_progress',
                    'failed',
                    run_id=run_id,
                    symbol=item.symbol,
                    symbols_succeeded=symbols_succeeded,
                    symbols_failed=symbols_failed,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if service.symbol_pause_seconds:
                time.sleep(service.symbol_pause_seconds)
    except Exception as exc:
        dependencies.log_fetch(
            'run_collection',
            'failed',
            run_id=run_id,
            mode=mode,
            trigger=trigger,
            start_date=normalized_start,
            end_date=normalized_end,
            latest_available_trade_date=latest_trade_date,
            symbols_total=len(symbols),
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            bars_upserted=bars_upserted,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        run_metadata = dependencies.update_run_stage_metadata(
            run_metadata,
            stage='price_collection',
            status='failed',
            current_step='price_collection_failed',
            current_activity='pykrx daily price collection failed',
            processed=symbols_succeeded + symbols_failed,
            total=len(symbols),
            succeeded=symbols_succeeded,
            failed=symbols_failed,
            current_symbol=None,
            extra={'bars_upserted': bars_upserted},
        )
        if failed_symbols:
            run_metadata = service._run_metadata_with_failed_symbols(run_metadata, failed_symbols)
        try:
            dependencies.complete_eod_collection_run(
                run_id,
                status='failed',
                symbols_succeeded=symbols_succeeded,
                symbols_failed=symbols_failed,
                error_message=str(exc),
                run_metadata=run_metadata,
            )
        except Exception as cleanup_exc:
            logger.warning(
                'EOD cleanup failed after price collection error for run '
                '%s: %s',
                run_id,
                cleanup_exc,
            )
        daily_pipeline_tracker.finish_run_preserving_primary_error(
            error_message=str(exc)
        )
        raise

    turnover_coverage = _summarize_turnover_coverage(
        turnover_coverage_counters
    )
    scoped_retry_with_existing_coverage = bool(
        retry_context
        and retry_context.get('retry_of_run_id') is not None
        and int(retry_context.get('baseline_symbols_succeeded') or 0) > 0
    )
    if (
        mode == 'daily_auto'
        and symbols_succeeded == 0
        and symbols_failed > 0
        and scoped_retry_with_existing_coverage
    ):
        unresolved_symbols = list(
            dict.fromkeys([*failed_symbols, *unattempted_symbols])
        )
        provider_outage_suspected = bool(
            early_abort_triggered
            and symbols_failed >= PYKRX_PROVIDER_OUTAGE_EARLY_ABORT_FAILURES
            and provider_failure_count == symbols_failed
        )
        retry_scope_status = (
            'provider_outage' if provider_outage_suspected else 'partial'
        )
        provider_outage_evidence = {
            'early_abort_triggered': early_abort_triggered,
            'all_attempted_failures_provider_related': bool(
                symbols_failed > 0
                and provider_failure_count == symbols_failed
            ),
            'attempted_failure_count': symbols_failed,
            'provider_failure_count': provider_failure_count,
            'unattempted_count': len(unattempted_symbols),
            'failure_types': sorted(price_failure_types),
        }
        if provider_outage_suspected:
            error_message = (
                'PyKRX bounded retry stopped after '
                f'{symbols_failed} attempted failures with provider-outage '
                f'evidence and {len(unattempted_symbols)} unattempted symbols; '
                'the prior broad-run coverage remains usable while all '
                f'{len(unresolved_symbols)} symbols stay unresolved.'
            )
        else:
            error_message = (
                'PyKRX failed for every symbol in the bounded retry scope; '
                'the prior broad-run coverage remains usable while these symbols '
                'stay unresolved.'
            )
        run_metadata = service._run_metadata_with_failed_symbols(
            run_metadata,
            failed_symbols,
        )
        run_metadata['unattempted_symbols'] = list(unattempted_symbols)
        run_metadata['unattempted_symbols_count'] = len(unattempted_symbols)
        run_metadata['unresolved_symbols'] = list(unresolved_symbols)
        run_metadata['unresolved_symbols_count'] = len(unresolved_symbols)
        run_metadata['provider_outage_suspected'] = provider_outage_suspected
        run_metadata['provider_outage_evidence'] = provider_outage_evidence
        run_metadata['retry_scope_status'] = retry_scope_status
        persist_stage_progress(
            stage='price_collection',
            status='partial',
            current_step='price_collection_retry_scope_unresolved',
            current_activity=error_message,
            processed=symbols_failed,
            total=len(symbols),
            succeeded=0,
            failed=symbols_failed,
            current_symbol=None,
            extra={
                'bars_upserted': bars_upserted,
                'provider_outage_suspected': provider_outage_suspected,
                'provider_outage_evidence': provider_outage_evidence,
                'downstream_stages_skipped': True,
                'unattempted_symbols': list(unattempted_symbols),
                'unattempted_symbols_count': len(unattempted_symbols),
                'unresolved_symbols': list(unresolved_symbols),
                'unresolved_symbols_count': len(unresolved_symbols),
                'retry_scope_status': retry_scope_status,
            },
        )
        complete_eod_run_or_fail_tracking(
            status='completed_with_errors',
            symbols_succeeded=0,
            symbols_failed=symbols_failed,
            error_message=error_message,
            run_metadata=run_metadata,
        )
        daily_pipeline_tracker.finish_run(
            status='partial',
            error_message=error_message,
        )
        return {
            'run_id': run_id,
            'source': 'pykrx',
            'status': 'completed_with_errors',
            'selected_stages': list(resolved_selected_stages),
            'markets': list(markets),
            'start_date': normalized_start,
            'requested_end_date': normalized_end,
            'latest_available_trade_date': latest_trade_date,
            'symbols_total': len(symbols),
            'symbols_succeeded': 0,
            'symbols_failed': symbols_failed,
            'failed_symbols': failed_symbols[:50],
            'unresolved_symbols': unresolved_symbols,
            'unresolved_symbols_count': len(unresolved_symbols),
            'unattempted_symbols': unattempted_symbols[:50],
            'unattempted_symbols_count': len(unattempted_symbols),
            'bars_upserted': bars_upserted,
            'turnover_coverage': turnover_coverage,
            'turnover_invalid_symbols': turnover_invalid_symbols[:50],
            'turnover_invalid_symbols_count': len(turnover_invalid_symbols),
            'error_type': 'PykrxRetryScopeUnresolved',
            'error_message': error_message,
            'provider_outage_suspected': provider_outage_suspected,
            'provider_outage_evidence': provider_outage_evidence,
            'retry_scope_status': retry_scope_status,
            'retry': dict(retry_context or {}),
            'refresh_universe': effective_refresh_universe,
            'universe_snapshot': universe_snapshot,
        }
    if mode == 'daily_auto' and symbols_succeeded == 0 and symbols_failed > 0:
        error_message = (
            'PyKRX daily price collection failed for every requested symbol; '
            'provider appears unavailable, so downstream EOD stages were not run.'
        )
        dependencies.log_fetch(
            'run_collection',
            'failed',
            run_id=run_id,
            mode=mode,
            trigger=trigger,
            start_date=normalized_start,
            end_date=normalized_end,
            latest_available_trade_date=latest_trade_date,
            symbols_total=len(symbols),
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            bars_upserted=bars_upserted,
            error_type='PykrxProviderUnavailable',
            error_message=error_message,
        )
        persist_stage_progress(
            stage='price_collection',
            status='failed',
            current_step='price_collection_provider_unavailable',
            current_activity=error_message,
            processed=symbols_failed,
            total=len(symbols),
            succeeded=0,
            failed=symbols_failed,
            current_symbol=None,
            extra={
                'bars_upserted': bars_upserted,
                'provider_outage_suspected': True,
                'downstream_stages_skipped': True,
            },
        )
        run_metadata = service._run_metadata_with_failed_symbols(run_metadata, failed_symbols)
        complete_eod_run_or_fail_tracking(
            status='failed',
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            error_message=error_message,
            run_metadata=run_metadata,
        )
        daily_pipeline_tracker.finish_run(status='failed', error_message=error_message)
        return {
            'run_id': run_id,
            'source': 'pykrx',
            'status': 'failed',
            'selected_stages': list(resolved_selected_stages),
            'markets': list(markets),
            'start_date': normalized_start,
            'requested_end_date': normalized_end,
            'latest_available_trade_date': latest_trade_date,
            'symbols_total': len(symbols),
            'symbols_succeeded': symbols_succeeded,
            'symbols_failed': symbols_failed,
            'failed_symbols': failed_symbols[:50],
            'unattempted_symbols': unattempted_symbols[:50],
            'unattempted_symbols_count': len(unattempted_symbols),
            'bars_upserted': bars_upserted,
            'turnover_coverage': turnover_coverage,
            'turnover_invalid_symbols': turnover_invalid_symbols[:50],
            'turnover_invalid_symbols_count': len(turnover_invalid_symbols),
            'error_type': 'PykrxProviderUnavailable',
            'error_message': error_message,
            'refresh_universe': effective_refresh_universe,
            'universe_snapshot': universe_snapshot,
        }

    failed_symbol_set = set(failed_symbols)
    price_context_symbols = [item.symbol for item in symbols if item.symbol not in failed_symbol_set]
    causal_price_skip = (
        {
            'causal_skip_reason': 'fresh_price_unavailable',
            'causally_skipped_symbols': failed_symbols[:50],
            'causally_skipped_symbols_count': len(failed_symbols),
        }
        if failed_symbols
        else {}
    )
    price_context_result: dict
    price_context_error_message: str | None = None
    current_context_successes: list[str] = []
    context_scope_is_exact = False
    if requested_symbol_codes:
        try:
            price_context_result = dependencies.price_context_service.rebuild_for_market_date(
                market_date=latest_trade_date,
                symbols=price_context_symbols,
                exact_scope_symbols=requested_symbol_codes,
                source='pykrx',
                run_id=run_id,
                include_retry_metadata=True,
                authoritative_market_scope=(
                    daily_derived_market_scope_authority
                ),
            )
            if failed_symbols:
                price_context_result.update(causal_price_skip)
        except Exception as exc:
            price_context_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='pykrx',
                error=exc,
                totals={
                    'requested_symbols': len(price_context_symbols),
                    'contexts_persisted': 0,
                    'contexts_failed': len(price_context_symbols),
                },
                failed_symbols=price_context_symbols,
                items_key='items',
            )
            price_context_result['_retry_metadata'] = {
                'failed_symbols': list(price_context_symbols),
            }
            price_context_error_message = f'Price context persistence failed: {exc}'
    else:
        price_context_result = {
            'skipped': True,
            'status': 'skipped',
            'reason': 'no_fresh_price_rows',
            'market_date': latest_trade_date,
            'source': 'pykrx',
            'requested_symbols': 0,
            'contexts_persisted': 0,
            'contexts_failed': 0,
            **causal_price_skip,
        }

    if requested_symbol_codes:
        context_partition = _validated_exact_scope_outcome(
            result=price_context_result,
            invocation_symbols=requested_symbol_codes,
            status=_resolve_result_status(
                price_context_result,
                stage_id='technical_snapshots',
            ),
        )
        context_finalization = price_context_result.get(
            'scope_finalization'
        )
        context_scope_is_exact = bool(
            context_partition is not None
            and context_partition['manifest_version']
            == 'price_contexts_scope_outcomes_v1'
            and isinstance(context_finalization, Mapping)
            and context_finalization.get('ready') is True
        )
        current_context_successes = (
            context_partition['successful_stock_codes']
            if context_scope_is_exact
            else []
        )
        if (
            not context_scope_is_exact
            and price_context_error_message is None
        ):
            price_context_error_message = (
                'Price context persistence did not return an exact '
                'atomically finalized scope; dependent derived stages '
                'were blocked.'
            )

    pykrx_enrichment_result: dict
    pykrx_enrichment_error_message: str | None = None
    if 'pykrx_enrichment' in requested_stage_set:
        try:
            persist_stage_progress(
                stage='pykrx_enrichment',
                status='running',
                current_step='collecting_pykrx_enrichment',
                current_activity='Collecting PyKRX enrichment categories',
                processed=0,
                total=len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                succeeded=0,
                failed=0,
                extra={
                    'categories_requested': list(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                    'required_downstream_categories': list(REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES),
                },
            )
            enrichment_kwargs: dict[str, Any] = {
                'market_date': latest_trade_date,
                'markets': markets,
                'symbols': [item.symbol for item in symbols],
                'use_full_market_coverage': True,
                'daily_price_bar_lock_owned': True,
            }
            if point_in_time_recovery:
                enrichment_kwargs['point_in_time_recovery'] = True
            pykrx_enrichment_result = dependencies.pykrx_enrichment_service.collect_for_market_date(
                **enrichment_kwargs
            )
            preflight = build_pykrx_enrichment_preflight(
                market_date=latest_trade_date,
                coverage=dict(pykrx_enrichment_result.get('coverage') or {}),
                categories=REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES,
            )
            pykrx_enrichment_result['downstream_preflight'] = preflight
            category_results = (
                pykrx_enrichment_result.get('categories')
                if isinstance(pykrx_enrichment_result.get('categories'), dict)
                else {}
            )
            category_row_counts = {
                category: int((details or {}).get('rows_persisted') or 0)
                for category, details in category_results.items()
                if isinstance(details, dict)
            }
            coverage_categories = (
                pykrx_enrichment_result.get('coverage', {}).get('categories')
                if isinstance(pykrx_enrichment_result.get('coverage'), dict)
                and isinstance(pykrx_enrichment_result.get('coverage', {}).get('categories'), dict)
                else {}
            )
            category_classification = _classify_pykrx_enrichment_categories(
                category_results=category_results,
                coverage_categories=coverage_categories,
                preflight_failed_categories=preflight.get('failed_categories') or [],
            )
            provider_error_categories = category_classification['provider_error_categories']
            component_retry_debt_categories = category_classification[
                'component_retry_debt_categories'
            ]
            recovered_categories = category_classification['recovered_categories']
            failed_categories = category_classification['unresolved_categories']
            pykrx_enrichment_result['provider_error_categories'] = provider_error_categories
            pykrx_enrichment_result['component_retry_debt_categories'] = (
                component_retry_debt_categories
            )
            pykrx_enrichment_result['recovered_categories'] = recovered_categories
            pykrx_enrichment_result['unresolved_categories'] = failed_categories
            (
                blocking_failed_categories,
                tolerated_failed_categories,
            ) = _partition_pykrx_enrichment_category_gaps(
                failed_categories=failed_categories,
                preflight=preflight,
            )
            pykrx_enrichment_result['blocking_failed_categories'] = (
                blocking_failed_categories
            )
            pykrx_enrichment_result['tolerated_failed_categories'] = (
                tolerated_failed_categories
            )
            stage_status = (
                'completed'
                if preflight.get('allowed') and not blocking_failed_categories
                else 'partial'
            )
            if component_retry_debt_categories:
                stage_status = 'partial'
            (
                exact_scope_outcome,
                exact_scope_retry_reasons,
            ) = _build_pykrx_enrichment_symbol_scope_outcome(
                selected_symbols=requested_symbol_codes,
                coverage_categories=coverage_categories,
            )
            exact_retry_symbols = list(
                exact_scope_outcome['retry_debt_stock_codes']
            )
            component_retry_symbols = _pykrx_component_retry_symbols(
                result=pykrx_enrichment_result,
                requested_symbols=requested_symbol_codes,
            )
            if exact_retry_symbols:
                stage_status = 'partial'
            pykrx_enrichment_result.update(exact_scope_outcome)
            pykrx_enrichment_result['exact_scope_retry_reasons'] = (
                exact_scope_retry_reasons
            )
            pykrx_enrichment_result['status'] = stage_status
            enrichment_failed_count = len(blocking_failed_categories)
            if stage_status != 'completed':
                if blocking_failed_categories:
                    pykrx_enrichment_error_message = (
                        'PyKRX enrichment incomplete for categories: '
                        + ','.join(blocking_failed_categories)
                    )
                elif component_retry_debt_categories:
                    pykrx_enrichment_error_message = (
                        'PyKRX enrichment has non-blocking component retry '
                        'debt: '
                        + ','.join(component_retry_debt_categories)
                    )
                else:
                    pykrx_enrichment_error_message = (
                        'PyKRX enrichment has symbol-level coverage debt'
                    )
                retry_symbols = list(
                    dict.fromkeys(
                        [*exact_retry_symbols, *component_retry_symbols]
                    )
                )
                if retry_symbols:
                    pykrx_enrichment_result['_retry_metadata'] = {
                        'failed_symbols': retry_symbols,
                    }
            persist_stage_progress(
                stage='pykrx_enrichment',
                status=stage_status,
                current_step='pykrx_enrichment_completed',
                current_activity=(
                    'PyKRX enrichment collected'
                    if stage_status == 'completed'
                    else 'PyKRX enrichment collected with coverage gaps'
                ),
                processed=len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                total=len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                succeeded=max(len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES) - enrichment_failed_count, 0),
                failed=enrichment_failed_count,
                extra={
                    'categories_requested': pykrx_enrichment_result.get('categories_requested'),
                    'category_row_counts': category_row_counts,
                    'failed_categories': failed_categories,
                    'provider_error_categories': provider_error_categories,
                    'component_retry_debt_categories': (
                        component_retry_debt_categories
                    ),
                    'component_retry_debt': pykrx_enrichment_result.get(
                        'component_retry_debt'
                    )
                    or [],
                    'recovered_categories': recovered_categories,
                    'blocking_failed_categories': blocking_failed_categories,
                    'tolerated_failed_categories': tolerated_failed_categories,
                    'minor_missing_gap_policy': {
                        'max_non_blocking_missing_ratio': MAX_NON_BLOCKING_STAGE_MISSING_RATIO,
                    },
                    **exact_scope_outcome,
                    'downstream_preflight_status': preflight.get('status'),
                    'downstream_allowed': bool(preflight.get('allowed')),
                    'zero_coverage_categories': preflight.get('zero_coverage_categories'),
                    'zero_decision_usable_categories': preflight.get(
                        'zero_decision_usable_categories'
                    ),
                },
            )
            dependencies.log_fetch(
                'pykrx_enrichment',
                stage_status,
                run_id=run_id,
                market_date=latest_trade_date,
                categories_requested=pykrx_enrichment_result.get('categories_requested'),
                category_row_counts=category_row_counts,
                failed_categories=failed_categories,
                provider_error_categories=provider_error_categories,
                component_retry_debt_categories=(
                    component_retry_debt_categories
                ),
                recovered_categories=recovered_categories,
                blocking_failed_categories=blocking_failed_categories,
                tolerated_failed_categories=tolerated_failed_categories,
                downstream_preflight_status=preflight.get('status'),
                downstream_allowed=bool(preflight.get('allowed')),
            )
        except Exception as exc:
            pykrx_enrichment_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='pykrx_enrichment',
                error=exc,
                totals={
                    'categories_requested': list(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                    'categories_failed': len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                },
            )
            pykrx_enrichment_result['_retry_metadata'] = {
                'failed_symbols': list(requested_symbol_codes),
            }
            pykrx_enrichment_result.update(
                _build_exact_scope_outcome(
                    selected_symbols=requested_symbol_codes,
                    successful_symbols=[],
                    retry_debt_symbols=requested_symbol_codes,
                )
            )
            pykrx_enrichment_error_message = f'PyKRX enrichment collection failed: {exc}'
            persist_stage_progress(
                stage='pykrx_enrichment',
                status='failed',
                current_step='pykrx_enrichment_failed',
                current_activity='PyKRX enrichment collection failed',
                processed=0,
                total=len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                succeeded=0,
                failed=len(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                extra={
                    'categories_requested': list(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES),
                    'required_downstream_categories': list(REQUIRED_EXEC_DECIDER_PYKRX_ENRICHMENT_CATEGORIES),
                },
            )
    else:
        pykrx_enrichment_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': 'pykrx_enrichment',
        }

    technical_snapshot_result: dict
    snapshot_error_message: str | None = None
    if (
        'technical_snapshots' in requested_stage_set
        and context_scope_is_exact
    ):
        try:
            technical_snapshot_result = dependencies.technical_snapshot_service.rebuild_for_market_date(
                market_date=latest_trade_date,
                symbols=current_context_successes,
                exact_scope_symbols=requested_symbol_codes,
                source='pykrx',
                run_id=run_id,
                progress_callback=make_stage_progress_callback(
                    persist_stage_progress,
                    stage='technical_snapshots',
                    default_step='computing_symbol_snapshot',
                    default_activity='Computing daily technical snapshots',
                    include_current_symbol=True,
                ),
                include_retry_metadata=True,
                authoritative_market_scope=(
                    daily_derived_market_scope_authority
                ),
            )
            if failed_symbols:
                technical_snapshot_result.update(causal_price_skip)
            technical_failed = int(technical_snapshot_result.get('snapshots_failed') or 0)
            technical_succeeded = int(technical_snapshot_result.get('snapshots_persisted') or 0)
            technical_total = technical_succeeded + technical_failed
            technical_status = _completion_status_for_gap(failed=technical_failed, total=technical_total)
            persist_stage_progress(
                stage='technical_snapshots',
                status=technical_status,
                current_step='technical_snapshots_completed',
                current_activity='Daily technical snapshots computed'
                if technical_status == 'completed'
                else 'Daily technical snapshots completed with errors',
                processed=technical_total,
                total=len(requested_symbol_codes),
                succeeded=technical_succeeded,
                failed=technical_failed,
                extra={
                    **_gap_policy_details(failed=technical_failed, total=technical_total),
                    **causal_price_skip,
                },
            )
        except Exception as exc:
            technical_snapshot_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='pykrx',
                error=exc,
                totals={
                    'requested_symbols': len(requested_symbol_codes),
                    'snapshots_persisted': 0,
                    'snapshots_failed': len(requested_symbol_codes),
                },
                failed_symbols=requested_symbol_codes,
                items_key='items',
            )
            technical_snapshot_result['_retry_metadata'] = {
                'failed_symbols': list(requested_symbol_codes),
            }
            snapshot_error_message = f'Technical snapshot persistence failed: {exc}'
            persist_stage_progress(
                stage='technical_snapshots',
                status='failed',
                current_step='technical_snapshots_failed',
                current_activity='Daily technical snapshot persistence failed',
                processed=0,
                total=len(requested_symbol_codes),
                succeeded=0,
                failed=len(requested_symbol_codes),
                extra=causal_price_skip,
            )
    elif 'technical_snapshots' in requested_stage_set:
        technical_snapshot_result = {
            'skipped': True,
            'status': 'blocked',
            'reason': 'price_context_exact_scope_unavailable',
            'market_date': latest_trade_date,
            'source': 'pykrx',
            'requested_symbols': len(requested_symbol_codes),
            'snapshots_persisted': 0,
            'snapshots_failed': len(requested_symbol_codes),
            **causal_price_skip,
        }
        snapshot_error_message = (
            'Technical snapshot persistence blocked: price context exact '
            'scope unavailable.'
        )
        persist_stage_progress(
            stage='technical_snapshots',
            status='skipped',
            current_step='technical_snapshots_skipped',
            current_activity='Skipped technical snapshots because no fresh prices were collected',
            processed=0,
            total=0,
            succeeded=0,
            failed=0,
            extra=causal_price_skip,
        )
    else:
        technical_snapshot_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': 'pykrx',
        }

    technical_successful_symbols: list[str] = []
    technical_retry_symbols: list[str] = list(requested_symbol_codes)
    technical_scope_is_exact = False
    if 'technical_snapshots' in requested_stage_set:
        technical_partition = _validated_exact_scope_outcome(
            result=technical_snapshot_result,
            invocation_symbols=requested_symbol_codes,
            status=_resolve_result_status(
                technical_snapshot_result,
                stage_id='technical_snapshots',
            ),
        )
        technical_finalization = technical_snapshot_result.get(
            'scope_finalization'
        )
        technical_scope_is_exact = bool(
            technical_partition is not None
            and technical_partition['manifest_version']
            == 'technical_snapshots_scope_outcomes_v1'
            and isinstance(technical_finalization, Mapping)
            and technical_finalization.get('ready') is True
        )
        if technical_scope_is_exact:
            technical_successful_symbols = list(
                technical_partition['successful_stock_codes']
            )
            technical_retry_symbols = list(
                technical_partition['retry_debt_stock_codes']
            )
        elif snapshot_error_message is None:
            snapshot_error_message = (
                'Technical snapshot persistence did not return an exact '
                'atomically finalized scope.'
            )

    action_summary_result: dict
    action_summary_error_message: str | None = None
    if (
        'action_summaries' in requested_stage_set
        and technical_scope_is_exact
    ):
        try:
            action_summary_result = dependencies.action_summary_service.rebuild_for_market_date(
                market_date=latest_trade_date,
                symbols=technical_successful_symbols,
                exact_scope_symbols=requested_symbol_codes,
                source='pykrx',
                run_id=run_id,
                progress_callback=make_stage_progress_callback(
                    persist_stage_progress,
                    stage='action_summaries',
                    default_step='computing_action_summary',
                    default_activity='Computing daily action summaries',
                    include_current_symbol=True,
                ),
                include_retry_metadata=True,
                authoritative_market_scope=(
                    daily_derived_market_scope_authority
                ),
            )
            if failed_symbols:
                action_summary_result.update(causal_price_skip)
            action_failed = int(action_summary_result.get('action_summaries_failed') or 0)
            action_succeeded = int(action_summary_result.get('action_summaries_persisted') or 0)
            action_total = action_succeeded + action_failed
            action_status = _completion_status_for_gap(failed=action_failed, total=action_total)
            persist_stage_progress(
                stage='action_summaries',
                status=action_status,
                current_step='action_summaries_completed',
                current_activity='Daily action summaries computed'
                if action_status == 'completed'
                else 'Daily action summaries completed with errors',
                processed=action_total,
                total=len(requested_symbol_codes),
                succeeded=action_succeeded,
                failed=action_failed,
                extra={
                    **_gap_policy_details(failed=action_failed, total=action_total),
                    **causal_price_skip,
                },
            )
        except Exception as exc:
            action_summary_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='pykrx',
                error=exc,
                totals={
                    'requested_symbols': len(requested_symbol_codes),
                    'action_summaries_persisted': 0,
                    'action_summaries_failed': len(requested_symbol_codes),
                },
                failed_symbols=requested_symbol_codes,
                items_key='items',
            )
            action_summary_result['_retry_metadata'] = {
                'failed_symbols': list(requested_symbol_codes),
            }
            action_summary_error_message = f'Action summary persistence failed: {exc}'
            persist_stage_progress(
                stage='action_summaries',
                status='failed',
                current_step='action_summaries_failed',
                current_activity='Daily action summary persistence failed',
                processed=0,
                total=len(requested_symbol_codes),
                succeeded=0,
                failed=len(requested_symbol_codes),
                extra=causal_price_skip,
            )
    elif 'action_summaries' in requested_stage_set:
        skip_reason = (
            'no_fresh_price_rows'
            if not price_context_symbols
            else 'technical_snapshot_success_scope_unavailable'
        )
        action_summary_result = {
            'skipped': True,
            'status': 'skipped',
            'reason': skip_reason,
            'market_date': latest_trade_date,
            'source': 'pykrx',
            'requested_symbols': 0,
            'action_summaries_persisted': 0,
            'action_summaries_failed': 0,
            **causal_price_skip,
        }
        persist_stage_progress(
            stage='action_summaries',
            status='skipped',
            current_step='action_summaries_skipped',
            current_activity=(
                'Skipped action summaries because no exact successful '
                'technical snapshot cohort was available'
            ),
            processed=0,
            total=0,
            succeeded=0,
            failed=0,
            extra=causal_price_skip,
        )
    else:
        action_summary_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': 'pykrx',
        }

    if 'action_summaries' in requested_stage_set:
        action_partition = _validated_exact_scope_outcome(
            result=action_summary_result,
            invocation_symbols=requested_symbol_codes,
            status=_resolve_result_status(
                action_summary_result,
                stage_id='action_summaries',
            ),
        )
        action_finalization = action_summary_result.get(
            'scope_finalization'
        )
        action_scope_is_exact = bool(
            action_partition is not None
            and action_partition['manifest_version']
            == 'action_summaries_scope_outcomes_v1'
            and isinstance(action_finalization, Mapping)
            and action_finalization.get('ready') is True
        )
        if not action_scope_is_exact and action_summary_error_message is None:
            action_summary_error_message = (
                'Action summary persistence did not return an exact '
                'atomically finalized scope.'
            )

    trending_rank_result: dict
    trending_rank_error_message: str | None = None
    if 'trending_ranks' in requested_stage_set:
        try:
            normalized_trending_markets = {
                str(market).strip().upper() for market in markets
            }
            if (
                not exact_piotroski_membership_symbols
                or not {'KOSPI', 'KOSDAQ'}.issubset(
                    normalized_trending_markets
                )
            ):
                raise RuntimeError(
                    'Trending rank rebuild requires validated complete '
                    'KOSPI/KOSDAQ membership; limited, single-market, or '
                    'unproven scopes cannot replace global ranks.'
                )
            if failed_symbols:
                raise RuntimeError(
                    'Trending rank rebuild requires complete fresh price '
                    'collection. Per-symbol downstream stages may continue, '
                    'but global ranks are preserved until recovery completes.'
                )
            if not technical_scope_is_exact:
                raise RuntimeError(
                    'Trending rank rebuild requires an exact atomically '
                    'finalized technical snapshot scope. Existing global '
                    'ranks are preserved when technical persistence '
                    'authority is unavailable.'
                )
            if technical_retry_symbols:
                raise RuntimeError(
                    'Trending rank rebuild requires complete technical '
                    'snapshot persistence for 5d/20d ranks. Existing global '
                    'ranks are preserved while technical retry debt remains: '
                    + ','.join(technical_retry_symbols[:20])
                )
            trending_rank_result = dependencies.compute_trending_ranks(
                market_date=latest_trade_date,
                source='pykrx',
                run_id=run_id,
                symbols=exact_piotroski_membership_symbols,
                authoritative_scope=True,
                progress_callback=make_stage_progress_callback(
                    persist_stage_progress,
                    stage='trending_ranks',
                    default_step='computing_trending_period',
                    default_activity='Computing daily trending ranks',
                    include_current_period=True,
                ),
            )
            trending_failed = (
                int(trending_rank_result.get('failed_count') or 0)
                if 'failed_count' in trending_rank_result
                else (
                    int(
                        trending_rank_result.get('periods_failed')
                        or 0
                    )
                    + int(
                        trending_rank_result.get(
                            'periods_unavailable'
                        )
                        or 0
                    )
                )
            )
            trending_succeeded = int(
                trending_rank_result.get('periods_persisted')
                or trending_rank_result.get('periods_completed')
                or (1 if int(trending_rank_result.get('trending_ranks_persisted') or 0) > 0 else 0)
            )
            trending_total = int(trending_rank_result.get('total') or trending_succeeded + trending_failed or 0)
            trending_status = _completion_status_for_gap(failed=trending_failed, total=trending_total)
            if trending_failed:
                trending_rank_error_message = (
                    f'Trending rank persistence incomplete: {trending_failed} period(s) failed'
                )
                # Rank-period failures are global rather than symbol-addressable.
                trending_rank_result['_retry_metadata'] = {
                    'failed_symbols': list(requested_symbol_codes),
                }
            persist_stage_progress(
                stage='trending_ranks',
                status=trending_status,
                current_step='trending_ranks_completed',
                current_activity='Daily trending ranks computed'
                if trending_status == 'completed'
                else 'Daily trending ranks completed with errors',
                processed=trending_succeeded + trending_failed,
                total=trending_total,
                succeeded=trending_succeeded,
                failed=trending_failed,
                extra=_gap_policy_details(failed=trending_failed, total=trending_total),
            )
        except Exception as exc:
            trending_rank_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='pykrx',
                error=exc,
                totals={
                    'periods': [],
                    'trending_ranks_persisted': 0,
                },
            )
            trending_rank_result['_retry_metadata'] = {
                'failed_symbols': list(requested_symbol_codes),
            }
            trending_rank_error_message = f'Trending rank persistence failed: {exc}'
            persist_stage_progress(
                stage='trending_ranks',
                status='failed',
                current_step='trending_ranks_failed',
                current_activity='Daily trending rank persistence failed',
                processed=0,
                total=0,
                succeeded=0,
                failed=1,
                extra={
                    'error_type': type(exc).__name__,
                    'error_message': str(exc),
                },
            )
    else:
        trending_rank_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': 'pykrx',
        }

    signal_feature_result: dict
    signal_feature_error_message: str | None = None
    if point_in_time_recovery and 'signal_features' in requested_stage_set:
        signal_feature_result = _build_point_in_time_current_only_skip(
            stage_id='signal_features',
            market_date=latest_trade_date,
        )
        persist_stage_progress(
            stage='signal_features',
            status='completed',
            current_step='signal_features_safety_skipped',
            current_activity='Skipped current-only KIS rankings during point-in-time recovery',
            processed=0,
            total=0,
            succeeded=0,
            failed=0,
            extra={'result_summary': signal_feature_result},
        )
    elif 'signal_features' in requested_stage_set:
        try:
            persist_stage_progress(
                stage='signal_features',
                status='running',
                current_step='collecting_daily_signal_features',
                current_activity='Collecting daily KIS signal features',
                processed=0,
                total=3,
                succeeded=0,
                failed=0,
            )
            signal_refresh_kwargs: dict[str, Any] = {
                'market_date': latest_trade_date,
                'include_retry_metadata': True,
                'exact_symbols': exact_piotroski_membership_symbols,
            }
            if daily_derived_market_scope_authority is not None:
                signal_refresh_kwargs['authoritative_market_scope'] = (
                    daily_derived_market_scope_authority
                )
            signal_feature_result = asyncio.run(
                dependencies.signal_feature_service.refresh_for_market_date(
                    **signal_refresh_kwargs,
                )
            )
            signal_counts = _extract_raw_result_counts(
                signal_feature_result,
                default_total=len(exact_piotroski_membership_symbols or ()),
                stage_id='signal_features',
            )
            signal_total = signal_counts['total']
            signal_source_errors = signal_feature_result.get('source_errors') or {}
            signal_incomplete_sources = list(
                signal_feature_result.get('incomplete_sources') or []
            )
            signal_failed = signal_counts['failed']
            signal_status = _completion_status_for_gap(failed=signal_failed, total=signal_total)
            if signal_source_errors or signal_incomplete_sources:
                signal_status = 'partial'
                signal_feature_error_message = (
                    'Daily signal feature sources incomplete: '
                    + ','.join(
                        sorted(
                            {
                                *(str(source) for source in signal_source_errors),
                                *(str(source) for source in signal_incomplete_sources),
                            }
                        )
                    )
                )
                # A failed ranking source can omit candidates without producing
                # per-symbol failures, so retry the admitted scope conservatively.
                signal_feature_result.setdefault(
                    '_retry_metadata',
                    {'failed_symbols': list(requested_symbol_codes)},
                )
            persist_stage_progress(
                stage='signal_features',
                status=signal_status,
                current_step='signal_features_completed',
                current_activity='Daily KIS signal features collected',
                processed=signal_counts['processed'],
                total=signal_total,
                succeeded=signal_counts['succeeded'],
                failed=signal_failed,
                extra={
                    'sources': signal_feature_result.get('sources'),
                    'source_counts': signal_feature_result.get('source_counts'),
                    'source_errors': signal_source_errors,
                    'incomplete_sources': signal_incomplete_sources,
                    'source_health': signal_feature_result.get('source_health'),
                    'coverage': signal_feature_result.get('coverage'),
                    **_gap_policy_details(failed=signal_failed, total=signal_total),
                },
            )
        except Exception as exc:
            signal_feature_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source='kis_ranking',
                error=exc,
                totals={
                    'sources': [],
                    'source_counts': {},
                    'features_persisted': 0,
                    'features_failed': len(symbols),
                },
                failed_symbols=[item.symbol for item in symbols],
                items_key='items',
                extra={'participation_context': {'error': str(exc)}},
            )
            signal_feature_result['_retry_metadata'] = {
                'failed_symbols': [item.symbol for item in symbols],
            }
            signal_feature_error_message = f'Daily signal feature collection failed: {exc}'
            persist_stage_progress(
                stage='signal_features',
                status='failed',
                current_step='signal_features_failed',
                current_activity='Daily KIS signal feature collection failed',
                processed=0,
                total=3,
                succeeded=0,
                failed=1,
                extra={
                    'error_type': type(exc).__name__,
                    'error_message': str(exc),
                },
            )
    else:
        signal_feature_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': 'kis_ranking',
        }

    market_risk_filter_result: dict
    market_risk_filter_error_message: str | None = None
    if (
        point_in_time_recovery
        and 'market_risk_filter' in requested_stage_set
        and should_skip_point_in_time_current_only_stage(
            stage_id='market_risk_filter',
            market_date=latest_trade_date,
        )
    ):
        market_risk_filter_result = _build_point_in_time_current_only_skip(
            stage_id='market_risk_filter',
            market_date=latest_trade_date,
        )
        persist_stage_progress(
            stage='market_risk_filter',
            status='completed',
            current_step='market_risk_filter_safety_skipped',
            current_activity='Skipped current-only KIS risk data during point-in-time recovery',
            processed=0,
            total=0,
            succeeded=0,
            failed=0,
            extra={'result_summary': market_risk_filter_result},
        )
    elif 'market_risk_filter' in requested_stage_set:
        market_risk_scope_finalized = False
        try:
            persist_stage_progress(
                stage='market_risk_filter',
                status='running',
                current_step='collecting_market_risk_snapshots_and_filters',
                current_activity='Collecting daily KIS risk snapshots and evaluating market risk filter',
                processed=0,
                total=len(price_context_symbols),
                succeeded=0,
                failed=0,
            )
            market_risk_filter_result = asyncio.run(
                dependencies.market_risk_filter_service.rebuild_for_market_date(
                    market_date=latest_trade_date,
                    symbols=price_context_symbols,
                    source=MARKET_RISK_FILTER_SOURCE,
                    run_id=run_id,
                    exact_scope_symbols=(
                        exact_piotroski_membership_symbols
                    ),
                    authoritative_market_scope=(
                        daily_derived_market_scope_authority
                        if set(price_context_symbols)
                        == set(exact_piotroski_membership_symbols or [])
                        else None
                    ),
                )
            )
            if not exact_piotroski_membership_symbols:
                raise RuntimeError(
                    'Market-risk filter requires validated exact EOD '
                    'membership proof.'
                )
            if not (
                isinstance(
                    market_risk_filter_result.get('scope_finalization'),
                    Mapping,
                )
                and market_risk_filter_result['scope_finalization'].get(
                    'ready'
                )
                is True
            ):
                raise RuntimeError(
                    'market_risk_filter atomic exact-scope finalization '
                    'proof is unavailable'
                )
            market_risk_scope_finalized = True
            _clear_exact_scope_outcome(market_risk_filter_result)
            market_risk_total = len(price_context_symbols)
            market_risk_failed = int(market_risk_filter_result.get('results_failed') or 0)
            market_risk_status = _completion_status_for_gap(
                failed=market_risk_failed,
                total=market_risk_total,
            )
            market_risk_retry_symbols = _peek_retry_failed_symbols(
                market_risk_filter_result
            )
            if (
                len(market_risk_retry_symbols) == market_risk_failed
                and set(market_risk_retry_symbols).issubset(
                    price_context_symbols
                )
                and int(
                    market_risk_filter_result.get(
                        'results_persisted'
                    )
                    or 0
                )
                + market_risk_failed
                == market_risk_total
            ):
                market_risk_retry_set = set(
                    market_risk_retry_symbols
                )
                market_risk_filter_result.update(
                    _build_exact_scope_outcome(
                        manifest_version=(
                            'market_risk_filter_scope_outcomes_v1'
                        ),
                        selected_symbols=price_context_symbols,
                        successful_symbols=[
                            symbol
                            for symbol in price_context_symbols
                            if symbol not in market_risk_retry_set
                        ],
                        retry_debt_symbols=[
                            symbol
                            for symbol in price_context_symbols
                            if symbol in market_risk_retry_set
                        ],
                    )
                )
                market_risk_filter_result['_retry_metadata'] = {
                    'failed_symbols': [
                        symbol
                        for symbol in price_context_symbols
                        if symbol in market_risk_retry_set
                    ]
                }
            market_risk_filter_result['status'] = market_risk_status
            persist_stage_progress(
                stage='market_risk_filter',
                status=market_risk_status,
                current_step='market_risk_filter_completed',
                current_activity='Daily KIS market risk filter computed',
                processed=(int(market_risk_filter_result.get('results_persisted') or 0) + market_risk_failed),
                total=market_risk_total,
                succeeded=market_risk_filter_result.get('results_persisted'),
                failed=market_risk_failed,
                extra={
                    'snapshot_results_failed': market_risk_filter_result.get('snapshot_collection', {}).get(
                        'snapshots_failed'
                    ),
                    'passed_count': market_risk_filter_result.get('summary', {}).get('passed_count'),
                    'watchlist_count': market_risk_filter_result.get('summary', {}).get('watchlist_count'),
                    **_bounded_failure_diagnostics(market_risk_filter_result),
                    **_gap_policy_details(
                        failed=market_risk_failed,
                        total=market_risk_total,
                    ),
                },
            )
        except Exception as exc:
            scope_finalization: dict[str, Any] | None = None
            scope_finalization_error: Exception | None = None
            if (
                exact_piotroski_membership_symbols
                and not market_risk_scope_finalized
            ):
                try:
                    scope_finalization = _finalize_daily_stage_scope(
                        market_date=latest_trade_date,
                        stage_id='market_risk_filter',
                        exact_symbols=exact_piotroski_membership_symbols,
                        result=None,
                        authoritative_market_scope=(
                            daily_derived_market_scope_authority
                            if set(price_context_symbols)
                            == set(exact_piotroski_membership_symbols)
                            else None
                        ),
                    )
                except Exception as cleanup_exc:
                    scope_finalization_error = cleanup_exc
            market_risk_filter_result = build_stage_failure_result(
                market_date=latest_trade_date,
                source=MARKET_RISK_FILTER_SOURCE,
                error=exc,
                totals={
                    'results_persisted': 0,
                    'results_failed': len(price_context_symbols),
                },
                failed_symbols=price_context_symbols,
                items_key='items',
            )
            market_risk_filter_result['_retry_metadata'] = {
                'failed_symbols': list(price_context_symbols),
            }
            market_risk_filter_result['scope_finalization'] = (
                scope_finalization
            )
            if scope_finalization_error is not None:
                market_risk_filter_result[
                    'scope_finalization_error_type'
                ] = type(scope_finalization_error).__name__
                market_risk_filter_result[
                    'scope_finalization_error_message'
                ] = str(scope_finalization_error)
            market_risk_filter_error_message = f'Market risk filter persistence failed: {exc}'
            persist_stage_progress(
                stage='market_risk_filter',
                status='failed',
                current_step='market_risk_filter_failed',
                current_activity='Daily KIS market risk filter persistence failed',
                processed=0,
                total=len(price_context_symbols),
                succeeded=0,
                failed=len(price_context_symbols),
            )
    else:
        market_risk_filter_result = {
            'skipped': True,
            'reason': 'stage_not_requested',
            'market_date': latest_trade_date,
            'source': MARKET_RISK_FILTER_SOURCE,
        }

    if (
        'market_risk_filter' in requested_stage_set
        and price_context_symbols
        and not market_risk_filter_result.get('safe_skip')
        and 'scope_outcome_manifest_version'
        not in market_risk_filter_result
    ):
        market_risk_failed = int(
            market_risk_filter_result.get('results_failed') or 0
        )
        market_risk_persisted = int(
            market_risk_filter_result.get('results_persisted') or 0
        )
        market_risk_retry_symbols = _peek_retry_failed_symbols(
            market_risk_filter_result
        )
        if (
            len(market_risk_retry_symbols) == market_risk_failed
            and set(market_risk_retry_symbols).issubset(
                price_context_symbols
            )
            and market_risk_persisted + market_risk_failed
            == len(price_context_symbols)
        ):
            market_risk_retry_set = set(market_risk_retry_symbols)
            market_risk_filter_result.update(
                _build_exact_scope_outcome(
                    manifest_version=(
                        'market_risk_filter_scope_outcomes_v1'
                    ),
                    selected_symbols=price_context_symbols,
                    successful_symbols=[
                        symbol
                        for symbol in price_context_symbols
                        if symbol not in market_risk_retry_set
                    ],
                    retry_debt_symbols=[
                        symbol
                        for symbol in price_context_symbols
                        if symbol in market_risk_retry_set
                    ],
                )
            )

    downstream_failures_by_stage: dict[str, set[str]] = {}
    for failed_stage_id, stage_result in (
        ('pykrx_enrichment', pykrx_enrichment_result),
        ('technical_snapshots', price_context_result),
        ('technical_snapshots', technical_snapshot_result),
        ('action_summaries', action_summary_result),
        ('trending_ranks', trending_rank_result),
        ('market_risk_filter', market_risk_filter_result),
        ('signal_features', signal_feature_result),
    ):
        stage_failed_symbols = _consume_retry_failed_symbols(stage_result)
        if stage_failed_symbols:
            downstream_failures_by_stage.setdefault(
                failed_stage_id,
                set(),
            ).update(stage_failed_symbols)
    downstream_failed_symbols = {
        failed_symbol
        for stage_failed_symbols in downstream_failures_by_stage.values()
        for failed_symbol in stage_failed_symbols
    }
    price_retry_symbol_codes = list(dict.fromkeys(failed_symbols))
    price_retry_symbol_set = set(price_retry_symbol_codes)
    current_downstream_retry_symbol_codes = [
        code
        for code in requested_symbol_codes
        if code in downstream_failed_symbols and code not in price_retry_symbol_set
    ]
    (
        pending_inherited_downstream_symbols,
        downstream_retry_symbol_codes,
    ) = _merge_inherited_downstream_scope(
        requested_symbols=requested_symbol_codes,
        inherited_symbols=inherited_downstream_symbols,
        current_price_failure_symbols=price_retry_symbol_codes,
        current_failure_symbols=current_downstream_retry_symbol_codes,
        preserve_deferred_followup_scope=bool(
            chain_fundamentals_followup and mode == 'daily_auto'
        ),
    )
    run_metadata['inherited_downstream_retry_symbols'] = (
        pending_inherited_downstream_symbols
    )
    run_metadata['inherited_downstream_retry_symbols_count'] = len(
        pending_inherited_downstream_symbols
    )
    downstream_resume_candidates: list[str] = []
    if (
        pending_inherited_downstream_symbols
        and inherited_downstream_resume_from_stage
    ):
        downstream_resume_candidates.append(
            inherited_downstream_resume_from_stage
        )
    current_downstream_retry_symbol_set = set(
        current_downstream_retry_symbol_codes
    )
    downstream_resume_candidates.extend(
        stage_id
        for stage_id, stage_failed_symbols in downstream_failures_by_stage.items()
        if current_downstream_retry_symbol_set.intersection(
            stage_failed_symbols
        )
    )
    if downstream_retry_symbol_codes and downstream_resume_candidates:
        run_metadata['downstream_retry_resume_from_stage'] = min(
            downstream_resume_candidates,
            key=AUTO_DAILY_TRACKED_STAGE_ORDER.index,
        )
    elif not downstream_retry_symbol_codes:
        run_metadata.pop('downstream_retry_resume_from_stage', None)
    retry_symbol_codes = list(
        dict.fromkeys(
            [*price_retry_symbol_codes, *downstream_retry_symbol_codes]
        )
    )
    price_retry_symbol_set = set(price_retry_symbol_codes)
    price_scope_outcome = _build_exact_scope_outcome(
        selected_symbols=requested_symbol_codes,
        successful_symbols=[
            stock_code
            for stock_code in requested_symbol_codes
            if stock_code not in price_retry_symbol_set
        ],
        retry_debt_symbols=price_retry_symbol_codes,
    )

    run_errors: list[str] = []
    if failed_symbols:
        run_errors.append(f'Failed symbols: {",".join(failed_symbols[:20])}')
    if turnover_coverage['status'] == 'partial':
        run_errors.append(
            'Turnover coverage incomplete: '
            f"{turnover_coverage['invalid_turnover_bars']} invalid bars and "
            f"{turnover_coverage['market_fetch_failures']} failed market fetches."
        )
    append_stage_error(
        run_errors,
        direct_error=price_context_error_message,
        result=price_context_result,
        failed_key='contexts_failed',
        incomplete_prefix='Price context persistence incomplete for symbols: ',
        exclude_symbols=failed_symbol_set,
        total_keys=('requested_symbols', 'contexts_persisted'),
    )
    append_stage_error(run_errors, direct_error=pykrx_enrichment_error_message)
    append_stage_error(
        run_errors,
        direct_error=snapshot_error_message,
        result=technical_snapshot_result,
        failed_key='snapshots_failed',
        incomplete_prefix='Technical snapshot persistence incomplete for symbols: ',
        total_keys=('requested_symbols', 'snapshots_persisted'),
    )
    append_stage_error(
        run_errors,
        direct_error=action_summary_error_message,
        result=action_summary_result,
        failed_key='action_summaries_failed',
        incomplete_prefix='Action summary persistence incomplete for symbols: ',
        total_keys=('requested_symbols', 'action_summaries_persisted'),
    )
    append_stage_error(run_errors, direct_error=trending_rank_error_message)
    append_stage_error(
        run_errors,
        direct_error=signal_feature_error_message,
        result=signal_feature_result,
        failed_key='features_failed',
        incomplete_prefix='Daily signal feature persistence incomplete for symbols: ',
        total_keys=('features_total', 'features_persisted'),
    )
    append_stage_error(
        run_errors,
        direct_error=market_risk_filter_error_message,
        result=market_risk_filter_result,
        failed_key='results_failed',
        incomplete_prefix='Market risk filter persistence incomplete for symbols: ',
        total_keys=('matched_symbols', 'matched_symbol_count', 'requested_symbol_count', 'results_persisted'),
    )
    if pending_inherited_downstream_symbols:
        run_errors.append(
            'Inherited downstream recovery remains pending for '
            f'{len(pending_inherited_downstream_symbols)} symbols.'
        )

    status = 'completed' if not run_errors else 'completed_with_errors'
    # Only collection failures belong in the full-pipeline retry scope.  A
    # downstream failure must resume from persisted prices instead of
    # downloading OHLCV again.
    run_metadata = service._run_metadata_with_failed_symbols(
        run_metadata,
        price_retry_symbol_codes,
    )
    run_metadata = service._run_metadata_with_downstream_retry_symbols(
        run_metadata,
        downstream_retry_symbol_codes,
    )
    run_metadata['turnover_invalid_symbols'] = list(
        dict.fromkeys(turnover_invalid_symbols)
    )
    run_metadata['turnover_invalid_symbols_count'] = len(
        run_metadata['turnover_invalid_symbols']
    )
    complete_eod_run_or_fail_tracking(
        status=status,
        symbols_succeeded=symbols_succeeded,
        symbols_failed=symbols_failed,
        error_message=' | '.join(run_errors) if run_errors else None,
        run_metadata=run_metadata,
    )
    result = {
        'run_id': run_id,
        'source': 'pykrx',
        'status': status,
        'selected_stages': list(resolved_selected_stages),
        'markets': list(markets),
        'start_date': normalized_start,
        'requested_end_date': normalized_end,
        'latest_available_trade_date': latest_trade_date,
        'symbols_total': len(symbols),
        'symbols_succeeded': symbols_succeeded,
        'symbols_failed': symbols_failed,
        'failed_symbols': failed_symbols[:50],
        'retry_symbols': retry_symbol_codes[:50],
        'retry_symbols_count': len(retry_symbol_codes),
        'price_retry_symbols': price_retry_symbol_codes[:50],
        'price_retry_symbols_count': len(price_retry_symbol_codes),
        'downstream_retry_symbols': downstream_retry_symbol_codes[:50],
        'downstream_retry_symbols_count': len(downstream_retry_symbol_codes),
        'bars_upserted': bars_upserted,
        'turnover_coverage': turnover_coverage,
        'turnover_invalid_symbols': turnover_invalid_symbols[:50],
        'turnover_invalid_symbols_count': len(turnover_invalid_symbols),
        **price_scope_outcome,
        'price_contexts': price_context_result,
        'pykrx_enrichment': pykrx_enrichment_result,
        'technical_snapshots': technical_snapshot_result,
        'action_summaries': action_summary_result,
        'trending_ranks': trending_rank_result,
        'signal_features': signal_feature_result,
        'market_risk_filter': market_risk_filter_result,
        'refresh_universe': effective_refresh_universe,
        'universe_snapshot': universe_snapshot,
        **invocation_scope_proof,
    }

    def record_followup_start(stage_id: str, activity: str) -> None:
        persist_stage_progress(
            stage=stage_id,
            status='running',
            current_step=f'{stage_id}_running',
            current_activity=activity,
            processed=0,
            total=0,
            succeeded=0,
            failed=0,
        )

    def record_followup_result(
        stage_id: str,
        stage_result: dict[str, Any],
        *,
        default_total: int = 0,
    ) -> None:
        stage_status = _resolve_result_status(
            stage_result,
            stage_id=stage_id,
        )
        counts = _extract_result_counts(
            stage_result,
            default_total=default_total,
            stage_id=stage_id,
        )
        persist_stage_progress(
            stage=stage_id,
            status=stage_status,
            current_step=f'{stage_id}_{stage_status}',
            current_activity=f'{_status_activity_prefix(stage_status)} {stage_id.replace("_", " ")}',
            processed=counts['processed'],
            total=counts['total'],
            succeeded=counts['succeeded'],
            failed=counts['failed'],
            source_run_ids=_extract_followup_source_run_ids(
                stage_id=stage_id,
                result=stage_result,
            ),
            extra={
                'terminal_count_authority': 'result',
                'result_summary': _compact_result_summary(stage_result),
            },
        )

    def record_followup_failure(stage_id: str, exc: Exception) -> None:
        persist_stage_progress(
            stage=stage_id,
            status='failed',
            current_step=f'{stage_id}_failed',
            current_activity=f'{stage_id.replace("_", " ")} failed: {exc}',
            processed=0,
            total=0,
            succeeded=0,
            failed=1,
            extra={
                'error_type': type(exc).__name__,
                'error_message': str(exc),
            },
        )

    def run_followup_stage(
        stage_id: str,
        *,
        activity: str,
        call: Callable[[], dict[str, Any]],
        default_total: int = 0,
    ) -> dict[str, Any]:
        record_followup_start(stage_id, activity)
        try:
            stage_result = call()
        except Exception as exc:
            record_followup_failure(stage_id, exc)
            raise
        record_followup_result(stage_id, stage_result, default_total=default_total)
        return stage_result

    def fundamentals_progress_callback(**payload: Any) -> None:
        details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
        source_run_ids = payload.get('source_run_ids') if isinstance(payload.get('source_run_ids'), dict) else None
        persist_stage_progress(
            stage='fundamentals_daily_refresh',
            status=str(payload.get('status') or 'running'),
            current_step=str(payload.get('current_step') or 'fundamentals_daily_refresh_running'),
            current_activity=str(payload.get('current_activity') or 'Running DART fundamentals daily refresh'),
            processed=payload.get('processed'),
            total=payload.get('total'),
            succeeded=payload.get('succeeded'),
            failed=payload.get('failed'),
            current_symbol=payload.get('current_symbol'),
            current_period=payload.get('current_period'),
            source_run_ids=source_run_ids,
            extra=details,
        )

    def feedback_progress_callback(**payload: Any) -> None:
        details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
        source_run_ids = payload.get('source_run_ids') if isinstance(payload.get('source_run_ids'), dict) else None
        persist_stage_progress(
            stage='agent_feedback_summaries',
            status=str(payload.get('status') or 'running'),
            current_step=str(payload.get('current_step') or 'agent_feedback_summaries_running'),
            current_activity=str(payload.get('current_activity') or 'Running agent feedback summary follow-up'),
            processed=payload.get('processed'),
            total=payload.get('total'),
            succeeded=payload.get('succeeded'),
            failed=payload.get('failed'),
            current_period=payload.get('current_period'),
            source_run_ids=source_run_ids,
            extra=details,
        )

    def signal_decider_progress_callback(**payload: Any) -> None:
        details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
        source_run_ids = payload.get('source_run_ids') if isinstance(payload.get('source_run_ids'), dict) else None
        persist_stage_progress(
            stage='signal_decider',
            status=str(payload.get('status') or 'running'),
            current_step=str(payload.get('current_step') or 'signal_decider_running'),
            current_activity=str(payload.get('current_activity') or 'Running signal-decider follow-up'),
            processed=payload.get('processed'),
            total=payload.get('total'),
            succeeded=payload.get('succeeded'),
            failed=payload.get('failed'),
            current_period=payload.get('current_period'),
            source_run_ids=source_run_ids,
            extra=details,
        )

    def record_factor_stage_result(
        stage_id: str,
        raw_stage_result: dict[str, Any],
        *,
        factor_followup_status: str | None,
    ) -> None:
        nested_result = (
            raw_stage_result.get('result')
            if isinstance(raw_stage_result.get('result'), dict)
            else {}
        )
        stage_status = _normalize_stage_status(
            raw_stage_result.get('status')
        )
        counts = _extract_result_counts(nested_result, default_total=0)
        if stage_status == 'failed' and counts['failed'] <= 0:
            counts['failed'] = 1
            counts['processed'] = max(counts['processed'], 1)
            counts['total'] = max(counts['total'], 1)
        persist_stage_progress(
            stage=stage_id,
            status=stage_status,
            current_step=f'{stage_id}_{stage_status}',
            current_activity=(
                f'{_status_activity_prefix(stage_status)} '
                f'{stage_id.replace("_", " ")}'
            ),
            processed=counts['processed'],
            total=counts['total'],
            succeeded=counts['succeeded'],
            failed=counts['failed'],
            extra={
                'terminal_count_authority': 'result',
                'factor_scoring_followup_status': factor_followup_status,
                'result_summary': _compact_result_summary(
                    nested_result or raw_stage_result
                ),
            },
        )

    def record_factor_followup_result(factor_result: dict[str, Any]) -> None:
        stage_results = factor_result.get('stages') if isinstance(factor_result.get('stages'), dict) else {}
        for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER:
            raw_stage_result = stage_results.get(stage_id) if isinstance(stage_results.get(stage_id), dict) else {}
            if factor_result.get('triggered') is True and not raw_stage_result:
                raw_stage_result = {
                    'status': 'failed',
                    'reason': 'factor_followup_missing_stage_result',
                    'market_date': factor_result.get('market_date'),
                }
            elif not raw_stage_result:
                raw_stage_result = {
                    **factor_result,
                    'status': _resolve_result_status(factor_result),
                }
            record_factor_stage_result(
                stage_id,
                raw_stage_result,
                factor_followup_status=factor_result.get('status'),
            )

    def build_blocked_followup_result(stage_id: str, reason: str, *, upstream_status: str | None = None) -> dict[str, Any]:
        return {
            'triggered': False,
            'status': 'blocked',
            'reason': reason,
            'market_date': latest_trade_date,
            'upstream_status': upstream_status,
            'stage': stage_id,
        }

    def run_factor_followup(
        *,
        admitted_symbols: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        started_factor_stages = {'momentum_factors'}
        record_followup_start(
            'momentum_factors',
            'Running daily momentum factors',
        )

        def execute_factor_stage(
            stage_id: str,
            call: Callable[[], dict[str, Any]],
        ) -> tuple[dict[str, Any], bool]:
            if stage_id not in started_factor_stages:
                record_followup_start(
                    stage_id,
                    f'Running daily {stage_id.replace("_", " ")}',
                )
                started_factor_stages.add(stage_id)
            return _execute_factor_stage_with_heartbeat(
                call=call,
                heartbeat=lambda: (
                    daily_pipeline_tracker.heartbeat_stage(stage=stage_id)
                ),
                interval_seconds=FACTOR_STAGE_HEARTBEAT_INTERVAL_SECONDS,
                thread_name=(
                    f'daily-eod-{run_id}-{stage_id}-heartbeat'
                ),
            )

        def record_incremental_factor_result(
            stage_id: str,
            stage_result: dict[str, Any],
        ) -> None:
            record_factor_stage_result(
                stage_id,
                stage_result,
                factor_followup_status=stage_result.get('status'),
            )

        try:
            factor_kwargs: dict[str, Any] = {
                'market_date': latest_trade_date,
                'exact_membership_symbols': (
                    exact_piotroski_membership_symbols
                ),
                'stage_executor': execute_factor_stage,
                'stage_result_callback': record_incremental_factor_result,
            }
            if daily_derived_market_scope_authority is not None:
                factor_kwargs['authoritative_market_scope'] = (
                    daily_derived_market_scope_authority
                )
            if admitted_symbols is not None:
                factor_kwargs['admitted_symbols'] = list(admitted_symbols)
            factor_result = _run_daily_factor_followup(
                **factor_kwargs,
            )
        except Exception as exc:
            record_followup_failure('momentum_factors', exc)
            raise
        recorded_incrementally = bool(
            factor_result.pop(
                '_stage_results_recorded_incrementally',
                False,
            )
        )
        if (
            not recorded_incrementally
            and factor_result.get('reason')
            != 'pipeline_run_ownership_lost'
        ):
            record_factor_followup_result(factor_result)
        return factor_result

    signal_feature_admitted_symbols: list[str] | None = None
    factor_admitted_symbols: list[str] | None = None
    signal_feature_successful_symbols: list[str] | None = None

    def collect_post_factor_signal_features() -> dict[str, Any]:
        refresh_kwargs: dict[str, Any] = {
            'market_date': latest_trade_date,
            'include_retry_metadata': True,
            'exact_symbols': exact_piotroski_membership_symbols,
        }
        if signal_feature_admitted_symbols is not None:
            refresh_kwargs['admitted_symbols'] = list(
                signal_feature_admitted_symbols
            )
        else:
            if daily_derived_market_scope_authority is not None:
                refresh_kwargs['authoritative_market_scope'] = (
                    daily_derived_market_scope_authority
                )
        stage_result = dict(
            asyncio.run(
                dependencies.signal_feature_service.refresh_for_market_date(
                    **refresh_kwargs,
                )
            )
        )
        source_errors = stage_result.get('source_errors')
        incomplete_sources = stage_result.get('incomplete_sources')
        if source_errors or incomplete_sources:
            stage_result['status'] = 'partial'
            stage_result.setdefault(
                'reason',
                'signal_feature_sources_incomplete',
            )
        return stage_result

    def run_signal_feature_followup() -> dict[str, Any]:
        return run_followup_stage(
            'signal_features',
            activity='Collecting post-factor daily KIS signal features',
            call=collect_post_factor_signal_features,
            default_total=len(requested_symbol_codes),
        )

    def run_intraday_snapshot_followup() -> dict[str, Any]:
        return run_followup_stage(
            'intraday_snapshots',
            activity='Collecting KIS intraday snapshot follow-up',
            call=lambda: _run_daily_intraday_snapshot_followup(
                market_date=latest_trade_date,
                enabled=bool(
                    dependencies.settings.kis_intraday_snapshot_enabled
                ),
            ),
            default_total=len(requested_symbol_codes),
        )

    dart_auto_followup_enabled = bool((dependencies.settings.dart_api_key or '').strip())

    def validated_successful_cohort(
        *,
        stage_result: Mapping[str, Any],
        invocation_symbols: Sequence[str],
        stage_id: str,
    ) -> list[str] | None:
        stage_status = _resolve_result_status(
            dict(stage_result),
            stage_id=stage_id,
        )
        partition = _validated_exact_scope_outcome(
            result=stage_result,
            invocation_symbols=invocation_symbols,
            status=stage_status,
        )
        if partition is not None:
            # A symbol-exhaustive debt partition can advance its successful
            # cohort.  A partial result without symbol debt represents a
            # shared/global quality failure and remains fail-closed.
            if (
                stage_status == 'partial'
                and not partition['retry_debt_stock_codes']
                and str(stage_result.get('gate_status') or '').lower()
                != 'ready'
            ):
                return None
            if str(stage_result.get('gate_status') or '').lower() == 'blocked':
                return None
            if str(stage_result.get('reason') or '').startswith(
                'piotroski_evidence_'
            ):
                return None
            return list(partition['successful_stock_codes'])
        if stage_result.get('scope_outcome_manifest_version') is not None:
            return None
        if stage_status == 'completed':
            return list(invocation_symbols)
        return None

    exact_followup_symbols = list(
        dict.fromkeys(exact_piotroski_membership_symbols or [])
    )
    daily_followup_admitted_symbols = list(exact_followup_symbols)
    early_scope_results: list[
        tuple[str, Mapping[str, Any], Sequence[str]]
    ] = [
        (
            'price_collection',
            {
                **price_scope_outcome,
                'status': (
                    'partial'
                    if price_retry_symbol_codes
                    else 'completed'
                ),
            },
            requested_symbol_codes,
        )
    ]
    for early_stage_id, early_stage_result, invocation_scope in (
        (
            'pykrx_enrichment',
            pykrx_enrichment_result,
            requested_symbol_codes,
        ),
        (
            'technical_snapshots',
            technical_snapshot_result,
            price_context_symbols,
        ),
        (
            'action_summaries',
            action_summary_result,
            technical_successful_symbols,
        ),
        (
            'signal_features',
            signal_feature_result,
            exact_followup_symbols,
        ),
        (
            'market_risk_filter',
            market_risk_filter_result,
            price_context_symbols,
        ),
    ):
        if early_stage_id in requested_stage_set:
            early_scope_results.append(
                (early_stage_id, early_stage_result, invocation_scope)
            )

    early_scope_proof_valid = bool(daily_followup_admitted_symbols)
    for early_stage_id, early_stage_result, invocation_scope in (
        early_scope_results
    ):
        successful_cohort = validated_successful_cohort(
            stage_result=early_stage_result,
            invocation_symbols=invocation_scope,
            stage_id=early_stage_id,
        )
        if successful_cohort is None:
            early_scope_proof_valid = False
            daily_followup_admitted_symbols = []
            break
        successful_set = set(successful_cohort)
        daily_followup_admitted_symbols = [
            symbol
            for symbol in daily_followup_admitted_symbols
            if symbol in successful_set
        ]
    if pending_inherited_downstream_symbols:
        # The inherited retry set has no current-attempt success partition.
        # Its recovery invocation must finish before it can admit symbols.
        early_scope_proof_valid = False
        daily_followup_admitted_symbols = []
    if early_scope_proof_valid:
        downstream_retry_set = set(downstream_retry_symbol_codes)
        daily_followup_admitted_symbols = [
            symbol
            for symbol in daily_followup_admitted_symbols
            if symbol not in downstream_retry_set
        ]
    elif chain_fundamentals_followup and mode == 'daily_auto':
        # Without an exhaustive current-attempt identity proof there is no
        # safe successful complement.  Persist the full exact follow-up scope
        # as debt so a later recovery cannot retry one named failure and
        # silently strand the other unproven members.
        downstream_retry_symbol_codes = list(
            dict.fromkeys(
                [
                    *downstream_retry_symbol_codes,
                    *exact_followup_symbols,
                ]
            )
        )
        retry_symbol_codes = list(
            dict.fromkeys(
                [*price_retry_symbol_codes, *downstream_retry_symbol_codes]
            )
        )
        run_metadata = service._run_metadata_with_downstream_retry_symbols(
            run_metadata,
            downstream_retry_symbol_codes,
        )
        result['downstream_retry_symbols'] = (
            downstream_retry_symbol_codes[:50]
        )
        result['downstream_retry_symbols_count'] = len(
            downstream_retry_symbol_codes
        )
        result['retry_symbols'] = retry_symbol_codes[:50]
        result['retry_symbols_count'] = len(retry_symbol_codes)

    followup_admission_scope_outcome = _build_exact_scope_outcome(
        manifest_version='auto_eod_followup_admission_scope_outcomes_v1',
        selected_symbols=exact_followup_symbols,
        successful_symbols=daily_followup_admitted_symbols,
        retry_debt_symbols=[
            symbol
            for symbol in exact_followup_symbols
            if symbol not in set(daily_followup_admitted_symbols)
        ],
    )
    result['followup_admission'] = followup_admission_scope_outcome
    run_metadata['followup_admission_scope_outcome'] = (
        followup_admission_scope_outcome
    )
    try:
        dependencies.update_eod_collection_run_progress(
            run_id,
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            run_metadata=run_metadata,
        )
    except Exception as exc:
        # The current run can continue from its in-memory proof, but a later
        # narrowed PITR run must fail closed rather than infer the successful
        # complement when this durable checkpoint is unavailable.
        logger.warning(
            'Unable to persist follow-up admission scope for EOD run %s: %s',
            run_id,
            exc,
        )
    should_chain_fundamentals = (
        chain_fundamentals_followup
        and 'market_risk_filter' in requested_stage_set
        and dart_auto_followup_enabled
        and mode == 'daily_auto'
        and status in {'completed', 'completed_with_errors'}
    )
    should_defer_daily_followups = (
        chain_fundamentals_followup
        and mode == 'daily_auto'
        and status in {'completed', 'completed_with_errors'}
        and bool(downstream_retry_symbol_codes)
        and not daily_followup_admitted_symbols
    )
    downstream_recovery_pending_reason = (
        'inherited_downstream_recovery_pending'
        if pending_inherited_downstream_symbols
        else 'downstream_recovery_pending'
    )

    def build_downstream_recovery_pending_result(
        stage_id: str,
    ) -> dict[str, Any]:
        stage_result = build_blocked_followup_result(
            stage_id,
            downstream_recovery_pending_reason,
            upstream_status=status,
        )
        stage_result['pending_symbols_count'] = len(
            downstream_retry_symbol_codes
        )
        stage_result['pending_symbols'] = downstream_retry_symbol_codes[:50]
        return stage_result

    try:
        if should_defer_daily_followups:
            result['fundamentals_followup'] = (
                build_downstream_recovery_pending_result(
                    'fundamentals_daily_refresh'
                )
            )
            record_followup_result(
                'fundamentals_daily_refresh',
                result['fundamentals_followup'],
            )
            result['piotroski_filter_followup'] = (
                build_downstream_recovery_pending_result(
                    'piotroski_filter'
                )
            )
            record_followup_result(
                'piotroski_filter',
                result['piotroski_filter_followup'],
            )
            result['factor_scoring_followup'] = (
                build_downstream_recovery_pending_result('momentum_factors')
            )
            record_factor_followup_result(result['factor_scoring_followup'])
            result['signal_feature_followup'] = (
                build_downstream_recovery_pending_result('signal_features')
            )
            record_followup_result(
                'signal_features',
                result['signal_feature_followup'],
            )
            result['signal_features'] = result['signal_feature_followup']
            result['external_news_followup'] = (
                build_downstream_recovery_pending_result(
                    'external_news_collect'
                )
            )
            record_followup_result(
                'external_news_collect',
                result['external_news_followup'],
            )
            result['external_news_enrichment_followup'] = (
                build_downstream_recovery_pending_result(
                    'external_news_enrich'
                )
            )
            record_followup_result(
                'external_news_enrich',
                result['external_news_enrichment_followup'],
            )
            result['macro_context_followup'] = (
                build_downstream_recovery_pending_result(
                    'macro_context_refresh'
                )
            )
            record_followup_result(
                'macro_context_refresh',
                result['macro_context_followup'],
            )
            result['intraday_snapshot_followup'] = (
                build_downstream_recovery_pending_result(
                    'intraday_snapshots'
                )
            )
            record_followup_result(
                'intraday_snapshots',
                result['intraday_snapshot_followup'],
            )
            result['feedback_followup'] = (
                build_downstream_recovery_pending_result(
                    'agent_feedback_summaries'
                )
            )
            record_followup_result(
                'agent_feedback_summaries',
                result['feedback_followup'],
            )
            result['signal_decider_followup'] = (
                build_downstream_recovery_pending_result('signal_decider')
            )
            record_followup_result(
                'signal_decider',
                result['signal_decider_followup'],
            )
            result['next_open_execution_plan_followup'] = (
                build_downstream_recovery_pending_result(
                    'next_open_execution_plan'
                )
            )
            record_followup_result(
                'next_open_execution_plan',
                result['next_open_execution_plan_followup'],
            )
            result['signal_decider_evaluation_followup'] = (
                build_downstream_recovery_pending_result(
                    'signal_decider_evaluations'
                )
            )
            record_followup_result(
                'signal_decider_evaluations',
                result['signal_decider_evaluation_followup'],
            )
        elif (
            point_in_time_recovery
            and chain_fundamentals_followup
            and mode == 'daily_auto'
            and status in {'completed', 'completed_with_errors'}
        ):
            for result_key, stage_id in (
                ('fundamentals_followup', 'fundamentals_daily_refresh'),
                ('signal_feature_followup', 'signal_features'),
                ('external_news_followup', 'external_news_collect'),
                ('external_news_enrichment_followup', 'external_news_enrich'),
                ('macro_context_followup', 'macro_context_refresh'),
            ):
                stage_result = _build_point_in_time_current_only_skip(
                    stage_id=stage_id,
                    market_date=latest_trade_date,
                )
                result[result_key] = stage_result
                record_followup_result(stage_id, stage_result)
            result['signal_features'] = result['signal_feature_followup']
            result['intraday_snapshot_followup'] = (
                _build_point_in_time_current_only_skip(
                    stage_id='intraday_snapshots',
                    market_date=latest_trade_date,
                )
            )
            record_followup_result(
                'intraday_snapshots',
                result['intraday_snapshot_followup'],
            )

            pitr_piotroski_invocation_symbols = list(
                daily_followup_admitted_symbols
            )
            try:
                pitr_piotroski_kwargs: dict[str, Any] = {
                    'market_date': latest_trade_date,
                    'exact_membership_symbols': (
                        exact_piotroski_membership_symbols
                    ),
                }
                if set(pitr_piotroski_invocation_symbols) != set(
                    exact_followup_symbols
                ):
                    pitr_piotroski_kwargs['admitted_symbols'] = list(
                        pitr_piotroski_invocation_symbols
                    )
                result['piotroski_filter_followup'] = run_followup_stage(
                    'piotroski_filter',
                    activity=(
                        'Rebuilding as-of Piotroski filter for '
                        'point-in-time recovery'
                    ),
                    call=lambda: _run_daily_piotroski_filter_followup(
                        **pitr_piotroski_kwargs
                    ),
                    default_total=len(requested_symbol_codes),
                )
            except Exception as exc:
                result['piotroski_filter_followup'] = {
                    'triggered': True,
                    'status': 'failed',
                    'reason': 'piotroski_filter_failed',
                    'market_date': latest_trade_date,
                    'error_type': type(exc).__name__,
                    'error_message': str(exc),
                }
            pitr_piotroski_admitted_symbols = validated_successful_cohort(
                stage_result=result['piotroski_filter_followup'],
                invocation_symbols=pitr_piotroski_invocation_symbols,
                stage_id='piotroski_filter',
            )
            if pitr_piotroski_admitted_symbols:
                factor_kwargs: dict[str, Any] = {}
                if set(pitr_piotroski_admitted_symbols) != set(
                    exact_followup_symbols
                ):
                    factor_kwargs['admitted_symbols'] = list(
                        pitr_piotroski_admitted_symbols
                    )
                result['factor_scoring_followup'] = run_factor_followup(
                    **factor_kwargs
                )
                if (
                    result['factor_scoring_followup'].get('reason')
                    == 'pipeline_run_ownership_lost'
                ):
                    result['status'] = 'interrupted'
                    result['reason'] = 'pipeline_run_ownership_lost'
                    return result
                pitr_factor_admitted_symbols = (
                    validated_successful_cohort(
                        stage_result=result['factor_scoring_followup'],
                        invocation_symbols=(
                            pitr_piotroski_admitted_symbols
                        ),
                        stage_id='composite_factors',
                    )
                )
            else:
                pitr_factor_admitted_symbols = None
                result['factor_scoring_followup'] = (
                    build_blocked_followup_result(
                        'momentum_factors',
                        'piotroski_filter_incomplete',
                        upstream_status=_resolve_result_status(
                            result['piotroski_filter_followup']
                        ),
                    )
                )
                record_factor_followup_result(
                    result['factor_scoring_followup']
                )

            feedback_ready = bool(pitr_factor_admitted_symbols)
            if feedback_ready:
                # PITR is bound to a frozen historical membership.  Always
                # pass that exact proven cohort, even when it equals the full
                # parent membership, so the feedback service cannot
                # recompute current automatic eligibility and admit symbols
                # outside the recovery snapshot.
                feedback_scope_kwargs: dict[str, Any] = {
                    'symbols': list(pitr_factor_admitted_symbols or [])
                }
                result['feedback_followup'] = run_followup_stage(
                    'agent_feedback_summaries',
                    activity='Running point-in-time recovery feedback summaries',
                    call=lambda: _run_daily_feedback_followup(
                        market_date=latest_trade_date,
                        enabled=bool(
                            dependencies.settings.openclaw_feedback_trigger_after_fundamentals_refresh
                        ),
                        knowledge_cutoff_at=(
                            resolve_feedback_knowledge_cutoff()
                        ),
                        progress_callback=feedback_progress_callback,
                        point_in_time_recovery=point_in_time_recovery,
                        **feedback_scope_kwargs,
                    ),
                )
            else:
                result['feedback_followup'] = build_blocked_followup_result(
                    'agent_feedback_summaries',
                    'stored_recovery_inputs_incomplete',
                    upstream_status='blocked',
                )
                record_followup_result(
                    'agent_feedback_summaries',
                    result['feedback_followup'],
                )

            if _is_satisfied_followup_result(
                result['feedback_followup'],
                stage_id='agent_feedback_summaries',
            ):
                result['signal_decider_followup'] = run_followup_stage(
                    'signal_decider',
                    activity='Running point-in-time recovery signal deciders',
                    call=lambda: _run_daily_signal_decider_followup(
                        market_date=latest_trade_date,
                        enabled=bool(
                            dependencies.settings.openclaw_feedback_trigger_after_fundamentals_refresh
                        ),
                        knowledge_cutoff_at=(
                            resolve_feedback_knowledge_cutoff()
                        ),
                        progress_callback=signal_decider_progress_callback,
                        point_in_time_recovery=point_in_time_recovery,
                        **feedback_scope_kwargs,
                    ),
                )
            else:
                result['signal_decider_followup'] = build_blocked_followup_result(
                    'signal_decider',
                    'feedback_followup_incomplete',
                    upstream_status=_resolve_result_status(
                        result['feedback_followup']
                    ),
                )
                record_followup_result(
                    'signal_decider', result['signal_decider_followup']
                )

            if _is_satisfied_followup_result(
                result['signal_decider_followup'],
                stage_id='signal_decider',
            ):
                result['next_open_execution_plan_followup'] = run_followup_stage(
                    'next_open_execution_plan',
                    activity='Refreshing point-in-time recovery next-open plan',
                    call=lambda: _run_daily_next_open_execution_plan_followup(
                        market_date=latest_trade_date,
                        enabled=True,
                    ),
                )
                result['signal_decider_evaluation_followup'] = run_followup_stage(
                    'signal_decider_evaluations',
                    activity='Evaluating point-in-time recovery deciders',
                    call=lambda: _run_daily_signal_decider_evaluation_followup(
                        market_date=latest_trade_date,
                        enabled=True,
                    ),
                )
            else:
                for result_key, stage_id in (
                    ('next_open_execution_plan_followup', 'next_open_execution_plan'),
                    ('signal_decider_evaluation_followup', 'signal_decider_evaluations'),
                ):
                    stage_result = build_blocked_followup_result(
                        stage_id,
                        'signal_decider_followup_incomplete',
                        upstream_status=_resolve_result_status(
                            result['signal_decider_followup']
                        ),
                    )
                    result[result_key] = stage_result
                    record_followup_result(stage_id, stage_result)
        elif should_chain_fundamentals:
            fundamentals_refresh_kwargs: dict[str, Any] = {
                'market_date': latest_trade_date,
                'capture_limit': 500,
                'trigger': 'eod_pykrx_followup',
                'run_piotroski_followup': False,
                'run_feedback_followup': False,
                'progress_callback': fundamentals_progress_callback,
            }
            if set(daily_followup_admitted_symbols) != set(
                exact_followup_symbols
            ):
                fundamentals_refresh_kwargs['symbols'] = list(
                    daily_followup_admitted_symbols
                )
            result['fundamentals_followup'] = run_followup_stage(
                'fundamentals_daily_refresh',
                activity='Running DART fundamentals daily refresh follow-up',
                call=lambda: asyncio.run(
                    dependencies.dart_fundamentals_capture_service.run_daily_incremental_refresh(
                        **fundamentals_refresh_kwargs,
                    )
                ),
            )
            fundamentals_status = _resolve_result_status(result['fundamentals_followup'])
            fundamentals_admitted_symbols = validated_successful_cohort(
                stage_result=result['fundamentals_followup'],
                invocation_symbols=daily_followup_admitted_symbols,
                stage_id='fundamentals_daily_refresh',
            )
            fundamentals_satisfied = bool(fundamentals_admitted_symbols)
            if fundamentals_satisfied:
                try:
                    piotroski_kwargs: dict[str, Any] = {
                        'market_date': latest_trade_date,
                        'exact_membership_symbols': (
                            exact_piotroski_membership_symbols
                        ),
                        'authoritative_market_scope': (
                            daily_derived_market_scope_authority
                        ),
                    }
                    if set(fundamentals_admitted_symbols or []) != set(
                        exact_followup_symbols
                    ):
                        piotroski_kwargs['admitted_symbols'] = list(
                            fundamentals_admitted_symbols or []
                        )
                    result['piotroski_filter_followup'] = (
                        run_followup_stage(
                            'piotroski_filter',
                            activity='Rebuilding daily Piotroski filter follow-up',
                            call=lambda: (
                                _run_daily_piotroski_filter_followup(
                                    **piotroski_kwargs,
                                )
                            ),
                            default_total=len(requested_symbol_codes),
                        )
                    )
                except Exception as exc:
                    result['piotroski_filter_followup'] = {
                        'triggered': True,
                        'status': 'failed',
                        'reason': 'piotroski_filter_failed',
                        'market_date': latest_trade_date,
                        'error_type': type(exc).__name__,
                        'error_message': str(exc),
                    }
            else:
                result['piotroski_filter_followup'] = (
                    build_blocked_followup_result(
                        'piotroski_filter',
                        'fundamentals_followup_incomplete',
                        upstream_status=fundamentals_status,
                    )
                )
                record_followup_result(
                    'piotroski_filter',
                    result['piotroski_filter_followup'],
                )
            piotroski_status = _resolve_result_status(
                result['piotroski_filter_followup']
            )
            piotroski_admitted_symbols = (
                validated_successful_cohort(
                    stage_result=result['piotroski_filter_followup'],
                    invocation_symbols=fundamentals_admitted_symbols or [],
                    stage_id='piotroski_filter',
                )
                if fundamentals_satisfied
                else None
            )
            piotroski_satisfied = bool(piotroski_admitted_symbols)
            if fundamentals_satisfied and piotroski_satisfied:
                factor_kwargs: dict[str, Any] = {}
                if set(piotroski_admitted_symbols or []) != set(
                    exact_followup_symbols
                ):
                    factor_kwargs['admitted_symbols'] = list(
                        piotroski_admitted_symbols or []
                    )
                result['factor_scoring_followup'] = run_factor_followup(
                    **factor_kwargs
                )
                if (
                    result['factor_scoring_followup'].get('reason')
                    == 'pipeline_run_ownership_lost'
                ):
                    result['status'] = 'interrupted'
                    result['reason'] = 'pipeline_run_ownership_lost'
                    return result
                factor_status = _resolve_result_status(
                    result['factor_scoring_followup']
                )
                factor_admitted_symbols = validated_successful_cohort(
                    stage_result=result['factor_scoring_followup'],
                    invocation_symbols=piotroski_admitted_symbols or [],
                    stage_id='composite_factors',
                )
                if factor_admitted_symbols:
                    signal_feature_admitted_symbols = (
                        None
                        if set(factor_admitted_symbols)
                        == set(exact_followup_symbols)
                        else list(factor_admitted_symbols)
                    )
                    result['signal_feature_followup'] = (
                        run_signal_feature_followup()
                    )
                else:
                    result['signal_feature_followup'] = (
                        build_blocked_followup_result(
                            'signal_features',
                            'factor_scoring_followup_incomplete',
                            upstream_status=factor_status,
                        )
                    )
                    record_followup_result(
                        'signal_features',
                        result['signal_feature_followup'],
                    )
                result['signal_features'] = result['signal_feature_followup']
                signal_feature_invocation_symbols = (
                    factor_admitted_symbols or []
                )
                signal_feature_successful_symbols = (
                    validated_successful_cohort(
                        stage_result=result['signal_feature_followup'],
                        invocation_symbols=(
                            signal_feature_invocation_symbols
                        ),
                        stage_id='signal_features',
                    )
                    if signal_feature_invocation_symbols
                    else None
                )
                result['external_news_followup'] = run_followup_stage(
                    'external_news_collect',
                    activity='Running external news collection follow-up',
                    call=lambda: _run_daily_external_news_followup(
                        market_date=latest_trade_date,
                        enabled=True,
                    ),
                )
                result['external_news_enrichment_followup'] = run_followup_stage(
                    'external_news_enrich',
                    activity='Running external news classification enrichment follow-up',
                    call=lambda: _run_daily_external_news_enrichment_followup(
                        market_date=latest_trade_date,
                        enabled=True,
                    ),
                )
                result['macro_context_followup'] = run_followup_stage(
                    'macro_context_refresh',
                    activity='Refreshing freshness-gated macro context follow-up',
                    call=lambda: _run_daily_macro_context_followup(
                        market_date=latest_trade_date,
                        enabled=True,
                    ),
                )
            else:
                factor_block_reason = (
                    'piotroski_filter_incomplete'
                    if fundamentals_satisfied
                    else 'fundamentals_followup_incomplete'
                )
                result['factor_scoring_followup'] = {
                    'triggered': False,
                    'status': 'blocked',
                    'reason': factor_block_reason,
                    'market_date': latest_trade_date,
                    'fundamentals_status': fundamentals_status or None,
                    'piotroski_status': piotroski_status or None,
                }
                record_factor_followup_result(result['factor_scoring_followup'])
                result['signal_feature_followup'] = (
                    build_blocked_followup_result(
                        'signal_features',
                        'factor_scoring_followup_incomplete',
                        upstream_status='blocked',
                    )
                )
                record_followup_result(
                    'signal_features',
                    result['signal_feature_followup'],
                )
                result['signal_features'] = result['signal_feature_followup']
                result['external_news_followup'] = {
                    'triggered': False,
                    'reason': 'factor_scoring_followup_incomplete',
                    'market_date': latest_trade_date,
                    'fundamentals_status': fundamentals_status or None,
                }
                record_followup_result('external_news_collect', result['external_news_followup'])
                result['external_news_enrichment_followup'] = build_blocked_followup_result(
                    'external_news_enrich',
                    'external_news_collection_incomplete',
                    upstream_status=_resolve_result_status(result['external_news_followup']),
                )
                record_followup_result(
                    'external_news_enrich',
                    result['external_news_enrichment_followup'],
                )
                result['macro_context_followup'] = build_blocked_followup_result(
                    'macro_context_refresh',
                    'external_news_enrichment_incomplete',
                    upstream_status='blocked',
                )
                record_followup_result('macro_context_refresh', result['macro_context_followup'])
            factor_status = _resolve_result_status(
                result.get('factor_scoring_followup') or {}
            )
            factor_satisfied = _is_satisfied_followup_result(
                result.get('factor_scoring_followup') or {}
            )
            signal_feature_status = _resolve_result_status(
                result.get('signal_feature_followup') or {}
            )
            result['intraday_snapshot_followup'] = (
                run_intraday_snapshot_followup()
            )
            if (
                factor_satisfied
                and bool(signal_feature_successful_symbols)
            ):
                feedback_scope_kwargs: dict[str, Any] = {}
                if set(signal_feature_successful_symbols or []) != set(
                    exact_followup_symbols
                ):
                    feedback_scope_kwargs['symbols'] = list(
                        signal_feature_successful_symbols or []
                    )
                result['feedback_followup'] = run_followup_stage(
                    'agent_feedback_summaries',
                    activity='Running agent feedback summary follow-up',
                    call=lambda: _run_daily_feedback_followup(
                        market_date=latest_trade_date,
                        enabled=bool(dependencies.settings.openclaw_feedback_trigger_after_fundamentals_refresh),
                        knowledge_cutoff_at=(
                            resolve_feedback_knowledge_cutoff()
                        ),
                        progress_callback=feedback_progress_callback,
                        **feedback_scope_kwargs,
                    ),
                )
                feedback_status = _resolve_result_status(result['feedback_followup'])
                if _is_satisfied_followup_result(
                    result['feedback_followup'],
                    stage_id='agent_feedback_summaries',
                ):
                    result['signal_decider_followup'] = run_followup_stage(
                        'signal_decider',
                        activity='Running signal-decider follow-up',
                        call=lambda: _run_daily_signal_decider_followup(
                            market_date=latest_trade_date,
                            enabled=bool(dependencies.settings.openclaw_feedback_trigger_after_fundamentals_refresh),
                            knowledge_cutoff_at=(
                                resolve_feedback_knowledge_cutoff()
                            ),
                            progress_callback=signal_decider_progress_callback,
                            **feedback_scope_kwargs,
                        ),
                    )
                    signal_decider_status = _resolve_result_status(result['signal_decider_followup'])
                else:
                    result['signal_decider_followup'] = build_blocked_followup_result(
                        'signal_decider',
                        'feedback_followup_incomplete',
                        upstream_status=feedback_status,
                    )
                    record_followup_result('signal_decider', result['signal_decider_followup'])
                    signal_decider_status = 'blocked'
                if _is_satisfied_followup_result(
                    result['signal_decider_followup'],
                    stage_id='signal_decider',
                ):
                    result['next_open_execution_plan_followup'] = run_followup_stage(
                        'next_open_execution_plan',
                        activity='Refreshing next-open execution plan after signal deciders',
                        call=lambda: _run_daily_next_open_execution_plan_followup(
                            market_date=latest_trade_date,
                            enabled=True,
                        ),
                    )
                    result['signal_decider_evaluation_followup'] = run_followup_stage(
                        'signal_decider_evaluations',
                        activity='Running signal-decider evaluation follow-up',
                        call=lambda: _run_daily_signal_decider_evaluation_followup(
                            market_date=latest_trade_date,
                            enabled=True,
                        ),
                    )
                else:
                    result['next_open_execution_plan_followup'] = build_blocked_followup_result(
                        'next_open_execution_plan',
                        'signal_decider_followup_incomplete',
                        upstream_status=signal_decider_status,
                    )
                    record_followup_result(
                        'next_open_execution_plan',
                        result['next_open_execution_plan_followup'],
                    )
                    result['signal_decider_evaluation_followup'] = build_blocked_followup_result(
                        'signal_decider_evaluations',
                        'signal_decider_followup_incomplete',
                        upstream_status=signal_decider_status,
                    )
                    record_followup_result(
                        'signal_decider_evaluations',
                        result['signal_decider_evaluation_followup'],
                    )
            else:
                downstream_reason = (
                    'factor_scoring_followup_incomplete'
                    if not factor_satisfied
                    else 'signal_feature_followup_incomplete'
                )
                downstream_status = (
                    factor_status
                    if not factor_satisfied
                    else signal_feature_status
                )
                result['feedback_followup'] = build_blocked_followup_result(
                    'agent_feedback_summaries',
                    downstream_reason,
                    upstream_status=downstream_status,
                )
                record_followup_result('agent_feedback_summaries', result['feedback_followup'])
                result['signal_decider_followup'] = build_blocked_followup_result(
                    'signal_decider',
                    downstream_reason,
                    upstream_status=downstream_status,
                )
                record_followup_result('signal_decider', result['signal_decider_followup'])
                result['next_open_execution_plan_followup'] = build_blocked_followup_result(
                    'next_open_execution_plan',
                    downstream_reason,
                    upstream_status=downstream_status,
                )
                record_followup_result(
                    'next_open_execution_plan',
                    result['next_open_execution_plan_followup'],
                )
                result['signal_decider_evaluation_followup'] = build_blocked_followup_result(
                    'signal_decider_evaluations',
                    downstream_reason,
                    upstream_status=downstream_status,
                )
                record_followup_result(
                    'signal_decider_evaluations',
                    result['signal_decider_evaluation_followup'],
                )
        elif chain_fundamentals_followup and mode == 'daily_auto' and status in {'completed', 'completed_with_errors'}:
            result['fundamentals_followup'] = {
                'triggered': False,
                'reason': 'dart_api_key_not_configured',
                'market_date': latest_trade_date,
            }
            record_followup_result('fundamentals_daily_refresh', result['fundamentals_followup'])
            result['piotroski_filter_followup'] = (
                build_blocked_followup_result(
                    'piotroski_filter',
                    'dart_api_key_not_configured',
                    upstream_status='skipped',
                )
            )
            record_followup_result(
                'piotroski_filter',
                result['piotroski_filter_followup'],
            )
            result['factor_scoring_followup'] = {
                'triggered': False,
                'status': 'blocked',
                'reason': 'dart_api_key_not_configured',
                'market_date': latest_trade_date,
            }
            record_factor_followup_result(result['factor_scoring_followup'])
            result['signal_feature_followup'] = build_blocked_followup_result(
                'signal_features',
                'factor_scoring_followup_incomplete',
                upstream_status='blocked',
            )
            record_followup_result(
                'signal_features',
                result['signal_feature_followup'],
            )
            result['signal_features'] = result['signal_feature_followup']
            result['external_news_followup'] = run_followup_stage(
                'external_news_collect',
                activity='Running external news collection follow-up',
                call=lambda: _run_daily_external_news_followup(
                    market_date=latest_trade_date,
                    enabled=True,
                ),
            )
            result['external_news_enrichment_followup'] = run_followup_stage(
                'external_news_enrich',
                activity='Running external news classification enrichment follow-up',
                call=lambda: _run_daily_external_news_enrichment_followup(
                    market_date=latest_trade_date,
                    enabled=True,
                ),
            )
            result['macro_context_followup'] = run_followup_stage(
                'macro_context_refresh',
                activity='Refreshing freshness-gated macro context follow-up',
                call=lambda: _run_daily_macro_context_followup(
                    market_date=latest_trade_date,
                    enabled=True,
                ),
            )
            result['intraday_snapshot_followup'] = (
                run_intraday_snapshot_followup()
            )
            result['feedback_followup'] = build_blocked_followup_result(
                'agent_feedback_summaries',
                'factor_scoring_followup_incomplete',
                upstream_status='blocked',
            )
            record_followup_result('agent_feedback_summaries', result['feedback_followup'])
            result['signal_decider_followup'] = build_blocked_followup_result(
                'signal_decider',
                'factor_scoring_followup_incomplete',
                upstream_status='blocked',
            )
            record_followup_result('signal_decider', result['signal_decider_followup'])
            result['next_open_execution_plan_followup'] = build_blocked_followup_result(
                'next_open_execution_plan',
                'factor_scoring_followup_incomplete',
                upstream_status='blocked',
            )
            record_followup_result(
                'next_open_execution_plan',
                result['next_open_execution_plan_followup'],
            )
            result['signal_decider_evaluation_followup'] = build_blocked_followup_result(
                'signal_decider_evaluations',
                'factor_scoring_followup_incomplete',
                upstream_status='blocked',
            )
            record_followup_result(
                'signal_decider_evaluations',
                result['signal_decider_evaluation_followup'],
            )
    except Exception as exc:
        daily_pipeline_tracker.finish_run_preserving_primary_error(
            error_message=str(exc)
        )
        raise
    daily_pipeline_tracker.finish_run()
    if retry_context is not None:
        result['retry'] = retry_context
    dependencies.log_fetch('run_collection', status, **{key: value for key, value in result.items() if key != 'status'})
    return result
