from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
import logging
import math
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from app.agent_context.derived_contexts import daily_price_context_service
from app.agent_context.roles import (
    DECISION_SUPPORT_AGENT_NAMES,
    SUMMARIZER_AGENT_NAMES,
)
from app.agent_context.prerequisite_service import (
    FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER,
)
from app.agent_context.run_state_service import (
    agent_context_feedback_run_state_service,
)
from app.agent_context.retry_service import (
    resolve_latest_completed_summary_knowledge_cutoff,
)
from app.agent_context.run_modes import (
    DAILY_RETRY_CYCLE_MODE,
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
    OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION,
    OPENCLAW_SHARED_DISPATCH_RETRYABLE_FAILURE_KINDS,
)
from app.core.audit_logging import log_fetch_event
from app.core.settings import settings
from app.eod.daily_pipeline_outcomes import (
    ORDERING_ONLY_OPTIONAL_CONTEXT_STAGE_IDS,
    daily_pipeline_outcome_is_satisfied,
    daily_pipeline_prefix_outcome_is_satisfied,
    terminal_continuation_feedback_lease_loss_run_id,
    terminal_continuation_marker_is_valid,
)
from app.eod.master import get_master_symbols
from app.eod.finality import validate_finalized_krx_write_date
from app.eod.market_membership import (
    build_market_membership_snapshot,
    market_membership_sha256,
    validate_market_membership_snapshot,
)
from app.eod.pykrx_pipeline_support import (
    build_daily_retry_context,
    build_daily_skip_result,
    compact_daily_pipeline_result,
)
from app.eod.run_stage_support import (
    AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER,
    SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT,
    SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY,
    SAME_DAY_INTRADAY_WATCH_REARM_VERSION,
    is_completed_auto_pitr_intraday_watch_recovery,
    is_canonical_same_day_signal_rearm_proof,
    is_exact_auto_pitr_intraday_watch_recovery_request,
    is_nonblocking_quality_only_piotroski_partial,
    is_nonblocking_quality_only_signal_features_partial,
    is_recoverable_same_day_intraday_watch_expiration,
    outcome_contains_reason,
    pending_same_day_intraday_watch_rearm_marker,
    validated_nonblocking_partial_progression_marker,
)
from app.eod.scope_outcomes import (
    FOLLOWUP_STAGE_RECOVERY_SCOPE_SCHEMA_VERSION,
    RECOVERY_PRICE_SCOPE_OUTCOME_SCHEMA_VERSION,
    STAGE_ATOMIC_FINALIZATION_STAGE_IDS,
    STAGE_EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS,
    build_exact_symbol_scope_outcome,
    exact_scope_partition_identity_proof_matches,
    validate_stage_bound_scope_outcome,
)
from app.eod.summary_cutoff_contract import (
    completed_summary_scope_matches_parent,
    is_resumable_summary_cutoff_contract_repair,
    is_summary_cutoff_contract_repair_candidate as _is_summary_cutoff_contract_repair_candidate,
    stage_result_summary as _stage_result_summary,
    summary_cutoff_contract_repair_marker_is_valid,
    summary_cutoff_proof_from_stage as _summary_cutoff_proof_from_stage,
)
from app.eod.pykrx_import import (
    import_pykrx_stock,
    pykrx_request_context,
)
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service
from app.eod.daily_pipeline_state import (
    _terminal_continuation_execution_context_error,
    daily_pipeline_run_state_service,
)
from app.eod.pykrx_enrichment import pykrx_enrichment_service
from app.eod.action_summaries import daily_action_summary_service
from app.eod.pykrx_pipeline_runner import (
    AUTO_DAILY_FOLLOWUP_STAGE_ORDER,
    AUTO_DAILY_TRACKED_STAGE_ORDER,
    FACTOR_FOLLOWUP_STAGE_DEPENDENCIES,
    FACTOR_FOLLOWUP_STAGE_ORDER,
    HISTORICAL_BACKFILL_STAGE_ORDER,
    NON_DART_FACTOR_FOLLOWUP_STAGE_IDS,
    POINT_IN_TIME_RECOVERY_BOUNDARY_TRIGGERS,
    PykrxPipelineDependencies,
    _validated_exact_scope_outcome,
    resolve_point_in_time_recovery,
    run_market_data_pipeline,
)
from app.market.session import latest_krx_trading_date
from app.eod.technical_snapshots import daily_technical_snapshot_service
from app.signals.daily_signal_features import daily_signal_feature_service
from app.signals.market_risk_filter import daily_market_risk_filter_service
from app.market_data_store import (
    advisory_eod_collection_lock,
    advisory_eod_collection_locks,
    compute_and_persist_daily_trending_ranks,
    count_eod_run_persisted_symbols,
    count_eod_collection_runs,
    complete_eod_collection_run,
    create_eod_collection_run,
    load_daily_price_bar_dates_for_symbol,
    list_symbols_with_daily_bars_in_range,
    load_eod_run_persisted_symbols,
    load_symbols,
    load_latest_eod_collection_run,
    persist_daily_price_bars,
    update_daily_price_bar_fundamentals,
    update_eod_collection_run_progress,
    upsert_symbol_universe,
)
from app.market.symbol_codes import normalize_krx_short_code
from app.repositories.eod_run_repository import (
    load_automatic_daily_eod_collection_run_by_id,
    load_exact_automatic_daily_eod_collection_run,
    load_latest_pykrx_enrichment_repair_checkpoint,
)

logger = logging.getLogger(__name__)


class _LazyPykrxStock:
    def __getattr__(self, name: str) -> Any:
        try:
            pykrx_stock = import_pykrx_stock()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f'PyKRX stock client is unavailable: {exc}') from exc
        return getattr(pykrx_stock, name)


stock = _LazyPykrxStock()

DEFAULT_MARKETS = ('KOSPI', 'KOSDAQ', 'KONEX')
MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE = 5
AUTO_PYKRX_DAILY_RETRY_MIN_INTERVAL_SECONDS = 5 * 60
MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE = 5
MAX_AUTO_FEEDBACK_REARM_RUNS_PER_TRADE_DATE = 3
FEEDBACK_RETRY_PARENT_CONTRACT_VERSION = 'feedback_retry_parent_v1'
AUTO_FOLLOWUP_RECOVERY_MIN_INTERVAL_SECONDS = 5 * 60
AUTO_FOLLOWUP_PROVIDER_OUTAGE_RETRY_INTERVAL_SECONDS = 60 * 60
AUTO_FOLLOWUP_RECOVERY_TRIGGER = 'auto_eod_followup_resume'
AUTO_EOD_PIPELINE_ENTRYPOINT = 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due'
REQUIRED_PROVIDER_CREDENTIAL_SETTINGS = {
    'dart': {'DART_API_KEY': 'dart_api_key'},
    'kis': {
        'KIS_APP_KEY': 'kis_app_key',
        'KIS_APP_SECRET': 'kis_app_secret',
    },
}
REQUIRED_PROVIDER_STAGE_IDS = {
    'dart': frozenset({'fundamentals_daily_refresh'}),
    'kis': frozenset({'market_risk_snapshots', 'market_risk_filter'}),
}
FOLLOWUP_STAGE_DEBT_METADATA_KEY = 'followup_stage_debt'
FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION = 'followup_stage_debt_v3'
FOLLOWUP_STAGE_PROOF_PUBLICATION_SCOPE_KEY = (
    '_validated_publication_stock_codes'
)
FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY = '_validated_recovery_lineage'
FOLLOWUP_STAGE_RECOVERY_LINEAGE_SCHEMA_VERSION = (
    'followup_recovery_lineage_v1'
)
FOLLOWUP_STAGE_DEBT_TERMINAL_PROGRESSION_METADATA_KEY = (
    'followup_stage_debt_terminal_progression'
)
FOLLOWUP_STAGE_DEBT_TERMINAL_PROGRESSION_SCHEMA_VERSION = (
    'followup_stage_debt_terminal_progression_v1'
)
FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER = (
    'fundamentals_daily_refresh',
    'piotroski_filter',
    *FACTOR_FOLLOWUP_STAGE_ORDER,
    'signal_features',
)
FOLLOWUP_EXACT_STAGE_MANIFEST_VERSIONS = {
    'fundamentals_daily_refresh': frozenset(
        {
            'dart_daily_data_scope_outcomes_v1',
            'dart_fundamentals_scope_outcomes_v1',
        }
    ),
    'piotroski_filter': frozenset({'piotroski_scope_outcomes_v1'}),
    **{
        stage_id: frozenset({'factor_scope_outcomes_v1'})
        for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER
    },
    'signal_features': frozenset({'signal_features_scope_outcomes_v1'}),
}
FOLLOWUP_EXACT_STAGE_RECOVERY_MANIFEST_VERSION = {
    'fundamentals_daily_refresh': 'dart_fundamentals_scope_outcomes_v1',
    'piotroski_filter': 'piotroski_scope_outcomes_v1',
    **{
        stage_id: 'factor_scope_outcomes_v1'
        for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER
    },
    'signal_features': 'signal_features_scope_outcomes_v1',
}
FOLLOWUP_EXACT_STAGE_DEPENDENCIES = {
    'piotroski_filter': ('fundamentals_daily_refresh',),
    **{
        stage_id: ('piotroski_filter',)
        for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER
        if stage_id != 'composite_factors'
    },
    **FACTOR_FOLLOWUP_STAGE_DEPENDENCIES,
    'signal_features': FACTOR_FOLLOWUP_STAGE_ORDER,
}


def _is_price_cohort_factor_stage(
    stage_id: str,
    accounted: set[str],
    dependency_scope: set[str],
) -> bool:
    """True when a price-only factor ran on the price cohort (e8b19f29).

    Since 2026-11-06 momentum/value/low_risk/beta run on the follow-up price
    cohort instead of the Piotroski success set. Their exact Piotroski dependency
    still describes older runs, so a stage that accounts for symbols outside the
    Piotroski-derived scope is judged against the parent admission cohort and
    inherits no DART closure. Older runs keep the dependency rules unchanged.
    """

    return stage_id in NON_DART_FACTOR_FOLLOWUP_STAGE_IDS and bool(
        accounted - dependency_scope
    )
RECOVERED_FOLLOWUP_ADMISSION_STAGE_IDS = (
    'pykrx_enrichment',
    'technical_snapshots',
    'action_summaries',
)
TERMINAL_FOLLOWUP_CONTINUATION_STAGES = (
    'agent_feedback_summaries',
    'signal_decider',
    'next_open_execution_plan',
)
LEGACY_TERMINAL_FOLLOWUP_CONTINUATION_STAGES = (
    *TERMINAL_FOLLOWUP_CONTINUATION_STAGES,
    'signal_decider_evaluations',
)
SIGNAL_DECIDER_RECOVERY_STAGES = (
    'signal_decider',
    'next_open_execution_plan',
)
LEGACY_SIGNAL_DECIDER_RECOVERY_STAGES = (
    *SIGNAL_DECIDER_RECOVERY_STAGES,
    'signal_decider_evaluations',
)
CURRENT_FEEDBACK_ORDERING_PREREQUISITE_STAGES = (
    *FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER,
    'signal_features',
)
RETRYABLE_DAILY_STATUSES = {'completed_with_errors', 'failed'}
PYKRX_STAGE_SEQUENCE = (
    'price_collection',
    'pykrx_enrichment',
    'technical_snapshots',
    'action_summaries',
    'trending_ranks',
    'signal_features',
    'market_risk_filter',
)


def _resolved_terminal_followup_continuation_stages(
    resolved_stages: object,
) -> tuple[str, ...] | None:
    if not isinstance(resolved_stages, (list, tuple)):
        return None
    normalized = tuple(str(stage_id or '').strip() for stage_id in resolved_stages)
    if not normalized or any(not stage_id for stage_id in normalized):
        return None
    for stage_order in (
        LEGACY_TERMINAL_FOLLOWUP_CONTINUATION_STAGES,
        TERMINAL_FOLLOWUP_CONTINUATION_STAGES,
    ):
        if normalized[-len(stage_order) :] == stage_order:
            return stage_order
    return None


def _resolved_signal_decider_recovery_stages(
    resolved_stages: object,
) -> tuple[str, ...] | None:
    if not isinstance(resolved_stages, (list, tuple)):
        return None
    normalized = tuple(str(stage_id or '').strip() for stage_id in resolved_stages)
    if normalized == LEGACY_SIGNAL_DECIDER_RECOVERY_STAGES:
        return LEGACY_SIGNAL_DECIDER_RECOVERY_STAGES
    if normalized == SIGNAL_DECIDER_RECOVERY_STAGES:
        return SIGNAL_DECIDER_RECOVERY_STAGES
    return None


def _legacy_signal_suffix_projection(
    pipeline_run: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(pipeline_run, Mapping):
        return None
    metadata = pipeline_run.get('run_metadata')
    stages = pipeline_run.get('stages')
    if not (
        isinstance(metadata, Mapping)
        and isinstance(stages, Mapping)
        and tuple(metadata.get('resolved_stages') or ())
        == LEGACY_SIGNAL_DECIDER_RECOVERY_STAGES
        and str(pipeline_run.get('current_stage') or '').strip()
        == LEGACY_SIGNAL_DECIDER_RECOVERY_STAGES[-1]
        and set(stages) == set(LEGACY_SIGNAL_DECIDER_RECOVERY_STAGES)
    ):
        return None
    projected_metadata = dict(metadata)
    projected_metadata['resolved_stages'] = list(
        SIGNAL_DECIDER_RECOVERY_STAGES
    )
    return {
        **dict(pipeline_run),
        'current_stage': SIGNAL_DECIDER_RECOVERY_STAGES[-1],
        'run_metadata': projected_metadata,
        'stages': {
            stage_id: stages[stage_id]
            for stage_id in SIGNAL_DECIDER_RECOVERY_STAGES
        },
    }


def _is_recoverable_same_day_intraday_watch_expiration_compatible(
    pipeline_run: Mapping[str, Any] | None,
    *,
    current_market_date: date | str,
) -> bool:
    if is_recoverable_same_day_intraday_watch_expiration(
        pipeline_run,
        current_market_date=current_market_date,
    ):
        return True
    projected_run = _legacy_signal_suffix_projection(pipeline_run)
    if projected_run is None:
        return False
    return is_recoverable_same_day_intraday_watch_expiration(
        projected_run,
        current_market_date=current_market_date,
    )


def _is_canonical_same_day_signal_rearm_proof_compatible(
    pipeline_run: Mapping[str, Any] | None,
    *,
    expected_eod_collection_run_id: int | None = None,
) -> bool:
    if is_canonical_same_day_signal_rearm_proof(
        pipeline_run,
        expected_eod_collection_run_id=expected_eod_collection_run_id,
    ):
        return True
    projected_run = _legacy_signal_suffix_projection(pipeline_run)
    return bool(
        projected_run is not None
        and is_canonical_same_day_signal_rearm_proof(
            projected_run,
            expected_eod_collection_run_id=(
                expected_eod_collection_run_id
            ),
        )
    )


def _log_pykrx_fetch(operation: str, status: str, **fields) -> None:
    bounded_fields = (
        compact_daily_pipeline_result(fields)
        if operation in {
            'run_daily_pipeline_if_due',
            'run_daily_pipeline_stages',
        }
        else fields
    )
    log_fetch_event(
        service='pykrx',
        operation=operation,
        status=status,
        **(bounded_fields or {}),
    )


@dataclass
class PykrxSymbol:
    symbol: str
    market: str
    name_ko: str
    standard_code: str | None = None


class PointInTimeRetrySymbols(list[PykrxSymbol]):
    """Retry subset carrying the full exact-date membership proof."""

    def __init__(
        self,
        symbols: Sequence[PykrxSymbol],
        *,
        universe_snapshot: dict[str, Any],
    ) -> None:
        super().__init__(symbols)
        self.universe_snapshot = universe_snapshot


class PointInTimeRetryCodes(list[str]):
    """Missing-symbol codes carrying the full exact-date membership proof."""

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        universe_snapshot: dict[str, Any],
        unexpected_persisted_symbols: Sequence[str] = (),
    ) -> None:
        super().__init__(symbols)
        self.universe_snapshot = universe_snapshot
        self.unexpected_persisted_symbols = _dedupe_symbol_codes(
            unexpected_persisted_symbols
        )
        self.coverage_mismatch = bool(self.unexpected_persisted_symbols)


def _normalize_yyyymmdd(value: str | None, *, default: str | None = None) -> str:
    candidate = value or default
    if not candidate:
        raise ValueError('A date value is required.')
    normalized = candidate.replace('-', '')
    datetime.strptime(normalized, '%Y%m%d')
    return normalized


def _to_iso_date(value: object) -> str:
    if hasattr(value, 'date'):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _normalize_iso_date(value: str) -> str:
    return datetime.strptime(value, '%Y%m%d').date().isoformat()


def _current_krx_calendar_date() -> date:
    return datetime.now(ZoneInfo('Asia/Seoul')).date()


def _call_pykrx(func, *args, **kwargs):
    with pykrx_request_context(
        timeout_seconds=settings.eod_pykrx_request_timeout_seconds
    ):
        return func(*args, **kwargs)


def _coerce_pykrx_ticker_name(value: object, *, ticker: str) -> str:
    """Return a DB-safe ticker name from pykrx output.

    pykrx normally returns a plain string from ``get_market_ticker_name``.
    During provider/session instability it can return non-scalar objects (for
    example an HTML-parsed DataFrame).  Those objects cannot be bound into the
    symbols table by psycopg, so keep the daily pipeline moving by falling back
    to the ticker code and logging the unexpected shape for diagnosis.
    """

    if isinstance(value, str):
        normalized = value.strip()
        return normalized or ticker
    if value is None:
        return ticker

    logger.warning(
        'pykrx ticker name lookup returned non-string value for %s: %s',
        ticker,
        type(value).__name__,
    )
    return ticker


def _get_market_ohlcv_by_date(start_date: str, end_date: str, symbol: str):
    getter = getattr(stock, 'get_market_ohlcv_by_date', None) or getattr(stock, 'get_market_ohlcv')
    return _call_pykrx(getter, start_date, end_date, symbol)


def _get_market_ohlcv_by_ticker(trade_date: str, market: str):
    """Return market-wide OHLCV for one date.

    PyKRX's per-symbol date-range API can omit/zero 거래대금 depending on the
    endpoint/version, while the market-wide daily API carries provider trading
    value.  Use this as the authoritative turnover source for EOD bars.
    """
    getter = getattr(stock, 'get_market_ohlcv_by_ticker', None) or getattr(stock, 'get_market_ohlcv')
    return _call_pykrx(getter, trade_date, market)


def _get_market_fundamental_by_date(start_date: str, end_date: str, symbol: str):
    getter = getattr(stock, 'get_market_fundamental_by_date', None) or getattr(stock, 'get_market_fundamental')
    return _call_pykrx(getter, start_date, end_date, symbol)


def _is_auto_daily_trigger(trigger: str) -> bool:
    return trigger.startswith('auto_')


def _dedupe_symbol_codes(symbols: Sequence[object]) -> list[str]:
    deduped: dict[str, None] = {}
    for symbol in symbols:
        normalized = str(symbol).strip()
        if not normalized:
            continue
        if normalized.isdigit():
            normalized = normalized.zfill(6)
        deduped.setdefault(normalized, None)
    return list(deduped.keys())


def _select_summary_cutoff_proof(
    *,
    pipeline_stages: Mapping[str, Any],
    run_metadata: Mapping[str, Any] | None = None,
    eod_stage_progress: Mapping[str, Any],
    market_date: str | date | None = None,
    fs_div: str | None = None,
    parent_symbols: Sequence[object] | None = None,
) -> dict[str, Any] | None:
    """Prefer the newest summary and never mask its invalid lineage."""

    if 'agent_feedback_summaries' in pipeline_stages:
        summary_stage = pipeline_stages.get('agent_feedback_summaries')
        completed_proof = _summary_cutoff_proof_from_stage(summary_stage)
        if completed_proof is not None:
            return completed_proof
        return _partial_summary_cutoff_proof_from_stage(
            summary_stage,
            market_date=market_date,
            fs_div=fs_div,
            parent_symbols=parent_symbols,
        )
    if (
        isinstance(run_metadata, Mapping)
        and 'feedback_knowledge_cutoff' in run_metadata
    ):
        metadata_proof = _summary_cutoff_proof_from_run_metadata(
            run_metadata
        )
        if metadata_proof is not None:
            return metadata_proof
        persisted_cutoff = run_metadata.get(
            'feedback_knowledge_cutoff'
        )
        legacy_missing_summary_marker = bool(
            isinstance(persisted_cutoff, Mapping)
            and persisted_cutoff.get('ready') is False
            and str(persisted_cutoff.get('reason') or '').strip()
            == 'summary_run_missing'
            and not str(
                persisted_cutoff.get('source_run_id') or ''
            ).strip()
            and str(
                persisted_cutoff.get('feedback_scope_mode') or ''
            ).strip()
            == FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
        )
        if not legacy_missing_summary_marker:
            return None
    return _summary_cutoff_proof_from_stage(
        eod_stage_progress.get('agent_feedback_summaries')
    )


def _partial_summary_cutoff_proof_from_stage(
    stage: object,
    *,
    market_date: str | date | None,
    fs_div: str | None,
    parent_symbols: Sequence[object] | None,
) -> dict[str, Any] | None:
    """Resolve one terminal partial summary to its exact success cohort."""

    if not isinstance(stage, Mapping) or market_date is None:
        return None
    result_summary = _stage_result_summary(stage)
    if not (
        str(stage.get('status') or '').strip().lower() == 'partial'
        and str(result_summary.get('status') or '').strip().lower()
        == 'partial'
    ):
        return None
    source_run_ids = (
        stage.get('source_run_ids')
        if isinstance(stage.get('source_run_ids'), Mapping)
        else {}
    )
    source_ids = {
        str(value or '').strip()
        for value in (
            source_run_ids.get('agent_feedback_run_id'),
            source_run_ids.get('run_id'),
            result_summary.get('run_id'),
        )
        if str(value or '').strip()
    }
    normalized_fs_div = str(fs_div or '').strip().upper()
    feedback_scope_mode = str(
        result_summary.get('feedback_scope_mode') or ''
    ).strip()
    if not (
        len(source_ids) == 1
        and normalized_fs_div in {'CFS', 'OFS'}
        and feedback_scope_mode
        in {
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        }
        and isinstance(parent_symbols, (list, tuple))
    ):
        return None
    normalized_parent_symbols = _dedupe_symbol_codes(parent_symbols)
    if not (
        normalized_parent_symbols
        and normalized_parent_symbols == list(parent_symbols)
    ):
        return None

    resolution = resolve_latest_completed_summary_knowledge_cutoff(
        market_date=market_date,
        feedback_scope_mode=feedback_scope_mode,
        fs_div=normalized_fs_div,
        source_run_id=next(iter(source_ids)),
        # The exact source run owns its admitted scope. FAST mode may
        # legitimately be a subset of the pipeline parent; validate that
        # relationship from the returned versioned proof below.
        requested_symbols=None,
    )
    completed_symbols = resolution.get('requested_symbols')
    completed_scope_proof = resolution.get('completed_scope_proof')
    terminal_role_scope_proof = resolution.get(
        'terminal_role_scope_proof'
    )
    knowledge_cutoff_at = resolution.get('knowledge_cutoff_at')
    if not (
        resolution.get('ready') is True
        and isinstance(completed_symbols, list)
        and completed_symbols
        and isinstance(terminal_role_scope_proof, Mapping)
        and completed_summary_scope_matches_parent(
            feedback_scope_mode=feedback_scope_mode,
            completed_symbols=completed_symbols,
            completed_scope_proof=completed_scope_proof,
            parent_symbols=normalized_parent_symbols,
        )
        and isinstance(knowledge_cutoff_at, datetime)
        and knowledge_cutoff_at.tzinfo is not None
        and knowledge_cutoff_at.utcoffset() is not None
    ):
        return None
    return {
        'source_run_id': next(iter(source_ids)),
        'feedback_scope_mode': feedback_scope_mode,
        # Re-resolve the exact source in the stage service. The expected
        # completed cohort prevents any later widening or narrowing.
        'requested_symbols': None,
        'expected_completed_symbols': list(completed_symbols),
        'knowledge_cutoff_at': knowledge_cutoff_at.astimezone(
            timezone.utc
        ).isoformat(),
        'fs_div': normalized_fs_div,
        'completed_scope_proof': dict(completed_scope_proof),
        'terminal_role_scope_proof': dict(terminal_role_scope_proof),
    }


def _summary_cutoff_proof_from_run_metadata(
    run_metadata: Mapping[str, Any],
) -> dict[str, Any] | None:
    persisted_cutoff = run_metadata.get('feedback_knowledge_cutoff')
    if not isinstance(persisted_cutoff, Mapping):
        return None
    raw_cutoff_at = persisted_cutoff.get('knowledge_cutoff_at')
    try:
        cutoff_at = (
            raw_cutoff_at
            if isinstance(raw_cutoff_at, datetime)
            else datetime.fromisoformat(str(raw_cutoff_at or ''))
        )
    except (TypeError, ValueError):
        return None
    if not (
        persisted_cutoff.get('ready') is True
        and cutoff_at.tzinfo is not None
        and cutoff_at.utcoffset() is not None
    ):
        return None
    source_run_id = str(
        persisted_cutoff.get('source_run_id') or ''
    ).strip()
    feedback_scope_mode = str(
        persisted_cutoff.get('feedback_scope_mode') or ''
    ).strip()
    fs_div = str(run_metadata.get('fs_div') or '').strip().upper()
    requested_symbols = persisted_cutoff.get('requested_symbols')
    if not (
        source_run_id
        and feedback_scope_mode
        in {
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        }
        and fs_div in {'CFS', 'OFS'}
        and isinstance(requested_symbols, list)
    ):
        return None
    normalized_symbols = _dedupe_symbol_codes(requested_symbols)
    if not normalized_symbols or normalized_symbols != requested_symbols:
        return None
    return {
        'source_run_id': source_run_id,
        'feedback_scope_mode': feedback_scope_mode,
        # Persisted feedback metadata stores the completed summarizer
        # intersection, not the original admitted request scope. Re-resolve
        # the exact source without narrowing its request, then compare the
        # returned completed symbols below the stage boundary.
        'requested_symbols': None,
        'expected_completed_symbols': normalized_symbols,
        'knowledge_cutoff_at': cutoff_at.astimezone(
            timezone.utc
        ).isoformat(),
        'fs_div': fs_div,
    }


def _is_retired_decider_cutoff_zero_work_run(
    pipeline_run: object,
) -> bool:
    """Recognize only the zero-work suffix emitted by the retired bug."""

    if not isinstance(pipeline_run, Mapping):
        return False
    metadata = pipeline_run.get('run_metadata')
    stages = pipeline_run.get('stages')
    if not isinstance(metadata, Mapping) or not isinstance(stages, Mapping):
        return False
    cutoff = metadata.get('feedback_knowledge_cutoff')
    expected_stage_ids = LEGACY_SIGNAL_DECIDER_RECOVERY_STAGES
    if not (
        str(pipeline_run.get('trigger') or '').strip()
        == AUTO_FOLLOWUP_RECOVERY_TRIGGER
        and str(pipeline_run.get('status') or '').strip().lower()
        == 'blocked'
        and str(pipeline_run.get('current_stage') or '').strip()
        == 'signal_decider_evaluations'
        and tuple(metadata.get('resolved_stages') or ())
        == expected_stage_ids
        and str(metadata.get('resume_from_stage') or '').strip()
        == 'signal_decider'
        and type(metadata.get('reused_eod_collection_run_id')) is int
        and int(metadata['reused_eod_collection_run_id']) > 0
        and isinstance(cutoff, Mapping)
        and cutoff.get('ready') is False
        and str(cutoff.get('reason') or '').strip()
        == 'summary_cutoff_proof_unavailable'
        and all(
            cutoff.get(field) is None
            for field in (
                'knowledge_cutoff_at',
                'source_run_id',
                'feedback_scope_mode',
                'requested_symbols',
            )
        )
        and set(stages) == set(expected_stage_ids)
        and str(metadata.get('fs_div') or '').strip().upper()
        in {'CFS', 'OFS'}
        and isinstance(metadata.get('markets'), list)
        and bool(metadata.get('markets'))
        and type(metadata.get('point_in_time_recovery')) is bool
    ):
        return False

    def zero_work_stage(stage_id: str, status: str) -> bool:
        stage = stages.get(stage_id)
        return bool(
            isinstance(stage, Mapping)
            and str(stage.get('status') or '').strip().lower() == status
            and isinstance(stage.get('source_run_ids'), Mapping)
            and not stage.get('source_run_ids')
            and all(
                type(stage.get(field)) is int
                and stage.get(field) == 0
                for field in (
                    'processed',
                    'total',
                    'succeeded',
                    'failed',
                    'remaining',
                )
            )
        )

    signal_summary = _stage_result_summary(stages['signal_decider'])
    plan_summary = _stage_result_summary(
        stages['next_open_execution_plan']
    )
    evaluation_summary = _stage_result_summary(
        stages['signal_decider_evaluations']
    )
    cutoff_resolution = signal_summary.get(
        'knowledge_cutoff_resolution'
    )
    return bool(
        all(zero_work_stage(stage_id, 'blocked') for stage_id in expected_stage_ids)
        and str(signal_summary.get('reason') or '').strip()
        == 'summary_knowledge_cutoff_unavailable'
        and not signal_summary.get('run_id')
        and 'openclaw_dispatch_circuit_breaker'
        not in signal_summary
        and isinstance(cutoff_resolution, Mapping)
        and cutoff_resolution.get('ready') is False
        and str(cutoff_resolution.get('reason') or '').strip()
        == 'summary_cutoff_proof_unavailable'
        and all(
            cutoff_resolution.get(field) is None
            for field in (
                'knowledge_cutoff_at',
                'source_run_id',
                'feedback_scope_mode',
                'requested_symbols',
            )
        )
        and str(plan_summary.get('reason') or '').strip()
        == 'signal_decider_stage_incomplete'
        and str(evaluation_summary.get('reason') or '').strip()
        == 'declared_dependencies_incomplete'
        and evaluation_summary.get('incomplete_dependencies')
        == ['signal_decider']
    )


def _is_transparent_core_restart_signal_suffix_run(
    pipeline_run: object,
    *,
    allow_pending_marker_restart: bool = False,
) -> bool:
    """Recognize an exact interrupted signal suffix that adds no new summary.

    A process restart can interrupt a retry after some decider rows were
    durably reused or written but before the pipeline copied its immutable
    summary cutoff into run metadata.  Such an interruption is transparent
    while walking backwards for the canonical summary proof; it is not proof
    that the interrupted decision work itself completed.
    """

    if not isinstance(pipeline_run, Mapping):
        return False
    metadata = pipeline_run.get('run_metadata')
    stages = pipeline_run.get('stages')
    if not isinstance(metadata, Mapping) or not isinstance(stages, Mapping):
        return False
    expected_stage_ids = _resolved_signal_decider_recovery_stages(
        metadata.get('resolved_stages')
    )
    interruption_reason = str(
        metadata.get('interrupted_reason') or ''
    ).strip()
    if not (
        str(pipeline_run.get('trigger') or '').strip()
        == AUTO_FOLLOWUP_RECOVERY_TRIGGER
        and str(pipeline_run.get('status') or '').strip().lower()
        == 'interrupted'
        and expected_stage_ids is not None
        and str(metadata.get('resume_from_stage') or '').strip()
        == 'signal_decider'
        and type(metadata.get('reused_eod_collection_run_id')) is int
        and int(metadata['reused_eod_collection_run_id']) > 0
        and str(metadata.get('entrypoint') or '').strip()
        == 'daily_pipeline_stage_service.run'
        and metadata.get('point_in_time_recovery') is True
        and metadata.get('force') is False
        and metadata.get('reuse_existing_feedback') is True
        and metadata.get('allow_expired_analysis_replay') is False
        and metadata.get(
            'same_day_intraday_watch_continuation_admitted'
        ) is True
        and (
            _summary_cutoff_proof_from_run_metadata(metadata) is None
            or allow_pending_marker_restart
        )
        and not (
            'feedback_knowledge_cutoff' in metadata
            and _summary_cutoff_proof_from_run_metadata(metadata) is None
        )
        and metadata.get('resumable') is True
        and interruption_reason
        and _parse_activity_datetime(metadata.get('interrupted_at'))
        is not None
    ):
        return False
    current_stage = str(pipeline_run.get('current_stage') or '').strip()
    if not stages:
        return bool(
            allow_pending_marker_restart
            and current_stage in {'', 'signal_decider'}
        )
    signal_stage = stages.get('signal_decider')
    if not (
        current_stage == 'signal_decider'
        and set(stages) == {'signal_decider'}
        and isinstance(signal_stage, Mapping)
        and str(signal_stage.get('status') or '').strip().lower()
        == 'interrupted'
        and str(signal_stage.get('current_step') or '').strip()
        == 'core_restart_interrupted'
        and _parse_activity_datetime(signal_stage.get('completed_at'))
        is not None
        and str(signal_stage.get('error_message') or '').strip()
        == interruption_reason
        and isinstance(signal_stage.get('source_run_ids'), Mapping)
    ):
        return False
    details = signal_stage.get('details')
    interruption_is_consistent = bool(
        isinstance(details, Mapping)
        and details.get('resumable') is True
        and str(details.get('interrupted_reason') or '').strip()
        == interruption_reason
    )
    if not interruption_is_consistent:
        return False
    source_run_ids = signal_stage.get('source_run_ids')
    if str(source_run_ids.get('agent_feedback_run_id') or '').strip():
        return True
    return bool(
        allow_pending_marker_restart
        and not any(
            int(signal_stage.get(field) or 0)
            for field in ('processed', 'succeeded', 'failed')
        )
        and not any(
            str(value or '').strip() for value in source_run_ids.values()
        )
    )


def _is_resumable_same_day_intraday_watch_post_signal_restart(
    pipeline_run: object,
) -> bool:
    """Recognize one admitted PITR run whose signal result is already durable.

    This shape must be resumed in place.  Starting another signal suffix would
    replay a completed decision, while starting only the remaining stage would
    lose the exact persisted recovery contract used to clear the durable EOD
    marker.
    """

    if not isinstance(pipeline_run, Mapping):
        return False
    metadata = pipeline_run.get('run_metadata')
    stages = pipeline_run.get('stages')
    expected_stage_ids = _resolved_signal_decider_recovery_stages(
        (metadata or {}).get('resolved_stages')
    )
    interruption_reason = str(
        (metadata or {}).get('interrupted_reason') or ''
    ).strip()
    if not (
        isinstance(metadata, Mapping)
        and isinstance(stages, Mapping)
        and bool(stages)
        and expected_stage_ids is not None
        and set(stages).issubset(set(expected_stage_ids))
        and str(pipeline_run.get('trigger') or '').strip()
        == AUTO_FOLLOWUP_RECOVERY_TRIGGER
        and str(pipeline_run.get('status') or '').strip().lower()
        == 'interrupted'
        and str(metadata.get('resume_from_stage') or '').strip()
        == expected_stage_ids[0]
        and type(metadata.get('reused_eod_collection_run_id')) is int
        and int(metadata['reused_eod_collection_run_id']) > 0
        and str(metadata.get('entrypoint') or '').strip()
        == 'daily_pipeline_stage_service.run'
        and metadata.get('point_in_time_recovery') is True
        and metadata.get('force') is False
        and metadata.get('reuse_existing_feedback') is True
        and metadata.get('allow_expired_analysis_replay') is False
        and metadata.get(
            'same_day_intraday_watch_continuation_admitted'
        ) is True
        and _summary_cutoff_proof_from_run_metadata(metadata) is not None
        and metadata.get('resumable') is True
        and interruption_reason
        and _parse_activity_datetime(metadata.get('interrupted_at'))
        is not None
    ):
        return False

    def satisfied(stage: object) -> bool:
        return bool(
            isinstance(stage, Mapping)
            and daily_pipeline_outcome_is_satisfied(stage)
            and _parse_activity_datetime(stage.get('completed_at'))
            is not None
            and not str(stage.get('error_message') or '').strip()
        )

    signal_stage = stages.get(expected_stage_ids[0])
    signal_sources = (
        signal_stage.get('source_run_ids')
        if isinstance(signal_stage, Mapping)
        else None
    )
    if not (
        satisfied(signal_stage)
        and isinstance(signal_sources, Mapping)
        and str(
            signal_sources.get('agent_feedback_run_id')
            or signal_sources.get('run_id')
            or ''
        ).strip()
    ):
        return False

    satisfied_prefix: list[str] = []
    for stage_id in expected_stage_ids:
        if not satisfied(stages.get(stage_id)):
            break
        satisfied_prefix.append(stage_id)
    if not satisfied_prefix or len(satisfied_prefix) == len(
        expected_stage_ids
    ):
        return False
    first_unfinished_index = len(satisfied_prefix)
    first_unfinished_stage_id = expected_stage_ids[first_unfinished_index]
    first_unfinished_stage = stages.get(first_unfinished_stage_id)

    def untouched_pending(stage: object) -> bool:
        return bool(
            isinstance(stage, Mapping)
            and str(stage.get('status') or '').strip().lower() == 'pending'
            and not any(
                int(stage.get(field) or 0)
                for field in (
                    'processed',
                    'total',
                    'succeeded',
                    'failed',
                    'remaining',
                )
            )
            and stage.get('completed_at') is None
            and not str(stage.get('error_message') or '').strip()
            and isinstance(stage.get('source_run_ids'), Mapping)
            and not stage.get('source_run_ids')
        )

    first_status = (
        str(first_unfinished_stage.get('status') or '').strip().lower()
        if isinstance(first_unfinished_stage, Mapping)
        else 'absent'
    )
    current_stage = str(pipeline_run.get('current_stage') or '').strip()
    if first_status == 'absent':
        if current_stage not in {
            first_unfinished_stage_id,
            satisfied_prefix[-1],
        }:
            return False
    elif first_status == 'pending':
        if not (
            untouched_pending(first_unfinished_stage)
            and current_stage
            in {first_unfinished_stage_id, satisfied_prefix[-1]}
        ):
            return False
    elif first_status == 'interrupted':
        details = first_unfinished_stage.get('details')
        if not (
            current_stage == first_unfinished_stage_id
            and str(
                first_unfinished_stage.get('current_step') or ''
            ).strip()
            == 'core_restart_interrupted'
            and _parse_activity_datetime(
                first_unfinished_stage.get('completed_at')
            )
            is not None
            and str(
                first_unfinished_stage.get('error_message') or ''
            ).strip()
            == interruption_reason
            and isinstance(details, Mapping)
            and details.get('resumable') is True
            and str(details.get('interrupted_reason') or '').strip()
            == interruption_reason
        ):
            return False
    else:
        return False

    return all(
        stage_id not in stages or untouched_pending(stages.get(stage_id))
        for stage_id in expected_stage_ids[first_unfinished_index + 1 :]
    )


def _select_prior_signal_rearm_evidence(
    *,
    current_pipeline_run: Mapping[str, Any],
    prior_pipeline_runs: Sequence[Mapping[str, Any]],
    current_market_date: date | str | None = None,
    allow_transparent_current: bool = False,
) -> tuple[Mapping[str, Any], dict[str, Any]] | None:
    """Select one coherent prior signal circuit row from the same lineage."""

    recoverable_same_day_expiration = bool(
        current_market_date is not None
        and _is_recoverable_same_day_intraday_watch_expiration_compatible(
            current_pipeline_run,
            current_market_date=current_market_date,
        )
    )
    if not (
        _is_retired_decider_cutoff_zero_work_run(current_pipeline_run)
        or recoverable_same_day_expiration
        or (
            allow_transparent_current
            and (
                _is_transparent_core_restart_signal_suffix_run(
                    current_pipeline_run,
                    allow_pending_marker_restart=True,
                )
                or _is_resumable_same_day_intraday_watch_post_signal_restart(
                    current_pipeline_run
                )
            )
        )
    ):
        return None
    current_metadata = current_pipeline_run['run_metadata']
    current_cutoff_proof = _summary_cutoff_proof_from_run_metadata(
        current_metadata
    )
    transparent_cutoff_proofs = (
        [current_cutoff_proof]
        if current_cutoff_proof is not None
        else []
    )
    same_day_chronology_required = bool(
        recoverable_same_day_expiration or allow_transparent_current
    )
    source_false_skip_started_at = (
        _parse_aware_activity_datetime(
            current_pipeline_run.get('started_at')
        )
        if recoverable_same_day_expiration
        else None
    )
    if recoverable_same_day_expiration and (
        source_false_skip_started_at is None
    ):
        return None
    current_eod_run_id = int(
        current_metadata['reused_eod_collection_run_id']
    )
    lineage_market_date = _to_iso_date(
        current_pipeline_run.get('market_date')
    )
    current_run_id = int(current_pipeline_run.get('id') or 0)
    current_context = {
        'fs_div': str(current_metadata.get('fs_div') or '')
        .strip()
        .upper(),
        'markets': current_metadata.get('markets'),
        'point_in_time_recovery': current_metadata.get(
            'point_in_time_recovery'
        ),
    }
    previous_run_id = current_run_id
    for prior_run in prior_pipeline_runs:
        prior_metadata = prior_run.get('run_metadata')
        if not (
            isinstance(prior_metadata, Mapping)
            and 0 < int(prior_run.get('id') or 0) < previous_run_id
            and _to_iso_date(prior_run.get('market_date'))
            == lineage_market_date
            and prior_metadata.get('reused_eod_collection_run_id')
            == current_eod_run_id
            and str(prior_metadata.get('fs_div') or '')
            .strip()
            .upper()
            == current_context['fs_div']
            and prior_metadata.get('markets')
            == current_context['markets']
            and prior_metadata.get('point_in_time_recovery')
            is current_context['point_in_time_recovery']
        ):
            return None
        previous_run_id = int(prior_run['id'])
        transparent_restart = bool(
            _is_transparent_core_restart_signal_suffix_run(
                prior_run,
                allow_pending_marker_restart=allow_transparent_current,
            )
            or (
                allow_transparent_current
                and _is_resumable_same_day_intraday_watch_post_signal_restart(
                    prior_run
                )
            )
        )
        if transparent_restart:
            transparent_proof = _summary_cutoff_proof_from_run_metadata(
                prior_metadata
            )
            if transparent_proof is not None:
                transparent_cutoff_proofs.append(transparent_proof)
            continue
        prior_is_same_day_expiration = bool(
            current_market_date is not None
            and _is_recoverable_same_day_intraday_watch_expiration_compatible(
                prior_run,
                current_market_date=current_market_date,
            )
        )
        if prior_is_same_day_expiration:
            if source_false_skip_started_at is not None:
                return None
            source_false_skip_started_at = (
                _parse_aware_activity_datetime(
                    prior_run.get('started_at')
                )
            )
            if source_false_skip_started_at is None:
                return None
            continue
        if _is_retired_decider_cutoff_zero_work_run(prior_run):
            continue
        if not _is_retryable_feedback_rearm_candidate(prior_run):
            return None
        retry_stage_id = _feedback_retry_stage_id(prior_run)
        proof = _summary_cutoff_proof_from_run_metadata(
            prior_metadata
        )
        if retry_stage_id == 'signal_decider' and proof is not None:
            proof_started_at = _parse_aware_activity_datetime(
                prior_run.get('started_at')
            )
            if same_day_chronology_required and not (
                proof_started_at is not None
                and source_false_skip_started_at is not None
                and proof_started_at <= source_false_skip_started_at
            ):
                return None
            if any(
                transparent_proof != proof
                for transparent_proof in transparent_cutoff_proofs
            ):
                return None
            return prior_run, proof
        return None
    return None


def _is_retryable_missing_upstream_decider_failure(stage: object) -> bool:
    if not isinstance(stage, Mapping):
        return False
    if str(stage.get('status') or '').strip().lower() != 'failed':
        return False
    result_summary = _stage_result_summary(stage)
    if str(result_summary.get('status') or '').strip().lower() != 'failed':
        return False
    progress = result_summary.get('progress')
    summary = progress.get('summary') if isinstance(progress, Mapping) else None
    if not isinstance(summary, Mapping):
        return False
    failure_groups = summary.get('failure_groups')
    if not isinstance(failure_groups, Mapping) or len(failure_groups) != 1:
        return False
    try:
        failed_items = int(summary.get('failed_items') or 0)
    except (TypeError, ValueError):
        return False
    if failed_items <= 0:
        return False

    group = failure_groups.get('dependency/missing-upstream')
    if not isinstance(group, Mapping):
        return False
    try:
        group_count = int(group.get('count') or 0)
        retryable_count = int(group.get('retryable_count') or 0)
    except (TypeError, ValueError):
        return False
    failure_kinds = group.get('failure_kinds')
    normalized_failure_kinds = {
        str(item or '').strip().lower()
        for item in failure_kinds
        if str(item or '').strip()
    } if isinstance(failure_kinds, (list, tuple, set)) else set()
    return (
        str(group.get('failure_status') or '').strip().lower()
        == 'dependency/missing-upstream'
        and str(group.get('failure_category') or '').strip().lower()
        == 'dependency'
        and group_count == failed_items
        and retryable_count == failed_items
        and normalized_failure_kinds == {'dependency_missing_upstream'}
    )


def _is_terminal_followup_continuation_candidate(
    pipeline_run: object,
) -> bool:
    """Admit only the one narrow dependency-only terminal suffix retry."""

    if not isinstance(pipeline_run, Mapping):
        return False
    if pipeline_run.get('trigger') != AUTO_FOLLOWUP_RECOVERY_TRIGGER:
        return False
    pipeline_status = str(
        pipeline_run.get('status') or ''
    ).strip().lower()
    run_metadata = pipeline_run.get('run_metadata')
    if not isinstance(run_metadata, Mapping):
        return False
    continuation_stages = _resolved_terminal_followup_continuation_stages(
        run_metadata.get('resolved_stages')
    )
    if continuation_stages is None:
        return False
    legacy_continuation = (
        continuation_stages
        == LEGACY_TERMINAL_FOLLOWUP_CONTINUATION_STAGES
    )
    try:
        continuation_count = int(
            run_metadata.get('terminal_continuation_count') or 0
        )
    except (TypeError, ValueError):
        return False

    def evidence_int(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    terminal_evidence: Mapping[str, Any] | None = None
    evidence_suffix: Mapping[str, Any] | None = None
    archived_continuation_stages = continuation_stages
    archived_legacy_continuation = legacy_continuation
    if (
        pipeline_status in {'blocked', 'partial'}
        and continuation_count == 0
    ):
        pass
    elif (
        pipeline_status in {'running', 'interrupted', 'failed'}
        and continuation_count == 1
    ):
        terminal_evidence = (
            run_metadata.get('terminal_continuation_evidence')
            if isinstance(
                run_metadata.get('terminal_continuation_evidence'),
                Mapping,
            )
            else None
        )
        evidence_suffix = (
            terminal_evidence.get('suffix_stages')
            if isinstance(terminal_evidence, Mapping)
            and isinstance(terminal_evidence.get('suffix_stages'), Mapping)
            else None
        )
        if isinstance(evidence_suffix, Mapping):
            evidence_stage_ids = set(evidence_suffix)
            if evidence_stage_ids == set(
                LEGACY_TERMINAL_FOLLOWUP_CONTINUATION_STAGES
            ):
                archived_continuation_stages = (
                    LEGACY_TERMINAL_FOLLOWUP_CONTINUATION_STAGES
                )
            elif evidence_stage_ids == set(
                TERMINAL_FOLLOWUP_CONTINUATION_STAGES
            ):
                archived_continuation_stages = (
                    TERMINAL_FOLLOWUP_CONTINUATION_STAGES
                )
            else:
                return False
        if (
            legacy_continuation
            and archived_continuation_stages
            != LEGACY_TERMINAL_FOLLOWUP_CONTINUATION_STAGES
        ):
            return False
        archived_legacy_continuation = (
            archived_continuation_stages
            == LEGACY_TERMINAL_FOLLOWUP_CONTINUATION_STAGES
        )
        parent_evidence = (
            terminal_evidence.get('parent')
            if isinstance(terminal_evidence, Mapping)
            and isinstance(terminal_evidence.get('parent'), Mapping)
            else {}
        )
        lineage_evidence = (
            terminal_evidence.get('lineage')
            if isinstance(terminal_evidence, Mapping)
            and isinstance(terminal_evidence.get('lineage'), Mapping)
            else {}
        )
        summary_proof = (
            terminal_evidence.get('summary_proof')
            if isinstance(terminal_evidence, Mapping)
            and isinstance(terminal_evidence.get('summary_proof'), Mapping)
            else {}
        )
        ordered_run_ids = lineage_evidence.get(
            'ordered_pipeline_run_ids'
        )
        summary_scope_mode = str(
            summary_proof.get('feedback_scope_mode') or ''
        ).strip()
        raw_summary_symbols = summary_proof.get('requested_symbols')
        summary_symbols = (
            _dedupe_symbol_codes(raw_summary_symbols)
            if isinstance(raw_summary_symbols, list)
            else []
        )
        persisted_scope_proof_present = (
            'completed_scope_proof' in summary_proof
        )
        if persisted_scope_proof_present:
            summary_scope_matches_execution = (
                completed_summary_scope_matches_parent(
                    feedback_scope_mode=summary_scope_mode,
                    completed_symbols=raw_summary_symbols,
                    completed_scope_proof=summary_proof.get(
                        'completed_scope_proof'
                    ),
                    parent_symbols=run_metadata.get('symbols'),
                )
            )
        else:
            raw_parent_symbols = run_metadata.get('symbols')
            normalized_parent_symbols = (
                _dedupe_symbol_codes(raw_parent_symbols)
                if isinstance(raw_parent_symbols, list)
                else []
            )
            # Legacy version-1 claims did not persist the exact completed
            # scope proof. This only admits them to the locked repair path;
            # that path reconstructs and validates the proof by source run.
            summary_scope_matches_execution = bool(
                summary_symbols
                and raw_summary_symbols == summary_symbols
                and normalized_parent_symbols
                and set(summary_symbols).issubset(
                    normalized_parent_symbols
                )
            )
        if not (
            run_metadata.get('terminal_continuation_version') == 1
            and str(
                run_metadata.get('terminal_continuation_reason') or ''
            ).strip()
            == 'summary_knowledge_cutoff_repair'
            and str(run_metadata.get('resume_from_stage') or '').strip()
            == continuation_stages[0]
            and _parse_activity_datetime(
                run_metadata.get('terminal_continuation_claimed_at')
            )
            is not None
            and str(parent_evidence.get('status') or '').strip().lower()
            == 'blocked'
            and str(parent_evidence.get('current_stage') or '').strip()
            == archived_continuation_stages[-1]
            and isinstance(evidence_suffix, Mapping)
            and set(evidence_suffix)
            == set(archived_continuation_stages)
            and evidence_int(
                lineage_evidence.get('reused_eod_collection_run_id')
            )
            == evidence_int(
                run_metadata.get('reused_eod_collection_run_id')
            )
            and evidence_int(lineage_evidence.get('expected_run_count'))
            == MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE + 1
            and evidence_int(lineage_evidence.get('observed_run_count'))
            == MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE + 1
            and evidence_int(lineage_evidence.get('target_ordinal'))
            == MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE + 1
            and evidence_int(lineage_evidence.get('latest_pipeline_run_id'))
            == evidence_int(pipeline_run.get('id'))
            and isinstance(ordered_run_ids, (list, tuple))
            and len(ordered_run_ids)
            == MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE + 1
            and evidence_int(ordered_run_ids[-1])
            == evidence_int(pipeline_run.get('id'))
            and summary_proof.get('ready') is True
            and str(summary_proof.get('source_run_id') or '').strip()
            and _parse_activity_datetime(
                summary_proof.get('knowledge_cutoff_at')
            )
            is not None
            and summary_scope_mode
            in {
                FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
            }
            and isinstance(raw_summary_symbols, list)
            and raw_summary_symbols == summary_symbols
            and evidence_int(
                summary_proof.get('requested_symbol_count')
            )
            == len(summary_symbols)
            and summary_scope_matches_execution
        ):
            return False
    else:
        return False
    reused_eod_collection_run_id = evidence_int(
        run_metadata.get('reused_eod_collection_run_id')
    )
    if (
        reused_eod_collection_run_id is None
        or reused_eod_collection_run_id <= 0
        or _terminal_continuation_execution_context_error(
            run_metadata,
            reused_eod_collection_run_id=(
                reused_eod_collection_run_id
            ),
        )
        is not None
    ):
        return False

    stages = pipeline_run.get('stages')
    if not isinstance(stages, Mapping):
        return False
    raw_resolved_stages = run_metadata.get('resolved_stages')
    if not isinstance(raw_resolved_stages, list):
        return False
    resolved_stages = [
        str(stage_id or '').strip()
        for stage_id in raw_resolved_stages
        if str(stage_id or '').strip()
    ]
    live_stage_ids = set(stages)
    expected_live_stage_ids = set(resolved_stages)
    if archived_legacy_continuation:
        # The active contract no longer advertises evaluation, but an older
        # claimed row is deliberately retained in storage for audit.  A fresh
        # reload therefore contains that one retired row even though it is not
        # executable or part of readiness.
        expected_live_stage_ids.add('signal_decider_evaluations')
    if (
        len(resolved_stages) != len(raw_resolved_stages)
        or len(resolved_stages) != len(set(resolved_stages))
        or tuple(resolved_stages[-len(continuation_stages) :])
        != continuation_stages
        or live_stage_ids != expected_live_stage_ids
    ):
        return False
    stage_statuses = run_metadata.get('stage_statuses')
    if not isinstance(stage_statuses, Mapping):
        return False
    suffix_start_index = len(resolved_stages) - len(
        continuation_stages
    )
    if any(
        str(stage_statuses.get(stage_id) or '').strip().lower()
        != str(stages[stage_id].get('status') or '').strip().lower()
        for stage_id in resolved_stages[:suffix_start_index]
        if isinstance(stages[stage_id], Mapping)
    ) or any(
        not isinstance(stages[stage_id], Mapping)
        for stage_id in resolved_stages[:suffix_start_index]
    ):
        return False

    if evidence_suffix is not None:
        def untouched_claim_stage(stage: object) -> bool:
            return bool(
                isinstance(stage, Mapping)
                and str(stage.get('status') or '').strip().lower()
                == 'pending'
                and str(stage.get('current_step') or '').strip()
                == 'terminal_continuation_pending'
                and evidence_int(stage.get('processed') or 0) == 0
                and evidence_int(stage.get('total') or 0) == 0
                and evidence_int(stage.get('succeeded') or 0) == 0
                and evidence_int(stage.get('failed') or 0) == 0
                and evidence_int(stage.get('remaining') or 0) == 0
                and stage.get('completed_at') is None
                and not str(stage.get('error_message') or '').strip()
                and isinstance(stage.get('source_run_ids'), Mapping)
                and not stage.get('source_run_ids')
                and isinstance(stage.get('details'), Mapping)
                and not stage.get('details')
            )

        satisfied_prefix: list[str] = []
        for stage_id in continuation_stages:
            live_stage = stages.get(stage_id)
            if not (
                isinstance(live_stage, Mapping)
                and daily_pipeline_outcome_is_satisfied(live_stage)
                and _parse_activity_datetime(
                    live_stage.get('completed_at')
                )
                is not None
                and not str(
                    live_stage.get('error_message') or ''
                ).strip()
            ):
                break
            satisfied_prefix.append(stage_id)

        if len(satisfied_prefix) == len(
            continuation_stages
        ):
            return False
        metadata_prefix_size = 0
        metadata_pending_seen = False
        for stage_id in continuation_stages:
            persisted_status = str(
                stage_statuses.get(stage_id) or ''
            ).strip().lower()
            if persisted_status == 'pending':
                metadata_pending_seen = True
                continue
            if metadata_pending_seen:
                return False
            live_stage = stages[stage_id]
            if not (
                isinstance(live_stage, Mapping)
                and persisted_status
                == str(live_stage.get('status') or '').strip().lower()
                and daily_pipeline_outcome_is_satisfied(live_stage)
            ):
                return False
            metadata_prefix_size += 1
        if metadata_prefix_size > len(satisfied_prefix):
            return False
        first_unfinished_index = len(satisfied_prefix)
        first_unfinished_stage = continuation_stages[
            first_unfinished_index
        ]
        first_unfinished = stages.get(first_unfinished_stage)
        if not isinstance(first_unfinished, Mapping):
            return False
        first_status = str(
            first_unfinished.get('status') or ''
        ).strip().lower()
        current_stage = str(
            pipeline_run.get('current_stage') or ''
        ).strip()
        if pipeline_status == 'failed':
            first_unfinished_is_valid = bool(
                first_status == 'failed'
                and terminal_continuation_feedback_lease_loss_run_id(
                    pipeline_run=pipeline_run,
                    stage=first_unfinished,
                )
            )
        elif first_status == 'pending':
            expected_current_stages = {first_unfinished_stage}
            if satisfied_prefix:
                expected_current_stages.add(satisfied_prefix[-1])
            first_unfinished_is_valid = bool(
                untouched_claim_stage(first_unfinished)
                and current_stage in expected_current_stages
            )
        elif first_status == 'running':
            first_unfinished_is_valid = bool(
                pipeline_status == 'running'
                and current_stage == first_unfinished_stage
                and first_unfinished.get('completed_at') is None
                and not str(
                    first_unfinished.get('error_message') or ''
                ).strip()
            )
        elif first_status == 'interrupted':
            interrupted_details = first_unfinished.get('details')
            first_unfinished_is_valid = bool(
                pipeline_status == 'interrupted'
                and current_stage == first_unfinished_stage
                and str(
                    first_unfinished.get('current_step') or ''
                ).strip()
                == 'core_restart_interrupted'
                and _parse_activity_datetime(
                    first_unfinished.get('completed_at')
                )
                is not None
                and str(
                    first_unfinished.get('error_message') or ''
                ).strip()
                and isinstance(interrupted_details, Mapping)
                and interrupted_details.get('resumable') is True
                and str(
                    interrupted_details.get('interrupted_reason') or ''
                ).strip()
            )
        else:
            first_unfinished_is_valid = False
        if not first_unfinished_is_valid or any(
            not untouched_claim_stage(stages.get(stage_id))
            for stage_id in continuation_stages[
                first_unfinished_index + 1 :
            ]
        ):
            return False
    elif any(
        str(stage_statuses.get(stage_id) or '').strip().lower()
        != str(stages[stage_id].get('status') or '').strip().lower()
        for stage_id in resolved_stages
        if isinstance(stages[stage_id], Mapping)
    ) or any(
        not isinstance(stages[stage_id], Mapping)
        for stage_id in resolved_stages
    ):
        return False

    terminal_stages = evidence_suffix or stages
    summaries = terminal_stages.get('agent_feedback_summaries')
    if evidence_suffix is not None:
        summary_source_run_ids = (
            summaries.get('source_run_ids')
            if isinstance(summaries, Mapping)
            and isinstance(summaries.get('source_run_ids'), Mapping)
            else {}
        )
        summary_source_ids = {
            str(summary_source_run_ids.get(key) or '').strip()
            for key in ('agent_feedback_run_id', 'run_id')
            if str(summary_source_run_ids.get(key) or '').strip()
        }
        summary_result_run_id = str(
            _stage_result_summary(summaries).get('run_id') or ''
        ).strip()
        if summary_result_run_id:
            summary_source_ids.add(summary_result_run_id)
        if summary_source_ids != {
            str(summary_proof.get('source_run_id') or '').strip()
        }:
            return False
    if (
        not isinstance(summaries, Mapping)
        or str(summaries.get('status') or '').strip().lower()
        != 'completed'
        or not daily_pipeline_outcome_is_satisfied(
            summaries if isinstance(summaries, Mapping) else {}
        )
    ):
        return False
    if not _is_retryable_missing_upstream_decider_failure(
        terminal_stages.get('signal_decider')
    ):
        return False

    plan = terminal_stages.get('next_open_execution_plan')
    if not isinstance(plan, Mapping) or str(
        plan.get('status') or ''
    ).strip().lower() != 'blocked':
        return False
    plan_summary = _stage_result_summary(plan)
    if (
        str(plan_summary.get('reason') or '').strip()
        != 'signal_decider_stage_incomplete'
        or str(plan_summary.get('status') or '')
        .strip()
        .lower()
        != 'blocked'
        or plan_summary.get('triggered') is not False
        or str(
            plan_summary.get('upstream_status') or ''
        ).strip().lower()
        != 'failed'
    ):
        return False

    if archived_legacy_continuation:
        evaluations = terminal_stages.get('signal_decider_evaluations')
        if not isinstance(evaluations, Mapping) or str(
            evaluations.get('status') or ''
        ).strip().lower() != 'blocked':
            return False
        evaluation_summary = _stage_result_summary(evaluations)
        if (
            str(evaluation_summary.get('reason') or '').strip()
            != 'declared_dependencies_incomplete'
            or str(evaluation_summary.get('status') or '')
            .strip()
            .lower()
            != 'blocked'
            or evaluation_summary.get('triggered') is not False
            or evaluation_summary.get('incomplete_dependencies')
            != ['signal_decider']
        ):
            return False

    allowed_incomplete = set(continuation_stages)
    for stage_id in resolved_stages:
        if stage_id in allowed_incomplete:
            continue
        stage = stages.get(stage_id)
        if not isinstance(stage, Mapping):
            return False
        if not daily_pipeline_prefix_outcome_is_satisfied(
            {**dict(stage), 'stage': stage_id}
        ):
            return False
    return True


def _point_in_time_source_markets(
    metadata: Mapping[str, Any],
    *,
    fallback: Sequence[str],
) -> tuple[str, ...]:
    raw_markets = metadata.get('markets')
    universe_snapshot = (
        metadata.get('universe_snapshot')
        if isinstance(metadata.get('universe_snapshot'), Mapping)
        else {}
    )
    if not isinstance(raw_markets, (list, tuple)):
        raw_markets = universe_snapshot.get('markets')
    if not isinstance(raw_markets, (list, tuple)):
        membership = universe_snapshot.get('market_membership')
        membership_markets = (
            membership.get('markets')
            if isinstance(membership, Mapping)
            and isinstance(membership.get('markets'), Mapping)
            else {}
        )
        raw_markets = list(membership_markets)
    normalized = tuple(
        dict.fromkeys(
            str(item).strip().upper()
            for item in raw_markets or []
            if str(item).strip()
        )
    )
    return normalized or tuple(fallback)


def _validated_point_in_time_membership_symbols(
    metadata: Mapping[str, Any],
    *,
    markets: Sequence[str],
    expected_market_date: str,
) -> set[str] | None:
    source_markets = _point_in_time_source_markets(
        metadata,
        fallback=markets,
    )
    universe_snapshot = (
        metadata.get('universe_snapshot')
        if isinstance(metadata.get('universe_snapshot'), Mapping)
        else {}
    )
    validated = _validate_point_in_time_universe_snapshot(
        universe_snapshot,
        markets=source_markets,
        expected_market_date=expected_market_date,
    )
    if validated is None:
        return None
    membership_symbols = {
        symbol
        for market_symbols in validated.values()
        for symbol in market_symbols
    }
    admitted_scope = _dedupe_symbol_codes(
        [
            *(metadata.get('requested_symbols') or []),
            *(metadata.get('failed_symbols') or []),
            *(metadata.get('unattempted_symbols') or []),
            *(metadata.get('downstream_retry_symbols') or []),
            *(metadata.get('inherited_downstream_retry_symbols') or []),
        ]
    )
    if admitted_scope and not set(admitted_scope).issubset(
        membership_symbols
    ):
        return None
    return membership_symbols


def _validated_frozen_daily_membership_evidence(
    metadata: Mapping[str, Any],
    *,
    markets: Sequence[str],
    expected_market_date: str,
) -> tuple[set[str], dict[str, Any]] | None:
    """Promote an immutable exact-date daily snapshot into recovery evidence."""

    universe_snapshot = (
        metadata.get('universe_snapshot')
        if isinstance(metadata.get('universe_snapshot'), Mapping)
        else None
    )
    if (
        universe_snapshot is None
        or universe_snapshot.get('complete') is not True
    ):
        return None
    promoted_snapshot = {
        **dict(universe_snapshot),
        'point_in_time': True,
        'current_universe_mutated': False,
        'promoted_from_frozen_daily_snapshot': True,
    }
    membership_symbols = _validated_point_in_time_membership_symbols(
        {
            **dict(metadata),
            'universe_snapshot': promoted_snapshot,
        },
        markets=markets,
        expected_market_date=expected_market_date,
    )
    if membership_symbols is None:
        return None
    return membership_symbols, promoted_snapshot


def _validate_point_in_time_universe_snapshot(
    universe_snapshot: Mapping[str, Any],
    *,
    markets: Sequence[str],
    expected_market_date: str,
) -> dict[str, list[str]] | None:
    try:
        expected_date = _normalize_yyyymmdd(expected_market_date)
        snapshot_date = _normalize_yyyymmdd(
            str(universe_snapshot.get('latest_trade_date') or '')
        )
    except ValueError:
        return None
    if (
        snapshot_date != expected_date
        or universe_snapshot.get('point_in_time') is not True
        or str(universe_snapshot.get('source') or '').strip().lower()
        != 'pykrx'
        or str(
            universe_snapshot.get('membership_basis') or ''
        ).strip()
        != 'end_date_snapshot'
    ):
        return None
    membership = universe_snapshot.get('market_membership')
    raw_membership_markets = (
        membership.get('markets')
        if isinstance(membership, Mapping)
        and isinstance(membership.get('markets'), Mapping)
        else {}
    )
    membership_symbols = _dedupe_symbol_codes(
        [
            symbol
            for entry in raw_membership_markets.values()
            if isinstance(entry, Mapping)
            for symbol in (entry.get('symbols') or [])
        ]
    )
    return validate_market_membership_snapshot(
        membership,
        requested_symbols=membership_symbols,
        markets=markets,
    )


def _build_point_in_time_universe_snapshot(
    provider_snapshot: Mapping[str, Any],
    *,
    markets: Sequence[str],
    expected_market_date: str,
) -> dict[str, Any] | None:
    try:
        snapshot_date = _normalize_yyyymmdd(
            str(provider_snapshot.get('latest_trade_date') or '')
        )
        expected_date = _normalize_yyyymmdd(expected_market_date)
    except ValueError:
        return None
    if (
        not provider_snapshot.get('complete', True)
        or snapshot_date != expected_date
    ):
        return None
    symbols = list(provider_snapshot.get('symbols') or [])
    try:
        membership = build_market_membership_snapshot(
            symbols=symbols,
            markets=markets,
        )
    except ValueError:
        return None
    membership['symbol_metadata'] = [
        {
            'symbol': item.symbol,
            'market': item.market,
            'name_ko': item.name_ko,
            'standard_code': item.standard_code,
        }
        for item in symbols
    ]
    return {
        'source': 'pykrx',
        'markets': list(markets),
        'symbol_count': len(symbols),
        'persisted_count': 0,
        'latest_trade_date': _normalize_iso_date(expected_date),
        'universe_source': provider_snapshot.get(
            'universe_source', 'pykrx_market_ohlcv'
        ),
        'membership_basis': 'end_date_snapshot',
        'complete': True,
        'deactivate_missing': False,
        'point_in_time': True,
        'current_universe_mutated': False,
        'market_membership': membership,
    }


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_activity_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_aware_activity_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _validated_required_provider_admission(
    stage: object,
    *,
    expected_stage_id: str,
) -> dict[str, Any] | None:
    """Validate a zero-attempt required-provider credential block."""

    if not isinstance(stage, Mapping):
        return None
    result_summary = _stage_result_summary(stage)
    provider_admission = result_summary.get('provider_admission')
    if not isinstance(provider_admission, Mapping):
        return None
    missing_credentials = provider_admission.get('missing_credentials')
    normalized_missing_credentials = (
        [
            str(value or '').strip().upper()
            for value in missing_credentials
        ]
        if isinstance(missing_credentials, list)
        else []
    )
    provider = str(provider_admission.get('provider') or '').strip().lower()
    allowed_credentials = REQUIRED_PROVIDER_CREDENTIAL_SETTINGS.get(
        provider
    )
    allowed_stage_ids = REQUIRED_PROVIDER_STAGE_IDS.get(provider)
    required_stage_ids = provider_admission.get('required_stage_ids')
    normalized_required_stage_ids = (
        [str(value or '').strip() for value in required_stage_ids]
        if isinstance(required_stage_ids, list)
        else []
    )
    classification = 'blocked_missing_credential'
    if not (
        str(stage.get('status') or '').strip().lower() == 'blocked'
        and str(result_summary.get('status') or '').strip().lower()
        == 'blocked'
        and str(result_summary.get('classification') or '').strip().lower()
        == classification
        and result_summary.get('triggered') is False
        and str(result_summary.get('stage') or '').strip()
        == expected_stage_id
        and str(result_summary.get('provider') or '').strip().lower()
        == provider
        and result_summary.get('attempt_started') is False
        and result_summary.get('consumes_retry_budget') is False
        and provider_admission.get('schema_version')
        == 'required_provider_admission_v1'
        and allowed_credentials is not None
        and allowed_stage_ids is not None
        and provider_admission.get('required') is True
        and provider_admission.get('ready') is False
        and str(provider_admission.get('status') or '').strip().lower()
        == classification
        and str(
            provider_admission.get('classification') or ''
        ).strip().lower()
        == classification
        and provider_admission.get('attempt_started') is False
        and provider_admission.get('consumes_retry_budget') is False
        and provider_admission.get('recoverable') is True
        and normalized_required_stage_ids
        and all(normalized_required_stage_ids)
        and required_stage_ids == normalized_required_stage_ids
        and len(normalized_required_stage_ids)
        == len(set(normalized_required_stage_ids))
        and set(normalized_required_stage_ids).issubset(allowed_stage_ids)
        and expected_stage_id in normalized_required_stage_ids
        and provider_admission.get('recovery')
        == {
            'action': 'configure_credentials',
            'recheck_required': True,
        }
        and normalized_missing_credentials
        and all(normalized_missing_credentials)
        and missing_credentials == normalized_missing_credentials
        and len(normalized_missing_credentials)
        == len(set(normalized_missing_credentials))
        and set(normalized_missing_credentials).issubset(
            allowed_credentials
        )
    ):
        return None
    return {
        **dict(provider_admission),
        'provider': provider,
        'classification': classification,
        'missing_credentials': normalized_missing_credentials,
    }


def _required_provider_credentials_are_ready(
    provider_admission: Mapping[str, Any],
) -> bool:
    missing_credentials = provider_admission.get('missing_credentials')
    if not isinstance(missing_credentials, list) or not missing_credentials:
        return False
    provider = str(provider_admission.get('provider') or '').strip().lower()
    credential_settings = REQUIRED_PROVIDER_CREDENTIAL_SETTINGS.get(
        provider
    )
    if credential_settings is None:
        return False
    return all(
        bool(
            str(
                getattr(
                    settings,
                    credential_settings.get(
                        str(env_name or '').strip().upper(),
                        '',
                    ),
                    '',
                )
                or ''
            ).strip()
        )
        for env_name in missing_credentials
    )


def _validated_followup_admission_partition(
    metadata: Mapping[str, Any],
) -> dict[str, Any] | None:
    raw_outcome = metadata.get('followup_admission_scope_outcome')
    if not isinstance(raw_outcome, Mapping):
        return None
    selected = raw_outcome.get('selected_stock_codes')
    retry_debt = raw_outcome.get('retry_debt_stock_codes')
    if not isinstance(selected, list) or not isinstance(retry_debt, list):
        return None
    partition = _validated_exact_scope_outcome(
        result=raw_outcome,
        invocation_symbols=selected,
        status='partial' if retry_debt else 'completed',
    )
    if (
        partition is None
        or partition['manifest_version']
        != 'auto_eod_followup_admission_scope_outcomes_v1'
    ):
        return None
    return partition


def _validated_followup_admitted_symbols(
    metadata: Mapping[str, Any],
) -> list[str] | None:
    partition = _validated_followup_admission_partition(metadata)
    if partition is None:
        return None
    successful_symbols = list(partition['successful_stock_codes'])
    return successful_symbols or None


def _build_recovery_price_scope_outcome(
    *,
    latest_eod_run: Mapping[str, Any],
    market_date: str,
    membership_symbols: Sequence[str],
    allow_terminal_failures: bool = False,
) -> dict[str, Any] | None:
    """Prove the exact persisted price partition for a frozen EOD run."""

    normalized_membership = sorted(
        _dedupe_symbol_codes(membership_symbols)
    )
    metadata = (
        latest_eod_run.get('run_metadata')
        if isinstance(latest_eod_run.get('run_metadata'), Mapping)
        else {}
    )
    raw_requested_symbols = metadata.get('requested_symbols') or []
    raw_failed_symbols = metadata.get('failed_symbols') or []
    raw_unattempted_symbols = metadata.get('unattempted_symbols') or []
    if not all(
        isinstance(raw_symbols, list)
        for raw_symbols in (
            raw_requested_symbols,
            raw_failed_symbols,
            raw_unattempted_symbols,
        )
    ):
        return None
    requested_symbols = sorted(
        _dedupe_symbol_codes(raw_requested_symbols)
    )
    failed_symbols = sorted(_dedupe_symbol_codes(raw_failed_symbols))
    unattempted_symbols = sorted(
        _dedupe_symbol_codes(raw_unattempted_symbols)
    )
    try:
        eod_run_id = int(latest_eod_run['id'])
        symbols_total = int(latest_eod_run.get('symbols_total') or 0)
        symbols_succeeded = int(
            latest_eod_run.get('symbols_succeeded') or 0
        )
        symbols_failed = int(
            latest_eod_run.get('symbols_failed') or 0
        )
    except (KeyError, TypeError, ValueError):
        return None
    if (
        not normalized_membership
        or requested_symbols != normalized_membership
        or len(requested_symbols) != len(raw_requested_symbols)
        or len(failed_symbols) != len(raw_failed_symbols)
        or len(unattempted_symbols) != len(raw_unattempted_symbols)
        or symbols_total != len(normalized_membership)
        or symbols_succeeded <= 0
        or symbols_succeeded + symbols_failed != symbols_total
        or symbols_failed != len(failed_symbols)
        or unattempted_symbols
        or not set(failed_symbols).issubset(
            set(normalized_membership)
        )
        or (failed_symbols and not allow_terminal_failures)
    ):
        return None
    expected_persisted_symbols = [
        symbol
        for symbol in normalized_membership
        if symbol not in set(failed_symbols)
    ]
    if len(expected_persisted_symbols) != symbols_succeeded:
        return None
    persisted_symbols = sorted(
        _dedupe_symbol_codes(
            load_eod_run_persisted_symbols(
                eod_run_id,
                trade_date=market_date,
            )
        )
    )
    if persisted_symbols != expected_persisted_symbols:
        return None

    finalization_stage_id = STAGE_ATOMIC_FINALIZATION_STAGE_IDS.get(
        'price_collection'
    )
    if not finalization_stage_id:
        return None
    membership_digest = market_membership_sha256(normalized_membership)
    successful_digest = market_membership_sha256(persisted_symbols)
    return {
        'schema_version': RECOVERY_PRICE_SCOPE_OUTCOME_SCHEMA_VERSION,
        'stage_id': 'price_collection',
        'eod_collection_run_id': eod_run_id,
        'market_date': market_date,
        'status': 'partial' if failed_symbols else 'completed',
        **build_exact_symbol_scope_outcome(
            manifest_version='price_contexts_scope_outcomes_v1',
            selected_symbols=normalized_membership,
            successful_symbols=persisted_symbols,
            retry_debt_symbols=failed_symbols,
        ),
        'scope_finalization': {
            'stage': finalization_stage_id,
            'market_date': market_date,
            'exact_symbol_count': len(normalized_membership),
            'exact_membership_sha256': membership_digest,
            'successful_symbol_count': len(persisted_symbols),
            'successful_membership_sha256': successful_digest,
            'deleted_stale_row_count': 0,
            'ready': True,
        },
    }


def _followup_exact_evidence_present(stage: object) -> bool:
    """Return whether a stage row carries raw or compact exact-scope proof."""

    if not isinstance(stage, Mapping):
        return False
    result = stage.get('result')
    result_summary = _stage_result_summary(stage)
    details = stage.get('details')
    candidates = (stage, result, result_summary)
    if any(
        isinstance(candidate, Mapping)
        and candidate.get('scope_outcome_manifest_version') is not None
        for candidate in candidates
    ):
        return True
    return bool(
        (
            isinstance(details, Mapping)
            and isinstance(
                details.get('exact_scope_outcome_proof'), Mapping
            )
        )
        or isinstance(stage.get('exact_scope_outcome_proof'), Mapping)
        or isinstance(
            result_summary.get('exact_scope_outcome_proof'), Mapping
        )
    )


def _followup_quality_only_partial_is_satisfied(
    *,
    stage_id: str,
    stage: Mapping[str, Any],
    result_summary: Mapping[str, Any],
    selected_symbols: Sequence[str],
    successful_symbols: Sequence[str],
    retry_debt_symbols: Sequence[str],
    terminal_not_applicable_symbols: Sequence[str],
) -> bool:
    """Accept only an exhaustive, explicitly non-blocking quality warning."""

    def exact_nonnegative_int(value: object) -> int | None:
        if type(value) is not int:
            return None
        return value if value >= 0 else None

    selected = _dedupe_symbol_codes(selected_symbols)
    successful = _dedupe_symbol_codes(successful_symbols)
    retry_debt = _dedupe_symbol_codes(retry_debt_symbols)
    terminal = _dedupe_symbol_codes(
        terminal_not_applicable_symbols
    )
    if stage_id == 'signal_features':
        if str(stage.get('status') or '').strip().lower() != 'partial':
            return False
        return is_nonblocking_quality_only_signal_features_partial(
            stage_result=result_summary,
            selected_symbols=selected,
            successful_symbols=successful,
            retry_debt_symbols=retry_debt,
            terminal_not_applicable_symbols=terminal,
        )
    if not (
        selected
        and selected == list(selected_symbols)
        and successful == list(successful_symbols)
        and not retry_debt
        and not terminal
        and set(successful) == set(selected)
        and str(stage.get('status') or '').strip().lower()
        == 'partial'
        and str(result_summary.get('status') or '').strip().lower()
        == 'partial'
        and str(result_summary.get('gate_status') or '').strip().lower()
        == 'ready'
        and str(
            result_summary.get('blocking_status') or ''
        ).strip().lower()
        == 'non_blocking'
        and str(
            result_summary.get('ranking_dependency_status') or ''
        ).strip().lower()
        in {'complete', 'completed', 'ready'}
        and exact_nonnegative_int(
            result_summary.get('results_persisted')
        )
        == len(selected)
        and exact_nonnegative_int(
            result_summary.get('results_failed')
        )
        == 0
        and daily_pipeline_outcome_is_satisfied(
            {**dict(stage), 'stage': stage_id}
        )
    ):
        return False

    reason = str(result_summary.get('reason') or '').strip()
    quality_warning = str(
        result_summary.get('quality_warning') or ''
    ).strip()
    if stage_id == 'piotroski_filter':
        return is_nonblocking_quality_only_piotroski_partial(
            stage_result=result_summary,
            selected_symbols=selected,
            successful_symbols=successful,
            retry_debt_symbols=retry_debt,
            terminal_not_applicable_symbols=terminal,
        )
    if stage_id in FACTOR_FOLLOWUP_STAGE_ORDER:
        factor_summary = result_summary.get('summary')
        if not isinstance(factor_summary, Mapping):
            factor_summary = {}
        ready_count = exact_nonnegative_int(
            result_summary.get('persisted_ready_count')
            if 'persisted_ready_count' in result_summary
            else factor_summary.get('persisted_ready_count')
        )
        unready_count = exact_nonnegative_int(
            result_summary.get('persisted_unready_count')
            if 'persisted_unready_count' in result_summary
            else factor_summary.get('persisted_unready_count')
        )
        remaining_retryable = exact_nonnegative_int(
            result_summary.get('remaining_retryable_events')
        )
        return bool(
            reason == 'partial_usable_coverage'
            and quality_warning
            and str(
                result_summary.get('quality_status') or ''
            ).strip().lower()
            == 'degraded'
            and remaining_retryable == 0
            and ready_count is not None
            and unready_count is not None
            and ready_count > 0
            and ready_count + unready_count == len(selected)
        )
    return False


def _followup_exact_result_authority(
    *,
    stage_id: str,
    stage: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Resolve raw and persisted compact stage proofs to one exact result.

    Persisted pipeline rows compact list-heavy result summaries once a stage
    exceeds the summary cutoff.  The separately persisted v2 proof remains
    authoritative, but it must be rebound to the retained atomic finalization
    before it can advance durable recovery debt.
    """

    def complete_raw_result(candidate: object) -> bool:
        return bool(
            isinstance(candidate, Mapping)
            and candidate.get('scope_outcome_manifest_version') is not None
            and all(
                isinstance(candidate.get(key), list)
                for key in (
                    'selected_stock_codes',
                    'successful_stock_codes',
                    'retry_debt_stock_codes',
                    'scope_outcome_manifest',
                )
            )
        )

    result = stage.get('result')
    result_summary = _stage_result_summary(stage)
    # A live stage outcome still carries its full raw partition and is the
    # immediate authority. Persisted rows lose the large lists, so a retained
    # compact proof must be validated before considering their summary.
    for candidate in (result, stage):
        if complete_raw_result(candidate):
            return dict(candidate)
    details = stage.get('details')
    proof_candidates = (
        (
            details.get('exact_scope_outcome_proof')
            if isinstance(details, Mapping)
            else None
        ),
        stage.get('exact_scope_outcome_proof'),
        result_summary.get('exact_scope_outcome_proof'),
    )
    proof = next(
        (
            candidate
            for candidate in proof_candidates
            if isinstance(candidate, Mapping)
        ),
        None,
    )
    if proof is None:
        return (
            dict(result_summary)
            if complete_raw_result(result_summary)
            else None
        )

    selected_raw = proof.get('selected_stock_codes')
    retry_raw = proof.get('retry_debt_stock_codes')
    terminal_raw = proof.get('terminal_not_applicable_stock_codes')
    if not all(
        isinstance(value, list)
        for value in (selected_raw, retry_raw, terminal_raw)
    ):
        return None
    selected = _dedupe_symbol_codes(selected_raw)
    retry_debt = _dedupe_symbol_codes(retry_raw)
    terminal = _dedupe_symbol_codes(terminal_raw)
    if (
        selected != selected_raw
        or retry_debt != retry_raw
        or terminal != terminal_raw
        or selected != sorted(selected)
        or not selected
    ):
        return None
    selected_set = set(selected)
    retry_set = set(retry_debt)
    terminal_set = set(terminal)
    if retry_set & terminal_set or not (
        retry_set | terminal_set
    ).issubset(selected_set):
        return None
    successful = [
        symbol
        for symbol in selected
        if symbol not in retry_set | terminal_set
    ]
    market_date = str(proof.get('market_date') or '').strip()
    manifest_version = str(proof.get('manifest_version') or '').strip()
    allowed_manifest_versions = (
        STAGE_EXACT_SCOPE_OUTCOME_MANIFEST_VERSIONS.get(
            stage_id,
            frozenset(),
        )
    )
    if (
        proof.get('schema_version') != 'exact_scope_outcome_proof_v2'
        or str(proof.get('stage_id') or '').strip() != stage_id
        or not market_date
        or manifest_version not in allowed_manifest_versions
        or proof.get('selected_symbol_count') != len(selected)
        or proof.get('selected_symbol_sha256')
        != market_membership_sha256(selected)
        or proof.get('successful_symbol_count') != len(successful)
        or proof.get('successful_symbol_sha256')
        != market_membership_sha256(successful)
        or not exact_scope_partition_identity_proof_matches(
            proof=proof,
            retry_debt_symbols=retry_debt,
            terminal_not_applicable_symbols=terminal,
        )
    ):
        return None

    failed_all = bool(
        retry_set == selected_set
        and not successful
        and not terminal
        and str(proof.get('outcome_status') or '').strip().lower()
        == 'failed'
    )
    partition_status = (
        'failed'
        if failed_all
        else 'partial'
        if retry_debt
        else 'completed'
    )
    persisted_status = str(
        stage.get('status')
        or result_summary.get('status')
        or partition_status
    ).strip().lower()
    proof_outcome_status = str(
        proof.get('outcome_status') or ''
    ).strip().lower()
    quality_only_partial = bool(
        partition_status == 'completed'
        and persisted_status == 'partial'
        and proof_outcome_status == 'partial'
        and _followup_quality_only_partial_is_satisfied(
            stage_id=stage_id,
            stage=stage,
            result_summary=result_summary,
            selected_symbols=selected,
            successful_symbols=successful,
            retry_debt_symbols=retry_debt,
            terminal_not_applicable_symbols=terminal,
        )
    )
    proof_status = (
        'partial' if quality_only_partial else partition_status
    )
    if (
        proof_outcome_status != proof_status
        or persisted_status != proof_status
    ):
        return None
    reconstructed = {
        'status': proof_status,
        'market_date': market_date,
        **build_exact_symbol_scope_outcome(
            manifest_version=manifest_version,
            selected_symbols=selected,
            successful_symbols=successful,
            retry_debt_symbols=retry_debt,
            terminal_not_applicable_symbols=terminal,
        ),
    }
    for scalar_key in (
        'blocking_status',
        'full_signal_coverage_status',
        'full_signal_incomplete_count',
        'full_signal_unusable_count',
        'full_signal_usable_count',
        'gate_status',
        'persisted_ready_count',
        'persisted_unready_count',
        'quality_status',
        'quality_warning',
        'ranking_dependency_status',
        'reason',
        'remaining_retryable_events',
        'results_failed',
        'results_persisted',
    ):
        if scalar_key in result_summary:
            reconstructed[scalar_key] = result_summary[scalar_key]
    finalization_stage_id = STAGE_ATOMIC_FINALIZATION_STAGE_IDS.get(
        stage_id
    )
    if finalization_stage_id is not None:
        finalization = result_summary.get('scope_finalization')
        if not isinstance(finalization, Mapping):
            return None
        proof_finalization_fields = (
            ('finalization_stage_id', 'stage'),
            ('finalization_market_date', 'market_date'),
            ('finalization_exact_symbol_count', 'exact_symbol_count'),
            (
                'finalization_exact_membership_sha256',
                'exact_membership_sha256',
            ),
            (
                'finalization_successful_symbol_count',
                'successful_symbol_count',
            ),
            (
                'finalization_successful_membership_sha256',
                'successful_membership_sha256',
            ),
        )
        if any(
            proof.get(proof_key) != finalization.get(finalization_key)
            for proof_key, finalization_key in proof_finalization_fields
        ):
            return None
        reconstructed['scope_finalization'] = dict(finalization)
    return reconstructed


def _recovered_followup_admission_scope_outcome(
    *,
    latest_eod_run: Mapping[str, Any],
    market_date: str,
    pipeline_run: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Rebuild stale admission authority from one exact linked PITR run."""

    eod_metadata = (
        latest_eod_run.get('run_metadata')
        if isinstance(latest_eod_run.get('run_metadata'), Mapping)
        else {}
    )
    requested_symbols = sorted(
        _dedupe_symbol_codes(eod_metadata.get('requested_symbols') or [])
    )
    prior_admission = _validated_followup_admission_partition(
        eod_metadata
    )
    try:
        eod_run_id = latest_eod_run['id']
        pipeline_run_id = pipeline_run['id']
    except KeyError:
        return None
    if (
        type(eod_run_id) is not int
        or eod_run_id <= 0
        or type(pipeline_run_id) is not int
        or pipeline_run_id <= 0
        or not requested_symbols
        or prior_admission is None
        or prior_admission.get('selected_stock_codes')
        != requested_symbols
        or not prior_admission.get('retry_debt_stock_codes')
        or str(pipeline_run.get('market_date') or '').strip()
        != market_date
        or str(pipeline_run.get('trigger') or '').strip()
        != AUTO_FOLLOWUP_RECOVERY_TRIGGER
    ):
        return None
    run_metadata = pipeline_run.get('run_metadata')
    stage_rows = pipeline_run.get('stages')
    if not isinstance(run_metadata, Mapping) or not isinstance(
        stage_rows, Mapping
    ):
        return None
    resolved_stages = run_metadata.get('resolved_stages')
    normalized_resolved_stages = (
        [str(stage_id or '').strip() for stage_id in resolved_stages]
        if isinstance(resolved_stages, list)
        else []
    )
    normalized_execution_symbols = _dedupe_symbol_codes(
        run_metadata.get('symbols') or []
    )
    if (
        str(run_metadata.get('entrypoint') or '').strip()
        != 'daily_pipeline_stage_service.run'
        or type(run_metadata.get('reused_eod_collection_run_id'))
        is not int
        or run_metadata.get('reused_eod_collection_run_id') != eod_run_id
        or run_metadata.get('point_in_time_recovery') is not True
        or str(run_metadata.get('resume_from_stage') or '').strip()
        != RECOVERED_FOLLOWUP_ADMISSION_STAGE_IDS[0]
        or not isinstance(resolved_stages, list)
        or normalized_resolved_stages != resolved_stages
        or len(normalized_resolved_stages)
        != len(set(normalized_resolved_stages))
        or not all(
            stage_id in normalized_resolved_stages
            for stage_id in RECOVERED_FOLLOWUP_ADMISSION_STAGE_IDS
        )
        or not normalized_execution_symbols
        or normalized_execution_symbols
        != list(run_metadata.get('symbols') or [])
        or not set(normalized_execution_symbols).issubset(
            set(requested_symbols)
        )
        or type(run_metadata.get('exact_membership_symbol_count'))
        is not int
        or run_metadata.get('exact_membership_symbol_count')
        != len(requested_symbols)
        or run_metadata.get('exact_membership_sha256')
        != market_membership_sha256(requested_symbols)
        or _build_recovery_price_scope_outcome(
            latest_eod_run=latest_eod_run,
            market_date=market_date,
            membership_symbols=requested_symbols,
        )
        is None
    ):
        return None

    for stage_id in RECOVERED_FOLLOWUP_ADMISSION_STAGE_IDS:
        stage = stage_rows.get(stage_id)
        if not isinstance(stage, Mapping) or not daily_pipeline_outcome_is_satisfied(
            {**dict(stage), 'stage': stage_id}
        ):
            return None
        exact_result = _followup_exact_result_authority(
            stage_id=stage_id,
            stage=stage,
        )
        validated_partition = (
            validate_stage_bound_scope_outcome(
                result=exact_result,
                expected_stage_id=stage_id,
                invocation_symbols=requested_symbols,
                status='completed',
                expected_market_date=market_date,
                publication_symbols=requested_symbols,
            )
            if exact_result is not None
            else None
        )
        if (
            exact_result is None
            or validated_partition is None
            or str(exact_result.get('status') or '').strip().lower()
            != 'completed'
            or validated_partition.get('selected_stock_codes')
            != requested_symbols
            or validated_partition.get('successful_stock_codes')
            != requested_symbols
            or validated_partition.get('retry_debt_stock_codes') != []
            or validated_partition.get(
                'terminal_not_applicable_stock_codes'
            )
            != []
        ):
            return None

    return build_exact_symbol_scope_outcome(
        manifest_version=(
            'auto_eod_followup_admission_scope_outcomes_v1'
        ),
        selected_symbols=requested_symbols,
        successful_symbols=requested_symbols,
        retry_debt_symbols=[],
    )


def _validated_followup_exact_partition(
    *,
    stage_id: str,
    stage: object,
    expected_market_date: str,
    expected_publication_symbols: Sequence[str],
    allowed_partition_symbols: Sequence[str],
    allow_quality_only_partial: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    if stage_id not in FOLLOWUP_EXACT_STAGE_MANIFEST_VERSIONS:
        return None
    if not isinstance(stage, Mapping):
        return None
    exact_result = _followup_exact_result_authority(
        stage_id=stage_id,
        stage=stage,
    )
    if exact_result is None:
        return None
    selected_symbols = exact_result.get('selected_stock_codes')
    if not isinstance(selected_symbols, list):
        return None
    normalized_status = str(
        exact_result.get('status') or stage.get('status') or ''
    ).strip().lower()
    partition = validate_stage_bound_scope_outcome(
        result=exact_result,
        expected_stage_id=stage_id,
        invocation_symbols=selected_symbols,
        status=normalized_status,
        allow_failed_all=True,
        expected_market_date=expected_market_date,
        publication_symbols=expected_publication_symbols,
    )
    if (
        partition is None
        or partition['manifest_version']
        not in FOLLOWUP_EXACT_STAGE_MANIFEST_VERSIONS[stage_id]
        or not set(partition['selected_stock_codes']).issubset(
            set(_dedupe_symbol_codes(allowed_partition_symbols))
        )
    ):
        return None
    if stage_id != 'fundamentals_daily_refresh':
        gate_status = str(
            exact_result.get('gate_status') or ''
        ).strip().lower()
        quality_only_partial = bool(
            allow_quality_only_partial
            and normalized_status == 'partial'
            and _followup_quality_only_partial_is_satisfied(
                stage_id=stage_id,
                stage=stage,
                result_summary=_stage_result_summary(stage),
                selected_symbols=partition[
                    'selected_stock_codes'
                ],
                successful_symbols=partition[
                    'successful_stock_codes'
                ],
                retry_debt_symbols=partition[
                    'retry_debt_stock_codes'
                ],
                terminal_not_applicable_symbols=partition[
                    'terminal_not_applicable_stock_codes'
                ],
            )
        )
        if (
            normalized_status == 'partial'
            and not partition['retry_debt_stock_codes']
            and not quality_only_partial
        ):
            return None
        if gate_status == 'blocked':
            return None
        if (
            str(exact_result.get('reason') or '').startswith(
                'piotroski_evidence_'
            )
            and not partition['retry_debt_stock_codes']
            and not quality_only_partial
        ):
            return None
        ranking_dependency_status = str(
            exact_result.get('ranking_dependency_status') or ''
        ).strip().lower()
        if ranking_dependency_status and ranking_dependency_status not in {
            'complete',
            'completed',
            'ready',
        }:
            return None
    elif str(exact_result.get('market_date') or '').strip() != str(
        expected_market_date
    ).strip():
        return None
    proof = dict(exact_result)
    proof[FOLLOWUP_STAGE_PROOF_PUBLICATION_SCOPE_KEY] = (
        _dedupe_symbol_codes(expected_publication_symbols)
    )
    return partition, proof


def _normalize_stage_symbol_map(
    value: object,
    *,
    parent_symbols: Sequence[str],
) -> dict[str, list[str]] | None:
    if not isinstance(value, Mapping):
        return None
    parent_set = set(parent_symbols)
    normalized: dict[str, list[str]] = {}
    for raw_stage_id, raw_symbols in value.items():
        stage_id = str(raw_stage_id or '').strip()
        if (
            stage_id not in FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER
            or not isinstance(raw_symbols, list)
        ):
            return None
        symbols = _dedupe_symbol_codes(raw_symbols)
        if (
            symbols != raw_symbols
            or len(symbols) != len(raw_symbols)
            or not set(symbols).issubset(parent_set)
        ):
            return None
        if symbols:
            normalized[stage_id] = symbols
    return normalized


def _validated_followup_stage_debt_state(
    value: object,
    *,
    market_date: str,
    eod_collection_run_id: int,
    parent_symbols: Sequence[str],
    publication_symbols: Sequence[str],
    require_pending: bool = True,
) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    if not set(_dedupe_symbol_codes(parent_symbols)).issubset(
        set(_dedupe_symbol_codes(publication_symbols))
    ):
        return None
    if not (
        value.get('schema_version')
        == FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION
        and str(value.get('market_date') or '').strip() == market_date
        and value.get('eod_collection_run_id') == eod_collection_run_id
    ):
        return None
    pending = _normalize_stage_symbol_map(
        value.get('pending_by_stage'),
        parent_symbols=parent_symbols,
    )
    successful = _normalize_stage_symbol_map(
        value.get('successful_by_stage'),
        parent_symbols=parent_symbols,
    )
    terminal = _normalize_stage_symbol_map(
        value.get('terminal_by_stage'),
        parent_symbols=parent_symbols,
    )
    proofs = value.get('proofs_by_stage')
    recovery_progress_count = value.get('recovery_progress_count', 0)
    raw_recovery_attempts = value.get('recovery_attempts_by_stage', {})
    raw_applied_run_ids = value.get(
        'applied_recovery_pipeline_run_ids',
        [],
    )
    if (
        pending is None
        or successful is None
        or terminal is None
        or not isinstance(proofs, Mapping)
        or not isinstance(raw_recovery_attempts, Mapping)
        or not isinstance(raw_applied_run_ids, list)
        or isinstance(recovery_progress_count, bool)
        or not isinstance(recovery_progress_count, int)
        or recovery_progress_count < 0
    ):
        return None
    recovery_attempts: dict[str, int] = {}
    for raw_stage_id, raw_attempt_count in raw_recovery_attempts.items():
        stage_id = str(raw_stage_id or '').strip()
        if (
            stage_id not in FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER
            or isinstance(raw_attempt_count, bool)
            or not isinstance(raw_attempt_count, int)
            or raw_attempt_count < 0
        ):
            return None
        if raw_attempt_count:
            recovery_attempts[stage_id] = raw_attempt_count
    applied_run_ids: list[int] = []
    for raw_run_id in raw_applied_run_ids:
        if (
            isinstance(raw_run_id, bool)
            or not isinstance(raw_run_id, int)
            or raw_run_id <= 0
            or raw_run_id in applied_run_ids
        ):
            return None
        applied_run_ids.append(raw_run_id)
    if sum(recovery_attempts.values()) != len(applied_run_ids):
        return None
    if recovery_progress_count > len(applied_run_ids):
        return None
    normalized_proofs: dict[str, list[dict[str, Any]]] = {}
    proof_classifications: dict[str, dict[str, set[str]]] = {}
    recovery_proof_run_ids: set[int] = set()
    recovery_progress_run_ids: set[int] = set()
    recovery_proof_counts_by_stage: dict[str, int] = {}
    for raw_stage_id, raw_proofs in proofs.items():
        stage_id = str(raw_stage_id or '').strip()
        if (
            stage_id not in FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER
            or not isinstance(raw_proofs, list)
        ):
            return None
        stage_proofs: list[dict[str, Any]] = []
        replayed_successful: set[str] = set()
        replayed_pending: set[str] = set()
        replayed_terminal: set[str] = set()
        for raw_proof in raw_proofs:
            if not isinstance(raw_proof, Mapping):
                return None
            proof_publication_symbols = raw_proof.get(
                FOLLOWUP_STAGE_PROOF_PUBLICATION_SCOPE_KEY
            )
            if not isinstance(proof_publication_symbols, list):
                return None
            normalized_publication_symbols = _dedupe_symbol_codes(
                proof_publication_symbols
            )
            if (
                normalized_publication_symbols
                != proof_publication_symbols
                or not normalized_publication_symbols
                or not set(normalized_publication_symbols).issubset(
                    set(publication_symbols)
                )
            ):
                return None
            proof_stage = {
                'status': raw_proof.get('status'),
                'result': raw_proof,
                'result_summary': raw_proof,
            }
            validated_proof = _validated_followup_exact_partition(
                stage_id=stage_id,
                stage=proof_stage,
                expected_market_date=market_date,
                expected_publication_symbols=(
                    normalized_publication_symbols
                ),
                allowed_partition_symbols=parent_symbols,
                allow_quality_only_partial=True,
            )
            if validated_proof is None:
                return None
            partition, _proof = validated_proof
            recovery_lineage = raw_proof.get(
                FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY
            )
            if recovery_lineage is not None:
                if not isinstance(recovery_lineage, Mapping):
                    return None
                recovery_pipeline_run_id = recovery_lineage.get(
                    'pipeline_run_id'
                )
                recovery_eod_collection_run_id = recovery_lineage.get(
                    'eod_collection_run_id'
                )
                if (
                    set(recovery_lineage)
                    != {
                        'schema_version',
                        'pipeline_run_id',
                        'eod_collection_run_id',
                        'stage_id',
                    }
                    or recovery_lineage.get('schema_version')
                    != FOLLOWUP_STAGE_RECOVERY_LINEAGE_SCHEMA_VERSION
                    or str(recovery_lineage.get('stage_id') or '').strip()
                    != stage_id
                    or isinstance(recovery_pipeline_run_id, bool)
                    or not isinstance(recovery_pipeline_run_id, int)
                    or recovery_pipeline_run_id <= 0
                    or recovery_pipeline_run_id not in applied_run_ids
                    or recovery_pipeline_run_id in recovery_proof_run_ids
                    or isinstance(recovery_eod_collection_run_id, bool)
                    or not isinstance(recovery_eod_collection_run_id, int)
                    or recovery_eod_collection_run_id
                    != eod_collection_run_id
                ):
                    return None
                recovery_proof_run_ids.add(recovery_pipeline_run_id)
                recovery_proof_counts_by_stage[stage_id] = (
                    recovery_proof_counts_by_stage.get(stage_id, 0) + 1
                )
                if (
                    partition['successful_stock_codes']
                    or partition['terminal_not_applicable_stock_codes']
                ):
                    recovery_progress_run_ids.add(
                        recovery_pipeline_run_id
                    )
            selected_set = set(partition['selected_stock_codes'])
            if selected_set & (replayed_successful | replayed_terminal):
                return None
            replayed_pending.difference_update(selected_set)
            replayed_successful.update(
                partition['successful_stock_codes']
            )
            replayed_pending.update(partition['retry_debt_stock_codes'])
            replayed_terminal.update(
                partition['terminal_not_applicable_stock_codes']
            )
            stage_proofs.append(dict(raw_proof))
        if stage_proofs:
            normalized_proofs[stage_id] = stage_proofs
            proof_classifications[stage_id] = {
                'successful': replayed_successful,
                'pending': replayed_pending,
                'terminal': replayed_terminal,
            }
    if recovery_progress_count != len(recovery_progress_run_ids):
        return None
    if any(
        proof_count > recovery_attempts.get(stage_id, 0)
        for stage_id, proof_count in recovery_proof_counts_by_stage.items()
    ):
        return None
    parent_set = set(_dedupe_symbol_codes(parent_symbols))
    pending_sets = {
        stage_id: set(symbols) for stage_id, symbols in pending.items()
    }
    successful_sets = {
        stage_id: set(symbols)
        for stage_id, symbols in successful.items()
    }
    terminal_sets = {
        stage_id: set(symbols) for stage_id, symbols in terminal.items()
    }
    for stage_id in FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER:
        stage_pending = pending_sets.get(stage_id, set())
        stage_successful = successful_sets.get(stage_id, set())
        stage_terminal = terminal_sets.get(stage_id, set())
        if (
            stage_pending & stage_successful
            or stage_pending & stage_terminal
            or stage_successful & stage_terminal
        ):
            return None

    provider_admission = value.get('provider_admission')
    normalized_provider_admission: dict[str, Any] | None = None
    if provider_admission is not None:
        provider_stage = {
            'status': 'blocked',
            'details': {'result_summary': provider_admission},
        }
        validated_admission = _validated_required_provider_admission(
            provider_stage,
            expected_stage_id='fundamentals_daily_refresh',
        )
        if validated_admission is None:
            return None
        normalized_provider_admission = dict(provider_admission)

    root_stage_id = FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER[0]
    root_accounted = {
        *pending_sets.get(root_stage_id, set()),
        *successful_sets.get(root_stage_id, set()),
        *terminal_sets.get(root_stage_id, set()),
    }
    if root_accounted != parent_set:
        return None
    root_proof = proof_classifications.get(root_stage_id)
    if root_proof is not None:
        if (
            pending_sets.get(root_stage_id, set())
            != root_proof['pending']
            or successful_sets.get(root_stage_id, set())
            != root_proof['successful']
            or terminal_sets.get(root_stage_id, set())
            != root_proof['terminal']
        ):
            return None
    elif (
        normalized_provider_admission is None
        or pending_sets.get(root_stage_id, set()) != parent_set
        or successful_sets.get(root_stage_id, set())
        or terminal_sets.get(root_stage_id, set())
    ):
        return None
    for stage_id in FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER[1:]:
        stage_present = bool(
            stage_id in pending_sets
            or stage_id in successful_sets
            or stage_id in terminal_sets
            or stage_id in normalized_proofs
        )
        if not stage_present:
            continue
        dependencies = FOLLOWUP_EXACT_STAGE_DEPENDENCIES.get(stage_id, ())
        if not dependencies:
            return None
        eligible = set(successful_sets.get(dependencies[0], set()))
        for dependency in dependencies[1:]:
            eligible.intersection_update(
                successful_sets.get(dependency, set())
            )
        stage_pending = pending_sets.get(stage_id, set())
        stage_successful = successful_sets.get(stage_id, set())
        stage_terminal = terminal_sets.get(stage_id, set())
        inherited_terminal = set().union(
            *(terminal_sets.get(dependency, set()) for dependency in dependencies)
        )
        if _is_price_cohort_factor_stage(
            stage_id,
            stage_pending | stage_successful | stage_terminal,
            eligible | inherited_terminal,
        ):
            eligible = set(parent_set)
            inherited_terminal = set()
        proof_classification = proof_classifications.get(
            stage_id,
            {'successful': set(), 'pending': set(), 'terminal': set()},
        )
        proof_classified = set().union(
            proof_classification['successful'],
            proof_classification['pending'],
            proof_classification['terminal'],
        )
        expected_pending = proof_classification['pending'] | (
            eligible - proof_classified - inherited_terminal
        )
        expected_terminal = (
            proof_classification['terminal'] | inherited_terminal
        )
        if (
            not (stage_pending | stage_successful).issubset(eligible)
            or not eligible.issubset(
                stage_pending | stage_successful | stage_terminal
            )
            or not stage_terminal.issubset(
                eligible | inherited_terminal
            )
            or stage_successful != proof_classification['successful']
            or stage_pending != expected_pending
            or stage_terminal != expected_terminal
        ):
            return None
    if require_pending and not pending:
        return None
    return {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': market_date,
        'eod_collection_run_id': eod_collection_run_id,
        'pending_by_stage': pending,
        'successful_by_stage': successful,
        'terminal_by_stage': terminal,
        'proofs_by_stage': normalized_proofs,
        'recovery_progress_count': recovery_progress_count,
        'recovery_attempts_by_stage': recovery_attempts,
        'applied_recovery_pipeline_run_ids': applied_run_ids,
        **(
            {'provider_admission': normalized_provider_admission}
            if normalized_provider_admission is not None
            else {}
        ),
    }


def _canonicalize_followup_stage_debt_graph(
    *,
    pending: dict[str, list[str]],
    successful: dict[str, list[str]],
    terminal: dict[str, list[str]],
    admit_newly_eligible: bool,
) -> None:
    """Propagate dependency closure without inventing successful output."""

    changed = True
    while changed:
        changed = False
        for stage_id in FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER[1:]:
            dependencies = FOLLOWUP_EXACT_STAGE_DEPENDENCIES.get(stage_id)
            if not dependencies:
                continue
            eligible = set(successful.get(dependencies[0]) or [])
            for dependency in dependencies[1:]:
                eligible.intersection_update(
                    successful.get(dependency) or []
                )
            inherited_terminal = set().union(
                *(set(terminal.get(dependency) or []) for dependency in dependencies)
            )
            stage_present = bool(
                pending.get(stage_id)
                or successful.get(stage_id)
                or terminal.get(stage_id)
            )
            if not stage_present and not admit_newly_eligible:
                continue
            if _is_price_cohort_factor_stage(
                stage_id,
                set().union(
                    pending.get(stage_id) or [],
                    successful.get(stage_id) or [],
                    terminal.get(stage_id) or [],
                ),
                eligible | inherited_terminal,
            ):
                continue

            stage_pending = set(pending.get(stage_id) or [])
            stage_successful = set(successful.get(stage_id) or [])
            stage_terminal = set(terminal.get(stage_id) or [])
            new_terminal = inherited_terminal - stage_terminal
            if new_terminal:
                stage_pending.difference_update(new_terminal)
                stage_successful.difference_update(new_terminal)
                stage_terminal.update(new_terminal)
                changed = True

            if admit_newly_eligible:
                newly_eligible = eligible - (
                    stage_pending | stage_successful | stage_terminal
                )
                if newly_eligible:
                    stage_pending.update(newly_eligible)
                    changed = True

            pending[stage_id] = sorted(stage_pending)
            successful[stage_id] = sorted(stage_successful)
            terminal[stage_id] = sorted(stage_terminal)


def _record_followup_stage_debt_attempt(
    state: Mapping[str, Any],
    *,
    stage_id: str,
    invocation_symbols: Sequence[str],
    recovery_pipeline_run_id: int,
) -> dict[str, Any] | None:
    """Record one lineage-bound stage attempt without accepting its output."""

    pending = deepcopy(state.get('pending_by_stage') or {})
    expected_symbols = _dedupe_symbol_codes(pending.get(stage_id) or [])
    normalized_invocation = _dedupe_symbol_codes(invocation_symbols)
    applied_run_ids = list(
        state.get('applied_recovery_pipeline_run_ids') or []
    )
    recovery_attempts = deepcopy(
        state.get('recovery_attempts_by_stage') or {}
    )
    if (
        not expected_symbols
        or set(expected_symbols) != set(normalized_invocation)
        or isinstance(recovery_pipeline_run_id, bool)
        or not isinstance(recovery_pipeline_run_id, int)
        or recovery_pipeline_run_id <= 0
        or recovery_pipeline_run_id in applied_run_ids
    ):
        return None
    current_attempts = recovery_attempts.get(stage_id, 0)
    if (
        isinstance(current_attempts, bool)
        or not isinstance(current_attempts, int)
        or current_attempts < 0
    ):
        return None
    recovery_attempts[stage_id] = current_attempts + 1
    applied_run_ids.append(recovery_pipeline_run_id)
    return {
        **dict(state),
        'recovery_attempts_by_stage': recovery_attempts,
        'applied_recovery_pipeline_run_ids': applied_run_ids,
    }


def _followup_stage_debt_recovery_proof_run_ids(
    state: Mapping[str, Any],
    *,
    stage_id: str,
) -> set[int]:
    """Return lineage IDs backed by an accepted exact stage proof."""

    raw_proofs = (state.get('proofs_by_stage') or {}).get(stage_id) or []
    if not isinstance(raw_proofs, list):
        return set()
    proof_run_ids: set[int] = set()
    for raw_proof in raw_proofs:
        if not isinstance(raw_proof, Mapping):
            continue
        recovery_lineage = raw_proof.get(
            FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY
        )
        if not isinstance(recovery_lineage, Mapping):
            continue
        pipeline_run_id = recovery_lineage.get('pipeline_run_id')
        if (
            recovery_lineage.get('schema_version')
            == FOLLOWUP_STAGE_RECOVERY_LINEAGE_SCHEMA_VERSION
            and str(recovery_lineage.get('stage_id') or '').strip()
            == stage_id
            and isinstance(pipeline_run_id, int)
            and not isinstance(pipeline_run_id, bool)
            and pipeline_run_id > 0
        ):
            proof_run_ids.add(pipeline_run_id)
    return proof_run_ids


def _followup_recovery_stage_attempt_finished(
    stage: object,
    *,
    expected_stage_id: str,
) -> bool:
    """Require durable start and terminal evidence before rotating debt."""

    if not isinstance(stage, Mapping):
        return False
    if _validated_required_provider_admission(
        stage,
        expected_stage_id=expected_stage_id,
    ) is not None:
        return False
    result_summary = _stage_result_summary(stage)
    if result_summary.get('attempt_started') is False:
        return False
    status = str(
        stage.get('status') or result_summary.get('status') or ''
    ).strip().lower()
    return bool(
        stage.get('started_at')
        and stage.get('completed_at')
        and status
        in {
            'blocked',
            'completed',
            'completed_with_errors',
            'failed',
            'interrupted',
            'partial',
            'skipped',
        }
    )


def _advance_followup_stage_debt_state(
    state: Mapping[str, Any],
    *,
    stage_id: str,
    invocation_symbols: Sequence[str],
    stage_result: Mapping[str, Any],
    recovery_pipeline_run_id: int | None = None,
    recovery_attempt_already_recorded: bool = False,
) -> dict[str, Any] | None:
    """Apply one exhaustive stage proof without touching sibling debt."""

    pending = deepcopy(state.get('pending_by_stage') or {})
    expected_symbols = _dedupe_symbol_codes(pending.get(stage_id) or [])
    normalized_invocation = _dedupe_symbol_codes(invocation_symbols)
    if not expected_symbols or set(expected_symbols) != set(
        normalized_invocation
    ):
        return None
    validated = _validated_followup_exact_partition(
        stage_id=stage_id,
        stage=stage_result,
        expected_market_date=str(state.get('market_date') or ''),
        expected_publication_symbols=normalized_invocation,
        allowed_partition_symbols=normalized_invocation,
        allow_quality_only_partial=True,
    )
    if validated is None:
        return None
    partition, proof = validated
    if set(partition['selected_stock_codes']) != set(expected_symbols):
        return None

    successful = deepcopy(state.get('successful_by_stage') or {})
    terminal = deepcopy(state.get('terminal_by_stage') or {})
    proofs = deepcopy(state.get('proofs_by_stage') or {})
    recovery_attempts = deepcopy(
        state.get('recovery_attempts_by_stage') or {}
    )
    applied_run_ids = list(
        state.get('applied_recovery_pipeline_run_ids') or []
    )
    if recovery_pipeline_run_id is not None:
        recovery_eod_collection_run_id = state.get(
            'eod_collection_run_id'
        )
        if (
            isinstance(recovery_pipeline_run_id, bool)
            or not isinstance(recovery_pipeline_run_id, int)
            or recovery_pipeline_run_id <= 0
            or isinstance(recovery_eod_collection_run_id, bool)
            or not isinstance(recovery_eod_collection_run_id, int)
            or recovery_eod_collection_run_id <= 0
        ):
            return None
        proof_run_ids = _followup_stage_debt_recovery_proof_run_ids(
            state,
            stage_id=stage_id,
        )
        if recovery_pipeline_run_id in proof_run_ids:
            return None
        current_attempts = recovery_attempts.get(stage_id, 0)
        if (
            isinstance(current_attempts, bool)
            or not isinstance(current_attempts, int)
            or current_attempts < 0
        ):
            return None
        if recovery_attempt_already_recorded:
            if (
                recovery_pipeline_run_id not in applied_run_ids
                or current_attempts <= 0
            ):
                return None
        else:
            if recovery_pipeline_run_id in applied_run_ids:
                return None
            recovery_attempts[stage_id] = current_attempts + 1
            applied_run_ids.append(recovery_pipeline_run_id)
        proof[FOLLOWUP_STAGE_PROOF_RECOVERY_LINEAGE_KEY] = {
            'schema_version': (
                FOLLOWUP_STAGE_RECOVERY_LINEAGE_SCHEMA_VERSION
            ),
            'pipeline_run_id': recovery_pipeline_run_id,
            'eod_collection_run_id': recovery_eod_collection_run_id,
            'stage_id': stage_id,
        }
    recovery_progress_count = state.get('recovery_progress_count', 0)
    if (
        isinstance(recovery_progress_count, bool)
        or not isinstance(recovery_progress_count, int)
        or recovery_progress_count < 0
    ):
        return None
    successful_set = set(successful.get(stage_id) or [])
    terminal_set = set(terminal.get(stage_id) or [])
    invocation_set = set(normalized_invocation)
    successful_set.difference_update(invocation_set)
    successful_set.update(partition['successful_stock_codes'])
    terminal_set.difference_update(invocation_set)
    terminal_set.update(partition['terminal_not_applicable_stock_codes'])
    pending[stage_id] = list(partition['retry_debt_stock_codes'])
    successful[stage_id] = sorted(successful_set)
    terminal[stage_id] = sorted(terminal_set)
    proofs.setdefault(stage_id, []).append(proof)
    if (
        recovery_pipeline_run_id is not None
        and (
            partition['successful_stock_codes']
            or partition['terminal_not_applicable_stock_codes']
        )
    ):
        recovery_progress_count += 1

    _canonicalize_followup_stage_debt_graph(
        pending=pending,
        successful=successful,
        terminal=terminal,
        admit_newly_eligible=True,
    )

    normalized_pending = {
        key: list(value)
        for key, value in pending.items()
        if value
    }
    return {
        **dict(state),
        'pending_by_stage': normalized_pending,
        'successful_by_stage': {
            key: list(value)
            for key, value in successful.items()
            if value
        },
        'terminal_by_stage': {
            key: list(value)
            for key, value in terminal.items()
            if value
        },
        'proofs_by_stage': proofs,
        'recovery_progress_count': recovery_progress_count,
        'recovery_attempts_by_stage': recovery_attempts,
        'applied_recovery_pipeline_run_ids': applied_run_ids,
    }


def _build_followup_stage_recovery_scope_outcome(
    state: Mapping[str, Any],
    *,
    stage_id: str,
) -> dict[str, Any] | None:
    """Bind one debt retry to the durable stage-local partition.

    The selected stage universe is deliberately not inferred as the full EOD
    membership.  Upstream debt may still be ineligible for this stage; only
    symbols already classified as success, retry debt, or terminal by the
    durable stage-debt state may authorize a recovery write.
    """

    manifest_version = FOLLOWUP_EXACT_STAGE_RECOVERY_MANIFEST_VERSION.get(
        stage_id
    )
    if manifest_version is None:
        return None
    pending = _dedupe_symbol_codes(
        (state.get('pending_by_stage') or {}).get(stage_id) or []
    )
    successful = _dedupe_symbol_codes(
        (state.get('successful_by_stage') or {}).get(stage_id) or []
    )
    terminal = _dedupe_symbol_codes(
        (state.get('terminal_by_stage') or {}).get(stage_id) or []
    )
    if not pending:
        return None
    selected = _dedupe_symbol_codes([*successful, *pending, *terminal])
    try:
        return {
            'schema_version': (
                FOLLOWUP_STAGE_RECOVERY_SCOPE_SCHEMA_VERSION
            ),
            'stage_id': stage_id,
            **build_exact_symbol_scope_outcome(
                manifest_version=manifest_version,
                selected_symbols=selected,
                successful_symbols=successful,
                retry_debt_symbols=pending,
                terminal_not_applicable_symbols=terminal,
            ),
        }
    except ValueError:
        return None


def _next_followup_stage_debt_request(
    state: Mapping[str, Any],
) -> tuple[str, list[str]] | None:
    pending = state.get('pending_by_stage')
    if not isinstance(pending, Mapping):
        return None
    raw_attempts = state.get('recovery_attempts_by_stage')
    attempts = raw_attempts if isinstance(raw_attempts, Mapping) else {}
    candidates: list[tuple[int, int, str, list[str]]] = []
    for stage_order, stage_id in enumerate(
        FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER
    ):
        symbols = _dedupe_symbol_codes(pending.get(stage_id) or [])
        if symbols:
            raw_attempt_count = attempts.get(stage_id, 0)
            attempt_count = (
                raw_attempt_count
                if isinstance(raw_attempt_count, int)
                and not isinstance(raw_attempt_count, bool)
                and raw_attempt_count >= 0
                else 0
            )
            candidates.append(
                (attempt_count, stage_order, stage_id, symbols)
            )
    if not candidates:
        return None
    _, _, stage_id, symbols = min(candidates)
    return stage_id, symbols


def _followup_stage_debt_terminal_cohort(
    state: Mapping[str, Any],
    *,
    point_in_time_recovery: bool,
    require_prior_attempts: bool = True,
) -> tuple[str, list[str]] | None:
    """Return the exact cohort safe to enter the terminal suffix.

    Prior-date recovery must not recreate current-only KIS signal features.
    It therefore uses the intersection that completed every stored-data
    factor.  A current-date recovery requires the stricter published signal
    feature success set.
    """

    successful = state.get('successful_by_stage')
    pending = state.get('pending_by_stage')
    attempts = state.get('recovery_attempts_by_stage')
    if not isinstance(successful, Mapping) or not isinstance(pending, Mapping):
        return None
    attempts = attempts if isinstance(attempts, Mapping) else {}
    attempted_stage_ids = (
        ('piotroski_filter', *FACTOR_FOLLOWUP_STAGE_ORDER)
        if point_in_time_recovery
        else (
            'piotroski_filter',
            *FACTOR_FOLLOWUP_STAGE_ORDER,
            'signal_features',
        )
    )
    if require_prior_attempts:
        for stage_id in attempted_stage_ids:
            stage_pending = _dedupe_symbol_codes(pending.get(stage_id) or [])
            raw_attempts = attempts.get(stage_id, 0)
            attempt_count = (
                raw_attempts
                if isinstance(raw_attempts, int)
                and not isinstance(raw_attempts, bool)
                and raw_attempts >= 0
                else 0
            )
            if stage_pending and attempt_count == 0:
                return None

    if point_in_time_recovery:
        factor_sets: list[set[str]] = []
        for stage_id in FACTOR_FOLLOWUP_STAGE_ORDER:
            stage_symbols = _dedupe_symbol_codes(
                successful.get(stage_id) or []
            )
            if not stage_symbols:
                return None
            factor_sets.append(set(stage_symbols))
        cohort = set.intersection(*factor_sets)
        cohort_basis = 'factor_intersection'
    else:
        cohort = set(
            _dedupe_symbol_codes(
                successful.get('signal_features') or []
            )
        )
        cohort_basis = 'signal_features'
    if not cohort:
        return None
    return cohort_basis, sorted(cohort)


def _validated_followup_stage_debt_terminal_progression_marker(
    value: object,
    *,
    state: Mapping[str, Any],
    market_date: str,
    eod_collection_run_id: int,
) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    if set(value) != {
        'schema_version',
        'market_date',
        'eod_collection_run_id',
        'processed_stock_codes',
        'processed_stock_code_count',
        'processed_membership_sha256',
        'batches',
    }:
        return None
    if not (
        value.get('schema_version')
        == FOLLOWUP_STAGE_DEBT_TERMINAL_PROGRESSION_SCHEMA_VERSION
        and str(value.get('market_date') or '').strip() == market_date
        and value.get('eod_collection_run_id') == eod_collection_run_id
    ):
        return None
    processed = value.get('processed_stock_codes')
    batches = value.get('batches')
    if not isinstance(processed, list) or not isinstance(batches, list):
        return None
    normalized_processed = sorted(_dedupe_symbol_codes(processed))
    if (
        not normalized_processed
        or normalized_processed != processed
        or value.get('processed_stock_code_count')
        != len(normalized_processed)
        or value.get('processed_membership_sha256')
        != market_membership_sha256(normalized_processed)
    ):
        return None

    batch_symbols_seen: set[str] = set()
    pipeline_run_ids_seen: set[int] = set()
    normalized_batches: list[dict[str, Any]] = []
    for raw_batch in batches:
        if not isinstance(raw_batch, Mapping) or set(raw_batch) != {
            'pipeline_run_id',
            'cohort_basis',
            'stock_codes',
            'stock_code_count',
            'membership_sha256',
            'outcome_status',
        }:
            return None
        pipeline_run_id = raw_batch.get('pipeline_run_id')
        cohort_basis = str(raw_batch.get('cohort_basis') or '').strip()
        stock_codes = raw_batch.get('stock_codes')
        outcome_status = str(
            raw_batch.get('outcome_status') or ''
        ).strip()
        if (
            isinstance(pipeline_run_id, bool)
            or not isinstance(pipeline_run_id, int)
            or pipeline_run_id <= 0
            or pipeline_run_id in pipeline_run_ids_seen
            or cohort_basis not in {'factor_intersection', 'signal_features'}
            or outcome_status
            not in {'completed', 'validated_nonblocking_partial'}
            or not isinstance(stock_codes, list)
        ):
            return None
        normalized_stock_codes = sorted(_dedupe_symbol_codes(stock_codes))
        cohort = _followup_stage_debt_terminal_cohort(
            state,
            point_in_time_recovery=(
                cohort_basis == 'factor_intersection'
            ),
            require_prior_attempts=False,
        )
        if (
            not normalized_stock_codes
            or normalized_stock_codes != stock_codes
            or raw_batch.get('stock_code_count')
            != len(normalized_stock_codes)
            or raw_batch.get('membership_sha256')
            != market_membership_sha256(normalized_stock_codes)
            or cohort is None
            or cohort[0] != cohort_basis
            or not set(normalized_stock_codes).issubset(set(cohort[1]))
            or batch_symbols_seen.intersection(normalized_stock_codes)
        ):
            return None
        pipeline_run_ids_seen.add(pipeline_run_id)
        batch_symbols_seen.update(normalized_stock_codes)
        normalized_batches.append(dict(raw_batch))
    if batch_symbols_seen != set(normalized_processed):
        return None
    return {
        'schema_version': (
            FOLLOWUP_STAGE_DEBT_TERMINAL_PROGRESSION_SCHEMA_VERSION
        ),
        'market_date': market_date,
        'eod_collection_run_id': eod_collection_run_id,
        'processed_stock_codes': normalized_processed,
        'processed_stock_code_count': len(normalized_processed),
        'processed_membership_sha256': market_membership_sha256(
            normalized_processed
        ),
        'batches': normalized_batches,
    }


def _validated_followup_stage_debt_terminal_progression_result(
    result: object,
    *,
    market_date: str,
    eod_collection_run_id: int,
    cohort_symbols: Sequence[str],
) -> dict[str, Any] | None:
    if not isinstance(result, Mapping):
        return None
    pipeline_run_id = result.get('pipeline_run_id')
    pipeline_run = result.get('pipeline_run')
    resolved_stages = result.get('resolved_stages')
    validated_resolved_stages = (
        _resolved_terminal_followup_continuation_stages(resolved_stages)
    )
    normalized_cohort = sorted(_dedupe_symbol_codes(cohort_symbols))
    if (
        not normalized_cohort
        or isinstance(pipeline_run_id, bool)
        or not isinstance(pipeline_run_id, int)
        or pipeline_run_id <= 0
        or not isinstance(pipeline_run, Mapping)
        or pipeline_run.get('id') != pipeline_run_id
        or str(result.get('market_date') or '').strip() != market_date
        or validated_resolved_stages is None
        or resolved_stages != list(validated_resolved_stages)
    ):
        return None
    run_metadata = pipeline_run.get('run_metadata')
    if not isinstance(run_metadata, Mapping) or not (
        pipeline_run.get('trigger') == AUTO_FOLLOWUP_RECOVERY_TRIGGER
        and run_metadata.get('reused_eod_collection_run_id')
        == eod_collection_run_id
        and run_metadata.get('resolved_stages')
        == list(validated_resolved_stages)
        and sorted(
            _dedupe_symbol_codes(run_metadata.get('symbols') or [])
        )
        == normalized_cohort
    ):
        return None
    nonblocking_marker = validated_nonblocking_partial_progression_marker(
        result,
        expected_eod_collection_run_id=eod_collection_run_id,
    )
    if daily_pipeline_outcome_is_satisfied(result):
        outcome_status = 'completed'
    elif nonblocking_marker is not None:
        outcome_status = 'validated_nonblocking_partial'
    else:
        return None
    return {
        'pipeline_run_id': pipeline_run_id,
        'outcome_status': outcome_status,
        **(
            {'nonblocking_partial_progression': nonblocking_marker}
            if nonblocking_marker is not None
            else {}
        ),
    }


def _resolve_followup_recovery_stages(
    *,
    recovery_scope: Sequence[str],
    incomplete_stages: Sequence[str],
    has_exact_stage_debt_request: bool,
) -> list[str]:
    """Return the minimal safe suffix, isolating exact trending repair."""

    if not incomplete_stages:
        return []
    resume_from_stage = incomplete_stages[0]
    recovery_scope_list = list(recovery_scope)
    resume_index = recovery_scope_list.index(resume_from_stage)
    if has_exact_stage_debt_request or list(incomplete_stages) == [
        'trending_ranks'
    ]:
        return [resume_from_stage]
    return recovery_scope_list[resume_index:]


def _resolve_followup_stage_debt_state(
    *,
    market_date: str,
    eod_collection_run_id: int,
    latest_eod_metadata: Mapping[str, Any],
    stage_rows: Mapping[str, Any],
    incomplete_stages: Sequence[str],
    latest_pipeline_run: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve only manifest-backed follow-up debt into per-stage scopes."""

    admission_partition = _validated_followup_admission_partition(
        latest_eod_metadata
    )
    parent_symbols = (
        list(admission_partition['successful_stock_codes'])
        if admission_partition is not None
        else []
    )
    publication_symbols = (
        list(admission_partition['selected_stock_codes'])
        if admission_partition is not None
        else []
    )
    raw_persisted_state = latest_eod_metadata.get(
        FOLLOWUP_STAGE_DEBT_METADATA_KEY
    )
    if raw_persisted_state is not None:
        if not parent_symbols:
            return {
                'required': True,
                'ready': False,
                'reason': 'followup_stage_debt_parent_scope_unavailable',
            }
        persisted_state = _validated_followup_stage_debt_state(
            raw_persisted_state,
            market_date=market_date,
            eod_collection_run_id=eod_collection_run_id,
            parent_symbols=parent_symbols,
            publication_symbols=publication_symbols,
            require_pending=False,
        )
        if persisted_state is None:
            return {
                'required': True,
                'ready': False,
                'reason': 'followup_stage_debt_state_invalid',
            }
        pipeline_metadata = (
            latest_pipeline_run.get('run_metadata')
            if isinstance(latest_pipeline_run, Mapping)
            else None
        )
        linked_resolved_stages = (
            pipeline_metadata.get('resolved_stages')
            if isinstance(pipeline_metadata, Mapping)
            else None
        )
        linked_recovery_stage = (
            str(linked_resolved_stages[0]).strip()
            if isinstance(linked_resolved_stages, list)
            and len(linked_resolved_stages) == 1
            and str(linked_resolved_stages[0]).strip()
            in FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER
            and str(latest_pipeline_run.get('trigger') or '').strip()
            == AUTO_FOLLOWUP_RECOVERY_TRIGGER
            and pipeline_metadata.get('reused_eod_collection_run_id')
            == eod_collection_run_id
            and _dedupe_symbol_codes(
                (
                    persisted_state.get('pending_by_stage') or {}
                ).get(str(linked_resolved_stages[0]).strip()) or []
            )
            else None
        )
        linked_recovery_symbols = (
            _dedupe_symbol_codes(
                (persisted_state.get('pending_by_stage') or {}).get(
                    linked_recovery_stage
                )
                or []
            )
            if linked_recovery_stage is not None
            else []
        )
        linked_recovery_run_id = (
            latest_pipeline_run.get('id')
            if linked_recovery_stage is not None
            and isinstance(latest_pipeline_run, Mapping)
            and isinstance(latest_pipeline_run.get('id'), int)
            and not isinstance(latest_pipeline_run.get('id'), bool)
            and int(latest_pipeline_run['id']) > 0
            else None
        )
        reconciled = False
        reconciliation_error: str | None = None
        if (
            linked_recovery_stage is not None
            and linked_recovery_run_id is not None
        ):
            candidate = stage_rows.get(linked_recovery_stage)
            exact_evidence_present = _followup_exact_evidence_present(
                candidate
            )
            attempt_already_recorded = linked_recovery_run_id in set(
                persisted_state.get(
                    'applied_recovery_pipeline_run_ids'
                )
                or []
            )
            proof_already_applied = linked_recovery_run_id in (
                _followup_stage_debt_recovery_proof_run_ids(
                    persisted_state,
                    stage_id=linked_recovery_stage,
                )
            )
            attempt_finished = _followup_recovery_stage_attempt_finished(
                candidate,
                expected_stage_id=linked_recovery_stage,
            )
            if not proof_already_applied and exact_evidence_present:
                validated = _validated_followup_exact_partition(
                    stage_id=linked_recovery_stage,
                    stage=candidate,
                    expected_market_date=market_date,
                    expected_publication_symbols=linked_recovery_symbols,
                    allowed_partition_symbols=linked_recovery_symbols,
                    allow_quality_only_partial=True,
                )
                if validated is None:
                    advanced_state = (
                        _record_followup_stage_debt_attempt(
                            persisted_state,
                            stage_id=linked_recovery_stage,
                            invocation_symbols=linked_recovery_symbols,
                            recovery_pipeline_run_id=(
                                linked_recovery_run_id
                            ),
                        )
                        if attempt_finished
                        and not attempt_already_recorded
                        else None
                    )
                    if (
                        advanced_state is None
                        and not attempt_already_recorded
                    ):
                        return {
                            'required': True,
                            'ready': False,
                            'reason': (
                                f'{linked_recovery_stage}_recovery_manifest_invalid'
                            ),
                        }
                    if advanced_state is not None:
                        reconciliation_error = (
                            f'{linked_recovery_stage}_recovery_manifest_invalid'
                        )
                else:
                    partition, _proof = validated
                    if set(partition['selected_stock_codes']) != set(
                        linked_recovery_symbols
                    ):
                        advanced_state = (
                            _record_followup_stage_debt_attempt(
                                persisted_state,
                                stage_id=linked_recovery_stage,
                                invocation_symbols=linked_recovery_symbols,
                                recovery_pipeline_run_id=(
                                    linked_recovery_run_id
                                ),
                            )
                            if attempt_finished
                            and not attempt_already_recorded
                            else None
                        )
                        if (
                            advanced_state is None
                            and not attempt_already_recorded
                        ):
                            return {
                                'required': True,
                                'ready': False,
                                'reason': (
                                    f'{linked_recovery_stage}_recovery_scope_mismatch'
                                ),
                            }
                        if advanced_state is not None:
                            reconciliation_error = (
                                f'{linked_recovery_stage}_recovery_scope_mismatch'
                            )
                    else:
                        advanced_state = _advance_followup_stage_debt_state(
                            persisted_state,
                            stage_id=linked_recovery_stage,
                            invocation_symbols=linked_recovery_symbols,
                            stage_result=candidate,
                            recovery_pipeline_run_id=(
                                linked_recovery_run_id
                            ),
                            recovery_attempt_already_recorded=(
                                attempt_already_recorded
                            ),
                        )
                if advanced_state is None and validated is not None:
                    return {
                        'required': True,
                        'ready': False,
                        'reason': (
                            f'{linked_recovery_stage}_recovery_transition_invalid'
                        ),
                    }
                if advanced_state is not None:
                    persisted_state = advanced_state
                    reconciled = True
            elif (
                not proof_already_applied
                and not attempt_already_recorded
                and attempt_finished
            ):
                advanced_state = _record_followup_stage_debt_attempt(
                    persisted_state,
                    stage_id=linked_recovery_stage,
                    invocation_symbols=linked_recovery_symbols,
                    recovery_pipeline_run_id=linked_recovery_run_id,
                )
                if advanced_state is None:
                    return {
                        'required': True,
                        'ready': False,
                        'reason': (
                            f'{linked_recovery_stage}_recovery_transition_invalid'
                        ),
                    }
                persisted_state = advanced_state
                reconciled = True
                reconciliation_error = (
                    f'{linked_recovery_stage}_recovery_manifest_missing'
                )
        return {
            'required': True,
            'ready': True,
            'state': persisted_state,
            'source': (
                'persisted_stage_debt_reconciled'
                if reconciled
                else 'persisted_stage_debt'
            ),
            **(
                {'reconciliation_error': reconciliation_error}
                if reconciliation_error is not None
                else {}
            ),
        }

    eod_stage_rows = latest_eod_metadata.get('stage_progress')
    if not isinstance(eod_stage_rows, Mapping):
        eod_stage_rows = {}
    incomplete_exact_stages = [
        stage_id
        for stage_id in incomplete_stages
        if stage_id in FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER
    ]
    partitions: dict[str, dict[str, Any]] = {}
    proofs_by_stage: dict[str, list[dict[str, Any]]] = {}
    provider_result_summary: dict[str, Any] | None = None
    exact_evidence_seen = False
    for stage_id in FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER:
        current_candidate = stage_rows.get(stage_id)
        candidates = (
            [current_candidate]
            if _followup_exact_evidence_present(current_candidate)
            else [
                eod_stage_rows.get(stage_id),
                current_candidate,
            ]
        )
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                continue
            result_summary = _stage_result_summary(candidate)
            exact_evidence_present = _followup_exact_evidence_present(
                candidate
            )
            exact_result = (
                _followup_exact_result_authority(
                    stage_id=stage_id,
                    stage=candidate,
                )
                if exact_evidence_present
                else None
            )
            if exact_evidence_present and exact_result is None:
                return {
                    'required': True,
                    'ready': False,
                    'reason': f'{stage_id}_stage_debt_manifest_invalid',
                }
            expected_stage_publication_symbols = publication_symbols
            if stage_id == 'signal_features' and exact_evidence_present:
                signal_selected = exact_result.get(
                    'selected_stock_codes'
                ) if isinstance(exact_result, Mapping) else None
                normalized_signal_selected = (
                    _dedupe_symbol_codes(signal_selected)
                    if isinstance(signal_selected, list)
                    else []
                )
                dependency_partitions = [
                    partitions.get(dependency_stage_id)
                    for dependency_stage_id in (
                        FOLLOWUP_EXACT_STAGE_DEPENDENCIES[stage_id]
                    )
                ]
                if (
                    not normalized_signal_selected
                    or normalized_signal_selected != signal_selected
                    or any(
                        partition is None
                        for partition in dependency_partitions
                    )
                ):
                    return {
                        'required': True,
                        'ready': False,
                        'reason': (
                            'signal_features_stage_debt_manifest_invalid'
                        ),
                    }
                eligible_signal_symbols = set(parent_symbols)
                for dependency_partition in dependency_partitions:
                    eligible_signal_symbols.intersection_update(
                        dependency_partition['successful_stock_codes']
                    )
                if set(normalized_signal_selected) != (
                    eligible_signal_symbols
                ):
                    return {
                        'required': True,
                        'ready': False,
                        'reason': (
                            'signal_features_stage_debt_scope_mismatch'
                        ),
                    }
                # Signal features are published only for the factor-admitted
                # intersection, not the full frozen EOD universe.  The
                # selected list is authority only after the dependency
                # intersection above proves that exact invocation.
                expected_stage_publication_symbols = (
                    normalized_signal_selected
                )
            validated = _validated_followup_exact_partition(
                stage_id=stage_id,
                stage=candidate,
                expected_market_date=market_date,
                expected_publication_symbols=(
                    expected_stage_publication_symbols
                ),
                allowed_partition_symbols=parent_symbols,
                allow_quality_only_partial=True,
            )
            if exact_evidence_present and validated is None:
                return {
                    'required': True,
                    'ready': False,
                    'reason': (
                        f'{stage_id}_stage_debt_manifest_invalid'
                    ),
                }
            if validated is not None:
                exact_evidence_seen = True
                partition, proof = validated
                dependencies = FOLLOWUP_EXACT_STAGE_DEPENDENCIES.get(
                    stage_id,
                    (),
                )
                if dependencies:
                    dependency_partitions = [
                        partitions.get(dependency_stage_id)
                        for dependency_stage_id in dependencies
                    ]
                    missing_dependency_partition = any(
                        dependency_partition is None
                        for dependency_partition in dependency_partitions
                    )
                    legacy_piotroski_quality_authority = bool(
                        missing_dependency_partition
                        and stage_id == 'piotroski_filter'
                        and str(
                            partition.get('status')
                            or exact_result.get('status')
                            or ''
                        ).strip().lower()
                        == 'partial'
                        and _followup_quality_only_partial_is_satisfied(
                            stage_id=stage_id,
                            stage=candidate,
                            result_summary=result_summary,
                            selected_symbols=partition[
                                'selected_stock_codes'
                            ],
                            successful_symbols=partition[
                                'successful_stock_codes'
                            ],
                            retry_debt_symbols=partition[
                                'retry_debt_stock_codes'
                            ],
                            terminal_not_applicable_symbols=partition[
                                'terminal_not_applicable_stock_codes'
                            ],
                        )
                    )
                    if (
                        missing_dependency_partition
                        and not legacy_piotroski_quality_authority
                    ):
                        return {
                            'required': True,
                            'ready': False,
                            'reason': (
                                f'{stage_id}_stage_debt_scope_mismatch'
                            ),
                        }
                    if legacy_piotroski_quality_authority:
                        expected_selected = set(parent_symbols)
                    else:
                        expected_selected = set(
                            dependency_partitions[0][
                                'successful_stock_codes'
                            ]
                        )
                        for dependency_partition in (
                            dependency_partitions[1:]
                        ):
                            expected_selected.intersection_update(
                                dependency_partition[
                                    'successful_stock_codes'
                                ]
                            )
                else:
                    expected_selected = set(parent_symbols)
                if (
                    set(partition['selected_stock_codes']) != expected_selected
                    and stage_id in NON_DART_FACTOR_FOLLOWUP_STAGE_IDS
                    and set(partition['selected_stock_codes'])
                    == set(parent_symbols)
                ):
                    # Price-cohort shape: see _is_price_cohort_factor_stage.
                    expected_selected = set(parent_symbols)
                if set(partition['selected_stock_codes']) != expected_selected:
                    return {
                        'required': True,
                        'ready': False,
                        'reason': f'{stage_id}_stage_debt_scope_mismatch',
                    }
                partitions[stage_id] = partition
                stage_proofs = proofs_by_stage.setdefault(stage_id, [])
                if proof not in stage_proofs:
                    stage_proofs.append(proof)
            if stage_id == 'fundamentals_daily_refresh':
                provider_admission = _validated_required_provider_admission(
                    candidate,
                    expected_stage_id=stage_id,
                )
                if provider_admission is not None:
                    exact_evidence_seen = True
                    provider_result_summary = dict(result_summary)
            if validated is not None:
                break

    if not exact_evidence_seen:
        return {'required': False, 'ready': False}
    if not parent_symbols:
        return {
            'required': True,
            'ready': False,
            'reason': 'followup_stage_debt_parent_scope_unavailable',
        }

    pending_by_stage: dict[str, list[str]] = {}
    successful_by_stage: dict[str, list[str]] = {}
    terminal_by_stage: dict[str, list[str]] = {}
    for stage_id, partition in partitions.items():
        retry_debt = list(partition['retry_debt_stock_codes'])
        if retry_debt:
            pending_by_stage[stage_id] = retry_debt
        successful = list(partition['successful_stock_codes'])
        if successful:
            successful_by_stage[stage_id] = successful
        terminal = list(partition['terminal_not_applicable_stock_codes'])
        if terminal:
            terminal_by_stage[stage_id] = terminal
    _canonicalize_followup_stage_debt_graph(
        pending=pending_by_stage,
        successful=successful_by_stage,
        terminal=terminal_by_stage,
        admit_newly_eligible=False,
    )
    pending_by_stage = {
        stage_id: symbols
        for stage_id, symbols in pending_by_stage.items()
        if symbols
    }
    successful_by_stage = {
        stage_id: symbols
        for stage_id, symbols in successful_by_stage.items()
        if symbols
    }
    terminal_by_stage = {
        stage_id: symbols
        for stage_id, symbols in terminal_by_stage.items()
        if symbols
    }
    if (
        provider_result_summary is not None
        and 'fundamentals_daily_refresh' not in partitions
    ):
        pending_by_stage['fundamentals_daily_refresh'] = list(
            parent_symbols
        )

    # A blocked downstream stage without its own manifest is not guessed into
    # debt. Its exact scope is admitted only after the upstream proof succeeds.
    # Conversely, a directly failed/partial exact stage must carry a manifest.
    for stage_id in incomplete_exact_stages:
        if stage_id in pending_by_stage:
            continue
        stage = stage_rows.get(stage_id) or eod_stage_rows.get(stage_id)
        result_summary = _stage_result_summary(stage)
        reason = str(result_summary.get('reason') or '').strip()
        status = str(
            result_summary.get('status')
            or (stage.get('status') if isinstance(stage, Mapping) else '')
            or ''
        ).strip().lower()
        upstream_block = bool(
            status == 'blocked'
            and (
                reason.endswith('_incomplete')
                or reason
                in {
                    'declared_dependencies_incomplete',
                    'required_provider_not_ready',
                    'stored_recovery_inputs_incomplete',
                }
            )
        )
        if (
            stage_id == 'fundamentals_daily_refresh'
            and provider_result_summary is not None
        ):
            continue
        if not upstream_block and stage_id not in partitions:
            return {
                'required': True,
                'ready': False,
                'reason': f'{stage_id}_stage_debt_manifest_unavailable',
            }

    if not pending_by_stage:
        return {'required': False, 'ready': False}
    state = {
        'schema_version': FOLLOWUP_STAGE_DEBT_SCHEMA_VERSION,
        'market_date': market_date,
        'eod_collection_run_id': eod_collection_run_id,
        'pending_by_stage': pending_by_stage,
        'successful_by_stage': successful_by_stage,
        'terminal_by_stage': terminal_by_stage,
        'proofs_by_stage': proofs_by_stage,
        'recovery_progress_count': 0,
        'recovery_attempts_by_stage': {},
        'applied_recovery_pipeline_run_ids': [],
        **(
            {'provider_admission': provider_result_summary}
            if provider_result_summary is not None
            else {}
        ),
    }
    return {
        'required': True,
        'ready': True,
        'state': state,
        'source': 'exact_stage_manifests',
    }


def _resolve_dart_followup_recovery_scope(
    *,
    incomplete_stages: Sequence[str],
    stage_rows: Mapping[str, Any],
    run_metadata: Mapping[str, Any],
    latest_eod_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    """Resolve a narrow DART replay scope or explicitly fail closed."""

    if not incomplete_stages or incomplete_stages[0] != (
        'fundamentals_daily_refresh'
    ):
        return {'required': False, 'ready': False, 'symbols': []}
    stage = stage_rows.get('fundamentals_daily_refresh')
    if not isinstance(stage, Mapping):
        return {
            'required': True,
            'ready': False,
            'symbols': [],
            'reason': 'dart_recovery_stage_evidence_unavailable',
        }
    result_summary = _stage_result_summary(stage)
    exact_evidence_present = _followup_exact_evidence_present(stage)
    exact_result = (
        _followup_exact_result_authority(
            stage_id='fundamentals_daily_refresh',
            stage=stage,
        )
        if exact_evidence_present
        else None
    )
    selected_symbols = (
        exact_result.get('selected_stock_codes')
        if isinstance(exact_result, Mapping)
        else None
    )
    if exact_evidence_present:
        if exact_result is None or not isinstance(selected_symbols, list):
            return {
                'required': True,
                'ready': False,
                'symbols': [],
                'reason': 'dart_recovery_exact_manifest_invalid',
            }
        if exact_result.get('scope_outcome_manifest_version') not in (
            FOLLOWUP_EXACT_STAGE_MANIFEST_VERSIONS[
                'fundamentals_daily_refresh'
            ]
        ):
            return {
                'required': True,
                'ready': False,
                'symbols': [],
                'reason': 'dart_recovery_manifest_version_invalid',
            }
        validated_dart_partition = _validated_followup_exact_partition(
            stage_id='fundamentals_daily_refresh',
            stage=stage,
            expected_market_date=str(
                run_metadata.get('market_date')
                or result_summary.get('market_date')
                or exact_result.get('market_date')
                or ''
            ),
            expected_publication_symbols=selected_symbols,
            allowed_partition_symbols=selected_symbols,
        )
        partition = (
            validated_dart_partition[0]
            if validated_dart_partition is not None
            else None
        )
        if partition is not None:
            if partition['manifest_version'] not in {
                'dart_daily_data_scope_outcomes_v1',
                'dart_fundamentals_scope_outcomes_v1',
            }:
                return {
                    'required': True,
                    'ready': False,
                    'symbols': [],
                    'reason': 'dart_recovery_manifest_version_invalid',
                }
            retry_debt_symbols = list(
                partition['retry_debt_stock_codes']
            )
            if retry_debt_symbols:
                persisted_run_symbols = _dedupe_symbol_codes(
                    run_metadata.get('symbols') or []
                )
                parent_symbols = _validated_followup_admitted_symbols(
                    latest_eod_metadata
                )
                if not persisted_run_symbols and not parent_symbols:
                    return {
                        'required': True,
                        'ready': False,
                        'symbols': [],
                        'reason': (
                            'dart_recovery_scope_authority_unavailable'
                        ),
                    }
                if persisted_run_symbols and set(
                    partition['selected_stock_codes']
                ) != set(persisted_run_symbols):
                    return {
                        'required': True,
                        'ready': False,
                        'symbols': [],
                        'reason': 'dart_recovery_manifest_run_scope_mismatch',
                    }
                if parent_symbols and not set(
                    partition['selected_stock_codes']
                ).issubset(parent_symbols):
                    return {
                        'required': True,
                        'ready': False,
                        'symbols': [],
                        'reason': 'dart_recovery_manifest_parent_scope_mismatch',
                    }
                return {
                    'required': True,
                    'ready': True,
                    'symbols': retry_debt_symbols,
                    'source': 'first_incomplete_exact_manifest',
                    'manifest_version': partition['manifest_version'],
                }
    if exact_evidence_present:
        return {
            'required': True,
            'ready': False,
            'symbols': [],
            'reason': 'dart_recovery_exact_manifest_invalid',
        }

    provider_admission = _validated_required_provider_admission(
        stage,
        expected_stage_id='fundamentals_daily_refresh',
    )
    if provider_admission is None:
        return {
            'required': True,
            'ready': False,
            'symbols': [],
            'reason': 'dart_recovery_exact_manifest_unavailable',
        }
    parent_symbols = _validated_followup_admitted_symbols(
        latest_eod_metadata
    )
    persisted_run_symbols = _dedupe_symbol_codes(
        run_metadata.get('symbols') or []
    )
    provider_scope = persisted_run_symbols or parent_symbols or []
    if (
        not provider_scope
        or (
            parent_symbols
            and not set(provider_scope).issubset(parent_symbols)
        )
    ):
        return {
            'required': True,
            'ready': False,
            'symbols': [],
            'reason': 'dart_provider_admission_scope_unavailable',
        }
    return {
        'required': True,
        'ready': True,
        'symbols': provider_scope,
        'source': 'required_provider_admission_frozen_scope',
        'provider_admission': provider_admission,
    }


def _count_nonconsuming_provider_admission_runs(
    runs: Sequence[Mapping[str, Any]],
) -> int:
    seen_run_ids: set[int] = set()
    count = 0
    for run in runs:
        raw_run_id = run.get('id')
        if isinstance(raw_run_id, bool):
            continue
        try:
            run_id = int(raw_run_id)
        except (TypeError, ValueError):
            continue
        if run_id <= 0 or run_id in seen_run_ids:
            continue
        seen_run_ids.add(run_id)
        if str(run.get('trigger') or '').strip() != (
            AUTO_FOLLOWUP_RECOVERY_TRIGGER
        ):
            continue
        run_metadata = run.get('run_metadata')
        resolved_stages = (
            run_metadata.get('resolved_stages')
            if isinstance(run_metadata, Mapping)
            else None
        )
        if not isinstance(resolved_stages, list) or not resolved_stages:
            continue
        first_resolved_stage = str(resolved_stages[0] or '').strip()
        if not first_resolved_stage:
            continue
        stages = run.get('stages')
        if not isinstance(stages, Mapping):
            continue
        # Exempt only a recovery whose first admitted stage was rejected
        # before an attempt. A later provider block cannot erase budget spent
        # by a real failure earlier in the same recovery run.
        if _validated_required_provider_admission(
            stages.get(first_resolved_stage),
            expected_stage_id=first_resolved_stage,
        ) is not None:
            count += 1
    return count


def _count_nonconsuming_stage_debt_progress_runs(
    state: Mapping[str, Any] | None,
) -> int:
    """Return the explicit count of retries that durably reduced debt.

    Proof order cannot distinguish a source-run baseline from the first
    recovery proof of a stage admitted later by dependency propagation.  The
    counter is therefore initialized with the source state and incremented
    only by ``_advance_followup_stage_debt_state`` when a recovery classifies
    at least one invoked symbol as success or terminal. Failed-all retries
    continue to consume the bounded failure budget.
    """

    if not isinstance(state, Mapping):
        return 0
    progress_count = state.get('recovery_progress_count', 0)
    if (
        isinstance(progress_count, bool)
        or not isinstance(progress_count, int)
        or progress_count < 0
    ):
        return 0
    return progress_count


def _count_all_nonconsuming_provider_admission_runs(
    *,
    current_run: Mapping[str, Any],
    raw_recovery_attempts: int,
    market_date: str,
    reused_eod_collection_run_id: int,
) -> int:
    """Count every exact zero-attempt provider block in one lineage."""

    if (
        str(current_run.get('trigger') or '').strip()
        != AUTO_FOLLOWUP_RECOVERY_TRIGGER
        or type(current_run.get('id')) is not int
        or int(current_run['id']) <= 0
        or raw_recovery_attempts <= 0
    ):
        return 0

    collected_runs: list[Mapping[str, Any]] = [current_run]
    seen_run_ids = {int(current_run['id'])}
    before_pipeline_run_id = int(current_run['id'])
    remaining = max(int(raw_recovery_attempts) - 1, 0)
    while remaining > 0:
        page = (
            daily_pipeline_run_state_service
            .list_prior_followup_runs_for_lineage(
                market_date=market_date,
                reused_eod_collection_run_id=(
                    reused_eod_collection_run_id
                ),
                before_pipeline_run_id=before_pipeline_run_id,
                limit=min(remaining, 8),
                order_by_id_desc=True,
            )
        )
        if not page:
            break
        new_runs: list[Mapping[str, Any]] = []
        new_run_ids: list[int] = []
        for run in page:
            raw_run_id = run.get('id') if isinstance(run, Mapping) else None
            if isinstance(raw_run_id, bool):
                continue
            try:
                run_id = int(raw_run_id)
            except (TypeError, ValueError):
                continue
            if (
                run_id <= 0
                or run_id >= before_pipeline_run_id
                or run_id in seen_run_ids
            ):
                continue
            seen_run_ids.add(run_id)
            new_run_ids.append(run_id)
            new_runs.append(run)
        if not new_runs:
            break
        collected_runs.extend(new_runs)
        remaining = max(remaining - len(new_runs), 0)
        before_pipeline_run_id = min(new_run_ids)
    return _count_nonconsuming_provider_admission_runs(collected_runs)


def _is_dart_provider_outage_rearm_candidate(
    pipeline_run: object,
) -> bool:
    """Recognize a DART maintenance failure that should not stay terminal."""

    if not isinstance(pipeline_run, Mapping):
        return False
    stages = pipeline_run.get('stages')
    if not isinstance(stages, Mapping):
        return False
    stage = stages.get('fundamentals_daily_refresh')
    if not isinstance(stage, Mapping):
        return False
    if str(stage.get('status') or '').strip().lower() not in {
        'failed',
        'partial',
    }:
        return False
    details = stage.get('details')
    result_summary = (
        details.get('result_summary')
        if isinstance(details, Mapping)
        else None
    )
    samples = (
        result_summary.get('samples')
        if isinstance(result_summary, Mapping)
        else None
    )
    lookup_errors = (
        samples.get('lookup_errors')
        if isinstance(samples, Mapping)
        else None
    )
    if not isinstance(lookup_errors, list):
        return False
    return any(
        isinstance(item, Mapping)
        and str(item.get('error_type') or '') == 'DartAPIError'
        and 'DART request failed [800]' in str(item.get('error') or '')
        for item in lookup_errors
    )


def _feedback_dispatch_circuit_marker(
    pipeline_run: object,
) -> tuple[bool, Mapping[str, Any] | None]:
    marker_present, _, marker = _feedback_dispatch_circuit_evidence(
        pipeline_run
    )
    return marker_present, marker


def _feedback_dispatch_circuit_evidence(
    pipeline_run: object,
) -> tuple[bool, str | None, Mapping[str, Any] | None]:
    if not isinstance(pipeline_run, Mapping):
        return False, None, None
    stages = pipeline_run.get('stages')
    if not isinstance(stages, Mapping):
        return False, None, None
    evidence: list[tuple[str, Mapping[str, Any] | None]] = []
    for stage_id in ('agent_feedback_summaries', 'signal_decider'):
        result_summary = _stage_result_summary(stages.get(stage_id))
        if 'openclaw_dispatch_circuit_breaker' not in result_summary:
            continue
        marker = result_summary.get(
            'openclaw_dispatch_circuit_breaker'
        )
        evidence.append(
            (
                stage_id,
                marker if isinstance(marker, Mapping) else None,
            )
        )
    if len(evidence) != 1:
        return bool(evidence), None, None
    stage_id, marker = evidence[0]
    return True, stage_id, marker


def _is_exact_signal_decider_retry_suffix(
    pipeline_run: object,
) -> bool:
    if not isinstance(pipeline_run, Mapping):
        return False
    run_metadata = pipeline_run.get('run_metadata')
    stages = pipeline_run.get('stages')
    if not isinstance(run_metadata, Mapping) or not isinstance(
        stages,
        Mapping,
    ):
        return False
    resolved_stages = tuple(run_metadata.get('resolved_stages') or ())
    resume_from_stage = str(
        run_metadata.get('resume_from_stage') or ''
    ).strip()
    completed_prefix = ('agent_feedback_summaries',)
    expected_stages = next(
        (
            stage_order
            for stage_order in (
                LEGACY_SIGNAL_DECIDER_RECOVERY_STAGES,
                SIGNAL_DECIDER_RECOVERY_STAGES,
            )
            if resolved_stages
            in {stage_order, completed_prefix + stage_order}
        ),
        None,
    )
    if expected_stages is None:
        return False
    legacy_suffix = expected_stages == LEGACY_SIGNAL_DECIDER_RECOVERY_STAGES
    exact_suffix_scope = bool(
        resolved_stages == expected_stages
        and resume_from_stage == expected_stages[0]
        and set(stages) == set(expected_stages)
    )
    normalized_suffix_scope = bool(
        resolved_stages == completed_prefix + expected_stages
        and resume_from_stage == completed_prefix[0]
        and set(stages) == set(resolved_stages)
        and all(
            str(
                (
                    stages.get(stage_id)
                    if isinstance(stages.get(stage_id), Mapping)
                    else {}
                ).get('status')
                or ''
            ).strip().lower()
            == 'completed'
            for stage_id in completed_prefix
        )
    )
    return bool(
        str(pipeline_run.get('trigger') or '').strip()
        == AUTO_FOLLOWUP_RECOVERY_TRIGGER
        and str(pipeline_run.get('status') or '').strip().lower()
        == 'blocked'
        and (exact_suffix_scope or normalized_suffix_scope)
        and all(
            str(
                (
                    stages.get(stage_id)
                    if isinstance(stages.get(stage_id), Mapping)
                    else {}
                ).get('status')
                or ''
            ).strip().lower()
            == 'blocked'
            for stage_id in expected_stages[1:]
        )
        and str(
            _stage_result_summary(
                stages.get('next_open_execution_plan')
            ).get('reason')
            or ''
        ).strip()
        == 'signal_decider_stage_incomplete'
        and (
            not legacy_suffix
            or (
                str(
                    _stage_result_summary(
                        stages.get('signal_decider_evaluations')
                    ).get('reason')
                    or ''
                ).strip()
                == 'declared_dependencies_incomplete'
                and _stage_result_summary(
                    stages.get('signal_decider_evaluations')
                ).get('incomplete_dependencies')
                == ['signal_decider']
            )
        )
    )


def _legacy_evaluation_only_completion_projection(
    pipeline_run: object,
) -> dict[str, Any] | None:
    """Project a completed legacy suffix onto the current execution boundary.

    The retired evaluation stage graded historical outcomes after the execution
    plan had already been persisted.  A terminal failure confined to that stage
    must remain readable, but it is neither new BUY debt nor authority to rerun
    the evaluation automatically.
    """

    if not isinstance(pipeline_run, Mapping):
        return None
    run_metadata = pipeline_run.get('run_metadata')
    stages = pipeline_run.get('stages')
    pipeline_run_id = pipeline_run.get('id')
    if (
        not isinstance(run_metadata, Mapping)
        or not isinstance(stages, Mapping)
        or isinstance(pipeline_run_id, bool)
        or not isinstance(pipeline_run_id, int)
        or pipeline_run_id <= 0
    ):
        return None
    raw_resolved_stages = run_metadata.get('resolved_stages')
    if not isinstance(raw_resolved_stages, list):
        return None
    resolved_stages = tuple(
        str(stage_id or '').strip() for stage_id in raw_resolved_stages
    )
    if (
        not resolved_stages
        or any(not stage_id for stage_id in resolved_stages)
        or len(resolved_stages) != len(set(resolved_stages))
        or resolved_stages[-1] != 'signal_decider_evaluations'
        or set(stages) != set(resolved_stages)
        or str(pipeline_run.get('current_stage') or '').strip()
        != 'signal_decider_evaluations'
        or str(pipeline_run.get('status') or '').strip().lower()
        in {'', 'queued', 'running', 'interrupted'}
    ):
        return None
    legacy_suffix = _resolved_terminal_followup_continuation_stages(
        resolved_stages
    )
    if legacy_suffix != LEGACY_TERMINAL_FOLLOWUP_CONTINUATION_STAGES:
        legacy_signal_suffix = _resolved_signal_decider_recovery_stages(
            resolved_stages
        )
        if legacy_signal_suffix != LEGACY_SIGNAL_DECIDER_RECOVERY_STAGES:
            return None

    evaluation_stage = stages.get('signal_decider_evaluations')
    if not isinstance(evaluation_stage, Mapping) or (
        daily_pipeline_outcome_is_satisfied(
            {
                **dict(evaluation_stage),
                'stage': 'signal_decider_evaluations',
            }
        )
    ):
        return None
    current_resolved_stages = resolved_stages[:-1]
    if any(
        not isinstance(stages.get(stage_id), Mapping)
        for stage_id in current_resolved_stages
    ):
        return None
    current_stages = {
        stage_id: dict(stages[stage_id])
        for stage_id in current_resolved_stages
    }
    if not all(
        daily_pipeline_outcome_is_satisfied(
            {**stage, 'stage': stage_id}
        )
        for stage_id, stage in current_stages.items()
    ):
        return None

    projected_metadata = dict(run_metadata)
    raw_requested_stages = projected_metadata.get('requested_stages')
    if isinstance(raw_requested_stages, list):
        requested_stages = [
            str(stage_id or '').strip()
            for stage_id in raw_requested_stages
        ]
        if requested_stages[-1:] == ['signal_decider_evaluations']:
            requested_stages = requested_stages[:-1]
        projected_metadata['requested_stages'] = requested_stages
    projected_metadata['resolved_stages'] = list(current_resolved_stages)
    projected_metadata['stage_statuses'] = {
        stage_id: str(stage.get('status') or '').strip().lower()
        for stage_id, stage in current_stages.items()
    }
    projected_pipeline_run = {
        **dict(pipeline_run),
        'status': 'completed',
        'current_stage': current_resolved_stages[-1],
        'run_metadata': projected_metadata,
        'stages': current_stages,
    }
    return {
        'status': 'completed',
        'pipeline_status': 'completed',
        'pipeline_run_id': pipeline_run_id,
        'pipeline_run': projected_pipeline_run,
        'market_date': pipeline_run.get('market_date'),
        'resolved_stages': list(current_resolved_stages),
        'stages': current_stages,
        'legacy_evaluation_stage_retired': True,
    }


def _active_execution_pipeline_projection(
    pipeline_run: Mapping[str, Any],
) -> dict[str, Any]:
    """Hide one retained legacy evaluation row from active-stage validation.

    Terminal-continuation claims preserve the old evaluation row in storage
    for audit while projecting ``resolved_stages`` to the current execution
    contract.  A reload therefore has one intentional extra row.  Only this
    exact archived shape may be filtered, and only in the copy passed to
    execution/readiness validators.
    """

    projected = dict(pipeline_run)
    metadata = pipeline_run.get('run_metadata')
    stages = pipeline_run.get('stages')
    if not (
        isinstance(metadata, Mapping)
        and isinstance(stages, Mapping)
        and terminal_continuation_marker_is_valid(metadata)
    ):
        return projected
    raw_resolved_stages = metadata.get('resolved_stages')
    if not isinstance(raw_resolved_stages, list):
        return projected
    resolved_stages = [
        str(stage_id or '').strip() for stage_id in raw_resolved_stages
    ]
    if (
        not resolved_stages
        or any(not stage_id for stage_id in resolved_stages)
        or len(resolved_stages) != len(set(resolved_stages))
        or 'signal_decider_evaluations' in resolved_stages
        or set(stages)
        != {*resolved_stages, 'signal_decider_evaluations'}
    ):
        return projected
    evidence = metadata.get('terminal_continuation_evidence')
    evidence_suffix = (
        evidence.get('suffix_stages')
        if isinstance(evidence, Mapping)
        and isinstance(evidence.get('suffix_stages'), Mapping)
        else None
    )
    if not (
        isinstance(evidence_suffix, Mapping)
        and set(evidence_suffix)
        == set(LEGACY_TERMINAL_FOLLOWUP_CONTINUATION_STAGES)
    ):
        return projected
    projected['stages'] = {
        stage_id: stages[stage_id] for stage_id in resolved_stages
    }
    return projected


def _feedback_retry_stage_id(pipeline_run: object) -> str:
    marker_present, marker_stage_id, _ = (
        _feedback_dispatch_circuit_evidence(pipeline_run)
    )
    if marker_present and marker_stage_id is not None:
        return marker_stage_id
    if not isinstance(pipeline_run, Mapping):
        return 'agent_feedback_summaries'
    stages = pipeline_run.get('stages')
    signal_stage = (
        stages.get('signal_decider')
        if isinstance(stages, Mapping)
        else None
    )
    if (
        not marker_present
        and _is_exact_signal_decider_retry_suffix(pipeline_run)
        and isinstance(signal_stage, Mapping)
        and str(signal_stage.get('status') or '').strip().lower()
        == 'partial'
    ):
        return 'signal_decider'
    return 'agent_feedback_summaries'


def _is_valid_retryable_feedback_dispatch_circuit_marker(
    marker: Mapping[str, Any] | None,
) -> bool:
    return bool(
        isinstance(marker, Mapping)
        and marker.get('schema_version')
        == OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION
        and marker.get('retryable') is True
        and str(marker.get('failure_kind') or '').strip()
        in OPENCLAW_SHARED_DISPATCH_RETRYABLE_FAILURE_KINDS
        and str(marker.get('source') or '').strip()
        in {
            'child_dispatch_circuit_breaker',
            'child_run_level_failure',
        }
    )


def _feedback_dispatch_rate_limit_backoff(
    *,
    marker: Mapping[str, Any] | None,
    pipeline_run: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any] | None:
    if not (
        _is_valid_retryable_feedback_dispatch_circuit_marker(marker)
        and str(marker.get('failure_kind') or '').strip()
        == 'openclaw_provider_rate_limited'
    ):
        return None

    completed_at = _parse_activity_datetime(
        pipeline_run.get('completed_at')
        or pipeline_run.get('updated_at')
        or pipeline_run.get('started_at')
    )
    if completed_at is None:
        return None
    maximum_retry_not_before = completed_at + timedelta(
        seconds=7 * 24 * 60 * 60
    )
    retry_not_before = _parse_activity_datetime(
        marker.get('retry_not_before')
    )
    retry_after_source = str(
        marker.get('retry_after_source') or ''
    ).strip()
    if retry_not_before is None:
        raw_retry_after_seconds = marker.get('retry_after_seconds')
        if (
            isinstance(raw_retry_after_seconds, (int, float))
            and not isinstance(raw_retry_after_seconds, bool)
            and math.isfinite(raw_retry_after_seconds)
            and raw_retry_after_seconds > 0
        ):
            retry_after_seconds = math.ceil(raw_retry_after_seconds)
            retry_after_source = (
                retry_after_source or 'persisted-retry-after-seconds'
            )
        else:
            retry_after_seconds = max(
                1,
                int(
                    getattr(
                        settings,
                        'openclaw_agent_rate_limit_fallback_cooldown_seconds',
                        900,
                    )
                    or 900
                ),
            )
            retry_after_source = 'configured-fallback'
        retry_after_seconds = min(
            retry_after_seconds,
            7 * 24 * 60 * 60,
        )
        retry_not_before = completed_at + timedelta(
            seconds=retry_after_seconds
        )
    elif retry_not_before > maximum_retry_not_before:
        retry_not_before = maximum_retry_not_before
        retry_after_source = 'persisted-deadline-capped-7d'

    remaining_seconds = max(
        0,
        math.ceil((retry_not_before - now).total_seconds()),
    )
    return {
        'active': remaining_seconds > 0,
        'retry_after_seconds': remaining_seconds,
        'retry_not_before': retry_not_before.isoformat(),
        'retry_after_source': retry_after_source or 'persisted-deadline',
    }


def _is_retryable_feedback_rearm_candidate(
    pipeline_run: object,
) -> bool:
    """Admit another bounded pass when only retryable feedback remains."""

    if not isinstance(pipeline_run, Mapping):
        return False
    run_metadata = (
        pipeline_run.get('run_metadata')
        if isinstance(pipeline_run.get('run_metadata'), Mapping)
        else {}
    )
    has_parent_contract = (
        'feedback_retry_parent_contract' in run_metadata
    )
    (
        circuit_marker_present,
        circuit_stage_id,
        shared_dispatch_circuit,
    ) = _feedback_dispatch_circuit_evidence(pipeline_run)
    if circuit_marker_present and circuit_stage_id is None:
        return False
    retry_stage_id = _feedback_retry_stage_id(pipeline_run)
    if (
        retry_stage_id == 'signal_decider'
        and _summary_cutoff_proof_from_run_metadata(run_metadata) is None
    ):
        return False
    stages = pipeline_run.get('stages')
    if (
        retry_stage_id == 'signal_decider'
        and not _is_exact_signal_decider_retry_suffix(pipeline_run)
    ):
        return False
    if not isinstance(stages, Mapping):
        return has_parent_contract
    stage = stages.get(retry_stage_id)
    if not isinstance(stage, Mapping):
        return has_parent_contract and retry_stage_id == 'agent_feedback_summaries'
    stage_status = str(stage.get('status') or '').strip().lower()
    if (
        has_parent_contract
        and retry_stage_id == 'agent_feedback_summaries'
        and not circuit_marker_present
    ):
        # Exact retry lineage is written before the retry starts. Preserve
        # that lane even when execution fails before emitting progress; an
        # explicit marker takes the bounded progress checks below instead.
        return stage_status != 'completed'
    if stage_status not in {'partial', 'failed'}:
        return False
    details = stage.get('details')
    result_summary = (
        details.get('result_summary')
        if isinstance(details, Mapping)
        else None
    )
    progress = (
        result_summary.get('progress')
        if isinstance(result_summary, Mapping)
        else None
    )
    prerequisites = (
        result_summary.get('prerequisites')
        if isinstance(result_summary, Mapping)
        else None
    )
    if not isinstance(prerequisites, Mapping) and isinstance(
        progress,
        Mapping,
    ):
        # Persisted stage results compact the retry service response under
        # ``progress``.  Accept that canonical stored shape as well as the
        # older top-level fixture shape, otherwise a valid partial parent is
        # incorrectly treated as non-retryable after process recovery.
        prerequisites = progress.get('prerequisites')
    inherited_admission = bool(
        run_metadata.get(
            'inherited_feedback_prerequisite_admission'
        )
        is True
        and _parse_activity_datetime(
            run_metadata.get(
                'inherited_feedback_knowledge_cutoff_at'
            )
        )
        is not None
        and str(
            run_metadata.get('inherited_feedback_source_run_id')
            or ''
        ).strip()
    )
    if not (
        (
            isinstance(prerequisites, Mapping)
            and prerequisites.get('ready') is True
        )
        or inherited_admission
    ):
        return False
    summary = (
        progress.get('summary')
        if isinstance(progress, Mapping)
        else None
    )
    role_progress = (
        summary.get('role_progress')
        if isinstance(summary, Mapping)
        else None
    )
    if not isinstance(role_progress, Mapping):
        return False
    if retry_stage_id == 'signal_decider':
        if not (
            role_progress
            and set(role_progress).issubset(
                DECISION_SUPPORT_AGENT_NAMES
            )
            and all(
                isinstance(role, Mapping)
                for role in role_progress.values()
            )
        ):
            return False
        if circuit_marker_present and not (
            isinstance(shared_dispatch_circuit, Mapping)
            and str(
                shared_dispatch_circuit.get('role') or ''
            ).strip()
            in DECISION_SUPPORT_AGENT_NAMES
            and str(
                shared_dispatch_circuit.get('role') or ''
            ).strip()
            in role_progress
        ):
            return False
        if not circuit_marker_present:
            failure_groups = summary.get('failure_groups')
            if failure_groups is not None and not isinstance(
                failure_groups,
                Mapping,
            ):
                return False
            try:
                visible_failures_are_retryable = all(
                    isinstance(group, Mapping)
                    and int(group.get('count') or 0) > 0
                    and int(group.get('retryable_count') or 0)
                    == int(group.get('count') or 0)
                    for group in (
                        failure_groups.values()
                        if isinstance(failure_groups, Mapping)
                        else ()
                    )
                )
            except (TypeError, ValueError):
                return False
            if not visible_failures_are_retryable:
                return False
    try:
        failed_items = int(summary.get('failed_items') or 0)
        retry_pending_items = sum(
            int(role.get('retry_pending_symbols') or 0)
            for role in role_progress.values()
            if isinstance(role, Mapping)
        )
        in_progress_items = sum(
            int(role.get('in_progress_symbols') or 0)
            for role in role_progress.values()
            if isinstance(role, Mapping)
        )
        pending_items = sum(
            int(role.get('pending_symbols') or 0)
            for role in role_progress.values()
            if isinstance(role, Mapping)
        )
    except (TypeError, ValueError):
        return False
    run_failed = (
        str(summary.get('run_status') or '').strip().lower()
        in {'partial', 'failed'}
    )
    terminal_retry_only = (
        failed_items > 0
        and retry_pending_items == failed_items
        and pending_items == 0
    )
    bounded_shared_outage_retry = (
        circuit_marker_present
        and _is_valid_retryable_feedback_dispatch_circuit_marker(
            shared_dispatch_circuit
        )
        and retry_pending_items > 0
        and retry_pending_items == failed_items + pending_items
    )
    return bool(
        run_failed
        and in_progress_items == 0
        and (
            (not circuit_marker_present and terminal_retry_only)
            or bounded_shared_outage_retry
        )
    )


def _feedback_retry_parent_contract(
    pipeline_run: object,
) -> dict[str, Any] | None:
    if not isinstance(pipeline_run, Mapping):
        return None
    run_metadata = pipeline_run.get('run_metadata')
    if not isinstance(run_metadata, Mapping):
        return None
    raw_contract = run_metadata.get('feedback_retry_parent_contract')
    if not isinstance(raw_contract, Mapping):
        return None
    source_run_id = str(
        raw_contract.get('source_run_id') or ''
    ).strip()
    knowledge_cutoff_at = _parse_activity_datetime(
        raw_contract.get('knowledge_cutoff_at')
    )
    feedback_scope_mode = str(
        raw_contract.get('feedback_scope_mode') or ''
    ).strip()
    persisted_source_run_id = str(
        run_metadata.get('inherited_feedback_source_run_id') or ''
    ).strip()
    persisted_cutoff_at = _parse_activity_datetime(
        run_metadata.get('inherited_feedback_knowledge_cutoff_at')
    )
    if not (
        str(raw_contract.get('schema_version') or '').strip()
        == FEEDBACK_RETRY_PARENT_CONTRACT_VERSION
        and source_run_id
        and knowledge_cutoff_at is not None
        and feedback_scope_mode
        in {
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        }
        and persisted_source_run_id == source_run_id
        and persisted_cutoff_at == knowledge_cutoff_at
    ):
        return None
    return {
        'schema_version': FEEDBACK_RETRY_PARENT_CONTRACT_VERSION,
        'source_run_id': source_run_id,
        'knowledge_cutoff_at': knowledge_cutoff_at,
        'feedback_scope_mode': feedback_scope_mode,
    }


def _retryable_feedback_rearm_source_run_id(
    pipeline_run: object,
) -> str | None:
    if not isinstance(pipeline_run, Mapping):
        return None
    stages = pipeline_run.get('stages')
    retry_stage_id = _feedback_retry_stage_id(pipeline_run)
    stage = (
        stages.get(retry_stage_id)
        if isinstance(stages, Mapping)
        else None
    )
    details = stage.get('details') if isinstance(stage, Mapping) else None
    result_summary = (
        details.get('result_summary')
        if isinstance(details, Mapping)
        else None
    )
    source_run_id = str(
        result_summary.get('run_id') or ''
        if isinstance(result_summary, Mapping)
        else ''
    ).strip()
    if source_run_id:
        return source_run_id
    run_metadata = (
        pipeline_run.get('run_metadata')
        if isinstance(pipeline_run.get('run_metadata'), Mapping)
        else {}
    )
    if 'feedback_retry_parent_contract' in run_metadata:
        parent_contract = _feedback_retry_parent_contract(pipeline_run)
        return (
            str(parent_contract.get('source_run_id') or '').strip()
            if isinstance(parent_contract, Mapping)
            else None
        )
    inherited_source_run_id = str(
        run_metadata.get('inherited_feedback_source_run_id') or ''
    ).strip()
    return inherited_source_run_id or None


def _retryable_feedback_rearm_config(
    pipeline_run: object,
    *,
    market_date: str,
) -> dict[str, Any] | None:
    """Recover the immutable retry inputs from the exact failed parent."""

    if not _is_retryable_feedback_rearm_candidate(pipeline_run):
        return None
    run_metadata = (
        pipeline_run.get('run_metadata')
        if isinstance(pipeline_run, Mapping)
        and isinstance(pipeline_run.get('run_metadata'), Mapping)
        else {}
    )
    parent_contract = _feedback_retry_parent_contract(pipeline_run)
    if (
        'feedback_retry_parent_contract' in run_metadata
        and parent_contract is None
    ):
        return None
    source_run_id = _retryable_feedback_rearm_source_run_id(pipeline_run)
    if not source_run_id:
        return None
    source_run = agent_context_feedback_run_state_service.get_run(
        run_id=source_run_id
    )
    request_payload = (
        source_run.get('request_payload')
        if isinstance(source_run, Mapping)
        else None
    )
    if not (
        isinstance(request_payload, Mapping)
        and _to_iso_date(source_run.get('market_date')) == market_date
        and str(source_run.get('status') or '').strip().lower()
        in {'partial', 'failed'}
        and str(request_payload.get('mode') or '').strip()
        == DAILY_RETRY_CYCLE_MODE
    ):
        return None
    if (
        isinstance(parent_contract, Mapping)
        and source_run_id != parent_contract['source_run_id']
        and str(
            request_payload.get('resumed_from_run_id') or ''
        ).strip()
        != parent_contract['source_run_id']
    ):
        return None

    raw_roles = request_payload.get('roles')
    roles = (
        tuple(str(role or '').strip() for role in raw_roles)
        if isinstance(raw_roles, list)
        else ()
    )

    def exact_symbols(value: object) -> list[str] | None:
        if not isinstance(value, list):
            return None
        normalized = _dedupe_symbol_codes(value)
        return normalized if normalized and normalized == value else None

    requested_symbols = exact_symbols(
        request_payload.get('requested_symbols')
    )
    raw_role_symbol_scopes = request_payload.get('role_symbol_scopes')
    role_symbol_scopes = (
        {
            role: exact_symbols(raw_role_symbol_scopes.get(role))
            for role in roles
        }
        if isinstance(raw_role_symbol_scopes, Mapping)
        else {}
    )

    feedback_scope_mode = str(
        request_payload.get('feedback_scope_mode') or ''
    ).strip()
    knowledge_cutoff_at = _parse_activity_datetime(
        request_payload.get('knowledge_cutoff_at')
    )
    admission_cutoff_at = _parse_activity_datetime(
        request_payload.get('admission_knowledge_cutoff_at')
    )
    fs_div = str(request_payload.get('fs_div') or '').strip().upper()
    raw_policy_context_as_of_date = request_payload.get(
        'policy_context_as_of_date'
    )
    try:
        policy_context_as_of_date = (
            raw_policy_context_as_of_date.date()
            if isinstance(raw_policy_context_as_of_date, datetime)
            else raw_policy_context_as_of_date
            if isinstance(raw_policy_context_as_of_date, date)
            else date.fromisoformat(
                str(raw_policy_context_as_of_date or '').strip()
            )
        )
    except ValueError:
        return None
    raw_scope_authority = request_payload.get('scope_authority')
    try:
        outer_batch_size = int(
            request_payload.get('outer_batch_size') or 0
        )
    except (TypeError, ValueError):
        return None
    if isinstance(parent_contract, Mapping) and not (
        parent_contract['knowledge_cutoff_at'] == knowledge_cutoff_at
        and parent_contract['feedback_scope_mode'] == feedback_scope_mode
    ):
        return None
    if not (
        roles == tuple(SUMMARIZER_AGENT_NAMES)
        and requested_symbols
        and isinstance(raw_role_symbol_scopes, Mapping)
        and set(raw_role_symbol_scopes) == set(roles)
        and all(role_symbol_scopes.values())
        and all(
            set(scope).issubset(requested_symbols)
            for scope in role_symbol_scopes.values()
        )
        and feedback_scope_mode in {
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        }
        and knowledge_cutoff_at is not None
        and (
            admission_cutoff_at is None
            or admission_cutoff_at == knowledge_cutoff_at
        )
        and fs_div in {'CFS', 'OFS'}
        and policy_context_as_of_date >= date.fromisoformat(market_date)
        and (
            raw_scope_authority is None
            or isinstance(raw_scope_authority, Mapping)
        )
        and outer_batch_size > 0
    ):
        return None

    idempotency_key = str(
        request_payload.get('idempotency_key') or ''
    ).strip() or None
    return {
        'source_run_id': source_run_id,
        'roles': list(roles),
        'requested_symbols': list(requested_symbols),
        'role_symbol_scopes': {
            role: list(scope)
            for role, scope in role_symbol_scopes.items()
        },
        'knowledge_cutoff_at': knowledge_cutoff_at,
        'feedback_scope_mode': feedback_scope_mode,
        'fs_div': fs_div,
        'policy_context_as_of_date': policy_context_as_of_date,
        'scope_authority': (
            deepcopy(dict(raw_scope_authority))
            if raw_scope_authority is not None
            else None
        ),
        'idempotency_key': idempotency_key,
        'outer_batch_size': outer_batch_size,
    }


def _retryable_feedback_rearm_knowledge_cutoff(
    pipeline_run: object,
    *,
    market_date: str,
) -> datetime | None:
    """Resolve the exact cutoff that made the parent feedback reusable."""

    config = _retryable_feedback_rearm_config(
        pipeline_run,
        market_date=market_date,
    )
    return (
        config['knowledge_cutoff_at']
        if isinstance(config, Mapping)
        else None
    )


def _retryable_feedback_rearm_max_cycles(
    pipeline_run: object,
) -> int | None:
    """Permit exactly one cycle beyond the persisted partial parent."""

    if not _is_retryable_feedback_rearm_candidate(pipeline_run):
        return None
    if not isinstance(pipeline_run, Mapping):
        return None
    stages = pipeline_run.get('stages')
    retry_stage_id = _feedback_retry_stage_id(pipeline_run)
    stage = (
        stages.get(retry_stage_id)
        if isinstance(stages, Mapping)
        else None
    )
    details = stage.get('details') if isinstance(stage, Mapping) else None
    result_summary = (
        details.get('result_summary')
        if isinstance(details, Mapping)
        else None
    )
    progress = (
        result_summary.get('progress')
        if isinstance(result_summary, Mapping)
        else None
    )
    summary = (
        progress.get('summary')
        if isinstance(progress, Mapping)
        else None
    )
    if not isinstance(summary, Mapping):
        parent_contract = _feedback_retry_parent_contract(pipeline_run)
        run_metadata = pipeline_run.get('run_metadata')
        if not (
            isinstance(parent_contract, Mapping)
            and isinstance(run_metadata, Mapping)
        ):
            return None
        try:
            persisted_max_cycles = int(
                run_metadata.get('feedback_max_cycles')
            )
        except (TypeError, ValueError):
            return None
        return persisted_max_cycles if persisted_max_cycles > 0 else None
    if not {'completed_cycles', 'cycle_number'}.issubset(summary):
        return None
    try:
        completed_cycles = int(summary['completed_cycles'])
        cycle_number = int(summary['cycle_number'])
    except (TypeError, ValueError):
        return None
    if (
        completed_cycles < 0
        or cycle_number < 1
        or cycle_number
        not in {completed_cycles, completed_cycles + 1}
    ):
        return None
    return max(completed_cycles + 1, cycle_number)


def _feedback_suffix_has_stale_prerequisite_ordering(
    *,
    stage_rows: Mapping[str, Any],
    point_in_time_recovery: bool,
) -> bool:
    """Detect terminal feedback artifacts produced before required inputs."""

    downstream_boundaries: list[datetime] = []
    for stage_id in ('agent_feedback_summaries', 'signal_decider'):
        stage = stage_rows.get(stage_id)
        if not isinstance(stage, Mapping):
            continue
        boundary = (
            _parse_activity_datetime(stage.get('started_at'))
            or _parse_activity_datetime(stage.get('completed_at'))
            or _parse_activity_datetime(stage.get('updated_at'))
        )
        if boundary is not None:
            downstream_boundaries.append(boundary)
    if not downstream_boundaries:
        return False

    prerequisite_stage_ids = (
        FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
        if point_in_time_recovery
        else (
            *CURRENT_FEEDBACK_ORDERING_PREREQUISITE_STAGES,
            *ORDERING_ONLY_OPTIONAL_CONTEXT_STAGE_IDS,
        )
    )
    downstream_started_at = min(downstream_boundaries)
    for stage_id in prerequisite_stage_ids:
        stage = stage_rows.get(stage_id)
        if not isinstance(stage, Mapping):
            continue
        prerequisite_completed_at = (
            _parse_activity_datetime(stage.get('completed_at'))
            or _parse_activity_datetime(stage.get('updated_at'))
        )
        if (
            prerequisite_completed_at is not None
            and prerequisite_completed_at > downstream_started_at
        ):
            return True
    return False


def _signal_features_have_stale_prerequisite_ordering(
    *,
    stage_rows: Mapping[str, Any],
    point_in_time_recovery: bool,
) -> bool:
    if point_in_time_recovery:
        return False
    signal_features = stage_rows.get('signal_features')
    if not isinstance(signal_features, Mapping):
        return False
    signal_started_at = (
        _parse_activity_datetime(signal_features.get('started_at'))
        or _parse_activity_datetime(signal_features.get('completed_at'))
        or _parse_activity_datetime(signal_features.get('updated_at'))
    )
    if signal_started_at is None:
        return False
    for stage_id in FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER:
        stage = stage_rows.get(stage_id)
        if not isinstance(stage, Mapping):
            continue
        prerequisite_completed_at = (
            _parse_activity_datetime(stage.get('completed_at'))
            or _parse_activity_datetime(stage.get('updated_at'))
        )
        if (
            prerequisite_completed_at is not None
            and prerequisite_completed_at > signal_started_at
        ):
            return True
    return False


def _update_run_stage_metadata(
    metadata: dict | None,
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
    extra: dict | None = None,
) -> dict:
    updated = dict(metadata or {})
    now = _utcnow_iso()

    stage_progress = dict(updated.get('stage_progress') or {})
    entry = dict(stage_progress.get(stage) or {})
    entry.setdefault('started_at', now)
    entry['updated_at'] = now
    entry['status'] = status
    entry['current_step'] = current_step
    entry['current_activity'] = current_activity

    if processed is not None:
        entry['processed'] = int(processed)
    if total is not None:
        entry['total'] = int(total)
    if succeeded is not None:
        entry['succeeded'] = int(succeeded)
    if failed is not None:
        entry['failed'] = int(failed)
    if current_symbol is not None:
        entry['current_symbol'] = current_symbol
    else:
        entry.pop('current_symbol', None)
    if current_period is not None:
        entry['current_period'] = current_period
    else:
        entry.pop('current_period', None)
    if extra:
        entry.update(extra)
    if status in {'completed', 'partial', 'failed'}:
        entry['completed_at'] = now

    stage_progress[stage] = entry
    updated['stage'] = stage
    updated['current_step'] = current_step
    updated['current_activity'] = current_activity
    updated['last_progress_at'] = now
    updated['stage_progress'] = stage_progress

    if current_symbol is not None:
        updated['current_symbol'] = current_symbol
    else:
        updated.pop('current_symbol', None)
    if current_period is not None:
        updated['current_period'] = current_period
    else:
        updated.pop('current_period', None)

    return updated


class PykrxBackfillService:
    symbol_pause_seconds = 0.02
    name_pause_seconds = 0.005
    max_symbol_fundamental_failures_per_run = 3

    def refresh_recent_universe(
        self,
        *,
        markets: Sequence[str] = DEFAULT_MARKETS,
        limit: int | None = None,
        deactivate_missing: bool = True,
    ) -> dict:
        _log_pykrx_fetch(
            'refresh_recent_universe',
            'started',
            markets=list(markets),
            limit=limit,
            deactivate_missing=deactivate_missing,
        )
        snapshot = self.fetch_recent_universe_snapshot(markets=markets, limit=limit)
        if not snapshot.get('complete', True):
            _log_pykrx_fetch(
                'refresh_recent_universe',
                'failed',
                markets=list(markets),
                limit=limit,
                complete=False,
                market_status=dict(snapshot.get('market_status') or {}),
                unavailable_markets=list(snapshot.get('unavailable_markets') or []),
                market_errors=list(snapshot.get('market_errors') or []),
            )
            raise RuntimeError(
                'PyKRX universe snapshot is incomplete for requested markets: '
                f"{','.join(snapshot['unavailable_markets'])}."
            )
        symbols = snapshot['symbols']
        persisted = self._persist_universe(symbols, deactivate_missing=deactivate_missing)
        result = {
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
        }
        _log_pykrx_fetch('refresh_recent_universe', 'completed', **result)
        return result

    def _persist_universe(self, symbols: Sequence[PykrxSymbol], *, deactivate_missing: bool) -> int:
        return upsert_symbol_universe(
            symbols,
            source='pykrx',
            stale_sources=['pykrx', 'kis_master'] if deactivate_missing else [],
            stale_markets=list({item.market for item in symbols}),
        )

    def fetch_recent_universe(
        self,
        *,
        markets: Sequence[str] = DEFAULT_MARKETS,
        limit: int | None = None,
    ) -> list[PykrxSymbol]:
        snapshot = self.fetch_recent_universe_snapshot(markets=markets, limit=limit)
        if not snapshot.get('complete', True):
            raise RuntimeError(
                'PyKRX universe snapshot is incomplete for requested markets: '
                f"{','.join(snapshot['unavailable_markets'])}."
            )
        return snapshot['symbols']

    def fetch_recent_universe_snapshot(
        self,
        *,
        markets: Sequence[str] = DEFAULT_MARKETS,
        limit: int | None = None,
        end_date: str | None = None,
        allow_master_fallback: bool = True,
    ) -> dict:
        latest_trade_date = self.resolve_latest_trade_date(end_date=end_date)
        as_of_date = latest_trade_date.replace('-', '')
        pykrx_snapshot = self._fetch_pykrx_market_universe(
            as_of_date=as_of_date,
            markets=markets,
            limit=limit,
            allow_master_fallback=allow_master_fallback,
        )
        items = pykrx_snapshot['symbols']
        universe_source = pykrx_snapshot['universe_source']
        if not items and allow_master_fallback:
            items = self._fetch_master_fallback_universe(markets=markets, limit=limit)
            universe_source = 'kis_master_fallback'
            requested_markets = [str(market).upper() for market in markets]
            covered_markets = {item.market.upper() for item in items}
            market_status = dict(pykrx_snapshot['market_status'])
            for market in requested_markets:
                if market in covered_markets:
                    market_status[market] = 'kis_master_fallback'
            unavailable_markets = [
                market for market in requested_markets if market not in covered_markets
            ]
        else:
            market_status = dict(pykrx_snapshot['market_status'])
            unavailable_markets = list(pykrx_snapshot['unavailable_markets'])
        return {
            'symbols': items,
            'latest_trade_date': latest_trade_date,
            'universe_source': universe_source,
            'membership_basis': 'end_date_snapshot',
            'membership_scope_warning': (
                'This snapshot represents membership at the resolved end date; '
                'it does not reconstruct membership for every date in a multi-day backfill.'
            ),
            'complete': not unavailable_markets,
            'market_status': market_status,
            'unavailable_markets': unavailable_markets,
            'market_errors': list(pykrx_snapshot['market_errors']),
        }

    def _fetch_pykrx_market_universe(
        self,
        *,
        as_of_date: str,
        markets: Sequence[str],
        limit: int | None = None,
        allow_master_fallback: bool = True,
    ) -> dict:
        items: list[PykrxSymbol] = []
        used_master_fallback = False
        market_status: dict[str, str] = {}
        market_errors: list[dict[str, str]] = []

        def build_result() -> dict:
            for requested_market in markets:
                normalized_requested_market = str(requested_market).upper()
                if normalized_requested_market in market_status:
                    continue
                market_status[normalized_requested_market] = (
                    'not_queried_due_to_limit'
                    if limit and len(items) >= limit
                    else 'unavailable'
                )
            unavailable_markets = [
                str(market).upper()
                for market in markets
                if market_status.get(str(market).upper()) == 'unavailable'
            ]
            return {
                'symbols': items,
                'universe_source': (
                    'pykrx_market_ohlcv_with_master_fallback'
                    if used_master_fallback
                    else 'pykrx_market_ohlcv'
                ),
                'complete': not unavailable_markets,
                'market_status': market_status,
                'unavailable_markets': unavailable_markets,
                'market_errors': market_errors,
            }

        for market in markets:
            normalized_market = str(market).upper()
            _log_pykrx_fetch('fetch_market_universe', 'started', market=market, as_of_date=as_of_date, limit=limit)
            try:
                frame = _call_pykrx(stock.get_market_ohlcv_by_ticker, as_of_date, market=market)
            except Exception as exc:
                logger.warning('pykrx market universe fetch failed for %s on %s: %s', market, as_of_date, exc)
                _log_pykrx_fetch(
                    'fetch_market_universe',
                    'failed',
                    market=market,
                    as_of_date=as_of_date,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    fallback_source='kis_master' if allow_master_fallback else None,
                )
                fallback_items = (
                    self._fetch_master_fallback_for_market(
                        market=market,
                        as_of_date=as_of_date,
                        current_count=len(items),
                        limit=limit,
                    )
                    if allow_master_fallback
                    else []
                )
                used_master_fallback = used_master_fallback or bool(fallback_items)
                market_status[normalized_market] = (
                    'kis_master_fallback' if fallback_items else 'unavailable'
                )
                market_errors.append(
                    {
                        'market': normalized_market,
                        'status': 'fetch_failed',
                        'error_type': type(exc).__name__,
                        'error_message': str(exc),
                    }
                )
                items.extend(fallback_items)
                if limit and len(items) >= limit:
                    break
                continue
            tickers = self._extract_market_universe_tickers(frame)
            if not tickers:
                logger.warning('pykrx market universe returned no usable tickers for %s on %s', market, as_of_date)
                _log_pykrx_fetch(
                    'fetch_market_universe',
                    'empty',
                    market=market,
                    as_of_date=as_of_date,
                    fallback_source='kis_master' if allow_master_fallback else None,
                )
                fallback_items = (
                    self._fetch_master_fallback_for_market(
                        market=market,
                        as_of_date=as_of_date,
                        current_count=len(items),
                        limit=limit,
                    )
                    if allow_master_fallback
                    else []
                )
                used_master_fallback = used_master_fallback or bool(fallback_items)
                market_status[normalized_market] = (
                    'kis_master_fallback' if fallback_items else 'unavailable'
                )
                market_errors.append(
                    {
                        'market': normalized_market,
                        'status': 'empty',
                        'error_type': 'EmptyUniverse',
                        'error_message': f'No usable tickers returned for {normalized_market} on {as_of_date}.',
                    }
                )
                items.extend(fallback_items)
                if limit and len(items) >= limit:
                    break
                continue
            market_status[normalized_market] = 'pykrx_market_ohlcv'
            for ticker in tickers:
                try:
                    raw_name_ko = _call_pykrx(stock.get_market_ticker_name, ticker)
                except Exception:
                    logger.warning('Falling back to ticker as name for %s', ticker)
                    name_ko = ticker
                else:
                    name_ko = _coerce_pykrx_ticker_name(raw_name_ko, ticker=ticker)
                items.append(
                    PykrxSymbol(
                        symbol=ticker,
                        market=market,
                        name_ko=name_ko,
                    )
                )
                if self.name_pause_seconds:
                    time.sleep(self.name_pause_seconds)
                if limit and len(items) >= limit:
                    _log_pykrx_fetch(
                        'fetch_market_universe',
                        'completed',
                        market=market,
                        as_of_date=as_of_date,
                        ticker_count=len(tickers),
                        collected_count=len(items),
                        limit=limit,
                        fallback_used=used_master_fallback,
                    )
                    return build_result()
            _log_pykrx_fetch(
                'fetch_market_universe',
                'completed',
                market=market,
                as_of_date=as_of_date,
                ticker_count=len(tickers),
                collected_count=len(items),
                limit=limit,
                fallback_used=used_master_fallback,
            )
        return build_result()

    def _extract_market_universe_tickers(self, frame) -> list[str]:
        if getattr(frame, 'empty', True):
            return []
        index = getattr(frame, 'index', None)
        if index is None:
            return []
        raw_tickers = index.tolist() if hasattr(index, 'tolist') else list(index)
        items: list[str] = []
        for ticker in raw_tickers:
            normalized = normalize_krx_short_code(ticker)
            if normalized is None:
                continue
            items.append(normalized)
        return items

    def _fetch_master_fallback_for_market(
        self,
        *,
        market: str,
        as_of_date: str,
        current_count: int,
        limit: int | None,
    ) -> list[PykrxSymbol]:
        remaining = None if limit is None else max(limit - current_count, 0)
        if remaining == 0:
            return []
        logger.warning('Falling back to KIS master universe for %s on %s', market, as_of_date)
        return self._fetch_master_fallback_universe(markets=(market,), limit=remaining)

    def _fetch_master_fallback_universe(
        self,
        *,
        markets: Sequence[str],
        limit: int | None = None,
    ) -> list[PykrxSymbol]:
        allowed_markets = {market.upper() for market in markets}
        items: list[PykrxSymbol] = []
        for entry in get_master_symbols():
            if entry.market.upper() not in allowed_markets:
                continue
            symbol_code = normalize_krx_short_code(entry.symbol)
            if symbol_code is None:
                continue
            items.append(
                PykrxSymbol(
                    symbol=symbol_code,
                    market=entry.market,
                    name_ko=entry.name_ko,
                    standard_code=entry.standard_code,
                )
            )
            if limit and len(items) >= limit:
                _log_pykrx_fetch(
                    'fetch_master_fallback_universe',
                    'completed',
                    markets=list(markets),
                    symbol_count=len(items),
                    limit=limit,
                )
                return items
        _log_pykrx_fetch(
            'fetch_master_fallback_universe',
            'completed',
            markets=list(markets),
            symbol_count=len(items),
            limit=limit,
        )
        return items

    def load_active_universe(
        self,
        *,
        markets: Sequence[str] = DEFAULT_MARKETS,
        symbol: str | None = None,
        limit: int | None = None,
    ) -> list[PykrxSymbol]:
        rows = load_symbols(is_active=True, markets=list(markets), limit=limit)
        items: list[PykrxSymbol] = []
        seen: set[str] = set()
        allowed_markets = {market.upper() for market in markets}
        for row in rows:
            symbol_code = normalize_krx_short_code(row['symbol'])
            market = str(row['market'] or '').strip()
            if symbol_code is None:
                continue
            if market.upper() not in allowed_markets:
                continue
            if symbol_code in seen:
                continue
            seen.add(symbol_code)
            items.append(
                PykrxSymbol(
                    symbol=symbol_code,
                    market=market,
                    name_ko=row['name_ko'],
                    standard_code=row['standard_code'],
                )
            )
        if symbol:
            items = [item for item in items if item.symbol == symbol]
        return items

    def find_symbol(self, symbol: str, *, markets: Sequence[str] = DEFAULT_MARKETS) -> PykrxSymbol | None:
        target = symbol.strip()
        for entry in get_master_symbols():
            if entry.symbol != target:
                continue
            if entry.market.upper() not in {market.upper() for market in markets}:
                return None
            return PykrxSymbol(
                symbol=entry.symbol,
                market=entry.market,
                name_ko=entry.name_ko,
                standard_code=entry.standard_code,
            )
        return None

    def resolve_latest_trade_date(
        self,
        *,
        end_date: str | None = None,
        probe_symbol: str = '005930',
    ) -> str:
        return _to_iso_date(datetime.strptime(self._resolve_reference_trade_date(end_date=end_date, probe_symbol=probe_symbol), '%Y%m%d'))

    def _resolve_reference_trade_date(
        self,
        *,
        end_date: str | None = None,
        probe_symbol: str = '005930',
        lookback_days: int = 730,
    ) -> str:
        normalized_end = _normalize_yyyymmdd(
            end_date,
            default=_current_krx_calendar_date().strftime('%Y%m%d'),
        )
        end_dt = datetime.strptime(normalized_end, '%Y%m%d')
        start_dt = end_dt - timedelta(days=lookback_days)
        try:
            frame = _get_market_ohlcv_by_date(start_dt.strftime('%Y%m%d'), normalized_end, probe_symbol)
            if frame.empty:
                raise RuntimeError(f'Unable to resolve a recent pykrx trading day on or before {normalized_end}.')
            return frame.index.max().strftime('%Y%m%d')
        except Exception as exc:
            fallback_date = latest_krx_trading_date(end_dt.date(), allow_fallback=True)
            if fallback_date is None:
                raise
            logger.warning(
                'Falling back to KRX calendar latest trade date for %s after PyKRX reference lookup failed: %s',
                normalized_end,
                exc,
            )
            return fallback_date.strftime('%Y%m%d')

    def run_backfill(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        markets: Sequence[str] = DEFAULT_MARKETS,
        trigger: str = 'manual',
        refresh_universe: bool = True,
        limit: int | None = None,
        symbol: str | None = None,
        selected_stages: Sequence[str] | None = None,
    ) -> dict:
        normalized_end = (
            _normalize_yyyymmdd(end_date)
            if end_date is not None
            else self.resolve_latest_trade_date().replace('-', '')
        )
        validate_finalized_krx_write_date(normalized_end)
        normalized_start = _normalize_yyyymmdd(start_date, default=normalized_end)
        if normalized_start != normalized_end:
            raise ValueError(
                'historical KRX backfill requires one exact market date per run'
            )
        market_date_iso = _normalize_iso_date(normalized_end)
        with advisory_eod_collection_lock(
            market='KRX',
            trade_date=market_date_iso,
            source='daily_price_bars',
        ) as lock_acquired:
            if not lock_acquired:
                raise RuntimeError(
                    'daily price-bar writer lock is already held for '
                    f'{market_date_iso}'
                )
            return self._run_market_data_pipeline(
                start_date=normalized_start,
                end_date=normalized_end,
                markets=markets,
                trigger=trigger,
                refresh_universe=refresh_universe,
                limit=limit,
                symbol=symbol,
                mode='historical_backfill',
                selected_stages=(
                    tuple(selected_stages)
                    if selected_stages is not None
                    else HISTORICAL_BACKFILL_STAGE_ORDER
                ),
                chain_fundamentals_followup=False,
            )

    @staticmethod
    def _pipeline_run_matches_eod_run(
        *,
        pipeline_run: Mapping[str, Any],
        eod_run: Mapping[str, Any],
    ) -> bool:
        try:
            eod_run_id = int(eod_run['id'])
        except (KeyError, TypeError, ValueError):
            return False

        pipeline_metadata = (
            pipeline_run.get('run_metadata')
            if isinstance(pipeline_run.get('run_metadata'), Mapping)
            else {}
        )
        explicit_eod_ids: list[int] = []
        for key in (
            'eod_collection_run_id',
            'reused_eod_collection_run_id',
        ):
            raw_value = pipeline_metadata.get(key)
            if raw_value is None:
                continue
            try:
                explicit_eod_ids.append(int(raw_value))
            except (TypeError, ValueError):
                return False

        stages = (
            pipeline_run.get('stages')
            if isinstance(pipeline_run.get('stages'), Mapping)
            else {}
        )
        for raw_stage in stages.values():
            if not isinstance(raw_stage, Mapping):
                continue
            source_run_ids = raw_stage.get('source_run_ids')
            if not isinstance(source_run_ids, Mapping):
                continue
            raw_value = source_run_ids.get('eod_run_id')
            if raw_value is None:
                continue
            try:
                explicit_eod_ids.append(int(raw_value))
            except (TypeError, ValueError):
                return False

        if explicit_eod_ids:
            return set(explicit_eod_ids) == {eod_run_id}

        eod_metadata = (
            eod_run.get('run_metadata')
            if isinstance(eod_run.get('run_metadata'), Mapping)
            else {}
        )
        raw_pipeline_run_id = eod_metadata.get('daily_pipeline_run_id')
        if raw_pipeline_run_id is None:
            return False
        try:
            return int(raw_pipeline_run_id) == int(pipeline_run['id'])
        except (KeyError, TypeError, ValueError):
            return False

    @staticmethod
    def _build_untracked_followup_recovery_parent(
        *,
        latest_eod_run: Mapping[str, Any],
    ) -> dict[str, Any]:
        metadata = (
            latest_eod_run.get('run_metadata')
            if isinstance(latest_eod_run.get('run_metadata'), Mapping)
            else {}
        )
        stage_progress = (
            metadata.get('stage_progress')
            if isinstance(metadata.get('stage_progress'), Mapping)
            else {}
        )
        all_resumable_stages = tuple(
            stage_id
            for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
            if stage_id != 'price_collection'
        )
        expected_stage_ids = {
            str(raw_stage_id).strip()
            for raw_stage_id in (
                metadata.get('expected_daily_pipeline_stages') or []
            )
        }
        expected_stages = tuple(
            stage_id
            for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
            if stage_id != 'price_collection'
            and stage_id in expected_stage_ids
        )
        resumable_stage_order = expected_stages or all_resumable_stages
        has_downstream_scope = bool(
            _dedupe_symbol_codes(
                metadata.get('downstream_retry_symbols') or []
            )
        )
        incomplete_indexes = [
            index
            for index, stage_id in enumerate(resumable_stage_order)
            if (
                expected_stages
                or isinstance(stage_progress.get(stage_id), Mapping)
            )
            and not daily_pipeline_outcome_is_satisfied(
                stage_progress.get(stage_id)
                if isinstance(stage_progress.get(stage_id), Mapping)
                else {}
            )
        ]
        if has_downstream_scope:
            durable_resume_stage = str(
                metadata.get('downstream_retry_resume_from_stage') or ''
            ).strip()
            if durable_resume_stage not in resumable_stage_order:
                durable_resume_stage = next(
                    (
                        stage_id
                        for stage_id in (
                            'pykrx_enrichment',
                            'technical_snapshots',
                        )
                        if stage_id in resumable_stage_order
                    ),
                    resumable_stage_order[0],
                )
            resume_index = resumable_stage_order.index(
                durable_resume_stage
            )
        elif not incomplete_indexes:
            resume_index = (
                resumable_stage_order.index('technical_snapshots')
                if 'technical_snapshots' in resumable_stage_order
                else 0
            )
        else:
            resume_index = min(incomplete_indexes)
        resolved_stages = list(resumable_stage_order[resume_index:])
        resume_stage = resolved_stages[0]
        stages: dict[str, dict[str, Any]] = {}
        for stage_id in resolved_stages:
            raw_progress = stage_progress.get(stage_id)
            stage_status = (
                str(raw_progress.get('status') or '').strip().lower()
                if isinstance(raw_progress, Mapping)
                else 'pending'
            )
            stages[stage_id] = {
                'status': (
                    'interrupted'
                    if stage_id == resume_stage
                    else stage_status or 'pending'
                )
            }
        return {
            'id': None,
            'trigger': str(
                latest_eod_run.get('trigger') or 'auto_close_scheduler'
            ),
            'status': 'interrupted',
            'run_metadata': {
                'resolved_stages': resolved_stages,
                'synthetic_recovery_parent': True,
                'reused_eod_collection_run_id': latest_eod_run.get('id'),
            },
            'stages': stages,
        }

    def _resolve_frozen_daily_membership_from_retry_lineage(
        self,
        *,
        market_date: str,
        metadata: Mapping[str, Any],
        markets: Sequence[str],
        eod_run_id: int | None = None,
    ) -> tuple[set[str], dict[str, Any], tuple[str, ...]] | None:
        """Resolve canonical exact-date evidence only from one retry lineage."""

        source_markets = _point_in_time_source_markets(
            metadata,
            fallback=markets,
        )
        current_evidence = _validated_frozen_daily_membership_evidence(
            metadata,
            markets=source_markets,
            expected_market_date=market_date,
        )
        if current_evidence:
            membership_symbols, universe_snapshot = current_evidence
            return membership_symbols, universe_snapshot, source_markets

        lineage_metadata = metadata
        visited_run_ids = (
            {int(eod_run_id)} if eod_run_id is not None else set()
        )
        max_lineage_depth = MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE + 1
        for _ in range(max_lineage_depth):
            retry_context = (
                lineage_metadata.get('retry')
                if isinstance(lineage_metadata.get('retry'), Mapping)
                else {}
            )
            try:
                parent_run_id = int(retry_context.get('retry_of_run_id'))
            except (TypeError, ValueError):
                break
            if parent_run_id <= 0 or parent_run_id in visited_run_ids:
                break
            visited_run_ids.add(parent_run_id)
            try:
                parent_run = load_automatic_daily_eod_collection_run_by_id(
                    eod_run_id=parent_run_id,
                    trade_date=market_date,
                )
            except Exception as exc:  # pragma: no cover - defensive guard
                logger.warning(
                    'Unable to inspect PyKRX retry ancestor %s for %s: %s',
                    parent_run_id,
                    market_date,
                    exc,
                )
                break
            if not isinstance(parent_run, Mapping):
                break
            parent_metadata = (
                parent_run.get('run_metadata')
                if isinstance(parent_run.get('run_metadata'), Mapping)
                else {}
            )
            parent_markets = _point_in_time_source_markets(
                parent_metadata,
                fallback=source_markets,
            )
            ancestor_evidence = _validated_frozen_daily_membership_evidence(
                {
                    **dict(metadata),
                    'markets': list(parent_markets),
                    'universe_snapshot': parent_metadata.get(
                        'universe_snapshot'
                    ),
                },
                markets=parent_markets,
                expected_market_date=market_date,
            )
            if ancestor_evidence:
                membership_symbols, universe_snapshot = ancestor_evidence
                logger.info(
                    'Recovered exact-date PyKRX membership for %s from retry ancestor %s.',
                    market_date,
                    parent_run_id,
                )
                return (
                    membership_symbols,
                    universe_snapshot,
                    parent_markets,
                )
            lineage_metadata = parent_metadata
        return None

    def _resolve_point_in_time_followup_membership(
        self,
        *,
        market_date: str,
        metadata: Mapping[str, Any],
        markets: Sequence[str],
        eod_run_id: int | None = None,
    ) -> tuple[set[str] | None, tuple[str, ...]]:
        """Return exact-date membership from the run lineage or PyKRX."""

        source_markets = _point_in_time_source_markets(
            metadata,
            fallback=markets,
        )
        membership_symbols = _validated_point_in_time_membership_symbols(
            metadata,
            markets=source_markets,
            expected_market_date=market_date,
        )
        if membership_symbols:
            return membership_symbols, source_markets

        lineage_evidence = (
            self._resolve_frozen_daily_membership_from_retry_lineage(
                market_date=market_date,
                metadata=metadata,
                markets=source_markets,
                eod_run_id=eod_run_id,
            )
        )
        if lineage_evidence:
            membership_symbols, _, source_markets = lineage_evidence
            return membership_symbols, source_markets

        try:
            provider_snapshot = self.fetch_recent_universe_snapshot(
                markets=source_markets,
                limit=None,
                end_date=market_date.replace('-', ''),
                allow_master_fallback=False,
            )
        except Exception as exc:
            logger.warning(
                'Unable to refetch exact-date PyKRX membership for %s: %s',
                market_date,
                exc,
            )
            return None, source_markets

        universe_snapshot = _build_point_in_time_universe_snapshot(
            provider_snapshot,
            markets=source_markets,
            expected_market_date=market_date,
        )
        if universe_snapshot is None:
            return None, source_markets
        refetched_metadata = {
            **dict(metadata),
            'markets': list(source_markets),
            'universe_snapshot': universe_snapshot,
        }
        return (
            _validated_point_in_time_membership_symbols(
                refetched_metadata,
                markets=source_markets,
                expected_market_date=market_date,
            ),
            source_markets,
        )

    @staticmethod
    def _record_failed_followup_scope_attempt(
        *,
        market_date: str,
        latest_available_trade_date: str,
        latest_eod_run: Mapping[str, Any],
        markets: Sequence[str],
        recovery_stages: Sequence[str],
        resume_from_stage: str,
        recovery_attempts: int,
        requested_symbols: Sequence[str],
        point_in_time_recovery: bool,
        failure_reason: str,
        provider_admission_fence_owned: bool = False,
    ) -> dict[str, Any]:
        """Persist a bounded attempt when follow-up scope is unprovable."""

        error_message = (
            'Exact-date PyKRX membership could not prove the follow-up '
            'symbol scope.'
        )
        started_run = daily_pipeline_run_state_service.start_run(
            market_date=market_date,
            trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            requested_stages=list(recovery_stages),
            resolved_stages=list(recovery_stages),
            run_metadata={
                'entrypoint': 'daily_pipeline_stage_service.run',
                'reused_eod_collection_run_id': int(latest_eod_run['id']),
                'point_in_time_recovery': point_in_time_recovery,
                'resume_from_stage': resume_from_stage,
                'markets': list(markets),
                'requested_symbols': _dedupe_symbol_codes(
                    requested_symbols
                ),
                'failure_reason': failure_reason,
            },
            provider_admission_fence_owned=(
                provider_admission_fence_owned
            ),
        )
        if started_run.get('deduplicated'):
            result = build_daily_skip_result(
                reason='followup_recovery_already_running',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'pipeline_run_id': started_run.get('id'),
                'resume_from_stage': resume_from_stage,
                'deduplicated': True,
            }
            return result

        pipeline_run_id = int(started_run['id'])
        daily_pipeline_run_state_service.finish_run(
            pipeline_run_id=pipeline_run_id,
            status='failed',
            error_message=error_message,
            run_metadata={'failure_reason': failure_reason},
        )
        result = build_daily_skip_result(
            reason=failure_reason,
            market_date=market_date,
            latest_available_trade_date=latest_available_trade_date,
            existing_run=latest_eod_run,
        )
        result.update(
            {
                'triggered': True,
                'status': 'failed',
                'reason': failure_reason,
            }
        )
        result['followup_recovery'] = {
            'status': 'failed',
            'reason': 'invalid_or_missing_price_scope',
            'attempt': recovery_attempts + 1,
            'max_attempts': MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE,
            'pipeline_run_id': pipeline_run_id,
            'reused_eod_collection_run_id': latest_eod_run.get('id'),
            'resume_from_stage': resume_from_stage,
            'stages': list(recovery_stages),
            'requested_symbols': _dedupe_symbol_codes(requested_symbols),
        }
        return result

    @staticmethod
    def _converge_point_in_time_price_complete_run(
        *,
        latest_eod_run: Mapping[str, Any],
        market_date: str,
        markets: Sequence[str],
        universe_snapshot: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Finalize a legacy failed row whose exact admitted prices all exist."""

        metadata = (
            dict(latest_eod_run.get('run_metadata') or {})
            if isinstance(latest_eod_run.get('run_metadata'), Mapping)
            else {}
        )
        evidence_metadata = {
            **metadata,
            'universe_snapshot': dict(universe_snapshot),
        }
        source_markets = _point_in_time_source_markets(
            evidence_metadata,
            fallback=markets,
        )
        validated = _validate_point_in_time_universe_snapshot(
            universe_snapshot,
            markets=source_markets,
            expected_market_date=market_date,
        )
        if validated is None:
            return None
        membership_symbols = {
            symbol
            for market_symbols in validated.values()
            for symbol in market_symbols
        }
        persisted_symbols = set(
            load_eod_run_persisted_symbols(
                int(latest_eod_run['id']),
                trade_date=market_date,
            )
        )
        if not membership_symbols or persisted_symbols != membership_symbols:
            return None

        completed_at = _utcnow_iso()
        metadata.update(
            {
                'markets': list(source_markets),
                'requested_symbols': sorted(membership_symbols),
                'failed_symbols': [],
                'failed_symbols_count': 0,
                'unattempted_symbols': [],
                'unattempted_symbols_count': 0,
                'universe_snapshot': dict(universe_snapshot),
                'point_in_time_recovery': True,
                'collection_coverage_checked_at': completed_at,
                'collection_persisted_symbols_count': len(
                    persisted_symbols
                ),
                'collection_expected_symbols_count': len(
                    membership_symbols
                ),
                'collection_coverage_inconsistent': False,
                'unexpected_persisted_symbols': [],
                'unexpected_persisted_symbols_count': 0,
                'resumable': False,
            }
        )
        complete_eod_collection_run(
            int(latest_eod_run['id']),
            status='completed',
            symbols_succeeded=len(membership_symbols),
            symbols_failed=0,
            error_message=None,
            run_metadata=metadata,
        )
        return {
            **dict(latest_eod_run),
            'status': 'completed',
            'symbols_total': len(membership_symbols),
            'symbols_succeeded': len(membership_symbols),
            'symbols_failed': 0,
            'error_message': None,
            'run_metadata': metadata,
        }

    def _preflight_untracked_followup_price_scope(
        self,
        *,
        market_date: str,
        latest_available_trade_date: str,
        latest_eod_run: Mapping[str, Any],
        markets: Sequence[str],
    ) -> dict[str, Any]:
        """Fence orphan downstream recovery behind exact EOD price identity."""

        metadata = (
            dict(latest_eod_run.get('run_metadata') or {})
            if isinstance(latest_eod_run.get('run_metadata'), Mapping)
            else {}
        )
        requested_symbols = _dedupe_symbol_codes(
            metadata.get('requested_symbols') or []
        )
        run_id = int(latest_eod_run['id'])
        persisted_count = count_eod_run_persisted_symbols(
            run_id,
            trade_date=market_date,
        )
        persisted_symbols = set(
            load_eod_run_persisted_symbols(
                run_id,
                trade_date=market_date,
            )
        )
        source_markets = _point_in_time_source_markets(
            metadata,
            fallback=markets,
        )
        if not requested_symbols:
            membership_symbols, source_markets = (
                self._resolve_point_in_time_followup_membership(
                    market_date=market_date,
                    metadata=metadata,
                    markets=source_markets,
                    eod_run_id=run_id,
                )
            )
            if not membership_symbols:
                return {
                    'verified': False,
                    'symbols': [],
                    'scope_unavailable': True,
                    'markets': source_markets,
                }
            requested_symbols = sorted(membership_symbols)
        requested_set = set(requested_symbols)
        missing_symbols = [
            symbol
            for symbol in requested_symbols
            if symbol not in persisted_symbols
        ]
        unexpected_symbols = sorted(persisted_symbols - requested_set)
        symbols_total = max(
            int(latest_eod_run.get('symbols_total') or 0),
            0,
        )
        if (
            persisted_count >= symbols_total == len(requested_symbols)
            and not missing_symbols
        ):
            downstream_symbols = _dedupe_symbol_codes(
                metadata.get('downstream_retry_symbols') or []
            )
            inherited_only_symbols = set(downstream_symbols) - requested_set
            if inherited_only_symbols:
                membership_symbols, source_markets = (
                    self._resolve_point_in_time_followup_membership(
                        market_date=market_date,
                        metadata=metadata,
                        markets=source_markets,
                        eod_run_id=run_id,
                    )
                )
                try:
                    exact_price_symbols = set(
                        list_symbols_with_daily_bars_in_range(
                            start_date=market_date,
                            end_date=market_date,
                        )
                    )
                except Exception as exc:
                    logger.warning(
                        'Unable to verify exact-date price scope for %s: %s',
                        market_date,
                        exc,
                    )
                    exact_price_symbols = set()
                downstream_set = set(downstream_symbols)
                if (
                    not membership_symbols
                    or not downstream_set.issubset(membership_symbols)
                    or not downstream_set.issubset(exact_price_symbols)
                ):
                    return {
                        'verified': False,
                        'symbols': [],
                        'scope_unavailable': True,
                        'markets': source_markets,
                        'requested_symbols': downstream_symbols,
                    }
            # Extra rows are a collection-integrity defect, but they cannot
            # leak into recovery when every admitted symbol is present and
            # the downstream run is explicitly bound to that admitted set.
            return {
                'verified': True,
                'symbols': requested_symbols,
                'downstream_symbols': downstream_symbols,
                'unexpected_symbols': unexpected_symbols,
            }

        covered_count = min(
            len(requested_set & persisted_symbols),
            symbols_total,
        )
        failed_count = max(symbols_total - covered_count, 0)
        checked_at = _utcnow_iso()
        metadata.update(
            {
                'failed_symbols': missing_symbols,
                'failed_symbols_count': len(missing_symbols),
                'unexpected_persisted_symbols': unexpected_symbols,
                'unexpected_persisted_symbols_count': len(
                    unexpected_symbols
                ),
                'collection_coverage_inconsistent': True,
                'collection_coverage_checked_at': checked_at,
                'collection_persisted_symbols_count': persisted_count,
                'collection_expected_symbols_count': symbols_total,
                'resumable': bool(failed_count or missing_symbols),
            }
        )
        error_message = (
            'Persisted price identity does not match the admitted EOD scope '
            'before follow-up recovery.'
        )
        complete_eod_collection_run(
            run_id,
            status='completed_with_errors',
            symbols_succeeded=covered_count,
            symbols_failed=failed_count,
            error_message=error_message,
            run_metadata=metadata,
        )
        result = build_daily_skip_result(
            reason='followup_recovery_price_scope_inconsistent',
            market_date=market_date,
            latest_available_trade_date=latest_available_trade_date,
            existing_run=latest_eod_run,
        )
        result.update(
            {
                'triggered': True,
                'status': 'completed_with_errors',
            }
        )
        result['followup_recovery'] = {
            'status': 'blocked',
            'reason': 'price_scope_inconsistent',
            'symbols': [],
            'state_convergence': {
                'updated': True,
                'status': 'completed_with_errors',
                'price_retry_pending': bool(
                    failed_count or missing_symbols
                ),
                'persisted_symbol_count': persisted_count,
                'coverage_gap_count': failed_count,
                'unexpected_symbol_count': len(unexpected_symbols),
            },
        }
        return {'verified': False, 'symbols': [], 'result': result}

    def _resume_auto_daily_followups_if_due(
        self,
        *,
        market_date: str,
        latest_available_trade_date: str,
        latest_eod_run: dict,
        markets: Sequence[str],
        allow_safe_downstream_scope_with_price_failures: bool = False,
        point_in_time_recovery: bool = False,
        provider_admission_fence_owned: bool = False,
    ) -> dict | None:
        """Resume failed post-collection stages without downloading prices again."""

        latest_eod_metadata = (
            latest_eod_run.get('run_metadata')
            if isinstance(latest_eod_run.get('run_metadata'), dict)
            else {}
        )
        current_krx_calendar_date = _current_krx_calendar_date()
        stale_pending_rearm = (
            pending_same_day_intraday_watch_rearm_marker(
                latest_eod_metadata.get(
                    SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
                )
            )
        )
        if stale_pending_rearm is not None and (
            date.fromisoformat(stale_pending_rearm['target_trade_date'])
            < current_krx_calendar_date
        ):
            rearm_block = self._block_same_day_intraday_watch_rearm(
                latest_eod_run=latest_eod_run,
                reason='next_open_window_expired',
            )
            result = build_daily_skip_result(
                reason='next_open_window_expired',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'retry_scheduled': False,
                'blocker': 'same_day_intraday_watch_rearm_terminal_expiry',
                'same_day_intraday_watch_rearm': rearm_block,
            }
            return result
        lineage_markets = _point_in_time_source_markets(
            latest_eod_metadata,
            fallback=markets,
        )
        repair_markets = latest_eod_metadata.get('markets')
        recovery_markets = (
            lineage_markets
            if point_in_time_recovery
            else tuple(markets)
        )
        point_in_time_membership_symbols = (
            _validated_point_in_time_membership_symbols(
                latest_eod_metadata,
                markets=recovery_markets,
                expected_market_date=market_date,
            )
            if point_in_time_recovery
            else None
        )
        price_retry_symbols = set(
            self._metadata_retry_symbol_codes(latest_eod_run)
        )
        downstream_retry_symbols = [
            symbol
            for symbol in _dedupe_symbol_codes(
                latest_eod_metadata.get('downstream_retry_symbols') or []
            )
            if symbol not in price_retry_symbols
        ]

        try:
            pipeline_run = daily_pipeline_run_state_service.get_latest_run_for_context(
                market_date=market_date,
                triggers=(AUTO_FOLLOWUP_RECOVERY_TRIGGER,),
                entrypoints=(AUTO_EOD_PIPELINE_ENTRYPOINT,),
            )
        except Exception as exc:  # pragma: no cover - defensive production guard
            logger.exception('Unable to inspect daily follow-up recovery state for %s', market_date)
            result = build_daily_skip_result(
                reason='followup_recovery_state_unavailable',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'status': 'state_unavailable',
                'error_type': type(exc).__name__,
            }
            return result

        if pipeline_run is not None and not self._pipeline_run_matches_eod_run(
            pipeline_run=pipeline_run,
            eod_run=latest_eod_run,
        ):
            logger.warning(
                'Ignoring daily pipeline run %s as recovery evidence for '
                'unrelated EOD run %s.',
                pipeline_run.get('id'),
                latest_eod_run.get('id'),
            )
            pipeline_run = None

        untracked_followup_parent = pipeline_run is None
        if pipeline_run is None:
            pipeline_run = self._build_untracked_followup_recovery_parent(
                latest_eod_run=latest_eod_run,
            )

        run_metadata = (
            pipeline_run.get('run_metadata')
            if isinstance(pipeline_run.get('run_metadata'), dict)
            else {}
        )
        stage_rows = pipeline_run.get('stages') if isinstance(pipeline_run.get('stages'), dict) else {}
        recovered_admission = _recovered_followup_admission_scope_outcome(
            latest_eod_run=latest_eod_run,
            market_date=market_date,
            pipeline_run=pipeline_run,
        )
        if recovered_admission is not None:
            prior_admission = _validated_followup_admission_partition(
                latest_eod_metadata
            )
            if prior_admission != recovered_admission:
                latest_eod_metadata = dict(latest_eod_metadata)
                latest_eod_metadata[
                    'followup_admission_scope_outcome'
                ] = recovered_admission
                latest_eod_metadata[
                    'followup_admission_reconciliation'
                ] = {
                    'schema_version': (
                        'followup_admission_reconciliation_v1'
                    ),
                    'market_date': market_date,
                    'eod_collection_run_id': int(latest_eod_run['id']),
                    'source_pipeline_run_id': int(pipeline_run['id']),
                    'symbol_count': len(
                        recovered_admission['selected_stock_codes']
                    ),
                    'membership_sha256': market_membership_sha256(
                        recovered_admission['selected_stock_codes']
                    ),
                    'prior_retry_debt_count': len(
                        prior_admission['retry_debt_stock_codes']
                    ),
                    'reconciled_at': _utcnow_iso(),
                }
                latest_eod_run['run_metadata'] = latest_eod_metadata
        latest_eod_stage_progress = (
            latest_eod_metadata.get('stage_progress')
            if isinstance(
                latest_eod_metadata.get('stage_progress'),
                Mapping,
            )
            else {}
        )
        summary_parent_symbols = (
            run_metadata.get('symbols')
            if isinstance(run_metadata.get('symbols'), list)
            else latest_eod_metadata.get('requested_symbols')
        )
        summary_fs_div = (
            run_metadata.get('fs_div')
            or latest_eod_metadata.get('fs_div')
        )
        summary_cutoff_proof = _select_summary_cutoff_proof(
            pipeline_stages=stage_rows,
            run_metadata=run_metadata,
            eod_stage_progress=latest_eod_stage_progress,
            market_date=market_date,
            fs_div=summary_fs_div,
            parent_symbols=summary_parent_symbols,
        )
        if (
            'agent_feedback_summaries' in stage_rows
            and _is_exact_signal_decider_retry_suffix(pipeline_run)
        ):
            # A completed summary prefix may remain in the persisted run even
            # though recovery has narrowed to the signal-decider suffix. Keep
            # using the suffix-bound immutable cutoff instead of treating the
            # historical prefix as a new summary lineage.
            summary_cutoff_proof = (
                _summary_cutoff_proof_from_run_metadata(run_metadata)
            )
        feedback_rearm_evidence_run: Mapping[str, Any] = pipeline_run
        prior_signal_rearm_evidence_used = False
        prior_signal_rearm_evidence: (
            tuple[Mapping[str, Any], dict[str, Any]] | None
        ) = None
        pending_same_day_rearm = (
            pending_same_day_intraday_watch_rearm_marker(
                latest_eod_metadata.get(
                    SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
                ),
                target_trade_date=current_krx_calendar_date,
            )
        )
        transparent_rearm_restart = bool(
            pending_same_day_rearm is not None
            and _is_transparent_core_restart_signal_suffix_run(
                pipeline_run,
                allow_pending_marker_restart=True,
            )
        )
        post_signal_rearm_restart = bool(
            pending_same_day_rearm is not None
            and _is_resumable_same_day_intraday_watch_post_signal_restart(
                pipeline_run
            )
        )
        recoverable_same_day_expiration = (
            _is_recoverable_same_day_intraday_watch_expiration_compatible(
                pipeline_run,
                current_market_date=current_krx_calendar_date,
            )
        )
        needs_prior_signal_rearm_evidence = bool(
            (
                summary_cutoff_proof is None
                or transparent_rearm_restart
                or post_signal_rearm_restart
            )
            and (
                _is_retired_decider_cutoff_zero_work_run(pipeline_run)
                or recoverable_same_day_expiration
                or transparent_rearm_restart
                or post_signal_rearm_restart
            )
        )
        resolved_stage_ids = {
            str(stage_id).strip()
            for stage_id in (run_metadata.get('resolved_stages') or stage_rows.keys())
            if str(stage_id).strip()
        }
        # Price failures are retried by the collection-specific branch. All
        # later tracked stages can safely resume from persisted prices without
        # another provider download.
        resumable_stage_order = tuple(
            stage_id
            for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
            if stage_id != 'price_collection'
        )
        recovery_scope = [
            stage_id
            for stage_id in resumable_stage_order
            if stage_id in resolved_stage_ids or stage_id in stage_rows
        ]
        if not recovery_scope:
            return None

        pipeline_status = str(
            pipeline_run.get('status') or ''
        ).strip().lower()
        resolved_stage_list = list(
            run_metadata.get('resolved_stages') or []
        )
        validation_pipeline_run = _active_execution_pipeline_projection(
            pipeline_run
        )
        validation_stage_rows = (
            validation_pipeline_run.get('stages')
            if isinstance(validation_pipeline_run.get('stages'), Mapping)
            else stage_rows
        )
        persisted_recovery_result: dict[str, Any] = {
            'status': pipeline_status,
            'pipeline_run_id': pipeline_run.get('id'),
            'pipeline_run': validation_pipeline_run,
            'market_date': pipeline_run.get('market_date'),
            'resolved_stages': resolved_stage_list,
            'stages': validation_stage_rows,
        }
        persisted_nonblocking_marker = (
            validated_nonblocking_partial_progression_marker(
                persisted_recovery_result,
                expected_eod_collection_run_id=int(
                    latest_eod_run['id']
                ),
            )
            if pipeline_run.get('trigger')
            == AUTO_FOLLOWUP_RECOVERY_TRIGGER
            and type(latest_eod_run.get('id')) is int
            else None
        )
        legacy_evaluation_only_completion = (
            _legacy_evaluation_only_completion_projection(pipeline_run)
        )
        outcome_is_satisfied = (
            daily_pipeline_prefix_outcome_is_satisfied
            if terminal_continuation_marker_is_valid(run_metadata)
            else daily_pipeline_outcome_is_satisfied
        )
        partial_summary_prefix_proven = bool(
            isinstance(
                stage_rows.get('agent_feedback_summaries'), Mapping
            )
            and str(
                stage_rows['agent_feedback_summaries'].get('status') or ''
            ).strip().lower()
            == 'partial'
            and isinstance(summary_cutoff_proof, Mapping)
            and isinstance(
                summary_cutoff_proof.get('completed_scope_proof'),
                Mapping,
            )
            and isinstance(
                summary_cutoff_proof.get('terminal_role_scope_proof'),
                Mapping,
            )
            and isinstance(
                summary_cutoff_proof.get('expected_completed_symbols'),
                list,
            )
            and bool(
                summary_cutoff_proof.get('expected_completed_symbols')
            )
        )
        incomplete_stages = []
        for stage_id in recovery_scope:
            stage = stage_rows.get(stage_id)
            stage_outcome = (
                {**dict(stage), 'stage': stage_id}
                if isinstance(stage, Mapping)
                else {}
            )
            if (
                stage_id == 'agent_feedback_summaries'
                and partial_summary_prefix_proven
            ):
                continue
            if (
                recoverable_same_day_expiration
                or not outcome_is_satisfied(stage_outcome)
            ):
                incomplete_stages.append(stage_id)
        stale_feedback_ordering = (
            _feedback_suffix_has_stale_prerequisite_ordering(
                stage_rows=stage_rows,
                point_in_time_recovery=point_in_time_recovery,
            )
        )
        stale_signal_feature_ordering = (
            _signal_features_have_stale_prerequisite_ordering(
                stage_rows=stage_rows,
                point_in_time_recovery=point_in_time_recovery,
            )
        )
        ordering_repair_stage = (
            'signal_features'
            if stale_signal_feature_ordering
            and 'signal_features' in recovery_scope
            else (
                'agent_feedback_summaries'
                if stale_feedback_ordering
                and 'agent_feedback_summaries' in recovery_scope
                else None
            )
        )
        if (
            ordering_repair_stage is not None
            and (
                not incomplete_stages
                or recovery_scope.index(ordering_repair_stage)
                < recovery_scope.index(incomplete_stages[0])
            )
        ):
            incomplete_stages.insert(0, ordering_repair_stage)
        downstream_resume_from_stage = str(
            latest_eod_metadata.get('downstream_retry_resume_from_stage') or ''
        ).strip()
        if downstream_resume_from_stage not in recovery_scope:
            downstream_resume_from_stage = next(
                (
                    stage_id
                    for stage_id in (
                        'pykrx_enrichment',
                        'technical_snapshots',
                    )
                    if stage_id in recovery_scope
                ),
                '',
            )
        first_exact_followup_index = next(
            (
                recovery_scope.index(stage_id)
                for stage_id in FOLLOWUP_EXACT_RECOVERY_STAGE_ORDER
                if stage_id in recovery_scope
            ),
            None,
        )
        full_point_in_time_scope = sorted(
            point_in_time_membership_symbols or []
        )
        persisted_recovery_symbols = _dedupe_symbol_codes(
            run_metadata.get('symbols') or []
        )
        downstream_resume_stage = stage_rows.get(
            downstream_resume_from_stage
        )
        narrow_upstream_recovery_scope = bool(
            pipeline_run.get('trigger') == AUTO_FOLLOWUP_RECOVERY_TRIGGER
            and point_in_time_recovery
            and not price_retry_symbols
            and run_metadata.get('point_in_time_recovery') is True
            and str(run_metadata.get('resume_from_stage') or '').strip()
            == downstream_resume_from_stage
            and downstream_resume_from_stage == 'pykrx_enrichment'
            and downstream_resume_from_stage in recovery_scope
            and first_exact_followup_index is not None
            and recovery_scope.index(downstream_resume_from_stage)
            < first_exact_followup_index
            and full_point_in_time_scope
            and run_metadata.get('exact_membership_symbol_count')
            == len(full_point_in_time_scope)
            and run_metadata.get('exact_membership_sha256')
            == market_membership_sha256(full_point_in_time_scope)
            and persisted_recovery_symbols
            and set(persisted_recovery_symbols)
            < set(full_point_in_time_scope)
            and set(persisted_recovery_symbols)
            == set(downstream_retry_symbols)
            and isinstance(downstream_resume_stage, Mapping)
            and daily_pipeline_outcome_is_satisfied(
                {
                    **dict(downstream_resume_stage),
                    'stage': downstream_resume_from_stage,
                }
            )
        )
        if (
            (
                pipeline_run.get('trigger')
                != AUTO_FOLLOWUP_RECOVERY_TRIGGER
                or narrow_upstream_recovery_scope
            )
            and downstream_retry_symbols
            and downstream_resume_from_stage in recovery_scope
            and (
                not incomplete_stages
                or recovery_scope.index(downstream_resume_from_stage)
                < recovery_scope.index(incomplete_stages[0])
            )
        ):
            incomplete_stages.insert(0, downstream_resume_from_stage)
        if persisted_nonblocking_marker is not None:
            # A crash can occur after the suffix run is finalized but before
            # its proof is copied to the EOD ancestor. Reconcile that exact
            # durable proof instead of rerunning a partial decider.
            incomplete_stages.clear()
        upstream_admission_rebuild_pending = bool(
            downstream_retry_symbols
            and incomplete_stages
            and incomplete_stages[0] == downstream_resume_from_stage
            and downstream_resume_from_stage in recovery_scope
            and first_exact_followup_index is not None
            and recovery_scope.index(downstream_resume_from_stage)
            < first_exact_followup_index
            and latest_eod_metadata.get(
                FOLLOWUP_STAGE_DEBT_METADATA_KEY
            )
            is None
        )
        if upstream_admission_rebuild_pending:
            # Exact follow-up debt is scoped by the upstream admission
            # partition. When enrichment (or another admission prerequisite)
            # must be rebuilt first, the persisted downstream partition is
            # necessarily stale. Defer debt reconstruction to the suffix run
            # instead of blocking that prerequisite repair with old proof.
            stage_debt_resolution = {
                'required': False,
                'ready': False,
                'source': 'deferred_for_upstream_admission_rebuild',
            }
        else:
            stage_debt_resolution = _resolve_followup_stage_debt_state(
                market_date=market_date,
                eod_collection_run_id=int(latest_eod_run['id']),
                latest_eod_metadata=latest_eod_metadata,
                stage_rows=stage_rows,
                incomplete_stages=incomplete_stages,
                latest_pipeline_run=pipeline_run,
            )
        if (
            stage_debt_resolution.get('required') is True
            and stage_debt_resolution.get('ready') is not True
        ):
            result = build_daily_skip_result(
                reason='followup_stage_debt_scope_unavailable',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'status': 'blocked',
                'retry_scheduled': False,
                'blocker': stage_debt_resolution.get('reason'),
            }
            return result
        stage_debt_state = (
            dict(stage_debt_resolution['state'])
            if stage_debt_resolution.get('ready') is True
            and isinstance(stage_debt_resolution.get('state'), Mapping)
            else None
        )
        reconciled_stage_debt_persisted = False
        if (
            stage_debt_state is not None
            and stage_debt_resolution.get('source')
            == 'persisted_stage_debt_reconciled'
        ):
            # A recovery may commit its exact stage result immediately before
            # the scheduler crashes, leaving the EOD ancestor with the old
            # pending ledger. Persist the reconciled tombstone before any
            # early return or convergence decision, including when this proof
            # resolves the final pending stage.
            reconciled_persistence = (
                self._persist_followup_stage_debt_state(
                    latest_eod_run=latest_eod_run,
                    state=stage_debt_state,
                )
            )
            if not reconciled_persistence.get('updated'):
                result = build_daily_skip_result(
                    reason='followup_stage_debt_state_unavailable',
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'status': 'state_unavailable',
                    'retry_scheduled': False,
                    'blocker': reconciled_persistence.get('reason'),
                }
                return result
            reconciled_stage_debt_persisted = True
            persisted_metadata = latest_eod_run.get('run_metadata')
            if isinstance(persisted_metadata, Mapping):
                latest_eod_metadata = dict(persisted_metadata)
        stage_debt_request = (
            _next_followup_stage_debt_request(stage_debt_state)
            if stage_debt_state is not None
            else None
        )
        stage_debt_terminal_progression = False
        stage_debt_terminal_progression_basis: str | None = None
        stage_debt_terminal_progression_symbols: list[str] = []
        deferred_stage_debt_request = stage_debt_request
        if stage_debt_state is not None:
            raw_terminal_marker = latest_eod_metadata.get(
                FOLLOWUP_STAGE_DEBT_TERMINAL_PROGRESSION_METADATA_KEY
            )
            validated_terminal_marker = None
            if raw_terminal_marker is not None:
                validated_terminal_marker = (
                    _validated_followup_stage_debt_terminal_progression_marker(
                        raw_terminal_marker,
                        state=stage_debt_state,
                        market_date=market_date,
                        eod_collection_run_id=int(latest_eod_run['id']),
                    )
                )
                if validated_terminal_marker is None:
                    result = build_daily_skip_result(
                        reason='followup_stage_debt_state_unavailable',
                        market_date=market_date,
                        latest_available_trade_date=(
                            latest_available_trade_date
                        ),
                        existing_run=latest_eod_run,
                    )
                    result['followup_recovery'] = {
                        'status': 'state_unavailable',
                        'retry_scheduled': False,
                        'blocker': (
                            'followup_stage_debt_terminal_marker_invalid'
                        ),
                        'stage_debt_preserved': True,
                    }
                    return result
            terminal_point_in_time_recovery = bool(
                date.fromisoformat(market_date)
                < current_krx_calendar_date
            )
            terminal_cohort = _followup_stage_debt_terminal_cohort(
                stage_debt_state,
                point_in_time_recovery=(
                    terminal_point_in_time_recovery
                ),
                # Exact per-stage success is sufficient authority for this
                # cohort. Newly admitted retry debt must not hold already
                # proven symbols behind an unrelated first-attempt gate.
                require_prior_attempts=False,
            )
            processed_terminal_symbols = set(
                (
                    validated_terminal_marker.get(
                        'processed_stock_codes'
                    )
                    if validated_terminal_marker is not None
                    else []
                )
                or []
            )
            if terminal_cohort is not None:
                cohort_basis, cohort_symbols = terminal_cohort
                latest_suffix_symbols = sorted(
                    _dedupe_symbol_codes(
                        run_metadata.get('symbols') or []
                    )
                )
                unrecorded_suffix_symbols = sorted(
                    set(latest_suffix_symbols)
                    - processed_terminal_symbols
                )
                if (
                    unrecorded_suffix_symbols
                    and set(unrecorded_suffix_symbols).issubset(
                        set(cohort_symbols)
                    )
                    and (
                        _resolved_terminal_followup_continuation_stages(
                            resolved_stage_list
                        )
                        in {
                            TERMINAL_FOLLOWUP_CONTINUATION_STAGES,
                            LEGACY_TERMINAL_FOLLOWUP_CONTINUATION_STAGES,
                        }
                    )
                    and _validated_followup_stage_debt_terminal_progression_result(
                        persisted_recovery_result,
                        market_date=market_date,
                        eod_collection_run_id=int(latest_eod_run['id']),
                        cohort_symbols=unrecorded_suffix_symbols,
                    )
                    is not None
                ):
                    suffix_reconciliation = (
                        self._persist_followup_stage_debt_terminal_progression(
                            latest_eod_run=latest_eod_run,
                            state=stage_debt_state,
                            cohort_basis=cohort_basis,
                            cohort_symbols=unrecorded_suffix_symbols,
                            recovery_result=persisted_recovery_result,
                        )
                    )
                    if suffix_reconciliation.get('updated'):
                        validated_terminal_marker = (
                            suffix_reconciliation.get('marker')
                        )
                        processed_terminal_symbols = set(
                            (
                                validated_terminal_marker.get(
                                    'processed_stock_codes'
                                )
                                if isinstance(
                                    validated_terminal_marker, Mapping
                                )
                                else []
                            )
                            or []
                        )
                        persisted_metadata = latest_eod_run.get(
                            'run_metadata'
                        )
                        if isinstance(persisted_metadata, Mapping):
                            latest_eod_metadata = dict(
                                persisted_metadata
                            )
                    elif suffix_reconciliation.get('reason') not in {
                        'followup_stage_debt_terminal_already_persisted',
                    }:
                        result = build_daily_skip_result(
                            reason='followup_stage_debt_state_unavailable',
                            market_date=market_date,
                            latest_available_trade_date=(
                                latest_available_trade_date
                            ),
                            existing_run=latest_eod_run,
                        )
                        result['followup_recovery'] = {
                            'status': 'state_unavailable',
                            'retry_scheduled': False,
                            'blocker': suffix_reconciliation.get(
                                'reason'
                            ),
                            'stage_debt_preserved': True,
                        }
                        return result
                unprocessed_terminal_symbols = sorted(
                    set(cohort_symbols) - processed_terminal_symbols
                )
                if unprocessed_terminal_symbols:
                    stage_debt_terminal_progression = True
                    stage_debt_terminal_progression_basis = cohort_basis
                    stage_debt_terminal_progression_symbols = (
                        unprocessed_terminal_symbols
                    )
                    stage_debt_request = None
                    point_in_time_recovery = (
                        terminal_point_in_time_recovery
                    )
                    point_in_time_membership_symbols = (
                        _validated_followup_admitted_symbols(
                            latest_eod_metadata
                        )
                    )
                    if not point_in_time_membership_symbols:
                        result = build_daily_skip_result(
                            reason='followup_stage_debt_scope_unavailable',
                            market_date=market_date,
                            latest_available_trade_date=(
                                latest_available_trade_date
                            ),
                            existing_run=latest_eod_run,
                        )
                        result['followup_recovery'] = {
                            'status': 'blocked',
                            'retry_scheduled': False,
                            'resume_from_stage': (
                                TERMINAL_FOLLOWUP_CONTINUATION_STAGES[0]
                            ),
                            'blocker': (
                                'frozen_stage_membership_unavailable'
                            ),
                            'stage_debt_preserved': True,
                        }
                        return result
                    for terminal_stage_id in (
                        TERMINAL_FOLLOWUP_CONTINUATION_STAGES
                    ):
                        if terminal_stage_id not in recovery_scope:
                            recovery_scope.append(terminal_stage_id)
                    incomplete_stages = list(
                        TERMINAL_FOLLOWUP_CONTINUATION_STAGES
                    )
        stage_debt_scope_outcome: dict[str, Any] | None = None
        if stage_debt_request is not None:
            debt_stage_id, debt_symbols = stage_debt_request
            if (
                debt_stage_id == 'signal_features'
                and date.fromisoformat(market_date)
                < current_krx_calendar_date
            ):
                result = build_daily_skip_result(
                    reason='followup_stage_debt_current_only_expired',
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'status': 'blocked',
                    'retry_scheduled': False,
                    'attempt_started': False,
                    'consumes_retry_budget': False,
                    'resume_from_stage': debt_stage_id,
                    'symbols': debt_symbols,
                    'blocker': 'historical_current_only_stage_debt',
                    'stage_debt_preserved': True,
                }
                return result
            point_in_time_recovery = debt_stage_id != 'signal_features'
            point_in_time_membership_symbols = (
                _validated_followup_admitted_symbols(latest_eod_metadata)
            )
            if not point_in_time_membership_symbols:
                result = build_daily_skip_result(
                    reason='followup_stage_debt_scope_unavailable',
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'status': 'blocked',
                    'retry_scheduled': False,
                    'resume_from_stage': debt_stage_id,
                    'symbols': debt_symbols,
                    'blocker': 'frozen_stage_membership_unavailable',
                    'stage_debt_preserved': True,
                }
                return result
            stage_debt_scope_outcome = (
                _build_followup_stage_recovery_scope_outcome(
                    stage_debt_state,
                    stage_id=debt_stage_id,
                )
            )
            if stage_debt_scope_outcome is None:
                result = build_daily_skip_result(
                    reason='followup_stage_debt_scope_unavailable',
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'status': 'blocked',
                    'retry_scheduled': False,
                    'resume_from_stage': debt_stage_id,
                    'symbols': debt_symbols,
                    'blocker': 'stage_debt_partition_invalid',
                    'stage_debt_preserved': True,
                }
                return result
            if not reconciled_stage_debt_persisted:
                debt_persistence = (
                    self._persist_followup_stage_debt_state(
                        latest_eod_run=latest_eod_run,
                        state=stage_debt_state,
                    )
                )
                if not debt_persistence.get('updated'):
                    result = build_daily_skip_result(
                        reason='followup_stage_debt_state_unavailable',
                        market_date=market_date,
                        latest_available_trade_date=(
                            latest_available_trade_date
                        ),
                        existing_run=latest_eod_run,
                    )
                    result['followup_recovery'] = {
                        'status': 'state_unavailable',
                        'retry_scheduled': False,
                        'blocker': debt_persistence.get('reason'),
                    }
                    return result
            incomplete_stages = [debt_stage_id]
            if debt_stage_id not in recovery_scope:
                recovery_scope.append(debt_stage_id)
        if not incomplete_stages:
            is_followup_recovery_parent = bool(
                pipeline_run.get('trigger')
                == AUTO_FOLLOWUP_RECOVERY_TRIGGER
            )
            if persisted_nonblocking_marker is not None:
                pipeline_convergence = {
                    'updated': False,
                    'pipeline_run_id': pipeline_run.get('id'),
                    'status': 'partial',
                    'reason': (
                        'validated_nonblocking_partial_progression'
                    ),
                }
            elif legacy_evaluation_only_completion is not None:
                pipeline_convergence = {
                    'updated': False,
                    'pipeline_run_id': pipeline_run.get('id'),
                    'status': pipeline_status,
                    'reason': 'legacy_evaluation_stage_retired',
                }
            else:
                convergence_kwargs: dict[str, Any] = {
                    'pipeline_run_id': int(pipeline_run['id']),
                }
                if is_followup_recovery_parent or pipeline_status in {
                    'completed',
                    'partial',
                    'skipped',
                }:
                    convergence_kwargs['allow_satisfied_terminal'] = True
                pipeline_convergence = (
                    daily_pipeline_run_state_service
                    .converge_terminal_stages_run(**convergence_kwargs)
                )
            pipeline_parent_finalized = bool(
                legacy_evaluation_only_completion is not None
                or pipeline_convergence.get('updated')
                or pipeline_convergence.get('reason')
                == 'already_non_resumable'
                or pipeline_convergence.get('reason')
                == 'validated_nonblocking_partial_progression'
            )
            if not pipeline_parent_finalized:
                return None

            eod_convergence: dict[str, Any] | None = None
            if is_followup_recovery_parent:
                converged_status = str(
                    pipeline_convergence.get('status') or pipeline_status
                ).strip().lower()
                convergence_result: dict[str, Any] = (
                    (
                        legacy_evaluation_only_completion
                        if legacy_evaluation_only_completion is not None
                        else persisted_recovery_result
                    )
                    if persisted_nonblocking_marker is not None
                    or legacy_evaluation_only_completion is not None
                    else {
                        # Persisted terminal stage rows remain sufficient for
                        # the legacy non-rearm convergence path. A pending
                        # rearm marker alone requires the full exact PITR
                        # proof below.
                        'status': 'completed',
                        'pipeline_run_id': pipeline_run.get('id'),
                        'pipeline_status': pipeline_status,
                    }
                )
                persisted_recovery_symbols = _dedupe_symbol_codes(
                    run_metadata.get('symbols') or []
                )
                if persisted_recovery_symbols:
                    convergence_result[
                        '_proven_resolved_downstream_symbols'
                    ] = persisted_recovery_symbols
                if (
                    pending_same_day_rearm is not None
                    and legacy_evaluation_only_completion is None
                ):
                    convergence_result.update(
                        {
                            'status': converged_status,
                            'pipeline_run': {
                                **dict(pipeline_run),
                                'status': converged_status,
                                'current_stage': list(
                                    run_metadata.get('resolved_stages') or []
                                )[-1],
                            },
                            'market_date': pipeline_run.get('market_date'),
                            'resolved_stages': list(
                                run_metadata.get('resolved_stages') or []
                            ),
                            'stages': stage_rows,
                        }
                    )
                eod_convergence = self._persist_successful_followup_recovery(
                    latest_eod_run=latest_eod_run,
                    recovery_result=convergence_result,
                )
            elif downstream_retry_symbols:
                # A primary parent is not terminal proof while an implicit
                # downstream artifact scope remains durable. The technical
                # resume branch above normally handles this; fail closed if
                # that stage was not part of this run's resolved scope.
                return None
            if not (
                pipeline_convergence.get('updated')
                or (
                    eod_convergence is not None
                    and eod_convergence.get('updated')
                )
            ):
                return None

            convergence = (
                dict(eod_convergence)
                if eod_convergence is not None
                else {'updated': False, 'reason': 'not_applicable'}
            )
            convergence['pipeline_run'] = pipeline_convergence
            result = build_daily_skip_result(
                reason=(
                    'followup_recovery_state_converged'
                    if is_followup_recovery_parent
                    else 'daily_pipeline_state_converged'
                ),
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'pipeline_run_id': pipeline_run.get('id'),
                'state_convergence': convergence,
            }
            return result

        resume_from_stage = incomplete_stages[0]
        recovery_stages = _resolve_followup_recovery_stages(
            recovery_scope=recovery_scope,
            incomplete_stages=incomplete_stages,
            has_exact_stage_debt_request=(
                stage_debt_request is not None
            ),
        )
        proven_partial_summary_downstream_continuation = bool(
            partial_summary_prefix_proven
            and resume_from_stage == 'signal_decider'
            and 'agent_feedback_summaries' not in recovery_stages
            and isinstance(summary_cutoff_proof, Mapping)
        )
        recovery_reaches_followup_stage = any(
            stage_id in AUTO_DAILY_FOLLOWUP_STAGE_ORDER
            for stage_id in recovery_stages
        )
        first_incomplete_stage = incomplete_stages[0]
        required_provider_admission = (
            _validated_required_provider_admission(
                stage_rows.get(first_incomplete_stage),
                expected_stage_id=first_incomplete_stage,
            )
        )
        provider_admission_rearm = bool(
            required_provider_admission is not None
            and _required_provider_credentials_are_ready(
                required_provider_admission
            )
        )
        if (
            required_provider_admission is not None
            and not provider_admission_rearm
        ):
            result = build_daily_skip_result(
                reason='followup_recovery_provider_not_ready',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'status': 'blocked_missing_credential',
                'retry_scheduled': True,
                'resume_from_stage': first_incomplete_stage,
                'attempt_started': False,
                'consumes_retry_budget': False,
                'provider_admission': required_provider_admission,
            }
            return result
        dart_recovery_scope = _resolve_dart_followup_recovery_scope(
            incomplete_stages=incomplete_stages,
            stage_rows=stage_rows,
            run_metadata=run_metadata,
            latest_eod_metadata=latest_eod_metadata,
        )
        if (
            dart_recovery_scope.get('required') is True
            and dart_recovery_scope.get('ready') is not True
        ):
            result = build_daily_skip_result(
                reason='followup_recovery_exact_scope_unavailable',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'status': 'blocked',
                'retry_scheduled': False,
                'resume_from_stage': first_incomplete_stage,
                'blocker': dart_recovery_scope.get('reason'),
            }
            return result
        if (
            stage_debt_request is not None
            and stage_debt_request[0]
            == 'fundamentals_daily_refresh'
            and (
                dart_recovery_scope.get('ready') is not True
                or set(dart_recovery_scope.get('symbols') or [])
                != set(stage_debt_request[1])
            )
        ):
            result = build_daily_skip_result(
                reason='followup_stage_debt_scope_mismatch',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'status': 'blocked',
                'retry_scheduled': False,
                'resume_from_stage': 'fundamentals_daily_refresh',
                'blocker': 'dart_stage_debt_scope_mismatch',
                'stage_debt_preserved': True,
            }
            return result
        recovering_safe_downstream_scope = bool(
            allow_safe_downstream_scope_with_price_failures
            and price_retry_symbols
            and downstream_retry_symbols
        )
        if recovery_reaches_followup_stage and price_retry_symbols:
            if not recovering_safe_downstream_scope:
                return None

        verified_orphan_symbols: list[str] = []
        verified_downstream_symbols: list[str] = []

        now = datetime.now(timezone.utc)
        latest_activity_at = _parse_activity_datetime(
            pipeline_run.get('updated_at') or pipeline_run.get('started_at')
        )
        if pipeline_status == 'running':
            stale_after_seconds = max(
                60,
                int(settings.eod_auto_pykrx_stale_run_minutes or 30) * 60,
            )
            if latest_activity_at is None or (now - latest_activity_at).total_seconds() < stale_after_seconds:
                result = build_daily_skip_result(
                    reason='followup_recovery_already_running',
                    market_date=market_date,
                    latest_available_trade_date=latest_available_trade_date,
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'pipeline_run_id': pipeline_run.get('id'),
                    'resume_from_stage': incomplete_stages[0],
                }
                return result

        nonconsuming_provider_admission_runs = 0
        nonconsuming_stage_debt_progress_runs = (
            _count_nonconsuming_stage_debt_progress_runs(
                stage_debt_state
            )
        )
        try:
            raw_recovery_attempts = daily_pipeline_run_state_service.count_runs(
                market_date=market_date,
                trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
                reused_eod_collection_run_id=int(latest_eod_run['id']),
            )
            if raw_recovery_attempts > 0:
                nonconsuming_provider_admission_runs = (
                    _count_all_nonconsuming_provider_admission_runs(
                        current_run=pipeline_run,
                        raw_recovery_attempts=raw_recovery_attempts,
                        market_date=market_date,
                        reused_eod_collection_run_id=int(
                            latest_eod_run['id']
                        ),
                    )
                )
            recovery_attempts = max(
                raw_recovery_attempts
                - nonconsuming_provider_admission_runs
                - nonconsuming_stage_debt_progress_runs,
                0,
            )
            repair_checkpoint: dict[str, Any] | None = None
            repair_checkpoint_after = (
                pipeline_run.get('completed_at')
                or pipeline_run.get('updated_at')
                or pipeline_run.get('started_at')
            )
            if (
                recovery_attempts
                == MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                and pipeline_run.get('trigger')
                == AUTO_FOLLOWUP_RECOVERY_TRIGGER
                and isinstance(repair_markets, list)
                and repair_checkpoint_after
            ):
                repair_checkpoint = (
                    load_latest_pykrx_enrichment_repair_checkpoint(
                        trade_date=market_date,
                        markets=tuple(repair_markets),
                        completed_after=repair_checkpoint_after,
                    )
                )
        except Exception as exc:  # pragma: no cover - defensive production guard
            logger.exception(
                'Unable to inspect daily follow-up recovery admission for %s',
                market_date,
            )
            result = build_daily_skip_result(
                reason='followup_recovery_state_unavailable',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'status': 'state_unavailable',
                'error_type': type(exc).__name__,
            }
            return result

        if (
            needs_prior_signal_rearm_evidence
            and (
                recovery_attempts
                == MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                or (
                    pending_same_day_rearm is not None
                    and MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                    < recovery_attempts
                    <= SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT
                )
            )
        ):
            try:
                prior_pipeline_runs = (
                    daily_pipeline_run_state_service
                    .list_prior_followup_runs_for_lineage(
                        market_date=market_date,
                        reused_eod_collection_run_id=int(
                            run_metadata[
                                'reused_eod_collection_run_id'
                            ]
                        ),
                        before_pipeline_run_id=int(pipeline_run['id']),
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive guard
                logger.exception(
                    'Unable to inspect prior feedback recovery evidence for %s',
                    market_date,
                )
                result = build_daily_skip_result(
                    reason='followup_recovery_state_unavailable',
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'status': 'state_unavailable',
                    'error_type': type(exc).__name__,
                }
                return result
            prior_signal_rearm_evidence = (
                _select_prior_signal_rearm_evidence(
                    current_pipeline_run=pipeline_run,
                    prior_pipeline_runs=prior_pipeline_runs,
                    current_market_date=current_krx_calendar_date,
                    allow_transparent_current=(
                        pending_same_day_rearm is not None
                    ),
                )
            )
            if prior_signal_rearm_evidence is not None:
                feedback_rearm_evidence_run, summary_cutoff_proof = (
                    prior_signal_rearm_evidence
                )
                prior_signal_rearm_evidence_used = True

        same_day_post_signal_continuation_in_place = bool(
            post_signal_rearm_restart
            and prior_signal_rearm_evidence_used
            and pending_same_day_rearm is not None
            and int(feedback_rearm_evidence_run.get('id') or 0)
            == int(pending_same_day_rearm['proof_pipeline_run_id'])
            and recovery_attempts
            <= SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT
        )

        cutoff_contract_repair_eligible = (
            not prior_signal_rearm_evidence_used
            and _is_summary_cutoff_contract_repair_candidate(
                pipeline_run=pipeline_run,
                summary_cutoff_proof=summary_cutoff_proof,
                price_retry_symbols=price_retry_symbols,
            )
        )
        cutoff_contract_repair = bool(
            cutoff_contract_repair_eligible
            and recovery_attempts
            == MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
            and repair_checkpoint is None
        )
        persisted_cutoff_repair_proof = run_metadata.get(
            'summary_cutoff_contract_repair_proof'
        )
        cutoff_contract_repair_resume = bool(
            recovery_attempts
            == MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE + 1
            and repair_checkpoint is None
            and is_resumable_summary_cutoff_contract_repair(pipeline_run)
            and isinstance(persisted_cutoff_repair_proof, Mapping)
            and dict(persisted_cutoff_repair_proof)
            == summary_cutoff_proof
        )
        cutoff_contract_repair_proven = bool(
            cutoff_contract_repair_eligible
            or summary_cutoff_contract_repair_marker_is_valid(run_metadata)
        )
        terminal_continuation_in_place = bool(
            recovery_attempts
            == MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE + 1
            and repair_checkpoint is None
            and _is_terminal_followup_continuation_candidate(pipeline_run)
        )
        provider_outage_rearm_candidate = bool(
            recovery_attempts
            >= MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
            and _is_dart_provider_outage_rearm_candidate(pipeline_run)
        )
        retryable_feedback_rearm_detected = (
            _is_retryable_feedback_rearm_candidate(
                feedback_rearm_evidence_run
            )
        )
        (
            feedback_circuit_marker_present,
            _,
            feedback_circuit_marker,
        ) = _feedback_dispatch_circuit_evidence(
            feedback_rearm_evidence_run
        )
        if (
            prior_signal_rearm_evidence_used
            and (
                recoverable_same_day_expiration
                or pending_same_day_rearm is not None
            )
            and not _is_canonical_same_day_signal_rearm_proof_compatible(
                feedback_rearm_evidence_run
            )
        ):
            rearm_block = self._block_same_day_intraday_watch_rearm(
                latest_eod_run=latest_eod_run,
                reason='signal_rearm_proof_not_canonical',
            )
            result = build_daily_skip_result(
                reason='followup_rearm_proof_not_canonical',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'attempts': recovery_attempts,
                'retry_scheduled': False,
                'blocker': 'signal_rearm_proof_not_canonical',
                'resume_from_stage': incomplete_stages[0],
                'same_day_intraday_watch_rearm': rearm_block,
            }
            return result
        feedback_retry_stage_id = _feedback_retry_stage_id(
            feedback_rearm_evidence_run
        )
        if (
            feedback_circuit_marker_present
            and not retryable_feedback_rearm_detected
        ):
            marker_retryable = bool(
                isinstance(feedback_circuit_marker, Mapping)
                and feedback_circuit_marker.get('retryable') is True
            )
            rearm_block = self._block_same_day_intraday_watch_rearm(
                latest_eod_run=latest_eod_run,
                reason='invalid_or_non_retryable_dispatch_circuit',
            )
            result = build_daily_skip_result(
                reason='followup_feedback_dispatch_circuit_blocked',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'attempts': recovery_attempts,
                'retryable_feedback_rearm': False,
                'retry_scheduled': False,
                'blocker': (
                    'invalid_dispatch_circuit_breaker'
                    if marker_retryable
                    else 'non_retryable_dispatch_circuit_breaker'
                ),
                'resume_from_stage': incomplete_stages[0],
                'same_day_intraday_watch_rearm': rearm_block,
            }
            return result
        feedback_provider_backoff = _feedback_dispatch_rate_limit_backoff(
            marker=feedback_circuit_marker,
            pipeline_run=feedback_rearm_evidence_run,
            now=now,
        )
        if (
            retryable_feedback_rearm_detected
            and isinstance(feedback_provider_backoff, Mapping)
            and feedback_provider_backoff.get('active') is True
        ):
            result = build_daily_skip_result(
                reason='followup_feedback_provider_backoff',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'attempts': recovery_attempts,
                'retryable_feedback_rearm': True,
                'retry_scheduled': True,
                'provider': 'openclaw',
                'provider_status': 'rate_limited',
                'retry_after_seconds': feedback_provider_backoff[
                    'retry_after_seconds'
                ],
                'retry_not_before': feedback_provider_backoff[
                    'retry_not_before'
                ],
                'retry_after_source': feedback_provider_backoff[
                    'retry_after_source'
                ],
                'resume_from_stage': incomplete_stages[0],
            }
            return result
        retryable_feedback_rearm_config = (
            _retryable_feedback_rearm_config(
                pipeline_run,
                market_date=market_date,
            )
            if retryable_feedback_rearm_detected
            and feedback_retry_stage_id != 'signal_decider'
            else None
        )
        retryable_feedback_source_run_id = (
            str(
                retryable_feedback_rearm_config.get('source_run_id')
                or ''
            ).strip()
            if isinstance(retryable_feedback_rearm_config, Mapping)
            else None
        )
        retryable_feedback_knowledge_cutoff_at = (
            retryable_feedback_rearm_config.get('knowledge_cutoff_at')
            if isinstance(retryable_feedback_rearm_config, Mapping)
            else None
        )
        retryable_feedback_max_cycles = (
            _retryable_feedback_rearm_max_cycles(
                feedback_rearm_evidence_run
            )
            if retryable_feedback_rearm_detected
            else None
        )
        retryable_feedback_rearm_candidate = bool(
            retryable_feedback_rearm_detected
            and recovery_attempts
            < (
                MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                + MAX_AUTO_FEEDBACK_REARM_RUNS_PER_TRADE_DATE
            )
            and retryable_feedback_max_cycles is not None
            and (
                (
                    feedback_retry_stage_id == 'signal_decider'
                    and summary_cutoff_proof is not None
                )
                or (
                    feedback_retry_stage_id != 'signal_decider'
                    and isinstance(
                        retryable_feedback_rearm_config,
                        Mapping,
                    )
                    and retryable_feedback_knowledge_cutoff_at
                    is not None
                    and retryable_feedback_source_run_id is not None
                )
            )
        )
        if (
            retryable_feedback_rearm_detected
            and recovery_attempts
            < (
                MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                + MAX_AUTO_FEEDBACK_REARM_RUNS_PER_TRADE_DATE
            )
            and not retryable_feedback_rearm_candidate
        ):
            rearm_block = self._block_same_day_intraday_watch_rearm(
                latest_eod_run=latest_eod_run,
                reason='exact_parent_feedback_contract_unavailable',
            )
            result = build_daily_skip_result(
                reason='followup_feedback_rearm_contract_unavailable',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'attempts': recovery_attempts,
                'retryable_feedback_rearm': True,
                'retry_scheduled': False,
                'blocker': 'exact_parent_feedback_contract_unavailable',
                'resume_from_stage': incomplete_stages[0],
                'same_day_intraday_watch_rearm': rearm_block,
            }
            return result
        provider_outage_retry_after_seconds: int | None = None
        provider_outage_rearm_due = False
        provider_outage_completed_at = _parse_activity_datetime(
            pipeline_run.get('completed_at')
            or pipeline_run.get('updated_at')
        )
        if (
            provider_outage_rearm_candidate
            and provider_outage_completed_at is not None
        ):
            provider_outage_elapsed_seconds = max(
                0.0,
                (now - provider_outage_completed_at).total_seconds(),
            )
            provider_outage_retry_after_seconds = max(
                0,
                int(
                    AUTO_FOLLOWUP_PROVIDER_OUTAGE_RETRY_INTERVAL_SECONDS
                    - provider_outage_elapsed_seconds
                ),
            )
            provider_outage_rearm_due = (
                provider_outage_elapsed_seconds
                >= AUTO_FOLLOWUP_PROVIDER_OUTAGE_RETRY_INTERVAL_SECONDS
            )
        if (
            recovery_attempts
            >= MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
            and repair_checkpoint is None
            and not stage_debt_terminal_progression
            and not terminal_continuation_in_place
            and not cutoff_contract_repair
            and not cutoff_contract_repair_resume
            and not provider_outage_rearm_due
            and not retryable_feedback_rearm_candidate
            and not same_day_post_signal_continuation_in_place
        ):
            rearm_block = (
                {'updated': False, 'reason': 'provider_backoff_pending'}
                if provider_outage_rearm_candidate
                else self._block_same_day_intraday_watch_rearm(
                    latest_eod_run=latest_eod_run,
                    reason='followup_recovery_limit_reached',
                )
            )
            result = build_daily_skip_result(
                reason=(
                    'followup_recovery_provider_backoff'
                    if provider_outage_rearm_candidate
                    else 'followup_recovery_limit_reached'
                ),
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'attempts': recovery_attempts,
                'max_attempts': (
                    MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                    + (
                        MAX_AUTO_FEEDBACK_REARM_RUNS_PER_TRADE_DATE
                        if retryable_feedback_rearm_detected
                        else (1 if cutoff_contract_repair_proven else 0)
                    )
                ),
                'resume_from_stage': incomplete_stages[0],
                'same_day_intraday_watch_rearm': rearm_block,
            }
            if provider_outage_rearm_candidate:
                result['followup_recovery'].update(
                    {
                        'provider': 'dart',
                        'provider_status': 'maintenance',
                        'retry_scheduled': True,
                        'retry_after_seconds': (
                            provider_outage_retry_after_seconds
                        ),
                    }
                )
            return result

        if (
            pipeline_run.get('trigger') == AUTO_FOLLOWUP_RECOVERY_TRIGGER
            and not provider_admission_rearm
        ):
            completed_at = _parse_activity_datetime(
                pipeline_run.get('completed_at') or pipeline_run.get('updated_at')
            )
            if (
                completed_at is not None
                and (now - completed_at).total_seconds() < AUTO_FOLLOWUP_RECOVERY_MIN_INTERVAL_SECONDS
            ):
                retry_after_seconds = max(
                    1,
                    int(
                        AUTO_FOLLOWUP_RECOVERY_MIN_INTERVAL_SECONDS
                        - (now - completed_at).total_seconds()
                    ),
                )
                result = build_daily_skip_result(
                    reason='followup_recovery_backoff',
                    market_date=market_date,
                    latest_available_trade_date=latest_available_trade_date,
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'attempts': recovery_attempts,
                    'max_attempts': (
                        MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                        + (
                            MAX_AUTO_FEEDBACK_REARM_RUNS_PER_TRADE_DATE
                            if retryable_feedback_rearm_candidate
                            else (1 if cutoff_contract_repair_proven else 0)
                        )
                    ),
                    'retry_after_seconds': retry_after_seconds,
                    'resume_from_stage': incomplete_stages[0],
                }
                if retryable_feedback_rearm_candidate:
                    result['followup_recovery'].update(
                        {
                            'retry_scheduled': True,
                            'retryable_feedback_rearm': True,
                        }
                    )
                return result

        current_price_scope_preflight_required = bool(
            not stage_debt_terminal_progression
            and not terminal_continuation_in_place
            and not cutoff_contract_repair
            and not cutoff_contract_repair_resume
            and not same_day_post_signal_continuation_in_place
            and not point_in_time_recovery
            and not price_retry_symbols
            and (
                untracked_followup_parent
                or pipeline_run.get('trigger')
                == AUTO_FOLLOWUP_RECOVERY_TRIGGER
            )
        )
        if current_price_scope_preflight_required:
            price_scope_preflight = (
                self._preflight_untracked_followup_price_scope(
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    latest_eod_run=latest_eod_run,
                    markets=recovery_markets,
                )
            )
            if price_scope_preflight.get('result') is not None:
                return dict(price_scope_preflight['result'])
            verified_orphan_symbols = list(
                price_scope_preflight.get('symbols') or []
            )
            verified_downstream_symbols = list(
                price_scope_preflight.get('downstream_symbols') or []
            )
            if price_scope_preflight.get('scope_unavailable'):
                return self._record_failed_followup_scope_attempt(
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    latest_eod_run=latest_eod_run,
                    markets=recovery_markets,
                    recovery_stages=recovery_stages,
                    resume_from_stage=resume_from_stage,
                    recovery_attempts=recovery_attempts,
                    requested_symbols=(
                        price_scope_preflight.get('requested_symbols')
                        or downstream_retry_symbols
                    ),
                    point_in_time_recovery=False,
                    failure_reason=(
                        'followup_recovery_price_scope_unavailable'
                    ),
                    provider_admission_fence_owned=(
                        provider_admission_fence_owned
                    ),
                )

        if (
            not stage_debt_terminal_progression
            and not terminal_continuation_in_place
            and not cutoff_contract_repair
            and not cutoff_contract_repair_resume
            and not same_day_post_signal_continuation_in_place
            and point_in_time_recovery
            and not point_in_time_membership_symbols
        ):
            (
                point_in_time_membership_symbols,
                recovery_markets,
            ) = self._resolve_point_in_time_followup_membership(
                market_date=market_date,
                metadata=latest_eod_metadata,
                markets=recovery_markets,
                eod_run_id=int(latest_eod_run['id']),
            )
            if not point_in_time_membership_symbols:
                requested_recovery_symbols = _dedupe_symbol_codes(
                    [
                        *(latest_eod_metadata.get('requested_symbols') or []),
                        *downstream_retry_symbols,
                    ]
                )
                return self._record_failed_followup_scope_attempt(
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    latest_eod_run=latest_eod_run,
                    markets=recovery_markets,
                    recovery_stages=recovery_stages,
                    resume_from_stage=resume_from_stage,
                    recovery_attempts=recovery_attempts,
                    requested_symbols=requested_recovery_symbols,
                    point_in_time_recovery=True,
                    failure_reason=(
                        'point_in_time_recovery_scope_unavailable'
                    ),
                    provider_admission_fence_owned=(
                        provider_admission_fence_owned
                    ),
                )

        # Imported lazily because the stage orchestrator depends on this service.
        from app.eod.daily_pipeline_stages import daily_pipeline_stage_service

        if stage_debt_terminal_progression:
            resume_from_stage = TERMINAL_FOLLOWUP_CONTINUATION_STAGES[0]
            recovery_stages = list(TERMINAL_FOLLOWUP_CONTINUATION_STAGES)
        elif terminal_continuation_in_place:
            resume_from_stage = TERMINAL_FOLLOWUP_CONTINUATION_STAGES[0]
            recovery_stages = list(TERMINAL_FOLLOWUP_CONTINUATION_STAGES)
            persisted_markets = run_metadata.get('markets')
            recovery_markets = tuple(
                str(market or '').strip().upper()
                for market in persisted_markets
                if str(market or '').strip()
            )
        elif same_day_post_signal_continuation_in_place:
            resume_from_stage = (
                AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER[0]
            )
            recovery_stages = list(
                AUTO_PITR_INTRADAY_WATCH_RECOVERY_STAGE_ORDER
            )
            persisted_markets = run_metadata.get('markets')
            recovery_markets = tuple(
                str(market or '').strip().upper()
                for market in persisted_markets
                if str(market or '').strip()
            )
        elif (
            retryable_feedback_rearm_candidate
            and not proven_partial_summary_downstream_continuation
            and feedback_retry_stage_id == TERMINAL_FOLLOWUP_CONTINUATION_STAGES[0]
            and resume_from_stage != TERMINAL_FOLLOWUP_CONTINUATION_STAGES[0]
            and TERMINAL_FOLLOWUP_CONTINUATION_STAGES[0] in recovery_stages
        ):
            # The parent feedback run already passed its prerequisite gate on
            # these inputs, and the rearm inherits that run's cutoff and scope.
            # A still-partial upstream stage (signal_features is partial most
            # days) must not widen the rearm: the stage executor accepts an
            # inherited feedback config only on the exact feedback suffix, and
            # anything wider raised on every tick (2026-11-09).
            resume_from_stage = TERMINAL_FOLLOWUP_CONTINUATION_STAGES[0]
            recovery_stages = list(TERMINAL_FOLLOWUP_CONTINUATION_STAGES)

        market_risk_recovery_scope_outcome: dict[str, Any] | None = None
        recovery_price_scope_outcome: dict[str, Any] | None = None
        if (
            point_in_time_recovery
            and point_in_time_membership_symbols
            and 'trending_ranks' in recovery_stages
        ):
            recovery_price_scope_outcome = (
                _build_recovery_price_scope_outcome(
                    latest_eod_run=latest_eod_run,
                    market_date=market_date,
                    membership_symbols=point_in_time_membership_symbols,
                    allow_terminal_failures=(
                        recovering_safe_downstream_scope
                    ),
                )
            )
            if recovery_price_scope_outcome is None:
                result = build_daily_skip_result(
                    reason='followup_recovery_price_scope_unavailable',
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'status': 'blocked',
                    'retry_scheduled': False,
                    'resume_from_stage': resume_from_stage,
                    'blocker': 'historical_price_scope_mismatch',
                }
                return result
        targeted_market_risk_recovery = bool(
            stage_debt_request is None
            and point_in_time_recovery
            and resume_from_stage == 'market_risk_filter'
            and recovery_stages[:1] == ['market_risk_filter']
            and downstream_retry_symbols
        )
        if targeted_market_risk_recovery:
            admission_partition = (
                _validated_followup_admission_partition(
                    latest_eod_metadata
                )
            )
            exact_membership = sorted(
                point_in_time_membership_symbols or []
            )
            if (
                admission_partition is None
                or not exact_membership
                or set(
                    admission_partition['selected_stock_codes']
                )
                != set(exact_membership)
                or set(
                    admission_partition['retry_debt_stock_codes']
                )
                != set(downstream_retry_symbols)
            ):
                result = build_daily_skip_result(
                    reason='followup_recovery_exact_scope_unavailable',
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'status': 'blocked',
                    'retry_scheduled': False,
                    'resume_from_stage': resume_from_stage,
                    'blocker': (
                        'market_risk_admission_partition_mismatch'
                    ),
                }
                return result
            market_risk_recovery_scope_outcome = {
                'schema_version': (
                    FOLLOWUP_STAGE_RECOVERY_SCOPE_SCHEMA_VERSION
                ),
                'stage_id': 'market_risk_filter',
                **build_exact_symbol_scope_outcome(
                    manifest_version=(
                        'market_risk_filter_scope_outcomes_v1'
                    ),
                    selected_symbols=admission_partition[
                        'selected_stock_codes'
                    ],
                    successful_symbols=admission_partition[
                        'successful_stock_codes'
                    ],
                    retry_debt_symbols=admission_partition[
                        'retry_debt_stock_codes'
                    ],
                    terminal_not_applicable_symbols=admission_partition[
                        'terminal_not_applicable_stock_codes'
                    ],
                ),
            }

        feedback_input_stage_ids = set(
            FEEDBACK_DATA_PREREQUISITE_STAGE_ORDER
        )
        if not point_in_time_recovery:
            feedback_input_stage_ids.add('signal_features')
            feedback_input_stage_ids.update(
                ORDERING_ONLY_OPTIONAL_CONTEXT_STAGE_IDS
            )
        rebuilds_feedback_inputs = bool(
            'agent_feedback_summaries' in recovery_stages
            and feedback_input_stage_ids.intersection(recovery_stages)
        )
        force_feedback_recompute = bool(
            ordering_repair_stage is not None
            or rebuilds_feedback_inputs
        )

        recovery_kwargs: dict[str, Any] = {
            'market_date': market_date,
            'stages': recovery_stages,
            'markets': recovery_markets,
            'force': force_feedback_recompute,
            'reuse_existing_feedback': not force_feedback_recompute,
            'resume_from_stage': resume_from_stage,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'reused_eod_collection_run_id': int(latest_eod_run['id']),
            'provider_admission_fence_owned': (
                provider_admission_fence_owned
            ),
        }
        if market_risk_recovery_scope_outcome is not None:
            recovery_kwargs['recovery_stage_scope_outcome'] = (
                market_risk_recovery_scope_outcome
            )
        if recovery_price_scope_outcome is not None:
            recovery_kwargs['recovery_price_scope_outcome'] = (
                recovery_price_scope_outcome
            )
        if (
            retryable_feedback_rearm_candidate
            and not proven_partial_summary_downstream_continuation
        ):
            recovery_kwargs['feedback_max_cycles'] = (
                retryable_feedback_max_cycles
            )
            if feedback_retry_stage_id == 'signal_decider':
                recovery_kwargs['fs_div'] = str(
                    summary_cutoff_proof['fs_div']
                )
            else:
                # Historical suffix-only retries reuse the parent's
                # prerequisite admission because their completed prefix is
                # no longer the latest pipeline row. Same-day retries keep
                # the live gate.
                if point_in_time_recovery:
                    recovery_kwargs[
                        'inherit_feedback_prerequisite_admission'
                    ] = True
                recovery_kwargs[
                    'inherited_feedback_knowledge_cutoff_at'
                ] = retryable_feedback_knowledge_cutoff_at
                recovery_kwargs['inherited_feedback_source_run_id'] = (
                    retryable_feedback_source_run_id
                )
                recovery_kwargs['feedback_outer_batch_size'] = int(
                    retryable_feedback_rearm_config['outer_batch_size']
                )
                recovery_kwargs['fs_div'] = str(
                    retryable_feedback_rearm_config['fs_div']
                )
                recovery_kwargs[
                    'inherited_feedback_retry_config'
                ] = dict(retryable_feedback_rearm_config)
        if (
            summary_cutoff_proof is not None
            and 'signal_decider' in recovery_stages
            and 'agent_feedback_summaries' not in recovery_stages
        ):
            recovery_kwargs['summary_cutoff_proof'] = summary_cutoff_proof
        if cutoff_contract_repair or cutoff_contract_repair_resume:
            recovery_kwargs['summary_cutoff_contract_repair'] = True
        if cutoff_contract_repair_resume:
            recovery_kwargs['existing_pipeline_run_id'] = int(
                pipeline_run['id']
            )
        if terminal_continuation_in_place:
            recovery_kwargs['existing_pipeline_run_id'] = int(
                pipeline_run['id']
            )
        if same_day_post_signal_continuation_in_place:
            recovery_kwargs.update(
                {
                    'existing_pipeline_run_id': int(pipeline_run['id']),
                    'same_day_intraday_watch_rearm_resume': True,
                    'fs_div': str(summary_cutoff_proof['fs_div']),
                }
            )
        persisted_point_in_time_recovery = bool(
            run_metadata.get('point_in_time_recovery')
        )
        if stage_debt_request is not None and point_in_time_recovery:
            recovery_kwargs['point_in_time_recovery'] = True
        elif point_in_time_recovery or (
            persisted_point_in_time_recovery
            and (
                terminal_continuation_in_place
                or recoverable_same_day_expiration
                or pending_same_day_rearm is not None
            )
        ):
            recovery_kwargs['point_in_time_recovery'] = True
        stage_debt_membership_symbols = (
            _validated_followup_admitted_symbols(latest_eod_metadata)
            if stage_debt_request is not None
            else None
        )
        if stage_debt_request is not None:
            if not stage_debt_membership_symbols:
                result = build_daily_skip_result(
                    reason='followup_stage_debt_scope_unavailable',
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'status': 'blocked',
                    'retry_scheduled': False,
                    'resume_from_stage': stage_debt_request[0],
                    'blocker': 'frozen_stage_membership_unavailable',
                    'stage_debt_preserved': True,
                }
                return result
            recovery_kwargs['exact_membership_symbols'] = sorted(
                stage_debt_membership_symbols
            )
            parent_scope_outcome = latest_eod_metadata.get(
                'followup_admission_scope_outcome'
            )
            if isinstance(parent_scope_outcome, Mapping):
                recovery_kwargs['recovery_parent_scope_outcome'] = dict(
                    parent_scope_outcome
                )
            if stage_debt_scope_outcome is not None:
                recovery_kwargs['recovery_stage_scope_outcome'] = dict(
                    stage_debt_scope_outcome
                )
        elif point_in_time_recovery and point_in_time_membership_symbols:
            recovery_kwargs['exact_membership_symbols'] = sorted(
                point_in_time_membership_symbols
            )
            parent_scope_outcome = latest_eod_metadata.get(
                'followup_admission_scope_outcome'
            )
            if (
                not isinstance(parent_scope_outcome, Mapping)
                and recovery_price_scope_outcome is not None
            ):
                parent_scope_outcome = build_exact_symbol_scope_outcome(
                    manifest_version=(
                        'auto_eod_followup_admission_scope_outcomes_v1'
                    ),
                    selected_symbols=recovery_price_scope_outcome[
                        'selected_stock_codes'
                    ],
                    successful_symbols=recovery_price_scope_outcome[
                        'successful_stock_codes'
                    ],
                    retry_debt_symbols=recovery_price_scope_outcome[
                        'retry_debt_stock_codes'
                    ],
                    terminal_not_applicable_symbols=(
                        recovery_price_scope_outcome.get(
                            'terminal_not_applicable_stock_codes'
                        )
                        or []
                    ),
                )
            if isinstance(parent_scope_outcome, Mapping):
                recovery_kwargs['recovery_parent_scope_outcome'] = dict(
                    parent_scope_outcome
                )
        # A later follow-up can fail independently of the immediate PyKRX
        # stages, so a healthy collection resumes the full universe. When a
        # price failure coexists, however, an unscoped resume would include a
        # symbol with no same-day bar; use the durable downstream-safe scope.
        has_incomplete_followup_stage = any(
            stage_id in AUTO_DAILY_FOLLOWUP_STAGE_ORDER
            for stage_id in incomplete_stages
        )
        if stage_debt_terminal_progression:
            recovery_symbols = list(
                stage_debt_terminal_progression_symbols
            )
        elif stage_debt_request is not None:
            recovery_symbols = list(stage_debt_request[1])
        elif terminal_continuation_in_place:
            recovery_symbols = _dedupe_symbol_codes(
                run_metadata.get('symbols') or []
            )
        elif same_day_post_signal_continuation_in_place:
            recovery_symbols = []
        elif proven_partial_summary_downstream_continuation:
            # The durable summary proof already pins the exact all-role
            # success intersection.  Keep failed summarizer symbols as retry
            # debt, but let the successful cohort enter the decider suffix.
            # Passing the failed-symbol retry config or symbol list here
            # would either violate the suffix contract or replace the proven
            # success scope with the failed scope.
            recovery_symbols = []
        elif retryable_feedback_rearm_candidate:
            recovery_symbols = (
                []
                if feedback_retry_stage_id == 'signal_decider'
                else list(
                    retryable_feedback_rearm_config[
                        'requested_symbols'
                    ]
                )
            )
        elif cutoff_contract_repair or cutoff_contract_repair_resume:
            # The decider stage resolves and locks the exact completed
            # summary scope. Rebuilding a historical market membership here
            # can neither strengthen that proof nor safely broaden it.
            recovery_symbols = []
        elif (
            dart_recovery_scope.get('required') is True
            and dart_recovery_scope.get('ready') is True
        ):
            recovery_symbols = list(dart_recovery_scope['symbols'])
        elif (
            point_in_time_recovery
            and upstream_admission_rebuild_pending
            and downstream_resume_from_stage == 'pykrx_enrichment'
            and not price_retry_symbols
            and point_in_time_membership_symbols
        ):
            # Admission-producing stages must see the full frozen membership.
            # Passing only the stale retry subset makes exact downstream
            # publishers classify every other frozen member as failed.
            recovery_symbols = sorted(point_in_time_membership_symbols)
        elif point_in_time_recovery and downstream_retry_symbols:
            recovery_symbols = downstream_retry_symbols
        elif recovering_safe_downstream_scope:
            recovery_symbols = downstream_retry_symbols
        elif point_in_time_recovery:
            requested_recovery_symbols = _dedupe_symbol_codes(
                latest_eod_metadata.get('requested_symbols') or []
            )
            recovery_symbols = (
                requested_recovery_symbols
                or sorted(point_in_time_membership_symbols)
            )
        elif verified_downstream_symbols:
            recovery_symbols = verified_downstream_symbols
        elif verified_orphan_symbols:
            recovery_symbols = verified_orphan_symbols
        elif recovery_reaches_followup_stage and not price_retry_symbols:
            recovery_symbols = []
        elif run_metadata.get('synthetic_recovery_parent'):
            recovery_symbols = downstream_retry_symbols
        elif has_incomplete_followup_stage:
            recovery_symbols = (
                downstream_retry_symbols if price_retry_symbols else []
            )
        else:
            recovery_symbols = downstream_retry_symbols
        if recovery_symbols:
            recovery_kwargs['symbols'] = recovery_symbols
        if (
            recoverable_same_day_expiration
            and prior_signal_rearm_evidence_used
            and retryable_feedback_rearm_candidate
        ):
            try:
                rearm_marker_result = (
                    self._persist_same_day_intraday_watch_rearm_pending(
                        latest_eod_run=latest_eod_run,
                        source_false_skip_run_id=int(pipeline_run['id']),
                        proof_pipeline_run_id=int(
                            feedback_rearm_evidence_run['id']
                        ),
                        target_trade_date=current_krx_calendar_date,
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive guard
                logger.exception(
                    'Unable to persist same-day intraday-watch rearm for %s',
                    market_date,
                )
                rearm_marker_result = {
                    'updated': False,
                    'reason': type(exc).__name__,
                }
            if rearm_marker_result.get('reason') not in {
                'rearm_marker_persisted',
                'rearm_marker_already_pending',
            }:
                result = build_daily_skip_result(
                    reason='followup_rearm_marker_unavailable',
                    market_date=market_date,
                    latest_available_trade_date=(
                        latest_available_trade_date
                    ),
                    existing_run=latest_eod_run,
                )
                result['followup_recovery'] = {
                    'status': 'state_unavailable',
                    'marker_reason': rearm_marker_result.get('reason'),
                    'resume_from_stage': incomplete_stages[0],
                }
                return result
        terminal_rearm_expiry_block: dict[str, Any] | None = None
        try:
            recovery_result = dict(
                daily_pipeline_stage_service.run(**recovery_kwargs)
            )
        except ValueError as exc:
            pending_rearm_before_call = (
                pending_same_day_intraday_watch_rearm_marker(
                    (
                        latest_eod_run.get('run_metadata') or {}
                    ).get(SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY),
                    target_trade_date=current_krx_calendar_date,
                )
                if isinstance(latest_eod_run.get('run_metadata'), Mapping)
                else None
            )
            if not (
                same_day_post_signal_continuation_in_place
                and pending_rearm_before_call is not None
                and str(exc)
                == (
                    'same-day intraday-watch rearm resume is outside its '
                    'target PREOPEN/OPEN session'
                )
            ):
                raise
            terminal_rearm_expiry_block = (
                self._block_same_day_intraday_watch_rearm(
                    latest_eod_run=latest_eod_run,
                    reason='next_open_window_expired',
                )
            )
            recovery_result = build_daily_skip_result(
                reason='next_open_window_expired',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            recovery_result.update(
                {
                    'status': 'skipped',
                    'pipeline_run_id': pipeline_run.get('id'),
                }
            )
        stage_debt_convergence: dict[str, Any] | None = None
        stage_debt_transition_error: str | None = None
        if stage_debt_request is not None:
            debt_stage_id, debt_symbols = stage_debt_request
            recovery_stage_rows = recovery_result.get('stages')
            recovery_stage = (
                recovery_stage_rows.get(debt_stage_id)
                if isinstance(recovery_stage_rows, Mapping)
                else None
            )
            recovery_pipeline_run_id = recovery_result.get(
                'pipeline_run_id'
            )
            recovery_pipeline_run = recovery_result.get('pipeline_run')
            recovery_run_metadata = (
                recovery_pipeline_run.get('run_metadata')
                if isinstance(recovery_pipeline_run, Mapping)
                and isinstance(
                    recovery_pipeline_run.get('run_metadata'), Mapping
                )
                else {}
            )
            resolved_recovery_stages = recovery_result.get(
                'resolved_stages'
            )
            if (
                isinstance(recovery_pipeline_run_id, bool)
                or not isinstance(recovery_pipeline_run_id, int)
                or recovery_pipeline_run_id <= 0
                or not isinstance(recovery_pipeline_run, Mapping)
                or recovery_pipeline_run.get('id')
                != recovery_pipeline_run_id
                or recovery_run_metadata.get(
                    'reused_eod_collection_run_id'
                )
                != latest_eod_run.get('id')
                or str(recovery_result.get('market_date') or '')
                != str(market_date)
                or resolved_recovery_stages != [debt_stage_id]
                or recovery_run_metadata.get('resolved_stages')
                != [debt_stage_id]
            ):
                advanced_stage_debt = None
                stage_debt_transition_error = (
                    'recovery_pipeline_run_lineage_invalid'
                )
            else:
                advanced_stage_debt = _advance_followup_stage_debt_state(
                    stage_debt_state or {},
                    stage_id=debt_stage_id,
                    invocation_symbols=debt_symbols,
                    stage_result=(
                        recovery_stage
                        if isinstance(recovery_stage, Mapping)
                        else {}
                    ),
                    recovery_pipeline_run_id=recovery_pipeline_run_id,
                )
                if advanced_stage_debt is None and (
                    _followup_recovery_stage_attempt_finished(
                        recovery_stage,
                        expected_stage_id=debt_stage_id,
                    )
                ):
                    advanced_stage_debt = (
                        _record_followup_stage_debt_attempt(
                            stage_debt_state or {},
                            stage_id=debt_stage_id,
                            invocation_symbols=debt_symbols,
                            recovery_pipeline_run_id=(
                                recovery_pipeline_run_id
                            ),
                        )
                    )
                    if advanced_stage_debt is not None:
                        stage_debt_transition_error = (
                            'recovery_stage_exact_manifest_invalid'
                        )
            if advanced_stage_debt is None:
                stage_debt_transition_error = (
                    stage_debt_transition_error
                    or 'recovery_stage_exact_manifest_invalid'
                )
            else:
                stage_debt_convergence = (
                    self._persist_followup_stage_debt_state(
                        latest_eod_run=latest_eod_run,
                        state=advanced_stage_debt,
                    )
                )
                if not stage_debt_convergence.get('updated'):
                    stage_debt_transition_error = str(
                        stage_debt_convergence.get('reason')
                        or 'followup_stage_debt_persist_failed'
                    )
        elif (
            not stage_debt_terminal_progression
            and recovery_symbols
            and daily_pipeline_outcome_is_satisfied(
                recovery_result
            )
        ):
            recovery_result[
                '_proven_resolved_downstream_symbols'
            ] = list(recovery_symbols)
        pending_rearm_after_recovery = (
            pending_same_day_intraday_watch_rearm_marker(
                (
                    latest_eod_run.get('run_metadata') or {}
                ).get(SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY),
                target_trade_date=current_krx_calendar_date,
            )
            if isinstance(latest_eod_run.get('run_metadata'), Mapping)
            else None
        )
        if (
            terminal_rearm_expiry_block is None
            and pending_rearm_after_recovery is not None
            and outcome_contains_reason(
                recovery_result,
                reason='next_open_window_expired',
            )
        ):
            terminal_rearm_expiry_block = (
                self._block_same_day_intraday_watch_rearm(
                    latest_eod_run=latest_eod_run,
                    reason='next_open_window_expired',
                )
            )
        pipeline_run_id = recovery_result.get('pipeline_run_id')
        nonblocking_partial_marker = (
            validated_nonblocking_partial_progression_marker(
                recovery_result,
                expected_eod_collection_run_id=int(
                    latest_eod_run['id']
                ),
            )
            if type(latest_eod_run.get('id')) is int
            else None
        )
        if (
            terminal_rearm_expiry_block is not None
        ):
            pipeline_convergence = {
                'updated': False,
                'pipeline_run_id': pipeline_run_id,
                'status': recovery_result.get('status'),
                'reason': 'same_day_intraday_watch_rearm_terminal_expiry',
            }
        elif nonblocking_partial_marker is not None:
            # The shared validator proves that every failed role/symbol pair
            # was excluded before the execution-plan tail. The run remains
            # truthfully partial, so the generic completed-only convergence
            # helper must not be used here.
            pipeline_convergence = {
                'updated': False,
                'pipeline_run_id': pipeline_run_id,
                'status': 'partial',
                'reason': 'validated_nonblocking_partial_progression',
            }
        elif (
            pipeline_run_id is not None
            and daily_pipeline_outcome_is_satisfied(recovery_result)
        ):
            pipeline_convergence = (
                daily_pipeline_run_state_service
                .converge_terminal_stages_run(
                    pipeline_run_id=int(pipeline_run_id),
                    allow_satisfied_terminal=True,
                )
            )
        else:
            pipeline_convergence = {
                'updated': False,
                'pipeline_run_id': pipeline_run_id,
                'status': recovery_result.get('status'),
                'reason': 'recovery_parent_not_satisfied',
            }
        pipeline_parent_finalized = bool(
            pipeline_convergence.get('updated')
            or pipeline_convergence.get('reason')
            == 'already_non_resumable'
            or pipeline_convergence.get('reason')
            == 'validated_nonblocking_partial_progression'
        )
        if terminal_rearm_expiry_block is not None:
            convergence = {
                'updated': bool(
                    terminal_rearm_expiry_block.get('updated')
                ),
                'reason': 'same_day_intraday_watch_rearm_terminal_expiry',
                'same_day_intraday_watch_rearm': (
                    terminal_rearm_expiry_block
                ),
            }
        elif stage_debt_request is not None:
            convergence = (
                dict(stage_debt_convergence)
                if stage_debt_convergence is not None
                else {
                    'updated': False,
                    'reason': (
                        stage_debt_transition_error
                        or 'followup_stage_debt_not_advanced'
                    ),
                }
            )
        elif stage_debt_terminal_progression:
            convergence = (
                self._persist_followup_stage_debt_terminal_progression(
                    latest_eod_run=latest_eod_run,
                    state=stage_debt_state or {},
                    cohort_basis=(
                        stage_debt_terminal_progression_basis or ''
                    ),
                    cohort_symbols=(
                        stage_debt_terminal_progression_symbols
                    ),
                    recovery_result=recovery_result,
                )
                if pipeline_parent_finalized
                else {
                    'updated': False,
                    'reason': 'recovery_parent_not_finalized',
                }
            )
        elif pipeline_parent_finalized:
            convergence = self._persist_successful_followup_recovery(
                latest_eod_run=latest_eod_run,
                recovery_result=recovery_result,
            )
        else:
            convergence = {
                'updated': False,
                'reason': 'recovery_parent_not_finalized',
            }
        convergence['pipeline_run'] = pipeline_convergence
        recovery_result.pop(
            '_proven_resolved_downstream_symbols',
            None,
        )
        recovery_result.setdefault('triggered', True)
        recovery_result.setdefault(
            'reason',
            (
                'stage_debt_success_cohort_resumed'
                if stage_debt_terminal_progression
                else (
                    'terminal_followup_continuation_resumed'
                    if terminal_continuation_in_place
                    else 'incomplete_followups_resumed'
                )
            ),
        )
        recovery_result.setdefault('market_date', market_date)
        recovery_result['latest_available_trade_date'] = latest_available_trade_date
        recovery_result['source'] = 'pykrx'
        recovery_result['followup_recovery'] = {
            'attempt': (
                recovery_attempts
                if stage_debt_terminal_progression
                or terminal_continuation_in_place
                or cutoff_contract_repair_resume
                or same_day_post_signal_continuation_in_place
                else recovery_attempts + 1
            ),
            'max_attempts': (
                max(
                    recovery_attempts,
                    MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                    + (1 if repair_checkpoint is not None else 0),
                )
                if terminal_continuation_in_place
                else (
                    SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT
                    if same_day_post_signal_continuation_in_place
                    else max(
                        MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                        + (
                            MAX_AUTO_FEEDBACK_REARM_RUNS_PER_TRADE_DATE
                            if retryable_feedback_rearm_candidate
                            else (
                                1
                                if repair_checkpoint is not None
                                or cutoff_contract_repair
                                or cutoff_contract_repair_resume
                                else 0
                            )
                        ),
                        recovery_attempts + 1
                        if provider_outage_rearm_due
                        or retryable_feedback_rearm_candidate
                        else 0,
                    )
                )
            ),
            'resumed_from_pipeline_run_id': pipeline_run.get('id'),
            'reused_eod_collection_run_id': latest_eod_run.get('id'),
            'resume_from_stage': resume_from_stage,
            'stages': recovery_stages,
            'symbols': recovery_symbols,
            'state_convergence': convergence,
        }
        if stage_debt_request is not None:
            next_stage_debt_request = (
                stage_debt_convergence.get('next_request')
                if stage_debt_convergence is not None
                else stage_debt_request
            )
            recovery_result['followup_recovery'].update(
                {
                    'stage_specific_recovery': True,
                    'stage_debt_preserved': bool(
                        next_stage_debt_request
                    ),
                    'next_stage_debt_request': (
                        next_stage_debt_request
                    ),
                }
            )
            if stage_debt_transition_error is not None:
                recovery_result['followup_recovery'][
                    'stage_debt_transition_error'
                ] = stage_debt_transition_error
        if stage_debt_terminal_progression:
            recovery_result['followup_recovery'].update(
                {
                    'stage_debt_success_cohort_progression': True,
                    'stage_debt_preserved': True,
                    'cohort_basis': (
                        stage_debt_terminal_progression_basis
                    ),
                    'cohort_symbol_count': len(
                        stage_debt_terminal_progression_symbols
                    ),
                    'deferred_stage_debt_request': (
                        deferred_stage_debt_request
                    ),
                }
            )
        if provider_outage_rearm_due:
            recovery_result['followup_recovery'].update(
                {
                    'provider_outage_rearm': True,
                    'provider': 'dart',
                    'provider_status': 'maintenance',
                    'retry_interval_seconds': (
                        AUTO_FOLLOWUP_PROVIDER_OUTAGE_RETRY_INTERVAL_SECONDS
                    ),
                }
            )
        if provider_admission_rearm:
            recovery_result['followup_recovery'].update(
                {
                    'provider_admission_rearm': True,
                    'provider': required_provider_admission['provider'],
                    'nonconsuming_provider_admission_runs': (
                        nonconsuming_provider_admission_runs
                    ),
                    'provider_admission': required_provider_admission,
                }
            )
        if dart_recovery_scope.get('required') is True:
            recovery_result['followup_recovery'][
                'symbol_scope_source'
            ] = dart_recovery_scope.get('source')
        if retryable_feedback_rearm_candidate:
            recovery_result['followup_recovery'][
                'retryable_feedback_rearm'
            ] = True
            if prior_signal_rearm_evidence_used:
                recovery_result['followup_recovery'][
                    'feedback_rearm_evidence_pipeline_run_id'
                ] = feedback_rearm_evidence_run.get('id')
        if ordering_repair_stage is not None:
            recovery_result['followup_recovery'][
                'ordering_repair'
            ] = (
                'signal_feature_prerequisite_completed_after_signal_started'
                if ordering_repair_stage == 'signal_features'
                else 'feedback_prerequisite_completed_after_feedback_started'
            )
        if terminal_continuation_in_place:
            recovery_result['followup_recovery'][
                'terminal_continuation_in_place'
            ] = True
        if same_day_post_signal_continuation_in_place:
            recovery_result['followup_recovery'][
                'same_day_intraday_watch_rearm_resumed_in_place'
            ] = True
        if terminal_rearm_expiry_block is not None:
            recovery_result['followup_recovery'][
                'same_day_intraday_watch_rearm'
            ] = terminal_rearm_expiry_block
            recovery_result['followup_recovery'][
                'same_day_intraday_watch_rearm_terminal_expiry'
            ] = True
        if cutoff_contract_repair:
            recovery_result['followup_recovery'][
                'cutoff_contract_repair'
            ] = True
        if cutoff_contract_repair_resume:
            recovery_result['followup_recovery'][
                'cutoff_contract_repair_resumed_in_place'
            ] = True
        if repair_checkpoint is not None:
            recovery_result['followup_recovery'][
                'repair_checkpoint_pipeline_run_id'
            ] = repair_checkpoint.get('pipeline_run_id')
        return recovery_result

    @staticmethod
    def _persist_same_day_intraday_watch_rearm_pending(
        *,
        latest_eod_run: dict[str, Any],
        source_false_skip_run_id: int,
        proof_pipeline_run_id: int,
        target_trade_date: date,
    ) -> dict[str, Any]:
        """Durably preserve the bounded retry authority rescued from a false skip."""

        run_id = int(latest_eod_run['id'])
        source_run_id = int(source_false_skip_run_id)
        proof_run_id = int(proof_pipeline_run_id)
        if min(run_id, source_run_id, proof_run_id) <= 0:
            return {'updated': False, 'reason': 'invalid_rearm_lineage'}

        metadata = (
            dict(latest_eod_run.get('run_metadata') or {})
            if isinstance(latest_eod_run.get('run_metadata'), Mapping)
            else {}
        )
        expected_identity = {
            'version': SAME_DAY_INTRADAY_WATCH_REARM_VERSION,
            'source_false_skip_run_id': source_run_id,
            'proof_pipeline_run_id': proof_run_id,
            'target_trade_date': target_trade_date.isoformat(),
            'attempt_limit': SAME_DAY_INTRADAY_WATCH_REARM_ATTEMPT_LIMIT,
        }
        existing_marker = metadata.get(
            SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
        )
        if existing_marker is not None:
            if not (
                isinstance(existing_marker, Mapping)
                and str(existing_marker.get('status') or '').strip()
                == 'pending'
                and all(
                    existing_marker.get(key) == value
                    for key, value in expected_identity.items()
                )
                and _parse_activity_datetime(existing_marker.get('started_at'))
                is not None
            ):
                return {'updated': False, 'reason': 'rearm_marker_conflict'}
            marker = dict(existing_marker)
        else:
            marker = {
                **expected_identity,
                'status': 'pending',
                'started_at': _utcnow_iso(),
            }

        metadata.update(
            {
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY: marker,
                'followup_recovery_required': True,
            }
        )
        update_eod_collection_run_progress(
            run_id,
            symbols_succeeded=max(
                int(latest_eod_run.get('symbols_succeeded') or 0),
                0,
            ),
            symbols_failed=max(
                int(latest_eod_run.get('symbols_failed') or 0),
                0,
            ),
            run_metadata=metadata,
        )
        latest_eod_run['run_metadata'] = metadata
        return {
            'updated': existing_marker is None,
            'reason': (
                'rearm_marker_persisted'
                if existing_marker is None
                else 'rearm_marker_already_pending'
            ),
            'marker': marker,
        }

    @staticmethod
    def _block_same_day_intraday_watch_rearm(
        *,
        latest_eod_run: dict[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        """Close only the incident-specific rearm lane after a terminal block."""

        metadata = (
            dict(latest_eod_run.get('run_metadata') or {})
            if isinstance(latest_eod_run.get('run_metadata'), Mapping)
            else {}
        )
        marker = metadata.get(SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY)
        if not (
            isinstance(marker, Mapping)
            and marker.get('version')
            == SAME_DAY_INTRADAY_WATCH_REARM_VERSION
            and str(marker.get('status') or '').strip() == 'pending'
        ):
            return {'updated': False, 'reason': 'rearm_marker_not_pending'}

        blocked_marker = {
            **dict(marker),
            'status': 'blocked',
            'blocked_reason': str(reason or '').strip() or 'unknown',
            'blocked_at': _utcnow_iso(),
        }
        metadata.update(
            {
                SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY: blocked_marker,
                'followup_recovery_required': False,
            }
        )
        try:
            update_eod_collection_run_progress(
                int(latest_eod_run['id']),
                symbols_succeeded=max(
                    int(latest_eod_run.get('symbols_succeeded') or 0),
                    0,
                ),
                symbols_failed=max(
                    int(latest_eod_run.get('symbols_failed') or 0),
                    0,
                ),
                run_metadata=metadata,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.exception(
                'Unable to persist terminal same-day rearm block for EOD %s',
                latest_eod_run.get('id'),
            )
            return {
                'updated': False,
                'reason': 'rearm_marker_persist_failed',
                'error_type': type(exc).__name__,
            }
        latest_eod_run['run_metadata'] = metadata
        return {
            'updated': True,
            'reason': 'rearm_marker_blocked',
            'marker': blocked_marker,
        }

    def _persist_followup_stage_debt_state(
        self,
        *,
        latest_eod_run: Mapping[str, Any],
        state: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist exact per-stage debt without clearing unrelated debt."""

        metadata = (
            dict(latest_eod_run.get('run_metadata') or {})
            if isinstance(latest_eod_run.get('run_metadata'), Mapping)
            else {}
        )
        admission_partition = _validated_followup_admission_partition(
            metadata
        )
        parent_symbols = (
            list(admission_partition['successful_stock_codes'])
            if admission_partition is not None
            else []
        )
        publication_symbols = (
            list(admission_partition['selected_stock_codes'])
            if admission_partition is not None
            else []
        )
        if not parent_symbols or type(latest_eod_run.get('id')) is not int:
            return {
                'updated': False,
                'reason': 'followup_stage_debt_parent_scope_unavailable',
            }
        validated_state = _validated_followup_stage_debt_state(
            state,
            market_date=str(latest_eod_run.get('trade_date') or ''),
            eod_collection_run_id=int(latest_eod_run['id']),
            parent_symbols=parent_symbols,
            publication_symbols=publication_symbols,
            require_pending=False,
        )
        if validated_state is None:
            return {
                'updated': False,
                'reason': 'followup_stage_debt_state_invalid',
            }
        # Keep the resolved proof ledger as a tombstone.  The original EOD
        # stage_progress remains immutable; deleting this state would let a
        # later scheduler tick re-seed already-resolved debt from that older
        # partial manifest.
        metadata[FOLLOWUP_STAGE_DEBT_METADATA_KEY] = validated_state
        metadata['followup_recovery_required'] = bool(
            _next_followup_stage_debt_request(validated_state)
            or metadata.get('downstream_retry_symbols')
            or metadata.get('inherited_downstream_retry_symbols')
        )
        try:
            update_eod_collection_run_progress(
                int(latest_eod_run['id']),
                symbols_succeeded=max(
                    int(latest_eod_run.get('symbols_succeeded') or 0),
                    0,
                ),
                symbols_failed=max(
                    int(latest_eod_run.get('symbols_failed') or 0),
                    0,
                ),
                run_metadata=metadata,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.exception(
                'Unable to persist follow-up stage debt for EOD %s',
                latest_eod_run.get('id'),
            )
            return {
                'updated': False,
                'reason': 'followup_stage_debt_persist_failed',
                'error_type': type(exc).__name__,
            }
        if isinstance(latest_eod_run, dict):
            latest_eod_run['run_metadata'] = metadata
        return {
            'updated': True,
            'reason': 'followup_stage_debt_persisted',
            'next_request': _next_followup_stage_debt_request(
                validated_state
            ),
        }

    def _persist_followup_stage_debt_terminal_progression(
        self,
        *,
        latest_eod_run: Mapping[str, Any],
        state: Mapping[str, Any],
        cohort_basis: str,
        cohort_symbols: Sequence[str],
        recovery_result: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Checkpoint a proven terminal suffix without clearing root debt."""

        metadata = (
            dict(latest_eod_run.get('run_metadata') or {})
            if isinstance(latest_eod_run.get('run_metadata'), Mapping)
            else {}
        )
        admission_partition = _validated_followup_admission_partition(
            metadata
        )
        parent_symbols = (
            list(admission_partition['successful_stock_codes'])
            if admission_partition is not None
            else []
        )
        publication_symbols = (
            list(admission_partition['selected_stock_codes'])
            if admission_partition is not None
            else []
        )
        eod_collection_run_id = latest_eod_run.get('id')
        market_date = str(latest_eod_run.get('trade_date') or '')
        if (
            not parent_symbols
            or isinstance(eod_collection_run_id, bool)
            or not isinstance(eod_collection_run_id, int)
            or eod_collection_run_id <= 0
        ):
            return {
                'updated': False,
                'reason': 'followup_stage_debt_parent_scope_unavailable',
            }
        validated_state = _validated_followup_stage_debt_state(
            state,
            market_date=market_date,
            eod_collection_run_id=eod_collection_run_id,
            parent_symbols=parent_symbols,
            publication_symbols=publication_symbols,
            require_pending=False,
        )
        expected_cohort = _followup_stage_debt_terminal_cohort(
            validated_state or {},
            point_in_time_recovery=(
                cohort_basis == 'factor_intersection'
            ),
            require_prior_attempts=False,
        )
        normalized_cohort = sorted(_dedupe_symbol_codes(cohort_symbols))
        if (
            validated_state is None
            or expected_cohort is None
            or expected_cohort[0] != cohort_basis
            or not normalized_cohort
            or not set(normalized_cohort).issubset(set(expected_cohort[1]))
        ):
            return {
                'updated': False,
                'reason': 'followup_stage_debt_terminal_cohort_invalid',
            }
        proof = _validated_followup_stage_debt_terminal_progression_result(
            recovery_result,
            market_date=market_date,
            eod_collection_run_id=eod_collection_run_id,
            cohort_symbols=normalized_cohort,
        )
        if proof is None:
            return {
                'updated': False,
                'reason': 'followup_stage_debt_terminal_result_invalid',
            }

        raw_marker = metadata.get(
            FOLLOWUP_STAGE_DEBT_TERMINAL_PROGRESSION_METADATA_KEY
        )
        if raw_marker is None:
            existing_marker = None
            processed_symbols: list[str] = []
            batches: list[dict[str, Any]] = []
        else:
            existing_marker = (
                _validated_followup_stage_debt_terminal_progression_marker(
                    raw_marker,
                    state=validated_state,
                    market_date=market_date,
                    eod_collection_run_id=eod_collection_run_id,
                )
            )
            if existing_marker is None:
                return {
                    'updated': False,
                    'reason': (
                        'followup_stage_debt_terminal_marker_invalid'
                    ),
                }
            processed_symbols = list(
                existing_marker['processed_stock_codes']
            )
            batches = list(existing_marker['batches'])
        if proof['pipeline_run_id'] in {
            batch['pipeline_run_id'] for batch in batches
        }:
            return {
                'updated': False,
                'reason': 'followup_stage_debt_terminal_already_persisted',
                'marker': existing_marker,
            }
        if set(processed_symbols).intersection(normalized_cohort):
            return {
                'updated': False,
                'reason': 'followup_stage_debt_terminal_scope_overlap',
            }

        batches.append(
            {
                'pipeline_run_id': proof['pipeline_run_id'],
                'cohort_basis': cohort_basis,
                'stock_codes': normalized_cohort,
                'stock_code_count': len(normalized_cohort),
                'membership_sha256': market_membership_sha256(
                    normalized_cohort
                ),
                'outcome_status': proof['outcome_status'],
            }
        )
        processed_symbols = sorted(
            set(processed_symbols).union(normalized_cohort)
        )
        marker = {
            'schema_version': (
                FOLLOWUP_STAGE_DEBT_TERMINAL_PROGRESSION_SCHEMA_VERSION
            ),
            'market_date': market_date,
            'eod_collection_run_id': eod_collection_run_id,
            'processed_stock_codes': processed_symbols,
            'processed_stock_code_count': len(processed_symbols),
            'processed_membership_sha256': market_membership_sha256(
                processed_symbols
            ),
            'batches': batches,
        }
        validated_marker = (
            _validated_followup_stage_debt_terminal_progression_marker(
                marker,
                state=validated_state,
                market_date=market_date,
                eod_collection_run_id=eod_collection_run_id,
            )
        )
        if validated_marker is None:
            return {
                'updated': False,
                'reason': 'followup_stage_debt_terminal_marker_invalid',
            }
        metadata[
            FOLLOWUP_STAGE_DEBT_TERMINAL_PROGRESSION_METADATA_KEY
        ] = validated_marker
        nonblocking_marker = proof.get('nonblocking_partial_progression')
        if isinstance(nonblocking_marker, Mapping):
            metadata['nonblocking_partial_progression'] = dict(
                nonblocking_marker
            )
            retry_debt = nonblocking_marker.get('retry_debt')
            if isinstance(retry_debt, Mapping):
                metadata['nonblocking_feedback_retry_debt'] = dict(
                    retry_debt
                )
        metadata['followup_recovery_required'] = bool(
            _next_followup_stage_debt_request(validated_state)
            or metadata.get('nonblocking_feedback_retry_debt')
        )
        try:
            update_eod_collection_run_progress(
                eod_collection_run_id,
                symbols_succeeded=max(
                    int(latest_eod_run.get('symbols_succeeded') or 0),
                    0,
                ),
                symbols_failed=max(
                    int(latest_eod_run.get('symbols_failed') or 0),
                    0,
                ),
                run_metadata=metadata,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.exception(
                'Unable to persist stage-debt terminal progression for EOD %s',
                eod_collection_run_id,
            )
            return {
                'updated': False,
                'reason': 'followup_stage_debt_terminal_persist_failed',
                'error_type': type(exc).__name__,
            }
        if isinstance(latest_eod_run, dict):
            latest_eod_run['run_metadata'] = metadata
        return {
            'updated': True,
            'reason': 'followup_stage_debt_terminal_progression_persisted',
            'marker': validated_marker,
        }

    def _persist_successful_followup_recovery(
        self,
        *,
        latest_eod_run: Mapping[str, Any],
        recovery_result: Mapping[str, Any],
    ) -> dict[str, Any]:
        metadata = (
            dict(latest_eod_run.get('run_metadata') or {})
            if isinstance(latest_eod_run.get('run_metadata'), dict)
            else {}
        )
        raw_rearm_marker = metadata.get(
            SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY
        )
        marker_claims_pending = bool(
            isinstance(raw_rearm_marker, Mapping)
            and str(raw_rearm_marker.get('status') or '').strip() == 'pending'
        )
        rearm_marker = pending_same_day_intraday_watch_rearm_marker(
            raw_rearm_marker
        )
        if marker_claims_pending and rearm_marker is None:
            return {'updated': False, 'reason': 'rearm_marker_invalid'}
        if rearm_marker is not None and not (
            type(latest_eod_run.get('id')) is int
            and is_completed_auto_pitr_intraday_watch_recovery(
                recovery_result,
                expected_eod_collection_run_id=int(latest_eod_run['id']),
            )
        ):
            return {
                'updated': False,
                'reason': 'same_day_intraday_watch_rearm_not_completed',
            }
        nonblocking_partial_marker = (
            validated_nonblocking_partial_progression_marker(
                recovery_result,
                expected_eod_collection_run_id=int(
                    latest_eod_run['id']
                ),
            )
            if type(latest_eod_run.get('id')) is int
            else None
        )
        if not (
            daily_pipeline_outcome_is_satisfied(recovery_result)
            or nonblocking_partial_marker is not None
        ):
            return {'updated': False, 'reason': 'recovery_not_completed'}

        downstream_retry_symbols = _dedupe_symbol_codes(
            metadata.get('downstream_retry_symbols') or []
        )
        inherited_downstream_retry_symbols = _dedupe_symbol_codes(
            metadata.get('inherited_downstream_retry_symbols') or []
        )
        proven_resolved_symbols = _dedupe_symbol_codes(
            recovery_result.get('_proven_resolved_downstream_symbols')
            or []
        )
        if (
            (downstream_retry_symbols or inherited_downstream_retry_symbols)
            and not proven_resolved_symbols
        ):
            return {
                'updated': False,
                'reason': 'resolved_downstream_scope_unavailable',
            }
        proven_resolved_set = set(proven_resolved_symbols)
        remaining_downstream_retry_symbols = [
            symbol
            for symbol in downstream_retry_symbols
            if symbol not in proven_resolved_set
        ]
        remaining_inherited_downstream_retry_symbols = [
            symbol
            for symbol in inherited_downstream_retry_symbols
            if symbol not in proven_resolved_set
        ]
        if (
            latest_eod_run.get('symbols_total') is None
            or latest_eod_run.get('symbols_succeeded') is None
        ):
            return {'updated': False, 'reason': 'collection_counts_unavailable'}

        price_retry_symbols = self._metadata_retry_symbol_codes(
            dict(latest_eod_run)
        )
        completed_at = _utcnow_iso()
        if not remaining_downstream_retry_symbols:
            metadata.pop('downstream_retry_resume_from_stage', None)
        if not remaining_inherited_downstream_retry_symbols:
            metadata.pop(
                'inherited_downstream_retry_resume_from_stage',
                None,
            )
        if rearm_marker is not None:
            metadata[SAME_DAY_INTRADAY_WATCH_REARM_METADATA_KEY] = {
                **rearm_marker,
                'status': 'completed',
                'completed_at': completed_at,
                'completion_pipeline_run_id': recovery_result.get(
                    'pipeline_run_id'
                ),
            }
        metadata.update(
            {
                'downstream_retry_symbols': (
                    remaining_downstream_retry_symbols
                ),
                'downstream_retry_symbols_count': len(
                    remaining_downstream_retry_symbols
                ),
                'inherited_downstream_retry_symbols': (
                    remaining_inherited_downstream_retry_symbols
                ),
                'inherited_downstream_retry_symbols_count': len(
                    remaining_inherited_downstream_retry_symbols
                ),
                'downstream_recovery_symbols_count': len(
                    proven_resolved_set
                    & set(
                        [
                            *downstream_retry_symbols,
                            *inherited_downstream_retry_symbols,
                        ]
                    )
                ),
                'downstream_recovery_completed_at': completed_at,
                'downstream_recovery_pipeline_run_id': recovery_result.get(
                    'pipeline_run_id'
                ),
                'followup_recovery_required': bool(
                    remaining_downstream_retry_symbols
                    or remaining_inherited_downstream_retry_symbols
                    or _next_followup_stage_debt_request(
                        metadata.get(FOLLOWUP_STAGE_DEBT_METADATA_KEY)
                        or {}
                    )
                ),
            }
        )
        if nonblocking_partial_marker is not None:
            metadata.update(
                {
                    'nonblocking_partial_progression': deepcopy(
                        nonblocking_partial_marker
                    ),
                    'nonblocking_feedback_retry_debt': deepcopy(
                        nonblocking_partial_marker['retry_debt']
                    ),
                }
            )
        run_id = int(latest_eod_run['id'])
        symbols_total = max(int(latest_eod_run.get('symbols_total') or 0), 0)
        symbols_succeeded = max(
            int(latest_eod_run.get('symbols_succeeded') or 0),
            0,
        )
        symbols_failed = max(
            int(latest_eod_run.get('symbols_failed') or 0),
            0,
        )
        trade_date = latest_eod_run.get('trade_date')
        persisted_symbol_count = (
            count_eod_run_persisted_symbols(
                run_id,
                trade_date=trade_date,
            )
            if symbols_total > 0
            and not price_retry_symbols
            else None
        )
        requested_symbols = _dedupe_symbol_codes(
            metadata.get('requested_symbols') or []
        )
        persisted_symbol_codes: set[str] | None = None
        missing_symbols: list[str] = []
        unexpected_symbols: list[str] = []
        if persisted_symbol_count is not None and requested_symbols:
            persisted_symbol_codes = set(
                load_eod_run_persisted_symbols(
                    run_id,
                    trade_date=trade_date,
                )
            )
            requested_symbol_set = set(requested_symbols)
            missing_symbols = [
                symbol
                for symbol in requested_symbols
                if symbol not in persisted_symbol_codes
            ]
            unexpected_symbols = sorted(
                persisted_symbol_codes - requested_symbol_set
            )
        identity_mismatch = bool(missing_symbols or unexpected_symbols)
        coverage_gap_count = max(
            symbols_total - int(persisted_symbol_count or 0),
            0,
        ) if persisted_symbol_count is not None else 0
        if coverage_gap_count or identity_mismatch:
            if persisted_symbol_codes is None:
                persisted_symbol_codes = set(
                    load_eod_run_persisted_symbols(
                        run_id,
                        trade_date=trade_date,
                    )
                )
            if not requested_symbols:
                covered_requested_count = min(
                    int(persisted_symbol_count or 0),
                    symbols_total,
                )
            else:
                covered_requested_count = min(
                    len(set(requested_symbols) & persisted_symbol_codes),
                    symbols_total,
                )
            reconciled_failed_count = max(
                symbols_total - covered_requested_count,
                0,
            )
            if not missing_symbols and requested_symbols:
                missing_symbols = [
                    symbol
                    for symbol in requested_symbols
                    if symbol not in persisted_symbol_codes
                ]
            metadata.update(
                {
                    'failed_symbols': missing_symbols,
                    'failed_symbols_count': len(missing_symbols),
                    'unexpected_persisted_symbols': unexpected_symbols,
                    'unexpected_persisted_symbols_count': len(
                        unexpected_symbols
                    ),
                    'collection_coverage_inconsistent': True,
                    'collection_coverage_checked_at': completed_at,
                    'collection_persisted_symbols_count': int(
                        persisted_symbol_count or 0
                    ),
                    'collection_expected_symbols_count': symbols_total,
                    'resumable': bool(
                        reconciled_failed_count or missing_symbols
                    ),
                }
            )
            coverage_error = (
                'Persisted price coverage is incomplete or does not match '
                'the admitted symbols after follow-up recovery: '
                f'{persisted_symbol_count}/{symbols_total} symbols, '
                f'{len(missing_symbols)} missing, '
                f'{len(unexpected_symbols)} unexpected.'
            )
            complete_eod_collection_run(
                run_id,
                status='completed_with_errors',
                symbols_succeeded=covered_requested_count,
                symbols_failed=reconciled_failed_count,
                error_message=coverage_error,
                run_metadata=metadata,
            )
            return {
                'updated': True,
                'status': 'completed_with_errors',
                'price_retry_pending': bool(
                    reconciled_failed_count or missing_symbols
                ),
                'persisted_symbol_count': persisted_symbol_count,
                'coverage_gap_count': reconciled_failed_count,
                'unexpected_symbol_count': len(unexpected_symbols),
            }
        if (
            persisted_symbol_count is not None
            and persisted_symbol_count > symbols_total
        ):
            metadata.update(
                {
                    'failed_symbols': [],
                    'failed_symbols_count': 0,
                    'unattempted_symbols': [],
                    'unattempted_symbols_count': 0,
                    'collection_coverage_inconsistent': True,
                    'collection_coverage_checked_at': completed_at,
                    'collection_persisted_symbols_count': (
                        persisted_symbol_count
                    ),
                    'collection_expected_symbols_count': symbols_total,
                    'resumable': False,
                }
            )
            coverage_error = (
                'Persisted price coverage exceeds the admitted collection '
                f'scope after follow-up recovery: '
                f'{persisted_symbol_count}/{symbols_total} symbols.'
            )
            complete_eod_collection_run(
                run_id,
                status='completed_with_errors',
                symbols_succeeded=symbols_total,
                symbols_failed=0,
                error_message=coverage_error,
                run_metadata=metadata,
            )
            return {
                'updated': True,
                'status': 'completed_with_errors',
                'price_retry_pending': False,
                'persisted_symbol_count': persisted_symbol_count,
                'coverage_excess_count': (
                    persisted_symbol_count - symbols_total
                ),
            }
        prices_complete = bool(
            symbols_total > 0
            and not price_retry_symbols
            and persisted_symbol_count == symbols_total
        )
        if prices_complete:
            metadata.update(
                {
                    'failed_symbols': [],
                    'failed_symbols_count': 0,
                    'unattempted_symbols': [],
                    'unattempted_symbols_count': 0,
                    'resumable': False,
                }
            )
            complete_eod_collection_run(
                run_id,
                status='completed',
                symbols_succeeded=symbols_total,
                symbols_failed=0,
                error_message=None,
                run_metadata=metadata,
            )
            return {
                'updated': True,
                'status': 'completed',
                'price_retry_pending': False,
                'persisted_symbol_count': persisted_symbol_count,
            }

        update_eod_collection_run_progress(
            run_id,
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            run_metadata=metadata,
        )
        return {
            'updated': True,
            'status': latest_eod_run.get('status'),
            'price_retry_pending': bool(price_retry_symbols),
            'persisted_symbol_count': persisted_symbol_count,
        }

    def run_daily_pipeline_if_due(
        self,
        *,
        market_date: str | None = None,
        markets: Sequence[str] = DEFAULT_MARKETS,
        trigger: str = 'auto_close',
        refresh_universe: bool = True,
        force: bool = False,
        point_in_time_recovery: bool = False,
        recovery_eod_run_id: int | None = None,
    ) -> dict:
        if market_date is None:
            latest_trade_date = self.resolve_latest_trade_date()
            normalized_market_date = latest_trade_date.replace('-', '')
            market_date_iso = latest_trade_date
        else:
            normalized_market_date = _normalize_yyyymmdd(market_date)
            market_date_iso = _normalize_iso_date(normalized_market_date)
            latest_trade_date = self.resolve_latest_trade_date(end_date=normalized_market_date)

        point_in_time_recovery = resolve_point_in_time_recovery(
            market_date=market_date_iso,
            requested=point_in_time_recovery,
            recovery_context=(
                trigger in POINT_IN_TIME_RECOVERY_BOUNDARY_TRIGGERS
            ),
            current_market_date=_current_krx_calendar_date(),
        )

        if latest_trade_date != market_date_iso:
            result = build_daily_skip_result(
                reason='trade_date_not_available_yet',
                market_date=market_date_iso,
                latest_available_trade_date=latest_trade_date,
            )
            _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
            return result

        validate_finalized_krx_write_date(market_date_iso)

        with advisory_eod_collection_lock(
            market='KRX',
            trade_date=market_date_iso,
            source='daily_price_bars',
        ) as lock_acquired:
            if not lock_acquired:
                result = build_daily_skip_result(
                    reason='already_running',
                    market_date=market_date_iso,
                    latest_available_trade_date=latest_trade_date,
                )
                _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                return result

            if recovery_eod_run_id is not None:
                latest_run = load_exact_automatic_daily_eod_collection_run(
                    eod_run_id=recovery_eod_run_id,
                    trade_date=market_date_iso,
                )
            else:
                latest_run = load_latest_eod_collection_run(
                    market='KRX',
                    trade_date=market_date_iso,
                    source='pykrx',
                )
            if latest_run and not force:
                latest_run = self.reconcile_latest_daily_run_status(
                    trade_date=market_date_iso,
                    latest_run=latest_run,
                    recovery_eod_run_id=recovery_eod_run_id,
                )
            if recovery_eod_run_id is not None and latest_run is None:
                result = build_daily_skip_result(
                    reason='recovery_target_invalid_or_superseded',
                    market_date=market_date_iso,
                    latest_available_trade_date=latest_trade_date,
                )
                result['recovery_target'] = {
                    'trade_date': market_date_iso,
                    'eod_run_id': recovery_eod_run_id,
                }
                _log_pykrx_fetch(
                    'run_daily_pipeline_if_due',
                    'skipped',
                    **result,
                )
                return result
            if latest_run and latest_run['status'] == 'running' and not force:
                result = build_daily_skip_result(
                    reason='already_running',
                    market_date=market_date_iso,
                    latest_available_trade_date=latest_trade_date,
                    existing_run=latest_run,
                )
                _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                return result
            symbols_override: list[PykrxSymbol] | None = None
            retry_context: dict | None = None
            if not force and latest_run:
                latest_status = latest_run.get('status')
                latest_failed = int(latest_run.get('symbols_failed') or 0)
                metadata_retry_symbols = self._metadata_retry_symbol_codes(latest_run)
                latest_metadata = (
                    latest_run.get('run_metadata')
                    if isinstance(latest_run.get('run_metadata'), dict)
                    else {}
                )
                if point_in_time_recovery:
                    markets = _point_in_time_source_markets(
                        latest_metadata,
                        fallback=markets,
                    )
                collection_is_terminal = bool(
                    (
                        latest_status == 'completed'
                        and latest_failed == 0
                    )
                    or (
                        latest_status == 'completed_with_errors'
                        and latest_failed == 0
                        and not metadata_retry_symbols
                    )
                    or self._failed_run_has_exact_complete_price_scope(
                        latest_run
                    )
                )
                has_downstream_retry_scope = bool(
                    latest_metadata.get('downstream_retry_symbols')
                )
                price_retry_pending = bool(
                    latest_failed > 0 or metadata_retry_symbols
                )
                is_automatic_retryable_run = bool(
                    _is_auto_daily_trigger(trigger)
                    and self._is_retryable_daily_run(latest_run)
                )
                automatic_run_count: int | None = None
                if (
                    is_automatic_retryable_run
                    and price_retry_pending
                    and has_downstream_retry_scope
                ):
                    automatic_run_count = count_eod_collection_runs(
                        market='KRX',
                        trade_date=market_date_iso,
                        source='pykrx',
                        trigger_prefix='auto_',
                    )
                if _is_auto_daily_trigger(trigger) and (
                    collection_is_terminal or has_downstream_retry_scope
                ):
                    recovery_result = self._resume_auto_daily_followups_if_due(
                        market_date=market_date_iso,
                        latest_available_trade_date=latest_trade_date,
                        latest_eod_run=latest_run,
                        markets=markets,
                        allow_safe_downstream_scope_with_price_failures=bool(
                            automatic_run_count is not None
                            and automatic_run_count
                            >= MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE
                        ),
                        point_in_time_recovery=point_in_time_recovery,
                        provider_admission_fence_owned=True,
                    )
                    recovery_exhausted = bool(
                        recovery_result is not None
                        and recovery_result.get('reason')
                        == 'followup_recovery_limit_reached'
                    )
                    if recovery_result is not None and not (
                        recovery_exhausted and price_retry_pending
                    ):
                        _log_pykrx_fetch(
                            'run_daily_pipeline_if_due',
                            str(recovery_result.get('status') or 'skipped'),
                            **{
                                key: value
                                for key, value in recovery_result.items()
                                if key != 'status'
                            },
                        )
                        return recovery_result
                if latest_status == 'completed' and latest_failed == 0:
                    result = build_daily_skip_result(
                        reason='already_completed',
                        market_date=market_date_iso,
                        latest_available_trade_date=latest_trade_date,
                        existing_run=latest_run,
                    )
                    _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                    return result
                if (
                    latest_status == 'completed_with_errors'
                    and latest_failed == 0
                    and not metadata_retry_symbols
                ):
                    result = build_daily_skip_result(
                        reason='already_completed_with_errors',
                        market_date=market_date_iso,
                        latest_available_trade_date=latest_trade_date,
                        existing_run=latest_run,
                    )
                    _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                    return result
                if is_automatic_retryable_run:
                    if automatic_run_count is None:
                        automatic_run_count = count_eod_collection_runs(
                            market='KRX',
                            trade_date=market_date_iso,
                            source='pykrx',
                            trigger_prefix='auto_',
                        )
                    if automatic_run_count >= MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE:
                        result = build_daily_skip_result(
                            reason='auto_retry_limit_reached',
                            market_date=market_date_iso,
                            latest_available_trade_date=latest_trade_date,
                            existing_run=latest_run,
                            auto_retry={
                                'attempts': automatic_run_count,
                                'max_attempts': MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE,
                            },
                        )
                        _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                        return result
                    latest_run_completed_at = _parse_activity_datetime(
                        latest_run.get('completed_at')
                    )
                    latest_run_activity_at = (
                        latest_run_completed_at
                        or _parse_activity_datetime(latest_run.get('started_at'))
                    )
                    if latest_run_activity_at is not None:
                        retry_elapsed_seconds = (
                            datetime.now(timezone.utc) - latest_run_activity_at
                        ).total_seconds()
                        if (
                            retry_elapsed_seconds
                            < AUTO_PYKRX_DAILY_RETRY_MIN_INTERVAL_SECONDS
                        ):
                            retry_after_seconds = max(
                                1,
                                math.ceil(
                                    AUTO_PYKRX_DAILY_RETRY_MIN_INTERVAL_SECONDS
                                    - retry_elapsed_seconds
                                ),
                            )
                            result = build_daily_skip_result(
                                reason='auto_retry_backoff',
                                market_date=market_date_iso,
                                latest_available_trade_date=latest_trade_date,
                                existing_run=latest_run,
                                auto_retry={
                                    'attempts': automatic_run_count,
                                    'max_attempts': (
                                        MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE
                                    ),
                                    'retry_after_seconds': retry_after_seconds,
                                },
                            )
                            result['retry_after_seconds'] = (
                                retry_after_seconds
                            )
                            _log_pykrx_fetch(
                                'run_daily_pipeline_if_due',
                                'skipped',
                                **result,
                            )
                            return result
                    failed_symbol_kwargs: dict[str, Any] = {
                        'markets': markets,
                    }
                    if point_in_time_recovery:
                        failed_symbol_kwargs['point_in_time_recovery'] = True
                    retry_symbol_codes = self._failed_symbol_codes_for_run(
                        latest_run,
                        **failed_symbol_kwargs,
                    )
                    retry_scope_snapshot = getattr(
                        retry_symbol_codes,
                        'universe_snapshot',
                        None,
                    )
                    retry_scope_coverage_mismatch = bool(
                        getattr(
                            retry_symbol_codes,
                            'coverage_mismatch',
                            False,
                        )
                    )
                    membership_scope_proven = bool(
                        point_in_time_recovery
                        and isinstance(retry_scope_snapshot, Mapping)
                        and not retry_scope_coverage_mismatch
                    )
                    retry_symbol_kwargs: dict[str, Any] = {
                        'markets': markets,
                        'symbol_codes': retry_symbol_codes,
                    }
                    if point_in_time_recovery:
                        retry_symbol_kwargs['point_in_time_recovery'] = True
                    symbols_override = self._load_retry_symbols_for_run(
                        latest_run,
                        **retry_symbol_kwargs,
                    )
                    resolved_retry_symbol_codes = {
                        item.symbol for item in symbols_override
                    }
                    unresolved_retry_symbol_codes = [
                        symbol_code
                        for symbol_code in retry_symbol_codes
                        if symbol_code not in resolved_retry_symbol_codes
                    ]
                    membership_scope_unresolved = bool(
                        point_in_time_recovery
                        and not retry_symbol_codes
                        and not membership_scope_proven
                    )
                    retry_scope_failure_reason = (
                        'point_in_time_retry_lineage_mismatch'
                        if retry_scope_coverage_mismatch
                        else 'point_in_time_retry_membership_unavailable'
                    )
                    if unresolved_retry_symbol_codes or membership_scope_unresolved:
                        if point_in_time_recovery:
                            attempt = automatic_run_count + 1
                            unresolved_count = max(
                                len(unresolved_retry_symbol_codes),
                                int(latest_run.get('symbols_failed') or 0),
                                int(latest_run.get('symbols_total') or 0)
                                - int(latest_run.get('symbols_succeeded') or 0),
                                1,
                            )
                            auto_retry = {
                                'attempt': attempt,
                                'attempts': attempt,
                                'max_attempts': (
                                    MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE
                                ),
                                'requested_symbol_count': len(
                                    retry_symbol_codes
                                ),
                                'resolved_symbol_count': len(
                                    resolved_retry_symbol_codes
                                ),
                                'unresolved_symbol_count': unresolved_count,
                                'unresolved_symbols': (
                                    unresolved_retry_symbol_codes[:50]
                                ),
                            }
                            retry_metadata = build_daily_retry_context(
                                latest_run,
                                attempt=attempt,
                                max_attempts=(
                                    MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE
                                ),
                                symbol_count=len(retry_symbol_codes),
                            )
                            attempt_metadata = {
                                'mode': 'daily_auto',
                                'point_in_time_recovery': True,
                                'requested_start_date': normalized_market_date,
                                'requested_end_date': normalized_market_date,
                                'latest_available_trade_date': (
                                    latest_trade_date
                                ),
                                'markets': list(markets),
                                'requested_symbols': retry_symbol_codes,
                                'failed_symbols': (
                                    unresolved_retry_symbol_codes
                                ),
                                'failed_symbols_count': len(
                                    unresolved_retry_symbol_codes
                                ),
                                'unattempted_symbols': sorted(
                                    resolved_retry_symbol_codes
                                ),
                                'unattempted_symbols_count': len(
                                    resolved_retry_symbol_codes
                                ),
                                'retry': retry_metadata,
                                'auto_retry': auto_retry,
                                'resumable': True,
                                'failure_reason': (
                                    retry_scope_failure_reason
                                ),
                            }
                            attempt_run_id = create_eod_collection_run(
                                market='KRX',
                                trade_date=market_date_iso,
                                symbols_total=unresolved_count,
                                trigger=trigger,
                                source='pykrx',
                                run_metadata=attempt_metadata,
                            )
                            error_message = (
                                'Point-in-time membership evidence cannot '
                                'authorize the retry scope: '
                                + ', '.join(
                                    (
                                        getattr(
                                            retry_symbol_codes,
                                            'unexpected_persisted_symbols',
                                            [],
                                        )
                                        or unresolved_retry_symbol_codes
                                    )[:50]
                                )
                            )
                            complete_eod_collection_run(
                                attempt_run_id,
                                status='failed',
                                symbols_succeeded=0,
                                symbols_failed=unresolved_count,
                                error_message=error_message,
                                run_metadata=attempt_metadata,
                            )
                            result = {
                                'triggered': True,
                                'run_id': attempt_run_id,
                                'source': 'pykrx',
                                'status': 'failed',
                                'reason': (
                                    retry_scope_failure_reason
                                ),
                                'market_date': market_date_iso,
                                'latest_available_trade_date': (
                                    latest_trade_date
                                ),
                                'symbols_total': unresolved_count,
                                'symbols_succeeded': 0,
                                'symbols_failed': unresolved_count,
                                'failed_symbols': (
                                    unresolved_retry_symbol_codes[:50]
                                ),
                                'auto_retry': auto_retry,
                            }
                            _log_pykrx_fetch(
                                'run_daily_pipeline_if_due',
                                'failed',
                                **{
                                    key: value
                                    for key, value in result.items()
                                    if key != 'status'
                                },
                            )
                            return result
                        result = build_daily_skip_result(
                            reason='retry_symbol_resolution_incomplete',
                            market_date=market_date_iso,
                            latest_available_trade_date=latest_trade_date,
                            existing_run=latest_run,
                            auto_retry={
                                'attempts': automatic_run_count,
                                'max_attempts': MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE,
                                'requested_symbol_count': len(
                                    retry_symbol_codes
                                ),
                                'resolved_symbol_count': len(
                                    resolved_retry_symbol_codes
                                ),
                                'unresolved_symbol_count': len(
                                    unresolved_retry_symbol_codes
                                ),
                                'unresolved_symbols': (
                                    unresolved_retry_symbol_codes[:50]
                                ),
                            },
                        )
                        _log_pykrx_fetch(
                            'run_daily_pipeline_if_due',
                            'skipped',
                            **result,
                        )
                        return result
                    if (
                        point_in_time_recovery
                        and membership_scope_proven
                        and not retry_symbol_codes
                    ):
                        converged_run = (
                            self._converge_point_in_time_price_complete_run(
                                latest_eod_run=latest_run,
                                market_date=market_date_iso,
                                markets=markets,
                                universe_snapshot=retry_scope_snapshot,
                            )
                        )
                        if converged_run is not None:
                            recovery_result = (
                                self._resume_auto_daily_followups_if_due(
                                    market_date=market_date_iso,
                                    latest_available_trade_date=(
                                        latest_trade_date
                                    ),
                                    latest_eod_run=converged_run,
                                    markets=markets,
                                    point_in_time_recovery=True,
                                    provider_admission_fence_owned=True,
                                )
                            )
                            if recovery_result is not None:
                                return recovery_result
                            result = build_daily_skip_result(
                                reason=(
                                    'point_in_time_price_scope_reconciled'
                                ),
                                market_date=market_date_iso,
                                latest_available_trade_date=(
                                    latest_trade_date
                                ),
                                existing_run=converged_run,
                            )
                            result.update(
                                {'triggered': True, 'status': 'completed'}
                            )
                            return result
                        membership_scope_unresolved = True

                    if not symbols_override:
                        result = build_daily_skip_result(
                            reason='no_failed_symbols_to_retry',
                            market_date=market_date_iso,
                            latest_available_trade_date=latest_trade_date,
                            existing_run=latest_run,
                            auto_retry={
                                'attempts': automatic_run_count,
                                'max_attempts': MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE,
                            },
                        )
                        _log_pykrx_fetch('run_daily_pipeline_if_due', 'skipped', **result)
                        return result
                    retry_context = build_daily_retry_context(
                        latest_run,
                        attempt=automatic_run_count + 1,
                        max_attempts=MAX_AUTO_PYKRX_DAILY_RUNS_PER_TRADE_DATE,
                        symbol_count=len(symbols_override),
                    )
                    if point_in_time_recovery:
                        resolved_retry_snapshot = getattr(
                            symbols_override,
                            'universe_snapshot',
                            None,
                        )
                        source_universe_snapshot = (
                            dict(resolved_retry_snapshot)
                            if isinstance(resolved_retry_snapshot, dict)
                            else latest_metadata.get('universe_snapshot')
                            if isinstance(
                                latest_metadata.get('universe_snapshot'), dict
                            )
                            else None
                        )
                        if source_universe_snapshot is not None:
                            if (
                                _validate_point_in_time_universe_snapshot(
                                    source_universe_snapshot,
                                    markets=markets,
                                    expected_market_date=market_date_iso,
                                )
                                is None
                            ):
                                source_universe_snapshot = None
                        if source_universe_snapshot is None:
                            retry_membership = build_market_membership_snapshot(
                                symbols=symbols_override,
                                markets=markets,
                            )
                            retry_membership['symbol_metadata'] = [
                                {
                                    'symbol': item.symbol,
                                    'market': item.market,
                                    'name_ko': item.name_ko,
                                    'standard_code': item.standard_code,
                                }
                                for item in symbols_override
                            ]
                            source_universe_snapshot = {
                                'source': 'pykrx',
                                'markets': list(markets),
                                'symbol_count': len(symbols_override),
                                'persisted_count': 0,
                                'latest_trade_date': market_date_iso,
                                'universe_source': 'point_in_time_retry_scope',
                                'membership_basis': 'end_date_snapshot',
                                'complete': True,
                                'deactivate_missing': False,
                                'point_in_time': True,
                                'current_universe_mutated': False,
                                'market_membership': retry_membership,
                            }
                        retry_context[
                            'point_in_time_universe_snapshot'
                        ] = source_universe_snapshot
                        validated_retry_membership = (
                            _validate_point_in_time_universe_snapshot(
                                source_universe_snapshot,
                                markets=markets,
                                expected_market_date=market_date_iso,
                            )
                        )
                        if validated_retry_membership is not None:
                            exact_membership_symbols = {
                                symbol
                                for market_symbols in (
                                    validated_retry_membership.values()
                                )
                                for symbol in market_symbols
                            }
                            persisted_parent_symbols = set(
                                load_eod_run_persisted_symbols(
                                    int(latest_run['id']),
                                    trade_date=market_date_iso,
                                )
                            )
                            inherited_symbols = _dedupe_symbol_codes(
                                [
                                    *(
                                        retry_context.get(
                                            'inherited_downstream_retry_symbols'
                                        )
                                        or []
                                    ),
                                    *sorted(
                                        (
                                            persisted_parent_symbols
                                            & exact_membership_symbols
                                        )
                                        - set(retry_symbol_codes)
                                    ),
                                ]
                            )
                            if inherited_symbols:
                                retry_context[
                                    'inherited_downstream_retry_symbols'
                                ] = inherited_symbols
                                retry_context.setdefault(
                                    'inherited_downstream_retry_resume_from_stage',
                                    'pykrx_enrichment',
                                )
                    inherited_resume_from_stage = str(
                        latest_metadata.get(
                            'downstream_retry_resume_from_stage'
                        )
                        or ''
                    ).strip()
                    if (
                        retry_context.get(
                            'inherited_downstream_retry_symbols'
                        )
                        and inherited_resume_from_stage
                    ):
                        retry_context[
                            'inherited_downstream_retry_resume_from_stage'
                        ] = inherited_resume_from_stage

            result = self._run_market_data_pipeline(
                start_date=normalized_market_date,
                end_date=normalized_market_date,
                markets=markets,
                trigger=trigger,
                refresh_universe=False if symbols_override is not None else refresh_universe,
                mode='daily_auto',
                symbols_override=symbols_override,
                retry_context=retry_context,
                selected_stages=PYKRX_STAGE_SEQUENCE,
                chain_fundamentals_followup=True,
                track_daily_pipeline=True,
                point_in_time_recovery=point_in_time_recovery,
                provider_admission_fence_owned=True,
            )
            result.setdefault('triggered', True)
            _log_pykrx_fetch(
                'run_daily_pipeline_if_due',
                result['status'],
                **{key: value for key, value in result.items() if key != 'status'},
            )
            return result

    def reconcile_latest_daily_run_status(
        self,
        *,
        trade_date: str | None = None,
        latest_run: dict | None = None,
        recovery_eod_run_id: int | None = None,
    ) -> dict | None:
        resolved_run = latest_run
        resolved_trade_date = trade_date or (
            str(latest_run.get('trade_date')) if latest_run is not None and latest_run.get('trade_date') else None
        )
        if resolved_trade_date is None:
            raise ValueError('trade_date is required to reconcile pykrx daily run status')

        if resolved_run is None:
            if recovery_eod_run_id is not None:
                resolved_run = load_exact_automatic_daily_eod_collection_run(
                    eod_run_id=recovery_eod_run_id,
                    trade_date=resolved_trade_date,
                )
            else:
                resolved_run = load_latest_eod_collection_run(
                    market='KRX',
                    trade_date=resolved_trade_date,
                    source='pykrx',
                )
        if resolved_run and resolved_run.get('status') == 'running' and self._is_stale_running_run(resolved_run):
            self._fail_stale_running_run(resolved_run)
            if recovery_eod_run_id is not None:
                resolved_run = load_exact_automatic_daily_eod_collection_run(
                    eod_run_id=recovery_eod_run_id,
                    trade_date=resolved_trade_date,
                )
            else:
                resolved_run = load_latest_eod_collection_run(
                    market='KRX',
                    trade_date=resolved_trade_date,
                    source='pykrx',
                )
        return resolved_run

    def run_daily_pipeline_stages(
        self,
        *,
        market_date: str,
        stages: Sequence[str],
        markets: Sequence[str] = DEFAULT_MARKETS,
        trigger: str = 'stage_orchestrator',
        refresh_universe: bool = True,
        force: bool = False,
        symbols: Sequence[str] | None = None,
        point_in_time_recovery: bool = False,
        owning_pipeline_run_id: int | None = None,
    ) -> dict:
        requested_stages = [stage for stage in PYKRX_STAGE_SEQUENCE if stage in set(stages)]
        if not requested_stages:
            raise ValueError('At least one pykrx stage must be requested.')
        if 'price_collection' not in requested_stages:
            raise ValueError('price_collection must be included when running grouped pykrx stages.')

        normalized_market_date = _normalize_yyyymmdd(market_date)
        market_date_iso = _normalize_iso_date(normalized_market_date)
        validate_finalized_krx_write_date(market_date_iso)
        point_in_time_recovery = resolve_point_in_time_recovery(
            market_date=market_date_iso,
            requested=point_in_time_recovery,
            recovery_context=(
                trigger in POINT_IN_TIME_RECOVERY_BOUNDARY_TRIGGERS
            ),
            current_market_date=_current_krx_calendar_date(),
        )
        latest_trade_date = self.resolve_latest_trade_date(end_date=normalized_market_date)
        if latest_trade_date != market_date_iso:
            return build_daily_skip_result(
                reason='trade_date_not_available_yet',
                market_date=market_date_iso,
                latest_available_trade_date=latest_trade_date,
            )

        symbols_override = None
        if symbols:
            requested_symbols = {str(item).strip().zfill(6) for item in symbols if str(item).strip()}
            if point_in_time_recovery:
                snapshot = self.fetch_recent_universe_snapshot(
                    markets=markets,
                    limit=None,
                    end_date=normalized_market_date,
                    allow_master_fallback=False,
                )
                if not snapshot.get('complete', True):
                    unavailable_markets = ','.join(
                        snapshot.get('unavailable_markets') or []
                    )
                    raise RuntimeError(
                        'PyKRX universe snapshot is incomplete for requested '
                        f'markets: {unavailable_markets or "unknown"}.'
                    )
                symbols_override = [
                    item
                    for item in snapshot['symbols']
                    if item.symbol in requested_symbols
                ]
                admitted_symbols = {
                    item.symbol for item in symbols_override
                }
                missing_requested_symbols = sorted(
                    requested_symbols - admitted_symbols
                )
                if missing_requested_symbols:
                    raise RuntimeError(
                        'Point-in-time universe is missing requested symbols: '
                        + ', '.join(missing_requested_symbols[:50])
                    )
            else:
                symbols_override = [
                    item
                    for item in self.load_active_universe(markets=markets)
                    if item.symbol in requested_symbols
                ]
            if not symbols_override:
                raise RuntimeError('No active pykrx symbols matched the requested stage scope.')

        with advisory_eod_collection_lock(
            market='KRX',
            trade_date=market_date_iso,
            source='daily_price_bars',
        ) as lock_acquired:
            if not lock_acquired:
                result = build_daily_skip_result(
                    reason='already_running',
                    market_date=market_date_iso,
                    latest_available_trade_date=latest_trade_date,
                )
                _log_pykrx_fetch('run_daily_pipeline_stages', 'skipped', **result)
                return result

            result = self._run_market_data_pipeline(
                start_date=normalized_market_date,
                end_date=normalized_market_date,
                markets=markets,
                trigger=trigger,
                refresh_universe=refresh_universe,
                mode='daily_stage_run',
                symbols_override=symbols_override,
                selected_stages=requested_stages,
                chain_fundamentals_followup=False,
                point_in_time_recovery=point_in_time_recovery,
                owning_pipeline_run_id=owning_pipeline_run_id,
            )
            result['triggered'] = True
            result['forced'] = force
            _log_pykrx_fetch(
                'run_daily_pipeline_stages',
                result['status'],
                **{key: value for key, value in result.items() if key != 'status'},
            )
            return result

    def _is_retryable_daily_run(self, run: dict) -> bool:
        status = run.get('status')
        if status not in RETRYABLE_DAILY_STATUSES:
            return False
        if status == 'failed':
            return True
        return int(run.get('symbols_failed') or 0) > 0 or bool(
            self._metadata_retry_symbol_codes(run)
        )

    @staticmethod
    def _failed_run_has_exact_complete_price_scope(run: Mapping[str, Any]) -> bool:
        if str(run.get('status') or '').strip().lower() != 'failed':
            return False
        metadata = (
            run.get('run_metadata')
            if isinstance(run.get('run_metadata'), Mapping)
            else {}
        )
        requested_symbols = _dedupe_symbol_codes(
            metadata.get('requested_symbols') or []
        )
        try:
            symbols_total = max(int(run.get('symbols_total') or 0), 0)
            symbols_succeeded = max(
                int(run.get('symbols_succeeded') or 0),
                0,
            )
            symbols_failed = max(
                int(run.get('symbols_failed') or 0),
                0,
            )
            persisted_count = int(
                metadata.get('collection_persisted_symbols_count')
            )
            expected_count = int(
                metadata.get('collection_expected_symbols_count')
            )
        except (TypeError, ValueError):
            return False
        return bool(
            symbols_total > 0
            and len(requested_symbols) == symbols_total
            and symbols_succeeded == symbols_total
            and symbols_failed == 0
            and persisted_count == symbols_total
            and expected_count == symbols_total
            and metadata.get('collection_coverage_checked_at')
            and metadata.get('collection_coverage_inconsistent') is False
            and not metadata.get('unexpected_persisted_symbols')
            and not _dedupe_symbol_codes(
                [
                    *(metadata.get('failed_symbols') or []),
                    *(metadata.get('unattempted_symbols') or []),
                ]
            )
        )

    @staticmethod
    def _metadata_retry_symbol_codes(run: dict) -> list[str]:
        metadata = run.get('run_metadata') if isinstance(run.get('run_metadata'), dict) else {}
        return _dedupe_symbol_codes(
            [
                *(metadata.get('failed_symbols') or []),
                *(metadata.get('unattempted_symbols') or []),
                *(metadata.get('turnover_invalid_symbols') or []),
            ]
        )

    def _load_retry_symbols_for_run(
        self,
        run: dict,
        *,
        markets: Sequence[str],
        symbol_codes: Sequence[str] | None = None,
        point_in_time_recovery: bool = False,
    ) -> list[PykrxSymbol]:
        retry_code_universe_snapshot = getattr(
            symbol_codes,
            'universe_snapshot',
            None,
        )
        resolved_symbol_codes = (
            _dedupe_symbol_codes(symbol_codes)
            if symbol_codes is not None
            else self._failed_symbol_codes_for_run(run, markets=markets)
        )
        if not resolved_symbol_codes:
            return []

        if point_in_time_recovery:
            metadata = (
                run.get('run_metadata')
                if isinstance(run.get('run_metadata'), dict)
                else {}
            )
            universe_snapshot = (
                retry_code_universe_snapshot
                if isinstance(retry_code_universe_snapshot, dict)
                else metadata.get('universe_snapshot')
                if isinstance(metadata.get('universe_snapshot'), dict)
                else {}
            )
            evidence_metadata = {
                **metadata,
                'universe_snapshot': universe_snapshot,
            }
            membership_markets = _point_in_time_source_markets(
                evidence_metadata,
                fallback=markets,
            )
            validated = _validate_point_in_time_universe_snapshot(
                universe_snapshot,
                markets=membership_markets,
                expected_market_date=str(run.get('trade_date') or ''),
            )
            if validated is None:
                lineage_evidence = (
                    self._resolve_frozen_daily_membership_from_retry_lineage(
                        market_date=str(run.get('trade_date') or ''),
                        metadata=evidence_metadata,
                        markets=membership_markets,
                        eod_run_id=int(run['id']),
                    )
                )
                if lineage_evidence:
                    (
                        _,
                        universe_snapshot,
                        membership_markets,
                    ) = lineage_evidence
                    evidence_metadata = {
                        **metadata,
                        'markets': list(membership_markets),
                        'universe_snapshot': universe_snapshot,
                    }
                    validated = _validate_point_in_time_universe_snapshot(
                        universe_snapshot,
                        markets=membership_markets,
                        expected_market_date=str(
                            run.get('trade_date') or ''
                        ),
                    )
            if (
                validated is not None
                and _validated_point_in_time_membership_symbols(
                    evidence_metadata,
                    markets=membership_markets,
                    expected_market_date=str(run.get('trade_date') or ''),
                )
                is None
            ):
                validated = None
            if validated is not None:
                stored_members = {
                    symbol
                    for market_symbols in validated.values()
                    for symbol in market_symbols
                }
                if not set(resolved_symbol_codes).issubset(stored_members):
                    validated = None
            if validated is None:
                try:
                    point_in_time_snapshot = self.fetch_recent_universe_snapshot(
                        markets=membership_markets,
                        limit=None,
                        end_date=str(run.get('trade_date') or '').replace('-', ''),
                        allow_master_fallback=False,
                    )
                except Exception:
                    return []
                full_snapshot_symbols = list(
                    point_in_time_snapshot.get('symbols') or []
                )
                exact_date_universe_snapshot = (
                    _build_point_in_time_universe_snapshot(
                        point_in_time_snapshot,
                        markets=membership_markets,
                        expected_market_date=str(
                            run.get('trade_date') or ''
                        ),
                    )
                )
                if exact_date_universe_snapshot is None:
                    return []
                refetched_members = {
                    item.symbol for item in full_snapshot_symbols
                }
                if not set(resolved_symbol_codes).issubset(
                    refetched_members
                ):
                    return []
                requested_symbol_set = set(resolved_symbol_codes)
                retry_symbols = [
                    item
                    for item in full_snapshot_symbols
                    if item.symbol in requested_symbol_set
                ]
                return PointInTimeRetrySymbols(
                    retry_symbols,
                    universe_snapshot=exact_date_universe_snapshot,
                )
            membership = universe_snapshot.get('market_membership')
            market_by_symbol = {
                symbol: market
                for market, market_symbols in validated.items()
                for symbol in market_symbols
            }
            raw_symbol_metadata = (
                membership.get('symbol_metadata')
                if isinstance(membership, dict)
                and isinstance(membership.get('symbol_metadata'), list)
                else []
            )
            symbol_metadata = {
                str(item.get('symbol') or '').strip().upper(): item
                for item in raw_symbol_metadata
                if isinstance(item, dict)
            }
            historical_items: list[PykrxSymbol] = []
            for symbol_code in resolved_symbol_codes:
                market = market_by_symbol.get(symbol_code)
                if market is None:
                    continue
                identity = symbol_metadata.get(symbol_code) or {}
                if str(identity.get('market') or market).strip().upper() != market:
                    identity = {}
                historical_items.append(
                    PykrxSymbol(
                        symbol=symbol_code,
                        market=market,
                        name_ko=str(identity.get('name_ko') or symbol_code),
                        standard_code=(
                            str(identity['standard_code'])
                            if identity.get('standard_code')
                            else None
                        ),
                    )
                )
            return PointInTimeRetrySymbols(
                historical_items,
                universe_snapshot=dict(universe_snapshot),
            )

        rows = load_symbols(
            is_active=True,
            source='pykrx',
            markets=list(markets),
            symbols=resolved_symbol_codes,
        )
        by_symbol = {
            row['symbol']: PykrxSymbol(
                symbol=row['symbol'],
                market=row['market'],
                name_ko=row['name_ko'],
                standard_code=row['standard_code'],
            )
            for row in rows
        }

        items: list[PykrxSymbol] = []
        for symbol_code in resolved_symbol_codes:
            item = by_symbol.get(symbol_code)
            if item is None:
                item = self.find_symbol(symbol_code, markets=markets)
            if item is not None:
                items.append(item)
        return items

    def _failed_symbol_codes_for_run(
        self,
        run: dict,
        *,
        markets: Sequence[str],
        point_in_time_recovery: bool = False,
    ) -> list[str]:
        metadata = run.get('run_metadata') if isinstance(run.get('run_metadata'), dict) else {}
        persisted_symbols = set(
            load_eod_run_persisted_symbols(
                run['id'],
                trade_date=run.get('trade_date'),
            )
        )
        requested_symbols = _dedupe_symbol_codes(
            metadata.get('requested_symbols') or []
        )
        turnover_retry_symbols = set(
            _dedupe_symbol_codes(
                metadata.get('turnover_invalid_symbols') or []
            )
        )
        metadata_retry_symbols = self._metadata_retry_symbol_codes(run)
        scoped_retry_symbols = (
            requested_symbols or metadata_retry_symbols
        )
        missing_scoped_symbols = [
            symbol
            for symbol in scoped_retry_symbols
            if symbol not in persisted_symbols
            or symbol in turnover_retry_symbols
        ]
        point_in_time_required = bool(
            point_in_time_recovery
            or metadata.get('point_in_time_recovery')
        )
        if not point_in_time_required:
            if requested_symbols:
                return missing_scoped_symbols
            if metadata_retry_symbols:
                return missing_scoped_symbols

        membership_markets = _point_in_time_source_markets(
            metadata,
            fallback=markets,
        )
        universe_snapshot = (
            metadata.get('universe_snapshot')
            if isinstance(metadata.get('universe_snapshot'), dict)
            else {}
        )
        membership_symbols = _validated_point_in_time_membership_symbols(
            metadata,
            markets=membership_markets,
            expected_market_date=str(run.get('trade_date') or ''),
        )
        canonical_snapshot: dict[str, Any] | None = (
            dict(universe_snapshot)
            if membership_symbols is not None
            else None
        )
        if point_in_time_required and membership_symbols is None:
            lineage_evidence = (
                self._resolve_frozen_daily_membership_from_retry_lineage(
                    market_date=str(run.get('trade_date') or ''),
                    metadata=metadata,
                    markets=membership_markets,
                    eod_run_id=int(run['id']),
                )
            )
            if lineage_evidence:
                (
                    membership_symbols,
                    canonical_snapshot,
                    membership_markets,
                ) = lineage_evidence

        if membership_symbols is not None:
            missing_symbols = (
                missing_scoped_symbols
                if scoped_retry_symbols
                else sorted(membership_symbols - persisted_symbols)
            )
            if not set(missing_symbols).issubset(membership_symbols):
                return []
            if point_in_time_required and canonical_snapshot is not None:
                return PointInTimeRetryCodes(
                    missing_symbols,
                    universe_snapshot=canonical_snapshot,
                    unexpected_persisted_symbols=sorted(
                        persisted_symbols - membership_symbols
                    ),
                )
            return missing_symbols

        if point_in_time_required:
            try:
                provider_snapshot = self.fetch_recent_universe_snapshot(
                    markets=membership_markets,
                    limit=None,
                    end_date=str(run.get('trade_date') or '').replace('-', ''),
                    allow_master_fallback=False,
                )
            except Exception:
                # A prior-date retry must never infer missing membership from
                # the current active table. The caller records a bounded,
                # durable failed attempt when exact-date evidence is unavailable.
                return []
            universe_snapshot = _build_point_in_time_universe_snapshot(
                provider_snapshot,
                markets=membership_markets,
                expected_market_date=str(run.get('trade_date') or ''),
            )
            if universe_snapshot is None:
                return []
            refetched_metadata = {
                **metadata,
                'markets': list(membership_markets),
                'universe_snapshot': universe_snapshot,
            }
            membership_symbols = (
                _validated_point_in_time_membership_symbols(
                    refetched_metadata,
                    markets=membership_markets,
                    expected_market_date=str(run.get('trade_date') or ''),
                )
            )
            if not membership_symbols:
                return []
            missing_symbols = (
                missing_scoped_symbols
                if scoped_retry_symbols
                else sorted(membership_symbols - persisted_symbols)
            )
            if not set(missing_symbols).issubset(membership_symbols):
                return []
            return PointInTimeRetryCodes(
                missing_symbols,
                universe_snapshot=universe_snapshot,
                unexpected_persisted_symbols=sorted(
                    persisted_symbols - membership_symbols
                ),
            )

        active_universe = self.load_active_universe(markets=markets)
        return [item.symbol for item in active_universe if item.symbol not in persisted_symbols]

    def _run_metadata_with_failed_symbols(self, metadata: dict | None, failed_symbols: Sequence[str]) -> dict:
        updated = dict(metadata or {})
        failed_symbol_codes = _dedupe_symbol_codes(failed_symbols)
        updated['failed_symbols'] = failed_symbol_codes
        updated['failed_symbols_count'] = len(failed_symbol_codes)
        return updated

    def _run_metadata_with_downstream_retry_symbols(
        self,
        metadata: dict | None,
        failed_symbols: Sequence[str],
    ) -> dict:
        updated = dict(metadata or {})
        failed_symbol_codes = _dedupe_symbol_codes(failed_symbols)
        updated['downstream_retry_symbols'] = failed_symbol_codes
        updated['downstream_retry_symbols_count'] = len(failed_symbol_codes)
        return updated

    def _run_activity_at(self, run: dict) -> datetime | None:
        metadata = run.get('run_metadata') if isinstance(run.get('run_metadata'), dict) else {}
        progress_at = metadata.get('last_progress_at')
        activity_at = _parse_activity_datetime(progress_at)
        if activity_at is not None:
            return activity_at
        return _parse_activity_datetime(run.get('started_at'))

    def _is_stale_running_run(self, run: dict) -> bool:
        stale_minutes = settings.eod_auto_pykrx_stale_run_minutes
        activity_at = self._run_activity_at(run)
        if run.get('status') != 'running' or stale_minutes <= 0 or activity_at is None:
            return False
        age = datetime.now(timezone.utc) - activity_at
        return age >= timedelta(minutes=stale_minutes)

    def _mark_stale_running_stage_progress_failed(
        self,
        metadata: dict | None,
        *,
        error_message: str,
    ) -> dict:
        updated = dict(metadata or {})
        stage_progress = dict(updated.get('stage_progress') or {})
        now = _utcnow_iso()
        running_stage: str | None = None

        for stage_name, raw_entry in list(stage_progress.items()):
            if not isinstance(raw_entry, dict):
                continue
            entry = dict(raw_entry)
            if str(entry.get('status') or '') != 'running':
                continue
            running_stage = stage_name
            entry['status'] = 'failed'
            entry['current_step'] = 'stale_run_failed'
            entry['current_activity'] = error_message
            entry['updated_at'] = now
            entry['completed_at'] = now
            stage_progress[stage_name] = entry

        if running_stage is not None:
            updated['stage_progress'] = stage_progress
            updated['stage'] = running_stage
            updated['current_step'] = 'stale_run_failed'
            updated['current_activity'] = error_message
            updated['last_progress_at'] = now
        else:
            current_stage = str(updated.get('stage') or '').strip()
            if current_stage:
                updated = _update_run_stage_metadata(
                    updated,
                    stage=current_stage,
                    status='failed',
                    current_step='stale_run_failed',
                    current_activity=error_message,
                    current_symbol=updated.get('current_symbol'),
                    current_period=updated.get('current_period'),
                )
            else:
                updated['last_progress_at'] = now

        updated['stale_failure_reason'] = error_message
        updated['stale_failed_at'] = now
        return updated

    def _fail_stale_running_run(self, run: dict) -> None:
        persisted_symbol_count = count_eod_run_persisted_symbols(
            run['id'],
            trade_date=run.get('trade_date'),
        )
        symbols_total = max(int(run.get('symbols_total') or 0), 0)
        metadata = run.get('run_metadata') if isinstance(run.get('run_metadata'), dict) else {}
        markets = tuple(metadata.get('markets') or DEFAULT_MARKETS)
        requested_symbols = _dedupe_symbol_codes(
            metadata.get('requested_symbols') or []
        )
        unexpected_persisted_symbols: list[str] = []
        if requested_symbols:
            persisted_symbol_codes = set(
                load_eod_run_persisted_symbols(
                    run['id'],
                    trade_date=run.get('trade_date'),
                )
            )
            requested_symbol_set = set(requested_symbols)
            symbols_succeeded = min(
                len(requested_symbol_set & persisted_symbol_codes),
                symbols_total,
            )
            failed_symbols = [
                symbol
                for symbol in requested_symbols
                if symbol not in persisted_symbol_codes
            ]
            unexpected_persisted_symbols = sorted(
                persisted_symbol_codes - requested_symbol_set
            )
        else:
            symbols_succeeded = min(persisted_symbol_count, symbols_total)
            failed_symbols = self._failed_symbol_codes_for_run(
                run,
                markets=markets,
            )
        symbols_failed = max(symbols_total - symbols_succeeded, 0)
        error_message = (
            f"Marked stale after {settings.eod_auto_pykrx_stale_run_minutes} minutes without completion."
        )
        metadata = self._mark_stale_running_stage_progress_failed(metadata, error_message=error_message)
        logger.warning('Marking stale pykrx EOD run %s as failed: %s', run['id'], error_message)
        _log_pykrx_fetch(
            'mark_stale_collection_run',
            'failed',
            run_id=run['id'],
            trade_date=run.get('trade_date'),
            symbols_total=symbols_total,
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            error_message=error_message,
        )
        metadata = self._run_metadata_with_failed_symbols(metadata, failed_symbols)
        metadata['unattempted_symbols'] = []
        metadata['unattempted_symbols_count'] = 0
        metadata['collection_coverage_checked_at'] = _utcnow_iso()
        metadata['collection_persisted_symbols_count'] = persisted_symbol_count
        metadata['collection_expected_symbols_count'] = symbols_total
        metadata['unexpected_persisted_symbols'] = (
            unexpected_persisted_symbols
        )
        metadata['unexpected_persisted_symbols_count'] = len(
            unexpected_persisted_symbols
        )
        metadata['collection_coverage_inconsistent'] = (
            persisted_symbol_count != symbols_succeeded
            or len(failed_symbols) != symbols_failed
            or bool(unexpected_persisted_symbols)
        )
        complete_eod_collection_run(
            run['id'],
            status='failed',
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            error_message=error_message,
            run_metadata=metadata,
        )

    def _run_market_data_pipeline(
        self,
        *,
        start_date: str,
        end_date: str,
        markets: Sequence[str],
        trigger: str,
        refresh_universe: bool,
        mode: str,
        limit: int | None = None,
        symbol: str | None = None,
        symbols_override: Sequence[PykrxSymbol] | None = None,
        retry_context: dict | None = None,
        selected_stages: Sequence[str] | None = None,
        chain_fundamentals_followup: bool = True,
        track_daily_pipeline: bool = False,
        point_in_time_recovery: bool = False,
        provider_admission_fence_owned: bool = False,
        owning_pipeline_run_id: int | None = None,
    ) -> dict:
        self._symbol_fundamentals_disabled_for_run = False
        self._symbol_fundamentals_failure_count_for_run = 0
        return run_market_data_pipeline(
            self,
            dependencies=PykrxPipelineDependencies(
                log_fetch=_log_pykrx_fetch,
                create_eod_collection_run=create_eod_collection_run,
                update_eod_collection_run_progress=update_eod_collection_run_progress,
                complete_eod_collection_run=complete_eod_collection_run,
                persist_daily_price_bars=persist_daily_price_bars,
                get_market_ohlcv_by_date=_get_market_ohlcv_by_date,
                get_market_ohlcv_by_ticker=_get_market_ohlcv_by_ticker,
                price_context_service=daily_price_context_service,
                technical_snapshot_service=daily_technical_snapshot_service,
                action_summary_service=daily_action_summary_service,
                compute_trending_ranks=compute_and_persist_daily_trending_ranks,
                pykrx_enrichment_service=pykrx_enrichment_service,
                signal_feature_service=daily_signal_feature_service,
                market_risk_filter_service=daily_market_risk_filter_service,
                dart_fundamentals_capture_service=dart_fundamentals_capture_service,
                settings=settings,
                normalize_yyyymmdd=_normalize_yyyymmdd,
                update_run_stage_metadata=_update_run_stage_metadata,
                daily_pipeline_run_state_service=daily_pipeline_run_state_service,
            ),
            start_date=start_date,
            end_date=end_date,
            markets=markets,
            trigger=trigger,
            refresh_universe=refresh_universe,
            mode=mode,
            limit=limit,
            symbol=symbol,
            symbols_override=symbols_override,
            retry_context=retry_context,
            selected_stages=selected_stages,
            chain_fundamentals_followup=chain_fundamentals_followup,
            track_daily_pipeline=track_daily_pipeline,
            point_in_time_recovery=point_in_time_recovery,
            provider_admission_fence_owned=(
                provider_admission_fence_owned
            ),
            owning_pipeline_run_id=owning_pipeline_run_id,
        )

    def _load_symbol_fundamentals(self, *, symbol: str, start_date: str, end_date: str):
        if getattr(self, '_symbol_fundamentals_disabled_for_run', False):
            return None
        try:
            frame = _get_market_fundamental_by_date(start_date, end_date, symbol)
        except Exception as exc:
            failure_count = int(getattr(self, '_symbol_fundamentals_failure_count_for_run', 0) or 0) + 1
            self._symbol_fundamentals_failure_count_for_run = failure_count
            if failure_count >= self.max_symbol_fundamental_failures_per_run:
                self._symbol_fundamentals_disabled_for_run = True
            logger.warning('pykrx fundamentals fetch failed for %s between %s and %s: %s', symbol, start_date, end_date, exc)
            _log_pykrx_fetch(
                'fetch_symbol_fundamentals',
                'failed',
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                error_type=type(exc).__name__,
                error_message=str(exc),
                disabled_for_run=getattr(self, '_symbol_fundamentals_disabled_for_run', False),
                failure_count=failure_count,
            )
            return None
        if getattr(frame, 'empty', True):
            _log_pykrx_fetch(
                'fetch_symbol_fundamentals',
                'empty',
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )
            return None
        _log_pykrx_fetch(
            'fetch_symbol_fundamentals',
            'completed',
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            row_count=len(frame.index),
        )
        return frame

    @staticmethod
    def _normalize_fundamental_columns(frame):
        if frame is None or getattr(frame, 'empty', True):
            return {}
        normalized: dict[date, dict[str, float | None]] = {}
        for trade_date, row in frame.sort_index().iterrows():
            normalized_trade_date = trade_date.date() if hasattr(trade_date, 'date') else trade_date
            normalized[normalized_trade_date] = {
                'per': PykrxBackfillService._to_optional_float(row.get('PER')),
                'pbr': PykrxBackfillService._to_optional_float(row.get('PBR')),
                'bps': PykrxBackfillService._to_optional_float(row.get('BPS')),
                'eps': PykrxBackfillService._to_optional_float(row.get('EPS')),
                'dividend_yield': PykrxBackfillService._to_optional_float(row.get('DIV')),
                'dps': PykrxBackfillService._to_optional_float(row.get('DPS')),
            }
        return normalized

    @staticmethod
    def _to_optional_float(value) -> float | None:
        if value is None:
            return None
        try:
            if hasattr(value, 'item'):
                value = value.item()
            converted = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(converted):
            return None
        return converted

    def _map_symbol_bars(self, frame, *, fundamentals_frame=None) -> list[dict]:
        items: list[dict] = []
        fundamentals_by_date = self._normalize_fundamental_columns(fundamentals_frame)
        for trade_date, row in frame.sort_index().iterrows():
            normalized_trade_date = trade_date.date() if hasattr(trade_date, 'date') else trade_date
            fundamentals = fundamentals_by_date.get(normalized_trade_date, {})
            items.append(
                {
                    'trade_date': normalized_trade_date,
                    'open_price': int(row.get('시가', 0) or 0),
                    'high_price': int(row.get('고가', 0) or 0),
                    'low_price': int(row.get('저가', 0) or 0),
                    'close_price': int(row.get('종가', 0) or 0),
                    'volume': int(row.get('거래량', 0) or 0),
                    'turnover': int(row.get('거래대금', 0) or 0),
                    'per': fundamentals.get('per'),
                    'pbr': fundamentals.get('pbr'),
                    'bps': fundamentals.get('bps'),
                    'eps': fundamentals.get('eps'),
                    'dividend_yield': fundamentals.get('dividend_yield'),
                    'dps': fundamentals.get('dps'),
                }
            )
        return items

    def backfill_daily_bar_fundamentals(
        self,
        *,
        start_date: str,
        end_date: str | None = None,
        markets: Sequence[str] = DEFAULT_MARKETS,
        limit: int | None = None,
        symbol: str | None = None,
    ) -> dict:
        normalized_start = _normalize_yyyymmdd(start_date)
        normalized_end = (
            _normalize_yyyymmdd(end_date)
            if end_date is not None
            else self.resolve_latest_trade_date().replace('-', '')
        )
        validate_finalized_krx_write_date(normalized_end)
        if symbol:
            target_symbols = [symbol]
        else:
            target_symbols = list_symbols_with_daily_bars_in_range(
                start_date=_normalize_iso_date(normalized_start),
                end_date=_normalize_iso_date(normalized_end),
            )
        if limit is not None:
            target_symbols = target_symbols[:limit]

        universe_by_symbol = {item.symbol: item for item in self.load_active_universe(markets=markets, limit=None)}
        updated_symbols = 0
        updated_rows = 0
        failed_symbols: list[str] = []

        for symbol_code in target_symbols:
            item = universe_by_symbol.get(symbol_code) or PykrxSymbol(symbol=symbol_code, market='KRX', name_ko=symbol_code)
            try:
                trade_dates = load_daily_price_bar_dates_for_symbol(
                    item.symbol,
                    start_date=_normalize_iso_date(normalized_start),
                    end_date=_normalize_iso_date(normalized_end),
                )
                if not trade_dates:
                    continue
                fundamentals_frame = self._load_symbol_fundamentals(
                    symbol=item.symbol,
                    start_date=normalized_start,
                    end_date=normalized_end,
                )
                fundamentals_by_date = self._normalize_fundamental_columns(fundamentals_frame)
                matched = {trade_date: fundamentals_by_date.get(trade_date, {}) for trade_date in trade_dates if trade_date in fundamentals_by_date}
                if not matched:
                    continue
                with advisory_eod_collection_locks(
                    market='KRX',
                    trade_dates=tuple(matched),
                    source='daily_price_bars',
                ) as (locks_acquired, blocked_trade_date):
                    if not locks_acquired:
                        raise RuntimeError(
                            'daily price-bar writer lock is already held for '
                            f'{blocked_trade_date}'
                        )
                    updated_count = update_daily_price_bar_fundamentals(
                        item.symbol,
                        matched,
                        source='pykrx',
                    )
                if updated_count > 0:
                    updated_symbols += 1
                    updated_rows += updated_count
            except Exception as exc:
                failed_symbols.append(item.symbol)
                logger.warning('pykrx fundamentals backfill failed for %s: %s', item.symbol, exc)
                _log_pykrx_fetch(
                    'backfill_daily_bar_fundamentals',
                    'failed',
                    symbol=item.symbol,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if self.symbol_pause_seconds:
                time.sleep(self.symbol_pause_seconds)

        result = {
            'source': 'pykrx',
            'start_date': _normalize_iso_date(normalized_start),
            'end_date': _normalize_iso_date(normalized_end),
            'symbol_count': len(target_symbols),
            'updated_symbols': updated_symbols,
            'updated_rows': updated_rows,
            'failed_symbols': failed_symbols,
        }
        _log_pykrx_fetch('backfill_daily_bar_fundamentals', 'completed', **result)
        return result


PYKRX_BACKFILL_SERVICE = PykrxBackfillService()


def _parse_markets(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return DEFAULT_MARKETS
    return tuple(item.strip().upper() for item in raw.split(',') if item.strip())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='pykrx-based EOD universe refresh and historical backfill')
    subparsers = parser.add_subparsers(dest='command', required=True)

    refresh_parser = subparsers.add_parser('refresh-universe', help='Refresh the active pykrx symbol universe')
    refresh_parser.add_argument('--markets', default=','.join(DEFAULT_MARKETS))
    refresh_parser.add_argument('--limit', type=int)

    backfill_parser = subparsers.add_parser('backfill', help='Backfill pykrx historical daily bars')
    backfill_parser.add_argument('--start-date')
    backfill_parser.add_argument('--end-date')
    backfill_parser.add_argument('--markets', default=','.join(DEFAULT_MARKETS))
    backfill_parser.add_argument('--limit', type=int)
    backfill_parser.add_argument('--symbol')
    backfill_parser.add_argument('--trigger', default='manual_cli')
    backfill_parser.add_argument('--skip-universe-refresh', action='store_true')
    backfill_parser.add_argument(
        '--stages',
        default=','.join(HISTORICAL_BACKFILL_STAGE_ORDER),
        help='Comma-separated point-in-time-safe stages.',
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    markets = _parse_markets(args.markets)

    if args.command == 'refresh-universe':
        result = PYKRX_BACKFILL_SERVICE.refresh_recent_universe(
            markets=markets,
            limit=args.limit,
            deactivate_missing=args.limit is None,
        )
        print(result)
        return 0

    if args.command == 'backfill':
        result = PYKRX_BACKFILL_SERVICE.run_backfill(
            start_date=args.start_date,
            end_date=args.end_date,
            markets=markets,
            trigger=args.trigger,
            refresh_universe=not args.skip_universe_refresh,
            limit=args.limit,
            symbol=args.symbol,
            selected_stages=tuple(
                stage.strip()
                for stage in args.stages.split(',')
                if stage.strip()
            ),
        )
        print(result)
        return 0

    parser.error(f'Unsupported command: {args.command}')
    return 2


if __name__ == '__main__':
    raise SystemExit(main())
