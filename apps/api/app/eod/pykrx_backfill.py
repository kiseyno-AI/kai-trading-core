from __future__ import annotations

import argparse
import asyncio
import logging
import math
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from app.agent_context.derived_contexts import daily_price_context_service
from app.core.audit_logging import log_fetch_event
from app.core.settings import settings
from app.eod.daily_pipeline_outcomes import (
    daily_pipeline_outcome_is_satisfied,
    daily_pipeline_prefix_outcome_is_satisfied,
    terminal_continuation_marker_is_valid,
)
from app.eod.master import get_master_symbols
from app.eod.market_membership import (
    build_market_membership_snapshot,
    validate_market_membership_snapshot,
)
from app.eod.pykrx_pipeline_support import (
    build_daily_retry_context,
    build_daily_skip_result,
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
    HISTORICAL_BACKFILL_STAGE_ORDER,
    POINT_IN_TIME_RECOVERY_BOUNDARY_TRIGGERS,
    PykrxPipelineDependencies,
    resolve_point_in_time_recovery,
    run_market_data_pipeline,
)
from app.market.session import latest_krx_trading_date
from app.eod.technical_snapshots import daily_technical_snapshot_service
from app.signals.daily_signal_features import daily_signal_feature_service
from app.signals.market_risk_filter import daily_market_risk_filter_service
from app.market_data_store import (
    advisory_eod_collection_lock,
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
AUTO_FOLLOWUP_RECOVERY_MIN_INTERVAL_SECONDS = 5 * 60
AUTO_FOLLOWUP_RECOVERY_TRIGGER = 'auto_eod_followup_resume'
AUTO_EOD_PIPELINE_ENTRYPOINT = 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due'
TERMINAL_FOLLOWUP_CONTINUATION_STAGES = (
    'agent_feedback_summaries',
    'signal_decider',
    'next_open_execution_plan',
    'signal_decider_evaluations',
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


def _log_pykrx_fetch(operation: str, status: str, **fields) -> None:
    log_fetch_event(service='pykrx', operation=operation, status=status, **fields)


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


def _stage_result_summary(stage: object) -> Mapping[str, Any]:
    if not isinstance(stage, Mapping):
        return {}
    details = stage.get('details')
    if not isinstance(details, Mapping):
        return {}
    result_summary = details.get('result_summary')
    return result_summary if isinstance(result_summary, Mapping) else {}


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
    if pipeline_status == 'blocked' and continuation_count == 0:
        pass
    elif (
        pipeline_status in {'running', 'interrupted'}
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
        if not (
            run_metadata.get('terminal_continuation_version') == 1
            and str(
                run_metadata.get('terminal_continuation_reason') or ''
            ).strip()
            == 'summary_knowledge_cutoff_repair'
            and str(run_metadata.get('resume_from_stage') or '').strip()
            == TERMINAL_FOLLOWUP_CONTINUATION_STAGES[0]
            and _parse_activity_datetime(
                run_metadata.get('terminal_continuation_claimed_at')
            )
            is not None
            and str(parent_evidence.get('status') or '').strip().lower()
            == 'blocked'
            and str(parent_evidence.get('current_stage') or '').strip()
            == 'signal_decider_evaluations'
            and isinstance(evidence_suffix, Mapping)
            and set(evidence_suffix)
            == set(TERMINAL_FOLLOWUP_CONTINUATION_STAGES)
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
            and str(
                summary_proof.get('feedback_scope_mode') or ''
            ).strip()
            == 'broad_feedback_universe'
            and evidence_int(
                summary_proof.get('requested_symbol_count')
            )
            == len(_dedupe_symbol_codes(run_metadata.get('symbols') or []))
            and summary_proof.get('requested_symbols')
            == _dedupe_symbol_codes(run_metadata.get('symbols') or [])
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
    if (
        len(resolved_stages) != len(raw_resolved_stages)
        or len(resolved_stages) != len(set(resolved_stages))
        or tuple(
            resolved_stages[-len(TERMINAL_FOLLOWUP_CONTINUATION_STAGES) :]
        )
        != TERMINAL_FOLLOWUP_CONTINUATION_STAGES
        or set(stages) != set(resolved_stages)
    ):
        return False
    stage_statuses = run_metadata.get('stage_statuses')
    if not isinstance(stage_statuses, Mapping):
        return False
    suffix_start_index = len(resolved_stages) - len(
        TERMINAL_FOLLOWUP_CONTINUATION_STAGES
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
        for stage_id in TERMINAL_FOLLOWUP_CONTINUATION_STAGES:
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
            TERMINAL_FOLLOWUP_CONTINUATION_STAGES
        ):
            return False
        metadata_prefix_size = 0
        metadata_pending_seen = False
        for stage_id in TERMINAL_FOLLOWUP_CONTINUATION_STAGES:
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
        first_unfinished_stage = TERMINAL_FOLLOWUP_CONTINUATION_STAGES[
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
        if first_status == 'pending':
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
            for stage_id in TERMINAL_FOLLOWUP_CONTINUATION_STAGES[
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

    allowed_incomplete = set(TERMINAL_FOLLOWUP_CONTINUATION_STAGES)
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
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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
            normalized = str(ticker).zfill(6)
            if not normalized.isdigit() or len(normalized) != 6:
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
            if not entry.symbol.isdigit() or len(entry.symbol) != 6:
                continue
            items.append(
                PykrxSymbol(
                    symbol=entry.symbol,
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
            symbol_code = str(row['symbol']).strip()
            market = str(row['market'] or '').strip()
            if not symbol_code.isdigit() or len(symbol_code) != 6:
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
        normalized_start = _normalize_yyyymmdd(start_date, default=normalized_end)
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
                'entrypoint': AUTO_EOD_PIPELINE_ENTRYPOINT,
                'reused_eod_collection_run_id': int(latest_eod_run['id']),
                'point_in_time_recovery': point_in_time_recovery,
                'resume_from_stage': resume_from_stage,
                'markets': list(markets),
                'requested_symbols': _dedupe_symbol_codes(
                    requested_symbols
                ),
                'failure_reason': failure_reason,
            },
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
    ) -> dict | None:
        """Resume failed post-collection stages without downloading prices again."""

        latest_eod_metadata = (
            latest_eod_run.get('run_metadata')
            if isinstance(latest_eod_run.get('run_metadata'), dict)
            else {}
        )
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

        outcome_is_satisfied = (
            daily_pipeline_prefix_outcome_is_satisfied
            if terminal_continuation_marker_is_valid(run_metadata)
            else daily_pipeline_outcome_is_satisfied
        )
        incomplete_stages = []
        for stage_id in recovery_scope:
            stage = stage_rows.get(stage_id)
            stage_outcome = (
                {**dict(stage), 'stage': stage_id}
                if isinstance(stage, Mapping)
                else {}
            )
            if not outcome_is_satisfied(stage_outcome):
                incomplete_stages.append(stage_id)
        pipeline_status = str(pipeline_run.get('status') or '').strip().lower()
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
        if (
            pipeline_run.get('trigger') != AUTO_FOLLOWUP_RECOVERY_TRIGGER
            and downstream_retry_symbols
            and downstream_resume_from_stage in recovery_scope
            and (
                not incomplete_stages
                or recovery_scope.index(downstream_resume_from_stage)
                < recovery_scope.index(incomplete_stages[0])
            )
        ):
            incomplete_stages.insert(0, downstream_resume_from_stage)
        if not incomplete_stages:
            is_followup_recovery_parent = bool(
                pipeline_run.get('trigger')
                == AUTO_FOLLOWUP_RECOVERY_TRIGGER
            )
            convergence_kwargs: dict[str, Any] = {
                'pipeline_run_id': int(pipeline_run['id']),
            }
            if is_followup_recovery_parent:
                convergence_kwargs['allow_satisfied_terminal'] = True
            pipeline_convergence = (
                daily_pipeline_run_state_service
                .converge_terminal_stages_run(**convergence_kwargs)
            )
            pipeline_parent_finalized = bool(
                pipeline_convergence.get('updated')
                or pipeline_convergence.get('reason')
                == 'already_non_resumable'
            )
            if not pipeline_parent_finalized:
                return None

            eod_convergence: dict[str, Any] | None = None
            if is_followup_recovery_parent:
                eod_convergence = self._persist_successful_followup_recovery(
                    latest_eod_run=latest_eod_run,
                    recovery_result={
                        # Every resolved recovery stage is terminally
                        # completed/skipped. That persisted stage evidence is
                        # stronger than a parent left interrupted between its
                        # last finish_stage and finish_run commits.
                        'status': 'completed',
                        'pipeline_run_id': pipeline_run.get('id'),
                        'pipeline_status': pipeline_status,
                    },
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
        resume_index = recovery_scope.index(resume_from_stage)
        recovery_stages = recovery_scope[resume_index:]
        recovery_reaches_followup_stage = any(
            stage_id in AUTO_DAILY_FOLLOWUP_STAGE_ORDER
            for stage_id in recovery_stages
        )
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

        try:
            recovery_attempts = daily_pipeline_run_state_service.count_runs(
                market_date=market_date,
                trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
                reused_eod_collection_run_id=int(latest_eod_run['id']),
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

        terminal_continuation_in_place = bool(
            recovery_attempts
            == MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE + 1
            and repair_checkpoint is None
            and _is_terminal_followup_continuation_candidate(pipeline_run)
        )
        if (
            recovery_attempts
            >= MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
            and repair_checkpoint is None
            and not terminal_continuation_in_place
        ):
            result = build_daily_skip_result(
                reason='followup_recovery_limit_reached',
                market_date=market_date,
                latest_available_trade_date=latest_available_trade_date,
                existing_run=latest_eod_run,
            )
            result['followup_recovery'] = {
                'attempts': recovery_attempts,
                'max_attempts': MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE,
                'resume_from_stage': incomplete_stages[0],
            }
            return result

        if pipeline_run.get('trigger') == AUTO_FOLLOWUP_RECOVERY_TRIGGER:
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
                    'max_attempts': MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE,
                    'retry_after_seconds': retry_after_seconds,
                    'resume_from_stage': incomplete_stages[0],
                }
                return result

        current_price_scope_preflight_required = bool(
            not terminal_continuation_in_place
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
                )

        if (
            not terminal_continuation_in_place
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
                )

        # Imported lazily because the stage orchestrator depends on this service.
        from app.eod.daily_pipeline_stages import daily_pipeline_stage_service

        if terminal_continuation_in_place:
            resume_from_stage = TERMINAL_FOLLOWUP_CONTINUATION_STAGES[0]
            recovery_stages = list(TERMINAL_FOLLOWUP_CONTINUATION_STAGES)
            persisted_markets = run_metadata.get('markets')
            recovery_markets = tuple(
                str(market or '').strip().upper()
                for market in persisted_markets
                if str(market or '').strip()
            )

        recovery_kwargs: dict[str, Any] = {
            'market_date': market_date,
            'stages': recovery_stages,
            'markets': recovery_markets,
            'force': False,
            'reuse_existing_feedback': True,
            'resume_from_stage': resume_from_stage,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'reused_eod_collection_run_id': int(latest_eod_run['id']),
        }
        if terminal_continuation_in_place:
            recovery_kwargs['existing_pipeline_run_id'] = int(
                pipeline_run['id']
            )
        persisted_point_in_time_recovery = bool(
            run_metadata.get('point_in_time_recovery')
        )
        if point_in_time_recovery or (
            terminal_continuation_in_place
            and persisted_point_in_time_recovery
        ):
            recovery_kwargs['point_in_time_recovery'] = True
        # A later follow-up can fail independently of the immediate PyKRX
        # stages, so a healthy collection resumes the full universe. When a
        # price failure coexists, however, an unscoped resume would include a
        # symbol with no same-day bar; use the durable downstream-safe scope.
        has_incomplete_followup_stage = any(
            stage_id in AUTO_DAILY_FOLLOWUP_STAGE_ORDER
            for stage_id in incomplete_stages
        )
        if terminal_continuation_in_place:
            recovery_symbols = _dedupe_symbol_codes(
                run_metadata.get('symbols') or []
            )
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
        recovery_result = dict(daily_pipeline_stage_service.run(**recovery_kwargs))
        pipeline_run_id = recovery_result.get('pipeline_run_id')
        if (
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
        )
        if pipeline_parent_finalized:
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
        recovery_result.setdefault('triggered', True)
        recovery_result.setdefault(
            'reason',
            (
                'terminal_followup_continuation_resumed'
                if terminal_continuation_in_place
                else 'incomplete_followups_resumed'
            ),
        )
        recovery_result.setdefault('market_date', market_date)
        recovery_result['latest_available_trade_date'] = latest_available_trade_date
        recovery_result['source'] = 'pykrx'
        recovery_result['followup_recovery'] = {
            'attempt': (
                recovery_attempts
                if terminal_continuation_in_place
                else recovery_attempts + 1
            ),
            'max_attempts': (
                max(
                    recovery_attempts,
                    MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                    + (1 if repair_checkpoint is not None else 0),
                )
                if terminal_continuation_in_place
                else MAX_AUTO_FOLLOWUP_RECOVERY_RUNS_PER_TRADE_DATE
                + (1 if repair_checkpoint is not None else 0)
            ),
            'resumed_from_pipeline_run_id': pipeline_run.get('id'),
            'reused_eod_collection_run_id': latest_eod_run.get('id'),
            'resume_from_stage': resume_from_stage,
            'stages': recovery_stages,
            'symbols': recovery_symbols,
            'state_convergence': convergence,
        }
        if terminal_continuation_in_place:
            recovery_result['followup_recovery'][
                'terminal_continuation_in_place'
            ] = True
        if repair_checkpoint is not None:
            recovery_result['followup_recovery'][
                'repair_checkpoint_pipeline_run_id'
            ] = repair_checkpoint.get('pipeline_run_id')
        return recovery_result

    def _persist_successful_followup_recovery(
        self,
        *,
        latest_eod_run: Mapping[str, Any],
        recovery_result: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not daily_pipeline_outcome_is_satisfied(recovery_result):
            return {'updated': False, 'reason': 'recovery_not_completed'}

        metadata = (
            dict(latest_eod_run.get('run_metadata') or {})
            if isinstance(latest_eod_run.get('run_metadata'), dict)
            else {}
        )
        downstream_retry_symbols = _dedupe_symbol_codes(
            metadata.get('downstream_retry_symbols') or []
        )
        if (
            latest_eod_run.get('symbols_total') is None
            or latest_eod_run.get('symbols_succeeded') is None
        ):
            return {'updated': False, 'reason': 'collection_counts_unavailable'}

        price_retry_symbols = self._metadata_retry_symbol_codes(
            dict(latest_eod_run)
        )
        completed_at = _utcnow_iso()
        metadata.pop('downstream_retry_resume_from_stage', None)
        metadata.pop(
            'inherited_downstream_retry_resume_from_stage',
            None,
        )
        metadata.update(
            {
                'downstream_retry_symbols': [],
                'downstream_retry_symbols_count': 0,
                'inherited_downstream_retry_symbols': [],
                'inherited_downstream_retry_symbols_count': 0,
                'downstream_recovery_symbols_count': len(
                    downstream_retry_symbols
                ),
                'downstream_recovery_completed_at': completed_at,
                'downstream_recovery_pipeline_run_id': recovery_result.get(
                    'pipeline_run_id'
                ),
                'followup_recovery_required': False,
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

        with advisory_eod_collection_lock(market='KRX', trade_date=market_date_iso, source='pykrx') as lock_acquired:
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
    ) -> dict:
        requested_stages = [stage for stage in PYKRX_STAGE_SEQUENCE if stage in set(stages)]
        if not requested_stages:
            raise ValueError('At least one pykrx stage must be requested.')
        if 'price_collection' not in requested_stages:
            raise ValueError('price_collection must be included when running grouped pykrx stages.')

        normalized_market_date = _normalize_yyyymmdd(market_date)
        market_date_iso = _normalize_iso_date(normalized_market_date)
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

        with advisory_eod_collection_lock(market='KRX', trade_date=market_date_iso, source='pykrx') as lock_acquired:
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
        metadata_retry_symbols = self._metadata_retry_symbol_codes(run)
        scoped_retry_symbols = (
            requested_symbols or metadata_retry_symbols
        )
        missing_scoped_symbols = [
            symbol
            for symbol in scoped_retry_symbols
            if symbol not in persisted_symbols
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
        if converted != converted:
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
                updated_count = update_daily_price_bar_fundamentals(item.symbol, matched, source='pykrx')
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
