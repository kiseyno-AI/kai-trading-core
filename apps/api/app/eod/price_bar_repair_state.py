from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Sequence
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import sys
from typing import Any

from sqlalchemy import and_, case, false, func, or_, select

from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    DailyActionSummary,
    DailyAdvancedValueFactorResult,
    DailyBetaFactorResult,
    DailyCompositeFactorResult,
    DeciderPostDecisionEvaluation,
    DailyLowRiskFactorResult,
    DailyMarketParticipationContext,
    DailyMomentumFactorResult,
    DailyPiotroskiFilterResult,
    DailyPriceBar,
    DailyPriceBarRepairAudit,
    DailyPriceBarRepairBatch,
    DailyPriceContext,
    DailyQualityFactorResult,
    DailySignalFeature,
    DailyTechnicalSnapshot,
    DailyTrendingRank,
    DailyUnifiedValueFactorResult,
    DailyValueFactorResult,
    SignalDeciderCalendarReviewItem,
    SignalDeciderCalendarReviewRun,
    SignalDeciderDecisionSnapshot,
    SignalDeciderEvaluation,
    SignalDeciderRealizedOutcome,
    Symbol,
)
from app.eod.market_membership import market_membership_sha256
from app.repositories.eod_run_repository import advisory_eod_collection_locks


SUPPORTED_MARKETS = ('KOSPI', 'KOSDAQ')
REQUIRED_TRENDING_PERIODS = ('1d', '5d', '20d')
REQUIRED_PRICE_COVERAGE_MODELS = (
    ('price_contexts', DailyPriceContext),
    ('technical_snapshots', DailyTechnicalSnapshot),
    ('action_summaries', DailyActionSummary),
)
REQUIRED_EXACT_FACTOR_MODELS = (
    ('piotroski_filter', DailyPiotroskiFilterResult),
    ('momentum_factors', DailyMomentumFactorResult),
    ('value_factors', DailyValueFactorResult),
    ('low_risk_factors', DailyLowRiskFactorResult),
    ('beta_factors', DailyBetaFactorResult),
    ('advanced_value_factors', DailyAdvancedValueFactorResult),
    ('unified_value_factors', DailyUnifiedValueFactorResult),
    ('quality_factors', DailyQualityFactorResult),
    ('composite_factors', DailyCompositeFactorResult),
)


class PendingPriceBarRebuildError(RuntimeError):
    """Raised when Core would start with stale price-dependent outputs."""


def _normalize_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip())


def _normalize_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        resolved = value
    else:
        resolved = datetime.fromisoformat(str(value).strip())
    if resolved.tzinfo is None:
        raise ValueError('rebuild timestamp must include a timezone')
    return resolved


def _normalize_required_dates(values: object) -> tuple[date, ...]:
    if not isinstance(values, list):
        return ()
    return tuple(sorted({_normalize_date(value) for value in values}))


def _serialize_batch(row: DailyPriceBarRepairBatch) -> dict[str, Any]:
    return {
        'repair_batch_id': row.repair_batch_id,
        'status': row.status,
        'affected_symbols': list(row.affected_symbols or []),
        'affected_trade_dates': list(row.affected_trade_dates or []),
        'required_rebuild_trade_dates': list(
            row.required_rebuild_trade_dates or []
        ),
        'required_evaluation_market_date_from': (
            row.required_evaluation_market_date_from.isoformat()
        ),
        'required_evaluation_market_date_to': (
            row.required_evaluation_market_date_to.isoformat()
        ),
        'evaluation_rebuilt_at': (
            row.evaluation_rebuilt_at.isoformat()
            if row.evaluation_rebuilt_at is not None
            else None
        ),
        'factor_rebuild_metadata': dict(row.factor_rebuild_metadata or {}),
        'repaired_row_count': int(row.repaired_row_count),
        'created_at': row.created_at.isoformat(),
        'completed_at': (
            row.completed_at.isoformat() if row.completed_at is not None else None
        ),
    }


def load_pending_price_bar_rebuild_batches() -> list[dict[str, Any]]:
    with session_scope() as session:
        rows = session.scalars(
            select(DailyPriceBarRepairBatch)
            .where(DailyPriceBarRepairBatch.status == 'pending')
            .order_by(
                DailyPriceBarRepairBatch.created_at.asc(),
                DailyPriceBarRepairBatch.repair_batch_id.asc(),
            )
        ).all()
        return [_serialize_batch(row) for row in rows]


def assert_no_pending_price_bar_rebuilds() -> None:
    pending = load_pending_price_bar_rebuild_batches()
    if not pending:
        return
    batch_ids = [str(item['repair_batch_id']) for item in pending]
    raise PendingPriceBarRebuildError(
        'Core startup blocked because repaired daily price bars still require '
        f'derived-data rebuild verification: {batch_ids}'
    )


def _stable_numeric(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _daily_price_input_fingerprint(
    session: Any,
    *,
    market_date: date,
    symbols: Sequence[str] | None = None,
) -> tuple[str, int]:
    """Hash every persisted numeric bar input used by deterministic factors."""

    statement = (
        select(
            Symbol.symbol,
            DailyPriceBar.open_price,
            DailyPriceBar.high_price,
            DailyPriceBar.low_price,
            DailyPriceBar.close_price,
            DailyPriceBar.volume,
            DailyPriceBar.turnover,
            DailyPriceBar.per,
            DailyPriceBar.pbr,
            DailyPriceBar.bps,
            DailyPriceBar.eps,
            DailyPriceBar.dividend_yield,
            DailyPriceBar.dps,
        )
        .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
        .where(
            DailyPriceBar.trade_date == market_date,
            Symbol.market.in_(SUPPORTED_MARKETS),
        )
        .order_by(Symbol.symbol.asc())
    )
    normalized_symbols = sorted(
        {
            str(symbol).strip().upper()
            for symbol in symbols or ()
            if str(symbol).strip()
        }
    )
    if symbols is not None:
        statement = statement.where(Symbol.symbol.in_(normalized_symbols))
    rows = session.execute(statement).all()
    payload = [
        [str(row[0]), *(_stable_numeric(value) for value in row[1:])]
        for row in rows
    ]
    digest = hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(',', ':'),
        ).encode('utf-8')
    ).hexdigest()
    return digest, len(payload)


def _stable_json_sha256(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(',', ':'),
            sort_keys=True,
        ).encode('utf-8')
    ).hexdigest()


def _signal_feature_fingerprint(
    session: Any,
    *,
    market_date: date,
) -> tuple[str, int, datetime | None, datetime | None]:
    rows = session.execute(
        select(
            Symbol.symbol,
            DailySignalFeature.source,
            DailySignalFeature.signal_count,
            DailySignalFeature.volume_power_rank,
            DailySignalFeature.volume_power,
            DailySignalFeature.overtime_volume_rank,
            DailySignalFeature.overtime_volume,
            DailySignalFeature.overtime_fluctuation_rank,
            DailySignalFeature.overtime_fluctuation_rate,
            DailySignalFeature.feature_payload,
            DailySignalFeature.updated_at,
        )
        .join(Symbol, Symbol.id == DailySignalFeature.symbol_id)
        .where(DailySignalFeature.market_date == market_date)
        .order_by(Symbol.symbol.asc())
    ).all()
    payload = [
        [
            str(row[0]),
            str(row[1]),
            int(row[2]),
            *(_stable_numeric(value) for value in row[3:9]),
            row[9] or {},
        ]
        for row in rows
    ]
    updated_at_values = [row[10] for row in rows if row[10] is not None]
    return (
        _stable_json_sha256(payload),
        len(rows),
        min(updated_at_values) if updated_at_values else None,
        max(updated_at_values) if updated_at_values else None,
    )


def _market_participation_fingerprint(
    session: Any,
    *,
    market_date: date,
) -> tuple[str, int, datetime | None]:
    rows = session.execute(
        select(
            DailyMarketParticipationContext.market,
            DailyMarketParticipationContext.source,
            DailyMarketParticipationContext.context_payload,
            DailyMarketParticipationContext.updated_at,
        )
        .where(
            DailyMarketParticipationContext.market == 'KRX',
            DailyMarketParticipationContext.market_date == market_date,
        )
        .order_by(DailyMarketParticipationContext.id.asc())
    ).all()
    payload = [
        [str(row[0]), str(row[1]), row[2] or {}]
        for row in rows
    ]
    return (
        _stable_json_sha256(payload),
        len(rows),
        rows[0][3] if len(rows) == 1 else None,
    )


def _signal_feature_dependency_watermark(
    session: Any,
    *,
    market_date: date,
) -> datetime | None:
    timestamps = [
        session.scalar(
            select(func.max(DailyPriceBar.captured_at)).where(
                DailyPriceBar.trade_date == market_date
            )
        )
    ]
    timestamps.extend(
        session.scalar(
            select(func.max(model.updated_at)).where(
                model.market_date == market_date
            )
        )
        for _, model in REQUIRED_EXACT_FACTOR_MODELS
    )
    present = [item for item in timestamps if item is not None]
    return max(present) if present else None


def record_pending_exact_factor_rebuild(
    *,
    market_date: date,
    exact_symbols: Sequence[str],
    provider_membership_sha256: str,
    repair_started_at: datetime,
    rebuild_completed_at: datetime | None = None,
    membership_evidence_source: str | None = None,
    membership_evidence_run_id: int | None = None,
    membership_evidence_run_source: str | None = None,
) -> dict[str, Any]:
    """Attach exact-scope and raw-input evidence to affected repair batches."""

    normalized_symbols = sorted(
        {
            str(symbol).strip().upper()
            for symbol in exact_symbols
            if str(symbol).strip()
        }
    )
    if not normalized_symbols:
        raise ValueError('exact factor rebuild evidence requires symbols')
    completed_at = rebuild_completed_at or datetime.now(timezone.utc)
    if repair_started_at.tzinfo is None or completed_at.tzinfo is None:
        raise ValueError('factor rebuild evidence timestamps must include timezone')
    normalized_evidence_source = (
        str(membership_evidence_source).strip()
        if membership_evidence_source is not None
        else None
    )
    normalized_run_source = (
        str(membership_evidence_run_source).strip()
        if membership_evidence_run_source is not None
        else None
    )
    normalized_run_id = (
        int(membership_evidence_run_id)
        if membership_evidence_run_id is not None
        else None
    )
    if normalized_evidence_source == 'stored_eod_run_metadata':
        if (
            normalized_run_id is None
            or normalized_run_id <= 0
            or not normalized_run_source
        ):
            raise ValueError(
                'stored membership evidence requires a positive run id and '
                'run source'
            )
    elif normalized_evidence_source == 'live_pykrx_market_ohlcv':
        if normalized_run_id is not None or normalized_run_source is not None:
            raise ValueError(
                'live membership evidence must not claim stored run lineage'
            )
    elif normalized_evidence_source is not None:
        raise ValueError(
            'unsupported membership evidence source: '
            f'{normalized_evidence_source}'
        )
    elif normalized_run_id is not None or normalized_run_source is not None:
        raise ValueError(
            'membership evidence lineage requires an evidence source'
        )
    market_date_key = market_date.isoformat()
    ranking_membership_sha256 = market_membership_sha256(normalized_symbols)

    with session_scope(
        statement_timeout_ms=(
            settings.price_bar_repair_db_statement_timeout_ms
        ),
    ) as session:
        pending_batches = session.scalars(
            select(DailyPriceBarRepairBatch)
            .where(DailyPriceBarRepairBatch.status == 'pending')
            .order_by(DailyPriceBarRepairBatch.created_at.asc())
            .with_for_update()
        ).all()
        affected_batches = [
            batch
            for batch in pending_batches
            if batch.created_at <= repair_started_at
            and market_date_key
            in {
                str(value)
                for value in batch.required_rebuild_trade_dates or []
            }
        ]
        if not affected_batches:
            return {
                'market_date': market_date_key,
                'updated_batch_count': 0,
                'ranking_membership_sha256': ranking_membership_sha256,
            }

        input_sha256, input_row_count = _daily_price_input_fingerprint(
            session,
            market_date=market_date,
            symbols=normalized_symbols,
        )
        if input_row_count != len(normalized_symbols):
            raise RuntimeError(
                'exact factor rebuild input coverage changed before evidence '
                f'was recorded: rows={input_row_count}, '
                f'expected={len(normalized_symbols)}'
            )
        date_evidence = {
            'market_date': market_date_key,
            'exact_membership_count': len(normalized_symbols),
            'ranking_membership_sha256': ranking_membership_sha256,
            'provider_membership_sha256': str(
                provider_membership_sha256
            ).strip().lower(),
            'membership_evidence_source': normalized_evidence_source,
            'membership_evidence_run_id': normalized_run_id,
            'membership_evidence_run_source': normalized_run_source,
            'daily_price_input_sha256': input_sha256,
            'repair_started_at': repair_started_at.isoformat(),
            'rebuild_completed_at': completed_at.isoformat(),
            'stage_ids': [item[0] for item in REQUIRED_EXACT_FACTOR_MODELS],
        }
        for batch in affected_batches:
            metadata = dict(batch.factor_rebuild_metadata or {})
            date_map = dict(metadata.get('dates') or {})
            date_map[market_date_key] = date_evidence
            batch.factor_rebuild_metadata = {
                'schema_version': 1,
                'dates': date_map,
            }
        return {
            **date_evidence,
            'updated_batch_count': len(affected_batches),
        }


def _expected_price_counts(
    session: Any,
    *,
    required_dates: Sequence[date],
) -> dict[date, int]:
    rows = session.execute(
        select(
            DailyPriceBar.trade_date,
            func.count(DailyPriceBar.id),
        )
        .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
        .where(
            DailyPriceBar.trade_date.in_(required_dates),
            Symbol.market.in_(SUPPORTED_MARKETS),
        )
        .group_by(DailyPriceBar.trade_date)
    ).all()
    return {row[0]: int(row[1]) for row in rows}


def _causal_raw_dependency_watermarks(
    session: Any,
    *,
    required_dates: Sequence[date],
) -> dict[date, datetime]:
    """Return the latest repaired raw input that can affect each output date.

    A raw row from a later trade date cannot invalidate an earlier derived
    output.  A repaired earlier row can, however, affect later rolling
    technical and factor outputs.  The running maximum therefore preserves
    that forward dependency without applying the newest timestamp backward to
    unrelated historical dates.
    """

    rows = session.execute(
        select(
            DailyPriceBar.trade_date,
            func.max(DailyPriceBar.captured_at),
        )
        .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
        .where(
            DailyPriceBar.trade_date.in_(required_dates),
            Symbol.market.in_(SUPPORTED_MARKETS),
        )
        .group_by(DailyPriceBar.trade_date)
    ).all()
    per_date = {
        market_date: captured_at
        for market_date, captured_at in rows
        if captured_at is not None
    }
    running_watermark: datetime | None = None
    result: dict[date, datetime] = {}
    for market_date in sorted(set(required_dates)):
        current = per_date.get(market_date)
        if current is not None and (
            running_watermark is None or current > running_watermark
        ):
            running_watermark = current
        if running_watermark is not None:
            result[market_date] = running_watermark
    return result


def _fresh_after_causal_dependency_condition(
    *,
    market_date_column: Any,
    updated_at_column: Any,
    raw_dependency_watermarks: dict[date, datetime],
) -> Any:
    conditions = [
        and_(
            market_date_column == market_date,
            updated_at_column >= watermark,
        )
        for market_date, watermark in raw_dependency_watermarks.items()
    ]
    return or_(*conditions) if conditions else false()


def _full_coverage_stage_evidence(
    session: Any,
    *,
    model: Any,
    required_dates: Sequence[date],
    expected_counts: dict[date, int],
    rebuilt_after: datetime,
    raw_dependency_watermarks: dict[date, datetime],
) -> dict[str, Any]:
    fresh_row = case((model.updated_at >= rebuilt_after, 1))
    fresh_after_raw_row = case(
        (
            and_(
                DailyPriceBar.id.isnot(None),
                model.updated_at >= DailyPriceBar.captured_at,
            ),
            1,
        )
    )
    fresh_after_dependency_row = case(
        (
            _fresh_after_causal_dependency_condition(
                market_date_column=model.market_date,
                updated_at_column=model.updated_at,
                raw_dependency_watermarks=raw_dependency_watermarks,
            ),
            1,
        )
    )
    rows = session.execute(
        select(
            model.market_date,
            func.count(model.id),
            func.count(DailyPriceBar.id),
            func.count(fresh_row),
            func.count(fresh_after_raw_row),
            func.count(fresh_after_dependency_row),
        )
        .select_from(model)
        .join(Symbol, Symbol.id == model.symbol_id)
        .outerjoin(
            DailyPriceBar,
            and_(
                DailyPriceBar.symbol_id == model.symbol_id,
                DailyPriceBar.trade_date == model.market_date,
            ),
        )
        .where(
            model.market_date.in_(required_dates),
            Symbol.market.in_(SUPPORTED_MARKETS),
        )
        .group_by(model.market_date)
    ).all()
    observed = {
        row[0]: {
            'observed': int(row[1]),
            'matched_price_rows': int(row[2]),
            'fresh': int(row[3]),
            'fresh_after_raw': int(row[4]),
            'fresh_after_dependency': int(row[5]),
        }
        for row in rows
    }
    issues: list[dict[str, Any]] = []
    totals = {
        'expected': 0,
        'observed': 0,
        'matched_price_rows': 0,
        'fresh': 0,
        'fresh_after_raw': 0,
        'fresh_after_dependency': 0,
    }
    for market_date in required_dates:
        expected = int(expected_counts.get(market_date, 0))
        counts = observed.get(
            market_date,
            {
                'observed': 0,
                'matched_price_rows': 0,
                'fresh': 0,
                'fresh_after_raw': 0,
                'fresh_after_dependency': 0,
            },
        )
        totals['expected'] += expected
        for key in (
            'observed',
            'matched_price_rows',
            'fresh',
            'fresh_after_raw',
            'fresh_after_dependency',
        ):
            totals[key] += int(counts[key])
        if (
            expected <= 0
            or counts['observed'] != expected
            or counts['matched_price_rows'] != expected
            or counts['fresh'] != expected
            or counts['fresh_after_raw'] != expected
            or counts['fresh_after_dependency'] != expected
        ):
            issues.append(
                {
                    'market_date': market_date.isoformat(),
                    'expected': expected,
                    **counts,
                }
            )
    return {
        'ready': not issues,
        'dates_verified': len(required_dates) - len(issues),
        'dates_required': len(required_dates),
        **totals,
        'issues': issues[:20],
    }


def _exact_factor_stage_evidence(
    session: Any,
    *,
    model: Any,
    required_dates: Sequence[date],
    factor_rebuild_metadata: dict[str, Any],
    raw_dependency_watermarks: dict[date, datetime],
) -> dict[str, Any]:
    date_scopes = dict(factor_rebuild_metadata.get('dates') or {})
    issues: list[dict[str, Any]] = []
    date_results: list[dict[str, Any]] = []
    for market_date in required_dates:
        date_key = market_date.isoformat()
        scope = dict(date_scopes.get(date_key) or {})
        try:
            expected_count = int(scope['exact_membership_count'])
            expected_membership_sha256 = str(
                scope['ranking_membership_sha256']
            )
            expected_input_sha256 = str(
                scope['daily_price_input_sha256']
            )
            rebuild_started_at = _normalize_datetime(
                scope['repair_started_at']
            )
        except (KeyError, TypeError, ValueError):
            issue = {
                'market_date': date_key,
                'reason': 'factor_rebuild_scope_missing_or_invalid',
            }
            issues.append(issue)
            date_results.append(issue)
            continue

        rows = session.execute(
            select(
                Symbol.symbol,
                model.updated_at,
                DailyPriceBar.id,
                DailyPriceBar.captured_at,
            )
            .select_from(model)
            .join(Symbol, Symbol.id == model.symbol_id)
            .outerjoin(
                DailyPriceBar,
                and_(
                    DailyPriceBar.symbol_id == model.symbol_id,
                    DailyPriceBar.trade_date == model.market_date,
                ),
            )
            .where(
                model.market_date == market_date,
                Symbol.market.in_(SUPPORTED_MARKETS),
            )
            .order_by(Symbol.symbol.asc())
        ).all()
        observed_symbols = [str(row[0]) for row in rows]
        observed_membership_sha256 = market_membership_sha256(
            observed_symbols
        )
        matched_price_rows = sum(1 for row in rows if row[2] is not None)
        fresh_after_rebuild = sum(
            1
            for row in rows
            if row[1] is not None and row[1] >= rebuild_started_at
        )
        fresh_after_raw = sum(
            1
            for row in rows
            if row[1] is not None
            and row[3] is not None
            and row[1] >= row[3]
        )
        raw_dependency_watermark = raw_dependency_watermarks.get(market_date)
        fresh_after_dependency = sum(
            1
            for row in rows
            if row[1] is not None
            and raw_dependency_watermark is not None
            and row[1] >= raw_dependency_watermark
        )
        observed_input_sha256, input_row_count = (
            _daily_price_input_fingerprint(
                session,
                market_date=market_date,
                symbols=observed_symbols,
            )
        )
        result = {
            'market_date': date_key,
            'expected': expected_count,
            'observed': len(observed_symbols),
            'matched_price_rows': matched_price_rows,
            'fresh_after_rebuild': fresh_after_rebuild,
            'fresh_after_raw': fresh_after_raw,
            'fresh_after_dependency': fresh_after_dependency,
            'raw_dependency_watermark': (
                raw_dependency_watermark.isoformat()
                if raw_dependency_watermark is not None
                else None
            ),
            'input_row_count': input_row_count,
            'expected_membership_sha256': expected_membership_sha256,
            'observed_membership_sha256': observed_membership_sha256,
            'expected_daily_price_input_sha256': expected_input_sha256,
            'observed_daily_price_input_sha256': observed_input_sha256,
        }
        ready = bool(
            expected_count > 0
            and len(observed_symbols) == expected_count
            and len(set(observed_symbols)) == expected_count
            and matched_price_rows == expected_count
            and fresh_after_rebuild == expected_count
            and fresh_after_raw == expected_count
            and fresh_after_dependency == expected_count
            and input_row_count == expected_count
            and observed_membership_sha256
            == expected_membership_sha256
            and observed_input_sha256 == expected_input_sha256
        )
        result['ready'] = ready
        date_results.append(result)
        if not ready:
            issues.append(result)
    return {
        'ready': not issues,
        'dates_verified': len(required_dates) - len(issues),
        'dates_required': len(required_dates),
        'dates': date_results,
        'issues': issues[:20],
    }


def _trending_evidence(
    session: Any,
    *,
    required_dates: Sequence[date],
    rebuilt_after: datetime,
    raw_dependency_watermarks: dict[date, datetime],
) -> dict[str, Any]:
    fresh_row = case((DailyTrendingRank.updated_at >= rebuilt_after, 1))
    fresh_after_raw_row = case(
        (
            and_(
                DailyPriceBar.id.isnot(None),
                DailyTrendingRank.updated_at >= DailyPriceBar.captured_at,
            ),
            1,
        )
    )
    fresh_after_dependency_row = case(
        (
            _fresh_after_causal_dependency_condition(
                market_date_column=DailyTrendingRank.market_date,
                updated_at_column=DailyTrendingRank.updated_at,
                raw_dependency_watermarks=raw_dependency_watermarks,
            ),
            1,
        )
    )
    top_absolute_rank = case(
        (
            DailyTrendingRank.absolute_rank <= 250,
            DailyTrendingRank.absolute_rank,
        )
    )
    rows = session.execute(
        select(
            DailyTrendingRank.market_date,
            DailyTrendingRank.period,
            func.count(DailyTrendingRank.id),
            func.count(DailyPriceBar.id),
            func.count(fresh_row),
            func.count(fresh_after_raw_row),
            func.count(fresh_after_dependency_row),
            func.count(top_absolute_rank),
            func.count(func.distinct(top_absolute_rank)),
            func.min(top_absolute_rank),
            func.max(top_absolute_rank),
            func.max(DailyTrendingRank.absolute_rank),
        )
        .select_from(DailyTrendingRank)
        .join(Symbol, Symbol.id == DailyTrendingRank.symbol_id)
        .outerjoin(
            DailyPriceBar,
            and_(
                DailyPriceBar.symbol_id == DailyTrendingRank.symbol_id,
                DailyPriceBar.trade_date == DailyTrendingRank.market_date,
            ),
        )
        .where(
            DailyTrendingRank.market_date.in_(required_dates),
            DailyTrendingRank.period.in_(REQUIRED_TRENDING_PERIODS),
            Symbol.market.in_(SUPPORTED_MARKETS),
        )
        .group_by(DailyTrendingRank.market_date, DailyTrendingRank.period)
    ).all()
    observed = {
        (row[0], str(row[1])): {
            'observed': int(row[2]),
            'matched_price_rows': int(row[3]),
            'fresh': int(row[4]),
            'fresh_after_raw': int(row[5]),
            'fresh_after_dependency': int(row[6]),
            'top_absolute_rank_rows': int(row[7]),
            'distinct_top_absolute_ranks': int(row[8]),
            'minimum_absolute_rank': (
                int(row[9]) if row[9] is not None else None
            ),
            'maximum_top_absolute_rank': (
                int(row[10]) if row[10] is not None else None
            ),
            'maximum_absolute_rank': (
                int(row[11]) if row[11] is not None else None
            ),
        }
        for row in rows
    }
    issues: list[dict[str, Any]] = []
    total_rows = 0
    total_fresh = 0
    for market_date in required_dates:
        for period in REQUIRED_TRENDING_PERIODS:
            counts = observed.get(
                (market_date, period),
                {
                    'observed': 0,
                    'matched_price_rows': 0,
                    'fresh': 0,
                    'fresh_after_raw': 0,
                    'fresh_after_dependency': 0,
                    'top_absolute_rank_rows': 0,
                    'distinct_top_absolute_ranks': 0,
                    'minimum_absolute_rank': None,
                    'maximum_top_absolute_rank': None,
                    'maximum_absolute_rank': None,
                },
            )
            total_rows += int(counts['observed'])
            total_fresh += int(counts['fresh'])
            required_top_rank_count = min(
                250,
                int(counts['maximum_absolute_rank'] or 0),
            )
            if (
                counts['observed'] <= 0
                or counts['matched_price_rows'] != counts['observed']
                or counts['fresh'] != counts['observed']
                or counts['fresh_after_raw'] != counts['observed']
                or counts['fresh_after_dependency'] != counts['observed']
                or required_top_rank_count <= 0
                or counts['top_absolute_rank_rows']
                != required_top_rank_count
                or counts['distinct_top_absolute_ranks']
                != required_top_rank_count
                or counts['minimum_absolute_rank'] != 1
                or counts['maximum_top_absolute_rank']
                != required_top_rank_count
            ):
                issues.append(
                    {
                        'market_date': market_date.isoformat(),
                        'period': period,
                        'required_top_absolute_rank_count': (
                            required_top_rank_count
                        ),
                        **counts,
                    }
                )
    required_groups = len(required_dates) * len(REQUIRED_TRENDING_PERIODS)
    return {
        'ready': not issues,
        'groups_verified': required_groups - len(issues),
        'groups_required': required_groups,
        'periods_required': list(REQUIRED_TRENDING_PERIODS),
        'observed': total_rows,
        'fresh': total_fresh,
        'issues': issues[:20],
    }


def _signal_context_rebuild_evidence(
    session: Any,
    *,
    batch: DailyPriceBarRepairBatch,
    required_dates: Sequence[date],
) -> dict[str, Any]:
    metadata = dict(batch.evaluation_rebuild_metadata or {})
    date_scopes = dict(
        (metadata.get('deterministic_signal_contexts') or {}).get('dates') or {}
    )
    try:
        rebuild_started_at = _normalize_datetime(metadata['run_started_at'])
    except (KeyError, TypeError, ValueError):
        rebuild_started_at = batch.created_at
        rebuild_started_at_valid = False
    else:
        rebuild_started_at_valid = rebuild_started_at >= batch.created_at

    issues: list[dict[str, Any]] = []
    date_results: list[dict[str, Any]] = []
    for market_date in required_dates:
        date_key = market_date.isoformat()
        scope = dict(date_scopes.get(date_key) or {})
        try:
            expected_signal_count = int(scope['signal_feature_count'])
            expected_signal_sha256 = str(scope['signal_feature_sha256'])
            expected_participation_sha256 = str(
                scope['market_participation_sha256']
            )
        except (KeyError, TypeError, ValueError):
            issue = {
                'market_date': date_key,
                'reason': 'deterministic_signal_context_scope_missing_or_invalid',
            }
            issues.append(issue)
            date_results.append(issue)
            continue

        (
            observed_signal_sha256,
            observed_signal_count,
            minimum_signal_updated_at,
            maximum_signal_updated_at,
        ) = _signal_feature_fingerprint(session, market_date=market_date)
        (
            observed_participation_sha256,
            observed_participation_count,
            participation_updated_at,
        ) = _market_participation_fingerprint(
            session,
            market_date=market_date,
        )
        dependency_watermark = _signal_feature_dependency_watermark(
            session,
            market_date=market_date,
        )
        signal_fresh = bool(
            observed_signal_count == 0
            or (
                minimum_signal_updated_at is not None
                and minimum_signal_updated_at >= rebuild_started_at
                and dependency_watermark is not None
                and minimum_signal_updated_at >= dependency_watermark
            )
        )
        participation_fresh = bool(
            participation_updated_at is not None
            and participation_updated_at >= rebuild_started_at
            and dependency_watermark is not None
            and participation_updated_at >= dependency_watermark
            and (
                maximum_signal_updated_at is None
                or participation_updated_at >= maximum_signal_updated_at
            )
        )
        ready = bool(
            rebuild_started_at_valid
            and dependency_watermark is not None
            and observed_signal_count == expected_signal_count
            and observed_signal_sha256 == expected_signal_sha256
            and signal_fresh
            and observed_participation_count == 1
            and observed_participation_sha256
            == expected_participation_sha256
            and participation_fresh
        )
        result = {
            'market_date': date_key,
            'ready': ready,
            'expected_signal_feature_count': expected_signal_count,
            'observed_signal_feature_count': observed_signal_count,
            'expected_signal_feature_sha256': expected_signal_sha256,
            'observed_signal_feature_sha256': observed_signal_sha256,
            'signal_features_fresh': signal_fresh,
            'observed_market_participation_count': (
                observed_participation_count
            ),
            'expected_market_participation_sha256': (
                expected_participation_sha256
            ),
            'observed_market_participation_sha256': (
                observed_participation_sha256
            ),
            'market_participation_fresh': participation_fresh,
            'dependency_watermark': (
                dependency_watermark.isoformat()
                if dependency_watermark is not None
                else None
            ),
        }
        date_results.append(result)
        if not ready:
            issues.append(result)
    return {
        'ready': not issues,
        'dates_verified': len(required_dates) - len(issues),
        'dates_required': len(required_dates),
        'dates': date_results,
        'issues': issues[:20],
    }


def _evaluation_table_freshness(
    session: Any,
    *,
    model: Any,
    scope_conditions: Sequence[Any],
    expected_count: int,
    rebuilt_after: datetime,
    computed_at_column: Any,
) -> dict[str, Any]:
    fresh_row = case((model.updated_at >= rebuilt_after, 1))
    freshly_computed_row = case((computed_at_column >= rebuilt_after, 1))
    observed, fresh, freshly_computed = session.execute(
        select(
            func.count(model.id),
            func.count(fresh_row),
            func.count(freshly_computed_row),
        )
        .select_from(model)
        .where(*scope_conditions)
    ).one()
    observed_count = int(observed or 0)
    fresh_count = int(fresh or 0)
    freshly_computed_count = int(freshly_computed or 0)
    return {
        'ready': (
            observed_count
            == fresh_count
            == freshly_computed_count
            == expected_count
        ),
        'expected': expected_count,
        'observed': observed_count,
        'fresh': fresh_count,
        'freshly_computed': freshly_computed_count,
        'stale': observed_count - fresh_count,
    }


def _evaluation_rebuild_evidence(
    session: Any,
    *,
    batch: DailyPriceBarRepairBatch,
    raw_dependency_watermark: datetime,
) -> dict[str, Any]:
    required_from = batch.required_evaluation_market_date_from
    required_to = batch.required_evaluation_market_date_to
    metadata = dict(batch.evaluation_rebuild_metadata or {})
    recorded_from = metadata.get('market_date_from')
    recorded_to = metadata.get('market_date_to')
    recorded_horizons = {
        int(item)
        for item in metadata.get('signal_decider_horizons') or []
        if str(item).isdigit()
    }
    recorded_primary_horizons = {
        int(item)
        for item in metadata.get('signal_decider_primary_horizons') or []
        if str(item).isdigit()
    }
    affected_symbols = {
        str(item).strip().upper()
        for item in metadata.get('affected_symbols') or []
        if str(item).strip()
    }
    batch_symbols = {
        str(item).strip().upper()
        for item in batch.affected_symbols or []
        if str(item).strip()
    }
    post_feedback_ids = {
        int(item)
        for item in metadata.get('post_decision_feedback_ids') or []
    }
    signal_snapshot_ids = {
        int(item)
        for item in metadata.get('signal_decider_snapshot_ids') or []
    }
    expected_post_count = int(
        metadata.get('expected_post_decision_evaluation_count') or 0
    )
    expected_outcome_count = int(
        metadata.get('expected_signal_decider_outcome_count') or 0
    )
    expected_signal_evaluation_count = int(
        metadata.get('expected_signal_decider_evaluation_count') or 0
    )
    expected_calendar_review_count = int(
        metadata.get('expected_calendar_review_count') or 0
    )
    calendar_review_run_id = int(
        metadata.get('calendar_review_run_id') or 0
    )
    evaluation_version = str(
        metadata.get('signal_decider_evaluation_version') or ''
    )
    post_summary = dict(
        (metadata.get('post_decision') or {}).get('summary') or {}
    )
    signal_summary = dict(
        (metadata.get('signal_decider') or {}).get('summary') or {}
    )
    calendar_summary = dict(
        (metadata.get('calendar_review') or {}).get('summary') or {}
    )
    result_counts_ready = bool(
        int(post_summary.get('feedback_rows_considered') or 0)
        == int(post_summary.get('evaluations_upserted') or 0)
        == expected_post_count
        and str(signal_summary.get('status') or '') == 'completed'
        and int(signal_summary.get('not_computed_count') or 0) == 0
        and int(signal_summary.get('outcomes_upserted') or 0)
        == expected_outcome_count
        and int(signal_summary.get('evaluations_upserted') or 0)
        == expected_signal_evaluation_count
        and int(calendar_summary.get('review_rows_upserted') or 0)
        == expected_calendar_review_count
    )
    try:
        rebuild_started_at = _normalize_datetime(metadata.get('run_started_at'))
    except (TypeError, ValueError):
        rebuild_started_at = batch.created_at
        rebuild_started_at_valid = False
    else:
        rebuild_started_at_valid = rebuild_started_at >= batch.created_at
    marker_ready = bool(
        batch.evaluation_rebuilt_at is not None
        and batch.evaluation_rebuilt_at >= batch.created_at
        and recorded_from
        and recorded_to
        and _normalize_date(recorded_from) <= required_from
        and _normalize_date(recorded_to) >= required_to
        and {1, 3, 5, 10}.issubset(recorded_horizons)
        and recorded_primary_horizons == recorded_horizons
        and int(metadata.get('post_decision_window_days') or 0) == 5
        and batch_symbols.issubset(affected_symbols)
        and len(post_feedback_ids) == expected_post_count
        and expected_outcome_count
        == len(signal_snapshot_ids) * len(recorded_horizons)
        and expected_signal_evaluation_count == expected_outcome_count
        and expected_calendar_review_count
        == len(signal_snapshot_ids) * len(recorded_primary_horizons)
        and calendar_review_run_id > 0
        and evaluation_version
        and rebuild_started_at_valid
        and rebuild_started_at >= raw_dependency_watermark
        and result_counts_ready
    )
    tables = {
        'post_decision_evaluations': _evaluation_table_freshness(
            session,
            model=DeciderPostDecisionEvaluation,
            scope_conditions=(
                DeciderPostDecisionEvaluation.feedback_id.in_(
                    post_feedback_ids
                ),
                DeciderPostDecisionEvaluation.window_days == 5,
            ),
            expected_count=expected_post_count,
            rebuilt_after=rebuild_started_at,
            computed_at_column=DeciderPostDecisionEvaluation.computed_at,
        ),
        'signal_decider_realized_outcomes': _evaluation_table_freshness(
            session,
            model=SignalDeciderRealizedOutcome,
            scope_conditions=(
                SignalDeciderRealizedOutcome.decision_snapshot_id.in_(
                    signal_snapshot_ids
                ),
                SignalDeciderRealizedOutcome.horizon_days.in_(
                    recorded_horizons
                ),
            ),
            expected_count=expected_outcome_count,
            rebuilt_after=rebuild_started_at,
            computed_at_column=SignalDeciderRealizedOutcome.computed_at,
        ),
        'signal_decider_evaluations': _evaluation_table_freshness(
            session,
            model=SignalDeciderEvaluation,
            scope_conditions=(
                SignalDeciderEvaluation.decision_snapshot_id.in_(
                    signal_snapshot_ids
                ),
                SignalDeciderEvaluation.horizon_days.in_(
                    recorded_horizons
                ),
                SignalDeciderEvaluation.evaluation_version
                == evaluation_version,
            ),
            expected_count=expected_signal_evaluation_count,
            rebuilt_after=rebuild_started_at,
            computed_at_column=SignalDeciderEvaluation.evaluated_at,
        ),
        'signal_decider_calendar_review_items': _evaluation_table_freshness(
            session,
            model=SignalDeciderCalendarReviewItem,
            scope_conditions=(
                SignalDeciderCalendarReviewItem.review_run_id
                == calendar_review_run_id,
                SignalDeciderCalendarReviewItem.decision_snapshot_id.in_(
                    signal_snapshot_ids
                ),
                SignalDeciderCalendarReviewItem.primary_horizon_days.in_(
                    recorded_primary_horizons
                ),
                SignalDeciderCalendarReviewItem.evaluation_version
                == evaluation_version,
            ),
            expected_count=expected_calendar_review_count,
            rebuilt_after=rebuild_started_at,
            computed_at_column=SignalDeciderCalendarReviewItem.refreshed_at,
        ),
    }
    calendar_run = session.get(
        SignalDeciderCalendarReviewRun,
        calendar_review_run_id,
    )
    calendar_run_ready = bool(
        calendar_run is not None
        and calendar_run.status == 'completed'
        and calendar_run.completed_at is not None
        and calendar_run.completed_at >= rebuild_started_at
        and calendar_run.market_date_from <= required_from
        and calendar_run.market_date_to >= required_to
        and calendar_run.evaluation_version == evaluation_version
    )
    return {
        'ready': (
            marker_ready
            and calendar_run_ready
            and all(item['ready'] for item in tables.values())
        ),
        'marker_ready': marker_ready,
        'calendar_run_ready': calendar_run_ready,
        'calendar_review_run_id': calendar_review_run_id or None,
        'required_market_date_from': required_from.isoformat(),
        'required_market_date_to': required_to.isoformat(),
        'rebuilt_at': (
            batch.evaluation_rebuilt_at.isoformat()
            if batch.evaluation_rebuilt_at is not None
            else None
        ),
        'tables': tables,
    }


def _cached_verification_evidence(
    cache: dict[tuple[Any, ...], Any],
    key: tuple[Any, ...],
    factory: Callable[[], Any],
) -> Any:
    if key not in cache:
        cache[key] = factory()
    return deepcopy(cache[key])


def _collect_batch_verification(
    session: Any,
    *,
    batch: DailyPriceBarRepairBatch,
    evidence_cache: dict[tuple[Any, ...], Any] | None = None,
    newest_created_at_by_dates: dict[tuple[date, ...], datetime] | None = None,
) -> dict[str, Any]:
    required_dates = _normalize_required_dates(batch.required_rebuild_trade_dates)
    cache = evidence_cache if evidence_cache is not None else {}
    newest_created_at = (
        newest_created_at_by_dates.get(required_dates, batch.created_at)
        if newest_created_at_by_dates is not None
        else batch.created_at
    )
    issues: list[str] = []
    if not required_dates:
        issues.append('required_rebuild_trade_dates_empty')

    audit_count = int(
        session.scalar(
            select(func.count(DailyPriceBarRepairAudit.id)).where(
                DailyPriceBarRepairAudit.repair_batch_id
                == batch.repair_batch_id
            )
        )
        or 0
    )
    if audit_count != int(batch.repaired_row_count):
        issues.append('repair_audit_count_mismatch')

    expected_counts = _cached_verification_evidence(
        cache,
        ('expected_price_counts', required_dates),
        lambda: _expected_price_counts(
            session,
            required_dates=required_dates,
        ),
    )
    missing_price_dates = [
        market_date.isoformat()
        for market_date in required_dates
        if int(expected_counts.get(market_date, 0)) <= 0
    ]
    if missing_price_dates:
        issues.append('required_price_dates_missing')

    raw_dependency_watermarks = _cached_verification_evidence(
        cache,
        ('raw_dependency_watermarks', required_dates),
        lambda: _causal_raw_dependency_watermarks(
            session,
            required_dates=required_dates,
        ),
    )
    raw_dependency_watermark = (
        max(raw_dependency_watermarks.values())
        if raw_dependency_watermarks
        else None
    )
    if raw_dependency_watermark is None:
        issues.append('required_price_dependency_watermark_missing')
        raw_dependency_watermark = datetime.now(timezone.utc)

    expected_count_key = tuple(sorted(expected_counts.items()))
    watermark_key = tuple(sorted(raw_dependency_watermarks.items()))
    stages: dict[str, dict[str, Any]] = {}
    for stage_id, model in REQUIRED_PRICE_COVERAGE_MODELS:
        strict_key = (
            'full_coverage_stage',
            stage_id,
            model,
            required_dates,
            expected_count_key,
            watermark_key,
            newest_created_at,
        )
        evidence = _cached_verification_evidence(
            cache,
            strict_key,
            lambda: _full_coverage_stage_evidence(
                session,
                model=model,
                required_dates=required_dates,
                expected_counts=expected_counts,
                rebuilt_after=newest_created_at,
                raw_dependency_watermarks=raw_dependency_watermarks,
            ),
        )
        # A ready result at the newest threshold has every row fresh, so the
        # evidence is identical for older batches.  On failure, retain each
        # batch's original freshness result by checking its own threshold.
        if not evidence['ready'] and batch.created_at != newest_created_at:
            evidence = _cached_verification_evidence(
                cache,
                (
                    'full_coverage_stage',
                    stage_id,
                    model,
                    required_dates,
                    expected_count_key,
                    watermark_key,
                    batch.created_at,
                ),
                lambda: _full_coverage_stage_evidence(
                    session,
                    model=model,
                    required_dates=required_dates,
                    expected_counts=expected_counts,
                    rebuilt_after=batch.created_at,
                    raw_dependency_watermarks=raw_dependency_watermarks,
                ),
            )
        stages[stage_id] = evidence
        if not evidence['ready']:
            issues.append(f'{stage_id}_incomplete_or_stale')

    factor_rebuild_metadata = dict(batch.factor_rebuild_metadata or {})
    factor_metadata_key = json.dumps(
        factor_rebuild_metadata,
        ensure_ascii=True,
        separators=(',', ':'),
        sort_keys=True,
    )
    for stage_id, model in REQUIRED_EXACT_FACTOR_MODELS:
        evidence = _cached_verification_evidence(
            cache,
            (
                'exact_factor_stage',
                stage_id,
                model,
                required_dates,
                factor_metadata_key,
                watermark_key,
            ),
            lambda: _exact_factor_stage_evidence(
                session,
                model=model,
                required_dates=required_dates,
                factor_rebuild_metadata=factor_rebuild_metadata,
                raw_dependency_watermarks=raw_dependency_watermarks,
            ),
        )
        stages[stage_id] = evidence
        if not evidence['ready']:
            issues.append(f'{stage_id}_incomplete_or_stale')

    trending = _cached_verification_evidence(
        cache,
        ('trending', required_dates, watermark_key, newest_created_at),
        lambda: _trending_evidence(
            session,
            required_dates=required_dates,
            rebuilt_after=newest_created_at,
            raw_dependency_watermarks=raw_dependency_watermarks,
        ),
    )
    # Trending freshness has the same monotonic threshold property above.
    if not trending['ready'] and batch.created_at != newest_created_at:
        trending = _cached_verification_evidence(
            cache,
            (
                'trending',
                required_dates,
                watermark_key,
                batch.created_at,
            ),
            lambda: _trending_evidence(
                session,
                required_dates=required_dates,
                rebuilt_after=batch.created_at,
                raw_dependency_watermarks=raw_dependency_watermarks,
            ),
        )
    stages['trending_ranks'] = trending
    if not trending['ready']:
        issues.append('trending_ranks_incomplete_or_stale')

    signal_contexts = _signal_context_rebuild_evidence(
        session,
        batch=batch,
        required_dates=required_dates,
    )
    stages['signal_features_and_market_participation'] = signal_contexts
    if not signal_contexts['ready']:
        issues.append(
            'signal_features_or_market_participation_incomplete_or_stale'
        )

    evaluations = _evaluation_rebuild_evidence(
        session,
        batch=batch,
        raw_dependency_watermark=raw_dependency_watermark,
    )
    stages['price_dependent_decision_evaluations'] = evaluations
    if not evaluations['ready']:
        issues.append('price_dependent_decision_evaluations_incomplete_or_stale')

    return {
        'repair_batch_id': batch.repair_batch_id,
        'ready': not issues,
        'repaired_row_count': int(batch.repaired_row_count),
        'repair_audit_count': audit_count,
        'required_rebuild_trade_dates': [
            item.isoformat() for item in required_dates
        ],
        'required_price_row_counts': {
            item.isoformat(): int(expected_counts.get(item, 0))
            for item in required_dates
        },
        'raw_dependency_watermark': raw_dependency_watermark.isoformat(),
        'raw_dependency_watermarks': {
            market_date.isoformat(): watermark.isoformat()
            for market_date, watermark in raw_dependency_watermarks.items()
        },
        'missing_price_dates': missing_price_dates,
        'stages': stages,
        'issues': issues,
        'verified_at': datetime.now(timezone.utc).isoformat(),
    }


def _slim_rebuild_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        'request': dict(result.get('request') or {}),
        'summary': dict(result.get('summary') or {}),
    }


def rebuild_pending_price_dependent_evaluations() -> dict[str, Any]:
    pending = load_pending_price_bar_rebuild_batches()
    if not pending:
        return {
            'status': 'completed',
            'reason': 'no_pending_repair_batches',
            'updated_batch_count': 0,
        }

    lock_dates = {
        _normalize_date(raw_date)
        for batch in pending
        for raw_date in batch.get('required_rebuild_trade_dates') or []
    }
    evaluation_from = min(
        _normalize_date(item['required_evaluation_market_date_from'])
        for item in pending
    )
    evaluation_to = max(
        _normalize_date(item['required_evaluation_market_date_to'])
        for item in pending
    )
    lock_dates.update(
        evaluation_from + timedelta(days=offset)
        for offset in range((evaluation_to - evaluation_from).days + 1)
    )
    with advisory_eod_collection_locks(
        market='KRX',
        trade_dates=sorted(lock_dates),
        source='daily_price_bars',
    ) as (locks_acquired, blocked_trade_date):
        if not locks_acquired:
            return {
                'status': 'blocked',
                'reason': 'daily_price_bar_writer_active',
                'blocked_trade_date': blocked_trade_date,
                'updated_batch_count': 0,
            }
        return _rebuild_pending_price_dependent_evaluations_locked(pending)


def _rebuild_pending_price_dependent_evaluations_locked(
    pending: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    batch_ids = [str(item['repair_batch_id']) for item in pending]
    affected_symbols = sorted(
        {
            str(symbol).strip().upper()
            for item in pending
            for symbol in item.get('affected_symbols') or []
            if str(symbol).strip()
        }
    )
    if not affected_symbols:
        raise RuntimeError('pending repair batches have no affected symbols')
    market_date_from = min(
        _normalize_date(item['required_evaluation_market_date_from'])
        for item in pending
    )
    market_date_to = max(
        _normalize_date(item['required_evaluation_market_date_to'])
        for item in pending
    )
    required_rebuild_dates = sorted(
        {
            _normalize_date(raw_date)
            for item in pending
            for raw_date in item.get('required_rebuild_trade_dates') or []
        }
    )
    if not required_rebuild_dates:
        raise RuntimeError('pending repair batches have no rebuild dates')
    run_started_at = datetime.now(timezone.utc)

    # These services read already persisted feedback/decision snapshots only.
    # They never invoke an agent or generate missing feedback.
    from app.agent_context.decider_evaluation_service import (
        decider_post_decision_evaluation_service,
    )
    from app.agent_context.signal_decider_evaluation_maintenance_service import (
        signal_decider_evaluation_maintenance_service,
    )
    from app.agent_context.signal_decider_loop_service import (
        DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
        SIGNAL_DECIDER_EVALUATION_HORIZONS,
    )
    from app.agent_context.derived_contexts import (
        daily_market_participation_context_service,
    )
    from app.signals.daily_signal_features import (
        SIGNAL_FEATURE_SOURCE,
        daily_signal_feature_service,
    )

    with session_scope(
        statement_timeout_ms=(
            settings.price_bar_repair_db_statement_timeout_ms
        ),
    ) as session:
        existing_evaluation_versions = {
            str(item)
            for item in session.scalars(
                select(SignalDeciderEvaluation.evaluation_version)
                .join(
                    SignalDeciderDecisionSnapshot,
                    SignalDeciderDecisionSnapshot.id
                    == SignalDeciderEvaluation.decision_snapshot_id,
                )
                .where(
                    SignalDeciderDecisionSnapshot.market_date
                    >= market_date_from,
                    SignalDeciderDecisionSnapshot.market_date
                    <= market_date_to,
                    SignalDeciderDecisionSnapshot.symbol.in_(affected_symbols),
                )
                .distinct()
            ).all()
        }
    unsupported_versions = sorted(
        existing_evaluation_versions
        - {DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION}
    )
    if unsupported_versions:
        raise RuntimeError(
            'cannot safely replay historical signal-decider evaluation '
            f'versions with current rules: {unsupported_versions}'
        )

    deterministic_signal_dates: dict[str, dict[str, Any]] = {}
    for rebuild_date in required_rebuild_dates:
        signal_result = (
            daily_signal_feature_service.rebuild_stored_owned_fallback_for_market_date(
                market_date=rebuild_date
            )
        )
        participation_result = (
            daily_market_participation_context_service.rebuild_for_market_date(
                market_date=rebuild_date,
                market='KRX',
                source=SIGNAL_FEATURE_SOURCE,
            )
        )
        with session_scope(
            statement_timeout_ms=(
                settings.price_bar_repair_db_statement_timeout_ms
            ),
        ) as session:
            (
                signal_sha256,
                signal_count,
                minimum_signal_updated_at,
                maximum_signal_updated_at,
            ) = _signal_feature_fingerprint(
                session,
                market_date=rebuild_date,
            )
            (
                participation_sha256,
                participation_count,
                participation_updated_at,
            ) = _market_participation_fingerprint(
                session,
                market_date=rebuild_date,
            )
        persisted_signal_count = int(
            signal_result.get('features_persisted') or 0
        )
        captured_symbol_count = int(
            participation_result.get('captured_symbol_count') or 0
        )
        if not (
            persisted_signal_count == signal_count
            and captured_symbol_count == signal_count
            and participation_count == 1
            and participation_updated_at is not None
        ):
            raise RuntimeError(
                'deterministic signal-context rebuild count mismatch for '
                f'{rebuild_date.isoformat()}: persisted={persisted_signal_count}, '
                f'observed={signal_count}, participation_symbols='
                f'{captured_symbol_count}, participation_rows='
                f'{participation_count}'
            )
        deterministic_signal_dates[rebuild_date.isoformat()] = {
            'signal_feature_count': signal_count,
            'signal_feature_sha256': signal_sha256,
            'minimum_signal_feature_updated_at': (
                minimum_signal_updated_at.isoformat()
                if minimum_signal_updated_at is not None
                else None
            ),
            'maximum_signal_feature_updated_at': (
                maximum_signal_updated_at.isoformat()
                if maximum_signal_updated_at is not None
                else None
            ),
            'market_participation_sha256': participation_sha256,
            'market_participation_updated_at': (
                participation_updated_at.isoformat()
            ),
            'preserved_direct_source_rows': int(
                signal_result.get('preserved_direct_source_rows') or 0
            ),
        }

    post_decision_result = (
        decider_post_decision_evaluation_service.compute_evaluations(
            market_date_from=market_date_from,
            market_date_to=market_date_to,
            symbols=affected_symbols,
            window_days=5,
            force_recompute=True,
        )
    )
    signal_maintenance_result = (
        signal_decider_evaluation_maintenance_service.repair_coverage_range(
            market_date_from=market_date_from,
            market_date_to=market_date_to,
            symbols=affected_symbols,
            horizons=SIGNAL_DECIDER_EVALUATION_HORIZONS,
            primary_horizons=SIGNAL_DECIDER_EVALUATION_HORIZONS,
            evaluation_version=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
            force_recompute=True,
            refresh_calendar=True,
            fail_on_mature_gaps=True,
            trigger='price_bar_repair',
        )
    )
    if str(signal_maintenance_result.get('status') or '') != 'completed':
        raise RuntimeError(
            'signal-decider evaluation maintenance did not complete: '
            f"{signal_maintenance_result.get('summary') or {}}"
        )
    signal_decider_result = dict(
        signal_maintenance_result.get('evaluation_refresh') or {}
    )
    calendar_review_result = dict(
        signal_maintenance_result.get('calendar_refresh') or {}
    )
    post_summary = dict(post_decision_result.get('summary') or {})
    feedback_rows_considered = int(
        post_summary.get('feedback_rows_considered') or 0
    )
    post_evaluations_upserted = int(
        post_summary.get('evaluations_upserted') or 0
    )
    post_items = list(post_decision_result.get('items') or [])
    post_feedback_ids = sorted(
        {
            int((item.get('source') or {}).get('feedback_id'))
            for item in post_items
            if (item.get('source') or {}).get('feedback_id') is not None
        }
    )
    if not (
        feedback_rows_considered
        == post_evaluations_upserted
        == len(post_items)
        == len(post_feedback_ids)
    ):
        raise RuntimeError(
            'post-decision evaluation rebuild count mismatch: '
            f'feedback={feedback_rows_considered}, '
            f'upserted={post_evaluations_upserted}, '
            f'items={len(post_items)}, ids={len(post_feedback_ids)}'
        )

    signal_summary = dict(signal_decider_result.get('summary') or {})
    decisions_considered = int(
        signal_summary.get('decisions_considered') or 0
    )
    expected_signal_rows = decisions_considered * len(
        SIGNAL_DECIDER_EVALUATION_HORIZONS
    )
    outcomes_upserted = int(signal_summary.get('outcomes_upserted') or 0)
    signal_evaluations_upserted = int(
        signal_summary.get('evaluations_upserted') or 0
    )
    signal_items = list(signal_decider_result.get('items') or [])
    signal_snapshot_ids = sorted(
        {
            int(item['snapshot_id'])
            for item in signal_items
            if item.get('snapshot_id') is not None
        }
    )
    if str(signal_summary.get('status') or '') != 'completed':
        raise RuntimeError(
            'signal-decider evaluation rebuild did not complete: '
            f'{signal_summary}'
        )
    if not (
        outcomes_upserted
        == signal_evaluations_upserted
        == expected_signal_rows
        and len(signal_items) == decisions_considered
        and len(signal_snapshot_ids) == decisions_considered
    ):
        raise RuntimeError(
            'signal-decider evaluation rebuild count mismatch: '
            f'decisions={decisions_considered}, '
            f'expected_rows={expected_signal_rows}, '
            f'outcomes={outcomes_upserted}, '
            f'evaluations={signal_evaluations_upserted}, '
            f'items={len(signal_items)}, ids={len(signal_snapshot_ids)}'
        )
    calendar_summary = dict(calendar_review_result.get('summary') or {})
    calendar_run = dict(calendar_review_result.get('run') or {})
    calendar_review_run_id = int(calendar_run.get('id') or 0)
    expected_calendar_review_count = len(signal_snapshot_ids) * len(
        SIGNAL_DECIDER_EVALUATION_HORIZONS
    )
    calendar_rows_upserted = int(
        calendar_summary.get('review_rows_upserted') or 0
    )
    if not (
        calendar_review_run_id > 0
        and str(calendar_run.get('status') or '') == 'completed'
        and calendar_rows_upserted == expected_calendar_review_count
    ):
        raise RuntimeError(
            'signal-decider calendar rebuild count mismatch: '
            f'run_id={calendar_review_run_id}, '
            f'run_status={calendar_run.get("status")}, '
            f'expected_rows={expected_calendar_review_count}, '
            f'upserted={calendar_rows_upserted}'
        )
    rebuilt_at = datetime.now(timezone.utc)
    metadata = {
        'market_date_from': market_date_from.isoformat(),
        'market_date_to': market_date_to.isoformat(),
        'post_decision_window_days': 5,
        'signal_decider_horizons': list(SIGNAL_DECIDER_EVALUATION_HORIZONS),
        'signal_decider_primary_horizons': list(
            SIGNAL_DECIDER_EVALUATION_HORIZONS
        ),
        'signal_decider_evaluation_version': (
            DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        ),
        'affected_symbols': affected_symbols,
        'post_decision_feedback_ids': post_feedback_ids,
        'signal_decider_snapshot_ids': signal_snapshot_ids,
        'expected_post_decision_evaluation_count': (
            feedback_rows_considered
        ),
        'expected_signal_decider_outcome_count': expected_signal_rows,
        'expected_signal_decider_evaluation_count': expected_signal_rows,
        'expected_calendar_review_count': expected_calendar_review_count,
        'calendar_review_run_id': calendar_review_run_id,
        'run_started_at': run_started_at.isoformat(),
        'rebuilt_at': rebuilt_at.isoformat(),
        'deterministic_signal_contexts': {
            'schema_version': 1,
            'dates': deterministic_signal_dates,
        },
        'post_decision': _slim_rebuild_result(post_decision_result),
        'signal_decider': _slim_rebuild_result(signal_decider_result),
        'calendar_review': _slim_rebuild_result(calendar_review_result),
    }

    with session_scope(
        statement_timeout_ms=(
            settings.price_bar_repair_db_statement_timeout_ms
        ),
    ) as session:
        batches = session.scalars(
            select(DailyPriceBarRepairBatch)
            .where(
                DailyPriceBarRepairBatch.status == 'pending',
                DailyPriceBarRepairBatch.repair_batch_id.in_(batch_ids),
                DailyPriceBarRepairBatch.created_at <= run_started_at,
            )
            .with_for_update()
        ).all()
        if len(batches) != len(batch_ids):
            raise RuntimeError(
                'pending repair batches changed during evaluation rebuild'
            )
        for batch in batches:
            batch.evaluation_rebuilt_at = rebuilt_at
            batch.evaluation_rebuild_metadata = metadata

    return {
        'status': 'completed',
        'updated_batch_count': len(batch_ids),
        'repair_batch_ids': batch_ids,
        **metadata,
    }


def verify_pending_price_bar_rebuilds(*, apply: bool = False) -> dict[str, Any]:
    pending = load_pending_price_bar_rebuild_batches()
    if not pending:
        return {
            'status': 'completed' if apply else 'ready',
            'applied': apply,
            'reason': 'no_pending_repair_batches',
            'pending_batch_count': 0,
            'completed_batch_count': 0,
            'batches': [],
        }
    lock_dates = sorted(
        {
            _normalize_date(raw_date)
            for batch in pending
            for raw_date in batch.get('required_rebuild_trade_dates') or []
        }
    )
    if not lock_dates:
        return {
            'status': 'blocked',
            'applied': False,
            'reason': 'required_rebuild_trade_dates_empty',
            'pending_batch_count': len(pending),
            'completed_batch_count': 0,
            'batches': [],
        }
    with advisory_eod_collection_locks(
        market='KRX',
        trade_dates=lock_dates,
        source='daily_price_bars',
    ) as (locks_acquired, blocked_trade_date):
        if not locks_acquired:
            return {
                'status': 'blocked',
                'applied': False,
                'reason': 'daily_price_bar_writer_active',
                'blocked_trade_date': blocked_trade_date,
                'pending_batch_count': len(pending),
                'completed_batch_count': 0,
                'batches': [],
            }
        return _verify_pending_price_bar_rebuilds_locked(apply=apply)


def _verify_pending_price_bar_rebuilds_locked(
    *,
    apply: bool,
) -> dict[str, Any]:
    completed_at = datetime.now(timezone.utc)
    with session_scope(
        statement_timeout_ms=(
            settings.price_bar_repair_db_statement_timeout_ms
        ),
    ) as session:
        statement = (
            select(DailyPriceBarRepairBatch)
            .where(DailyPriceBarRepairBatch.status == 'pending')
            .order_by(
                DailyPriceBarRepairBatch.created_at.asc(),
                DailyPriceBarRepairBatch.repair_batch_id.asc(),
            )
        )
        if apply:
            statement = statement.with_for_update()
        batches = session.scalars(statement).all()
        if not batches:
            return {
                'status': 'completed' if apply else 'ready',
                'applied': apply,
                'reason': 'no_pending_repair_batches',
                'pending_batch_count': 0,
                'completed_batch_count': 0,
                'batches': [],
            }

        evidence_cache: dict[tuple[Any, ...], Any] = {}
        newest_created_at_by_dates: dict[tuple[date, ...], datetime] = {}
        for batch in batches:
            required_dates = _normalize_required_dates(
                batch.required_rebuild_trade_dates
            )
            newest_created_at = newest_created_at_by_dates.get(required_dates)
            if (
                newest_created_at is None
                or batch.created_at > newest_created_at
            ):
                newest_created_at_by_dates[required_dates] = batch.created_at
        evidence = [
            _collect_batch_verification(
                session,
                batch=batch,
                evidence_cache=evidence_cache,
                newest_created_at_by_dates=newest_created_at_by_dates,
            )
            for batch in batches
        ]
        all_ready = all(item['ready'] for item in evidence)
        completed_count = 0
        if apply and all_ready:
            for batch, batch_evidence in zip(batches, evidence, strict=True):
                batch.status = 'completed'
                batch.completed_at = completed_at
                batch.verification_metadata = batch_evidence
                completed_count += 1

        return {
            'status': (
                'completed' if apply and all_ready
                else ('ready' if all_ready else 'blocked')
            ),
            'applied': bool(apply and all_ready),
            'reason': None if all_ready else 'derived_rebuild_verification_failed',
            'pending_batch_count': len(batches),
            'completed_batch_count': completed_count,
            'batches': evidence,
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Verify price-dependent derived data for pending final-bar repairs. '
            'Use --complete only after every required numeric rebuild finished.'
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--rebuild-evaluations',
        action='store_true',
        help=(
            'refresh deterministic signal-feature fallbacks and market '
            'participation, then replay stored decision evaluations without '
            'calling agents'
        ),
    )
    mode.add_argument('--complete', action='store_true')
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        result = (
            rebuild_pending_price_dependent_evaluations()
            if args.rebuild_evaluations
            else verify_pending_price_bar_rebuilds(apply=args.complete)
        )
    except Exception as exc:  # noqa: BLE001
        result = {
            'status': 'failed',
            'applied': False,
            'error_type': type(exc).__name__,
            'error_message': str(exc),
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result['status'] in {'ready', 'completed'} else 2


if __name__ == '__main__':
    raise SystemExit(main())
