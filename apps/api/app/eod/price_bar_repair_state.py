from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping, Sequence
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
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
    EODCollectionRun,
    SignalDeciderCalendarReviewItem,
    SignalDeciderCalendarReviewRun,
    SignalDeciderDecisionSnapshot,
    SignalDeciderEvaluation,
    SignalDeciderRealizedOutcome,
    Symbol,
)
from app.eod.market_membership import (
    market_membership_sha256,
    validate_market_membership_snapshot,
)
from app.repositories.eod_run_repository import advisory_eod_collection_locks


SUPPORTED_MARKETS = ('KOSPI', 'KOSDAQ')
MINIMUM_MARKET_MEMBERSHIP = {'KOSPI': 700, 'KOSDAQ': 1200}
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
PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE = (
    'pending_missing_bar_repair_metadata'
)
FACTOR_REBUILD_METADATA_SCHEMA_VERSION = 3
SUPPORTED_FACTOR_REBUILD_METADATA_SCHEMA_VERSIONS = frozenset({2, 3})


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
        'verification_metadata': dict(row.verification_metadata or {}),
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
        .where(DailyPriceBar.trade_date == market_date)
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
    else:
        statement = statement.where(Symbol.market.in_(SUPPORTED_MARKETS))
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


def exact_daily_price_bar_scope_fingerprint(
    session: Any,
    *,
    market_date: date,
    symbols: Sequence[str],
) -> tuple[str, int]:
    """Hash an exact historical date/symbol scope without current classification.

    Point-in-time repair evidence must remain valid when a symbol is later
    reclassified in ``symbols.market``.  The provider-reviewed symbol set is
    therefore the only membership authority for this fingerprint.
    """

    normalized_symbols = sorted(
        {
            str(symbol).strip().upper()
            for symbol in symbols
            if str(symbol).strip()
        }
    )
    if not normalized_symbols:
        return _stable_json_sha256([]), 0
    rows = session.execute(
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
            Symbol.symbol.in_(normalized_symbols),
        )
        .order_by(Symbol.symbol.asc())
    ).all()
    payload = [
        [str(row[0]), *(_stable_numeric(value) for value in row[1:])]
        for row in rows
    ]
    return _stable_json_sha256(payload), len(payload)


def _stable_json_sha256(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(',', ':'),
            sort_keys=True,
        ).encode('utf-8')
    ).hexdigest()


def _stable_output_value(value: object) -> object:
    """Normalize persisted derived values for deterministic fingerprints."""

    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _stable_output_value(item)
            for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_stable_output_value(item) for item in value]
    return str(value)


def _derived_output_fingerprint(
    session: Any,
    *,
    model: Any,
    market_date: date,
    symbols: Sequence[str] | None,
    periods: Sequence[str] | None = None,
) -> tuple[str, int]:
    """Hash all non-identity output columns for one derived scope."""

    normalized_symbols = (
        tuple(
            sorted(
                {
                    str(symbol).strip().upper()
                    for symbol in symbols
                    if str(symbol).strip()
                }
            )
        )
        if symbols is not None
        else None
    )
    if symbols is not None and not normalized_symbols:
        raise ValueError('derived output fingerprint scope must not be empty')
    excluded_columns = {
        'id',
        'symbol_id',
        'market_date',
        'created_at',
        'updated_at',
        'captured_at',
    }
    output_columns = tuple(
        column.name
        for column in model.__table__.columns
        if column.name not in excluded_columns
    )
    statement = (
        select(
            Symbol.symbol,
            *(getattr(model, name) for name in output_columns),
        )
        .join(model, model.symbol_id == Symbol.id)
        .where(model.market_date == market_date)
        .order_by(
            Symbol.symbol.asc(),
            *(
                (model.period.asc(),)
                if hasattr(model, 'period')
                else ()
            ),
        )
    )
    if normalized_symbols is not None:
        statement = statement.where(
            Symbol.symbol.in_(normalized_symbols)
        )
    if periods is not None:
        if not hasattr(model, 'period'):
            raise ValueError('period scope is unsupported for this model')
        normalized_periods = tuple(
            sorted({str(period).strip().lower() for period in periods})
        )
        if not normalized_periods:
            raise ValueError('derived output period scope must not be empty')
        statement = statement.where(model.period.in_(normalized_periods))
    rows = session.execute(statement).all()
    payload = [
        [
            str(row[0]),
            {
                name: _stable_output_value(value)
                for name, value in zip(output_columns, row[1:], strict=True)
            },
        ]
        for row in rows
    ]
    return _stable_json_sha256(payload), len(payload)


def _signal_feature_fingerprint(
    session: Any,
    *,
    market_date: date,
    symbols: Sequence[str] | None = None,
) -> tuple[str, int, datetime | None, datetime | None]:
    statement = (
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
    )
    if symbols is not None:
        normalized_symbols = tuple(
            sorted(
                {
                    str(symbol).strip().upper()
                    for symbol in symbols
                    if str(symbol).strip()
                }
            )
        )
        if not normalized_symbols:
            raise ValueError(
                'signal-feature fingerprint scope must not be empty'
            )
        statement = statement.where(
            Symbol.symbol.in_(normalized_symbols)
        )
    rows = session.execute(statement).all()
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


def _factor_evidence_symbols_for_batch(
    *,
    batch: DailyPriceBarRepairBatch,
    market_date: date,
    rebuilt_symbols: Sequence[str],
) -> tuple[str, ...]:
    """Resolve the exact historical scope that this batch must verify.

    Missing-bar batches freeze a separate point-in-time membership manifest.
    The rebuild and evidence must use that exact ranking population; a live or
    stored superset would change every cross-sectional rank.  Conflicting
    pending manifests for one date must therefore block before any rewrite.
    """

    rebuilt = tuple(sorted(set(rebuilt_symbols)))
    metadata = dict(getattr(batch, 'verification_metadata', None) or {})
    recorded = dict(metadata.get('repair_evidence') or {})
    if recorded.get('repair_kind') != 'missing_bar_insert':
        return rebuilt
    try:
        if str(recorded['schema_version']) != 'missing_bar_insert_v3':
            raise ValueError('unsupported missing-bar evidence version')
        raw_scope = dict(
            dict(recorded['price_dependent_membership_by_date'])[
                market_date.isoformat()
            ]
        )
        raw_symbols = [
            str(symbol).strip().upper()
            for symbol in raw_scope['symbols']
            if str(symbol).strip()
        ]
        frozen_symbols = tuple(sorted(set(raw_symbols)))
        frozen_count = int(raw_scope['symbol_count'])
        frozen_sha256 = str(raw_scope['membership_sha256']).strip().lower()
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            'pending missing-bar batch has invalid frozen factor scope: '
            f'batch={batch.repair_batch_id}, date={market_date.isoformat()}'
        ) from exc
    if (
        not frozen_symbols
        or len(raw_symbols) != len(frozen_symbols)
        or frozen_count != len(frozen_symbols)
        or frozen_sha256 != market_membership_sha256(frozen_symbols)
    ):
        raise RuntimeError(
            'pending missing-bar batch has invalid frozen factor scope: '
            f'batch={batch.repair_batch_id}, date={market_date.isoformat()}'
        )
    if frozen_symbols != rebuilt:
        raise RuntimeError(
            'exact factor rebuild ranking scope does not exactly match '
            'pending frozen membership: '
            f'batch={batch.repair_batch_id}, date={market_date.isoformat()}, '
            f'rebuilt_count={len(rebuilt)}, '
            f'frozen_count={len(frozen_symbols)}, '
            f'rebuilt_sha256={market_membership_sha256(rebuilt)}, '
            f'frozen_sha256={market_membership_sha256(frozen_symbols)}'
        )
    return frozen_symbols


def _frozen_signal_feature_symbols_for_batch(
    *,
    batch: Mapping[str, Any] | DailyPriceBarRepairBatch,
    market_date: date,
) -> tuple[str, ...] | None:
    """Return one batch's validated point-in-time signal-feature scope."""

    if isinstance(batch, Mapping):
        metadata = dict(batch.get('verification_metadata') or {})
        repair_batch_id = str(batch.get('repair_batch_id') or '')
    else:
        metadata = dict(
            getattr(batch, 'verification_metadata', None) or {}
        )
        repair_batch_id = str(
            getattr(batch, 'repair_batch_id', '') or ''
        )
    recorded = dict(metadata.get('repair_evidence') or {})
    if recorded.get('repair_kind') != 'missing_bar_insert':
        return None
    try:
        if str(recorded['schema_version']) != 'missing_bar_insert_v3':
            raise ValueError('unsupported missing-bar evidence version')
        raw_scope = dict(
            dict(recorded['price_dependent_membership_by_date'])[
                market_date.isoformat()
            ]
        )
        raw_symbols = [
            str(symbol).strip().upper()
            for symbol in raw_scope['symbols']
            if str(symbol).strip()
        ]
        frozen_symbols = tuple(sorted(set(raw_symbols)))
        frozen_count = int(raw_scope['symbol_count'])
        frozen_sha256 = str(
            raw_scope['membership_sha256']
        ).strip().lower()
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            'pending missing-bar batch has invalid frozen signal-feature '
            f'scope: batch={repair_batch_id}, '
            f'date={market_date.isoformat()}'
        ) from exc
    if (
        not frozen_symbols
        or len(raw_symbols) != len(frozen_symbols)
        or frozen_count != len(frozen_symbols)
        or frozen_sha256 != market_membership_sha256(frozen_symbols)
    ):
        raise RuntimeError(
            'pending missing-bar batch has invalid frozen signal-feature '
            f'scope: batch={repair_batch_id}, '
            f'date={market_date.isoformat()}'
        )
    return frozen_symbols


def _pending_frozen_signal_feature_scopes(
    *,
    pending: Sequence[Mapping[str, Any]],
    required_dates: Sequence[date],
) -> tuple[
    dict[date, tuple[str, ...]],
    dict[date, dict[str, tuple[str, ...]]],
]:
    """Build union write scopes and every distinct batch verification scope."""

    union_symbols_by_date: dict[date, set[str]] = {
        market_date: set() for market_date in required_dates
    }
    distinct_scopes_by_date: dict[
        date,
        dict[str, tuple[str, ...]],
    ] = {market_date: {} for market_date in required_dates}
    required_date_set = set(required_dates)
    for batch in pending:
        batch_dates = {
            _normalize_date(value)
            for value in batch.get('required_rebuild_trade_dates') or []
        }
        for market_date in sorted(batch_dates.intersection(required_date_set)):
            frozen_symbols = _frozen_signal_feature_symbols_for_batch(
                batch=batch,
                market_date=market_date,
            )
            if frozen_symbols is None:
                continue
            membership_sha256 = market_membership_sha256(
                frozen_symbols
            )
            union_symbols_by_date[market_date].update(frozen_symbols)
            distinct_scopes_by_date[market_date][
                membership_sha256
            ] = frozen_symbols
    conflicting_dates = {
        market_date: sorted(scopes)
        for market_date, scopes in distinct_scopes_by_date.items()
        if len(scopes) > 1
    }
    if conflicting_dates:
        conflict_preview = {
            market_date.isoformat(): scope_hashes
            for market_date, scope_hashes in sorted(
                conflicting_dates.items()
            )
        }
        raise RuntimeError(
            'pending missing-bar batches have conflicting frozen signal '
            f'membership scopes: {conflict_preview}'
        )
    return (
        {
            market_date: tuple(sorted(symbols))
            for market_date, symbols in union_symbols_by_date.items()
            if symbols
        },
        {
            market_date: scopes
            for market_date, scopes in distinct_scopes_by_date.items()
            if scopes
        },
    )


def resolve_pending_frozen_price_dependent_membership(
    *,
    market_date: str | date,
    pending: Sequence[Mapping[str, Any]] | None = None,
) -> tuple[str, ...] | None:
    """Return the sole durable frozen scope for a pending repair date.

    Exact factor and trending rebuilds must establish this authority before
    any mutation.  Live provider membership is corroboration only once a
    missing-bar batch has frozen its point-in-time ranking population.
    """

    normalized_date = _normalize_date(market_date)
    pending_batches = list(
        load_pending_price_bar_rebuild_batches()
        if pending is None
        else pending
    )
    scopes: dict[str, tuple[str, ...]] = {}
    for batch in pending_batches:
        required_dates = {
            _normalize_date(value)
            for value in batch.get('required_rebuild_trade_dates') or []
        }
        if normalized_date not in required_dates:
            continue
        frozen_symbols = _frozen_signal_feature_symbols_for_batch(
            batch=batch,
            market_date=normalized_date,
        )
        if frozen_symbols is None:
            continue
        scopes[
            market_membership_sha256(frozen_symbols)
        ] = frozen_symbols
    if len(scopes) > 1:
        raise RuntimeError(
            'pending price-bar batches have conflicting frozen '
            'price-dependent memberships: '
            f'date={normalized_date.isoformat()}, '
            f'hashes={sorted(scopes)}'
        )
    return next(iter(scopes.values()), None)


def _is_sha256(value: object) -> bool:
    normalized = str(value or '').strip().lower()
    return len(normalized) == 64 and all(
        character in '0123456789abcdef' for character in normalized
    )


def _normalize_pending_provider_exclusions(
    value: object,
) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise ValueError('pending provider exclusions are not a list')
    normalized: list[dict[str, str]] = []
    seen_codes: set[str] = set()
    for raw_entry in value:
        if not isinstance(raw_entry, Mapping):
            raise ValueError('pending provider exclusion is not an object')
        raw_market = str(raw_entry.get('market') or '').strip()
        raw_code = str(raw_entry.get('code') or '').strip()
        market = raw_market.upper()
        code = raw_code.upper()
        if (
            raw_market != market
            or market not in ('KOSPI', 'KOSDAQ', 'KONEX')
            or raw_code != code
            or len(code) != 6
            or not code[:4].isdigit()
            or not code[4:].isalnum()
            or not code.isascii()
            or code.isdigit()
        ):
            raise ValueError('pending provider exclusion is not canonical')
        if code in seen_codes:
            raise ValueError('pending provider exclusion is duplicated')
        seen_codes.add(code)
        normalized.append({'market': market, 'code': code})
    return sorted(normalized, key=lambda entry: (entry['market'], entry['code']))


def _pending_provider_exclusions_sha256(
    entries: Sequence[Mapping[str, str]],
) -> str:
    return _stable_json_sha256(
        [
            {
                'market': str(entry['market']),
                'code': str(entry['code']),
            }
            for entry in entries
        ]
    )


def _default_exact_scope_input_fingerprint(
    market_date: date,
    symbols: Sequence[str],
) -> tuple[str, int]:
    with session_scope() as session:
        return exact_daily_price_bar_scope_fingerprint(
            session,
            market_date=market_date,
            symbols=symbols,
        )


def resolve_pending_exact_market_membership_evidence(
    *,
    market_date: str | date,
    pending: Sequence[Mapping[str, Any]] | None = None,
    input_fingerprint_loader: (
        Callable[[date, Sequence[str]], tuple[str, int]] | None
    ) = None,
) -> dict[str, Any] | None:
    """Resolve strict historical PyKRX authority from pending repair proof.

    This is deliberately narrower than the frozen ranking-scope resolver.  It
    accepts only provider-reviewed target membership or an exact-date PyKRX
    fallback manifest, verifies the full market split and excluded provider
    codes, rejects conflicting batches, and rechecks the persisted price input
    before returning authority to an exact factor repair.
    """

    normalized_date = _normalize_date(market_date)
    market_date_key = normalized_date.isoformat()
    pending_batches = list(
        load_pending_price_bar_rebuild_batches()
        if pending is None
        else pending
    )
    proofs: dict[str, dict[str, Any]] = {}
    proof_batch_ids: dict[str, set[str]] = {}

    for batch in pending_batches:
        required_dates = {
            _normalize_date(value)
            for value in batch.get('required_rebuild_trade_dates') or []
        }
        if normalized_date not in required_dates:
            continue
        batch_id = str(batch.get('repair_batch_id') or '').strip()
        if not batch_id:
            raise RuntimeError('pending membership proof has no repair batch id')
        recorded = dict(
            dict(batch.get('verification_metadata') or {}).get(
                'repair_evidence'
            )
            or {}
        )
        if recorded.get('repair_kind') != 'missing_bar_insert':
            continue
        if str(recorded.get('schema_version') or '') != 'missing_bar_insert_v3':
            raise RuntimeError(
                f'unsupported pending membership evidence: batch={batch_id}'
            )
        try:
            raw_scope = dict(
                dict(recorded['price_dependent_membership_by_date'])[
                    market_date_key
                ]
            )
            frozen_symbols = tuple(
                sorted(str(value).strip().upper() for value in raw_scope['symbols'])
            )
            symbol_count = int(raw_scope['symbol_count'])
            membership_sha256 = str(
                raw_scope['membership_sha256']
            ).strip().lower()
            input_sha256 = str(
                raw_scope['daily_price_input_sha256']
            ).strip().lower()
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                'pending membership scope is missing or invalid: '
                f'batch={batch_id}, date={market_date_key}'
            ) from exc
        if (
            not frozen_symbols
            or len(frozen_symbols) != len(set(frozen_symbols))
            or any(len(symbol) != 6 or not symbol.isdigit() for symbol in frozen_symbols)
            or symbol_count != len(frozen_symbols)
            or membership_sha256 != market_membership_sha256(frozen_symbols)
            or not _is_sha256(input_sha256)
        ):
            raise RuntimeError(
                'pending membership scope failed exact validation: '
                f'batch={batch_id}, date={market_date_key}'
            )

        basis = str(raw_scope.get('basis') or '').strip()
        symbols_by_market: dict[str, list[str]]
        excluded_codes: list[dict[str, str]]
        if basis == 'reviewed_live_provider_membership':
            if (
                _normalize_date(recorded.get('market_date')) != normalized_date
                or recorded.get('provider') != 'pykrx'
            ):
                raise RuntimeError('target pending provider identity is invalid')
            provider_manifest = dict(recorded.get('provider_membership_manifest') or {})
            raw_market_entries = dict(provider_manifest.get('markets') or {})
            all_provider_symbols = [
                str(symbol).strip().upper()
                for market in ('KOSPI', 'KOSDAQ', 'KONEX')
                for symbol in dict(raw_market_entries.get(market) or {}).get(
                    'symbols', []
                )
            ]
            validated = validate_market_membership_snapshot(
                provider_manifest,
                requested_symbols=all_provider_symbols,
                markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            )
            if (
                validated is None
                or int(recorded.get('expected_provider_membership_count') or -1)
                != len(all_provider_symbols)
                or str(
                    recorded.get('expected_provider_membership_sha256') or ''
                ).strip().lower()
                != market_membership_sha256(all_provider_symbols)
                or provider_manifest.get('membership_sha256')
                != market_membership_sha256(all_provider_symbols)
            ):
                raise RuntimeError('target pending provider manifest is invalid')
            symbols_by_market = {
                market: sorted(str(symbol) for symbol in validated[market])
                for market in SUPPORTED_MARKETS
            }
            expected_ranking = tuple(
                sorted(
                    symbol
                    for market in SUPPORTED_MARKETS
                    for symbol in symbols_by_market[market]
                )
            )
            if (
                expected_ranking != frozen_symbols
                or list(recorded.get('price_dependent_membership_symbols') or [])
                != list(frozen_symbols)
                or int(recorded.get('price_dependent_membership_count') or -1)
                != len(frozen_symbols)
                or str(
                    recorded.get('price_dependent_membership_sha256') or ''
                ).strip().lower()
                != membership_sha256
            ):
                raise RuntimeError('target pending ranking scope is invalid')
            excluded_codes = _normalize_pending_provider_exclusions(
                recorded.get('provider_excluded_ohlcv_codes') or []
            )
            expected_exclusion_sha256 = str(
                recorded.get('provider_excluded_ohlcv_code_sha256') or ''
            ).strip().lower()
        elif basis == 'reviewed_live_exact_date_pykrx_membership':
            if (
                raw_scope.get('eod_run_id') is not None
                or raw_scope.get('eod_run_source') != 'pykrx'
                or raw_scope.get('source') != 'pykrx'
                or raw_scope.get('universe_source') != 'pykrx_market_ohlcv'
                or raw_scope.get('market_date') != market_date_key
                or raw_scope.get('markets') != list(SUPPORTED_MARKETS)
                or raw_scope.get('daily_price_input_count') != len(frozen_symbols)
            ):
                raise RuntimeError('fallback pending provider identity is invalid')
            membership = raw_scope.get('market_membership')
            validated = validate_market_membership_snapshot(
                membership,
                requested_symbols=list(frozen_symbols),
                markets=SUPPORTED_MARKETS,
            )
            if validated is None:
                raise RuntimeError('fallback pending market manifest is invalid')
            symbols_by_market = {
                market: sorted(str(symbol) for symbol in validated[market])
                for market in SUPPORTED_MARKETS
            }
            if tuple(
                sorted(
                    symbol
                    for market in SUPPORTED_MARKETS
                    for symbol in symbols_by_market[market]
                )
            ) != frozen_symbols:
                raise RuntimeError('fallback pending ranking scope is invalid')
            excluded_codes = _normalize_pending_provider_exclusions(
                raw_scope.get('excluded_ohlcv_codes') or []
            )
            expected_exclusion_sha256 = str(
                raw_scope.get('excluded_ohlcv_code_sha256') or ''
            ).strip().lower()
            fallback_manifests = {
                str(raw_date): {
                    key: value
                    for key, value in dict(scope).items()
                    if key not in {'eod_run_id', 'eod_run_source'}
                }
                for raw_date, scope in dict(
                    recorded['price_dependent_membership_by_date']
                ).items()
                if dict(scope).get('basis')
                == 'reviewed_live_exact_date_pykrx_membership'
            }
            fallback_bundle_sha256 = _stable_json_sha256(
                {
                    'schema_version': 'missing_bar_downstream_membership_v1',
                    'manifests': fallback_manifests,
                }
            )
            if (
                int(recorded.get('downstream_fallback_manifest_count') or 0)
                != len(fallback_manifests)
                or str(
                    recorded.get('downstream_fallback_bundle_sha256') or ''
                ).strip().lower()
                != fallback_bundle_sha256
            ):
                raise RuntimeError('fallback pending proof bundle is invalid')
        elif basis == 'validated_eod_run_market_membership':
            # The canonical EOD run remains the authority for this basis.  If
            # that run is missing or invalid, callers must fail closed rather
            # than reinterpret the repair metadata as a substitute.
            continue
        else:
            raise RuntimeError(
                f'unsupported pending membership basis: {basis or "missing"}'
            )

        if (
            expected_exclusion_sha256
            != _pending_provider_exclusions_sha256(excluded_codes)
        ):
            raise RuntimeError('pending provider exclusion hash is invalid')
        for market in SUPPORTED_MARKETS:
            if len(symbols_by_market[market]) < MINIMUM_MARKET_MEMBERSHIP[market]:
                raise RuntimeError(
                    f'{market} pending membership is below the safety floor'
                )
        protected_codes_by_market = {
            market: sorted(
                entry['code']
                for entry in excluded_codes
                if entry['market'] == market
            )
            for market in SUPPORTED_MARKETS
        }
        protected_codes = sorted(
            code
            for market in SUPPORTED_MARKETS
            for code in protected_codes_by_market[market]
        )
        full_provider_membership = sorted([*frozen_symbols, *protected_codes])
        normalized_proof = {
            'schema_version': 1,
            'market_date': market_date_key,
            'source': 'pykrx',
            'symbols_by_market': symbols_by_market,
            'protected_provider_codes_by_market': protected_codes_by_market,
            'ranking_membership_count': len(frozen_symbols),
            'ranking_membership_sha256': membership_sha256,
            'provider_membership_count': len(full_provider_membership),
            'provider_membership_sha256': market_membership_sha256(
                full_provider_membership
            ),
            'daily_price_input_count': len(frozen_symbols),
            'daily_price_input_sha256': input_sha256,
        }
        proof_sha256 = _stable_json_sha256(normalized_proof)
        proofs[proof_sha256] = normalized_proof
        proof_batch_ids.setdefault(proof_sha256, set()).add(batch_id)

    if not proofs:
        return None
    if len(proofs) != 1:
        raise RuntimeError(
            'pending repair batches have conflicting exact market proofs: '
            f'date={market_date_key}, hashes={sorted(proofs)}'
        )
    proof_sha256, proof = next(iter(proofs.items()))
    observed_sha256, observed_count = (
        input_fingerprint_loader or _default_exact_scope_input_fingerprint
    )(normalized_date, [
        symbol
        for market in SUPPORTED_MARKETS
        for symbol in proof['symbols_by_market'][market]
    ])
    if (
        observed_count != proof['daily_price_input_count']
        or observed_sha256 != proof['daily_price_input_sha256']
    ):
        raise RuntimeError(
            'pending exact market proof no longer matches stored price input: '
            f'date={market_date_key}'
        )
    return {
        **proof,
        'membership_evidence_source': (
            PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE
        ),
        'membership_evidence_sha256': proof_sha256,
        'membership_evidence_repair_batch_ids': sorted(
            proof_batch_ids[proof_sha256]
        ),
        'membership_evidence_run_id': None,
        'membership_evidence_run_source': 'pykrx',
    }


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
    membership_evidence_sha256: str | None = None,
    membership_evidence_repair_batch_ids: Sequence[str] | None = None,
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
    if isinstance(membership_evidence_run_id, bool):
        raise ValueError('membership evidence run id must not be boolean')
    normalized_run_id = (
        int(membership_evidence_run_id)
        if membership_evidence_run_id is not None
        else None
    )
    normalized_evidence_sha256 = (
        str(membership_evidence_sha256).strip().lower()
        if membership_evidence_sha256 is not None
        else None
    )
    normalized_evidence_batch_ids = sorted(
        {
            str(batch_id).strip()
            for batch_id in membership_evidence_repair_batch_ids or ()
            if str(batch_id).strip()
        }
    )
    normalized_provider_membership_sha256 = str(
        provider_membership_sha256
    ).strip().lower()
    if not _is_sha256(normalized_provider_membership_sha256):
        raise ValueError(
            'provider membership evidence requires a canonical SHA-256'
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
        if normalized_evidence_sha256 is not None or normalized_evidence_batch_ids:
            raise ValueError(
                'stored membership evidence must not claim pending proof lineage'
            )
    elif normalized_evidence_source == 'live_pykrx_market_ohlcv':
        if normalized_run_id is not None or normalized_run_source is not None:
            raise ValueError(
                'live membership evidence must not claim stored run lineage'
            )
        if normalized_evidence_sha256 is not None or normalized_evidence_batch_ids:
            raise ValueError(
                'live membership evidence must not claim pending proof lineage'
            )
    elif normalized_evidence_source == (
        PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE
    ):
        if (
            normalized_run_id is not None
            or normalized_run_source != 'pykrx'
            or not _is_sha256(normalized_evidence_sha256)
            or not normalized_evidence_batch_ids
        ):
            raise ValueError(
                'pending repair membership evidence requires a PyKRX source, '
                'canonical proof hash, and repair batch lineage'
            )
    elif normalized_evidence_source is not None:
        raise ValueError(
            'unsupported membership evidence source: '
            f'{normalized_evidence_source}'
        )
    else:
        raise ValueError(
            'factor rebuild evidence requires a membership evidence source'
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
        if normalized_evidence_source == (
            PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE
        ):
            affected_batch_ids = sorted(
                str(batch.repair_batch_id) for batch in affected_batches
            )
            if not set(normalized_evidence_batch_ids).issubset(
                affected_batch_ids
            ):
                raise RuntimeError(
                    'pending membership proof authority is no longer part of '
                    'the affected factor rebuild batch scope'
                )

        evidence_by_symbols: dict[tuple[str, ...], dict[str, Any]] = {}

        def evidence_for(
            batch_symbols: tuple[str, ...],
        ) -> dict[str, Any]:
            if batch_symbols in evidence_by_symbols:
                return evidence_by_symbols[batch_symbols]
            input_sha256, input_row_count = _daily_price_input_fingerprint(
                session,
                market_date=market_date,
                symbols=list(batch_symbols),
            )
            if input_row_count != len(batch_symbols):
                raise RuntimeError(
                    'exact factor rebuild input coverage changed before '
                    'evidence was recorded: '
                    f'rows={input_row_count}, '
                    f'expected={len(batch_symbols)}'
                )
            stage_output_fingerprints: dict[str, dict[str, Any]] = {}
            for stage_id, model in REQUIRED_EXACT_FACTOR_MODELS:
                output_sha256, output_row_count = (
                    _derived_output_fingerprint(
                        session,
                        model=model,
                        market_date=market_date,
                        symbols=batch_symbols,
                    )
                )
                if output_row_count != len(batch_symbols):
                    raise RuntimeError(
                        'exact factor output coverage changed before '
                        'evidence was recorded: '
                        f'stage={stage_id}, rows={output_row_count}, '
                        f'expected={len(batch_symbols)}'
                    )
                stage_output_fingerprints[stage_id] = {
                    'row_count': output_row_count,
                    'output_sha256': output_sha256,
                }
            evidence = {
                'market_date': market_date_key,
                'exact_membership_count': len(batch_symbols),
                'ranking_membership_sha256': (
                    market_membership_sha256(batch_symbols)
                ),
                'provider_membership_sha256': (
                    normalized_provider_membership_sha256
                ),
                'membership_evidence_source': normalized_evidence_source,
                'membership_evidence_run_id': normalized_run_id,
                'membership_evidence_run_source': normalized_run_source,
                'membership_evidence_sha256': normalized_evidence_sha256,
                'membership_evidence_repair_batch_ids': (
                    normalized_evidence_batch_ids
                ),
                'daily_price_input_sha256': input_sha256,
                'stage_output_fingerprints': stage_output_fingerprints,
                'repair_started_at': repair_started_at.isoformat(),
                'rebuild_completed_at': completed_at.isoformat(),
                'stage_ids': [
                    item[0] for item in REQUIRED_EXACT_FACTOR_MODELS
                ],
            }
            evidence_by_symbols[batch_symbols] = evidence
            return evidence

        batch_scopes = [
            (
                batch,
                _factor_evidence_symbols_for_batch(
                    batch=batch,
                    market_date=market_date,
                    rebuilt_symbols=normalized_symbols,
                ),
            )
            for batch in affected_batches
        ]
        recorded_batch_scopes = {
            batch_symbols for _, batch_symbols in batch_scopes
        }
        for batch, batch_symbols in batch_scopes:
            metadata = dict(batch.factor_rebuild_metadata or {})
            date_map = dict(metadata.get('dates') or {})
            existing_date_scope = dict(
                date_map.get(market_date_key) or {}
            )
            date_evidence = dict(evidence_for(batch_symbols))
            if 'trending_output_fingerprints' in existing_date_scope:
                date_evidence['trending_output_fingerprints'] = dict(
                    existing_date_scope['trending_output_fingerprints']
                )
            date_map[market_date_key] = date_evidence
            batch.factor_rebuild_metadata = {
                'schema_version': FACTOR_REBUILD_METADATA_SCHEMA_VERSION,
                'dates': date_map,
            }
        caller_evidence = evidence_for(tuple(normalized_symbols))
        return {
            **caller_evidence,
            'updated_batch_count': len(affected_batches),
            'recorded_membership_scope_count': len(recorded_batch_scopes),
        }


def record_pending_trending_rebuild(
    *,
    market_date: str | date,
    periods: Sequence[str],
    repair_started_at: datetime,
    rebuild_completed_at: datetime | None = None,
    exact_symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Freeze exact trending outputs for every affected pending batch."""

    normalized_date = _normalize_date(market_date)
    market_date_key = normalized_date.isoformat()
    requested_periods = {
        str(period).strip().lower()
        for period in periods
        if str(period).strip()
    }
    if not requested_periods:
        raise ValueError('trending rebuild evidence requires periods')
    unsupported_periods = sorted(
        requested_periods.difference(REQUIRED_TRENDING_PERIODS)
    )
    if unsupported_periods:
        raise ValueError(
            'unsupported trending rebuild evidence periods: '
            f'{unsupported_periods}'
        )
    normalized_periods = tuple(
        period
        for period in REQUIRED_TRENDING_PERIODS
        if period in requested_periods
    )
    normalized_symbols = (
        tuple(
            sorted(
                {
                    str(symbol).strip().upper()
                    for symbol in exact_symbols
                    if str(symbol).strip()
                }
            )
        )
        if exact_symbols is not None
        else None
    )
    if exact_symbols is not None and not normalized_symbols:
        raise ValueError('exact trending rebuild scope must not be empty')
    completed_at = rebuild_completed_at or datetime.now(timezone.utc)
    if repair_started_at.tzinfo is None or completed_at.tzinfo is None:
        raise ValueError(
            'trending rebuild evidence timestamps must include timezone'
        )

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
                'periods': list(normalized_periods),
                'updated_batch_count': 0,
            }

        for batch in affected_batches:
            frozen_symbols = _frozen_signal_feature_symbols_for_batch(
                batch=batch,
                market_date=normalized_date,
            )
            if frozen_symbols is None:
                continue
            if normalized_symbols != frozen_symbols:
                raise RuntimeError(
                    'exact trending rebuild scope does not exactly match '
                    'pending frozen membership: '
                    f'batch={batch.repair_batch_id}, '
                    f'date={market_date_key}, '
                    f'rebuilt_count={len(normalized_symbols or ())}, '
                    f'frozen_count={len(frozen_symbols)}'
                )

        period_evidence: dict[str, dict[str, Any]] = {}
        for period in normalized_periods:
            output_sha256, output_row_count = _derived_output_fingerprint(
                session,
                model=DailyTrendingRank,
                market_date=normalized_date,
                symbols=None,
                periods=(period,),
            )
            if output_row_count <= 0:
                raise RuntimeError(
                    'trending output coverage is empty before evidence was '
                    f'recorded: period={period}'
                )
            period_evidence[period] = {
                'row_count': output_row_count,
                'output_sha256': output_sha256,
                'repair_started_at': repair_started_at.isoformat(),
                'rebuild_completed_at': completed_at.isoformat(),
                'exact_scope': normalized_symbols is not None,
                'scope_symbol_count': len(normalized_symbols or ()),
                'scope_membership_sha256': (
                    market_membership_sha256(normalized_symbols or ())
                ),
            }

        for batch in affected_batches:
            metadata = dict(batch.factor_rebuild_metadata or {})
            date_map = dict(metadata.get('dates') or {})
            date_scope = dict(date_map.get(market_date_key) or {})
            output_fingerprints = dict(
                date_scope.get('trending_output_fingerprints') or {}
            )
            output_fingerprints.update(period_evidence)
            date_scope['trending_output_fingerprints'] = (
                output_fingerprints
            )
            date_map[market_date_key] = date_scope
            batch.factor_rebuild_metadata = {
                'schema_version': FACTOR_REBUILD_METADATA_SCHEMA_VERSION,
                'dates': date_map,
            }
        return {
            'market_date': market_date_key,
            'periods': list(normalized_periods),
            'output_fingerprints': period_evidence,
            'updated_batch_count': len(affected_batches),
        }


def _historical_membership_scopes(
    *,
    market_date_column: Any,
    required_dates: Sequence[date],
    frozen_symbols_by_date: Mapping[date, Sequence[str]] | None,
) -> tuple[tuple[tuple[date, ...], Any], ...]:
    """Return disjoint query scopes without combining every frozen manifest.

    SQLAlchemy expands ``IN`` membership values into one bind parameter per
    symbol at execution time.  Combining a few dozen full KRX manifests in a
    single OR expression can therefore exceed PostgreSQL's 65,535 parameter
    limit.  Each frozen date is deliberately emitted as its own query scope;
    dates without a frozen manifest can still share the current-market query.
    """

    required = tuple(sorted(set(required_dates)))
    required_set = set(required)
    frozen = {
        market_date: tuple(sorted(set(symbols)))
        for market_date, symbols in (frozen_symbols_by_date or {}).items()
        if market_date in required_set
    }
    scopes: list[tuple[tuple[date, ...], Any]] = []
    ordinary_dates = tuple(
        sorted(required_set.difference(frozen))
    )
    if ordinary_dates:
        scopes.append(
            (
                ordinary_dates,
                and_(
                    market_date_column.in_(ordinary_dates),
                    Symbol.market.in_(SUPPORTED_MARKETS),
                ),
            )
        )
    for market_date, symbols in sorted(frozen.items()):
        scopes.append(
            (
                (market_date,),
                (
                    and_(
                        market_date_column == market_date,
                        Symbol.symbol.in_(symbols),
                    )
                    if symbols
                    else false()
                ),
            )
        )
    return tuple(scopes)


def _expected_price_counts(
    session: Any,
    *,
    required_dates: Sequence[date],
    frozen_symbols_by_date: Mapping[date, Sequence[str]] | None = None,
) -> dict[date, int]:
    rows = []
    for _, membership_condition in _historical_membership_scopes(
        market_date_column=DailyPriceBar.trade_date,
        required_dates=required_dates,
        frozen_symbols_by_date=frozen_symbols_by_date,
    ):
        rows.extend(
            session.execute(
                select(
                    DailyPriceBar.trade_date,
                    func.count(DailyPriceBar.id),
                )
                .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                .where(membership_condition)
                .group_by(DailyPriceBar.trade_date)
            ).all()
        )
    return {row[0]: int(row[1]) for row in rows}


def _causal_raw_dependency_watermarks(
    session: Any,
    *,
    required_dates: Sequence[date],
    frozen_symbols_by_date: Mapping[date, Sequence[str]] | None = None,
) -> dict[date, datetime]:
    """Return the latest repaired raw input that can affect each output date.

    A raw row from a later trade date cannot invalidate an earlier derived
    output.  A repaired earlier row can, however, affect later rolling
    technical and factor outputs.  The running maximum therefore preserves
    that forward dependency without applying the newest timestamp backward to
    unrelated historical dates.
    """

    rows = []
    for _, membership_condition in _historical_membership_scopes(
        market_date_column=DailyPriceBar.trade_date,
        required_dates=required_dates,
        frozen_symbols_by_date=frozen_symbols_by_date,
    ):
        rows.extend(
            session.execute(
                select(
                    DailyPriceBar.trade_date,
                    func.max(DailyPriceBar.captured_at),
                )
                .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                .where(membership_condition)
                .group_by(DailyPriceBar.trade_date)
            ).all()
        )
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
    frozen_symbols_by_date: Mapping[date, Sequence[str]] | None = None,
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
    rows = []
    for scope_dates, membership_condition in _historical_membership_scopes(
        market_date_column=model.market_date,
        required_dates=required_dates,
        frozen_symbols_by_date=frozen_symbols_by_date,
    ):
        scoped_watermarks = {
            market_date: raw_dependency_watermarks[market_date]
            for market_date in scope_dates
            if market_date in raw_dependency_watermarks
        }
        fresh_after_dependency_row = case(
            (
                _fresh_after_causal_dependency_condition(
                    market_date_column=model.market_date,
                    updated_at_column=model.updated_at,
                    raw_dependency_watermarks=scoped_watermarks,
                ),
                1,
            )
        )
        rows.extend(
            session.execute(
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
                .where(membership_condition)
                .group_by(model.market_date)
            ).all()
        )
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
    stage_id: str,
    model: Any,
    required_dates: Sequence[date],
    factor_rebuild_metadata: dict[str, Any],
    raw_dependency_watermarks: dict[date, datetime],
    frozen_symbols_by_date: Mapping[date, Sequence[str]] | None = None,
    pending_membership_authority_by_date: (
        Mapping[date, Mapping[str, Any] | None] | None
    ) = None,
) -> dict[str, Any]:
    date_scopes = dict(factor_rebuild_metadata.get('dates') or {})
    try:
        metadata_schema_version = int(
            factor_rebuild_metadata['schema_version']
        )
    except (KeyError, TypeError, ValueError):
        metadata_schema_version = 0
    issues: list[dict[str, Any]] = []
    date_results: list[dict[str, Any]] = []
    for market_date in required_dates:
        date_key = market_date.isoformat()
        scope = dict(date_scopes.get(date_key) or {})
        try:
            if metadata_schema_version != FACTOR_REBUILD_METADATA_SCHEMA_VERSION:
                raise ValueError(
                    'factor rebuild evidence requires current schema'
                )
            expected_count = int(scope['exact_membership_count'])
            expected_membership_sha256 = str(
                scope['ranking_membership_sha256']
            )
            provider_membership_sha256 = str(
                scope['provider_membership_sha256']
            ).strip().lower()
            if not _is_sha256(provider_membership_sha256):
                raise ValueError('provider membership hash is invalid')
            evidence_source = str(
                scope['membership_evidence_source']
            ).strip()
            raw_run_id = scope.get('membership_evidence_run_id')
            if raw_run_id is not None and (
                isinstance(raw_run_id, bool)
                or not isinstance(raw_run_id, int)
                or raw_run_id <= 0
            ):
                raise ValueError('membership evidence run id is invalid')
            evidence_run_source = (
                str(scope.get('membership_evidence_run_source')).strip()
                if scope.get('membership_evidence_run_source') is not None
                else None
            )
            evidence_sha256 = (
                str(scope.get('membership_evidence_sha256')).strip().lower()
                if scope.get('membership_evidence_sha256') is not None
                else None
            )
            raw_evidence_batch_ids = scope.get(
                'membership_evidence_repair_batch_ids'
            )
            evidence_batch_ids = sorted(
                {
                    str(batch_id).strip()
                    for batch_id in raw_evidence_batch_ids or ()
                    if str(batch_id).strip()
                }
            )
            if (
                not isinstance(raw_evidence_batch_ids, list)
                or raw_evidence_batch_ids != evidence_batch_ids
            ):
                raise ValueError(
                    'membership evidence batch lineage is invalid'
                )
            if evidence_source == 'stored_eod_run_metadata':
                lineage_valid = bool(
                    raw_run_id is not None
                    and evidence_run_source
                    and evidence_sha256 is None
                    and not evidence_batch_ids
                )
            elif evidence_source == 'live_pykrx_market_ohlcv':
                lineage_valid = bool(
                    raw_run_id is None
                    and evidence_run_source is None
                    and evidence_sha256 is None
                    and not evidence_batch_ids
                )
            elif evidence_source == (
                PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE
            ):
                lineage_valid = bool(
                    raw_run_id is None
                    and evidence_run_source == 'pykrx'
                    and _is_sha256(evidence_sha256)
                    and evidence_batch_ids
                )
            else:
                lineage_valid = False
            if not lineage_valid:
                raise ValueError('membership evidence lineage is invalid')
            expected_input_sha256 = str(
                scope['daily_price_input_sha256']
            )
            output_scope = dict(
                dict(scope['stage_output_fingerprints'])[stage_id]
            )
            expected_output_count = int(output_scope['row_count'])
            expected_output_sha256 = str(output_scope['output_sha256'])
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

        current_pending_authority_matches = True
        if evidence_source == PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE:
            current_authority = dict(
                (pending_membership_authority_by_date or {}).get(market_date)
                or {}
            )
            current_pending_authority_matches = bool(
                current_authority.get('membership_evidence_source')
                == PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE
                and current_authority.get('membership_evidence_run_id') is None
                and current_authority.get('membership_evidence_run_source')
                == evidence_run_source
                and current_authority.get('membership_evidence_sha256')
                == evidence_sha256
                and current_authority.get(
                    'membership_evidence_repair_batch_ids'
                )
                == evidence_batch_ids
                and current_authority.get('ranking_membership_count')
                == expected_count
                and current_authority.get('ranking_membership_sha256')
                == expected_membership_sha256
                and current_authority.get('provider_membership_sha256')
                == provider_membership_sha256
                and current_authority.get('daily_price_input_sha256')
                == expected_input_sha256
            )

        has_frozen_membership = bool(
            frozen_symbols_by_date is not None
            and market_date in frozen_symbols_by_date
        )
        frozen_symbols = tuple(
            (frozen_symbols_by_date or {}).get(market_date) or ()
        )
        membership_condition = (
            Symbol.symbol.in_(frozen_symbols)
            if has_frozen_membership
            else Symbol.market.in_(SUPPORTED_MARKETS)
        )
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
                membership_condition,
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
        observed_output_sha256, output_row_count = (
            _derived_output_fingerprint(
                session,
                model=model,
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
            'provider_membership_sha256': provider_membership_sha256,
            'membership_evidence_source': evidence_source,
            'membership_evidence_run_id': raw_run_id,
            'membership_evidence_run_source': evidence_run_source,
            'membership_evidence_sha256': evidence_sha256,
            'membership_evidence_repair_batch_ids': evidence_batch_ids,
            'current_pending_authority_matches': (
                current_pending_authority_matches
            ),
            'expected_daily_price_input_sha256': expected_input_sha256,
            'observed_daily_price_input_sha256': observed_input_sha256,
            'expected_output_row_count': expected_output_count,
            'observed_output_row_count': output_row_count,
            'expected_output_sha256': expected_output_sha256,
            'observed_output_sha256': observed_output_sha256,
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
            and current_pending_authority_matches
            and observed_input_sha256 == expected_input_sha256
            and output_row_count == expected_output_count == expected_count
            and observed_output_sha256 == expected_output_sha256
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
    frozen_symbols_by_date: Mapping[date, Sequence[str]] | None = None,
    factor_rebuild_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rebuild_metadata = dict(factor_rebuild_metadata or {})
    try:
        metadata_schema_version = int(rebuild_metadata['schema_version'])
    except (KeyError, TypeError, ValueError):
        metadata_schema_version = 0
    date_scopes = dict(rebuild_metadata.get('dates') or {})
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
    top_absolute_rank = case(
        (
            DailyTrendingRank.absolute_rank <= 250,
            DailyTrendingRank.absolute_rank,
        )
    )
    rows = []
    for scope_dates, membership_condition in _historical_membership_scopes(
        market_date_column=DailyTrendingRank.market_date,
        required_dates=required_dates,
        frozen_symbols_by_date=frozen_symbols_by_date,
    ):
        scoped_watermarks = {
            market_date: raw_dependency_watermarks[market_date]
            for market_date in scope_dates
            if market_date in raw_dependency_watermarks
        }
        fresh_after_dependency_row = case(
            (
                _fresh_after_causal_dependency_condition(
                    market_date_column=DailyTrendingRank.market_date,
                    updated_at_column=DailyTrendingRank.updated_at,
                    raw_dependency_watermarks=scoped_watermarks,
                ),
                1,
            )
        )
        rows.extend(
            session.execute(
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
                        DailyPriceBar.symbol_id
                        == DailyTrendingRank.symbol_id,
                        DailyPriceBar.trade_date
                        == DailyTrendingRank.market_date,
                    ),
                )
                .where(
                    membership_condition,
                    DailyTrendingRank.period.in_(
                        REQUIRED_TRENDING_PERIODS
                    ),
                )
                .group_by(
                    DailyTrendingRank.market_date,
                    DailyTrendingRank.period,
                )
            ).all()
        )
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
    output_fingerprints_verified = 0
    for market_date in required_dates:
        for period in REQUIRED_TRENDING_PERIODS:
            counts = dict(
                observed.get(
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
            )
            output_fingerprint_valid = False
            expected_output_count: int | None = None
            expected_output_sha256: str | None = None
            observed_output_count: int | None = None
            observed_output_sha256: str | None = None
            try:
                if metadata_schema_version not in (
                    SUPPORTED_FACTOR_REBUILD_METADATA_SCHEMA_VERSIONS
                ):
                    raise ValueError('unsupported rebuild evidence schema')
                output_scope = dict(
                    dict(
                        dict(date_scopes[market_date.isoformat()])[
                            'trending_output_fingerprints'
                        ]
                    )[period]
                )
                expected_output_count = int(output_scope['row_count'])
                expected_output_sha256 = str(
                    output_scope['output_sha256']
                )
                if expected_output_count <= 0 or not expected_output_sha256:
                    raise ValueError('invalid trending output fingerprint')
                (
                    observed_output_sha256,
                    observed_output_count,
                ) = _derived_output_fingerprint(
                    session,
                    model=DailyTrendingRank,
                    market_date=market_date,
                    symbols=None,
                    periods=(period,),
                )
                output_fingerprint_valid = bool(
                    observed_output_count == expected_output_count
                    and observed_output_sha256 == expected_output_sha256
                )
            except (KeyError, TypeError, ValueError):
                output_fingerprint_valid = False
            if output_fingerprint_valid:
                output_fingerprints_verified += 1
            counts.update(
                {
                    'output_fingerprint_valid': output_fingerprint_valid,
                    'expected_output_row_count': expected_output_count,
                    'observed_output_row_count': observed_output_count,
                    'expected_output_sha256': expected_output_sha256,
                    'observed_output_sha256': observed_output_sha256,
                }
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
                or not output_fingerprint_valid
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
        'output_fingerprints_verified': output_fingerprints_verified,
        'issues': issues[:20],
    }


def _signal_context_rebuild_evidence(
    session: Any,
    *,
    batch: DailyPriceBarRepairBatch,
    required_dates: Sequence[date],
) -> dict[str, Any]:
    metadata = dict(batch.evaluation_rebuild_metadata or {})
    context_metadata = dict(
        metadata.get('deterministic_signal_contexts') or {}
    )
    date_scopes = dict(context_metadata.get('dates') or {})
    try:
        context_schema_version = int(context_metadata['schema_version'])
    except (KeyError, TypeError, ValueError):
        context_schema_version = 0
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
        frozen_symbols = _frozen_signal_feature_symbols_for_batch(
            batch=batch,
            market_date=market_date,
        )
        frozen_membership_sha256 = (
            market_membership_sha256(frozen_symbols)
            if frozen_symbols is not None
            else None
        )
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

        frozen_scope = {}
        expected_scoped_signal_count: int | None = None
        expected_scoped_signal_sha256: str | None = None
        frozen_scope_metadata_valid = frozen_symbols is None
        if frozen_symbols is not None:
            frozen_scope = dict(
                dict(scope.get('frozen_membership_scopes') or {}).get(
                    frozen_membership_sha256,
                )
                or {}
            )
            try:
                recorded_membership_count = int(
                    frozen_scope['membership_count']
                )
                recorded_membership_sha256 = str(
                    frozen_scope['membership_sha256']
                )
                expected_scoped_signal_count = int(
                    frozen_scope['signal_feature_count']
                )
                expected_scoped_signal_sha256 = str(
                    frozen_scope['signal_feature_sha256']
                )
            except (KeyError, TypeError, ValueError):
                recorded_membership_count = 0
                recorded_membership_sha256 = ''
            frozen_scope_metadata_valid = bool(
                context_schema_version == 2
                and recorded_membership_count == len(frozen_symbols)
                and recorded_membership_sha256
                == frozen_membership_sha256
                and expected_scoped_signal_count == len(frozen_symbols)
                and expected_scoped_signal_count > 0
                and expected_scoped_signal_sha256
            )

        (
            observed_signal_sha256,
            observed_signal_count,
            minimum_signal_updated_at,
            maximum_signal_updated_at,
        ) = _signal_feature_fingerprint(session, market_date=market_date)
        if frozen_symbols is not None:
            (
                observed_scoped_signal_sha256,
                observed_scoped_signal_count,
                minimum_scoped_signal_updated_at,
                maximum_scoped_signal_updated_at,
            ) = _signal_feature_fingerprint(
                session,
                market_date=market_date,
                symbols=frozen_symbols,
            )
        else:
            observed_scoped_signal_sha256 = observed_signal_sha256
            observed_scoped_signal_count = observed_signal_count
            minimum_scoped_signal_updated_at = minimum_signal_updated_at
            maximum_scoped_signal_updated_at = maximum_signal_updated_at
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
            observed_scoped_signal_count > 0
            and minimum_scoped_signal_updated_at is not None
            and minimum_scoped_signal_updated_at >= rebuild_started_at
            and dependency_watermark is not None
            and minimum_scoped_signal_updated_at >= dependency_watermark
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
            and frozen_scope_metadata_valid
            and (
                frozen_symbols is None
                or (
                    observed_scoped_signal_count
                    == expected_scoped_signal_count
                    and observed_scoped_signal_sha256
                    == expected_scoped_signal_sha256
                )
            )
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
            'frozen_scope_required': frozen_symbols is not None,
            'frozen_scope_metadata_valid': frozen_scope_metadata_valid,
            'expected_frozen_membership_count': (
                len(frozen_symbols)
                if frozen_symbols is not None
                else None
            ),
            'expected_frozen_membership_sha256': (
                frozen_membership_sha256
            ),
            'expected_scoped_signal_feature_count': (
                expected_scoped_signal_count
            ),
            'observed_scoped_signal_feature_count': (
                observed_scoped_signal_count
            ),
            'expected_scoped_signal_feature_sha256': (
                expected_scoped_signal_sha256
            ),
            'observed_scoped_signal_feature_sha256': (
                observed_scoped_signal_sha256
            ),
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


def _missing_bar_insert_repair_evidence(
    session: Any,
    *,
    batch: DailyPriceBarRepairBatch,
) -> dict[str, Any]:
    """Verify the durable raw-input evidence for a missing-row insertion.

    ``daily_price_bar_repair_audits`` is intentionally limited by schema to
    KIS before/after corrections.  Missing PyKRX rows have no truthful
    before-image, so their exact insert scope is retained on the existing
    repair batch instead of fabricating a KIS audit record.
    """

    metadata = dict(batch.verification_metadata or {})
    recorded = dict(metadata.get('repair_evidence') or {})
    issues: list[str] = []
    try:
        market_date = _normalize_date(recorded['market_date'])
        inserted_symbols = sorted(
            {
                str(symbol).strip().upper()
                for symbol in recorded['inserted_symbols']
                if str(symbol).strip()
            }
        )
        inserted_symbol_count = int(recorded['inserted_symbol_count'])
        expected_scope_sha256 = str(
            recorded['inserted_price_input_sha256']
        ).strip().lower()
        expected_provider_count = int(
            recorded['expected_provider_membership_count']
        )
        expected_provider_sha256 = str(
            recorded['expected_provider_membership_sha256']
        ).strip().lower()
        expected_provider_payload_sha256 = str(
            recorded['expected_provider_payload_sha256']
        ).strip().lower()
        provider_price_input_sha256 = str(
            recorded['provider_price_input_sha256']
        ).strip().lower()
        provider_manifest = dict(recorded['provider_membership_manifest'])
        raw_markets = dict(provider_manifest['markets'])
        provider_symbols = sorted(
            str(symbol).strip().upper()
            for market_entry in raw_markets.values()
            for symbol in dict(market_entry).get('symbols') or []
            if str(symbol).strip()
        )
        price_dependent_symbols = sorted(
            {
                str(symbol).strip().upper()
                for symbol in recorded['price_dependent_membership_symbols']
                if str(symbol).strip()
            }
        )
        price_dependent_count = int(
            recorded['price_dependent_membership_count']
        )
        price_dependent_sha256 = str(
            recorded['price_dependent_membership_sha256']
        ).strip().lower()
        reviewed_missing_count = int(
            recorded['reviewed_missing_symbol_count']
        )
        reviewed_missing_sha256 = str(
            recorded['reviewed_missing_membership_sha256']
        ).strip().lower()
        raw_membership_by_date = dict(
            recorded['price_dependent_membership_by_date']
        )
        fallback_manifest_count = int(
            recorded.get('downstream_fallback_manifest_count', 0)
        )
        fallback_bundle_sha256 = str(
            recorded.get('downstream_fallback_bundle_sha256')
            or _stable_json_sha256(
                {
                    'schema_version': (
                        'missing_bar_downstream_membership_v1'
                    ),
                    'manifests': {},
                }
            )
        ).strip().lower()
        membership_by_date: dict[date, tuple[str, ...]] = {}
        membership_evidence_by_date: dict[date, dict[str, Any]] = {}
        for raw_market_date, raw_scope in raw_membership_by_date.items():
            scope_market_date = _normalize_date(raw_market_date)
            if scope_market_date in membership_by_date:
                raise ValueError('duplicate frozen date membership scope')
            scope = dict(raw_scope)
            raw_scope_symbols = [
                str(symbol).strip().upper()
                for symbol in scope['symbols']
                if str(symbol).strip()
            ]
            scope_symbols = tuple(sorted(set(raw_scope_symbols)))
            scope_count = int(scope['symbol_count'])
            scope_membership_sha256 = str(
                scope['membership_sha256']
            ).strip().lower()
            scope_input_sha256 = str(
                scope['daily_price_input_sha256']
            ).strip().lower()
            if (
                len(raw_scope_symbols) != len(scope_symbols)
                or scope_count != len(scope_symbols)
                or scope_count <= 0
                or scope_membership_sha256
                != market_membership_sha256(scope_symbols)
                or len(scope_input_sha256) != 64
                or any(
                    character not in '0123456789abcdef'
                    for character in scope_input_sha256
                )
            ):
                raise ValueError('invalid frozen date membership scope')
            membership_by_date[scope_market_date] = scope_symbols
            membership_evidence_by_date[scope_market_date] = scope
    except (KeyError, TypeError, ValueError):
        return {
            'ready': False,
            'repair_kind': 'missing_bar_insert',
            'issues': ['missing_bar_insert_evidence_missing_or_invalid'],
            'recorded': recorded,
        }

    affected_symbols = sorted(
        {
            str(symbol).strip().upper()
            for symbol in batch.affected_symbols or []
            if str(symbol).strip()
        }
    )
    affected_dates = sorted(
        {
            _normalize_date(value)
            for value in batch.affected_trade_dates or []
        }
    )
    if str(recorded.get('schema_version') or '') != 'missing_bar_insert_v3':
        issues.append('missing_bar_insert_schema_version_invalid')
    if str(recorded.get('provider') or '') != 'pykrx':
        issues.append('missing_bar_insert_provider_invalid')
    if inserted_symbol_count <= 0 or inserted_symbol_count != len(
        inserted_symbols
    ):
        issues.append('missing_bar_insert_symbol_count_invalid')
    if inserted_symbol_count != int(batch.repaired_row_count):
        issues.append('missing_bar_insert_repaired_row_count_mismatch')
    if inserted_symbols != affected_symbols:
        issues.append('missing_bar_insert_affected_symbols_mismatch')
    if affected_dates != [market_date]:
        issues.append('missing_bar_insert_affected_trade_dates_mismatch')
    if expected_provider_count < inserted_symbol_count:
        issues.append('missing_bar_insert_provider_membership_count_invalid')
    if len(expected_provider_sha256) != 64 or any(
        character not in '0123456789abcdef'
        for character in expected_provider_sha256
    ):
        issues.append('missing_bar_insert_provider_membership_sha256_invalid')
    if len(expected_provider_payload_sha256) != 64 or any(
        character not in '0123456789abcdef'
        for character in expected_provider_payload_sha256
    ):
        issues.append('missing_bar_insert_provider_payload_sha256_invalid')
    manifest_symbols_by_market = validate_market_membership_snapshot(
        provider_manifest,
        requested_symbols=provider_symbols,
        markets=('KOSPI', 'KOSDAQ', 'KONEX'),
    )
    if (
        manifest_symbols_by_market is None
        or expected_provider_count != len(provider_symbols)
        or expected_provider_sha256
        != market_membership_sha256(provider_symbols)
        or provider_manifest.get('membership_sha256')
        != expected_provider_sha256
    ):
        issues.append('missing_bar_insert_provider_manifest_invalid')
    expected_price_dependent_symbols = sorted(
        symbol
        for market in ('KOSPI', 'KOSDAQ')
        for symbol in (
            (manifest_symbols_by_market or {}).get(market) or []
        )
    )
    if (
        price_dependent_symbols != expected_price_dependent_symbols
        or price_dependent_count != len(price_dependent_symbols)
        or price_dependent_sha256
        != market_membership_sha256(price_dependent_symbols)
    ):
        issues.append(
            'missing_bar_insert_price_dependent_manifest_invalid'
        )
    required_dates = _normalize_required_dates(
        batch.required_rebuild_trade_dates
    )
    if set(membership_by_date) != set(required_dates):
        issues.append('missing_bar_insert_frozen_membership_dates_mismatch')
    if tuple(price_dependent_symbols) != membership_by_date.get(
        market_date,
        (),
    ):
        issues.append('missing_bar_insert_target_frozen_membership_mismatch')
    for required_date in required_dates:
        scope = membership_evidence_by_date.get(required_date, {})
        basis = str(scope.get('basis') or '')
        if required_date == market_date:
            if basis != 'reviewed_live_provider_membership':
                issues.append(
                    'missing_bar_insert_target_membership_basis_invalid'
                )
        else:
            run_id = scope.get('eod_run_id')
            stored_proof_valid = (
                basis == 'validated_eod_run_market_membership'
                and not isinstance(run_id, bool)
                and isinstance(run_id, int)
                and run_id > 0
                and str(scope.get('eod_run_source') or '') == 'pykrx'
            )
            if stored_proof_valid:
                stored_run = session.get(EODCollectionRun, run_id)
                if stored_run is None:
                    stored_proof_valid = False
                    issues.append(
                        'missing_bar_insert_stored_membership_run_missing'
                    )
                else:
                    # Import locally because missing_bar_repair owns the
                    # canonical stored-run validator and imports this module's
                    # price fingerprint helper.  Reusing it here keeps the
                    # preparation and completion gates identical without a
                    # module-import cycle.
                    from app.eod.missing_bar_repair import (
                        _strict_stored_price_dependent_membership,
                    )

                    try:
                        strict_symbols = tuple(
                            _strict_stored_price_dependent_membership(
                                stored_run,
                                market_date=required_date,
                            )
                        )
                    except (KeyError, TypeError, ValueError):
                        stored_proof_valid = False
                        issues.append(
                            'missing_bar_insert_stored_membership_lineage_invalid'
                        )
                    else:
                        if strict_symbols != membership_by_date.get(
                            required_date,
                            (),
                        ):
                            stored_proof_valid = False
                            issues.append(
                                'missing_bar_insert_stored_membership_scope_mismatch'
                            )
            live_membership = scope.get('market_membership')
            live_membership_valid = (
                basis == 'reviewed_live_exact_date_pykrx_membership'
                and run_id is None
                and str(scope.get('eod_run_source') or '') == 'pykrx'
                and str(scope.get('source') or '') == 'pykrx'
                and str(scope.get('universe_source') or '')
                == 'pykrx_market_ohlcv'
                and str(scope.get('market_date') or '')
                == required_date.isoformat()
                and scope.get('markets') == list(SUPPORTED_MARKETS)
                and scope.get('daily_price_input_count')
                == len(membership_by_date.get(required_date, ()))
                and validate_market_membership_snapshot(
                    live_membership,
                    requested_symbols=list(
                        membership_by_date.get(required_date, ())
                    ),
                    markets=SUPPORTED_MARKETS,
                )
                is not None
                and all(
                    len(
                        dict(live_membership or {})
                        .get('markets', {})
                        .get(market, {})
                        .get('symbols', [])
                    )
                    >= MINIMUM_MARKET_MEMBERSHIP[market]
                    for market in SUPPORTED_MARKETS
                )
            )
            if not stored_proof_valid and not live_membership_valid:
                issues.append(
                    'missing_bar_insert_downstream_membership_basis_invalid'
                )
    live_fallback_manifests = {
        scope_date.isoformat(): {
            key: value
            for key, value in membership_evidence_by_date[scope_date].items()
            if key not in {'eod_run_id', 'eod_run_source'}
        }
        for scope_date in sorted(membership_evidence_by_date)
        if membership_evidence_by_date[scope_date].get('basis')
        == 'reviewed_live_exact_date_pykrx_membership'
    }
    observed_fallback_bundle_sha256 = _stable_json_sha256(
        {
            'schema_version': 'missing_bar_downstream_membership_v1',
            'manifests': live_fallback_manifests,
        }
    )
    if (
        fallback_manifest_count != len(live_fallback_manifests)
        or fallback_bundle_sha256 != observed_fallback_bundle_sha256
    ):
        issues.append('missing_bar_insert_downstream_fallback_bundle_invalid')
    if (
        reviewed_missing_count != inserted_symbol_count
        or reviewed_missing_sha256
        != market_membership_sha256(inserted_symbols)
    ):
        issues.append('missing_bar_insert_reviewed_missing_scope_invalid')

    observed_scope_sha256, observed_scope_count = (
        exact_daily_price_bar_scope_fingerprint(
            session,
            market_date=market_date,
            symbols=inserted_symbols,
        )
    )
    if observed_scope_count != inserted_symbol_count:
        issues.append('missing_bar_insert_scope_count_mismatch')
    if observed_scope_sha256 != expected_scope_sha256:
        issues.append('missing_bar_insert_scope_sha256_mismatch')
    observed_provider_sha256, observed_provider_count = (
        exact_daily_price_bar_scope_fingerprint(
            session,
            market_date=market_date,
            symbols=provider_symbols,
        )
    )
    if observed_provider_count != expected_provider_count:
        issues.append('missing_bar_insert_provider_scope_count_mismatch')
    if observed_provider_sha256 != provider_price_input_sha256:
        issues.append('missing_bar_insert_provider_scope_sha256_mismatch')
    observed_membership_counts: dict[str, int] = {}
    for scope_date, scope_symbols in membership_by_date.items():
        observed_membership_sha256, scope_count = (
            exact_daily_price_bar_scope_fingerprint(
                session,
                market_date=scope_date,
                symbols=scope_symbols,
            )
        )
        expected_membership_input_sha256 = str(
            membership_evidence_by_date[scope_date][
                'daily_price_input_sha256'
            ]
        ).strip().lower()
        observed_membership_counts[scope_date.isoformat()] = scope_count
        if scope_count != len(scope_symbols):
            issues.append(
                'missing_bar_insert_frozen_membership_price_scope_missing'
            )
        if observed_membership_sha256 != expected_membership_input_sha256:
            issues.append(
                'missing_bar_insert_frozen_membership_input_sha256_mismatch'
            )
    return {
        'ready': not issues,
        'repair_kind': 'missing_bar_insert',
        'market_date': market_date.isoformat(),
        'inserted_symbol_count': inserted_symbol_count,
        'observed_scope_count': observed_scope_count,
        'expected_scope_sha256': expected_scope_sha256,
        'observed_scope_sha256': observed_scope_sha256,
        'provider_membership_count': expected_provider_count,
        'observed_provider_scope_count': observed_provider_count,
        'expected_provider_scope_sha256': provider_price_input_sha256,
        'observed_provider_scope_sha256': observed_provider_sha256,
        'expected_provider_payload_sha256': (
            expected_provider_payload_sha256
        ),
        'price_dependent_membership_symbols': price_dependent_symbols,
        'price_dependent_membership_count': price_dependent_count,
        'price_dependent_membership_sha256': price_dependent_sha256,
        'price_dependent_membership_by_date': {
            scope_date.isoformat(): list(scope_symbols)
            for scope_date, scope_symbols in membership_by_date.items()
        },
        'observed_membership_counts_by_date': observed_membership_counts,
        'issues': issues,
        'recorded': recorded,
    }


def _collect_batch_verification(
    session: Any,
    *,
    batch: DailyPriceBarRepairBatch,
    evidence_cache: dict[tuple[Any, ...], Any] | None = None,
    newest_created_at_by_dates: dict[tuple[date, ...], datetime] | None = None,
    pending_membership_authority_by_date: (
        Mapping[date, Mapping[str, Any] | None] | None
    ) = None,
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
    repair_metadata = dict(
        getattr(batch, 'verification_metadata', None) or {}
    )
    repair_evidence_payload = dict(
        repair_metadata.get('repair_evidence') or {}
    )
    if (
        repair_evidence_payload.get('repair_kind')
        == 'missing_bar_insert'
    ):
        repair_evidence = _missing_bar_insert_repair_evidence(
            session,
            batch=batch,
        )
        issues.extend(repair_evidence['issues'])
    else:
        repair_evidence = {
            'ready': audit_count == int(batch.repaired_row_count),
            'repair_kind': 'kis_final_bar_update',
            'issues': (
                []
                if audit_count == int(batch.repaired_row_count)
                else ['repair_audit_count_mismatch']
            ),
        }
        if not repair_evidence['ready']:
            issues.append('repair_audit_count_mismatch')

    frozen_symbols_by_date: dict[date, tuple[str, ...]] = {}
    if repair_evidence_payload.get('repair_kind') == 'missing_bar_insert':
        raw_frozen_by_date = dict(
            repair_evidence.get('price_dependent_membership_by_date') or {}
        )
        for required_date in required_dates:
            frozen_symbols_by_date[required_date] = tuple(
                raw_frozen_by_date.get(required_date.isoformat()) or ()
            )
        if any(
            not frozen_symbols_by_date[required_date]
            for required_date in required_dates
        ):
            issues.append('missing_bar_insert_frozen_membership_invalid')
    frozen_scope_key = tuple(
        (
            market_date,
            len(symbols),
            market_membership_sha256(symbols),
        )
        for market_date, symbols in sorted(frozen_symbols_by_date.items())
    )

    expected_counts = _cached_verification_evidence(
        cache,
        ('expected_price_counts', required_dates, frozen_scope_key),
        lambda: _expected_price_counts(
            session,
            required_dates=required_dates,
            frozen_symbols_by_date=frozen_symbols_by_date,
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
        ('raw_dependency_watermarks', required_dates, frozen_scope_key),
        lambda: _causal_raw_dependency_watermarks(
            session,
            required_dates=required_dates,
            frozen_symbols_by_date=frozen_symbols_by_date,
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
            frozen_scope_key,
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
                frozen_symbols_by_date=frozen_symbols_by_date,
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
                    frozen_scope_key,
                    batch.created_at,
                ),
                lambda: _full_coverage_stage_evidence(
                    session,
                    model=model,
                    required_dates=required_dates,
                    expected_counts=expected_counts,
                    rebuilt_after=batch.created_at,
                    raw_dependency_watermarks=raw_dependency_watermarks,
                    frozen_symbols_by_date=frozen_symbols_by_date,
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
    pending_authority_key = json.dumps(
        {
            market_date.isoformat(): dict(authority or {})
            for market_date, authority in sorted(
                (pending_membership_authority_by_date or {}).items()
            )
        },
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
                pending_authority_key,
                watermark_key,
                frozen_scope_key,
            ),
            lambda: _exact_factor_stage_evidence(
                session,
                stage_id=stage_id,
                model=model,
                required_dates=required_dates,
                factor_rebuild_metadata=factor_rebuild_metadata,
                raw_dependency_watermarks=raw_dependency_watermarks,
                frozen_symbols_by_date=frozen_symbols_by_date,
                pending_membership_authority_by_date=(
                    pending_membership_authority_by_date
                ),
            ),
        )
        stages[stage_id] = evidence
        if not evidence['ready']:
            issues.append(f'{stage_id}_incomplete_or_stale')

    trending = _cached_verification_evidence(
        cache,
        (
            'trending',
            required_dates,
            factor_metadata_key,
            watermark_key,
            frozen_scope_key,
            newest_created_at,
        ),
        lambda: _trending_evidence(
            session,
            required_dates=required_dates,
            rebuilt_after=newest_created_at,
            raw_dependency_watermarks=raw_dependency_watermarks,
            frozen_symbols_by_date=frozen_symbols_by_date,
            factor_rebuild_metadata=factor_rebuild_metadata,
        ),
    )
    # Trending freshness has the same monotonic threshold property above.
    if not trending['ready'] and batch.created_at != newest_created_at:
        trending = _cached_verification_evidence(
            cache,
            (
                'trending',
                required_dates,
                factor_metadata_key,
                watermark_key,
                frozen_scope_key,
                batch.created_at,
            ),
            lambda: _trending_evidence(
                session,
                required_dates=required_dates,
                rebuilt_after=batch.created_at,
                raw_dependency_watermarks=raw_dependency_watermarks,
                frozen_symbols_by_date=frozen_symbols_by_date,
                factor_rebuild_metadata=factor_rebuild_metadata,
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

    result = {
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
    if repair_evidence_payload.get('repair_kind') == 'missing_bar_insert':
        result['repair_evidence'] = repair_evidence
    return result


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
    (
        frozen_signal_union_by_date,
        frozen_signal_scopes_by_date,
    ) = _pending_frozen_signal_feature_scopes(
        pending=pending,
        required_dates=required_rebuild_dates,
    )
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
        exact_symbols = frozen_signal_union_by_date.get(rebuild_date)
        signal_rebuild_kwargs: dict[str, Any] = {
            'market_date': rebuild_date,
        }
        if exact_symbols is not None:
            signal_rebuild_kwargs['exact_symbols'] = exact_symbols
        signal_result = (
            daily_signal_feature_service.
            rebuild_stored_owned_fallback_for_market_date(
                **signal_rebuild_kwargs
            )
        )
        participation_result = (
            daily_market_participation_context_service.rebuild_for_market_date(
                market_date=rebuild_date,
                market='KRX',
                source=SIGNAL_FEATURE_SOURCE,
                exact_symbols=exact_symbols,
            )
        )
        frozen_scope_evidence: dict[str, dict[str, Any]] = {}
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
            for (
                membership_sha256,
                scope_symbols,
            ) in sorted(
                (
                    frozen_signal_scopes_by_date.get(rebuild_date)
                    or {}
                ).items()
            ):
                (
                    scope_signal_sha256,
                    scope_signal_count,
                    scope_minimum_updated_at,
                    scope_maximum_updated_at,
                ) = _signal_feature_fingerprint(
                    session,
                    market_date=rebuild_date,
                    symbols=scope_symbols,
                )
                if scope_signal_count != len(scope_symbols):
                    raise RuntimeError(
                        'deterministic signal-context rebuild did not cover '
                        'frozen membership for '
                        f'{rebuild_date.isoformat()}: expected='
                        f'{len(scope_symbols)}, observed={scope_signal_count}, '
                        f'membership_sha256={membership_sha256}'
                    )
                frozen_scope_evidence[membership_sha256] = {
                    'membership_count': len(scope_symbols),
                    'membership_sha256': membership_sha256,
                    'signal_feature_count': scope_signal_count,
                    'signal_feature_sha256': scope_signal_sha256,
                    'minimum_signal_feature_updated_at': (
                        scope_minimum_updated_at.isoformat()
                        if scope_minimum_updated_at is not None
                        else None
                    ),
                    'maximum_signal_feature_updated_at': (
                        scope_maximum_updated_at.isoformat()
                        if scope_maximum_updated_at is not None
                        else None
                    ),
                }
        persisted_signal_count = int(
            signal_result.get('features_persisted') or 0
        )
        captured_symbol_count = int(
            participation_result.get('captured_symbol_count') or 0
        )
        expected_persisted_count = (
            len(exact_symbols)
            if exact_symbols is not None
            else signal_count
        )
        expected_participation_count = expected_persisted_count
        if exact_symbols is not None:
            returned_scope_count = int(
                signal_result.get('scope_symbol_count') or 0
            )
            returned_scope_sha256 = str(
                signal_result.get('scope_membership_sha256') or ''
            )
            expected_scope_sha256 = market_membership_sha256(
                exact_symbols
            )
            if (
                signal_result.get('exact_scope') is not True
                or returned_scope_count != len(exact_symbols)
                or returned_scope_sha256 != expected_scope_sha256
            ):
                raise RuntimeError(
                    'deterministic signal-context rebuild scope mismatch for '
                    f'{rebuild_date.isoformat()}: expected_count='
                    f'{len(exact_symbols)}, returned_count='
                    f'{returned_scope_count}, expected_sha256='
                    f'{expected_scope_sha256}, returned_sha256='
                    f'{returned_scope_sha256}'
                )
            participation_scope_count = int(
                participation_result.get('scope_symbol_count') or 0
            )
            participation_scope_sha256 = str(
                participation_result.get('scope_membership_sha256') or ''
            )
            if (
                participation_result.get('exact_scope') is not True
                or participation_scope_count != len(exact_symbols)
                or participation_scope_sha256 != expected_scope_sha256
            ):
                raise RuntimeError(
                    'deterministic participation-context scope mismatch for '
                    f'{rebuild_date.isoformat()}: expected_count='
                    f'{len(exact_symbols)}, returned_count='
                    f'{participation_scope_count}, expected_sha256='
                    f'{expected_scope_sha256}, returned_sha256='
                    f'{participation_scope_sha256}'
                )
        if not (
            persisted_signal_count == expected_persisted_count
            and captured_symbol_count == expected_participation_count
            and participation_count == 1
            and participation_updated_at is not None
        ):
            raise RuntimeError(
                'deterministic signal-context rebuild count mismatch for '
                f'{rebuild_date.isoformat()}: persisted={persisted_signal_count}, '
                f'expected_persisted={expected_persisted_count}, '
                f'observed={signal_count}, expected_participation='
                f'{expected_participation_count}, participation_symbols='
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
            'exact_scope_union_count': (
                len(exact_symbols)
                if exact_symbols is not None
                else None
            ),
            'exact_scope_union_sha256': (
                market_membership_sha256(exact_symbols)
                if exact_symbols is not None
                else None
            ),
            'frozen_membership_scopes': frozen_scope_evidence,
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
            'schema_version': 2,
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
    pending_batch_ids = tuple(
        sorted(
            {
                str(batch.get('repair_batch_id') or '').strip()
                for batch in pending
                if str(batch.get('repair_batch_id') or '').strip()
            }
        )
    )
    if len(pending_batch_ids) != len(pending):
        return {
            'status': 'blocked',
            'applied': False,
            'reason': 'pending_repair_batch_identity_invalid',
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
        return _verify_pending_price_bar_rebuilds_locked(
            apply=apply,
            expected_batch_ids=pending_batch_ids,
        )


def _verify_pending_price_bar_rebuilds_locked(
    *,
    apply: bool,
    expected_batch_ids: Sequence[str],
) -> dict[str, Any]:
    normalized_batch_ids = tuple(
        sorted({str(batch_id).strip() for batch_id in expected_batch_ids})
    )
    if (
        len(normalized_batch_ids) != len(expected_batch_ids)
        or any(not batch_id for batch_id in normalized_batch_ids)
    ):
        raise ValueError('expected pending repair batch ids must be unique')
    completed_at = datetime.now(timezone.utc)
    with session_scope(
        statement_timeout_ms=(
            settings.price_bar_repair_db_statement_timeout_ms
        ),
    ) as session:
        statement = (
            select(DailyPriceBarRepairBatch)
            .where(
                DailyPriceBarRepairBatch.status == 'pending',
                DailyPriceBarRepairBatch.repair_batch_id.in_(
                    normalized_batch_ids
                ),
            )
            .order_by(
                DailyPriceBarRepairBatch.created_at.asc(),
                DailyPriceBarRepairBatch.repair_batch_id.asc(),
            )
        )
        if apply:
            statement = statement.with_for_update()
        batches = session.scalars(statement).all()
        loaded_batch_ids = tuple(
            sorted(str(batch.repair_batch_id) for batch in batches)
        )
        if loaded_batch_ids != normalized_batch_ids:
            return {
                'status': 'blocked',
                'applied': False,
                'reason': 'pending_repair_batches_changed',
                'pending_batch_count': len(batches),
                'completed_batch_count': 0,
                'batches': [],
            }

        pending_batch_snapshots = [
            {
                'repair_batch_id': str(batch.repair_batch_id),
                'required_rebuild_trade_dates': list(
                    batch.required_rebuild_trade_dates or []
                ),
                'verification_metadata': dict(
                    getattr(batch, 'verification_metadata', None) or {}
                ),
            }
            for batch in batches
        ]
        pending_membership_authority_by_date: dict[
            date, Mapping[str, Any] | None
        ] = {}
        for required_date in sorted(
            {
                market_date
                for batch in batches
                for market_date in _normalize_required_dates(
                    batch.required_rebuild_trade_dates
                )
            }
        ):
            try:
                pending_membership_authority_by_date[required_date] = (
                    resolve_pending_exact_market_membership_evidence(
                        market_date=required_date,
                        pending=pending_batch_snapshots,
                        input_fingerprint_loader=lambda market_date, symbols: (
                            exact_daily_price_bar_scope_fingerprint(
                                session,
                                market_date=market_date,
                                symbols=symbols,
                            )
                        ),
                    )
                )
            except (RuntimeError, TypeError, ValueError) as exc:
                pending_membership_authority_by_date[required_date] = {
                    'resolution_error': type(exc).__name__,
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
                pending_membership_authority_by_date=(
                    pending_membership_authority_by_date
                ),
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
