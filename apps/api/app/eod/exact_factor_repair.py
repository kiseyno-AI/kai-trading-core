from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import date, datetime, timezone
import json
import logging
import sys
from typing import Any

from sqlalchemy import delete, select, text

from app.db import session_scope
from app.db_models import (
    DailyAdvancedValueFactorResult,
    DailyBetaFactorResult,
    DailyCompositeFactorResult,
    DailyLowRiskFactorResult,
    DailyMomentumFactorResult,
    DailyPiotroskiFilterResult,
    DailyQualityFactorResult,
    DailyUnifiedValueFactorResult,
    DailyValueFactorResult,
    Symbol,
)
from app.eod.market_membership import (
    MARKET_MEMBERSHIP_BASIS,
    MARKET_MEMBERSHIP_SCHEMA_VERSION,
    market_membership_sha256,
    validate_market_membership_snapshot,
)
from app.eod.finality import validate_finalized_krx_write_date
from app.eod.pykrx_backfill import (
    PYKRX_BACKFILL_SERVICE,
    _get_market_ohlcv_by_ticker,
)
from app.eod.price_bar_repair_state import (
    PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE,
    load_pending_price_bar_rebuild_batches,
    record_pending_exact_factor_rebuild,
    resolve_pending_exact_market_membership_evidence,
    resolve_pending_frozen_price_dependent_membership,
)
from app.market_data_store import (
    advisory_eod_collection_lock,
    build_daily_derived_market_scope_authority,
    list_symbols_with_daily_bar_on_date,
)
from app.repositories.eod_run_repository import list_eod_collection_runs
from app.signals.advanced_value_factor import (
    daily_advanced_value_factor_service,
)
from app.signals.beta_factor import daily_beta_factor_service
from app.signals.composite_factor import daily_composite_factor_service
from app.signals.low_risk_factor import daily_low_risk_factor_service
from app.signals.momentum_factor import daily_momentum_factor_service
from app.signals.piotroski_filter import daily_piotroski_filter_service
from app.signals.quality_factor import daily_quality_factor_service
from app.signals.unified_value_factor import (
    daily_unified_value_factor_service,
)
from app.signals.value_factor import daily_value_factor_service


logger = logging.getLogger(__name__)
EXACT_FACTOR_REPAIR_MARKETS = ('KOSPI', 'KOSDAQ')
MINIMUM_MARKET_MEMBERSHIP = {
    'KOSPI': 700,
    'KOSDAQ': 1200,
}
EXACT_FACTOR_REPAIR_STATEMENT_TIMEOUT_MS = 60_000
MEMBERSHIP_POLICY_LIVE_THEN_STORED = 'live_then_stored'
MEMBERSHIP_POLICY_STORED_ONLY = 'stored_only'
MEMBERSHIP_POLICIES = frozenset(
    {
        MEMBERSHIP_POLICY_LIVE_THEN_STORED,
        MEMBERSHIP_POLICY_STORED_ONLY,
    }
)

FACTOR_RESULT_MODELS = {
    'piotroski_filter': DailyPiotroskiFilterResult,
    'momentum_factors': DailyMomentumFactorResult,
    'value_factors': DailyValueFactorResult,
    'low_risk_factors': DailyLowRiskFactorResult,
    'beta_factors': DailyBetaFactorResult,
    'advanced_value_factors': DailyAdvancedValueFactorResult,
    'unified_value_factors': DailyUnifiedValueFactorResult,
    'quality_factors': DailyQualityFactorResult,
    'composite_factors': DailyCompositeFactorResult,
}


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    for pattern in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(raw, pattern).date()
        except ValueError:
            continue
    raise ValueError('market date must use YYYY-MM-DD or YYYYMMDD format')


def _normalize_provider_symbol(value: object) -> str | None:
    raw = str(value or '').strip().upper()
    symbol = raw.zfill(6)
    if len(symbol) != 6 or not symbol.isdigit():
        return None
    return symbol


def _extract_exact_market_membership(
    frame: object,
    *,
    market: str,
) -> tuple[list[str], list[str]]:
    if frame is None or bool(getattr(frame, 'empty', True)):
        raise ValueError(f'{market} market payload is empty')
    index = getattr(frame, 'index', None)
    tolist = getattr(index, 'tolist', None)
    if not callable(tolist):
        raise ValueError(f'{market} market payload has no iterable index')
    symbols: list[str] = []
    skipped_codes: list[str] = []
    all_provider_codes: list[str] = []
    for value in tolist():
        raw = str(value or '').strip().upper()
        if not raw:
            raise ValueError(f'{market} market payload contains a blank symbol')
        symbol = _normalize_provider_symbol(value)
        if symbol is None:
            skipped_codes.append(raw)
            all_provider_codes.append(raw)
            continue
        symbols.append(symbol)
        all_provider_codes.append(symbol)
    if len(all_provider_codes) != len(set(all_provider_codes)):
        raise ValueError(f'{market} market payload contains duplicate symbols')
    minimum = MINIMUM_MARKET_MEMBERSHIP[market]
    if len(symbols) < minimum:
        raise ValueError(
            f'{market} membership below safety floor: {len(symbols)} < {minimum}'
        )
    return sorted(symbols), sorted(skipped_codes)


def _extract_stored_exact_market_membership(
    run: Mapping[str, Any],
    *,
    market_date: date,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    if not isinstance(run, Mapping):
        raise ValueError('same-date EOD run metadata is unavailable')
    if (
        isinstance(run.get('id'), bool)
        or not isinstance(run.get('id'), int)
        or int(run['id']) <= 0
    ):
        raise ValueError('stored EOD run id is invalid')
    if _normalize_market_date(run.get('trade_date')) != market_date:
        raise ValueError('stored EOD run date does not match requested date')
    if run.get('market') != 'KRX':
        raise ValueError('stored EOD run market is not KRX')
    if str(run.get('source') or '').strip() != 'pykrx':
        raise ValueError('stored EOD run source is not pykrx')
    if run.get('status') not in {'completed', 'completed_with_errors'}:
        raise ValueError('stored EOD run status is not complete')

    metadata = run.get('run_metadata')
    snapshot = (
        metadata.get('universe_snapshot')
        if isinstance(metadata, Mapping)
        else None
    )
    expected_status = {
        market: 'pykrx_market_ohlcv'
        for market in EXACT_FACTOR_REPAIR_MARKETS
    }
    if not isinstance(snapshot, Mapping):
        raise ValueError('stored EOD run has no universe snapshot')
    if snapshot.get('complete') is not True:
        raise ValueError('stored universe snapshot is incomplete')
    if _normalize_market_date(snapshot.get('latest_trade_date')) != market_date:
        raise ValueError('stored universe snapshot date does not match requested date')
    if snapshot.get('universe_source') != 'pykrx_market_ohlcv':
        raise ValueError('stored universe snapshot is not direct PyKRX evidence')
    if snapshot.get('source') != 'pykrx':
        raise ValueError('stored universe snapshot source is not pykrx')
    if snapshot.get('markets') != list(EXACT_FACTOR_REPAIR_MARKETS):
        raise ValueError('stored universe snapshot markets are not exact')
    if snapshot.get('market_status') != expected_status:
        raise ValueError('stored universe snapshot market status is not direct PyKRX')
    if snapshot.get('market_errors') != []:
        raise ValueError('stored universe snapshot contains market errors')
    if snapshot.get('unavailable_markets') != []:
        raise ValueError('stored universe snapshot contains unavailable markets')
    if snapshot.get('membership_basis') != 'end_date_snapshot':
        raise ValueError('stored universe snapshot membership basis is invalid')

    membership = snapshot.get('market_membership')
    raw_market_entries = (
        membership.get('markets')
        if isinstance(membership, Mapping)
        and isinstance(membership.get('markets'), Mapping)
        else {}
    )
    requested_symbols = [
        symbol
        for market in EXACT_FACTOR_REPAIR_MARKETS
        for symbol in (
            raw_market_entries.get(market, {}).get('symbols', [])
            if isinstance(raw_market_entries.get(market), Mapping)
            else []
        )
    ]
    validated = validate_market_membership_snapshot(
        membership,
        requested_symbols=requested_symbols,
        markets=EXACT_FACTOR_REPAIR_MARKETS,
    )
    if validated is None:
        raise ValueError('stored market membership proof is invalid')
    if (
        isinstance(snapshot.get('symbol_count'), bool)
        or snapshot.get('symbol_count') != len(requested_symbols)
    ):
        raise ValueError('stored universe symbol count does not match membership')

    symbols_by_market: dict[str, list[str]] = {}
    skipped_by_market: dict[str, list[str]] = {}
    for market in EXACT_FACTOR_REPAIR_MARKETS:
        numeric_symbols: list[str] = []
        skipped_codes: list[str] = []
        for raw_symbol in validated[market]:
            normalized = str(raw_symbol).strip().upper()
            symbol = _normalize_provider_symbol(normalized)
            if symbol is None or symbol != normalized:
                skipped_codes.append(normalized)
            else:
                numeric_symbols.append(symbol)
        minimum = MINIMUM_MARKET_MEMBERSHIP[market]
        if len(numeric_symbols) < minimum:
            raise ValueError(
                f'{market} stored membership below safety floor: '
                f'{len(numeric_symbols)} < {minimum}'
            )
        symbols_by_market[market] = sorted(numeric_symbols)
        skipped_by_market[market] = sorted(skipped_codes)
    return symbols_by_market, skipped_by_market


def _build_ranking_membership_snapshot(
    symbols_by_market: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    if set(symbols_by_market) != set(EXACT_FACTOR_REPAIR_MARKETS):
        raise ValueError('ranking membership markets are incomplete')
    normalized = {
        market: sorted(str(symbol).strip().upper() for symbol in symbols)
        for market, symbols in symbols_by_market.items()
    }
    all_symbols = [
        symbol
        for market in EXACT_FACTOR_REPAIR_MARKETS
        for symbol in normalized[market]
    ]
    if len(all_symbols) != len(set(all_symbols)):
        raise ValueError('ranking membership contains duplicate symbols')
    return {
        'schema_version': MARKET_MEMBERSHIP_SCHEMA_VERSION,
        'membership_basis': MARKET_MEMBERSHIP_BASIS,
        'symbol_count': len(all_symbols),
        'markets': {
            market: {
                'symbols': normalized[market],
                'symbol_count': len(normalized[market]),
                'membership_sha256': market_membership_sha256(
                    normalized[market]
                ),
            }
            for market in EXACT_FACTOR_REPAIR_MARKETS
        },
    }


def _build_factor_symbols_for_date_query(
    model: Any,
    *,
    market_date: date,
):
    return (
        select(Symbol.symbol)
        .select_from(model)
        .join(Symbol, Symbol.id == model.symbol_id)
        .where(model.market_date == market_date)
    )


def load_stale_outside_membership_details(
    *,
    market_date: date,
    exact_symbols: Sequence[str],
    protected_symbols: Sequence[str] = (),
) -> dict[str, dict[str, Any]]:
    membership = list(exact_symbols)
    if not membership:
        raise ValueError('exact membership must not be empty')
    details: dict[str, dict[str, Any]] = {}
    retained_membership_set = set(membership).union(protected_symbols)
    with session_scope(
        statement_timeout_ms=EXACT_FACTOR_REPAIR_STATEMENT_TIMEOUT_MS,
    ) as session:
        for stage_id, model in FACTOR_RESULT_MODELS.items():
            stored_symbols = list(
                session.scalars(
                    _build_factor_symbols_for_date_query(
                        model,
                        market_date=market_date,
                    )
                )
            )
            # The exact KRX scope normally contains thousands of symbols.
            # Filtering that scope in SQL expands into an equally large NOT IN
            # parameter list and can exhaust the repair statement timeout.
            # Keep the indexed, market/date-bounded lookup small and apply the
            # already-loaded membership proof in memory instead.
            outside_symbols = sorted(
                str(symbol)
                for symbol in stored_symbols
                if str(symbol) not in retained_membership_set
            )
            details[stage_id] = {
                'count': len(outside_symbols),
                'symbol_preview': outside_symbols[:20],
            }
    return details


def verify_exact_factor_rows(
    *,
    market_date: date,
    exact_symbols: Sequence[str],
    repair_started_at: datetime,
) -> dict[str, dict[str, int]]:
    """Prove that every deterministic stage actually wrote the exact scope."""

    membership = set(exact_symbols)
    if not membership:
        raise ValueError('exact membership must not be empty')

    verified: dict[str, dict[str, int]] = {}
    with session_scope(
        statement_timeout_ms=EXACT_FACTOR_REPAIR_STATEMENT_TIMEOUT_MS,
    ) as session:
        for stage_id, model in FACTOR_RESULT_MODELS.items():
            rows = session.execute(
                select(Symbol.symbol, model.updated_at)
                .join(model, model.symbol_id == Symbol.id)
                .where(
                    model.market_date == market_date,
                    Symbol.symbol.in_(membership),
                )
            ).all()
            observed_symbols = [str(row.symbol) for row in rows]
            observed_set = set(observed_symbols)
            missing = sorted(membership - observed_set)
            duplicate_count = len(observed_symbols) - len(observed_set)
            stale = sorted(
                str(row.symbol)
                for row in rows
                if row.updated_at is None or row.updated_at < repair_started_at
            )
            if missing or duplicate_count or stale:
                raise RuntimeError(
                    f'{stage_id} persistence verification failed: '
                    f'observed={len(observed_symbols)}, expected={len(membership)}, '
                    f'missing={missing[:20]}, duplicates={duplicate_count}, '
                    f'stale={stale[:20]}'
                )
            verified[stage_id] = {
                'exact_row_count': len(observed_symbols),
                'fresh_exact_row_count': len(observed_symbols) - len(stale),
            }
    return verified


def prune_stale_factor_rows(
    *,
    market_date: date,
    exact_symbols: Sequence[str],
    repair_started_at: datetime,
    expected_stale_counts: Mapping[str, int],
    protected_symbols: Sequence[str] = (),
) -> dict[str, int]:
    membership = list(exact_symbols)
    if not membership:
        raise ValueError('exact membership must not be empty')
    if set(expected_stale_counts) != set(FACTOR_RESULT_MODELS):
        raise ValueError('expected stale counts must cover every factor table')

    pruned_counts: dict[str, int] = {}
    membership_set = set(membership)
    retained_membership_set = membership_set.union(protected_symbols)
    with session_scope(
        statement_timeout_ms=EXACT_FACTOR_REPAIR_STATEMENT_TIMEOUT_MS,
    ) as session:
        preparer = session.connection().dialect.identifier_preparer
        for model in FACTOR_RESULT_MODELS.values():
            table_name = preparer.format_table(model.__table__)
            session.execute(
                text(
                    f'LOCK TABLE {table_name} '
                    'IN SHARE ROW EXCLUSIVE MODE'
                )
            )
        symbol_by_id = {
            int(row.id): str(row.symbol)
            for row in session.execute(
                select(Symbol.id.label('id'), Symbol.symbol.label('symbol'))
            ).all()
        }
        rows_by_stage: dict[str, list[tuple[int, str, datetime]]] = {}
        for stage_id, model in FACTOR_RESULT_MODELS.items():
            stored_rows = [
                (
                    int(row.id),
                    symbol_by_id[int(row.symbol_id)],
                    row.updated_at,
                )
                for row in session.execute(
                    select(
                        model.id.label('id'),
                        model.symbol_id.label('symbol_id'),
                        model.updated_at.label('updated_at'),
                    )
                    .where(model.market_date == market_date)
                ).all()
            ]
            rows_by_stage[stage_id] = stored_rows
            exact_rows = sum(
                1 for _row_id, symbol, _updated_at in stored_rows
                if symbol in membership_set
            )
            fresh_exact_rows = sum(
                1
                for _row_id, symbol, updated_at in stored_rows
                if symbol in membership_set and updated_at >= repair_started_at
            )
            if exact_rows != len(membership) or fresh_exact_rows != len(membership):
                raise RuntimeError(
                    f'{stage_id} exact rows are not fully fresh: '
                    f'exact={exact_rows}, fresh={fresh_exact_rows}, '
                    f'expected={len(membership)}'
                )
        for stage_id, model in FACTOR_RESULT_MODELS.items():
            expected = int(expected_stale_counts[stage_id])
            stale_ids = [
                row_id
                for row_id, symbol, _updated_at in rows_by_stage[stage_id]
                if symbol not in retained_membership_set
            ]
            observed = len(stale_ids)
            if observed != expected:
                raise RuntimeError(
                    f'{stage_id} stale count changed before prune: '
                    f'observed={observed}, expected={expected}'
                )
            deleted = session.execute(
                delete(model).where(model.id.in_(stale_ids))
            ).rowcount
            deleted_count = int(deleted or 0)
            if deleted_count != expected:
                raise RuntimeError(
                    f'{stage_id} stale delete mismatch: '
                    f'deleted={deleted_count}, expected={expected}'
                )
            pruned_counts[stage_id] = deleted_count

        for stage_id, model in FACTOR_RESULT_MODELS.items():
            remaining_symbol_ids = list(
                session.scalars(
                    select(model.symbol_id)
                    .where(model.market_date == market_date)
                )
            )
            remaining = sum(
                1
                for symbol_id in remaining_symbol_ids
                if symbol_by_id[int(symbol_id)] not in retained_membership_set
            )
            if remaining:
                raise RuntimeError(
                    f'{stage_id} still has {remaining} rows outside membership'
                )
    return pruned_counts


def _default_stage_runners() -> dict[str, Callable[..., dict[str, Any]]]:
    return {
        'piotroski_filter': daily_piotroski_filter_service.rebuild_for_market_date,
        'momentum_factors': daily_momentum_factor_service.rebuild_for_market_date,
        'value_factors': daily_value_factor_service.rebuild_for_market_date,
        'low_risk_factors': daily_low_risk_factor_service.rebuild_for_market_date,
        'beta_factors': daily_beta_factor_service.rebuild_for_market_date,
        'advanced_value_factors': (
            daily_advanced_value_factor_service.rebuild_for_market_date
        ),
        'unified_value_factors': (
            daily_unified_value_factor_service.rebuild_for_market_date
        ),
        'quality_factors': daily_quality_factor_service.rebuild_for_market_date,
        'composite_factors': daily_composite_factor_service.rebuild_for_market_date,
    }


class ExactFactorRepairService:
    def __init__(
        self,
        *,
        trade_date_resolver: Callable[..., str] | None = None,
        market_ohlcv_loader: Callable[[str, str], object] | None = None,
        stored_eod_run_loader: (
            Callable[..., Sequence[dict[str, Any]]] | None
        ) = None,
        price_bar_symbols_loader: Callable[[str], list[str]] | None = None,
        stale_details_loader: Callable[..., dict[str, dict[str, Any]]] | None = None,
        applied_scope_verifier: Callable[..., dict[str, dict[str, int]]] | None = None,
        stale_pruner: Callable[..., dict[str, int]] | None = None,
        completion_recorder: Callable[..., dict[str, Any]] | None = None,
        pending_batch_loader: (
            Callable[[], Sequence[Mapping[str, Any]]] | None
        ) = None,
        pending_membership_evidence_resolver: (
            Callable[..., Mapping[str, Any] | None] | None
        ) = None,
        lock_factory: Callable[..., AbstractContextManager[bool]] | None = None,
        stage_runners: Mapping[str, Callable[..., dict[str, Any]]] | None = None,
    ) -> None:
        self.trade_date_resolver = (
            trade_date_resolver or PYKRX_BACKFILL_SERVICE.resolve_latest_trade_date
        )
        self.market_ohlcv_loader = (
            market_ohlcv_loader or _get_market_ohlcv_by_ticker
        )
        self.stored_eod_run_loader = (
            stored_eod_run_loader or list_eod_collection_runs
        )
        self.price_bar_symbols_loader = (
            price_bar_symbols_loader or list_symbols_with_daily_bar_on_date
        )
        self.stale_details_loader = (
            stale_details_loader or load_stale_outside_membership_details
        )
        self.applied_scope_verifier = (
            applied_scope_verifier or verify_exact_factor_rows
        )
        self.stale_pruner = stale_pruner or prune_stale_factor_rows
        self.completion_recorder = (
            completion_recorder or record_pending_exact_factor_rebuild
        )
        self.pending_batch_loader = (
            pending_batch_loader
            or load_pending_price_bar_rebuild_batches
        )
        self.pending_membership_evidence_resolver = (
            pending_membership_evidence_resolver
            or resolve_pending_exact_market_membership_evidence
        )
        self.lock_factory = lock_factory or advisory_eod_collection_lock
        self.stage_runners = dict(stage_runners or _default_stage_runners())
        if list(self.stage_runners) != list(FACTOR_RESULT_MODELS):
            raise ValueError(
                'factor repair stages must exactly match deterministic dependency order'
            )

    @staticmethod
    def _result(
        *,
        market_date: date,
        apply: bool,
        status: str,
        reason: str | None = None,
        **details: Any,
    ) -> dict[str, Any]:
        result = {
            'market_date': market_date.isoformat(),
            'markets': list(EXACT_FACTOR_REPAIR_MARKETS),
            'mode': 'apply' if apply else 'dry_run',
            'status': status,
            'applied': status == 'completed' and apply,
            **details,
        }
        if reason:
            result['reason'] = reason
        return result

    @staticmethod
    def _validated_atomic_scope_finalization(
        *,
        stage_id: str,
        market_date: str,
        exact_symbols: Sequence[str],
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        finalization = result.get('scope_finalization')
        if not isinstance(finalization, Mapping):
            raise RuntimeError(
                f'{stage_id} did not return atomic exact-scope finalization'
            )
        membership_hash = market_membership_sha256(exact_symbols)
        expected = {
            'stage': stage_id,
            'market_date': market_date,
            'exact_symbol_count': len(exact_symbols),
            'exact_membership_sha256': membership_hash,
            'successful_symbol_count': len(exact_symbols),
            'successful_membership_sha256': membership_hash,
            'authoritative_market_scope': True,
            'authoritative_markets': sorted(EXACT_FACTOR_REPAIR_MARKETS),
            'all_market_authority': False,
            'ready': True,
        }
        mismatches = [
            field
            for field, expected_value in expected.items()
            if finalization.get(field) != expected_value
        ]
        deleted_count = finalization.get('deleted_stale_row_count')
        if (
            isinstance(deleted_count, bool)
            or not isinstance(deleted_count, int)
            or deleted_count < 0
        ):
            mismatches.append('deleted_stale_row_count')
        if mismatches:
            raise RuntimeError(
                f'{stage_id} atomic exact-scope finalization mismatch: '
                + ','.join(dict.fromkeys(mismatches))
            )
        return dict(finalization)

    def repair_for_market_date(
        self,
        *,
        market_date: str | date,
        apply: bool = False,
        prune_stale: bool = False,
        expected_provider_membership_count: int | None = None,
        expected_membership_sha256: str | None = None,
        expected_membership_evidence_source: str | None = None,
        expected_membership_evidence_run_id: int | None = None,
        expected_membership_evidence_run_source: str | None = None,
        expected_membership_evidence_sha256: str | None = None,
        expected_membership_evidence_repair_batch_ids: (
            Sequence[str] | None
        ) = None,
        membership_policy: str = MEMBERSHIP_POLICY_LIVE_THEN_STORED,
    ) -> dict[str, Any]:
        normalized_date = _normalize_market_date(market_date)
        normalized_membership_policy = str(membership_policy).strip().lower()
        if normalized_membership_policy not in MEMBERSHIP_POLICIES:
            return self._result(
                market_date=normalized_date,
                apply=apply,
                status='blocked',
                reason='unsupported_membership_policy',
                membership_policy=normalized_membership_policy,
            )
        if apply:
            validate_finalized_krx_write_date(
                normalized_date,
                operation='exact KRX factor repair',
            )
        if prune_stale and not apply:
            return self._result(
                market_date=normalized_date,
                apply=False,
                status='blocked',
                reason='prune_stale_requires_apply',
                prune_stale_requested=True,
            )
        iso_date = normalized_date.isoformat()
        compact_date = normalized_date.strftime('%Y%m%d')

        with self.lock_factory(
            market='KRX',
            trade_date=iso_date,
            source='daily_price_bars',
        ) as lock_acquired:
            if not lock_acquired:
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason='advisory_lock_unavailable',
                )

            try:
                pending_batches = list(self.pending_batch_loader())
                frozen_membership = (
                    resolve_pending_frozen_price_dependent_membership(
                        market_date=normalized_date,
                        pending=pending_batches,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason='pending_frozen_membership_invalid',
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )

            stored_only = (
                normalized_membership_policy
                == MEMBERSHIP_POLICY_STORED_ONLY
            )
            live_trade_date_error: Exception | None = None
            if stored_only:
                live_trade_date_error = RuntimeError(
                    'live membership disabled by stored-only policy'
                )
                resolved_date = None
            else:
                try:
                    resolved_date = _normalize_market_date(
                        self.trade_date_resolver(end_date=compact_date)
                    )
                except Exception as exc:  # noqa: BLE001
                    live_trade_date_error = exc
                    resolved_date = None
            if (
                live_trade_date_error is None
                and resolved_date != normalized_date
            ):
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason='not_an_exact_trading_session',
                    resolved_market_date=resolved_date.isoformat(),
                )

            exact_symbols: list[str] = []
            ranking_symbols_by_market: dict[str, list[str]] = {}
            market_symbol_counts: dict[str, int] = {}
            skipped_provider_code_counts_by_market: dict[str, int] = {}
            skipped_provider_codes: list[dict[str, str]] = []
            seen_provider_codes: set[str] = set()
            protected_provider_codes: list[str] = []
            membership_evidence_source = 'live_pykrx_market_ohlcv'
            membership_evidence_run_id: int | None = None
            membership_evidence_run_source: str | None = None
            membership_evidence_sha256: str | None = None
            membership_evidence_repair_batch_ids: list[str] = []
            live_membership_error: Exception | None = live_trade_date_error
            live_markets = (
                ()
                if live_membership_error is not None
                else EXACT_FACTOR_REPAIR_MARKETS
            )
            for market in live_markets:
                try:
                    frame = self.market_ohlcv_loader(compact_date, market)
                    market_symbols, market_skipped_codes = (
                        _extract_exact_market_membership(
                            frame,
                            market=market,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    live_membership_error = exc
                    break
                provider_codes = [*market_symbols, *market_skipped_codes]
                duplicates = sorted(seen_provider_codes.intersection(provider_codes))
                if duplicates:
                    return self._result(
                        market_date=normalized_date,
                        apply=apply,
                        status='blocked',
                        reason='duplicate_cross_market_membership',
                        duplicate_symbol_count=len(duplicates),
                        duplicate_symbols=duplicates[:20],
                    )
                seen_provider_codes.update(provider_codes)
                exact_symbols.extend(market_symbols)
                ranking_symbols_by_market[market] = list(market_symbols)
                protected_provider_codes.extend(market_skipped_codes)
                market_symbol_counts[market] = len(market_symbols)
                skipped_provider_code_counts_by_market[market] = len(
                    market_skipped_codes
                )
                skipped_provider_codes.extend(
                    {'market': market, 'code': code[:64]}
                    for code in market_skipped_codes[:20]
                )

            if live_membership_error is not None:
                stored_error: Exception | None = None
                stored_run: Mapping[str, Any] | None = None
                stored_symbols: dict[str, list[str]] | None = None
                stored_skipped: dict[str, list[str]] | None = None
                try:
                    stored_runs = self.stored_eod_run_loader(
                        market='KRX',
                        trade_date=iso_date,
                        source='pykrx',
                        limit=20,
                    )
                    for candidate in stored_runs:
                        try:
                            candidate_symbols, candidate_skipped = (
                                _extract_stored_exact_market_membership(
                                    candidate,
                                    market_date=normalized_date,
                                )
                            )
                        except (TypeError, ValueError):
                            continue
                        stored_run = candidate
                        stored_symbols = candidate_symbols
                        stored_skipped = candidate_skipped
                        break
                    if stored_run is None:
                        raise ValueError(
                            'no strict same-date PyKRX EOD membership proof is available'
                        )
                except Exception as exc:  # noqa: BLE001
                    stored_error = exc

                pending_proof: Mapping[str, Any] | None = None
                if stored_run is None:
                    try:
                        pending_proof = (
                            self.pending_membership_evidence_resolver(
                                market_date=normalized_date,
                                pending=pending_batches,
                            )
                        )
                        if pending_proof is None:
                            raise ValueError(
                                'no strict pending PyKRX repair proof is available'
                            )
                    except Exception as exc:  # noqa: BLE001
                        return self._result(
                            market_date=normalized_date,
                            apply=apply,
                            status='blocked',
                            reason='invalid_exact_market_membership',
                            error_type=type(exc).__name__,
                            error_message=str(exc),
                            stored_error_type=(
                                type(stored_error).__name__
                                if stored_error is not None
                                else None
                            ),
                            stored_error_message=(
                                str(stored_error)
                                if stored_error is not None
                                else None
                            ),
                            live_error_type=type(live_membership_error).__name__,
                            live_error_message=str(live_membership_error),
                        )

                exact_symbols = []
                ranking_symbols_by_market = {}
                market_symbol_counts = {}
                skipped_provider_code_counts_by_market = {}
                skipped_provider_codes = []
                protected_provider_codes = []
                if stored_run is not None:
                    if stored_symbols is None or stored_skipped is None:
                        raise RuntimeError('stored membership proof is incomplete')
                    symbols_by_market = stored_symbols
                    skipped_by_market = stored_skipped
                    membership_evidence_source = 'stored_eod_run_metadata'
                    membership_evidence_run_id = int(stored_run['id'])
                    membership_evidence_run_source = str(stored_run['source'])
                else:
                    if pending_proof is None:
                        raise RuntimeError('pending membership proof is unavailable')
                    symbols_by_market = {
                        market: list(
                            dict(pending_proof['symbols_by_market'])[market]
                        )
                        for market in EXACT_FACTOR_REPAIR_MARKETS
                    }
                    skipped_by_market = {
                        market: list(
                            dict(
                                pending_proof[
                                    'protected_provider_codes_by_market'
                                ]
                            )[market]
                        )
                        for market in EXACT_FACTOR_REPAIR_MARKETS
                    }
                    membership_evidence_source = str(
                        pending_proof['membership_evidence_source']
                    )
                    if membership_evidence_source != (
                        PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE
                    ):
                        raise RuntimeError('pending membership source is invalid')
                    membership_evidence_run_id = None
                    membership_evidence_run_source = str(
                        pending_proof['membership_evidence_run_source']
                    )
                    membership_evidence_sha256 = str(
                        pending_proof['membership_evidence_sha256']
                    )
                    membership_evidence_repair_batch_ids = list(
                        pending_proof[
                            'membership_evidence_repair_batch_ids'
                        ]
                    )
                for market in EXACT_FACTOR_REPAIR_MARKETS:
                    market_symbols = symbols_by_market[market]
                    market_skipped_codes = skipped_by_market[market]
                    exact_symbols.extend(market_symbols)
                    ranking_symbols_by_market[market] = list(market_symbols)
                    protected_provider_codes.extend(market_skipped_codes)
                    market_symbol_counts[market] = len(market_symbols)
                    skipped_provider_code_counts_by_market[market] = len(
                        market_skipped_codes
                    )
                    skipped_provider_codes.extend(
                        {'market': market, 'code': code[:64]}
                        for code in market_skipped_codes[:20]
                    )

            exact_symbols = sorted(exact_symbols)
            protected_provider_codes = sorted(protected_provider_codes)
            full_provider_membership = sorted(
                [*exact_symbols, *protected_provider_codes]
            )
            membership_hash = market_membership_sha256(full_provider_membership)
            provider_ranking_membership_hash = market_membership_sha256(
                exact_symbols
            )
            if (
                frozen_membership is not None
                and tuple(exact_symbols) != frozen_membership
            ):
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason=(
                        'provider_membership_mismatch_pending_frozen_scope'
                    ),
                    provider_ranking_membership_count=len(exact_symbols),
                    provider_ranking_membership_sha256=(
                        provider_ranking_membership_hash
                    ),
                    frozen_membership_count=len(frozen_membership),
                    frozen_membership_sha256=market_membership_sha256(
                        frozen_membership
                    ),
                )
            if frozen_membership is not None:
                exact_symbols = list(frozen_membership)
            ranking_membership_hash = market_membership_sha256(exact_symbols)
            try:
                authoritative_market_scope = (
                    build_daily_derived_market_scope_authority(
                        market_date=normalized_date,
                        exact_symbols=exact_symbols,
                        market_membership_snapshot=(
                            _build_ranking_membership_snapshot(
                                ranking_symbols_by_market
                            )
                        ),
                    )
                )
            except (TypeError, ValueError) as exc:
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason='invalid_authoritative_market_scope',
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            exact_symbol_set = set(exact_symbols)
            full_provider_membership_set = set(full_provider_membership)
            try:
                stored_bar_symbols = set(
                    self.price_bar_symbols_loader(iso_date)
                )
                stale_details = self.stale_details_loader(
                    market_date=normalized_date,
                    exact_symbols=exact_symbols,
                    protected_symbols=protected_provider_codes,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    'Unable to validate stored exact factor scope for %s',
                    iso_date,
                )
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason='stored_scope_validation_failed',
                    membership_basis='exact_market_date_ohlcv',
                    membership_sha256=membership_hash,
                    exact_membership_count=len(exact_symbols),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if set(stale_details) != set(FACTOR_RESULT_MODELS):
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason='stale_factor_scope_incomplete',
                    membership_basis='exact_market_date_ohlcv',
                    membership_sha256=membership_hash,
                    exact_membership_count=len(exact_symbols),
                    observed_stale_tables=sorted(stale_details),
                    expected_stale_tables=list(FACTOR_RESULT_MODELS),
                )
            missing_price_symbols = sorted(exact_symbol_set - stored_bar_symbols)
            outside_price_symbols = sorted(
                stored_bar_symbols - full_provider_membership_set
            )
            stale_counts = {
                stage_id: int(details.get('count') or 0)
                for stage_id, details in stale_details.items()
            }
            evidence = {
                'membership_policy': normalized_membership_policy,
                'membership_basis': 'exact_market_date_ohlcv',
                'membership_evidence_source': membership_evidence_source,
                'membership_evidence_run_id': membership_evidence_run_id,
                'membership_evidence_run_source': membership_evidence_run_source,
                'membership_evidence_sha256': membership_evidence_sha256,
                'membership_evidence_repair_batch_ids': (
                    membership_evidence_repair_batch_ids
                ),
                'membership_sha256': membership_hash,
                'ranking_membership_sha256': ranking_membership_hash,
                'ranking_membership_authority': (
                    'pending_price_bar_repair_frozen_manifest'
                    if frozen_membership is not None
                    else membership_evidence_source
                ),
                'pending_frozen_membership': (
                    frozen_membership is not None
                ),
                'exact_provider_membership_count': len(full_provider_membership),
                'exact_membership_count': len(exact_symbols),
                'market_symbol_counts': market_symbol_counts,
                'provider_excluded_from_ranking_count': sum(
                    skipped_provider_code_counts_by_market.values()
                ),
                'provider_excluded_from_ranking_counts_by_market': (
                    skipped_provider_code_counts_by_market
                ),
                'provider_excluded_from_ranking_codes': (
                    protected_provider_codes
                ),
                'provider_excluded_from_ranking_preview': skipped_provider_codes[:20],
                'stored_price_bar_count': len(stored_bar_symbols),
                'missing_exact_price_bar_count': len(missing_price_symbols),
                'missing_exact_price_bar_symbols': missing_price_symbols[:20],
                'stale_outside_membership_price_bar_count': len(
                    outside_price_symbols
                ),
                'stale_outside_membership_price_bar_symbols': (
                    outside_price_symbols[:20]
                ),
                'stale_outside_membership_counts': stale_counts,
                'stale_outside_membership': stale_details,
                'prune_stale_requested': prune_stale,
                'pruned_stale_counts': {
                    stage_id: 0 for stage_id in FACTOR_RESULT_MODELS
                },
                'stages': list(self.stage_runners),
            }
            if apply and (
                expected_provider_membership_count is None
                or expected_membership_sha256 is None
            ):
                return self._result(
                    market_date=normalized_date,
                    apply=True,
                    status='blocked',
                    reason='expected_membership_evidence_required',
                    **evidence,
                )
            if apply and (
                int(expected_provider_membership_count)
                != len(full_provider_membership)
                or str(expected_membership_sha256).strip().lower()
                != membership_hash
            ):
                return self._result(
                    market_date=normalized_date,
                    apply=True,
                    status='blocked',
                    reason='expected_membership_evidence_mismatch',
                    expected_provider_membership_count=(
                        expected_provider_membership_count
                    ),
                    expected_membership_sha256=expected_membership_sha256,
                    **evidence,
                )
            if apply and expected_membership_evidence_source is not None:
                expected_batch_ids = sorted(
                    {
                        str(batch_id).strip()
                        for batch_id in (
                            expected_membership_evidence_repair_batch_ids or ()
                        )
                        if str(batch_id).strip()
                    }
                )
                expected_evidence_sha256 = (
                    str(expected_membership_evidence_sha256).strip().lower()
                    if expected_membership_evidence_sha256 is not None
                    else None
                )
                if isinstance(expected_membership_evidence_run_id, bool):
                    return self._result(
                        market_date=normalized_date,
                        apply=True,
                        status='blocked',
                        reason='expected_membership_lineage_invalid',
                        **evidence,
                    )
                expected_run_id = (
                    int(expected_membership_evidence_run_id)
                    if expected_membership_evidence_run_id is not None
                    else None
                )
                expected_run_source = (
                    str(expected_membership_evidence_run_source).strip()
                    if expected_membership_evidence_run_source is not None
                    else None
                )
                if (
                    str(expected_membership_evidence_source).strip()
                    != membership_evidence_source
                    or expected_run_id != membership_evidence_run_id
                    or expected_run_source != membership_evidence_run_source
                    or expected_evidence_sha256 != membership_evidence_sha256
                    or expected_batch_ids
                    != membership_evidence_repair_batch_ids
                ):
                    return self._result(
                        market_date=normalized_date,
                        apply=True,
                        status='blocked',
                        reason='expected_membership_lineage_mismatch',
                        expected_membership_evidence_source=(
                            expected_membership_evidence_source
                        ),
                        expected_membership_evidence_run_id=(
                            expected_run_id
                        ),
                        expected_membership_evidence_run_source=(
                            expected_run_source
                        ),
                        expected_membership_evidence_sha256=(
                            expected_evidence_sha256
                        ),
                        expected_membership_evidence_repair_batch_ids=(
                            expected_batch_ids
                        ),
                        **evidence,
                    )
            if missing_price_symbols:
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason='exact_membership_price_bars_incomplete',
                    **evidence,
                )
            if not apply:
                return self._result(
                    market_date=normalized_date,
                    apply=False,
                    status='ready',
                    **evidence,
                )

            repair_started_at = datetime.now(timezone.utc)
            evidence['repair_started_at'] = repair_started_at.isoformat()
            stage_results: dict[str, dict[str, Any]] = {}
            for stage_id, runner in self.stage_runners.items():
                try:
                    result = runner(
                        market_date=iso_date,
                        symbols=exact_symbols,
                        ranking_symbols=exact_symbols,
                        exact_scope_symbols=exact_symbols,
                        authoritative_market_scope=(
                            authoritative_market_scope
                        ),
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        'Exact factor repair stage %s failed for %s',
                        stage_id,
                        iso_date,
                    )
                    return self._result(
                        market_date=normalized_date,
                        apply=True,
                        status='partial',
                        reason='factor_stage_failed',
                        failed_stage=stage_id,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        stage_results=stage_results,
                        **evidence,
                    )
                stage_results[stage_id] = {
                    key: value
                    for key, value in result.items()
                    if key not in {
                        'items',
                        'requested_symbols',
                        'matched_symbols',
                        'missing_symbols',
                        'failed_symbols',
                        'failed_symbol_errors',
                    }
                }
                persisted = int(result.get('results_persisted') or 0)
                failed = int(result.get('results_failed') or 0)
                missing = list(result.get('missing_symbols') or [])
                if (
                    persisted != len(exact_symbols)
                    or failed
                    or missing
                ):
                    return self._result(
                        market_date=normalized_date,
                        apply=True,
                        status='partial',
                        reason='factor_stage_incomplete',
                        failed_stage=stage_id,
                        expected_persisted=len(exact_symbols),
                        observed_persisted=persisted,
                        observed_failed=failed,
                        observed_missing_symbols=missing[:20],
                        stage_results=stage_results,
                        **evidence,
                    )
                try:
                    atomic_finalization = (
                        self._validated_atomic_scope_finalization(
                            stage_id=stage_id,
                            market_date=iso_date,
                            exact_symbols=exact_symbols,
                            result=result,
                        )
                    )
                except (RuntimeError, TypeError, ValueError) as exc:
                    return self._result(
                        market_date=normalized_date,
                        apply=True,
                        status='partial',
                        reason='factor_stage_atomic_finalization_failed',
                        failed_stage=stage_id,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        stage_results=stage_results,
                        **evidence,
                    )
                stage_results[stage_id]['scope_finalization'] = (
                    atomic_finalization
                )

            try:
                persistence_verification = self.applied_scope_verifier(
                    market_date=normalized_date,
                    exact_symbols=exact_symbols,
                    repair_started_at=repair_started_at,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    'Exact factor persistence verification failed for %s',
                    iso_date,
                )
                return self._result(
                    market_date=normalized_date,
                    apply=True,
                    status='partial',
                    reason='factor_persistence_verification_failed',
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    stage_results=stage_results,
                    **evidence,
                )

            pruned_stale_counts = {
                stage_id: 0 for stage_id in FACTOR_RESULT_MODELS
            }
            if prune_stale:
                try:
                    pruned_stale_counts = self.stale_pruner(
                        market_date=normalized_date,
                        exact_symbols=exact_symbols,
                        repair_started_at=repair_started_at,
                        expected_stale_counts=stale_counts,
                        protected_symbols=protected_provider_codes,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        'Exact factor stale prune failed for %s',
                        iso_date,
                    )
                    return self._result(
                        market_date=normalized_date,
                        apply=True,
                        status='partial',
                        reason='stale_factor_prune_failed',
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        factor_repair_applied=True,
                        stage_results=stage_results,
                        **evidence,
                    )

            completed_evidence = {
                **evidence,
                'persistence_verification': persistence_verification,
                'pruned_stale_counts': pruned_stale_counts,
            }
            try:
                completion_evidence = self.completion_recorder(
                    market_date=normalized_date,
                    exact_symbols=exact_symbols,
                    provider_membership_sha256=membership_hash,
                    repair_started_at=repair_started_at,
                    rebuild_completed_at=datetime.now(timezone.utc),
                    membership_evidence_source=membership_evidence_source,
                    membership_evidence_run_id=membership_evidence_run_id,
                    membership_evidence_run_source=(
                        membership_evidence_run_source
                    ),
                    membership_evidence_sha256=membership_evidence_sha256,
                    membership_evidence_repair_batch_ids=(
                        membership_evidence_repair_batch_ids
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    'Unable to record exact factor rebuild evidence for %s',
                    iso_date,
                )
                return self._result(
                    market_date=normalized_date,
                    apply=True,
                    status='partial',
                    reason='factor_rebuild_evidence_failed',
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    factor_repair_applied=True,
                    stage_results=stage_results,
                    **completed_evidence,
                )
            return self._result(
                market_date=normalized_date,
                apply=True,
                status='completed',
                stage_results=stage_results,
                completion_evidence=completion_evidence,
                **completed_evidence,
            )


exact_factor_repair_service = ExactFactorRepairService()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Dry-run or rebuild deterministic factor rows for one exact '
            'historical KOSPI/KOSDAQ membership. Stale rows are retained '
            'unless --apply and --prune-stale are both explicit.'
        )
    )
    parser.add_argument('--market-date', required=True)
    parser.add_argument(
        '--apply',
        action='store_true',
        help=(
            'Deprecated and blocked here. Use '
            'app.eod.exact_factor_bulk_repair for guarded publication.'
        ),
    )
    parser.add_argument(
        '--prune-stale',
        action='store_true',
        help=(
            'After a fully successful --apply rebuild, transactionally delete '
            'only same-date derived factor rows outside exact membership.'
        ),
    )
    parser.add_argument(
        '--expected-provider-membership-count',
        type=int,
        help=(
            'Required with --apply; copy exact_provider_membership_count from '
            'a reviewed dry-run of the same date.'
        ),
    )
    parser.add_argument(
        '--expected-membership-sha256',
        help=(
            'Required with --apply; copy membership_sha256 from a reviewed '
            'dry-run of the same date.'
        ),
    )
    parser.add_argument(
        '--stored-membership-only',
        action='store_true',
        help=(
            'Disable all live trade-date and PyKRX membership calls; require '
            'a pending frozen scope plus either strict same-date stored EOD '
            'metadata or canonical validated pending missing-bar PyKRX proof. '
            'Pending proof is persisted evidence only and never performs a '
            'live provider call.'
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.apply:
        result = {
            'market_date': args.market_date,
            'mode': 'apply',
            'status': 'blocked',
            'applied': False,
            'reason': 'direct_apply_requires_exact_factor_bulk_repair',
            'required_driver': 'app.eod.exact_factor_bulk_repair',
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 1

    repair_kwargs: dict[str, Any] = {
        'market_date': args.market_date,
        'apply': bool(args.apply),
        'prune_stale': bool(args.prune_stale),
        'expected_provider_membership_count': (
            args.expected_provider_membership_count
        ),
        'expected_membership_sha256': args.expected_membership_sha256,
    }
    if args.stored_membership_only:
        repair_kwargs['membership_policy'] = MEMBERSHIP_POLICY_STORED_ONLY
    result = exact_factor_repair_service.repair_for_market_date(
        **repair_kwargs,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get('status') in {'ready', 'completed'} else 1


if __name__ == '__main__':
    sys.exit(main())
