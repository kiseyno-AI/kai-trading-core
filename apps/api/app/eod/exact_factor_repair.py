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
from app.eod.market_membership import market_membership_sha256
from app.eod.pykrx_backfill import (
    PYKRX_BACKFILL_SERVICE,
    _get_market_ohlcv_by_ticker,
)
from app.market_data_store import (
    advisory_eod_collection_lock,
    list_symbols_with_daily_bar_on_date,
)
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
    membership_set = set(membership).union(protected_symbols)
    with session_scope() as session:
        symbol_by_id = {
            int(row.id): str(row.symbol)
            for row in session.execute(
                select(Symbol.id.label('id'), Symbol.symbol.label('symbol'))
            ).all()
        }
        for stage_id, model in FACTOR_RESULT_MODELS.items():
            stored_symbol_ids = list(
                session.scalars(
                    select(model.symbol_id)
                    .where(model.market_date == market_date)
                )
            )
            outside_symbols = sorted(
                symbol_by_id[int(symbol_id)]
                for symbol_id in stored_symbol_ids
                if symbol_by_id[int(symbol_id)] not in membership_set
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
    with session_scope() as session:
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
    with session_scope() as session:
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
        price_bar_symbols_loader: Callable[[str], list[str]] | None = None,
        stale_details_loader: Callable[..., dict[str, dict[str, Any]]] | None = None,
        applied_scope_verifier: Callable[..., dict[str, dict[str, int]]] | None = None,
        stale_pruner: Callable[..., dict[str, int]] | None = None,
        lock_factory: Callable[..., AbstractContextManager[bool]] | None = None,
        stage_runners: Mapping[str, Callable[..., dict[str, Any]]] | None = None,
    ) -> None:
        self.trade_date_resolver = (
            trade_date_resolver or PYKRX_BACKFILL_SERVICE.resolve_latest_trade_date
        )
        self.market_ohlcv_loader = (
            market_ohlcv_loader or _get_market_ohlcv_by_ticker
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

    def repair_for_market_date(
        self,
        *,
        market_date: str | date,
        apply: bool = False,
        prune_stale: bool = False,
        expected_provider_membership_count: int | None = None,
        expected_membership_sha256: str | None = None,
    ) -> dict[str, Any]:
        normalized_date = _normalize_market_date(market_date)
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
            source='pykrx',
        ) as lock_acquired:
            if not lock_acquired:
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason='advisory_lock_unavailable',
                )

            try:
                resolved_date = _normalize_market_date(
                    self.trade_date_resolver(end_date=compact_date)
                )
            except Exception as exc:  # noqa: BLE001
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason='trade_date_resolution_failed',
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if resolved_date != normalized_date:
                return self._result(
                    market_date=normalized_date,
                    apply=apply,
                    status='blocked',
                    reason='not_an_exact_trading_session',
                    resolved_market_date=resolved_date.isoformat(),
                )

            exact_symbols: list[str] = []
            market_symbol_counts: dict[str, int] = {}
            skipped_provider_code_counts_by_market: dict[str, int] = {}
            skipped_provider_codes: list[dict[str, str]] = []
            seen_provider_codes: set[str] = set()
            protected_provider_codes: list[str] = []
            for market in EXACT_FACTOR_REPAIR_MARKETS:
                try:
                    frame = self.market_ohlcv_loader(compact_date, market)
                    market_symbols, market_skipped_codes = (
                        _extract_exact_market_membership(
                            frame,
                            market=market,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    return self._result(
                        market_date=normalized_date,
                        apply=apply,
                        status='blocked',
                        reason='invalid_exact_market_membership',
                        failed_market=market,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
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
            ranking_membership_hash = market_membership_sha256(exact_symbols)
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
                'membership_basis': 'exact_market_date_ohlcv',
                'membership_sha256': membership_hash,
                'ranking_membership_sha256': ranking_membership_hash,
                'exact_provider_membership_count': len(full_provider_membership),
                'exact_membership_count': len(exact_symbols),
                'market_symbol_counts': market_symbol_counts,
                'provider_excluded_from_ranking_count': sum(
                    skipped_provider_code_counts_by_market.values()
                ),
                'provider_excluded_from_ranking_counts_by_market': (
                    skipped_provider_code_counts_by_market
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
            return self._result(
                market_date=normalized_date,
                apply=True,
                status='completed',
                stage_results=stage_results,
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
        help='Persist exact-membership factor results; default is dry-run.',
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = exact_factor_repair_service.repair_for_market_date(
        market_date=args.market_date,
        apply=bool(args.apply),
        prune_stale=bool(args.prune_stale),
        expected_provider_membership_count=(
            args.expected_provider_membership_count
        ),
        expected_membership_sha256=args.expected_membership_sha256,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get('status') in {'ready', 'completed'} else 1


if __name__ == '__main__':
    sys.exit(main())
