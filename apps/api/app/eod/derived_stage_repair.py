from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, suppress
from copy import deepcopy
from datetime import date
import json
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError

from app.agent_context.derived_contexts import daily_price_context_service
from app.dart.signals_service import AUDIT_SIGNAL_WRITER_LOCK_KEY
from app.db import session_scope
from app.db_models import (
    DailyActionSummary,
    DailyPipelineRun,
    DailyPriceContext,
    DailyTechnicalSnapshot,
    DartDisclosureSyncRun,
    DartFundamentalsCaptureRun,
    DartFundamentalsRefreshRun,
    EODCollectionRun,
    Symbol,
)
from app.eod.action_summaries import daily_action_summary_service
from app.eod.finality import validate_finalized_krx_write_date
from app.eod.market_membership import market_membership_sha256
from app.eod.price_bar_repair_state import _derived_output_fingerprint
from app.eod.technical_snapshots import daily_technical_snapshot_service
from app.internal.exact_repair_manifest import (
    canonical_json_sha256,
    load_checkpoint,
    load_json_mapping,
    write_checkpoint,
)
from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_recent_price_bars_by_symbol,
)
from app.repositories.eod_run_repository import advisory_eod_collection_locks


MANIFEST_SCHEMA_VERSION = 'derived_stage_exact_date_manifest_v1'
CHECKPOINT_SCHEMA_VERSION = 'derived_stage_exact_date_checkpoint_v1'
STAGE_ORDER = ('price_contexts', 'technical_snapshots', 'action_summaries')
REPAIR_SOURCE = 'exact_derived_stage_repair_v1'
_WRITER_RUN_MODELS = (
    DailyPipelineRun,
    EODCollectionRun,
    DartDisclosureSyncRun,
    DartFundamentalsCaptureRun,
    DartFundamentalsRefreshRun,
)
_WRITER_RUN_TABLES = tuple(model.__tablename__ for model in _WRITER_RUN_MODELS)
DERIVED_SOURCE_HISTORY_ROWS = 60
# Exact factor publication spans nine independently committed stages.  Keep
# its table/advisory fences bounded, but above the one-minute role default.
DERIVED_GUARD_IDLE_TRANSACTION_TIMEOUT_MS = 15 * 60 * 1_000
DERIVED_GUARD_IDLE_TRANSACTION_TIMEOUT_MAX_MS = 24 * 60 * 60 * 1_000
_STAGE_MODELS = {
    'price_contexts': DailyPriceContext,
    'technical_snapshots': DailyTechnicalSnapshot,
    'action_summaries': DailyActionSummary,
}


class GuardTransactionExitError(RuntimeError):
    """Interrupt a pending control transfer after a guard exit failure."""


@contextmanager
def capture_guard_transaction_failure(
    guard_factory: Callable[..., AbstractContextManager[Any]],
    *,
    state: dict[str, Any],
    **guard_kwargs: Any,
) -> Any:
    """Expose a post-yield fence transaction failure for reconciliation."""

    body_completed = False
    try:
        with guard_factory(**guard_kwargs) as guard:
            yield guard
            body_completed = True
    except SQLAlchemyError as exc:
        if not body_completed:
            raise
        state['error'] = exc
        raise GuardTransactionExitError from exc


def _normalize_symbol(value: Any) -> str:
    symbol = str(value or '').strip().upper()
    if len(symbol) != 6 or not symbol.isdigit():
        raise ValueError('manifest symbols must contain exactly six digits')
    return symbol


def normalize_manifest(raw: Mapping[str, Any]) -> dict[str, Any]:
    if raw.get('schema_version') != MANIFEST_SCHEMA_VERSION:
        raise ValueError(f'manifest schema_version must be {MANIFEST_SCHEMA_VERSION}')
    raw_dates = raw.get('dates')
    if not isinstance(raw_dates, list) or not raw_dates:
        raise ValueError('manifest dates must be a non-empty list')
    date_count = raw.get('date_count')
    if (
        isinstance(date_count, bool)
        or not isinstance(date_count, int)
        or date_count != len(raw_dates)
    ):
        raise ValueError('manifest date_count does not match dates')
    normalized_dates: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    for raw_item in raw_dates:
        if not isinstance(raw_item, Mapping):
            raise ValueError('every date manifest entry must be an object')
        market_date = date.fromisoformat(str(raw_item.get('market_date') or ''))
        iso_date = market_date.isoformat()
        if iso_date in seen_dates:
            raise ValueError('manifest contains duplicate market_date values')
        seen_dates.add(iso_date)
        raw_symbols = raw_item.get('symbols')
        if not isinstance(raw_symbols, list) or not raw_symbols:
            raise ValueError('every date manifest entry needs a non-empty symbols list')
        symbols = [_normalize_symbol(symbol) for symbol in raw_symbols]
        if symbols != sorted(symbols) or len(symbols) != len(set(symbols)):
            raise ValueError('date manifest symbols must be unique and sorted')
        symbol_count = raw_item.get('symbol_count')
        if (
            isinstance(symbol_count, bool)
            or not isinstance(symbol_count, int)
            or symbol_count != len(symbols)
        ):
            raise ValueError('date manifest symbol_count does not match symbols')
        membership_hash = market_membership_sha256(symbols)
        if raw_item.get('membership_sha256') != membership_hash:
            raise ValueError('date manifest membership_sha256 does not match symbols')
        normalized_dates.append(
            {
                'market_date': iso_date,
                'symbol_count': len(symbols),
                'membership_sha256': membership_hash,
                'symbols': symbols,
            }
        )
    if [item['market_date'] for item in normalized_dates] != sorted(seen_dates):
        raise ValueError('manifest dates must be sorted by market_date')
    return {
        'schema_version': MANIFEST_SCHEMA_VERSION,
        'date_count': len(normalized_dates),
        'stage_order': list(STAGE_ORDER),
        'dates': normalized_dates,
    }


@contextmanager
def exact_derived_stage_guard(
    *,
    market_date: date,
    lock_symbol_writes: bool = False,
) -> Any:
    if not (
        60_000 < DERIVED_GUARD_IDLE_TRANSACTION_TIMEOUT_MS
        <= DERIVED_GUARD_IDLE_TRANSACTION_TIMEOUT_MAX_MS
    ):
        raise ValueError('derived guard idle transaction budget is invalid')
    with advisory_eod_collection_locks(
        market='KRX',
        trade_dates=(market_date.isoformat(),),
        source='daily_price_bars',
        exclusive_admission=True,
    ) as (lock_acquired, _blocked_date):
        if not lock_acquired:
            yield False, 'daily_eod_advisory_lock_unavailable'
            return
        guard_yielded = False
        try:
            with session_scope(
                statement_timeout_ms=5_000,
                lock_timeout_ms=1,
                isolated_connection=True,
            ) as session:
                postgresql = session.get_bind().dialect.name == 'postgresql'
                if postgresql:
                    session.execute(
                        text(
                            'SET LOCAL idle_in_transaction_session_timeout '
                            f'= {DERIVED_GUARD_IDLE_TRANSACTION_TIMEOUT_MS}'
                        )
                    )
                    session.execute(
                        text(
                            'LOCK TABLE '
                            f'{", ".join(_WRITER_RUN_TABLES)} '
                            'IN SHARE ROW EXCLUSIVE MODE'
                        )
                    )
                    session.execute(
                        text(
                            'SELECT pg_advisory_xact_lock('
                            'hashtext(:dart_signal_writer_lock_key))'
                        ),
                        {
                            'dart_signal_writer_lock_key': (
                                AUDIT_SIGNAL_WRITER_LOCK_KEY
                            )
                        },
                    )
                    if lock_symbol_writes:
                        # Factor outputs read live symbol metadata and the
                        # persistence helper may otherwise race master-data
                        # refreshes between per-stage commits.  Factor repair
                        # rejects placeholder names before compute, so its own
                        # nested persistence never needs a Symbol write.
                        session.execute(text('LOCK TABLE symbols IN SHARE MODE'))
                active_writer = any(
                    session.execute(
                        select(func.count())
                        .select_from(model)
                        .where(model.status == 'running')
                    ).scalar_one()
                    for model in _WRITER_RUN_MODELS
                )
                if active_writer:
                    guard_yielded = True
                    yield False, 'active_daily_or_eod_writer'
                    return
                guard_yielded = True
                yield True, None
        except SQLAlchemyError:
            if guard_yielded:
                raise
            yield False, 'writer_table_lock_unavailable'


def _success_symbols(result: Mapping[str, Any]) -> list[str]:
    raw_successes = result.get('successful_stock_codes')
    if isinstance(raw_successes, list):
        return sorted({_normalize_symbol(symbol) for symbol in raw_successes})
    items = result.get('items')
    if not isinstance(items, list):
        return []
    return sorted(
        {
            _normalize_symbol(item.get('symbol'))
            for item in items
            if isinstance(item, Mapping)
        }
    )


def _load_derived_completion_state(
    market_date: str,
    exact_symbols: Sequence[str],
) -> dict[str, Any]:
    parsed_date = date.fromisoformat(market_date)
    symbols = sorted({_normalize_symbol(symbol) for symbol in exact_symbols})
    bars_by_symbol = load_recent_price_bars_by_symbol(
        market_date=parsed_date,
        symbols=symbols,
        history_days=DERIVED_SOURCE_HISTORY_ROWS,
    )
    source_payload = [
        [symbol, list(bars_by_symbol.get(symbol) or [])]
        for symbol in symbols
    ]
    source_fingerprint = {
        'history_row_limit': DERIVED_SOURCE_HISTORY_ROWS,
        'row_count': sum(len(rows) for _symbol, rows in source_payload),
        'source_sha256': canonical_json_sha256(source_payload),
    }
    memberships: dict[str, list[str]] = {}
    output_fingerprints: dict[str, dict[str, Any]] = {}
    with session_scope(statement_timeout_ms=10_000) as session:
        for stage_id, model in _STAGE_MODELS.items():
            memberships[stage_id] = list(
                session.execute(
                    select(Symbol.symbol)
                    .join(model, model.symbol_id == Symbol.id)
                    .where(model.market_date == parsed_date)
                    .order_by(Symbol.symbol.asc())
                ).scalars()
            )
            output_sha256, row_count = _derived_output_fingerprint(
                session,
                model=model,
                market_date=parsed_date,
                symbols=symbols,
            )
            output_fingerprints[stage_id] = {
                'row_count': row_count,
                'output_sha256': output_sha256,
            }
    return {
        'source_fingerprint': source_fingerprint,
        'memberships': memberships,
        'output_fingerprints': output_fingerprints,
    }


class ExactDerivedStageRepair:
    def __init__(
        self,
        manifest: Mapping[str, Any],
        *,
        price_runner: Callable[..., Mapping[str, Any]] | None = None,
        technical_runner: Callable[..., Mapping[str, Any]] | None = None,
        action_runner: Callable[..., Mapping[str, Any]] | None = None,
        stored_symbols_loader: Callable[[str], Sequence[str]] | None = None,
        completion_state_loader: Callable[
            [str, Sequence[str]], Mapping[str, Any]
        ]
        | None = None,
        guard_factory: Callable[..., AbstractContextManager[Any]] | None = None,
        finality_validator: Callable[[date], None] | None = None,
    ) -> None:
        self.manifest = normalize_manifest(manifest)
        self.manifest_sha256 = canonical_json_sha256(self.manifest)
        self.price_runner = (
            price_runner or daily_price_context_service.rebuild_for_market_date
        )
        self.technical_runner = (
            technical_runner
            or daily_technical_snapshot_service.rebuild_for_market_date
        )
        self.action_runner = (
            action_runner or daily_action_summary_service.rebuild_for_market_date
        )
        self.stored_symbols_loader = (
            stored_symbols_loader or list_symbols_with_daily_bar_on_date
        )
        self.completion_state_loader = (
            completion_state_loader or _load_derived_completion_state
        )
        self.guard_factory = guard_factory or exact_derived_stage_guard
        self.finality_validator = finality_validator or (
            lambda market_date: validate_finalized_krx_write_date(
                market_date,
                operation='exact stored-data derived-stage repair',
            )
        )

    def _validate_stored_scope(self, item: Mapping[str, Any]) -> None:
        expected = list(item['symbols'])
        observed = sorted(
            {
                _normalize_symbol(symbol)
                for symbol in self.stored_symbols_loader(str(item['market_date']))
            }
        )
        if observed != expected:
            missing = sorted(set(expected) - set(observed))
            unexpected = sorted(set(observed) - set(expected))
            raise ValueError(
                'stored daily-price-bar membership drift: '
                f'missing={missing[:20]}, unexpected={unexpected[:20]}'
            )

    @staticmethod
    def _validated_stage_finalization(
        *,
        result: Mapping[str, Any],
        market_date: str,
        stage_id: str,
        exact_symbols: Sequence[str],
        successful_symbols: Sequence[str],
    ) -> Mapping[str, Any]:
        finalization = result.get('scope_finalization')
        if not isinstance(finalization, Mapping):
            raise RuntimeError(
                f'{stage_id} did not return atomic exact-scope finalization'
            )
        expected = {
            'stage': stage_id,
            'market_date': market_date,
            'exact_symbol_count': len(exact_symbols),
            'exact_membership_sha256': market_membership_sha256(exact_symbols),
            'successful_symbol_count': len(successful_symbols),
            'successful_membership_sha256': market_membership_sha256(
                successful_symbols
            ),
            'ready': True,
        }
        mismatches = [
            field
            for field, expected_value in expected.items()
            if finalization.get(field) != expected_value
        ]
        if mismatches:
            raise RuntimeError(
                f'{stage_id} exact-scope finalization mismatch: '
                + ','.join(mismatches)
            )
        return finalization

    @staticmethod
    def _valid_source_fingerprint(value: Any) -> bool:
        if not isinstance(value, Mapping):
            return False
        row_count = value.get('row_count')
        source_sha256 = value.get('source_sha256')
        return (
            value.get('history_row_limit') == DERIVED_SOURCE_HISTORY_ROWS
            and isinstance(row_count, int)
            and not isinstance(row_count, bool)
            and row_count > 0
            and isinstance(source_sha256, str)
            and len(source_sha256) == 64
        )

    @classmethod
    def _validated_completion_evidence(
        cls,
        state: Mapping[str, Any],
        item: Mapping[str, Any],
        *,
        require_complete: bool,
    ) -> dict[str, Any]:
        source_fingerprint = state.get('source_fingerprint')
        memberships = state.get('memberships')
        output_fingerprints = state.get('output_fingerprints')
        if (
            not cls._valid_source_fingerprint(source_fingerprint)
            or not isinstance(memberships, Mapping)
            or not isinstance(output_fingerprints, Mapping)
            or set(memberships) != set(STAGE_ORDER)
            or set(output_fingerprints) != set(STAGE_ORDER)
        ):
            raise RuntimeError('derived completion fingerprints are incomplete')
        expected_symbols = list(item['symbols'])
        validated_outputs: dict[str, dict[str, Any]] = {}
        for stage_id in STAGE_ORDER:
            raw_membership = memberships.get(stage_id)
            raw_fingerprint = output_fingerprints.get(stage_id)
            if (
                not isinstance(raw_membership, Sequence)
                or isinstance(raw_membership, (str, bytes))
                or not isinstance(raw_fingerprint, Mapping)
            ):
                raise RuntimeError('derived completion fingerprint is invalid')
            membership = sorted(
                {_normalize_symbol(symbol) for symbol in raw_membership}
            )
            row_count = raw_fingerprint.get('row_count')
            output_sha256 = raw_fingerprint.get('output_sha256')
            if (
                len(membership) != len(raw_membership)
                or isinstance(row_count, bool)
                or not isinstance(row_count, int)
                or row_count != len(membership)
                or not isinstance(output_sha256, str)
                or len(output_sha256) != 64
            ):
                raise RuntimeError('derived completion fingerprint is invalid')
            if require_complete and membership != expected_symbols:
                raise RuntimeError('derived completion membership is incomplete')
            validated_outputs[stage_id] = {
                'row_count': row_count,
                'output_sha256': output_sha256,
            }
        return {
            'source_fingerprint': dict(source_fingerprint),
            'output_fingerprints': validated_outputs,
        }

    def _run_date(self, item: Mapping[str, Any]) -> dict[str, Any]:
        market_date = str(item['market_date'])
        exact_symbols = list(item['symbols'])
        self._validate_stored_scope(item)
        initial_state = self.completion_state_loader(
            market_date,
            exact_symbols,
        )
        initial_source = initial_state.get('source_fingerprint')
        if not self._valid_source_fingerprint(initial_source):
            raise RuntimeError('derived source fingerprint is unavailable')

        price_result = dict(
            self.price_runner(
                market_date=market_date,
                symbols=exact_symbols,
                exact_scope_symbols=exact_symbols,
                source=REPAIR_SOURCE,
                include_retry_metadata=True,
            )
        )
        price_successes = _success_symbols(price_result)
        price_finalization = self._validated_stage_finalization(
            result=price_result,
            market_date=market_date,
            stage_id='price_contexts',
            exact_symbols=exact_symbols,
            successful_symbols=price_successes,
        )

        technical_result = dict(
            self.technical_runner(
                market_date=market_date,
                symbols=price_successes,
                exact_scope_symbols=exact_symbols,
                source=REPAIR_SOURCE,
                include_retry_metadata=True,
            )
        )
        technical_successes = _success_symbols(technical_result)
        technical_finalization = self._validated_stage_finalization(
            result=technical_result,
            market_date=market_date,
            stage_id='technical_snapshots',
            exact_symbols=exact_symbols,
            successful_symbols=technical_successes,
        )

        action_result = dict(
            self.action_runner(
                market_date=market_date,
                symbols=technical_successes,
                exact_scope_symbols=exact_symbols,
                source=REPAIR_SOURCE,
                include_retry_metadata=True,
            )
        )
        action_successes = _success_symbols(action_result)
        action_finalization = self._validated_stage_finalization(
            result=action_result,
            market_date=market_date,
            stage_id='action_summaries',
            exact_symbols=exact_symbols,
            successful_symbols=action_successes,
        )

        success_sets = {
            'price_contexts': price_successes,
            'technical_snapshots': technical_successes,
            'action_summaries': action_successes,
        }
        retry_debt = {
            stage_id: sorted(set(exact_symbols) - set(successes))
            for stage_id, successes in success_sets.items()
        }
        completed = all(not symbols for symbols in retry_debt.values())
        final_state = self.completion_state_loader(
            market_date,
            exact_symbols,
        )
        if final_state.get('source_fingerprint') != initial_source:
            raise RuntimeError('derived source dependency changed during repair')
        completion_evidence = self._validated_completion_evidence(
            final_state,
            item,
            require_complete=completed,
        )
        return {
            'market_date': market_date,
            'status': 'completed' if completed else 'partial',
            'exact_symbol_count': len(exact_symbols),
            'membership_sha256': item['membership_sha256'],
            'successful_symbols': success_sets,
            'retry_debt_symbols': retry_debt,
            'completion_evidence': completion_evidence,
            'stage_results': {
                'price_contexts': {
                    'result': price_result,
                    'exact_finalization': dict(price_finalization),
                },
                'technical_snapshots': {
                    'result': technical_result,
                    'exact_finalization': dict(technical_finalization),
                },
                'action_summaries': {
                    'result': action_result,
                    'exact_finalization': dict(action_finalization),
                },
            },
        }

    @classmethod
    def _completed_date_checkpoint_is_valid(
        cls,
        checkpoint_unit: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> bool:
        result = checkpoint_unit.get('result')
        if not isinstance(result, Mapping) or result.get('status') != 'completed':
            return False
        if (
            result.get('market_date') != item['market_date']
            or result.get('exact_symbol_count') != item['symbol_count']
            or result.get('membership_sha256') != item['membership_sha256']
        ):
            return False
        successes = result.get('successful_symbols')
        retry_debt = result.get('retry_debt_symbols')
        if not isinstance(successes, Mapping) or not isinstance(retry_debt, Mapping):
            return False
        exact_symbols = list(item['symbols'])
        if not all(
            successes.get(stage_id) == exact_symbols
            and retry_debt.get(stage_id) == []
            for stage_id in STAGE_ORDER
        ):
            return False
        stage_results = result.get('stage_results')
        if not isinstance(stage_results, Mapping):
            return False
        expected_finalization = {
            'market_date': item['market_date'],
            'exact_symbol_count': item['symbol_count'],
            'exact_membership_sha256': item['membership_sha256'],
            'successful_symbol_count': item['symbol_count'],
            'successful_membership_sha256': item['membership_sha256'],
            'ready': True,
        }
        for stage_id in STAGE_ORDER:
            stage_result = stage_results.get(stage_id)
            if not isinstance(stage_result, Mapping):
                return False
            finalization = stage_result.get('exact_finalization')
            nested_result = stage_result.get('result')
            if (
                not isinstance(finalization, Mapping)
                or not isinstance(nested_result, Mapping)
                or nested_result.get('scope_finalization') != finalization
                or finalization.get('stage') != stage_id
            ):
                return False
            if any(
                finalization.get(field) != expected_value
                for field, expected_value in expected_finalization.items()
            ):
                return False
        completion_evidence = result.get('completion_evidence')
        if not isinstance(completion_evidence, Mapping):
            return False
        try:
            cls._validated_completion_evidence(
                {
                    'source_fingerprint': completion_evidence.get(
                        'source_fingerprint'
                    ),
                    'memberships': {
                        stage_id: list(item['symbols'])
                        for stage_id in STAGE_ORDER
                    },
                    'output_fingerprints': completion_evidence.get(
                        'output_fingerprints'
                    ),
                },
                item,
                require_complete=True,
            )
        except RuntimeError:
            return False
        return True

    def _completed_date_live_state_is_valid(
        self,
        checkpoint_unit: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> bool:
        self._validate_stored_scope(item)
        live = self.completion_state_loader(
            str(item['market_date']),
            list(item['symbols']),
        )
        if not isinstance(live, Mapping):
            return False
        try:
            live_evidence = self._validated_completion_evidence(
                live,
                item,
                require_complete=True,
            )
        except RuntimeError:
            return False
        expected = checkpoint_unit['result'].get('completion_evidence')
        return isinstance(expected, Mapping) and live_evidence == dict(expected)

    def _reconcile_applied_result(
        self,
        *,
        item: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> tuple[bool, dict[str, Any]]:
        """Re-read durable exact rows after a fence/checkpoint uncertainty."""

        reconciled_unit = {'result': dict(result)}
        try:
            live_valid = bool(
                self._completed_date_checkpoint_is_valid(
                    reconciled_unit,
                    item,
                )
                and self._completed_date_live_state_is_valid(
                    reconciled_unit,
                    item,
                )
            )
        except Exception as exc:  # noqa: BLE001
            return False, {
                'error_type': type(exc).__name__,
                'error_message': str(exc)[:500],
            }
        completion = result.get('completion_evidence')
        return live_valid, {
            'completion_evidence': (
                dict(completion)
                if isinstance(completion, Mapping)
                else None
            )
        }

    def run(
        self,
        *,
        apply: bool = False,
        expected_manifest_sha256: str | None = None,
        checkpoint_path: str | Path | None = None,
    ) -> dict[str, Any]:
        base = {
            'schema_version': MANIFEST_SCHEMA_VERSION,
            'manifest_sha256': self.manifest_sha256,
            'date_count': self.manifest['date_count'],
            'stage_order': list(STAGE_ORDER),
            'mode': 'apply' if apply else 'dry_run',
        }
        if not apply:
            errors: list[dict[str, str]] = []
            for item in self.manifest['dates']:
                try:
                    self._validate_stored_scope(item)
                except ValueError as exc:
                    errors.append(
                        {
                            'market_date': item['market_date'],
                            'error': str(exc),
                        }
                    )
            return {
                **base,
                'status': 'dry_run' if not errors else 'blocked',
                'applied': False,
                'validated_date_count': self.manifest['date_count'] - len(errors),
                'scope_errors': errors,
            }

        expected_hash = str(expected_manifest_sha256 or '').strip().lower()
        if not expected_hash:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'expected_manifest_sha256_required',
            }
        if expected_hash != self.manifest_sha256:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'expected_manifest_sha256_mismatch',
            }
        if checkpoint_path is None:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_path_required',
            }
        try:
            checkpoint = load_checkpoint(
                checkpoint_path,
                schema_version=CHECKPOINT_SCHEMA_VERSION,
                manifest_sha256=self.manifest_sha256,
                item_count=self.manifest['date_count'],
                unit_size=1,
            )
        except ValueError as exc:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_manifest_mismatch',
                'error_message': str(exc),
            }

        expected_dates = {
            str(item['market_date']) for item in self.manifest['dates']
        }
        if set(checkpoint['units']) - expected_dates:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_date_set_mismatch',
            }
        completed_dates = 0
        skipped_dates = 0
        failed_dates = 0
        for item in self.manifest['dates']:
            market_date = str(item['market_date'])
            item_sha256 = canonical_json_sha256(item)
            previous = checkpoint['units'].get(market_date)
            completed_checkpoint: Mapping[str, Any] | None = None
            if isinstance(previous, Mapping):
                if previous.get('item_sha256') != item_sha256:
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': 'checkpoint_date_hash_mismatch',
                        'market_date': market_date,
                    }
                if previous.get('status') == 'completed':
                    if not self._completed_date_checkpoint_is_valid(
                        previous,
                        item,
                    ):
                        return {
                            **base,
                            'status': 'blocked',
                            'applied': False,
                            'reason': 'checkpoint_completed_date_invalid',
                            'market_date': market_date,
                        }
                    completed_checkpoint = previous
            try:
                self.finality_validator(date.fromisoformat(market_date))
            except (TypeError, ValueError) as exc:
                checkpoint['units'][market_date] = {
                    'status': 'failed',
                    'item_sha256': item_sha256,
                    'reason': 'market_date_not_finalized',
                    'error_message': str(exc),
                }
                write_checkpoint(checkpoint_path, checkpoint)
                failed_dates += 1
                continue

            result: dict[str, Any] | None = None
            guard_exit_state: dict[str, Any] = {}
            with suppress(
                GuardTransactionExitError
            ), capture_guard_transaction_failure(
                self.guard_factory,
                state=guard_exit_state,
                market_date=date.fromisoformat(market_date),
            ) as guard:
                acquired, reason = guard
                if not acquired:
                    checkpoint['units'][market_date] = {
                        'status': 'blocked',
                        'item_sha256': item_sha256,
                        'reason': str(reason or 'active_writer'),
                    }
                    write_checkpoint(checkpoint_path, checkpoint)
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': str(reason or 'active_writer'),
                        'market_date': market_date,
                        'completed_dates': completed_dates,
                        'skipped_dates': skipped_dates,
                    }
                if completed_checkpoint is not None:
                    try:
                        live_state_valid = self._completed_date_live_state_is_valid(
                            completed_checkpoint,
                            item
                        )
                    except ValueError:
                        live_state_valid = False
                    except Exception as exc:  # noqa: BLE001
                        return {
                            **base,
                            'status': 'blocked',
                            'applied': False,
                            'reason': 'checkpoint_live_revalidation_failed',
                            'market_date': market_date,
                            'error_type': type(exc).__name__,
                            'error_message': str(exc)[:500],
                        }
                    if live_state_valid:
                        completed_dates += 1
                        skipped_dates += 1
                        continue
                try:
                    result = self._run_date(item)
                except Exception as exc:  # noqa: BLE001
                    result = {
                        'market_date': market_date,
                        'status': 'failed',
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }

            guard_exit_error = guard_exit_state.get('error')
            if isinstance(guard_exit_error, Exception):
                live_valid = False
                reconciliation: dict[str, Any] = {}
                if result is not None:
                    live_valid, reconciliation = (
                        self._reconcile_applied_result(
                            item=item,
                            result=result,
                        )
                    )
                uncertain_attempt = {
                    'reason': 'guard_transaction_exit_failed',
                    'error_type': type(guard_exit_error).__name__,
                    'error_message': str(guard_exit_error)[:500],
                    'result_status': (
                        result.get('status')
                        if isinstance(result, Mapping)
                        else None
                    ),
                    'live_completion_verified': live_valid,
                    'reconciliation': reconciliation,
                }
                uncertain_unit = (
                    dict(previous)
                    if isinstance(previous, Mapping)
                    else {
                        'status': 'uncertain',
                        'item_sha256': item_sha256,
                    }
                )
                uncertain_unit['last_uncertain_attempt'] = uncertain_attempt
                checkpoint['units'][market_date] = uncertain_unit
                checkpoint_error: Exception | None = None
                try:
                    write_checkpoint(checkpoint_path, checkpoint)
                except Exception as exc:  # noqa: BLE001
                    checkpoint_error = exc
                response = {
                    **base,
                    'status': 'partial' if live_valid else 'blocked',
                    'applied': False,
                    'reason': 'guard_transaction_exit_failed',
                    'market_date': market_date,
                    'error_type': type(guard_exit_error).__name__,
                    'error_message': str(guard_exit_error)[:500],
                    'uncertain_commit': True,
                    'checkpoint_persisted': checkpoint_error is None,
                    'reconciled_proof_count': int(live_valid),
                    'retry_required': True,
                    'reconciliation': reconciliation,
                }
                if checkpoint_error is not None:
                    response.update(
                        {
                            'checkpoint_error_type': type(
                                checkpoint_error
                            ).__name__,
                            'checkpoint_error_message': str(
                                checkpoint_error
                            )[:500],
                        }
                    )
                return response
            if result is None:
                return {
                    **base,
                    'status': 'blocked',
                    'applied': False,
                    'reason': 'derived_apply_result_missing',
                    'market_date': market_date,
                }

            checkpoint['units'][market_date] = {
                'status': result['status'],
                'item_sha256': item_sha256,
                'result': result,
            }
            try:
                write_checkpoint(checkpoint_path, checkpoint)
            except Exception as checkpoint_error:  # noqa: BLE001
                live_valid, reconciliation = self._reconcile_applied_result(
                    item=item,
                    result=result,
                )
                return {
                    **base,
                    'status': 'partial' if live_valid else 'blocked',
                    'applied': False,
                    'reason': 'checkpoint_write_failed_after_commit',
                    'market_date': market_date,
                    'error_type': type(checkpoint_error).__name__,
                    'error_message': str(checkpoint_error)[:500],
                    'uncertain_commit': True,
                    'checkpoint_persisted': False,
                    'reconciled_proof_count': int(live_valid),
                    'retry_required': True,
                    'reconciliation': reconciliation,
                }
            if result['status'] == 'completed':
                completed_dates += 1
            else:
                failed_dates += 1

        status = 'completed' if failed_dates == 0 else 'partial'
        return {
            **base,
            'status': status,
            'applied': status == 'completed',
            'completed_dates': completed_dates,
            'skipped_dates': skipped_dates,
            'failed_dates': failed_dates,
            'retry_required': failed_dates > 0,
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Rebuild exact stored-data price contexts, technical snapshots, '
            'and action summaries without invoking a daily pipeline.'
        )
    )
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--checkpoint')
    parser.add_argument('--expected-manifest-sha256')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--apply', action='store_true')
    mode.add_argument('--dry-run', action='store_true')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.apply and args.checkpoint and (
        Path(args.manifest).resolve() == Path(args.checkpoint).resolve()
    ):
        raise ValueError('manifest and checkpoint paths must be different')
    manifest = load_json_mapping(args.manifest, label='manifest')
    repair = ExactDerivedStageRepair(deepcopy(manifest))
    result = repair.run(
        apply=bool(args.apply),
        expected_manifest_sha256=args.expected_manifest_sha256,
        checkpoint_path=args.checkpoint,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result['status'] in {'dry_run', 'completed'} else 2


if __name__ == '__main__':
    raise SystemExit(main())
