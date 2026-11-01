from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, suppress
from copy import deepcopy
from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any

from sqlalchemy import delete, select, text
from app.db import session_scope
from app.db_models import (
    DailyMarketRiskFilterResult,
    DailySignalFeature,
    Symbol,
)
from app.eod.derived_stage_repair import (
    GuardTransactionExitError,
    capture_guard_transaction_failure,
    exact_derived_stage_guard,
    normalize_manifest,
)
from app.eod.finality import validate_finalized_krx_write_date
from app.eod.market_membership import market_membership_sha256
from app.eod.price_bar_repair_state import (
    REQUIRED_EXACT_FACTOR_MODELS,
    _derived_output_fingerprint,
    _stable_output_value,
    load_pending_price_bar_rebuild_batches,
    resolve_pending_frozen_price_dependent_membership,
)
from app.internal.exact_repair_manifest import (
    canonical_json_sha256,
    checkpoint_file_lock,
    load_checkpoint,
    load_json_mapping,
    write_checkpoint,
)
from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_recent_price_bars_by_symbol,
)
from app.signals.daily_signal_features import daily_signal_feature_service


CHECKPOINT_SCHEMA_VERSION = 'signal_feature_exact_date_checkpoint_v1'
IMPLEMENTATION_CONTRACT = 'signal_feature_exact_date_implementation_v2'
REPAIR_SOURCE = 'stored_owned_fallback_exact_scope'
PRICE_HISTORY_ROWS = 2

_EXACT_FACTOR_SOURCE_MODELS = {
    component: model
    for component, model in REQUIRED_EXACT_FACTOR_MODELS
    if component != 'piotroski_filter'
}
_FULL_DATE_UNIVERSE_SOURCE_MODELS = {
    'piotroski_filter': dict(REQUIRED_EXACT_FACTOR_MODELS)[
        'piotroski_filter'
    ],
    'market_risk_filter': DailyMarketRiskFilterResult,
}
SOURCE_MODELS = {
    **_EXACT_FACTOR_SOURCE_MODELS,
    **_FULL_DATE_UNIVERSE_SOURCE_MODELS,
}
SOURCE_COMPONENTS = frozenset(
    {
        'direct_signal_evidence',
        'price_history',
        'symbol_metadata',
        *SOURCE_MODELS,
    }
)


def _normalize_symbols(values: Sequence[str]) -> list[str]:
    symbols = sorted(
        {
            str(value).strip().upper()
            for value in values
            if str(value).strip()
        }
    )
    if any(len(symbol) != 6 or not symbol.isdigit() for symbol in symbols):
        raise ValueError('signal-feature repair scope contains an invalid symbol')
    return symbols


def _fingerprint_payload(
    payload: Any,
    *,
    row_count: int | None = None,
) -> dict[str, Any]:
    normalized = _stable_output_value(payload)
    resolved_count = (
        row_count
        if row_count is not None
        else len(normalized)
        if isinstance(normalized, list)
        else 1
    )
    return {
        'row_count': resolved_count,
        'source_sha256': canonical_json_sha256(normalized),
    }


def _model_source_fingerprint(
    session: Any,
    *,
    model: Any,
    market_date: date,
    symbols: Sequence[str] | None,
    required_symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    excluded_columns = {'id', 'symbol_id', 'created_at', 'updated_at'}
    column_names = tuple(
        column.name
        for column in model.__table__.columns
        if column.name not in excluded_columns
    )
    statement = (
        select(Symbol.symbol, *(getattr(model, name) for name in column_names))
        .join(model, model.symbol_id == Symbol.id)
        .where(model.market_date == market_date)
        .order_by(Symbol.symbol.asc())
    )
    if symbols is not None:
        statement = statement.where(Symbol.symbol.in_(symbols))
    rows = session.execute(statement).all()
    payload = [
        [
            str(row[0]),
            {
                name: _stable_output_value(value)
                for name, value in zip(
                    column_names,
                    row[1:],
                    strict=True,
                )
            },
        ]
        for row in rows
    ]
    fingerprint = _fingerprint_payload(payload)
    if required_symbols is not None:
        required = _normalize_symbols(required_symbols)
        required_set = set(required)
        observed_required = sorted(
            str(row[0])
            for row in rows
            if str(row[0]) in required_set
        )
        fingerprint.update(
            {
                'exact_scope_row_count': len(observed_required),
                'exact_scope_membership_sha256': (
                    market_membership_sha256(observed_required)
                ),
            }
        )
    return fingerprint


def _preserved_direct_signal_payload(
    row: Any,
    *,
    symbol: str,
) -> dict[str, Any] | None:
    payload = deepcopy(dict(row.feature_payload or {}))
    sources = deepcopy(payload.get('sources') or {})
    fallback = payload.get('fallback_features') or {}
    quote_snapshot = (
        deepcopy(fallback.get('quote_snapshot'))
        if isinstance(fallback, Mapping)
        else None
    )
    direct_values = (
        int(row.signal_count or 0),
        row.volume_power_rank,
        row.volume_power,
        row.overtime_volume_rank,
        row.overtime_volume,
        row.overtime_fluctuation_rank,
        row.overtime_fluctuation_rate,
    )
    collection = deepcopy(payload.get('collection') or {})
    if (
        not sources
        and not quote_snapshot
        and not any(direct_values)
        and collection.get('repair_mode')
        == 'stored_owned_fallback_exact_scope'
    ):
        # A row materialized by this repair has no preserved provider input.
        # Excluding it keeps the proof stable across the first rebuild while
        # older fallback-only rows still bind their preserved row metadata.
        return None

    if isinstance(collection, dict):
        collection.pop('universe', None)
    for owned_key in (
        'feature_version',
        'row_provenance',
        'freshness',
        'fallback_features',
    ):
        payload.pop(owned_key, None)
    payload['sources'] = sources
    payload['collection'] = collection
    if quote_snapshot:
        payload['preserved_quote_snapshot'] = quote_snapshot
    return {
        'symbol': symbol,
        'source': str(row.source),
        'signal_count': direct_values[0],
        'volume_power_rank': direct_values[1],
        'volume_power': _stable_output_value(direct_values[2]),
        'overtime_volume_rank': direct_values[3],
        'overtime_volume': direct_values[4],
        'overtime_fluctuation_rank': direct_values[5],
        'overtime_fluctuation_rate': _stable_output_value(direct_values[6]),
        'preserved_payload': _stable_output_value(payload),
    }


def _load_source_fingerprints(
    market_date: str,
    exact_symbols: Sequence[str],
) -> dict[str, dict[str, Any]]:
    parsed_date = date.fromisoformat(market_date)
    symbols = _normalize_symbols(exact_symbols)
    bars_by_symbol = load_recent_price_bars_by_symbol(
        market_date=parsed_date,
        symbols=symbols,
        history_days=PRICE_HISTORY_ROWS,
    )
    price_payload = [
        [symbol, list(bars_by_symbol.get(symbol) or [])]
        for symbol in symbols
    ]
    fingerprints = {
        'price_history': {
            **_fingerprint_payload(
                price_payload,
                row_count=sum(len(rows) for _symbol, rows in price_payload),
            ),
            'history_row_limit': PRICE_HISTORY_ROWS,
        }
    }

    with session_scope(statement_timeout_ms=30_000) as session:
        symbol_rows = session.execute(
            select(Symbol)
            .where(Symbol.symbol.in_(symbols))
            .order_by(Symbol.symbol.asc())
        ).scalars().all()
        symbol_payload = [
            {
                'symbol': str(row.symbol),
                'name_ko': str(row.name_ko or ''),
                'market': str(row.market or ''),
                'is_active': bool(row.is_active),
            }
            for row in symbol_rows
        ]
        fingerprints['symbol_metadata'] = {
            **_fingerprint_payload(symbol_payload),
            'scope_symbol_count': len(symbols),
            'placeholder_name_count': sum(
                not str(row.name_ko or '').strip()
                or str(row.name_ko).strip() == str(row.symbol)
                for row in symbol_rows
            ),
        }

        direct_rows = session.execute(
            select(
                DailySignalFeature,
                Symbol.symbol.label('symbol'),
            )
            .join(Symbol, Symbol.id == DailySignalFeature.symbol_id)
            .where(
                DailySignalFeature.market_date == parsed_date,
                Symbol.symbol.in_(symbols),
            )
            .order_by(Symbol.symbol.asc())
        ).all()
        direct_payload: list[dict[str, Any]] = []
        for feature, symbol in direct_rows:
            preserved = _preserved_direct_signal_payload(
                feature,
                symbol=str(symbol),
            )
            if preserved is not None:
                direct_payload.append(preserved)
        fingerprints['direct_signal_evidence'] = _fingerprint_payload(
            direct_payload
        )

        for component, model in _EXACT_FACTOR_SOURCE_MODELS.items():
            fingerprints[component] = _model_source_fingerprint(
                session,
                model=model,
                market_date=parsed_date,
                symbols=symbols,
            )
        for component, model in _FULL_DATE_UNIVERSE_SOURCE_MODELS.items():
            fingerprints[component] = _model_source_fingerprint(
                session,
                model=model,
                market_date=parsed_date,
                symbols=None,
                required_symbols=symbols,
            )
    return fingerprints


def _signal_rows_fingerprint(rows: Sequence[Any]) -> dict[str, Any]:
    payload = [
        [
            str(row.symbol),
            str(row.source),
            int(row.signal_count),
            _stable_output_value(row.volume_power_rank),
            _stable_output_value(row.volume_power),
            _stable_output_value(row.overtime_volume_rank),
            _stable_output_value(row.overtime_volume),
            _stable_output_value(row.overtime_fluctuation_rank),
            _stable_output_value(row.overtime_fluctuation_rate),
            _stable_output_value(row.feature_payload or {}),
        ]
        for row in rows
    ]
    return {
        'row_count': len(rows),
        'membership_sha256': market_membership_sha256(
            [str(row.symbol) for row in rows]
        ),
        'output_sha256': canonical_json_sha256(payload),
    }


def _load_completion_state(
    market_date: str,
    exact_symbols: Sequence[str],
) -> dict[str, Any]:
    parsed_date = date.fromisoformat(market_date)
    symbols = _normalize_symbols(exact_symbols)
    exact_set = set(symbols)
    with session_scope(statement_timeout_ms=30_000) as session:
        rows = session.execute(
            select(
                DailySignalFeature,
                Symbol.symbol.label('symbol'),
            )
            .join(Symbol, Symbol.id == DailySignalFeature.symbol_id)
            .where(DailySignalFeature.market_date == parsed_date)
            .order_by(Symbol.symbol.asc())
        ).all()
        normalized_rows = []
        for feature, symbol in rows:
            feature.symbol = symbol
            normalized_rows.append(feature)
        output_membership = [str(row.symbol) for row in normalized_rows]
        stale_rows = [
            row for row in normalized_rows if str(row.symbol) not in exact_set
        ]
        stale_extras = _signal_rows_fingerprint(stale_rows)
        output_sha256, output_count = _derived_output_fingerprint(
            session,
            model=DailySignalFeature,
            market_date=parsed_date,
            symbols=None,
        )
    return {
        'source_fingerprints': _load_source_fingerprints(
            market_date,
            symbols,
        ),
        'output_membership': output_membership,
        'output_fingerprint': {
            'row_count': output_count,
            'output_sha256': output_sha256,
        },
        'stale_extras': stale_extras,
    }


def verify_exact_signal_feature_rows(
    *,
    market_date: date,
    exact_symbols: Sequence[str],
    repair_started_at: datetime,
) -> dict[str, Any]:
    symbols = _normalize_symbols(exact_symbols)
    with session_scope(statement_timeout_ms=30_000) as session:
        rows = session.execute(
            select(Symbol.symbol, DailySignalFeature.updated_at)
            .join(
                DailySignalFeature,
                DailySignalFeature.symbol_id == Symbol.id,
            )
            .where(
                DailySignalFeature.market_date == market_date,
                Symbol.symbol.in_(symbols),
            )
            .order_by(Symbol.symbol.asc())
        ).all()
    observed = [str(row[0]) for row in rows]
    fresh = [
        str(row[0])
        for row in rows
        if row[1] is not None and row[1] >= repair_started_at
    ]
    if observed != symbols or fresh != symbols:
        raise RuntimeError(
            'signal-feature exact persistence verification failed: '
            f'observed={len(observed)}, fresh={len(fresh)}, '
            f'expected={len(symbols)}'
        )
    return {
        'exact_row_count': len(observed),
        'fresh_exact_row_count': len(fresh),
        'membership_sha256': market_membership_sha256(observed),
    }


def prune_stale_signal_feature_rows(
    *,
    market_date: date,
    exact_symbols: Sequence[str],
    repair_started_at: datetime,
    expected_stale_extras: Mapping[str, Any],
) -> dict[str, Any]:
    symbols = _normalize_symbols(exact_symbols)
    exact_set = set(symbols)
    with session_scope(statement_timeout_ms=30_000) as session:
        session.execute(
            text(
                'LOCK TABLE daily_signal_features '
                'IN SHARE ROW EXCLUSIVE MODE'
            )
        )
        rows = session.execute(
            select(
                DailySignalFeature,
                Symbol.symbol.label('symbol'),
            )
            .join(Symbol, Symbol.id == DailySignalFeature.symbol_id)
            .where(DailySignalFeature.market_date == market_date)
            .order_by(Symbol.symbol.asc())
        ).all()
        normalized_rows = []
        for feature, symbol in rows:
            feature.symbol = symbol
            normalized_rows.append(feature)
        exact_rows = [
            row for row in normalized_rows if str(row.symbol) in exact_set
        ]
        fresh_exact_symbols = sorted(
            str(row.symbol)
            for row in exact_rows
            if row.updated_at is not None
            and row.updated_at >= repair_started_at
        )
        if (
            sorted(str(row.symbol) for row in exact_rows) != symbols
            or fresh_exact_symbols != symbols
        ):
            raise RuntimeError(
                'signal-feature exact rows are not fully fresh before prune'
            )

        stale_rows = [
            row for row in normalized_rows if str(row.symbol) not in exact_set
        ]
        observed_stale = _signal_rows_fingerprint(stale_rows)
        expected = {
            'row_count': expected_stale_extras.get('row_count'),
            'membership_sha256': expected_stale_extras.get(
                'membership_sha256'
            ),
            'output_sha256': expected_stale_extras.get('output_sha256'),
        }
        if observed_stale != expected:
            raise RuntimeError(
                'signal-feature stale extras changed before prune'
            )
        stale_ids = [int(row.id) for row in stale_rows]
        deleted_count = int(
            session.execute(
                delete(DailySignalFeature).where(
                    DailySignalFeature.id.in_(stale_ids)
                )
            ).rowcount
            or 0
        )
        if deleted_count != len(stale_ids):
            raise RuntimeError(
                'signal-feature stale prune delete count mismatch'
            )
        remaining = list(
            session.execute(
                select(Symbol.symbol)
                .join(
                    DailySignalFeature,
                    DailySignalFeature.symbol_id == Symbol.id,
                )
                .where(DailySignalFeature.market_date == market_date)
                .order_by(Symbol.symbol.asc())
            ).scalars()
        )
        if remaining != symbols:
            raise RuntimeError(
                'signal-feature membership is not exact after stale prune'
            )
    return {
        'reviewed_stale_row_count': expected['row_count'],
        'reviewed_stale_membership_sha256': expected[
            'membership_sha256'
        ],
        'reviewed_stale_output_sha256': expected['output_sha256'],
        'pruned_stale_row_count': deleted_count,
        'remaining_symbol_count': len(symbols),
        'remaining_membership_sha256': market_membership_sha256(symbols),
    }


@contextmanager
def exact_signal_feature_guard(*, market_date: date):
    with exact_derived_stage_guard(
        market_date=market_date,
        lock_symbol_writes=True,
    ) as guard:
        yield guard


class ExactSignalFeatureRepair:
    """Rebuild historical signal rows from stored data, then prune extras."""

    def __init__(
        self,
        manifest: Mapping[str, Any],
        *,
        rebuild_runner: Callable[..., Mapping[str, Any]] | None = None,
        stored_symbols_loader: Callable[[str], Sequence[str]] | None = None,
        pending_batch_loader: Callable[[], Sequence[Mapping[str, Any]]]
        | None = None,
        frozen_membership_loader: Callable[..., Sequence[str] | None]
        | None = None,
        completion_state_loader: Callable[
            [str, Sequence[str]], Mapping[str, Any]
        ]
        | None = None,
        exact_verifier: Callable[..., Mapping[str, Any]] | None = None,
        stale_pruner: Callable[..., Mapping[str, Any]] | None = None,
        guard_factory: Callable[..., AbstractContextManager[Any]] | None = None,
        finality_validator: Callable[[date], None] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.manifest = normalize_manifest(manifest)
        self.manifest_sha256 = canonical_json_sha256(self.manifest)
        self.rebuild_runner = (
            rebuild_runner
            or daily_signal_feature_service.
            rebuild_stored_owned_fallback_for_market_date
        )
        self.stored_symbols_loader = (
            stored_symbols_loader or list_symbols_with_daily_bar_on_date
        )
        self.pending_batch_loader = (
            pending_batch_loader or load_pending_price_bar_rebuild_batches
        )
        self.frozen_membership_loader = (
            frozen_membership_loader
            or resolve_pending_frozen_price_dependent_membership
        )
        self.completion_state_loader = (
            completion_state_loader or _load_completion_state
        )
        self.exact_verifier = (
            exact_verifier or verify_exact_signal_feature_rows
        )
        self.stale_pruner = stale_pruner or prune_stale_signal_feature_rows
        self.guard_factory = guard_factory or exact_signal_feature_guard
        self.finality_validator = finality_validator or (
            lambda market_date: validate_finalized_krx_write_date(
                market_date,
                operation='exact stored-data signal-feature repair',
            )
        )
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def _validate_manifest_scope(self, item: Mapping[str, Any]) -> None:
        expected = list(item['symbols'])
        stored = _normalize_symbols(
            self.stored_symbols_loader(str(item['market_date']))
        )
        if not set(expected).issubset(stored):
            raise ValueError(
                'stored daily-price-bar membership is missing manifest symbols'
            )
        frozen = self.frozen_membership_loader(
            market_date=str(item['market_date']),
            pending=list(self.pending_batch_loader()),
        )
        if frozen is None:
            raise ValueError(
                'pending frozen price-dependent membership is unavailable'
            )
        if _normalize_symbols(frozen) != expected:
            raise ValueError(
                'pending frozen membership does not match manifest'
            )

    @staticmethod
    def _validated_source_fingerprints(
        value: Any,
        *,
        expected_symbol_count: int,
        expected_membership_sha256: str,
    ) -> dict[str, dict[str, Any]] | None:
        if not isinstance(value, Mapping) or set(value) != SOURCE_COMPONENTS:
            return None
        normalized: dict[str, dict[str, Any]] = {}
        for component in sorted(SOURCE_COMPONENTS):
            entry = value.get(component)
            if not isinstance(entry, Mapping):
                return None
            row_count = entry.get('row_count')
            digest = str(entry.get('source_sha256') or '').strip().lower()
            if (
                isinstance(row_count, bool)
                or not isinstance(row_count, int)
                or row_count < 0
                or len(digest) != 64
                or any(character not in '0123456789abcdef' for character in digest)
            ):
                return None
            normalized_entry = {
                'row_count': row_count,
                'source_sha256': digest,
            }
            if component == 'price_history':
                if (
                    entry.get('history_row_limit') != PRICE_HISTORY_ROWS
                    or row_count < expected_symbol_count
                ):
                    return None
                normalized_entry['history_row_limit'] = PRICE_HISTORY_ROWS
            if component == 'symbol_metadata':
                if (
                    entry.get('scope_symbol_count') != expected_symbol_count
                    or row_count != expected_symbol_count
                    or entry.get('placeholder_name_count') != 0
                ):
                    return None
                normalized_entry.update(
                    {
                        'scope_symbol_count': expected_symbol_count,
                        'placeholder_name_count': 0,
                    }
                )
            if component in _EXACT_FACTOR_SOURCE_MODELS and (
                row_count != expected_symbol_count
            ):
                return None
            if component in _FULL_DATE_UNIVERSE_SOURCE_MODELS:
                exact_scope_row_count = entry.get(
                    'exact_scope_row_count'
                )
                exact_scope_membership_sha256 = str(
                    entry.get('exact_scope_membership_sha256') or ''
                ).strip().lower()
                if (
                    row_count < expected_symbol_count
                    or exact_scope_row_count != expected_symbol_count
                    or exact_scope_membership_sha256
                    != expected_membership_sha256
                ):
                    return None
                normalized_entry.update(
                    {
                        'exact_scope_row_count': expected_symbol_count,
                        'exact_scope_membership_sha256': (
                            expected_membership_sha256
                        ),
                    }
                )
            normalized[component] = normalized_entry
        return normalized

    @staticmethod
    def _validated_stale_extras(value: Any) -> dict[str, Any] | None:
        if not isinstance(value, Mapping):
            return None
        row_count = value.get('row_count')
        membership_sha256 = str(
            value.get('membership_sha256') or ''
        ).strip().lower()
        output_sha256 = str(value.get('output_sha256') or '').strip().lower()
        if (
            isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
            or any(
                len(digest) != 64
                or any(
                    character not in '0123456789abcdef'
                    for character in digest
                )
                for digest in (membership_sha256, output_sha256)
            )
        ):
            return None
        return {
            'row_count': row_count,
            'membership_sha256': membership_sha256,
            'output_sha256': output_sha256,
        }

    def _review_date_with_state(
        self,
        item: Mapping[str, Any],
    ) -> tuple[dict[str, Any], Mapping[str, Any]]:
        self._validate_manifest_scope(item)
        state = self.completion_state_loader(
            str(item['market_date']),
            list(item['symbols']),
        )
        sources = self._validated_source_fingerprints(
            state.get('source_fingerprints')
            if isinstance(state, Mapping)
            else None,
            expected_symbol_count=int(item['symbol_count']),
            expected_membership_sha256=str(item['membership_sha256']),
        )
        stale_extras = self._validated_stale_extras(
            state.get('stale_extras')
            if isinstance(state, Mapping)
            else None
        )
        if sources is None or stale_extras is None:
            raise RuntimeError(
                'signal-feature source or stale-row fingerprint is invalid'
            )
        return (
            {
                'implementation_contract': IMPLEMENTATION_CONTRACT,
                'market_date': item['market_date'],
                'exact_symbol_count': item['symbol_count'],
                'membership_sha256': item['membership_sha256'],
                'source_fingerprints': sources,
                'stale_extras': stale_extras,
            },
            state,
        )

    @classmethod
    def _completed_checkpoint_is_valid(
        cls,
        unit: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> bool:
        proof = unit.get('dry_proof')
        result = unit.get('result')
        if not isinstance(proof, Mapping) or not isinstance(result, Mapping):
            return False
        sources = cls._validated_source_fingerprints(
            result.get('source_fingerprints'),
            expected_symbol_count=int(item['symbol_count']),
            expected_membership_sha256=str(item['membership_sha256']),
        )
        reviewed_sources = cls._validated_source_fingerprints(
            proof.get('source_fingerprints'),
            expected_symbol_count=int(item['symbol_count']),
            expected_membership_sha256=str(item['membership_sha256']),
        )
        if sources is None or reviewed_sources != sources:
            return False
        reviewed_stale = cls._validated_stale_extras(
            proof.get('stale_extras')
        )
        if reviewed_stale is None:
            return False
        verification = result.get('persistence_verification')
        prune_result = result.get('stale_prune')
        completion = result.get('completion_evidence')
        if not all(
            isinstance(value, Mapping)
            for value in (verification, prune_result, completion)
        ):
            return False
        expected_count = int(item['symbol_count'])
        expected_hash = str(item['membership_sha256'])
        output_sha256 = str(
            completion.get('output_fingerprint', {}).get(
                'output_sha256'
            )
            or ''
        ).strip().lower()
        output_hash_valid = bool(
            len(output_sha256) == 64
            and all(
                character in '0123456789abcdef'
                for character in output_sha256
            )
        )
        return bool(
            proof.get('implementation_contract') == IMPLEMENTATION_CONTRACT
            and result.get('implementation_contract')
            == IMPLEMENTATION_CONTRACT
            and result.get('status') == 'completed'
            and result.get('applied') is True
            and result.get('market_date') == item['market_date']
            and result.get('exact_symbol_count') == expected_count
            and result.get('membership_sha256') == expected_hash
            and verification.get('exact_row_count') == expected_count
            and verification.get('fresh_exact_row_count') == expected_count
            and verification.get('membership_sha256') == expected_hash
            and prune_result.get('reviewed_stale_row_count')
            == reviewed_stale['row_count']
            and prune_result.get('reviewed_stale_membership_sha256')
            == reviewed_stale['membership_sha256']
            and prune_result.get('reviewed_stale_output_sha256')
            == reviewed_stale['output_sha256']
            and prune_result.get('pruned_stale_row_count')
            == reviewed_stale['row_count']
            and prune_result.get('remaining_symbol_count') == expected_count
            and prune_result.get('remaining_membership_sha256')
            == expected_hash
            and completion.get('output_membership') == list(item['symbols'])
            and completion.get('output_fingerprint', {}).get('row_count')
            == expected_count
            and output_hash_valid
        )

    def _completed_live_state_is_valid(
        self,
        unit: Mapping[str, Any],
        item: Mapping[str, Any],
        *,
        live_state: Mapping[str, Any],
    ) -> bool:
        if not self._completed_checkpoint_is_valid(unit, item):
            return False
        live_sources = self._validated_source_fingerprints(
            live_state.get('source_fingerprints'),
            expected_symbol_count=int(item['symbol_count']),
            expected_membership_sha256=str(item['membership_sha256']),
        )
        expected_sources = self._validated_source_fingerprints(
            unit['result'].get('source_fingerprints'),
            expected_symbol_count=int(item['symbol_count']),
            expected_membership_sha256=str(item['membership_sha256']),
        )
        return bool(
            live_sources is not None
            and live_sources == expected_sources
            and live_state.get('output_membership') == list(item['symbols'])
            and live_state.get('output_fingerprint')
            == unit['result']['completion_evidence'].get(
                'output_fingerprint'
            )
            and self._validated_stale_extras(
                live_state.get('stale_extras')
            )
            == {
                'row_count': 0,
                'membership_sha256': market_membership_sha256(()),
                'output_sha256': canonical_json_sha256([]),
            }
        )

    @classmethod
    def _safe_zero_stale_revalidation(
        cls,
        *,
        reviewed_proof: Mapping[str, Any],
        current_proof: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> bool:
        reviewed_sources = cls._validated_source_fingerprints(
            reviewed_proof.get('source_fingerprints'),
            expected_symbol_count=int(item['symbol_count']),
            expected_membership_sha256=str(item['membership_sha256']),
        )
        current_sources = cls._validated_source_fingerprints(
            current_proof.get('source_fingerprints'),
            expected_symbol_count=int(item['symbol_count']),
            expected_membership_sha256=str(item['membership_sha256']),
        )
        current_stale = cls._validated_stale_extras(
            current_proof.get('stale_extras')
        )
        return bool(
            reviewed_sources is not None
            and current_sources == reviewed_sources
            and reviewed_proof.get('implementation_contract')
            == IMPLEMENTATION_CONTRACT
            and reviewed_proof.get('market_date') == item['market_date']
            and reviewed_proof.get('exact_symbol_count')
            == item['symbol_count']
            and reviewed_proof.get('membership_sha256')
            == item['membership_sha256']
            and cls._validated_stale_extras(
                reviewed_proof.get('stale_extras')
            )
            is not None
            and current_proof.get('implementation_contract')
            == IMPLEMENTATION_CONTRACT
            and current_proof.get('market_date') == item['market_date']
            and current_proof.get('exact_symbol_count')
            == item['symbol_count']
            and current_proof.get('membership_sha256')
            == item['membership_sha256']
            and current_stale
            == {
                'row_count': 0,
                'membership_sha256': market_membership_sha256(()),
                'output_sha256': canonical_json_sha256([]),
            }
        )

    def _apply_date(
        self,
        item: Mapping[str, Any],
        dry_proof: Mapping[str, Any],
        *,
        before_state: Mapping[str, Any],
    ) -> dict[str, Any]:
        reviewed_sources = self._validated_source_fingerprints(
            dry_proof.get('source_fingerprints'),
            expected_symbol_count=int(item['symbol_count']),
            expected_membership_sha256=str(item['membership_sha256']),
        )
        before_sources = self._validated_source_fingerprints(
            before_state.get('source_fingerprints'),
            expected_symbol_count=int(item['symbol_count']),
            expected_membership_sha256=str(item['membership_sha256']),
        )
        if reviewed_sources is None or before_sources != reviewed_sources:
            raise RuntimeError(
                'signal-feature source dependency changed after dry review'
            )
        repair_started_at = self.clock()
        if repair_started_at.tzinfo is None:
            raise ValueError('repair clock must return a timezone-aware value')
        rebuild_result = dict(
            self.rebuild_runner(
                market_date=str(item['market_date']),
                exact_symbols=list(item['symbols']),
            )
        )
        expected_count = int(item['symbol_count'])
        expected_hash = str(item['membership_sha256'])
        if (
            rebuild_result.get('exact_scope') is not True
            or rebuild_result.get('features_persisted') != expected_count
            or rebuild_result.get('scope_symbol_count') != expected_count
            or rebuild_result.get('scope_membership_sha256')
            != expected_hash
        ):
            raise RuntimeError(
                'signal-feature rebuild did not persist the exact manifest scope'
            )
        persistence_verification = dict(
            self.exact_verifier(
                market_date=date.fromisoformat(str(item['market_date'])),
                exact_symbols=list(item['symbols']),
                repair_started_at=repair_started_at,
            )
        )
        if (
            persistence_verification.get('exact_row_count') != expected_count
            or persistence_verification.get('fresh_exact_row_count')
            != expected_count
            or persistence_verification.get('membership_sha256')
            != expected_hash
        ):
            raise RuntimeError(
                'signal-feature exact persistence proof is incomplete'
            )

        persisted_state = self.completion_state_loader(
            str(item['market_date']),
            list(item['symbols']),
        )
        persisted_sources = self._validated_source_fingerprints(
            persisted_state.get('source_fingerprints'),
            expected_symbol_count=expected_count,
            expected_membership_sha256=expected_hash,
        )
        if persisted_sources != before_sources:
            raise RuntimeError(
                'signal-feature source dependency changed during persistence'
            )

        stale_prune = dict(
            self.stale_pruner(
                market_date=date.fromisoformat(str(item['market_date'])),
                exact_symbols=list(item['symbols']),
                repair_started_at=repair_started_at,
                expected_stale_extras=dict(dry_proof['stale_extras']),
            )
        )
        final_state = self.completion_state_loader(
            str(item['market_date']),
            list(item['symbols']),
        )
        final_sources = self._validated_source_fingerprints(
            final_state.get('source_fingerprints'),
            expected_symbol_count=expected_count,
            expected_membership_sha256=expected_hash,
        )
        if final_sources != before_sources:
            raise RuntimeError(
                'signal-feature source dependency changed during stale prune'
            )
        result = {
            'implementation_contract': IMPLEMENTATION_CONTRACT,
            'market_date': item['market_date'],
            'status': 'completed',
            'applied': True,
            'repair_source': REPAIR_SOURCE,
            'exact_symbol_count': expected_count,
            'membership_sha256': expected_hash,
            'source_fingerprints': final_sources,
            'rebuild_result': rebuild_result,
            'persistence_verification': persistence_verification,
            'stale_prune': stale_prune,
            'completion_evidence': {
                'output_membership': final_state.get('output_membership'),
                'output_fingerprint': final_state.get('output_fingerprint'),
            },
        }
        completed_unit = {'dry_proof': dry_proof, 'result': result}
        if not self._completed_checkpoint_is_valid(completed_unit, item):
            raise RuntimeError(
                'signal-feature apply did not complete the reviewed scope'
            )
        if not self._completed_live_state_is_valid(
            completed_unit,
            item,
            live_state=final_state,
        ):
            raise RuntimeError(
                'signal-feature live completion evidence does not match result'
            )
        return result

    def _reconcile_applied_result(
        self,
        *,
        item: Mapping[str, Any],
        dry_proof: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> tuple[bool, dict[str, Any]]:
        """Re-read durable rows after a guard/checkpoint uncertainty window."""

        try:
            live_state = self.completion_state_loader(
                str(item['market_date']),
                list(item['symbols']),
            )
            reconciled_unit = {
                'dry_proof': dict(dry_proof),
                'result': dict(result),
            }
            live_valid = bool(
                self._completed_checkpoint_is_valid(
                    reconciled_unit,
                    item,
                )
                and self._completed_live_state_is_valid(
                    reconciled_unit,
                    item,
                    live_state=live_state,
                )
            )
        except Exception as exc:  # noqa: BLE001
            return False, {
                'error_type': type(exc).__name__,
                'error_message': str(exc)[:500],
            }
        output_membership = live_state.get('output_membership')
        output_fingerprint = live_state.get('output_fingerprint')
        return live_valid, {
            'output_symbol_count': (
                len(output_membership)
                if isinstance(output_membership, list)
                else None
            ),
            'output_membership_sha256': (
                market_membership_sha256(output_membership)
                if isinstance(output_membership, list)
                else None
            ),
            'output_fingerprint': (
                dict(output_fingerprint)
                if isinstance(output_fingerprint, Mapping)
                else None
            ),
        }

    def run(
        self,
        *,
        apply: bool = False,
        expected_manifest_sha256: str | None = None,
        checkpoint_path: str | Path | None = None,
        _checkpoint_lock_held: bool = False,
    ) -> dict[str, Any]:
        base = {
            'manifest_sha256': self.manifest_sha256,
            'date_count': self.manifest['date_count'],
            'mode': 'apply' if apply else 'dry_run',
            'implementation_contract': IMPLEMENTATION_CONTRACT,
        }
        if checkpoint_path is None:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_path_required',
            }
        if not _checkpoint_lock_held:
            with checkpoint_file_lock(checkpoint_path) as lock_acquired:
                if not lock_acquired:
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': 'checkpoint_lock_unavailable',
                    }
                return self.run(
                    apply=apply,
                    expected_manifest_sha256=expected_manifest_sha256,
                    checkpoint_path=checkpoint_path,
                    _checkpoint_lock_held=True,
                )
        if apply:
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
        reviewed_dates = 0
        failed_dates = 0
        for item in self.manifest['dates']:
            market_date = str(item['market_date'])
            item_sha256 = canonical_json_sha256(item)
            previous = checkpoint['units'].get(market_date)
            if isinstance(previous, Mapping) and (
                previous.get('item_sha256') != item_sha256
            ):
                return {
                    **base,
                    'status': 'blocked',
                    'applied': False,
                    'reason': 'checkpoint_date_hash_mismatch',
                    'market_date': market_date,
                }
            if apply and (
                not isinstance(previous, Mapping)
                or not isinstance(previous.get('dry_proof'), Mapping)
            ):
                return {
                    **base,
                    'status': 'blocked',
                    'applied': False,
                    'reason': 'reviewed_dry_proof_required',
                    'market_date': market_date,
                }
            try:
                self.finality_validator(date.fromisoformat(market_date))
            except (TypeError, ValueError) as exc:
                failed_dates += 1
                checkpoint['units'][market_date] = {
                    'status': 'failed',
                    'item_sha256': item_sha256,
                    **(
                        {'dry_proof': dict(previous['dry_proof'])}
                        if isinstance(previous, Mapping)
                        and isinstance(previous.get('dry_proof'), Mapping)
                        else {}
                    ),
                    'reason': 'market_date_not_finalized',
                    'error_message': str(exc)[:500],
                }
                write_checkpoint(checkpoint_path, checkpoint)
                continue

            applied_result: dict[str, Any] | None = None
            reviewed_proof: dict[str, Any] | None = None
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
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': str(reason or 'active_writer'),
                        'market_date': market_date,
                    }
                try:
                    current_proof, current_state = (
                        self._review_date_with_state(item)
                    )
                except Exception as exc:  # noqa: BLE001
                    checkpoint['units'][market_date] = {
                        'status': 'failed',
                        'item_sha256': item_sha256,
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }
                    write_checkpoint(checkpoint_path, checkpoint)
                    failed_dates += 1
                    continue

                completed_valid = False
                if (
                    isinstance(previous, Mapping)
                    and previous.get('status') == 'completed'
                    and self._completed_checkpoint_is_valid(previous, item)
                ):
                    try:
                        completed_valid = self._completed_live_state_is_valid(
                            previous,
                            item,
                            live_state=current_state,
                        )
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
                if completed_valid:
                    completed_dates += 1
                    skipped_dates += 1
                    continue

                if not apply:
                    checkpoint['units'][market_date] = {
                        'status': 'reviewed',
                        'item_sha256': item_sha256,
                        'dry_proof': current_proof,
                    }
                    write_checkpoint(checkpoint_path, checkpoint)
                    reviewed_dates += 1
                    continue

                reviewed_proof = dict(previous['dry_proof'])
                if current_proof != reviewed_proof:
                    if self._safe_zero_stale_revalidation(
                        reviewed_proof=reviewed_proof,
                        current_proof=current_proof,
                        item=item,
                    ):
                        # The prior proof already authorized the manifest
                        # writes.  Rebinding to an empty stale set is safe on
                        # resume because this retry cannot delete any row.
                        reviewed_proof = current_proof
                    else:
                        return {
                            **base,
                            'status': 'blocked',
                            'applied': False,
                            'reason': 'reviewed_dry_proof_drift',
                            'market_date': market_date,
                        }
                try:
                    applied_result = self._apply_date(
                        item,
                        reviewed_proof,
                        before_state=current_state,
                    )
                except Exception as exc:  # noqa: BLE001
                    checkpoint['units'][market_date] = {
                        'status': 'failed',
                        'item_sha256': item_sha256,
                        'dry_proof': reviewed_proof,
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }
                    write_checkpoint(checkpoint_path, checkpoint)
                    failed_dates += 1
                    continue
            guard_exit_error = guard_exit_state.get('error')
            if isinstance(guard_exit_error, Exception):
                live_valid = False
                reconciliation: dict[str, Any] = {}
                if applied_result is not None and reviewed_proof is not None:
                    live_valid, reconciliation = (
                        self._reconcile_applied_result(
                            item=item,
                            dry_proof=reviewed_proof,
                            result=applied_result,
                        )
                    )
                uncertain_attempt = {
                    'reason': 'guard_transaction_exit_failed',
                    'error_type': type(guard_exit_error).__name__,
                    'error_message': str(guard_exit_error)[:500],
                    'live_completion_verified': live_valid,
                    'reconciliation': reconciliation,
                }
                uncertain_unit = (
                    deepcopy(dict(previous))
                    if isinstance(previous, Mapping)
                    else {
                        'status': 'uncertain',
                        'item_sha256': item_sha256,
                    }
                )
                uncertain_unit['last_uncertain_attempt'] = (
                    uncertain_attempt
                )
                checkpoint['units'][market_date] = uncertain_unit
                checkpoint_error: Exception | None = None
                try:
                    write_checkpoint(checkpoint_path, checkpoint)
                except Exception as exc:  # noqa: BLE001
                    checkpoint_error = exc
                uncertain_result = {
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
                    uncertain_result.update(
                        {
                            'checkpoint_error_type': type(
                                checkpoint_error
                            ).__name__,
                            'checkpoint_error_message': str(
                                checkpoint_error
                            )[:500],
                        }
                    )
                return uncertain_result
            if applied_result is None or reviewed_proof is None:
                return {
                    **base,
                    'status': 'blocked',
                    'applied': False,
                    'reason': 'signal_feature_apply_result_missing',
                    'market_date': market_date,
                }
            checkpoint['units'][market_date] = {
                'status': 'completed',
                'item_sha256': item_sha256,
                'dry_proof': reviewed_proof,
                'result': applied_result,
            }
            try:
                write_checkpoint(checkpoint_path, checkpoint)
            except Exception as checkpoint_error:  # noqa: BLE001
                live_valid, reconciliation = self._reconcile_applied_result(
                    item=item,
                    dry_proof=reviewed_proof,
                    result=applied_result,
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
            completed_dates += 1

        if not apply:
            status = 'dry_run' if failed_dates == 0 else 'blocked'
            return {
                **base,
                'status': status,
                'applied': False,
                'reviewed_dates': reviewed_dates,
                'completed_dates': completed_dates,
                'skipped_dates': skipped_dates,
                'failed_dates': failed_dates,
            }
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
            'Checkpoint reviewed stored-only signal-feature sources, rebuild '
            'each exact manifest date, then prune stale extras.'
        )
    )
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--checkpoint', required=True)
    parser.add_argument('--expected-manifest-sha256')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--dry-run', action='store_true')
    mode.add_argument('--apply', action='store_true')
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if Path(args.manifest).resolve() == Path(args.checkpoint).resolve():
        raise ValueError('manifest and checkpoint paths must be different')
    manifest = load_json_mapping(args.manifest, label='manifest')
    repair = ExactSignalFeatureRepair(manifest)
    result = repair.run(
        apply=bool(args.apply),
        expected_manifest_sha256=args.expected_manifest_sha256,
        checkpoint_path=args.checkpoint,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result['status'] in {'completed', 'dry_run'} else 2


if __name__ == '__main__':
    raise SystemExit(main())
