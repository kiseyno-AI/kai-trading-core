from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from datetime import date
import json
from pathlib import Path
from typing import Any

from sqlalchemy import case, func, select

from app.db import session_scope
from app.db_models import (
    DailyPriceBar,
    DailyPykrxEnrichmentRow,
    DailyTechnicalSnapshot,
    DartAuditSignal,
    DartDilutionSignal,
    DartDisclosureEvent,
    DartFundamentalsHighlight,
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
    Symbol,
)
from app.eod.derived_stage_repair import (
    exact_derived_stage_guard,
    normalize_manifest,
)
from app.eod.exact_factor_repair import (
    FACTOR_RESULT_MODELS,
    MEMBERSHIP_POLICY_STORED_ONLY,
    ExactFactorRepairService,
)
from app.eod.finality import validate_finalized_krx_write_date
from app.eod.market_membership import market_membership_sha256
from app.eod.price_bar_repair_state import (
    _daily_price_input_fingerprint,
    _derived_output_fingerprint,
    _stable_output_value,
    load_pending_price_bar_rebuild_batches,
    resolve_pending_frozen_price_dependent_membership,
)
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


CHECKPOINT_SCHEMA_VERSION = 'factor_exact_date_bulk_checkpoint_v1'
FACTOR_PRICE_HISTORY_ROWS = 253
FACTOR_SOURCE_COMPONENTS = frozenset(
    {
        'price_history',
        'symbol_metadata',
        'technical_snapshots',
        'valuation_enrichment',
        'dart_disclosure_events',
        'dart_fundamentals_snapshots',
        'dart_fundamentals_rows',
        'dart_fundamentals_highlights',
        'dart_audit_signals',
        'dart_dilution_signals',
    }
)


def _fingerprint_payload(payload: Any) -> dict[str, Any]:
    normalized = _stable_output_value(payload)
    return {
        'row_count': len(normalized) if isinstance(normalized, list) else 1,
        'source_sha256': canonical_json_sha256(normalized),
    }


def _model_row_payload(
    model: Any,
    rows: Sequence[Any],
    *,
    excluded_columns: Sequence[str] = (),
) -> list[dict[str, Any]]:
    excluded = {'created_at', 'updated_at', *excluded_columns}
    column_names = tuple(
        column.name
        for column in model.__table__.columns
        if column.name not in excluded
    )
    return [
        {
            column_name: _stable_output_value(getattr(row, column_name))
            for column_name in column_names
        }
        for row in rows
    ]


def _load_factor_source_fingerprints(
    market_date: str,
    exact_symbols: Sequence[str],
) -> dict[str, dict[str, Any]]:
    """Fingerprint every persisted source family read by the factor chain.

    The DART queries deliberately fingerprint the complete eligible annual
    source scope instead of only today's winning row.  That conservative
    superset makes a checkpoint stale whenever a newly eligible filing could
    change the service's latest-row selection.
    """

    parsed_date = date.fromisoformat(market_date)
    symbols = _normalize_symbols(exact_symbols)
    bars_by_symbol = load_recent_price_bars_by_symbol(
        market_date=parsed_date,
        symbols=symbols,
        history_days=FACTOR_PRICE_HISTORY_ROWS,
    )
    price_payload = [
        [
            symbol,
            [
                {
                    key: _stable_output_value(value)
                    for key, value in sorted(bar.items())
                    if key != 'captured_at'
                }
                for bar in bars_by_symbol.get(symbol, [])
            ],
        ]
        for symbol in symbols
    ]
    price_fingerprint = _fingerprint_payload(price_payload)
    price_fingerprint['row_count'] = sum(
        len(rows) for _symbol, rows in price_payload
    )
    fingerprints: dict[str, dict[str, Any]] = {
        'price_history': {
            'history_row_limit': FACTOR_PRICE_HISTORY_ROWS,
            **price_fingerprint,
        }
    }

    with session_scope(statement_timeout_ms=30_000) as session:
        symbol_rows = list(
            session.execute(
                select(Symbol)
                .where(Symbol.symbol.in_(symbols))
                .order_by(Symbol.symbol.asc())
            ).scalars()
        )
        symbol_metadata_payload = [
            {
                'symbol': str(row.symbol),
                'name_ko': str(row.name_ko or ''),
                'market': str(row.market or ''),
                'is_active': bool(row.is_active),
            }
            for row in symbol_rows
        ]
        symbol_metadata_fingerprint = _fingerprint_payload(
            symbol_metadata_payload
        )
        symbol_metadata_fingerprint.update(
            {
                'scope_symbol_count': len(symbols),
                'placeholder_name_count': sum(
                    not str(row.name_ko or '').strip()
                    or str(row.name_ko).strip() == str(row.symbol)
                    for row in symbol_rows
                ),
            }
        )
        fingerprints['symbol_metadata'] = symbol_metadata_fingerprint

        technical_rows = list(
            session.execute(
                select(DailyTechnicalSnapshot)
                .join(Symbol, Symbol.id == DailyTechnicalSnapshot.symbol_id)
                .where(
                    DailyTechnicalSnapshot.market_date == parsed_date,
                    Symbol.symbol.in_(symbols),
                )
                .order_by(Symbol.symbol.asc())
            ).scalars()
        )
        fingerprints['technical_snapshots'] = _fingerprint_payload(
            _model_row_payload(DailyTechnicalSnapshot, technical_rows)
        )

        valuation_rows = list(
            session.execute(
                select(DailyPykrxEnrichmentRow)
                .join(Symbol, Symbol.id == DailyPykrxEnrichmentRow.symbol_id)
                .where(
                    DailyPykrxEnrichmentRow.market_date == parsed_date,
                    DailyPykrxEnrichmentRow.category == 'valuation_snapshot',
                    DailyPykrxEnrichmentRow.scope_type == 'symbol',
                    Symbol.symbol.in_(symbols),
                    DailyPykrxEnrichmentRow.source == 'pykrx',
                )
                .order_by(Symbol.symbol.asc())
            ).scalars()
        )
        fingerprints['valuation_enrichment'] = _fingerprint_payload(
            _model_row_payload(DailyPykrxEnrichmentRow, valuation_rows)
        )

        snapshot_rows = list(
            session.execute(
                select(DartFundamentalsSnapshot)
                .join(
                    DartDisclosureEvent,
                    DartDisclosureEvent.id
                    == DartFundamentalsSnapshot.disclosure_event_id,
                )
                .where(
                    DartFundamentalsSnapshot.stock_code.in_(symbols),
                    DartFundamentalsSnapshot.business_year <= parsed_date.year,
                    DartFundamentalsSnapshot.report_code == '11011',
                    DartFundamentalsSnapshot.fs_div.in_(['CFS', 'OFS']),
                    DartFundamentalsSnapshot.source_status == '000',
                    DartDisclosureEvent.receipt_date.is_not(None),
                    DartDisclosureEvent.receipt_date <= parsed_date,
                )
                .order_by(
                    DartFundamentalsSnapshot.stock_code.asc(),
                    DartFundamentalsSnapshot.business_year.asc(),
                    DartFundamentalsSnapshot.id.asc(),
                )
            ).scalars()
        )
        snapshot_ids = [row.id for row in snapshot_rows]
        event_ids = sorted({row.disclosure_event_id for row in snapshot_rows})
        event_rows = (
            list(
                session.execute(
                    select(DartDisclosureEvent)
                    .where(DartDisclosureEvent.id.in_(event_ids))
                    .order_by(DartDisclosureEvent.id.asc())
                ).scalars()
            )
            if event_ids
            else []
        )
        fundamental_rows = (
            list(
                session.execute(
                    select(DartFundamentalsRow)
                    .where(DartFundamentalsRow.snapshot_id.in_(snapshot_ids))
                    .order_by(
                        DartFundamentalsRow.snapshot_id.asc(),
                        DartFundamentalsRow.row_order.asc().nulls_last(),
                        DartFundamentalsRow.id.asc(),
                    )
                ).scalars()
            )
            if snapshot_ids
            else []
        )
        highlight_rows = (
            list(
                session.execute(
                    select(DartFundamentalsHighlight)
                    .where(
                        DartFundamentalsHighlight.snapshot_id.in_(snapshot_ids)
                    )
                    .order_by(
                        DartFundamentalsHighlight.snapshot_id.asc(),
                        DartFundamentalsHighlight.metric_key.asc(),
                    )
                ).scalars()
            )
            if snapshot_ids
            else []
        )
        fingerprints['dart_disclosure_events'] = _fingerprint_payload(
            _model_row_payload(DartDisclosureEvent, event_rows)
        )
        fingerprints['dart_fundamentals_snapshots'] = _fingerprint_payload(
            _model_row_payload(
                DartFundamentalsSnapshot,
                snapshot_rows,
                excluded_columns=('request_payload', 'response_payload'),
            )
        )
        fingerprints['dart_fundamentals_rows'] = _fingerprint_payload(
            _model_row_payload(DartFundamentalsRow, fundamental_rows)
        )
        fingerprints['dart_fundamentals_highlights'] = _fingerprint_payload(
            _model_row_payload(DartFundamentalsHighlight, highlight_rows)
        )

        eligible_rcept_nos = sorted({row.rcept_no for row in snapshot_rows})
        audit_rows = (
            list(
                session.execute(
                    select(DartAuditSignal)
                    .where(
                        DartAuditSignal.rcept_no.in_(eligible_rcept_nos),
                        DartAuditSignal.receipt_date.is_not(None),
                        DartAuditSignal.receipt_date <= parsed_date,
                    )
                    .order_by(
                        DartAuditSignal.stock_code.asc(),
                        DartAuditSignal.receipt_date.asc(),
                        DartAuditSignal.rcept_no.asc(),
                    )
                ).scalars()
            )
            if eligible_rcept_nos
            else []
        )
        fingerprints['dart_audit_signals'] = _fingerprint_payload(
            _model_row_payload(DartAuditSignal, audit_rows)
        )

        eligible_corp_codes = sorted({row.corp_code for row in event_rows})
        dilution_rows = (
            list(
                session.execute(
                    select(DartDilutionSignal)
                    .where(
                        DartDilutionSignal.corp_code.in_(eligible_corp_codes),
                        DartDilutionSignal.receipt_date.is_not(None),
                        DartDilutionSignal.receipt_date <= parsed_date,
                    )
                    .order_by(
                        DartDilutionSignal.corp_code.asc(),
                        DartDilutionSignal.receipt_date.asc(),
                        DartDilutionSignal.rcept_no.asc(),
                    )
                ).scalars()
            )
            if eligible_corp_codes
            else []
        )
        fingerprints['dart_dilution_signals'] = _fingerprint_payload(
            _model_row_payload(DartDilutionSignal, dilution_rows)
        )

    return fingerprints


def _normalize_symbols(values: Sequence[str]) -> list[str]:
    symbols = sorted(
        {
            str(value).strip().upper()
            for value in values
            if str(value).strip()
        }
    )
    if any(len(symbol) != 6 or not symbol.isdigit() for symbol in symbols):
        raise ValueError('factor repair scope contains an invalid symbol')
    return symbols


@contextmanager
def _already_guarded_lock(**_kwargs: Any):
    """Avoid a nested advisory lock while the bulk guard owns the fence."""

    yield True


@contextmanager
def exact_factor_bulk_guard(*, market_date: date):
    with exact_derived_stage_guard(
        market_date=market_date,
        lock_symbol_writes=True,
    ) as guard:
        yield guard


def _default_repair_service() -> ExactFactorRepairService:
    return ExactFactorRepairService(lock_factory=_already_guarded_lock)


def _load_factor_completion_state(
    market_date: str,
    exact_symbols: Sequence[str],
) -> dict[str, Any]:
    parsed_date = date.fromisoformat(market_date)
    normalized_symbols = _normalize_symbols(exact_symbols)
    exact_hash = market_membership_sha256(normalized_symbols)
    output_fingerprints: dict[str, dict[str, Any]] = {}
    output_memberships: dict[str, list[str]] = {}
    with session_scope(statement_timeout_ms=30_000) as session:
        input_sha256, input_row_count = _daily_price_input_fingerprint(
            session,
            market_date=parsed_date,
            symbols=normalized_symbols,
        )
        for stage_id, model in FACTOR_RESULT_MODELS.items():
            stored_symbols = list(
                session.execute(
                    select(Symbol.symbol)
                    .join(model, model.symbol_id == Symbol.id)
                    .where(model.market_date == parsed_date)
                    .order_by(Symbol.symbol.asc())
                ).scalars()
            )
            output_memberships[stage_id] = stored_symbols
            output_sha256, row_count = _derived_output_fingerprint(
                session,
                model=model,
                market_date=parsed_date,
                symbols=normalized_symbols,
            )
            output_fingerprints[stage_id] = {
                'row_count': row_count,
                'output_sha256': output_sha256,
            }

    affected = [
        batch
        for batch in load_pending_price_bar_rebuild_batches()
        if market_date
        in {
            str(value)
            for value in batch.get('required_rebuild_trade_dates') or []
        }
    ]
    metadata_verified = bool(affected)
    provider_membership_hashes: set[str] = set()
    evidence_lineages: set[tuple[str, int, str]] = set()
    for batch in affected:
        try:
            metadata = dict(batch['factor_rebuild_metadata'])
            if int(metadata['schema_version']) != 2:
                raise ValueError('unsupported factor rebuild schema')
            recorded = dict(dict(metadata['dates'])[market_date])
            if (
                int(recorded['exact_membership_count'])
                != len(normalized_symbols)
                or recorded['ranking_membership_sha256'] != exact_hash
                or recorded['daily_price_input_sha256'] != input_sha256
                or recorded['membership_evidence_source']
                != 'stored_eod_run_metadata'
                or int(recorded['membership_evidence_run_id']) <= 0
                or not str(recorded['membership_evidence_run_source']).strip()
            ):
                raise ValueError('factor rebuild evidence scope drift')
            provider_membership_hashes.add(
                str(recorded['provider_membership_sha256']).strip().lower()
            )
            evidence_lineages.add(
                (
                    str(recorded['membership_evidence_source']).strip(),
                    int(recorded['membership_evidence_run_id']),
                    str(recorded['membership_evidence_run_source']).strip(),
                )
            )
            recorded_outputs = dict(recorded['stage_output_fingerprints'])
            for stage_id in FACTOR_RESULT_MODELS:
                if dict(recorded_outputs[stage_id]) != output_fingerprints[stage_id]:
                    raise ValueError('factor rebuild output evidence drift')
        except (KeyError, TypeError, ValueError):
            metadata_verified = False
            break
    if len(provider_membership_hashes) > 1 or len(evidence_lineages) > 1:
        metadata_verified = False
    lineage = next(iter(evidence_lineages), (None, None, None))
    return {
        'source_fingerprints': _load_factor_source_fingerprints(
            market_date,
            normalized_symbols,
        ),
        'input_row_count': input_row_count,
        'input_sha256': input_sha256,
        'output_fingerprints': output_fingerprints,
        'output_memberships': output_memberships,
        'metadata_verified': metadata_verified,
        'pending_batch_count': len(affected),
        'provider_membership_sha256': (
            next(iter(provider_membership_hashes), None)
        ),
        'membership_evidence_source': lineage[0],
        'membership_evidence_run_id': lineage[1],
        'membership_evidence_run_source': lineage[2],
    }


class ExactFactorBulkRepair:
    """Run the reviewed stored-only exact factor repair over frozen dates."""

    def __init__(
        self,
        manifest: Mapping[str, Any],
        *,
        repair_service: ExactFactorRepairService | Any | None = None,
        stored_symbols_loader: Callable[[str], Sequence[str]] | None = None,
        pending_batch_loader: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
        frozen_membership_loader: Callable[..., Sequence[str] | None]
        | None = None,
        completion_state_loader: Callable[
            [str, Sequence[str]], Mapping[str, Any]
        ]
        | None = None,
        guard_factory: Callable[..., AbstractContextManager[Any]] | None = None,
        finality_validator: Callable[[date], None] | None = None,
    ) -> None:
        self.manifest = normalize_manifest(manifest)
        self.manifest_sha256 = canonical_json_sha256(self.manifest)
        self.repair_service = repair_service or _default_repair_service()
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
            completion_state_loader or _load_factor_completion_state
        )
        self.guard_factory = guard_factory or exact_factor_bulk_guard
        self.finality_validator = finality_validator or (
            lambda market_date: validate_finalized_krx_write_date(
                market_date,
                operation='exact stored-data factor bulk repair',
            )
        )

    def _validate_manifest_scope(self, item: Mapping[str, Any]) -> None:
        expected = list(item['symbols'])
        stored = _normalize_symbols(
            self.stored_symbols_loader(str(item['market_date']))
        )
        if not set(expected).issubset(stored):
            raise ValueError(
                'stored daily-price-bar membership is missing manifest symbols'
            )
        pending = list(self.pending_batch_loader())
        frozen = self.frozen_membership_loader(
            market_date=str(item['market_date']),
            pending=pending,
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
    def _dry_proof(
        result: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> dict[str, Any]:
        required = {
            'status': 'ready',
            'mode': 'dry_run',
            'membership_policy': MEMBERSHIP_POLICY_STORED_ONLY,
            'membership_evidence_source': 'stored_eod_run_metadata',
            'pending_frozen_membership': True,
            'exact_membership_count': item['symbol_count'],
            'ranking_membership_sha256': item['membership_sha256'],
        }
        mismatches = [
            field
            for field, expected_value in required.items()
            if result.get(field) != expected_value
        ]
        provider_count = result.get('exact_provider_membership_count')
        provider_hash = str(result.get('membership_sha256') or '').strip().lower()
        protected_codes = result.get('provider_excluded_from_ranking_codes')
        run_id = result.get('membership_evidence_run_id')
        run_source = str(
            result.get('membership_evidence_run_source') or ''
        ).strip()
        if (
            isinstance(provider_count, bool)
            or not isinstance(provider_count, int)
            or provider_count < item['symbol_count']
        ):
            mismatches.append('exact_provider_membership_count')
        if not isinstance(protected_codes, list):
            mismatches.append('provider_excluded_from_ranking_codes')
            normalized_protected_codes: list[str] = []
        else:
            normalized_protected_codes = sorted(
                {
                    str(value).strip().upper()
                    for value in protected_codes
                    if str(value).strip()
                }
            )
            if (
                normalized_protected_codes != protected_codes
                or set(normalized_protected_codes).intersection(item['symbols'])
                or provider_count
                != item['symbol_count'] + len(normalized_protected_codes)
                or market_membership_sha256(
                    sorted([*item['symbols'], *normalized_protected_codes])
                )
                != provider_hash
            ):
                mismatches.append('provider_excluded_from_ranking_codes')
        try:
            valid_provider_hash = (
                len(provider_hash) == 64
                and int(provider_hash, 16) >= 0
            )
        except ValueError:
            valid_provider_hash = False
        if not valid_provider_hash:
            mismatches.append('membership_sha256')
        if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
            mismatches.append('membership_evidence_run_id')
        if not run_source:
            mismatches.append('membership_evidence_run_source')
        if mismatches:
            repair_reason = str(result.get('reason') or '').strip()
            raise RuntimeError(
                'stored-only factor dry proof mismatch: '
                + ','.join(dict.fromkeys(mismatches))
                + (f'; reason={repair_reason}' if repair_reason else '')
            )
        return {
            'market_date': item['market_date'],
            'exact_symbol_count': item['symbol_count'],
            'ranking_membership_sha256': item['membership_sha256'],
            'provider_membership_count': provider_count,
            'provider_membership_sha256': provider_hash,
            'protected_provider_codes': normalized_protected_codes,
            'membership_evidence_source': 'stored_eod_run_metadata',
            'membership_evidence_run_id': run_id,
            'membership_evidence_run_source': run_source,
        }

    @staticmethod
    def _validated_source_fingerprints(
        value: Any,
    ) -> dict[str, dict[str, Any]] | None:
        if not isinstance(value, Mapping) or set(value) != FACTOR_SOURCE_COMPONENTS:
            return None
        normalized: dict[str, dict[str, Any]] = {}
        for component in sorted(FACTOR_SOURCE_COMPONENTS):
            entry = value.get(component)
            if not isinstance(entry, Mapping):
                return None
            row_count = entry.get('row_count')
            digest = str(entry.get('source_sha256') or '').strip().lower()
            if (
                isinstance(row_count, bool)
                or not isinstance(row_count, int)
                or row_count < 0
            ):
                return None
            try:
                valid_digest = len(digest) == 64 and int(digest, 16) >= 0
            except ValueError:
                valid_digest = False
            if not valid_digest:
                return None
            normalized_entry = {
                'row_count': row_count,
                'source_sha256': digest,
            }
            if component == 'price_history':
                if entry.get('history_row_limit') != FACTOR_PRICE_HISTORY_ROWS:
                    return None
                normalized_entry['history_row_limit'] = FACTOR_PRICE_HISTORY_ROWS
            if component == 'symbol_metadata':
                scope_symbol_count = entry.get('scope_symbol_count')
                placeholder_name_count = entry.get('placeholder_name_count')
                if (
                    isinstance(scope_symbol_count, bool)
                    or not isinstance(scope_symbol_count, int)
                    or scope_symbol_count <= 0
                    or row_count != scope_symbol_count
                    or placeholder_name_count != 0
                ):
                    return None
                normalized_entry.update(
                    {
                        'scope_symbol_count': scope_symbol_count,
                        'placeholder_name_count': 0,
                    }
                )
            normalized[component] = normalized_entry
        return normalized

    @staticmethod
    def _completed_checkpoint_is_valid(
        unit: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> bool:
        proof = unit.get('dry_proof')
        result = unit.get('result')
        if not isinstance(proof, Mapping) or not isinstance(result, Mapping):
            return False
        if (
            ExactFactorBulkRepair._validated_source_fingerprints(
                result.get('source_fingerprints')
            )
            is None
        ):
            return False
        if (
            result.get('status') != 'completed'
            or result.get('applied') is not True
            or result.get('market_date') != item['market_date']
            or result.get('membership_policy') != MEMBERSHIP_POLICY_STORED_ONLY
            or result.get('exact_membership_count') != item['symbol_count']
            or result.get('ranking_membership_sha256')
            != item['membership_sha256']
            or result.get('membership_sha256')
            != proof.get('provider_membership_sha256')
            or result.get('membership_evidence_source')
            != proof.get('membership_evidence_source')
            or result.get('membership_evidence_run_id')
            != proof.get('membership_evidence_run_id')
            or result.get('membership_evidence_run_source')
            != proof.get('membership_evidence_run_source')
        ):
            return False
        stage_results = result.get('stage_results')
        if not isinstance(stage_results, Mapping) or set(stage_results) != set(
            FACTOR_RESULT_MODELS
        ):
            return False
        for stage_id in FACTOR_RESULT_MODELS:
            stage_result = stage_results.get(stage_id)
            if not isinstance(stage_result, Mapping):
                return False
            finalization = stage_result.get('scope_finalization')
            expected = {
                'stage': stage_id,
                'market_date': item['market_date'],
                'exact_symbol_count': item['symbol_count'],
                'exact_membership_sha256': item['membership_sha256'],
                'successful_symbol_count': item['symbol_count'],
                'successful_membership_sha256': item['membership_sha256'],
                'authoritative_market_scope': True,
                'authoritative_markets': ['KOSDAQ', 'KOSPI'],
                'all_market_authority': False,
                'ready': True,
            }
            if not isinstance(finalization, Mapping) or any(
                finalization.get(field) != value
                for field, value in expected.items()
            ):
                return False
            deleted_count = finalization.get('deleted_stale_row_count')
            if (
                isinstance(deleted_count, bool)
                or not isinstance(deleted_count, int)
                or deleted_count < 0
            ):
                return False
        completion = result.get('completion_evidence')
        if not isinstance(completion, Mapping):
            return False
        fingerprints = completion.get('stage_output_fingerprints')
        if (
            int(completion.get('updated_batch_count') or 0) <= 0
            or completion.get('exact_membership_count') != item['symbol_count']
            or completion.get('ranking_membership_sha256')
            != item['membership_sha256']
            or completion.get('provider_membership_sha256')
            != proof.get('provider_membership_sha256')
            or completion.get('membership_evidence_source')
            != proof.get('membership_evidence_source')
            or completion.get('membership_evidence_run_id')
            != proof.get('membership_evidence_run_id')
            or completion.get('membership_evidence_run_source')
            != proof.get('membership_evidence_run_source')
            or not isinstance(fingerprints, Mapping)
            or set(fingerprints) != set(FACTOR_RESULT_MODELS)
        ):
            return False
        return True

    def _completed_live_state_is_valid(
        self,
        unit: Mapping[str, Any],
        item: Mapping[str, Any],
    ) -> bool:
        live = self.completion_state_loader(
            str(item['market_date']),
            list(item['symbols']),
        )
        if not isinstance(live, Mapping) or live.get('metadata_verified') is not True:
            return False
        if (
            live.get('input_row_count') != item['symbol_count']
            or live.get('provider_membership_sha256')
            != unit['dry_proof']['provider_membership_sha256']
            or live.get('membership_evidence_source')
            != unit['dry_proof']['membership_evidence_source']
            or live.get('membership_evidence_run_id')
            != unit['dry_proof']['membership_evidence_run_id']
            or live.get('membership_evidence_run_source')
            != unit['dry_proof']['membership_evidence_run_source']
        ):
            return False
        expected_completion = unit['result']['completion_evidence']
        expected_sources = self._validated_source_fingerprints(
            unit['result'].get('source_fingerprints')
        )
        live_sources = self._validated_source_fingerprints(
            live.get('source_fingerprints')
        )
        if expected_sources is None or live_sources != expected_sources:
            return False
        if live.get('input_sha256') != expected_completion.get(
            'daily_price_input_sha256'
        ):
            return False
        memberships = live.get('output_memberships')
        fingerprints = live.get('output_fingerprints')
        if not isinstance(memberships, Mapping) or not isinstance(
            fingerprints, Mapping
        ):
            return False
        expected_symbols = list(item['symbols'])
        allowed_extras = set(unit['dry_proof']['protected_provider_codes'])
        expected_fingerprints = expected_completion.get(
            'stage_output_fingerprints'
        )
        if not isinstance(expected_fingerprints, Mapping):
            return False
        for stage_id in FACTOR_RESULT_MODELS:
            observed_membership = memberships.get(stage_id)
            if not isinstance(observed_membership, list):
                return False
            if observed_membership != sorted(set(observed_membership)):
                return False
            observed_set = set(observed_membership)
            if (
                not set(expected_symbols).issubset(observed_set)
                or observed_set.difference(expected_symbols)
                - allowed_extras
            ):
                return False
            if fingerprints.get(stage_id) != expected_fingerprints.get(stage_id):
                return False
        return True

    def _review_date(self, item: Mapping[str, Any]) -> dict[str, Any]:
        self._validate_manifest_scope(item)
        result = self.repair_service.repair_for_market_date(
            market_date=str(item['market_date']),
            apply=False,
            membership_policy=MEMBERSHIP_POLICY_STORED_ONLY,
        )
        return self._dry_proof(result, item)

    def _apply_date(
        self,
        item: Mapping[str, Any],
        dry_proof: Mapping[str, Any],
    ) -> dict[str, Any]:
        before_state = self.completion_state_loader(
            str(item['market_date']),
            list(item['symbols']),
        )
        before_sources = self._validated_source_fingerprints(
            before_state.get('source_fingerprints')
            if isinstance(before_state, Mapping)
            else None
        )
        if before_sources is None:
            raise RuntimeError('factor source dependency fingerprint is invalid')
        result = dict(
            self.repair_service.repair_for_market_date(
                market_date=str(item['market_date']),
                apply=True,
                prune_stale=True,
                expected_provider_membership_count=(
                    dry_proof['provider_membership_count']
                ),
                expected_membership_sha256=(
                    dry_proof['provider_membership_sha256']
                ),
                membership_policy=MEMBERSHIP_POLICY_STORED_ONLY,
            )
        )
        after_state = self.completion_state_loader(
            str(item['market_date']),
            list(item['symbols']),
        )
        after_sources = self._validated_source_fingerprints(
            after_state.get('source_fingerprints')
            if isinstance(after_state, Mapping)
            else None
        )
        if after_sources is None:
            raise RuntimeError('factor source dependency fingerprint is invalid')
        if after_sources != before_sources:
            raise RuntimeError('factor source dependency changed during repair')
        result['source_fingerprints'] = after_sources
        completed_unit = {'dry_proof': dry_proof, 'result': result}
        if not self._completed_checkpoint_is_valid(completed_unit, item):
            raise RuntimeError(
                'factor apply did not complete reviewed exact scope: '
                f"status={result.get('status')}, "
                f"reason={result.get('reason')}"
            )
        if not self._completed_live_state_is_valid(completed_unit, item):
            raise RuntimeError(
                'factor apply live output/source evidence does not match result'
            )
        return result

    def run(
        self,
        *,
        apply: bool = False,
        expected_manifest_sha256: str | None = None,
        checkpoint_path: str | Path | None = None,
    ) -> dict[str, Any]:
        base = {
            'manifest_sha256': self.manifest_sha256,
            'date_count': self.manifest['date_count'],
            'mode': 'apply' if apply else 'dry_run',
            'membership_policy': MEMBERSHIP_POLICY_STORED_ONLY,
        }
        if checkpoint_path is None:
            return {
                **base,
                'status': 'blocked',
                'applied': False,
                'reason': 'checkpoint_path_required',
            }
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
            if apply:
                try:
                    self.finality_validator(date.fromisoformat(market_date))
                except (TypeError, ValueError) as exc:
                    failed_dates += 1
                    checkpoint['units'][market_date] = {
                        'status': 'failed',
                        'item_sha256': item_sha256,
                        'dry_proof': dict(previous['dry_proof']),
                        'reason': 'market_date_not_finalized',
                        'error_message': str(exc)[:500],
                    }
                    write_checkpoint(checkpoint_path, checkpoint)
                    continue

            with self.guard_factory(
                market_date=date.fromisoformat(market_date)
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
                    current_proof = self._review_date(item)
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

                if not apply:
                    completed_valid = False
                    if (
                        isinstance(previous, Mapping)
                        and previous.get('status') == 'completed'
                        and previous.get('dry_proof') == current_proof
                        and self._completed_checkpoint_is_valid(previous, item)
                    ):
                        try:
                            completed_valid = (
                                self._completed_live_state_is_valid(
                                    previous,
                                    item,
                                )
                            )
                        except Exception as exc:  # noqa: BLE001
                            return {
                                **base,
                                'status': 'blocked',
                                'applied': False,
                                'reason': (
                                    'checkpoint_live_revalidation_failed'
                                ),
                                'market_date': market_date,
                                'error_type': type(exc).__name__,
                                'error_message': str(exc)[:500],
                            }
                    if completed_valid:
                        completed_dates += 1
                        skipped_dates += 1
                        continue
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
                    return {
                        **base,
                        'status': 'blocked',
                        'applied': False,
                        'reason': 'reviewed_dry_proof_drift',
                        'market_date': market_date,
                    }
                completed_valid = False
                if (
                    previous.get('status') == 'completed'
                    and self._completed_checkpoint_is_valid(previous, item)
                ):
                    try:
                        completed_valid = self._completed_live_state_is_valid(
                            previous,
                            item,
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
                try:
                    result = self._apply_date(item, reviewed_proof)
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
            checkpoint['units'][market_date] = {
                'status': 'completed',
                'item_sha256': item_sha256,
                'dry_proof': reviewed_proof,
                'result': result,
            }
            write_checkpoint(checkpoint_path, checkpoint)
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
            'Checkpoint reviewed stored-only exact factor proofs, then rebuild '
            'and prune only the immutable manifest scope.'
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
    repair = ExactFactorBulkRepair(manifest)
    result = repair.run(
        apply=bool(args.apply),
        expected_manifest_sha256=args.expected_manifest_sha256,
        checkpoint_path=args.checkpoint,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result['status'] in {'completed', 'dry_run'} else 2


if __name__ == '__main__':
    raise SystemExit(main())
