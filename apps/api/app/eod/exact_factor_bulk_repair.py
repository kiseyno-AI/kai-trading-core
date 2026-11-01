from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, suppress
from datetime import date, datetime, timedelta
import hashlib
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
    GuardTransactionExitError,
    capture_guard_transaction_failure,
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
    PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE,
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
from app.signals.preferred_share_fundamentals import (
    resolve_preferred_fundamentals_issuer_symbols,
)
from app.signals.advanced_value_factor import (
    ADVANCED_VALUE_FUNDAMENTALS_FS_DIV,
    ADVANCED_VALUE_FUNDAMENTALS_FS_DIVS,
    ADVANCED_VALUE_FUNDAMENTALS_PERIOD_TYPE,
    ADVANCED_VALUE_FUNDAMENTALS_REPORT_CODE,
)
from app.signals.piotroski_filter import (
    DILUTION_EVIDENCE_SIGNAL_TYPES,
    DILUTION_NEGATIVE_COVERAGE_DAYS,
)


CHECKPOINT_SCHEMA_VERSION = 'factor_exact_date_bulk_checkpoint_v2'
FACTOR_REPAIR_IMPLEMENTATION_CONTRACT = (
    'factor_exact_date_bulk_implementation_v2'
)
FACTOR_LIVE_RECONCILIATION_ORIGIN = 'live_reconciliation_v1'
FACTOR_LIVE_RECONCILIATION_SCHEMA_VERSION = 1
FACTOR_PRICE_HISTORY_ROWS = 253
FACTOR_PRICE_FINGERPRINT_SYMBOL_CHUNK_SIZE = 250
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


def _fingerprint_mapping_rows(
    rows: Any,
) -> dict[str, Any]:
    """Hash an ordered result incrementally without retaining raw DART rows."""

    digest = hashlib.sha256()
    row_count = 0
    for row in rows:
        payload = json.dumps(
            _stable_output_value(dict(row)),
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
        ).encode('utf-8')
        digest.update(len(payload).to_bytes(8, 'big'))
        digest.update(payload)
        row_count += 1
    return {
        'row_count': row_count,
        'source_sha256': digest.hexdigest(),
    }


def _fingerprint_price_history(
    *,
    market_date: date,
    symbols: Sequence[str],
) -> dict[str, Any]:
    """Hash the consumer-visible 253-row histories without retaining them."""

    digest = hashlib.sha256()
    digest.update(b'[')
    row_count = 0
    first_symbol = True
    for start in range(
        0,
        len(symbols),
        FACTOR_PRICE_FINGERPRINT_SYMBOL_CHUNK_SIZE,
    ):
        symbol_chunk = list(
            symbols[
                start:start + FACTOR_PRICE_FINGERPRINT_SYMBOL_CHUNK_SIZE
            ]
        )
        bars_by_symbol = load_recent_price_bars_by_symbol(
            market_date=market_date,
            symbols=symbol_chunk,
            history_days=FACTOR_PRICE_HISTORY_ROWS,
        )
        for symbol in symbol_chunk:
            normalized_bars = [
                {
                    key: _stable_output_value(value)
                    for key, value in sorted(bar.items())
                    if key != 'captured_at'
                }
                for bar in bars_by_symbol.get(symbol, [])
            ]
            item = _stable_output_value([symbol, normalized_bars])
            if not first_symbol:
                digest.update(b',')
            digest.update(
                json.dumps(
                    item,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(',', ':'),
                ).encode('utf-8')
            )
            first_symbol = False
            row_count += len(normalized_bars)
    digest.update(b']')
    return {
        'history_row_limit': FACTOR_PRICE_HISTORY_ROWS,
        'row_count': row_count,
        'source_sha256': digest.hexdigest(),
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

    DART scope is the union of the exact Piotroski and Advanced/Unified/Quality
    winner selectors. A newly eligible winner therefore invalidates the
    checkpoint without hashing historical filings those consumers cannot use.
    """

    parsed_date = date.fromisoformat(market_date)
    symbols = _normalize_symbols(exact_symbols)
    fingerprints: dict[str, dict[str, Any]] = {
        'price_history': _fingerprint_price_history(
            market_date=parsed_date,
            symbols=symbols,
        )
    }

    with session_scope(statement_timeout_ms=30_000) as session:
        fundamentals_issuer_by_symbol = (
            resolve_preferred_fundamentals_issuer_symbols(
                session,
                symbols,
            )
        )
        fundamentals_symbols = sorted(
            {
                *symbols,
                *fundamentals_issuer_by_symbol.values(),
            }
        )
        symbol_rows = list(
            session.execute(
                select(Symbol)
                .where(Symbol.symbol.in_(fundamentals_symbols))
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
        fundamentals_issuer_aliases = dict(
            sorted(fundamentals_issuer_by_symbol.items())
        )
        symbol_metadata_fingerprint = {
            'row_count': len(symbol_metadata_payload),
            'source_sha256': canonical_json_sha256(
                _stable_output_value(
                    {
                        'rows': symbol_metadata_payload,
                        'fundamentals_scope_symbols': fundamentals_symbols,
                        'fundamentals_issuer_aliases': (
                            fundamentals_issuer_aliases
                        ),
                    }
                )
            ),
        }
        symbol_metadata_fingerprint.update(
            {
                'scope_symbol_count': len(fundamentals_symbols),
                'requested_symbol_count': len(symbols),
                'fundamentals_scope_symbols': fundamentals_symbols,
                'fundamentals_issuer_aliases': fundamentals_issuer_aliases,
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

        piotroski_latest_rank = func.row_number().over(
            partition_by=(
                DartFundamentalsSnapshot.stock_code,
                DartFundamentalsSnapshot.business_year,
            ),
            order_by=(
                DartDisclosureEvent.receipt_date.desc(),
                DartDisclosureEvent.rcept_no.desc(),
                case(
                    (
                        DartFundamentalsSnapshot.fs_div
                        == ADVANCED_VALUE_FUNDAMENTALS_FS_DIV,
                        0,
                    ),
                    else_=1,
                ).asc(),
                DartFundamentalsSnapshot.id.desc(),
            ),
        ).label('row_rank')
        piotroski_ranked_snapshots = (
            select(
                DartFundamentalsSnapshot.id.label('snapshot_id'),
                DartFundamentalsSnapshot.stock_code.label('stock_code'),
                DartFundamentalsSnapshot.business_year.label(
                    'business_year'
                ),
                piotroski_latest_rank,
            )
            .join(
                DartDisclosureEvent,
                DartDisclosureEvent.id
                == DartFundamentalsSnapshot.disclosure_event_id,
            )
            .where(
                DartFundamentalsSnapshot.stock_code.in_(
                    fundamentals_symbols
                ),
                DartFundamentalsSnapshot.business_year <= parsed_date.year,
                DartFundamentalsSnapshot.report_code == '11011',
                DartFundamentalsSnapshot.fs_div.in_(['CFS', 'OFS']),
                DartFundamentalsSnapshot.source_status == '000',
                DartDisclosureEvent.fundamentals_capture_status == 'captured',
                DartDisclosureEvent.receipt_date.is_not(None),
                DartDisclosureEvent.receipt_date <= parsed_date,
            )
            .subquery()
        )
        piotroski_latest_snapshot_rows = session.execute(
            select(piotroski_ranked_snapshots).where(
                piotroski_ranked_snapshots.c.row_rank == 1
            )
        ).mappings().all()
        piotroski_snapshot_ids_by_symbol: dict[
            str, list[tuple[int, int]]
        ] = {}
        for row in piotroski_latest_snapshot_rows:
            piotroski_snapshot_ids_by_symbol.setdefault(
                str(row['stock_code']), []
            ).append(
                (int(row['business_year']), int(row['snapshot_id']))
            )
        piotroski_selected_snapshot_ids = sorted(
            snapshot_id
            for rows in piotroski_snapshot_ids_by_symbol.values()
            for _business_year, snapshot_id in sorted(
                rows,
                reverse=True,
            )[:2]
        )
        piotroski_current_snapshot_id_by_symbol = {
            symbol: max(rows)[1]
            for symbol, rows in piotroski_snapshot_ids_by_symbol.items()
        }

        advanced_latest_rank = func.row_number().over(
            partition_by=DartFundamentalsSnapshot.stock_code,
            order_by=(
                DartFundamentalsSnapshot.business_year.desc(),
                DartDisclosureEvent.receipt_date.desc().nullslast(),
                DartDisclosureEvent.rcept_no.desc(),
                case(
                    (DartFundamentalsSnapshot.fs_div == 'CFS', 0),
                    else_=1,
                ).asc(),
                DartFundamentalsSnapshot.id.desc(),
            ),
        ).label('row_rank')
        advanced_ranked_snapshots = (
            select(
                DartFundamentalsSnapshot.id.label('snapshot_id'),
                advanced_latest_rank,
            )
            .join(
                DartDisclosureEvent,
                DartDisclosureEvent.id
                == DartFundamentalsSnapshot.disclosure_event_id,
            )
            .where(
                DartFundamentalsSnapshot.stock_code.in_(
                    fundamentals_symbols
                ),
                DartFundamentalsSnapshot.business_year <= parsed_date.year,
                DartFundamentalsSnapshot.report_code
                == ADVANCED_VALUE_FUNDAMENTALS_REPORT_CODE,
                DartFundamentalsSnapshot.fs_div.in_(
                    ADVANCED_VALUE_FUNDAMENTALS_FS_DIVS
                ),
                DartFundamentalsSnapshot.period_type
                == ADVANCED_VALUE_FUNDAMENTALS_PERIOD_TYPE,
                DartFundamentalsSnapshot.source_status == '000',
                DartDisclosureEvent.receipt_date.is_not(None),
                DartDisclosureEvent.receipt_date <= parsed_date,
            )
            .subquery()
        )
        advanced_snapshot_ids = {
            int(row['snapshot_id'])
            for row in session.execute(
                select(advanced_ranked_snapshots).where(
                    advanced_ranked_snapshots.c.row_rank == 1
                )
            ).mappings()
        }
        snapshot_ids = sorted(
            {
                *piotroski_selected_snapshot_ids,
                *advanced_snapshot_ids,
            }
        )
        snapshot_rows = (
            session.execute(
                select(
                    DartFundamentalsSnapshot.id.label('id'),
                    DartFundamentalsSnapshot.disclosure_event_id.label(
                        'disclosure_event_id'
                    ),
                    DartFundamentalsSnapshot.rcept_no.label('rcept_no'),
                    DartFundamentalsSnapshot.corp_code.label('corp_code'),
                    DartFundamentalsSnapshot.stock_code.label('stock_code'),
                    DartFundamentalsSnapshot.corp_name.label('corp_name'),
                    DartFundamentalsSnapshot.business_year.label(
                        'business_year'
                    ),
                    DartFundamentalsSnapshot.report_code.label('report_code'),
                    DartFundamentalsSnapshot.fs_div.label('fs_div'),
                    DartFundamentalsSnapshot.period_type.label('period_type'),
                    DartFundamentalsSnapshot.currency.label('currency'),
                    DartFundamentalsSnapshot.captured_at.label('captured_at'),
                    DartFundamentalsSnapshot.source_status.label(
                        'source_status'
                    ),
                )
                .where(DartFundamentalsSnapshot.id.in_(snapshot_ids))
                .order_by(
                    DartFundamentalsSnapshot.stock_code.asc(),
                    DartFundamentalsSnapshot.business_year.asc(),
                    DartFundamentalsSnapshot.id.asc(),
                )
            ).mappings().all()
            if snapshot_ids
            else []
        )
        event_ids = sorted(
            {int(row['disclosure_event_id']) for row in snapshot_rows}
        )
        event_rows = (
            session.execute(
                select(
                    DartDisclosureEvent.id.label('id'),
                    DartDisclosureEvent.rcept_no.label('rcept_no'),
                    DartDisclosureEvent.corp_code.label('corp_code'),
                    DartDisclosureEvent.stock_code.label('stock_code'),
                    DartDisclosureEvent.receipt_date.label('receipt_date'),
                    DartDisclosureEvent.fundamentals_capture_status.label(
                        'fundamentals_capture_status'
                    ),
                )
                .where(DartDisclosureEvent.id.in_(event_ids))
                .order_by(DartDisclosureEvent.id.asc())
            ).mappings().all()
            if event_ids
            else []
        )
        fingerprints['dart_disclosure_events'] = _fingerprint_payload(
            [dict(row) for row in event_rows]
        )
        fingerprints['dart_fundamentals_snapshots'] = _fingerprint_payload(
            [dict(row) for row in snapshot_rows]
        )
        if snapshot_ids:
            fundamental_statement = (
                select(
                    DartFundamentalsRow.id.label('id'),
                    DartFundamentalsRow.snapshot_id.label('snapshot_id'),
                    DartFundamentalsRow.row_key.label('row_key'),
                    DartFundamentalsRow.statement_type.label(
                        'statement_type'
                    ),
                    DartFundamentalsRow.statement_name.label(
                        'statement_name'
                    ),
                    DartFundamentalsRow.account_id.label('account_id'),
                    DartFundamentalsRow.account_name.label('account_name'),
                    DartFundamentalsRow.account_detail.label(
                        'account_detail'
                    ),
                    DartFundamentalsRow.current_amount.label(
                        'current_amount'
                    ),
                    DartFundamentalsRow.current_cumulative_amount.label(
                        'current_cumulative_amount'
                    ),
                    DartFundamentalsRow.prior_amount.label('prior_amount'),
                    DartFundamentalsRow.prior_cumulative_amount.label(
                        'prior_cumulative_amount'
                    ),
                    DartFundamentalsRow.pre_prior_amount.label(
                        'pre_prior_amount'
                    ),
                    DartFundamentalsRow.row_order.label('row_order'),
                    DartFundamentalsRow.currency.label('currency'),
                )
                .where(DartFundamentalsRow.snapshot_id.in_(snapshot_ids))
                .order_by(
                    DartFundamentalsRow.snapshot_id.asc(),
                    DartFundamentalsRow.row_order.asc().nulls_last(),
                    DartFundamentalsRow.id.asc(),
                )
                .execution_options(yield_per=1_000)
            )
            fingerprints['dart_fundamentals_rows'] = (
                _fingerprint_mapping_rows(
                    session.execute(fundamental_statement).mappings()
                )
            )
            highlight_statement = (
                select(
                    DartFundamentalsHighlight.id.label('id'),
                    DartFundamentalsHighlight.snapshot_id.label(
                        'snapshot_id'
                    ),
                    DartFundamentalsHighlight.metric_key.label('metric_key'),
                    DartFundamentalsHighlight.statement_type.label(
                        'statement_type'
                    ),
                    DartFundamentalsHighlight.statement_name.label(
                        'statement_name'
                    ),
                    DartFundamentalsHighlight.account_id.label('account_id'),
                    DartFundamentalsHighlight.account_name.label(
                        'account_name'
                    ),
                    DartFundamentalsHighlight.current_amount.label(
                        'current_amount'
                    ),
                    DartFundamentalsHighlight.current_cumulative_amount.label(
                        'current_cumulative_amount'
                    ),
                    DartFundamentalsHighlight.ratio_value.label('ratio_value'),
                    DartFundamentalsHighlight.currency.label('currency'),
                )
                .where(
                    DartFundamentalsHighlight.snapshot_id.in_(snapshot_ids)
                )
                .order_by(
                    DartFundamentalsHighlight.snapshot_id.asc(),
                    DartFundamentalsHighlight.metric_key.asc(),
                    DartFundamentalsHighlight.id.asc(),
                )
                .execution_options(yield_per=1_000)
            )
            fingerprints['dart_fundamentals_highlights'] = (
                _fingerprint_mapping_rows(
                    session.execute(highlight_statement).mappings()
                )
            )
        else:
            fingerprints['dart_fundamentals_rows'] = (
                _fingerprint_mapping_rows(())
            )
            fingerprints['dart_fundamentals_highlights'] = (
                _fingerprint_mapping_rows(())
            )

        snapshot_by_id = {int(row['id']): row for row in snapshot_rows}
        current_snapshot_by_symbol = {
            symbol: snapshot_by_id[snapshot_id]
            for symbol, snapshot_id in (
                piotroski_current_snapshot_id_by_symbol.items()
            )
            if snapshot_id in snapshot_by_id
        }
        eligible_rcept_nos = sorted(
            {
                str(row['rcept_no'])
                for row in current_snapshot_by_symbol.values()
            }
        )
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

        event_by_id = {int(row['id']): row for row in event_rows}
        eligible_corp_codes = sorted(
            {
                str(
                    event_by_id[int(row['disclosure_event_id'])][
                        'corp_code'
                    ]
                )
                for row in current_snapshot_by_symbol.values()
                if int(row['disclosure_event_id']) in event_by_id
            }
        )
        dilution_rows = (
            list(
                session.execute(
                    select(DartDilutionSignal)
                    .where(
                        DartDilutionSignal.corp_code.in_(eligible_corp_codes),
                        DartDilutionSignal.signal_type.in_(
                            DILUTION_EVIDENCE_SIGNAL_TYPES
                        ),
                        DartDilutionSignal.receipt_date.is_not(None),
                        DartDilutionSignal.receipt_date
                        >= parsed_date
                        - timedelta(days=DILUTION_NEGATIVE_COVERAGE_DAYS),
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
    evidence_lineages: set[
        tuple[str, int | None, str | None, str | None, tuple[str, ...]]
    ] = set()
    completion_records: set[tuple[str, str, tuple[str, ...]]] = set()
    for batch in affected:
        try:
            metadata = dict(batch['factor_rebuild_metadata'])
            if int(metadata['schema_version']) not in {2, 3}:
                raise ValueError('unsupported factor rebuild schema')
            recorded = dict(dict(metadata['dates'])[market_date])
            evidence_source = str(
                recorded['membership_evidence_source']
            ).strip()
            raw_run_id = recorded.get('membership_evidence_run_id')
            if raw_run_id is not None and (
                isinstance(raw_run_id, bool)
                or not isinstance(raw_run_id, int)
            ):
                raise ValueError('membership evidence run id is invalid')
            run_id = raw_run_id
            run_source = (
                str(recorded.get('membership_evidence_run_source')).strip()
                if recorded.get('membership_evidence_run_source') is not None
                else None
            )
            evidence_sha256 = (
                str(recorded.get('membership_evidence_sha256')).strip().lower()
                if recorded.get('membership_evidence_sha256') is not None
                else None
            )
            raw_batch_ids = recorded.get(
                'membership_evidence_repair_batch_ids'
            )
            evidence_batch_ids = tuple(
                sorted(
                    {
                        str(batch_id).strip()
                        for batch_id in raw_batch_ids or ()
                        if str(batch_id).strip()
                    }
                )
            )
            if raw_batch_ids is not None and (
                not isinstance(raw_batch_ids, list)
                or raw_batch_ids != list(evidence_batch_ids)
            ):
                raise ValueError(
                    'membership evidence repair batch lineage is invalid'
                )
            if evidence_source == 'stored_eod_run_metadata':
                lineage_valid = (
                    run_id is not None
                    and run_id > 0
                    and bool(run_source)
                    and evidence_sha256 is None
                    and not evidence_batch_ids
                )
            elif evidence_source == (
                PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE
            ):
                lineage_valid = (
                    run_id is None
                    and run_source == 'pykrx'
                    and evidence_sha256 is not None
                    and len(evidence_sha256) == 64
                    and all(
                        character in '0123456789abcdef'
                        for character in evidence_sha256
                    )
                    and bool(evidence_batch_ids)
                )
            else:
                lineage_valid = False
            if (
                int(recorded['exact_membership_count'])
                != len(normalized_symbols)
                or recorded['ranking_membership_sha256'] != exact_hash
                or recorded['daily_price_input_sha256'] != input_sha256
                or not lineage_valid
            ):
                raise ValueError('factor rebuild evidence scope drift')
            provider_membership_hashes.add(
                str(recorded['provider_membership_sha256']).strip().lower()
            )
            evidence_lineages.add(
                (
                    evidence_source,
                    run_id,
                    run_source,
                    evidence_sha256,
                    evidence_batch_ids,
                )
            )
            recorded_outputs = dict(recorded['stage_output_fingerprints'])
            for stage_id in FACTOR_RESULT_MODELS:
                if dict(recorded_outputs[stage_id]) != output_fingerprints[stage_id]:
                    raise ValueError('factor rebuild output evidence drift')
            repair_started_at = datetime.fromisoformat(
                str(recorded['repair_started_at'])
            )
            rebuild_completed_at = datetime.fromisoformat(
                str(recorded['rebuild_completed_at'])
            )
            stage_ids = tuple(recorded['stage_ids'])
            if (
                repair_started_at.tzinfo is None
                or rebuild_completed_at.tzinfo is None
                or rebuild_completed_at < repair_started_at
                or stage_ids != tuple(FACTOR_RESULT_MODELS)
            ):
                raise ValueError('factor rebuild completion evidence invalid')
            completion_records.add(
                (
                    repair_started_at.isoformat(),
                    rebuild_completed_at.isoformat(),
                    stage_ids,
                )
            )
        except (KeyError, TypeError, ValueError):
            metadata_verified = False
            break
    if (
        len(provider_membership_hashes) > 1
        or len(evidence_lineages) > 1
        or len(completion_records) > 1
    ):
        metadata_verified = False
    if evidence_lineages:
        authority_batch_ids = set(next(iter(evidence_lineages))[4])
        affected_batch_ids = {
            str(batch['repair_batch_id']) for batch in affected
        }
        if not authority_batch_ids.issubset(affected_batch_ids):
            metadata_verified = False
    lineage = next(
        iter(evidence_lineages),
        (None, None, None, None, ()),
    )
    completion_record = next(
        iter(completion_records),
        (None, None, ()),
    )
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
        'membership_evidence_sha256': lineage[3],
        'membership_evidence_repair_batch_ids': list(lineage[4]),
        'completion_metadata': {
            'repair_started_at': completion_record[0],
            'rebuild_completed_at': completion_record[1],
            'stage_ids': list(completion_record[2]),
        },
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
        evidence_source = str(
            result.get('membership_evidence_source') or ''
        ).strip()
        evidence_sha256 = (
            str(result.get('membership_evidence_sha256')).strip().lower()
            if result.get('membership_evidence_sha256') is not None
            else None
        )
        raw_evidence_batch_ids = result.get(
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
        if evidence_source == 'stored_eod_run_metadata':
            if (
                isinstance(run_id, bool)
                or not isinstance(run_id, int)
                or run_id <= 0
            ):
                mismatches.append('membership_evidence_run_id')
            if not run_source:
                mismatches.append('membership_evidence_run_source')
            if evidence_sha256 is not None or evidence_batch_ids:
                mismatches.append('membership_evidence_pending_lineage')
        elif evidence_source == PENDING_MISSING_BAR_MEMBERSHIP_EVIDENCE_SOURCE:
            if run_id is not None:
                mismatches.append('membership_evidence_run_id')
            if run_source != 'pykrx':
                mismatches.append('membership_evidence_run_source')
            if (
                evidence_sha256 is None
                or len(evidence_sha256) != 64
                or any(
                    character not in '0123456789abcdef'
                    for character in evidence_sha256
                )
            ):
                mismatches.append('membership_evidence_sha256')
            if (
                not isinstance(raw_evidence_batch_ids, list)
                or raw_evidence_batch_ids != evidence_batch_ids
                or not evidence_batch_ids
            ):
                mismatches.append(
                    'membership_evidence_repair_batch_ids'
                )
        else:
            mismatches.append('membership_evidence_source')
        if mismatches:
            repair_reason = str(result.get('reason') or '').strip()
            raise RuntimeError(
                'stored-only factor dry proof mismatch: '
                + ','.join(dict.fromkeys(mismatches))
                + (f'; reason={repair_reason}' if repair_reason else '')
            )
        return {
            'implementation_contract': FACTOR_REPAIR_IMPLEMENTATION_CONTRACT,
            'market_date': item['market_date'],
            'exact_symbol_count': item['symbol_count'],
            'ranking_membership_sha256': item['membership_sha256'],
            'provider_membership_count': provider_count,
            'provider_membership_sha256': provider_hash,
            'protected_provider_codes': normalized_protected_codes,
            'membership_evidence_source': evidence_source,
            'membership_evidence_run_id': run_id,
            'membership_evidence_run_source': run_source,
            'membership_evidence_sha256': evidence_sha256,
            'membership_evidence_repair_batch_ids': evidence_batch_ids,
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
            proof.get('implementation_contract')
            != FACTOR_REPAIR_IMPLEMENTATION_CONTRACT
            or result.get('implementation_contract')
            != FACTOR_REPAIR_IMPLEMENTATION_CONTRACT
        ):
            return False
        if (
            ExactFactorBulkRepair._validated_source_fingerprints(
                result.get('source_fingerprints')
            )
            is None
            or ExactFactorBulkRepair._validated_source_fingerprints(
                proof.get('source_fingerprints')
            )
            != ExactFactorBulkRepair._validated_source_fingerprints(
                result.get('source_fingerprints')
            )
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
            or result.get('membership_evidence_sha256')
            != proof.get('membership_evidence_sha256')
            or result.get('membership_evidence_repair_batch_ids')
            != proof.get('membership_evidence_repair_batch_ids')
        ):
            return False
        completion_origin = result.get('completion_origin')
        if completion_origin == FACTOR_LIVE_RECONCILIATION_ORIGIN:
            if (
                result.get('completion_schema_version')
                != FACTOR_LIVE_RECONCILIATION_SCHEMA_VERSION
                or 'stage_results' in result
            ):
                return False
            reconciliation = result.get('live_reconciliation_evidence')
            if not isinstance(reconciliation, Mapping):
                return False
            memberships = reconciliation.get('output_memberships')
            reconciled_fingerprints = reconciliation.get(
                'output_fingerprints'
            )
            completion_metadata = reconciliation.get(
                'completion_metadata'
            )
            try:
                repair_started_at = datetime.fromisoformat(
                    str(completion_metadata['repair_started_at'])
                )
                rebuild_completed_at = datetime.fromisoformat(
                    str(completion_metadata['rebuild_completed_at'])
                )
                stage_ids = list(completion_metadata['stage_ids'])
            except (KeyError, TypeError, ValueError):
                return False
            if (
                reconciliation.get('metadata_verified') is not True
                or int(reconciliation.get('pending_batch_count') or 0) <= 0
                or not isinstance(memberships, Mapping)
                or set(memberships) != set(FACTOR_RESULT_MODELS)
                or not isinstance(reconciled_fingerprints, Mapping)
                or set(reconciled_fingerprints) != set(FACTOR_RESULT_MODELS)
                or repair_started_at.tzinfo is None
                or rebuild_completed_at.tzinfo is None
                or rebuild_completed_at < repair_started_at
                or stage_ids != list(FACTOR_RESULT_MODELS)
            ):
                return False
            exact_symbols = set(item['symbols'])
            protected_symbols = set(proof['protected_provider_codes'])
            for stage_id in FACTOR_RESULT_MODELS:
                stage_membership = memberships.get(stage_id)
                if not isinstance(stage_membership, list):
                    return False
                if stage_membership != sorted(set(stage_membership)):
                    return False
                observed = set(stage_membership)
                if (
                    not exact_symbols.issubset(observed)
                    or observed.difference(exact_symbols)
                    - protected_symbols
                ):
                    return False
        else:
            stage_results = result.get('stage_results')
            if not isinstance(stage_results, Mapping) or set(
                stage_results
            ) != set(FACTOR_RESULT_MODELS):
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
                    'successful_membership_sha256': item[
                        'membership_sha256'
                    ],
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
            or completion.get('membership_evidence_sha256')
            != proof.get('membership_evidence_sha256')
            or completion.get('membership_evidence_repair_batch_ids')
            != proof.get('membership_evidence_repair_batch_ids')
            or completion.get('daily_price_input_sha256')
            != proof.get('daily_price_input_sha256')
            or not isinstance(fingerprints, Mapping)
            or set(fingerprints) != set(FACTOR_RESULT_MODELS)
        ):
            return False
        if completion_origin == FACTOR_LIVE_RECONCILIATION_ORIGIN and (
            completion.get('updated_batch_count')
            != result['live_reconciliation_evidence'].get(
                'pending_batch_count'
            )
            or fingerprints
            != result['live_reconciliation_evidence'].get(
                'output_fingerprints'
            )
        ):
            return False
        return True

    def _completed_live_state_is_valid(
        self,
        unit: Mapping[str, Any],
        item: Mapping[str, Any],
        *,
        live_state: Mapping[str, Any] | None = None,
    ) -> bool:
        live = (
            live_state
            if live_state is not None
            else self.completion_state_loader(
                str(item['market_date']),
                list(item['symbols']),
            )
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
            or live.get('membership_evidence_sha256')
            != unit['dry_proof']['membership_evidence_sha256']
            or live.get('membership_evidence_repair_batch_ids')
            != unit['dry_proof'][
                'membership_evidence_repair_batch_ids'
            ]
        ):
            return False
        expected_completion = unit['result']['completion_evidence']
        if unit['result'].get(
            'completion_origin'
        ) == FACTOR_LIVE_RECONCILIATION_ORIGIN:
            reconciliation = unit['result'].get(
                'live_reconciliation_evidence'
            )
            if (
                not isinstance(reconciliation, Mapping)
                or live.get('pending_batch_count')
                != reconciliation.get('pending_batch_count')
                or live.get('completion_metadata')
                != reconciliation.get('completion_metadata')
            ):
                return False
        expected_sources = self._validated_source_fingerprints(
            unit['result'].get('source_fingerprints')
        )
        live_sources = self._validated_source_fingerprints(
            live.get('source_fingerprints')
        )
        if expected_sources is None or live_sources != expected_sources:
            return False
        reviewed_sources = self._validated_source_fingerprints(
            unit['dry_proof'].get('source_fingerprints')
        )
        if reviewed_sources is None or live_sources != reviewed_sources:
            return False
        if (
            live.get('input_row_count')
            != unit['dry_proof'].get('daily_price_input_count')
            or live.get('input_sha256')
            != unit['dry_proof'].get('daily_price_input_sha256')
        ):
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

    def _review_date_with_state(
        self,
        item: Mapping[str, Any],
    ) -> tuple[dict[str, Any], Mapping[str, Any]]:
        self._validate_manifest_scope(item)
        result = self.repair_service.repair_for_market_date(
            market_date=str(item['market_date']),
            apply=False,
            membership_policy=MEMBERSHIP_POLICY_STORED_ONLY,
        )
        proof = self._dry_proof(result, item)
        source_state = self.completion_state_loader(
            str(item['market_date']),
            list(item['symbols']),
        )
        source_fingerprints = self._validated_source_fingerprints(
            source_state.get('source_fingerprints')
            if isinstance(source_state, Mapping)
            else None
        )
        input_row_count = (
            source_state.get('input_row_count')
            if isinstance(source_state, Mapping)
            else None
        )
        input_sha256 = str(
            source_state.get('input_sha256') or ''
            if isinstance(source_state, Mapping)
            else ''
        ).strip().lower()
        if (
            source_fingerprints is None
            or input_row_count != item['symbol_count']
            or len(input_sha256) != 64
            or any(
                character not in '0123456789abcdef'
                for character in input_sha256
            )
        ):
            raise RuntimeError(
                'factor dry proof source dependency fingerprint is invalid'
            )
        proof['source_fingerprints'] = source_fingerprints
        proof['daily_price_input_count'] = input_row_count
        proof['daily_price_input_sha256'] = input_sha256
        return proof, source_state

    def _review_date(self, item: Mapping[str, Any]) -> dict[str, Any]:
        proof, _source_state = self._review_date_with_state(item)
        return proof

    def _live_reconciliation_unit(
        self,
        *,
        item: Mapping[str, Any],
        item_sha256: str,
        dry_proof: Mapping[str, Any],
        live_state: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Bind durable factor evidence to a reviewed checkpoint on resume."""

        pending_batch_count = live_state.get('pending_batch_count')
        output_memberships = live_state.get('output_memberships')
        output_fingerprints = live_state.get('output_fingerprints')
        completion_metadata = live_state.get('completion_metadata')
        try:
            repair_started_at = datetime.fromisoformat(
                str(completion_metadata['repair_started_at'])
            )
            rebuild_completed_at = datetime.fromisoformat(
                str(completion_metadata['rebuild_completed_at'])
            )
            stage_ids = list(completion_metadata['stage_ids'])
        except (KeyError, TypeError, ValueError):
            return None
        if (
            live_state.get('metadata_verified') is not True
            or isinstance(pending_batch_count, bool)
            or not isinstance(pending_batch_count, int)
            or pending_batch_count <= 0
            or not isinstance(output_memberships, Mapping)
            or not isinstance(output_fingerprints, Mapping)
            or repair_started_at.tzinfo is None
            or rebuild_completed_at.tzinfo is None
            or rebuild_completed_at < repair_started_at
            or stage_ids != list(FACTOR_RESULT_MODELS)
        ):
            return None
        result = {
            'implementation_contract': FACTOR_REPAIR_IMPLEMENTATION_CONTRACT,
            'completion_origin': FACTOR_LIVE_RECONCILIATION_ORIGIN,
            'completion_schema_version': (
                FACTOR_LIVE_RECONCILIATION_SCHEMA_VERSION
            ),
            'market_date': item['market_date'],
            'mode': 'apply',
            'status': 'completed',
            'applied': True,
            'membership_policy': MEMBERSHIP_POLICY_STORED_ONLY,
            'exact_membership_count': item['symbol_count'],
            'ranking_membership_sha256': item['membership_sha256'],
            'membership_sha256': dry_proof[
                'provider_membership_sha256'
            ],
            'membership_evidence_source': dry_proof[
                'membership_evidence_source'
            ],
            'membership_evidence_run_id': dry_proof[
                'membership_evidence_run_id'
            ],
            'membership_evidence_run_source': dry_proof[
                'membership_evidence_run_source'
            ],
            'membership_evidence_sha256': dry_proof[
                'membership_evidence_sha256'
            ],
            'membership_evidence_repair_batch_ids': list(
                dry_proof['membership_evidence_repair_batch_ids']
            ),
            'source_fingerprints': dict(
                live_state['source_fingerprints']
            ),
            'completion_evidence': {
                'updated_batch_count': pending_batch_count,
                'exact_membership_count': item['symbol_count'],
                'ranking_membership_sha256': item['membership_sha256'],
                'provider_membership_sha256': dry_proof[
                    'provider_membership_sha256'
                ],
                'membership_evidence_source': dry_proof[
                    'membership_evidence_source'
                ],
                'membership_evidence_run_id': dry_proof[
                    'membership_evidence_run_id'
                ],
                'membership_evidence_run_source': dry_proof[
                    'membership_evidence_run_source'
                ],
                'membership_evidence_sha256': dry_proof[
                    'membership_evidence_sha256'
                ],
                'membership_evidence_repair_batch_ids': list(
                    dry_proof['membership_evidence_repair_batch_ids']
                ),
                'daily_price_input_sha256': dry_proof[
                    'daily_price_input_sha256'
                ],
                'stage_output_fingerprints': dict(output_fingerprints),
            },
            'live_reconciliation_evidence': {
                'metadata_verified': True,
                'pending_batch_count': pending_batch_count,
                'completion_metadata': {
                    'repair_started_at': repair_started_at.isoformat(),
                    'rebuild_completed_at': rebuild_completed_at.isoformat(),
                    'stage_ids': stage_ids,
                },
                'output_memberships': {
                    stage_id: list(symbols)
                    for stage_id, symbols in output_memberships.items()
                    if isinstance(symbols, list)
                },
                'output_fingerprints': dict(output_fingerprints),
            },
        }
        unit = {
            'status': 'completed',
            'item_sha256': item_sha256,
            'dry_proof': dict(dry_proof),
            'result': result,
        }
        if not self._completed_checkpoint_is_valid(unit, item):
            return None
        if not self._completed_live_state_is_valid(
            unit,
            item,
            live_state=live_state,
        ):
            return None
        return unit

    def _apply_date(
        self,
        item: Mapping[str, Any],
        dry_proof: Mapping[str, Any],
        *,
        before_state: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if (
            dry_proof.get('implementation_contract')
            != FACTOR_REPAIR_IMPLEMENTATION_CONTRACT
        ):
            raise RuntimeError(
                'factor dry proof implementation contract is stale'
            )
        if before_state is None:
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
        reviewed_sources = self._validated_source_fingerprints(
            dry_proof.get('source_fingerprints')
        )
        if reviewed_sources is None or before_sources != reviewed_sources:
            raise RuntimeError(
                'factor source dependency changed after dry review'
            )
        if (
            before_state.get('input_row_count')
            != dry_proof.get('daily_price_input_count')
            or before_state.get('input_sha256')
            != dry_proof.get('daily_price_input_sha256')
        ):
            raise RuntimeError('factor price input changed after dry review')
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
                expected_membership_evidence_source=(
                    dry_proof['membership_evidence_source']
                ),
                expected_membership_evidence_run_id=(
                    dry_proof['membership_evidence_run_id']
                ),
                expected_membership_evidence_run_source=(
                    dry_proof['membership_evidence_run_source']
                ),
                expected_membership_evidence_sha256=(
                    dry_proof['membership_evidence_sha256']
                ),
                expected_membership_evidence_repair_batch_ids=(
                    dry_proof[
                        'membership_evidence_repair_batch_ids'
                    ]
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
        result['implementation_contract'] = (
            FACTOR_REPAIR_IMPLEMENTATION_CONTRACT
        )
        completed_unit = {'dry_proof': dry_proof, 'result': result}
        if not self._completed_checkpoint_is_valid(completed_unit, item):
            raise RuntimeError(
                'factor apply did not complete reviewed exact scope: '
                f"status={result.get('status')}, "
                f"reason={result.get('reason')}"
            )
        if not self._completed_live_state_is_valid(
            completed_unit,
            item,
            live_state=after_state,
        ):
            raise RuntimeError(
                'factor apply live output/source evidence does not match result'
            )
        return result

    def _reconcile_applied_result(
        self,
        *,
        item: Mapping[str, Any],
        dry_proof: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> tuple[bool, dict[str, Any]]:
        """Re-read exact durable rows after the outer fence exit failed."""

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
        memberships = live_state.get('output_memberships')
        fingerprints = live_state.get('output_fingerprints')
        return live_valid, {
            'stage_symbol_counts': (
                {
                    stage_id: len(symbols)
                    for stage_id, symbols in memberships.items()
                    if isinstance(symbols, list)
                }
                if isinstance(memberships, Mapping)
                else None
            ),
            'output_fingerprints': (
                dict(fingerprints)
                if isinstance(fingerprints, Mapping)
                else None
            ),
        }

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
            'implementation_contract': (
                FACTOR_REPAIR_IMPLEMENTATION_CONTRACT
            ),
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

            applied_result: dict[str, Any] | None = None
            reviewed_proof: dict[str, Any] | None = None
            guard_exit_state: dict[str, Any] = {}
            guard_context = (
                capture_guard_transaction_failure(
                    self.guard_factory,
                    state=guard_exit_state,
                    market_date=date.fromisoformat(market_date),
                )
                if apply
                else self.guard_factory(
                    market_date=date.fromisoformat(market_date)
                )
            )
            with suppress(GuardTransactionExitError), guard_context as guard:
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
                                    live_state=current_state,
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
                if previous.get('status') != 'completed':
                    reconciled_unit = self._live_reconciliation_unit(
                        item=item,
                        item_sha256=item_sha256,
                        dry_proof=reviewed_proof,
                        live_state=current_state,
                    )
                    if reconciled_unit is not None:
                        checkpoint['units'][market_date] = reconciled_unit
                        try:
                            write_checkpoint(checkpoint_path, checkpoint)
                        except Exception as exc:  # noqa: BLE001
                            return {
                                **base,
                                'status': 'partial',
                                'applied': False,
                                'reason': (
                                    'checkpoint_write_failed_after_'
                                    'live_reconciliation'
                                ),
                                'market_date': market_date,
                                'error_type': type(exc).__name__,
                                'error_message': str(exc)[:500],
                                'uncertain_commit': False,
                                'checkpoint_persisted': False,
                                'reconciled_proof_count': 1,
                                'retry_required': True,
                            }
                        completed_dates += 1
                        skipped_dates += 1
                        continue
                completed_valid = False
                if (
                    previous.get('status') == 'completed'
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
                    'reason': 'factor_apply_result_missing',
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
