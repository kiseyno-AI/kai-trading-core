from __future__ import annotations

import asyncio
import hashlib
import io
import re
import zipfile
from copy import deepcopy
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from html import unescape
from typing import Any, Mapping

from sqlalchemy import and_, case, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    DartAuditSignal,
    DailyPriceBar,
    DartDilutionSignal,
    DartDisclosureEvent,
    DartFundamentalsSnapshot,
    Symbol,
)
from app.dart.sync_service import (
    _is_corporation_local_persistence_error,
    _is_systemic_provider_failure,
)
from app.eod.finality import validate_finalized_krx_write_date
from app.integrations.dart.client import DartClient, dart_client


DEFAULT_AUDIT_BACKFILL_LIMIT = 25
MAX_AUDIT_BACKFILL_LIMIT = 100
DEFAULT_AUDIT_BACKFILL_CONCURRENCY = 1
MAX_AUDIT_BACKFILL_CONCURRENCY = 8
AUDIT_BACKFILL_SYSTEMIC_FAILURE_LIMIT = 3
AUDIT_BACKFILL_SELECTION_POLICY = 'piotroski_latest_annual_audit_v1'
MAX_AUDIT_ARCHIVE_COMPRESSED_BYTES = 50 * 1024 * 1024
MAX_AUDIT_ARCHIVE_MEMBERS = 2_000
MAX_AUDIT_ARCHIVE_MEMBER_BYTES = 25 * 1024 * 1024
MAX_AUDIT_ARCHIVE_EXPANDED_BYTES = 100 * 1024 * 1024
MAX_AUDIT_DIAGNOSTIC_MEMBERS = 50
MAX_AUDIT_DIAGNOSTIC_MEMBER_NAME_CHARS = 255
MAX_AUDIT_DIAGNOSTIC_ENCODING_CHARS = 64
AUDIT_ARCHIVE_DIAGNOSTICS_VERSION = 'dart_audit_archive_diagnostics_v1'
AUDIT_AUTHORITATIVE_ABSENCE_SCHEMA_VERSION = (
    'dart_audit_authoritative_absence_v1'
)
AUDIT_AUTHORITATIVE_ABSENCE_EVIDENCE_SEMANTICS = (
    'authoritative_absence'
)
AUDIT_SIGNAL_WRITER_LOCK_KEY = 'dart_audit_signals_writer_v1'
_AUDIT_SIGNAL_WRITER_LOCK_OWNED: ContextVar[bool] = ContextVar(
    'dart_audit_signal_writer_lock_owned',
    default=False,
)


@contextmanager
def audit_signal_writer_lock_owner():
    """Allow a guarded repair to publish through its separately held lock.

    The exact repair holds the advisory transaction lock on its guard
    connection.  Its capture service persists on another connection, so that
    nested mutation must not wait on the repair's own lock.  Context ownership
    is limited to tasks spawned inside the guard; ordinary writers still take
    the database lock in ``_store_signals``.
    """

    token = _AUDIT_SIGNAL_WRITER_LOCK_OWNED.set(True)
    try:
        yield
    finally:
        _AUDIT_SIGNAL_WRITER_LOCK_OWNED.reset(token)

_AUDIT_PATTERNS = [
    (re.compile(r'의견거절'), 'disclaimer', '의견거절'),
    (re.compile(r'부적정의견|부적정'), 'adverse', '부적정'),
    (re.compile(r'한정의견|한정'), 'qualified', '한정'),
    (re.compile(r'적정의견|적정'), 'unqualified', '적정'),
]
_SEMANTIC_AUDIT_PATTERNS = [
    (
        re.compile(r'의견\s*거절|(?<![가-힣A-Za-z0-9])거절(?![가-힣A-Za-z0-9])'),
        'disclaimer',
        '의견거절',
    ),
    (re.compile(r'부적정의견|부적정'), 'adverse', '부적정'),
    (re.compile(r'한정의견|한정'), 'qualified', '한정'),
    (re.compile(r'적정의견|적정'), 'unqualified', '적정'),
]
_AUDIT_OPINION_SEVERITY = {
    'unqualified': 0,
    'qualified': 1,
    'adverse': 2,
    'disclaimer': 3,
}
_AUDIT_NEGATIVE_AUTHORITY_SENTINELS = {
    'legacy_aud_opn_period_without_current_v1',
    'semantic_audit_table_current_period_absent_v1',
}
_AUDIT_AMBIGUOUS_AUTHORITY_SENTINELS = {
    'legacy_aud_opn_markerless_conflict_v1',
    'semantic_audit_table_current_opinion_unrecognized_v1',
    'semantic_audit_table_current_period_ambiguous_v1',
}

_AUDIT_TABLE_PATTERN = re.compile(
    r'<TABLE-GROUP\b[^>]*\bACLASS\s*=\s*["\']AUD_OPN["\'][^>]*>'
    r'(.*?)</TABLE-GROUP>',
    re.IGNORECASE | re.DOTALL,
)
_PLAIN_TABLE_PATTERN = re.compile(
    r'<TABLE(?=[\s>])[^>]*>(.*?)</TABLE>',
    re.IGNORECASE | re.DOTALL,
)
_AUDIT_ROW_PATTERN = re.compile(r'<TR[^>]*>(.*?)</TR>', re.IGNORECASE | re.DOTALL)
_AUDIT_CELL_PATTERN = re.compile(r'<T[EDHU][^>]*>(.*?)</T[EDHU]>', re.IGNORECASE | re.DOTALL)
_TAG_PATTERN = re.compile(r'<[^>]+>')
_WS_PATTERN = re.compile(r'\s+')
_CURRENT_PERIOD_PATTERN = re.compile(
    r'[\(（]\s*당\s*기\s*[\)）]'
)
_EXPLICIT_PERIOD_PATTERN = re.compile(
    r'[\(（]\s*(?:당\s*기|전\s*기|전\s*전\s*기)\s*[\)）]'
)
_SEMANTIC_AUDIT_HEADERS = (
    '사업연도',
    '감사인',
    '감사의견',
    '핵심감사사항',
)
_ENCODING_DECLARATION_PATTERN = re.compile(
    r'(?:encoding|charset)\s*=\s*["\']?\s*([A-Za-z0-9._-]+)',
    re.IGNORECASE,
)
_ENCODING_ALIASES = {
    'cp949': 'cp949',
    'euc-kr': 'euc-kr',
    'euckr': 'euc-kr',
    'ks_c_5601-1987': 'euc-kr',
    'ks_c_5601-1989': 'euc-kr',
    'ksc5601': 'euc-kr',
    'ms949': 'cp949',
    'uhc': 'cp949',
    'utf-8': 'utf-8',
    'utf8': 'utf-8',
}

_DILUTION_RULES = [
    ('유상증자', 'paid_increase', 'equity'),
    ('전환사채', 'convertible_bond', 'cb'),
    ('신주인수권부사채', 'bond_with_warrant', 'bw'),
    ('교환사채', 'exchange_bond', 'eb'),
]
SHARE_ISSUANCE_POSITIVE_SIGNAL_TYPES = tuple(
    signal_type for _token, signal_type, _security_type in _DILUTION_RULES
)

SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS = 365
SHARE_ISSUANCE_NEGATIVE_COVERAGE_SCHEMA = 'dart_share_issuance_coverage_v1'
SHARE_ISSUANCE_RULE_VERSION = 'dart_share_issuance_rules_v1'
SHARE_ISSUANCE_EVIDENCE_SOURCE = 'dart_disclosure_events'
SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION = (
    'krx_post_close_snapshot_overlap_v1'
)
SHARE_ISSUANCE_LATE_ARRIVAL_POLICY = 'next_scan_one_day_overlap_v1'
_POSITIVE_DATE_NOT_LOADED = object()


@dataclass
class ParsedAuditSignal:
    opinion_code: str | None
    opinion_label: str | None
    has_audit_issue: bool
    source_section: str | None
    evidence_text: str | None
    payload: dict[str, Any] | None


_AUTHORITATIVE_ABSENCE_PAYLOAD_FIELDS = frozenset(
    {
        'schema_version',
        'evidence_semantics',
        'audit_signal_evaluated',
        'event_id',
        'rcept_no',
        'receipt_date',
        'audit_diagnostics',
    }
)


def _canonical_audit_receipt_date(value: Any) -> str | None:
    if isinstance(value, datetime):
        return None
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str):
        return None
    try:
        canonical = date.fromisoformat(value).isoformat()
    except ValueError:
        return None
    return canonical if value == canonical else None


def is_authoritative_absence_audit_payload(
    payload: Any,
    *,
    event_id: int | None = None,
    rcept_no: str | None = None,
    receipt_date: date | str | None = None,
) -> bool:
    if not isinstance(payload, Mapping):
        return False
    if set(payload) != _AUTHORITATIVE_ABSENCE_PAYLOAD_FIELDS:
        return False
    if (
        payload.get('schema_version')
        != AUDIT_AUTHORITATIVE_ABSENCE_SCHEMA_VERSION
        or payload.get('evidence_semantics')
        != AUDIT_AUTHORITATIVE_ABSENCE_EVIDENCE_SEMANTICS
        or payload.get('audit_signal_evaluated') is not True
    ):
        return False

    stored_event_id = payload.get('event_id')
    if (
        isinstance(stored_event_id, bool)
        or not isinstance(stored_event_id, int)
        or stored_event_id <= 0
    ):
        return False
    stored_rcept_no = payload.get('rcept_no')
    if (
        not isinstance(stored_rcept_no, str)
        or not stored_rcept_no
        or len(stored_rcept_no) > 32
    ):
        return False
    if event_id is not None and stored_event_id != event_id:
        return False
    if rcept_no is not None and stored_rcept_no != rcept_no:
        return False
    stored_receipt_date = payload.get('receipt_date')
    if (
        not isinstance(stored_receipt_date, str)
        or _canonical_audit_receipt_date(stored_receipt_date)
        != stored_receipt_date
    ):
        return False
    if receipt_date is not None:
        expected_receipt_date = _canonical_audit_receipt_date(receipt_date)
        if (
            expected_receipt_date is None
            or stored_receipt_date != expected_receipt_date
        ):
            return False

    diagnostics = payload.get('audit_diagnostics')
    if not _has_complete_audit_authority_evaluation(diagnostics):
        return False
    if diagnostics.get('version') != AUDIT_ARCHIVE_DIAGNOSTICS_VERSION:
        return False
    archive_sha256 = diagnostics.get('archive_sha256')
    if (
        not isinstance(archive_sha256, str)
        or re.fullmatch(r'[0-9a-f]{64}', archive_sha256) is None
    ):
        return False
    archive_size_bytes = diagnostics.get('archive_size_bytes')
    if (
        isinstance(archive_size_bytes, bool)
        or not isinstance(archive_size_bytes, int)
        or archive_size_bytes <= 0
    ):
        return False
    return True


def claims_authoritative_absence_audit_payload(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    # Every field in this envelope is reserved for the durable negative
    # sentinel.  Treat even a partly corrupted envelope as a sentinel claim so
    # it cannot downgrade into an apparently clean parsed opinion.
    return bool(set(payload).intersection(_AUTHORITATIVE_ABSENCE_PAYLOAD_FIELDS))


def is_authoritative_absence_audit_signal(
    signal: ParsedAuditSignal | Mapping[str, Any] | Any,
    *,
    event_id: int | None = None,
    rcept_no: str | None = None,
    receipt_date: date | str | None = None,
) -> bool:
    def field(name: str) -> Any:
        if isinstance(signal, Mapping):
            return signal.get(name)
        return getattr(signal, name, None)

    return (
        is_authoritative_absence_audit_payload(
            field('payload'),
            event_id=event_id,
            rcept_no=rcept_no,
            receipt_date=receipt_date,
        )
        and field('opinion_code') is None
        and field('opinion_label') is None
        and field('has_audit_issue') is False
        and field('source_section') is None
        and field('evidence_text') is None
    )


_PARSED_AUDIT_OPINION_ISSUE = {
    'unqualified': False,
    'qualified': True,
    'adverse': True,
    'disclaimer': True,
}
_PARSED_AUDIT_OPINION_LABEL = {
    'unqualified': '적정',
    'qualified': '한정',
    'adverse': '부적정',
    'disclaimer': '의견거절',
}
_PARSED_AUDIT_PAYLOAD_PARSERS = frozenset(
    {
        'semantic_audit_table_v1',
        'legacy_aud_opn_explicit_current_v1',
        'legacy_aud_opn_markerless_v1',
    }
)
_PARSED_AUDIT_SELECTION_FIELDS = frozenset(
    {
        'selection_policy',
        'evidence_count',
        'opinion_codes',
        'conflicting_opinion_codes',
        'findings_by_opinion',
        'source_sections',
        'selected_source_section',
    }
)


def _is_nonempty_audit_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_nonempty_audit_text_list(value: Any) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, str) for item in value)
        and any(item.strip() for item in value)
    )


def _has_durable_parsed_audit_payload(
    payload: Any,
    *,
    opinion_code: str,
    source_section: str,
) -> bool:
    if not isinstance(payload, Mapping):
        return False
    parser = payload.get('parser')
    if parser is not None and (
        not isinstance(parser, str)
        or parser not in _PARSED_AUDIT_PAYLOAD_PARSERS
    ):
        return False
    opinion_cell = payload.get('opinion_cell')
    if not _is_nonempty_audit_text(opinion_cell):
        return False
    evidence_cells = payload.get('row_cells')
    if parser == 'semantic_audit_table_v1' and evidence_cells is None:
        evidence_cells = payload.get('column_cells')
    if (
        not _is_nonempty_audit_text_list(evidence_cells)
        or opinion_cell not in evidence_cells
    ):
        return False
    parsed_opinion_code = next(
        (
            code
            for pattern, code, _label in _SEMANTIC_AUDIT_PATTERNS
            if pattern.search(opinion_cell)
        ),
        None,
    )
    if parsed_opinion_code != opinion_code:
        return False

    present_selection_fields = set(payload).intersection(
        _PARSED_AUDIT_SELECTION_FIELDS
    )
    if not present_selection_fields:
        return True
    if present_selection_fields != _PARSED_AUDIT_SELECTION_FIELDS:
        return False
    if (
        payload.get('selection_policy')
        != 'most_conservative_audit_opinion_v1'
    ):
        return False
    evidence_count = payload.get('evidence_count')
    opinion_codes = payload.get('opinion_codes')
    conflicting_codes = payload.get('conflicting_opinion_codes')
    findings_by_opinion = payload.get('findings_by_opinion')
    source_sections = payload.get('source_sections')
    valid_opinion_codes = (
        isinstance(opinion_codes, list)
        and bool(opinion_codes)
        and all(
            isinstance(code, str)
            and code in _PARSED_AUDIT_OPINION_ISSUE
            for code in opinion_codes
        )
    )
    valid_source_sections = (
        _is_nonempty_audit_text_list(source_sections)
        and all(section.strip() for section in source_sections)
    )
    canonical_opinion_codes = (
        sorted(
            set(opinion_codes),
            key=lambda code: (-_AUDIT_OPINION_SEVERITY[code], code),
        )
        if valid_opinion_codes
        else []
    )
    if (
        isinstance(evidence_count, bool)
        or not isinstance(evidence_count, int)
        or evidence_count <= 0
        or not valid_opinion_codes
        or opinion_codes != canonical_opinion_codes
        or opinion_code != opinion_codes[0]
        or len(opinion_codes) != len(set(opinion_codes))
        or conflicting_codes != (opinion_codes if len(opinion_codes) > 1 else [])
        or not isinstance(findings_by_opinion, Mapping)
        or set(findings_by_opinion) != set(opinion_codes)
        or any(
            isinstance(count, bool)
            or not isinstance(count, int)
            or count <= 0
            for count in findings_by_opinion.values()
        )
        or sum(findings_by_opinion.values()) != evidence_count
        or not valid_source_sections
        or source_sections != sorted(set(source_sections))
        or source_section not in source_sections
        or payload.get('selected_source_section') != source_section
    ):
        return False
    return True


def classify_durable_audit_signal(
    signal: ParsedAuditSignal | Mapping[str, Any] | Any,
    *,
    event_id: int | None = None,
    rcept_no: str | None = None,
    receipt_date: date | str | None = None,
) -> str | None:
    def field(name: str) -> Any:
        if isinstance(signal, Mapping):
            return signal.get(name)
        return getattr(signal, name, None)

    payload = field('payload')
    if is_authoritative_absence_audit_signal(
        signal,
        event_id=event_id,
        rcept_no=rcept_no,
        receipt_date=receipt_date,
    ):
        return 'authoritative_absence'
    if claims_authoritative_absence_audit_payload(payload):
        return None
    opinion_code = str(field('opinion_code') or '').strip()
    expected_issue = _PARSED_AUDIT_OPINION_ISSUE.get(opinion_code)
    if (
        expected_issue is None
        or field('has_audit_issue') is not expected_issue
        or field('opinion_label')
        != _PARSED_AUDIT_OPINION_LABEL[opinion_code]
        or not _is_nonempty_audit_text(field('source_section'))
        or not _is_nonempty_audit_text(field('evidence_text'))
        or not _has_durable_parsed_audit_payload(
            payload,
            opinion_code=opinion_code,
            source_section=str(field('source_section')),
        )
    ):
        return None
    return 'parsed_opinion'


def build_authoritative_absence_audit_signal(
    event: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
) -> ParsedAuditSignal:
    payload = {
        'schema_version': AUDIT_AUTHORITATIVE_ABSENCE_SCHEMA_VERSION,
        'evidence_semantics': (
            AUDIT_AUTHORITATIVE_ABSENCE_EVIDENCE_SEMANTICS
        ),
        'audit_signal_evaluated': True,
        'event_id': event.get('id'),
        'rcept_no': event.get('rcept_no'),
        'receipt_date': _canonical_audit_receipt_date(
            event.get('receipt_date')
        ),
        'audit_diagnostics': deepcopy(dict(diagnostics)),
    }
    if not is_authoritative_absence_audit_payload(payload):
        raise ValueError(
            'authoritative audit absence requires exact event identity and '
            'complete versioned diagnostics'
        )
    return ParsedAuditSignal(
        opinion_code=None,
        opinion_label=None,
        has_audit_issue=False,
        source_section=None,
        evidence_text=None,
        payload=payload,
    )


class DartSignalsRepository:
    def list_missing_annual_audit_signal_candidates(
        self,
        *,
        market_date: date,
        limit: int,
        after_event_id: int = 0,
    ) -> list[dict[str, Any]]:
        latest_filing_per_year_rank = func.row_number().over(
            partition_by=(
                DartFundamentalsSnapshot.stock_code,
                DartFundamentalsSnapshot.business_year,
            ),
            order_by=(
                DartDisclosureEvent.receipt_date.desc(),
                DartDisclosureEvent.rcept_no.desc(),
                case(
                    (DartFundamentalsSnapshot.fs_div == 'CFS', 0),
                    else_=1,
                ).asc(),
                DartFundamentalsSnapshot.id.desc(),
            ),
        ).label('filing_rank')
        ranked_snapshots = (
            select(
                DartFundamentalsSnapshot.id.label('snapshot_id'),
                DartFundamentalsSnapshot.disclosure_event_id.label('event_id'),
                DartFundamentalsSnapshot.stock_code.label('stock_code'),
                DartFundamentalsSnapshot.business_year.label('business_year'),
                DartFundamentalsSnapshot.fs_div.label('fs_div'),
                DartFundamentalsSnapshot.rcept_no.label('rcept_no'),
                DartDisclosureEvent.receipt_date.label('receipt_date'),
                DartDisclosureEvent.report_nm.label('report_nm'),
                latest_filing_per_year_rank,
            )
            .join(
                DartDisclosureEvent,
                DartDisclosureEvent.id == DartFundamentalsSnapshot.disclosure_event_id,
            )
            .join(Symbol, Symbol.symbol == DartFundamentalsSnapshot.stock_code)
            .join(
                DailyPriceBar,
                and_(
                    DailyPriceBar.symbol_id == Symbol.id,
                    DailyPriceBar.trade_date == market_date,
                ),
            )
            .where(
                DartFundamentalsSnapshot.rcept_no == DartDisclosureEvent.rcept_no,
                DartFundamentalsSnapshot.stock_code == DartDisclosureEvent.stock_code,
                DartFundamentalsSnapshot.business_year <= market_date.year,
                DartFundamentalsSnapshot.report_code == '11011',
                DartFundamentalsSnapshot.fs_div.in_(['CFS', 'OFS']),
                DartFundamentalsSnapshot.source_status == '000',
                DartDisclosureEvent.fundamentals_capture_status == 'captured',
                DartDisclosureEvent.receipt_date.is_not(None),
                DartDisclosureEvent.receipt_date <= market_date,
            )
            .subquery()
        )
        selected_filing_per_year = (
            select(ranked_snapshots)
            .where(ranked_snapshots.c.filing_rank == 1)
            .subquery()
        )
        latest_business_year_rank = func.row_number().over(
            partition_by=selected_filing_per_year.c.stock_code,
            order_by=(
                selected_filing_per_year.c.business_year.desc(),
                selected_filing_per_year.c.receipt_date.desc(),
                selected_filing_per_year.c.rcept_no.desc(),
                selected_filing_per_year.c.snapshot_id.desc(),
            ),
        ).label('business_year_rank')
        selected_latest_annual = (
            select(selected_filing_per_year, latest_business_year_rank)
            .subquery()
        )
        stmt = (
            select(
                selected_latest_annual,
                DartAuditSignal.id.label('audit_signal_id'),
                DartAuditSignal.opinion_code.label('audit_opinion_code'),
                DartAuditSignal.opinion_label.label('audit_opinion_label'),
                DartAuditSignal.has_audit_issue.label(
                    'audit_has_audit_issue'
                ),
                DartAuditSignal.source_section.label(
                    'audit_source_section'
                ),
                DartAuditSignal.evidence_text.label(
                    'audit_evidence_text'
                ),
                DartAuditSignal.payload.label('audit_payload'),
            )
            .outerjoin(
                DartAuditSignal,
                and_(
                    DartAuditSignal.disclosure_event_id
                    == selected_latest_annual.c.event_id,
                    DartAuditSignal.rcept_no
                    == selected_latest_annual.c.rcept_no,
                    DartAuditSignal.stock_code
                    == selected_latest_annual.c.stock_code,
                    DartAuditSignal.receipt_date
                    == selected_latest_annual.c.receipt_date,
                ),
            )
            .where(
                selected_latest_annual.c.business_year_rank == 1,
            )
            .order_by(selected_latest_annual.c.event_id.asc())
        )

        with session_scope() as session:
            candidates: list[dict[str, Any]] = []
            scan_after_event_id = after_event_id
            while len(candidates) < limit:
                scan_limit = limit
                rows = (
                    session.execute(
                        stmt.where(
                            selected_latest_annual.c.event_id
                            > scan_after_event_id
                        ).limit(scan_limit)
                    )
                    .mappings()
                    .all()
                )
                if not rows:
                    break
                scan_after_event_id = int(rows[-1]['event_id'])
                for row in rows:
                    audit_signal = None
                    if row['audit_signal_id'] is not None:
                        audit_signal = {
                            'opinion_code': row['audit_opinion_code'],
                            'opinion_label': row['audit_opinion_label'],
                            'has_audit_issue': row[
                                'audit_has_audit_issue'
                            ],
                            'source_section': row['audit_source_section'],
                            'evidence_text': row['audit_evidence_text'],
                            'payload': row['audit_payload'],
                        }
                    durable_classification = classify_durable_audit_signal(
                        audit_signal,
                        event_id=int(row['event_id']),
                        rcept_no=str(row['rcept_no']),
                        receipt_date=row['receipt_date'],
                    )
                    if durable_classification is not None:
                        continue
                    candidates.append(
                        {
                            'event_id': int(row['event_id']),
                            'snapshot_id': int(row['snapshot_id']),
                            'rcept_no': str(row['rcept_no']),
                            'stock_code': str(row['stock_code']),
                            'business_year': int(row['business_year']),
                            'fs_div': str(row['fs_div']),
                            'receipt_date': row[
                                'receipt_date'
                            ].isoformat(),
                            'report_nm': str(row['report_nm']),
                        }
                    )
                    if len(candidates) == limit:
                        break
                if len(rows) < scan_limit:
                    break
            return candidates

    def list_recent_share_issuance_scan_evidence(
        self,
        *,
        stock_code: str,
        corp_code: str,
        before_date: date,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        stmt = (
            select(
                DartDilutionSignal.id.label('id'),
                DartDilutionSignal.receipt_date.label('receipt_date'),
                DartDilutionSignal.payload.label('payload'),
            )
            .where(
                DartDilutionSignal.stock_code == stock_code,
                DartDilutionSignal.corp_code == corp_code,
                DartDilutionSignal.signal_type == 'share_issuance',
                DartDilutionSignal.has_dilution_signal.is_(False),
                DartDilutionSignal.receipt_date.is_not(None),
                DartDilutionSignal.receipt_date < before_date,
            )
            .order_by(
                DartDilutionSignal.receipt_date.desc(),
                DartDilutionSignal.id.desc(),
            )
            .limit(limit)
        )
        with session_scope() as session:
            return [dict(row) for row in session.execute(stmt).mappings().all()]

    def get_latest_share_issuance_positive_event_date(
        self,
        *,
        stock_code: str,
        corp_code: str,
        on_or_before: date,
    ) -> date | None:
        stmt = select(func.max(DartDilutionSignal.receipt_date)).where(
            DartDilutionSignal.stock_code == stock_code,
            DartDilutionSignal.corp_code == corp_code,
            DartDilutionSignal.signal_type.in_(
                SHARE_ISSUANCE_POSITIVE_SIGNAL_TYPES
            ),
            DartDilutionSignal.has_dilution_signal.is_(True),
            DartDilutionSignal.receipt_date.is_not(None),
            DartDilutionSignal.receipt_date <= on_or_before,
        )
        with session_scope() as session:
            return session.execute(stmt).scalar_one_or_none()

    def list_current_share_issuance_coverage(
        self,
        *,
        corp_codes: list[str],
        on_or_before: date,
    ) -> list[dict[str, Any]]:
        if not corp_codes:
            return []
        stmt = (
            select(
                DartDilutionSignal.id.label('id'),
                DartDilutionSignal.corp_code.label('corp_code'),
                DartDilutionSignal.stock_code.label('stock_code'),
                DartDilutionSignal.receipt_date.label('receipt_date'),
                DartDilutionSignal.payload.label('payload'),
            )
            .where(
                DartDilutionSignal.corp_code.in_(corp_codes),
                DartDilutionSignal.rcept_no.like('DILCOV-%'),
                DartDilutionSignal.signal_type == 'share_issuance',
                DartDilutionSignal.has_dilution_signal.is_(False),
                DartDilutionSignal.receipt_date.is_not(None),
                DartDilutionSignal.receipt_date <= on_or_before,
            )
        )
        with session_scope() as session:
            return [dict(row) for row in session.execute(stmt).mappings().all()]

    def list_latest_share_issuance_positive_event_dates(
        self,
        *,
        corp_codes: list[str],
        on_or_before: date,
    ) -> dict[str, date]:
        if not corp_codes:
            return {}
        stmt = (
            select(
                DartDilutionSignal.corp_code,
                func.max(DartDilutionSignal.receipt_date).label(
                    'latest_receipt_date'
                ),
            )
            .where(
                DartDilutionSignal.corp_code.in_(corp_codes),
                DartDilutionSignal.signal_type.in_(
                    SHARE_ISSUANCE_POSITIVE_SIGNAL_TYPES
                ),
                DartDilutionSignal.has_dilution_signal.is_(True),
                DartDilutionSignal.receipt_date.is_not(None),
                DartDilutionSignal.receipt_date <= on_or_before,
            )
            .group_by(DartDilutionSignal.corp_code)
        )
        with session_scope() as session:
            return {
                str(row.corp_code): row.latest_receipt_date
                for row in session.execute(stmt).all()
                if row.latest_receipt_date is not None
            }


class DartSignalsService:
    def __init__(
        self,
        client: DartClient | None = None,
        repository: DartSignalsRepository | None = None,
        event_timeout_seconds: int | None = None,
    ) -> None:
        self.client = client or dart_client
        self.repository = repository or DartSignalsRepository()
        self.event_timeout_seconds = (
            event_timeout_seconds
            if event_timeout_seconds is not None
            else settings.dart_fundamentals_event_timeout_seconds
        )

    async def capture_signals_for_event(self, *, rcept_no: str) -> dict[str, Any]:
        event = self._load_event(rcept_no)
        audit_signal, audit_diagnostics = (
            await self._extract_audit_signal_with_diagnostics(event)
        )
        dilution_signal = self._extract_dilution_signal(event)
        authoritative_absence = audit_signal is None and (
            _has_complete_audit_authority_evaluation(audit_diagnostics)
        )
        if authoritative_absence:
            audit_signal = build_authoritative_absence_audit_signal(
                event,
                audit_diagnostics,
            )
        audit_signal_evaluated = audit_signal is not None
        self._store_signals(
            event=event,
            audit_signal=audit_signal,
            dilution_signal=dilution_signal,
            audit_signal_evaluated=audit_signal_evaluated,
        )
        result = {
            'rcept_no': rcept_no,
            'audit_signal': audit_signal.__dict__ if audit_signal else None,
            'audit_signal_evaluated': audit_signal_evaluated,
            'dilution_signal': dilution_signal,
        }
        if (
            (audit_signal is None or authoritative_absence)
            and audit_diagnostics is not None
        ):
            result['audit_diagnostics'] = audit_diagnostics
        return result

    async def capture_audit_signal_for_event(self, *, rcept_no: str) -> dict[str, Any]:
        event = self._load_event(rcept_no)
        audit_signal, audit_diagnostics = (
            await self._extract_audit_signal_with_diagnostics(event)
        )
        authoritative_absence = audit_signal is None and (
            _has_complete_audit_authority_evaluation(audit_diagnostics)
        )
        if authoritative_absence:
            audit_signal = build_authoritative_absence_audit_signal(
                event,
                audit_diagnostics,
            )
        audit_signal_evaluated = audit_signal is not None
        self._store_signals(
            event=event,
            audit_signal=audit_signal,
            dilution_signal=None,
            audit_signal_evaluated=audit_signal_evaluated,
        )
        result = {
            'rcept_no': rcept_no,
            'audit_signal': audit_signal.__dict__ if audit_signal else None,
            'audit_signal_evaluated': audit_signal_evaluated,
        }
        if (
            (audit_signal is None or authoritative_absence)
            and audit_diagnostics is not None
        ):
            result['audit_diagnostics'] = audit_diagnostics
        return result

    async def backfill_missing_annual_audit_signals(
        self,
        *,
        market_date: date,
        limit: int = DEFAULT_AUDIT_BACKFILL_LIMIT,
        after_event_id: int = 0,
        concurrency: int = DEFAULT_AUDIT_BACKFILL_CONCURRENCY,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if not 1 <= limit <= MAX_AUDIT_BACKFILL_LIMIT:
            raise ValueError(
                f'limit must be between 1 and {MAX_AUDIT_BACKFILL_LIMIT}'
            )
        if after_event_id < 0:
            raise ValueError('after_event_id must be greater than or equal to 0')
        if not 1 <= concurrency <= MAX_AUDIT_BACKFILL_CONCURRENCY:
            raise ValueError(
                'concurrency must be between 1 and '
                f'{MAX_AUDIT_BACKFILL_CONCURRENCY}'
            )
        if self.event_timeout_seconds <= 0:
            raise ValueError('event_timeout_seconds must be greater than 0')

        candidates_with_sentinel = self.repository.list_missing_annual_audit_signal_candidates(
            market_date=market_date,
            limit=limit + 1,
            after_event_id=after_event_id,
        )
        page_has_more = len(candidates_with_sentinel) > limit
        candidates = candidates_with_sentinel[:limit]
        preview_last_event_id = (
            int(candidates[-1]['event_id']) if candidates else after_event_id
        )
        if dry_run:
            return {
                'status': 'dry_run',
                'dry_run': True,
                'market_date': market_date.isoformat(),
                'selection_policy': AUDIT_BACKFILL_SELECTION_POLICY,
                'limit': limit,
                'concurrency': concurrency,
                'after_event_id': after_event_id,
                # Inspection must never hand callers a cursor that skips work.
                'next_after_event_id': after_event_id,
                # This is a preview-only forward-scan cursor.  It may be used
                # to inspect later pages, but not as proof that this page was
                # repaired.
                'next_forward_after_event_id': preview_last_event_id,
                'has_more_after_cursor': page_has_more,
                'forward_scan_has_more': page_has_more,
                'candidate_count': len(candidates),
                'candidates_attempted': 0,
                'audit_signals_captured': 0,
                'audit_signals_authoritatively_absent': 0,
                'audit_evidence_missing': 0,
                'events_failed': 0,
                'retry_required': False,
                'retry_event_ids': [],
                'failed_event_ids': [],
                'audit_evidence_missing_event_ids': [],
                'unattempted_event_ids': [],
                'circuit_breaker': {
                    'tripped': False,
                    'threshold': AUDIT_BACKFILL_SYSTEMIC_FAILURE_LIMIT,
                    'consecutive_systemic_failures': 0,
                },
                'results': [
                    {**candidate, 'status': 'pending'} for candidate in candidates
                ],
            }

        captured_count = 0
        authoritative_absence_count = 0
        missing_count = 0
        failed_count = 0
        retry_event_ids: list[int] = []
        failed_event_ids: list[int] = []
        missing_evidence_event_ids: list[int] = []
        unattempted_event_ids: list[int] = []
        consecutive_systemic_failures = 0
        circuit_breaker_tripped = False
        last_attempted_event_id = after_event_id
        results: list[dict[str, Any]] = []

        async def attempt_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
            result_item = dict(candidate)

            async def capture_with_origin() -> tuple[
                dict[str, Any] | None,
                Exception | None,
            ]:
                """Keep provider failures distinct from our per-event deadline."""
                try:
                    return (
                        await self.capture_audit_signal_for_event(
                            rcept_no=str(candidate['rcept_no']),
                        ),
                        None,
                    )
                except Exception as exc:
                    # DartClient transport timeouts and provider error
                    # envelopes are systemic candidates.  Returning the
                    # original exception lets the outer wait_for TimeoutError
                    # mean only that this one event exhausted its bounded
                    # processing deadline.
                    return None, exc

            try:
                capture_result, capture_error = await asyncio.wait_for(
                    capture_with_origin(),
                    timeout=self.event_timeout_seconds,
                )
            except TimeoutError:
                result_item.update(
                    {
                        'status': 'failed',
                        # The provider client has its own shorter transport
                        # timeout and retry policy.  Reaching this outer
                        # deadline is therefore event-local retry debt (for
                        # example, one unusually large document), not proof
                        # that DART is unavailable for every event.
                        'systemic_provider_failure': False,
                        'event_timeout': True,
                        'error': (
                            'TimeoutError: audit event processing exceeded '
                            f'{self.event_timeout_seconds}s deadline'
                        )[:500],
                    }
                )
            else:
                if capture_error is not None:
                    result_item.update(
                        {
                            'status': 'failed',
                            'systemic_provider_failure': (
                                _is_systemic_provider_failure(capture_error)
                            ),
                            'event_timeout': False,
                            'error': (
                                f'{type(capture_error).__name__}: '
                                f'{capture_error}'
                            )[:500],
                        }
                    )
                elif capture_result is None:
                    result_item.update(
                        {
                            'status': 'failed',
                            'systemic_provider_failure': False,
                            'event_timeout': False,
                            'error': (
                                'RuntimeError: audit capture returned no result'
                            ),
                        }
                    )
                elif capture_result.get('audit_signal'):
                    audit_signal = capture_result['audit_signal']
                    payload = (
                        audit_signal.get('payload')
                        if isinstance(audit_signal, Mapping)
                        else None
                    )
                    classification = classify_durable_audit_signal(
                        audit_signal,
                        event_id=int(candidate['event_id']),
                        rcept_no=str(candidate['rcept_no']),
                        receipt_date=candidate.get('receipt_date'),
                    )
                    if classification == 'authoritative_absence':
                        result_item.update(
                            {
                                'status': 'authoritative_absence',
                                'opinion_code': None,
                                'has_audit_issue': False,
                            }
                        )
                    elif classification != 'parsed_opinion':
                        result_item['status'] = 'audit_evidence_missing'
                        if isinstance(payload, Mapping):
                            audit_diagnostics = payload.get(
                                'audit_diagnostics'
                            )
                            if isinstance(audit_diagnostics, dict):
                                result_item['audit_diagnostics'] = (
                                    audit_diagnostics
                                )
                    else:
                        result_item.update(
                            {
                                'status': 'captured',
                                'opinion_code': audit_signal.get(
                                    'opinion_code'
                                ),
                                'has_audit_issue': audit_signal.get(
                                    'has_audit_issue'
                                ),
                            }
                        )
                else:
                    result_item['status'] = 'audit_evidence_missing'
                    audit_diagnostics = capture_result.get(
                        'audit_diagnostics'
                    )
                    if isinstance(audit_diagnostics, dict):
                        result_item['audit_diagnostics'] = audit_diagnostics
            return result_item

        for wave_start in range(0, len(candidates), concurrency):
            wave = candidates[wave_start : wave_start + concurrency]
            wave_results = await asyncio.gather(
                *(attempt_candidate(candidate) for candidate in wave)
            )
            last_attempted_event_id = int(wave[-1]['event_id'])
            for result_item in wave_results:
                event_id = int(result_item['event_id'])
                status = result_item['status']
                if status == 'failed':
                    failed_count += 1
                    retry_event_ids.append(event_id)
                    failed_event_ids.append(event_id)
                    # A full wave is already in flight.  Latch the first
                    # ordered threshold breach while still accounting for
                    # every result from that wave exactly once.
                    if not circuit_breaker_tripped:
                        if result_item['systemic_provider_failure']:
                            consecutive_systemic_failures += 1
                        else:
                            consecutive_systemic_failures = 0
                        if (
                            consecutive_systemic_failures
                            >= AUDIT_BACKFILL_SYSTEMIC_FAILURE_LIMIT
                        ):
                            circuit_breaker_tripped = True
                else:
                    if not circuit_breaker_tripped:
                        consecutive_systemic_failures = 0
                    if status == 'captured':
                        captured_count += 1
                    elif status == 'authoritative_absence':
                        authoritative_absence_count += 1
                    else:
                        missing_count += 1
                        retry_event_ids.append(event_id)
                        missing_evidence_event_ids.append(event_id)
                results.append(result_item)
            if circuit_breaker_tripped:
                unattempted_event_ids = [
                    int(item['event_id'])
                    for item in candidates[wave_start + len(wave) :]
                ]
                break

        has_more_after_cursor = page_has_more or bool(unattempted_event_ids)
        retry_required = bool(retry_event_ids)
        if has_more_after_cursor:
            status = 'partial'
        elif retry_required:
            status = 'completed_with_gaps'
        else:
            status = 'completed'
        next_after_event_id = (
            after_event_id if retry_required else last_attempted_event_id
        )
        return {
            'status': status,
            'dry_run': False,
            'market_date': market_date.isoformat(),
            'selection_policy': AUDIT_BACKFILL_SELECTION_POLICY,
            'limit': limit,
            'concurrency': concurrency,
            'after_event_id': after_event_id,
            # Successful rows disappear from the candidate query.  Holding the
            # input cursor on any gap makes the next invocation retry only the
            # unresolved rows without skipping a lower event id.
            'next_after_event_id': next_after_event_id,
            # Forward scanning is intentionally separate from convergence.
            # Operators can continue from the last attempted row, then finish
            # with after_event_id=0 so unresolved gaps are retried.
            'next_forward_after_event_id': last_attempted_event_id,
            'has_more_after_cursor': has_more_after_cursor,
            'forward_scan_has_more': has_more_after_cursor,
            'candidate_count': len(candidates),
            'candidates_attempted': len(results),
            'audit_signals_captured': captured_count,
            'audit_signals_authoritatively_absent': (
                authoritative_absence_count
            ),
            'audit_evidence_missing': missing_count,
            'events_failed': failed_count,
            'retry_required': retry_required,
            'retry_event_ids': retry_event_ids,
            'failed_event_ids': failed_event_ids,
            'audit_evidence_missing_event_ids': missing_evidence_event_ids,
            'unattempted_event_ids': unattempted_event_ids,
            'circuit_breaker': {
                'tripped': circuit_breaker_tripped,
                'threshold': AUDIT_BACKFILL_SYSTEMIC_FAILURE_LIMIT,
                'consecutive_systemic_failures': consecutive_systemic_failures,
            },
            'results': results,
        }

    def _load_event(self, rcept_no: str) -> dict[str, Any]:
        with session_scope() as session:
            row = session.execute(
                select(DartDisclosureEvent).where(DartDisclosureEvent.rcept_no == rcept_no).limit(1)
            ).scalar_one()
            return {
                'id': row.id,
                'rcept_no': row.rcept_no,
                'corp_code': row.corp_code,
                'stock_code': row.stock_code,
                'corp_name': row.corp_name,
                'receipt_date': row.receipt_date,
                'report_nm': row.report_nm,
                'filing_category': row.filing_category,
                'payload': row.payload,
            }

    async def _extract_audit_signal(self, event: dict[str, Any]) -> ParsedAuditSignal | None:
        audit_signal, _diagnostics = (
            await self._extract_audit_signal_with_diagnostics(event)
        )
        return audit_signal

    async def _extract_audit_signal_with_diagnostics(
        self,
        event: dict[str, Any],
    ) -> tuple[ParsedAuditSignal | None, dict[str, Any] | None]:
        if not any(token in (event.get('report_nm') or '') for token in ('사업보고서', '반기보고서', '분기보고서', '감사보고서')):
            return None, None
        artifact = await self.client.download_document(rcept_no=event['rcept_no'])
        content = artifact.get('content')
        if not isinstance(content, bytes):
            raise ValueError('DART document archive content must be bytes')
        return await asyncio.to_thread(
            self._extract_audit_signal_from_archive_with_diagnostics,
            content,
        )

    def _extract_audit_signal_from_archive(
        self,
        archive_bytes: bytes,
    ) -> ParsedAuditSignal | None:
        audit_signal, _diagnostics = (
            self._extract_audit_signal_from_archive_with_diagnostics(
                archive_bytes
            )
        )
        return audit_signal

    def _extract_audit_signal_from_archive_with_diagnostics(
        self,
        archive_bytes: bytes,
    ) -> tuple[ParsedAuditSignal | None, dict[str, Any]]:
        if len(archive_bytes) > MAX_AUDIT_ARCHIVE_COMPRESSED_BYTES:
            raise ValueError('DART document archive exceeds compressed size limit')

        findings: list[ParsedAuditSignal] = []
        expanded_bytes = 0
        decoded_member_count = 0
        aud_opn_table_count = 0
        audit_report_label_row_count = 0
        known_opinion_token_row_count = 0
        member_diagnostics: list[dict[str, Any]] = []
        diagnostics_truncated = False
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            members = [info for info in archive.infolist() if not info.is_dir()]
            if len(members) > MAX_AUDIT_ARCHIVE_MEMBERS:
                raise ValueError('DART document archive exceeds member count limit')
            if any(info.file_size > MAX_AUDIT_ARCHIVE_MEMBER_BYTES for info in members):
                raise ValueError('DART document archive member exceeds size limit')
            if sum(info.file_size for info in members) > MAX_AUDIT_ARCHIVE_EXPANDED_BYTES:
                raise ValueError('DART document archive exceeds expanded size limit')

            for info in members:
                with archive.open(info) as member:
                    raw_content = member.read(MAX_AUDIT_ARCHIVE_MEMBER_BYTES + 1)
                if len(raw_content) > MAX_AUDIT_ARCHIVE_MEMBER_BYTES:
                    raise ValueError('DART document archive member exceeds size limit')
                expanded_bytes += len(raw_content)
                if expanded_bytes > MAX_AUDIT_ARCHIVE_EXPANDED_BYTES:
                    raise ValueError('DART document archive exceeds expanded size limit')
                declared_encoding = _detect_declared_encoding(raw_content)
                document = _decode_document_text(raw_content)
                if len(member_diagnostics) < MAX_AUDIT_DIAGNOSTIC_MEMBERS:
                    member_name = str(info.filename)
                    name_truncated = (
                        len(member_name)
                        > MAX_AUDIT_DIAGNOSTIC_MEMBER_NAME_CHARS
                    )
                    declared_encoding_truncated = bool(
                        declared_encoding
                        and len(declared_encoding)
                        > MAX_AUDIT_DIAGNOSTIC_ENCODING_CHARS
                    )
                    member_diagnostics.append(
                        {
                            'name': member_name[
                                :MAX_AUDIT_DIAGNOSTIC_MEMBER_NAME_CHARS
                            ],
                            'name_truncated': name_truncated,
                            'size_bytes': int(info.file_size),
                            'declared_encoding': (
                                declared_encoding[
                                    :MAX_AUDIT_DIAGNOSTIC_ENCODING_CHARS
                                ]
                                if declared_encoding
                                else None
                            ),
                            'declared_encoding_truncated': (
                                declared_encoding_truncated
                            ),
                            'decode_success': document is not None,
                        }
                    )
                    diagnostics_truncated = (
                        diagnostics_truncated
                        or name_truncated
                        or declared_encoding_truncated
                    )
                else:
                    diagnostics_truncated = True
                if document is None:
                    continue
                decoded_member_count += 1
                document_counts = _audit_document_diagnostic_counts(document)
                aud_opn_table_count += document_counts['aud_opn_table_count']
                audit_report_label_row_count += document_counts[
                    'audit_report_label_row_count'
                ]
                known_opinion_token_row_count += document_counts[
                    'known_opinion_token_row_count'
                ]
                findings.extend(
                    self._parse_audit_findings_from_document(
                        document,
                        source_section=info.filename,
                    )
                )
        finding_parser_names = [
            str((finding.payload or {}).get('parser') or '')
            for finding in findings
        ]
        diagnostics = {
            'version': AUDIT_ARCHIVE_DIAGNOSTICS_VERSION,
            'archive_sha256': hashlib.sha256(archive_bytes).hexdigest(),
            'archive_size_bytes': len(archive_bytes),
            'member_count': len(members),
            'members': member_diagnostics,
            'decoded_member_count': decoded_member_count,
            'aud_opn_table_count': aud_opn_table_count,
            'audit_report_label_row_count': audit_report_label_row_count,
            'known_opinion_token_row_count': known_opinion_token_row_count,
            'positive_authority_evidence_count': sum(
                finding.opinion_code is not None for finding in findings
            ),
            'negative_authority_evidence_count': sum(
                parser in _AUDIT_NEGATIVE_AUTHORITY_SENTINELS
                for parser in finding_parser_names
            ),
            'ambiguous_authority_evidence_count': sum(
                parser in _AUDIT_AMBIGUOUS_AUTHORITY_SENTINELS
                for parser in finding_parser_names
            ),
            'truncated': diagnostics_truncated,
        }
        return self._select_conservative_audit_signal(findings), diagnostics

    def _parse_audit_from_document(self, text: str) -> ParsedAuditSignal | None:
        return self._select_conservative_audit_signal(
            self._parse_audit_findings_from_document(
                text,
                source_section='AUD_OPN',
            )
        )

    def _parse_audit_findings_from_document(
        self,
        text: str,
        *,
        source_section: str,
    ) -> list[ParsedAuditSignal]:
        findings: list[ParsedAuditSignal] = []
        aud_opn_ranges: list[tuple[int, int]] = []
        for group_match in _AUDIT_TABLE_PATTERN.finditer(text):
            aud_opn_ranges.append(group_match.span())
            group_html = group_match.group(1)
            nested_tables = _PLAIN_TABLE_PATTERN.findall(group_html)
            semantic_findings: list[ParsedAuditSignal] = []
            semantic_table_found = False
            for table_html in nested_tables or [group_html]:
                table_findings, recognized = self._parse_semantic_audit_table(
                    table_html,
                    source_section=source_section,
                )
                semantic_table_found = semantic_table_found or recognized
                semantic_findings.extend(table_findings)
            if semantic_table_found:
                findings.extend(semantic_findings)
                continue
            findings.extend(
                self._parse_legacy_aud_opn_table(
                    group_html,
                    source_section=source_section,
                )
            )

        for table_match in _PLAIN_TABLE_PATTERN.finditer(text):
            table_start, table_end = table_match.span()
            if any(
                group_start <= table_start and table_end <= group_end
                for group_start, group_end in aud_opn_ranges
            ):
                continue
            table_findings, recognized = self._parse_semantic_audit_table(
                table_match.group(1),
                source_section=source_section,
            )
            if recognized:
                findings.extend(table_findings)
        return findings

    @staticmethod
    def _audit_authority_sentinel(
        *,
        parser: str,
        source_section: str,
    ) -> ParsedAuditSignal:
        return ParsedAuditSignal(
            opinion_code=None,
            opinion_label=None,
            has_audit_issue=False,
            source_section=source_section[:255],
            evidence_text=None,
            payload={'parser': parser},
        )

    def _parse_semantic_audit_table(
        self,
        table_html: str,
        *,
        source_section: str,
    ) -> tuple[list[ParsedAuditSignal], bool]:
        findings: list[ParsedAuditSignal] = []
        business_year_column_index: int | None = None
        opinion_column_index: int | None = None
        semantic_header_found = False
        current_period_row_found = False
        rows = [
            [
                _clean_html_text(cell)
                for cell in _AUDIT_CELL_PATTERN.findall(row_html)
            ]
            for row_html in _AUDIT_ROW_PATTERN.findall(table_html)
        ]
        canonical_rows = [
            [_WS_PATTERN.sub('', cell) for cell in cells]
            for cells in rows
        ]
        for cells, canonical_cells in zip(rows, canonical_rows):
            if not cells:
                continue
            if all(
                any(header in cell for cell in canonical_cells)
                for header in _SEMANTIC_AUDIT_HEADERS
            ):
                semantic_header_found = True
                business_year_column_index = next(
                    index
                    for index, cell in enumerate(canonical_cells)
                    if '사업연도' in cell
                )
                opinion_column_index = next(
                    index
                    for index, cell in enumerate(canonical_cells)
                    if '감사의견' in cell
                )
                continue
            if (
                business_year_column_index is None
                or opinion_column_index is None
            ):
                continue
            if (
                business_year_column_index >= len(cells)
                or opinion_column_index >= len(cells)
            ):
                continue
            if not _CURRENT_PERIOD_PATTERN.search(
                cells[business_year_column_index]
            ):
                continue
            current_period_row_found = True
            opinion_cell = cells[opinion_column_index]
            matched_opinion = next(
                (
                    (code, label)
                    for pattern, code, label in _SEMANTIC_AUDIT_PATTERNS
                    if pattern.search(opinion_cell)
                ),
                None,
            )
            if matched_opinion is None:
                continue
            code, label = matched_opinion
            findings.append(
                ParsedAuditSignal(
                    opinion_code=code,
                    opinion_label=label,
                    has_audit_issue=code
                    in {'disclaimer', 'adverse', 'qualified'},
                    source_section=source_section[:255],
                    evidence_text=' | '.join(cell for cell in cells if cell),
                    payload={
                        'parser': 'semantic_audit_table_v1',
                        'business_year_column_index': (
                            business_year_column_index
                        ),
                        'opinion_cell': opinion_cell,
                        'opinion_column_index': opinion_column_index,
                        'row_cells': cells,
                    },
                )
            )
        if semantic_header_found:
            if findings:
                return findings, True
            sentinel_parser = (
                'semantic_audit_table_current_opinion_unrecognized_v1'
                if current_period_row_found
                else 'semantic_audit_table_current_period_absent_v1'
            )
            return [
                self._audit_authority_sentinel(
                    parser=sentinel_parser,
                    source_section=source_section,
                )
            ], True

        header_locations: dict[str, tuple[int, int]] = {}
        for header in _SEMANTIC_AUDIT_HEADERS:
            locations = [
                (row_index, column_index)
                for row_index, canonical_cells in enumerate(canonical_rows)
                for column_index, cell in enumerate(canonical_cells)
                if header in cell
            ]
            if len(locations) != 1:
                return [], False
            header_locations[header] = locations[0]
        if (
            len({row_index for row_index, _ in header_locations.values()})
            != len(_SEMANTIC_AUDIT_HEADERS)
            or len(
                {
                    column_index
                    for _, column_index in header_locations.values()
                }
            )
            != 1
        ):
            return [], False

        business_year_row_index = header_locations['사업연도'][0]
        current_period_column_indices = [
            column_index
            for column_index, cell in enumerate(rows[business_year_row_index])
            if _CURRENT_PERIOD_PATTERN.search(cell)
        ]
        if not current_period_column_indices:
            return [
                self._audit_authority_sentinel(
                    parser='semantic_audit_table_current_period_absent_v1',
                    source_section=source_section,
                )
            ], True
        if len(current_period_column_indices) != 1:
            return [
                self._audit_authority_sentinel(
                    parser=(
                        'semantic_audit_table_current_period_ambiguous_v1'
                    ),
                    source_section=source_section,
                )
            ], True
        current_period_column_index = current_period_column_indices[0]
        opinion_row_index = header_locations['감사의견'][0]
        if current_period_column_index >= len(rows[opinion_row_index]):
            return [
                self._audit_authority_sentinel(
                    parser=(
                        'semantic_audit_table_current_opinion_unrecognized_v1'
                    ),
                    source_section=source_section,
                )
            ], True
        opinion_cell = rows[opinion_row_index][current_period_column_index]
        matched_opinion = next(
            (
                (code, label)
                for pattern, code, label in _SEMANTIC_AUDIT_PATTERNS
                if pattern.search(opinion_cell)
            ),
            None,
        )
        if matched_opinion is None:
            return [
                self._audit_authority_sentinel(
                    parser=(
                        'semantic_audit_table_current_opinion_unrecognized_v1'
                    ),
                    source_section=source_section,
                )
            ], True
        code, label = matched_opinion
        current_column_cells = [
            cells[current_period_column_index]
            for cells in rows
            if current_period_column_index < len(cells)
        ]
        return [
            ParsedAuditSignal(
                opinion_code=code,
                opinion_label=label,
                has_audit_issue=code
                in {'disclaimer', 'adverse', 'qualified'},
                source_section=source_section[:255],
                evidence_text=' | '.join(
                    cell for cell in current_column_cells if cell
                ),
                payload={
                    'parser': 'semantic_audit_table_v1',
                    'orientation': 'period_columns',
                    'business_year_row_index': business_year_row_index,
                    'current_period_column_index': (
                        current_period_column_index
                    ),
                    'opinion_cell': opinion_cell,
                    'opinion_row_index': opinion_row_index,
                    'column_cells': current_column_cells,
                },
            )
        ], True

    def _parse_legacy_aud_opn_table(
        self,
        table_html: str,
        *,
        source_section: str,
    ) -> list[ParsedAuditSignal]:
        candidates: list[
            tuple[list[str], str, str, str, str, str]
        ] = []
        explicit_period_marker_found = False
        for row_html in _AUDIT_ROW_PATTERN.findall(table_html):
            cells = [
                _clean_html_text(cell)
                for cell in _AUDIT_CELL_PATTERN.findall(row_html)
            ]
            if not cells:
                continue
            row_text = ' | '.join(cell for cell in cells if cell)
            report_label_cell = next(
                (
                    cell
                    for cell in cells
                    if '감사보고서' in cell
                    or '연결감사보고서' in cell
                ),
                None,
            )
            if report_label_cell is None:
                continue
            explicit_period_marker_found = (
                explicit_period_marker_found
                or bool(_EXPLICIT_PERIOD_PATTERN.search(row_text))
            )
            opinion_cell = next(
                (
                    cell
                    for cell in cells
                    if any(
                        pattern.search(cell)
                        for pattern, _, _ in _AUDIT_PATTERNS
                    )
                ),
                None,
            )
            if not opinion_cell:
                continue
            for pattern, code, label in _AUDIT_PATTERNS:
                if pattern.search(opinion_cell):
                    candidates.append(
                        (
                            cells,
                            row_text,
                            opinion_cell,
                            code,
                            label,
                            report_label_cell,
                        )
                    )
                    break

        if explicit_period_marker_found:
            candidates = [
                candidate
                for candidate in candidates
                if _CURRENT_PERIOD_PATTERN.search(candidate[5])
            ]
            if not candidates:
                return [
                    self._audit_authority_sentinel(
                        parser='legacy_aud_opn_period_without_current_v1',
                        source_section=source_section,
                    )
                ]
            parser = 'legacy_aud_opn_explicit_current_v1'
        else:
            opinion_codes = {candidate[3] for candidate in candidates}
            if len(opinion_codes) > 1:
                return [
                    self._audit_authority_sentinel(
                        parser='legacy_aud_opn_markerless_conflict_v1',
                        source_section=source_section,
                    )
                ]
            parser = 'legacy_aud_opn_markerless_v1'

        findings: list[ParsedAuditSignal] = []
        for (
            cells,
            row_text,
            opinion_cell,
            code,
            label,
            _report_label_cell,
        ) in candidates:
            findings.append(
                ParsedAuditSignal(
                    opinion_code=code,
                    opinion_label=label,
                    has_audit_issue=code
                    in {'disclaimer', 'adverse', 'qualified'},
                    source_section=source_section[:255],
                    evidence_text=row_text,
                    payload={
                        'parser': parser,
                        'opinion_cell': opinion_cell,
                        'row_cells': cells,
                    },
                )
            )
        return findings

    def _select_conservative_audit_signal(
        self,
        findings: list[ParsedAuditSignal],
    ) -> ParsedAuditSignal | None:
        if not findings:
            return None
        parser_names = {
            str((item.payload or {}).get('parser') or '')
            for item in findings
        }
        if parser_names & _AUDIT_AMBIGUOUS_AUTHORITY_SENTINELS:
            return None
        semantic_findings = [
            item
            for item in findings
            if item.opinion_code
            and (item.payload or {}).get('parser')
            == 'semantic_audit_table_v1'
        ]
        if semantic_findings:
            findings = semantic_findings
        else:
            explicit_current_findings = [
                item
                for item in findings
                if item.opinion_code
                and (item.payload or {}).get('parser')
                == 'legacy_aud_opn_explicit_current_v1'
            ]
            if explicit_current_findings:
                findings = explicit_current_findings
            elif parser_names & _AUDIT_NEGATIVE_AUTHORITY_SENTINELS:
                return None
            elif parser_names & (
                _AUDIT_AMBIGUOUS_AUTHORITY_SENTINELS
                | _AUDIT_NEGATIVE_AUTHORITY_SENTINELS
            ):
                return None
            else:
                markerless_findings = [
                    item for item in findings if item.opinion_code
                ]
                markerless_opinion_codes = {
                    item.opinion_code for item in markerless_findings
                }
                if len(markerless_opinion_codes) != 1:
                    return None
                findings = markerless_findings
        ordered = sorted(
            findings,
            key=lambda item: (
                -_AUDIT_OPINION_SEVERITY.get(item.opinion_code or '', -1),
                item.source_section or '',
                item.evidence_text or '',
            ),
        )
        selected = ordered[0]
        opinion_codes = sorted(
            {item.opinion_code for item in ordered if item.opinion_code},
            key=lambda code: (-_AUDIT_OPINION_SEVERITY[code], code),
        )
        source_sections = sorted(
            {item.source_section for item in ordered if item.source_section}
        )
        findings_by_opinion = {
            code: sum(item.opinion_code == code for item in ordered)
            for code in opinion_codes
        }
        selected_payload = dict(selected.payload or {})
        selected_payload.update(
            {
                'selection_policy': 'most_conservative_audit_opinion_v1',
                'evidence_count': len(ordered),
                'opinion_codes': opinion_codes,
                'conflicting_opinion_codes': opinion_codes if len(opinion_codes) > 1 else [],
                'findings_by_opinion': findings_by_opinion,
                'source_sections': source_sections,
                'selected_source_section': selected.source_section,
            }
        )
        return ParsedAuditSignal(
            opinion_code=selected.opinion_code,
            opinion_label=selected.opinion_label,
            has_audit_issue=selected.has_audit_issue,
            source_section=selected.source_section,
            evidence_text=selected.evidence_text,
            payload=selected_payload,
        )

    def _extract_dilution_signal(self, event: dict[str, Any]) -> dict[str, Any] | None:
        report_nm = event.get('report_nm') or ''
        for token, signal_type, security_type in _DILUTION_RULES:
            if token in report_nm:
                return {
                    'signal_type': signal_type,
                    'has_dilution_signal': True,
                    'security_type': security_type,
                    'reason': report_nm,
                    'payload': {
                        'matched_token': token,
                        'evidence_semantics': 'positive_event_match',
                    },
                }
        return None

    def persist_share_issuance_scan_evidence(
        self,
        *,
        corporation: dict[str, str],
        raw_items: list[dict[str, Any]],
        window_start: date,
        window_end: date,
        pagination_completed: bool,
        sync_run_id: int | None = None,
        failure_reason: str | None = None,
        positive_events_already_persisted: bool = False,
        previous_evidence_rows: list[dict[str, Any]] | None = None,
        latest_positive_date: date | None | object = _POSITIVE_DATE_NOT_LOADED,
        defer_store: bool = False,
    ) -> dict[str, Any]:
        """Persist share-issuance evidence only from a completed corporation scan."""
        if not pagination_completed:
            incomplete_rows = self._persist_incomplete_share_issuance_attempt(
                corporation=corporation,
                window_start=window_start,
                window_end=window_end,
                sync_run_id=sync_run_id,
                failure_reason=failure_reason,
            )
            return {
                'pagination_completed': False,
                'positive_event_count': 0,
                'negative_coverage_count': 0,
                'negative_segment_count': 0,
                'rows_upserted': incomplete_rows,
                'coverage_status': 'incomplete',
            }

        corp_code = str(corporation.get('corp_code') or '').strip()
        if not corp_code:
            raise ValueError('corp_code is required for share issuance evidence')
        requested_stock_code = self._normalize_stock_code(
            corporation.get('stock_code')
        )
        corporation_name = str(corporation.get('corp_name') or '').strip()
        if window_end < window_start:
            raise ValueError('share issuance scan window_end precedes window_start')
        scan_completed_at = datetime.now(timezone.utc)
        try:
            validate_finalized_krx_write_date(
                window_end,
                now=scan_completed_at,
                operation='DART share issuance negative coverage',
            )
        except ValueError as exc:
            self._persist_incomplete_share_issuance_attempt(
                corporation=corporation,
                window_start=window_start,
                window_end=window_end,
                sync_run_id=sync_run_id,
                failure_reason=str(exc),
                scan_completed_at=scan_completed_at,
            )
            raise
        self._validate_completed_scan_items(
            raw_items=raw_items,
            corp_code=corp_code,
            window_start=window_start,
            window_end=window_end,
        )

        evidence_rows: list[dict[str, Any]] = []
        matched_event_count = 0
        matched_receipt_dates: list[date] = []
        seen_receipt_numbers: set[str] = set()
        for raw_item in raw_items:
            report_name = str(raw_item.get('report_nm') or '').strip()
            dilution_signal = self._extract_dilution_signal(
                {'report_nm': report_name}
            )
            if dilution_signal is None:
                continue

            receipt_number = str(raw_item.get('rcept_no') or '').strip()
            if not receipt_number:
                raise ValueError(
                    'share issuance disclosure is missing rcept_no'
                )
            if receipt_number in seen_receipt_numbers:
                continue
            seen_receipt_numbers.add(receipt_number)

            raw_corp_code = str(raw_item.get('corp_code') or '').strip()
            if raw_corp_code and raw_corp_code != corp_code:
                raise ValueError(
                    'share issuance disclosure corp_code does not match scan scope'
                )
            receipt_date = self._parse_dart_receipt_date(
                raw_item.get('rcept_dt')
            )
            if receipt_date is None:
                raise ValueError(
                    'share issuance disclosure is missing a valid receipt date'
                )
            if not window_start <= receipt_date <= window_end:
                raise ValueError(
                    'share issuance disclosure falls outside the completed scan window'
                )

            stock_code = (
                self._normalize_stock_code(raw_item.get('stock_code'))
                or requested_stock_code
            )
            dilution_signal = {
                **dilution_signal,
                'payload': {
                    **dilution_signal['payload'],
                    'source': SHARE_ISSUANCE_EVIDENCE_SOURCE,
                    'scan_window_start': window_start.isoformat(),
                    'scan_window_end': window_end.isoformat(),
                    'pagination_completed': True,
                },
            }
            event = {
                'id': None,
                'rcept_no': receipt_number,
                'corp_code': corp_code,
                'stock_code': stock_code,
                'corp_name': (
                    str(raw_item.get('corp_name') or '').strip()
                    or corporation_name
                    or corp_code
                ),
                'receipt_date': receipt_date,
                'report_nm': report_name,
            }
            if not positive_events_already_persisted:
                evidence_rows.append(
                    {'event': event, 'dilution_signal': dilution_signal}
                )
            matched_event_count += 1
            matched_receipt_dates.append(receipt_date)

        negative_coverage_count = 0
        negative_segment_count = 0
        coverage_status = 'missing_stock_code'
        if requested_stock_code:
            resolved_latest_positive_date = (
                max(matched_receipt_dates)
                if matched_receipt_dates
                else (
                    self.repository.get_latest_share_issuance_positive_event_date(
                        stock_code=requested_stock_code,
                        corp_code=corp_code,
                        on_or_before=window_end,
                    )
                    if latest_positive_date is _POSITIVE_DATE_NOT_LOADED
                    else latest_positive_date
                )
            )
            negative_window_start = max(
                window_start,
                resolved_latest_positive_date + timedelta(days=1)
                if isinstance(resolved_latest_positive_date, date)
                else window_start,
            )
            continued_from_evidence_id: int | None = None
            if not matched_receipt_dates:
                candidate_previous_rows = (
                    previous_evidence_rows
                    if previous_evidence_rows is not None
                    else self.repository.list_recent_share_issuance_scan_evidence(
                        stock_code=requested_stock_code,
                        corp_code=corp_code,
                        before_date=window_end + timedelta(days=1),
                        limit=10,
                    )
                )
                for previous_row in candidate_previous_rows:
                    previous_coverage = self._validated_share_issuance_coverage(
                        previous_row,
                        corp_code=corp_code,
                    )
                    if previous_coverage is None:
                        continue
                    previous_end = date.fromisoformat(
                        str(previous_coverage['window_end'])
                    )
                    if window_start > previous_end + timedelta(days=1):
                        break
                    previous_start = date.fromisoformat(
                        str(previous_coverage['window_start'])
                    )
                    if (
                        isinstance(resolved_latest_positive_date, date)
                        and previous_start <= resolved_latest_positive_date
                    ):
                        break
                    if previous_start <= previous_end:
                        negative_window_start = min(
                            negative_window_start,
                            previous_start,
                        )
                        continued_from_evidence_id = int(previous_row['id'])
                    break

            coverage_complete = (
                negative_window_start <= window_end
                and negative_window_start
                <= window_end
                - timedelta(days=SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS)
            )
            if negative_window_start > window_end:
                if evidence_rows and not defer_store:
                    self._store_dilution_signal_rows(evidence_rows)
                result = {
                    'pagination_completed': True,
                    'positive_event_count': matched_event_count,
                    'negative_coverage_count': 0,
                    'negative_segment_count': 0,
                    'rows_upserted': len(evidence_rows),
                    'coverage_status': 'positive_events',
                }
                if defer_store:
                    result['_evidence_rows'] = evidence_rows
                return result
            coverage_state = 'complete' if coverage_complete else 'segment'
            coverage = {
                'schema_version': SHARE_ISSUANCE_NEGATIVE_COVERAGE_SCHEMA,
                'rule_version': SHARE_ISSUANCE_RULE_VERSION,
                'status': coverage_state,
                'source': SHARE_ISSUANCE_EVIDENCE_SOURCE,
                'corp_code': corp_code,
                'window_start': negative_window_start.isoformat(),
                'window_end': window_end.isoformat(),
                'matched_event_count': 0,
                'scan_window_start': window_start.isoformat(),
                'scan_window_end': window_end.isoformat(),
                'scan_matched_event_count': matched_event_count,
                'pagination_completed': True,
                'scan_completed_at': scan_completed_at.isoformat(),
                'authority_policy_version': (
                    SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION
                ),
                'authoritative_as_of_cutoff': True,
                'late_arrival_reconciliation': (
                    SHARE_ISSUANCE_LATE_ARRIVAL_POLICY
                ),
            }
            if sync_run_id is not None:
                coverage['sync_run_id'] = int(sync_run_id)
            if continued_from_evidence_id is not None:
                coverage['continued_from_evidence_id'] = (
                    continued_from_evidence_id
                )
            event = {
                'id': None,
                'rcept_no': (
                    f'DILCOV-{corp_code[:16]}'
                ),
                'corp_code': corp_code,
                'stock_code': requested_stock_code,
                'corp_name': corporation_name or corp_code,
                'receipt_date': window_end,
                'report_nm': 'DART share issuance coverage',
            }
            evidence_rows.append(
                {
                    'event': event,
                    'dilution_signal': {
                        'signal_type': 'share_issuance',
                        'has_dilution_signal': False,
                        'security_type': None,
                        'reason': (
                            'completed disclosure scan established a share issuance '
                            'negative-evidence segment'
                        ),
                        'payload': {
                            'evidence_semantics': (
                                'complete_negative_coverage'
                                if coverage_complete
                                else 'negative_coverage_segment'
                            ),
                            'evidence_coverage': coverage,
                        },
                    },
                }
            )
            evidence_rows.append(
                {
                    'event': {
                        **event,
                        'rcept_no': f'DILATT-{corp_code[:16]}',
                        'report_nm': 'DART share issuance latest scan attempt',
                    },
                    'dilution_signal': {
                        'signal_type': 'share_issuance',
                        'has_dilution_signal': False,
                        'security_type': None,
                        'reason': (
                            'latest completed share issuance disclosure scan '
                            'authority marker'
                        ),
                        'payload': {
                            'evidence_semantics': 'completed_scan_attempt',
                            'evidence_coverage': coverage,
                        },
                    },
                }
            )
            if coverage_complete:
                negative_coverage_count = 1
                coverage_status = 'complete_negative'
            else:
                negative_segment_count = 1
                coverage_status = 'negative_segment'

        if evidence_rows and not defer_store:
            self._store_dilution_signal_rows(evidence_rows)

        result = {
            'pagination_completed': True,
            'positive_event_count': matched_event_count,
            'negative_coverage_count': negative_coverage_count,
            'negative_segment_count': negative_segment_count,
            'rows_upserted': len(evidence_rows),
            'coverage_status': coverage_status,
        }
        if defer_store:
            result['_evidence_rows'] = evidence_rows
        return result

    def plan_global_share_issuance_scan(
        self,
        *,
        corporations: list[dict[str, str]],
        window_end: date,
    ) -> dict[str, Any]:
        corp_codes = list(
            dict.fromkeys(
                str(item.get('corp_code') or '').strip()
                for item in corporations
                if str(item.get('corp_code') or '').strip()
            )
        )
        current_rows = self.repository.list_current_share_issuance_coverage(
            corp_codes=corp_codes,
            on_or_before=window_end,
        )
        rows_by_corp = {
            str(row.get('corp_code') or ''): row for row in current_rows
        }
        starts: list[date] = []
        missing_corp_codes: list[str] = []
        for corp_code in corp_codes:
            current_row = rows_by_corp.get(corp_code)
            coverage = (
                self._validated_share_issuance_coverage(
                    current_row,
                    corp_code=corp_code,
                )
                if current_row is not None
                else None
            )
            if coverage is None:
                missing_corp_codes.append(corp_code)
                continue
            # Re-read the prior end date on the next global scan so filings that
            # arrive after the fixed post-close snapshot are reconciled.
            starts.append(date.fromisoformat(str(coverage['window_end'])))
            if coverage.get('status') != 'complete':
                missing_corp_codes.append(corp_code)
        return {
            'window_start': min(starts) if starts else window_end,
            'window_end': window_end,
            'corporation_count': len(corp_codes),
            'baseline_required_count': len(missing_corp_codes),
            'baseline_required_corp_codes': missing_corp_codes,
            'late_arrival_reconciliation': (
                SHARE_ISSUANCE_LATE_ARRIVAL_POLICY
            ),
        }

    def persist_global_share_issuance_scan_evidence(
        self,
        *,
        corporations: list[dict[str, str]],
        raw_items: list[dict[str, Any]],
        window_start: date,
        window_end: date,
        sync_run_id: int,
        positive_events_already_persisted: bool = True,
    ) -> dict[str, Any]:
        corp_codes = list(
            dict.fromkeys(
                str(item.get('corp_code') or '').strip()
                for item in corporations
                if str(item.get('corp_code') or '').strip()
            )
        )
        corporation_by_corp = {
            str(item.get('corp_code') or '').strip(): item
            for item in corporations
            if str(item.get('corp_code') or '').strip()
        }
        raw_items_by_corp: dict[str, list[dict[str, Any]]] = {
            corp_code: [] for corp_code in corp_codes
        }
        for raw_item in raw_items:
            corp_code = str(raw_item.get('corp_code') or '').strip()
            if not corp_code:
                raise ValueError(
                    'global share issuance scan item is missing corp_code'
                )
            if corp_code in raw_items_by_corp:
                raw_items_by_corp[corp_code].append(raw_item)

        current_rows = self.repository.list_current_share_issuance_coverage(
            corp_codes=corp_codes,
            on_or_before=window_end,
        )
        current_rows_by_corp: dict[str, list[dict[str, Any]]] = {}
        for row in current_rows:
            current_rows_by_corp.setdefault(
                str(row.get('corp_code') or ''), []
            ).append(row)
        latest_positive_dates = (
            self.repository.list_latest_share_issuance_positive_event_dates(
                corp_codes=corp_codes,
                on_or_before=window_end,
            )
        )

        pending_by_corp: dict[
            str, tuple[dict[str, Any], list[dict[str, Any]]]
        ] = {}
        failed_corp_reasons: dict[str, str] = {}
        for corp_code in corp_codes:
            corporation = corporation_by_corp[corp_code]
            try:
                result = self.persist_share_issuance_scan_evidence(
                    corporation=corporation,
                    raw_items=raw_items_by_corp[corp_code],
                    window_start=window_start,
                    window_end=window_end,
                    pagination_completed=True,
                    sync_run_id=sync_run_id,
                    positive_events_already_persisted=(
                        positive_events_already_persisted
                    ),
                    previous_evidence_rows=current_rows_by_corp.get(
                        corp_code, []
                    ),
                    latest_positive_date=latest_positive_dates.get(corp_code),
                    defer_store=True,
                )
                corp_evidence_rows = result.pop('_evidence_rows', [])
            except Exception as exc:
                failure_reason = str(exc)[:500]
                failed_corp_reasons[corp_code] = failure_reason
                try:
                    self._persist_incomplete_share_issuance_attempt(
                        corporation=corporation,
                        window_start=window_start,
                        window_end=window_end,
                        sync_run_id=sync_run_id,
                        failure_reason=failure_reason,
                    )
                except Exception:
                    # The original corporation-local failure remains the
                    # authoritative retry reason.  A marker write failure must
                    # not erase other corporations' completed evidence.
                    pass
                continue
            pending_by_corp[corp_code] = (result, corp_evidence_rows)

        persisted_corp_codes: set[str] = set()

        def persist_corp_batch(batch_corp_codes: list[str]) -> None:
            if not batch_corp_codes:
                return
            batch_rows = [
                row
                for corp_code in batch_corp_codes
                for row in pending_by_corp[corp_code][1]
            ]
            try:
                if batch_rows:
                    self._store_dilution_signal_rows(batch_rows)
            except Exception as exc:
                if not _is_corporation_local_persistence_error(exc):
                    raise
                if len(batch_corp_codes) > 1:
                    midpoint = len(batch_corp_codes) // 2
                    persist_corp_batch(batch_corp_codes[:midpoint])
                    persist_corp_batch(batch_corp_codes[midpoint:])
                    return
                corp_code = batch_corp_codes[0]
                failed_corp_reasons[corp_code] = (
                    f'evidence persistence failed: {exc}'[:500]
                )
                return
            persisted_corp_codes.update(batch_corp_codes)

        pending_corp_codes = list(pending_by_corp)
        for index in range(0, len(pending_corp_codes), 250):
            persist_corp_batch(pending_corp_codes[index:index + 250])

        for corp_code in sorted(
            set(pending_by_corp) - persisted_corp_codes
        ):
            try:
                self._persist_incomplete_share_issuance_attempt(
                    corporation=corporation_by_corp[corp_code],
                    window_start=window_start,
                    window_end=window_end,
                    sync_run_id=sync_run_id,
                    failure_reason=failed_corp_reasons[corp_code],
                )
            except Exception:
                pass

        evidence_rows: list[dict[str, Any]] = []
        positive_event_count = 0
        negative_coverage_count = 0
        negative_segment_count = 0
        coverage_ready_corp_codes: list[str] = []
        baseline_debt_corp_codes: list[str] = []
        coverage_status_by_corp: dict[str, str] = {
            corp_code: 'failed' for corp_code in failed_corp_reasons
        }
        for corp_code in corp_codes:
            if corp_code not in persisted_corp_codes:
                continue
            result, corp_evidence_rows = pending_by_corp[corp_code]
            evidence_rows.extend(corp_evidence_rows)
            positive_event_count += int(
                result.get('positive_event_count') or 0
            )
            negative_coverage_count += int(
                result.get('negative_coverage_count') or 0
            )
            negative_segment_count += int(
                result.get('negative_segment_count') or 0
            )
            coverage_status = str(
                result.get('coverage_status') or 'unknown'
            )
            coverage_status_by_corp[corp_code] = coverage_status
            latest_positive_date = latest_positive_dates.get(corp_code)
            has_recent_positive_evidence = (
                isinstance(latest_positive_date, date)
                and latest_positive_date
                >= window_end
                - timedelta(days=SHARE_ISSUANCE_NEGATIVE_COVERAGE_DAYS)
            )
            if (
                int(result.get('positive_event_count') or 0) > 0
                or coverage_status == 'complete_negative'
                or has_recent_positive_evidence
            ):
                coverage_ready_corp_codes.append(corp_code)
                if (
                    has_recent_positive_evidence
                    and coverage_status != 'complete_negative'
                ):
                    coverage_status_by_corp[corp_code] = (
                        'recent_positive_evidence'
                    )
            else:
                baseline_debt_corp_codes.append(corp_code)
        return {
            'pagination_completed': True,
            'corporation_count': len(corp_codes),
            'positive_event_count': positive_event_count,
            'negative_coverage_count': negative_coverage_count,
            'negative_segment_count': negative_segment_count,
            'rows_upserted': len(evidence_rows),
            'coverage_ready_corp_codes': coverage_ready_corp_codes,
            'baseline_debt_corp_codes': baseline_debt_corp_codes,
            'coverage_status_by_corp': coverage_status_by_corp,
            'failed_corp_codes': sorted(failed_corp_reasons),
            'failed_corp_reasons': failed_corp_reasons,
        }

    def persist_global_share_issuance_incomplete_attempts(
        self,
        *,
        corporations: list[dict[str, str]],
        window_start: date,
        window_end: date,
        sync_run_id: int,
        failure_reason: str,
    ) -> int:
        completed_at = datetime.now(timezone.utc)
        rows: list[dict[str, Any]] = []
        seen_corp_codes: set[str] = set()
        for corporation in corporations:
            corp_code = str(corporation.get('corp_code') or '').strip()
            stock_code = self._normalize_stock_code(
                corporation.get('stock_code')
            )
            if (
                not corp_code
                or not stock_code
                or corp_code in seen_corp_codes
            ):
                continue
            seen_corp_codes.add(corp_code)
            coverage = {
                'schema_version': SHARE_ISSUANCE_NEGATIVE_COVERAGE_SCHEMA,
                'rule_version': SHARE_ISSUANCE_RULE_VERSION,
                'status': 'incomplete',
                'source': SHARE_ISSUANCE_EVIDENCE_SOURCE,
                'corp_code': corp_code,
                'window_start': window_start.isoformat(),
                'window_end': window_end.isoformat(),
                'matched_event_count': None,
                'scan_window_start': window_start.isoformat(),
                'scan_window_end': window_end.isoformat(),
                'pagination_completed': False,
                'scan_completed_at': completed_at.isoformat(),
                'authority_policy_version': (
                    SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION
                ),
                'authoritative_as_of_cutoff': False,
                'late_arrival_reconciliation': (
                    SHARE_ISSUANCE_LATE_ARRIVAL_POLICY
                ),
                'sync_run_id': int(sync_run_id),
                'failure_reason': str(failure_reason)[:500],
            }
            rows.append(
                {
                    'event': {
                        'id': None,
                        'rcept_no': f'DILATT-{corp_code[:16]}',
                        'corp_code': corp_code,
                        'stock_code': stock_code,
                        'corp_name': (
                            str(corporation.get('corp_name') or '').strip()
                            or corp_code
                        ),
                        'receipt_date': window_end,
                        'report_nm': 'DART share issuance coverage attempt',
                    },
                    'dilution_signal': {
                        'signal_type': 'share_issuance',
                        'has_dilution_signal': False,
                        'security_type': None,
                        'reason': 'share issuance disclosure scan was incomplete',
                        'payload': {
                            'evidence_semantics': 'incomplete_scan_attempt',
                            'evidence_coverage': coverage,
                        },
                    },
                }
            )
        if rows:
            self._store_dilution_signal_rows(rows)
        return len(rows)

    def persist_share_issuance_positive_events(
        self,
        *,
        corporation: dict[str, str],
        raw_items: list[dict[str, Any]],
        window_start: date,
        window_end: date,
        sync_run_id: int | None = None,
        pagination_completed: bool = False,
    ) -> dict[str, Any]:
        """Persist adverse matches immediately; negative proof waits for all pages."""
        corp_code = str(corporation.get('corp_code') or '').strip()
        if not corp_code:
            raise ValueError('corp_code is required for share issuance evidence')
        requested_stock_code = self._normalize_stock_code(
            corporation.get('stock_code')
        )
        corporation_name = str(corporation.get('corp_name') or '').strip()
        rows: list[dict[str, Any]] = []
        seen_receipt_numbers: set[str] = set()
        for raw_item in raw_items:
            report_name = str(raw_item.get('report_nm') or '').strip()
            dilution_signal = self._extract_dilution_signal(
                {'report_nm': report_name}
            )
            if dilution_signal is None:
                continue
            receipt_number = str(raw_item.get('rcept_no') or '').strip()
            if not receipt_number or receipt_number in seen_receipt_numbers:
                continue
            raw_corp_code = str(raw_item.get('corp_code') or '').strip()
            if raw_corp_code and raw_corp_code != corp_code:
                raise ValueError(
                    'share issuance disclosure corp_code does not match scan scope'
                )
            receipt_date = self._parse_dart_receipt_date(
                raw_item.get('rcept_dt')
            )
            if receipt_date is None or not window_start <= receipt_date <= window_end:
                raise ValueError(
                    'share issuance disclosure falls outside the scan window'
                )
            seen_receipt_numbers.add(receipt_number)
            payload = {
                **dilution_signal['payload'],
                'source': SHARE_ISSUANCE_EVIDENCE_SOURCE,
                'scan_window_start': window_start.isoformat(),
                'scan_window_end': window_end.isoformat(),
                'pagination_completed': pagination_completed,
                'evidence_semantics': 'positive_event_match',
            }
            if sync_run_id is not None:
                payload['sync_run_id'] = int(sync_run_id)
            rows.append(
                {
                    'event': {
                        'id': None,
                        'rcept_no': receipt_number,
                        'corp_code': corp_code,
                        'stock_code': (
                            self._normalize_stock_code(raw_item.get('stock_code'))
                            or requested_stock_code
                        ),
                        'corp_name': (
                            str(raw_item.get('corp_name') or '').strip()
                            or corporation_name
                            or corp_code
                        ),
                        'receipt_date': receipt_date,
                        'report_nm': report_name,
                    },
                    'dilution_signal': {
                        **dilution_signal,
                        'payload': payload,
                    },
                }
            )
        if rows:
            self._store_dilution_signal_rows(rows)
        return {
            'positive_event_count': len(rows),
            'rows_upserted': len(rows),
        }

    def _persist_incomplete_share_issuance_attempt(
        self,
        *,
        corporation: dict[str, str],
        window_start: date,
        window_end: date,
        sync_run_id: int | None,
        failure_reason: str | None,
        scan_completed_at: datetime | None = None,
    ) -> int:
        corp_code = str(corporation.get('corp_code') or '').strip()
        stock_code = self._normalize_stock_code(corporation.get('stock_code'))
        if not corp_code or not stock_code:
            return 0
        coverage: dict[str, Any] = {
            'schema_version': SHARE_ISSUANCE_NEGATIVE_COVERAGE_SCHEMA,
            'rule_version': SHARE_ISSUANCE_RULE_VERSION,
            'status': 'incomplete',
            'source': SHARE_ISSUANCE_EVIDENCE_SOURCE,
            'corp_code': corp_code,
            'window_start': window_start.isoformat(),
            'window_end': window_end.isoformat(),
            'matched_event_count': None,
            'scan_window_start': window_start.isoformat(),
            'scan_window_end': window_end.isoformat(),
            'pagination_completed': False,
            'scan_completed_at': (
                scan_completed_at or datetime.now(timezone.utc)
            ).isoformat(),
            'authority_policy_version': (
                SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION
            ),
            'authoritative_as_of_cutoff': False,
            'late_arrival_reconciliation': (
                SHARE_ISSUANCE_LATE_ARRIVAL_POLICY
            ),
        }
        if sync_run_id is not None:
            coverage['sync_run_id'] = int(sync_run_id)
        if failure_reason:
            coverage['failure_reason'] = str(failure_reason)[:500]
        self._store_dilution_signal_rows(
            [
                {
                    'event': {
                        'id': None,
                        'rcept_no': f'DILATT-{corp_code[:16]}',
                        'corp_code': corp_code,
                        'stock_code': stock_code,
                        'corp_name': (
                            str(corporation.get('corp_name') or '').strip()
                            or corp_code
                        ),
                        'receipt_date': window_end,
                        'report_nm': 'DART share issuance coverage attempt',
                    },
                    'dilution_signal': {
                        'signal_type': 'share_issuance',
                        'has_dilution_signal': False,
                        'security_type': None,
                        'reason': 'share issuance disclosure scan was incomplete',
                        'payload': {
                            'evidence_semantics': 'incomplete_scan_attempt',
                            'evidence_coverage': coverage,
                        },
                    },
                }
            ]
        )
        return 1

    @staticmethod
    def _validated_share_issuance_coverage(
        row: dict[str, Any],
        *,
        corp_code: str,
    ) -> dict[str, Any] | None:
        payload = row.get('payload')
        if not isinstance(payload, dict):
            return None
        coverage = payload.get('evidence_coverage')
        if not isinstance(coverage, dict):
            return None
        if coverage.get('schema_version') != SHARE_ISSUANCE_NEGATIVE_COVERAGE_SCHEMA:
            return None
        if coverage.get('rule_version') != SHARE_ISSUANCE_RULE_VERSION:
            return None
        if coverage.get('source') != SHARE_ISSUANCE_EVIDENCE_SOURCE:
            return None
        if coverage.get('corp_code') != corp_code:
            return None
        if coverage.get('status') not in {'complete', 'segment'}:
            return None
        if coverage.get('matched_event_count') != 0:
            return None
        if coverage.get('pagination_completed') is not True:
            return None
        if coverage.get('authoritative_as_of_cutoff') is not True:
            return None
        if (
            coverage.get('authority_policy_version')
            != SHARE_ISSUANCE_AUTHORITY_POLICY_VERSION
        ):
            return None
        if (
            coverage.get('late_arrival_reconciliation')
            != SHARE_ISSUANCE_LATE_ARRIVAL_POLICY
        ):
            return None
        try:
            datetime.fromisoformat(str(coverage.get('scan_completed_at') or ''))
        except ValueError:
            return None
        try:
            window_start = date.fromisoformat(str(coverage.get('window_start')))
            window_end = date.fromisoformat(str(coverage.get('window_end')))
        except ValueError:
            return None
        if window_start > window_end:
            return None
        return coverage

    @staticmethod
    def _normalize_stock_code(value: Any) -> str | None:
        normalized = str(value or '').strip()
        if not normalized:
            return None
        return normalized.zfill(6) if normalized.isdigit() else normalized

    @staticmethod
    def _parse_dart_receipt_date(value: Any) -> date | None:
        normalized = str(value or '').strip()
        if len(normalized) < 8 or not normalized[:8].isdigit():
            return None
        try:
            return date(
                int(normalized[:4]),
                int(normalized[4:6]),
                int(normalized[6:8]),
            )
        except ValueError:
            return None

    @classmethod
    def _validate_completed_scan_items(
        cls,
        *,
        raw_items: list[dict[str, Any]],
        corp_code: str,
        window_start: date,
        window_end: date,
    ) -> None:
        for raw_item in raw_items:
            if not str(raw_item.get('rcept_no') or '').strip():
                raise ValueError(
                    'completed disclosure scan contains an item without rcept_no'
                )
            if not str(raw_item.get('report_nm') or '').strip():
                raise ValueError(
                    'completed disclosure scan contains an item without report_nm'
                )
            raw_corp_code = str(raw_item.get('corp_code') or '').strip()
            if raw_corp_code and raw_corp_code != corp_code:
                raise ValueError(
                    'completed disclosure scan contains an out-of-scope corporation'
                )
            receipt_date = cls._parse_dart_receipt_date(raw_item.get('rcept_dt'))
            if receipt_date is None or not window_start <= receipt_date <= window_end:
                raise ValueError(
                    'completed disclosure scan contains an invalid receipt date'
                )

    def _store_dilution_signal_rows(
        self,
        evidence_rows: list[dict[str, Any]],
    ) -> None:
        with session_scope() as session:
            for index in range(0, len(evidence_rows), 1_000):
                values = []
                for evidence_row in evidence_rows[index:index + 1_000]:
                    event = evidence_row['event']
                    dilution_signal = evidence_row['dilution_signal']
                    values.append(
                        {
                            'disclosure_event_id': event.get('id'),
                            'rcept_no': event['rcept_no'],
                            'corp_code': event['corp_code'],
                            'stock_code': event['stock_code'],
                            'corp_name': event['corp_name'],
                            'receipt_date': event['receipt_date'],
                            'report_nm': event['report_nm'],
                            'signal_type': dilution_signal['signal_type'],
                            'has_dilution_signal': dilution_signal[
                                'has_dilution_signal'
                            ],
                            'security_type': dilution_signal['security_type'],
                            'reason': dilution_signal['reason'],
                            'payload': dilution_signal['payload'],
                        }
                    )
                stmt = pg_insert(DartDilutionSignal).values(values)
                session.execute(
                    stmt.on_conflict_do_update(
                        constraint='uq_dart_dilution_signals_rcept_no',
                        set_={
                            'disclosure_event_id': func.coalesce(
                                stmt.excluded.disclosure_event_id,
                                DartDilutionSignal.disclosure_event_id,
                            ),
                            'corp_code': stmt.excluded.corp_code,
                            'stock_code': stmt.excluded.stock_code,
                            'corp_name': stmt.excluded.corp_name,
                            'receipt_date': stmt.excluded.receipt_date,
                            'report_nm': stmt.excluded.report_nm,
                            'signal_type': stmt.excluded.signal_type,
                            'has_dilution_signal': (
                                stmt.excluded.has_dilution_signal
                            ),
                            'security_type': stmt.excluded.security_type,
                            'reason': stmt.excluded.reason,
                            'payload': stmt.excluded.payload,
                        },
                        where=self._dilution_signal_upsert_cas_condition(stmt),
                    )
                )

    @staticmethod
    def _dilution_signal_upsert_cas_condition(stmt: Any) -> Any:
        excluded_authority_strength = case(
            (
                stmt.excluded.payload[
                    'evidence_semantics'
                ].as_string()
                == 'completed_scan_attempt',
                1,
            ),
            else_=0,
        )
        stored_authority_strength = case(
            (
                DartDilutionSignal.payload[
                    'evidence_semantics'
                ].as_string()
                == 'completed_scan_attempt',
                1,
            ),
            else_=0,
        )
        return and_(
            stmt.excluded.receipt_date >= DartDilutionSignal.receipt_date,
            or_(
                ~stmt.excluded.rcept_no.like('DILATT-%'),
                stmt.excluded.receipt_date > DartDilutionSignal.receipt_date,
                excluded_authority_strength >= stored_authority_strength,
            ),
        )

    def _store_signals(
        self,
        *,
        event: dict[str, Any],
        audit_signal: ParsedAuditSignal | None,
        dilution_signal: dict[str, Any] | None,
        audit_signal_evaluated: bool = False,
    ) -> None:
        if audit_signal is None and audit_signal_evaluated:
            raise ValueError(
                'completed audit evaluation requires an opinion signal or '
                'authoritative absence sentinel'
            )
        if audit_signal is not None:
            classification = classify_durable_audit_signal(
                audit_signal,
                event_id=event.get('id'),
                rcept_no=event.get('rcept_no'),
                receipt_date=event.get('receipt_date'),
            )
            if classification == 'authoritative_absence':
                if not audit_signal_evaluated:
                    raise ValueError(
                        'authoritative absence sentinel fields are invalid'
                    )
            elif classification != 'parsed_opinion':
                raise ValueError(
                    'audit signal failed strict validation for durable evidence'
                )
        with session_scope() as session:
            dart_signal_mutation = (
                audit_signal is not None
                or audit_signal_evaluated
                or dilution_signal is not None
            )
            if (
                dart_signal_mutation
                and session.get_bind().dialect.name == 'postgresql'
                and not _AUDIT_SIGNAL_WRITER_LOCK_OWNED.get()
            ):
                # All audit/dilution signal mutations flow through this
                # transaction.  A transaction-scoped advisory lock therefore
                # serializes ordinary capture/backfill writers with exact
                # audit and factor repair fences.
                session.execute(
                    text(
                        'SELECT pg_advisory_xact_lock('
                        'hashtext(:audit_signal_writer_lock_key))'
                    ),
                    {
                        'audit_signal_writer_lock_key': (
                            AUDIT_SIGNAL_WRITER_LOCK_KEY
                        )
                    },
                )
            if audit_signal is not None:
                stmt = pg_insert(DartAuditSignal).values(
                    disclosure_event_id=event['id'],
                    rcept_no=event['rcept_no'],
                    corp_code=event['corp_code'],
                    stock_code=event['stock_code'],
                    corp_name=event['corp_name'],
                    receipt_date=event['receipt_date'],
                    report_nm=event['report_nm'],
                    opinion_code=audit_signal.opinion_code,
                    opinion_label=audit_signal.opinion_label,
                    has_audit_issue=audit_signal.has_audit_issue,
                    source_section=audit_signal.source_section,
                    evidence_text=audit_signal.evidence_text,
                    payload=audit_signal.payload,
                )
                session.execute(
                    stmt.on_conflict_do_update(
                        constraint='uq_dart_audit_signals_rcept_no',
                        set_={
                            'disclosure_event_id': stmt.excluded.disclosure_event_id,
                            'corp_code': stmt.excluded.corp_code,
                            'stock_code': stmt.excluded.stock_code,
                            'corp_name': stmt.excluded.corp_name,
                            'receipt_date': stmt.excluded.receipt_date,
                            'report_nm': stmt.excluded.report_nm,
                            'opinion_code': stmt.excluded.opinion_code,
                            'opinion_label': stmt.excluded.opinion_label,
                            'has_audit_issue': stmt.excluded.has_audit_issue,
                            'source_section': stmt.excluded.source_section,
                            'evidence_text': stmt.excluded.evidence_text,
                            'payload': stmt.excluded.payload,
                        },
                    )
                )
            if dilution_signal is not None:
                self._execute_dilution_upsert(
                    session,
                    event=event,
                    dilution_signal=dilution_signal,
                )

    @staticmethod
    def _execute_dilution_upsert(
        session: Any,
        *,
        event: dict[str, Any],
        dilution_signal: dict[str, Any],
    ) -> None:
        stmt = pg_insert(DartDilutionSignal).values(
            disclosure_event_id=event.get('id'),
            rcept_no=event['rcept_no'],
            corp_code=event['corp_code'],
            stock_code=event['stock_code'],
            corp_name=event['corp_name'],
            receipt_date=event['receipt_date'],
            report_nm=event['report_nm'],
            signal_type=dilution_signal['signal_type'],
            has_dilution_signal=dilution_signal['has_dilution_signal'],
            security_type=dilution_signal['security_type'],
            reason=dilution_signal['reason'],
            payload=dilution_signal['payload'],
        )
        session.execute(
            stmt.on_conflict_do_update(
                constraint='uq_dart_dilution_signals_rcept_no',
                set_={
                    'disclosure_event_id': func.coalesce(
                        stmt.excluded.disclosure_event_id,
                        DartDilutionSignal.disclosure_event_id,
                    ),
                    'corp_code': stmt.excluded.corp_code,
                    'stock_code': stmt.excluded.stock_code,
                    'corp_name': stmt.excluded.corp_name,
                    'receipt_date': stmt.excluded.receipt_date,
                    'report_nm': stmt.excluded.report_nm,
                    'signal_type': stmt.excluded.signal_type,
                    'has_dilution_signal': stmt.excluded.has_dilution_signal,
                    'security_type': stmt.excluded.security_type,
                    'reason': stmt.excluded.reason,
                    'payload': stmt.excluded.payload,
                },
            )
        )


def _clean_html_text(value: str) -> str:
    text = _TAG_PATTERN.sub(' ', value)
    text = unescape(text)
    text = _WS_PATTERN.sub(' ', text)
    return text.strip()


def _audit_document_diagnostic_counts(document: str) -> dict[str, int]:
    audit_report_label_row_count = 0
    known_opinion_token_row_count = 0
    for row_html in _AUDIT_ROW_PATTERN.findall(document):
        row_text = _clean_html_text(row_html)
        if '감사보고서' in row_text or '연결감사보고서' in row_text:
            audit_report_label_row_count += 1
        if any(pattern.search(row_text) for pattern, _, _ in _AUDIT_PATTERNS):
            known_opinion_token_row_count += 1
    return {
        'aud_opn_table_count': len(_AUDIT_TABLE_PATTERN.findall(document)),
        'audit_report_label_row_count': audit_report_label_row_count,
        'known_opinion_token_row_count': known_opinion_token_row_count,
    }


def _has_complete_audit_authority_evaluation(
    diagnostics: dict[str, Any] | None,
) -> bool:
    """Return whether a negative parse may revoke prior audit authority.

    A downloaded archive is not a completed authority evaluation merely because
    bounded diagnostics were produced.  Every member must be decodable and at
    least one decoded document must contain audit-opinion evidence.  Otherwise
    the absence of a parsed signal remains retry debt and the last-known-good
    signal must be preserved.
    """

    if not isinstance(diagnostics, dict):
        return False
    member_count = diagnostics.get('member_count')
    decoded_member_count = diagnostics.get('decoded_member_count')
    if (
        isinstance(member_count, bool)
        or not isinstance(member_count, int)
        or member_count <= 0
        or isinstance(decoded_member_count, bool)
        or not isinstance(decoded_member_count, int)
        or decoded_member_count != member_count
    ):
        return False
    positive_count = diagnostics.get('positive_authority_evidence_count')
    negative_count = diagnostics.get('negative_authority_evidence_count')
    ambiguous_count = diagnostics.get('ambiguous_authority_evidence_count')
    return (
        isinstance(positive_count, int)
        and not isinstance(positive_count, bool)
        and positive_count == 0
        and isinstance(negative_count, int)
        and not isinstance(negative_count, bool)
        and negative_count > 0
        and isinstance(ambiguous_count, int)
        and not isinstance(ambiguous_count, bool)
        and ambiguous_count == 0
    )


def _decode_document_text(raw_bytes: bytes) -> str | None:
    for encoding in _iter_document_encodings(raw_bytes):
        try:
            return raw_bytes.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return None


def _iter_document_encodings(raw_bytes: bytes) -> list[str]:
    declared_encoding = _detect_declared_encoding(raw_bytes)
    encodings: list[str] = []
    seen: set[str] = set()
    for encoding in (declared_encoding, 'utf-8', 'cp949', 'euc-kr'):
        if not encoding:
            continue
        normalized = encoding.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        encodings.append(encoding)
    return encodings


def _detect_declared_encoding(raw_bytes: bytes) -> str | None:
    header = raw_bytes[:2048].decode('ascii', errors='ignore')
    match = _ENCODING_DECLARATION_PATTERN.search(header)
    if not match:
        return None
    return _ENCODING_ALIASES.get(match.group(1).strip().lower(), match.group(1).strip())


dart_signals_service = DartSignalsService()
