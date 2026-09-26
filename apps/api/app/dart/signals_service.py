from __future__ import annotations

import asyncio
import io
import re
import zipfile
from dataclasses import dataclass
from datetime import date
from html import unescape
from typing import Any

from sqlalchemy import and_, case, func, select
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
from app.dart.sync_service import _is_systemic_provider_failure
from app.integrations.dart.client import DartClient, dart_client


DEFAULT_AUDIT_BACKFILL_LIMIT = 25
MAX_AUDIT_BACKFILL_LIMIT = 100
AUDIT_BACKFILL_SYSTEMIC_FAILURE_LIMIT = 3
AUDIT_BACKFILL_SELECTION_POLICY = 'piotroski_latest_annual_audit_v1'
MAX_AUDIT_ARCHIVE_COMPRESSED_BYTES = 50 * 1024 * 1024
MAX_AUDIT_ARCHIVE_MEMBERS = 2_000
MAX_AUDIT_ARCHIVE_MEMBER_BYTES = 25 * 1024 * 1024
MAX_AUDIT_ARCHIVE_EXPANDED_BYTES = 100 * 1024 * 1024

_AUDIT_PATTERNS = [
    (re.compile(r'의견거절'), 'disclaimer', '의견거절'),
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

_AUDIT_TABLE_PATTERN = re.compile(
    r'<TABLE-GROUP[^>]+ACLASS="AUD_OPN"[^>]*>(.*?)</TABLE-GROUP>',
    re.IGNORECASE | re.DOTALL,
)
_AUDIT_ROW_PATTERN = re.compile(r'<TR[^>]*>(.*?)</TR>', re.IGNORECASE | re.DOTALL)
_AUDIT_CELL_PATTERN = re.compile(r'<T[EDHU][^>]*>(.*?)</T[EDHU]>', re.IGNORECASE | re.DOTALL)
_TAG_PATTERN = re.compile(r'<[^>]+>')
_WS_PATTERN = re.compile(r'\s+')
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


@dataclass
class ParsedAuditSignal:
    opinion_code: str | None
    opinion_label: str | None
    has_audit_issue: bool
    source_section: str | None
    evidence_text: str | None
    payload: dict[str, Any] | None


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
        consumed_audit_signal_exists = (
            select(DartAuditSignal.id)
            .where(
                DartAuditSignal.rcept_no == selected_latest_annual.c.rcept_no,
                DartAuditSignal.stock_code == selected_latest_annual.c.stock_code,
                DartAuditSignal.receipt_date.is_not(None),
                DartAuditSignal.receipt_date <= market_date,
            )
            .correlate(selected_latest_annual)
            .exists()
        )
        stmt = (
            select(selected_latest_annual)
            .where(
                selected_latest_annual.c.business_year_rank == 1,
                selected_latest_annual.c.event_id > after_event_id,
                ~consumed_audit_signal_exists,
            )
            .order_by(selected_latest_annual.c.event_id.asc())
            .limit(limit)
        )

        with session_scope() as session:
            rows = session.execute(stmt).mappings().all()
            return [
                {
                    'event_id': int(row['event_id']),
                    'snapshot_id': int(row['snapshot_id']),
                    'rcept_no': str(row['rcept_no']),
                    'stock_code': str(row['stock_code']),
                    'business_year': int(row['business_year']),
                    'fs_div': str(row['fs_div']),
                    'receipt_date': row['receipt_date'].isoformat(),
                    'report_nm': str(row['report_nm']),
                }
                for row in rows
            ]


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
        audit_signal = await self._extract_audit_signal(event)
        dilution_signal = self._extract_dilution_signal(event)
        self._store_signals(event=event, audit_signal=audit_signal, dilution_signal=dilution_signal)
        return {
            'rcept_no': rcept_no,
            'audit_signal': audit_signal.__dict__ if audit_signal else None,
            'dilution_signal': dilution_signal,
        }

    async def capture_audit_signal_for_event(self, *, rcept_no: str) -> dict[str, Any]:
        event = self._load_event(rcept_no)
        audit_signal = await self._extract_audit_signal(event)
        self._store_signals(event=event, audit_signal=audit_signal, dilution_signal=None)
        return {
            'rcept_no': rcept_no,
            'audit_signal': audit_signal.__dict__ if audit_signal else None,
        }

    async def backfill_missing_annual_audit_signals(
        self,
        *,
        market_date: date,
        limit: int = DEFAULT_AUDIT_BACKFILL_LIMIT,
        after_event_id: int = 0,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        if not 1 <= limit <= MAX_AUDIT_BACKFILL_LIMIT:
            raise ValueError(
                f'limit must be between 1 and {MAX_AUDIT_BACKFILL_LIMIT}'
            )
        if after_event_id < 0:
            raise ValueError('after_event_id must be greater than or equal to 0')
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
        for candidate_index, candidate in enumerate(candidates):
            event_id = int(candidate['event_id'])
            last_attempted_event_id = event_id
            result_item = dict(candidate)
            try:
                capture_result = await asyncio.wait_for(
                    self.capture_audit_signal_for_event(
                        rcept_no=str(candidate['rcept_no']),
                    ),
                    timeout=self.event_timeout_seconds,
                )
            except Exception as exc:
                systemic_failure = _is_systemic_provider_failure(exc)
                consecutive_systemic_failures = (
                    consecutive_systemic_failures + 1
                    if systemic_failure
                    else 0
                )
                failed_count += 1
                retry_event_ids.append(event_id)
                failed_event_ids.append(event_id)
                result_item.update(
                    {
                        'status': 'failed',
                        'systemic_provider_failure': systemic_failure,
                        'error': f'{type(exc).__name__}: {exc}'[:500],
                    }
                )
            else:
                consecutive_systemic_failures = 0
                audit_signal = capture_result.get('audit_signal')
                if audit_signal:
                    captured_count += 1
                    result_item.update(
                        {
                            'status': 'captured',
                            'opinion_code': audit_signal.get('opinion_code'),
                            'has_audit_issue': audit_signal.get('has_audit_issue'),
                        }
                    )
                else:
                    missing_count += 1
                    retry_event_ids.append(event_id)
                    missing_evidence_event_ids.append(event_id)
                    result_item['status'] = 'audit_evidence_missing'
            results.append(result_item)
            if (
                consecutive_systemic_failures
                >= AUDIT_BACKFILL_SYSTEMIC_FAILURE_LIMIT
            ):
                circuit_breaker_tripped = True
                unattempted_event_ids = [
                    int(item['event_id'])
                    for item in candidates[candidate_index + 1 :]
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
        if not any(token in (event.get('report_nm') or '') for token in ('사업보고서', '반기보고서', '분기보고서', '감사보고서')):
            return None
        artifact = await self.client.download_document(rcept_no=event['rcept_no'])
        content = artifact.get('content')
        if not isinstance(content, bytes):
            raise ValueError('DART document archive content must be bytes')
        return await asyncio.to_thread(
            self._extract_audit_signal_from_archive,
            content,
        )

    def _extract_audit_signal_from_archive(
        self,
        archive_bytes: bytes,
    ) -> ParsedAuditSignal | None:
        if len(archive_bytes) > MAX_AUDIT_ARCHIVE_COMPRESSED_BYTES:
            raise ValueError('DART document archive exceeds compressed size limit')

        findings: list[ParsedAuditSignal] = []
        expanded_bytes = 0
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
                document = _decode_document_text(raw_content)
                if document is None:
                    continue
                findings.extend(
                    self._parse_audit_findings_from_document(
                        document,
                        source_section=info.filename,
                    )
                )
        return self._select_conservative_audit_signal(findings)

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
        for table_html in _AUDIT_TABLE_PATTERN.findall(text):
            for row_html in _AUDIT_ROW_PATTERN.findall(table_html):
                cells = [
                    _clean_html_text(cell)
                    for cell in _AUDIT_CELL_PATTERN.findall(row_html)
                ]
                if not cells:
                    continue
                row_text = ' | '.join(cell for cell in cells if cell)
                if '감사보고서' not in row_text and '연결감사보고서' not in row_text:
                    continue
                opinion_cell = next(
                    (
                        cell
                        for cell in cells
                        if any(pattern.search(cell) for pattern, _, _ in _AUDIT_PATTERNS)
                    ),
                    None,
                )
                if not opinion_cell:
                    continue
                for pattern, code, label in _AUDIT_PATTERNS:
                    if pattern.search(opinion_cell):
                        findings.append(
                            ParsedAuditSignal(
                                opinion_code=code,
                                opinion_label=label,
                                has_audit_issue=code
                                in {'disclaimer', 'adverse', 'qualified'},
                                source_section=source_section[:255],
                                evidence_text=row_text,
                                payload={
                                    'opinion_cell': opinion_cell,
                                    'row_cells': cells,
                                },
                            )
                        )
                        break
        return findings

    def _select_conservative_audit_signal(
        self,
        findings: list[ParsedAuditSignal],
    ) -> ParsedAuditSignal | None:
        if not findings:
            return None
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
                    'payload': {'matched_token': token},
                }
        return None

    def _store_signals(self, *, event: dict[str, Any], audit_signal: ParsedAuditSignal | None, dilution_signal: dict[str, Any] | None) -> None:
        with session_scope() as session:
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
                stmt = pg_insert(DartDilutionSignal).values(
                    disclosure_event_id=event['id'],
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
