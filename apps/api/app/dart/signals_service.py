from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from html import unescape
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import session_scope
from app.db_models import DartAuditSignal, DartDilutionSignal, DartDisclosureEvent
from app.integrations.dart.client import DartClient, dart_client

_AUDIT_PATTERNS = [
    (re.compile(r'의견거절'), 'disclaimer', '의견거절'),
    (re.compile(r'부적정의견|부적정'), 'adverse', '부적정'),
    (re.compile(r'한정의견|한정'), 'qualified', '한정'),
    (re.compile(r'적정의견|적정'), 'unqualified', '적정'),
]

_AUDIT_TABLE_PATTERN = re.compile(
    r'<TABLE-GROUP[^>]+ACLASS="AUD_OPN"[^>]*>(.*?)</TABLE-GROUP>',
    re.IGNORECASE | re.DOTALL,
)
_AUDIT_ROW_PATTERN = re.compile(r'<TR[^>]*>(.*?)</TR>', re.IGNORECASE | re.DOTALL)
_AUDIT_CELL_PATTERN = re.compile(r'<T[EDHU][^>]*>(.*?)</T[EDHU]>', re.IGNORECASE | re.DOTALL)
_TAG_PATTERN = re.compile(r'<[^>]+>')
_WS_PATTERN = re.compile(r'\s+')

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


class DartSignalsService:
    def __init__(self, client: DartClient | None = None) -> None:
        self.client = client or dart_client

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
        with zipfile.ZipFile(io.BytesIO(artifact['content'])) as archive:
            for name in archive.namelist()[:20]:
                try:
                    content = archive.read(name).decode('utf-8', errors='ignore')
                except Exception:
                    continue
                parsed = self._parse_audit_from_document(content)
                if parsed is not None:
                    parsed.source_section = name
                    return parsed
        return None

    def _parse_audit_from_document(self, text: str) -> ParsedAuditSignal | None:
        table_match = _AUDIT_TABLE_PATTERN.search(text)
        if not table_match:
            return None
        table_html = table_match.group(1)
        for row_html in _AUDIT_ROW_PATTERN.findall(table_html):
            cells = [_clean_html_text(cell) for cell in _AUDIT_CELL_PATTERN.findall(row_html)]
            if not cells:
                continue
            row_text = ' | '.join(cell for cell in cells if cell)
            if '감사보고서' not in row_text and '연결감사보고서' not in row_text:
                continue
            opinion_cell = next((cell for cell in cells if any(p.search(cell) for p, _, _ in _AUDIT_PATTERNS)), None)
            if not opinion_cell:
                continue
            for pattern, code, label in _AUDIT_PATTERNS:
                if pattern.search(opinion_cell):
                    return ParsedAuditSignal(
                        opinion_code=code,
                        opinion_label=label,
                        has_audit_issue=code in {'disclaimer', 'adverse', 'qualified'},
                        source_section='AUD_OPN',
                        evidence_text=row_text,
                        payload={'opinion_cell': opinion_cell, 'row_cells': cells},
                    )
        return None

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


dart_signals_service = DartSignalsService()
