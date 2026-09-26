import io
import unittest
import zipfile
from contextlib import contextmanager
from datetime import date
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from sqlalchemy.orm import Session

from app.db import engine
from app.db_models import (
    DailyPriceBar,
    DartAuditSignal,
    DartDisclosureEvent,
    DartFundamentalsSnapshot,
    Symbol,
)
from app.dart.signals_service import (
    DartSignalsRepository,
    DartSignalsService,
    _decode_document_text,
)


def _build_archive(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode='w') as archive:
        for name, content in members:
            archive.writestr(name, content)
    return buffer.getvalue()


def _audit_document(*opinions):
    rows = ''.join(
        f'<TR><TD>감사보고서</TD><TD>{opinion}</TD></TR>'
        for opinion in opinions
    )
    return f'<TABLE-GROUP ACLASS="AUD_OPN">{rows}</TABLE-GROUP>'.encode('utf-8')


class FakeSignalsClient:
    def __init__(self, *, members):
        self.members = members
        self.download_args = None

    async def download_document(self, *, rcept_no):
        self.download_args = {'rcept_no': rcept_no}
        content = _build_archive(self.members)
        return {
            'content': content,
            'content_type': 'application/zip',
            'filename': 'dart-document.zip',
            'size_bytes': len(content),
        }


class FakeSignalsRepository:
    def __init__(self, candidates):
        self.candidates = list(candidates)
        self.list_args = None

    def list_missing_annual_audit_signal_candidates(self, **kwargs):
        self.list_args = kwargs
        return self.candidates[: kwargs['limit']]


class DartSignalsServiceTests(unittest.IsolatedAsyncioTestCase):
    def test_candidate_query_requires_captured_successful_annual_snapshot_without_audit(self):
        statements = []

        class EmptyResult:
            def mappings(self):
                return self

            def all(self):
                return []

        class RecordingSession:
            def execute(self, statement):
                statements.append(statement)
                return EmptyResult()

        @contextmanager
        def fake_session_scope():
            yield RecordingSession()

        repository = DartSignalsRepository()
        with patch('app.dart.signals_service.session_scope', fake_session_scope):
            result = repository.list_missing_annual_audit_signal_candidates(
                market_date=date(2026, 4, 30),
                limit=4,
                after_event_id=10,
            )

        self.assertEqual(result, [])
        self.assertEqual(len(statements), 1)
        compiled = statements[0].compile()
        parameter_values = list(compiled.params.values())
        self.assertIn('captured', parameter_values)
        self.assertIn('11011', parameter_values)
        self.assertIn(['CFS', 'OFS'], parameter_values)
        self.assertIn('000', parameter_values)
        self.assertIn(10, parameter_values)
        self.assertIn(4, parameter_values)
        compiled_sql = str(compiled)
        self.assertGreaterEqual(compiled_sql.count('row_number() OVER'), 2)
        self.assertIn('daily_price_bars', compiled_sql)
        self.assertIn('dart_audit_signals.rcept_no', compiled_sql)
        self.assertIn('dart_fundamentals_snapshots.disclosure_event_id', compiled_sql)
        self.assertIn(
            'dart_fundamentals_snapshots.rcept_no = dart_disclosure_events.rcept_no',
            compiled_sql,
        )
        self.assertIn(
            'dart_fundamentals_snapshots.stock_code = dart_disclosure_events.stock_code',
            compiled_sql,
        )

    def test_decode_document_text_falls_back_to_cp949_without_losing_korean_keywords(self):
        document = (
            '<TABLE-GROUP ACLASS="AUD_OPN">'
            '<TR><TD>감사보고서</TD><TD>부적정</TD></TR>'
            '</TABLE-GROUP>'
        )

        decoded = _decode_document_text(document.encode('cp949'))

        self.assertIsNotNone(decoded)
        self.assertIn('감사보고서', decoded)
        self.assertIn('부적정', decoded)

    async def test_extract_audit_signal_scans_past_first_twenty_archive_members(self):
        filler_members = [
            (f'attachments/{index:02d}.xml', '<DOCUMENT>no audit table here</DOCUMENT>')
            for index in range(24)
        ]
        audit_member = (
            'reports/late-audit.xml',
            (
                '<?xml version="1.0" encoding="utf-8"?>'
                '<TABLE-GROUP ACLASS="AUD_OPN">'
                '<TR><TD>감사보고서</TD><TD>의견거절</TD></TR>'
                '</TABLE-GROUP>'
            ),
        )
        members = [(name, content.encode('utf-8')) for name, content in filler_members]
        members.append((audit_member[0], audit_member[1].encode('utf-8')))
        client = FakeSignalsClient(members=members)
        service = DartSignalsService(client=client)

        result = await service._extract_audit_signal(
            {
                'rcept_no': '20260422000001',
                'report_nm': '사업보고서',
            }
        )

        self.assertEqual(client.download_args, {'rcept_no': '20260422000001'})
        self.assertIsNotNone(result)
        self.assertEqual(result.opinion_code, 'disclaimer')
        self.assertEqual(result.opinion_label, '의견거절')
        self.assertTrue(result.has_audit_issue)
        self.assertEqual(result.source_section, 'reports/late-audit.xml')
        self.assertIn('의견거절', result.evidence_text)

    async def test_extract_audit_signal_is_order_invariant_and_selects_worst_finding(self):
        member_sets = [
            [
                ('reports/clean.xml', _audit_document('적정의견')),
                ('reports/risk.xml', _audit_document('부적정의견')),
            ],
            [
                ('reports/risk.xml', _audit_document('부적정의견')),
                ('reports/clean.xml', _audit_document('적정의견')),
            ],
        ]

        results = []
        for members in member_sets:
            result = await DartSignalsService(
                client=FakeSignalsClient(members=members)
            )._extract_audit_signal(
                {'rcept_no': '20260422000002', 'report_nm': '사업보고서'}
            )
            results.append(result)

        for result in results:
            self.assertIsNotNone(result)
            self.assertEqual(result.opinion_code, 'adverse')
            self.assertTrue(result.has_audit_issue)
            self.assertEqual(result.source_section, 'reports/risk.xml')
            self.assertEqual(result.payload['evidence_count'], 2)
            self.assertEqual(
                result.payload['conflicting_opinion_codes'],
                ['adverse', 'unqualified'],
            )
            self.assertEqual(
                result.payload['source_sections'],
                ['reports/clean.xml', 'reports/risk.xml'],
            )

    async def test_extract_audit_signal_scans_every_relevant_row(self):
        client = FakeSignalsClient(
            members=[
                (
                    'reports/mixed.xml',
                    _audit_document('적정의견', '한정의견', '의견거절'),
                )
            ]
        )

        result = await DartSignalsService(client=client)._extract_audit_signal(
            {'rcept_no': '20260422000003', 'report_nm': '사업보고서'}
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.opinion_code, 'disclaimer')
        self.assertEqual(result.payload['evidence_count'], 3)
        self.assertEqual(
            result.payload['findings_by_opinion'],
            {'disclaimer': 1, 'qualified': 1, 'unqualified': 1},
        )

    async def test_extract_audit_signal_rejects_oversized_compressed_input(self):
        service = DartSignalsService(client=FakeSignalsClient(members=[]))
        with patch(
            'app.dart.signals_service.MAX_AUDIT_ARCHIVE_COMPRESSED_BYTES',
            8,
        ):
            with self.assertRaisesRegex(ValueError, 'compressed size limit'):
                await service._extract_audit_signal(
                    {'rcept_no': '20260422000004', 'report_nm': '사업보고서'}
                )

    async def test_extract_audit_signal_rejects_archive_member_count_limit(self):
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[('one.xml', b'one'), ('two.xml', b'two')]
            )
        )
        with patch('app.dart.signals_service.MAX_AUDIT_ARCHIVE_MEMBERS', 1):
            with self.assertRaisesRegex(ValueError, 'member count limit'):
                await service._extract_audit_signal(
                    {'rcept_no': '20260422000005', 'report_nm': '사업보고서'}
                )

    async def test_extract_audit_signal_rejects_zip_bomb_member_and_total_sizes(self):
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[('one.xml', b'a' * 8), ('two.xml', b'b' * 8)]
            )
        )
        with patch('app.dart.signals_service.MAX_AUDIT_ARCHIVE_MEMBER_BYTES', 7):
            with self.assertRaisesRegex(ValueError, 'member exceeds size limit'):
                await service._extract_audit_signal(
                    {'rcept_no': '20260422000006', 'report_nm': '사업보고서'}
                )
        with (
            patch('app.dart.signals_service.MAX_AUDIT_ARCHIVE_MEMBER_BYTES', 8),
            patch('app.dart.signals_service.MAX_AUDIT_ARCHIVE_EXPANDED_BYTES', 15),
        ):
            with self.assertRaisesRegex(ValueError, 'expanded size limit'):
                await service._extract_audit_signal(
                    {'rcept_no': '20260422000006', 'report_nm': '사업보고서'}
                )

    async def test_backfill_missing_audit_signals_marks_more_pages_partial_without_retry(self):
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': 11,
                    'rcept_no': '20260422000011',
                    'stock_code': '005930',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                },
                {
                    'event_id': 12,
                    'rcept_no': '20260422000012',
                    'stock_code': '000660',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                },
                {
                    'event_id': 13,
                    'rcept_no': '20260422000013',
                    'stock_code': '035420',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                },
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
            event_timeout_seconds=5,
        )
        service.capture_audit_signal_for_event = AsyncMock(
            side_effect=[
                {
                    'rcept_no': '20260422000011',
                    'audit_signal': {'opinion_code': 'unqualified'},
                },
                {
                    'rcept_no': '20260422000012',
                    'audit_signal': {'opinion_code': 'unqualified'},
                },
            ]
        )

        result = await service.backfill_missing_annual_audit_signals(
            market_date=date(2026, 4, 30),
            limit=2,
            after_event_id=10,
            dry_run=False,
        )

        self.assertEqual(
            repository.list_args,
            {
                'market_date': date(2026, 4, 30),
                'limit': 3,
                'after_event_id': 10,
            },
        )
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['market_date'], '2026-04-30')
        self.assertEqual(result['candidates_attempted'], 2)
        self.assertEqual(result['audit_signals_captured'], 2)
        self.assertEqual(result['audit_evidence_missing'], 0)
        self.assertEqual(result['events_failed'], 0)
        self.assertTrue(result['has_more_after_cursor'])
        self.assertFalse(result['retry_required'])
        self.assertEqual(result['next_after_event_id'], 12)
        self.assertEqual(result['next_forward_after_event_id'], 12)
        self.assertTrue(result['forward_scan_has_more'])
        self.assertEqual(result['retry_event_ids'], [])
        self.assertEqual(service.capture_audit_signal_for_event.await_count, 2)

    async def test_backfill_missing_audit_signals_reports_retryable_evidence_gap(self):
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': 18,
                    'rcept_no': '20260422000018',
                    'stock_code': '005930',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                }
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
        )
        service.capture_audit_signal_for_event = AsyncMock(
            return_value={
                'rcept_no': '20260422000018',
                'audit_signal': None,
            }
        )

        result = await service.backfill_missing_annual_audit_signals(
            market_date=date(2026, 4, 30),
            limit=1,
            after_event_id=17,
            dry_run=False,
        )

        self.assertEqual(result['status'], 'completed_with_gaps')
        self.assertFalse(result['has_more_after_cursor'])
        self.assertTrue(result['retry_required'])
        self.assertEqual(result['retry_event_ids'], [18])
        self.assertEqual(result['audit_evidence_missing_event_ids'], [18])
        self.assertEqual(result['failed_event_ids'], [])
        self.assertEqual(result['next_after_event_id'], 17)
        self.assertEqual(result['next_forward_after_event_id'], 18)
        self.assertFalse(result['forward_scan_has_more'])

    async def test_backfill_gap_page_exposes_separate_forward_scan_cursor(self):
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': event_id,
                    'rcept_no': f'202604220000{event_id}',
                    'stock_code': f'{event_id:06d}',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                }
                for event_id in (11, 12, 13)
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
        )
        service.capture_audit_signal_for_event = AsyncMock(
            side_effect=[
                {'rcept_no': '20260422000011', 'audit_signal': None},
                {'rcept_no': '20260422000012', 'audit_signal': None},
            ]
        )

        result = await service.backfill_missing_annual_audit_signals(
            market_date=date(2026, 4, 30),
            limit=2,
            after_event_id=10,
            dry_run=False,
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['audit_evidence_missing'], 2)
        self.assertEqual(result['retry_event_ids'], [11, 12])
        self.assertEqual(result['next_after_event_id'], 10)
        self.assertEqual(result['next_forward_after_event_id'], 12)
        self.assertTrue(result['forward_scan_has_more'])
        self.assertEqual(service.capture_audit_signal_for_event.await_count, 2)

    async def test_backfill_missing_audit_signals_defaults_to_non_advancing_dry_run(self):
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': 21,
                    'rcept_no': '20260422000021',
                    'stock_code': '005930',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                }
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
        )
        service.capture_audit_signal_for_event = AsyncMock()

        result = await service.backfill_missing_annual_audit_signals(
            market_date=date(2026, 4, 30),
            limit=1,
        )

        self.assertEqual(result['status'], 'dry_run')
        self.assertEqual(result['candidates_attempted'], 0)
        self.assertEqual(result['candidate_count'], 1)
        self.assertEqual(result['next_after_event_id'], 0)
        self.assertEqual(result['next_forward_after_event_id'], 21)
        self.assertFalse(result['forward_scan_has_more'])
        self.assertFalse(result['retry_required'])
        service.capture_audit_signal_for_event.assert_not_awaited()

    async def test_backfill_missing_audit_signals_contains_event_failure_and_keeps_cursor(self):
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': 31,
                    'rcept_no': '20260422000031',
                    'stock_code': '005930',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                }
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
        )
        service.capture_audit_signal_for_event = AsyncMock(
            side_effect=RuntimeError('provider unavailable')
        )

        result = await service.backfill_missing_annual_audit_signals(
            market_date=date(2026, 4, 30),
            limit=1,
            after_event_id=30,
            dry_run=False,
        )

        self.assertEqual(result['status'], 'completed_with_gaps')
        self.assertEqual(result['events_failed'], 1)
        self.assertEqual(result['next_after_event_id'], 30)
        self.assertEqual(result['next_forward_after_event_id'], 31)
        self.assertFalse(result['forward_scan_has_more'])
        self.assertTrue(result['retry_required'])
        self.assertEqual(result['retry_event_ids'], [31])
        self.assertEqual(result['failed_event_ids'], [31])
        self.assertEqual(result['results'][0]['status'], 'failed')
        self.assertIn('provider unavailable', result['results'][0]['error'])

    async def test_backfill_missing_audit_signals_trips_systemic_failure_circuit_breaker(self):
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': event_id,
                    'rcept_no': f'202604220000{event_id}',
                    'stock_code': f'{event_id:06d}',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                }
                for event_id in range(41, 46)
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
        )
        service.capture_audit_signal_for_event = AsyncMock(
            side_effect=TimeoutError('DART timed out')
        )

        result = await service.backfill_missing_annual_audit_signals(
            market_date=date(2026, 4, 30),
            limit=5,
            after_event_id=40,
            dry_run=False,
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['candidates_attempted'], 3)
        self.assertEqual(result['retry_event_ids'], [41, 42, 43])
        self.assertEqual(result['unattempted_event_ids'], [44, 45])
        self.assertEqual(result['next_after_event_id'], 40)
        self.assertEqual(result['next_forward_after_event_id'], 43)
        self.assertTrue(result['forward_scan_has_more'])
        self.assertTrue(result['has_more_after_cursor'])
        self.assertTrue(result['retry_required'])
        self.assertEqual(
            result['circuit_breaker'],
            {
                'tripped': True,
                'threshold': 3,
                'consecutive_systemic_failures': 3,
            },
        )
        self.assertEqual(service.capture_audit_signal_for_event.await_count, 3)

    async def test_backfill_missing_audit_signals_rejects_unbounded_or_invalid_scope(self):
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=FakeSignalsRepository([]),
        )

        with self.assertRaisesRegex(ValueError, 'limit must be between 1 and 100'):
            await service.backfill_missing_annual_audit_signals(
                market_date=date(2026, 4, 30),
                limit=101,
            )
        with self.assertRaisesRegex(ValueError, 'after_event_id'):
            await service.backfill_missing_annual_audit_signals(
                market_date=date(2026, 4, 30),
                after_event_id=-1,
            )


class DartSignalsRepositoryPostgresTests(unittest.TestCase):
    def test_point_in_time_candidate_selection_matches_piotroski_audit_consumption(self):
        market_date = date(2099, 12, 30)
        symbol_prefix = f'T{uuid4().hex[:8]}'

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)

            def add_symbol(suffix: str, *, with_price: bool = True) -> Symbol:
                symbol = Symbol(
                    symbol=f'{symbol_prefix}{suffix}',
                    name_ko=f'Test {suffix}',
                    market='KOSPI',
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                )
                session.add(symbol)
                session.flush()
                if with_price:
                    session.add(
                        DailyPriceBar(
                            symbol_id=symbol.id,
                            trade_date=market_date,
                            open_price=100,
                            high_price=110,
                            low_price=90,
                            close_price=105,
                            volume=1_000,
                            turnover=105_000,
                            source='test',
                        )
                    )
                    session.flush()
                return symbol

            def add_snapshot(
                symbol: Symbol,
                *,
                business_year: int,
                receipt_date: date,
                rcept_no: str,
                fs_div: str = 'CFS',
                event: DartDisclosureEvent | None = None,
            ) -> tuple[DartDisclosureEvent, DartFundamentalsSnapshot]:
                resolved_event = event
                if resolved_event is None:
                    resolved_event = DartDisclosureEvent(
                        rcept_no=rcept_no,
                        corp_code=f'C{uuid4().hex[:12]}',
                        corp_name=f'Test {symbol.symbol}',
                        stock_code=symbol.symbol,
                        corp_cls='Y',
                        report_nm='사업보고서',
                        filing_category='annual_report',
                        is_financial_report=True,
                        receipt_date=receipt_date,
                        payload={'rcept_no': rcept_no},
                        fundamentals_capture_status='captured',
                    )
                    session.add(resolved_event)
                    session.flush()
                snapshot = DartFundamentalsSnapshot(
                    disclosure_event_id=resolved_event.id,
                    capture_run_id=None,
                    rcept_no=resolved_event.rcept_no,
                    corp_code=resolved_event.corp_code,
                    stock_code=symbol.symbol,
                    corp_name=resolved_event.corp_name,
                    business_year=business_year,
                    report_code='11011',
                    filing_category='annual_report',
                    fs_div=fs_div,
                    statement_type='financial_statement',
                    period_type='annual',
                    period_end_month=12,
                    currency='KRW',
                    source_status='000',
                    source_message='OK',
                    request_payload={},
                    response_payload={},
                )
                session.add(snapshot)
                session.flush()
                return resolved_event, snapshot

            eligible = add_symbol('A')
            _, eligible_snapshot = add_snapshot(
                eligible,
                business_year=2098,
                receipt_date=date(2099, 3, 1),
                rcept_no=f'R{uuid4().hex[:20]}',
            )

            no_price = add_symbol('B', with_price=False)
            add_snapshot(
                no_price,
                business_year=2098,
                receipt_date=date(2099, 3, 1),
                rcept_no=f'R{uuid4().hex[:20]}',
            )

            future_filing = add_symbol('C')
            add_snapshot(
                future_filing,
                business_year=2098,
                receipt_date=date(2099, 12, 31),
                rcept_no=f'R{uuid4().hex[:20]}',
            )

            corrected = add_symbol('D')
            add_snapshot(
                corrected,
                business_year=2098,
                receipt_date=date(2099, 3, 1),
                rcept_no=f'R{uuid4().hex[:20]}',
            )
            _, corrected_latest_snapshot = add_snapshot(
                corrected,
                business_year=2098,
                receipt_date=date(2099, 4, 1),
                rcept_no=f'R{uuid4().hex[:20]}',
            )

            cfs_preferred = add_symbol('E')
            shared_event, _ = add_snapshot(
                cfs_preferred,
                business_year=2098,
                receipt_date=date(2099, 5, 1),
                rcept_no=f'R{uuid4().hex[:20]}',
                fs_div='OFS',
            )
            _, cfs_snapshot = add_snapshot(
                cfs_preferred,
                business_year=2098,
                receipt_date=date(2099, 5, 1),
                rcept_no=shared_event.rcept_no,
                fs_div='CFS',
                event=shared_event,
            )

            latest_year = add_symbol('F')
            add_snapshot(
                latest_year,
                business_year=2097,
                receipt_date=date(2099, 6, 1),
                rcept_no=f'R{uuid4().hex[:20]}',
            )
            _, latest_year_snapshot = add_snapshot(
                latest_year,
                business_year=2098,
                receipt_date=date(2099, 2, 1),
                rcept_no=f'R{uuid4().hex[:20]}',
            )

            audited = add_symbol('G')
            audited_event, _ = add_snapshot(
                audited,
                business_year=2098,
                receipt_date=date(2099, 7, 1),
                rcept_no=f'R{uuid4().hex[:20]}',
            )
            session.add(
                DartAuditSignal(
                    disclosure_event_id=audited_event.id,
                    rcept_no=audited_event.rcept_no,
                    corp_code=audited_event.corp_code,
                    stock_code=audited.symbol,
                    corp_name=audited_event.corp_name,
                    receipt_date=audited_event.receipt_date,
                    report_nm=audited_event.report_nm,
                    opinion_code='unqualified',
                    opinion_label='적정',
                    has_audit_issue=False,
                    source_section='AUD_OPN',
                    evidence_text='적정',
                    payload={},
                )
            )
            session.flush()

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            try:
                repository = DartSignalsRepository()
                with patch(
                    'app.dart.signals_service.session_scope',
                    rollback_only_session_scope,
                ):
                    rows = repository.list_missing_annual_audit_signal_candidates(
                        market_date=market_date,
                        limit=100,
                    )

                rows_by_symbol = {row['stock_code']: row for row in rows}
                self.assertEqual(
                    set(rows_by_symbol),
                    {
                        eligible.symbol,
                        corrected.symbol,
                        cfs_preferred.symbol,
                        latest_year.symbol,
                    },
                )
                self.assertEqual(
                    rows_by_symbol[eligible.symbol]['rcept_no'],
                    eligible_snapshot.rcept_no,
                )
                self.assertNotIn(no_price.symbol, rows_by_symbol)
                self.assertNotIn(future_filing.symbol, rows_by_symbol)
                self.assertEqual(
                    rows_by_symbol[corrected.symbol]['rcept_no'],
                    corrected_latest_snapshot.rcept_no,
                )
                self.assertEqual(
                    rows_by_symbol[cfs_preferred.symbol]['snapshot_id'],
                    cfs_snapshot.id,
                )
                self.assertEqual(rows_by_symbol[cfs_preferred.symbol]['fs_div'], 'CFS')
                self.assertEqual(
                    rows_by_symbol[latest_year.symbol]['rcept_no'],
                    latest_year_snapshot.rcept_no,
                )
                self.assertEqual(rows_by_symbol[latest_year.symbol]['business_year'], 2098)
                self.assertNotIn(audited.symbol, rows_by_symbol)
            finally:
                session.close()
                transaction.rollback()

    def test_market_date_runs_are_idempotent_and_keep_corrections_independent(self):
        first_market_date = date(2026, 9, 15)
        corrected_market_date = date(2026, 9, 16)
        symbol_code = f'I{uuid4().hex[:8]}'

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)

            symbol = Symbol(
                symbol=symbol_code,
                name_ko='Idempotency test',
                market='KOSPI',
                instrument_type='equity',
                source='test',
                is_active=True,
            )
            session.add(symbol)
            session.flush()
            for trade_date in (first_market_date, corrected_market_date):
                session.add(
                    DailyPriceBar(
                        symbol_id=symbol.id,
                        trade_date=trade_date,
                        open_price=100,
                        high_price=110,
                        low_price=90,
                        close_price=105,
                        volume=1_000,
                        turnover=105_000,
                        source='test',
                    )
                )

            def add_filing(*, receipt_date: date) -> DartDisclosureEvent:
                rcept_no = f'R{uuid4().hex[:20]}'
                event = DartDisclosureEvent(
                    rcept_no=rcept_no,
                    corp_code=f'C{uuid4().hex[:12]}',
                    corp_name='Idempotency test',
                    stock_code=symbol_code,
                    corp_cls='Y',
                    report_nm='사업보고서',
                    filing_category='annual_report',
                    is_financial_report=True,
                    receipt_date=receipt_date,
                    payload={'rcept_no': rcept_no},
                    fundamentals_capture_status='captured',
                )
                session.add(event)
                session.flush()
                session.add(
                    DartFundamentalsSnapshot(
                        disclosure_event_id=event.id,
                        capture_run_id=None,
                        rcept_no=event.rcept_no,
                        corp_code=event.corp_code,
                        stock_code=symbol_code,
                        corp_name=event.corp_name,
                        business_year=2025,
                        report_code='11011',
                        filing_category='annual_report',
                        fs_div='CFS',
                        statement_type='financial_statement',
                        period_type='annual',
                        period_end_month=12,
                        currency='KRW',
                        source_status='000',
                        source_message='OK',
                        request_payload={},
                        response_payload={},
                    )
                )
                session.flush()
                return event

            def add_audit(event: DartDisclosureEvent) -> None:
                session.add(
                    DartAuditSignal(
                        disclosure_event_id=event.id,
                        rcept_no=event.rcept_no,
                        corp_code=event.corp_code,
                        stock_code=symbol_code,
                        corp_name=event.corp_name,
                        receipt_date=event.receipt_date,
                        report_nm=event.report_nm,
                        opinion_code='unqualified',
                        opinion_label='적정',
                        has_audit_issue=False,
                        source_section='AUD_OPN',
                        evidence_text='적정',
                        payload={},
                    )
                )
                session.flush()

            original_event = add_filing(receipt_date=first_market_date)
            corrected_event = add_filing(receipt_date=corrected_market_date)

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            try:
                repository = DartSignalsRepository()
                with patch(
                    'app.dart.signals_service.session_scope',
                    rollback_only_session_scope,
                ):
                    first_candidates = (
                        repository.list_missing_annual_audit_signal_candidates(
                            market_date=first_market_date,
                            limit=10,
                        )
                    )
                    self.assertEqual(
                        [row['rcept_no'] for row in first_candidates],
                        [original_event.rcept_no],
                    )

                    add_audit(original_event)
                    self.assertEqual(
                        repository.list_missing_annual_audit_signal_candidates(
                            market_date=first_market_date,
                            limit=10,
                        ),
                        [],
                    )

                    corrected_candidates = (
                        repository.list_missing_annual_audit_signal_candidates(
                            market_date=corrected_market_date,
                            limit=10,
                        )
                    )
                    self.assertEqual(
                        [row['rcept_no'] for row in corrected_candidates],
                        [corrected_event.rcept_no],
                    )

                    add_audit(corrected_event)
                    self.assertEqual(
                        repository.list_missing_annual_audit_signal_candidates(
                            market_date=corrected_market_date,
                            limit=10,
                        ),
                        [],
                    )
            finally:
                session.close()
                transaction.rollback()
