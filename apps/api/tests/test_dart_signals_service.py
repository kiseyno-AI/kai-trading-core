import asyncio
import hashlib
import io
import json
import unittest
import zipfile
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from app.db import engine
from app.db_models import (
    DailyPriceBar,
    DartAuditSignal,
    DartDilutionSignal,
    DartDisclosureEvent,
    DartFundamentalsSnapshot,
    Symbol,
)
from app.dart.signals_service import (
    AUDIT_ARCHIVE_DIAGNOSTICS_VERSION,
    AUDIT_SIGNAL_WRITER_LOCK_KEY,
    DartSignalsRepository,
    DartSignalsService,
    ParsedAuditSignal,
    _decode_document_text,
    audit_signal_writer_lock_owner,
    build_authoritative_absence_audit_signal,
    claims_authoritative_absence_audit_payload,
    classify_durable_audit_signal,
    is_authoritative_absence_audit_payload,
    is_authoritative_absence_audit_signal,
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


def _semantic_audit_document(
    *rows: tuple[str, str],
    table_class: str = 'NORMAL',
    use_table_group: bool = False,
):
    header = (
        '<TR><TH>사업연도</TH><TH>감사인</TH><TH>감사의견</TH>'
        '<TH>강조사항 등</TH><TH>핵심감사사항</TH></TR>'
    )
    body = ''.join(
        '<TR>'
        f'<TD>{business_year}</TD><TD>한영회계법인</TD>'
        f'<TD>{opinion}</TD><TD>-</TD><TD>-</TD>'
        '</TR>'
        for business_year, opinion in rows
    )
    if use_table_group:
        return (
            '<TABLE-GROUP ACLASS="AUD_OPN">'
            f'{header}{body}'
            '</TABLE-GROUP>'
        ).encode('utf-8')
    return (
        f'<TABLE ACLASS="{table_class}">{header}{body}</TABLE>'
    ).encode('utf-8')


def _unmodified_auditor_report_document(
    *,
    same_block: bool = False,
    basis_inline: bool = False,
    extra_content: str = '',
) -> bytes:
    report_body = (
        '우리는 테스트 주식회사(이하 "회사")의 재무제표를 감사하였습니다. '
        '해당 재무제표는 2025년 12월 31일 현재의 재무상태표, 동일로 종료되는 '
        '보고기간의 포괄손익계산서, 자본변동표, 현금흐름표 그리고 중요한 '
        '회계정책 정보를 포함한 재무제표의 주석으로 구성되어 있습니다. '
        '우리의 의견으로는 별첨된 회사의 재무제표는 회사의 2025년 12월 31일 '
        '현재의 재무상태와 동일로 종료되는 보고기간의 재무성과 및 현금흐름을 '
        '한국채택국제회계기준에 따라 중요성의 관점에서 공정하게 표시하고 '
        '있습니다.'
    )
    inline_basis = (
        ' 감사의견 근거 우리는 대한민국의 회계감사기준에 따라 '
        '감사를 수행하였습니다.'
        if basis_inline
        else ''
    )
    opinion_section = (
        '<P><SPAN USERMARK="B">감사의견</SPAN>'
        f'<SPAN>{report_body}{inline_basis}</SPAN></P>'
        if same_block
        else (
            '<P USERMARK="B">감사의견</P>'
            f'<P>{report_body}{inline_basis}</P>'
        )
    )
    separate_basis = (
        ''
        if basis_inline
        else (
            '<P USERMARK="B">감사의견근거</P>'
            '<P>우리는 대한민국의 회계감사기준에 따라 감사를 수행하였습니다.</P>'
        )
    )
    return (
        '<DOCUMENT>'
        '<TITLE>독립된 감사인의 감사보고서</TITLE>'
        f'{opinion_section}'
        f'{separate_basis}'
        f'{extra_content}'
        '</DOCUMENT>'
    ).encode('utf-8')


def _complete_negative_audit_diagnostics():
    return {
        'version': AUDIT_ARCHIVE_DIAGNOSTICS_VERSION,
        'archive_sha256': 'a' * 64,
        'archive_size_bytes': 123,
        'member_count': 1,
        'members': [],
        'decoded_member_count': 1,
        'aud_opn_table_count': 1,
        'audit_report_label_row_count': 1,
        'known_opinion_token_row_count': 1,
        'positive_authority_evidence_count': 0,
        'negative_authority_evidence_count': 1,
        'ambiguous_authority_evidence_count': 0,
        'truncated': False,
    }


def _audit_event(
    *,
    event_id: int = 99,
    rcept_no: str = '20250325000541',
):
    return {
        'id': event_id,
        'rcept_no': rcept_no,
        'corp_code': '00960623',
        'stock_code': '266690',
        'corp_name': '덕산넥코어스',
        'receipt_date': date(2025, 3, 25),
        'report_nm': '사업보고서 (2024.12)',
    }


def _durable_unqualified_audit_signal():
    return {
        'opinion_code': 'unqualified',
        'opinion_label': '적정',
        'has_audit_issue': False,
        'source_section': 'audit.xml',
        'evidence_text': '감사의견 | 적정의견',
        'payload': {
            'parser': 'semantic_audit_table_v1',
            'opinion_cell': '적정의견',
            'row_cells': ['감사의견', '적정의견'],
        },
    }


def _share_issuance_attempt_row(
    *,
    corp_code: str,
    stock_code: str,
    receipt_date: date,
    evidence_semantics: str,
    authority_marker: str,
):
    completed = evidence_semantics == 'completed_scan_attempt'
    return {
        'event': {
            'id': None,
            'rcept_no': f'DILATT-{corp_code}',
            'corp_code': corp_code,
            'stock_code': stock_code,
            'corp_name': 'Authority CAS test',
            'receipt_date': receipt_date,
            'report_nm': 'DART share issuance coverage attempt',
        },
        'dilution_signal': {
            'signal_type': 'share_issuance',
            'has_dilution_signal': False,
            'security_type': None,
            'reason': f'{evidence_semantics} test row',
            'payload': {
                'evidence_semantics': evidence_semantics,
                'authority_marker': authority_marker,
                'evidence_coverage': {
                    'pagination_completed': completed,
                    'authoritative_as_of_cutoff': completed,
                },
            },
        },
    }


class FakeSignalsClient:
    def __init__(self, *, members):
        self.members = members
        self.download_args = None
        self.download_content = None

    async def download_document(self, *, rcept_no):
        self.download_args = {'rcept_no': rcept_no}
        content = _build_archive(self.members)
        self.download_content = content
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
    def test_authoritative_absence_builder_emits_strict_versioned_payload(self):
        event = {
            'id': 99,
            'rcept_no': '20260319000589',
            'receipt_date': date(2026, 3, 19),
        }
        diagnostics = _complete_negative_audit_diagnostics()

        signal = build_authoritative_absence_audit_signal(event, diagnostics)

        self.assertIsNone(signal.opinion_code)
        self.assertIsNone(signal.opinion_label)
        self.assertFalse(signal.has_audit_issue)
        self.assertIsNone(signal.source_section)
        self.assertIsNone(signal.evidence_text)
        self.assertEqual(
            signal.payload['schema_version'],
            'dart_audit_authoritative_absence_v1',
        )
        self.assertEqual(
            signal.payload['evidence_semantics'],
            'authoritative_absence',
        )
        self.assertTrue(signal.payload['audit_signal_evaluated'])
        self.assertEqual(signal.payload['receipt_date'], '2026-03-19')
        self.assertTrue(
            is_authoritative_absence_audit_payload(
                signal.payload,
                event_id=99,
                rcept_no='20260319000589',
                receipt_date=date(2026, 3, 19),
            )
        )
        self.assertFalse(
            is_authoritative_absence_audit_payload(
                signal.payload,
                event_id=100,
            )
        )

    def test_authoritative_absence_validator_rejects_mutated_proof(self):
        event = _audit_event()
        signal = build_authoritative_absence_audit_signal(
            event,
            _complete_negative_audit_diagnostics(),
        )

        mutations = {
            'extra_field': lambda payload: payload.update({'extra': True}),
            'wrong_schema': lambda payload: payload.update(
                {'schema_version': 'dart_audit_authoritative_absence_v2'}
            ),
            'wrong_semantics': lambda payload: payload.update(
                {'evidence_semantics': 'unknown'}
            ),
            'noncanonical_receipt_date': lambda payload: payload.update(
                {'receipt_date': '20250325'}
            ),
            'evaluated_integer': lambda payload: payload.update(
                {'audit_signal_evaluated': 1}
            ),
            'wrong_diagnostic_version': lambda payload: payload[
                'audit_diagnostics'
            ].update({'version': 'dart_audit_archive_diagnostics_v2'}),
            'bad_hash': lambda payload: payload['audit_diagnostics'].update(
                {'archive_sha256': 'not-a-sha256'}
            ),
            'zero_archive_size': lambda payload: payload[
                'audit_diagnostics'
            ].update({'archive_size_bytes': 0}),
            'incomplete_decode': lambda payload: payload[
                'audit_diagnostics'
            ].update({'decoded_member_count': 0}),
            'positive_authority': lambda payload: payload[
                'audit_diagnostics'
            ].update({'positive_authority_evidence_count': 1}),
            'missing_negative_authority': lambda payload: payload[
                'audit_diagnostics'
            ].update({'negative_authority_evidence_count': 0}),
            'ambiguous_authority': lambda payload: payload[
                'audit_diagnostics'
            ].update({'ambiguous_authority_evidence_count': 1}),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                payload = json.loads(json.dumps(signal.payload))
                mutate(payload)
                self.assertFalse(
                    is_authoritative_absence_audit_payload(
                        payload,
                        event_id=event['id'],
                        rcept_no=event['rcept_no'],
                        receipt_date=event['receipt_date'],
                    )
                )

        self.assertFalse(
            is_authoritative_absence_audit_payload(
                signal.payload,
                receipt_date=date(2025, 3, 26),
            )
        )

        self.assertFalse(
            is_authoritative_absence_audit_payload(
                {'parser': 'semantic_audit_table_v1'}
            )
        )
        ambiguous_diagnostics = _complete_negative_audit_diagnostics()
        ambiguous_diagnostics['ambiguous_authority_evidence_count'] = 1
        with self.assertRaisesRegex(ValueError, 'complete versioned diagnostics'):
            build_authoritative_absence_audit_signal(
                event,
                ambiguous_diagnostics,
            )

    def test_durable_audit_classifier_rejects_claim_and_envelope_drift(self):
        event = _audit_event()
        signal = build_authoritative_absence_audit_signal(
            event,
            _complete_negative_audit_diagnostics(),
        )
        identity = {
            'event_id': event['id'],
            'rcept_no': event['rcept_no'],
            'receipt_date': event['receipt_date'],
        }
        self.assertTrue(
            is_authoritative_absence_audit_signal(signal, **identity)
        )
        self.assertEqual(
            classify_durable_audit_signal(signal, **identity),
            'authoritative_absence',
        )

        malformed_claim = {
            **signal.__dict__,
            'opinion_code': 'unqualified',
            'has_audit_issue': False,
            'payload': json.loads(json.dumps(signal.payload)),
        }
        malformed_claim['payload']['evidence_semantics'] = 'tampered'
        self.assertTrue(
            claims_authoritative_absence_audit_payload(
                malformed_claim['payload']
            )
        )
        self.assertIsNone(
            classify_durable_audit_signal(malformed_claim, **identity)
        )

        doubly_corrupted_claim = json.loads(json.dumps(malformed_claim))
        doubly_corrupted_claim['payload']['schema_version'] = 'tampered'
        self.assertTrue(
            claims_authoritative_absence_audit_payload(
                doubly_corrupted_claim['payload']
            )
        )
        self.assertIsNone(
            classify_durable_audit_signal(
                doubly_corrupted_claim,
                **identity,
            )
        )

        envelope_mutations = {
            'opinion_code': 'qualified',
            'opinion_label': '한정',
            'has_audit_issue': True,
            'source_section': 'audit',
            'evidence_text': 'contradiction',
        }
        for field, value in envelope_mutations.items():
            with self.subTest(field=field):
                mutated = {**signal.__dict__, field: value}
                self.assertIsNone(
                    classify_durable_audit_signal(mutated, **identity)
                )

        self.assertEqual(
            classify_durable_audit_signal(
                {
                    'opinion_code': 'unqualified',
                    'opinion_label': '적정',
                    'has_audit_issue': False,
                    'source_section': 'audit.xml',
                    'evidence_text': '감사의견 | 적정의견',
                    'payload': {
                        'parser': 'semantic_audit_table_v1',
                        'opinion_cell': '적정의견',
                        'row_cells': ['감사의견', '적정의견'],
                    },
                }
            ),
            'parsed_opinion',
        )
        self.assertEqual(
            classify_durable_audit_signal(
                {
                    'opinion_code': 'unqualified',
                    'opinion_label': '적정',
                    'has_audit_issue': False,
                    'source_section': 'legacy.xml',
                    'evidence_text': '감사의견 | 적정의견',
                    'payload': {
                        'opinion_cell': '적정의견',
                        'row_cells': ['감사의견', '적정의견'],
                    },
                }
            ),
            'parsed_opinion',
        )
        malformed_parsed_signals = {
            'missing_label': {
                'opinion_label': None,
            },
            'mismatched_label': {
                'opinion_label': '한정',
            },
            'blank_source': {
                'source_section': ' ',
            },
            'blank_evidence': {
                'evidence_text': '',
            },
            'missing_payload': {
                'payload': None,
            },
            'unknown_parser': {
                'payload': {
                    'parser': 'unknown_v1',
                    'opinion_cell': '적정의견',
                    'row_cells': ['적정의견'],
                },
            },
            'non_string_parser': {
                'payload': {
                    'parser': {'unexpected': 'mapping'},
                    'opinion_cell': '적정의견',
                    'row_cells': ['적정의견'],
                },
            },
            'missing_parser_evidence': {
                'payload': {'parser': 'semantic_audit_table_v1'},
            },
            'contradictory_opinion_cell': {
                'payload': {
                    'parser': 'semantic_audit_table_v1',
                    'opinion_cell': '한정의견',
                    'row_cells': ['감사의견', '한정의견'],
                },
            },
            'unhashable_selection_opinion': {
                'payload': {
                    'parser': 'semantic_audit_table_v1',
                    'opinion_cell': '적정의견',
                    'row_cells': ['감사의견', '적정의견'],
                    'selection_policy': (
                        'most_conservative_audit_opinion_v1'
                    ),
                    'evidence_count': 1,
                    'opinion_codes': [{'unexpected': 'mapping'}],
                    'conflicting_opinion_codes': [],
                    'findings_by_opinion': {'unqualified': 1},
                    'source_sections': ['audit.xml'],
                    'selected_source_section': 'audit.xml',
                },
            },
        }
        valid_parsed_signal = {
            'opinion_code': 'unqualified',
            'opinion_label': '적정',
            'has_audit_issue': False,
            'source_section': 'audit.xml',
            'evidence_text': '감사의견 | 적정의견',
            'payload': {
                'parser': 'semantic_audit_table_v1',
                'opinion_cell': '적정의견',
                'row_cells': ['감사의견', '적정의견'],
            },
        }
        for name, mutation in malformed_parsed_signals.items():
            with self.subTest(name=name):
                self.assertIsNone(
                    classify_durable_audit_signal(
                        {**valid_parsed_signal, **mutation}
                    )
                )
        valid_with_blank_table_cell = {
            **valid_parsed_signal,
            'payload': {
                **valid_parsed_signal['payload'],
                'row_cells': ['감사의견', '', '적정의견'],
            },
        }
        self.assertEqual(
            classify_durable_audit_signal(valid_with_blank_table_cell),
            'parsed_opinion',
        )
        valid_with_selection_metadata = {
            'opinion_code': 'qualified',
            'opinion_label': '한정',
            'has_audit_issue': True,
            'source_section': 'audit-a.xml',
            'evidence_text': '감사의견 | 한정의견',
            'payload': {
                'parser': 'semantic_audit_table_v1',
                'opinion_cell': '한정의견',
                'row_cells': ['감사의견', '한정의견'],
                'selection_policy': (
                    'most_conservative_audit_opinion_v1'
                ),
                'evidence_count': 2,
                'opinion_codes': ['qualified', 'unqualified'],
                'conflicting_opinion_codes': [
                    'qualified',
                    'unqualified',
                ],
                'findings_by_opinion': {
                    'qualified': 1,
                    'unqualified': 1,
                },
                'source_sections': [
                    'audit-a.xml',
                    'audit-b.xml',
                ],
                'selected_source_section': 'audit-a.xml',
            },
        }
        self.assertEqual(
            classify_durable_audit_signal(
                valid_with_selection_metadata
            ),
            'parsed_opinion',
        )
        invalid_selection_field_values = {
            'selection_policy': 'unknown_policy_v1',
            'evidence_count': 3,
            'opinion_codes': ['qualified', 'unknown'],
            'conflicting_opinion_codes': [],
            'findings_by_opinion': {
                'qualified': 2,
                'unqualified': 1,
            },
            'source_sections': ['audit-b.xml', 'audit-a.xml'],
            'selected_source_section': 'audit-b.xml',
        }
        for field, invalid_value in invalid_selection_field_values.items():
            with self.subTest(selection_field=field):
                invalid_signal = json.loads(
                    json.dumps(valid_with_selection_metadata)
                )
                invalid_signal['payload'][field] = invalid_value
                self.assertIsNone(
                    classify_durable_audit_signal(invalid_signal)
                )

        nonconservative_selection = json.loads(
            json.dumps(valid_with_selection_metadata)
        )
        nonconservative_selection.update(
            {
                'opinion_code': 'unqualified',
                'opinion_label': '적정',
                'has_audit_issue': False,
                'evidence_text': '감사의견 | 적정의견',
            }
        )
        nonconservative_selection['payload'].update(
            {
                'opinion_cell': '적정의견',
                'row_cells': ['감사의견', '적정의견'],
            }
        )
        self.assertIsNone(
            classify_durable_audit_signal(nonconservative_selection)
        )
        for opinion_code, has_audit_issue in (
            ('unqualified', True),
            ('qualified', False),
            ('adverse', False),
            ('disclaimer', False),
        ):
            with self.subTest(opinion_code=opinion_code):
                self.assertIsNone(
                    classify_durable_audit_signal(
                        {
                            'opinion_code': opinion_code,
                            'has_audit_issue': has_audit_issue,
                            'payload': None,
                        }
                    )
                )

    def test_global_evidence_isolates_one_corporation_failure(self):
        repository = MagicMock()
        repository.list_current_share_issuance_coverage.return_value = []
        repository.list_latest_share_issuance_positive_event_dates.return_value = {}
        service = DartSignalsService(repository=repository)

        def persist_one(*, corporation, **_kwargs):
            if corporation['corp_code'] == 'CORP-B':
                raise ValueError('malformed corporation evidence')
            return {
                'positive_event_count': 0,
                'negative_coverage_count': 1,
                'negative_segment_count': 0,
                'coverage_status': 'complete_negative',
                '_evidence_rows': [
                    {
                        'event': {'corp_code': 'CORP-A'},
                        'dilution_signal': {},
                    }
                ],
            }

        with (
            patch.object(
                service,
                'persist_share_issuance_scan_evidence',
                side_effect=persist_one,
            ),
            patch.object(service, '_store_dilution_signal_rows') as store,
            patch.object(
                service,
                '_persist_incomplete_share_issuance_attempt',
                return_value=1,
            ) as mark_incomplete,
        ):
            result = service.persist_global_share_issuance_scan_evidence(
                corporations=[
                    {
                        'corp_code': 'CORP-A',
                        'stock_code': '005930',
                    },
                    {
                        'corp_code': 'CORP-B',
                        'stock_code': '000660',
                    },
                ],
                raw_items=[],
                window_start=date(2026, 1, 1),
                window_end=date(2026, 4, 1),
                sync_run_id=7,
            )

        store.assert_called_once()
        mark_incomplete.assert_called_once()
        self.assertEqual(result['coverage_ready_corp_codes'], ['CORP-A'])
        self.assertEqual(result['failed_corp_codes'], ['CORP-B'])
        self.assertEqual(
            result['failed_corp_reasons']['CORP-B'],
            'malformed corporation evidence',
        )

    def test_global_evidence_batches_full_universe_writes(self):
        repository = MagicMock()
        repository.list_current_share_issuance_coverage.return_value = []
        repository.list_latest_share_issuance_positive_event_dates.return_value = {}
        service = DartSignalsService(repository=repository)
        corporations = [
            {
                'corp_code': f'CORP-{index:04d}',
                'stock_code': f'{index:06d}',
            }
            for index in range(501)
        ]

        def persist_one(*, corporation, **_kwargs):
            return {
                'positive_event_count': 0,
                'negative_coverage_count': 1,
                'negative_segment_count': 0,
                'coverage_status': 'complete_negative',
                '_evidence_rows': [
                    {
                        'event': {
                            'corp_code': corporation['corp_code'],
                        },
                        'dilution_signal': {},
                    }
                ],
            }

        with (
            patch.object(
                service,
                'persist_share_issuance_scan_evidence',
                side_effect=persist_one,
            ),
            patch.object(service, '_store_dilution_signal_rows') as store,
        ):
            result = service.persist_global_share_issuance_scan_evidence(
                corporations=corporations,
                raw_items=[],
                window_start=date(2026, 1, 1),
                window_end=date(2026, 4, 1),
                sync_run_id=8,
            )

        self.assertEqual(store.call_count, 3)
        self.assertEqual(
            [len(item.args[0]) for item in store.call_args_list],
            [250, 250, 1],
        )
        self.assertEqual(result['corporation_count'], 501)
        self.assertEqual(len(result['coverage_ready_corp_codes']), 501)
        self.assertEqual(result['failed_corp_codes'], [])

    def test_global_evidence_bisects_failed_batch_to_one_corporation(self):
        repository = MagicMock()
        repository.list_current_share_issuance_coverage.return_value = []
        repository.list_latest_share_issuance_positive_event_dates.return_value = {}
        service = DartSignalsService(repository=repository)
        corporations = [
            {'corp_code': 'CORP-A', 'stock_code': '005930'},
            {'corp_code': 'CORP-B', 'stock_code': '000660'},
            {'corp_code': 'CORP-C', 'stock_code': '035420'},
        ]

        def persist_one(*, corporation, **_kwargs):
            return {
                'positive_event_count': 0,
                'negative_coverage_count': 1,
                'negative_segment_count': 0,
                'coverage_status': 'complete_negative',
                '_evidence_rows': [
                    {
                        'event': {'corp_code': corporation['corp_code']},
                        'dilution_signal': {},
                    }
                ],
            }

        def store_rows(rows):
            if any(
                row['event']['corp_code'] == 'CORP-B'
                for row in rows
            ):
                class UniqueViolation(ValueError):
                    pgcode = '23505'

                raise IntegrityError(
                    'insert dilution evidence',
                    {},
                    UniqueViolation(
                        'one corporation violates storage contract'
                    ),
                )

        with (
            patch.object(
                service,
                'persist_share_issuance_scan_evidence',
                side_effect=persist_one,
            ),
            patch.object(
                service,
                '_store_dilution_signal_rows',
                side_effect=store_rows,
            ),
            patch.object(
                service,
                '_persist_incomplete_share_issuance_attempt',
                return_value=1,
            ) as mark_incomplete,
        ):
            result = service.persist_global_share_issuance_scan_evidence(
                corporations=corporations,
                raw_items=[],
                window_start=date(2026, 1, 1),
                window_end=date(2026, 4, 1),
                sync_run_id=9,
            )

        self.assertEqual(
            result['coverage_ready_corp_codes'],
            ['CORP-A', 'CORP-C'],
        )
        self.assertEqual(result['failed_corp_codes'], ['CORP-B'])
        self.assertIn(
            'one corporation violates storage contract',
            result['failed_corp_reasons']['CORP-B'],
        )
        self.assertEqual(result['rows_upserted'], 2)
        mark_incomplete.assert_called_once()
        self.assertEqual(
            mark_incomplete.call_args.kwargs['corporation']['corp_code'],
            'CORP-B',
        )

    def test_global_evidence_does_not_bisect_systemic_database_failure(self):
        repository = MagicMock()
        repository.list_current_share_issuance_coverage.return_value = []
        repository.list_latest_share_issuance_positive_event_dates.return_value = {}
        service = DartSignalsService(repository=repository)
        corporations = [
            {'corp_code': 'CORP-A', 'stock_code': '005930'},
            {'corp_code': 'CORP-B', 'stock_code': '000660'},
            {'corp_code': 'CORP-C', 'stock_code': '035420'},
        ]

        def persist_one(*, corporation, **_kwargs):
            return {
                'positive_event_count': 0,
                'negative_coverage_count': 1,
                'negative_segment_count': 0,
                'coverage_status': 'complete_negative',
                '_evidence_rows': [
                    {
                        'event': {'corp_code': corporation['corp_code']},
                        'dilution_signal': {},
                    }
                ],
            }

        outage = OperationalError(
            'insert dilution evidence',
            {},
            ConnectionError('database connection reset'),
            connection_invalidated=True,
        )
        with (
            patch.object(
                service,
                'persist_share_issuance_scan_evidence',
                side_effect=persist_one,
            ),
            patch.object(
                service,
                '_store_dilution_signal_rows',
                side_effect=outage,
            ) as store,
            patch.object(
                service,
                '_persist_incomplete_share_issuance_attempt',
            ) as mark_incomplete,
        ):
            with self.assertRaises(OperationalError):
                service.persist_global_share_issuance_scan_evidence(
                    corporations=corporations,
                    raw_items=[],
                    window_start=date(2026, 1, 1),
                    window_end=date(2026, 4, 1),
                    sync_run_id=10,
                )

        store.assert_called_once()
        mark_incomplete.assert_not_called()

    def test_dilution_signal_records_only_positive_event_evidence(self):
        service = DartSignalsService(client=FakeSignalsClient(members=[]))

        matched = service._extract_dilution_signal(
            {'report_nm': '유상증자 결정'}
        )
        unmatched = service._extract_dilution_signal(
            {'report_nm': '정기주주총회 결과'}
        )

        self.assertIsNotNone(matched)
        self.assertTrue(matched['has_dilution_signal'])
        self.assertEqual(
            matched['payload']['evidence_semantics'],
            'positive_event_match',
        )
        self.assertIsNone(unmatched)

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

    def test_candidate_query_filters_only_strict_durable_audit_rows(self):
        def row(
            *,
            event_id: int,
            opinion_code: str,
            has_audit_issue: bool,
            payload: dict,
            opinion_label: str | None = None,
            source_section: str | None = None,
            evidence_text: str | None = None,
        ) -> dict:
            return {
                'event_id': event_id,
                'snapshot_id': event_id + 100,
                'rcept_no': f'202603010000{event_id}',
                'stock_code': f'{event_id:06d}',
                'business_year': 2025,
                'fs_div': 'CFS',
                'receipt_date': date(2026, 3, 1),
                'report_nm': '사업보고서',
                'audit_signal_id': event_id + 200,
                'audit_opinion_code': opinion_code,
                'audit_opinion_label': opinion_label,
                'audit_has_audit_issue': has_audit_issue,
                'audit_source_section': source_section,
                'audit_evidence_text': evidence_text,
                'audit_payload': payload,
            }

        pages = [
            [
                row(
                    event_id=11,
                    opinion_code='unqualified',
                    has_audit_issue=False,
                    opinion_label='적정',
                    source_section='audit.xml',
                    evidence_text='감사의견 | 적정의견',
                    payload={
                        'parser': 'semantic_audit_table_v1',
                        'opinion_cell': '적정의견',
                        'row_cells': ['감사의견', '적정의견'],
                    },
                ),
                row(
                    event_id=12,
                    opinion_code='qualified',
                    has_audit_issue=False,
                    payload={'parser': 'semantic_audit_table_v1'},
                ),
            ],
            [
                row(
                    event_id=13,
                    opinion_code='unqualified',
                    has_audit_issue=False,
                    payload={
                        'schema_version': (
                            'dart_audit_authoritative_absence_v1'
                        ),
                        'evidence_semantics': 'tampered',
                    },
                )
            ],
        ]

        class PageResult:
            def __init__(self, items):
                self.items = items

            def mappings(self):
                return self

            def all(self):
                return self.items

        class PagingSession:
            def __init__(self):
                self.calls = 0

            def execute(self, _statement):
                items = pages[self.calls]
                self.calls += 1
                return PageResult(items)

        session = PagingSession()

        @contextmanager
        def fake_session_scope():
            yield session

        repository = DartSignalsRepository()
        with patch('app.dart.signals_service.session_scope', fake_session_scope):
            result = repository.list_missing_annual_audit_signal_candidates(
                market_date=date(2026, 4, 30),
                limit=2,
                after_event_id=10,
            )

        self.assertEqual(
            [item['event_id'] for item in result],
            [12, 13],
        )
        self.assertEqual(session.calls, 2)

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

    async def test_extract_audit_signal_rejects_conflicting_markerless_members(self):
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
            self.assertIsNone(result)

    async def test_extract_audit_signal_rejects_conflicting_markerless_rows(self):
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

        self.assertIsNone(result)

    async def test_extract_audit_signal_legacy_uses_only_explicit_current_row(self):
        document = (
            '<TABLE-GROUP ACLASS="AUD_OPN">'
            '<TR><TD>감사보고서(당기)</TD><TD>적정의견</TD></TR>'
            '<TR><TD>감사보고서(전기)</TD><TD>의견거절</TD></TR>'
            '</TABLE-GROUP>'
        ).encode('utf-8')

        result = await DartSignalsService(
            client=FakeSignalsClient(members=[('reports/legacy.xml', document)])
        )._extract_audit_signal(
            {'rcept_no': '20260319000007', 'report_nm': '사업보고서'}
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.opinion_code, 'unqualified')
        self.assertFalse(result.has_audit_issue)
        self.assertIn('감사보고서(당기)', result.evidence_text)
        self.assertNotIn('감사보고서(전기)', result.evidence_text)

    async def test_extract_audit_signal_legacy_prior_only_fails_closed(self):
        document = (
            '<TABLE-GROUP ACLASS="AUD_OPN">'
            '<TR><TD>감사보고서(전기)</TD><TD>의견거절</TD></TR>'
            '</TABLE-GROUP>'
        ).encode('utf-8')

        result = await DartSignalsService(
            client=FakeSignalsClient(members=[('reports/prior.xml', document)])
        )._extract_audit_signal(
            {'rcept_no': '20260319000008', 'report_nm': '사업보고서'}
        )

        self.assertIsNone(result)

    async def test_extract_audit_signal_semantic_current_outranks_legacy_prior(self):
        semantic_current = _semantic_audit_document(
            ('제10기(당기)', '적정의견')
        )
        legacy_prior = (
            '<TABLE-GROUP ACLASS="AUD_OPN">'
            '<TR><TD>감사보고서(전기)</TD><TD>의견거절</TD></TR>'
            '</TABLE-GROUP>'
        ).encode('utf-8')

        result = await DartSignalsService(
            client=FakeSignalsClient(
                members=[
                    ('reports/semantic.xml', semantic_current),
                    ('reports/legacy-prior.xml', legacy_prior),
                ]
            )
        )._extract_audit_signal(
            {'rcept_no': '20260319000009', 'report_nm': '사업보고서'}
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.opinion_code, 'unqualified')
        self.assertEqual(result.source_section, 'reports/semantic.xml')
        self.assertFalse(result.has_audit_issue)

    async def test_extract_audit_signal_accepts_semantic_plain_table_current_row_only(self):
        client = FakeSignalsClient(
            members=[
                (
                    'reports/annual.xml',
                    _semantic_audit_document(
                        ('제70기(당기)', '적정의견'),
                        ('제69기(전기)', '의견거절'),
                    ),
                )
            ]
        )

        result = await DartSignalsService(client=client)._extract_audit_signal(
            {'rcept_no': '20260325001443', 'report_nm': '사업보고서'}
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.opinion_code, 'unqualified')
        self.assertFalse(result.has_audit_issue)
        self.assertEqual(result.payload['evidence_count'], 1)
        self.assertIn('제70기(당기)', result.evidence_text)
        self.assertNotIn('제69기(전기)', result.evidence_text)

    async def test_extract_audit_signal_accepts_semantic_aud_opn_without_report_label(self):
        client = FakeSignalsClient(
            members=[
                (
                    'reports/aud-opn.xml',
                    _semantic_audit_document(
                        ('제52기(당기)', '적정'),
                        table_class='AUD_OPN',
                        use_table_group=True,
                    ),
                )
            ]
        )

        result = await DartSignalsService(client=client)._extract_audit_signal(
            {'rcept_no': '20260319000588', 'report_nm': '사업보고서'}
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.opinion_code, 'unqualified')
        self.assertFalse(result.has_audit_issue)

    async def test_extract_audit_signal_accepts_unmodified_auditor_report_narrative(self):
        cases = (
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        )
        for same_block, basis_inline in cases:
            with self.subTest(
                same_block=same_block,
                basis_inline=basis_inline,
            ):
                result = await DartSignalsService(
                    client=FakeSignalsClient(
                        members=[
                            (
                                'reports/annual-audit.xml',
                                _unmodified_auditor_report_document(
                                    same_block=same_block,
                                    basis_inline=basis_inline,
                                ),
                            )
                        ]
                    )
                )._extract_audit_signal(
                    {
                        'rcept_no': '20260327000199',
                        'report_nm': '[첨부정정]사업보고서 (2025.12)',
                    }
                )

                self.assertIsNotNone(result)
                self.assertEqual(result.opinion_code, 'unqualified')
                self.assertEqual(result.opinion_label, '적정')
                self.assertFalse(result.has_audit_issue)
                self.assertEqual(
                    result.payload['parser'],
                    'auditor_report_unmodified_opinion_v1',
                )
                self.assertEqual(
                    classify_durable_audit_signal(result),
                    'parsed_opinion',
                )
                tampered_body = result.payload['opinion_body'].replace(
                    '공정하게 표시하고 있습니다',
                    '의견을 확인할 수 없습니다',
                )
                tampered_payload = dict(result.payload)
                tampered_payload['opinion_body'] = tampered_body
                tampered_payload['evidence_blocks'] = [
                    '독립된 감사인의 감사보고서',
                    '감사의견',
                    tampered_body,
                ]
                self.assertIsNone(
                    classify_durable_audit_signal(
                        {
                            'opinion_code': result.opinion_code,
                            'opinion_label': result.opinion_label,
                            'has_audit_issue': result.has_audit_issue,
                            'source_section': result.source_section,
                            'evidence_text': result.evidence_text,
                            'payload': tampered_payload,
                        }
                    )
                )

    async def test_auditor_report_narrative_outranks_ambiguous_secondary_table(self):
        ambiguous_secondary_table = _semantic_audit_document(
            ('제30기(당기)', '-')
        ).decode('utf-8')
        result = await DartSignalsService(
            client=FakeSignalsClient(
                members=[
                    (
                        'reports/annual-audit.xml',
                        _unmodified_auditor_report_document(),
                    ),
                    (
                        'reports/business-report.xml',
                        (
                            _semantic_audit_document(
                                ('제30기(당기)', '적정의견')
                            ).decode('utf-8')
                            + ambiguous_secondary_table
                        ).encode('utf-8'),
                    ),
                ]
            )
        )._extract_audit_signal(
            {'rcept_no': '20260319000268', 'report_nm': '사업보고서'}
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.opinion_code, 'unqualified')
        self.assertEqual(
            result.payload['parser'],
            'auditor_report_unmodified_opinion_v1',
        )

    async def test_modified_semantic_opinion_outranks_unmodified_narrative(self):
        cases = (
            ('한정의견', 'qualified'),
            ('부적정의견', 'adverse'),
            ('의견거절', 'disclaimer'),
        )
        for semantic_opinion, expected_code in cases:
            with self.subTest(semantic_opinion=semantic_opinion):
                result = await DartSignalsService(
                    client=FakeSignalsClient(
                        members=[
                            (
                                'reports/annual-audit.xml',
                                _unmodified_auditor_report_document(),
                            ),
                            (
                                'reports/business-report.xml',
                                _semantic_audit_document(
                                    ('제30기(당기)', semantic_opinion)
                                ),
                            ),
                        ]
                    )
                )._extract_audit_signal(
                    {
                        'rcept_no': '20260319000273',
                        'report_nm': '사업보고서',
                    }
                )

                self.assertIsNotNone(result)
                self.assertEqual(result.opinion_code, expected_code)
                self.assertTrue(result.has_audit_issue)
                self.assertEqual(
                    result.payload['parser'],
                    'semantic_audit_table_v1',
                )

    async def test_scoped_audit_capture_revalidates_before_persistence(self):
        repository = MagicMock()
        repository.audit_capture_scope_matches.return_value = False
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[
                    (
                        'reports/annual-audit.xml',
                        _unmodified_auditor_report_document(),
                    )
                ]
            ),
            repository=repository,
        )
        event = {
            'id': 101,
            'rcept_no': '20260319000274',
            'corp_code': '00126380',
            'stock_code': '005930',
            'corp_name': '삼성전자',
            'receipt_date': date(2026, 3, 19),
            'report_nm': '사업보고서 (2025.12)',
            'filing_category': 'annual_report',
            'payload': {},
        }

        with patch.object(service, '_load_event', return_value=event):
            with self.assertRaisesRegex(
                ValueError,
                'selected annual audit candidate changed',
            ):
                await service.capture_audit_signal_for_event(
                    rcept_no=event['rcept_no'],
                    selected_candidate_identity={
                        'event_id': 101,
                        'rcept_no': event['rcept_no'],
                        'stock_code': event['stock_code'],
                    },
                    selected_candidate_market_date=date(2026, 10, 30),
                )

        repository.audit_capture_scope_matches.assert_called_once()

    async def test_reviewed_replacement_projects_parsed_opinion_to_target(self):
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[('reports/audit.xml', _audit_document('적정의견'))]
            ),
            repository=MagicMock(),
        )
        source_event = {
            'id': 102,
            'rcept_no': '20260318000002',
            'corp_code': '00126380',
            'stock_code': '005930',
            'corp_name': '삼성전자',
            'receipt_date': date(2026, 3, 18),
            'report_nm': '감사보고서 replacement',
            'filing_category': 'audit_report',
            'payload': {},
        }
        target_event = {
            **source_event,
            'id': 101,
            'rcept_no': '20260319000001',
            'receipt_date': date(2026, 3, 19),
            'report_nm': '사업보고서 (2025.12)',
            'filing_category': 'annual_report',
        }
        events = {
            source_event['rcept_no']: source_event,
            target_event['rcept_no']: target_event,
        }
        replacement_target = {
            'event_id': target_event['id'],
            'rcept_no': target_event['rcept_no'],
            'stock_code': target_event['stock_code'],
            'source_error_code': '014',
            'reason': 'reviewed source document is unavailable',
        }

        with (
            patch.object(
                service,
                '_load_event',
                side_effect=lambda rcept_no: events[rcept_no],
            ),
            patch.object(service, '_store_signals') as store_signals,
        ):
            result = await service.capture_audit_signal_for_event(
                rcept_no=source_event['rcept_no'],
                selected_candidate_identity={
                    key: replacement_target[key]
                    for key in ('event_id', 'rcept_no', 'stock_code')
                },
                selected_candidate_market_date=date(2026, 10, 30),
                replacement_target_identity=replacement_target,
                replacement_manifest_sha256='a' * 64,
            )

        stored = store_signals.call_args.kwargs
        self.assertEqual(stored['event'], target_event)
        self.assertEqual(stored['expected_event'], source_event)
        self.assertEqual(stored['expected_persistence_event'], target_event)
        projected = stored['audit_signal']
        self.assertEqual(
            classify_durable_audit_signal(
                projected,
                event_id=target_event['id'],
                rcept_no=target_event['rcept_no'],
                receipt_date=target_event['receipt_date'],
            ),
            'parsed_opinion',
        )
        lineage = projected.payload['replacement_lineage']
        self.assertEqual(lineage['source']['event_id'], source_event['id'])
        self.assertEqual(lineage['target']['event_id'], target_event['id'])
        self.assertEqual(lineage['manifest_sha256'], 'a' * 64)
        self.assertEqual(result['persisted_rcept_no'], target_event['rcept_no'])

        tampered = dict(projected.__dict__)
        tampered['payload'] = dict(projected.payload)
        tampered['payload']['replacement_lineage'] = dict(lineage)
        tampered['payload']['replacement_lineage']['reason'] = 'tampered'
        self.assertIsNone(
            classify_durable_audit_signal(
                tampered,
                event_id=target_event['id'],
                rcept_no=target_event['rcept_no'],
                receipt_date=target_event['receipt_date'],
            )
        )

        missing_replacement = dict(projected.__dict__)
        missing_replacement['payload'] = dict(projected.payload)
        missing_replacement['payload'].pop('replacement_lineage')
        self.assertIsNone(
            classify_durable_audit_signal(
                missing_replacement,
                event_id=target_event['id'],
                rcept_no=target_event['rcept_no'],
                receipt_date=target_event['receipt_date'],
            )
        )

    async def test_replacement_never_projects_authoritative_absence(self):
        service = DartSignalsService(client=MagicMock(), repository=MagicMock())
        source_event = _audit_event(event_id=102, rcept_no='20260318000002')
        target_event = {
            **source_event,
            'id': 101,
            'rcept_no': '20260319000001',
            'receipt_date': date(2026, 3, 19),
        }
        source_event['receipt_date'] = date(2026, 3, 18)
        events = {
            source_event['rcept_no']: source_event,
            target_event['rcept_no']: target_event,
        }
        with (
            patch.object(
                service,
                '_load_event',
                side_effect=lambda rcept_no: events[rcept_no],
            ),
            patch.object(
                service,
                '_extract_audit_signal_with_diagnostics',
                new=AsyncMock(
                    return_value=(None, _complete_negative_audit_diagnostics())
                ),
            ),
            patch.object(service, '_store_signals') as store_signals,
        ):
            with self.assertRaisesRegex(
                ValueError,
                'absence evidence cannot be projected',
            ):
                await service.capture_audit_signal_for_event(
                    rcept_no=source_event['rcept_no'],
                    selected_candidate_identity={
                        'event_id': target_event['id'],
                        'rcept_no': target_event['rcept_no'],
                        'stock_code': target_event['stock_code'],
                    },
                    selected_candidate_market_date=date(2026, 10, 30),
                    replacement_target_identity={
                        'event_id': target_event['id'],
                        'rcept_no': target_event['rcept_no'],
                        'stock_code': target_event['stock_code'],
                        'source_error_code': '014',
                        'reason': 'reviewed source document is unavailable',
                    },
                    replacement_manifest_sha256='b' * 64,
                )

        store_signals.assert_not_called()

    async def test_exact_direct_authoritative_absence_carries_repair_lineage(self):
        service = DartSignalsService(client=MagicMock(), repository=MagicMock())
        event = _audit_event()
        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(
                service,
                '_extract_audit_signal_with_diagnostics',
                new=AsyncMock(
                    return_value=(None, _complete_negative_audit_diagnostics())
                ),
            ),
            patch.object(service, '_store_signals') as store_signals,
        ):
            result = await service.capture_audit_signal_for_event(
                rcept_no=event['rcept_no'],
                selected_candidate_identity={
                    'event_id': event['id'],
                    'rcept_no': event['rcept_no'],
                    'stock_code': event['stock_code'],
                },
                selected_candidate_market_date=date(2026, 10, 30),
                repair_manifest_sha256='c' * 64,
            )

        signal = store_signals.call_args.kwargs['audit_signal']
        lineage = signal.payload['exact_repair_lineage']
        self.assertEqual(lineage['manifest_sha256'], 'c' * 64)
        self.assertEqual(lineage['mode'], 'direct')
        self.assertEqual(
            classify_durable_audit_signal(
                signal,
                event_id=event['id'],
                rcept_no=event['rcept_no'],
                receipt_date=event['receipt_date'],
            ),
            'authoritative_absence',
        )
        self.assertTrue(result['audit_signal_evaluated'])

    async def test_auditor_report_narrative_requires_direct_report_authority(self):
        internal_control_only = (
            '<DOCUMENT>'
            '<TITLE>독립된 감사인의 내부회계관리제도 감사보고서</TITLE>'
            '<P USERMARK="B">감사의견</P>'
            '<P>우리는 회사의 재무제표를 감사하였습니다. 우리의 의견으로는 '
            '회사의 재무상태와 재무성과 및 현금흐름을 한국채택국제회계기준에 '
            '따라 중요성의 관점에서 공정하게 표시하고 있습니다.</P>'
            '<P USERMARK="B">감사의견근거</P>'
            '</DOCUMENT>'
        ).encode('utf-8')

        result = await DartSignalsService(
            client=FakeSignalsClient(
                members=[('reports/internal-control.xml', internal_control_only)]
            )
        )._extract_audit_signal(
            {'rcept_no': '20260319000269', 'report_nm': '사업보고서'}
        )

        self.assertIsNone(result)

    async def test_auditor_report_narrative_requires_nearby_direct_report_title(self):
        distant_preamble = ''.join(
            f'<P>unrelated preamble {index}</P>' for index in range(20)
        )
        distant_opinion = _unmodified_auditor_report_document().decode(
            'utf-8'
        ).replace(
            '<TITLE>독립된 감사인의 감사보고서</TITLE>',
            f'<TITLE>독립된 감사인의 감사보고서</TITLE>{distant_preamble}',
        )

        result = await DartSignalsService(
            client=FakeSignalsClient(
                members=[
                    ('reports/distant-opinion.xml', distant_opinion.encode())
                ]
            )
        )._extract_audit_signal(
            {'rcept_no': '20260319000271', 'report_nm': '사업보고서'}
        )

        self.assertIsNone(result)

    async def test_auditor_report_narrative_fails_closed_on_unrecognized_opinion_body(self):
        malformed_primary = (
            '<DOCUMENT>'
            '<TITLE>독립된 감사인의 감사보고서</TITLE>'
            '<P USERMARK="B">감사의견</P>'
            '<P>감사 절차를 수행했으나 이 문단은 의견을 명시하지 않습니다.</P>'
            '<P USERMARK="B">감사의견근거</P>'
            '</DOCUMENT>'
        ).encode('utf-8')

        result = await DartSignalsService(
            client=FakeSignalsClient(
                members=[
                    ('reports/primary.xml', malformed_primary),
                    (
                        'reports/table.xml',
                        _semantic_audit_document(
                            ('제30기(당기)', '적정의견')
                        ),
                    ),
                ]
            )
        )._extract_audit_signal(
            {'rcept_no': '20260319000270', 'report_nm': '사업보고서'}
        )

        self.assertIsNone(result)

    async def test_auditor_report_narrative_does_not_promote_qualified_opinion(self):
        qualified_primary = _unmodified_auditor_report_document().decode(
            'utf-8'
        ).replace(
            '우리의 의견으로는 별첨된 회사의 재무제표는',
            '우리의 의견으로는 한정의견 근거 단락에 기술된 사항이 '
            '미치는 영향을 제외하고는 별첨된 회사의 재무제표는',
        )

        result = await DartSignalsService(
            client=FakeSignalsClient(
                members=[
                    ('reports/qualified-audit.xml', qualified_primary.encode())
                ]
            )
        )._extract_audit_signal(
            {'rcept_no': '20260319000272', 'report_nm': '사업보고서'}
        )

        self.assertIsNone(result)

    async def test_capture_audit_signal_uses_current_column_from_transposed_semantic_table(self):
        document = (
            '<TABLE ACLASS="NORMAL">'
            '<TR><TH>사업연도</TH><TD>제10기(당기)</TD><TD>제9기(전기)</TD></TR>'
            '<TR><TH>감사인</TH><TD>한영회계법인</TD><TD>삼정회계법인</TD></TR>'
            '<TR><TH>감사의견</TH><TD>적정</TD><TD>의견거절</TD></TR>'
            '<TR><TH>강조사항 등</TH><TD>-</TD><TD>-</TD></TR>'
            '<TR><TH>핵심감사사항</TH><TD>-</TD><TD>-</TD></TR>'
            '</TABLE>'
        ).encode('utf-8')
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[('reports/transposed-audit.xml', document)]
            ),
            repository=MagicMock(),
        )
        event = {
            'id': 99,
            'rcept_no': '20260319000589',
            'corp_code': '00960623',
            'stock_code': '266690',
            'corp_name': '덕산넥코어스',
            'receipt_date': date(2026, 3, 19),
            'report_nm': '사업보고서 (2025.12)',
        }

        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(service, '_store_signals') as store_signals,
        ):
            result = await service.capture_audit_signal_for_event(
                rcept_no=event['rcept_no']
            )

        audit_signal = store_signals.call_args.kwargs['audit_signal']
        self.assertIsNotNone(audit_signal)
        self.assertEqual(audit_signal.opinion_code, 'unqualified')
        self.assertFalse(audit_signal.has_audit_issue)
        self.assertIn('제10기(당기)', audit_signal.evidence_text)
        self.assertNotIn('의견거절', audit_signal.evidence_text)
        self.assertTrue(
            store_signals.call_args.kwargs['audit_signal_evaluated']
        )
        self.assertEqual(result['audit_signal']['opinion_code'], 'unqualified')

    async def test_extract_audit_signal_maps_bare_disclaimer_only_in_current_opinion_cell(self):
        current_disclaimer = await DartSignalsService(
            client=FakeSignalsClient(
                members=[
                    (
                        'reports/current.xml',
                        _semantic_audit_document(('제10기(당기)', '거절')),
                    )
                ]
            )
        )._extract_audit_signal(
            {'rcept_no': '20260319000001', 'report_nm': '사업보고서'}
        )
        unrelated_narrative = await DartSignalsService(
            client=FakeSignalsClient(
                members=[
                    (
                        'reports/narrative.xml',
                        b'<DOCUMENT>\xea\xb1\xb0\xec\xa0\x88</DOCUMENT>',
                    )
                ]
            )
        )._extract_audit_signal(
            {'rcept_no': '20260319000002', 'report_nm': '사업보고서'}
        )

        self.assertIsNotNone(current_disclaimer)
        self.assertEqual(current_disclaimer.opinion_code, 'disclaimer')
        self.assertTrue(current_disclaimer.has_audit_issue)
        self.assertIsNone(unrelated_narrative)

    async def test_extract_audit_signal_does_not_map_other_current_row_columns(self):
        document = (
            '<TABLE>'
            '<TR><TH>사업연도</TH><TH>감사인</TH><TH>감사의견</TH>'
            '<TH>강조사항 등</TH><TH>핵심감사사항</TH></TR>'
            '<TR><TD>제10기(당기)</TD><TD>한영회계법인</TD><TD>적정</TD>'
            '<TD>의견거절 가능성을 검토했음</TD><TD>-</TD></TR>'
            '</TABLE>'
        ).encode('utf-8')

        result = await DartSignalsService(
            client=FakeSignalsClient(members=[('reports/current.xml', document)])
        )._extract_audit_signal(
            {'rcept_no': '20260319000005', 'report_nm': '사업보고서'}
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.opinion_code, 'unqualified')
        self.assertFalse(result.has_audit_issue)

    async def test_extract_audit_signal_selects_worst_conflicting_current_semantic_rows(self):
        client = FakeSignalsClient(
            members=[
                (
                    'reports/conflict.xml',
                    _semantic_audit_document(
                        ('제10기(당기)', '적정'),
                        ('제10기(당기)', '한정의견'),
                        ('제9기(전기)', '부적정의견'),
                    ),
                )
            ]
        )

        result = await DartSignalsService(client=client)._extract_audit_signal(
            {'rcept_no': '20260319000003', 'report_nm': '사업보고서'}
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.opinion_code, 'qualified')
        self.assertEqual(result.payload['evidence_count'], 2)
        self.assertEqual(
            result.payload['conflicting_opinion_codes'],
            ['qualified', 'unqualified'],
        )

    async def test_extract_audit_signal_rejects_semantic_lookalikes_without_exact_headers(self):
        client = FakeSignalsClient(
            members=[
                (
                    'reports/lookalike.xml',
                    (
                        '<TABLE ACLASS="NORMAL">'
                        '<TR><TH>사업연도</TH><TH>감사의견</TH></TR>'
                        '<TR><TD>제10기(당기)</TD><TD>적정</TD></TR>'
                        '</TABLE>'
                        '<DOCUMENT>감사인 핵심감사사항 의견거절</DOCUMENT>'
                    ).encode('utf-8'),
                )
            ]
        )

        result = await DartSignalsService(client=client)._extract_audit_signal(
            {'rcept_no': '20260319000004', 'report_nm': '사업보고서'}
        )

        self.assertIsNone(result)

    async def test_extract_audit_signal_requires_current_marker_in_business_year_column(self):
        client = FakeSignalsClient(
            members=[
                (
                    'reports/wrong-current-column.xml',
                    (
                        '<TABLE ACLASS="NORMAL">'
                        '<TR><TH>사업연도</TH><TH>감사인</TH><TH>감사의견</TH>'
                        '<TH>강조사항 등</TH><TH>핵심감사사항</TH></TR>'
                        '<TR><TD>제9기(전기)</TD><TD>한영회계법인(당기)</TD>'
                        '<TD>의견거절</TD><TD>-</TD><TD>-</TD></TR>'
                        '</TABLE>'
                    ).encode('utf-8'),
                )
            ]
        )

        result = await DartSignalsService(client=client)._extract_audit_signal(
            {'rcept_no': '20260319000005', 'report_nm': '사업보고서'}
        )

        self.assertIsNone(result)

    async def test_extract_audit_signal_requires_legacy_current_marker_in_report_label_cell(self):
        client = FakeSignalsClient(
            members=[
                (
                    'reports/legacy-wrong-current-column.xml',
                    (
                        '<TABLE-GROUP ACLASS="AUD_OPN">'
                        '<TR><TD>감사보고서(전기)</TD><TD>한영회계법인(당기)</TD>'
                        '<TD>의견거절</TD></TR>'
                        '</TABLE-GROUP>'
                    ).encode('utf-8'),
                )
            ]
        )

        result = await DartSignalsService(client=client)._extract_audit_signal(
            {'rcept_no': '20260319000006', 'report_nm': '사업보고서'}
        )

        self.assertIsNone(result)

    async def test_extract_audit_signal_does_not_fallback_when_current_row_is_missing(self):
        client = FakeSignalsClient(
            members=[
                (
                    'reports/stale-only.xml',
                    (
                        '<TABLE-GROUP ACLASS="AUD_OPN">'
                        '<TR><TH>사업연도</TH><TH>감사인</TH><TH>감사의견</TH>'
                        '<TH>강조사항 등</TH><TH>핵심감사사항</TH></TR>'
                        '<TR><TD>제9기(전기)</TD><TD>한영회계법인</TD>'
                        '<TD>의견거절</TD><TD>-</TD><TD>-</TD></TR>'
                        '<TR><TD>감사보고서</TD><TD>의견거절</TD></TR>'
                        '</TABLE-GROUP>'
                    ).encode('utf-8'),
                )
            ]
        )

        result = await DartSignalsService(client=client)._extract_audit_signal(
            {'rcept_no': '20260319000006', 'report_nm': '사업보고서'}
        )

        self.assertIsNone(result)

    async def test_missing_audit_signal_reports_bounded_non_content_archive_diagnostics(self):
        secret_body = 'SECRET-AUDIT-BODY-MUST-NOT-BE-PERSISTED'
        members = [
            (
                'reports/non-authoritative-table.xml',
                (
                    '<?xml version="1.0" encoding="utf-8"?>'
                    '<TABLE-GROUP ACLASS="OTHER">'
                    '<TR><TD>감사보고서</TD><TD>적정의견</TD>'
                    f'<TD>{secret_body}</TD></TR>'
                    '</TABLE-GROUP>'
                ).encode('utf-8'),
            ),
            ('reports/undecodable.xml', b'\x81'),
            ('reports/plain.xml', b'<DOCUMENT>plain</DOCUMENT>'),
        ]
        client = FakeSignalsClient(members=members)
        repository = MagicMock()
        service = DartSignalsService(client=client, repository=repository)
        event = {
            'id': 99,
            'rcept_no': '20250325000540',
            'corp_code': '00960623',
            'stock_code': '266690',
            'corp_name': '덕산넥코어스',
            'receipt_date': date(2025, 3, 25),
            'report_nm': '사업보고서 (2024.12)',
        }

        with patch.object(service, '_load_event', return_value=event):
            result = await service.capture_audit_signal_for_event(
                rcept_no=event['rcept_no']
            )

        archive = client.download_content
        self.assertIsNotNone(archive)
        diagnostics = result['audit_diagnostics']
        self.assertIsNone(result['audit_signal'])
        self.assertEqual(diagnostics['version'], 'dart_audit_archive_diagnostics_v1')
        self.assertEqual(
            diagnostics['archive_sha256'],
            hashlib.sha256(archive).hexdigest(),
        )
        self.assertEqual(diagnostics['archive_size_bytes'], len(archive))
        self.assertEqual(diagnostics['member_count'], 3)
        self.assertEqual(diagnostics['decoded_member_count'], 2)
        self.assertEqual(diagnostics['aud_opn_table_count'], 0)
        self.assertEqual(diagnostics['audit_report_label_row_count'], 1)
        self.assertEqual(diagnostics['known_opinion_token_row_count'], 1)
        self.assertFalse(diagnostics['truncated'])
        self.assertEqual(
            [item['decode_success'] for item in diagnostics['members']],
            [True, False, True],
        )
        self.assertEqual(
            diagnostics['members'][0]['declared_encoding'],
            'utf-8',
        )
        persisted_shape = json.dumps(diagnostics, ensure_ascii=False)
        self.assertNotIn(secret_body, persisted_shape)
        self.assertNotIn('감사보고서', persisted_shape)
        self.assertNotIn('적정의견', persisted_shape)

    async def test_non_audit_document_preserves_stale_parser_signal(self):
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[('reports/plain.xml', b'<DOCUMENT>plain</DOCUMENT>')]
            ),
            repository=MagicMock(),
        )
        event = {
            'id': 99,
            'rcept_no': '20250325000541',
            'corp_code': '00960623',
            'stock_code': '266690',
            'corp_name': '덕산넥코어스',
            'receipt_date': date(2025, 3, 25),
            'report_nm': '사업보고서 (2024.12)',
        }

        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(service, '_store_signals') as store_signals,
        ):
            result = await service.capture_audit_signal_for_event(
                rcept_no=event['rcept_no']
            )

        self.assertIsNone(result['audit_signal'])
        self.assertIn('audit_diagnostics', result)
        self.assertFalse(result['audit_signal_evaluated'])
        store_signals.assert_called_once_with(
            event=event,
            audit_signal=None,
            dilution_signal=None,
            audit_signal_evaluated=False,
        )

    async def test_prior_period_audit_authority_revokes_stale_parser_signal(self):
        document = (
            '<TABLE-GROUP ACLASS="AUD_OPN">'
            '<TR><TD>감사보고서(전기)</TD><TD>의견거절</TD></TR>'
            '</TABLE-GROUP>'
        ).encode('utf-8')
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[('reports/prior-only.xml', document)]
            ),
            repository=MagicMock(),
        )
        event = {
            'id': 99,
            'rcept_no': '20250325000542',
            'corp_code': '00960623',
            'stock_code': '266690',
            'corp_name': '덕산넥코어스',
            'receipt_date': date(2025, 3, 25),
            'report_nm': '사업보고서 (2024.12)',
        }

        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(service, '_store_signals') as store_signals,
        ):
            result = await service.capture_audit_signal_for_event(
                rcept_no=event['rcept_no']
            )

        self.assertTrue(
            is_authoritative_absence_audit_payload(
                result['audit_signal']['payload'],
                event_id=event['id'],
                rcept_no=event['rcept_no'],
                receipt_date=event['receipt_date'],
            )
        )
        self.assertIn('audit_diagnostics', result)
        self.assertTrue(result['audit_signal_evaluated'])
        self.assertEqual(
            result['audit_diagnostics'][
                'negative_authority_evidence_count'
            ],
            1,
        )
        stored_signal = store_signals.call_args.kwargs['audit_signal']
        self.assertTrue(
            is_authoritative_absence_audit_payload(
                stored_signal.payload,
                event_id=event['id'],
                rcept_no=event['rcept_no'],
                receipt_date=event['receipt_date'],
            )
        )

    async def test_combined_capture_persists_authoritative_absence_sentinel(self):
        document = (
            '<TABLE-GROUP ACLASS="AUD_OPN">'
            '<TR><TD>감사보고서(전기)</TD><TD>의견거절</TD></TR>'
            '</TABLE-GROUP>'
        ).encode('utf-8')
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[('reports/prior-only.xml', document)]
            ),
            repository=MagicMock(),
        )
        event = _audit_event(rcept_no='20250325000542')

        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(service, '_store_signals') as store_signals,
        ):
            result = await service.capture_signals_for_event(
                rcept_no=event['rcept_no']
            )

        self.assertTrue(
            is_authoritative_absence_audit_payload(
                result['audit_signal']['payload'],
                event_id=event['id'],
                rcept_no=event['rcept_no'],
                receipt_date=event['receipt_date'],
            )
        )
        stored_signal = store_signals.call_args.kwargs['audit_signal']
        self.assertTrue(
            is_authoritative_absence_audit_payload(
                stored_signal.payload,
                event_id=event['id'],
                rcept_no=event['rcept_no'],
                receipt_date=event['receipt_date'],
            )
        )

    async def test_incomplete_parse_reuses_durable_exact_repair_signal(self):
        service = DartSignalsService(
            client=MagicMock(),
            repository=MagicMock(),
        )
        event = _audit_event(rcept_no='20250325000543')
        stored = _durable_unqualified_audit_signal()
        stored['payload']['exact_repair_lineage'] = {
            'schema_version': 'dart_audit_exact_repair_lineage_v1',
            'manifest_sha256': 'a' * 64,
            'source': {'rcept_no': event['rcept_no']},
            'target': {'rcept_no': event['rcept_no']},
            'mode': 'direct',
            'lineage_sha256': 'b' * 64,
        }
        durable_signal = ParsedAuditSignal(**stored)
        incomplete_diagnostics = {
            'member_count': 1,
            'decoded_member_count': 0,
            'positive_authority_evidence_count': 0,
            'negative_authority_evidence_count': 0,
            'ambiguous_authority_evidence_count': 1,
        }

        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(
                service,
                '_extract_audit_signal_with_diagnostics',
                new=AsyncMock(
                    return_value=(None, incomplete_diagnostics)
                ),
            ),
            patch.object(
                service,
                '_extract_dilution_signal',
                return_value=None,
            ),
            patch.object(
                service,
                '_store_signals',
                return_value=durable_signal,
            ) as store_signals,
        ):
            result = await service.capture_signals_for_event(
                rcept_no=event['rcept_no']
            )

        self.assertTrue(result['audit_signal_evaluated'])
        self.assertEqual(
            result['audit_signal']['payload']['exact_repair_lineage'],
            stored['payload']['exact_repair_lineage'],
        )
        self.assertNotIn('audit_diagnostics', result)
        self.assertTrue(
            store_signals.call_args.kwargs[
                'reuse_protected_existing_audit'
            ]
        )

    async def test_explicit_current_opinion_wins_over_prior_only_document(self):
        prior_only = _semantic_audit_document(
            ('제9기(전기)', '의견거절'),
            use_table_group=True,
        )
        explicit_current = (
            '<TABLE-GROUP ACLASS="AUD_OPN">'
            '<TR><TD>감사보고서(당기)</TD><TD>적정의견</TD></TR>'
            '</TABLE-GROUP>'
        ).encode('utf-8')
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[
                    ('reports/prior-only.xml', prior_only),
                    ('reports/current.xml', explicit_current),
                ]
            ),
            repository=MagicMock(),
        )
        event = {
            'id': 99,
            'rcept_no': '20250325000545',
            'corp_code': '00960623',
            'stock_code': '266690',
            'corp_name': '덕산넥코어스',
            'receipt_date': date(2025, 3, 25),
            'report_nm': '사업보고서 (2024.12)',
        }

        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(service, '_store_signals') as store_signals,
        ):
            result = await service.capture_audit_signal_for_event(
                rcept_no=event['rcept_no']
            )

        self.assertEqual(result['audit_signal']['opinion_code'], 'unqualified')
        self.assertTrue(result['audit_signal_evaluated'])
        stored_signal = store_signals.call_args.kwargs['audit_signal']
        self.assertEqual(stored_signal.opinion_code, 'unqualified')
        self.assertTrue(store_signals.call_args.kwargs['audit_signal_evaluated'])

    async def test_undecodable_archive_preserves_stale_parser_signal(self):
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[('reports/undecodable.xml', b'\x81')]
            ),
            repository=MagicMock(),
        )
        event = {
            'id': 99,
            'rcept_no': '20250325000543',
            'corp_code': '00960623',
            'stock_code': '266690',
            'corp_name': '덕산넥코어스',
            'receipt_date': date(2025, 3, 25),
            'report_nm': '사업보고서 (2024.12)',
        }

        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(service, '_store_signals') as store_signals,
        ):
            result = await service.capture_audit_signal_for_event(
                rcept_no=event['rcept_no']
            )

        self.assertIsNone(result['audit_signal'])
        self.assertEqual(
            result['audit_diagnostics']['decoded_member_count'],
            0,
        )
        self.assertFalse(result['audit_signal_evaluated'])
        store_signals.assert_called_once_with(
            event=event,
            audit_signal=None,
            dilution_signal=None,
            audit_signal_evaluated=False,
        )

    async def test_unknown_current_opinion_preserves_stale_parser_signal(self):
        transposed = (
            '<TABLE ACLASS="NORMAL">'
            '<TR><TH>사업연도</TH><TD>제10기(당기)</TD>'
            '<TD>제9기(전기)</TD></TR>'
            '<TR><TH>감사인</TH><TD>한영회계법인</TD>'
            '<TD>삼정회계법인</TD></TR>'
            '<TR><TH>감사의견</TH><TD>검토중</TD>'
            '<TD>의견거절</TD></TR>'
            '<TR><TH>핵심감사사항</TH><TD>-</TD><TD>-</TD></TR>'
            '</TABLE>'
        ).encode('utf-8')
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[
                    (
                        'reports/aud-opn-current-unknown.xml',
                        _semantic_audit_document(
                            ('제10기(당기)', '검토중'),
                            ('제9기(전기)', '의견거절'),
                            use_table_group=True,
                        ),
                    ),
                    ('reports/transposed-current-unknown.xml', transposed),
                ]
            ),
            repository=MagicMock(),
        )
        event = {
            'id': 99,
            'rcept_no': '20250325000544',
            'corp_code': '00960623',
            'stock_code': '266690',
            'corp_name': '덕산넥코어스',
            'receipt_date': date(2025, 3, 25),
            'report_nm': '사업보고서 (2024.12)',
        }

        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(service, '_store_signals') as store_signals,
        ):
            result = await service.capture_audit_signal_for_event(
                rcept_no=event['rcept_no']
            )

        self.assertIsNone(result['audit_signal'])
        self.assertFalse(result['audit_signal_evaluated'])
        self.assertEqual(
            result['audit_diagnostics'][
                'ambiguous_authority_evidence_count'
            ],
            2,
        )
        store_signals.assert_called_once_with(
            event=event,
            audit_signal=None,
            dilution_signal=None,
            audit_signal_evaluated=False,
        )

    def test_store_signals_rejects_completed_evaluation_without_sentinel(self):
        service = DartSignalsService(
            client=MagicMock(),
            repository=MagicMock(),
        )
        event = {'rcept_no': '20250325000541'}
        session = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = session
        context.__exit__.return_value = False

        with (
            self.assertRaisesRegex(ValueError, 'authoritative absence'),
            patch(
                'app.dart.signals_service.session_scope',
                return_value=context,
            ) as session_scope,
        ):
            service._store_signals(
                event=event,
                audit_signal=None,
                dilution_signal=None,
                audit_signal_evaluated=True,
            )

        session_scope.assert_not_called()

        session.reset_mock()
        with patch(
            'app.dart.signals_service.session_scope',
            return_value=context,
        ):
            service._store_signals(
                event=event,
                audit_signal=None,
                dilution_signal=None,
                audit_signal_evaluated=False,
            )
        session.execute.assert_not_called()

    def test_store_signals_serializes_every_audit_mutation_transaction(self):
        service = DartSignalsService(
            client=MagicMock(),
            repository=MagicMock(),
        )
        event = _audit_event()
        audit_signal = build_authoritative_absence_audit_signal(
            event,
            _complete_negative_audit_diagnostics(),
        )
        session = MagicMock()
        session.get_bind.return_value.dialect.name = 'postgresql'
        context = MagicMock()
        context.__enter__.return_value = session
        context.__exit__.return_value = False

        with patch(
            'app.dart.signals_service.session_scope',
            return_value=context,
        ):
            service._store_signals(
                event=event,
                audit_signal=audit_signal,
                dilution_signal=None,
                audit_signal_evaluated=True,
            )

        self.assertEqual(session.execute.call_count, 3)
        lock_call, existing_call, upsert_call = session.execute.call_args_list
        self.assertIn('pg_advisory_xact_lock', str(lock_call.args[0]))
        self.assertEqual(
            lock_call.args[1],
            {'audit_signal_writer_lock_key': AUDIT_SIGNAL_WRITER_LOCK_KEY},
        )
        self.assertTrue(existing_call.args[0].is_select)
        self.assertTrue(upsert_call.args[0].is_insert)

    def test_ordinary_capture_cannot_overwrite_exact_repair_lineage(self):
        service = DartSignalsService(
            client=MagicMock(),
            repository=MagicMock(),
        )
        event = _audit_event()
        identity = {
            'event_id': event['id'],
            'rcept_no': event['rcept_no'],
            'stock_code': event['stock_code'],
            'receipt_date': event['receipt_date'].isoformat(),
        }
        lineage = {
            'schema_version': 'dart_audit_exact_repair_lineage_v1',
            'manifest_sha256': 'a' * 64,
            'source': identity,
            'target': identity,
            'mode': 'direct',
        }
        lineage['lineage_sha256'] = hashlib.sha256(
            json.dumps(
                lineage,
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':'),
            ).encode('utf-8')
        ).hexdigest()
        stored = _durable_unqualified_audit_signal()
        stored['payload']['exact_repair_lineage'] = lineage
        existing = SimpleNamespace(
            disclosure_event_id=event['id'],
            rcept_no=event['rcept_no'],
            receipt_date=event['receipt_date'],
            **stored,
        )
        session = MagicMock()
        session.get_bind.return_value.dialect.name = 'postgresql'
        lock_result = MagicMock()
        existing_result = MagicMock()
        existing_result.scalar_one_or_none.return_value = existing
        session.execute.side_effect = [lock_result, existing_result]
        context = MagicMock()
        context.__enter__.return_value = session
        context.__exit__.return_value = False

        incoming = build_authoritative_absence_audit_signal(
            event,
            _complete_negative_audit_diagnostics(),
        )
        with patch(
            'app.dart.signals_service.session_scope',
            return_value=context,
        ):
            stored_signal = service._store_signals(
                event=event,
                audit_signal=incoming,
                dilution_signal=None,
                audit_signal_evaluated=True,
            )

        self.assertEqual(session.execute.call_count, 2)
        self.assertTrue(session.execute.call_args_list[1].args[0].is_select)
        self.assertEqual(stored_signal.payload, stored['payload'])

    def test_incomplete_ordinary_capture_reads_protected_exact_repair_lineage(
        self,
    ):
        service = DartSignalsService(
            client=MagicMock(),
            repository=MagicMock(),
        )
        event = _audit_event(rcept_no='20250325000544')
        identity = {
            'event_id': event['id'],
            'rcept_no': event['rcept_no'],
            'stock_code': event['stock_code'],
            'receipt_date': event['receipt_date'].isoformat(),
        }
        lineage = {
            'schema_version': 'dart_audit_exact_repair_lineage_v1',
            'manifest_sha256': 'a' * 64,
            'source': identity,
            'target': identity,
            'mode': 'direct',
        }
        lineage['lineage_sha256'] = hashlib.sha256(
            json.dumps(
                lineage,
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':'),
            ).encode('utf-8')
        ).hexdigest()
        stored = _durable_unqualified_audit_signal()
        stored['payload']['exact_repair_lineage'] = lineage
        existing = SimpleNamespace(
            disclosure_event_id=event['id'],
            rcept_no=event['rcept_no'],
            receipt_date=event['receipt_date'],
            **stored,
        )
        session = MagicMock()
        session.get_bind.return_value.dialect.name = 'postgresql'
        existing_result = MagicMock()
        existing_result.scalar_one_or_none.return_value = existing
        session.execute.side_effect = [MagicMock(), existing_result]
        context = MagicMock()
        context.__enter__.return_value = session
        context.__exit__.return_value = False

        with patch(
            'app.dart.signals_service.session_scope',
            return_value=context,
        ):
            stored_signal = service._store_signals(
                event=event,
                audit_signal=None,
                dilution_signal=None,
                audit_signal_evaluated=False,
                reuse_protected_existing_audit=True,
            )

        self.assertEqual(session.execute.call_count, 2)
        self.assertIn(
            'pg_advisory_xact_lock',
            str(session.execute.call_args_list[0].args[0]),
        )
        self.assertTrue(session.execute.call_args_list[1].args[0].is_select)
        self.assertEqual(stored_signal.payload, stored['payload'])

    def test_store_signals_rejects_malformed_authoritative_absence(self):
        service = DartSignalsService(
            client=MagicMock(),
            repository=MagicMock(),
        )
        event = _audit_event()
        audit_signal = build_authoritative_absence_audit_signal(
            event,
            _complete_negative_audit_diagnostics(),
        )
        audit_signal.payload['schema_version'] = 'invalid'

        with (
            self.assertRaisesRegex(ValueError, 'strict validation'),
            patch('app.dart.signals_service.session_scope') as session_scope,
        ):
            service._store_signals(
                event=event,
                audit_signal=audit_signal,
                dilution_signal=None,
                audit_signal_evaluated=True,
            )

        session_scope.assert_not_called()

    def test_guard_owned_audit_mutation_bypasses_nested_lock_without_deadlock(self):
        service = DartSignalsService(
            client=MagicMock(),
            repository=MagicMock(),
        )
        session = MagicMock()
        session.get_bind.return_value.dialect.name = 'postgresql'
        context = MagicMock()
        context.__enter__.return_value = session
        context.__exit__.return_value = False

        with (
            patch(
                'app.dart.signals_service.session_scope',
                return_value=context,
            ),
            audit_signal_writer_lock_owner(),
        ):
            event = _audit_event()
            service._store_signals(
                event=event,
                audit_signal=build_authoritative_absence_audit_signal(
                    event,
                    _complete_negative_audit_diagnostics(),
                ),
                dilution_signal=None,
                audit_signal_evaluated=True,
            )

        self.assertEqual(session.execute.call_count, 2)
        self.assertTrue(session.execute.call_args_list[0].args[0].is_select)
        self.assertTrue(session.execute.call_args_list[1].args[0].is_insert)

    def test_dilution_only_mutation_uses_shared_dart_signal_writer_lock(self):
        service = DartSignalsService(
            client=MagicMock(),
            repository=MagicMock(),
        )
        session = MagicMock()
        session.get_bind.return_value.dialect.name = 'postgresql'
        context = MagicMock()
        context.__enter__.return_value = session
        context.__exit__.return_value = False

        with (
            patch(
                'app.dart.signals_service.session_scope',
                return_value=context,
            ),
            patch.object(service, '_execute_dilution_upsert') as upsert,
        ):
            service._store_signals(
                event={'rcept_no': '20250325000541'},
                audit_signal=None,
                dilution_signal={'signal_type': 'share_issuance'},
                audit_signal_evaluated=False,
            )

        session.execute.assert_called_once()
        self.assertIn(
            'pg_advisory_xact_lock',
            str(session.execute.call_args.args[0]),
        )
        upsert.assert_called_once_with(
            session,
            event={'rcept_no': '20250325000541'},
            dilution_signal={'signal_type': 'share_issuance'},
        )

    async def test_missing_audit_signal_diagnostics_bound_member_metadata_and_names(self):
        members = [
            (f'reports/{index}-' + ('x' * 100) + '.xml', b'<DOCUMENT>plain</DOCUMENT>')
            for index in range(4)
        ]
        service = DartSignalsService(
            client=FakeSignalsClient(members=members),
            repository=MagicMock(),
        )
        event = {
            'id': 99,
            'rcept_no': '20250325000540',
            'corp_code': '00960623',
            'stock_code': '266690',
            'corp_name': '덕산넥코어스',
            'receipt_date': date(2025, 3, 25),
            'report_nm': '사업보고서 (2024.12)',
        }

        with (
            patch.object(service, '_load_event', return_value=event),
            patch(
                'app.dart.signals_service.MAX_AUDIT_DIAGNOSTIC_MEMBERS',
                2,
            ),
            patch(
                'app.dart.signals_service.MAX_AUDIT_DIAGNOSTIC_MEMBER_NAME_CHARS',
                24,
            ),
        ):
            result = await service.capture_signals_for_event(
                rcept_no=event['rcept_no']
            )

        diagnostics = result['audit_diagnostics']
        self.assertIsNone(result['audit_signal'])
        self.assertEqual(diagnostics['member_count'], 4)
        self.assertEqual(diagnostics['decoded_member_count'], 4)
        self.assertEqual(len(diagnostics['members']), 2)
        self.assertTrue(diagnostics['truncated'])
        self.assertTrue(all(len(item['name']) <= 24 for item in diagnostics['members']))
        self.assertTrue(all(item['name_truncated'] for item in diagnostics['members']))

    async def test_successful_audit_capture_preserves_response_shape_without_diagnostics(self):
        service = DartSignalsService(
            client=FakeSignalsClient(
                members=[('reports/audit.xml', _audit_document('적정의견'))]
            ),
            repository=MagicMock(),
        )
        event = {
            'id': 99,
            'rcept_no': '20260327000781',
            'corp_code': '00960623',
            'stock_code': '266690',
            'corp_name': '덕산넥코어스',
            'receipt_date': date(2026, 3, 27),
            'report_nm': '사업보고서 (2025.12)',
        }

        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(service, '_store_signals') as store_signals,
        ):
            result = await service.capture_audit_signal_for_event(
                rcept_no=event['rcept_no']
            )

        self.assertEqual(result['audit_signal']['opinion_code'], 'unqualified')
        self.assertNotIn('audit_diagnostics', result)
        store_signals.assert_called_once()

    async def test_audit_capture_preserves_provider_error_semantics(self):
        provider_error = RuntimeError('provider error envelope')
        client = MagicMock()
        client.download_document = AsyncMock(side_effect=provider_error)
        service = DartSignalsService(client=client, repository=MagicMock())
        event = {
            'id': 99,
            'rcept_no': '20250325000540',
            'corp_code': '00960623',
            'stock_code': '266690',
            'corp_name': '덕산넥코어스',
            'receipt_date': date(2025, 3, 25),
            'report_nm': '사업보고서 (2024.12)',
        }

        with (
            patch.object(service, '_load_event', return_value=event),
            patch.object(service, '_store_signals') as store_signals,
        ):
            with self.assertRaises(RuntimeError) as raised:
                await service.capture_audit_signal_for_event(
                    rcept_no=event['rcept_no']
                )

        self.assertIs(raised.exception, provider_error)
        store_signals.assert_not_called()

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
                    'audit_signal': _durable_unqualified_audit_signal(),
                },
                {
                    'rcept_no': '20260422000012',
                    'audit_signal': _durable_unqualified_audit_signal(),
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
        audit_diagnostics = {
            'version': 'dart_audit_archive_diagnostics_v1',
            'archive_sha256': 'b' * 64,
            'archive_size_bytes': 456,
            'member_count': 1,
            'members': [],
            'decoded_member_count': 0,
            'aud_opn_table_count': 0,
            'audit_report_label_row_count': 0,
            'known_opinion_token_row_count': 0,
            'truncated': True,
        }
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
                'audit_diagnostics': audit_diagnostics,
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
        self.assertEqual(
            result['results'][0]['audit_diagnostics'],
            audit_diagnostics,
        )

    async def test_backfill_treats_authoritative_absence_as_terminal(self):
        event = _audit_event(
            event_id=31,
            rcept_no='20260422000031',
        )
        sentinel = build_authoritative_absence_audit_signal(
            event,
            _complete_negative_audit_diagnostics(),
        )
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': event['id'],
                    'rcept_no': event['rcept_no'],
                    'stock_code': event['stock_code'],
                    'receipt_date': event['receipt_date'].isoformat(),
                    'report_nm': event['report_nm'],
                }
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
        )
        service.capture_audit_signal_for_event = AsyncMock(
            return_value={
                'rcept_no': event['rcept_no'],
                'audit_signal': sentinel.__dict__,
                'audit_signal_evaluated': True,
            }
        )

        result = await service.backfill_missing_annual_audit_signals(
            market_date=date(2026, 4, 30),
            limit=1,
            after_event_id=30,
            dry_run=False,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            result['results'][0]['status'],
            'authoritative_absence',
        )
        self.assertEqual(result['audit_signals_captured'], 0)
        self.assertEqual(result['audit_signals_authoritatively_absent'], 1)
        self.assertEqual(result['audit_evidence_missing'], 0)
        self.assertFalse(result['retry_required'])
        self.assertEqual(result['retry_event_ids'], [])
        self.assertEqual(result['next_after_event_id'], 31)

    async def test_backfill_retries_malformed_authoritative_absence(self):
        event = _audit_event(
            event_id=32,
            rcept_no='20260422000032',
        )
        sentinel = build_authoritative_absence_audit_signal(
            event,
            _complete_negative_audit_diagnostics(),
        )
        sentinel.payload['audit_diagnostics']['archive_size_bytes'] = 0
        sentinel.payload['schema_version'] = 'invalid'
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': event['id'],
                    'rcept_no': event['rcept_no'],
                    'stock_code': event['stock_code'],
                    'receipt_date': event['receipt_date'].isoformat(),
                    'report_nm': event['report_nm'],
                }
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
        )
        service.capture_audit_signal_for_event = AsyncMock(
            return_value={
                'rcept_no': event['rcept_no'],
                'audit_signal': sentinel.__dict__,
                'audit_signal_evaluated': True,
            }
        )

        result = await service.backfill_missing_annual_audit_signals(
            market_date=date(2026, 4, 30),
            limit=1,
            after_event_id=31,
            dry_run=False,
        )

        self.assertEqual(result['status'], 'completed_with_gaps')
        self.assertEqual(
            result['results'][0]['status'],
            'audit_evidence_missing',
        )
        self.assertEqual(result['audit_signals_captured'], 0)
        self.assertEqual(result['audit_signals_authoritatively_absent'], 0)
        self.assertEqual(result['audit_evidence_missing'], 1)
        self.assertTrue(result['retry_required'])
        self.assertEqual(result['retry_event_ids'], [32])

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

    async def test_backfill_event_deadline_is_retry_debt_and_preserves_forward_progress(self):
        event_ids = list(range(31, 36))
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': event_id,
                    'rcept_no': f'202604220000{event_id}',
                    'stock_code': f'{event_id:06d}',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                }
                for event_id in event_ids
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
            event_timeout_seconds=0.001,
        )

        async def capture(*, rcept_no):
            event_id = int(rcept_no[-2:])
            if event_id == 32:
                await asyncio.Event().wait()
            return {
                'rcept_no': rcept_no,
                'audit_signal': _durable_unqualified_audit_signal(),
            }

        service.capture_audit_signal_for_event = AsyncMock(side_effect=capture)

        result = await service.backfill_missing_annual_audit_signals(
            market_date=date(2026, 4, 30),
            limit=5,
            after_event_id=30,
            dry_run=False,
        )

        self.assertEqual(result['status'], 'completed_with_gaps')
        self.assertEqual(result['candidates_attempted'], 5)
        self.assertEqual(result['audit_signals_captured'], 4)
        self.assertEqual(result['events_failed'], 1)
        self.assertEqual(result['retry_event_ids'], [32])
        self.assertEqual(result['next_after_event_id'], 30)
        self.assertEqual(result['next_forward_after_event_id'], 35)
        self.assertFalse(result['circuit_breaker']['tripped'])
        self.assertEqual(
            result['circuit_breaker']['consecutive_systemic_failures'],
            0,
        )
        failed = result['results'][1]
        self.assertEqual(failed['status'], 'failed')
        self.assertTrue(failed['event_timeout'])
        self.assertFalse(failed['systemic_provider_failure'])
        self.assertIn('audit event processing exceeded', failed['error'])
        self.assertEqual(service.capture_audit_signal_for_event.await_count, 5)

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

    async def test_backfill_missing_audit_signals_runs_bounded_ordered_waves(self):
        event_ids = list(range(51, 56))
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': event_id,
                    'rcept_no': f'202604220000{event_id}',
                    'stock_code': f'{event_id:06d}',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                }
                for event_id in event_ids
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
        )
        started: list[int] = []
        release = {event_id: asyncio.Event() for event_id in event_ids}
        started_counts = {count: asyncio.Event() for count in (2, 4, 5)}
        active_count = 0
        max_active_count = 0

        async def capture(*, rcept_no):
            nonlocal active_count, max_active_count
            event_id = int(rcept_no[-2:])
            started.append(event_id)
            active_count += 1
            max_active_count = max(max_active_count, active_count)
            if len(started) in started_counts:
                started_counts[len(started)].set()
            await release[event_id].wait()
            active_count -= 1
            return {
                'rcept_no': rcept_no,
                'audit_signal': _durable_unqualified_audit_signal(),
            }

        service.capture_audit_signal_for_event = AsyncMock(side_effect=capture)
        backfill_task = asyncio.create_task(
            service.backfill_missing_annual_audit_signals(
                market_date=date(2026, 4, 30),
                limit=5,
                after_event_id=50,
                concurrency=2,
                dry_run=False,
            )
        )

        await asyncio.wait_for(started_counts[2].wait(), timeout=1)
        self.assertEqual(started, [51, 52])
        release[52].set()
        await asyncio.sleep(0)
        self.assertEqual(started, [51, 52])
        release[51].set()

        await asyncio.wait_for(started_counts[4].wait(), timeout=1)
        self.assertEqual(started, [51, 52, 53, 54])
        release[54].set()
        await asyncio.sleep(0)
        self.assertEqual(started, [51, 52, 53, 54])
        release[53].set()

        await asyncio.wait_for(started_counts[5].wait(), timeout=1)
        self.assertEqual(started, event_ids)
        release[55].set()
        result = await asyncio.wait_for(backfill_task, timeout=1)

        self.assertEqual(max_active_count, 2)
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['concurrency'], 2)
        self.assertEqual(result['candidates_attempted'], 5)
        self.assertEqual(result['audit_signals_captured'], 5)
        self.assertEqual(
            [item['event_id'] for item in result['results']],
            event_ids,
        )
        self.assertEqual(result['next_after_event_id'], 55)
        self.assertEqual(result['next_forward_after_event_id'], 55)

    async def test_backfill_circuit_breaker_finishes_started_wave_then_stops(self):
        event_ids = list(range(61, 66))
        repository = FakeSignalsRepository(
            [
                {
                    'event_id': event_id,
                    'rcept_no': f'202604220000{event_id}',
                    'stock_code': f'{event_id:06d}',
                    'receipt_date': '2026-04-22',
                    'report_nm': '사업보고서',
                }
                for event_id in event_ids
            ]
        )
        service = DartSignalsService(
            client=FakeSignalsClient(members=[]),
            repository=repository,
        )

        async def capture(*, rcept_no):
            event_id = int(rcept_no[-2:])
            if event_id in {61, 62, 63}:
                raise TimeoutError(f'DART timed out for {event_id}')
            if event_id == 64:
                return {
                    'rcept_no': rcept_no,
                    'audit_signal': _durable_unqualified_audit_signal(),
                }
            raise AssertionError('the circuit breaker must stop the next wave')

        service.capture_audit_signal_for_event = AsyncMock(side_effect=capture)

        result = await service.backfill_missing_annual_audit_signals(
            market_date=date(2026, 4, 30),
            limit=5,
            after_event_id=60,
            concurrency=2,
            dry_run=False,
        )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['candidates_attempted'], 4)
        self.assertEqual(result['events_failed'], 3)
        self.assertEqual(result['audit_signals_captured'], 1)
        self.assertEqual(result['retry_event_ids'], [61, 62, 63])
        self.assertEqual(result['failed_event_ids'], [61, 62, 63])
        self.assertEqual(result['unattempted_event_ids'], [65])
        self.assertEqual(result['next_after_event_id'], 60)
        self.assertEqual(result['next_forward_after_event_id'], 64)
        self.assertEqual(
            [item['status'] for item in result['results']],
            ['failed', 'failed', 'failed', 'captured'],
        )
        self.assertEqual(
            result['circuit_breaker'],
            {
                'tripped': True,
                'threshold': 3,
                'consecutive_systemic_failures': 3,
            },
        )
        self.assertEqual(service.capture_audit_signal_for_event.await_count, 4)

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
        for concurrency in (0, 9):
            with self.subTest(concurrency=concurrency):
                with self.assertRaisesRegex(
                    ValueError,
                    'concurrency must be between 1 and 8',
                ):
                    await service.backfill_missing_annual_audit_signals(
                        market_date=date(2026, 4, 30),
                        concurrency=concurrency,
                    )


class DartSignalsRepositoryPostgresTests(unittest.TestCase):
    def test_same_date_incomplete_attempt_cannot_downgrade_completed_authority(self):
        corp_code = uuid4().hex[:8]
        stock_code = f'T{uuid4().hex[:8]}'
        receipt_date = date(2025, 10, 30)

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            try:
                service = DartSignalsService(client=object())
                with patch(
                    'app.dart.signals_service.session_scope',
                    rollback_only_session_scope,
                ):
                    service._store_dilution_signal_rows(
                        [
                            _share_issuance_attempt_row(
                                corp_code=corp_code,
                                stock_code=stock_code,
                                receipt_date=receipt_date,
                                evidence_semantics='completed_scan_attempt',
                                authority_marker='completed-first',
                            )
                        ]
                    )
                    service._store_dilution_signal_rows(
                        [
                            _share_issuance_attempt_row(
                                corp_code=corp_code,
                                stock_code=stock_code,
                                receipt_date=receipt_date,
                                evidence_semantics='incomplete_scan_attempt',
                                authority_marker='incomplete-later',
                            )
                        ]
                    )

                session.expire_all()
                row = (
                    session.query(DartDilutionSignal)
                    .filter(
                        DartDilutionSignal.rcept_no == f'DILATT-{corp_code}'
                    )
                    .one()
                )
                self.assertEqual(
                    row.payload['evidence_semantics'],
                    'completed_scan_attempt',
                )
                self.assertEqual(
                    row.payload['authority_marker'],
                    'completed-first',
                )
            finally:
                session.close()
                transaction.rollback()

    def test_completed_authority_can_promote_same_date_and_refresh_newer_date(self):
        corp_code = uuid4().hex[:8]
        stock_code = f'T{uuid4().hex[:8]}'
        first_date = date(2025, 10, 30)
        newer_date = date(2025, 10, 31)

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            try:
                service = DartSignalsService(client=object())
                with patch(
                    'app.dart.signals_service.session_scope',
                    rollback_only_session_scope,
                ):
                    for receipt_date, semantics, marker in (
                        (
                            first_date,
                            'incomplete_scan_attempt',
                            'incomplete-first',
                        ),
                        (
                            first_date,
                            'completed_scan_attempt',
                            'same-date-completed',
                        ),
                        (
                            newer_date,
                            'completed_scan_attempt',
                            'newer-completed',
                        ),
                    ):
                        service._store_dilution_signal_rows(
                            [
                                _share_issuance_attempt_row(
                                    corp_code=corp_code,
                                    stock_code=stock_code,
                                    receipt_date=receipt_date,
                                    evidence_semantics=semantics,
                                    authority_marker=marker,
                                )
                            ]
                        )

                session.expire_all()
                row = (
                    session.query(DartDilutionSignal)
                    .filter(
                        DartDilutionSignal.rcept_no == f'DILATT-{corp_code}'
                    )
                    .one()
                )
                self.assertEqual(row.receipt_date, newer_date)
                self.assertEqual(
                    row.payload['evidence_semantics'],
                    'completed_scan_attempt',
                )
                self.assertEqual(
                    row.payload['authority_marker'],
                    'newer-completed',
                )
            finally:
                session.close()
                transaction.rollback()

    def test_positive_issuance_date_queries_include_every_positive_signal_type(self):
        receipt_date = date(2025, 10, 30)
        positive_signal_types = (
            'paid_increase',
            'convertible_bond',
            'bond_with_warrant',
            'exchange_bond',
        )

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            try:
                corporation_rows = []
                for signal_type in positive_signal_types:
                    corp_code = uuid4().hex[:8]
                    stock_code = f'T{uuid4().hex[:8]}'
                    corporation_rows.append((corp_code, stock_code))
                    session.add(
                        DartDilutionSignal(
                            disclosure_event_id=None,
                            rcept_no=f'R{uuid4().hex[:20]}',
                            corp_code=corp_code,
                            stock_code=stock_code,
                            corp_name='Positive issuance type test',
                            receipt_date=receipt_date,
                            report_nm='Positive issuance disclosure',
                            signal_type=signal_type,
                            has_dilution_signal=True,
                            security_type='test',
                            reason='positive issuance type test',
                            payload={'evidence_semantics': 'positive_event_match'},
                        )
                    )
                session.flush()

                repository = DartSignalsRepository()
                with patch(
                    'app.dart.signals_service.session_scope',
                    rollback_only_session_scope,
                ):
                    latest_by_corp = (
                        repository.list_latest_share_issuance_positive_event_dates(
                            corp_codes=[row[0] for row in corporation_rows],
                            on_or_before=receipt_date,
                        )
                    )
                    latest_by_single_corp = {
                        corp_code: repository.get_latest_share_issuance_positive_event_date(
                            stock_code=stock_code,
                            corp_code=corp_code,
                            on_or_before=receipt_date,
                        )
                        for corp_code, stock_code in corporation_rows
                    }

                expected = {
                    corp_code: receipt_date
                    for corp_code, _stock_code in corporation_rows
                }
                self.assertEqual(latest_by_corp, expected)
                self.assertEqual(latest_by_single_corp, expected)
            finally:
                session.close()
                transaction.rollback()

    def test_complete_negative_share_issuance_coverage_is_idempotent(self):
        corp_code = uuid4().hex[:8]
        stock_code = f'T{uuid4().hex[:8]}'
        window_start = date(2024, 10, 30)
        window_end = date(2025, 10, 30)

        with engine.connect() as connection:
            transaction = connection.begin()
            session = Session(bind=connection)

            @contextmanager
            def rollback_only_session_scope():
                yield session
                session.flush()

            try:
                service = DartSignalsService(client=object())
                with patch(
                    'app.dart.signals_service.session_scope',
                    rollback_only_session_scope,
                ):
                    for _ in range(2):
                        result = service.persist_share_issuance_scan_evidence(
                            corporation={
                                'corp_code': corp_code,
                                'stock_code': stock_code,
                                'corp_name': 'Idempotent coverage test',
                            },
                            raw_items=[],
                            window_start=window_start,
                            window_end=window_end,
                            pagination_completed=True,
                        )
                        self.assertEqual(result['negative_coverage_count'], 1)

                rows = (
                    session.query(DartDilutionSignal)
                    .filter(
                        DartDilutionSignal.corp_code == corp_code,
                        DartDilutionSignal.receipt_date == window_end,
                    )
                    .all()
                )
                self.assertEqual(len(rows), 2)
                rows_by_receipt = {row.rcept_no: row for row in rows}
                self.assertEqual(
                    set(rows_by_receipt),
                    {f'DILATT-{corp_code}', f'DILCOV-{corp_code}'},
                )
                current_row = rows_by_receipt[f'DILCOV-{corp_code}']
                self.assertFalse(current_row.has_dilution_signal)
                self.assertEqual(
                    current_row.payload['evidence_coverage'][
                        'matched_event_count'
                    ],
                    0,
                )
            finally:
                session.close()
                transaction.rollback()

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
            eligible_event, eligible_snapshot = add_snapshot(
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
            corrected_stale_event, _ = add_snapshot(
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
                    payload={
                        'opinion_cell': '적정',
                        'row_cells': ['적정'],
                    },
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
                    selected_rows = (
                        repository.list_selected_annual_audit_signal_candidates(
                            market_date=market_date,
                            event_ids=[
                                eligible_event.id,
                                audited_event.id,
                                corrected_stale_event.id,
                            ],
                        )
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
                self.assertEqual(
                    {row['event_id'] for row in selected_rows},
                    {eligible_event.id, audited_event.id},
                )
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
                        payload={
                            'opinion_cell': '적정',
                            'row_cells': ['적정'],
                        },
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
