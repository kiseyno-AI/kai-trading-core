import io
import unittest
import zipfile

from app.dart.signals_service import DartSignalsService, _decode_document_text


def _build_archive(members):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode='w') as archive:
        for name, content in members:
            archive.writestr(name, content)
    return buffer.getvalue()


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


class DartSignalsServiceTests(unittest.IsolatedAsyncioTestCase):
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
