import io
import unittest
import zipfile

import httpx

from app.integrations.kis_overseas_master.client import (
    KISOverseasMasterClient,
    parse_kis_overseas_master_zip,
)


def _archive_bytes(filename: str, row: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as archive:
        archive.writestr(filename, row.encode('cp949'))
    return buffer.getvalue()


class KISOverseasMasterClientTests(unittest.IsolatedAsyncioTestCase):
    def test_parses_official_cp949_master_archive(self):
        row = '\t'.join([
            'US', '22', 'NAS', '나스닥', 'AAPL', 'NASAAPL', '애플', 'APPLE INC', '2', 'USD', '4', '',
            '220.1250', '1', '1', '930', '1600', 'N', '', '013', '1', '0', '004', '',
        ])

        items = parse_kis_overseas_master_zip(_archive_bytes('nasmst.cod', row), market_code='nas')

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].symbol, 'AAPL')
        self.assertEqual(items[0].exchange, 'NASDAQ')
        self.assertEqual(items[0].currency, 'USD')
        self.assertEqual(items[0].base_price, '220.1250')
        self.assertFalse(items[0].is_depositary_receipt)

    async def test_fetches_requested_us_market_archives(self):
        paths: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            paths.append(request.url.path)
            code = request.url.path.rsplit('/', 1)[-1][:3]
            row = '\t'.join([
                'US', '22', code.upper(), code.upper(), code.upper(), code.upper(), code.upper(), code.upper(),
                '2', 'USD', '4', '', '10.0000', '1', '1', '930', '1600', 'N', '', '000', '0', '0', '004', '',
            ])
            return httpx.Response(200, content=_archive_bytes(f'{code}mst.cod', row), request=request)

        client = KISOverseasMasterClient(transport=httpx.MockTransport(handler))

        items = await client.fetch_us_universe()

        self.assertEqual(paths, [
            '/common/master/nasmst.cod.zip',
            '/common/master/nysmst.cod.zip',
            '/common/master/amsmst.cod.zip',
        ])
        self.assertEqual(len(items), 3)


if __name__ == '__main__':
    unittest.main()
