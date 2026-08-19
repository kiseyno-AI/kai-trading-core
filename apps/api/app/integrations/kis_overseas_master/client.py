from __future__ import annotations

import csv
from dataclasses import dataclass
from io import BytesIO, StringIO
import zipfile

import httpx

from app.core.audit_logging import log_fetch_event, request_with_audit
from app.core.settings import settings


class KISOverseasMasterError(RuntimeError):
    pass


_EXCHANGE_BY_MARKET_CODE = {
    'nas': 'NASDAQ',
    'nys': 'NYSE',
    'ams': 'NYSE_AMERICAN',
}
_FIELD_COUNT = 24


@dataclass(frozen=True, slots=True)
class KISOverseasMasterSecurity:
    symbol: str
    realtime_symbol: str
    exchange: str
    korean_name: str
    english_name: str
    security_type: str
    currency: str
    decimal_places: int | None
    base_price: str
    bid_order_size: str
    ask_order_size: str
    market_open_hhmm: str
    market_close_hhmm: str
    is_depositary_receipt: bool
    industry_code: str | None
    constituent_flag: str | None
    etp_type_code: str | None


def parse_kis_overseas_master_zip(payload: bytes, *, market_code: str) -> list[KISOverseasMasterSecurity]:
    normalized_market = market_code.strip().lower()
    if normalized_market not in _EXCHANGE_BY_MARKET_CODE:
        raise ValueError(f'Unsupported KIS overseas master market code: {market_code}')
    try:
        with zipfile.ZipFile(BytesIO(payload)) as archive:
            members = [name for name in archive.namelist() if not name.endswith('/')]
            if not members:
                raise KISOverseasMasterError('KIS overseas master archive is empty')
            raw_text = archive.read(members[0]).decode('cp949')
    except (zipfile.BadZipFile, UnicodeDecodeError) as exc:
        raise KISOverseasMasterError('Invalid KIS overseas master archive') from exc

    items: list[KISOverseasMasterSecurity] = []
    for row in csv.reader(StringIO(raw_text), delimiter='\t'):
        if len(row) < _FIELD_COUNT:
            continue
        decimal_text = row[10].strip()
        try:
            decimal_places = int(decimal_text) if decimal_text else None
        except ValueError:
            decimal_places = None
        symbol = row[4].strip().upper()
        if not symbol:
            continue
        items.append(
            KISOverseasMasterSecurity(
                symbol=symbol,
                realtime_symbol=row[5].strip(),
                exchange=_EXCHANGE_BY_MARKET_CODE[normalized_market],
                korean_name=row[6].strip(),
                english_name=row[7].strip(),
                security_type=row[8].strip(),
                currency=row[9].strip().upper(),
                decimal_places=decimal_places,
                base_price=row[12].strip(),
                bid_order_size=row[13].strip(),
                ask_order_size=row[14].strip(),
                market_open_hhmm=row[15].strip(),
                market_close_hhmm=row[16].strip(),
                is_depositary_receipt=row[17].strip().upper() == 'Y',
                industry_code=row[19].strip() or None,
                constituent_flag=row[20].strip() or None,
                etp_type_code=row[22].strip() or None,
            )
        )
    return sorted(items, key=lambda item: (item.symbol, item.exchange))


class KISOverseasMasterClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        timeout_seconds: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = (base_url or settings.kis_overseas_master_base_url).rstrip('/')
        self.timeout_seconds = timeout_seconds or settings.kis_overseas_master_request_timeout_seconds
        self._transport = transport

    async def fetch_us_universe(self) -> list[KISOverseasMasterSecurity]:
        items: list[KISOverseasMasterSecurity] = []
        for market_code in ('nas', 'nys', 'ams'):
            items.extend(await self.fetch_market(market_code))
        return sorted(items, key=lambda item: (item.symbol, item.exchange))

    async def fetch_market(self, market_code: str) -> list[KISOverseasMasterSecurity]:
        normalized_market = market_code.strip().lower()
        if normalized_market not in _EXCHANGE_BY_MARKET_CODE:
            raise ValueError(f'Unsupported KIS overseas master market code: {market_code}')
        path = f'/common/master/{normalized_market}mst.cod.zip'
        async with httpx.AsyncClient(timeout=self.timeout_seconds, transport=self._transport) as client:
            response = await request_with_audit(client, 'kis_overseas_master', 'GET', f'{self.base_url}{path}')
        if response.status_code >= 400:
            raise KISOverseasMasterError(
                f'KIS overseas master request failed [{response.status_code}] for {normalized_market}'
            )
        items = parse_kis_overseas_master_zip(response.content, market_code=normalized_market)
        log_fetch_event(
            service='kis_overseas_master',
            operation='fetch_market',
            status='completed',
            market_code=normalized_market,
            row_count=len(items),
        )
        return items
