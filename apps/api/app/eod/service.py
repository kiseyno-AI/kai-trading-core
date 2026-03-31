from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.brokers.kis.provider import kis_client
from app.eod.sink import snapshot_sink
from app.eod.state import job_state
from app.eod.universe import UniverseSymbol, universe_service
from app.market.clock import get_market_state


@dataclass
class EODSnapshot:
    symbol: str
    market: str
    business_date: str
    open_price: int
    high_price: int
    low_price: int
    close_price: int
    volume: int
    turnover: int
    source: str = 'kis'
    persisted: bool = False

    def to_dict(self) -> dict:
        return self.__dict__.copy()


class EODService:
    batch_size = 50
    batch_pause_seconds = 0.2

    async def capture_if_due(self, now: Optional[datetime] = None) -> dict:
        market = get_market_state(now)
        market_date = market.server_time[:10]

        if market.state != 'CLOSED':
            return {
                'triggered': False,
                'reason': f'market_not_closed:{market.state}',
                'market_date': market_date,
                'job': job_state.snapshot(),
            }

        if job_state.last_market_date == market_date and job_state.last_status == 'completed':
            return {
                'triggered': False,
                'reason': 'already_completed',
                'market_date': market_date,
                'job': job_state.snapshot(),
            }

        return await self.capture_market_date(market_date)

    async def capture_market_date(self, market_date: str) -> dict:
        symbols = list(universe_service.iter_symbols())
        job_state.start(market_date=market_date, symbols_total=len(symbols))
        items: list[dict] = []

        try:
            for offset in range(0, len(symbols), self.batch_size):
                batch = symbols[offset: offset + self.batch_size]
                for item in batch:
                    try:
                        snapshot = await self.fetch_symbol_eod(item=item, market_date=market_date)
                        payload = snapshot.to_dict()
                        items.append(payload)
                        self.persist_snapshot(payload)
                        job_state.mark_success()
                    except Exception:
                        job_state.mark_failure(item.symbol)
                if offset + self.batch_size < len(symbols):
                    await asyncio.sleep(self.batch_pause_seconds)
        except Exception as exc:
            job_state.fail(exc)
            raise

        job_state.complete()
        return {
            'triggered': True,
            'market_date': market_date,
            'items': items,
            'job': job_state.snapshot(),
        }

    async def fetch_symbol_eod(self, item: UniverseSymbol, market_date: str) -> EODSnapshot:
        raw = await kis_client.fetch_domestic_daily_chart(
            symbol=item.symbol,
            market_code=item.market,
            start_date=market_date.replace('-', ''),
            end_date=market_date.replace('-', ''),
        )
        output = raw.get('output2', []) if isinstance(raw, dict) else []
        latest = output[0] if output else {}
        return EODSnapshot(
            symbol=item.symbol,
            market=item.market,
            business_date=market_date,
            open_price=int(latest.get('stck_oprc', 0) or 0),
            high_price=int(latest.get('stck_hgpr', 0) or 0),
            low_price=int(latest.get('stck_lwpr', 0) or 0),
            close_price=int(latest.get('stck_clpr', 0) or 0),
            volume=int(latest.get('acml_vol', 0) or 0),
            turnover=int(latest.get('acml_tr_pbmn', 0) or 0),
        )

    def persist_snapshot(self, snapshot: dict) -> None:
        snapshot_sink.persist(snapshot)


EOD_SERVICE = EODService()
