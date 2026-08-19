from __future__ import annotations

import asyncio
from collections import Counter
from typing import Callable

from app.integrations.kis_overseas_master.client import KISOverseasMasterClient
from app.integrations.nasdaq_trader.client import NasdaqTraderClient
from app.integrations.sec_edgar.client import SECEdgarClient
from app.repositories.symbol_repository import upsert_symbol_universe
from app.us_equities.symbols import build_us_symbol_universe


class USUniverseRefreshService:
    def __init__(
        self,
        *,
        nasdaq_client: NasdaqTraderClient | None = None,
        kis_master_client: KISOverseasMasterClient | None = None,
        sec_client: SECEdgarClient | None = None,
        persister: Callable[..., int] = upsert_symbol_universe,
    ) -> None:
        self._nasdaq_client = nasdaq_client
        self._kis_master_client = kis_master_client
        self._sec_client = sec_client
        self._persister = persister

    async def refresh(self, *, persist: bool = True, preview_limit: int = 20) -> dict:
        nasdaq_client = self._nasdaq_client or NasdaqTraderClient()
        kis_master_client = self._kis_master_client or KISOverseasMasterClient()
        sec_client = self._sec_client or SECEdgarClient()
        listed, kis_rows, sec_rows = await asyncio.gather(
            nasdaq_client.fetch_universe(),
            kis_master_client.fetch_us_universe(),
            sec_client.fetch_company_ticker_exchange_mapping(),
        )
        integrated = build_us_symbol_universe(listed, kis_rows, sec_rows)
        persisted_count = 0
        if persist and integrated.symbols:
            persisted_count = await asyncio.to_thread(
                self._persister,
                list(integrated.symbols),
                source='us_integrated_master',
                stale_sources=['us_integrated_master'],
                stale_markets=['NASDAQ', 'NYSE', 'NYSE_AMERICAN'],
                instrument_type='equity',
            )
        exclusion_counts = Counter(
            reason for reasons in integrated.exclusions.values() for reason in reasons
        )
        sec_matched = sum(item.sec_mapping_status == 'matched' for item in integrated.symbols)
        return {
            'status': 'completed',
            'persist': persist,
            'counts': {
                'nasdaq_trader_rows': len(listed),
                'kis_master_rows': len(kis_rows),
                'sec_ticker_rows': len(sec_rows),
                'integrated': len(integrated.symbols),
                'sec_matched': sec_matched,
                'sec_missing': len(integrated.symbols) - sec_matched,
                'excluded_symbols': len(integrated.exclusions),
            },
            'exclusion_reason_counts': dict(sorted(exclusion_counts.items())),
            'persisted_count': persisted_count,
            'preview': [
                {
                    'symbol': item.symbol,
                    'kis_symbol': item.kis_symbol,
                    'name': item.name,
                    'exchange': item.exchange,
                    'currency': item.currency,
                    'cik': item.cik,
                    'sec_mapping_status': item.sec_mapping_status,
                }
                for item in integrated.symbols[: max(0, preview_limit)]
            ],
        }


us_universe_refresh_service = USUniverseRefreshService()
