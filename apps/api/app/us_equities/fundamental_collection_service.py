from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import asdict
from datetime import date
from decimal import Decimal
from typing import Any, Callable

from app.core.settings import settings
from app.integrations.sec_edgar.client import SECEdgarClient
from app.repositories.us_equity_repository import (
    load_us_price_screen_candidates,
    update_us_market_data_run_fundamentals,
    upsert_us_fundamental_snapshot,
    upsert_us_screening_snapshots,
)
from app.us_equities.screening import (
    USFundamentalScreenMetrics,
    USScreeningPolicy,
    screen_us_fundamental_candidates,
)
from app.us_equities.sec_fundamentals import SECFundamentalSnapshotData, parse_sec_fundamental_snapshot


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, 'f')
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _missing_metrics(symbol: str) -> USFundamentalScreenMetrics:
    return USFundamentalScreenMetrics(
        symbol=symbol,
        latest_periodic_filing_date=None,
        ttm_revenue=None,
        ttm_operating_cash_flow=None,
        ttm_net_income=None,
        total_assets=None,
        total_liabilities=None,
        shares_outstanding_growth_yoy=None,
        has_bankruptcy_or_delinquency_flag=False,
    )


class USFundamentalCollectionService:
    def __init__(
        self,
        *,
        sec_client: SECEdgarClient | None = None,
        candidate_loader: Callable[..., list[dict]] = load_us_price_screen_candidates,
    ) -> None:
        self._sec_client = sec_client
        self._candidate_loader = candidate_loader

    async def collect_and_screen(
        self,
        *,
        market_date: date,
        persist: bool = True,
        policy: USScreeningPolicy | None = None,
    ) -> dict[str, Any]:
        candidates = await asyncio.to_thread(self._candidate_loader, market_date=market_date, limit=300)
        if not candidates:
            raise ValueError('No price-screen candidates found for the requested market date')
        client = self._sec_client or SECEdgarClient()
        semaphore = asyncio.Semaphore(settings.us_equity_sec_concurrency)
        errors: dict[str, str] = {}

        async def collect_one(item: dict) -> tuple[dict, SECFundamentalSnapshotData | None]:
            symbol = str(item['symbol'])
            cik = str(item.get('regulatory_id') or '').strip()
            if not cik:
                errors[symbol] = 'missing_sec_cik'
                return item, None
            async with semaphore:
                try:
                    companyfacts, submissions = await asyncio.gather(
                        client.fetch_companyfacts(cik),
                        client.fetch_submissions(cik),
                    )
                    snapshot = parse_sec_fundamental_snapshot(
                        symbol=symbol,
                        cik=cik,
                        companyfacts=companyfacts,
                        submissions=submissions,
                        market_date=market_date,
                    )
                    if snapshot is None:
                        errors[symbol] = 'no_periodic_sec_facts'
                    elif persist:
                        await asyncio.to_thread(
                            upsert_us_fundamental_snapshot,
                            symbol_id=int(item['id']),
                            snapshot=snapshot,
                        )
                    return item, snapshot
                except Exception as exc:
                    errors[symbol] = f'{type(exc).__name__}: {exc}'
                    return item, None

        collected = await asyncio.gather(*(collect_one(item) for item in candidates))
        metrics_by_symbol = {
            str(item['symbol']): snapshot.metrics if snapshot else _missing_metrics(str(item['symbol']))
            for item, snapshot in collected
        }
        result = screen_us_fundamental_candidates(
            metrics_by_symbol.values(),
            market_date=market_date,
            policy=policy,
        )
        selected_symbols = {item.symbol for item in result.selected}
        ranks = {item.symbol: index for index, item in enumerate(result.selected, start=1)}
        screening_rows = []
        for item, snapshot in collected:
            symbol = str(item['symbol'])
            reasons = list(result.exclusions.get(symbol, ()))
            if symbol in errors and errors[symbol] not in reasons:
                reasons.append(errors[symbol])
            screening_rows.append({
                'symbol_id': int(item['id']),
                'collection_run_id': item.get('collection_run_id'),
                'market_date': market_date,
                'stage': 'fundamental_screen',
                'eligible_for_agent': symbol in selected_symbols,
                'rank': ranks.get(symbol),
                'composite_score': result.scores.get(symbol),
                'metrics_payload': {
                    'price': _json_value(item.get('metrics_payload') or {}),
                    'fundamental': _json_value(asdict(metrics_by_symbol[symbol])),
                },
                'decision_payload': {
                    'selected': symbol in selected_symbols,
                    'exclusion_reasons': reasons,
                    'policy_version': 'us_fundamental_screen_v1',
                    'sec_accession_number': snapshot.accession_number if snapshot else None,
                },
            })
        if persist:
            await asyncio.to_thread(upsert_us_screening_snapshots, rows=screening_rows)
            run_ids = {int(item['collection_run_id']) for item in candidates if item.get('collection_run_id') is not None}
            for run_id in run_ids:
                await asyncio.to_thread(
                    update_us_market_data_run_fundamentals,
                    run_id,
                    fundamental_eligible_count=len(result.selected),
                    error_count_delta=len(errors),
                )

        exclusion_counts = Counter(reason for reasons in result.exclusions.values() for reason in reasons)
        return {
            'status': 'completed' if not errors else 'completed_with_errors',
            'market_date': market_date.isoformat(),
            'persist': persist,
            'counts': {
                'price_candidates': len(candidates),
                'sec_snapshots_collected': sum(snapshot is not None for _, snapshot in collected),
                'fundamental_eligible_before_cap': result.eligible_before_cap,
                'agent_candidates': len(result.selected),
                'errors': len(errors),
            },
            'exclusion_reason_counts': dict(sorted(exclusion_counts.items())),
            'selected_symbols': [item.symbol for item in result.selected],
            'error_preview': dict(list(sorted(errors.items()))[:20]),
        }


us_fundamental_collection_service = USFundamentalCollectionService()
