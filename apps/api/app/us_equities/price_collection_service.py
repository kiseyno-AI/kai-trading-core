from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import asdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Callable

from app.brokers.kis.client import KISClient
from app.brokers.kis.provider import kis_client as process_wide_kis_client
from app.core.settings import settings
from app.repositories.us_equity_repository import (
    create_us_market_data_run,
    finish_us_market_data_run,
    load_active_us_symbols,
    upsert_us_price_bars,
    upsert_us_screening_snapshots,
)
from app.us_equities.market_data import USPriceBar, compute_us_price_screen_metrics, parse_kis_daily_price_payload
from app.us_equities.screening import USPriceScreenMetrics, USScreeningPolicy, screen_us_price_candidates


def _ranking_symbol(row: dict[str, Any]) -> str | None:
    for key in ('symb', 'SYMB', 'ovrs_pdno', 'OVRS_PDNO', 'rsym', 'RSYM'):
        value = str(row.get(key) or '').strip().upper()
        if value:
            return value
    return None


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


def build_ranked_prefilter(
    symbols: list[dict],
    ranking_payloads: list[dict[str, Any]],
    *,
    cap: int,
) -> list[dict]:
    by_kis_symbol: dict[str, dict] = {}
    for item in symbols:
        source_payload = item.get('source_payload') if isinstance(item.get('source_payload'), dict) else {}
        kis_symbol = str(source_payload.get('kis_symbol') or item.get('symbol') or '').strip().upper()
        if kis_symbol:
            by_kis_symbol[kis_symbol] = item

    scores: Counter[str] = Counter()
    first_seen: dict[str, int] = {}
    sequence = 0
    for payload in ranking_payloads:
        rows = payload.get('output2') if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            continue
        row_count = len(rows)
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            kis_symbol = _ranking_symbol(row)
            if kis_symbol not in by_kis_symbol:
                continue
            scores[kis_symbol] += max(1, row_count - index)
            first_seen.setdefault(kis_symbol, sequence)
            sequence += 1
    ranked_symbols = sorted(scores, key=lambda symbol: (-scores[symbol], first_seen[symbol], symbol))
    return [by_kis_symbol[symbol] for symbol in ranked_symbols[:cap]]


class USPriceCollectionService:
    def __init__(
        self,
        *,
        kis_client: KISClient | None = None,
        symbol_loader: Callable[..., list[dict]] = load_active_us_symbols,
    ) -> None:
        self._kis_client = kis_client or process_wide_kis_client
        self._symbol_loader = symbol_loader

    async def _load_rankings(self) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for exchange in ('NASDAQ', 'NYSE', 'NYSE_AMERICAN'):
            payloads.append(
                await self._kis_client.fetch_overseas_market_cap_ranking(
                    exchange=exchange,
                    max_pages=settings.us_equity_ranking_max_pages,
                )
            )
            payloads.append(
                await self._kis_client.fetch_overseas_trading_value_ranking(
                    exchange=exchange,
                    minimum_price='5',
                    max_pages=settings.us_equity_ranking_max_pages,
                )
            )
            if settings.us_equity_history_request_pause_seconds:
                await asyncio.sleep(settings.us_equity_history_request_pause_seconds)
        return payloads

    async def _load_history(self, symbol_row: dict, *, market_date: date) -> list[USPriceBar]:
        source_payload = symbol_row.get('source_payload') if isinstance(symbol_row.get('source_payload'), dict) else {}
        kis_symbol = str(source_payload.get('kis_symbol') or symbol_row['symbol']).strip().upper()
        exchange = str(symbol_row.get('exchange_code') or symbol_row.get('market') or '').strip().upper()
        as_of = market_date
        bars_by_date: dict[date, USPriceBar] = {}
        previous_oldest: date | None = None
        for _ in range(settings.us_equity_history_max_pages):
            payload = await self._kis_client.fetch_overseas_daily_prices(
                exchange=exchange,
                symbol=kis_symbol,
                as_of_date=as_of.strftime('%Y%m%d'),
                adjusted=True,
            )
            page = parse_kis_daily_price_payload(payload, symbol=str(symbol_row['symbol']))
            if not page:
                break
            for bar in page:
                bars_by_date.setdefault(bar.trade_date, bar)
            oldest = min(bar.trade_date for bar in page)
            if previous_oldest is not None and oldest >= previous_oldest:
                break
            previous_oldest = oldest
            as_of = oldest - timedelta(days=1)
            if settings.us_equity_history_request_pause_seconds:
                await asyncio.sleep(settings.us_equity_history_request_pause_seconds)
        return [bars_by_date[key] for key in sorted(bars_by_date)]

    async def collect_and_screen(
        self,
        *,
        market_date: date,
        persist: bool = True,
        prefilter_cap: int | None = None,
        policy: USScreeningPolicy | None = None,
    ) -> dict[str, Any]:
        symbols = await asyncio.to_thread(self._symbol_loader)
        if not symbols:
            raise ValueError('US symbol universe is empty; refresh /us-equities/universe first')
        run_id = (
            await asyncio.to_thread(
                create_us_market_data_run,
                market_date=market_date,
                universe_count=len(symbols),
            )
            if persist
            else None
        )
        try:
            rankings = await self._load_rankings()
            candidates = build_ranked_prefilter(
                symbols,
                rankings,
                cap=min(
                    prefilter_cap or settings.us_equity_price_prefilter_cap,
                    settings.us_equity_price_prefilter_cap,
                ),
            )
            semaphore = asyncio.Semaphore(settings.us_equity_history_concurrency)
            errors: dict[str, str] = {}

            async def collect_one(item: dict) -> tuple[dict, list[USPriceBar]]:
                async with semaphore:
                    try:
                        bars = await self._load_history(item, market_date=market_date)
                        if persist and bars:
                            await asyncio.to_thread(
                                upsert_us_price_bars,
                                symbol_id=int(item['id']),
                                bars=bars,
                                collection_run_id=run_id,
                            )
                        return item, bars
                    except Exception as exc:
                        errors[str(item['symbol'])] = f'{type(exc).__name__}: {exc}'
                        return item, []

            collected = await asyncio.gather(*(collect_one(item) for item in candidates))
            metrics_by_symbol: dict[str, USPriceScreenMetrics] = {
                str(item['symbol']): compute_us_price_screen_metrics(str(item['symbol']), bars)
                for item, bars in collected
            }
            result = screen_us_price_candidates(metrics_by_symbol.values(), policy=policy)
            selected_symbols = {item.symbol for item in result.selected}
            ranks = {item.symbol: index for index, item in enumerate(result.selected, start=1)}
            symbol_by_code = {str(item['symbol']): item for item in candidates}
            screening_rows = []
            for symbol, metrics in metrics_by_symbol.items():
                reasons = result.exclusions.get(symbol, ())
                screening_rows.append(
                    {
                        'symbol_id': int(symbol_by_code[symbol]['id']),
                        'collection_run_id': run_id,
                        'market_date': market_date,
                        'stage': 'price_screen',
                        'eligible_for_agent': symbol in selected_symbols,
                        'rank': ranks.get(symbol),
                        'composite_score': result.scores.get(symbol),
                        'metrics_payload': _json_value(asdict(metrics)),
                        'decision_payload': {
                            'selected': symbol in selected_symbols,
                            'exclusion_reasons': list(reasons),
                            'policy_version': 'us_price_screen_v1',
                        },
                    }
                )
            if persist and screening_rows:
                await asyncio.to_thread(upsert_us_screening_snapshots, rows=screening_rows)

            exclusion_counts = Counter(reason for reasons in result.exclusions.values() for reason in reasons)
            summary = {
                'status': 'completed' if not errors else 'completed_with_errors',
                'market_date': market_date.isoformat(),
                'run_id': run_id,
                'persist': persist,
                'counts': {
                    'universe': len(symbols),
                    'ranking_prefilter': len(candidates),
                    'histories_collected': sum(bool(bars) for _, bars in collected),
                    'price_eligible_before_cap': result.eligible_before_cap,
                    'price_selected': len(result.selected),
                    'errors': len(errors),
                },
                'exclusion_reason_counts': dict(sorted(exclusion_counts.items())),
                'selected_symbols': [item.symbol for item in result.selected],
                'error_preview': dict(list(sorted(errors.items()))[:20]),
            }
            if persist and run_id is not None:
                await asyncio.to_thread(
                    finish_us_market_data_run,
                    run_id,
                    status=summary['status'],
                    price_history_count=summary['counts']['histories_collected'],
                    price_eligible_count=len(result.selected),
                    error_count=len(errors),
                    summary_payload=summary,
                )
            return summary
        except Exception as exc:
            if persist and run_id is not None:
                await asyncio.to_thread(
                    finish_us_market_data_run,
                    run_id,
                    status='failed',
                    price_history_count=0,
                    price_eligible_count=0,
                    error_count=1,
                    summary_payload={'error': f'{type(exc).__name__}: {exc}'},
                )
            raise


us_price_collection_service = USPriceCollectionService()
