"""Deterministic market-environment inputs for the market-context analyst.

Every number is produced here, in code, from APIs the system already uses
(KIS, Bank of Korea ECOS, Korea Exim via the stored FX snapshot) and from the
stored daily risk snapshots. The agent only interprets. Each leg is isolated:
a failing leg is reported as an error and never hides the others.
"""
from __future__ import annotations

import asyncio
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Awaitable, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

KST = ZoneInfo('Asia/Seoul')
LEG_TIMEOUT_SECONDS = 40.0
# KIS has no client-side rate limiter and Core may be calling it at the same time:
# issue KIS requests one at a time with a small gap.
KIS_REQUEST_SPACING_SECONDS = 0.25
LIMIT_MOVE_PCT = 29.0

INDEX_CODES = {'kospi': '0001', 'kosdaq': '1001'}
INVESTOR_MARKETS = {'kospi': ('KSP', '0001'), 'kosdaq': ('KSQ', '1001')}
# US proxies: (label, KIS exchange, symbol, what it stands for)
US_PROXIES = (
    ('spy', 'NYSE_AMERICAN', 'SPY', 'S&P 500'),
    ('qqq', 'NASDAQ', 'QQQ', 'Nasdaq 100'),
    ('soxx', 'NASDAQ', 'SOXX', 'US semiconductors'),
    ('ewy', 'NYSE_AMERICAN', 'EWY', 'MSCI Korea (US-listed)'),
    ('tlt', 'NASDAQ', 'TLT', 'US 20y+ Treasuries (rates, inverse)'),
    ('uup', 'NYSE_AMERICAN', 'UUP', 'US dollar index'),
    ('uso', 'NYSE_AMERICAN', 'USO', 'WTI crude oil'),
    ('gld', 'NYSE_AMERICAN', 'GLD', 'gold'),
)
BOK_SERIES = {
    'ktb_3y': dict(stat_code='817Y002', cycle='D', item_code_1='010200000'),
    'ktb_10y': dict(stat_code='817Y002', cycle='D', item_code_1='010210000'),
    'call_1d': dict(stat_code='817Y002', cycle='D', item_code_1='010101000'),
    'usdkrw_bok': dict(stat_code='731Y001', cycle='D', item_code_1='0000001'),
}


def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value).replace(',', '').strip())
    except ValueError:
        return None


def _pick(source: Mapping[str, Any], mapping: Mapping[str, str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for key, source_key in mapping.items():
        number = _num(source.get(source_key))
        if number is not None:
            out[key] = number
    return out


def parse_index(output: Mapping[str, Any]) -> dict[str, Any]:
    data = _pick(
        output,
        {
            'level': 'bstp_nmix_prpr', 'change': 'bstp_nmix_prdy_vrss', 'change_pct': 'bstp_nmix_prdy_ctrt',
            'open': 'bstp_nmix_oprc', 'high': 'bstp_nmix_hgpr', 'low': 'bstp_nmix_lwpr',
            'volume_thousand_shares': 'acml_vol', 'turnover_million_krw': 'acml_tr_pbmn',
            'prev_turnover_million_krw': 'prdy_tr_pbmn', 'advancers': 'ascn_issu_cnt',
            'decliners': 'down_issu_cnt', 'unchanged': 'stnr_issu_cnt', 'limit_up': 'uplm_issu_cnt',
            'limit_down': 'lslm_issu_cnt', 'high_52w': 'dryy_bstp_nmix_hgpr', 'low_52w': 'dryy_bstp_nmix_lwpr',
            'pct_from_52w_high': 'dryy_hgpr_vrss_prpr_rate', 'pct_from_52w_low': 'dryy_lwpr_vrss_prpr_rate',
        },
    )
    if 'high' in data and 'low' in data and data['low'] > 0 and 'level' in data:
        data['day_range_pct'] = round((data['high'] - data['low']) / data['low'] * 100, 3)
        data['close_position_in_range'] = (
            round((data['level'] - data['low']) / (data['high'] - data['low']), 3) if data['high'] > data['low'] else None
        )
    return data


def parse_flows(row: Mapping[str, Any]) -> dict[str, Any]:
    groups = {
        'foreign': 'frgn', 'institution': 'orgn', 'individual': 'prsn', 'securities': 'scrt',
        'investment_trust': 'ivtr', 'private_fund': 'pe_fund', 'bank': 'bank', 'insurance': 'insu',
    }
    out: dict[str, Any] = {'unit': 'million_krw'}
    for label, prefix in groups.items():
        net = _num(row.get(f'{prefix}_ntby_tr_pbmn'))
        if net is not None:
            out[f'{label}_net'] = net
    for label, prefix in (('foreign', 'frgn'), ('institution', 'orgn')):
        buy, sell = _num(row.get(f'{prefix}_shnu_tr_pbmn')), _num(row.get(f'{prefix}_seln_tr_pbmn'))
        if buy is not None and sell is not None:
            out[f'{label}_gross_traded'] = buy + sell
    return out


def _first_row(output: Any) -> Mapping[str, Any]:
    if isinstance(output, list):
        return output[0] if output and isinstance(output[0], Mapping) else {}
    return output if isinstance(output, Mapping) else {}


def parse_quote(output: Mapping[str, Any]) -> dict[str, Any]:
    data = _pick(output, {'last': 'last', 'previous_close': 'base', 'change': 'diff', 'change_pct': 'rate', 'volume': 'tvol'})
    return data


def summarize_series(rows: Sequence[Mapping[str, Any]], *, unit_hint: str | None = None) -> dict[str, Any]:
    points = sorted(
        ((str(r.get('TIME')), _num(r.get('DATA_VALUE'))) for r in rows if _num(r.get('DATA_VALUE')) is not None),
    )
    if not points:
        return {'status': 'empty'}
    last_time, last = points[-1]
    out: dict[str, Any] = {'as_of': last_time, 'last': last, 'recent': [{'date': t, 'value': v} for t, v in points[-5:]]}
    if len(points) >= 2:
        out['change_vs_previous'] = round(last - points[-2][1], 4)
    if unit_hint == 'yield_pct' and len(points) >= 2:
        out['change_bp'] = round((last - points[-2][1]) * 100, 1)
    return out


def aggregate_breadth(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """rows: market, sector, ret (pct), turnover (KRW), flags."""
    by_market: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    by_sector: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get('ret') is None:
            continue
        market = str(row.get('market') or 'unknown').upper()
        by_market[market].append(row)
        if row.get('sector'):
            by_sector[str(row['sector'])].append(row)

    def market_stats(items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        rets = [float(i['ret']) for i in items]
        turnover = sum(float(i.get('turnover') or 0) for i in items)
        weighted = (
            sum(float(i['ret']) * float(i.get('turnover') or 0) for i in items) / turnover if turnover > 0 else None
        )
        return {
            'names': len(items),
            'advancers': sum(r > 0 for r in rets), 'decliners': sum(r < 0 for r in rets), 'unchanged': sum(r == 0 for r in rets),
            'advancer_share': round(sum(r > 0 for r in rets) / len(rets), 4),
            'median_return_pct': round(statistics.median(rets), 3),
            'mean_return_pct': round(statistics.fmean(rets), 3),
            'turnover_weighted_return_pct': round(weighted, 3) if weighted is not None else None,
            'turnover_krw': round(turnover),
            'down_5pct_or_worse': sum(r <= -5 for r in rets), 'up_5pct_or_better': sum(r >= 5 for r in rets),
            'limit_down_like': sum(r <= -LIMIT_MOVE_PCT for r in rets), 'limit_up_like': sum(r >= LIMIT_MOVE_PCT for r in rets),
        }

    sectors = []
    for name, items in by_sector.items():
        if len(items) < 5:
            continue
        stats = market_stats(items)
        sectors.append({'sector': name, **{k: stats[k] for k in ('names', 'median_return_pct', 'turnover_weighted_return_pct', 'turnover_krw', 'advancer_share')}})
    sectors.sort(key=lambda s: (s['median_return_pct'], s['sector']))
    flags = {
        'investment_caution': sum(bool(r.get('caution')) for r in rows),
        'trade_halted': sum(bool(r.get('halted')) for r in rows),
        'vi_triggered': sum(bool(r.get('vi')) for r in rows),
    }
    return {
        'markets': {m: market_stats(items) for m, items in sorted(by_market.items())},
        'weakest_sectors': sectors[:5],
        'strongest_sectors': list(reversed(sectors[-5:])),
        'sector_count': len(sectors),
        'warning_flags': flags,
    }


BREADTH_SQL = """
select snapshot_payload->'raw'->>'rprs_mrkt_kor_name' as market,
       snapshot_payload->'raw'->>'bstp_kor_isnm' as sector,
       snapshot_payload->'raw'->>'prdy_ctrt' as ret,
       snapshot_payload->'raw'->>'acml_tr_pbmn' as turnover,
       is_investment_caution as caution, is_trade_halted as halted, is_vi_triggered as vi
from daily_market_risk_snapshots where market_date = :d
"""


def load_breadth_rows(market_date: date, *, session_scope: Callable[..., Any] | None = None) -> list[dict[str, Any]]:
    from sqlalchemy import text

    if session_scope is None:
        from app.db import session_scope as default_scope

        session_scope = default_scope
    with session_scope() as session:
        session.execute(text("SET LOCAL statement_timeout = '60s'"))
        rows = session.execute(text(BREADTH_SQL), {'d': market_date}).mappings().all()
    return [
        {**dict(r), 'ret': _num(r['ret']), 'turnover': _num(r['turnover'])}
        for r in rows
    ]


def breadth_history(
    market_date: date, *, days: int = 5, loader: Callable[[date], list[dict[str, Any]]] = load_breadth_rows,
    is_trading_day: Callable[[date], bool] | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cursor = market_date - timedelta(days=1)
    steps = 0
    while len(out) < days and steps < 14:
        steps += 1
        if cursor.weekday() < 5 and (is_trading_day is None or is_trading_day(cursor)):
            rows = [r for r in loader(cursor) if r.get('ret') is not None]
            if rows:
                rets = [r['ret'] for r in rows]
                out.append(
                    {
                        'date': cursor.isoformat(), 'names': len(rows),
                        'advancer_share': round(sum(x > 0 for x in rets) / len(rets), 4),
                        'median_return_pct': round(statistics.median(rets), 3),
                    }
                )
        cursor -= timedelta(days=1)
    return out


def latest_session_as_of(now_kst: datetime, is_trading_day: Callable[[date], bool] | None) -> date:
    """The most recent session whose closing data the live KIS endpoints still describe."""
    day = now_kst.date() if now_kst.hour >= 9 else now_kst.date() - timedelta(days=1)
    for _ in range(14):
        if day.weekday() < 5 and (is_trading_day is None or is_trading_day(day)):
            return day
        day -= timedelta(days=1)
    return day


async def _leg(name: str, coro: Awaitable[Any]) -> dict[str, Any]:
    try:
        return {'status': 'ok', 'data': await asyncio.wait_for(coro, LEG_TIMEOUT_SECONDS)}
    except Exception as exc:  # isolate: one failed leg must not hide the others
        return {'status': 'error', 'error': f'{type(exc).__name__}: {str(exc)[:120]}'}


def _unwrap(leg: Mapping[str, Any], transform: Callable[[Any], Any]) -> dict[str, Any]:
    if leg.get('status') != 'ok':
        return dict(leg)
    try:
        return {'status': 'ok', 'data': transform(leg['data'])}
    except Exception as exc:
        return {'status': 'error', 'error': f'parse {type(exc).__name__}'}


async def collect_market_inputs(
    market_date: date,
    *,
    kis: Any,
    ecos: Any,
    now: datetime | None = None,
    breadth_loader: Callable[[date], list[dict[str, Any]]] = load_breadth_rows,
    fx_loader: Callable[[], dict[str, Any] | None] | None = None,
    is_trading_day: Callable[[date], bool] | None = None,
    kis_spacing_seconds: float = KIS_REQUEST_SPACING_SECONDS,
) -> dict[str, Any]:
    now_kst = (now or datetime.now(KST)).astimezone(KST)
    live = market_date == latest_session_as_of(now_kst, is_trading_day)
    legs: dict[str, Any] = {}

    jobs: list[tuple[str, str, Awaitable[Any]]] = []
    if live:
        for name, code in INDEX_CODES.items():
            jobs.append(('index', name, _leg(name, kis.fetch_market_index_price(index_code=code))))
        for name, (market_code, sector_code) in INVESTOR_MARKETS.items():
            jobs.append(('flow', name, _leg(name, kis.fetch_investor_time_by_market(market_code=market_code, sector_code=sector_code))))
        for label, exchange, symbol, _desc in US_PROXIES:
            jobs.append(('us', label, _leg(label, kis.fetch_overseas_quote(exchange=exchange, symbol=symbol))))
    start = (market_date - timedelta(days=14)).strftime('%Y%m%d')
    for key, kw in BOK_SERIES.items():
        jobs.append(('bok', key, _leg(key, ecos.fetch_series(start_period=start, end_period=market_date.strftime('%Y%m%d'), end_row=20, **kw))))
    results: dict[tuple[str, str], Any] = {}
    for kind, name, coro in jobs:
        if kind == 'bok':
            continue
        results[(kind, name)] = await coro
        if kis_spacing_seconds > 0:
            await asyncio.sleep(kis_spacing_seconds)
    bok_jobs = [job for job in jobs if job[0] == 'bok']  # a different server: fine to run together
    for job, leg in zip(bok_jobs, await asyncio.gather(*(job[2] for job in bok_jobs))):
        results[(job[0], job[1])] = leg

    def group(kind: str) -> list[str]:
        return [name for (k, name) in results if k == kind]

    if live:
        legs['indices'] = {n: _unwrap(results[('index', n)], lambda d: parse_index(d.get('output') or {})) for n in group('index')}
        legs['investor_flows'] = {
            n: _unwrap(results[('flow', n)], lambda d: parse_flows(_first_row(d.get('output'))))
            for n in group('flow')
        }
        legs['us_overnight'] = {
            label: {**_unwrap(results[('us', label)], lambda d: parse_quote(d.get('output') or {})), 'proxy_for': desc}
            for label, _ex, _sym, desc in US_PROXIES
        }
        legs['us_overnight_note'] = (
            'US ETF proxies read at collection time (KST evening): the last completed or in-progress US session; '
            'not an index close.'
        )
    else:
        legs['indices'] = legs['investor_flows'] = legs['us_overnight'] = {'status': 'skipped_historical_date'}

    rates = {
        key: _unwrap(results[('bok', key)], lambda d, key=key: summarize_series(((d.get('StatisticSearch') or {}).get('row')) or [], unit_hint='yield_pct' if key != 'usdkrw_bok' else None))
        for key in BOK_SERIES
    }
    legs['rates_and_fx'] = rates
    try:
        fx = fx_loader() if fx_loader else None
    except Exception as exc:
        fx = {'status': 'error', 'error': type(exc).__name__}
    legs['fx_snapshot'] = fx or {'status': 'unavailable'}

    try:
        rows = breadth_loader(market_date)
        legs['stored_breadth'] = (
            {'status': 'ok', 'data': aggregate_breadth(rows), 'source_rows': len(rows)} if rows else {'status': 'unavailable', 'reason': 'no stored risk snapshots for the date'}
        )
        legs['breadth_history'] = breadth_history(market_date, loader=breadth_loader, is_trading_day=is_trading_day)
    except Exception as exc:
        legs['stored_breadth'] = {'status': 'error', 'error': f'{type(exc).__name__}: {str(exc)[:120]}'}
        legs['breadth_history'] = []

    legs['event_calendar'] = {'status': 'not_available', 'note': 'No economic or earnings calendar source is wired; treat event risk as unknown.'}
    return {'task': 'market_context', 'market_date': market_date.isoformat(), 'collected_at_kst': now_kst.isoformat(), 'live_legs_collected': live, 'inputs': legs}
