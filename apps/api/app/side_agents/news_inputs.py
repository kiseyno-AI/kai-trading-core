"""Inputs for the news/event analyst: headlines, official DART filing titles and price context."""
from __future__ import annotations

import statistics
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Callable, Mapping, Sequence

HIGH_KEYWORDS = (
    '유상증자', '무상증자', '감자', '전환사채', '신주인수권', '교환사채', '합병', '분할', '영업양수', '영업양도',
    '최대주주', '횡령', '배임', '불성실공시', '상장폐지', '관리종목', '거래정지', '감사의견', '회생', '파산',
    '소송', '단일판매', '공급계약', '자기주식', '현금ㆍ현물배당', '배당', '잠정)실적', '매출액또는손익구조',
    '투자판단관련주요경영사항', '타법인주식및출자증권', '시설투자', '신규시설',
)
LOW_KEYWORDS = (
    '임원ㆍ주요주주특정증권등소유상황보고서', '증권발행실적보고서', '투자설명서', '일괄신고', '기업설명회',
    '주식등의대량보유상황보고서(약식)', '정기주주총회결과', '주주총회소집공고',
)
MEDIUM_KEYWORDS = ('대량보유', '정정', '사업보고서', '반기보고서', '분기보고서', '주주총회')

DART_SQL = """
select stock_code, rcept_no, report_nm, filing_category, filer_name, receipt_date, remark
from dart_disclosure_events
where stock_code = any(:codes) and receipt_date is not null and receipt_date >= :since
  and report_nm not like 'direct_backfill%'
order by receipt_date desc, id desc
"""
DART_RECENT_SQL = """
select stock_code, rcept_no, report_nm, filing_category, filer_name, receipt_date, remark
from dart_disclosure_events
where receipt_date is not null and receipt_date >= :since and report_nm not like 'direct_backfill%'
order by receipt_date desc, id desc
limit 20000
"""
PRICE_SQL = """
select s.symbol, b.trade_date, b.close_price, b.volume
from daily_price_bars b join symbols s on s.id = b.symbol_id
where s.symbol = any(:codes) and b.trade_date <= :d and b.trade_date >= :since
order by s.symbol, b.trade_date
"""
NAME_SQL = "select symbol, name_ko from symbols where symbol = any(:codes)"


def classify_filing(report_name: str | None) -> str:
    name = (report_name or '').replace(' ', '')
    if not name:
        return 'low'
    if any(k in name for k in LOW_KEYWORDS):
        return 'low'
    if any(k in name for k in HIGH_KEYWORDS):
        return 'high'
    if any(k in name for k in MEDIUM_KEYWORDS):
        return 'medium'
    return 'medium'


def trim_filing(row: Mapping[str, Any]) -> dict[str, Any]:
    receipt = row.get('receipt_date')
    return {
        'receipt_no': row.get('rcept_no'),
        'title': str(row.get('report_nm') or '')[:200],
        'received': receipt.isoformat() if hasattr(receipt, 'isoformat') else receipt,
        'category': row.get('filing_category'),
        'filer': row.get('filer_name'),
        'importance': classify_filing(row.get('report_nm')),
        'source_tier': 'official',
        'content_basis': 'title_only',
    }


def group_filings(rows: Sequence[Mapping[str, Any]], *, per_symbol: int = 5) -> dict[str, list[dict[str, Any]]]:
    order = {'high': 0, 'medium': 1, 'low': 2}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        code = row.get('stock_code')
        if code:
            grouped[str(code)].append(trim_filing(row))
    for code, items in grouped.items():
        items.sort(key=lambda f: f['received'] or '', reverse=True)  # newest first ...
        items.sort(key=lambda f: order[f['importance']])  # ... within each importance (stable)
        grouped[code] = [f for f in items if f['importance'] != 'low'][:per_symbol] or items[:1]
    return dict(grouped)


def compute_price_context(bars: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """bars: ascending by trade_date with close_price and volume."""
    closes = [(b['trade_date'], float(b['close_price'])) for b in bars if b.get('close_price')]
    if len(closes) < 2:
        return {'status': 'insufficient_history'}
    last_date, last = closes[-1]
    prev = closes[-2][1]
    out: dict[str, Any] = {
        'as_of': last_date.isoformat() if hasattr(last_date, 'isoformat') else str(last_date),
        'move_1d_pct': round((last / prev - 1) * 100, 3),
    }
    if len(closes) >= 6:
        out['move_5d_pct'] = round((last / closes[-6][1] - 1) * 100, 3)
    volumes = [float(b['volume']) for b in bars if b.get('volume') is not None]
    if len(volumes) >= 21:
        base = statistics.fmean(volumes[-21:-1])
        if base > 0:
            out['volume_ratio_20d'] = round(volumes[-1] / base, 3)
    return out


def select_symbols(
    news_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    filings_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    max_symbols: int,
) -> list[str]:
    """Interleave the best filing-driven and news-driven symbols so neither source crowds out the other."""
    every = set(news_by_symbol) | set(filings_by_symbol)

    def filing_rank(symbol: str) -> tuple[int, int, str]:
        filings = filings_by_symbol.get(symbol, [])
        return (-sum(f['importance'] == 'high' for f in filings), -sum(f['importance'] == 'medium' for f in filings), symbol)

    def news_rank(symbol: str) -> tuple[int, int, str]:
        news = news_by_symbol.get(symbol, [])
        return (-sum(n.get('materiality') == 'material' for n in news), -len(news), symbol)

    by_filing = [s for s in sorted(every, key=filing_rank) if filings_by_symbol.get(s) and any(f['importance'] == 'high' for f in filings_by_symbol[s])]
    by_news = [s for s in sorted(news_by_symbol, key=news_rank)]
    chosen: list[str] = []
    for pair in zip(by_news + [None] * len(by_filing), by_filing + [None] * len(by_news)):
        for symbol in pair:
            if symbol is not None and symbol not in chosen:
                chosen.append(symbol)
            if len(chosen) >= max_symbols:
                return chosen
    for symbol in sorted(every, key=filing_rank):
        if symbol not in chosen and len(chosen) < max_symbols:
            chosen.append(symbol)
    return chosen


def load_dart_filings(since: date, *, session_scope: Callable[..., Any] | None = None, codes: Sequence[str] | None = None) -> list[dict[str, Any]]:
    from sqlalchemy import text

    if session_scope is None:
        from app.db import session_scope as default_scope

        session_scope = default_scope
    with session_scope() as session:
        session.execute(text("SET LOCAL statement_timeout = '60s'"))
        if codes is None:
            rows = session.execute(text(DART_RECENT_SQL), {'since': since}).mappings().all()
        else:
            rows = session.execute(text(DART_SQL), {'codes': list(codes), 'since': since}).mappings().all()
    return [dict(r) for r in rows]


def load_price_contexts(codes: Sequence[str], market_date: date, *, session_scope: Callable[..., Any] | None = None) -> dict[str, dict[str, Any]]:
    from sqlalchemy import text

    if not codes:
        return {}
    if session_scope is None:
        from app.db import session_scope as default_scope

        session_scope = default_scope
    with session_scope() as session:
        session.execute(text("SET LOCAL statement_timeout = '60s'"))
        rows = session.execute(text(PRICE_SQL), {'codes': list(codes), 'd': market_date, 'since': market_date - timedelta(days=45)}).mappings().all()
    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_symbol[row['symbol']].append(dict(row))
    return {code: compute_price_context(by_symbol.get(code, [])) for code in codes}


def load_names(codes: Sequence[str], *, session_scope: Callable[..., Any] | None = None) -> dict[str, str]:
    from sqlalchemy import text

    if not codes:
        return {}
    if session_scope is None:
        from app.db import session_scope as default_scope

        session_scope = default_scope
    with session_scope() as session:
        rows = session.execute(text(NAME_SQL), {'codes': list(codes)}).all()
    return {r[0]: r[1] for r in rows}
