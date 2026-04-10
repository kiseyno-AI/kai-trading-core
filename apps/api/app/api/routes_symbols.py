from datetime import date

from fastapi import APIRouter, HTTPException, Query

from app.eod.master import get_master_symbols
from app.market_data_store import load_daily_bars_for_symbol_range

router = APIRouter(prefix='/symbols', tags=['symbols'])


@router.get('/search')
async def search_symbols(q: str = Query('', min_length=0), limit: int = Query(20, ge=1, le=100)) -> dict:
    query = q.strip().lower()
    symbols = get_master_symbols()

    if not query:
        items = symbols[:limit]
    else:
        matched = []
        for item in symbols:
            if query in item.symbol.lower() or query in item.name_ko.lower():
                matched.append(item)
            if len(matched) >= limit:
                break
        items = matched

    return {
        'query': q,
        'count': len(items),
        'items': [
            {
                'symbol': item.symbol,
                'standard_code': item.standard_code,
                'name_ko': item.name_ko,
                'market': item.market,
            }
            for item in items
        ],
    }


@router.get('/{symbol}/price-bars')
async def read_symbol_price_bars(
    symbol: str,
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=10000),
) -> dict:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail='start_date must be on or before end_date')

    items = load_daily_bars_for_symbol_range(
        symbol,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )

    return {
        'symbol': symbol,
        'count': len(items),
        'start_date': items[0]['trade_date'] if items else (start_date.isoformat() if start_date else None),
        'end_date': items[-1]['trade_date'] if items else (end_date.isoformat() if end_date else None),
        'requested_start_date': start_date.isoformat() if start_date else None,
        'requested_end_date': end_date.isoformat() if end_date else None,
        'items': items,
    }
