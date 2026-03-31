from fastapi import APIRouter, Query

from app.eod.master import get_master_symbols

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
