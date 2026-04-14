from fastapi import APIRouter, HTTPException

from app.market_data_store import load_favorite_symbols, remove_favorite_symbol

router = APIRouter(prefix='/symbols/favorites', tags=['symbols:favorites'])


def _normalize_symbol_path(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError('symbol is required')
    return normalized


@router.get('')
async def get_favorite_symbols() -> dict:
    try:
        items = load_favorite_symbols()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Favorite symbol read failed: {exc}') from exc
    return {
        'count': len(items),
        'items': items,
    }


@router.delete('/{symbol}')
async def delete_favorite_symbol(symbol: str) -> dict:
    try:
        normalized_symbol = _normalize_symbol_path(symbol)
        removed = remove_favorite_symbol(normalized_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Favorite symbol delete failed: {exc}') from exc
    return {
        'symbol': normalized_symbol,
        'removed': removed,
    }
