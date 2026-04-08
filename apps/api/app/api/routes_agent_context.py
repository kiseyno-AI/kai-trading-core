from datetime import date

from fastapi import APIRouter, HTTPException, Query

from app.agent_context.read_service import agent_context_read_service

router = APIRouter(prefix='/agent-context', tags=['agent-context'])


@router.get('/symbol-day')
async def get_symbol_day_context(
    market_date: date = Query(...),
    symbols: list[str] = Query(...),
    fs_div: str | None = Query(default=None),
    disclosures_limit: int = Query(default=5, ge=0, le=20),
) -> dict:
    try:
        return agent_context_read_service.get_symbol_day_context(
            market_date=market_date,
            symbols=symbols,
            fs_div=fs_div,
            disclosures_limit=disclosures_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Agent context read failed: {exc}') from exc
