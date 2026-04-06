from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.fx.service import KoreaEximInvalidSearchDateError, KoreaEximRateNotFoundError, fx_service
from app.integrations.fx.client import KoreaEximAPIError, KoreaEximConfigurationError

router = APIRouter(prefix='/fx/koreaexim', tags=['fx:koreaexim'])


@router.get('/rates')
async def get_exchange_rates(
    search_date: Optional[str] = Query(default=None, pattern=r'^\d{8}$'),
    currency: Optional[str] = Query(default=None, min_length=3, max_length=3),
    fallback_days: Optional[int] = Query(default=None, ge=0, le=10),
) -> dict:
    try:
        return await fx_service.get_exchange_rates(
            search_date=search_date,
            currency=currency,
            fallback_days=fallback_days,
        )
    except KoreaEximInvalidSearchDateError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KoreaEximConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except KoreaEximRateNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except KoreaEximAPIError as exc:
        raise HTTPException(status_code=502, detail=f'Korea Exim request failed: {exc}') from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Korea Exim FX service failed: {exc}') from exc
