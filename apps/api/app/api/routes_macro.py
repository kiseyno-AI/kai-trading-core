from typing import Optional

from fastapi import APIRouter, HTTPException

from app.integrations.bok.client import ECOSAPIError, ECOSClient, ECOSConfigurationError

router = APIRouter(prefix='/macro')
ecos_client = ECOSClient()


async def _fetch_ecos_series(
    *,
    stat_code: str,
    start_period: str,
    end_period: str,
    cycle: str = 'D',
    item_code_1: Optional[str] = None,
    item_code_2: Optional[str] = None,
    item_code_3: Optional[str] = None,
    item_code_4: Optional[str] = None,
    start_row: int = 1,
    end_row: int = 100,
    lang: str = 'kr',
) -> dict:
    try:
        return await ecos_client.fetch_series(
            stat_code=stat_code,
            cycle=cycle,
            start_period=start_period,
            end_period=end_period,
            item_code_1=item_code_1,
            item_code_2=item_code_2,
            item_code_3=item_code_3,
            item_code_4=item_code_4,
            start_row=start_row,
            end_row=end_row,
            lang=lang,
        )
    except ECOSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except ECOSAPIError as exc:
        raise HTTPException(status_code=502, detail=f'ECOS series fetch failed: {exc}') from exc


@router.get('/ecos/series', tags=['macro:ecos'])
async def get_ecos_series(
    stat_code: str,
    start_period: str,
    end_period: str,
    cycle: str = 'D',
    item_code_1: Optional[str] = None,
    item_code_2: Optional[str] = None,
    item_code_3: Optional[str] = None,
    item_code_4: Optional[str] = None,
    start_row: int = 1,
    end_row: int = 100,
    lang: str = 'kr',
) -> dict:
    return await _fetch_ecos_series(
        stat_code=stat_code,
        start_period=start_period,
        end_period=end_period,
        cycle=cycle,
        item_code_1=item_code_1,
        item_code_2=item_code_2,
        item_code_3=item_code_3,
        item_code_4=item_code_4,
        start_row=start_row,
        end_row=end_row,
        lang=lang,
    )


@router.get('/bok/series', tags=['macro:bok'])
async def get_bok_series(
    series_id: str,
    start_period: str,
    end_period: str,
    cycle: str = 'D',
    item_code_1: Optional[str] = None,
    item_code_2: Optional[str] = None,
    item_code_3: Optional[str] = None,
    item_code_4: Optional[str] = None,
    start_row: int = 1,
    end_row: int = 100,
    lang: str = 'kr',
) -> dict:
    return await _fetch_ecos_series(
        stat_code=series_id,
        start_period=start_period,
        end_period=end_period,
        cycle=cycle,
        item_code_1=item_code_1,
        item_code_2=item_code_2,
        item_code_3=item_code_3,
        item_code_4=item_code_4,
        start_row=start_row,
        end_row=end_row,
        lang=lang,
    )
