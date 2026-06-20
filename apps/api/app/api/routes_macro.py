from __future__ import annotations

import asyncio
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.integrations.bok.client import ECOSAPIError, ECOSClient, ECOSConfigurationError
from app.integrations.bok.series_catalog import list_series_aliases, resolve_series_alias
from app.macro_context.service import macro_context_service

router = APIRouter(prefix='/macro')
ecos_client = ECOSClient()


async def _fetch_ecos_series(
    *,
    stat_code: Optional[str],
    start_period: str,
    end_period: str,
    cycle: Optional[str] = None,
    series_alias: Optional[str] = None,
    item_code_1: Optional[str] = None,
    item_code_2: Optional[str] = None,
    item_code_3: Optional[str] = None,
    item_code_4: Optional[str] = None,
    start_row: int = 1,
    end_row: int = 100,
    lang: str = 'kr',
) -> dict:
    try:
        resolved_request = _resolve_series_request(
            stat_code=stat_code,
            cycle=cycle,
            series_alias=series_alias,
            item_code_1=item_code_1,
            item_code_2=item_code_2,
            item_code_3=item_code_3,
            item_code_4=item_code_4,
        )
        payload = await ecos_client.fetch_series(
            stat_code=resolved_request['stat_code'],
            cycle=resolved_request['cycle'],
            start_period=start_period,
            end_period=end_period,
            item_code_1=resolved_request['item_code_1'],
            item_code_2=resolved_request['item_code_2'],
            item_code_3=resolved_request['item_code_3'],
            item_code_4=resolved_request['item_code_4'],
            start_row=start_row,
            end_row=end_row,
            lang=lang,
        )
        return _normalize_ecos_response(
            payload,
            resolved_request={
                **resolved_request,
                'start_period': start_period,
                'end_period': end_period,
                'start_row': start_row,
                'end_row': end_row,
                'lang': lang,
            },
        )
    except ECOSConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except ECOSAPIError as exc:
        raise HTTPException(status_code=502, detail=f'ECOS series fetch failed: {exc}') from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get('/ecos/series', tags=['macro:ecos'])
async def get_ecos_series(
    stat_code: Optional[str] = None,
    start_period: str = Query(...),
    end_period: str = Query(...),
    cycle: Optional[str] = None,
    series_alias: Optional[str] = None,
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
        series_alias=series_alias,
        item_code_1=item_code_1,
        item_code_2=item_code_2,
        item_code_3=item_code_3,
        item_code_4=item_code_4,
        start_row=start_row,
        end_row=end_row,
        lang=lang,
    )


@router.get('/ecos/catalog', tags=['macro:ecos'])
async def get_ecos_catalog() -> dict:
    aliases = list_series_aliases()
    return {
        'aliases': aliases,
        'count': len(aliases),
    }


@router.get('/bok/series', tags=['macro:bok'])
async def get_bok_series(
    series_id: Optional[str] = None,
    start_period: str = Query(...),
    end_period: str = Query(...),
    cycle: Optional[str] = None,
    series_alias: Optional[str] = None,
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
        series_alias=series_alias,
        item_code_1=item_code_1,
        item_code_2=item_code_2,
        item_code_3=item_code_3,
        item_code_4=item_code_4,
        start_row=start_row,
        end_row=end_row,
        lang=lang,
    )


@router.get('/context', tags=['macro:context'])
async def get_macro_context(
    as_of_date: Optional[str] = None,
) -> dict:
    try:
        return await asyncio.to_thread(
            macro_context_service.get_context,
            as_of_date=as_of_date,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/context/refresh', tags=['macro:context'])
async def refresh_macro_context(force: bool = False) -> dict:
    try:
        return await asyncio.to_thread(
            macro_context_service.refresh_context,
            force=force,
            trigger='manual_api',
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _resolve_series_request(
    *,
    stat_code: Optional[str],
    cycle: Optional[str],
    series_alias: Optional[str],
    item_code_1: Optional[str],
    item_code_2: Optional[str],
    item_code_3: Optional[str],
    item_code_4: Optional[str],
) -> dict:
    alias_config = resolve_series_alias(series_alias)
    if alias_config is None and series_alias:
        raise ValueError(f'Unknown series_alias: {series_alias}')

    resolved_stat_code = stat_code or (alias_config or {}).get('stat_code')
    if not resolved_stat_code:
        raise ValueError('One of stat_code/series_id or series_alias is required')

    return {
        'series_alias': (alias_config or {}).get('alias'),
        'stat_code': resolved_stat_code,
        'cycle': cycle or (alias_config or {}).get('cycle') or 'D',
        'item_code_1': item_code_1 if item_code_1 is not None else (alias_config or {}).get('item_code_1'),
        'item_code_2': item_code_2 if item_code_2 is not None else (alias_config or {}).get('item_code_2'),
        'item_code_3': item_code_3 if item_code_3 is not None else (alias_config or {}).get('item_code_3'),
        'item_code_4': item_code_4 if item_code_4 is not None else (alias_config or {}).get('item_code_4'),
    }


def _normalize_ecos_response(payload: dict, *, resolved_request: dict) -> dict:
    body = payload.get('StatisticSearch') or {}
    rows = body.get('row') or []

    observations = []
    for row in rows:
        observations.append(
            {
                'period': row.get('TIME'),
                'value': _parse_numeric_value(row.get('DATA_VALUE')),
                'value_text': row.get('DATA_VALUE'),
                'unit_name': row.get('UNIT_NAME'),
                'item_codes': {
                    'item_code_1': row.get('ITEM_CODE1'),
                    'item_code_2': row.get('ITEM_CODE2'),
                    'item_code_3': row.get('ITEM_CODE3'),
                    'item_code_4': row.get('ITEM_CODE4'),
                },
                'item_names': {
                    'item_name_1': row.get('ITEM_NAME1'),
                    'item_name_2': row.get('ITEM_NAME2'),
                    'item_name_3': row.get('ITEM_NAME3'),
                    'item_name_4': row.get('ITEM_NAME4'),
                },
            }
        )

    first_row = rows[0] if rows else {}
    return {
        'request': resolved_request,
        'series': {
            'stat_code': first_row.get('STAT_CODE', resolved_request['stat_code']),
            'stat_name': first_row.get('STAT_NAME'),
            'cycle': first_row.get('CYCLE', resolved_request['cycle']),
            'unit_name': first_row.get('UNIT_NAME'),
            'item_codes': {
                'item_code_1': first_row.get('ITEM_CODE1', resolved_request.get('item_code_1')),
                'item_code_2': first_row.get('ITEM_CODE2', resolved_request.get('item_code_2')),
                'item_code_3': first_row.get('ITEM_CODE3', resolved_request.get('item_code_3')),
                'item_code_4': first_row.get('ITEM_CODE4', resolved_request.get('item_code_4')),
            },
            'item_names': {
                'item_name_1': first_row.get('ITEM_NAME1'),
                'item_name_2': first_row.get('ITEM_NAME2'),
                'item_name_3': first_row.get('ITEM_NAME3'),
                'item_name_4': first_row.get('ITEM_NAME4'),
            },
        },
        'observation_count': len(observations),
        'observations': observations,
        'StatisticSearch': body,
    }


def _parse_numeric_value(value: Optional[str]) -> float | None:
    if value in (None, ''):
        return None
    try:
        return float(value)
    except ValueError:
        return None
