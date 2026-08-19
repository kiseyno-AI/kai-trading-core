from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pandas as pd

from app.repositories.symbol_repository import _build_symbol_universe_rows


def test_build_symbol_universe_rows_sanitizes_non_scalar_fields() -> None:
    rows, valid_items = _build_symbol_universe_rows(
        [
            SimpleNamespace(
                symbol='5930',
                market='KOSPI',
                name_ko=pd.DataFrame({'unexpected': ['html']}),
                standard_code=pd.DataFrame({'unexpected': ['html']}),
            ),
            SimpleNamespace(
                symbol=pd.DataFrame({'bad': ['symbol']}),
                market='KOSPI',
                name_ko='bad',
                standard_code=None,
            ),
        ],
        source='pykrx',
        instrument_type='equity',
        refreshed_at=datetime(2026, 5, 19, 10, 0, 0),
    )

    assert len(valid_items) == 1
    assert rows == [
        {
            'symbol': '005930',
            'standard_code': None,
            'name_ko': '005930',
            'name_en': None,
            'market': 'KOSPI',
            'country_code': None,
            'currency_code': None,
            'exchange_code': None,
            'regulatory_id': None,
            'source_payload': {},
            'instrument_type': 'equity',
            'source': 'pykrx',
            'is_active': True,
            'last_refreshed_at': datetime(2026, 5, 19, 10, 0, 0),
        }
    ]


def test_build_symbol_universe_rows_preserves_us_market_metadata() -> None:
    rows, valid_items = _build_symbol_universe_rows(
        [
            SimpleNamespace(
                symbol='brk.b',
                market='NYSE',
                name_ko='버크셔 해서웨이',
                name_en='Berkshire Hathaway Inc.',
                standard_code='BRK/B',
                country_code='US',
                currency_code='USD',
                exchange_code='NYSE',
                regulatory_id='0001067983',
                source_payload={'kis_symbol': 'BRK/B'},
            )
        ],
        source='us_integrated_master',
        instrument_type='equity',
        refreshed_at=datetime(2026, 8, 19, 10, 0, 0),
    )

    assert len(valid_items) == 1
    assert rows[0]['symbol'] == 'BRK.B'
    assert rows[0]['standard_code'] == 'BRK/B'
    assert rows[0]['name_en'] == 'Berkshire Hathaway Inc.'
    assert rows[0]['country_code'] == 'US'
    assert rows[0]['currency_code'] == 'USD'
    assert rows[0]['exchange_code'] == 'NYSE'
    assert rows[0]['regulatory_id'] == '0001067983'
    assert rows[0]['source_payload'] == {'kis_symbol': 'BRK/B'}
