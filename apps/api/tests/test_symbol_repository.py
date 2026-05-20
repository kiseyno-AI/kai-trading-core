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
            'market': 'KOSPI',
            'instrument_type': 'equity',
            'source': 'pykrx',
            'is_active': True,
            'last_refreshed_at': datetime(2026, 5, 19, 10, 0, 0),
        }
    ]
