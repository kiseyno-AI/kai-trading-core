from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import engine
from app.db_models import Symbol
from app.repositories.symbol_repository import (
    SYMBOL_UNIVERSE_UPSERT_BATCH_SIZE,
    _build_symbol_universe_rows,
    _iter_symbol_universe_chunks,
    upsert_master_symbols,
    upsert_symbol_universe,
)


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


def test_symbol_universe_chunks_keep_large_upserts_below_driver_parameter_limit() -> None:
    rows = [{'symbol': f'US{index:05d}'} for index in range(4_945)]

    chunks = list(_iter_symbol_universe_chunks(rows))

    assert [len(chunk) for chunk in chunks] == [
        SYMBOL_UNIVERSE_UPSERT_BATCH_SIZE,
        SYMBOL_UNIVERSE_UPSERT_BATCH_SIZE,
        SYMBOL_UNIVERSE_UPSERT_BATCH_SIZE,
        SYMBOL_UNIVERSE_UPSERT_BATCH_SIZE,
        4_945 - (4 * SYMBOL_UNIVERSE_UPSERT_BATCH_SIZE),
    ]
    assert [row for chunk in chunks for row in chunk] == rows


def test_master_upsert_requests_existing_lifecycle_preservation() -> None:
    items = [SimpleNamespace(symbol='005930', market='KOSPI', name_ko='삼성전자')]

    with patch('app.repositories.symbol_repository.upsert_symbol_universe', return_value=1) as upsert:
        result = upsert_master_symbols(items)

    assert result == 1
    upsert.assert_called_once_with(
        items,
        source='kis_master',
        preserve_existing_lifecycle=True,
    )


def test_preserved_lifecycle_keeps_external_state_and_reactivates_owned_rows() -> None:
    symbols = ['989971', '989972', '989973', '989974']
    external_refreshed_at = datetime(2026, 5, 1, tzinfo=timezone.utc)
    with engine.connect() as connection:
        transaction = connection.begin()
        session = Session(bind=connection)
        session.add_all(
            [
                Symbol(
                    symbol='989971',
                    market='KOSPI',
                    name_ko='PyKRX inactive',
                    instrument_type='equity',
                    source='pykrx',
                    source_payload={'provider': 'pykrx', 'as_of': '2026-05-01'},
                    is_active=False,
                    last_refreshed_at=external_refreshed_at,
                ),
                Symbol(
                    symbol='989973',
                    market='KOSPI',
                    name_ko='KIS inactive',
                    instrument_type='equity',
                    source='kis_master',
                    is_active=False,
                ),
                Symbol(
                    symbol='989974',
                    market='KOSPI',
                    name_ko='KIS-owned inactive',
                    instrument_type='equity',
                    source='kis_master',
                    is_active=False,
                ),
            ]
        )
        session.flush()

        @contextmanager
        def rollback_only_session_scope():
            yield session
            session.flush()

        try:
            with patch(
                'app.repositories.symbol_repository.session_scope',
                rollback_only_session_scope,
            ):
                upsert_symbol_universe(
                    [
                        SimpleNamespace(symbol='989971', market='KOSPI', name_ko='Master refreshed'),
                        SimpleNamespace(symbol='989972', market='KOSPI', name_ko='New listing'),
                        SimpleNamespace(symbol='989974', market='KOSPI', name_ko='KIS relisted'),
                    ],
                    source='kis_master',
                    stale_sources=[],
                    preserve_existing_lifecycle=True,
                )
                upsert_symbol_universe(
                    [SimpleNamespace(symbol='989973', market='KOSPI', name_ko='PyKRX refreshed')],
                    source='pykrx',
                    stale_sources=[],
                )

            session.expire_all()
            records = {
                record.symbol: record
                for record in session.execute(
                    select(Symbol).where(Symbol.symbol.in_(symbols))
                ).scalars()
            }

            assert records['989971'].name_ko == 'Master refreshed'
            assert records['989971'].source == 'pykrx'
            assert records['989971'].is_active is False
            assert records['989971'].source_payload == {
                'provider': 'pykrx',
                'as_of': '2026-05-01',
            }
            assert records['989971'].last_refreshed_at == external_refreshed_at
            assert records['989972'].source == 'kis_master'
            assert records['989972'].is_active is True
            assert records['989973'].name_ko == 'PyKRX refreshed'
            assert records['989973'].source == 'pykrx'
            assert records['989973'].is_active is True
            assert records['989974'].name_ko == 'KIS relisted'
            assert records['989974'].source == 'kis_master'
            assert records['989974'].is_active is True
        finally:
            session.close()
            transaction.rollback()
