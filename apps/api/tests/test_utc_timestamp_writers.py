from __future__ import annotations

import inspect
from datetime import timedelta

import pytest

from app import market_data_store
from app.eod import kis_valuation_backfill
from app.repositories import symbol_repository


UTC_WRITER_MODULES = (
    market_data_store,
    symbol_repository,
    kis_valuation_backfill,
)


@pytest.mark.parametrize(
    'writer_module',
    UTC_WRITER_MODULES,
    ids=lambda module: module.__name__,
)
def test_writer_module_clock_returns_aware_utc(writer_module) -> None:
    observed = writer_module._utcnow()

    assert observed.tzinfo is not None
    assert observed.utcoffset() == timedelta(0)


@pytest.mark.parametrize(
    'writer_module',
    UTC_WRITER_MODULES,
    ids=lambda module: module.__name__,
)
def test_writer_module_has_no_naive_utcnow_calls(writer_module) -> None:
    assert '.utcnow(' not in inspect.getsource(writer_module)
