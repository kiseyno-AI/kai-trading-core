from __future__ import annotations

import pytest

import app.agent_context.signal_decider_loop_service as loop_service
from app.agent_context.signal_decider_loop_service import _iter_bulk_upsert_chunks


def test_iter_bulk_upsert_chunks_preserves_order_and_caps_chunk_size() -> None:
    items = [{'value': value} for value in range(7)]

    chunks = _iter_bulk_upsert_chunks(items, chunk_size=3)

    assert [len(chunk) for chunk in chunks] == [3, 3, 1]
    assert [item['value'] for chunk in chunks for item in chunk] == list(range(7))


def test_iter_bulk_upsert_chunks_rejects_invalid_chunk_size() -> None:
    with pytest.raises(ValueError, match='chunk_size'):
        _iter_bulk_upsert_chunks([{'value': 1}], chunk_size=0)


def test_iter_bulk_upsert_chunks_resolves_default_at_call_time(monkeypatch) -> None:
    monkeypatch.setattr(loop_service, 'SIGNAL_DECIDER_BULK_UPSERT_CHUNK_SIZE', 2)

    chunks = _iter_bulk_upsert_chunks([{'value': value} for value in range(5)])

    assert [len(chunk) for chunk in chunks] == [2, 2, 1]


def test_default_bulk_upsert_chunk_stays_small_for_json_heavy_rows() -> None:
    items = [{'value': value} for value in range(25)]

    chunks = _iter_bulk_upsert_chunks(items)

    assert loop_service.SIGNAL_DECIDER_BULK_UPSERT_CHUNK_SIZE == 10
    assert [len(chunk) for chunk in chunks] == [10, 10, 5]
