from __future__ import annotations

import pytest

from app.agent_context.signal_decider_loop_service import _iter_bulk_upsert_chunks


def test_iter_bulk_upsert_chunks_preserves_order_and_caps_chunk_size() -> None:
    items = [{'value': value} for value in range(7)]

    chunks = _iter_bulk_upsert_chunks(items, chunk_size=3)

    assert [len(chunk) for chunk in chunks] == [3, 3, 1]
    assert [item['value'] for chunk in chunks for item in chunk] == list(range(7))


def test_iter_bulk_upsert_chunks_rejects_invalid_chunk_size() -> None:
    with pytest.raises(ValueError, match='chunk_size'):
        _iter_bulk_upsert_chunks([{'value': 1}], chunk_size=0)
