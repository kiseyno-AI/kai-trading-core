from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from app.market_data_store import persist_daily_price_bar


class SnapshotSink(Protocol):
    def persist(self, snapshot: dict, *, run_id: int | None = None) -> None:
        ...


@dataclass
class DBSnapshotSink:
    def persist(self, snapshot: dict, *, run_id: int | None = None) -> None:
        persist_daily_price_bar(snapshot, run_id=run_id)


snapshot_sink: SnapshotSink = DBSnapshotSink()
