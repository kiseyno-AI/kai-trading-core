from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class SnapshotSink(Protocol):
    def persist(self, snapshot: dict) -> None:
        ...


@dataclass
class NullSnapshotSink:
    def persist(self, snapshot: dict) -> None:
        # TODO: replace with DB repository-backed sink when the market DB is ready.
        return None


snapshot_sink: SnapshotSink = NullSnapshotSink()
