from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from app.core.settings import settings


def default_output_dir() -> Path:
    configured = (settings.side_agents_output_dir or '').strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parents[4] / 'var' / 'side-agents'


class SideAgentStore:
    def __init__(self, root: Path | None = None) -> None:
        self.root = Path(root) if root is not None else default_output_dir()

    def _write_json(self, path: Path, value: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix='.tmp-', suffix='.json')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as handle:
                json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        return path

    def result_path(self, agent: str, unit: str) -> Path:
        return self.root / agent / f'{unit}.json'

    def write_result(self, agent: str, unit: str, record: dict[str, Any]) -> Path:
        return self._write_json(self.result_path(agent, unit), record)

    def write_rejected(self, agent: str, unit: str, record: dict[str, Any]) -> Path:
        return self._write_json(self.root / agent / 'rejected' / f'{unit}.json', record)

    def load_result(self, agent: str, unit: str) -> dict[str, Any] | None:
        path = self.result_path(agent, unit)
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            return None

    def _state_path(self, agent: str, key: str) -> Path:
        return self.root / 'state' / f'{agent}-{key}.json'

    def load_state(self, agent: str, key: str) -> dict[str, Any]:
        try:
            state = json.loads(self._state_path(agent, key).read_text(encoding='utf-8'))
        except (OSError, ValueError):
            state = {}
        state.setdefault('attempts', 0)
        state.setdefault('done_units', [])
        state.setdefault('last_error', None)
        state.setdefault('last_attempt_at', None)
        return state

    def save_state(self, agent: str, key: str, state: dict[str, Any]) -> None:
        self._write_json(self._state_path(agent, key), state)
