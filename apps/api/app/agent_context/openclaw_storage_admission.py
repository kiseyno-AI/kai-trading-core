from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any


def _nearest_existing_path(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate.parent != candidate:
        candidate = candidate.parent
    return candidate


def inspect_openclaw_storage_admission(
    *,
    state_dir: str,
    min_free_bytes: int,
) -> dict[str, Any]:
    """Return a fail-closed admission decision for new OpenClaw work.

    The check is intentionally read-only. Cleanup remains the responsibility of
    the durable session-cleanup queue; this guard never removes OpenClaw state.
    """

    required_free_bytes = max(0, int(min_free_bytes))
    configured_path = Path(state_dir).expanduser()
    probe_path = _nearest_existing_path(configured_path)
    base = {
        'state_dir': str(configured_path),
        'probe_path': str(probe_path),
        'min_free_bytes': required_free_bytes,
    }
    if required_free_bytes == 0:
        return {
            **base,
            'admitted': True,
            'status': 'disabled',
            'reason': 'minimum_free_bytes_disabled',
            'free_bytes': None,
            'shortfall_bytes': 0,
        }

    try:
        usage = shutil.disk_usage(probe_path)
    except OSError as exc:
        return {
            **base,
            'admitted': False,
            'status': 'blocked',
            'reason': 'disk_usage_unavailable',
            'free_bytes': None,
            'shortfall_bytes': None,
            'error_type': type(exc).__name__,
        }

    free_bytes = int(usage.free)
    admitted = free_bytes >= required_free_bytes
    return {
        **base,
        'admitted': admitted,
        'status': 'ready' if admitted else 'blocked',
        'reason': None if admitted else 'disk_free_below_minimum',
        'free_bytes': free_bytes,
        'shortfall_bytes': max(required_free_bytes - free_bytes, 0),
    }
