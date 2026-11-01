from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping


def canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(payload).hexdigest()


def load_json_mapping(path: str | Path, *, label: str) -> dict[str, Any]:
    resolved = Path(path)
    try:
        payload = json.loads(resolved.read_text(encoding='utf-8'))
    except FileNotFoundError as exc:
        raise ValueError(f'{label} file does not exist: {resolved}') from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f'{label} is not valid JSON: {resolved}') from exc
    if not isinstance(payload, dict):
        raise ValueError(f'{label} must be a JSON object')
    return payload


def load_checkpoint(
    path: str | Path,
    *,
    schema_version: str,
    manifest_sha256: str,
    item_count: int,
    unit_size: int,
) -> dict[str, Any]:
    resolved = Path(path)
    if not resolved.exists():
        return {
            'schema_version': schema_version,
            'manifest_sha256': manifest_sha256,
            'item_count': item_count,
            'unit_size': unit_size,
            'units': {},
        }
    checkpoint = load_json_mapping(resolved, label='checkpoint')
    for count_field in ('item_count', 'unit_size'):
        count_value = checkpoint.get(count_field)
        if isinstance(count_value, bool) or not isinstance(count_value, int):
            raise ValueError(
                f'checkpoint {count_field} must be an integer'
            )
    expected = {
        'schema_version': schema_version,
        'manifest_sha256': manifest_sha256,
        'item_count': item_count,
        'unit_size': unit_size,
    }
    mismatches = [
        field
        for field, expected_value in expected.items()
        if checkpoint.get(field) != expected_value
    ]
    if mismatches:
        raise ValueError(
            'checkpoint manifest contract mismatch: ' + ','.join(mismatches)
        )
    units = checkpoint.get('units')
    if not isinstance(units, dict):
        raise ValueError('checkpoint units must be a JSON object')
    return checkpoint


def write_checkpoint(path: str | Path, checkpoint: Mapping[str, Any]) -> None:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_name(f'.{resolved.name}.{os.getpid()}.tmp')
    try:
        payload = (
            json.dumps(
                dict(checkpoint),
                ensure_ascii=False,
                sort_keys=True,
                separators=(',', ':'),
            )
            + '\n'
        )
        with temporary.open('w', encoding='utf-8') as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, resolved)
        directory_fd = os.open(resolved.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
