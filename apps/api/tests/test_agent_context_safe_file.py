from __future__ import annotations

import errno
import os
from pathlib import Path

import pytest

from app.agent_context import safe_file
from app.agent_context.safe_file import SafeFileReadError, read_bounded_regular_file


def _write_contract(root: Path, *, text: str = 'contract') -> Path:
    target = root / 'workspace-signal-decider' / 'AGENTS.md'
    target.parent.mkdir(parents=True)
    target.write_text(text, encoding='utf-8')
    return target


def test_safe_reader_reads_bounded_regular_file(tmp_path) -> None:
    target = _write_contract(tmp_path)

    snapshot = read_bounded_regular_file(target, max_bytes=1024)

    assert snapshot.content == b'contract'
    assert set(snapshot.fingerprint) == {'dev', 'ino', 'size', 'mtime_ns', 'ctime_ns'}
    assert len(snapshot.path_fingerprint) >= 2
    assert all(
        set(component) == {'dev', 'ino', 'mode', 'size', 'mtime_ns', 'ctime_ns'}
        for component in snapshot.path_fingerprint
    )


@pytest.mark.parametrize('symlink_component', ['state-root', 'workspace', 'target'])
def test_safe_reader_rejects_symlink_at_any_scoped_component(tmp_path, symlink_component) -> None:
    real_root = tmp_path / 'real-state'
    target = _write_contract(real_root)
    requested_target = target
    if symlink_component == 'state-root':
        linked_root = tmp_path / 'linked-state'
        linked_root.symlink_to(real_root, target_is_directory=True)
        requested_target = linked_root / 'workspace-signal-decider' / 'AGENTS.md'
    elif symlink_component == 'workspace':
        linked_root = tmp_path / 'linked-state'
        linked_root.mkdir()
        (linked_root / 'workspace-signal-decider').symlink_to(
            target.parent,
            target_is_directory=True,
        )
        requested_target = linked_root / 'workspace-signal-decider' / 'AGENTS.md'
    else:
        linked_root = tmp_path / 'linked-state'
        linked_workspace = linked_root / 'workspace-signal-decider'
        linked_workspace.mkdir(parents=True)
        requested_target = linked_workspace / 'AGENTS.md'
        requested_target.symlink_to(target)

    with pytest.raises(SafeFileReadError):
        read_bounded_regular_file(requested_target, max_bytes=1024)


def test_safe_reader_rejects_nonregular_target(tmp_path) -> None:
    target = tmp_path / 'workspace-signal-decider' / 'AGENTS.md'
    target.mkdir(parents=True)

    with pytest.raises(SafeFileReadError):
        read_bounded_regular_file(target, max_bytes=1024)


def test_safe_reader_fails_closed_on_io_error_without_content(monkeypatch, tmp_path) -> None:
    target = _write_contract(tmp_path, text='must-not-leak')

    def fail_read(*_args, **_kwargs):
        raise OSError(errno.EIO, 'synthetic read failure')

    monkeypatch.setattr(safe_file.os, 'read', fail_read)
    with pytest.raises(SafeFileReadError) as exc_info:
        read_bounded_regular_file(target, max_bytes=1024)

    assert 'must-not-leak' not in str(exc_info.value)


def test_safe_reader_closes_file_descriptor_when_file_fstat_fails(
    monkeypatch,
    tmp_path,
) -> None:
    target = _write_contract(tmp_path)
    original_open = safe_file.os.open
    original_fstat = safe_file.os.fstat
    original_close = safe_file.os.close
    opened_file_descriptor = None
    closed_descriptors: list[int] = []

    def tracking_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal opened_file_descriptor
        descriptor = original_open(path, flags, mode, dir_fd=dir_fd)
        if path == target.name:
            opened_file_descriptor = descriptor
        return descriptor

    def failing_fstat(descriptor):
        if descriptor == opened_file_descriptor:
            raise OSError(errno.EIO, 'synthetic file fstat failure')
        return original_fstat(descriptor)

    def tracking_close(descriptor):
        closed_descriptors.append(descriptor)
        return original_close(descriptor)

    monkeypatch.setattr(safe_file.os, 'open', tracking_open)
    monkeypatch.setattr(safe_file.os, 'fstat', failing_fstat)
    monkeypatch.setattr(safe_file.os, 'close', tracking_close)

    with pytest.raises(SafeFileReadError):
        read_bounded_regular_file(target, max_bytes=1024)

    assert opened_file_descriptor is not None
    assert opened_file_descriptor in closed_descriptors


def test_safe_reader_rejects_parent_identity_change_during_verification(monkeypatch, tmp_path) -> None:
    workspace_name = 'workspace-signal-decider'
    workspace = tmp_path / workspace_name
    target = _write_contract(tmp_path, text='original')
    displaced = tmp_path / 'workspace-displaced'
    original_open = safe_file.os.open
    workspace_open_count = 0

    def swapping_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal workspace_open_count
        if path == workspace_name:
            workspace_open_count += 1
            if workspace_open_count == 2:
                workspace.rename(displaced)
                workspace.mkdir()
                (workspace / 'AGENTS.md').write_text('replacement', encoding='utf-8')
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(safe_file.os, 'open', swapping_open)

    with pytest.raises(SafeFileReadError):
        read_bounded_regular_file(target, max_bytes=1024)


def test_safe_reader_rejects_oversized_target(tmp_path) -> None:
    target = _write_contract(tmp_path, text='x' * 32)

    with pytest.raises(SafeFileReadError):
        read_bounded_regular_file(target, max_bytes=16)


def test_safe_reader_path_fingerprint_changes_when_parent_directory_changes(tmp_path) -> None:
    target = _write_contract(tmp_path)
    before = read_bounded_regular_file(target, max_bytes=1024)

    marker = tmp_path / 'transient-marker'
    marker.write_text('x', encoding='utf-8')
    marker.unlink()
    after = read_bounded_regular_file(target, max_bytes=1024)

    assert before.content == after.content
    assert before.fingerprint == after.fingerprint
    assert before.path_fingerprint != after.path_fingerprint


def test_default_expanduser_state_root_symlink_is_rejected(monkeypatch, tmp_path) -> None:
    home = tmp_path / 'home'
    home.mkdir()
    real_state = tmp_path / 'real-state'
    target = _write_contract(real_state)
    (home / '.openclaw').symlink_to(real_state, target_is_directory=True)
    monkeypatch.setenv('HOME', str(home))

    with pytest.raises(SafeFileReadError):
        read_bounded_regular_file(
            Path('~/.openclaw/workspace-signal-decider/AGENTS.md').expanduser(),
            max_bytes=1024,
        )
