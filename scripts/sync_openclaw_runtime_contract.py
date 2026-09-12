#!/usr/bin/env python3
"""Check or install versioned OpenClaw runtime contracts safely."""

from __future__ import annotations

import argparse
import hashlib
import os
import secrets
import stat
from pathlib import Path


CONTRACT_VERSION = "v1"
AGENT_WORKSPACES = {
    "short-term-decider": "workspace-short-term-decider",
    "signal-decider": "workspace-signal-decider",
}
REPO_ROOT = Path(__file__).resolve().parents[1]


class UnsafeTargetError(Exception):
    """Raised when AGENTS.md is not a stable regular-file target."""


class UnsafeWorkspaceError(Exception):
    """Raised when the configured workspace is not a stable real directory."""


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _print_status(
    *,
    agent: str,
    expected_sha256: str,
    actual_sha256: str,
    status: str,
) -> None:
    print(
        f"agent={agent} version={CONTRACT_VERSION} "
        f"expected_sha256={expected_sha256} actual_sha256={actual_sha256} "
        f"status={status}"
    )


def _open_workspace(workspace: Path) -> tuple[int, os.stat_result]:
    try:
        workspace_stat = workspace.lstat()
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise UnsafeWorkspaceError from exc
    if not stat.S_ISDIR(workspace_stat.st_mode):
        raise UnsafeWorkspaceError

    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        workspace_descriptor = os.open(workspace, flags)
    except OSError as exc:
        raise UnsafeWorkspaceError from exc
    opened_stat = os.fstat(workspace_descriptor)
    if (
        not stat.S_ISDIR(opened_stat.st_mode)
        or opened_stat.st_dev != workspace_stat.st_dev
        or opened_stat.st_ino != workspace_stat.st_ino
    ):
        os.close(workspace_descriptor)
        raise UnsafeWorkspaceError
    return workspace_descriptor, opened_stat


def _read_target(
    workspace_descriptor: int,
) -> tuple[bytes | None, int, os.stat_result | None]:
    try:
        target_stat = os.stat(
            "AGENTS.md",
            dir_fd=workspace_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None, 0o600, None
    if not stat.S_ISREG(target_stat.st_mode):
        raise UnsafeTargetError

    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        file_descriptor = os.open(
            "AGENTS.md",
            flags,
            dir_fd=workspace_descriptor,
        )
    except FileNotFoundError:
        return None, 0o600, None
    except OSError as exc:
        raise UnsafeTargetError from exc
    try:
        opened_stat = os.fstat(file_descriptor)
        if (
            not stat.S_ISREG(opened_stat.st_mode)
            or opened_stat.st_dev != target_stat.st_dev
            or opened_stat.st_ino != target_stat.st_ino
        ):
            raise UnsafeTargetError
        with os.fdopen(file_descriptor, "rb") as target_file:
            file_descriptor = -1
            content = target_file.read()
            final_stat = os.fstat(target_file.fileno())
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)
    if (
        final_stat.st_size != opened_stat.st_size
        or final_stat.st_mtime_ns != opened_stat.st_mtime_ns
    ):
        raise UnsafeTargetError
    return content, stat.S_IMODE(final_stat.st_mode), final_stat


def _assert_workspace_identity(
    workspace: Path,
    expected_stat: os.stat_result,
) -> None:
    try:
        current_stat = workspace.lstat()
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise UnsafeWorkspaceError from exc
    if (
        not stat.S_ISDIR(current_stat.st_mode)
        or current_stat.st_dev != expected_stat.st_dev
        or current_stat.st_ino != expected_stat.st_ino
    ):
        raise UnsafeWorkspaceError


def _assert_target_identity(
    workspace_descriptor: int,
    expected_stat: os.stat_result | None,
) -> None:
    try:
        current_stat = os.stat(
            "AGENTS.md",
            dir_fd=workspace_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        current_stat = None
    if expected_stat is None:
        if current_stat is not None:
            raise UnsafeTargetError
        return
    if current_stat is None or not stat.S_ISREG(current_stat.st_mode):
        raise UnsafeTargetError
    if (
        current_stat.st_dev != expected_stat.st_dev
        or current_stat.st_ino != expected_stat.st_ino
        or current_stat.st_size != expected_stat.st_size
        or current_stat.st_mtime_ns != expected_stat.st_mtime_ns
    ):
        raise UnsafeTargetError


def _atomic_install(
    *,
    workspace: Path,
    workspace_descriptor: int,
    workspace_stat: os.stat_result,
    target_stat: os.stat_result | None,
    content: bytes,
    target_mode: int,
) -> None:
    temporary_name = f".AGENTS.md.{secrets.token_hex(12)}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    file_descriptor = os.open(
        temporary_name,
        flags,
        0o600,
        dir_fd=workspace_descriptor,
    )
    try:
        os.fchmod(file_descriptor, target_mode)
        with os.fdopen(file_descriptor, "wb") as temporary_file:
            file_descriptor = -1
            temporary_file.write(content)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        _assert_workspace_identity(workspace, workspace_stat)
        _assert_target_identity(workspace_descriptor, target_stat)
        os.replace(
            temporary_name,
            "AGENTS.md",
            src_dir_fd=workspace_descriptor,
            dst_dir_fd=workspace_descriptor,
        )
        os.fsync(workspace_descriptor)
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)
        try:
            os.unlink(temporary_name, dir_fd=workspace_descriptor)
        except FileNotFoundError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--install", action="store_true")
    parser.add_argument("--agent", required=True, choices=sorted(AGENT_WORKSPACES))
    parser.add_argument(
        "--openclaw-home",
        type=Path,
        default=Path.home() / ".openclaw",
    )
    args = parser.parse_args()

    template = (
        REPO_ROOT
        / "runtime_contracts"
        / "openclaw"
        / CONTRACT_VERSION
        / args.agent
        / "AGENTS.md"
    )
    expected = template.read_bytes()
    expected_sha256 = _sha256(expected)
    workspace = args.openclaw_home / AGENT_WORKSPACES[args.agent]
    try:
        workspace_descriptor, workspace_stat = _open_workspace(workspace)
    except UnsafeWorkspaceError:
        _print_status(
            agent=args.agent,
            expected_sha256=expected_sha256,
            actual_sha256="unavailable",
            status="unsafe_workspace",
        )
        return 2

    try:
        try:
            actual, target_mode, target_stat = _read_target(workspace_descriptor)
            matches = actual == expected
            if args.install and not matches:
                _atomic_install(
                    workspace=workspace,
                    workspace_descriptor=workspace_descriptor,
                    workspace_stat=workspace_stat,
                    target_stat=target_stat,
                    content=expected,
                    target_mode=target_mode,
                )
                actual = expected
                matches = True
                status = "installed"
            else:
                status = (
                    "match"
                    if matches
                    else "missing"
                    if actual is None
                    else "mismatch"
                )
        except UnsafeWorkspaceError:
            _print_status(
                agent=args.agent,
                expected_sha256=expected_sha256,
                actual_sha256="unavailable",
                status="unsafe_workspace",
            )
            return 2
        except UnsafeTargetError:
            _print_status(
                agent=args.agent,
                expected_sha256=expected_sha256,
                actual_sha256="unavailable",
                status="unsafe_target",
            )
            return 2
        except OSError:
            _print_status(
                agent=args.agent,
                expected_sha256=expected_sha256,
                actual_sha256="unavailable",
                status="io_error",
            )
            return 2
    finally:
        os.close(workspace_descriptor)

    _print_status(
        agent=args.agent,
        expected_sha256=expected_sha256,
        actual_sha256=_sha256(actual) if actual is not None else "missing",
        status=status,
    )
    return 0 if matches else 1


if __name__ == "__main__":
    raise SystemExit(main())
