from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts" / "sync_openclaw_runtime_contract.py"
TEMPLATE = (
    REPO_ROOT
    / "runtime_contracts"
    / "openclaw"
    / "v1"
    / "signal-decider"
    / "AGENTS.md"
)


def test_default_check_is_read_only_and_reports_version_and_hash(tmp_path: Path) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-signal-decider" / "AGENTS.md"
    target.parent.mkdir(parents=True)
    content = TEMPLATE.read_bytes()
    target.write_bytes(content)
    before = target.stat()

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--openclaw-home",
            str(openclaw_home),
            "--agent",
            "signal-decider",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "agent=signal-decider" in result.stdout
    assert "version=v1" in result.stdout
    assert f"expected_sha256={hashlib.sha256(content).hexdigest()}" in result.stdout
    assert "status=match" in result.stdout
    assert target.read_bytes() == content
    assert target.stat().st_mtime_ns == before.st_mtime_ns


def test_explicit_check_supports_short_term_decider(tmp_path: Path) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-short-term-decider" / "AGENTS.md"
    target.parent.mkdir(parents=True)
    template = (
        REPO_ROOT
        / "runtime_contracts"
        / "openclaw"
        / "v1"
        / "short-term-decider"
        / "AGENTS.md"
    )
    target.write_bytes(template.read_bytes())

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--check",
            "--openclaw-home",
            str(openclaw_home),
            "--agent",
            "short-term-decider",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "agent=short-term-decider" in result.stdout
    assert "version=v1" in result.stdout
    assert "status=match" in result.stdout


@pytest.mark.parametrize("target_exists", [True, False])
def test_check_reports_drift_without_exposing_target_content(
    tmp_path: Path,
    target_exists: bool,
) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-signal-decider" / "AGENTS.md"
    target.parent.mkdir(parents=True)
    sensitive_drift = b"do-not-echo-this-runtime-content\n"
    if target_exists:
        target.write_bytes(sensitive_drift)

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--check",
            "--openclaw-home",
            str(openclaw_home),
            "--agent",
            "signal-decider",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "version=v1" in result.stdout
    assert "expected_sha256=" in result.stdout
    expected_status = "mismatch" if target_exists else "missing"
    expected_actual = (
        hashlib.sha256(sensitive_drift).hexdigest() if target_exists else "missing"
    )
    assert f"actual_sha256={expected_actual}" in result.stdout
    assert f"status={expected_status}" in result.stdout
    assert sensitive_drift.decode().strip() not in result.stdout


def test_install_atomically_replaces_target_and_preserves_mode(tmp_path: Path) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-signal-decider" / "AGENTS.md"
    target.parent.mkdir(parents=True)
    target.write_text("stale contract\n", encoding="utf-8")
    target.chmod(0o640)
    original_inode = target.stat().st_ino

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--install",
            "--openclaw-home",
            str(openclaw_home),
            "--agent",
            "signal-decider",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert target.read_bytes() == TEMPLATE.read_bytes()
    assert target.stat().st_ino != original_inode
    assert stat.S_IMODE(target.stat().st_mode) == 0o640
    assert os.listdir(target.parent) == ["AGENTS.md"]
    assert "agent=signal-decider" in result.stdout
    assert "version=v1" in result.stdout
    assert "status=installed" in result.stdout


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_symlink_target_fails_closed_without_mutation(
    tmp_path: Path,
    operation: str,
) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-signal-decider" / "AGENTS.md"
    target.parent.mkdir(parents=True)
    outside = tmp_path / "outside.md"
    outside.write_text("outside contract\n", encoding="utf-8")
    target.symlink_to(outside)

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            operation,
            "--openclaw-home",
            str(openclaw_home),
            "--agent",
            "signal-decider",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "status=unsafe_target" in result.stdout
    assert target.is_symlink()
    assert outside.read_text(encoding="utf-8") == "outside contract\n"


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_symlink_workspace_fails_closed_without_mutation(
    tmp_path: Path,
    operation: str,
) -> None:
    openclaw_home = tmp_path / ".openclaw"
    openclaw_home.mkdir()
    outside_workspace = tmp_path / "outside-workspace"
    outside_workspace.mkdir()
    outside_target = outside_workspace / "AGENTS.md"
    outside_target.write_text("outside contract\n", encoding="utf-8")
    (openclaw_home / "workspace-signal-decider").symlink_to(
        outside_workspace,
        target_is_directory=True,
    )

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            operation,
            "--openclaw-home",
            str(openclaw_home),
            "--agent",
            "signal-decider",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "status=unsafe_workspace" in result.stdout
    assert outside_target.read_text(encoding="utf-8") == "outside contract\n"


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_missing_workspace_fails_closed_without_creation(
    tmp_path: Path,
    operation: str,
) -> None:
    openclaw_home = tmp_path / ".openclaw"
    openclaw_home.mkdir()

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            operation,
            "--openclaw-home",
            str(openclaw_home),
            "--agent",
            "signal-decider",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "status=unsafe_workspace" in result.stdout
    assert list(openclaw_home.iterdir()) == []


def test_install_creates_only_agents_file_with_private_mode(tmp_path: Path) -> None:
    openclaw_home = tmp_path / ".openclaw"
    workspace = openclaw_home / "workspace-short-term-decider"
    workspace.mkdir(parents=True)
    adjacent = workspace / "runtime-state.json"
    adjacent.write_text('{"preserve": true}\n', encoding="utf-8")
    target = workspace / "AGENTS.md"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--install",
            "--openclaw-home",
            str(openclaw_home),
            "--agent",
            "short-term-decider",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert target.exists()
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert adjacent.read_text(encoding="utf-8") == '{"preserve": true}\n'
    assert sorted(path.name for path in workspace.iterdir()) == [
        "AGENTS.md",
        "runtime-state.json",
    ]


@pytest.mark.parametrize("operation", ["--check", "--install"])
def test_non_regular_target_fails_closed(tmp_path: Path, operation: str) -> None:
    openclaw_home = tmp_path / ".openclaw"
    target = openclaw_home / "workspace-signal-decider" / "AGENTS.md"
    target.mkdir(parents=True)

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            operation,
            "--openclaw-home",
            str(openclaw_home),
            "--agent",
            "signal-decider",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "status=unsafe_target" in result.stdout
    assert target.is_dir()


@pytest.mark.parametrize("agent", ["exec-agent", "../signal-decider"])
def test_unknown_agent_and_path_traversal_are_rejected(
    tmp_path: Path,
    agent: str,
) -> None:
    openclaw_home = tmp_path / ".openclaw"
    openclaw_home.mkdir()

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--install",
            "--openclaw-home",
            str(openclaw_home),
            "--agent",
            agent,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert list(openclaw_home.iterdir()) == []


def test_check_and_install_cannot_be_combined(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--check",
            "--install",
            "--openclaw-home",
            str(tmp_path),
            "--agent",
            "signal-decider",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
