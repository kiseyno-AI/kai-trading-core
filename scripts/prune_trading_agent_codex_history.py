#!/usr/bin/env python3
"""Offline prune of finished Codex thread history for the trading agents.

Why: Core dispatches every summarizer/decider/side-agent batch into a fresh
OpenClaw session, and each one leaves a native Codex thread (rollout files and
rows in ``codex-home/state_5.sqlite``) that is never resumed. The external
native-thread retention controller stopped deleting anything in late 2026-10
(``native_thread_attestation_invalid``: OpenClaw now redacts long ids in
``sessions.get`` output), so ``~/.openclaw/agents`` grew to 56 GB and free
space fell under Core's 20 GiB dispatch guard on 2026-11-07, which blocks
every agent call.

What it does (only with ``--apply``):
  1. refuses unless it is a KST weekend (or ``--force``), Core reports no
     active daily pipeline, and the gateway is reachable;
  2. stops the gateway (``openclaw gateway stop --force``) and waits until
     the port is closed and no process holds files in the Codex homes;
  3. deletes only history inside each trading agent's ``codex-home``:
     ``sessions/``, ``shell_snapshots/``, ``tmp/``, ``.tmp/``,
     ``thread-writer-locks/``, ``memories/``, and the ``state_5``,
     ``thread_history_1``, ``logs_2``, ``memories_1`` SQLite files;
     config, installation id, skills and caches stay, as do OpenClaw's own
     session database, auth profiles and every non-trading agent;
  4. starts the gateway and requires ``/health`` to answer 200.
Without ``--apply`` it only reports what it would delete.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

KST = ZoneInfo('Asia/Seoul')
TRADING_AGENTS = (
    'eod-summarizer',
    'technical-summarizer',
    'fundamental-summarizer',
    'signal-decider',
    'short-term-decider',
    'market-context-analyst',
    'news-event-analyst',
    'postmortem-analyst',
)
HISTORY_DIRS = ('sessions', 'shell_snapshots', 'tmp', '.tmp', 'thread-writer-locks', 'memories')
HISTORY_DB_PREFIXES = ('state_5.sqlite', 'thread_history_1.sqlite', 'logs_2.sqlite', 'memories_1.sqlite')
GATEWAY_PORT = 18789


def _size(path: Path) -> int:
    if path.is_file() or path.is_symlink():
        return path.lstat().st_size
    total = 0
    for child in path.rglob('*'):
        try:
            if child.is_file() and not child.is_symlink():
                total += child.lstat().st_size
        except OSError:
            continue
    return total


def history_targets(agents_root: Path) -> list[Path]:
    targets: list[Path] = []
    for agent in TRADING_AGENTS:
        home = agents_root / agent / 'agent' / 'codex-home'
        if not home.is_dir() or home.is_symlink():
            continue
        targets.extend(home / name for name in HISTORY_DIRS if (home / name).exists())
        targets.extend(
            child
            for child in sorted(home.iterdir())
            if child.is_file() and child.name.startswith(HISTORY_DB_PREFIXES)
        )
    return targets


def _port_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1.0)
        return sock.connect_ex(('127.0.0.1', port)) == 0


def _http_status(url: str, timeout: float = 5.0) -> int | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310 - local only
            return response.status
    except Exception:  # noqa: BLE001
        return None


def _core_pipeline_active(core_url: str, token: str | None) -> bool | None:
    request = urllib.request.Request(f'{core_url}/eod/status')
    if token:
        request.add_header('Authorization', f'Bearer {token}')
    try:
        with urllib.request.urlopen(request, timeout=20) as response:  # noqa: S310 - local only
            payload = json.loads(response.read().decode('utf-8'))
    except Exception:  # noqa: BLE001
        return None
    auto = payload.get('pykrx_auto') if isinstance(payload, dict) else None
    return bool((auto or {}).get('active_task'))


def _files_open_under(paths: list[Path]) -> int:
    roots = sorted({str(p.parent) for p in paths})
    if not roots or not shutil.which('lsof'):
        return 0
    count = 0
    for root in roots:
        result = subprocess.run(['lsof', '+D', root], capture_output=True, text=True, check=False)
        count += max(0, len(result.stdout.splitlines()) - 1)
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--apply', action='store_true', help='actually stop the gateway and delete history')
    parser.add_argument('--force', action='store_true', help='allow outside a KST weekend')
    parser.add_argument('--agents-root', default=str(Path.home() / '.openclaw' / 'agents'))
    parser.add_argument('--openclaw-bin', default=shutil.which('openclaw') or '/opt/homebrew/bin/openclaw')
    parser.add_argument('--core-url', default='http://127.0.0.1:8000')
    parser.add_argument('--core-token', default=None)
    parser.add_argument(
        '--min-free-gib',
        type=float,
        default=40.0,
        help='only prune when free space is below this (default 40; Core blocks dispatch under 20)',
    )
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc).astimezone(KST)
    agents_root = Path(args.agents_root).expanduser()
    targets = history_targets(agents_root)
    planned = sum(_size(path) for path in targets)
    report: dict = {
        'at': now.isoformat(),
        'mode': 'apply' if args.apply else 'dry-run',
        'targets': len(targets),
        'planned_bytes': planned,
        'free_bytes_before': shutil.disk_usage(agents_root).free if agents_root.exists() else None,
    }
    if not args.apply:
        report['status'] = 'dry_run'
        print(json.dumps(report, ensure_ascii=False))
        return 0
    free_before = report['free_bytes_before'] or 0
    if free_before >= args.min_free_gib * (1024 ** 3) and not args.force:
        report.update(status='skipped', reason='enough_free_space')
        print(json.dumps(report, ensure_ascii=False))
        return 0
    if now.weekday() < 5 and not args.force:
        report.update(status='refused', reason='not_kst_weekend')
        print(json.dumps(report, ensure_ascii=False))
        return 2
    active = _core_pipeline_active(
        args.core_url, args.core_token or os.environ.get('UPSTREAM_BEARER_TOKEN') or None
    )
    if active is not False:
        report.update(status='refused', reason='core_pipeline_active' if active else 'core_status_unavailable')
        print(json.dumps(report, ensure_ascii=False))
        return 2
    if not targets:
        report.update(status='nothing_to_prune')
        print(json.dumps(report, ensure_ascii=False))
        return 0

    subprocess.run([args.openclaw_bin, 'gateway', 'stop', '--force'], capture_output=True, text=True, check=False)
    deadline = time.monotonic() + 90
    while _port_open(GATEWAY_PORT) and time.monotonic() < deadline:
        time.sleep(2)
    if _port_open(GATEWAY_PORT):
        report.update(status='failed', reason='gateway_did_not_stop')
        print(json.dumps(report, ensure_ascii=False))
        return 1
    try:
        open_files = _files_open_under(targets)
        if open_files:
            report.update(status='failed', reason='history_files_in_use', open_files=open_files)
            return 1
        deleted = 0
        for path in targets:
            size = _size(path)
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
            deleted += size
        report['deleted_bytes'] = deleted
    finally:
        subprocess.run([args.openclaw_bin, 'gateway', 'start'], capture_output=True, text=True, check=False)
        deadline = time.monotonic() + 120
        health = None
        while time.monotonic() < deadline:
            health = _http_status(f'http://127.0.0.1:{GATEWAY_PORT}/health')
            if health == 200:
                break
            time.sleep(3)
        report['gateway_health'] = health
        report['free_bytes_after'] = shutil.disk_usage(agents_root).free
        if report.get('status') is None:
            report['status'] = 'pruned' if health == 200 else 'pruned_gateway_unhealthy'
        print(json.dumps(report, ensure_ascii=False))
    return 0 if report.get('status') == 'pruned' else 1


if __name__ == '__main__':
    sys.exit(main())
