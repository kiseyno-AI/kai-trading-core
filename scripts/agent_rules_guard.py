#!/usr/bin/env python3
"""Refuse automated agent-rule changes that go beyond one marked calibration block.

The improvement loop (docs/AGENT_IMPROVEMENT_LOOP.md) lets an AI propose wording for the
two decider contracts, but only inside

    <!-- BEGIN AUTO-CALIBRATION -->
    - plain bullet lines
    <!-- END AUTO-CALIBRATION -->

Everything else in the contract (the fail-closed rules, gate semantics, output schema)
must stay byte-identical. The only other file it may touch is the append-only
``agent_rules/CHANGELOG.md``. This file is deliberately dependency-free and is itself not
an allowed path, so the automation cannot edit its own guard.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from typing import Mapping

BEGIN = '<!-- BEGIN AUTO-CALIBRATION -->'
END = '<!-- END AUTO-CALIBRATION -->'
CHANGELOG = 'agent_rules/CHANGELOG.md'
CONTRACT_PATH = re.compile(r'^runtime_contracts/openclaw/v1/(signal-decider|short-term-decider)/AGENTS\.md$')

MAX_BLOCK_CHARS = 1500
MAX_BLOCK_LINES = 12
MAX_LINE_CHARS = 300

# Wording that could widen what an agent may do or exfiltrate something. A calibration note
# may only make an agent more careful or better informed.
FORBIDDEN = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r'\b(ignore|override|bypass|disable|skip|disregard)\b',
        r'\b(increase|raise|widen|expand|enlarge)\b.{0,40}\b(size|sizing|risk|leverage|budget|limit|exposure|urgency)',
        r'\b(relax|loosen|lower|weaken|soften)\b.{0,40}\b(gate|threshold|check|guard|rule)',
        r'guarantee',
        r'https?://',
        r'\b(reveal|exfiltrate)\b',
        r'credential|api[_ -]?key|password|secret|token',
        r'\b(always|regardless)\b.{0,30}\b(buy|approve|execute)',
        r'\b(buy|approve|execute)\b.{0,30}\b(always|regardless)',
    )
)
CHANGELOG_ENTRY = re.compile(r'report:\S+.*finding:\S+|finding:\S+.*report:\S+')


def _split(text: str) -> tuple[str, str, str] | None:
    if text.count(BEGIN) != 1 or text.count(END) != 1:
        return None
    start, end = text.index(BEGIN), text.index(END)
    if start > end:
        return None
    return text[:start], text[start + len(BEGIN) : end], text[end + len(END) :]


def _check_block(block: str, *, max_chars: int, max_lines: int) -> list[str]:
    violations: list[str] = []
    lines = [line for line in block.strip('\n').split('\n') if line.strip()]
    if len(block.strip()) > max_chars:
        violations.append(f'block_too_large: {len(block.strip())} > {max_chars} chars')
    if len(lines) > max_lines:
        violations.append(f'block_too_long: {len(lines)} > {max_lines} lines')
    for line in lines:
        if not line.startswith('- '):
            violations.append(f'not_a_bullet: {line[:60]!r}')
        if len(line) > MAX_LINE_CHARS:
            violations.append(f'line_too_long: {len(line)} > {MAX_LINE_CHARS} chars')
        for pattern in FORBIDDEN:
            if pattern.search(line):
                violations.append(f'forbidden_wording[{pattern.pattern[:30]}]: {line[:60]!r}')
    return violations


def validate(
    files: Mapping[str, tuple[str, str]],
    *,
    max_block_chars: int = MAX_BLOCK_CHARS,
    max_block_lines: int = MAX_BLOCK_LINES,
) -> list[str]:
    """``files`` maps each changed path to ``(base_text, head_text)``; returns violations."""

    violations: list[str] = []
    contract_changed = False
    for path, (base, head) in files.items():
        if path == CHANGELOG:
            if not head.startswith(base):
                violations.append('changelog_not_append_only')
            else:
                added = head[len(base):]
                lines = [line for line in added.split('\n') if line.strip()]
                if not lines or not all(CHANGELOG_ENTRY.search(line) for line in lines):
                    violations.append('changelog_entry must name report:<id> and finding:<id> on every added line')
            continue
        if not CONTRACT_PATH.match(path):
            violations.append(f'path_not_allowed: {path}')
            continue
        if base == head:
            continue
        contract_changed = True
        base_parts, head_parts = _split(base), _split(head)
        if base_parts is None or head_parts is None:
            violations.append(f'markers_missing_or_duplicated: {path}')
            continue
        if (base_parts[0], base_parts[2]) != (head_parts[0], head_parts[2]):
            violations.append(f'outside_calibration_block: {path}')
        violations.extend(
            f'{path}: {item}'
            for item in _check_block(head_parts[1], max_chars=max_block_chars, max_lines=max_block_lines)
        )
    if contract_changed and CHANGELOG not in files:
        violations.append('changelog_missing: a contract change needs an agent_rules/CHANGELOG.md entry')
    return violations


# ------------------------------------------------------------------------- git glue
def _git(*args: str) -> str:
    return subprocess.run(['git', '--no-replace-objects', *args], check=True, capture_output=True, text=True).stdout


def _read(ref: str, path: str) -> str:
    result = subprocess.run(['git', '--no-replace-objects', 'show', f'{ref}:{path}'], capture_output=True, text=True)
    return result.stdout if result.returncode == 0 else ''


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--base', required=True, help='git ref to compare against, e.g. origin/main')
    parser.add_argument('--head', default='HEAD')
    args = parser.parse_args(argv)
    names = [n for n in _git('diff', '--name-only', f'{args.base}...{args.head}').split('\n') if n]
    files = {name: (_read(args.base, name), _read(args.head, name)) for name in names}
    violations = validate(files)
    for item in violations:
        print(f'GUARD VIOLATION: {item}')
    print(f'agent_rules_guard: {len(names)} changed file(s), {len(violations)} violation(s)')
    return 1 if violations else 0


if __name__ == '__main__':
    sys.exit(main())
