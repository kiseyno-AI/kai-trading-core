from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date
from typing import Sequence

from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service


def _parse_iso_date(value: str | None) -> date | None:
    if value in (None, ''):
        return None
    return date.fromisoformat(value)


def _parse_int_at_least(value: str, *, minimum: int, name: str) -> int:
    parsed = int(value)
    if parsed < minimum:
        raise argparse.ArgumentTypeError(f'{name} must be >= {minimum}')
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run DART fundamentals backfill for the current active universe.')
    parser.add_argument('--symbol-limit', type=lambda value: _parse_int_at_least(value, minimum=1, name='symbol_limit'), default=25)
    parser.add_argument('--symbol-offset', type=lambda value: _parse_int_at_least(value, minimum=0, name='symbol_offset'), default=0)
    parser.add_argument('--capture-limit', type=lambda value: _parse_int_at_least(value, minimum=1, name='capture_limit'), default=250)
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--fs-div', default=None)
    parser.add_argument('--start-date', default=None, help='ISO date, e.g. 2015-01-01')
    parser.add_argument('--end-date', default=None, help='ISO date, e.g. 2026-04-07')
    return parser


async def _run_backfill(args: argparse.Namespace) -> dict:
    return await dart_fundamentals_capture_service.backfill_current_universe_fundamentals(
        symbol_limit=args.symbol_limit,
        symbol_offset=args.symbol_offset,
        capture_limit=args.capture_limit,
        force=args.force,
        fs_div=args.fs_div,
        start_date=_parse_iso_date(args.start_date),
        end_date=_parse_iso_date(args.end_date),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        result = asyncio.run(_run_backfill(args))
    except Exception as exc:
        print(json.dumps({'status': 'failed', 'error': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1

    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
