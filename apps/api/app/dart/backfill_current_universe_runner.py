from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from app.dart.backfill_current_universe import _parse_int_at_least, _parse_iso_date
from app.dart.fundamentals_capture_service import dart_fundamentals_capture_service


def _default_log_path() -> Path:
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    return Path('var/log') / f'dart-backfill-runner-{timestamp}.log'


def _append_log(log_path: Path, *, event: str, **fields: Any) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'event': event,
        **fields,
    }
    with log_path.open('a', encoding='utf-8') as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + '\n')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Run DART fundamentals backfill batches until the current active universe is done.')
    parser.add_argument('--start-offset', type=lambda value: _parse_int_at_least(value, minimum=0, name='start_offset'), default=0)
    parser.add_argument('--batch-size', type=lambda value: _parse_int_at_least(value, minimum=1, name='batch_size'), default=25)
    parser.add_argument('--capture-limit', type=lambda value: _parse_int_at_least(value, minimum=1, name='capture_limit'), default=250)
    parser.add_argument('--fs-div', default=None)
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--start-date', default=None, help='ISO date, e.g. 2015-01-01')
    parser.add_argument('--end-date', default=None, help='ISO date, e.g. 2026-04-07')
    parser.add_argument('--log-file', default=None, help='Optional log path. Defaults to var/log/dart-backfill-runner-<timestamp>.log')
    parser.add_argument('--max-batches', type=lambda value: _parse_int_at_least(value, minimum=1, name='max_batches'), default=None)
    return parser


async def _run_batches(args: argparse.Namespace, *, log_path: Path) -> dict[str, Any]:
    current_offset = args.start_offset
    batches_completed = 0
    total_events_inserted = 0
    total_events_captured = 0
    total_events_failed = 0
    total_symbols_resolved = 0
    active_symbols_total: int | None = None
    last_result: dict[str, Any] | None = None

    while True:
        _append_log(
            log_path,
            event='batch_started',
            current_offset=current_offset,
            batch_size=args.batch_size,
            capture_limit=args.capture_limit,
            fs_div=args.fs_div,
        )
        try:
            result = await dart_fundamentals_capture_service.backfill_current_universe_fundamentals(
                symbol_limit=args.batch_size,
                symbol_offset=current_offset,
                capture_limit=args.capture_limit,
                force=args.force,
                fs_div=args.fs_div,
                start_date=_parse_iso_date(args.start_date),
                end_date=_parse_iso_date(args.end_date),
            )
        except Exception as exc:
            _append_log(
                log_path,
                event='batch_failed',
                current_offset=current_offset,
                batches_completed=batches_completed,
                error=str(exc),
            )
            raise

        last_result = result
        active_symbols_total = result['active_symbols_total']
        symbols_selected = result['symbols_selected']
        sync_result = result.get('sync') if isinstance(result.get('sync'), dict) else {}
        capture_result = result.get('capture') if isinstance(result.get('capture'), dict) else {}
        next_offset = current_offset + symbols_selected
        batch_status = str(result.get('status') or 'completed')
        if batch_status != 'completed':
            summary = {
                'status': batch_status if batch_status in {'partial', 'failed'} else 'failed',
                'reason': 'incomplete_batch',
                'batches_completed': batches_completed,
                'start_offset': args.start_offset,
                'last_offset': current_offset,
                'next_offset': current_offset,
                'active_symbols_total': active_symbols_total,
                'total_symbols_resolved': total_symbols_resolved,
                'total_events_inserted': total_events_inserted,
                'total_events_captured': total_events_captured,
                'total_events_failed': total_events_failed,
                'failed_stock_codes': list(result.get('failed_stock_codes') or []),
                'log_file': str(log_path),
                'last_result': result,
            }
            _append_log(
                log_path,
                event='runner_incomplete',
                **summary,
            )
            return summary
        total_events_inserted += int(sync_result.get('events_inserted') or 0)
        total_events_captured += int(capture_result.get('events_captured') or 0)
        total_events_failed += int(capture_result.get('events_failed') or 0)
        total_symbols_resolved += result['symbols_resolved']
        batches_completed += 1

        _append_log(
            log_path,
            event='batch_completed',
            current_offset=current_offset,
            next_offset=next_offset,
            active_symbols_total=active_symbols_total,
            symbols_selected=symbols_selected,
            symbols_resolved=result['symbols_resolved'],
            sync_run_id=sync_result.get('run_id'),
            capture_run_id=capture_result.get('run_id'),
            events_inserted=int(sync_result.get('events_inserted') or 0),
            events_captured=int(capture_result.get('events_captured') or 0),
            events_failed=int(capture_result.get('events_failed') or 0),
            events_skipped=int(capture_result.get('events_skipped') or 0),
            sync_status=sync_result.get('status'),
            capture_status=capture_result.get('status'),
        )

        if args.max_batches is not None and batches_completed >= args.max_batches:
            summary = {
                'status': 'stopped',
                'reason': 'max_batches_reached',
                'batches_completed': batches_completed,
                'start_offset': args.start_offset,
                'last_offset': current_offset,
                'next_offset': next_offset,
                'active_symbols_total': active_symbols_total,
                'total_symbols_resolved': total_symbols_resolved,
                'total_events_inserted': total_events_inserted,
                'total_events_captured': total_events_captured,
                'total_events_failed': total_events_failed,
                'log_file': str(log_path),
                'last_result': last_result,
            }
            _append_log(log_path, event='runner_stopped', **summary)
            return summary

        if symbols_selected == 0 or next_offset >= active_symbols_total:
            summary = {
                'status': 'completed',
                'batches_completed': batches_completed,
                'start_offset': args.start_offset,
                'last_offset': current_offset,
                'next_offset': next_offset,
                'active_symbols_total': active_symbols_total,
                'total_symbols_resolved': total_symbols_resolved,
                'total_events_inserted': total_events_inserted,
                'total_events_captured': total_events_captured,
                'total_events_failed': total_events_failed,
                'log_file': str(log_path),
                'last_result': last_result,
            }
            _append_log(log_path, event='runner_completed', **summary)
            return summary

        current_offset = next_offset


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    log_path = Path(args.log_file) if args.log_file else _default_log_path()
    _append_log(
        log_path,
        event='runner_started',
        start_offset=args.start_offset,
        batch_size=args.batch_size,
        capture_limit=args.capture_limit,
        fs_div=args.fs_div,
        force=args.force,
        start_date=args.start_date,
        end_date=args.end_date,
        max_batches=args.max_batches,
    )

    try:
        result = asyncio.run(_run_batches(args, log_path=log_path))
    except Exception as exc:
        print(json.dumps({'status': 'failed', 'log_file': str(log_path), 'error': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1

    print(json.dumps(result, ensure_ascii=False))
    return 1 if result.get('reason') == 'incomplete_batch' else 0


if __name__ == '__main__':
    raise SystemExit(main())
