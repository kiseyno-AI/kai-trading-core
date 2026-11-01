from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date
from typing import Sequence

from app.dart.fundamentals_capture_service import (
    LEGACY_CFS_NO_DATA_RECOVERY_MODE,
    LEGACY_CFS_RECOVERY_CANDIDATE_MANIFEST_VERSION,
    _normalize_exact_stock_code_manifest,
    _stock_code_manifest_sha256,
    dart_fundamentals_capture_service,
    legacy_cfs_recovery_candidate_manifest,
    legacy_cfs_recovery_candidate_manifest_sha256,
)
from app.dart.fundamentals_store import dart_fundamentals_repository


def _parse_symbols(value: str) -> list[str]:
    return _normalize_exact_stock_code_manifest(str(value).split(','))


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Dry-run or apply an exact, hash-bound retry of legacy annual '
            'CFS [013] snapshots through the current CFS-to-OFS fallback.'
        )
    )
    parser.add_argument(
        '--symbols',
        required=True,
        help='Comma-separated exact stock-code manifest.',
    )
    parser.add_argument('--start-date', required=True, type=_parse_date)
    parser.add_argument('--end-date', required=True, type=_parse_date)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--expected-stock-code-manifest-sha256')
    parser.add_argument('--expected-candidate-manifest-sha256')
    return parser


def _inspect_candidates(
    *,
    stock_codes: list[str],
    start_date: date,
    end_date: date,
) -> tuple[list[dict], dict]:
    if start_date > end_date:
        raise ValueError('start-date must be on or before end-date')
    candidates = (
        dart_fundamentals_repository
        .list_legacy_cfs_no_data_recovery_candidates(
            stock_codes=stock_codes,
            receipt_date_from=start_date,
            receipt_date_to=end_date,
        )
    )
    stock_hash = _stock_code_manifest_sha256(stock_codes)
    candidate_hash = legacy_cfs_recovery_candidate_manifest_sha256(
        candidates
    )
    return candidates, {
        'recovery_mode': LEGACY_CFS_NO_DATA_RECOVERY_MODE,
        'candidate_manifest_version': (
            LEGACY_CFS_RECOVERY_CANDIDATE_MANIFEST_VERSION
        ),
        'stock_code_manifest': stock_codes,
        'stock_code_manifest_sha256': stock_hash,
        'candidate_manifest': legacy_cfs_recovery_candidate_manifest(
            candidates
        ),
        'candidate_manifest_sha256': candidate_hash,
        'candidate_count': len(candidates),
        'receipt_date_from': start_date.isoformat(),
        'receipt_date_to': end_date.isoformat(),
    }


async def _run(args: argparse.Namespace) -> tuple[int, dict]:
    stock_codes = _parse_symbols(args.symbols)
    candidates, inspection = _inspect_candidates(
        stock_codes=stock_codes,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    if not args.apply:
        return 0, {'status': 'dry_run', **inspection}

    expected_stock_hash = str(
        args.expected_stock_code_manifest_sha256 or ''
    ).strip().lower()
    expected_candidate_hash = str(
        args.expected_candidate_manifest_sha256 or ''
    ).strip().lower()
    if not expected_stock_hash or not expected_candidate_hash:
        raise ValueError(
            '--apply requires both expected manifest SHA256 values'
        )
    if expected_stock_hash != inspection['stock_code_manifest_sha256']:
        raise ValueError('stock-code manifest SHA256 mismatch')
    if expected_candidate_hash != inspection['candidate_manifest_sha256']:
        raise ValueError('candidate manifest SHA256 mismatch')
    if not candidates:
        raise ValueError('reviewed legacy CFS recovery scope is empty')

    event_ids = [candidate['id'] for candidate in candidates]
    result = (
        await dart_fundamentals_capture_service.capture_recent_fundamentals(
            limit=None,
            force=False,
            trigger='legacy_cfs_013_exact_repair',
            fs_div='CFS',
            stock_codes=stock_codes,
            receipt_date_from=args.start_date,
            receipt_date_to=args.end_date,
            event_ids=event_ids,
            exact_stock_code_manifest=stock_codes,
            root_stock_code_manifest=stock_codes,
            legacy_cfs_no_data_recovery=True,
            expected_recovery_candidate_manifest_sha256=(
                expected_candidate_hash
            ),
        )
    )
    remaining_candidates, remaining = _inspect_candidates(
        stock_codes=stock_codes,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    partial = bool(
        result.get('events_failed')
        or remaining_candidates
    )
    return (
        2 if partial else 0,
        {
            'status': 'partial' if partial else 'completed',
            **inspection,
            'capture': result,
            'remaining_candidate_count': len(remaining_candidates),
            'remaining_candidate_manifest': remaining[
                'candidate_manifest'
            ],
            'remaining_candidate_manifest_sha256': remaining[
                'candidate_manifest_sha256'
            ],
        },
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
        exit_code, result = asyncio.run(_run(args))
    except Exception as exc:
        print(
            json.dumps(
                {'status': 'failed', 'error': str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
