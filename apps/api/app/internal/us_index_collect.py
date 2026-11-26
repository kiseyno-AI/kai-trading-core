"""Standalone US index data collection; never dispatches daily jobs or orders."""

import argparse
import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
import json
import sys

from app.market.us_index_collector import collect_us_index_inputs
from app.market.us_index_prices import SUPPORTED_US_INDEX_CODES


def main(argv: Sequence[str] | None = None, *, collector=collect_us_index_inputs) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--indices', default=','.join(SUPPORTED_US_INDEX_CODES),
        help='Comma-separated SPX,COMP,SOX subset (default: all three).',
    )
    parser.add_argument(
        '--as-of', type=datetime.fromisoformat,
        help='ISO timestamp with timezone offset fixing the session window (default: now in UTC).',
    )
    parser.add_argument('--include-history', action='store_true', help='Require 22 current-window session closes.')
    args = parser.parse_args(argv)
    try:
        result = asyncio.run(collector(
            as_of=args.as_of or datetime.now(UTC), index_codes=tuple(args.indices.split(',')),
            **({'include_history': True} if args.include_history else {}),
        ))
    except ValueError as exc:
        parser.error(str(exc))
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    return 0 if result['status'] == 'completed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
