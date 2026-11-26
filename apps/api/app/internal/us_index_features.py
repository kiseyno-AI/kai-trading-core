"""Read explicit sealed US-index inputs and calculate descriptive features."""

import argparse
from collections.abc import Sequence
from datetime import date
import json
import sys

from app.market.us_index_features import REFERENCE_WINDOW, calculate_us_index_features
from app.market.us_index_input_freeze import load_frozen_us_index_inputs


def main(argv: Sequence[str] | None = None, *, loader=load_frozen_us_index_inputs) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target-trade-date', required=True, type=date.fromisoformat)
    parser.add_argument('--history-date', action='append', default=[], type=date.fromisoformat)
    parser.add_argument('--source-environment', choices=('real', 'mock'), default='real')
    parser.add_argument('--state-dir', default='var/us-index-preopen')
    parser.add_argument('--include-history', action='store_true', help='Read the current 22-close history seal.')
    args = parser.parse_args(argv)
    if len(args.history_date) > REFERENCE_WINDOW:
        parser.error('At most 20 explicit historical dates are supported')
    if args.include_history and args.history_date:
        parser.error('--include-history cannot be combined with --history-date')
    try:
        chronology = [*args.history_date, args.target_trade_date]
        if any(older >= newer for older, newer in zip(chronology, chronology[1:])):
            raise ValueError('Historical dates must be distinct, oldest first and before the target')
        records = []
        for day in [args.target_trade_date, *args.history_date]:
            record = loader(
                target_trade_date=day, source_environment=args.source_environment, state_dir=args.state_dir,
                **({'include_history': True} if args.include_history else {}),
            )
            if record is None:
                result = {
                    'status': 'unavailable', 'target_trade_date': args.target_trade_date.isoformat(),
                    'reason': 'no_frozen_inputs' if day == args.target_trade_date else 'missing_history',
                    'missing_target_trade_date': day.isoformat(),
                }
                break
            records.append(record)
        else:
            result = calculate_us_index_features(records[0], history=records[1:])
    except Exception as exc:
        result = {'status': 'failed', 'stage': 'features', 'error_type': type(exc).__name__}
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + '\n')
    return 1 if result['status'] == 'failed' else 0


if __name__ == '__main__':
    raise SystemExit(main())
