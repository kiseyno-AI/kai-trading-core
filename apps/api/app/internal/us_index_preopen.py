"""Run one isolated, budgeted US-index preopen collection tick."""

import argparse
from collections.abc import Sequence
import json
import sys

from app.market.us_index_preopen import run_us_index_preopen


def main(argv: Sequence[str] | None = None, *, runner=run_us_index_preopen) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', default='var/us-index-preopen')
    args = parser.parse_args(argv)
    try:
        result = runner(state_dir=args.state_dir)
    except Exception as exc:
        result = {'status': 'failed', 'stage': 'schedule', 'error_type': type(exc).__name__}
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + '\n')
    return 0 if result['status'] in ('completed', 'not_due', 'expired') else 1


if __name__ == '__main__':
    raise SystemExit(main())
