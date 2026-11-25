"""Run one isolated, budgeted US-index preopen collection tick."""

import argparse
from collections.abc import Sequence
from datetime import date
import json
import sys

from app.market.us_index_preopen import run_us_index_preopen
from app.market.us_index_input_freeze import freeze_us_index_inputs


def main(argv: Sequence[str] | None = None, *, runner=run_us_index_preopen, freezer=freeze_us_index_inputs) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', default='var/us-index-preopen')
    args = parser.parse_args(argv)
    stage = 'schedule'
    try:
        result = runner(state_dir=args.state_dir)
        if result.get('data_complete'):
            stage = 'freeze'
            frozen = freezer(
                target_trade_date=date.fromisoformat(result['target_trade_date']),
                snapshot_ids=result['snapshot_ids'], source_environment=result['source_environment'],
                state_dir=args.state_dir,
            )
            result['input_freeze'] = (
                {'status': 'frozen', 'manifest_id': frozen.manifest_id, 'snapshot_ids': frozen.snapshot_ids}
                if frozen is not None else {'status': 'unavailable'}
            )
    except Exception as exc:
        result = {'status': 'failed', 'stage': stage, 'error_type': type(exc).__name__}
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + '\n')
    return 0 if result['status'] in ('completed', 'not_due', 'expired') else 1


if __name__ == '__main__':
    raise SystemExit(main())
