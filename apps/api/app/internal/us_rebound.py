"""Advance one independent US-rebound observation tick."""

import argparse
from collections.abc import Sequence
from datetime import date
import json
import sys

from app.core.settings import settings
from app.overnight_rebound.runner import run_us_rebound_tick


def main(argv: Sequence[str] | None = None, *, runner=run_us_rebound_tick) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', default=settings.us_rebound_state_dir)
    args = parser.parse_args(argv)
    try:
        enabled_from = (
            date.fromisoformat(settings.us_rebound_enabled_from)
            if settings.us_rebound_mode != 'disabled' and settings.us_rebound_enabled_from else None
        )
        result = runner(mode=settings.us_rebound_mode, enabled_from=enabled_from, state_dir=args.state_dir)
    except Exception as exc:
        result = {'status': 'failed', 'stage': 'us_rebound', 'error_type': type(exc).__name__}
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + '\n')
    return 1 if result['status'] == 'failed' else 0


if __name__ == '__main__':
    raise SystemExit(main())
