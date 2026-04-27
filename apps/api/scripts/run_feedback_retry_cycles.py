from __future__ import annotations

import argparse
import json
import os
from datetime import date

from app.agent_context.retry_service import EXPECTED_ROLES, FeedbackRetryCycleConfig, agent_feedback_retry_service

MAX_CYCLES = 3
OUTER_BATCH_SIZE = 20
MARKET_DATE_ENV_VARS = ('FEEDBACK_RETRY_MARKET_DATE', 'MARKET_DATE')


def _default_market_date() -> date:
    for name in MARKET_DATE_ENV_VARS:
        value = os.getenv(name)
        if value:
            return date.fromisoformat(value)
    return date.today()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run bounded retry cycles for agent feedback collection.')
    parser.add_argument(
        '--market-date',
        type=date.fromisoformat,
        default=_default_market_date(),
        help='Target market date in YYYY-MM-DD format. Defaults to FEEDBACK_RETRY_MARKET_DATE, MARKET_DATE, then today.',
    )
    parser.add_argument(
        '--symbols',
        nargs='*',
        default=None,
        help='Optional explicit symbols to bound the retry cycle to. Accepts repeated args or comma-separated values.',
    )
    parser.add_argument('--max-cycles', type=int, default=MAX_CYCLES)
    parser.add_argument('--outer-batch-size', type=int, default=OUTER_BATCH_SIZE)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = agent_feedback_retry_service.run_retry_cycles(
        config=FeedbackRetryCycleConfig(
            market_date=args.market_date,
            roles=EXPECTED_ROLES,
            symbols=tuple(args.symbols) if args.symbols else None,
            max_cycles=args.max_cycles,
            outer_batch_size=args.outer_batch_size,
            reuse_existing=True,
        )
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
