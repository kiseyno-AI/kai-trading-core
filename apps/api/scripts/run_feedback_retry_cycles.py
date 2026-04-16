from __future__ import annotations

import argparse
import json
from datetime import date

from app.agent_context.retry_service import EXPECTED_ROLES, FeedbackRetryCycleConfig, agent_feedback_retry_service

MAX_CYCLES = 3
OUTER_BATCH_SIZE = 20


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run bounded retry cycles for agent feedback collection.')
    parser.add_argument('--market-date', type=date.fromisoformat, default=date.today(), help='Target market date in YYYY-MM-DD format. Defaults to today.')
    parser.add_argument('--max-cycles', type=int, default=MAX_CYCLES)
    parser.add_argument('--outer-batch-size', type=int, default=OUTER_BATCH_SIZE)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = agent_feedback_retry_service.run_retry_cycles(
        config=FeedbackRetryCycleConfig(
            market_date=args.market_date,
            roles=EXPECTED_ROLES,
            max_cycles=args.max_cycles,
            outer_batch_size=args.outer_batch_size,
            reuse_existing=True,
        )
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
