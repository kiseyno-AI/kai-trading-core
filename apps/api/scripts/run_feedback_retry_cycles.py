from __future__ import annotations

import json
from datetime import date

from app.agent_context.retry_service import EXPECTED_ROLES, FeedbackRetryCycleConfig, agent_feedback_retry_service

MARKET_DATE = date.fromisoformat('2026-04-15')
MAX_CYCLES = 3
OUTER_BATCH_SIZE = 20


def main() -> None:
    result = agent_feedback_retry_service.run_retry_cycles(
        config=FeedbackRetryCycleConfig(
            market_date=MARKET_DATE,
            roles=EXPECTED_ROLES,
            max_cycles=MAX_CYCLES,
            outer_batch_size=OUTER_BATCH_SIZE,
            reuse_existing=True,
        )
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
