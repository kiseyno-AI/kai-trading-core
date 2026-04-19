from __future__ import annotations

import argparse
import json

from app.agent_context.session_cleanup_service import agent_feedback_session_cleanup_service


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Delete queued OpenClaw feedback sessions after DB persistence and keep failed deletions retryable.')
    parser.add_argument('--limit', type=int, default=100, help='Maximum queued session deletions to attempt in one run')
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = agent_feedback_session_cleanup_service.run_pending_cleanup(limit=max(1, args.limit))
    print(json.dumps(result, ensure_ascii=False))
    failed = int(result.get('summary', {}).get('failed', 0))
    return 1 if failed > 0 else 0


if __name__ == '__main__':
    raise SystemExit(main())
