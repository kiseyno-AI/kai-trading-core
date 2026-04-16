from __future__ import annotations

import argparse
import json


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Inspect legacy cleanup targets for feedback sessions. Runtime session cleanup is currently disabled because pooled session reuse is intentional and OpenClaw session deletion remains auth-blocked.'
    )
    parser.add_argument('--minutes', type=int, default=30, help='Retained for CLI compatibility only')
    parser.add_argument('--limit', type=int, default=200, help='Retained for CLI compatibility only')
    parser.add_argument('--agent-name', action='append', dest='agent_names', help='Retained for CLI compatibility only')
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    print(
        json.dumps(
            {
                'ok': True,
                'action': 'noop',
                'reason': 'feedback session cleanup is disabled',
                'details': {
                    'pooled_sessions': True,
                    'runtime_session_deletion_blocked': True,
                    'minutes': args.minutes,
                    'limit': args.limit,
                    'agent_names': args.agent_names or [],
                },
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
