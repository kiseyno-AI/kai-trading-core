from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.db import session_scope
from app.db_models import AgentContextFeedback


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Kill stale OpenClaw feedback sessions recorded in agent_context_feedback summary payloads.')
    parser.add_argument('--minutes', type=int, default=30, help='Only inspect feedback rows older than this many minutes (default: 30)')
    parser.add_argument('--limit', type=int, default=200, help='Maximum feedback rows to inspect (default: 200)')
    parser.add_argument('--agent-name', action='append', dest='agent_names', help='Optional agent_name filter, repeatable')
    return parser.parse_args()


def _extract_cleanup_target(record: AgentContextFeedback) -> dict[str, Any] | None:
    summary = record.summary_payload or {}
    dispatch = summary.get('dispatch') if isinstance(summary, dict) else None
    if not isinstance(dispatch, dict):
        return None
    session_ref = dispatch.get('session_ref')
    if isinstance(session_ref, dict) and session_ref.get('session_key'):
        return session_ref
    return None


def main() -> int:
    args = _parse_args()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=max(1, args.minutes))

    with session_scope() as session:
        query = session.query(AgentContextFeedback).filter(AgentContextFeedback.received_at <= cutoff)
        if args.agent_names:
            query = query.filter(AgentContextFeedback.agent_name.in_(args.agent_names))
        rows = query.order_by(AgentContextFeedback.received_at.desc()).limit(max(1, args.limit)).all()

        results: list[dict[str, Any]] = []
        client = agent_feedback_orchestrator._get_client()
        for row in rows:
            target = _extract_cleanup_target(row)
            if not target:
                continue
            cleanup = client.cleanup_session(target)
            summary = dict(row.summary_payload or {})
            dispatch = dict(summary.get('dispatch') or {})
            dispatch['cleanup'] = cleanup
            summary['dispatch'] = dispatch
            row.summary_payload = summary
            results.append(
                {
                    'feedback_id': row.id,
                    'agent_name': row.agent_name,
                    'symbol': row.symbol,
                    'market_date': row.market_date.isoformat(),
                    'session_key': target.get('session_key'),
                    'cleanup': cleanup,
                }
            )
        session.flush()

    print(json.dumps({'inspected': len(rows), 'cleaned': len(results), 'results': results}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
