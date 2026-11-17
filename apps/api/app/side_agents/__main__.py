from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from app.side_agents.runner import (
    KST,
    _default_summary_loader,
    load_summary_file,
    run_due,
    run_market_context,
    run_news_events,
    run_postmortem,
)
from app.side_agents.client import SideAgentClient
from app.side_agents.daily_review import run_due_daily_review
from app.side_agents.schemas import MARKET_AGENT, NEWS_AGENT, POSTMORTEM_AGENT, SIDE_AGENT_NAMES
from app.side_agents.store import SideAgentStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='python -m app.side_agents')
    sub = parser.add_subparsers(dest='command', required=True)
    due = sub.add_parser('run-due', help='run whatever is due now (idempotent; safe to call every 30 minutes)')
    due.add_argument('--dry-run', action='store_true')
    due.add_argument('--only', action='append', choices=SIDE_AGENT_NAMES)
    review = sub.add_parser('run-daily-review', help='run the separate KRX post-close review if due')
    review.add_argument('--dry-run', action='store_true')
    one = sub.add_parser('run', help='run one agent now')
    one.add_argument('--agent', required=True, choices=SIDE_AGENT_NAMES)
    one.add_argument('--date', help='market date YYYY-MM-DD (default: today KST)')
    one.add_argument('--summary', type=Path, help='postmortem summary JSON file (default: computed from stored decisions)')
    one.add_argument('--dry-run', action='store_true')
    sub.add_parser('status', help='list stored results')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    store = SideAgentStore()

    if args.command == 'status':
        for agent in SIDE_AGENT_NAMES:
            files = sorted((store.root / agent).glob('*.json')) if (store.root / agent).is_dir() else []
            print(f'{agent}: {len(files)} results, latest={files[-1].name if files else "-"}')
        return 0

    if args.command == 'run-due':
        report = run_due(store=store, dry_run=args.dry_run, only=args.only)
        print(json.dumps(report.as_dict(), ensure_ascii=False))
        return 0

    if args.command == 'run-daily-review':
        outcome = run_due_daily_review(store=store, dry_run=args.dry_run)
        print(json.dumps(vars(outcome) if outcome else None, ensure_ascii=False))
        return int(outcome is not None and outcome.status == 'failed')

    now = datetime.now(timezone.utc).astimezone(KST)
    market_date = date.fromisoformat(args.date) if args.date else now.date()
    client = SideAgentClient()
    if args.agent == MARKET_AGENT:
        outcomes = [run_market_context(market_date, client=client, store=store, now=now, dry_run=args.dry_run)]
    elif args.agent == NEWS_AGENT:
        outcomes = run_news_events(market_date, client=client, store=store, now=now, dry_run=args.dry_run)
    else:
        # --date (default: today KST) is the review's as-of day for both the
        # outcome window and the ISO-week unit, so a manual re-run of a past
        # week is labeled and windowed consistently.
        iso = market_date.isocalendar()
        summary = load_summary_file(args.summary) if args.summary else _default_summary_loader(market_date)
        outcomes = [run_postmortem(f'{iso.year}-W{iso.week:02d}', summary=summary, client=client, store=store, now=now, dry_run=args.dry_run)]
    print(json.dumps([vars(o) for o in outcomes], ensure_ascii=False))
    return 0 if all(o.status in {'ok', 'skipped'} for o in outcomes) else 1


if __name__ == '__main__':
    sys.exit(main())
