from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Sequence
from zoneinfo import ZoneInfo

from app.core.settings import settings
from app.side_agents.client import SideAgentClient, SideAgentDispatchError
from app.side_agents.payloads import (
    build_market_payload,
    build_news_payloads,
    build_postmortem_payload,
    load_recent_news,
    select_news_symbols,
)
from app.side_agents.schemas import (
    MARKET_AGENT,
    NEWS_AGENT,
    POSTMORTEM_AGENT,
    SIDE_AGENT_NAMES,
    SideAgentValidationError,
    parse_json_object,
    validate_market_context,
    validate_news_events,
    validate_postmortem,
)
from app.side_agents.store import SideAgentStore

logger = logging.getLogger(__name__)
KST = ZoneInfo('Asia/Seoul')
RETRY_GAP = timedelta(minutes=25)
MAX_POSTMORTEM_BYTES = 120_000
MARKET_DUE_HHMM = (18, 30)
NEWS_DUE_HHMM = (19, 0)
POSTMORTEM_DUE_HHMM = (9, 0)
POSTMORTEM_KNOWN_AGENTS = (
    'eod-summarizer', 'technical-summarizer', 'fundamental-summarizer',
    'signal-decider', 'short-term-decider', MARKET_AGENT, NEWS_AGENT, 'desk',
)


@dataclass
class UnitOutcome:
    agent: str
    unit: str
    status: str  # ok | failed | rejected | skipped | blocked
    detail: str = ''


@dataclass
class RunReport:
    outcomes: list[UnitOutcome] = field(default_factory=list)

    def add(self, outcome: UnitOutcome) -> None:
        self.outcomes.append(outcome)
        logger.info('side_agent %s %s -> %s %s', outcome.agent, outcome.unit, outcome.status, outcome.detail)

    def as_dict(self) -> list[dict[str, str]]:
        return [vars(o) for o in self.outcomes]


def _payload_sha(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()


def _may_attempt(state: dict[str, Any], now: datetime) -> tuple[bool, str]:
    if state['attempts'] >= settings.side_agents_max_daily_attempts:
        return False, 'attempt_budget_exhausted'
    last = state.get('last_attempt_at')
    if last:
        try:
            if now - datetime.fromisoformat(last) < RETRY_GAP:
                return False, 'retry_gap'
        except ValueError:
            pass
    return True, ''


def _dispatch_unit(
    *,
    agent: str,
    task: str,
    unit: str,
    state_key: str,
    payload: dict[str, Any],
    validate: Callable[[dict[str, Any]], dict[str, Any]],
    client: SideAgentClient,
    store: SideAgentStore,
    state: dict[str, Any],
    now: datetime,
    meta: dict[str, Any],
) -> UnitOutcome:
    # Only failures consume the budget and start the retry gap; successes do not.
    try:
        raw = client.call(agent=agent, task=task, payload=payload, run_id=unit.replace('/', '-'))
    except SideAgentDispatchError as exc:
        state['attempts'] += 1
        state['last_attempt_at'] = now.isoformat()
        state['last_error'] = str(exc)[:300]
        store.save_state(agent, state_key, state)
        return UnitOutcome(agent, unit, 'failed', state['last_error'])
    try:
        validated = validate(parse_json_object(raw))
    except SideAgentValidationError as exc:
        state['attempts'] += 1
        state['last_attempt_at'] = now.isoformat()
        state['last_error'] = f'rejected: {exc}'[:300]
        store.save_state(agent, state_key, state)
        store.write_rejected(agent, unit, {'raw_output': raw[:20000], 'error': str(exc), 'at': now.isoformat()})
        return UnitOutcome(agent, unit, 'rejected', str(exc))
    store.write_result(
        agent,
        unit,
        {
            'agent': agent,
            'unit': unit,
            'completed_at': now.isoformat(),
            'payload_sha256': _payload_sha(payload),
            'advisory_only': True,
            'output': validated,
            **meta,
        },
    )
    state['done_units'] = sorted(set(state['done_units']) | {unit})
    state['last_error'] = None
    store.save_state(agent, state_key, state)
    return UnitOutcome(agent, unit, 'ok')


def run_market_context(
    market_date: date, *, client: SideAgentClient, store: SideAgentStore, now: datetime,
    payload_builder: Callable[[date], dict[str, Any]] = build_market_payload, dry_run: bool = False,
) -> UnitOutcome:
    key = market_date.isoformat()
    state = store.load_state(MARKET_AGENT, key)
    if key in state['done_units']:
        return UnitOutcome(MARKET_AGENT, key, 'skipped', 'already_done')
    ok, why = _may_attempt(state, now)
    if not ok:
        return UnitOutcome(MARKET_AGENT, key, 'blocked', why)
    payload = payload_builder(market_date)
    if dry_run:
        return UnitOutcome(MARKET_AGENT, key, 'skipped', f'dry_run payload_bytes={len(json.dumps(payload, default=str))}')
    return _dispatch_unit(
        agent=MARKET_AGENT, task='market_context', unit=key, state_key=key, payload=payload,
        validate=lambda obj: validate_market_context(obj, market_date=market_date),
        client=client, store=store, state=state, now=now, meta={'market_date': key},
    )


def run_news_events(
    market_date: date, *, client: SideAgentClient, store: SideAgentStore, now: datetime,
    news_loader: Callable[[], list[dict[str, Any]]] | None = None, dry_run: bool = False,
) -> list[UnitOutcome]:
    key = market_date.isoformat()
    state = store.load_state(NEWS_AGENT, key)
    items = news_loader() if news_loader else load_recent_news(now=now)
    selected = select_news_symbols(items, max_symbols=settings.side_agents_news_max_symbols)
    if not selected:
        return [UnitOutcome(NEWS_AGENT, key, 'skipped', 'no_recent_news')]
    payloads = build_news_payloads(market_date, selected, batch_size=settings.side_agents_news_batch_size)
    outcomes: list[UnitOutcome] = []
    for index, payload in enumerate(payloads):
        unit = f'{key}-b{index:02d}'
        if unit in state['done_units']:
            outcomes.append(UnitOutcome(NEWS_AGENT, unit, 'skipped', 'already_done'))
            continue
        ok, why = _may_attempt(state, now)
        if not ok:
            outcomes.append(UnitOutcome(NEWS_AGENT, unit, 'blocked', why))
            break
        if dry_run:
            outcomes.append(UnitOutcome(NEWS_AGENT, unit, 'skipped', f'dry_run symbols={len(payload["symbols"])}'))
            continue
        outcome = _dispatch_unit(
            agent=NEWS_AGENT, task='news_events', unit=unit, state_key=key, payload=payload,
            validate=lambda obj, symbols=tuple(payload['symbols']): validate_news_events(
                obj, market_date=market_date, symbols=symbols
            ),
            client=client, store=store, state=state, now=now,
            meta={'market_date': key, 'symbols': payload['symbols']},
        )
        outcomes.append(outcome)
        if outcome.status == 'failed':
            break  # gateway/quota problem: do not burn the attempt budget on the remaining batches
    return outcomes


def find_latest_summary(dirs: Sequence[Path]) -> Path | None:
    candidates: list[Path] = []
    for directory in dirs:
        if directory.is_dir():
            candidates.extend(p for p in directory.glob('report-*.json') if p.is_file())
    return max(candidates, key=lambda p: p.stat().st_mtime, default=None)


def run_postmortem(
    week_key: str, *, summary_path: Path | None, client: SideAgentClient, store: SideAgentStore,
    now: datetime, dry_run: bool = False,
) -> UnitOutcome:
    state = store.load_state(POSTMORTEM_AGENT, week_key)
    if week_key in state['done_units']:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', 'already_done')
    if summary_path is None:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', 'no_summary')
    ok, why = _may_attempt(state, now)
    if not ok:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'blocked', why)
    try:
        raw = summary_path.read_bytes()
        summary = json.loads(raw)
    except (OSError, ValueError) as exc:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', f'summary_unreadable: {type(exc).__name__}')
    if len(raw) > MAX_POSTMORTEM_BYTES or not isinstance(summary, dict):
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', 'summary_too_large_or_not_object')
    payload = build_postmortem_payload(summary)
    if dry_run:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', f'dry_run summary_bytes={len(raw)}')
    return _dispatch_unit(
        agent=POSTMORTEM_AGENT, task='postmortem', unit=week_key, state_key=week_key, payload=payload,
        validate=lambda obj: validate_postmortem(obj, known_agents=POSTMORTEM_KNOWN_AGENTS),
        client=client, store=store, state=state, now=now,
        meta={'summary_file': summary_path.name, 'summary_sha256': hashlib.sha256(raw).hexdigest()},
    )


def _is_trading_day(day: date) -> bool:
    from app.market.session import get_krx_calendar_day

    return bool(get_krx_calendar_day(day, allow_fallback=True).is_open)


def run_due(
    *, client: SideAgentClient | None = None, store: SideAgentStore | None = None, now: datetime | None = None,
    is_trading_day: Callable[[date], bool] = _is_trading_day, summary_dirs: Sequence[Path] | None = None,
    dry_run: bool = False, only: Sequence[str] | None = None,
    news_loader: Callable[[], list[dict[str, Any]]] | None = None,
    payload_builder: Callable[[date], dict[str, Any]] = build_market_payload,
) -> RunReport:
    current = (now or datetime.now(timezone.utc)).astimezone(KST)
    client = client or SideAgentClient()
    store = store or SideAgentStore()
    wanted = set(only) if only else set(SIDE_AGENT_NAMES)
    report = RunReport()
    today = current.date()
    hhmm = (current.hour, current.minute)
    if is_trading_day(today):
        if MARKET_AGENT in wanted and hhmm >= MARKET_DUE_HHMM:
            report.add(run_market_context(today, client=client, store=store, now=current, payload_builder=payload_builder, dry_run=dry_run))
        if NEWS_AGENT in wanted and hhmm >= NEWS_DUE_HHMM:
            for outcome in run_news_events(today, client=client, store=store, now=current, news_loader=news_loader, dry_run=dry_run):
                report.add(outcome)
    if POSTMORTEM_AGENT in wanted and current.weekday() == 5 and hhmm >= POSTMORTEM_DUE_HHMM:
        iso = current.isocalendar()
        dirs = list(summary_dirs) if summary_dirs is not None else [Path(__file__).resolve().parents[4] / 'var' / 'alpha-lab']
        report.add(
            run_postmortem(
                f'{iso.year}-W{iso.week:02d}', summary_path=find_latest_summary(dirs), client=client,
                store=store, now=current, dry_run=dry_run,
            )
        )
    return report
