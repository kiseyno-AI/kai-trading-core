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
    load_news_inputs,
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
MAX_POSTMORTEM_BYTES = 120_000  # payload cap; larger inputs are skipped, never truncated
# The daily pipeline writes bars after ~20:10 KST and the risk snapshots (breadth, sectors)
# around 21:00, so the evening agents wait for those inputs, not for a clock time alone.
DAILY_DUE_HHMM = (21, 30)
CATCHUP_UNTIL_HHMM = (6, 0)
MIN_READY_ROWS = 500
READINESS_SQL = (
    'select (select count(*) from daily_market_risk_snapshots where market_date = :d) as risk_rows, '
    '(select count(*) from daily_price_bars where trade_date = :d) as bar_rows'
)
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
    inputs_loader: Callable[[date, datetime], dict[str, dict[str, Any]]] = load_news_inputs, dry_run: bool = False,
) -> list[UnitOutcome]:
    key = market_date.isoformat()
    state = store.load_state(NEWS_AGENT, key)
    inputs = inputs_loader(market_date, now)
    if not inputs:
        return [UnitOutcome(NEWS_AGENT, key, 'skipped', 'no_recent_news_or_filings')]
    # Completion is tracked per symbol: a retry re-ranks the inputs, so a
    # batch index from an earlier attempt can hold different symbols.
    done_symbols = set(state.get('done_symbols') or [])
    pending = {symbol: value for symbol, value in inputs.items() if symbol not in done_symbols}
    if not pending:
        return [UnitOutcome(NEWS_AGENT, key, 'skipped', 'already_done')]
    payloads = build_news_payloads(market_date, pending, batch_size=settings.side_agents_news_batch_size)
    outcomes: list[UnitOutcome] = []
    for payload in payloads:
        unit = f"{key}-{hashlib.sha256(','.join(payload['symbols']).encode()).hexdigest()[:10]}"
        ok, why = _may_attempt(state, now)
        if not ok:
            outcomes.append(UnitOutcome(NEWS_AGENT, unit, 'blocked', why))
            break
        if dry_run:
            outcomes.append(UnitOutcome(NEWS_AGENT, unit, 'skipped', f'dry_run symbols={len(payload["symbols"])} bytes={len(json.dumps(payload, default=str))}'))
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
        if outcome.status == 'ok':
            state['done_symbols'] = sorted(set(state.get('done_symbols') or []) | set(payload['symbols']))
            store.save_state(NEWS_AGENT, key, state)
        if outcome.status == 'failed':
            break  # gateway/quota problem: do not burn the attempt budget on the remaining batches
    return outcomes


def find_latest_summary(dirs: Sequence[Path]) -> Path | None:
    candidates: list[Path] = []
    for directory in dirs:
        if directory.is_dir():
            candidates.extend(p for p in directory.glob('report-*.json') if p.is_file())
    return max(candidates, key=lambda p: p.stat().st_mtime, default=None)


def load_summary_file(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_bytes()
        summary = json.loads(raw)
    except (OSError, ValueError):
        return None
    if len(raw) > MAX_POSTMORTEM_BYTES or not isinstance(summary, dict):
        return None
    return summary


def _default_summary_loader(today: date) -> dict[str, Any] | None:
    from app.side_agents.postmortem_inputs import load_summary

    return load_summary(today)


def run_postmortem(
    week_key: str, *, summary: dict[str, Any] | None, client: SideAgentClient, store: SideAgentStore,
    now: datetime, dry_run: bool = False, known_agents: Sequence[str] | None = None,
) -> UnitOutcome:
    state = store.load_state(POSTMORTEM_AGENT, week_key)
    if week_key in state['done_units']:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', 'already_done')
    if not summary:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', 'no_summary')
    total = summary.get('total_resolved')
    if isinstance(total, int) and total < 1:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', 'no_resolved_outcomes')
    ok, why = _may_attempt(state, now)
    if not ok:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'blocked', why)
    payload = build_postmortem_payload(summary)
    size = len(json.dumps(payload, default=str))
    if size > MAX_POSTMORTEM_BYTES:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', 'summary_too_large')
    if dry_run:
        return UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', f'dry_run summary_bytes={size}')
    agents = tuple(known_agents) if known_agents is not None else tuple(
        sorted(set(POSTMORTEM_KNOWN_AGENTS) | set(summary.get('known_agents') or []))
    )
    return _dispatch_unit(
        agent=POSTMORTEM_AGENT, task='postmortem', unit=week_key, state_key=week_key, payload=payload,
        validate=lambda obj: validate_postmortem(
            obj,
            known_agents=agents,
            measured_sufficient={
                agent: bool(info.get('sufficient_sample'))
                for agent, info in (summary.get('agents') or {}).items()
            },
        ),
        client=client, store=store, state=state, now=now,
        meta={'summary_sha256': _payload_sha(payload), 'period': summary.get('period')},
    )


def daily_inputs_ready(market_date: date) -> tuple[bool, str]:
    from sqlalchemy import text

    from app.db import session_scope

    with session_scope() as session:
        row = session.execute(text(READINESS_SQL), {'d': market_date}).mappings().first()
    risk_rows, bar_rows = int(row['risk_rows'] or 0), int(row['bar_rows'] or 0)
    ready = risk_rows >= MIN_READY_ROWS and bar_rows >= MIN_READY_ROWS
    return ready, f'risk_rows={risk_rows} bar_rows={bar_rows}'


def target_market_date(
    current: datetime, is_trading_day: Callable[[date], bool], *, due_hhmm: tuple[int, int] = DAILY_DUE_HHMM,
) -> date | None:
    """Select today's session once due, or the previous session for overnight catch-up."""
    hhmm = (current.hour, current.minute)
    today = current.date()
    if hhmm >= due_hhmm:
        return today if is_trading_day(today) else None
    if hhmm < CATCHUP_UNTIL_HHMM:
        day = today - timedelta(days=1)
        for _ in range(10):
            if is_trading_day(day):
                return day
            day -= timedelta(days=1)
    return None


def _is_trading_day(day: date) -> bool:
    from app.market.session import get_krx_calendar_day

    return bool(get_krx_calendar_day(day, allow_fallback=True).is_open)


def run_due(
    *, client: SideAgentClient | None = None, store: SideAgentStore | None = None, now: datetime | None = None,
    is_trading_day: Callable[[date], bool] = _is_trading_day,
    dry_run: bool = False, only: Sequence[str] | None = None,
    news_inputs_loader: Callable[[date, datetime], dict[str, dict[str, Any]]] = load_news_inputs,
    payload_builder: Callable[[date], dict[str, Any]] = build_market_payload,
    summary_loader: Callable[[date], dict[str, Any] | None] = _default_summary_loader,
    inputs_ready: Callable[[date], tuple[bool, str]] = daily_inputs_ready,
) -> RunReport:
    current = (now or datetime.now(timezone.utc)).astimezone(KST)
    client = client or SideAgentClient()
    store = store or SideAgentStore()
    wanted = set(only) if only else set(SIDE_AGENT_NAMES)
    report = RunReport()
    today = current.date()
    hhmm = (current.hour, current.minute)
    target = target_market_date(current, is_trading_day)
    evening = [a for a in (MARKET_AGENT, NEWS_AGENT) if a in wanted]
    if target is not None and evening:
        ready, detail = inputs_ready(target)
        if not ready:
            for agent in evening:  # waiting does not consume the attempt budget
                report.add(UnitOutcome(agent, target.isoformat(), 'waiting', f'daily inputs not ready: {detail}'))
        else:
            if MARKET_AGENT in evening:
                report.add(run_market_context(target, client=client, store=store, now=current, payload_builder=payload_builder, dry_run=dry_run))
            if NEWS_AGENT in evening:
                for outcome in run_news_events(target, client=client, store=store, now=current, inputs_loader=news_inputs_loader, dry_run=dry_run):
                    report.add(outcome)
    if POSTMORTEM_AGENT in wanted and current.weekday() == 5 and hhmm >= POSTMORTEM_DUE_HHMM:
        iso = current.isocalendar()
        week_key = f'{iso.year}-W{iso.week:02d}'
        if week_key in store.load_state(POSTMORTEM_AGENT, week_key)['done_units']:
            report.add(UnitOutcome(POSTMORTEM_AGENT, week_key, 'skipped', 'already_done'))
        else:
            report.add(
                run_postmortem(
                    week_key, summary=summary_loader(today), client=client, store=store, now=current, dry_run=dry_run,
                )
            )
    return report
