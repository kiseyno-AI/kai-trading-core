from __future__ import annotations

import fcntl
import hashlib
import json
import logging
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from app.agent_context.model_identity import DECIDER_ROLES
from app.agent_context.openclaw_storage_admission import inspect_openclaw_storage_admission
from app.agent_context.response_contract import decode_strict_agent_json_object
from app.agent_context.safe_file import SafeFileReadError, read_bounded_regular_file
from app.core.settings import settings
from app.market.session import get_krx_exchange_calendar_day
from app.side_agents.client import SideAgentClient, SideAgentDispatchError
from app.side_agents.runner import KST, RETRY_GAP, UnitOutcome, target_market_date
from app.side_agents.schemas import POSTMORTEM_AGENT, SideAgentValidationError
from app.side_agents.store import SideAgentStore

logger = logging.getLogger(__name__)
REVIEW = 'daily-review'
CONTRACTS = Path(__file__).resolve().parents[4] / 'runtime_contracts' / 'openclaw' / 'v1'
MIN_WATCH_OBSERVATIONS = 5
MAX_EVIDENCE_BYTES = 120_000
BEGIN = '<!-- BEGIN AUTO-CALIBRATION -->'
END = '<!-- END AUTO-CALIBRATION -->'

Focus = Literal['entry_liquidity', 'price_confirmation', 'market_regime', 'business_quality', 'dated_news', 'volatility']
FOCUS_NOTES: dict[Focus, str] = {
    'entry_liquidity': 'Compare displayed offer depth with the liquidity assumed in the entry thesis.',
    'price_confirmation': 'Distinguish last-trade evidence from an executable offer within the proposed entry range.',
    'market_regime': 'Test whether the supplied market and sector regime supports the thesis at its stated horizon.',
    'business_quality': 'Check whether current business-line and filing evidence supports the thesis at its stated horizon.',
    'dated_news': 'Separate dated, attributable catalysts from headline-only claims when assessing the thesis.',
    'volatility': 'Test the proposed entry and holding horizon against recent volatility and gap behavior.',
}
FOCUS_SOURCES: dict[Focus, str] = {
    'entry_liquidity': 'watch', 'price_confirmation': 'watch', 'market_regime': 'market',
    'business_quality': 'business', 'dated_news': 'news', 'volatility': 'market',
}
FOCUS_REQUIRED_COUNTS: dict[Focus, tuple[str, ...]] = {
    'entry_liquidity': ('ask_touch_count',),
    'price_confirmation': ('trade_touch_count', 'ask_entry_count'),
}
WATCH_RESULT_FIELDS = (
    'observation_count', 'watch_event_count', 'trade_touch_count', 'ask_touch_count', 'ask_entry_count',
    'actual_entry_fill_count', 'actual_closed_trade_count', 'source_status',
)
ReviewText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=600)]


class Finding(BaseModel):
    model_config = ConfigDict(extra='forbid')

    source: Literal['watch', 'actual_execution', 'market', 'business', 'news', 'recent_sessions']
    observation_ko: ReviewText
    implication_ko: ReviewText


class RoleReview(BaseModel):
    model_config = ConfigDict(extra='forbid')

    assessment_ko: ReviewText
    findings: list[Finding] = Field(max_length=5)
    focus: Focus | None


class DailyReviewOutput(BaseModel):
    model_config = ConfigDict(extra='forbid')

    schema_version: Literal['daily_review_v1']
    market_date: date
    summary_ko: ReviewText
    roles: dict[str, RoleReview]


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _window_close(day: date) -> datetime:
    return datetime.combine(day + timedelta(days=1), time(6), KST)


def _visible_status(record: dict[str, Any], *, store: SideAgentStore, key: str) -> dict[str, Any]:
    if record.get('status') == 'running':
        try:
            with (store.root / REVIEW / '.locks' / key).open() as lock:
                fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
                record = store.load_result(REVIEW, key) or record
        except BlockingIOError:
            return record
        except FileNotFoundError:
            pass
        if record.get('status') == 'running':
            can_retry = record['attempt'] < max(1, settings.side_agents_max_daily_attempts)
            record = {**record, 'status': 'retry_wait' if can_retry else 'failed', 'error': 'interrupted'}
    if record.get('status') == 'retry_wait' and datetime.now(KST) >= _window_close(date.fromisoformat(record['market_date'])):
        return {**record, 'status': 'failed'}
    return record


def _candidate(role: str, day: str, feedback: RoleReview, evidence: dict[str, Any]) -> tuple[dict[str, Any], str]:
    base = (CONTRACTS / role / 'AGENTS.md').read_text(encoding='utf-8')
    if base.count(BEGIN) != 1 or base.count(END) != 1 or base.index(BEGIN) >= base.index(END):
        raise ValueError('invalid calibration block')
    status = 'unchanged'
    draft = base
    if feedback.focus is not None:
        source = FOCUS_SOURCES[feedback.focus]
        observation_count = evidence['observation_count']
        if (
            observation_count < MIN_WATCH_OBSERVATIONS
            or evidence['source_status'].get(source) != 'available'
            or not all(evidence[field] for field in FOCUS_REQUIRED_COUNTS.get(feedback.focus, ()))
            or not any(finding.source == source for finding in feedback.findings)
        ):
            status = 'insufficient_evidence'
        else:
            line = f'- Daily review {day} ({observation_count} tracked WAIT decisions): {FOCUS_NOTES[feedback.focus]}'
            prefix, after_begin = base.split(BEGIN)
            block, suffix = after_begin.split(END)
            if FOCUS_NOTES[feedback.focus] not in block:
                block = f'{block.rstrip()}\n{line}\n'
                if len(block.strip()) <= 1500 and len(block.strip().splitlines()) <= 12:
                    draft = f'{prefix}{BEGIN}{block}{END}{suffix}'
                    status = 'ready_for_review'
                else:
                    status = 'calibration_block_full'
    return {
        'status': status,
        'focus': feedback.focus,
        'base_sha256': _digest(base),
        'contract_sha256': _digest(draft),
    }, draft


def list_daily_reviews(*, market_date: date | None, limit: int) -> dict[str, Any]:
    store = SideAgentStore()
    if market_date is not None:
        keys = [market_date.isoformat()]
    else:
        keys = [path.stem for path in sorted((store.root / REVIEW).glob('????-??-??.json'), reverse=True)[:limit]]
    runs = []
    for key in keys:
        record = store.load_result(REVIEW, key)
        if record is not None:
            record = _visible_status(record, store=store, key=key)
            runs.append({
                field: record[field] for field in (
                    'market_date', 'status', 'stage', 'attempt', 'observed_at', 'updated_at', 'error',
                    'analyst_contract_sha256', 'evidence_sha256', 'evidence_coverage', 'watch_outcomes',
                    'feedback', 'candidates',
                ) if field in record
            })
    return {'market': 'KRX', 'runs': runs}


def _verified_analyst_contract() -> str:
    canonical = CONTRACTS / POSTMORTEM_AGENT / 'AGENTS.md'
    live = Path(settings.openclaw_state_dir).expanduser() / f'workspace-{POSTMORTEM_AGENT}' / 'AGENTS.md'
    expected = read_bounded_regular_file(canonical, max_bytes=64_000).content
    if read_bounded_regular_file(live, max_bytes=64_000).content != expected:
        raise SideAgentValidationError('live postmortem contract differs from canonical')
    return hashlib.sha256(expected).hexdigest()


def _require_storage_admission() -> None:
    admission = inspect_openclaw_storage_admission(
        state_dir=settings.openclaw_state_dir, min_free_bytes=settings.openclaw_dispatch_min_free_bytes,
    )
    if admission['admitted'] is not True:
        raise SideAgentValidationError('OpenClaw storage admission denied')


def _is_krx_session(day: date) -> bool:
    return get_krx_exchange_calendar_day(day).is_open


def run_due_daily_review(
    *, now: datetime | None = None, store: SideAgentStore | None = None,
    client: SideAgentClient | None = None, evidence_loader: Callable[..., dict] | None = None,
    is_trading_day: Callable[[date], bool] = _is_krx_session, dry_run: bool = False,
) -> UnitOutcome | None:
    current = (now or datetime.now(timezone.utc)).astimezone(KST)
    market_date = target_market_date(current, is_trading_day)
    if market_date is None or current >= _window_close(market_date):
        return None
    day = market_date.isoformat()
    if dry_run:
        return UnitOutcome(REVIEW, day, 'skipped', 'dry_run')
    store = store or SideAgentStore()
    lock_dir = store.root / REVIEW / '.locks'
    lock_dir.mkdir(parents=True, exist_ok=True)
    with (lock_dir / day).open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return UnitOutcome(REVIEW, day, 'skipped', 'already_running')
        return _run_review(market_date, current, store, client, evidence_loader)


def _run_review(
    market_date: date, now: datetime, store: SideAgentStore, client: SideAgentClient | None,
    evidence_loader: Callable[..., dict] | None,
) -> UnitOutcome:
    day = market_date.isoformat()
    previous = store.load_result(REVIEW, day) or {}
    if previous.get('status') in {'completed', 'failed'}:
        return UnitOutcome(REVIEW, day, 'skipped', 'already_done')
    attempts = previous.get('attempt', 0)
    limit = max(1, settings.side_agents_max_daily_attempts)
    if attempts >= limit:
        store.write_result(REVIEW, day, {**previous, 'status': 'failed', 'error': 'interrupted'})
        return UnitOutcome(REVIEW, day, 'failed', 'interrupted')
    if attempts and now - datetime.fromisoformat(previous['observed_at']) < RETRY_GAP:
        return UnitOutcome(REVIEW, day, 'skipped', 'retry_gap')

    record = {
        'market_date': day, 'status': 'running', 'stage': 'evidence', 'attempt': attempts + 1,
        'observed_at': now.isoformat(), 'updated_at': now.isoformat(), 'error': None,
    }
    store.write_result(REVIEW, day, record)
    error = 'openclaw_storage_admission_blocked'
    try:
        _require_storage_admission()
        error = 'postmortem_contract_mismatch'
        contract_sha = _verified_analyst_contract()
        error = 'evidence_unavailable'
        if evidence_loader is None:
            from app.side_agents.daily_review_inputs import load_daily_review_evidence

            evidence_loader = load_daily_review_evidence
        evidence = evidence_loader(market_date, observed_at=now, store=store)
        payload = {'schema_version': 'daily_review_v1', 'market_date': day, 'evidence': evidence}
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)
        if len(encoded.encode()) > MAX_EVIDENCE_BYTES:
            raise ValueError('evidence payload exceeded its limit')
        payload = json.loads(encoded)
        record.update(
            stage='postmortem', evidence_sha256=_digest(encoded), evidence_coverage=evidence.get('coverage', {}),
            watch_outcomes={
                role: {field: payload['evidence']['roles'][role][field] for field in WATCH_RESULT_FIELDS}
                for role in sorted(DECIDER_ROLES)
            },
            analyst_contract_sha256=contract_sha, updated_at=datetime.now(KST).isoformat(),
        )
        store.write_result(REVIEW, day, record)
        error = 'postmortem_contract_mismatch'
        if _verified_analyst_contract() != contract_sha:
            raise SideAgentValidationError('postmortem contract changed during evidence collection')
        error = 'openclaw_storage_admission_blocked'
        _require_storage_admission()
        error = 'gateway_failed'
        raw = (client or SideAgentClient()).call(
            agent=POSTMORTEM_AGENT, task='daily_review', payload=payload, run_id=f'daily-review-{day}-{attempts + 1}',
        )
        error = 'postmortem_contract_mismatch'
        if _verified_analyst_contract() != contract_sha:
            raise SideAgentValidationError('postmortem contract changed during dispatch')
        error = 'invalid_postmortem'
        obj, _ = decode_strict_agent_json_object(raw)
        if obj is None:
            raise SideAgentValidationError('expected a single JSON object')
        review = DailyReviewOutput.model_validate(obj)
        if review.market_date != market_date or set(review.roles) != DECIDER_ROLES:
            raise SideAgentValidationError('date or role mismatch')
        record.update(stage='contracts', updated_at=datetime.now(KST).isoformat())
        store.write_result(REVIEW, day, record)
        error = 'contract_generation_failed'
        candidates = {}
        contracts = {}
        for role, feedback in review.roles.items():
            candidates[role], contracts[role] = _candidate(role, day, feedback, record['watch_outcomes'][role])
    except (OSError, SafeFileReadError, SideAgentDispatchError, ValueError) as exc:
        logger.warning('daily review %s %s (%s)', day, error, type(exc).__name__)
    except Exception:
        logger.exception('daily review %s %s', day, error)
    else:
        record.update(
            status='completed', stage='complete', updated_at=datetime.now(KST).isoformat(),
            feedback=review.model_dump(mode='json'), candidates=candidates, candidate_contracts=contracts,
            evidence=payload['evidence'],
        )
        store.write_result(REVIEW, day, record)
        return UnitOutcome(REVIEW, day, 'ok')

    can_retry = attempts + 1 < limit and now + RETRY_GAP < _window_close(market_date)
    record.update(status='retry_wait' if can_retry else 'failed', error=error, updated_at=datetime.now(KST).isoformat())
    store.write_result(REVIEW, day, record)
    return UnitOutcome(REVIEW, day, 'failed', error)
