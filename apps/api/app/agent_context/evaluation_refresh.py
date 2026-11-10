"""Off-chain daily refresh of signal-decider evaluations.

Historical outcome grading was removed from the execution-critical daily
chain (``signal_decider_evaluations`` is ``default_selected=False``) so it can
never delay or block the plan. Nothing scheduled it afterwards, and the
evaluation table stopped updating on 2026-11-04, so the performance gates
read stale grades.

This stays explicit maintenance (#685): it is a standalone command, not part
of the background runner, so a failure here can never skip runner work.
A launchd job (``scripts/install_decider_evaluation_refresh_launchd.sh``)
runs it nightly; it refreshes once per latest KRX session, late evening on a
trading day or any time on weekends and holidays:

    python -m app.agent_context.evaluation_refresh
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo('Asia/Seoul')
TRADING_DAY_EARLIEST = time(22, 30)
MIN_ATTEMPT_INTERVAL = timedelta(minutes=30)
MARKER_SCHEMA_VERSION = 'signal_decider_evaluation_refresh_v1'

_last_attempt_at: dict[tuple[Path, str], datetime] = {}


def default_marker_dir() -> Path:
    configured = os.environ.get('SIGNAL_DECIDER_EVALUATION_REFRESH_DIR', '').strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parents[2] / 'var' / 'decider-evaluation-refresh'


def _load_marker(root: Path, market_date: str) -> dict[str, Any] | None:
    try:
        payload = json.loads((root / f'{market_date}.json').read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get('schema_version') != MARKER_SCHEMA_VERSION:
        return None
    return payload


def _save_marker(root: Path, market_date: str, payload: dict[str, Any]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.marker-', dir=str(root))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, default=str)
        os.replace(tmp, root / f'{market_date}.json')
    finally:
        Path(tmp).unlink(missing_ok=True)


def _default_compute(market_date: str) -> dict[str, Any]:
    from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
    from app.agent_context.signal_decider_loop_service import (
        DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
        SIGNAL_DECIDER_EVALUATION_PREREQUISITE_STAGE_IDS,
        signal_decider_loop_service,
    )

    prerequisites = agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
        market_date=market_date,
        required_stage_ids=SIGNAL_DECIDER_EVALUATION_PREREQUISITE_STAGE_IDS,
    )
    if not prerequisites['ready']:
        return {
            'summary': {'status': prerequisites['status'], 'gated': True},
            'blocked_stage_ids': prerequisites.get('blocked_stage_ids'),
            'waiting_stage_ids': prerequisites.get('waiting_stage_ids'),
        }
    return signal_decider_loop_service.compute_recent_evaluations(
        as_of_market_date=market_date,
        lookback_days=DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
    )


def refresh_signal_decider_evaluations_if_due(
    *,
    market: Any,
    now: datetime | None = None,
    compute: Callable[[str], dict[str, Any]] | None = None,
    marker_dir: Path | None = None,
) -> dict[str, Any]:
    return _refresh_if_due(
        market=market, now=now, compute=compute or _default_compute,
        root=marker_dir or default_marker_dir(),
    )


def refresh_decider_watch_audit_if_due(
    *,
    market: Any,
    now: datetime | None = None,
    compute: Callable[[str], dict[str, Any]] | None = None,
    marker_dir: Path | None = None,
) -> dict[str, Any]:
    observed_at = now or datetime.now(timezone.utc)

    def compute_watch_report(market_date: str) -> dict[str, Any]:
        from sqlalchemy import text

        from app.agent_context.decider_watch_audit import build_decider_watch_audit
        from app.db import SessionLocal

        with SessionLocal() as session:
            session.execute(text('SET TRANSACTION READ ONLY'))
            return build_decider_watch_audit(
                session, as_of_market_date=date.fromisoformat(market_date), observed_at=observed_at,
            )

    return _refresh_if_due(
        market=market, now=observed_at, compute=compute or compute_watch_report,
        root=(marker_dir or default_marker_dir()) / 'watch-audit', save_report=True,
    )


def _refresh_if_due(
    *,
    market: Any,
    now: datetime | None,
    compute: Callable[[str], dict[str, Any]],
    root: Path,
    save_report: bool = False,
) -> dict[str, Any]:
    now_kst = (now or datetime.now(timezone.utc)).astimezone(KST)
    state = getattr(market, 'state', None)
    if state not in {'CLOSED', 'WEEKEND', 'HOLIDAY'}:
        return {'status': 'skipped', 'reason': 'market_not_idle'}
    if state == 'CLOSED' and now_kst.time() < TRADING_DAY_EARLIEST:
        return {'status': 'skipped', 'reason': 'before_evening_window'}
    market_date = str(getattr(market, 'latest_valid_trading_date', '') or '').strip()
    if not market_date:
        return {'status': 'skipped', 'reason': 'no_latest_trading_date'}
    marker = _load_marker(root, market_date)
    marker_summary = marker.get('summary') if marker is not None else None
    if (
        marker is not None
        and marker.get('status') == 'completed'
        and isinstance(marker_summary, dict)
        and marker_summary.get('status') == 'completed'
        and not marker_summary.get('gated')
    ):
        return {'status': 'skipped', 'reason': 'already_refreshed', 'market_date': market_date}
    attempt_key = (root, market_date)
    last = _last_attempt_at.get(attempt_key)
    if last is not None and now_kst - last < MIN_ATTEMPT_INTERVAL:
        return {'status': 'skipped', 'reason': 'attempt_interval', 'market_date': market_date}
    _last_attempt_at[attempt_key] = now_kst

    result = compute(market_date)
    summary = result.get('summary') if isinstance(result.get('summary'), dict) else {}
    gated = bool(summary.get('gated'))
    status = 'gated' if gated else str(summary.get('status') or 'failed')
    _save_marker(
        root,
        market_date,
        {
            'schema_version': MARKER_SCHEMA_VERSION,
            'market_date': market_date,
            'attempted_at': now_kst.isoformat(),
            'status': status,
            'summary': summary,
            'blocked_stage_ids': result.get('blocked_stage_ids'),
            'waiting_stage_ids': result.get('waiting_stage_ids'),
            **({'report': result} if save_report else {}),
        },
    )
    outcome = {'status': status, 'market_date': market_date, 'summary': summary}
    logger.info('%s %s', 'decider_watch_audit' if save_report else 'signal_decider_evaluation_refresh', outcome)
    return outcome


def main() -> int:
    from app.market.clock import async_get_market_state

    import asyncio

    market = asyncio.run(async_get_market_state())
    exit_code = 0
    for name, refresh in (
        ('evaluations', refresh_signal_decider_evaluations_if_due),
        ('watch_audit', refresh_decider_watch_audit_if_due),
    ):
        try:
            outcome = refresh(market=market)
        except Exception as exc:  # noqa: BLE001 - let independent jobs finish
            outcome = {'status': 'failed', 'error': f'{type(exc).__name__}: {exc}'[:500]}
            exit_code = 1
        print(json.dumps({'job': name, **outcome}, ensure_ascii=False, default=str))
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
