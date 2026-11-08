"""Run the weekly agent improvement loop from Core's background runner.

Core runs around the clock, so it owns the schedule instead of a separate launchd
job: once per ISO week, on Saturday from 09:00 KST, the background runner starts
``scripts/run_agent_improvement_local.sh`` (signal lab, findings, and the guarded
calibration PR when ``var/alpha-lab/loop.env`` opts in) in a worker thread. The
script keeps its own schedule check and writes ``var/alpha-lab/.done-<ISO week>``
only when it finishes, so a failed run is retried on the next hourly check of the
same Saturday and never more than once per week after success.

``AGENT_IMPROVEMENT_LOOP_ENABLED=false`` turns the step off; the script can still be
run by hand (``--force`` ignores the schedule).
"""

from __future__ import annotations

from datetime import datetime, time
import logging
from pathlib import Path
import subprocess
from typing import Any, Callable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

KST = ZoneInfo('Asia/Seoul')
REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT = REPO_ROOT / 'scripts' / 'run_agent_improvement_local.sh'
OUT_DIR = REPO_ROOT / 'var' / 'alpha-lab'
RUN_DAY = 5  # Saturday (datetime.weekday)
RUN_FROM = time(9, 0)
# Signal lab, findings and a Codex edit together normally finish well within this.
RUN_TIMEOUT_SECONDS = 2 * 60 * 60


def iso_week_label(moment: datetime) -> str:
    """Same label as the script's ``date +%G-W%V`` in Asia/Seoul."""

    year, week, _ = moment.astimezone(KST).isocalendar()
    return f'{year}-W{week:02d}'


def marker_path(moment: datetime, out_dir: Path = OUT_DIR) -> Path:
    return out_dir / f'.done-{iso_week_label(moment)}'


def is_due(moment: datetime, *, out_dir: Path = OUT_DIR) -> bool:
    local = moment.astimezone(KST)
    if local.weekday() != RUN_DAY or local.time() < RUN_FROM:
        return False
    return not marker_path(moment, out_dir).exists()


def run(
    *,
    now: datetime | None = None,
    out_dir: Path = OUT_DIR,
    script: Path = SCRIPT,
    runner: Callable[..., Any] = subprocess.run,
) -> dict[str, Any]:
    """Start the weekly loop when due; blocking, meant for a worker thread."""

    moment = now or datetime.now(KST)
    if not is_due(moment, out_dir=out_dir):
        return {'status': 'not_due'}
    week = iso_week_label(moment)
    try:
        completed = runner(
            ['/bin/bash', str(script)],
            cwd=str(script.parents[1]),
            capture_output=True,
            text=True,
            timeout=RUN_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        logger.warning('Agent improvement loop for %s timed out after %ss', week, RUN_TIMEOUT_SECONDS)
        return {'status': 'timed_out', 'week': week}
    done = marker_path(moment, out_dir).exists()
    result = {'status': 'completed' if done else 'failed', 'week': week, 'returncode': completed.returncode}
    if done:
        logger.info('Agent improvement loop for %s finished (see var/alpha-lab/loop-*.log)', week)
    else:
        logger.warning(
            'Agent improvement loop for %s did not finish (exit %s); it is retried on the next hourly check. '
            'See var/alpha-lab/last-*.log',
            week,
            completed.returncode,
        )
    return result
