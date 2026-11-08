"""When a partial upstream agent stage may start the next agent stage.

The daily agent chain is summaries -> deciders -> next-open plan. Two executors
run it and, for the summaries -> deciders edge, they decide differently; both
rules live here, side by side, so changing either is a change in one place:

* ``finished_partial_feeds_downstream`` -- the follow-up chain of the first run
  (``pykrx_pipeline_runner``): a ``partial`` upstream run that has finished
  (nothing pending or in flight) with at least one completed symbol may feed the
  next stage, which then fails only the symbols without their own upstream rows.
* ``summary_partial_dependency_is_proven`` -- the stage executor used by resume
  runs (``daily_pipeline_stages``): a ``partial`` summary run counts only with
  an exact lineage proof (the resolved knowledge cutoff names that run and its
  completed-summarizer intersection matches the parent scope).

A fully satisfied upstream outcome passes in both executors; that check stays
in ``daily_pipeline_outcomes.daily_pipeline_outcome_is_satisfied``.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from app.eod.summary_cutoff_contract import completed_summary_scope_matches_parent


def finished_partial_feeds_downstream(result: Mapping[str, Any], *, status: str) -> bool:
    """First-run rule for a ``partial`` upstream agent run (``status`` as resolved by the caller)."""

    if status != 'partial':
        return False
    progress = result.get('progress') if isinstance(result.get('progress'), dict) else {}
    summary = progress.get('summary') if isinstance(progress.get('summary'), dict) else {}

    def _count(key: str) -> int:
        try:
            return int(summary.get(key) or 0)
        except (TypeError, ValueError):
            return 0

    return (
        summary.get('is_live') is not True
        and _count('completed_symbols') > 0
        and _count('pending_symbols') == 0
        and _count('in_progress_symbols') == 0
    )


def summary_partial_dependency_is_proven(
    *,
    outcome: dict[str, Any] | None,
    cutoff_resolution: dict[str, Any] | None,
    parent_symbols: Sequence[str] | None,
) -> bool:
    """Resume-path rule: accept only an exact finalized success intersection as input."""

    if not (
        isinstance(outcome, dict)
        and str(outcome.get('status') or '').strip().lower()
        == 'partial'
        and isinstance(cutoff_resolution, dict)
        and cutoff_resolution.get('ready') is True
        and cutoff_resolution.get('knowledge_cutoff_at') is not None
    ):
        return False
    result = outcome.get('result')
    if not isinstance(result, dict):
        return False
    source_run_id = str(
        cutoff_resolution.get('source_run_id') or ''
    ).strip()
    completed_symbols = cutoff_resolution.get('requested_symbols')
    return bool(
        source_run_id
        and source_run_id == str(result.get('run_id') or '').strip()
        and isinstance(completed_symbols, list)
        and completed_symbols
        and completed_summary_scope_matches_parent(
            feedback_scope_mode=cutoff_resolution.get(
                'feedback_scope_mode'
            ),
            completed_symbols=completed_symbols,
            completed_scope_proof=cutoff_resolution.get(
                'completed_scope_proof'
            ),
            parent_symbols=parent_symbols,
        )
    )
