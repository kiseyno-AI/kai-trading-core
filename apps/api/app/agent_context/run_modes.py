from __future__ import annotations

DAILY_RETRY_CYCLE_MODE = 'daily_retry_cycle_role_sequenced'
FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH = 'decider_scope_fast_path'
FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE = 'broad_feedback_universe'
FEEDBACK_SCOPE_MODES = (
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
)


def normalize_feedback_scope_mode(value: str | None) -> str:
    resolved = str(value or FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH).strip()
    if resolved not in FEEDBACK_SCOPE_MODES:
        raise ValueError(
            'feedback_scope_mode must be one of: '
            + ', '.join(FEEDBACK_SCOPE_MODES)
        )
    return resolved
