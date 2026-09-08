from __future__ import annotations

from typing import Any

SUMMARIZER_AGENT_NAMES: tuple[str, ...] = (
    'eod-summarizer',
    'technical-summarizer',
    'fundamental-summarizer',
)

SIGNAL_DECIDER_AGENT_NAME = 'signal-decider'
SHORT_TERM_DECIDER_AGENT_NAME = 'short-term-decider'

SHORT_TERM_DECIDER_TACTICAL_ACTIONS: frozenset[str] = frozenset(
    {
        'scalp_buy_candidate',
        'watch_for_trigger',
        'avoid_chase',
        'wait',
        'reject',
    }
)

DECISION_SUPPORT_AGENT_NAMES: tuple[str, ...] = (SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME)
DEFAULT_AGENT_NAMES: tuple[str, ...] = SUMMARIZER_AGENT_NAMES + DECISION_SUPPORT_AGENT_NAMES


def short_term_tactical_action_to_decision_action(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    tactical_action = value.strip().lower()
    if tactical_action not in SHORT_TERM_DECIDER_TACTICAL_ACTIONS:
        return None
    if tactical_action == 'scalp_buy_candidate':
        return 'buy'
    if tactical_action == 'reject':
        return 'reject'
    return 'wait'


def canonical_evaluation_action(
    *,
    agent_name: str,
    persisted_action: Any,
    response_payload: Any,
) -> str | None:
    """Return the action whose economic outcome may be evaluated.

    Short-term snapshots historically stored observation-only tactical states as
    generic buys. Their explicit tactical action is therefore authoritative.
    ``None`` means that the stored payload cannot safely enter learning/replay.
    """
    if agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
        if not isinstance(response_payload, dict):
            return None
        tactical_action = response_payload.get('tactical_action')
        if tactical_action is None:
            tactical_action = response_payload.get('preferred_action')
        return short_term_tactical_action_to_decision_action(tactical_action)

    if not isinstance(persisted_action, str):
        return None
    normalized_action = persisted_action.strip().lower()
    if normalized_action not in {'buy', 'wait', 'reject'}:
        return None
    return normalized_action
