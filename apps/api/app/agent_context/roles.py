from __future__ import annotations

SUMMARIZER_AGENT_NAMES: tuple[str, ...] = (
    'eod-summarizer',
    'technical-summarizer',
    'fundamental-summarizer',
)

SIGNAL_DECIDER_AGENT_NAME = 'signal-decider'
SHORT_TERM_DECIDER_AGENT_NAME = 'short-term-decider'

DECISION_SUPPORT_AGENT_NAMES: tuple[str, ...] = (SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME)
DEFAULT_AGENT_NAMES: tuple[str, ...] = SUMMARIZER_AGENT_NAMES + DECISION_SUPPORT_AGENT_NAMES
