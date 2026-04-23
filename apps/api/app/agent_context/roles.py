from __future__ import annotations

SUMMARIZER_AGENT_NAMES: tuple[str, ...] = (
    'eod-summarizer',
    'technical-summarizer',
    'fundamental-summarizer',
)

SIGNAL_DECIDER_AGENT_NAME = 'signal-decider'

DEFAULT_AGENT_NAMES: tuple[str, ...] = SUMMARIZER_AGENT_NAMES + (SIGNAL_DECIDER_AGENT_NAME,)
