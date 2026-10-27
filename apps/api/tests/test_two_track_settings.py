from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.core.settings import Settings


@pytest.mark.parametrize('gate_mode', ['hard_block', 'downsize_probe'])
def test_dispatch_runtime_rejects_cross_lane_alignment_enforcement(
    gate_mode: str,
) -> None:
    with pytest.raises(
        ValidationError,
        match='dispatch mode requires independent decider lanes',
    ):
        Settings(
            _env_file=None,
            signal_decider_exec_integration_mode='dispatch',
            signal_decider_wait_conflict_gate_mode=gate_mode,
        )


@pytest.mark.parametrize('gate_mode', ['disabled', 'report_only'])
def test_dispatch_runtime_allows_non_enforcing_alignment_observation(
    gate_mode: str,
) -> None:
    configured = Settings(
        _env_file=None,
        signal_decider_exec_integration_mode='dispatch',
        signal_decider_wait_conflict_gate_mode=gate_mode,
    )

    assert configured.signal_decider_wait_conflict_gate_mode == gate_mode


@pytest.mark.parametrize(
    ('integration_mode', 'gate_mode'),
    [
        ('disabled', 'hard_block'),
        ('disabled', 'downsize_probe'),
        ('observe', 'hard_block'),
        ('observe', 'downsize_probe'),
    ],
)
def test_non_dispatch_runtime_preserves_alignment_evaluation_modes(
    integration_mode: str,
    gate_mode: str,
) -> None:
    configured = Settings(
        _env_file=None,
        signal_decider_exec_integration_mode=integration_mode,
        signal_decider_wait_conflict_gate_mode=gate_mode,
    )

    assert configured.signal_decider_wait_conflict_gate_mode == gate_mode


@pytest.mark.parametrize(
    ('overrides', 'message'),
    [
        (
            {
                'signal_decider_wait_recheck_evidence_window_seconds': 5,
                'signal_decider_wait_recheck_evidence_min_span_seconds': 6,
            },
            'MIN_SPAN_SECONDS cannot exceed',
        ),
        (
            {
                'signal_decider_wait_recheck_evidence_max_samples': 3,
                'signal_decider_wait_recheck_evidence_min_samples': 4,
            },
            'MIN_SAMPLES cannot exceed',
        ),
    ],
)
def test_wait_recheck_rejects_impossible_evidence_windows(
    overrides: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        Settings(_env_file=None, **overrides)
