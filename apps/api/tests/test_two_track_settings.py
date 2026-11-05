from __future__ import annotations

from app.core.settings import Settings


def test_settings_have_no_cross_lane_alignment_or_wait_recheck_runtime_fields() -> None:
    field_names = set(Settings.model_fields)

    assert 'signal_decider_wait_conflict_gate_mode' not in field_names
    assert 'signal_decider_wait_conflict_probe_quantity' not in field_names
    assert not {
        name
        for name in field_names
        if name.startswith('signal_decider_wait_recheck_')
    }
