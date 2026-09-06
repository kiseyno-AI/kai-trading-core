from app.orders.signal_decider_alignment_gate import (
    apply_alignment_quantity_enforcement,
    build_signal_decider_alignment_gate,
)


def _strong_override_quality() -> dict:
    return {
        'buy_quality_score': 76,
        'return_percentile': 0.82,
        'volume_percentile': 0.68,
        'reward_risk_ratio': 2.5,
        'raw_trading_value_krw': 9_000_000_000,
        'continuation_setup_detected': True,
    }


def test_hard_block_enforces_zero_quantity_for_wait_conflict() -> None:
    gate = build_signal_decider_alignment_gate(
        signal_decider_context={'action': 'wait', 'market_date': '2026-09-04'},
        short_term_entry_quality={},
        tactical_action='watch_for_trigger',
        mode='hard_block',
        expected_market_date='2026-09-04',
    )

    enforced = apply_alignment_quantity_enforcement(gate, requested_quantity=30, probe_quantity=1)

    assert enforced['alignment_gate_action'] == 'block'
    assert enforced['enforcement_decision'] == 'blocked'
    assert enforced['requested_quantity'] == 30
    assert enforced['final_quantity'] == 0


def test_downsize_probe_is_deterministic_and_never_increases_quantity() -> None:
    gate = build_signal_decider_alignment_gate(
        signal_decider_context={'action': 'wait', 'market_date': '2026-09-04'},
        short_term_entry_quality={},
        tactical_action='watch_for_trigger',
        mode='downsize_probe',
        expected_market_date='2026-09-04',
    )

    large = apply_alignment_quantity_enforcement(gate, requested_quantity=30, probe_quantity=2)
    small = apply_alignment_quantity_enforcement(gate, requested_quantity=1, probe_quantity=2)

    assert large['enforcement_decision'] == 'downsized'
    assert large['final_quantity'] == 2
    assert small['final_quantity'] == 1


def test_valid_wait_override_survives_hard_block_mode() -> None:
    gate = build_signal_decider_alignment_gate(
        signal_decider_context={'action': 'wait', 'market_date': '2026-09-04'},
        short_term_entry_quality=_strong_override_quality(),
        tactical_action='watch_for_trigger',
        mode='hard_block',
        expected_market_date='2026-09-04',
    )

    enforced = apply_alignment_quantity_enforcement(gate, requested_quantity=30)

    assert enforced['signal_decider_alignment'] == 'wait_conflict_override'
    assert enforced['override']['allowed'] is True
    assert enforced['enforcement_decision'] == 'allowed_by_override'
    assert enforced['final_quantity'] == 30


def test_missing_and_stale_context_never_silently_approve() -> None:
    missing = build_signal_decider_alignment_gate(
        signal_decider_context=None,
        short_term_entry_quality=_strong_override_quality(),
        tactical_action='watch_for_trigger',
        mode='hard_block',
        expected_market_date='2026-09-04',
    )
    stale = build_signal_decider_alignment_gate(
        signal_decider_context={'action': 'buy', 'market_date': '2026-09-03'},
        short_term_entry_quality=_strong_override_quality(),
        tactical_action='watch_for_trigger',
        mode='hard_block',
        expected_market_date='2026-09-04',
    )
    missing_action = build_signal_decider_alignment_gate(
        signal_decider_context={'market_date': '2026-09-04'},
        short_term_entry_quality=_strong_override_quality(),
        tactical_action='watch_for_trigger',
        mode='hard_block',
        expected_market_date='2026-09-04',
    )

    assert missing['signal_decider_alignment'] == 'no_signal_decider_context'
    assert missing['alignment_gate_action'] == 'block'
    assert stale['signal_decider_alignment'] == 'stale_signal_decider_context'
    assert stale['alignment_gate_action'] == 'block'
    assert missing_action['signal_decider_alignment'] == 'no_signal_decider_context'
    assert missing_action['alignment_gate_action'] == 'block'


def test_report_only_records_candidate_without_changing_quantity() -> None:
    gate = build_signal_decider_alignment_gate(
        signal_decider_context={'action': 'wait', 'market_date': '2026-09-04'},
        short_term_entry_quality={},
        tactical_action='watch_for_trigger',
        mode='report_only',
        expected_market_date='2026-09-04',
    )

    enforced = apply_alignment_quantity_enforcement(gate, requested_quantity=30)

    assert enforced['alignment_gate_action'] == 'report_only_block_candidate'
    assert enforced['enforcement_decision'] == 'allowed_report_only'
    assert enforced['requested_quantity'] == 30
    assert enforced['final_quantity'] == 30
