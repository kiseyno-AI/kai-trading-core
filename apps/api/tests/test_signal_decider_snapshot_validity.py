from datetime import date, datetime, timezone
from types import SimpleNamespace

from app.agent_context.signal_decider_snapshot_validity import (
    decision_snapshot_observed_at_utc,
    decision_snapshot_target_open_at_utc,
    is_decision_snapshot_pre_target_open,
    is_decision_snapshot_valid_for_source_date,
)


def test_snapshot_observed_exactly_at_target_open_is_invalid() -> None:
    snapshot = SimpleNamespace(
        feedback_received_at=datetime(2026, 4, 30, 0, 0, tzinfo=timezone.utc),
        created_at=datetime(2026, 4, 29, 23, 59, tzinfo=timezone.utc),
    )

    assert not is_decision_snapshot_pre_target_open(
        snapshot,
        target_trade_date=date(2026, 4, 30),
    )


def test_target_open_is_exposed_as_an_aware_utc_timestamp() -> None:
    assert decision_snapshot_target_open_at_utc(date(2026, 4, 30)) == datetime(
        2026,
        4,
        30,
        0,
        0,
        tzinfo=timezone.utc,
    )


def test_created_at_is_used_only_when_feedback_observation_is_missing() -> None:
    created_at = datetime(2026, 4, 29, 23, 59, tzinfo=timezone.utc)
    snapshot = {
        'feedback_received_at': None,
        'created_at': created_at,
    }

    assert decision_snapshot_observed_at_utc(snapshot) == created_at


def test_naive_feedback_observation_fails_closed_without_created_at_fallback() -> None:
    snapshot = SimpleNamespace(
        feedback_received_at=datetime(2026, 4, 29, 23, 59),
        created_at=datetime(2026, 4, 29, 22, 0, tzinfo=timezone.utc),
    )

    assert decision_snapshot_observed_at_utc(snapshot) is None
    assert not is_decision_snapshot_pre_target_open(
        snapshot,
        target_trade_date=date(2026, 4, 30),
    )


def test_source_date_validity_uses_the_next_krx_trading_session() -> None:
    snapshot = SimpleNamespace(
        market_date=date(2026, 5, 1),
        feedback_received_at=datetime(2026, 5, 3, 23, 59, tzinfo=timezone.utc),
        created_at=datetime(2026, 5, 3, 23, 58, tzinfo=timezone.utc),
    )
    observed_source_dates: list[date] = []

    def resolve_next_session(source_market_date: date) -> date:
        observed_source_dates.append(source_market_date)
        return date(2026, 5, 4)

    assert is_decision_snapshot_valid_for_source_date(
        snapshot,
        next_trading_date_resolver=resolve_next_session,
    )
    assert observed_source_dates == [date(2026, 5, 1)]
