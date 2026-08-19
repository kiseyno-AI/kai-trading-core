from datetime import UTC, date, datetime

from app.us_equities.scheduler import is_us_pipeline_due


def test_us_pipeline_due_uses_new_york_clock_independent_of_krx() -> None:
    # 22:00 UTC is 18:00 EDT and 07:00 the next day in Seoul.
    assert is_us_pipeline_due(
        now=datetime(2026, 8, 19, 22, 0, tzinfo=UTC),
        last_completed_market_date=None,
        scheduled_local_time='18:00',
    )


def test_us_pipeline_not_due_twice_or_on_exchange_holiday() -> None:
    assert not is_us_pipeline_due(
        now=datetime(2026, 8, 19, 23, 0, tzinfo=UTC),
        last_completed_market_date=date(2026, 8, 19),
        scheduled_local_time='18:00',
    )
    assert not is_us_pipeline_due(
        now=datetime(2026, 7, 3, 23, 0, tzinfo=UTC),
        last_completed_market_date=None,
        scheduled_local_time='18:00',
    )
