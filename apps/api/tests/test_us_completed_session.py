from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pytest

from app.market.us_session import latest_completed_us_session


def test_selects_the_closed_session_and_its_previous_session() -> None:
    result = latest_completed_us_session(as_of=datetime(2026, 11, 24, 22, tzinfo=UTC))

    assert result.session_date == date(2026, 11, 24)
    assert result.close_at == datetime(2026, 11, 24, 21, tzinfo=UTC)
    assert result.previous_session_date == date(2026, 11, 23)
    assert result.previous_close_at == datetime(2026, 11, 23, 21, tzinfo=UTC)


def test_does_not_select_a_session_just_before_its_close() -> None:
    result = latest_completed_us_session(
        as_of=datetime(2026, 11, 24, 20, 59, 59, 999999, tzinfo=UTC),
    )

    assert result.session_date == date(2026, 11, 23)
    assert result.close_at == datetime(2026, 11, 23, 21, tzinfo=UTC)
    assert result.previous_session_date == date(2026, 11, 20)


def test_rejects_a_timestamp_without_a_timezone() -> None:
    with pytest.raises(ValueError, match='timezone-aware'):
        latest_completed_us_session(as_of=datetime(2026, 11, 24, 22))


@pytest.mark.parametrize(
    ('as_of', 'session_date', 'close_at', 'previous_session_date', 'previous_close_at'),
    [
        ('2026-11-24T21:00:00+00:00', '2026-11-24', '2026-11-24T21:00:00+00:00',
         '2026-11-23', '2026-11-23T21:00:00+00:00'),
        ('2026-11-24T05:59:59.999999+09:00', '2026-11-20', '2026-11-20T21:00:00+00:00',
         '2026-11-19', '2026-11-19T21:00:00+00:00'),
        ('2026-11-24T06:00:00+09:00', '2026-11-23', '2026-11-23T21:00:00+00:00',
         '2026-11-20', '2026-11-20T21:00:00+00:00'),
        ('2026-11-26T23:00:00+00:00', '2026-11-25', '2026-11-25T21:00:00+00:00',
         '2026-11-24', '2026-11-24T21:00:00+00:00'),
        ('2026-11-27T17:59:59.999999+00:00', '2026-11-25', '2026-11-25T21:00:00+00:00',
         '2026-11-24', '2026-11-24T21:00:00+00:00'),
        ('2026-11-27T18:00:00+00:00', '2026-11-27', '2026-11-27T18:00:00+00:00',
         '2026-11-25', '2026-11-25T21:00:00+00:00'),
        ('2026-11-30T07:00:00+09:00', '2026-11-27', '2026-11-27T18:00:00+00:00',
         '2026-11-25', '2026-11-25T21:00:00+00:00'),
        ('2026-03-09T19:59:59.999999+00:00', '2026-03-06', '2026-03-06T21:00:00+00:00',
         '2026-03-05', '2026-03-05T21:00:00+00:00'),
        ('2026-03-09T20:00:00+00:00', '2026-03-09', '2026-03-09T20:00:00+00:00',
         '2026-03-06', '2026-03-06T21:00:00+00:00'),
        ('2026-11-02T20:00:00+00:00', '2026-10-30', '2026-10-30T20:00:00+00:00',
         '2026-10-29', '2026-10-29T20:00:00+00:00'),
        ('2026-11-02T21:00:00+00:00', '2026-11-02', '2026-11-02T21:00:00+00:00',
         '2026-10-30', '2026-10-30T20:00:00+00:00'),
    ],
    ids=[
        'regular-exact-close', 'before-us-close-in-seoul', 'at-us-close-in-seoul',
        'thanksgiving', 'before-early-close', 'at-early-close', 'monday-krx-preopen',
        'before-spring-dst-close', 'spring-dst-close', 'before-fall-dst-close', 'fall-dst-close',
    ],
)
def test_uses_exchange_closes_across_calendar_boundaries(
    as_of: str, session_date: str, close_at: str,
    previous_session_date: str, previous_close_at: str,
) -> None:
    result = latest_completed_us_session(as_of=datetime.fromisoformat(as_of))

    assert result.session_date == date.fromisoformat(session_date)
    assert result.close_at == datetime.fromisoformat(close_at)
    assert result.previous_session_date == date.fromisoformat(previous_session_date)
    assert result.previous_close_at == datetime.fromisoformat(previous_close_at)


@pytest.mark.parametrize('timezone', ['UTC', 'America/New_York', 'Asia/Seoul'])
def test_equivalent_instants_select_the_same_session(timezone: str) -> None:
    as_of = datetime(2026, 11, 24, 22, tzinfo=UTC).astimezone(ZoneInfo(timezone))

    result = latest_completed_us_session(as_of=as_of)

    assert result.session_date == date(2026, 11, 24)
    assert result.close_at == datetime(2026, 11, 24, 21, tzinfo=UTC)


@pytest.mark.parametrize('as_of', [None, '2026-11-24T22:00:00Z', date(2026, 11, 24)])
def test_rejects_inputs_that_are_not_datetimes(as_of) -> None:
    with pytest.raises(ValueError, match='timezone-aware datetime'):
        latest_completed_us_session(as_of=as_of)


@pytest.mark.parametrize('year', [1900, 2100])
def test_calendar_bounds_do_not_fall_back_to_weekdays(year: int) -> None:
    with pytest.raises(ValueError):
        latest_completed_us_session(as_of=datetime(year, 1, 15, 22, tzinfo=UTC))
