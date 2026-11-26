from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from app.market import us_index_prices, us_session


CLOSE_DATES = (
    '2026-10-26', '2026-10-27', '2026-10-28', '2026-10-29', '2026-10-30',
    '2026-11-02', '2026-11-03', '2026-11-04', '2026-11-05', '2026-11-06',
    '2026-11-09', '2026-11-10', '2026-11-11', '2026-11-12', '2026-11-13',
    '2026-11-16', '2026-11-17', '2026-11-18', '2026-11-19', '2026-11-20',
    '2026-11-23', '2026-11-24',
)


@pytest.fixture
def current_session():
    return us_session.latest_completed_us_session(as_of=datetime(2026, 11, 24, 22, tzinfo=UTC))


@pytest.fixture
def payload():
    rows = [
        {'stck_bsop_date': day.replace('-', ''), 'ovrs_nmix_prpr': f'{7000 + offset}.123456789012345678'}
        for offset, day in enumerate(CLOSE_DATES)
    ]
    return {
        'rt_cd': '0',
        'output1': {'stck_shrn_iscd': 'SPX', 'ovrs_nmix_prpr': '9999'},
        'output2': [*rows[::2], {'stck_bsop_date': '20261125', 'ovrs_nmix_prpr': None}, *rows[1::2]],
    }


@pytest.fixture
def captured_prices(payload, current_session):
    return (
        us_index_prices.validate_kis_us_index_prices(payload, index_code='SPX', session=current_session),
        us_index_prices.validate_kis_us_index_history(payload, index_code='SPX', session=current_session),
    )


def test_prior_windows_require_22_closes_and_preserve_the_dst_transition() -> None:
    current = us_session.latest_completed_us_session(as_of=datetime(2026, 11, 24, 22, tzinfo=UTC))

    history = us_session.prior_completed_us_sessions(current)

    assert len(history) == 20
    assert tuple(window.session_date.isoformat() for window in history) == CLOSE_DATES[1:-1]
    assert history[0].previous_session_date == date(2026, 10, 26)
    assert history[4].previous_close_at == datetime(2026, 10, 30, 20, tzinfo=UTC)
    assert history[4].close_at == datetime(2026, 11, 2, 21, tzinfo=UTC)
    assert history[-1].session_date == current.previous_session_date
    assert history[-1].close_at == current.previous_close_at


def test_capture_selects_20_exact_prior_pairs_from_unordered_daily_rows(payload, current_session) -> None:
    before = deepcopy(payload)

    history = us_index_prices.validate_kis_us_index_history(
        payload, index_code=' spx ', session=current_session,
    )

    assert len(history) == 20
    assert tuple(item.session.session_date.isoformat() for item in history) == CLOSE_DATES[1:-1]
    assert history[0].index_code == 'SPX'
    assert history[0].price == Decimal('7001.123456789012345678')
    assert history[0].previous_price == Decimal('7000.123456789012345678')
    assert history[-1].price == Decimal('7020.123456789012345678')
    assert history[-1].previous_price == Decimal('7019.123456789012345678')
    assert payload == before


@pytest.mark.parametrize('missing_date', CLOSE_DATES)
def test_capture_requires_every_date_without_substituting_older_rows(
    payload, current_session, missing_date,
) -> None:
    compact_date = missing_date.replace('-', '')
    payload['output2'] = [row for row in payload['output2'] if row['stck_bsop_date'] != compact_date]
    payload['output2'].append({'stck_bsop_date': '20261023', 'ovrs_nmix_prpr': '6999'})

    with pytest.raises(us_index_prices.USIndexPricesUnavailable, match=compact_date):
        us_index_prices.validate_kis_us_index_history(payload, index_code='SPX', session=current_session)


@pytest.mark.parametrize('duplicate_date', ['20261026', '20261109', '20261123', '20261124'])
@pytest.mark.parametrize('conflicting', [False, True])
def test_capture_rejects_required_duplicate_dates_even_when_prices_match(
    payload, current_session, duplicate_date, conflicting,
) -> None:
    row = next(row for row in payload['output2'] if row['stck_bsop_date'] == duplicate_date)
    payload['output2'].append(dict(row, ovrs_nmix_prpr='9999' if conflicting else row['ovrs_nmix_prpr']))

    with pytest.raises(ValueError, match='duplicate'):
        us_index_prices.validate_kis_us_index_history(payload, index_code='SPX', session=current_session)


@pytest.mark.parametrize('required_date', ['20261026', '20261109', '20261123', '20261124'])
@pytest.mark.parametrize('bad_price', [None, 0, 'NaN', 'not a price'])
def test_capture_rejects_malformed_prices_in_historical_and_current_pairs(
    payload, current_session, required_date, bad_price,
) -> None:
    row = next(row for row in payload['output2'] if row['stck_bsop_date'] == required_date)
    row['ovrs_nmix_prpr'] = bad_price

    with pytest.raises(ValueError, match='positive finite'):
        us_index_prices.validate_kis_us_index_history(payload, index_code='SPX', session=current_session)


@pytest.mark.parametrize('row', [None, 'not a row', {'stck_bsop_date': '20260230'}, {'stck_bsop_date': 20261026}])
def test_capture_rejects_malformed_rows_instead_of_treating_them_as_missing(payload, current_session, row) -> None:
    payload['output2'].append(row)

    with pytest.raises(ValueError, match='row'):
        us_index_prices.validate_kis_us_index_history(payload, index_code='SPX', session=current_session)


@pytest.mark.parametrize(('missing_date', 'bad_date'), [
    ('20261124', '20261026'), ('20261026', '20261109'),
])
@pytest.mark.parametrize('invalid_kind', ['duplicate', 'malformed_price'])
def test_malformed_required_history_is_not_hidden_by_another_missing_date(
    payload, current_session, missing_date, bad_date, invalid_kind,
) -> None:
    payload['output2'] = [row for row in payload['output2'] if row['stck_bsop_date'] != missing_date]
    row = next(row for row in payload['output2'] if row['stck_bsop_date'] == bad_date)
    if invalid_kind == 'duplicate':
        payload['output2'].append(dict(row))
    else:
        row['ovrs_nmix_prpr'] = 'NaN'

    with pytest.raises(ValueError):
        us_index_prices.validate_kis_us_index_history(payload, index_code='SPX', session=current_session)


def test_prior_windows_preserve_thanksgiving_and_its_early_close() -> None:
    current = us_session.latest_completed_us_session(as_of=datetime(2026, 11, 30, 22, tzinfo=UTC))

    history = us_session.prior_completed_us_sessions(current)

    assert len(history) == 20
    assert history[-1] == us_session.CompletedUSSession(
        session_date=date(2026, 11, 27), close_at=datetime(2026, 11, 27, 18, tzinfo=UTC),
        previous_session_date=date(2026, 11, 25), previous_close_at=datetime(2026, 11, 25, 21, tzinfo=UTC),
    )
    assert history[-1].session_date == current.previous_session_date
    assert date(2026, 11, 26) not in tuple(window.session_date for window in history)


def test_capture_rejects_a_current_window_that_disagrees_with_the_calendar(payload, current_session) -> None:
    changed = replace(current_session, previous_close_at=datetime(2026, 11, 23, 20, tzinfo=UTC))

    with pytest.raises(ValueError, match='window'):
        us_index_prices.validate_kis_us_index_history(payload, index_code='SPX', session=changed)


def test_stored_history_replays_without_a_calendar(captured_prices, monkeypatch) -> None:
    current, history = captured_prices

    def unavailable_calendar(*args, **kwargs):
        raise AssertionError('Stored history must not query the exchange calendar')

    monkeypatch.setattr(us_session.exchange_calendars, 'get_calendar', unavailable_calendar)

    assert us_index_prices.validate_us_index_price_history(current, history) is None


@pytest.mark.parametrize('count', [0, 19, 21])
def test_stored_history_requires_exactly_20_pairs(captured_prices, count) -> None:
    current, history = captured_prices
    history = (*history, history[-1])[:count]

    with pytest.raises(ValueError, match='20'):
        us_index_prices.validate_us_index_price_history(current, history)


@pytest.mark.parametrize('position', [0, 9, 20], ids=['oldest', 'middle', 'current'])
@pytest.mark.parametrize('field', ['price', 'previous_price'])
@pytest.mark.parametrize('bad_price', [
    Decimal('0'), Decimal('-1'), Decimal('NaN'), Decimal('sNaN'), Decimal('Infinity'),
    '7000', 7000, True, None,
])
def test_stored_history_requires_finite_positive_decimal_prices(
    captured_prices, position, field, bad_price,
) -> None:
    current, history = captured_prices
    pairs = [*history, current]
    pairs[position] = replace(pairs[position], **{field: bad_price})

    with pytest.raises(ValueError, match='positive.*Decimal'):
        us_index_prices.validate_us_index_price_history(pairs[-1], pairs[:-1])


@pytest.mark.parametrize('position', [0, 9, 20], ids=['oldest', 'middle', 'current'])
def test_stored_history_requires_matching_index_identities(captured_prices, position) -> None:
    current, history = captured_prices
    pairs = [*history, current]
    pairs[position] = replace(pairs[position], index_code='COMP')

    with pytest.raises(ValueError, match='index'):
        us_index_prices.validate_us_index_price_history(pairs[-1], pairs[:-1])


@pytest.mark.parametrize('position', [0, 9, 20], ids=['oldest', 'middle', 'current'])
@pytest.mark.parametrize('field', ['close_at', 'previous_close_at'])
def test_all_frozen_closes_require_timezone_aware_timestamps(captured_prices, position, field) -> None:
    current, history = captured_prices
    pairs = [*history, current]
    window = pairs[position].session
    window = replace(window, **{field: getattr(window, field).replace(tzinfo=None)})
    pairs[position] = replace(pairs[position], session=window)

    with pytest.raises(ValueError, match='timezone-aware'):
        us_index_prices.validate_us_index_price_history(pairs[-1], pairs[:-1])


@pytest.mark.parametrize('changes', [
    {'close_at': datetime(2026, 11, 23, 21, tzinfo=UTC)},
    {'previous_close_at': datetime(2026, 11, 24, 21, tzinfo=UTC)},
    {'session_date': date(2026, 11, 23)},
    {'previous_session_date': date(2026, 11, 24)},
    {'session_date': datetime(2026, 11, 24, tzinfo=UTC)},
    {'previous_session_date': None},
    {'close_at': datetime(2026, 11, 25, 21, tzinfo=UTC)},
    {'previous_close_at': datetime(2026, 11, 22, 21, tzinfo=UTC)},
])
def test_frozen_windows_require_ordered_dates_and_matching_close_dates(captured_prices, changes) -> None:
    current, history = captured_prices
    current = replace(current, session=replace(current.session, **changes))

    with pytest.raises(ValueError, match='window'):
        us_index_prices.validate_us_index_price_history(current, history)


@pytest.mark.parametrize('position', [0, 9, 19])
def test_overlapping_historical_closes_must_agree_exactly(captured_prices, position) -> None:
    current, history = captured_prices
    history = list(history)
    history[position] = replace(history[position], price=Decimal('7500'))

    with pytest.raises(ValueError, match='price continuity'):
        us_index_prices.validate_us_index_price_history(current, history)


def test_current_previous_close_must_agree_with_the_last_history_price(captured_prices) -> None:
    current, history = captured_prices
    current = replace(current, previous_price=Decimal('7500'))

    with pytest.raises(ValueError, match='price continuity'):
        us_index_prices.validate_us_index_price_history(current, history)


@pytest.mark.parametrize('change', ['reversed', 'duplicate', 'gap', 'current'])
def test_stored_history_requires_distinct_oldest_first_consecutive_pairs(captured_prices, change) -> None:
    current, history = captured_prices
    history = list(history)
    if change == 'reversed':
        history.reverse()
    elif change == 'duplicate':
        history[9] = history[8]
    elif change == 'gap':
        history[9] = replace(history[9], session=replace(
            history[9].session, previous_session_date=date(2026, 11, 5),
            previous_close_at=datetime(2026, 11, 5, 21, tzinfo=UTC),
        ))
    else:
        history[-1] = current

    with pytest.raises(ValueError, match='window|consecutive'):
        us_index_prices.validate_us_index_price_history(current, history)


def test_overlapping_frozen_close_timestamps_must_agree(captured_prices) -> None:
    current, history = captured_prices
    current = replace(current, session=replace(current.session, previous_close_at=datetime(2026, 11, 23, 20, tzinfo=UTC)))

    with pytest.raises(ValueError, match='window'):
        us_index_prices.validate_us_index_price_history(current, history)
