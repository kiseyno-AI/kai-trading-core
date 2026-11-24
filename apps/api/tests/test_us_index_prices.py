from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.market.us_index_prices import validate_kis_us_index_prices
from app.market.us_session import latest_completed_us_session


@pytest.fixture
def session():
    return latest_completed_us_session(as_of=datetime(2026, 11, 24, 16, tzinfo=UTC))


@pytest.fixture
def payload():
    return {
        'rt_cd': '0',
        'output1': {
            'stck_shrn_iscd': 'SPX',
            'ovrs_nmix_prpr': '9999',
            'ovrs_nmix_prdy_clpr': '8888',
        },
        'output2': [
            {'stck_bsop_date': '20261124', 'ovrs_nmix_prpr': None},
            {'stck_bsop_date': '20261120', 'ovrs_nmix_prpr': '7000.000001'},
            {'stck_bsop_date': '20261123', 'ovrs_nmix_prpr': '7100.123456789012345678'},
        ],
    }


def test_selects_exact_session_prices_instead_of_current_header_prices(payload, session) -> None:
    result = validate_kis_us_index_prices(payload, index_code=' spx ', session=session)

    assert result.index_code == 'SPX'
    assert result.session == session
    assert result.price == Decimal('7100.123456789012345678')
    assert result.previous_price == Decimal('7000.000001')


@pytest.mark.parametrize('bad_price', [
    0, -1, True, False, None, '', 'NaN', 'Infinity', '-Infinity',
    float('nan'), float('inf'), '1,2', {}, [],
])
@pytest.mark.parametrize('row_index', [1, 2])
def test_rejects_bad_prices_in_either_required_session(payload, session, row_index, bad_price) -> None:
    payload['output2'][row_index]['ovrs_nmix_prpr'] = bad_price

    with pytest.raises(ValueError, match='positive finite'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


@pytest.mark.parametrize('provider_code', ['COMP', 'SOX', '', None, True])
def test_rejects_missing_or_mismatched_index_identity(payload, session, provider_code) -> None:
    payload['output1']['stck_shrn_iscd'] = provider_code

    with pytest.raises(ValueError, match='identity'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


@pytest.mark.parametrize('index_code', ['', None, False, 'KOSPI', 'DAX'])
def test_rejects_unsupported_requested_indices(payload, session, index_code) -> None:
    with pytest.raises(ValueError, match='index_code'):
        validate_kis_us_index_prices(payload, index_code=index_code, session=session)


@pytest.mark.parametrize('row_index', [1, 2])
def test_rejects_duplicate_required_dates_even_when_prices_match(payload, session, row_index) -> None:
    payload['output2'].append(dict(payload['output2'][row_index]))

    with pytest.raises(ValueError, match='duplicate'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


@pytest.mark.parametrize('missing_date', ['20261120', '20261123'])
def test_missing_required_date_is_unavailable_without_using_an_older_row(payload, session, missing_date) -> None:
    payload['output2'] = [row for row in payload['output2'] if row['stck_bsop_date'] != missing_date]
    payload['output2'].append({'stck_bsop_date': '20261119', 'ovrs_nmix_prpr': '6800'})

    with pytest.raises(LookupError, match=missing_date):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


@pytest.mark.parametrize('empty_rows', [[], {}])
def test_successful_empty_response_is_unavailable(payload, session, empty_rows) -> None:
    payload.update(output1={}, output2=empty_rows)

    with pytest.raises(LookupError, match='20261123.*20261120'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


def test_single_provider_row_is_unavailable_for_the_other_required_date(payload, session) -> None:
    payload['output2'] = payload['output2'][2]

    with pytest.raises(LookupError, match='20261120'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


@pytest.mark.parametrize('result_code', ['1', None, 0, False])
@pytest.mark.parametrize('empty_rows', [False, True])
def test_rejects_non_success_result_codes_before_classifying_missing_data(
    payload, session, result_code, empty_rows,
) -> None:
    payload['rt_cd'] = result_code
    if empty_rows:
        payload.update(output1={}, output2=[])

    with pytest.raises(ValueError, match='rt_cd'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


@pytest.mark.parametrize('bad_date', [None, '', '2026112', '2026-11-23', '20260230', 20261123])
def test_rejects_malformed_business_dates(payload, session, bad_date) -> None:
    payload['output2'][0]['stck_bsop_date'] = bad_date

    with pytest.raises(ValueError, match='stck_bsop_date'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


def test_rejects_a_daily_row_that_is_not_an_object(payload, session) -> None:
    payload['output2'].append('not a daily row')

    with pytest.raises(ValueError, match='row'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


@pytest.mark.parametrize('bad_payload', [None, [], 'not a response'])
def test_rejects_responses_that_are_not_objects(session, bad_payload) -> None:
    with pytest.raises(ValueError, match='rt_cd'):
        validate_kis_us_index_prices(bad_payload, index_code='SPX', session=session)


@pytest.mark.parametrize('bad_rows', [None, False, 0, 'not daily rows'])
def test_rejects_malformed_row_containers(payload, session, bad_rows) -> None:
    payload['output2'] = bad_rows

    with pytest.raises(ValueError, match='output2'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


@pytest.mark.parametrize('header', [None, [], {}, 'not a header'])
def test_rejects_malformed_identity_headers(payload, session, header) -> None:
    payload['output1'] = header

    with pytest.raises(ValueError, match='identity'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


@pytest.mark.parametrize('row_index', [1, 2])
def test_rejects_missing_prices_in_required_rows(payload, session, row_index) -> None:
    payload['output2'][row_index].pop('ovrs_nmix_prpr')

    with pytest.raises(ValueError, match='positive finite'):
        validate_kis_us_index_prices(payload, index_code='SPX', session=session)


@pytest.mark.parametrize('index_code', ['SPX', 'COMP', 'SOX'])
def test_accepts_supported_index_identity_and_preserves_formatted_prices(payload, session, index_code) -> None:
    payload['output1']['stck_shrn_iscd'] = f' {index_code.lower()} '
    payload['output2'][2]['ovrs_nmix_prpr'] = ' 7,100.123456 '

    result = validate_kis_us_index_prices(payload, index_code=index_code, session=session)

    assert result.index_code == index_code
    assert result.price == Decimal('7100.123456')


def test_validation_does_not_mutate_the_provider_payload(payload, session) -> None:
    before = deepcopy(payload)

    validate_kis_us_index_prices(payload, index_code='SPX', session=session)

    assert payload == before
