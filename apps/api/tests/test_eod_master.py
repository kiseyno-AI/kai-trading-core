from __future__ import annotations

from contextlib import contextmanager
import pytest

from app.eod import master
from app.eod.master import _parse_master_lines


def _master_line(
    *,
    symbol: str,
    standard_code: str,
    name_ko: str,
    security_group: str,
    trailer_field_width: int,
    newline: bytes = b'\n',
) -> bytes:
    head = (
        symbol.encode('ascii').ljust(9)
        + standard_code.encode('ascii').ljust(12)
        + name_ko.encode('cp949').ljust(40)
    )
    trailer = security_group.encode('ascii').ljust(trailer_field_width)
    return head + trailer + newline


@contextmanager
def _available_master_lock(**_kwargs):
    yield True


@pytest.mark.parametrize('security_group', ['ST', 'RT', 'IF', 'MF', 'DR', 'FS'])
def test_parse_master_lines_keeps_only_stock_like_krx_groups(security_group: str) -> None:
    raw = b''.join(
        [
            _master_line(
                symbol='005930',
                standard_code='KR7005930003',
                name_ko='삼성전자',
                security_group=security_group,
                trailer_field_width=227,
            ),
            _master_line(
                symbol='069500',
                standard_code='KR7069500007',
                name_ko='KODEX 200',
                security_group='EF',
                trailer_field_width=227,
            ),
            _master_line(
                symbol='500067',
                standard_code='KRG500000067',
                name_ko='신한 ETN',
                security_group='EN',
                trailer_field_width=227,
            ),
        ]
    )

    rows = _parse_master_lines(raw, trailer_width=227, market='KOSPI')

    assert [(row.symbol, row.name_ko, row.security_group) for row in rows] == [
        ('005930', '삼성전자', security_group)
    ]
    assert rows[0].source_payload == {'security_group': security_group}


def test_parse_master_lines_rejects_non_numeric_and_non_six_digit_codes() -> None:
    raw = b''.join(
        [
            _master_line(
                symbol='00088K',
                standard_code='KRA00088K000',
                name_ko='주식관련권리',
                security_group='ST',
                trailer_field_width=221,
            ),
            _master_line(
                symbol='12345',
                standard_code='KR7000123456',
                name_ko='잘못된단축코드',
                security_group='ST',
                trailer_field_width=221,
            ),
        ]
    )

    rows = _parse_master_lines(raw, trailer_width=221, market='KOSDAQ')

    assert rows == []


def test_parse_master_lines_reads_trailer_when_final_line_has_no_newline() -> None:
    raw = _master_line(
        symbol='035900',
        standard_code='KR7035900000',
        name_ko='JYP Ent.',
        security_group='ST',
        trailer_field_width=221,
        newline=b'',
    )

    rows = _parse_master_lines(raw, trailer_width=221, market='KOSDAQ')

    assert [(row.symbol, row.name_ko, row.security_group) for row in rows] == [
        ('035900', 'JYP Ent.', 'ST')
    ]


@pytest.mark.parametrize(
    ('market', 'trailer_width', 'line'),
    [
        (
            'KOSPI',
            227,
            _master_line(
                symbol='005930',
                standard_code='KR7005930003',
                name_ko='삼성전자',
                security_group='ST',
                trailer_field_width=227,
                newline=b'',
            ),
        ),
        (
            'KOSDAQ',
            221,
            _master_line(
                symbol='900110',
                standard_code='HK0000057197',
                name_ko='딥커머스',
                security_group='FS',
                trailer_field_width=221,
                newline=b'',
            ),
        ),
    ],
)
def test_parse_real_format_golden_rows(
    market: str,
    trailer_width: int,
    line: bytes,
) -> None:
    rows = _parse_master_lines(line, trailer_width=trailer_width, market=market)

    assert len(rows) == 1
    assert rows[0].market == market


@pytest.mark.parametrize('standard_code', ['', 'KR7005930004', 'NOT-AN-ISIN'])
def test_parse_master_lines_fails_closed_on_invalid_standard_code(
    standard_code: str,
) -> None:
    raw = _master_line(
        symbol='005930',
        standard_code=standard_code,
        name_ko='삼성전자',
        security_group='ST',
        trailer_field_width=227,
    )

    with pytest.raises(ValueError, match='invalid stock identity fields'):
        _parse_master_lines(raw, trailer_width=227, market='KOSPI')


def test_parse_master_lines_fails_closed_on_truncated_nonempty_line() -> None:
    with pytest.raises(ValueError, match='shorter than its trailer'):
        _parse_master_lines(b'not-empty', trailer_width=227, market='KOSPI')


def test_parse_master_lines_fails_closed_on_empty_numeric_stock_name() -> None:
    raw = _master_line(
        symbol='005930',
        standard_code='KR7005930003',
        name_ko='',
        security_group='ST',
        trailer_field_width=227,
    )

    with pytest.raises(ValueError, match='invalid stock identity fields'):
        _parse_master_lines(raw, trailer_width=227, market='KOSPI')


def test_parse_master_lines_fails_closed_on_invalid_encoding() -> None:
    raw = bytearray(
        _master_line(
            symbol='005930',
            standard_code='KR7005930003',
            name_ko='삼성전자',
            security_group='ST',
            trailer_field_width=227,
        )
    )
    raw[21] = 0xFF

    with pytest.raises(ValueError, match='invalid encoded fields'):
        _parse_master_lines(bytes(raw), trailer_width=227, market='KOSPI')


def _master_symbols(*, market: str, count: int, start: int) -> list[master.MasterSymbol]:
    def valid_isin(sequence: int) -> str:
        prefix = f'KR{sequence:09d}'
        return next(
            prefix + str(check_digit)
            for check_digit in range(10)
            if master._is_valid_isin(prefix + str(check_digit))
        )

    return [
        master.MasterSymbol(
            symbol=f'{start + index:06d}',
            standard_code=valid_isin(start + index),
            name_ko=f'{market} {index}',
            market=market,
            security_group='ST',
        )
        for index in range(count)
    ]


def _complete_master_universe() -> list[master.MasterSymbol]:
    return [
        *_master_symbols(
            market='KOSPI',
            count=master.MIN_MASTER_STOCK_COUNTS['KOSPI'],
            start=100_000,
        ),
        *_master_symbols(
            market='KOSDAQ',
            count=master.MIN_MASTER_STOCK_COUNTS['KOSDAQ'],
            start=500_000,
        ),
    ]


def test_validate_master_universe_accepts_complete_unique_two_market_snapshot() -> None:
    master._validate_master_universe(_complete_master_universe())


@pytest.mark.parametrize('market', ['KOSPI', 'KOSDAQ'])
def test_validate_master_universe_rejects_market_below_safe_minimum(market: str) -> None:
    symbols = _complete_master_universe()
    removed = next(index for index, item in enumerate(symbols) if item.market == market)
    symbols.pop(removed)

    with pytest.raises(ValueError, match=f'{market} master count'):
        master._validate_master_universe(symbols)


def test_validate_master_universe_rejects_duplicate_symbol_across_markets() -> None:
    symbols = _complete_master_universe()
    replacement_standard_code = symbols[-1].standard_code
    symbols[-1] = master.MasterSymbol(
        symbol=symbols[0].symbol,
        standard_code=replacement_standard_code,
        name_ko='duplicate',
        market='KOSDAQ',
        security_group='ST',
    )

    with pytest.raises(ValueError, match='duplicate symbols'):
        master._validate_master_universe(symbols)


def test_validate_master_universe_rejects_duplicate_standard_code() -> None:
    symbols = _complete_master_universe()
    symbols[-1].standard_code = symbols[0].standard_code

    with pytest.raises(ValueError, match='duplicate standard codes'):
        master._validate_master_universe(symbols)


def test_validate_master_universe_requires_both_markets() -> None:
    kospi_only = _master_symbols(
        market='KOSPI',
        count=master.MIN_MASTER_STOCK_COUNTS['KOSPI'],
        start=100_000,
    )

    with pytest.raises(ValueError, match='KOSDAQ master count 0'):
        master._validate_master_universe(kospi_only)


def test_refresh_rejects_incomplete_master_before_persistence() -> None:
    previous_state = dict(master.master_refresh_state.__dict__)
    incomplete = _master_symbols(market='KOSPI', count=10, start=100_000)
    try:
        with (
            pytest.MonkeyPatch.context() as monkeypatch,
        ):
            monkeypatch.setattr(
                master,
                'get_market_state',
                lambda _now=None: type('Market', (), {'server_time': '2026-09-27T00:00:00+09:00'})(),
            )
            monkeypatch.setattr(master, 'load_all_domestic_masters', lambda: incomplete)
            monkeypatch.setattr(
                master,
                'advisory_eod_collection_lock',
                _available_master_lock,
            )
            persisted: list[list[master.MasterSymbol]] = []
            monkeypatch.setattr(master, 'upsert_master_symbols', lambda items: persisted.append(items))

            with pytest.raises(ValueError, match='KOSPI master count'):
                master.refresh_master_if_due(force=True)

            assert persisted == []
            assert master.master_refresh_state.last_status == 'failed'
    finally:
        master.master_refresh_state.__dict__.clear()
        master.master_refresh_state.__dict__.update(previous_state)


def test_refresh_does_not_publish_completed_state_when_persistence_fails() -> None:
    previous_state = dict(master.master_refresh_state.__dict__)
    complete = _complete_master_universe()
    try:
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(
                master,
                'get_market_state',
                lambda _now=None: type(
                    'Market',
                    (),
                    {'server_time': '2026-09-27T00:00:00+09:00'},
                )(),
            )
            monkeypatch.setattr(master, 'load_all_domestic_masters', lambda: complete)
            monkeypatch.setattr(
                master,
                'advisory_eod_collection_lock',
                _available_master_lock,
            )

            def fail_persistence(_items: list[master.MasterSymbol]) -> None:
                raise RuntimeError('database unavailable')

            monkeypatch.setattr(master, 'upsert_master_symbols', fail_persistence)

            with pytest.raises(RuntimeError, match='database unavailable'):
                master.refresh_master_if_due(force=True)

            assert master.master_refresh_state.last_status == 'failed'
            assert master.master_refresh_state.symbols == []
    finally:
        master.master_refresh_state.__dict__.clear()
        master.master_refresh_state.__dict__.update(previous_state)


def test_refresh_does_not_download_when_master_lock_is_unavailable() -> None:
    previous_state = dict(master.master_refresh_state.__dict__)

    @contextmanager
    def unavailable_lock(**_kwargs):
        yield False

    try:
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(
                master,
                'get_market_state',
                lambda _now=None: type(
                    'Market',
                    (),
                    {'server_time': '2026-09-27T00:00:00+09:00'},
                )(),
            )
            monkeypatch.setattr(
                master,
                'advisory_eod_collection_lock',
                unavailable_lock,
            )
            monkeypatch.setattr(
                master,
                'load_all_domestic_masters',
                lambda: pytest.fail('download must not run without the lock'),
            )

            with pytest.raises(RuntimeError, match='lock is already held'):
                master.refresh_master_if_due(force=True)
    finally:
        master.master_refresh_state.__dict__.clear()
        master.master_refresh_state.__dict__.update(previous_state)
