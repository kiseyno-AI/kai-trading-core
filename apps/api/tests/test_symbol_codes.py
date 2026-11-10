from app.market.symbol_codes import (
    is_canonical_krx_short_code,
    is_krx_preferred_share,
    normalize_krx_short_code,
)


def test_krx_preferred_share_requires_preferred_name_and_market() -> None:
    assert is_krx_preferred_share('005935', '삼성전자우', 'KOSPI')
    assert is_krx_preferred_share('00104K', 'CJ4우(전환)', 'KOSPI')
    assert is_krx_preferred_share('33626L', '두산퓨얼셀2우B', 'KOSPI')
    assert not is_krx_preferred_share('005935', '삼성전자', 'KOSPI')
    assert not is_krx_preferred_share('005930', '삼성전자우', 'KOSPI')
    assert not is_krx_preferred_share('005935', '삼성전자우', 'NASDAQ')


def test_normalize_krx_short_code_accepts_numeric_and_alpha_codes() -> None:
    assert normalize_krx_short_code('5930') == '005930'
    assert normalize_krx_short_code('005930') == '005930'
    assert normalize_krx_short_code(' 0010a0 ') == '0010A0'


def test_normalize_krx_short_code_rejects_non_krx_shapes() -> None:
    assert normalize_krx_short_code('') is None
    assert normalize_krx_short_code('ABC123') is None
    assert normalize_krx_short_code('0010-A') is None
    assert normalize_krx_short_code('００５９３０') is None
    assert normalize_krx_short_code('0010A') is None


def test_canonical_krx_short_code_requires_exact_uppercase_shape() -> None:
    assert is_canonical_krx_short_code('005930') is True
    assert is_canonical_krx_short_code('0010A0') is True
    assert is_canonical_krx_short_code('0010a0') is False
    assert is_canonical_krx_short_code('5930') is False
