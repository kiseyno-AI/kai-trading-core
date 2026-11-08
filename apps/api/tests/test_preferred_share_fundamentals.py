import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db_models import Symbol
from app.signals.preferred_share_fundamentals import (
    resolve_preferred_fundamentals_issuer_symbols,
)


class PreferredShareFundamentalsResolverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.engine = create_engine(
            'sqlite+pysqlite:///:memory:',
            connect_args={'check_same_thread': False},
            poolclass=StaticPool,
        )
        Symbol.__table__.create(bind=cls.engine, checkfirst=True)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.engine.dispose()

    def setUp(self) -> None:
        with Session(self.engine) as session:
            session.query(Symbol).delete()
            session.commit()

    def _add_symbol(
        self,
        *,
        symbol: str,
        name_ko: str,
        market: str = 'KOSPI',
        is_active: bool = True,
    ) -> None:
        with Session(self.engine) as session:
            session.add(
                Symbol(
                    symbol=symbol,
                    name_ko=name_ko,
                    market=market,
                    source='test',
                    is_active=is_active,
                )
            )
            session.commit()

    def _resolve(self, *symbols: str) -> dict[str, str]:
        with Session(self.engine) as session:
            return resolve_preferred_fundamentals_issuer_symbols(
                session,
                list(symbols),
            )

    def test_resolves_only_strict_current_same_market_preferred_symbols(self) -> None:
        rows = (
            ('005930', '삼성전자', 'KOSPI', True),
            ('005935', '삼성전자우', 'KOSPI', True),
            ('000080', '하이트진로', 'KOSPI', True),
            ('000087', '하이트진로 2우 B  ', 'KOSPI', True),
            ('123450', '테스트', 'KOSDAQ', True),
            ('123459', '테스트3우B', 'KOSDAQ', True),
        )
        for symbol, name_ko, market, is_active in rows:
            self._add_symbol(
                symbol=symbol,
                name_ko=name_ko,
                market=market,
                is_active=is_active,
            )

        self.assertEqual(
            self._resolve('005935', '000087', '123459'),
            {
                '005935': '005930',
                '000087': '000080',
                '123459': '123450',
            },
        )

    def test_rejects_invalid_suffix_name_inactive_missing_and_market_mismatch(
        self,
    ) -> None:
        rows = (
            ('111110', '접미사보통', 'KOSPI', True),
            ('111111', '접미사우', 'KOSPI', True),
            ('222220', '이름보통', 'KOSPI', True),
            ('222225', '이름보통주', 'KOSPI', True),
            ('333330', '시장보통', 'KOSDAQ', True),
            ('333337', '시장1우', 'KOSPI', True),
            ('444440', '비활성보통', 'KOSPI', True),
            ('444449', '비활성우', 'KOSPI', False),
            ('555555', '누락우', 'KOSPI', True),
            ('666660', '잘못보통', 'KOSPI', True),
            ('666665', '잘못4우', 'KOSPI', True),
            ('777770', '일반주', 'KOSPI', True),
        )
        for symbol, name_ko, market, is_active in rows:
            self._add_symbol(
                symbol=symbol,
                name_ko=name_ko,
                market=market,
                is_active=is_active,
            )

        self.assertEqual(
            self._resolve(
                '111111',
                '222225',
                '333337',
                '444449',
                '555555',
                '666665',
                '777770',
                '888889',
            ),
            {},
        )

    def test_rejects_active_preferred_symbol_when_common_symbol_is_inactive(
        self,
    ) -> None:
        self._add_symbol(
            symbol='999990',
            name_ko='상장폐지보통',
            is_active=False,
        )
        self._add_symbol(
            symbol='999995',
            name_ko='상장폐지우',
            is_active=True,
        )

        self.assertEqual(self._resolve('999995'), {})


if __name__ == '__main__':
    unittest.main()
