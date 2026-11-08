import asyncio
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import delete

from app.db import engine, session_scope
from app.db_models import SymbolIndustryProfile
from app.market import business_line_profiles as profiles
from app.market.business_lines import BUSINESS_LINES, KRX_SECTOR_RULES, KSIC_RULES, SYMBOL_OVERRIDES, classify_business_line
from app.market_news.catalog import MARKET_NEWS_TOPICS


class ClassificationTests(unittest.TestCase):
    def test_codes_seen_in_dart_map_to_the_expected_lines(self):
        cases = {
            ('012450', '31321'): ('defense', 'override'),
            ('999990', '31321'): ('defense', 'ksic'),  # aircraft engines
            ('999991', '31113'): ('shipbuilding', 'ksic'),
            ('999992', '28202'): ('battery_cells', 'ksic'),
            ('999993', '20423'): ('cosmetics', 'ksic'),
            ('999994', '26110'): ('semiconductor_memory', 'ksic'),
            ('999995', '29271'): ('semiconductor_equipment_materials', 'ksic'),
            ('999996', '64992'): ('holding_companies', 'ksic'),
            ('999997', '264'): ('it_hardware', 'ksic'),
            ('005930', '264'): ('semiconductor_memory', 'override'),  # filed as telecom equipment
            ('247540', '28202'): ('battery_materials', 'override'),
            ('105560', '64992'): ('banks', 'override'),
        }
        for (symbol, ksic), expected in cases.items():
            with self.subTest(symbol=symbol, ksic=ksic):
                self.assertEqual(classify_business_line(symbol=symbol, ksic_code=ksic), expected)

    def test_krx_sector_then_default_when_no_industry_code(self):
        self.assertEqual(classify_business_line(symbol='999999', ksic_code=None, krx_sector='은행'), ('banks', 'krx_sector'))
        self.assertEqual(classify_business_line(symbol='999999', ksic_code='', krx_sector='모름'), ('other', 'default'))

    def test_every_key_used_anywhere_is_a_declared_line(self):
        used = set(KSIC_RULES.values()) | set(KRX_SECTOR_RULES.values()) | set(SYMBOL_OVERRIDES.values())
        used |= {line for topic in MARKET_NEWS_TOPICS for line in topic.business_lines}
        self.assertEqual(used - BUSINESS_LINES.keys(), set())


class ProfileHistoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SymbolIndustryProfile.__table__.create(bind=engine, checkfirst=True)

    def setUp(self):
        self._clean()
        self.addCleanup(self._clean)

    def _clean(self):
        with session_scope() as session:
            session.execute(delete(SymbolIndustryProfile))

    def _record(self, ksic, now):
        return profiles.record_profile(
            symbol='123450', corp_code='00000001', corp_name='테스트', ksic_code=ksic, krx_sector='화학', now=now
        )

    def test_rows_are_added_only_when_the_classification_changes(self):
        day1 = datetime(2026, 11, 8, 1, tzinfo=timezone.utc)
        day2 = day1 + timedelta(days=30)
        day3 = day2 + timedelta(days=30)

        self.assertEqual(self._record('20111', day1), 'inserted')
        self.assertEqual(self._record('20111', day2), 'unchanged')
        self.assertEqual(self._record('20423', day3), 'inserted')

        self.assertEqual(profiles.profiles_as_of(date(2026, 11, 20))['123450'], 'refining_chemicals')
        self.assertEqual(profiles.profiles_as_of(day3.date())['123450'], 'cosmetics')
        self.assertNotIn('123450', profiles.profiles_as_of(date(2026, 11, 1)))

    def test_a_same_day_correction_replaces_that_days_row(self):
        now = datetime(2026, 11, 8, 1, tzinfo=timezone.utc)
        self._record('20111', now)
        self._record('20423', now + timedelta(hours=1))

        with session_scope() as session:
            rows = session.query(SymbolIndustryProfile).all()
            self.assertEqual([(row.ksic_code, row.business_line) for row in rows], [('20423', 'cosmetics')])


class FakeDart:
    def __init__(self, codes):
        self.codes = codes
        self.calls = []

    async def fetch_company_overview(self, corp_code):
        self.calls.append(corp_code)
        if corp_code == 'BAD':
            raise RuntimeError('DART status 020: request limit')
        return {'corp_name': 'X', 'induty_code': self.codes[corp_code]}


class RefreshTests(ProfileHistoryTests):
    def test_refresh_skips_recently_verified_and_survives_a_dart_error(self):
        now = datetime(2026, 11, 8, 1, tzinfo=timezone.utc)
        corp = {'000001': ('C1', 'A'), '000002': ('BAD', 'B'), '000003': ('C3', 'C')}
        dart = FakeDart({'C1': '31113', 'C3': '58212'})

        async def no_sleep(_seconds):
            return None

        with (
            patch.object(profiles, '_known_corp_codes', return_value=corp),
            patch.object(profiles, '_latest_krx_sectors', return_value={'000002': '은행'}),
        ):
            first = asyncio.run(profiles.refresh_profiles(symbols=list(corp), client=dart, now=now, sleep=no_sleep))
            second = asyncio.run(profiles.refresh_profiles(symbols=list(corp), client=dart, now=now + timedelta(days=1), sleep=no_sleep))

        self.assertEqual((first['inserted'], first['errors']), (3, 1))
        self.assertEqual(second['due'], 0)
        lines = profiles.profiles_as_of(now.date())
        self.assertEqual(lines, {'000001': 'shipbuilding', '000002': 'banks', '000003': 'games'})


if __name__ == '__main__':
    unittest.main()
