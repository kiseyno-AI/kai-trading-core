import hashlib
import unittest
from copy import deepcopy
from datetime import UTC, date, datetime
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_market
from app.market.live_sector_projection import LiveSectorProjectionService


SESSION_DATE = date(2026, 11, 5)
SOURCE_DATE = date(2026, 11, 4)
CHECKED_AT = datetime(2026, 11, 5, 1, 0, tzinfo=UTC)


def _membership_sha256(symbols: list[str]) -> str:
    return hashlib.sha256('\n'.join(sorted(symbols)).encode('utf-8')).hexdigest()


def _sector_rows(
    *,
    complete: bool = True,
    market_caps: tuple[int, ...] = (50, 30, 20),
) -> list[dict]:
    symbols = [f'{index:06d}' for index in range(1, len(market_caps) + 1)]
    membership_sha = _membership_sha256(symbols)
    rows = []
    for symbol, market_cap in zip(symbols, market_caps, strict=True):
        rows.append(
            {
                'market_date': SOURCE_DATE.isoformat(),
                'market': 'KOSPI',
                'category': 'sector_classification',
                'scope_type': 'symbol',
                'scope_key': symbol,
                'source': 'pykrx',
                'symbol': symbol,
                'captured_at': '2026-11-04T16:10:00+09:00',
                'payload': {
                    'sector_name': '검증업종',
                    'market_cap': market_cap,
                    # Deliberately extreme: live projection must never reuse it.
                    'change_pct': 99.0,
                    'universe_market_complete': complete,
                    'universe_reference_source': 'pykrx_market_ticker_list',
                    'universe_reference_count': len(symbols),
                    'universe_reference_sha256': membership_sha,
                },
            }
        )
    return rows


class FakeRowsLoader:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def __call__(self, **kwargs) -> list[dict]:
        rows = [
            row
            for row in self.rows
            if row['market_date'] == kwargs['market_date'].isoformat()
        ]
        if kwargs.get('markets'):
            rows = [row for row in rows if row['market'] in kwargs['markets']]
        if kwargs.get('symbols'):
            rows = [row for row in rows if row['symbol'] in kwargs['symbols']]
        return deepcopy(rows)


class FakeRealtimePriceService:
    def __init__(self, quotes: dict[str, dict]) -> None:
        self.client = SimpleNamespace(use_mock=False)
        self.quotes = quotes
        self.calls: list[str] = []

    def price_fields_for_symbol(self, symbol: str) -> dict:
        self.calls.append(symbol)
        return deepcopy(self.quotes.get(symbol) or {})


class FakeQuoteClient:
    use_mock = False

    def __init__(self, quotes: dict[str, dict] | None = None) -> None:
        self.quotes = quotes or {}
        self.calls: list[str] = []

    async def fetch_domestic_quote(self, symbol: str) -> dict:
        self.calls.append(symbol)
        return deepcopy(self.quotes.get(symbol) or {})


def _market_state(state: str = 'OPEN', *, is_open: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        state=state,
        is_open=is_open,
        current_calendar_date=SESSION_DATE.isoformat(),
        server_time='2026-11-05T10:00:00+09:00',
    )


def _snapshot() -> dict:
    observed_at = '2026-11-05T09:59:55+09:00'
    return {
        # Top-level staleness can come from the independent FX leg. The fresh,
        # same-session KIS index leg remains valid benchmark evidence.
        'status': 'stale',
        'markets': {
            'kospi': {
                'label': 'KOSPI',
                'provider_business_date': SESSION_DATE.isoformat(),
                'provider_observed_at': observed_at,
                'change_percent': 0.5,
                'index_leg': {
                    'status': 'ready',
                    'observation_source': 'kis_index_intraday_bar',
                    'provider_observed_at': observed_at,
                },
            }
        },
    }


def _realtime_quotes() -> dict[str, dict]:
    return {
        '000001': {
            'price_status': 'available',
            'price_source': 'kis_realtime_ws',
            'price_as_of': '2026-11-05T09:59:40+09:00',
            'current_price': 1000,
            'change_pct': 2.0,
        },
        '000002': {
            'price_status': 'available',
            'price_source': 'kis_realtime_ws',
            'price_as_of': '2026-11-05T09:59:45+09:00',
            'current_price': 2000,
            'change_pct': 0.0,
        },
        '000003': {
            'price_status': 'available',
            'price_source': 'kis_realtime_ws',
            'price_as_of': '2026-11-05T09:59:50+09:00',
            'current_price': 3000,
            'change_pct': -1.0,
        },
    }


class LiveSectorProjectionServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_ready_projection_uses_only_current_session_evidence(self) -> None:
        realtime = FakeRealtimePriceService(_realtime_quotes())
        rest = FakeQuoteClient()
        service = LiveSectorProjectionService(
            sector_rows_loader=FakeRowsLoader(_sector_rows()),
            market_state_loader=lambda: _async_value(_market_state()),
            market_snapshot_loader=lambda: _async_value(_snapshot()),
            realtime_price_service=realtime,
            quote_client=rest,
            now=lambda: CHECKED_AT,
            cache_ttl_seconds=0,
        )

        result = await service.get_projection(
            symbol='1',
            source_market_date=SOURCE_DATE,
        )

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['symbol'], '000001')
        self.assertEqual(result['projection']['sector_return_pct'], 0.8)
        self.assertEqual(result['projection']['benchmark_return_pct'], 0.5)
        self.assertEqual(result['projection']['relative_return_pct'], 0.3)
        self.assertEqual(result['observed_at'], '2026-11-05T09:59:40+09:00')
        self.assertEqual(result['cohort']['live_market_cap_coverage_ratio'], 1.0)
        self.assertEqual(rest.calls, [])
        self.assertNotEqual(result['projection']['sector_return_pct'], 99.0)

    async def test_preopen_reports_pending_without_loading_live_evidence(self) -> None:
        async def unexpected_snapshot() -> dict:
            self.fail('PREOPEN must not load live benchmark evidence')

        realtime = FakeRealtimePriceService({})
        service = LiveSectorProjectionService(
            sector_rows_loader=FakeRowsLoader(_sector_rows()),
            market_state_loader=lambda: _async_value(
                _market_state('PREOPEN', is_open=False)
            ),
            market_snapshot_loader=unexpected_snapshot,
            realtime_price_service=realtime,
            quote_client=FakeQuoteClient(),
            now=lambda: CHECKED_AT,
            cache_ttl_seconds=0,
        )

        result = await service.get_projection(
            symbol='000001',
            source_market_date=SOURCE_DATE,
        )

        self.assertEqual(result['status'], 'pending_current_session')
        self.assertIsNone(result['projection']['sector_return_pct'])
        self.assertEqual(realtime.calls, [])

    async def test_stale_quotes_fail_closed_without_eod_performance_fallback(self) -> None:
        stale_realtime = {
            symbol: {
                **fields,
                'price_as_of': '2026-11-04T15:29:00+09:00',
            }
            for symbol, fields in _realtime_quotes().items()
        }
        prior_day_rest = {
            symbol: {
                'output': {
                    'stck_bsop_date': '20261104',
                    'stck_cntg_hour': '152900',
                    'stck_prpr': '1000',
                    'prdy_ctrt': '88.0',
                }
            }
            for symbol in stale_realtime
        }
        service = LiveSectorProjectionService(
            sector_rows_loader=FakeRowsLoader(_sector_rows()),
            market_state_loader=lambda: _async_value(_market_state()),
            market_snapshot_loader=lambda: _async_value(_snapshot()),
            realtime_price_service=FakeRealtimePriceService(stale_realtime),
            quote_client=FakeQuoteClient(prior_day_rest),
            now=lambda: CHECKED_AT,
            cache_ttl_seconds=0,
        )

        result = await service.get_projection(
            symbol='000001',
            source_market_date=SOURCE_DATE,
        )

        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(
            result['reason'],
            'current_session_sector_quote_coverage_below_threshold',
        )
        self.assertIsNone(result['projection']['sector_return_pct'])
        self.assertIsNone(result['projection']['relative_return_pct'])
        self.assertEqual(result['cohort']['live_market_cap_coverage_ratio'], 0.0)

    async def test_unverified_market_membership_is_unavailable(self) -> None:
        realtime = FakeRealtimePriceService(_realtime_quotes())
        service = LiveSectorProjectionService(
            sector_rows_loader=FakeRowsLoader(_sector_rows(complete=False)),
            market_state_loader=lambda: _async_value(_market_state()),
            market_snapshot_loader=lambda: _async_value(_snapshot()),
            realtime_price_service=realtime,
            quote_client=FakeQuoteClient(),
            now=lambda: CHECKED_AT,
            cache_ttl_seconds=0,
        )

        result = await service.get_projection(
            symbol='000001',
            source_market_date=SOURCE_DATE,
        )

        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['reason'], 'pykrx_sector_market_membership_unverified')
        self.assertEqual(realtime.calls, [])

    async def test_top_market_cap_cohort_must_cover_sixty_percent(self) -> None:
        realtime = FakeRealtimePriceService(_realtime_quotes())
        service = LiveSectorProjectionService(
            sector_rows_loader=FakeRowsLoader(
                _sector_rows(market_caps=(10, 10, 10, 10, 10))
            ),
            market_state_loader=lambda: _async_value(_market_state()),
            market_snapshot_loader=lambda: _async_value(_snapshot()),
            realtime_price_service=realtime,
            quote_client=FakeQuoteClient(),
            now=lambda: CHECKED_AT,
            max_cohort_symbols=2,
            cache_ttl_seconds=0,
        )

        result = await service.get_projection(
            symbol='000001',
            source_market_date=SOURCE_DATE,
        )

        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(
            result['reason'],
            'sector_cohort_market_cap_coverage_below_threshold',
        )
        self.assertEqual(result['cohort']['selected_symbol_count'], 2)
        self.assertEqual(result['cohort']['market_cap_coverage_ratio'], 0.4)
        self.assertEqual(realtime.calls, [])

    async def test_rest_fallback_requires_provider_session_timestamp(self) -> None:
        rest_quotes = {
            symbol: {
                'output': {
                    'stck_bsop_date': '20261105',
                    'stck_cntg_hour': observed_time,
                    'stck_prpr': current_price,
                    'prdy_ctrt': change_pct,
                }
            }
            for symbol, observed_time, current_price, change_pct in (
                ('000001', '095940', '1000', '2.0'),
                ('000002', '095945', '2000', '0.0'),
                ('000003', '095950', '3000', '-1.0'),
            )
        }
        service = LiveSectorProjectionService(
            sector_rows_loader=FakeRowsLoader(_sector_rows()),
            market_state_loader=lambda: _async_value(_market_state()),
            market_snapshot_loader=lambda: _async_value(_snapshot()),
            realtime_price_service=FakeRealtimePriceService({}),
            quote_client=FakeQuoteClient(rest_quotes),
            now=lambda: CHECKED_AT,
            cache_ttl_seconds=0,
        )

        result = await service.get_projection(
            symbol='000001',
            source_market_date=SOURCE_DATE,
        )

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['projection']['sector_return_pct'], 0.8)
        self.assertEqual(
            {member['quote_source'] for member in result['cohort']['symbols']},
            {'kis_rest_inquire_price'},
        )


class FakeLiveSectorProjectionService:
    async def get_projection(self, *, symbol: str, source_market_date: date) -> dict:
        return {
            'schema_version': 'live_sector_projection_v1',
            'status': 'pending_current_session',
            'reason': 'current_session_sector_performance_pending_market_open',
            'symbol': symbol.zfill(6),
            'market': 'KOSPI',
            'market_date': '2026-11-05',
            'source_market_date': source_market_date.isoformat(),
            'observed_at': None,
            'source': 'core_verified_pykrx_sector_plus_kis_current_session',
            'sector_identity': {'sector_name': '검증업종'},
            'cohort': {'symbols': []},
            'benchmark': None,
            'projection': {
                'status': 'pending_current_session',
                'sector_return_pct': None,
            },
            'missing_fields': ['current_session_quotes'],
            'served_from_cache': False,
        }


class LiveSectorProjectionRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_market.router)
        self.original_service = routes_market.live_sector_projection_service

    def tearDown(self) -> None:
        routes_market.live_sector_projection_service = self.original_service

    def test_route_parses_exact_source_date_and_returns_projection(self) -> None:
        routes_market.live_sector_projection_service = (
            FakeLiveSectorProjectionService()
        )

        response = TestClient(self.app).get(
            '/market/live-sector-projection',
            params={'symbol': '1', 'source_market_date': SOURCE_DATE.isoformat()},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['symbol'], '000001')
        self.assertEqual(response.json()['status'], 'pending_current_session')

    def test_route_requires_source_market_date(self) -> None:
        response = TestClient(self.app).get(
            '/market/live-sector-projection',
            params={'symbol': '000001'},
        )

        self.assertEqual(response.status_code, 422)


async def _async_value(value):
    return value


if __name__ == '__main__':
    unittest.main()
