import unittest
from copy import deepcopy
from datetime import date, datetime

from app.core.settings import settings
from app.fx.snapshot_store import FXRateSnapshotRecord
from app.integrations.fx.client import KoreaEximAPIError
from app.market.snapshot import (
    MarketSnapshotConfigurationError,
    MarketSnapshotService,
)
from app.market.snapshot_store import MarketSnapshotCacheRecord


class FakeKISClient:
    def __init__(self) -> None:
        self.index_calls: list[str] = []
        self.flow_calls: list[tuple[str, str]] = []
        self.token_calls = 0

    async def issue_access_token(self, force_refresh: bool = False) -> dict:
        self.token_calls += 1
        return {'access_token': 'token', 'cached': False}

    async def fetch_market_index_price(self, *, index_code: str) -> dict:
        self.index_calls.append(index_code)
        if index_code == '0001':
            return {
                'output': {
                    'bstp_nmix_prpr': '2642.07',
                    'bstp_nmix_prdy_vrss': '2.78',
                    'bstp_nmix_prdy_ctrt': '0.11',
                    'bstp_nmix_oprc': '2651.63',
                    'bstp_nmix_hgpr': '2653.87',
                    'bstp_nmix_lwpr': '2634.29',
                    'acml_vol': '305715',
                    'acml_tr_pbmn': '4736153',
                    'ascn_issu_cnt': '512',
                    'stnr_issu_cnt': '94',
                    'down_issu_cnt': '330',
                }
            }
        return {
            'output': {
                'bstp_nmix_prpr': '857.60',
                'bstp_nmix_prdy_vrss': '-1.61',
                'bstp_nmix_prdy_ctrt': '-0.19',
                'bstp_nmix_oprc': '863.69',
                'bstp_nmix_hgpr': '864.24',
                'bstp_nmix_lwpr': '854.72',
                'acml_vol': '1312496',
                'acml_tr_pbmn': '11507962',
                'ascn_issu_cnt': '828',
                'stnr_issu_cnt': '94',
                'down_issu_cnt': '716',
            }
        }

    async def fetch_investor_time_by_market(self, *, market_code: str, sector_code: str) -> dict:
        self.flow_calls.append((market_code, sector_code))
        if market_code == 'KSP':
            return {
                'output': [
                    {
                        'frgn_ntby_qty': '-5290',
                        'frgn_ntby_tr_pbmn': '148656',
                        'orgn_ntby_qty': '720',
                        'orgn_ntby_tr_pbmn': '-55532',
                    }
                ]
            }
        return {
            'output': [
                {
                    'frgn_ntby_qty': '-18565',
                    'frgn_ntby_tr_pbmn': '-597490',
                    'orgn_ntby_qty': '-4738',
                    'orgn_ntby_tr_pbmn': '-150685',
                }
            ]
        }


class FakeFXService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def get_exchange_rates(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {
            'provider': 'koreaexim',
            'requested_date': '20260429',
            'effective_date': '20260429',
            'fallback_applied': False,
            'currency': kwargs.get('currency'),
            'count': 1,
            'items': [{'currency_code': 'USD', 'exchange_rate': 1444.5}],
        }


class SequencedFXService:
    def __init__(self, responses: list[dict | Exception]) -> None:
        self.responses = list(responses)
        self.calls: list[dict] = []

    async def get_exchange_rates(self, **kwargs):
        self.calls.append(dict(kwargs))
        if not self.responses:
            raise AssertionError('No more fake FX responses are configured')
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _parse_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return None


def _parse_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if len(normalized) == 8 and normalized.isdigit():
            return date.fromisoformat(f'{normalized[:4]}-{normalized[4:6]}-{normalized[6:8]}')
        return date.fromisoformat(normalized)
    raise ValueError(f'Unsupported test date value: {value!r}')


class InMemoryCacheStore:
    def __init__(self) -> None:
        self.record: MarketSnapshotCacheRecord | None = None

    def load(self) -> MarketSnapshotCacheRecord | None:
        return deepcopy(self.record)

    def save(
        self,
        *,
        payload: dict | None,
        last_refresh_attempted_at: datetime | None,
        last_refresh_succeeded_at: datetime | None,
        last_refresh_status: str,
        last_refresh_error: str | None,
        refresh_trigger: str | None,
    ) -> MarketSnapshotCacheRecord:
        created_at = self.record.created_at if self.record is not None else last_refresh_attempted_at
        payload_copy = deepcopy(payload) if isinstance(payload, dict) else None
        self.record = MarketSnapshotCacheRecord(
            payload=payload_copy,
            snapshot_status=payload_copy.get('status') if payload_copy is not None else None,
            stale_reason=payload_copy.get('stale_reason') if payload_copy is not None else None,
            as_of=_parse_datetime(payload_copy.get('as_of')) if payload_copy is not None else None,
            last_refresh_attempted_at=last_refresh_attempted_at,
            last_refresh_succeeded_at=last_refresh_succeeded_at,
            last_refresh_status=last_refresh_status,
            last_refresh_error=last_refresh_error,
            refresh_trigger=refresh_trigger,
            created_at=created_at,
            updated_at=last_refresh_attempted_at,
        )
        return deepcopy(self.record)


class InMemoryFXSnapshotStore:
    def __init__(self) -> None:
        self.record: FXRateSnapshotRecord | None = None
        self.saved_records: list[FXRateSnapshotRecord] = []

    def load_latest_usable(
        self,
        *,
        provider: str,
        currency_pair: str,
    ) -> FXRateSnapshotRecord | None:
        if self.record is None:
            return None
        if self.record.provider != provider or self.record.currency_pair != currency_pair:
            return None
        return deepcopy(self.record)

    def upsert_snapshot(
        self,
        *,
        provider: str,
        currency_pair: str,
        base_currency: str,
        quote_currency: str,
        exchange_rate: float,
        requested_date: date | str,
        effective_date: date | str,
        fallback_applied: bool,
        snapshot_status: str,
        stale_reason: str | None,
        detail: str | None,
        payload: dict | None,
        captured_at: datetime | str,
    ) -> FXRateSnapshotRecord:
        captured_dt = _parse_datetime(captured_at)
        assert captured_dt is not None
        created_at = self.record.created_at if self.record is not None else captured_dt
        self.record = FXRateSnapshotRecord(
            provider=provider,
            currency_pair=currency_pair,
            base_currency=base_currency,
            quote_currency=quote_currency,
            exchange_rate=float(exchange_rate),
            requested_date=_parse_date(requested_date),
            effective_date=_parse_date(effective_date),
            fallback_applied=fallback_applied,
            snapshot_status=snapshot_status,
            stale_reason=stale_reason,
            detail=detail,
            payload=deepcopy(payload) if isinstance(payload, dict) else None,
            captured_at=captured_dt,
            created_at=created_at,
            updated_at=captured_dt,
        )
        self.saved_records.append(deepcopy(self.record))
        return deepcopy(self.record)


class MarketSnapshotServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.original_settings = {
            'kis_use_mock': settings.kis_use_mock,
            'market_snapshot_kis_use_real': settings.market_snapshot_kis_use_real,
            'kis_app_key': settings.kis_app_key,
            'kis_app_secret': settings.kis_app_secret,
            'koreaexim_api_key': settings.koreaexim_api_key,
        }
        settings.kis_app_key = 'test-kis-key'
        settings.kis_app_secret = 'test-kis-secret'
        settings.koreaexim_api_key = 'test-koreaexim-key'
        self.fx_snapshot_store = InMemoryFXSnapshotStore()

    def tearDown(self) -> None:
        for field_name, value in self.original_settings.items():
            setattr(settings, field_name, value)

    async def test_refresh_primes_cache_and_repeated_reads_do_not_hit_upstream(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        client = FakeKISClient()
        fx_service = FakeFXService()
        cache_store = InMemoryCacheStore()
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=fx_service,
            cache_store=cache_store,
            fx_snapshot_store_instance=self.fx_snapshot_store,
            refresh_interval_seconds=3600,
        )

        refreshed = await service.refresh_market_snapshot_if_due(trigger='test_refresh', force=True)
        first_read = await service.get_market_snapshot()
        second_read = await service.get_market_snapshot()

        self.assertIsNotNone(refreshed)
        self.assertEqual(client.token_calls, 1)
        self.assertEqual(client.index_calls, ['0001', '1001'])
        self.assertEqual(client.flow_calls, [('KSP', '0001'), ('KSQ', '1001')])
        self.assertEqual(len(fx_service.calls), 1)
        self.assertEqual(first_read['status'], 'ready')
        self.assertEqual(first_read['markets']['kospi']['value'], 2642.07)
        self.assertEqual(first_read['usd_krw']['source'], 'live')
        self.assertTrue(first_read['freshness']['served_from_cache'])
        self.assertEqual(first_read['freshness']['last_refresh_status'], 'success')
        self.assertEqual(first_read['as_of'], second_read['as_of'])
        self.assertEqual(len(fx_service.calls), 1)

    async def test_get_market_snapshot_requires_real_kis_domain_when_override_is_disabled_without_cache(self) -> None:
        settings.kis_use_mock = True
        settings.market_snapshot_kis_use_real = False
        service = MarketSnapshotService(
            kis_api_client=FakeKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )

        with self.assertRaises(MarketSnapshotConfigurationError) as ctx:
            await service.get_market_snapshot()

        self.assertIn('MARKET_SNAPSHOT_KIS_USE_REAL', str(ctx.exception))

    async def test_refresh_returns_partial_when_fx_quota_is_exhausted_without_cache(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        fx_service = SequencedFXService([KoreaEximAPIError('Korea Exim returned result=4')])
        service = MarketSnapshotService(
            kis_api_client=FakeKISClient(),
            koreaexim_service=fx_service,
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )

        result = await service.refresh_market_snapshot_if_due(trigger='test_partial', force=True)
        cached_result = await service.get_market_snapshot()

        self.assertIsNotNone(result)
        self.assertEqual(result['status'], 'partial')
        self.assertIn('usd_krw.rate', result['missing_fields'])
        self.assertEqual(result['stale_reason'], 'koreaexim_quota_exhausted')
        self.assertEqual(result['usd_krw']['status'], 'missing')
        self.assertEqual(result['usd_krw']['source'], 'unavailable')
        self.assertEqual(result['freshness']['last_refresh_status'], 'degraded')
        self.assertIn('result=4', result['freshness']['last_refresh_error'])
        self.assertEqual(cached_result['status'], 'partial')
        self.assertEqual(len(fx_service.calls), 1)

    async def test_refresh_uses_cached_fx_after_quota_exhaustion_and_skips_same_day_retry(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        fx_service = SequencedFXService(
            [
                {
                    'provider': 'koreaexim',
                    'requested_date': '20260429',
                    'effective_date': '20260429',
                    'fallback_applied': False,
                    'currency': 'USD',
                    'count': 1,
                    'items': [{'currency_code': 'USD', 'exchange_rate': 1444.5}],
                },
                KoreaEximAPIError('Korea Exim returned result=4'),
            ]
        )
        service = MarketSnapshotService(
            kis_api_client=FakeKISClient(),
            koreaexim_service=fx_service,
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
            refresh_interval_seconds=0,
        )

        ready_result = await service.refresh_market_snapshot_if_due(trigger='test_ready', force=True)
        stale_result = await service.refresh_market_snapshot_if_due(trigger='test_stale', force=True)
        throttled_result = await service.refresh_market_snapshot_if_due(trigger='test_throttled', force=True)

        self.assertIsNotNone(ready_result)
        self.assertEqual(ready_result['status'], 'ready')
        self.assertEqual(stale_result['status'], 'stale')
        self.assertEqual(stale_result['stale_reason'], 'koreaexim_quota_exhausted')
        self.assertEqual(stale_result['usd_krw']['status'], 'stale')
        self.assertEqual(stale_result['usd_krw']['source'], 'cache')
        self.assertEqual(stale_result['usd_krw']['rate'], 1444.5)
        self.assertIn('result=4', stale_result['freshness']['last_refresh_error'])
        self.assertEqual(throttled_result['status'], 'stale')
        self.assertEqual(throttled_result['usd_krw']['source'], 'cache')
        self.assertEqual(throttled_result['usd_krw']['stale_reason'], 'koreaexim_quota_exhausted')
        self.assertEqual(len(fx_service.calls), 2)

    async def test_refresh_uses_persisted_fx_fallback_after_restart_when_live_fx_is_unavailable(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        durable_fx_store = InMemoryFXSnapshotStore()

        seed_service = MarketSnapshotService(
            kis_api_client=FakeKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=durable_fx_store,
        )
        seeded = await seed_service.refresh_market_snapshot_if_due(trigger='seed_snapshot', force=True)

        failing_fx_service = SequencedFXService([KoreaEximAPIError('Korea Exim returned result=4')])
        restarted_service = MarketSnapshotService(
            kis_api_client=FakeKISClient(),
            koreaexim_service=failing_fx_service,
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=durable_fx_store,
        )
        recovered = await restarted_service.refresh_market_snapshot_if_due(trigger='restart_refresh', force=True)

        self.assertIsNotNone(seeded)
        self.assertEqual(len(durable_fx_store.saved_records), 1)
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered['status'], 'stale')
        self.assertEqual(recovered['stale_reason'], 'koreaexim_quota_exhausted')
        self.assertEqual(recovered['usd_krw']['status'], 'stale')
        self.assertEqual(recovered['usd_krw']['source'], 'cache')
        self.assertEqual(recovered['usd_krw']['rate'], 1444.5)
        self.assertEqual(recovered['usd_krw']['effective_date'], '2026-04-29')
        self.assertEqual(recovered['usd_krw']['captured_at'], seeded['usd_krw']['captured_at'])
        self.assertEqual(recovered['missing_fields'], [])
        self.assertEqual(recovered['freshness']['last_refresh_status'], 'degraded')
        self.assertEqual(len(failing_fx_service.calls), 1)


if __name__ == '__main__':
    unittest.main()
