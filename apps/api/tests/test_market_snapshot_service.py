import asyncio
import unittest
from copy import deepcopy
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import patch

from app.brokers.kis.client import KISClient, KISClientError
from app.core.settings import settings
from app.fx.snapshot_store import FXRateSnapshotRecord
from app.integrations.fx.client import KoreaEximAPIError
from app.market.snapshot import (
    MARKET_SNAPSHOT_TARGETS,
    MarketSnapshotConfigurationError,
    MarketSnapshotService,
    MarketSnapshotUpstreamError,
    _parse_float,
    _parse_int,
)
from app.market.snapshot_store import MarketSnapshotCacheRecord


def test_market_snapshot_numeric_parsers_reject_nonfinite_values() -> None:
    for value in ('NaN', 'Infinity', '-Infinity', float('nan'), float('inf')):
        with unittest.TestCase().assertRaises(MarketSnapshotUpstreamError):
            _parse_float(value, field_name='test_float')
        with unittest.TestCase().assertRaises(MarketSnapshotUpstreamError):
            _parse_int(value, field_name='test_int')


def test_market_snapshot_integer_parser_rejects_fractional_and_exponent_values() -> None:
    for value in ('1.9', '-2.5', '1e3', '1E+3', '1,23', '1,234.5'):
        with unittest.TestCase().assertRaises(MarketSnapshotUpstreamError):
            _parse_int(value, field_name='test_int')

    for value, expected in (('1,234', 1234), ('1.0', 1), ('-2.000', -2)):
        with unittest.TestCase().subTest(value=value):
            assert _parse_int(value, field_name='test_int') == expected


class FakeKISClient:
    def __init__(self) -> None:
        self.index_calls: list[str] = []
        self.index_time_calls: list[str] = []
        self.flow_calls: list[tuple[str, str]] = []
        self.token_calls = 0
        self.provider_business_date = '20260429'
        self.provider_session_time = '100500'

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
                    'prdy_tr_pbmn': '18944612',
                    'uplm_issu_cnt': '3',
                    'ascn_issu_cnt': '512',
                    'stnr_issu_cnt': '94',
                    'down_issu_cnt': '330',
                    'lslm_issu_cnt': '1',
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
                'prdy_tr_pbmn': '46031848',
                'uplm_issu_cnt': '7',
                'ascn_issu_cnt': '828',
                'stnr_issu_cnt': '94',
                'down_issu_cnt': '716',
                'lslm_issu_cnt': '2',
            }
        }

    async def fetch_market_index_time_series(self, *, index_code: str) -> dict:
        self.index_time_calls.append(index_code)
        return {
            'output1': {},
            'output2': [
                {
                    'stck_bsop_date': self.provider_business_date,
                    'stck_cntg_hour': self.provider_session_time,
                    'bstp_nmix_prpr': '2642.07' if index_code == '0001' else '857.60',
                    'acml_tr_pbmn': '4736153' if index_code == '0001' else '11507962',
                }
            ],
        }

    async def fetch_investor_time_by_market(self, *, market_code: str, sector_code: str) -> dict:
        self.flow_calls.append((market_code, sector_code))
        if market_code == 'KSP':
            return {
                'output': [
                    {
                        'frgn_ntby_qty': '-5290',
                        'frgn_ntby_tr_pbmn': '-1455933',
                        'orgn_ntby_qty': '720',
                        'orgn_ntby_tr_pbmn': '283792',
                    }
                ]
            }
        return {
            'output': [
                {
                    'frgn_ntby_qty': '-18565',
                    'frgn_ntby_tr_pbmn': '-211205',
                    'orgn_ntby_qty': '-4738',
                    'orgn_ntby_tr_pbmn': '-304502',
                }
            ]
        }


class RecordingKISClient(KISClient):
    def __init__(self) -> None:
        super().__init__(use_mock=False)
        self.request: tuple[str, str, dict] | None = None

    async def _get(self, path: str, *, tr_id: str, params: dict) -> dict:
        self.request = (path, tr_id, dict(params))
        return {'output1': {}, 'output2': []}


class FailingFlowKISClient(FakeKISClient):
    async def fetch_investor_time_by_market(self, *, market_code: str, sector_code: str) -> dict:
        self.flow_calls.append((market_code, sector_code))
        raise KISClientError('flow unavailable')


class FailingIndexObservationKISClient(FakeKISClient):
    async def fetch_market_index_time_series(self, *, index_code: str) -> dict:
        self.index_time_calls.append(index_code)
        raise KISClientError('index observation unavailable')


class MismatchedIndexObservationKISClient(FakeKISClient):
    async def fetch_market_index_time_series(self, *, index_code: str) -> dict:
        payload = await super().fetch_market_index_time_series(index_code=index_code)
        payload['output2'][0]['bstp_nmix_prpr'] = '2500.00'
        return payload


class PriorSessionIndexObservationKISClient(FakeKISClient):
    async def fetch_market_index_time_series(self, *, index_code: str) -> dict:
        payload = await super().fetch_market_index_time_series(index_code=index_code)
        payload['output2'][0]['stck_bsop_date'] = '20260428'
        return payload


class AfterHoursIndexObservationKISClient(FakeKISClient):
    def __init__(self) -> None:
        super().__init__()
        self.provider_session_time = '153000'


class BlockingKISClient(FakeKISClient):
    def __init__(self) -> None:
        super().__init__()
        self.index_started = asyncio.Event()
        self.release_index = asyncio.Event()

    async def fetch_market_index_price(self, *, index_code: str) -> dict:
        result = await super().fetch_market_index_price(index_code=index_code)
        self.index_started.set()
        await self.release_index.wait()
        return result


class ReceiptTimedKISClient(FakeKISClient):
    def __init__(self, *, clock: dict[str, datetime], receipt_at: datetime) -> None:
        super().__init__()
        self.clock = clock
        self.receipt_at = receipt_at

    async def fetch_market_index_time_series(self, *, index_code: str) -> dict:
        payload = await super().fetch_market_index_time_series(index_code=index_code)
        self.clock['now'] = self.receipt_at
        return payload


class BracketedIndexKISClient(FakeKISClient):
    def __init__(
        self,
        *,
        clock: dict[str, datetime],
        before_turnover: int,
        bar_turnover: int,
        after_turnover: int,
        before_price: str = '2642.07',
        bar_price: str = '2640.50',
        after_price: str = '2643.10',
        provider_session_time: str = '100510',
    ) -> None:
        super().__init__()
        self.clock = clock
        self.before_turnover = before_turnover
        self.bar_turnover = bar_turnover
        self.after_turnover = after_turnover
        self.before_price = before_price
        self.bar_price = bar_price
        self.after_price = after_price
        self.provider_session_time = provider_session_time
        self.current_call_count = 0

    async def fetch_market_index_price(self, *, index_code: str) -> dict:
        payload = await super().fetch_market_index_price(index_code=index_code)
        self.current_call_count += 1
        if self.current_call_count == 1:
            payload['output']['acml_tr_pbmn'] = str(self.before_turnover)
            payload['output']['bstp_nmix_prpr'] = self.before_price
            self.clock['now'] = datetime.fromisoformat(
                '2026-04-29T10:05:05+09:00'
            )
        else:
            payload['output']['acml_tr_pbmn'] = str(self.after_turnover)
            payload['output']['bstp_nmix_prpr'] = self.after_price
            self.clock['now'] = datetime.fromisoformat(
                '2026-04-29T10:05:15+09:00'
            )
        return payload

    async def fetch_market_index_time_series(self, *, index_code: str) -> dict:
        payload = await super().fetch_market_index_time_series(
            index_code=index_code
        )
        payload['output2'][0]['acml_tr_pbmn'] = str(self.bar_turnover)
        payload['output2'][0]['bstp_nmix_prpr'] = self.bar_price
        return payload


class KISMarketIndexObservationContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_market_index_time_series_uses_official_intraday_index_contract(self) -> None:
        client = RecordingKISClient()

        result = await client.fetch_market_index_time_series(index_code='0001')

        self.assertEqual(result, {'output1': {}, 'output2': []})
        self.assertEqual(
            client.request,
            (
                '/uapi/domestic-stock/v1/quotations/inquire-time-indexchartprice',
                'FHKUP03500200',
                {
                    'FID_COND_MRKT_DIV_CODE': 'U',
                    'FID_ETC_CLS_CODE': '0',
                    'FID_INPUT_ISCD': '0001',
                    'FID_INPUT_HOUR_1': '60',
                    'FID_PW_DATA_INCU_YN': 'N',
                },
            ),
        )


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
        self.claim_result = True
        self.claim_calls: list[dict] = []

    def load(self) -> MarketSnapshotCacheRecord | None:
        return deepcopy(self.record)

    def try_claim_refresh_bucket(self, **kwargs) -> bool:
        self.claim_calls.append(dict(kwargs))
        return self.claim_result

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
        self.now_patcher = patch.object(
            MarketSnapshotService,
            '_now',
            return_value=datetime.fromisoformat('2026-04-29T10:05:20+09:00'),
        )
        self.now_patcher.start()
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
        self.now_patcher.stop()
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
        self.assertEqual(len(client.index_calls), 4)
        self.assertEqual(client.index_calls.count('0001'), 2)
        self.assertEqual(client.index_calls.count('1001'), 2)
        self.assertEqual(client.flow_calls, [('KSP', '0001'), ('KSQ', '1001')])
        self.assertEqual(len(fx_service.calls), 1)
        self.assertEqual(first_read['status'], 'ready')
        self.assertEqual(first_read['markets']['kospi']['value'], 2642.07)
        self.assertEqual(first_read['markets']['kospi']['trading_value_unit'], 'million_krw')
        self.assertEqual(first_read['markets']['kospi']['foreign_net_value'], -1_455_933_000_000)
        self.assertEqual(first_read['markets']['kospi']['institution_net_value'], 283_792_000_000)
        self.assertEqual(first_read['markets']['kospi']['investor_flow_value_unit'], 'krw')
        self.assertEqual(first_read['markets']['kosdaq']['foreign_net_value'], -211_205_000_000)
        self.assertEqual(first_read['markets']['kosdaq']['institution_net_value'], -304_502_000_000)
        self.assertEqual(first_read['usd_krw']['source'], 'live')
        self.assertTrue(first_read['freshness']['served_from_cache'])
        self.assertEqual(first_read['freshness']['last_refresh_status'], 'success')
        self.assertEqual(first_read['as_of'], second_read['as_of'])
        self.assertEqual(len(fx_service.calls), 1)

    async def test_refresh_preserves_provider_session_prior_turnover_and_all_breadth_buckets(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        client = FakeKISClient()
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        service._now = lambda: datetime.fromisoformat('2026-04-29T10:05:20+09:00')

        result = await service.refresh_market_snapshot_if_due(trigger='provider_provenance', force=True)

        self.assertIsNotNone(result)
        assert result is not None
        kospi = result['markets']['kospi']
        self.assertEqual(client.index_time_calls, ['0001', '1001'])
        self.assertEqual(kospi['provider_business_date'], '2026-04-29')
        self.assertEqual(kospi['provider_session_time'], '10:05:00')
        self.assertEqual(kospi['provider_observed_at'], '2026-04-29T10:05:00+09:00')
        self.assertEqual(kospi['prior_trading_value'], 18_944_612)
        self.assertEqual(kospi['prior_trading_value_unit'], 'million_krw')
        self.assertEqual(
            kospi['breadth_raw_buckets'],
            {
                'upper_limit_count': 3,
                'advancers': 512,
                'unchanged': 94,
                'decliners': 330,
                'lower_limit_count': 1,
            },
        )
        self.assertEqual(kospi['index_leg']['status'], 'ready')
        self.assertEqual(kospi['index_leg']['observation_source'], 'kis_index_intraday_bar')
        self.assertEqual(kospi['breadth_completeness']['status'], 'unverified')

    async def test_refresh_binds_provider_age_and_success_to_response_receipt_time(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        refresh_started_at = datetime.fromisoformat('2026-04-29T10:05:00+09:00')
        response_received_at = datetime.fromisoformat('2026-04-29T10:05:02+09:00')
        clock = {'now': refresh_started_at}
        client = ReceiptTimedKISClient(
            clock=clock,
            receipt_at=response_received_at,
        )
        client.provider_session_time = '100501'
        cache_store = InMemoryCacheStore()
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=FakeFXService(),
            cache_store=cache_store,
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        service._now = lambda: clock['now']

        result = await service.refresh_market_snapshot_if_due(
            trigger='receipt_time_provenance',
            force=True,
        )

        assert result is not None
        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['collected_at'], response_received_at.isoformat())
        self.assertEqual(
            result['markets']['kospi']['index_leg']['collected_at'],
            response_received_at.isoformat(),
        )
        self.assertEqual(
            result['markets']['kospi']['index_leg']['observation_age_seconds'],
            1.0,
        )
        self.assertEqual(
            result['freshness']['last_refresh_attempted_at'],
            refresh_started_at.isoformat(),
        )
        self.assertEqual(
            result['freshness']['last_refresh_succeeded_at'],
            response_received_at.isoformat(),
        )

    async def test_completed_same_date_kis_master_bounds_provider_directional_breadth(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        service = MarketSnapshotService(
            kis_api_client=FakeKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        service._now = lambda: datetime.fromisoformat('2026-04-29T10:05:20+09:00')
        master_symbols = [
            *(SimpleNamespace(symbol=f'P{index:04d}', market='KOSPI') for index in range(940)),
            *(SimpleNamespace(symbol=f'Q{index:04d}', market='KOSDAQ') for index in range(1647)),
        ]
        master_state = SimpleNamespace(
            symbols=master_symbols,
            snapshot=lambda: {
                'market_date': '2026-04-29',
                'last_status': 'completed',
                'last_completed_at': '2026-04-29T00:00:00+00:00',
                'symbol_count': 2587,
                'kospi_count': 940,
                'kosdaq_count': 1647,
            },
        )

        with patch('app.market.snapshot.master_refresh_state', master_state):
            result = await service.refresh_market_snapshot_if_due(
                trigger='breadth_completeness',
                force=True,
            )

        assert result is not None
        kospi = result['markets']['kospi']
        kosdaq = result['markets']['kosdaq']
        self.assertEqual(kospi['breadth_completeness']['status'], 'verified')
        self.assertEqual(kospi['breadth_completeness']['expected_symbol_count'], 940)
        self.assertEqual(kospi['breadth_completeness']['observed_symbol_count'], 936)
        self.assertEqual(kospi['breadth_completeness']['coverage_ratio'], 0.995745)
        self.assertEqual(
            kospi['breadth_completeness']['limit_bucket_semantics'],
            'included_in_direction_counts',
        )
        self.assertEqual(
            kospi['breadth_completeness']['verification_basis'],
            'kis_directional_buckets_with_same_date_completed_master_upper_bound',
        )
        self.assertEqual(
            kospi['breadth_completeness']['master_count_relation'],
            'upper_bound',
        )
        self.assertEqual(kosdaq['breadth_completeness']['status'], 'verified')
        self.assertEqual(kosdaq['breadth_completeness']['expected_symbol_count'], 1647)
        self.assertEqual(kosdaq['breadth_completeness']['observed_symbol_count'], 1638)
        self.assertEqual(
            kospi['breadth_completeness']['expected_universe']['provider'],
            'kis_master',
        )

    async def test_breadth_completeness_rejects_limit_bucket_subset_violations(self) -> None:
        service = MarketSnapshotService(
            kis_api_client=FakeKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        target = MARKET_SNAPSHOT_TARGETS[0]
        master_symbols = [
            SimpleNamespace(symbol=f'P{index:04d}', market='KOSPI')
            for index in range(940)
        ]
        master_state = SimpleNamespace(
            symbols=master_symbols,
            snapshot=lambda: {
                'market_date': '2026-04-29',
                'last_status': 'completed',
                'last_completed_at': '2026-04-29T00:00:00+00:00',
                'symbol_count': 940,
                'kospi_count': 940,
                'kosdaq_count': 0,
            },
        )
        base_row = {
            'provider_observed_at': '2026-04-29T10:05:00+09:00',
            'breadth_raw_buckets': {
                'upper_limit_count': 3,
                'advancers': 512,
                'unchanged': 94,
                'decliners': 330,
                'lower_limit_count': 1,
            },
        }

        with patch('app.market.snapshot.master_refresh_state', master_state):
            upper_violation = deepcopy(base_row)
            upper_violation['breadth_raw_buckets']['upper_limit_count'] = 513
            upper_result = service._build_breadth_completeness(
                target=target,
                market_row=upper_violation,
            )
            lower_violation = deepcopy(base_row)
            lower_violation['breadth_raw_buckets']['lower_limit_count'] = 331
            lower_result = service._build_breadth_completeness(
                target=target,
                market_row=lower_violation,
            )

        self.assertEqual(upper_result['status'], 'unverified')
        self.assertEqual(upper_result['reason'], 'provider_limit_bucket_subset_invalid')
        self.assertEqual(lower_result['status'], 'unverified')
        self.assertEqual(lower_result['reason'], 'provider_limit_bucket_subset_invalid')

    async def test_breadth_completeness_rejects_directional_count_above_master(self) -> None:
        service = MarketSnapshotService(
            kis_api_client=FakeKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        target = MARKET_SNAPSHOT_TARGETS[0]
        master_symbols = [
            SimpleNamespace(symbol=f'P{index:04d}', market='KOSPI')
            for index in range(940)
        ]
        master_state = SimpleNamespace(
            symbols=master_symbols,
            snapshot=lambda: {
                'market_date': '2026-04-29',
                'last_status': 'completed',
                'last_completed_at': '2026-04-29T00:00:00+00:00',
                'symbol_count': 940,
                'kospi_count': 940,
                'kosdaq_count': 0,
            },
        )
        market_row = {
            'provider_observed_at': '2026-04-29T10:05:00+09:00',
            'breadth_raw_buckets': {
                'upper_limit_count': 3,
                'advancers': 520,
                'unchanged': 94,
                'decliners': 327,
                'lower_limit_count': 1,
            },
        }

        with patch('app.market.snapshot.master_refresh_state', master_state):
            result = service._build_breadth_completeness(
                target=target,
                market_row=market_row,
            )

        self.assertEqual(result['status'], 'unverified')
        self.assertEqual(
            result['reason'],
            'provider_directional_count_exceeds_master_upper_bound',
        )

    async def test_breadth_completeness_rejects_truncated_directional_coverage(self) -> None:
        service = MarketSnapshotService(
            kis_api_client=FakeKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        target = MARKET_SNAPSHOT_TARGETS[0]
        master_symbols = [
            SimpleNamespace(symbol=f'P{index:04d}', market='KOSPI')
            for index in range(940)
        ]
        master_state = SimpleNamespace(
            symbols=master_symbols,
            snapshot=lambda: {
                'market_date': '2026-04-29',
                'last_status': 'completed',
                'last_completed_at': '2026-04-29T00:00:00+00:00',
                'symbol_count': 940,
                'kospi_count': 940,
                'kosdaq_count': 0,
            },
        )
        market_row = {
            'provider_observed_at': '2026-04-29T10:05:00+09:00',
            'breadth_raw_buckets': {
                'upper_limit_count': 1,
                'advancers': 300,
                'unchanged': 50,
                'decliners': 400,
                'lower_limit_count': 1,
            },
        }

        with patch('app.market.snapshot.master_refresh_state', master_state):
            result = service._build_breadth_completeness(
                target=target,
                market_row=market_row,
            )

        self.assertEqual(result['status'], 'unverified')
        self.assertEqual(
            result['reason'],
            'provider_directional_coverage_below_minimum',
        )
        self.assertEqual(result['coverage_ratio'], 0.797872)

    async def test_flow_failure_keeps_fresh_index_leg_available(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        service = MarketSnapshotService(
            kis_api_client=FailingFlowKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )

        result = await service.refresh_market_snapshot_if_due(trigger='flow_failure', force=True)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['legs']['index']['status'], 'ready')
        self.assertEqual(result['legs']['flow']['status'], 'unavailable')
        self.assertEqual(result['legs']['fx']['status'], 'ready')
        self.assertEqual(result['markets']['kospi']['index_leg']['status'], 'ready')
        self.assertEqual(result['markets']['kospi']['flow_leg']['status'], 'unavailable')
        self.assertEqual(result['markets']['kospi']['value'], 2642.07)
        self.assertIsNone(result['markets']['kospi']['foreign_net_value'])

    async def test_missing_fx_configuration_does_not_block_index_leg(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        settings.koreaexim_api_key = ''
        fx_service = FakeFXService()
        service = MarketSnapshotService(
            kis_api_client=FakeKISClient(),
            koreaexim_service=fx_service,
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )

        result = await service.refresh_market_snapshot_if_due(trigger='fx_unconfigured', force=True)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['legs']['index']['status'], 'ready')
        self.assertEqual(result['legs']['fx']['status'], 'missing')
        self.assertEqual(result['usd_krw']['stale_reason'], 'koreaexim_not_configured')
        self.assertEqual(fx_service.calls, [])
        self.assertTrue(service.runtime_status()['ready'])

    async def test_missing_provider_time_proof_preserves_price_but_marks_index_unverified(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        service = MarketSnapshotService(
            kis_api_client=FailingIndexObservationKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )

        result = await service.refresh_market_snapshot_if_due(trigger='observation_failure', force=True)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['legs']['index']['status'], 'unverified')
        self.assertEqual(result['markets']['kospi']['value'], 2642.07)
        self.assertEqual(result['markets']['kospi']['index_leg']['status'], 'unverified')
        self.assertIsNone(result['markets']['kospi']['provider_observed_at'])
        self.assertIsNone(result['as_of'])

    async def test_nonmonotonic_price_move_does_not_invalidate_same_provider_turnover_proof(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        service = MarketSnapshotService(
            kis_api_client=MismatchedIndexObservationKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )

        result = await service.refresh_market_snapshot_if_due(
            trigger='mismatched_index_observation',
            force=True,
        )

        assert result is not None
        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['markets']['kospi']['value'], 2642.07)
        self.assertEqual(result['markets']['kospi']['index_leg']['status'], 'ready')
        self.assertTrue(
            result['markets']['kospi']['index_leg']['observation_payload_match']
        )

    async def test_async_current_reads_bracket_intraday_turnover(self) -> None:
        clock = {
            'now': datetime.fromisoformat('2026-04-29T10:05:00+09:00')
        }
        client = BracketedIndexKISClient(
            clock=clock,
            before_turnover=4_736_153,
            bar_turnover=4_736_160,
            after_turnover=4_736_170,
        )
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        service._now = lambda: clock['now']

        result = await service._fetch_market_row(
            client,
            MARKET_SNAPSHOT_TARGETS[0],
            include_optional_legs=False,
            cached_market_row=None,
        )

        self.assertEqual(client.index_calls, ['0001', '0001'])
        self.assertEqual(client.index_time_calls, ['0001'])
        self.assertEqual(result['value'], 2643.10)
        self.assertEqual(result['trading_value'], 4_736_170)
        self.assertEqual(result['index_leg']['status'], 'ready')
        self.assertTrue(result['index_leg']['observation_payload_match'])
        self.assertEqual(
            result['index_leg']['collected_at'],
            '2026-04-29T10:05:15+09:00',
        )

    async def test_decreasing_current_turnover_fails_provider_bracket(self) -> None:
        clock = {
            'now': datetime.fromisoformat('2026-04-29T10:05:00+09:00')
        }
        client = BracketedIndexKISClient(
            clock=clock,
            before_turnover=4_736_170,
            bar_turnover=4_736_170,
            after_turnover=4_736_150,
            before_price='2642.07',
            bar_price='2642.07',
            after_price='2642.07',
            provider_session_time='100500',
        )
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        service._now = lambda: clock['now']

        result = await service._fetch_market_row(
            client,
            MARKET_SNAPSHOT_TARGETS[0],
            include_optional_legs=False,
            cached_market_row=None,
        )

        self.assertEqual(client.index_calls, ['0001', '0001'])
        self.assertEqual(result['index_leg']['status'], 'unverified')
        self.assertFalse(result['index_leg']['observation_payload_match'])

    async def test_client_collection_time_never_relabels_prior_session_provider_row(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        service = MarketSnapshotService(
            kis_api_client=PriorSessionIndexObservationKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        service._now = lambda: datetime.fromisoformat('2026-04-29T10:05:20+09:00')

        result = await service.refresh_market_snapshot_if_due(
            trigger='prior_session_provenance',
            force=True,
        )

        assert result is not None
        self.assertEqual(result['as_of'], '2026-04-28T10:05:00+09:00')
        self.assertEqual(result['collected_at'], '2026-04-29T10:05:20+09:00')
        self.assertEqual(
            result['markets']['kospi']['provider_business_date'],
            '2026-04-28',
        )
        self.assertEqual(result['markets']['kospi']['index_leg']['status'], 'unverified')
        self.assertEqual(
            result['markets']['kospi']['provider_observed_at'],
            '2026-04-28T10:05:00+09:00',
        )

    async def test_after_hours_collection_does_not_mark_closing_bar_fresh(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        service = MarketSnapshotService(
            kis_api_client=AfterHoursIndexObservationKISClient(),
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        service._now = lambda: datetime.fromisoformat('2026-04-29T20:00:00+09:00')

        result = await service.refresh_market_snapshot_if_due(
            trigger='after_hours_provenance',
            force=True,
        )

        assert result is not None
        self.assertEqual(result['markets']['kospi']['provider_observed_at'], '2026-04-29T15:30:00+09:00')
        self.assertEqual(result['markets']['kospi']['index_leg']['status'], 'unverified')
        self.assertEqual(result['legs']['index']['status'], 'unverified')

    async def test_weekend_refresh_does_not_relabel_prior_session_bar(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        client = FakeKISClient()
        client.provider_business_date = '20260424'
        client.provider_session_time = '153000'
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )
        service._now = lambda: datetime.fromisoformat('2026-04-26T10:00:00+09:00')

        result = await service.refresh_market_snapshot_if_due(
            trigger='weekend_provenance',
            force=True,
        )

        assert result is not None
        self.assertEqual(result['collected_at'], '2026-04-26T10:00:00+09:00')
        self.assertEqual(result['as_of'], '2026-04-24T15:30:00+09:00')
        self.assertEqual(result['markets']['kospi']['provider_business_date'], '2026-04-24')
        self.assertEqual(result['markets']['kospi']['index_leg']['status'], 'unverified')

    async def test_concurrent_forced_refreshes_share_one_process_local_flight(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        client = BlockingKISClient()
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )

        first = asyncio.create_task(
            service.refresh_market_snapshot_if_due(trigger='concurrent:first', force=True)
        )
        await client.index_started.wait()
        second = asyncio.create_task(
            service.refresh_market_snapshot_if_due(trigger='concurrent:second', force=True)
        )
        await asyncio.sleep(0)
        client.release_index.set()
        first_result, second_result = await asyncio.gather(first, second)

        self.assertIsNotNone(first_result)
        self.assertIsNotNone(second_result)
        self.assertEqual(client.token_calls, 1)
        self.assertEqual(len(client.index_calls), 4)
        self.assertEqual(client.index_calls.count('0001'), 2)
        self.assertEqual(client.index_calls.count('1001'), 2)
        self.assertEqual(client.index_time_calls, ['0001', '1001'])

    async def test_index_refresh_skips_provider_when_another_process_owns_bucket(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        client = FakeKISClient()
        cache_store = InMemoryCacheStore()
        cache_store.claim_result = False
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=FakeFXService(),
            cache_store=cache_store,
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )

        result = await service.refresh_market_snapshot_if_due(
            trigger='other_process_same_bucket',
            force_index=True,
        )

        self.assertIsNone(result)
        self.assertEqual(client.token_calls, 0)
        self.assertEqual(len(cache_store.claim_calls), 1)
        self.assertEqual(
            cache_store.claim_calls[0]['refresh_trigger'],
            'other_process_same_bucket',
        )

    async def test_forced_index_refresh_reuses_optional_legs_until_their_own_cadence_is_due(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        client = FakeKISClient()
        fx_service = FakeFXService()
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=fx_service,
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
            refresh_interval_seconds=3600,
        )
        current = datetime.fromisoformat('2026-04-29T09:00:20+09:00')
        client.provider_session_time = '090000'
        service._now = lambda: current
        await service.refresh_market_snapshot_if_due(trigger='initial', force=True)
        current = datetime.fromisoformat('2026-04-29T09:05:20+09:00')
        client.provider_session_time = '090500'

        result = await service.refresh_market_snapshot_if_due(
            trigger='five_minute_index',
            force_index=True,
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(len(client.index_calls), 8)
        self.assertEqual(client.index_calls.count('0001'), 4)
        self.assertEqual(client.index_calls.count('1001'), 4)
        self.assertEqual(client.flow_calls, [('KSP', '0001'), ('KSQ', '1001')])
        self.assertEqual(len(fx_service.calls), 1)
        self.assertEqual(result['legs']['flow']['status'], 'cached')
        self.assertEqual(result['legs']['fx']['status'], 'cached')
        self.assertEqual(result['markets']['kospi']['flow_leg']['status'], 'cached')
        self.assertEqual(result['markets']['kospi']['foreign_net_value'], -1_455_933_000_000)

    async def test_index_only_refresh_does_not_relabel_unavailable_flow_as_cached(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        client = FailingFlowKISClient()
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
            refresh_interval_seconds=3600,
        )
        current = datetime.fromisoformat('2026-04-29T10:05:20+09:00')
        service._now = lambda: current
        await service.refresh_market_snapshot_if_due(trigger='flow_failure', force=True)
        current = datetime.fromisoformat('2026-04-29T10:10:20+09:00')
        client.provider_session_time = '101000'

        result = await service.refresh_market_snapshot_if_due(
            trigger='index_only_after_flow_failure',
            force_index=True,
        )

        assert result is not None
        self.assertEqual(result['markets']['kospi']['index_leg']['status'], 'ready')
        self.assertEqual(result['markets']['kospi']['flow_leg']['status'], 'unavailable')
        self.assertEqual(result['legs']['flow']['status'], 'unavailable')
        self.assertEqual(result['status'], 'partial')

    async def test_index_only_refresh_does_not_relabel_missing_fx_as_cached(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        settings.koreaexim_api_key = ''
        client = FakeKISClient()
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=FakeFXService(),
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
            refresh_interval_seconds=3600,
        )
        current = datetime.fromisoformat('2026-04-29T10:05:20+09:00')
        service._now = lambda: current
        await service.refresh_market_snapshot_if_due(trigger='fx_missing', force=True)
        current = datetime.fromisoformat('2026-04-29T10:10:20+09:00')
        client.provider_session_time = '101000'

        result = await service.refresh_market_snapshot_if_due(
            trigger='index_only_after_fx_missing',
            force_index=True,
        )

        assert result is not None
        self.assertEqual(result['markets']['kospi']['index_leg']['status'], 'ready')
        self.assertEqual(result['legs']['fx']['status'], 'missing')
        self.assertEqual(result['usd_krw']['status'], 'missing')
        self.assertEqual(result['status'], 'partial')

    async def test_index_only_refresh_does_not_delay_hourly_optional_leg_due_time(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        client = FakeKISClient()
        fx_service = FakeFXService()
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=fx_service,
            cache_store=InMemoryCacheStore(),
            fx_snapshot_store_instance=self.fx_snapshot_store,
            refresh_interval_seconds=3600,
        )
        current = datetime.fromisoformat('2026-04-29T14:00:20+09:00')
        client.provider_session_time = '140000'
        service._now = lambda: current
        await service.refresh_market_snapshot_if_due(trigger='initial', force=True)

        current = datetime.fromisoformat('2026-04-29T14:30:20+09:00')
        client.provider_session_time = '143000'
        index_only_result = await service.refresh_market_snapshot_if_due(
            trigger='last_index_window_bucket',
            force_index=True,
        )
        assert index_only_result is not None
        self.assertEqual(
            index_only_result['freshness']['next_refresh_due_at'],
            '2026-04-29T15:00:20+09:00',
        )

        current = datetime.fromisoformat('2026-04-29T15:00:20+09:00')
        client.provider_session_time = '150000'
        result = await service.refresh_market_snapshot_if_due(
            trigger='regular_hourly_due',
        )

        assert result is not None
        self.assertEqual(len(client.index_calls), 12)
        self.assertEqual(client.index_calls.count('0001'), 6)
        self.assertEqual(client.index_calls.count('1001'), 6)
        self.assertEqual(client.flow_calls, [('KSP', '0001'), ('KSQ', '1001')] * 2)
        self.assertEqual(len(fx_service.calls), 2)
        self.assertEqual(result['legs']['flow']['status'], 'ready')
        self.assertEqual(result['legs']['fx']['status'], 'ready')

    async def test_get_market_snapshot_normalizes_legacy_cached_investor_flow_values(self) -> None:
        settings.kis_use_mock = False
        settings.market_snapshot_kis_use_real = True
        client = FakeKISClient()
        fx_service = FakeFXService()
        cache_store = InMemoryCacheStore()
        cached_at = datetime.fromisoformat('2026-04-29T10:05:12+09:00')
        cache_store.record = MarketSnapshotCacheRecord(
            payload={
                'as_of': cached_at.isoformat(),
                'status': 'ready',
                'missing_fields': [],
                'stale_reason': None,
                'markets': {
                    'kospi': {
                        'label': 'KOSPI',
                        'index_code': '0001',
                        'value': 2642.07,
                        'change': 2.78,
                        'change_percent': 0.11,
                        'open': 2651.63,
                        'high': 2653.87,
                        'low': 2634.29,
                        'volume': 305715,
                        'trading_value': 4736153,
                        'advancers': 512,
                        'unchanged': 94,
                        'decliners': 330,
                        'foreign_net_quantity': -5290,
                        'foreign_net_value': -1455933,
                        'institution_net_quantity': 720,
                        'institution_net_value': 283792,
                    },
                    'kosdaq': {
                        'label': 'KOSDAQ',
                        'index_code': '1001',
                        'value': 857.60,
                        'change': -1.61,
                        'change_percent': -0.19,
                        'open': 863.69,
                        'high': 864.24,
                        'low': 854.72,
                        'volume': 1312496,
                        'trading_value': 11507962,
                        'advancers': 828,
                        'unchanged': 94,
                        'decliners': 716,
                        'foreign_net_quantity': -18565,
                        'foreign_net_value': -211205,
                        'institution_net_quantity': -4738,
                        'institution_net_value': -304502,
                    },
                },
                'usd_krw': {
                    'pair': 'USD/KRW',
                    'rate': 1444.5,
                    'effective_date': '2026-04-29',
                    'fallback_applied': False,
                    'status': 'ready',
                    'source': 'live',
                    'stale_reason': None,
                    'detail': None,
                    'captured_at': cached_at.isoformat(),
                },
            },
            snapshot_status='ready',
            stale_reason=None,
            as_of=cached_at,
            last_refresh_attempted_at=cached_at,
            last_refresh_succeeded_at=cached_at,
            last_refresh_status='success',
            last_refresh_error=None,
            refresh_trigger='seed_cache',
            created_at=cached_at,
            updated_at=cached_at,
        )
        service = MarketSnapshotService(
            kis_api_client=client,
            koreaexim_service=fx_service,
            cache_store=cache_store,
            fx_snapshot_store_instance=self.fx_snapshot_store,
        )

        result = await service.get_market_snapshot()

        self.assertEqual(client.token_calls, 0)
        self.assertEqual(len(fx_service.calls), 0)
        self.assertEqual(result['markets']['kospi']['trading_value_unit'], 'million_krw')
        self.assertEqual(result['markets']['kospi']['foreign_net_value'], -1_455_933_000_000)
        self.assertEqual(result['markets']['kospi']['institution_net_value'], 283_792_000_000)
        self.assertEqual(result['markets']['kospi']['investor_flow_value_unit'], 'krw')
        self.assertEqual(result['markets']['kosdaq']['foreign_net_value'], -211_205_000_000)
        self.assertEqual(result['markets']['kosdaq']['institution_net_value'], -304_502_000_000)
        self.assertEqual(result['markets']['kosdaq']['investor_flow_value_unit'], 'krw')
        self.assertTrue(result['freshness']['served_from_cache'])

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
