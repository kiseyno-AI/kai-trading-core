import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.brokers.kis.client import KISAPIError, KISHTTPError, KISTimeoutError, KISTransportError
from app.core.settings import settings
from app.market.kis_intraday_snapshots import (
    KIS_INTRADAY_SNAPSHOT_SOURCE,
    IntradaySymbolInput,
    KISIntradaySnapshotService,
)


class FakeKISIntradayClient:
    async def fetch_domestic_quote(self, symbol: str, market_code: str = 'J') -> dict:
        return {
            'output': {
                'stck_prpr': '71550',
                'prdy_vrss': '50',
                'prdy_ctrt': '0.07',
                'acml_vol': '150000',
                'acml_tr_pbmn': '9000000000',
                'bidp': '71500',
                'askp': '71600',
                'stck_cntg_hour': '101530',
                'stck_bsop_date': '20260507',
            }
        }

    async def fetch_domestic_orderbook(self, symbol: str, market_code: str = 'J') -> dict:
        return {
            'output1': {
                'aspr_acpt_hour': '101531',
                'askp1': '71600',
                'bidp1': '71500',
                'askp_rsqn1': '1200',
                'bidp_rsqn1': '1500',
                'total_askp_rsqn': '8400',
                'total_bidp_rsqn': '9800',
                'ntby_aspr_rsqn': '1400',
            },
            'output2': {
                'antc_cnpr': '71550',
                'antc_cntg_vrss': '50',
                'antc_cntg_prdy_ctrt': '0.07',
                'antc_vol': '1000',
            },
        }

    async def fetch_domestic_program_trade_by_stock(self, symbol: str, market_code: str = 'J') -> dict:
        return {
            'output': [
                {
                    'bsop_hour': '101530',
                    'stck_prpr': '71550',
                    'acml_vol': '150000',
                    'whol_smtn_ntby_qty': '2300',
                    'whol_smtn_ntby_tr_pbmn': '164565000',
                    'whol_smtn_seln_vol': '8200',
                    'whol_smtn_shnu_vol': '10500',
                    'whol_smtn_seln_tr_pbmn': '586710000',
                    'whol_smtn_shnu_tr_pbmn': '751275000',
                }
            ]
        }

    async def fetch_domestic_investor_trend_estimate(self, symbol: str) -> dict:
        return {
            'output2': [
                {
                    'bsop_hour_gb': '4',
                    'frgn_fake_ntby_qty': '1200',
                    'orgn_fake_ntby_qty': '900',
                    'sum_fake_ntby_qty': '2100',
                }
            ]
        }

    async def fetch_domestic_news_titles(self, **kwargs) -> dict:
        return {
            'output': [
                {
                    'cntt_usiq_srno': '1',
                    'news_ofer_entp_code': '01',
                    'data_dt': '20260507',
                    'data_tm': '100500',
                    'hts_pbnt_titl_cntt': '삼성전자 실적 기대감에 강세',
                    'news_lrdv_code': '10',
                    'dorg': '연합뉴스',
                    'iscd1': '005930',
                },
                {
                    'cntt_usiq_srno': '2',
                    'news_ofer_entp_code': '02',
                    'data_dt': '20260507',
                    'data_tm': '095500',
                    'hts_pbnt_titl_cntt': '삼성전자 대형 공급 계약 체결',
                    'news_lrdv_code': '10',
                    'dorg': '한국경제',
                    'iscd1': '005930',
                },
            ],
            'page_count': 1,
            'max_pages_reached': False,
        }

    async def fetch_domestic_ranking_volume_power(self, **kwargs) -> dict:
        return {
            'output': [
                {
                    'stck_shrn_iscd': '005930',
                    'data_rank': '3',
                    'tday_rltv': '138.5',
                    'shnu_cnqn_smtn': '1020',
                    'seln_cnqn_smtn': '740',
                }
            ],
            'page_count': 1,
            'max_pages_reached': False,
        }

    async def fetch_domestic_volume_rank(self, **kwargs) -> dict:
        return {
            'output': [
                {
                    'mksc_shrn_iscd': '005930',
                    'data_rank': '9',
                }
            ]
        }

    async def fetch_domestic_quote_balance_ranking(self, **kwargs) -> dict:
        return {
            'output': [
                {
                    'mksc_shrn_iscd': '005930',
                    'data_rank': '4',
                    'total_askp_rsqn': '8400',
                    'total_bidp_rsqn': '9800',
                    'total_ntsl_bidp_rsqn': '1400',
                    'shnu_rsqn_rate': '53.8',
                    'seln_rsqn_rate': '46.2',
                }
            ],
            'page_count': 1,
            'max_pages_reached': False,
        }


def _collected_item(
    symbol: str,
    *,
    availability_status: str = 'available',
) -> dict:
    return {
        'symbol': symbol,
        'name_ko': None,
        'market': 'KRX',
        'market_code': 'J',
        'source': KIS_INTRADAY_SNAPSHOT_SOURCE,
        'feature_version': KIS_INTRADAY_SNAPSHOT_SOURCE,
        'availability_status': availability_status,
        'freshness_status': 'fresh',
        'as_of_market_date': '2026-05-07',
        'as_of_timestamp': '2026-05-07T10:15:30+09:00',
        'snapshot_payload': {
            'symbol': symbol,
            'availability_status': availability_status,
        },
    }


class KISIntradaySnapshotServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.original_app_key = settings.kis_app_key
        self.original_app_secret = settings.kis_app_secret
        self.original_enabled = settings.kis_intraday_snapshot_enabled
        settings.kis_app_key = 'test-key'
        settings.kis_app_secret = 'test-secret'
        settings.kis_intraday_snapshot_enabled = True
        self.now_kst_patcher = patch(
            'app.market.kis_intraday_snapshots._now_kst',
            return_value=datetime.fromisoformat(
                '2026-05-07T10:15:35+09:00'
            ),
        )
        self.now_kst_patcher.start()

    def tearDown(self) -> None:
        self.now_kst_patcher.stop()
        settings.kis_app_key = self.original_app_key
        settings.kis_app_secret = self.original_app_secret
        settings.kis_intraday_snapshot_enabled = self.original_enabled

    async def test_transient_investor_failure_retries_only_that_surface(self) -> None:
        client = FakeKISIntradayClient()
        investor_response = await client.fetch_domestic_investor_trend_estimate('115480')
        client.fetch_domestic_investor_trend_estimate = AsyncMock(side_effect=[
            KISHTTPError('KIS request failed [500]: 초당 거래건수를 초과하였습니다.', status_code=500, retryable=True),
            investor_response,
        ])
        for method in ('fetch_domestic_quote', 'fetch_domestic_orderbook', 'fetch_domestic_program_trade_by_stock'):
            setattr(client, method, AsyncMock(wraps=getattr(client, method)))
        service = KISIntradaySnapshotService(client=client)
        with (
            patch.object(service, '_capture_universe', return_value=[IntradaySymbolInput('115480', None, 'KRX')]),
            patch('app.market.kis_intraday_snapshots.async_get_market_state', return_value=SimpleNamespace(
                state='CLOSED', is_trading_day=True, server_time='2026-05-07T16:15:35+09:00',
            )),
            patch('app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot') as persist,
            patch('app.market.kis_intraday_snapshots.asyncio.sleep', new_callable=AsyncMock) as sleep,
        ):
            result = await service.collect_snapshots(symbols=['115480'], include_news=False)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(client.fetch_domestic_investor_trend_estimate.await_count, 2)
        client.fetch_domestic_quote.assert_awaited_once()
        client.fetch_domestic_orderbook.assert_awaited_once()
        client.fetch_domestic_program_trade_by_stock.assert_awaited_once()
        sleep.assert_awaited_once_with(1.0)
        payload = result['items'][0]
        self.assertEqual(payload['quote']['price'], 71550)
        self.assertEqual(payload['investor_program_activity']['investor_trend_estimate']['combined_estimated_net_quantity'], 2100)
        self.assertNotIn('investor_trend_estimate', payload['provenance'].get('surface_errors', {}))
        persist.assert_called_once()

    async def test_surface_exhaustion_preserves_error_and_auth_business_errors_do_not_retry(self) -> None:
        cases = (
            (KISHTTPError('original KIS HTTP 500 rate-limit error', status_code=500), 4, [1.0, 2.0, 4.0]),
            (KISHTTPError('original KIS HTTP 403 authorization error', status_code=403, retryable=True), 1, []),
            (KISAPIError('original KIS business rejection', status_code=200, msg_code='BUSINESS_ERROR', retryable=True), 1, []),
        )
        for error, attempts, delays in cases:
            with self.subTest(error=str(error)):
                client = FakeKISIntradayClient()
                client.fetch_domestic_investor_trend_estimate = AsyncMock(side_effect=error)
                client.fetch_domestic_quote = AsyncMock(wraps=client.fetch_domestic_quote)
                client.fetch_domestic_orderbook = AsyncMock(wraps=client.fetch_domestic_orderbook)
                service = KISIntradaySnapshotService(client=client)
                with (
                    patch.object(service, '_capture_universe', return_value=[IntradaySymbolInput('115480', None, 'KRX')]),
                    patch('app.market.kis_intraday_snapshots.async_get_market_state', return_value=SimpleNamespace(
                        state='CLOSED', is_trading_day=True, server_time='2026-05-07T16:15:35+09:00',
                    )),
                    patch('app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot') as persist,
                    patch('app.market.kis_intraday_snapshots.asyncio.sleep', new_callable=AsyncMock) as sleep,
                ):
                    result = await service.collect_snapshots(symbols=['115480'], include_news=False)
                self.assertEqual(result['status'], 'partial')
                self.assertEqual(client.fetch_domestic_investor_trend_estimate.await_count, attempts)
                self.assertEqual([item.args[0] for item in sleep.await_args_list], delays)
                client.fetch_domestic_quote.assert_awaited_once()
                client.fetch_domestic_orderbook.assert_awaited_once()
                payload = result['items'][0]
                self.assertEqual(payload['quote']['price'], 71550)
                self.assertEqual(payload['provenance']['surface_errors']['investor_trend_estimate'], str(error))
                self.assertEqual(payload['investor_program_activity']['source_statuses']['investor_trend_estimate'], 'unavailable')
                self.assertNotIn('investor_trend_estimate', payload['investor_program_activity'])
                persist.assert_called_once()

    async def test_timeout_transport_429_and_kis_rate_code_retry_but_valid_empty_does_not(self) -> None:
        for error in (
            KISTimeoutError('KIS timed out'),
            KISTransportError('KIS connection reset'),
            KISHTTPError('KIS HTTP 429', status_code=429),
            KISAPIError('KIS rate limit', status_code=200, msg_code='EGW00201'),
            None,
        ):
            with self.subTest(error=type(error).__name__):
                client = FakeKISIntradayClient()
                client.fetch_domestic_investor_trend_estimate = AsyncMock(
                    side_effect=[error, {'output2': []}] if error else None, return_value={'output2': []},
                )
                service = KISIntradaySnapshotService(client=client)
                with (
                    patch.object(service, '_capture_universe', return_value=[IntradaySymbolInput('115480', None, 'KRX')]),
                    patch('app.market.kis_intraday_snapshots.async_get_market_state', return_value=SimpleNamespace(
                        state='CLOSED', is_trading_day=True, server_time='2026-05-07T16:15:35+09:00',
                    )),
                    patch('app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot'),
                    patch('app.market.kis_intraday_snapshots.asyncio.sleep', new_callable=AsyncMock) as sleep,
                ):
                    result = await service.collect_snapshots(symbols=['115480'], include_news=False)
                self.assertEqual(result['status'], 'ok')
                self.assertEqual(client.fetch_domestic_investor_trend_estimate.await_count, 2 if error else 1)
                self.assertEqual(sleep.await_count, 1 if error else 0)
                self.assertEqual(result['items'][0]['investor_program_activity']['source_statuses']['investor_trend_estimate'], 'empty')

    async def test_collect_snapshots_normalizes_execution_context_and_persists_rows(self) -> None:
        service = KISIntradaySnapshotService(client=FakeKISIntradayClient())

        with (
            patch(
                'app.market.kis_intraday_snapshots.async_get_market_state',
                return_value=SimpleNamespace(
                    state='CLOSED',
                    is_trading_day=True,
                    server_time='2026-05-07T16:15:35+09:00',
                ),
            ),
            patch('app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot') as persist_snapshot,
        ):
            result = await service.collect_snapshots(symbols=['005930'], max_ranking_pages=1, max_news_items=2)

        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['market_state'], 'CLOSED')
        self.assertEqual(result['snapshots_persisted'], 1)
        self.assertEqual(result['summary']['available_count'], 1)
        persist_snapshot.assert_called_once()

        payload = result['items'][0]
        self.assertEqual(payload['availability_status'], 'available')
        self.assertEqual(payload['quote']['spread_points'], 100)
        self.assertAlmostEqual(payload['orderbook']['imbalance'], 0.076923, places=5)
        self.assertEqual(payload['liquidity']['volume_power_rank'], 3)
        self.assertEqual(payload['liquidity']['quote_balance_rank'], 4)
        self.assertEqual(
            payload['investor_program_activity']['program_trade']['net_buy_quantity'],
            2300,
        )
        self.assertEqual(
            payload['investor_program_activity']['investor_trend_estimate']['combined_estimated_net_quantity'],
            2100,
        )
        self.assertEqual(payload['news_hints']['item_count'], 2)
        self.assertEqual(payload['news_hints']['event_tags'], ['contract', 'earnings'])
        self.assertEqual(payload['coverage']['status'], 'available')
        self.assertEqual(payload['freshness_status'], 'fresh')

    async def test_collect_snapshots_skips_weekend_before_market_state_lookup(self) -> None:
        service = KISIntradaySnapshotService(client=FakeKISIntradayClient())

        with (
            patch(
                'app.market.kis_intraday_snapshots._now_kst',
                return_value=datetime.fromisoformat(
                    '2026-05-09T10:15:35+09:00'
                ),
            ),
            patch(
                'app.market.kis_intraday_snapshots.async_get_market_state',
                new=AsyncMock(
                    side_effect=AssertionError(
                        'weekend fast-path must not query KIS market state'
                    )
                ),
            ) as market_state,
            patch.object(
                service,
                '_capture_universe',
                side_effect=AssertionError('should not load universe'),
            ) as capture_universe,
            patch(
                'app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot',
                side_effect=AssertionError('should not persist snapshot'),
            ) as persist_snapshot,
        ):
            result = await service.collect_snapshots(symbols=['005930'])

        self.assertEqual(result['status'], 'market_closed')
        self.assertEqual(result['reason'], 'non_trading_day')
        self.assertEqual(result['market_state'], 'WEEKEND')
        self.assertEqual(result['market_date'], '2026-05-09')
        self.assertEqual(result['requested_symbols'], ['005930'])
        self.assertEqual(result['snapshots_persisted'], 0)
        market_state.assert_not_awaited()
        capture_universe.assert_not_called()
        persist_snapshot.assert_not_called()

    async def test_collect_snapshots_skips_calendar_holiday_before_provider_calls(self) -> None:
        service = KISIntradaySnapshotService(client=FakeKISIntradayClient())

        with (
            patch(
                'app.market.kis_intraday_snapshots.async_get_market_state',
                return_value=SimpleNamespace(
                    state='HOLIDAY',
                    is_trading_day=False,
                    server_time='2026-05-05T10:15:35+09:00',
                ),
            ),
            patch.object(
                service,
                '_capture_universe',
                side_effect=AssertionError('should not load universe'),
            ) as capture_universe,
            patch.object(
                service,
                '_load_shared_rankings',
                new=AsyncMock(side_effect=AssertionError('should not load rankings')),
            ) as load_rankings,
            patch.object(
                service,
                '_collect_symbol_snapshot',
                new=AsyncMock(side_effect=AssertionError('should not load symbol surfaces')),
            ) as collect_symbol,
            patch(
                'app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot',
                side_effect=AssertionError('should not persist snapshot'),
            ) as persist_snapshot,
        ):
            result = await service.collect_snapshots(symbols=['005930'])

        self.assertEqual(result['status'], 'market_closed')
        self.assertEqual(result['reason'], 'non_trading_day')
        self.assertEqual(result['market_state'], 'HOLIDAY')
        self.assertEqual(result['market_date'], '2026-05-05')
        self.assertEqual(result['requested_symbols'], ['005930'])
        self.assertEqual(result['summary']['matched_symbol_count'], 1)
        self.assertEqual(result['snapshots_persisted'], 0)
        self.assertFalse(result['items'])
        capture_universe.assert_not_called()
        load_rankings.assert_not_awaited()
        collect_symbol.assert_not_awaited()
        persist_snapshot.assert_not_called()

    async def test_collect_snapshots_treats_successful_empty_rankings_as_not_ranked(self) -> None:
        client = FakeKISIntradayClient()
        service = KISIntradaySnapshotService(client=client)

        with (
            patch.object(
                client,
                'fetch_domestic_ranking_volume_power',
                new=AsyncMock(
                    return_value={
                        'output': [],
                        'page_count': 1,
                        'max_pages_reached': False,
                    }
                ),
            ),
            patch.object(
                client,
                'fetch_domestic_volume_rank',
                new=AsyncMock(return_value={'output': []}),
            ),
            patch.object(
                client,
                'fetch_domestic_quote_balance_ranking',
                new=AsyncMock(
                    return_value={
                        'output': [],
                        'page_count': 1,
                        'max_pages_reached': False,
                    }
                ),
            ),
            patch(
                'app.market.kis_intraday_snapshots.async_get_market_state',
                return_value=SimpleNamespace(
                    state='OPEN',
                    is_trading_day=True,
                    server_time='2026-05-07T10:15:35+09:00',
                ),
            ),
            patch(
                'app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot'
            ),
        ):
            result = await service.collect_snapshots(
                symbols=['005930'],
                include_news=False,
            )

        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['summary']['available_count'], 1)
        payload = result['items'][0]
        self.assertEqual(payload['availability_status'], 'available')
        self.assertEqual(payload['liquidity']['status'], 'available')
        self.assertEqual(
            payload['liquidity']['source_statuses'],
            {
                'volume_power': 'not_ranked',
                'volume_rank': 'not_ranked',
                'quote_balance': 'not_ranked',
            },
        )
        self.assertEqual(payload['coverage']['total_blocks'], 4)
        self.assertEqual(payload['coverage']['excluded_blocks'], ['news_hints'])

    async def test_collect_snapshots_distinguishes_empty_optional_activity_from_errors(self) -> None:
        for case, program_response, program_error, status, block_status in (
            ('one empty', None, None, 'ok', 'available'),
            ('both empty', {'output': []}, None, 'ok', 'empty'),
            ('provider error', None, RuntimeError('provider timeout'), 'partial', 'partial'),
            ('malformed payload', {'output': 'invalid'}, None, 'partial', 'partial'),
        ):
            with self.subTest(case=case):
                client = FakeKISIntradayClient()
                client.fetch_domestic_investor_trend_estimate = AsyncMock(return_value={'output2': []})
                if program_response is not None or program_error is not None:
                    client.fetch_domestic_program_trade_by_stock = AsyncMock(
                        return_value=program_response, side_effect=program_error,
                    )

                service = KISIntradaySnapshotService(client=client)
                with (
                    patch(
                        'app.market.kis_intraday_snapshots.async_get_market_state',
                        return_value=SimpleNamespace(
                            state='CLOSED',
                            is_trading_day=True,
                            server_time='2026-05-07T16:15:35+09:00',
                        ),
                    ),
                    patch('app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot'),
                ):
                    result = await service.collect_snapshots(symbols=['005930'], include_news=False)

                self.assertEqual(result['status'], status)
                self.assertEqual(result['snapshots_persisted'], 1)
                self.assertEqual(result['snapshots_failed'], 0)
                self.assertEqual(result['retry_required'], status == 'partial')
                payload = result['items'][0]['investor_program_activity']
                self.assertEqual(payload['status'], block_status)
                self.assertEqual(payload['source_statuses']['investor_trend_estimate'], 'empty')
                self.assertNotIn('investor_trend_estimate', payload)

    async def test_collect_snapshots_is_disabled_without_credentials_before_universe_lookup(self) -> None:
        settings.kis_app_key = ''
        settings.kis_app_secret = ''
        client = FakeKISIntradayClient()
        service = KISIntradaySnapshotService(client=client)

        with (
            patch.object(service, '_capture_universe', side_effect=AssertionError('should not load universe')),
            patch.object(client, 'fetch_domestic_quote', new=AsyncMock(side_effect=AssertionError('should not call KIS'))),
        ):
            result = await service.collect_snapshots(symbols=['005930'])

        self.assertEqual(result['status'], 'disabled')
        self.assertEqual(result['reason'], 'kis_credentials_missing')
        self.assertEqual(result['requested_symbols'], ['005930'])
        self.assertEqual(result['summary']['matched_symbol_count'], 1)
        self.assertEqual(result['snapshots_persisted'], 0)
        self.assertFalse(result['items'])

    async def test_collect_snapshots_is_disabled_by_feature_flag_before_universe_lookup(self) -> None:
        settings.kis_intraday_snapshot_enabled = False
        client = FakeKISIntradayClient()
        service = KISIntradaySnapshotService(client=client)

        with (
            patch.object(service, '_capture_universe', side_effect=AssertionError('should not load universe')),
            patch.object(client, 'fetch_domestic_quote', new=AsyncMock(side_effect=AssertionError('should not call KIS'))),
        ):
            result = await service.collect_snapshots(symbols=['005930'])

        self.assertEqual(result['status'], 'disabled')
        self.assertEqual(result['reason'], 'kis_intraday_snapshot_disabled')
        self.assertEqual(result['requested_symbols'], ['005930'])
        self.assertEqual(result['summary']['matched_symbol_count'], 1)
        self.assertEqual(result['snapshots_persisted'], 0)
        self.assertFalse(result['items'])

    async def test_collect_snapshots_shared_source_failure_is_partial_for_full_scope(self) -> None:
        client = FakeKISIntradayClient()
        service = KISIntradaySnapshotService(client=client)
        symbol_inputs = [
            IntradaySymbolInput(symbol='005930', name_ko=None, market='KRX'),
            IntradaySymbolInput(symbol='000660', name_ko=None, market='KRX'),
        ]

        with (
            patch.object(service, '_capture_universe', return_value=symbol_inputs),
            patch.object(
                client,
                'fetch_domestic_ranking_volume_power',
                new=AsyncMock(side_effect=RuntimeError('x' * 1200)),
            ),
            patch.object(
                service,
                '_collect_symbol_snapshot',
                new=AsyncMock(
                    side_effect=lambda **kwargs: _collected_item(
                        kwargs['symbol_input'].symbol
                    )
                ),
            ),
            patch(
                'app.market.kis_intraday_snapshots.async_get_market_state',
                return_value=SimpleNamespace(
                    state='OPEN',
                    is_trading_day=True,
                    server_time='2026-05-07T10:15:35+09:00',
                ),
            ),
            patch('app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot'),
        ):
            result = await service.collect_snapshots(
                symbols=['005930', '000660'],
                include_news=False,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'shared_source_incomplete')
        self.assertEqual(result['partial_reasons'], ['shared_source_errors'])
        self.assertEqual(
            result['partial_details']['shared_error_sources'],
            ['volume_power'],
        )
        self.assertEqual(len(result['shared_source_errors']['volume_power']), 1000)
        self.assertEqual(result['summary']['available_count'], 2)
        self.assertEqual(result['retry_scope']['mode'], 'all_requested_symbols')
        self.assertEqual(result['retry_symbols'], ['005930', '000660'])
        self.assertEqual(result['retry_symbols_count'], 2)
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            ['005930', '000660'],
        )

    async def test_collect_snapshots_pagination_truncation_is_partial_for_full_scope(self) -> None:
        client = FakeKISIntradayClient()
        service = KISIntradaySnapshotService(client=client)
        symbol_inputs = [
            IntradaySymbolInput(symbol='005930', name_ko=None, market='KRX'),
            IntradaySymbolInput(symbol='000660', name_ko=None, market='KRX'),
        ]
        truncated_response = await client.fetch_domestic_ranking_volume_power()
        truncated_response['max_pages_reached'] = True

        with (
            patch.object(service, '_capture_universe', return_value=symbol_inputs),
            patch.object(
                client,
                'fetch_domestic_ranking_volume_power',
                new=AsyncMock(return_value=truncated_response),
            ),
            patch.object(
                service,
                '_collect_symbol_snapshot',
                new=AsyncMock(
                    side_effect=lambda **kwargs: _collected_item(
                        kwargs['symbol_input'].symbol
                    )
                ),
            ),
            patch(
                'app.market.kis_intraday_snapshots.async_get_market_state',
                return_value=SimpleNamespace(
                    state='OPEN',
                    is_trading_day=True,
                    server_time='2026-05-07T10:15:35+09:00',
                ),
            ),
            patch('app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot'),
        ):
            result = await service.collect_snapshots(
                symbols=['005930', '000660'],
                include_news=False,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'shared_source_incomplete')
        self.assertEqual(
            result['partial_reasons'],
            ['shared_source_pagination_truncated'],
        )
        self.assertEqual(
            result['partial_details']['pagination_truncated_sources'],
            ['volume_power'],
        )
        self.assertEqual(result['retry_scope']['mode'], 'all_requested_symbols')
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            ['005930', '000660'],
        )

    async def test_collect_snapshots_item_gap_retries_only_affected_symbol(self) -> None:
        service = KISIntradaySnapshotService(client=FakeKISIntradayClient())
        symbol_inputs = [
            IntradaySymbolInput(symbol='005930', name_ko=None, market='KRX'),
            IntradaySymbolInput(symbol='000660', name_ko=None, market='KRX'),
        ]

        with (
            patch.object(service, '_capture_universe', return_value=symbol_inputs),
            patch.object(
                service,
                '_load_shared_rankings',
                new=AsyncMock(return_value=({}, {}, {})),
            ),
            patch.object(
                service,
                '_collect_symbol_snapshot',
                new=AsyncMock(
                    side_effect=lambda **kwargs: _collected_item(
                        kwargs['symbol_input'].symbol,
                        availability_status=(
                            'partial'
                            if kwargs['symbol_input'].symbol == '000660'
                            else 'available'
                        ),
                    )
                ),
            ),
            patch(
                'app.market.kis_intraday_snapshots.async_get_market_state',
                return_value=SimpleNamespace(
                    state='OPEN',
                    is_trading_day=True,
                    server_time='2026-05-07T10:15:35+09:00',
                ),
            ),
            patch('app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot'),
        ):
            result = await service.collect_snapshots(
                symbols=['005930', '000660'],
                include_news=False,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'snapshot_items_incomplete')
        self.assertEqual(result['partial_reasons'], ['partial_items'])
        self.assertEqual(result['retry_scope']['mode'], 'affected_symbols')
        self.assertEqual(result['retry_symbols'], ['000660'])
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            ['000660'],
        )

    async def test_collect_snapshots_persistence_failure_is_partial(self) -> None:
        service = KISIntradaySnapshotService(client=FakeKISIntradayClient())
        symbol_input = IntradaySymbolInput(
            symbol='005930',
            name_ko=None,
            market='KRX',
        )

        with (
            patch.object(
                service,
                '_capture_universe',
                return_value=[symbol_input],
            ),
            patch.object(
                service,
                '_load_shared_rankings',
                new=AsyncMock(return_value=({}, {}, {})),
            ),
            patch.object(
                service,
                '_collect_symbol_snapshot',
                new=AsyncMock(return_value=_collected_item('005930')),
            ),
            patch(
                'app.market.kis_intraday_snapshots.async_get_market_state',
                return_value=SimpleNamespace(
                    state='OPEN',
                    is_trading_day=True,
                    server_time='2026-05-07T10:15:35+09:00',
                ),
            ),
            patch(
                'app.market.kis_intraday_snapshots.persist_kis_intraday_snapshot',
                side_effect=RuntimeError('p' * 1200),
            ),
        ):
            result = await service.collect_snapshots(
                symbols=['005930'],
                include_news=False,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'snapshot_failures')
        self.assertEqual(result['partial_reasons'], ['snapshot_failures'])
        self.assertEqual(result['snapshots_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['005930'])
        self.assertEqual(len(result['failed_symbol_errors'][0]['error']), 1000)
        self.assertEqual(result['retry_scope']['mode'], 'affected_symbols')
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            ['005930'],
        )

    def test_inspect_summarizes_current_rows(self) -> None:
        service = KISIntradaySnapshotService(client=FakeKISIntradayClient())

        with patch(
            'app.market.kis_intraday_snapshots.load_current_kis_intraday_snapshots',
            return_value=[
                {'symbol': '005930', 'availability_status': 'available', 'freshness_status': 'fresh'},
                {'symbol': '000660', 'availability_status': 'partial', 'freshness_status': 'stale'},
            ],
        ):
            result = service.inspect(symbols=['005930', '000660'], limit=10)

        self.assertEqual(result['summary']['entries'], 2)
        self.assertEqual(result['summary']['available_count'], 1)
        self.assertEqual(result['summary']['partial_count'], 1)
        self.assertEqual(result['summary']['fresh_count'], 1)
        self.assertEqual(result['summary']['stale_count'], 1)


if __name__ == '__main__':
    unittest.main()
