import unittest
from datetime import date
from unittest.mock import patch

from sqlalchemy import delete, select

from app.brokers.kis.client import KISHTTPError
from app.db import engine, session_scope
from app.db_models import DailyMarketRiskSnapshot, EODCollectionRun, Symbol
from app.market_data_store import load_daily_market_risk_snapshots
from app.repositories.eod_run_repository import create_eod_collection_run
from app.signals.market_risk_snapshot import (
    DailyMarketRiskSnapshotService,
    MarketRiskSymbolInput,
    build_market_risk_snapshot,
)


class FakeKISRiskClient:
    def __init__(self, responses: dict[str, dict], errors: dict[str, Exception] | None = None):
        self.responses = responses
        self.errors = errors or {}
        self.calls: list[str] = []

    async def fetch_domestic_quote_risk_status(self, symbol: str, market_code: str = 'J') -> dict:
        self.calls.append(symbol)
        error = self.errors.get(symbol)
        if isinstance(error, list):
            if error:
                next_error = error.pop(0)
                if next_error is not None:
                    raise next_error
        elif error is not None:
            raise error
        return self.responses[symbol]


class MarketRiskSnapshotNormalizationTests(unittest.TestCase):
    def test_vi_yn_codes_are_normalized_without_treating_n_as_triggered(self) -> None:
        inactive = build_market_risk_snapshot(
            symbol='005930',
            market_date='2026-04-14',
            quote_response={'output': {'vi_cls_code': 'N'}},
        )
        active = build_market_risk_snapshot(
            symbol='000660',
            market_date='2026-04-14',
            quote_response={'output': {'vi_cls_code': 'Y'}},
        )

        self.assertFalse(inactive['is_vi_triggered'])
        self.assertEqual(inactive['vi_evidence_status'], 'inactive')
        self.assertTrue(active['is_vi_triggered'])
        self.assertEqual(active['vi_evidence_status'], 'active')

    def test_unknown_vi_code_is_preserved_as_auditable_uncertainty(self) -> None:
        snapshot = build_market_risk_snapshot(
            symbol='005930',
            market_date='2026-04-14',
            quote_response={'output': {'vi_cls_code': 'unexpected'}},
        )

        self.assertFalse(snapshot['is_vi_triggered'])
        self.assertEqual(snapshot['vi_evidence_status'], 'unknown')
        self.assertEqual(snapshot['snapshot_payload']['vi_evidence_status'], 'unknown')

    def test_build_market_risk_snapshot_normalizes_quote_flags(self) -> None:
        snapshot = build_market_risk_snapshot(
            symbol='005930',
            market_date='2026-04-14',
            market='KOSPI',
            name_ko='삼성전자',
            quote_response={
                'output': {
                    'mrkt_warn_cls_code': '02',
                    'mrkt_warn_cls_name': '투자경고',
                    'mang_issu_yn': 'N',
                    'invt_caful_yn': 'Y',
                    'short_over_yn': 'Y',
                    'short_over_cls_code': '01',
                    'ssts_hot_yn': 'N',
                    'vi_cls_code': '01',
                    'insn_pbnt_yn': 'N',
                    'trht_yn': 'Y',
                }
            },
        )

        self.assertEqual(snapshot['market_date'], '2026-04-14')
        self.assertEqual(snapshot['market_warning_code'], '02')
        self.assertTrue(snapshot['is_investment_caution'])
        self.assertTrue(snapshot['is_short_overheated'])
        self.assertTrue(snapshot['is_vi_triggered'])
        self.assertTrue(snapshot['is_trade_halted'])
        self.assertEqual(snapshot['source_endpoint'], 'inquire-price-2')
        self.assertEqual(snapshot['snapshot_payload']['market_code'], 'J')


class DailyMarketRiskSnapshotServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_collect_for_market_date_continues_on_fetch_and_persist_failures(self) -> None:
        service = DailyMarketRiskSnapshotService(
            client=FakeKISRiskClient(
                responses={
                    '005930': {
                        'output': {
                            'mrkt_warn_cls_code': '01',
                            'mrkt_warn_cls_name': '투자주의',
                            'mang_issu_yn': 'N',
                            'invt_caful_yn': 'Y',
                            'short_over_yn': 'N',
                            'ssts_hot_yn': 'N',
                            'vi_cls_code': '00',
                            'insn_pbnt_yn': 'N',
                            'trht_yn': 'N',
                        }
                    },
                    '000660': {
                        'output': {
                            'mrkt_warn_cls_code': '00',
                            'mrkt_warn_cls_name': '해당없음',
                            'mang_issu_yn': 'N',
                            'invt_caful_yn': 'N',
                            'short_over_yn': 'N',
                            'ssts_hot_yn': 'N',
                            'vi_cls_code': '00',
                            'insn_pbnt_yn': 'N',
                            'trht_yn': 'N',
                        }
                    },
                },
                errors={'051910': RuntimeError('kis unavailable')},
            )
        )
        inputs = [
            MarketRiskSymbolInput(symbol='005930', name_ko='삼성전자', market='KOSPI'),
            MarketRiskSymbolInput(symbol='000660', name_ko='SK하이닉스', market='KOSPI'),
            MarketRiskSymbolInput(symbol='051910', name_ko='LG화학', market='KOSPI'),
        ]

        def upsert_side_effect(item: dict) -> None:
            if item['symbol'] == '000660':
                raise RuntimeError('write failed')

        with (
            patch.object(service, '_load_symbol_rows', return_value=(inputs, ['005930', '000660', '051910'], [])),
            patch(
                'app.signals.market_risk_snapshot.upsert_daily_market_risk_snapshots',
                side_effect=RuntimeError('bulk write failed'),
            ) as bulk_upsert,
            patch(
                'app.signals.market_risk_snapshot.upsert_daily_market_risk_snapshot',
                side_effect=upsert_side_effect,
            ) as upsert,
        ):
            result = await service.collect_for_market_date(market_date='2026-04-14')

        self.assertEqual(result['snapshots_persisted'], 1)
        self.assertEqual(result['snapshots_failed'], 2)
        self.assertEqual(result['summary']['matched_symbol_count'], 3)
        self.assertEqual(result['summary']['collected_count'], 2)
        self.assertEqual(result['failed_symbols'], ['051910', '000660'])
        self.assertEqual([item['symbol'] for item in result['items']], ['005930'])
        bulk_upsert.assert_called_once()
        self.assertEqual(upsert.call_count, 2)
        persisted_symbols = {call.args[0]['symbol'] for call in upsert.call_args_list}
        self.assertEqual(persisted_symbols, {'005930', '000660'})

    async def test_collect_for_market_date_retries_kis_rate_limit_errors(self) -> None:
        client = FakeKISRiskClient(
            responses={
                '005930': {
                    'output': {
                        'mrkt_warn_cls_code': '00',
                        'mang_issu_yn': 'N',
                        'invt_caful_yn': 'N',
                        'short_over_yn': 'N',
                        'ssts_hot_yn': 'N',
                        'vi_cls_code': '00',
                        'insn_pbnt_yn': 'N',
                        'trht_yn': 'N',
                    }
                }
            },
            errors={'005930': [RuntimeError('초당 거래건수를 초과하였습니다.'), None]},
        )
        service = DailyMarketRiskSnapshotService(client=client)
        inputs = [MarketRiskSymbolInput(symbol='005930', name_ko='삼성전자', market='KOSPI')]

        with (
            patch.object(service, '_load_symbol_rows', return_value=(inputs, ['005930'], [])),
            patch('app.signals.market_risk_snapshot.asyncio.sleep', return_value=None) as sleep,
            patch('app.signals.market_risk_snapshot.upsert_daily_market_risk_snapshots') as upsert,
        ):
            result = await service.collect_for_market_date(market_date='2026-04-14')

        self.assertEqual(client.calls, ['005930', '005930'])
        self.assertEqual(result['snapshots_persisted'], 1)
        self.assertEqual(result['snapshots_failed'], 0)
        upsert.assert_called_once()
        self.assertEqual(upsert.call_args.args[0][0]['symbol'], '005930')
        self.assertTrue(any(call.args[0] == 1.0 for call in sleep.call_args_list))

    async def test_collect_for_market_date_retries_kis_timeout_errors(self) -> None:
        client = FakeKISRiskClient(
            responses={
                '272110': {
                    'output': {
                        'mrkt_warn_cls_code': '00',
                        'mang_issu_yn': 'N',
                        'invt_caful_yn': 'N',
                        'short_over_yn': 'N',
                        'ssts_hot_yn': 'N',
                        'vi_cls_code': '00',
                        'insn_pbnt_yn': 'N',
                        'trht_yn': 'N',
                    }
                }
            },
            errors={
                '272110': [
                    RuntimeError('KIS request timed out for /uapi/domestic-stock/v1/quotations/inquire-price-2'),
                    None,
                ]
            },
        )
        service = DailyMarketRiskSnapshotService(client=client)
        inputs = [MarketRiskSymbolInput(symbol='272110', name_ko='케이엔제이', market='KOSDAQ')]

        with (
            patch.object(service, '_load_symbol_rows', return_value=(inputs, ['272110'], [])),
            patch('app.signals.market_risk_snapshot.asyncio.sleep', return_value=None) as sleep,
            patch('app.signals.market_risk_snapshot.upsert_daily_market_risk_snapshots') as upsert,
        ):
            result = await service.collect_for_market_date(market_date='2026-06-15')

        self.assertEqual(client.calls, ['272110', '272110'])
        self.assertEqual(result['snapshots_persisted'], 1)
        self.assertEqual(result['snapshots_failed'], 0)
        upsert.assert_called_once()
        self.assertEqual(upsert.call_args.args[0][0]['symbol'], '272110')
        self.assertTrue(any(call.args[0] == 1.0 for call in sleep.call_args_list))

    async def test_collect_for_market_date_reports_timeout_after_retry_exhaustion(self) -> None:
        timeout_error = RuntimeError(
            'KIS request timed out for /uapi/domestic-stock/v1/quotations/inquire-price-2'
        )
        client = FakeKISRiskClient(
            responses={},
            errors={'272110': [timeout_error, timeout_error, timeout_error, timeout_error]},
        )
        service = DailyMarketRiskSnapshotService(client=client)
        inputs = [MarketRiskSymbolInput(symbol='272110', name_ko='케이엔제이', market='KOSDAQ')]

        with (
            patch.object(service, '_load_symbol_rows', return_value=(inputs, ['272110'], [])),
            patch('app.signals.market_risk_snapshot.asyncio.sleep', return_value=None) as sleep,
            patch('app.signals.market_risk_snapshot.upsert_daily_market_risk_snapshots') as upsert,
        ):
            result = await service.collect_for_market_date(market_date='2026-06-15')

        self.assertEqual(client.calls, ['272110', '272110', '272110', '272110'])
        self.assertEqual(result['snapshots_persisted'], 0)
        self.assertEqual(result['snapshots_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['272110'])
        self.assertIn('KIS request timed out', result['failed_symbol_errors'][0]['error'])
        self.assertEqual(upsert.call_count, 0)
        retry_delays = [call.args[0] for call in sleep.call_args_list if call.args[0] >= 1.0]
        self.assertEqual(retry_delays, [1.0, 2.0, 4.0])

    async def test_collect_for_market_date_honors_kis_retryable_error_contract(self) -> None:
        client = FakeKISRiskClient(
            responses={
                '008775': {
                    'output': {
                        'mrkt_warn_cls_code': '00',
                        'mang_issu_yn': 'N',
                        'invt_caful_yn': 'N',
                        'short_over_yn': 'N',
                        'ssts_hot_yn': 'N',
                        'vi_cls_code': '00',
                        'insn_pbnt_yn': 'N',
                        'trht_yn': 'N',
                    }
                },
                '159800': {
                    'output': {
                        'mrkt_warn_cls_code': '00',
                        'mang_issu_yn': 'N',
                        'invt_caful_yn': 'N',
                        'short_over_yn': 'N',
                        'ssts_hot_yn': 'N',
                        'vi_cls_code': '00',
                        'insn_pbnt_yn': 'N',
                        'trht_yn': 'N',
                    }
                },
            },
            errors={
                '008775': [
                    KISHTTPError(
                        'KIS request failed [500] inquire-price-2',
                        status_code=500,
                        retryable=True,
                    ),
                    None,
                ],
                '159800': [
                    KISHTTPError(
                        'KIS request failed [429] rate limit',
                        status_code=429,
                        retryable=False,
                    ),
                    None,
                ],
            },
        )
        service = DailyMarketRiskSnapshotService(client=client)
        inputs = [
            MarketRiskSymbolInput(symbol='008775', name_ko='호텔신라우', market='KOSPI'),
            MarketRiskSymbolInput(symbol='159800', name_ko='마이티코스피100', market='KOSPI'),
        ]

        with (
            patch.object(
                service,
                '_load_symbol_rows',
                return_value=(inputs, ['008775', '159800'], []),
            ),
            patch('app.signals.market_risk_snapshot.asyncio.sleep', return_value=None) as sleep,
            patch('app.signals.market_risk_snapshot.upsert_daily_market_risk_snapshots') as upsert,
        ):
            result = await service.collect_for_market_date(market_date='2026-09-11')

        self.assertEqual(client.calls.count('008775'), 2)
        self.assertEqual(client.calls.count('159800'), 1)
        self.assertEqual(result['snapshots_persisted'], 1)
        self.assertEqual(result['snapshots_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['159800'])
        upsert.assert_called_once()
        self.assertEqual(upsert.call_args.args[0][0]['symbol'], '008775')
        retry_delays = [call.args[0] for call in sleep.call_args_list if call.args[0] >= 1.0]
        self.assertEqual(retry_delays, [1.0])

    def test_inspect_resolves_latest_on_or_before_requested_market_date(self) -> None:
        service = DailyMarketRiskSnapshotService()
        stored_items = [
            {
                'symbol': '005930',
                'feature_version': 'kis_quote2_snapshot_v1',
                'market_date': '2026-04-14',
                'is_management_item': False,
                'is_trade_halted': True,
            }
        ]

        with (
            patch(
                'app.signals.market_risk_snapshot.load_latest_available_daily_market_risk_snapshot_date',
                return_value='2026-04-14',
            ) as latest_date,
            patch(
                'app.signals.market_risk_snapshot.load_daily_market_risk_snapshots',
                return_value=stored_items,
            ) as loader,
        ):
            result = service.inspect(market_date='2026-04-15', symbol='5930', limit=50)

        latest_date.assert_called_once_with(
            market_date=date(2026, 4, 15),
            symbols=['005930'],
        )
        loader.assert_called_once_with(
            market_date='2026-04-14',
            symbols=['005930'],
            limit=1,
        )
        self.assertEqual(result['requested_market_date'], '2026-04-15')
        self.assertEqual(result['resolved_market_date'], '2026-04-14')
        self.assertTrue(result['is_latest_fallback'])
        self.assertEqual(result['summary']['trade_halted_count'], 1)
        self.assertEqual(result['item'], stored_items[0])


class MarketRiskSnapshotPersistenceIntegrationTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        DailyMarketRiskSnapshot.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.service = DailyMarketRiskSnapshotService(
            client=FakeKISRiskClient(
                responses={
                    '196001': {
                        'output': {
                            'mrkt_warn_cls_code': '01',
                            'mrkt_warn_cls_name': '투자주의',
                            'mang_issu_yn': 'N',
                            'invt_caful_yn': 'Y',
                            'short_over_yn': 'N',
                            'ssts_hot_yn': 'N',
                            'vi_cls_code': '00',
                            'insn_pbnt_yn': 'N',
                            'trht_yn': 'N',
                        }
                    },
                    '196002': {
                        'output': {
                            'mrkt_warn_cls_code': '00',
                            'mrkt_warn_cls_name': '해당없음',
                            'mang_issu_yn': 'Y',
                            'invt_caful_yn': 'N',
                            'short_over_yn': 'N',
                            'ssts_hot_yn': 'N',
                            'vi_cls_code': '00',
                            'insn_pbnt_yn': 'N',
                            'trht_yn': 'Y',
                        }
                    },
                }
            )
        )
        self.market_date = date(2026, 4, 25)
        self.symbols = ['196001', '196002']
        self.run_id = create_eod_collection_run(
            market='KRX',
            trade_date=self.market_date.isoformat(),
            source='pykrx',
            trigger='test_market_risk_snapshot',
            symbols_total=2,
            run_metadata={'mode': 'daily_auto'},
        )

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(session.execute(select(Symbol.id).where(Symbol.symbol.in_(self.symbols))).scalars().all())
            if symbol_ids:
                session.execute(delete(DailyMarketRiskSnapshot).where(DailyMarketRiskSnapshot.symbol_id.in_(symbol_ids)))
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))
            session.execute(delete(EODCollectionRun).where(EODCollectionRun.id == self.run_id))

    async def test_collect_persists_and_inspect_reads_single_symbol(self) -> None:
        inputs = [
            MarketRiskSymbolInput(symbol='196001', name_ko='테스트주의', market='KOSPI'),
            MarketRiskSymbolInput(symbol='196002', name_ko='테스트관리', market='KOSDAQ'),
        ]

        with patch.object(self.service, '_load_symbol_rows', return_value=(inputs, self.symbols, [])):
            result = await self.service.collect_for_market_date(
                market_date=self.market_date,
                run_id=self.run_id,
            )

        inspection = self.service.inspect(market_date=self.market_date, symbol='196002')
        stored_items = load_daily_market_risk_snapshots(
            market_date=self.market_date,
            symbols=['196002'],
        )

        self.assertEqual(result['snapshots_persisted'], 2)
        self.assertEqual(result['summary']['management_item_count'], 1)
        self.assertEqual(result['summary']['trade_halted_count'], 1)
        self.assertEqual(inspection['item']['symbol'], '196002')
        self.assertTrue(inspection['item']['is_management_item'])
        self.assertTrue(inspection['item']['is_trade_halted'])
        self.assertTrue(stored_items)
        self.assertEqual(stored_items[0]['eod_run_id'], self.run_id)
        self.assertEqual(stored_items[0]['market_warning_code'], '00')


if __name__ == '__main__':
    unittest.main()
