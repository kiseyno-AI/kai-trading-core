import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from sqlalchemy import delete, select

from app.brokers.kis.client import KISClient
from app.db import session_scope
from app.db_models import DailyMarketRiskFilterResult, EODCollectionRun, Symbol
from app.market_data_store import load_daily_market_risk_filter_results
from app.repositories.eod_run_repository import create_eod_collection_run
from app.signals.market_risk_filter import (
    DailyMarketRiskFilterService,
    MarketRiskSymbolInput,
    build_market_risk_filter_result,
)


class FakeKISRiskClient:
    def __init__(self, responses: dict[str, dict], errors: dict[str, Exception] | None = None):
        self.responses = responses
        self.errors = errors or {}

    async def fetch_domestic_quote_risk_status(self, symbol: str, market_code: str = 'J') -> dict:
        if symbol in self.errors:
            raise self.errors[symbol]
        return self.responses[symbol]


class MarketRiskFilterNormalizationTests(unittest.TestCase):
    def test_build_market_risk_filter_result_normalizes_blocking_and_watchlist_flags(self) -> None:
        result = build_market_risk_filter_result(
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

        self.assertEqual(result['market_date'], '2026-04-14')
        self.assertFalse(result['passed_filter'])
        self.assertTrue(result['watchlist_flagged'])
        self.assertEqual(result['risk_state'], 'fail')
        self.assertEqual(result['market_warning_code'], '02')
        self.assertTrue(result['is_investment_caution'])
        self.assertTrue(result['is_short_overheated'])
        self.assertTrue(result['is_trade_halted'])
        self.assertEqual(
            result['filter_payload']['blocking_reasons'],
            ['market_warning_02', 'trade_halted'],
        )
        self.assertEqual(
            result['filter_payload']['watchlist_reasons'],
            ['investment_caution', 'short_overheated', 'volatility_interruption'],
        )


class DailyMarketRiskFilterServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_rebuild_for_market_date_continues_on_fetch_and_persist_failures(self) -> None:
        service = DailyMarketRiskFilterService(
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
                'app.signals.market_risk_filter.upsert_daily_market_risk_filter_result',
                side_effect=upsert_side_effect,
            ) as upsert,
        ):
            result = await service.rebuild_for_market_date(market_date='2026-04-14')

        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['results_failed'], 2)
        self.assertEqual(result['summary']['matched_symbol_count'], 3)
        self.assertEqual(result['summary']['evaluated_count'], 2)
        self.assertEqual(result['summary']['passed_count'], 2)
        self.assertEqual(result['summary']['watchlist_count'], 1)
        self.assertEqual(result['failed_symbols'], ['051910', '000660'])
        self.assertEqual(upsert.call_count, 2)
        persisted_symbols = {call.args[0]['symbol'] for call in upsert.call_args_list}
        self.assertEqual(persisted_symbols, {'005930', '000660'})

    async def test_kis_client_fetch_domestic_quote_risk_status_uses_quote2_endpoint(self) -> None:
        client = KISClient()

        with patch.object(client, '_get', new=AsyncMock(return_value={'output': {}})) as get_request:
            await client.fetch_domestic_quote_risk_status('005930', market_code='UN')

        get_request.assert_awaited_once_with(
            '/uapi/domestic-stock/v1/quotations/inquire-price-2',
            tr_id='FHPST01010000',
            params={
                'FID_COND_MRKT_DIV_CODE': 'UN',
                'FID_INPUT_ISCD': '005930',
            },
        )

    def test_inspect_resolves_latest_on_or_before_requested_market_date(self) -> None:
        service = DailyMarketRiskFilterService()
        stored_items = [
            {
                'symbol': '005930',
                'feature_version': 'kis_market_risk_v1',
                'market_date': '2026-04-14',
                'passed_filter': True,
                'watchlist_flagged': True,
            }
        ]

        with (
            patch(
                'app.signals.market_risk_filter.load_latest_available_daily_market_risk_filter_result_date',
                return_value='2026-04-14',
            ) as latest_date,
            patch(
                'app.signals.market_risk_filter.load_daily_market_risk_filter_results',
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
        self.assertEqual(result['summary']['watchlist_count'], 1)
        self.assertEqual(result['item'], stored_items[0])


class MarketRiskFilterPersistenceIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.service = DailyMarketRiskFilterService(
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
                            'trht_yn': 'N',
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
            trigger='test_market_risk_filter',
            symbols_total=2,
            run_metadata={'mode': 'daily_auto'},
        )

    def tearDown(self) -> None:
        with session_scope() as session:
            symbol_ids = list(session.execute(select(Symbol.id).where(Symbol.symbol.in_(self.symbols))).scalars().all())
            if symbol_ids:
                session.execute(delete(DailyMarketRiskFilterResult).where(DailyMarketRiskFilterResult.symbol_id.in_(symbol_ids)))
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))
            session.execute(delete(EODCollectionRun).where(EODCollectionRun.id == self.run_id))

    async def test_rebuild_persists_and_inspect_reads_single_symbol(self) -> None:
        inputs = [
            MarketRiskSymbolInput(symbol='196001', name_ko='테스트주의', market='KOSPI'),
            MarketRiskSymbolInput(symbol='196002', name_ko='테스트관리', market='KOSDAQ'),
        ]

        with patch.object(self.service, '_load_symbol_rows', return_value=(inputs, self.symbols, [])):
            result = await self.service.rebuild_for_market_date(
                market_date=self.market_date,
                run_id=self.run_id,
            )

        inspection = self.service.inspect(market_date=self.market_date, symbol='196001')
        stored_items = load_daily_market_risk_filter_results(
            market_date=self.market_date,
            symbols=['196001'],
        )

        self.assertEqual(result['results_persisted'], 2)
        self.assertEqual(result['summary']['watchlist_count'], 1)
        self.assertEqual(result['summary']['failed_count'], 1)
        self.assertEqual(inspection['item']['symbol'], '196001')
        self.assertEqual(inspection['item']['risk_state'], 'watchlist')
        self.assertTrue(inspection['item']['watchlist_flagged'])
        self.assertTrue(stored_items)
        self.assertEqual(stored_items[0]['eod_run_id'], self.run_id)
        self.assertEqual(stored_items[0]['market_warning_code'], '01')


if __name__ == '__main__':
    unittest.main()
