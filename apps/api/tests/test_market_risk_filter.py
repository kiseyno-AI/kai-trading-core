import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from sqlalchemy import delete, select

from app.db import engine, session_scope
from app.db_models import (
    DailyMarketRiskFilterResult,
    DailyMarketRiskSnapshot,
    EODCollectionRun,
    Symbol,
)
from app.market_data_store import (
    load_daily_market_risk_filter_results,
    load_daily_market_risk_snapshots,
)
from app.repositories.eod_run_repository import create_eod_collection_run
from app.signals.market_risk_evaluators import (
    build_market_risk_filter_result_from_snapshot,
    evaluate_market_risk_snapshot,
    evaluate_market_warning_reasons,
)
from app.signals.market_risk_filter import DailyMarketRiskFilterService
from app.signals.market_risk_snapshot import (
    DailyMarketRiskSnapshotService,
    MarketRiskSymbolInput,
    build_market_risk_snapshot,
)


class FakeKISRiskClient:
    def __init__(self, responses: dict[str, dict], errors: dict[str, Exception] | None = None):
        self.responses = responses
        self.errors = errors or {}

    async def fetch_domestic_quote_risk_status(self, symbol: str, market_code: str = 'J') -> dict:
        if symbol in self.errors:
            raise self.errors[symbol]
        return self.responses[symbol]


class MarketRiskEvaluatorTests(unittest.TestCase):
    def test_unknown_vi_evidence_is_conservatively_watchlisted_and_auditable(self) -> None:
        snapshot = {
            'symbol': '005930',
            'market_date': '2026-04-14',
            'vi_class_code': 'unexpected',
            'is_vi_triggered': False,
            'snapshot_payload': {'vi_evidence_status': 'unknown'},
        }

        result = build_market_risk_filter_result_from_snapshot(snapshot)

        self.assertTrue(result['passed_filter'])
        self.assertTrue(result['watchlist_flagged'])
        self.assertIn('volatility_interruption_evidence_unknown', result['filter_payload']['watchlist_reasons'])
        self.assertEqual(result['filter_payload']['vi_evidence_status'], 'unknown')

    def test_vi_code_reclassifies_legacy_stale_boolean_snapshot(self) -> None:
        result = build_market_risk_filter_result_from_snapshot(
            {
                'symbol': '005930',
                'market_date': '2026-04-14',
                'vi_class_code': 'N',
                'is_vi_triggered': True,
                'snapshot_payload': {},
            }
        )

        self.assertFalse(result['watchlist_flagged'])
        self.assertEqual(result['filter_payload']['vi_evidence_status'], 'inactive')

    def test_evaluate_market_warning_reasons_distinguishes_blocking_and_watchlist_codes(self) -> None:
        blocking, watchlist = evaluate_market_warning_reasons({'market_warning_code': '03'})
        self.assertEqual(blocking, ['market_warning_03'])
        self.assertEqual(watchlist, [])

        blocking, watchlist = evaluate_market_warning_reasons({'market_warning_code': '01'})
        self.assertEqual(blocking, [])
        self.assertEqual(watchlist, ['market_warning_01'])

    def test_build_market_risk_filter_result_from_snapshot_combines_focused_evaluators(self) -> None:
        snapshot = {
            'symbol': '005930',
            'market_date': '2026-04-14',
            'market': 'KOSPI',
            'name_ko': '삼성전자',
            'source': 'kis_quote2_snapshot_v1',
            'feature_version': 'kis_quote2_snapshot_v1',
            'market_code': 'J',
            'source_endpoint': 'inquire-price-2',
            'market_warning_code': '01',
            'market_warning_name': '투자주의',
            'is_management_item': False,
            'is_investment_caution': True,
            'is_short_overheated': True,
            'short_overheat_class_code': '01',
            'is_short_sale_overheated': False,
            'vi_class_code': '01',
            'is_vi_triggered': True,
            'is_dishonest_disclosure': False,
            'is_trade_halted': False,
            'snapshot_payload': {'raw': {'fid': 'value'}},
            'captured_at': '2026-04-14T15:00:00+09:00',
        }

        evaluation = evaluate_market_risk_snapshot(snapshot)
        result = build_market_risk_filter_result_from_snapshot(snapshot)

        self.assertEqual(
            evaluation['watchlist_reasons'],
            ['market_warning_01', 'investment_caution', 'short_overheated', 'volatility_interruption'],
        )
        self.assertTrue(result['passed_filter'])
        self.assertTrue(result['watchlist_flagged'])
        self.assertEqual(result['risk_state'], 'watchlist')
        self.assertEqual(result['filter_payload']['snapshot_source'], 'kis_quote2_snapshot_v1')
        self.assertEqual(result['filter_payload']['snapshot_source_endpoint'], 'inquire-price-2')
        self.assertEqual(result['_snapshot_fields']['market_warning_code'], '01')


class DailyMarketRiskFilterServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_rebuild_for_market_date_uses_snapshot_collection_and_continues_on_filter_persist_failures(self) -> None:
        snapshot_service = DailyMarketRiskSnapshotService()
        service = DailyMarketRiskFilterService(snapshot_service=snapshot_service)
        snapshot_collection = {
            'market_date': '2026-04-14',
            'source': 'kis_quote2_snapshot_v1',
            'feature_version': 'kis_quote2_snapshot_v1',
            'requested_symbols': ['005930', '000660', '051910'],
            'matched_symbols': ['005930', '000660', '051910'],
            'missing_symbols': [],
            'summary': {
                'matched_symbol_count': 3,
                'collected_count': 2,
                'management_item_count': 0,
                'warning_count': 1,
                'trade_halted_count': 0,
            },
            'snapshots_persisted': 2,
            'snapshots_failed': 1,
            'failed_symbols': ['051910'],
            'failed_symbol_errors': [{'symbol': '051910', 'error': 'kis unavailable'}],
            'items': [
                build_market_risk_snapshot(
                    symbol='005930',
                    market_date='2026-04-14',
                    market='KOSPI',
                    name_ko='삼성전자',
                    quote_response={
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
                ),
                build_market_risk_snapshot(
                    symbol='000660',
                    market_date='2026-04-14',
                    market='KOSPI',
                    name_ko='SK하이닉스',
                    quote_response={
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
                ),
            ],
        }

        def upsert_side_effect(item: dict) -> None:
            if item['symbol'] == '000660':
                raise RuntimeError('write failed')

        with (
            patch.object(
                snapshot_service,
                'collect_for_market_date',
                new=AsyncMock(return_value=snapshot_collection),
            ) as collect_snapshots,
            patch(
                'app.signals.market_risk_filter.upsert_daily_market_risk_filter_results',
                side_effect=RuntimeError('bulk write failed'),
            ) as bulk_upsert,
            patch(
                'app.signals.market_risk_filter.upsert_daily_market_risk_filter_result',
                side_effect=upsert_side_effect,
            ) as upsert,
        ):
            result = await service.rebuild_for_market_date(market_date='2026-04-14')

        collect_snapshots.assert_awaited_once_with(
            market_date=date(2026, 4, 14),
            symbols=None,
            market_code='J',
            run_id=None,
            fetch_concurrency=None,
            progress_callback=None,
        )
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['results_failed'], 2)
        self.assertEqual(result['snapshot_collection']['snapshots_persisted'], 2)
        self.assertEqual(result['summary']['matched_symbol_count'], 3)
        self.assertEqual(result['summary']['evaluated_count'], 2)
        self.assertEqual(result['summary']['watchlist_count'], 1)
        self.assertEqual(result['failed_symbols'], ['051910', '000660'])
        bulk_upsert.assert_called_once()
        self.assertEqual(upsert.call_count, 2)
        self.assertEqual([item['symbol'] for item in result['items']], ['005930'])
        self.assertEqual(
            result['items'][0]['filter_payload']['watchlist_reasons'],
            ['market_warning_01', 'investment_caution'],
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
    @classmethod
    def setUpClass(cls) -> None:
        DailyMarketRiskSnapshot.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        snapshot_client = FakeKISRiskClient(
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
        self.snapshot_service = DailyMarketRiskSnapshotService(client=snapshot_client)
        self.filter_service = DailyMarketRiskFilterService(snapshot_service=self.snapshot_service)
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
                session.execute(delete(DailyMarketRiskSnapshot).where(DailyMarketRiskSnapshot.symbol_id.in_(symbol_ids)))
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.symbols)))
            session.execute(delete(EODCollectionRun).where(EODCollectionRun.id == self.run_id))

    async def test_rebuild_persists_snapshot_and_filter_views(self) -> None:
        inputs = [
            MarketRiskSymbolInput(symbol='196001', name_ko='테스트주의', market='KOSPI'),
            MarketRiskSymbolInput(symbol='196002', name_ko='테스트관리', market='KOSDAQ'),
        ]

        with patch.object(self.snapshot_service, '_load_symbol_rows', return_value=(inputs, self.symbols, [])):
            result = await self.filter_service.rebuild_for_market_date(
                market_date=self.market_date.isoformat(),
                run_id=self.run_id,
            )

        snapshot_inspection = self.snapshot_service.inspect(market_date=self.market_date, symbol='196001')
        filter_inspection = self.filter_service.inspect(market_date=self.market_date, symbol='196001')
        stored_snapshots = load_daily_market_risk_snapshots(
            market_date=self.market_date,
            symbols=['196001'],
        )
        stored_filters = load_daily_market_risk_filter_results(
            market_date=self.market_date,
            symbols=['196001'],
        )

        self.assertEqual(result['snapshot_collection']['snapshots_persisted'], 2)
        self.assertEqual(result['results_persisted'], 2)
        self.assertEqual(result['summary']['watchlist_count'], 1)
        self.assertEqual(result['summary']['failed_count'], 1)
        self.assertEqual(snapshot_inspection['item']['market_warning_code'], '01')
        self.assertEqual(filter_inspection['item']['risk_state'], 'watchlist')
        self.assertTrue(filter_inspection['item']['watchlist_flagged'])
        self.assertTrue(stored_snapshots)
        self.assertTrue(stored_filters)
        self.assertEqual(stored_snapshots[0]['eod_run_id'], self.run_id)
        self.assertEqual(stored_filters[0]['eod_run_id'], self.run_id)
        self.assertEqual(
            stored_filters[0]['filter_payload']['watchlist_reasons'],
            ['market_warning_01', 'investment_caution'],
        )
        self.assertNotIn('market_warning_code', stored_filters[0])


if __name__ == '__main__':
    unittest.main()
