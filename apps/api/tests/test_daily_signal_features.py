import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.brokers.kis.client import KISClient
from app.eod.exact_signal_feature_repair import (
    _preserved_direct_signal_payload,
)
from app.eod.market_membership import market_membership_sha256
from app.market_data_store import sanitize_daily_signal_feature_payload
from app.signals.daily_signal_features import (
    OVERTIME_FLUCTUATION,
    OVERTIME_VOLUME,
    SIGNAL_SOURCE_RETRY_BASE_SECONDS,
    VOLUME_POWER,
    DailySignalFeatureCollection,
    DailySignalFeatureService,
    _build_quote_snapshot_features,
    _expand_rows_with_owned_fallback,
    _merge_owned_fallback_features,
    build_daily_signal_feature_rows,
    main,
)
from app.signals.overtime_ranking_capture import (
    CAPTURE_SCHEMA_VERSION,
    OvertimeRankingCaptureStore,
)
from app.signals.signal_feature_readiness import (
    classify_signal_feature_readiness,
)


class FakeKISRankingClient:
    async def fetch_domestic_ranking_volume_power(self, **kwargs):
        # Core queries common stock per market; this fixture's row is KOSPI.
        if kwargs.get('input_iscd') != '0001':
            return {'output': [], 'page_count': 1, 'max_pages_reached': False}
        return {
            'output': [
                {
                    'stck_shrn_iscd': '005930',
                    'data_rank': '1',
                    'hts_kor_isnm': '삼성전자',
                    'stck_prpr': '71000',
                    'prdy_ctrt': '2.3',
                    'acml_vol': '1000000',
                    'tday_rltv': '145.7',
                    'seln_cnqn_smtn': '700',
                    'shnu_cnqn_smtn': '1020',
                }
            ],
            'page_count': 1,
            'max_pages_reached': False,
        }

    async def fetch_domestic_ranking_overtime_volume(self, **kwargs):
        return {
            'output1': [{'ovtm_untp_acml_vol': '3000'}],
            'output2': [
                {
                    'stck_shrn_iscd': '005930',
                    'hts_kor_isnm': '삼성전자',
                    'ovtm_untp_prpr': '71500',
                    'ovtm_untp_prdy_ctrt': '0.7',
                    'ovtm_untp_vol': '120000',
                    'ovtm_vrss_acml_vol_rlim': '11.2',
                },
                {
                    'stck_shrn_iscd': '000660',
                    'hts_kor_isnm': 'SK하이닉스',
                    'ovtm_untp_prpr': '190000',
                    'ovtm_untp_prdy_ctrt': '1.2',
                    'ovtm_untp_vol': '90000',
                },
            ],
            'page_count': 1,
            'max_pages_reached': False,
        }

    async def fetch_domestic_ranking_overtime_fluctuation(self, **kwargs):
        return {
            'output1': [{'ovtm_untp_ascn_issu_cnt': '22'}],
            'output2': [
                {
                    'mksc_shrn_iscd': '000660',
                    'hts_kor_isnm': 'SK하이닉스',
                    'ovtm_untp_prpr': '193000',
                    'ovtm_untp_prdy_ctrt': '3.3',
                    'ovtm_untp_vol': '80000',
                }
            ],
            'page_count': 1,
            'max_pages_reached': False,
        }


class EmptyKISRankingClient:
    async def fetch_domestic_ranking_volume_power(self, **kwargs):
        return {'output': [], 'page_count': 0, 'max_pages_reached': False}

    async def fetch_domestic_ranking_overtime_volume(self, **kwargs):
        return {'output1': [], 'output2': [], 'page_count': 0, 'max_pages_reached': False}

    async def fetch_domestic_ranking_overtime_fluctuation(self, **kwargs):
        return {'output1': [], 'output2': [], 'page_count': 0, 'max_pages_reached': False}


class FailingKISRankingClient:
    async def fetch_domestic_ranking_volume_power(self, **kwargs):
        raise RuntimeError('volume power unavailable')

    async def fetch_domestic_ranking_overtime_volume(self, **kwargs):
        return {'output1': [], 'output2': [], 'page_count': 0, 'max_pages_reached': False}

    async def fetch_domestic_ranking_overtime_fluctuation(self, **kwargs):
        return {'output1': [], 'output2': [], 'page_count': 0, 'max_pages_reached': False}


class RetryableRankingError(RuntimeError):
    retryable = True


def _kospi_segment_only(response):
    """Volume-power mock: the given response for KOSPI, empty for KOSDAQ."""

    empty = {'output': [], 'page_count': 1, 'max_pages_reached': False}

    async def fetch(**kwargs):
        return response if kwargs.get('input_iscd') == '0001' else empty

    return AsyncMock(side_effect=fetch)


def _ready_direct_feature_item(
    symbol: str,
    *,
    market_date: str = '2026-04-14',
) -> dict:
    return {
        'symbol': symbol,
        'market_date': market_date,
        'signal_count': 1,
        'feature_payload': {
            'market_date': market_date,
            'sources': {
                VOLUME_POWER: {
                    'rank': 1,
                    'volume_power': 145.7,
                },
            },
            'row_provenance': {
                'row_kind': 'direct_only',
                'direct_signal_source_count': 1,
                'fallback_groups_present': [],
            },
            'freshness': {'market_date': market_date},
            'fallback_features': {},
        },
    }


class DailySignalFeatureBuilderTests(unittest.TestCase):
    def test_quote_snapshot_features_reject_non_same_day_evidence(self):
        rejected_quotes = (
            {'price': 71_000},
            {'price': 71_000, 'trade_date': '2026-05-05'},
            {'price': 71_000, 'trade_date': '2026-05-07'},
            {
                'available': False,
                'price': None,
                'trade_date': '2026-05-06',
            },
            {
                'available': True,
                'price': 0,
                'trade_date': '2026-05-06',
            },
            {
                'price': 71_000,
                'cached_at': '2026-05-05T15:30:00+00:00',
                'captured_at': '2026-05-06T15:30:00+00:00',
            },
        )

        for quote_row in rejected_quotes:
            with self.subTest(quote_row=quote_row):
                payload, freshness = _build_quote_snapshot_features(
                    quote_row=quote_row,
                    market_date='2026-05-06',
                )
                self.assertIsNone(payload)
                self.assertIsNone(freshness)

    def test_quote_snapshot_features_keep_same_day_evidence(self):
        for quote_row in (
            {'price': 71_000, 'trade_date': '2026-05-06'},
            {'price': 71_000, 'cached_at': '2026-05-05T15:30:00+00:00'},
        ):
            with self.subTest(quote_row=quote_row):
                payload, freshness = _build_quote_snapshot_features(
                    quote_row=quote_row,
                    market_date='2026-05-06',
                )
                self.assertEqual(payload['price'], 71_000)
                self.assertIsNotNone(freshness)

    def test_direct_signal_row_does_not_publish_unusable_quote_payload(self):
        unusable_quotes = (
            {'price': 71_000, 'trade_date': '2026-05-05'},
            {
                'available': False,
                'price': None,
                'trade_date': '2026-05-06',
            },
            {
                'available': True,
                'price': 0,
                'trade_date': '2026-05-06',
            },
        )

        for quote_row in unusable_quotes:
            with self.subTest(quote_row=quote_row):
                entry = build_daily_signal_feature_rows(
                    market_date='2026-05-06',
                    source_rows={
                        VOLUME_POWER: [
                            {
                                'stck_shrn_iscd': '005930',
                                'tday_rltv': '145.7',
                            }
                        ]
                    },
                )[0]
                support_maps = {
                    'symbol_map': {},
                    'quote_map': {'005930': quote_row},
                    'price_bars_by_symbol': {},
                    'value_map': {},
                    'unified_value_map': {},
                    'quality_map': {},
                    'momentum_map': {},
                    'advanced_value_map': {},
                    'low_risk_map': {},
                    'beta_map': {},
                    'composite_map': {},
                }

                merged = _merge_owned_fallback_features(
                    entry=entry,
                    market_date='2026-05-06',
                    universe={
                        'active_symbols': ['005930'],
                        'candidate_symbols': [],
                        'target_symbols': ['005930'],
                    },
                    support_maps=support_maps,
                    collection_payload={
                        'collected_at': '2026-05-06T15:31:00+09:00'
                    },
                    source_errors={},
                )

                self.assertEqual(
                    merged['feature_payload']['row_provenance']['row_kind'],
                    'direct_only',
                )
                self.assertNotIn(
                    'quote_snapshot',
                    merged['feature_payload']['fallback_features'],
                )
                self.assertNotIn(
                    'quote_snapshot',
                    merged['feature_payload']['freshness'],
                )

    def test_stored_rebuild_preserves_absent_legacy_collection_keys(self):
        entry = build_daily_signal_feature_rows(
            market_date='2026-05-06',
            source_rows={
                VOLUME_POWER: [
                    {
                        'stck_shrn_iscd': '005930',
                        'tday_rltv': '145.7',
                    }
                ]
            },
        )[0]
        entry['feature_payload']['collection'] = {
            'legacy_marker': 'preserved',
        }
        support_maps = {
            'symbol_map': {},
            'quote_map': {},
            'price_bars_by_symbol': {},
            'value_map': {},
            'unified_value_map': {},
            'quality_map': {},
            'momentum_map': {},
            'advanced_value_map': {},
            'low_risk_map': {},
            'beta_map': {},
            'composite_map': {},
        }

        merged = _merge_owned_fallback_features(
            entry=entry,
            market_date='2026-05-06',
            universe={
                'active_symbols': ['005930'],
                'candidate_symbols': [],
                'target_symbols': ['005930'],
            },
            support_maps=support_maps,
            collection_payload={'legacy_marker': 'preserved'},
            source_errors={},
            preserve_collection_shape=True,
        )

        collection = merged['feature_payload']['collection']
        self.assertEqual(collection['legacy_marker'], 'preserved')
        self.assertNotIn('collected_at', collection)
        self.assertNotIn('source_errors', collection)

    def test_stored_rebuild_keeps_invalid_raw_source_out_of_provenance(self):
        raw_source = {'rank': 1, 'overtime_volume': 0}
        entry = {
            'symbol': '005930',
            'name_ko': '삼성전자',
            'market': 'KOSPI',
            'market_date': '2026-05-06',
            'source': 'kis_ranking',
            'signal_count': 1,
            'volume_power_rank': None,
            'volume_power': None,
            'overtime_volume_rank': 1,
            'overtime_volume': 0,
            'overtime_fluctuation_rank': None,
            'overtime_fluctuation_rate': None,
            'feature_payload': {
                'sources': {OVERTIME_VOLUME: raw_source},
                'collection': {},
            },
        }
        support_maps = {
            'symbol_map': {},
            'quote_map': {},
            'price_bars_by_symbol': {},
            'value_map': {},
            'unified_value_map': {},
            'quality_map': {},
            'momentum_map': {},
            'advanced_value_map': {},
            'low_risk_map': {},
            'beta_map': {},
            'composite_map': {},
        }

        merged = _merge_owned_fallback_features(
            entry=entry,
            market_date='2026-05-06',
            universe={
                'active_symbols': ['005930'],
                'candidate_symbols': [],
                'target_symbols': ['005930'],
            },
            support_maps=support_maps,
            collection_payload={},
            source_errors={},
            preserve_collection_shape=True,
        )

        self.assertEqual(
            merged['feature_payload']['sources'][OVERTIME_VOLUME],
            raw_source,
        )
        provenance = merged['feature_payload']['row_provenance']
        self.assertEqual(provenance['direct_signal_source_count'], 0)
        self.assertEqual(provenance['direct_signal_sources'], [])
        self.assertEqual(provenance['row_kind'], 'fallback_only')
        self.assertEqual(
            sanitize_daily_signal_feature_payload(
                merged['feature_payload']
            )['sources'],
            {},
        )
        readiness = classify_signal_feature_readiness(
            signal_count=merged['signal_count'],
            feature_payload=merged['feature_payload'],
            expected_market_date='2026-05-06',
        )
        self.assertFalse(readiness['semantic_ready'])

    def test_build_daily_signal_feature_rows_merges_source_features_by_symbol(self):
        rows = build_daily_signal_feature_rows(
            market_date='2026-04-14',
            source_rows={
                VOLUME_POWER: [
                    {
                        'stck_shrn_iscd': '005930',
                        'data_rank': '2',
                        'hts_kor_isnm': '삼성전자',
                        'tday_rltv': '123.4',
                    }
                ],
                OVERTIME_VOLUME: [
                    {
                        'stck_shrn_iscd': '005930',
                        'hts_kor_isnm': '삼성전자',
                        'ovtm_untp_vol': '1000',
                    }
                ],
                OVERTIME_FLUCTUATION: [
                    {
                        'mksc_shrn_iscd': '000660',
                        'hts_kor_isnm': 'SK하이닉스',
                        'ovtm_untp_prdy_ctrt': '4.4',
                    }
                ],
            },
        )

        samsung = next(item for item in rows if item['symbol'] == '005930')
        hynix = next(item for item in rows if item['symbol'] == '000660')
        self.assertEqual(samsung['signal_count'], 2)
        self.assertEqual(samsung['volume_power_rank'], 2)
        self.assertEqual(samsung['volume_power'], 123.4)
        self.assertEqual(samsung['overtime_volume_rank'], 1)
        self.assertEqual(samsung['overtime_volume'], 1000)
        self.assertIn(VOLUME_POWER, samsung['feature_payload']['sources'])
        self.assertEqual(hynix['overtime_fluctuation_rate'], 4.4)

    def test_placeholder_metrics_do_not_create_strategy_signals(self):
        rows = build_daily_signal_feature_rows(
            market_date='2026-10-30',
            source_rows={
                VOLUME_POWER: [
                    {
                        'stck_shrn_iscd': '069500',
                        'data_rank': '1',
                        'hts_kor_isnm': 'KODEX 200',
                        'tday_rltv': '999.99',
                    }
                ],
                OVERTIME_VOLUME: [
                    {
                        'stck_shrn_iscd': '005930',
                        'data_rank': '1',
                        'hts_kor_isnm': '삼성전자',
                        'ovtm_untp_vol': '0',
                    }
                ],
            },
        )

        self.assertEqual(rows, [])

    def test_mixed_validity_keeps_only_usable_source_rows(self):
        rows = build_daily_signal_feature_rows(
            market_date='2026-10-30',
            source_rows={
                OVERTIME_VOLUME: [
                    {'stck_shrn_iscd': '005930', 'data_rank': '1', 'ovtm_untp_vol': '0'},
                    {'stck_shrn_iscd': '000660', 'data_rank': '2', 'ovtm_untp_vol': '1200'},
                ]
            },
        )

        self.assertEqual([row['symbol'] for row in rows], ['000660'])
        self.assertEqual(rows[0]['signal_count'], 1)
        self.assertEqual(rows[0]['overtime_volume'], 1200)

    def test_generation_universe_excludes_provider_and_candidate_symbols_without_same_day_bar(self):
        direct_rows = build_daily_signal_feature_rows(
            market_date='2026-10-30',
            source_rows={
                VOLUME_POWER: [
                    {'stck_shrn_iscd': '005930', 'tday_rltv': '145.7'},
                    {'stck_shrn_iscd': '069500', 'tday_rltv': '130.0'},
                    {'stck_shrn_iscd': 'A12345', 'tday_rltv': '120.0'},
                ]
            },
        )

        with (
            patch(
                'app.signals.daily_signal_features.list_symbols_with_daily_bar_on_date',
                return_value=['005930', '000660'],
            ),
            patch(
                'app.signals.daily_signal_features._candidate_universe_metadata',
                return_value={
                    'availability': 'available',
                    'symbols': ['005930', '069500'],
                    'has_piotroski_rows': True,
                    'has_market_risk_rows': True,
                },
            ),
            patch('app.signals.daily_signal_features._load_support_maps', return_value={}),
            patch(
                'app.signals.daily_signal_features._merge_owned_fallback_features',
                side_effect=lambda **kwargs: kwargs['entry'],
            ),
        ):
            items, universe = _expand_rows_with_owned_fallback(
                market_date='2026-10-30',
                direct_rows=direct_rows,
                collection_payload={
                    'source_counts': {VOLUME_POWER: 3},
                    'provider_symbols': ['005930', '069500', 'A12345'],
                },
                source_errors={},
            )

        self.assertEqual(universe['target_symbols'], ['000660', '005930'])
        self.assertEqual(universe['candidate_symbols'], ['005930'])
        self.assertEqual(
            universe['excluded_provider_symbols'],
            ['069500', 'A12345'],
        )
        self.assertEqual(universe['excluded_provider_symbol_count'], 2)
        self.assertEqual([item['symbol'] for item in items], ['005930', '000660'])

    def test_authoritative_universe_keeps_exact_participation_count_without_fallback(self):
        with (
            patch(
                'app.signals.daily_signal_features.list_symbols_with_daily_bar_on_date',
                return_value=['005930'],
            ),
            patch(
                'app.signals.daily_signal_features._candidate_universe_metadata',
                return_value={
                    'availability': 'unavailable',
                    'symbols': [],
                    'has_piotroski_rows': False,
                    'has_market_risk_rows': False,
                },
            ),
            patch('app.signals.daily_signal_features._load_support_maps', return_value={}),
        ):
            items, universe = _expand_rows_with_owned_fallback(
                market_date='2026-10-30',
                direct_rows=[],
                collection_payload={'source_counts': {}, 'provider_symbols': []},
                source_errors={},
            )

        self.assertEqual(universe['target_symbols'], ['005930'])
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['symbol'], '005930')
        self.assertEqual(items[0]['signal_count'], 0)
        self.assertEqual(
            items[0]['feature_payload']['row_provenance']['direct_signal_source_count'],
            0,
        )


class DailySignalFeatureServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Never read real after-hours captures from the checkout's var/ dir.
        self._capture_dir = tempfile.TemporaryDirectory()
        self.capture_store = OvertimeRankingCaptureStore(Path(self._capture_dir.name))
        patcher = patch(
            'app.signals.daily_signal_features.overtime_capture_store',
            self.capture_store,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self._capture_dir.cleanup)

    def _save_capture(self, market_date, sources):
        self.capture_store.save(
            {
                'schema_version': CAPTURE_SCHEMA_VERSION,
                'market_date': market_date,
                'captured_at': f'{market_date}T17:55:00+09:00',
                'sources': sources,
                'errors': {},
            }
        )

    async def test_volume_power_queries_common_stock_per_market_and_reranks_by_strength(self):
        calls = []

        async def fetch(**kwargs):
            calls.append(kwargs)
            rows = {
                '0001': [
                    {'stck_shrn_iscd': '002760', 'data_rank': '1', 'tday_rltv': '364.29'},
                    {'stck_shrn_iscd': '105630', 'data_rank': '2', 'tday_rltv': '254.14'},
                ],
                '1001': [
                    {'stck_shrn_iscd': '050120', 'data_rank': '1', 'tday_rltv': '999.99'},
                    {'stck_shrn_iscd': '140070', 'data_rank': '2', 'tday_rltv': '341.14'},
                ],
            }[kwargs['input_iscd']]
            return {'output': rows, 'page_count': 1, 'max_pages_reached': False}

        client = type('SegmentClient', (), {'fetch_domestic_ranking_volume_power': AsyncMock(side_effect=fetch)})()
        service = DailySignalFeatureService(client=client)

        with patch(
            'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
            side_effect=lambda **kwargs: (kwargs['direct_rows'], {}),
        ):
            result = await service.collect_features(market_date='2026-11-06', sources=[VOLUME_POWER])

        self.assertEqual([c['input_iscd'] for c in calls], ['0001', '1001'])
        for call_kwargs in calls:
            self.assertEqual(call_kwargs['div_cls_code'], '1')
            self.assertEqual(call_kwargs['min_volume'], '100000')
        self.assertEqual(result.raw_source_counts, {VOLUME_POWER: 4})
        self.assertEqual(result.source_counts, {VOLUME_POWER: 3})
        ranks = {item['symbol']: item['volume_power_rank'] for item in result.items}
        # KOSDAQ 341.14 sits between the two KOSPI rows once merged.
        self.assertEqual(ranks, {'002760': 1, '140070': 2, '105630': 3})
        self.assertEqual(
            {item['reason'] for item in result.source_diagnostics[VOLUME_POWER]},
            {'placeholder_volume_power'},
        )

    async def test_volume_power_keeps_the_surviving_market_when_one_segment_fails(self):
        async def fetch(**kwargs):
            if kwargs['input_iscd'] == '1001':
                raise RuntimeError('kosdaq ranking down')
            return {
                'output': [{'stck_shrn_iscd': '002760', 'data_rank': '1', 'tday_rltv': '364.29'}],
                'page_count': 1,
                'max_pages_reached': False,
            }

        client = type('HalfClient', (), {'fetch_domestic_ranking_volume_power': AsyncMock(side_effect=fetch)})()
        service = DailySignalFeatureService(client=client)
        with patch(
            'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
            side_effect=lambda **kwargs: (kwargs['direct_rows'], {}),
        ):
            result = await service.collect_features(market_date='2026-11-06', sources=[VOLUME_POWER])

        self.assertEqual(result.source_errors, {})
        self.assertEqual(result.source_counts, {VOLUME_POWER: 1})
        self.assertEqual(result.source_health[VOLUME_POWER]['status'], 'partial')
        self.assertEqual(result.source_health[VOLUME_POWER]['reason'], 'segment_error')

    async def test_refresh_marks_surviving_segment_rows_degraded_when_other_market_fails(self):
        class PartialMarketClient(FakeKISRankingClient):
            async def fetch_domestic_ranking_volume_power(self, **kwargs):
                if kwargs['input_iscd'] == '1001':
                    raise RuntimeError('kosdaq ranking down')
                return await super().fetch_domestic_ranking_volume_power(**kwargs)

        service = DailySignalFeatureService(client=PartialMarketClient())
        with (
            patch(
                'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
                side_effect=lambda **kwargs: (
                    [_ready_direct_feature_item(item['symbol']) for item in kwargs['direct_rows']],
                    {'target_symbols': ['005930'], 'active_symbols': ['005930']},
                ),
            ),
            patch('app.signals.daily_signal_features.upsert_daily_signal_features') as persist,
            patch(
                'app.signals.daily_signal_features.daily_market_participation_context_service.rebuild_for_market_date',
                return_value={'captured_symbol_count': 1},
            ),
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                sources=[VOLUME_POWER],
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['incomplete_sources'], [VOLUME_POWER])
        self.assertEqual(result['successful_stock_codes'], ['005930'])
        self.assertEqual(result['retry_debt_stock_codes'], [])
        self.assertEqual(result['features_persisted'], 1)
        readiness = persist.call_args.args[0][0]['feature_payload']['readiness']
        self.assertEqual(readiness['quality_status'], 'degraded')
        self.assertIn('source_incomplete', readiness['quality_reasons'])

    async def test_overtime_sources_use_the_same_day_after_hours_capture(self):
        self._save_capture(
            '2026-11-06',
            {
                OVERTIME_VOLUME: {
                    'rows': [{'stck_shrn_iscd': '000660', 'ovtm_untp_vol': '90000', 'ovtm_untp_prdy_ctrt': '1.2'}],
                    'summary_rows': [{'ovtm_untp_exch_vol': '90000'}],
                },
                OVERTIME_FLUCTUATION: {
                    'rows': [{'mksc_shrn_iscd': '005930', 'ovtm_untp_prdy_ctrt': '3.3'}],
                    'summary_rows': [],
                },
            },
        )
        client = type(
            'LiveClient',
            (),
            {
                'fetch_domestic_ranking_overtime_volume': AsyncMock(),
                'fetch_domestic_ranking_overtime_fluctuation': AsyncMock(),
            },
        )()
        service = DailySignalFeatureService(client=client)
        with patch(
            'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
            side_effect=lambda **kwargs: (kwargs['direct_rows'], {}),
        ):
            result = await service.collect_features(
                market_date='2026-11-06',
                sources=[OVERTIME_VOLUME, OVERTIME_FLUCTUATION],
            )

        client.fetch_domestic_ranking_overtime_volume.assert_not_awaited()
        client.fetch_domestic_ranking_overtime_fluctuation.assert_not_awaited()
        self.assertEqual(result.source_counts, {OVERTIME_VOLUME: 1, OVERTIME_FLUCTUATION: 1})
        self.assertEqual(sorted(item['symbol'] for item in result.items), ['000660', '005930'])

    async def test_overtime_falls_back_to_live_when_capture_has_no_usable_rows(self):
        self._save_capture(
            '2026-11-06',
            {OVERTIME_VOLUME: {'rows': [{'stck_shrn_iscd': '000660', 'ovtm_untp_vol': '0'}], 'summary_rows': []}},
        )
        live = AsyncMock(
            return_value={
                'output1': [],
                'output2': [{'stck_shrn_iscd': '005930', 'ovtm_untp_vol': '1200'}],
                'page_count': 1,
                'max_pages_reached': False,
            }
        )
        client = type('LiveClient', (), {'fetch_domestic_ranking_overtime_volume': live})()
        service = DailySignalFeatureService(client=client)
        with patch(
            'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
            side_effect=lambda **kwargs: (kwargs['direct_rows'], {}),
        ):
            result = await service.collect_features(market_date='2026-11-06', sources=[OVERTIME_VOLUME])

        live.assert_awaited_once()
        self.assertEqual(result.source_counts, {OVERTIME_VOLUME: 1})

    async def test_collect_features_rejects_all_rows_without_symbol_or_fluctuation_rate(self):
        client = type(
            'InvalidFluctuationRankingClient',
            (),
            {
                'fetch_domestic_ranking_overtime_fluctuation': AsyncMock(
                    return_value={
                        'output1': [],
                        'output2': [
                            {
                                'mksc_shrn_iscd': '',
                                'ovtm_untp_prdy_ctrt': '1.2',
                            },
                            {
                                'mksc_shrn_iscd': '000660',
                                'ovtm_untp_prdy_ctrt': 'not-a-number',
                            },
                            {
                                'mksc_shrn_iscd': 'ABCDEF',
                                'ovtm_untp_prdy_ctrt': '1.2',
                            },
                            {
                                'mksc_shrn_iscd': '１２３４ＡＢ',
                                'ovtm_untp_prdy_ctrt': '1.2',
                            },
                            {
                                'mksc_shrn_iscd': '005930',
                                'ovtm_untp_prdy_ctrt': 'NaN',
                            },
                            {
                                'mksc_shrn_iscd': '035420',
                                'ovtm_untp_prdy_ctrt': 'Infinity',
                            },
                        ],
                        'page_count': 1,
                        'max_pages_reached': False,
                    }
                ),
            },
        )()
        service = DailySignalFeatureService(client=client)

        with patch(
            'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
            side_effect=lambda **kwargs: (kwargs['direct_rows'], {}),
        ):
            result = await service.collect_features(
                market_date='2026-10-30',
                sources=[OVERTIME_FLUCTUATION],
            )

        self.assertEqual(result.raw_source_counts, {OVERTIME_FLUCTUATION: 6})
        self.assertEqual(result.source_counts, {OVERTIME_FLUCTUATION: 0})
        self.assertEqual(result.items, [])
        self.assertEqual(
            result.source_health[OVERTIME_FLUCTUATION]['status'],
            'unavailable',
        )
        self.assertEqual(
            result.source_health[OVERTIME_FLUCTUATION]['reason'],
            'all_rows_invalid',
        )
        self.assertEqual(
            {item['reason'] for item in result.source_diagnostics[OVERTIME_FLUCTUATION]},
            {'invalid_source_symbol', 'missing_overtime_fluctuation_rate'},
        )

    async def test_collect_features_keeps_zero_fluctuation_among_mixed_validity_rows(self):
        client = type(
            'MixedFluctuationRankingClient',
            (),
            {
                'fetch_domestic_ranking_overtime_fluctuation': AsyncMock(
                    return_value={
                        'output1': [],
                        'output2': [
                            {
                                'mksc_shrn_iscd': '',
                                'ovtm_untp_prdy_ctrt': '1.2',
                            },
                            {
                                'mksc_shrn_iscd': '000660',
                                'ovtm_untp_prdy_ctrt': '',
                            },
                            {
                                'mksc_shrn_iscd': '005930',
                                'ovtm_untp_prdy_ctrt': '0',
                            },
                        ],
                        'page_count': 1,
                        'max_pages_reached': False,
                    }
                ),
            },
        )()
        service = DailySignalFeatureService(client=client)

        with patch(
            'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
            side_effect=lambda **kwargs: (kwargs['direct_rows'], {}),
        ):
            result = await service.collect_features(
                market_date='2026-10-30',
                sources=[OVERTIME_FLUCTUATION],
            )

        self.assertEqual(result.raw_source_counts, {OVERTIME_FLUCTUATION: 3})
        self.assertEqual(result.source_counts, {OVERTIME_FLUCTUATION: 1})
        self.assertEqual([item['symbol'] for item in result.items], ['005930'])
        self.assertEqual(result.items[0]['overtime_fluctuation_rate'], 0.0)
        self.assertEqual(
            result.source_health[OVERTIME_FLUCTUATION]['status'],
            'partial',
        )
        self.assertEqual(
            result.source_health[OVERTIME_FLUCTUATION]['reason'],
            'some_rows_invalid',
        )

    async def test_collect_features_marks_page_capped_source_partial(self):
        client = type(
            'PageCappedRankingClient',
            (),
            {
                'fetch_domestic_ranking_volume_power': _kospi_segment_only(
                    {
                        'output': [
                            {
                                'stck_shrn_iscd': '005930',
                                'tday_rltv': '145.7',
                            },
                        ],
                        'page_count': 1,
                        'max_pages_reached': True,
                    }
                ),
            },
        )()
        service = DailySignalFeatureService(client=client)

        with patch(
            'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
            side_effect=lambda **kwargs: (kwargs['direct_rows'], {}),
        ):
            result = await service.collect_features(
                market_date='2026-10-30',
                sources=[VOLUME_POWER],
            )

        self.assertEqual(result.source_counts, {VOLUME_POWER: 1})
        self.assertEqual(result.source_health[VOLUME_POWER]['status'], 'partial')
        self.assertEqual(
            result.source_health[VOLUME_POWER]['reason'],
            'pagination_truncated',
        )
        self.assertTrue(
            result.source_health[VOLUME_POWER]['pagination_truncated'],
        )

    async def test_collect_features_reports_partial_and_unavailable_source_health(self):
        client = type(
            'ValidityClient',
            (),
            {
                'fetch_domestic_ranking_volume_power': _kospi_segment_only(
                    {
                        'output': [
                            {'stck_shrn_iscd': '069500', 'tday_rltv': '999.99'},
                            {'stck_shrn_iscd': '005930', 'tday_rltv': 'NaN'},
                        ],
                        'page_count': 1,
                        'max_pages_reached': False,
                    }
                ),
                'fetch_domestic_ranking_overtime_volume': AsyncMock(
                    return_value={
                        'output1': [],
                        'output2': [
                            {'stck_shrn_iscd': '005930', 'ovtm_untp_vol': '0'},
                            {'stck_shrn_iscd': '000660', 'ovtm_untp_vol': '1200'},
                        ],
                        'page_count': 1,
                        'max_pages_reached': False,
                    }
                ),
            },
        )()
        service = DailySignalFeatureService(client=client)

        with patch(
            'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
            side_effect=lambda **kwargs: (
                kwargs['direct_rows'],
                {
                    'active_symbols': ['000660'],
                    'candidate_symbols': [],
                    'candidate_universe_availability': 'unavailable',
                },
            ),
        ):
            result = await service.collect_features(
                market_date='2026-10-30',
                sources=[VOLUME_POWER, OVERTIME_VOLUME],
            )

        self.assertEqual(result.raw_source_counts, {VOLUME_POWER: 2, OVERTIME_VOLUME: 2})
        self.assertEqual(result.source_counts, {VOLUME_POWER: 0, OVERTIME_VOLUME: 1})
        self.assertEqual(result.source_health[VOLUME_POWER]['status'], 'unavailable')
        self.assertEqual(result.source_health[VOLUME_POWER]['invalid_count'], 2)
        self.assertEqual(result.source_health[OVERTIME_VOLUME]['status'], 'partial')
        self.assertEqual(result.source_health[OVERTIME_VOLUME]['invalid_count'], 1)
        self.assertEqual([item['symbol'] for item in result.items], ['000660'])
        self.assertEqual(
            {item['reason'] for item in result.source_diagnostics[VOLUME_POWER]},
            {'placeholder_volume_power', 'missing_volume_power'},
        )
        self.assertEqual(
            {item['reason'] for item in result.source_diagnostics[OVERTIME_VOLUME]},
            {'non_positive_overtime_volume'},
        )
    async def test_collect_features_retries_each_retryable_kis_ranking_read_once(self):
        client = type(
            'RetryingKISRankingClient',
            (),
            {
                'fetch_domestic_ranking_volume_power': AsyncMock(
                    side_effect=[
                        # one retry per market segment (KOSPI, then KOSDAQ)
                        RetryableRankingError('temporary volume power failure'),
                        {'output': [], 'page_count': 1, 'max_pages_reached': False},
                        RetryableRankingError('temporary volume power failure'),
                        {'output': [], 'page_count': 1, 'max_pages_reached': False},
                    ]
                ),
                'fetch_domestic_ranking_overtime_volume': AsyncMock(
                    side_effect=[
                        RetryableRankingError('temporary overtime volume failure'),
                        {'output1': [], 'output2': [], 'page_count': 1, 'max_pages_reached': False},
                    ]
                ),
                'fetch_domestic_ranking_overtime_fluctuation': AsyncMock(
                    side_effect=[
                        RetryableRankingError('temporary overtime fluctuation failure'),
                        {'output1': [], 'output2': [], 'page_count': 1, 'max_pages_reached': False},
                    ]
                ),
            },
        )()
        service = DailySignalFeatureService(client=client)

        with (
            patch('app.signals.daily_signal_features.asyncio.sleep', new=AsyncMock()) as sleep,
            patch(
                'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
                return_value=([], {}),
            ),
        ):
            result = await service.collect_features(market_date='2026-05-06')

        self.assertEqual(result.source_errors, {})
        self.assertEqual(client.fetch_domestic_ranking_volume_power.await_count, 4)
        self.assertEqual(client.fetch_domestic_ranking_overtime_volume.await_count, 2)
        self.assertEqual(client.fetch_domestic_ranking_overtime_fluctuation.await_count, 2)
        self.assertEqual(sleep.await_count, 4)

    async def test_collect_features_does_not_retry_nonretryable_kis_ranking_error(self):
        client = type(
            'NonRetryingKISRankingClient',
            (),
            {
                'fetch_domestic_ranking_volume_power': AsyncMock(
                    side_effect=RuntimeError('invalid request')
                ),
            },
        )()
        service = DailySignalFeatureService(client=client)

        with (
            patch('app.signals.daily_signal_features.asyncio.sleep', new=AsyncMock()) as sleep,
            patch(
                'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
                return_value=([], {}),
            ),
        ):
            result = await service.collect_features(
                market_date='2026-05-06',
                sources=[VOLUME_POWER],
            )

        self.assertEqual(result.source_errors, {VOLUME_POWER: 'invalid request'})
        # no retry: one call per market segment
        self.assertEqual(client.fetch_domestic_ranking_volume_power.await_count, 2)
        sleep.assert_not_awaited()

    async def test_collect_features_stops_after_one_retryable_read_retry(self):
        client = type(
            'ExhaustedKISRankingClient',
            (),
            {
                'fetch_domestic_ranking_volume_power': AsyncMock(
                    side_effect=RetryableRankingError('ranking still unavailable')
                ),
            },
        )()
        service = DailySignalFeatureService(client=client)

        with (
            patch('app.signals.daily_signal_features.asyncio.sleep', new=AsyncMock()) as sleep,
            patch(
                'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
                return_value=([], {}),
            ),
        ):
            result = await service.collect_features(
                market_date='2026-05-06',
                sources=[VOLUME_POWER],
            )

        self.assertEqual(
            result.source_errors,
            {VOLUME_POWER: 'ranking still unavailable'},
        )
        # each market segment: first try plus exactly one retry
        self.assertEqual(client.fetch_domestic_ranking_volume_power.await_count, 4)
        self.assertEqual(sleep.await_count, 2)
        sleep.assert_awaited_with(SIGNAL_SOURCE_RETRY_BASE_SECONDS)

    def test_repair_rebuilds_owned_fallback_without_rewriting_direct_evidence(
        self,
    ) -> None:
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        stored_quote = {
            'source': 'kis',
            'market_state': 'CLOSED',
            'price': Decimal('71000.00'),
            'trade_date': '2026-05-06',
            'captured_at': '2026-05-06T15:30:00+09:00',
            'legacy_marker': {'available': None, 'raw_scale': '2'},
        }
        stored_sources = {
            VOLUME_POWER: {'data_rank': '1', 'tday_rltv': '145.7'}
        }
        stored_row = {
            'symbol': '005930',
            'name_ko': '삼성전자',
            'market': 'KOSPI',
            'market_date': '2026-05-06',
            'source': 'kis_ranking',
            'signal_count': 1,
            'volume_power_rank': 1,
            'volume_power': Decimal('145.7000'),
            'overtime_volume_rank': None,
            'overtime_volume': None,
            'overtime_fluctuation_rank': None,
            'overtime_fluctuation_rate': None,
            'feature_payload': {
                'market_date': '2026-05-06',
                'sources': stored_sources,
                'collection': {'source_errors': None},
                'fallback_features': {
                    'price_action': {'close_price': 70_000},
                    'quote_snapshot': stored_quote,
                },
                'legacy_unowned_payload': {'shape': ['must', 'survive']},
            },
        }
        expected_preserved_payload = _preserved_direct_signal_payload(
            SimpleNamespace(**stored_row),
            symbol='005930',
        )
        support_maps = {
            'symbol_map': {
                '005930': {
                    'symbol': '005930',
                    'name_ko': '삼성전자',
                    'market': 'KOSPI',
                }
            },
            # This current quote must never replace historical quote evidence.
            'quote_map': {'005930': {'price': 99_999}},
            'price_bars_by_symbol': {
                '005930': [
                    {
                        'trade_date': '2026-05-06',
                        'open_price': 70_000,
                        'high_price': 72_500,
                        'low_price': 69_500,
                        'close_price': 72_000,
                        'volume': 120_000,
                        'turnover': 8_640_000_000,
                        'source': 'kis_final_bar_repair',
                        'captured_at': '2026-05-07T01:00:00+00:00',
                    },
                    {
                        'trade_date': '2026-05-02',
                        'open_price': 69_000,
                        'high_price': 70_500,
                        'low_price': 68_500,
                        'close_price': 70_000,
                        'volume': 100_000,
                        'turnover': 7_000_000_000,
                        'source': 'pykrx',
                        'captured_at': '2026-05-02T15:30:00+09:00',
                    },
                ]
            },
            'value_map': {},
            'unified_value_map': {},
            'quality_map': {},
            'momentum_map': {},
            'advanced_value_map': {},
            'low_risk_map': {},
            'beta_map': {},
            'composite_map': {},
        }

        with (
            patch(
                'app.signals.daily_signal_features.'
                'load_raw_daily_signal_features_for_repair',
                return_value=[stored_row],
            ),
            patch(
                'app.signals.daily_signal_features.'
                '_resolve_exact_repair_generation_universe',
                return_value={
                    'target_symbols': ['005930'],
                    'active_symbols': ['005930'],
                    'candidate_symbols': ['005930'],
                    'candidate_universe_availability': 'available',
                },
            ),
            patch(
                'app.signals.daily_signal_features._load_support_maps',
                return_value=support_maps,
            ),
            patch(
                'app.signals.daily_signal_features.upsert_daily_signal_features',
                return_value=1,
            ) as upsert,
        ):
            result = service.rebuild_stored_owned_fallback_for_market_date(
                market_date='2026-05-06',
                exact_symbols=['005930'],
            )

        self.assertEqual(result['features_persisted'], 1)
        rebuilt = upsert.call_args.args[0][0]
        self.assertEqual(rebuilt['signal_count'], 1)
        self.assertEqual(rebuilt['volume_power_rank'], 1)
        self.assertEqual(rebuilt['volume_power'], Decimal('145.7000'))
        self.assertEqual(rebuilt['feature_payload']['sources'], stored_sources)
        self.assertEqual(
            rebuilt['feature_payload']['row_provenance'][
                'direct_signal_source_count'
            ],
            1,
        )
        self.assertEqual(
            sanitize_daily_signal_feature_payload(
                rebuilt['feature_payload']
            )['sources'],
            stored_sources,
        )
        readiness = classify_signal_feature_readiness(
            signal_count=rebuilt['signal_count'],
            feature_payload=rebuilt['feature_payload'],
            expected_market_date='2026-05-06',
        )
        self.assertTrue(readiness['semantic_ready'])
        self.assertNotIn(
            'collected_at', rebuilt['feature_payload']['collection']
        )
        self.assertIsNone(
            rebuilt['feature_payload']['collection']['source_errors']
        )
        self.assertEqual(
            rebuilt['feature_payload']['fallback_features']['price_action'][
                'close_price'
            ],
            72_000,
        )
        self.assertEqual(
            rebuilt['feature_payload']['fallback_features']['quote_snapshot'],
            stored_quote,
        )
        self.assertEqual(
            _preserved_direct_signal_payload(
                SimpleNamespace(**rebuilt),
                symbol='005930',
            ),
            expected_preserved_payload,
        )

    def test_repair_preserves_stale_quote_shape_without_making_it_usable(
        self,
    ) -> None:
        stale_quote = {
            'source': 'kis',
            'price': 71_000,
            'trade_date': '2026-05-05',
            'captured_at': '2026-05-05T15:30:00+09:00',
            'legacy_marker': {'available': None},
        }
        merged = _merge_owned_fallback_features(
            entry={
                'symbol': '005930',
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'market_date': '2026-05-06',
                'source': 'kis_ranking',
                'signal_count': 0,
                'volume_power_rank': None,
                'volume_power': None,
                'overtime_volume_rank': None,
                'overtime_volume': None,
                'overtime_fluctuation_rank': None,
                'overtime_fluctuation_rate': None,
                'feature_payload': {
                    'market_date': '2026-05-06',
                    'sources': {},
                    'collection': {},
                },
            },
            market_date='2026-05-06',
            universe={
                'target_symbols': ['005930'],
                'active_symbols': ['005930'],
                'candidate_symbols': [],
                'candidate_universe_availability': 'available',
            },
            support_maps={
                'symbol_map': {},
                'quote_map': {'005930': stale_quote},
                'price_bars_by_symbol': {},
                'value_map': {},
                'unified_value_map': {},
                'quality_map': {},
                'momentum_map': {},
                'advanced_value_map': {},
                'low_risk_map': {},
                'beta_map': {},
                'composite_map': {},
            },
            collection_payload={},
            source_errors={},
            preserve_collection_shape=True,
            preserve_quote_snapshot_shape=True,
        )

        self.assertIsNotNone(merged)
        feature_payload = merged['feature_payload']
        self.assertEqual(
            feature_payload['fallback_features']['quote_snapshot'],
            stale_quote,
        )
        readiness = classify_signal_feature_readiness(
            signal_count=merged['signal_count'],
            feature_payload=feature_payload,
            expected_market_date='2026-05-06',
        )
        self.assertFalse(readiness['semantic_ready'])
        self.assertNotIn(
            'quote_snapshot', readiness['usable_fallback_groups']
        )

    def test_repair_non_exact_scope_uses_bounded_raw_rows(self) -> None:
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        stored_sources = {
            VOLUME_POWER: {
                'data_rank': '1',
                'tday_rltv': '145.7',
            },
            OVERTIME_VOLUME: {
                'data_rank': '2',
                'ovtm_untp_vol': '0',
            },
        }
        stored_row = {
            'symbol': '005930',
            'name_ko': '삼성전자',
            'market': 'KOSPI',
            'market_date': '2026-05-06',
            'source': 'kis_ranking',
            'signal_count': 2,
            'volume_power_rank': 1,
            'volume_power': Decimal('145.7000'),
            'overtime_volume_rank': 2,
            'overtime_volume': 0,
            'overtime_fluctuation_rank': None,
            'overtime_fluctuation_rate': None,
            'feature_payload': {
                'sources': stored_sources,
                'collection': {},
            },
        }
        support_maps = {
            'symbol_map': {
                '005930': {
                    'symbol': '005930',
                    'name_ko': '삼성전자',
                    'market': 'KOSPI',
                }
            },
            'quote_map': {},
            'price_bars_by_symbol': {
                '005930': [
                    {
                        'trade_date': '2026-05-06',
                        'open_price': 70_000,
                        'high_price': 72_000,
                        'low_price': 69_000,
                        'close_price': 71_000,
                        'volume': 100_000,
                        'turnover': 7_100_000_000,
                        'source': 'pykrx',
                        'captured_at': '2026-05-06T15:30:00+09:00',
                    }
                ]
            },
            'value_map': {},
            'unified_value_map': {},
            'quality_map': {},
            'momentum_map': {},
            'advanced_value_map': {},
            'low_risk_map': {},
            'beta_map': {},
            'composite_map': {},
        }

        with (
            patch(
                'app.signals.daily_signal_features.'
                'list_symbols_with_daily_bar_on_date',
                return_value=['005930'],
            ) as load_bar_symbols,
            patch(
                'app.signals.daily_signal_features.'
                'load_raw_daily_signal_features_for_repair',
                return_value=[stored_row],
            ) as load_features,
            patch(
                'app.signals.daily_signal_features.'
                '_resolve_generation_universe',
                return_value={
                    'target_symbols': ['005930'],
                    'active_symbols': ['005930'],
                    'candidate_symbols': [],
                    'candidate_universe_availability': 'available',
                },
            ),
            patch(
                'app.signals.daily_signal_features._load_support_maps',
                return_value=support_maps,
            ),
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_features',
                return_value=1,
            ) as upsert,
        ):
            result = service.rebuild_stored_owned_fallback_for_market_date(
                market_date='2026-05-06',
            )

        load_bar_symbols.assert_called_once_with('2026-05-06')
        load_features.assert_called_once_with(
            market_date='2026-05-06',
            symbols=['005930'],
        )
        rebuilt = upsert.call_args.args[0][0]
        self.assertFalse(upsert.call_args.kwargs['delete_rows_outside_scope'])
        self.assertIsInstance(rebuilt['volume_power'], Decimal)
        self.assertEqual(rebuilt['volume_power'], Decimal('145.7000'))
        self.assertEqual(rebuilt['overtime_volume'], 0)
        self.assertEqual(
            rebuilt['feature_payload']['sources'],
            stored_sources,
        )
        self.assertEqual(
            rebuilt['feature_payload']['row_provenance'][
                'direct_signal_sources'
            ],
            [VOLUME_POWER],
        )
        self.assertFalse(result['exact_scope'])

    def test_repair_exact_scope_creates_missing_fallback_rows(self) -> None:
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        stored_row = {
            'symbol': '005930',
            'name_ko': '삼성전자',
            'market': 'KOSPI',
            'market_date': '2026-05-06',
            'source': 'kis_ranking',
            'signal_count': 1,
            'volume_power_rank': 1,
            'volume_power': 145.7,
            'overtime_volume_rank': None,
            'overtime_volume': None,
            'overtime_fluctuation_rank': None,
            'overtime_fluctuation_rate': None,
            'feature_payload': {
                'sources': {
                    VOLUME_POWER: {
                        'data_rank': '1',
                        'tday_rltv': '145.7',
                    }
                },
                'collection': {
                    'collected_at': '2026-05-06T15:31:00+09:00',
                    'source_errors': {},
                },
            },
        }
        support_maps = {
            'symbol_map': {
                '000660': {
                    'symbol': '000660',
                    'name_ko': 'SK하이닉스',
                    'market': 'KOSPI',
                },
                '005930': {
                    'symbol': '005930',
                    'name_ko': '삼성전자',
                    'market': 'KOSPI',
                },
            },
            'quote_map': {},
            'price_bars_by_symbol': {
                symbol: [
                    {
                        'trade_date': '2026-05-06',
                        'open_price': 100,
                        'high_price': 110,
                        'low_price': 90,
                        'close_price': 105,
                        'volume': 1_000,
                        'turnover': 105_000,
                        'source': 'pykrx',
                        'captured_at': '2026-05-06T15:30:00+09:00',
                    }
                ]
                for symbol in ('000660', '005930')
            },
            'value_map': {},
            'unified_value_map': {},
            'quality_map': {},
            'momentum_map': {},
            'advanced_value_map': {},
            'low_risk_map': {},
            'beta_map': {},
            'composite_map': {},
        }

        with (
            patch(
                'app.signals.daily_signal_features.'
                'load_raw_daily_signal_features_for_repair',
                return_value=[stored_row],
            ) as load_features,
            patch(
                'app.signals.daily_signal_features.'
                'list_symbols_with_daily_bar_on_date',
                return_value=['000660', '005930'],
            ),
            patch(
                'app.signals.daily_signal_features.'
                '_candidate_universe_metadata',
                return_value={
                    'availability': 'available',
                    'symbols': ['000660', '005930'],
                    'has_piotroski_rows': True,
                    'has_market_risk_rows': True,
                },
            ),
            patch(
                'app.signals.daily_signal_features._load_support_maps',
                return_value=support_maps,
            ),
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_features',
                return_value=2,
            ) as upsert,
        ):
            result = service.rebuild_stored_owned_fallback_for_market_date(
                market_date='2026-05-06',
                exact_symbols=['005930', '000660'],
            )

        load_features.assert_called_once_with(
            market_date='2026-05-06',
            symbols=['000660', '005930'],
        )
        rebuilt_by_symbol = {
            row['symbol']: row for row in upsert.call_args.args[0]
        }
        self.assertFalse(
            upsert.call_args.kwargs['delete_rows_outside_scope']
        )
        self.assertEqual(set(rebuilt_by_symbol), {'000660', '005930'})
        self.assertEqual(rebuilt_by_symbol['005930']['signal_count'], 1)
        self.assertEqual(rebuilt_by_symbol['000660']['signal_count'], 0)
        self.assertEqual(
            rebuilt_by_symbol['000660']['feature_payload'][
                'row_provenance'
            ]['row_kind'],
            'fallback_only',
        )
        self.assertEqual(result['features_persisted'], 2)
        self.assertEqual(result['scope_symbol_count'], 2)
        self.assertEqual(
            result['scope_membership_sha256'],
            market_membership_sha256(('000660', '005930')),
        )
        self.assertTrue(result['exact_scope'])

    def test_repair_exact_scope_rejects_missing_daily_bar(self) -> None:
        service = DailySignalFeatureService(client=EmptyKISRankingClient())

        with (
            patch(
                'app.signals.daily_signal_features.'
                'load_raw_daily_signal_features_for_repair',
                return_value=[],
            ),
            patch(
                'app.signals.daily_signal_features.'
                'list_symbols_with_daily_bar_on_date',
                return_value=['005930'],
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'missing daily bars',
            ):
                service.rebuild_stored_owned_fallback_for_market_date(
                    market_date='2026-05-06',
                    exact_symbols=['005930', '000660'],
                )

    async def test_refresh_for_market_date_calls_kis_sources_and_upserts_rows(self):
        service = DailySignalFeatureService(client=FakeKISRankingClient())

        with (
            patch(
                'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
                side_effect=lambda **kwargs: (
                    [
                        _ready_direct_feature_item(item['symbol'])
                        for item in kwargs['direct_rows']
                    ],
                    {
                        'active_symbols': ['005930', '000660'],
                        'candidate_symbols': ['005930'],
                        'candidate_universe_availability': 'available',
                    },
                ),
            ),
            patch('app.signals.daily_signal_features.upsert_daily_signal_feature') as fallback_upsert,
            patch('app.signals.daily_signal_features.upsert_daily_signal_features') as bulk_upsert,
            patch(
                'app.signals.daily_signal_features.daily_market_participation_context_service.rebuild_for_market_date',
                return_value={'captured_symbol_count': 2},
            ) as persist_participation,
        ):
            result = await service.refresh_for_market_date(market_date='2026-04-14')

        self.assertEqual(result['market_date'], '2026-04-14')
        self.assertEqual(result['sources'], [VOLUME_POWER, OVERTIME_VOLUME, OVERTIME_FLUCTUATION])
        self.assertEqual(result['source_counts'][VOLUME_POWER], 1)
        self.assertEqual(result['source_counts'][OVERTIME_VOLUME], 2)
        self.assertEqual(result['source_counts'][OVERTIME_FLUCTUATION], 1)
        self.assertEqual(result['features_persisted'], 2)
        bulk_upsert.assert_called_once()
        self.assertFalse(
            bulk_upsert.call_args.kwargs['delete_rows_outside_scope']
        )
        persisted_symbols = {item['symbol'] for item in bulk_upsert.call_args.args[0]}
        self.assertEqual(persisted_symbols, {'005930', '000660'})
        fallback_upsert.assert_not_called()
        self.assertEqual(result['participation_context'], {'captured_symbol_count': 2})
        self.assertEqual(result['items_total'], 2)
        self.assertEqual(result['items_returned'], 2)
        self.assertEqual(result['coverage']['candidate_universe']['present_rows'], 1)
        persist_participation.assert_called_once_with(
            market_date='2026-04-14',
            market='KRX',
            source='kis_ranking',
            exact_symbols=None,
        )

    async def test_refresh_can_return_full_internal_retry_scope_without_expanding_public_preview(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        symbols = [f'{index:06d}' for index in range(60)]
        collection = DailySignalFeatureCollection(
            market_date='2026-04-14',
            sources=[VOLUME_POWER],
            source_counts={VOLUME_POWER: len(symbols)},
            source_errors={},
            universe={
                'active_symbols': symbols,
                'candidate_symbols': symbols,
                'candidate_universe_availability': 'available',
            },
            items=[_ready_direct_feature_item(symbol) for symbol in symbols],
        )

        with (
            patch.object(service, 'collect_features', AsyncMock(return_value=collection)),
            patch(
                'app.signals.daily_signal_features.upsert_daily_signal_features',
                side_effect=RuntimeError('bulk persistence failed'),
            ),
            patch(
                'app.signals.daily_signal_features.upsert_daily_signal_feature',
                side_effect=RuntimeError('row persistence failed'),
            ),
            patch(
                'app.signals.daily_signal_features.daily_market_participation_context_service.rebuild_for_market_date',
                return_value={'captured_symbol_count': 0},
            ),
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                include_retry_metadata=True,
            )

        self.assertEqual(result['features_failed'], 60)
        self.assertEqual(result['failed_symbols'], symbols[:50])
        self.assertEqual(result['_retry_metadata']['failed_symbols'], symbols)

    async def test_refresh_item_persistence_failure_admits_other_ninety_nine_symbols(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        symbols = [f'{index:06d}' for index in range(1, 101)]
        failed_symbol = symbols[-1]
        collection = DailySignalFeatureCollection(
            market_date='2026-04-14',
            sources=[VOLUME_POWER],
            source_counts={VOLUME_POWER: len(symbols)},
            source_errors={},
            universe={
                'target_symbols': symbols,
                'active_symbols': symbols,
                'candidate_symbols': symbols,
                'candidate_universe_availability': 'available',
            },
            items=[_ready_direct_feature_item(symbol) for symbol in symbols],
            raw_source_counts={VOLUME_POWER: len(symbols)},
            source_health={
                VOLUME_POWER: {
                    'status': 'available',
                    'reason': 'valid_rows_available',
                    'raw_count': len(symbols),
                    'valid_count': len(symbols),
                    'invalid_count': 0,
                },
            },
        )

        def persist_one(item):
            if item['symbol'] == failed_symbol:
                raise RuntimeError('row persistence failed')

        with (
            patch.object(
                service,
                'collect_features',
                AsyncMock(return_value=collection),
            ),
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_features',
                side_effect=RuntimeError('bulk persistence failed'),
            ),
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_feature',
                side_effect=persist_one,
            ),
            patch(
                'app.signals.daily_signal_features.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
                return_value={'captured_symbol_count': 99},
            ),
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                include_retry_metadata=True,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['features_persisted'], 99)
        self.assertEqual(result['features_failed'], 1)
        self.assertEqual(
            result['scope_outcome_manifest_version'],
            'signal_features_scope_outcomes_v1',
        )
        self.assertEqual(result['selected_stock_codes'], symbols)
        self.assertEqual(result['successful_stock_codes'], symbols[:-1])
        self.assertEqual(result['retry_debt_stock_codes'], [failed_symbol])
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            [failed_symbol],
        )

    async def test_refresh_full_official_scope_uses_market_authority_only_at_finalizer(
        self,
    ) -> None:
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        symbols = ['005930', '000660']
        authority = {'schema_version': 'test_market_scope_authority'}
        collection = DailySignalFeatureCollection(
            market_date='2026-04-14',
            sources=[VOLUME_POWER],
            source_counts={VOLUME_POWER: 2},
            source_errors={},
            universe={
                'target_symbols': symbols,
                'active_symbols': symbols,
                'candidate_symbols': symbols,
                'candidate_universe_availability': 'available',
            },
            items=[_ready_direct_feature_item(symbol) for symbol in symbols],
            raw_source_counts={VOLUME_POWER: 2},
            source_health={
                VOLUME_POWER: {
                    'status': 'available',
                    'reason': 'valid_rows_available',
                    'raw_count': 2,
                    'valid_count': 2,
                    'invalid_count': 0,
                },
            },
        )

        with (
            patch.object(
                service,
                'collect_features',
                AsyncMock(return_value=collection),
            ),
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_features',
            ) as bulk_upsert,
            patch(
                'app.signals.daily_signal_features.'
                'replace_daily_signal_features_exact_scope',
                return_value={
                    'successful_symbols': symbols,
                    'failed_symbols': [],
                    'scope_finalization': {'ready': True},
                },
            ) as exact_publish,
            patch(
                'app.signals.daily_signal_features.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
                return_value={'captured_symbol_count': 2},
            ) as participation_rebuild,
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                exact_symbols=symbols,
                authoritative_market_scope=authority,
            )

        self.assertTrue(result['authoritative_market_scope'])
        bulk_upsert.assert_not_called()
        exact_publish.assert_called_once()
        publish_kwargs = exact_publish.call_args.kwargs
        self.assertEqual(publish_kwargs['market_date'], '2026-04-14')
        self.assertEqual(publish_kwargs['exact_symbols'], symbols)
        self.assertEqual(
            publish_kwargs['authoritative_market_scope'], authority
        )
        self.assertEqual(
            [item['symbol'] for item in publish_kwargs['features']],
            symbols,
        )
        self.assertTrue(
            all(
                item['feature_payload']['readiness']['semantic_ready']
                for item in publish_kwargs['features']
            )
        )
        participation_rebuild.assert_called_once()

    async def test_refresh_rejects_market_authority_for_narrowed_admitted_scope(
        self,
    ) -> None:
        service = DailySignalFeatureService(client=EmptyKISRankingClient())

        with self.assertRaisesRegex(
            ValueError,
            'cannot use a narrowed admitted scope',
        ):
            await service.refresh_for_market_date(
                market_date='2026-04-14',
                exact_symbols=['005930', '000660'],
                admitted_symbols=['005930'],
                authoritative_market_scope={
                    'schema_version': 'test_market_scope_authority'
                },
            )

    async def test_refresh_marks_placeholder_only_source_partial_and_retries_full_scope(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        symbols = ['005930', '000660']
        collection = DailySignalFeatureCollection(
            market_date='2026-04-14',
            sources=[VOLUME_POWER],
            source_counts={VOLUME_POWER: 0},
            source_errors={},
            universe={
                'target_symbols': symbols,
                'active_symbols': symbols,
                'candidate_symbols': symbols,
                'candidate_universe_availability': 'available',
            },
            items=[{'symbol': symbol} for symbol in symbols],
            raw_source_counts={VOLUME_POWER: 1},
            source_health={
                VOLUME_POWER: {
                    'status': 'unavailable',
                    'reason': 'all_rows_invalid',
                    'raw_count': 1,
                    'valid_count': 0,
                    'invalid_count': 1,
                },
            },
        )

        with (
            patch.object(
                service,
                'collect_features',
                AsyncMock(return_value=collection),
            ),
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_features',
            ),
            patch(
                'app.signals.daily_signal_features.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
                return_value={'captured_symbol_count': 2},
            ) as participation_rebuild,
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                include_retry_metadata=True,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            result['reason'],
            'signal_feature_sources_incomplete',
        )
        self.assertEqual(result['features_failed'], 0)
        self.assertEqual(result['incomplete_sources'], [VOLUME_POWER])
        self.assertEqual(result['source_completeness_status'], 'partial')
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            symbols,
        )
        self.assertEqual(result['successful_stock_codes'], [])
        self.assertEqual(result['retry_debt_stock_codes'], symbols)
        participation_rebuild.assert_not_called()

    async def test_refresh_non_exact_source_error_preserves_authoritative_rows(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        symbols = ['005930', '000660']
        collection = DailySignalFeatureCollection(
            market_date='2026-04-14',
            sources=[VOLUME_POWER],
            source_counts={VOLUME_POWER: 0},
            source_errors={VOLUME_POWER: 'provider timeout'},
            universe={
                'target_symbols': symbols,
                'active_symbols': symbols,
                'candidate_symbols': symbols,
                'candidate_universe_availability': 'available',
            },
            items=[{'symbol': symbol} for symbol in symbols],
            source_health={},
        )

        with (
            patch.object(
                service,
                'collect_features',
                AsyncMock(return_value=collection),
            ),
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_features',
            ) as bulk_upsert,
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_feature',
            ) as fallback_upsert,
            patch(
                'app.signals.daily_signal_features.'
                'replace_daily_signal_features_exact_scope',
            ) as exact_publish,
            patch(
                'app.signals.daily_signal_features.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
                return_value={'captured_symbol_count': 2},
            ) as participation_rebuild,
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                include_retry_metadata=True,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['features_persisted'], 0)
        self.assertEqual(result['incomplete_sources'], [VOLUME_POWER])
        self.assertEqual(result['successful_stock_codes'], [])
        self.assertEqual(result['retry_debt_stock_codes'], symbols)
        self.assertEqual(result['items'], [])
        bulk_upsert.assert_not_called()
        fallback_upsert.assert_not_called()
        exact_publish.assert_not_called()
        participation_rebuild.assert_not_called()
        self.assertEqual(
            result['participation_context']['status'],
            'skipped',
        )
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            symbols,
        )

    async def test_refresh_exact_empty_publication_clears_participation_context(
        self,
    ) -> None:
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        symbols = ['005930', '000660']
        collection = DailySignalFeatureCollection(
            market_date='2026-04-14',
            sources=[VOLUME_POWER],
            source_counts={VOLUME_POWER: 0},
            source_errors={VOLUME_POWER: 'provider timeout'},
            universe={
                'target_symbols': symbols,
                'active_symbols': symbols,
                'candidate_symbols': symbols,
                'candidate_universe_availability': 'available',
            },
            items=[{'symbol': symbol} for symbol in symbols],
            source_health={},
        )

        with (
            patch.object(
                service,
                'collect_features',
                AsyncMock(return_value=collection),
            ),
            patch(
                'app.signals.daily_signal_features.'
                'replace_daily_signal_features_exact_scope',
                return_value={
                    'successful_symbols': [],
                    'failed_symbols': [],
                    'scope_finalization': {'status': 'completed'},
                },
            ) as exact_publish,
            patch(
                'app.signals.daily_signal_features.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
                return_value={'captured_symbol_count': 0},
            ) as participation_rebuild,
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                exact_symbols=symbols,
                include_retry_metadata=True,
            )

        self.assertEqual(result['features_persisted'], 0)
        self.assertEqual(result['retry_debt_stock_codes'], symbols)
        self.assertEqual(
            exact_publish.call_args.kwargs['features'],
            [],
        )
        participation_rebuild.assert_called_once_with(
            market_date='2026-04-14',
            market='KRX',
            source='kis_ranking',
            exact_symbols=symbols,
        )
        self.assertEqual(
            result['participation_context'],
            {'captured_symbol_count': 0},
        )

    async def test_refresh_pagination_truncation_retries_admitted_or_full_scope(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        exact_symbols = ['005930', '000660']
        cases = (
            ('admitted', ['005930'], ['005930']),
            ('full', None, exact_symbols),
        )

        for label, admitted_symbols, expected_retry_symbols in cases:
            with self.subTest(scope=label):
                collection = DailySignalFeatureCollection(
                    market_date='2026-04-14',
                    sources=[VOLUME_POWER],
                    source_counts={VOLUME_POWER: len(expected_retry_symbols)},
                    source_errors={},
                    universe={
                        'target_symbols': expected_retry_symbols,
                        'active_symbols': expected_retry_symbols,
                        'candidate_symbols': expected_retry_symbols,
                        'candidate_universe_availability': 'available',
                    },
                    items=[
                        {'symbol': symbol}
                        for symbol in expected_retry_symbols
                    ],
                    raw_source_counts={
                        VOLUME_POWER: len(expected_retry_symbols),
                    },
                    source_health={
                        VOLUME_POWER: {
                            'status': 'partial',
                            'reason': 'pagination_truncated',
                            'raw_count': len(expected_retry_symbols),
                            'valid_count': len(expected_retry_symbols),
                            'invalid_count': 0,
                            'pagination_truncated': True,
                        },
                    },
                )

                with (
                    patch.object(
                        service,
                        'collect_features',
                        AsyncMock(return_value=collection),
                    ),
                    patch(
                        'app.signals.daily_signal_features.'
                        'upsert_daily_signal_features',
                    ) as bulk_upsert,
                    patch(
                        'app.signals.daily_signal_features.'
                        'replace_daily_signal_features_exact_scope',
                        return_value={
                            'successful_symbols': [],
                            'failed_symbols': [],
                            'scope_finalization': {
                                'status': 'completed'
                            },
                        },
                    ) as exact_publish,
                    patch(
                        'app.signals.daily_signal_features.'
                        'daily_market_participation_context_service.'
                        'rebuild_for_market_date',
                        return_value={'captured_symbol_count': len(expected_retry_symbols)},
                    ),
                ):
                    result = await service.refresh_for_market_date(
                        market_date='2026-04-14',
                        include_retry_metadata=True,
                        exact_symbols=exact_symbols,
                        admitted_symbols=admitted_symbols,
                    )

                self.assertEqual(result['status'], 'partial')
                self.assertEqual(
                    result['incomplete_sources'],
                    [VOLUME_POWER],
                )
                self.assertEqual(
                    result['_retry_metadata']['failed_symbols'],
                    expected_retry_symbols,
                )
                bulk_upsert.assert_not_called()
                exact_publish.assert_called_once()
                self.assertEqual(
                    exact_publish.call_args.kwargs['exact_symbols'],
                    expected_retry_symbols,
                )
                self.assertEqual(
                    exact_publish.call_args.kwargs['features'],
                    [],
                )

    async def test_refresh_partial_source_publishes_usable_subset_as_degraded(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        symbols = ['005930', '000660']
        usable_item = {
            'symbol': '005930',
            'market_date': '2026-04-14',
            'signal_count': 0,
            'feature_payload': {
                'market_date': '2026-04-14',
                'sources': {},
                'row_provenance': {
                    'row_kind': 'fallback_only',
                    'direct_signal_source_count': 0,
                    'fallback_groups_present': ['price_action'],
                },
                'freshness': {'market_date': '2026-04-14'},
                'fallback_features': {
                    'price_action': {'close_return_1d_pct': 1.2},
                },
            },
        }
        collection = DailySignalFeatureCollection(
            market_date='2026-04-14',
            sources=[VOLUME_POWER],
            source_counts={VOLUME_POWER: 1},
            source_errors={},
            universe={
                'target_symbols': symbols,
                'active_symbols': symbols,
                'candidate_symbols': symbols,
                'candidate_universe_availability': 'available',
            },
            items=[usable_item, {'symbol': '000660'}],
            raw_source_counts={VOLUME_POWER: 1},
            source_health={
                VOLUME_POWER: {
                    'status': 'partial',
                    'reason': 'some_rows_invalid',
                    'raw_count': 2,
                    'valid_count': 1,
                    'invalid_count': 1,
                },
            },
        )

        with (
            patch.object(
                service,
                'collect_features',
                AsyncMock(return_value=collection),
            ),
            patch(
                'app.signals.daily_signal_features.'
                'replace_daily_signal_features_exact_scope',
                return_value={
                    'successful_symbols': ['005930'],
                    'failed_symbols': [],
                    'scope_finalization': {'status': 'completed'},
                },
            ) as exact_publish,
            patch(
                'app.signals.daily_signal_features.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
                return_value={'captured_symbol_count': 1},
            ) as participation_rebuild,
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                include_retry_metadata=True,
                exact_symbols=symbols,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['quality_status'], 'degraded')
        self.assertEqual(result['complete_stock_codes'], [])
        self.assertEqual(result['degraded_stock_codes'], ['005930'])
        self.assertEqual(result['successful_stock_codes'], ['005930'])
        self.assertEqual(result['retry_debt_stock_codes'], ['000660'])
        self.assertEqual(
            result['_retry_metadata']['failed_symbols'],
            ['000660'],
        )
        published_features = exact_publish.call_args.kwargs['features']
        self.assertEqual(len(published_features), 1)
        self.assertEqual(published_features[0]['symbol'], '005930')
        self.assertEqual(
            published_features[0]['feature_payload']['readiness'][
                'quality_status'
            ],
            'degraded',
        )
        self.assertIn(
            'source_incomplete',
            published_features[0]['feature_payload']['readiness'][
                'quality_reasons'
            ],
        )
        self.assertNotIn('readiness', usable_item['feature_payload'])
        self.assertEqual(
            exact_publish.call_args.kwargs['exact_symbols'],
            symbols,
        )
        participation_rebuild.assert_called_once()

    async def test_refresh_complete_source_with_retry_debt_is_degraded(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        symbols = ['005930', '000660']
        collection = DailySignalFeatureCollection(
            market_date='2026-04-14',
            sources=[VOLUME_POWER],
            source_counts={VOLUME_POWER: 1},
            source_errors={},
            universe={
                'target_symbols': symbols,
                'active_symbols': symbols,
                'candidate_symbols': symbols,
                'candidate_universe_availability': 'available',
            },
            items=[
                _ready_direct_feature_item('005930'),
                {'symbol': '000660'},
            ],
            raw_source_counts={VOLUME_POWER: 1},
            source_health={
                VOLUME_POWER: {
                    'status': 'available',
                    'reason': 'valid_rows_available',
                    'raw_count': 1,
                    'valid_count': 1,
                    'invalid_count': 0,
                },
            },
        )

        with (
            patch.object(
                service,
                'collect_features',
                AsyncMock(return_value=collection),
            ),
            patch(
                'app.signals.daily_signal_features.'
                'replace_daily_signal_features_exact_scope',
                return_value={
                    'successful_symbols': ['005930'],
                    'failed_symbols': [],
                    'scope_finalization': {'status': 'completed'},
                },
            ),
            patch(
                'app.signals.daily_signal_features.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
                return_value={'captured_symbol_count': 1},
            ),
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                exact_symbols=symbols,
                include_retry_metadata=True,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['quality_status'], 'degraded')
        self.assertEqual(result['complete_stock_codes'], ['005930'])
        self.assertEqual(result['degraded_stock_codes'], [])
        self.assertEqual(result['retry_debt_stock_codes'], ['000660'])

    async def test_non_exact_refresh_publishes_only_usable_partial_subset(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        symbols = ['005930', '000660']
        usable_item = {
            'symbol': '005930',
            'market_date': '2026-04-14',
            'signal_count': 0,
            'feature_payload': {
                'market_date': '2026-04-14',
                'sources': {},
                'row_provenance': {
                    'row_kind': 'fallback_only',
                    'direct_signal_source_count': 0,
                    'fallback_groups_present': ['price_action'],
                },
                'freshness': {'market_date': '2026-04-14'},
                'fallback_features': {
                    'price_action': {'close_return_1d_pct': 1.2},
                },
            },
        }
        collection = DailySignalFeatureCollection(
            market_date='2026-04-14',
            sources=[VOLUME_POWER],
            source_counts={VOLUME_POWER: 1},
            source_errors={},
            universe={
                'target_symbols': symbols,
                'active_symbols': symbols,
                'candidate_symbols': symbols,
                'candidate_universe_availability': 'available',
            },
            items=[usable_item, {'symbol': '000660'}],
            raw_source_counts={VOLUME_POWER: 1},
            source_health={
                VOLUME_POWER: {
                    'status': 'partial',
                    'reason': 'some_rows_invalid',
                    'raw_count': 2,
                    'valid_count': 1,
                    'invalid_count': 1,
                },
            },
        )

        with (
            patch.object(
                service,
                'collect_features',
                AsyncMock(return_value=collection),
            ),
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_features',
                return_value=1,
            ) as bulk_upsert,
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_feature',
            ) as fallback_upsert,
            patch(
                'app.signals.daily_signal_features.'
                'replace_daily_signal_features_exact_scope',
            ) as exact_publish,
            patch(
                'app.signals.daily_signal_features.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
                return_value={'captured_symbol_count': 1},
            ),
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                include_retry_metadata=True,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['successful_stock_codes'], ['005930'])
        self.assertEqual(result['retry_debt_stock_codes'], ['000660'])
        self.assertEqual(result['degraded_stock_codes'], ['005930'])
        published_items = bulk_upsert.call_args.args[0]
        self.assertEqual([item['symbol'] for item in published_items], ['005930'])
        self.assertEqual(
            published_items[0]['feature_payload']['readiness'][
                'quality_status'
            ],
            'degraded',
        )
        fallback_upsert.assert_not_called()
        exact_publish.assert_not_called()

    async def test_refresh_complete_source_keeps_unusable_row_as_retry_debt(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())
        symbol = '005930'
        collection = DailySignalFeatureCollection(
            market_date='2026-04-14',
            sources=[VOLUME_POWER],
            source_counts={VOLUME_POWER: 1},
            source_errors={},
            universe={
                'target_symbols': [symbol],
                'active_symbols': [symbol],
                'candidate_symbols': [symbol],
                'candidate_universe_availability': 'available',
            },
            items=[{'symbol': symbol}],
            raw_source_counts={VOLUME_POWER: 1},
            source_health={
                VOLUME_POWER: {
                    'status': 'available',
                    'reason': 'valid_rows_available',
                    'raw_count': 1,
                    'valid_count': 1,
                    'invalid_count': 0,
                },
            },
        )

        with (
            patch.object(
                service,
                'collect_features',
                AsyncMock(return_value=collection),
            ),
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_features',
            ) as bulk_upsert,
            patch(
                'app.signals.daily_signal_features.'
                'upsert_daily_signal_feature',
            ) as single_upsert,
            patch(
                'app.signals.daily_signal_features.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
            ) as participation_rebuild,
        ):
            result = await service.refresh_for_market_date(
                market_date='2026-04-14',
                include_retry_metadata=True,
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            result['reason'],
            'signal_feature_evidence_incomplete',
        )
        self.assertEqual(result['successful_stock_codes'], [])
        self.assertEqual(result['retry_debt_stock_codes'], [symbol])
        self.assertEqual(result['semantic_ready_count'], 0)
        self.assertEqual(result['semantic_retry_count'], 1)
        bulk_upsert.assert_not_called()
        single_upsert.assert_not_called()
        participation_rebuild.assert_not_called()

    async def test_collect_features_can_scope_to_one_source(self):
        service = DailySignalFeatureService(client=FakeKISRankingClient())

        with patch(
            'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
            side_effect=lambda **kwargs: (
                kwargs['direct_rows'],
                {
                    'active_symbols': ['005930'],
                    'candidate_symbols': [],
                    'candidate_universe_availability': 'unavailable',
                },
            ),
        ):
            result = await service.collect_features(market_date='2026-04-14', sources=[VOLUME_POWER])

        self.assertEqual(result.sources, [VOLUME_POWER])
        self.assertEqual(result.source_counts, {VOLUME_POWER: 1})
        self.assertEqual([item['symbol'] for item in result.items], ['005930'])
        self.assertEqual(result.universe['active_symbols'], ['005930'])

    async def test_collect_features_builds_fallback_rows_from_owned_data_and_records_provenance(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())

        with (
            patch('app.signals.daily_signal_features.list_symbols_with_daily_bar_on_date', return_value=['005930', '000660']),
            patch('app.signals.daily_signal_features.load_daily_piotroski_filter_results', return_value=[{'symbol': '005930'}]),
            patch('app.signals.daily_signal_features.load_daily_market_risk_filter_results', return_value=[{'symbol': '005930'}]),
            patch('app.signals.daily_signal_features.load_agent_feedback_eligible_symbols', return_value=['005930']),
            patch(
                'app.signals.daily_signal_features.load_symbols',
                return_value=[
                    {'symbol': '005930', 'name_ko': '삼성전자', 'market': 'KOSPI'},
                    {'symbol': '000660', 'name_ko': 'SK하이닉스', 'market': 'KOSPI'},
                ],
            ),
            patch(
                'app.signals.daily_signal_features.load_current_quotes',
                return_value=[
                    {
                        'symbol': '000660',
                        'source': 'kis',
                        'market_state': 'OPEN',
                        'price': 190000,
                        'change': 2000,
                        'change_pct': 1.06,
                        'volume': 550000,
                        'turnover': 103000000000,
                        'bid_price': 189900,
                        'ask_price': 190000,
                        'trade_date': '2026-05-06',
                        'cached_at': '2026-05-06T09:57:00+09:00',
                        'captured_at': '2026-05-06T09:57:00+09:00',
                        'company_name': 'SK하이닉스',
                        'fallback': False,
                        'is_live': True,
                    }
                ],
            ),
            patch(
                'app.signals.daily_signal_features.load_recent_price_bars_by_symbol',
                return_value={
                    '005930': [
                        {
                            'trade_date': '2026-05-06',
                            'open_price': 70000,
                            'high_price': 71500,
                            'low_price': 69800,
                            'close_price': 71000,
                            'volume': 120000,
                            'turnover': 0,
                            'source': 'pykrx',
                            'captured_at': '2026-05-06T15:30:00+09:00',
                        },
                        {
                            'trade_date': '2026-05-02',
                            'open_price': 69000,
                            'high_price': 70100,
                            'low_price': 68800,
                            'close_price': 70000,
                            'volume': 100000,
                            'turnover': 7000000000,
                            'source': 'pykrx',
                            'captured_at': '2026-05-02T15:30:00+09:00',
                        },
                    ],
                    '000660': [
                        {
                            'trade_date': '2026-05-06',
                            'open_price': 185000,
                            'high_price': 191000,
                            'low_price': 184500,
                            'close_price': 190000,
                            'volume': 550000,
                            'turnover': 0,
                            'source': 'pykrx',
                            'captured_at': '2026-05-06T15:30:00+09:00',
                        },
                        {
                            'trade_date': '2026-05-02',
                            'open_price': 182000,
                            'high_price': 184000,
                            'low_price': 180000,
                            'close_price': 183000,
                            'volume': 430000,
                            'turnover': 78690000000,
                            'source': 'pykrx',
                            'captured_at': '2026-05-02T15:30:00+09:00',
                        },
                    ],
                },
            ),
            patch(
                'app.signals.daily_signal_features.load_daily_unified_value_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'source': 'unified_value_v1',
                        'feature_version': 'unified_value_v1',
                        'unified_value_score': 0.83,
                        'unified_value_rank': 12,
                        'unified_value_percentile': 0.97,
                        'unified_value_status': 'scored',
                        'per': 10.1,
                        'pbr': 1.2,
                        'dividend_yield': 2.1,
                        'fcf_yield': 4.0,
                        'ev_to_ebitda': 8.5,
                        'captured_at': '2026-05-06T15:40:00+09:00',
                    }
                ],
            ),
            patch(
                'app.signals.daily_signal_features.load_daily_value_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'source': 'value_factor_v1',
                        'feature_version': 'value_factor_v1',
                        'value_score': 0.79,
                        'value_rank': 18,
                        'value_percentile': 0.94,
                        'value_status': 'scored',
                        'per': 10.1,
                        'pbr': 1.2,
                        'dividend_yield': 2.1,
                        'captured_at': '2026-05-06T15:38:00+09:00',
                    }
                ],
            ),
            patch('app.signals.daily_signal_features.load_daily_quality_factor_results', return_value=[]),
            patch(
                'app.signals.daily_signal_features.load_daily_momentum_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'source': 'daily_price_bars',
                        'feature_version': 'price_momentum_v1',
                        'momentum_percentile': 0.91,
                        'momentum_rank': 15,
                        'momentum_12_1': 0.31,
                        'return_5d': 0.04,
                        'ma_60': 70000.0,
                        'ma_120': 68000.0,
                        'is_momentum_eligible': True,
                        'captured_at': '2026-05-06T15:35:00+09:00',
                    }
                ],
            ),
            patch('app.signals.daily_signal_features.load_daily_advanced_value_factor_results', return_value=[]),
            patch(
                'app.signals.daily_signal_features.load_daily_low_risk_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'source': 'low_risk_v1',
                        'feature_version': 'low_risk_v1',
                        'low_risk_score': 0.72,
                        'low_risk_classification': 'eligible',
                        'is_low_risk_eligible': True,
                        'volatility_60d': 0.21,
                        'volatility_120d': 0.24,
                        'max_drawdown_120d': -0.16,
                        'avg_trading_value_20d': 8420000000,
                        'liquidity_percentile': 0.81,
                        'captured_at': '2026-05-06T15:35:00+09:00',
                    }
                ],
            ),
            patch('app.signals.daily_signal_features.load_daily_beta_factor_results', return_value=[]),
            patch('app.signals.daily_signal_features.load_daily_composite_factor_results', return_value=[]),
        ):
            result = await service.collect_features(market_date='2026-05-06')

        self.assertEqual(result.source_counts[VOLUME_POWER], 0)
        self.assertEqual(result.universe['candidate_symbols'], ['005930'])
        self.assertEqual([item['symbol'] for item in result.items], ['000660', '005930'])
        samsung = next(item for item in result.items if item['symbol'] == '005930')
        self.assertEqual(samsung['signal_count'], 0)
        self.assertEqual(samsung['feature_payload']['row_provenance']['row_kind'], 'fallback_only')
        self.assertTrue(samsung['feature_payload']['row_provenance']['candidate_universe_member'])
        self.assertEqual(samsung['feature_payload']['fallback_features']['liquidity']['turnover_source'], 'estimated')
        self.assertAlmostEqual(
            samsung['feature_payload']['fallback_features']['returns']['close_return_1d_pct'],
            1.428571,
            places=5,
        )
        self.assertEqual(
            samsung['feature_payload']['fallback_features']['factor_scores']['unified_value']['score'],
            0.83,
        )
        self.assertEqual(
            samsung['feature_payload']['fallback_features']['factor_scores']['value']['score'],
            0.79,
        )

    async def test_collect_features_records_source_errors_and_still_builds_fallback_rows(self):
        service = DailySignalFeatureService(client=FailingKISRankingClient())

        with (
            patch('app.signals.daily_signal_features.list_symbols_with_daily_bar_on_date', return_value=['005930']),
            patch('app.signals.daily_signal_features.load_daily_piotroski_filter_results', return_value=[]),
            patch('app.signals.daily_signal_features.load_daily_market_risk_filter_results', return_value=[]),
            patch('app.signals.daily_signal_features.load_agent_feedback_eligible_symbols', return_value=[]),
            patch(
                'app.signals.daily_signal_features.load_symbols',
                return_value=[{'symbol': '005930', 'name_ko': '삼성전자', 'market': 'KOSPI'}],
            ),
            patch('app.signals.daily_signal_features.load_current_quotes', return_value=[]),
            patch(
                'app.signals.daily_signal_features.load_recent_price_bars_by_symbol',
                return_value={
                    '005930': [
                        {
                            'trade_date': '2026-05-06',
                            'open_price': 70000,
                            'high_price': 71000,
                            'low_price': 69900,
                            'close_price': 70500,
                            'volume': 90000,
                            'turnover': 0,
                            'source': 'pykrx',
                            'captured_at': '2026-05-06T15:30:00+09:00',
                        },
                        {
                            'trade_date': '2026-05-02',
                            'open_price': 69500,
                            'high_price': 70000,
                            'low_price': 69000,
                            'close_price': 70000,
                            'volume': 85000,
                            'turnover': 5950000000,
                            'source': 'pykrx',
                            'captured_at': '2026-05-02T15:30:00+09:00',
                        },
                    ]
                },
            ),
            patch('app.signals.daily_signal_features.load_daily_value_factor_results', return_value=[]),
            patch('app.signals.daily_signal_features.load_daily_unified_value_factor_results', return_value=[]),
            patch('app.signals.daily_signal_features.load_daily_quality_factor_results', return_value=[]),
            patch('app.signals.daily_signal_features.load_daily_momentum_factor_results', return_value=[]),
            patch('app.signals.daily_signal_features.load_daily_advanced_value_factor_results', return_value=[]),
            patch('app.signals.daily_signal_features.load_daily_low_risk_factor_results', return_value=[]),
            patch('app.signals.daily_signal_features.load_daily_beta_factor_results', return_value=[]),
            patch('app.signals.daily_signal_features.load_daily_composite_factor_results', return_value=[]),
        ):
            result = await service.collect_features(market_date='2026-05-06')

        self.assertEqual(result.source_errors[VOLUME_POWER], 'volume power unavailable')
        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0]['feature_payload']['row_provenance']['row_kind'], 'fallback_only')

    def test_inspect_reports_market_date_and_candidate_coverage(self):
        service = DailySignalFeatureService(client=EmptyKISRankingClient())

        with (
            patch(
                'app.signals.daily_signal_features.load_daily_signal_features',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-05-06',
                        'source': 'kis_ranking',
                        'signal_count': 0,
                        'volume_power_rank': None,
                        'volume_power': None,
                        'overtime_volume_rank': None,
                        'overtime_volume': None,
                        'overtime_fluctuation_rank': None,
                        'overtime_fluctuation_rate': None,
                        'provenance': {'row_kind': 'fallback_only', 'direct_signal_source_count': 0, 'fallback_groups_present': ['price_action']},
                        'feature_payload': {'row_provenance': {'row_kind': 'fallback_only'}},
                    }
                ],
            ),
            patch(
                'app.signals.daily_signal_features.load_daily_signal_feature_context_rows',
                return_value=[
                    {
                        'symbol': '005930',
                        'signal_count': 0,
                        'provenance': {'row_kind': 'fallback_only'},
                        'feature_payload': {'row_provenance': {'row_kind': 'fallback_only'}},
                    },
                    {
                        'symbol': '000660',
                        'signal_count': 1,
                        'provenance': {'row_kind': 'direct_only'},
                        'feature_payload': {'row_provenance': {'row_kind': 'direct_only'}},
                    },
                ],
            ),
            patch('app.signals.daily_signal_features.list_symbols_with_daily_bar_on_date', return_value=['005930', '000660', '035420']),
            patch('app.signals.daily_signal_features.load_daily_piotroski_filter_results', return_value=[{'symbol': '005930'}]),
            patch('app.signals.daily_signal_features.load_daily_market_risk_filter_results', return_value=[{'symbol': '005930'}]),
            patch('app.signals.daily_signal_features.load_agent_feedback_eligible_symbols', return_value=['005930', '035420']),
        ):
            result = service.inspect(market_date='2026-05-06', symbols=['005930'], limit=5)

        self.assertEqual(result['summary']['entries'], 1)
        self.assertEqual(result['summary']['entries_for_market_date'], 2)
        self.assertEqual(result['summary']['coverage']['direct_signal_source_row_count'], 1)
        self.assertEqual(result['summary']['coverage']['fallback_only_row_count'], 1)
        self.assertEqual(result['summary']['coverage']['direct_signal_source_coverage_ratio'], 0.5)
        self.assertEqual(result['summary']['coverage']['direct_signal_source_coverage_status'], 'partial')
        self.assertEqual(result['summary']['coverage']['active_universe']['present_rows'], 2)
        self.assertEqual(result['summary']['coverage']['active_universe']['missing_rows'], 1)
        self.assertEqual(result['summary']['coverage']['candidate_universe']['present_rows'], 1)
        self.assertEqual(result['summary']['coverage']['candidate_universe']['missing_rows'], 1)
        self.assertEqual(result['summary']['coverage']['candidate_universe']['coverage_status'], 'partial')


class KISRankingClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_paginated_ranking_merges_pages_and_sends_continuation_header(self):
        client = KISClient()
        get_response = AsyncMock(
            side_effect=[
                ({'output': [{'stck_shrn_iscd': '005930'}]}, 'M'),
                ({'output': [{'stck_shrn_iscd': '000660'}]}, ''),
            ]
        )

        with patch.object(client, '_get_response', get_response):
            result = await client._get_paginated_ranking(
                '/uapi/domestic-stock/v1/ranking/volume-power',
                tr_id='FHPST01680000',
                params={},
                output_keys=('output',),
                max_pages=5,
            )

        self.assertEqual(result['output'], [{'stck_shrn_iscd': '005930'}, {'stck_shrn_iscd': '000660'}])
        self.assertEqual(result['page_count'], 2)
        self.assertFalse(result['max_pages_reached'])
        self.assertEqual(get_response.call_args_list[0].kwargs['tr_cont'], '')
        self.assertEqual(get_response.call_args_list[1].kwargs['tr_cont'], 'N')


class DailySignalFeatureCliTests(unittest.TestCase):
    @patch('app.signals.daily_signal_features._run_from_args', new_callable=AsyncMock)
    def test_main_returns_nonzero_when_collection_is_partial(self, run_from_args):
        run_from_args.return_value = {'status': 'partial'}

        exit_code = main(['--market-date', '2026-05-06'])

        self.assertEqual(exit_code, 2)

    @patch('app.signals.daily_signal_features._run_from_args', new_callable=AsyncMock)
    def test_main_returns_zero_when_collection_completed(self, run_from_args):
        run_from_args.return_value = {'status': 'completed'}

        exit_code = main(['--market-date', '2026-05-06'])

        self.assertEqual(exit_code, 0)
