import unittest
from unittest.mock import AsyncMock, patch

from app.brokers.kis.client import KISClient
from app.signals.daily_signal_features import (
    OVERTIME_FLUCTUATION,
    OVERTIME_VOLUME,
    VOLUME_POWER,
    DailySignalFeatureService,
    build_daily_signal_feature_rows,
)


class FakeKISRankingClient:
    async def fetch_domestic_ranking_volume_power(self, **kwargs):
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


class DailySignalFeatureBuilderTests(unittest.TestCase):
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


class DailySignalFeatureServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_for_market_date_calls_kis_sources_and_upserts_rows(self):
        service = DailySignalFeatureService(client=FakeKISRankingClient())

        with (
            patch(
                'app.signals.daily_signal_features._expand_rows_with_owned_fallback',
                side_effect=lambda **kwargs: (
                    kwargs['direct_rows'],
                    {
                        'active_symbols': ['005930', '000660'],
                        'candidate_symbols': ['005930'],
                        'candidate_universe_availability': 'available',
                    },
                ),
            ),
            patch('app.signals.daily_signal_features.upsert_daily_signal_feature') as upsert,
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
        self.assertEqual(upsert.call_count, 2)
        persisted_symbols = {call.args[0]['symbol'] for call in upsert.call_args_list}
        self.assertEqual(persisted_symbols, {'005930', '000660'})
        self.assertEqual(result['participation_context'], {'captured_symbol_count': 2})
        self.assertEqual(result['items_total'], 2)
        self.assertEqual(result['items_returned'], 2)
        self.assertEqual(result['coverage']['candidate_universe']['present_rows'], 1)
        persist_participation.assert_called_once_with(
            market_date='2026-04-14',
            market='KRX',
            source='kis_ranking',
        )

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
