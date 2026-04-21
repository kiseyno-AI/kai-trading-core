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
    async def test_collect_and_persist_for_market_date_calls_kis_sources_and_upserts_rows(self):
        service = DailySignalFeatureService(client=FakeKISRankingClient())

        with (
            patch('app.signals.daily_signal_features.upsert_daily_signal_feature') as upsert,
            patch(
                'app.signals.daily_signal_features.daily_market_participation_context_service.compute_and_persist_for_market_date',
                return_value={'captured_symbol_count': 2},
            ) as persist_participation,
        ):
            result = await service.collect_and_persist_for_market_date(market_date='2026-04-14')

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
        persist_participation.assert_called_once_with(
            market_date='2026-04-14',
            market='KRX',
            source='kis_ranking',
        )

    async def test_collect_features_can_scope_to_one_source(self):
        service = DailySignalFeatureService(client=FakeKISRankingClient())

        result = await service.collect_features(market_date='2026-04-14', sources=[VOLUME_POWER])

        self.assertEqual(result.sources, [VOLUME_POWER])
        self.assertEqual(result.source_counts, {VOLUME_POWER: 1})
        self.assertEqual([item['symbol'] for item in result.items], ['005930'])


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
