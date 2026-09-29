from contextlib import ExitStack
from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from sqlalchemy.exc import SQLAlchemyError

from app.agent_context.read_service import (
    AgentContextReadService,
    PERSISTED_FACTOR_FAMILIES,
    _build_compact_liquidity_context,
    _build_decider_data_quality_context,
    _build_decider_intraday_quality_context,
    _build_factor_summary_entry,
    _build_intraday_execution_context,
    _build_intraday_execution_freshness_provenance,
    _build_market_regime_context,
    _build_market_open_drive_context,
    _build_pre_decider_enrichment_context,
    _build_pre_decider_quote_orderbook_snapshot,
    _build_price_context,
    _build_quote_context,
    _build_quote_freshness_provenance,
    _build_recent_dart_events_context,
    _build_short_selling_context,
    _build_recent_disclosures_freshness_provenance,
    _build_us_market_session_metadata,
    _historical_external_news_knowledge_cutoff_at,
    _load_latest_symbol_day_block_history,
    _load_persisted_factor_rows_with_retry,
    _resolve_external_news_knowledge_cutoff_at,
    _resolve_symbol_market_session,
    resolve_external_news_knowledge_cutoff_for_symbols,
)
from app.core.settings import settings

EMPTY_LATEST_PRIOR_ROWS = {
    'factor_rows': {family: {} for family in PERSISTED_FACTOR_FAMILIES},
    'signal_features': {},
}


class PersistedFactorReadRetryTests(unittest.TestCase):
    def test_transient_sqlalchemy_error_returns_retry_result(self):
        expected_rows = [{'symbol': '005930'}]
        loader = Mock(
            side_effect=[
                SQLAlchemyError('transient factor read failure'),
                expected_rows,
            ]
        )

        rows, storage_unavailable = _load_persisted_factor_rows_with_retry(
            loader,
            empty_factory=list,
        )

        self.assertIs(rows, expected_rows)
        self.assertFalse(storage_unavailable)
        self.assertEqual(loader.call_count, 2)

    def test_repeated_sqlalchemy_error_fails_closed_after_retry(self):
        loader = Mock(side_effect=SQLAlchemyError('persistent factor read failure'))

        rows, storage_unavailable = _load_persisted_factor_rows_with_retry(
            loader,
            empty_factory=list,
        )

        self.assertEqual(rows, [])
        self.assertTrue(storage_unavailable)
        self.assertEqual(loader.call_count, 2)


class ShortSellingContextQualityTests(unittest.TestCase):
    def test_empty_optional_metric_groups_remain_partial(self) -> None:
        context = _build_short_selling_context(
            {
                'payload': {
                    'short_volume': {
                        'short_volume': 100,
                        'total_volume': 1000,
                        'short_volume_ratio_pct': 10.0,
                        'short_value': None,
                        'total_value': None,
                        'short_value_ratio_pct': None,
                    },
                    'short_balance': {
                        'short_balance_qty': None,
                        'listed_shares': None,
                        'short_balance_amount': None,
                        'market_cap': None,
                        'short_balance_ratio_pct': None,
                    },
                    'missing_fields': ['short_value', 'short_balance'],
                }
            }
        )

        self.assertEqual(context['status'], 'partial')
        self.assertEqual(context['missing_fields'], ['short_value', 'short_balance'])


def _fresh_provenance() -> dict[str, object]:
    return {
        'freshness_status': 'fresh',
        'provenance_status': 'provider_sourced',
        'as_of_market_date': '2026-04-29',
        'latest_available_market_date': '2026-04-29',
    }


def _stale_provenance() -> dict[str, object]:
    return {
        **_fresh_provenance(),
        'freshness_status': 'stale',
        'reason': 'latest_available_market_date_before_request',
    }


def _market_breadth_context(*, up: int = 650, down: int = 280, flat: int = 70) -> dict[str, object]:
    return {
        'status': 'available',
        'market_symbol_count': up + down + flat,
        'up_count': up,
        'down_count': down,
        'flat_count': flat,
        'advance_decline_ratio': round(up / down, 4) if down else None,
        'missing_fields': [],
    }


def _sector_context(*, sector_avg: float = 0.4) -> dict[str, object]:
    return {
        'status': 'available',
        'market': 'KOSPI',
        'sector_name': 'Semiconductors',
        'same_sector_symbol_count': 3,
        'peer_count': 2,
        'self': {'symbol': '005930', 'change_pct': sector_avg},
        'peers': [
            {'symbol': '000660', 'change_pct': sector_avg},
            {'symbol': '123456', 'change_pct': sector_avg},
        ],
        'missing_fields': [],
    }


def _relative_context(*, return_percentile: float = 0.72, volume_percentile: float = 0.68) -> dict[str, object]:
    return {
        'status': 'available',
        'market_date': '2026-04-29',
        'market': 'KOSPI',
        'breadth': {'market_symbol_count': 1000, 'up_count': 650, 'down_count': 280},
        'relative_strength': {
            'return_percentile': return_percentile,
            'volume_percentile': volume_percentile,
        },
        'missing_fields': [],
    }


def _macro_context_fx(*, value: float = 1320.0, direction: str = 'stable') -> dict[str, object]:
    return {
        'status': 'available',
        'series': [
            {
                'source': 'fred',
                'series_id': 'DEXKOUS',
                'series_name': 'USD/KRW exchange rate from FRED',
                'status': 'available',
                'latest_value': value,
                'direction': direction,
                'unit': 'KRW per USD',
                'freshness': {'freshness_status': 'fresh'},
            }
        ],
        'macro_regime_summary': {
            'fx_label': 'usdkrw_elevated' if value >= 1400 else 'usdkrw_normal',
        },
    }


def _open_drive_context(
    *,
    gap_up_pct: float = 0.65,
    index_change_pct: float = 1.05,
    early_continuation_pct: float = 0.35,
    trading_value_expansion_ratio: float = 1.8,
    gap_fade_detected: bool = False,
    early_continuation_confirmed: bool | None = None,
    weak_continuation_detected: bool | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        'status': 'available',
        'authoritative': True,
        'market': 'KOSPI',
        'market_date': '2026-07-23',
        'phase': 'opening',
        'source': 'kis_market_snapshot_v1',
        'source_table': 'current_market_snapshot_cache',
        'gap_up_pct': gap_up_pct,
        'index_change_pct': index_change_pct,
        'early_continuation_pct': early_continuation_pct,
        'trading_value_expansion_ratio': trading_value_expansion_ratio,
        'gap_fade_detected': gap_fade_detected,
        'provenance': {
            'validation_status': 'validated',
            'snapshot_source': 'kis_market_snapshot_v1',
            'baseline_source': 'pykrx',
            'elapsed_fraction_adjusted': True,
        },
    }
    if early_continuation_confirmed is not None:
        payload['early_continuation_confirmed'] = early_continuation_confirmed
    if weak_continuation_detected is not None:
        payload['weak_continuation_detected'] = weak_continuation_detected
    return payload


def _stored_market_snapshot_context(
    *,
    as_of: str,
    market_key: str = 'kospi',
    source: str = 'kis_market_snapshot_v1',
) -> dict[str, object]:
    observed_at = datetime.fromisoformat(as_of)
    provider_observed_at = observed_at.isoformat()
    return {
        'source': source,
        'source_table': 'current_market_snapshot_cache',
        'snapshot_status': 'ready',
        'last_refresh_status': 'success',
        'as_of': as_of,
        'last_refresh_succeeded_at': as_of,
        'payload': {
            'status': 'ready',
            'as_of': as_of,
            'legs': {
                'index': {'status': 'ready'},
                'flow': {'status': 'ready'},
                'fx': {'status': 'ready'},
            },
            'markets': {
                market_key: {
                    'label': 'KOSPI' if market_key == 'kospi' else 'KOSDAQ',
                    'index_code': '0001' if market_key == 'kospi' else '1001',
                    'value': 2700.0,
                    'change': 27.0,
                    'change_percent': 1.01,
                    'open': 2685.0,
                    'trading_value': 3_000_000,
                    'trading_value_unit': 'million_krw',
                    'upper_limit_count': 0,
                    'advancers': 700,
                    'unchanged': 50,
                    'decliners': 200,
                    'lower_limit_count': 0,
                    'breadth_raw_buckets': {
                        'upper_limit_count': 0,
                        'advancers': 700,
                        'unchanged': 50,
                        'decliners': 200,
                        'lower_limit_count': 0,
                    },
                    'breadth_completeness': {
                        'status': 'verified',
                        'expected_symbol_count': 970,
                        'observed_symbol_count': 950,
                        'limit_bucket_semantics': 'included_in_direction_counts',
                        'verification_basis': (
                            'kis_directional_buckets_with_same_date_completed_master_upper_bound'
                        ),
                        'master_count_relation': 'upper_bound',
                        'coverage_ratio': round(950 / 970, 6),
                        'expected_universe': {
                            'provider': 'kis_master',
                            'market_date': observed_at.date().isoformat(),
                            'completed_at': observed_at.replace(
                                hour=8,
                                minute=0,
                                second=0,
                            ).isoformat(),
                            'symbol_count': 970,
                            'relation_to_observed_count': 'upper_bound',
                            'membership_sha256': 'a' * 64,
                        },
                    },
                    'provider_business_date': observed_at.date().isoformat(),
                    'provider_session_time': observed_at.strftime('%H:%M:%S'),
                    'provider_observed_at': provider_observed_at,
                    'index_leg': {
                        'status': 'ready',
                        'observation_source': 'kis_index_intraday_bar',
                        'provider_observed_at': provider_observed_at,
                    },
                }
            },
        },
    }


def _prior_market_turnover_baseline(*, market: str = 'KOSPI') -> dict[str, object]:
    return {
        'market': market,
        'trade_date': '2026-04-28',
        'turnover_krw': 8_000_000_000_000,
        'observed_symbol_count': 950,
        'expected_symbol_count': 950,
        'coverage_ratio': 1.0,
        'source': 'pykrx',
        'source_table': 'daily_price_bars',
        'basis': 'prior_complete_market_session_positive_provider_turnover_sum',
        'eod_run_id': 73,
        'eod_run_status': 'completed',
        'eod_run_completed_at': '2026-04-28T16:00:00+09:00',
        'coverage_proof': {
            'status': 'verified',
            'membership_status': 'exact',
            'requested_symbol_count': 950,
            'persisted_symbol_count': 950,
            'market_symbol_count': 950,
            'universe_complete': True,
            'universe_source': 'pykrx',
            'membership_basis': 'end_date_snapshot',
            'membership_trade_date': '2026-04-28',
            'requested_membership_sha256': 'a' * 64,
            'persisted_membership_sha256': 'a' * 64,
        },
    }


def _closed_market_session() -> dict[str, object]:
    return {
        'status': 'weekend',
        'current_market_state': 'WEEKEND',
        'current_calendar_date': '2026-04-11',
        'latest_valid_trading_date': '2026-04-10',
        'recommendation_basis': 'latest_completed_eod',
        'quote_data_expected': False,
        'intraday_data_expected': False,
    }


def _open_market_session(*, market_date: str = '2026-04-08') -> dict[str, object]:
    return {
        'requested_date': market_date,
        'status': 'OPEN',
        'current_market_state': 'OPEN',
        'current_calendar_date': market_date,
        'latest_valid_trading_date': market_date,
        'recommendation_basis': 'live_intraday',
        'quote_data_expected': True,
        'intraday_data_expected': True,
    }


def _make_intraday_execution_row(
    *,
    symbol: str = '005930',
    availability_status: str = 'partial',
    as_of_market_date: str = '2026-04-08',
    as_of_timestamp: str = '2026-04-08T09:57:00+09:00',
    captured_at: str = '2026-04-08T09:57:05+09:00',
    market_state: str = 'OPEN',
) -> dict[str, object]:
    block_statuses = {
        'quote': 'available',
        'orderbook': 'available',
        'liquidity': 'available',
        'investor_program_activity': availability_status,
        'news_hints': 'available',
    }
    return {
        'symbol': symbol,
        'market_code': 'J',
        'source': 'kis_intraday_snapshot_v1',
        'feature_version': 'kis_intraday_snapshot_v1',
        'availability_status': availability_status,
        'freshness_status': 'fresh',
        'market_state': market_state,
        'as_of_market_date': as_of_market_date,
        'as_of_timestamp': as_of_timestamp,
        'captured_at': captured_at,
        'coverage': {
            'status': availability_status,
            'available_blocks': 4 if availability_status == 'partial' else 5,
            'partial_blocks': 1 if availability_status == 'partial' else 0,
            'total_blocks': 5,
            'block_statuses': block_statuses,
        },
        'availability': {
            'quote': True,
            'orderbook': True,
            'liquidity': True,
            'investor_program_activity': availability_status != 'unavailable',
            'news_hints': True,
        },
        'freshness': {
            'market_date': as_of_market_date,
            'status': 'fresh',
        },
        'quote': {
            'price': 71550,
            'spread_points': 100,
            'change_pct': 0.7,
        },
        'orderbook': {
            'imbalance': 0.076923,
            'ask_total_size': 14000,
            'bid_total_size': 16300,
        },
        'liquidity': {
            'volume_power': 155.2,
            'volume_power_rank': 3,
            'volume_rank': 9,
            'quote_balance_rank': 5,
        },
        'investor_program_activity': {
            'status': availability_status,
            'program_net_quantity': 12000,
            'foreign_net_quantity': 3100,
            'institution_net_quantity': -2800,
        },
        'news_hints': {
            'status': 'available',
            'item_count': 1,
            'event_tags': ['contract', 'earnings'],
            'items': [
                {
                    'title': '삼성전자 반도체 공급 계약 체결',
                    'published_at': '2026-04-08T08:40:00+09:00',
                    'event_tags': ['contract'],
                }
            ],
        },
    }


class MarketRegimeContextTests(unittest.TestCase):
    def test_open_drive_prefers_same_provider_prior_turnover_and_provider_observation(self) -> None:
        snapshot = _stored_market_snapshot_context(
            as_of='2026-04-29T09:10:20+09:00'
        )
        market_row = snapshot['payload']['markets']['kospi']
        market_row.update(
            {
                'provider_business_date': '2026-04-29',
                'provider_session_time': '09:10:00',
                'provider_observed_at': '2026-04-29T09:10:00+09:00',
                'prior_trading_value': 8_000_000,
                'prior_trading_value_unit': 'million_krw',
                'upper_limit_count': 2,
                'lower_limit_count': 1,
                'breadth_raw_buckets': {
                    'upper_limit_count': 2,
                    'advancers': 700,
                    'unchanged': 50,
                    'decliners': 200,
                    'lower_limit_count': 1,
                },
                'breadth_completeness': {
                    'status': 'verified',
                    'expected_symbol_count': 970,
                    'observed_symbol_count': 950,
                    'limit_bucket_semantics': 'included_in_direction_counts',
                    'verification_basis': (
                        'kis_directional_buckets_with_same_date_completed_master_upper_bound'
                    ),
                    'master_count_relation': 'upper_bound',
                    'coverage_ratio': round(950 / 970, 6),
                    'expected_universe': {
                        'provider': 'kis_master',
                        'market_date': '2026-04-29',
                        'completed_at': '2026-04-29T08:00:00+09:00',
                        'symbol_count': 970,
                        'relation_to_observed_count': 'upper_bound',
                        'membership_sha256': 'a' * 64,
                    },
                },
                'index_leg': {
                    'status': 'ready',
                    'observation_source': 'kis_index_intraday_bar',
                    'provider_observed_at': '2026-04-29T09:10:00+09:00',
                },
            }
        )
        snapshot['payload']['legs'] = {
            'index': {'status': 'ready'},
            'flow': {'status': 'unavailable'},
            'fx': {'status': 'missing'},
        }

        result = _build_market_open_drive_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
            snapshot_context=snapshot,
            prior_market_turnover_baseline=None,
        )

        self.assertEqual(result['status'], 'available')
        self.assertEqual(result['observed_at'], '2026-04-29T09:10:00+09:00')
        self.assertAlmostEqual(result['trading_value_expansion_ratio'], 1.5, places=4)
        self.assertEqual(result['market_breadth_context']['up_count'], 700)
        self.assertEqual(result['market_breadth_context']['down_count'], 200)
        self.assertEqual(result['provenance']['baseline_source'], 'kis_market_snapshot_v1')
        self.assertEqual(result['provenance']['baseline_provider_field'], 'prdy_tr_pbmn')
        self.assertEqual(result['provenance']['baseline_relative_session'], 'previous')
        regime = _build_market_regime_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            market_breadth_context=result['market_breadth_context'],
            market_breadth_provenance=result['market_breadth_provenance'],
            sector_peer_context=_sector_context(sector_avg=1.4),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(return_percentile=0.84, volume_percentile=0.78),
            market_relative_provenance=_fresh_provenance(),
            macro_context=_macro_context_fx(value=1320.0),
            open_drive_context=result,
        )
        self.assertEqual(regime['market_regime'], 'aggressive_open')

    def test_unverified_or_partial_five_bucket_breadth_cannot_confirm_strong_breadth(self) -> None:
        for case_name in ('unverified_semantics', 'partial_response_with_limit_down'):
            with self.subTest(case=case_name):
                snapshot = _stored_market_snapshot_context(
                    as_of='2026-04-29T09:10:00+09:00'
                )
                market_row = snapshot['payload']['markets']['kospi']
                market_row['prior_trading_value'] = 8_000_000
                market_row['prior_trading_value_unit'] = 'million_krw'
                market_row['upper_limit_count'] = 4
                market_row['lower_limit_count'] = 25
                market_row['breadth_raw_buckets'] = {
                    'upper_limit_count': 4,
                    'advancers': 700,
                    'unchanged': 50,
                    'decliners': 200,
                    'lower_limit_count': 25,
                }
                if case_name == 'unverified_semantics':
                    market_row['breadth_completeness'] = {
                        'status': 'unverified',
                        'expected_symbol_count': None,
                        'observed_symbol_count': 950,
                        'limit_bucket_semantics': 'unknown',
                    }
                else:
                    market_row['breadth_completeness'] = {
                        'status': 'verified',
                        'expected_symbol_count': 951,
                        'observed_symbol_count': 950,
                        'limit_bucket_semantics': 'included_in_direction_counts',
                    }

                open_drive = _build_market_open_drive_context(
                    requested_market_date=date(2026, 4, 29),
                    market='KOSPI',
                    decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
                    snapshot_context=snapshot,
                    prior_market_turnover_baseline=None,
                )
                regime = _build_market_regime_context(
                    requested_market_date=date(2026, 4, 29),
                    market='KOSPI',
                    market_breadth_context=open_drive['market_breadth_context'],
                    market_breadth_provenance=open_drive['market_breadth_provenance'],
                    sector_peer_context=_sector_context(sector_avg=1.4),
                    sector_peer_provenance=_fresh_provenance(),
                    market_relative_context=_relative_context(return_percentile=0.84, volume_percentile=0.78),
                    market_relative_provenance=_fresh_provenance(),
                    macro_context=_macro_context_fx(value=1320.0),
                    open_drive_context=open_drive,
                )

                self.assertEqual(open_drive['status'], 'available')
                self.assertEqual(open_drive['market_breadth_context']['status'], 'unavailable')
                self.assertEqual(regime['market_regime'], 'normal')
                self.assertLessEqual(regime['quantity_multiplier'], 1.0)

    def test_build_market_open_drive_context_derives_conservative_provider_evidence(self) -> None:
        result = _build_market_open_drive_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
            snapshot_context=_stored_market_snapshot_context(
                as_of='2026-04-29T09:10:00+09:00'
            ),
            prior_market_turnover_baseline=_prior_market_turnover_baseline(),
        )

        self.assertEqual(result['status'], 'available')
        self.assertTrue(result['authoritative'])
        self.assertEqual(result['phase'], 'opening')
        self.assertAlmostEqual(result['gap_up_pct'], 0.4489, places=4)
        self.assertAlmostEqual(result['index_change_pct'], 1.01, places=4)
        self.assertAlmostEqual(result['early_continuation_pct'], 0.5587, places=4)
        self.assertAlmostEqual(result['trading_value_expansion_ratio'], 1.5, places=4)
        self.assertEqual(result['market_breadth_context']['up_count'], 700)
        self.assertEqual(result['market_breadth_context']['down_count'], 200)
        self.assertEqual(result['market_breadth_context']['flat_count'], 50)
        self.assertEqual(
            result['market_breadth_provenance']['source_provider'],
            'kis_market_snapshot_v1',
        )
        calculation = result['provenance']['calculation']
        self.assertTrue(calculation['elapsed_fraction_adjusted'])
        self.assertEqual(calculation['elapsed_fraction_denominator'], 0.25)
        self.assertEqual(
            calculation['trading_value_expansion_method'],
            'current_cumulative_krw_divided_by_prior_full_session_krw_times_conservative_elapsed_fraction',
        )
        self.assertFalse(calculation['full_session_linear_extrapolation_used'])

    def test_only_official_included_limit_bucket_contract_is_accepted(self) -> None:
        snapshot = _stored_market_snapshot_context(
            as_of='2026-04-29T09:10:00+09:00'
        )
        market_row = snapshot['payload']['markets']['kospi']
        market_row.update(
            {
                'upper_limit_count': 100,
                'advancers': 150,
                'unchanged': 100,
                'decliners': 300,
                'lower_limit_count': 350,
                'breadth_raw_buckets': {
                    'upper_limit_count': 100,
                    'advancers': 150,
                    'unchanged': 100,
                    'decliners': 300,
                    'lower_limit_count': 350,
                },
                'breadth_completeness': {
                    'status': 'verified',
                    'expected_symbol_count': 1000,
                    'observed_symbol_count': 1000,
                    'limit_bucket_semantics': 'disjoint_from_direction_counts',
                    'verification_basis': (
                        'kis_directional_buckets_with_same_date_completed_master_upper_bound'
                    ),
                    'master_count_relation': 'upper_bound',
                    'coverage_ratio': 1.0,
                },
            }
        )

        open_drive = _build_market_open_drive_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
            snapshot_context=snapshot,
            prior_market_turnover_baseline=None,
        )
        breadth = open_drive['market_breadth_context']

        self.assertEqual(breadth['status'], 'unavailable')
        self.assertEqual(breadth['reason'], 'breadth_completeness_unverified')

    def test_master_upper_bound_larger_than_provider_breadth_uses_observed_count(self) -> None:
        snapshot = _stored_market_snapshot_context(
            as_of='2026-04-29T09:10:00+09:00'
        )
        snapshot['payload']['markets']['kospi']['breadth_completeness'][
            'expected_universe'
        ]['completed_at'] = '2026-04-29T09:10:20+09:00'

        open_drive = _build_market_open_drive_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
            snapshot_context=snapshot,
            prior_market_turnover_baseline=None,
        )
        breadth = open_drive['market_breadth_context']

        self.assertEqual(breadth['status'], 'available')
        self.assertEqual(breadth['market_symbol_count'], 950)
        self.assertEqual(breadth['expected_symbol_count'], 970)
        self.assertEqual(breadth['observed_symbol_count'], 950)
        self.assertEqual(breadth['up_count'], 700)
        self.assertEqual(breadth['down_count'], 200)
        self.assertEqual(breadth['flat_count'], 50)

    def test_breadth_contract_rejects_subset_violation_and_observed_above_master(self) -> None:
        for case_name in ('upper_not_subset', 'observed_above_master', 'coverage_below_minimum'):
            with self.subTest(case=case_name):
                snapshot = _stored_market_snapshot_context(
                    as_of='2026-04-29T09:10:00+09:00'
                )
                market_row = snapshot['payload']['markets']['kospi']
                if case_name == 'upper_not_subset':
                    market_row['upper_limit_count'] = 701
                    market_row['breadth_raw_buckets']['upper_limit_count'] = 701
                else:
                    if case_name == 'observed_above_master':
                        market_row['breadth_completeness']['expected_symbol_count'] = 949
                    else:
                        market_row['breadth_completeness']['expected_symbol_count'] = 1100
                        market_row['breadth_completeness']['coverage_ratio'] = round(
                            950 / 1100,
                            6,
                        )
                        market_row['breadth_completeness']['expected_universe'][
                            'symbol_count'
                        ] = 1100

                open_drive = _build_market_open_drive_context(
                    requested_market_date=date(2026, 4, 29),
                    market='KOSPI',
                    decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
                    snapshot_context=snapshot,
                    prior_market_turnover_baseline=None,
                )

                self.assertEqual(
                    open_drive['market_breadth_context']['status'],
                    'unavailable',
                )

    def test_breadth_contract_rejects_missing_verification_basis(self) -> None:
        snapshot = _stored_market_snapshot_context(
            as_of='2026-04-29T09:10:00+09:00'
        )
        snapshot['payload']['markets']['kospi']['breadth_completeness'].pop(
            'verification_basis'
        )

        open_drive = _build_market_open_drive_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
            snapshot_context=snapshot,
            prior_market_turnover_baseline=None,
        )

        self.assertEqual(open_drive['market_breadth_context']['status'], 'unavailable')

    def test_market_open_drive_accepts_fresh_kis_leg_when_only_fx_leg_is_degraded(self) -> None:
        snapshot = _stored_market_snapshot_context(
            as_of='2026-04-29T09:10:00+09:00'
        )
        snapshot['snapshot_status'] = 'partial'
        snapshot['last_refresh_status'] = 'degraded'
        snapshot['payload']['status'] = 'partial'
        snapshot['payload']['missing_fields'] = ['usd_krw.rate']

        result = _build_market_open_drive_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
            snapshot_context=snapshot,
            prior_market_turnover_baseline=_prior_market_turnover_baseline(),
        )

        self.assertEqual(result['status'], 'available')
        self.assertEqual(result['provenance']['snapshot_status'], 'partial')
        self.assertEqual(result['provenance']['snapshot_refresh_status'], 'degraded')

    def test_missing_turnover_baseline_preserves_fresh_index_leg_but_cannot_upsize(self) -> None:
        snapshot = _stored_market_snapshot_context(as_of='2026-04-29T09:10:00+09:00')

        open_drive = _build_market_open_drive_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
            snapshot_context=snapshot,
            prior_market_turnover_baseline=None,
        )
        regime = _build_market_regime_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            market_breadth_context=open_drive['market_breadth_context'],
            market_breadth_provenance=open_drive['market_breadth_provenance'],
            sector_peer_context=_sector_context(sector_avg=1.4),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(return_percentile=0.84, volume_percentile=0.78),
            market_relative_provenance=_fresh_provenance(),
            macro_context=_macro_context_fx(value=1320.0),
            open_drive_context=open_drive,
        )

        self.assertEqual(open_drive['status'], 'partial')
        self.assertEqual(open_drive['index_leg']['status'], 'available')
        self.assertIsNotNone(open_drive['gap_up_pct'])
        self.assertIsNotNone(open_drive['early_continuation_pct'])
        self.assertIsNone(open_drive['trading_value_expansion_ratio'])
        self.assertEqual(open_drive['baseline']['status'], 'unavailable')
        self.assertEqual(regime['market_regime'], 'normal')
        self.assertLessEqual(regime['quantity_multiplier'], 1.0)

    def test_market_open_drive_maps_kosdaq_to_its_provider_index(self) -> None:
        result = _build_market_open_drive_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSDAQ',
            decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
            snapshot_context=_stored_market_snapshot_context(
                as_of='2026-04-29T09:10:00+09:00',
                market_key='kosdaq',
            ),
            prior_market_turnover_baseline=_prior_market_turnover_baseline(
                market='KOSDAQ'
            ),
        )

        self.assertEqual(result['status'], 'available')
        self.assertEqual(result['market'], 'KOSDAQ')

    def test_invalid_market_open_drive_provenance_fails_closed(self) -> None:
        valid_snapshot = _stored_market_snapshot_context(
            as_of='2026-04-29T09:10:00+09:00'
        )
        valid_baseline = _prior_market_turnover_baseline()
        cases: dict[str, tuple[dict[str, object], dict[str, object]]] = {}

        stale_snapshot = deepcopy(valid_snapshot)
        stale_row = stale_snapshot['payload']['markets']['kospi']
        stale_row['provider_session_time'] = '09:00:00'
        stale_row['provider_observed_at'] = '2026-04-29T09:00:00+09:00'
        stale_row['index_leg']['provider_observed_at'] = stale_row['provider_observed_at']
        cases['stale_snapshot'] = (stale_snapshot, deepcopy(valid_baseline))

        future_snapshot = deepcopy(valid_snapshot)
        future_row = future_snapshot['payload']['markets']['kospi']
        future_row['provider_session_time'] = '09:11:00'
        future_row['provider_observed_at'] = '2026-04-29T09:11:00+09:00'
        future_row['index_leg']['provider_observed_at'] = future_row['provider_observed_at']
        cases['future_snapshot'] = (future_snapshot, deepcopy(valid_baseline))

        wrong_date_snapshot = deepcopy(valid_snapshot)
        wrong_date_row = wrong_date_snapshot['payload']['markets']['kospi']
        wrong_date_row['provider_business_date'] = '2026-04-28'
        wrong_date_row['provider_observed_at'] = '2026-04-28T09:10:00+09:00'
        wrong_date_row['index_leg']['provider_observed_at'] = wrong_date_row['provider_observed_at']
        cases['wrong_date_snapshot'] = (wrong_date_snapshot, deepcopy(valid_baseline))

        after_hours_snapshot = deepcopy(valid_snapshot)
        after_hours_row = after_hours_snapshot['payload']['markets']['kospi']
        after_hours_row['provider_session_time'] = '15:31:00'
        after_hours_row['provider_observed_at'] = '2026-04-29T15:31:00+09:00'
        after_hours_row['index_leg']['provider_observed_at'] = after_hours_row['provider_observed_at']
        cases['after_hours_provider_row'] = (after_hours_snapshot, deepcopy(valid_baseline))

        missing_source_snapshot = deepcopy(valid_snapshot)
        missing_source_snapshot['source'] = None
        cases['missing_snapshot_source'] = (missing_source_snapshot, deepcopy(valid_baseline))

        missing_field_snapshot = deepcopy(valid_snapshot)
        del missing_field_snapshot['payload']['markets']['kospi']['open']
        cases['missing_provider_field'] = (missing_field_snapshot, deepcopy(valid_baseline))

        wrong_unit_snapshot = deepcopy(valid_snapshot)
        wrong_unit_snapshot['payload']['markets']['kospi']['trading_value_unit'] = 'krw'
        cases['wrong_trading_value_unit'] = (wrong_unit_snapshot, deepcopy(valid_baseline))

        missing_baseline_source = deepcopy(valid_baseline)
        missing_baseline_source['source'] = None
        cases['missing_baseline_source'] = (deepcopy(valid_snapshot), missing_baseline_source)

        low_coverage_baseline = deepcopy(valid_baseline)
        low_coverage_baseline['coverage_ratio'] = 0.4
        cases['low_baseline_coverage'] = (deepcopy(valid_snapshot), low_coverage_baseline)

        count_only_baseline = deepcopy(valid_baseline)
        count_only_baseline.pop('coverage_proof')
        cases['count_only_baseline'] = (deepcopy(valid_snapshot), count_only_baseline)

        digest_missing_baseline = deepcopy(valid_baseline)
        digest_missing_baseline['coverage_proof'].pop('persisted_membership_sha256')
        cases['membership_digest_missing'] = (deepcopy(valid_snapshot), digest_missing_baseline)

        for case_name, (snapshot_context, baseline) in cases.items():
            with self.subTest(case=case_name):
                open_drive = _build_market_open_drive_context(
                    requested_market_date=date(2026, 4, 29),
                    market='KOSPI',
                    decision_time=datetime.fromisoformat('2026-04-29T09:10:30+09:00'),
                    snapshot_context=snapshot_context,
                    prior_market_turnover_baseline=baseline,
                )
                regime = _build_market_regime_context(
                    requested_market_date=date(2026, 4, 29),
                    market='KOSPI',
                    market_breadth_context=_market_breadth_context(up=780, down=140, flat=80),
                    market_breadth_provenance=_fresh_provenance(),
                    sector_peer_context=_sector_context(sector_avg=1.4),
                    sector_peer_provenance=_fresh_provenance(),
                    market_relative_context=_relative_context(return_percentile=0.84, volume_percentile=0.78),
                    market_relative_provenance=_fresh_provenance(),
                    macro_context=_macro_context_fx(value=1320.0),
                    open_drive_context=open_drive,
                )

                expected_status = (
                    'partial'
                    if case_name in {
                        'missing_baseline_source',
                        'low_baseline_coverage',
                        'count_only_baseline',
                        'membership_digest_missing',
                    }
                    else 'unavailable'
                )
                self.assertEqual(open_drive['status'], expected_status)
                self.assertEqual(regime['market_regime'], 'normal')
                self.assertLessEqual(regime['quantity_multiplier'], 1.0)

    def test_build_market_regime_context_returns_normal_posture(self) -> None:
        result = _build_market_regime_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            market_breadth_context=_market_breadth_context(),
            market_breadth_provenance=_fresh_provenance(),
            sector_peer_context=_sector_context(),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(),
            market_relative_provenance=_fresh_provenance(),
        )

        self.assertEqual(result['schema_version'], 'market_regime_context_v1')
        self.assertEqual(result['posture'], 'normal')
        self.assertEqual(result['status'], 'available')
        self.assertEqual(result['market_regime'], 'normal')
        self.assertEqual(result['recommended_exec_action'], 'allow')
        self.assertEqual(result['recommended_sizing_multiplier'], 1.0)
        self.assertEqual(result['quantity_multiplier'], 1.0)
        self.assertNotIn('fx.usd_krw', result['missing_fields'])
        self.assertEqual(result['market_breadth']['up_ratio'], 0.65)
        self.assertEqual(result['sector_context']['sector_name'], 'Semiconductors')
        self.assertTrue(result['relative_strength_context']['leader'])

    def test_build_market_regime_context_detects_aggressive_open_drive(self) -> None:
        result = _build_market_regime_context(
            requested_market_date=date(2026, 7, 23),
            market='KOSPI',
            market_breadth_context=_market_breadth_context(up=780, down=140, flat=80),
            market_breadth_provenance=_fresh_provenance(),
            sector_peer_context=_sector_context(sector_avg=1.4),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(return_percentile=0.84, volume_percentile=0.78),
            market_relative_provenance=_fresh_provenance(),
            macro_context=_macro_context_fx(value=1320.0),
            open_drive_context=_open_drive_context(),
        )

        self.assertEqual(result['market_regime'], 'aggressive_open')
        self.assertEqual(result['quantity_multiplier'], 1.0)
        self.assertGreater(result['shadow_quantity_multiplier'], 1.0)
        self.assertLessEqual(result['shadow_quantity_multiplier'], 2.0)
        self.assertEqual(result['live_upsize_status'], 'shadow_only_uncalibrated')
        self.assertFalse(result['entry_priority_boost'])
        self.assertTrue(result['sizing_window']['shadow_entry_priority_boost'])
        self.assertEqual(result['sizing_window']['quantity_multiplier_cap'], 1.0)
        self.assertEqual(result['valid_until'], '2026-07-23T09:20:00+09:00')
        self.assertIn('early_continuation_confirmed', result['regime_reasons'])

    def test_build_market_regime_context_dampens_high_fx_for_exporter_with_strong_breadth(self) -> None:
        result = _build_market_regime_context(
            requested_market_date=date(2026, 7, 23),
            market='KOSPI',
            market_breadth_context=_market_breadth_context(up=760, down=160, flat=80),
            market_breadth_provenance=_fresh_provenance(),
            sector_peer_context=_sector_context(sector_avg=1.2),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(return_percentile=0.82, volume_percentile=0.76),
            market_relative_provenance=_fresh_provenance(),
            macro_context=_macro_context_fx(value=1455.0, direction='falling'),
            open_drive_context=_open_drive_context(),
        )

        self.assertEqual(result['market_regime'], 'aggressive_open')
        self.assertEqual(result['fx_context']['sector_sensitivity'], 'exporter_or_dollar_revenue_sensitive')
        self.assertEqual(result['fx_context']['penalty_status'], 'dampened')
        self.assertLess(result['fx_context']['penalty_score'], 0.30)
        self.assertEqual(result['quantity_multiplier'], 1.0)
        self.assertGreater(result['shadow_quantity_multiplier'], 1.0)
        self.assertIn('fx_penalty_dampened_for_exporter_sector', result['regime_reasons'])

    def test_build_market_regime_context_keeps_high_fx_penalty_with_weak_breadth(self) -> None:
        result = _build_market_regime_context(
            requested_market_date=date(2026, 7, 23),
            market='KOSPI',
            market_breadth_context=_market_breadth_context(up=420, down=520, flat=60),
            market_breadth_provenance=_fresh_provenance(),
            sector_peer_context={
                **_sector_context(sector_avg=0.2),
                'sector_name': 'Retail',
            },
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(return_percentile=0.55, volume_percentile=0.48),
            market_relative_provenance=_fresh_provenance(),
            macro_context=_macro_context_fx(value=1455.0, direction='rising'),
            open_drive_context=_open_drive_context(),
        )

        self.assertEqual(result['market_regime'], 'defensive')
        self.assertEqual(result['posture'], 'caution')
        self.assertLessEqual(result['quantity_multiplier'], 1.0)
        self.assertFalse(result['entry_priority_boost'])
        self.assertNotEqual(result['recommended_exec_action'], 'allow')
        self.assertEqual(result['fx_context']['sector_sensitivity'], 'domestic_or_import_cost_sensitive')

    def test_build_market_regime_context_blocks_aggressive_mode_on_gap_fade(self) -> None:
        result = _build_market_regime_context(
            requested_market_date=date(2026, 7, 23),
            market='KOSPI',
            market_breadth_context=_market_breadth_context(up=780, down=140, flat=80),
            market_breadth_provenance=_fresh_provenance(),
            sector_peer_context=_sector_context(sector_avg=1.4),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(return_percentile=0.84, volume_percentile=0.78),
            market_relative_provenance=_fresh_provenance(),
            macro_context=_macro_context_fx(value=1320.0),
            open_drive_context=_open_drive_context(early_continuation_pct=-0.25, gap_fade_detected=True),
        )

        self.assertEqual(result['market_regime'], 'normal')
        self.assertEqual(result['reason_code'], 'gap_fade_or_weak_continuation')
        self.assertLessEqual(result['quantity_multiplier'], 1.0)
        self.assertFalse(result['entry_priority_boost'])

    def test_build_market_regime_context_blocks_aggressive_mode_on_weak_continuation(self) -> None:
        result = _build_market_regime_context(
            requested_market_date=date(2026, 7, 23),
            market='KOSPI',
            market_breadth_context=_market_breadth_context(up=780, down=140, flat=80),
            market_breadth_provenance=_fresh_provenance(),
            sector_peer_context=_sector_context(sector_avg=1.4),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(return_percentile=0.84, volume_percentile=0.78),
            market_relative_provenance=_fresh_provenance(),
            macro_context=_macro_context_fx(value=1320.0),
            open_drive_context=_open_drive_context(
                early_continuation_pct=0.05,
                trading_value_expansion_ratio=2.0,
                early_continuation_confirmed=False,
                weak_continuation_detected=True,
            ),
        )

        self.assertEqual(result['market_regime'], 'normal')
        self.assertEqual(result['reason_code'], 'gap_fade_or_weak_continuation')
        self.assertLessEqual(result['quantity_multiplier'], 1.0)
        self.assertFalse(result['open_drive_context']['early_continuation_confirmed'])

    def test_build_market_regime_context_cautions_on_weak_breadth(self) -> None:
        result = _build_market_regime_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            market_breadth_context=_market_breadth_context(up=420, down=520, flat=60),
            market_breadth_provenance=_fresh_provenance(),
            sector_peer_context=_sector_context(),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(),
            market_relative_provenance=_fresh_provenance(),
        )

        self.assertEqual(result['posture'], 'caution')
        self.assertEqual(result['market_regime'], 'defensive')
        self.assertEqual(result['reason_code'], 'weak_market_breadth')
        self.assertEqual(result['recommended_exec_action'], 'downsize')
        self.assertEqual(result['recommended_sizing_multiplier'], 0.5)

    def test_build_market_regime_context_marks_risk_off_on_severe_breadth(self) -> None:
        result = _build_market_regime_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            market_breadth_context=_market_breadth_context(up=250, down=680, flat=70),
            market_breadth_provenance=_fresh_provenance(),
            sector_peer_context=_sector_context(),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(),
            market_relative_provenance=_fresh_provenance(),
        )

        self.assertEqual(result['posture'], 'risk_off')
        self.assertEqual(result['reason_code'], 'severe_market_breadth')
        self.assertEqual(result['recommended_exec_action'], 'reject')
        self.assertEqual(result['recommended_sizing_multiplier'], 0.0)

    def test_build_market_regime_context_marks_stale_and_missing_breadth_explicitly(self) -> None:
        stale = _build_market_regime_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            market_breadth_context=_market_breadth_context(),
            market_breadth_provenance=_stale_provenance(),
            sector_peer_context=_sector_context(),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(),
            market_relative_provenance=_fresh_provenance(),
        )
        missing = _build_market_regime_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            market_breadth_context={
                'status': 'unavailable',
                'reason': 'no_stored_pykrx_market_breadth_row_for_request_date',
                'missing_fields': ['market_symbol_count', 'up_count', 'down_count', 'flat_count'],
            },
            market_breadth_provenance={'freshness_status': 'missing', 'provenance_status': 'derived_from_provider_data'},
            sector_peer_context=_sector_context(),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(),
            market_relative_provenance=_fresh_provenance(),
        )

        self.assertEqual(stale['posture'], 'stale')
        self.assertEqual(stale['recommended_exec_action'], 'no_opinion')
        self.assertEqual(missing['posture'], 'unavailable')
        self.assertEqual(missing['reason_code'], 'market_breadth_unavailable')
        self.assertIn('market_breadth', missing['missing_fields'])

    def test_build_market_regime_context_cautions_on_weak_sector_context(self) -> None:
        result = _build_market_regime_context(
            requested_market_date=date(2026, 4, 29),
            market='KOSPI',
            market_breadth_context=_market_breadth_context(),
            market_breadth_provenance=_fresh_provenance(),
            sector_peer_context=_sector_context(sector_avg=-2.1),
            sector_peer_provenance=_fresh_provenance(),
            market_relative_context=_relative_context(),
            market_relative_provenance=_fresh_provenance(),
        )

        self.assertEqual(result['posture'], 'caution')
        self.assertEqual(result['reason_code'], 'weak_sector_context')
        self.assertTrue(result['sector_context']['weak_sector'])
        self.assertEqual(result['recommended_exec_action'], 'downsize')


class FakeFundamentalsRepository:
    def list_stored_fundamentals(self, *, stock_codes, fs_div=None):
        raise AssertionError('agent context should use summary fundamentals path')

    def list_stored_fundamentals_summary(
        self,
        *,
        stock_codes,
        fs_div=None,
        captured_before=None,
        receipt_date_to=None,
    ):
        assert stock_codes == ['005930', '000660']
        assert fs_div == 'CFS'
        assert captured_before == datetime(
            2026,
            4,
            8,
            6,
            30,
            tzinfo=timezone.utc,
        )
        assert receipt_date_to == date(2026, 4, 8)
        return [
            {
                'rcept_no': '20250312000123',
                'corp_code': '00126380',
                'stock_code': '005930',
                'corp_name': '삼성전자',
                'business_year': 2024,
                'report_code': '11011',
                'filing_category': 'annual_report',
                'fs_div': 'CFS',
                'statement_type': 'consolidated',
                'period_type': 'annual',
                'period_end_month': 12,
                'currency': 'KRW',
                'source_status': '000',
                'source_message': 'OK',
                'captured_at': '2025-03-12T01:02:03+00:00',
                'disclosure': {
                    'report_nm': '사업보고서 (2024.12)',
                    'receipt_date': '2025-03-12',
                },
                'highlights': [
                    {
                        'metric_key': 'revenue',
                        'metric_type': 'account',
                        'statement_type': 'IS',
                        'statement_name': '손익계산서',
                        'account_id': 'ifrs-full_Revenue',
                        'account_name': '매출액',
                        'current_amount': 100,
                        'current_cumulative_amount': 100,
                        'prior_amount': 90,
                        'prior_cumulative_amount': 90,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'operating_income',
                        'metric_type': 'account',
                        'statement_type': 'IS',
                        'statement_name': '손익계산서',
                        'account_id': 'dart_OperatingIncomeLoss',
                        'account_name': '영업이익',
                        'current_amount': 20,
                        'current_cumulative_amount': 20,
                        'prior_amount': 18,
                        'prior_cumulative_amount': 18,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'net_income',
                        'metric_type': 'account',
                        'statement_type': 'IS',
                        'statement_name': '손익계산서',
                        'account_id': 'ifrs-full_ProfitLoss',
                        'account_name': '당기순이익',
                        'current_amount': 15,
                        'current_cumulative_amount': 15,
                        'prior_amount': 14,
                        'prior_cumulative_amount': 14,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'total_assets',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_Assets',
                        'account_name': '자산총계',
                        'current_amount': 500,
                        'current_cumulative_amount': None,
                        'prior_amount': 470,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'current_assets',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_CurrentAssets',
                        'account_name': '유동자산',
                        'current_amount': 240,
                        'current_cumulative_amount': None,
                        'prior_amount': 220,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'cash_and_cash_equivalents',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_CashAndCashEquivalents',
                        'account_name': '현금및현금성자산',
                        'current_amount': 90,
                        'current_cumulative_amount': None,
                        'prior_amount': 75,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'total_liabilities',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_Liabilities',
                        'account_name': '부채총계',
                        'current_amount': 260,
                        'current_cumulative_amount': None,
                        'prior_amount': 250,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'current_liabilities',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_CurrentLiabilities',
                        'account_name': '유동부채',
                        'current_amount': 120,
                        'current_cumulative_amount': None,
                        'prior_amount': 115,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'total_equity',
                        'metric_type': 'account',
                        'statement_type': 'BS',
                        'statement_name': '재무상태표',
                        'account_id': 'ifrs-full_Equity',
                        'account_name': '자본총계',
                        'current_amount': 240,
                        'current_cumulative_amount': None,
                        'prior_amount': 220,
                        'prior_cumulative_amount': None,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                    {
                        'metric_key': 'operating_cash_flow',
                        'metric_type': 'account',
                        'statement_type': 'CF',
                        'statement_name': '현금흐름표',
                        'account_id': 'ifrs-full_CashFlowsFromUsedInOperatingActivities',
                        'account_name': '영업활동순현금흐름',
                        'current_amount': 24,
                        'current_cumulative_amount': 24,
                        'prior_amount': 20,
                        'prior_cumulative_amount': 20,
                        'pre_prior_amount': None,
                        'currency': 'KRW',
                        'ratio_value': None,
                    },
                ],
                'statement_row_count': 1,
            },
            {
                'rcept_no': '20260410000123',
                'corp_code': '00126380',
                'stock_code': '005930',
                'corp_name': '삼성전자',
                'business_year': 2025,
                'report_code': '11013',
                'filing_category': 'quarterly_report',
                'fs_div': 'CFS',
                'statement_type': 'consolidated',
                'period_type': 'q1',
                'period_end_month': 3,
                'currency': 'KRW',
                'source_status': '000',
                'source_message': 'OK',
                'captured_at': '2026-04-10T01:02:03+00:00',
                'disclosure': {
                    'report_nm': '분기보고서 (2025.03)',
                    'receipt_date': '2026-04-10',
                },
                'highlights': [],
                'statement_row_count': 0,
            },
        ]


class FakeDisclosureRepository:
    def list_recent_events_by_stock_codes(
        self,
        *,
        stock_codes,
        receipt_date_to=None,
        observed_before=None,
        limit_per_symbol=5,
    ):
        assert stock_codes == ['005930', '000660']
        assert receipt_date_to.isoformat() == '2026-04-08'
        assert observed_before == datetime(
            2026,
            4,
            8,
            6,
            30,
            tzinfo=timezone.utc,
        )
        assert limit_per_symbol == 3
        return {
            '005930': [
                {
                    'rcept_no': '20260407000001',
                    'corp_code': '00126380',
                    'corp_name': '삼성전자',
                    'report_nm': '[기재정정] 사업보고서 (2025.12)',
                    'filing_category': 'annual_report',
                    'is_financial_report': True,
                    'receipt_date': '2026-04-07',
                    'remark': '정',
                    'fundamentals_capture_status': 'captured',
                },
                {
                    'rcept_no': '20260406000055',
                    'corp_code': '00126380',
                    'corp_name': '삼성전자',
                    'report_nm': '주식등의 대량보유상황보고서',
                    'filing_category': 'other',
                    'is_financial_report': False,
                    'receipt_date': '2026-04-06',
                    'remark': None,
                    'fundamentals_capture_status': 'not_applicable',
                }
            ],
            '000660': [],
        }


class DartEventContextTests(unittest.TestCase):
    def test_recent_dart_events_distinguishes_empty_from_storage_unavailable(self):
        empty = _build_recent_dart_events_context(
            disclosures=[],
            fundamentals={'latest_snapshot': None},
            storage_unavailable=False,
        )
        unavailable = _build_recent_dart_events_context(
            disclosures=[],
            fundamentals={'latest_snapshot': None},
            storage_unavailable=True,
        )
        provenance = _build_recent_disclosures_freshness_provenance(
            disclosures=[],
            requested_market_date=date(2026, 4, 8),
            disclosures_limit=3,
            storage_unavailable=True,
        )

        self.assertEqual(empty['status'], 'empty')
        self.assertEqual(empty['reason'], 'no_recent_disclosures')
        self.assertEqual(unavailable['status'], 'unavailable')
        self.assertEqual(unavailable['reason'], 'dart_disclosure_storage_unavailable')
        self.assertEqual(provenance['provenance_status'], 'storage_unavailable')
        self.assertEqual(provenance['reason'], 'dart_disclosure_storage_unavailable')


class ExternalNewsKnowledgeCutoffTests(unittest.TestCase):
    def test_symbol_scope_resolver_normalizes_case_before_market_lookup(self):
        with patch(
            'app.agent_context.read_service.load_symbols',
            return_value=[
                {'symbol': '005930', 'market': 'KOSPI'},
                {'symbol': 'AAPL', 'market': 'NASDAQ'},
            ],
        ) as load_symbols_mock:
            with self.assertRaisesRegex(
                ValueError,
                'required when a request mixes KRX and US symbols',
            ):
                resolve_external_news_knowledge_cutoff_for_symbols(
                    market_date='2026-04-08',
                    symbols=['005930', 'aapl'],
                    knowledge_cutoff_at=None,
                    now=datetime(2026, 4, 9, 12, tzinfo=timezone.utc),
                )

        self.assertEqual(
            load_symbols_mock.call_args.kwargs['symbols'],
            ['005930', 'AAPL'],
        )

    def test_cutoff_applies_only_to_historical_market_dates(self):
        now = datetime(2026, 4, 9, 12, tzinfo=timezone.utc)

        self.assertEqual(
            _historical_external_news_knowledge_cutoff_at(date(2026, 4, 8), now=now),
            datetime(2026, 4, 8, 6, 30, tzinfo=timezone.utc),
        )
        self.assertIsNone(
            _historical_external_news_knowledge_cutoff_at(date(2026, 4, 9), now=now)
        )

    def test_cutoff_uses_seoul_calendar_day_near_utc_midnight(self):
        now = datetime(2026, 4, 8, 16, tzinfo=timezone.utc)

        self.assertEqual(
            _historical_external_news_knowledge_cutoff_at(date(2026, 4, 8), now=now),
            datetime(2026, 4, 8, 6, 30, tzinfo=timezone.utc),
        )

    def test_aapl_cutoff_uses_xnys_session_close(self):
        cutoff, source = _resolve_external_news_knowledge_cutoff_at(
            date(2026, 4, 8),
            None,
            market='NASDAQ',
            now=datetime(2026, 4, 9, 12, tzinfo=timezone.utc),
        )

        self.assertEqual(cutoff, datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc))
        self.assertEqual(source, 'default_xnys_regular_session_close')

    def test_mixed_krx_and_us_request_requires_explicit_cutoff(self):
        service = AgentContextReadService()
        with patch(
            'app.agent_context.read_service.load_symbols',
            return_value=[
                {'symbol': '005930', 'market': 'KOSPI'},
                {'symbol': 'AAPL', 'market': 'NASDAQ'},
            ],
        ):
            with self.assertRaisesRegex(
                ValueError,
                'required when a request mixes KRX and US symbols',
            ):
                service.get_symbol_day_context(
                    market_date='2026-04-08',
                    symbols=['005930', 'AAPL'],
                )

    def test_mixed_request_accepts_explicit_cutoff(self):
        cutoff, source = _resolve_external_news_knowledge_cutoff_at(
            date(2026, 4, 8),
            '2026-04-08T19:30:00Z',
            market='MIXED',
            now=datetime(2026, 4, 9, 12, tzinfo=timezone.utc),
        )

        self.assertEqual(cutoff, datetime(2026, 4, 8, 19, 30, tzinfo=timezone.utc))
        self.assertEqual(source, 'explicit')

    def test_resolver_uses_request_time_for_current_and_future_dates(self):
        now = datetime(2026, 4, 9, 12, tzinfo=timezone.utc)

        self.assertEqual(
            _resolve_external_news_knowledge_cutoff_at(date(2026, 4, 9), None, now=now),
            (now, 'request_time'),
        )
        self.assertEqual(
            _resolve_external_news_knowledge_cutoff_at(date(2026, 4, 10), None, now=now),
            (now, 'request_time'),
        )

    def test_resolver_accepts_aware_explicit_cutoff_and_normalizes_to_utc(self):
        cutoff, source = _resolve_external_news_knowledge_cutoff_at(
            date(2026, 4, 8),
            '2026-04-08T15:15:00+09:00',
            now=datetime(2026, 4, 9, 12, tzinfo=timezone.utc),
        )

        self.assertEqual(cutoff, datetime(2026, 4, 8, 6, 15, tzinfo=timezone.utc))
        self.assertEqual(source, 'explicit')

    def test_resolver_rejects_naive_or_future_explicit_cutoff(self):
        now = datetime(2026, 4, 9, 12, tzinfo=timezone.utc)

        with self.assertRaisesRegex(ValueError, 'timezone offset'):
            _resolve_external_news_knowledge_cutoff_at(
                date(2026, 4, 8),
                datetime(2026, 4, 8, 15, 30),
                now=now,
            )
        with self.assertRaisesRegex(ValueError, 'current time'):
            _resolve_external_news_knowledge_cutoff_at(
                date(2026, 4, 10),
                datetime(2026, 4, 9, 12, 0, 1, tzinfo=timezone.utc),
                now=now,
            )


class AgentContextReadServiceTests(unittest.TestCase):
    def test_symbol_day_context_wires_fresh_stored_open_drive_evidence_into_regime(self):
        class EmptyFundamentalsRepository:
            def list_stored_fundamentals_summary(
                self,
                *,
                stock_codes,
                fs_div=None,
                captured_before=None,
                receipt_date_to=None,
            ):
                return []

        class EmptyDisclosureRepository:
            def list_recent_events_by_stock_codes(
                self,
                *,
                stock_codes,
                receipt_date_to=None,
                observed_before=None,
                limit_per_symbol=5,
            ):
                return {symbol: [] for symbol in stock_codes}

        service = AgentContextReadService(
            fundamentals_repository=EmptyFundamentalsRepository(),
            disclosure_repository=EmptyDisclosureRepository(),
        )
        valid_open = _stored_market_snapshot_context(as_of='2026-04-29T09:10:00+09:00')
        stale = _stored_market_snapshot_context(as_of='2026-04-29T09:00:00+09:00')
        future = _stored_market_snapshot_context(as_of='2026-04-29T09:11:00+09:00')
        wrong_date = _stored_market_snapshot_context(as_of='2026-04-28T09:10:00+09:00')
        missing_source = deepcopy(valid_open)
        missing_source['source'] = None
        missing_field = deepcopy(valid_open)
        del missing_field['payload']['markets']['kospi']['open']
        low_coverage = _prior_market_turnover_baseline()
        low_coverage['coverage_ratio'] = 0.4
        same_provider_prior = deepcopy(valid_open)
        same_provider_prior['payload']['markets']['kospi'].update(
            {
                'prior_trading_value': 8_000_000,
                'prior_trading_value_unit': 'million_krw',
            }
        )
        fresh_downside_without_baseline = deepcopy(valid_open)
        fresh_downside_market = fresh_downside_without_baseline['payload']['markets']['kospi']
        fresh_downside_market.update(
            {
                'advancers': 250,
                'unchanged': 20,
                'decliners': 680,
                'breadth_raw_buckets': {
                    'upper_limit_count': 0,
                    'advancers': 250,
                    'unchanged': 20,
                    'decliners': 680,
                    'lower_limit_count': 0,
                },
            }
        )
        cases = (
            (
                'same_provider_prior_skips_eod_fallback',
                '2026-04-29T09:10:30+09:00',
                same_provider_prior,
                SQLAlchemyError('fallback must not be called'),
                'aggressive_open',
            ),
            (
                'fresh_opening',
                '2026-04-29T09:10:30+09:00',
                valid_open,
                _prior_market_turnover_baseline(),
                'aggressive_open',
            ),
            (
                'fresh_trend',
                '2026-04-29T09:25:30+09:00',
                _stored_market_snapshot_context(as_of='2026-04-29T09:25:00+09:00'),
                _prior_market_turnover_baseline(),
                'trend_day',
            ),
            (
                'fresh_downside_without_turnover_baseline',
                '2026-04-29T09:10:30+09:00',
                fresh_downside_without_baseline,
                None,
                'defensive',
            ),
            ('stale', '2026-04-29T09:10:30+09:00', stale, _prior_market_turnover_baseline(), 'normal'),
            ('future', '2026-04-29T09:10:30+09:00', future, _prior_market_turnover_baseline(), 'normal'),
            ('wrong_date', '2026-04-29T09:10:30+09:00', wrong_date, _prior_market_turnover_baseline(), 'normal'),
            (
                'missing_source',
                '2026-04-29T09:10:30+09:00',
                missing_source,
                _prior_market_turnover_baseline(),
                'normal',
            ),
            (
                'missing_field',
                '2026-04-29T09:10:30+09:00',
                missing_field,
                _prior_market_turnover_baseline(),
                'normal',
            ),
            ('low_coverage', '2026-04-29T09:10:30+09:00', valid_open, low_coverage, 'normal'),
            (
                'baseline_storage_error',
                '2026-04-29T09:10:30+09:00',
                valid_open,
                SQLAlchemyError('baseline unavailable'),
                'normal',
            ),
            (
                'baseline_timeout',
                '2026-04-29T09:10:30+09:00',
                valid_open,
                TimeoutError('baseline query timed out'),
                'normal',
            ),
            (
                'snapshot_storage_timeout',
                '2026-04-29T09:10:30+09:00',
                TimeoutError('snapshot query timed out'),
                _prior_market_turnover_baseline(),
                'normal',
            ),
        )

        for case_name, server_time, snapshot_context, baseline, expected_regime in cases:
            with self.subTest(case=case_name), ExitStack() as stack:
                stack.enter_context(
                    patch(
                        'app.agent_context.read_service.load_symbols',
                        return_value=[
                            {
                                'id': 1,
                                'symbol': '005930',
                                'standard_code': 'KR7005930003',
                                'name_ko': '삼성전자',
                                'market': 'KOSPI',
                                'instrument_type': 'equity',
                                'source': 'pykrx',
                                'is_active': True,
                                'last_refreshed_at': None,
                            }
                        ],
                    )
                )
                empty_loaders = (
                    'load_daily_technical_snapshots',
                    'load_daily_signal_features',
                    'load_daily_action_summaries',
                    'load_daily_price_contexts',
                    'load_daily_price_context_history',
                    'load_current_quotes',
                    'load_current_kis_intraday_snapshots',
                    'load_daily_value_factor_results',
                    'load_daily_unified_value_factor_results',
                    'load_daily_quality_factor_results',
                    'load_daily_momentum_factor_results',
                    'load_daily_advanced_value_factor_results',
                    'load_daily_low_risk_factor_results',
                    'load_daily_beta_factor_results',
                    'load_daily_composite_factor_results',
                    'load_daily_market_relative_contexts',
                )
                for loader_name in empty_loaders:
                    stack.enter_context(
                        patch(f'app.agent_context.read_service.{loader_name}', return_value=[])
                    )
                stack.enter_context(
                    patch(
                        'app.agent_context.read_service.load_current_market_snapshot_context',
                        side_effect=(
                            snapshot_context
                            if isinstance(snapshot_context, Exception)
                            else None
                        ),
                        return_value=(
                            None
                            if isinstance(snapshot_context, Exception)
                            else snapshot_context
                        ),
                    )
                )
                baseline_loader = stack.enter_context(
                    patch(
                        'app.agent_context.read_service.load_prior_market_turnover_baselines',
                        side_effect=baseline if isinstance(baseline, Exception) else None,
                        return_value=None if isinstance(baseline, Exception) else {'KOSPI': baseline},
                    )
                )
                stack.enter_context(
                    patch(
                        'app.agent_context.read_service.pykrx_enrichment_service.load_symbol_day_context_inputs',
                        return_value={
                            'by_symbol': {},
                            'by_market': {},
                            'sector_rows_by_market': {},
                        },
                    )
                )
                stack.enter_context(
                    patch(
                        'app.agent_context.read_service._load_latest_symbol_day_block_history',
                        return_value=EMPTY_LATEST_PRIOR_ROWS,
                    )
                )
                stack.enter_context(
                    patch('app.agent_context.read_service.build_signal_decider_scores', return_value={})
                )
                stack.enter_context(
                    patch(
                        'app.agent_context.read_service.get_market_state',
                        return_value=SimpleNamespace(
                            market='KRX',
                            state='OPEN',
                            is_trading_day=True,
                            is_open=True,
                            server_time=server_time,
                        ),
                    )
                )
                stack.enter_context(
                    patch(
                        'app.agent_context.read_service.build_market_session_metadata',
                        return_value=_open_market_session(market_date='2026-04-29'),
                    )
                )
                stack.enter_context(
                    patch(
                        'app.agent_context.read_service.macro_context_service.get_context',
                        return_value={'status': 'unavailable', 'series': []},
                    )
                )
                stack.enter_context(
                    patch.object(settings, 'external_news_context_enabled', False)
                )

                result = service.get_symbol_day_context(
                    market_date='2026-04-29',
                    symbols=['005930'],
                )

            item = result['items'][0]
            self.assertEqual(item['market_regime_context']['market_regime'], expected_regime)
            if case_name == 'fresh_downside_without_turnover_baseline':
                self.assertEqual(item['market_regime_context']['posture'], 'risk_off')
                self.assertEqual(item['market_regime_context']['recommended_exec_action'], 'reject')
                self.assertEqual(item['market_breadth_context']['source'], 'kis_market_snapshot_v1')
                self.assertEqual(item['market_breadth_context']['up_count'], 250)
                self.assertEqual(item['market_breadth_context']['down_count'], 680)
                self.assertEqual(
                    item['context_provenance']['market_breadth_context']['source_provider'],
                    'kis_market_snapshot_v1',
                )
            if expected_regime in {'aggressive_open', 'trend_day'}:
                self.assertEqual(item['market_regime_context']['quantity_multiplier'], 1.0)
                self.assertGreater(
                    item['market_regime_context']['shadow_quantity_multiplier'],
                    1.0,
                )
                self.assertEqual(
                    item['market_regime_context']['live_upsize_status'],
                    'shadow_only_uncalibrated',
                )
                self.assertEqual(item['market_breadth_context']['source'], 'kis_market_snapshot_v1')
                self.assertEqual(
                    item['context_provenance']['market_breadth_context']['source_provider'],
                    'kis_market_snapshot_v1',
                )
                self.assertEqual(
                    item['market_regime_context']['open_drive_context']['provenance'][
                        'validation_status'
                    ],
                    'validated',
                )
            else:
                self.assertLessEqual(item['market_regime_context']['quantity_multiplier'], 1.0)
                self.assertFalse(
                    item['market_regime_context']['open_drive_context']['authoritative']
                )
            if case_name in {'baseline_storage_error', 'baseline_timeout'}:
                self.assertEqual(
                    item['market_regime_context']['open_drive_context']['status'],
                    'unavailable',
                    item['market_regime_context']['open_drive_context'],
                )
                self.assertEqual(
                    item['market_regime_context']['open_drive_context']['index_leg']['status'],
                    'available',
                )
            if case_name in {
                'same_provider_prior_skips_eod_fallback',
                'snapshot_storage_timeout',
            }:
                baseline_loader.assert_not_called()
            else:
                baseline_loader.assert_called_once_with(
                    market_date=date(2026, 4, 29),
                    markets=['KOSPI'],
                )

    def test_krx_symbol_uses_request_session_gate_instead_of_unguarded_current_cache(self):
        krx_session = _closed_market_session()

        resolved = _resolve_symbol_market_session(
            company_market='KOSPI',
            requested_date=date(2026, 4, 10),
            krx_market_session=krx_session,
        )
        quote_context = _build_quote_context(
            {'symbol': '005930', 'price': 70000},
            market_date=date(2026, 4, 10),
            market_session=resolved,
        )

        self.assertIs(resolved, krx_session)
        self.assertEqual(quote_context['status'], 'not_applicable')
        self.assertFalse(quote_context['available'])
        self.assertIsNone(quote_context['price'])

    def test_us_market_session_lookup_failure_fails_current_cache_closed(self):
        with patch(
            'app.agent_context.read_service.exchange_calendars.get_calendar',
            side_effect=RuntimeError('calendar unavailable'),
        ):
            session = _build_us_market_session_metadata(
                date(2026, 4, 8),
                now=datetime(2026, 4, 8, 15, tzinfo=timezone.utc),
            )

        self.assertEqual(session['status'], 'unavailable')
        self.assertEqual(session['reason'], 'us_market_session_unavailable')
        self.assertFalse(session['quote_data_expected'])
        self.assertFalse(session['intraday_data_expected'])
        quote_context = _build_quote_context(
            {'symbol': 'AAPL', 'price': 200},
            market_date=date(2026, 4, 8),
            market_session=session,
        )
        self.assertEqual(quote_context['status'], 'not_applicable')
        self.assertIsNone(quote_context['price'])
        self.assertEqual(quote_context['freshness']['reason'], 'us_market_session_unavailable')

    def test_nyse_american_symbol_uses_us_session_gate(self):
        us_session = {
            **_closed_market_session(),
            'reason': 'historical_requested_date',
            'calendar_source': 'exchange_calendars:XNYS',
        }

        resolved = _resolve_symbol_market_session(
            company_market='NYSE_AMERICAN',
            requested_date=date(2026, 4, 10),
            krx_market_session=_open_market_session(),
            us_market_session=us_session,
        )

        self.assertIs(resolved, us_session)
        self.assertFalse(resolved['quote_data_expected'])

    @patch('app.agent_context.read_service.load_daily_market_relative_contexts')
    @patch('app.agent_context.read_service.load_daily_composite_factor_results')
    @patch('app.agent_context.read_service.load_daily_beta_factor_results')
    @patch('app.agent_context.read_service.load_daily_low_risk_factor_results')
    @patch('app.agent_context.read_service.load_daily_advanced_value_factor_results')
    @patch('app.agent_context.read_service.load_daily_momentum_factor_results')
    @patch('app.agent_context.read_service.load_daily_quality_factor_results')
    @patch('app.agent_context.read_service.load_daily_unified_value_factor_results')
    @patch('app.agent_context.read_service.load_daily_value_factor_results')
    @patch('app.agent_context.read_service.load_current_quotes')
    @patch('app.agent_context.read_service.load_current_kis_intraday_snapshots')
    @patch('app.agent_context.read_service.load_daily_price_context_history')
    @patch('app.agent_context.read_service.load_daily_price_contexts')
    @patch('app.agent_context.read_service.load_daily_action_summaries')
    @patch('app.agent_context.read_service.load_daily_signal_features')
    @patch('app.agent_context.read_service.load_daily_technical_snapshots')
    @patch('app.agent_context.read_service.load_symbols')
    def test_get_symbol_day_context_aggregates_existing_blocks(
        self,
        load_symbols_mock,
        load_technical_mock,
        load_signal_features_mock,
        load_actions_mock,
        load_price_contexts_mock,
        load_price_context_history_mock,
        load_current_kis_intraday_snapshots_mock,
        load_current_quotes_mock,
        load_value_mock,
        load_unified_value_mock,
        load_quality_mock,
        load_momentum_mock,
        load_advanced_value_mock,
        load_low_risk_mock,
        load_beta_mock,
        load_composite_mock,
        load_market_relative_mock,
    ):
        load_symbols_mock.return_value = [
            {
                'id': 1,
                'symbol': '005930',
                'standard_code': 'KR7005930003',
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'instrument_type': 'equity',
                'source': 'pykrx',
                'is_active': True,
                'last_refreshed_at': None,
            }
        ]
        load_technical_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'pykrx',
                'close_price': 71000,
                'volume': 100,
                'trend_state': 'uptrend',
                'ma_alignment': 'bullish',
                'breakout_20d': True,
                'breakdown_20d': False,
                'return_5d': 5.2,
                'return_20d': 12.1,
                'volume_ratio_20d': 1.8,
                'atr_14': 1200.0,
                'atr_pct_14': 1.7,
                'signal_payload': {},
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_actions_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'pykrx',
                'action_label': 'long_breakout',
                'bias_direction': 'long',
                'signal_score': 78,
                'confidence_tier': 'high',
                'risk_tier': 'low',
                'trigger_price': 71000.0,
                'invalidation_price': 69800.0,
                'support_price': 68000.0,
                'resistance_price': 70800.0,
                'reasons': ['fresh_20d_breakout'],
                'summary_payload': {},
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_signal_features_mock.return_value = [
            {
                'symbol': '005930',
                'name_ko': '삼성전자',
                'market': 'KOSPI',
                'market_date': '2026-04-08',
                'source': 'kis_ranking',
                'signal_count': 2,
                'volume_power_rank': 3,
                'volume_power': 140.5,
                'overtime_volume_rank': 7,
                'overtime_volume': 120000,
                'overtime_fluctuation_rank': None,
                'overtime_fluctuation_rate': None,
                'row_kind': 'direct_plus_fallback',
                'provenance': {
                    'row_kind': 'direct_plus_fallback',
                    'direct_signal_source_count': 2,
                    'fallback_groups_present': ['price_action', 'factor_scores'],
                },
                'freshness': {
                    'market_date': '2026-04-08',
                    'direct_sources_collected_at': '2026-04-08T15:00:00+09:00',
                },
                'fallback_features': {
                    'price_action': {'close_return_1d_pct': 2.43},
                },
                'feature_payload': {
                    'sources': {'volume-power': {'rank': 3}},
                    'row_provenance': {
                        'row_kind': 'direct_plus_fallback',
                        'direct_signal_source_count': 2,
                        'fallback_groups_present': ['price_action', 'factor_scores'],
                    },
                },
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_value_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'value_factor_v1',
                'feature_version': 'value_factor_v1',
                'per': 10.1,
                'pbr': 1.2,
                'dividend_yield': 2.1,
                'per_status': 'available',
                'per_rank': 15,
                'pbr_rank': 22,
                'dividend_yield_rank': 31,
                'per_percentile': 0.97,
                'pbr_percentile': 0.92,
                'dividend_yield_percentile': 0.88,
                'value_score': 0.923333,
                'value_rank': 9,
                'value_percentile': 0.95,
                'value_status': 'scored',
                'factor_payload': {
                    'availability': {
                        'valid_component_keys': ['per', 'pbr', 'dividend_yield'],
                        'missing_component_keys': [],
                    }
                },
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_unified_value_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'unified_value_v1',
                'feature_version': 'unified_value_v1',
                'unified_value_score': 0.84,
                'unified_value_rank': 5,
                'unified_value_percentile': 0.96,
                'components_used': 5,
                'unified_value_status': 'scored',
                'per': 10.1,
                'pbr': 1.2,
                'dividend_yield': 2.1,
                'fcf_yield': 4.3,
                'ev_to_ebitda': 8.4,
                'per_status': 'available',
                'pbr_status': 'available',
                'dividend_yield_status': 'available',
                'fcf_yield_status': 'available',
                'ev_to_ebitda_status': 'available',
                'factor_payload': {
                    'availability': {
                        'components_used': 5,
                        'components_required': 5,
                        'valid_component_keys': ['per', 'pbr', 'dividend_yield', 'fcf_yield', 'ev_to_ebitda'],
                        'missing_component_keys': [],
                    },
                    'component_statuses': {
                        'per': 'available',
                        'pbr': 'available',
                        'dividend_yield': 'available',
                        'fcf_yield': 'available',
                        'ev_to_ebitda': 'available',
                    },
                    'methodology': {
                        'score_method': 'equal_weight_mean_of_five_component_percentiles',
                        'score_requirement': 'all_five_components_required',
                        'ranking_directions': {
                            'per': 'lower_better_positive_only',
                            'pbr': 'lower_better_positive_only',
                            'dividend_yield': 'higher_better_non_negative',
                            'fcf_yield': 'higher_better',
                            'ev_to_ebitda': 'lower_better',
                            'unified_value_score': 'higher_better',
                        },
                    },
                },
            }
        ]
        load_quality_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'quality_v1',
                'feature_version': 'quality_v1',
                'quality_score': 0.741,
                'quality_rank': 7,
                'quality_percentile': 0.88,
                'components_used': 5,
                'components_possible': 6,
                'is_quality_score_complete': False,
                'quality_status': 'scored_partial',
                'roe': 0.12,
                'roa': 0.08,
                'gross_profit_to_assets': 0.11,
                'operating_cash_flow_to_net_income': 1.2,
                'debt_ratio': 0.41,
                'accruals_ratio': None,
                'roe_status': 'available',
                'roa_status': 'available',
                'gross_profit_to_assets_status': 'available',
                'operating_cash_flow_to_net_income_status': 'available',
                'debt_ratio_status': 'available',
                'accruals_ratio_status': 'missing_total_assets',
                'factor_payload': {
                    'availability': {
                        'components_used': 5,
                        'components_possible': 6,
                        'is_quality_score_complete': False,
                        'valid_component_keys': [
                            'roe',
                            'roa',
                            'gross_profit_to_assets',
                            'operating_cash_flow_to_net_income',
                            'debt_ratio',
                        ],
                        'missing_component_keys': ['accruals_ratio'],
                    },
                    'component_statuses': {
                        'roe': 'available',
                        'roa': 'available',
                        'gross_profit_to_assets': 'available',
                        'operating_cash_flow_to_net_income': 'available',
                        'debt_ratio': 'available',
                        'accruals_ratio': 'missing_total_assets',
                    },
                    'methodology': {
                        'score_method': 'equal_weight_mean_of_available_component_percentiles',
                        'score_requirement': 'at_least_one_component_required',
                        'complete_requirement': 'all_six_components_required',
                        'ranking_directions': {
                            'roe': 'higher_better',
                            'roa': 'higher_better',
                            'gross_profit_to_assets': 'higher_better',
                            'operating_cash_flow_to_net_income': 'higher_better_positive_net_income_required',
                            'debt_ratio': 'lower_better',
                            'accruals_ratio': 'lower_better',
                            'quality_score': 'higher_better',
                        },
                    },
                },
            }
        ]
        load_momentum_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'daily_price_bars',
                'feature_version': 'price_momentum_v1',
                'close_price': 71500,
                'ma_60': 68500.0,
                'ma_120': 64100.0,
                'momentum_12_1': 0.421,
                'return_5d': 0.052,
                'momentum_rank': 12,
                'momentum_percentile': 0.91,
                'momentum_percentile_threshold': 0.8,
                'passes_momentum_top_percentile': True,
                'is_momentum_eligible': True,
                'factor_payload': {
                    'feature_version': 'price_momentum_v1',
                    'ranking': {
                        'momentum_percentile_threshold': 0.8,
                    },
                    'eligibility': {
                        'passes_trend_confirmation': True,
                        'passes_momentum_top_percentile': True,
                        'momentum_eligibility_status': 'eligible',
                        'is_momentum_eligible': True,
                    },
                    'eligibility_rule': {
                        'rule_version': 'momentum_top_percentile_v1',
                        'percentile_gate': {
                            'field': 'momentum_percentile',
                            'comparison': '>=',
                            'threshold': 0.8,
                        },
                    },
                    'overheat': {
                        'has_sufficient_cross_sectional_context': True,
                        'is_recent_overheated': False,
                    },
                    'overheat_rule': {
                        'rule_version': 'momentum_overheat_v2',
                    },
                    'volatility': {
                        'return_5d_vol_adjusted': 1.4,
                    },
                },
            }
        ]
        load_advanced_value_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'advanced_value_v1',
                'feature_version': 'advanced_value_v1',
                'advanced_value_score': 0.81,
                'advanced_value_rank': 9,
                'advanced_value_percentile': 0.92,
                'advanced_value_components_used': 2,
                'advanced_value_classification': 'eligible',
                'is_advanced_value_eligible': True,
                'market_cap_proxy': 1000000,
                'enterprise_value': 1200000,
                'ebitda': 50000,
                'free_cash_flow': 42000,
                'fcf_yield': 0.041,
                'ev_to_ebitda': 24.0,
                'fcf_yield_status': 'available',
                'ev_to_ebitda_status': 'available',
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_low_risk_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'low_risk_v1',
                'feature_version': 'low_risk_v1',
                'low_risk_score': 0.67,
                'low_risk_classification': 'watchlist',
                'is_low_risk_eligible': False,
                'volatility_60d': 0.12,
                'volatility_120d': 0.18,
                'max_drawdown_120d': 0.22,
                'avg_trading_value_20d': 1200000000,
                'volatility_60d_percentile': 0.81,
                'volatility_120d_percentile': 0.75,
                'max_drawdown_120d_percentile': 0.62,
                'liquidity_percentile': 0.91,
                'passes_liquidity_floor': True,
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_beta_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'beta_v1',
                'feature_version': 'beta_v1',
                'low_beta_score': 0.58,
                'low_beta_classification': 'eligible',
                'is_low_beta_eligible': True,
                'benchmark_id': 'KOSPI',
                'benchmark_market': 'KOSPI',
                'beta_60d': 0.84,
                'beta_120d': 0.92,
                'beta_60d_percentile': 0.77,
                'beta_120d_percentile': 0.69,
                'history_days_available': 120,
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_composite_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'composite_v1',
                'feature_version': 'composite_v1',
                'composite_score': 0.73,
                'composite_rank': 11,
                'composite_percentile': 0.89,
                'composite_classification': 'eligible',
                'is_composite_eligible': True,
                'valid_factor_count': 5,
                'available_base_weight': 1.0,
                'has_required_inputs': True,
                'is_hard_excluded': False,
                'momentum_input': 0.91,
                'value_input': 0.63,
                'advanced_value_input': 0.81,
                'low_risk_input': 0.67,
                'low_beta_input': 0.58,
                'captured_at': '2026-04-08T15:00:00+09:00',
            }
        ]
        load_current_quotes_mock.return_value = [
            {
                'symbol': '005930',
                'source': 'kis',
                'market_state': 'OPEN',
                'is_live': True,
                'fallback': False,
                'available': True,
                'price': 71550,
                'change': 50,
                'change_pct': 0.07,
                'volume': 150000,
                'turnover': 9000000000,
                'bid_price': 71500,
                'ask_price': 71600,
                'as_of': '095700',
                'trade_date': '2026-04-08',
                'cached_at': '2026-04-08T09:57:00+09:00',
                'captured_at': '2026-04-08T09:57:00+09:00',
            }
        ]
        load_current_kis_intraday_snapshots_mock.return_value = [
            _make_intraday_execution_row()
        ]
        load_price_contexts_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'pykrx',
                'context_payload': {
                    'trade_date': '2026-04-08',
                    'source': 'pykrx',
                    'captured_at': '2026-04-08T15:30:00+09:00',
                    'open_price': 70000,
                    'high_price': 72000,
                    'low_price': 69950,
                    'close_price': 71500,
                    'volume': 123456,
                    'turnover': 8800000000,
                    'liquidity': {
                        'turnover_krw': 8800000000,
                        'turnover_source': 'provider',
                        'turnover_is_estimated': False,
                        'provider_turnover_krw': 8800000000,
                        'estimated_turnover_krw': 8827104000,
                    },
                    'price_bar_quality': {
                        'open_price_zero': False,
                        'turnover_missing_or_zero': False,
                        'volume_zero': False,
                        'gap_unreliable_due_to_open': False,
                        'possible_suspension_or_special_session': False,
                        'has_any_issue': False,
                        'issue_codes': [],
                    },
                    'candle': {
                        'direction': 'bullish',
                        'body_points': 1500,
                        'body_percent_of_open': 2.1429,
                        'body_percent_of_range': 73.1707,
                        'body_size': 'large',
                        'upper_wick_points': 500,
                        'lower_wick_points': 50,
                        'wick_bias': 'upper',
                    },
                    'session_range': {
                        'points': 2050,
                        'percent_of_open': 2.9286,
                        'close_position_in_range': 0.7561,
                        'close_position_label': 'upper_half',
                    },
                    'gap': {
                        'previous_trade_date': '2026-04-07',
                        'previous_close_price': 69800,
                        'gap_points': 200,
                        'gap_percent': 0.2865,
                        'direction': 'up',
                        'filled_intraday': False,
                        'reliable': True,
                    },
                },
                'captured_at': '2026-04-08T15:30:00+09:00',
            }
        ]
        load_price_context_history_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-07',
                'source': 'pykrx',
                'context_payload': {
                    'trade_date': '2026-04-07',
                    'open_price': 69400,
                    'close_price': 69800,
                    'candle': {'direction': 'bullish'},
                    'session_range': {'close_position_label': 'upper_half'},
                    'gap': {'direction': 'flat'},
                },
                'captured_at': '2026-04-07T15:30:00+09:00',
            },
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'source': 'pykrx',
                'context_payload': {
                    'trade_date': '2026-04-08',
                    'open_price': 70000,
                    'close_price': 71500,
                    'candle': {'direction': 'bullish'},
                    'session_range': {'close_position_label': 'upper_half'},
                    'gap': {'direction': 'up'},
                },
                'captured_at': '2026-04-08T15:30:00+09:00',
            },
        ]
        load_market_relative_mock.return_value = [
            {
                'symbol': '005930',
                'market_date': '2026-04-08',
                'status': 'available',
                'market': 'KOSPI',
                'breadth': {
                    'basis': 'close_vs_previous_close',
                    'market_symbol_count': 810,
                    'up_count': 470,
                    'down_count': 260,
                    'flat_count': 80,
                    'missing_previous_close_count': 0,
                },
                'relative_strength': {
                    'return_pct': 2.43,
                    'return_rank': 41,
                    'return_percentile': 0.95,
                    'return_population': 810,
                    'volume': 123456,
                    'volume_rank': 55,
                    'volume_percentile': 0.93,
                    'volume_population': 810,
                },
            }
        ]

        service = AgentContextReadService(
            fundamentals_repository=FakeFundamentalsRepository(),
            disclosure_repository=FakeDisclosureRepository(),
        )
        with (
            patch(
                'app.agent_context.decision_input_quality._utcnow',
                return_value=datetime(2026, 4, 8, 0, 57, 10, tzinfo=timezone.utc),
            ),
            patch(
                'app.agent_context.read_service.build_market_session_metadata',
                return_value=_open_market_session(),
            ),
            patch(
                'app.agent_context.read_service._load_latest_symbol_day_block_history',
                return_value=EMPTY_LATEST_PRIOR_ROWS,
            ),
            patch(
                'app.agent_context.read_service._external_news_provider_status',
                return_value={
                    'enabled': False,
                    'status': 'disabled',
                    'reason': 'naver_search_credentials_missing',
                },
            ),
            patch(
                'app.agent_context.read_service.list_recent_external_news_for_symbols',
                return_value={'005930': [], '000660': []},
            ) as external_news_mock,
            patch(
                'app.agent_context.read_service.pykrx_enrichment_service.load_symbol_day_context_inputs',
                return_value={'by_symbol': {}, 'by_market': {}, 'sector_rows_by_market': {}},
            ),
            patch(
                'app.agent_context.read_service.macro_context_service.get_context',
                return_value={'status': 'unavailable', 'freshness': {'reason': 'test_fixture'}},
            ),
            patch.object(settings, 'external_news_context_enabled', True),
        ):
            result = service.get_symbol_day_context(
                market_date='2026-04-08',
                symbols=['005930', '000660'],
                fs_div='cfs',
                disclosures_limit=3,
            )

        self.assertEqual(load_technical_mock.call_args.kwargs['fallback_window_days'], 0)
        knowledge_cutoff = datetime(2026, 4, 8, 6, 30, tzinfo=timezone.utc)
        self.assertEqual(external_news_mock.call_args.kwargs['published_before'], knowledge_cutoff)
        self.assertEqual(external_news_mock.call_args.kwargs['collected_before'], knowledge_cutoff)

        self.assertEqual(result['market_session']['requested_date'], '2026-04-08')
        self.assertEqual(result['market_session']['latest_valid_trading_date'], '2026-04-08')
        self.assertEqual(result['summary']['market_session']['requested_date'], '2026-04-08')
        self.assertEqual(result['summary']['requested_symbol_count'], 2)
        self.assertEqual(result['summary']['matched_symbol_count'], 1)
        self.assertEqual(result['summary']['quote_context_count'], 1)
        self.assertEqual(result['summary']['price_context_count'], 1)
        self.assertEqual(result['summary']['intraday_execution_context_count'], 1)
        self.assertEqual(result['summary']['technical_snapshot_count'], 1)
        self.assertEqual(result['summary']['action_summary_count'], 1)
        self.assertEqual(result['summary']['signal_feature_count'], 1)
        self.assertEqual(result['summary']['market_relative_context_count'], 1)
        self.assertEqual(result['summary']['fundamentals_count'], 1)
        self.assertEqual(result['summary']['missing_symbols'], ['000660'])
        self.assertEqual(result['summary']['factor_row_counts']['unified_value'], 1)
        self.assertEqual(result['summary']['context_completeness']['signal_features']['coverage_status'], 'sparse')
        self.assertEqual(result['summary']['context_completeness']['signal_features']['missing_rows'], 1)

        first_item = result['items'][0]
        self.assertEqual(
            first_item['recent_external_news']['freshness_provenance']['knowledge_cutoff_at'],
            '2026-04-08T06:30:00+00:00',
        )
        self.assertEqual(
            first_item['recent_external_news']['freshness_provenance']['knowledge_cutoff_source'],
            'default_krx_regular_session_close',
        )
        self.assertEqual(
            first_item['recent_external_news']['freshness_provenance']['knowledge_cutoff_operator'],
            '<',
        )
        self.assertEqual(first_item['symbol'], '005930')
        self.assertEqual(first_item['company']['name_ko'], '삼성전자')
        self.assertEqual(first_item['company']['dart']['corp_code'], '00126380')
        self.assertTrue(first_item['quote_context']['available'])
        self.assertEqual(first_item['quote_context']['source'], 'kis')
        self.assertEqual(first_item['quote_context']['spread']['spread_points'], 100)
        self.assertEqual(first_item['quote_context']['spread']['spread_bps_of_price'], 13.9762)
        self.assertEqual(first_item['market_session']['requested_date'], '2026-04-08')
        self.assertEqual(first_item['market_session']['latest_valid_trading_date'], '2026-04-08')
        self.assertEqual(
            first_item['quote_context']['freshness']['trade_date_relation_to_request'],
            'same_market_date',
        )
        self.assertEqual(first_item['quote_context']['freshness_provenance']['freshness_status'], 'fresh')
        self.assertEqual(first_item['quote_context']['freshness_provenance']['provenance_status'], 'provider_sourced')
        self.assertEqual(first_item['price_context']['open_price'], 70000)
        self.assertEqual(first_item['price_context']['liquidity']['turnover_source'], 'provider')
        self.assertEqual(first_item['price_context']['price_bar_quality']['issue_codes'], [])
        self.assertEqual(first_item['price_context']['gap']['previous_close_price'], 69800)
        self.assertEqual(first_item['price_context']['gap']['direction'], 'up')
        self.assertFalse(first_item['price_context']['gap']['filled_intraday'])
        self.assertTrue(first_item['price_context']['gap']['reliable'])
        self.assertEqual(first_item['price_context']['session_range']['close_position_label'], 'upper_half')
        self.assertEqual(first_item['price_context']['candle']['wick_bias'], 'upper')
        self.assertEqual(first_item['price_context']['freshness_provenance']['freshness_status'], 'fresh')
        self.assertEqual(first_item['price_context']['freshness_provenance']['provenance_status'], 'derived_from_provider_data')
        self.assertEqual(first_item['liquidity_context']['status'], 'available')
        self.assertEqual(first_item['liquidity_context']['turnover']['turnover_krw'], 8800000000)
        self.assertEqual(first_item['liquidity_context']['turnover']['turnover_source'], 'provider')
        self.assertEqual(first_item['liquidity_context']['ranking']['liquidity_percentile'], 0.91)
        self.assertEqual(first_item['liquidity_context']['ranking']['liquidity_rank'], None)
        self.assertEqual(first_item['liquidity_context']['intraday']['status'], 'partial')
        self.assertEqual(first_item['liquidity_context']['intraday']['quote_spread_points'], 100)
        self.assertEqual(first_item['liquidity_context']['intraday']['quote_balance_rank'], 5)
        self.assertEqual(first_item['decider_data_quality_context']['status'], 'partial')
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['liquidity_turnover']['status'],
            'available',
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['liquidity_turnover']['turnover']['status'],
            'available',
        )
        self.assertEqual(first_item['decision_input_quality_context']['schema_version'], 'decision_input_quality_v1')
        executable_quote = first_item['pre_decider_enrichment_context']['quote_orderbook_snapshot']
        self.assertEqual(executable_quote['snapshot_type'], 'live_intraday')
        self.assertEqual(executable_quote['spread_source_context'], 'quote_context')
        self.assertEqual(executable_quote['quote_freshness_timestamp'], '2026-04-08T09:57:00+09:00')
        self.assertEqual(executable_quote['freshness']['quote_context']['source_provider'], 'kis')
        self.assertEqual(
            first_item['decision_input_quality_context']['execution_feasibility']['expected_slippage_basis'],
            'quote_spread_bps',
        )
        self.assertEqual(
            first_item['decision_input_quality_context']['data_quality']['blocks']['liquidity_turnover']['decision_effect'],
            'confirmed',
        )
        self.assertIn(
            'technical_evidence_available',
            first_item['decision_input_quality_context']['final_judgment']['positive_confirmations'],
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['decision_priority']['core_execution']['status'],
            'partial',
        )
        self.assertIn(
            'intraday_realtime',
            first_item['decider_data_quality_context']['decision_priority']['core_execution']['degraded_block_names'],
        )
        self.assertFalse(first_item['decider_data_quality_context']['decision_priority']['auxiliary_context']['blocking_by_default'])
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['intraday_realtime']['status'],
            'partial',
        )
        intraday_quality = first_item['decider_data_quality_context']['blocks']['intraday_realtime']['intraday_snapshot']
        self.assertTrue(intraday_quality['quote_available'])
        self.assertTrue(intraday_quality['orderbook_available'])
        self.assertTrue(intraday_quality['liquidity_available'])
        self.assertTrue(intraday_quality['investor_program_activity_available'])
        self.assertTrue(intraday_quality['news_hints_available'])
        self.assertEqual(first_item['intraday_execution_context']['status'], 'partial')
        self.assertEqual(first_item['intraday_execution_context']['quote']['spread_points'], 100)
        self.assertAlmostEqual(first_item['intraday_execution_context']['orderbook']['imbalance'], 0.076923, places=6)
        self.assertEqual(first_item['intraday_execution_context']['liquidity']['volume_power_rank'], 3)
        self.assertEqual(first_item['intraday_execution_context']['news_hints']['event_tags'], ['contract', 'earnings'])
        self.assertEqual(first_item['intraday_execution_context']['freshness_provenance']['freshness_status'], 'fresh')
        self.assertEqual(
            first_item['intraday_execution_context']['freshness_provenance']['trade_date_relation_to_request'],
            'same_market_date',
        )
        self.assertEqual(len(first_item['price_context_history']), 2)
        self.assertEqual(first_item['price_context_history'][0]['market_date'], '2026-04-07')
        self.assertEqual(first_item['price_context_history'][1]['market_date'], '2026-04-08')
        self.assertEqual(first_item['price_context_history'][1]['price_context']['trade_date'], '2026-04-08')
        self.assertEqual(first_item['technical_snapshot']['trend_state'], 'uptrend')
        self.assertEqual(first_item['action_summary']['action_label'], 'long_breakout')
        self.assertEqual(first_item['signal_features']['signal_count'], 2)
        self.assertEqual(first_item['signal_features']['provenance']['row_kind'], 'direct_plus_fallback')
        self.assertEqual(first_item['signal_features']['fallback_features']['price_action']['close_return_1d_pct'], 2.43)
        self.assertEqual(first_item['signal_features']['freshness_provenance']['provenance_status'], 'provider_plus_fallback')
        self.assertEqual(first_item['signal_features']['freshness_provenance']['issue_types'], ['derived_or_fallback'])
        self.assertEqual(first_item['scores']['unified_value']['status'], 'available')
        self.assertEqual(first_item['scores']['unified_value']['score'], 0.84)
        self.assertEqual(first_item['scores']['unified_value']['freshness_provenance']['freshness_status'], 'fresh')
        self.assertEqual(first_item['factor_summary']['factor_scores']['value']['status'], 'available')
        self.assertEqual(first_item['factor_summary']['factor_scores']['value']['source_status'], 'scored')
        self.assertAlmostEqual(first_item['factor_summary']['factor_scores']['value']['score'], 0.923333)
        self.assertEqual(
            first_item['factor_summary']['factor_scores']['value']['provenance']['freshness_status'],
            'fresh',
        )
        self.assertEqual(first_item['scores']['quality']['status'], 'partial')
        self.assertEqual(first_item['scores']['quality']['components_used'], 5)
        self.assertEqual(first_item['scores']['quality']['components_possible'], 6)
        self.assertIn('Missing quality components: accruals_ratio.', first_item['scores']['quality']['coverage_notes'])
        self.assertEqual(first_item['scores']['momentum']['status'], 'available')
        self.assertEqual(first_item['scores']['momentum']['score'], 0.91)
        self.assertEqual(first_item['scores']['momentum']['components_possible'], 3)
        self.assertEqual(first_item['factor_summary']['factor_scores']['advanced_value']['status'], 'available')
        self.assertEqual(first_item['factor_summary']['factor_scores']['low_risk']['status'], 'not_eligible')
        self.assertEqual(first_item['factor_summary']['factor_scores']['beta']['status'], 'available')
        self.assertEqual(first_item['factor_summary']['factor_scores']['composite']['status'], 'available')
        self.assertEqual(first_item['factor_summary']['signal_features']['status'], 'available')
        self.assertEqual(first_item['factor_summary']['signal_features']['row_kind'], 'direct_plus_fallback')
        self.assertEqual(first_item['persisted_factor_rows']['advanced_value']['advanced_value_classification'], 'eligible')
        self.assertEqual(
            first_item['persisted_factor_rows']['advanced_value']['freshness_provenance']['freshness_status'],
            'fresh',
        )
        self.assertEqual(first_item['persisted_factor_rows']['low_risk']['low_risk_classification'], 'watchlist')
        self.assertEqual(first_item['persisted_factor_rows']['beta']['low_beta_classification'], 'eligible')
        self.assertEqual(first_item['persisted_factor_rows']['composite']['composite_classification'], 'eligible')
        self.assertEqual(first_item['market_relative_context']['relative_strength']['return_percentile'], 0.95)
        self.assertEqual(first_item['context_completeness']['factor_rows']['quality']['row_status'], 'present')
        self.assertEqual(
            first_item['context_completeness']['factor_rows']['quality']['null_submetric_fields'],
            ['accruals_ratio'],
        )
        self.assertEqual(first_item['context_completeness']['signal_features']['row_status'], 'present')
        self.assertEqual(first_item['context_completeness']['signal_features']['row_kind'], 'direct_plus_fallback')
        self.assertEqual(first_item['context_completeness']['signal_features']['fallback_groups_present'], ['price_action', 'factor_scores'])
        self.assertEqual(first_item['context_completeness']['intraday_execution_context']['status'], 'partial')
        self.assertEqual(first_item['context_completeness']['intraday_execution_context']['available_blocks'], 4)
        self.assertEqual(
            first_item['context_completeness']['intraday_execution_context']['block_statuses']['investor_program_activity'],
            'partial',
        )
        self.assertEqual(first_item['context_completeness']['market_relative_context']['status'], 'available')
        self.assertEqual(first_item['context_completeness']['requested_symbol_coverage']['signal_features']['present_rows'], 1)
        self.assertEqual(
            first_item['context_completeness']['requested_symbol_coverage']['intraday_execution_context']['coverage_status'],
            'partial',
        )
        self.assertEqual(first_item['fundamentals']['latest_snapshot']['period']['period_end_date'], '2024-12-31')
        self.assertEqual(first_item['fundamentals']['latest_snapshot']['statement']['row_count'], 1)
        self.assertEqual(
            first_item['fundamentals']['latest_snapshot']['fundamental_context']['normalized_facts']['balance_sheet']['current_assets']['current_value'],
            240,
        )
        self.assertEqual(
            first_item['fundamentals']['latest_snapshot']['fundamental_context']['analysis']['leverage_liquidity']['metrics']['current_ratio'],
            '2.0000',
        )
        self.assertEqual(
            first_item['fundamentals']['latest_snapshot']['fundamental_context']['analysis']['cash_flow_quality']['metrics']['operating_cash_flow_to_net_income'],
            '1.6000',
        )
        self.assertEqual(
            first_item['fundamentals']['latest_snapshot']['fundamental_context']['source_provenance']['provider'],
            'dart',
        )
        self.assertEqual(first_item['fundamentals']['freshness_provenance']['freshness_status'], 'available_but_old')
        self.assertIn(
            'operating_cash_flow',
            first_item['decider_data_quality_context']['blocks']['financials_cashflow']['available_core_fields'],
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['financials_cashflow']['report_period_type'],
            'annual',
        )
        self.assertTrue(
            first_item['decider_data_quality_context']['blocks']['financials_cashflow']['annual_only_context']
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['financials_cashflow']['decision_guidance']['decision_tier'],
            'auxiliary_context',
        )
        self.assertEqual(first_item['recent_disclosures']['count'], 2)
        self.assertEqual(first_item['recent_disclosures']['freshness_provenance']['freshness_status'], 'available_but_old')
        self.assertEqual(first_item['recent_disclosures']['items'][0]['is_correction'], True)
        self.assertEqual(first_item['recent_disclosures']['items'][1]['is_large_holder_related'], True)
        self.assertEqual(
            first_item['fundamentals']['disclosure_context']['summary']['correction_count'],
            1,
        )
        self.assertEqual(
            first_item['fundamentals']['disclosure_context']['summary']['ownership_related_count'],
            1,
        )
        self.assertEqual(
            first_item['fundamentals']['disclosure_context']['recent_ownership_large_holder_changes'][0]['rcept_no'],
            '20260406000055',
        )
        self.assertEqual(
            first_item['context_provenance']['signal_features']['requested_symbol_coverage']['missing_rows'],
            1,
        )
        self.assertEqual(first_item['context_provenance']['intraday_execution_context']['freshness_status'], 'fresh')
        self.assertEqual(
            first_item['context_provenance']['intraday_execution_context']['requested_symbol_coverage']['partial_rows'],
            1,
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['technical_evidence']['support_price'],
            68000.0,
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['technical_evidence']['resistance_price'],
            70800.0,
        )
        self.assertEqual(
            first_item['decider_data_quality_context']['blocks']['factor_score_coverage']['required_scores']['quality']['status'],
            'available',
        )
        self.assertTrue(first_item['availability']['quote_context'])
        self.assertTrue(first_item['availability']['price_context'])
        self.assertTrue(first_item['availability']['intraday_execution_context'])

        second_item = result['items'][1]
        self.assertEqual(second_item['symbol'], '000660')
        self.assertEqual(second_item['quote_context']['status'], 'unavailable')
        self.assertEqual(second_item['quote_context']['reason'], 'no_current_quote_cache_row')
        self.assertEqual(second_item['quote_context']['freshness']['status'], 'missing')
        self.assertEqual(second_item['quote_context']['spread']['status'], 'unavailable')
        self.assertIsNone(second_item['price_context'])
        self.assertEqual(second_item['intraday_execution_context']['status'], 'unavailable')
        self.assertEqual(
            second_item['intraday_execution_context']['reason'],
            'no_current_kis_intraday_snapshot_cache_row',
        )
        self.assertEqual(second_item['intraday_execution_context']['quote']['status'], 'unavailable')
        self.assertEqual(second_item['intraday_execution_context']['orderbook']['status'], 'unavailable')
        self.assertEqual(second_item['intraday_execution_context']['liquidity']['status'], 'unavailable')
        self.assertEqual(
            second_item['intraday_execution_context']['investor_program_activity']['status'],
            'unavailable',
        )
        self.assertEqual(second_item['intraday_execution_context']['news_hints']['status'], 'unavailable')
        self.assertEqual(second_item['price_context_history'], [])
        self.assertIsNone(second_item['technical_snapshot'])
        self.assertIsNone(second_item['signal_features'])
        self.assertEqual(second_item['market_relative_context']['status'], 'unavailable')
        self.assertEqual(second_item['sector_peer_context']['status'], 'unavailable')
        self.assertEqual(second_item['market_breadth_context']['status'], 'unavailable')
        self.assertEqual(second_item['investor_flow_context']['status'], 'unavailable')
        self.assertEqual(second_item['short_selling_context']['status'], 'unavailable')
        self.assertEqual(second_item['valuation_context']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['unified_value']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['quality']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['momentum']['status'], 'unavailable')
        self.assertEqual(second_item['scores']['unified_value']['freshness_provenance']['freshness_status'], 'missing')
        self.assertEqual(second_item['factor_summary']['factor_scores']['value']['status'], 'missing_source_row')
        self.assertEqual(second_item['factor_summary']['factor_scores']['unified_value']['status'], 'missing_source_row')
        self.assertEqual(second_item['factor_summary']['signal_features']['status'], 'missing_source_row')
        self.assertEqual(second_item['context_completeness']['signal_features']['row_status'], 'missing')
        self.assertEqual(second_item['context_completeness']['signal_features']['row_kind'], 'missing')
        self.assertEqual(second_item['context_completeness']['intraday_execution_context']['status'], 'unavailable')
        self.assertEqual(second_item['context_completeness']['market_relative_context']['status'], 'unavailable')
        self.assertEqual(second_item['context_completeness']['sector_peer_mapping']['status'], 'unavailable')
        self.assertIsNone(second_item['fundamentals']['latest_snapshot'])
        self.assertEqual(second_item['context_provenance']['signal_features']['freshness_status'], 'missing')
        self.assertEqual(second_item['context_provenance']['intraday_execution_context']['freshness_status'], 'missing')
        self.assertEqual(second_item['context_provenance']['market_relative_context']['freshness_status'], 'missing')
        self.assertEqual(second_item['recent_disclosures']['count'], 0)
        self.assertEqual(
            second_item['decider_data_quality_context']['blocks']['intraday_realtime']['status'],
            'unavailable',
        )
        missing_intraday_quality = second_item['decider_data_quality_context']['blocks']['intraday_realtime']['intraday_snapshot']
        self.assertFalse(missing_intraday_quality['quote_available'])
        self.assertFalse(missing_intraday_quality['orderbook_available'])
        self.assertFalse(missing_intraday_quality['liquidity_available'])
        self.assertFalse(missing_intraday_quality['investor_program_activity_available'])
        self.assertFalse(missing_intraday_quality['news_hints_available'])
        self.assertEqual(
            second_item['decider_data_quality_context']['blocks']['factor_score_coverage']['status'],
            'unavailable',
        )
        self.assertFalse(second_item['availability']['quote_context'])
        self.assertFalse(second_item['availability']['price_context'])
        self.assertFalse(second_item['availability']['intraday_execution_context'])
        self.assertFalse(second_item['availability']['market_relative_context'])
        self.assertFalse(second_item['availability']['sector_peer_context'])
        self.assertFalse(second_item['availability']['market_breadth_context'])
        self.assertFalse(second_item['availability']['investor_flow_context'])
        self.assertFalse(second_item['availability']['short_selling_context'])
        self.assertFalse(second_item['availability']['valuation_context'])

    def test_get_symbol_day_context_requires_at_least_one_symbol(self):
        service = AgentContextReadService(
            fundamentals_repository=FakeFundamentalsRepository(),
            disclosure_repository=FakeDisclosureRepository(),
        )

        with self.assertRaisesRegex(ValueError, 'At least one symbol is required'):
            service.get_symbol_day_context(market_date='2026-04-08', symbols=[])

    def test_build_price_context_marks_gap_fill_and_flat_candle_conservatively(self):
        context = _build_price_context(
            [
                {
                    'trade_date': '2026-04-08',
                    'open_price': 100,
                    'high_price': 108,
                    'low_price': 95,
                    'close_price': 100,
                    'volume': 10,
                    'turnover': 1000,
                    'source': 'pykrx',
                    'captured_at': '2026-04-08T15:30:00+09:00',
                },
                {
                    'trade_date': '2026-04-07',
                    'open_price': 95,
                    'high_price': 99,
                    'low_price': 94,
                    'close_price': 96,
                    'volume': 12,
                    'turnover': 1200,
                    'source': 'pykrx',
                    'captured_at': '2026-04-07T15:30:00+09:00',
                },
            ]
        )

        self.assertEqual(context['candle']['direction'], 'flat')
        self.assertEqual(context['candle']['body_points'], 0)
        self.assertEqual(context['gap']['direction'], 'up')
        self.assertTrue(context['gap']['filled_intraday'])
        self.assertEqual(context['session_range']['close_position_label'], 'lower_half')

    def test_build_compact_liquidity_context_marks_derived_turnover_from_close_and_volume(self):
        context = _build_compact_liquidity_context(
            requested_market_date=date.fromisoformat('2026-04-08'),
            quote_context=None,
            quote_freshness_provenance={},
            price_context={
                'trade_date': '2026-04-08',
                'source': 'pykrx',
                'captured_at': '2026-04-08T15:30:00+09:00',
                'close_price': 71000,
                'volume': 120000,
                'turnover': 0,
                'liquidity': {
                    'turnover_krw': 8520000000,
                    'turnover_source': 'estimated',
                    'turnover_is_estimated': True,
                    'provider_turnover_krw': None,
                    'estimated_turnover_krw': 8520000000,
                },
                'price_bar_quality': {
                    'issue_codes': ['turnover_missing_or_zero'],
                },
            },
            price_context_history=[],
            price_context_freshness_provenance={
                'freshness_status': 'fresh',
                'provenance_status': 'provider_plus_fallback',
                'source_tables': ['daily_price_contexts', 'daily_price_bars'],
            },
            price_context_storage_unavailable=False,
            low_risk_row=None,
            latest_prior_low_risk_row=None,
            low_risk_freshness_provenance={},
            low_risk_storage_unavailable=False,
            low_risk_prior_storage_unavailable=False,
            intraday_execution_context=None,
            intraday_execution_freshness_provenance={},
        )

        self.assertEqual(context['status'], 'derived')
        self.assertEqual(context['turnover']['status'], 'derived')
        self.assertEqual(context['turnover']['turnover_krw'], 8520000000)
        self.assertEqual(context['turnover']['turnover_source'], 'derived')
        self.assertEqual(context['turnover']['trading_value_source'], 'estimated')
        self.assertEqual(context['turnover']['estimated_trading_value_krw'], 8520000000)
        self.assertEqual(context['turnover']['reason'], 'derived_from_close_price_x_volume')

    def test_build_compact_liquidity_context_rejects_negative_provider_turnover(self):
        context = _build_compact_liquidity_context(
            requested_market_date=date.fromisoformat('2026-04-08'),
            quote_context=None,
            quote_freshness_provenance={},
            price_context={
                'trade_date': '2026-04-08',
                'source': 'pykrx',
                'captured_at': '2026-04-08T15:30:00+09:00',
                'close_price': 71000,
                'volume': 120000,
                'turnover': -8520000000,
                'liquidity': {
                    'turnover_krw': -8520000000,
                    'provider_turnover_krw': -8520000000,
                    'estimated_turnover_krw': 8520000000,
                },
            },
            price_context_history=[],
            price_context_freshness_provenance=_fresh_provenance(),
            price_context_storage_unavailable=False,
            low_risk_row=None,
            latest_prior_low_risk_row=None,
            low_risk_freshness_provenance={},
            low_risk_storage_unavailable=False,
            low_risk_prior_storage_unavailable=False,
            intraday_execution_context=None,
            intraday_execution_freshness_provenance={},
        )

        self.assertEqual(context['status'], 'missing_price_or_volume')
        self.assertEqual(context['turnover']['reason'], 'nonpositive_turnover_value')
        self.assertEqual(context['turnover']['trading_value_source'], 'missing')
        self.assertIsNone(context['turnover']['turnover_krw'])
        self.assertIsNone(context['turnover']['raw_trading_value_krw'])
        self.assertIsNone(context['turnover']['estimated_trading_value_krw'])

    def test_build_compact_liquidity_context_does_not_hide_negative_valuation_turnover(self):
        context = _build_compact_liquidity_context(
            requested_market_date=date.fromisoformat('2026-04-08'),
            quote_context=None,
            quote_freshness_provenance={},
            price_context={
                'trade_date': '2026-04-08',
                'source': 'pykrx',
                'captured_at': '2026-04-08T15:30:00+09:00',
                'close_price': 71000,
                'volume': 120000,
                'turnover': 8520000000,
                'liquidity': {'provider_turnover_krw': 8520000000},
            },
            price_context_history=[],
            price_context_freshness_provenance=_fresh_provenance(),
            price_context_storage_unavailable=False,
            low_risk_row=None,
            latest_prior_low_risk_row=None,
            low_risk_freshness_provenance={},
            low_risk_storage_unavailable=False,
            low_risk_prior_storage_unavailable=False,
            intraday_execution_context=None,
            intraday_execution_freshness_provenance={},
            valuation_context={
                'status': 'available',
                'valuation_snapshot': {'trading_value': -9870000000},
            },
            valuation_context_freshness_provenance=_fresh_provenance(),
        )

        self.assertEqual(context['status'], 'missing_price_or_volume')
        self.assertEqual(context['turnover']['reason'], 'nonpositive_turnover_value')
        self.assertEqual(context['turnover']['trading_value_source'], 'missing')
        self.assertIsNone(context['turnover']['turnover_krw'])

    def test_build_compact_liquidity_context_prefers_raw_pykrx_trading_value(self):
        context = _build_compact_liquidity_context(
            requested_market_date=date.fromisoformat('2026-04-08'),
            quote_context=None,
            quote_freshness_provenance={},
            price_context={
                'trade_date': '2026-04-08',
                'source': 'pykrx',
                'captured_at': '2026-04-08T15:30:00+09:00',
                'close_price': 71000,
                'volume': 120000,
                'turnover': 0,
                'liquidity': {
                    'provider_turnover_krw': None,
                    'estimated_turnover_krw': 8520000000,
                },
                'price_bar_quality': {
                    'issue_codes': ['turnover_missing_or_zero'],
                },
            },
            price_context_history=[],
            price_context_freshness_provenance={
                'freshness_status': 'fresh',
                'provenance_status': 'provider_plus_fallback',
                'source_tables': ['daily_price_contexts', 'daily_price_bars'],
            },
            price_context_storage_unavailable=False,
            low_risk_row=None,
            latest_prior_low_risk_row=None,
            low_risk_freshness_provenance={},
            low_risk_storage_unavailable=False,
            low_risk_prior_storage_unavailable=False,
            intraday_execution_context=None,
            intraday_execution_freshness_provenance={},
            valuation_context={
                'status': 'available',
                'valuation_snapshot': {
                    'trading_value': 9870000000,
                    'close_price': 71000,
                    'volume': 120000,
                },
            },
            valuation_context_freshness_provenance={
                'freshness_status': 'fresh',
                'provenance_status': 'provider_sourced',
                'as_of_market_date': '2026-04-08',
                'latest_available_timestamp': '2026-04-08T15:30:00+09:00',
                'source_tables': ['daily_pykrx_enrichment_rows'],
            },
        )

        self.assertEqual(context['status'], 'available')
        self.assertEqual(context['turnover']['turnover_krw'], 9870000000)
        self.assertEqual(context['turnover']['trading_value_source'], 'raw')
        self.assertEqual(context['turnover']['raw_trading_value_krw'], 9870000000)
        self.assertIsNone(context['turnover']['derived_turnover_krw'])
        self.assertEqual(context['turnover']['trading_value_source_detail'], 'pykrx_valuation_snapshot.trading_value')

    def test_build_compact_liquidity_context_marks_missing_price_or_volume_when_turnover_not_derivable(self):
        context = _build_compact_liquidity_context(
            requested_market_date=date.fromisoformat('2026-04-08'),
            quote_context=None,
            quote_freshness_provenance={},
            price_context={
                'trade_date': '2026-04-08',
                'source': 'pykrx',
                'captured_at': '2026-04-08T15:30:00+09:00',
                'close_price': 71000,
                'volume': 0,
                'turnover': 0,
                'liquidity': {
                    'turnover_source': 'missing',
                    'turnover_is_estimated': False,
                    'provider_turnover_krw': None,
                    'estimated_turnover_krw': None,
                },
                'price_bar_quality': {
                    'issue_codes': ['turnover_missing_or_zero', 'volume_zero'],
                },
            },
            price_context_history=[],
            price_context_freshness_provenance={
                'freshness_status': 'fresh',
                'provenance_status': 'derived_from_provider_data',
                'source_tables': ['daily_price_contexts', 'daily_price_bars'],
            },
            price_context_storage_unavailable=False,
            low_risk_row=None,
            latest_prior_low_risk_row=None,
            low_risk_freshness_provenance={},
            low_risk_storage_unavailable=False,
            low_risk_prior_storage_unavailable=False,
            intraday_execution_context=None,
            intraday_execution_freshness_provenance={},
        )

        self.assertEqual(context['status'], 'missing_price_or_volume')
        self.assertEqual(context['turnover']['status'], 'missing_price_or_volume')
        self.assertEqual(context['turnover']['trading_value_source'], 'missing')
        self.assertEqual(context['turnover']['reason'], 'volume_missing_or_zero')

    def test_build_pre_decider_enrichment_marks_closed_market_quote_snapshot_stale(self):
        market_date = date.fromisoformat('2026-04-11')
        market_session = _closed_market_session()
        quote_context = _build_quote_context(None, market_date=market_date, market_session=market_session)
        intraday_context = _build_intraday_execution_context(None, market_session=market_session)

        context = _build_pre_decider_enrichment_context(
            requested_market_date=market_date,
            market_session=market_session,
            quote_context=quote_context,
            quote_freshness_provenance={'freshness_status': 'not_applicable'},
            intraday_execution_context=intraday_context,
            intraday_execution_freshness_provenance={'freshness_status': 'not_applicable'},
            price_context={
                'trade_date': '2026-04-10',
                'close_price': 71000,
                'captured_at': '2026-04-10T15:30:00+09:00',
            },
            liquidity_context={
                'turnover': {
                    'status': 'available',
                    'trading_value_source': 'raw',
                    'trading_value_krw': 8200000000,
                    'raw_trading_value_krw': 8200000000,
                    'trade_date': '2026-04-10',
                    'freshness_status': 'fresh',
                    'provenance': {'freshness_status': 'fresh'},
                }
            },
            investor_flow_context={'status': 'unavailable'},
            investor_flow_provenance={},
            short_selling_context={'status': 'unavailable'},
            short_selling_provenance={},
            sector_peer_context={'status': 'unavailable'},
            sector_peer_provenance={},
            market_breadth_context={'status': 'unavailable'},
            market_breadth_provenance={},
        )

        snapshot = context['quote_orderbook_snapshot']
        phase_contract = context['market_data_phase_contract']
        self.assertEqual(snapshot['status'], 'stale_eod')
        self.assertEqual(snapshot['snapshot_type'], 'eod_stale')
        self.assertEqual(snapshot['current_price'], 71000)
        self.assertEqual(snapshot['quote_freshness_timestamp'], '2026-04-10T15:30:00+09:00')
        self.assertEqual(snapshot['freshness']['quote_context']['freshness_status'], 'not_applicable')
        self.assertEqual(phase_contract['recommendation_basis'], 'latest_completed_eod')
        self.assertFalse(phase_contract['realtime_quote_orderbook_required'])
        self.assertTrue(phase_contract['eod_reference_price_accepted'])
        self.assertTrue(phase_contract['agent_guidance']['daily_process_agents_use_completed_eod'])

    def test_pre_decider_spread_metadata_uses_one_source_provenance_bundle(self):
        market_date = date(2026, 4, 8)
        quote_observed_at = '2026-04-08T09:57:00+09:00'
        snapshot = _build_pre_decider_quote_orderbook_snapshot(
            requested_market_date=market_date,
            market_session={},
            quote_context={
                'status': 'available',
                'source': 'kis',
                'captured_at': quote_observed_at,
                'price': 50_000,
                'spread': {
                    'bid_price': 49_950,
                    'ask_price': 50_050,
                    'spread_points': 100,
                    'spread_bps_of_price': 20,
                },
            },
            quote_freshness_provenance={
                'freshness_status': 'fresh',
                'as_of_market_date': market_date.isoformat(),
                'as_of_timestamp': quote_observed_at,
            },
            intraday_execution_context={
                'status': 'available',
                'as_of_market_date': '2026-04-07',
                'as_of_timestamp': '2026-04-07T15:30:00+09:00',
            },
            intraday_execution_freshness_provenance={
                'freshness_status': 'stale',
                'as_of_market_date': '2026-04-07',
                'as_of_timestamp': '2026-04-07T15:30:00+09:00',
            },
            price_context=None,
        )

        self.assertEqual(snapshot['spread_source_context'], 'quote_context')
        self.assertEqual(snapshot['quote_freshness_status'], 'fresh')
        self.assertEqual(snapshot['quote_freshness_timestamp'], quote_observed_at)
        self.assertEqual(snapshot['as_of_market_date'], market_date.isoformat())
        self.assertEqual(snapshot['as_of_timestamp'], quote_observed_at)
        self.assertEqual(
            snapshot['spread_component_source_context'],
            {
                'current_price': 'quote_context',
                'bid_price': 'quote_context',
                'ask_price': 'quote_context',
                'spread_points': 'quote_context',
                'spread_bps': 'quote_context',
            },
        )

    def test_pre_decider_spread_prefers_fresh_intraday_over_coherent_stale_quote(self):
        market_date = date(2026, 4, 8)
        stale_quote_at = '2026-04-07T15:30:00+09:00'
        fresh_intraday_at = '2026-04-08T09:57:01+09:00'

        snapshot = _build_pre_decider_quote_orderbook_snapshot(
            requested_market_date=market_date,
            market_session={},
            quote_context={
                'status': 'available',
                'source': 'kis',
                'captured_at': stale_quote_at,
                'price': 50_000,
                'spread': {
                    'bid_price': 49_950,
                    'ask_price': 50_050,
                    'spread_points': 100,
                    'spread_bps_of_price': 20,
                },
            },
            quote_freshness_provenance={
                'freshness_status': 'stale',
                'provenance_status': 'provider_sourced',
                'as_of_market_date': '2026-04-07',
                'as_of_timestamp': stale_quote_at,
                'source_provider': 'kis',
                'source_tables': ['current_quote_cache', 'quote_snapshots'],
            },
            intraday_execution_context={
                'status': 'available',
                'source': 'kis_intraday_snapshot_v1',
                'as_of_market_date': market_date.isoformat(),
                'as_of_timestamp': fresh_intraday_at,
                'quote': {
                    'price': 51_000,
                    'bid_price': 50_949,
                    'ask_price': 51_051,
                    'spread_points': 102,
                    'spread_bps_of_price': 20,
                },
            },
            intraday_execution_freshness_provenance={
                'freshness_status': 'fresh',
                'provenance_status': 'provider_sourced',
                'as_of_market_date': market_date.isoformat(),
                'as_of_timestamp': fresh_intraday_at,
                'source_provider': 'kis_intraday_snapshot_v1',
                'source_tables': [
                    'current_kis_intraday_snapshot_cache',
                    'kis_intraday_snapshots',
                ],
            },
            price_context=None,
        )

        self.assertEqual(
            snapshot['spread_source_context'],
            'intraday_execution_context',
        )
        self.assertEqual(snapshot['current_price'], 51_000)
        self.assertEqual(snapshot['spread_bps'], 20)
        self.assertEqual(snapshot['quote_freshness_status'], 'fresh')
        self.assertEqual(snapshot['quote_freshness_timestamp'], fresh_intraday_at)

    def test_pre_decider_spread_metadata_never_mixes_quote_and_intraday_components(self):
        market_date = date(2026, 4, 8)
        quote_observed_at = '2026-04-08T09:57:00+09:00'
        intraday_observed_at = '2026-04-08T09:57:01+09:00'

        snapshot = _build_pre_decider_quote_orderbook_snapshot(
            requested_market_date=market_date,
            market_session={},
            quote_context={
                'status': 'available',
                'source': 'kis',
                'captured_at': quote_observed_at,
                'price': 50_000,
                'spread': {'spread_bps_of_price': 20},
            },
            quote_freshness_provenance={
                'freshness_status': 'fresh',
                'as_of_market_date': market_date.isoformat(),
                'as_of_timestamp': quote_observed_at,
            },
            intraday_execution_context={
                'status': 'available',
                'source': 'kis_intraday_snapshot_v1',
                'as_of_market_date': market_date.isoformat(),
                'as_of_timestamp': intraday_observed_at,
                'quote': {
                    'price': 51_000,
                    'bid_price': 50_950,
                    'ask_price': 51_050,
                    'spread_points': 100,
                },
            },
            intraday_execution_freshness_provenance={
                'freshness_status': 'fresh',
                'as_of_market_date': market_date.isoformat(),
                'as_of_timestamp': intraday_observed_at,
            },
            price_context=None,
        )

        self.assertEqual(snapshot['spread_source_context'], 'intraday_execution_context')
        self.assertEqual(snapshot['current_price'], 51_000)
        self.assertEqual(snapshot['bid_price'], 50_950)
        self.assertEqual(snapshot['ask_price'], 51_050)
        self.assertEqual(snapshot['spread_points'], 100)
        self.assertEqual(snapshot['spread_bps'], 19.6078)
        self.assertEqual(
            set(snapshot['spread_component_source_context'].values()),
            {'intraday_execution_context'},
        )

    def test_get_symbol_day_context_marks_liquidity_context_storage_unavailable_when_price_context_store_fails(self):
        class EmptyFundamentalsRepository:
            def list_stored_fundamentals_summary(
                self,
                *,
                stock_codes,
                fs_div=None,
                captured_before=None,
                receipt_date_to=None,
            ):
                return []

        class EmptyDisclosureRepository:
            def list_recent_events_by_stock_codes(
                self,
                *,
                stock_codes,
                receipt_date_to=None,
                observed_before=None,
                limit_per_symbol=5,
            ):
                return {symbol: [] for symbol in stock_codes}

        service = AgentContextReadService(
            fundamentals_repository=EmptyFundamentalsRepository(),
            disclosure_repository=EmptyDisclosureRepository(),
        )

        with (
            patch(
                'app.agent_context.read_service.load_symbols',
                return_value=[
                    {
                        'id': 1,
                        'symbol': '005930',
                        'standard_code': 'KR7005930003',
                        'name_ko': '삼성전자',
                        'market': 'KOSPI',
                        'instrument_type': 'equity',
                        'source': 'pykrx',
                        'is_active': True,
                        'last_refreshed_at': None,
                    }
                ],
            ),
            patch('app.agent_context.read_service.load_daily_technical_snapshots', return_value=[]),
            patch('app.agent_context.read_service.load_daily_action_summaries', return_value=[]),
            patch('app.agent_context.read_service.load_daily_signal_features', return_value=[]),
            patch('app.agent_context.read_service.load_daily_value_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_unified_value_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_quality_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_momentum_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_advanced_value_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_low_risk_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_beta_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_composite_factor_results', return_value=[]),
            patch(
                'app.agent_context.read_service.load_current_quotes',
                return_value=[
                    {
                        'symbol': '005930',
                        'source': 'kis',
                        'market_state': 'OPEN',
                        'is_live': True,
                        'fallback': False,
                        'available': True,
                        'price': 71550,
                        'change': 50,
                        'change_pct': 0.07,
                        'volume': 150000,
                        'turnover': 9000000000,
                        'trade_date': '2026-04-08',
                        'cached_at': '2026-04-08T09:57:00+09:00',
                        'captured_at': '2026-04-08T09:57:00+09:00',
                    }
                ],
            ),
            patch('app.agent_context.read_service.load_current_kis_intraday_snapshots', return_value=[]),
            patch(
                'app.agent_context.read_service.load_daily_price_contexts',
                side_effect=SQLAlchemyError('daily price contexts unavailable'),
            ),
            patch('app.agent_context.read_service.load_daily_price_context_history', return_value=[]),
            patch('app.agent_context.read_service.load_daily_market_relative_contexts', return_value=[]),
            patch(
                'app.agent_context.read_service._load_latest_symbol_day_block_history',
                return_value=EMPTY_LATEST_PRIOR_ROWS,
            ),
        ):
            result = service.get_symbol_day_context(
                market_date='2026-04-08',
                symbols=['005930'],
                fs_div='CFS',
                disclosures_limit=3,
            )

        item = result['items'][0]
        self.assertEqual(item['liquidity_context']['status'], 'storage_unavailable')
        self.assertEqual(item['liquidity_context']['turnover']['status'], 'storage_unavailable')
        self.assertEqual(item['liquidity_context']['turnover']['reason'], 'daily_price_context_storage_unavailable')
        self.assertEqual(item['context_provenance']['price_context']['reason'], 'daily_price_context_storage_unavailable')

    def test_get_symbol_day_context_marks_stale_and_available_but_old_blocks_without_silently_omitting_them(self):
        latest_prior_rows = {
            'factor_rows': {
                family: (
                    {
                        '005930': {
                            'market_date': '2026-04-07',
                            'captured_at': '2026-04-07T15:00:00+09:00',
                            'source': 'unified_value_v1',
                        }
                    }
                    if family == 'unified_value'
                    else {
                        '005930': {
                            'market_date': '2026-04-07',
                            'captured_at': '2026-04-07T15:00:00+09:00',
                            'source': 'low_risk_v1',
                            'avg_trading_value_20d': 6400000000.0,
                            'liquidity_rank': 17,
                            'liquidity_percentile': 0.82,
                            'passes_liquidity_floor': True,
                        }
                    }
                    if family == 'low_risk'
                    else {}
                )
                for family in PERSISTED_FACTOR_FAMILIES
            },
            'signal_features': {
                '005930': {
                    'market_date': '2026-04-07',
                    'captured_at': '2026-04-07T15:00:00+09:00',
                    'source': 'kis_ranking',
                }
            },
        }

        class EmptyFundamentalsRepository:
            def list_stored_fundamentals_summary(
                self,
                *,
                stock_codes,
                fs_div=None,
                captured_before=None,
                receipt_date_to=None,
            ):
                return []

        class EmptyDisclosureRepository:
            def list_recent_events_by_stock_codes(
                self,
                *,
                stock_codes,
                receipt_date_to=None,
                observed_before=None,
                limit_per_symbol=5,
            ):
                return {symbol: [] for symbol in stock_codes}

        service = AgentContextReadService(
            fundamentals_repository=EmptyFundamentalsRepository(),
            disclosure_repository=EmptyDisclosureRepository(),
        )

        with (
            patch(
                'app.agent_context.read_service.load_symbols',
                return_value=[
                    {
                        'id': 1,
                        'symbol': '005930',
                        'standard_code': 'KR7005930003',
                        'name_ko': '삼성전자',
                        'market': 'KOSPI',
                        'instrument_type': 'equity',
                        'source': 'pykrx',
                        'is_active': True,
                        'last_refreshed_at': None,
                    }
                ],
            ),
            patch('app.agent_context.read_service.load_daily_technical_snapshots', return_value=[]),
            patch('app.agent_context.read_service.load_daily_action_summaries', return_value=[]),
            patch('app.agent_context.read_service.load_daily_signal_features', return_value=[]),
            patch('app.agent_context.read_service.load_daily_value_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_unified_value_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_quality_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_momentum_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_advanced_value_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_low_risk_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_beta_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_composite_factor_results', return_value=[]),
            patch(
                'app.agent_context.read_service.load_current_quotes',
                return_value=[
                    {
                        'symbol': '005930',
                        'source': 'kis',
                        'market_state': 'CLOSED',
                        'is_live': False,
                        'fallback': False,
                        'available': True,
                        'price': 70100,
                        'change': -200,
                        'change_pct': -0.28,
                        'volume': 99000,
                        'turnover': 6400000000,
                        'as_of': '153000',
                        'trade_date': '2026-04-07',
                        'cached_at': '2026-04-07T15:30:00+09:00',
                        'captured_at': '2026-04-07T15:30:00+09:00',
                    }
                ],
            ),
            patch(
                'app.agent_context.read_service.load_current_kis_intraday_snapshots',
                return_value=[
                    _make_intraday_execution_row(
                        availability_status='available',
                        as_of_market_date='2026-04-07',
                        as_of_timestamp='2026-04-07T15:28:00+09:00',
                        captured_at='2026-04-07T15:28:05+09:00',
                        market_state='CLOSED',
                    )
                ],
            ),
            patch('app.agent_context.read_service.load_daily_price_contexts', return_value=[]),
            patch(
                'app.agent_context.read_service.load_daily_price_context_history',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-07',
                        'source': 'pykrx',
                        'context_payload': {
                            'trade_date': '2026-04-07',
                            'source': 'pykrx',
                            'captured_at': '2026-04-07T15:30:00+09:00',
                            'close_price': 70100,
                        },
                        'captured_at': '2026-04-07T15:30:00+09:00',
                    }
                ],
            ),
            patch('app.agent_context.read_service.load_daily_market_relative_contexts', return_value=[]),
            patch(
                'app.agent_context.read_service._load_latest_symbol_day_block_history',
                return_value=latest_prior_rows,
            ),
        ):
            result = service.get_symbol_day_context(
                market_date='2026-04-08',
                symbols=['005930'],
                fs_div='CFS',
                disclosures_limit=3,
            )

        item = result['items'][0]
        self.assertEqual(item['quote_context']['freshness_provenance']['freshness_status'], 'not_applicable')
        self.assertEqual(item['quote_context']['freshness_provenance']['issue_types'], [])
        self.assertEqual(item['quote_context']['freshness_provenance']['reason'], 'closed_market')
        self.assertIsNone(item['price_context'])
        self.assertEqual(item['context_provenance']['price_context']['freshness_status'], 'stale')
        self.assertEqual(item['context_provenance']['price_context']['latest_available_market_date'], '2026-04-07')
        self.assertEqual(item['liquidity_context']['status'], 'stale')
        self.assertEqual(item['liquidity_context']['turnover']['trade_date'], '2026-04-07')
        self.assertEqual(
            item['liquidity_context']['turnover']['reason'],
            'latest_price_context_before_requested_market_date',
        )
        self.assertEqual(item['liquidity_context']['ranking']['status'], 'stale')
        self.assertEqual(
            item['liquidity_context']['ranking']['reason'],
            'latest_prior_low_risk_liquidity_row_available',
        )
        self.assertEqual(item['liquidity_context']['ranking']['as_of_market_date'], '2026-04-07')
        self.assertEqual(item['liquidity_context']['ranking']['avg_trading_value_20d'], 6400000000.0)
        self.assertEqual(item['liquidity_context']['ranking']['liquidity_rank'], 17)
        self.assertEqual(item['liquidity_context']['ranking']['liquidity_percentile'], 0.82)
        self.assertTrue(item['liquidity_context']['ranking']['passes_liquidity_floor'])
        self.assertEqual(item['decider_data_quality_context']['blocks']['liquidity_turnover']['status'], 'stale')
        self.assertEqual(
            item['decider_data_quality_context']['blocks']['intraday_realtime']['status'],
            'not_applicable',
        )
        self.assertEqual(item['decision_input_quality_context']['final_judgment']['decision_band'], 'block')
        self.assertIn(
            'liquidity_turnover',
            item['decision_input_quality_context']['final_judgment']['missing_context'],
        )
        self.assertIsNone(item['signal_features'])
        self.assertEqual(item['context_provenance']['signal_features']['freshness_status'], 'stale')
        self.assertEqual(item['context_provenance']['signal_features']['latest_available_market_date'], '2026-04-07')
        self.assertEqual(item['intraday_execution_context']['status'], 'not_applicable')
        self.assertEqual(
            item['context_provenance']['intraday_execution_context']['freshness_status'],
            'not_applicable',
        )
        self.assertEqual(
            item['context_provenance']['intraday_execution_context']['reason'],
            'closed_market',
        )
        self.assertIsNone(item['persisted_factor_rows']['unified_value'])
        self.assertEqual(item['context_provenance']['factor_rows']['unified_value']['freshness_status'], 'available_but_old')
        self.assertEqual(item['scores']['unified_value']['freshness_provenance']['freshness_status'], 'available_but_old')
        self.assertEqual(item['factor_summary']['factor_scores']['value']['status'], 'missing_source_row')
        self.assertEqual(item['factor_summary']['factor_scores']['unified_value']['status'], 'not_computed_for_market_date')
        self.assertEqual(item['factor_summary']['signal_features']['status'], 'not_computed_for_market_date')

    def test_get_symbol_day_context_factor_summary_distinguishes_storage_missing_non_eligible_and_history_states(self):
        class EmptyFundamentalsRepository:
            def list_stored_fundamentals_summary(
                self,
                *,
                stock_codes,
                fs_div=None,
                captured_before=None,
                receipt_date_to=None,
            ):
                return []

        class EmptyDisclosureRepository:
            def list_recent_events_by_stock_codes(
                self,
                *,
                stock_codes,
                receipt_date_to=None,
                observed_before=None,
                limit_per_symbol=5,
            ):
                return {symbol: [] for symbol in stock_codes}

        service = AgentContextReadService(
            fundamentals_repository=EmptyFundamentalsRepository(),
            disclosure_repository=EmptyDisclosureRepository(),
        )
        latest_prior_rows = {
            'factor_rows': {family: {} for family in PERSISTED_FACTOR_FAMILIES},
            'factor_storage_unavailable': {family: False for family in PERSISTED_FACTOR_FAMILIES},
            'signal_features': {},
            'signal_features_storage_unavailable': False,
        }

        with (
            patch(
                'app.agent_context.read_service.load_symbols',
                return_value=[
                    {
                        'id': 1,
                        'symbol': '005930',
                        'standard_code': 'KR7005930003',
                        'name_ko': '삼성전자',
                        'market': 'KOSPI',
                        'instrument_type': 'equity',
                        'source': 'pykrx',
                        'is_active': True,
                        'last_refreshed_at': None,
                    }
                ],
            ),
            patch('app.agent_context.read_service.load_daily_technical_snapshots', return_value=[]),
            patch('app.agent_context.read_service.load_daily_action_summaries', return_value=[]),
            patch('app.agent_context.read_service.load_daily_signal_features', return_value=[]),
            patch(
                'app.agent_context.read_service.load_daily_value_factor_results',
                side_effect=SQLAlchemyError('value table unavailable'),
            ) as load_value_factor_results_mock,
            patch('app.agent_context.read_service.load_daily_unified_value_factor_results', return_value=[]),
            patch('app.agent_context.read_service.load_daily_quality_factor_results', return_value=[]),
            patch(
                'app.agent_context.read_service.load_daily_momentum_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-08',
                        'source': 'daily_price_bars',
                        'feature_version': 'price_momentum_v1',
                        'close_price': 71500,
                        'ma_60': 68500.0,
                        'ma_120': 70000.0,
                        'momentum_12_1': 0.25,
                        'return_5d': 0.012,
                        'has_sufficient_history': True,
                        'is_price_above_ma120': True,
                        'is_ma60_above_ma120': False,
                        'is_recent_overheated': False,
                        'momentum_rank': 64,
                        'momentum_percentile': 0.62,
                        'momentum_percentile_threshold': 0.8,
                        'passes_momentum_top_percentile': False,
                        'is_momentum_eligible': False,
                        'factor_payload': {
                            'eligibility': {
                                'passes_trend_confirmation': False,
                                'passes_momentum_top_percentile': False,
                                'momentum_eligibility_status': 'trend_not_confirmed',
                                'is_momentum_eligible': False,
                            },
                            'ranking': {'momentum_percentile_threshold': 0.8},
                            'overheat': {
                                'has_sufficient_cross_sectional_context': True,
                                'is_recent_overheated': False,
                            },
                        },
                        'captured_at': '2026-04-08T15:00:00+09:00',
                    }
                ],
            ),
            patch(
                'app.agent_context.read_service.load_daily_advanced_value_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-08',
                        'source': 'advanced_value_v1',
                        'feature_version': 'advanced_value_v1',
                        'advanced_value_score': 0.54,
                        'advanced_value_rank': 90,
                        'advanced_value_percentile': 0.43,
                        'advanced_value_components_used': 2,
                        'advanced_value_classification': 'watchlist',
                        'is_advanced_value_eligible': False,
                        'market_cap_proxy': 10.0,
                        'enterprise_value': 12.0,
                        'ebitda': 3.0,
                        'free_cash_flow': 2.0,
                        'fcf_yield': 0.05,
                        'ev_to_ebitda': 4.0,
                        'fcf_yield_status': 'available',
                        'ev_to_ebitda_status': 'available',
                        'captured_at': '2026-04-08T15:00:00+09:00',
                    }
                ],
            ),
            patch(
                'app.agent_context.read_service.load_daily_low_risk_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-08',
                        'source': 'low_risk_v1',
                        'feature_version': 'low_risk_v1',
                        'low_risk_score': None,
                        'low_risk_classification': 'insufficient_history',
                        'is_low_risk_eligible': False,
                        'volatility_60d': None,
                        'volatility_120d': None,
                        'max_drawdown_120d': None,
                        'avg_trading_value_20d': None,
                        'volatility_60d_percentile': None,
                        'volatility_120d_percentile': None,
                        'max_drawdown_120d_percentile': None,
                        'liquidity_percentile': None,
                        'passes_liquidity_floor': None,
                        'captured_at': '2026-04-08T15:00:00+09:00',
                    }
                ],
            ),
            patch(
                'app.agent_context.read_service.load_daily_beta_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-08',
                        'source': 'beta_v1',
                        'feature_version': 'beta_v1',
                        'low_beta_score': None,
                        'low_beta_classification': 'missing_market_date_bar',
                        'is_low_beta_eligible': False,
                        'benchmark_id': 'KOSPI_EQW',
                        'benchmark_market': 'KOSPI',
                        'beta_60d': None,
                        'beta_120d': None,
                        'beta_60d_percentile': None,
                        'beta_120d_percentile': None,
                        'history_days_available': 0,
                        'captured_at': '2026-04-08T15:00:00+09:00',
                    }
                ],
            ),
            patch(
                'app.agent_context.read_service.load_daily_composite_factor_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'market_date': '2026-04-08',
                        'source': 'composite_v1',
                        'feature_version': 'composite_v1',
                        'composite_score': 0.31,
                        'composite_rank': 155,
                        'composite_percentile': 0.18,
                        'composite_classification': 'hard_excluded',
                        'is_composite_eligible': False,
                        'valid_factor_count': 3,
                        'available_base_weight': 0.65,
                        'has_required_inputs': True,
                        'is_hard_excluded': True,
                        'momentum_input': 0.62,
                        'value_input': 0.71,
                        'advanced_value_input': 0.54,
                        'low_risk_input': None,
                        'low_beta_input': None,
                        'captured_at': '2026-04-08T15:00:00+09:00',
                    }
                ],
            ),
            patch('app.agent_context.read_service.load_current_quotes', return_value=[]),
            patch('app.agent_context.read_service.load_current_kis_intraday_snapshots', return_value=[]),
            patch('app.agent_context.read_service.load_daily_price_contexts', return_value=[]),
            patch('app.agent_context.read_service.load_daily_price_context_history', return_value=[]),
            patch('app.agent_context.read_service.load_daily_market_relative_contexts', return_value=[]),
            patch(
                'app.agent_context.read_service._load_latest_symbol_day_block_history',
                return_value=latest_prior_rows,
            ),
        ):
            result = service.get_symbol_day_context(
                market_date='2026-04-08',
                symbols=['005930'],
                fs_div='CFS',
                disclosures_limit=3,
            )

        item = result['items'][0]
        summary = item['factor_summary']['factor_scores']
        self.assertEqual(summary['value']['status'], 'storage_unavailable')
        self.assertEqual(summary['unified_value']['status'], 'missing_source_row')
        self.assertEqual(summary['momentum']['status'], 'not_eligible')
        self.assertEqual(summary['momentum']['source_status'], 'trend_not_confirmed')
        self.assertEqual(summary['advanced_value']['status'], 'not_eligible')
        self.assertEqual(summary['advanced_value']['source_status'], 'watchlist')
        self.assertEqual(summary['low_risk']['status'], 'insufficient_history')
        self.assertEqual(summary['beta']['status'], 'not_computed_for_market_date')
        self.assertEqual(summary['beta']['source_status'], 'missing_market_date_bar')
        self.assertEqual(summary['composite']['status'], 'not_eligible')
        self.assertEqual(load_value_factor_results_mock.call_count, 2)
        self.assertEqual(item['liquidity_context']['ranking']['status'], 'missing')
        self.assertEqual(item['liquidity_context']['ranking']['reason'], 'low_risk_liquidity_fields_missing')
        self.assertEqual(item['context_completeness']['factor_rows']['value']['row_status'], 'storage_unavailable')
        self.assertEqual(item['factor_summary']['signal_features']['status'], 'missing_source_row')

    def test_build_decider_data_quality_context_marks_factor_score_provider_error_for_factor_storage_failure(self):
        result = _build_decider_data_quality_context(
            market_date=date(2026, 4, 8),
            scores={
                'unified_value': {
                    'status': 'unavailable',
                    'freshness_provenance': {'provenance_status': 'storage_unavailable'},
                },
                'quality': {'status': 'unavailable'},
                'momentum': {'status': 'unavailable'},
            },
            factor_summary={
                'signal_features': {'status': 'missing_source_row'},
            },
            liquidity_context={},
            quote_context=None,
            quote_context_provenance={},
            intraday_execution_context=None,
            intraday_execution_provenance={},
            intraday_execution_completeness={'status': 'unavailable'},
            fundamentals={},
            fundamentals_provenance={},
            recent_dart_events={},
            recent_disclosures_provenance={},
            recent_external_news={},
            recent_external_news_provenance={},
            investor_flow_context=None,
            investor_flow_provenance={},
            short_selling_context=None,
            short_selling_provenance={},
            market_relative_context=None,
            market_relative_provenance=None,
            sector_peer_context=None,
            sector_peer_provenance=None,
            market_breadth_context=None,
            market_breadth_provenance=None,
            technical_snapshot=None,
            action_summary=None,
            unified_value_row=None,
            advanced_value_row=None,
        )

        factor_score_block = result['blocks']['factor_score_coverage']
        self.assertEqual(result['status'], 'provider_error')
        self.assertEqual(factor_score_block['status'], 'provider_error')
        self.assertEqual(factor_score_block['required_scores']['unified_value']['status'], 'provider_error')
        self.assertEqual(result['block_status_counts']['provider_error'], 1)
        self.assertEqual(result['block_status_counts']['unavailable'], 7)

    def test_build_decider_intraday_quality_context_keeps_structured_unavailable_blocks_false(self):
        intraday_execution_context = _build_intraday_execution_context(None)

        result = _build_decider_intraday_quality_context(
            quote_context=None,
            quote_provenance={'freshness_status': 'missing', 'provenance_status': 'provider_sourced'},
            intraday_execution_context=intraday_execution_context,
            intraday_execution_provenance={
                'freshness_status': 'missing',
                'provenance_status': 'provider_sourced',
                'reason': 'no_current_kis_intraday_snapshot_cache_row',
            },
            intraday_execution_completeness=intraday_execution_context['coverage'],
        )

        intraday_snapshot = result['intraday_snapshot']
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(intraday_snapshot['status'], 'unavailable')
        self.assertFalse(intraday_snapshot['quote_available'])
        self.assertFalse(intraday_snapshot['orderbook_available'])
        self.assertFalse(intraday_snapshot['liquidity_available'])
        self.assertFalse(intraday_snapshot['investor_program_activity_available'])
        self.assertFalse(intraday_snapshot['news_hints_available'])

    def test_build_decider_intraday_quality_context_uses_status_fallback_when_block_availability_is_missing(self):
        intraday_execution_context = _build_intraday_execution_context(_make_intraday_execution_row())
        intraday_execution_context.pop('availability', None)
        intraday_execution_context['quote']['status'] = 'derived'

        result = _build_decider_intraday_quality_context(
            quote_context={'status': 'available', 'trade_date': '2026-04-08'},
            quote_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            intraday_execution_context=intraday_execution_context,
            intraday_execution_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            intraday_execution_completeness=intraday_execution_context['coverage'],
        )

        intraday_snapshot = result['intraday_snapshot']
        self.assertTrue(intraday_snapshot['quote_available'])
        self.assertTrue(intraday_snapshot['orderbook_available'])
        self.assertTrue(intraday_snapshot['liquidity_available'])
        self.assertTrue(intraday_snapshot['investor_program_activity_available'])
        self.assertTrue(intraday_snapshot['news_hints_available'])

    def test_closed_market_quote_and_intraday_contexts_are_not_applicable(self):
        market_session = _closed_market_session()

        quote_context = _build_quote_context(
            None,
            market_date=date(2026, 4, 11),
            market_session=market_session,
        )
        quote_provenance = _build_quote_freshness_provenance(
            row=None,
            requested_market_date=date(2026, 4, 11),
            market_session=market_session,
        )
        intraday_execution_context = _build_intraday_execution_context(
            None,
            market_session=market_session,
        )
        intraday_execution_provenance = _build_intraday_execution_freshness_provenance(
            row=None,
            requested_market_date=date(2026, 4, 11),
            market_session=market_session,
        )

        self.assertEqual(quote_context['status'], 'not_applicable')
        self.assertEqual(quote_context['reason'], 'closed_market')
        self.assertEqual(quote_context['market_session_status'], 'weekend')
        self.assertEqual(quote_context['latest_valid_trading_date'], '2026-04-10')
        self.assertFalse(quote_context['data_expected'])
        self.assertEqual(quote_provenance['freshness_status'], 'not_applicable')
        self.assertEqual(quote_provenance['provenance_status'], 'not_applicable')
        self.assertEqual(intraday_execution_context['status'], 'not_applicable')
        self.assertEqual(intraday_execution_context['coverage']['status'], 'not_applicable')
        self.assertEqual(
            intraday_execution_context['coverage']['block_statuses']['quote'],
            'not_applicable',
        )
        self.assertEqual(intraday_execution_provenance['freshness_status'], 'not_applicable')
        self.assertEqual(intraday_execution_provenance['provenance_status'], 'not_applicable')

    def test_build_decider_intraday_quality_context_marks_closed_market_as_not_applicable(self):
        market_session = _closed_market_session()
        quote_context = _build_quote_context(
            None,
            market_date=date(2026, 4, 11),
            market_session=market_session,
        )
        intraday_execution_context = _build_intraday_execution_context(
            None,
            market_session=market_session,
        )

        result = _build_decider_intraday_quality_context(
            quote_context=quote_context,
            quote_provenance=_build_quote_freshness_provenance(
                row=None,
                requested_market_date=date(2026, 4, 11),
                market_session=market_session,
            ),
            intraday_execution_context=intraday_execution_context,
            intraday_execution_provenance=_build_intraday_execution_freshness_provenance(
                row=None,
                requested_market_date=date(2026, 4, 11),
                market_session=market_session,
            ),
            intraday_execution_completeness=intraday_execution_context['coverage'],
        )

        self.assertEqual(result['status'], 'not_applicable')
        self.assertEqual(result['market_session_status'], 'weekend')
        self.assertEqual(result['recommendation_basis'], 'latest_completed_eod')
        self.assertEqual(result['latest_valid_trading_date'], '2026-04-10')
        self.assertEqual(result['quote']['status'], 'not_applicable')
        self.assertEqual(result['intraday_snapshot']['status'], 'not_applicable')
        self.assertFalse(result['intraday_snapshot']['quote_available'])
        self.assertFalse(result['intraday_snapshot']['orderbook_available'])
        self.assertFalse(result['intraday_snapshot']['liquidity_available'])
        self.assertFalse(result['intraday_snapshot']['investor_program_activity_available'])
        self.assertFalse(result['intraday_snapshot']['news_hints_available'])

    def test_build_decider_data_quality_context_marks_factor_score_provider_error_for_signal_feature_storage_failure(self):
        result = _build_decider_data_quality_context(
            market_date=date(2026, 4, 8),
            scores={
                'unified_value': {'status': 'unavailable'},
                'quality': {'status': 'unavailable'},
                'momentum': {'status': 'unavailable'},
            },
            factor_summary={
                'signal_features': {
                    'status': 'storage_unavailable',
                    'provenance': {'provenance_status': 'storage_unavailable'},
                }
            },
            liquidity_context={},
            quote_context=None,
            quote_context_provenance={},
            intraday_execution_context=None,
            intraday_execution_provenance={},
            intraday_execution_completeness={'status': 'unavailable'},
            fundamentals={},
            fundamentals_provenance={},
            recent_dart_events={},
            recent_disclosures_provenance={},
            recent_external_news={},
            recent_external_news_provenance={},
            investor_flow_context=None,
            investor_flow_provenance={},
            short_selling_context=None,
            short_selling_provenance={},
            market_relative_context=None,
            market_relative_provenance=None,
            sector_peer_context=None,
            sector_peer_provenance=None,
            market_breadth_context=None,
            market_breadth_provenance=None,
            technical_snapshot=None,
            action_summary=None,
            unified_value_row=None,
            advanced_value_row=None,
        )

        factor_score_block = result['blocks']['factor_score_coverage']
        self.assertEqual(result['status'], 'provider_error')
        self.assertEqual(factor_score_block['status'], 'provider_error')
        self.assertEqual(factor_score_block['signal_features']['status'], 'provider_error')
        self.assertEqual(factor_score_block['signal_features']['source_status'], 'storage_unavailable')

    def test_build_decider_data_quality_context_marks_empty_news_and_annual_financials_as_auxiliary(self):
        result = _build_decider_data_quality_context(
            market_date=date(2026, 4, 8),
            scores={
                'unified_value': {'status': 'available'},
                'quality': {'status': 'available'},
                'momentum': {'status': 'available'},
            },
            factor_summary={
                'signal_features': {'status': 'available'},
            },
            liquidity_context={
                'turnover': {'status': 'available', 'provenance': {'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'}},
                'ranking': {'status': 'available', 'provenance': {'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'}},
                'intraday': {
                    'status': 'available',
                    'provenance': {
                        'intraday_execution_context': {
                            'freshness_status': 'fresh',
                            'provenance_status': 'provider_sourced',
                        }
                    },
                },
            },
            quote_context={'trade_date': '2026-04-08'},
            quote_context_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            intraday_execution_context={'status': 'available'},
            intraday_execution_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            intraday_execution_completeness={'status': 'available'},
            fundamentals={
                'latest_snapshot': {
                    'filing': {'receipt_date': '2025-03-12', 'report_name': '사업보고서 (2024.12)'},
                    'period': {'period_end_date': '2024-12-31', 'business_year': 2024, 'report_code': '11011'},
                    'statement': {'source_status': 'available'},
                    'fundamental_context': {
                        'normalized_facts': {
                            'income_statement': {
                                'gross_profit': {'status': 'available'},
                            },
                            'balance_sheet': {
                                'current_assets': {'status': 'available'},
                                'current_liabilities': {'status': 'available'},
                                'cash_and_cash_equivalents': {'status': 'available'},
                            },
                            'cash_flow_statement': {
                                'operating_cash_flow': {'status': 'available'},
                                'investing_cash_flow': {'status': 'missing'},
                                'financing_cash_flow': {'status': 'missing'},
                            },
                        }
                    },
                }
            },
            fundamentals_provenance={'freshness_status': 'available_but_old', 'provenance_status': 'provider_sourced'},
            recent_dart_events={'status': 'empty', 'reason': 'no_recent_disclosures', 'summary': {}, 'item_count': 0},
            recent_disclosures_provenance={
                'freshness_status': 'missing',
                'provenance_status': 'provider_sourced',
                'reason': 'no_recent_disclosures_up_to_request_date',
            },
            recent_external_news={
                'status': 'empty',
                'reason': 'no_recent_external_news_found',
                'item_count': 0,
                'provider': {'provider': 'naver_news_search', 'enabled': True, 'status': 'available', 'reason': None},
            },
            recent_external_news_provenance={
                'freshness_status': 'missing',
                'provenance_status': 'provider_sourced',
                'reason': 'no_recent_external_news_found',
            },
            investor_flow_context={'status': 'available', 'symbol_flow': {}, 'market_flow': {}},
            investor_flow_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            short_selling_context={'status': 'available', 'short_volume': {}, 'short_balance': {}},
            short_selling_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            market_relative_context={'status': 'available', 'relative_strength': {}, 'breadth': {}},
            market_relative_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            sector_peer_context={'status': 'available', 'sector_name': 'Semiconductor', 'peer_count': 5, 'same_sector_symbol_count': 6},
            sector_peer_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            market_breadth_context={'status': 'available', 'market_symbol_count': 900, 'up_count': 500, 'down_count': 300, 'flat_count': 100},
            market_breadth_provenance={'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
            technical_snapshot={'trend_state': 'uptrend'},
            action_summary={'support_price': 68000, 'resistance_price': 70800, 'invalidation_price': 66500},
            unified_value_row=None,
            advanced_value_row=None,
        )

        self.assertEqual(result['decision_priority']['core_execution']['status'], 'available')
        self.assertFalse(result['decision_priority']['auxiliary_context']['blocking_by_default'])
        self.assertTrue(result['blocks']['disclosures_news']['empty_news_is_neutral'])
        self.assertTrue(
            result['blocks']['disclosures_news']['decision_guidance']['empty_is_neutral_by_default']
        )
        self.assertIn(
            'neutral unless separate risk evidence',
            result['blocks']['disclosures_news']['decision_guidance']['interpretation'],
        )
        self.assertEqual(result['blocks']['financials_cashflow']['report_period_type'], 'annual')
        self.assertTrue(result['blocks']['financials_cashflow']['annual_only_context'])
        self.assertEqual(
            result['blocks']['financials_cashflow']['decision_guidance']['default_missing_effect'],
            'confidence_reducer',
        )
        self.assertIn(
            'Annual fundamentals are still usable context',
            result['blocks']['financials_cashflow']['decision_guidance']['interpretation'],
        )
        self.assertEqual(result['block_status_counts']['provider_error'], 0)
        self.assertEqual(result['block_status_counts']['empty'], 1)

    def test_load_latest_symbol_day_block_history_only_requests_low_risk_liquidity_fields(self):
        with patch('app.agent_context.read_service._load_latest_prior_rows_for_model', return_value={}) as load_prior_rows_mock:
            _load_latest_symbol_day_block_history(
                market_date=date(2026, 4, 8),
                symbols=['005930'],
            )

        self.assertEqual(load_prior_rows_mock.call_count, len(PERSISTED_FACTOR_FAMILIES) + 1)
        extra_fields_by_model = {
            call.kwargs['model'].__name__: call.kwargs.get('extra_fields', ())
            for call in load_prior_rows_mock.call_args_list
        }
        self.assertEqual(
            extra_fields_by_model['DailyLowRiskFactorResult'],
            (
                'avg_trading_value_20d',
                'liquidity_rank',
                'liquidity_percentile',
                'passes_liquidity_floor',
            ),
        )
        for model_name, extra_fields in extra_fields_by_model.items():
            if model_name != 'DailyLowRiskFactorResult':
                self.assertEqual(extra_fields, ())

    def test_load_latest_symbol_day_block_history_retries_transient_factor_error(self):
        value_attempts = 0

        def load_prior_rows(**kwargs):
            nonlocal value_attempts
            if kwargs['model'].__name__ != 'DailyValueFactorResult':
                return {}
            value_attempts += 1
            if value_attempts == 1:
                raise SQLAlchemyError('transient prior factor read failure')
            return {'005930': {'market_date': '2026-04-07'}}

        with patch(
            'app.agent_context.read_service._load_latest_prior_rows_for_model',
            side_effect=load_prior_rows,
        ):
            result = _load_latest_symbol_day_block_history(
                market_date=date(2026, 4, 8),
                symbols=['005930'],
            )

        self.assertEqual(value_attempts, 2)
        self.assertEqual(
            result['factor_rows']['value'],
            {'005930': {'market_date': '2026-04-07'}},
        )
        self.assertFalse(result['factor_storage_unavailable']['value'])

    def test_build_factor_summary_entry_marks_unscored_value_families_not_eligible(self):
        cases = (
            (
                'value',
                {
                    'market_date': '2026-04-08',
                    'source': 'value_factor_v1',
                    'feature_version': 'value_factor_v1',
                    'value_status': 'missing_metric',
                    'value_score': None,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
                'missing_metric',
            ),
            (
                'unified_value',
                {
                    'market_date': '2026-04-08',
                    'source': 'unified_value_v1',
                    'feature_version': 'unified_value_v1',
                    'unified_value_status': 'insufficient_components',
                    'unified_value_score': None,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
                'insufficient_components',
            ),
            (
                'quality',
                {
                    'market_date': '2026-04-08',
                    'source': 'quality_v1',
                    'feature_version': 'quality_v1',
                    'quality_status': 'insufficient_components',
                    'quality_score': None,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
                'insufficient_components',
            ),
        )

        for family, row, expected_source_status in cases:
            with self.subTest(family=family, source_status=expected_source_status):
                entry = _build_factor_summary_entry(
                    family=family,
                    row=row,
                    score_context={},
                    freshness_provenance={'freshness_status': 'fresh'},
                    current_storage_unavailable=False,
                    prior_storage_unavailable=False,
                )
                self.assertEqual(entry['status'], 'not_eligible')
                self.assertEqual(entry['source_status'], expected_source_status)
                self.assertFalse(entry['score_available'])

    def test_build_factor_summary_entry_preserves_available_for_scored_value_families(self):
        cases = (
            (
                'value',
                {
                    'market_date': '2026-04-08',
                    'source': 'value_factor_v1',
                    'feature_version': 'value_factor_v1',
                    'value_status': 'scored',
                    'value_score': 0.91,
                    'value_rank': 8,
                    'value_percentile': 0.92,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
            ),
            (
                'unified_value',
                {
                    'market_date': '2026-04-08',
                    'source': 'unified_value_v1',
                    'feature_version': 'unified_value_v1',
                    'unified_value_status': 'scored',
                    'unified_value_score': 0.84,
                    'unified_value_rank': 15,
                    'unified_value_percentile': 0.79,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
            ),
            (
                'quality',
                {
                    'market_date': '2026-04-08',
                    'source': 'quality_v1',
                    'feature_version': 'quality_v1',
                    'quality_status': 'scored_partial',
                    'quality_score': 0.67,
                    'quality_rank': 22,
                    'quality_percentile': 0.58,
                    'captured_at': '2026-04-08T15:00:00+09:00',
                },
            ),
        )

        for family, row in cases:
            with self.subTest(family=family):
                entry = _build_factor_summary_entry(
                    family=family,
                    row=row,
                    score_context={},
                    freshness_provenance={'freshness_status': 'fresh'},
                    current_storage_unavailable=False,
                    prior_storage_unavailable=False,
                )
                self.assertEqual(entry['status'], 'available')
                self.assertTrue(entry['score_available'])

    @patch('app.agent_context.read_service.load_daily_market_relative_contexts', return_value=[])
    @patch('app.agent_context.read_service.load_daily_composite_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_beta_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_low_risk_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_advanced_value_factor_results', return_value=[])
    @patch('app.agent_context.read_service.load_daily_momentum_factor_results')
    @patch('app.agent_context.read_service.load_daily_quality_factor_results')
    @patch('app.agent_context.read_service.load_daily_unified_value_factor_results')
    @patch('app.agent_context.read_service.load_daily_value_factor_results')
    @patch('app.agent_context.read_service.load_current_quotes', return_value=[])
    @patch('app.agent_context.read_service.load_current_kis_intraday_snapshots', return_value=[])
    @patch('app.agent_context.read_service.load_daily_price_context_history', return_value=[])
    @patch('app.agent_context.read_service.load_daily_price_contexts', return_value=[])
    @patch('app.agent_context.read_service.load_daily_action_summaries', return_value=[])
    @patch('app.agent_context.read_service.load_daily_signal_features')
    @patch('app.agent_context.read_service.load_daily_technical_snapshots', return_value=[])
    @patch('app.agent_context.read_service.load_symbols')
    def test_get_symbol_day_context_reports_factor_complete_and_signal_features_sparse_for_requested_universe(
        self,
        load_symbols_mock,
        _load_technical_mock,
        load_signal_features_mock,
        _load_actions_mock,
        _load_price_contexts_mock,
        _load_price_context_history_mock,
        _load_current_kis_intraday_snapshots_mock,
        _load_current_quotes_mock,
        load_value_mock,
        load_unified_value_mock,
        load_quality_mock,
        load_momentum_mock,
        *_unused,
    ):
        symbols = [f'{index:06d}' for index in range(1, 77)]
        load_symbols_mock.return_value = [
            {
                'id': index,
                'symbol': symbol,
                'standard_code': f'KR{index:010d}',
                'name_ko': symbol,
                'market': 'KOSPI',
                'instrument_type': 'equity',
                'source': 'pykrx',
                'is_active': True,
                'last_refreshed_at': None,
            }
            for index, symbol in enumerate(symbols, start=1)
        ]
        load_signal_features_mock.return_value = [
            {
                'symbol': symbol,
                'market_date': '2026-05-06',
                'source': 'kis_ranking',
                'signal_count': 1,
                'volume_power_rank': offset,
                'volume_power': 120.0 + offset,
                'overtime_volume_rank': None,
                'overtime_volume': None,
                'overtime_fluctuation_rank': None,
                'overtime_fluctuation_rate': None,
                'feature_payload': {},
                'captured_at': '2026-05-06T15:00:00+09:00',
            }
            for offset, symbol in enumerate(symbols[:2], start=1)
        ]
        load_value_mock.return_value = [
            {
                'symbol': symbol,
                'market_date': '2026-05-06',
                'source': 'value_factor_v1',
                'feature_version': 'value_factor_v1',
                'per': 10.0,
                'pbr': 1.0,
                'dividend_yield': 2.0,
                'per_status': 'available',
                'per_rank': index,
                'pbr_rank': index,
                'dividend_yield_rank': index,
                'per_percentile': 1.0,
                'pbr_percentile': 1.0,
                'dividend_yield_percentile': 1.0,
                'value_score': 0.82,
                'value_rank': index,
                'value_percentile': 1.0,
                'value_status': 'scored',
                'factor_payload': {},
                'captured_at': '2026-05-06T15:00:00+09:00',
            }
            for index, symbol in enumerate(symbols, start=1)
        ]
        load_unified_value_mock.return_value = [
            {
                'symbol': symbol,
                'market_date': '2026-05-06',
                'source': 'unified_value_v1',
                'feature_version': 'unified_value_v1',
                'unified_value_score': 0.8,
                'unified_value_rank': index,
                'unified_value_percentile': 1.0,
                'components_used': 5,
                'unified_value_status': 'scored',
                'per': 10.0,
                'pbr': 1.0,
                'dividend_yield': 2.0,
                'fcf_yield': 3.0,
                'ev_to_ebitda': 8.0,
                'per_status': 'available',
                'pbr_status': 'available',
                'dividend_yield_status': 'available',
                'fcf_yield_status': 'available',
                'ev_to_ebitda_status': 'available',
                'captured_at': '2026-05-06T15:00:00+09:00',
            }
            for index, symbol in enumerate(symbols, start=1)
        ]
        load_quality_mock.return_value = [
            {
                'symbol': symbol,
                'market_date': '2026-05-06',
                'source': 'quality_v1',
                'feature_version': 'quality_v1',
                'quality_score': 0.7,
                'quality_rank': index,
                'quality_percentile': 1.0,
                'components_used': 6,
                'components_possible': 6,
                'is_quality_score_complete': True,
                'quality_status': 'scored_complete',
                'roe': 0.12,
                'roa': 0.08,
                'gross_profit_to_assets': 0.11,
                'operating_cash_flow_to_net_income': 1.1,
                'debt_ratio': 0.4,
                'accruals_ratio': 0.02,
                'roe_status': 'available',
                'roa_status': 'available',
                'gross_profit_to_assets_status': 'available',
                'operating_cash_flow_to_net_income_status': 'available',
                'debt_ratio_status': 'available',
                'accruals_ratio_status': 'available',
                'captured_at': '2026-05-06T15:00:00+09:00',
            }
            for index, symbol in enumerate(symbols, start=1)
        ]
        load_momentum_mock.return_value = [
            {
                'symbol': symbol,
                'market_date': '2026-05-06',
                'source': 'daily_price_bars',
                'feature_version': 'price_momentum_v1',
                'momentum_percentile': 0.9,
                'momentum_rank': index,
                'momentum_12_1': 0.31,
                'return_5d': 0.04,
                'ma_60': 100.0,
                'ma_120': 90.0,
                'momentum_percentile_threshold': 0.8,
                'passes_momentum_top_percentile': True,
                'is_momentum_eligible': True,
                'captured_at': '2026-05-06T15:00:00+09:00',
            }
            for index, symbol in enumerate(symbols, start=1)
        ]

        class EmptyFundamentalsRepository:
            def list_stored_fundamentals_summary(
                self,
                *,
                stock_codes,
                fs_div=None,
                captured_before=None,
                receipt_date_to=None,
            ):
                return []

        class EmptyDisclosureRepository:
            def list_recent_events_by_stock_codes(
                self,
                *,
                stock_codes,
                receipt_date_to=None,
                observed_before=None,
                limit_per_symbol=5,
            ):
                return {symbol: [] for symbol in stock_codes}

        service = AgentContextReadService(
            fundamentals_repository=EmptyFundamentalsRepository(),
            disclosure_repository=EmptyDisclosureRepository(),
        )
        with patch(
            'app.agent_context.read_service._load_latest_symbol_day_block_history',
            return_value=EMPTY_LATEST_PRIOR_ROWS,
        ):
            result = service.get_symbol_day_context(
                market_date='2026-05-06',
                symbols=symbols,
                fs_div='CFS',
                disclosures_limit=3,
            )

        coverage = result['summary']['context_completeness']
        self.assertEqual(coverage['factor_rows']['value']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['unified_value']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['quality']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['momentum']['present_rows'], 76)
        self.assertEqual(coverage['factor_rows']['value']['coverage_status'], 'complete')
        self.assertEqual(coverage['factor_rows']['unified_value']['coverage_status'], 'complete')
        self.assertEqual(coverage['signal_features']['present_rows'], 2)
        self.assertEqual(coverage['signal_features']['missing_rows'], 74)
        self.assertEqual(coverage['signal_features']['coverage_status'], 'sparse')
        self.assertAlmostEqual(coverage['signal_features']['coverage_ratio'], 0.0263, places=4)
        self.assertEqual(
            result['items'][0]['context_completeness']['requested_symbol_coverage']['signal_features']['missing_rows'],
            74,
        )
        self.assertEqual(
            result['items'][0]['context_provenance']['signal_features']['requested_symbol_coverage']['present_rows'],
            2,
        )
        self.assertEqual(
            result['items'][0]['context_completeness']['requested_symbol_coverage']['intraday_execution_context']['coverage_status'],
            'not_applicable',
        )

    def test_get_symbol_day_context_includes_pykrx_enrichment_blocks(self):
        captured_at = '2026-04-08T15:00:00+09:00'

        class EmptyFundamentalsRepository:
            def list_stored_fundamentals_summary(
                self,
                *,
                stock_codes,
                fs_div=None,
                captured_before=None,
                receipt_date_to=None,
            ):
                return []

        class EmptyDisclosureRepository:
            def list_recent_events_by_stock_codes(
                self,
                *,
                stock_codes,
                receipt_date_to=None,
                observed_before=None,
                limit_per_symbol=5,
            ):
                return {symbol: [] for symbol in stock_codes}

        service = AgentContextReadService(
            fundamentals_repository=EmptyFundamentalsRepository(),
            disclosure_repository=EmptyDisclosureRepository(),
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    'app.agent_context.read_service.load_symbols',
                    return_value=[
                        {
                            'id': 1,
                            'symbol': '005930',
                            'standard_code': 'KR7005930003',
                            'name_ko': '삼성전자',
                            'market': 'KOSPI',
                            'instrument_type': 'equity',
                            'source': 'pykrx',
                            'is_active': True,
                            'last_refreshed_at': None,
                        }
                    ],
                )
            )
            stack.enter_context(patch('app.agent_context.read_service.load_daily_technical_snapshots', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_signal_features', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_action_summaries', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_price_contexts', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_price_context_history', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_current_quotes', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_current_kis_intraday_snapshots', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_value_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_unified_value_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_quality_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_momentum_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_advanced_value_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_low_risk_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_beta_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_composite_factor_results', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.load_daily_market_relative_contexts', return_value=[]))
            stack.enter_context(patch('app.agent_context.read_service.build_signal_decider_scores', return_value={}))
            stack.enter_context(
                patch(
                    'app.agent_context.read_service._load_latest_symbol_day_block_history',
                    return_value=EMPTY_LATEST_PRIOR_ROWS,
                )
            )
            stack.enter_context(
                patch(
                    'app.agent_context.read_service.pykrx_enrichment_service.load_symbol_day_context_inputs',
                    return_value={
                        'by_symbol': {
                            '005930': {
                                'sector_classification': {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'name_ko': '삼성전자',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'sector_name': '반도체',
                                        'close_price': 71000,
                                        'market_cap': 420000000000,
                                        'change_pct': 1.2,
                                    },
                                },
                                'investor_flow_symbol': {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'investor_groups': {
                                            '외국인': {'net_value': 1200000},
                                            '기관합계': {'net_value': -350000},
                                        }
                                    },
                                },
                                'short_selling': {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'short_volume': {'short_volume': 1000, 'short_volume_ratio_pct': 0.35},
                                        'short_balance': {'short_balance_ratio_pct': 0.45},
                                    },
                                },
                                'valuation_snapshot': {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'market_cap': 420000000000,
                                        'trading_value': 9870000000,
                                        'per': 11.1,
                                        'pbr': 1.3,
                                    },
                                },
                                'foreign_ownership': {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'foreign_ownership_ratio_pct': 51.2,
                                        'foreign_limit_exhaustion_rate_pct': 73.4,
                                    },
                                },
                            }
                        },
                        'by_market': {
                            'KOSPI': {
                                'market_breadth': {
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'market_symbol_count': 900,
                                        'up_count': 500,
                                        'down_count': 300,
                                        'flat_count': 100,
                                        'advance_decline_ratio': 1.6667,
                                    },
                                },
                                'investor_flow_market': {
                                    'market': 'KOSPI',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'investor_groups': [
                                            {'investor_type': '외국인', 'net_value': 800000000},
                                            {'investor_type': '기관합계', 'net_value': -240000000},
                                        ]
                                    },
                                },
                            }
                        },
                        'sector_rows_by_market': {
                            'KOSPI': [
                                {
                                    'symbol': '005930',
                                    'market': 'KOSPI',
                                    'name_ko': '삼성전자',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'sector_name': '반도체',
                                        'market_cap': 420000000000,
                                        'change_pct': 1.2,
                                    },
                                },
                                {
                                    'symbol': '000660',
                                    'market': 'KOSPI',
                                    'name_ko': 'SK하이닉스',
                                    'captured_at': captured_at,
                                    'payload': {
                                        'sector_name': '반도체',
                                        'market_cap': 180000000000,
                                        'change_pct': 0.9,
                                    },
                                },
                            ]
                        },
                    },
                )
            )

            result = service.get_symbol_day_context(
                market_date='2026-04-08',
                symbols=['005930'],
                fs_div='CFS',
                disclosures_limit=3,
            )

        item = result['items'][0]
        self.assertEqual(item['sector_peer_context']['sector_name'], '반도체')
        self.assertEqual(item['sector_peer_context']['peer_count'], 1)
        self.assertEqual(item['sector_peer_context']['peers'][0]['symbol'], '000660')
        self.assertEqual(item['market_breadth_context']['advance_decline_ratio'], 1.6667)
        self.assertEqual(item['investor_flow_context']['symbol_flow']['investor_groups']['외국인']['net_value'], 1200000)
        self.assertEqual(item['investor_flow_context']['market_flow']['investor_groups'][0]['investor_type'], '외국인')
        self.assertEqual(item['short_selling_context']['short_balance']['short_balance_ratio_pct'], 0.45)
        self.assertEqual(item['valuation_context']['valuation_snapshot']['per'], 11.1)
        self.assertEqual(item['valuation_context']['foreign_ownership']['foreign_ownership_ratio_pct'], 51.2)
        self.assertEqual(item['liquidity_context']['turnover']['trading_value_source'], 'raw')
        self.assertEqual(item['liquidity_context']['turnover']['turnover_krw'], 9870000000)
        self.assertEqual(item['pre_decider_enrichment_context']['trading_value']['source'], 'raw')
        self.assertEqual(
            item['pre_decider_enrichment_context']['flow_short_sector_context']['investor_flow']['symbol_flow'][
                'investor_groups'
            ]['외국인']['net_value'],
            1200000,
        )
        self.assertEqual(
            item['pre_decider_enrichment_context']['flow_short_sector_context']['short_selling']['short_balance'][
                'short_balance_ratio_pct'
            ],
            0.45,
        )
        self.assertEqual(
            item['pre_decider_enrichment_context']['flow_short_sector_context']['sector_peers']['peer_count'],
            1,
        )
        self.assertEqual(item['context_provenance']['sector_peer_context']['freshness_status'], 'fresh')
        self.assertEqual(item['context_provenance']['market_breadth_context']['provenance_status'], 'derived_from_provider_data')
        self.assertEqual(item['context_provenance']['investor_flow_context']['source_tables'], ['daily_pykrx_enrichment_rows'])
        self.assertEqual(item['context_completeness']['sector_peer_mapping']['status'], 'available')
        self.assertEqual(item['context_completeness']['requested_symbol_coverage']['valuation_context']['coverage_status'], 'complete')
        self.assertEqual(result['summary']['sector_peer_context_count'], 1)
        self.assertEqual(result['summary']['investor_flow_context_count'], 1)
        self.assertEqual(result['summary']['pre_decider_enrichment_context_count'], 1)
        self.assertEqual(result['summary']['context_completeness']['market_breadth_context']['coverage_status'], 'complete')
