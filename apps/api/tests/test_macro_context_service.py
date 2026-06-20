import unittest
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import delete, func, select

from app.core.settings import settings
from app.db import engine, session_scope
from app.db_models import MacroContextObservation
from app.macro_context.service import (
    DEFAULT_WORLD_BANK_SERIES,
    FRED_SOURCE,
    MacroContextService,
    WORLD_BANK_SOURCE,
)
from app.macro_context.store import MacroContextObservationStore


class FakeWorldBankClient:
    def __init__(self, values: dict[str, float] | None = None) -> None:
        self.values = values or {
            'NY.GDP.MKTP.KD.ZG': 2.8,
            'FP.CPI.TOTL.ZG': 2.2,
            'SL.UEM.TOTL.ZS': 2.7,
        }
        self.calls: list[dict] = []

    async def fetch_indicator(self, **kwargs):
        self.calls.append(kwargs)
        indicator = kwargs['indicator']
        return {
            'metadata': {'source': 'fake-world-bank'},
            'rows': [
                {
                    'date': '2024',
                    'value': str(self.values[indicator]),
                    'indicator': {'id': indicator, 'value': DEFAULT_WORLD_BANK_SERIES[indicator].series_name},
                    'country': {'id': 'KOR', 'value': 'Korea, Rep.'},
                }
            ],
            'raw': {},
        }


class FailingFredClient:
    async def fetch_latest_observations(self, **kwargs):  # pragma: no cover - should not be called without key.
        raise AssertionError('FRED should not be called when FRED_API_KEY is missing')


class MacroContextServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        MacroContextObservation.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.original_settings = {
            'macro_context_enabled': settings.macro_context_enabled,
            'macro_context_refresh_min_interval_hours': settings.macro_context_refresh_min_interval_hours,
            'macro_context_stale_after_hours': settings.macro_context_stale_after_hours,
            'macro_context_world_bank_enabled': settings.macro_context_world_bank_enabled,
            'macro_context_world_bank_country': settings.macro_context_world_bank_country,
            'macro_context_world_bank_series': settings.macro_context_world_bank_series,
            'macro_context_world_bank_mrnev': settings.macro_context_world_bank_mrnev,
            'macro_context_fred_enabled': settings.macro_context_fred_enabled,
            'macro_context_fred_series': settings.macro_context_fred_series,
            'fred_api_key': settings.fred_api_key,
        }
        settings.macro_context_enabled = True
        settings.macro_context_refresh_min_interval_hours = 24
        settings.macro_context_stale_after_hours = 72
        settings.macro_context_world_bank_enabled = True
        settings.macro_context_world_bank_country = 'KOR'
        settings.macro_context_world_bank_series = 'NY.GDP.MKTP.KD.ZG,FP.CPI.TOTL.ZG,SL.UEM.TOTL.ZS'
        settings.macro_context_world_bank_mrnev = 1
        settings.macro_context_fred_enabled = True
        settings.macro_context_fred_series = 'DEXKOUS'
        settings.fred_api_key = ''
        with session_scope() as session:
            session.execute(delete(MacroContextObservation))

    def tearDown(self) -> None:
        with session_scope() as session:
            session.execute(delete(MacroContextObservation))
        for key, value in self.original_settings.items():
            setattr(settings, key, value)

    def _service(self, world_bank_client: FakeWorldBankClient | None = None) -> MacroContextService:
        return MacroContextService(
            store=MacroContextObservationStore(),
            world_bank_client=world_bank_client or FakeWorldBankClient(),
            fred_client=FailingFredClient(),
        )

    def test_refresh_persists_world_bank_series(self) -> None:
        world_bank = FakeWorldBankClient()
        service = self._service(world_bank)

        result = service.refresh_context(force=True, trigger='unit_test')

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['observations_upserted'], 3)
        self.assertEqual(result['observations_failed'], 0)
        self.assertEqual(result['source_statuses'][WORLD_BANK_SOURCE]['status'], 'available')
        self.assertEqual(result['source_statuses'][FRED_SOURCE]['status'], 'disabled_missing_api_key')
        self.assertEqual(len(world_bank.calls), 3)
        with session_scope() as session:
            row_payloads = [
                {
                    'source': row.source,
                    'series_id': row.series_id,
                    'source_status': row.source_status,
                    'observation_period': row.observation_period,
                    'observation_date': row.observation_date.isoformat(),
                    'value': row.value,
                    'fetched_at': row.fetched_at,
                }
                for row in session.execute(
                    select(MacroContextObservation).order_by(MacroContextObservation.series_id.asc())
                ).scalars().all()
            ]
            count = session.execute(select(func.count(MacroContextObservation.id))).scalar_one()
        self.assertEqual(count, 3)
        self.assertEqual({row['source'] for row in row_payloads}, {WORLD_BANK_SOURCE})
        self.assertEqual({row['source_status'] for row in row_payloads}, {'available'})
        self.assertEqual({row['observation_period'] for row in row_payloads}, {'2024'})
        self.assertEqual({row['observation_date'] for row in row_payloads}, {'2024-12-31'})
        self.assertTrue(all(row['value'] is not None for row in row_payloads))
        self.assertTrue(all(row['fetched_at'] is not None for row in row_payloads))

        context = service.get_context(as_of_date='2026-06-20')

        self.assertEqual(context['status'], 'available')
        self.assertEqual(context['macro_regime_summary']['growth_label'], 'growth_expansion')
        self.assertEqual(context['macro_regime_summary']['inflation_label'], 'inflation_stable')
        self.assertEqual(context['macro_regime_summary']['labor_label'], 'labor_tight')
        self.assertFalse(context['macro_regime_summary']['llm_generated'])
        self.assertTrue(context['usage_policy']['contextual_input_only'])
        self.assertFalse(context['usage_policy']['direct_buy_sell_signal'])

    def test_disabled_keyed_provider_is_reported_without_failure(self) -> None:
        service = self._service()

        result = service.refresh_context(force=True, trigger='unit_test')

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['source_statuses'][FRED_SOURCE]['status'], 'disabled_missing_api_key')
        self.assertEqual(result['observations_failed'], 0)
        self.assertEqual(
            result['series_statuses'][-1],
            {
                'source': FRED_SOURCE,
                'series_id': None,
                'status': 'disabled_missing_api_key',
                'reason': 'FRED_API_KEY is not configured',
            },
        )

    def test_get_context_marks_stale_data(self) -> None:
        settings.macro_context_stale_after_hours = 24
        service = self._service()
        old_fetched_at = datetime.now(timezone.utc) - timedelta(days=4)
        for series_id, config in DEFAULT_WORLD_BANK_SERIES.items():
            service.store.upsert_observation(
                source=WORLD_BANK_SOURCE,
                series_id=series_id,
                series_name=config.series_name,
                country_code='KOR',
                observation_period='2024',
                observation_date='2024-12-31',
                value=2.0,
                unit=config.unit,
                frequency=config.frequency,
                fetched_at=old_fetched_at,
                source_status='available',
            )

        context = service.get_context(as_of_date='2026-06-20')

        self.assertEqual(context['status'], 'stale')
        self.assertEqual(context['freshness']['status'], 'stale')
        self.assertEqual(context['macro_regime_summary']['freshness_label'], 'macro_context_stale')
        self.assertEqual(
            {item['freshness']['freshness_status'] for item in context['series']},
            {'stale'},
        )

    def test_get_context_excludes_rows_fetched_after_historical_as_of_date(self) -> None:
        service = self._service()
        fetched_after_replay = datetime(2026, 6, 19, 12, tzinfo=timezone.utc)
        now = datetime(2026, 6, 20, 9, tzinfo=timezone.utc)
        for series_id, config in DEFAULT_WORLD_BANK_SERIES.items():
            service.store.upsert_observation(
                source=WORLD_BANK_SOURCE,
                series_id=series_id,
                series_name=config.series_name,
                country_code='KOR',
                observation_period='2024',
                observation_date='2024-12-31',
                value=2.0,
                unit=config.unit,
                frequency=config.frequency,
                fetched_at=fetched_after_replay,
                source_status='available',
            )

        historical_context = service.get_context(as_of_date='2025-01-01', now=now)

        self.assertEqual(historical_context['status'], 'unavailable')
        self.assertIsNone(historical_context['latest_fetched_at'])
        self.assertTrue(historical_context['freshness']['knowledge_cutoff_applied'])
        self.assertEqual(historical_context['freshness']['knowledge_cutoff_at'], '2025-01-02T00:00:00+00:00')
        self.assertEqual(historical_context['freshness']['status'], 'unavailable')
        self.assertEqual(historical_context['macro_regime_summary']['freshness_label'], 'macro_context_unavailable')
        self.assertEqual(historical_context['source_statuses'][WORLD_BANK_SOURCE]['status'], 'unavailable')
        self.assertEqual(historical_context['source_statuses'][WORLD_BANK_SOURCE]['available_count'], 0)
        self.assertEqual(historical_context['source_statuses'][WORLD_BANK_SOURCE]['missing_count'], 3)
        self.assertEqual({item['status'] for item in historical_context['series']}, {'missing'})
        self.assertTrue(all(item['latest_value'] is None for item in historical_context['series']))

        current_context = service.get_context(as_of_date='2026-06-20', now=now)

        self.assertEqual(current_context['status'], 'available')
        self.assertFalse(current_context['freshness']['knowledge_cutoff_applied'])
        self.assertEqual({item['status'] for item in current_context['series']}, {'available'})

    def test_get_context_reports_partial_when_cutoff_excludes_one_historical_series(self) -> None:
        service = self._service()
        now = datetime(2025, 1, 3, tzinfo=timezone.utc)
        fetched_before_cutoff = datetime(2025, 1, 1, 12, tzinfo=timezone.utc)
        fetched_after_cutoff = datetime(2026, 6, 19, 12, tzinfo=timezone.utc)
        excluded_series_id = 'NY.GDP.MKTP.KD.ZG'
        for series_id, config in DEFAULT_WORLD_BANK_SERIES.items():
            service.store.upsert_observation(
                source=WORLD_BANK_SOURCE,
                series_id=series_id,
                series_name=config.series_name,
                country_code='KOR',
                observation_period='2024',
                observation_date=date(2024, 12, 31),
                value=2.0,
                unit=config.unit,
                frequency=config.frequency,
                fetched_at=fetched_after_cutoff if series_id == excluded_series_id else fetched_before_cutoff,
                source_status='available',
            )

        context = service.get_context(as_of_date=date(2025, 1, 1), now=now)
        series_by_id = {item['series_id']: item for item in context['series']}

        self.assertEqual(context['status'], 'partial')
        self.assertEqual(context['freshness']['status'], 'partial')
        self.assertTrue(context['freshness']['knowledge_cutoff_applied'])
        self.assertEqual(context['macro_regime_summary']['freshness_label'], 'macro_context_partial')
        self.assertEqual(context['source_statuses'][WORLD_BANK_SOURCE]['status'], 'partial')
        self.assertEqual(context['source_statuses'][WORLD_BANK_SOURCE]['available_count'], 2)
        self.assertEqual(context['source_statuses'][WORLD_BANK_SOURCE]['missing_count'], 1)
        self.assertEqual(series_by_id[excluded_series_id]['status'], 'missing')
        self.assertIsNone(series_by_id[excluded_series_id]['latest_value'])


if __name__ == '__main__':
    unittest.main()
