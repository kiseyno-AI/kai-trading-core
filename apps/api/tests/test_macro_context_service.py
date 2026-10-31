import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

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
from app.macro_context.store import MacroContextObservationRecord, MacroContextObservationStore


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


class EmptyErrorWorldBankClient:
    async def fetch_indicator(self, **kwargs):
        raise RuntimeError()


class EmptyErrorFredClient:
    async def fetch_latest_observations(self, **kwargs):
        raise RuntimeError()


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
        now = datetime(2026, 6, 20, 9, tzinfo=timezone.utc)

        with patch('app.macro_context.service._utcnow', return_value=now):
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

        context = service.get_context(as_of_date='2026-06-20', now=now)

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

    def test_world_bank_failure_reports_retry_contract_with_nonblank_reason(self) -> None:
        settings.macro_context_world_bank_series = 'NY.GDP.MKTP.KD.ZG'
        settings.macro_context_fred_enabled = False
        service = self._service(EmptyErrorWorldBankClient())
        now = datetime(2026, 6, 20, 9, tzinfo=timezone.utc)
        config = DEFAULT_WORLD_BANK_SERIES['NY.GDP.MKTP.KD.ZG']
        service.store.upsert_observation(
            source=WORLD_BANK_SOURCE,
            series_id=config.series_id,
            series_name=config.series_name,
            country_code='KOR',
            observation_period='2024',
            observation_date=date(2024, 12, 31),
            value=2.8,
            unit=config.unit,
            frequency=config.frequency,
            fetched_at=now - timedelta(hours=1),
            source_status='available',
        )

        with patch('app.macro_context.service._utcnow', return_value=now):
            result = service.refresh_context(force=True, trigger='unit_test')

        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['observations_failed'], 1)
        self.assertEqual(
            result['series_statuses'][0],
            {
                'source': WORLD_BANK_SOURCE,
                'series_id': 'NY.GDP.MKTP.KD.ZG',
                'status': 'failed',
                'reason': 'RuntimeError',
                'error_type': 'RuntimeError',
                'retryable': True,
                'action_required': 'automatic_retry_pending',
            },
        )
        self.assertEqual(result['context']['status'], 'available')
        self.assertEqual(result['context']['series'][0]['latest_value'], 2.8)
        latest = service.store.load_latest_by_series(
            series_keys=[(WORLD_BANK_SOURCE, config.series_id)],
            as_of_date=now.date(),
        )
        self.assertEqual(
            latest[(WORLD_BANK_SOURCE, config.series_id)].status_detail,
            f'refresh_failed_at={now.isoformat()}; RuntimeError',
        )

    def test_fred_failure_reports_retry_contract_with_nonblank_reason(self) -> None:
        settings.macro_context_world_bank_enabled = False
        settings.fred_api_key = 'test-fred-key'
        service = MacroContextService(
            store=MacroContextObservationStore(),
            world_bank_client=FakeWorldBankClient(),
            fred_client=EmptyErrorFredClient(),
        )

        result = service.refresh_context(force=True, trigger='unit_test')

        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(result['observations_failed'], 1)
        self.assertEqual(
            result['series_statuses'][0],
            {
                'source': FRED_SOURCE,
                'series_id': 'DEXKOUS',
                'status': 'failed',
                'reason': 'RuntimeError',
                'error_type': 'RuntimeError',
                'retryable': True,
                'action_required': 'automatic_retry_pending',
            },
        )

    def test_get_context_marks_stale_data(self) -> None:
        settings.macro_context_stale_after_hours = 24
        service = self._service()
        now = datetime(2026, 6, 20, 9, tzinfo=timezone.utc)
        old_fetched_at = now - timedelta(days=4)
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

        context = service.get_context(as_of_date='2026-06-20', now=now)

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


class _InMemoryMacroStore:
    def __init__(self) -> None:
        self.records: dict[tuple[str, str], MacroContextObservationRecord] = {}

    def latest_fetched_at(self):
        return max((record.fetched_at for record in self.records.values()), default=None)

    def load_latest_by_series(self, *, series_keys, as_of_date=None, fetched_at_before=None):
        return {
            key: self.records[key]
            for key in series_keys
            if key in self.records
            and (as_of_date is None or self.records[key].observation_date <= as_of_date)
            and (fetched_at_before is None or self.records[key].fetched_at < fetched_at_before)
        }

    def upsert_observation(self, **values):
        observation_date = values['observation_date']
        if isinstance(observation_date, str):
            observation_date = date.fromisoformat(observation_date)
        record = MacroContextObservationRecord(
            id=len(self.records) + 1,
            source=values['source'],
            series_id=values['series_id'],
            series_name=values.get('series_name'),
            country_code=values.get('country_code'),
            observation_period=values['observation_period'],
            observation_date=observation_date,
            value=float(values['value']),
            unit=values.get('unit'),
            frequency=values.get('frequency'),
            fetched_at=values['fetched_at'],
            source_status=values['source_status'],
            status_detail=values.get('status_detail'),
            payload=values.get('payload'),
            created_at=None,
            updated_at=None,
        )
        self.records[(record.source, record.series_id)] = record
        return record

    def mark_refresh_failure(self, *, source, series_id, failed_at, reason):
        key = (source, series_id)
        record = self.records.get(key)
        if record is None:
            return False
        if record.fetched_at >= failed_at:
            return False
        self.records[key] = MacroContextObservationRecord(
            **{
                **record.__dict__,
                'status_detail': (
                    f'refresh_failed_at={failed_at.isoformat()}; {reason}'
                ),
            }
        )
        return True


class _OnceFailingWorldBankClient(FakeWorldBankClient):
    def __init__(self, failed_series_id: str) -> None:
        super().__init__()
        self.failed_series_id = failed_series_id
        self.failed_once = False

    async def fetch_indicator(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs['indicator'] == self.failed_series_id and not self.failed_once:
            self.failed_once = True
            raise RuntimeError('temporary provider error')
        indicator = kwargs['indicator']
        return {
            'metadata': {'source': 'fake-world-bank'},
            'rows': [{'date': '2024', 'value': str(self.values[indicator])}],
            'raw': {},
        }


def test_partial_refresh_retries_only_missing_series_inside_global_cooldown() -> None:
    failed_series_id = 'FP.CPI.TOTL.ZG'
    store = _InMemoryMacroStore()
    world_bank = _OnceFailingWorldBankClient(failed_series_id)
    service = MacroContextService(
        store=store,
        world_bank_client=world_bank,
        fred_client=FailingFredClient(),
    )
    first_now = datetime(2026, 6, 20, 9, tzinfo=timezone.utc)
    second_now = first_now + timedelta(hours=1)

    with (
        patch.object(settings, 'macro_context_enabled', True),
        patch.object(settings, 'macro_context_refresh_min_interval_hours', 24),
        patch.object(settings, 'macro_context_stale_after_hours', 72),
        patch.object(settings, 'macro_context_world_bank_enabled', True),
        patch.object(
            settings,
            'macro_context_world_bank_series',
            'NY.GDP.MKTP.KD.ZG,FP.CPI.TOTL.ZG,SL.UEM.TOTL.ZS',
        ),
        patch.object(settings, 'macro_context_fred_enabled', False),
        patch('app.macro_context.service._utcnow', side_effect=[first_now, second_now]),
    ):
        first = service.refresh_context(force=True, trigger='unit_test')
        calls_after_first = len(world_bank.calls)
        second = service.refresh_context(force=False, trigger='unit_test')

    self_retry_calls = world_bank.calls[calls_after_first:]
    assert first['status'] == 'partial'
    assert second['status'] == 'completed'
    assert second['retry_scope']['missing_or_due_series'] == [[WORLD_BANK_SOURCE, failed_series_id]]
    assert [call['indicator'] for call in self_retry_calls] == [failed_series_id]


def test_partial_refresh_retries_failed_series_with_prior_success_inside_cooldown() -> None:
    failed_series_id = 'FP.CPI.TOTL.ZG'
    store = _InMemoryMacroStore()
    world_bank = _OnceFailingWorldBankClient(failed_series_id)
    world_bank.failed_once = True
    service = MacroContextService(
        store=store,
        world_bank_client=world_bank,
        fred_client=FailingFredClient(),
    )
    first_now = datetime(2026, 6, 20, 9, tzinfo=timezone.utc)
    failed_now = first_now + timedelta(hours=1)
    retry_now = failed_now + timedelta(hours=1)

    with (
        patch.object(settings, 'macro_context_enabled', True),
        patch.object(settings, 'macro_context_refresh_min_interval_hours', 24),
        patch.object(settings, 'macro_context_stale_after_hours', 72),
        patch.object(settings, 'macro_context_world_bank_enabled', True),
        patch.object(
            settings,
            'macro_context_world_bank_series',
            'NY.GDP.MKTP.KD.ZG,FP.CPI.TOTL.ZG,SL.UEM.TOTL.ZS',
        ),
        patch.object(settings, 'macro_context_fred_enabled', False),
        patch(
            'app.macro_context.service._utcnow',
            side_effect=[first_now, failed_now, retry_now],
        ),
    ):
        baseline = service.refresh_context(force=True, trigger='unit_test')
        world_bank.failed_once = False
        partial = service.refresh_context(force=True, trigger='unit_test')
        calls_after_partial = len(world_bank.calls)
        retry = service.refresh_context(force=False, trigger='unit_test')

    retry_calls = world_bank.calls[calls_after_partial:]
    assert baseline['status'] == 'completed'
    assert partial['status'] == 'partial'
    assert retry['status'] == 'completed'
    assert retry['retry_scope']['missing_or_due_series'] == [
        [WORLD_BANK_SOURCE, failed_series_id]
    ]
    assert [call['indicator'] for call in retry_calls] == [failed_series_id]
    assert store.records[(WORLD_BANK_SOURCE, failed_series_id)].status_detail is None
