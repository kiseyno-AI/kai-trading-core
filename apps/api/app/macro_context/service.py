from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import threading
from typing import Any, Sequence

from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.core.audit_logging import exception_message
from app.core.settings import settings
from app.integrations.fred.client import FREDAPIError, FREDClient, FREDConfigurationError
from app.integrations.world_bank.client import WorldBankAPIError, WorldBankClient
from app.macro_context.store import MacroContextObservationRecord, MacroContextObservationStore, macro_context_observation_store

WORLD_BANK_SOURCE = 'world_bank'
FRED_SOURCE = 'fred'
MACRO_CONTEXT_SCHEMA_VERSION = 'macro_context_v1'
REFRESH_FAILURE_STATUS_PREFIX = 'refresh_failed_at='


def _has_pending_refresh_failure(
    record: MacroContextObservationRecord | None,
) -> bool:
    if record is None:
        return False
    return str(record.status_detail or '').startswith(
        REFRESH_FAILURE_STATUS_PREFIX
    )


@dataclass(frozen=True)
class MacroSeriesConfig:
    source: str
    series_id: str
    series_name: str
    country_code: str | None = None
    unit: str | None = None
    frequency: str | None = None
    regime_key: str | None = None


DEFAULT_WORLD_BANK_SERIES: dict[str, MacroSeriesConfig] = {
    'NY.GDP.MKTP.KD.ZG': MacroSeriesConfig(
        source=WORLD_BANK_SOURCE,
        series_id='NY.GDP.MKTP.KD.ZG',
        series_name='Korea real GDP growth',
        country_code='KOR',
        unit='percent',
        frequency='annual',
        regime_key='growth',
    ),
    'FP.CPI.TOTL.ZG': MacroSeriesConfig(
        source=WORLD_BANK_SOURCE,
        series_id='FP.CPI.TOTL.ZG',
        series_name='Korea consumer inflation',
        country_code='KOR',
        unit='percent',
        frequency='annual',
        regime_key='inflation',
    ),
    'SL.UEM.TOTL.ZS': MacroSeriesConfig(
        source=WORLD_BANK_SOURCE,
        series_id='SL.UEM.TOTL.ZS',
        series_name='Korea unemployment rate',
        country_code='KOR',
        unit='percent',
        frequency='annual',
        regime_key='labor',
    ),
}

DEFAULT_FRED_SERIES: dict[str, MacroSeriesConfig] = {
    'DEXKOUS': MacroSeriesConfig(
        source=FRED_SOURCE,
        series_id='DEXKOUS',
        series_name='USD/KRW exchange rate from FRED',
        country_code=None,
        unit='KRW per USD',
        frequency='daily',
        regime_key='fx',
    ),
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _run_coroutine(coro) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            result['value'] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - defensive bridge for non-threadpool callers.
            error['value'] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if 'value' in error:
        raise error['value']
    return result.get('value')


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        normalized = value.strip().replace(',', '')
        if not normalized or normalized == '.':
            return None
        try:
            return float(normalized)
        except ValueError:
            return None
    return None


def _series_list(raw_value: str, defaults: dict[str, MacroSeriesConfig]) -> list[MacroSeriesConfig]:
    series_ids = [item.strip() for item in str(raw_value or '').split(',') if item.strip()]
    if not series_ids:
        return list(defaults.values())
    configs: list[MacroSeriesConfig] = []
    for series_id in series_ids:
        default = defaults.get(series_id)
        if default is not None:
            configs.append(default)
            continue
        configs.append(
            MacroSeriesConfig(
                source=next(iter(defaults.values())).source if defaults else 'unknown',
                series_id=series_id,
                series_name=series_id,
            )
        )
    return configs


def _observation_date_from_period(period: str) -> date:
    normalized = str(period or '').strip()
    if len(normalized) == 4 and normalized.isdigit():
        return date(int(normalized), 12, 31)
    if len(normalized) == 7 and normalized[4] == '-':
        year = int(normalized[:4])
        month = int(normalized[5:7])
        if month == 12:
            return date(year, 12, 31)
        return date(year, month + 1, 1) - timedelta(days=1)
    return date.fromisoformat(normalized[:10])


def _age_hours(fetched_at: datetime | None, *, now: datetime) -> float | None:
    if fetched_at is None:
        return None
    comparable = fetched_at if fetched_at.tzinfo is not None else fetched_at.replace(tzinfo=timezone.utc)
    return round(max((now - comparable.astimezone(timezone.utc)).total_seconds(), 0.0) / 3600.0, 3)


def _historical_knowledge_cutoff_at(as_of_date: date, *, now: datetime) -> datetime | None:
    comparable_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    if as_of_date >= comparable_now.astimezone(timezone.utc).date():
        return None
    return datetime.combine(as_of_date + timedelta(days=1), time.min, tzinfo=timezone.utc)


def _context_status(statuses: Sequence[str]) -> str:
    normalized = [str(status or '').strip().lower() for status in statuses]
    if not normalized or all(status in {'missing', 'unavailable'} for status in normalized):
        return 'unavailable'
    if any(status == 'missing' for status in normalized):
        return 'partial'
    if any(status == 'stale' for status in normalized):
        return 'stale'
    if all(status == 'available' for status in normalized):
        return 'available'
    return 'partial'


def _source_status_from_series(series_items: Sequence[dict[str, Any]]) -> str:
    statuses = [str(item.get('status') or '') for item in series_items]
    return _context_status(statuses)


def _series_item(
    *,
    config: MacroSeriesConfig,
    record: MacroContextObservationRecord | None,
    now: datetime,
    stale_after_hours: int,
) -> dict[str, Any]:
    if record is None:
        return {
            'source': config.source,
            'series_id': config.series_id,
            'series_name': config.series_name,
            'status': 'missing',
            'source_status': 'missing_source_row',
            'latest_value': None,
            'latest_period': None,
            'observation_date': None,
            'unit': config.unit,
            'frequency': config.frequency,
            'freshness': {
                'fetched_at': None,
                'age_hours': None,
                'stale_after_hours': stale_after_hours,
                'freshness_status': 'missing',
            },
        }

    age = _age_hours(record.fetched_at, now=now)
    stale = age is not None and age > stale_after_hours
    status = 'stale' if stale else 'available'
    return {
        'source': record.source,
        'series_id': record.series_id,
        'series_name': record.series_name or config.series_name,
        'status': status,
        'source_status': record.source_status,
        'latest_value': record.value,
        'latest_period': record.observation_period,
        'observation_date': record.observation_date.isoformat(),
        'unit': record.unit or config.unit,
        'frequency': record.frequency or config.frequency,
        'freshness': {
            'fetched_at': record.fetched_at.isoformat(),
            'age_hours': age,
            'stale_after_hours': stale_after_hours,
            'freshness_status': 'stale' if stale else 'fresh',
        },
    }


def _label_growth(value: float | None) -> str:
    if value is None:
        return 'growth_unknown'
    if value >= 2.5:
        return 'growth_expansion'
    if value <= 1.0:
        return 'growth_slowdown'
    return 'growth_steady'


def _label_inflation(value: float | None) -> str:
    if value is None:
        return 'inflation_unknown'
    if value >= 3.0:
        return 'inflation_pressure'
    if value <= 1.0:
        return 'inflation_soft'
    return 'inflation_stable'


def _label_labor(value: float | None) -> str:
    if value is None:
        return 'labor_unknown'
    if value >= 4.5:
        return 'labor_slack'
    if value <= 3.0:
        return 'labor_tight'
    return 'labor_balanced'


def _label_fx(value: float | None) -> str:
    if value is None:
        return 'fx_unknown'
    if value >= 1400.0:
        return 'usdkrw_elevated'
    if value <= 1200.0:
        return 'usdkrw_low'
    return 'usdkrw_normal'


def _composite_label(*, growth_label: str, inflation_label: str, labor_label: str) -> str:
    if growth_label == 'growth_slowdown' and inflation_label == 'inflation_pressure':
        return 'macro_stagflation_watch'
    if growth_label == 'growth_expansion' and inflation_label in {'inflation_stable', 'inflation_soft'}:
        return 'macro_growth_supportive'
    if growth_label == 'growth_slowdown':
        return 'macro_growth_risk'
    if inflation_label == 'inflation_pressure':
        return 'macro_inflation_watch'
    if labor_label == 'labor_slack':
        return 'macro_labor_softness'
    return 'macro_balanced'


def _build_regime_summary(series_by_key: dict[str, dict[str, Any]], *, context_status: str) -> dict[str, Any]:
    growth = _to_float(series_by_key.get('growth', {}).get('latest_value'))
    inflation = _to_float(series_by_key.get('inflation', {}).get('latest_value'))
    labor = _to_float(series_by_key.get('labor', {}).get('latest_value'))
    fx = _to_float(series_by_key.get('fx', {}).get('latest_value'))
    growth_label = _label_growth(growth)
    inflation_label = _label_inflation(inflation)
    labor_label = _label_labor(labor)
    return {
        'schema_version': 'macro_regime_summary_v1',
        'method': 'deterministic_threshold_labels',
        'growth_label': growth_label,
        'inflation_label': inflation_label,
        'labor_label': labor_label,
        'fx_label': _label_fx(fx),
        'composite_label': _composite_label(
            growth_label=growth_label,
            inflation_label=inflation_label,
            labor_label=labor_label,
        ),
        'freshness_label': (
            'macro_context_stale'
            if context_status == 'stale'
            else 'macro_context_partial'
            if context_status == 'partial'
            else 'macro_context_unavailable'
            if context_status == 'unavailable'
            else 'macro_context_fresh'
        ),
        'llm_generated': False,
    }


class MacroContextService:
    def __init__(
        self,
        *,
        store: MacroContextObservationStore | None = None,
        world_bank_client: WorldBankClient | None = None,
        fred_client: FREDClient | None = None,
    ) -> None:
        self.store = store or macro_context_observation_store
        self.world_bank_client = world_bank_client or WorldBankClient()
        self.fred_client = fred_client or FREDClient()

    def refresh_context(
        self,
        *,
        force: bool = False,
        trigger: str = 'manual',
    ) -> dict[str, Any]:
        now = _utcnow()
        if not settings.macro_context_enabled:
            return {
                'status': 'skipped',
                'reason': 'macro_context_disabled',
                'trigger': trigger,
                'source_statuses': {
                    'macro_context': {
                        'status': 'disabled',
                        'reason': 'MACRO_CONTEXT_ENABLED=false',
                    }
                },
                'observations_upserted': 0,
                'observations_failed': 0,
                'context': self.get_context(now=now),
            }

        latest_fetched_at = self.store.latest_fetched_at()
        min_interval = timedelta(hours=max(0, int(settings.macro_context_refresh_min_interval_hours or 0)))
        retry_series_keys: set[tuple[str, str]] | None = None
        retry_scope: dict[str, Any] | None = None
        if not force and latest_fetched_at is not None and min_interval.total_seconds() > 0:
            comparable = latest_fetched_at if latest_fetched_at.tzinfo else latest_fetched_at.replace(tzinfo=timezone.utc)
            if now - comparable.astimezone(timezone.utc) < min_interval:
                refresh_configs = self._enabled_refresh_series()
                latest_by_series = self.store.load_latest_by_series(
                    series_keys=[(config.source, config.series_id) for config in refresh_configs],
                    as_of_date=now.date(),
                )
                cutoff = now - min_interval
                retry_series_keys = {
                    (config.source, config.series_id)
                    for config in refresh_configs
                    if (
                        (record := latest_by_series.get((config.source, config.series_id))) is None
                        or _has_pending_refresh_failure(record)
                        or (
                            record.fetched_at
                            if record.fetched_at.tzinfo is not None
                            else record.fetched_at.replace(tzinfo=timezone.utc)
                        ).astimezone(timezone.utc)
                        <= cutoff
                    )
                }
                if not retry_series_keys:
                    return {
                        'status': 'skipped',
                        'reason': 'macro_context_refresh_not_due',
                        'trigger': trigger,
                        'latest_fetched_at': latest_fetched_at.isoformat(),
                        'next_due_at': (comparable.astimezone(timezone.utc) + min_interval).isoformat(),
                        'observations_upserted': 0,
                        'observations_failed': 0,
                        'source_statuses': self._configured_source_statuses(),
                        'context': self.get_context(now=now),
                    }
                retry_scope = {
                    'mode': 'missing_or_due_series',
                    'missing_or_due_series': [list(key) for key in sorted(retry_series_keys)],
                    'recent_series_skipped': len(refresh_configs) - len(retry_series_keys),
                }

        source_result = _run_coroutine(
            self._refresh_sources(now=now, only_series_keys=retry_series_keys)
        )
        observations_upserted = int(source_result.get('observations_upserted') or 0)
        observations_failed = int(source_result.get('observations_failed') or 0)
        context = self.get_context(now=now)
        if observations_failed == 0 and context.get('status') == 'available':
            status = 'completed'
        elif observations_upserted > 0 or context.get('status') in {'partial', 'stale'}:
            status = 'partial'
        else:
            status = 'unavailable'
        result = {
            'status': status,
            'trigger': trigger,
            'fetched_at': now.isoformat(),
            'observations_upserted': observations_upserted,
            'observations_failed': observations_failed,
            'source_statuses': source_result.get('source_statuses') or {},
            'series_statuses': source_result.get('series_statuses') or [],
            'context': context,
        }
        if retry_scope is not None:
            result['retry_scope'] = retry_scope
        return result

    def get_context(
        self,
        *,
        as_of_date: str | date | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        resolved_now = now or _utcnow()
        resolved_as_of = self._normalize_as_of_date(as_of_date, now=resolved_now)
        knowledge_cutoff_at = _historical_knowledge_cutoff_at(resolved_as_of, now=resolved_now)
        stale_after_hours = max(1, int(settings.macro_context_stale_after_hours or 72))
        configs = self._context_series()
        try:
            latest = self.store.load_latest_by_series(
                series_keys=[(config.source, config.series_id) for config in configs],
                as_of_date=resolved_as_of,
                fetched_at_before=knowledge_cutoff_at,
            )
        except IntegrityError:
            raise
        except SQLAlchemyError as exc:
            return self._unavailable_context(
                as_of_date=resolved_as_of,
                reason='macro_context_storage_unavailable',
                detail=str(exc),
                now=resolved_now,
            )

        series_items: list[dict[str, Any]] = []
        series_by_regime_key: dict[str, dict[str, Any]] = {}
        for config in configs:
            item = _series_item(
                config=config,
                record=latest.get((config.source, config.series_id)),
                now=resolved_now,
                stale_after_hours=stale_after_hours,
            )
            series_items.append(item)
            if config.regime_key:
                series_by_regime_key.setdefault(config.regime_key, item)

        status = _context_status([item['status'] for item in series_items])
        latest_fetched_at = max(
            (
                item['freshness']['fetched_at']
                for item in series_items
                if item.get('freshness') and item['freshness'].get('fetched_at')
            ),
            default=None,
        )
        source_statuses = self._configured_source_statuses()
        for source in sorted({item['source'] for item in series_items}):
            source_items = [item for item in series_items if item['source'] == source]
            source_statuses[source] = {
                **source_statuses.get(source, {}),
                'status': _source_status_from_series(source_items),
                'series_count': len(source_items),
                'available_count': sum(1 for item in source_items if item['status'] == 'available'),
                'stale_count': sum(1 for item in source_items if item['status'] == 'stale'),
                'missing_count': sum(1 for item in source_items if item['status'] == 'missing'),
            }
        return {
            'schema_version': MACRO_CONTEXT_SCHEMA_VERSION,
            'status': status,
            'as_of_date': resolved_as_of.isoformat(),
            'generated_at': resolved_now.isoformat(),
            'latest_fetched_at': latest_fetched_at,
            'freshness': {
                'stale_after_hours': stale_after_hours,
                'status': 'stale' if status == 'stale' else 'fresh' if status == 'available' else status,
                'knowledge_cutoff_at': knowledge_cutoff_at.isoformat() if knowledge_cutoff_at else None,
                'knowledge_cutoff_applied': knowledge_cutoff_at is not None,
            },
            'source_statuses': source_statuses,
            'series': series_items,
            'macro_regime_summary': _build_regime_summary(series_by_regime_key, context_status=status),
            'usage_policy': {
                'contextual_input_only': True,
                'direct_buy_sell_signal': False,
                'llm_generated': False,
            },
        }

    def count_for_status(self, *, market_date: str | date) -> dict[str, Any]:
        now = _utcnow()
        resolved_market_date = self._normalize_as_of_date(market_date, now=now)
        knowledge_cutoff_at = _historical_knowledge_cutoff_at(resolved_market_date, now=now)
        return self.store.count_for_date(
            as_of_date=resolved_market_date,
            fetched_at_before=knowledge_cutoff_at,
        )

    async def _refresh_sources(
        self,
        *,
        now: datetime,
        only_series_keys: set[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        source_statuses: dict[str, dict[str, Any]] = {}
        series_statuses: list[dict[str, Any]] = []
        observations_upserted = 0
        observations_failed = 0

        world_bank_configs = self._world_bank_series()
        fred_configs = self._fred_series()
        if only_series_keys is not None:
            world_bank_configs = [
                config
                for config in world_bank_configs
                if (config.source, config.series_id) in only_series_keys
            ]
            if not (
                settings.macro_context_fred_enabled
                and not settings.fred_api_key.strip()
            ):
                fred_configs = [
                    config
                    for config in fred_configs
                    if (config.source, config.series_id) in only_series_keys
                ]

        world_bank_result = await self._refresh_world_bank(now=now, configs=world_bank_configs)
        source_statuses[WORLD_BANK_SOURCE] = world_bank_result['source_status']
        series_statuses.extend(world_bank_result['series_statuses'])
        observations_upserted += int(world_bank_result['observations_upserted'])
        observations_failed += int(world_bank_result['observations_failed'])

        fred_result = await self._refresh_fred(now=now, configs=fred_configs)
        source_statuses[FRED_SOURCE] = fred_result['source_status']
        series_statuses.extend(fred_result['series_statuses'])
        observations_upserted += int(fred_result['observations_upserted'])
        observations_failed += int(fred_result['observations_failed'])

        return {
            'source_statuses': source_statuses,
            'series_statuses': series_statuses,
            'observations_upserted': observations_upserted,
            'observations_failed': observations_failed,
        }

    async def _refresh_world_bank(
        self,
        *,
        now: datetime,
        configs: Sequence[MacroSeriesConfig] | None = None,
    ) -> dict[str, Any]:
        configs = list(self._world_bank_series() if configs is None else configs)
        if not settings.macro_context_world_bank_enabled:
            return {
                'source_status': {
                    'status': 'disabled',
                    'reason': 'MACRO_CONTEXT_WORLD_BANK_ENABLED=false',
                    'observations_upserted': 0,
                    'observations_failed': 0,
                },
                'series_statuses': [],
                'observations_upserted': 0,
                'observations_failed': 0,
            }

        series_statuses: list[dict[str, Any]] = []
        observations_upserted = 0
        observations_failed = 0
        for config in configs:
            try:
                payload = await self.world_bank_client.fetch_indicator(
                    country=config.country_code or settings.macro_context_world_bank_country,
                    indicator=config.series_id,
                    mrnev=settings.macro_context_world_bank_mrnev,
                )
                row = self._select_first_numeric_world_bank_row(payload.get('rows') or [])
                if row is None:
                    raise WorldBankAPIError('World Bank returned no numeric observations')
                value = _to_float(row.get('value'))
                if value is None:
                    raise WorldBankAPIError('World Bank returned an empty latest value')
                period = str(row.get('date') or '').strip()
                if not period:
                    raise WorldBankAPIError('World Bank returned an empty observation period')
                record = self.store.upsert_observation(
                    source=WORLD_BANK_SOURCE,
                    series_id=config.series_id,
                    series_name=config.series_name,
                    country_code=config.country_code or settings.macro_context_world_bank_country,
                    observation_period=period,
                    observation_date=_observation_date_from_period(period),
                    value=value,
                    unit=config.unit,
                    frequency=config.frequency,
                    fetched_at=now,
                    source_status='available',
                    status_detail=None,
                    payload={
                        'provider': WORLD_BANK_SOURCE,
                        'metadata': payload.get('metadata') or {},
                        'raw_observation': row,
                    },
                )
                observations_upserted += 1
                series_statuses.append(
                    {
                        'source': WORLD_BANK_SOURCE,
                        'series_id': config.series_id,
                        'status': 'available',
                        'observation_period': record.observation_period,
                        'observation_date': record.observation_date.isoformat(),
                        'fetched_at': record.fetched_at.isoformat(),
                    }
                )
            except IntegrityError:
                raise
            except Exception as exc:  # noqa: BLE001 - source failures must not abort the refresh.
                observations_failed += 1
                reason = exception_message(exc)
                try:
                    self.store.mark_refresh_failure(
                        source=WORLD_BANK_SOURCE,
                        series_id=config.series_id,
                        failed_at=now,
                        reason=reason,
                    )
                except IntegrityError:
                    raise
                except Exception:  # noqa: BLE001 - retain the provider failure as the primary result.
                    pass
                series_statuses.append(
                    {
                        'source': WORLD_BANK_SOURCE,
                        'series_id': config.series_id,
                        'status': 'failed',
                        'reason': reason,
                        'error_type': type(exc).__name__,
                        'retryable': True,
                        'action_required': 'automatic_retry_pending',
                    }
                )

        if observations_upserted == len(configs) and observations_failed == 0:
            status = 'available'
        elif observations_upserted > 0:
            status = 'partial'
        else:
            status = 'unavailable'
        return {
            'source_status': {
                'status': status,
                'series_count': len(configs),
                'observations_upserted': observations_upserted,
                'observations_failed': observations_failed,
            },
            'series_statuses': series_statuses,
            'observations_upserted': observations_upserted,
            'observations_failed': observations_failed,
        }

    async def _refresh_fred(
        self,
        *,
        now: datetime,
        configs: Sequence[MacroSeriesConfig] | None = None,
    ) -> dict[str, Any]:
        configs = list(self._fred_series() if configs is None else configs)
        if not settings.macro_context_fred_enabled:
            return self._fred_disabled_result(status='disabled', reason='MACRO_CONTEXT_FRED_ENABLED=false')
        if not settings.fred_api_key.strip():
            return self._fred_disabled_result(
                status='blocked_missing_credential',
                reason='FRED_API_KEY is not configured',
                configs=configs,
                configuration_blocked=True,
            )
        if not configs:
            return self._fred_disabled_result(status='disabled_no_series', reason='MACRO_CONTEXT_FRED_SERIES is empty')

        series_statuses: list[dict[str, Any]] = []
        observations_upserted = 0
        observations_failed = 0
        for config in configs:
            try:
                payload = await self.fred_client.fetch_latest_observations(series_id=config.series_id, limit=5)
                row = self._select_first_numeric_fred_row(payload.get('observations') or [])
                if row is None:
                    raise FREDAPIError('FRED returned no numeric observations')
                value = _to_float(row.get('value'))
                if value is None:
                    raise FREDAPIError('FRED returned an empty latest value')
                period = str(row.get('date') or '').strip()
                if not period:
                    raise FREDAPIError('FRED returned an empty observation date')
                record = self.store.upsert_observation(
                    source=FRED_SOURCE,
                    series_id=config.series_id,
                    series_name=config.series_name,
                    country_code=config.country_code,
                    observation_period=period,
                    observation_date=_observation_date_from_period(period),
                    value=value,
                    unit=config.unit,
                    frequency=config.frequency,
                    fetched_at=now,
                    source_status='available',
                    payload={'provider': FRED_SOURCE, 'raw_observation': row},
                )
                observations_upserted += 1
                series_statuses.append(
                    {
                        'source': FRED_SOURCE,
                        'series_id': config.series_id,
                        'status': 'available',
                        'observation_period': record.observation_period,
                        'observation_date': record.observation_date.isoformat(),
                        'fetched_at': record.fetched_at.isoformat(),
                    }
                )
            except FREDConfigurationError as exc:
                return self._fred_disabled_result(
                    status='blocked_missing_credential',
                    reason=exception_message(exc),
                    configs=configs,
                    configuration_blocked=True,
                )
            except IntegrityError:
                raise
            except Exception as exc:  # noqa: BLE001 - source failures must not abort the refresh.
                observations_failed += 1
                reason = exception_message(exc)
                try:
                    self.store.mark_refresh_failure(
                        source=FRED_SOURCE,
                        series_id=config.series_id,
                        failed_at=now,
                        reason=reason,
                    )
                except IntegrityError:
                    raise
                except Exception:  # noqa: BLE001 - retain the provider failure as the primary result.
                    pass
                series_statuses.append(
                    {
                        'source': FRED_SOURCE,
                        'series_id': config.series_id,
                        'status': 'failed',
                        'reason': reason,
                        'error_type': type(exc).__name__,
                        'retryable': True,
                        'action_required': 'automatic_retry_pending',
                    }
                )

        status = 'available' if observations_upserted == len(configs) and observations_failed == 0 else 'partial' if observations_upserted > 0 else 'unavailable'
        return {
            'source_status': {
                'status': status,
                'series_count': len(configs),
                'observations_upserted': observations_upserted,
                'observations_failed': observations_failed,
            },
            'series_statuses': series_statuses,
            'observations_upserted': observations_upserted,
            'observations_failed': observations_failed,
        }

    @staticmethod
    def _fred_disabled_result(
        *,
        status: str,
        reason: str,
        configs: Sequence[MacroSeriesConfig] | None = None,
        configuration_blocked: bool = False,
    ) -> dict[str, Any]:
        configured_series = list(configs or [])
        observations_failed = (
            len(configured_series) if configuration_blocked else 0
        )
        series_statuses = [
            {
                'source': FRED_SOURCE,
                'series_id': config.series_id,
                'status': status,
                'reason': reason,
                **(
                    {
                        'retryable': False,
                        'action_required': 'configure_fred_api_key',
                    }
                    if configuration_blocked
                    else {}
                ),
            }
            for config in configured_series
        ]
        if not series_statuses:
            series_statuses = [
                {
                    'source': FRED_SOURCE,
                    'series_id': None,
                    'status': status,
                    'reason': reason,
                }
            ]
        return {
            'source_status': {
                'status': status,
                'reason': reason,
                'series_count': len(configured_series),
                'observations_upserted': 0,
                'observations_failed': observations_failed,
                **(
                    {
                        'retryable': False,
                        'action_required': 'configure_fred_api_key',
                    }
                    if configuration_blocked
                    else {}
                ),
            },
            'series_statuses': series_statuses,
            'observations_upserted': 0,
            'observations_failed': observations_failed,
        }

    @staticmethod
    def _select_first_numeric_world_bank_row(rows: Sequence[dict[str, Any]]) -> dict[str, Any] | None:
        for row in rows:
            if isinstance(row, dict) and _to_float(row.get('value')) is not None:
                return row
        return None

    @staticmethod
    def _select_first_numeric_fred_row(rows: Sequence[dict[str, Any]]) -> dict[str, Any] | None:
        for row in rows:
            if isinstance(row, dict) and _to_float(row.get('value')) is not None:
                return row
        return None

    def _configured_source_statuses(self) -> dict[str, dict[str, Any]]:
        statuses: dict[str, dict[str, Any]] = {}
        statuses[WORLD_BANK_SOURCE] = {
            'status': 'configured' if settings.macro_context_world_bank_enabled else 'disabled',
            'country': settings.macro_context_world_bank_country,
        }
        if not settings.macro_context_fred_enabled:
            fred_status = 'disabled'
            reason = 'MACRO_CONTEXT_FRED_ENABLED=false'
        elif not settings.fred_api_key.strip():
            fred_status = 'blocked_missing_credential'
            reason = 'FRED_API_KEY is not configured'
        else:
            fred_status = 'configured'
            reason = None
        statuses[FRED_SOURCE] = {
            'status': fred_status,
            'reason': reason,
            **(
                {
                    'retryable': False,
                    'action_required': 'configure_fred_api_key',
                }
                if fred_status == 'blocked_missing_credential'
                else {}
            ),
        }
        return statuses

    def _world_bank_series(self) -> list[MacroSeriesConfig]:
        return _series_list(settings.macro_context_world_bank_series, DEFAULT_WORLD_BANK_SERIES)

    def _fred_series(self) -> list[MacroSeriesConfig]:
        return _series_list(settings.macro_context_fred_series, DEFAULT_FRED_SERIES)

    def _context_series(self) -> list[MacroSeriesConfig]:
        configs = self._world_bank_series()
        if settings.macro_context_fred_enabled and settings.fred_api_key.strip():
            configs.extend(self._fred_series())
        return configs

    def _enabled_refresh_series(self) -> list[MacroSeriesConfig]:
        configs: list[MacroSeriesConfig] = []
        if settings.macro_context_world_bank_enabled:
            configs.extend(self._world_bank_series())
        if settings.macro_context_fred_enabled and settings.fred_api_key.strip():
            configs.extend(self._fred_series())
        return configs

    @staticmethod
    def _normalize_as_of_date(value: str | date | None, *, now: datetime) -> date:
        if isinstance(value, date):
            return value
        if isinstance(value, str) and value.strip():
            return date.fromisoformat(value.strip()[:10])
        return now.date()

    @staticmethod
    def _unavailable_context(
        *,
        as_of_date: date,
        reason: str,
        detail: str | None,
        now: datetime,
    ) -> dict[str, Any]:
        return {
            'schema_version': MACRO_CONTEXT_SCHEMA_VERSION,
            'status': 'unavailable',
            'as_of_date': as_of_date.isoformat(),
            'generated_at': now.isoformat(),
            'latest_fetched_at': None,
            'freshness': {
                'stale_after_hours': settings.macro_context_stale_after_hours,
                'status': 'unavailable',
                'reason': reason,
                'detail': detail,
            },
            'source_statuses': {
                'macro_context': {
                    'status': 'unavailable',
                    'reason': reason,
                }
            },
            'series': [],
            'macro_regime_summary': {
                'schema_version': 'macro_regime_summary_v1',
                'method': 'deterministic_threshold_labels',
                'growth_label': 'growth_unknown',
                'inflation_label': 'inflation_unknown',
                'labor_label': 'labor_unknown',
                'fx_label': 'fx_unknown',
                'composite_label': 'macro_unknown',
                'freshness_label': 'macro_context_unavailable',
                'llm_generated': False,
            },
            'usage_policy': {
                'contextual_input_only': True,
                'direct_buy_sell_signal': False,
                'llm_generated': False,
            },
        }


macro_context_service = MacroContextService()
