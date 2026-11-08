from __future__ import annotations

import asyncio
import copy
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable

from app.core.settings import settings
from app.market.clock import async_get_market_state
from app.orders.account_inquiry_service import (
    TradingAccountInquiryError,
    TradingAccountInquiryValidationError,
    trading_account_inquiry_service,
)
from app.orders.account_scope import parse_kis_account_reference
from app.orders.execution_handoff_contract import (
    PORTFOLIO_SUMMARY_READ_MODEL_SOURCE,
    serialize_portfolio_risk_equivalent_payload,
)
from app.orders.service import trading_order_service

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _to_number(value: Decimal | None) -> int | float | None:
    if value is None:
        return None
    if value == value.to_integral():
        return int(value)
    return float(value)


def _to_percent(value: Any) -> float | None:
    decimal_value = _safe_decimal(value)
    if decimal_value is None:
        return None
    return float(decimal_value * Decimal('100'))


def _coalesce_non_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _latest_timestamp(*values: str | None) -> str | None:
    latest_value: str | None = None
    latest_timestamp: float | None = None
    for value in values:
        if not value:
            continue
        normalized = value.strip().replace('Z', '+00:00')
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            continue
        timestamp = parsed.timestamp()
        if latest_timestamp is None or timestamp > latest_timestamp:
            latest_timestamp = timestamp
            latest_value = value
    return latest_value


@dataclass(slots=True)
class ConfiguredPortfolioAccount:
    account_reference: str
    label: str
    account_number: str
    account_product_code: str


@dataclass(frozen=True)
class PortfolioSummaryRuntimeStatus:
    ready: bool
    app_env: str
    kis_environment: str
    kis_base_url: str
    kis_use_mock: bool
    kis_app_key_configured: bool
    kis_app_secret_configured: bool
    portfolio_account_references_configured: bool
    portfolio_account_references_valid: bool
    configured_account_count: int
    blocking_reasons: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            'ready': self.ready,
            'app_env': self.app_env,
            'kis_environment': self.kis_environment,
            'kis_base_url': self.kis_base_url,
            'kis_use_mock': self.kis_use_mock,
            'kis_app_key_configured': self.kis_app_key_configured,
            'kis_app_secret_configured': self.kis_app_secret_configured,
            'portfolio_account_references_configured': self.portfolio_account_references_configured,
            'portfolio_account_references_valid': self.portfolio_account_references_valid,
            'configured_account_count': self.configured_account_count,
            'blocking_reasons': list(self.blocking_reasons),
        }


@dataclass(slots=True)
class PortfolioSummaryCacheRecord:
    payload: dict[str, Any]
    as_of: str | None
    cached_at: datetime
    last_refresh_attempted_at: datetime | None
    last_refresh_succeeded_at: datetime | None
    last_refresh_status: str
    last_refresh_error: str | None
    refresh_trigger: str | None


@dataclass(slots=True)
class PortfolioSummaryRefreshOutcome:
    cache_record: PortfolioSummaryCacheRecord | None
    served_from_cache: bool
    uncached_payload: dict[str, Any] | None = None


class PortfolioSummaryService:
    def __init__(
        self,
        *,
        account_inquiry_service: Any | None = None,
        order_service: Any | None = None,
        portfolio_account_references: str | None = None,
        cache_ttl_seconds: int | None = None,
        market_state_loader: Callable[[], Awaitable[Any]] | None = None,
    ) -> None:
        self.account_inquiry_service = account_inquiry_service or trading_account_inquiry_service
        self.order_service = order_service or trading_order_service
        self.portfolio_account_references = portfolio_account_references
        configured_ttl = (
            settings.portfolio_summary_cache_ttl_seconds
            if cache_ttl_seconds is None
            else cache_ttl_seconds
        )
        self.cache_ttl_seconds = max(int(configured_ttl), 0)
        self.market_state_loader = market_state_loader or async_get_market_state
        self._cache: PortfolioSummaryCacheRecord | None = None
        self._refresh_lock = asyncio.Lock()
        self._refresh_task: asyncio.Task | None = None
        self._active_refresh_count = 0

    def runtime_status(self) -> dict[str, Any]:
        payload = self._runtime_status().to_payload()
        payload['cache'] = self._build_cache_status_payload(self._cache)
        return payload

    async def get_portfolio_summary(
        self,
        *,
        refresh: bool = False,
        trigger: str = 'route_read',
    ) -> dict[str, Any]:
        if refresh:
            return await self.refresh_portfolio_summary_if_due(trigger=f'{trigger}:refresh', force=True)

        cache_record = self._cache
        if cache_record is None:
            return await self.refresh_portfolio_summary_if_due(trigger=trigger, force=True)
        if self._refresh_is_due(cache_record):
            try:
                market_state = await self.market_state_loader()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    'Portfolio summary market-state check failed; serving cached data: %s',
                    exc,
                )
                return self._decorate_cached_payload(
                    cache_record,
                    served_from_cache=True,
                    stale_reason_override='market_state_unavailable_cache',
                    market_state='UNKNOWN',
                )
            normalized_market_state = str(
                getattr(market_state, 'state', '') or ''
            ).strip().upper()
            if normalized_market_state in {'WEEKEND', 'HOLIDAY'}:
                return self._decorate_cached_payload(
                    cache_record,
                    served_from_cache=True,
                    stale_reason_override='market_closed_cache',
                    market_state=normalized_market_state,
                )
            self._ensure_background_refresh(trigger=f'{trigger}:stale_cache')
        return self._decorate_cached_payload(cache_record, served_from_cache=True)

    async def refresh_portfolio_summary_if_due(
        self,
        *,
        trigger: str,
        force: bool = False,
    ) -> dict[str, Any]:
        async with self._refresh_lock:
            cache_record = self._cache
            if cache_record is not None and not force and not self._refresh_is_due(cache_record):
                return self._decorate_cached_payload(cache_record, served_from_cache=True)
            if self._runtime_status().ready:
                guarded_payload = await self._guard_refresh_by_market_state(
                    cache_record=cache_record,
                )
                if guarded_payload is not None:
                    return guarded_payload
            self._active_refresh_count += 1
            try:
                outcome = await self._refresh_portfolio_summary(
                    cache_record=cache_record,
                    trigger=trigger,
                )
            finally:
                self._active_refresh_count -= 1

            if outcome.cache_record is not None:
                return self._decorate_cached_payload(
                    outcome.cache_record,
                    served_from_cache=outcome.served_from_cache,
                )
            if outcome.uncached_payload is None:
                raise RuntimeError('Portfolio summary refresh completed without a result.')
            return self._decorate_uncached_payload(outcome.uncached_payload)

    async def _guard_refresh_by_market_state(
        self,
        *,
        cache_record: PortfolioSummaryCacheRecord | None,
    ) -> dict[str, Any] | None:
        attempted_at = _utc_now()
        try:
            market_state = await self.market_state_loader()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                'Portfolio summary market-state check failed; refresh deferred: %s',
                exc,
            )
            if cache_record is not None:
                return self._decorate_cached_payload(
                    cache_record,
                    served_from_cache=True,
                    stale_reason_override='market_state_unavailable_cache',
                    market_state='UNKNOWN',
                )
            message = (
                'Portfolio summary refresh was deferred because market state '
                'is unavailable.'
            )
            freshness = self._build_uncached_freshness_payload(
                attempted_at=attempted_at,
                last_refresh_status='error',
                last_refresh_error=message,
            )
            freshness['marketState'] = 'UNKNOWN'
            return self._decorate_uncached_payload(
                self._build_unavailable_summary(
                    as_of=attempted_at.isoformat(),
                    status='unavailable',
                    source='portfolio_summary_market_state',
                    message=message,
                    freshness=freshness,
                )
            )

        normalized_market_state = str(
            getattr(market_state, 'state', '') or ''
        ).strip().upper()
        if normalized_market_state not in {'WEEKEND', 'HOLIDAY'}:
            return None
        if cache_record is not None:
            return self._decorate_cached_payload(
                cache_record,
                served_from_cache=True,
                stale_reason_override='market_closed_cache',
                market_state=normalized_market_state,
            )

        message = (
            'Portfolio summary refresh is deferred while the market is closed.'
        )
        freshness = self._build_uncached_freshness_payload(
            attempted_at=attempted_at,
            last_refresh_status='degraded',
            last_refresh_error=None,
        )
        freshness['marketState'] = normalized_market_state
        return self._decorate_uncached_payload(
            self._build_unavailable_summary(
                as_of=attempted_at.isoformat(),
                status='unavailable',
                source='portfolio_summary_market_state',
                message=message,
                freshness=freshness,
            )
        )

    def _ensure_background_refresh(self, *, trigger: str) -> None:
        if self._refresh_task is not None and not self._refresh_task.done():
            return
        self._refresh_task = asyncio.create_task(
            self.refresh_portfolio_summary_if_due(trigger=trigger, force=False),
            name='portfolio-summary-refresh',
        )
        self._refresh_task.add_done_callback(self._finalize_refresh_task)

    def _finalize_refresh_task(self, task: asyncio.Task) -> None:
        if self._refresh_task is task:
            self._refresh_task = None
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.warning('Portfolio summary background refresh failed: %s', exc)

    async def _refresh_portfolio_summary(
        self,
        *,
        cache_record: PortfolioSummaryCacheRecord | None,
        trigger: str,
    ) -> PortfolioSummaryRefreshOutcome:
        attempted_at = _utc_now()
        runtime_status = self._runtime_status()
        if not runtime_status.ready:
            message = self._configuration_message(runtime_status)
            if cache_record is not None:
                cached = self._store_refresh_failure(
                    cache_record=cache_record,
                    attempted_at=attempted_at,
                    error=message,
                    trigger=trigger,
                )
                return PortfolioSummaryRefreshOutcome(
                    cache_record=cached,
                    served_from_cache=True,
                )
            return PortfolioSummaryRefreshOutcome(
                cache_record=None,
                served_from_cache=False,
                uncached_payload=self._build_unavailable_summary(
                    as_of=attempted_at.isoformat(),
                    status='not_configured',
                    source='portfolio_summary_runtime',
                    message=message,
                    freshness=self._build_uncached_freshness_payload(
                        attempted_at=attempted_at,
                        last_refresh_status='error',
                        last_refresh_error=message,
                    ),
                ),
            )

        try:
            payload = await self._build_live_portfolio_summary()
        except Exception as exc:
            if cache_record is not None:
                cached = self._store_refresh_failure(
                    cache_record=cache_record,
                    attempted_at=attempted_at,
                    error=str(exc),
                    trigger=trigger,
                )
                return PortfolioSummaryRefreshOutcome(
                    cache_record=cached,
                    served_from_cache=True,
                )
            raise

        refresh_status, refresh_error = self._classify_refresh_result(payload)
        refreshed_record = PortfolioSummaryCacheRecord(
            payload=copy.deepcopy(payload),
            as_of=str(payload.get('asOf') or '') or None,
            cached_at=attempted_at,
            last_refresh_attempted_at=attempted_at,
            last_refresh_succeeded_at=attempted_at,
            last_refresh_status=refresh_status,
            last_refresh_error=refresh_error,
            refresh_trigger=trigger,
        )
        self._cache = refreshed_record
        return PortfolioSummaryRefreshOutcome(
            cache_record=refreshed_record,
            served_from_cache=False,
        )

    async def _build_live_portfolio_summary(self) -> dict[str, Any]:
        as_of = _utc_now().isoformat()
        configured_accounts, _ = self._resolve_accounts()

        results = await asyncio.gather(
            *(self._build_account_payload(account, as_of=as_of) for account in configured_accounts)
        )
        accounts = [item['account'] for item in results]
        notices = [notice for item in results for notice in item['notices']]
        successful_accounts = [item for item in results if item['status'] == 'synced']
        degraded_accounts = [item for item in results if item['status'] != 'synced']
        configured_failures = [item for item in results if item['configured_failure']]

        sync_status = 'synced'
        sync_message: str | None = None
        if configured_failures and not successful_accounts:
            sync_status = 'not_configured'
            sync_message = 'Portfolio account sync is not configured for the current deployment.'
        elif not successful_accounts:
            sync_status = 'unavailable'
            sync_message = 'Portfolio summary is currently unavailable from the configured KIS account inquiries.'
        elif degraded_accounts:
            sync_status = 'stale'
            sync_message = (
                f'Portfolio summary is partially stale: {len(successful_accounts)} of {len(results)} '
                'configured account summaries synced successfully.'
            )

        last_synced_at = _latest_timestamp(
            *[
                item['account'].get('lastSyncedAt')
                for item in results
                if isinstance(item.get('account'), dict)
            ]
        )
        if sync_message:
            notices = [sync_message, *notices]

        return serialize_portfolio_risk_equivalent_payload({
            'asOf': as_of,
            'source': PORTFOLIO_SUMMARY_READ_MODEL_SOURCE,
            'sync': {
                'status': sync_status,
                'lastSyncedAt': last_synced_at,
                'source': 'kis_account_inquiry_v1',
                'message': sync_message,
            },
            'accountCoverage': {
                'status': (
                    'complete'
                    if len(accounts) == len(configured_accounts)
                    else 'incomplete'
                ),
                'configuredCount': len(configured_accounts),
                'returnedCount': len(accounts),
            },
            'accounts': accounts,
            'notices': notices,
        })

    def _classify_refresh_result(self, payload: dict[str, Any]) -> tuple[str, str | None]:
        sync = payload.get('sync') if isinstance(payload.get('sync'), dict) else {}
        sync_status = str(sync.get('status') or '').strip()
        sync_message = str(sync.get('message') or '').strip() or None
        if sync_status == 'synced':
            return 'success', None
        if sync_status in {'stale', 'unavailable', 'not_configured'}:
            return 'degraded', sync_message
        return 'error', sync_message or 'Portfolio summary refresh did not produce a usable snapshot.'

    def _store_refresh_failure(
        self,
        *,
        cache_record: PortfolioSummaryCacheRecord,
        attempted_at: datetime,
        error: str,
        trigger: str,
    ) -> PortfolioSummaryCacheRecord:
        updated = PortfolioSummaryCacheRecord(
            payload=copy.deepcopy(cache_record.payload),
            as_of=cache_record.as_of,
            cached_at=cache_record.cached_at,
            last_refresh_attempted_at=attempted_at,
            last_refresh_succeeded_at=cache_record.last_refresh_succeeded_at,
            last_refresh_status='error',
            last_refresh_error=error,
            refresh_trigger=trigger,
        )
        self._cache = updated
        return updated

    async def _build_account_payload(
        self,
        account: ConfiguredPortfolioAccount,
        *,
        as_of: str,
    ) -> dict[str, Any]:
        summary_result, positions_result = await asyncio.gather(
            self._capture_account_summary(account.account_reference),
            self._capture_positions(account.account_reference),
        )
        activity_result = await self._capture_recent_activity(account.account_reference)

        status = 'synced'
        notices: list[str] = []
        if summary_result['error'] is not None or positions_result['error'] is not None:
            status = self._resolve_account_status(
                summary_error=summary_result['error'],
                positions_error=positions_result['error'],
            )
        if summary_result['error'] is not None:
            notices.append(
                f'{account.label}: account summary unavailable ({summary_result["error"]["detail"]}).'
            )
        if positions_result['error'] is not None:
            notices.append(
                f'{account.label}: holdings unavailable ({positions_result["error"]["detail"]}).'
            )
        if activity_result['error'] is not None:
            notices.append(
                f'{account.label}: recent activity unavailable ({activity_result["error"]}).'
            )

        account_payload: dict[str, Any] = {
            'id': account.account_reference,
            'label': account.label,
            'broker': 'kis',
            'accountType': f'Product {account.account_product_code}',
            'status': status,
            'baseCurrency': 'KRW',
            'lastSyncedAt': summary_result['as_of'] or positions_result['as_of'],
        }
        if summary_result['data'] is not None:
            account_payload['summary'] = self._normalize_account_summary(summary_result['data'])
        if positions_result['data'] is not None:
            holdings = self._normalize_holdings(
                positions_result['data'],
                account_reference=account.account_reference,
                as_of=positions_result['as_of'] or as_of,
            )
            account_payload['holdings'] = holdings
            account_payload['allocation'] = self._build_allocation_rows(holdings)
        if positions_result.get('observed_data') is not None:
            account_payload['observedHoldings'] = self._normalize_holdings(
                positions_result['observed_data'],
                account_reference=account.account_reference,
                as_of=positions_result['as_of'] or as_of,
            )
        if positions_result.get('completeness') is not None:
            account_payload['holdingsCompleteness'] = copy.deepcopy(
                positions_result['completeness']
            )
        if activity_result['items'] is not None:
            account_payload['recentActivity'] = activity_result['items']

        return {
            'status': status,
            'account': account_payload,
            'configured_failure': bool(
                (summary_result['error'] or {}).get('configured') is False
                or (positions_result['error'] or {}).get('configured') is False
            ),
            'notices': notices,
        }

    async def _capture_account_summary(self, account_reference: str) -> dict[str, Any]:
        try:
            data = await self.account_inquiry_service.get_account_summary(account_reference=account_reference)
            return {'data': data, 'error': None, 'as_of': data.get('as_of')}
        except TradingAccountInquiryError as exc:
            return {'data': None, 'error': self._normalize_inquiry_error(exc), 'as_of': None}

    async def _capture_positions(self, account_reference: str) -> dict[str, Any]:
        try:
            data = await self.account_inquiry_service.get_positions(account_reference=account_reference)
            completeness = data.get('completeness')
            if isinstance(completeness, dict) and completeness.get('status') != 'complete':
                malformed_row_count = completeness.get('malformed_row_count')
                detail = (
                    f'Holdings incomplete: KIS returned {malformed_row_count} malformed position row(s).'
                    if isinstance(malformed_row_count, int) and malformed_row_count > 0
                    else 'Holdings incomplete: KIS positions response did not establish complete coverage.'
                )
                return {
                    'data': None,
                    'observed_data': data,
                    'error': {
                        'configured': True,
                        'detail': detail,
                        'retryable': True,
                        'upstream_error_type': 'TradingAccountPositionsCompletenessError',
                    },
                    'as_of': data.get('as_of'),
                    'completeness': completeness,
                }
            return {
                'data': data,
                'observed_data': None,
                'error': None,
                'as_of': data.get('as_of'),
                'completeness': completeness if isinstance(completeness, dict) else None,
            }
        except TradingAccountInquiryError as exc:
            return {
                'data': None,
                'observed_data': None,
                'error': self._normalize_inquiry_error(exc),
                'as_of': None,
                'completeness': None,
            }

    async def _capture_recent_activity(self, account_reference: str) -> dict[str, Any]:
        try:
            data = await asyncio.to_thread(
                self.order_service.list_orders,
                account_reference=account_reference,
                limit=5,
            )
        except Exception as exc:
            return {'items': None, 'error': str(exc)}
        items = [
            self._normalize_recent_activity(item)
            for item in (data.get('items') or [])
            if isinstance(item, dict)
        ]
        return {'items': items, 'error': None}

    def _normalize_inquiry_error(self, exc: TradingAccountInquiryError) -> dict[str, Any]:
        payload = dict(exc.payload or {})
        detail = str(payload.get('detail') or str(exc))
        return {
            'configured': payload.get('configured', not isinstance(exc, TradingAccountInquiryValidationError)),
            'detail': detail,
            'retryable': bool(payload.get('retryable', False)),
            'upstream_error_type': payload.get('upstream_error_type') or type(exc).__name__,
        }

    def _runtime_status(self) -> PortfolioSummaryRuntimeStatus:
        configured_accounts, config_error = self._resolve_accounts()
        entries = self._configured_account_entries()
        blocking_reasons: list[str] = []
        if config_error is not None:
            blocking_reasons.append(config_error)
        if not settings.kis_app_key:
            blocking_reasons.append('KIS_APP_KEY must be configured for portfolio summary account inquiries.')
        if not settings.kis_app_secret:
            blocking_reasons.append('KIS_APP_SECRET must be configured for portfolio summary account inquiries.')

        normalized_app_env = str(settings.app_env or '').strip()
        if normalized_app_env.lower() == 'production' and settings.kis_use_mock:
            blocking_reasons.append(
                'KIS_USE_MOCK must be false in production so portfolio summary uses live KIS account inquiries.'
            )

        return PortfolioSummaryRuntimeStatus(
            ready=not blocking_reasons,
            app_env=normalized_app_env,
            kis_environment='demo' if settings.kis_use_mock else 'real',
            kis_base_url=settings.kis_mock_base_url if settings.kis_use_mock else settings.kis_base_url,
            kis_use_mock=settings.kis_use_mock,
            kis_app_key_configured=bool(settings.kis_app_key),
            kis_app_secret_configured=bool(settings.kis_app_secret),
            portfolio_account_references_configured=bool(entries),
            portfolio_account_references_valid=config_error is None,
            configured_account_count=len(configured_accounts),
            blocking_reasons=tuple(blocking_reasons),
        )

    def _configuration_message(self, runtime_status: PortfolioSummaryRuntimeStatus) -> str:
        reasons = list(runtime_status.blocking_reasons)
        if not reasons:
            return 'Portfolio summary runtime is not ready.'
        if len(reasons) == 1:
            return reasons[0]
        return 'Portfolio summary runtime is not ready: ' + '; '.join(reasons)

    def _configured_account_entries(self) -> list[str]:
        raw_value = self.portfolio_account_references
        if raw_value is None:
            raw_value = settings.portfolio_account_references
        return [entry.strip() for entry in str(raw_value or '').split(',') if entry.strip()]

    def _resolve_accounts(self) -> tuple[list[ConfiguredPortfolioAccount], str | None]:
        entries = self._configured_account_entries()
        if not entries:
            return [], (
                'No portfolio accounts are configured. Set PORTFOLIO_ACCOUNT_REFERENCES to one or more '
                'KIS:<account_number>-<product_code> values.'
            )

        accounts: list[ConfiguredPortfolioAccount] = []
        for entry in entries:
            label, account_reference = self._parse_account_entry(entry)
            try:
                scope = parse_kis_account_reference(account_reference)
            except ValueError as exc:
                return [], f'Invalid PORTFOLIO_ACCOUNT_REFERENCES entry "{entry}": {exc}'
            accounts.append(
                ConfiguredPortfolioAccount(
                    account_reference=scope.account_reference,
                    label=label or f'KIS {scope.account_number}-{scope.account_product_code}',
                    account_number=scope.account_number,
                    account_product_code=scope.account_product_code,
                )
            )
        return accounts, None

    def _parse_account_entry(self, entry: str) -> tuple[str | None, str]:
        if '=' not in entry:
            return None, entry.strip()
        label, account_reference = entry.split('=', 1)
        return label.strip() or None, account_reference.strip()

    def _resolve_account_status(
        self,
        *,
        summary_error: dict[str, Any] | None,
        positions_error: dict[str, Any] | None,
    ) -> str:
        errors = [error for error in (summary_error, positions_error) if error is not None]
        if any(error.get('retryable') for error in errors):
            return 'stale'
        return 'unavailable'

    def _normalize_account_summary(self, data: dict[str, Any]) -> dict[str, Any]:
        cash = data.get('cash') or {}
        valuation = data.get('valuation') or {}
        unrealized_pnl_amount = valuation.get('pnl_amount')
        raw_day_pnl_amount = valuation.get('day_pnl_amount')
        has_day_pnl_amount = _safe_decimal(raw_day_pnl_amount) is not None
        day_pnl_amount = raw_day_pnl_amount if has_day_pnl_amount else unrealized_pnl_amount
        total_pnl_pct = (
            valuation.get('day_pnl_pct')
            if has_day_pnl_amount
            else valuation.get('pnl_pct')
        )
        return {
            'totalAssets': self._money(valuation.get('total_asset_value')),
            'totalEquity': self._money(valuation.get('total_asset_value')),
            'cash': self._money(_coalesce_non_none(cash.get('withdrawable_cash'), cash.get('settled_cash'))),
            'buyingPower': self._money(cash.get('orderable_cash')),
            'investedValue': self._money(valuation.get('securities_value')),
            'pnl': {
                'unrealized': self._money(unrealized_pnl_amount),
                'day': self._money(day_pnl_amount),
                'total': self._money(day_pnl_amount),
                'totalPercent': _to_percent(total_pnl_pct),
            },
        }

    def _normalize_holdings(
        self,
        data: dict[str, Any],
        *,
        account_reference: str,
        as_of: str,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for row in data.get('items') or []:
            if not isinstance(row, dict):
                continue
            symbol = row.get('symbol')
            if not symbol:
                continue
            items.append(
                {
                    'id': f'{account_reference}:{symbol}',
                    'symbol': symbol,
                    'name': row.get('name'),
                    'market': 'KRX',
                    'quantity': row.get('quantity'),
                    'averagePrice': self._money(row.get('average_price')),
                    'lastPrice': self._money(row.get('last_price')),
                    'marketValue': self._money(row.get('market_value')),
                    'unrealizedPnl': self._money(row.get('pnl_amount')),
                    'unrealizedPnlPercent': _to_percent(row.get('pnl_pct')),
                    'allocationPercent': _to_percent(row.get('weight_pct')),
                    'currency': 'KRW',
                    'updatedAt': as_of,
                }
            )
        return items

    def _build_allocation_rows(self, holdings: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                'id': item['id'],
                'label': item.get('name') or item['symbol'],
                'value': item.get('marketValue'),
                'percent': item.get('allocationPercent'),
            }
            for item in holdings
        ]

    def _normalize_recent_activity(self, item: dict[str, Any]) -> dict[str, Any]:
        quantity = _safe_decimal(item.get('requested_quantity'))
        filled_quantity = _safe_decimal(item.get('filled_quantity'))
        return {
            'id': str(item.get('client_order_id') or item.get('id')),
            'symbol': item.get('symbol'),
            'side': item.get('side') or 'unknown',
            'status': item.get('current_status'),
            'quantity': _to_number(quantity),
            'filledQuantity': _to_number(filled_quantity),
            'averageFillPrice': self._money(item.get('average_fill_price')),
            'submittedAt': item.get('first_submitted_at') or item.get('created_at'),
            'updatedAt': item.get('last_broker_update_at') or item.get('updated_at'),
        }

    def _money(self, value: Any, *, currency: str = 'KRW') -> dict[str, Any] | None:
        decimal_value = _safe_decimal(value)
        if decimal_value is None:
            return None
        return {
            'amount': _to_number(decimal_value),
            'currency': currency,
        }

    def _build_unavailable_summary(
        self,
        *,
        as_of: str,
        status: str,
        source: str,
        message: str,
        freshness: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            'asOf': as_of,
            'source': PORTFOLIO_SUMMARY_READ_MODEL_SOURCE,
            'sync': {
                'status': status,
                'lastSyncedAt': None,
                'source': source,
                'message': message,
            },
            'accounts': [],
            'notices': [message],
        }
        if freshness is not None:
            payload['freshness'] = freshness
        return serialize_portfolio_risk_equivalent_payload(payload)

    def _decorate_cached_payload(
        self,
        cache_record: PortfolioSummaryCacheRecord,
        *,
        served_from_cache: bool,
        stale_reason_override: str | None = None,
        market_state: str | None = None,
    ) -> dict[str, Any]:
        payload = copy.deepcopy(cache_record.payload)
        freshness = self._build_freshness_payload(cache_record, served_from_cache=served_from_cache)
        if stale_reason_override is not None:
            freshness['isStale'] = True
            freshness['staleReason'] = stale_reason_override
            freshness['refreshInFlight'] = False
        if market_state is not None:
            freshness['marketState'] = market_state
        payload['freshness'] = freshness

        sync = payload.get('sync') if isinstance(payload.get('sync'), dict) else {}
        notices = payload.get('notices') if isinstance(payload.get('notices'), list) else []
        stale_notice: str | None = None
        if freshness['staleReason'] == 'refresh_due':
            stale_notice = 'Portfolio summary is serving cached data while a refresh runs in background.'
        elif freshness['staleReason'] == 'refresh_failed':
            stale_notice = 'Portfolio summary is serving cached data because the last refresh failed.'
        elif freshness['staleReason'] == 'market_closed_cache':
            stale_notice = 'Portfolio summary is serving the last cached data while the market is closed.'
        elif freshness['staleReason'] == 'market_state_unavailable_cache':
            stale_notice = 'Portfolio summary is serving cached data because market state is unavailable.'

        if stale_notice is not None:
            if sync.get('status') == 'synced':
                sync['status'] = 'stale'
            sync['message'] = self._merge_messages(sync.get('message'), stale_notice)
            notices = self._prepend_notice(notices, stale_notice)

        payload['sync'] = sync
        payload['notices'] = notices
        return serialize_portfolio_risk_equivalent_payload(payload)

    def _decorate_uncached_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        decorated = copy.deepcopy(payload)
        freshness = (
            decorated.get('freshness')
            if isinstance(decorated.get('freshness'), dict)
            else {}
        )
        freshness['refreshInFlight'] = self._has_refresh_in_flight()
        decorated['freshness'] = freshness
        return serialize_portfolio_risk_equivalent_payload(decorated)

    def _build_cache_status_payload(
        self,
        cache_record: PortfolioSummaryCacheRecord | None,
    ) -> dict[str, Any]:
        stale_reason = self._stale_reason(cache_record)
        return {
            'available': True,
            'has_snapshot': cache_record is not None,
            'as_of': cache_record.as_of if cache_record is not None else None,
            'cached_at': self._format_datetime(cache_record.cached_at if cache_record is not None else None),
            'cache_age_seconds': self._cache_age_seconds(cache_record),
            'is_stale': stale_reason is not None,
            'stale_reason': stale_reason,
            'refresh_in_flight': self._has_refresh_in_flight(),
            'last_refresh_attempted_at': (
                self._format_datetime(cache_record.last_refresh_attempted_at) if cache_record is not None else None
            ),
            'last_refresh_succeeded_at': (
                self._format_datetime(cache_record.last_refresh_succeeded_at) if cache_record is not None else None
            ),
            'next_refresh_due_at': self._format_datetime(self._next_refresh_due_at(cache_record)),
            'last_refresh_status': cache_record.last_refresh_status if cache_record is not None else 'never',
            'last_refresh_error': cache_record.last_refresh_error if cache_record is not None else None,
            'refresh_interval_seconds': self.cache_ttl_seconds,
        }

    def _build_freshness_payload(
        self,
        cache_record: PortfolioSummaryCacheRecord,
        *,
        served_from_cache: bool,
    ) -> dict[str, Any]:
        stale_reason = self._stale_reason(cache_record)
        return {
            'servedFromCache': served_from_cache,
            'hasCachedSnapshot': True,
            'cachedAt': self._format_datetime(cache_record.cached_at),
            'cacheAgeSeconds': self._cache_age_seconds(cache_record),
            'isStale': stale_reason is not None,
            'staleReason': stale_reason,
            'refreshInFlight': self._has_refresh_in_flight(),
            'refreshIntervalSeconds': self.cache_ttl_seconds,
            'lastRefreshAttemptedAt': self._format_datetime(cache_record.last_refresh_attempted_at),
            'lastRefreshSucceededAt': self._format_datetime(cache_record.last_refresh_succeeded_at),
            'nextRefreshDueAt': self._format_datetime(self._next_refresh_due_at(cache_record)),
            'lastRefreshStatus': cache_record.last_refresh_status,
            'lastRefreshError': cache_record.last_refresh_error,
        }

    def _build_uncached_freshness_payload(
        self,
        *,
        attempted_at: datetime,
        last_refresh_status: str,
        last_refresh_error: str | None,
    ) -> dict[str, Any]:
        return {
            'servedFromCache': False,
            'hasCachedSnapshot': False,
            'cachedAt': None,
            'cacheAgeSeconds': None,
            'isStale': False,
            'staleReason': None,
            'refreshInFlight': self._has_refresh_in_flight(),
            'refreshIntervalSeconds': self.cache_ttl_seconds,
            'lastRefreshAttemptedAt': self._format_datetime(attempted_at),
            'lastRefreshSucceededAt': None,
            'nextRefreshDueAt': None,
            'lastRefreshStatus': last_refresh_status,
            'lastRefreshError': last_refresh_error,
        }

    def _stale_reason(self, cache_record: PortfolioSummaryCacheRecord | None) -> str | None:
        if cache_record is None:
            return None
        if cache_record.last_refresh_status == 'error':
            return 'refresh_failed'
        if self._refresh_is_due(cache_record):
            return 'refresh_due'
        return None

    def _refresh_is_due(self, cache_record: PortfolioSummaryCacheRecord | None) -> bool:
        if cache_record is None:
            return True
        if self.cache_ttl_seconds == 0:
            return True
        next_due_at = self._next_refresh_due_at(cache_record)
        if next_due_at is None:
            return True
        return _utc_now() >= next_due_at

    def _next_refresh_due_at(
        self,
        cache_record: PortfolioSummaryCacheRecord | None,
    ) -> datetime | None:
        if cache_record is None:
            return None
        reference = (
            cache_record.last_refresh_attempted_at
            or cache_record.last_refresh_succeeded_at
            or cache_record.cached_at
        )
        return reference + timedelta(seconds=self.cache_ttl_seconds)

    def _cache_age_seconds(self, cache_record: PortfolioSummaryCacheRecord | None) -> int | None:
        if cache_record is None:
            return None
        age = _utc_now() - cache_record.cached_at
        return max(int(age.total_seconds()), 0)

    def _has_refresh_in_flight(self) -> bool:
        background_task = self._refresh_task
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        background_refresh_scheduled = bool(
            background_task is not None
            and not background_task.done()
            and background_task is not current_task
        )
        return self._active_refresh_count > 0 or background_refresh_scheduled

    def _format_datetime(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.isoformat()

    def _merge_messages(self, current: Any, new_message: str) -> str:
        current_text = str(current or '').strip()
        if not current_text:
            return new_message
        if new_message in current_text:
            return current_text
        return f'{current_text} {new_message}'

    def _prepend_notice(self, notices: list[Any], notice: str) -> list[Any]:
        normalized = [item for item in notices if isinstance(item, str) and item.strip()]
        if notice in normalized:
            return normalized
        return [notice, *normalized]


portfolio_summary_service = PortfolioSummaryService()
