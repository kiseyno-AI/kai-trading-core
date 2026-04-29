from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from app.core.settings import settings
from app.orders.account_inquiry_service import (
    TradingAccountInquiryError,
    TradingAccountInquiryValidationError,
    trading_account_inquiry_service,
)
from app.orders.account_scope import parse_kis_account_reference
from app.orders.service import trading_order_service


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


class PortfolioSummaryService:
    def __init__(
        self,
        *,
        account_inquiry_service: Any | None = None,
        order_service: Any | None = None,
        portfolio_account_references: str | None = None,
    ) -> None:
        self.account_inquiry_service = account_inquiry_service or trading_account_inquiry_service
        self.order_service = order_service or trading_order_service
        self.portfolio_account_references = portfolio_account_references

    def runtime_status(self) -> dict[str, Any]:
        return self._runtime_status().to_payload()

    async def get_portfolio_summary(self) -> dict[str, Any]:
        as_of = _utc_now().isoformat()
        runtime_status = self._runtime_status()
        if not runtime_status.ready:
            return self._build_unavailable_summary(
                as_of=as_of,
                status='not_configured',
                source='portfolio_summary_runtime',
                message=self._configuration_message(runtime_status),
            )
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

        return {
            'asOf': as_of,
            'source': 'kis_portfolio_summary_v1',
            'sync': {
                'status': sync_status,
                'lastSyncedAt': last_synced_at,
                'source': 'kis_account_inquiry_v1',
                'message': sync_message,
            },
            'accounts': accounts,
            'notices': notices,
        }

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
            return {'data': data, 'error': None, 'as_of': data.get('as_of')}
        except TradingAccountInquiryError as exc:
            return {'data': None, 'error': self._normalize_inquiry_error(exc), 'as_of': None}

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
        return {
            'totalAssets': self._money(valuation.get('total_asset_value')),
            'totalEquity': self._money(valuation.get('total_asset_value')),
            'cash': self._money(_coalesce_non_none(cash.get('withdrawable_cash'), cash.get('settled_cash'))),
            'buyingPower': self._money(cash.get('orderable_cash')),
            'investedValue': self._money(valuation.get('securities_value')),
            'pnl': {
                'unrealized': self._money(valuation.get('pnl_amount')),
                'total': self._money(valuation.get('pnl_amount')),
                'totalPercent': _to_percent(valuation.get('pnl_pct')),
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
    ) -> dict[str, Any]:
        return {
            'asOf': as_of,
            'source': 'kis_portfolio_summary_v1',
            'sync': {
                'status': status,
                'lastSyncedAt': None,
                'source': source,
                'message': message,
            },
            'accounts': [],
            'notices': [message],
        }


portfolio_summary_service = PortfolioSummaryService()
