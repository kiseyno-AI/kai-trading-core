from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from app.brokers.kis.client import (
    KISClient,
    KISClientError,
    KISConfigurationError,
)
from app.brokers.kis.provider import kis_client
from app.orders.account_scope import AccountScope, parse_kis_account_reference


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_text(value: Any, *, field: str, uppercase: bool = False, lowercase: bool = False) -> str:
    normalized = str(value or '').strip()
    if not normalized:
        raise TradingAccountInquiryValidationError(f'{field} is required')
    if uppercase:
        return normalized.upper()
    if lowercase:
        return normalized.lower()
    return normalized


def _normalize_optional_text(value: Any, *, uppercase: bool = False, lowercase: bool = False) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if uppercase:
        return normalized.upper()
    if lowercase:
        return normalized.lower()
    return normalized


def _to_decimal(value: Any, *, field: str, positive: bool = False) -> Decimal:
    try:
        normalized = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TradingAccountInquiryValidationError(f'{field} must be numeric') from exc
    if positive and normalized <= Decimal('0'):
        raise TradingAccountInquiryValidationError(f'{field} must be greater than zero')
    return normalized


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == '':
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _pick_first(mapping: dict[str, Any] | None, *keys: str) -> Any:
    if not isinstance(mapping, dict):
        return None
    lowered = {str(key).lower(): value for key, value in mapping.items()}
    for key in keys:
        if key in mapping:
            return mapping[key]
        candidate = lowered.get(key.lower())
        if candidate is not None:
            return candidate
    return None


def _to_json_number(value: Decimal | None) -> int | float | None:
    if value is None:
        return None
    if value == value.to_integral():
        return int(value)
    return float(value)


def _ratio_from_percent(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value / Decimal('100'))


def _ratio(numerator: Decimal | None, denominator: Decimal | None) -> float | None:
    if numerator is None or denominator is None or denominator == Decimal('0'):
        return None
    return float(numerator / denominator)


def _ensure_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return next((item for item in reversed(value) if isinstance(item, dict)), {})
    return {}


class TradingAccountInquiryError(RuntimeError):
    def __init__(self, message: str, *, payload: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.payload = payload or {}


class TradingAccountInquiryValidationError(TradingAccountInquiryError):
    pass


class TradingAccountInquiryUpstreamError(TradingAccountInquiryError):
    pass


class TradingAccountInquiryService:
    def __init__(self, *, client: KISClient | None = None) -> None:
        self.client = client or kis_client

    async def get_account_summary(self, *, account_reference: str) -> dict[str, Any]:
        scope = self._parse_account_scope(account_reference)
        try:
            balance = await self.client.fetch_domestic_account_balance(
                account_number=scope.account_number,
                account_product_code=scope.account_product_code,
            )
            asset_summary = await self.client.fetch_domestic_account_asset_summary(
                account_number=scope.account_number,
                account_product_code=scope.account_product_code,
            )
        except KISClientError as exc:
            raise self._map_upstream_error(exc, account_reference=scope.account_reference) from exc

        balance_totals = _ensure_mapping(balance.get('output2'))
        asset_totals = _ensure_mapping(asset_summary.get('output2'))
        purchase_amount = self._first_decimal(
            balance_totals,
            ('pchs_amt_smtl_amt',),
            asset_totals,
            ('pchs_amt_smtl', 'pchs_amt'),
        )
        pnl_amount = self._first_decimal(
            balance_totals,
            ('evlu_pfls_smtl_amt', 'evlu_pfls_amt_smtl'),
            asset_totals,
            ('evlu_pfls_amt_smtl', 'evlu_pfls_amt'),
        )
        day_pnl_amount = self._first_decimal(
            asset_totals,
            ('asst_icdc_amt',),
            balance_totals,
            ('asst_icdc_amt',),
        )
        day_pnl_pct = self._first_decimal(
            asset_totals,
            ('asst_icdc_erng_rt',),
            balance_totals,
            ('asst_icdc_erng_rt',),
        )

        return {
            'account_reference': scope.account_reference,
            'broker_name': 'kis',
            'as_of': _utc_now().isoformat(),
            'cash': {
                'settled_cash': _to_json_number(
                    self._first_decimal(balance_totals, ('dnca_tot_amt',), asset_totals, ('dncl_amt', 'tot_dncl_amt'))
                ),
                'withdrawable_cash': _to_json_number(
                    self._first_decimal(asset_totals, ('dncl_amt', 'tot_dncl_amt'), balance_totals, ('dnca_tot_amt',))
                ),
                'orderable_cash': _to_json_number(
                    self._first_decimal(balance_totals, ('dnca_tot_amt',), asset_totals, ('tot_dncl_amt', 'dncl_amt'))
                ),
                'pending_buy_amount': _to_json_number(self._first_decimal(balance_totals, ('thdt_buy_amt',))),
                'pending_sell_amount': _to_json_number(self._first_decimal(balance_totals, ('thdt_sll_amt',))),
            },
            'valuation': {
                'securities_value': _to_json_number(
                    self._first_decimal(
                        balance_totals,
                        ('scts_evlu_amt', 'evlu_amt_smtl_amt'),
                        asset_totals,
                        ('evlu_amt_smtl', 'evlu_amt'),
                    )
                ),
                'total_asset_value': _to_json_number(
                    self._first_decimal(balance_totals, ('tot_evlu_amt',), asset_totals, ('tot_asst_amt', 'nass_tot_amt'))
                ),
                'pnl_amount': _to_json_number(pnl_amount),
                'pnl_pct': _ratio(pnl_amount, purchase_amount),
                'day_pnl_amount': _to_json_number(day_pnl_amount),
                'day_pnl_pct': _ratio_from_percent(day_pnl_pct),
                'estimated_d2_deposit': _to_json_number(
                    self._first_decimal(balance_totals, ('prvs_rcdl_excc_amt', 'nxdy_excc_amt'))
                ),
            },
            'raw': {
                'provider': 'kis',
                'source_api': 'inquire-balance|inquire-account-balance',
                'balance': balance,
                'account_asset_summary': asset_summary,
            },
        }

    async def get_positions(
        self,
        *,
        account_reference: str,
        symbol: str | None = None,
        include_zero: bool = False,
    ) -> dict[str, Any]:
        scope = self._parse_account_scope(account_reference)
        normalized_symbol = _normalize_optional_text(symbol, uppercase=True)
        try:
            balance = await self.client.fetch_domestic_account_balance(
                account_number=scope.account_number,
                account_product_code=scope.account_product_code,
            )
        except KISClientError as exc:
            raise self._map_upstream_error(exc, account_reference=scope.account_reference) from exc

        balance_totals = _ensure_mapping(balance.get('output2'))
        total_asset_value = self._first_decimal(balance_totals, ('tot_evlu_amt',))
        rows = [row for row in balance.get('output1', []) if isinstance(row, dict)]

        filtered_rows: list[dict[str, Any]] = []
        for row in rows:
            row_symbol = _normalize_optional_text(_pick_first(row, 'pdno'), uppercase=True)
            if normalized_symbol and row_symbol != normalized_symbol:
                continue
            quantity = _safe_decimal(_pick_first(row, 'hldg_qty')) or Decimal('0')
            if not include_zero and quantity <= Decimal('0'):
                continue
            filtered_rows.append(row)

        total_positions_value = sum(
            (_safe_decimal(_pick_first(row, 'evlu_amt')) or Decimal('0'))
            for row in filtered_rows
        )

        items = [self._normalize_position(row, total_asset_value=total_asset_value, total_positions_value=total_positions_value) for row in filtered_rows]
        items.sort(key=lambda item: item.get('market_value') or 0, reverse=True)

        return {
            'account_reference': scope.account_reference,
            'broker_name': 'kis',
            'as_of': _utc_now().isoformat(),
            'count': len(items),
            'items': items,
            'raw': {
                'provider': 'kis',
                'source_api': 'inquire-balance',
                'balance': balance,
            },
        }

    async def get_buying_power(
        self,
        *,
        account_reference: str,
        symbol: str,
        price: Any,
        order_type: str,
        include_cma: bool = False,
        include_overseas: bool = False,
        side: str = 'buy',
    ) -> dict[str, Any]:
        scope = self._parse_account_scope(account_reference)
        normalized_symbol = _normalize_text(symbol, field='symbol', uppercase=True)
        normalized_side = _normalize_text(side, field='side', lowercase=True)
        if normalized_side != 'buy':
            raise TradingAccountInquiryValidationError('only buy-side buying power is supported for the initial KIS inquiry adapter')

        normalized_price = _to_decimal(price, field='price', positive=True)
        normalized_order_type = _normalize_text(order_type, field='order_type', lowercase=True)
        order_division_code = self._resolve_kis_order_code(order_type=normalized_order_type)

        try:
            payload = await self.client.fetch_domestic_buying_power(
                account_number=scope.account_number,
                account_product_code=scope.account_product_code,
                symbol=normalized_symbol,
                order_price=format(normalized_price, 'f'),
                order_division_code=order_division_code,
                include_cma_evaluation='Y' if include_cma else 'N',
                include_overseas='Y' if include_overseas else 'N',
            )
        except KISClientError as exc:
            raise self._map_upstream_error(exc, account_reference=scope.account_reference) from exc

        output = _ensure_mapping(payload.get('output'))
        buyable_amount = self._first_decimal(output, ('nrcvb_buy_amt', 'max_buy_amt'))
        buyable_quantity = self._first_decimal(output, ('nrcvb_buy_qty', 'max_buy_qty'))
        max_buy_amount = self._first_decimal(output, ('max_buy_amt',))
        max_buy_quantity = self._first_decimal(output, ('max_buy_qty',))

        return {
            'account_reference': scope.account_reference,
            'broker_name': 'kis',
            'symbol': normalized_symbol,
            'as_of': _utc_now().isoformat(),
            'input': {
                'price': _to_json_number(normalized_price),
                'order_type': normalized_order_type,
                'side': normalized_side,
                'include_cma': include_cma,
                'include_overseas': include_overseas,
            },
            'buying_power': {
                'buyable_quantity': _to_json_number(buyable_quantity),
                'buyable_amount': _to_json_number(buyable_amount),
                'max_buy_quantity': _to_json_number(max_buy_quantity),
                'max_buy_amount': _to_json_number(max_buy_amount),
                'orderable_cash': _to_json_number(self._first_decimal(output, ('ord_psbl_cash',))),
                'cash_limited': bool(
                    (buyable_quantity is not None and max_buy_quantity is not None and buyable_quantity < max_buy_quantity)
                    or (buyable_amount is not None and max_buy_amount is not None and buyable_amount < max_buy_amount)
                ),
            },
            'raw': {
                'provider': 'kis',
                'source_api': 'inquire-psbl-order',
                'order_division_code': order_division_code,
                'response': payload,
            },
        }

    def _parse_account_scope(self, account_reference: str) -> AccountScope:
        normalized = _normalize_text(account_reference, field='account_reference')
        try:
            return parse_kis_account_reference(normalized)
        except ValueError as exc:
            raise TradingAccountInquiryValidationError(str(exc), payload={'account_reference': normalized}) from exc

    def _normalize_position(
        self,
        row: dict[str, Any],
        *,
        total_asset_value: Decimal | None,
        total_positions_value: Decimal | None,
    ) -> dict[str, Any]:
        quantity = _safe_decimal(_pick_first(row, 'hldg_qty')) or Decimal('0')
        market_value = _safe_decimal(_pick_first(row, 'evlu_amt'))
        pnl_amount = _safe_decimal(_pick_first(row, 'evlu_pfls_amt'))
        purchase_amount = _safe_decimal(_pick_first(row, 'pchs_amt'))
        weight_denominator = total_asset_value if total_asset_value and total_asset_value > Decimal('0') else total_positions_value
        pnl_pct = _ratio_from_percent(_safe_decimal(_pick_first(row, 'evlu_pfls_rt', 'evlu_erng_rt')))
        if pnl_pct is None:
            pnl_pct = _ratio(pnl_amount, purchase_amount)

        return {
            'symbol': _normalize_optional_text(_pick_first(row, 'pdno'), uppercase=True),
            'name': _normalize_optional_text(_pick_first(row, 'prdt_name')),
            'quantity': _to_json_number(quantity),
            'sellable_quantity': _to_json_number(_safe_decimal(_pick_first(row, 'ord_psbl_qty'))),
            'average_price': _to_json_number(_safe_decimal(_pick_first(row, 'pchs_avg_pric'))),
            'last_price': _to_json_number(_safe_decimal(_pick_first(row, 'prpr'))),
            'market_value': _to_json_number(market_value),
            'pnl_amount': _to_json_number(pnl_amount),
            'pnl_pct': pnl_pct,
            'weight_pct': _ratio(market_value, weight_denominator),
        }

    def _resolve_kis_order_code(self, *, order_type: str) -> str:
        if order_type == 'limit':
            return '00'
        if order_type == 'market':
            return '01'
        if order_type == 'best':
            return '03'
        raise TradingAccountInquiryValidationError(f'unsupported order_type for KIS buying power inquiry: {order_type}')

    def _first_decimal(
        self,
        primary: dict[str, Any] | None,
        primary_keys: tuple[str, ...],
        secondary: dict[str, Any] | None = None,
        secondary_keys: tuple[str, ...] = (),
    ) -> Decimal | None:
        for key in primary_keys:
            candidate = _safe_decimal(_pick_first(primary, key))
            if candidate is not None:
                return candidate
        for key in secondary_keys:
            candidate = _safe_decimal(_pick_first(secondary, key))
            if candidate is not None:
                return candidate
        return None

    def _map_upstream_error(self, exc: KISClientError, *, account_reference: str) -> TradingAccountInquiryUpstreamError:
        return TradingAccountInquiryUpstreamError(
            str(exc),
            payload={
                'account_reference': account_reference,
                'detail': str(exc),
                'upstream_error_type': type(exc).__name__,
                'provider': 'kis',
                'retryable': getattr(exc, 'retryable', False),
                'configured': not isinstance(exc, KISConfigurationError),
            },
        )


trading_account_inquiry_service = TradingAccountInquiryService()
