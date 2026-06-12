# GET /trading/accounts/{account_reference}/summary, /positions, and /buying-power

## Summary
Read normalized KIS-backed account cash or valuation, current holdings, and symbol-aware buyable quantity before submitting an order.

## Example request
```http
GET /trading/accounts/KIS:12345678-01/summary
GET /trading/accounts/KIS:12345678-01/positions?symbol=005930&include_zero=false
GET /trading/accounts/KIS:12345678-01/buying-power?symbol=005930&price=57000&order_type=market
```

## Example summary response
```json
{
  "account_reference": "KIS:12345678-01",
  "broker_name": "kis",
  "as_of": "2026-04-28T06:30:00+00:00",
  "cash": {
    "settled_cash": 2500000,
    "withdrawable_cash": 2400000,
    "orderable_cash": 2500000,
    "pending_buy_amount": 100000,
    "pending_sell_amount": 50000
  },
  "valuation": {
    "securities_value": 572000,
    "total_asset_value": 3072000,
    "pnl_amount": 22000,
    "pnl_pct": 0.04,
    "day_pnl_amount": 20366,
    "day_pnl_pct": 0.0066,
    "estimated_d2_deposit": 2480000
  },
  "raw": {
    "provider": "kis",
    "source_api": "inquire-balance|inquire-account-balance"
  }
}
```

`valuation.pnl_amount` and `valuation.pnl_pct` come from KIS holding evaluation fields. `valuation.day_pnl_amount` maps KIS `asst_icdc_amt` when the broker account asset summary or balance payload provides it, so realized same-day sell profit can remain visible after the sold holding leaves the positions list.

## Example positions response
```json
{
  "account_reference": "KIS:12345678-01",
  "broker_name": "kis",
  "as_of": "2026-04-28T06:30:00+00:00",
  "items": [
    {
      "symbol": "005930",
      "name": "Samsung Electronics",
      "quantity": 10,
      "sellable_quantity": 8,
      "average_price": 55000,
      "last_price": 57200,
      "market_value": 572000,
      "pnl_amount": 22000,
      "pnl_pct": 0.04,
      "weight_pct": 0.1862
    }
  ]
}
```

## Example buying-power response
```json
{
  "account_reference": "KIS:12345678-01",
  "broker_name": "kis",
  "symbol": "005930",
  "as_of": "2026-04-28T06:30:00+00:00",
  "buying_power": {
    "buyable_quantity": 17,
    "buyable_amount": 969000,
    "max_buy_quantity": 22,
    "max_buy_amount": 1292000,
    "orderable_cash": 970000,
    "cash_limited": true
  },
  "raw": {
    "provider": "kis",
    "source_api": "inquire-psbl-order"
  }
}
```
