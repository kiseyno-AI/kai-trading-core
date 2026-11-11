# GET /execution/runtime/monitoring-session-results

Read the results for a recorded KRX monitoring session. Omit `market_date` to get the most recent trading date with recorded monitoring. Before the next session opens and on non-trading days, the default stays on the latest recorded session. With an explicit date and no recorded monitoring, the response has that date and an empty item list. When the default finds no session, `market_date` is null.

Each symbol appears once. Membership comes from KIS price subscriptions confirmed during the session by a successful acknowledgment or verified trade, verified same-session Exec monitoring, and existing WAIT observation records with evidence of tracking during the session. Core saves KIS trade observations in a local session archive with a two-second checkpoint interval and a final checkpoint on graceful shutdown. The endpoint reads Core's archive and database with the usual Core bearer authentication.

| `price_status` | Recorded price used for per-symbol change |
| --- | --- |
| `close` | Exact-date KRX regular-session close from Core's `daily_price_bars` |
| `last_quote` | Latest saved same-session KIS trade from the session archive or a verified WAIT touch record, returned in `last_price` and `last_price_at` |
| `unavailable` | Both session price sources are missing |

The previous close is read for the exact preceding date in the KRX exchange calendar. Price dates are null when the corresponding close is missing. `change` and `change_pct` are null if either comparison price is missing. Normal PyKRX daily collection starts no earlier than 20:10 KST after a 15:30 close, and each symbol switches to `close` when its stored daily bar arrives. Earlier sessions can use their existing WAIT history and daily bars. The new archive begins saving KIS trades when it is deployed.

`hypothetical_return` assumes the same amount of money is invested in every monitored symbol at its verified saved `WAIT` trigger. It is the arithmetic mean of each included symbol's percentage return: `mean((valuation price / trigger - 1) × 100)`. Execution-plan existence, order eligibility, and planned quantities do not affect the hypothetical allocation. Each symbol is counted once: use the long-term (`signal-decider`) trigger when it is verified, otherwise the short-term trigger. The observation must match the latest persisted decision from a source date before the monitored session, known by the session cutoff. An earlier-session WAIT carried into the current monitoring list remains eligible until a newer prior-session decision replaces it; a newer reject invalidates its lane. Current/future source-session decisions do not rewrite the monitored session.

During the current session, fresh same-session display quotes from `kis_realtime_ws` or `kis_rest_multi_quote` may value the hypothetical portfolio, including symbols beyond WebSocket capacity. Only available, positive, timestamped quotes from within the regular session qualify. Receipt time on REST snapshots remains receipt time: these display prices never become recorded trades, trigger-touch evidence, or archive entries. The `items` price fields above retain their recorded-trade/close meaning, so an item may lack a recorded trade while its fresh display quote contributes to the current aggregate. Historical sessions use only their saved session prices, with the exact-date close taking precedence; live prices from a later session never replace them.

The model assumes fractional quantities and a purchase at each trigger without requiring a touch. It excludes fills, exits and fees. Symbols without a verified positive trigger or a usable valuation price are omitted from `included_symbol_count`; `monitored_symbol_count` still describes the session membership. If no symbol qualifies, `return_pct` is null. The response contains only the aggregate and per-symbol market prices; no investment amount or quantity is returned.

## Example request

```http
GET /execution/runtime/monitoring-session-results?market_date=2026-11-06
```

## Example response

```json
{
  "market_date": "2026-11-06",
  "previous_trade_date": "2026-11-05",
  "items": [
    {
      "symbol": "005930",
      "price_status": "close",
      "close_price": 10500,
      "close_trade_date": "2026-11-06",
      "previous_close_price": 10000,
      "previous_close_trade_date": "2026-11-05",
      "last_price": null,
      "last_price_at": null,
      "change": 500,
      "change_pct": 5.0
    }
  ],
  "hypothetical_return": {
    "return_pct": 5.0,
    "included_symbol_count": 1,
    "monitored_symbol_count": 1
  }
}
```
