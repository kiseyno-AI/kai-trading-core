# GET /execution/runtime/monitoring-session-results

Read the results for a recorded KRX monitoring session. Omit `market_date` to get the most recent trading date with recorded monitoring. Before the next session opens and on non-trading days, the default stays on the latest recorded session. With an explicit date and no recorded monitoring, the response has that date and an empty item list. When the default finds no session, `market_date` is null.

`session_open_at` and `session_close_at` are timezone-aware boundaries of the selected KRX exchange-calendar session, including nonstandard schedules. They identify the regular-session window, not when Core actually started or stopped monitoring and not a daily-bar ingestion timestamp. They are null when no recorded session is returned. `last_price_at` remains the saved trade observation timestamp; a `close` is associated with the selected session’s `session_close_at`.

Each symbol appears once. Membership comes from KIS price subscriptions confirmed during the session by a successful acknowledgment or verified trade, verified same-session Exec monitoring, and existing WAIT observation records with evidence of tracking during the session. Core saves KIS trade observations in a local session archive with a two-second checkpoint interval and a final checkpoint on graceful shutdown. The endpoint reads Core's archive and database with the usual Core bearer authentication.

| `price_status` | Price used for change and valuation |
| --- | --- |
| `close` | Exact-date KRX regular-session close from Core's `daily_price_bars` |
| `last_quote` | Latest saved same-session KIS trade from the session archive or a verified WAIT touch record, returned in `last_price` and `last_price_at` |
| `unavailable` | Both session price sources are missing |

The previous close is read for the exact preceding date in the KRX exchange calendar. Price dates are null when the corresponding close is missing. `change` and `change_pct` are null if either comparison price is missing. Normal PyKRX daily collection starts no earlier than 20:10 KST after a 15:30 close, and each symbol switches to `close` when its stored daily bar arrives. Earlier sessions can use their existing WAIT history and daily bars. The new archive begins saving KIS trades when it is deployed.

`hypothetical_return` values each verified original `WAIT` at its own saved trigger and positive Core plan quantity. The plan must match a recorded WAIT for the target session and the latest persisted decision in that lane as of the session cutoff. The calculation is `Σ((valuation price - trigger) × quantity) / Σ(trigger × quantity) × 100`. It models a purchase at the trigger without requiring a touch and excludes fills, exits, and fees. If both lanes qualify, both amounts contribute. Counts are distinct symbols. Unavailable or unverifiable inputs are omitted, and the return is null if no plan qualifies. The response contains only the aggregate and per-symbol market prices.

## Example request

```http
GET /execution/runtime/monitoring-session-results?market_date=2026-11-06
```

## Example response

```json
{
  "market_date": "2026-11-06",
  "previous_trade_date": "2026-11-05",
  "session_open_at": "2026-11-06T00:00:00Z",
  "session_close_at": "2026-11-06T06:30:00Z",
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
