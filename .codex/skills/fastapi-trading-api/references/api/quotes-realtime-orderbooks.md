# GET /quotes/realtime/orderbooks

Returns the latest in-memory KRX best bid and ask for protected monitoring symbols with an active KIS book subscription. Books need a valid exchange time no older than two seconds. A disconnect, invalid book, or stale exchange time removes the corresponding live hint. The route does not request data from KIS.

The `passive_quote_symbols` list identifies WAIT symbols covered by the latest successful samples within ten seconds. Coverage expires at `passive_quote_expires_at` (UTC), ten seconds after the first usable batch in that round or at regular-session close, whichever comes first. An empty coverage list has a null expiration. A connected socket and the passive sampling coverage are independent. An empty result supports the caller's existing REST fallback. A book is a scheduling hint for Exec, with order eligibility and pre-submit validation still owned by Exec.

```http
GET /quotes/realtime/orderbooks
Authorization: Bearer <configured Core token>
```

```json
{"connected":true,"passive_quote_symbols":["000660"],"passive_quote_expires_at":"2026-11-10T05:25:10+00:00","items":[{"symbol":"005930","bid_price":71000,"ask_price":71100,"bid_size":200,"ask_size":150,"price_as_of":"2026-11-10T05:25:00+00:00","price_received_at":"2026-11-10T05:25:00.100000+00:00","source":"kis_orderbook_ws"}]}
```
