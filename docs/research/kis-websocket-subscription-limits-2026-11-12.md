# KIS WebSocket subscription limits

Research date: 2026-11-12. This note separates broker policy from example-client behavior. Research used public documentation and local cached health; no quota-exceeding probes or orders were performed. The existing managed price connection was restarted only to apply the documented allowance below.

## Published broker policy

The official FAQ **“웹소켓 실시간 최대 등록 사용 개수”**, published 2022-08-05 and retrieved from the current FAQ listing on 2026-11-12, permits **one session and 41 total registrations across all real-time product types**. It explicitly combines trade data, order books, and execution notices. This is 41 registrations, not 41 distinct symbols. [Official FAQ](https://apiportal.koreainvestment.com/community/10000000-0000-0011-0000-000000000002/post/171eef4d-06c1-4d75-980e-dedffb54386a), [public JSON representation](https://apiportal.koreainvestment.com/api/forums/10000000-0000-0011-0000-000000000002/posts/171eef4d-06c1-4d75-980e-dedffb54386a).

Short source excerpts:

> 세션 연결은 1개만 가능합니다.
>
> 체결 데이터(H0STCNT0)와 호가 데이터(H0STASP0), 체결통보 등 모든 등록 건을 합쳐 최대 41개 등록 가능합니다.

| Question | Conclusion and evidence boundary |
| --- | --- |
| Is the cap 40 or 41? | The FAQ states 41 combined registrations. |
| How should `TR_ID + TR_KEY` be budgeted? | Count separate feed/key requests, including trade plus order book for one symbol, separately. This is a budgeting inference from the aggregate rule and distinct request shapes below; formal deduplication semantics are not documented. |
| Is the identity scope AppKey or HTS ID? | The FAQ permits one session but does not name the identity that owns it. Do not claim either scope as verified. |
| Can Core and Exec multiply capacity using separate sockets? | No such entitlement is established. Process or socket separation does not justify multiplying the published allowance. |
| Is a higher allowance available or authorized? | No supporting broker policy or account-specific authorization was found. |

Capacity arithmetic must include every registration. Forty trade registrations plus one execution-notice registration equal 41 only within an allowed single session. Core currently runs price and notice consumers on separate sockets; enabling both is not made valid by reserving one registration. Use only one current consumer for the same allowance. Multiplexing both onto one connection would require a separate implementation change.

## What the official examples establish

- The repository describes its examples as reference material: “고객님의 개발 부담을 줄이고자 참고용으로 제공되고 있습니다.” Its README does not publish a WebSocket registration cap. [Official README](https://github.com/koreainvestment/open-trading-api/blob/3a9be59a3ed8745720dc3aa9d584bdf3b7830362/README.md).
- In `examples_user/kis_auth.py`, `subscribe()` keys `open_map` by the request function's name. `add_open_map()` appends all symbols to that function's item list; the runner checks the number of map keys, then sends each item. Therefore its `> 40` check counts function buckets, not subscriptions or symbols. This is a source-code observation, **not a server policy**. [Subscription registration](https://github.com/koreainvestment/open-trading-api/blob/3a9be59a3ed8745720dc3aa9d584bdf3b7830362/examples_user/kis_auth.py#L800), [map and item storage](https://github.com/koreainvestment/open-trading-api/blob/3a9be59a3ed8745720dc3aa9d584bdf3b7830362/examples_user/kis_auth.py#L632), [runner check](https://github.com/koreainvestment/open-trading-api/blob/3a9be59a3ed8745720dc3aa9d584bdf3b7830362/examples_user/kis_auth.py#L740).
- The KRX trade example constructs `H0STCNT0` and the KRX order-book example constructs `H0STASP0`, each with the stock code in `tr_key`. These are distinct requests for one symbol; the samples do not state whether those requests consume one or two quota slots. [Trade request](https://github.com/koreainvestment/open-trading-api/blob/3a9be59a3ed8745720dc3aa9d584bdf3b7830362/examples_llm/domestic_stock/ccnl_krx/ccnl_krx.py#L54), [order-book request](https://github.com/koreainvestment/open-trading-api/blob/3a9be59a3ed8745720dc3aa9d584bdf3b7830362/examples_llm/domestic_stock/asking_price_krx/asking_price_krx.py#L53).

## Deployment decision

The running Core reported `KIS_REALTIME_ORDER_NOTICE_ENABLED=false`; its notice health was disabled, and the local process socket inventory showed one established KIS realtime connection. Core initially limited the combined price/book budget to 40 while its existing sender already imposed a hard maximum of 41. [Budget calculation](../../apps/api/app/quotes/kis_realtime_prices.py#L1680), [subscription sender](../../apps/api/app/quotes/kis_realtime_prices.py#L1753).

Only the private deployment setting `KIS_REALTIME_PRICE_MAX_SUBSCRIPTIONS=41` was added, followed by a managed restart. Python logic, order controls, decider scores, and strategy eligibility were unchanged. Subsequent local read APIs reported 54 desired symbols, 41 trade subscriptions, zero book subscriptions, 13 capacity-limited symbols, an active connection, and no subscription error. All 54 symbols had display prices; a subscribed symbol may still use a labeled REST price until its next trade arrives.

The default 40 remains unchanged as a local configuration value. It does not authorize enabling the separate notice socket beside the price socket. Any future shared-session implementation must include notices within the same total of 41. AppKey/HTS-ID identity scope and account-specific exceptions are not established by this FAQ; separate processes or sockets do not grant additional capacity.

The browser investigation tab was closed after the portal's recurring developer-tools alert. The decisive FAQ was retrieved using two public JSON reads without executing page scripts. No further browser access or live quota test was used.
