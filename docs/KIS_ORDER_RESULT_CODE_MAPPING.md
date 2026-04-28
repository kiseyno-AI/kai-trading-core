# KIS domestic cash order result-code mapping

This document records the internal state contract used by the first KIS domestic cash order adapter in `apps/api/app/orders/service.py`.

It is intentionally narrow:
- domestic stock cash submit
- cancel
- amend or replace
- inquiry-driven reconciliation

## Submit / cancel / amend REST result mapping

| KIS response shape | Internal request status | Internal order status | Event behavior |
| --- | --- | --- | --- |
| `rt_cd == "0"` on submit | `submitted` | `submitted` | append `submit_response` |
| `rt_cd == "0"` on cancel | existing request row unchanged | `cancel_pending` | append `cancel_request` with `rest_submit` source |
| `rt_cd == "0"` on amend | existing request row unchanged | `replace_pending` | append `replace_request` with `rest_submit` source |
| non-zero `rt_cd` returned as `KISAPIError` | `rejected` for submit | `rejected` for submit, otherwise keep prior control-state snapshot | append `reject` |
| timeout / transport / ambiguous 5xx on submit | `submitting` | `pending_submit` | append `submit_response` error snapshot and require later inquiry |
| timeout / transport / ambiguous 5xx on cancel | existing request row unchanged | `cancel_pending` | preserve pending control state and surface a pending / upstream error |
| timeout / transport / ambiguous 5xx on amend | existing request row unchanged | `replace_pending` | preserve pending control state and surface a pending / upstream error |
| non-ambiguous configuration or upstream transport failure | `failed` for submit | `rejected` for submit, otherwise keep prior order state | preserve error payload and raise stable upstream error |

## Inquiry reconciliation mapping

Single-order refresh uses two KIS inquiry surfaces:
- daily order and fill inquiry
- cancelable order inquiry

The adapter maps the inquiry payloads into internal broker-order states as follows.

| Inquiry evidence | Internal order status | Event type |
| --- | --- | --- |
| `cncl_yn == "Y"` or `cnc_cfrm_qty > 0` | `canceled` | `cancel_confirmed` |
| `rjct_qty > 0` and `tot_ccld_qty == 0` | `rejected` | `reject` |
| `tot_ccld_qty >= ord_qty` | `filled` | `fill` |
| `tot_ccld_qty > 0` and `rmn_qty > 0` | `partial_fill` | `partial_fill` |
| row still present in cancelable inquiry after replace request | `replaced` | `replace_confirmed` |
| row present in cancelable inquiry without fill/cancel evidence | `accepted` | `reconcile_snapshot` |
| no stronger inquiry evidence but order already submitted | `submitted` | `reconcile_snapshot` |

## Realtime notice mapping

The KIS websocket order-notice consumer subscribes to `H0STCNI0` in real accounts and `H0STCNI9` in mock accounts. It maps domestic order-notice payloads into internal order events as follows.

| Realtime evidence | Internal order status | Event type |
| --- | --- | --- |
| `RFUS_YN` indicates rejection | `rejected` | `reject` |
| `CNTG_YN == "2"` and cumulative filled quantity is still below requested quantity | `partial_fill` | `partial_fill` |
| `CNTG_YN == "2"` and cumulative filled quantity reaches requested quantity | `filled` | `fill` |
| non-fill notice with cancel classification (`RCTF_CLS` cancel or local order already `cancel_pending`) | `canceled` | `cancel_confirmed` |
| non-fill notice with replace classification (`RCTF_CLS` replace or local order already `replace_pending`) | `replaced` | `replace_confirmed` |
| non-fill notice without reject/cancel/replace evidence | `accepted` | `accept` |

Realtime-notice persistence is append-only. If an older notice arrives after a newer broker snapshot is already known, the older event is still stored for audit, but the current `broker_orders` row is not allowed to regress.

## Idempotency and persistence guardrails

- Submit idempotency is enforced before any broker call. The service compares the incoming payload to the stored `request_payload` for the same `account_reference + client_order_id`.
- Same payload: return the stored request, order, and events with `idempotent_replay=true`.
- Different payload: raise an idempotency conflict and do not call KIS.
- Every control path preserves broker payloads in at least one of:
  - `order_requests.submission_payload`
  - `broker_orders.raw_latest_payload`
  - `broker_order_events.raw_payload`

## Notes

- `submitted` means KIS accepted the HTTP request and returned a broker order id, not that the order is already filled.
- `accepted` is currently an inquiry-derived state used when the order is still live at the broker and cancelable inquiry shows it as active.
- `pending_submit`, `cancel_pending`, and `replace_pending` are intentionally durable so operators can reconcile ambiguous broker outcomes without risking duplicate submission.
