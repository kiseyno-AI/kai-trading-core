# POST /broker/kis/token

## Summary

Issue or reuse the environment-scoped KIS OAuth token and return operational metadata. This is an authenticated mutation endpoint and never returns the raw access token.

## Authentication

Set `UPSTREAM_BEARER_TOKEN`, then send its exact value with the case-sensitive Bearer scheme:

```http
Authorization: Bearer <UPSTREAM_BEARER_TOKEN>
```

The route performs its own constant-time credential comparison, including when its router is mounted without the application-wide middleware. If `UPSTREAM_BEARER_TOKEN` is unset, the endpoint is disabled.

## Query parameters

- `force_refresh=false` reuses a token until its shared `refresh_after` boundary.
- `force_refresh=true` asks for a new generation, but still adopts a generation written by a concurrent peer and never bypasses the shared minimum issuance interval. A deferred response is a successful safe reuse, not proof that KIS issued a new token.

## Example request

```http
POST /broker/kis/token?force_refresh=true
Authorization: Bearer <UPSTREAM_BEARER_TOKEN>
```

## 200 response

```json
{
  "access_token_present": true,
  "token_type": "Bearer",
  "expires_in": 3578,
  "cached": true,
  "refresh_deferred": true,
  "defer_reason": "minimum_issue_interval"
}
```

`defer_reason` is omitted when no refresh was deferred. Its only values are `minimum_issue_interval` and `peer_refresh`. `expires_in` is remaining usable lifetime after response latency and any earlier provider absolute-expiry bound.

## Migrating from the raw-token response

This is an intentional response-schema break. Older clients read the raw KIS credential:

```text
Before: response.access_token -> build or store a KIS Authorization header
```

They must now use operational metadata only:

```text
Now: response.access_token_present -> confirm availability
     response.cached -> distinguish reuse from issuance
     response.refresh_deferred + response.defer_reason -> handle a safe deferred force request
```

Do not reconstruct a KIS bearer header from this endpoint. KIS authentication and token storage remain server-side. Treat an authenticated `200` with `refresh_deferred: true` as successful reuse, not as evidence that the provider issued a new token.

## Errors

- `401 Unauthorized`: missing, malformed, or incorrect Bearer authorization. The response includes `WWW-Authenticate: Bearer`.
- `502 Bad Gateway`: KIS issuance, validation, or safe-cache handling failed. The detail is intentionally generic.
- `503 Service Unavailable`: `UPSTREAM_BEARER_TOKEN` is not configured, so the mutation endpoint is disabled.

No response or error detail includes the KIS access token, app credentials, raw upstream exception, or account data.
