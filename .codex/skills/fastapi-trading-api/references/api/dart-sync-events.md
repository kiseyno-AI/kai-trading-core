# POST /disclosures/dart/sync/events

## Summary
Syncs recent DART disclosure events into the local store.

## Example request
```http
POST /disclosures/dart/sync/events?days=5
```

## Example response
```json
{
  "status": "ok",
  "days": 5,
  "events_synced": 182
}
```
