# GET /agent-context/daily-review

Read the latest KRX Daily Review runs or the run for one session. Use the normal Core `Authorization: Bearer …` header when configured. `market_date` is optional, using `YYYY-MM-DD` in KRX local time. `limit` defaults to 10 and accepts 1–30. An unrecorded date returns `200` with `runs: []`.

```http
GET /agent-context/daily-review?market_date=2026-11-10
```

```json
{
  "market": "KRX",
  "runs": [{
    "market_date": "2026-11-10",
    "status": "running",
    "stage": "postmortem",
    "attempt": 1,
    "observed_at": "2026-11-10T21:35:00+09:00",
    "updated_at": "2026-11-10T21:35:07+09:00",
    "error": null,
    "watch_outcomes": {
      "signal-decider": {"observation_count": 35, "trade_touch_count": 2, "ask_touch_count": 6,
        "ask_entry_count": 5, "actual_entry_fill_count": 0, "actual_closed_trade_count": 0,
        "source_status": {"watch": "available", "actual_execution": "unavailable"}},
      "short-term-decider": {"observation_count": 2, "trade_touch_count": 0, "ask_touch_count": 0,
        "ask_entry_count": 0, "actual_entry_fill_count": 0, "actual_closed_trade_count": 0,
        "source_status": {"watch": "available", "actual_execution": "unavailable"}}
    }
  }]
}
```

| Field | Meaning |
| --- | --- |
| `status` | `running`, `retry_wait`, `completed`, or `failed` |
| `stage` | `evidence`, `postmortem`, `contracts`, or `complete` |
| `error` | A short machine-readable failure reason when available |
| `observed_at` | Evidence cutoff for this attempt |
| `evidence_coverage` | Source availability and counts once evidence is assembled |
| `watch_outcomes` | Per-role counts for tracked WAIT decisions, trade touches, best-ask touches, asks in the entry band, recorded fills and closed trades, with source availability |
| `evidence_sha256`, `analyst_contract_sha256` | Evidence payload and verified analyst contract digests |
| `feedback` | On completion, the Korean summary and `roles` keyed by `signal-decider` and `short-term-decider`. Each has `assessment_ko`, `findings` (`source`, `observation_ko`, `implication_ko`), and a nullable `focus`. |
| `candidates` | On completion, per-role `status`, `focus`, `base_sha256`, and `contract_sha256`. Status is `ready_for_review`, `unchanged`, `insufficient_evidence`, or `calibration_block_full`. |

Raw evidence, contract text and internal files are not returned by this route.
