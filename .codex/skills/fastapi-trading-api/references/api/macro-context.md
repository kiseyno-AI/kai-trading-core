# GET /macro/context and POST /macro/context/refresh

## Summary
Reads and refreshes persisted slow-moving macro context for agent and decider inputs.

The MVP uses no-key World Bank indicators by default for Korea GDP growth, CPI inflation, and unemployment. Optional FRED series are only fetched when `FRED_API_KEY` is configured; otherwise FRED is reported with `disabled_missing_api_key`.

Macro context is contextual input only. It is not a direct buy/sell signal, position-sizing signal, or live-trading trigger. `macro_regime_summary` labels are deterministic threshold labels and are not LLM-generated.

## Example read request
```http
GET /macro/context?as_of_date=2026-06-20
```

## Example refresh request
```http
POST /macro/context/refresh?force=true
```

## Example response
```json
{
  "schema_version": "macro_context_v1",
  "status": "available",
  "as_of_date": "2026-06-20",
  "latest_fetched_at": "2026-06-20T06:00:00+00:00",
  "freshness": {
    "stale_after_hours": 72,
    "status": "fresh"
  },
  "source_statuses": {
    "world_bank": {
      "status": "available",
      "series_count": 3,
      "available_count": 3
    },
    "fred": {
      "status": "disabled_missing_api_key",
      "reason": "FRED_API_KEY is not configured"
    }
  },
  "series": [
    {
      "source": "world_bank",
      "series_id": "NY.GDP.MKTP.KD.ZG",
      "series_name": "Korea real GDP growth",
      "status": "available",
      "source_status": "available",
      "latest_value": 2.6,
      "latest_period": "2024",
      "observation_date": "2024-12-31",
      "unit": "percent",
      "frequency": "annual"
    }
  ],
  "macro_regime_summary": {
    "schema_version": "macro_regime_summary_v1",
    "method": "deterministic_threshold_labels",
    "growth_label": "growth_expansion",
    "inflation_label": "inflation_stable",
    "labor_label": "labor_tight",
    "composite_label": "macro_growth_supportive",
    "freshness_label": "macro_context_fresh",
    "llm_generated": false
  },
  "usage_policy": {
    "contextual_input_only": true,
    "direct_buy_sell_signal": false,
    "llm_generated": false
  }
}
```

## Notes
- Stored rows live in `macro_context_observations` and include `source`, `series_id`, `observation_period`, `observation_date`, `value`, `fetched_at`, and `source_status`.
- Refreshes return partial/unavailable source statuses for upstream failures instead of failing the whole daily pipeline.
- `macro_context_refresh` is the daily pipeline stage that refreshes this context before agent feedback.
