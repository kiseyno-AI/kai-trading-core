# GET /agent-context/symbol-day

## Summary
Returns compact symbol/day analysis context for downstream agents. The payload combines the latest `quote_context` from `current_quote_cache` with persisted `price_context` from `daily_price_contexts`, structured factor-score summaries, same-day `signal_features`, and persisted PyKRX enrichment blocks such as `sector_peer_context`, `market_breadth_context`, `investor_flow_context`, `short_selling_context`, and `valuation_context`. Major blocks now carry machine-readable `freshness_provenance`, and the top-level `context_provenance` mirror preserves stale, available-but-old, and fallback evidence even when a current block is absent.

## Example request
```http
GET /agent-context/symbol-day?market_date=2026-04-15&symbols=005930&fs_div=CFS&disclosures_limit=5
```

## Example response
```json
{
  "request": {
    "market_date": "2026-04-15",
    "symbols": ["005930"],
    "fs_div": "CFS",
    "disclosures_limit": 5
  },
  "summary": {
    "requested_symbol_count": 1,
    "matched_symbol_count": 1,
    "quote_context_count": 1,
    "price_context_count": 1,
    "signal_feature_count": 1,
    "factor_row_counts": {
      "unified_value": 1,
      "quality": 1,
      "momentum": 1,
      "advanced_value": 1,
      "low_risk": 1,
      "beta": 1,
      "composite": 1
    },
    "context_completeness": {
      "signal_features": {
        "present_rows": 1,
        "missing_rows": 0,
        "coverage_status": "complete"
      }
    }
  },
  "items": [
    {
      "symbol": "005930",
      "market_date": "2026-04-15",
      "company": {"name_ko": "삼성전자", "market": "KOSPI"},
      "quote_context": {
        "available": true,
        "source": "kis",
        "trade_date": "2026-04-15",
        "cached_at": "2026-04-15T09:57:00+09:00",
        "freshness_provenance": {
          "freshness_status": "fresh",
          "provenance_status": "provider_sourced",
          "issue_types": [],
          "requested_market_date": "2026-04-15",
          "as_of_market_date": "2026-04-15",
          "as_of_timestamp": "2026-04-15T09:57:00+09:00",
          "latest_available_market_date": "2026-04-15",
          "latest_available_timestamp": "2026-04-15T09:57:00+09:00",
          "trade_date_relation_to_request": "same_market_date",
          "source_tables": ["current_quote_cache", "quote_snapshots"],
          "source_provider": "kis",
          "row_count": 1,
          "history_row_count": 1,
          "is_missing": false,
          "is_stale": false,
          "is_available_but_old": false,
          "is_provider_sourced": true,
          "is_derived": false,
          "is_fallback": false
        }
      },
      "price_context": {
        "trade_date": "2026-04-15",
        "close_price": 72100,
        "liquidity": {
          "turnover_krw": 9300000000,
          "turnover_source": "provider",
          "turnover_is_estimated": false
        },
        "price_bar_quality": {
          "open_price_zero": false,
          "turnover_missing_or_zero": false,
          "gap_unreliable_due_to_open": false,
          "issue_codes": []
        },
        "freshness_provenance": {
          "freshness_status": "fresh",
          "provenance_status": "derived_from_provider_data",
          "issue_types": [],
          "requested_market_date": "2026-04-15",
          "as_of_market_date": "2026-04-15",
          "latest_available_market_date": "2026-04-15",
          "trade_date_relation_to_request": "same_market_date",
          "source_tables": ["daily_price_contexts", "daily_price_bars"],
          "source_provider": "pykrx",
          "row_count": 1,
          "history_row_count": 7,
          "is_missing": false,
          "is_stale": false,
          "is_available_but_old": false,
          "is_provider_sourced": false,
          "is_derived": true,
          "is_fallback": false
        }
      },
      "signal_features": {
        "symbol": "005930",
        "market_date": "2026-04-15",
        "source": "kis_ranking",
        "signal_count": 2,
        "volume_power_rank": 3,
        "overtime_volume_rank": 7,
        "row_kind": "direct_plus_fallback",
        "provenance": {
          "row_kind": "direct_plus_fallback",
          "direct_signal_source_count": 2,
          "fallback_groups_present": [
            "price_action",
            "liquidity",
            "returns",
            "volatility",
            "factor_scores",
            "quote_snapshot"
          ]
        },
        "freshness": {
          "market_date": "2026-04-15",
          "price_bars": {
            "trade_date": "2026-04-15",
            "trade_date_relation_to_market_date": "same_market_date"
          },
          "quote_snapshot": {
            "trade_date": "2026-04-15",
            "trade_date_relation_to_market_date": "same_market_date"
          }
        },
        "fallback_features": {
          "price_action": {
            "close_return_1d_pct": 2.43553,
            "candle_direction": "bullish",
            "close_location_label": "near_high"
          },
          "factor_scores": {
            "quality": {
              "score": 0.741,
              "status": "partial"
            }
          }
        },
        "freshness_provenance": {
          "freshness_status": "fresh",
          "provenance_status": "provider_plus_fallback",
          "issue_types": ["derived_or_fallback"],
          "requested_market_date": "2026-04-15",
          "as_of_market_date": "2026-04-15",
          "latest_available_market_date": "2026-04-15",
          "trade_date_relation_to_request": "same_market_date",
          "source_tables": ["daily_signal_features", "daily_price_bars", "current_quote_cache"],
          "source_provider": "kis_ranking",
          "row_count": 1,
          "history_row_count": 1,
          "is_missing": false,
          "is_stale": false,
          "is_available_but_old": false,
          "is_provider_sourced": false,
          "is_derived": true,
          "is_fallback": true
        }
      },
      "scores": {
        "quality": {
          "status": "partial",
          "score": 0.741,
          "freshness_provenance": {
            "freshness_status": "fresh",
            "provenance_status": "derived_from_provider_data",
            "issue_types": [],
            "requested_market_date": "2026-04-15",
            "as_of_market_date": "2026-04-15",
            "latest_available_market_date": "2026-04-15",
            "source_tables": ["daily_quality_factor_results"],
            "source_provider": "internal_factor_pipeline",
            "row_count": 1,
            "history_row_count": 1
          }
        },
        "momentum": {
          "status": "available",
          "score": 0.914
        }
      },
      "persisted_factor_rows": {
        "quality": {
          "score": 0.741,
          "freshness_provenance": {
            "freshness_status": "fresh",
            "provenance_status": "derived_from_provider_data",
            "issue_types": [],
            "requested_market_date": "2026-04-15",
            "as_of_market_date": "2026-04-15",
            "latest_available_market_date": "2026-04-15",
            "source_tables": ["daily_quality_factor_results"],
            "source_provider": "internal_factor_pipeline",
            "row_count": 1,
            "history_row_count": 1
          }
        }
      },
      "fundamentals": {
        "latest_snapshot": {
          "period": {
            "period_end_date": "2024-12-31"
          }
        },
        "freshness_provenance": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced",
          "issue_types": ["available_but_old"],
          "requested_market_date": "2026-04-15",
          "as_of_market_date": "2024-12-31",
          "latest_available_market_date": "2024-12-31",
          "source_tables": ["dart_fundamentals_snapshots"],
          "source_provider": "dart"
        }
      },
      "recent_disclosures": {
        "count": 1,
        "items": [
          {
            "receipt_no": "20260415000123"
          }
        ],
        "freshness_provenance": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced",
          "issue_types": ["available_but_old"],
          "requested_market_date": "2026-04-15",
          "as_of_market_date": "2026-04-14",
          "latest_available_market_date": "2026-04-14",
          "source_tables": ["dart_disclosure_events"],
          "source_provider": "dart"
        }
      },
      "context_provenance": {
        "quote_context": {
          "freshness_status": "fresh",
          "provenance_status": "provider_sourced",
          "issue_types": []
        },
        "price_context": {
          "freshness_status": "fresh",
          "provenance_status": "derived_from_provider_data",
          "issue_types": []
        },
        "signal_features": {
          "freshness_status": "fresh",
          "provenance_status": "provider_plus_fallback",
          "issue_types": ["derived_or_fallback"],
          "requested_symbol_coverage": {
            "present_rows": 1,
            "missing_rows": 0,
            "coverage_status": "complete"
          }
        },
        "fundamentals": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced",
          "issue_types": ["available_but_old"]
        },
        "recent_disclosures": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced",
          "issue_types": ["available_but_old"]
        },
        "factor_rows": {
          "quality": {
            "freshness_status": "fresh",
            "provenance_status": "derived_from_provider_data",
            "issue_types": []
          }
        }
      },
      "context_completeness": {
        "factor_rows": {
          "quality": {
            "row_status": "present",
            "null_submetric_fields": ["accruals_ratio"]
          }
        },
        "signal_features": {
          "row_status": "present",
          "row_kind": "direct_plus_fallback",
          "direct_signal_source_count": 2,
          "fallback_groups_present": [
            "price_action",
            "liquidity",
            "returns",
            "volatility",
            "factor_scores",
            "quote_snapshot"
          ]
        },
        "sector_peer_mapping": {"status": "unavailable"}
      }
    }
  ]
}
```

## Notes
- `freshness_provenance` is attached to the concrete block payload when that block exists in the current response.
- `context_provenance` is the authoritative stale/fallback mirror for major blocks and remains present even when `quote_context`, `price_context`, `signal_features`, or a stored factor row is `null` for the requested `market_date`.
- Consumers should treat `freshness_status=stale` or `available_but_old` as "data exists, but it is older than the requested market date", not as a missing-data condition.
