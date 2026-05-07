# GET /agent-context/symbol-day

## Summary
Returns compact symbol/day analysis context for downstream agents. The payload now combines the latest `quote_context` from `current_quote_cache` with persisted `price_context` from `daily_price_contexts`, including quote freshness, optional bid/ask spread, derived liquidity (`turnover_source`, provider vs estimated turnover), and explicit `price_bar_quality` flags for zero-open or zero-turnover bars. `price_context_history` still carries up to the latest seven stored rows ending at the requested `market_date`, and the same response keeps structured `scores`, compact `persisted_factor_rows`, `market_relative_context`, and `context_completeness` metadata so downstream agents can distinguish present factor rows, genuinely missing rows, null submetrics, sparse `daily_signal_features`, and unavailable peer mappings without guessing.

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
        "freshness": {
          "trade_date_relation_to_request": "same_market_date",
          "same_market_date_as_request": true
        },
        "spread": {
          "available": true,
          "bid_price": 72050,
          "ask_price": 72150,
          "spread_points": 100
        }
      },
      "market_relative_context": {
        "status": "available",
        "market": "KOSPI",
        "breadth": {
          "up_count": 470,
          "down_count": 260,
          "flat_count": 80
        },
        "relative_strength": {
          "return_percentile": 0.95,
          "volume_percentile": 0.93
        }
      },
      "price_context": {
        "trade_date": "2026-04-15",
        "open_price": 70800,
        "high_price": 72500,
        "low_price": 70300,
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
        "candle": {"direction": "bullish", "body_size": "medium", "wick_bias": "upper"},
        "session_range": {
          "points": 2200,
          "percent_of_open": 3.1073,
          "close_position_in_range": 0.8182,
          "close_position_label": "near_high"
        },
        "gap": {
          "previous_trade_date": "2026-04-14",
          "previous_close_price": 70400,
          "gap_points": 400,
          "gap_percent": 0.5682,
          "direction": "up",
          "filled_intraday": true,
          "reliable": true
        }
      },
      "price_context_history": [
        {
          "market_date": "2026-04-14",
          "source": "pykrx",
          "captured_at": "2026-04-14T15:30:00+09:00",
          "price_context": {
            "trade_date": "2026-04-14",
            "close_price": 70400,
            "candle": {"direction": "bullish"}
          }
        },
        {
          "market_date": "2026-04-15",
          "source": "pykrx",
          "captured_at": "2026-04-15T15:30:00+09:00",
          "price_context": {
            "trade_date": "2026-04-15",
            "close_price": 72100,
            "candle": {"direction": "bullish"}
          }
        }
      ],
      "technical_snapshot": {"market_date": "2026-04-15", "trend_state": "uptrend"},
      "action_summary": {"market_date": "2026-04-15", "action_label": "hold"},
      "scores": {
        "unified_value": {
          "status": "available",
          "score": 0.873,
          "score_scale": "0_to_1_percentile_mean",
          "rank": 14,
          "percentile": 0.958,
          "higher_is_better": true,
          "meaning": "Composite value attractiveness score across stored cross-sectional value percentiles.",
          "method": "Equal-weight mean of PER, PBR, dividend yield, FCF yield, and EV/EBITDA percentiles; requires all five components, with lower PER/PBR/EV-EBITDA and higher dividend yield/FCF yield ranking better.",
          "components_used": 5,
          "components_possible": 5,
          "coverage_notes": [],
          "as_of_market_date": "2026-04-15"
        },
        "quality": {
          "status": "partial",
          "score": 0.741,
          "score_scale": "0_to_1_percentile_mean",
          "rank": 29,
          "percentile": 0.884,
          "higher_is_better": true,
          "components_used": 5,
          "components_possible": 6,
          "coverage_notes": [
            "The quality score was computed from the available components only.",
            "Missing quality components: accruals_ratio."
          ],
          "as_of_market_date": "2026-04-15"
        },
        "momentum": {
          "status": "available",
          "score": 0.914,
          "score_scale": "0_to_1_percentile",
          "rank": 21,
          "percentile": 0.914,
          "higher_is_better": true,
          "components_used": 3,
          "components_possible": 3,
          "coverage_notes": [],
          "as_of_market_date": "2026-04-15"
        }
      },
      "persisted_factor_rows": {
        "advanced_value": {
          "advanced_value_score": 0.81,
          "advanced_value_classification": "eligible"
        },
        "low_risk": {
          "low_risk_score": 0.67,
          "low_risk_classification": "watchlist"
        },
        "beta": {
          "low_beta_score": 0.58,
          "low_beta_classification": "eligible"
        },
        "composite": {
          "composite_score": 0.73,
          "composite_classification": "eligible"
        }
      },
      "context_completeness": {
        "factor_rows": {
          "quality": {
            "row_status": "present",
            "null_submetric_fields": ["accruals_ratio"]
          }
        },
        "signal_features": {"row_status": "present"},
        "sector_peer_mapping": {"status": "unavailable"},
        "requested_symbol_coverage": {
          "signal_features": {
            "present_rows": 1,
            "missing_rows": 0,
            "coverage_status": "complete"
          }
        }
      },
      "fundamentals": {"latest_business_year": 2025},
      "recent_disclosures": {"count": 5, "items": []}
    }
  ]
}
```
