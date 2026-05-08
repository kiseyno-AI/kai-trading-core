# GET /agent-context/symbol-day

## Summary
Returns compact symbol/day analysis context for downstream agents. The payload combines quote, price, factor, and PyKRX enrichment blocks with stored DART fundamentals and recent disclosure events. The DART section now includes `fundamentals.latest_snapshot.fundamental_context` for canonical BS/IS/CF facts plus profitability, leverage/liquidity, and cash-flow quality metrics, and `fundamentals.disclosure_context` for recent corrections and ownership or large-holder disclosure signals.

The response also includes a compact `factor_summary` block per symbol/day. This is the prompt-trim-safe status-first summary for persisted factor families and stored signal features. It complements the richer `scores` and `persisted_factor_rows` blocks rather than replacing them.

The response now also includes a compact `liquidity_context` block per symbol/day. It keeps the daily turnover state explicit with `available`, `derived`, `stale`, `missing_price_or_volume`, and `storage_unavailable` statuses, plus low-risk liquidity ranking fields and intraday quote/orderbook freshness where those rows exist.

Expected decider-facing context blocks are status-first even when the requested-date source row is missing. `quote_context`, `intraday_execution_context`, and the provider-backed optional PyKRX blocks stay object-shaped with explicit `status`, `reason`, and `freshness_provenance` instead of collapsing to raw `null`.

The response also includes a compact `decider_data_quality_context` block per symbol/day. This is the decider-facing availability and provenance summary over factor-score coverage, liquidity/turnover, intraday freshness, financial/cash-flow detail, disclosures/news, investor flow and short-selling, sector/relative-strength context, and technical evidence. It keeps `available`, `partial`, `stale`, `empty`, `unavailable`, and `provider_error` semantics explicit so downstream deciders can distinguish no recent items from provider/storage failures, and it now adds `decision_priority` plus per-block `decision_guidance` so deciders can separate core execution blockers from auxiliary confidence-only gaps.

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
    "fundamentals_count": 1,
    "quote_context_count": 1
  },
  "items": [
    {
      "symbol": "005930",
      "company": {
        "name_ko": "삼성전자",
        "dart": {
          "corp_code": "00126380"
        }
      },
      "quote_context": {
        "status": "available",
        "available": true,
        "source": "kis",
        "trade_date": "2026-04-15",
        "price": 71550,
        "spread": {
          "status": "available",
          "spread_points": 100
        },
        "freshness_provenance": {
          "freshness_status": "fresh",
          "provenance_status": "provider_sourced"
        }
      },
      "intraday_execution_context": {
        "status": "partial",
        "market_state": "OPEN",
        "coverage": {
          "status": "partial",
          "available_blocks": 4,
          "partial_blocks": 1,
          "total_blocks": 5
        },
        "quote": {
          "status": "available",
          "spread_points": 100
        },
        "orderbook": {
          "status": "available",
          "imbalance": 0.076923
        },
        "investor_program_activity": {
          "status": "partial"
        },
        "news_hints": {
          "status": "available",
          "item_count": 1
        },
        "freshness_provenance": {
          "freshness_status": "fresh",
          "provenance_status": "provider_sourced"
        }
      },
      "fundamentals": {
        "latest_snapshot": {
          "filing": {
            "rcept_no": "20250312000123",
            "report_name": "사업보고서 (2024.12)",
            "receipt_date": "2025-03-12",
            "is_correction": false
          },
          "period": {
            "business_year": 2024,
            "report_code": "11011",
            "period_end_date": "2024-12-31"
          },
          "fundamental_context": {
            "normalized_facts": {
              "balance_sheet": {
                "current_assets": {
                  "status": "available",
                  "current_value": 240
                },
                "current_liabilities": {
                  "status": "available",
                  "current_value": 120
                }
              },
              "cash_flow_statement": {
                "operating_cash_flow": {
                  "status": "available",
                  "current_value": 24
                }
              }
            },
            "analysis": {
              "leverage_liquidity": {
                "status": "available",
                "metrics": {
                  "current_ratio": "2.0000",
                  "cash_ratio": "0.7500"
                }
              },
              "cash_flow_quality": {
                "status": "partial",
                "metrics": {
                  "operating_cash_flow_to_net_income": "1.6000"
                }
              }
            },
            "source_provenance": {
              "provider": "dart",
              "dataset": "stored_dart_fundamentals",
              "rcept_no": "20250312000123",
              "fs_div": "CFS",
              "currency": "KRW",
              "freshness_provenance": {
                "freshness_status": "available_but_old"
              }
            }
          }
        },
        "disclosure_context": {
          "summary": {
            "correction_count": 1,
            "ownership_related_count": 1,
            "large_holder_related_count": 1
          },
          "recent_ownership_large_holder_changes": [
            {
              "rcept_no": "20260406000055",
              "report_nm": "주식등의 대량보유상황보고서",
              "is_large_holder_related": true
            }
          ]
        },
        "freshness_provenance": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced"
        }
      },
      "recent_disclosures": {
        "count": 2,
        "items": [
          {
            "rcept_no": "20260407000001",
            "report_nm": "[기재정정] 사업보고서 (2025.12)",
            "is_correction": true,
            "disclosure_tags": ["financial_report", "annual_report", "correction"]
          },
          {
            "rcept_no": "20260406000055",
            "report_nm": "주식등의 대량보유상황보고서",
            "is_large_holder_related": true
          }
        ]
      },
      "context_provenance": {
        "fundamentals": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced"
        },
        "recent_disclosures": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced"
        }
      },
      "factor_summary": {
        "requested_market_date": "2026-04-15",
        "factor_scores": {
          "value": {
            "status": "available",
            "source_status": "scored",
            "score": 0.92,
            "score_available": true,
            "rank": 8,
            "percentile": 0.96,
            "source": "value_factor_v1",
            "feature_version": "value_factor_v1",
            "as_of_market_date": "2026-04-15",
            "captured_at": "2026-04-15T15:00:00+09:00",
            "provenance": {
              "freshness_status": "fresh",
              "provenance_status": "derived_from_provider_data",
              "source_tables": ["daily_value_factor_results"]
            }
          },
          "momentum": {
            "status": "not_eligible",
            "source_status": "trend_not_confirmed",
            "score": 0.67,
            "score_available": true,
            "rank": 55,
            "percentile": 0.67,
            "eligible": false,
            "source": "daily_price_bars",
            "feature_version": "price_momentum_v1",
            "as_of_market_date": "2026-04-15",
            "captured_at": "2026-04-15T15:00:00+09:00",
            "provenance": {
              "freshness_status": "fresh",
              "provenance_status": "derived_from_provider_data",
              "source_tables": ["daily_momentum_factor_results"]
            }
          },
          "low_risk": {
            "status": "insufficient_history",
            "source_status": "insufficient_history",
            "score_available": false,
            "source": "low_risk_v1",
            "feature_version": "low_risk_v1",
            "as_of_market_date": "2026-04-15",
            "captured_at": "2026-04-15T15:00:00+09:00",
            "provenance": {
              "freshness_status": "fresh",
              "provenance_status": "derived_from_provider_data",
              "source_tables": ["daily_low_risk_factor_results"]
            }
          }
        },
        "signal_features": {
          "status": "available",
          "signal_count": 1,
          "row_kind": "direct_plus_fallback",
          "direct_signal_source_count": 1,
          "fallback_groups_present": ["price_action", "factor_scores"],
          "source": "kis_ranking",
          "feature_version": "kis_ranking",
          "as_of_market_date": "2026-04-15",
          "captured_at": "2026-04-15T15:05:00+09:00",
          "provenance": {
            "freshness_status": "fresh",
            "provenance_status": "provider_plus_fallback",
            "source_tables": ["daily_signal_features"]
          }
        }
      },
      "liquidity_context": {
        "status": "derived",
        "reason": "derived_from_close_price_x_volume",
        "turnover": {
          "status": "derived",
          "trade_date": "2026-04-15",
          "turnover_krw": 8520000000,
          "turnover_source": "derived",
          "provider_turnover_krw": null,
          "derived_turnover_krw": 8520000000,
          "volume": 120000,
          "close_price": 71000,
          "provenance": {
            "freshness_status": "fresh",
            "provenance_status": "provider_plus_fallback"
          }
        },
        "ranking": {
          "status": "available",
          "avg_trading_value_20d": 1200000000,
          "liquidity_percentile": 0.91,
          "passes_liquidity_floor": true
        },
        "intraday": {
          "status": "partial",
          "freshness_status": "fresh",
          "quote_spread_points": 100,
          "orderbook_imbalance": 0.076923,
          "volume_power_rank": 3,
          "quote_balance_rank": 5
        }
      },
      "decider_data_quality_context": {
        "status": "partial",
        "decision_priority": {
          "core_execution": {
            "status": "available",
            "blocking_by_default": true,
            "degraded_block_names": []
          },
          "auxiliary_context": {
            "status": "partial",
            "blocking_by_default": false,
            "empty_is_neutral_by_default": true,
            "degraded_block_names": ["disclosures_news", "financials_cashflow"]
          }
        },
        "block_status_counts": {
          "available": 2,
          "partial": 3,
          "stale": 1,
          "empty": 0,
          "unavailable": 0,
          "provider_error": 1
        },
        "blocks": {
          "liquidity_turnover": {
            "status": "available",
            "turnover": {
              "status": "partial",
              "source_status": "derived",
              "turnover_source": "derived",
              "turnover_krw": 8520000000
            },
            "liquidity_ranking": {
              "status": "available",
              "avg_trading_value_20d": 1200000000
            },
            "intraday_liquidity": {
              "status": "partial",
              "quote_spread_points": 100,
              "quote_balance_rank": 5
            }
          },
          "disclosures_news": {
            "status": "provider_error",
            "decision_guidance": {
              "decision_tier": "auxiliary_context",
              "severity": "low",
              "default_missing_effect": "confidence_reducer",
              "empty_is_neutral_by_default": true
            },
            "providers": {
              "dart_recent_events": {
                "status": "empty",
                "item_count": 0
              },
              "external_news": {
                "status": "provider_error",
                "provider_reason": "naver_search_credentials_missing"
              }
            }
          },
          "technical_evidence": {
            "status": "available",
            "decision_guidance": {
              "decision_tier": "supporting_context",
              "severity": "medium"
            },
            "support_price": 68000,
            "resistance_price": 70800,
            "breakout_20d": true
          }
        }
      }
    }
  ]
}
```

## Missing-data example
```json
{
  "quote_context": {
    "status": "unavailable",
    "reason": "no_current_quote_cache_row",
    "freshness_provenance": {
      "freshness_status": "missing",
      "provenance_status": "provider_sourced"
    }
  },
  "intraday_execution_context": {
    "status": "unavailable",
    "reason": "no_current_kis_intraday_snapshot_cache_row",
    "quote": {"status": "unavailable"},
    "orderbook": {"status": "unavailable"},
    "liquidity": {"status": "unavailable"},
    "investor_program_activity": {"status": "unavailable"},
    "news_hints": {"status": "unavailable"}
  },
  "market_relative_context": {
    "status": "unavailable",
    "reason": "no_stored_market_relative_context_row_for_request_date"
  }
}
```

## Notes
- `fundamentals.latest_snapshot.fundamental_context` is the canonical normalized DART block for downstream agents.
- Missing DART facts are explicit with `status="missing"` instead of being silently omitted.
- `fundamentals.disclosure_context` summarizes recent corrections and ownership or large-holder changes from the existing DART disclosure event stream; it does not require a separate ownership-only endpoint.
- `factor_summary.factor_scores` covers the persisted factor families `value`, `unified_value`, `quality`, `momentum`, `advanced_value`, `low_risk`, `beta`, and `composite`.
- `liquidity_context.turnover` is the prompt-trim-safe turnover summary. `status="derived"` means provider turnover was missing or zero but `close_price * volume` was safely derivable from the stored daily bar. `status="stale"` means Core only had a prior stored price-context row for that symbol.
- `decider_data_quality_context` is the compact prompt-safe quality overlay that both deciders can read without re-parsing the heavier raw context blocks. It surfaces source-specific `empty` vs `provider_error` semantics for DART and external news, keeps stale or derived liquidity/intraday inputs explicit, and now marks which gaps are `core_execution`, `supporting_context`, or `auxiliary_context`.
- `decision_priority.auxiliary_context` is intentionally non-blocking by default. Empty recent news means no captured recent optional context, not automatic event risk.
- `blocks.financials_cashflow.decision_guidance` marks annual-only or missing detailed quarterly/cash-flow detail as confidence-reducing by default unless the trading thesis explicitly depends on those fields.
- `factor_summary` status values are canonical and machine-readable: `available`, `not_eligible`, `insufficient_history`, `missing_source_row`, `storage_unavailable`, and `not_computed_for_market_date`.
- `factor_summary.signal_features` gives the same compact status/provenance treatment to the persisted `daily_signal_features` row.
