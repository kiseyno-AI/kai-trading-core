# GET /eod/pipeline/status

## Summary
Operator-facing live status API for the post-close EOD pipeline.

Canonical semantics for this route and `GET /eod/freshness` live in `docs/EOD_STATUS_SEMANTICS_CONTRACT.md`.

Unlike `GET /eod/freshness`, this route focuses on in-flight work. It reads the first-class durable daily pipeline run state first, merges same-date historical official stage rows when the latest run is only a resumed subset, then falls back to persisted-row and source-run inference for stages with no official stage row. It reports the current stage status from master refresh through signal-decider evaluations for the requested `market_date`.

The same payload also reports the default-selected filtering, factor/scoring, external-news, agent-feedback, signal-decider, and signal-decider-evaluation stages. Persisted `daily_signal_features`, `external_news_items` classification state, `agent_context_feedback`, and `signal_decider_decision_snapshots` rows can mark omitted same-day stages completed/available so a resumed single-stage run does not make earlier persisted outputs look pending.

The default `market_date` is the current Seoul calendar date from `server_time[:10]`. On weekends, holidays, or other break-days, callers must pass `market_date` explicitly if they want the previous trading day's pipeline view instead of the current calendar date.

The response now also includes `market` and `market_session` blocks. `market` is the current exchange-clock state, and `market_session` describes the semantics of the requested `market_date`, including the latest valid trading date and whether live quote or intraday data is expected for that date.

The `pykrx_enrichment` stage is also the operator status source for the decider enrichment join. Completed storage coverage for `valuation_snapshot`, `investor_flow_symbol`, `investor_flow_market`, `short_selling`, `sector_classification`, and `market_breadth` means `GET /agent-context/symbol-day` can populate `pre_decider_enrichment_context.trading_value` and `flow_short_sector_context`. Decider gate readiness is reported separately through stage-level `gate_status` fields and the top-level `gate_summary`, so `pykrx_enrichment.status` can be `completed` while `gate_status` is `blocked` due below-threshold category coverage. Scheduled PyKRX daily runs publish live `pykrx_enrichment` stage progress before rows are fully visible, including downstream preflight status, category row counts, and failed categories. Live quote/orderbook snapshots are outside the EOD stage and appear in symbol-day context as available, unavailable, or `stale_eod` with a timestamp when the market is closed.

Row-count-derived stages expose missing-row diagnostics when they are partial. For example, `market_risk_filter.details.missing_count` and `missing_symbols_sample` show the full-universe coverage gap, while `gate_status`, `severity`, and `blocking_status` tell feedback/dashboard consumers whether that gap blocks the active feedback scope.

When a same-day feedback run is active, `agent_feedback_summaries` reflects the run from feedback runtime/durable state even if no official daily pipeline stage row exists. That keeps active feedback progress separate from upstream degraded coverage.

Pre-open/global stages such as `master_refresh` and `pykrx_universe_refresh` may be `skipped` with reason metadata once same-date downstream output exists and there is no active/pre-open official stage row. This prevents `current_stage` from pointing at `master_refresh` after a resumed EOD or agent-stage recovery has already completed.

## Example request
```http
GET /eod/pipeline/status?market_date=2026-04-21&fs_div=CFS
```

## Example response
```json
{
  "market_date": "2026-04-21",
  "market": {
    "market": "KRX",
    "state": "OPEN",
    "is_trading_day": true,
    "is_open": true,
    "reason": null,
    "server_time": "2026-04-21T14:12:00+09:00",
    "current_calendar_date": "2026-04-21",
    "latest_valid_trading_date": "2026-04-21",
    "market_session": "open"
  },
  "market_session": {
    "requested_date": "2026-04-21",
    "current_calendar_date": "2026-04-21",
    "latest_valid_trading_date": "2026-04-21",
    "status": "open",
    "is_trading_day": true,
    "intraday_data_expected": true,
    "quote_data_expected": true,
    "recommendation_basis": "live_intraday"
  },
  "fs_div": "CFS",
  "status": "running",
  "total_stages": 24,
  "completed_stages": 16,
  "skipped_stages": 0,
  "partial_stages": 2,
  "running_stages": 1,
  "pending_stages": 5,
  "current_stage": "technical_snapshots",
  "gate_summary": {
    "status": "blocked",
    "ready_stage_ids": ["market_risk_filter"],
    "waiting_stage_ids": [],
    "blocked_stage_ids": ["pykrx_enrichment"],
    "stages": {
      "pykrx_enrichment": {
        "stage": "pykrx_enrichment",
        "status": "partial",
        "gate_status": "blocked",
        "blocking_status": "blocking",
        "required_categories": ["sector_classification", "market_breadth", "investor_flow_market", "investor_flow_symbol", "short_selling", "valuation_snapshot"],
        "blocking_categories": [
          {
            "category": "investor_flow_symbol",
            "coverage_ratio": 0.596429,
            "min_coverage_ratio": 0.99,
            "reason": "required_pykrx_enrichment_category_below_min_coverage"
          }
        ]
      }
    }
  },
  "stage_order": [
    "master_refresh",
    "pykrx_universe_refresh",
    "price_collection",
    "pykrx_enrichment",
    "technical_snapshots",
    "action_summaries",
    "trending_ranks",
    "signal_features",
    "market_risk_filter",
    "fundamentals_daily_refresh",
    "piotroski_filter",
    "momentum_factors",
    "value_factors",
    "low_risk_factors",
    "beta_factors",
    "advanced_value_factors",
    "unified_value_factors",
    "quality_factors",
    "composite_factors",
    "external_news_collect",
    "external_news_enrich",
    "agent_feedback_summaries",
    "signal_decider",
    "signal_decider_evaluations"
  ],
  "official_run": {
    "id": 812,
    "trigger": "internal_ops_cli",
    "status": "running",
    "current_stage": "technical_snapshots",
    "error_message": null
  },
  "stages": {
    "price_collection": {
      "status": "completed",
      "processed": 3579,
      "total": 3579,
      "failed": 0,
      "source_run_ids": {
        "eod_run_id": 184
      }
    },
    "pykrx_enrichment": {
      "status": "partial",
      "gate_status": "blocked",
      "blocking_status": "blocking",
      "processed": 7,
      "total": 7,
      "failed": 2,
      "source_run_ids": {
        "eod_run_id": 184
      },
      "details": {
        "progress_source": "live_run_metadata",
        "downstream_allowed": false,
        "downstream_preflight_status": "blocked",
        "failed_categories": ["investor_flow_symbol", "short_selling"],
        "category_row_counts": {
          "sector_classification": 3579,
          "investor_flow_symbol": 0,
          "short_selling": 0
        },
        "categories": {
          "sector_classification": {
            "coverage_status": "complete",
            "present_rows": 3579,
            "expected_rows": 3579
          },
          "valuation_snapshot": {
            "coverage_status": "complete",
            "present_rows": 3579,
            "expected_rows": 3579
          },
          "investor_flow_symbol": {
            "coverage_status": "missing",
            "present_rows": 0,
            "expected_rows": 3579
          },
          "short_selling": {
            "coverage_status": "missing",
            "present_rows": 0,
            "expected_rows": 3579
          }
        }
      }
    },
    "signal_features": {
      "status": "completed",
      "source_status": "stored_signal_features_present",
      "processed": 2823,
      "total": 2823,
      "details": {
        "mode": "derived_from_daily_signal_features",
        "available": true,
        "reason": "persisted_signal_features_available",
        "rows_persisted": 2823
      }
    },
    "external_news_collect": {
      "status": "completed",
      "source_status": "stored_external_news_present",
      "details": {
        "mode": "derived_from_external_news_items",
        "reason": "persisted_external_news_items_available",
        "items_persisted": 1091,
        "symbols_with_news": 114
      }
    },
    "external_news_enrich": {
      "status": "completed",
      "source_status": "stored_external_news_classified",
      "details": {
        "mode": "derived_from_external_news_items",
        "reason": "persisted_external_news_classification_available",
        "items_persisted": 1091,
        "classified_items": 1091,
        "classification_coverage_ratio": 1.0
      }
    },
    "technical_snapshots": {
      "status": "running",
      "processed": 1284,
      "total": 3579,
      "failed": 3,
      "current_step": "computing_symbol_snapshot",
      "current_activity": "Computing daily technical snapshots for 005930",
      "current_symbol": "005930",
      "source_run_ids": {
        "eod_run_id": 184
      }
    },
    "fundamentals_daily_refresh": {
      "status": "running",
      "processed": 120,
      "total": 200,
      "failed": 2,
      "current_step": "capturing_fundamentals",
      "current_activity": "Capturing DART fundamentals for 75 events, retry cycle 2/3, batch 1/2",
      "source_run_ids": {
        "refresh_run_id": 55,
        "sync_run_ids": [880, 881],
        "capture_run_id": 700,
        "capture_run_ids": [699, 700]
      }
    },
    "market_risk_filter": {
      "status": "partial",
      "gate_status": "ready",
      "severity": "degraded",
      "blocking_status": "non_blocking",
      "processed": 2805,
      "total": 2806,
      "failed": 0,
      "current_activity": "Stored 2805/2806 rows for KIS market-risk filter compute",
      "details": {
        "rows_persisted": 2805,
        "expected_rows": 2806,
        "coverage_ratio": 0.999644,
        "missing_count": 1,
        "missing_symbols_sample": ["176750"],
        "gate_reason": "missing_rows_outside_active_feedback_scope",
        "feedback_scope_gap": {
          "missing_count": 0,
          "missing_symbols_sample": []
        }
      }
    },
    "agent_feedback_summaries": {
      "status": "running",
      "processed": 280,
      "total": 565,
      "failed": 0,
      "current_activity": "Running agent feedback for fundamental-summarizer",
      "details": {
        "run_id": "feedback-cycle-2026-05-27-c21e78a409",
        "run_status": "running",
        "current_role": "fundamental-summarizer",
        "failed_items": 0,
        "feedback_failure_status": "no_failures",
        "feedback_health": "running"
      }
    },
    "signal_decider": {
      "status": "completed",
      "source_status": "stored_signal_decider_outputs_present",
      "details": {
        "mode": "derived_from_signal_decider_outputs",
        "reason": "persisted_signal_decider_outputs_available",
        "feedback_rows": 114,
        "decision_snapshots": 228
      }
    },
    "momentum_factors": {
      "status": "pending",
      "processed": 0,
      "total": 3579,
      "failed": 0,
      "current_activity": "Pending momentum factor compute",
      "details": {
        "rows_persisted": 0,
        "expected_rows": 3579,
        "default_selected": true
      }
    }
  }
}
```
