# POST /eod/signal-features/collect and GET /eod/signal-features

## Summary
Collects or reads daily signal features. Direct KIS ranking rows still seed the dataset, but the collect path now expands coverage across the active daily-bar universe and the current agent-feedback candidate universe from already-owned bars, persisted factor rows, and cached quote data when direct ranking rows are missing. The same collect path also refreshes the persisted market-wide participation context used by market summarizer payloads.

## Example collect request
```http
POST /eod/signal-features/collect?market_date=2026-04-15&sources=volume-power,overtime-volume,overtime-fluctuation
```

## Example collect response
```json
{
  "market_date": "2026-04-15",
  "source": "kis_ranking",
  "sources": ["volume-power", "overtime-volume", "overtime-fluctuation"],
  "source_counts": {
    "volume-power": 2,
    "overtime-volume": 1,
    "overtime-fluctuation": 0
  },
  "source_errors": {
    "overtime-fluctuation": "KIS overtime fluctuation endpoint timed out"
  },
  "features_persisted": 76,
  "features_failed": 0,
  "coverage": {
    "market_date": "2026-04-15",
    "persisted_row_count": 76,
    "direct_signal_source_row_count": 12,
    "fallback_only_row_count": 64,
    "row_kind_counts": {
      "direct_plus_fallback": 12,
      "fallback_only": 64
    }
  },
  "participation_context": {
    "market": "KRX",
    "market_date": "2026-04-15",
    "source": "kis_ranking",
    "captured_symbol_count": 76
  },
  "items_total": 76,
  "items_returned": 76
}
```

## Example read request
```http
GET /eod/signal-features?market_date=2026-04-15&symbols=005930,000660
```

## Example read response
```json
{
  "market_date": "2026-04-15",
  "summary": {
    "entries": 1,
    "entries_for_market_date": 76,
    "requested_symbols": ["005930", "000660"],
    "matched_symbols": ["005930"],
    "missing_symbols": ["000660"],
    "coverage": {
      "persisted_row_count": 76,
      "row_kind_counts": {
        "direct_plus_fallback": 12,
        "fallback_only": 64
      }
    }
  },
  "items": [
    {
      "symbol": "005930",
      "market_date": "2026-04-15",
      "source": "kis_ranking",
      "signal_count": 2,
      "row_kind": "direct_plus_fallback",
      "provenance": {
        "direct_signal_source_count": 2,
        "fallback_groups_present": ["price_action", "liquidity", "returns", "volatility", "factor_scores", "quote_snapshot"]
      },
      "freshness": {
        "market_date": "2026-04-15",
        "price_bars": {
          "trade_date_relation_to_market_date": "same_market_date"
        }
      },
      "fallback_features": {
        "price_action": {
          "close_return_1d_pct": 1.287553
        },
        "factor_scores": {
          "momentum": {
            "score": 0.914
          }
        }
      }
    }
  ]
}
```
