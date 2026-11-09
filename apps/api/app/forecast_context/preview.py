"""Read-only TimesFM evidence preview against Core's stored KRX daily bars."""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
from time import perf_counter


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbols', required=True, help='Comma-separated KRX symbols')
    parser.add_argument('--market-date', required=True, type=date.fromisoformat)
    parser.add_argument('--knowledge-cutoff-at', required=True, help='Actual timezone-aware decision cutoff')
    args = parser.parse_args(argv)

    # Match the API launcher without printing credentials or changing its config.
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / '.env', override=False)
    from app.core.settings import settings
    from app.forecast_context.service import TimesFMForecastService

    settings.timesfm_enabled = True  # Only this read-only preview process.
    symbols = list(dict.fromkeys(s.strip().upper() for s in args.symbols.split(',') if s.strip()))
    if not symbols:
        parser.error('--symbols must contain at least one symbol')
    started = perf_counter()
    evidence = TimesFMForecastService().build_for_symbols(
        market_date=args.market_date, symbols=symbols,
        knowledge_cutoff_at=args.knowledge_cutoff_at,
    )
    print(json.dumps({
        'elapsed_ms': round((perf_counter() - started) * 1000, 3),
        'timing_scope': 'price_history_read_validation_worker_and_evidence_assembly_excludes_decider',
        'items': evidence,
    }, indent=2, allow_nan=False))
    return 0 if evidence and all(item.get('status') == 'available' for item in evidence.values()) else 1


if __name__ == '__main__':
    raise SystemExit(main())
