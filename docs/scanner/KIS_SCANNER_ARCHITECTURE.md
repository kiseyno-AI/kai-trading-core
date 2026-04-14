# KIS Scanner Architecture

## Goal
Track a reduced candidate set in near real time and expose it to the dashboard.

## Design
1. Universe sync (daily)
2. Candidate reduction layer (batch ranking / filters)
3. Realtime tracked symbol registry
4. Tick ingestion / quote cache
5. Scanner state API

## Core entities
- universe_symbols
- tracked_symbols
- tracked_symbol_events
- quote_cache
- scanner_runs

## Initial scope
- no full-market per-second scan
- focus on tracked symbols only
- dashboard must show which symbols are currently tracked

## Realtime trending API
- `GET /scanner/realtime-trending` reads KIS ranking sources at request time, with a short in-memory cache.
- The current source set is limited to `volume-rank`, `fluctuation`, and `hts-top-view`.
- The route returns normalized source buckets plus merged symbol items; each merged item keeps the source/rank metadata that caused the symbol to appear.
- This path is intentionally not persisted as an EOD signal snapshot.
