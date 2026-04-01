# TASKS.md

## Current Priorities
1. Define canonical domain models
2. Design execution engine boundaries
3. Design market data pipeline
4. Prepare backend API structure
5. Connect documentation to implementation roadmap

## TODO
- Add pre-market master refresh workflow and operational scheduling.
- Extend domestic master universe loader from KOSPI/KOSDAQ to KRX/KONEX coverage as needed.
- Persist EOD OHLCV snapshots into the market database once DB schema/repository is ready.

## Tooling (Codex)
- Trust this repo in **Codex** so `.codex/config.toml` and skills load.
- After API edits: from `apps/api`, run `python -c "from app.main import app"`; see `.codex/skills/trading-core-quality`.
- Keep **`docs/API_BUILD_ORDER.md`** updated when routes change.
