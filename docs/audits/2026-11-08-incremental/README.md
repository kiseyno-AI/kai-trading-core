# Incremental source audit — Core

**Status: in progress.** This is a review checkpoint, not a statement that every file is reviewed or that the final feature branch has passed its aggregate suite.

The requested constraint is to preserve intended normal behavior and split each demonstrated defect into an independent commit. Changes remain on `codex/core-incremental-audit-20261108`. Source changes are not promoted to `main`.

## Evidence

- `source-inventory.csv` identifies full, partial, and unread source ranges. Individual reviewer records preserve the relevant local revisions and scope. Test execution is separate from manual reading.
- `published-commits.json` records the independently published commits, affected paths, and verified Git tree hashes. Local and remote commit IDs differ because the authenticated publication API creates the remote commit; their file contents and tree hashes were checked.
- `validation.json` separates the completed baseline suite from the pending aggregate feature suite. Individual defect regressions and connected checks are recorded in the reviewer evidence.

## Validation boundary

The production baseline is `2ee26af61fe3107797f41c34c174d59b5ab5a651`. Its isolated full test result is `5223 passed, 14 warnings in 198.20s (0:03:18)`. Only test isolation fixtures were added to that baseline; production source remained unchanged.

Broker/provider behavior is exercised with fakes. Persistence and concurrency regressions use disposable PostgreSQL. Live trading, production database migration, market-feed timing, and deployment behavior have not been validated. No strategy thresholds, position-sizing formulas, or policy-cohort changes are included merely as speculative improvements.

## Remaining work

Finish the source ranges marked partial or unread, review each new patch in its connected callers, integrate the remaining atomic commits, and run the aggregate feature suite. Confirm both remote feature trees and unchanged main references before marking this review complete.
