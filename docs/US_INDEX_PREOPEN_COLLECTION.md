# Independent US-index preopen collection

This data-only job connects the existing manual US-index collector to an opt-in
standalone schedule. It does not import the daily runner, call agents, alter
scores, publish trading plans or start/restart Core or Exec. No HTTP route or
schema migration is added. Apply the existing snapshot migration `20261124_0091`
before enabling it.

## Schedule and request budget

For the KRX target date, the job fixes `as_of` at **07:00 Asia/Seoul** and uses the
existing completed-US-session selector for the exact two price dates. Eligible
attempt slots are **07:00, 07:05 and 07:10**, with a strict **07:12 cutoff**.
Each invocation considers only the latest eligible slot. Missed slots are not
replayed in a burst. A normal successful empty response remains unavailable,
and both unavailable and technically failed indices may be retried at the next
slot. Previously committed, validated captures are reused without quote calls.

With the same preserved state directory, each slot is durably claimed once,
before external work: at most three scheduled fetch invocations per index, or
nine for `SPX,COMP,SOX` per target date. Existing KIS token caching, pacing,
authentication handling and transport limits are reused; no additional retry,
pagination or fallback loop is introduced. Ordinary successful collection needs
only three fetches. The default asynchronous collection budget is 90 seconds per
tick, bounded by the remaining time to 07:12; a programmatic caller may lower it.

The clock/window are checked after database reads, before each index and again
at the collector's quote boundary. Responses received at or after the cutoff are
not saved. An already-running synchronous database operation cannot be forcibly
cancelled by an asyncio timeout; Core's existing SQL/connect timeouts still apply.
A late completion is expired, and no further quote is started by that tick.

The job uses Core's existing XKRX calendar, including its confirmed 2026 closure
overrides, and checks calendar-provided 09:00–15:30 hours. Closed dates, known
special-hours dates and dates with no new completed US session since the previous
KRX session are normal no-ops. Calendar bounds/errors are technical failures,
not synthetic closed days. Calendar coverage is not a live special-hours or
emergency-closure attestation; strategy activation still needs verified session
hours. A US holiday is not recorded as a zero-return event.

## Durable recovery and isolation

State defaults to `apps/api/var/us-index-preopen/<real|mock>-YYYY-MM-DD.json`.
Use one stable directory across restarts and invocations; `--state-dir` can point
to a persisted SSD directory. It must remain outside Git. Do not delete or reset
today's checkpoint: that loses its spent-attempt budget.

The existing stable sidecar `flock` prevents overlapping processes using that
directory. Claims/results use the existing fsync + atomic replacement writer.
Invalid JSON, contract drift or malformed attempt maps fail without resetting
the budget. This is single-host coordination; use database claims before any
multi-host deployment. Separate state paths do not share an attempt budget.

Recovery reads the snapshot database with the exact frozen `as_of`, expected US
session, source environment and observed receipt-time bound. Hash/metadata
validation remains mandatory. Other dates, environments, future receipts and
different calendar windows cannot satisfy the pending set. This also finds a
capture committed just before a process interruption, even if its result never
reached the checkpoint. Interrupted slots stay spent; only unfinished indices
are tried in a later slot.

The collection result contains status, target date, slot, completeness and safe
snapshot IDs. Checkpoints retain per-slot requested indices and safe outcomes.
The standalone CLI emits JSON and exits 0 for completed/normal no-op/expired,
1 for technical failure or partial failure, and 2 for invalid CLI arguments.
Exception text and credentials are not emitted. Failures have no original daily
stage, retry-debt, block, monitoring or order side effects.

## Running and installing

From `apps/api`, with existing Core KIS/database configuration:

```sh
python -m app.internal.us_index_preopen --state-dir var/us-index-preopen
```

Preview the exact launchd configuration without installation:

```sh
bash scripts/install_us_index_preopen_launchd.sh --print
```

After deploying a reviewed remote revision and applying the prerequisite
migration, install from that stable deployment checkout:

```sh
bash scripts/install_us_index_preopen_launchd.sh
```

The per-user label is `com.kiseyno.kai-us-index-preopen`. Its `StartInterval=60`
polls regardless of host timezone; `RunAtLoad=true` performs an initial schedule
check, and `KeepAlive=false` avoids failure restart loops. The shell wrapper
skips Python entirely outside 07:00–07:11 KST, then Python checks the exact clock
and calendar before requests. stdout/stderr go to separate `apps/api/var/log`
files. Remove with `launchctl bootout gui/$(id -u)/com.kiseyno.kai-us-index-preopen`.

Polling does not promise execution exactly on the minute. `StartInterval` firings
can be missed while the Mac sleeps or the job is running; waking after 07:12
does not backfill the expired session. See Apple's [launchd jobs guide](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html)
and [timed jobs guide](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/ScheduledJobs.html#//apple_ref/doc/uid/10000172i-CH1-SW2),
plus the installed `launchd.plist(5)` manual for current interval behavior.

These IDs are collection/recovery evidence, not a sealed cutoff manifest, a
provider-finality attestation or an executable signal. Input freezing, feature
calculation, rule validation, strategy publication and deployment enablement
remain separate steps. This change does not install or enable the job itself.
