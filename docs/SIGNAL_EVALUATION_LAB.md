# Signal evaluation lab

`python -m app.analytics.signal_lab` (read-only) answers one question with the same rules for every
signal: *does ranking stocks by this score earn money after costs, beyond what a plain risk exposure
would?* It scores deterministic factors, the stored factor tables and the agents' own scores and writes
`var/alpha-lab/report-<time>.{md,json}`. It never writes to the database and never places an order.

## Conventions (each is tested in `tests/test_signal_evaluation.py`)

* A signal on date `D` may only use information available at the `D` close (the pipeline runs after
  20:10 KST, so factor tables and agent feedback for `D` qualify).
* Agent rows are admitted by when Core stored them, not by their `market_date`. A summarizer row counts
  only if `received_at` is before 09:00 KST of the session after `D` (the open the return enters at); a
  decider row uses the latest of `created_at`, `feedback_received_at` and `updated_at`, so a value revised
  after the open is not scored as if it had been known. Of the admitted rows per symbol and date the latest
  stored one wins. Rows stored at or after the open, rows without a stored time and dates with no next
  session yet are dropped and counted per agent in the report. On the production sample about a fifth of
  decider rows were stored after the next open; without this rule they were scored with look-ahead.
* The judged return enters at the **next session's open** and exits at the **close of the h-th session
  after `D`** (`r1`, `r5`, `r10`, percent). Entry on a halted day, a missing bar, or a non-positive price
  gives NaN. Alignment is on the market calendar, so one missing bar never shifts a symbol's returns.
* IC = Spearman rank correlation within each date. The daily ICs are tested with a Newey-West t (lag
  `h-1`) because `h`-day windows overlap, and again on every h-th date only.
* `partial IC` removes the rank of 20-session volatility from both sides: it separates a real signal
  from "this score is just volatility", which is the single largest effect in the current sample.
* A label is `candidate_positive` / `candidate_inverse` only if `|t_hac|` clears a Bonferroni threshold for the number of
  signal x horizon pairs tested, the non-overlapping test agrees, and the IC has the same sign in both
  halves of the sample and `|IC| >= 0.02`. Otherwise `no_evidence`; fewer than 40 days is `insufficient`.
  A seeded random `placebo_random` signal is always evaluated and must come out `no_evidence`.
* The top-k simulation holds the `k` best names for `h` sessions, rebalances every `h` sessions (no overlap),
  and charges an assumed round-trip cost (default 51 bp: commission, the 18 bp sell tax, slippage). The
  report shows 0 / 30 / 51 / 80 bp so the cost sensitivity is visible. Those costs are assumptions.
* Agent decisions are compared with the same-day universe and, within the agent's own pool, `buy` and
  `reject` against `wait`.

## What it does not prove

One market regime (2026-04..11, weak, defensive), at most ~140 sessions, long-only returns with no
short leg, bars that were partly provisional before the official-frame fix. A candidate is a reason to
paper-trade, not to commit money.

## Roadmap to a profitable system (not implemented here)

1. **Shadow portfolio.** Persist a daily, rule-based top-k and the agents' picks as hypothetical positions
   with entry/exit rules and costs, and compare them to the baseline every week with this lab. Nothing real
   until a rule clears the thresholds out of sample.
2. **Deterministic core.** Build the ranker from signals that survive volatility control (currently quality
   and the composite), size positions for a small account, and gate exposure by market regime instead of
   being fully invested in a falling market.
3. **Agents as discoverers and vetoes.** Require structured output (direction, horizon, expected move,
   probability, enum reasons), log it before the outcome is known, and only keep an agent behavior that the
   lab confirms. Remove the end-of-day "spread/depth" core blocker that makes ~90% of rows block.
4. **Entry timing.** The lab reports the entry session's open-to-close drift; if it is persistently
   negative, test entering later in that session (this is analysis only; execution changes need approval).
