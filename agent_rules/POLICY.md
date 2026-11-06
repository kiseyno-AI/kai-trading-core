# Calibration policy for automated edits

You may write short calibration notes for the decider agents. A note exists only to make an agent more careful or
better calibrated against evidence it cannot see itself: for example "stated upside has not tracked realised returns,
so do not let a large stated upside raise confidence".

Hard limits (a guard rejects the change if any is broken):

* Edit only the lines between `<!-- BEGIN AUTO-CALIBRATION -->` and `<!-- END AUTO-CALIBRATION -->` in the named contract.
* At most 3 bullet lines in the block, each starting with `- ` and under 300 characters. No headings, prose, links or code.
* Never loosen, ignore, bypass, relax or reinterpret any gate, threshold, schema rule, fail-closed rule or boundary.
* Never raise size, risk, leverage, exposure, urgency or approval. A note may lower confidence, prefer `wait`/`reject`, or
  ask for more evidence; it may not make a `buy` easier.
* Never mention credentials, tokens, keys, file paths or the environment. Never promise or guarantee returns.
* Base every note on the finding given to you, and say only what that evidence supports. If the evidence is thin, write nothing.
* Replace stale bullets rather than accumulating them. Add exactly one line per change to `agent_rules/CHANGELOG.md`.
