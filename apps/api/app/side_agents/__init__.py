"""Out-of-band OpenClaw analyst agents (market context, news events, postmortem).

These agents run beside the per-symbol decision pipeline: they are dispatched by
their own CLI/scheduler, validated fail-closed, and written to files. Nothing
here feeds the deciders or touches orders.
"""
