# Proposed correctness-only qualification control

The original frozen control is `d5102ee70321ca2b0cb0b18fe22fb871c7c6ace4`.
Its live deployments and failed reports remain unchanged. A separately prepared
control at `21205a7021` fixes two JavaScript correctness defects, with no native
module, resource-limit, algorithm, arrival-rate or gate-threshold changes.

The [two-file patch](../../examples/cloudflare-serverless/qualification/control-correctness.patch)
changes queued pool admission to resume the submitting caller's continuation.
In the actual Worker runtime, the original pool returns successful HTTP responses
but executes queued callbacks with another actor's async context. The isolated
patched pool preserves all three actor contexts in the same test. This is a
context-isolation defect, not a measured performance comparison.

The second change refreshes an older admission clock through a fresh timer event
before native entry. With the original frozen native module, a synthetic one-ms
older context produces `ManifestReadLeaseExpired` and leaves the original host
unusable. The patched host refreshes once, returns the exact count and preserves
the original mutation receipt. A real regression still present after refresh is
rejected and retires the host; no clock is clamped or high-water mark reset.

The [reproduction evidence](../../examples/cloudflare-serverless/qualification/control-correctness-evidence.json)
binds source/patch hashes and the unchanged full/compact native hashes. The
replacement is prepared in `.worktrees/cloudflare-gate-control-context` on
`fix/cloudflare-gate-control-context`. The user approved this replacement after reviewing the proposal. Freeze its
bundles separately, retain the original failed comparisons, and run three fresh
paired corpora and complete trials. Never
relabel an old report or clear a consumed cold-epoch marker.
