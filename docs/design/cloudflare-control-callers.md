# Remaining corrected-control caller ownership

Proposed control: `c44896ac8c`, based on approved control `8a61f42df8`.
This proposal is prepared and tested, but has not replaced the live control.

The approved control still drains later mutation groups and runtime-actor jobs
through the first caller's continuation. Actual-workerd probes submit callers
`a`, `b`, `c` and observe execution contexts `a`, `a`, `a` in both queues. The
isolated correction and candidate each observe `a`, `b`, `c`. The probes use real
Durable Objects and asynchronous caller contexts with deterministic work; they
establish scheduling ownership, not the cause of every live cancellation.

Only two JavaScript files change:

- `mutation-groups.mjs` registers each group's continuation in its submitter,
  starts its coalescing timer there, and finishes accounting and next admission
  before answering the completed owner. An expired selected owner hands off
  before returning its error.
- `runtime-actors.mjs` hands the next job a permit instead of running it in the
  completed caller's loop. Each admitted caller executes and settles its own
  job; accounting and next admission precede its response.

The proposal preserves the original control's resource and batching policy:

| Policy | Unchanged control value |
| --- | --- |
| Actor executions | One per actor, for both reader and builder |
| Actor pending input | Eight jobs, four MiB, one MiB per request |
| Actor deadline | Thirty seconds from submission |
| Mutation pending input | Sixteen jobs, four MiB, one MiB per request |
| Mutation group | Sixteen callers, 768 KiB, 1,024 records |
| Coalescing timer | Two milliseconds |
| Priority aging | Five seconds |

All other source files remain at approved `8a61f42df8`. Original native module
hashes remain `563936db…dac97` (full) and `789469d7…5f77` (compact), with no
rebuild. No candidate cache partition, read overlap, admission capacity increase,
checkpoint optimization or native algorithm enters the control. Its original
90 Node tests pass. Six workerd probes preserve the approved-control failures
and verify both proposed-control and candidate behavior.

Review artifacts are
[the two-file patch](../../examples/cloudflare-serverless/qualification/caller-control-correctness.patch)
and [proof and hashes](../../examples/cloudflare-serverless/qualification/caller-control-correctness-evidence.json).

The frozen-control protocol requires a replacement decision before deployment:
the existing authorization names `8a61f42df8`; this is a new proposed baseline.
On acceptance, retain all prior bundles and failed reports, record both the
incremental patch and combined original-control patch hashes, deploy the new
control to the same disposable stacks, and prepare fresh paired corpora.
Native ANN evidence remains tied to the same original module hashes. All warm
latency, error, scale, soak and single-attempt cold requirements remain in force.
Acceptance does not turn any earlier comparison into passing evidence.
