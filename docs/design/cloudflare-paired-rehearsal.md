# Representative Cloudflare paired rehearsal

The October 29 rehearsal completed its database workload and exact verification.
The candidate is substantially faster than the corrected control, but misses the
read/write latency targets. The rehearsal remains **failed**, and does not
qualify the full gate. Canonical export coverage also remains incomplete.

## Measured result

The immutable application pair is corrected control `c44896ac8c` and candidate
runtime `9b81fa0021`. Application bundles, native modules, capacities and gate
thresholds were unchanged. The driver source was `d8a25dd2e3`. Both sides used
four tenants, 512 documents per tenant, the existing 1 KiB payload, six arrivals
30 seconds apart, and the same lookup/write/compaction schedule.

| Metric | Control | Candidate |
| --- | ---: | ---: |
| Read samples | 72 | 72 |
| Read p50 | 3,571 ms | 766 ms |
| Read p95 | 11,623 ms | 3,198 ms |
| Write samples | 24 | 24 |
| Write p50 | 5,031 ms | 1,280 ms |
| Write p95 | 13,184 ms | 4,211 ms |
| Exact body/count checks | 2,052 passed | 2,052 passed |
| Original/latest receipt replays | 8 passed | 8 passed |

Candidate read/write p95 is approximately 72.5%/68.1% lower in this short run.
Neither satisfies the unchanged 1,000/3,000 ms target. This is one descriptive
paired run, with too few samples for the full gate or a causal performance claim.
It is not a reason to start three expensive hour-long qualification runs yet.

One control compaction request failed with `ECONNRESET` after 33 ms. The driver
did not replay this non-idempotent operation. Its operation, round-correctness and
offered-round entries describe one failed request, not three independent
failures. The candidate had no final client errors. All acknowledged body/count
and receipt checks passed on both sides. Publication lag was at most one;
observed queue backlog was at most seven and observed DLQ count was zero, over
only three samples per side. These samples do not meet the full queue-population
requirement.

## What the received phase records show

`native-received-profile.json` correlates physical HTTP attempt hashes with
sanitized native Tail records. This is descriptive profiling, not a replacement
for canonical provider identity or completeness proof. Local phase durations can
overlap and must not be summed as independent end-to-end times.

For candidate reads:

- `native:openReadView` p95 is 790 ms over 72 reports.
- Metadata `observe_read` reports I/O-wait p95 of 640 ms over 70 reports, with
  five GETs at p50/p95 in the enclosing snapshot metrics.
- Eleven of 72 reads use `rpc:openReadView`; that span's p95 is 2,812 ms. The
  read-admission code uses this path when a local snapshot requires admission or
  a pin, or authority is missing/recovering. This run does not distinguish those
  reasons, so none is declared the sole cause.
- Reported kernel-queue p95 is zero. Candidate app CPU p95 is 177 ms. App CPU
  is not directly comparable with the control app, which delegates its reader
  execution to another Worker.
- Lookup metrics report 17 native operations at p50, 21 at p95, and about
  31 KiB mean transferred bytes. These are native operation/transfer metrics,
  not an assertion that every native operation is a remote R2 request.

For candidate writes, `writer_rpc` p95 is 3,909 ms over 24 reports. The next
profiling step must separate writer admission, native preparation, durable
metadata commit and post-commit work; the HTTP span alone cannot identify which
one dominates.

Received terminal populations include 3,920/767 writer invocations for
control/candidate, and 2,850/zero reader invocations, within their native boundary
witnesses. The candidate's direct app reads avoid the reader Worker. These are
received populations, not independently qualified total counts.

## Diagnostic result and corrections

A separate observer received immutable terminal records for all eight producer
roles. Distinct credential-free 401/404 witnesses bracket each source. The
received populations total 13,898 terminal records. Their resource classifier
reports zero CPU/memory failures, zero truncated records and no unmatched
physical-I/O lifetimes. This does **not** establish that no missing invocation
failed: canonical trace export did not reconcile, so complete resource coverage
is not claimed.

All acknowledged HTTP attempts match received native records. Seventeen control
and six candidate physical attempts without responses are absent from that
channel. Their absence is retained as unproven; it is not silently converted to
successful requests or complete coverage.

The original evidence reader hit its 120-second source snapshot budget and a
connection reset. A separate read-only Worker now pipelines metadata inventories
and at most four bounded body requests, each limited to 32 objects/4 MiB.
The source limits remain 20,000 objects, 64 MiB and 120 seconds. Listing completion
still does not establish settlement: two closed-window observations and exact
population reconciliation remain necessary. Only read-only transport resets may
retry, within the original deadline; witnesses are not blindly replayed.

The live export receiver records `TelemetryOtelEnvelopeInvalid` rejections.
The evidence does not establish which bulk envelope shape caused them. Node tests now exercise JSON arrays, individual resource groups, and
resource/record counts larger than one sanitized storage chunk. Actual-workerd
tests verify that a 256-resource JSON array persists all 256 provider records. The separate
receiver bounds total input, resource and record counts, partitioning into
128-resource/1,000-record chunks before the existing sanitizer and immutable
writer. Actual provider bulk delivery still needs validation before claiming the
export issue fixed.

The final workflow provisions this receiver **before** creating the two export
destinations. Its lifecycle creates those destinations against the final URL,
rather than changing URLs/headers during a measured execution. The disposable
receiver prototype in this run was never successfully routed; failed destination
update attempts remain preserved. No database workload was repeated to improve
these results, and the original counter/export receiver was not upgraded.
Known `canceled` terminal outcomes remain counted and reported rather than being
misclassified as missing metadata or memory failures.

## Next performance work

Prioritize portable read-admission improvements, rather than another soak:

1. Record the bounded fallback reason at the read-admission boundary. Reproduce
   each reason locally with concurrent publication, pin expiry, recovery and
   collection, using the existing cancellation/physical-settlement guarantees.
2. Extend the shared serverless snapshot-admission contract to reuse a validated
   view when its visibility satisfies the requested fence and its reader lease
   still protects every required object. Coalesce compatible pin/admission work.
   Preserve fresh validation for consistency modes that require the latest
   head; never silently substitute bounded-stale reads. Test writer handoff,
   GC, lease expiry, clock defects and late physical I/O before cloud comparison.
3. Reduce the observed metadata round trips and repeated native decoding through
   that validated view and existing immutable caches. Evaluate reduced native
   work and bytes as well as wall latency, with unchanged memory budgets.
4. Profile the writer's internal phases before changing its commit protocol.
   Optimize the serial I/O chain while retaining conditional publication,
   fencing, receipts and ambiguous-acknowledgement semantics. Adding more batch
   delay alone does not help the offered load's one write per tenant per round.

These changes belong in the common native serving/storage contracts so S3 and
GCS hosts can benefit. They require provider-specific qualification afterward;
this Cloudflare run establishes no AWS/GCS latency result. Validate actual bulk
export readiness before another short paired run, and reserve full hour/cold
qualification until the short run meets both performance and evidence criteria.

## Evidence and cleanup

Execution: `2a8d7ddf-ec1e-4a8d-b098-49907a294aac`.
Local reports are under
`examples/cloudflare-serverless/dist/qualification/antfly-1278-observe-mvtzv4202649/`.
The original driver report, failed retrievals, two immutable received-data
snapshots, corrected offline reconciliation, deployment audit and received-record
profile are retained separately. An original report left at `settlement` records
the stopped, superseded reader; it is not presented as a completed gate run.

Independent cleanup verifies absence of eleven Workers, six actor namespaces,
three R2 buckets and their test data, four Queues/DLQs, both OTLP destinations
and related export jobs. All three generated local keys are removed. The four
unused trial-two/three proposals were not deployed. Reports remain local.

Final validation passes 391 Node regressions and 38 actual-workerd regressions.
The workerd log contains explicitly labeled test-only `SQLITE_NOMEM` injections;
those are local regression fixtures, not evidence of a live allocator recurrence
or a platform allocator fix. All 28 frozen application bundle artifacts still
match their original hashes. Cache GC was invoked and its activity/inspection
guard skipped deletion.
