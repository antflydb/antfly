# Bounded fenced read views and recovery memory

Issue: [#1278](https://github.com/antflydb/antfly/issues/1278). Implementation:
[PR #1320](https://github.com/antflydb/antfly/pull/1320).

The representative paired rehearsal put candidate read p95 at 3,198 ms and write
p95 at 4,211 ms, above the unchanged 1,000/3,000 ms targets. In the received
native diagnostics, local view opening had p95 790 ms, metadata observation I/O
wait had p95 640 ms, and 11 of 72 reads used coordinator admission (RPC p95
2,812 ms). Those are distributions of overlapping spans, not additive costs.
The previous reports remain sealed. Canonical OTLP coverage was incomplete;
these observations do not constitute qualification.

## Read contract

Latest reads still authenticate fresh authority. A lookup, list, count or search
with an explicit `required_visibility` may reuse an immutable captured cut that
meets that minimum fence. `fresh: true`, cursors and exact ANN generations bypass
reuse. Native code validates the full namespace token, WAL and manifest on every
query. The host retains a cut only after native execution returns its matching
visibility token; supplying a token alone cannot certify an unchecked cut.

The provider-neutral host implementation lives in
`zig/lib/objectstore/wasm/read-view-lease.mjs`. It stores plain cloned bytes and
lease metadata. It retains no request context, transport, lifetime callback or
completed admission promise. Same-fence in-flight admissions may share a
protected view; requests receiving an unprotected view acquire their own view.
Pending groups and members remain bounded. Coordinator fallback reasons use
fixed labels, and lease hits/joins and retained byte charges are observable.

A new R2 authority may advertise `read_lease_policy` format 1: 30,000 ms maximum
root lifetime and 60,000 ms collection grace. Existing authorities and SQLite
migration descriptors are not silently upgraded. Disabling
`FENCED_READ_LEASES` suppresses grants and reuse. A future authority upgrade must
explicitly establish the same collector contract before enabling readers.

The lease starts before metadata observation, expires within 30 seconds, and
also ends at the durable manifest pin expiry if earlier. Every reuse preserves
its original monotonic anchor and absolute expiry. Transport calls and successful
query completion check the lease; clock regression invalidates it permanently.
A delayed native confirmation cannot extend the deadline, rewind a newer cut,
or restore a cache invalidated by a query/admission failure. New writes can
advance a visibility fence without making the old immutable cut unreadable.
Latest requests continue to discover migration and ownership changes freshly.
Migration retirement must wait for outstanding bounded readers before deletion.

One observer retains at most 32 KiB of certified view bytes. This is subtracted
from its existing 512 KiB decoded row budget. At most eight app observers remain
retained, keeping the aggregate metadata allowance unchanged. Unprotected views
continue through the existing per-request path.

## Shared recovery memory

The larger local experiment exposed `OutOfMemory` during cold fused metadata
observation: replay retained the JSON bodies and parsed roots of many historical
frames in the request arena. One observed failure replayed 49 operations and
reached 7,733,248 bytes of linear memory in the unchanged eight MiB observation
module. This was a native allocation error, not `SQLITE_NOMEM`.

`immutable_log.zig` now reuses a scratch arena between replay steps. Only the
current bounded root crosses steps, and only the final cursor is copied into the
caller's allocator. Checkpoint authentication, parent revision checks, ownership
fencing, seal repair, replay limits and conditional publication stay in place.
A regression recovers 63 near-limit roots using a 256 KiB fixed allocator,
including an escaped JSON root and output lifetime verification.

This recovery implementation is shared by provider transports, so the memory
bound benefits S3 and GCS as well as R2. The reusable lease helper is portable;
other hosts must explicitly implement its pin and collector contract before
issuing grants. Local S3/GCS transport tests do not establish hosted latency or
live credential qualification.

## Writer measurements

Mutation admission records the oldest caller's monotonic queue wait. Native
writer traces distinguish authority loading, recovery refresh, immutable native
preparation, and final metadata commit. Physical transport counters and I/O wait
accompany the preparation metrics. Traces are emitted
on failures as well as successes. These spans overlap and cannot be summed.
Immutable preparation already runs outside serialized metadata commit; no receipt
or ownership check is removed on the basis of an aggregate writer RPC duration.
The next write optimization must be justified by these phase measurements.

## Validation and rollout

Current local comparisons use `qualification/read-state-latency.mjs` with identical
native modules, resource limits and explicit variant activation. Verify exact
bodies, counts, latest reads and visibility fences before interpreting timings.

A Cloudflare comparison requires fresh disposable namespaces, approved corrected
control `c44896ac8c`, a sealed candidate, and a functioning dedicated bulk export
receiver before source attachment. Preserve the previous failed reports and
control modules. A short comparison remains descriptive. Full qualification
still requires all three paired hour-long trials, cold trials, exact/receipt
checks, queue/lag/ANN checks and complete diagnostics with zero resource failures.

### Local results (2026-10-29)

Implementation commit: `543a3ea27a`. The rebuilt full native module hashes to
`b8f632e776447ea49fc210a244ecc767350ac2f6ab5f3f7b08f100bbe4705105`.
The isolated four-tenant, 512-document-per-tenant, 1 KiB body comparison used
128 reads and 16 writes per variant with 20 ms injected binding delay:

| Measurement | Reuse off | Reuse on |
| --- | ---: | ---: |
| Read p50 | 37.64 ms | 27.25 ms |
| Read p95 | 58.93 ms | 50.67 ms |
| App R2 reads | 512 | 140 |
| Write p95 | 317.51 ms | 323.41 ms |

This is about 14% lower read p95 and 73% fewer app R2 reads. It does not establish
a write win; the isolated write result is slightly worse. A run concurrent with
tests showed a larger read p95 difference (100.45 to 59.29 ms), illustrating why
that result alone should not be used as the improvement estimate. Exact bodies,
counts, and stronger fences after writes pass in both rebuilt variants.

The original native memory failures remain preserved. Rebuilt runs passed at the
same observation/query/pool caps. Validation reports 400 Node tests, 39 actual
workerd tests, and 162 native object-store tests passed; two live-provider tests
were skipped. A dedicated workerd regression verifies retained fenced cuts skip
native authority observations and explicit fresh requests resume observation.
The six preceding evidence closures still verify all 922 artifacts unchanged.

Local closure: `examples/cloudflare-serverless/dist/qualification/fenced-read-leases-1793317282250/evidence.json`
(19 artifacts, including copied native modules, failed runs, successful reports
and test logs). The Cloudflare comparison is a separate fresh proposal,
`gate-mvu6mu7h7d17`, with the approved control unchanged. Its outcome must be
recorded independently; these local measurements do not pass the hosted gate.

### Private writer profiling

`profile-paired-rehearsal.mjs REPORT CAPTURE_DIR OUTPUT --all-roles` also profiles
writer, reader and builder records. It maps logical application traces from
matched physical HTTP attempts to their workload phases, then measures those
logs in private native records. Conflicting trace classifications are excluded;
a terminal containing multiple phases contributes no duplicated CPU/wall sample.
Unmatched resource failures and truncation remain visible. This association is
for profiling and does not replace canonical provider identity or completeness.
The profiler can consume hash-verified captures referenced by the driver report.

Applied to the preceding sealed rehearsal, candidate writer preparation had p95
1,747 ms (25 spans), native finalization 1,012 ms (25 spans), and coordinator
queue wait 451 ms (42 group reports). Physical preparation I/O wait had p95
1,831 ms (25 reports). These counts are groups/spans, not independent client
requests; their percentiles overlap and must not be added. This locates real
storage work as well as admission pressure. The new hosted comparison adds
explicit mutation admission, authority and refresh spans before choosing another
write optimization. Five profiler regressions cover private correlation and CLI evidence hashing.
A further cache-retention regression brings the Node suite to 406 tests.


Retained cuts project only the documented view, record and lease fields. Unknown
properties, including non-cloneable request context and oversized auxiliary
buffers, are never retained. Malformed versions and content types are ineligible
for caching. This hardening is subsequent to sealed candidate `543a3ea27a`; it
must be included in the next hosted candidate rather than retroactively changing
that candidate's provenance.

A lease cache remains an optimization: Cloudflare does not guarantee that later
requests reach the same isolate ([runtime documentation](https://developers.cloudflare.com/workers/reference/how-workers-works/)),
and its Cache API is local to a data center ([Cache API documentation](https://developers.cloudflare.com/workers/runtime-apis/cache/)).
Therefore local cache-hit improvements cannot establish hosted tail latency.
Use the measured lease-hit, admission-miss and stronger-fence populations to
judge whether request distribution or actual storage work dominates next.
