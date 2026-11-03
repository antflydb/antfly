# Remote serving performance qualification

Tracking issue: [#1471](https://github.com/antflydb/antfly/issues/1471).

The November 2 baseline uses `fd965dfa9bb11886e16b83cbaeb18541c3efa941`
(merged #1320), native ARM64 ReleaseFast with stripping, Metal/CUDA disabled,
a 4 GiB standalone process, and navigation warming disabled. The source is an
unchanged 10,000-row BigQuery Snappy Parquet export (4,566,395 bytes); the current
table contains 128 recent rows copied from it. This is a shared developer host,
not a regional deployment or full-archive capacity test.

| Measurement | Local objects, stored source | GCS, source hydration |
| --- | ---: | ---: |
| Initial publication | 9.14 s | 441 s |
| First search after serving-cache reset | 170.37 ms | 6,850.90 ms |
| Warm median | 6.745 ms | 595.955 ms |
| Restart with retained cache | 44.12 ms | 765.15 ms |
| Union warm p95 | 61.30 ms | 6,540.07 ms |
| Saved overlay warm p95 | 60.77 ms | 7,457.35 ms |

The local warm samples are **3926.98**, 11.70, 9.59, 7.46, 6.61, 6.36,
6.22, 5.97, 6.88, 6.51 ms. The unexplained first-repeat outlier is retained;
with ten samples its nearest-rank p95 is also 3926.98 ms. A median alone does
not qualify this run's tail latency.

Provider and source-storage settings differ in this baseline. These numbers do
not prove a code regression. Cold GCS hydration took about 2.57 s; the cold query
recorded 53 serving-cache provider reads and 3,624,472 bytes. Restart recorded
zero such reads, but spent about 461 ms before search, 236 ms retaining its
cursor, and 42 ms searching. These cache counters exclude control operations.
Composed profiles put nearly all elapsed time in leaves; composition itself
was below 1 ms.

## Matched runs

The real HN deployment and catalog setup already enable `store_source`. Keep it
enabled for public serving. `poc.py` retains its historical default for existing
qualification configurations; pass `--store-source` or `--no-store-source`
explicitly when comparing. A reused publication must not be relabeled as if its
source-storage setting had changed.

`remote_performance.py` gives each binary/provider/source-storage/cycle its own
publication and state. It fingerprints both harness scripts, the binary and local Parquet fixture,
rejects changed recipes on resume, retains all timing samples and checks top
HN IDs/scores across cells. It only reads an existing export; it does not launch
BigQuery jobs, VMs, IAM changes or a hosted deployment. GCS runs write artifacts
under the explicitly supplied fresh prefix. Respect the existing qualification
budget and object lifecycle policy.

```sh
uv run --project examples/hackernews python examples/hackernews/remote_performance.py \
  --binary before=/absolute/path/to/before/antfly \
  --binary after=/absolute/path/to/after/antfly \
  --filesystem-source /absolute/path/to/raw-source \
  --state /absolute/path/to/matrix-state \
  --bucket EXISTING_BUCKET --gcs-source-prefix EXISTING_EXPORT_PREFIX \
  --gcs-artifact-prefix FRESH_QUALIFICATION_PREFIX
```

This prints the plan. Add `--run` to execute it serially. Start with
`--providers local` to avoid cloud access. Run query timings after compilers and
other qualification servers stop. Use equal optimized builds for comparisons;
use Debug to investigate and fix engine failures. OS caches are not flushed.
Run regional qualification with the serving process in the bucket's region;
label developer cross-region measurements separately.

Resume skips only completed cells with unchanged recipes and fingerprints.
Preserve failed or interrupted attempts, then use fresh state and a fresh
artifact prefix for their replacements. Reusing an incomplete engine would
mislabel remaining construction time as a fresh publication measurement.

Local GCS runs validate a conservative credential lifetime before every child
start and refresh when insufficient. A still-valid in-memory token is retained
across restarts even if another CLI user refreshes the shared gcloud cache. A single cached `gcloud auth print-access-token` result can
expire partway through a long qualification, even when it was valid at startup.
Externally supplied `--bearer-stdin` tokens retain their caller-owned lifetime;
they cannot be refreshed by the harness. Cloud must use its managed refreshing
credential source and qualify operation beyond one access-token lifetime.

`--diagnose-warm` adds serial metrics collection around every warm request.
This introduces idle intervals, so retain it as a separate diagnostic scenario,
not a replacement for uninterrupted warm measurements. Do not pass `--profile`:
that changes query planning. Slowest lake-request phase gauges preserve a
request-local outlier even when subsequent warm requests are fast. Diagnostic
contention skips capture instead of blocking a query; the drop counter remains
visible.

## Work and safeguards

1. Remove unnecessary bucket metadata probes when wrapping configured clients.
   Keep explicit provisioning checks; object operations retain authorization.
2. Reuse verified immutable native chunks across short-lived snapshot readers,
   with node-owned, process-accounted residency (at most 32 MiB), optional
   admission and nonblocking pressure reclamation. Manifest authorization, expiry,
   namespace/generation checks and credential isolation remain authoritative.
   Authoritative retirement invalidates shared residency. A cold-loss test must
   remove both the physical local cut and any verified shared chunk copy.
   Equivalent lake cursor envelopes can share a small expiry cohort, rounded
   upward by less than five seconds and no more than one percent of requested
   retention, always bounded by the protocol maximum. The key binds complete
   state, reader token, incarnation, recipe, expiry and current credential scope;
   fresh source/WAL authority checks still precede reuse.
3. Measure logical object operations in addition to serving-cache misses.
   `antfly_objectstore_*` metrics include local clients, nested fallback calls,
   background work and failed calls observed by the linked API runtime. Separately
   compiled runtime owners maintain independent counters; this exposition is not
   an aggregate of all native-owner I/O. Durations overlap. These are not provider
   HTTP request counts, bytes on the wire or cloud billing counters.
4. Measure native capture/reuse/recovery and publication write amplification;
   improve existing checkpoint/reference, packed-object and bounded I/O paths
   where the evidence identifies redundant work. Never substitute a TTL cache
   for mutable catalog/WAL authority or reader protection.
   Check the primary extent-checkpoint epoch first; on a miss, adjacent epoch
   lookups share a bounded wave and every child drains before return. Build
   read-ahead must recognize direct managed-host allocation budgets as well as
   SQL/build quotas. Unknown wrappers and arenas keep their conservative path;
   actual allocations still pass through every admission layer.
5. Reproduce and fix tail outliers with phase, queue/lock and maintenance
   evidence. Preserve cancellation, GC safety and restart/owner-loss behavior.

## Snapshot discovery contract

The raw-prefix matrix deliberately retains fresh Parquet file discovery. Even
with stored source and warm text indexes, a cached listing cannot prove that an
arbitrary prefix has not gained files. Do not remove those checks to improve a
benchmark. The [Parquet manifest protocol](../../plans/lake-ingestion-and-publication.md)
provides a small fresh commit head and immutable file-list chunks instead.

The HN Iceberg setup now explicitly declares `object_mutability: immutable`, as
required by its writer's new-file-URI contract. The commit pointer remains fresh;
a matching authenticated publication can reuse immutable file/navigation proofs.
This enables an existing fast path and does not freeze the mutable commit head.
Prefix and committed-snapshot timings must be labeled as different contracts.

## Credential rotation and publication identity

Durable publication identity must describe configured source authority and the
physical storage location. A short-lived bearer or S3 session token must not
change an immutable publication's identity. Credential-material fingerprints
instead isolate data, decoded metadata, vector/text runtimes, retained state and
warming caches. Reopening a request-local lazy adapter checks both identities.
Current connection policy and required provider reads still authorize access;
rotating a credential does not grant new bucket/prefix permissions.

The earlier development format used credential fingerprints in persisted
publication signatures; this predates #1320. Publications from that format need republishing once
when upgrading to the stable identity format. This is a prerelease format
transition; it must be included in release/upgrade qualification rather than
reported as transparent compatibility with existing retained lake cursors.

The matrix does not qualify full-archive capacity, Cloud control-plane setup,
production SLOs or upgrades. Those remain explicit release/deployment checks.

Shutdown can interrupt a newer Iceberg index publication while accepted reads
continue through the prior publication and WAL overlay. The coordinator already
retries the interrupted attempt. Readiness reports cancellation/deadline/timeout
interruptions and transient transport failures as pending publication, retaining
the error for diagnostics. Reconciliation reacquires its fence and resumes
verified checkpoints after backoff; this does not enable blind HTTP write
replays. Integrity, TLS authentication/protocol, authorization and admission
failures remain failed. A pending attempt never grants queryability without an
independent serving proof.

## Optional native checkpoint hints

Remote extent checkpoints are deduplication hints, not publication authority.
A stalled or transiently unavailable hint must not consume the entire query
deadline or prevent publication from an already sealed local snapshot. Hint
lookups and writes use a child deadline: about one second for small metadata,
with size-based transfer headroom capped at five seconds. Caller cancellation
always wins, and parallel lookups drain before the fallback starts.

Transient hint failures fall back to verified chunk publication. Required chunk
and manifest operations remain mandatory; their transient provider failures
cross the native runtime boundary as `StorageReadTemporarilyUnavailable`,
rather than an untransportable provider-specific error. Authorization, integrity
and TLS protocol failures are not treated as cache misses. This changes neither
commit authority nor the immutable object format and introduces no blind retries.

This addresses a failure reproduced in Debug after a matched GCS run returned
HTTP 504 during warm overlay search: the sampled stack was blocked in a remote
checkpoint-hint GET, and the eventual TLS transport failure escaped the native
callback ABI. That establishes one concrete failure path; it does not establish
the cause of every timeout or the original 3.9-second local outlier. Failed
attempts remain part of the qualification evidence in issue #1471.
