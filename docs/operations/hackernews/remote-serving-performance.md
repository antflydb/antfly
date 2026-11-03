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

The runner replaces the top-level summary with `complete: false` before validating
or launching the current plan. Its `plan_sha256` and `planned_cells` bind completion
to that exact recipe. Extending a completed local run with a failing GCS cell
therefore cannot leave the earlier summary looking complete. Existing cell reports
remain available for inspection and validated resume.

Lake readiness uses HTTPX's canonical retryable transport classification, including
connection refusal and truncated/failed reads. Its name-based form reads durable
diagnostics without changing HTTP replay policy. The Python compatibility path
recognizes these legacy failed statuses under the original build deadline;
authorization, integrity and TLS protocol failures remain terminal.

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
   admission and nonblocking pressure reclamation. Reclamation returns unused
   allocator credit before evicting payloads and reports actual charge relief.
   Pinned buffers remain protected and contended owners are skipped.
   Manifest authorization, expiry,
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

At the benchmark revisions below, the full Debug engine passed 64 API and five
embedded snapshot tests without allocator leaks, 180 copied-state real-GCS overlay
queries, and all six retained-cursor recovery cases across filesystem, an S3 fixture
and real GCS with both vector-storage modes. The harness had 25 passing Python
tests then; the subsequent readiness and matrix-summary fixes bring that to 27.

## Matched optimized result (November 3)

All eight cells passed, including ranked HN ID/score agreement, highlights, filters,
pagination/restart and union/saved-overlay semantics. This is one paired cycle
with 20 uninterrupted warm samples per workload on the shared macOS ARM64 host.
Own compilation finished before timing; unrelated host workloads may remain.
OS caches were not flushed. The fixture generation, size, MD5 and local SHA256
were checked again after the run and were unchanged.

The control is the exact pre-#1320 parent `bc168380f3`; the candidate engine is
`0db255d3b6bec1eed04a27375aebcc982a1bf584`, ReleaseFast/stripped with Metal/CUDA
disabled. The harness is `058812f98e`; the later startup correction changed no
engine code. Full fingerprints, every warm sample, p95/max and scoped counters
are retained in the [local evidence](https://github.com/antflydb/antfly/issues/1471#issuecomment-6599960593) and
[GCS evidence](https://github.com/antflydb/antfly/issues/1471#issuecomment-6599961048).

| Revision / objects / stored source | Publish s | Cold ms | Warm median / p95 / max ms | Restart ms | Union warm p95 ms | Overlay warm p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Before / local / yes | 5.05 | 16.18 | 6.68 / 7.29 / 9.46 | 20.04 | 54.58 | 82.47 |
| After / local / yes | 5.05 | 16.91 | 4.85 / 6.18 / 8.33 | 19.51 | 51.95 | 62.75 |
| Before / local / no | 4.04 | 62.35 | 9.18 / 10.41 / 10.84 | 21.06 | 53.86 | 61.00 |
| After / local / no | 4.05 | 19.62 | 7.49 / 7.72 / 9.35 | 19.09 | 52.61 | 60.20 |
| Before / gcs / yes | 570.50 | 6870.70 | 543.58 / 595.76 / 616.95 | 674.58 | 6087.84 | 6241.19 |
| After / gcs / yes | 590.35 | 6348.89 | 189.07 / 339.83 / 340.91 | 489.95 | 4371.30 | 4631.30 |
| Before / gcs / no | 421.13 | 5459.10 | 545.59 / 572.70 / 573.65 | 664.73 | 6193.65 | 6220.32 |
| After / gcs / no | 418.23 | 6054.10 | 193.94 / 350.74 / 365.31 | 480.52 | 4661.46 | 4645.49 |

For GCS with stored source, warm archive median fell from 543.58 to about 189 ms,
union median from 2903.19 to 1068.35 ms, and saved-overlay median from 4344.75
to 1309.02 ms. The corresponding composed p95s still reach 4–5 seconds.
Cold results are mixed: the stored-source archive improved slightly, while the
hydrated archive was slower in this cycle. Publication shows no consistent
improvement. These observations establish no production SLO or DuckDB parity.

Cold serving-cache provider reads were unchanged: 47 with stored source
(about 2.80 MB) and 53 with hydration (about 3.62 MB). The new API-owner counters
recorded zero bucket probes. The older control lacks these logical counters;
missing values are unavailable, not zero. Background/nested calls overlap these
intervals, and separately compiled native-owner operations remain outside their
scope. The retained-object inventories also showed no consistent reduction;
expiry and background GC affect them, so they do not measure publication-only
write amplification.

Stored source increased publication cost in this small fixture. Its Parquet
export fits in cache; full-archive hydration costs need separate qualification.
Keep the HN serving configuration explicit and measure its archive-scale storage,
build and response costs before changing that policy.

The original 3926.98 ms local first-repeat outlier remains unexplained. Additional
optimized and Debug local repeats did not reproduce it. Earlier GCS publication
and overlay failures remain recorded in #1471; the passing replacement matrix
does not erase them.

Remaining work is publication profiling, cold payload/hydration and native
snapshot tail latency, followed by same-region Cloud and full-archive sizing.
Raw-prefix discovery must retain fresh authority checks. Also qualify upgrades
with the documented republish transition and managed credentials across an access-token
lifetime before release/deployment signoff.
