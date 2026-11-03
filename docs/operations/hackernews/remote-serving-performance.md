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

These measurements motivated the cold-serving and publication follow-up below.
Same-region Cloud and full-archive sizing remain separate qualification work.
Raw-prefix discovery must retain fresh authority checks. Also qualify upgrades
with the documented republish transition and managed credentials across an access-token
lifetime before release/deployment signoff.

## Bounded cold-serving and publication follow-up

Text-directory readers, source navigation and the existing background warmer
now share decoded navigation leases and authenticated, coalesced range waves.
Independent missing ranges use the existing bounded scheduler; denied optional
admission retains the required serial read path. Warming keeps its existing
four-publication/four-lane, 64-page, two-level and two-second bounds. A failed
warming attempt releases its slot and can retry after a five-second backoff;
retry history holds at most 256 recent publication keys independently of the
reusable execution slots. Only completed attempts are remembered as successful
hints. Every consumer still checks its current cancellation and reader authority.

Source census and costing trees use the existing publication packing machinery,
including incremental batch flushes. Artifact recovery preserves a backend's
optional batch-put hook. Native object storage deliberately has no such hook;
forwarding it benefits hosted backends and is not evidence of a native GCS
bottleneck. Packing reuses existing artifact references and collection rules.

Publication also keeps a bounded attempt-local copy of successfully written,
authenticated small artifacts: at most 8 MiB, 512 entries and 256 KiB per body.
It is enabled only for a synchronized, refundable working-set allocator and
participates in that allocator's pressure reclamation. It can avoid reading a
just-written immutable page back from remote storage. Required-output allocation
can release a pinned hint and fall back to the backend. Missing hints, unknown
allocators and optional allocation failures retain ordinary authenticated reads.
Provider content verification, retention witnesses and durable commit checks
remain required; these hints do not prove remote durability. Cancellation,
range budgets and delete invalidation apply equally to cached reads. Delete
attempts invalidate hints even when a committed remote deletion returns a
cancellation or transport error; a later read must discover absence through the
backend rather than reuse a stale hint.

`poc.py` reports `publication_io` from before table registration through all
index-readiness checks, before validation/search traffic. The ending metrics
sample waits for health exposition refresh, outside query timing, so this
interval can include background work and is longer than `ready_seconds`.
It identifies fresh registration versus resumed work. Missing or reset counters
are unavailable rather than zero. Logical input/output body bytes exclude
transport overhead; failed attempts and nested batch/single calls can overlap.
Do not add nested counters or equate them with GCS billing or physical object
write amplification. Separately compiled runtime owners remain outside these
API-owner counters. Retained-object inventories include query/GC artifacts and
must not be substituted for publication-only measurements.

A Debug replay of the unchanged 10,000-row fixture completed 1,000 uninterrupted
archive warm searches and 1,000 repetitions of each of five metadata filters.
The archive maximum was 59.58 ms; the maximum among 6,024 observed archive/filter
requests was 420.82 ms. No request reached the 750 ms slow-profile threshold.
Three additional complete cold-start/restart replays, with 100 warm samples
and 100 repetitions per filter each, added 1,872 archive/filter requests; their
maximum was 163.86 ms. First-warm searches took 28.58, 21.70 and 22.70 ms.
The combined 7,896 requests never reached 750 ms. The additional replays ran
under compiler load and are diagnostic evidence, not optimized comparisons.
This does not reproduce or explain the original 3,926.98 ms local outlier.
`archive_warm_metrics` is captured immediately after the uninterrupted burst so
later restart/composed phases cannot overwrite its diagnostic snapshot. Keep
Debug diagnosis separate from final ReleaseFast comparisons and preserve failed
attempts alongside successful replacements.

### Follow-up qualification method

The follow-up control is instrumentation-only revision `28cccbf259a50722a8d7a7213c382dbb02681364`;
the candidate engine is `ecc91eb6c7d89045f35dbfc9efd8c3cdc37b8ccb`. Both use the
same pinned Zig 0.17.0 compiler, native ARM64 ReleaseFast, stripping, Metal/CUDA
disabled, default `antfly-version=dev`, and a 4 GiB standalone process budget.
Source revisions and complete binary/harness/fixture SHA256 fingerprints identify
the runs; the executable's `dev` version string does not identify its source.
The matrix uses one paired cycle with fresh independent state/artifact prefixes
for each binary, provider and stored-source setting, and 20 uninterrupted warm
samples per workload. Its nearest-rank p95 is the nineteenth sorted sample;
the maximum remains separate.

These are explicitly **contended developer-host measurements**. Concurrent
builds caused high load and substantial memory compression/swap use.
Per-cell load before/after, start/end
times and periodic host-pressure samples accompany the retained raw evidence.
During the matrix, sampled one-minute load ranged from 8.46 to 211.08 on
14 cores, and swap use ranged from 17.13 to 38.11 GiB.
Interpret timing differences alongside scoped object-work counts; they do not
establish isolated-host speedups, production SLOs, same-region Cloud acceptance
or DuckDB parity. The qualification's compiler processes finished before the
matched matrix; unrelated builds continued.

A separate candidate ReleaseFast functional run with navigation warming enabled
passed search/highlights, metadata filters, exact filtering, concurrency, cursor
pagination and restart checks. Its first warm-up recorded 50 pages and zero
failures. This run overlapped control compilation and contributes no matched
timing comparison. The matrix disables warming and resets serving caches for
its cold measurements. No OS cache flush is performed.

The follow-up local hydrated-source candidate also recorded new composed-query
tails: 2,135.67 ms for a cold union and 1,713.73 ms for a warm saved overlay.
The retained slowest-request profiles attribute 1,694.82 and 1,273.48 ms,
respectively, to execution of the native current-table leaf; composition itself
used 2.37 and 0.24 ms. Native-leaf time overlaps total leaf time. These are
separate from the historical direct-archive outlier. The profiles locate elapsed
work but do not establish whether scheduling, paging or engine work caused it.
Keep these maxima visible alongside medians, and do not infer a fix from the
absence of an outlier in a later replay.

### Matched follow-up results

All eight cells passed search/highlights, filters, cursors, restart and composed
result checks. Top HN IDs and scores agree across cells. `before` and `after`
refer to the control/candidate engine revisions above; `hydrated` means
`store_source=false`. Times remain contended-host measurements.

| Provider/source | Engine | Ready s | Cold ms | Warm median / p95 / max ms | Restart ms |
|---|---|---:|---:|---:|---:|
| local / stored | before | 5.03 | 73.07 | 7.47 / 10.19 / 13.80 | 50.46 |
| local / stored | after | 6.05 | 296.69 | 7.75 / 11.62 / 45.78 | 79.25 |
| local / hydrated | before | 4.03 | 56.85 | 10.22 / 12.47 / 12.92 | 49.55 |
| local / hydrated | after | 8.05 | 169.71 | 11.23 / 14.55 / 15.16 | 46.48 |
| gcs / stored | before | 648.78 | 7824.11 | 217.88 / 396.02 / 397.82 | 577.53 |
| gcs / stored | after | 907.48 | 9593.72 | 208.40 / 345.08 / 374.22 | 571.97 |
| gcs / hydrated | before | 425.64 | 6384.47 | 209.51 / 381.89 / 395.94 | 611.35 |
| gcs / hydrated | after | 371.95 | 6156.26 | 203.44 / 222.70 / 430.25 | 561.93 |

| Cell | Logical GETs | Logical PUTs | PUT input MiB | GET output MiB |
|---|---:|---:|---:|---:|
| before-local-stored-1-1 | 4121 | 1175 | 182.17 | 156.37 |
| after-local-stored-1-1 | 3087 | 1175 | 182.17 | 126.06 |
| before-local-stored-0-1 | 2683 | 1074 | 151.75 | 90.21 |
| after-local-stored-0-1 | 1736 | 1074 | 151.75 | 62.31 |
| before-gcs-stored-1-1 | 4342 | 1356 | 183.92 | 168.21 |
| after-gcs-stored-1-1 | 7372 | 3018 | 308.25 | 302.69 |
| before-gcs-stored-0-1 | 2902 | 1246 | 153.30 | 102.41 |
| after-gcs-stored-0-1 | 2021 | 1255 | 153.41 | 76.44 |

| Cell | Union cold; warm median / p95 / max ms | Overlay cold; warm median / p95 / max ms |
|---|---:|---:|
| before-local-stored-1-1 | 118.90; 60.33 / 71.78 / 84.95 | 710.20; 66.41 / 114.86 / 386.54 |
| after-local-stored-1-1 | 131.55; 56.24 / 66.23 / 97.93 | 178.64; 72.90 / 102.75 / 103.15 |
| before-local-stored-0-1 | 83.03; 64.66 / 80.97 / 86.56 | 180.33; 84.73 / 103.60 / 159.29 |
| after-local-stored-0-1 | 2135.67; 74.44 / 100.19 / 101.68 | 203.85; 125.00 / 1058.60 / 1713.73 |
| before-gcs-stored-1-1 | 21109.62; 1457.80 / 6012.57 / 6314.16 | 15279.87; 1555.26 / 5550.08 / 6169.96 |
| after-gcs-stored-1-1 | 17403.03; 1375.07 / 5502.43 / 5592.07 | 13326.80; 1499.71 / 5121.83 / 6068.68 |
| before-gcs-stored-0-1 | 15442.83; 1206.88 / 4806.01 / 5345.38 | 12805.78; 1474.35 / 5762.28 / 6034.26 |
| after-gcs-stored-0-1 | 17572.21; 1232.35 / 5602.71 / 5855.29 | 13333.13; 1434.82 / 5353.54 / 5695.36 |

Local publication logical GETs decreased 25.1% with stored source and 35.3%
without it; PUT counts were unchanged. The GCS hydrated-source pair reduced
GETs from 2,902 to 2,021 (30.4%), while PUT attempts increased from 1,246 to
1,255. Readiness was 425.64 versus 371.95 seconds, but this single contended
cycle does not establish an isolated-host speedup. Neither hydrated GCS cell
reported a publication failure.

The GCS stored-source candidate recovered from `RemoteUnavailable` during
publication, observed by readiness polling at 426.3 and 427.3 seconds. Its
907.48-second readiness and operation/body-byte totals include failed and
rebuilt work; both observations are of the same failure. The control completed
in 648.78 seconds. Retain the failed work and do not estimate a clean candidate
result by subtracting retry time. Operation error counters alone also include
expected conditional-write failures; they do not identify transport failures.

Cold archive and composed timings remain mixed. In particular, the GCS
hydrated candidate's cold union and overlay took 17,572.21 and 13,333.13 ms.
Warm composed tails remain several seconds in both engines. The local
single-file fixture does not demonstrate publication object-count savings
from census packing; 256-file incremental/refresh/deletion tests cover that
path. Lower GET counts are evidence of less repeated read work, not reduced
physical write amplification or full-archive qualification.

A fresh local Debug diagnostic at `6202fea1aa` passed 100 warm samples per
composed workload: union maximum 695.66 ms and saved-overlay maximum 726.50 ms.
A further 186 identity/score-checked composed requests across three restarts
reached 760.06 ms once. That request's retained profile recorded 563.07 ms in
the native leaf, 92.40 ms in cursor work and 0.68 ms in composition. Stack
sampling did not establish a blocking hotspot; much of the sample was taken
after the request completed. Neither diagnostic reproduced the optimized
1.71/2.14-second local tails or the historical 3.93-second archive outlier.
This frozen Debug revision contains the measured query changes; the final
candidate additionally fixes ambiguous-delete invalidation and warming retry
history. Warming was disabled during these diagnostics. Treat this as negative
diagnostic evidence, not a final-candidate optimized comparison or a root-cause
fix. A preliminary copied-state probe was rejected by the expected
`NativeArtifactStoreLocationChanged` fence; qualification used fresh state.

Post-matrix GCS generation, size and MD5, and the local fixture SHA256, match
the unchanged source. All source/artifact GCS operations used the existing
qualification project/bucket; no export, VM or hosted deployment was launched.
The harness retains complete warm sample histories in its qualification reports;
the tables above summarize them. Same-region Cloud, archive-scale capacity and credential-lifetime
qualification remain outstanding, alongside the unexplained outliers.

A subsequent review fix makes navigation-cache allocation failure fall back to
request-owned authenticated decoding. Denied flight, item or payload allocation
must not fail a required read when its request still has workspace. Single and
batched reads preserve their original budget charge, cancellation and deadline
checks; failures of required request allocations still propagate. Nine focused
Debug navigation, warming, source-resolution and text-directory tests pass
without leaks. The performance tables remain pinned to the recorded engine
revision above; this memory-pressure fix is validated separately.
