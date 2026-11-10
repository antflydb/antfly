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

Streaming read allowances are now renewed before page-store dispatch, including
single-page and batched decoded-cache callbacks. Routing cursors can continue
past their initial allowance using bounded per-page/per-wave admission; ordinary
query stores retain cumulative limits on both cold reads and warm cache hits.
The regression covers raw and decoded dispatch, exhausted prior allowances,
single and batched reads, and cancellation. All 20 focused Debug navigation,
publication and coalesced-I/O tests passed without failures or leaks.

## Composed reader lifetime and immutable HN tables

A composed text/scalar request owns a bounded scope of up to 16 prepared lake
readers, with a 64 MiB preparation budget. Refills and overlay key probes reuse
the exact source cut first returned by that leaf. Each leaf still resolves and
authorizes its table; reuse fences incarnation, object generation, index recipe,
visibility, receipt watermark, expiry, cancellation and the reader/snapshot lease.
Readers are request-local, never shared across principals or unrelated requests.
An explicit `full_text_index` selector uses the same shared text reader even
though the parser represents it as one named component. Vector and
multi-component execution keep their existing preparation path.

The prepared store must be constructed at its final address before publication
selection: cloud-generation bindings retain a pointer to its inline object-store
implementation, including during cleanup. Moving an already bound store leaves
that pointer dangling. Mutable overlay wrappers belong to individual executions;
retained overlays cache their physical suppression mask in the prepared reader's
bounded arena, so subsequent leaves can reuse it without retaining a pointer into
a freed leaf arena. WAL-cache pins retain their separately owned masks as before.

Overlay windows grow with observed suppression, up to the existing 128-row
physical batch limit. Unfiltered indexed probes still cover every replacement
key, including matching buffered rows, tombstones and newer nonmatching rows.
Skipping probes based only on matching buffered hits would let duplicate newer
keys outside that ranked window evade validation. These changes preserve source
ranks and continuation positions.

In the initial reader-sharing implementation measured below, internal refills
use request-local cut references. Only a returned continuation materializes
durable leaf capabilities and its composed cursor. Counts and complete result
sets need no retained cursor uploads. Public cursors use the existing
`source2:`/`lake2:` protocol and expiry; a restart must reopen the same published
source and accepted WAL cut. That implementation reduces redundant setup and
needless retention writes while retaining fresh remote authority checks and
continuation uploads. The native owner-admission follow-up below moves those
remaining operations out of the supported query path.

On November 7 UTC, the managed rc.10 HN qualification instance's public bindings
were migrated to explicit `object_mutability: immutable`. The archive retains
its frozen Parquet input; the current writer uses a fresh Iceberg warehouse
prefix and new file URIs. All five original CDC transactions were replayed with
their original source epoch, checkpoints and batch IDs. Exact retries returned
the original WAL positions. Full row comparisons verified 175 archive rows and
117 current rows; the saved source and existing scoped read key were preserved.
Four combined cursor pages returned all 72 distinct `database` matches. The old
bindings remain as `hn_archive_mutable_20261107` and
`hn_current_mutable_20261107`; their files were not deleted. The producer remains
at checkpoint 5 with its native state and backup retained.

This is qualification data, not the full HN archive. Migration alone did not
remove the composed metadata overhead. Managed end-to-end medians in a small
before/after diagnostic were:

| View | Before (3 samples) | After (5 samples) |
| --- | ---: | ---: |
| History | 245 ms | 359 ms |
| Recent | 853 ms | 831 ms |
| All | 2,592 ms | 2,369 ms |

After samples used a 1.2-second inter-request delay. An earlier unpaced All
request returned HTTP 500; the runtime recorded `RateLimited` from GCS, and the
immediate recheck succeeded. Keep that failure separate from successful latency
samples. Different pacing, small samples, fresh publications and an unchanged
rc.10 runtime make this an operational check, not a causal engine speedup or a
production latency qualification. The new reader-sharing code needs its own
release and managed benchmark.

The local regression in `zig/e2e/antfly/test_composed_lake_readers.py` uses real
Parquet, managed Iceberg CDC, a newer nonmatching row and a tombstone. It requires
two source/publication preparations despite multiple composed leaves, exact
counts and duplicate-free pagination after later accepted writes and a process
restart. Fresh requests must see the new ACK while the first cursor keeps its
original cut. Released
rc.10 performs seven source/publication preparations on that same fixture.

The fixture keeps a later CDC batch pending by revoking only warehouse write
access before acceptance. WAL admission remains writable. It resumes both the
original published cut and the later nonempty WAL cut after restart, checks the
exact distinct IDs, and checks reuse across several resumed leaf calls. Its GCS
variant requires explicit qualification bucket/prefix/token-file settings and
verifies that both retained publications contain cloud-generation maps. The
credential is resolved through a secret reference; it is absent from the config.

The GCS correctness fixture spaces API requests by 1.2 seconds without retrying
failed queries. Cloud Storage documents a [one-write-per-second limit for the
same object name](https://docs.cloud.google.com/storage/quotas#objects). An
unpaced rc.10 baseline failed during resumed pagination with `RateLimited` and
HTTP 500; the paced baseline passed. The log does not identify the throttled
object or prove which quota caused it. Preserve that failed run and qualify
unpaced throughput separately. Request latency excludes the pacing delay.

Set `ANTFLY_E2E_COMPOSED_PROFILE_ROUNDS=5` for opt-in measurements of the first
query after restart and five warm queries, including sampled server RSS and CPU.
These use the fixture's small corpus, retain durable/OS caches, and must run
without competing compilers. They supplement correctness checks; they do not
qualify full-archive performance or a regional managed deployment.

### Candidate qualification, November 7 UTC

The native ARM64 ReleaseSafe candidate at `dd2d98ce38` (documentation head
`a1ee317fa6`) built successfully. Its binary SHA-256 is
`03f068d1935b2f0ac040740af56f5685f4b302e578bc708d5c1cd664cf22c424`.
Twenty fresh focused regressions passed with zero failures/leaks and direct exit
0. All four HTTP cases passed: explicit/inferred text-index selection, each with
local and real GCS artifacts. Every case measured **two preparations for seven
fresh leaf calls**, and **two for three calls** on the resumed pending-WAL page.
Both retained cuts returned their exact distinct IDs after writes and restart;
GCS cases verified both publication generation maps and nonempty retained WAL
change parts. The budget-exhaustion regression retained no readers or charge.

The HTTP corpus has 100 historical rows and 31 initial current rows. GCS holds
native artifacts and the WAL; Parquet and the managed Iceberg warehouse remain
local. Measurements below use five warm requests after one restart query,
retaining durable/OS caches. GCS requests have a 1.2-second inter-request delay,
excluded from latency. RSS is the sampled server maximum during those requests.

| Artifacts | Text index | Restart query | Warm median | Sampled peak RSS |
| --- | --- | ---: | ---: | ---: |
| Local | Explicit | 28.23 ms | 15.26 ms | 127.75 MiB |
| Local | Inferred | 31.84 ms | 13.82 ms | 129.16 MiB |
| GCS | Explicit | 2,086.51 ms | 1,157.51 ms | 184.95 MiB |
| GCS | Inferred | 2,001.11 ms | 1,157.24 ms | 185.80 MiB |

A separate local matrix passed both stored-source and Parquet-hydration cells
with 10,000 unique real HN comments across two Snappy Parquet files (4,935,176
bytes). Inputs came from bounded `tabledata.list` reads of
`bigquery-public-data.hacker_news.full`, normalized with the example's serving
row conversion. No BigQuery query/export jobs were created. These unordered
pages are not a random sample: story/title/score diversity and external comment
ancestors are unqualified. The files were written by PyArrow, not BigQuery.
Input SHA-256 values are
`b34eb0bc2acd66944170158ef9a5492f0781060ee00070692c8850835ce35b3f`
and `e34f09a1dd84b0e21ab4f0ab7c2f346ed4654da11a9a416003a423ea2ab5e39b`.

| Local archive mode | Cold search | Warm median | Warm maximum | Restart | Child peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Stored source | 25.16 ms | 8.75 ms | 14.52 ms | 19.85 ms | 302.31 MiB |
| Parquet hydration | 27.46 ms | 5.19 ms | 8.65 ms | 20.79 ms | 221.03 MiB |

This matrix uses a 4 GiB process budget, five warm samples, navigation warming
disabled and the serving cache cleared after publication. OS caches are not
flushed, and setup includes SQL count. Child RSS is the maximum over the entire
run, including publication and restarts, rather than query-only memory. Both
cells verified highlights, metadata filters against residual execution, exact
filters and score/pagination stability after restart. With 128 native current
rows, union warm medians were 48.58/48.56 ms and saved-overlay medians were
54.06/53.54 ms for stored-source/hydration respectively. These current rows use
native storage, unlike the managed Iceberg HTTP regression above.

All measurements are from a shared developer host, with no competing builds
from this task during timings. Other host activity, small sample counts and
different fixture/build settings prevent a causal comparison with earlier
rc.10 measurements or a full-archive claim. The roughly 1.16-second GCS warm
result still includes remote admission and continuation work; the proposed
authoritative owner cuts, precomputed cursor references and broader warming
remain planned work, not measured improvements in this candidate.


### Native owner admission and reusable capabilities

The follow-up candidate admits a compact serving cut through native metadata.
A cut binds the table incarnation, owner epoch, desired index recipe, immutable
publication, accepted WAL watermark and retained image references. The compact
projection is updated in the same transaction as lifecycle authority and is
included in native snapshot recovery. Older metadata peers use the existing
linearizable lifecycle read until their compact projection endpoint is available.

Enable `lake_indexes.serving_cuts.enabled` only after all writers and maintenance
workers run a compatible release. Ownership is durable and remains enforced if
that option is later disabled. Managed Iceberg catalog and WAL heads carry a
conditional owner claim: writers that bypass native acknowledgment cannot
silently change an owned source. A sealed Parquet manifest fixes archive
membership and pins each object's generation or ETag. Mutable inventories and
shared external REST catalogs retain strict remote authority checks.

Publication and write-acceptance work constructs durable retained images before
queries need them. Fresh owned reads admit their snapshot and WAL position from
native authority, open captured metadata, and issue authenticated `lake3:` or
`source3:` references without uploading cursor state. Queries use a read-only
artifact store. Immutable cache misses still fetch remote blocks; this does not
promise a zero-I/O cold request. A remote commit whose native acknowledgment is
unconfirmed returns an unknown outcome and no read receipt. Producers retain and
retry the exact request until native acknowledgment is confirmed.

Capabilities authenticate kind, scope, incarnation, owner and expiry. Raw payloads
are bounded to 1 MiB; composed encoded tokens to 2 MiB + 4096 bytes, and leaf tokens
to 16 KiB. Encoding checks the same limits as decoding, including JSON escaping,
before issuing a token. The HN gateway accepts opaque ASCII cursors in POST bodies
within the composed limit and reserves capacity before reading the body.
Legacy `source2:` and `lake2:` continuations remain supported.

A retained physical suppression mask belongs to the prepared reader's arena,
never a leaf's temporary arena. Prepared readers are limited to 16 entries and a
64 MiB request budget. Object-store bindings are constructed at their final
address before binding cloud generations. Bounded background warming uses the
same authenticated caches and yields when the scheduler denies speculative work.

Serving-cut renewal runs independently before WAL drain and publication work.
Blocked warehouse writes must not prevent renewal over an existing publication
and durable pending WAL. Increasing cursor retention also forces renewal rather
than reusing a cut with an insufficient horizon. Old retained cursors continue
to use their original protected generation and accepted watermark.

Qualification must use the candidate binary, with matched owned/strict modes,
nonempty pending replacements across restart, real GCS generations, both index
selectors, and stored-source/Parquet hydration with highlights. First requests
after restart, warm requests and requests after deleting the fixture's durable
serving cache are different measurements. Clearing that cache does not flush the
OS or provider caches. No earlier rc.10 or reader-sharing measurements establish
the performance of native owner admission, and synthetic fixture results do not
qualify the full Hacker News archive or the deployed Cloud service.


Immutable Iceberg manifest lists use the same bounded decode-cache mechanism as
immutable manifests. The cache key includes the credential scope, list URI and
complete admitted metadata image. A lease pins the parsed list through delete
planning; warm queries avoid both HEAD/range setup and repeated Avro decoding.
Consumer size limits and cancellation still apply on hits, and cache admission
failure falls back to the existing read path. Mutable sources continue remote
revalidation. This caches immutable evidence after authority selection; it does
not substitute a cached catalog pointer for current authority.

To reproduce the local matched-binding matrix from a worktree with a freshly
built server:

```sh
ANTFLY_BIN="$PWD/zig/zig-out/bin/antfly" \
ANTFLY_E2E_COMPOSED_PROFILE_ROUNDS=5 \
uv run --project zig/e2e/antfly pytest -q \
  zig/e2e/antfly/test_composed_lake_readers.py -k local \
  --junitxml=qualification-assets/owned-local.xml
```

Both modes register the same sealed archive shape. Only native mode enables
serving-cut ownership. The fixture checks malformed-manifest rejection, two
preparations over multiple leaves, full result sets, later mutations, pending
replacements across restart, cursor tampering, cancellation recovery and a
retention increase while warehouse writes remain disabled. Owned mode also
checks zero query-time cursor-publication calls and a burst of 12 searches at
concurrency four.

GCS runs require `ANTFLY_E2E_GCS_BUCKET`, a fresh
`ANTFLY_E2E_GCS_PREFIX=qualification/...`, and an OAuth token in a private file
specified by `ANTFLY_E2E_GCS_TOKEN_FILE`; select `-k 'not local'`. Each case adds a
UUID and records its exact `artifact_prefix` for scoped cleanup. `gcs` keeps raw
source files local; `gcs_remote` puts source Parquet and the managed Iceberg
warehouse in GCS too. Mutation calls and strict-control reads are paced to avoid
coordination-object write-rate interference; native-owned composed reads and
concurrent searches are unpaced. Reported latency excludes that pacing.

For real comment bodies, set `ANTFLY_E2E_COMPOSED_CORPUS_DIR` to a directory of
Parquet files containing `body`, set `ANTFLY_E2E_COMPOSED_ARCHIVE_ROWS` to their
combined row count, and choose `ANTFLY_E2E_COMPOSED_PROFILE_TERM=database` and
`ANTFLY_E2E_COMPOSED_HIGHLIGHTS=1`. The fixture assigns deterministic IDs and adds
a correctness marker to the first 100 bodies; performance queries search the
independent profile term over the whole corpus. Set
`ANTFLY_E2E_COMPOSED_STORE_SOURCE=1` or `0` to compare stored-source and Parquet
hydration. JUnit properties retain corpus hashes, binary hash, host load,
concurrent compiler count, samples, CPU, sampled peak RSS/footprint, response
bytes, continuation size and cache-cleared startup/query time. UTF-8 highlight
spans are checked against returned fragment bytes.

Native-admitted cuts also expose their retained generation lease through the
lake reader's snapshot-protection port. That pin owns a separate checked reader
handle and preserves cancellation, deadlines and expiry. An authenticated
publication's exact-version delete proof can then be reused under native
retention, without a fresh delete-object HEAD on every search. The adapter is
installed only after admission of the native cut; strict external reads retain
their catalog pin and qualification rules. Missing or unsupported version proofs
continue to use the existing validation path.

## Native-owner candidate qualification (November 7)

The integrated candidate uses source `a6f7ece028753bf2017dfd9855cb9252db31a295`,
including main through `e08df520acecd6431355852b4e60c430d0d19bfb`. Its server
SHA-256 is `c5c98b06c2ab4cb4e7ccc428d34fe85b4a410a8b77d47c4b45d27a9ec9312530`.
It is an ARM64 macOS ReleaseSafe build with stripping and Metal disabled, built
with `zig build antfly capi -Doptimize=ReleaseSafe -Dstrip=true -Dmetal=false -j1`.
The complete build passes all 85 steps. The matching Lite library SHA-256 is
`4b8b9b2ac174ca0799807ff058043d40943769b18365d1141887bcc26f9245a2`.

Integrated regression coverage passes 39 API, 5 embedded/storage, 22 catalog and
44 error-ABI tests without failures or leaks. The full HN functional suite
passes 220 tests and 50 subtests, with 18 conditional skips. The native HTTP
gateway check also passes. Command ownership now includes nested source UUIDs,
retained image references and provider generations, with allocation-failure
coverage after integration with main's independent query-artifact collection.

In the matched small local fixture, each mode prepares two sources across seven
leaf calls. Resumed requests with pending replacements prepare twice across
three leaf calls. Exact result IDs, cloud-bound reader destruction, cursor
mutation/restart behavior, retention increases with blocked warehouse writes,
and cancellation recovery are checked before accepting measurements. Native
queries record zero cursor-publication calls; strict controls record three.
These are retention-operation counters, not provider HTTP or billing counts.

| Local objects, small fixture | Warm median | First query after restart | Serving-cache-cleared query | Sampled query footprint |
| --- | ---: | ---: | ---: | ---: |
| Strict, explicit index | 19.35 ms | 31.20 ms | 33.56 ms | 96.05 MiB |
| Strict, inferred index | 16.77 ms | 31.84 ms | 39.16 ms | 93.83 MiB |
| Native-owned, explicit index | 14.94 ms | 23.47 ms | 26.35 ms | 96.17 MiB |
| Native-owned, inferred index | 14.84 ms | 23.60 ms | 23.19 ms | 97.38 MiB |

Each warm median uses five requests. Native local bursts of 12 requests at
concurrency four have medians of 21.71–21.78 ms and maxima of 32.76–36.70 ms.
The shared developer host had 1–6 concurrent Zig compilers during these local
cases; none belonged to this qualification. These samples are diagnostics,
not tail-latency SLO qualification. Sampled query footprint excludes index
construction and is not a whole-lifetime peak. Serving-cache clearing leaves OS
and provider caches intact. The small fixture does not represent archive scale.

For the same small sealed fixture, the GCS matrix passes all eight cases:

| GCS configuration | Warm median | First query after restart | Serving-cache-cleared query | Sampled query footprint |
| --- | ---: | ---: | ---: | ---: |
| Strict, explicit, artifacts only | 1167.62 ms | 2033.10 ms | 5264.68 ms | 166.36 MiB |
| Strict, explicit, sources + artifacts | 1834.39 ms | 2839.79 ms | 6243.11 ms | 182.56 MiB |
| Strict, inferred, artifacts only | 1143.68 ms | 2025.17 ms | 5198.01 ms | 167.34 MiB |
| Strict, inferred, sources + artifacts | 1748.01 ms | 2846.69 ms | 6190.33 ms | 178.10 MiB |
| Native-owned, explicit, artifacts only | 15.26 ms | 246.39 ms | 3823.24 ms | 170.95 MiB |
| Native-owned, explicit, sources + artifacts | 12.59 ms | 260.88 ms | 3886.37 ms | 165.39 MiB |
| Native-owned, inferred, artifacts only | 13.96 ms | 251.89 ms | 3503.89 ms | 156.75 MiB |
| Native-owned, inferred, sources + artifacts | 13.45 ms | 328.79 ms | 3879.67 ms | 173.67 MiB |

Native-owned fully remote warm requests now have similar latency to the local
fixture. This is consistent with removing required provider round trips from
the hot path, including the redundant delete-object qualification HEAD.
Restart and cache-cleared requests still pay remote reads: the fully remote
native cases take 261–329 ms and about 3.9 s respectively. This is a warm-path
improvement, not a claim of local-database cold performance. Both selectors
retain exact mutation/restart pagination, two source preparations across repeated
leaf calls and zero query-time cursor-publication calls in native mode.

The larger fixture projects 10,000 real HN comment bodies into `id`, `body` and
`deleted`; it does not preserve the full source schema. It contains 3,586,561
UTF-8 body bytes after correctness markers (mean 358.66 bytes, range 1–7,554).
The first 100 rows receive a marker for pagination assertions. Timed requests
search the independent term `database`, request highlights and use either
stored-source text or source-Parquet hydration. The original corpus-file
SHA-256 values are:

- `part-0.parquet`: `b34eb0bc2acd66944170158ef9a5492f0781060ee00070692c8850835ce35b3f`
- `part-1.parquet`: `e34f09a1dd84b0e21ab4f0ab7c2f346ed4654da11a9a416003a423ea2ab5e39b`

| 10,000 comment bodies, highlights | Warm median | First query after restart | Serving-cache-cleared query | Sampled query footprint |
| --- | ---: | ---: | ---: | ---: |
| Strict, local, stored source | 14.54 ms | 37.16 ms | 42.98 ms | 101.97 MiB |
| Native-owned, local, stored source | 9.11 ms | 28.20 ms | 27.81 ms | 102.25 MiB |
| Strict, fully remote GCS, stored source | 1835.45 ms | 5471.47 ms | 9592.48 ms | 192.60 MiB |
| Native-owned, fully remote GCS, stored source | 12.63 ms | 2380.45 ms | 6957.05 ms | 171.92 MiB |
| Strict, local, Parquet hydration | 13.22 ms | 48.00 ms | 31.85 ms | 136.09 MiB |
| Native-owned, local, Parquet hydration | 9.52 ms | 21.55 ms | 25.76 ms | 133.16 MiB |
| Strict, fully remote GCS, Parquet hydration | 1742.90 ms | 3680.36 ms | 11883.86 ms | 235.49 MiB |
| Native-owned, fully remote GCS, Parquet hydration | 7.49 ms | 1074.20 ms | 8510.16 ms | 216.11 MiB |

All 21 HTTP cases pass: 12 small-fixture cells, the native gateway check and eight
real-body cells. Every measured case's binary hash matches the candidate above;
all real-body cases have the same input corpus hashes. Each retained pending
refill uses two preparations over three leaf calls. Native mode records no
query-time cursor-publication calls in either source-storage mode. The fully
remote real-body native bursts at concurrency four measure 19.51 ms median /
25.54 ms maximum with stored source and 8.91 ms median / 12.26 ms maximum with
Parquet hydration. The developer host records 0–6 other concurrent Zig compilers
across the matrix. No qualification build runs during these measurements.

The warm-path improvement is substantial on these fixtures. It does not remove
cold/restart I/O: the real-body native GCS cases still take 1.07–2.38 s on the
first query after restart and 6.96–8.51 s after clearing the serving cache
(7.27–8.82 s including process startup). Their sampled query footprints are
172–216 MiB. Stored-source and hydration cases have different cache access and
host-contention histories; these few samples do not establish that hydration
is generally faster. Raw timing reports, cursor references, access tokens and
large fixtures are not committed to the repository.

Full-archive capacity, diverse-query tail latency, replacement-owner cold loss,
refreshing credentials beyond one token lifetime and the managed Cloud route
remain separate deployment qualifications. These results neither deploy the
candidate nor establish DuckDB cold-performance parity. Upgrade and release
coordination still precede enabling durable native ownership in production.

## Reviewed candidate qualification (November 8)

The reviewed implementation is `737f1385065ecd6616e215d1a88b1422541fb984`, with
qualification-fixture refinements in `3375bcbe52`. The matching ReleaseSafe
server/Lite build passed all 85 steps and the 141-function export boundary:

- Server SHA-256: `a72f167b8065275fe9da8da527b161df7b4cf6cd1c3985733d5ca5ee6942848d`
- Lite SHA-256: `c3f6e16e4c7e0a11ee87dbf2cc3b84b49738d1f26149acd05e9425352a074b46`

The same 10,000-comment corpus above was queried through fully remote GCS,
with highlights and seven hits. Warm values are medians of five single-attempt
requests. Query memory is sampled during restart, warm and cache-cleared reads.

| Source delivery | Warm median | First query after restart | Verified-cache-cleared query | Peak sampled RSS / footprint |
| --- | ---: | ---: | ---: | ---: |
| Stored source | 8.90 ms | 2345.33 ms | 6206.82 ms | 189.20 / 163.16 MiB |
| Parquet hydration | 7.51 ms | 831.89 ms | 8206.94 ms | 250.19 / 220.89 MiB |

Full restart, including shutdown, startup and the first query, took 2797.91 ms
and 1290.58 ms respectively. Cache-cleared startup plus query, excluding
shutdown, took 6526.45 ms and 8535.66 ms. OS/provider caches were retained.
Five other Zig compilers were active at stored-source profiling admission and
three at hydration admission; one-minute host loads were 14.57 and 11.28.
These current-load samples show seconds-scale cold latency and do not establish
an order-of-magnitude cold improvement or isolate GCS wait from host scheduling.

Both profiling requests recorded two preparations across three leaf calls.
The GCS stored-source case kept readers active through three acknowledged
batches and published visibility: 476 searches succeeded during that interval,
with zero rebuilding responses. Its fresh composed search included all three
new IDs, while retained cursors preserved their earlier cut. The independent
local stress case recorded 68 successful searches, zero rebuilding responses
and one retried published-snapshot binding conflict, using the original
incarnation-fenced receipt. Accepted readers remain strict about conflicts.

Validation passed: 188 focused native tests, 49 runtime-ABI tests, 237 HN tests
and 52 subtests, two additional real-GCS native-cursor modes, four local
composed cases, the native gateway case, eight GCS correctness cases and both
real-corpus profiling cases. The unchanged optional DuckDB comparison test was
not run. Initial construction encountered one provider timeout; retrying the
existing fixture completed publication. Initial 10k readiness now has a bounded
600-second setup allowance, separate from unchanged single-attempt query
timings. The deployment qualifications described above still apply.

Full-lake E2E now collects a native HN managed-overlay bridge, exercising the
hierarchy/restart regression through the assembled artifacts. This additional
local, credential-free bridge passed with the locked lake/Iceberg extras.


## Cold working-set follow-up

The follow-up to PR #1550 keeps the existing 16-segment / 16 MiB optional
warming allowance and reader/cancellation fences. Within each admitted segment,
opening metadata takes priority over arbitrary posting prefixes. A process-local
history records up to 16 recently demanded blocks for each of 64 immutable
segment directories. History keys include the credential cache scope and exact
directory identity; entries contain no credentials, provider clients, leases or
request pointers. Recording uses a nonblocking mutex attempt, so optional
history cannot stall demand I/O. Warming resolves hints through the current
validated directory and authenticates every block normally.

This history improves rewarming after eviction within a process. It is not a
durable hot-set manifest: after restart, the verified disk cache and publication
opening metadata provide the cold-start path. The bounded publication traversal
still limits coverage of large archives; no full-archive residency claim follows
from this change.

Owned text/ordered-row readiness now selects the retained native cut through the
same preparation path as queries. It retains credential/store/definition checks,
reader protection, cut expiry and accepted-WAL visibility while avoiding fresh
live-source catalog discovery. This additional readiness preparation does not contribute query-phase metrics.
Tables containing vector or materialization definitions retain strict live-source
qualification. Manifest loading transfers its verified allocation into the decoded
owner, and source planning transfers its manifest into the serving source instead
of duplicating it. Parallel views continue borrowing from their parent source.

The composed HTTP fixture accepts `ANTFLY_E2E_COMPOSED_CACHE_MIB` (1–256,
default 64) and `ANTFLY_E2E_COMPOSED_PENDING_ROWS` (0–10,000, default 0). Extra
pending rows do not match the correctness query, but must survive retained-cut
construction, resumed pagination and suppression checks. These options extend the
existing real-corpus, selectivity, concurrent-write, restart and cache-cleared
checks without changing their expected results or retry contracts.

The larger pending-WAL case exposed a preparation-budget defect: physical-mask
construction retained each partition's temporary predicate plans in the shared
prepared-reader arena. Resolve each partition in reclaimable scratch while
preserving the enclosing memory budgets, then copy only its completed physical
row set into the retained owner. No query limit was increased. A native
regression resolves 256 partitions with 128 KiB of temporary planning each under
a 256 KiB budget, checks all retained row identities and verifies complete
budget release. The original HTTP reproduction also passes with 10,000 real
comments, 1,000 additional pending changes and an 8 MiB memory cache.

Qualification used engine source `ee54d8350811e697b6638a0f578e26f57a1e3c19`
with the same corpus hashes recorded above. The ReleaseSafe server SHA-256 is
`9f7d5f101ac4b2eff91a44cab01ef0df01a11b43026fd6314b71da2ad2ad9b56`;
the matching Lite library is
`678d9aa63ed76836505f6c4472858260f401024fa6807f92bfed17b8768e0295`.
The integrated build passed 96/96 steps and the 141-function public export
boundary. All 70 focused native tests passed without failures, skips or leaks.
The four local composed modes passed. The HN hierarchy/restart bridge initially
returned HTTP 503 `InvalidSocketOption` during dry-run vacuum; an unchanged
repeat passed. That intermittent transport observation remains visible here.

Both fully remote GCS modes passed with highlights, 1,000 additional pending
changes and an 8 MiB memory cache. They verified two preparations across three
leaf calls for both profiling and resumed pending cursors, zero query-time
cursor publication, and three acknowledged ingestion batches during concurrent
reads. Stored-source and Parquet-hydration modes completed 424 and 449 successful
publication-overlapping reads respectively, with no binding conflicts or
retryable rebuild responses. Single-request samples were:

| Mode | First after restart | Warm median (3) | Cache-cleared query | Sampled peak RSS | Sampled peak footprint |
|---|---:|---:|---:|---:|---:|
| Stored source | 2,643 ms | 81 ms | 6,399 ms | 194 MiB | 181 MiB |
| Parquet hydration | 1,169 ms | 83 ms | 7,581 ms | 196 MiB | 233 MiB |

These are client HTTP times excluding startup; memory peaks cover the restart,
warm and cache-cleared samples. The two modes ran concurrently on a busy host
(load averages approximately 14/18/17, four concurrent Zig compilers). The
12-query burst at concurrency four had medians of 121/116 ms. Reads during
ingestion/publication had medians of 319/327 ms and maxima of 5.49/5.24 seconds.
Separate selectivity probes ranged from 74 ms to 1.86 seconds, including zero
hits for the English stopword `the` and the absent term. Profiling uses
`database`; the initial profiling attempts incorrectly required seven hits for
`the`, after their correctness checks had passed. No analyzer or correctness
expectation was relaxed.
All object versions under the task-owned GCS UUID prefixes were deleted and
their absence verified after the runs; private qualification token files were
removed. No infrastructure, IAM, deployment or release was changed.

This pressure workload differs from the earlier baseline. The results do not
establish a cold-latency speedup, a matched A/B comparison, DuckDB parity or
full-archive performance. Restart and cache-cleared reads still incur remote
I/O; the bounded demand history alone does not remove those costs. Only this
compact new evidence is retained in the repository.

Remaining deployment qualification uses a released candidate on the existing
managed route and intended CPU/memory shape, sustained credential refresh and
owner replacement with loss of local cache. Full-archive query diversity and
cache pressure must be measured before making archive-wide latency claims. Real
S3 qualification remains waived for this task. Keep compact new results here;
the November 8 baseline above does not need duplicated artifacts or reruns.

### Reset-socket recovery and original-settings check

Local syscall tracing identified the intermittent HN transport failure:
Darwin returns `EINVAL` when setting a valid timeout on a TCP socket made
defunct by a peer reset. The observed value was 25 ms on a live socket
descriptor. An independent loopback reproduction confirmed both timeout
options fail this way even after `recv` consumes `SO_ERROR`; `getpeername`
still identifies the disconnected socket. Timeout setup now reports
`ConnectionResetByPeer` for this state. Invalid descriptors and option errors
on connected sockets retain their existing diagnostics. Healthy timeout setup
performs no additional syscall.

Buffered and streaming HTTP/1 pool admission replace at most one such reset
connection before publishing the request socket or transmitting headers/body.
The replacement shares the request's cancellation and deadline. Existing
method-aware replay rules continue to govern failures during transmission or
response delivery. A deterministic loopback regression seeds a reset pooled
connection, consumes its socket error, then sends a POST with retries disabled
through each response API and verifies exactly one server-side request and one
response body. Disabling the fix reproduces `InvalidSocketOption`; restoring it
passes. The HTTP library gate passed 619 tests with eight platform skips.

The original-settings check used the same previously recorded engine binary
and corpus, `database` with highlights, a 64 MiB cache, zero additional pending
rows and five warm samples. It isolates the preparation/warming changes from
the subsequent transport correction. Results against the earlier recorded
baseline were:

| Mode / measurement | Earlier baseline | Original-settings candidate |
|---|---:|---:|
| Stored-source warm median | 8.90 ms | 7.47 ms |
| Parquet-hydration warm median | 7.51 ms | 9.04 ms |
| Stored-source first after restart | 2,345 ms | 2,764 ms |
| Parquet-hydration first after restart | 832 ms | 720 ms |
| Stored-source cache-cleared query | 6,207 ms | 7,360 ms |
| Parquet-hydration cache-cleared query | 8,207 ms | 7,823 ms |

Both cases passed and again recorded two preparations across three leaf calls.
Stored-source/hydration sampled peak RSS was 202/291 MiB and footprint was
174/267 MiB. One-minute host loads were 15.0/9.7 with three/four concurrent Zig
compilers. These reuse the original workload settings; they are not a matched
host experiment. Warm latency remains approximately 7–9 ms, and cold queries
remain dominated by seconds-scale setup. This establishes no material cold
speedup. The demonstrated improvement is bounded memory and successful retained
pagination/publication with the larger pending overlay described above.

The transport correction in `32697344d1` passed a fresh ReleaseSafe server/Lite
build: 96/96 steps and 141 public exports, with no internal exports. The server
SHA-256 is `706c0b1d75b8d4c182fefc3de25f31541be49d368183e85847a7a8cc6f1628f7`;
the matching Lite library is
`d1c40c7dba354d648732b1974ff5f06b3f7ee15ea5525a2c1f731d1b4060e251`.
The native HN maintenance/hierarchy/restart fixture passed in 164 seconds,
including an explicit assertion rejecting `InvalidSocketOption` anywhere in
the server log, including background work. One preceding run timed out at the
unchanged 180-second scheduled-WAL-GC deadline with a saved reader-grace
`waiting` result, zero recorded job failures and no invalid socket options.
This separate timing observation remains recorded; no timeout, grace period or
correctness assertion was relaxed. The fixture now retains at most 32 scheduler
state transitions in timeout diagnostics.

Main at `7db8b07aee` subsequently merged cleanly, without changing the transport
sources or this HN fixture. The HTTP library gate passed again using its verified
cache. The optimized binaries above precede that merge; full CI must validate
the final merged revision after maintainer approval.

### Publication opening bundles and restart inventory

Candidate `a16071381e` implements the [cold-opening design](../../design/table-object-storage.md#cold-opening-and-restart-recovery):
publication-wide verified metadata/range bundles, bounded parallel admission,
clean disk-inventory recovery, early optional restoration and request-owned I/O
profiles. Reader protocol 98 retains protocol-97 compatibility; metadata
capability 85 fences new catalog writes. Existing publications gain bundles on
publication/rebuild. Source authority, reader protection and memory limits remain
mandatory.

The fresh ReleaseSafe server/Lite build passed 98/98 steps and the export boundary
check: 141 public functions, no internal exports. Server SHA-256:
`0ef8ca80e16596f70a003c4a3d6a22dd0b34a5940e4211aa0c5051bfd70fc845`.
Matching Lite SHA-256:
`74c0e520529977814720d06367aebd4fb7ebf051b3039f786b8c5832a925fc04`.
All 104 final focused query, recovery, cancellation, preparation-budget and
metric-exposition regressions passed without failures, skips or leaks.

Both real-GCS composed cases passed with the same 10,000-comment corpus,
`database`, highlights, a 64 MiB cache, zero additional pending rows and five warm
samples. Parentheses contain the preceding original-settings candidate above;
these are developer-machine samples with different host loads, not a matched A/B.

| Mode | Warm median | First after restart | Cache-cleared query |
|---|---:|---:|---:|
| Stored source | 7.86 ms (7.47) | 2.36 s (2.76) | 5.26 s (7.36) |
| Parquet hydration | 9.09 ms (9.04) | 1.28 s (0.72) | 5.78 s (7.82) |

Restart-plus-query totals were 2.75/1.73 s; empty-cache startup-plus-query totals
were 5.58/6.10 s (stored/hydration). Sampled peak RSS was 220/261 MiB and footprint
193/230 MiB. One-minute host loads were 11.3/33.6 with two/one other Zig compilers.
Both cases verified two preparations across three leaf calls, retained pending
pagination, concurrent ingestion/publication and exact highlighted hits. All
1,710 task-owned GCS object versions were deleted, absence verified, and private
credential files removed.

The retained request profiles show bundle opening at 0.63/0.51 s with an empty
cache and below 7 ms after restart. The stored-source cold request recorded 36
covered artifact loads (2.88 MB), 5.86 s summed provider time and peak concurrency
four; overlapping I/O sums are not wall time. Hydration recorded 11 covered
artifact loads (0.35 MB); its Parquet data reads are outside this artifact trace.
These profiles cover immutable Antfly artifact/cache loads, not every provider
metadata/authentication operation, Parquet read, HTTP request or billing event.

The empty-cache samples are 26–29% lower than the preceding samples, while restart
results are mixed and warm latency remains about 8–9 ms. This does not establish a
10× improvement, DuckDB parity or archive-wide latency. Further cold work should
use the remaining on-demand artifact and Parquet reads identified by these
profiles. The fixture retains only bounded diagnostic snapshots; its inventory
counter prefix filter was corrected without rerunning these timing cases.

The assembled Hacker News managed-lake HTTP regression passed in 174 seconds on
these matching artifacts, including hierarchy, pending enrichment, maintenance,
pagination and restart. The existing 180-second maintenance deadline and reader
grace remain unchanged, and the strict background-log assertion found no
`InvalidSocketOption`.

### Demand-read candidate qualification (2026-11-09)

Native revision `d2cd8141f9` passed the 98-step ReleaseSafe server/Lite build and
141-function export check. Server SHA-256:
`c83e22558ffa46bd9e7dbdfc0f6408e034359a20d5a318a5abf32da300bd9799`;
Lite SHA-256:
`aa83c3dcad3751ea846d787fb50965f850c292f16e9682bee4978ff38bd56ac3`.
The final selected regression gates passed 14 focused and 85 broader cases
(overlapping selections), with zero failures, skips or leaks.

Both real-GCS cases passed using the same original settings above. Parentheses
contain the preceding candidate; these are individual developer-host samples,
not a controlled A/B or full-archive qualification.

| Mode | Warm median | First after restart | Cache-cleared query |
|---|---:|---:|---:|
| Stored source | 12.14 ms (7.86) | 1.80 s (2.36) | 4.38 s (5.26) |
| Parquet hydration | 7.66 ms (9.09) | 1.73 s (1.28) | 4.75 s (5.78) |

Cold sampled RSS was 161/268 MiB and footprint was 188/238 MiB
(stored/hydration). Both cases verified two preparations across three leaf calls,
including resumed pending cursors, and concurrent ingestion/publication.

Cold query CPU was 368/355 ms (stored/hydration); most elapsed time remains
outside CPU execution. The stored-source leaf recorded opening at 262 ms,
ranking at 2.02 s and 21 covered artifact loads (5.01 MB), compared with
630 ms and 36 loads (2.88 MB) previously. Read-ahead trades fewer requests for
more bytes. Its fresh-process native-hydration aggregate was 1.90 s.
The Parquet leaf recorded opening at 289 ms and ranking at 2.15 s, with eight
covered artifact loads (0.62 MB) and 33 source loads (3.89 MB). Its fresh-process
source-hydration aggregate was 2.01 s. Process aggregates can include startup or
background work; overlapping provider durations are not wall time, and these
counters do not cover every authentication, catalog or billing operation.

The cold samples are 17–18% lower; warm and restart changes are mixed. The
sub-second and 10x goals remain unmet. Next work should reduce the remaining
dependent ranking reads and batch/prefetch identity and winning-row hydration
blocks, guided by these request profiles rather than larger speculative reads.

The assembled Hacker News HTTP regression passed in 175 seconds, including
hierarchy, pending enrichment, maintenance, pagination and restart, with the
existing deadlines and strict background-log assertion unchanged. All 1,690
task-owned GCS versions were deleted, absence verified, and credential files
removed. Raw qualification artifacts remain outside the PR.

### Batched-read candidate qualification (2026-11-09)

Native revision `8299117c73` includes the five-part read batching/locator change
and main through `42d1a0b9e2`. All 98 ReleaseSafe server/Lite build steps and the
141-function export check passed. The post-merge gate passed 119 cases
(76 API / 43 local), with zero failures, skips or leaks. Server SHA-256:
`aed141e3d0a7cefed4191b2195c0224fc8a137f3bd24bd6f3b95c1f74c478093`;
Lite SHA-256:
`e35df9fdd37c19ddab8aebeafd37f4a0b197d9eed640dbb6e5a32c4184031e9d`.

Both original-settings GCS cases passed. Parentheses contain the preceding
`d2cd8141f9` samples; these are individual developer-host observations.

| Mode | Warm median | First after restart | Cache-cleared query |
|---|---:|---:|---:|
| Stored source | 9.27 ms (12.14) | 1.74 s (1.80) | 4.97 s (4.38) |
| Parquet hydration | 15.41 ms (7.66) | 1.92 s (1.73) | 4.95 s (4.75) |

Cold CPU was 396/562 ms, sampled RSS 252/208 MiB and footprint 222/270 MiB
(stored/hydration). Both cases again verified two preparations across three leaf
calls, nonempty retained pending pagination, and concurrent ingestion/publication.

Stored-source native hydration fell from the preceding process aggregate of
1.90 s to 0.82 s. Its retained slowest lake call was 2.74 s, including opening
0.50 s and ranking 1.36 s; the composed request took 4.97 s across three calls.
That leaves about 2.23 s in other leaf calls, which the single retained lake-call
profile does not individually attribute. The composed profile recorded 29
artifact loads (3.59 MB), four traced map loads (0.48 s) and 23 direct-locator hits.

Parquet source hydration remained 2.14 s, with 33 source loads (3.89 MB), peak
source concurrency six and no recorded file-lane submissions. The fixture
combines the input corpus into one Parquet file with 1,000-row groups; the added
selected-file split therefore does not exercise multiple hydration lanes here.
Its composed profile recorded 14 artifact loads (1.93 MB versus the preceding
0.62 MB), four traced map loads (0.52 s) and five direct-locator hits. Additional
metadata/read amplification needs attention. Process aggregates may include
background work, and nested/overlapping provider durations are not additive.

These samples do not establish an end-to-end speedup; the sub-second and 10x
goals remain unmet. The next targets are selected-row-group splitting inside one
file, remaining map/opening dependencies, and per-leaf attribution of refill and
current-source costs. The assembled HN HTTP regression passed in 177 seconds.
All 1,673 test object versions were deleted with absence verified, and private
credential files removed. Raw logs and fixture results remain outside the PR.


### Selected row-group candidate (b9fb4d7260, November 9)

The optimized server/Lite gate completed all 98 steps and the 141-function export
check. The focused gate passed 34 cases and the broader gate passed 124 cases
(77 API / 47 local), with no failures, skips or leaks. Both original-settings
real-GCS cases and the assembled HN mutation/restart regression passed; HN took
174.54 seconds. No quiet-host rerun was required.

| Delivery | Warm median | First query after restart | Empty-cache query |
|---|---:|---:|---:|
| Stored source | 13.21 ms | 1.91 s | 3.71 s |
| Parquet hydration | 7.94 ms | 1.29 s | 3.79 s |

These are individual developer-host samples. The preceding cold results were
4.97 / 4.95 seconds; sub-second cold latency remains unmet. The Parquet case
recorded eight admitted row-group lane submissions, none declined, peak source
concurrency seven, and 27 source reads (previously 33). Both cases confirmed two
preparations across three leaf calls. Both still performed six generation-map
reads. Stored-source leaf times were 3.35 s for archive, 356 ms for current, and
1.57 ms for the current refill, motivating independent source waves and physical
locator capture for range-backed navigation.

Cold stored-source CPU/RSS/footprint were 436 ms / 173.7 MiB / 219.5 MiB;
Parquet values were 385 ms / 284.9 MiB / 254.2 MiB. The server SHA-256 is
`03426db292ca9cdb64e1b8451718681387f99c5a4ad0b8dcfc3a2b288df66607`;
Lite remains `e35df9fdd37c19ddab8aebeafd37f4a0b197d9eed640dbb6e5a32c4184031e9d`.
All 1,734 versioned objects from the two scoped prefixes were deleted with zero
remaining, and private credential files were removed. Subsequent source-wave,
locator-capture and posting-lookahead changes are not measured by these results.

### Source-wave candidate (390239b3f9, November 9)

The optimized build passed 98/98 steps and the 141-function export boundary;
148 broader tests passed without failures, skips or leaks. Stored-source GCS
qualification passed with a 3.27 s empty-cache query and 1.70 s restart query.
The composed trace recorded 22 artifact GETs and five map GETs; archive ranking
still took 1.67 s and preparation 0.79 s. Source calls now overlap: their summed
4.16 s exceeds the 3.27 s composed wall time.

The first Parquet run reached a published-visibility timeout. An unchanged-binary
reproduction passed, including concurrent publication and retained pagination:
3.88 s empty-cache, 1.07 s restart, 6.93 ms warm median. This does not establish
that the intermittent publication problem is resolved. The HN regression then
exposed a restart maintenance failure: an interrupted scheduler claim retained
its 120 s lease before the collector's separate 60 s reader grace could begin.
The three scoped GCS attempts deleted 2,600 object versions with zero remaining.

The next fixes target these observed gaps, before another optimized build:

1. Keep retention scans and scheduled maintenance outside publication's worker;
   run maintenance with independent bounded admission, fresh incarnation checks
   and existing provider-side CAS ownership. Release interrupted claims with a
   short conditional cleanup so orderly restart does not strand their leases.
   Fresh claim nonces also prevent ETag ABA on identical same-millisecond claims.
2. Capture physical selectors on leased artifact reads as well as allocated
   reads; retain authoritative map fallback for dependencies without selectors.
3. Reuse bounded prefetch flights on overlapping demand reads instead of issuing
   a second whole-pack read. Verify transferred bytes, cancellation and cache-hit
   behavior, rather than assuming fewer calls means less work.
4. Qualify the finished candidate against the original GCS paths and HN restart,
   publication, maintenance and cursor checks. Keep existing timeouts and reader
   protection. Sub-second cold latency remains unproven; broader archive/payload
   layout work depends on the resulting trace.
