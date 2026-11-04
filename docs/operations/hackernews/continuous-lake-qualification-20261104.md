# Continuous writable-lake qualification — 2026-11-04 UTC

**Both the baseline and first follow-up optimized runs failed qualification on
each provider.** The catalog-history
changes in #1480 work in this workload, and indexed acknowledged rows survived
searchable publication and restart. Accepted-search availability and maintenance completion
remain release/deployment qualification gaps.

## Revision and workload

Engine baseline: `e6144685f8699214df33f61fe544a8a05d097515`, including #1480
(`6817ccb184`) and #1483 (`3b01dbc34d`). Both native standalone builds succeeded
with Zig 0.17.0, Metal/CUDA disabled. Metadata used standalone mode with Raft
disabled and an explicit 4 GiB process-memory budget. CPU was not constrained to
the eventual Cloud instance shape.

ReleaseFast executable SHA-256:
`77129c112545cd7c47a21e09d040d5c2a65868987fb5adf54b04aadbbc758e8c`.
Debug executable SHA-256:
`9cf6aa12be574fceba2c540449a41ded3482155465be75ae41d914b67fd57454`.

Later main at `2c0905501d` includes #1479 (`270057158c`), changing Lite/LSM
writer admission and commit outcomes, and #1386's inference work. Neither was in
the frozen engine baseline. The Debug follow-up below does not clear the original
ReleaseFast failures; fresh optimized qualification is required before attributing
those stalls to current main.

The input was an existing 10,000-row HN Parquet export, SHA-256
`e23a8fe4215d3a8c66cd79b98314225211c8396cb5ff827365835bf6bce4947a`.
Only 128 normalized rows were used. Bodies were truncated and given a common
search term; this is a dense-match functional fixture, not natural archive term
frequency or a full-archive benchmark.

The [driver](../../../examples/hackernews/continuous_lake.py) used two fresh,
Antfly-managed Iceberg object tables: a single-key catalog-history table without
search indexes, and an indexed table with stored-source text and five scalar
indexes. It made 120 paced history commits, then a 32-transaction burst, followed
by 780 seconds of continuous changes, published/accepted highlighted searches and
vacuum. Requested update/query intervals were 3/2 seconds. Vacuum used normal
600,000 ms retention, `keep_latest: 2` and `max_deleted: 32`; the initial driver
waited 15 seconds between turns, including incomplete turns. Post-restart recovery
repeated the same pending operation IDs at one-second intervals.

The full ReleaseFast measurements used the driver at `ebd9bfa900`; the later
instrumentation and guard improvements were validated with focused tests and a
ReleaseFast smoke run, rather than represented as a new full baseline.

All timings came from a contended native ARM64 developer host. The filesystem and
GCS profiles overlapped; other builds were active, and Debug compilation/cache
inspection overlapped part of GCS traffic. GCS was accessed remotely from that
host. These are not matched fixed-throughput comparisons, colocated Cloud
latencies, a service SLO, or comparisons with DuckDB.

## Catalog-history behavior

| Observation | Filesystem | GCS |
|---|---:|---:|
| Acknowledged history/indexed transactions, including seed | 269 / 254 | 161 / 184 |
| Maximum observed metadata-log entries | 100 | 100 |
| First manifest merge, at WAL coverage 101 | 200 → 4 | 200 → 4 |
| Snapshots created by 32-transaction burst | 12 | 8 |
| Distinct observed snapshots across both tables | 381 | 146 |
| Distinct observed published revisions | 50 | 7 |

Snapshot/revision observations are lower bounds between samples. Fewer GCS
snapshots/revisions do not establish an efficiency improvement: fewer changes
completed there. The current metadata JSON can still grow with retained
snapshots; the 100-entry log bound is not a constant-size metadata guarantee.
Manifest merging does not compact Parquet data or equality-delete files.

## Search and visibility

Successful-request latencies, in milliseconds:

| Provider / visibility | Successes | Median | p95 | Maximum |
|---|---:|---:|---:|---:|
| Filesystem / published | 305 | 30 | 215 | 2,807 |
| Filesystem / accepted | 303 | 55 | 175 | 3,930 |
| GCS / published | 139 | 1,374 | 2,291 | 4,509 |
| GCS / accepted | 136 | 1,959 | 2,743 | 4,777 |

**Failures are additional to that table:** filesystem accepted search had two
60-second client timeouts; GCS accepted search had three retryable HTTP 503
responses. The original redactor retained those statuses and retryability but
mapped their unknown public codes to `http_error`; their precise codes were not
preserved. Do not infer the specific code from later harness improvements.
Both engine logs contained `CatalogRoutingSnapshotTimeout` warnings. The 3.93 s
accepted request is a new observation on this updating fixture, not a matched
reproduction or attribution of the earlier static-archive outlier.

Indexed-table first-observed lag after the acceptance ACK:

| Observation | Filesystem | GCS |
|---|---:|---:|
| Catalog visibility median / p95 | 12.0 / 22.4 s | 71.4 / 165.7 s |
| Searchable publication median / p95 | 14.2 / 49.3 s | 204.4 / 379.5 s |
| Final publication catch-up after producers stopped | 9.4 s | 211.9 s |

These are sampled upper bounds, not intrinsic commit/publication durations.
Every post-seed upsert image was eventually observed in published search
(203 filesystem, 147 GCS); delete latency was not assigned zero. History-table continuous-phase
catalog lag had median 2.6 s over 116 transactions locally and 94.4 s over only
eight GCS transactions. That phase must not be represented by the faster paced
setup commits.

The filesystem run verified exact final accepted SQL keys/versions, published row images,
restart visibility and post-vacuum visibility. GCS verified final accepted SQL,
published images and the initial post-restart accepted image. Its unfinished
maintenance phase prevented the final post-vacuum oracle and independent deletion
checks from running.

## Maintenance and object work

Neither provider reported physical deletions during the continuous phase. The
filesystem jobs subsequently completed after restart, reported 36 deletions, and
nine independently inspected expired manifest-list objects were absent. This
proves some physical retirement; it does not prove reclamation of all superseded
data/delete files, which current snapshots can still reference.

GCS reported one deletion but did not finish the original 300-second restart
recovery allowance. A separate continuation reopened the same configuration,
checked the acknowledged accepted image, and reused the remaining history job's
operation ID. That additional bounded attempt also remained incomplete after
21 acknowledged turns (about 550 seconds total). Its last result reported
33 expired snapshots and 102 remaining eligible snapshots. The saved job and
input/proof state were retained. This is not evidence of unsafe deletion or
irrecoverability; it does leave recovery throughput unqualified.

The mixed registration/write/search/maintenance interval before restart recorded
642,773 filesystem logical GETs and 361,458 logical PUTs; GCS recorded 25,934 and
12,092 respectively. These API-runtime counters exclude separately owned runtimes
and direct diagnostic reads. Categories can overlap nested/batch operations;
they are neither isolated publication amplification nor billable provider request
counts. Operation amplification needs its own phase-attributed measurement.

The GCS continuation initially listed 10,450 qualification objects totaling
112,777,114 bytes. The experiment used an existing bucket and export, created no
VMs or infrastructure, and sampled inventories remained below its
50,000-object / 1 GiB storage guards.
A storage envelope is not a billing measurement.

## Debug investigation and remaining work

Three Debug probes completed with exact row checks and no search failures:

- A faster-update, shallow-history probe: 711 successful searches per visibility.
- Original history depth, burst and pacing, with a 600-second continuous phase:
  264 per visibility; maximum successful request 345 ms.
- Eight concurrent readers, one-second updates and a 300-second phase: 5,108 per
  visibility; maximum successful request 411 ms.

These shorter/differently scheduled runs do not clear the original ReleaseFast
failures or qualify retention. No slow Debug request crossed the three-second
stack-capture threshold. A retained ReleaseFast slow-query profile accounted for
only milliseconds of its 2.8-second total in measured opening/ranking/delivery
phases; unlisted admission, WAL-cut setup and cleanup still need attribution.
These observations alone do not establish the cause of the original timeouts.

## Engine corrections and Debug recovery

The follow-up merges main's #1479 writer-admission changes and addresses four
independently established implementation problems:

- Standalone metadata callbacks checked cancellation before an unbounded mutex
  wait. Request-aware admission now checks cancellation/deadlines while queued
  and checks the deadline after acquisition. Cancellation callbacks run outside
  the metadata mutex because they may read the same catalog. HA
  mutation-barrier admission follows the same contract and retains the existing
  barrier-before-catalog ordering and role-binding checks. This bounds admission;
  it does not change the outcome contract of an already admitted durable commit.
- Reachability and retirement inserted URIs into immutable tries individually,
  persisting intermediate roots. Bounded batch insertion writes the final changed
  nodes bottom-up and preserves the existing canonical format. Checkpoints and
  proofs publish only after the nodes are durable. Retirement still requires an
  exact-metadata reachability proof and catalog HEAD CAS; cancellation cannot
  authorize deletion through an unpublished partial root.
  Vacuum traversal now batches its visited-set updates too. Parent frames stay
  in memory until checkpointing, which persists only the surviving parent chain
  before returning the cursor. The cursor format is unchanged; failed or
  cancelled publication cannot expose an incomplete closure.
- A reachability turn discarded its partial checkpoint if HEAD changed during
  traversal, so frequent appends could repeatedly erase useful work. An
  interleaving test reproduced the missing checkpoint. Progress now persists
  through its own CAS even after an append. Before retargeting a checkpoint for
  a different metadata document, the worker checks that its selected table is
  still current. This prevents a stale table read from rebasing newer work
  backwards; the checkpoint CAS also protects a competing worker's later save.
- Content-addressed trie reads issued a separate GCS metadata request before
  fetching the body. They now use the existing direct-read option, keeping the
  response-size limit and SHA-256 body check. A cold-reader regression rejects
  an object replaced with a syntactically valid empty node.

Request-local slow-query profiles now expose reader admission and native
catalog/WAL-cut preparation separately. Inline publications with no dense/sparse
segments also skip empty recent-vector preparation; directory-backed publications
still resolve their declarations. Incomplete qualification vacuum jobs resume at
one-second intervals, independently of the interval between new jobs per table.

These changes require a fresh optimized workload. They do not retroactively qualify the original measurements
or establish that all publication lag and accepted-search failures are resolved.

Focused validation passes: 45 Python tests, 65 shared runtime contract tests,
the standalone admission and public catalog cancellation tests, and 694 lake
tests (one additional test skipped), with no native test leaks. The checkpoint
interleaving regression failed with a missing checkpoint before the guard change
and passed afterward. The two focused lake API checks also passed.

At `b68a495444`, a fresh Debug engine build and eight-row functional smoke passed
through changes, highlighted search, exact rows and restart, with zero query
failures. That short run does not qualify retention or archive performance.
An additional Debug continuation of the retained GCS job remained incomplete
after 19 acknowledged turns and 540.47 seconds. It reported zero deletions; its
reachability checkpoint ended at snapshot cursor 73 of 104. The accepted row image
was checked before maintenance; post-maintenance row/deletion checks were not
reached. A Debug stack sample and object counters identified substantial remote
reachability/inventory work. The immutable-read probe removal was validated in a
later Debug continuation; it was not present in that attempt. ReleaseFast
requalification remains outstanding.

The continuation at `e2a67d7103`, with direct trie reads, also reached its bound:
41 acknowledged turns and 546.96 seconds, with 19 reported deletions and 76
retained objects. It checked 103 accepted row images before maintenance, but did
not reach its post-maintenance checks. The next measured amplification was the
traversal visited set and parent frames, which still wrote intermediate objects.
Their checkpoint batching passes 21 focused vacuum tests, including lost frontier
acknowledgments, cancellation, shared roots and cold recovery through a
512-level inventory graph. Logical operation counters include concurrent artifact
GC and must not be attributed entirely to this vacuum job.

At `e21dbbd624`, the same retained history job completed after 32 further turns,
at 253.77 seconds, reporting 14 additional deletions. Validation and shutdown
finished at 325.90 seconds. Accepted and published search both matched all 103
expected row images. Independent PyIceberg parsing checked current manifest sizes,
and PyArrow checked the sizes and readable footers of all referenced Parquet
files: 76 manifests / 274 files for history, and 18 manifests / 18 files for the
indexed table. A separate read-only check used six previous committed history
metadata documents to identify four expired history manifest lists; all four
returned 404. The continuation's own deletion check also found an absent expired
indexed-table list.

This establishes completion and data preservation for that retained job across
restarts and engine updates. Each continuation starts at a different traversal
position, so their durations are not a controlled speedup comparison. The job's
completion does not process snapshots that became eligible after its original
plan. It is not a fresh continuous-write or full-archive qualification. The next
section records the fresh optimized profiles separately.

## Fresh profiles at `e21dbbd624`

The optimized executable built successfully with the same ReleaseFast/stripped,
Metal/CUDA-disabled configuration. Its SHA-256 is
`f317f09206382b23a4b7f1a8ecf6d1a094fd867cb1f652bec24b406ff3c0254c`.
The harness was at `8534c3ea79`. A short eight-row smoke passed first; its ten
successful searches and restart checks do not qualify retention. Both fresh full
profiles ran on the contended developer host. Later Debug probes and unrelated
builds overlapped parts of them. No fixed-throughput or isolated speedup claim is
supported.

Successful-query latencies, in milliseconds:

| Provider / visibility | Successes | Median | p95 | Maximum |
|---|---:|---:|---:|---:|
| Filesystem / published | 318 | 31 | 772 | 4,274 |
| Filesystem / accepted | 317 | 64 | 236 | 3,400 |
| GCS / published | 141 | 1,345 | 2,249 | 5,295 |
| GCS / accepted | 139 | 1,935 | 2,474 | 5,016 |

Additional failures remain: **one 60-second filesystem accepted-search timeout
and two retryable GCS accepted-search HTTP 503 responses**, lasting 5.06 and
5.43 seconds. The public 503 reasons were still redacted to `http_error`; they
cannot be reconstructed from these records. The follow-up allowlist preserves
the public API's stable unavailable reason codes, without retaining provider
messages or unknown values. It does not retroactively identify these failures.

The filesystem run completed in 977.05 seconds and verified all 103 final row
images through publication, restart and maintenance. It reported one deletion
during writes and 40 overall; nine independently checked expired manifest lists
were absent. All checks except search availability passed. Initial published
search took 38.38 ms; post-restart accepted search took 71.66 ms. Indexed
publication lag was median 14.54 s / p95 48.16 s, with 30.84 s final catch-up.

The GCS run acknowledged 161 history and 184 indexed transactions and verified
103 final rows and the initial restart image. It reported two deletions during
writes but again exceeded the 300-second restart-maintenance allowance, ending
incomplete after 2,177.74 seconds. Final post-maintenance row checks and independent
deletion checks were not reached. Indexed publication lag remained median
205.56 s / p95 375.52 s; final catch-up took 214.46 s. Initial published search
took 4.50 s; post-restart accepted search took 9.83 s. This does not establish
acceptable GCS serving or maintenance performance.

The new Debug stress profile used eight readers, one-second updates and the full
retention window. It passed every check, with 2,753 successful queries per
visibility, no failures, 24 deletions during writes and exact preservation of
102 final rows. A separate Debug profile with the original pacing completed 330
queries per visibility without a failure, but did not finish maintenance within
its 300-second recovery allowance. Neither Debug result clears the optimized
timeout or qualifies an optimized performance comparison.

The local timeout occurred while other catalog reads and writes continued. A
different roughly 3-second local lake profile attributed 2.69 s to cursor
retention, with microsecond reader admission and millisecond WAL preparation.
The Debug stress profile similarly attributed 3.45 s of a 3.92-second request to
cursor retention. Those profiles explain those completed requests, not the
60-second failure. A Debug sample observed retained-capability single-flight
waits and scoped upload inventory work. A separate three-query GCS diagnostic
after writes stopped succeeded; it is outside the qualification counts and does
not reproduce or explain the 503s.

## Remaining maintenance and request attribution

A Debug continuation reopened the fresh GCS jobs, verified all 103 accepted rows
and kept the saved operation IDs. It still had a pending history job after an
additional 540.20 seconds, so its post-maintenance checks were not reached. A
symbolic sample spent substantial time fetching immutable completed-manifest
trie nodes during reachability marking. Existing per-turn RAM caching did not
survive the next worker turn.

The next change adds one replaceable, bounded 128 KiB trie-hint key per catalog.
Nodes retain their SHA-256 checks; missing, corrupt, stale or partial hints fall
back to required canonical reads. Hint bytes never establish a successful
upload, resolve a lost upload response or alter an exact-metadata proof, progress
CAS or retirement HEAD CAS. Optional cache I/O has a one-second sub-deadline and
progress is saved before warming. Provider retention of older object versions
is independent of this current-object size bound. Existing checkpoint formats
remain unchanged.

Additional diagnostics distinguish the internal causes mapped to query 503s,
time public-query binding and dispatch, and include cleanup in the enclosing
request total around the narrower lake profile.
Fixed-capacity live-stage tracking also exposes unfinished requests; saturation
drops diagnostics rather than queuing queries. The optimized measurements above
do not include these changes.

At `c1de2d750e`, the new Debug engine built successfully. Its SHA-256 is
`ff7b570c2dcd5c4c98d5acbe21b0f8f3486b87ce17080564a207743315b1bda1`.
The retained fresh GCS history job then completed after 41 further turns, at
344.15 seconds, reporting 28 additional deletions. Full validation and shutdown
took 414.01 seconds. Accepted and published queries preserved all 103 expected
rows. Independent current-source checks verified 78 manifests / 276 Parquet files
for history and 18 / 18 for the indexed table. A separate read-only check
authenticated the current metadata and walked 25 committed records, inspecting
nine distinct metadata documents; four expired history manifest lists returned
404. The stored hint object was 96,909 bytes, below its 128 KiB bound.

This proves completion and preservation for that retained job. Prior attempts
had advanced its cursor, so the durations do not establish a controlled speedup.
It does not qualify a fresh GCS run or clear the query failures above.

Traversal also revealed repeated long child lists in successive snapshot plans.
New plans group children into shared 16-entry pages; readers still accept the
previous 256-entry nodes and unchanged cursor format. A Debug control using 128
successive snapshot roots checks identical, duplicate-free object unions and
more than halves traversal steps for both append and newest-first orderings.
Existing published plans keep their original layout. New-plan provider and
optimized qualification remains required.

The final hint/diagnostic source passed 697 Debug lake tests (one skipped), four
focused API tests and 46 Python tests with 11 subtests. The plan-sharing change
passed all 22 focused Debug vacuum tests, including both control orderings, cold
resume, deep/shared graphs, cancellation and lost responses. Native tests
reported no leaks.

Next work should:

1. Attribute the remaining accepted-search failures with live public stages,
   request-scoped catalog/lease/WAL timings and unavailable-cause counters.
   Repeat the optimized workload under recorded host conditions before changing
   behavior based on an unproven cause.
2. Measure WAL-drain and publication queue fairness and construction phases under
   continuous small changes. Qualify batching/compaction and incremental serving
   without weakening accepted visibility or publication proofs.
3. Qualify fresh maintenance plans with shared pages and warm trie hints, including
   reachability, retirement and input-cleanup progress. Measure required object
   round trips and conditional deletion throughput before increasing concurrency.
4. Requalify in the deployment region through Antfly Cloud after the relevant
   engine release. These developer-host results do not establish Cloud readiness.

The follow-up harness retains request start/completion IDs, periodic and
pre-restart phase counters, a bounded credential helper and an independent engine
stop watchdog. Raw reports, logs, credentials and local investigation text files
are not repository artifacts. See the [runbook](../../../examples/hackernews/CONTINUOUS_LAKE.md)
for bounds and interpretation.

Baseline validation: both native engine builds succeeded; 44 relevant Python tests, Ruff
and documentation-link checks passed. Final ReleaseFast smoke tests passed
through changes, published/accepted search, row checks and restart. Raw full-run
failures remain qualification failures despite those successful checks.
