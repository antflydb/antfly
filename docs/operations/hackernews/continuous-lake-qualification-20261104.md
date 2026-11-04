# Continuous writable-lake qualification — 2026-11-04 UTC

**The full profile did not qualify on either provider.** The catalog-history
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

## Engine corrections under validation

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

These changes require a fresh optimized workload and continuation of the retained
GCS maintenance job. They do not retroactively qualify the original measurements
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
reachability/inventory work. The immutable-read probe removal is under separate
Debug validation; it was not present in that continuation. ReleaseFast
requalification remains outstanding.

Next work should:

1. Attribute accepted-search stalls with request-scoped catalog/lease admission,
   WAL-cut and cleanup timings, preserving failure/cancellation evidence. Repeat
   the optimized workload under recorded host conditions before changing behavior.
2. Measure WAL-drain and publication queue fairness and construction phases under
   continuous small changes. Qualify batching/compaction and incremental serving
   without weakening accepted visibility or publication proofs.
3. Drive incomplete maintenance separately from new-job scheduling and expose
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
