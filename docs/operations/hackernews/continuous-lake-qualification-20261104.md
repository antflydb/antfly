# Continuous writable-lake qualification — 2026-11-04 UTC

**Through `8bdf82339e`, three fresh full optimized filesystem runs passed; GCS had
no query failures in the latest runs but still failed bounded vacuum recovery.** Both the baseline and
first follow-up optimized runs failed qualification on each provider. The catalog-history
changes in #1480 work in this workload, and indexed acknowledged rows survived
searchable publication and restart. GCS maintenance completion remains a
release/deployment qualification gap; the later results below do not qualify
full-archive performance or Antfly Cloud deployment.

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

An additional Debug regression independently reproduced an accepted-overlay
correctness gap: retaining a published base and the current head while expiring
an intermediate snapshot leaves valid base WAL coverage but breaks the native
ancestry check with `LakeOverlayCoverageUnavailable`. That error is one of the
internal causes mapped to a retryable query 503; the recorded GCS failures cannot
be attributed to it because their precise codes were discarded.

Vacuum now protects the ancestry from the current head through every live
publication/reader base and partial-publication checkpoint before managed or
provider-delegated maintenance. WAL collection also honors checkpoint coverage.
Protection is computed once using indexed parent lookup, with deterministic,
deduplicated IDs. Missing/cyclic ancestry retains known snapshots conservatively,
including a missing main-chain parent that an older saved job might otherwise
retire. This prevents further damage but does not certify an already-broken
chain. Unrelated older history remains eligible when the required chain is
intact. This can extend retention while publications/readers need that ancestry;
accepted query validation and deletion proofs remain unchanged.

## Fresh optimized run after hints and ancestry protection

The frozen `14772abf19` ReleaseFast binary has SHA-256
`55059f0b7b4f59dd852b9cf69e4ec3cd6de3272f033af7f9479e10eeccbd73ca`.
Its local smoke passed, but its fresh full local profile remained **unqualified**:
one accepted search timed out after 60.00 seconds before response headers.
The run otherwise completed in 977.93 seconds with all row, publication, restart
and maintenance checks passing. Published searches had median / p95 / maximum
27.56 / 274.71 / 4,817.03 ms; successful accepted searches had
48.73 / 82.39 / 1,369.18 ms. The timeout is excluded from that successful-request
distribution. Publication lag was median 11.15 / p95 28.98 seconds, with 11.47
seconds of final catch-up. Maintenance reported one deletion during writes and
73 overall; 16 expired snapshot manifest lists were independently absent.

Live metrics captured the timed-out request inside public dispatch, with a
55.74-second admission age at client failure. The stack traversed result
hydration into the Parquet column coordinator's completion-event wait. Other
reads and writes continued. This locates the stalled path; it does not establish
the precise ordering that stranded it. A small reset-ordering control did not
reproduce a lost notification, so weak-memory reordering remains a hypothesis.

The follow-up replaces the resettable completion event with a generation
counter shared by the SQL scheduler's notified cohorts. Coordinators observe
the generation before inspecting readiness, then futex-wait only while that
generation is unchanged. Completion publishes readiness before advancing it.
The Parquet coordinator also bounds its park and checks the request context
before scanning and after waiting, then cancels/joins outstanding work on exit.
Tests cover coalesced notifications before parking, wraparound, expiry without
a notification, immediate worker cohorts and cancellation with parked siblings.
This is not yet evidence that the optimized timeout has been eliminated.

The concurrent GCS run reproduced five accepted 503s. New counters attribute
them to overlay coverage, rather than the other reader/cache/lease causes.
It also exceeded the 300-second restart-maintenance allowance, ending incomplete
at 2,140.87 seconds. Published searches had median / p95 / maximum
1,295.70 / 1,610.73 / 4,298.03 ms; successful accepted searches had
1,857.97 / 2,553.36 / 4,874.19 ms. Publication lag was median 203.33 / p95 353.54
seconds, with 199.80 seconds of final catch-up. Maintenance reported two
deletions during writes and three overall; the independent post-maintenance
deletion checks were not reached. All 102 expected row images were verified
before and after restart, but post-maintenance validation was not reached.
These are contended, cross-region developer
measurements, not production SLOs.

Investigation independently reproduced another coverage bug in Debug: 72 valid
WAL records, with catalog commit keeping pace at every append, could not replay
from an older searchable publication. The adapter applied the 64-record
catalog-admission backlog limit to publication replay as well. These are
different cuts: search publication can lag an otherwise healthy catalog.

Replay now retains the existing 32 MiB encoded/payload/descriptor budget and
derives its record guard from that budget. The uncommitted ingestion backlog
remains 64. Missing retained WAL coverage still fails closed. The regression
failed with `LakeOverlayCoverageUnavailable` before the change and passes after
it; the existing admission/backpressure controls also pass. A cached replay
test verifies normalization of the longer committed suffix to its final row
image. Fresh optimized qualification of these subsequent fixes remains required.
The shared SQL scheduling tests, all 59 object-WAL owner tests and 697 Debug
lake tests (one additional test skipped) passed without leaks. The SQL test
owner also needed its missing native runtime ABI import for shared memory
admission's named callbacks.

The production Debug engine at `de3921a563` built and passed its local HTTP
smoke with zero query failures. Its SHA-256 is
`3f39f3389fa197cfdf3666c7bd9083eaad9e180c646aea39f7ceda2065944567`.
A continuation of the retained GCS history operation completed after 53 further
turns, at 474.47 seconds; validation and shutdown took 543.50 seconds overall.
It reported 29 additional deletions and preserved all 102 expected accepted and
published row images. Independent checks read 98 current manifests and all 296
referenced Parquet files; two expired indexed-table manifest lists returned 404.
This proves recovery from that saved cursor, not a controlled speedup or a pass
of the earlier 300-second qualification allowance.

A Debug sample of that continuation found remote trie reads while testing
already-completed manifests during reachability marking. Read-only checkpoint
inspection found a 113,222-byte hint with 140 nodes but 65 missing nodes reachable
from the completed-manifest root. A cold control with 384 long manifest URIs and
a larger object-membership tree required 110 additional canonical GETs after
loading the previous hint.

The next change compresses hint JSON and prioritizes the first supplied root,
the completed-manifest tree. The stored object remains bounded at 128 KiB and
256 nodes; decoded JSON has a separate 1 MiB bound. All restored nodes retain
their canonical SHA-256 checks and are not treated as durable uploads. Legacy
plain JSON remains readable; older readers decline compressed optional hints
and use canonical reads. The cache key, canonical trie/checkpoint formats and
deletion authority are unchanged. Required reads remain the fallback for absent,
partial, stale, malformed or oversized hints. The control now needs zero extra
canonical GETs for the repeated manifest cohort. This is a native operation-count
result, not a measured GCS speedup.

Another cold control exposed 114 unnecessary provider operations when a hinted
root received a duplicate bulk insertion. Bulk insertion now returns unchanged
input roots without rewriting them, matching single-insert idempotence. That
control now needs zero provider operations. Newly introduced nodes still require
durable upload and lost-response verification. Tests retain the unpersisted-node
guard and cover legacy JSON, truncated compression, excessive decoded size,
excessive node count and bounded partial-cache fallback. All six hint tests and
700 Debug lake tests passed, with one additional skip and no leaks. Fresh
provider and optimized workload qualification of these changes remains required.

A later, heavier Debug job expired 108 snapshots and continued making deletion
progress, but remained pending after two bounded continuations. Samples located
additional cold reads in `Walker.enter`: the visited-set trie was batched but
did not reuse persisted hints across turns. Its traversal namespace now has its
own bounded hint key, loaded on resume and warmed only after the execution cursor
CAS succeeds. The cursor and canonical hash checks remain authoritative; absent
or invalid hints fall back to required reads. A 200-snapshot integration control
failed with a missing hint before this change and passes afterward, alongside
all 25 focused vacuum tests without leaks. Fresh optimized qualification remains
required; the larger job's durations are not a controlled comparison with the
earlier qualification workload.

The heavier retained operation eventually completed across six bounded Debug
attempts, with 308 reported deletions overall. Its final continuation at
`79bba11f0b` took 105.87 seconds and required four maintenance turns. Independent
validation preserved all 102 expected accepted and published row images, read
all 98 current manifests and 296 referenced Parquet files with valid lengths and
footers, and found six expired manifest lists absent. This establishes durable
continuation and current-source safety for that operation; its multiple bounded
attempts do not establish acceptable fresh-job throughput or a controlled speedup.

The final production Debug binary at `79bba11f0b` passed its local HTTP smoke
without query failures. Its SHA-256 is
`d5c74211131fbbba9a7a11f8bfbcc0333241bf1743c0b54b5ccdbb6628aee266`.
The matching stripped ReleaseFast build completed in 2,616.01 seconds and passed
its local HTTP smoke without query failures. Its SHA-256 is
`9d9fb43d5f4a7c91815f3a615809daa9661e2df5e32eb37163a861f505c2e207`.
Fresh full local and GCS runs used this frozen binary and unchanged qualification
bounds. The host was not isolated: the initial load
averages were 5.66/6.14/6.59, and both profiles run concurrently with developer
activity. These measurements must retain that qualification.

At that revision both full filesystem attempts passed every check in 918.53 and
934.02 seconds, with 1,486 continuous-phase queries and no failures. Each verified
102 final row images before and after restart/vacuum, reported physical deletion
during writes (11 and one objects), and independently found 16 expired manifest
lists absent. Reported total deletions were 67 and 71. Accepted-query medians were
49.84/49.83 ms, p95 values 79.75/68.28 ms, and maxima 207.74/1,435.11 ms. Published
medians were 25.92 ms in both, p95 70.59/51.45 ms and maxima 1,764.41/1,878.65 ms.
The earlier 60-second timeout did not recur; two passing attempts do not establish
its exact original trigger. Publication catch-up took 5.19/10.62 seconds.

The fresh GCS attempt still failed qualification after 2,171.34 seconds because
vacuum exceeded the unchanged 300-second restart-recovery allowance. All 284
continuous-phase queries succeeded. Published median/p95/maximum were
1,333.65/1,847.50/4,297.73 ms; accepted values were
1,957.73/2,526.61/4,931.29 ms. Publication catch-up took 225.99 seconds, and the
first accepted query after restart took 8.49 seconds. All 103 expected accepted
SQL and published row images were verified, including accepted visibility after
restart, but the post-vacuum oracle and independent deletion checks were not
reached. Two physical deletions were reported during continuous writes. Sampled
publication lag remained high: median 197.12 seconds, p95 399.95 seconds. Successful
accepted reads must not be represented as fast publication or fast cold serving.

A separate Debug continuation of that same GCS operation completed after 40
additional turns with 26 additional reported deletions. Validation and shutdown
took 374.93 seconds overall. It preserved all 103 accepted/published row images,
verified 94 current manifests and 292 referenced Parquet files, and found an
expired indexed-table manifest list absent. This proves recovery of that saved
operation, not a pass of the failed fresh-run bound. A read-only checkpoint sample
located unfinished reachability at snapshot 100 of 111; its manifest hint covered
all reachable completed-manifest nodes but stopped at 256 total nodes while using
only 34,160 of the allowed 131,072 stored bytes. Debug samples also located
sequential conditional PUTs in the visited-trie checkpoint. Further changes target
those bounded-cache and upload costs; their qualification remains outstanding.

The next change stages the final nodes of a bulk trie update, then uploads
independent immutable nodes using up to four optional parallel slots per process.
Each slot has a separate, caller-funded 1 MiB temporary buffer; buffer exhaustion
falls back to the caller lane after workers drain. Cancellation and failure drain
every worker, and a root cannot escape until all uploads are durably confirmed.
Conditional writes and byte-for-byte canonical verification still resolve lost
responses. Cache nodes staged before a failed upload remain unconfirmed.
Providers must explicitly advertise concurrent request support, and the owner
must opt in after synchronizing every client construction allocator in the
scope. Vacuum shares the existing reclamation-aware allocator wrapper across its
providers. Built-in GCS/S3 HTTP transports opt in; custom transports and memory
clients remain serial by default. Tests exercise both missing opt-ins and buffer
exhaustion during lost-response verification as well as during upload.

Hints now select up to 2,048 nodes within the existing 1 MiB decoded and 128 KiB
stored limits. Incompressible cohorts shrink deterministically until they fit;
missing nodes retain required-read fallback. A cold two-tree control reproduced
292 canonical GETs with the old node cap and now requires zero. Tests cover
concurrent overlap, lost acknowledgments, cancellation/draining and retry,
temporary-buffer fallback, excessive node/decoded size and incompressible input.
All ten focused retirement tests passed without leaks. These are operation-count
and safety controls, not yet a fresh optimized GCS qualification pass.
The full Debug lake suite passed 703 tests, with one additional skip and no leaks.
The object-store unit suite passed 210 tests with two live-provider tests skipped;
its local mock GCS HTTP test required loopback permission. The final focused
retirement controls also passed after exercising both concurrency opt-in gates.

At `8bdf82339e`, the Debug engine built in 568.26 seconds and passed its HTTP
smoke. Its SHA-256 is
`a9f8844b4d551cb53832e4eb06cf34b7a79e2ed5fe41d9d5ad29e96a0e42f34b`.
The stripped ReleaseFast engine built in 2,858.52 seconds and passed its smoke;
its SHA-256 is
`b88b56003bfe02322e701ff48d30dce35b0a6a1a076d72ca3fb8306c9f3863c1`.
The full filesystem run passed every check in 940.72 seconds with 738
continuous-phase queries and zero failures, 102 verified final row images, one
reported deletion during writes and 73 overall. Accepted median/p95/maximum were
47.74/85.63/1,438.12 ms; published values were 25.59/66.26/3,795.53 ms. Publication
catch-up took 8.37 seconds. These are updating-fixture observations, not a matched
attribution of the earlier static-archive outlier.

The fresh optimized GCS run at that revision remained unqualified after 2,210.17
seconds: all 284 continuous-phase queries succeeded and all 103 expected accepted
SQL/published row images survived restart, but the 31-snapshot history operation
exceeded 300 seconds of restart recovery. Published median/p95/maximum were
1,337.26/1,842.58/3,794.01 ms, and accepted values were
1,929.59/2,709.93/5,996.05 ms. Publication catch-up took 213.06 seconds; sampled
publication lag was median 212.06 seconds and p95 378.42 seconds. The driver
reported two deletions during writes and 26 overall, without reaching its final
post-vacuum oracle. No timeout, retention, work bound or oracle was relaxed.

A separate full Debug mixed workload at the same revision had zero query
failures but also exceeded restart recovery, for 37 expired snapshots. Its saved
operation subsequently completed in an additional bounded continuation;
validation and shutdown took 340.06 seconds. Independent checks preserved 102 row
images and verified 92 current manifests and 290 Parquet files. The separate,
larger 109-snapshot diagnostic still required multiple continuations and remained
pending; a validation-only attempt independently preserved all 103 row images
and read all 94 current manifests and 292 Parquet files. That validation is not
completion of the pending job. Real GCS stack samples confirmed the new parallel
upload path, but none of these controls clears the failed fresh optimized bound.

Further inspection found an unused metadata preflight in the body-only reader
for catalog-selected immutable Iceberg files. A provider-level control reproduced
two GCS requests per read; the reader now uses one bounded body GET. Mutable heads
retain revision-bearing catalog operations. Table scope and response-size bounds
remain enforced, and context checks preserve deadline expiry before and during
I/O. All 704 Debug lake tests passed, with one additional skip and no leaks.
Fresh engine/provider qualification of this subsequent change remains required.


Cold reachability bootstrap also spent its entire source allowance after four
small native Avro files. A regression with eight single-row snapshots failed to
finish despite spare mutation and byte budgets. Bootstrap now accounts for the
encoded bytes of uncompressed containers, with a 1 MiB minimum charge per file
and at most 64 files per turn. The total credit remains 64 MiB, each object stays
bounded to 16 MiB, and compressed containers retain the previous four-object
allowance because their encoded size does not bound decoded work. A source that
cannot fit the remainder yields without publishing an inventory or advancing its
cursor, then resumes with fresh credit. Mutation, edge, deadline and decoded-block
limits remain unchanged.

All 709 Debug lake tests passed, with one additional skip and no leaks. Controls
cover the formerly failing cold cohort, all four supported Avro codecs, truncated
and malformed headers, yielding and resuming without partial receipts, and
rejecting expanded budgets before I/O. This is a bounded-work regression fix;
fresh production-engine/provider qualification remains required.


The production Debug engine at `e7fc61d1d0` built in 608.12 seconds and passed
its bounded functional smoke. Its SHA-256 is
`eb5e51c88e1be4dce20efdb84b6edbd9400fda5d76b658724a9f887e0591ae82`.
A continuation of the saved 31-snapshot GCS operation completed at 155.08 seconds;
independent validation and shutdown finished at 229.14 seconds. All 103 expected
accepted/published row images survived, all 94 current manifests and 292 Parquet
files were verified, and two expired manifest lists independently returned 404.
This continuation does not clear the original failed fresh qualification.
The fresh full Debug GCS run subsequently failed after 1,558.12 seconds: the
history producer exceeded its unchanged 300-second catalog-coverage allowance,
with acknowledged LSN 160 while observed coverage remained 159. Its continuous
phase lasted 728.41 seconds with 244 successful searches and no search failures;
vacuum reported 29 deletions during writes. The indexed table had acknowledged
LSN 169 while observed catalog coverage was 136. This is a failed qualification,
not a passing recovery bound. A bounded restart diagnosis is checking coverage
and current source files; the matching ReleaseFast build remains in progress.

Concurrent inspection identified another retirement throughput problem: raw
candidates were limited by `min(16, max_deleted)` before reference and ownership
filtering. Retained files could therefore spend an entire turn's selection
allowance without using its traversal budget. A regression reproduced the old
behavior with 24 retained files before four expired native files and
`max_deleted: 1`: the turn examined only one retained file.

Selection now uses the existing 64-step traversal budget independently of the
physical deletion allowance. Catalog retirement stays bounded to 16 objects;
excess candidates are retained in a bounded execution checkpoint and rechecked
against current catalog authority before fencing. Physical deletion remains a
separate, durably approved phase with its original per-turn cap. New version-three
execution checkpoints read and migrate version-one and version-two progress;
older binaries cannot resume the new execution-checkpoint format.

All 26 focused Debug vacuum tests passed with no skips or leaks. This includes
the reproduced underfill, buffered overflow through cold one-deletion turns,
200-snapshot operation budgets, advancing authority, interrupted deletion and
legacy replay. The broader integration target also exposed a stale extra address
in a native-cursor configuration test after the repository began owning a config
pointer; its corrected contract test passed with no leaks. `zig build lake-vacuum-test -Doptimize=Debug` provides a focused
gate for these contracts. These concurrent changes were isolated from the frozen
`e7fc61d1d0` engine; its running provider qualification does not qualify the newer
candidate-buffer implementation.

Next work should:

1. Monitor accepted searches with live public stages,
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
