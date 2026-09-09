# Vector store correctness and design review — 2026-09-08

The subsequent [memory and bookkeeping investigation](../.benchmark-results/vector-structural-refined/README.md)
did not reproduce extra catalog heap or mapped-file retention. A WAL-membership
prototype passed lifecycle checks but did not establish an overall 50K win. The
current [deferred-inventory and recovery revision](../.benchmark-results/vector-structural-recovery/README.md)
instead avoids eager occurrence-map construction when a validated receipt already
provides physical totals. Its 37 source checks pass with receipts enabled.
Timeout investigation reproduced competing startup reconstruction while durable
generation repair waited for capacity. The ownership and posting-sequence fixes
pass five focused checks. Both five-case public API suites, saved-database
recovery/update/delete/repeated restart, and both controlled capacity-resume cases
now pass on the pinned release. The scale results below include a separate memory
failure; both default and explicit ANN settings use native storage.


The subsequent [source publication memory fix](../.benchmark-results/vector-source-memory-fix/README.md)
reproduces the suspected OOM as a source-budget rejection of a second WAL-sized
allocation during GC preparation. Full and selective GC now prepare readers and
inventory before publication, reuse the committed WAL suffix, and preserve the
current generation on pre-publication failure. Scratch admission and a
budget-derived WAL bound protect foreground progress; ambiguous durable writes
still require recovery. The controlled memory test now succeeds, and validation
covers allocation failures, old readers, retry, update/delete and repeated restart.
That revision passes fresh 50K ABBA and its lifecycle checks, but the second
1M candidate logs one maintenance OOM and is excluded. A same-binary diagnostic
identifies a separate 77,594,648-byte mark-map allocation with 332,745,026 live
bytes against the 402,653,184-byte source slice. The
[mark-workspace follow-up](../.benchmark-results/vector-source-memory-admission/README.md)
admits the map and source leases against the resident ANN snapshot before map
allocation. Rejected setup releases its temporary snapshots and retries later;
backing allocation and I/O failures still propagate. Its regression verifies
repeated deferral without retained memory, reclamation after pressure clears,
old readers and repeated reopen. Fresh qualification of this follow-up is
pending. Earlier failed arms remain excluded and no defaults are promoted.

The completed isolated deferred-inventory 50K ABBA passes all four timed and
reclamation arms and both independent full inventories, but it is not selected:
median paired mixed writes fall 25.6%, churn time rises 12.9%, and mixed physical
footprint rises 15.8% despite RSS falling 13.7%. The completed comparison under
`.benchmark-results/vector-structural-selected/` combines shared catalogs with
**eager** incremental inventory against the earlier locked baseline; lazy
inventory is disabled. It uses the same recovery-qualified binary and a separate
frozen harness, preserving prior measurements. Combined 50K passes all four
timed/reclamation arms and both independent inventory audits. Median paired mixed
writes improve 3.3%, churn time 2.1%, and mixed p99 4.0%; query and
physical-footprint directions vary by pair.

All four 1M attempts finished lifecycle checks, including both independent
candidate inventories, but **1M performance qualification is incomplete**.
The last control encountered three out-of-memory errors and source-store
poisoning during ingestion; the client retried four HTTP 500s and exited
successfully. That control is excluded, leaving three clean arms and only one
complete clean pair. In that pair mixed queries improve 7.5%, writes 7.4%, p99
13.6%, and fixed churn time 5.4%; mixed RSS rises 27.0% and physical footprint
6.4%. These are single-pair observations, not a repeated scale win or evidence
for promotion.

The benchmark now rejects write errors/retries even when the client exits zero,
and the 1M gate rechecks historical 50K logs. Four regression tests pass; an audit
of all twelve timed arms in this revision finds only the last 1M control affected.
Its failed-run data and original receipts are preserved. Source heap rose during
collection before the errors, but the failing allocation is not identified.
Next, reproduce memory admission/collection scratch pressure with allocation
evidence, fix it while preserving publication and recovery fences, then rerun
fresh 1M ABBA. No defaults change. See the
[full results](../.benchmark-results/vector-structural-selected/RESULTS.md) and
[memory-failure evidence](../.benchmark-results/vector-structural-selected/MEMORY_FAILURE.md).

Implementation follow-up: the first three opportunities below are now optional
experiments under `.benchmark-results/vector-structural/`. Its `README.md`,
`validation.json`, and `RESULTS.md` record implementation, correctness checks, and
performance qualification. The fourth (integrity/scrubbing) is not part of this
implementation. The remainder of this document records the initial review.

Qualification follow-up: 35 source checks in both configurations, 32 native checks,
five API lifecycle checks, all 12 timed 50K arms, and all 12 reclamation clones
passed. Two inventory candidates also passed independent full-inventory reopen
audits. Shared catalogs improved query/write/churn performance in both orders but
increased mixed RSS; incremental inventory reduced publication lock time while
increasing bookkeeping cost and reducing mixed query throughput. Independent
scanning showed no overall benefit. All switches remain experimental, with no
combined subset or new 1M qualification. The full
[results and next investigations](../.benchmark-results/vector-structural/RESULTS.md)
supersede the initial performance priorities below.

The ownership shape remains useful: primary transactions own artifact-version
references; a table-owned source store owns payloads independently of ANN indexes;
serving generations retain immutable source views. The latest experiments establish
reclamation progress and reduced reappend I/O, but no overall performance winner.
All new scheduling/reuse settings remain experimental. These comparisons used the
2 ms unlocked source-store baseline, not `primary_lsm`, and do not establish an
end-to-end win over primary storage or the earlier row-bounded baseline.

This is an initial code review and prioritization, not a completed lifecycle audit.
No new correctness defect was confirmed in this pass. No product code or tests
were changed or run for this review. The previous measured revision passed 61
storage checks, five API checks, 12 timed 50K arms, and 12 reclamation clones.
The subsequent planning-failure fix passed 30 source checks with ownership off
and on; its binary was not benchmarked again. See
[the measurement record](../.benchmark-results/vector-progress-dedup/RESULTS.md).

## Correctness boundaries to preserve and review further

| Boundary | Current mechanism inspected | Further review / proof needed |
|---|---|---|
| Artifact/model/version identity | `Reference.forArtifact` hashes the artifact key and full envelope; physical lookup is not keyed only by document ID. | Trace each producer's model/index/incarnation key and stale-completion fence through updates, deletion, recreation, and rebuild. |
| Prepare before primary commit | `Txn.commit` stages the reference epoch, prepares durable source payloads, then attempts primary commit; sessions fence unresolved outcomes. | Sweep injected failures through all allocation and durability boundaries, including rescue plus unrelated batch failure. |
| GC reachability | Primary snapshot, durable ANN references, post-cut tail, and rescued preparations participate in retention. Newly rescued owners invalidate the old complete receipt. | Stress concurrent mark/prepare/delete/cancel/publication and repeated reopen; verify abandoned preparations eventually disappear. |
| Historical readers | Read sessions are acquired before primary read transactions; ANN snapshots retain source blocks and WAL roots. | Audit table/index teardown order and lifetimes of borrowed directory/cache pointers, especially across serving replacement and the last-index drop. |
| Publication failure | Failed planning discards its consumed mark; ambiguous publication poisons the writer and preserves files for recovery. | Expand the single planning-allocation regression into failure sweeps across selected-segment planning, reader reopening, inventory, and receipt publication. |
| Deployment scope | Source open rejects unsupported backend/HA configurations; lifecycle operations have explicit gates. | Keep those gates until restore, movement, split, and HA ownership paths have their own proofs. Standalone results do not qualify those paths. |

The existing regressions are valuable evidence for these boundaries, not an
exhaustive proof of every interleaving. Prioritize failure cleanup and lifetime
review before introducing another ownership or publication mechanism.

## Larger design opportunities

### 1. Make active scanning independent of DB metadata maintenance

`runArtifactRepairMetadataMaintenancePass` scans outside the DB apply lock, then
acquires that lock, runs repair-metadata maintenance, refreshes ownership scopes,
and calls the collection step on every turn. `advanceMarkingLocked` immediately
returns when the unlocked scan is incomplete. Faster scan scheduling therefore
also schedules repeated visits to the outer lock and unrelated maintenance.

Give the collector explicit setup, scanning, ready-to-plan, copying, and publish
states. An incomplete scan should continue under its retained immutable views;
repair metadata should keep its own cadence. Acquire DB apply for the transitions
that need primary/catalog authority. Preserve cancellation and ownership-scope
invalidation, and bound foreground assistance under WAL pressure. This is the
first experiment to try because it removes a demonstrated coupling with a
relatively small change to the durability design.

### 2. Share immutable segment metadata across snapshots and WAL successors

`Opened.prepareWalSuccessor` already shares block contents and persistent WAL
nodes, but allocates/copies the block array, readers, reader order, and shard
offsets and retains every block. `Opened.clone` uses this same path. Each prepare
batch and snapshot can therefore pay work proportional to segment count even
when the immutable segment set has not changed.

Use a retained immutable segment catalog plus a retained WAL root. A WAL-only
successor can then share the catalog; a segment publication creates a successor
catalog. Preserve allocator/resource accounting and tie hint/cache lifetime to
an explicit owner. Measure copied metadata bytes, allocations, snapshot time,
and preparation time against segment count before claiming this is a dominant
cost. This is a concrete scaling opportunity, not yet a measured speedup.

### 3. Make selective publication update only affected inventory

`advanceCollectionLocked` calls `inventoryRetainedPayloads` after selective
publication. That function scans every reader and WAL entry into a new unique
digest map. Directory retirement also compares old and new reader sets. Selective
payload copying therefore still has global metadata work at publication, inside
the source critical section. Setup/publication dominate the measured lock stages.

Retain exact digest occurrence/location metadata and generation-bound per-segment
summaries, then apply additions and removals for changed segments. Duplicates mean
that subtracting segment counts alone is incorrect: removing one occurrence must
not remove a digest still present elsewhere. Prepare successor metadata outside
the publication lock and validate its generation before installation. The manifest
and durable primary ownership remain authoritative; summaries need a rebuild path
and cannot become a second independently committed ownership journal.

### 4. Separate reachability verification from full payload scrubbing

The exact all-live verification path uses `Opened.get`, reaching payload CRC
validation in `ValueLocation.valueFromPayload`. It touches vector bytes merely to
establish that each retained reference exists with the expected dimensions.

Explore a checked immutable metadata lookup for reachability, with payload
validation at clearly specified write/read/scrub boundaries. Do not silently
replace the current integrity guarantee with unchecked hints, or compare a weaker
durability/integrity policy as though it were the same workload. Count payload
bytes touched by verification first. This is a separate design decision from
the scheduler experiment.

## Proceeding

Finish the focused failure/lifetime audit, then test the independent scan pipeline
and shared segment catalog separately. Follow with incremental publication metadata
if finer profiling confirms the expected global work. A fully incremental owner
delta collector may eventually avoid repeated whole-table marks, but requires
primary/ANN reconciliation and restart proofs and should follow these smaller
structural changes.

Use fresh sequential 50K A/B and B/A on the available host; measure readiness,
fixed-count churn, query throughput/tails, memory, process I/O, reclamation,
apply-lock visits/waits, metadata allocations, and work versus segment count.
Keep integrity, encoding, recall, and durability constant. Qualify a selected
subset against the earlier row-bounded baseline before new 1M runs. Do not combine
several speculative changes into one treatment or promote based only on GC time.
