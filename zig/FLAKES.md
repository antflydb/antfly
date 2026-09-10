# Zig runtime flakes

See also the [E2E flake history](e2e/FLAKES.md). Record the original evidence,
reproduction conditions, deterministic regression, and before/after results;
a passing soak alone does not establish a failure's cause.

## Metadata request waiters advance election time outside the cadence driver (#694)

The 60-case diagnostic backup run on `00dd1e4aef` recorded 6,665 metadata
Raft ticks (666.5 seconds of virtual time) during a short-lived fixture. Ready
messages grew past the 1 GiB hard ceiling and quarantined the metadata group.
Mutation, lifecycle, and CDC lease waiters still called tick-bearing Raft rounds
at their 1 ms polling interval; the dedicated 100 ms cadence driver was therefore
not the sole clock owner. These waits now drain pending/inbound/Ready work using
the existing progress-only API. Explicit cadence and combined driver entry
points retain their ticks. Lease-acquisition helpers no longer execute unrelated
control rounds while waiting for their own proposal.

The HTTP regression covers pending replica synchronization, lease acquisition,
table deletion, CDC lease progress, and lifecycle progress after an explicit
single-node election. Each helper must make progress without moving virtual
Raft time. The old sync path failed with expected 2000 ms, observed 2100 ms
(`/private/tmp/ci694-cadence-before.log`). All 80 metadata service checks pass
without skips or leaks (`/private/tmp/ci694-cadence-fixed-final.log`). The outbound
ceiling remains unchanged;
Linux soak validation is still required before attributing every remaining
consensus failure to this defect.

## Cold repair completion evicts its newly resident writer (#694)

The `00dd1e4aef` Linux mixed soak passed 294/300; quickstart failed once
immediately after restart with `StorageReadTemporarilyUnavailable` during RAG.
Cold repair selected the normal writer cache and completed repair, but its
completion cleanup tested whether a writer existed at entry rather than the
actual admitted owner. It retired the newly installed serving DB under a
read-compatible operation and published completion with no resident writer.
Completion now uses `managed_owner_is_live_writer`, preserving both preexisting
and newly promoted resident writers. Isolated restore owners still retire.
The structural-repair regression forces every quantum through cold admission
and immediately leases the resident DB after completion. It failed before the
fix with `ResidentDbRetryRequired`; all 222 lifecycle checks passed after the
fix without skips or leaks (`/private/tmp/ci694-owner-lifecycle-final.log`).

## CDC scheduling authority loss and backup ambiguity transport (#694)

The same soak terminated a metadata follower with `NotLeader`. CDC scheduling
acquired its reconcile lease outside the control round's authority-error policy.
Expected authority loss now defers scheduling before any CDC work is enqueued;
the next tick rereads the lease. Corruption still propagates. The focused
regression verifies no enqueue on leadership/proposal/ambiguous lease outcomes,
then successful scheduling after recovery (1/1 passed).

A separate backup timeout logged an untransportable `BackupOutcomeAmbiguous`.
That outcome must reach the coordinator intact: only this classification
prevents rollback of work whose remote completion is unknown. Append its stable
runtime ABI detail and preserve its conflict class. The regression failed with
an internal classification before the fix; all 10 error-ABI tests pass afterward.
Neither change extends deadlines or authorizes retry of an ambiguous mutation.

## Backup repository lifetime and usable bounded native diagnostics (#694)

One 3x3 case completed its assertions but failed deleting its temporary backup
repository while the running cluster's reclaimer created
`.antfly-backup-reclaim-cursor.publish.lock`. The repository now lives below the
cluster fixture root and is removed only after every process stops. This also
preserves repository evidence on failure. DELETE failures include their response
body so a future conflict is attributable.

Default GDB symbol loading exhausted the 10-second capture budget on the 520 MB
release executable. `--readnever -q -nx` retains minimal symbols/native unwinding;
timeouts retain partial output. A live Linux attach completed in 0.29 seconds
with thread stacks. All 73 Python harness checks and pinned Ruff checks passed.
Fatal data apply conflicts now emit bounded group/index/term/type/digest evidence
without document payloads; the conflict guard remains strict.

These fixes are not full acceptance. The 100-per-scenario soak remains
99/100 quickstart, 95/100 backup/restore, 100/100 retry exhaustion. A subsequent
60-case diagnostic backup run on the old runtime passed 57/60 and exposed
metadata authority loss, outbound Ready growth/quarantine, and an ambiguous seed
write. The restore apply conflict and remaining request/consensus progress
failures are still being investigated. See `e2e/FLAKES.md` for artifact paths.

## Dense reset fixture bypasses the catalog lifetime barrier (#694)

[Run 34520726158, job 103018251656](https://github.com/antflydb/antfly/actions/runs/34520726158/job/103018251656?pr=694)
aborted on Linux x86_64 at `00dd1e4aef` during
`db chunked dense enrichment replays cached artifacts after dense reset without re-embedding`.
The native publication worker crashed retaining an HBC posting generation through
`denseProjectionCheckpointMetadata`; the unit process exited with code 134.

The fixture directly closed and reopened the dense index through
`IndexManager.resetDenseIndexForArtifactRebuild`, bypassing DB structural
admission. `runUntilIdle` drains replay but does not stop the independently owned
native publisher. That publisher pins the catalog without holding the apply
lock, so an unguarded reset can poison the HBC while it is still being read.

All three fixture callers now use a DB helper that acquires the existing
structural catalog barrier before apply exclusive. The low-level reset asserts
that catalog admission is closed and readers are drained. The original tests
retain their background workers and cached-artifact/re-embedding assertions.
A deterministic regression holds the same catalog pin as native publication,
verifies reset closes admission without completing, then releases the pin and
checks reset completes and admission reopens. Before the fix, reset completed
without the barrier and the regression failed with `TestUnexpectedResult`.
The regression and all three affected fixtures are included in the focused dense
lifecycle suite.

With the fix, all 81 focused dense lifecycle checks passed on macOS ARM64 without
skips or leaks. A separate 25-iteration run of the four reset regressions passed
100/100 test executions, with the original fixtures' background workers enabled.
Logs: `/private/tmp/pr694-reset-before.log`, `/private/tmp/pr694-reset-after.log`,
`/private/tmp/pr694-reset-soak.log`, and `/private/tmp/pr694-reset-dense-suite.log`.
The failing CI run is Linux evidence; these local passes do not replace the
Linux rerun of the fixed commit.

## Completed CLI index readiness regresses after a delayed notification (#696, #694)

[PR #696, run 34428885099, job 102726530711](https://github.com/antflydb/antfly/actions/runs/34428885099/job/102726530711?pr=696)
failed `test_cli_inline_create_load_wait_query_image_and_rag_pipeline` after
`index get` reported complete readiness. A subsequent `index list` retained
the thumbnail index's target/published revision 6 and all three coverage
outcomes, but regressed `source_coverage.observation_complete` to false.
The same job also failed the backup seed batch with HTTP 409, the forwarding
failure described below. The serverless filesystem GET regression did not
recur, and the Autograph case passed.

The runtime owner can sample an accepted source target before that write's
deferred publication callback reaches the status cache. The cache previously
deduplicated callbacks only against earlier notifications. With no notification
watermark yet, even an already-observed revision minted a new observation
fence and revoked completion. A deterministic cache regression reproduces
this ordering without sleeps: publish complete target 6, deliver notifications
for 6 and 5, and inspect both list and detail snapshots.

The cache now also recognizes a completed observation of the notified source
revision in the current table epoch. Each accepted group publication records
its cache epoch; a retained snapshot from before a catalog fence cannot
acknowledge a notification in the new epoch. Newer source revisions,
unsequenced structural invalidations, and owner retirement still fence
completion. Cached and synthetic publications cannot establish this proof.
A separate regression checks the same revision across a catalog fence,
including republishing retained facts in the new epoch. No readiness
assertions or production deadlines were relaxed.

The ordering regression fails before the fix. With the fix, all 173
`antfly-api-derived-coverage-test` tests pass on macOS ARM64 without leaks.
Nine focused publication-fence and owner-lifecycle tests also passed.

The Linux CI executable from `3ab5f6aba` still reproduced this signature in
1/30 CLI runs. The exact-index notification path independently had the same
ordering gap. A second deterministic regression publishes source/index target
6 with applied sequence 6, then delivers that index's target-6 callback. It
reproduces the false list/detail observation even after the group-level fix.
Exact-index callbacks now preserve completion only when an authoritative
observation in the current epoch has both observed and applied the target.
They still record the source and reducing watermarks, preserving deletion
authority for future merges; a newer target still fences completion.
All 174 derived-coverage tests passed before merging the native storage
implementation, including callback permutations and deletion/replacement cases.
Retry exhaustion now has its own independently seeded CLI E2E test, retaining
the production backoff and degraded/restart assertions. The quickstart keeps
its readiness, maintenance-cycle, query, and restart checks and takes about
13 seconds instead of 77 seconds in the first Linux split validation.
Linux reproduction and soak results are recorded in the
[E2E history](e2e/FLAKES.md#completed-cli-readiness-regresses-after-publication-696).

## Coverage reads straddle the first atomic outcome commit (#694)

The `70e0b11869` Linux soak failed the retry test's initial healthy seed once,
before provider failures were enabled. The derived worker reported
`InvalidDerivedCoverageCounter` at `target_advance`. Later diagnostics showed
one posting boundary at 4 and the shared vector generation at 5, leaving
initial readiness pending. The shared base contained two artifact families;
its total count of two is not evidence of duplicate vectors in either index.

The producer commits produced/skipped/terminal-failed counters atomically, but
target validation read each key through a separate latest-value lookup. A
first commit between those reads can return a missing first counter and present
later counters, manufacturing a partial tuple that fails the worker. Outcome
transitions and source deletion can similarly mix incompatible cardinalities.

Target validation, repair certification, query admission, and status tuple
reads now capture a consistent batch of five keys: all three outcomes, the
range-local source count, and the legacy artifact counter. The LSM backend
captures mutable hits and pins the immutable source layout under its short
backend lock, then performs any disk reads outside it. An explicit capability
keeps backends with only per-key probe reads on their ordinary snapshot path.
General MVCC snapshots can clone the mutable table; they are not used on the
normal LSM counter path. Single-counter reads retain their existing point
probes. Legacy missing range counters retain their scan fallback, re-reading
the whole proof in one snapshot. Persisted partial tuples and counter overflow
remain errors, and current-generation outcomes still supersede an invalid
legacy artifact counter when that fallback is not needed.

The regression fails with `InvalidDerivedCoverageCounter` when the old read
behavior is restored in an isolated worktree. The fixed reader captures the
whole batch before a forced commit: that call observes the old empty state and
the next sees the complete new state. Removing a persisted counter still
errors. A storage regression retains captured values across overwrites,
deletions, and insertion of a previously missing key and verifies zero mutable
snapshot clones. The focused dense lifecycle suite passes 77 checks without
skips or leaks; the storage suite passes 737 checks with one intentionally
inactive cross-process child helper skipped and no failures or leaks. Its
standalone build target also declares the PDF dependency already required by
the shared background runtime. The posting/vector lag is an observed
consequence, not independent proof that every native publication stall has
this cause; Linux acceptance remains required.

## Online vector-publication test counts unrelated posting progress (#694)

The dense lifecycle suite intermittently expected zero progress from a second
online vector-publication call but received one. A previously queued posting
checkpoint completed between calls; the API's aggregate count includes that
valid handoff. The test now compares the exact-vector manifest generation and
its storage sync count, retaining the primary-scan hook assertion. Foreground
and posting-mutation admission must still remain open inside vector staging.
This tests the no-rebuild contract directly without assuming checkpoint timing.
The corrected combined run passed all 77 dense lifecycle checks and 737 storage
checks (one inactive child helper skipped), with no failures or leaks.

## Partial-source replay and stale follower restore-job observations (#694)

The `70e0b11869` Linux soak also reproduced an independently seeded retry case
with one covered, one terminal-failed, and one pending source. The healthy native
generation contained one searchable vector, but its replay watermark remained at
5 behind target 12. The target-proof callback required every source to have a
terminal outcome, even for replay records that could not produce an embedding.
It repeatedly logged an unavailable artifact counter and prevented projection
publication while the test deliberately held the later provider request.

Replay now distinguishes the count of materialized embeddings from completion
of all source work. Generation-scoped outcome tuples still fence the cardinality,
and replay must prove there are no unapplied applicable artifact records.
Already-clean projection maintenance uses that materialized target; initial
build, rebuilding-checkpoint finalization, and shadow cutover retain the strict
all-sources proof. The regression leaves a real source pending, verifies that
strict coverage remains unavailable, and permits advancing the certified index
across its source-only replay tail. Existing overflow, incomplete-coverage, and
same-name-generation tests retain their fail-closed behavior.

A separate restore poll timed out after 120 seconds because job-detail GETs could
serve a present but stale follower row. Decoding the retained native SSTs with
the production table decoder showed job `4153919785164652446` succeeded on
metadata-1 and metadata-3 at `1789063384173` ms, **15.403 seconds** after creation.
Metadata-2 retained its running attempt-1 state at `1789063369536` ms. Public
job-detail reads now require leader authority, matching list and mutation
operations, and persistence reads require a linearizable fence. A follower with
an existing row returns the retryable metadata-not-leader response instead of
hiding a completed job. The regression covers both absent and present follower
rows. This fixes the stale-read contract; the captured SSTs alone do not explain
the follower's replication lag.

The focused runs passed 76 dense lifecycle checks and the metadata server
regression without leaks. Fixed Linux acceptance remains pending; neither the
failed baseline nor partial counts are accepted as 100/100.

## Slow Raft sync kills the runtime; targeted activation joins sibling work (#694)

The Linux soak of `70e0b11869` exposed two additional signatures, separate from
late source notification and catalog-admission coverage lag. A quickstart
could see the new thumbnail incarnation in the catalog without any owning-group
runtime observation within its five-second activation deadline. Two retained
standalone logs showed structural reconciliation pending during dense projection
finalization. The cached-writer path started its replacement enrichment runtime
before catalog mutation, forcing a second cancellation/join; target reconciliation
also synced every index, including unrelated storage maintenance.

Cached-writer reconciliation now installs the replacement producer paused,
reconciles durable index admission, then starts it and publishes the matching
configuration fingerprint. Targeted reconciliation syncs only its installed
index; deletion does not sync surviving siblings. The lifecycle regression
checks the paused admission boundary, and another regression holds a sibling's
actual LSM storage lock while creating and dropping the target index.

Two metadata processes exited with `RaftProgressDriverStalled` after successful
WAL syncs of 6.315 and 6.495 seconds. The five-second progress watchdog was used
both for readiness and terminal supervision. Supervision now observes only
actual source errors; elapsed round duration continues to make readiness false.
Its failure-event wait retains the full control cadence, preventing a hot loop
while a round is stalled. Both metadata and data runtimes use this separation.
No durability acknowledgement moves ahead of sync. Raft's existing
`async_storage_writes` flag still calls the persistence hook inline, so enabling
that flag alone would not remove the I/O wait.

A deterministic blocked-round regression verifies unhealthy readiness, continued
nonfatal supervision with a real timed wait, and readiness recovery on the same
driver after release. Actual source errors still fail immediately. The focused
Raft run passed all 12 checks; the final activation lifecycle run passed all
222 checks, including both new regressions, without leaks. Fixed Linux acceptance
is pending. Backup failure teardown now captures bounded native thread stacks
through the existing opt-in disposable-child launcher; the shared helper is also
used by scaling tests. All 71 affected Python harness checks passed, and debugger
attachment was verified in the Linux runner without changing host ptrace policy.

The same diagnostic soak also recorded a seed batch with unknown write outcome
and contended Raft state, a restore job remaining nonterminal for 120 seconds,
and a partial-coverage index whose replay target stalled while a later source
was pending. Their cluster states and logs are retained separately; these
changes do not yet establish that those causes are resolved. Failed soak executions
remain baseline evidence and are not included in final acceptance counts.

## Initial catalog admission quarantines a healthy generation on shadow coverage lag (#694)

Splitting CLI retry exhaustion from the quickstart exposed an additional
availability failure on the native-storage merge. The separately seeded retry
test retained one covered image, one terminal source failure, and one pending
image, but reported `pending` / `queryable=false` instead of
`queryable_partial`. Three of four independent Linux executions failed; the
combined quickstart/retry execution passed. These exploratory results are not
part of final acceptance.

The retained `index_repair.checkpoint` bound the failure to the thumbnail's
catalog-admission initial build: trigger 10, work class 2, terminal phase 10,
`RepairSourceCoverageIncomplete`, with an inactive shadow and no activated
pointer. The generic repair error classification treated an incomplete shadow
as permanent failure for catalog admission even though the existing canonical
generation had a certified publication. It already treated this condition as
recoverable for replacement/artifact repair.

Catalog admission now uses the same recoverable coverage classification for
both new failures and terminal checkpoints persisted by older binaries. The
owner can discard only its inactive incomplete candidate and resume normal
convergence; structurally invalid generations remain gated. Initial admission
does not re-arm terminal provider outcomes. A durable-state regression failed
before this change and passes after it, proving recovery to a searchable
canonical generation and retirement of completed admission debt. All 75 dense
lifecycle checks passed without leaks. Linux validation is pending the rebuilt
executable.

## Restore completion owns progress retirement (#694)

The original Linux baseline also exposed a completed restore whose progress
did not disappear within 30 seconds. Inspection of retained metadata copies
showed two replicas with cleared intents/progress and a third with the earlier
restore state. That observation does not independently establish why the third
replica lagged. A subsequent `3ab5f6aba` Linux soak passed all 30 backup cases.

Apply-time review found a separate lifecycle gap: completing a restore cleared
the intent but left progress retirement to data-node refreshes. A delayed
progress command could recreate a completed restore's row or overwrite a
replacement restore's progress. Progress-only updates do not advance the
catalog epoch, so catalog refresh is not a reliable cleanup trigger.

Metadata now retires a range's progress in the same transaction that completes
its exact restore intent. Cleanup is scoped to that table and range and does
not depend on the reporting node remaining alive. Progress upserts validate
the active table/range and full reported restore identity at apply time.
The completion command also declares its restore-progress snapshot projection.
Legacy removal commands contain no restore incarnation, so they may retire
only orphaned progress; they cannot delete an active replacement's observation.

A committed-entry regression fails before the fix with `expected 0, found 1`
after completion. It verifies atomic retirement, delayed-report rejection,
replacement identity, stale completion/removal, and preservation of a sibling range.
All 80 metadata storage tests passed, followed by the strengthened sibling
regression. Backup failure diagnostics now retain per-replica restore state
and preserve request-timeout details without retrying ambiguous requests.

## Metadata mutation discovery exhausts admission time (#694)

The first forwarding-fix soak passed 59/60 backup runs. The remaining run never
reached a write: five explicit pre-admission `503 metadata_leader_unavailable`
responses exhausted the existing 30-second create budget.

Two concrete discovery defects were reproduced:

- A status GET used the entire five-second mutation budget. A stalled first
  endpoint prevented discovery from reaching a healthy configured leader, and
  each public retry started over at that same endpoint. Discovery now divides
  remaining time among the remaining probes and reserves a share for mutation
  delivery. The initial endpoint preference is captured once, so concurrent
  affinity updates cannot skip endpoints. Discovery still consumes no forwarding
  hops; delivered mutations retain the original hop, campaign, deadline, and
  ambiguity rules.
- `MetadataHttpClient.fetchStatus` returned a role string borrowed from an HTTP
  body it had already freed. Leader discovery could read corrupted bytes and
  classify the leader as a follower. Role stabilization now occurs inside the
  HTTP client before releasing either response or parser memory. Recognized
  roles use static strings, unknown roles become `unknown`, and temporary parser
  allocations (including escaped JSON strings) are released.

The virtual-clock regression fails before the probe-budget change and passes
with it. It covers a slow first endpoint, all endpoints timing out, an
overshooting executor, changing affinity, and preserved delivery authority.
The response-lifetime regression explicitly overwrites released response
storage: it reads `######` instead of `leader` before the fix and succeeds
afterward, including escaped strings without leaks. All 100 metadata service
checks and four focused data discovery/status checks passed.

A live HTTP proxy also reproduced the original create-admission signature
before the fix and completed the full backup/restore test afterward. The
retained regression keeps a stalled alternate status route first and all three
direct metadata addresses available, so leadership changes cannot accidentally
hide the only leader route. Fault activation follows cluster bootstrap.
See the [E2E history](e2e/FLAKES.md#table-create-admission-timeout-during-694-validation)
for the final merged-runtime soak and the limits of the exploratory evidence.

## Synchronous resolver retry returns before queued backfill applies (#694)

[PR #694, run 34432995411, job 102732550441](https://github.com/antflydb/antfly/actions/runs/34432995411/job/102732550441)
failed `db drains pending resolver backfill when retrying a no-op upsertResolver`
with `NotFound` when reading the expected resolution artifact.

The resolver worker can enqueue the final corpus window and clear its durable
cursor before replay materializes the output. A synchronous no-op upsert checked
only that cursor, so it could return with replay still pending. A synchronous
backfill driver could likewise see a completed window with zero records queued
by that driver and skip its final replay drain.

Synchronous upserts now drain replay even when the corpus cursor is already
empty, and synchronous backfill always completes a final replay drain. Managed
`drain_backfill=false` callers retain asynchronous execution and nonblocking
catalog admission. No production timeout or test assertion is relaxed.

The regression preserves the original worker-enabled case and adds a controlled
interleaving: disable resolver workers, enqueue all corpus windows, verify the
cursor is gone but target sequence exceeds applied sequence and the output is
absent, then retry the same catalog config. It fails with the exact CI
`NotFound` before the fix and passes afterward. All five focused catalog,
generation-change, deferred-reopen, and backfill checks passed without leaks in
`zig build antfly-resolver-backfill-test -j4`.

## Standalone routing watch deadline (#689, #694)

[PR #689, run 34418061842, job 102687782332](https://github.com/antflydb/antfly/actions/runs/34418061842/job/102687782332)
failed `standalone routing watch does not report absence after one probe` with
`expected .authoritative_absence, found .retry`. The real-clock test used a 60 ms
deadline; scheduling delay can exhaust the confirmation budget, so the
deadline-aware mutex correctly returns a retry instead of certifying absence.

[PR #694](https://github.com/antflydb/antfly/pull/694) accepts that retry only after
deadline expiry and retains the minimum-wait and unexpected-change assertions.
The watch and mutex deadline checks share an injectable monotonic clock. Manual
time requires successful confirmation before expiry and checks an overshooting
sleep, an already-expired caller budget, and mutex contention. Production keeps
the same monotonic time, sleep, and yield operations.

Validation on macOS ARM64: all 108 standalone runtime tests passed without
leaks. Setting the confirmation budget to zero temporarily made the new test
fail with the original expected/actual mismatch; the mutation was reverted.
Both routing-watch tests passed in #694's subsequent Linux x86_64 CI run
34423487352. That run failed in the unrelated dense rollback test below.

## Dense generation rollback activation budget (#694, #695)

[PR #694, run 34423487352, job 102704099568](https://github.com/antflydb/antfly/actions/runs/34423487352/job/102704099568)
failed `db failed activated dense generation rolls back to retained predecessor`.
The second repair returned `failed=1`, `unresolved=1`, and remaining debt instead
of reaching the injected `TestCrashBeforeReplacementValidation` error.

The functional rollback test used the 250 ms production activation budget.
A temporary 350 ms pause after persisting the activating phase reproduced the
CI signature on macOS ARM64. [PR #695](https://github.com/antflydb/antfly/pull/695),
commit `d36b3492d`, uses the existing five-second functional-test options for the
predecessor build, crash-injected replacement, and final recovery rebuild. It
also asserts that the predecessor completed successfully before testing
rollback. The production limit and crash, rollback, and search assertions stay
in place. The fix is also included in #694.

The injected-pause case passed after the fix. After removing the temporary hook,
all 68 tests in `zig build dense-index-lifecycle-regression-test -j4` passed
without leaks. Formatting and diff checks passed. The Linux x86_64 unit job on
#695 also passed ([run 34427566807, job 102716322007](https://github.com/antflydb/antfly/actions/runs/34427566807/job/102716322007)).
The same 68 tests passed again after inclusion in #694 with current main.

## Backup seed forwarding exhausts the control executor (#694)

[PR #694, run 34423487352, job 102714559943](https://github.com/antflydb/antfly/actions/runs/34423487352/job/102714559943)
failed the three-by-three backup E2E case while seeding its three documents:
HTTP 409 `write outcome unknown`. The artifact contained only the executable,
so the original CI log does not identify the underlying transport error.

On macOS ARM64, a native Debug build based on main `9f192f9be` reproduced the
same seed failure in **1/30 concurrent runs**. Adding error-path diagnostics
then reproduced it in **7/60 runs**, all with `ConcurrencyUnavailable` inside
the forwarded group batch HTTP client. The client conservatively translates
transport errors after its send boundary into an ambiguous write outcome;
the test correctly fails instead of blindly replaying the batch.

Both known-leader and placement-fallback forwarding used `dataRaftIo()`, which
selects the runtime's eight-worker control executor. HTTP forwarding submits
nested request, deadline, and connection tasks. Concurrent transaction
participants compete with control work for those eight slots, and admission
failure can occur after a request starts. Forwarding now uses the existing
bounded outbound Raft network executor. Control waits retain their executor,
and borrowed runtimes retain their caller-supplied transport authority. Neither
the aggregate worker budget nor the write deadline is increased.

The deterministic regression occupies every control slot, verifies that one
more task is rejected, then sends a real forwarded batch to a loopback peer.
With the old executor it fails with `RaftBatchWriteTransportOutcomeUnknown`
and underlying `ConcurrencyUnavailable`; with the fix it receives HTTP 201 and
verifies exactly one request. The borrowed-VoprIo and forwarding-error
classification regressions also pass. Diagnostics now retain the underlying
HTTP error, delivery phase, and timeout without logging request bodies.
The broader HTTP client regression had three stale expectations for transport
failures after #692 introduced the distinct internal error; they now require
`RaftBatchWriteTransportOutcomeUnknown`. Peer-reported ambiguity still requires
`RaftBatchWriteOutcomeUnknown`, and definite pre-send failure remains distinct.

The E2E fixture also declares its process resource explicitly: before the fix,
pytest collection classified this six-process cluster as `light`; afterward it
uses `antfly-process`. See the [E2E entry](e2e/FLAKES.md#three-by-three-backup-seed-batch-unknown-outcome-694)
for final soak results and remaining CI validation.

## Serverless build-status PreconditionFailed during publication (#692)

[CI run 34420585088, job 102704104941](https://github.com/antflydb/antfly/actions/runs/34420585088/job/102704104941?pr=692)
failed `test_index_lifecycle.py::test_serverless_named_embedding_indexes_report_publication_actions`
while polling build status after deleting `semantic_a` and republishing the
remaining named embedding indexes. `/build-status` returned HTTP 500, and the
server logged `table build status failed ... err=PreconditionFailed`.
The job finished with 398 passed, five skipped, and this one failure. The
Autograph test passed; this is separate from the resolver/Raft cycle below.

### Cause and fix

Filesystem object publication writes a complete staged object and atomically
renames it over the destination. `FilesystemClient.getObject` previously opened
the path for metadata, closed it, then reopened it to read the payload. If a
publisher replaced the path between those opens, the second header had a new
ETag and the reader returned `PreconditionFailed` even for an unconditional GET.
Concurrent deletion could similarly produce `FileNotFound` after metadata had
already been selected. Build-status reads mutable progress/HEAD objects through
this backend, making active publication a trigger for the HTTP failure. The CI
log does not identify the particular object key that raced.

GET now keeps one file descriptor open for metadata, conditional ETag checks,
range/part selection, and payload reads. Atomic replacement or unlink leaves
that selected generation readable through the open descriptor. A later GET
observes the replacement or deletion. Explicit stale `If-Match` requests still
fail; response-size limits and cancellation remain in force. No HTTP retries,
longer polling deadlines, or publication locks are added.

### Deterministic regression and validation

The regression publishes or deletes the object at the existing cancellation
checkpoint after metadata selection and before payload reading, without
canceling the read. On unmodified `origin/main` at
`9f192f9be68219d08944d51552dbf4eb782891f1`, it fails with `PreconditionFailed`
from the second open in `readObjectRangeAlloc`. With the fix, all 12 cases pass:
shorter replacement, longer replacement, and deletion, each during full, range,
part, and matching conditional GETs. Assertions check the original body, size,
ETag, checksum metadata, and content type, plus subsequent reads and stale ETags.

The standalone object-store suite passes in Debug and ReleaseFast: **69 passed,
two opt-in cloud integration tests skipped** in each mode. The root
`lib-objectstore-test` target also passes with the same counts; the suite is now
included in the default `lib-test` CI aggregate. The focused serverless manifest
suite passes **12/12**.

The macOS arm64 ReleaseFast executable (SHA-256
`fa2c790e57cea2cc35cc449ce01839e146eb781795b9d525e043f2af74cf32bd`)
passes **30/30** repetitions of the failing E2E test with three concurrent
workers. All **11/11** serverless index-lifecycle cases also pass. The first
sandboxed launch could not bind a localhost port; these results are from the
successful rerun with local-server access. The HTTP flake was not reproduced
in a baseline E2E soak; the before/after evidence is the deterministic backend
regression, and the passing E2E soak validates the integrated fix.

```sh
SKIP_BUILD=1 ANTFLY_E2E_ENV_LOADED=1 \
  ANTFLY_E2E_REGRESSION_WORKERS=3 ANTFLY_E2E_REGRESSION_REPEATS=10 \
  scripts/ci/zig-e2e-regression-loop.sh \
  e2e/antfly/test_index_lifecycle.py::test_serverless_named_embedding_indexes_report_publication_actions
ANTFLY_E2E_WORKERS=1 scripts/ci/zig-antfly-e2e-pytest.sh \
  e2e/antfly/test_index_lifecycle.py -k serverless
```

## Autograph second-document write timeout (#690)

[CI run 34395199129, job 102623777993](https://github.com/antflydb/antfly/actions/runs/34395199129/job/102623777993?pr=690)
failed the first of three low-descriptor repetitions of
`test_resolution.py::test_multinode_autograph_resolves_promotes_and_hydrates_entities`.
The `doc:b` batch timed out after 30 seconds. Data node 101 reported
`target=4 applied=3`, `diagnostics=raft_mutex_contended`, and an unknown outcome
after proposal. Resolution also reported `ReadIndexTimeout`. GDB attachment was
denied by the runner's ptrace policy, leaving no usable native stacks.

The worktree starts at `origin/main` commit `26e332ed8c335d75eecd875548c6c36bcb47d183`.
The original ReleaseFast CI executable reproduces the same `doc:b` timeout and
Raft diagnostics under concurrent Linux load: **5 failures in 30 runs**. Three
serial Linux runs passed. The reproduction uses the runner image, a 50 GiB
`standard-rwo` disk, eight-CPU affinity, and `RLIMIT_NOFILE=256`. A disposable
test launcher enables same-UID GDB attachment; the original artifact is stripped,
so those stacks contain addresses without Antfly symbols.

### Cause and fix

A symbolized, unmodified `main` ReleaseFast build reproduced the failure in
**1 of 30 runs**. The native stacks established this cycle:

1. `runProvisionedRootRefresh` holds a group refresh activity and calls
   `reconcileReplicaRootTablesWithWriteCache` → `backfillResolverCorpus`.
2. Resolver catch-up calls the distributed candidate source, whose cross-shard
   scan waits for `data_raft_mutex` to submit ReadIndex.
3. The Raft progress thread holds `data_raft_mutex` in `applyReady` and waits
   inside `beginReplicatedApplyOperationLocked` for that same refresh activity.

Managed metadata refresh now persists resolver configuration and durable backfill
cursors without running resolution or promotion inline. The resolution worker
owns bounded cursor scans and replay, wakes for catalog-only changes, and reloads
pending cursors after reopen. Cached cold opens defer resolver workers until the
stable cache entry has its distributed candidate source, entity sink, and
promotion-owner callbacks installed.

Catalog changes use nonblocking resolution/promotion callback fences while a
managed refresh owns a group activity. A busy worker causes the refresh to yield
and retry. Material configuration changes atomically reset both durable cursors
with the catalog, so a previous scan cannot clear a new generation's work.
Promotion checks the live resolver generation under the same callback fence and
skips queued artifacts from superseded configurations.
Removal retains the catalog until artifact retirement and graph replay finish;
retries do not enqueue deletes for already absent artifacts. Follower retirement
fences callbacks without waiting for leader-only promotion work. Embedded callers
retain their synchronous resolver API.

Committed Raft apply also tries the read-compatible group activity once, including
nonblocking acquisition of its bookkeeping mutex. Contention returns
`RaftApplyWriterUnavailable` before document mutation or cache invalidation. The
state-machine checkpoint preserves exactly-once document effects across retries.
The original nonblocking fix still returned from the entire host drain on this
error: a review probe showed three rounds with zero healthy read completions or
transport sends until the blocked group was released.

MultiRaft now classifies replay-safe apply backpressure explicitly and defers only
the affected group. It preserves that group's snapshot, entry, and ReadState order
while a persistent cursor gives other groups turns, including with a one-task
budget. Persisted transport can flush during deferral. Fatal apply/persistence
errors still stop the turn. Async storage-apply acknowledgements and snapshot
compaction are emitted only after the corresponding task completes. Legacy apply
queues retain their completed-prefix contract unless they opt into this scheduling.

Regressions hold real refresh activity and its bookkeeping mutex while applying a
committed increment. Attempts must return before release, leave the document and
watermark unchanged, and keep write/read waiters pending. The production router
must complete another group's read while refresh remains held. After release, the
increment applies exactly once. MultiRaft tests separately cover transport, bounded
fairness, snapshot order, fatal errors, opt-in queues, and async acknowledgements.
DB tests cover durable worker-only backfill after deferred activation/reopen,
configuration fencing and cursor reset, and resolver retirement on leaders and
followers. Watchdogs release held owners before joining on regression failure.

### Read deadline defect

`DataServer.waitDataReadSafeWithCancellation` registered its waiter, acquired
`data_raft_mutex` with an unbounded wait, and only then started its five-second
read timeout. Resolution's cross-shard reads therefore could neither time out
nor cancel while waiting for a contended Raft owner. Managed writer/worker
dependencies can keep that wait alive beyond the public write deadline.

The read now uses one absolute deadline across owner-lock admission and the
matching quorum/apply barrier. Lock contention yields through the runtime's
`std.Io`, checks cancellation and remaining time, and releases waiter ownership
on failure. It rechecks the deadline after acquiring the mutex before submitting
ReadIndex. Quorum, applied-index, restart-fencing, and write-success requirements
remain in force; neither the test's write timeout nor its retry policy changes.

The deterministic regression holds the owner mutex, starts a read, and verifies
that timeout or cancellation completes before the owner is released. A watchdog
always releases the owner before joining, so the original code fails without
hanging the runner. It also checks expired requests, pre-cancellation, lock
release, and waiter cleanup. The original implementation fails this regression;
the fixed implementation passes. The test is selected by the default data-runtime
suite.

### Diagnostics and validation

The regression script's existing `ANTFLY_E2E_NATIVE_STACKS=1` now enables an
exec launcher for the multi-node fixture on Linux. Only disposable test children
opt into sibling debugger attachment; host ptrace policy and production startup
are unchanged. The launcher preserves server PIDs and arguments.

Validation on 2026-09-09:

- Original native Debug: 160/160 passes at 256 descriptors, including six
  concurrent workers. This did not reproduce the Linux failure.
- Descriptor probes: 9/9 passes at 192 and 9/9 at 160. At 128, three runs failed
  with descriptor-exhausted node exits, a different signature.
- Deadline-only native Debug: 30/30 concurrent runs passed.
- Deadline-only Linux Debug: 30/30 concurrent runs passed.
- Final code: all 109 data-Raft tests passed, as did the production DataServer
  VOPR merge/read-barrier scenario. Both deterministic regressions fail on
  the original implementation and pass with the fix. No leaks were reported.
- Fifteen Python launcher/resolution checks passed; sibling GDB attachment was
  also verified on the Linux runner.
- Deadline-only Linux ReleaseFast: 60/60 concurrent runs passed.
- Final nonblocking apply + deadline Linux ReleaseFast: **60/60 concurrent
  runs passed** at 256 descriptors with eight-CPU affinity.
- Final Linux ReleaseFast: the regression script’s default schema-migration
  and automatic shard-split cases both passed under the same resource limits.

Reproduce from the worktree root with a built executable:

```sh
SKIP_BUILD=1 ANTFLY_E2E_ENV_LOADED=1 ANTFLY_E2E_NOFILE_LIMIT=256 \
ANTFLY_E2E_REGRESSION_WORKERS=3 ANTFLY_E2E_REGRESSION_REPEATS=10 \
scripts/ci/zig-e2e-regression-loop.sh \
  e2e/antfly/test_resolution.py::test_multinode_autograph_resolves_promotes_and_hydrates_entities
```

On Linux, prefix the script with `taskset -c 0-7` when those CPUs are available.
Build with `-Doptimize=ReleaseFast -Dstrip=false` for symbolized reproduction.
Local investigation logs are retained under `/private/tmp/antfly-ci690-*`.

Run the deterministic regressions from `zig/`:

```sh
zig build antfly-data-runtime-test -- \
  --test-filter 'data raft apply defers refresh contention' \
  --test-filter 'data raft read safety' \
  --test-filter 'data raft retry checkpoints' \
  --test-filter 'production DataServer replicated merge actions run on VoprIo'
```

### Architectural follow-up validation (#692)

The review-driven worker ownership and per-group apply changes are tracked in
[PR #692](https://github.com/antflydb/antfly/pull/692).

- Full standalone Raft suite: 391 tests passed.
- Full data-runtime suite: 173 tests passed, including production VOPR cases.
- Resolver/promotion suite: 38 tests passed, including deferred worker
  activation/reopen, configuration fencing, stale promotion rejection, and
  follower removal. The final focused data-Raft/VOPR suite also passed 110 tests.
- Managed writer-cache/restore lifecycle suite: 218 tests passed.
- Python launcher/resolution checks: 15 passed.
- Final Linux ReleaseFast soak: **60/60 passed**, with three workers,
  eight CPUs (`0-6,8`), and 256 descriptors.
- The same final Linux build passed both default regression-loop cases:
  full-text schema migration and automatic shard splitting.
  Executable SHA-256: `a3c6e7987c0eadc0f633b58373d18ec0e13e0e6529fded12eaf37d966fe317f1`.

Run the durable backfill and catalog lifecycle checks from `zig/`:

```sh
zig build antfly-storage-db-test -- --test-filter resolver --test-filter reresolve \
  --test-filter PromotionRuntime --test-filter processResolutionArtifact \
  --test-filter catchUpWindow --test-filter 'db promotes'
```

Follow-up validation logs use `/private/tmp/antfly-pr692-*`.
