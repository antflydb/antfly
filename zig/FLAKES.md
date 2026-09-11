# Zig runtime flakes

See also the [E2E flake history](e2e/FLAKES.md). Record the original evidence,
reproduction conditions, deterministic regression, and before/after results;
a passing soak alone does not establish a failure's cause.

## 2026-09-11: recoverable restore admission and bounded progress retirement (#694)

Review found a remaining admission gap: a Raft wait can time out before an
enqueue commits. Losing the generated identity invites a second independent
job. The `0259bab66` soak retained two jobs for the same request; the later
receipt fix addressed its leadership classification, but not the timeout path.

Every new restore has a recoverable idempotency key, including requests that
omit the header. Its principal/resource namespace and key determine the job ID.
A conditional-create Raft command atomically claims that ID; replays return the
existing record without overwriting running or terminal progress. Identity and
request fingerprints are checked before adopting it, including fail-closed
handling of truncated-ID collisions. Metadata decoder capability 5 gates this
command across all members; ordinary topology retains its existing minimum
version. Persistence ABI 3 rejects older adapters rather than emulating a
conditional create with a racy get/put pair.

Only confirmed admission returns 202. An uncertain response carries the job
location and recovery key and instructs clients to poll or retry with the same
key. A missing row does not prove non-admission. Clients must supply a stable
key before their first request to recover from losing the entire HTTP response.
Unconfirmed work never enters the local dispatch queue.

Retiring each range previously scanned all remaining progress for its table.
A derived `(metadata group, table, range, node)` index is now maintained in the
same transaction as primary progress. Completion visits only that range's node
entries and removes both namespaces while clearing the restore intent.
Derived-index version 3 rebuilds older stores once from primary rows. Snapshot
replacement removes these derived rows and their version marker; rollback and
upgrade also rebuild rather than trusting rows an older binary did not maintain.

Deterministic regressions cover unknown admission, recovery with an empty local
index, preservation of a running checkpoint, repeated conditional claims,
delayed incarnation reports, legacy index migration, and visiting exactly
three entries with 300 progress rows across 100 ranges. Final-revision soaks
remain required. This is separate from the progressive-index heap corruption
in #705.

Native macOS Debug validation passed 24 restore-store tests, 36 restore/API
integration tests, and 247 metadata tests (one existing opt-in skip), with no
failures or leaks. The native binary build passed all 41 steps. `make generate`,
`make build-antfarm`, `make fmt`, and the Go SDK suite passed. Generation used
an isolated Zig cache after the shared cache was missing `libcompiler_rt.a`;
socket-based tests ran outside the filesystem/network sandbox.

## 2026-09-11: admitted topology receipt loses leadership (#694)

The merged Linux soak on `0259bab66` failed its first backup iteration with a
public create returning an unknown-outcome 409. Metadata logged `NotLeader`
after admission. The retained uncompacted logs on all three replicas contain
the common term-1 prefix through index 73, a term-3 no-op at 74, and no table
create. The request had not survived the election; this was not a coverage
counter failure. The fixture correctly refused to replay an unknown outcome.

The receipt waiter previously classified a leader change as supersession before
observing the admitted entry's applied identity. It now keeps the existing
bounded, event-driven wait across leader changes. An applied matching term
proves success; an applied different term proves replacement; missing status
or term proof remains unknown. The deterministic stepdown regression failed
before the fix: expected `pending`, observed `superseded` (101 other metadata
checks passed, with no leaks).

Only a single atomic topology proposal promotes proven replacement into the
new `MetadataMutationNotApplied` outcome. Compound reconciliation/other mutation
callers retain ambiguity, because replacement of their last entry cannot prove
that earlier entries did not commit. Authenticated forwarding carries the
separate `not-applied-v1` response; it never mislabels an admitted command as
`not-proposed-v1`. Older clients do not recognize the new value and fail closed.
Metadata and data routing may retry the complete atomic operation with this
proof, retaining the existing absolute deadline, hop budget, and campaign
ownership. Public exhaustion preserves the distinct proof. Transport failures,
missing proof, wrong status codes, and unresolved outcomes remain non-replayable.
The runtime error detail is appended, preserving all existing ABI values.

Evidence: `/private/tmp/ci694-main-review-first-failure.tar.gz` (SHA-256
`8893cda56544fb8af67124724cd730cb42c56900b46c4bbcec23021d4ca1eda9`, verified
against the runner), `/private/tmp/ci694-main-review-raft-state.jsonl`, and
`/private/tmp/ci694-receipt-stepdown-regression-before.log`.
Initial validation passes 179 data-runtime tests, 104 metadata/routing tests,
and 10 ABI tests without failures or leaks. Final receipt-mapping and public
response validation, the corrected Linux build, and fresh 100-per-scenario
acceptance are pending. The failed `0259bab66` batch is diagnostic evidence,
not acceptance for this fix.

## 2026-09-11: mixed-version Raft rejection compatibility (#694)

A fresh review of `667e09a58` found that the new rejection correlation assumed
all followers echoed the rejected AppendEntries previous index. Released main
instead reports its own last index in both `log_index` and `reject_hint`, with
the same version-1 wire encoding. A new leader could therefore ignore every
rejection from a lagging older follower and never catch it up.

The deterministic regression fails before the fix (expected next index 2,
observed 5) and covers empty, lagging, and longer conflicting follower logs.
Legacy-shaped feedback now backs a probe toward the confirmed prefix. During
pipelined replication, ambiguous rejections coalesce into one heartbeat-paced
retry; an intervening forward acknowledgement cancels it. Confirmed progress
never regresses, message/byte limits remain intact, and pending snapshots retain
exclusive ownership. The wire format is unchanged. A burst of 32 legacy
rejections produces no immediate payload retransmissions; the next heartbeat
issues one bounded retry, or preserves the pipeline if progress has resumed.

Validation: all **403 Raft library tests passed**, including compacted-log
snapshot recovery and the two compatibility regressions. **100/100 stable
etcd differential seeds**, 48 actions each, matched with the fix. Evidence:
`/private/tmp/ci694-legacy-rejection-before.log`,
`/private/tmp/ci694-legacy-rejection-final-library.log`, and
`/private/tmp/ci694-legacy-rejection-stable-differential.log`.

The preceding merged revision also passed **1,477 integration tests** (one
existing opt-in external endpoint skip), **179 data-runtime tests**, and
**73 Python harness checks**, without failures or leaks. Fresh Linux acceptance
must use the compatibility fix; the pre-merge 300/300 below is historical.

## Peer endpoint changes bypass stable placement reconciliation (#694 review)

After merging `origin/main` (`444440574`) in `6b0985d09`, review found that
`DataRaftPlacementInputs` compared placement and split inputs but omitted
store Raft endpoints. A peer restart can change its URL without changing
membership or the local metadata counter. The stable path then returned before
publishing updated transport routes, indefinitely retaining the old endpoint.

The cache now owns and compares node IDs and Raft URLs alongside its existing
inputs. It deliberately excludes heartbeat generations, capacity, and health
telemetry. Unchanged inputs use an allocation-free linear comparison and retain
the admitted topology; a changed route is published through reconciliation.
Local placement changes still require linearizable metadata authority.

The deterministic production DataServer regression fails before the fix:
transport retains port 31001 when metadata advertises port 31002. It passes
after the fix and also verifies that heartbeat/capacity changes and repeated
unchanged observations retain the admitted plan. The merged data-runtime suite
passes **179/179**, without skips or leaks, and **73 Python harness checks**
pass. Evidence: `/private/tmp/ci694-main-review-peer-before.log`,
`/private/tmp/ci694-main-review-data-fixed-unrestricted.log`, and
`/private/tmp/ci694-main-review-python.log`. The initial sandboxed full suite
could not bind local HTTP listeners; its nine failures and two skips are
superseded by the unrestricted 179/179 run, not counted as acceptance.

The merge preserves all 34 regression build additions in main's new owner
modules. A fresh Linux build and 100-per-scenario soak are required for the
merged endpoint fix; the earlier 300/300 result below predates this merge.

## 2026-09-11: mixed Linux acceptance before the main merge (#694)

The fresh run completed **300/300**, with **100/100 each** for CLI quickstart,
three-by-three metadata backup/restore, and CLI retry exhaustion/restart.
Four workers each ran 25 iterations of all three scenarios, from 02:21:01 to
03:17:04 UTC. There were zero failures, errors, skips, or failed-case retries;
the driver exited 0. These results are from one revision and do not combine
passes from the earlier failed runs.

Production commit `05f96814e` includes the atomic coverage batch, lifecycle and
placement authority fixes, durable Raft replacement/abort fixes, and monotonic
replication progress. Test revision `b63468094` includes complete publication
before exact semantic ranking. The subsequent `574af3586` formatter correction
has an identical Python AST. The Linux x86_64 ReleaseFast executable was built
with a fresh compiler cache and verified SHA-256:
`dfdbe000a2712b11d0919a4dc66888c51f905030d3d14a8a360bee4d5cbc3265`.
The driver used eight allowed CPUs (`0-6,8`) on a pod requesting four CPUs and
limited to eight; these were not dedicated cores. The retry scenario retained
all 36 provider attempts and the real production backoff.

Complete logs, runner script, and machine-checked acceptance manifest are in
`/private/tmp/ci694-replication-acceptance-results.tar.gz`, SHA-256
`f78936d40adbf5f6b96519bdc3a8c871e96531b0f168a838192531326515a9cd`,
verified against the runner archive. Historical failure archives remain
preserved separately. This acceptance result does not establish that unrelated
flakes cannot occur; deterministic regressions and evidence limits are recorded
below. GitHub CI is tracked separately from this controlled Linux soak.

## Delayed Raft responses amplify replication and exhaust outbound admission (#694)

The `42cb81c6a` run ultimately finished **297/300**: quickstart 99/100,
backup/restore 98/100, retry exhaustion 100/100. The second restore failure
(`yiezk7ry`) retained completed progress on metadata node 1 while nodes 2 and 3
had retired it; node 2 logged up to 2,377 messages / 642,867,360 bytes in a Ready
batch. Both failures and the CLI failure below are preserved in
`/private/tmp/ci694-durable-complete-results.tar.gz`.

The fresh Linux mixed soak on `42cb81c6a` failed backup/restore in worker 1,
iteration 1. Metadata node 1 accumulated a Ready batch of **1,144,753,225 bytes**,
exceeding the unchanged 1,140,850,688-byte hard ceiling, and quarantined its
group. Earlier batches grew from hundreds of messages to thousands. The first
failure's logs, durable state, and native stacks are preserved in
`/private/tmp/ci694-durable-first-failure.tar.gz` (SHA-256
`70658edd776376a2c5dc2a966b59d6941b0ea9a37b3030200e8d7fe835350657`, verified
against the runner archive). This run is failed acceptance.

The leader assigned every success response directly to both `match_index` and
`next_index`, rewinding acknowledged progress and the optimistic send cursor.
A deterministic 32-entry pipeline reproduces the amplification: acknowledging
its first entry requeues the remaining **31 already-sent entries**
(`/private/tmp/ci694-raft-progress-before-focused.log`). Successes now preserve
monotonic Match and Next, while current empty-probe acknowledgements can still
resume replication. Rejections identify the rejected append's previous index
separately from the follower's last-index hint; delayed rejections cannot clear
a newer flight window or supersede a newer probe. Earlier-term acknowledgements
cannot authorize progress in a replacement leader's term.

A full window also needs recovery if an append or its acknowledgement is lost.
Heartbeat responses now allow a payload-free append probe at the last sent
prefix. Its acknowledgement releases the window; a valid rejection resumes
catch-up from the known matching prefix. Normal acknowledgements continue
unsent work even when another voter has already advanced commit. Message/byte
limits, outbound quarantine, and request deadlines are unchanged.

The same response-path review reproduced an older-term entry being committed
by counting replicas alone (`/private/tmp/ci694-raft-current-term-before.log`,
expected commit 1, observed 2). A quorum now directly commits only a current-term
entry, which commits its older prefix indirectly. The snapshot-abort harness
also now installs the acknowledged prefix before claiming the follower has it;
the old fixture invented a durable acknowledgement for absent data and relied
on Match regression to recover. Its corrected history matches the etcd 3.6.0
oracle (`/private/tmp/ci694-snapshot-abort-oracle.log`). All **401 Raft library
tests pass**, with no skips or leaks (`/private/tmp/ci694-raft-progress-fixed.log`).
All **176 data-runtime integration tests** also pass without skips or leaks
(`/private/tmp/ci694-replication-data-runtime.log`). The etcd comparison matches
**100/100 stable-profile seeds**, 48 actions each, with pre-vote and quorum
checking (`/private/tmp/ci694-raft-progress-stable-differential.log`). The broader
stress-profile comparison encounters an existing removed-voter visibility
difference at seed 3, step 25; the pre-fix revision reproduces it as well
(`/private/tmp/ci694-raft-stress-seed3-before-oracle.log`). This comparison is
not counted as passing acceptance. Repeated compaction actions now have the
same idempotent storage semantics in both harnesses. The fresh Linux
100-per-scenario acceptance above passes for this revision.

Isolated-cache validation of the complete Raft and data-runtime integration
targets passes **1,045 tests**, with one existing opt-in external wrong-route
endpoint check skipped (`/private/tmp/ci694-replication-fresh-integration.log`).
The optimized Linux build on `05f96814e` passed all 27 steps, using a fresh
compiler cache; executable SHA-256:
`dfdbe000a2712b11d0919a4dc66888c51f905030d3d14a8a360bee4d5cbc3265`.

## Exact semantic ranking requires complete source publication (#694)

Worker 4, iteration 23 of the `42cb81c6a` soak returned an empty semantic result
after `searchable-artifacts=1`, then indexed directly into the empty hit list.
The test expected Alpha to rank first across two source documents even though
that milestone guarantees only the first searchable artifact, not which source
has published. A controlled Linux probe holds Alpha's embedding while Beta is
published: the partial wait succeeds and returns only Beta; after releasing
Alpha and waiting for `complete`, Alpha ranks first and both documents appear
(`/private/tmp/ci694-partial-cli-probe.log`). This proves the ranking assumption
invalid; it does not independently reproduce the original empty result.

The quickstart retains its partial milestone assertion, then waits for the
existing `complete` milestone before asserting exact corpus ranking, matching
its image-query contract. The existing 20-second wait bound is unchanged, and
no query is retried to pass. A failed complete query now reports both wait
results, query output/stderr, and current index status instead of an unhelpful
IndexError. The original standalone log/root is retained in
`/private/tmp/ci694-durable-cli-failure.tar.gz`. Fresh acceptance includes this
test contract correction alongside the production replication fixes.

## Provider-restart regression races an independent index consumer (#694)

The x86 job in run `34543627560` failed
`db restart after provider failure resumes enrichment from retained async replay`
at an assertion that the derived consumer remained below the failed enrichment
source sequence. Production explicitly permits that consumer to advance while
enrichment remains behind; `truncateReplaySequenceAsync` retains replay through
the durable enrichment checkpoint. The test now stops at observed provider
failure, waits for the derived consumer to reach the source sequence, then
asserts retention and actual embedding/search recovery after reopening. This
exercises the adverse ordering directly instead of racing a negative progress
assertion. The three focused retention/restart tests pass with no skips or leaks
(`/private/tmp/ci694-replay-retention-fixed.log`). Original CI log:
`/private/tmp/ci694-26ff-x86-ci.log`.
The corrected restart test also passed **100/100 native macOS arm64 executions**
on `42cb81c6a`, using four workers with 25 fresh processes each and no retries
(`/private/tmp/ci694-replay-unit-soak/results.json` and `manifest.json`).

## Leadership replacement skips persistence and loses a confirmed abort outcome (#694)

The `9e5b558ce` Linux soak completed **299/300**: quickstart 100/100,
backup/restore 99/100, retry exhaustion 100/100. Its seed failure is preserved in
`/private/tmp/ci694-authority-complete-results.tar.gz` (SHA-256
`5be3b103ec4dc4d07a6abc5779c7bac5131496c16120f84c98923679d1396881`).
Decoded Raft records for group `8476403407145147734` show node 4 persisting a
term-2 prepare at index 4, then leadership changing to node 6 in term 3. Nodes 5
and 6 retain a term-3 no-op at index 4; node 4's final checkpoint incorrectly
retains the old term-2 prepare. All three transaction records are aborted.
The read-only decoder and output source are `/private/tmp/ci694-decode-raft.py`
and `/private/tmp/ci694-seed-failure-state.tsv`.

`RaftLog` replaced conflicting entries without moving back its stable/persisting
watermarks. Ready therefore omitted a replacement at a previously durable index,
allowing memory and restart history to disagree. The deterministic regression
fails before the fix with expected first unstable index 2, observed 3
(`/private/tmp/ci694-raft-replacement-before.log`). Replacement now invalidates
both watermarks from the conflict; persistence completions require matching
index and term. Borrowed append clones before changing the old suffix so an
allocation failure cannot leave partial mutation. Duplicate appends preserve
matching entries and acknowledge only the supplied prefix; committed conflicts
remain errors. Regressions cover stale/in-flight completions, apply fencing,
matching prefixes, and persistence/restart in both storage modes.

The superseded prepare also exposed a transaction outcome error. After the
coordinator confirms its durable abort, a prepare's unknown Raft/transport outcome
does not make the transaction decision unknown. These failures now return the
existing participant-unavailable conflict only after `abortParticipants`
confirms abort; an unconfirmed abort still propagates `AbortDecisionNotDurable`.
The existing bounded stateless retry owner may then create a fresh transaction.
No prepare is replayed blindly, and commit ambiguity remains conservative. The
coordinator regression failed with `RaftBatchWriteOutcomeUnknown` before the fix
(`/private/tmp/ci694-prepare-abort-before.log`); it covers direct abort success,
abort confirmation through status, and pending/committed decisions that cannot
authorize a fresh attempt. Production retry limits and deadlines are unchanged.

Validation: all 395 Raft library tests, 19 transaction coordinator/participant
contracts, four existing stateless retry-bound contracts, and 176 data-runtime
tests pass without skips or leaks. Logs: `/private/tmp/ci694-raft-final.log`,
`/private/tmp/ci694-transaction-contracts-fixed.log`,
`/private/tmp/ci694-abort-retry-contracts.log`, and
`/private/tmp/ci694-raft-replacement-data-runtime.log`. Fresh Linux acceptance
remains required for this combined revision.

## Review follow-up: forwarding capacity and equal placement counters (#694)

Review reproduced two independent defects on `9e5b558ce`. Saturating the shared
32-worker outbound executor prevented an actual Raft HTTP request from starting
with `ConcurrencyUnavailable` (`/private/tmp/ci694-review-outbound-capacity.log`).
Forwarded application writes now acquire a lease on their own bounded executor
before transport admission. Each lease reserves six workers for the nested
HTTP/1 request, connect, socket, and deadline tasks; the default 32-worker lane
admits five simultaneous forwards. Excess requests report the existing safe
leader-unavailable classification before sending. Teardown closes admission and
drains leases before destroying the executor. Borrowed I/O remains authoritative.
The default API allocation changes from 48 to 16 workers to fund the new lane;
the aggregate remains 252 under the existing 256-worker ceiling. A regression
proves both forwarding under Raft/control saturation and Raft HTTP progress under
forwarding saturation. Capacity and shutdown tests cover rejection and draining.

The second regression supplied changed placement with the same peer-local epoch
as the admitted plan. Before the fix, observation reconciliation succeeded
instead of requiring authority (`/private/tmp/ci694-review-epoch-collision.log`).
The stable path now compares owned placement inputs by value: metadata identity,
all local and remote placement rows, and split destination IDs. Equal counters
cannot bypass authority for a changed local plan. Unrelated status/counter changes
reuse the admitted plan without allocation, topology reconstruction, or a quorum
read. This retains one copy of placement inputs and performs an exact linear
comparison; changed inputs rebuild the candidate plan before deciding whether
fresh authority is required. Regressions cover equal-counter replacement/removal,
unchanged placement with a different counter, and authoritative deletion.

All 176 data-runtime tests and 62 backend-runtime/capacity tests pass with no
skips or leaks (`/private/tmp/ci694-review-fixes-data-runtime.log` and
`/private/tmp/ci694-review-fixes-lane-lifecycle.log`). Fresh Linux acceptance is
required. The prior `9e5b558ce` soak finished 299/300; the subsequent durable-log
and transaction-decision investigation is recorded above. Bounded prepare/apply
outcome diagnostics retain the relevant phase without changing deadlines.

## Metadata cache incorrectly orders peer-local lifecycle counters (#694)

Ordinary metadata cache refresh compared `AdminSnapshot.status.metadata_epoch`
numerically across peers. Those lifecycle counters are process-local: an old
empty catalog at counter 1000 could suppress a newly created table returned by
another peer at counter 1 indefinitely. The regression failed before the fix
with expected one table, observed zero (`/private/tmp/ci694-observation-before.log`).

Observation publication now uses the existing snapshot fence generation and
mutation-invalidation rules, without ordering peer-local counters. A concurrent
linearizable publication still supersedes an older in-flight observation; a
mutation invalidation still rejects it when no replacement exists. The cache
publication helper owns and releases its incoming snapshot on every path.
Changed placement and retirement retain their separate authority requirements.
The new regression exercises refresh, concurrent fence publication, invalidation,
and ownership cleanup, and is included in the default data-runtime test lane.
All 11 metadata-source checks pass without skips or leaks
(`/private/tmp/ci694-observation-fixed.log`).

## Follower catalog observations can retire live data-Raft history (#694)

The failed `00dd1e4aef` restore case (`sjng4qip`) recorded repeated admission of
both original and restored groups before `ConflictingDataApplyBatch`. Its
preserved restored Raft stores contained fresh bootstrap checkpoints while the
data apply journal retained committed identities. Data-Raft reconciliation was
allowed to retire local groups from ordinary cached/follower snapshots, and
`metadata_epoch` is only a process-local lifecycle counter. A stale peer can
therefore omit a live group while presenting a numerically larger epoch.

The real data-runtime regression admits group 77, then supplies an older empty
catalog with a larger lifecycle counter. Before the fix it fails with expected
`active`, observed `absent` (`/private/tmp/ci694-retirement-before.log`). Remote
reconciliation now requires a coherent linearizable snapshot before changing
local replica placement, membership, or generation. The authority read and
publication share the reconciliation mutex; the Raft progress owner remains
independent. Unsupported or unavailable authority cannot authorize retirement.
The status reporter also uses the refreshed snapshot after reconciliation.

Unchanged placement reuses its prior admitted plan without a quorum read.
Exact value comparison covers replica/bootstrap identity, voter/learner arrays,
and scalar relocation fields. A fresh authoritative snapshot bypasses the
process-local epoch shortcut so a real deletion still applies when two peers'
counters happen to match. The regression covers stale removal, stale generation
replacement, unchanged cached progress, and authoritative deletion with an
equal counter. All 175 data-runtime checks pass without skips or leaks
(`/private/tmp/ci694-retirement-runtime-final.log`). Linux acceptance remains
pending; the strict apply-history conflict check is unchanged.

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
this cause. The final mixed Linux acceptance above includes this coverage fix
and passes all 100 runs of each scenario.

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
failure can occur after a request starts. The initial fix moved forwarding to
the bounded outbound Raft network executor. The review follow-up above replaces
that sharing with a dedicated lane and whole-request admission. Control waits retain their executor,
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
