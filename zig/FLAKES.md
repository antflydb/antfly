# Zig runtime flakes

See also the [E2E flake history](e2e/FLAKES.md). Record the original evidence,
reproduction conditions, deterministic regression, and before/after results;
a passing soak alone does not establish a failure's cause.

## 2026-09-16: replay-retention fixture timed out on a checkpoint proxy

PR #691's [x86_64 unit job](https://github.com/antflydb/antfly/actions/runs/35176786855/job/105060523520)
at `37b9910520` failed only
`db async replay truncation retains journal behind generated enrichment`.
`waitForAppliedSequenceAdvance` timed out waiting for `ft_v1` to persist a
checkpoint within 100 ten-millisecond polls. The production E2E job passed,
including the prior shutdown regression. Twenty fresh local Debug processes,
with four running concurrently, did not reproduce this Linux timeout.

The retention fixture used checkpoint appearance as a proxy for a truncation
attempt, then watched the journal for another half second. Neither established
that the truncation callback had finished. A delayed checkpoint is not evidence
of replay loss, and the trace alone does not establish a stuck production worker.
The test now observes the real provider rejection through an event, requests
truncation through the captured source tail, and compares every retained record's
sequence and payload after that call returns. It then permits provider recovery,
drains the production pipeline, checks both durable index checkpoints, and
queries the generated vectors for both documents. This covers retention and
recovery without requiring autonomous checkpoint publication within one second.

The revised case passed 40/40 fresh Debug processes with four concurrent workers.
As a negative control, temporarily removing only the generated-enrichment clamp
in `truncateReplaySequenceAsync` made it fail at the record-count assertion
(`expected 1, found 0`). Restoring the clamp restores the passing test; the
committed change modifies only the fixture and this investigation record. All
five related replay-retention, provider-restart, and index-worker cases pass.

## 2026-09-16: source quiescence freed activity borrowed by transaction callbacks

PR #691's [base Antfly E2E job](https://github.com/antflydb/antfly/actions/runs/35168905132/job/105044672774)
tested `19ffc3c820` and passed the body of
`test_multinode_exact_candidates_follow_redirects_across_entity_shards`, then
failed teardown when data node 101 exited with `SIGSEGV`. The uploaded build
artifact contained only the executable; the failure-log upload glob omitted
multinode roots. CI now retains their node logs, failure diagnostics, and any
captured native stacks as well as standalone logs.

A local Debug repetition failed on the fourteenth run, again during node 101
teardown. This time `endGroupOperationLocked` asserted because a
`ResolveFollowerFanoutTask` finishing `txnResolveGroupLocalWithCancellation`
could no longer find its activity record. `ProvisionedTableWriteSource.quiesce`
freed those records after joining source-owned jobs, before the data server
joined DB-owned promotion workers that could still call the transaction source.
The same premature destruction exists on main. Without a Linux stack, the local
assertion establishes a concrete lifecycle defect but cannot prove the exact
instruction behind CI's segmentation fault.

Source quiescence must close its own job admission and drain those jobs while
retaining bookkeeping borrowed by external callbacks. Final source destruction
releases that state after the caller has joined the DB workers. The regression
holds a real transaction-resolution operation across source quiescence and
requires normal activity release on both success and cancellation; production
E2E keeps its crash-rejecting teardown assertion.
The deterministic regression fails before the fix in `endGroupOperationLocked`
and passes afterwards. The full writer lifecycle target passes all 228 tests;
the merged error-ABI suites pass all 17 tests.
On macOS ARM64, the rebuilt Debug and ReleaseSafe production executables each
pass all 35 resolution/Autograph module cases (including data-node restart) and
20 fresh-cluster repetitions of the originally failing exact-candidate case.
That is 40/40 additional shutdown repetitions with the crash assertion enabled.
Linux CI must still confirm the original platform.

## 2026-09-16: constrained Autograph restart lost retryable owner admission

The second retained-corpus qualification of PR #704 at `81ab94c8b1` failed its
[production E2E job](https://github.com/antflydb/antfly/actions/runs/35126679231/job/104951437450).
All eight VOPR campaign shards and all four corpus replay jobs passed. Under
the 256-descriptor Autograph profile, worker 2, iteration 16,
`test_multinode_autograph_recovers_after_data_restart` completed its assertions
but failed teardown because data node 102 exited with code 1. Its log identifies
`StorageBusy` applying the `documents` Raft entry at index 6. The artifact
`production-e2e-soak` retains the failed root `antfly-zig-scaling-e2e-l3fykcbz`.

The descriptor-pinned committed-entry path used foreground owner acquisition,
which waits up to five seconds for registry/owner admission before returning
`StorageBusy`. The apply driver recognized only writer-unavailable and resource
budget errors as retryable, so the busy result escaped and stopped the process.
The logs do not identify which lease held admission. The regression forces
registry contention, an exclusive owner lease, and publication independently.

Committed-entry admission now makes one attempt and yields conflicts to the
existing Raft pending-apply queue, before encoding the batch. `StorageBusy` and
publication's `StorageReadTemporarilyUnavailable` retain the original term/index
and retry checkpoint. Neither advances the applied watermark, completes a write
waiter, nor marks the command rejected. Recovery replays the same entry identity;
the owner persists that identity atomically with the mutation. Unexpected errors
retain their fatal behavior. This avoids parking the shared progress driver behind an owner
whose maintenance callback may itself need Raft progress.

The same failed root logged `DistributedQueryUnavailable` becoming
`RuntimeBoundaryFailure` and then `InternalFailure`. Register that exact
retryable identity in both callback and owner failure formats so the HTTP layer
can retain its existing 503 behavior. The new cross-unit callback assertion
failed before the fix with exactly that identity loss.

The Autograph step was misleadingly green because GitHub's implicit Bash shell
did not enable `pipefail`; `tee` hid the script's nonzero exit. The final JUnit
evidence check correctly failed the job. Explicit Bash now propagates pipeline
failures while retaining logs. Injected failures in all three production
pipelines fail against the original workflow and preserve their exit status
with the fix. The existing 200-case Autograph soak, including restart and
teardown checks in both profiles, remains enabled. The Raft checkpoint regression
also runs in scheduled VOPR qualification.

## 2026-09-16: Debug stack unwinding escaped a fresh VOPR fiber

Local macOS ARM64 Debug profiling of `origin/main` (`70098c101d`) crashed in
these borrowed-runtime tests:

- `db apply fences wait through their borrowed runtime`
- `graph ownership cleanup runs on borrowed VoprIo before replicated merge`
- `background maintenance services lifecycle runs on borrowed VoprIo`
- `ttl runtime executes production pass on borrowed VoprIo`
- `transaction recovery executes production pass on borrowed VoprIo`

The apply-fence case also failed with timing instrumentation disabled. The
reported segmentation fault was at address `0x8`; stack-trace printing then
stalled. DebugAllocator captures allocation/free traces on the VOPR task stack,
so stack unwinding itself can cause this fault before any diagnostic is printed.

`Kernel.createTask` initialized the fiber frame pointer to zero, but `Entry.entry`
left ARM64's link register inherited from the scheduler. Metadata-based unwinding
could follow that unrelated return address and interpret the null frame pointer
as another frame. A standalone task-kernel regression reproduced the same
`EXC_BAD_ACCESS` at `0x8` in `std.debug.Dwarf.SelfUnwinder.nextInner` under LLDB.
The equivalent x86_64 synthetic return-address slot was uninitialized too.

The entry trampoline now initializes the nonexistent caller's return address to
zero on both architectures. This costs one instruction at task entry and leaves
normal context switches unchanged. Allocator diagnostics and stack tracing stay
enabled. The regression captures a complete trace and exercises DebugAllocator
both before and after yielding the same task back to the scheduler.

After the fix, macOS ARM64 Debug passed all **32 runtime adapter tests**, including
the five failures, and all **167 VOPR library tests**. The standalone task-kernel
suite also passed all **14 tests in ReleaseSafe**.
The five reported cases additionally passed **20 fresh processes / 100 cases**
with zero skips, failures, or leaks. The 14 task-kernel tests passed in Debug on
Linux x86_64 under local container emulation. The pre-fix standalone regression
also passed there, so the Linux run validates the fix but does not establish a
Linux reproduction of the macOS fault.

## 2026-09-16: metadata ownership fixtures bypassed Raft serialization

PR #704's [x86_64 unit job](https://github.com/antflydb/antfly/actions/runs/35052615288/job/104656431732)
at `fadf936c29` aborted in
`metadata ownership preserves same-id remote backfill observations`.
The restore supervisor's `ensureLinearizableRead` called Raft `readIndex` while
`Scheduler.ready_pass_active` was true, failing the scheduler's non-reentrancy
assertion. The graph-repair and resolver-reopen regressions from the preceding
failure both passed; the SDK, ARM codec, and base E2E suites also passed.

The ownership fixtures added in [#737](https://github.com/antflydb/antfly/pull/737)
start a real metadata server, including its asynchronous restore supervisor, but
four calls in two fixtures advanced
`svc.raft.runRaftRoundOnly()` directly. The supervisor uses the owning
`MetadataHttpService` runtime mutex; those raw-host calls bypassed it. Production
service round and read-barrier entrypoints already share that mutex.

The fixtures now advance through `svc.runRaftRoundOnly()`, preserving the live
supervisor and serializing Ready processing with its read barriers through the
existing `std.Io.Mutex`. This fixes the invalid concurrent caller, without
weakening the scheduler assertion, disabling background work, or adding another
lock to the Raft hot path. Both the placement/restart fixture and all four
ownership-projection cases use the same service boundary.

With native localhost socket access, the unchanged `fadf936c29` binary reproduced
the same backfill-test abort in repetition 32 of a four-process runner: the
restore supervisor entered `resumeGroupOnActivity` through ReadIndex during an
active Ready pass. The runner stopped scheduling new work after that failure;
34 other processes completed successfully. Sandbox-only runs skipped all eight
listener-dependent tests and are not counted as validation.

After the fix, the failed CI shard's exact runtime filter/skip selection passed
all **207 tests** locally with native sockets, zero skips, failures, or leaks.
The separate read-cadence regression also passed, verifying linearizable reads
do not take over the election/heartbeat clock.
All eight ownership cases then passed **200/200 fresh processes (1,600 test
executions)** with four concurrent processes and native sockets, with no skips,
failures, or leaks. Formatting and diff checks passed. These are local results;
standard CI and full-soak qualification must be checked for the pushed commit.

## 2026-09-15: graph repair yield accounting and independent resolver completion

PR #704's [x86_64 unit job](https://github.com/antflydb/antfly/actions/runs/35044256661/job/104631027746)
at `95b0e74334` failed `db index repair streams graph artifact rebuild in batches`
with `expected 2051, found 0`, then aborted while freeing the fixture's writes.
The separate DB-core partition failed
`db resolver workers recover pending journal targets after reopen without new writes`
because resolution still required catch-up. The previous replay-truncation
regression completed; this run did not hit the idle watchdog.

The graph repair wrapper converted activation-budget exhaustion into an empty
`yielded` result, dropping the work count even after durable snapshot construction.
A one-millisecond activation budget deterministically forces this path because it
cannot cover the five-millisecond publication reserve. The graph batching
regression now exercises that yield, checks retained work and candidate state,
reopens the DB, and verifies activation reuses the same candidate without another
batch flush or recounting previously completed work. The wrapper retains the
current turn's completed-work and discovered-artifact-debt counts on a deadline
yield. This adds no scans or storage I/O. Production pause limits and retry policy
remain unchanged. The original CI log did not retain repair state,
so it cannot independently prove which early-return path produced its zero count.

The fixture also left function-scoped `errdefer` frees armed after handing its
last key/value pair to the writes list. A later assertion error freed that pair
twice. Allocation-to-list transfer now has a local scope, so assertion failures
report normally and the list remains the sole owner after append.

Resolver output publication and resolution checkpoint/backfill completion are
independent. The test waited for promotion and two sink writes, then immediately
asserted resolution was idle. Its completion condition now checks both stages;
it keeps the existing bounded wait and still tests automatic recovery without
new writes for both pending-resolution and pending-promotion reopen cases.

Before the production accounting fix, the forced graph-yield test failed with
`expected 2051, found 0`; the corrected fixture cleanup reported that assertion
normally, without an abort or leaks. Repeating the two original tests against
`95b0e74334` reproduced the resolver failure in fresh process 75 (74 prior passes).

After the fix, all 20 focused repair/resolver tests passed in ReleaseSafe,
including graph and full-text replacement, corrupt-artifact debt, activation
budget yielding, and dense candidate yield/reopen accounting. The graph-yield
and resolver-reopen cases passed **200/200 fresh processes (400 selected test
executions)** with four concurrent processes, no skips, failures, or leaks.
Formatting and diff checks passed. These are local regression results; standard
CI and full-soak qualification must be checked separately for the pushed commit.

## 2026-09-15: merged runtime-lane assertions and replay fixture deadlock

PR #704's [x86_64 unit job](https://github.com/antflydb/antfly/actions/runs/35025324900/job/104571966002)
at `7c061722ac` failed the Raft service-worker test twice, then terminated after
1,801 seconds without log progress. No OOM kill was recorded. These were
merge regressions, not evidence of an intermittent production race.

The Raft host had been assigned `raftOutboundIo()` while the merged test and
runtime ownership contract required `runtime.io()`. Native runtimes give those
lanes distinct identities. Host synchronization/reconciliation now use the
general owner runtime; transport retains its dedicated outbound lane. The VOPR
regression supplies distinct general and outbound runtimes with different clock
values and checks both identities, in addition to the native worker-budget and
rollback test.

A local run of the DB-core complement reproduced the stall in
`db async replay truncation retains durable enrichment debt`. A native stack
sample showed its main thread parked in `truncateReplaySequenceAsync` on the
repair-pin mutex. The fixture had locked that mutex and installed an incomplete
fake vtable in `AsyncContext.io`, expecting its wait callback to unlock it.
Truncation correctly uses `index_manager.checkpointIo()`, so the fake callback
could never execute. The durable-retention fixture now tests the journal
watermark directly. Contention remains covered by the real scheduled VOPR test
`db replay truncation waits for repair pins through borrowed VoprIo`, including
both truncation entrypoints and an absent operation-level Io override. The
owner's synchronization authority remains stable throughout the operation.

CI's checked Run output buffered the entire unfinished DB partition, while its
stack-dump selector missed named `*-tests` executables. Partition output now
also streams to per-process logs, including a test name without its final
newline. The watchdog observes those logs, prints their tails on failure, and
includes named test executables in stack capture; the workflow retains the logs
as artifacts. A process handshake regression verifies partial output is visible
before the child exits and a nonzero child status is preserved. This preserves
the existing watchdog limits rather than masking a blocked test with more time.

Validation: all three focused Raft host cases and six managed-Raft contracts
passed with native socket access. The three replay retention/repair-pin/restart
cases passed; the retention and repair-pin cases also passed 20 fresh-process
repetitions (40 executions). All 32 `vopr-runtime-test` cases passed without
skips or leaks. Workflow lint, 55 CI policy tests, three partition-runner tests,
and two validation-scope tests passed. This is local regression evidence; fresh
standard CI and full-soak qualification must be checked separately.

## 2026-09-15: snapshot ownership leaked between cooperative tasks

Review of #704 reproduced a pre-existing `SnapshotAdmission` bug on both
`096a1f9e4` and `origin/main` (`0fb01a4add`). A VOPR task acquired capture and
slept for ten virtual milliseconds. Before advancing time, another task acquired
mutation admission on the same OS thread and reported `capture_active=true`,
`mutation_overlapped=true`, and `lease_bypassed=true`. Thread-local ownership
mistook an unrelated task for the capture owner. The same mechanism also made
leases unsafe to transfer between native Io workers.

Admission now belongs to explicit operation leases. Nested mutations retain a
shared lease without re-entering the writer-closed gate. Capture lends a scoped
mutation capability to its catalog guard. Bulk-ingest nested batches pass their
parent lease; LSM maintenance calls its admitted helper. Dense replay passes the
exact catch-up session token through both manual and Io executor callbacks,
retains its lease under the session-map mutex, and rejects stale or mismatched
tokens. Closing the session cannot retire a lease still held by an active batch.
Ordinary acquisition remains allocation-free; waits use the owner's `std.Io`.

Five admission regressions cover independent VOPR tasks, cancellation, a queued
capture with an explicitly retained mutation, capture-owned maintenance, and
release on a different native Io worker. They passed **200/200 fresh processes
(1,000 test executions)**. The DB token regression also checks that a retained
batch keeps capture excluded after session close and that closed/wrong-index
tokens cannot borrow admission. These cases run in `vopr-runtime-test` and
production VOPR qualification. The focused DB integration run passed 28 cases
(native snapshots, concurrent capture/writes, bulk ingest, and replay-worker
lifecycle), with one LSM-backend fixture skip and no leaks. The final
`vopr-runtime-test` run passed all 25 storage/runtime and seven DataServer tests
with no skips or leaks. Full-soak results must be associated with the new commit;
an earlier-head soak cannot qualify this fix.

## 2026-09-15: runtime-bound apply locks delayed handoff by polling

Review of #704 found that binding `ApplyRwLock` to the DB runtime selected
50-microsecond to 1-millisecond sleeps, while unlock skipped the futex wake for
bound locks. This affected ordinary native DBs as well as VOPR. A native
ReleaseSafe handoff probe (30 fresh reader tasks, each parked behind a writer
for two milliseconds) measured **617 microseconds average / 1,252 worst** from
unlock to acquisition on `256bb99782`, versus 4 / 11 for the unbound native lock.
These are local latency measurements, not production throughput estimates.

Contended acquisition now captures the wake epoch before checking admission and
parks on the owner's `std.Io` futex. Unlock advances the epoch and wakes that
same runtime. Sequentially consistent waiter registration and epoch rechecking
let uncontended unlock avoid the wake call without losing a concurrent waiter.
The last priority reader signals unconditionally: checking a separate writer
count could miss a concurrently registering writer on a weakly ordered CPU.
DB apply and snapshot admission, plus HBC publication and cache locks, are bound
before use. A caller's separate filesystem/request I/O lane cannot move the wait
to a different scheduler. Writer intent, reader priority, and cancellation
cleanup retain their existing admission rules.

Backend task cancellation wakes its futex directly. Callback-only cancellation
tokens lack wake registration, so their futex waits retain a one-millisecond
cancellation recheck; unlock still wakes immediately. Dense-posting admission
uses one absolute 50-millisecond deadline on the owning runtime clock, including
VOPR, instead of polling the host clock. Timeout releases writer intent and the
reader gate, leaving repair work pending.

The deterministic regressions cover shared/exclusive handoff without advancing
virtual time, an unlock between failed admission and parking, and deadline and
callback cancellation after closing reader admission. The no-clock-advance
regression fails against the old lock with `LockWaiterDidNotPark` and passes
after the change. They run in
`vopr-runtime-test` and production VOPR qualification. The native contention
regression uses `std.Io.Group.concurrent` for both bound and unbound locks.
The same handoff probe after the fix measured **6 microseconds average / 20 worst**
for the bound lock (unbound: 7 / 29). Four snapshot admission tests pass. The
14-test lock suite passed 200/200 fresh processes (2,800 test executions),
including four million writes across the native bound/unbound exclusion test.
`zig build vopr-runtime-test -Doptimize=ReleaseSafe -j1` also passes: 18
storage/runtime adapter tests and seven DataServer tests, with no skips or leaks.

## 2026-09-15: multi-node Autograph promotion stalls with an untransportable read timeout

[The base E2E shard](https://github.com/antflydb/antfly/actions/runs/34928784180/job/104259435053)
failed `test_resolution.py::test_multinode_autograph_resolves_promotes_and_hydrates_entities`
on `4dedffc703eb4aa744ae89d1d9fbd0ddc4b97931` (`codex/spfresh-segment-wal`).
[The linked aggregate job](https://github.com/antflydb/antfly/actions/runs/34928784180/job/104263235408)
only reports that shard failure. The shard finished with 447 passes, five skips,
and one failure. Neither `person/ada_lovelace` nor `org/antfly` became visible
within the 115-second promotion deadline. Its last lookup returned HTTP 500;
the server logged `runtime callback returned untransportable error method=lookup
err=ReadIndexTimeout`, `RuntimeBoundaryFailure`, and resolution
`StorageKernelFailure`. Startup also logged `WriterLocked`. Attempts to capture
six native process stacks were denied by ptrace policy.

The failed revision lacks the `read_index_timeout` semantic identity and the
public/internal read availability mapping already in #704. These preserve the
error through the compiled callback and HTTP client boundaries and return
503 with `Retry-After`, without weakening the Raft read barrier. The boundary
round-trip and public-read parity regressions exercise this concrete error.
The failed revision already contains bounded read-owner admission: that earlier
fix alone did not prevent this failure. There are no usable stacks proving a
new resolver/Raft deadlock in that CI job.

Local production repetitions on the merged-main executable (SHA-256
`710889d55cec215eeffd136ccf0327487cbb3b84d8cc99443f5ab330b42c7658`)
reproduced **two failures in 100 fresh clusters**: normal 48/50, descriptor-limited
50/50. Another 50 normal cases passed, illustrating why a rerun cannot clear the
failure. Twenty repetitions of the original CI Linux executable under local
emulation passed; that is not equivalent to native Linux CI qualification.

The retained LSM tables from a local stall contain a generation-one resolution
artifact with both canonical entities and a durable promotion replay record at
sequence 3. Resolution and promotion report target/applied 2, while the graph is
ready at 3 and the entity table is empty. Runtime initialization restored only
the applied checkpoint, losing the volatile target on reopen. Both stages now
recover their target from their own durable replay lane, independently of graph
progress. The deterministic DB regression forces reopen before resolution and
after resolution/before promotion, checks that the journal contains pending
work, then requires completion without another write or synchronous drain. It
fails before the change and passes with it, alongside the existing backfill and
catalog-fencing regressions. The original CI's final read timeout is not by itself
proof that its entire history followed this same reopen path.

The new production data-restart case also exposed `AddressUnavailable` escaping
the lookup callback as `RuntimeBoundaryFailure`/HTTP 500. Known read-transport
failures now become `StorageReadTemporarilyUnavailable` before that boundary;
cancellation, timeouts, and unexpected defects retain their distinct meanings.
An injected failing transport verifies that normalization makes one attempt and
does not silently retry or return a missing document.
The pre-fix restart experiment failed 7/10 cases (three HTTP 500s and four
promotion deadlines); its native stacks and roots are retained. Startup also
returned `TableNotFound` from routed batch validation before invoking the writer.
That now retains the existing `Unavailable`/503 response with the explicit
`not_proposed_v1` outcome, rather than reporting an ambiguous HTTP 500. The
operation regression asserts the writer was never called on those failures.

The first corrected executable (`05ee0bf5c6`, SHA-256
`c0c7056ebc8594d31d6d59a5838fcd9f6578dab9024e676e5639152151b110a2`)
still failed **3/10 restart cases**: two second-document `WriterLocked` failures
and one promotion deadline. Its retained log also caught a shutdown use-after-free:
`PromotionRuntime.workerStep` called `GroupLeadershipSource.isLocalLeader` through
a freed `ManagedHttpHostService` (`0xaaaaaaaaaaaab1da`, process exit `SIGABRT`).
The provider shutdown barrier closed inline caches but skipped compiled owners,
whose workers survived until after Raft destruction. Compiled-owner quiescence
now fences admission, drains leases, and joins workers before destroying Raft or
provider callback contexts. Its regression holds an actual promotion callback
across shutdown using `std.Io.concurrent` and verifies admission stays closed.
Restart and final teardown now reject spontaneous crashes and retain failure
diagnostics instead of silently replacing or discarding the failed process;
intentional bounded-stop `SIGKILL` remains a crash-recovery case.

Compiled owner open also started resolver workers before reconciling the
authoritative resolver configuration. A worker could hold the resolver catalog
fence and make that configuration return `WriterLocked`. Open now defers those
workers until configuration completes, matching the resident-cache lifecycle.
Unchanged resolver refreshes also used the mutation fence and rewrote the
catalog. With a distributed promotion in flight this could return `WriterLocked`,
retire the owner, and repeatedly make promotion return `StorageBusy`. Exact
configuration equality now uses a short catalog read lock and skips allocation,
catalog persistence, and the replay mutation fence. Changed configurations still
require that fence. The catalog regression holds each replay mutex in turn and
requires an unchanged refresh to succeed while a changed refresh remains fenced.
The compiled-owner suite passes all ten cases with no skips or leaks. The
unchanged/changed resolver fence and both replay/reopen regressions also pass.
The corrected production executable from `256bb99782` (SHA-256
`58c637b81274785e5502f6f208fb45c2da90502295686e99c710b898ba54c99c`)
passed ten restart probes and the full local 200-case Autograph soak: 100 normal
and 100 under a 256-descriptor limit, with exactly 50 ordinary-resolution and
50 data-restart cases per profile, zero failures/errors/skips, and the same
executable hash after completion. These results precede the apply-lock handoff
fix above; they do not qualify a later executable or establish the cause of the
original CI read-timeout history.

The E2E poller now stops on unexpected HTTP errors, including 500, instead of
hiding them behind a generic promotion timeout. Only 503 with `Retry-After` and
transport timeouts/disconnects are retryable. Its original deadline still applies.
The first failure captures native stacks with a five-second per-process limit;
teardown reuses that snapshot and stores it with the retained server root.
macOS uses `sample`; the disposable Linux launcher opts test children into
debugger attachment and the scheduled job installs GDB. Poll deadlines that
fall below the request floor still report pending keys and graph diagnostics.

The full nightly/manual soak now runs the production Autograph scenario under
both normal and 256-descriptor limits, two concurrent workers and 25 repetitions
per case per profile: **200 fresh six-process clusters**, split equally between
the original case and a case restarting all data nodes after the initial write.
Reports are distinct from the
200 restore repetitions and must contain the exact expected test names and
counts with no skips or failures. A failed normal profile does not prevent
collecting constrained-profile evidence, and either failure fails the soak.
The source revision and retained production executable identify the tested bytes.
Run locally against a prebuilt executable with:

```sh
SKIP_BUILD=1 ANTFLY_BIN="$PWD/zig/zig-out/bin/antfly" \
  ANTFLY_E2E_REGRESSION_REPORT_DIR=/tmp/autograph-soak-reports \
  scripts/ci/zig-e2e-autograph-soak.sh
```

The poller/helper regressions pass locally (29 tests), as do the six regression
orchestration tests, including profile isolation and failure propagation.
Post-fix production repetition and Linux qualification results are pending; adding a
soak is not proof that the progress failure is fixed.

## 2026-09-15: distributed-data materialization fails during exact replay

[The second full soak](https://github.com/antflydb/antfly/actions/runs/34927431365)
failed distributed-data history 6 during replay with
`VoprIndexMaterializationIncomplete`. The recording completed four transitions
without property failures. The retained trace is
`history-6-b54cda599d6a3d7e.voprtrace`, seed `13064056697522896254`; its original
Linux executable SHA-256 is
`163c27a17d1a0943f766840998bc9678fc88300578f126a4fea717a3700128fa`.

The unchanged trace passed 30 native macOS replays, 25 emulated Linux replays,
and [200 fresh-process native Linux replays under GDB](https://github.com/antflydb/antfly/actions/runs/34993873598).
The retained log contains all 200 completed four-transition replays. Those
passes do not explain or clear the original failure. The materialization helper
uses 40 immediate repair polls; durable repair backoff uses host time. A normal
activation-budget miss became a failed attempt with roughly 30 seconds of
backoff, which the helper does not wait out. Budget exhaustion now returns the
existing cooperative-yield result and keeps the durable candidate runnable;
missing replay progress and actual storage errors retain failure handling.
A deterministic regression gives activation less than its fixed publication
reserve, requires no failure streak/backoff, then completes the same candidate
with a sufficient budget. All four focused activation, coverage-failure, and
candidate-resume tests pass locally. This independently validated scheduling
fix is not proven attribution of this particular CI history.
Native diagnostics now retain repair phase, attempt,
failure streak, retry deadline, and last error when a replay fails.

## 2026-09-15: standby/scaling corpus runner lost during cache publication

[Corpus job 104276235667](https://github.com/antflydb/antfly/actions/runs/34927431365/job/104276235667)
completed replay/merge validation and uploaded the small diagnostics artifact.
The runner then lost communication during `actions/cache/save`; GitHub's check
annotation reports runner loss, and full artifact retention was cancelled.
Both scaling campaign shards passed. This is not evidence of a scaling assertion
or replay failure, and the available log does not identify why the runner died.
The workflow now retains the complete merged corpus before publishing its working
cache. Cache publication still fails the job on failure; it is not treated as a
passing seeded soak. The second full run failed independently in distributed-data.

## 2026-09-14: soak qualification exceeds declared compiler memory (#704)

[Qualification job 104248350481](https://github.com/antflydb/antfly/actions/runs/34927431365/job/104248350481)
on `0f56ccd77e` reports a Linux ReleaseSafe compilation peak of
13,255,065,600 bytes against a declared upper bound of 7,516,192,768 bytes
(7 GiB). The production DataServer VOPR artifact uses
`productionVoprCompileMaxRss`; the separate background-adapter artifact has its
own declaration. The eight standby lifecycle tests and seven DataServer runtime
tests shown in the log passed with zero skips, failures, or leaks. The completed
qualification job succeeded with 27/27 build steps. Zig 0.16 records this RSS
overrun as a diagnostic without returning `MakeFailed`; it is an inaccurate
build-scheduling reservation, not a test failure or a killed compiler.

The shared production-owner compile reservation now uses 16 GiB on Linux, with
headroom over the observed 12.35 GiB peak; macOS keeps its existing 18 GiB
reservation. This allows Zig to account for the actual compiler footprint when
admitting concurrent build steps. It does not alter production memory limits,
suppress resource checks, reduce test coverage, or retry away the failure.
The revised reservation passed [Linux qualification on `43b79fc10b`](https://github.com/antflydb/antfly/actions/runs/34993639738/job/104465497701).
The second full soak failed separately in its distributed-data replay;
qualification itself passed.

## 2026-09-14: standby/scaling read fails after sibling merge (#704)

[Adversarial scaling job 104181278895](https://github.com/antflydb/antfly/actions/runs/34899531048/job/104181278895)
on `919fa9d124` completed two histories with one clean and one failed history.
Both received exact replay; there were no harness errors or replay divergences.
The failure was a document lookup returning generic HTTP 500 after the automatic
sibling merge, immediately before scale-in. `history-completes` and
`promotion-and-topology-converge` failed; `owners-quiesce` passed with zero live
tasks, files, or sockets after teardown. This is a failed soak, not a timeout.

The failed history is `history-1-9e3779ba20f0d116.voprtrace`, seed
`11400714822035230998`, mutated at choice 251091 from the first history of
campaign seed `2712032513`. The original Linux executable SHA-256 is
`8d44bc8e6ba94292e93b68b57369aa6f74bd31fceb261d49a7aaf85e9d448a5c`.
The trace, mutation schedule, flight recording, and reports are retained in
run artifacts `vopr-run-ha-scaling--1` and `vopr-diagnostics-ha-scaling--1`.
Several earlier merge observations report `UnknownGroup`; that alone does not
identify the later lookup error.

The [original Linux diagnostic replay](https://github.com/antflydb/antfly/actions/runs/34913890221)
captured `ReadIndexTimeout` in the HTTP transport's concrete error argument for
`GET /db/v1/tables/tenant_b_docs/documents/tenant%3Aq`. The adversarial suffix
advances time by roughly a minute while tasks remain pending. The production
Raft read barrier correctly expires without returning an unproven read, but
the public lookup adapter omitted this availability error and emitted generic
HTTP 500. This request reads the separate tenant table; the preceding sibling
merge and its `UnknownGroup` observations do not establish a merge defect.

Test-linked executables suppress HTTP stderr to preserve Zig's test protocol.
The debugger now captures the transport function's concrete error argument,
numeric value, and request without changing the retained executable or its
schedule. An ingress-only breakpoint missed inlined code; a source-line
breakpoint exposed the error-union discriminant rather than the error payload.
Symbol-based transport diagnostics avoid both problems. Native macOS replay of
this Linux trace diverges at the first scheduled task and is not reproduction
proof.

The fix preserves `ReadIndexTimeout` through typed internal read operations,
internal HTTP responses, and client decoding. Public lookup and pre-stream scan
return HTTP 503 with `Retry-After: 1`, using their existing group-unavailable
response. Explicit request deadlines remain HTTP 504, and unexpected storage
errors still propagate as failures. The Raft barrier's quorum/apply requirement
and timeout budget are unchanged.

The focused public-read regression failed on the baseline with
`ReadIndexTimeout` (one executed test, zero leaks). It now exercises lookup and
scan, the typed internal lookup and its HTTP/client round trip, explicit
deadline handling, and fail-closed corruption handling. Existing production
DataServer VOPR coverage also forces a real read-barrier timeout by pausing Raft
drivers and verifies that no waiter remains. The updated public API parity suite
passed all 192 tests with zero skips, failures, or leaks. Final Linux CI and
full-soak qualification remain pending.

## 2026-09-14: restore publication races storage-owner users (#704)

[CI job 104174381057](https://github.com/antflydb/antfly/actions/runs/34899514358/job/104174381057?pr=704)
on `919fa9d124` failed `test_cluster_restore_modes` during cluster overwrite
restore. The public restore job reported `InternalFailure`; the retained server
log showed staged repair complete, then `table restore failed phase=execution
class=StorageBusy` and `cluster restore failed phase=materialization
class=StorageBusy`. This is separate from the earlier transaction HTTP 503.

The unchanged native executable reproduced the same signature **5 times in 100
fresh-server repetitions** (four workers, 25 iterations each; 95 passed).
Binary SHA-256: `ae0f4294a59fd9eb0b90a58e5516e927927fa47d456bdd9a89abfe5d109dbdce`.
Evidence is retained in `/tmp/pr704-919-restore-modes-soak.log`,
`/tmp/pr704-919-restore-soak-evidence.json`, and its five retained server roots.
The failing immediate-busy retirement function and restore caller were identical
to `origin/main`; the retirement check dates to #536. This establishes a
pre-existing handoff defect, but no matched base-binary reproduction establishes
whether #704 changed its probability. A passing transaction soak is not evidence
that this restore race is fixed.

Table generation admission does not drain every storage-owner borrower: status
and maintenance can already hold leases. Publication previously returned busy
immediately, and retirement alone would allow a replacement owner to open after
the last old entry closed but before namespace exchange completed.

Publication now closes group admission under the owner registry mutex, drains
existing users and owner shutdown using the operation's I/O and cancellation,
and retains the gate across physical publication, catalog commit or rollback,
and staged snapshot destruction. Timeout/cancellation releases the gate while
existing leases keep their owners alive. Ordinary reads encountering the gate
return temporary unavailability so callers can refresh their catalog descriptor.
The same contract covers restore reconciliation and Raft snapshot installation.

The deterministic compiled-owner regression forces a held read, status, or
maintenance lease to release at the first drain wait. It checks successful
handoff, exclusion after the entry disappears, and timeout/cancellation cleanup.
The production restore composition runs real maintenance and status workers
across portable/native restore, rejected publication, and reconciliation. The
scheduled VOPR workflow also runs repeated public cluster restores with
concurrent document reads and index-status observation, retaining failure roots
and the exact executable. These production stress runs complement replayable
VOPR campaigns; their OS schedules are not replay traces.

The new concurrent HTTP case also failed on the unchanged executable with
HTTP 500 and `GenerationTransitionActive` in the server log
(`/tmp/pr704-publication-observer-baseline.log`). Lookup and pre-stream scan
admission now translate that retryable identity and bounded `StorageBusy` into
the existing structured storage-unavailable 503 with `Retry-After`. The response
contract regression covers both routes. The stress test rejects arbitrary 500s
and unclassified 503s and does not retry a restore request.

Native post-fix validation passed all 56 build steps, six compiled-owner tests,
and 192 public HTTP contract tests. Twenty additional owner-suite repetitions
passed all 120 test executions without leaks. The public restore soak passed
**200/200 fresh-server cases**: 100 original mode tests and 100 with concurrent
read/status observers, four workers, no retries of failed restores and no skips.
All used executable SHA-256
`410911f311beeec02a44d723357e6f4d17edd5f81d28f325d3bc30005450f89a`;
the digest was checked again after the soak. Evidence:
`/tmp/pr704-publication-focused-build-approved.log`,
`/tmp/pr704-publication-owner-soak/verified.json`, and
`/tmp/pr704-publication-final/verified.json` with individual JUnit reports.
Linux CI and the separately reported scaling finding remain outstanding.

## 2026-09-14: standby VOPR promotion overtakes apply (#704)

[Full soak 34878521929](https://github.com/antflydb/antfly/actions/runs/34878521929)
on `03e1a8e0d2` stopped both standalone standby shards with
`PromotionRequiresForce`, at histories 171 and 759. The other six campaign
shards and all four corpus jobs passed. Those partial results do not qualify
the full soak, and no second required-seed run was started.

The lifecycle scenario obtained a non-forced fence after catching up, then
allowed another primary append and standby receive before promotion. The
production standby correctly rejected promotion with unapplied received data;
the scenario propagated that expected refusal as a fatal harness error.
The scripted regression reproduces the same error, then covers rejection,
restart, another rejection, apply, successful promotion, and former-primary
assessment with exact replay. Explicit forced promotion remains covered too.

The scenario now records the refusal as a rejected transition. A separate
safety property checks the applied-tail requirement and verifies rejection
preserves identity and progress. Unexpected errors still escape. Production
promotion rules and force authorization are unchanged. Scenario version 3
marks the changed trace/property contract, and the reusable qualification job
runs the standby lifecycle regressions before admitting full campaigns.

The initial scripted regression failed with `PromotionRequiresForce`; after
the fix it passed with exact replay and no leaks. Evidence is retained in
`/tmp/pr704-promotion-regression-red.log` and
`/tmp/pr704-promotion-regression-green.log`. New complete soaks must validate
the corrected revision; earlier successful shards are historical evidence.

The same revision's ordinary CI independently failed
`test_session_stage_transform_commit` with HTTP 503 (446 other tests passed).
Its retained server log contained no underlying error, and 30 unchanged-binary
native repetitions passed. The transaction helper now retains the response
body through its existing error checker; it does not retry commits or treat
503 as success. There is no evidence establishing a common cause with the
standby rejection or proving #723 resolves that transaction failure.

## 2026-09-14: producer readiness snapshot loss (#723)

[The Antfly E2E job on #723](https://github.com/antflydb/antfly/actions/runs/34801864332/job/103853126220)
ran `07bb51bdf0a063ad0f4782e35a48669b6c253f2c` and reported one failure,
445 passes, and five skips. The four previously investigated cases passed.
`test_embedding_producer_registry_rejects_orphans_and_owner_mismatches`
timed out requiring `enrichment_runtime.embed_batches_completed > 0`, despite
one indexed, query-visible document and complete durable source coverage.
Its aggregate readiness also reported `source_observation_incomplete` while
the individual shard reported complete publication without its source list.

The counter is scoped to the enrichment runtime, not durable completion.
Catalog descriptor changes can close/reopen an owner, and restart necessarily
replaces it. The exact CI replacement interleaving was not captured; the
unchanged executable passed 20 isolated repetitions locally. The test now
requires complete readiness for the configured artifact source and successful
semantic queries through both the producer index and its artifact consumer,
before and after a forced process restart. It does not require replaying
already-published artifacts just to increment a new runtime's counters.

The readiness contradiction has a deterministic production cause:
`runtime_status.cloneDBStats` omitted `DBIndexStats.source_replay`. That copy
runs when an ABI response is detached from its JSON parser and when the
control-plane cache publishes or returns a snapshot. Source names, replay
watermarks, repair facts, and observation counts now retain independent
ownership across these copies. The regression covers complete, lagging, and
failed sources, frees the original backing arena, destroys the intermediate
cache, and checks the retained result. Allocation-failure injection checks
partial-clone cleanup. It belongs to the existing runtime snapshot test family.

The same native reproduction exposed poisoned diagnostic labels (`0xaa` byte
arrays) after the ABI parser was freed. Enrichment phase/checkpoint/stall and
index repair/checkpoint labels now resolve to immutable labels, using the
owner enums where available and an explicit `unknown` for unsupported peer
values. This keeps native DBStats' borrowed-label contract and does not add
allocations to status merges.

Before the fix, the new unit regression reported **expected three sources,
found zero**; the strengthened E2E case timed out on the original executable
with `source_observation_incomplete`. Logs are
`/tmp/pr723-source-negative.log` and `/tmp/pr723-source-e2e-before.log`.
After merging `origin/main` at `3554f82101` (merge `19162ef60f`), the native
macOS arm64 Debug build passed all 49 steps and six focused owner/snapshot
checks. The binary SHA-256 is
`d7dce3ac5b091d32acad40c2b5fd99eb4a91b01b22c73dbc1a248dce90698eb2`.
It passed **100/100 repetitions of each of the five flake scenarios, 500/500
total**, with four regression workers and 25 iterations per worker. There
were no failed-case retries, skips, or timeout increases. The complete three
E2E modules passed **54 tests**, with only the existing opt-in million-chunk
scale case skipped, using four pytest workers and two process slots alongside
the soak.

Expanding validation to the snapshot/source-readiness family exposed two
pre-existing fixture inconsistencies: the lifecycle-continuity test expected
cached lifecycle to override an explicit `action_required` failure, and the
replay summary expected two LSM samples while providing only one. The former
now tests both ordinary continuity and authoritative failure; the latter
supplies both intended samples without relaxing the expected totals. Both
are included in the existing derived-coverage suite. All **114 checks** in
the expanded linked owner/snapshot/source-readiness run passed, including
allocation-failure injection, with no leaks. These last fixture-only changes
do not change the runtime code used by the soak. Temporary validation targets
were removed.

Evidence: `/tmp/pr723-merged-build.log`,
`/tmp/pr723-merged-soak-500.log`, `/tmp/pr723-merged-modules.log`, and
`/tmp/pr723-merged-snapshot-suite-fixed.log`. The initial expanded run is
retained at `/tmp/pr723-merged-snapshot-suite.log`. Linux validation remains
CI's responsibility; passing native soaks do not establish the precise
interleaving of the original CI counter reset.

### Snapshot review follow-up

A review of the same ABI copy path reproduced two further defects. After
replacing the source buffer, both table and index `last_merge_error` contained
`xxxxxxxxxxxx` instead of `InvalidChunk`. A snapshot with schema epoch 7,
catalog generation 8, and an enabled two-worker graph-metric runtime returned
zero/disabled defaults. The original reproductions are retained in
`/tmp/pr723-review-snapshot.log`.

Table and index merge error names, and the graph-metric runtime error name,
now contain their own bytes using the existing inline status-text convention
(256-byte identifiers; oversized JSON values are rejected). JSON remains a
string, or null for the optional graph-metric diagnostic. Copies, retained
artifact visibility, and aggregates need no new allocation or ownership
transfer. The C ABI's JSON projection retains the value too.

`LocalTableRuntimeStatus`, `DBStats`, and `DBIndexStats` cloning now preserve
value fields by default. A recursive compile-time pointer check requires an
explicit ownership override for pointer-bearing fields. Schema state labels
are re-interned through the catalog enum. This preserves columnar maintenance,
schema/catalog/row-format facts, visibility, and graph-metric runtime facts
without maintaining a second list of every counter.

The regression seeds scalar fields, compares the complete snapshot after an
ABI JSON round trip, destroys the response/parser and intermediate cache,
and checks diagnostic ownership through allocation-free copies and JSON
serialization. Allocation-failure injection also covers optional algebraic
candidate/progress diagnostics; their partial-clone cleanup is now complete.
All **111 snapshot checks** passed with no leaks, including existing
allocation-free commit checks (`/tmp/pr723-snapshot-focused-final.log`). The
repository's locked Ruff formatter and Python format check also pass; Black
is not the repository formatter.

The final linked Debug build passed **49/49 steps**, and the expanded
owner/snapshot/source-readiness suite passed **116 checks**, with no leaks.
The three affected E2E modules passed **54 tests** with the existing opt-in
scale case skipped. Evidence is in `/tmp/pr723-snapshot-fix-build-final.log`
and `/tmp/pr723-snapshot-fix-modules.log`. The native macOS arm64 executable
SHA-256 is `cfe5769abc4a2a3fe11dbc2fd1024e903da1449ff0f30409976126dd893be68a`.
Temporary validation target/root changes were removed.
The same executable passed **500/500 flake repetitions**, exactly 100 per
scenario, using four workers and 25 iterations each. The complete E2E modules
ran concurrently with the beginning of the soak; there were no failed-case
retries, skips in the soak, or timeout increases. The final soak evidence is
`/tmp/pr723-snapshot-fix-soak-500.log`. `origin/main` was fetched again and was
already included before this push. Linux validation remains CI's responsibility.

### Synthetic refresh preservation

The next review reproduced loss after a live → synthetic cache publication:
schema epoch 7, catalog generation 8, columnar passes 5, visibility hits 6,
and an enabled two-worker graph-metric runtime became zero/disabled, while
document counts survived (`/tmp/pr723-review-current.log`). The clone was
correct; the synthetic merge still restored only a list of selected fields.

The merge now retains the complete owned table stats from the cached owner,
then applies catalog index membership using the existing incarnation fences.
Fresh disk observations remain independent, and stale metadata cannot assert
latest-target completeness. It reuses the existing two clones and transfers
ownership without adding allocations. Both unused placeholder diagnostics
and removed cached indexes are freed by the retained temporary snapshot.

The transition regression also reproduced a second refresh erasing an idle
owner after its runtimes became disabled (`/tmp/pr723-synthetic-idle-negative.log`).
Preservation now recognizes the physical runtime-owner identity rather than
requiring nonzero workload counters. Root invalidation still discards it.

The existing snapshot family now covers all table values, heap-backed resolver
and index diagnostics after both source lifetimes end, index additions/removals,
repeated synthetic refreshes, live updates that decrease or clear counters,
same-root catalog fencing, replacement roots, and rejected delayed publications.
All **112 snapshot checks** pass, including allocation-failure injection and
existing allocation-free publication checks, with no leaks
(`/tmp/pr723-synthetic-fix-unit-final.log`). No test target or filter was added.

The final native macOS arm64 Debug build passed **37/37 steps**
(`/tmp/pr723-synthetic-fix-build-final.log`). Its executable SHA-256 is
`643f68bb54b5a5b0d6f1c29d6df19ed5d4401757eaacee2e412e162a6828f8cc`.
The same executable passed **54 E2E tests** (the existing opt-in scale case
skipped) across the three affected modules, and **500/500 flake repetitions**:
exactly 100 for each of the five scenarios, four workers and 25 iterations.
The modules ran concurrently with the start of the soak. There were no failed-case
retries, soak skips, or timeout increases. Evidence:
`/tmp/pr723-synthetic-fix-modules.log` and
`/tmp/pr723-synthetic-fix-soak-500.log`. The latest fetched `origin/main` was
already included. Linux validation remains CI's responsibility.

## 2026-09-13: overlapping storage owner E2E failures (#626, #722)

[PR #626's Antfly E2E job](https://github.com/antflydb/antfly/actions/runs/34781521711/job/103794277696)
ran merge revision `62181d9cbae16aa1599c7e4c9202fcabf9aa0dbc` and reported
**two failures, 444 passes, five skips** on Linux with Python 3.12.3:

- `test_artifact_coverage_terminal_outcomes_by_policy_after_restart` lost the
  partial-policy index's runtime observation after restart: zero reported
  groups, one missing group, `runtime_unavailable`, and `missing_group`. This
  matches the signature in both #722 CI attempts and the retained native
  reproduction described in [the E2E investigation](e2e/FLAKES.md).
- `test_stateful_external_embeddings_index_detail_supports_packed_ingest_and_query`
  failed its batch request with HTTP 500 `RuntimeBoundaryFailure`. The server
  first logged `IndexNotFound` in the `full_text_index_v0` derived worker at
  journal sequence 1, then an untransportable `StorageKernelFailure` from
  `commit_batch_with_cancellation`. This is an additional failure, distinct
  from #722's query callback returning an untransportable `StorageBusy`.

The #626 log is retained at `/tmp/pr626-job103794277696.log`. Repeated
`EmptyTargetedIndexObservation` messages precede the batch failure. They are
evidence to investigate, not proof that reconciliation caused it. The restart
signature's recurrence on another branch does not by itself establish its
introducing commit. The deterministic follow-up investigation is recorded
below on `codex/maintenance-e2e-fixes`.

The additional batch case belongs in the existing regression loop alongside
the three #722 cases; preserve the failing process roots and test the containing
`test_index_lifecycle.py` module as well, because CI shares that process between
tests. Do not retry committed writes or relax coverage/deadline assertions to
hide these failures.

The unchanged merged executable passed **100/100** isolated repetitions of the
additional packed-ingest case (four workers, 25 repetitions). Evidence is
`/tmp/pr626-local-packed-100.log`; this does not reproduce the shared-module
history or prove the worker is race-free.

The follow-up has established these defects with deterministic negative controls:

1. Warmup retired a resident owner installed by another operation. The linked
   owner regression expected one owner after delayed warmup and found zero.
   Transient cleanup now retires only the exact, still-leased owner it created,
   and preserves owners adopted by another operation. The same rule closes
   the release-then-retire race in transient reconciliation.
2. Compiled structural reconciliation returned its mutation result without
   appending an observation to the control-plane publication batch. An unchanged
   empty dense index reproduced eight `EmptyTargetedIndexObservation` retries
   and failed the existing five-second bound. Reconciliation now samples status
   under its exclusive owner lease and passes it through the existing root,
   catalog, and targeted-activation fences. A busy observation retains the group
   for another bounded quantum; it cannot acknowledge completion without facts.
3. Text publication planning reported an ordinary same-instance projection
   revision change as `IndexNotFound`. The regression changes an empty index's
   schema between capture and planning and failed at that exact check. Both
   JSON and parsed-document planning now refresh the context while holding the
   analysis lease used to construct the plan. Admission still revalidates it;
   delete/recreate replacement remains rejected. This explains a path from a
   legitimate schema refresh to a terminal derived-worker failure without
   treating every `IndexNotFound` as retryable.

`git blame` traces the warmup and compiled-reconciliation defects to
`332817c668f` ([#536](https://github.com/antflydb/antfly/pull/536)), and the
projection revision check to `aefe3bad405`
([#502](https://github.com/antflydb/antfly/pull/502)). Both are ancestors of
`c1a39a3aeb65e62d9e4494c5b785a028e4520c5d`, #722's unchanged base, so these
demonstrated defects predate #722 even though its base soak did not expose them.

Status collection also preserves independent sibling observations when another
group is cold or busy, probes only the requested group's owner, and retains
refresh debt when that probe is busy. `StorageBusy` and `StorageKernelFailure`
retain their semantic identities across the callback ABI; public query
contention is HTTP 503 rather than HTTP 500. Mutation retry semantics are
unchanged.

Negative-control logs: `/tmp/maintenance-owner-before.log`,
`/tmp/maintenance-owner-activation-before.log`, and
`/tmp/maintenance-text-before.log`. The warmup and activation fixes passed the
linked owner suite with no leaks. A fresh batch containing those two fixes
passed **100/100 per scenario, 400/400 total**, with the four unchanged E2E
tests and no retries or skips. Its executable SHA-256 is
`988f3c8414a168f76a1ffc9a75b79d94c8fd091e1555b9d209e657718dfaa010`;
output is `/tmp/maintenance-followup-e2e-100.log`. That executable predates the
text-planning and observation/error-transport changes; final validation of the
complete change set is recorded below. These deterministic defects and passing
soaks do not establish the precise interleaving of every original CI failure.

Final native Debug focused checks passed: the linked-owner suite (two tests),
text publication (two substantive tests plus two import checks), public query
availability (one table-driven test plus six import checks), the compiled
busy-owner refresh-debt regression, and all 12 stable runtime error ABI tests.
No leaks were reported. Temporary focused build steps used for the negative
controls were removed; the regressions remain in their existing owner suites.
The final binary SHA-256 is
`de2c5f7e99bb3074ed3238ed7e64f9b230387e949af6b0559b551ce1d6503519`.
All **54/54** tests in the three affected E2E modules passed using four pytest
workers and two process slots, preserving CI's shared-module fixture history.
Evidence: `/tmp/maintenance-complete-build-tests-final.log`,
`/tmp/maintenance-data-observation-tests.log`,
`/tmp/maintenance-error-abi-tests.log`, and `/tmp/maintenance-final-modules.log`.
The fresh final-binary batch passed **100/100 per scenario, 400/400 total**,
with four workers and no retries, failures, or skips. The log is
`/tmp/maintenance-final-e2e-100.log`; per-scenario counts and the executable
hash are recorded in `/tmp/maintenance-followup-validation.json`. These are
native macOS arm64 Debug results; Linux CI validation remains pending.

### Review follow-up: transient borrowing and activation assertions

Review of #723 found that an observational lease marked a transient owner as
adopted. A status probe overlapping warmup could therefore retain an idle DB
indefinitely. Residency now belongs explicitly to foreground admission or
retained background debt. Status and maintenance leases only borrow the owner;
transient cleanup records retirement and the final borrower closes it. New
observations cannot prolong pending retirement, while foreground admission can
adopt the same owner before the last borrower releases it. Prepared Raft writes
also adopt residency. Restore-marker reads use the same pinned retirement
protocol, eliminating their remaining release-then-retire-by-group path.

The linked owner suite exercises five deterministic lease histories: a status
probe finishing before warmup cleanup, a probe spanning cleanup, maintenance
spanning cleanup, foreground adoption, and prepared-Raft adoption. It also
checks that completed leases cannot retire a replacement owner. The activation
regression now requires a newer, fresh, target-complete publication for group
7001 and the expected dense-index generation/configuration hash, instead of
accepting an idle worker alone. These regressions run in the existing storage
owner aggregates.

Restoring observational adoption in a temporary negative-control build made
the new lease-history regression fail at `expect(!original.resident)`; the
other three checks passed, and no leaks were reported. The corrected source
was restored byte-for-byte. Evidence:
`/tmp/maintenance-review-negative-control.log`.

The restored revision passed **4/4** linked owner checks with no skips,
failures, or leaks (`/tmp/maintenance-review-owner-final.log`). A fresh soak
again passed **100/100 per scenario, 400/400 total**, with four workers and no
retries or skips (`/tmp/maintenance-review-e2e-100.log`). The three complete E2E
modules passed **54 tests**, with the opt-in million-chunk scale test skipped,
using four pytest workers and two process slots
(`/tmp/maintenance-review-modules.log`). Formatting and diff checks passed;
the temporary validation build step was removed.

The native macOS arm64 Debug binary SHA-256 is
`1bab589bbe0948595bdd213bec52f399f61b1f72fd24da1ee59999361c08de25`.
Per-scenario counts are in `/tmp/maintenance-followup-validation.json`.
Linux CI must validate this pushed revision independently.

## 2026-09-12: combined unit build/test budget terminates progressing DB-core (#716)

[The Linux unit job](https://github.com/antflydb/antfly/actions/runs/34714680318/job/103609868433)
started its combined build/test step at 19:49:03 UTC, but DB-core execution only
started at 20:41:19. The hard watchdog terminated the step at 20:49:05 after
3,602 seconds. The remaining partition advanced from test 512 to 539 in the
last 45 seconds; the category partition had already passed. No assertion
failures or OOM kills were reported. The physical-usage tests and original
physical-churn benchmark passed, including both GC configurations.

Increase the combined hard limit from 60 to 90 minutes while preserving the
30-minute idle watchdog and 120-minute outer job limit. The build-tooling
contract tests check these defaults and their ordering. This gives cold builds
execution headroom; it does not establish that a future run will complete or
replace future work on phase-specific budgets and per-partition progress.

## 2026-09-12: physical-churn measurement races obsolete-file deletion (#503)

[PR #503's Linux unit job](https://github.com/antflydb/antfly/actions/runs/34672950333/job/103497870209)
failed only `relational columnar production LSM physical churn benchmark` with
`FileNotFound`; its DB-core partition reported 1,140 passes, five skips, one
failure and no leaks. Build-budget checks passed and the cgroup recorded no OOM
kills. The original log does not identify the throwing filesystem call, and an
isolated unmodified local run passed both GC configurations.

The old size helper walked mutable active/obsolete inventories without a lock
or ownership lease and required every obsolete file to exist. Obsolete cleanup
intentionally unlinks off-lock before retiring its ledger entry, so even taking
the writer mutex around that helper would not make this contract sound.

The follow-up uses the backend-owned physical usage observer described in
[LSM version publication](../docs/design/lsm-version-publication.md#physical-usage-observation).
A deterministic storage hook forces capture, obsolete unlink, live-ledger
replacement and stat in that order. Separate checks preserve errors for missing
active files and denied I/O, force compaction while active SSTs are pinned, and
verify reclamation after the observer releases its pins. Tests also cover WAL
accounting, duplicate active/obsolete membership, overflow and allocation-failure
cleanup. This reproduces and closes the measurement race without claiming that
the original untraced CI failure proves the same throwing call.

Validation on the follow-up: the focused physical-usage, ledger-reclamation and
expired-obsolete cleanup suite passed 10 tests with one existing ReleaseFast-only
benchmark skipped and no leaks. The original production physical-churn test
passed three consecutive local runs (six GC scenarios: 0% and 50% per run).
These are native Debug correctness checks, not a throughput improvement claim.

```sh
cd zig
zig build lsm-backend-test -- --test-filter 'lsm physical usage' --test-filter 'ledger reclamation' --test-filter 'lsm backend removes expired obsolete'
zig build antfly-storage-db-test -- --test-filter 'relational columnar production LSM physical churn benchmark'
```

## 2026-09-11: main merge retains observation proofs for delayed notifications (#694)

Merging `aefe3bad4` exposed an interaction between the PR's redundant-notification
suppression and main's retained target-observation authority. A notification
for an already-observed source returned without recording a requirement. A
later snapshot marked pending could then erase the accepted observation.
Main's `accepted target observation survives late snapshots but not new commit
fences` regression failed on the initial merge at the settled group assertion.

The redundant-notification path now records the accepted source proof without
advancing the notification fence. The equivalent exact-index path also retains
its accepted proof. New targets and catalog fences still invalidate completion;
allocation failure retains the existing conservative invalidation behavior.
The merge keeps the PR's batched consistent coverage reads and main's pinned
snapshot regression, with packed-row-aware cardinality fallback.

Merged native Debug validation passed all 179 derived-coverage API tests,
92 DB coverage tests, and two packed-row/range-cardinality regressions, with
zero skips, failures, or leaks. The API suite ran with local listener access;
the initial sandboxed run also rejected two HTTP listener binds. SDK OAPI tests,
`make generate` (including `make build-antfarm`), `make fmt`, and whitespace
checks passed. Evidence uses `/private/tmp/ci694-main-merge-*`.

## 2026-09-11: last dense bulk-lease fixture races physical publication (#694)

While investigating [the Linux DB-core abort in run 34658598331](https://github.com/antflydb/antfly/actions/runs/34658598331/job/103457081854),
the complete local Linux Debug partitions exposed a separate assertion in
`db last external dense bulk lease finalizes covered rebuilding generations`.
The fixture failed in **10/10 full runs** on `17eed29b0`: two isolated containers
ran five repetitions each, one with the default allocator and one with
`MALLOC_PERTURB_=165` and the glibc tcache disabled. All ten runs passed the
watermark-reopen test that aborted in CI; their only failure was this assertion.

The fixture used best-effort online publication before creating a rebuilding
checkpoint and expecting the last external bulk lease to certify it immediately.
Online publication can leave the asynchronous physical checkpoint build pending.
Logical embedding coverage alone does not establish that publication boundary.
The fixture now calls `publishVectorBlockBasesAtStableTip` before introducing
the rebuilding checkpoint. Its immediate completion, clean status, generation
increment, and independent query-readiness assertions are preserved. Production
publication, scheduling, and timeout behavior are unchanged.

Local full-partition runs mount the repository read-only at `/workspace`, use
`/workspace/zig` as their working directory, and provide a writable `.zig-cache`.
This makes the split-replay and WebP fixture files available; the initial
reproduction's missing-file failures were harness errors.

The corrected fixture passed **100/100 native macOS Debug** and **100/100 Linux
x86_64 Debug** repetitions, with no failed-case retries. Evidence:
`/private/tmp/ci694-bulk-lease-fixed-native.log` and
`/private/tmp/ci694-bulk-lease-fixed-linux/results.json`.
The full Linux Debug DB-core run then passed both CI partitions: **1,116 passed,
eight skipped, zero failed, zero leaked**, exit 0 in 144 seconds. It used the
CI selection filters with the corrected fixture mounts and no allocator
instrumentation. Evidence: `/private/tmp/ci694-db-core-fixed-validation`.
`zig fmt --check` and `git diff --check` also passed.
Run the focused regression from `zig/`:

```sh
zig build antfly-storage-db-test -Doptimize=Debug -- \
  --test-filter 'db last external dense bulk lease finalizes covered rebuilding generations'
```

The original CI `munmap_chunk(): invalid pointer` abort remains unreproduced;
this fixture correction does not establish a fix for that memory error.
Before-fix evidence is retained in `/private/tmp/ci694-db-core-repeat-{1,2}`.

## 2026-09-11: forwarding-retirement native Debug soak passed (#694)

The executable built from `c11eead5aabe7027e4a84498bbd08489b580a89e`
passed **100/100 quickstart**, **100/100 three-by-three backup/restore**, and
**100/100 retry-exhaustion/restart**: 300 executions, zero failures, zero
failed-case retries, exit 0. Four workers each ran 25 repetitions of all three
scenarios. The native macOS Debug build completed 33/33 steps.

The run started at 20:53:06 UTC and finished at 21:34:31 UTC on September 11
(41m25s including the review pause). The four loop workers were paused during
the fresh PR review; their first quickstart executions finished normally, and
further iterations resumed after the review completed at 20:56:42 UTC.
Binary SHA-256:
`1b5262bc4d1246e9a77589d7f4d53984d19f71e065b36121acab2cb38f281386`.
Manifest, review record, combined worker output, and exit status are retained
under `/private/tmp/ci694-retirement-native-soak-c11eead5a`.

The subsequent main merge and release-test fix through `c1200f998` changed
container configuration, test code, and documentation, not the native runtime
sources exercised here. These results do not certify the container image or
resolve issue #705's separate progressive-index heap-corruption workload.

## 2026-09-11: release driver-discovery test depends on CI Python loader paths (#694)

SDK CI run `34645983534`, job `103416766382`, passed formatting and SDK
validation but failed release packaging in
`test_dlopen_finds_driver_in_injected_mount`. CI tested the merge with main's
`2bd96e33e` (#707). The test replaced `LD_LIBRARY_PATH` with fixture directories
and then launched the toolcache Python, which exited 127. CI had supplied its
Python library directory through that same environment variable; the probe
therefore also depended on the interpreter's native-library installation.

The test now compiles a minimal native `dlopen`/`dlsym` probe and executes it
with only the mapped container paths. It checks the fake driver's return value
and verifies failure when those paths are removed. Loader errors are included
in assertion diagnostics. No GPU, host driver, inherited Python library path,
retry, or runtime-image workaround is needed.

Validation: both tests passed in a local Linux x86_64 container, including the
missing-path negative control. The macOS release suite passed (12 packaging
tests and 114 release tests, with the Linux-only probe skipped), as did
`make generate`, `make build-antfarm`, and `make fmt`; generation produced no
tracked changes. Logs: `/private/tmp/ci694-runtime-container-linux.log` and
`/private/tmp/ci694-release-mac-fixed.log`. This changes test code only; the
native Debug runtime soak built from `c11eead5a` continues independently.

## 2026-09-11: forwarding retirement admission and dense-finalization fixture (#694)

The forwarding review gap below is fixed by checking the native executor's
busy count under its scheduler mutex after acquiring a request lease. Admission
accounts for all still-busy tasks (including completed requests still retiring)
plus the full remaining grant of every active lease. Already-submitted work is
conservatively counted twice so an admission cannot consume another request's
unused grant. Insufficient capacity returns the existing pre-transport overload
classification. This constant-time check adds no retries, polling, task churn,
or workers; the 252-worker aggregate and dedicated Raft lane remain unchanged.
Borrowed schedulers retain their own admission contract.

A regression pauses real Threaded group-task destruction after group completion,
releases the request lease, and verifies that a supported six-slot forwarding
lane rejects new admission until physical capacity is available. It fails on
the old implementation with `TestUnexpectedResult`, then passes with the fix.
The production path does not wait for executor retirement.
Negative control: `/private/tmp/ci694-forward-finalization-negative.log`.

The Linux failure in `db dense finalization owner drains requests queued during
publication` used an online, best-effort publication attempt as if it established
a synchronous native-generation boundary. CI logged both finalization attempts
before the posting checkpoint finished installing. The fixture now explicitly
publishes at a stable tip before asserting certification and acquires the
finalization claim through the normal claim function. The queued-request and
clean-generation assertions remain intact. Production background publication
and deadlines are unchanged. The original fixture passed 100 local repetitions;
that does not invalidate the recorded Linux interleaving.

Native Debug validation passed the 62-test DB/backend selection and 67-test
runtime selection (including real forwarding under saturated control/Raft
executors), with zero skips, failures, or leaks. The two targeted regressions
passed 100 repetitions each, 200 executions total. Evidence:
`/private/tmp/ci694-capacity-finalization-final.log`,
`/private/tmp/ci694-capacity-data-final.log`,
`/private/tmp/ci694-capacity-finalization-repeat.log`, and
`/private/tmp/ci694-finalization-original-repeat.log`.
The initial runtime test attempt could not bind its listener inside the sandbox;
the same binary passed with local socket access. Fresh binary soak and CI
acceptance for this follow-up are recorded separately when complete.

## 2026-09-11: combined native Debug acceptance and remaining review gap (#694)

The Debug executable built from `cdc9ffcc8c4d0ed9020564427694173d80185629`
passed **100/100 quickstart**, **100/100 three-by-three backup/restore**, and
**100/100 retry-exhaustion/restart**, with zero failures or failed-case retries.
Four workers each ran 25 repetitions of all three scenarios. The run started
at 19:23:06 UTC and finished at 20:01:01 UTC on September 11 (37m55s), exit 0.
The build completed 33/33 steps. Binary SHA-256:
`b7ee84555588fffaf9aa5c9d6c59b580db2493cc7ab459b3361f07a84c73ce6a`.
Manifest, revision, counts, and combined worker output are retained under
`/private/tmp/ci694-coherent-native-soak-cdc9ffcc8`. These results cover the
prefetch and coherent-publication fixes above the earlier failed soak.
Issue #705's separate progressive-index heap corruption remains unresolved;
these scenarios do not reproduce or certify that workload.

Fresh review identified a remaining executor-capacity gap, separate from the
passed soak: request-forward leases count completed requests, whereas Zig
0.16 releases `Threaded.busy_count` after group completion and task destruction.
An allocator pause at that actual retirement edge deterministically leaves six
completed group tasks occupying all six slots of a supported forwarding lane;
the next submission returns `ConcurrencyUnavailable`. The production default's
two spare slots do not establish an ordering guarantee either. The reproduction
isolates the executor contract; it does not inject this pause into a live HTTP
forwarder. Source and output are retained at
`/private/tmp/ci694-forward-retirement-review.zig` and
`/private/tmp/ci694-forward-retirement-review.log`. Forwarding admission still
needs a capacity guarantee that includes executor retirement; the passing soak
must not be presented as resolving this review finding.

The latest Linux x86 job passed all five prefetch tests but failed
`db dense finalization owner drains requests queued during publication` with
`TestUnexpectedResult`. Its cause is not established by the passing native
soak. CI evidence: [job 103392541552](https://github.com/antflydb/antfly/actions/runs/34638056167/job/103392541552)
and `/private/tmp/ci694-cdc-x86-ci-full.log`.

## 2026-09-11: partial operational stats erase the published index inventory (#694)

The native Debug soak of `badafbaf9` finished with quickstart **97/100**, while
backup/restore and retry/restart each passed **100/100**. Two quickstart polls
lost the already-serving text index after creating the image index; another
returned zero source documents after the image searchable-artifact wait.
Evidence is retained under `/private/tmp/ci694-reservation-native-soak-badafbaf9`.

A diagnostic variant polled the existing index 100 times after releasing the
embedder. It reproduced the missing-runtime response twice in 20 runs. The
instrumented API received `source=live_writer_publish`, `docs=3`, `indexes=0`;
the immutable table read view had not been invalidated. See
`/private/tmp/ci694-api-diag-soak/tmp/antfly-zig-standalone-e2e-z3pie08a/server.log`
and the sibling failure `antfly-zig-standalone-e2e-0lvzz9hb`.

The runtime refresher first missed writer-cache admission, then acquired a
lease in its fallback probe. That fallback called `DB.stats()`, whose bounded
operational contract permits an empty index inventory while apply is busy.
Later overlays could refresh document counters without restoring those index
rows. Publishing this as a fresh writer observation let the complete refresh
replace the prior index inventory with an empty one.

The fallback now uses `runtimeStatusStatsConsistentIfAvailable`: only a
coherent observation receives writer authority. Contention falls through to
the retained cached observation, or an explicit synthetic placeholder if no
runtime has published yet. The cold startup publication path uses the same
boundary and returns retryable `WriterLocked` instead of manufacturing an
observed empty inventory. Neither path adds a blocking apply-lock acquisition,
a polling loop, or a longer public timeout.

The deterministic regression releases the writer-cache mutex between the two
probes while holding apply through the fallback observation. It fails on the
old code with `expected 1, found 0`, without leaks. With the fix it retains
source cardinality and the serving index, then observes a subsequent write.
The cold-publication regression covers opening, artifact rebuild, and startup
catch-up phases. Native Debug validation passed ten focused refresh/publication
tests and all 226 writer lifecycle regressions (236 total), without skips,
failures, or leaks; the combined build completed 17/17 steps. Logs:
`/private/tmp/ci694-status-fallback-before.log` and
`/private/tmp/ci694-status-fallback-fixed.log`.

## 2026-09-11: prefetch restart races executor capacity release (#694)

[CI run 34627106321](https://github.com/antflydb/antfly/actions/runs/34627106321/job/103355428154)
failed `prefetch queue background wake stop and restart preserve lock policy`
with `ConcurrencyUnavailable` on the second `startWorker`. The Zig 0.16
threaded executor wakes a future's awaiter before its worker decrements the
executor's busy count. Awaiting the old task therefore does not guarantee that
the queue's one-slot executor can admit its replacement. The recorded stack
lands on the capacity-limit check, not thread creation or allocation failure.

The queue now owns one task from first successful start through deinit.
Stop withdraws processing admission and waits for an in-flight callback to
quiesce; restart resumes the parked task. Deinit signals final shutdown,
awaits the task, and joins its executor before releasing ownership. Pending
items remain available for a later resume or manual drain. This avoids task
churn, capacity retries, and polling in production while preserving both
locked and unlocked callback policies.

The restart regression disables all new executor admission after the first
start, so the old resubmission design deterministically fails. A second
regression holds an unlocked callback across stop, verifies stop cannot return
early, and checks that queued work remains available for manual draining.
All five focused tests passed 100 consecutive native Debug repetitions
(500 test executions). The original implementation passed 100 native repeats;
those passes did not invalidate the failing Linux CI interleaving.

Run from `zig/`:

```sh
zig test pkg/inference/src/runtime/tier/prefetch.zig
```

Evidence: `/private/tmp/ci694-badafbaf9-x86-ci-full.log` and
`/private/tmp/ci694-prefetch-fixed-repeat.log`. The negative control is recorded
in `/private/tmp/ci694-prefetch-original-regression.log`.

The native Debug soak of `badafbaf9` completed separately with quickstart
**97/100**, backup/restore **100/100**, and retry/restart **100/100**.
Two quickstart failures lost the existing text index's runtime observation
after image-index creation; one returned a zero image source cardinality after
the searchable-artifact wait. These motivated the coherent-publication
follow-up recorded above.
Evidence is retained under `/private/tmp/ci694-reservation-native-soak-badafbaf9`;
the binary SHA-256 is
`60d7a097b6924e1821b7aba37d3dd38cffb4e10e3bb693cd3abc94def1ce0491`.

## 2026-09-11: fence stale expiry and reserve uncertain restore capacity (#694)

Review of `8a4d2d83e` added two deterministic failures in the isolated
`ci694-expiry-race-review` worktree. An old poll captured an uncached expired
record; re-admission retired it and committed a replacement with an unknown
receipt. Neither operation advanced the missing-record refresh revision, so
the old poll erased the new durable job. Separately, a full 64 MiB retained-byte
budget still admitted a durable create before the cache rejected it.
`/private/tmp/ci694-review-expiry-race-tests.log` records both failures,
with the 26 existing tests passing and no leaks.

Every attempted persistence mutation now invalidates older observations,
including unknown outcomes and absent cache entries. Point expiry proposes a
SHA-256 digest-conditional delete: Raft apply removes only the exact observed
record, so a delayed command cannot erase a changed or replacement job.
The wire command is gated on metadata protocol version 6; restore persistence
ABI version 4 carries the conditional callback and has no unsafe fallback.
The digest keeps the expiry command small without copying a 64 KiB record.

Admission reserves bytes and a job slot before proposing. Reservations remain
charged after an unknown outcome; a missing point read cannot release them.
Uncertain updates also reserve potential record growth. Cache adoption moves
the existing charge without double counting, while confirmed writes/deletes
resolve reservations. A newly fenced leadership preparation rebuilds accounting
from its linearizable durable view. Ordinary operations maintain this ledger
in constant time; there is no per-admission durable scan or added Raft round.

Regressions cover stale polling, replayed digest-conditional deletion across
key reuse, aggregate byte exhaustion before persistence, missing-row polling,
uncertain create recovery, and uncertain update growth. Run the restore-store,
metadata-logic, and restore HTTP targets in Debug. Historical soaks of
`0126d8b30` and `8a4d2d83e` do not validate these changes.

Native macOS Debug validation passed all 29 restore-store, 248 metadata-logic,
and 36 restore HTTP tests, with no skips, failures, or leaks. The store build
completed 14/14 steps and the combined metadata/HTTP build completed 17/17.
Evidence: `/private/tmp/ci694-reservation-store-tests.log` and
`/private/tmp/ci694-reservation-integration-tests.log`. Fresh E2E acceptance
requires rebuilding this revision and reaching 100/100 in each scenario.

## 2026-09-11: restore expiry must retire durable idempotency ownership (#694)

Fresh review of `0126d8b30` reproduced an expiry regression: after a terminal
job's seven-day retention period, a detail poll evicted only the local record.
Conditional admission then rediscovered the expired durable row, returned it
as accepted, and queued no work. The next detail poll returned not found.
The deterministic polled-expiry regression passed with main's restore-store
implementation (`c3bc001353`) and failed on the PR with
`expected .queued, found .succeeded`. Evidence:
`/private/tmp/ci694-review-expiry-base-focused.log` and
`/private/tmp/ci694-review-expiry-test.log`.

Expiry now confirms durable deletion under the store's leadership term before
releasing the cache and key reservation. Re-admission checks the requested key
even when bounded periodic pruning has not reached it. Conditional creation
also retires matching expired records left by older caches, then retries
creation once; it never accepts an expired record or deletes a different key
after a hash collision. Failed or uncertain deletion preserves cached ownership.
The ordinary admission path adds no extra persistence round.

Regressions cover polling, cached retry, and an uncached durable record across
successful deletion, failure before deletion, an unknown outcome after deletion,
and leadership loss. They verify a changed request can reuse an expired key and
creates exactly one durable job, history entry, and runnable item. Run
`zig build antfly-api-restore-jobs-test -Doptimize=Debug`.
Short-lived E2E soaks do not exercise the retention boundary.

Validation: **26 restore-store tests** (including the 12 fault combinations)
and **36 restore HTTP tests** passed in native Debug with no skips, failures,
or leaks. Both focused builds completed all 14 steps. Zig formatting and
`git diff --check` pass. Logs: `/private/tmp/ci694-expiry-fixed-tests.log` and
`/private/tmp/ci694-expiry-http-tests.log`.

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
