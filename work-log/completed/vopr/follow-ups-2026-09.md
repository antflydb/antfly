# VOPR Follow-ups (2026-09)

> Relocated verbatim from `zig/VOPR.md` (lines 3236–3852 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`VOPR.md`](../../../zig/VOPR.md). Durable decisions from this log were folded into that document before the move.

### Runtime Correctness Review and Fixes (2026-09-06)

The focused review at `6fe629d2e` reproduced four correctness defects; all four
are now fixed with regression tests. Passing replay tests alone does not
establish std.Io contract conformance: a wrong runtime behavior can replay
exactly. This focused review does not certify the entire branch.

1. **P1 — Cross-archive executor error identity (fixed).** Raw `IoBorrow`
   vtables reproduced `FileNotFound` becoming `WriteFailed` in a separately
   compiled consumer. `runtime_io_abi.zig` now dispatches through the executor's
   owning compilation unit and translates errors by name, including nested
   operation results, completed batch entries, and cached file-reader errors.
   File-reader and locked-stderr wrappers use local vtables. Task callbacks
   retain their own compilation-unit result handling. API request handlers own
   a synchronous receiver; inference keeps its receiver at a stable address
   until shutdown. Same-unit calls retain a direct fast path. This is a private,
   version-checked, same-toolchain bridge, not a general cross-version Zig ABI;
   native ABI version 4 rejects old borrows and shared-manager layouts. Existing HTTP callback status enums
   and the secret-store owner dispatcher remain separate safe boundaries.
2. **P1 — Group cancellation skipped ownership cleanup (fixed).** Queued
   callbacks now enter with cancellation pending and drain alongside entered
   fibers. Their cleanup defers run, including callbacks added while the group
   is cancelling. Awaiting an empty group releases its token before delivering
   cancellation. The previous regression that expected queued work to disappear
   now requires callback entry and cleanup. The durable-job VOPR adapter checks
   cancellation after installing its cleanup defer, so closing an owner still
   skips queued job bodies without relying on the scheduler to discard callbacks
   or on a second manual cleanup path.
3. **P2 — Group argument alignment (fixed).** Context storage includes alignment
   padding. Alignments above the supported 16-byte storage alignment are
   rejected before concurrent ownership transfer; `Group.async` uses its eager
   fallback. Tests cover 16-byte padding, 64-byte concurrent rejection, and
   correctly aligned eager async arguments.
4. **P1 — Protected waits completed early on cancellation (fixed).** A pending
   request no longer wakes a cancellation-protected task or an uncancelable
   futex wait. Protected sleep reaches its real deadline and pending cancellation
   is delivered after unblocking. Protected group awaits likewise do not
   prematurely propagate cancellation to children. Dedicated regressions cover
   the timed-sleep deadline and a futex that remains parked until an actual wake.

VoprIo model version 8 records the changed cancellation semantics in backend
identity; histories from the old model must be freshly recorded, not migrated.
The reusable library now passes 160 tests in Debug and ReleaseSafe, including
five new `vopr_io_contract_test.zig` regressions. `zig build runtime-io-abi-test`
compiles its provider and consumer independently to test file/network errors,
cancellation, batch and reader state, and task results across real error domains;
all four tests pass in Debug and ReleaseSafe.
The Antfly `vopr-runtime-test` adapter gate passes all 18 tests, including the
durable-job close/pause/reopen regressions, alongside `vopr-contract-test`.
The secret-store archive gate was expanded from one to three tests, covering
layered rotation, retained reader values, malformed and missing replacements,
and injected cancellation without mutation; config and secret reload gates
also pass. The production `make zig-build`, 17 previously failing managed
embedding/artifact/backup E2E cases, Go SDK tests, and repository formatting
check pass after merging `origin/main`. These focused gates are not a claim of
a fresh aggregate `vopr-test` or full-platform CI run. The recent
extension fixture now owns its committed projection; the merge-adjusted
DataServer fixtures explicitly retain a host-filesystem differential boundary.
Neither is evidence of fully virtual storage coverage. No additional replay
algorithm defect was confirmed in the reviewed enabled-set, choice-consumption,
canonical-artifact, checkpoint-prefix, and teardown paths.

### Production Review Follow-up (2026-09-06)

The subsequent non-VOPR review reproduced three production defects. These fixes
exercise the actual owners, not substitute simulation models:

- **Restart-safe read identities.** Applied reads and transaction-recovery
  ReadIndex requests now include a fresh 128-bit runtime incarnation as well
  as a non-reused request counter. Initialization draws the incarnation through
  the owning backend's std.Io (VoprIo in controlled histories), fails on entropy
  errors, and cleans up partially constructed ownership. Delayed pre-restart
  responses cannot complete new waiters even when group and counter match.
  Counter exhaustion fails closed. Transient v1 contexts are rejected, not
  migrated. Reusable tracker tests and DataServer state-machine regressions
  cover stale-response rejection and completion with the current identity.
- **Retention/publication snapshot ordering.** Retention reads published HEAD
  before listing immutable manifests. Publication between those calls no longer
  causes a false `PublishedHeadManifestMissing` and terminal worker failure.
  An unpublished namespace is a no-op; a concurrently pruned old HEAD whose
  current HEAD has advanced is retried on the next pass. A genuinely missing
  current root still fails closed, including when the listing is empty.
  Object-store regressions cover publication and old-root removal at this seam.
  This is not a claim that arbitrary overlapping retention workers are fenced.
- **Shared UserManager executor ownership.** The manager retains an
  `runtime_io_abi.Borrow`, not a foreign raw std.Io. Synchronous methods create
  a caller-local receiver that lives through mutex release; movable manager and
  seed-lease values never retain a stack receiver. An independently compiled
  provider/consumer test covers `Canceled` and `EntropyUnavailable` during user
  creation, password update, and API-key creation, unchanged credential state,
  and lock release. This fixes embedded I/O error translation, not a general
  certification of every auth storage or policy callback boundary.

The focused gates are `raft-read-gate-test`,
`antfly-data-runtime-test -- "data raft read safety barrier"`,
`lib-serverless-manifest-test`, `lib-usermgr-test`, and
`lib-usermgr-abi-test`. The new read-gate and independent auth-archive gates
are included in the regular unit aggregate.

Validation: read-gate, manifest/retention, user-manager, and independent auth
archive gates pass in Debug and ReleaseSafe. The DataServer restart regressions,
auth-lifecycle and serverless-workflow VOPR campaigns, production runtime build,
seven HTTP auth E2E tests, and repository formatting checks also pass.
At that checkpoint, `production-cluster-join-split-vopr-test` was **not green**:
it reproduced a 175-byte packet payload replay divergence followed by
`VoprIoTeardownStalled`. A separate clean worktree at the pre-fix checkpoint
`91dab5df73` reproduces the same failure class (choice 11221 versus 11227
with these fixes). Packet-level determinism and abort-path teardown therefore
were not fixed by that checkpoint. See the readiness and replay follow-up below
for the subsequent root-cause investigation.

The latest pre-fix Linux CI E2E run reported a same-name serverless dense-index
update status error and a CLI semantic-query timeout (321 passed, two failed).
Both failing test cases pass locally against this checkpoint's rebuilt binary.
That local rerun does not establish their CI root cause or certify Linux timing;
the next CI run remains necessary.

### Production Lifetime and Retention Follow-up (2026-09-06)

The next non-VOPR review reproduced and fixed two additional production defects:

- **HTTP client final-release lifetime.** The last closed request remains counted
  until it owns the drain mutex. Shutdown cannot observe zero and destroy the
  client while release still needs to broadcast or unlock. Open and non-final
  releases retain the atomic fast path, with no gate access after relinquishing
  their reference. A deterministic lock interleaving reproduces the old race.
  `lib-httpx-client-lifecycle-test` runs all five admission/watchdog regressions
  and is included in `lib-httpx-test` and the regular unit aggregate.
- **Retention policy increases after GC.** A durable, monotonic
  `MANIFEST_GC_FLOOR` records the manifest-version boundary before deletion.
  Increasing retention stops at previously retired history instead of treating
  an intentionally deleted parent as corruption and terminating maintenance.
  The boundary also prevents resurrection of partially deleted versions after
  cancellation. It is separate from the WAL watermark because multiple
  publications can share a WAL position. Filesystem and object-store progress
  owners persist it through their existing durable/CAS mechanisms. Missing HEAD
  or retained ancestors still fail closed; missing history without a recorded
  retirement boundary is not silently accepted. Increasing retention preserves
  available history and accumulates future versions; it cannot restore retired
  content. This does not add fencing for arbitrary overlapping pruners.

The manifest gate covers equal-WAL publications, larger retention after GC,
owner reopen, monotonic CAS, genuine missing history, and cancellation after
exactly one obsolete artifact is removed. HTTP lifecycle and manifest gates
pass in Debug and ReleaseSafe; the 13-test serverless-workflow VOPR gate passes
in Debug. The production runtime build, repository formatting check, and
same-name serverless dense-index update E2E regression also pass locally.
The broader replay and CI limitations recorded above remain separate from
these focused production fixes.

### Shared Recovery Ownership Follow-up (2026-09-06)

Review of the stable transaction-recovery wrapper found two remaining ownership
defects: it retained a freed enrichment runtime after producer replacement,
and recovered commits updated a private visibility summary while the serving
wrapper continued using stale live/tombstone counts and query caches.

Identity visibility now belongs to the heap-owned DB core. Serving writes,
transaction recovery, and TTL cleanup publish into the same summary and
invalidate the same live/non-visible query-set caches. TTL cleanup borrows this
stable state directly, without waiting for a serving-wrapper address to be
registered. Cache allocations are reclaimed through their owning allocator
after the workers are joined.

Recovery borrows the current enrichment runtime from the shared async context
only for the duration of a resolution. A shared provider mutex serializes that
borrow with replacement/removal; the recovery wrapper retains no enrichment
pointer between calls. Failed replacement releases the mutex and preserves the
previous provider. Private native ABI version 5 rejects the old DB/core layouts.

Three regressions cover recovered deletes and bidirectional visibility,
invalidation of both query caches, successful resolution/full-text visibility
after provider replacement, provider removal, and failed-replacement guard
cleanup. The 43-test transaction/TTL gate passes in Debug and ReleaseSafe,
the enrichment-worker gate passes all 21 tests, and the four cross-archive I/O
ABI tests pass in both modes. The production build and repository formatting
checks also pass. Against the rebuilt binary, all 21 transaction E2E tests,
the concurrent insert/delete publication regression, and the same-name
serverless dense-index update regression pass (23 E2E tests total).

The initial DB/index VOPR rerun passed 11 of 12 tests, with
`IndexRebuilding` in managed readiness. Reproducing it at `20f359ab7f`
established its history, not an acceptable exemption from the gate. The
follow-up below fixes it and investigates the distributed replay failure.

### Readiness and Replay Root-Cause Follow-up (2026-09-06–07)

**Managed readiness is green: 12/12 DB/index VOPR tests pass in Debug and
ReleaseSafe.** The fixture admitted a repair intent but never ran the repair
owner's deferred source discovery, and it disabled the canonical index worker.
It now enables that worker on the manual backend, advances the actual repair
intent, drains replay, and verifies a durable one-document checkpoint before
testing progressive visibility. A serviceable partial generation must not be
reported as degraded; atomic and initial-publication reads still fail closed.
Scenario version 2 requires fresh histories, with no legacy migration.

The distributed investigation found several distinct faults rather than an
opaque packet-replay limitation:

- A manually constructed backend omitted its filesystem I/O capability.
  The fixture now supplies VoprIo for both general and filesystem operations;
  durable identity publication must not fall back to host storage.
- Routing clients and cache/session/write-admission paths used host-clock
  elapsed time in `X-Antfly-Routing-Remaining-Ms`. Identical controlled choices
  could therefore produce different packet bytes. The routing capability now
  carries its borrowed clock through capture, cache TTLs, deadlines, retries,
  and pinned projections. Clock borrows use the independent-library I/O bridge.
  Packet identity and payload-digest checks are unchanged.
- Internal routed lookup ingress also established host-clock deadlines.
  Ingress now pairs those deadlines with its executor, and operation and lookup
  checks retain that authority. Native transaction-only ingress keeps its
  existing explicit clock contract; this is not a repo-wide clock migration.
- Six Raft snapshot-sender tasks could remain in uncancelable condition waits
  during abort teardown. Non-joining transport shutdown now closes sender
  admission, cancels active requests, and wakes idle workers before scheduler
  draining. The owner retains responsibility for joining and freeing them.
  Metadata fixture teardown signals its transports too. Snapshot retry deadlines
  use the owning executor's clock.
- The broadcast-join oracle incorrectly required a shuffle worker-attempt
  ledger. Broadcast still must prove distributed execution, both groups, and
  the expected rows; durable shuffle retains its worker/finalizer requirements.
- Shard I/O could re-enter metadata apply and replace the transition array
  while its executor retained a record pointer. Debug replay exposed a
  use-after-free during split-request serialization. Observation and action
  passes now own deep record snapshots, fence reentrant passes, and revalidate
  the live record after each external call before publishing results. Tests
  cover split and merge removal/replacement during successful and failing
  callbacks, array reallocation, nested passes, and allocation-failure cleanup.
- The fixture suppressed all data-node Raft tickers during a yielding control
  round, and otherwise advanced election ticks at its 1 ms control quantum
  instead of production's 100 ms Raft cadence. Tickers now remain independent,
  defer only a busy node through a non-blocking production progress seam, and
  use the production Raft and control intervals independently. Live inspection
  found stable term-1 source/destination leaders, but the old control hot loop
  had spent over 156,000 transitions advancing only eight virtual seconds;
  repeated cached observations consumed the history budget before normal
  heartbeat/cache intervals elapsed. The real DataServer regression verifies
  busy-node deferral and subsequent progress, work-cost callbacks outside the
  Raft mutex, and cancellation without retaining the lock.
- Deliberate aborts exposed an unclassified `ClientShuttingDown` at the public
  batch boundary. It produced an internal error and failed the test runner's
  error-log check even after all 16 scenario tests passed. Shutdown now returns
  the existing conservative `WriteOutcomeUnknown` result: it neither claims
  that nothing committed nor invites a blind retry. The HTTP regression checks
  the 409 response and one commit-hook invocation for shutdown and ambiguous
  Raft outcomes. Unexpected-error logging and the runner's error-log check
  remain enabled.

Focused validation includes all 41 Raft transport tests in Debug and
ReleaseSafe, all 14 transition-service tests in both modes, four metadata
routing protocol tests, 20 remote-routing/source
tests, and a virtual ingress deadline regression. The sender regression covers
both not-yet-entered and idle workers, repeated shutdown, rejected admission,
scheduler quiescence, and owner cleanup. Four independent-library I/O ABI tests
pass in ReleaseSafe. The native ABI is version 6, and the composed full-cluster
scenario is version 54. The full join/split gate additionally checks bounded
early termination and exact replay, not just successful-history teardown.

The production binary and repository formatting checks pass. The previous
pushed checkpoint's Linux AutoGraph multi-node write test timed out; it passes
locally against the rebuilt binary. That rerun does not establish the CI root
cause or certify Linux timing. All 34 local transaction, resolution, and
automatic shard-split E2E tests pass against the final rebuilt binary,
including the shutdown classification fix.

Complete join/split histories and their fresh-world exact replays pass in both
Debug and ReleaseSafe within the unchanged 420,000-transition budget.
Both full gates exit successfully with all 16 tests passing, including bounded
early-abort exact replay. The final Debug rerun also verifies that expected
shutdown no longer emits the six error-level logs that failed its earlier
build exit. This closes the named packet-replay and teardown failures; it does
not certify the separate v53 managed-publication completion work or every
transitive production callback's determinism.

### Native Query Deadline Follow-up (2026-09-07)

The non-VOPR review caught incomplete clock boundaries in routing and HTTP
readiness retries: public queries still create native `CLOCK_MONOTONIC`
deadlines, whereas catalogs and retries use their borrowed executor's `.awake`
clock. On Darwin these are different clocks. The pre-fix binary returned
immediate 504s for otherwise successful one- and two-shard queries with 1 s or 5 s budgets;
the observed clock offset was approximately 11 s.

Request-to-routing and HTTP retry boundaries now translate the remaining budget
into the destination clock without changing the query engine's native execution
deadline.
Lookup translation preserves its explicitly borrowed request clock, and join
routing creates relative deadlines directly in the catalog clock. Same-clock
deadlines remain unchanged, absent deadlines remain absent, and an expired
request cannot acquire a fresh budget. A shifted-clock unit regression covers
the cross-domain, same-domain, and expired cases on every platform. Public E2E
regressions exercise both one and two shards, short and long valid deadlines,
and immediate expiry. This is boundary translation, not a claim that every
query execution callback is now driven by VoprIo.

Validation: the table-read gate passes **77/77** and the focused HTTP retry,
deadline, and cancellation tests pass **5/5**, in both Debug and ReleaseSafe.
The rebuilt server passes **20/20** deadline, query-string, exact-sort, join,
and graph E2Es. The original separate metadata/data-server reproduction now
returns 200 for 1 s, 5 s, and 30 s budgets (5–6 ms observed). Formatting checks
pass. An intermediate broader run observed a post-restart exact-sort status
assertion failure; its isolated rerun and the final broader run passed. This
deadline fix does not claim to resolve that intermittent status observation.

### Composed Query Deadline Follow-up (2026-09-07)

The next production review reproduced the same native/borrowed clock mismatch
beyond plain reads: three join E2Es, a multi-shard graph E2E, and a mock-provider
semantic E2E all returned `query_timeout` with 1 s budgets while passing with
30 s budgets. The earlier routing/HTTP fix alone did not close these paths.

Join contexts now translate native and explicitly borrowed request deadlines
into their coordinator clock. Query callbacks, foreign-source requests, and
native CPU deadline pollers receive a translated native deadline on the way
back out. Graph workers translate the native query deadline at ingress and
translate again when a catalog has a different clock. Semantic planning
translates the budget into the cache's clock while leaving the provider's
native execution deadline unchanged. Cache TTLs and modeled waits remain on
the borrowed executor. These are explicit boundaries, not a claim that native
query callbacks are fully deterministic.

Remote join partition, rows, unmatched, and finalize clients now classify HTTP
504 as `Timeout`, just like 408; a worker's `DeadlineExceeded` must not become
an unexpected-status/internal failure at the coordinator. The focused HTTP
test root now explicitly discovers the join-client contract and the new
clock-boundary tests. Permanent public deadline tests cover plain, join,
graph, and semantic queries with valid short/long budgets and immediate expiry.

Validation: the five focused clock/client contracts pass in Debug and
ReleaseSafe, the table-read gate passes 77 tests, the broader HTTP runtime gate
passes 114 tests with local listeners permitted, and the query-embedding-cache
VOPR gate passes. The production rebuild and formatting checks pass. All 21
selected deadline/join/graph/semantic E2Es pass, as do the five original
review reproductions forced to use 1 s budgets.

During regression development, a graph source document containing only
`_edges` stalled a `full_index` batch with repeated `ReplayDocumentNotVisible`
retries. The deadline regression uses an ordinary source document with a
`title`, matching the existing graph scenario. That separate edges-only
document observation is not repaired or claimed covered by this deadline fix.

### Merge Import and Enrichment Deadline Follow-up (2026-09-07)

Production review found that merge artifact import acquired its apply locks
inside `if/else` blocks whose `defer`s released them before copying began.
Unlocking now occurs at function scope in reverse acquisition order. A
regression exercises the actual import's first allocation in both path orders,
verifies both locks are held there, and verifies allocation failure releases
them. Transition admission still owns public-operation exclusion; it does not
replace these DB locks against maintenance.

Enrichment also retained a native inference boundary after its runtime clock
became executor-owned. Provider contexts now receive translated native
deadlines, and progress reports translate those deadlines back into the
runtime clock. Coverage includes runtime epochs ahead of and behind native
time, live and expired deadlines, fallback budgets, cancellation, and progress
without a deadline. This does not make native provider execution deterministic.

The Linux CI abort in the paged graph-anchor test came from an undefined
worker fixture: deadline validation now consults the worker clock even when
the pattern has no edges. The fixture now initializes its worker and catalog
clock fields without bypassing production deadline validation.
The broader graph run also repaired three catalog fixtures that had not opted
into the existing test routing adapter.

Validation: both new regressions pass in Debug and ReleaseSafe; the enrichment
suite passes 116 tests, graph/merge-cutover coverage passes 53, and merge
coordinator coverage passes nine. The production rebuild and formatting checks
pass, as do all 21 selected public deadline/join/graph/semantic E2Es. These
local checks cover the reported Linux CI abort; they do not claim
that a new remote CI run has completed.

### Merge Outcome and Artifact Preservation (2026-09-07)

The follow-up review found two data-preservation defects beyond import lock
scope: replicated merge copied only primary JSON, losing stripped graph and
explicit dense/sparse inputs; local bootstrap replayed historical requests over
an already-current snapshot, while its tail decoder ignored transforms,
predicates, and transaction outcomes.

Live local merge now reads raw primary outcomes from the transition-leased
donor DB. Bootstrap and catch-up replace the donor-owned receiver slice, then
copy current artifacts before advancing the watermark. They do not replay old
inserts, transforms, aborted prepares, or failed conditional writes. Offline
coordinators still require an already-reconciled primary projection. Local
artifact import uses the durable batch/index journal rather than mutating live
index projections outside that journal.

Replicated merge refreshes the receiver slice using receiver-key deletion
proposals, current primary writes, and bounded artifact pages. It no longer
depends on retained request history for tombstones. Private `_merge_artifacts`
commands carry binary-safe store rows with receiver identity, reject public,
unscoped, mixed-mutation, and non-artifact payloads, account for admission bytes,
and persist artifacts, replay work, and the Raft apply receipt together. Durable
protocol v5 activation prevents older replicas from silently ignoring them.
Every receiver replica applies these commands before the completion checkpoint.

Embedding-only changes notify dense and sparse replay. Graph export authenticates
the donor's current index generation and emits portable edges that receiver
replay binds to its own generation; retired-generation edges are not revived.
Primary and artifact export pages stop at 128 rows or approximately 1 MiB,
allowing one oversized row for progress. Receiver range clearing and the offline
projection path retain their existing whole-range allocation; this is not a
claim that every transition phase is memory-bounded.

Regressions cover bootstrap and tail transforms, aborted and rejected
conditional prepares, delete/recreate, removal of stale documents, preservation
of the receiver base range, binary payload round trips and rejection, vector
and graph searches after replay/reopen, and production DataServer graph merge
through rollback, fresh retry, finalization, and receiver reopen on `VoprIo`.
Broader disjoint-placement, artifact-heavy snapshot-install, and overlapping
failure campaigns remain roadmap work, not newly proven by these focused tests.

Validation: the data-storage suite passes 68 tests and the merge-coordinator
filter passes ten. The artifact/reopen, wire-validation, production graph-merge,
and transaction-outcome regressions pass in ReleaseSafe. The three-production-
owner merge/split record-and-replay history and lifecycle protocol check pass
without skips after granting local listener access. The production build,
formatting checks, and all 21 selected public deadline/join/graph/semantic E2Es
pass. GitHub reported no CI checks for the prior PR head at validation time;
these are local results, not a claim of completed remote CI.

### Merge Copy Fencing and Receiver Reuse (2026-09-07)

The next production review found that copy payloads carried transition identity
without checking the receiver's durable phase. A delayed coordinator could
therefore delete or overwrite post-cutover data. Both document DB apply and the
Raft apply projection now accept copy effects only for the exact active
receiver transition. Stale committed writes, deletes, and artifact pages become
no-ops that advance their Raft receipt without changing data or derived
visibility; unfenced direct mutations fail with `MergeCopyFenced`.

A successful merge also left a terminal receipt that blocked any subsequent
donor. Both finalized and rolled-back receivers now admit a fresh accept against
their current range. Production range construction does not reuse another
transition's base or merged range, and observation presents an unrelated
terminal receipt as awaiting acceptance. Durable retired transition IDs survive
subsequent checkpoints, reopen, and snapshot transfer; old accepts cannot
resurrect a retired merge. The direct coordinator follows the same retirement
rules. No claim is made here about arbitrary overlapping active coordinators or
all disjoint-placement histories.

Regressions exercise successive donors, stale writes/deletes/artifacts after
finalization and during a later merge, unchanged derived visibility with
advancing Raft receipts, retired checkpoint replay after reopen/snapshot
transfer, and fresh-versus-retired metadata observations.

Validation: the default data-storage gate passes 69/69 and the focused merge
storage/coordinator gate passes 15/15. The DB fencing, checkpoint, and artifact
replay/reopen tests pass 3/3 in both Debug and ReleaseSafe. The production
observation and two DataServer merge/split record-and-replay histories pass
3/3 without skips. The production build and `make fmt-check` pass. New
regressions are included in the existing data-runtime and data-storage default
filters. GitHub reported no checks for the prior PR head; these are local
validation results, not a remote CI certification.

### Merge Copy Attempt Fencing (2026-09-07)

The terminal-state fence above left a same-transition failover window: an old
donor leader's delayed clear could arrive after its replacement completed
bootstrap but before receiver finalization. The receiver was still accepting
and the transition/group IDs still matched, so the clear could delete data
that finalization would publish without another verification.

Each production copy now opens a durable `begin_copy` checkpoint carrying a
`(donor_term, sequence)` attempt token. The elected donor allocates sequences
under its Raft mutex; retries in one process cannot reuse a sequence, and a
restarted donor must win election in a newer term. Receiver apply orders tokens
by term first, then sequence. A newer attempt resets bootstrap evidence; a
delayed begin cannot reclaim ownership. Every document, delete, and artifact
page carries that token. Both serving DB and Raft projection apply reject
superseded copies, and bootstrap completion closes even the winning attempt
against late duplicate packets. Completion and finalization must match the
persisted attempt; stale checkpoints cannot certify a replacement copy.

Finalization carries the token returned by its own copy, rather than borrowing
a newer receipt from the receiver. Donor finalize/rollback proposals also
require the captured local leadership term under the proposal mutex and cannot
be forwarded into a successor term. Rollback cleanup opens its own attempt;
checkpoint commands cannot bundle unfenced document mutations. The exclusive
direct coordinator persists a fresh attempt before refreshing completed data.

Raft batch protocol v6 activates these semantics before receiver commands are
proposed. Attempt identity survives forwarding, durable DB reopen, and
projection snapshot transfer. Regressions cover term-over-sequence ordering,
stale begin/completion/finalization, delayed clears and artifact pages before
finalization, winning-attempt closure, unchanged derived visibility, advancing
Raft receipts, and repeated production copies with stale packets through the
real proposal path. These are bounded regression histories, not a claim of
exhaustive overlapping-coordinator or placement coverage.

Validation: the final default data-storage gate passes 69/69. The focused
runtime/protocol gate passes 7/7, including the expanded production stale-copy
history and the three-node merge/split record-and-replay history; the additional
codec assertions pass 3/3. The four DB merge regressions pass in Debug and
ReleaseSafe, together with the 34 query tests included by each DB invocation.
All report zero skips, failures, or leaks. The production build,
`make fmt-check`, and `git diff --check` pass. Before push, GitHub checks on the
previous head had no reported failures but several jobs were still pending;
these results are local validation, not a remote CI certification.

### Remote Merge Control Routing and Terminal Retries (2026-09-07)

The hosted merge adapter routed transition-control RPCs to the donor leader,
but the receiving HTTP operation still required the receiver group in the URL.
That mismatch rejected all four remotely routed merge actions with HTTP 400
while local dispatch bypassed the check. Endpoint validation now agrees with
donor-owned execution. Copy payloads and receiver checkpoints still target the
receiver group; this change applies only to transition-control RPCs.

An exact accept retry after rollback could also commit a receiver checkpoint
that failed apply with `ConflictingMergeTransition`, blocking later entries in
the still-live group. The analogous prepare retry after finalization failed
donor projection apply. Acceptance now checks terminal/retired receipts before
opening databases or proposing controls. Matching controls already in flight
when rollback or finalization won fold to durable no-ops in both receiver and
source apply, preserving terminal evidence while allowing Raft receipts to
advance. Conflicting participant identities and receiver range/namespace
contracts remain errors; this is not blanket suppression of apply failures.

Regression coverage includes all four actions through the hosted remote
adapter and actual HTTP listener, typed-operation rejection of
receiver/unrelated group IDs, no new Raft entries for terminal accept retries
in the production DataServer history, source and receiver terminal-control replay, subsequent
ordinary writes after rollback, and durable reopen/snapshot evidence. These
tests cover exact terminal retries, not arbitrary invalid internal commands
or all overlapping transition histories.

Validation: the default data-storage gate passes 69/69; the focused production
runtime/remote-HTTP gate passes 4/4. The five DB merge regressions pass in Debug
and ReleaseSafe, with the 34 delegated query tests also passing in each mode.
All report zero skips, failures, and leaks. The production build,
`make fmt-check`, and `git diff --check` pass. Pre-push CI on the previous head
reported no failures but still had pending jobs; these are local results,
not certification of the new commit by remote CI.

### Terminal Merge Retries After Split-Start (2026-09-07)

The terminal-control fold still compared the live receiver range with the
historical merge range before recognizing an exact terminal receipt. A later
split-start legitimately narrows that range while retaining the receipt, so
an already-in-flight merge checkpoint could fail committed apply with
`MergeRangeStateMismatch`. The send-side obsolete-accept guard cannot recall
such requests.

Matching terminal controls now preserve the live range before active-transition
range validation. Checkpoint syntax and the durable participant, original
range, and namespace contracts are still validated; nonterminal range
mismatches remain errors. Terminal phase, bootstrap evidence, and copy-attempt
ownership are unchanged while the enclosing Raft applied index advances.

Regressions compose both merge terminal outcomes with production split-start
mutations and all five delayed receiver control kinds. DB coverage includes
reopen before replay and after a subsequent ordinary write; apply-store
coverage restores the split-start snapshot before replay. Both check the
narrowed range, preserved terminal evidence, continued write/apply progress,
and rejection of a conflicting historical contract. This is scoped coverage
of delayed merge controls after split-start, not every overlapping topology
history.

Validation: the new DB regression first reproduced `MergeRangeStateMismatch`
at the delayed accept before the fix. With the fix, all five focused DB merge
regressions pass in Debug and ReleaseSafe, and the default data-storage gate
passes 69/69, all with zero skips, failures, and leaks. `make fmt-check`,
`zig fmt --check` on the changed Zig files, and `git diff --check` pass. The
pre-push checks on the previous head had no failures but still had pending
jobs; these results do not certify remote CI for this checkpoint.

### Merge Receipt Ownership Through Physical Split Cutover (2026-09-07)

The split-start retry fix did not cover physical LSM finalization. The old
`raftmerge:state` key sorted after encoded document keys, so the physical split
copied it to the child and discarded it from the parent. Subsequent receiver
checkpoints could then fail with `MergeTransitionNotReady`, or resurrect an
old accept when the retained range happened to equal its original base.

Merge receipts now live in the protected system-metadata prefix: physical
cutover retains them on the parent and destination cleanup excludes them
from the child. Existing production records under the old key remain readable;
checkpoint and direct-coordinator writes retire that key atomically. Split
preparation and finalization promote any remaining old-key receipt with an
atomic write/delete batch and sync it before the physical split. This avoids
a receipt-loss crash window between destructive rewriting and restoration.
Destination cleanup also removes inherited old-key records.

The migration regression exposed a second storage defect: physical split
rewrites assigned new L0 run IDs newest-first, making older writes and
tombstones outrank newer data. Child construction now assigns IDs oldest-first.
When a parent L0 run straddles the boundary, all surviving parent L0 runs are
rewritten oldest-first, including newer left-only runs that would otherwise
be outranked by newly numbered replacements. Lower-level ordering and physical
run-size limits remain unchanged. A low-level regression checks overwrites,
tombstones, revived keys, straddling runs, and newer one-sided runs on both
parent and child after reopen.

The LSM regression covers finalized and rolled-back receipts in both current
and old-key layouts, retains retired-transition IDs and copy-attempt evidence,
performs real split preparation/finalization, checks the raw destructive-rewrite
boundary before any later restoration, and reopens both databases.
It replays all five receiver controls plus a retired accept, checks continued
parent write progress and range ownership, verifies the child has no inherited
receipt, and starts an independent child merge at Raft index one. The finalized
case deliberately splits back to the original receiver base to guard against
old-accept resurrection. This preserves receipts through future cutovers; it
does not reconstruct receipts already lost by an earlier physical split.

Validation: the DB regression reproduced `MissingMergeReceiptAfterSplit`
before the key fix, and boundary inspection then reproduced old-tombstone
precedence during migration before the L0 ordering fix. With both fixes, the
seven focused DB/split regressions pass in Debug and ReleaseSafe; the three
focused LSM split/run-cap tests, all 12 LSM workload/fault-recovery tests, and
the default 69-test data-storage gate pass. All report zero skips, failures,
and leaks. `make fmt-check`, changed-file `zig fmt --check`, and
`git diff --check` pass. Pre-push checks on the previous head had no failures,
with the x86 Zig and E2E build jobs still pending; remote CI for this checkpoint
is not yet verified.


## Metadata Planning Fixes (2026-09-07)

> Relocated verbatim from `zig/VOPR.md` (lines 655–692 at commit 271838a195) on 2026-09-16 during the documentation cleanup. The durable `disk_bytes_known` planner rule was folded into the Completion-Claim Audit before the move.

Metadata planning fixes (2026-09-07): rename validation exposed four failures
that also reproduced at merged, pre-rename commit `6d21a583f`. The shared
candidate-report fixture supplied disk sizes without setting
`disk_bytes_known`, so the production planner correctly rejected them as
inconclusive. Explicit scenario size observations now carry that flag; unknown
size and known-zero size paired with live documents remain fenced.

Once planning resumed, automatic split exposed an ownership error in the
harness: polling reopened and reseeded the apply store while the active split
coordinator still owned it. Polling now reads through the matching coordinator.
First observation establishes that fixture owner even before a destination DB
exists; missing initial progress is an unbootstrapped state, not a file error.
After the coordinator releases a terminal transition, observation reads durable
progress without reopening or reseeding its possibly retired source primary.
This also works after reconstructing the runtime: the terminal record, rather
than an in-memory completion flag, is authoritative.
Initial source seeding also uses a read-only DB view, so it can coexist with a
public writer without acquiring a second writer. Filesystem-backed read-only
views load the existing validated root-identity checkpoint through the borrowed
filesystem I/O; missing external-backend identities still fail closed.
The initial snapshot uses `seedGroupSnapshotFromAuthoritativeStoreIfAbsent`
with typed range bounds and opaque document keys/values. It does not synthesize
legacy text commands or consume Raft entry indexes. A regression covers an
open-ended range and keys/values containing the old delimiters.
Neither fix relaxes LSM's single-writer guard. The split fixture regression
covers seeding beside a live
writer, failed initialization and retry, initial status before destination
creation, status observation between
transition actions, terminal observation and runtime reconstruction without the
former source identity checkpoint, destination contents, and preservation of the source
identity namespace; the planner regression
covers the known/unknown and empty/non-empty size-evidence matrix.

Verification: the complete virtual-transport gate now passes **12/12**, smoke
**5/5**, seeded campaigns **2/2**, and public HTTP integration **4/4**, with no
skips, failures, leaks, or storage-ownership warnings. The focused planner
contract checks pass **2/2** in Debug; the split-lifecycle and candidate-report
regressions pass **2/2** in both Debug and ReleaseSafe.

## Soak Investigation Findings

> Relocated verbatim from `zig/VOPR.md` (lines 3150–3177 at commit 271838a195) on 2026-09-16 during the documentation cleanup. Durable invariants from this narrative were folded into the Raft and Metadata and Distributed Data sections before the move.

Initial search at base seed `0xa17f5500` found a Raft oracle failure after a
crash discarded an unpersisted term. The version-2 Raft scenario checks
volatile monotonicity within an incarnation and durable term/commit plus
completed application across restarts. It also preserves each node's append
and apply lane order, matching the [async Raft storage contract](https://github.com/etcd-io/raft/blob/main/raft.go).
These corrections preserve checks for lost durable state; they do not suppress
all regressions after a restart.

The same search found that the distributed-data scenario required retirement
on all three replicas while one remained partitioned. Version 2 verifies
surviving-quorum reads and retirement first, explicitly heals, then verifies
every replica. Healing restores metadata blackhole routes as well as virtual
network faults, so a later partition cannot accidentally retain an earlier
one. Metadata uses the shared Raft cluster restart path to preserve virtual
endpoints and establishes a ReadIndex proof before post-restart writes.
The delayed-transport choice injects a two-tick delay into the actual virtual
network and asserts that it was exercised; a supplied native executor was
previously overwritten during cluster setup. An uncertain reconcile-lease
proposal remains pending until
committed authority is observed; it does not abort the whole partition
history or grant authority from an unknown response. This scenario still
schedules coarse fault modes around a
native HTTP differential; its four-choice trace is not packet-level replay
of the production cluster. The native HA seed-snapshot regression now creates
its deadline from the DB's runtime clock, matching the production caller.
The forced-reallocation restart regression keeps the request pending until
every voter reports the exact observed request ID; pre-request size reports
cannot acknowledge the scan.
