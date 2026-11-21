# Metadata VOPR Determinism Audit

Status: complete; retained as a record.

This is the preserved Phase 0 baseline inventory for the first metadata world:
`metadata/vopr_harness.zig`, including its `raft/vopr_harness.zig` transport.
The table records the risks as originally audited; the resolution summary below
states the current adapter status.

| Input class | Current use | Phase 0 classification | Adapter requirement |
|---|---|---|---|
| Campaign randomness | `runMetadataVoprCampaign` creates `DefaultPrng` from `cfg.seed`; transport action selection and parameters consume that stream. | Seeded, but opaque. A seed reproduces only while draw order and enabled actions remain identical. | Move every draw behind `ChoiceSource`; record the site, occurrence, canonical enabled IDs, and selected ID. |
| Network delivery | `VirtualHttpNetwork` owns a virtual tick, FIFO/seeded-random release, partitions, drops, duplication, and tick delays. Managed HTTP simulation forces synchronous transport and `sim://` endpoints. | Controlled. The internal random streams are independently seeded but are not yet recorded choices. | Expose message delivery and loss decisions as transitions; retain virtual ticks as world state. |
| Metadata time | `MetadataHttpClusterVopr` owns a `ManualClock`, starts at 1,000 ms, and advances 100 ms in `stepAll`/`stepAllExcept`. Retry clocks use virtual network ticks. | Controlled in the first world. | Make clock advance an explicit transition and include logical time in observations. |
| Real time fallback | `currentGroupStatusTimestampMs` calls `Clock.real().nowRealtimeMs()`. | Uncontrolled whenever this helper participates in scenario-visible state. | Inject the scenario clock and remove the fallback from replayable paths before the metadata Phase 1 exit condition. |
| Sleep | `DelayingRequestExecutor` and several Raft harness tests call POSIX `nanosleep`. | Uncontrolled and unsuitable for a replayable scenario. The first metadata VOPR uses virtual delay faults, but shares a module containing this path. | Reject real-delay executors in replayable worlds; model delay as queued events and virtual-clock advances. |
| Threads | There are no direct `std.Thread`/spawn calls in either audited harness. `ManagedHttpClusterSimulation.init` forces `async_transport = false`; virtual base URIs make host `start`/`stop` no-ops, and metadata background runtimes use manual mode. | Controlled in the first world. Broader non-virtual harness modes can transitively start listeners or async runtimes and are outside this claim. | Add construction-time capability checks so replayable scenarios require manual background runtimes, virtual endpoints, and synchronous transport; admit task wakeups only as scheduler transitions. |
| Filesystem | Campaigns allocate `std.testing.tmpDir`, construct LMDB/catalog paths, and execute real storage code beneath those roots. Directory names are nondeterministic but are not intended semantic inputs. Host filesystem behavior and persisted bytes are real. | Partially controlled. Unique clean roots prevent cross-run contamination, but paths, host I/O errors, and physical timing must not enter choices or observations. | Normalize paths out of artifacts, hash fixture contents, use modeled storage for injected failures, and retain physical-storage replay as a differential mode. |
| External network | The first metadata world replaces HTTP routes with `VirtualHttpNetwork`; blackhole endpoints use loopback only in other focused tests. | Controlled for the first world. | Fail scenario construction if any executor is not registered with the virtual network. |
| Iteration/order | Enabled VOPR actions are currently selected by integer ranges and local control flow; several maps/catalogs may have implementation-defined iteration order. | Not yet proven canonical. | Scenario adapters must emit explicit transition descriptors and call `List.canonicalize`; observations must use `Builder.canonicalize`. |
| Process/environment | Temp roots, target, optimize mode, and source revision can differ. | Diagnostic only unless they alter behavior. | Record them in the header, but determine replay compatibility from simulator ABI plus scenario version. |

Phase 0 established the contracts that enforce the right edge of this table.
The metadata adapter now records structured choices and canonical enabled sets,
selects individual virtual messages/node rounds/time advances, requires virtual
synchronous transport for the replayable world, and stamps simulated group
status from the cluster's injected manual clock rather than host realtime.
Debug and ReleaseSafe each pass the dedicated 100-consecutive exact-replay
gate. Wall-clock delay executors, real listeners, and physical filesystem
behavior remain confined to explicitly identified integration tests; they are
not silently admitted into the in-process metadata replay kernel.

The preserved table is now backed by the executable
`vopr-determinism-audit` gate. It checks every exported Antfly VOPR source and
the `DistributedDataVoprScenario` and `MetadataVoprScenario` regions in the
mixed legacy metadata harness. Direct host entropy, delayed private PRNGs,
platform clocks, native threads or I/O, native libraries, unordered iteration,
and pointer-derived identity fail the gate. The remaining physical-storage
differential boundaries are line-local allowances with mandatory rationale;
they are not permitted to contribute choices, observations, events, or stable
IDs. Runtime evidence independently catalogs immediate structured choices and
borrowed-`std.Io` entropy calls.

The production standby/scaling composition also audits `production_standby.zig`. Its
primary, standby receive/progress, replication slots, and fencing stores borrow
the shared VOPR filesystem and clock; promoted slot ownership preserves those
same dependencies. Automatic sharding borrows the owner's wall and monotonic
clocks, including cooldown expiry independently of realtime corrections, reads disk
size through its filesystem, and obtains median keys through the production
shard RPC adapter. Compact observations include standby durable/apply frontiers and production
Raft progress rather than native paths or addresses. The production fixture's
ancillary restore-job LMDB remains inside its established unique native temp
namespace and is not exercised as a modeled crash store by this history.

## Shadow-index-repair activation pacing (outside the first metadata world)

The full-cluster VOPR's `history-completes` liveness fix (fixing
`writeReadyForPhysicalFormatWithStorage` and `catchUpShadowReplacementUntil`'s
yield accounting) let `full-cluster.clean` and `full-cluster.resource-pressure`
run shadow-index-repair catch-up/activation to completion for the first time.
That exposed a second, previously-latent bug in the same code: the
activation/convergence pacing loop in
`pkg/antfly-embedded/src/storage/db/db.zig` measured elapsed time with the
module-local `monotonicTimeNs()`/`elapsedSince()` helpers, which call
`antfly_platform`'s `time.monotonicNs()` directly — the real OS monotonic
clock, unconditionally, with no `std.Io` indirection and therefore no way for
VOPR to control it. Before the liveness fix this code rarely ran long enough
to matter; after it, the pacing loop runs on every repair and reads real
wall-clock jitter into what VOPR's `choose`/replay machinery treats as
reproducible state, matching this file's long-standing "Real time fallback"
and "Sleep" risk categories for code outside the first metadata world.

| Function / call site | Old time source | New time source |
|---|---|---|
| `DB.repairPacingNowNs` / `DB.repairPacingClock` (new helpers) | n/a | `self.repair_pacing_clock orelse self.backend_runtime.monotonicClock()` — an injectable `platform.clock.Clock`, defaulting to `BackendRuntime.monotonicClock()`, which reads `std.Io.Clock.now(.awake, io)` when an `Io` is borrowed (VOPR's `VoprIo`, deterministic) and the real OS monotonic clock otherwise (`io() == null`, same value `platform_time.monotonicNs()` gave before) |
| `rebuildIndexWithShadowReplacementOwned`: `catch_up_start_ns`, `elapsedSince(catch_up_start_ns)` (initial and convergence-round catch-up), `activation_started_ns`, `activation_pause_ns`, `recordIndexRepairActivation`'s elapsed arg, `repairActivationReplayDeadline`'s `now_ns` arg | bare `monotonicTimeNs()` / `elapsedSince()` | `self.repairPacingNowNs()` / `self.repairPacingElapsedNs()` |
| `DB.ensureRepairActivationDeadline` (now takes `self`), `DB.lockApplyUntil`, `DB.beginIndexCatalogBarrierUntil` | bare `monotonicTimeNs()` | `self.repairPacingNowNs()` |
| `DB.catchUpShadowReplacementUntil`'s three deadline checks | bare `monotonicTimeNs()` | `self.repairPacingNowNs()`; also passes `.clock = self.repairPacingClock()` into `derived_worker.CatchUpOptions` so the window below observes the same clock |
| `derived/derived_worker.zig`: `catchUpIndexFromMatchingCursor`'s `call_started_ns` and its three `options.deadline_ns`/`max_call_ns` admission checks; `applyBatchBounded`'s `yield_deadline` check | bare module-local `monotonicTimeNs()` (`platform_time.monotonicNs()`) | new `CatchUpOptions.clock: ?platform_clock.Clock`, resolved through `catchUpNowNs(options.clock)` (falls back to the same bare `monotonicTimeNs()` when unset, so every caller besides shadow-index repair is unchanged) |

Left unconverted, deliberately: `collect_started_ns`/`stats.window_collect_ns`
and `apply_started_ns`/`stats.apply_ns` in `derived_worker.zig` (pure
telemetry, never compared against a deadline or fed into a decision), and the
many other `monotonicTimeNs()`/`platform_time.monotonicNs()` call sites in
`db.zig` and `catalog/index_manager.zig` used purely for logging or exported
metrics (e.g. `index_manager.zig`'s `metrics_enabled`/`debug_timing`-gated
profiling blocks). `index_manager.zig`'s quarantined-index-reopen backoff
(`graphMetricStatusHasRunnableWorkerPage`'s `platform_clock.Clock.real()`
fallback) is a related but distinct, still-real-time-backed mechanism outside
the shadow-index-repair activation path this change addresses; it was left
alone.

Verification: a new deterministic unit test,
`"repair activation pacing decisions follow the injected clock, not wall
time"` in `db.zig`, opens a `DB` with `repair_pacing_clock` set to a
`platform.clock.ManualClock`, and asserts `repairPacingNowNs()` reads exactly
the injected value and that `ensureRepairActivationDeadline` flips
deterministically and reproducibly at the exact nanosecond boundary, with no
dependence on real elapsed wall time. Independently, a temporary diagnostic
added while isolating this fix (reading `repairPacingNowNs()` under
`full-cluster-resource-pressure-vopr-test`, removed before committing)
confirmed the fix: the clock values recorded during VOPR's record pass and
its exact replay of the same choice sequence were bit-identical in order,
right up to the point of an unrelated, still-open `ReplaySelectedTransitionDiverged`
failure in that same scenario (a `vopr-io.packet_deliver` payload mismatch) —
proving this conversion closes the specific real-time leak it targets, while
that scenario's remaining divergence has a different, not-yet-identified
cause.

## Durable repair identity generation (continued investigation, partial)

Following up on the still-open divergence above: `full-cluster.clean` also
diverges with the identical symptom (`vopr-io.packet_deliver`, same packet
`parameter` i.e. byte length, different payload digest i.e. different
content), so the remaining cause is not resource-pressure-specific. One
contributing source, found and fixed: `index_repair_state.newRepairId`
(`derived/index_repair_state.zig`) draws its 16 bytes of identity from
`std.Options.debug_io` unconditionally — the global debug runtime, not
whatever `Io` the owning `DB` was actually opened with — which is this file's
"Direct host entropy" risk category. Under VOPR that bypasses `VoprIo`'s
seeded, replay-deterministic `randomSecure` entirely and draws real host
entropy instead, so every `IndexRepairIntent.repair_id` (and the dense-
artifact-counter attempt id, and the shadow generation id) differs between a
recorded run and its own exact replay. `DB.newDurableRepairId` (new helper
next to `repairPacingClock`) routes this through
`self.backend_runtime.io()` when one is borrowed — `VoprIo`'s seeded PRNG
under VOPR, the same real entropy source in production — and converts the
six `IndexRepairIntent`/dense-artifact-counter/shadow-generation call sites
in `db.zig` that have a `DB` receiver available. Left unconverted,
deliberately: `createUniqueRepairShadowBaseWithStorage`'s and
`createUniqueRepairShadowBase`'s own identity/nonce-based directory-naming
schemes (`db.zig`) — these only ever become a local filesystem path
component (`.repair-shadow-<id>`), never wire-visible content, so their
nondeterminism cannot by itself produce a replay-visible divergence.

A second, related source, also found and fixed: the durable repair-state
checkpoint's own `Location.io` (`derived/index_repair_state.zig`, consumed by
`newReplicaIdentityWithIo` for each DB/replica's own `db_identity`/
`replica_id`, which becomes part of every `IndexRepairIntent` built from that
checkpoint) is constructed in `core.zig`'s `openCoreResourcesFromPrimaryStore`
with no `Io` available to it at all, so it keeps `Location`'s own
`std.Options.debug_io` default — the same risk category, one level up.
`openCoreResourcesFromPrimaryStore` has a single caller (`db.zig`'s
`DB.open`), so rather than threading a new parameter through its already
long signature, `IndexRepairCheckpoint` gained its own `io` field (mirroring
`Location`'s) and `DB.open` patches it from `backend_runtime.io()` immediately
after `core` is constructed, before anything reads it.

**Status: verified not sufficient alone.** Both conversions are real,
individually-justified fixes (confirmed via `antfly-storage-test` that
existing checkpoint/repair-identity tests still pass), and the full-cluster
suite's unit-test prerequisites are unaffected, but `full-cluster.clean` and
`full-cluster.resource-pressure` still diverge after both are applied
(`full-cluster-vopr-test`: 23/25 both before and after, same two properties,
`ReplaySelectedTransitionDiverged` at `vopr-io.packet_deliver`, `parameter`
unchanged, payload still differs) — the choice index the divergence first
appears at shifts forward each time a source of nondeterminism earlier in the
history is removed, which is consistent with these conversions each
genuinely removing *a* source, just not *the* (or *the only*) remaining one.
`raft-vopr-test` (4/4) and `lsm-vopr-test` (3/3) stay green throughout. The
remaining source was not identified before this investigation paused; a
profitable next step is a temporary diagnostic that hashes/logs every HTTP
request and response body (not just the repair-pacing clock, which is now
proven deterministic) during a record pass and its own exact replay of
`full-cluster.clean`, to find the first byte range that differs and trace it
back to its source, the same technique that isolated the pacing-clock fix
above.

## Applied-sequence flush: a mutex held across a yield, not just a real-time leak

Continuing from the entropy fix above: `full-cluster-vopr-test` started
hanging outright (no output for an hour, `zig build full-cluster-vopr-test`
hit a 2h timeout) after `perf/lwa-integration` picked up origin/main. `sample`
on the live process during the hang showed the main thread parked for the
whole run inside `storage.db.mutation_commit...lockAtomicWithBackoffProfiled`'s
`sleepNs` (a raw `nanosleep`), reached through
`DB.independentMaintenanceRun`'s `.repair` branch →
`finalizeDenseProjectionLifecycleForIdle` → `flushAppliedSequences`, spinning
on `applied_sequence_mutex`. This is a different bug from the two entropy/
clock leaks above, not a consequence of them: a lock held across a
cooperative yield, with the raw-`nanosleep` backoff of a *contending* caller
compounding it into an outright deadlock.

`persistAppliedSequencesBatchAsync` (added by `02e6575157`, "skip redundant
progress-v2 commits and batch forced-flush persists") and the other four
callers of `flushPendingAppliedSequencesLocked`/
`flushFinishedDenseAppliedSequenceLocked` held `applied_sequence_mutex` —
a plain `std.atomic.Mutex`, spin-then-real-`nanosleep` on contention — across
the entire durable commit those functions run (`publishManagedProgress`,
which can go through a WAL append and its group-commit leader/follower wait:
a follower cooperatively yields through Io until the group's leader task
finishes the write and signals it). VOPR's deterministic scenario runs its
cooperative tasks on a single logical worker. Once that follower's holder
yielded while still holding `applied_sequence_mutex`, any *other* caller
contending for that same mutex fell into
`lockAtomicWithBackoffProfiled`'s raw-`nanosleep` backoff tail — blocking the
one OS thread the holder needed scheduled again to finish its commit and
release the lock. Nothing could run again: not the holder, not the group-
commit leader, not the contender. A resident production backend's real OS
thread pool has other threads free to run the holder forward, so this was
invisible there; VOPR's single-worker model turned it into a hang the moment
anything actually contended for this specific mutex while a flush was
yielding mid-commit, which the liveness fix two sections up made happen for
the first time (full-cluster's shadow-index repair previously never
survived long enough to reach a flush that overlapped the WAL's group-commit
wait with contention on the same mutex).

Two independent fixes, both required (confirmed by reproducing the hang with
either one alone):

1. **Never hold a plain spin-mutex across the yield.** Split what
   `applied_sequence_mutex` protected into two locks with different shapes.
   `applied_sequence_mutex` (unchanged type, still a bounded, never-yielding
   critical section) now protects only the coalescer's `pending` map itself
   — a brief drain, immediately copying out (`alloc.dupe`) and removing
   exactly the entries about to be flushed, so nothing below touches the map
   again and a `note()` arriving mid-commit lands cleanly in the
   now-partially-drained map for the next flush to pick up. The new
   `AsyncContext.applied_sequence_flush_mutex` (`std.Io.Mutex`) is acquired
   around the *whole* drain-then-commit sequence instead, by every caller of
   `flushPendingAppliedSequencesLocked`/`flushFinishedDenseAppliedSequenceLocked`.
   It exists because `savePhysicalProjectionMetadataForAppliedSequenceUpdates`
   and `checkpointManagedProjectionEffectsForAppliedSequenceUpdates` overwrite
   the durable checkpoint rather than merge it: two overlapping, unordered
   commits for the same index could otherwise regress the watermark once
   `applied_sequence_mutex` itself stopped serializing the whole operation.
2. **`lockAtomicWithBackoff`/`lockAtomicWithBackoffProfiled` must not fall
   back to a raw `nanosleep`.** Added Io-aware counterparts,
   `lockAtomicWithBackoffIo`/`lockAtomicWithBackoffProfiledIo`
   (`mutation_commit.zig`), identical except their backoff tail calls
   `io.sleep(..., .awake)` instead of `sleepNs`. Used only for
   `applied_sequence_mutex`'s now-brief critical section (six call sites in
   `db.zig`), not for the other ~60 `lockAtomicWithBackoff(Profiled)` call
   sites in this file (`dense_admission.mutex`, `index_repair_control_mutex`,
   and others): none of those were implicated in the hang, and routing all
   of them through an explicit `Io` would mean auditing every one of their
   callers for a reachable `Io` and for its own cooperative-yield safety —
   out of scope for this fix. They remain bare `std.atomic.Mutex` spinlocks,
   same as before; if any of them is ever found holding across a yield the
   same way, it needs the same two-part treatment, not just the backoff
   swap alone.

Production behavior is unchanged: `io.sleep` and `BackendRuntime`'s own
`io()` both resolve to the real OS clock/thread when not running under VOPR,
identical to the `nanosleep`/`std.atomic.Mutex` they replace for
`applied_sequence_mutex` specifically.

Regression: `execution_resources.zig`'s `"a mutex held across a cooperative
yield does not deadlock a single-worker Io scheduler"` constructs a
single-worker `std.Io.Threaded` (`.async_limit = .nothing`, the same shape
VOPR's cooperative scenario has) and runs two concurrent tasks on a bare
`std.Io.Mutex`: task A locks it, `io.sleep`s while still holding it (the
exact shape `flushPendingAppliedSequencesLocked` has around its commit), then
releases; task B starts once A already holds the lock and contends for it.
The test hangs (and the enclosing build step times out) if `std.Io.Mutex`
ever regresses to blocking the underlying OS thread instead of cooperatively
parking a contending task; it also asserts task B never observes the
critical section as available before task A actually released it, i.e. that
splitting the two locks did not also break mutual exclusion.

Verified: the full-cluster VOPR hang is gone —
`full-cluster-vopr-diag` (the clean/resource-pressure bundle) completes in
its ordinary ~10-15 minutes instead of hitting the 50-minute timeout, back
to the same `23/25, same two ReplaySelectedTransitionDiverged properties`
state the entropy-fix section above already described and left open.
`raft-vopr-test` and `lsm-vopr-test` stay green.

## Replay-content divergence: `graph_ttl_now_ns` leaking real time into a wire payload

With the hang above fixed, `full-cluster.clean` and
`full-cluster.resource-pressure` both diverged on exact replay with
`ReplaySelectedTransitionDiverged` at `vopr-io.packet_deliver`: same packet
length, different payload digest. A temporary packet-content logging hook
in `vopr_io_net.zig`'s `executeReady` (id/actor/resource/len/digest/160-byte
prefix per delivered packet, gated behind a compile-time flag, never
committed enabled) isolated the diverging packet on its own, uninterleaved
test run (a throwaway `b.addTest` filtered to exactly one test, since the
shared test binary's output from unrelated tests otherwise interleaves with
the target test's own stdout and defeats any ordering-sensitive diff).

The harness's own replay check already reports the first diverging choice
directly (`choice=23686`, `expected`/`actual` payload hashes, same
`parameter=1050` length) — no manual two-pass diffing needed. The packet:
a `POST /internal/v1/groups/<g>/tables/docs/query` whose first 160 bytes
(method, headers, and the start of the
`x-antfly-prepared-query-routing` header value) were byte-identical between
record and replay; the difference was further into that header's JSON body.
`prepared_query_routing.Envelope.graph_ttl_now_ns` is a wall-clock deadline
anchor the coordinator stamps onto a request and ships to the remote group
in that header. Four call sites read it from `platform_time.realtimeNs()`
— the host's real OS clock — directly, unconditionally, with no VOPR
awareness at all:

- `table_reads.zig`'s `graphTimedSearchRequest` and `graphScopedSearchRequest`
  (the coordinator-side stamping before a query is dispatched)
- `distributed_graph.zig`'s `executeCrossRange` and
  `executeCrossRangeWithMatchAnchorsLifecycle` (the same stamp on the
  cross-range graph-coordinator path, reached when the first two leave
  `graph_ttl_now_ns` at 0)

Record and replay each call `platform_time.realtimeNs()` at a different
real wall-clock instant, so the stamped value — and therefore the request
body's bytes — differ between the two passes even though every VOPR-
controlled input (PRNG draws, scheduling order, the simulated clock) replays
identically. This is exactly the "real time leaking into replay" class
described earlier in this document, just reached through the query/graph
request-construction path instead of repair pacing.

Fix: route each call through the attached simulation `Io`'s `.real` clock
instead of the host's. `table_reads.zig` already carries a `?FanoutIo` on
both `ProvisionedTableReadSource` and `HostedProvisionedTableReadSource`
(`self.io_impl`, used by the existing `monotonicNs()` helper for the
`.awake` clock); `graphTimedSearchRequest`/`graphScopedSearchRequest` now
take that `?FanoutIo` and a new `graphDeadlineRealtimeNs` helper reads
`std.Io.Clock.now(.real, resolved.io())` when one is attached, falling back
to `platform_time.realtimeNs()` only when it isn't (outside VOPR, and in the
three unit tests that call these helpers directly with `null`).
`distributed_graph.zig`'s `Worker` already exposes `worker.fanoutIo() ?std.Io`
for the same purpose elsewhere in that file, so its `graphDeadlineRealtimeNs`
helper reads `std.Io.Clock.now(.real, io)` off that instead, with the same
real-clock fallback; this also fixed two further leaks on the incoming-edge
probe path (`appendIncomingProbeBatches`, `probeIncomingEdgesForKeysWithOptions`)
that stamp `IncomingProbeOptions.ttl_now_ns` the same way. All four sites
converge on `VoprIo`'s own `realtime_ns` field under simulation — the same
clock `BackendRuntime.clock()` already uses for the `GraphIndex`-level
`platform.Clock` (`db.zig`'s `self.backend_runtime.clock()`), so there is no
baseline mismatch between the coordinator's stamped deadline and the
receiving side's own clock. Production behavior is unchanged: outside VOPR
there is no attached `Io`/`Worker` fanout `Io`, so every converted site falls
straight back to `platform_time.realtimeNs()`, the same call it made before.

Not converted (left as direct `platform_time.realtimeNs()`/
`std.posix.clock_gettime(.REALTIME)` calls): the dozens of other real-time
reads elsewhere in the tree (serverless lease timestamps, lake snapshot pin
bookkeeping, standalone-runtime auth proof timestamps, etc.) that are not on
the request/response path this divergence investigation traced a byte to.
Converting those was out of scope for this fix; each would need its own
confirmed leak (a VOPR scenario that actually exercises it and diverges) before
being worth the same treatment, per this document's running principle of
fixing a proven leak at its root rather than sweeping every real-time call
in the codebase.

Regression: `table_reads.zig`'s existing unit test for these two helpers
(`"single-group graph requests carry the complete-snapshot execution
scope"`) now exercises the `io_impl`-threading signature with `null`
(unchanged behavior outside a simulation); a VOPR-specific regression is
the full-cluster suite itself — see below.

Verified (isolated, single-test diagnostic binary, `full-cluster.clean`
only, to avoid interleaving another test's stdout with the packet log):
the `ReplaySelectedTransitionDiverged` at `vopr-io.packet_deliver` for
`full-cluster.clean` is gone; its exact replay now matches the recorded run
bit-for-bit. That same run surfaced a *different*, previously-unreached
failure for `full-cluster.metadata-partition` (`history-completes`, a
liveness-class failure, not a replay-content divergence) — unreached before
because this test's mode loop runs `full-cluster.clean` first and `try`s
every mode in sequence, so no later mode in this test function had ever
been exercised past the point where `clean` itself used to fail. This is
new, previously-invisible territory, not a regression from this fix (the
`graph_ttl_now_ns` value feeds a header on a cross-group query path that
`metadata-partition`'s early-stage traffic barely touches — its own failure
dump shows every `production-*` feature still at 0), and is out of scope for
this document's determinism focus: it is a liveness bug, not a nondeterminism
one. Left for separate investigation; full `full-cluster-vopr-test` 25/25
twice is not yet reached pending that.

## `PrefixedFairSeeded` could starve `time_advance` for an entire transition budget

Continuing the metadata-partition investigation above: fixing the
transaction-begin classification gap (see `internal_group_operations.zig`'s
commit) got `full-cluster.metadata-partition`'s production read/write/tenant
round trip to succeed, but `history-completes` still failed. Isolated
diagnostics narrowed it to one HTTP GET (`fetchLookup` for a cross-table
isolation probe) that never resolved — logging showed the request was still
pending 4+ *virtual* seconds in, with no sign of progress, yet nothing else
in the scenario ever reported a hang either; the whole 50,000-transition
budget just ran out first.

The actual bug: `vopr.choice.PrefixedFairSeeded` — the "fair" scheduling
policy `full-cluster.zig` uses for every non-`production_*` mode (`clean`,
`metadata-partition`, etc., via `choice.PrefixedFairSeeded.init` in
`Scenario.run`'s `choice_source` selection) — picks a `vopr-io.time_advance`
transition only when *no other* transition is enabled that round. Its
sibling, `PrefixedCooperativeSeeded` (used for `production_*` modes), has
the same postponement but bounds it: after `max_non_time_choices` (256)
consecutive non-time picks it forces a `time_advance` pick anyway.
`PrefixedFairSeeded` had no such bound — unconditional, indefinite
postponement. A full-cluster scenario with a few dozen concurrently-live
service fibers (confirmed via `full-cluster.active-tasks` in the failure
dump: ~40) *always* has some non-time transition enabled, so `time_advance`
was starved for the scenario's entire transition budget. This is a pure
scheduler-fairness bug, not a leak of the host's real clock — VOPR's
simulated `.awake`/`.real` clocks were already correctly deterministic and
controlled; they simply never got told to advance.

The consequence: any deadline whose only path to firing is a `time_advance`
transition is indistinguishable from a true hang under `PrefixedFairSeeded`
scenarios that stay busy. This almost certainly includes server-side
catalog/read-index readiness retries with a deadline (`lookupWithReadinessRetry`
in `http_server.zig` retries every 50ms for up to 5 seconds on
`StorageReadTemporarilyUnavailable`-class errors) and any client-side
request timeout — confirmed concretely: adding a non-`null` `timeout_ms` to
an otherwise-untimed-out `fetchLookup` call (attempting to fix the hang
above) made the *previously-passing* `full-cluster.clean` mode hang too,
on the exact same symptom, with no fault injected at all. That dead end is
what led here: the timeout itself wasn't broken, `time_advance` just never
got picked while anything else was runnable, so the watchdog timer behind
`httpx`'s own request-timeout `Io.Select` race never fired either.

Fix: gave `PrefixedFairSeeded` the same `non_time_choices`/
`max_non_time_choices` bound `PrefixedCooperativeSeeded` already has,
sharing its `default_max_non_time_choices` constant and its `isTimeAdvance`
helper. "Postpones time while application work is runnable" (the existing,
still-true doc comment and still-passing test) is now bounded rather than
unconditional.

Regression: `"prefixed fair seeded source eventually forces time_advance
when work never stops being runnable"` in `lib/vopr/src/choice.zig` keeps a
non-time transition continuously enabled alongside `time_advance` and
asserts the latter wins within `max_non_time_choices + 1` choices — it
previously would never win. Fast, fully isolated (`lib/vopr`'s own `zig
build test`, no antfly binary needed); confirmed passing.

Verified: full-cluster.metadata-partition's transaction-begin failures stay
fixed and the production round trip keeps succeeding with this change
(unaffected, as expected — it's a scheduler-fairness fix, not a correctness
one for that path). `history-completes` for metadata-partition is **not**
yet green: the isolation-probe lookup still doesn't resolve, now confirmed
*not* to be this starvation bug (this fix alone doesn't change the outcome),
and its true root is a separate, still-open investigation — see the
coordinator report for where that stands. `full-cluster-vopr-test` 25/25
twice is not yet reached.
