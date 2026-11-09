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
