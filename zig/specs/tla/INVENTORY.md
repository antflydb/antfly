# TLA+ Model Coverage Inventory

This inventory is the working map between Antfly's Zig implementation, tests,
and TLA+ models. The `Status` column deliberately records evidence shape, not
a maturity score:

- **Checked**: TLC checks a bounded abstraction, mutants demonstrate that its
  named properties are load-bearing, and named implementation tests exist.
  This does not imply implementation refinement or completeness.
- **Bounded**: the abstraction is useful, but important state, fairness, or
  executable correspondence remains outside it.
- **Contract**: deliberately small design guardrail with collapsed internals.
- **Trace-linked**: checked fixtures are consumed by a trace-refinement model.
- **Routed**: direct tests, generated-artifact comparison, fuzzing, or
  benchmarks are stronger evidence than a TLA+ abstraction for this concern.

Every row should eventually have a negative-validation entry showing that the
listed invariant can fail when the modeled bug is injected.

Remaining weak spots are tracked inline in this file: the Safety-pin
migration debt list (audit-reported), the liveness mutant backlog, the
bounds backlog, and the routed work items below.

## Coverage Matrix

| Domain | TLA model/config | Status | Code anchors | Test/trace anchors | Current invariants | Known gaps | Next validation step |
|---|---|---:|---|---|---|---|---|
| Distributed transactions | `AntflyTransaction.tla`, `MC.tla`, `AntflyTransaction.cfgs`, `AntflyTransactionBadSkipIntentConflict` | Checked | `pkg/antfly/src/storage/db/db.zig`, transaction recovery/runtime code | transaction lifecycle, OCC predicate, participant recovery tests; transaction trace validation | atomicity, no orphaned intents, OCC serialization, LWW/read consistency | session savepoints and identity side effects are mostly outside this model; only one core mutant is currently wired | add a second negative config for orphaned-intent cleanup if per-invariant mutant coverage is required |
| Transaction traces | `TraceAntflyTransaction.tla`, `TraceAntflyTransaction.cfgs` | Trace-linked | transaction trace emitter/filter scripts | `make tla-trace-txn TRACE_FILES=...` | trace consumption plus transaction safety invariants | trace filtering can hide unsupported events; coverage depends on emitted test traces | add documented trace generation command and malformed-trace negative check |
| Session savepoints | `AntflyTransactionSession.tla`, `.cfgs`, `AntflyTransactionSessionBadRollback`, `AntflyTransactionSessionBadRecoveryDecision`, `AntflyTransactionSessionBadCleanup` | Bounded | transaction/session handling and recovery in `db.zig`, `pkg/antfly/src/storage/transactions.zig` | transaction abort/commit/recovery tests, participant cleanup tests, identity-row recovery test, `lib-db-txn-test` | committed-base visibility separated from staged writes, rollback-to-savepoint discards staged intents, all prepared participants must resolve before cleanup, finalized committed orphan intents publish document and identity rows, finalized aborted orphan intents publish neither | still abstracts OCC predicate details already covered by `AntflyTransaction.tla`, exact transaction record layout, per-document versions/timestamps, multiple keys, local vs remote participant metadata shape, trace refinement, and exact low-level recovery batch rows | map transaction trace events to session/recovery actions and add a heavier multi-key/version-predicate config |
| Transaction session trace fixtures | `TraceAntflyTransactionSession.tla`, `.cfgs`, `specs/tla/traces/txn_session_*.ndjson`, `specs/tla/traces/negative/txn_session_bad_cleanup.ndjson` | Trace-linked | `pkg/antfly/src/storage/transactions.zig`, `pkg/antfly/src/api/transactions.zig`, `pkg/antfly/src/api/http_server.zig` | checked-in fixtures derived from recoverTransactions, session savepoint, participant cleanup, and HTTP session-route scenarios; `make tla-trace-txn-session TRACE_FILES="specs/tla/traces/txn_session_*.ndjson"` | trace consumption, post-action state assertions, rollback-discarded data remains invisible, committed finalized orphan publishes docs and identity rows, aborted finalized orphan publishes neither, stale pending auto-aborts, cleanup requires all prepared participants resolved | fixtures are checked in rather than emitted by live Zig tests; counts stand in for concrete keys/version timestamps; no OCC predicate trace bridge; direct standalone `zig test pkg/antfly/src/api/transactions.zig` does not compile outside build-module wiring | add live or generated trace hooks for `transactions.zig` recovery tests if trace coverage should become implementation-emitted |
| Batcher coalescing | `AntflyBatcherCoalescing.tla`, `.cfgs`, delete/write, write/delete, and partial-visibility bad cfgs | Bounded | provisioned write coalescer and DB batch coalescing in `table_writes.zig` / `db.zig` | focused provisioned coalescer tests, including write->delete and delete->write correspondence cases | last operation wins per key for both operation orders; stale delete/write visibility is rejected; flush publication is not partially visible | abstracts payload bytes, multi-key conflict indexing, backpressure, allocator failure, and transaction/session integration; the new delete->write correspondence test likely exposes a product order-loss bug | fix the provisioned coalescer order issue separately, then keep the focused test as the code anchor |
| Shard split | `AntflyShardSplit.tla`, `ShardSplitMC.tla`, `.cfgs`, `AntflyShardSplitBadPrematureChildDefault` | Bounded | raft/shard split orchestration and metadata routing | split prepare/finalize, routing, rollback tests | no data loss, no double-serving, no premature cutover | its liveness assumes destination eligibility once leader/replay facts hold; B1 showed the implementation also depends on independently aggregated stable-placement evidence | keep the data-path model separate, and carry stable-placement readiness through `AntflyPlacementReadiness` and the refinement bridge |
| Shard split sequence deltas | `AntflyShardSplitSeq.tla`, `.cfgs`, `AntflyShardSplitSeqBadKeySetCutover` | Bounded | shard split fence/cutover delta tracking | split prepare/finalize and DB split replay tests | cutover preserves every fenced write sequence, repeated writes to one key cannot collapse to a set-membership proof, stale fences cannot complete after later parent writes | bounded to two writes and one moved range; abstracts payload bytes, leader election, and multi-child split shapes | keep this as a focused sibling for sequence bugs that would be hidden by the broader set-oriented split model |
| DB split visibility | `AntflyDbSplitVisibility.tla`, `.cfgs`, `AntflyDbSplitVisibilityBadParentWrite`, `AntflyDbSplitVisibilityBadChildServe`, `AntflyDbSplitVisibilityBadMergeDonor`, `AntflyDbSplitVisibilityBadEnrichmentOwner` | Bounded | `pkg/antfly/src/storage/db/db.zig`, split range/index replay code, range-state persistence, enrichment cutover fencing | DB split/merge/fencing tests, durable split/reopen tests, durable index routing tests | parent right-range ownership is trimmed at split cutover, parent cannot accept child-range writes after cutover, split deltas are replayed before child serving, child text/sparse/graph shadow indexes are complete before serving, child artifacts move remote after split finalization, enrichment publishes only for current right-range owner, merge donor cannot serve after handoff, receiver routes text/sparse/graph indexes through merged range | still abstracts exact encoded split-delta/artifact rows, shard-level split metadata refinement, non-atomic crash points inside prepare/finalize, deletion payloads, multiple child ranges, exact generated-enrichment payload replay, and byte-level index contents | add a trace/refinement bridge to `AntflyShardSplit.tla`, add crash/reopen actions around split finalize and merge finalize, and expand range ownership to multiple child ranges/artifact IDs |
| Split refinement bridge | `AntflySplitRefinementBridge.tla`, `.cfgs`, placement-bootstrap, route, serving, and stale-fence Bad* configs | Bounded | `AntflyPlacementReadiness.tla`, `AntflyShardSplit.tla`, `AntflyDbSplitVisibility.tla`, split transition driver and reconciler | focused DB split/merge tests and checked split bridge fixtures | destination bootstrap requires stable placement; shard cutover requires a current fence and DB replay; child serving requires shard cutover plus text/sparse/graph catch-up; routing requires both layers | the voter evidence itself remains in the placement-readiness model; fixtures are checked in rather than emitted by the implementation; one right-range child | add stable-placement fields to implementation-emitted transition diagnostics and generate a B1-shaped bridge fixture |
| Query completeness at split routing | `AntflyQueryCompleteness.tla`, `.cfgs`, route-before-ready, double-serve, and missing-doc bad cfgs | Bounded | query routing over split parent/child ownership and DB child serving publication | split routing/query completeness tests and split bridge fixtures | query-visible routing cannot move to the child before child serving is complete, a moved doc is neither missing nor double-counted while parent ownership is trimmed | intentionally excludes ranking, top-K, vector distance, analyzers, pagination math, and byte contents; single moved doc and one split boundary | keep with split/route tests; model only ordering/visibility, and validate ranking/pagination with property/golden tests |
| Split bridge trace fixtures | `TraceAntflySplitRefinementBridge.tla`, `.cfgs`, `traces/split_bridge_*.ndjson`, `traces/negative/split_bridge_route_before_db_serving.ndjson` | Trace-linked | `AntflySplitRefinementBridge.tla`, split metadata transition and DB split visibility tests | checked-in cutover, rollback, and route-before-serving fixtures; `make tla-trace-split-bridge TRACE_FILES="specs/tla/traces/split_bridge_*.ndjson"` | trace consumption, after-state assertions for key bridge variables, same bridge safety invariants as the model | fixtures are hand-authored rather than emitted by Zig tests; no concrete range IDs, shard leader events, or persisted DB row payloads | add a generated-fixture hook from focused split tests if this boundary needs tighter implementation correspondence |
| Snapshot transfer | `AntflySnapshotTransfer.tla`, `SnapshotTransferMC.tla`, safety/full cfgs, `AntflySnapshotTransferBadApplyWithoutPut` | Checked | raft snapshot transport/storage | snapshot transfer tests | no stale apply, retry bounds, GC safety, no fetching without need, applied snapshot has local archive | safety config is routinely used; full liveness is heavier | add scheduled full-liveness run notes and expected runtime |
| Snapshot content provenance | `AntflySnapshotContent.tla`, `.cfgs`, wrong-content and GC-needed-content bad cfgs | Bounded | raft snapshot archive/apply and GC provenance | snapshot transfer/apply/GC tests | applied snapshot content must match the applied raft index; fetched target content cannot be GC'd before apply | abstracts actual bytes, checksums, transport chunking, retry classes, and full liveness | use byte/checksum restore tests outside TLA; keep the GC-needed-content mutant wired to prevent dead-guard regressions |
| Raft scheduler fairness | `AntflyRaftSchedulerFairness.tla`, `.cfgs`, hot-tick and hot-Ready bad cfgs | Checked | `lib/raft/src/runtime/scheduler.zig` bounded tick/Ready scans | focused scheduler tests for hot-group tick and Ready starvation | every active group is selected within one bounded scan; weakly fair execution continues to tick and drain Ready for every group | abstracts quiesce/register churn, wall-clock duration, transport backpressure, and payload size | keep the round-robin correspondence tests paired with both scheduler cursors |
| Raft Ready pipeline | `AntflyRaftReadyPipeline.tla`, `.cfgs`, scheduling, ownership, message, and membership Bad* configs | Checked | `lib/raft/src/runtime/scheduler.zig`, `lib/raft/src/runtime/multi_raft.zig` | focused scheduler/MultiRaft tests, including backpressure rejection before cloning | fair coverage precedes productive continuations; denied work owns no messages; config apply follows ownership; Ready messages survive apply; snapshot membership matches its applied index; budget remains bounded | abstracts payload bytes, persistence/transport failure after ownership, quiesce/register churn, and wall-clock diagnostics | retain the fast projections and composed heavy liveness check |
| Raft consensus | `etcdraft.tla`, `MCetcdraft.tla`, `.cfg` | Checked | `pkg/antfly/src/raft`, `lib/raft` | raft tests and trace validation | election/log/commit durability invariants | direct config-change and snapshot edge coverage depends on trace richness | keep trace segmentation fixtures updated with implementation events |
| Raft traces | `Traceetcdraft.tla`, `.cfg` | Trace-linked | raft trace logger and segmentation scripts | `make tla-trace-raft TRACE_FILES=...` | trace matched, raft safety invariants | trace compatibility shims need ongoing review when raft internals change | add malformed or reordered event negative trace |
| LSM lifecycle/OOM | `AntflyLsmLifecycle.tla`, `.cfgs`, `AntflyLsmLifecycleBadIndexTempLeak` | Checked / bounded | LSM read cache, mutable snapshot, index writer cleanup | LSM lifecycle and OOM-style tests | cache lease/free safety, snapshot reader safety, temp index cleanup | does not cover WAL segment/checkpoint durability; only one allocator-failure mutant is currently wired | keep separate from WAL model; add more allocator-failure mutants only for newly identified ownership handoff bugs |
| LSM reserve/failure cleanup | `AntflyLsmReserveCleanup.tla`, `.cfgs`, `AntflyLsmReserveCleanupBadPublishWithoutReserve`, `AntflyLsmReserveCleanupBadFailureLeaksTemp` | Bounded | LSM write/flush/cleanup reservation and temporary ownership handoffs | LSM lifecycle and allocator-failure tests | publication requires a cleanup reserve; failed operations release temporary ownership before quiescence | abstracts file bytes, WAL replay, compaction generation selection, and exact allocator classes | keep as a small explicit-reserve sibling; add code-emitted traces only if cleanup ownership changes frequently |
| LSM WAL/checkpoint/compaction | `AntflyLsmWalCompaction.tla`, `.cfgs`, `AntflyLsmWalCompactionBadCheckpoint`, `AntflyLsmWalCompactionBadCorruptRotate`, `AntflyLsmWalCompactionBadPinnedRetire` | Bounded | `pkg/antfly/src/storage/lsm_backend`, WAL/storage IO | WAL segment/corrupt-tail tests, LSM backend reopen/checkpoint/retirement tests, root snapshot pinning test | segment-aware WAL append/sync/replay, durable checkpoint prefix, corrupt current-tail isolation, compaction input/publication ordering, reader-pinned segment retention, safe segment retirement | abstracts record bytes/CRC validation, temp-file replacement, run-file generations, full manifest contents, exact mutable flush rows, and multi-reader generation mapping | map exact backend APIs to actions and add trace fixtures for replay/reopen sequences |
| LMDB commit/readers | `AntflyLmdbCommit.tla`, `.cfgs`, `AntflyLmdbCommitBadMetaBeforeData`, `AntflyLmdbCommitBadReaderReuse` | Bounded | `pkg/antfly/src/lmdb/txn.zig`, `prepare_commit_support.zig`, `commit_support.zig`, `free_db.zig`, `readers.zig`, wrapper transaction handling | LMDB crash publish phase fixtures, active reader/free-page reuse tests, oldest-reader reclaim tests, nested child transaction tests | two meta-page selection, prepared data pages, data-sync before meta-write, crash/reopen meta choice, reader snapshots over page images, free-record reuse gated by oldest reader, child commit/abort merge shape | abstracts exact B-tree shape, page bytes/checksums, named DB metadata details, overflow span layout, writer-thread/async scheduling internals, local reader mmap remap lifetime, and map/no-sync policy variants | map simulation fixtures to model actions and add heavier config with multiple dirty pages/retired pages |
| HA gates | `AntflyHAGates.tla`, `.cfgs`, `AntflyHAGatesBadStandbyRuntime` | Bounded | HA write/read/owner-job gate code and DB standby runtime startup gating | HA DB gate tests, standby runtime suppression test, `ha-compat-test`, `ha-chaos-test` | fail-closed does not append/ack unsatisfied writes, fenced primary cannot write, owner jobs and mutating background runtimes require an unfenced primary, standby does not run mutating background runtimes | static decision-table model, no stream timeline or standby apply state machine; background runtime model is a coarse allow/suppress decision | add table row provenance to code branches and negative ack-on-degrade config |
| HA gate transitions | `AntflyHAGateTransitions.tla`, `.cfgs`, `AntflyHAGateTransitionsBadStaleAllow` | Bounded | HA write/read/background runtime gates across role/fence transitions | HA write-gate, standby runtime, and transition tests | mutating decisions after a transition require the current role to be unfenced primary; stale allow decisions cannot survive demotion or fencing | abstracts stream progress and exact request payloads; it is a transition sibling to the static decision table | keep with HA gate tests; add live trace events only if stale decision caching appears in implementation |
| HA replication | `AntflyHAReplication.tla`, `.cfgs`, `AntflyHAReplicationBadStaleTimelineAck` | Bounded | HA replication records, write gates, sync wait provider, standby apply | HA primary timeline/status tests, HA DB primary-progress sync wait tests, standby write gate tests, HA chaos restart/timeline tests | eligible sync slots current/active/not reseed, fail-closed ack safety, sync waits freeze target timeline/LSN/mode, accepted ack timeline matches target timeline, accepted ack LSN satisfies target | model is broad but still abstracts record payload shapes, exact stream record bytes, standby apply payload semantics, and sync wait provider scheduling; post-wait transition boundary is intentionally narrowed to receive/apply/status-ack paths to keep the heavy check practical | add trace refinement from HA chaos/compat events |
| HA failover safety | `AntflyHAFailoverSafety.tla`, `.cfgs`, `AntflyHAFailoverSafetyBadPromoteMissingAck`, `AntflyHAFailoverSafetyBadOldPrimaryWrite` | Bounded | HA promotion/fencing design contracts, sync acknowledgement evidence, old-primary write authority boundaries | HA sync-boundary, promotion, timeline-switch, and rejoin tests; currently model-checked rather than trace-refined | acknowledged writes are preserved by the promoted standby, every ack has pre-promotion standby evidence, promotion requires a fence, promoted standby and old primary cannot both be writable, old-primary post-promotion writes are rejected | focused two-standby model; abstracts async/RPO modes, exact sync policy/quorum selection, WAL bytes, admin tokens, base-backup manifests, and HTTP promotion mechanics | add trace fixtures or implementation-emitted events from promotion/fencing tests if this becomes a release gate |
| HA partition fence delivery | `AntflyHAPartitionFence.tla`, `.cfgs`, `AntflyHAPartitionFenceBadPromoteBeforeFence` | Bounded | HA promotion/fence propagation and old-primary write authority | HA chaos partition/fence/promotion tests | promotion requires the old primary to have received the fence; old primary and promoted primary cannot both be writable; old-primary writes after promotion are rejected | abstracts lease clocks, admin auth, HTTP transport retries, and WAL bytes; complements `AntflyHAFailoverSafety` rather than replacing it | add implementation-emitted HA promotion/fence trace fixtures if this becomes a merge gate |
| HA sync wait | `AntflyHASyncWait.tla`, `.cfgs`, `AntflyHASyncWaitBadMoveTarget`, `AntflyHASyncWaitBadWrongTimelineAck`, `AntflyHASyncWaitBadBelowTargetAck` | Bounded | HA sync wait provider, primary status ack handling, slot timeline/progress status, promotion timeline changes | HA DB sync wait tests, HA primary status timeline rejection tests, HA chaos sync-boundary restart tests, broad `ha-test` | frozen sync target timeline/LSN cannot move after promotion, accepted ack matches the frozen timeline, accepted ack LSN satisfies the frozen target, captured ack source timeline/LSN proves the slot was eligible at acceptance time | deliberately small fast submodel; abstracts wait-provider scheduling, quorum selection, exact slot store persistence, remote-write vs remote-apply mode distinction, and record bytes already covered by broader HA tests/models | add trace refinement from HA sync-boundary chaos events |
| HA timeline switch | `AntflyHATimelineSwitch.tla`, `.cfgs`, `AntflyHATimelineSwitchBadBeforeApplied`, `AntflyHATimelineSwitchBadNonMonotonic`, `AntflyHATimelineSwitchBadOldTimeline`, `AntflyHATimelineSwitchBadRecoveryPrevious` | Bounded | `pkg/antfly/src/storage/ha/standby.zig`, `primary.zig`, `http_replication_client.zig`, DB replication-boundary handling | standby timeline-switch tests, promoted reopen/reject-old tests, primary old-timeline status rejection, DB durable replication-boundary test, HA chaos timeline-switch restart test, broad `ha-test` | switch record requires parent received/applied/safe progress caught up, switch timeline/epoch are monotonic, switch record LSN is contiguous with `previous_lsn`, switched identity/progress match the switch record, crash recovery uses the recovered parent progress as switch `previous_lsn`, old-timeline records are rejected after switching | deliberately small fast submodel; abstracts payload JSON fields, cluster/shard/table identity checks, receive-log byte encoding, progress WAL serialization, HTTP frame metadata, and promotion fencing details covered by other HA tests/models | add trace refinement from HA chaos timeline-switch events |
| HA standby apply | `AntflyHAStandbyApply.tla`, `.cfgs`, `AntflyHAStandbyApplyBadFailureAdvances`, `AntflyHAStandbyApplyBadDuplicateEffect`, `AntflyHAStandbyApplyBadCrashLosesReceive`, `AntflyHAStandbyApplyBadClientWrite`, `AntflyHAStandbyApplyBadBackgroundRuntime` | Bounded | `pkg/antfly/src/storage/ha/standby.zig`, `pkg/antfly/src/storage/db/db.zig`, `pkg/antfly/src/storage/ha/write_gate.zig`, DB standby runtime startup gating | standby durable receive/apply/reopen tests, DB apply-failure remote-write ack tests, standby write-gate/runtime suppression tests, HA chaos receive/apply crash tests, broad `ha-test` | failed apply cannot advance applied/safe read/DB marker progress, replicated apply is idempotent and does not duplicate side effects, durable receive survives crash/reopen before apply, client writes are rejected on standby, mutating background runtimes are suppressed on standby | deliberately small fast submodel; abstracts record payload bytes, exact apply callback semantics, progress WAL serialization, DB transaction contents, primary slot status update flow, and multiple payload kinds | add HA trace refinement from chaos receive/apply events |
| HA former-primary rejoin | `AntflyHARejoin.tla`, `.cfgs`, `AntflyHARejoinBadUnfencedRewind`, `AntflyHARejoinBadExpiredWalRewind`, `AntflyHARejoinBadForcedRewind`, `AntflyHARejoinBadIdentityMismatchRewind`, `AntflyHARejoinBadStaleAssessment`, `AntflyHARejoinBadForkMismatch` | Bounded | `pkg/antfly/src/storage/ha/rejoin.zig`, `operator.zig`, `http_admin.zig`, `http_client.zig`, `chaos.zig` | rejoin unit tests, operator rejoin plan tests, admin/http rejoin tests, retention/rejoin chaos tests, broad `ha-test` | no-fence assessment rejects instead of rewinding, rewind requires compatible fence identity/old-primary/parent timeline, retained WAL covers fork, forced promotion needs explicit policy, stale assessment cannot truncate after a late write, fork record must be retained and identity-matched before truncation, reseed execution publishes reseed/base-backup requirements, data-loss flag is exact | deliberately small fast submodel; abstracts exact JSON/admin wire encoding, token strings, all identity fields as booleans, replication-log byte layout, backup manifest contents, and route/admin URL selection | add trace refinement from HA rejoin/admin events and split seed manifest/base-backup detail only if it grows beyond operator tests |
| HA trace fixtures | `TraceAntflyHA.tla`, `TraceAntflyHA.cfgs`, `specs/tla/traces/ha_*.ndjson` | Trace-linked | `pkg/antfly/src/storage/ha/standby.zig`, `primary.zig`, `rejoin.zig`, `operator.zig`, `chaos.zig` | checked-in fixtures derived from HA sync/apply, timeline-switch, and rejoin scenarios; `make tla-trace-ha TRACE_FILES="specs/tla/traces/ha_*.ndjson"` | trace consumption, progress order/effect counts, failed apply does not advance, sync ack matches frozen target, timeline switch rejects old timeline, exact rejoin data-loss flag, rewind execution requires fresh assessment/fork retention/forced-policy, reseed publishes base-backup requirement | fixtures are checked in rather than emitted by live Zig HA tests; event payloads abstract identity booleans and selected progress fields; no segmentation/filtering; admin wire parsing and record bytes remain outside the trace model | add live HA trace writer or test-generated fixture hook if HA trace coverage should become implementation-emitted |
| Derived replay | `AntflyDerivedReplay.tla`, `.cfgs`, `AntflyDerivedReplay-heavy-depth`, `AntflyDerivedReplay-heavy-multi-index`, `AntflyDerivedReplay-heavy`, `AntflyDerivedReplayBad` | Bounded | `pkg/antfly/src/storage/db/derived/replay_source.zig`, `derived_worker.zig`, primary-store replay rows/metadata in `docstore.zig`, dense bulk replay paths in `db.zig` | thin replay, replay catch-up, reopen, truncation, replay-source primary-store tests | replay-all rows, hint lane rows, and latest hint metadata publication ordering; latest metadata must correspond to hint-lane rows; applied watermark cannot skip visible matching hinted replay; unavailable hint lanes cannot be treated as applied; query target stays within applied/target; truncation keeps needed replay; bulk sessions block target/query advancement | model still abstracts payload decoding, chunk sizing, coverage-rebuild guards, and generated enrichment worker interaction; all-lane rows alone are modeled as insufficient for targeted primary-store replay because current Zig tests require the hint lane; the full MaxSeq=3/two-index config is manual because of its fingerprint-collision estimate | add trace/test mapping for the direct replay-source tests and decide whether all-lane-only targeted rows are impossible by construction or a product bug to repair |
| Generated enrichment runtime | `AntflyEnrichmentLease.tla`, `.cfgs`, `AntflyEnrichmentLeaseBadStalePublish`, `AntflyEnrichmentLeaseBadEmptyPending` | Bounded | `pkg/antfly/src/storage/db/enrichment/enrichment_runtime.zig`, `enrichment_worker.zig`, generated artifact publication/replay paths in `db.zig` | generated dense/sparse/chunk tests, retry/restore tests, missing-model retryability test, worker thin-change-journal collection test | target/applied sequence ordering, replay visibility before generated publication, lease-owned collection/generation/publication, stale owner cannot publish, applied watermark cannot skip hidden pending generated work, retrying does not advance applied, isolated request failures do not poison the whole worker | abstracts request payload bytes, exact provider/rate-limit error taxonomy, backoff timing, per-window byte sizing, multiple artifact payloads per request, split-range fencing, and direct refinement to derived replay model state | map more generated enrichment DB tests to individual actions and decide whether to add a separate split-fenced enrichment model |
| Document identity | `AntflyDocumentIdentity.tla`, `.cfgs`, `AntflyDocumentIdentityBadReuseOrdinal`, `AntflyDocumentIdentityBadStaleFilter`, `AntflyDocumentIdentityBadNamespaceMismatch` | Bounded | `pkg/antfly/src/storage/db/doc_identity.zig`, DB identity namespace/generation/filter paths in `db.zig` | identity namespace/reassignment tests, generation projection tests, stale generation rejection, ordinal exhaustion, compaction preservation, restore namespace/metadata tests | stable ordinal ownership, live docs resolve through current visible ordinal state, tombstones hide current generation, resolved filters must match current namespace/generation, canonical rows match stored namespace, strict open rejects namespace mismatch | abstracts exact encoded key bytes, canonical hash collisions, primary document payload/index row coupling, range-specific split reassignment, full restore import stream ordering, and multi-shard namespace coordination | add trace/refinement hooks for resolved filter wire context and restore import repair |
| Document identity range/restore repair | `AntflyDocumentIdentityRangeRepair.tla`, `.cfgs`, split/merge plus restore identity, replica completion, readiness, and split-gate Bad* configs | Checked | metadata split/merge validators and reconciler; DB deferred restore/import/runtime repair; metadata runtime readiness overlay | focused split/merge tests, restore identity/progress/readiness tests, and checked trace fixtures | namespace guards; import-before-repair; progress must match the committed artifact; every placement replica completes before intent clear; restore-pending groups are neither ready nor splittable | hashes are identity atoms; artifact bytes/checksum computation, exact row encoding, and request concurrency remain concrete-test concerns | add live trace fields for artifact identity only if the event vocabulary becomes stable |
| Document identity range repair trace fixtures | `TraceAntflyDocumentIdentityRangeRepair.tla`, `TraceAntflyDocumentIdentityRangeRepair.cfgs`, `specs/tla/traces/doc_identity_restore_*.ndjson`, `specs/tla/traces/negative/doc_identity_restore_*.ndjson` | Trace-linked | same document identity range/restore repair anchors plus DB restore/import ordering anchors in `storage/db/db.zig` | checked-in positive fixtures for namespace rejection and repair ordering; expected-failure fixtures for accepting mismatched strict restore and early intent clear | every event is consumed by a valid model action, strict deferred restore cannot accept a mismatched namespace, runtime repair waits for recovered primary import, restore intent clears only after repair completion | checked fixtures are hand-authored rather than emitted by live Zig tests; exact restore artifact bytes, persisted status row schemas, import stream payloads, and multi-placement quorum progress remain abstract | add live/test-generated fixture emission only if model-fixture evidence is not enough for future restore repair changes |
| Managed host lifecycle | `AntflyManagedHostLifecycle.tla`, `.cfgs`, lifecycle plus mismatched-artifact and catalog-binding Bad* configs | Checked | managed raft host/catalog/bootstrap code and reconciler restore projection | managed-host restore, restart, removal, and catalog tests | activation requires verified exact artifact identity; restored catalog retains that binding; routes/active/catalog remain desired and durable across failure, cancellation, and restart | abstracts artifact bytes, peer lists, WAL/proposal persistence, leader election, and bootstrap error taxonomy | add host simulation traces only if catalog/route ordering changes |
| Placement membership repair | `AntflyPlacementRepair.tla`, `.cfgs`, repair, evidence, contraction, and liveness Bad* configs | Checked | placement planner repair and reconciler membership-transition index | focused duplicate recovery, stable retry, final-membership latch, per-store proof, and source-retirement tests | quota-full duplicate recovery retains the advanced member; replacement identity is stable; final membership is latched; only exact live-leader evidence from the authoritative store permits retirement; stale followers cannot block contraction | one group/replacement; abstracts scoring, bytes, and Raft joint-consensus internals | retain fast safety projections and heavy contraction liveness |
| Placement readiness aggregation | `AntflyPlacementReadiness.tla`, `.cfgs`, B1 unknown-report ambiguity and leader-only admission Bad* configs | Bounded | metadata state/reconciler group-status aggregation, stable-placement predicate, hosted shard transition admission | B1 diagnostic/unit reproducer remains outside this branch; existing readiness tests currently hard-code `voter_count_known = true` | unknown voter-set reports may seed but cannot create authoritative conflict; transition admission requires exact known healthy non-joint placement; converged evidence recovers readiness | the seed-versus-conflict rule is a proposed design contract pending owner decision; two stores/counts; no live emitted trace | add the B1 reproducer to both aggregation sites, settle merge semantics, and emit the failing conjunct in diagnostics |
| Runtime status and schema reconciliation | `AntflyRuntimeStatusReconciliation.tla`, `.cfgs`, precedence, disk provenance, join, migration, and read-availability Bad* configs | Bounded | runtime-status preference/overlay, data runtime root generation, metadata schema finalization, HA read gate | runtime-status authority, complete fresh disk/join, schema E2E, and standby read tests | old/removed/wrong-root observations cannot win; known disk facts match their root; join input is complete/fresh; finalization requires reported current coverage; read schema stays available | its heavy liveness previously treated schema-build progress as directly fair; B5 shows that worker admission/wakeup is a separate obligation | compose reported schema progress with scheduler-aware `AntflyIndexLifecycle` once B5's concrete stalled state is instrumented |
| Index rebuild lifecycle | `AntflyIndexLifecycle.tla`, `.cfgs`, incomplete swap, stale recovery, and lost-second-wakeup Bad* configs | Bounded | DB catalog/index manager, durable index status snapshots, schema-migration rebuild scheduling | focused index lifecycle tests; B5 mixed-load reproducer is external smokeout evidence | fresh implies caught up; requested rebuild debt remains durably schedulable; bounded competing work drains; two successive schema generations converge under explicit admission fairness | B5 root cause is not yet known; competing work is an abstract bound rather than the actual scheduler/queue; one index | instrument the stalled v1 rebuild, replace the provisional lost-wakeup mutant with the observed mechanism if different, and add a mixed-load correspondence test |
| Lite publication | `AntflyLitePublication.tla`, `.cfgs`, `AntflyLitePublicationBadManifestBeforeArtifacts`, `AntflyLitePublicationBadFailedHead`, `AntflyLitePublicationBadPinnedCleanup`, `AntflyLitePublicationBadMixedGeneration` | Bounded | Lite/serverless publication, manifest, segment, query-runtime pinning, restore-staging, native checkpoint/snapshot, and cleanup code | serverless publication/runtime/object-store/catalog tests; Lite native and CLI restore/snapshot tests | artifact families publish before manifest references, manifest head advances only from complete published refs, crash after manifest/no-head can be retried, visible generation references one generation, reader pins one generation while head advances, failed publication cannot advance head, cleanup cannot delete reader-pinned generation | abstracts exact object-store CAS/write bytes, native page/checkpoint layout, multiple simultaneous readers, manifest schema fields, per-index payload contents, retention horizons beyond two generations, and real concurrent publisher arbitration | add trace/test mapping for serverless publication actions, expand to multi-reader retention, and split native checkpoint/free-page detail into a sibling model if it grows too large |
| ML graph passes/runtime | `AntflyMlGraphPasses.tla`, `.cfgs`, `AntflyMlGraphPassesBadDanglingCse`, `AntflyMlGraphPassesBadParameterDedup`, `AntflyMlGraphPassesBadMissingLowerClosure`, `AntflyMlGraphPassesBadFallbackRuntime`, `AntflyMlGraphPassesBadPartialPublish` | Bounded | `lib/ml/src/graph/passes/{pipeline,cse,dce,fuse}.zig`, `lib/ml/src/graph/lower.zig`, `pkg/inference/src/graph/{partition_export,runtime,pjrt_compiler}.zig` | focused `lib/ml` graph root filters, lower test, inference partition-export/runtime filters | pass output ordering, no dangling current/exported graph references, output preservation, CSE cannot collapse parameter/constant identity, fused export preserves primitive lower closure when vjp alternate is present, external cross-partition runtime input is materialized, failed pass output is not visible, runtime publish fails closed on fallback partitions | bounded representative graph, abstracts concrete op/hash/shape equality, fixed-point iteration counts, exact fused pattern catalog, PJRT/native compiler artifact bytes, ONNX import lowering, scheduling/concurrency, and runtime executor lifetimes; current full standalone `lib/ml` test step has an unrelated existing SDPA fuse failure so focused tests are the counted alignment evidence | add a sibling model for pattern-specific fuse/shape rewrites if needed, and keep PJRT compiler output-selection/externalization coverage in the compiler-specific model |
| ML graph DAG CSE/DCE | `AntflyMlGraphDagPasses.tla`, `.cfgs`, `AntflyMlGraphDagPassesBadCseMissDuplicate`, `AntflyMlGraphDagPassesBadCseNoConsumerRemap`, `AntflyMlGraphDagPassesBadDceDropReachable`, `AntflyMlGraphDagPassesBadDceNonTopoMap` | Bounded | `lib/ml/src/graph/passes/{cse,dce,pipeline}.zig` | focused `lib/ml` graph root filters for unary/binary CSE, DCE `id_map`, DCE vjp/fused cases, and cleanup/fixed-point pipeline behavior | duplicate op producers redirect to earlier equal expressions, data nodes are never deduplicated, consumers/outputs/parameters remap through CSE redirects, DCE keeps exactly nodes reachable from remapped outputs, DCE `id_map` is compact and topological, final compacted references are valid | bounded five-node DAG shapes, abstracts concrete op attribute/hash/shape equality and hash collisions, allocator failures, exact vjp-alternate reachability in the arbitrary-DAG model, fuse rewrites, export/runtime publication, and fixed-point pass iteration counts; vjp/fuse/export/runtime are covered by separate models/tests | add op-attribute/hash-collision variants or live graph-pass trace fixtures only if future graph-pass changes need more correspondence |
| ML compiler publication | `AntflyMlCompilerPublication.tla`, `.cfgs`, `AntflyMlCompilerPublicationBadStaleCompile`, `AntflyMlCompilerPublicationBadMissingInput`, `AntflyMlCompilerPublicationBadOutputSelection`, `AntflyMlCompilerPublicationBadFallbackPublish`, `AntflyMlCompilerPublicationBadPartialArtifact` | Bounded | `pkg/inference/src/graph/partition_export.zig`, `pkg/inference/src/graph/pjrt_compiler.zig`, `pkg/inference/src/graph/runtime.zig` | PJRT compiler tests with `-Dpjrt=true`, partition export tests, graph runtime gate/native executor tests | partition export materializes parameters and semantic KV cache inputs, export includes compiler outputs, compiler artifacts are complete exports, runtime publication uses a fresh graph/export version, failed compiler artifacts are not visible, runtime output selection exposes only selected final outputs, fallback partitions fail closed before executor publication | abstracts exact HLO/native artifact bytes, dynamic shape/layout details, full partition planner search, runtime executor lifetime, real cache invalidation keys, compile concurrency, and backend-specific capability matrices | add native/PJRT artifact hash/refinement checks if compiler outputs become traceable; add pattern-specific compiler lowering models for GQA/RoPE only if bugs justify the state space |

## High-Risk Areas With Incomplete Evidence

| Area | Current coverage | Risk | Next action |
|---|---|---|---|
| Replay hint lane visibility | Partial derived replay model plus direct replay-source/DB tests | stale hint metadata or empty hint lane can skip real work; current tests also encode hint-lane-required primary-store behavior | map direct replay-source tests to individual TLA actions and add trace fixtures only if the worker event vocabulary stabilizes |
| HA failover acknowledged-write preservation and fencing | Focused failover, partition-fence, gate-transition, sync-wait, timeline-switch, standby-apply, and rejoin models with expected-failure mutants | a promoted standby can lose a write that another standby acknowledged, an old primary can accept writes after promotion, or a stale gate decision can outlive demotion/fencing | add trace or generated fixtures from HA promotion/fencing tests after the model contract stabilizes |
| Product write/query control plane | Batcher coalescing, CDC cutover, split bridge, query completeness, and transaction/session models | stale or partial visibility around write flush, snapshot/stream handoff, split routing, or generated API publication can hide or duplicate user-visible state | keep TLA+ to ordering/visibility/publication; validate top-K, pagination math, analyzers, predicates, and payload bytes with property/golden/fuzz tests |
| Enrichment worker target advancement | Partial enrichment worker model plus focused Zig tests | applied watermark can advance past hidden generated work | map remaining split-fencing and asset-producer tests to model actions |
| Restore runtime repair | Partial document-identity range/restore repair model, metadata restore-intent tests, and checked restore trace fixtures for strict mismatch and import-before-repair ordering; DB restore tests exist but still lack a clean focused build hook | mixed restored generated artifacts and replay debt, especially when primary import recovery and runtime repair ordering diverge | add implementation-emitted or test-generated restore traces only if checked fixtures need to become live evidence |
| Vector/search/backup data plane | TLA+ covers backup publication/identity/readiness control-plane boundaries; bytes remain routed | byte corruption, HBC/kmeans recall, BM25/analyzer math, predicate/hash computation bugs are poorly served by state abstraction | retain checksum/restore, simulation, differential, golden corpus, and property tests; use TLA+ only for ordering, binding, authority, visibility, and publication |
| OpenAPI compatibility | ROUTED, no model: `zig build openapi-root-check` and `generated-check` verify the real generated artifacts directly, which is stronger evidence than a hand-modeled abstraction (model removed in review) | stale generated public/internal API mismatch | keep the build checks wired to every generated package |
| ML graph runtime pass ordering | Partial graph pass/export/runtime gate model, bounded DAG CSE/DCE model, compiler publication model, and focused tests | invalid pass output can be published and consumed, especially around fused lower closure, DCE remapping, and fallback runtime gates | add pattern-specific fuse/shape rewrites or graph-pass trace fixtures if bugs justify the extra state space |

The control-plane surfaces found by the July 1 final critique are now all
either modeled or explicitly routed away from TLA+: node drain
(`AntflyNodeDrainLifecycle`), table create/drop (`AntflyTableLifecycle`),
WAL retention/reseed + backup slots (`AntflyHARetentionReseed`), entity
promotion single-owner across split/merge (`AntflyPromotionOwnerHandoff`),
and index lifecycle (`AntflyIndexLifecycle`). Distributed join leases are
routed to the deterministic sim harness (advisory lease, continuous-time
expiry, data-shaped dedup); per-subsystem owner-job gate composition is
routed to Zig race tests (structural class already covered by
`AntflyHAGateTransitions`); merge prepare->cutover rollback is not modeled
because the implementation has no merge-abort path.

Anchor note for `AntflyCdcCutover`: the snapshot -> stream cutover lifecycle
(phase, cursors, checkpoint) is managed by the metadata service replication
runtime (`metadata/service.zig` replication source status/phase handling),
not by `foreign/postgres_source.zig`, which only provides the snapshot query
and replication stream primitives. Correspondence work should anchor there.

Liveness note: `AntflyHAFailoverSafety`, `AntflyHARejoin`,
`AntflyDerivedReplay`, and `AntflyEnrichmentLease` now check no-permanent-stall
temporal properties through a `FairSpec` used only by their positive configs
(mutant configs keep the unfair `Spec`). The properties are conditional where
the model legitimately allows a stall (unavailable hint lane, lease churn,
stale rejoin assessment); see the module comments. Stall-injecting liveness
mutants are future work.

## Evidence Shapes

These groupings are navigation aids, not a maturity ladder. In particular,
an implementation test named in this file is evidence only if the build target
actually discovers and executes it. The July-25 B4 finding—29 Raft chaos
filters resolving to zero tests—demonstrated why target names alone do not
establish correspondence.

| Evidence | Models |
|---|---|
| Checked | `AntflyBatcherCoalescing` (red->green correspondence test), `AntflyDerivedReplay` (executed co-write tests), `AntflyEnrichmentLease`, `AntflyLmdbCommit`, `AntflyLsmWalCompaction`, `AntflyTransaction`, `AntflyTransactionSession` (+trace fixtures), `AntflySnapshotTransfer`, `AntflyRaftSchedulerFairness`, `AntflyRaftReadyPipeline`, `AntflyPlacementRepair`, `AntflyDocumentIdentity`, `AntflyDocumentIdentityRangeRepair` (+trace fixtures), `AntflyHASyncWait`, `AntflyHATimelineSwitch`, `AntflyHAStandbyApply`, `AntflyHARejoin`, `AntflyHAReplication`, `AntflyLitePublication`, `AntflyMlGraphPasses`, `AntflyMlGraphDagPasses`, `AntflyMlCompilerPublication`, `AntflyManagedHostLifecycle` |
| Bounded | `AntflyPlacementReadiness` (candidate seed-versus-conflict contract), `AntflyShardSplit` and `AntflySplitRefinementBridge` (+trace fixtures; stable-placement bridge newly added), `AntflyRuntimeStatusReconciliation` and `AntflyIndexLifecycle` (scheduler-aware two-generation rebuild contract), `AntflyHAFailoverSafety`, `AntflyHAPartitionFence`, `AntflyHAGateTransitions`, `AntflyHAGates`, `AntflyCdcCutover` (anchor correction pending), `AntflyQueryCompleteness`, `AntflyShardSplitSeq`, `AntflySnapshotContent`, `AntflyLsmReserveCleanup`, `AntflyDbSplitVisibility`, `AntflyLsmLifecycle`, `AntflyNodeDrainLifecycle`, `AntflyTableLifecycle`, `AntflyHARetentionReseed` |
| Contract | `AntflyPromotionOwnerHandoff` — a focused authority/ownership guardrail (detach-before-transfer-before-attach), NOT deep implementation correspondence: its state space is tiny and it intentionally collapses raft leadership, group identity, and runtime hook detail into range ownership + an attachment bit. |
| Routed | B2 model-catalog latency/readiness budget — performance and CI-marker tests. B3 OpenAPI compatibility — generated-schema and compatibility diff checks. B4 test discovery — build/test audit requiring nonzero matched tests. Distributed join leases — fault-injection shuffle-join scenarios in `metadata/sim_harness.zig`. Owner-job gate composition — Zig race tests around `data/runtime.zig` `haOwnerJobCanRun`. Merge prepare->cutover rollback — not modeled; the implementation has no abort path. |

Negative-config pinning status (checked by `bash ../scripts/tla-check.sh audit`): critic-response,
new-coverage, and the six migrated priority groups (HA sync-wait /
timeline-switch / standby-apply / rejoin, document identity range repair,
managed host lifecycle, ML graph/DAG/compiler publication) pin named semantic
invariants; 21 older configs (DbSplitVisibility, DocumentIdentity, HAGates,
LitePublication, LmdbCommit, LsmWalCompaction, LsmLifecycle,
SplitRefinementBridge) still include the broad `Safety` conjunction and are
tracked migration debt. Do NOT describe the suite as fully pinned until
`bash ../scripts/tla-check.sh audit` reports zero Safety-pinned configs.

Bounds backlog: a few of the newest models hardcode their bounds inside the
spec (`AntflyBatcherCoalescing` 2 ops, `AntflyShardSplitSeq` 2 writes/no
deletes, `AntflyHARetentionReseed` MaxLsn 3, `AntflyIndexLifecycle` MaxSeq 2).
Lifting these to config constants with heavy-tier variants (3 ops / 3 writes
plus a delete / MaxLsn 4 / MaxSeq 3) is cheap belt-and-suspenders with low
expected yield (protocol bugs overwhelmingly manifest at small scopes, and
the heavy tier already pushes the riskiest models to tens of millions of
states). Not worth blocking on; pure state machines (gate transitions,
promotion handoff) gain nothing from larger bounds.

## Liveness Mutant Backlog

The temporal properties below are checked but have no stall-injecting mutant
demonstrating each would fail on a real progress regression. Recorded per
model: what a regression looks like and the mutant that would expose it.

| Model / property | Stall regression | Exposing mutant (future) |
|---|---|---|
| `AntflyHAFailoverSafety` `EventuallyPromoted` | promotion guard tightened so no standby ever qualifies | `BuggyPromotionRequiresBothStandbys`: FenceAndPromote requires the acked set on BOTH standbys |
| `AntflyHARejoin` `RejoinEventuallyExecutes` | executable rewind never executed (executor gate inverted) | `BuggyExecutorSkipsRewind`: ExecuteRewind guard requires `~CanExecuteRewind` |
| `AntflyDerivedReplay` `CatchupEventuallyCompletes` | catch-up loop never re-observes an advanced target | `BuggyTargetObservationDisabled`: ObserveReplayTarget disabled after first observation |
| `AntflyEnrichmentLease` `EnrichmentEventuallyDrains` | retry loop never exits (RetryLater disabled) | `BuggyPermanentRetry`: RetryLater requires FALSE |
| `AntflyNodeDrainLifecycle` `DrainEventuallyReportsSafe` | status endpoint never recomputes after debt clears | `BuggyStatusComputedOnce`: ComputeStatus disabled after first call |
| `AntflyTableLifecycle` `TopologyEventuallyConverges` | reconciler never removes undesired intents | `BuggyRemoveIntentDisabled` |
| `AntflyHARetentionReseed` `NoPermanentUnmarkedLag` | marking loop skips one slot forever | `BuggyMarkSkipsSlot(s)` |
| `AntflyPromotionOwnerHandoff` `NewSideEventuallyOwns` | attach gate requires an event that never fires | `BuggyAttachRequiresOldCrash` |
| `AntflyIndexLifecycle` `BuildEventuallyConverges` | build loop wedges below target | `BuggyBuildStopsEarly`: BuildStep requires `applied < target - 1` |

## Required Negative Validation Backlog

| Model | Negative scenario | Expected failing invariant |
|---|---|---|
| `AntflyTransaction` | OCC predicate check ignores a pending intent on a conflicting key | `OCCSerializationInvariant` |
| `AntflyShardSplit` | child transitions to default/serving from replay-caught-up before split cutover is ready | `NoPrematureCutover` |
| `AntflySnapshotTransfer` | transfer is marked done/applied without the target node having the snapshot in local archive | `AppliedSnapshotIsValid` |
| `AntflyLsmLifecycle` | `IndexWriter.removeSegments` second allocation failure leaks the temporary allocation | `Safety` via `IndexNoTempLeak` / `IndexFailedOpFreedTemps` |
| `AntflyDerivedReplay` | per-hint latest metadata advances without a corresponding hint-lane row | `LatestHintMetadataHasHintRows` / `NoAppliedSkipsTargetedReplay` / `Safety` |
| `AntflyEnrichmentLease` | worker publishes generated artifact after losing lease epoch | `Safety` via published artifact validity |
| `AntflyEnrichmentLease` | empty visible-pending scan advances through hidden generated work | `Safety` via applied-does-not-skip-generated-work |
| `AntflyHAReplication` | stale timeline ack satisfies current timeline write | `SyncAckMatchesTargetTimeline` |
| `AntflyHAFailoverSafety` | failover promotes a standby that lacks a sync-acknowledged write (async acks carry no preservation promise) | `PromotedNodeHasAllSyncAckedWrites` |
| `AntflyHAFailoverSafety` | old primary accepts a write after standby promotion | `NoSplitBrainWrites` |
| `AntflyHAFailoverSafety` | sync ack recorded without durable standby receipt evidence | `AckEvidenceExists` |
| `AntflyHAGates` | standby DB starts mutating background runtimes | `MutatingBackgroundRequiresWritablePrimary` / `StandbyDoesNotRunMutatingBackground` |
| `AntflyHAGateTransitions` | cached/stale allow decision survives demotion or fencing | `AllowedWriteRequiresUnfencedPrimary` / `MutatingRuntimeRequiresUnfencedPrimary` / `Safety` |
| `AntflyHAPartitionFence` | standby promotion becomes writable before the old primary receives the fence | `PromotionRequiresDeliveredFence` / `NoSplitBrainWritable` |
| `AntflyHAPartitionFence` | still-unfenced old primary appends via the normal write path after promotion | `NoOldPrimaryWritesAfterPromotion` |
| `AntflyHASyncWait` | promotion mutates an already frozen sync-wait target | `FrozenTargetDoesNotMove` / `Safety` |
| `AntflyHASyncWait` | standby ack from a wrong timeline satisfies a frozen target | `AckMatchesFrozenTimeline` / `AckComesFromMatchingSlot` / `Safety` |
| `AntflyHASyncWait` | standby ack below the frozen target LSN satisfies a wait | `AckSatisfiesFrozenLsn` / `AckSlotAppliedEnough` / `Safety` |
| `AntflyHATimelineSwitch` | timeline switch accepted before parent received/applied/safe progress catches up | `SwitchRequiresAppliedProgress` / `Safety` |
| `AntflyHATimelineSwitch` | timeline switch reuses current timeline or epoch | `SwitchRecordIsMonotonic` / `Safety` |
| `AntflyHATimelineSwitch` | old-timeline record is accepted after switching to the new timeline | `OldTimelineRejectedAfterSwitch` / `Safety` |
| `AntflyHATimelineSwitch` | crash recovery applies durable switch whose `previous_lsn` does not match recovered parent progress | `RecoveryUsesRecoveredProgress` / `Safety` |
| `AntflyHAStandbyApply` | failed replicated apply advances applied/safe-read/DB marker progress | `FailedApplyDoesNotAdvance` / `Safety` |
| `AntflyHAStandbyApply` | duplicate replay of an already-applied record repeats its side effects | `ReplicatedApplyIsIdempotent` / `Safety` |
| `AntflyHAStandbyApply` | crash/reopen loses a durably received unapplied record | `DurableReceiveSurvivesCrash` / `Safety` |
| `AntflyHAStandbyApply` | standby accepts a client write | `ClientWritesRejectedOnStandby` / `Safety` |
| `AntflyHAStandbyApply` | standby starts a mutating background runtime | `MutatingRuntimeSuppressedOnStandby` / `Safety` |
| `AntflyHARejoin` | former primary rewinds without a promotion fence | `AssessmentMatchesPolicy` / `RejectUnfencedDoesNotExecute` / `Safety` |
| `AntflyHARejoin` | former primary rewinds even though retained WAL no longer covers the fork | `AssessmentMatchesPolicy` / `RewindRequiresCompatibleFence` / `Safety` |
| `AntflyHARejoin` | forced-promotion rewind proceeds without explicit allow policy | `AssessmentMatchesPolicy` / `RewindRequiresCompatibleFence` / `Safety` |
| `AntflyHARejoin` | identity, old-primary, or parent-timeline mismatch still rewinds | `AssessmentMatchesPolicy` / `RewindRequiresCompatibleFence` / `Safety` |
| `AntflyHARejoin` | late write after assessment is truncated by stale rewind execution | `RewindExecutionRequiresFreshAssessment` / `Safety` |
| `AntflyHARejoin` | missing or mismatched fork record is truncated as if retained | `RewindExecutionValidatesForkRecord` / `Safety` |
| `AntflyBatcherCoalescing` | delete followed by write is flushed in delete-visible order | `LastOperationWinsPerKey` |
| `AntflyBatcherCoalescing` | write followed by delete is flushed in write-visible order | `LastOperationWinsPerKey` |
| `AntflyBatcherCoalescing` | a multi-row batch becomes partially visible | `NoPartialVisibility` |
| `AntflyBatcherCoalescing` | deleted value becomes durable without a trailing delete operation | `DeleteVisibleBeforeWriteOnly` |
| `AntflyCdcCutover` | stream starts at the snapshot high-water row and duplicates snapshot delivery | `NoDuplicateDelivery` |
| `AntflyCdcCutover` | checkpoint advances past rows actually delivered to the consumer | `CheckpointOnlyCoversDelivered` |
| `AntflyCdcCutover` | resume reconstructs snapshot/stream cursor behind durable checkpoint | `ResumeCursorStartsAtCheckpoint` |
| `AntflyCdcCutover` | crash after a premature checkpoint resumes into stream phase with undelivered snapshot rows | `StreamStartsAfterSnapshotHighWater` |
| `AntflyShardSplitSeq` | split cutover proves only key membership and loses a second write to the same key | `SecondWriteCannotBeCollapsedByKey` |
| `AntflyShardSplitSeq` | split cutover completes while a fenced parent write is not replayed | `CutoverPreservesAllFencedWrites` |
| `AntflySnapshotContent` | target applies content for a different raft snapshot index | `AppliedContentMatchesIndex` |
| `AntflySnapshotContent` | follower GC removes fetched content still needed for apply | `TargetContentNotGcBeforeApply` |
| `AntflyLsmReserveCleanup` | publication proceeds without a cleanup reserve | `PublishedResourceHasCleanupReserve` / `Safety` |
| `AntflyLsmReserveCleanup` | failed operation leaks temporary ownership | `NoTempLeakAfterFailure` / `Safety` |
| `AntflyNodeDrainLifecycle` | finalize accepted while the node is still active | `FinalizeRequiresDrained` |
| `AntflyNodeDrainLifecycle` | node re-registration clears lifecycle while stores keep drain flags | `DrainStateConsistent` |
| `AntflyNodeDrainLifecycle` | safe_to_terminate reported while placement/hosted debt remains | `SafeReportMatchesDebt` |
| `AntflyTableLifecycle` | desired range admitted without its table (UnknownTable guard removed) | `DesiredRangesHaveTable` |
| `AntflyTableLifecycle` | placement intent planned for a range the desired topology dropped | `NoIntentPlannedForUndesiredRange` |
| `AntflyHARetentionReseed` | WAL truncation floor skips an active slot the mark-reseed loop has not marked yet | `TruncationCoversUnmarkedActiveSlots` |
| `AntflyHARetentionReseed` | backup end reports success after its slot was lost or its WAL truncated | `BackupEndFailsClosed` |
| `AntflyPromotionOwnerHandoff` | child/receiver promotion owner attached while parent/donor still attached | `AtMostOneAttachedOwner` |
| `AntflyPromotionOwnerHandoff` | promotion owner attached after handoff start but before range transfer | `AttachedImpliesRangeOwner` |
| `AntflyPromotionOwnerHandoff` | promotion catch-up advances without the isLocalOwner predicate | `NoUnownedPromotion` |
| `AntflyIndexLifecycle` | shadow index swaps in while its applied watermark is behind | `FreshImpliesCaughtUp` |
| `AntflyIndexLifecycle` | crash recovery trusts a stale durable "fresh" snapshot and serves queries | `NoFreshServeBehind` |
| `AntflyQueryCompleteness` | metadata routes queries to a child before the child has a complete serving view | `RouteRequiresChildServing` |
| `AntflyQueryCompleteness` | child publishes serving without the moved doc | `NoMissingDocs` |
| `AntflyQueryCompleteness` | parent and child both serve the moved range to one query | `NoDuplicateDocs` |
| `AntflyLsmWalCompaction` | checkpoint includes unsynced WAL entry | `Safety` via checkpoint-only-covers-durable-WAL |
| `AntflyLsmWalCompaction` | corrupt current-tail segment is rotated into retained history | `Safety` via corrupt-tail-only-on-current-segment |
| `AntflyLsmWalCompaction` | reader-pinned WAL segment is retired | `Safety` via reader-pins-retained / retired-segments-safe |
| `AntflyLmdbCommit` | meta page written before dirty data pages are durable | `Safety` via meta-write-after-data-sync |
| `AntflyLmdbCommit` | free page reused while reader snapshot can still see it | `Safety` via reader snapshot page visibility |
| `AntflyTransactionSession` | rollback-to-savepoint publishes discarded staged write | `NoVisibleUncommittedWrites` / `RollbackDiscardedNotVisible` |
| `AntflyTransactionSession` | aborted finalized orphan intents recover as committed data | `NoVisibleUncommittedWrites` / `ParticipantRecoveryDoesNotPublishAbortedData` |
| `AntflyTransactionSession` | coordinator cleanup runs while prepared participant is unresolved | `CleanupRequiresAllResolved` |
| `TraceAntflyTransactionSession` | fixture attempts cleanup while one prepared participant remains unresolved | `TraceMatched` / `TraceSafety` |
| `AntflyDocumentIdentity` | tombstoned ordinal reused for a different logical document | `AllocatedOrdinalsHaveStableOwner` |
| `AntflyDocumentIdentity` | current search accepts a resolved-doc-filter built for a stale identity generation | `ResolvedFilterMatchesCurrentContext` |
| `AntflyDocumentIdentity` | strict open accepts a mismatched configured/stored namespace | `StrictOpenRejectsNamespaceMismatch` |
| `AntflyDocumentIdentityRangeRepair` | split validation accepts unhealthy or active-reassignment source identity status | `SplitRequiresHealthySource` |
| `AntflyDocumentIdentityRangeRepair` | split destination reports a namespace different from the requested destination range namespace | `SplitDestinationStatusMatchesExpectedNamespace` |
| `AntflyDocumentIdentityRangeRepair` | merge accepts incompatible healthy donor/receiver namespaces without explicit reassignment opt-in | `MergeRequiresCompatibleIdentityStatus` |
| `AntflyDocumentIdentityRangeRepair` | merge receiver namespace is reassigned without opt-in or while either side is unhealthy | `MergeReassignmentRequiresOptInAndHealthyStatus` |
| `AntflyDocumentIdentityRangeRepair` | strict deferred restore accepts mismatched source/target doc identity namespace | `StrictRestoreRejectsNamespaceMismatch` |
| `AntflyDocumentIdentityRangeRepair` | restore intent clears before primary import recovery and runtime repair complete | `RuntimeRepairRequiresRecoveredPrimaryImport` / `RestoreIntentClearsOnlyAfterRepairComplete` |
| `TraceAntflyDocumentIdentityRangeRepair` | fixture accepts mismatched strict deferred restore namespace | `TraceMatched` / `TraceSafety` |
| `TraceAntflyDocumentIdentityRangeRepair` | fixture clears restore intent before runtime repair completes | `TraceMatched` / `TraceSafety` |
| `AntflySplitRefinementBridge` | metadata routes the right range to the child before DB child serving is ready | `MetadataChildRouteRequiresBothLayersReady` |
| `AntflySplitRefinementBridge` | DB child starts serving before shard split cutover is complete | `DbServingRequiresShardCutoverAndIndexes` |
| `AntflySplitRefinementBridge` | shard cutover completes from a stale fence sequence | `ShardCutoverRequiresCurrentReplay` |
| `TraceAntflySplitRefinementBridge` | fixture routes metadata to child before DB serving event is published | `TraceMatched` / `TraceSafety` |
| `AntflyLitePublication` | manifest is visible before referenced segment is published | `ManifestReferencesPublishedArtifacts` / `VisibleManifestReferencesPublishedArtifacts` |
| `AntflyLitePublication` | failed publication advances visible head | `FailedPublicationCannotAdvanceVisibleGeneration` |
| `AntflyLitePublication` | cleanup deletes data pinned by an open reader | `CleanupCannotDeleteReaderPinnedGeneration` |
| `AntflyLitePublication` | visible generation mixes document/index segment refs from different generations | `VisibleManifestReferencesPublishedArtifacts` / `ReaderGenerationIsPinnedAndConsistent` |
| `AntflyMlGraphPasses` | CSE removes a duplicate producer without remapping consumers | `CurrentGraphReferencesValid` |
| `AntflyMlGraphPasses` | CSE collapses parameter/constant identity while leaving a structurally valid graph | `ParameterAndConstantIdentityPreserved` |
| `AntflyMlGraphPasses` | partition export omits a fused node's primitive lower closure | `ExportedGraphReferencesValid` |
| `AntflyMlGraphPasses` | graph runtime publishes despite fallback partitions under a fail-closed gate | `RuntimeGateFailsClosed` |
| `AntflyMlGraphPasses` | a failed pass leaves partial output visible to later pipeline/export steps | `FailedPassOutputNotVisible` |
| `AntflyMlGraphDagPasses` | CSE misses an available duplicate op in a bounded DAG | `CseMapIsSemantic` |
| `AntflyMlGraphDagPasses` | CSE redirects a duplicate producer but leaves consumers on stale old IDs | `CseRemapsConsumersOutputsAndParameters` |
| `AntflyMlGraphDagPasses` | DCE drops a reachable node while compacting the graph | `DceKeepsExactlyReachableNodes` / `FinalGraphReferencesValid` |
| `AntflyMlGraphDagPasses` | DCE creates a compact `id_map` that violates source topological order | `DceMapIsCompactTopological` |
| `AntflyMlCompilerPublication` | compiler publishes an artifact from a stale graph/export version | `RuntimePublishesOnlyFreshCompleteArtifact` |
| `AntflyMlCompilerPublication` | partition export omits required parameter or semantic KV cache input | `ExportMaterializesRequiredRuntimeInputs` |
| `AntflyMlCompilerPublication` | runtime exposes semantic KV side outputs instead of only selected final output | `RuntimeOutputSelectionIsExact` |
| `AntflyMlCompilerPublication` | runtime publishes executor despite fallback partition and fail-closed gate | `RuntimeGateFailsClosed` |
| `AntflyMlCompilerPublication` | failed compiler leaves partial artifact visible | `FailedCompileArtifactNotVisible` |
| `AntflyManagedHostLifecycle` | restore bootstrap activates/routes replica before bootstrap success and durable store/catalog publication | `RestoreDoesNotActivateBeforeSuccess` / `DurableStoreForHostedReplica` |
| `AntflyManagedHostLifecycle` | metadata removal clears active state but leaves a stale route | `RoutesOnlyForActiveReplicas` / `NoUndesiredRoute` |
| `AntflyManagedHostLifecycle` | removed replica catalog entry is retained and can revive after restart | `CatalogOnlyForDesiredReplica` / `RestartRestoresOnlyCatalogedDesiredReplicas` |
| `AntflyManagedHostLifecycle` | metadata removal does not cancel pending restore bootstrap state | `RestoreBootstrapRequiresDesiredGroup` |

## Validation Commands

Fast model validation:

```bash
make tla-clean
make tla-check TIER=fast
```

Heavy model validation:

```bash
make tla-check TIER=heavy
```

Negative model validation:

```bash
bash ../scripts/tla-check.sh negative
```

Core existing model validation:

```bash
make tla-check TIER=core
```

Focused validation already run for the legacy/core expected-failure harness:

- `make tla-check TIER=core`: green after adding disabled bug flags to the core configs.
  The run covered `AntflyTransaction`, `AntflyShardSplit`,
  `AntflySnapshotTransfer-safety`, and `AntflyLsmLifecycle`; the snapshot safety
  check remains the large core run at 27,361,379 distinct states.
- `make tla-check CHECK=AntflyTransactionBadSkipIntentConflict`: pending-intent conflict
  mutant fails on `OCCSerializationInvariant`.
- `make tla-check CHECK=AntflyShardSplitBadPrematureChildDefault`: child-default-before
  cutover mutant fails on `NoPrematureCutover`.
- `make tla-check CHECK=AntflySnapshotTransferBadApplyWithoutPut`: transfer-done-without-local
  archive mutant fails on `AppliedSnapshotIsValid`.
- `make tla-check CHECK=AntflyLsmLifecycleBadIndexTempLeak`: index temporary allocation
  leak mutant fails through the lifecycle safety invariants.
- `bash ../scripts/tla-check.sh negative`: green after wiring the four legacy/core mutants
  into the aggregate expected-failure harness.
- `make tla-clean && rg --files specs/tla | rg '_TTrace_' || true`: no trace
  artifacts remain after the expected-failure runs.

Critic follow-up validation on 2026-07-01:

- `make tla-check TIER=fast`: green after repairing snapshot-content GC
  non-vacuity, CDC crash/resume, batcher bidirectional operation order, HA
  `former_primary` transitions, and query missing-doc coverage.
- `make tla-check TIER=fast`: green after wiring the critic-response models into
  the fast focused tier.
- `make tla-check CHECK=AntflyBatcherCoalescing`: green, 27 distinct states, depth 6.
- `make tla-check CHECK=AntflyCdcCutover`: green, 9 distinct states, depth 6.
- `make tla-check CHECK=AntflyQueryCompleteness`: green, 8 distinct states, depth 5.
- `make tla-check CHECK=AntflySnapshotContentBadGcNeededContent`: GC of needed
  fetched snapshot content fails on `TargetContentNotGcBeforeApply`.
- `make tla-check CHECK=AntflyBatcherCoalescingBadWriteDeleteInversion`:
  write/delete inversion fails on `LastOperationWinsPerKey`.
- `make tla-check CHECK=AntflyCdcCutoverBadResumeReplay`: bad resume cursor fails
  on `ResumeCursorStartsAtCheckpoint`.
- `make tla-check CHECK=AntflyQueryCompletenessBadRouteBeforeChildReady`: route
  before child serving fails on `RouteRequiresChildServing`.
- `make tla-check CHECK=AntflyQueryCompletenessBadDoubleServe`: parent/child
  double-serving fails on `NoDuplicateDocs`.
- `make tla-check CHECK=AntflyQueryCompletenessBadMissingDoc`: child serving without
  the moved doc fails on `NoMissingDocs`.
- `bash ../scripts/tla-check.sh negative`: green after repinning critic-response bad cfgs to
  named semantic invariants and adding the missing mutants.
- Zig correspondence tests were added for replay co-write decode-failure
  fallback, provisioned coalescer same-key order, and DB split same-key latest
  content. They could not be executed locally because no `zig` binary was
  available on PATH in this session. The provisioned coalescer delete->write
  test is expected to expose a product issue in the current write/delete merge
  order and should be run during the product-fix pass.
- `make tla-clean && rg --files specs/tla | rg '_TTrace_' || true`: no trace
  artifacts remain after the final negative run.
- TLA+ coverage remains intentionally limited to ordering, crash, visibility,
  authority, and publication contracts. Vector recall, ranking math, analyzer
  behavior, predicate/hash correctness, payload bytes, and backup byte integrity
  require simulation, property, golden, fuzz, differential, or checksum/restore
  validation outside TLA+.

Smoke parsing validation:

```bash
bash ../scripts/tla-check.sh smoke
```

Trace validation dispatch:

```bash
make tla-trace TRACE=raft TRACE_FILES=/tmp/raft-trace.ndjson
make tla-trace TRACE=txn TRACE_FILES=/tmp/txn-trace.ndjson
make tla-trace TRACE=txn-session TRACE_FILES="specs/tla/traces/txn_session_*.ndjson"
make tla-trace TRACE=ha TRACE_FILES="specs/tla/traces/ha_*.ndjson"
make tla-trace TRACE=doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson"
```

## Consolidated TLC Runtime Budget

Measured on 2026-06-30 with TLC 2026.05.26, 14 workers, 8192MB heap, and the
current Makefile configs. `00s` means TLC reported less than one second. These
rows cover the positive model-checking targets; trace validators are tracked in
their focused validation notes because live raft/transaction traces depend on
external `TRACE_FILES`.

| Tier | Make target | Model/check | Generated states | Distinct states | Depth | Wall time |
|---|---|---|---:|---:|---:|---:|
| Core | `tla-check-txn` | transaction spec | 49,276 | 11,140 | 18 | 01s |
| Core | `tla-check-split` | shard split spec | 824 | 166 | 16 | 00s |
| Core | `tla-check-snap` | snapshot transfer safety | 228,587,191 | 27,361,379 | 45 | 01min 03s |
| Core | `tla-check-lsm` | LSM lifecycle OOM safety | 3,626 | 625 | 13 | 00s |
| Fast | `tla-check CHECK=AntflyHAGates` | HA gate decision table | 64,800 | 64,800 | 1 | 00s |
| Fast | `tla-check CHECK=AntflyHASyncWait` | HA sync wait target provenance | 3,214 | 1,080 | 16 | 00s |
| Fast | `tla-check CHECK=AntflyHATimelineSwitch` | HA timeline switch boundary | 28 | 25 | 9 | 00s |
| Fast | `tla-check CHECK=AntflyHAStandbyApply` | HA standby apply/replay suppression | 69 | 31 | 6 | 00s |
| Fast | `tla-check CHECK=AntflyHARejoin` | HA former-primary rejoin/reseed | 42,613 | 35,651 | 6 | 00s |
| Fast | `tla-check CHECK=AntflyHAFailoverSafety` | HA acknowledged-write failover safety | 312 | 205 | 10 | 00s |
| Fast | `tla-check CHECK=AntflyHAPartitionFence` | HA partition fence delivery | 60 | 30 | 7 | 00s |
| Fast | `tla-check CHECK=AntflyHAGateTransitions` | HA gate transition stale-decision safety | 12 | 5 | 4 | 00s |
| Fast | `tla-check CHECK=AntflyBatcherCoalescing` | batcher per-key coalescing order | 11 | 9 | 6 | 00s |
| Fast | `tla-check CHECK=AntflyCdcCutover` | CDC snapshot/stream cutover | 13 | 6 | 5 | 00s |
| Fast | `tla-check CHECK=AntflyShardSplitSeq` | shard split sequence-level delta safety | 32 | 19 | 7 | 00s |
| Fast | `tla-check CHECK=AntflySnapshotContent` | snapshot content/index provenance | 57 | 28 | 10 | 00s |
| Fast | `tla-check CHECK=AntflyLsmReserveCleanup` | LSM reserve/failure cleanup | 29 | 12 | 7 | 00s |
| Fast | `tla-check CHECK=AntflyQueryCompleteness` | split query routing completeness | 11 | 8 | 5 | 00s |
| Fast | `tla-check CHECK=AntflyDerivedReplay` | derived replay hint lanes | 797 | 252 | 16 | 00s |
| Fast | `tla-check CHECK=AntflyEnrichmentLease` | enrichment lease publication | 106,576 | 36,302 | 22 | 00s |
| Fast | `tla-check CHECK=AntflyLmdbCommit` | LMDB commit crash/readers | 193,752 | 36,967 | 35 | 00s |
| Fast | `tla-check CHECK=AntflyLsmWalCompaction` | LSM WAL checkpoint compaction | 53,332 | 10,026 | 19 | 00s |
| Fast | `tla-check CHECK=AntflyDbSplitVisibility` | DB split visibility | 3,113 | 870 | 16 | 00s |
| Fast | `tla-check CHECK=AntflySplitRefinementBridge` | split refinement bridge | 758 | 319 | 16 | 00s |
| Fast | `tla-check CHECK=AntflyDocumentIdentity` | document identity uniqueness | 4,337 | 899 | 9 | 00s |
| Fast | `tla-check CHECK=AntflyDocumentIdentityRangeRepair` | document identity range repair | 742,048 | 95,040 | 9 | 00s |
| Fast | `tla-check CHECK=AntflyTransactionSession` | transaction session savepoints | 5,740 | 1,946 | 32 | 00s |
| Fast | `tla-check CHECK=AntflyManagedHostLifecycle` | managed host lifecycle | 1,153 | 288 | 15 | 00s |
| Fast | `tla-check CHECK=AntflyLitePublication` | Lite publication | 1,591 | 599 | 23 | 00s |
| Fast | `tla-check CHECK=AntflyMlGraphPasses` | ML graph pass ordering | 61 | 36 | 9 | 00s |
| Fast | `tla-check CHECK=AntflyMlGraphDagPasses` | ML graph DAG CSE/DCE remapping | 9 | 9 | 3 | 00s |
| Fast | `tla-check CHECK=AntflyMlCompilerPublication` | ML compiler publication | 67 | 36 | 7 | 00s |
| Heavy | `tla-check CHECK=AntflyHAReplication` | HA replication slot/fence/promotion/rejoin | 1,195,393,777 | 36,781,728 | 25 | 04min 27s |
| Heavy | `tla-check CHECK=AntflyDerivedReplay-heavy-depth` | derived replay MaxSeq=3 single-index bounds | 3,981 | 1,160 | 19 | 00s |
| Heavy | `tla-check CHECK=AntflyDerivedReplay-heavy-multi-index` | derived replay MaxSeq=2 two-index bounds | 90,773 | 17,176 | 27 | 00s |
| Manual | `tla-check CHECK=AntflyDerivedReplay-heavy` | derived replay MaxSeq=3/two-index bounds | 2,599,876,961 | 267,648,400 | 53 | 19min 14s |

Runtime-budget notes:

- `tla-check-smoke`, `tla-check-fast`, and `tla-check-negative` are suitable
  developer-facing gates; the largest fast-tier positive is
  document identity range repair at 95,040 distinct states.
- `tla-check-snap` and `tla-check CHECK=AntflyHAReplication` are the positive scheduled
  checks above one million distinct states.
- `tla-check CHECK=AntflyDerivedReplay-heavy` preserves the old combined MaxSeq=3 /
  two-index run as a manual confidence target. It has a nontrivial TLC
  fingerprint collision estimate (`calculated .034`, `actual .0019`) at the
  current bound, so the scheduled heavy tier uses the split depth and
  multi-index configs instead.

Smoke note: `tla-check-smoke` intentionally skips `occ-2pc.tla` because that
legacy file's top-level module is named `model`, so SANY rejects it when invoked
by filename. It is not part of the checked Makefile model targets.

Focused Zig validation for the first deepening area:

```bash
zig build lib-db-test -- --test-filter replay
zig build lib-db-test -- --test-filter "replay source primary store"
zig build lib-db-test -- --test-filter "db catch-up"
zig build lib-db-test -- --test-filter "isolated enrichment request error"
zig build lib-db-test -- --test-filter "enrichment runtime restore"
zig build lib-db-test -- --test-filter "enrichment treats missing local model as retryable"
zig build lib-db-test -- --test-filter "enrichment worker collects changed documents"
zig build lib-db-test -- --test-filter "generated enrichment"
zig build lib-db-test -- --test-filter "async asset producer graph source materializes through replay"
```

Current replay-filtered validation note: the direct replay-source and DB replay
tests pass through the modeled area, but a full `--test-filter replay` run is
not currently green in this worktree because later API/provisioned replay-status
tests fail. Treat that as a validation gap, not as implementation correspondence for the
derived replay model.

Focused validation already run for the deepened replay model:

- `make tla-check CHECK=AntflyDerivedReplay`: green after the code-alignment repair, 252
  distinct states.
- `make tla-check TIER=heavy`: green after the code-alignment repair;
  depth-heavy checked 1,160 distinct states and multi-index-heavy checked
  17,176 distinct states.
- `make tla-check CHECK=AntflyDerivedReplayBad`: latest-metadata-without-hint-lane
  mutant fails as expected.
- `bash ../scripts/tla-check.sh negative`: derived replay and enrichment mutants fail as
  expected.
- `make tla-check TIER=fast`: green with the bounded replay config.
- `make tla-check TIER=fast`: green after the critique repair pass.
- `make tla-check TIER=fast`: green after adding `AntflyHAFailoverSafety` to
  the focused fast tier.
- `zig build lib-db-test -- --test-filter "replay source primary store"`:
  green, 15 tests.
- `zig build lib-db-test -- --test-filter "db catch-up"`: green, 10 tests.

Focused validation already run for the deepened enrichment model:

- `make tla-check CHECK=AntflyEnrichmentLease`: green, 36,302 distinct states.
- `make tla-check CHECK=AntflyEnrichmentLeaseBadStalePublish`: stale lease publish
  mutant fails as expected.
- `make tla-check CHECK=AntflyEnrichmentLeaseBadEmptyPending`: hidden pending
  advancement mutant fails as expected.
- `make tla-check TIER=fast`: green after the enrichment deepening.
- `zig build lib-db-test -- --test-filter "isolated enrichment request error"`:
  green, 9 tests.
- `zig build lib-db-test -- --test-filter "enrichment runtime restore"`: green,
  10 tests.
- `zig build lib-db-test -- --test-filter "enrichment treats missing local model as retryable"`:
  green, 9 tests.
- `zig build lib-db-test -- --test-filter "enrichment worker collects changed documents"`:
  green, 9 tests.
- `zig build lib-db-test -- --test-filter "generated enrichment"`: green,
  17 tests.

Focused validation already run for the deepened LSM WAL/checkpoint/compaction
model:

- `make tla-check CHECK=AntflyLsmWalCompaction`: green, 10,026 distinct states.
- `make tla-check CHECK=AntflyLsmWalCompactionBadCheckpoint`: unsynced checkpoint
  mutant fails as expected.
- `make tla-check CHECK=AntflyLsmWalCompactionBadCorruptRotate`: corrupt-tail
  rotation mutant fails as expected.
- `make tla-check CHECK=AntflyLsmWalCompactionBadPinnedRetire`: reader-pinned
  retirement mutant fails as expected.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the LSM
  mutants.
- `make tla-check TIER=fast`: green after the LSM deepening.
- `zig build wal-test -- --test-filter "lsm wal rotates small segments and replays all records"`:
  green for the full `wal-test` target, 457 passed, 1 skipped. The filter did
  not narrow this target, but the relevant WAL segment/replay/corrupt-tail tests
  were included.
- `zig build root-test -- --test-filter "held snapshot pins exactly the retired segments it references"`:
  green, 9 tests.
- `zig build lsm-backend-test -- --test-filter "retires covered wal segments"`:
  green for the broader `lsm-backend-test` target, 228 passed, 1 skipped,
  including checkpoint/reopen/WAL-retirement coverage.

Current LSM validation note: an accidental broad `zig build unit-test
-- --test-filter "held snapshot pins exactly"` run was terminated after it
expanded into a large aggregate target. It is not counted as full validation.

Focused validation already run for the deepened LMDB commit/readers model:

- `make tla-check CHECK=AntflyLmdbCommit`: green, 36,967 distinct states.
- `make tla-check CHECK=AntflyLmdbCommitBadMetaBeforeData`: meta-before-data
  mutant fails as expected.
- `make tla-check CHECK=AntflyLmdbCommitBadReaderReuse`: reader-visible page reuse
  mutant fails as expected.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the
  LMDB mutants.
- `make tla-check TIER=fast`: green after the LMDB deepening.
- `zig build lmdb-test -- --test-filter "active readers delay free-page reuse across environments"`:
  green.
- `zig build lmdb-test -- --test-filter "oldest reader across multiple snapshots controls reclaim across reopen"`:
  green.
- `zig build storage-lmdb-test -- --test-filter "zig crash publish phases reopen to committed or previous state"`:
  green.
- `zig build storage-lmdb-test -- --test-filter "nested child transaction commit merges into parent state"`:
  green.
- `zig build storage-lmdb-test -- --test-filter "nested child transaction abort discards child state"`:
  green.
- `zig build lmdb-test`: green.
- `zig build storage-lmdb-test`: green.

Focused validation already run for the deepened HA replication/gates models:

- `make tla-check CHECK=AntflyHAGates`: green, 64,800 distinct states.
- `make tla-check CHECK=AntflyHAReplication`: green, 36,781,728 distinct states, depth
  24, 4m33s.
- `make tla-check CHECK=AntflyHAReplicationBadStaleTimelineAck`: stale timeline
  ack mutant fails on `SyncAckMatchesTargetTimeline`.
- `make tla-check CHECK=AntflyHAGatesBadStandbyRuntime`: standby mutating runtime
  mutant fails on `MutatingBackgroundRequiresWritablePrimary`.
- `make tla-check CHECK=AntflyHASyncWait`: green, 1,080 distinct states.
- `make tla-check CHECK=AntflyHASyncWaitBadMoveTarget`: promotion mutating a
  frozen target fails as expected.
- `make tla-check CHECK=AntflyHASyncWaitBadWrongTimelineAck`: wrong-timeline ack
  fails as expected.
- `make tla-check CHECK=AntflyHASyncWaitBadBelowTargetAck`: below-target ack
  fails as expected.
- `make tla-check CHECK=AntflyHATimelineSwitch`: green, 25 distinct states.
- `make tla-check CHECK=AntflyHATimelineSwitchBadBeforeApplied`:
  switch-before-parent-apply mutant fails as expected.
- `make tla-check CHECK=AntflyHATimelineSwitchBadNonMonotonic`:
  non-monotonic switch timeline/epoch mutant fails as expected.
- `make tla-check CHECK=AntflyHATimelineSwitchBadOldTimeline`: old-timeline
  record acceptance after switch fails as expected.
- `make tla-check CHECK=AntflyHATimelineSwitchBadRecoveryPrevious`: recovery from
  a switch whose `previous_lsn` mismatches recovered progress fails as expected.
- `make tla-check CHECK=AntflyHAStandbyApply`: green, 31 distinct states.
- `make tla-check CHECK=AntflyHAStandbyApplyBadFailureAdvances`:
  failed-apply progress advancement mutant fails as expected.
- `make tla-check CHECK=AntflyHAStandbyApplyBadDuplicateEffect`: duplicate replay
  side-effect mutant fails as expected.
- `make tla-check CHECK=AntflyHAStandbyApplyBadCrashLosesReceive`: crash losing a
  durably received unapplied record fails as expected.
- `make tla-check CHECK=AntflyHAStandbyApplyBadClientWrite`: standby client write
  mutant fails as expected.
- `make tla-check CHECK=AntflyHAStandbyApplyBadBackgroundRuntime`: standby
  mutating-background-runtime mutant fails as expected.
- `make tla-check CHECK=AntflyHARejoin`: green, 35,651 distinct states.
- `make tla-check CHECK=AntflyHARejoinBadUnfencedRewind`: unfenced rewind mutant
  fails as expected.
- `make tla-check CHECK=AntflyHARejoinBadExpiredWalRewind`: expired-WAL rewind
  mutant fails as expected.
- `make tla-check CHECK=AntflyHARejoinBadForcedRewind`: forced-promotion rewind
  without explicit policy mutant fails as expected.
- `make tla-check CHECK=AntflyHARejoinBadIdentityMismatchRewind`: identity or
  timeline mismatch rewind mutant fails as expected.
- `make tla-check CHECK=AntflyHARejoinBadStaleAssessment`: stale assessment
  truncate mutant fails as expected.
- `make tla-check CHECK=AntflyHARejoinBadForkMismatch`: missing/mismatched fork
  record truncate mutant fails as expected.
- `make tla-trace-ha TRACE_FILES="specs/tla/traces/ha_*.ndjson"`: green,
  3 checked-in HA trace fixtures validated.
- `make tla-trace TRACE=ha TRACE_FILES="specs/tla/traces/ha_*.ndjson"`:
  green, 3 checked-in HA trace fixtures validated through the dispatcher.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the HA
  mutants.
- `make tla-check TIER=fast`: green after the HA gate/runtime, sync-wait, and
  timeline-switch, standby-apply, and rejoin deepening.
- `make tla-check TIER=heavy`: green after the HA replication deepening and
  derived replay heavy split. HA replication checked 36,781,728 distinct states
  in 4m27s in the earlier HA pass. After the derived replay code-alignment
  repair, depth-heavy checked 1,160 distinct states and multi-index-heavy
  checked 17,176 distinct states.
- `make tla-clean && bash ../scripts/tla-check.sh negative && make tla-clean && make tla-check TIER=fast && make tla-clean && rg --files specs/tla | rg '_TTrace_' || true`:
  green after adding `AntflyHASyncWait`, `AntflyHATimelineSwitch`, and
  `AntflyHAStandbyApply`, and `AntflyHARejoin`; no generated TLC trace specs
  remained.
- `zig build ha-test`: green, 274 passed.
- `zig build root-test -- --test-filter "storage.ha primary rejects status updates for old timeline slots"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "storage.ha db standby role suppresses mutating background runtimes"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "storage.ha db primary progress sync wait observes reported remote apply ack"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "storage.ha db primary progress sync wait returns would block without reported ack"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "storage.ha db primary progress sync wait survives primary restart before ack"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "storage.ha db write gate rejects client writes on standby but allows replicated apply"`:
  green, 9 tests.
- `zig build ha-chaos-test -- --test-filter "storage.ha chaos primary restart preserves synchronous acknowledgement boundaries"`:
  exited successfully with no emitted test log.
- `zig build ha-chaos-test -- --test-filter "storage.ha chaos rejects noncontiguous records and follows timeline switch across restart"`:
  exited successfully with no emitted test log.

Current HA validation note: `AntflyHAReplication.tla` deliberately narrows
post-sync-wait exploration to receive/apply/status-ack paths. Full HA behavior
is still explored before a sync wait exists; this keeps the stale-ack slice
within the heavy tier while preserving the lower-level target timeline/LSN/mode
contract.

Focused validation already run for the HA failover safety critique repair:

- `make tla-check CHECK=AntflyHAFailoverSafety`: green, 205 distinct states.
- `make tla-check CHECK=AntflyHAFailoverSafetyBadPromoteMissingAck`: promoted-standby
  missing acknowledged write mutant fails as expected.
- `make tla-check CHECK=AntflyHAFailoverSafetyBadOldPrimaryWrite`: old-primary
  post-promotion write mutant fails as expected.

Focused validation already run for the deepened transaction/session model:

- `make tla-check CHECK=AntflyTransactionSession`: green, 1,946 distinct states.
- `make tla-check CHECK=AntflyTransactionSessionBadRollback`: rollback mutant fails
  on `NoVisibleUncommittedWrites`.
- `make tla-check CHECK=AntflyTransactionSessionBadRecoveryDecision`: wrong
  recovery decision mutant fails on `NoVisibleUncommittedWrites`.
- `make tla-check CHECK=AntflyTransactionSessionBadCleanup`: premature cleanup
  mutant fails on `CleanupRequiresAllResolved`.
- `make tla-trace-txn-session TRACE_FILES="specs/tla/traces/txn_session_*.ndjson"`:
  green, 3 checked-in session trace fixtures validated.
- `make tla-trace TRACE=txn-session TRACE_FILES="specs/tla/traces/txn_session_*.ndjson"`:
  green, 3 checked-in session trace fixtures validated through the dispatcher.
- `bash ../scripts/tla-check.sh negative`: premature
  cleanup fixture rejected as expected.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the
  transaction/session trace negative fixture.
- `make tla-check TIER=fast`: green after the transaction/session deepening.
- `zig build root-test -- --test-filter "db transaction abort leaves no visible document"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db aborted transaction preserves prior committed state and version"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db explicit resolveTransactionIntents applies participant-style commit version"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db recoverTransactions auto-aborts stale pending intents"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db participant recovery preserves finalized transaction until all participants resolve"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db transaction recovery runtime resolves participants and unblocks cleanup"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db transaction recovery runtime appends identity rows for committed orphaned intents"`:
  green, 9 tests.
- `zig build lib-db-txn-test`: green with no emitted test log.
- `zig build lib-db-txn-test -- --test-filter "recoverTransactions"`: green
  with no emitted test log.
- `zig build root-test -- --test-filter "api http server serves long-lived public transaction session routes"`:
  green, 10 selected tests.
- `zig build root-test -- --test-filter "transaction session"`: assertions
  passed in 26 selected tests, but the command failed because
  `api http server reloads durable transaction sessions after restart` leaked a
  JSON allocation in the test. This was treated as an existing validation caveat
  and not fixed during modeling.
- Direct `zig test pkg/antfly/src/api/transactions.zig --test-filter "transaction session registry"`:
  not a valid standalone command because the file imports modules outside the
  direct test module path; use build steps instead.

Current transaction/session validation note: an attempted `zig build db-test
-- --test-filter ...` lower-level run expanded into broad graph-heavy DB tests
instead of the requested transaction filters. It was terminated after unrelated
graph/replay failures appeared and is not counted as transaction validation.

Focused validation already run for the deepened document identity model:

- `make tla-check CHECK=AntflyDocumentIdentity`: green, 899 distinct states.
- `make tla-check CHECK=AntflyDocumentIdentityBadReuseOrdinal`: tombstoned ordinal
  reuse mutant fails on `AllocatedOrdinalsHaveStableOwner`.
- `make tla-check CHECK=AntflyDocumentIdentityBadStaleFilter`: stale filter mutant
  fails on `ResolvedFilterMatchesCurrentContext`.
- `make tla-check CHECK=AntflyDocumentIdentityBadNamespaceMismatch`: strict-open
  namespace mismatch mutant fails on `StrictOpenRejectsNamespaceMismatch`.
- `zig build root-test -- --test-filter "db resolved doc-set projection honors identity read generation"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db search requests default to current identity generation snapshot"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db validates internal resolved doc filter wire namespace and generation"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "identity namespace reassignment preserves snapshot generations and rejects stale writers"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "identity namespace reassignment rewrites canonical states"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "batch identity metadata persists ordinal mappings and delete generations"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "batch identity metadata fails closed at ordinal capacity"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db strict namespace reopen recovers after identity reassignment repair"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db explicit doc-id filter resolution honors identity generation"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db lsm primary compaction preserves doc identity ordinals"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db restore snapshot rejects invalid doc identity metadata"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db deferred restore rejects strict doc identity namespace mismatch"`:
  green, 9 tests.

Current document identity validation note: this model now covers stable ordinal
ownership, namespace/generation filter context, tombstone visibility, and
namespace repair/open behavior. It still abstracts exact identity key encoding,
canonical hash collision handling, split-range namespace transitions, and full
restore import ordering.

Focused validation already run for the document identity range/restore repair
model:

- `make tla-check CHECK=AntflyDocumentIdentityRangeRepair`: green, 95,040 distinct
  states.
- `make tla-check CHECK=AntflyDocumentIdentityRangeRepairBadSplitUnhealthy`:
  unhealthy split source mutant fails on `SplitRequiresHealthySource`.
- `make tla-check CHECK=AntflyDocumentIdentityRangeRepairBadSplitDestNamespace`:
  stale split destination namespace mutant fails on
  `SplitDestinationStatusMatchesExpectedNamespace`.
- `make tla-check CHECK=AntflyDocumentIdentityRangeRepairBadMergeMismatch`:
  merge namespace mismatch mutant fails on
  `MergeRequiresCompatibleIdentityStatus`.
- `make tla-check CHECK=AntflyDocumentIdentityRangeRepairBadMergeActiveReassign`:
  unapproved merge reassignment mutant fails on
  `MergeReassignmentRequiresOptInAndHealthyStatus`.
- `make tla-check CHECK=AntflyDocumentIdentityRangeRepairBadRestoreNamespace`:
  strict restore namespace mismatch mutant fails on
  `StrictRestoreRejectsNamespaceMismatch`.
- `make tla-check CHECK=AntflyDocumentIdentityRangeRepairBadRestoreEarlyClear`:
  early restore-intent clear mutant fails on
  `RestoreIntentClearsOnlyAfterRepairComplete`.
- `make tla-trace-doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson"`:
  green, 2 checked-in restore trace fixtures.
- `make tla-trace TRACE=doc-identity-range-repair TRACE_FILES="specs/tla/traces/doc_identity_restore_*.ndjson"`:
  green through the generic trace dispatcher.
- `bash ../scripts/tla-check.sh negative`:
  expected-failure trace rejected a mismatched strict deferred restore.
- `bash ../scripts/tla-check.sh negative`:
  expected-failure trace rejected restore-intent clear before runtime repair
  completion.
- `bash ../scripts/tla-check.sh smoke`, `make tla-check TIER=fast`, and
  `bash ../scripts/tla-check.sh negative`: green after wiring this model into the aggregate
  targets.
- `zig build root-test -- --test-filter "metadata split request validation rejects stale doc identity namespace"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "metadata merge request validation rejects incompatible doc identity namespaces" --test-filter "metadata merge validation handles rolling mixed-version doc identity status fixtures" --test-filter "metadata http server rejects split and merge during active doc identity reassignment before source mutation"`:
  green, 11 tests.
- `zig build lib-data-storage-test`: green, 11 tests, including destination
  namespace allocation, stale destination namespace rejection, and merge opt-in
  namespace application/rollback cases.
- `zig build root-test -- --test-filter "metadata service keeps restore intent until runtime repair completes"`:
  green, 9 tests.

Current document identity range/restore validation note: checked restore
fixtures now close the specific model-fixture gap for strict namespace mismatch
and incomplete-import-before-runtime-repair ordering. Attempted filtered
`zig build db-test -- --test-filter "db deferred restore rejects strict doc
identity namespace mismatch" --test-filter "db incomplete deferred restore
import recovers before runtime repair"` still expanded into the broad
2,185-test DB suite and encountered unrelated graph/enrichment failures before
the requested restore tests could be used as clean implementation evidence. It
was terminated and is not counted as passing Zig validation. Attempts to select
`db merge coordinator reassigns receiver identity namespace only after opt-in`
through `lib-data-storage-test` and `root-test` selected zero relevant tests, so
that exact test remains a build-filter validation gap even though adjacent
handoff opt-in tests passed.

Focused validation already run for the deepened DB split visibility model:

- `make tla-check CHECK=AntflyDbSplitVisibility`: green, 870 distinct states.
- `make tla-check CHECK=AntflyDbSplitVisibilityBadParentWrite`: parent
  child-range write mutant fails on
  `ParentCannotAcceptChildRangeAfterCutover`.
- `make tla-check CHECK=AntflyDbSplitVisibilityBadChildServe`: premature child
  serving mutant fails on `ChildServingRequiresReplayAndIndexes`.
- `make tla-check CHECK=AntflyDbSplitVisibilityBadMergeDonor`: post-handoff
  donor serving mutant fails on `MergeDonorDoesNotServeAfterHandoff`.
- `make tla-check CHECK=AntflyDbSplitVisibilityBadEnrichmentOwner`: stale
  enrichment owner mutant fails on `EnrichmentOnlyForCurrentRightOwner`.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the DB
  split/merge mutants.
- `make tla-check TIER=fast`: green after the DB split/merge deepening.
- `zig build root-test -- --test-filter "db split state and split deltas are exposed through public api"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db split finalization marks split-off document child ranges remote"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db shadow index manager backfills split-off range and ignores parent-range live writes after split"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db split prepare and finalize produce destination shard and trim parent range"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db split cutover fences enrichment to the owning range"`:
  green, 10 tests, including the durable LSM primary backend variant.
- `zig build root-test -- --test-filter "db merge-style cutover fences enrichment to the merged receiver range"`:
  green, 10 tests, including the durable LSM primary backend variant.
- `zig build root-test -- --test-filter "db split prepare survives reopen and finalizes text sparse and graph indexes with durable lsm primary backend"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db merge-style cutover routes text sparse and graph indexes to the merged receiver range with durable lsm primary backend"`:
  green, 9 tests.
- `zig build root-test -- --test-filter "db split cutover preserves enrichment resume and fencing across reopen"`:
  green, 10 tests, including the durable LSM primary backend variant.
- `zig build root-test -- --test-filter "db merge-style cutover preserves enrichment resume and fencing across reopen"`:
  green, 10 tests, including the durable LSM primary backend variant.

Current DB split visibility validation note: this model now covers the
implementation-level ownership, replay, index-readiness, artifact-placement,
enrichment-fencing, and merge-handoff contracts that the focused DB tests
exercise. It does not yet model exact row encodings, multiple child ranges,
payload contents, non-atomic durable crash points inside finalize, or a formal
refinement relation to `AntflyShardSplit.tla`.

Focused validation already run for the split refinement bridge model:

- `make tla-check CHECK=AntflySplitRefinementBridge`: green, 319 distinct states.
- `make tla-check CHECK=AntflySplitRefinementBridgeBadRouteBeforeDbServing`:
  metadata child-route-before-DB-serving mutant fails as expected.
- `make tla-check CHECK=AntflySplitRefinementBridgeBadDbServeBeforeShardCutover`:
  DB child-serving-before-shard-cutover mutant fails as expected.
- `make tla-check CHECK=AntflySplitRefinementBridgeBadStaleFenceCutover`: stale
  fence completion mutant fails as expected.
- `bash ../scripts/tla-check.sh smoke`: green after adding the bridge.
- `make tla-check TIER=fast`: green after adding the bridge to the fast tier.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the
  bridge mutants.
- `zig build root-test -- --test-filter "db split state and split deltas are exposed through public api" --test-filter "db split finalization marks split-off document child ranges remote" --test-filter "db shadow index manager backfills split-off range and ignores parent-range live writes after split" --test-filter "db merge-style cutover fences enrichment to the merged receiver range" --test-filter "db merge-style cutover routes text sparse and graph indexes to the merged receiver range with durable lsm primary backend"`:
  green, 14 tests.
- `zig build db-split-vopr-test`: green.
- `zig build db-split-sim-test`: green.
- `make tla-trace-split-bridge TRACE_FILES="specs/tla/traces/split_bridge_*.ndjson"`:
  green, 2 checked-in traces.
- `make tla-trace TRACE=split-bridge TRACE_FILES="specs/tla/traces/split_bridge_*.ndjson"`:
  green through the generic trace dispatcher.
- `bash ../scripts/tla-check.sh negative`:
  expected-failure trace rejected metadata child routing before DB serving.

Current split refinement bridge validation note: this model is intentionally a
small boundary check between `AntflyShardSplit.tla` and
`AntflyDbSplitVisibility.tla`. It strengthens the previous documentation-only
refinement boundary and now has checked trace fixtures, but those fixtures are
hand-authored rather than emitted by Zig tests. It does not model concrete row
encodings, multiple child ranges, leader timing, or crash/reopen persistence.

Focused validation already run for the deepened Lite/serverless publication
model:

- `make tla-check CHECK=AntflyLitePublication`: green, 599 distinct states.
- `make tla-check CHECK=AntflyLitePublicationBadManifestBeforeArtifacts`:
  manifest-before-artifacts mutant fails on
  `ManifestReferencesPublishedArtifacts`.
- `make tla-check CHECK=AntflyLitePublicationBadFailedHead`: failed-publication
  head advancement mutant fails on
  `FailedPublicationCannotAdvanceVisibleGeneration`.
- `make tla-check CHECK=AntflyLitePublicationBadPinnedCleanup`: reader-pinned
  cleanup mutant fails on `CleanupCannotDeleteReaderPinnedGeneration`.
- `make tla-check CHECK=AntflyLitePublicationBadMixedGeneration`: mixed visible
  generation mutant fails on `VisibleManifestReferencesPublishedArtifacts`.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the
  Lite publication mutants.
- `make tla-check TIER=fast`: green after the Lite publication deepening.
- `zig build serverless-test`: green in two complete broad runs, each with 330
  passed, 2 skipped, 0 failed, 0 leaked. This target ignored test filters, but
  the run included manifest CAS, manifest-write/no-head retry, vector/sparse/
  graph segment publication, query-runtime manifest pinning, publisher
  arbitration, retention/pruning, and object-store/catalog coverage.
- `zig build lite-cli-test`: green, 49 passed, including restore and snapshot
  publication paths.
- `zig build lite-native-test`: green in a serial rerun, 100 passed, including
  reader-pinned checkpoints, stable snapshots, free-map retention, native
  restore staging, aflite bridge, and check/vacuum coverage.

Current Lite validation note: a duplicate parallel `lite-native-test` run once
failed in restore staging while another full Lite native suite was running. The
same case passed in the serial rerun, so it is not counted as model/product
evidence. Future focused validation should avoid launching duplicate full Lite
targets because several build targets ignore `--test-filter`.

Focused validation already run for the deepened managed host lifecycle model:

- `make tla-check CHECK=AntflyManagedHostLifecycle`: green, 288 distinct states.
- `make tla-check CHECK=AntflyManagedHostLifecycleBadPrematureRestore`: premature
  restore activation mutant fails through
  `RestoreDoesNotActivateBeforeSuccess` / `DurableStoreForHostedReplica`.
- `make tla-check CHECK=AntflyManagedHostLifecycleBadStaleRoute`: stale route
  after metadata removal mutant fails through `RoutesOnlyForActiveReplicas` /
  `NoUndesiredRoute`.
- `make tla-check CHECK=AntflyManagedHostLifecycleBadReviveRemoved`: removed
  replica catalog revival mutant fails through `CatalogOnlyForDesiredReplica`.
- `make tla-check CHECK=AntflyManagedHostLifecycleBadRestoreCancel`: uncancelled
  restore bootstrap mutant fails through `RestoreBootstrapRequiresDesiredGroup`.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the
  managed-host mutants.
- `make tla-check TIER=fast`: green after the managed-host deepening.
- `zig build raft-test -- --test-filter "managed host restores backup bootstrap replicas from file-backed catalog on restart"`:
  green; this step is quiet on success.
- `zig build raft-test -- --test-filter "managed host removes live peer routes on metadata removal"`:
  green; this step is quiet on success.
- `zig build raft-test -- --test-filter "managed host default metadata and data apply stores survive restart"`:
  green; this step is quiet on success.
- `zig build lib-raft-sim-test -- --test-filter "managed host simulation removes routes and replicas across deterministic steps"`:
  green; this step is quiet on success.

Current managed-host validation note: the model now covers restore bootstrap
prepare/success/failure, cancellation on metadata removal, file-backed catalog
restart recovery, durable apply-store gating, and route removal. It still
abstracts peer membership details, WAL/proposal persistence, and leader
election.

Focused validation already run for the deepened ML graph/pass/runtime model:

- `make tla-check CHECK=AntflyMlGraphPasses`: green, 36 distinct states.
- `make tla-check CHECK=AntflyMlGraphPassesBadDanglingCse`: stale CSE remap
  mutant fails through `CurrentGraphReferencesValid`.
- `make tla-check CHECK=AntflyMlGraphPassesBadParameterDedup`: parameter/constant
  identity collapse mutant fails through
  `ParameterAndConstantIdentityPreserved`.
- `make tla-check CHECK=AntflyMlGraphPassesBadMissingLowerClosure`: fused export
  without primitive lower closure mutant fails through
  `ExportedGraphReferencesValid`.
- `make tla-check CHECK=AntflyMlGraphPassesBadFallbackRuntime`: fallback
  partition runtime publication mutant fails through `RuntimeGateFailsClosed`.
- `make tla-check CHECK=AntflyMlGraphPassesBadPartialPublish`: failed-pass
  partial output mutant fails through `FailedPassOutputNotVisible`.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the ML
  graph mutants.
- `make tla-check TIER=fast`: green after the ML graph/pass/runtime deepening.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "default pipeline optimizes graph"`:
  green, 3 selected tests.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "DCE removes vjp_alternate subgraph"`:
  green, 3 selected tests.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "CSE"`: green, 3
  selected tests, including the custom fold-then-CSE pipeline case.
- `cd lib/ml && zig test src/graph/lower.zig --test-filter "lower replaces fused linear with primitives"`:
  green, 1 selected test.

Focused validation already run for the bounded arbitrary-DAG ML CSE/DCE model:

- `source ../scripts/tla-tools.sh && cd specs/tla && "$TLA_JAVA" -cp "$TLA2TOOLS" tla2sany.SANY AntflyMlGraphDagPasses.tla`:
  green.
- `make tla-check CHECK=AntflyMlGraphDagPasses`: green, 9 distinct states over three
  bounded DAG shapes.
- `make tla-check CHECK=AntflyMlGraphDagPassesBadCseMissDuplicate`: missed
  duplicate CSE mutant fails through `Safety`.
- `make tla-check CHECK=AntflyMlGraphDagPassesBadCseNoConsumerRemap`: stale
  consumer/output/parameter remap mutant fails through `Safety`.
- `make tla-check CHECK=AntflyMlGraphDagPassesBadDceDropReachable`: dropped
  reachable-node DCE mutant fails through `Safety`.
- `make tla-check CHECK=AntflyMlGraphDagPassesBadDceNonTopoMap`: non-topological
  compact `id_map` mutant fails through `Safety`.
- `bash ../scripts/tla-check.sh smoke`: green after adding the DAG model.
- `make tla-check TIER=fast`: green after adding the DAG model to the fast suite.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the
  four DAG mutants.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "eliminate duplicate unary ops"`:
  green, 3 selected tests.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "eliminate duplicate binary ops"`:
  green, 3 selected tests.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "DCE id_map is correct"`:
  green, 3 selected tests.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "cleanup pipeline runs DCE"`:
  green, 3 selected tests.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "custom pipeline: fold then CSE"`:
  green, 3 selected tests.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "pipeline runs to fixed point"`:
  green, 3 selected tests.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "DCE removes vjp_alternate subgraph"`:
  green, 3 selected tests.
- `cd lib/ml && zig test src/graph/root.zig --test-filter "DCE preserves chained fused ops"`:
  green, 3 selected tests.
- `make tla-clean && rg --files specs/tla | rg '_TTrace_' || true`: no trace
  artifacts remain.
- `cd pkg/inference && zig build test -- --test-filter "computeOutputs finds graph outputs and cross-partition edges"`:
  green, 27 selected tests.
- `cd pkg/inference && zig build test -- --test-filter "buildExportableSubgraph preserves fused decomposition closure for lowering"`:
  green, 27 selected tests.
- `cd pkg/inference && zig build test -- --test-filter "buildExportableSubgraph materializes external pair second output as runtime input"`:
  green, 27 selected tests.
- `cd pkg/inference && zig build test -- --test-filter "graph runtime partition gates fail closed on fallback partitions"`:
  green, 27 selected tests.
- `cd pkg/inference && zig build test -- --test-filter "native graph runtime attaches native partition executors"`:
  green, 27 selected tests.

Current ML graph validation note: the full standalone `cd lib/ml && zig build
test` target is not green in this worktree because
`graph.passes.fuse.test.fuse detects SDPA pattern: 4D dynamic additive bias is
preserved` expects input index 9 and finds 4. That existing product-test
failure is not fixed or counted here. Direct `zig test` filters through
`src/graph/root.zig` are the counted pass-alignment evidence for this model.

Focused validation already run for the ML compiler/runtime publication model:

- `make tla-check CHECK=AntflyMlCompilerPublication`: green, 36 distinct states.
- `make tla-check CHECK=AntflyMlCompilerPublicationBadStaleCompile`: stale
  graph/export compiler publication mutant fails through
  `RuntimePublishesOnlyFreshCompleteArtifact`.
- `make tla-check CHECK=AntflyMlCompilerPublicationBadMissingInput`: missing
  parameter/cache runtime input mutant fails through
  `ExportMaterializesRequiredRuntimeInputs`.
- `make tla-check CHECK=AntflyMlCompilerPublicationBadOutputSelection`: semantic
  KV side-output leak mutant fails through `RuntimeOutputSelectionIsExact`.
- `make tla-check CHECK=AntflyMlCompilerPublicationBadFallbackPublish`: fallback
  partition executor publication mutant fails through `RuntimeGateFailsClosed`.
- `make tla-check CHECK=AntflyMlCompilerPublicationBadPartialArtifact`: failed
  compiler partial artifact mutant fails through
  `FailedCompileArtifactNotVisible`.
- `bash ../scripts/tla-check.sh negative`: green expected-failure harness after adding the ML
  compiler publication mutants.
- `make tla-check TIER=fast`: green after adding the ML compiler publication
  model.
- `cd pkg/inference && zig build test -Dpjrt=true -- --test-filter "PJRT compiler can externalize graph parameters as inputs"`:
  green, 27 selected tests.
- `cd pkg/inference && zig build test -Dpjrt=true -- --test-filter "PJRT compiler semantic KV option adds past inputs and present outputs"`:
  green, 27 selected tests.
- `cd pkg/inference && zig build test -Dpjrt=true -- --test-filter "PJRT semantic KV output selection keeps only the final graph output"`:
  green, 27 selected tests.
- `cd pkg/inference && zig build test -Dpjrt=true -- --test-filter "computeOutputs finds graph outputs in partition"`:
  green, 27 selected tests.
- `cd pkg/inference && zig build test -- --test-filter "graph runtime partition gate summary counts fallback partitions"`:
  green, 27 selected tests.

Current ML compiler validation note: default inference test builds do not include
the PJRT compiler tests, so PJRT-specific filters must be run with
`-Dpjrt=true` to count as evidence. The model still abstracts exact HLO/native
artifact bytes and backend capability matrices.

Trace validation commands:

```bash
make tla-trace-raft TRACE_FILES=/tmp/raft-trace.ndjson
make tla-trace-txn TRACE_FILES=/tmp/txn-trace.ndjson
make tla-trace-txn-session TRACE_FILES="specs/tla/traces/txn_session_*.ndjson"
make tla-trace-ha TRACE_FILES="specs/tla/traces/ha_*.ndjson"
```

## Maintenance Rules

- Add a row here when adding a new model.
- Update `Known gaps` when a model intentionally abstracts away behavior.
- Record the evidence shape rather than a maturity score; positive TLC validation
  and at least one documented negative validation are both required for `Checked`.
- Keep state-count/runtime notes in the model header or README after each major
  deepening pass.
- Run `make tla-clean` before checking status after TLC failures.
