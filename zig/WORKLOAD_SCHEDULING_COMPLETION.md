# Full scheduling completion work

This tracks the requested items 1–8 against the implementation, starting at
`3dc3ac6c4d`. It does not replace or reduce the design in
[WORKLOAD_SCHEDULING.md](WORKLOAD_SCHEDULING.md). All eight requirements remain
open until the evidence below exists. Fixed-policy performance qualification,
new defaults, adaptive policy and Cloud product integration are separate from
this requested implementation/correctness scope.

| Item | Required final behavior | Evidence required before completion |
| --- | --- | --- |
| 1. Ordinary and transaction-control writes | Every accepted mutation has a canonical physical plan and retained completion resources before consensus acceptance, including ordinary writes and begin/decision/ack records; no false prepared transaction or early applied progress | Real DB/compiler/owner/Raft tests covering each mutation, concurrent prepared reservations, allocation/descriptor exhaustion, acceptance retry, log replacement and restart |
| 2. Capacity guarantees | Aggregate and contiguous memory, retained generations, maintenance output count, future accepted growth and counter headroom are certified before acceptance | Adversarial legal maximum shapes and allocator fragmentation, four outstanding cells, pinned readers, repeated maintenance, counter exhaustion and restored obligations; no allocation after the promise that requires ordinary admission |
| 3. Self-contained restoration | Trusted local installation state reconstructs existing obligations without metadata/service-key availability, even with new admission disabled; authoritative later metadata cannot silently replace identity | Atomic-publication failure tests, missing/corrupt/mismatched capsule tests, actual restart with metadata unavailable and subsequent reconciliation |
| 4. Write profiles and lifecycle | External payloads, child-range outboxes and named acknowledgements have owned recovery or explicit supported-profile restrictions; installed pools have safe schema/policy, snapshot/compaction, retirement and replica/topology transitions | Supported-path functional tests and unsupported-path rejection before durable obligations; race/restart tests at lifecycle boundaries; documented restrictions |
| 5. Replicated recovery | Promises survive real leader/follower/process failure, durable-log replacement, rolling versions and shutdown after decisions; uncertain I/O preserves ownership | End-to-end quorum scenarios with forced failure points, actual partial-write/fsync/publication fault injection, bounded recovery and exact data/progress assertions |
| 6. Protected process progress | Control, replication, recovery and cleanup have protected resources through connections, dispatch/execution, memory and storage under foreground saturation | Combined resource saturation with measurable protected progress, bounded use and cleanup; independent connection/executor/memory pressure and their combination |
| 7. Operator scheduling and allocation | Supported operators/runtimes account for helpers, fan-out, merges, scratch and retained state; audited suspension, lane isolation, demotion, queue selection and starvation rules hold | Operator/entry-point ownership inventory tied to code; deterministic suspend/resume/cancel/demotion/large-request tests and real mixed-runtime saturation checks |
| 8. Distributed/client contracts | Original deadlines and ownership survive SDK streams/retries and RPC fan-out; unknown writes are not replayed; cancellation, shutdown, late replies and rolling peers preserve bounded uncertainty | All supported SDK/frontend contract tests plus real/deterministic coordinator-worker fault schedules; transaction coordinator recovery-handoff audit |

## Current work

- Item 1: introduce explicit single-phase canonical mutations; extend the real
  physical compiler, native reservation/application and DATA proposal routing.
- Item 2: implement format-cost certificates, separated retained/transient
  capacity and checked arithmetic headroom.
- Item 3: persist and restore a trusted installation capsule before acceptance.
- Item 8: close streamed-response deadline ownership gaps and continue the
  distributed/client contract audit.

The existing prepare/resolve and local restart tests remain useful component
evidence. They do not prove the rows above. Production activation must not be
enabled until its write, capacity, restoration and replicated recovery
prerequisites are implemented and verified.

## Current integration evidence and boundaries

- Canonical single-phase envelopes use wire version 2 and require Raft batch
  protocol 8. Version 1 prepares remain byte-compatible. Seven owning codec
  tests passed. DATA now advertises version 8 and requires that floor for
  ordinary canonical mutations; prepare retains its version 7 floor. The
  owning DATA selection regression passed. Actual mixed-peer proposal and
  Raft-WAL fault schedules remain to qualify; production activation is disabled.
- The native capacity certificate stage passed ten owning tests, including
  actual SST encoding, before-acceptance counter/capacity rejection, accepted
  restart and retained readers. Separating retained allocations from transient
  workspace and proving aggregate allocation overhead remain in progress.
- Python streamed-response deadline and TypeScript cancellation ownership
  fixes are committed. The coordinator now discards late successful responses
  after retiring a verified terminal attempt; unsigned outcomes remain charged.
  Its owning API test passed all four signed/unsigned and cancel/expiry cases.
- Ordinary-write compilation is being integrated at the real DB planner's
  final physical batch boundary. Compilation must not publish primary data,
  consume a replay sequence, or mutate the artifact-presence hint. Baselines
  include source documents even when semantic-noop elimination removes their
  writes, timestamp predicates, absent intent locks, and physical-writer reads.
  The owning durable-completion gate passed 26 tests, including complete
  physical before/after comparison with an independently published ordinary
  batch for document and relational rows, nonpublishing repeat compilation,
  and native rejection when only a predicate timestamp or intent lock changes.
  The subsequent 26-test owning gate also accepted and applied that candidate
  through the real retained C lease with ordinary memory admission exhausted,
  verified duplicate application and complete physical state, and confirmed
  that no prepared transaction record was created. Native apply drains the
  mutation and its progress together; DB publication consumes retained replay
  and visibility ownership before cohort retirement. DATA routing is connected;
  its full proposal/fault qualification and transaction-control reservations
  remain open.
- The subsequent owning durable-completion gate passed 27 tests, including
  single-phase failure cuts after the actual primary WAL and manifest
  publication. Reopening with admission disabled restores the document and
  exact applied identity, creates no prepared transaction, and tolerates
  duplicate application. These are storage component cuts, not process/quorum
  qualification.
- Canonical begin compilation now runs the real transaction manager against a
  read-only overlay, preserving coordinator/follower participant selection and
  completion-ledger accounting. The C owner accepts the resulting single-phase
  metadata write without inventing a document replay event. The owning gate
  passed 28 tests: exact physical comparison with independently applied begin,
  application with ordinary memory admission denied, and document/begin restart
  after acceptance, WAL append and manifest publication (six restart cases).
  This exposed and fixed a lazy replay-cache lock re-entry during accepted-only
  restoration; installation also reconciles the ledger before creating a pool.
  These results cover the initial begin write, not its future control debt:
  abort-before-prepare, metadata-only coordinator decisions, and named ACKs
  after local resolution still need retained control ownership. Named prepares
  remain restricted and production activation remains disabled.
- Trusted local installation capsules now restore actual accepted ordinary
  debt with empty metadata/catalog access, no service keys, and new admission
  disabled. Five owning SourceOwner tests passed, including immutable identity,
  atomic publication cuts, exact accepted reconciliation and duplicate apply.
  Six owning DATA tests passed for local restoration/authorization, native-owner
  shutdown before coordinator destruction, and canonical protocol selection.
  These component results do not replace real replicated process/quorum faults.
- The current single-phase candidate profile rejects external payload stores,
  generated-enrichment producers, graph-index catalogs, child-range dispatch,
  HA mirrors, split/shadow/bulk state, and structural commands. In particular,
  point dependencies do not certify artifact-prefix phantoms or skipped
  coverage updates by asynchronous producers. Enabling those profiles requires
  owned range/version dependencies covering every producer, or another fully
  verified recovery path. These restrictions are checked before candidate
  publication and again at its serialized capture boundary.
- A full pool of prepared transactions cannot be allowed to consume the
  resources needed by its own decision/ack writes. Their control-mutation
  capacity must be reserved with the transaction and retained through its
  final acknowledgement. Adding a free-slot check alone would deadlock this
  lifecycle; the separate control reservation is still outstanding.
