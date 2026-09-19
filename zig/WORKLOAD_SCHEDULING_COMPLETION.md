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
- The metadata-control compiler now runs actual transaction-manager decision,
  acknowledgement, and cleanup logic on the bounded read-only overlay. It
  handles abort-before-prepare and metadata-only commit, terminal named ACKs
  after both prepared and unprepared outcomes, duplicate controls, retained
  terminal history, and already-absent cleanup without inventing a record.
  The owning gate passed 31 tests, including complete physical-state comparison
  against independently applied control operations for commit/abort and
  prepared/unprepared lifecycles, unchanged backing state during compilation,
  repeatable candidate bytes, and rejection of prepared document resolution
  or unlisted participants. This is physical-plan compilation, not completed
  control reservation: DATA routing and replay-free publication must wait for
  independent control ownership retained from begin through the final ACK.
- Transaction admission now includes a checked lifetime control-write bound.
  Each unique ACK rewrites the accumulated participant list; the previous fixed
  64x name-byte allowance did not bound that cumulative traffic. New begins and
  legacy-ledger initialization charge at least the complete metadata WAL bound,
  while preserving the existing small-transaction allowance. The ReleaseSafe
  durable-completion gate passed 32/32 tests, including actual canonical begin,
  commit/abort and 192 unequal binary participant ACKs measured with the native
  WAL encoder. Both orders exceed the old charge and fit the new certificate;
  admission rejects insufficient capacity before creating a transaction, and
  lowering the ceiling after begin does not block final-ACK retirement. This
  certificate excludes intent application, native ownership rows and arbitrary
  duplicate commands at new Raft indices. It is ledger admission accounting,
  not the outstanding physical control reservation or a retroactive upgrade of
  already-existing ledger entries. Evidence: `/tmp/workload-control-budget1.log`,
  actual exit0. Command from `zig/`: `zig build antfly-durable-completion-test
  -Doptimize=ReleaseSafe -j1 --cache-dir /tmp/zig-workload-scheduling-cache
  --global-cache-dir /tmp/zig-global-cache`.
- The owning durable-completion gate subsequently passed 29 tests, including
  six actual child-process kills: document and named-begin mutations stopped
  after acceptance, primary WAL append, and manifest publication. The parent
  forcibly terminates the child without DB close or error unwind, reopens with
  admission disabled, and verifies every final canonical physical operation,
  exact applied identity and duplicate application. These prove native storage
  process-death recovery at those boundaries; they do not prove quorum failure,
  partial-write/fsync faults, or transaction-control completion ownership.
- Native I/O fault qualification subsequently passed the 29-test owning gate
  with 20 child-process cases. Both ordinary documents and transaction begin
  now cover real, synced partial WAL and manifest-journal appends, injected
  append fsync failures, and injected SST file-sync, rename, and directory-sync
  failures, in addition to the six earlier completed-write crash boundaries.
  Each failed apply fences same-process retry and preserves the exact accepted
  sidecar before the parent kills the process. Recovery with admission disabled
  verifies every final physical mutation and its applied identity; a second
  reopen verifies durable progress with no pending accepted cell, then repeats
  the physical-state and idempotent-apply checks. The low-level callbacks are
  test-only. These are actual native filesystem/process tests with injected
  syscall errors, not power-loss durability tests or live-quorum qualification;
  accepted-only reconciliation still uses a test-supplied durable-log proof.
- Trusted local installation capsules now restore actual accepted ordinary
  debt with empty metadata/catalog access, no service keys, and new admission
  disabled. Five owning SourceOwner tests passed, including immutable identity,
  atomic publication cuts, exact accepted reconciliation and duplicate apply.
  Six owning DATA tests passed for local restoration/authorization, native-owner
  shutdown before coordinator destruction, and canonical protocol selection.
  These component results do not replace real replicated process/quorum faults.
- The subsequent owning DATA gate passed three tests (one implementation,
  two compiled consumers; zero skips, failures or leaks). A two-voter RawNode
  fixture rejects a v7 peer before native installation, applies the protocol8
  barrier through the real local Raft WAL, and rechecks current membership
  before publishing backing. It then proposes an ordinary write through the
  C physical compiler/native reservation, restarts the whole DATA server with
  admission disabled and no service keys, and applies the retained WAL entry
  after a higher-term leader heartbeat. Document state and the permanent native
  progress digest match the accepted entry. BEGIN selects protocol8, while
  prepare retains protocol7. Peer votes/acknowledgements are delivered explicitly
  in-process; this does not certify a real peer's durable acknowledgement or a
  multi-process quorum fault. Fresh production activation remains disabled.
  Evidence: `/tmp/workload-completion-proposal-wal3.log`, actual exit0.
- Original public admission deadlines now reach DATA with their borrowed clock
  and cancellation intact. A synchronous Raft admission callback checks again
  after the native reservation and before RawNode assigns an index; rejection
  invokes the null-receipt cancellation path. The owning DATA gate passed all
  three tests with zero skips, failures or leaks, including expiry after an
  actual accepted sidecar is reserved, unchanged Raft log and zero retained
  accepted cells, then successful retry after production idle maintenance/rearm.
  SourceOwner invokes that existing trusted installation path before taking
  DATA's lock, so cancellation cannot permanently strand a spent generation.
  Expiry after index assignment leaves the accepted result unchanged; the same
  write survives disabled-admission WAL restart. Evidence:
  `/tmp/workload-completion-original-deadline-data2.log`, actual exit0.
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

- First coordinator decisions now use a dedicated local DATA callback carrying
  the original deadline. It never forwards or campaigns; only an actual user
  entry receipt makes the result uncertain, independently of protocol barriers.
  SourceOwner checks the original context after descriptor/owner acquisition and
  before invoking C, and preserves all errors after that invocation. The owning
  DATA gate passed four tests (one implementation, three consumer), with zero
  skips, failures or leaks. Its real single-voter WAL/C-owner fixture verifies
  missing-owner and catalog-time expiry rejection, unchanged log on routing
  expiry, accepted-then-lost-response uncertainty followed by committed apply
  after expiry, and follower rejection without forwarding. Existing native
  reservation cancellation/rearm and capsule/WAL restart regressions also pass.
  Evidence: `/tmp/workload-first-decision-data1.log`, actual exit0. This tests
  the decision certainty boundary on a legacy table; protected control capacity
  and replicated quorum-failure qualification remain separate requirements.
- SourceOwner preserves the independent recovery ACK deadline and abort token
  through owner acquisition, checks them immediately before invoking the C
  owner, and leaves post-invocation results unchanged. The real compiled-owner
  regression forces expiry during catalog acquisition, verifies no late ACK or
  abort was submitted, then completes both with a fresh recovery window. It
  also preserves the native InvalidParticipant error through the checked ABI.
  Evidence: `/tmp/workload-recovery-owner-deadline6.log`, actual exit0, one test
  with zero skips, failures or leaks. This stage prevents late submission;
  bounding descriptor acquisition itself remains the next stage.
