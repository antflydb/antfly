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
