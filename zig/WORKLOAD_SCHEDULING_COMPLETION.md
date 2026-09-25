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

- Trusted local restart now reconstructs one slot-0 applied BEGIN owner before
  WAL replay from its published guard, measured manifest runs, and retained
  capacity. It requires exact committed v2 BEGIN proof before readiness and
  then applies a real owner-backed decision. The DB test rejects missing
  manifest, orphan document guard, output-ID alias, and wrong v2 digest;
  the durable gate passed 51/51 with no leaks. Accepted transition sidecars,
  already-applied decision/ACK restart, multiple owners, and larger run
  profiles remain fenced; production activation is still disabled.
- A fresh ReleaseFast binary at `08807fe878` passed the six-process ordinary
  DATA quorum driver with zero cleanup errors. The driver checked all-replica
  applied progress, successor election after DATA leader loss, exact reads,
  and restart of the killed node. Receipt:
  `/private/tmp/antfly-workload-data-quorum-08807-qual-1/receipt.json`.
  This does not qualify replicated physical completion or performance.
- An ordinary DATA startup failure under a contended owner-source mutex was
  fixed by publishing an exact installed-group side index. A missing
  installation now returns `NotFound` to the completion provider while an
  installed group or side-index overflow remains fail-closed. The focused
  owner-source and DATA callback gates each passed 1/1; the live driver above
  passed on a binary containing the fix.
- A sole staged owner's terminal ACK now tombstones its owner row in the
  canonical WAL batch, checkpoints the full mutable state to its reserved run,
  journals the manifest, resets WAL, then unlinks/syncs its guard and releases
  ownership. Other retained owners or live document completions reject the
  terminal ACK before acceptance. A real DB fixture covers success/reopen,
  same-process requalification, post-manifest and post-WAL-reset interruption,
  and a native journal-sync fault. The durable gate passed 50/50; the workload
  gate passed 310/310 owning and 393 broad tests with one skip, no leaks.
  Runnable nonterminal-owner restoration and multi-owner terminal interleavings
  remain open.
- A volatile, test-only control transition gate now accepts the first decision
  for a staged BEGIN into an exact durable sidecar before Raft acceptance. Its
  retained owner prepays the canonical decision, native owner/progress rows,
  document applied marker, and group progress in one WAL batch; uncertainty
  fences admission, exact duplicate apply is idempotent, and restart remains
  fail-closed. The focused durable gate passed 49/49 after merging main.
- The same staged owner now applies named ACKs with a v3 progress receipt. Its
  208-byte row carries an exact 96-bit participant bitmap, checked against the
  canonical resolved list before admission and apply. A real four-owner DB
  fixture covers out-of-order ACKs and duplicate rejection; the durable gate
  passed 49/49, the workload gate 309/309 owning and 392 broad tests with one
  skip, and the v3 capacity fixture passed 1/1. The staged path rejects cohorts
  above 96 participants. Production activation remains disabled; final
  checkpoint/owner retirement, runnable restoration, and failure-injected
  replicated ACK recovery remain open.
- A coordinator regression now reuses a stable transaction ID, BEGIN timestamp,
  and participant cohort with different writes after an ambiguous committed
  status. It remains `CommitDecisionUnknown` without prepare, resolve, or
  abort; the API transaction gate passed 95/95. A durable write-set binding is
  still required before identity-bearing status can authorize a retry.
- A direct rejection of a staged native BEGIN now retires its exact accepted
  sidecar and control guard before refunding the retained owner. A real DB
  reopen succeeds afterward. Generic completion cells reject metadata
  decisions/ACKs touching a staged owner's transaction keys before acceptance,
  including prepared outcome templates. The owning durable-completion gate
  passed 44/44 tests. Accepted control owners still need owner-backed
  decision/ACK apply, durable retirement, and runnable restart restoration.
- A staged BEGIN also prepays and pins the exact future output-run path named
  in its durable guard for the owner's retained life; proven rejection and
  teardown release that pin. The real DB gate passed 44/44 again. This is one
  output-capacity prerequisite, not a certificate for interleaved control
  transitions or their accepted replay.
- Staged control proof is now capped at the 68 input handles actually prepaid
  by the native path. A real DB fixture reaches 64 runs after installation and
  rejects a canonical BEGIN requiring 69 inputs before owner/guard/transaction
  publication; configured reopen succeeds. Another DB fixture accepts four
  interleaved staged owners across maintenance with three retained readers.
  The durable-completion gate passed 45/45, no leaks. Aggregate publication,
  future control transitions, and restart capacity remain open.
- Trusted local restart now decodes and validates every staged control guard
  against the configured installation before retaining the fail-closed fence.
  The real DB restart test distinguishes a valid guard's
  `CompletionRecoveryCapacityRequired` from a corrupted envelope's checksum
  failure; the owning gate passed 45/45. The current durable-log ABI still
  lacks independent control-owner observations and cannot restore runnable
  resources, so this validation does not complete item 3.
- The checksummed local completion capsule now carries the authenticated
  first-open range hint. The two-voter DATA regression passed 1/1 after
  offline restoration and later catalog admission; its prior failure was a
  pinned-owner descriptor mismatch because the hint was absent. This does not
  complete multi-process quorum fault qualification.
- A waiter behind a blocked scheduler head now expires at its own max-wait
  bound and releases queue ownership (focused regression 1/1). A separate
  HTTPX listener remains available with the public listener's connection slot
  saturated and after public-listener shutdown (full HTTPX suite 608 passed,
  eight skipped). Production internal endpoint routing/trust and combined
  process-pressure qualification remain open.
- A real socket fixture now holds all public connection slots, the sole
  general request task, and the HTTP/1 body-buffer permit while a one-record
  durable workload journal is full. The protected listener still returns a
  signed terminal status, and public resources drain within bounded time after
  release. Its focused DATA runtime gate passed 1/1 with no leaks. Native LSM
  disk exhaustion and a shared process-wide memory ceiling remain open.
- A real native FD-pool fixture now blocks a foreground file write at the
  capacity-two descriptor limit while a preowned completion scope appends and
  syncs WAL data and atomically publishes/syncs its allowlisted SST. The
  foreground write resumes only after the scope releases both descriptors;
  waiter and descriptor counts drain to zero. The focused storage gate passed
  1/1 with no leaks, and the aggregate storage target exited 0. This proves
  shared FD-pool progress, not disk-full recovery or all storage domains.
- Store metadata now has an optional internal HTTP endpoint in JSON and the
  durable record extension. Older empty registrations retain their old wire
  encoding. A nonempty endpoint requires metadata decoder protocol 14 on every
  applying member before admission; three focused metadata gates passed 1/1
  each. DATA can now opt into a separately bound and advertised authenticated
  recovery listener with its own protected listener/connection/request workers
  and HTTP runtime. The dedicated Data-Raft transaction-status call selects the
  internal endpoint after protected metadata lookup; ordinary routes keep the
  public endpoint. The real socket test passed 1/1 under public connection
  saturation and after public shutdown, with auth/config validation; direct
  and linked API ingress passed 3/3, and the production DATA artifact built
  33/33. Full internal client routing, storage/memory pressure, and combined
  process-wide progress remain open.
- Hosted transaction decide/resolve/status/ack now select a placement-checked
  internal endpoint only when the worker has service-signing credentials;
  older or unsigned peers use the public URL. Begin/prepare and ordinary group
  routing stay public. The transaction API gate passed 95/95, and DATA's
  dedicated recovery-status auth fallback plus socket gate passed 2/2. The
  workload coordinator now uses a versioned v2 ABI to route authenticated
  discovery, fencing, and reconciliation to the internal endpoint while query
  traffic stays public. Version 1 and older catalog records retain the public
  fallback. The workload gate passed 305/305 owning tests and 388 broad tests
  (one skip), with no failures or leaks. Combined resource saturation remains
  open for item 6.
- A separate v2 control-proof ABI now carries retained BEGIN observations and
  the unchanged v1 document proof from one borrowed durable-log image. Native
  reconciliation validates both under one DB/backend lock; v1 reconciliation
  refuses a group with retained control owners. A missing v2 issuer, replaced
  or compacted BEGIN, and mismatched identity fail closed. The connected native
  gate passed 45/45, and the workload gate above exercised its DATA bridge.
  This does not yet restore runnable control resources or certify decision/ACK
  capacity and replay.
- The real preaccept path now classifies a retained owner's canonical decision
  and unique named ACKs against current transaction state but still rejects
  them before a generic cell can be allocated. A separate checksummed v2
  progress receipt format retains BEGIN/latest identities, decision kind, ACK
  count and resolved-set digest; control capacity charges its larger row while
  v1 receipt bytes stay unchanged. Real compiler tests cover decision, three
  unique ACKs and duplicate rejection; the durable gate passed 47/47. The
  receipt is not written yet, and the decision/ACK apply and self-contained
  restoration paths remain fenced.
- A two-process Raft fixture now sends production binary frames between a
  leader child and follower parent. The follower reserves native completion
  capacity and persists its Ready before ACK; after leader process kill it
  reopens its WAL and native DB, reconciles the accepted cell, and checks the
  exact committed payload, document, index/term and progress digest. It passed
  in the same 47/47 durable gate. It uses file IPC and does not exercise
  DataServer networking, catalog recovery, or replacement-leader election.
- A six-process ordinary DATA quorum driver now checks real metadata/DATA
  voter identity, all-replica applied progress, successor election after DATA
  leader kill, and exact reads after the killed node restarts. Its script and
  existing metadata-runner tests passed 12/12. A live run still needs a fresh
  pinned umbrella binary; replicated physical completion remains explicitly
  disabled and is outside this driver's claim.
- Distributed join now stages all fanout hits until every batch succeeds;
  late-batch and sequential failures leave caller output unchanged. The
  curated focused API regression passed 1/1. This covers atomic result
  publication, not the full distributed/client contract inventory.
- Parallel table-read preflight now joins each worker wave and checks the
  request cancellation/deadline before dispatching later waves or publishing
  success. A deterministic five-group regression cancels during the first
  two-worker wave, returns `Cancelled`, and proves the remaining three were
  not dispatched; the focused API test passed 1/1 with no leaks.
- Parallel graph expansion now charges every worker's transient response arena
  to the request allocator through one synchronized backing object. Its
  lifetime extends through result merge and worker join, including canceled
  groups. A 1 MiB request quota rejects two workers' 2 MiB scratch allocations;
  the curated graph gate passed 16/16 with no leaks. Incoming, root, hydration,
  and edge fanout now use the same request-backed lifetime rule, with canceled
  groups joined before slot inspection. The post-merge graph gate passed 17/17,
  including quota assertions for incoming, root, edge, and expansion/hydration
  paths. Other operators and real mixed-runtime saturation remain open.
- An explicit conflicting stable-ID BEGIN no longer uses a pending or
  committed status enum to abort or propagate an older transaction whose
  BEGIN identity has not been proved equal. Coordinator and follower conflict
  schedules passed the curated stable-retry regression 1/1. Ambiguous committed
  retries also fail closed as `CommitDecisionUnknown`: a status-only committed
  reply cannot authorize success or phase two without a matching BEGIN
  timestamp and participant identity. The renamed curated retry regression
  passed 1/1. Identity-bearing status and eventual successful retry remain
  open; the wire schema is unchanged for rolling peers.
- Scan working-memory allocation now clears stale admission failure provenance
  after success and reports backing resize/remap OOM accurately. Its focused
  failing-allocator regression passed 1/1; after a stale HTTP test-call repair
  and a corrected WAL physical-versus-completed-applied assertion, the
  workload-admission gate passed 302/302 owning tests and 387 broad tests
  (one skip), with no leaks. The WAL fixture now requires state-machine replay
  before reporting a completed applied index.
- Streamed scan output now suspends a read lease only when the scan owns both
  that lease and its prepaid working-memory buffer. A borrowed lease stays
  runnable while a generic caller allocator backs scan state; the real LMDB
  callback regression passed. The workload-admission gate passed 303/303
  owning and 387 broad tests (one skip), no leaks. Other operator footprints
  and mixed-runtime saturation still need qualification.
- Native restart now fails closed on any published or interrupted transaction
  control guard before ordinary WAL replay, including when a completion pool is
  configured. BEGIN guard validation binds the accepted index and term to the
  canonical envelope. The owning durable-completion gate passed 42/42 tests,
  including an actual native reopen with corrupt and pending control guards.
  This is a restoration fence, not restoration of control obligations; items
  1–3 remain open.
- Commit completion and required cleanup now have separate fixed executor
  reservations within the previous eight protected workers. The focused
  background-runtime tests passed 2/2, including cleanup saturation while a
  commit job completes. Nested I/O, memory, storage, connections, and combined
  process pressure remain open for item 6.
- HTTP/1 body ingress now has an opt-in nonborrowable recovery slot inside the
  configured total. Its request classifier verifies the internal-service
  credential before selecting that slot. The unfiltered lib-httpx suite passed
  606 tests with eight skips, including held general-upload, recovery-body and
  disconnect tests. The direct and linked Antfly ingress adapter gate passed
  2/2. Connection-slot and aggregate body-byte pressure remain open.
- Python synchronous and TypeScript streamed query responses now retain the
  original deadline during body consumption; late chunks are rejected and the
  underlying stream is closed or canceled. Python SDK passed 258 tests and the
  TypeScript SDK passed 382 tests with one existing skip. A synchronous Python
  transport read remains noninterruptible until that read returns.
- The Go SDK now keeps the original query deadline through successful response
  body consumption and rejects late headers/chunks even when a custom transport
  ignores cancellation. Retried query bodies rewrite only bounded top-level
  `timeout_ms` value tokens to the remaining original budget; writes and
  ambiguous bodies are never replayed. The full Go SDK suite and race-tested
  read-retry tests passed.
- Go, Python, TypeScript and Rust now reject ambiguous 429 retry proofs with
  duplicate fields, malformed UTF-8 or case-changed keys; an unambiguous proof
  with a future extension field still qualifies. Focused gates passed for Go,
  Python (29/29), TypeScript (27/27) and Rust (4/4). A preexisting Rust loopback
  fixture also now makes its accepted socket blocking before reading.
- Fresh transaction BEGIN acceptance now validates the actual canonical
  control shape before spending a native accepted cell. The DB fixture
  truncates a real compiled BEGIN and verifies rejection without sidecar
  publication, then accepts the valid candidate. The owning durable-completion
  gate passed 42/42. This does not yet retain decision/ACK capacity.
- Distributed recovery status requests use the earlier of the worker recovery
  deadline and an explicit request deadline, including scoped restore probes.
  Replayed scoped participants also carry their parsed restore scope and plan
  into the resolution request. The new bounded-status test and existing scoped
  LSM-reopen test passed. The broader API transaction step passed 89/92 tests;
  unrelated metadata retirement, native read-index absence, and stable retry
  tests failed and require separate triage. Item 8 remains open.
- Item 6: durable maintenance and mandatory commit/cleanup jobs now use
  separate `BackendRuntime` executors. The fixed default budget allocates
  40 maintenance workers and 8 protected workers within the existing 252-worker
  aggregate. This reserves their initial dispatch/execution threads while the
  maintenance executor is saturated. Nested I/O paths and process-wide memory,
  storage, connection, and combined-pressure qualification remain open.
- Item 1: retain separate durable control ownership from begin through the final
  acknowledgement, then connect its preowned compiler and DATA proposal path.
  Ordinary writes and initial begin application already have canonical plans.
- Item 2: extend the pooled allocation/output/counter certificates to the entire
  control lifetime, including interleaved owners and reader-retained versions.
- Item 3: extend the existing trusted local capsule restoration to these new
  control obligations and their durable-log reconciliation.
- Item 8: finish bounded owner acquisition and independently owned background
  open/recovery handoff, including deadline clock translation and cold owners.

The existing prepare/resolve and local restart tests remain useful component
evidence. They do not prove the rows above. Production activation must not be
enabled until its write, capacity, restoration and replicated recovery
prerequisites are implemented and verified.

### Retained control resources and immutable owner format

The physical resource component now provides a reusable proposal buffer and
critical compiler scope separate from monotonic publication backing and WAL
credit. The real transaction-manager compiler uses that scope for metadata
decisions and ACKs. It releases shared compiler scratch before invoking native
admission, so admission can reuse the workspace without invalidating the proposal.
Rejected captures consume neither publication capacity nor WAL credit. WAL credit
transfers to the backend before an append attempt and stays there after uncertain
I/O; reader-held publication allocations outlive the resource handle.

A fixed 256-byte immutable native control-owner record binds group, incarnation,
policy, schema, generation, transaction, exact accepted BEGIN identity, and the
ordered participant encoding's digest/count/length. Its separate 48-byte receipt
is retained after terminal owner deletion. These are codec and resource building
blocks: persisting/restoring owners, connecting their acceptance/application, and
retaining them through final ACK remain open. No consensus authority follows
from constructing a resource handle or decoding a checksummed record.

Canonical initial mutations and both outcome templates now reject writes to
native ownership namespaces, including the new owner and receipt keys. Public
control sizing also reports distinct maximum key and record sizes, checked against
the actual 192-participant commit/abort compiler output rather than treating a
large participant-list value as an SST key bound.

The owning Debug durable-completion gate passed 38/38 tests, zero skips, failures,
or leaks, with actual process exit 0. This includes repeated canceled captures,
all construction allocation failures, a live publication reader after owner
release, actual decision/three-ACK compilation with ordinary allocation and
admission denied, exhausted ordinary compiler epochs, record corruption and
identity mismatch, and forged native rows in prepare/commit/abort plans. Snapshot
acquisition and durable publication are outside the compiler-only proof. Command
from `zig/`: `zig build antfly-durable-completion-test -j1 --cache-dir
.zig-cache/workload-local --global-cache-dir .zig-cache/workload-global`.
Evidence: `/tmp/workload-control-resources2.log`.

### Deterministic acknowledgement rejection preserves Raft progress

`InvalidParticipant` now crosses the native failure ABI precisely and is an
expected DATA command rejection. Previously it could become an opaque native
failure, or escape the apply state machine's deterministic rejection set and
stop the committed batch behind an invalid named ACK. The real apply fixture now
applies an unenlisted-participant ACK, a valid ACK, and an ordinary write in one
Ready batch, verifies the exact rejection plus both later successes and final
applied index, then verifies duplicate Ready replay does not regress progress.
Storage corruption and resource failures remain outside this rejection set.

The focused DATA gate passed its one matching compiled consumer test, zero
skips, failures, or leaks, actual process exit 0; the implementation artifact had
no matching test. Command from `zig/`: `zig build antfly-data-runtime-test -j1
--cache-dir .zig-cache/workload-local --global-cache-dir .zig-cache/workload-global
-- --test-filter 'data raft apply records transaction conflicts without stopping
replica progress'`. Evidence: `/tmp/workload-invalid-participant-data1.log`.

## Operator ownership inventory (verified paths)

| Entry point and helper | Transient and retained ownership | Join/cancellation boundary and evidence |
| --- | --- | --- |
| Public joined query through `distributed_join.executeSupportedJoinedPublicTableQueryRequest` and `appendJoinReadJobs` | Up to eight right-side reads use per-worker result arenas backed by one synchronized wrapper over the caller's request allocator. Parsed responses stay in those arenas; cloned hits and final output use the same caller allocator. | Each wave joins before inspecting errors, cloning hits, or releasing arenas. A late failed wave publishes no partial join. The focused join gate passed 2/2, including a 1 MiB quota against two 600 KiB worker allocations, with no leaks. |
| Relational transaction preparation through `relational_integrity_commit.prepareModeInternal` and `Builder.preloadWork` | The preparation arena uses the caller allocator. Up to eight primary-read response arenas share a synchronized wrapper over that preparation allocator; successful observations are copied into builder work only after the wave succeeds. | The worker wave joins before the request deadline/cancellation check, local-admission retry, and work publication. The API transaction gate passed 96/96, including a 1 MiB quota against two 600 KiB lookup responses and existing retry/cancellation tests, with no leaks. |
| Distributed text-stat collection through `table_reads.collectProvisionedSearchRequestTextStatsParallel` and its hosted variant | Per-shard response and decoded-field arenas share a synchronized wrapper over the caller allocator; merged stats use that same caller allocator. | Every wave joins before a cancellation check; a canceled request dispatches no later wave or merged response. The focused text-stat gate passed 2/2, including a 1 MiB caller budget against two 600 KiB worker allocations and a canceled first wave, with no leaks. |
| Distributed search through `table_reads.queryProvisionedAcrossGroupsParallel` and its hosted variant | Per-shard search-result arenas share a synchronized wrapper over the caller allocator. The final merged result uses the caller allocator. | Every wave joins before cancellation/error inspection, and cancellation is checked again before result publication. The focused read-fanout gate passed 4/4, including search with a 1 MiB budget against two 600 KiB worker allocations and a canceled first wave, with no leaks. |
| Distributed preflight through `table_reads.preflightProvisionedGroupsParallel` and its hosted variant | Per-shard preflight-summary arenas share a synchronized wrapper over the caller allocator; the merged summary uses the caller allocator. | Existing before/after-wave cancellation checks and join order remain intact. The focused read-fanout gate passed 4/4, including the existing canceled-wave test and a new 1 MiB quota test against two 600 KiB worker allocations, with no leaks. |

These are scoped ownership proofs for the named paths. The new search and
preflight quota/cancellation regressions use provisioned-local fixtures; hosted
routes share the same fanout backing and wave logic but do not yet have a
separate remote-peer saturation test. Other query operators, background helpers,
and real mixed-runtime saturation remain item-7 work.

## Current integration evidence and boundaries

- Canonical single-phase envelopes use wire version 2 and require Raft batch
  protocol 14 after the `origin/main` protocol merge. Version 1 prepares remain
  byte-compatible and require protocol 13. Versions 7–12 belong to source
  transfer, snapshots, relational transfer, and source-scope authority. Seven
  owning codec tests passed. DATA advertises version 14 and requires that floor
  for ordinary canonical mutations. The
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
  fixture rejects a peer below protocol 14 before native installation, applies the protocol-14
  barrier through the real local Raft WAL, and rechecks current membership
  before publishing backing. It then proposes an ordinary write through the
  C physical compiler/native reservation, restarts the whole DATA server with
  admission disabled and no service keys, and applies the retained WAL entry
  after a higher-term leader heartbeat. Document state and the permanent native
  progress digest match the accepted entry. BEGIN selects protocol 14, while
  prepare requires protocol 13. Peer votes/acknowledgements are delivered explicitly
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
- Recovery owner acquisition now uses the bounded point catalog projection,
  translates the original native deadline into the catalog clock, and borrows
  only an exact resident owner. It never falls back to an administrative scan
  or opens/closes native storage on the caller's recovery budget. A fixed,
  deduplicated queue owns cold-owner hints; DATA's existing shutdown-owned
  lifecycle lane rotates failed groups and restores owners independently of
  request admission, registration, and unrelated schema retry backoff. Resolve,
  ACK, and raw local status share this path. DATA retains the same deadline
  when reading owner status after the real Raft read barrier.
  The owning compiled SourceOwner gate passed four tests, and DATA passed
  three consumer tests, all with zero skips, failures or leaks. They verify
  shifted catalog clocks, no legacy projection fallback, cancellation before C
  submission, cold status/ACK followed by lifecycle restoration and successful
  retry, bounded queue overflow/deduplication/shutdown, actual DATA job
  scheduling, and status expiry after the quorum barrier. Existing canonical
  proposal and disabled-admission WAL restart coverage also passed. Evidence:
  `/tmp/workload-recovery-owner-bounded2.log` (actual exit0) and
  `/tmp/workload-recovery-owner-data2.log` (actual exit0).
  This is metadata-routed recovery: offline transaction-control recovery still
  requires the separately owned durable control proof. Catalog point callbacks
  accept a deadline but no cancellation token; cancellation is checked before
  and after the bounded fetch and before mutation submission. Fresh production
  activation remains disabled.
- Public batch name resolution now translates its request-owned `std.Io` awake
  deadline before comparing it with the metadata service's platform monotonic
  ReadIndex timeout. Both embedded and HTTP metadata services retain the
  original clock for cancellation checks. A genuine pre-write metadata timeout
  returns HTTP 503 and the batch client classifies it as retryable availability,
  while unknown post-admission outcomes retain their separate contract. The
  focused public API smoke and batch-client tests passed 2/2; the direct HTTP
  metadata clock and catalog status tests passed 2/2. The metadata service gate
  passed 131/131, though its fixed filter did not include the new direct test.
- After merging `origin/main`, native control-owner guard publication stages a
  complete guard before atomic rename and directory sync. Restoration validates
  all four guard records against trusted local identity, rejects duplicate
  transaction/output owners, and rebuilds the aggregate capacity certificate.
  The owning durable-completion gate passed 42/42. This restores local guard
  evidence, but the pool still does not own control capacity at BEGIN or retain
  decision/ACK resources through final retirement; item 1 remains open.
- Bounded worker batches now reject same-executor reentry before enqueueing,
  preventing a one-worker protected lane from waiting on itself. The focused
  five-test lane passed. Process-wide connection, memory, nested I/O, and storage
  pressure still require item 6 qualification.
- Native Data Raft leader forwarding now has a separate six-worker executor
  inside the existing 32-worker forwarding budget. Four general read-forwarding
  graphs can run concurrently; one Data Raft graph remains available even when
  those reads and their retiring tasks occupy every general worker. Four
  focused runtime cases passed for saturation, retirement, shutdown, and a
  reduced 12-worker configuration. Borrowed schedulers enforce logical quotas
  but own their physical isolation; aggregate mixed-resource qualification
  and the throughput effect of reserving this grant remain open.
- Data Raft/recovery snapshot HTTP now uses a separate six-worker executor,
  independent of general API work and Data Raft leader forwarding. The default
  252-worker aggregate moves six workers from API (16 to 10) into this lane;
  custom limits account for it explicitly. A real metadata head fetch and a
  nonexpired DataServer snapshot call progressed with general API workers and
  Raft forwarding saturated; focused DATA 1/1 and runtime 3/3 tests passed.
  Borrowed schedulers must provide physical isolation themselves. The reduced
  API capacity and combined connection/memory/storage pressure still need
  throughput and overload qualification before policy activation.
- The merged metadata codec now preserves relational retirement metadata,
  accepts both historical and current table-storage extensions on restore,
  and encodes current dense-storage migration admissions behind topology
  protocol 13. Secret-bearing snapshot version 2 is emitted only after durable
  protocol-13 activation; legacy snapshot installation rejects a target with
  secrets, and a legacy snapshot build refuses to omit source secrets. The
  full metadata gate exited successfully; visible lane summaries were 290,
  221, and 144 passing tests, with no failures or leaks.
  API transaction qualification then passed 93/93 with no failures or leaks:
  an unproved peer 404 cannot certify row absence, and a proved read-index miss
  is accepted only after a key-specific linearizable metadata route check.
  That check is paid on misses, not successful lookups.
  Metadata membership changes still lack a durable decoder-capability proof for
  replacement peers; a v13 snapshot fails closed on an older decoder. An empty
  target cannot prove that a historical v1 source snapshot omitted no secrets.
  Fencing joins and adding snapshot provenance before production use remain
  open for item 4.
- The post-merge storage-owner gate passed 53/53 with no failures or leaks.
  Owner open again binds native source authority, installs scoped hidden
  restore bootstrap, and resumes source-pin recovery before accepting work.
  DATA completion versions 13/14 no longer collide with source formats 7–12;
  the focused DATA runtime gate passed 2/2 with those version checks. The
  owner tests also allow background index repair to complete before the
  backup assertion, while still requiring quiescent final backup.
- An explicitly enabled internal pool path now stages a fresh BEGIN owner
  before accepted-sidecar acknowledgment: it certifies aggregate control
  obligations, retains separate publication and WAL resources, and publishes
  the immutable guard. The real DB fixture accepts/applies BEGIN, finds the
  retained resources and durable guard, then verifies restart fails closed
  until runnable owner restoration exists. The durable-completion gate passed
  43/43 with no failures or leaks. The default path and its successful BEGIN
  restart remain unchanged; production activation is still disabled. Decision
  and ACK routing, guard retirement, preowned output descriptors, and runnable
  restoration remain open.
- Rust SDK query retries now rewrite bounded top-level JSON/NDJSON `timeout_ms`
  values to the original operation's remaining budget while preserving other
  bytes and rejecting ambiguous duplicate keys from retry qualification.
  The four focused Rust retry tests passed, including a delayed HTTP retry.
- The post-merge owner-source gate passed 34/34 with no failures or leaks.
  Scoped BEGIN validates the scoped local participant; resident owner reuse
  compares the catalog restore identity; scoped status waits for read safety
  and resolves the exact restore descriptor under the request context. A
  blocked read-safety barrier itself does not accept a deadline or cancellation
  token, although the callback rejects late status after it returns. Bounded
  barrier occupancy remains open for item 8.
