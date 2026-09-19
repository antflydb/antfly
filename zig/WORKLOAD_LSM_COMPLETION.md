# Internal LSM completion allocation ownership

## Allocator provenance prerequisite

This stage preserves allocator provenance when an in-memory publication uses
an allocator different from the backend allocator. Persistent AVL nodes free
through their originating allocator, including recursive and incremental
retirement after the last reader releases a root. The spare-node vector records
its allocator separately. Switching its allocation domain first retires the
old unused pool; a transferred spare vector therefore cannot contain nodes
from another allocator domain.

The prerequisite kept the existing shared memory `Account` unchanged and alive through
the same generation and allocation references. Mixed publication allocators do
not create new accounts or exclude any allocations from ordinary accounting.
The allocator contexts themselves must outlive every allocation made through
them. This stage does not supply a retained allocator-context owner.

On the tested 64-bit aarch64 target, the memtable `OrderedIndex.Node` grows from
120 to 136 bytes: 16 bytes, or 13.3% of the node structure, excluding its document
payload. `OrderedIndex` grows from 40 to 64 bytes. Existing `@sizeOf`-based
accounting and preparation estimates include those increases. This affects
default nodes too; a future compact domain owner could reduce the overhead,
but no throughput or memory performance qualification is claimed here.

Validation uses the actual in-memory LSM backend for alternate candidate
allocation followed by ordinary writes, retained old/new snapshots, final
retirement, and an allocation-failure sweep. The state tests additionally
exercise incremental reclamation, shared-account conservation, and allocator
domain changes in a spare pool. Final focused results: 25 state/backend-type
tests and two actual LSM backend tests in each of Debug and ReleaseSafe.

## Sealed memory-only point batches

`Backend.prepareCompletionPointBatch(namespace, operations, limits)` is an
internal, callable backend path. It copies bounded point puts/tombstones and
prepares the complete COW successor before returning a heap-stable ticket.
The caller supplies operation, encoded-input and physical-allocation ceilings.
The physical ceiling includes the owner, ticket, copied input, nodes, spare
vectors, shared entry headers and preparation overlap. An insufficient ceiling
or backing allocation failure aborts preparation without publishing anything;
it is not an estimate promising that arbitrary input will fit. Range/bulk
operations are absent from the API. Persistent roots and storage-backed modes
are rejected before reservation. There is no public configuration or transaction
policy activation.

The allocator stages exact requested buffer bytes from `CompletionCredit`
before calling its backing allocator. Resizing uses allocate-copy-free, so the
overlap is reserved as well. The owner metadata is itself reserved before
allocation. Each physical buffer retains the owner, including buffers retained
by readers after the submitting ticket is destroyed. The manager and backing
allocator must outlive those buffers. Allocator-private overhead, resource
manager identity-ledger storage and whole-process RSS are outside this byte
accounting contract.

The same shared `Account` follows old and new roots. Its single atomic
`ordinary_bytes` counter contains only buffers charged by the ordinary backend
observer. The trusted prepaid allocator's buffers retain the account but are
charged solely by their separate credit observer. This preserves old ordinary
nodes without counting the prepaid subset twice, and avoids subtracting two
independently changing counters. This change adds no `Account` size overhead.
The backend has one additional pending-ticket list pointer.

Outstanding tickets participate in the backend's ordinary accounting walk,
including their pinned original root. A competing write or mutable rotation
makes publication fail as stale; the original root remains charged until
ticket destruction. Successful publication checks the pinned root identity,
marks credit published, and swaps the prepared root without allocation. It
can use already-owned capacity after aggregate or slice limits are reduced.
Ticket destruction releases unused credit; subsequent physical frees release
their exact charges. Destruction retains immutable account handles on the
backend list while destructive tree cursors run outside the writer lock in
64-node slices. This matters when a tiny stale batch is the final pin of a
large prior generation. Close waits for outstanding tickets just as for readers;
callers must destroy tickets before synchronously closing their backend.

Actual backend tests cover saturation and reduced limits, failed backing
allocation at publication, retained old/new readers across ordinary writes,
mutable rotation, stale and persistent-path rejection, insufficient physical
capacity, 64 preparation-allocation failure positions, off-lock retirement of
a 2,001-key stale generation, and close waiting for both ticket and reader.
They validate
resource-manager byte ownership, not process memory or throughput.

The final focused suites passed 27 tests each in Debug and ReleaseSafe, with
zero leaks: seven actual backend tests and twenty existing state regressions.
Run from `zig/` with `zig build lsm-backend-test -j1` (add
`-Doptimize=ReleaseSafe` for that mode), followed by
`-- --test-filter 'workload admission lsm' --test-filter 'storage.lsm_backend.state.test'`.

This is not durable transaction completion. No WAL or manifest admission is
bypassed. Durable pre-prepare physical-plan certificates, restart reservations,
replay/backlog capacity, and protected WAL/manifest/flush resources remain
requirements before public transaction policy can claim mandatory completion.

## One-shot native WAL point commits

`Backend.applyCompletionPointBatchWithWal(namespace, operations, limits)` now
provides an internal native persistent path. It accepts bounded point puts and
tombstones on a writable, backend-owned native store with WAL enabled and a
ResourceManager. Custom providers, memory-only stores, read-only handles and
active bulk sessions fail closed. No public configuration invokes it.

Before sealing, it applies ordinary aggregate-memory, manifest/backlog and WAL
retention admission, including any pressure-driven flush. It then owns the
complete copied input, COW successor, encoded WAL record, prepared native paths,
two native FD permits, WAL serialization, and a backend lifecycle pin. COW is
built after admission that can release the backend mutex. Observer metadata is
pinned before the irreversible boundary; these pins grant no byte credit.
WAL growth is separately admitted into a temporary observer and atomically
transferred into the backend retention observer after the attempt. The same
bytes are never released and reacquired or counted twice. This follows the
existing ResourceManager treatment of WAL retention; it does not reserve
filesystem free space or guarantee successful device I/O.

The seal forbids further allocation in the physical owner. Native append and
publication keep the backend and WAL locks. Publication is a prepared root
swap with no subsequent stale-state validation. A failed or uncertain storage
attempt fences the backend, preserves manifest debt and retains conservative
WAL-growth accounting until recovery or an authoritative retention snapshot.
The original error is returned, with no implicit resend. Fully written but
unpublished records remain recoverable through ordinary backend reopen.

The operation marks maintenance deadlines and debt without starting new
allocating maintenance. Its caller must continue the normal maintenance loop;
this API does not finish SST construction or manifest publication. Native
working buffers and unused credit retire on return, while shared tree nodes
remain charged to their heap-stable owner through the final reader. Ticket
reclamation runs in bounded slices outside the writer lock, with a backend pin
held through cleanup. Close waits for that ownership.

Native tests cover allocation/FD/admission exhaustion after sealing, lower
limits, pinned readers, WAL rotation, pressure checkpoints before COW, manifest
backlog and FD rejection, preparation allocation failures, partial and fully
synced unknown outcomes, failure between append and publication, writable
recovery, and close overlapping the sealed operation. These are local resource
ownership and storage correctness tests, not throughput qualification.

This remains a single-call internal mechanism. It supplies no durable
pre-prepare transaction certificate, retained cross-request ticket, restart
reservation reconstruction, or protected SST/manifest completion guarantee.
Those boundaries must be implemented before enabling a public transaction
policy that promises mandatory completion.

The native stage's focused suite passed 21 tests in each of Debug and
ReleaseSafe, with zero failures, skips or leaks. From `zig/`, run
`zig build lsm-backend-test -j1` (or add `-Doptimize=ReleaseSafe`), followed by
`-- --test-filter 'workload admission lsm' --test-filter 'observer metadata pin' --test-filter 'lsm WAL uncertainty' --test-filter 'lsm backend write stats separate'`.

## Durable transaction slot requirements

The requirements below preceded the implementation described in the final section.
They are not a public activation instruction.
The accepted scheduling design requires completion resources and durable recovery
information before an irrevocable vote or decision. Calling the one-shot helper
at resolution does not meet that requirement: its memory, manifest, WAL and FD
admission can still fail after a participant has voted prepared. Its COW candidate
also describes the current mutable root; retaining that candidate through other
writes does not make it a valid successor later or after restart.

The first target is a transaction with one participant and one overwrite of an
existing live document, with an inline value, complete identity mappings and a
pinned schema. It excludes inserts, resurrection, deletes, relational storage,
payload externalization, secondary or derived indexes, generated/graph/artifact
work, child dispatch and HA outboxes. Unsupported profiles must be rejected
before `prepared=true`. The existing identity classifier can prove an ordinary
overwrite needs no identity mutations, but schema, identity and topology fencing
must preserve that proof through resolution. This profile is not yet certified.

The integration boundary is `TxnManager.writeIntentsExtraBatch`: its atomic
batch publishes `prepared=true` alongside intents, membership, locks, admission
and the schema lease. `DB.prepareTransactionRows` currently prepares relational
row encoding, not a complete physical plan. Resolution enters the general DB
batch expander again. Even the proposed document overwrite needs document and
TTL writes, transaction status, intent/member/lock/admission/schema cleanup,
completion accounting and derived replay. Transaction resolution explicitly
cannot elide replay; `docstore.emitReplayMutations` adds sequence metadata and
replay entries. Commit version, replay sequence, Raft markers and shared ledger
values cannot be frozen to their earlier values. The persisted plan must enumerate
operations plus bounded late-binding fields and encoding workspace. It must never
replay a stale shared-counter value over intervening transactions.

Implement this as one internal vertical slice: a single durable completion slot
holding a private bounded delta generation, rather than rebasing against an
arbitrarily changed foreground tree. Before publishing the prepare batch, reserve
the slot's generation/publication metadata, input and late-binding buffers, WAL
encoding and retained growth, native FD bundle, and one bounded SST/manifest drain
workspace. Persist a versioned descriptor containing the operation shape, bounds,
transaction identity, intent revision and fencing information in that same batch.
Ordinary writers must not consume the reserved slot or drain capacity. Resolution
publishes the private generation without new ordinary allocation or FD admission;
reader-held state keeps its physical charge until its last reference disappears.
The delta must participate in existing read precedence and atomic publication,
including writes that occurred while the transaction was prepared.

This requires actual SST and manifest support beyond `NativeWalCompletionIo`,
which currently serves only WAL operations. Existing manifest wire/backlog credit
is not preallocated table-builder memory, output buffers, run-directory nodes or
file capacity. WAL reservation must cover prepare, decision, application and
cleanup records, segment/control overhead and retention until a durable checkpoint;
it cannot be released merely because one append succeeded. Existing I/O errors
still produce durable uncertainty. Resource accounting does not reserve filesystem
free space or promise a device operation will succeed.

Startup restoration belongs in this same slice. Before foreground admission,
read authoritative pending-slot records and restore their bounded memory,
publication/drain slots, WAL debt and FD capacity, including undecided prepared
votes. Smaller configured limits must not silently discard old obligations or
send them through ordinary admission. `reconcileCompletionAdmission` currently
reconstructs logical count/byte debt only; its records are not physical slot
certificates. Preserve the original transaction identity and existing terminal,
participant-acknowledgement and outbox retirement conditions. Replicated activation
must establish compatible capacity before accepting obligations; a committed Raft
apply cannot acquire a new node-local admission veto.

Acceptance for this stage requires actual prepare and resolution paths:

- Prepare, perform unrelated writes, reduce ordinary memory and FD limits, then
  resolve using only the reserved slot and drain resources.
- Crash while prepared and after a durable decision but before publication;
  restore ownership before traffic and complete under the original identity.
- Hold an old reader through resolution and retirement, proving its physical
  memory stays charged and ordinary writes cannot consume completion resources.
- Exercise allocation, storage and publication failures without losing debt,
  repeating a mutation under a new identity, or reporting uncertain work aborted.
- Reject every unsupported profile before the prepare vote, and verify exact
  read/reopen results plus atomic metadata cleanup for both commit and abort.

Keep this path internal until those proofs pass. A physical-plan encoder alone,
or a successful one-shot WAL test, does not establish mandatory transaction
completion or full performance qualification.

### Implemented inspection and shared accounting foundation

`completion_mutations.Plan` owns bounded copied mutation slots and bytes. The
transaction manager's internal resolution inspection uses the real decision and
cleanup paths, recomputes current completion accounting, and emits DocStore's
replay expansion into those slots. It aborts its backend batch without publishing
changes or resolution traces. Unsupported keyspaces fail closed because DocStore
may expand them into relational or payload-ownership operations. Tests compare
the entire resulting physical store for commit and abort, including binary keys,
four replay lanes, metadata cleanup and terminal retries after newer writes.

This is an observation at resolution time, not a pre-prepare compiler. Its shared
counter and sequence values become stale after inspection. It has no apply API,
persisted descriptor or reserved backend resources; scratch used to derive the
operations may still allocate. The separately factored replay emitter preserves
the existing malformed/decode-failure fallback, so bounds must charge the values
actually emitted, including full-payload fallback copies.

The private DB profile inspector recognizes a still narrower initial candidate:
one overwrite in an explicitly schemaless, local-only document table, with a
bounded scalar JSON value and complete live identity mappings. It records root,
schema, index, range and identity observations and rejects unsupported configured
work. There is no production caller or public activation. It retains no lease;
observing eligibility before prepare does not fence later schema, index, identity
or topology changes through retirement.

Completion-ledger updates now explicitly acquire a backend-shared writer gate
through commit or abort. Ordinary native LSM batches serialize publication but
not the whole read/modify/write operation, so independent transaction managers
could previously lose shared accounting updates. Native LSM stores expose
their shared gate; LMDB exposes its intrinsic writer serialization. The memory
backend is excluded because ordinary commits replace its entire snapshot. Nested
DocStore adapters preserve that capability without acquiring the same gate twice.
Unknown providers fail closed. Ordinary batches remain concurrent, and this gate
does not establish cancellation, bounded progress or physical completion credit.

At this foundation stage, durable slot integration remained open. The following
section describes the implemented internal integration and its limits.

## Durable local slot implementation

The next integration stage implements `DB.prepareDurableCompletion` and
`DB.resolveDurableCompletion` behind the internal
`OpenOptions.allow_local_durable_completion` opt-in. The default is false; no
HTTP, Cloud, Raft, or public transaction configuration enables it. The supported
profile is one local transaction overwriting one existing schemaless document
with a JSON object containing only scalar fields, at most 64 KiB of encoded data,
a key of at most 1 KiB, and complete live identity mappings. An exact, bounded
inspection of the target's stored records rejects attached artifacts;
schema, indexes, relational storage, external payloads, named participants and
HA outboxes remain excluded. Unsupported work fails before the prepare vote.
These profile ceilings also remain subject to the compiled plan and slot limits;
a document below the ceiling can still exceed the available reservation.

The compiler runs the real transaction prepare and resolution machinery against
an isolated, bounded overlay. It persists copied commit and abort mutations with
explicit timestamp, replay-sequence and shared-ledger bindings. Resolution reads
the current decision, intent revision and shared accounting under the backend's
writer gate. It never overwrites intervening ledger changes with sampled values.
Derived replay admission also retains a vector slot across unrelated writes;
byte credit alone was insufficient to guarantee its eventual publication.

Before the actual atomic prepare batch, the native backend obtains two physical
slabs (32 MiB scratch and 4 MiB publication), an 8 MiB future-WAL accounting
contribution, two retained native FD permits, exact permitted file paths, and
observer metadata pins. Initial reservation requires four usable transient FD
slots after lifetime locks and startup headroom: two retained for completion and
two for the ordinary WAL append that publishes prepare. Acquisition checks both
pairs atomically before retaining one, so concurrent durable reservations cannot
consume each other's remaining prepare capacity. Slab/context overhead is
charged in addition to their payload capacities. The encoded descriptor fixes the
resource shape and is bounded to 256 operations across both outcomes and 256 KiB
total. One protected SST is bounded to 16 MiB. These conservative internal limits
are correctness bounds, not qualified product defaults; they reserve neither filesystem free
space nor successful device I/O.

Ordinary writes retain their own admission. While the slot lives, they cannot
flush or rotate the manifest underneath it, alter its control records, or exceed
the bounded foreground envelope: 256 KiB current mutable data, 2,048 cumulative
appended entries, 128 records and 1 MiB cumulative WAL. Repeated overwrites count
against these cumulative bounds. Structural transition guards cover schema,
indexes, identity/range, storage migration, runtime hooks and generation exchange.
A guard on both live and incoming generation roots prevents offline publication
from bypassing this exclusion.

Resolution binds a private delta using already owned memory, appends the atomic
completion WAL record, and merges that delta with the current bounded mutable
into one protected SST. A synced manifest edit publishes both completion and
intervening writes. Run metadata and pinned readers retain their originating slab
until their final reference disappears. Ordinary allocation failure, lower memory
limits and lower FD limits cannot revoke the retained completion bundle. Device
errors or an uncertain publication retain the durable handoff and fence mutation
until recovery.

`completion-slot.guard` is a checked copy of the descriptor, synced before
prepare. The real prepare batch stores the descriptor alongside intents and
`prepared=true`. The completion WAL record atomically retires that descriptor
and writes an applied marker. Startup reconstructs physical ownership from the
guard before WAL replay or foreground admission. A prepared slot remains pending;
an applied marker causes a protected flush of the already replayed state, never
another application of the templates. If the manifest already proves completion,
restart only finishes checkpoint/guard cleanup; repeated crashes do not create
additional SSTs. A torn prepare with no complete descriptor
can retire only after its valid prefix is durably preserved. Tail repair requires
exact evidence that the discarded bytes belong to the current segment and cannot
truncate a different or earlier damaged segment. A malformed guard
fails closed and is retained.

Retirement requires the SST and manifest, a protected WAL reset, and durable
guard removal. Reset first excludes old segments with a durable checkpoint cut,
then truncates the first segment and deletes the old extent before reusing its
numbering. Recovery finishes an interrupted cut before accepting new writes,
including a cut made before the guard was created. This avoids replaying an old
WAL prefix over a completed transaction or appending onto orphaned old segments.

Restoration uses `Backend.openInto` at a stable address, as the production
backend owner already does. The value-returning helper rejects guard-bearing
roots because moving a backend would invalidate pinned observer identities.
Insufficient restart capacity fails opening without dropping the guard or its
obligation. Pending slots close through their durable handoff rather than an
ordinary flush after releasing their protected resources.

A pending decision has no age-based abort. Uncertain completion requires reopen
before mutation resumes. The profile binds the canonical root and durable
identity/value state; moving or copying a root is not an authorized transfer of
the obligation. Physical completion covers the protected storage boundary;
ordinary postcommit visibility callbacks retain their existing behavior.

The broader mandatory-completion policy remains disabled. Multi-participant
coordination, replicated admission compatibility, other document/index profiles,
and optimized workload qualification remain separate work.

## Current replicated-pool release gates

The native four-cell pool now has prepaid checkpoint/rearm maintenance and
bounded retention of two obsolete file generations. Its focused tests cover
repeated maintenance under denied ordinary allocation/FD admission, idle and
65-run restoration, publication-boundary crash hooks, and real pinned readers.
These mechanisms do not yet qualify production pre-ACK completion admission.
The following gates remain:

- **Exact record shape.** Canonical prepare, both outcome templates, and native
  descriptor/receipt/progress records must fit the maintenance record limit,
  including the 13-byte SST header, namespace, key, and value. A 256 KiB encoded
  descriptor alone does not fit a 256 KiB SST-record limit. The exact admission
  check and boundary tests passed the owning Debug gate (7/7 total, including
  pool and generation-guard regressions; no failures or leaks).
- **Publication generations and remaining spans (implemented).**
  Installation physically acquires three disjoint cell generations, each with
  one contiguous 8 MiB span per configured cell, and four separate 8 MiB
  metadata generations. Rearm selects an entirely empty generation; reader
  references retain their original allocation domain. Admission bounds actual
  writer/directory payloads, persistent tree edits, namespace/key/path copies,
  allocator headers and alignment before accepting a future output shape.
  It also checks each cell's remaining private span after envelope and owner
  preparation against the remaining native prepare/completion allocations.
  A failed partial preparation spends that cell until maintenance rearms it.
  These domains charge about 104 MiB fixed memory plus 24 MiB per cell, with
  exact allocator/domain overhead added, and a separate 8 MiB WAL credit per
  cell. Capacity four therefore needs about 200 MiB physical memory and 32 MiB
  WAL credit before installation; global resource admission may reject it.
  This covers four prepare/outcome cells, not the separate decision/ack control
  reservations. The maintenance aggregate scratch certificate is described
  below; the separate completion-drain scratch proof remains outstanding. The owning
  Debug gate passed 14/14 with zero skips, failures, or leaks, including four
  256-operation prepare/outcome plans under denied ordinary memory/FD admission,
  retained mutable readers, actual 64-run binary-bound metadata publication,
  generation exhaustion/reuse, low-budget installation rejection, restart and
  physical output-file splitting. Output limits use the native 512 MiB file
  allowance, rather than the larger generic encoder format maximum.
- **Fixed block workspaces and single-run drain metadata (implemented stage).**
  Maintenance cursors retain two maximum-block buffers per input and borrow one
  metadata buffer during initialization; block advancement no longer allocates.
  Direct prefix encoding and two-buffer decoding preserve v11 bytes, including
  prefix+Snappy and binary keys larger than a normal block. The bounded encoder
  helper reserves precise arrays and a caller-owned compression workspace, and
  rejects its metadata ceiling before flushing. Maintenance now integrates
  that helper using the aggregate partition described below. Cohort admission
  also bounds its own future metadata below
  the single protected SST's input-metadata limit. Existing immutable database
  size is excluded from that cohort-only check. The owning Debug gate passed
  18/18 with zero skips, failures, or leaks: byte differential/reuse tests,
  actual oversized-key SST metadata, pre-sidecar cumulative-cohort rejection,
  and all prior generation/capacity/restart regressions.
- **Aggregate maintenance workspace (implemented stage).** The pre-ACK check
  sums current/future cursor buffers and compact indexes, one input metadata
  buffer, exact encoder arrays, compression bytes, path/buffer overhead, and
  all simultaneously retained output metadata, including allocator headers and
  alignment. The sum must fit the physically owned 32 MiB compiler domain.
  Maintenance borrows that empty domain exclusively, retains fixed input
  buffers, and resets one fixed writer arena between outputs. Its production
  builder checks the same format-cost output certificate before output I/O;
  no per-block or per-record allocation can fragment the writer workspace.
  Completed output metadata owns separate copies and cannot retain arena
  slices. After construction frees the writer/cursor buffers, checkpointing
  needs one 64 KiB buffer plus bounded paths; the released writer span alone
  exceeds that requirement. The owning Debug gate passed 20/20, zero skips,
  failures, or leaks, including a sum-of-individually-valid-buffers rejection
  and actual split SST construction with exactly the certified scratch bytes
  while backing allocation and ordinary FD admission were disabled. This
  certificate covers no-debt maintenance; Slot.drain's shared scratch and
  retained canonical-prepare payloads still require their separate proof.
- **Fixed pooled completion drain (implemented stage).** Pooled Slot drains
  merge their two sorted memory snapshots using the fixed maintenance encoder
  in an exclusive compiler workspace. Admission checks the entire cohort's
  possible mutable data, metadata and framing against the single 16 MiB output
  and configured record/metadata limits. The output remains L0 with its reserved
  visibility ID. Startup releases lookup temporaries before borrowing this
  writer workspace. Canonical prepare copies SharedEntry payloads into the
  already-accounted cell publication span, so reader-held data no longer pins
  temporary incoming scratch. The owning Debug gate passed 22/22 with zero
  skips, failures, or leaks, including exact-budget binary-key/tombstone merge,
  file-cap rejection, payload provenance after scratch release, and prior
  four-cell/restart/generation tests. This does not cover the standalone Slot's
  generic writer or shared scratch used by WAL/delta/replay work; those remain
  explicit capacity gates.
- **Mandatory workspace borrowing (implemented).** Drains, no-debt maintenance,
  and bounded resolution parsing borrow the compiler domain through a lexical
  completion scope with no escaping epoch token and no epoch increment. Normal
  admission/qualification still use checked monotonic epochs. Normal tokens
  cannot allocate or release during a completion scope, and nested borrowers
  are rejected. Callback cleanup must leave the domain empty on both success
  and failure; otherwise it stays unavailable. The owning Debug gate passed
  28/28 with zero skips, failures, or leaks. It includes stale/nested/error
  scope checks and four maximum accepted plans completing commit/abort plus
  maintenance after normal epochs reach u64 maximum, while new admission stays
  closed. Existing accepted completion does not depend on restarting to reset
  this process-local counter.
- **Whole pooled operation workspace (implemented).** Canonical prepare,
  one-phase mutation, outcome binding/drain, and replay/manifest/checkpoint
  cleanup now use one exclusive empty compiler domain for the entire operation.
  The pre-ACK certificate sums temporary ordered-tree allocation, binding
  copies, WAL framing, fixed SST writer buffers, exact manifest encoding, and
  checkpoint paths, including allocator headers, alignment and split tails.
  Its production maxima are 256 public plus four private records, a 512 KiB
  canonical envelope, a 256 KiB descriptor, null or `docs` namespace, and 151
  bounded path allocations (four append, eleven reset controls, and at most
  136 retired WAL segments). Both cumulative allocation and largest contiguous
  request must fit the installed 32 MiB domain. Publication copies retain
  their own allocator, so no operation workspace escapes into mutable readers.
  A one-allocation v11 manifest encoder matches the existing encoder byte for
  byte. Journal admission reserves both maximum record-sized key bounds for
  every remaining cohort output; restoration does not charge already-published
  outputs twice, and an idle full journal requires protected maintenance before
  fresh qualification. The owning Debug gate passed 32/32, zero skips, failures,
  or leaks (`/tmp/workload-native-whole-operation1.log`). It completes four
  maximum plans with exactly the computed compiler capacity while nearly all
  independent replay scratch is held, backing/ordinary FD allocation is denied,
  and normal borrowing epochs are exhausted. This closes the pooled healthy
  operation scratch gap above. The standalone generic writer and standalone
  journal reservation remain outside this pooled certificate.
- **Pooled restart allocation certificate (implemented).** Installation also
  checks the complete replay allocation bound against its separately retained
  32 MiB domain. Startup must begin with an empty ordered mutable and an empty
  replay domain. Without readers, existing AVL paths are uniquely owned and
  mutate in place: the typed bound counts inserted leaves, maximum spares,
  vector/account growth, every payload version, and allocator overhead. A fixed
  caller-owned pending buffer covers the entire permitted retained WAL and is
  never grown, shrunk or freed by the parser. Retention measurement and replay
  cover the same legacy file and checkpoint-to-current segment range under one
  WAL lock. The bound includes 423 maximum-length path allocations, 4,128
  legitimate foreground/prepare/outcome entries plus the post-decode rejection
  entry, and wire/descriptor sizes plus both phases' explicit native-record and
  WAL framing costs. The owning Debug gate passed 43/43 with zero skips,
  failures or leaks (`/tmp/workload-native-replay-workspace2.log`). It includes
  4,128 distinct records and replacement/tombstone versions crossing chunk
  boundaries on exactly certified backing, denied backing allocation, explicit
  undersized-buffer rejection, existing crash/reader/pool tests, and ordinary
  WAL replay regressions. This proves bounded restoration for the current
  pooled profile; it does not provide the still-separate retained transaction
  control owner or broaden standalone completion guarantees.
- **Output-count and future-frontier certificate (implemented).** Qualification
  scans CRC-checked physical records and keeps additive encoded-data, metadata,
  and block costs. Admission adds canonical operations, both possible outcomes,
  and private records without refunding overwritten/deleted data or retired
  cells. The v11 encoder's greedy splitting cost proves at most 64 outputs;
  adjacent-block packing bounds sequential-index memory and live block buffers.
  Both the current-input-plus-growth frontier and future replacement frontier
  must fit 16 MiB. This is a format-cost limit, not a fixed database byte ceiling.
  Maintenance starts from the separate empty compiler workspace, so retained
  replay nodes cannot consume its initial span. Maintenance allocator and
  result costs are included in the aggregate workspace check above.
- **Counter headroom (implemented).** Before acceptance, checked native bounds
  cover cohort manifest increments, the next checkpoint/output identifiers,
  and bounded WAL rotations. Authoritative and proposed replay-next/summary
  values retain cohort headroom, including the largest actual admitted credit
  size. Ordinary counter puts receive the same check. The owning Debug gate
  passed 10/10 with zero leaks: arithmetic boundaries, actual SST encoder
  comparisons, pre-sidecar rejection, restart/crash/reader regressions, and
  maintenance under denied ordinary memory/FD admission.
- **Actual I/O fault coverage.** Maintenance crash hooks verify the selected
  publication boundaries. Partial writes, fsync failures, and failures after
  pointer rename still need direct injected-I/O coverage, including durable
  obsolete-path ownership, orphan-output recovery, and unchanged data/progress.

The physical pool can guarantee retained memory and participation in native FD
admission; lowering those ordinary limits cannot revoke already owned capacity.
It does not reserve filesystem free space, device success, process-wide OS file
handles, or completion time. Those I/O failures must retain uncertain ownership
and fence further mutation. Pinned readers may delay rearm of a fully completed
cohort; they must never cause resources for an already accepted obligation to be
released or replaced by a fresh ordinary admission request.
