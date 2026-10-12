# Ordered artifact activation gate: implementation log

> Relocated verbatim from `docs/sql-extraction.md` (lines 2427–3766) at commit 3ce5aff4ea on 2026-10-12. Durable decisions were folded into [Ordered artifact publication](../../ordered-artifacts.md) before the move. This is a historical implementation log; it describes work at the time it was performed, and its counts and test totals are not current guarantees.

### Ordered artifact activation gate

The expanded asynchronous-artifact protocol is still **not advertised**. The
current implementation must not be treated as full feature activation. The
following mechanisms are installed behind that gate:

- Compact, owned publication commands and bounded replicated upload chunks.
  Finalization authenticates chunks outside the apply lock, then rechecks the
  manifest root and original begin index in the final transaction. Effects,
  provenance, semantic receipts, terminal transport acknowledgement, quota
  release, and chunk retirement share that transaction. Standby replay carries
  the small upload control, not another copy of the assembled publication.
- Transport acknowledgement storage is a fixed 1,024-entry durable ring, with
  constant point-addressed retirement per decision. Eviction removes only a
  retransmission cache entry; the durable semantic receipt/guard path remains
  authoritative. Active uploads separately retain their count/byte quotas.
- Large dispatch jobs yield after two chunks, retaining one queue admission,
  the original owned command, and their accepted chunk position. Scheduler
  pressure preserves progress; shutdown drains ownership. The compact command
  is not decoded or hashed again for each scheduling slice.
- Producer admission leaves two job slots and 16 MiB for activation controls
  in both the local queue and durable upload store (within their existing
  total limits). The upload root authenticates its control class; assembly
  verifies that class against the decoded command. Restart reconstructs the
  same quota partition, and retirement releases the exact class's reservation.
- Fully staged uploads survive loss of their finalize job. Each chunk updates
  an incarnation-bound, checksummed receipt bitmap in the same transaction as
  its bytes. Recovery examines at most eight manifest/bitmap pairs, never the
  payloads, and queues a 133-byte recovery hint through reserved control capacity.
  It releases the snapshot before dispatch, retains its cursor on refusal, and
  fairly wraps ready uploads at the idle maintenance cadence. Finalization still
  authenticates the complete command; a stale begin-index hint cannot consume a
  pruned/recreated upload. Terminal retirement and pruning also remove the bitmap.
- Incomplete uploads have a bounded idle detector (eight observations, five
  minutes without chunk progress). Its clock chooses proposals only. Ordered
  abandonment compares the incarnation, root and authenticated progress in the
  writer transaction, refusing retirement after a racing chunk or completion.
  Restart and clock regression restart the grace period; ready retries alternate
  with idle retirement so pending coverage cannot strand upload capacity. No
  semantic receipt, terminal success, or producer completion is manufactured.
  Lost producer output still requires the required-stream execution driver to
  regenerate it from the pending exact-input obligation.
- Root-producer discovery now appends immutable-catalog requests to the existing
  replay journal, atomically with a checksummed per-obligation dispatch sequence.
  Scheduling leaves semantic obligations pending. Primary/dependency re-dirtying
  resets dispatch even at the same primary position; stale catalog/input guards
  cannot consume work. Bounded, budgeted pages release their read snapshot before
  admission, keep a fair local cursor, and back off after refusal or a full sweep.
  Reopen tests distinguish durable dispatch from completion. Scoped downstream
  enumeration and completion callbacks still belong to the gate below.
- Pending root streams now have bounded, proof-aware retry sweeps independent of
  transport lifetime. Each receiver verifies the immutable plan's provider
  requirements outside the writer, skips current accepted live/absent output,
  and atomically appends only pending requests with a separate durable retry
  cursor. Accepted pages advance metadata without empty journal records. The
  original dispatch cursor, obligation revision/count and completion evidence
  remain unchanged. Checksummed owner-local round IDs prevent repeat admission
  within a sweep; input/dependency changes invalidate stale prepared retries.
  Per-document pages rotate fairly and preserve their ordinal across passes.
  Persisted scheduling time prevents cold-owner churn from continually resetting
  the five-minute retry grace; wall-clock regression may retry early but grants
  no acceptance. Runtime deadlines use the owner's awake clock. This repairs
  lost root requests even before upload Begin, so no extra transport-to-document
  reverse index or mass dispatch reset is needed. Tests cover lost queues,
  reopen, aborted retry transactions, stale inputs/rounds, bounded accepted
  prefixes, absence, same-byte output replacement with a surviving receipt,
  record corruption and retry backoff. Complete scoped callbacks, producer
  fault recovery and all-member completion remain separate activation gates.
- Authoritative coverage markers/counters are scoped by namespace, producer
  epoch, and catalog digest. Replica-local legacy markers are not scanned into
  distributed authority: each epoch initializes empty counters transactionally
  and credits only ordered publications. Statistics capture the authority and
  counters in one atomic point read, with bounded retry across epoch changes.
  Missing counters in populated authoritative generations remain pending, even when a
  publication would leave its coverage marker unchanged. A pending finalization
  retains its upload for retry rather than creating a terminal acknowledgement.
- Raft baseline discovery prepares owned, bounded pages outside apply (128 keys,
  64 KiB or 2 ms, allowing one oversized key up to the cursor limit). Pages bind
  the observed Raft cut, expected cursor, physical row keys and fixed upper bound.
  Followers apply the selected page instead of scanning their local work records.
  Cursor progress, current-input obligations and the Raft/outbox cut commit
  atomically. Foreground mutations capture behind-cursor and new tail writes;
  the fixed bound prevents continuous inserts from extending the migration.
  Queue admission is not completion, and a missing dispatcher cannot advance it.
- Dirty work is keyed by namespace and authority epoch. The scheduler work-page API
  returns owned pages with exact input revisions and epoch-fenced cursors;
  the snapshot is released before work can be dispatched. Both current-work
  scans and obsolete-epoch reclamation are capped at 128 records, 64 KiB, or
  2 ms (one oversized key may advance). Reclamation rechecks the authority
  before committing and never changes active pending counts or grants seal.
- Strict current-stream verification resolves accepted provenance by receipt
  identity, revalidates its complete logical read-set, and checks every output's
  revision witness without reading large artifact bodies. This is deliberately
  distinct from retry acknowledgement: shared graph outputs can be legitimately
  superseded and need projection reconciliation, not repeated inference.
- Accepted producer proofs now also stage a document-ordered reference in the
  same atomic apply, including publications whose output is absent. Variable
  key construction occurs before the apply lock; cold epoch retirement recovers
  the document from the bounded proof rather than duplicating long document
  keys on the write path. A bounded, two-phase maintenance page verifies old
  references and document entries outside its writer transaction, then retires
  them with reference counts and proof bodies under an authority CAS. This is
  the range-seekable source evidence index
  needed for bounded snapshot export, not itself a portable proof stream or a
  receiver-local adoption certificate. A pinned, document-range proof-reference
  reader now seeks directly to encoded binary lower bounds, pages by entry and
  byte limits, and checks each index against its live source reference and
  proof-body presence. It is candidate enumeration, not portable validation of
  the proof's causal input/output scope. The ordered-artifact target
  passes, including allocation faults, LSM apply, 129-proof paged retirement,
  binary range/resume, corrupted-reference rejection, pinned-reader preservation,
  and current-epoch isolation.
  Accepted proof bodies now use APF3 v3, a compact checksummed binary record of
  the same bounded logical read set, historical output compare-and-swap guards,
  and output digests. Those output guards are kept distinct from causal inputs:
  receiver adoption must compare the imported postimage, not replay the donor's
  pre-publication guard. APF3 removes JSON's
  binary-key expansion so one legal proof fits within the AFB2 block ceiling;
  it does not itself add a portable proof block or confer receiver authority.
  The 206-case ordered-artifact target passes, including forged-field and
  allocation-failure codec tests.
  Certified AFB2 source-copy snapshots now include APF3 proofs once per
  publication digest, plus a compact bitmap naming only output sources whose
  receipt still selects that proof at the immutable cut. Export checks the
  document index and source reference in that pinned cut. The source
  certificate covers the new private block; AFB2 reader capability v5 rejects
  older decoders. One-pass and checkpointed import verify proof framing,
  checksum, source namespace, selected output ordinals and ordering, then
  store the bytes under an inert source-proof prefix, never donor receipt or
  authority keys. Borrowed decode avoids another proof-sized allocation.
  The 208-case ordered-artifact and 28-case retained-transfer targets pass,
  including binary document keys, full source-pin reopen, source certificate
  verification and checkpointed staging import. Receiver-local adoption and
  retained-effect/tail provenance transfer remain open.
  Online merge now reads source proofs from certified AFB2 objects through the
  bounded positional descriptor and carries them in a distinct provenance
  page/chunk payload, not vector/graph effects. The receiver validates APF3
  before apply and commits only inert, source-pin-scoped evidence keys with
  the page receipt. A durable pending bit refuses the final tail certificate
  until receiver-local adoption is implemented; transferred donor bytes do
  not grant acceptance. Tests cover a donor/receiver snapshot, lost page
  replies, a restart between three 1 MiB chunks, wrong-source keys, and the
  finalization barrier. Receiver-local adoption, retained-tail provenance,
  and bounded reclamation of abandoned merge evidence remain open. Replicated
  snapshot publication and final-fence apply now reject a provenance-free
  certificate if producer authority activated after its immutable pin, so a
  late activation cannot turn a previously empty proof stream into an
  apparently complete receiver cut. Import now also checks APF3 source order,
  physical position namespace, artifact guard ownership, output key family,
  duplicate outputs, document ownership and the live scoped-producer guard
  rule before storing candidate bytes;
  a valid checksum alone is not an adoption certificate. Bounded off-lock
  receiver preparation can now recapture every causal primary/artifact input,
  compare its current value and timestamp, and own the receiver's physical
  revisions. It also checks selected output digests, tombstones and donor-range
  ownership; changed inputs or outputs yield a stale candidate rather than
  inheriting donor positions. This remains candidate work until an ordered,
  replayable receiver transaction revalidates and installs local receipts.
  A receiver-side writer-transaction verifier now repeats the exact primary,
  artifact-input, and selected-output checks against local physical revisions,
  including same-byte ABA changes. Candidate preparation also owns the source
  pin, APF3 checksum, donor producer identity and selected-source bitmap after
  release of the transfer buffer. Preparation also requires the certified
  donor binding to match the APF3 epoch/digest and the receiver's ordered
  semantic catalog; writer revalidation repeats the receiver catalog and
  active authority fence. APF3 can encode a receiver-owned adopted origin that
  commits the donor pin, proof checksum and selected subset into a local
  publication digest; no caller installs receipts from it yet. A reusable
  receiver producer plan now validates both exact and semantic catalog
  bindings once per source cut, maps index/graph producer names and kinds to
  receiver-owned physical generations in O(1) per proof, and rejects donor
  generation drift. It also maps enrichment authority epochs and resolver
  definition generations, including default-zero resolver generations, from
  the authenticated catalog pair. Graph effect rebinding and promotion
  identity remain open; mapping is candidate identity, not acceptance.
  The receiver can now construct that plan from the donor catalog durably
  bound to the exact merge attempt at its checkpoint and the current ordered
  receiver catalog. It rejects an attempt change or local catalog drift without
  a donor round trip or per-proof catalog transfer.
  A selected direct-index candidate can now build a receiver-owned APF3 body
  off-lock from remapped local inputs and selected postimages. The body binds
  donor lineage through its adoption digest, drops donor historical output
  preconditions, and refuses graph/non-index adoption; it does not stage
  receipts or grant authority until ordered receiver apply is wired. The
  adopted-proof staging participant now takes selected source ownership from
  APF3 effects rather than fabricated mutation bodies, stages only those
  receipts and document/artifact references, and fences each artifact against
  its actual receiver-local revision. It still requires a certified-evidence
  fence and causal/postimage revalidation in the same ordered transaction;
  graph rebindings remain gated. For an absent selected output with no copied
  revision, adoption staging can now CAS absence and mint a receiver-local
  tombstone revision at its ordered apply position. A lost-response retry is
  accepted only if that revision and proof reference still name the same
  adopted proof; an unrelated same-position write cannot earn a receipt.
  Certified online-merge proof import now stages a 32-byte witness atomically
  with each inert APF3 record. It binds the already-verified APF3 checksum to
  the selected bitmap, avoiding a second multi-megabyte hash; writer
  revalidation CASes only the witness under the apply lock.
  Online artifact-page apply now writes receiver-local artifact revisions in
  the same batch as each transferred afterimage or deletion. Those revisions
  are the exact output witnesses later adoption needs, and replay-only pages
  cannot advance them to a new Raft position. Receiver candidate preparation
  treats a present output without such a revision as stale before constructing
  an adopted proof; absent outputs can still acquire a tombstone revision in
  ordered apply. Selected direct-index preparation now owns the receiver
  candidate, encoded adopted APF3, exact output positions and document
  reference keys as one bundle before apply; releasing the imported proof
  buffer cannot invalidate the prepared transaction inputs.
  Direct-index proof adoption now has a private isolated data-Raft command and
  a distinct standby payload version. Preparation owns the exact receiver
  merge-state/progress bytes; ordered apply compares both again and validates
  local inputs, postimages, catalog and import witness before atomically
  committing adopted receipts, proof references, the Raft marker and HA
  outbox. A stale copy attempt consumes only its ordered marker. Data-Raft
  admission requires protocol 17, which remains above the current activated
  version; no merge coordinator submits this command yet. Bounded proof
  enumeration, retained-tail adoption, graph/scoped-producer rebinding,
  completion discharge and all-member activation still remain open.
- Completion verification now reconciles shared graph winner/count outputs
  against their current accepted projection, while keeping private contender
  and stream outputs revision-exact. Replacement proofs and effect lookups
  are cached within the read; large artifact bodies are not reread.
- After baseline discovery, maintenance prepares bounded provenance-validation
  pages outside apply. A typed, binary-safe ordered control advances the exact
  cursor only at its unchanged mutation epoch, and atomically re-dirties stale
  streams using current input revisions. Followers apply the selected page;
  they do not rescan their local state. The control uses reserved upload/queue
  capacity and retains its exact apply identity through standby replay.
  Completing this validation pass does not clear required-stream obligations.
  The seal transaction requires both drained obligations and a completed,
  unchanged validation epoch; an empty work queue cannot bypass this guard.
- Producer input capture binds compiled templates, physical generations, and
  catalog bytes from one pinned write-plan epoch. It no longer deserializes
  the index catalog or borrows live generation numbers for each input row.
- Resolver callbacks inherit accepted upstream causal input sets, including
  neighboring primary rows and artifact dependencies. Sorted source sets are
  merged linearly and existing guard ordinals are remapped once per proof.
  A current accepted stream is checked before candidate/provider execution,
  avoiding repeat inference after lost replies. This is causal inheritance
  within an owner, not cross-owner provenance adoption.
- Resolver and asset providers now share an owned causal-input context. Copy
  and deferred asset callbacks capture the pinned producer definition before
  execution, inherit accepted upstream proofs, and publish through ordered
  transactions instead of local artifact/skip-state writes. Missing inputs
  publish explicit absence; durable receipts suppress repeat inference after
  acceptance. Provider queue admission alone leaves replay pending. Tokens
  retain digests/proofs, not duplicate document/upstream bodies, and their
  retained memory participates in provider batch budgets.
  Shared producer dispatch treats bounded queue pressure and leadership loss
  as pending control state, not terminal inference failure; cancellation and
  malformed-command/allocation failures retain their distinct dispositions.
- Asset publication derives full-text projections and downstream replay from
  the authenticated catalog. Consumer membership is compiled once per
  publication, and shared document coverage uses point reads across sibling
  provenance/revision witnesses rather than treating one deleted asset as a
  deleted document. Old or stale sibling bytes cannot earn authoritative
  coverage, and large output bodies are not reread for that decision.
  Epoch activation now initializes full-text coverage alongside vector/graph
  coverage. Direct and deferred callback regressions exercise real leases,
  Raft acceptance, stale upstream rejection, absence, and duplicate suppression;
  storage tests cover output authorization, shared coverage and reopen.
- Acceptance receipts and source-proof references belong only to output
  owners, including explicit deletion outputs. Read-only dependencies still
  invalidate the complete input digest but cannot certify their own producer
  work. Duplicate detection uses the same bounded output-owner bitset.
- Chunk output-set preparation now owns one canonical encoding shared by
  storage and text projection, with per-row scratch reuse and allocation-fault
  cleanup. Private checksummed manifests describe contiguous ordinals, total
  payload bytes, and a streaming content digest; replacement validation requires
  the complete old-tail retirement. The prepared writer fence uses one manifest
  point read, rejects stale replacements, and treats missing inventory as a
  baseline gap. LSM rollback/reopen tests cover inventory/member atomicity.
  Root chunk callbacks now use ordered publication when an inventory exists:
  they pin the catalog and input, skip already accepted work before chunking,
  and atomically publish members, tail deletions, inventory, receipts, provenance,
  coverage, and projection work. Preparation authenticates member identities and
  parses each payload once outside apply; commit checks the actual old count with
  one manifest point read. Shared text coverage is reconciled once per document
  and consumer rather than once per chunk. Missing inventory remains control-plane
  pending without provider invocation or replay acknowledgement, and now submits
  bounded ordered inventory-reconstruction controls. Unit-scoped callbacks and
  staged generations above atomic command limits remain part of the producer
  activation gate below.
- Unit-scoped chunk receiver preparation now shares the root replacement
  validator while binding an independent unit ordinal space. The scope is the
  canonical parent-unit key; authorization checks its document, configured
  extraction producer, member identities, payload parent fields and complete
  tail retirement. It resolves accepted parent provenance off-lock and requires
  the consumer to inherit every causal input. Final apply checks a fixed-size
  provenance reference alongside the manifest fence; absent acceptance cannot
  be replaced by plausible unit bytes. Unit projections do not grant terminal
  document coverage. A scope-specific accepted-set reader cannot substitute
  root or sibling evidence. This is the receiver boundary, not activation of
  extraction/unit callbacks: the accepted upstream unit inventory, bounded
  scope census, and large staged publication path remain required.
- Unit chunk input capture now binds the immutable extraction template and
  selects its child from the pinned catalog once per capture session. This
  follows the existing extraction-owned execution model without inventing a
  competing top-level worker template. Each captured input owns the payload,
  inherited causal sources, exact unit identity, and previous chunk manifest;
  no snapshot or catalog lease needs to survive provider execution. A missing
  unit with a live primary requires accepted upstream deletion provenance,
  rather than being inferred from raw absence. Scope, allocation-failure,
  snapshot-lifetime and retirement-fence regressions exercise this boundary.
  Unit inventory reconstruction uses the same extraction-owned authorization
  on sender and receiver, including when no standalone child worker template
  exists. Its bounded ordered pages reconstruct only that unit's old set and
  resume across reopen without manufacturing producer acceptance or a root
  manifest. Primary source identity is captured once per pinned input session.
  This is preparation machinery, not completed extraction callback activation:
  accepted unit-set enumeration and staged publication still gate that path.
- An ordered per-unit chunk callback now consumes that owned accepted input,
  reconstructs missing inventory through scoped ordered census controls, and
  releases storage/catalog leases before chunking. It decodes the retained unit
  and its provenance once, preserves page/document offsets and transcript timing,
  and uses the existing chunk payload encoder. Publication atomically replaces
  the complete bounded unit set and retires old tails; accepted retries skip
  chunking, and accepted upstream deletion produces an empty replacement. The
  callback-to-Raft regression supplies upstream acceptance explicitly and covers
  inventory bootstrap, payload provenance, allocation failures, retry suppression,
  and retirement. This is not extraction-worker activation: enumeration of the
  accepted complete unit set, durable cross-unit continuation, and staged output
  sets beyond the atomic command limit remain required before wiring that worker.
- Chunk reconstruction now has a bounded off-lock pager and portable fixed-size
  digest checkpoints for root/unit scopes. Physical chunk writes and tombstones
  advance a deduplicated stream revision even before its manifest exists;
  reconstruction resumes only in the same catalog epoch and unchanged stream.
  The new, unreleased manifest uses a resumable hash chain rather than serialized
  standard-library hash internals. Discovery still creates no inventory or
  producer completion proof. Root callbacks now order reconstruction pages through
  the existing census transport. Each receiver independently recomputes the
  bounded page and checks the exact before/after digest; sender time slicing is
  converted to an observed row limit for deterministic receiver verification.
  The final writer checks root, catalog, predecessor and stream-revision fences,
  and commits the inventory with its applied marker/outbox while deleting the
  temporary checkpoint. Partial pages resume after restart, and selected immutable
  generation heads cannot be mistaken for empty old-layout streams. This is
  receiver-local inventory, not producer acceptance or all-member agreement:
  staged-generation adoption or explicit replica-baseline agreement is still
  required before distributed retirement may depend on it.
  Reconstruction discovery and verification use transient physical scans; no
  provider invocation or query-cache admission is required. LSM tests cover
  multi-page receiver verification, lost-reply replay, rollback, restart,
  same-byte mutation races, forged page claims, all checkpoint-byte corruption,
  allocator failures, and final checkpoint removal. Root callback tests cover
  the missing-inventory-to-accepted-output lifecycle separately, preserving the
  distinction between inventory metadata and producer completion.
- Chunk query projection now walks a cursor in the caller's pinned snapshot
  rather than retaining a second, owned raw-prefix result alongside projected
  JSON. Ordinary stream names use borrowed key views; binary names retain the
  owning decode path. Projection cloning reserves container capacity before
  ownership transfers through a shared owned-JSON helper, and nested special-field
  handoff is allocation-fault safe. Exclusion-path allocation failures propagate
  instead of silently omitting requested exclusions. Chunk projection now uses
  a snapshot-bound logical cursor that merges legacy scopes and generation heads
  in key order. Selected heads suppress the whole old scope, including empty
  outputs; a prefix-successor seek skips obsolete tails without walking members
  or their derived descendants. Root/unit neighbors and binary producer names
  remain distinct. Generation-only streams are discoverable without legacy rows.
  Index consumers, graph consumers, transfer, and recovery still need the same
  head-selection semantics before production generation publication is enabled.
- Sparse ordinal projection now uses the logical chunk cursor, narrowed to the
  selected producer so unrelated generated streams are not decoded. Artifact
  discovery and document identities share one primary read snapshot; doc-number
  resolution reuses one sparse snapshot rather than opening a transaction per
  member. Embedding-source discovery also streams borrowed rows instead of
  retaining a complete physical range. Regressions keep retired unit postings
  alive and verify exact projected membership, duplicate input ordinals, binary
  producer isolation, and allocation-fault cleanup. This does not establish an
  atomic cross-store cut or certify derived embedding freshness; those still
  require the ordered publication/input-proof integration.
- Staged chunk generations now have checksummed scope/input/catalog-bound
  identities, bounded append preparation outside the writer, durable resumable
  progress, byte-verified duplicate pages, and a guarded atomic head switch.
  Snapshot views perform one seek plus sequential member reads, and never use
  legacy tails once a head selects a generation (including an empty output).
  Public artifact point lookup uses the same pinned-head resolver and releases
  decoded identity ownership on absence. LSM tests cover restart, aborted head
  publication, stale head/input rejection, duplicate append, allocation failures,
  divergent legacy tails, and old-reader visibility after member deletion.
  Document-extraction state recovery now enumerates logical unit and child
  chunk keys from one pinned read; a selected (including empty) extraction
  head suppresses obsolete physical units, and selected chunk heads suppress
  their legacy tails. Navigation-key recovery shares that read and avoids
  loading block values. This closes a recovery-reader visibility gap, not the
  ordered producer-publication or provenance-adoption gate.
  Ordered graph planning also reads a selected extraction root through that
  generation and inherits its accepted head proof, not a stale physical root.
  A head replacement invalidates prepared graph commands at Raft apply; the
  graph callback still needs producer-side extraction activation and the
  remaining cross-owner adoption barriers.
  These are storage/read foundations, not production activation: ordered command
  admission, receipts, quotas, producer regeneration, remaining index/graph
  readers, and transfer/adoption must be integrated before publishing heads in
  production. The storage lifecycle now reserves increasing stream incarnations
  atomically with begin and durably fences retirement before removing members.
  Bounded GC pages resume from persisted ordinal progress, reject active heads,
  and remove terminal attempt state. A single checksummed, scope-bound high-water
  mark prevents delayed begins from recreating reclaimed identities without
  keeping permanent per-attempt tombstones. Pinned readers retain old members
  through MVCC. Append/GC budgets include physical key bytes, with a bounded
  one-member escape for large keys. Fault tests cover restart mid-GC, duplicate
  pages, late append/publish/begin, empty abandoned attempts, and incarnation reuse.
  Authenticated retirement policy, admission, recovery scheduling and ordered
  command/standby wiring remain required before activating this lifecycle.
- Staged-generation recovery now has a bounded metadata-only discovery pager
  with checksummed, scope/authority-bound restart cursors. Root and unit scopes
  cannot consume each other's attempts. Pages verify state identities and the
  selected head's complete, non-retiring state, copy no member bodies, and release
  their read snapshot before returning. Incarnation proposals are read-only;
  ordered begin resolves racing proposals and an aborted reservation is reusable.
  Tests cover reopen/resume, catalog drift, binary scopes, malformed cursors,
  proposal races, and allocation-fault cleanup. Discovery must be scheduled
  fairly and wrap because concurrent begins may sort behind a cursor; scan-end
  is neither an admission barrier nor a producer completion certificate. Recovery
  still needs ordered resume/finalize/retire dispatch and producer-input recovery.
- Extraction generations now have a distinct document-owned physical namespace
  while sharing the existing staged append, incarnation reservation, atomic
  head switch, bounded retirement and recovery machinery. Chunk and extraction
  streams with identical document/producer names cannot share clocks, heads or
  recovery pages. Extraction head changes participate in owner obligations and
  their own stream revision; private rows remain non-visible preparation state.
  Namespace and LSM lifecycle tests cover partial append/reopen, duplicate pages,
  empty replacement, retirement and pinned old readers. This is the shared
  storage lifecycle, not an accepted extraction directory: complete unit
  enumeration, ordered authorization and reader/transfer integration
  still gate publication from the extraction worker.
- Extraction generations now have an immutable named-output directory over that
  lifecycle. Names are bound into each checksummed payload envelope and the
  generation digest; name/ordinal metadata and payload progress commit together.
  Duplicate names cannot silently replace previous pages. Selected point lookup
  reads one name entry and one payload; sequential enumeration and retirement
  use ordinal metadata without loading unit bodies. Checksummed resume positions
  bind the generation identity, so a head change cannot reuse an old ordinal.
  Page admission accounts for both directory indexes and payload keys, with a
  bounded single-large-member path. LSM tests cover reopen, duplicate pages and
  names, exact lookup read counts, allocation faults, metadata-only GC, empty
  replacement, and pinned old readers; codec tests reject corruption and cross-
  generation metadata. This is the named storage directory, not worker
  activation: semantic output authorization, accepted head publication,
  ordered upload/finalization, and durable cross-unit consumers still need wiring.
- Unit inputs, resolver inputs, and public artifact point lookup now select the
  extraction directory in their pinned snapshot. Typed unit names avoid
  repeating the document and producer in every directory member. Head absence
  is guarded before initial publication; an existing head forbids fallback to
  obsolete physical rows, including for an empty replacement. Unit chunk
  preparation requires the selected head guard or both head-absence and exact
  physical-unit guards. Receiver certification binds the accepted head proof;
  nonempty child output cannot consume an absent unit. Missing causal proof
  stays pending, including physical absence. Regression coverage includes binary
  unit IDs, allocation faults, cross-producer isolation, public lookup, stale
  head guards, and pinned reads across retirement. This does not authorize
  extraction publication or activate the worker/cross-unit scheduler.
- Authorized unit capture sessions can now enumerate an accepted extraction
  directory in bounded metadata-only pages. A page owns its logical unit keys,
  selected-head guard, and inherited producer proof; it releases every storage
  cursor before returning. Proof identity must match the selected generation's
  input digest and extraction producer. Non-unit metadata consumes the visit
  budget and advances the continuation, while a deferred large member never
  advances it. Empty directories require the same accepted proof. This supports
  cross-unit work discovery without loading all unit bodies or keeping provider
  work inside a snapshot; discovery/end-of-page is not child completion. The
  producer-side discovery/retirement driver and extraction publication
  authorization are still release gates. Unit-head guards are also recognized
  by command-level scoped validation, not just the receiver-specific validator.
- Unit capture sessions also provide bounded retirement discovery over existing
  child scopes. Membership checks use one named-directory metadata lookup,
  never a unit body, so a replacement with no units can still find every old
  child scope. Current units and other child producers are excluded. Returned
  scopes own the same accepted head/provenance as desired-unit pages. Checksummed
  continuation tokens bind the extraction generation and canonical child scope;
  stale heads, malformed cursors, corrupt inventories and cross-child resumes
  fail closed. Visits and copied key bytes are bounded, including a single-large-
  identity progress path. Discovery must wrap/revalidate because concurrent child
  inventories may sort behind a cursor: scan-end is explicitly not an obligation
  discharge or child-completion certificate. Retirement publication and the
  producer-side driver still need integration; receiver reconciliation is below.
- Desired-unit and retirement pages have a shared receiver-side child verifier.
  Each child must have current accepted provenance, retain the parent's causal
  inputs, and guard the exact selected parent head/revision. Retirement also
  requires a zero-count accepted output: an empty raw inventory is insufficient.
  The verifier obtains counts from the already-read manifest or generation spec
  rather than scanning chunk bodies, and returns an observation retaining any
  cross-document dependency fence alongside the page receipt digest. This is a
  prerequisite for durable page advancement, not a document-completion
  certificate.
- Unit-child reconciliation now has a durable receiver registry and bounded
  ordered `reconcile_units` controls. The receiver derives desired/retiring
  pages from its own predecessor, verifies every child before advancing, and
  rechecks root, catalog, observation and predecessor fences in the same writer
  as the applied marker/outbox. A changed owner materialization cut resets the
  prefix; cross-document inputs additionally retain their validation epoch.
  Claims bind the parent/child identity and generation but not a replica's
  physical root; stored records and local closures are root-bound. Last-page
  retries are idempotent, including terminal retries. Closure is for one child
  requirement only and cannot substitute for the parent's or sibling's proof.
  Obsolete root/epoch records use the existing bounded maintenance collector.
  Producer scheduling and the semantic extraction publication path still need
  wiring. Completion-plan consumption uses an independent unit-child witness,
  not the extraction parent's or another sibling's receipt.
  Reconciliation uses a sorted union of child inventories, selected generation
  heads, and historical raw unit-chunk scopes. It deduplicates overlapping
  representations and seeks past each raw scope's entire tail instead of walking
  its chunks. Continuations remain canonical inventory-scope identities, so
  discovery survives removal of one representation without skipping another.
  Physical cursors do not hydrate external chunk payloads; raw discovery inspects
  at most the first stored entry of each scope. Missing inventories or accepted
  receipts remain pending rather than silently certifying raw/head-only scopes.
  Ordered-artifact coverage includes checkpoint restart, terminal lost-reply
  retry through Raft apply, stale-predecessor rejection, allocation failures,
  corrupt/root-rekeyed records, and invalidation by a behind-cursor insertion.
- Completion plans compile extraction-owned chunk children directly from the
  catalog, independently of top-level worker templates. Each child has a stable
  definition-bound requirement and a local parent-template ordinal excluded from
  portable identity. Template/catalog reordering preserves identity; changing
  the child definition invalidates it. Configured extraction children remain
  pending requirements even when their parent has no executable template.
  A compiled name index replaces repeated child-catalog parsing and parent
  searches in unit authorization/reconciliation controls. The completion
  verifier consumes only current root-bound child reconciliation closures;
  parent extraction, projections, native effects and sibling requirements
  remain independent and must still pass their own gates.
- Completion scheduling now follows its durable verified prefix and proposes a
  bounded unit-reconciliation control when the next requirement is a blocked
  extraction child. Proposals own their data and release the read snapshot
  before queue admission. Missing child receipts remain pending; admission does
  not move either checkpoint. Existing maintenance polling rediscovers refused
  submissions and resumes accepted pages without a tight retry loop. This wires
  receiver reconciliation, not the still-gated extraction writer, scoped
  provider discovery/callbacks, or all-member completion barriers.
  Verification-only sessions avoid redundant primary-document capture/hashing;
  producer-input sessions retain that capture. A point-probe regression rejects
  primary document/typed-row body reads while verifying a current child closure.
  Validation: the ordered-artifact target passes 196 tests with zero failures or
  leaks, including proposal rediscovery, durable-prefix selection, missing-child
  refusal, allocation failures, restart, and the primary-body-read prohibition.
- Unit producer discovery now has a separate generation-bound continuation for
  desired children and obsolete-scope empty replacements. It selects only
  missing/stale child receipts, while rejecting stale parent evidence and
  propagating corruption. Cursor identity binds the authority/catalog,
  document, parent and child; checksummed framing rejects phase confusion and
  foreign-child resume. Owned pages retain no read snapshot and provide a
  point-only admission fence. Accepted child writes do not rewind discovery;
  changing the selected parent generation requires rediscovery. Scan exhaustion
  is not completion and subsequent sweeps must wrap for behind-cursor inserts.
  Durable scoped-job admission and actual replay callbacks are still required:
  the current thin worker journal coalesces generated references into document
  keys and rebuilds root requests, so adding unit fields to those references
  alone would silently discard the scoped work identity. Admission must retain
  each child/unit/generation together with cursor advancement atomically.
  Validation: 197 ordered-artifact tests pass with zero failures or leaks,
  including missing-result selection, obsolete-scope retirement, stale-parent
  rejection, wrong-child cursors, allocation failures, and a discovery reader
  that rejects extraction-unit payload reads.
- Scoped unit jobs now have a root-local durable outbox admission layer. It
  atomically stages the discovery cursor and generation-bound unit identities,
  deduplicates exact admissions, rejects stale predecessors, and accounts for
  outstanding job count/bytes across pages and generations. Capacity rejection
  occurs before writes and cannot skip the refused page. Payload preparation,
  hashing and cursor encoding happen before the writer; writer-side guards use
  fixed metadata and bounded point probes. Root/epoch cleanup uses the shared
  bounded registry collector. This is not worker activation: the worker must
  still execute/retire scoped jobs with current-generation receipt checks before
  freeing headroom.
  Validation: 198 ordered-artifact tests pass with zero failures or leaks. New
  coverage checks atomic job/cursor persistence across reopen, quota refusal
  without partial insertion, duplicate admission, stale predecessors, allocation
  failures, binary identities, and root/generation-bound checksums.
- Scoped-job admission now joins the durable document wakeup through the shared
  replay transaction, backlog admission, catalog fence and standby write gates.
  Refusal aborts both jobs and wakeup; metadata-only discovery pages advance
  without empty replay records. Thin journal coalescing no longer needs to carry
  unit identities because the same transaction retains those in the outbox.
  Bounded outbox reads own their returned identities, verify each record once,
  and include older generations so workers can retire obsolete jobs and release
  capacity. Workers must wrap discovery cursors for behind-cursor insertions.
  Automatic scoped callback execution and worker-loop integration remain gated.
  Validation: 198 ordered-artifact tests pass with zero failures or leaks,
  including refusal with no visible replay/job insertion, metadata-only pages
  with no wakeup, reopen with the wakeup and jobs intact, and allocation-fault
  coverage of bounded reads across both current and obsolete generations.
- Scoped-job retirement now requires a current accepted child result, including
  an explicitly empty result for a unit absent from the selected parent, or a
  newly accepted parent proving that the queued generation is obsolete. Missing
  receipts remain pending; callback submission never frees capacity. Parent and
  child verification share the reconciliation verifier. A bounded retirement
  session amortizes catalog binding and parent verification across jobs, while
  each prepared retirement owns point-only writer fences. Job deletion and
  counter release commit atomically without advancing the discovery revision or
  discharging stream obligations. Current counters allow concurrent admission
  and retirement; duplicate retirement does not decrement twice. The DB entry
  point uses the existing authority, snapshot and apply fences. The worker still
  needs to invoke these paths around actual scoped callback execution.
  Validation: 198 ordered-artifact tests pass with zero failures or leaks,
  including accepted-result resolution, stale-parent fencing, unfinished jobs
  retaining capacity, batched obsolete-generation retirement, allocation faults,
  idempotent deletion, exact counter release, and pinned-reader visibility.
- Scoped-job worker turns now own bounded scan pages and persist an independent,
  checksummed, root/catalog-fenced continuation. Failed jobs retain their quota
  but cannot pin every attempt to the first page; exhaustion wraps, and inserts
  behind the continuation are retried on the next sweep. Cursor commits are
  idempotent, stale concurrent commits fail, and receipt retirement/admission
  do not rewind worker progress. The DB entrypoint uses the existing standby,
  snapshot-replay, and apply fences. Generation-bound callback capture rejects
  an obsolete/missing parent before unit-body reads or provider execution;
  stale/already accepted callbacks also avoid deserializing chunker settings.
  Maintenance follows the existing blocked completion prefix and either
  submits a verified control or atomically admits scoped jobs with their wake.
  Discovery resumes its own durable cursor, resetting only on parent replacement
  or sweep exhaustion. The runtime job callback is generation-bound; automatic
  bounded worker execution and receipt retirement still need wiring, including
  fair scheduling across children, route selection, and large staged outputs.
  These scheduling records grant no acceptance or activation evidence.
  Scope-local outbox capacity refusal leaves admission unchanged and lets the
  maintenance document sweep continue. Validation: 199 ordered-artifact tests
  pass with zero failures or leaks, including durable continuation/reopen,
  failed-job wraparound, stale cursor CAS, allocation failures, parent-generation
  reset, blocked-child scheduling, and stale callback rejection.
- Nonempty child outboxes now have a root-local, per-document scheduling
  directory. First-job admission installs its fixed-size entry atomically;
  last-job receipt retirement removes it in the same transaction. Document
  cursors select one child with a bounded seek, without walking catalog nodes
  or unit bodies. Independent child and document continuations can commit
  together, preserving fair retry across failing siblings and wrapping for
  insertions behind the saved key. Live-child counts let the last retirement
  remove document cursor metadata in O(1); a first-admission generation fence
  rejects old cursors after a drained queue is refilled. Cursor commits preserve
  concurrent live-child count changes. Document-key hashing is prepared before
  the apply lock; final counter/directory fences use fixed-size keys and values.
  Root/epoch GC handles both registries.
  The directory itself grants no acceptance evidence; the replay worker and
  receiver-verified retirement paths below consume it.
  Validation: 200 ordered-artifact tests pass with zero failures or leaks,
  including sibling retry fairness, binary document isolation, behind-cursor
  admission, current-key retirement, joint-cursor reopen, concurrent count
  preservation, drained/refilled generation fencing, last-child cleanup,
  directory corruption rejection, and obsolete-root GC.
- The local worker checkpoint can now commit both fair continuations and up to
  128 receiver-verified receipt retirements in one store transaction. A stale
  cursor aborts the whole page without dropping jobs. A duplicate retirement
  returns after an absent-job point read, even if a later parent generation
  has invalidated the old receipt; present jobs still require the current
  catalog, authority, and accepted-result fences. Callback selection and
  execution remain the activation boundary.
- Required-work maintenance now visits one document's scoped outbox after
  native input closure, checks up to eight jobs per child against receiver-local
  accepted or obsolete receipts, and checkpoints retirement with both fair
  cursors. Pending callbacks remain queued. Documents without an outbox use a
  point lookup and never open a physical worker cursor. Job preparation reuses
  the bounded page's owned bytes instead of refetching each record from the
  LSM; the writer still checks exact bytes and current receipt fences. This is
  automatic receipt reclamation, not automatic producer execution or stream
  completion.
- The scoped unit chunk callback now takes its child range and placement from
  the accepted typed unit payload, so its route is independent of the number
  of chunks later produced. Chunk and unit range kinds remain distinct even
  when they share the same range ID. A remote unit route is rejected before
  chunking or publication until the routed child command path is available;
  callers cannot supply an unverified route for a durable job. Dynamic
  child-range inspection and routed remote publication remain open.
- Generated replay wakes now select one child and a bounded four-job page from
  the durable directory. The runtime clones the parent template and releases
  the read snapshot and plan before invoking each generation-fenced callback;
  a stable DB-owned context commits both fair cursors under the ordinary HA,
  snapshot, and apply gates. The replay remains pending while the document has
  any queued jobs, including on a wrap turn, and receiver-verified maintenance
  retires them. Missing dispatch or durable-turn infrastructure also fails
  closed while an outbox exists. A restart fixture checks the document-only
  wake, unavailable hooks, and refusal of the durable turn commit without
  cursor advancement or job loss. After receipt retirement, the fixture
  accepts a fresh typed parent generation, admits its current unit job, and
  verifies that bounded worker turns submit an actual child publish command
  while retaining the job pending receiver acceptance. The receiver then
  applies that command, retries the still-queued job without a duplicate
  submission, and retires it and its document wake only through accepted
  receipt maintenance. This wires local
  callbacks but does not complete remote child placement, large staged
  outputs, all-required stream closure, or the distributed activation fault
  matrix.
- Persisted unit encoding now lives in a shared typed payload contract rather
  than the runtime implementation. Ordered unit chunk callbacks decode through
  that contract once, bind document/producer/unit identity, reject provenance
  overrides of identity/text, conflicting mirrored metadata, and route
  contradictions (a local route has no remote owner; a committed remote route
  has one), and verify the
  current logical unit fingerprint before invoking a provider. Transcript/text
  ranges and document offsets are checked against the unit body. Owned decoded
  slices survive release of the raw input; preparation no longer constructs a
  second merged JSON object. The encoder preserves the existing artifact shape.
  This is the contract needed by extraction receiver preparation, not activation
  of the still-gated extraction writer or its ordered continuation protocol.
- Generation heads are now canonical read-set guards, not generic artifact
  mutation authority. The physical commit hook stamps both the exact head and
  its stream witness in the same native/Raft transaction; private staging rows
  do not invalidate visible inputs. Owned producer observations record head
  absence before first publication and use the selected head as the immutable
  set's provenance witness afterward. Asset callbacks and resolver reads select
  those inputs in one snapshot and inherit the head's accepted causal proof,
  including for empty output sets. Missing producer certification remains pending
  rather than adopting a legacy member receipt. Root asset reads, like unit
  reads, resolve through the selected extraction head; a selected empty set
  cannot fall back to stale physical root bytes. Downstream asset publication
  validates that root membership and the accepted producer proof off-lock,
  then fences the receiver-local proof reference at atomic apply. LSM tests
  cover private append, rollback, head replacement, stale/absent guards,
  downstream acceptance, stale/empty guard rejection, and pinned old observations.
  Ordered staged-generation finalization must still publish that proof together
  with the head, receipts, coverage, and consumer work before these paths activate.
- Accepted asset retries now drive ordered graph consumers before completing,
  including absence and deferred-provider acceptance races. They plan against
  the accepted value and its causal proof in one pinned snapshot, without copying
  the asset through an extra preliminary read or rerunning the provider. Derived
  replay under producer authority consumes committed graph effects rather than
  calling the legacy primary-state materializer. The erased document-store
  adapter forwards snapshot forks while retaining payload/import-reader lifetime.
  Regression coverage includes transitive generation-head guards, queued graph
  rejection and accepted-proof invalidation after a head replacement, graph queue
  refusal, real edge publication/absence retirement, accepted retries, binary
  graph-owner keys, and forked snapshots surviving parent close. Graph keys use
  their own suffix grammar rather than public artifact-ID parsing; nested review
  ownership remains restricted to the resolver source scopes. These
  repairs do not complete staged-generation publication, stream drain/seal,
  receiver adoption, or the distributed activation fault matrix.
- Required-work observations carry durable owner-local revisions in addition to
  primary source positions. Re-marking a dependency advances the revision even
  when the primary is unchanged; completing/removing a work record cannot reuse
  an old identity. Journal admission compares that revision atomically with its
  dispatch marker and replay append. Completion also requires the current local
  revision and authority, after receipt validation in the same writer snapshot.
  These revisions are not portable completion certificates: an ordered driver
  must validate each receiver's current obligations and causal receipts rather
  than copying a leader's local token. The required-stream enumeration and
  ordered drain/seal driver remain open.
- Root-template dispatch is now paged rather than allocating and journaling the
  whole catalog for each document. A borrowed fixed-size page preserves the
  immutable plan's upstream-first order, caps requests and escaped-key byte
  estimates, and permits one bounded oversized request. The next-template cursor
  and last dispatch sequence commit with each journal append. Partial progress
  remains schedulable across restart; dependency changes reset it under a new
  work revision. Dispatch completion still does not discharge an obligation or
  certify dynamically scoped unit/chunk, resolver, graph, or promotion streams.
- Logical chunk cursors support exclusive ordinal resume with direct seeks into
  the selected generation, skipping shadowed legacy tails and preserving unit
  boundaries. Resume keys remain positions, not completion certificates; a
  multi-snapshot consumer must validate its work/input revision. Stored embedding
  input fallback and source-hash checks now resolve selected generations in one
  snapshot, including empty output, rather than reading obsolete physical rows.
  Fallback extraction streams rows without retaining a second full raw scan.
  Ordered materialized dense/sparse callbacks now use selected logical chunks
  and retained vector scopes together; other chunk/unit production paths and
  stream completion are still required before activation.
- Scoped chunk-vector commands now authenticate their logical member and exact
  output, require head and output-precondition guards, inherit the accepted
  upstream proof, and project only to matching immutable-catalog consumers.
  Dense/sparse payloads and dimensions are validated before apply. Input capture
  owns the selected bytes and causal read set without retaining an LSM snapshot
  across provider work; provider identity uses the compiled write-plan template
  rather than reparsing catalog JSON per chunk. Provenance decoding happens
  off-lock under a tracked preparation budget; serialized apply rechecks the
  inherited inputs and a fixed-size receiver-local proof reference. Replacing or
  retiring that reference invalidates preparation even if output bytes match.
  Tests cover allocation failure, shared consumers, uncertified input rejection,
  proof-reference races, and sparse projection through actual Raft apply.
  A member receipt does not grant document-wide completion. Full stream
  reconciliation and drain/seal remain activation gates; this
  receiver-local fence is not a portable provenance-adoption certificate.
- Materialized chunk-vector callbacks prepare bounded dense/sparse provider
  batches with owned causal inputs under tracked preparation memory. The
  snapshot and catalog read fence close before inference or dispatcher calls.
  Accepted retries skip provider invocation; publication and absence cleanup
  use the ordered command path, never the legacy direct artifact writer.
  A merged candidate cursor includes existing vector scopes whose chunk has
  disappeared, so shrinking/empty selected generations retire obsolete vectors.
  It resumes across binary names, generation/root/unit boundaries, deduplicates
  live scopes, and avoids rescanning the physical tail ahead of every selected
  member. Tests exercise allocation failures and actual callback-to-Raft output
  acceptance and absence retirement. Completion remains pending: this scan is
  not a portable exact-input stream certificate. Durable scheduler continuation,
  unit/extraction/promotion paths, and the
  stream-completion/obligation driver still require integration before activation.
- Producer censuses now have a document materialization revision, staged in the
  same physical transaction as primary and visible artifact mutations. New unit
  scopes, vector/graph changes, generation-head switches, and nested resolver
  outputs invalidate the owning document's census; unrelated document writes do
  not. Private generation uploads and aborted head publication do not advance
  visibility. One revision write per affected owner avoids per-stream fan-out.
  Receipt-only chunk-vector passes pin this witness across page snapshots and
  inspect the complete accepted proof without decoding it twice. Foreign-input
  proofs additionally pin the mutation epoch, so neighbor changes cannot bless
  a mixed-input pass. A publishing pass cannot also certify completion. These
  observations are preparation fences, not durable/portable completion records;
  the ordered stream-completion command, required-stream
  closure, receiver adoption and drain/seal integration remain open.
- Accepted-member chunk-vector census pages now persist receiver-local
  checkpoints with their exact input observation, exclusive binary cursor,
  member count and rolling receipt digest. Atomic compare-and-swap rejects
  concurrent/regressing scans; duplicate pages are idempotent, and stale input
  observations permit a fresh census rather than trusting old progress. EOF is
  explicitly enumeration, not stream closure: it neither advances replay nor
  discharges a document obligation. Runtime retries reuse current checkpoints
  without repeating provider work. LSM reopen, corruption, allocation failure,
  stale-page and lost-reply tests exercise the persistence contract. Keys have
  namespace/epoch prefixes; obsolete checkpoints share bounded obligation GC,
  preserving pinned readers and current-epoch state through catalog churn.
  Portable completion commands, provenance-adoption certificates,
  durable scheduler continuation, full stream closure and drain/seal are still
  required; these local checkpoints must not be adopted as remote certificates.
- Local census checkpoints are now bound to the durable physical-root
  incarnation in both their key and checksummed value. The resident DB injects
  this identity into replacement runtimes; provider configuration and imported
  metadata cannot override it. Ordinary reopen preserves useful progress, while
  imported donor keys are outside a fresh root's lookup namespace and re-keyed
  donor bytes fail validation. Unknown physical identity fails closed before
  provider invocation or publication. Cleanup also retires foreign-root records
  in the active catalog epoch by seeking over the current root's entire prefix.
  Tests cover reopen identity, fresh-root metadata copies, forged re-keying,
  absent identity, and old/current-epoch GC with pinned readers. This local
  binding does not replace portable source/provenance adoption or distributed
  completion/capability barriers.
- Chunk-vector censuses now distinguish physical-tail budget yields from EOF.
  Checkpoints retain an inclusive physical scan position independently of the
  last accepted logical member, allowing ignored-only pages to survive restart
  without inventing accepted members or repeatedly inspecting the same tail.
  Accepted-only passes yield after each durable page; physical scans share
  visit, byte and elapsed-time budgets. Binary-key, unrelated-output, expired
  deadline, allocation-failure and LSM-reopen tests cover ordering and resume.
  Publishing-pass scheduling still needs separate time slicing; these changes
  do not make the entire reconciliation bounded.
  Checkpoint encoding is prepared into owned bytes before the writer fence;
  input validation and checkpoint CAS remain in the committing transaction.
  No completion, obligation discharge or capability activation is inferred.
- Logical chunk scans now share the page budget with physical output scans.
  A checksummed merge position retains the next generation ordinal and legacy
  row independently, including progress through empty generation heads and
  ignored legacy rows. Census checkpoints persist that position with CAS
  checks that forbid either merge input from rewinding. Prefetched logical
  members retain their predecessor position when an older output is returned,
  so snapshot-per-result retries neither skip members nor repeatedly traverse
  empty heads. An expired deadline still permits forward progress in both
  inputs. Ordered stream-closure commands and receiver verification remain
  open; a resumable local scan is not a portable completion certificate.
  Regression coverage includes checkpoint reopen without accepted members,
  independent merge-input rewind rejection, allocation failures, and
  snapshot-per-result scans through 64 empty heads and obsolete outputs with
  an operation-count bound to catch repeated empty-head traversal.
- A receiver-side chunk-vector census primitive now recomputes bounded pages
  from the immutable producer plan and current accepted provenance. Claims
  bind the producer/index generation, authority, owner input witness, receipt
  chain, and both cursor positions; verification takes a receiver-supplied
  predecessor rather than trusting a sender's local checkpoint. Deterministic
  visit/byte limits reproduce page boundaries without provider invocation.
  Owner-local progress ignores unrelated mutation epochs; foreign-input proof
  participation pins the epoch. Dense/sparse callback tests cover live and
  empty streams, missing acceptance, forged predecessor/result claims, stale
  inputs and allocation failures. This is read-only preparation; the progress
  registry and command below supply the commit boundary. Upstream stream
  closure, obligation discharge, adoption and distributed fault validation
  remain open. EOF alone
  still cannot certify that an upstream producer has finished creating scopes.
- Receiver-verified pages now have a separate durable progress registry. An
  owned prepared record carries the receiver's expected predecessor; the final
  writer rechecks the input witness, physical-root identity, seal state and
  predecessor CAS before installing it. Aborted writes do not advance the
  registry, lost-reply duplicates are idempotent, and competing or stale pages
  cannot overwrite newer progress. Stale input observations restart the scan.
  Fast paths still check the immutable producer plan, including empty streams.
  Root-bound records authenticate both page claims and progress bytes and share
  bounded old-epoch/foreign-root cleanup with other producer metadata. Tests
  exercise real LSM transactions, abort/retry, competing pages, stale prepared
  work, corruption and allocation failures while preserving replay position
  and pending obligations. This writer primitive is not by itself a stream
  closure command or an activation certificate.
- Census pages now have a bounded binary-safe publication control command.
  Receivers resolve the immutable catalog template, independently verify the
  claimed page outside apply using a tracked working-set allocator, then
  commit progress together with the Raft applied marker and the existing
  standby marker/outbox machinery. Stale source/catalog, pending provenance
  and sealed-source outcomes use the same durable rejection path as other
  producer controls. JSON/compact codec tests bind producer identity, limits
  and page claims; dense/sparse callback tests exercise actual Raft installation,
  duplicate apply and stale-generation/input rejection. Upstream closure,
  obligation discharge, provenance
  adoption and distributed crash/promotion validation are still open.
- Materialized chunk-vector workers now automatically drive bounded verified
  census pages through the publication control dispatcher before attempting
  provider work. Missing acceptance falls back to materialization; current
  enumerated progress avoids repeated scans and dispatch. The worker releases
  its read, plan and ownership fence before enqueueing, and queue refusal does
  not advance durable progress or replay. An accepted prefix can advance even
  when its next member is pending, but that member's cursor is not skipped.
  Tests exercise dense/sparse dispatch, refusal/retry, multi-page continuation,
  accepted-prefix verification, and no reinference after acceptance. Normal
  publication waits no longer log warnings, inflate error counters or inherit
  exponential provider/pipeline backoff. Workers still retain replay debt at
  enumeration: upstream closure and exact obligation discharge remain required.
- Census EOF now requires a current accepted root-chunk manifest, including
  explicit empty output, and inherits that producer's complete causal inputs.
  Missing manifests stay pending and altered manifest bytes fail the accepted
  output digest check. Selected generation heads take precedence and require
  their own accepted proof; they cannot borrow an older manifest's acceptance.
  Dense/sparse regressions cover these failures, unaccepted baseline manifests,
  and the valid empty result. This proves only the root replacement boundary, not
  unit/DAG closure or document completion. Catalog authorization is now bound
  once per pinned census/provider batch and reused across members; per-member
  upstream provenance, source capture and output CAS remain independent.
  An operation-count regression verifies one ordered-catalog read across
  sixteen live/absence captures with independent accepted-proof validation.
- The receiver now persists root-only chunk-vector stream closure in the
  census transaction (AVP2), alongside the Raft/standby control markers. Closure
  is derived locally from the pinned catalog and verified enumeration; no
  sender-supplied bit can grant it. Unit/extraction and neighbor-dependent scope
  sets remain open pending their upstream scope certificates. A closure handle
  owns its document identity and rechecks physical root, causal observation and
  exact registry version in the final writer without scanning or decoding
  provenance there. Dense/sparse tests cover live and empty closure, allocation
  faults, partial/missing progress, root mismatch, stale input and unrelated
  writes. One closed stream still does not discharge document obligations or
  advance replay; it must participate in the all-required-stream verifier below.
- Immutable write plans now compile a catalog-wide completion checklist, not
  just the generated-provider queue. It contains mandatory native/authored
  effects, every provider template, every index projection, and separate
  resolver and promotion requirements. Resolver output, upstream scope kind
  and target table remain explicit. Stable framed identities exclude local
  template ordinals; catalog-bound cursors expose allocation-free pages capped
  at 128 requirements/64 KiB (with single-item progress for larger identities).
  Tests cover all index kinds, empty-provider catalogs, promotion requirements,
  canonical ordering, stale catalog cursors, count/byte pagination and allocator
  failures. A requirement is not acceptance: the completion executor and the
  unresolved producer/scope handlers still have to certify every node before
  clearing document work. Provider ordinals map directly to canonical nodes,
  and stream closure handles bind their requirement ID for that verifier.
  Provider identity hashes use the same exhaustive definition-field selection
  as authorization, excluding only row/replay identity and consumer routing.
  Cursor fingerprints cover both catalog bytes and the complete canonical
  requirement set, so adding a requirement cannot reuse an old ordinal cursor.
  Identical provider definitions shared across consumer routes coalesce into
  one requirement while preserving direct ordinal lookup and separate index
  projection requirements; distinct provider inputs never coalesce.
  Compiled definition lookup also resolves row-specific requests without
  scanning the entire template list; execution authorization still compares
  the canonical template's complete definition and fences the active catalog.
  This checklist does not enable protocol 15.
- Required-stream completion now has a distinct private ordered control. The
  work driver prepares bounded pages and submits them without discharging local
  work on queue admission. Every receiver independently verifies the exact
  before/after page identity against its pinned catalog and causal cut; portable
  fingerprints exclude physical roots and local obligation counters. Receiver
  verification uses the sender's observed requirement count rather than its
  machine-dependent deadline. Final apply checks receiver-local root, catalog,
  work revision, predecessor and witness fences, then atomically commits either
  a prefix checkpoint or exact obligation discharge with checkpoint deletion,
  the Raft applied marker, and standby outbox. No terminal certificate remains
  per document. Missing requirement verifiers still stop progress; all-member
  evidence agreement, repair/retry coordination and drain/seal remain separate
  activation gates. A sender's completed page is not remote acceptance.
  Extraction-owned producer scopes now use a separate receiver-local closure:
  it authorizes the immutable template, verifies the fully published named
  generation and accepted head proof/inputs, then carries exact head bytes,
  the proof reference, and bounded private generation-state CAS witnesses into
  the final writer. Child streams remain separate requirements. Missing heads
  stay pending, and head-byte or private-state mutation invalidates a prepared
  parent witness without rescanning units under apply. Allocation-failure
  coverage exercises witness preparation and release. This does not activate
  the still-gated extraction writer, portable adoption or all-member drain/seal.
  LSM regressions exercise native-only completion on two independent roots with
  different work revisions, receiver evidence missing/present after restart,
  rollback, duplicate delivery, stale inputs, forged claims, allocation failures,
  and actual work-driver submission. A separate indexed-table test keeps work
  pending at an unverified index requirement; the 257-requirement engine test
  checks deterministic bounded page claims, restart and final checkpoint deletion.
- Completion prefixes for indexed catalogs now pin a durable receiver-local
  projection lifecycle epoch. Sidecar creation/replacement, status or generation
  changes, configuration identity changes, watermark regression, same-cut count
  replacement and checkpoint removal revoke old evidence before publication;
  ordinary monotonic watermark advances do not. Primary-metadata resets revoke
  inside their transaction. Failed sidecar publication may leave a conservative
  revocation, never a new sidecar with an old completion prefix. Plans without
  index requirements do not read this fence. Portable page identities exclude
  the local epoch value; each receiver still checks its own fence at commit.
  This supplies revocation, not index durability evidence: actual projection
  witnesses and their publication/recovery wiring remain activation work.
  Checks cover restart of a revoked completion prefix, unchanged document/work
  revisions, portable fingerprints across local epochs, corruption, monotonic
  progress, reset rollback and allocation failure between revocation and sidecar
  replacement. The ordered-artifact target now includes the shared apply-state
  suite, including concurrent sidecar writers and checkpoint-format regression
  tests, rather than testing only the new control path.
- Materialization revisions retain the exact receiver-local replay boundary of
  their owner's visible mutation. The physical transaction observer grants a
  boundary only for a fresh all-effects journal append paired with its advancing
  next-sequence marker. Unjournaled/imported changes and ambiguous journal batches
  retain an explicit absent boundary; they cannot borrow another document's old
  journal tip. Unrelated writes therefore do not continuously move the target
  needed for a document's projection evidence. This field is local evidence,
  not portable producer identity or proof that an index has applied the record.
  The PR-only materialization record has one strict versioned encoding, without
  a legacy decoder. Regression coverage checks namespace/version/truncation,
  exact replay capture, stale journal-key reuse, unrelated writes, abort and
  reopen. Live dense/sparse/graph resets revoke projection completion before
  closing storage; interrupted full-text reset also revokes before clearing.
  Full-text rebuild initiation durably publishes a rebuilding checkpoint and
  restart marker before destructive reset, and publishes clean only after
  backfill and sync. A restart-marker publication fault must preserve the old
  physical index while retaining explicit rebuilding debt. These boundaries do
  not activate index completion: physical projection witnesses and coordinated
  publication/recovery still need their own implementation and release evidence.
- Projection evidence preparation has a nonblocking guarded sidecar snapshot:
  it retains the existing checkpoint-publication lock across decoded checkpoint
  access and final primary-metadata fencing. This prevents reading a new
  revocation epoch together with an old clean sidecar while its replacement is
  in flight. One snapshot supports multiple index lookups; absent entries are
  explicitly absent rather than fabricated clean zero-watermark records. Busy
  publication yields for retry, and allocation/error paths release the lease.
  Independent raw reset or catalog changes still fail the final transaction's
  authority/epoch check. Writer reopen revokes local projection evidence before
  loading physical indexes; read-only/status opens do not mutate this fence.
  Tests cover lease lifetime, busy retry, allocation failures, missing sidecars,
  authority/lifecycle changes and writer versus read-only reopen. The guarded
  snapshot is not itself physical evidence; physical validation and aggregate
  certificates supply the additional checks described below.
- Full-text replay for activated physical owners now publishes a checksummed
  projection seal in its own index metadata before advancing the external
  applied-sequence sidecar, on both direct and batched/coalesced replay paths.
  The seal binds physical root, namespace, immutable coverage generation,
  configuration identity and replay cut. Publication forces index/WAL durability
  before writing the seal; retries do not regress its cut and finish any pending
  seal durability barrier. A different physical identity cannot reuse it.
  Unpublished managers have no serving-root authority. Reset and structural
  segment removal delete the seal in the same index transaction as the data
  change, rather than relying on a later best-effort primary cleanup. Tests
  exercise byte corruption, monotonic retry, wrong-root rejection, reopen,
  retained pre-reset readers, real full-text replay and modeled power loss/reset
  sync failure. These seals establish the full-text physical publication
  boundary only: automatic certificate refresh, startup/transfer adoption and
  other index-kind adapters remain open.
- Full-text physical validation now joins the guarded sidecar to the live
  generation's seal. It rejects missing/nonclean/ahead checkpoints, mismatched
  root/namespace/config/coverage generation, incomplete rebuild markers and
  pending repair/admission. Progressive query serviceability does not override
  corpus-wide repair debt. Catalog and per-index apply ownership are retained
  until evidence publication; try-lock acquisition avoids a lock-order cycle
  with replay publishers. A final primary-transaction check repeats the local
  lifecycle/catalog fence and rejects newly durable managed admission. The
  sidecar lease must outlive this physical guard. LSM replay tests cover wrong
  generations, busy retry, allocation cleanup, new admission and a forged clean
  sidecar ahead of physical data. Split pruning serializes with the index apply
  lane and revokes both primary evidence and the physical seal before changing
  the corpus, including mixed-segment replacement (which is not equivalent to
  ordinary compaction). The guarded generation is the prerequisite for the
  aggregate evidence publication described below.
- Full-text aggregate projection certificates now have a strict key-bound
  encoding and transactional issuance from a retained physical guard. One slot
  per index replaces old roots/epochs instead of accumulating per-row or
  historical certificates; index removal deletes the slot in the same catalog
  transaction. Same-identity publication is monotonic and idempotent. Readers
  reject damaged certificates; a newly validated physical guard can reconstruct
  that derived record without trusting its damaged predecessor. Completion
  can consume the record against a document's exact local replay boundary and
  performs only metadata point checks in the final writer. New source mutations,
  lifecycle changes, root changes, stale authority and insufficient coverage
  invalidate the witness. Missing/unrecorded source boundaries require the
  completed snapshot-adoption proof described below; no global journal tip is
  substituted. The ordered completion verifier now
  supports this full-text requirement alongside native/provider requirements.
  Tests cover certificate corruption/slot substitution, aborted and duplicate
  issuance, unknown requirement identities, wrong roots, lagging coverage,
  reopen invalidation, catalog-drop cleanup and a native-plus-full-text ordered
  completion. Leader discovery and receiver preparation now automatically refresh
  missing/lagging full-text certificates before opening their proof snapshot.
  Refresh shares the completion engine's exact resume state and bounds work to
  the next visit/byte-limited page, avoiding starvation beyond the first 128
  requirements. Valid cached coverage uses only metadata probes; missing
  evidence shares one nonblocking sidecar lease, validates physical generations
  outside the primary writer, and repeats fences at commit. Leader maintenance
  also yields between indexes on its time budget. Busy leases leave work pending;
  catalog/source races retain ordered rejection semantics on receivers. Tests
  cover busy retry, a cached page without any sidecar file, and receiver-local
  reconstruction after deleting the discovery certificate. A two-root LSM
  fixture rejects a copied leader certificate, reconstructs the receiver's own
  evidence during ordered apply, and retries the same entry after a lost reply.
  Broader historical adoption, other index kinds and all-member drain remain
  release gates; completed full-text snapshot adoption is described below.
- Authority activation now captures a strict, checksummed receiver-local replay
  boundary in the same primary transaction. This is the immutable historical
  baseline cut, not a projection certificate. Repeated activation retains the
  original cut even after the journal advances; in-memory reservations cannot
  inflate it. A new epoch validates the previous boundary and rejects a
  regressed durable journal in the same namespace. Corrupt/missing boundary
  evidence for an existing authority is not reconstructed from today's tip.
  Native and ordered baseline enrollment validate this authority-bound record;
  native discovery repeats the boundary fence in its committing transaction.
  The private key is excluded from portable metadata. Codec and LSM tests cover
  byte corruption, abort atomicity, duplicate activation, uncommitted sequence
  reservations, restart, epoch replacement, malformed/regressed replay metadata,
  and an empty new namespace. Historical projection credit still requires a
  complete snapshot/replay adoption proof tied to this cut; replay-only seals
  and clean sidecars do not provide that proof.
- Under active artifact authority, physical input capture now advances a
  receiver-local source-gap epoch in the same transaction as an unjournaled or
  ambiguous materialization. Ordinary
  journaled writes leave it unchanged. Snapshot guards bind this epoch to the
  immutable activation boundary, including the absence of authority, so an
  activation race also invalidates a pre-activation candidate. Durable repair
  checkpoints persist the guard beside the original replay floor. Resumed
  candidates and final shadow activation recheck it; an invalidated candidate
  is discarded only after releasing its physical handles, then rebuilds from a
  fresh snapshot without failure backoff. Older candidate checkpoints without
  a guard must rebuild rather than receive inferred source completeness.
  LSM materialization tests cover journaled progress, unjournaled invalidation,
  abort and restart; checkpoint tests cover guard persistence, and full-text
  repair tests contrast source-gap restart with journal-only catch-up. This
  closes the source-gap fence needed by snapshot/replay adoption, but is not
  by itself historical projection or producer-provenance evidence.
- Completed full-text shadow snapshot/replay builds now persist an adopted
  baseline in the physical generation's seal before publishing its pointer.
  The seal binds the owning root, catalog generation, activation boundary and
  snapshot source-gap guard. Incremental replay preserves this proof but cannot
  create it; physical reset/removal retires it with the seal. Receiver-local
  aggregate certificates carry baseline evidence only while its source guard
  is current and its coverage reaches the immutable activation cut. Untouched
  pre-activation rows and recorded unjournaled rows included in that snapshot
  can now satisfy their full-text requirement using that proof. An unchanged
  source-gap epoch excludes later unjournaled mutations. Final discharge
  rechecks the source guard, exact row revision, local lifecycle, authority
  and certificate in the same primary transaction. Tests exercise both
  journal-only catch-up and a fresh build after an unjournaled source race,
  followed by real ordered baseline enrollment and native-plus-full-text
  completion of an unchanged historical row. A staged source-gap change rejects
  discharge. LSM tests cover same-cut proof strengthening, preservation across
  incremental replay/reopen and retirement on reset; codecs reject mismatched
  authority, missing baseline identity and insufficient coverage. Other index
  kinds, imported producer provenance and all-member coordination remain
  activation work.
- Full-text adoption discovery now feeds the existing durable generation-build
  scheduler on both leader discovery and receiver completion preparation. Clean
  idle generations with absent/corrupt/incompatible physical proof, or historical rows whose
  existing proof lacks a current baseline, request bounded adoption work;
  checkpoint/apply contention and existing repair are not treated as proof loss.
  Admission releases all physical/sidecar leases first, rechecks the exact
  authority and catalog under the structural/apply fence, and admits at most one
  new build per completion page. The revision-tracked scheduler name map
  deduplicates existing jobs in O(1), including paused and terminal jobs, without
  repeatedly scanning durable repair state per document. The dedicated
  `artifact_baseline_adoption` trigger distinguishes this work from corruption
  and operator requests and retains the serving generation while building.
  Imported seals bound to another root, namespace, generation or configuration,
  and seals behind the clean checkpoint, also request reconstruction; a sidecar
  cannot relabel physical evidence. Two-index LSM tests cover a foreign-root
  seal alongside an absent seal, busy discovery without false admission, bounded
  scheduling, duplicate no-ops, query availability, pause/restart persistence,
  ordinary worker execution and final ordered historical completion. This is
  local durable scheduling; all-member completion/retry coordination remains
  an independent activation gate.
- Document-wide dense/sparse provider streams now have receiver-verified
  closure handles alongside the materialized-chunk census handles. Preparation
  resolves current accepted provenance for the exact physical primary row or
  tombstone, including explicit absent output. A surviving transport receipt
  cannot certify a replaced output. Owned handles retain only bounded identity
  and revision fences; their final check uses three metadata point reads for
  document-local inputs, not vector payloads or provenance decoding. Foreign
  dependencies retain the additional mutation-epoch fence. These handles close
  one generated requirement, not native effects, index projections, or the
  complete document obligation; the all-required-stream executor remains open.
  LSM-backed regressions cover dense vectors on typed rows and sparse vectors
  on document rows, live/absent/deleted inputs, same-byte output replacement,
  unrelated writes, physical-root mismatch, allocator failures and reopen.
- Root chunk replacements and singleton asset providers now participate in the
  same all-required-stream verifier. Root completion requires current accepted
  manifest/generation evidence and the producer's latest reference, including
  explicit empty output; reconstructed inventories alone cannot close work.
  Singleton asset completion retains all accepted causal inputs, including
  upstream assets. Handles own their identity/fences, survive database reopen,
  and check only metadata in the final writer. Tests exercise real callbacks,
  live/empty/deleted owners, stale upstreams, manifest reimports, allocation
  failure and closure after restart. Projection, resolver and promotion nodes
  remain independent and cannot be skipped by provider success.
  Scope classification is compiled once into the immutable completion plan:
  singleton assets use document scope, document extraction retains its dynamic
  scope requirement, and upstream-unit chunks retain unit scope. Authorization
  shares exact definition lookup and catalog/owner fences with vector producers,
  eliminating per-row template scans and provider-JSON parsing. Root-chunk
  closure now uses a pinned, O(1) producer-scope lookup bound into the plan
  digest; duplicate or missing chunk producers remain fail-closed. Dynamic unit,
  extraction and neighbor scope closure remains pending its own verifier.
  Deletion regressions also fixed graph cleanup's tombstone validation: canonical
  empty ownership roots from the owning graph generation are permitted alongside
  zero visible-count witnesses. Nonempty state, segments, trailing bytes, wrong
  generations and other graph owners remain invalid; checks are fixed-width and
  allocation-free. Normal graph preparation still enforces catalog and exact
  mutation/count preconditions before accepting these cleanup records.
- Receiver-local completion accumulation now checkpoints strict prefixes of
  the complete immutable requirement plan. Pages are bounded by count, bytes
  and time; they retain exact obligation revisions, physical-root identity and
  causal materialization observations. Foreign-input requirements additionally
  pin the shared mutation epoch without penalizing owner-local requirements.
  Commit repeats the fixed-width catalog stamp, obligation/cursor CAS and
  read-only witness fences. A terminal page atomically discharges the exact
  obligation and deletes its checkpoint; a persisted EOF cannot grant credit.
  The checkpoint engine's multi-page fault fixture uses explicit test witnesses,
  while native/document/chunk stream adapters use actual accepted evidence.
  Projection, resolver and promotion handlers and the ordered drain-command
  integration remain open: generated-only success never bypasses those nodes.
  Obsolete epochs and imported root identities use bounded local reclamation.
- Vector ingress now distinguishes explicit authored values from generated
  output with a tagged origin, rather than inferring authorship from a missing
  source hash (multimodal provider output can also lack a hash). Dense/sparse
  envelopes preserve descriptive authored origin; contradictory origin/hash or
  graph envelopes are rejected. Direct-field index replay preserves that origin
  and ordinary portable backups use the raw-artifact path for authored vectors,
  avoiding lossy compact batches. Tests cover payload equivalence, malformed
  envelopes, allocation failures, and exact dense/sparse backup round-trips.
  This metadata is not accepted provenance or a completion certificate. Native
  writer-bound certification and receiver adoption remain required; flagless
  historical or imported vectors are not implicitly upgraded to authored output.
- Ordinary DB ingress and coordinated transaction resolution prepare receiver-local authored-vector acceptance
  for active publication epochs. It deduplicates exact primary/timestamp/vector
  postimages and hashes each once before opening the physical writer. The
  physical input-capture hook observes those bytes and issues acceptance in the
  same transaction as their source/artifact revision stamps; incomplete writes,
  replacement postimages, and retries cannot inherit prior observations. Records
  bind catalog, namespace, epoch, physical root, source position and output
  digest. Reads use revision point checks; raw imported origin flags grant no
  acceptance. Obsolete epochs/roots use the shared bounded reclamation walker.
  Native/Raft tests cover raw imports, missing retry postimages, source changes,
  re-keyed records, allocation failures, and dense/sparse outputs. Historical
  provenance adoption remains a separate requirement. Protocol 15 remains disabled.
  Identical dense projection replay now preserves the ingress artifact instead
  of rewriting it and invalidating its source revision; this also avoids a
  redundant vector WAL write. An LSM-backed DB regression verifies acceptance
  after full projection completion and reopening the same physical root.
- Native/base-vector completion now uses a receiver-local, resumable physical
  prefix census. Each 128-visit/64 KiB/2 ms page checks current authored acceptance
  or generated provenance without point-fetching vector bodies or materializing
  external blobs. Persisted cursors
  bind catalog, physical root, causal revisions and predecessor CAS; a restart
  resumes rather than rescanning the accepted prefix. A tombstoned owner with
  surviving vectors or a raw imported flag remains pending. The completion
  adapter consumes this closure, including native-only catalogs; it does not
  bypass independent projection, resolver or promotion requirements.
  Artifact-only mutations now reopen the owning document's work exactly once,
  preserving its primary input position while advancing its work revision.
  This prevents new scopes from hiding behind an earlier completed obligation.
  Maintenance runs the census even with no provider templates; progress requests
  another bounded turn, while missing evidence backs off. Fault tests cover
  130 dense/sparse vectors, cancellation, allocation failures, cursor races,
  restart, exact discharge and reopening after an uncertified vector appears.
- Current receiver-local authored evidence also closes the corresponding
  document-vector provider requirement and suppresses provider execution.
  Both paths bind the exact source and physical root, not the origin flag.
  Dense/sparse and batched-dense callback regressions require zero inference
  or publication calls for accepted authored vectors. Generated retry checks
  now require current provenance rather than a transport receipt that may
  outlive its output. Closure commit checks only fixed metadata fences and the
  accepted record version. Historical receiver adoption, non-document provider
  scopes and ordered all-required discharge remain separate activation gates.
- Coordinated resolution and ordinary ingress share one borrowed commit
  participant interface through backend type erasure. Attachment must precede
  mutations, retries reset observations, and the final callback can write only
  private metadata after physical source revisions are stamped. Unsupported
  backends reject participation. Callback failure rolls back the decision,
  primary writes and acceptance together; terminal lost-reply retries do not
  replay postimages or re-certify them over newer rows. Native/Raft fault tests
  and document/typed-row transaction API tests cover these boundaries. No
  participant, preparation pointer or callback is persisted as durable work.
  The DB regression reopens after durable intent preparation, resolves from
  recovered intents, waits for projection, and reopens again to verify local
  acceptance on both document and typed-row tables.
- Current-epoch authored acceptance retirement scans metadata only, with
  cold-cache admission and bounded 128-record/64 KiB/2 ms pages. Its durable
  cursor advances across live records, so a live prefix cannot starve stale
  receipts. Preparation releases the reader before the writer; commit repeats
  exact-record, artifact-revision, authority, root and cursor checks. Renewal
  racing cleanup therefore survives, and primary-only changes retain the
  authoring evidence of an unchanged output. Completed sweeps are cached at
  the durable native/Raft mutation clock. Mutations during a sweep force a
  follow-up pass (including keys behind the cursor), while idle maintenance
  performs point reads without reopening the certificate scan. Regression
  coverage includes a 257-certificate live-prefix/churn fixture, aborted
  cleanup, competing cursors, racing renewal, pinned readers, byte limits,
  native/Raft clock invalidation, epoch/root fences and restart. This is local
  reclamation, not historic provenance adoption or native-stream closure.

The ordered-artifact regression target covers upload refusal without false
success, restart/resume, pending-coverage retry without retransmission, accepted
graph provenance/coverage, exact standby control identity, and bounded terminal
retirement. Baseline coverage includes two owners with different local work
records, enqueue refusal, restart, duplicate pages, behind-cursor writes and
continuous tail growth. It also fills the producer-upload quota on both owners
before uploading/finalizing the baseline through reserved control capacity.
Wire tests cover binary row keys, control-class tampering and allocation failures.
Validation regressions cover two-owner replay, mutation racing an empty page,
new-leader discovery, duplicate apply, stale graph projection repairs, binary
repair keys, allocation failures, and standby control identity.
Obsolete-work reclamation tests preserve pinned readers and active counts while
removing a multi-page old epoch; cursors from that epoch cannot drive current work.
Upload recovery tests cover restart with pending coverage, queue refusal,
duplicate hints, stale incarnation fences, and payload-free fair discovery.
Runtime tests saturate producer admission while admitting a finalize hint, and
verify scheduler refusal and shutdown release the exact queue reservations.
These are component guarantees, not deployment activation proof.

Focused checks (from `zig/`, with `-Doptimize=debug`):

- `zig build antfly-ordered-artifact-test`
- `zig build antfly-retained-transfer-test`
- `zig build antfly-data-runtime-test -- --test-filter 'data runtime ordered artifact upload handoff'`
- `zig build antfly-storage-native-fk-test`
- `zig build antfly-standalone-initial-fk-test` (the six native FK/TRUNCATE
  integration cases require this storage-owner-linked target, not the general
  standalone runtime unit-test target).

Before advertising the expanded protocol, finish and validate:

1. Enumerate and schedule every required producer stream from the immutable
   catalog, including unit/chunk scope and shared-output reconciliation. Clear
   each exact-input obligation only after those streams are complete. Bounded
   receiver-verified completion controls and local work-driver submission are
   wired; complete the missing requirement verifiers and the all-member drain/seal
   coordinator (bounded replicated provenance validation is wired). Replicated baseline scan
   completion alone is not an activation certificate.
2. Complete chunk/unit/document-extraction, neighbor-context asset, and promotion
   producer preparation and actual callbacks (root chunk callbacks with an
   existing or receiver-verified reconstructed inventory, and direct/accepted-upstream asset callbacks are wired),
   including remaining input proofs and absence cleanup. Direct document-vector
   authored certification now bypasses inference; historical/adopted authored
   output still requires receiver-local adoption evidence.
3. Historic provenance carried with retained effects and snapshots, atomic
   receiver-local adoption certificates, and contender/promotion recovery.
   The direct-index ordered adoption participant is wired and tested locally,
   but its bounded coordinator, distributed retry outcomes, other producer
   families and admission/activation barrier are not complete.
4. Complete scoped regeneration through the required-stream driver and all-member capability/catalog
   barriers, followed by the distributed crash, lost-reply, leadership-change,
   and standby-promotion fault matrix.
   Root retries now derive identity from the existing exact-input obligation
   and immutable plan, not from transport staging. Finish regeneration for the
   remaining scoped producers and prove it across incomplete/retired uploads,
   membership changes and standby promotion. A retry is not stream completion
   or all-member evidence, and must not replace those barriers.
