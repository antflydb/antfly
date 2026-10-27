# Experimental immutable document and field storage

Status: **deferred from PR #1263; not qualified for automatic upgrades**.

The implementation is preserved on `experiment/immutable-document-format`.
Fresh-table migration now requires the internal, explicit
`DocStore.experimental_document_migration` permission in addition to qualified
readers. New source-family publication uses the same permission; indexes with
already-admitted source families retain their required publication semantics.
Reader capability alone does not activate a new format. Already-admitted
stores retain mandatory decoding, retention and retirement behavior. The older
`document_payload_prototype` switch remains an internal fixture mechanism.
Neither switch is a public API or a recommended production setting.

The intended eventual release may automate eligible writes, but the measured
small-row penalty and failed update-speed gate block that rollout. The historical
implementation stages below describe experimental capabilities and earlier
plans; statements about automatic migration are conditional on the split's
qualification criteria, not claims about released behavior.

Certificate reuse avoids re-decoding unchanged leaves, but a full PUT still
validates and hashes all submitted JSON. The immutable-value prototype in
`storage/document_payload.zig` and `DocStore.Txn.patchMetadata` separates that
cost from storage-level partial updates. It is behind an internal qualification
gate, disabled in ordinary stores. Production schema writes retain ASCH25; readers understand admitted formats
through ASCH29. Eligible-write promotion remains gated below. This is not a public opt-in:
automatic migration on eligible writes is the intended released behavior after
qualification:

- Small/write-once/changing rows retain inline JSON. A qualified write reusing
  substantial large values can automatically migrate a document to a compact
  manifest; no per-table opt-in or synchronous table rewrite is required.
- Values of at least 64 KiB use row-owned immutable keys in the same transactional
  KV store. Ownership is scoped by physical document key and raw value digest,
  avoiding a global reference counter. Reused values are neither loaded nor
  written during an eligible metadata patch.
- Logical authority uses the explicit `logical-json-v1` algorithm, independently
  of inline/external layout. Objects commit to sorted decoded names and child
  commitments, arrays retain order, and scalars share the pinned canonical-value
  ABI. The manifest's separate checksum commits to its physical parts. The
  manifest, algorithm-tagged proof, path directory, canonical certificate and
  mutation revision publish atomically. Public `sv1` identities retain their ABI.
  The writer reuses validated scalar commitments from its directory/certificate
  pass; opaque subtrees use the recursive reference implementation.
- The storage patch planner accepts bounded top-level field replacements and
  removals, with at most 32 KiB of inline JSON. It has no storage-reader or writer
  capability. It preserves external values and their canonical identities;
  touching a payload, nested external layouts or an attached index participant
  returns `Unsupported` before staging a mutation.
  Callers must use the established logical transform in those cases.
- A sharded decoded-manifest cache is bounded to 2 MiB and 64 entries. Leases
  prevent eviction of active plans; each hit verifies the current manifest
  checksum. It contains no payload bytes or authorization grants. Disposable row
  hints select a pinned read strategy, never replace the actual snapshot lookup.
  Batched physical range reads sort up to 64 immutable keys per batch and copy
  only overlapping ranges into the final output buffer. Metadata-only projections
  do not fetch external values.
- Ordinary reads use the same snapshot and backend checksum guarantees as inline
  JSON. Verified projections hash selected extents; explicit scrubs use
  `readVerified`. Full reads reconstruct directly into the owned-result allocator
  where possible. They do not repeatedly hash every unchanged external value.
- Existing engine MVCC retains older value versions for pinned snapshots. Blob
  retirement and manifest replacement use the same writer transaction. A compact
  sorted current-membership record avoids scanning the row or fetching payloads
  on deletion. Row-local durable references distinguish current ownership from
  retained-history pins; removing the last current reference cannot collect a
  value still pinned by history. Physical checkpoints preserve the complete
  keyspace; logical cursors reconstruct documents and skip all private value,
  ownership and membership namespaces.
- ASCH 26 is the reader capability fence. Opening an old store alone does not
  rewrite it; the first write using logical authority or publishing a manifest
  upgrades its schema fence atomically. Older schema writes cannot downgrade a store that admits manifests. Rollback to an
  older binary requires restoring a pre-upgrade physical snapshot.

Qualification now verifies equal logical authorities across inline/external
layouts, certificate disposal/backfill, restarts and pinned snapshots. The pinned
schema capability selects the algorithm; an absent disposable certificate cannot
select a different one. The internal authority envelope has an explicit v2
`algorithm: "logical-json-v1"` and `digest`; v1's `sha256` retains its old meaning.
Mixed algorithms do not compare equal, and a v1 transport cannot reinterpret a
v2 root. Full PUTs still validate submitted bytes, even when canonical commitments
and unchanged immutable values can be reused.

Retained history stores checksummed `RFS1` afterimage metadata and the exact
manifest at the mutation's cut. Its row-local pins retain immutable values across
current-row replacement, deletion and restart. Retention quotas charge each unique
pinned value once, plus each small frame, and reclaim releases that charge only
with the final pin. Frame publication, pins and mutation commit atomically.
Consumers use a bounded 1 MiB adapter to produce existing authenticated REF3/4/G
and RFV5 transfer frames. The adapter also checks each immutable value against its committed raw digest
while streaming, before publishing transport hashes. Its byte checksums are
computed on consumption, not invented from logical roots or paid on each metadata write. Receivers and resume
protocols retain their established semantics. Large inline afterimages fall back
to the existing chunk writer instead of reducing its accepted size limit.

Production activation still requires a persisted table/group capability after
all participating replicas and snapshot consumers support the format. The apply
barrier must publish a monotone catalog capability together with the schema fence;
new binaries cannot activate it from local cache state or binary version alone.
Qualification must include independently compiled storage/query owners: native
reader cells and non-serializable lifetime evidence cannot assume identical
module-local registry or function addresses across an ownership boundary.
Mixed-version routing and retained snapshots must honor that capability. Automatic
migration on eligible writes remains the intended released UX, with no public
opt-in. Opening an already-admitted document table now infers its native payload
read/retirement path from the decoded persisted schema before publishing the
core to workers. This reuses startup's existing schema read, adds no per-request
probe and does not promote a legacy table. Document cursors, logical identities,
validation provenance and subsequent transforms therefore no longer depend on
restoring a process-local qualification flag after restart. Relational schemas
retain their own row path even when their reader fence is 27. The production
reader maximum and new-admission gate remain unchanged pending cohort rollout. The internal qualification gate remains until this activation protocol
and the public mutation pipeline below are implemented and qualified.

Indexed-source ownership initializes independently of that shared reader fence.
A table can already require reader 27 for scoped producer proofs or logical
import without having any indexed-source budget. First admission atomically
creates its budget and a fixed initialization witness after checking that no
claim, recipe, value-set or recovery record is orphaned. An existing budget is
validated and preserved. A missing budget after initialization is corruption,
including after all live claims have been reclaimed; it never resets ownership
charges. Bulk preparation borrows one native write transaction and amortizes
schema admission across its claims. Revision, uniqueness, recipe, lease and quota
checks remain per claim, schema changes invalidate cached admission, and failures
poison the compound transaction. Oversized current/retained recipes fail before
copy allocation; a zero-capacity allocation fixture verifies the current-row limit.

Logical cursor privacy also applies before the payload reader is configured.
Forward/reverse and keys-only traversal skip private ownership namespaces;
ordinary rows first take a reserved-namespace check rather than probing every
private prefix. An unresolved native document descriptor produces an error
instead of being exported as logical JSON. Physical maintenance cursors remain
the explicit route to ownership records.
Startup presence checks use a physical key-only lookup before payload readers
exist. Prefix/range key discovery and median-key navigation likewise avoid
value hydration. The logical key-only adapter returns empty values even when an
underlying adapter lacks a key-only primitive; it still enforces metadata privacy.

Public batch transforms now carry a native prepared physical publication through
coalescing, extraction, overwrite probing and storage commit. Eligible local
native-format writes also carry private schema-validation effects. A bounded resolver
keeps untouched root values as validated JSON spans and decodes only changed
roots. The public pipeline still supplies complete logical JSON to indexing, observers,
journaling and replay; validation can use the bounded plan described below; it is not a constant-cost public
transform path. Completing that goal requires a change-aware mutation plan: reuse proven index artifacts for untouched fields,
validate changed paths against the schema, retain immutable references in change
history, and preserve pending writes, predicates, timestamps and replay semantics.
An optimization must fall back when a whole-source operator or affected index
requires the full document; it must never bypass indexing or authorization.

The public mutation integration must carry a typed prepared change through the
existing pipeline rather than pass a manifest or a partial object as complete
JSON. Its long-term contract is:

1. Pin the source revision, schema/rule epoch and immutable index generation.
   Resolve same-request ordering and normalized transform paths against that
   exact cut. A prepared change owns its new root, changed paths, preserved
   immutable descriptors and bounded logical field projections.
2. Persist validation provenance bound to the logical root and schema/rule epoch.
   Reuse it only for unchanged paths. Missing provenance, schema changes,
   cross-field validation and whole-source operators require the reference path;
   omitted fields must never be interpreted as absent required fields.
3. Compile dependencies per index and producer, including template, chunk,
   completion and resolver dependencies. Plan affected targets only; retain
   untouched index membership and derived artifacts. A global document input
   revision cannot invalidate preserved producer inputs: provenance needs
   separately scoped producer input revisions, or a conservative full fallback.
4. Carry those target decisions through extraction, the commit journal, replay,
   cloning and background consumers. A partial base update cannot masquerade as
   a full upsert that clears every index. Affected consumers receive the complete
   logical fields they require; root consumers request complete source explicitly.
5. At the existing apply fence, recheck source revision and schema/index epochs,
   then atomically publish the manifest, authority, validation provenance,
   timestamps, target effects and retained pins. Retries rebuild the plan. The
   public transform/PUT API and authorization semantics remain unchanged.

Native document validation now has a durable, disposable certificate (`DVF2`).
The common writer binds it to the actual source logical root, exact final physical
manifest, row identity, schema version and validator-rule fingerprint. A
root-local HMAC key is domain-separated from the private signing identity; neither
that seed nor the key enters the LSM, public JSON, journals or portable exports.
Certificates copied to another root are cache misses. A checksum alone is not a
validation attestation. Malformed records, schema changes, missing certificates
and unsupported rules retain complete validation.

For a composable document schema, the native preparer compares authenticated root
member commitments, parses only changed values and the `_type` discriminator, and
constructs an internal validation object containing every real root name. Presence,
required/dependentRequired, property names and cardinality rules still run over
that complete name set; unchanged values are skipped only under the matching
certificate. Changed subtrees run the ordinary recursive validator, including
nested compositions. Root compositions, dependentSchemas, SQL expressions/checks,
normalizing validators and unknown whole-source rules retain full validation.
This internal object never reaches extraction, observers or an index builder.

The native local admission boundary pins the immutable schema before preparation
and rechecks it under the apply fence. Its validated-byte capability crosses the
existing private commit participant; the writer must match those exact logical
bytes before certifying the actual stored afterimage. Certificate publication,
source revisions and document bytes share one transaction. Failed admission or
retries publish no certificate. Certificates grant no source-membership,
authorization, ownership or reclamation rights.

Ingress reuses the installed immutable compiled validator instead of reparsing the
schema for each batch. Its existing pre-proposal validation remains: committed
Raft entries must not be reinterpreted under a newer active schema. Removing that
last repeated transform pass requires a generic durable admission binding shared
by proposal, ordered apply and recovery. Unsupported/replayed paths retain the
complete logical source and do not manufacture validation capabilities. Automatic
production activation remains gated on reader/peer qualification.

Trusted schema publication preserves an already-promoted physical reader fence,
including DDL followed by restart. If a document write promotes the root after
schema preparation, publication rejects the stale candidate for re-preparation
against the new fence. Logical schema and
integrity fingerprints stay independent of that root-local physical version.
Low-level logical schema replacement still cannot silently downgrade the fence.
`document-validation-test` checks the reader fence, bounded validation, stale and
forged attestations, publication failure poisoning and allocation-failure cleanup.

Prepared transforms also fence the exact document mutation revision, not just
its timestamp. Reusing an internal timestamp or deleting/reinserting identical
JSON must not revive an old prepared read set. Native source roots are verified
in sorted windows of at most 64 owners, using fixed metadata scratch and no body
reads. Missing or malformed disposable content caches fall back to the small
physical manifest; malformed mutation-revision authority remains an error.
A writer that sees a changed source root rejects the stale plan for reprepare.
A representation-only rewrite of the same logical document can still use the
complete-value fallback, while old raw references remain bound to their exact
manifest footer. The source checks and existing timestamp predicates both apply.

The native prepared receipt owns the new manifest, directory and commitment
certificate. It binds both the source logical root and the exact source manifest
checksum: equal logical content cannot authorize references from a retired
physical representation. A bounded native change planner interprets only touched
inline roots from authenticated source metadata; it has no payload-read capability.
It preserves immutable descriptors and computes the new logical root, directory
and certificate before materialization. Same-document operations share one plan
in request order, including interleaved wide batches. Rename, time-dependent,
reserved-root, oversized and payload-touching operations conservatively retain
the reference path. Reference-path transform results can also verify a prepared receipt against
preserved source spans. The existing apply fence rechecks the
receipt, and any mismatch uses the ordinary full-write path. Receipts are private
sidecars carried by the existing native commit participant. KV pairs retain their
original two-slice layout and ordinary JSON codec, including its SIMD fast path;
there is no receipt field to serialize or deserialize. Strict JSON rejects an
injected field, and permissive streaming/value-tree decoding cannot construct a
receipt. Replay and clones retain a complete logical fallback. The preparation
hook composes with the existing observer/stamping callbacks and grants no
publication or authorization rights. Snapshot metadata lives in a sparse sidecar,
so ordinary unactivated stores allocate no per-row payload preparation records.
Wide batches group applicable transforms once and binary-search sorted receipts;
small batches keep the direct lookup plan. Positive native changes never
reconstruct or scan the old complete source: capture reads the physical manifest
once from the same MVCC cut as the source version, plans against its inline
metadata, and renders the final complete result once. Multiple transforms of that
document share this rendering. Opaque extraction borrows the request-owned result
through commit rather than copying it again. Fallback capture reconstructs directly
into request-owned buffers, avoiding another full logical copy owned by the read
transaction. Prepared sidecars must outlive coalesced and extracted consumers;
only the snapshot owns their bytes. The read transaction closes before downstream
validation and indexing. This is a compatibility boundary, not yet a fully lazy
consumer pipeline.

Owned point reads retain that same discipline: the storage read resolves a
manifest and its immutable values under one pinned view into the caller's final
buffer. The DB layer transfers already logical JSON through its owned
materialization API instead of copying the complete result again. Typed rows
still decode against the pinned schema, and ownership transfers on both success
and error; TTL and row-policy fences retain their checks before return. This
removes an allocation/copy for ordinary JSON documents as well as native payloads,
without a new cache, format choice or extra I/O. When a pinned backend supports
sorted reads into caller buffers, reconstruction opens a disposable scope from
that same view and fills whole immutable extents directly into their final JSON
positions. Batches remain bounded to 64 sorted keys; duplicate-key aliases are
copied into their respective destinations. Partial-extent reads retain ranged
I/O, and unsupported adapters keep the original transaction-owned path. Decoder
scratch is independently reclaimed rather than retained with the returned value.

Physical-disposition cache bits remain hints. Preparation reads the actual
manifest, and retirement checks transactional current membership before treating
a row as inline. A stale hint must neither lose an optimization nor orphan blobs.

New prototype manifests retain a unified `DLC2` field tree: the bounded directory
is serialized once with canonical digests for every retained node. It preserves
the existing layout-independent logical root; byte offsets and raw-value digests
belong to the physical manifest commitment. Object digests are recomputed from
complete children on admission, and readers check the tree's byte length against
the manifest. Old `DLC1` commitments remain readable. Borrowed navigation copies
neither names nor canonical digests; only the bounded descriptor array is owned.
Metadata patches graft unchanged object branches into the new layout, rebasing
ranges and packed parent indices without reading immutable payloads. Structural
paths preserve literal dotted keys. Width/metadata limits retain opaque subtree
commitments rather than manufacturing complete coverage. Changed opaque branches
retain the existing bounded subtree interpretation. The patch and source-metadata
capture paths derive scalar certificates from this immutable tree, so missing
disposable directory/identity caches do not force a payload read or prevent a
qualified patch. Existing consumers may still use those disposable records.

A captured indexed source can now read selected structural paths from its own
claimed manifest. Missing fields are proven absent only through complete objects;
arrays and opaque objects return their covered ancestor for the ordinary query
projection interpreter to finish. Selected values are authenticated against raw
field digests, and results include their canonical identity and covered depth.
Duplicate selections and selected descendants of another selected field alias one
final output range. Native reads open a disposable scope from the same pinned
primary view, release its decoder scratch before return, and preserve the ordinary
read adapter when direct scopes are unsupported. Path count and selected byte budgets are checked before body
I/O. The implementation never consults the latest primary row or its disposable
metadata. Older/inline recipes explicitly request complete-source fallback. Native
fixtures cover metadata patches after removing the disposable records, source
projection after primary deletion, literal dots, arrays, missing/scalar traversal,
overlapping selections, output limits and allocation-failure cleanup. This is the
selective source-read primitive, not yet ordinary query-driver integration.
`document_query.lookupClaimedSourceWithPlan` adapts those ranges to the shared query projection
interpreter without a value-sized JSON DOM. It preserves ordered parent/child
selection, empty object ancestry for missing values, numeric array selection,
fanout and exclusions. Differential fixtures compare it with complete-source
projection, including allocation failures. A native read session loads and
decodes the authenticated recipe once and borrows the same primary read view and
reader lease through completion. Unsupported, over-budget, inline and
whole-document projections use that session's complete-source fallback, without
reloading its recipe or consulting the latest document. The session checks the
actual lease before each operation; it does not extend its caller's lifetime.
Fixtures verify one snapshot-recipe read through projection fallback and reject
operations after lease release.

Ordinary query integration must carry the indexed publication/local document
identity through candidate selection and residual value predicates, not recover
it later from a public document ID. Acquire the source root before opening the
primary ownership read view. Residual predicates and delivered source fields must
refer to that same indexed revision; current visibility, TTL and authorization
retain their own read-barrier checks. Substituting an old excerpt only after
filtering against the latest document would still mix revisions. Hydrate only the
surviving result window, retain the claimed root across deferred delivery, and
route remote storage through the source owner's lifetime/authority boundary.
The projection adapter alone does not implement this query-wide contract.

Current correctness qualification passes 20 source/projection checks and 35
payload checks (one optional benchmark skipped) on native Debug and constrained
ARM64 Linux ReleaseFast. The publication/recovery suite passes 25 checks on native
and Linux Debug, including the existing DB maintenance API. Native validation,
transform and provider suites pass 6, 13 and 9 checks; scoped producer/proof tests
pass 35. Broader native persistent-index regression passes 67 checks with one
optional benchmark skipped. These counts include their selected module-root
checks and overlap between suites. This is correctness/lifetime qualification;
no new end-to-end throughput or peak-memory claim is made for this head.

Native local validation provenance now implements the composable-rule portion
of step 2. Generic durable admission binding, rule-level reuse for whole-source
compositions, and scoped index/producer effects remain work in steps 2–4; they
are not satisfied by merely skipping index work.
Current public writes preserve established validation and index maintenance,
including fresh stored source in full-text results. Whole-source and unknown
dependencies conservatively require complete source.

The large remaining full-text cost is a separate consumer-format problem. A
metadata change must not rebuild untouched body postings or duplicate the whole
stored source. Retaining postings requires field-scoped generations and input
provenance, rather than advancing one document generation that invalidates every
field. Native stored-source references must bind the exact indexed revision and
logical root, with durable claims on its immutable values; hydrating the latest
primary row would mix index and source revisions. Reference claims must cross
outbox publication, retries, replay and index retirement atomically, with bounded
recovery and reclamation. Reuse the retained-recipe/ownership primitives and keep
verified logical fallback bytes for unsupported consumers. This needs its own
publication and compatibility proof before replacing current full maintenance;
merely omitting unchanged fields from an upsert would lose postings or freshness.

The next consumer format must preserve logical document identity across field
updates. Store immutable field generations beneath a segment-family root, with
unchanged field roots reused and changed postings/doc values replaced together.
Keep document ordinals stable; never append partial documents as independent live
hits. Each snapshot pins its exact family root, deletion masks, norms, position
metadata and source map. A family-root CAS must check the source/index generation
used by preparation. Compaction may flatten bounded deltas, but cannot resurrect
replaced field generations or release bytes still owned by an older snapshot.
Catch-all `_all` additionally needs per-source-field contributions, position
lengths and scoring metadata; stateful/cross-field analyzers retain full rebuilds
until their dependency rules are represented. Merely keeping old body postings
while replacing `_all` with metadata postings is incorrect.

The incremental family implementation must also separate locator scaling from
ownership reuse. `ISM2` bounds preparation memory and lookup depth, but its nodes
are publication-bound; it does not make a one-document source refresh constant
work. The incremental contract is:

- Store immutable, content-addressed source-directory nodes beneath each family
  root. A leaf covers a bounded ordinal cohort and owns its own exact claims.
  Updating one cohort copies its leaf and directory path, with no reference to
  an older family root and no cloning of unrelated claims. Unchanged postings
  and source subtrees remain direct shared children. The family root CAS binds
  the ordinal inventory, field roots, deletion generation and source-tree root.
- Count durable incoming node edges and active-root references in the index
  transaction. Creating a new node increments its direct children once; sharing
  a child never walks its descendants. Reclaim zero-reference nodes with a
  durable bounded frontier. Query readers retain their actual family root until
  capture finishes, so old roots remain reclamation owners until reader drain.
- Keep leaf claim identity independent of a changing family publication. A
  query authenticates the path under its held family root, then acquires the
  actual leaf reader lease before releasing the parent. Selected-row pages
  share that leaf lease. A batch crossing leaves returns multiple bounded
  cohorts. Primary reads use the leaf's exact receipt and claim binding; a
  copied family digest cannot authorize a leaf or a historical primary row.
- Prepare newly needed primary leaf claims and their journals durably before
  publishing the family root and edge updates in the separate index store.
  Recovery distinguishes an unpublished leaf from a published child still
  reachable through any live root. Once its final edge disappears durably,
  retire the leaf, drain its readers and reclaim its claims. Never release a
  whole earlier publication merely because one document was replaced.
- Preserve stable document ordinals across field updates. `_all` needs explicit
  per-source-field positions, lengths and norm contributions. Cross-field or
  stateful analyzers keep full rebuilds until their input rules are represented.
  Ordinary mutation/outbox/replay must carry the prepared change and publication
  journal, including idempotent retry and source-configuration revision checks.
- Reserve bounded replacement headroom before steady admission. A hard ownership
  quota applies to held old generations too; compaction cannot invent a credit
  that lets those generations escape accounting. Near-quota and held-reader
  fixtures must demonstrate bounded memory, safe backpressure and eventual
  reclamation after release.

The native metadata ownership primitive now exists in `immutable_graph.zig`.
It stores owner-scoped, content-addressed nodes, with up to 64 direct children
and 4 KiB of metadata payload. Branch levels strictly descend to leaves; family
roots are level 7 and cannot be children, preventing chains of historical family
roots. Creating a node retains its direct children once; reusing the same node
does not recharge it or walk descendants. Root CAS stages an incoming root edge
and archives the old edge in the index transaction. The archived edge remains
charged until the native retirement decision and matching reader-drain proof
release it. Admission reserves the queue and longer retired representation, so
removing a current root needs no additional headroom. Missing initialized budgets
fail closed, including after complete collection.

`replacePath` rebuilds only the selected path from a held native root, with at
most seven ancestors and one fixed child buffer. Replacing an existing identity
with itself performs no writes. The deeper-tree fixture replaces one leaf and
creates exactly three nodes; sharing an unchanged directory adds one incoming
edge to that directory without changing its descendant's count. The caller
stages the resulting family receipt and root CAS in the same index transaction;
a stale publication plan aborts newly staged nodes and edges too.

Collection reads at most 128 candidate IDs, closes its cursor before mutations,
and advances a durable bookmark without recursive deletion. Blocked leaves do
not starve other candidates. Leaf adapters observe idempotent native resource
retirement completed outside the index transaction; metadata GC alone never
authorizes releasing primary source values. Checks cover shared descendants,
stale CAS, partial edge-write rollback, quota failures, blocked retirement,
restart/resumption and complete budget reclamation.

`ISP3` adds an explicit family receipt role and requires an epoch-bound birth and
one family identity. `ISP1`/`ISP2` retain the segment/map role. Equality and native
decisions preserve the role; flat source capture, acknowledgment, handoff and
retirement reject family decisions. Receipt collection retains a family decision
while its graph edge is archived, even after readers have drained. The real index
fixture verifies this ordering independently of primary claims. Held metadata
navigation opens from the actual native family lease and persisted receipt,
follows authenticated descending child IDs, and continues to select the old
family after replacement. It grants no primary/source reads, rejects JSON
serialization/construction and fails after the borrowed lease is released.

The native qualification path now includes fixed source cohorts in
`storage/index_source_family.zig`. Each cohort owns at most 64 claims, a source
map and an epoch-bound `ISP3` source-leaf receipt with no physical segment IDs.
`ISJ2` journals distinguish these jobs from legacy segment jobs; the primary
reader fence is ASCH28, and ISO3 fences incompatible index owners. Production
schema writers/readers remain ASCH25 until the complete consumer is qualified.

A source-family root binds the physical segment, ordinal count and configuration
identity. Its bounded directory navigates to an authenticated cohort descriptor;
only the actual family lease can begin that navigation. A leaf's independently
acquired native lease authorizes its source reads. It cannot be replaced by the
parent's digest, a JSON descriptor or a current-primary lookup. Query caches use
cohort-relative windows, while returned result pages preserve original result
order across several cohorts. Structural merge/prune handoff translates those
windows back to global segment ordinals and retains stable leaf leases through
primary preparation. Legacy maps still occupy one capture slot.

Initial native publication forces all prepared claims before atomically publishing
leaf receipts, maps, graph resources, the family root and catalog binding. A native
snapshot preparation stages every fallible allocation before that catalog commit.
A bounded multi-cohort refresh uses one primary durability barrier, one prepared
writer snapshot and one family-root CAS for up to 64 sorted, distinct cohorts.
The graph stages disjoint paths beneath the actual held root, copying each shared
ancestor once. Empty and identity path batches write nothing. Invalid, duplicate,
overlapping or unsorted paths poison the index transaction. A failed later cohort
cannot expose earlier staged leaves; retry succeeds only if every requested leaf
is already present under the current generation. Old family and selected-leaf
readers continue owning their original sources. Larger changes retain the full
rebuild fallback until their atomic publication protocol is represented. This
primitive does not establish that analyzer inputs stayed unchanged.

Source-only cohort replacement inherits the physical identity from the actual held
family: it does not rehash the unchanged posting artifact. It prepares only the
selected cohort and copies its bounded ancestor path, preserving every other
child directly. The physical segment and reader remain shared, without historical
physical-parent or snapshot chains. A retry of the already installed cohort
returns its actual current native family decision.

Resource reverse edges are budgeted and retained until source cleanup completes.
They prevent direct retirement of reachable leaves and receipt collection before
the external resource has been released. The maintenance owner enumerates archived
family roots in bounded windows, drains their canonical reader cells, then releases
edges. Zero-edge source leaves are durably retired before primary cleanup runs
outside the index transaction. After primary completion is forced, a fresh index
transaction rechecks the leaf, its reader drain and absent map before deleting the
reverse edge and graph node. Closed-owner cleanup also waits for graph completion.
Unknown future field-resource types remain blocked for their own typed cleanup.

The disk-backed native fixture verifies cross-cohort reads, source-only replacement,
old snapshot and selected-leaf survival, sharing, retry, reclamation and reopen.
It can measure these phases with the production allocator:

```sh
cd zig
zig build source-family-bench-build -Doptimize=ReleaseFast
cd ..
python3 scripts/bench/source_family.py \
  --binary zig/zig-out/bin/antfly-source-family-bench \
  --output output/source-family --trials 7
```

To compare the atomic multi-cohort primitive with sequential publication:

```sh
zig build --build-file zig/build.zig source-cohort-batch-bench-build -Doptimize=ReleaseFast -j1
python3 scripts/bench/source_cohort_batch.py \
  --binary zig/zig-out/bin/antfly-source-cohort-batch-bench \
  --output output/source-cohort-batch --documents 131 4099 --payload 0 4096 --trials 7
```

Both conditions use the same source histories and modify metadata in the first
and last source cohorts. They verify current delivery, held old-reader bytes,
unchanged postings and one versus two durability barriers. Publication time
excludes source preparation; CPU/RSS include setup, retry verification and
cleanup. The runner hashes the frozen binary, randomizes conditions, records
per-run logs and refuses nonoptimized performance results. The two-cohort fixture
is not evidence for arbitrary field reanalysis or automatic mutation eligibility.

The 2026-10-25 macOS ReleaseFast comparison completed 56 randomized fresh-store
runs (seven trials per condition) with binary SHA-256
`227662a3c1569573d1873b78e71abf01c575effe4b283822f3255069dd6d1067`:

| Documents | Padding bytes/document | Atomic publication median | Sequential publication median |
|---:|---:|---:|---:|
| 131 | 0 | 1.522 ms | 2.743 ms |
| 131 | 4,096 | 1.536 ms | 3.009 ms |
| 4,099 | 0 | 2.334 ms | 4.103 ms |
| 4,099 | 4,096 | 3.343 ms | 4.520 ms |

Atomic publication won 6, 7, 5 and 6 of the seven matched trials respectively.
These are observed publication gains of 1.35–1.96× on this host, not universal
throughput claims. Whole-fixture CPU and peak RSS were similar: setup, input
construction and cleanup dominate those totals. Both conditions used identical
source histories and retained-reader checks. Evidence is in ignored
`output/next-pass/source-cohort-batch-release/`; no generated report is required
in the source tree. The correctness suite exercises rollback of an invalid second
cohort after the first was staged, and the graph test verifies that two leaves
beneath one directory create exactly one successor directory and one root.

The focused target compiles only the storage workload and strips optimized debug
information; broad correctness suites remain separate debug builds. The runner
records binary SHA-256, host details, randomized trials, CPU, peak RSS
and separate native preparation/publication/update/read/reopen/reclamation/drop
timings. Its parser requires successful source, sharing and cleanup assertions.
The synthetic documents share repeated ASCII padding and differ in ID and
metadata; only `body` is indexed. Each trial changes metadata on the last
document and replaces either a full 64-document cohort or a three-document
tail. This isolates source refresh, not field reanalysis or distinct real-world
payloads. These are native storage measurements; whole-fixture CPU/RSS include
input generation, setup and teardown. They do not qualify the public mutation/outbox pipeline or establish a
speedup without a comparable baseline. Reports and logs remain under ignored
`output/`.

Qualification on 2026-10-25 passed 50 publication/recovery checks and 28
claim/map checks on macOS and in the ARM64 Linux VM (two CPUs, 1 GiB RAM), with
no leaks. The native query/transport contract suite passed 157 checks; report
validation passed four Python checks. The benchmark completed 70 macOS runs
(seven trials per case) and 18 Linux confirmation runs (three per case). A larger
Linux matrix was explicitly interrupted and is not included in these counts.

Representative medians for zero padding are below. "Update" is the median of
preparation plus publication within each run, excluding later reclamation;
separate medians must not be added to reconstruct this statistic.

| Corpus documents | Replaced cohort | macOS update | macOS publication | Linux VM update | Linux VM publication |
|---|---:|---:|---:|---:|---:|
| 128 | 64 | 3.47 ms | 1.61 ms | 32.85 ms | 21.29 ms |
| 131 | 3 | 2.73 ms | 1.45 ms | 24.04 ms | 18.76 ms |
| 4,096 | 64 | 8.31 ms | 2.42 ms | 41.86 ms | 22.32 ms |
| 4,099 | 3 | 58.21 ms | 2.03 ms | 92.27 ms | 15.09 ms |

The small tail at 4,099 documents exposes primary preparation costs despite
bounded graph replacement. With 4 KiB padding its macOS preparation median was
146.89 ms, while directory publication was 2.56 ms. This is not a zero-cost
update or a demonstrated improvement over a comparable previous public pipeline.
Preparation needs further profiling into admission, source/recipe reads, commit
and durability waits; aggregate timing does not establish which is responsible.
The VM uses a host bind mount and does not establish native Linux hardware or
cold-cache performance. Whole-fixture median RSS includes corpus inputs/setup:
roughly 17–123 MiB on macOS and 20–78 MiB in the measured Linux cases.

Ignored local evidence is in `output/next-pass/source-family-native-release-matrix`,
`source-family-native-release-large`, `source-family-linux-confirmation` and
`source-family-linux-large-confirmation`. Each contains binary SHA-256, host and
resource limits, per-run timings and logs. The initial source layout is a
qualification prototype; these measurements do not activate the public writer.

Source-only eligibility must come from the compiled consumer's closed logical
input dependencies, checked against the held indexed source. A shared physical
identity alone does not establish unchanged analyzer inputs. The native fixture
uses a known body-only consumer; unknown, computed or cross-field dependencies
must keep their full rebuild until represented.

The qualified JSON foreground and outbox/replay consumers share source-family
preparation and publication. Collection holds one stable native MVCC read cut,
compares the actual mapper bytes against that cut (bounded scratch for immutable
payloads), and attaches a nonserializable native observation to each ordinary
JSON source. Preparation imports claims from that held cut even if current rows
advance or old values are deleted; a caller-supplied revision/digest or a live
probe cannot replace it. Output IDs resolve source coordinates after projection
filtering and index sorting, using a batch-sized lookup rather than a corpus map.

Immutable files and primary claims are prepared and forced before acquiring the
managed-index apply guard. Preparation hashes the retained immutable mapping once
and carries its native content stamp through apply; publication checks the same
source capability instead of rescanning posting bytes under the guard. Heap
encoder buffers are then released. A prepared
family owns its file mapping, bounded source-leaf preparations, publisher
registrations and a retained storage-cleanup context. It borrows no collector or
movable catalog entry after preparation. Schema invalidation or removal can
abandon the prepared output safely; pending claims follow normal cancellation
recovery. Reserved file IDs are durably advanced before handing out prepared
ownership, so a reopened physical root cannot reuse a path still owned by a
late discard. This durability barrier belongs to preparation, not apply.
First-owner reservation converges under one storage lock for concurrent producers. Publication atomically installs the previously invisible physical
segment, source maps, receipts, family root and catalog binding. Every fallible
snapshot allocation precedes that catalog commit. Query visibility and
authorization still follow current authority; retained bytes do not resurrect a
deleted document.

Claimed-source delivery has a separate read policy from producer capture.
The selected row's actual native publication lease protects its immutable claim
recipe and values until reading finishes. A nonserializable reader checks that
lease before every raw lookup, rejects current document/relational/group rows,
and can borrow immutable values through a scoped primary probe. Point loans require a completed selection of at most one 64-reference source-map
page and a backend cost hint. Unknown backends decline. The LSM backend declines
when a reusable mutable snapshot, SSTs or sealed memtables are present; for an
uncached all-mutable layout it compares estimated point-lookup work against
copying the mutable entry set. This hint chooses an I/O strategy only: it never
authorizes a source read or replaces the publication lease.
Bulk delivery and predicates evaluated before selection use a stable snapshot,
which amortizes lookup work without accumulating one point lease per result.
A small selected result need not clone the complete mutable primary state.
Native provider context checks and source-owner lifetimes still surround delivery.
Legacy delivery keeps its stable primary snapshot, and producer observations
still require a true retained read cut. A live probe alone never authorizes an
indexed afterimage or a negative proof about current rows.

Current correctness qualification covers 64 publication/recovery checks on both
macOS and the constrained ARM64 Linux VM, with no reported leaks. The earlier
29 claim/map checks also passed on both platforms; 157 query/transport checks
pass on macOS. Top-10 smoke conditions on each platform verify actual reader selection,
forced snapshots, legacy controls and retained source bytes. The family, replay and cohort-batch benchmark parsers have fourteen contract
checks, and cache-cleanup activity detection has four. These checks qualify the
consumer paths exercised here, not the unreleased migration/transfer protocol.

This consumer requires a primary already admitted to ASCH28 and the native payload
qualification mode. Released schemas stay ASCH25. Inline/legacy replay retains
its existing probe and buffer-transfer path; unsupported artifact/typed sources
do not receive a fictitious JSON observation. Source-only refresh without
rebuilding postings for partially updated inventories, automatic eligible-write
admission after reader-cohort qualification, and complete distributed lifecycle
qualification remain production prerequisites. Native field resources, their
closed input calculator and the shared producer route are implemented below.
The replay fixture exercises collector, mapper, publication and public DB search
across source cohorts, after current rows change and the collector's native cut
is released. Separate native claims tests cover replacement and deletion before
claim import. A staged-family fixture checks discard after its catalog owner
closes and eventual claim recovery.

Native split preparation now imports whole and pruned source families into the
receiver primary. It uses the actual retained donor index snapshot, maps output
ordinals back to that snapshot, and authenticates each source through its held
leaf lease. Imports decode at most one source recipe at a time, force receiver
claims before root publication, and do not overwrite current receiver rows.
Older flat source maps use the same bounded import path. Native posting, norm,
position and typed-value pruning preserves the paired source-coordinate map;
it does not reanalyze text. Range-backed files copy through a 64 KiB buffer.
Cold child construction infers payload decoding from the copied schema fence
before residual indexing. The public split fixture reopens the child and checks
claimed source delivery through ordinary DB search.

Generation-local transfer provenance is part of the active family root. An
unchanged retry reuses the committed import without another file or claim set.
Source-cohort replacement clears this provenance atomically with its root CAS.
Changed or compacted generations with overlapping live IDs fail closed; callers
must prepare a fresh unpublished destination. This is not an unlimited transfer
history or a read credential. Inventory construction scans the destination once
per text index, rather than once per imported segment. These local split checks
do not establish the full distributed merge, snapshot-install and format-rollout
protocol for the proposed field resources.

Foreground delete/backfill finalization schedules source-owned compaction through
the native merge task path. It must not invoke the legacy synchronous replacement
publisher, which lacks source-root handoff. The scheduled path prepares claims
outside the foreground apply fence and reconciles deletion deltas at publication.
This also preserves ordinary delete behavior when background workers are disabled:
the delete is visible and merge debt remains queued for an explicit maintenance
drain. Explicit forced source compaction continues to use native preparation.

`segment_composite.zig` and `field_pack.zig` provide the native range-reader
foundation for field reuse. Composite descriptors flatten direct immutable
resource ranges, deduplicate resource acquisition, and retain the native
provider's actual read authority. Pack metadata commits to 64 KiB page hashes;
opening a pack reads metadata, and sparse reads authenticate only touched pages.
Verified subsequent reads avoid rehashing. Allocation failures unwind resources
and provider leases. Pack identities are domain-separated Merkle identities,
not legacy whole-segment byte hashes.

`index_field_generation.zig`, `index_field_build.zig`, `index_field_source.zig`
and `index_field_inputs.zig` now connect those primitives to native persistent
field publication. An ISF3 logical family owns its source directory and an
independently retained IFD3 field directory. IFR1 leaves own exact pack files;
IFG1 nodes move every section of a field together, including postings, positions,
norms and typed values. The private stored-identity inventory retains document
coordinates separately. Native field receipts use role `fields` under an ISO4
owner fence. Copied hashes or serialized descriptors cannot acquire a lease.

The ordinary foreground/replay producer uses this route for a field-admitted
table: its durable ASCH29 fence automatically admits the index on the next exact
source write. Actual projected contributions and analyzer definitions form field input
identities. Inferred typed values use the same borrowed visitor as indexing;
integer/float discriminants, contribution order and canonical document coordinates
remain distinct. Durable configuration identities hash actual pinned definitions,
not an in-process revision counter that resets on reopen. Full PUT still parses,
validates, projects and builds the submitted document; skipping that compilation
for a partial edit requires a narrower native producer plan.

A weak ordinal-inventory directory finds a prior field publication in constant
metadata lookups. It grants no ownership: reuse acquires the actual field lease
and reselects each field from its held root. An unchanged section retains a direct
immutable edge and contributes no bytes to the new pack. Admission flattens before
pack fanout can exceed the reader's bounded resource count. A metadata-only cost
check also repacks when surviving unchanged sections would retain a pack at less
than half utilization. It does not open pack files to make that decision. The
threshold is a bounded default, not an optimal policy proven for every workload.

Before file creation, the persistent coordinator forces its ID high-water mark
and an exact-name preparation journal. The native exclusive writer streams to
that reserved immutable filename without an untracked temporary path. Forced
packs and primary claims precede the atomic field receipt, logical family CAS,
active catalog, deletion mask and preparation-marker removal. Reopen resolves
the field catalog marker through the normal authenticated segment decoder. Pack
metadata/body decoding stays lazy, but admission captures actual immutable inode
versions immediately. Cold mappings release the opening descriptor; replacement,
unlink and parent storage shutdown cannot redirect an unread pack.

Graph collection retires physical field scopes independently of logical source
claims, waits for actual readers/publishers, and commits pack-file retirement debt
with graph removal. Abandoned preparations reconcile through the same durable
unlink path. Checkpoint export copies the complete retained pack closure through
bounded buffers and deduplicates shared physical IDs. Whole and pruned family
imports prepare receiver-owned packs from actual donor decoders and import exact
source claims; they preserve field sections without reanalysis or receiver-current
source substitution. Repacking transfers reachable sections and avoids carrying
obsolete pack extents.

Native tests cover foreground publication, reopen, retained reads, pack replacement
before first read, allocation failures, exact typed inputs, independent reader
retirement, abandoned preparation recovery, checkpoint restore and cross-primary
packed-family import. These are correctness checks, not end-to-end performance
measurements. Reader/peer activation and automatic eligible-write promotion remain
disabled for released ASCH25 writers until the full rollout protocol is implemented.

To measure the actual public indexed update path with packed fields:

```sh
zig build --build-file zig/build.zig document-patch-public-test-build -Doptimize=ReleaseFast -j1
python3 scripts/bench/document_payload.py --suite public \
  --binary zig/zig-out/bin/antfly-document-patch-public-tests \
  --layouts production adaptive fields --trials 7 --bytes 65536 --rounds 6 \
  --output output/field-public-updates
```

`fields` uses an admitted ASCH29 table and ordinary indexed batch transforms.
It is rejected for the storage-only suite, which has no text index. The runner
retains separate process CPU/RSS, sync assertions, byte/query verification and
paired uncertainty intervals. Supply an earlier optimized `--baseline-binary`
to distinguish changes to the common production path from layout effects. The
checked-allocator Debug smoke verifies the new condition but is not throughput
evidence. Repeat across payload sizes, entropy, read ratios and hosts before
making a performance claim.

To compare qualified initial JSON replay with legacy publication:

```sh
zig build --build-file zig/build.zig source-replay-bench-build -Doptimize=ReleaseFast -j1
python3 scripts/bench/source_replay.py \
  --binary zig/zig-out/bin/antfly-source-replay-bench \
  --output output/source-replay --documents 65 131 1024 --trials 7
```

Use `--baseline-binary <archived-executable>` to interleave an earlier build;
legacy workloads remain controls for changes outside native-source delivery.
`--compare-snapshot --limit 10` compares the bounded reader with a fixture-forced
snapshot on the same binary and selected result count. The force control exists
only in tests; it adds no public API or runtime policy flag. Native output reports
and the runner verifies the selected limit and actual point-reader use. Older
fixtures that do not implement those controls fail the measurement contract.

The first unconditional probe experiment was rejected after an 84-run optimized
comparison: at 1,024 returned documents its native query median was 12.260 ms,
versus 10.352 ms for the earlier snapshot implementation, with whole-fixture
median RSS 54.16 versus 52.44 MiB. Small cases improved, which does not justify
using an unbounded value-loan scope for all result densities. A subsequent 84-run top-10 comparison also rejected using the source-map-page
budget alone: at 4,099 documents the native query median was 4.486 ms versus
3.319 ms with forced snapshots, and whole-fixture median RSS was 137.25 versus
109.28 MiB. Smaller cases improved, but selected-row count did not predict the
crossover. The backend admission hint therefore also accounts for snapshot
reuse and storage layout. Its qualification must cover both top-k and dense
queries; this heuristic is not a universal performance guarantee.

Each trial uses fresh disk stores and the process allocator. The runner records
binary hashes, host/cgroup constraints, randomized conditions, per-run logs,
whole-process CPU/peak RSS (including the parent's inherited RSS floor) and
collection/preparation/publication/public-query times. It rejects missing or
duplicate metrics, failed byte verification and nonoptimized binaries unless
explicitly requested for a correctness smoke run. Matched-trial summaries include
wins, median differences and the observed ratio range, preserving regressions
and variation instead of treating a ratio of medians as a universal speedup.
The family condition changes
current rows outside timed phases before querying retained bytes; the legacy
condition keeps current rows because it has no retained source contract. Query
timing therefore compares the two delivery contracts, not identical primary
histories. Setup, authority mutations and teardown belong to whole-process
elapsed/CPU measurements, not the reported phase totals. These measurements do
not yet cover sustained automatic migration, field reuse or distributed transfer.

Independent cross-primary transfer must
import/rebase owners through native source handoff; copying counters or UUIDs
does not establish authority. Composite field layouts need typed manifest/Merkle
identities distinct from legacy full-byte SHA-256 stamps. No production format
activation or universal/zero-cost performance claim follows from this native path.

Producer input capture now uses a pinned authority/source snapshot after a
cheap negative authority probe for legacy execution. The private observation
callback verifies equality instead of returning a second allocated JSON document.
Primary and group-source JSON are compared while borrowed; relational rows retain
schema-aware decoding with an owned temporary. A zero-allocation fixture verifies
the JSON path. Hash borrowed input before verification or another read can retire
its buffer; shared source validation also reduces each present row encoding to an
owned digest before looking up the alternate encoding. Existing whole-source
guards and wire semantics are unchanged. Ordered dense/sparse and asset fixtures
cover source capture, acceptance and replay, including unchanged-timestamp ABA.

Qualification rejected a metadata-only native observation candidate. Comparing
inline bytes and immutable extent hashes avoided a body read but still required
whole-source SHA-256 for the existing proof. The extra hashing pass measured about
1.9x the logical-reference latency on both 1 MiB datasets on macOS. The candidate
is test-only and not selected by provider capture; its optional erased-reader
physical hook was removed. A composable, explicitly versioned producer guard is
needed to remove that work without guessing a guard's meaning from its digest.

Prefer content-derived producer input revisions to an extra write counter for
every producer on every document edit. A closed compiled dependency set can bind
its configuration, source namespace, document incarnation, field presence and
canonical field identities. An unrelated edit then leaves that input revision
unchanged without a metadata write. The document's global source position still
advances for visibility and replay; keep that observation position separate from
input equality. Completion must credit a still-valid scoped input against the
current document obligation, and deletion/reinsertion must change the incarnation.
Unknown templates, graph/neighbor inputs, root JSON renderers and unsupported
canonicalization retain complete-source guards. Scope declarations must come from
the pinned producer definition, never from a caller-supplied narrower read set.
Inherited inputs for the same document form a conjunction: retaining one field's
guard cannot discard another producer's dependency. A whole-source dependency
retains complete-source validity checks.
The immutable completion plan now compiles the closed literal-root-field read set
for eligible document-wide dense/sparse text producers. The field name belongs to
the compiled plan; provider authorization returns that dependency only from the
same pinned requirement. Template, graph, prior/group context, media and upstream
unit inputs retain whole-source planning. A compile-time inventory requires every
new executable-template field to be classified before it can narrow dependencies.
Tables without eligible providers allocate no dependency sidecar.

`producer_input_revision.zig` supplies the internal equality primitives: input
identity binds source namespace, document key, canonical identity and live creation
generation, exact producer definition, reprocess generation, literal field name,
presence and canonical value identity. It excludes the observation position and
user timestamp. A separate observation digest binds those actual clocks for
publication/audit. Bounded conjunction retains each producer's input term and
rejects conflicting revisions instead of replacing an inherited guard.

Protocol 17 now carries the compiled input terms in a sorted source-ordinal
sidecar. `Source` retains its five-field representation. Empty sidecars preserve
APF3 proofs, APB1 transport, APR2 receipts, legacy JSON and their digests. Scoped
results use APF4, APB2 and APR3; semantic input equality excludes source observation
clocks and the observed root, while the immutable publication identity still
binds those observations. Scoped adoption uses its own versioned publication
digest so equal inputs at different receiver observations cannot collide under
one immutable proof key.

The active pinned catalog supplies each selector and exact producer definition;
caller-supplied field lists cannot authorize narrowing. Native validation reads
the manifest's authenticated field commitments and live document incarnation,
checks the independent reprocess intent, and requires an output owner's own
producer term. It does not fetch immutable source values. Unsupported definitions
retain whole-source guards. Downstream proof inheritance forms a bounded sorted
conjunction, remaps source ordinals once, preserves whole predicates, and rejects
conflicting terms or incarnations before changing the live token. Repeated
already-inherited proofs allocate nothing. Receiver adoption verifies certified
catalog equivalence and field identities, then recomputes input revisions under
the receiver namespace and actual live incarnation; it never imports donor
completion positions or silently drops scoped dependencies.

Scoped accepted-result lookup resolves the persisted stream proof, APR3 receipt
and current output revision witnesses before source hydration. Dense and sparse
replay use a process-local hint enabled by a captured scoped token; false or
missing hints fall back to ordinary capture, and every positive lookup rechecks
catalog/source authority in a pinned snapshot. Completion independently captures
and fences the current materialization observation. An old still-valid model
result may satisfy that current obligation; its original observation cannot
advance completion. Mixed whole-source proofs keep complete-source checks.

Protocol admission raises the table's monotone reader fence to ASCH27 in the
same transaction as the ordered catalog. Protocol capability checks preserve
protocol16 grouped-source retention and activation. The released reader stays25,
so automatic production activation remains unavailable until reader/peer cohort
qualification. This is native consumer integration for scoped document providers,
not activation of the remaining full-text/source-ownership format.

Once dependencies are closed, capture authority, incarnation, input identities
and observation position in one backend-proven atomic read window. Backends
without that capability retain a pinned snapshot. Individually consistent probe
calls cannot substitute for one coherent cut. This can remove snapshot-cloning
cost without weakening the input fence. It requires a versioned producer-proof/
transfer protocol and compatibility tests for restart, ordered replay, adoption
and source completion before activation.

Exact indexed-source references need a monotonic ownership handoff across the
primary/index commit boundary: establish a durable pending claim before publishing
an index root, publish that root with the exact source revision and logical root,
then acknowledge the committed claim. A crash between these steps may retain
extra bytes but must never expose an unowned reference. Recovery checks the
immutable publication receipt before finishing or canceling a pending claim.
Cancellation must durably fence that publication ID at the index owner before
releasing the claim; an absent receipt alone can race a late publisher. Acquiring
from retained history needs a verified handoff from that frame's live pin;
current-row membership alone cannot authorize a historical source revision.
Retirement first removes the durable index root, then waits for its runtime reader
leases before releasing source claims. Portable fallback can flatten verified
logical values; it must not hydrate the mutable latest primary document under an
older index revision.

The native storage layer now implements the ownership and publication primitives
for this handoff, under a test-only ASCH27 reader fence. `index_source_claim.zig`
validates the current mutation revision or a still-live retained afterimage in the
primary writer transaction. Claims share immutable `ISS1` source snapshots and
`ISV1` value sets, with separate small refcount records. Forking an index claim or
acknowledging a publication never rewrites the snapshot or reads a body. A
metadata-only source revision reuses the value set without updating every value's
pin record. `DVR2` keeps index and history pin counts separate: either budget can
release its charge while the other still owns the same stored value. Default
internal limits bound source metadata to 64 MiB, indexed values to 256 MiB and
claims to 1,048,576. A failed compound operation poisons its writer transaction;
catching an allocation/quota error cannot commit partial ownership or a reader
fence without its budget.

`PersistentIndex` reserves a durable globally unique physical owner (`ISO1`)
before claim preparation, so even the first unpublished claim has an unambiguous
recovery destination. Reopen preserves that owner; another owner's publication or
cancellation is rejected. It commits an `ISP1` publication receipt in the same
metadata transaction as the active segment. It serializes cancellation with publication,
forces physical durability before returning a decision, and makes identical retries
idempotent. Source-map publication adds authenticated `ISM1` manifests and `ISG1`
pages, each covering at most 64 segment-local document IDs. The receipt's root
binds both the segment digest and every source-map page; map preparation also
checks document identity against the segment's actual document order. One lookup
reads the small manifest and one bounded page, independently of source size.
Preparation retains only the page-hash directory and borrows immutable native
references until publication. A single 6,758-byte scratch page encodes both the
preparation hashes and publication writes; publication verifies each re-encoded
page against its prepared digest. Changed borrowed coordinates fail the atomic
publication instead of changing a map beneath its receipt. At maximum capacity,
the retained directory is 32,842 bytes, replacing roughly 6.6 MiB of separately
allocated encoded pages. This bounds temporary encoding memory; the owned claim
references, primary transaction staging and index metadata still have their own
budgets. It does not establish an elapsed-time or process-RSS improvement.
Small maps retain their existing `ISM1` flat directory and two-read random lookup.
Above 65,536 documents, `ISM2` replaces that directory with a 104-byte manifest
and authenticated `ISD1` directory nodes. A fixed fanout of 128 covers the full
32-bit segment document space in at most four levels. Preparation and staging
stream through one source page and four fixed directory buffers; they never
retain every page hash or allocate every encoded page. An opened large-map view
allocates no directory. Lookup copies the selected child digest before another
backend read can retire the borrowed bytes, then authenticates one path and one
source page. Source-frame windows still cache the selected references.

Large-map publication recomputes the directory root while staging; a changed
borrowed coordinate fails publication and the enclosing metadata transaction
aborts. Stage and reclamation errors poison their enclosing transaction;
metadata adapters abort immediately and primary adapters retain their existing
failed-transaction state. Catching a final root mismatch cannot commit rewritten
leaf pages beneath an earlier root. Regression checks attempt that commit and
verify both the original map and unrelated transaction metadata roll back.
Reclamation extends the existing durable frontier over leaf pages and
then deterministic directory-node coordinates, with at most 128 deletes per
transaction. Aborting a turn advances neither deletion nor progress. The
65,537-document fixture verifies fixed manifest allocation, exact boundary
windows, zero-allocation read-view open, changed borrowed-input rejection,
aborted reclamation and complete leaf/directory removal. Directory fixtures
cover fanout boundaries, three-level traversal, borrowed backend buffers,
corruption and the maximum ordinal depth. Native claim/map checks pass 24 cases;
publication/recovery checks pass 42 cases on macOS and the constrained ARM64
Linux VM, without leaks. Actual primary
claim/value/metadata budgets remain enforced: representable map cardinality does
not authorize an unbounded publication. Impossible per-publication claim counts
are rejected before output hashing, coordinate allocation or owner reservation;
transactional aggregate quotas still decide actual admission.
Fresh native preparations and imports enforce document uniqueness at the claim
key in the same primary transaction. They keep no second document-digest hash
table. A duplicate fails and poisons the compound transaction, including its
first claim, value pins and reader-fence changes. Preparations that explicitly
reuse an existing publication ID retain idempotent low-level claim semantics
and their prevalidation; completed handoffs retry from the persisted journal. Structural forks reuse the already authenticated source record
and avoid a redundant original-header read. No changed mode grants historical
source authority: held source leases and publication receipts are still checked.
Capturing a source
clones the held segment's reader lease before the primary snapshot opens. Queries
can then read the exact claimed afterimage even before acknowledgment, after a
primary overwrite, or after the index root retires. They never substitute the
current primary row.

Reader lifetimes follow the selected logical source generation and held query
leases. Physical segment storage has independent reference counts.
A process-local rendezvous first selects the physical owner and then one of its
publication shards, joining the same durable identity across close/reopen.
Ownership maintenance retains that registry; cells retain their originating
registry through the last query. Cleanup scans only that owner's cells, while
query lease clone/release keeps its existing publication-shard path and gains no
owner-wide counter or lock. Registries allocate only for source-owning indexes,
own no source bytes, and disappear with their final handle. Retirement seals fresh lease registration while
allowing clones of already-held query work. Primary claims and source-map pages
can be reclaimed only after durable retirement and complete reader drainage.
Cloned handles retain their originating shard for locking and destruction.
Fresh lookups can now use an owner-supplied canonical broker across separately
compiled static runtime archives. The broker checks its method/argument/result
contract and a digest of eleven private reader-state layouts before accepting a
port; the dispatch also checks the call and concrete layouts before opening an
owner. Runtime requests copy the borrowed interface, including original-request
prefix compatibility. The receiver keeps its own checked static descriptor; it
does not retain the caller's descriptor storage. The interface is process-local, conveys no source bytes
and adds no query-time rendezvous. A fixture links an independently compiled
archive, proves its held reader blocks retirement through an injected broker,
and uses the receiver's independent local registry as a negative control.
The five targeted storage-owner tests also pass against the linked production
runtime archives, including allocation-failure cleanup, invalid descriptor
rejection, the old request prefix and borrowed filesystem lifetime.
This establishes same-process static-archive registry sharing, not distributed
ownership or independent restore compatibility. Independent physical clones must
rebase owner/claim bindings through import or establish exclusive activation;
sharing an owner digest across independently mutable stores is not authority. Native content-stamp consumption is
currently confined to its originating implementation. Neither a copied native
struct nor a serialized digest substitutes for these owner-side lifetimes.
Source-map reclamation deletes at most 128 pages per transaction; its checksummed
resume frontier commits with the deletes and cannot be advanced by a caller
cursor. It is idempotent; publication tombstones remain to reject late publishers. Logical
primary cursors hide claim, snapshot, value-set and budget keys in both directions.
Native physical snapshots preserve their actual storage records.

`index_source_recovery.zig` now coordinates the native two-store handoff. It
reserves the durable physical index owner, retains a live preparation pin, and
prepares all exact claims and a small
checksummed `ISJ1` recovery journal in one primary transaction, and forces primary
durability before exposing an index publication. The journal binds the segment
hash, authenticated source-map root, document count, recovery phase and frontier;
its bytes consume the same bounded metadata budget as the claims. Unsupported
relational inputs retain their complete-source path until their schema lifetimes
are represented. Duplicate inputs and reused publication IDs are rejected.

Recovery serializes cancellation at the index owner before releasing abandoned
claims. A live in-process preparation is pinned under that same lock, so a
maintenance pass cannot mistake a publisher between its two commits for a crash.
The pin is released on abandonment or preparation teardown; process restart needs
no recovery of process-local pins. If publication won that race, it instead finishes the index durability
barrier and acknowledges the existing root. Each turn visits at most 128 fixed
claim headers; acknowledgment reads no immutable snapshot recipes or source
values. Reclamation resolves one authenticated recipe at a time, with a 16 MiB
recipe-work ceiling per turn. The source deletes, ownership accounting and resume
frontier commit together. Retired publications wait for the actual shared reader
leases to drain before releasing claims. A later phase removes source-map pages
in bounded windows and forces index durability before forgetting the primary
journal. A restart can resume every phase without reconstructing source bytes
from the mutable primary row. Terminal index receipts remain as publication fences.

Dormant journals retain ownership while their index roots remain published.
Maintenance uses a read-only current-tip state hint for that idle case; it grants
no authority to publish or release claims and needs no primary writer or fsync.
A bounded sweep cursor visits each owner's journals without making dormant roots
an infinite run-until-idle loop, and continues a partially reclaimed publication
before rescanning unrelated dormant roots. The existing DB metadata-maintenance
owner now drives these turns under its catalog/apply fence. Source-aware segment
removal marks the publication retired in the same index transaction. Last-reader
and preparation release signal a retained wake context; it survives close/reopen
without dereferencing a closed index or starting I/O. Once an ownership sweep is
idle or waiting for readers, it stops polling until another signal arrives.
Ordinary legacy indexes keep only a null state pointer, with no ownership-state
allocation or metadata lookup on this path. The state caches the physical owner
reservation after its first successful durability barrier in each writer lifetime.
No public repair-readiness or completion fields are broadened by this scheduler
hint. Ordinary mutation/outbox production still needs source-aware publication;
ordinary query hydration remains separate consumer work.

Managed and explicit forced full-text compaction now perform an exact-source
ownership handoff.
Preparation captures each input root and its shared reader lease once, reads
source-map references in windows of 64, and uses native merge provenance to place
those references in output document order. Primary claims fork the authenticated
indexed afterimages, including publications awaiting acknowledgment, rather than
reading or retokenizing the current documents. One source-map directory is held
per input; a reference window authenticates at most two map pages. Output hashing
and primary durability barriers happen before the final publication lock. A
borrowed native content stamp binds the hash to the retained immutable output
source; it is not a wire credential or permission to mutate that source.

The index commits replacement source maps, publication receipts, active roots,
concurrent deletion masks, and retirement of removed publications together.
Heap compatibility outputs transfer into the index allocator or independent
immutable files; streamed outputs retain their final native artifacts. Multiple
outputs publish atomically. Stale plans retain their journals until fenced
cancellation, and committed retries preserve the original artifacts even when a
malformed request fails before receipt validation. Artifact cleanup treats an
uncertain ownership lookup conservatively. Mixed legacy/exact-source candidates
compact in separate groups, preserving the source revision of every result.
This path does not make field updates incremental: it still rewrites the merged
postings and forks one claim for every surviving document.

A durable `IOC1` owner-close fence rejects new source preparations and new
source-root commits under the physical owner's storage lock, including publishers
that started before closure. Published retries and held readers remain valid.
Ordinary query opens reject a closed owner, so reader quiescence cannot race a
fresh reopen. Closure is idempotent across restart.

Catalog deletion now commits a source-ownership cleanup phase and the original
physical owner identity in the existing deletion outbox, atomically with catalog
removal. That primary route becomes durable before physical closure. The existing
validated active-root pointer and same-name admission fence preserve the route;
cleanup verifies the physical owner before changing it. A cached metadata-only
handle opens each retired owner once per runtime. It does not map segment files,
replay payload-bearing WAL, hydrate documents, or rebuild query readers. Each turn
retires at most 64 active roots and advances a bounded recovery step. Surviving
queries and preparations retain their original native leases through catalog
removal and reopen. Last-pin release signals a retained cleanup context; a reader
wait settles the cleanup worker rather than repeatedly retrying it as lock
contention. Waiting owners do not starve another index's cleanup.

After every publisher and reader has drained, cleanup checks for remaining
primary journals and forces primary cleanup durability before advancing the
outbox. The final metadata deletes atomically persist the finalization phase; returning
a completed in-memory page does not replace that durable state. Physical
finalization flushes namespace deletion before forgetting its primary tombstone. Per-publication receipts and the permanent owner-close fence
are reclaimed together with that physical namespace. The separate finalization
lease must revalidate the current primary tombstone: a previously read completed
page does not authorize deleting a newly staged same-name namespace after another
worker has finished the old job. This revalidation also protects ordinary legacy
index cleanup.

Active source owners now support bounded receipt reclamation behind an `ISO2`
reader fence. The header retains the current birth epoch and a monotonic admission
floor; `ISP2` receipts retain their birth epoch. Existing `ISO1`/`ISP1` records
remain readable. Native factories select an epoch and retain a preparation pin
under the same owner lock. Advancing births does not abort older preparations:
they can publish until their pins drain, after which a forced index durability
barrier advances the floor. Published older roots and identical retries remain
valid. After a receipt is collected, that permanent floor rejects late publishers
and prevents stale cancellation retries from recreating the receipt. For an
abandoned preparation whose primary journal exists but whose index receipt was
never published, the synced owner floor itself yields the native cancellation
decision. Recovery releases that journal's claims without recreating a receipt;
absence by itself still supplies no cancellation authority.

Reclamation scans at most 64 receipts per turn. Terminal older-epoch records must
have drained actual readers, no primary journal or claim keys, no source-map root,
and no active index segment. It forces primary cleanup durability off the index
lock, then revalidates the receipt and ownership before deletion. Failed syncs
retain the individual fence and rearm maintenance. It reads fixed headers and
physical claim keys, never source recipes, logical documents or immutable values.
Current-epoch terminal receipts accumulate until 256 records justify a coalesced
epoch transition. A newer outstanding preparation delays only the next floor;
receipts already below the durable floor remain independently collectible. Last-preparation release wakes floor advancement even when query
readers remain; last-reader release wakes reclamation. A quiescent owner stops
polling. Legacy owners retain their existing idle path; native entry into `ISO2`
remains gated with the source-ownership format, rather than silently enabling an
incompatible reader format through ordinary maintenance.

Source-owned split-range pruning now uses the splitter's native ordinal
provenance to fork only surviving afterimages. Source references are read in
windows of 64; there is no source-body hydration or per-document hash table for
rediscovering output identity. Replacement source maps and root retirement use the
same primary-before-index durability protocol as compaction. The existing heap
split builder still rewrites postings; this is not field-generation reuse or a
complete cross-primary shard transfer/import protocol.

A held native source session can now prepare an index publication in a different
primary store. It imports the original immutable recipe and revision, authenticates
and copies each missing immutable value, and establishes destination index pins
without modifying current-row membership. A value set already owned by that
primary links its existing snapshot without rereading the source body. Destination
claims, copied values, accounting and the recovery journal commit atomically;
primary durability precedes destination root publication. Quota, malformed-value
or duplicate-document failures abort the whole destination transaction. Source
reader leases and its pinned read view remain live throughout preparation,
including when the source root is already retired. Native immutable segment
sources use content stamps instead of flattening the segment into a heap buffer.
This is an owner-local transfer primitive, not a new wire credential: distributed
split/import still needs an authenticated transport, resumable bounded transfer
pages, destination activation and retained source-owner routing before rollout.
Activation must validate schema/analyzer compatibility and destination document
ordinal ownership; byte-identical segment bytes alone do not establish either.
A rebuilt/rebased segment must preserve its native source-to-output provenance.

Qualification covers quota/allocation failure rollback, independent history/index
budgets, retained-history handoff followed by history reclamation and restart,
publication retry/conflict/cancellation, index reopen with old readers, source-map
page corruption, held-query clones after retirement, and cross-store reads at the
publication-before-acknowledgment cut. Epoch qualification includes admitted old
publishers, held readers, missing receipts with abandoned primary journals,
failed primary syncs, unjournaled claim retention, coalesced maintenance, idle
quiescence, and reopen. Native cross-primary qualification covers an already
retired origin root, a different destination current row, corrupted values,
duplicate sources, quota rollback, shared value reuse, reopen and complete
reclamation in both independent primaries. The index writer rejects legacy structural
replacement/transfer of source-owned segments instead of silently discarding
ownership. The inline legacy WAL and grouped rebuild-page APIs do not yet publish
source receipts; those paths retain complete-source fallback.
Managed-compaction regressions delete the mutable primary documents before
merging, retain old readers across replacement and reopen, and reclaim all
ownership through the ordinary DB metadata-maintenance path. Source handoff now resolves provenance in bounded windows before opening
source-map pages. Span maps use one span search, dense maps read contiguous
coordinates, and file maps use one contiguous cache read for up to 64 entries.
Compaction and range pruning share this path; an entirely discarded window
performs no source-reference read. The native fixture retains only document 64
from a 65-document root and verifies one source read instead of two, then checks
shuffled output coordinates. A differential fixture covers span, dense and
externally sorted file maps, deletions, window boundaries and multiple outputs.
Single-output location mapping also avoids the output-boundary binary search.
These checks establish navigation and read-elision behavior, not an elapsed-time
or RSS guarantee.

Multi-output structural handoff prepares all new claims and journals in one
primary transaction and forces primary durability once for the cohort. Output
hashing occurs before opening that writer, and all actual publication birth
registrations remain held through preparation. Impossible aggregate claim counts
fail before hashing or allocation. A failed preparation aborts every output;
post-commit durability errors leave recovery journals for native cancellation.
Index publication retains its separate validation/barrier and commits all new
roots and old-root retirement atomically. Each output has a single-segment
receipt, so the receipt codec's 64-segment limit does not limit output count.
The existing task allocator and primary ownership quotas bound admitted work.
Duplicate publication IDs use sorted 32-byte identities, replacing quadratic
pairwise checks; one-output publication needs no uniqueness allocation. The
65-output native fixture verifies one preparation sync, preserved old sources
after primary changes, retirement, and exact hydration after index reopen. It
also rejects impossible aggregate quotas before allocation and injects failure
after one output's claim/journal are staged, verifying that the old index and
ownership budget remain unchanged. These checks pass on native macOS and the
constrained ARM64 Linux VM; they measure sync counts and correctness, not a
workload-independent throughput or latency improvement.

Native qualification
covers 50 publication/recovery checks and 28 claim/map checks on macOS
and in the constrained ARM64 Linux VM, with no leaks. The native persistent-index
suite passes 68 checks, with one optional benchmark skipped. Formatting, diff
validation and the license-boundary check pass. Broader native split
regressions pass 98 checks, including their checked-in replay fixtures; native
catalog cleanup/removal regressions pass 65 checks. These correctness checks
are not a new throughput or process-memory qualification.

Query consumers retain an actual native corpus cut before resolving any shared
source-dependent filters. When a selected index has source ownership, all selected
text snapshots are pinned together under one catalog cut. Declared runtime
configuration revisions are checked when each component starts; short analysis
leases protect additive field discovery during ranking. A request-local context is
reconstructed by the DB, never accepted from public input or inherited as another
table's authority. Legacy indexes allocate no source plan. Current visibility,
identity masks and authorization remain separate from indexed source content.

Original-value predicates use a bounded source-map window and one lazy primary
read view. The memo key binds both the original immutable snapshot and its closed
logical read paths; projection cannot reduce the original-byte evaluation budget.
Structured residual filters collect their own logical dependencies instead of
using response masks. Their source projection preserves opaque array containers
and original positions, including JSON-pointer evaluation. Root-wide or oversized
dependency sets fall back to the same claimed original source, never the current
primary row.

Physical segment sharing and logical source ownership now have separate native
reference counts. Inventory copies retain a small logical generation, whose single
registration owns the source root; immutable posting bytes keep their independent
physical lifetime. Visibility wrappers transfer source ownership instead of making
the physical-parent chain retain prior logical roots. Source capture resolves the
binding from its actual held lease and canonical receipt, rather than consulting
today's segment-to-publication mapping. This is the ownership prerequisite for
source-only generations. Both single-row and window capture derive their binding
from the actual held reader lease, so removing or rebinding today's segment
metadata does not redirect an older reader. Query-page caches store compact
coordinates and share each root binding, reducing inline cache storage by 8 KiB
per native source frame without allocating another full-reference window.
Qualified JSON replay can prepare and publish initial source families from a
held native read cut. Source-only refresh and field-family reuse are not yet
wired into ordinary mutation/replay.

Selected hits carry one-pointer native handles to bounded pages of at most 64
compact references. A page owns one real publication lease and one shared query
reader; it contains no source bodies. Ranking releases its full corpus inventory
once selected references are captured. Fusion preserves the representative
component's exact coordinates, so it neither builds another ID-to-source map nor
rehydrates winners from current rows. Sorting fallback, residual predicates,
highlights and final projections consume that retained afterimage. Explicit
highlight paths load closed dependencies, preserving both dotted object paths and
literal dotted keys; complete source needed for final delivery is read once.
Guarded provider sources share their actual execution/metadata owner until the
last selected row retires. Physical local sources keep the short ranking lease.
Conditional
source identities use the claimed immutable canonical tree for scalar leaves.
Reference-only fields and matching conditions omit their value reads entirely;
remaining values share one decoded recipe with projection. Two request-owned
selectors amortize response-mask compilation across winners and own their mask
strings independently of caller slices. Opaque containers
retain the reference path rather than treating logical object hashes as public
value identities. An all-native window avoids opening a current-primary
transaction. Before a public DB search
returns, final winners are materialized or omitted as requested and all native
DB-borrowing handles are released. A current-primary distributed document token
cannot certify indexed old bytes: that combination fails closed pending a tagged
retained-owner transport. Generated artifact/member integration also remains a
separate prerequisite for production activation.

Source maps now cover the segment's full 32-bit document space with bounded
directory scratch. Atomic source replacement still caps each transaction at 64
outputs. Workload-sized output partitioning and publication admission must respect
that atomic boundary before format activation; a resource error is not a scalable
compaction policy. Source-map validation currently requires stored document keys;
ordinal-only roots need authenticated native ordinal evidence. Legacy/exact-source
separation can leave mixed singleton tiers uncompacted. Steady admission must
also leave bounded ownership headroom for a replacement generation. Simply
raising the hard quota during compaction is incorrect when old readers keep
those generations alive. Source-tree/field-family reuse must bound the new
ownership work, and pressure must defer or reject admission without releasing
bytes still held by readers. Qualification must exercise a near-quota table with
held old queries and verify that retries make progress after those queries drain.

Ordinary foreground and mutation outbox/replay JSON paths share the native
source consumer. ASCH25 remains the default serializer for newly created tables;
format promotion happens automatically on eligible writes after reader
qualification. A monotone manager hint keeps tables without source-owned roots
out of source-context planning without a catalog lock or allocation. This hint
never grants reads or replaces snapshot/receipt validation.

Managed and forced compaction, native cross-primary source-map split/import,
posting-preserving range pruning, retained pack checkpoint/restore and
cross-primary packed-family imports preserve native field generations. The
coordinator makes primary claims physically durable before publishing a root in
a separate index store; an asynchronous primary commit alone is insufficient.

### Automatic admission and bounded field compilation reuse

Data-Raft protocol 25 qualifies ASCH29 readers across every applying member,
including joint-consensus outgoing voters and learners. Large writes and
transforms request this upgrade opportunistically. The leader probes a membership
snapshot once, activates the highest common required/preferred protocol, and
rechecks the membership fingerprint under proposal ordering. Older peers retain
the existing write path. A short negative retry deadline suppresses repeated
optional upgrade probes; it cannot authorize a format. An accepted barrier is
usable only in its leader term until its durable marker becomes visible.

Each subsequent ordered envelope carries `document_reader_format: 29` into the
native apply receipt. Raw apply requires the earlier protocol barrier, including
when barrier and mutation share one apply fold. Storage never infers permission
from a raw apply watermark that may have advanced beyond document apply. Older
envelopes retain reader floor zero and cannot initiate migration. The private
storage ABI uses its reserved request word for this authority without changing
its size. Standby ordinary replay uses fail-closed payload version 24 when a
receipt carries this authority; older payload versions cannot carry it. An
unqualified independent replication mirror defers new automatic promotion.
Single-owner filesystem execution qualifies its local reader directly; imports,
restore replay and mirrored execution cannot borrow that permission.

Reader qualification and physical format activation use separate atomic hints.
Qualifying a small table therefore does not enable native read planning or
replace its batched read path. An eligible primary write raises the physical
schema floor in the same primary transaction as its new authority. Immutable
values still require at least 64 KiB of substantial unchanged content; small,
write-once and changing bodies retain inline storage. Medium inline documents
follow the table's canonical identity algorithm after admission. Reopen and
native transfer adopt the durable floor before publishing workers. Logical DDL
preserves that floor, and raw downgrade writes remain rejected. No public
migration opt-in or caller-supplied proof flag is required. Rolling back an
admitted root to an older reader requires restoring a pre-upgrade snapshot.

For a matching document ordinal inventory, native producer preparation hashes
the actual projected text and typed inputs before tokenization. It pins the
actual previous field root and skips compilation only for identical inputs and
configuration. All contributions sharing a field name are skipped together.
Publication rechecks the held proof and grafts those native sections into the
new field directory. Changed and internal sections compile normally. The
configuration digest's versioned compiler domain is a semantic epoch: changes
to projection, analysis or default-analyzer behavior must bump it. Process-local
revision counters and arbitrary build IDs cannot identify those semantics. A weak
inventory pointer is selected and pinned under the same lock as retirement;
encoding happens outside that lock. The hash alone never grants ownership.

Reuse admits at most 63 retained old packs plus one new pack, and requires at
least half of each retained pack's payload to remain useful. Compilation reuse
is independent of physical retention: low-utility packs stream their verified
unchanged encoded sections into the fresh pack with a 64 KiB window, without
tokenization or old-pack references. The pack finalizes only after all changed
and copied sections enter it. A bounded remapping path also handles subsets
and changed row order. Incompatible or expensive shapes use normal compilation.
This bounds history and dead bytes instead of promising every workload improves.
The public benchmark reports `field_compilations_avoided` as well as elapsed
time, memory, WAL bytes and syncs. Its `production` arm now exercises automatic
admission; `inline` remains an explicit storage control, not an older binary.

Managed compaction prepares packed outputs from native section decoders and
exact task-held source observations, without hydrating current JSON or
retokenizing it. Publication checks the actual source binding as well as segment
IDs, so a source-only refresh invalidates stale merge work. Deletion deltas,
cohort receipts, field roots and active segment replacement share one index
transaction. Range pruning uses the retained decoder rather than assuming a
logical segment ID has a physical `.seg` file. When compaction changes ordinal
coordinates, it composes optional raw-input evidence using the task's exact
source-to-output map. Missing observations stay explicitly unknown. It never
infers raw-input equality from cooked posting bytes.

The merge encoder writes stored identities and field sections directly into a
private AFP1 pack. Its temporary flat decoder directory remains in bounded RAM
(up to 32 MiB), so ordinary packed compaction does not write, reopen and recopy
an intermediate flat file. Native field publication adopts the forced pack's
actual section offsets. Private construction patches rehash only dirty 64 KiB
pages before sealing the pack. The preview decoder is temporary; published
readers still require the independently pinned field graph and exact source
receipt. Durable preparation reservations, sync barriers, stale-source CAS and
abandoned-file recovery remain mandatory. Backends without native preparation
retain the existing route.

Composable input evidence is optional metadata of an actual held field node,
authenticated by its compiler-input digest. IFL1 stores a configuration/analyzer
context and a known/unknown digest per document; each known row commits to its
ID, canonical coordinate and complete ordered text/typed contributions. Evidence
is calculated only for fields with at least 64 KiB of projected text, at no more
than 1/64 of their raw input bytes, and at most 16,384 documents. Producer cache
admission requires an observed unchanged field; write-once and changing fields
do not populate it. Compaction propagates only already-known observations.
Consequently a cold cache may compile the first update after consolidation;
subsequent observed reuse can establish evidence for later consolidations.

The optional cache is content-addressed by the actual field input digest, scoped
to its index owner. Native field nodes authenticate the bytes; cache keys never
select postings, grant reads or hold a resource lease. Thirty-two two-way buckets
hold at most 64 entries of 256 KiB: 16 MiB of evidence plus fixed key/tag metadata.
Hits do not write recency counters. Insertion and eviction share the publication
transaction; duplicate proof data across copied field generations is written
once. Large entries, absent observations and collisions use normal compilation.
Cache eviction is independent of field retirement and long-running readers, so
old snapshots cannot saturate the cache indefinitely. A prepared remap retains
its authenticated evidence bytes through construction, making concurrent cache
eviction a performance event rather than a publication failure. Dropping the
index removes this metadata with its native store. Ordinary query readers do not
load it. This adds no field-pack reader format or migration fence.

After an ordinal-inventory lookup misses, the producer checks at most eight
actual retained roots, each with at most 4,096 documents and 256 fields, and
reads at most 64 KiB of document IDs. Remapping is admitted only when each candidate field's
encoded sections occupy at most a quarter of its new raw input bytes and no
more than 8 MiB. The planner and constructor both check the held configuration,
raw-input rows and actual document identities. Native posting/typed-column
writers translate selected old rows into the new coordinates without analysis;
internal ordinal/sort sections are always rebuilt. Substantial sorted batches
use the compiler's own ordering rules before planning. Other batches keep the
ordinary sort/compile path. This is deliberately a bounded active-root search,
not a new persistent per-document locator or an unbounded history scan.

#### Reusing analyzed source contributions

The source collector owns ordinary JSON materialization and its native
observation together. It reads physical rows in sorted batches from the held
MVCC cut, closes borrowed manifests before blob reads, and materializes directly
into the observation's final allocation. Inline rows do not pass through a
second whole-batch copy. This avoids the former materialize/copy/reread-and-compare
sequence. Missing rows, transformed assets and relational sources keep their
existing paths. The compatibility capture operation still compares supplied
bytes against the snapshot; callers cannot substitute a digest for that check.

For eligible native sources, projection opens the actual directory and canonical
scalar certificate under that observation, checks its mutation/root, and parses
the observed bytes itself. A private binding records the exact parsed string
span and its canonical value identity. It supports escaped strings and Unicode;
a copied string, caller-provided parsed root, array/opaque value, transformed
text or unknown canonical ABI retains ordinary byte hashing. The binding cannot
be serialized. Text and matching typed-byte fingerprints can consume this
identity without hashing an unchanged body again. Compiler input/evidence
contexts use version 2 so older byte-based observations safely miss. This adds
no reader-format or protocol upgrade beyond the existing ASCH29 admission.

A separate optional, process-local cache stores analyzed contributions, keyed by
the complete resolved analyzer pipeline and exact input bytes or that closed
native scalar identity. It is shared across a manager's text indexes, allowing
`body` and `_all` to reuse the same analysis. Reordered or sparse batches can
rebuild postings from individual contributions without walking an old field's
whole dictionary. `_all` retains contribution order, token-count offsets, gaps,
duplicate positions and field norms. Changed metadata is analyzed normally.
Cached terms are merged once per distinct term; position sequences use lossless
signed delta varints and are decoded directly into the document accumulator.

Admission is lazy and requires at least 8 KiB of input and repeated observation
or a bounded prediction of another actual, unskipped consumer with the same
borrowed bytes and analyzer. This permits one cold analysis for `body` and
`_all`, while unhelpful dictionaries remain rejected.
A compact entry must occupy at most half its source bytes and at most 512 KiB.
Thirty-two fixed slots cap payload residency at 16 MiB; page-rounded allocations
and metadata are charged to the shared ResourceManager. Admission never invokes
reclamation while holding a cache lock. Clean entries participate in resource
reclamation, and bounded leases protect active decoders. Contention, eviction,
unsupported analyzers/inputs and allocation pressure use normal compilation.
Oversized candidates remain rejected until the slot observes another input,
avoiding repeated cache construction for unhelpful dictionaries. Small fields
and databases without eligible text do not allocate this cache. It does not
write source documents, field packs, recency records or other disk metadata.

This still materializes/parses source before projection and still encodes changed
postings/typed columns. It does not make the complete metadata update constant
cost. Fully lazy projection needs a closed plan for every consumer, including
typed fields and aggregates; optional token evidence cannot authorize placeholder
values or silently omit a field. Unknown consumers continue to require source.

`scripts/bench/document_contributions.py` adds paired, randomized multi-document
qualification. Its fixture exercises reordered dense/sparse edits, changed
bodies, escaped real source code, prose, entropy, small inputs and large
compactions. It verifies stored afterimages, metadata and `_all` query results,
and WAL synchronization. The cache-disabled native control isolates compilation
reuse. A separate inline control measures the complete layout comparison;
these comparisons must not be conflated. Binary/corpus hashes, per-run CPU/RSS,
update/compaction timings and paired bootstrap intervals accompany ignored raw
reports. ReleaseFast is required for qualification. The upgrade gate requires
at least a 2x lower-bound speedup on the selected update workload and rejects
confirmed CPU/RSS regressions above 5%; strict gating also rejects inconclusive
regression limits. Correctness or a cache hit count alone cannot satisfy it.

#### Sparse FST registry cleanup

A native CPU sample of the multi-document workload found dictionary registry
cleanup consuming roughly one fifth of main-thread samples. FST registries now
record touched buckets in a bounded bitmap and a list of nonempty bitmap words.
Destruction and generation rollover visit only those buckets, including stale
nodes from earlier resets. Ordinary reset remains constant cost. Small
vocabularies no longer scan and fault every page of a maximum-sized zero-backed
registry. Node minimization and wire encoding are unchanged; allocation failures,
rollover, repeated builds and the Go byte-compatibility vectors remain covered.
Merge dictionary builders also derive a vocabulary upper bound from already
validated source dictionary block counts. This avoids allocating the maximum
registry for every small metadata field without traversing source terms.

#### Qualification outcome for contribution reuse

The subsequent 2026-10-26 qualification pinned the pre-FST binary and compared
its cache-disabled control with the optimized cache-enabled binary. Seven paired
trials covered eight multi-document shapes on the shared macOS ARM64 host.
Ratios below are optimized/reference medians, not cross-host guarantees.

| Workload | Update elapsed | Process CPU | Peak RSS |
|---|---:|---:|---:|
| Dense code, 8 × 64 KiB | 0.838 | 0.844 | 0.853 |
| Sparse code, 32 × 64 KiB | 0.981 | 0.951 | 0.983 |
| Wide code, 64 × 16 KiB | 0.969 | 0.977 | 0.908 |
| Large compaction, 128 × 64 KiB | 0.963 | 0.922 | 0.997 |
| Prose, 8 × 64 KiB | 0.853 | 0.847 | 0.905 |
| Entropy, 8 × 64 KiB | 0.965 | 0.977 | 0.951 |
| Changing code bodies, 8 × 64 KiB | 0.859 | 0.879 | 0.915 |
| Already-admitted small rows, 8 × 4 KiB | 0.929 | 0.950 | 0.871 |

The large-compaction row uses a separate 21-pair confirmation: its RSS ratio's
95% interval was 0.976–1.018, resolving the initial seven-pair uncertainty.
All native CPU/RSS comparisons were within the 5% noninferiority budget after
that confirmation. The 2x update-speed release gate **did not pass**. A separate
1,000-build small-dictionary microbenchmark used 22x less CPU and 76% less peak
RSS for sparse FST cleanup with identical output checksums; that component result
must not be presented as an application-wide speedup.

A seven-pair comparison against the earlier demand-proof binary's existing
256 KiB public-update fixture found 10% lower CPU and 19% lower RSS for repeated
data. Entropy CPU remained within budget and RSS was 13% lower. This earlier
fixture is single-document and does not replace the multi-document matrix.

The explicit inline storage control revealed a separate migration tradeoff.
Dense 64 KiB code updates used 21% less CPU, 15% less RSS and 3.1% as many primary
WAL bytes; sparse updates used 8.2% as many WAL bytes. Continuously changing
bodies did not show a material WAL reduction. However, 4 KiB rows in an
already-admitted native table consumed about **4.0x CPU**, **3.7x elapsed time**,
**1.37x peak RSS** and **2.3x WAL bytes** relative to the legacy inline control.
Fresh small-only tables normally avoid automatic physical activation; the
admitted-table case still matters after a table has accepted eligible large
writes. A byte-density benefit for one body cannot justify claiming that every
later consumer or small-row update is cheaper.

Consequently these optimizations support upgrading existing native execution,
but they do **not** qualify unconditional format conversion for every workload.
The remaining structural work is closed source planning before hydration,
cheap source snapshots for small rows (for example index-owned inline source
pages with explicit reader/transfer fencing), and compiled field-block reuse
across changed publication inventories. Durability-scoped publication batching
and actual directory capabilities should be evaluated against the remaining
open/rename/sync overhead. None may weaken source authorization, snapshot
retention, stale-publication CAS or crash-recovery barriers. Reusing a caller
hash, emitting placeholder typed values, or suppressing durability syncs is not
an acceptable shortcut.

A separate two-CPU ARM64 Linux VM comparison used the final binary in both
arms, isolating contribution caching rather than the FST change. Twenty-one
paired trials found dense-code update elapsed time 8.3% lower, CPU 14.9% lower,
and RSS within the 5% budget (95% ratio interval 0.952–1.010). Sparse-code CPU
was 4.1% lower while update elapsed time was effectively unchanged. Sparse RSS
remains inconclusive against that budget: median ratio 1.032, interval
0.998–1.058. These VM results do not substitute for independent native Linux
hardware qualification, and they do not establish a universal regression-free
upgrade.

The final native public suite passed 33 tests, ownership passed 30, source
publication/recovery passed 81, and FST passed 21 in both Debug and ReleaseFast.
The Linux VM passed the 30 ownership and 33 public tests. Raw reports, sampling
stacks and pinned executables remain in ignored `output/next-pass/`; no generated
benchmark JSON or logs are part of the source change.

These policies reduce redundant work; they do not establish a universal speedup.
Write-once data still pays input-identity costs, high-entropy dictionaries may decline
remapping, and storage/fsync costs remain. Qualification must compare the prior
binary across small/changing, compressible/entropy, sorted, read-heavy and
compaction workloads, including CPU, memory, first-result latency and physical
bytes, before widening admission. Generated reports belong under ignored
`output/`, not in the source tree.

The 2026-10-26 qualification used pinned native binaries and the process allocator
on a shared macOS ARM64 host. Seven paired trials covered repeated and seeded
entropy data for 256 KiB/32-update, 4 KiB/32-update/eight-read, and
64 KiB/16-update/64-read workloads. CPU medians ranged from 3.2% lower to 2.3%
higher than the prior native binary; RSS medians ranged from 1.5% lower to 0.6%
higher. There were no confirmed regressions beyond the 5% gate, but 16 of the
30 fixture/process comparisons remained inconclusive, largely elapsed timing.
A separate 21-trial comparison with the per-generation evidence prototype put
fixture elapsed time, CPU and RSS within that 5% budget; repeated-data whole-
process elapsed time remained narrowly inconclusive. This establishes bounded
correctness and specific noninferiority results, not a universal runtime or
memory improvement. macOS and the two-CPU Linux VM each passed 81 source and
31 public API tests without leaks. Linux correctness is not cross-host
performance qualification. The reproducible harness remains checked in; raw
reports, source hashes and per-run logs remain under ignored `output/`.

Cross-binary qualification rejected both scratch-only and process-default
OS-backed large-block allocator policies: they introduced confirmed CPU
regressions in ordinary-layout workloads. Those policies are not implemented.
The native concurrent allocator also regressed CPU/RSS on other workloads and
was rejected. The existing default allocator is unchanged. These experiments do
not establish a universal memory win. No process-global malloc tuning or per-request
`malloc_trim` is part of the implementation. Caller-provided allocators and quota
wrappers must retain their ownership and accounting contracts. Lower live engine
memory alone does not establish lower process RSS.

Admission uses both stable bytes and descriptor/ownership overhead. Entry requires
at least 64 KiB of reusable values, half the document, and a 16:1 payload-to-metadata
ratio; an existing layout uses an 8:1 exit threshold. After demotion, three
substantial stable writes are required before readmission. Two spare bits in the
existing content record carry this bounded hysteresis without another key.
These physical-byte criteria do not yet account for every consumer's CPU and
read amplification. A whole-source text index can still dominate a cheap primary
metadata update. Extending admission requires measured consumer dependencies and
scoped effects, rather than a host-specific threshold or an assumption that
payload reuse makes the whole request cheap. Unknown dependencies retain full
source and the established maintenance path.
A conservative owner-level absence hint eliminates repeated schema/ownership
probes on ineligible legacy writes, and format/import changes invalidate it.
Current membership remains durable; retained-pin records are created only when
history actually pins a value and removed when the last pin is released.

The checked-in `scripts/bench/document_payload.py` harness compares the
automatic eligible-write production layout, forced inline, and adaptive layouts. It runs each
layout in a separate process in randomized trial order, records per-process
CPU/RSS and binary hashes, and reports paired bootstrap intervals. Stable,
continuously changing and bursty inputs, repeated/seeded high-entropy payloads,
read/write mixes and configurable sustained rounds are explicit workload axes.
Every timed commit asserts a full WAL sync. Reopen timing includes recovery;
optional Linux cache advice applies only to the fixture's files and is reported
separately from uncontrolled OS caches. `--gate` fails on confirmed regressions
against production; passing these synthetic cases is not a universal guarantee.
`--strict-gate` also rejects inconclusive intervals: each measured metric's upper
95% confidence bound must remain within the regression budget. Reports distinguish
improved, within-budget, inconclusive and regressed conditions. Rollout decisions
need evidence within budget rather than merely an absence of confirmed regressions.
Benchmark fixtures explicitly select the process allocator (`--allocator production`)
or checked test allocation (`--allocator checked`). Normal correctness tests always
retain checked allocation and leak checks. Process-footprint and throughput
qualification uses the process allocator for workload and DB allocations;
checked-mode reports include additional allocator bookkeeping and are separate
diagnostic evidence. The executable still uses test-runner I/O/setup, so these
fixtures are not standalone production-server profiles. Earlier reports that omitted
this axis used checked allocation. Never mix allocator modes in paired comparisons. `--baseline-binary` compares
against an earlier executable and includes a `current-production` condition, so
a change affecting both layouts cannot hide a regression by moving the baseline.
Reports record both binary hashes and malloc-policy environment overrides.
Public benchmarks normally omit the extra manual correctness preflight; ordinary
tests retain it. `--preflight` reproduces that diagnostic probe explicitly and
reports it as a workload setting. `--transforms` varies same-document changes per
batch separately from committed update rounds. `--memory` records phase-level
resident/anonymous/dirty memory, primary cache and memtable sizes, and snapshot
clone counters. Memory instrumentation cannot run with the timing gate. Process
RSS includes indexing, allocations and mappings beyond the primary-cache counters;
these counters are attribution evidence, not a complete allocator census.

The performance target is metadata work bounded by changed metadata, descriptor
count and affected index entries, independent of unchanged payload byte length.
It is not zero total I/O or zero durability cost. Qualification compares full PUT,
metadata patch and full/projection reads across small, changing and reused large
values, plus WAL bytes, allocation failure, snapshots, restart and malformed
input. Cross-version snapshot exchange and the complete public mutation/index
pipeline remain release gates; these local measurements alone do not qualify an
automatic production rollout or retirement of the inline reader.

Earlier payload-only qualification showed large warm update savings and cold-
reopen gate failures on several Linux VM conditions; macOS repeats were noisy.
Those measurements predate the automatic reader-cohort admission and field
producer described above. The new warm indexed-update runs do not requalify those
cold conditions. A 16-row/128-update fixture also drove normal maintenance and
two to four production compactions per trial on both hosts; stable adaptive
writes avoided those compactions. Automatic eligible-write admission is now
implemented, while universal performance qualification and retirement of the
inline reader remain separate requirements. These are synthetic corpora rather
than multi-host or production traffic qualification. Keep these limitations
separate from the already passing storage, transform, identity and public indexed
update correctness checks. Raw performance reports belong in ignored `output/`;
keep reproducible harnesses and interpreted evidence in the repository.

A current certificate permits unchanged leaves to be omitted without loading or
hashing those payloads. Current permissions, revision, query membership and
publication fences still apply. Missing, stale, unsupported or ABI-incompatible
certificates use the full-value reference path. Existing bounded authority
backfill repairs eligible legacy rows without changing their logical revisions.
Opaque/subtree values and source-dependent operators retain reference execution.
Other selected metadata may still require storage-frame reads or a full-layout
fallback: certificate reuse is not a guarantee of zero physical disk I/O.


The CLI retains bounded verified descriptors (digest, MIME, dimensions, length
and server identity), never image payloads, in the existing atomic artifact
cache. Keys include endpoint, table, parent, field and credential context, plus
a build digest of the image validators. The cache uses 256 buckets and four
1 KiB slots per bucket; records can be evicted at any time. Current permissions
and expected revision are rechecked before any descriptor is accepted. Lower
current image-byte/pixel limits still apply. Before grouped retrieval, a bounded score-cache probe identifies fields whose
current question already has a reusable decision. Changed questions request full
values immediately in the same grouped query, avoiding conditional-then-full
round trips. This probe is only a planning hint; actual score use rechecks expiry
after fresh authorization and identity verification. The request retains the
exact verified descriptors it sends, so later cache eviction does not require
reopening them. Expired/evicted scores still force ordinary materialization.
Image constructors retain an immutable URL extent proven to contain only the
supported data-URL prefix and validated base64 alphabet. Exact evidence sizing
counts escaped text/framing and adds the known URL length without rescanning the
payload. Request encoders emit owned framing plus borrowed qualified URL segments
through HTTPx's existing H1/H2 transport. URLs below 4 KiB remain in contiguous
framing to avoid extra frames/syscalls for tiny inputs. Replacing a URL invalidates its extent
proof; unqualified strings retain ordinary escaping. Inputs stay pinned through
completion/cancellation/retries, and the original request-size, concurrency and
response limits still apply. The local inference ABI receives a contiguous body.

Descriptors never enter provider requests: an explicit materialization guard
runs before coalescing and dispatch. Conditional fallback retains ordinary
source/request budgets and avoids charging the same logical image twice.
Conditional-response transfer admission accounts for small identity responses
separately from full-image processing scratch. The shared processing admission
budget remains unchanged. Bucket-striped writer locks serialize cache bucket
read/choose/publish operations within the process, preserving fitting colliding
entries; readers remain lock-free and cross-process races remain safe misses.
Disabling caches also disables conditional identity work. Older servers that
ignore the extension return full values; those rejecting it receive one ordinary
projection retry, charged to the same request budget. No permission result is
cached by this mechanism.

Manifest pages have independent worker leases. Completed lanes can refill across
page boundaries while output stays in discovery order. At most eight active
jobs, sixteen staged results, the coordinator page and one prefetched page are
retained; there is no corpus-sized page or reorder inventory. Explicit finite
evaluation budgets retain serial admission. Every launch, cancellation, decode,
output and drain failure joins borrowers before releasing their pages. The shared
64 MiB conservative image admission pool remains in place; warm reuse removes
transfer and validation work instead of raising that pool's limit.


## Split validation

The explicit-permission guard and default-off source-family admission passed
37 payload tests (one optional benchmark skip) and 34 public indexed-write,
validation, replay and reopen tests on macOS, with no reported leaks. The public
suite includes ordinary large indexed writes retaining ASCH25 and no admitted
source-family/field format, explicit cohort qualification, and reopen of admitted
stores. This validates the rollout guard; it does not qualify the experimental
format's performance or mixed-version cluster rollout.
