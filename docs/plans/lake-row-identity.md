# Lake row identity and index reuse

Status: accepted decision; typed stable-key relocation implemented for Parquet and Iceberg.
Per-index input proofs and incremental identity maps are implemented. Additional
identity adapters and logical row-set delta reuse are staged below.

## Decision

Use one logical row identity contract for native writes and attached lake data:
logical row ID, change version, and current physical location are distinct.
Compact ordinals and physical coordinates remain valid internal index addresses.
A generation-bound resolver connects those addresses to logical rows and their
current locations. Business primary keys are optional identity adapters.

Antfly owns its native write path and serving artifacts, but attached lake files
may be rewritten by independent engines. Moving a row does not require rerunning
analysis, embedding, or index construction when its indexed values are unchanged.
Conversely, the same primary key can have different text, predicates, vectors,
or covering values. Identity alone cannot authorize reuse.

Current serving compaction preserves canonical source coordinates and needs no
new relocation map. The new resolver applies when underlying source rows move.

## Identity adapters

- Native owned rows use the existing engine-owned row identity and committed
  change version; users do not need a new primary-key requirement.
- Iceberg lineage uses a table-scoped row ID plus the row update sequence,
  including inheritance rules. Missing lineage on upgraded historical files
  declines reuse. Equality-delete replacement is removal plus a new row.
- Declared stable-key adapters verify non-null uniqueness using the existing
  typed key semantics and retain indexed-value/content fingerprints. A declared
  constraint without validation is insufficient.
- Trusted writer rewrites supply snapshot-bound old-to-new row mappings and
  unchanged-value evidence. Filename, row count, sort order and offset heuristics
  never establish logical identity.
- Sources without sufficient evidence keep ordinary index construction and
  query behavior. Only affected indexes/files rebuild; table attachment remains
  usable without a primary key. The first relocation implementation certifies
  whole visible ID sets. Each native index can reuse its original artifacts when
  its own captured inputs are unchanged, even when other indexes must rebuild.

## Durable contract

Namespace every logical row ID by table incarnation and identity-adapter recipe.
Separate logical IDs from physical locations, snapshot tokens and index-local
ordinals. Unchanged values retain their version through relocation; updates
change the relevant version/fingerprint. Duplicate and null keys, incompatible
recipes and conflicting lineage must not produce a reuse certificate.

Publish authenticated forward and reverse location navigation with the index
generation. A reuse certificate binds old index generation/recipe, old identity
state, current identity state, visibility/deletion coverage and verified value
equivalence. New locations, identity state, reused artifacts and certificates
advance under the existing catalog compare-and-swap and reader protocol fence.
Uploads without that commit never become readable.

Use bounded spill-backed sorting and immutable page trees instead of an
archive-sized hash map. Resolve selected rows in batches, retain typed covering
payloads, and avoid business-key lookups on each hot posting. Flatten/combine
translations at publication boundaries so lookup depth does not grow with
compaction history. Stable public IDs are distinct from snapshot-fenced cursors. Existing public
physical IDs retain their current compatibility contract until logical public
identity is enabled across every reader and cursor path.

## Consumer and retention rules

Text, scalar predicates, ordered scans, vector/sparse retrieval, algebraic state,
deletes, covering payloads, highlights and public identity must agree on the same
pinned logical cut. Reverse membership must translate current locations back to
index addresses as well as translating candidates to current locations. A
text-only mapping is not a complete implementation.

Existing reader leases retain the identity/certificate closure alongside index
artifacts. Current readers retain current source locations; older pinned readers
retain their own locations. GC must not retire either closure while its reader
can still serve queries. Cloud generations and retention certificates cover the
additional artifacts through the same publication-bound reachability rules.
These leases protect Antfly's derived artifacts. External owners still control
source-file/version retention; a pinned query fails source validation if its
required source version has been removed or replaced. A relocation certificate
does not manufacture retention guarantees for independently owned files.

## Cost and failure behavior

Mappings reduce index reconstruction at the cost of navigation bytes, validation
and translation. Stable-key comparison can still require reading new files; it
avoids expensive index work rather than claiming a free discovery path. Favor
existing lineage and writer proofs when available. Keep sparse lookup and broad
scan paths costed by requests, bytes, decoding and cache residency.

Missing evidence or optional construction admission selects rebuilding. Corrupt
authenticated maps, mismatched domains/snapshots and conflicting proven identities
are integrity errors; they do not silently reuse old postings. Cancellation and
failed publication drain work and release spill/reservations. Resource limits,
result limits and source-version validation remain in force.

## Implemented publication and query path

An external source may opt into verified keys:

```json
{"kind":"external","table_id":"events","format":"parquet",
 "uri":"s3://bucket/events/","row_identity":{"key_columns":["event_id"]}}
```

The same `row_identity` option applies to Iceberg sources. Keys use native typed
index encoding, including composite keys and integer precision beyond 2^53.
Unknown or unsupported key columns are rejected when binding the typed schema.
Null or duplicate row keys decline optional relocation reuse; they do not make the
table unavailable or imply an enforced SQL primary-key constraint.

The builder reads only stable-key columns and the union of native index inputs
from delete-aware batches. Typed vector kernels hash integers without a floating
point conversion and distinguish SQL null from JSON null. Dictionary values are
hashed by value. A per-index recipe includes keys, predicates, covering fields,
stored source and aggregate cohort inputs. Generated embeddings or remote media
with uncaptured dependencies cannot receive a reuse proof.

Identity state uses bounded native spill sorts and immutable copy-on-write trees.
Stable file slots and per-file membership let an authenticated file delta retain
unchanged rows without sorting or rebuilding the entire identity map. A separate
ordered membership tree supplies canonical ranks without rewriting every retained
row on append. Identity construction and compact translation construction share
the node CPU scheduler and park CPU admission during artifact I/O.

Optional identity work has a 64 MiB working allowance, bounded by retained-state
admission independently of its smaller CPU execution lease, and `lake_indexes.row_identity.max_spill_bytes` defaults to
1 GiB. Zero disables optional construction. Local working-set/spill exhaustion,
oversized root evidence, null keys and duplicate keys decline the optimization;
ordinary complete native index construction continues. Cancellation, backing
allocator failures, source failures and corrupt authenticated evidence still
propagate. These limits are separate from SQL query limits.

A rewrite with the same visible logical IDs proves reuse separately for each
native declaration. A label-only change can keep a body-only text index and raw
vectors while rebuilding covering rows or an index that stores the complete
source. Shared aggregate cohorts certify the union of their inputs. The directory
can therefore contain both fresh and retained indexes. Each retained declaration
names its exact original coordinate space; later rewrites flatten directly to
that space, even when different indexes originated in different snapshots.

The certificate binds the declaration closure, current source cut, namespace,
recipes, input evidence and exact name-to-space bindings. A shared publication
owner exposes borrowed per-index resolver views. Snapshot labels alone never
select a coordinate space: a provider can rewrite a snapshot under the same label.
Forward hydration, reverse predicate membership, covering/highlight fields,
dense/sparse retrieval and SQL consumers all use the selected index view. Fresh
indexes in a mixed directory retain normal pruning and ordering fast paths.
Compressed predicate blocks from a fresh metadata index are batch-translated
before applying a retained text index’s ordinal kernels. Blocks are collected
into one compressed mask before selecting the translation direction. A broad
mask probes only candidates from the admitted text window; a small mask uses
reverse translation. Thus broad overlay masks do not expand and repeat across
all windows. Metadata and text indexes with the same authenticated coordinate
space intersect their compressed blocks directly, including retained indexes;
different original spaces always use the translation path.
Selective predicate pack proofs use shared encoded spill runs to avoid repeatedly
merging an ever-growing prefix. Predicate block directories use shared multi-key
runs too, preserving sibling file capacity without repeated prefix rewrites.

Complete rewrites with no retained files rebuild identity trees directly from the
fresh stream. They neither erase old per-file row memberships nor point-probe old
IDs. Partial rewrites compare incoming sorted IDs with one forward-only cursor,
so collision verification retains authenticated paths across rows.

Reuse proofs compare authenticated tree ranges and skip identical copy-on-write
subtrees. Equal authenticated page contents can skip evidence reads even when
upload attempt addresses differ. Changes to tree shape use an exact ordered
comparison. Unchanged per-index input trees therefore need no repeated leaf scan.
New compact translations contain only 16-byte source and destination coordinates;
canonical rank stays in the current ordered membership tree. A later rewrite
compares the previous and current authenticated ID trees, skips unchanged COW
subtrees and applies only changed addresses to the prior forward/reverse maps.
Value-only changes leave those maps intact. Swapped addresses resolve removals
before final insertions across every spill run. First construction and upgrades
from historical rank-bearing maps still use bounded spill sorting. Admission
failure keeps the authenticated ID-map reader fallback.

Batch translation sorts probes and retains cursor paths; duplicate inputs share
work. Each operation owns its cursor/page-cache state. Shared decoded-page leases
avoid repeated parsing and authentication of immutable pages. The owner mutex
covers budget accounting, tiny memo updates and metadata installation; remote
map reads and cold navigation loads run outside it. Every access still checks
the pinned reader's authority and deadline. No query materializes the complete
row map. Covering hydration groups requested rows by block and reads each
authenticated block once, preserving result order.

Optional predicate planning admits at most 4,096 relocated rows and 16 MiB of
map reads across both metadata-to-current and current-to-text conversion, reserving 32 MiB of the remaining query
allowance for required execution. Broad predicates decline before expansion.
Optional readers can borrow shared decoded pages but never lead a shared cache
flight with a narrower planning budget. A declined prepass cannot propagate its
local quota failure into a concurrent required reader. Text falls back to exact
candidate-window membership. Sparse vectors intersect compressed native ordinal
masks directly when metadata shares the authenticated index space, use bounded
reverse translation for selective current-space masks, and evaluate broad
predicates in reached native ordinal windows. Include/exclude masks share one
conversion allowance and are published only when complete; backing allocation
failures and cancellation propagate.

Reader leases, GC traversal, cloud publication certificates, startup navigation
warming and recovery include all current and original navigation/map roots and
compact translations. Protocol 69 additionally fences readers that cannot interpret
logical native fragment spaces; protocol 68 introduced rank-free coordinate maps.
Historical rank-bearing map certificates retain their original serialization and
remain readable. Reconciliation probes bounded unique navigation roots. A deeper
reader failure records a generation- and publication-token-bound repair hint;
the coordinator rebuilds that publication without reuse. Stale hints cannot
invalidate a later publication. Hints are process-local and are rediscovered by a
failed reader after restart.

Existing physical public IDs remain unchanged; the logical ID codec is internal
and does not introduce a public stable-ID promise. Worst-case identity verification,
fragment census hashing and proof comparisons still scale with row count; unchanged
authenticated subtrees skip verification reads. Cross-file rewrite savings must be
measured against this verification/publication cost.

## Native delta reuse across membership changes

Native publication now persists a logical fragment space independently of physical
Parquet files. A radix census starts with sixteen 4-bit prefixes and splits a leaf
above 8,192 live IDs, retaining unaffected boundaries and empty leaves. The census
is capped at 16,384 fragments under publication admission. Each fragment version
binds the verified ID and captured input-value fingerprints, excluding physical
coordinates. Native removal/addition and contribution paths replace only affected
fragments, updating postings, collection statistics, dense/sparse membership,
covering tuples and algebraic retractions in the same atomic publication.
Fresh sparse fragment keys append native immutable posting segments, including
incremental generations. Scalar insertion would repeatedly requantize retained
chunks and accumulate score error. The v9 sparse writer binding migrates older
checkpoints on their next rebuild; it does not change the reader's wire codec.

Coordinates within a fragment are collision-free ordered ordinals, never truncated
ID hashes. A changed fragment is replaced completely before its ordinals can be
reused. Per-index equivalence proofs still retain exact artifact roots for indexes
whose captured inputs are unchanged. Retained indexes can therefore refer to older
fragment spaces; each index binds its own authenticated resolver and coordinate map.

Construction gathers changed fragments through bounded 256-row physical selections
and the existing projected replay shared by native builders. Each consumer owns a
parallel physical reader. A typed scatter restores logical order across physical
file/page boundaries before exposing each tile; wide integers, decimals and NULLs
retain their native representations. Expanded payloads above an 8 MiB target
retry with fewer selected rows before copying, down to one row subject to the
existing decoder and native construction limits. It does not scan the archive for every fragment. The exact
verified census supplies COUNT metadata without interpreting internal fragment
identifiers as source object paths. Invalid/missing identity evidence or exhausted
optional construction admission selects ordinary complete physical rebuilding.

Only retained aggregate declarations keep prior contribution roots live; replaced
roots retire through counted ownership. Aggregate deltas require the seed's
original authenticated fragment census. Older retained censuses reconcile file
metadata and reuse matching contributions rather than assuming a deletion map.

A new fragment space, its navigation, native mutations, contribution roots and
coordinate maps enter the same publication certificate, reader lease, GC and restart
closure. Rank-free maps update the union of changed IDs on both the old/new native
and physical sides; additions take priority over deleted addresses during swaps.
They retain unchanged COW pages rather than re-sorting all rows after membership
changes. Canonical tie ranks come from current physical membership. Competitive
text hits resolve ranks in batches of 64; each operation owns its cache and shares
map/tree paths without holding a mutex across remote I/O.

Selective dense constraints also cross the native owner boundary as complete
include/exclude vector-ID sets (at most 4,096 IDs in a 4 MiB planning workspace).
The planner verifies native metadata against each physical key before accepting a
hashed vector ID. Partial plans retain the exact residual predicate; declined
translations never reinterpret current coordinates as historical native addresses.
Broad selections keep bounded candidate membership evaluation.

Real Parquet and Iceberg E2Es exercise append, removal, per-index input changes,
mixed original spaces, forward/backward pages and restart against fresh-build
oracles. These are correctness qualifications, not archive-scale throughput or
live-cloud retention measurements.

## Staged extensions

- Connect native Iceberg v3 manifest/row lineage to the shared identity contract.
  The inheritance rules are modeled and tested, but the current reader does not
  claim automatic v3 lineage reuse.
- Accept independently authenticated, snapshot-bound writer mapping proofs.
- Enable stable logical public IDs only with complete lookup, overlay, cursor,
  filtering and compatibility coverage.

These extensions must preserve the same proof and publication boundary; none
may infer identity from file layout or silently trust a user-declared key.

## Validation

`zig/e2e/antfly/test_lake_row_identity.py` runs independent PyArrow Parquet and
PyIceberg rewrites against fresh-build oracle tables. It checks repeated
relocation, per-index value changes, mixed original snapshot spaces, append with
shifted file slots, removal, restart, text scores, indexed filters, sorting, forward/backward paging, covering/highlight fields,
dense/sparse membership and SQL aggregation. The Iceberg parameter participates
in `e2e-full`. Native tests cover spill bounds, duplicate keys, proof tampering,
flattening, reader-safe GC, certificate binding, cancellation, allocation failures,
missing derived navigation, incremental coordinate swaps, zero-work map reuse,
parallel cold-page reads, bounded optional read admission and relocated sparse
predicate fallback. Fragment regressions cover isolated append/removal, radix
splits, typed cross-page ordering, wide Parquet tile retries and artifact closure.
Ten successive sparse replacement rounds verify original vector scores within
one quantization bound; the old scalar writer fails that regression. Sparse E2E
scores use the existing per-block 8-bit tolerance and deliberately separated top
scores, retaining exact result-ID and ordering checks across different layouts.

Further qualification should exercise Parquet and Iceberg rewrites that preserve rows, arbitrarily
reorder rows, update indexed and unindexed values, remove rows and introduce null
or duplicate keys. Verify exact search scores, metadata filters, ordered/reverse
paging, vectors, aggregates, covering fields, highlights and public IDs against a
rebuild oracle. Check restart, old-reader leases, publication failure, mapping
composition, reader-safe GC and bounded memory/spill with allocation failures.

Qualification on the final implementation with main `10899c8e9b`: both real
Parquet/Iceberg E2Es passed in 350.10 seconds, including append/removal and three
restarts per format. Twelve targeted lake tests passed without leaks, including
aggregate root retirement, census compatibility, key-namespace migration,
wide Parquet gathers and sparse numerical stability. The shared ranking/filter
suite passed 43 tests (one benchmark skipped), and the native dense callback
regression passed. Archive-scale throughput and live-cloud retention remain
unmeasured.

## Prior art

Lance fragment reuse defers physical index remapping through verified transitions:
https://lance.org/format/index/system/frag_reuse/

Databricks row tracking separates stable identity from update versions:
https://docs.databricks.com/aws/en/tables/features/row-tracking

Iceberg defines lineage and inheritance, including the limits of historical
upgrades and equality-delete updates:
https://iceberg.apache.org/spec/#row-lineage

Elasticsearch exposes logical document IDs while Lucene document numbers remain
ephemeral. Antfly additionally needs an explicit contract for externally owned
file rewrites.
