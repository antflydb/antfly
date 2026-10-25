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

Per-row evidence is keyed by a semantic input cohort (schema and the sorted,
deduplicated input column set), rather than by declaration name or the table's
whole index configuration. Declaration names and configuration semantics have
separate authenticated bindings. Equal input cohorts share one evidence tree
range; overlapping cohorts hash each typed column value once per 64-row tile. Changing one declaration still requires its own matching
configuration proof, without invalidating unrelated declaration evidence.
Legacy per-declaration evidence remains readable and is rebuilt on migration.

The authenticated root separately binds the publication's `definition` signature
and its membership recipe. Reader protocol 73 introduces evidence format 3:
identity is independent of projected values, and each cohort separately certifies
its exact typed inputs. Older roots retain their historical recipe and hash
semantics and rebuild before entering this compact format. Changing declaration
names, adding an index on an existing cohort, or changing configuration without
changing inputs retains existing membership and cohort evidence.

A new cohort over retained, provider-qualified files reads only its input columns
and stable keys. An ordered join against authenticated physical membership rejects
changed IDs, coordinate mismatches, duplicate lanes and omitted rows. Changed
physical files still verify every current cohort. Retired cohort ranges are
removed without rereading source payloads. Adding or removing projections therefore
does not invalidate membership or unrelated evidence. Whole-directory reuse still
requires the current desired declaration set, matching definitions and complete
value evidence; key equality alone never certifies index reuse.

Identity state uses bounded native spill sorts and immutable copy-on-write trees.
Compact ID records store only their 16-byte physical coordinate. A single reverse
membership map stores coordinate-to-ID and also supplies per-file membership;
value fingerprints live only in the cohort evidence tree. Ordered membership
remains separate to support exact canonical ranks. File dictionaries participate
in coordinate-space identity, so a slot-preserving rename cannot accidentally
make two physical spaces interchangeable. Legacy record widths remain readable.

Compact maps opt into authenticated page-local key prefixes, reducing repeated
file/cohort bytes without changing logical keys, cursors or subtree counts. The
same codec applies to private spill forests and durable pages. Decoding bounds
the expanded key buffer independently of encoded size, and query scheduling
accounts for expanded keys and entry descriptors. A 256-row, one-cohort regression
measures 125,380 bytes across legacy navigation versus 54,270 compact bytes (57%
less), including distinct reachable page roots. This is a fixture measurement,
not an archive throughput claim. Stable file slots and per-file membership let an authenticated file delta retain
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

Full verification of at least 128 rows can also capture the projected typed
payloads during that same source pass. A bounded native external sort clusters
those payloads by logical ID before writing private compressed 64-row pages.
The sort keeps typed payloads inline through its spill/merge passes: an indirect
offset-only sort would preserve physical scan order and defeat read locality.
Once keys, duplicates and root admission are verified, construction merges the
captured logical-ID/offset stream directly with the native ID census and sorts
only the final fragment coordinate/offset routes. Adjacent selected radix leaves coalesce into
half-open joins; untouched ranges are never scanned again during adoption. The
capture first certifies its complete route count, and every selected range checks
matching IDs, exact native coordinates and expected row counts. The selective
regression visits 17 of 257 rows instead of all 257. No physical-to-logical
intermediate tree or physical census reread is needed. Broad native rebuilds
reuse those pages instead of reading and decoding Parquet again. Decimal payloads
keep exact 128-bit coefficients and scale; compatible precisions widen to their
maximum. Mixed types/scales decline the optional spool. Each producer batch is
validated and sized once; bounded selections reuse immutable row costs instead
of revalidating every dictionary for every captured row. Partial file verification
continues to use selective gathers or a separate broad spool, since its capture
would not cover unchanged files. The optional capture has a separate 64 MiB
retained admission, uses at most half the configured spill quota during verification,
and leaves only the remaining spill allowance to routing afterward. Exhaustion
drops the capture and preserves the existing complete construction path. A pure
relocation may discard unused captured payloads; this trades bounded temporary
writes for avoiding a second source pass when payloads have changed.

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
compact translations. Protocol 70 introduces a v2 identity root with shared
coordinate-space/translation descriptors and compact declaration bindings.
Descriptors coalesce by content even when independently loaded. The complete
prospective binding set is admitted before native payload construction; a final
mixed-space overflow retries ordinary physical construction with identity disabled.
Historical v1 certificates retain their exact serialization. Protocol 69
additionally fences readers that cannot interpret
logical native fragment spaces; protocol 68 introduced rank-free coordinate maps.
Historical rank-bearing map certificates retain their original serialization and
remain readable. Reconciliation probes bounded unique navigation roots. A deeper
reader failure records a generation- and publication-token-bound repair hint;
the coordinator rebuilds that publication without reuse. Stale hints cannot
invalidate a later publication. Hints are process-local and are rediscovered by a
failed reader after restart.

Existing physical public IDs remain unchanged; the logical ID codec is internal
and does not introduce a public stable-ID promise. Worst-case identity verification
and proof comparisons still scale with row count for a complete external rewrite; unchanged
authenticated subtrees skip verification reads. Ordinary append/update census planning
uses COW differences and rehashes only affected radix leaves. Cross-file rewrite savings must be
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
chunks and accumulate score error. Publication compacts pressured size tiers of
32 posting/docmap segments, both during ingestion and before checkpointing, rather
than rebuilding the historical base after every small update. Compaction reads
the exact retained forward vectors once into a bounded external (term, ordinal)
sort; it never requantizes previously decoded weights. The ordinary live-incarnation
proofs and atomic native compaction publication continue to govern visibility.
The v9 sparse writer binding migrates older
checkpoints on their next rebuild; it does not change the reader's wire codec.

Coordinates within a fragment are collision-free ordered ordinals, never truncated
ID hashes. A changed fragment is replaced completely before its ordinals can be
reused. Per-index equivalence proofs still retain exact artifact roots for indexes
whose captured inputs are unchanged. Retained indexes can therefore refer to older
fragment spaces; each index binds its own authenticated resolver and coordinate map.

Census planning walks COW differences of verified logical IDs and input-version
trees. It ignores coordinate-only changes and equal records in changed pages.
Per-input cohorts share fingerprints only when their captured column sets match;
separate authenticated navigation roots give each native builder exactly its own
fragment versions and private identities. Ordered and aggregate builders fuse
only consumers sharing that census. A body-only update therefore does not
replace neighboring vector fragments. Splits and missing/legacy input censuses
rehash the affected ranges conservatively.

Construction chooses between two bounded source paths. Selective changes use
256-row physical gathers and the existing projected replay. When at least one
eighth of a source with 128 or more rows needs rebuilding, one delete-aware
projected physical pass clusters typed payloads by logical coordinate with the
same bounded external sorter, then partitions compact references into private
spill trees. A complete verification
capture already owns those payloads, so any nonempty changed selection can reuse
it without another physical pass, including selections below the broad-spool
threshold. Independent builders then
read those partitions without rescanning or decoding Parquet. The spool shares
a 64 MiB retained-memory reservation and the configured identity spill quota;
quota denial selects ordinary gathers. It is attempt-private and is never a
serving artifact or a durable recovery checkpoint.

A typed scatter restores logical order across physical file/page boundaries.
Wide integers, exact decimal coefficients, NULLs, and shared string dictionaries
retain their representations. Expanded payloads above an 8 MiB target split
before copying, down to one row subject to existing decoder/native limits. Spool
payload pages hold at most 64 rows; reference/scatter tiles hold at most 256.
The exact verified census supplies COUNT metadata without interpreting internal
fragment identifiers as source object paths. Invalid/missing identity evidence
or exhausted optional construction admission selects complete physical rebuilding.

Only retained aggregate declarations keep prior contribution roots live; replaced
roots retire through counted ownership. Unchanged aggregate graphs stay rooted
across unrelated index-definition changes in the same logical namespace, even
when the global desired fingerprint changes. Aggregate deltas require each
declaration seed's original authenticated input
census, including its source, schema, snapshot, file count and content cut. The
publication owner binds these proofs before parallel reducers start. An aggregate
retained across unrelated updates can therefore use a delta against its own older
input census. A mismatched cut safely reconciles metadata and matching contributions
instead of applying an unrelated deletion map.

Tree comparisons use two lazy ordered subtree frontiers, retaining authenticated
references through unequal fences and heights. Shared ranges become skippable as
soon as the frontiers align again. Balanced page splits and a quarter-page repair
threshold avoid cascading tiny-tail rewrites. Boundary insertion/deletion tests
measure page reads as well as changed records; logical equality still comes from
exact ordered records when independently packed trees share no subtree hashes.

A new fragment space, its navigation, native mutations, contribution roots and
coordinate maps enter the same publication certificate, reader lease, GC and restart
closure. Rank-free maps update the union of changed IDs on both the old/new native
and physical sides; additions take priority over deleted addresses during swaps.
They retain unchanged COW pages rather than re-sorting all rows after membership
changes. Canonical tie ranks come from current physical membership. Text rank
lookups wait for equal-score comparisons or final winners; repeated score classes
resolve in batches of 64. Each operation owns its cache and shares map/tree paths
without holding a mutex across remote I/O. Exact multi-seeks,
lower-bound covering seeks, and batched ranks partition requests by authenticated
child fences. Independent children load in waves of at most eight through shared
query I/O admission. Each wave joins on success, failure, or cancellation; budget
owners synchronize shared charges or reserve the whole wave before spawning.
Decoded row-map leases remain shareable across concurrent readers. Covering
hydration also batches reverse/forward lookups and reads up to four distinct
payload blocks in a wave, while preserving ranked result order and performing
all block/key checks before exposing a row. Each bounded decode lane requests
16 MiB, so concurrent reads fit the scheduler's default 32 MiB I/O allowance.
The wave retains its aggregate pull reservation until its decoded leases have
been consumed and released. Narrow decode flights have distinct cache identities;
a local size decline selects ordinary source hydration, while genuine backing
allocation failures propagate. Cold-block regressions assert actual overlap,
joined read failures/cancellation and complete admission release. Required inline
fallbacks remain available when scheduling admission is full. No publication CPU/I/O capability
is inherited by these independent query workers.

Selective dense constraints also cross the native owner boundary as complete
include/exclude vector-ID sets (at most 4,096 IDs in a 4 MiB planning workspace).
The planner verifies native metadata against each physical key before accepting a
hashed vector ID. Verification uses one pinned native read transaction and a sorted
batch metadata lookup, with exact key comparison before accepting each ID.
Partial plans retain the exact residual predicate; declined
translations never reinterpret current coordinates as historical native addresses.
Broad selections keep their compressed include/exclude masks. Native HBC
filters metadata in 128-candidate tiles, translating each tile through the
per-index resolver before exact membership evaluation. File lookup is shared
inside each tile, while the legacy scalar callback remains available to older
owners and singleton paths. There is no archive-sized dense ID expansion.
Cancellation and translation failures abort the search rather than publishing a
partial selection.

Real Parquet and Iceberg E2Es exercise append, removal, per-index input changes,
mixed original spaces, forward/backward pages and restart against fresh-build
oracles. These are correctness qualifications, not archive-scale throughput or
live-cloud retention measurements.

## Packed immutable navigation

Reader protocol 74 permits native lake publication builders to concatenate up
to eight navigation pages into one immutable object (at most 8 MiB). Each page
reference still commits its own digest, byte length, record count, height and
attempt, and additionally binds the pack digest, object length and range offset.
Sorted builders upload a complete batch and install its locators before encoding
parents. Failed or interrupted uploads expose no partial root; standalone writes
and old references keep their previous representation. Copy-on-write changes can
reuse packed siblings without rewriting their containing object.

Cold point reads fetch only the authenticated page range. Shared memory/disk
cache keys include the object identity, offset, length and individual page digest;
decoded-page keys include the complete locator. Packing reduces publication
object and PUT fan-out. It does not by itself guarantee fewer GETs for unrelated
point seeks, and no network-latency claim is based on local measurements.

Collectors retain the complete containing object but expand each logical page
independently. Resumable marks distinguish page digest, offset and interpretation,
so visiting one packed sibling cannot hide another sibling's payload dependencies.
Cloud generation maps record actual pack uploads, and publication-bound retention
certificates audit that same closure. Reader leases keep old packs reachable until
their last retained publication retires. Graph topology/document-fact writers
continue using their standalone-page publication path.

## Dependency contracts and bounded serving navigation

Reader protocol 75 and the v3 input-cohort/v4 producer recipes use a shared
canonical schema projection. A field index binds the declared input paths and
their interpretation: types, nullability, ancestor metadata, analysis, and
remaining global table semantics. Object declaration order is immaterial.
Unrelated properties and their `required` entries do not invalidate that cohort.
Table schema version, base-source descriptors, and ordered-index declarations
remain separately bound by publication, source, identity and declaration proofs.
They are not copied into every row-input contract.

Whole-row text/source retention and templates bind the complete row contract.
Uncaptured enrichments remain ineligible for row-input reuse. Unknown paths,
references, schema composition and conditional dependencies conservatively retain
the full schema. Temporary parsing/canonicalization uses reclaimable workspace
under the enclosing quotas; retained metadata arenas keep only the output
contracts, rather than every prior schema parse. Adding an unrelated nullable
table column therefore preserves
selected text/vector/sparse cohorts; changing a selected field's type or analysis
requires new evidence. This does not remove the physical inventory schema
fingerprint from file identity, or permit reuse across an unverified source
rewrite. Iceberg declarations still require a matching field in the pinned
Iceberg schema; the real-file regression exposes an existing nullable source
field while retaining unrelated native indexes. Older protocol publications
reconcile before serving the new contracts.

Compact row translation batches both dictionary proofs (file digest to slot,
then slot to exact filename), translation keys, canonical rank keys, and output
filenames. Ordered public tie participants likewise resolve file dictionaries in
bounded waves before entering the spill sort. Duplicate and shuffled coordinates
retain caller order. Single-row callers keep the small slot memo; a broad tile
does not depend on that memo fitting every file.

An ordered reader credits at most 512 distinct warm dictionary page identities.
Every lookup still checks live authority and cancellation. Its first encounter
consumes the logical read allowance, and cold rereads after eviction always pay
again, including extra bytes in coalesced ranges. Overflow receives ordinary
charging. This avoids repeatedly charging a warm ancestor during a wide tie,
without removing operation read limits or growing an archive-sized ledger.

Sort leases expose a reusable execution-batch view with typed physical columns,
frozen values and raw JSON capabilities. Payload clustering uses that view for
both resident and spilled rows. Descriptors and fixed-width gathers belong to
bounded batch scratch; underlying payload/dictionary bytes remain leased until
append completes. Supported primitive columns bypass scalar `Datum` decoding;
other logical types retain the scalar fallback and exact null/decimal semantics.

Ranked collectors retain 128 exact score tags across candidate tiles. A recurring
competitive score class can batch its rank requests even when each tile contains
only one candidate in that class. A tag collision drops this optional hint and
uses the exact heap path. Distinct scores still perform no rank lookup until a
comparison needs one or final winners are delivered. Heap/cursor order and error
propagation remain authoritative; hints never decide row ordering. The deterministic
4,096-candidate/512-winner regression reduces rank-resolution callbacks from 898
to 124 with identical results. These are callback counts, not cloud latency
measurements.

Inline aggregate navigation owns every write callback when rebinding its store
context, including packed writes. Larger external trees retain ordinary packed
publication. Concurrency qualification measures independent physical uploads;
a complete navigation tree fitting one pack legitimately has one upload.

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

Qualification with main `ddba6584b3`: all three real Parquet/Iceberg and mixed
precision decimal E2Es passed in 345.91 seconds, including append/removal,
filtered text/vector/sparse queries, forward/backward paging, highlights and
three restarts per rewrite format. Sixteen focused lake tests and all 84 graph
page tests passed without leaks. Definition-refresh regressions block Parquet
reads during evidence rebinding and check index addition, rename, removal and
unchanged artifact retention. Batch-read regressions cover actual filesystem
page waves, duplicate/missing candidates, lower-bound gaps, ranks, cancellation,
joined read failures and exhaustive allocation-failure cleanup. Catalog,
error/source-catalog, Apache boundary, license and formatting checks passed.
Archive-scale throughput and live-cloud retention remain unmeasured.

Protocol 73 qualification: all three real-file Parquet,
PyIceberg and mixed-precision decimal E2Es passed in 413.57 seconds, including
new-cohort index publication after external rewrites and restart. The tests assert
compact membership remains stable across value-only updates while input-specific
navigation changes. All 85 graph/page tests passed, including prefixed/legacy
interoperability, malformed prefix framing and exhaustive allocation failures.
Twenty-one focused lake tests plus three supplemental membership/identity checks
passed without leaks. Cold covering waves also verify joined failures,
cancellation, retained admission and decoder size declines without poisoning
ordinary reads or classifying genuine allocator failures as optional limits.
Latest main `864910137a` was then merged. The CLI rebuild, regenerated Go SDK
tests, joined OpenAPI, Rust SDK spec, catalog and error checks passed on the merged
result. Archive-scale throughput and real-provider retention qualification remain unmeasured. The
1 GiB default remains an explicit scratch quota; larger archives may require
more configured scratch capacity even with compact navigation.

Protocol 74 qualification with main `f95a5a5820`: all three real-file Parquet,
PyIceberg and mixed-precision decimal E2Es passed in 355.62 seconds. The two
8,193-row hashed-ID spool regressions cover both capture paths and repeated
consumers, bounding payload decodes to four times encoded pages; the old physical
layout measured 27.57 times on that fixture. This is a decode-locality regression,
not an archive throughput benchmark. Allocation-failure sweeps cover covering
entries, exact decimal/raw-JSON payloads and packed builders. All 88 page/storage
tests passed, as did the native dense batch/scalar equivalence and cancellation
checks. Cloud generation/certificate tests cover packed range reads after restart,
all sibling dependencies, durable per-page expansion marks and retained readers.
The main merge keeps borrowed membership pins and batched relocation ranks; a
combined regression and the filtered-top-k suite passed. Generic and typed page
expansion have independent marks in both synchronous and resumable GC. Full
archive throughput and live-provider retention qualification remain unmeasured.

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

## Navigation and replay execution refinements

Native metadata mutations consume one validated ordered stream against a lazy
old-tree frontier. Unaffected half-open routing intervals retain their existing
page references; intersecting leaves and routing boundaries flow through one
bounded sorted builder. Touched bulk leaves retain their larger packing size
class instead of splitting into ordinary small pages. Page arenas and reusable
order-check storage avoid per-record allocation/free traffic. There is no
mutation-count threshold that forces a full
old-tree replay, and no repeated publication of a root for each 256 changes.
The old root stays authoritative until the existing publication CAS succeeds.
Producer errors, duplicate/out-of-order edits, cancellation and upload failures
cannot expose a partial replacement.

Cold query navigation groups nearby missing ranges from the same immutable pack
into physical requests of at most 1 MiB, allowing gaps of at most 16 KiB. It
retains individual page digests and cache identities. Both logical page reads
and extra gap bytes consume read allowance. Distinct physical ranges use the
shared I/O scheduler. Identical or containing in-flight ranges share the existing
physical-flight coordinator even when residency is denied. Each waiter validates
its own page slices. All jobs drain before an error or owned result escapes.
Warm decoded row-map pages bypass prefetch; warm encoded pages require no new
provider request. Row-map prefetch holds an independent bounded retained-memory
reservation through consumption and declines to the ordinary page path under
admission pressure. Ordered reverse/forward navigation uses the same coalescing
contract. This changes neither object generation selectors nor reader authority.

Publication replay retains an operation-owned, evictable cache of up to eight
decoded typed payload pages with an 8 MiB residency target. Independent index
configurations share those immutable pages, including decimal normalization.
Projected scatters copy only their requested columns once; contiguous selections
borrow sliced vectors through explicit page leases. Small selections still copy
to avoid pinning disproportionate payloads. Eviction releases the cache's lease,
not leases held by active builders. Required decode can evict optional residency
before retrying known working-memory exhaustion. The spool retains its capture owner explicitly, so releasing the caller's
capture handle cannot retire payloads still leased by replay. All consumers drain
before spool teardown; no payload cache becomes durable serving authority.

Native vector residuals probe each indexed predicate over one candidate tile.
Current-to-index relocation and reverse navigation are batched, shuffled and
duplicate inputs preserve their positions, and conjunctions short-circuit only
rows already rejected. Tile work is admitted before probing. Exhausted point-read
allowance discards partial tile output and selects complete exact predicate
materialization. Integrity, snapshot and cancellation failures still propagate.

Ordered forward/reverse readers share immutable decoded navigation pages across
predicate tiles and queries. Ordered and row-map page residency shares a separate
16 MiB/256-entry navigation pool (at most one quarter of the encoded-cache
allowance), so pinned tree paths cannot displace compact file-order metadata.
Loading still uses bounded shared decode admission. Keys include the store scope, reclamation domain,
full page commitment and packed locator. A warm lease still checks the caller's
live authority, cancellation and logical read allowance. Cold decoded waves use
the same coalesced encoded range path; cache loading pressure falls back to a
required independently owned page. No request-owned descriptors enter the cache.

Deferred text/sparse predicate producers use bounded 128-candidate tiles for
native-to-current relocation and reverse membership, matching the vector
residual path. Scratch is reclaimed per tile. A probe or scratch allowance decline
discards all partial output before complete exact materialization; snapshot,
integrity and genuine backing allocation failures remain errors.

Verified fragment spools advertise an explicit independently replayable typed
source capability. Serial and file-major replay open projected cursors directly
over that frozen route instead of encoding a second spill run. Ordinary sealed
disk replays share up to eight decoded typed pages, bounded by 8 MiB and one
quarter of their replay quota, with exact whole-block resume positions. Page and
column leases retain the replay until release, including after cursor close;
reset cannot retire a leased file or allocator. Cache eviction preserves consumer
leases. Frozen runs in one file-major window can reclaim each other's optional
residency on known quota exhaustion. Reclamation releases the current cache lock
before acquiring peer locks, so it cannot form lock cycles. A later file cannot
be denied solely by an earlier file's idle cache.

All page-retention capabilities report the entire parent ownership chain, so a
small projected descriptor cannot conceal a large payload/dictionary. Manifest
prefetch uses an independently bounded child allocator that preserves every
enclosing build and host quota, in addition to its scheduler reservation. A
point-in-time headroom observation never authorizes allocation outside that
chain. Concurrent required work can decline speculation; joining it releases all
state before retrying the required read.

Filesystem retention inheritance and sealing share a blocking mutex per store
handle. Parallel native families cannot race pending-root replacement, queue
reallocation or arena destruction; sealing observes a complete batch. Durable
certificate writes remain atomic and namespace epochs still invalidate changed
payloads. An optional prefetch quota decline cannot override a later explicit
source, integrity or lease failure in publication diagnostics.


Ranked collectors defer relocation tie lookup while scores alone determine heap
and cursor order. Bounded tiles batch repeated score classes; a heap resolves
cross-tile ties before comparisons, and final winners are prepared before pure
sorting or delivery. Sift paths are planned before mutation, so navigation or
allocation failure cannot leave a partially reordered heap. Scalar consumers
also support batch-only resolver capabilities. The 257 distinct-score regression
resolves only its three winners; equal-score workloads preserve their batched
navigation and canonical pagination.

Private native segment reads park CPU admission before waiting on a blocking
`std.Io.Mutex`. The lock protects shared file buffers and is released before CPU
admission resumes, including read failures. Independent prefetch reads use the
same lock without borrowing the serial builder's scheduling callback. No file
lock spans a CPU/I/O admission transition.


### Exact mutations inside stable fragments

The input-version COW difference now also identifies changed logical rows.
Admitted deltas prove unchanged fragment membership and the same old/current
native coordinates. They are bounded to 32,768 identities per cohort; a missing
proof keeps the existing whole-fragment path. Input fragment versions use
canonical bucketed Merkle commitments. An affected radix bucket rehashes its
ordered IDs and captured input fingerprints; untouched buckets retain their
commitments. XOR composition is never evidence of unchanged inputs.

Ordered/covering indexes retire exact coordinate ranges in their reverse tree
before inserting fresh tuples. Vector fragments bind a stable private-key digest
separately from the changing input-version digest. The native dense/sparse
mutation path retains untouched key chunks and vectors and gathers changed rows
only. Additions, removals and radix splits still invalidate affected fragments;
reusing their shifted row offsets would be incorrect. Each retained reader pins
its own coordinate space, navigation, native files and cloud certificate. Reader
protocol 76 covers the optional coordinate-key digest; legacy native generations
upgrade by rebuilding the affected fragments. Protocol 77 adds stable private
text keys, bounded posting-level merges and per-row aggregate contributions.

Sparse text changes analyze the selected rows and merge their postings with
retained native postings after masking old coordinates. The final bounded native
fragment remains immutable, with fresh physical maps and exact statistics; the
source rows outside the delta are not reread. Workspace denial before publication
selects the complete fragment encoder. Changes to membership still rebuild the
affected fragment because they can shift native coordinates.

Keyed algebraic cohorts persist typed row transitions plus reverse group routing
under their existing shared aggregate descriptor. COW edits update selected rows;
COUNT/integer SUM retract directly and other exact reducers recompute affected
groups from their current transitions. All-NULL groups remain groups, and an empty
group disappears. These roots are included in ordinary and resumable GC and cloud
retention closure. Dense/sparse key-state directories now use the logical census
without claiming physical object immutability, retaining untouched pages during
sparse refreshes. Legacy and unsupported representations keep complete rebuilding.


### Collision-resistant versions and bounded native publication

Reader protocol 78 adds an optional `input_commitments` page-tree root to native
coordinate spaces. Its immutable records hold 256 fixed radix bucket digests and
counts per fragment/input cohort. Each bucket hashes ordered, fixed-width logical
IDs and captured input fingerprints. Eight domain-separated binary Merkle levels
and a root binding namespace, cohort, fragment prefix/depth and row count produce
the file version. The root is independent of physical locations, page packing and
publication attempts. A sparse refresh rehashes reached buckets and path-copies
their bounded commitment records; unchanged fragments require no evidence replay.
The ordinary identity dependency iterator retains these pages for readers, cloud
certificates and both GC modes. Retired cohorts and split fragment records are
removed when the corresponding census changes.
The current native space owns the shared commitment root once. Both new and
reused per-index query views omit it, preserving their exact coordinate metadata
across unrelated cohort and schema changes without retaining obsolete censuses.

Legacy `iv1`/`iv2`/`iv3` versions are readable but cannot seed incremental input
commitments. New non-coordinate versions use `iv4`; stable text/vector coordinates
use `iv5`. A legacy fragment rebuilds its version before incremental reuse. This
migration matters because XOR of row hashes is linear: among 257 candidate row
updates, a nonempty subset necessarily cancels a 256-bit XOR accumulator. The
regression constructs that subset, verifies that the historical digest stays
unchanged, and checks that the real native planner nevertheless rebuilds the
changed fragment under its Merkle version. A separate migration regression
removes the old commitment root and restores legacy version prefixes, then
checks that every fragment rebuilds without seeding row deltas.

Optional encoded navigation waves reclaim their private allocation owner before
falling back to required page reads after a known quota decline. A completed wave
charge, including coalesced gaps, is not charged again. Failure before that charge
still pays each required logical read. Cancellation, provider/integrity failures
and genuine backing OOM remain errors. The navigation owner is explicitly
registered in the lake integration test root.

Keyed aggregate fragment jobs have private source views, allocator arenas and
CPU/I/O execution capabilities. Bounded retained-state reservations precede child
execution, and the parent parks CPU admission while joining. Workers publish
immutable row-state roots; only the coordinator adopts references and updates
contribution ownership after all jobs drain. Rejected concurrency keeps serial
execution. Tests exercise overlapping jobs with one shared I/O lane, failure and
deadline cleanup, and persistence of forward/reverse row contributions.

Partial aggregate merges gather a bounded source tile before pinning destination
pages. This permits source and destination to share a pager with one resident
frame. Each reached source/destination page is pinned once per tile, and lanes
for each group preserve their original reduction order. Primitive states remain
typed; AGS merges likewise update one destination page at a time. The shuffled,
duplicate-lane regression uses four pins for 128 partials across two source and
two destination pages, and two pins for the equivalent fixed-state merge.

Native posting publication uses content-defined SHA256 blocks between 8 and
64 KiB, clipped at physical pack and metadata boundaries. A small insertion can
therefore resynchronize with unchanged content despite moving native byte
offsets. Canonical row updates and cross-file serving merges load a bounded
operation-owned block directory, retain matching old pack/range references and
upload only unmatched payloads. Fresh directories preserve independently
verified ranges, compressed opening metadata and exact collection statistics;
queries need no historical delta chain or new visibility semantics. The ordinary
retention closure follows reused physical packs, including older attempts.
Unknown/denied optional reuse admission keeps complete publication.

The 4 MiB deterministic insertion fixture requires more than 75% of payload bytes
to be reused and less than 25% uploaded, verifies exact readback after releasing
the local reuse owner and reopening the artifact store without verification
caches, and verifies zero payload uploads on an identical repeat.
These are local structural bounds, not archive throughput measurements. Bounded
native merges still read retained postings and compute the new native output;
this refinement reduces provider write amplification without claiming to remove
that merge cost. The bounded posting-delta contract below adds that optimization
for eligible sparse updates without an unbounded history of query overlays.

Sparse maintenance after serving compaction resolves a complete fragment cover
through the paged source-to-pack index. It opens only those serving descriptors,
masks unrelated fragments and changed logical rows, and merges retained postings
with freshly analyzed rows. Canonical payloads remain collectible: maintenance
needs their compact physical maps and statistics, rather than a duplicate copy of
historical postings. Checkpoint-local deltas still read the canonical payloads
they own until those deltas enter the serving publication. Optional merge
admission bounds navigation to 8 MiB of memory and 16 MiB of metadata reads,
4096 routes, 64 packs and 16 MiB of native input; larger work uses full fragment
encoding. Authentication failures and cancellation still propagate.

A deterministic publication regression rewrites three real Parquet files six
times, changes index cohorts independently, and collects unreachable artifacts
between publications through the object-store transport. It requires a payload
change to preserve the untouched text navigation after a preceding sparse text
update. The posting-merge regression separately exercises mixed-fragment packs
and proves that unrelated rows and obsolete terms cannot enter the result.

### Bounded posting deltas and ordered extrema (reader protocol 79)

A sparse text update can now retain one immutable native base and publish only
its changed-row segment. This path admits at most 16 changed rows, 8192 base
documents and one delta segment. The base must cover one complete logical
fragment. Mixed-fragment packs and a second affected update use the ordinary
bounded merge, so readers never follow a recursive delta chain. This is a
deliberate refresh/query tradeoff: a small update avoids rewriting retained
postings while the number of query inputs and dead ordinals stays bounded.

Canonical maintenance manifests retain per-segment source spans, the current
physical row map and exact live collection totals. Serving descriptors carry
authenticated live ordinal ranges. A private query generation installs their
complement as native tombstones before publication; sharing a mutable deletion
cell between generations is forbidden. Masked generations cannot seed physical
reader sharing into an unmasked generation, including when restored content
reuses a historical artifact identity.

Exact term frequencies are built by seeking the retired ordinals in each term,
without decoding their positions or serializing the retained posting lists.
Removing those frequencies also removes their exact field lengths. Conservative
block bounds may still include dead rows, but collection counts, field totals,
DF and term-directory membership describe only live rows. The physical ordinal
population is recorded separately from the live collection count: ranked windows
and pending-ingestion offsets use physical ordinals; BM25 uses live statistics.
Both cuts and their physical maps remain in ordinary GC and retention closure.

Optional delta planning cannot mutate shared DF/directory reducers. A known
workspace decline therefore selects the existing merge before adoption.
Cancellation, integrity failures and backing allocation failures remain errors.
The zero-input row-patch case is an explicit successful empty result rather than
an invalid corpus or a reason to rescan the fragment.

Keyed aggregate maintenance adds an ordered multiset under its existing row
state. Keys bind group, reducer slot, native typed value and stable row ordinal;
values identify the owning row contribution. MIN/MAX seek their current winner
and count the non-null range after a retraction. Equal values retain the first
row's representation, including signed zero and NUMERIC display scale. COUNT,
integer SUM and extrema cohorts no longer refold every row of an affected group.
Other exact reducer cohorts keep their validated reduction path. Oversized keys,
legacy state without the multiset and denied optional workspace select complete
construction. The multiset is authenticated and marked with both row-routing
trees; protocol 79 prevents an older consumer from overlooking that GC edge.

Rewrite proof construction splits a borrowed typed batch into at most four CPU
lanes on the shared scheduler. Each lane owns key-expression and fingerprint
scratch; dictionaries and source vectors stay immutable until all lanes drain.
Only the coordinator captures payloads and mutates the global uniqueness census.
Opaque decoders and denied concurrency retain inline execution. CPU admission is
parked before joining children, including failure and cancellation.

Reuse-aware native publication now shares fresh publication's bounded read-ahead
and transport batches. Changed packs upload concurrently while retained blocks
keep their prior locators. Wave width accounts for both source capture and
changed-byte staging; an opaque or constrained allocator retains width one.

Contracts exercise exact score/row-map parity for replacement and text removal,
reopening without query caches, closure retention of the historical base, and
compaction on a subsequent affected update. Typed proof lanes must reproduce
serial bytes and return admission after null-key errors. Archive-scale speedups
and live-provider qualification remain unmeasured.


### Shared spill locality and bounded reducer maintenance (reader protocol 80)

The follow-up to #1234 separates logical merge runs from physical descriptors.
Shared-run sorts keep a dynamically allocated, quota-accounted directory of
balanced levels. A caller's four-descriptor reservation no longer forces four
logical runs into unequal-level merges. Merge heads remain bounded by decoded
workspace; final consolidation consumes smaller physical runs first. Nonshared
legacy sorts retain their descriptor cap. Native file-hash and tie directories
use the shared run owner. The maximum directory follows the machine ordinal
width and maximum fan-in, independently of archive row count.

Spilled joins use an adaptive radix directory over full 64-bit hash slots when
the paged path is admitted. A pressured leaf splits locally by the next hash bit;
fixed typed slots and directory nodes use the statement execution pager. Probe
tiles group directory and leaf reads by page, and only a complete hash match
opens the variable-width key/payload record. Full hashes do not replace typed
equality: exact comparisons still reject collisions, duplicate chains retain
build order, and matched flags continue to address the original spill records
for outer joins. Small workspaces retain the existing bounded flat fallback.
This keeps streaming probe order without materializing the entire probe input.

Native text writers optionally emit exact document/term/frequency evidence while
encoding ordinary and merged postings. A bounded shared-run sort publishes an
authenticated forward tree per canonical segment. The explicit version also
represents complete, empty evidence. Accepted terms too wide for the forward
key or known optional workspace declines keep the legacy exact statistics path.
Eligible one-base posting updates seek only the retired documents' forward
ranges, adjust reached DF keys in COW summary pages and subtract exact field
lengths. Global DF updates visit only summary differences. The base term
directory replacement is adopted during finalization, after the last durable
file checkpoint; its marker survives restart so recovered work cannot apply a
second retirement. Changed-row segments keep their ordinary exact contributions.
Mixed serving packs, legacy forward metadata and other ineligible bases retain
complete construction or the existing exact merge. Both canonical forward roots
and surviving summary roots belong to publication, certificate, reader-lease and
GC closure; protocol 80 fences readers that do not know these dependencies.

Mixed exact aggregate cohorts keep independent maintenance capabilities. COUNT,
integer SUM and indexed extrema update directly. Other accepted exact reducers,
including NUMERIC SUM/AVG and boolean reducers, read a COW reduction tree instead
of refolding every contribution in an affected group. A leaf covers 64 stable row
ordinals; each ancestor folds at most sixteen ordered child summaries. The
ordered fold preserves exact decimal scales, NULL state and first-row
presentation. Changed groups repair only their reached leaves and ancestors.
Legacy row state without summaries rebuilds once. The reduction root is
versioned, authenticated and retained alongside the contribution, reverse and
extrema roots. Query behavior and public source identity remain unchanged.

Structural regressions bound merge input work independently of descriptor caps,
exercise radix splits with one resident frame and exact duplicate/outer-join
behavior, compare changed-term retractions with a complete native statistics
oracle, and require a 257-row exact group update to inspect 64 contributions and
five child summaries while the previous immutable root remains readable. These
are local work bounds, not archive-scale latency or throughput measurements.

### Maintenance locality, allocation ownership and evidence reuse

Forward evidence has an explicitly scoped spill owner. Its allocator is backed
by the 4 MiB forward budget, while disk bytes, physical file admission and I/O
counters also charge the enclosing statement owner. Known child or parent quota
declines disable optional forward evidence; genuine allocator failures propagate.
The scope closes before its parent and does not create an independent disk quota.

Fresh exact reductions stream the ordered contribution tree once. A bounded
carry stack folds 64-row blocks and sixteen ordered children per ancestor, then
externally sorts compact summaries for bulk publication. It does not build a
large dirty-key census or seek once per group. Incremental repairs use operation
local authenticated decoded-page leases, shared with contribution, reverse and
extrema maintenance. The cache accounts encoded bytes, decoded entries and
expanded keys, never evicts pinned pages, checks authority/cancellation on hits,
and falls back to ordinary owned pages when a working set cannot be retained.
Forward retractions sort their reached terms and use bounded multi-seek waves,
sharing the same decoded page capability for summary reads and COW mutation.

A structural high-cardinality fixture with 257 groups now reads its six
contribution pages once during fresh construction and uses seven backing page
reads for repair, replacing the previous 772-read construction. These counts are
local work contracts, not measured cloud latency or archive-scale throughput.

Ordinary large SQL joins already use Grace partitioning: both relations are
partitioned, partitions build and probe locally, and skew retains the existing
bounded fallback. Reusable/recursive disk hash owners need their persistent
lookup representation instead. Their directory lookups now route larger bounded
waves in page order; native candidate records are primed in file-offset order.
Probes retain immutable record coordinates instead of borrowed decode buffers,
leave duplicate chains lazy, preserve caller lanes and defer payload hydration.
This complements Grace execution without materializing another complete probe
relation for a reused selective lookup.

Append compaction carries complete forward evidence from its staged producers.
Sorted/deleting canonical compaction can stream published forward trees and
staged evidence through the native merge's source map. Tombstones are omitted and
all surviving document ordinals are rewritten from that verified mapping. No
file-name or positional guess establishes row identity. Input sorts are rewound
before their owners can be reused; optional quota declines disable incomplete
evidence, while legacy inputs retain encoder capture. Serving-only packs request
summary capture without forward capture because they do not publish canonical
forward trees. This removes the otherwise unnecessary posting decode from copied
sections. Existing protocol 80 representations, closure edges and public UX apply
unchanged; no new wire format is needed.


### Publication read waves and projected reductions (reader protocol 81)

Publication maintenance uses the existing execution I/O phase to admit bounded
read waves. The coordinator validates references, reserves read bytes, and groups
adjacent or overlapping ranges from the same immutable pack, attempt and size.
A combined range never exceeds one maximum page; at most eight page references
enter a batch. There is no speculative gap overfetch. The phase bounds concurrent
range requests; workers borrow immutable jobs and a synchronized allocator,
without sharing counters, cache slots or the mutable phase. Every worker joins
before CPU admission resumes, errors escape or storage capabilities retire.
Authentication and decoding run after resumption, independently for every page.
Query read budgets retain their cumulative semantics; streaming construction
admits each bounded batch. Stores with a separate read-cache capability retain
its established serial admission path.

The decoded operation cache separates pinned hits from misses and forwards only
misses to the backing batch capability. Adoption and eviction remain serial;
failed reads or allocations release all partial pages and hit leases. A mixed
six-page fixture retains two warm pages, reads the remaining packed pairs in one
bounded wave, and requires three total range reads rather than six. Backend
failure and cancellation on resumption must leave no readers or phase active.

Append compaction streams remapped forward evidence into the existing presorted
spill representation. Source maps concatenate disjoint document ranges, and the
consumer verifies strict key order across source boundaries. This retains bounded
spilling while eliminating sort and merge rewrites for that path. Deleting or
arbitrarily reordered merges keep the general sorter. Regression coverage compares
both paths against the same authenticated forward tree and checks zero merge work
for the ordered stream.

Reduction state version 2 projects summary records to the reducers that require a
reduction tree. COUNT, integer SUM and indexed extrema remain in contribution and
result state, but no longer occupy or merge at every summary level. Slot order is
derived from the authenticated cohort recipe; original-to-summary lookups are
constant-time. Projection copies portable transition bytes, including NUMERIC
coefficients and display scale, without decoding them. All-reduction recipes reuse
their existing record bytes. Legacy reduction layouts rebuild once from the verified contribution tree,
without rereading unchanged Parquet rows; subsequent repairs reuse version 2
summaries. Protocol 81 prevents older readers from interpreting the new layout. Publication, certificate, lease and GC dependencies remain the same.

These are structural work and correctness contracts. Archive-scale throughput,
cold cloud latency and live-provider retention qualification remain unmeasured.


### Compact decoded navigation and shared probe candidates

Decoded page descriptors separate immutable read views from mutable serialization
entries. A leaf descriptor uses 56 bytes on the qualified 64-bit native target,
instead of reserving the 168-byte serialization entry's inline child reference
for every row. Branch references live in a page-owned array; their pointers are
borrowed for the page/lease lifetime. Mutation and serialization boundaries copy
references by value. Encoded page bytes, checksums, publication authority and
reader protocol 81 remain unchanged.

Decoded cache accounting includes descriptors, branch-reference arrays, wire
bytes and expanded prefix keys. The operation cache retains its 512 KiB limit.
When a decoded page cannot fit, authenticated encoded bytes can still be retained
within that same budget. A later read owns its decoded copy and checks authority,
cancellation and domain before using the retained bytes. Pinned decoded leases
remain protected during fallback and eviction; allocation failure releases every
partial descriptor and branch allocation.

Reduction repairs order the borrowed dirty keys at each level before seeking
contribution and summary ranges. The temporary order directory charges the
existing workspace and is freed at the end of the level. Together with compact
pages, this preserves locality beyond the previous small-fixture working set.
The 2,049-group no-op repair fixture with 34 contribution pages needs 45 backing
reads instead of the measured 3,847 reads with the earlier decoded representation
and hash-order repair. Updating every contribution in that fixture needs 47
backing reads, compares against complete rebuilding and retains the prior
immutable reduction root. Encoded fallback, prefix/branch
ownership and all allocation-failure exits have separate coverage.

Reusable native disk join probes group identical immutable candidate offsets
within each existing, bounded 512-lane wave. One metadata decode serves all lanes
that reach the record; each lane still evaluates exact typed key equality and
retains its own next duplicate and residual/matched state. Results retain stable
coordinates, and payload decoding remains lazy. A scalar probe oracle and a
1,025-lane repeated-key fixture require 1,025 versus three key-record decodes,
with no payload hydration during priming. NULL lanes and residual rejection of
one duplicate preserve their existing behavior. A second fixture uses ordinary
SMP allocation behind a tracked 32 MiB budget and verifies a zero live balance.
For 8,193 repeated lanes it needs 8,193 versus seventeen metadata decodes; one
local Debug run measured 16.29 ms versus 5.11 ms. These timings support the work
reduction and have no timing assertion. Archive-scale throughput and cloud
latency remain unmeasured.


### Reuse-admitted join metadata and direct decoded-page ownership

Native spilled joins retain immutable key/coordinate records only after a
bounded exact-offset probation filter observes reuse. A hash collision in that
filter declines admission; it never establishes key equality. One-pass distinct
candidate offsets do not allocate the retained metadata directory. Admitted records occupy
at most eight LRU slots and 64 KiB of arena capacity, further capped at one eighth
of the join budget and admitted only with 128 KiB of remaining scratch headroom.
Directory storage and arena bookkeeping also charge the existing request budget.
The cache moves completed decode arenas rather than copying typed keys. Local
and shared admission pressure can reclaim those arenas before allocating more
scratch. Small-budget joins retain the scalar path.

Every probe still checks cancellation and evaluates exact typed equality. Native
matches retain immutable record/payload coordinates, so later decodes, eviction,
and payload reads cannot invalidate saved matches. Payload hydration remains
lazy. Duplicate traversal uses the same metadata cache as initial probing.
Mutable matched flags never determine cached probe results; outer-join unmatched
scans bypass the cache and read fresh record headers. Allocation-failure, cancellation, residual,
NULL, eviction and zero-live-budget checks cover these ownership boundaries.

The 8,193 repeated-lane fixture needs two initial and two duplicate-tail metadata
decodes instead of 8,193 each. One local Debug run measured duplicate traversal at
14.3 ms versus 2.25 ms. A distinct 2,049-key fixture compares three runs per mode:
it retains the same 6,147 total decodes, with median times of 5.26 ms and 5.20 ms.
The probation policy avoids the measured roughly 13% distinct-key regression of
unconditional cache admission. Timing is diagnostic, never a test assertion.

Cold decoded-page misses now allocate directly through the operation cache's
owner and move independently owned pages into admitted slots. A borrowed backing
lease or a differently owned page still takes the independent-copy path. Only
the cache boundary records an alternate deallocation owner for uncached oversized
pages; ordinary decoding does not retain temporary worker allocator wrappers.
Batched misses apply the same ownership transfer after workers join. Existing
512 KiB admission, pinned leases, encoded fallback, authentication and cancellation
checks remain in force.

Across 128 cold admissions of a prefix-compressed 1,024-row page, this removes
one full decode and ownership copy per miss: 22,826,496 versus 11,413,248 allocated
bytes and 768 versus 384 allocations. One local Debug run measured 41.1 ms versus
21.0 ms. Separate caller/cache allocators, oversized fallback and every injected
allocation failure release their owners correctly. These are in-memory changes
under reader protocol 81; wire artifacts, publication authority and GC closure
are unchanged. Archive-scale throughput and cloud latency remain unmeasured.


### Archive qualification after the locality refinements

[Lake archive qualification](../../zig/bench/lake/README.md) provides a repeatable
real-Parquet gate for source scans, metadata lookup/filtering, skewed joins,
numeric text sorting and broad incremental publication. Deterministic generated
fixtures use independent answer oracles and atomic manifest commits; user-supplied
archives are copied into isolated fixtures without rewriting originals. Frozen
remote plans retain their declared source identity and credential references.

Reports distinguish process-restarted, warm and startup work. They do not treat
retained persistent/OS caches as empty, sampled RSS as exact request allocation,
serving-cache counters as complete transport traces, or newly retained artifacts
as total disk writes. SQL runtime profiles preserve their own coverage and spill
fields. Every sample and input/binary/collector fingerprint is retained; report
comparison requires equal answers and compatible inputs, cache conditions and
collector/environment before reporting a ratio.

This establishes the measurement path for further implementation decisions.
Native reader protocol 81, publication authority and GC closure are unchanged.
The initial Debug filesystem run exposes all-candidate numeric text sorting and
publication artifact volume as targets for deeper profiling, without making
archive-scale or live-cloud performance claims.

### Qualification integrity and native physical ordering

Archive qualification version 3 binds the executable, effective configuration
(including the generated default), collector, harness and runtime-environment
digest before startup. It rejects changes around restarts and measurements and
verifies content before completing a report. Comparison requires complete,
matching provenance, including the generated refresh recipe, rewritten-source hash and changed-row count. Version-2 reports remain historical observations and are not comparable through the current collector. Environment values and configuration contents are not
included in reports. Readiness includes embedded table indexes, additional index
declarations and schema relational indexes. A declared identity field controls
the exact ID oracle and answer normalization independently of physical source
coordinates. Python tooling tests run in the regular CI unittest suite.

For stable-key sources, logical fragments assign native coordinates in declared
`schema.index_sort` order. The existing publication-bound by-ID and by-location
maps authenticate both directions; the physical source space, logical identity and public IDs remain distinct. Sorting uses captured typed payloads and the shared
native key comparator, with at most 8,192 rows per fragment and the existing
scheduler, memory and spill admission. Only changed input fragments are sorted. Incremental verification can omit a
complete payload capture; those fragments gather only ordering columns at their
authenticated physical row coordinates instead of abandoning native layout.
Sort inputs live in one internal authenticated input cohort, separate from text,
vector, relational and aggregate consumer cohorts. Only membership and that
ordering cohort invalidate physical layout. A payload-only edit no longer sorts
unchanged keys, and a rank-only edit can retain a field-only text/vector generation
in its original coordinate space. Ordering configuration is bound by navigation,
not by consumer payload interpretation. Missing,
null or unsupported keys retain canonical coordinates and the exact
doc-values top-N fallback. Failed admission retains the physical-source fallback. Reversed or leading
`_id` sort components retain the exact fallback; private coordinate IDs can close
an otherwise ordered tuple ascending.

A normalized ordering recipe and ordered-member digest travel in authenticated
fragment navigation. The digest also binds native coordinate reuse: a changed
sort key that moves rows forces a full run rebuild, while unchanged layouts
retain the original coordinate pages and can retain row patches. The identity
builder admits a provisional file reuse and switches to a rescan only after its
ordering commitment changes; payload-only edits do not republish coordinate maps. Source recipes fence earlier producers that omitted ordering
or ignored disabled detailed text mappings. Native encoders attest the observed
physical order before writing ordinary sort metadata and endpoint bounds. They
never silently rearrange documents after source ordinals have been assigned.

Public-ID projection uses a bounded k-way merge of native primary sort tuples.
Cursor seeks retain the complete primary boundary tie, then the existing exact
collector applies public identities and final cursor comparisons. The stream
uses authenticated endpoint bounds as lazy heap entries, opening sort-value streams
only when their bound becomes competitive. Inclusive seeks retain complete primary
ties. Publication-wide winners pass a trusted primary boundary to later routing
windows; strictly worse runs need no sort-key probes, while exact matching bitmap
totals and equal-key public-ID candidates remain intact. It shares the candidate-probe budget; skewed
membership resumes the original compressed membership iterator after discarding
the partial window. Metadata coverage alone is insufficient qualification:
profiles must show positive ordered work and fewer decorated candidates.

Append compaction preserves ordering only when all input metadata and bounds
prove the concatenation monotone. Lake serving compaction also merges overlapping
ordered runs using the native merge's output-order provenance stream, composing
each emitted coordinate with original-file ranges. Adjacent coordinates coalesce
into authenticated ranges. Existing eight-way, byte, workspace and source-range
limits apply; highly fragmented mappings split into smaller packs instead of
publishing unbounded row metadata. A pack admits at most 65,536 ranges and 4 MiB
of authenticated source pages. A pack shares one source dictionary across its
pages, followed by fixed-width native range records. Pending inputs retain shared
source descriptors and 16-byte ranges instead of full Span structures. Pending-tier
and open-tail decisions charge retained native data rather than a repeated JSON
estimate; merge workspace admission additionally charges actual pending arena
capacity. Ownership publication emits one mutation per distinct source file/pack,
regardless of the number of interleaved ranges. Query admission
includes a decoded-range floor
independent of wire size. Inverse routes coalesce contiguous original-file ranks
even when their serving positions interleave. Physical-generation changes and
source-rank gaps retain separate routes; historical uncoalesced maps remain
removable during refresh.
Native text metadata v13 and reader protocol 83 fence the shared-dictionary writer;
retained v9-v12 roots, historical JSON and page-local dictionary pages remain
readable. Nested serving and
checkpoint roots resolve their own authenticated versions. Existing publication
certificates, reader leases and GC closure own every dictionary source reference;
there is no additional artifact family.

Detailed `x-antfly-field.index: false` suppresses that field's text postings.
Stored source, explicitly enabled named subfields and independently requested
typed doc values retain their own policies. This avoids generating dictionaries
and statistics for large payload fields that the schema excluded from indexing.
Named subfields are scoped to the selected document schema and honor its unindexed
paths. The deployed logical ASCH lowering remains unchanged. Physical projection
semantics have their own provenance revision: only an exact known prior witness
admits a leader-owned shadow generation rebuild through the durable repair queue.
Unknown provenance mismatches keep their existing manual policy. Active postings
are never relabeled under new semantics. Schema binding also recognizes the exact,
complete omitted-source encoding briefly emitted by this PR; unrelated changes,
partial omissions and lost enabled fields remain invalid. Stored epoch bytes are
preserved rather than rewritten under the same version, including owner descriptor
rehydration. Read-only repair observation exposes verified startup debt even before
an intent exists; the next leader-admitted turn discovers and advances that debt.
A named repair never discovers unrelated startup failures.

Qualification must compare projected answers on frozen Parquet bytes, including
numeric/timestamp directions, nulls, ties, filtering, cursor continuation,
deletions, rewrites and restarts. Reduced candidate work and repeated timing
improvements must be weighed against bounded build memory and publication cost.
The earlier configuration-only probe remains historical evidence, not evidence
for the implementation above. Filesystem observations do not qualify live-cloud
transport or retention behavior.

The [version-3 native compaction observation](../../zig/bench/lake/native-compaction-v3-observation.json)
retains equal-answer comparisons on frozen Parquet inputs, including the
intermediate JSON metadata regression and the compact-page result. It records
all sort-work/timing samples and refresh provenance, distinguishing the explicit
native ordering plan from the generated fallback control. These local Debug
observations establish reduced work and metadata growth; timing attribution and
live-cloud qualification remain separate.


### Shared pack navigation and admission before publication (reader protocol 83)

The source dictionary is authenticated by the ordered page references in the pack
manifest. Dictionary bytes can span 64 KiB pages, so one large valid file descriptor
does not become an artificial page-size failure. Range pages carry four little-endian
32-bit fields: source index, original rank, serving position and count. Readers
verify dictionary and range offsets, reference domains, complete coverage and all
coordinate extents. The 4 MiB total admission limit includes page headers and the
dictionary. Collectors traverse every unique physical source reference; publication
certificates and reader leases retain their existing authority.

Encoding and full pack-manifest admission precede posting publication, statistics
and ownership mutations. An admission decline releases the candidate's readers,
workspace and staged segment, then retries smaller input groups. Provider failures
abort the publication; they never replay partially applied reducer contributions.
The regression forces eight-way splitting and checks exact term frequency and
retained pack ownership. It also covers the formerly failing 65,536-range case:
eight long source names need 1,070,237 bytes with the shared dictionary, versus
6,523,136 bytes with page-local dictionaries.

Query readers keep the dictionary and 16-byte range views rather than expanding
one complete file descriptor per range. Public identity mappings own source paths
once per file, with compact forward ranges and inverse intervals. A real ordered
merge of 16,384 interleaved coordinates initializes identities within a 2 MiB
allocation budget (observed peak below 1.2 MiB). Maintenance paths can expand the
same view when they need ordinary spans. Streaming merge provenance removes the
post-merge inverse lookup, row permutation allocation and permutation sort.

Ordered top-K membership is deferred where an unmasked segment provides an exact
header-only count: match-all, match-none and a single term without synonyms,
including identity Boolean wrappers. Skipped runs contribute their exact totals
without decoding their membership bitmaps. Proven match-all and match-none runs
use implicit ranges for membership and enumeration. Partial single-term probes
retain one leased segment, its scoped dictionary and one decoded postings chunk;
arbitrary forward and backward probes seek through authenticated chunk metadata
and never decode positions. Switching segments releases the previous lease before
opening the next, bounding reader residency independently of segment count.
Deletions, synonyms and compound predicates retain ordinary execution; fallback
enumeration materializes partial runs when a complete bitmap is actually needed.
Empty Boolean filters retain their zero-match semantics. Attachment and query
options do not change.

The [shared-navigation comparison](../../zig/bench/lake/native-shared-navigation-v3-observation.json)
retains the final binary identity, all samples and the exact-answer comparison.
On the frozen 16,384-row source, process-restarted median latency is 215 ms versus
922 ms for the prior compact-page implementation; warm medians remain about
32 ms, with the same 21 decorated candidates and 23 ordered probes. These local
Debug observations supplement the deterministic metadata-size and allocation
checks; they do not qualify cloud transport or controlled timing attribution.


### Reusing physical leading prefixes

The native primary candidate producer accepts a leading prefix of the declared
physical order, with matching field directions. For physical `(id, revision, _id)`,
a public query ordered by `(id, _id)` can stream the `id` prefix. The complete
physical recipe and endpoint bounds must still authenticate against the schema.
This proof is separate from full-order segment-seek eligibility: an unused
physical suffix can permute public-ID ties, so it cannot authorize an executor
that assumes the complete requested order is monotone.

The primary producer removes only the final public `_id` key for traversal and
inclusive cursor seeks. The collector resolves the whole primary boundary tie,
projects public identities, and applies exact cursors and final tie directions.
Competitive boundaries prune only strictly worse primary tuples. Allocation and
candidate budgets keep the existing exhaustive fallback. Regressions compare
both complete and prefix physical recipes through forward/backward cursors,
deletions and public IDs that reverse private identities; real Parquet tests
exercise both declared physical recipes after import and process restart.

### Reverse traversal, public tie navigation and sparse tombstones

Native primary prefixes can traverse in either physical direction when every
primary field agrees on the same reversal. Mixed reversals retain exact fallback.
Query direction and `search_before` combine to choose physical traversal; heap
comparison still uses the requested order. The public `_id` tie-breaker retains
its ascending API contract. Binary seeks and endpoint proofs compare in the
physical domain, so reversing a query never reverses only part of its tuple.

Candidate admission charges new native probes. Buffered heads and EOF can drain
with no additional allowance. A bounded fallback retains its current top-K window
and a compressed set of examined matching ordinals; membership enumeration skips
those ordinals rather than decorating the same rows again. This also makes a
poorly selective optional ordered producer safe to abandon without replaying its
completed sort work.

Large primary ties can reuse the publication's existing public-file navigation,
current `by_order` row tree and the bound index space's `by_id` map. A query-owned
cursor visits public file digests and physical row coordinates in public-ID order,
then translates stable IDs into the pinned index's coordinates. This works after
external rewrites and retained-generation reuse; private index coordinates never
stand in for public tie order. Native binary seeks identify each primary group's
intervals before opening the cursor. Small/sparse groups decline this path using
a cardinality cost estimate. The ordinary primary stream remains authoritative
when this capability is unavailable.

The optional cursor has an independent decoder lane, bounded tiles, a 4,096-row
allowance and 16 MiB read allowance while preserving 32 MiB for required reads.
Skew, optional memory pressure or exhausted optional reads falls back to native
intervals. Complete-order evidence belongs to each emitted group and is withdrawn
when that group falls back; exact membership, tombstone and cursor checks still
run in the collector. Scratch/cache storage is reclaimed independently of a result
arena. The cursor borrows its query's publication/reader authority and closes before
that owner. No additional persisted index, artifact family or metadata protocol is
needed: all traversed trees already belong to the publication's retention/GC graph.

Unmasked term headers retain raw document frequencies. Match-all and match-none
can also derive exact live counts with any validated tombstone mask. For a partial
term, at most 4,096 deleted ordinals, covering no more than one sixteenth of the
segment, are tested with chunk-local point probes; subtracting their matches gives
an exact live count without expanding the postings. Dense masks and unsupported
filters keep ordinary bitmap execution. Point membership rejects tombstones before
using implicit-all or term probes, and exhaustive enumeration remains complete.

### Independent public-ID ordering and complete competitive bounds

A query ordered only by `_id` can use the same publication-bound public-row
navigation as a large primary tie. The exact membership collector streams public
IDs, applies filters and tombstones, and decorates only the retained page plus its
lookahead. Forward and backward cursors share the public identity ordering proof;
private logical hashes never supply this order. Optional budget exhaustion
withdraws the complete-order proof and resumes exact enumeration without replay.

The coordinator's complete winning tuple also bounds public navigation. Primary
groups strictly better than that tuple retain all IDs; only an equal primary group
receives the inclusive public-ID endpoint. Independent ID ordering has no primary
prefix and can apply that endpoint directly. Invalid, foreign-snapshot or
unsupported cursor domains decline the optimization. The collector still checks
the exact public cursor and complete comparison tuple.

Each navigation tile translates at most 256 stable IDs with authenticated tree
multi-get. Shared routing and leaf reads replace per-row tree seeks; filename
resolution is batched too. Coalesced and parallel read lanes charge the same
optional allowance under the owner's lock and preserve the required-read reserve.
The cursor retains its existing row, byte, cache and scheduler bounds.

### Repairing declared text projection exclusions

Dynamic templates select undeclared runtime values using both path and value
type. A declared text source instead takes physical `index:false` policy from
its own exact source mapping. Applying the declaration-only resolver to a declared
string could incorrectly suppress it when a numeric-only dynamic template matched
the path. Logical schema epochs and their potential-source descriptions remain
stable; the correction belongs to physical projection identity.

Affected local generations receive a new projection witness. Only exact prior
witnesses authorize the existing leader-owned private replacement; unknown drift
continues to require manual repair. Unaffected witnesses remain stable. Native
publication definition revision 11 and reader protocol 84 force revalidation;
only affected whole-document text binding hashes change. Field-selected text and
vector bindings retain their existing identity and artifact reuse proofs. This
is a semantic migration, with no change to native text metadata encoding.


## Incremental ordering-key state (reader protocol 85)

Native fragment spaces optionally retain a copy-on-write page tree keyed by
logical row ID. Each value contains primitive typed ordering keys and the exact
ordering-input digest used to derive them. The root also binds the ordering
cohort, including its schema, input interpretation and emitted field positions.
The cache recipe is separate from the source-input cohort, which intentionally
sorts and deduplicates column names. Permuting fields or selecting another alias
cannot reinterpret a cached tuple; reversing directions can reuse the same keys.
Keys retain signed
nanosecond timestamps and integers beyond 2^53 without conversion to floating
point. They contain ordering inputs, not source bodies or an independent row
identity.

When an input edit requires sorting a fragment again, the builder joins its
current input-version cursor with the previous key cursor. Matching evidence
supplies unchanged keys; only missing or changed rows gather ordering columns
from the current Parquet/Iceberg locations. Deleted IDs remove their key records.
A new cohort starts a fresh tree. Historical publications without keys remain
readable and populate keys as fragments are processed. The bounded fragment sort,
spill manager, shared scheduler and publication admission remain authoritative.

The tree advances with the same catalog publication as the coordinate layout.
Its pages belong to the ordinary identity dependency closure, including reader
leases, durable cloud generation maps and GC traversal. Frozen per-index reuse
spaces omit this builder-only cache, so they do not independently retain obsolete
ordering state. A cache hit never substitutes for current input evidence or
source-version validation during result hydration.

## Early routing dominance and lineage read waves

Native text metadata v14 attaches portable typed first/last bounds to each
segment's authenticated routing record. A full field-order top-K can reject a
strictly dominated primary range before descriptor, segment or lineage admission.
The leading physical prefix must match with one uniform direction reversal;
unknown mappings, unsupported directions, missing bounds and invalid bounds keep
the pack eligible. Equal primary values remain eligible because private segment
IDs cannot bound public-ID ties. Forward and backward pages use their respective
competitive endpoint. Exact counts and full-match evaluation do not prune; a
pruned result reports a lower-bound total. This streams compact routing metadata;
it does not yet provide a best-first multi-field metadata tree. Prefetch owns a
copy of the cutoff so concurrent winner movement cannot invalidate its values.

Cold lineage loading admits all logical page bytes before issuing reads, then
uses waves of at most eight shared scheduler I/O tasks. Authentication stays per
artifact and decoding stays in publication order. Every submitted task is joined
before failure or cancellation returns. Saturation uses caller execution;
publication-owned execution stays serial to preserve its CPU/I/O phase ownership.
The extra encoded residency is bounded by eight 64-KiB pages and scheduler
workspace admission. This reduces serial provider round trips without widening
query limits or creating another worker pool.

## Ordered serving admission and demand-paged lineage (reader protocol 86)

Field-sort admission uses the same competitive primary-tuple predicate for both
full routing walks and term/predicate-selected segments. Equal primary keys stay
eligible for the public-ID tie. Pruned searches report a lower-bound total;
count-only and complete-match evaluation retain exhaustive admission.

Native text metadata v15 adds an optional `directories.ordered` page-tree root.
Each segment contributes its first and last endpoint, with a memcomparable typed
key in physical sort order and its authenticated segment identity. The tree's
reduction binds a common field/direction/value-domain layout. Queries require
that proof and exactly two records per live segment before using the directory.
Absent, partial, mixed-domain or unknown-order covers retain ordinary routing.
The query walks first endpoints forward or last endpoints backward, accounting
for request direction and backward pagination. Strict primary dominance proves
that the rest of the ordered range cannot improve the winners, so the query can
stop before reading subsequent routing records, descriptors or lineage. A broad
selected segment set may intersect this stream; sparse sets retain direct seeks
with the same cutoff predicate. Admission begins with a small window and grows
when winners stop changing, preserving batching for wide equal-key groups. The
endpoint record carries the same conservative byte estimate as ordinary routing;
segment-count and byte limits still bound each window. The directory is publication-owned and belongs
to existing reader leases, cloud closure certificates, navigation warming and GC.
It does not introduce a separately mutable index or a new publication boundary.

Unfiltered field-sort windows with a proven ordered endpoint cover may admit
shared-dictionary lineage as a dictionary
and authenticated range-page references, without decoding every range. Native
ordinal lookup binary-searches page bounds and then fixed-width records inside
one page. Current v3 lineage pages already carry the required positions; no
second row-index format is needed. Legacy encodings and masked packs use complete
views. Score-order queries, overlays, predicates, count-only queries and complete
match evaluation retain their existing full mapping semantics.

A public-ID tie traversal can require the inverse map. That operation explicitly
materializes it through the existing bounded shared decoded cache. An operation
holds a lease across its tiles; later queries can reuse the immutable inverse
map without decoding it again. The cache key binds the full publication/layout,
and each lease checks the current request authority. Query resolvers are attached
after acquisition and are never retained in the shared value.
Failure of this optional admission withdraws the complete tie-order proof and
returns to native ranges. Cancellation, authority checks and read accounting use
the current query capability, never a capability retained from cache admission.
The shared corpus cache distinguishes complete and paged admission modes.
Immutable descriptors, page references and complete inverse maps may be shared;
mutable traversal state remains query-owned. Point reads retain
no archive-sized permutation; sustained point work switches to the complete map
before consuming the read allowance reserved for that fallback. Every range page remains an ordinary flat
`source_pages` dependency, so reclamation and retention use the existing closure.

Complete payload captures now use the same input-version-checked ordering-key
cache as projected gathers. Missing keys are grouped by captured payload offset
before extraction. Thus one changed key does not reread every payload in its
fragment, and a fresh capture reads each missing block once per fragment rather
than depending on random hash order fitting a small decode cache. Consumer
coordinate generations, tuple recipes, schema epochs and public cursors retain
their existing contracts.

Admission storage can be released immediately after a native corpus is built.
Deferred lineage therefore deeply owns its compact source descriptors, page
references and segment counts. It retains neither expanded admission-root slices
nor an acquired snapshot pointer. Complete inverse maps derive counts from these
owned offsets and reattach the caller's partial-inventory semantics on every
lease, just as they reattach the current row resolver. The interleaved-compaction
regression explicitly frees admission storage before both point and inverse
reads, including allocation-failure coverage of the ownership transfer.


## Bounded lineage leases, term probes and binary refresh keys (reader protocol 87)

Range pages use the existing verified immutable block-lease API. A synchronous
query operation pins at most eight 64-KiB pages under scope, domain and exact
content identity. Repeated probes reuse those bytes instead of allocating and
copying each page. Every hit checks the current deadline, cancellation and lease
hooks; the cache retains no request capability. Newly admitted pages consume the
operation's read allowance, including re-admission after eviction. Result tiles
visit native ordinals in order and restore caller order, preserving duplicates
before batched current-coordinate translation. A separate 4,096-point CPU limit
and the existing required-read reserve switch sustained work to the shared
complete inverse map. Cached pages release before complete-map admission.

Sorted text membership retains up to eight exact term probes under a 2-MiB
query-owned budget. Interleaved sorted runs reuse each probe's dictionary and
position-free posting chunk. Least-recently-used probes release segment leases
and decoded buffers before replacement. A probe that cannot fit declines to the
existing exact bitmap path; unsupported predicates retain their existing plans.
Mutable probes remain local to the synchronous collector. No archive-wide probe
array or process-global mutable cursor is introduced.

Ordering-cache rows use the self-describing `AFOK001` binary codec: magic,
32-byte input version, bounded tuple count, explicit scalar tags and exact
little-endian values. Signed 128-bit timestamps, signed/unsigned integers,
finite floating values, booleans and owned variable-width bytes preserve their
native domains. Field positions remain bound by the existing tuple recipe.
Malformed lengths/tags, nonfinite floats and trailing bytes fail closed. Empty
or oversized tuples do not create optional per-row state. Readers also accept
historical JSON rows, and old/new entries may share one copy-on-write tree;
only newly remembered values are rewritten. Decode results own byte strings
independently of cursor/input storage. Reader protocol 87 fences these writers;
native text metadata remains v15 because serving artifacts are unchanged.
Publication leases, cloud certificates and GC follow the same ordering-key root
and flat range-page closure. No new public configuration or query semantics are
required. Performance qualification records page acquisitions, probe openings,
cache pressure and encoded tuple bytes separately from end-to-end latency.

## Compound probes, nullable ordering and layout cohorts (reader protocol 88)

Eligible native top-K searches compile term/all/none and nested Boolean
membership into an immutable program of at most 128 nodes and 32 distinct terms.
Each of the eight query-local probe slots owns its segment leases and position-free
posting chunks under the same 2-MiB budget. AND, OR, exclusions, optional clauses
and minimum-should-match retain the ordinary Boolean semantics. Unsupported
leaves, synonyms, program overflow and local cursor/chunk pressure retain exact
bitmap execution. No mutable cursor or request capability enters a shared cache.

Unknown compound cardinality is an admission estimate, never a reported exact
count or lower bound. Ordered early termination reports only observed matches
with `gte`; count-only, zero-limit and full-match evaluation retain exact counting.
If the ordered walk loses, at most 1,024–65,536 probes (scaled to requested top-K)
precede complete bitmap fallback. Existing seen ordinals and winners survive the
switch. Cursor filtering and public-ID ties continue to determine the exact page.

The field mapping opts in with `missing_null_policy: nulls_ordered`; the
existing `missing_rejected` default continues to reject live missing/null values
and null cursors. Native ordering has an explicit null tag matching the existing comparator:
ascending nulls first, with descending order reversing that placement. Tagged
refresh tuples retain nulls without creating a fake typed scalar value. Sort
metadata v2 carries a per-field completeness witness from a full producer walk.
Only that authenticated witness permits missing typed values or an entirely
missing typed section to represent null. Older metadata and sparse coverage
without a witness retain the existing rejection/fallback. Sort-only admission uses
the witness and typed header/domain without a whole-column coverage scan; general
column-density summaries keep their existing validation. Malformed typed sections
still fail closed. Streaming compaction, spill keys, bounds and binary refresh
keys preserve the same null domain. A new layout recipe upgrades previously
unordered nullable fragments while allowing unchanged consumer payloads and
historical ordering-cache rows to remain reusable.

Endpoint records are grouped by a physical recipe/domain prefix in one existing
page tree. Its aggregate counts at most 32 cohorts; complete paired record counts
bind the cover to the publication's segment count. Query-owned cursors process
incompatible/unknown groups exactly and merge the best endpoints from compatible
groups. Each compatible group can stop independently at strict primary dominance;
equal primary groups retain every public-ID tie. A separate 8-MiB owner bounds
retained cursor pages and group heads. Excess cohorts withdraw this optional plan
and retain ordinary routing. No separate shadow index or new public setting is
introduced.

A first publication refresh migrates an old flat endpoint directory from final
segment metadata, one descriptor at a time. This migration does not reopen source
Parquet files or rebuild native payload generations. The existing directory root
keeps the same publication lease, cloud-certificate and GC closure. Historical
flat directories remain readable. Native text metadata v16 and reader protocol 88
fence the new writer; readers retain v9–v15 publications and historical JSON/binary
refresh entries.
