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
proof keeps the existing whole-fragment path. Input fragment versions compose
independent domain-separated row hashes, so a payload change applies the old
and new fingerprint without scanning the other input fingerprints.

Ordered/covering indexes retire exact coordinate ranges in their reverse tree
before inserting fresh tuples. Vector fragments bind a stable private-key digest
separately from the changing input-version digest. The native dense/sparse
mutation path retains untouched key chunks and vectors and gathers changed rows
only. Additions, removals and radix splits still invalidate affected fragments;
reusing their shifted row offsets would be incorrect. Each retained reader pins
its own coordinate space, navigation, native files and cloud certificate. Reader
protocol 76 covers the optional coordinate-key digest; legacy native generations
upgrade by rebuilding the affected fragments. Text segments and aggregate
contributions retain their bounded fragment-level replacement path.
