# Native lake performance follow-up

Status: implemented; representative archive benchmarks remain pending. This follow-up builds on
PR #1006 and keeps native execution, public snapshot tokens, exact filters, and
reader leases. The contracts below describe the implementations and their
validation requirements.

## Native sparse predicate intersection

Implemented through query-owned compressed native ordinal selections. Native
sparse recipe v6 persists authenticated 1024-row physical-to-native ordinal
blocks inside the checkpoint. Each entry stores a two-byte physical offset and
four-byte native ordinal; ingestion coalesces writes with at most 64 resident
blocks. Inserts, replacements, deletes and compaction update these maps in the
same transaction. A completeness marker distinguishes missing vectors from
legacy checkpoints, which retain point lookup fallback until rebuilt. Queries
translate a physical selection with one lookup per occupied block and check
cancellation between blocks, without formatting a document key per selected row.

All positive selections use the canonical quantized posting scorer. Point filters
seek directly to the posting block covering the next selected native ordinal;
broad filters intersect the same ordinal bitmaps with posting ranges. This keeps
scores and ties identical to unfiltered scoring, including zero and negative
scores for overlapping terms. Forward locators remain identity/update metadata.

New immutable segments publish an `ASPSPG01` term-directory root and independent
posting-block KV values keyed by segment, term, and final ordinal. Chunk payloads
remain V1. Query cursors retain one encoded block per active stream, with a shared
64 MiB resident-block admission budget; navigation has a separate byte budget
instead of a fixed 4096-stream limit. Selective seeks avoid loading earlier blocks.
Legacy `ASPSSEG1` roots remain readable with their original encoded-byte budget.
Maintenance merges posting streams from a pinned backend snapshot, retaining one
encoded block per active source and one output chunk. Native owners spool output
blocks into a private capacity-accounted run; publication reads one bounded block
at a time. Complete input segments and corpus-wide decoded posting/sort arrays are
no longer required for paged compaction. Legacy roots remain readable. Term
directories, captured incarnation proofs and docmap maintenance still use explicit
memory admission; this is a bound on posting working state, not constant total
maintenance memory. Pages, roots, incarnations and physical maps publish atomically. Replaced block keys are
deleted in the same transaction, preserving existing backend snapshot readers.
The v6 producer fence rebuilds older remote publications with term-to-segment
routes. Routes, posting pages and roots commit atomically; a coverage marker
allows older local checkpoints to retain segment discovery until their next
publication creates complete routes. Compaction removes old routes in the same
transaction. Queries seek relevant routes and reuse one posting-page cursor,
avoiding archive-wide root discovery and per-block cursor construction. Native
queries also warm the following posting block through at most eight speculative
jobs on the shared CPU/I/O scheduler. Each job owns an independent fork of the
same immutable snapshot; saturation yields to required work. Completion or
cancellation joins the worker before releasing its read lease. Speculation keeps
no result buffers and relies on the independently bounded native read caches.

Document-at-a-time scoring retains one document accumulator and k winners.
Conservative block bounds include zero and both signed weight endpoints; strict
score pruning preserves ties. Block-prefix pivots exclude terms whose next
posting lies beyond the lead range, with a fence at the earliest block end.
Nonfinite bounds disable pruning. Nonfinite contributions or f32 accumulation
overflow return `SparseScoreOverflow` before ranking in both streaming and spill
paths. The comparator also defines a total order for defensive NaN handling. Contributions retain source/term/chunk f32 addition
order. Prepared bitmap ranks reject disjoint blocks without decoded arrays.
Legacy checkpoints, unresolved key predicates and admission overflow retain the
bounded spill fallback: 65,536 in-memory partial scores and a 1 GiB spill-input
budget, with native capacity reservations and cancellation. Complete identities
exclude tombstones before winner admission.

Acceptance: point and broad filters, exclusions, mixed terms, changed files,
deletes, restart, and cancellation agree with an exact reference scorer. Measure
reverse metadata reads, posting blocks decoded, scored rows, peak memory, and
provider bytes. A point predicate must not walk a common term's entire posting
list merely to resolve membership. Preserve score and tie behavior.

## Residual evaluation over narrowed physical selections

Implemented for text and vector predicates by retaining an indexed conjunction superset and
a separate residual IR containing only unresolved children. One pinned Parquet
cursor borrows compressed physical selections for the entire scan; file/group/page
pruning and reusable reader plans avoid reopening every 1024 rows. Only residual
dependency columns are projected. Direct-column expressions execute shared
predicate leaves over page masks, preserving Boolean short circuiting and
projected document null semantics without per-row JSON objects. Dictionary columns
evaluate shared predicate leaves once per reached dictionary entry and cache null
evaluation separately. Canonical i64/f64 terms and supported numeric ranges use
eight-lane kernels over active selections. Boolean terms, scalar null terms and
scalar existence predicates avoid per-row document shaping. Standard integer
bounds retain exact integer comparison; numeric-range operators retain their
existing f64 domain. Wide mixed bounds, nonfinite values and composite columns
keep authoritative shared semantics, including null evaluation and errors. Nested paths and document-ID expressions retain the shared
document evaluator. The resulting exact physical set is shared by dense and sparse
membership. Text selections exceeding the late-visibility budget invert native
ordinals into physical selections through the pinned file/block directory.
Contiguous extents remain compressed, authenticated live-row bitmaps restore
deleted holes, and temporary artifact copies are released between blocks.
Only selected blocks and residual dependencies reach the pinned scan.

Keep the indexed superset for a partially resolved conjunction. Iterate it in
bounded file/group/row windows, project the authoritative expression dependencies,
and evaluate the shared compiled predicate on those rows. Produce an exact
physical selection before vector ranking. OR and exclusions require exact sets;
unsupported expressions retain the authoritative fallback. Admission and spilling
must bound scratch without imposing a matching-ID list limit.

Acceptance: indexed point plus unindexed prefix/range/Boolean residuals match full
scans for text, dense, sparse, and hybrid requests. Include nulls, deletes, and
more than 100,000 selected rows. Record decoded rows and bytes to prove a selective
conjunct narrows residual I/O. Test cancellation and allocation failures.

## Ordered lake index top-N

Implemented as an optional ordered-candidate provider in shared native text
sorting. A cardinality/row-goal cost check preserves bounded native sorting for
tiny memberships. A runtime probe budget restarts the bounded native collector
when skewed membership defeats that estimate, retaining truthful traversal counts
and the `ordered_lake_index_then_text_postings` source. The provider enforces the
remaining physical probe budget inside each pull, including rows absent from the
text corpus. Compatible direct relational keys stream pinned physical row
references without Parquet hydration. Safe required predicates provide tuple
bounds and equality prefixes; unsupported leaves remain native membership checks.
Forward and backward cursors seek inclusively on the first ordered field and keep
complete boundary ties for public-ID comparison. Backward traversal follows
preceding B-tree children from the upper-bound path; it does not reverse a full
forward scan. Signed datetime keys and native date-range query bounds use the same
i128 nanosecond domain as SQL; public parsing and serialization preserve pre-epoch
and wide timestamps. The collector stops only after the boundary key group,
preserves exact totals, and reports matching candidates plus
`ordered_scanned_count` (all traversed physical references before membership).
Offset is supported. Incompatible null policies/collations and unproven orders
retain the native doc-value fallback. Existing scoring uses full-corpus
statistics.

Use compatible ordered relational indexes as candidate producers for field sorts.
Intersect each ordered candidate with exact search membership, collect the page,
and stop when the ordering contract proves no later candidate can enter it.
Compatibility includes direction, null policy, collation, equality prefixes,
public-ID ties, and forward/backward cursor semantics. A private producer key is
not proof of public-ID tie order. Unsupported shapes keep bounded doc-value top-N.
Compute selected-hit scores against full-corpus statistics. Keep exact count work
separate from early stopping when the request asks for a total.

Acceptance: differential sorted pagination across files and segments, equal keys,
nulls, both cursor directions, offset, count-only, filters, and snapshot changes.
A compatible small page should avoid decorating every matching document. Expose
visited candidates and sort-value reads in profiles.

## Bulk bitmap slices and count kernels

Implemented `sliceRebased` and `rangeCardinality` in shared Roaring storage.
Slices clone only intersecting containers, mask boundary words, and shift with
word kernels. Segment doc-number filters and counts use these kernels. A direct
bitmap count over a segment without deletions uses rank/cardinality with no result
allocation. Compound filters retain bitmap operations and deletion masks.
Union/shift allocation failures now propagate with ownership-safe cleanup.
Container membership uses binary search. Query-owned sparse selections prepare
per-word rank prefixes once; mutations invalidate that navigation metadata before
changing containers.

Fused ordered-index builds reserve spill-file capacity across the cohort: at most
eight simultaneous sorts retain four runs each, leaving room in the unchanged
64-file budget for pending writes and merge outputs. Level-aware compaction
remains in the shared sort implementation. Publication already shares decoded
replay across index builders. Direct ordered-index callers now create a union
projection replay when they exceed eight definitions. A separately tested
changed-file planner unions dependencies across all cohorts, reuses proved seed
roots and captures each required file once; unchanged files need no Parquet
rescan. Cohort sort ownership and the file budget remain unchanged.

Add a Roaring range-slice/rebase operation that copies or combines containers and
masks boundary words without iterating every selected row. Use it to lower global
predicate selections into segment-local filters. Add cardinality-only kernels for
supported Boolean/count shapes so counts need not materialize result candidates.
Preserve sparse arrays, dense containers, deleted rows, and full u32 boundaries.

Acceptance: differential tests against scalar iteration for empty, sparse, dense,
overlapping, unaligned, and boundary ranges, including allocator failures. Compare
large broad-filter counts and peak allocations with the existing scalar path.

## Shared temporal predicate semantics

Implemented signed i128 Unix-nanosecond temporal range operands in the shared
datetime module. Standard pattern ranges normalize RFC3339 offsets and compare
typed numeric timestamps with the same order. The lake index planner admits
proven datetime ranges; SQL lake comparison and existing Iceberg partition
pruning use the shared signed datetime conversion. Plain RFC3339 string columns
retain the fallback because their lexical indexes cannot prove chronological
order. String term equality remains literal equality.

Define a common timestamp representation, units, offset normalization, and range
comparison contract for the pattern evaluator and ordered index planner. Prove
compatibility per schema/type and bound; use normalized timestamp indexes or
expression indexes for RFC3339 strings rather than treating lexical order as
chronological order. Reuse the proven predicate for Iceberg partition pruning.
Keep fallback evaluation for unproven or mixed representations.

Acceptance: timezone offsets, fractional precision, negative epochs where
supported, inclusive/exclusive bounds, nulls, and malformed values agree with the
shared evaluator. Test real Parquet and Iceberg in e2e-full, and verify selective
date filters avoid full archive scans.

## Delivery and measurement

- [x] Implement native sparse ordinal intersection and selective posting seeks.
- [x] Evaluate vector residuals over indexed physical selections.
- [x] Add compatible ordered-index top-N execution.
- [x] Add bulk bitmap slice/rebase and cardinality kernels.
- [x] Prove and push down normalized temporal predicates.
- [x] Run focused differential/OOM/cancellation checks and real Parquet/Iceberg e2e-full.
- [ ] Record cold/warm benchmark results, including a representative large archive.

Each implementation commit should state the workload, baseline, observed resource
counts, and remaining fallback shapes. Existing 100,003-row E2E correctness results
do not establish throughput for a 50-million-row archive. Preserve build, spill,
metadata, and response budgets throughout these changes.

Validation on 2026-10-07: the Debug server build, 319 SQL tests (three skips),
27 sparse tests, shared lake API tests, and 28 standalone bitmap tests passed.
The real lake E2E files passed all 20 Parquet tests; the independent PyIceberg
case passed with the required Iceberg extras enabled. The expanded 100,003-row
two-file Parquet test passed again against the rebuilt binary, together with
Iceberg (two tests, 191 seconds). Its ascending and descending offset-7/limit-3
checks require at most 11 decorated candidates and an exact total of 100,003;
the cross-file boundary tie check requires at most four. These counters establish
early stopping, not a cold/warm throughput comparison. Final temporal admission
and wide integer timestamp regressions additionally cover pre-epoch lexical
index rejection and signed values beyond i64. The refinements below supersede the residual dependency and forward-cursor
limitations recorded by that validation run.

## Signed datetime, reverse traversal, and bounded sparse refinements

Native mapped datetime doc values use wire tag 7: signed i128 Unix nanoseconds,
encoded as 16 little-endian bytes. Legacy tag-0 unsigned datetime columns remain
readable and normalize into the signed domain for sorting and cursors. Typed
column merges promote legacy unsigned values when a signed datetime column is
present. Index-sort bounds carry a distinct signed timestamp tag. Public cursor
values remain normalized RFC3339 strings, with exact nanosecond precision and
date-only input compatibility. The native lake producer recipe advances to v3
so rebuilds cannot reuse unsigned-only source projections.

The persistent page tree supports a reverse half-open cursor with one initial
upper-bound descent and lazy preceding-child reads. Ordered native search uses
that traversal for search-before, retains the complete boundary tuple group, and
returns the previous page in the requested order. Cost and runtime probe guards
apply in both traversal directions.

Sparse accumulation keeps small queries in a fixed-size hash table and switches
to native spill runs above the limit. Records sort by native ordinal and original
contribution sequence; a streaming reducer feeds the existing bounded winner
heap. This preserves native f32 addition order instead of relying on nonnegative
WAND bounds. Cancellation is checked during ingestion, merge and reduction.
Memory-only callers without spill I/O fail scratch admission rather than growing
without bound. Legacy checkpoints retain defensive identity/hydration fallback.
The contribution sequence and spill byte budget have explicit checked limits.

## Embedded Parquet pruning

Parquet and Iceberg scans share row-group statistics, standard ColumnIndex /
OffsetIndex page skipping, and standard split-block Bloom equality probes. Bloom
metadata survives inventory encoding v18; older inventories, including v17,
remain readable. Readers support both length-bearing and older offset-only Bloom
metadata. A probe reads at most 256 header bytes and one 32-byte bitset block,
through the same versioned range cache and cancellation context as other reads.
When the selected block fits in the header lease, it is reused without a second
range request. Invalid compact page-header tags return a decoding error instead
of terminating the process.
Only BLOCK / XXHASH / UNCOMPRESSED headers and proven physical encodings supply
negative evidence. Unsupported algorithms, annotations and malformed headers
retain scanning. Bloom filters never replace residual predicates.

These structures complement snapshot-bound secondary indexes. Source data files
remain unchanged. WAND-style posting-work reduction and representative archive
cold/warm benchmarks remain future opportunities; bounded accumulation alone is
not a claim of sublinear posting traversal.


Validation on 2026-10-08: the Zig 0.17.0 Debug production build passed, along
with lake-test (397 embedded and 131 server tests), lake-api-test (107 embedded
and 87 server tests), all 31 sparse tests, 43 native query-reader tests, and
319 SQL tests (three benchmark skips). Signed datetime regressions cover
pre-epoch and year-9999 projection, legacy unsigned reads and sorted compaction,
and concrete cursor domains without schema metadata. Sparse spill tests force
multiple merge passes and compare every f32 result bit with original-order
signed accumulation.

All 22 real Parquet/PyIceberg e2e-full tests passed together in 276.59 seconds
against the final runtime. The 100,003-row two-file fixture covers nonpositive
sparse scores, broad indexed predicates, point-filter native sorting, forward
and backward pages in both sort directions, pre-epoch native datetime sorting,
combined bounds, residual predicates, exact totals and public-ID ties. Skewed
text membership retains bounded ordered probing and exact native fallback.

An independent PyArrow reader verifies the extended standard Bloom fixture.
With statistics disabled and unreadable data pages, absent equality succeeds
through embedded Bloom pruning; present equality attempts decoding, returns an
error, and leaves the server alive. Unit tests cover offset-only legacy Bloom
metadata, unsupported annotations/algorithms, small-filter lease reuse and
bounded probes into larger filters. Real Iceberg snapshot, field-ID, partition,
delete and restart coverage also passes. Embedded boundary validation (747
production sources), Zig formatting, Python syntax and diff whitespace checks
passed. No cold/warm archive throughput benchmark is claimed.

## Signed histogram and paging regressions

Native embedded date histograms, including nested bucket keys, retain signed i128
nanoseconds. Shared UTC truncation uses floor division for pre-epoch intervals
and correct Gregorian conversion at year 0000 and 9999. The unsigned collector
remains available for legacy callers. Tests cover negative/wide timestamps,
calendar boundaries, nested results, posting-page OOM cleanup and reclamation,
large-segment admission through small live blocks, and ten ordered indexes across
two bounded cohorts. A real Parquet E2E regression compares filtered sparse
scores and ranking with the unfiltered quantized scorer before and after restart.
Representative archive throughput and cold-cache measurements remain pending.

Paged sparse roots also carry conservative authenticated native ordinal bounds.
Positive selections reject disjoint segments before opening posting streams,
avoiding a first-block read from every later segment for a point query. Older
paged roots without the optional bounds retain the ordinary read path. The
multi-segment regression admits only the overlapping stream and reads one block.

## Bounded native generation publication and metadata maintenance

Native sparse checkpoint recipe v7 and index-definition fence v8 add independently
addressable score summaries and ordinal bucket routes. A term's routes carry
conservative segment ordinal bounds. Positive selections occupying at most eight
16-bit ordinal buckets seek their interval-tree ancestors, deduplicate segment identities,
and reject disjoint extents without loading term-directory roots. Broad selections
use the primary term route. Legacy routes retain root-based discovery until rebuilt.
Each segment/term has one dyadic covering route, preventing wide compactions
from multiplying routes across every covered bucket. Navigation admission includes
the deduplication table.

Posting score summaries contain the first ordinal and the canonical quantizer's
minimum/maximum decoded weights. Native DAAT streams initially load these small
summaries; conservative block and prefix bounds can reject a block before its
payload is read. Scoring or advancing within an admitted block materializes its
payload. Signed weights, exact ties, canonical f32 accumulation, and legacy blocks
keep their existing semantics. Summary and payload keys share a generation and
publish together. Speculative payload warming remains bounded by the shared scheduler.

Compaction allocates a durable, never-reused generation ID and an intent record.
Posting pages, score summaries, incarnation proofs, and paged forward-vector
metadata stage in transactions bounded by 4 MiB of payload (plus the largest
individual metadata record). No query discovers a generation until the transaction
that publishes its roots and routes. The root switch also retires old roots and
records cleanup intents. Reclamation then deletes at most 128 keys per transaction;
readers retain their original backend snapshots. Writable startup processes the
small intent directory to reclaim abandoned staging and interrupted retirement,
without scanning all archive pages. Reclamation checks posting and document-map
roots independently because both families can share a numeric generation ID.

Term-route cleanup copies each term directory once before mutation. It no longer
calls transaction get once per term, avoiding retained quadratic copies in LSM
write transactions.

For modern input generations, incarnation sidecars stream in ordinal order into
private fixed-width disk proofs. Term merging resolves these proofs without
retaining public IDs or a corpus-sized incarnation hash table. Per-stream ordinal
hints use galloping seeks, so sequential terms reuse the bounded proof page cache
instead of repeating a full binary search for every posting. A bounded merge of
the source proofs emits output epochs. Forward-vector metadata uses the shared
external sorter, deduplicates native ordinals, and stages independently addressable
records under a small document-map root. Legacy blobs remain readable. Locator
refresh follows publication in bounded transactions, resumes from durable intents
after a crash, and checks current incarnation
and tombstones under the write gate; until refresh finishes, the ordinary fallback
resolves the published document-map root by ordinal through the small in-flight
intent directory, including read-only snapshots. The complete-locator fast-miss
fence therefore remains valid during refresh. Physical-to-native mappings retain the
same identities throughout compaction.

Bloom lookahead uses up to four independent row-group jobs on the shared scheduler.
It starts before the first matching group, covers all-negative cursor scans, and
advances a monotonic plan position to avoid submitting duplicate probes.
Credential-scoped, immutable cache keys coalesce speculative and required range
reads. Every worker joins before inventory/descriptors are released and inherits
request cancellation/deadlines. A negative Bloom result prevents speculative data
page decoding, and constant statistics matching an equality predicate avoid a
Bloom probe that cannot help prune. No new query options or source authority are
introduced.

Validation includes exact signed scoring against an exhaustive reference,
metadata-only block rejection, single-get directory cleanup, bounded disk-proof
lookup, hidden partial output, recovery of abandoned staging, old-reader leases,
replacement/deletion fencing, forward lookup, and restart. Archive-scale cold/warm
throughput remains a measurement requirement, not a claimed benchmark result.
