# Native lake performance follow-up

Status: active follow-up. Native execution, public snapshot tokens, exact filters,
and reader leases are implemented on top of PR #1006. The contracts below describe
implemented refinements and their validation. Publication-wide best-first text
navigation, adaptive native batches, shared query preparation, and bounded
profiling/composed/pruner/reranker/evaluation admission are implemented
in the active follow-up. Eligible accepted-WAL text queries also use bounded
windows with combined global statistics and recent-first cutoff seeding.
Cross-source-file serving packs and durable publication-bound cloud generation
maps/certificates are implemented, as are eligible accepted vector/hybrid shapes.
Real-provider retention enforcement qualification and representative archive
benchmarks remain deferred; the final implementation notes distinguish these
from deterministic contract tests.

## Native sparse predicate intersection

Implemented through query-owned compressed native ordinal selections. Native
sparse recipe v8 persists authenticated 1024-row physical-to-native ordinal
blocks inside the checkpoint. Each entry stores a two-byte physical offset and
four-byte native ordinal; ingestion coalesces writes with at most 64 resident
blocks. Inserts, replacements, deletes and compaction update these maps in the
same transaction. A completeness marker distinguishes missing vectors from
legacy checkpoints, which retain point lookup fallback until rebuilt. Queries
translate a physical selection with one borrowed cursor lookup per occupied block
and check cancellation between blocks, without formatting a document key per
selected row or retaining every LSM point-read payload until transaction close.
Tombstone and incarnation checks use independent cursor leases and capped scalar
epoch caches, so visibility metadata ownership remains bounded for broad queries.

All positive selections use the canonical quantized posting scorer. Point filters
seek directly to the posting block covering the next selected native ordinal;
broad filters intersect the same ordinal bitmaps with posting ranges. This keeps
scores and ties identical to unfiltered scoring, including zero and negative
scores for overlapping terms. Forward locators remain identity/update metadata.

New immutable segments publish a 32-byte `ASPSPG02` root. Term directories live in
independent pages of at most 64 terms; compaction iterators retain one copied page
per input. Posting-block KV values remain keyed by segment, term, and final
ordinal. Their optional `ASP2` transport bit-packs positive ordinal gaps and keeps
the first absolute ordinal, V1 quantized weight bytes, range data and f32 decoding
order unchanged. Blocks that do not shrink keep their original encoding. Readers
validate and expand at most one bounded block per stream; the shared 64 MiB
resident budget accounts expanded bytes. Navigation has a separate byte budget
instead of a fixed 4096-stream limit. Selective seeks avoid earlier posting blocks.
`ASPSSEG1` and `ASPSPG01` roots and uncompressed blocks remain readable. The v8
native sparse recipe and v9 catalog definition fence rebuild older remote
publications; local checkpoints upgrade through ordinary copy-on-write maintenance.

Maintenance pins its input snapshot and reserves a durable generation intent in a
short apply section. Modern incarnation proof capture, posting merges, directory
spooling and bounded output staging run outside the apply lock. Each directory
page stages its term routes and interval routes in the same transaction. A guarded
route is usable only if the reader's same snapshot contains its generation root;
partial staging and retired generations cannot enter scoring. Publication validates
input roots, activates the new roots and retires old roots under the apply lock.
Locator refresh reacquires the lock for at most 256 records per batch and checks
both the live generation and current document incarnation before writing. A
competing publication therefore cannot restore an obsolete locator.

Retirement reclaims directory pages and their route ledgers in bounded batches,
outside the apply lock, followed by posting, summary, incarnation and docmap
entries. Durable intents recover interrupted staging, refresh and reclamation on
restart. A live docmap retains its intent until refresh completes. Backend
snapshots retain old-reader visibility throughout retirement. Legacy roots retain
explicit memory admission and compatibility capture/publication paths. Modern
maintenance retains one posting block per input, one output chunk, bounded sort
state and file-backed proof/directory runs; the entire archive is not materialized.

Term-to-segment and interval routes avoid archive-wide root discovery. A coverage
marker keeps legacy local checkpoints on the discovery fallback until their next
publication creates complete routes. Queries reuse posting and root cursors.
Native
queries also warm the following posting block through at most eight speculative
jobs on the shared CPU/I/O scheduler. Each job owns an independent fork of the
same immutable snapshot; saturation yields to required work. Completion or
cancellation joins the worker before releasing its read lease. Speculation keeps
no result buffers and relies on the independently bounded native read caches.

Document-at-a-time scoring retains one document accumulator and k winners.
Conservative block bounds include zero and both signed decoded endpoints;
score/ordinal pruning preserves the exact winner order. Block-prefix pivots exclude terms whose next
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
Contiguous extents remain compressed. Authenticated live-row bitmaps restore
deleted holes through a four-slot cache with at most 4 MiB of decode/rank backing
storage, including with arena-backed requests. Each slot prepares word-rank
prefixes once; sparse selected ordinals use binary container/word rank-select
rather than walking every live row. Broad selections retain sequential bitmap
intersection. Eviction reuses fixed buffers instead of accumulating freed arena
objects.
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
Container membership and the starting container for range slices use binary
search. Query-owned sparse selections prepare
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

Native sparse checkpoint recipe v8 and index-definition fence v9 retain independently
addressable score summaries and ordinal bucket routes. A term's routes carry
conservative segment ordinal bounds. Positive selections occupying at most eight
16-bit ordinal buckets seek their interval-tree ancestors, deduplicate segment identities,
and reject disjoint extents before loading posting payloads. Guarded routes check a
constant-size active root in the same snapshot. Broad selections
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
individual metadata record). Guarded routes stage with their directory pages and
become usable only when a final transaction activates their generation roots.
The root switch also retires old roots and records cleanup intents. Posting and
metadata reclamation deletes at most 128 keys per transaction; a directory-ledger
batch deletes at most 129 route/directory keys. Readers retain their original
backend snapshots. Writable startup processes the
small intent directory to reclaim abandoned staging and interrupted retirement,
without scanning all archive pages. Reclamation checks posting and document-map
roots independently because both families can share a numeric generation ID.

Modern term-route cleanup copies one directory page before each mutation batch.
The page is a durable ledger for both route families, including generations that
never activated. Legacy route cleanup retains its single-directory-copy path.
Neither path retains one root copy per term in an LSM write transaction.

For modern input generations, incarnation sidecars stream in ordinal order into
private fixed-width disk proofs. Term merging resolves these proofs without
retaining public IDs or a corpus-sized incarnation hash table. Per-stream ordinal
hints use galloping seeks, so sequential terms reuse the bounded proof page cache
instead of repeating a full binary search for every posting. A bounded merge of
the source proofs emits output epochs. Forward-vector metadata uses the shared
external sorter, deduplicates native ordinals, and stages independently addressable
records under a small document-map root. Legacy blobs remain readable. Locator
refresh follows publication in batches of at most 256 records, resumes from
durable intents after a crash, and rechecks the active generation, current
incarnation and tombstones under each short write gate; until refresh finishes, the ordinary fallback
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

## Validation and remaining measurement

Focused regressions cover bounded physical-map cursor reads, packed transport
round trips across all gap widths, malformed input and allocation failures,
constant-size roots with multiple term pages, hidden staged routes, abandoned
output reclamation, pinned old readers, restart recovery, and generation retirement
between locator batches. Live-row cache tests cycle more artifacts than fit in the
cache while allowing exactly four backing buffers; rank/select tests compare each
selected ordinal with scalar iteration across dense holes and u32 boundaries.
Existing score differential tests cover negative terms, ties, filtering and spill.
Visibility lookup regressions exercise 100,003 missing keys with one seek per
independent metadata lane, cached EOF, backward lookups and seek-error recovery.
The same proven-interval reuse serves streamed disk-proof capture without
alternating one cursor between epoch and deletion key families.
Real Parquet and PyIceberg end-to-end tests remain the integration gate.

Representative archive benchmarks are still required to quantify throughput,
provider bytes, expanded-block residency and apply-lock latency. The packed codec
reduces bytes for dense-gap fixtures; that ratio is not an archive throughput
claim. Legacy conversion work and source/document directories still have explicit
admission costs. These changes do not promise constant total memory for arbitrary
legacy checkpoints or eliminate the need to measure skewed workloads.

### Canonical sparse dimensions and complete public ordering

New sparse writes sort dimensions and coalesce duplicates in original input
order at the shared index ingestion boundary, before either bulk or delta writes.
Bulk producers and compaction attest unique ordinals across a term with an
`O32U` trailer. Older `O32B` and unextended framed streams remain readable and
accumulate repeated ordinals within and across blocks; without that producer
proof, score bounds are disabled. Compaction coalesces legacy repeated postings
before emitting a proved stream. Handoff and rewritten legacy chunks do not
invent a uniqueness proof from one locally unique chunk.

Posting summaries retain the actual decoded minimum and maximum quantized
weights and a uniqueness flag. Older 12-byte summaries take the conservative
path; current 13-byte summaries can prune before payload reads. Equal-score
blocks/prefixes are skipped only when their first possible ordinal loses to the
heap boundary. Signed contributions retain canonical f32 addition order.

Each bounded native text corpus owns one immutable physical-to-native identity
directory, shared by search, planning, and highlights through the corpus lease.
Directory construction copies typed blocks and bitmap references directly;
queries no longer serialize and parse the entire directory through JSON. Directory
allocations count against the existing corpus heap budget and remain pinned
until the last reader releases that corpus generation.

Ordered-row recipe v6 adds an authenticated tuple/file count directory alongside
the existing forward, reverse, and predicate trees. A large logical-key tie group
has one count per participating file. Initial construction groups counts in a
bounded spill stream; incremental publication adjusts only changed tuple/file
counts and retains untouched pages. Artifact GC marks the new tree under the
same reader-safe publication lease as every other native artifact.

When the relational index proves every nonconstant sort field, ordered text
search uses this directory to visit participating files in public `lake1:` digest
order and seeks physical rows within each file. The decoded-root cache computes
snapshot-specific file digests once; changing their permutation requires no row
reindexing. Search-after/search-before use the complete tuple and exclusive
physical coordinate boundary. The native cursor supports both public-ID traversal
directions; the public API retains its ascending final `_id` tie-breaker. The
collector can stop within a tie group after its bounded winner window. Extra nonconstant keys,
legacy roots, and incompatible orderings retain the existing complete-tie fallback.
Per-group and per-file arenas are reset independently; prefetch remains bounded
by the requested window, 256 references, and the physical probe budget.

Representative cold/warm archive throughput benchmarks remain pending. Kernel
and pagination counters establish avoided scoring/traversal, not an archive-scale
latency or throughput claim.

Validation on 2026-10-09: the Zig 0.17.0 optimized production build passed.
The sparse suite passed all 60 tests after merging main's storage changes from
#1027; the mounted and embedded API test binaries passed 108 and 116 tests with
zero leaks. The real Parquet/PyIceberg suite passed all 26 cases in 84.47 seconds
with `ANTFLY_E2E_FULL_LAKE=1` and normal filesystem disk safeguards. Its two-file,
100,003-row fixture checks three-row pages through a 100,000-row timestamp tie
in both primary-sort directions, including after/before cursors, with at most
four scanned candidates per page. The public `_id` tie-breaker remains ascending;
the fixture declares matching ascending and descending timestamp indexes.
Regenerated Go SDK tests, license checks, both source-boundary audits, formatting
and diff checks also pass. Archive-scale cold/warm throughput remains unmeasured.


### Adaptive predicate membership and warm metadata

Exact whole-index metadata conjunctions can defer membership until text filtering
produces local candidates. Binary searches map native ordinals into the shared
file/block directory, including rank/select over authenticated delete holes.
A reverse-tree point probe checks the stored tuple against exact predicate bounds
without Parquet hydration. One request owner holds the predicate metadata lease,
bounded reusable path scratch, and live-row cache until every scoring/count pass
finishes. Errors and cancellation propagate through the native filter contract.

The admission model charges 64 work units per point probe and never spends more
point work than the cheaper index/scan full-membership estimate across segments
and passes. On fallback the existing whole-condition planner still chooses among
exact scan, whole-index, and separate-index intersection plans.
An independent 8 MiB authenticated page-read allowance switches plans before
another worst-case reverse-tree path could exceed it, preserving the existing
full-materialization read budget. If candidates are broad or either probe budget
expires, one lazily materialized compressed bitmap serves all subsequent passes. Cheap metadata predicates still materialize
up front, preserving direct bitmap pushdown into native bounded scorers. Exact
counts and exclusions use the same producer; a candidate-local answer is never
cached as complete membership. General Boolean/residual plans retain their
existing exact evaluator. This does not make arbitrary broad queries LIMIT-bounded.

Decoded ordered roots carry structural-validation proof and a sorted file-slot
directory. Warm readers retain that cache lease and repeat the request fingerprint
check, without rebuilding an O(files) validation hash table. Publication, credential,
snapshot and cancellation checks retain their request-owned authority.

Sparse positive epoch reads use bounded 64-ordinal pages for 32 independently
pinned key prefixes after observing two nearby probes. Extra prefixes keep the
existing point/gap fallback rather than evicting hot pages. Missing families retain
EOF/interval proof reuse, and scattered/selective probes avoid speculative scans.
A read failure cannot leave a valid partial page, and old transactions continue
seeing their original epoch values after a newer publication.

Fresh bitmap seeks now binary-search container keys. A local ReleaseFast CPU
measurement of one million probes into 50 million dense ordinals improved from
383,519,375 ns to 9,662,500 ns (about 40x). This is one kernel measurement with
concurrent compilation active, not remote query latency or archive throughput.
Run `zig build lake-bitmap-seek-bench -Doptimize=ReleaseFast` to reproduce the
current kernel and compare its checksum against prepared rank/select.
The captured sample is in `bench/baselines/native-lake-bitmap-seeks.json`.

Validation of these refinements after merging `origin/main` through `df65a4c8e8`:
the native reader suite passed 358 tests, sparse passed 62, bitmap encoding passed
31, and lake integration passed 113 embedded and 98 API tests, with zero leaks.
The overlapping-generation fixture retains its authenticated physical directory
while selecting shared segments; production metadata validation remains strict.
The optimized production server built successfully, and all 26 real Parquet/
PyIceberg E2E cases passed in 135.65 seconds with `ANTFLY_E2E_FULL_LAKE=1` and
normal filesystem disk safeguards. The two-file 100,003-row predicate fixture
also checks rare-term inclusion/exclusion, exact counts and score parity across
native segment offsets. License checks, both source-boundary audits, formatting
and diff checks passed. Representative archive-scale latency remains unmeasured.


## Filtered top-K and repeated pagination follow-up

This follow-up to #1017 removes three remaining archive-sized serving paths.

### Text scoring and exact metadata membership

Simple term/match queries and supported same-field Boolean queries feed candidate
scores directly into one bounded top-K collector. Deferred metadata membership
receives at most 64 live candidates from one native segment per batch. Include
and exclusion producers share that candidate batch and retain their existing
request-owned adaptive reverse-index probes and full-membership fallback.
When adaptive probing materializes complete membership, a borrowed complete-set
hook immediately enables ordinal seeks in the same scoring pass. Candidate-local
answers never enter that hook. Later score batches borrow the complete bitmap
directly instead of copying full segment membership. A pending batch can only
delay the competitive cutoff, so block pruning remains conservative. Segment transitions flush before
changing ordinal offsets.

Filtered minimum-one disjunctions use the native Block-Max WAND scorer with the
same corpus document frequencies, field lengths, BM25 configuration and bound
cache as unfiltered ranking. Query-wide statistics are resolved once. Filtered and
unfiltered queries share segment-bound planning, scoring the strongest segments
first on fragmented snapshots and pruning weaker segments with strict score
bounds that preserve ordinal ties. Segment access leases cover scoring.
Exact compressed include/exclude masks provide monotone ordinal lower bounds
before scoring. Sparse includes jump directly to their next member. Word-level
intersection-minus-exclusion inspects at most 64 words per seek, then yields to
posting navigation and cancellation. This avoids both archive-wide alternating
mask walks and scanning the gap before a selective include. Hit admission remains
exact when navigation returns a conservative lower bound. Supported conjunctions retain
block pruning with membership applied before a hit raises the cutoff.

Exact counts still execute the authoritative filter path. Aggregations, cursor
ranking, distributed statistics and unsupported Boolean/boost shapes retain their
existing fallback semantics. Ranked totals are lower bounds when competitive
blocks are skipped. No arbitrary query is promised to be LIMIT-bounded.

### Sparse exclusion masks

The same-transaction native ordinal interface now accepts independent optional
include and exclusion masks. A missing include admits the universe; an empty
include admits nothing. Physical selections of at most 4096 rows resolve to
compressed native ordinals. Broader includes or exclusion-only selections retain
an exact residual key predicate in the same pinned generation. Bounded
document-at-a-time scoring resolves only reached candidate identities and shares
compressed allow/deny decisions across terms. An independent reverse-key cursor
releases identity scratch as it advances instead of retaining point-read payloads
in the parent transaction; broad metadata masks are never
translated in full merely to score a rare term. Legacy positive-only callbacks
and callers without ordinal selectors use the same candidate path when complete
native identities are proven.

The scorer seeks with bounded word-level intersection/difference navigation and
rejects fully excluded materialized block ranges before payload decoding. Both bounded and
spill fallback paths apply the same masks, deletion/incarnation checks and direct
constraints. Legacy positive-only callbacks remain supported. When an API query
also has a positive physical selection, exclusions are subtracted before native
ordinal conversion so a one-row include does not require translating a broad
exclusion independently.

### Snapshot-scoped public tie ordering

Verified ordered metadata derives the public file permutation once. Cursor
boundary file resolution uses binary search over that permutation. Participating
files for a logical tie are sorted and stored in the existing bounded,
singleflight decoded cache; reverse pagination traverses the same array backward.
Cold scans retain the sequential directory cursor and its pending next-group
record. Only groups estimated to span directory pages are cached. Warm cache hits
seek past the group's directory entries. Both paths binary-search the participating file array at a
pagination boundary instead of rejecting all preceding files individually.
This preserves the read budget for high-cardinality sort keys.

Cache keys bind the serving scope, immutable tie-tree identity, root domain and
fingerprint, source, snapshot and logical tuple. Cache waits retain request
deadlines and cancellation. Payloads own only file slots in the cache arena;
cursors copy a bounded slot array before releasing the lease, so eviction cannot
invalidate active pagination. The physical row trees and on-disk metadata format
remain reusable across snapshot changes. A hit seeks past the tuple's directory
entries instead of rescanning and sorting every participating file.

Validation targets include exhaustive score/rank parity, exact counts, signed
sparse weights, exclusion-only execution with a one-document accumulation budget,
forward/backward tie pagination, scope fencing, eviction and reduced warm page
reads. The real Parquet E2E archive fixture also exercises broad exclusion-only
sparse queries with positive, negative and zero weights. Representative cold/warm
archive latency measurements remain required; no end-to-end speedup is claimed.

Review regressions cover a 5000-distinct-key ordered scan in both directions under
the unchanged 256 MiB read budget, overlapping million-row masks with a bounded
seek, direct sparse-include jumps to the u32 endpoint, fragmented segment pruning,
and iterator ownership on failed WAND admission. The sparse API planning test
proves a 100000-row exclusion performs no ordinal lookups, while a selective
include subtracts it before resolving its remaining row. Signed/zero sparse weights
retain exact results with a one-entry accumulation limit even for residual
predicates. The shared WAND helper consumes its incoming iterator on success and
failure so allocation failures release authenticated metadata owners.

Validation of the review refinements on 2026-10-09: all 20 focused tests, 63 sparse
tests, and 372 native reader tests pass without leaks after merging `origin/main`
through `a202a18842`. The unchanged bitmap implementation also passes all 33
standalone tests. The 5000-distinct-key scan succeeds forward and backward under
the existing read budget. License headers, Apache and embedded source boundaries,
formatting and whitespace checks pass.

Both Debug and ReleaseFast server builds pass at `c11d61de61` (main through
`bfbcb03eae`). All 26 real Parquet/PyIceberg E2E cases pass against that optimized
binary in 91 seconds with unchanged fixture limits, including the 100003-row
predicate archive. These server/E2E results precede the final upstream merge;
the focused, sparse and reader checks above were rerun afterward. A diagnostic
Debug E2E run passed 25 cases but exceeded the archive fixture's 300-second
index-publication deadline before its query assertions. No fixture deadline or
production limit was relaxed. Representative archive throughput remains
unmeasured.

## Follow-up to #1046: adaptive vector membership and bounded scoring state

Status: implemented in #1051, on top of merged main `cc5fb8abfb`. These changes
preserve the public query API and native artifact formats. They address the four
remaining opportunities from the #1046 review.

### Adaptive metadata membership for vector queries

`lake_index_text_predicate.zig` now owns an adaptive physical membership provider
for each vector include/exclude predicate. `lake_index_text_query.zig` shares
those query-owned providers across dense, sparse, and hybrid consumers while
keeping the existing source, publication, authorization and runtime pins alive.

Cheap exact selections still materialize immediately. A broader predicate can
start with reverse-row-index probes of candidates actually reached by ranking.
Direct probes require authenticated reverse trees and proof that tuple bounds
enforce **every** condition (`rangeEnforcesConditions`); an indexed superset is
insufficient. A whole-predicate index or a conjunction of independent exact
column indexes provides that proof. Independent predicates short circuit in
increasing estimated-cardinality order. OR/residual shapes retain the existing
full predicate planner.

Each point probe costs 64 relative work units, consistent with the text producer.
A composed conjunction reserves 64 units per child before evaluating a candidate.
Accumulated point work cannot exceed the cheaper combined index-walk/column-scan
estimate. Exhausting this budget, or the reader's independent authenticated
page-read budget, switches once to the full planner's compressed physical set.
Subsequent hybrid consumers reuse that set. Include/exclude state is independent.
Cancellation and lease/deadline checks run even when membership is fully cached.

Selections with at most 4,096 candidates in their cheapest exact index materialize
upfront, independently of cold metadata setup cost. For composed predicates,
materialization drives the smallest physical selection and chooses bounded point
probes or a compressed index intersection for each remaining column. An exhausted
point-read budget falls back to the independent tuple walk without admitting
partial output.

Sparse planning converts only inexpensive completed physical sets to ordinal
masks. A query-local completion revision refreshes those masks in the same pinned
native transaction before the next DAAT seek or fallback chunk. Thus completion
helps the current search immediately. An incomplete or broad provider remains an
exact candidate predicate inside scoring, before heap admission. Dense ranking uses its existing exact
eligibility callback and retains its existing ANN approximation contract.
Physical coordinates remain distinct from native ordinals across generations.

### Bounded sparse predicate decisions

Sparse scoring replaces two growing allowed/denied bitmaps with a fixed 256-slot
exact-tag decision cache. Its storage is at most 4 KiB, independent of archive
size. DAAT processes a document's contributing streams together and reuses one
predicate decision. The unordered term-at-a-time/spill fallback can evict and
repeat exact probes; it never assumes monotone order or reuses a colliding tag.

Deletion and incarnation checks remain per stream and precede shared key
membership. The reverse-identity cursor and existing bounded scratch remain in
place. Score accumulation order, signed contributions and bitmap constraints are
unchanged. A 100,000-ordinal collision regression verifies exact eviction, and
existing sparse differential tests verify multi-term predicate reuse and signed
score equivalence.

### Shared native text top-k heap

`scorer.offerTopK` provides one worst-first bounded heap to both `FastTopK` and
`TopKCollector`. The root supplies the competitive score and tie document ID in
O(1); accepted replacements take O(log k). Equal scores retain the smallest
document IDs. Counts, relations, producer batching, deletion checks and final
result ownership remain in their existing layers. Underfilled collectors retain
the established zero competitive threshold, and final output is sorted once.

Differential tests compare full sorted output for 4,096 deterministic arrivals
with ties, forward/reverse order, k=0, underfilled windows and k up to 5,000. WAND
cutoff/tie, filtered producer and allocation-failure regressions also pass.

A ReleaseFast microbenchmark offers 40,000 increasing-score candidates (every
candidate after filling the window is an accepted replacement). It compares the
previous linear replacement primitive with the shared heap and verifies exact
final output. One local run measured:

| k | Linear replacement | Heap replacement |
|---|---:|---:|
| 10 | 1.71 ms | 0.79 ms |
| 100 | 16.40 ms | 0.96 ms |
| 1,000 | 165.90 ms | 1.56 ms |
| 10,000 | 881.70 ms | 1.84 ms |

These are collector microbenchmarks, not archive-query speedups. Rejected
candidates compare with the root without rescanning the heap. Default-window
results do not justify adding a separate small-window implementation.

### Streaming nested and mixed-field Boolean queries

Native Boolean execution now lowers supported nested/mixed-field clauses into
per-segment monotone seekable nodes. Term, analyzed match, match-all/match-none,
and bitmap leaves compose with must, should/minimum-should-match, must-not,
pure optional clauses and nested boosts. Each clause retains its current hit;
ranking retains a bounded heap and existing 64-candidate predicate batches.
Unique terms are collected before segment execution and document frequencies
are loaded in one batch per field through the shared snapshot statistics cache
and scheduler. Each segment opens one scoped reader per field, shared by its term
iterators. Reader contexts, iterator buffers and node arrays use the reusable
segment arena, rather than retaining every segment's scratch in the outer request
arena. Iterators close before readers, and the arena resets between segments.

Existing same-field fast paths remain first. Nested simple nodes preserve their
established lowering and f32 arithmetic order, including legacy BM25 normalization,
boost placement and grouped optional contributions. An N-of-M posting-head pivot
skips candidates that cannot meet minimum-should-match, including optional clauses
under a required conjunction. Common OR/AND cases avoid per-candidate sorting.

Conservative subtree bounds compose each term's own field statistics and posting
block metadata in scorer arithmetic order. Bounds are cached until their earliest
block boundary. Rejected ranges advance metadata cursors without decoding posting
payloads; a competitive seek loads its target block. Negative-boost or unsupported
bounds disable competitive pruning. Strict score/document-ID comparisons preserve
cutoff ties. A pruned search reports a truthful lower-bound hit count (`gte`), like
the existing native WAND paths; an unpruned search retains exact counts.

Phrase/position and other unsupported leaves, distributed statistics,
aggregations and search-after retain the authoritative existing paths. Public
sort/cursor orchestration remains unchanged. Future streaming position leaves
must verify positions before admission. Position/phrase streaming remains future
work outside this term/match tree.

Seeded randomized differential coverage compares 1,000 nested shapes across two
segments with mixed fields, duplicate clauses, zero/negative boosts, optional
clauses, minimum-should-match, deletes, bitmap filters and offsets. Candidate
producer includes/exclusions are compared separately with the all-hit reference
for the first 100 shapes. The remaining 900 compare bounded ranking with a full
unpruned tree, preserving the same lowering and f32 arithmetic policy.
Scores and document IDs must match exactly, without a floating point tolerance.
Unpruned counts remain exact; pruned counts must be valid lower bounds. Phrase
fallback eligibility is checked explicitly. Exhaustive allocation-failure injection covers iterator, statistics, producer-batch, heap
and stored-result cleanup for a nested mixed-field tree.

### Qualification

The implementation was qualified after merging main using Zig 0.17.0:

- Debug: 65 sparse tests, 388 bounded native reader tests (one additional
  ReleaseFast-only benchmark skipped), 21 focused filtered text/scorer tests
  (one additional benchmark skipped),
  and four filtered reader tests; no failures or leaks.
- ReleaseFast: 22 focused text/scorer tests, including the heap benchmark, and
  four filtered reader tests; no failures or leaks.
- Production Debug `antfly` build passed.
- The existing real Parquet/Iceberg `e2e-full` fixture now publishes text, sparse
  and dense indexes, exercises rare/common sparse candidates with broad predicates,
  exclusion-only queries, dense/hybrid filters, and retains text sort/cursor
  assertions before and after restart: both formats passed (20.10 seconds total).
  The separate quantized sparse-score E2E regression passed (1.92 seconds).

Representative 50-million-row cold/warm throughput and peak process memory
remain unmeasured. The heap microbenchmark and bounded state guarantees do not
substitute for that archive-scale qualification.

### Follow-up review regressions and qualification

The follow-up fixes the two review findings: cold/warm selective metadata predicates
retain upfront sparse ordinal seeks, and native segment scratch stays bounded
when the caller uses an arena. It also implements the three remaining opportunities:
minimum-should-match/subtree pruning, shared field readers with batched statistics,
and adaptive conjunction membership across independent metadata indexes.

Work-count regressions verify that a late rare posting jumps over a 2,000-row common
clause, including under required clauses; metadata-only block navigation leaves the
posting decoder on its original block; and sparse membership completion after three
candidate probes refreshes the current 10,000-row search exactly once and seeks to
the final row. An upfront exact mask uses no reverse-key predicate callbacks. Native
arena retention at 1/8/32 segments must stay within three times the one-segment
capacity, and two same-field terms must share one reader. Existing exhaustive
allocation-failure and signed-score differential checks remain enabled.

The real Parquet/Iceberg E2E fixture additionally exercises cold/warm selective
conjunctions, rare/common candidates across independent metadata indexes, transition
to a selective intersection, and composed exclusions, before and after restart.

## Follow-up to #1051: compressed sparse masks, segment plans and positions

The branch merges main's embedded SQL/catalog changes without changing the lake
query API or native artifact formats. Three further native execution refinements
address the remaining review opportunities.

### Sparse constraints bounded by compressed state and translation work

Completed physical sets no longer face a 4,096-matching-row cutoff. Sparse
planning translates authenticated physical-directory blocks into native ordinal
bitmaps in the same pinned read transaction. Consecutive native ordinals are
coalesced into ranges; no external key list or complement is constructed. An
include subtracts a completed exclusion in physical space before translation,
so even two broad sets can yield an inexpensive selective ordinal mask.

Optional planning owns a stable live-byte budget for masks, navigation and
reusable translation scratch (4 MiB), plus shared directory/legacy-seek work
budgets (4,096 physical directory blocks and 4,096 legacy identity point seeks).
The directory block bound limits each native translation to 1,024 physical rows.
These are optimization budgets, not public result or predicate-cardinality
limits. They admit large compressed selections while bounding fragmented
translations. Legacy generations retain bounded point resolution when their
physical directory lacks a completeness proof.

Memory/work exhaustion discards all partial masks and keeps the exact candidate
predicate. Storage errors, cancellation and ordinary allocation failures still
propagate. Mask allocator ownership moves into the sparse query state and ends
after scoring; adaptive completion still refreshes masks only on its one-way
revision. Incomplete providers continue probing reached candidates, so rare
postings do not eagerly walk broad metadata indexes.

### Mixed-field Boolean segment planning

Fragmented snapshots (more than 16 segments, matching the existing text planner's
amortization policy) use a metadata-only prepass. The existing scorer tree's
lowering composes each field's term bounds, boosts, optional grouping and baseline
in scoring arithmetic order. Unsupported signed/non-finite bounds disable
competitive pruning. The prepass shares scoped field readers and opens no posting
iterators. Its arena is reused by segment scoring.

Segments execute in descending score-ceiling order with their original global
document offsets. The admitted heap cutoff can reject a whole segment before
posting streams open. Strict score/document-ID comparisons retain cutoff ties;
pending predicate batches flush before the cutoff is observed. Pruned queries
report lower-bound totals. Healthy small snapshots avoid the dictionary prepass.

### Positional leaves in the streaming Boolean tree

Exact term phrases, analyzed phrases and fixed multi-phrase alternatives compose
with required, optional, prohibited and nested clauses. Each phrase position owns
monotone term heads; all positions must align on a document before position
verification. Deferred position records for rejected approximation documents are
skipped. Two-term exact phrases reuse the packed-position kernel; other shapes
use current-document position buffers rather than corpus-sized position maps.

Exact phrases preserve phrase-frequency BM25 and the sum of constituent term IDFs,
including repeated terms. Fixed alternatives and analyzed position gaps preserve
the established filter score and slop semantics. Missing/empty alternatives match
nothing. Phrase verification precedes heap/predicate admission, and the producer
fast path accepts positional leaves directly. Query-owned analyzed tokens and
phrase filters are shared across segment planning and execution. Fuzzy positional
expansion, distributed statistics, aggregations and search-after continue using
the authoritative existing paths.

Regression coverage includes broad compressed mask translation and budget
fallback, native directory seeks with signed sparse weights, fragmented mixed
field segment pruning with stable ties, randomized positional Boolean queries,
repeated terms, alternatives, analyzed gaps, deletes, offsets, includes/exclusions,
and exhaustive allocation failures. The real Parquet/Iceberg fixture also checks
phrase order under indexed metadata includes/exclusions before and after restart.
Archive-scale cold/warm throughput and peak process memory still require separate
measurement; no archive speedup is inferred from these work-count regressions.

### Qualification of the compressed-mask/segment/position refinement

`origin/main` at `f599f36da5` is merged. Its embedded relational worker move
required regenerating `source_catalog_control.zig` from the current ownership
graph: the two control-safe worker exports are included, and CAPI modules that
now reach the physical owner are excluded. The generated contents match
`tools/check_storage_compilation.py`; the production Debug server builds.

Validation uses Zig 0.17.0:

- Debug: 66 sparse, 390 bounded-reader, 23 focused text/scorer and five API tests
  passed, with no failures or leaks. Two ReleaseFast-only benchmarks skipped.
- ReleaseFast: 66 sparse, 24 text/scorer and five API tests passed, including the
  existing collector benchmark, with no failures or leaks. Exhaustive request
  allocation-failure tests force allocate/copy growth, following the existing
  SQL test pattern, so backing allocator remaps cannot vary the fault sequence.
- A 100,000-row compressed mask occupies less than 128 KiB in the API fixture.
  A real 5,000-row native sparse selection uses six physical directory blocks,
  preserves signed/zero scores and invokes no reverse-key predicate callbacks.
  Explicit work/memory exhaustion discards partially built masks.
- A 20-segment mixed-field fixture searches one segment and prunes 19 before
  opening their postings; only two posting iterators open. Signed/zero boosts,
  stable cutoff ties and offsets are checked against the established scorer.
  Randomized positional trees preserve exact scores/IDs with alternative and
  repeated terms, gaps, deletes, includes/exclusions and native range readers.
- All four real-data E2Es passed against the freshly built server (26.05 seconds):
  independently written Iceberg snapshots/schema IDs/partitions/deletes and
  restart, Parquet and Iceberg indexed conjunctions/sort/cursors/phrase order,
  and quantized sparse score/ranking preservation. The Iceberg fixtures remain
  in `e2e-full`; the entire `e2e-full` suite was not run.
- Zig formatting, Python lint/formatting and `git diff --check` passed.

Representative archive-scale cold/warm throughput and peak process memory
remain unmeasured. Fuzzy positional expansion continues through its existing
authoritative execution path.

## Two-phase verification, lazy sparse windows and parallel ranked pages

The final review identified execution opportunities rather than a confirmed
correctness defect. These refinements keep the same public API, artifact formats,
visibility checks and score/document-ID ordering.

Boolean navigation now exposes a monotone approximation independently of exact
verification. Required clauses and minimum-should-match pivots align posting heads
without unpacking phrase positions. Complete native include/exclude masks and
deletions reject candidates before verification. Cheap exact required/prohibited
clauses run before positional clauses, while score additions retain the established
clause order. Repeated verification of a current document is cached. Incomplete
adaptive providers still refine verified batches; completing providers immediately
supply immutable masks to navigation. A 10,000-document phrase joined with a
one-document term must verify one document and read two position records, in either
clause order; a one-document native mask has the same work bound.

Sparse planning retains the existing 4 MiB compressed-mask allowance. Generations
with native ordinal-window support eagerly translate at most 64 physical directory
blocks; legacy generations retain their 4,096-block/point planning allowance.
Exhaustion discards every partial mask. Score bounds run before deferred identity
refinement. The first 16 distinct competitive candidates in each aligned window
use exact borrowed-cursor point checks; only denser competitive work triggers a
1,024-ordinal translation using a sequential reverse-identity cursor in the same
pinned read transaction. The threshold is a bounded cost heuristic, not an
archive-specific throughput claim. Include/exclude membership is applied together,
and the window is prepared for bitmap seeks. The query retains one reusable window arena;
changing windows and membership revisions invalidate its mask. An empty window
advances posting navigation to its boundary, allowing a rare posting to jump over
intervening directory windows. No archive-wide complement or matching-ID list is
built. Missing completeness proofs retain exact per-candidate predicates; storage,
cancellation and allocation errors propagate. Streaming and bounded spill scoring
share this constraint state and preserve canonical quantized signed arithmetic.

Supported Boolean scoring on query-bound remote snapshots with at least 4,096
documents uses the shared CPU/I/O scheduler when there are multiple admitted work
ranges. A corpus-scaled grain creates at most 4,096 ranges plus one per segment,
with a minimum grain of 4,096 rows; a single large segment can use multiple lanes.
The first planned range seeds the global cutoff. Workers claim disjoint ranges from one atomic queue, so uneven segments do not strand work
on fixed lanes. At most four lanes own separate field readers, position buffers
and heaps bounded by the requested page window. Each lane has an 8 MiB live scratch
allowance; scheduler admission accounts concurrent lanes within a 32 MiB operator
allowance for lane workspaces. Request-owned planning/statistics, seed scratch,
the global result heap and independently bounded read caches are separate. A
cancellation-aware blocking coordinator serializes adaptive producer calls and
winner admission, so query-owned providers and their stored request allocators
are never accessed concurrently. The global winner heap is reserved before workers
start, and its atomic score/document-ID cutoff lets other lanes prune
conservatively. Running lanes check for a stronger cutoff every 128 candidate
visits after flushing adaptive membership batches, without merging duplicate
winners. Cutoffs only improve and always represent at least `k + offset` unique
admitted hits. Completed provider masks also publish once through atomics, avoiding per-document coordinator
locks during posting navigation. Local counts and diagnostics merge only after a
range succeeds. Explicit worker cap exhaustion discards partial local results and
retries that entire range on the caller after all workers join. A retry uses only
retained global winners, so it recovers discarded local winners even when their
published cutoff pruned other ranges. Ordinary allocation errors propagate. Saturation runs required work inline. Every
exit joins or cancels outstanding tasks before releasing snapshot/statistics state
and scheduler leases. Small or unbound snapshots retain serial scoring.

Ranked native search-after requests admit only hits strictly following the cursor
into a heap of at most `k + offset`; matching hits before the cursor still contribute
to total-hit accounting. Supported leaves compose with the Boolean scorer instead
of requesting an all-hit window. Distributed BM25 field/term statistics are shared
read-only by workers and used for both scores and bounds. Complete distributed
contexts skip redundant local document-frequency requests, including cold
segment dictionary reads; only contexts requiring authoritative local fallback
load local frequencies. Constant-score positional alternatives do not load local
BM25 frequencies. Phrase nodes reuse the query's cached field average. An analyzed
match uses an override only when it covers every analyzed term, preserving the
previous partial-statistics fallback, zero-frequency segment fallback and corpus
frequency clamping. Existing distributed phrase constant-score semantics remain
unchanged. Locally scored exact phrases compose authenticated posting-block
frequency/norm ceilings through the earliest constituent block fence, skipping
noncompetitive position records. The first term bounds the number of starts and
supplies the scoring norm; later terms bound presence and range only, preserving
stacked duplicate positions. Saturated frequency metadata is widened
conservatively. Missing legacy metadata and signed subtrees retain conservative
bounds. Aggregations and fuzzy positional expansion retain
the established authoritative paths. Competitive pruning continues reporting
lower-bound totals.

The real Parquet/Iceberg regression now writes 70,003 rows, crossing the eager
native-directory budget, and checks broad includes/exclusions, signed/zero sparse
scores, positional conjunctions, sorted cursor pages and restart. Native unit
regressions additionally cover late rare-window seeks, deleted rows, forced spill,
legacy proof fallback, distributed and partial statistics, cursor ties and offsets,
threaded scoring, scheduler saturation, worker cap retries, provider/read errors,
cancellation and allocation-failure ownership. Representative 50-million-row
throughput and peak process RSS still require separate measurement.

### Qualification of competitive refinement and shared range scheduling

`origin/main` at `551b8b3895` is included through merge commit `a31e0cf0e3`,
without conflicts. A fresh fetch after qualification found no newer main commits.
The generated control source catalog matches the current ownership graph.
Qualification uses Zig 0.17.0.

- Debug: 67 sparse, 29 focused text/scorer and six API tests passed with no
  failures or leaks; one optimized-only benchmark skipped.
- ReleaseFast: 67 sparse, 397 bounded-reader, 30 focused text/scorer and six API
  tests passed with no failures or leaks (500 checks across these suites).
- The forced-deferred 10,000-row broad sparse regression returns nine exact
  winners with nine identity checks and zero translated rows/windows. Dense
  competitive candidates still amortize window translation. Rare postings,
  includes/exclusions, signed/zero scores, deleted rows and legacy proofs retain
  differential checks. Removing the complete-map proof and supplying a one-score
  allowance plus spill I/O exercises the legacy disk-spill path.
- An 8,192-row phrase regression preserves exact IDs/scores while decoding at
  most 3,072 position records, one 1,024-row impact range across three terms.
  Duplicate first-term starts can share later-term occurrences; the authenticated
  ceiling remains conservative. Negative/zero boosts and missing block metadata
  retain exact reference results. Saturated frequency tests cover legacy,
  byte-ID and packed metadata. The earlier selective-conjunction fixture still
  verifies one document and reads two position records in either clause order.
- A single 12,288-row query-bound segment splits into three ranges and uses
  parallel lanes after its bounded seed. Differential checks cover deleted rows,
  signed/zero boosts, ties and cursor offsets. A lane publishes a cutoff before
  global winner admission; discarding its local winners, scoring another range,
  and retrying serially recovers the exact reference ranking. Explicit cap
  exhaustion, scheduler saturation, provider/read errors and cancellation retain
  lease/ownership checks. Completed provider masks publish without stale batches.
- A failing cold read context proves complete distributed statistics and
  constant positional alternatives skip local frequency requests. Partial match
  overrides still request authoritative local statistics. Cursor regressions
  cover complete, partial, zero and overcount statistics and preserve standalone
  and nested f32 score arithmetic.
- The production Debug server builds. All four real-data E2Es passed against it
  in 185.03 seconds: independent Iceberg snapshots/schema IDs/partitions/deletes
  and restart; 70,003-row Parquet and Iceberg indexed includes/exclusions,
  signed/zero sparse ranking, positional conjunctions, sorting/cursor pages and
  persistence; quantized sparse score/ranking preservation. Iceberg remains in
  `e2e-full`; the entire suite was not run.
- Zig formatting, generated catalog equality and `git diff --check` passed.
  Representative 50-million-row cold/warm throughput and peak process RSS remain
  unmeasured; work-count regressions do not replace archive-scale benchmarks.

## Preflight sparse translation and share prepared text segments

The next refinement removes two avoidable setup costs and improves fragmented
snapshot planning without changing query syntax, artifact formats, ranking, or
result-size limits.

Sparse selection planning now counts effective 1,024-row physical directory
windows before native translation. It applies completed include/exclude
subtraction first and stops as soon as both directory and point budgets cannot
finish. Broad selections therefore enter deferred membership without reading and
discarding the first 64 native blocks. A selection with many directory windows
but at most 4,096 effective rows uses exact identity point seeks instead; a
one-row difference between broad include/exclude sets still gets an upfront
ordinal mask. Coordinates retain independent file/group/high-row windows,
including the final u32/u64 rows. The preflight uses bounded compressed scratch,
checks cancellation, and retains existing ordinary-error propagation and
whole-selection fallback rules.

Text node construction no longer consumes the first posting. Approximation
initializes each iterator at the useful target selected by the range and native
masks. Metadata bounds remain conservative until that iterator has a current
posting. First and long impact-range seeks use binary navigation; adjacent
sequential crossings retain a constant-time path. Consequently a late range
neither decodes document zero nor linearly re-walks its impact-ID prefix.

Query-bound remote scoring shares a query-scoped prepared segment cache when
ranges reuse segments or a fragmented snapshot needs metadata planning. Each
prepared entry owns the lowered Boolean tree, one reader per field, unique term
lookups, immutable decoded impact IDs, and its segment ceiling. Scoring clones
only mutable node/iterator state; repeated phrase positions keep independent
iterators, while their immutable navigation is shared. Preparation uses the
established lowering and f32 score grouping. Small single-range queries and
serial scoring without a prepass avoid this extra preparation.

At most four entries reside at once, each with an 8 MiB live allocation cap,
including navigation and backing-cache slabs. The aggregate preparation allowance
is 32 MiB plus fixed coordinator/entry records. It is separate from the existing
32 MiB concurrent scoring-lane allowance, request plans/statistics, seed/retry
scratch, result heaps, and independently bounded source caches. Concurrent field
caches reserve four hot slabs and one fill slab before publication, preserving
the existing 64 KiB read grain; mutable payload
and position decoder buffers remain lane-owned. Cached slab allocations use a
locked direct backing allocator rather than a shared mutable arena.

Cache misses are singleflight per segment. Construction and source reads run
outside the coordinator mutex. Cancellation-aware waiters borrow reference-counted
leases, and eviction destroys only idle readers after every borrowing iterator
has closed. Explicit preparation caps memoize an unavailable entry and retain
conservative planning plus authoritative per-range construction. Ordinary
allocation/storage errors propagate. Optional resize/remap cap denials cannot
misclassify a later backing allocation failure as optimization exhaustion.
Cached hits still check the current read
capability; prepared readers never survive the query or its pinned snapshot.

For snapshots with more than 16 segments, the shared scheduler admits up to four
metadata-planning lanes under the preparation allowance. Each lane claims segments
from one atomic queue. Scalar score ceilings remain in the query's segment plans
even when an idle prepared tree is evicted. Saturation runs required work inline,
and every error/cancellation path joins tasks before releasing plans, statistics,
cache entries, or scheduler leases. Healthy snapshots avoid the prepass and its
all-impact-table ceiling calculations.

Exact phrase segment ceilings now use authenticated first-term frequency/norm
limits with the complete phrase IDF. Later phrase terms constrain presence, while
start frequency remains bounded by the first term so stacked/repeated positions
cannot undercut the ceiling. Saturated frequency metadata widens to u32; missing
legacy metadata and signed/unsupported configurations retain conservative bounds.
Distributed constant-score phrase behavior is unchanged.

Work-count regressions cover the 64/65-window boundary, zero-directory-read broad
fallback, fragmented exact point seeks, subtraction, full-width coordinates,
late range initialization, one preparation across three ranges, borrowed impact
navigation, eviction with an active reader, explicit cache caps, ordinary
allocation failures, canceled singleflight waiters, and cached capability errors.
A threaded 20-segment positional fixture verifies parallel planning and exact
ranking: its authenticated phrase ceilings permit scoring one segment and pruning
19, with signed/zero boosts retaining differential checks. Representative
50-million-row cold/warm throughput and peak process RSS remain unmeasured.

### Qualification of shared segment preparation

A final fetch confirms `origin/main` at `551b8b3895` is already included;
merging it reports no additional commits or conflicts. Qualification uses
Zig 0.17.0.

- Debug: 68 sparse, 30 focused text/scorer and seven API tests passed (105
  checks), with no failures or leaks; one optimized-only benchmark skipped.
- ReleaseFast: 68 sparse, 398 bounded-reader, 31 focused text/scorer and seven
  API tests passed (504 checks), with no failures or leaks.
- The production Debug server builds. Generated control-catalog equality,
  Zig formatting and `git diff --check` pass.
- The 65-window broad-selection fixture invokes zero native directory callbacks.
  Exactly 64 windows retain eager translation; include-minus-exclude can reduce
  that broad membership to one row, and 65 fragmented rows use 65 exact point
  seeks instead of exhausting the directory budget. Full-width coordinates and
  allocation-failure cleanup retain explicit checks.
- A 12,288-row remote segment uses one immutable preparation for three ranges.
  Cloned iterators own zero impact-ID capacity and borrow the prepared arrays;
  a range beginning at document 8,192 initializes its decoder at that range,
  without first consuming document zero. Existing exact-score, signed/zero,
  cursor, live-cutoff and discarded-winner retry checks remain enabled.
- Threaded metadata planning admits scheduler tasks for a 20-segment phrase
  fixture; only its high-impact segment is scored and 19 are pruned. Active
  reader leases survive cache eviction. Explicit caps retain exact per-range
  fallback; exhaustive ordinary allocation failures, cached read-context errors
  and canceled singleflight waiters preserve ownership. A separate allocator
  regression verifies denied resize/remap probes cannot hide later backing
  allocation failures.
- All four real-data E2Es passed against that server in 177.97 seconds:
  independent Iceberg snapshots/schema IDs/partitions/deletes and restart;
  70,003-row Parquet and Iceberg indexed predicates, signed/zero sparse ranking,
  phrases, sorted cursor pages and persistence; quantized sparse score/ranking
  preservation. Iceberg remains in `e2e-full`; the entire suite was not run.

Representative 50-million-row cold/warm throughput and peak process RSS remain
unmeasured. No archive speedup is inferred from these work-count regressions or
the fixture's total runtime.

## Saturated bounds, concurrent fills, reusable summaries and broader scheduling

Saturated impact frequencies are now treated consistently as an open-ended
frequency bound. Scalar block ceilings, whole-term ceilings and precomputed
packed-frequency tables use the BM25 asymptote for the escape value. A mixed-field
`k=1` regression retains a later 100,000-frequency winner over an earlier
80,000-frequency document under both default and custom supported BM25 settings.
The previous block ceiling could discard that winner despite the correct segment
and phrase bounds.

Concurrent native caches now have four bounded fill flights alongside four hot
slabs. Independent cold blocks read concurrently; requests for the same block
wait for the active fill and recheck the hot cache. Backend I/O runs outside the
allocator and hot-cache locks. Query-bound waiters use cancellation-aware I/O
conditions, with a yielding fallback for sources without an I/O runtime. A failed
or canceled fill releases its flight without publishing partial bytes. Active
flights prevent pressure reclamation of their slabs. Cache budgets include all
eight slabs; prepared field readers reserve 512 KiB to retain the 64 KiB grain.
Reader adapters forward the I/O runtime and current read-context check through
cache layers. Threaded regressions check overlapping physical reads, one read for
a duplicate block, failed-fill retry, cancellation of a duplicate waiter and
bounded retained storage.

Immutable snapshots now retain scalar term summaries across query facades for
the exact same corpus. A summary records segment-local document frequency and an
IDF-independent TF ceiling, keyed by segment, exact field/term bytes, average
field length and BM25 configuration. The cache admits at most 4,096 entries and
256 KiB of owned key bytes, with bounded eviction. It stores no readers, query
capabilities, borrowed navigation or decoders. Hits check the current facade's
read authority, and a changed corpus starts a separate cache. Query IDF and boost
are applied when composing term and phrase ceilings.

Fragmented metadata planning now builds only summary-based clause trees. It does
not populate or churn the four-entry full-reader cache. Full prepared readers are
created only after a range survives its scalar segment ceiling. The 20-segment
positional fixture therefore prepares one full reader for its competitive
segment, rather than preparing all 20 before pruning 19. Subsequent queries reuse
all 60 cached term summaries, including different outer boosts. Metadata planning
continues to use bounded shared-scheduler lanes and conservative cap fallback.

Remote simple Boolean queries and default-boost standalone terms/matches now use
the shared bounded text scheduler ahead of their serial fast paths. The existing
simple clause lowering preserves score arithmetic; unsupported shapes and small
or unbound snapshots retain their established execution. Differential tests
compare real public dispatch against a serial facade over the same immutable
bytes, alongside the existing cursor, filter and signed-score coverage.

Native sparse DAAT scoring now supports disjoint document ranges on the shared
scheduler. Authenticated routing intervals are coalesced before partitioning, so
holes and rare postings do not schedule an archive-sized ordinal domain. Only
streams whose proved interval intersects a range are opened there. Legacy streams
without interval proofs retain a conservative domain. Small covered domains and
selective ordinal masks keep serial scoring.

Each sparse lane owns an independent fork of the exact pinned read transaction,
visibility/incarnation caches, decoder buffers and adaptive ordinal windows.
Initial immutable masks are borrowed from the coordinator rather than translated
again per lane. Adaptive provider callbacks are serialized, while physical page
reads and scoring proceed independently. A document and all of its contributions
belong to one range, retaining canonical signed f32 addition order. Lanes publish
monotone competitive cutoffs and merge bounded heaps. Up to four 16 MiB lanes
share a 64 MiB operator allowance, separate from request routing/results and the
existing serial fallback. Required work runs inline under scheduler saturation.
Explicit lane/page caps discard the entire parallel attempt and rerun the exact
serial/spill path without a discarded cutoff; ordinary errors propagate after
all tasks join. Regressions cover signed/zero weights, deletes, residual filters,
forced one-byte lane caps and provider errors.

These changes preserve query syntax, artifact formats, snapshot/cursor identity
and result-size limits. Representative archive-scale cold/warm throughput and
peak process RSS still require measurement; unit work counts and E2E runtime do
not establish an archive speedup.

Qualification for these refinements with Zig 0.17.0:

- Debug: 399 bounded-reader, 70 sparse, 31 focused text/scorer and seven API
  checks passed (507 total), with no failures or leaks. The two optimized-only
  benchmark runs were skipped.
- ReleaseFast: 400 bounded-reader, 70 sparse, 32 focused text/scorer and seven
  API checks passed (509 total), with no failures or leaks.
- Sparse range planning verifies coalesced coverage, skipped ordinal holes,
  complete include/exclude masks, the exclusive `2^32` endpoint, conservative
  legacy coverage and allocation-failure cleanup. Parallel differential checks
  retain exact score bits and IDs under signed/zero weights and forced caps.
- The production Debug server build, generated storage-catalog equality, Zig
  formatting and `git diff --check` passed.
- `origin/main` at `551b8b3895` is included; the latest fetch required no merge
  changes or conflict resolution.
- All four real-data E2Es passed against the rebuilt server in 178.68 seconds:
  independently written Iceberg snapshots/schema IDs/partitions/deletes/restart;
  70,003-row Parquet and Iceberg indexed conjunctions, includes/exclusions,
  signed/zero sparse ranking, positional filters, sorted cursor pages and
  persistence; quantized sparse score/ranking preservation. The entire
  `e2e-full` suite was not run; the Iceberg fixtures remain registered there.

## Persisted statistics and request-wide planning (#1058)

Status: implemented. This phase keeps the native engine, query syntax, scoring
semantics and snapshot/cursor contracts. Representative archive benchmarks remain
pending; work-count regressions demonstrate the eliminated work without claiming
throughput on the 50-million-row archive.

### Authenticated text statistics and segment summaries

New native text publications include an immutable global term-frequency B-tree
and a summary B-tree for each segment. Keys encode the field length, field and
term without delimiter ambiguity. Global values store u64 document frequencies;
segment values store document frequency, maximum frequency and minimum norm.
Existing authenticated native field headers supply field totals and average
lengths. Serving seeks only requested term pages, avoiding the cold dictionary
pass across every segment. Summary planning still visits the segment inventory,
but opens statistics pages rather than each segment's native dictionary.

The page trees reuse the existing authenticated page format, SHA-256 identities,
publication domains and upload attempts. Pages target 32 KiB with a 1 MiB hard
limit. Requests use current credentials, publication leases and cancellation;
shared corpus entries retain only immutable references and bounded scalar caches.
Warm cache hits still check request authority. Referenced missing, malformed or
cross-domain pages fail the request. Legacy publications without statistics keep
the exact dictionary fallback. An authenticated statistics-version fence prevents reusing old builds as new
statistics-bearing publications; optional fields preserve metadata compatibility.
The stored-source recipe stays stable, preserving legacy coverage attestation.

Builders stream sorted segment dictionary metadata without decoding postings.
Signed per-file deltas spill with a 4 MiB in-memory run target under the existing
1 GiB temporary disk cap. Publication working-set admission also bounds sorting,
merge buffers and concurrent runs. Incremental publications subtract replaced or removed file
contributions and add new contributions, retaining unchanged summaries. Global
updates apply sorted mutations in batches of at most 4096 entries or 2 MiB and
copy only touched B-tree paths. Initial builds stream the merged vocabulary into
the tree. Publication read/write budgets remain explicit; exceeding them fails
publication before its fenced commit, leaving the prior generation readable.

Raw envelopes exclude request scoring parameters. Readers derive conservative
BM25 ceilings using the request's average length, k1 and b. Frequency saturation
retains the asymptotic ceiling. Combining a segment's maximum frequency and
minimum norm may produce a looser bound than individual block pairs; it cannot
exclude a valid winner. Full-corpus deletion semantics and complete/partial
distributed overrides retain their established behavior.

Global and segment roots belong to the authenticated corpus/file manifests and
GC reachability graph. Durable collection traverses every page and retains roots
reachable only from a live reader's pinned publication. Releasing that reader
allows the obsolete pages to be reclaimed.

### Sparse active-stream routing

Parallel sparse search builds one immutable interval directory, sorted by stream
start and augmented with subtree maximum ends. Each task enumerates overlapping
streams and restores their original index order before scoring, preserving
signed f32 addition and ordinal ties. Unknown legacy bounds cover the whole
pinned domain. The exclusive 2^32 endpoint and existing mask semantics remain
supported.

Task planning and routing share a 4 MiB allocation cap. The directory uses one
24-byte record per stream; it never stores a ranges-times-streams matrix. Each
lane reuses decoder and index arrays sized to its maximum active overlap, instead
of allocating arrays for the entire archive inventory or rescanning that inventory
for each range. Existing per-lane and shared scheduler workspace limits still
apply. Explicit planning-cap exhaustion selects the exact serial/spill path
before any shared cutoff is published. Ordinary allocation failures propagate;
worker cancellation and errors join all tasks before releasing the directory.

The routing regression uses 8192 streams, returns the three canonical overlapping
indices, and visits fewer than 64 tree nodes. It also covers whole-domain legacy
bounds, the final u32 row and allocation-failure cleanup. Existing signed-score,
mask, provider-error and parallel fallback regressions exercise the serving path.

### Analyze and lower text queries once per request

An immutable request-owned query tree resolves analysis, deduplication, simple
Boolean flattening, minimum-should-match rules, optional grouping, normalization
and distributed-statistics choices before segment planning starts. Phrase groups
retain alternatives, repeated positions, slop and analyzed gaps. Metadata planning,
prepared readers and scoring lanes bind segment-local scoring constants, readers
and private mutable iterators to that tree.

The tree lives in the request statistics arena and never enters a snapshot cache.
Cached readers and scalar summaries continue to use query-bound read authority.
The regression lowers repeated match clauses across 20 segments, changes the
analyzer afterward, then proves segment binding retains the established streaming
score bits. Allocation-failure checks cover the lowered request's ownership.
Existing differential tests cover nested/mixed-field clauses, phrases, filters,
negative/zero boosts, cursors and complete/partial distributed statistics.

### Qualification

Focused Debug and ReleaseFast suites, incremental publication/GC integration,
the production build and real Parquet/Iceberg fixtures qualify this phase. Iceberg
remains registered in `e2e-full`. Statistics regressions prove cold reads avoid
native dictionary I/O, incremental updates preserve exact frequencies, sorted
runs spill, a one-term update rewrites less than 64 KiB of an 8000-term tree, and
obsolete statistics remain reachable until the last pinned reader releases them.

Archive-scale cold/warm throughput, fetched bytes, cache hit rates, planning and
lane peaks, and process RSS still need measurement on a representative fragmented
archive. No archive-scale speedup is asserted by these bounded regressions.

## Term-directed planning and adaptive statistics (#1063)

Status: implemented; qualification results are recorded below. This follows
#1058's persisted global frequencies, segment summaries and request lowering.

### Term-first hierarchy and lazy exact refinement

New statistics-version-2 publications include two authenticated B-trees: exact
`(field, term, immutable-segment-identity)` leaves and per-term hash-bucket
branches. Keys escape embedded zero bytes and terminate term identities without
ambiguity. Segment identity hashes the complete immutable artifact identity,
including its upload attempt; pinned readers resolve identities to current
segment ordinals rather than reusing previous generations' offsets.

A term has at most 256 nonempty branch records. Each branch stores an identity
range and a compact raw scoring envelope. The request prepares these records in
batched seeks with a bounded decoded-page cache, before starting worker lanes.
Its raw-envelope cache has a 4 MiB retained-data target, uses the admitted corpus
allocator and contains no native reader or mutable lane state. The initial
segment prepass composes coarse bounds from these immutable records without
opening per-segment statistics or dictionaries. Identity ranges are conservative
supersets; gaps never imply that a document matches. A rare term appearing in one
segment has a singleton range. Broad terms can cover most segments in their
buckets. Directory-version-2 branches also retain exact segment counts. For a
term with at most 4,096 segments, the request enumerates its exact identities
within a bounded read budget, then resolves them through a generation-owned
sorted identity/ordinal/offset table. Positive necessary Boolean clauses and
phrase groups intersect these sets; required alternatives union them. The
planner allocates and visits only the resulting candidates. Unknown sets,
optional clauses and prohibitions retain conservative full-inventory behavior.
Union overflow, legacy directories and exhausted budgets fall back for the
entire term, never to a truncated membership set.

Surviving segments refine their bounds with batched exact leaf lookups when a
competitive cutoff exists, before preparing dictionaries and postings. Nested
Boolean and phrase nodes use their established bound composition and positional
verification. Baselines, optional clauses, prohibitions, signed boosts and
unsupported scoring configurations retain conservative behavior. Exact posting
scores and their addition order are unchanged. Raw envelopes are independent of
BM25 parameters; parameter changes derive new ceilings from the same metadata.
The request's shared preparation coordinator singleflights exact segment-bound
refinement across scoring ranges in a separate four-entry scalar cache. Builds
use the already admitted lane scratch space; warm values recheck statistics and
range-reader authority. Bounds retain no borrowed readers or scratch allocations.

Incremental publication streams only changed segment contributions into a
bounded spill sorter, feeds both statistics builders from a single authenticated
summary cursor, updates exact leaves with sorted COW batches, and recomputes
each affected hash bucket once. Directory-version-2 leaf pages authenticate
bounded raw envelopes and record counts in both their headers and parent routing
entries. Range folding consumes whole covered subtree envelopes and reads only
boundary descendants; legacy or missing reductions descend to exact records.
Sparse changes use COW, while dense changes stream-merge sorted unique mutations
with the prior leaves. Initial construction streams sorted leaves and branch
aggregates. Empty changes preserve both root references. Unchanged file
summaries and native segments remain reusable, including when adding a directory
to a statistics-version-1 seed. Both tree roots belong to the corpus manifest and
the existing durable page-GC frontier, including reader-only pinned generations.
The page reference and reclamation graph remain unchanged. Trees without a
reducer keep their existing wire encoding. Generic edits without a reducer safely
drop summaries on modified paths. A directory-version-1 seed is rebuilt from
its authenticated segment summaries without rereading Parquet data.

Legacy publications retain exact dictionary/summary readers. Legacy 12-byte
summaries remain decodable. Missing optional directory roots choose the exact
path; referenced missing or malformed pages fail validation. An unusually long
legacy-accepted term that exceeds the escaped directory key limit disables the
optional directory for that publication. The same overflow in a query term
selects the exact native summary reader during competitive refinement instead
of failing the query. Exhausting the request's preparation
cache leaves an entire term unprepared and uses exact summaries for that term.
The current publication remains readable if a new build exceeds its budgets.

### Adaptive global-statistics publication

Small updates retain sorted, bounded COW mutations. Dense updates stream-merge
sorted unique signed deltas with the prior global-frequency cursor and build a
new ordered tree, avoiding individual old-count seeks. Counts use checked wide
arithmetic, negative results fail, and zero results are omitted. No vocabulary
or all-update array is retained in memory.

Automatic selection counts distinct nonzero signed deltas in a bounded replay
of the finished spill stream: at least `max(4096, prior_term_count / 4)` chooses
streaming merge. Repetition and cancellation cannot force a vocabulary rewrite.
Directory publication uses the same threshold against its prior leaf count.
Rewind reuses existing owned spill runs and their bounded merge heads without
retaining all changes or duplicating temporary files; leased streams cannot rewind.
Explicit strategy overrides support differential tests and profiling; the builder
exposes the selected strategy and point-seek count. Empty deltas retain the prior
root, including fully cancelling deltas. Existing publication admission,
cancellation, fencing, read/write budgets and the 1 GiB temporary disk limit apply
to both strategies.

### Compact scoring envelopes and batched reads

New summaries retain at most eight nondominated `(maximum frequency, minimum
length)` pairs from authenticated block metadata. Dominance is independent of
valid BM25 parameters. Overflow conservatively merges pairs using maximum
frequency and minimum length; it never discards coverage. Branch envelopes use
the same bounded union. Saturated frequencies retain the asymptotic ceiling,
invalid parameters disable finite bounds, and existing node-bound padding covers
score arithmetic rounding.

Global-frequency batches compact only cold terms and scatter results back into
original slots. Exact segment-summary batches likewise skip warm scalar results
and share decoded B-tree routing pages across requested terms. The existing
bounded generation-specific scalar cache and current-authority checks remain in
use. Request-owned raw branch envelopes are shared read-only across lanes and
released with the query lease.

Main's numeric-predicate fast path uses the same constrained scoring policy as
bitmap predicates and candidate producers. Streaming lowering and segment
binding share that single constraint flag, including boosted distributed leaves.
Filtered WAND uses the selected distributed term frequencies alongside its
matching document count and average length for both planning and scoring.
Differential tests use unequal local/global frequencies and compare exact scores
across numeric includes/exclusions, repeated match tokens and signed boosts.

Retained cursor source binding dispatches on the publication's source kind. Raw
Parquet preserves its object-version selection policy and validates the resolved
inventory against the publication. Only Iceberg uses a catalog snapshot ID; its
empty-publication sentinel retains current-snapshot discovery. Mismatched source
kinds return a catalog-generation error rather than accessing an inactive union.

### Remote fetch scheduling refinement

Remote range work now has a separate admission class from CPU kernels, joins,
partition scans and decode work. The defaults admit eight CPU tasks and up to
32 I/O tasks under the existing 64 MiB temporary-work allowance. I/O may reserve
at most 32 MiB and never more than half the configured shared allowance, leaving
workspace for CPU operators. CPU fan-out counts only occupied CPU slots, while
both classes charge the same available bytes. Required work retains its inline
fallback when admission or the I/O backend is unavailable; speculative work
yields. Native posting-range prefetch and parallel required posting reads, raw
Parquet range prefetch and Bloom lookahead use I/O admission. Mixed fetch/decode
lookahead remains CPU work.

Parquet range prefetch uses an eight-slot rolling queue rather than draining the
previous batch before submitting more work. Each reader also caps outstanding
range bytes at 32 MiB; the shared admission uses the existing workspace estimate of
twice the range length and can shrink that window further. Duplicate in-flight
versioned ranges share a slot. Oversized or unadmitted ranges do not prevent a
later small range from being tried. Scan pulls reap completed workers without
waiting for unrelated slow ranges. Required reads retain the versioned cache's
singleflight contract. Completion releases execution admission before joining;
close still cancels and joins all borrowers before metadata or providers are
freed. Resident physical and decoded caches retain their separate bounded
budgets; this is temporary-work admission, not a replacement buffer manager.

This follows the scheduling lesson in
[DuckDB's asynchronous I/O design](https://duckdb.org/2026/07/31/asynchronous-io):
remote request concurrency and CPU concurrency need independent controls, with
read-ahead constrained by memory. It uses the existing `std.Io.concurrent`
backend and keeps production Threaded defaults. See `lib/platform/EVENTED.md`
for Evented qualification. Rolling row-group lookahead and exact partial aggregation below compatible
joins are described below. Cross-query fairness beyond bounded admission and
typed nested Parquet/JSON path execution remain separate work. No latency-adaptive tuning or
archive-scale speedup is claimed by this refinement.

### Qualification

With Zig 0.17.0 after merging origin/main at `c0562216e1` and adding the I/O
refinement, Debug qualification passed all 50 focused tests (one optimized-only
benchmark skipped), all 314 lake integration tests and the production server
build. The five standalone scheduling tests passed in both Debug and ReleaseFast.
The shared integration owner includes the blocked-batch, duplicate range,
small-budget and cancellation regression. Final qualification reported no
failures or allocator leaks. Zig formatting, Python lint/format checks, the full
license/boundary gate and `git diff --check` also pass.

Before the I/O refinement, after the earlier `083142678c` main merge, ReleaseFast
passed all 51 focused tests, including the collector benchmark. All 16 page-tree
tests also passed independently, including generic legacy encoding, integrity,
snapshot retention, allocation failure and recovery.

The branch then merged origin/main through `b7e4f43a30`, including its Lite read
navigation and batch-scratch changes, without conflicts. The combined production
Debug server rebuilt successfully and all six real-data E2Es passed again in
253.16 seconds. License headers and the Apache dependency boundary also pass
after that merge. The focused/integration and optimized scheduling results above
were collected with the same I/O refinement before this additional main merge.

Regressions cover scalar-bound tightening, conservative envelope coarsening over
custom BM25 configurations, saturation, dense-merge/COW equivalence, zero dense
point seeks, changed/removed directory contributions, unchanged-root reuse, rare
planning across 10,000 identities, reordered generation identities, missing pages,
allocation failure and bounded-cache fallback. Refinement regressions also
verify one plan among 10,000 segments, necessary-clause candidate algebra,
subtree reuse for a 10,000-record bucket, COW/dense directory equivalence,
repeated spilling contributions selecting COW from one distinct delta, and
shared exact bounds rejecting revoked authority. Publication and GC integration
cover version-2 roots and directory retention/reclamation.

Real-data fixtures now include 17-file Parquet and Iceberg corpora, forcing the
fragmented parallel text path. They compare nested Boolean and phrase results,
zero/negative scores, repeated requests and cold restart. They also cover the
oversized-term competitive-refinement fallback, limit-1 ranking, broad
alternatives and prohibited-only queries. All six real-data E2Es passed in one
fresh run after the I/O refinement against the rebuilt production server
(253.11 seconds), with the disk safety floor enabled. The Parquet and Iceberg
pagination fixtures exercise retained snapshot tokens with forward and backward cursors. These are
the five Iceberg/Parquet SQL fixtures and the native sparse metadata score/ranking
regression. Iceberg remains in `e2e-full`.

Representative archive benchmarks remain pending. Qualification uses work-count
assertions; it makes no archive-scale throughput claim. Measure cold/warm latency,
metadata/posting bytes, provider requests, publication amplification, temporary
disk, planning/lane peaks and process RSS on a representative archive before
changing strategy thresholds or envelope/bucket sizes.

### Lazy admission, rolling scan lookahead, and exact join partials

New text file manifests carry versioned admission descriptors authenticated by
that manifest's SHA-256 identity: source size, section offsets, document counts,
stored-identity navigation header, and per-field scoring statistics. Corpus
assembly builds native readers from these descriptors without opening each
segment directory. The full directory loads only when a selected segment is
accessed. Publication owns the descriptors; query capabilities own provider
reads, cancellation and deadlines. Failed directory loads leave no retained
partial arena, and another authorized query can retry. Waiters check their own
capability without holding a spin mutex across provider I/O. Canonically sorted
field names and a section-kind bitset bound structural validation to linear work.

The range source must independently authenticate every payload. Lake readers
verify SHA-256 packs and blocks; descriptor admission explicitly preserves that
contract instead of falling back to a whole-section CRC scan. Legacy manifests
keep eager admission until maintenance upgrades their descriptors. Unchanged
native segments can upgrade without rescanning Parquet. File manifests and
corpus metadata retain their existing byte limits, and every query still uses
full-corpus statistics and the pinned physical identity mapping. Descriptor
copies use the bounded corpus allocator without charging unvisited directory
payloads to the artifact read budget; legacy directory reads retain that budget.
Required reads discover cold native directories; optional prefetch never performs that
discovery under its queue mutex.

Page-header discovery runs on I/O workers, reserving the bounded possible page
payload before submission. Complete pages already contained in a header probe
need no second provider GET. Four rolling row-group jobs warm first required
pages while the consumer advances; completed jobs release admission and permit
new submissions. Temporary input, decode and structural workspace is reserved
and worker allocation is capped. Resident raw and decoded caches keep their
independent limits. Saturation leaves required reads available through the
existing synchronous path.

Prefetch jobs own their immutable object and version strings. File cleanup
cancels borrowed current-file work while preserving next-file footer jobs. A
required next-file open joins that same parsed-footer flight. Scan close cancels
and joins every remaining job before releasing providers or metadata. This
ownership distinction is covered by a gated provider and a real Parquet footer,
including the assertion that handoff performs one provider request.

Aggregate reuse now has a separate group-constant residual proof. Direct group
keys, constants, parameters, comparisons, boolean operations and null tests can
serve WHERE and aggregate FILTER from unfiltered materialized state. FILTER
rejection imports an empty reducer for that slot while preserving the group;
COUNT returns zero and SUM remains SQL NULL. Row-dependent expressions, casts,
functions and unproven shapes keep row execution. HAVING, ordering and delivery
continue above reduction, and persisted recipe identity stays unfiltered.

A relational partial provider aggregates both sides before a proven direct-key
inner equijoin. Its grouping keys contain the join keys and required output
keys. COUNT state carries input multiplicity; selected reducer states cross the
join with checked multiplication rather than expanding duplicate row pairs.
COUNT, integer SUM, MIN/MAX and boolean reducers are supported. Both grouping
and the compact join build use existing typed, byte-bounded, spillable state.
The provider follows the row executor's cost-based build orientation and installs
its bounded dynamic filter before pulling any probe rows. VALUES cardinality is
the checked sum of its arms, so a small constant build remains small on either
side of a physical join.
Floating SUM/AVG, DISTINCT, outer joins, correlated joins, residual ON predicates
and expressions without an equivalence proof retain ordinary execution. The
fanout regression compares exact results and admits 144 pairs from 24 inputs
under a 64-row execution budget, where the expanding fallback exceeds the limit.
Real Parquet and independent PyIceberg E2Es cover group-key predicates, FILTER,
join multiplicity, snapshot changes and restart.

Cross-query fairness beyond bounded admission, latency-adaptive queue sizing,
typed nested Parquet/JSON paths, and broader algebraic join rewrites remain
future work. Archive-scale cold/warm latency and RSS measurements are still
required before claiming an end-to-end speedup or changing strategy thresholds.


Qualification after merging main through `121699433e` (Zig 0.17.0):

- Debug: all 319 lake integration tests pass (130 shared lake/SQL and 189 API),
  with no allocator leaks. Coverage includes zero-I/O admission, authorized
  retry, OOM unwinding, preserved footer flights, both dynamic-filter probe
  orientations, and an explicit disk-write assertion for 4,096 unique join
  keys with the 2 MiB retained-state setting.
- All 50 focused ranking tests pass; the optimized-only benchmark is skipped
  in Debug. The three existing native column-batch/deferred-error join
  regressions pass in both owner paths. All five standalone scheduler tests
  pass in Debug and ReleaseFast.
- The final Debug production server builds successfully. All seven real
  Parquet/Iceberg/sparse and incremental-generation E2Es pass in a fresh run
  against that rebuilt server (254.33 seconds), after the admission read-budget
  correction. They exercise filtering, ordering/cursors, aggregate FILTER,
  join multiplicity, independent Iceberg commits/deletes, and restart.
- Canonical license headers, the Apache dependency boundary (2,019 source
  files), Zig formatting, Python lint/format, and diff checks pass.

### Evictable directories, sparse capabilities, and costed typed join partials

Admitted native posting directories now belong to the shared decoded metadata
cache, keyed by artifact identity, metadata type and authorization scope. The
corpus owner retains the immutable reference and admitted byte count. Every
required read pins its parsed directory only for the read, including all parallel
range workers. Speculative queues pin resident directories until their tasks are
joined or canceled; speculation never discovers a cold directory. Cold loads
use the decoded cache's bounded admission and retryable singleflight. Pinned
pressure can return an uncached lease, while released directories can be evicted
independently of the corpus. Current cancellation and deadline checks apply to
cache hits as well as provider reads. Uncached and legacy readers preserve their
existing source authentication contract.

The remote query path uses an explicit read capability with an independent
checkpoint and I/O runtime. Its facade pins and borrows the immutable segment
inventory and field totals, rather than copying and retaining every entry.
Only selected payload readers acquire stable, query-owned source/decoder entries
in a synchronized sparse map. Immutable section coverage checks do not bind
readers. Metadata candidates keep their original segment ordinals and document
offsets; scoring caches still belong to the exact immutable corpus. Closing a
query quiesces its bound sources before releasing its capability. The older
context-only binder retains eager validation for providers without an independent
capability checkpoint.

A semantic proof alone no longer selects join partial aggregation. A bounded
sample of at most 128 build rows must show at least 25 percent reduction on the
complete partial grouping key. The sample is replayed exactly once into either
executor, with native pages borrowed until consumed. Probe input is not sampled:
the executor still seals and installs its build filter before advancing the
probe. This prevents unique-key joins from paying for two unnecessary reducers.
The initial sample is a cost hint, not an equality or cardinality assertion;
later distributions can differ without affecting correctness.

Selected partial joins consume native batches and encoded expression columns
through the existing typed grouping kernels. Compact join payloads use native
count, signed integer-sum limbs, boolean and extreme-value columns. Compact
pairs read only their required reducer slots; they do not decode and re-encode
versioned aggregate strings. Typed partial states merge directly into the final
group columns. Versioned aggregate cells remain the interchange contract at
backend and spill boundaries. Build overflow transfers the retained prefix and
remaining compact groups to the existing sequential Grace partition join, with
repartitioning and its bounded skew fallback, rather than starting an unpartitioned
disk-chain join.

Cold statistics batches use up to four independently admitted I/O lanes. Each
lane owns its routing-page cache and bounded decode workspace, reusing shared
verified-artifact singleflight for overlapping physical reads. Lanes write
disjoint slots in original term order and share one synchronized 64 MiB read
budget. All submitted work is joined or canceled before output or reader
capabilities leave scope; scheduler saturation uses inline required work.

Regression coverage includes independent directory eviction under a 16 KiB
corpus heap, reader leases under pinned pressure, expired warm reads, zero
bindings for an unvisited 32-segment inventory, replay of 12,000 unique join keys,
NULL and FILTER semantics, signed wide SUM intermediates, forced partial-state
spilling, and serial/parallel statistics equivalence. The independent Parquet
E2E also compares unique-key and repeated-key aggregate joins with their expected
results and ordinary join fallback. Archive-scale latency, provider traffic and
RSS qualification remains a separate benchmark requirement.

Broad E2E qualification also covers the generic native consumers that sit
outside text scoring. Sort coverage, physical sort bounds, membership filters,
identity/ordinal conversion and filtered statistics acquire the selected
query-bound segment before reading payloads. Counts, offsets and immutable
section inventories still use the borrowed physical metadata.

Native aggregate publication and scalar binding now share canonical column type
identities, including default integer/double widths and explicit numeric widths.
This prevents a published SUM or COUNT(column) recipe from missing its eligible
SQL consumer because one side omitted the input identity. Existing untyped
recipes remain conservative scan fallbacks until republished; incompatible
states are not relabeled or merged. The recipe regression compares native
publication against bound SQL across eight scalar identity shapes.

The independent artifact-integrity E2E stops publication/GC before inspecting
all matching aggregate cohorts and makes both RAM and persistent caches cold
before testing damaged source objects. Valid cached immutable bytes are not
mistaken for scan fallback, and restoration does not resurrect collected old
artifacts. Ordered and timestamp DDL fixtures retry only the explicit idle, retryable 53300 response
introduced by main's relation-catalog publication fence.


Qualification after merging main through `7d6b35000d` (Zig 0.17.0, Debug):

- All 323 lake integration tests pass: 132 shared lake/SQL and 191 API, with
  no allocator leaks. The new scalar-identity comparison and actual native
  Parquet aggregate consumer both pass; consumer selection uses recipe identity
  rather than declaration position.
- All 50 focused ranking tests pass with no leaks. The optimized-only ranking
  benchmark is skipped in Debug.
- The Debug production server builds. All 14 selected real-data E2Es pass across
  the four Parquet page-version/dictionary variants and the ten text, timestamp,
  Iceberg, sparse and incremental-generation cases. These include cold aggregate
  integrity, native ordering/highlights/cursors, both typed partial and unique-key
  joins, pgwire streaming, independently written Iceberg deletes, and restart.
- Sequential paired HTTP/Parquet checks use 12,000 input rows, three alternating
  automatic/fallback pairs, exact COUNT/SUM/MIN expectations and fresh servers.
  Unique-key median: 0.187 s automatic versus 0.192 s forced row execution.
  With 32 repeated rows per key (375 groups, 384,000 logical pairs), median:
  0.079 s typed partial execution versus 1.477 s forced row execution.
  These are small Debug fixtures; they do not establish archive-scale throughput
  or RSS, and strategy thresholds remain cost hints.
- Canonical license headers, the Apache dependency boundary (2,019 source files),
  Zig formatting across all 38 changed Zig files, Python lint/format and diff
  checks pass.

### Byte-bounded partial delivery and incremental manifest admission

Partial join delivery retains packed typed inputs and a reducer-slot reader per
matched compact pair. It no longer expands all reducers for every pair in a
row-count-only page. Pages stop at the smaller of the SQL page byte allowance
and one sixteenth of retained memory; one large pair may cross that boundary to
ensure progress. The consumer materializes reducers in a separate arena reset
for each partial, borrows existing typed partial arrays when possible, and copies
states only when aggregate FILTER needs to replace a slot. Checked signed SUM
multiplicity, NULL behavior, and versioned durable aggregate cells are unchanged.
A real independently written Parquet regression uses 8,192 rows, 128 SUM columns,
multiple row groups and a duplicate-key join. Every result is checked against
Python integer arithmetic and ordinary join execution.

Grace partition selection now estimates compact build volume from observed
packed bytes and group cardinality instead of passing zero. Resident cardinality
is exact; an externally grouped input supplies a conservative input-row bound.
Partition fanout retains the existing cap of sixteen and bounded skew fallback.
Transferring the resident build prefix reclaims materialization scratch after
each row. Variable-sized grouping keys and extrema contribute to observed widths.

Both serial and parallel Boolean segment planning use immutable physical
metadata. Coarse hierarchy bounds and exact leaf refinements run before acquiring
query-bound segment readers. A missing metadata summary binds only the reader
needed for the legacy payload fallback. Surviving scoring work still binds its
current reader capability before opening payloads. Tests cover sparse candidates,
10,000 broad candidates, parallel planning, and exact-bound pruning with zero
query reader bindings, including current-authority checks on cached refinements.

Text source acquisition caches the authenticated root directory independently
from its file inventory. A ready physical corpus skips inventory expansion.
Cold corpus admission borrows independently cached file manifests on up to four
shared I/O lanes, preserving publication order and joining workers on errors or
cancellation. Unchanged file manifests can reuse their parsed values across
publications. Directory decoding finishes before child decode acquisition, which
avoids nested exclusive decode admission. Manifest leases remain pinned until
the corpus has copied all retained metadata; cache eviction cannot invalidate a
borrowed file descriptor.

Serving admits at most 64 MiB of pinned decoded manifest capacity and uses a
separate 64 MiB ceiling for inventory assembly. Active decode work remains under
the shared decoded-cache admission budget, while physical corpus metadata stays
under its existing process budget. Legacy/maintenance root expansion now bounds
actual parse allocations by 64 MiB instead of rejecting a sum of more than
32 MiB of encoded manifests. Per-artifact authentication and 4 MiB wire limits,
publication binding and file/segment count limits continue to apply. Warm corpus
admission is lazy; a cold corpus still requires the complete file inventory for
its global statistics and physical row identities. Skipping unrelated manifests
on a cold query would require a persisted global inventory/statistics contract.


Qualification for byte-bounded delivery and manifest admission (Zig 0.17.0,
Debug, main through `7d6b35000d`):

- All 323 lake integration tests pass after the final source rebuild, with no
  allocator leaks. This includes forced compact join spilling, allocation-failure
  unwinding, and independent manifest reuse, warm admission and expired reads.
- All 50 focused ranking tests pass with no leaks; one optimized-only benchmark
  is skipped in Debug. The broad planning and exact-pruning regression verifies
  zero bindings across a 10,000-segment inventory.
- The production server builds from the final source. All 15 selected real-data
  E2Es pass across the final affected runs: the 128-SUM wide join, four Parquet
  page-version/dictionary attachment variants, two native ordered index variants,
  remote text/highlights, two signed timestamp variants, incremental generations,
  sparse ranking, independent Iceberg commits/deletes/restart, and fragmented
  hierarchy ranking for both Parquet and Iceberg.
- The initial broad E2E run exposed the ordered fixture's missing DDL-publication
  retry and one timestamp publication timeout. Ordered and timestamp fixtures
  now share the same explicit idle/retryable 53300 policy. Both ordered and both
  timestamp cases pass together against the final server; SQL query failures
  remain immediate assertions.
- Zig formatting, Python lint/format, license headers, the authored embedded
  production import boundary, and diff checks pass. Archive-scale latency,
  traffic and RSS measurements remain pending.

### Shared cold admission, paged serving inventories and broad range planning

Cold native text admission now installs a process-owned flight before expanding
or validating the inventory. Same-key requests wait under their own deadline,
cancellation and authority, then acquire the resident corpus. Canceling a waiter
does not cancel construction; a failed leader permits a waiter to retry under
its own capability. At most sixteen distinct cold flights run concurrently.
Inventory assembly and paged-reader scratch share one 64 MiB process budget,
separate from the retained corpus budget. Admission pressure returns the existing
retryable cache-busy result and releases both the flight and temporary storage.

New publications include an optional authenticated serving-inventory page-tree
root. This packs the exact file descriptors used by maintenance into ordinary
32 KiB pages, with descriptor records chunked at 16 KiB. One encoded descriptor
is assembled at a time, under an 8 MiB scratch cap; parsed descriptors enter the
shared admission budget. Each assembled descriptor must match its maintenance
manifest's length and SHA-256 checksum. Consequently serving and GC cannot
silently disagree about the segment, statistics or physical-row children.
Cold serving reads these pages instead of opening one object per source file.
The existing manifests remain independently reusable maintenance and GC units.
Old publications without a serving inventory use their previous manifest path;
an exact maintenance refresh adds the new optional root. Reader-safe collection
marks serving pages for current publications and leased older publications.

This removes per-file object request overhead, rather than making all cold
metadata work independent of archive size. Exact global statistics, physical
row identities and immutable reader admissions still require the complete
compact inventory. Publication currently streams the serving tree in full;
it does not yet apply COW deltas to that tree. Content-addressed page verification,
existing per-descriptor limits, and request authority checks remain mandatory.

Broad text planning can now route by immutable segment identity and fold the
term directory's authenticated interior score envelopes over an identity range.
A query-owned page cache retains at most sixteen pages / 512 KiB. Optional range
reads have a 64 MiB budget; exhaustion returns an unknown infinite ceiling,
never a partial bound. Corruption and authority failures still fail the query.
The same lowered Boolean/phrase expression composes range and leaf bounds.

A best-first heap retains at most 4,096 pending ranges and emits at most 64
segment plans per batch. Scoring retains the shared scheduler, bounded prepared
reader cache, exact leaf refinement and deterministic global document offsets.
A competitive cutoff can discard an entire unseen range before query reader
binding or posting access. Strict score comparisons preserve ties across the
identity routing order; signed boosts retain conservative infinite bounds.
Batch workspace is reclaimed between batches, independently of a caller's arena.
Sparse exact term inventories retain direct candidate planning. Broad exact
inventories covering at least half of a fragmented corpus use range planning,
as do unknown broad memberships. Legacy statistics providers keep their previous
planner.

Regression coverage includes concurrent real-Parquet cold acquisition with a
canceled waiter, process-budget exhaustion, serving with a missing maintenance
manifest, reader-leased inventory GC, authenticated interior range folding and
optional-budget fallback, a 10,000-segment metadata-only pruning case, and real
postings differential checks for signed scores, ties, filters and pagination.
Archive-scale latency, provider traffic and peak RSS still need measurement.

Qualification for shared admission, paged inventory and range planning
(Zig 0.17.0, Debug, main merged through `22eebec31a`):

- All 324 lake integration tests pass (132 shared and 192 API), with no leaks.
  This includes independent canceled waiters and canceled-leader recovery,
  shared admission exhaustion, legacy manifest reuse, missing-maintenance-object
  serving, manifest/inventory consistency checks and reader-leased inventory GC.
- All 52 focused ranking tests pass, with no leaks; the optimized-only heap
  benchmark is skipped in Debug. Differential real-postings checks preserve
  signed scores, ties, exclusions and cursor pages. The synthetic statistics
  provider uses representable native field norms, matching stored quantization.
- The production server builds after the clean merge of main. All six selected
  public E2Es pass on that binary: fragmented text and restart for both Parquet
  and Iceberg, independently written Iceberg commits/schema IDs/deletes, remote
  text/highlights/cursors, incremental generations, and main's 4,000-table
  startup/publication regression.
- Formatting and license checks pass for all 39 Zig files changed against main.
  The embedded production boundary (816 sources), Apache product boundary
  (2,019 sources), and diff checks pass.


### Compact serving routes, incremental inventory, and fused spill reduction

The follow-up review found that the dedicated `COUNT(*)` consumer assumed
serialized aggregate cells even when an exact join provider returned an
in-process state reader. All partial consumers now share representation and
signature validation. Plain COUNT, multiple reducers, and durable aggregate
cells use the same contract; typed readers and serialized cells cannot be
mixed in one partial. Resident and forced-spill joins cover plain COUNT,
including empty matches and the public SQL endpoint over real Parquet input.

New publications use serving inventory version 2. Keys are the SHA-256 identity
of an immutable file manifest followed by a chunk offset, rather than the
file's current ordinal. File insertion, removal, replacement, and inventory
reordering therefore do not rename unrelated keys. Compatible unchanged file
manifests retain their original scoped artifact references. Publication applies
only removed and added descriptor chunks to copy-on-write page trees, while a
first publication bulk-builds sorted trees. Maintenance continues to authenticate
complete descriptors against the original manifest digest. Version 1 inventories
and independently cached legacy manifests remain readable.

A second authenticated tree contains compact scoring routes: file identity,
segment references, physical segment length, document count, field collection
statistics, section-presence masks, and summary references. Cold serving reads
these routes instead of all section descriptors and physical-row maps. Physical
snapshot entries retain compact scoring metadata; selected query readers load
their detailed file manifest through the bounded, credential-scoped metadata
cache and validate it against their route before binding native sections.
Provider capabilities remain query-owned. Descriptor lookup belongs to the
pinned generation, so a shared physical reader never substitutes an earlier
generation's manifest reference after GC. A failed or canceled descriptor load
installs no query reader. Metadata-only text-field and ordinal-coverage checks
use the section mask without acquiring detailed descriptors.

Physical-to-native and native-to-physical lookup similarly admit only selected
file manifests. A query retains at most four manifest leases alongside its
existing bounded bitmap slots; file identity and live row count are checked
before binary-search/rank conversion. Delete holes continue to use authenticated
physical block bitmaps. GC marks both serving trees, all reused manifest objects,
and their children while publication or reader leases retain the generation.

Spilled exact aggregate joins now feed borrowed partition pairs directly into
bounded typed group columns. They no longer clone packed pair payloads into
provider result pages for a subsequent reduction. The fused reducer retains the
existing checked multiplicity and wide integer SUM rules, null and extreme-value
semantics, cancellation checkpoints, and bounded grouping spill fallback.
Only reduced partials cross the join/group consumer boundary when output keys
omit join keys. When every join key survives, compact pairs already identify
distinct final groups; a structural proof keeps them streaming and avoids a
redundant hash reduction. Durable spill frames still use the versioned aggregate
interchange format.

This shape still retains compact metadata proportional to the number of files,
segments, and indexed fields. Full descriptor admission is selective; the
compact corpus inventory is not a sublinear routing structure. Incremental
publication still enumerates file identities to find the changed set, and
maintenance may expand complete manifests. The page-reuse regression measures
changed-path writes and reader retention; archive-scale cold/warm latency,
provider bytes, RSS, and skewed-join measurements remain separate qualification.

Qualification after merging `origin/main` through `2afbdd306c` (Zig 0.17.0,
Debug):

- All 328 lake integration tests pass (135 shared SQL/lake, 193 API), with no
  allocator leaks. The 52 focused ranking tests pass; the optimized-only
  benchmark is skipped in Debug. The production `antfly` CLI builds.
- The original public VALUES COUNT join returns HTTP 200 with `4`; COUNT/SUM
  and the residual row execution reference agree.
- The 2,048-descriptor COW regression writes 80 initial pages and 12 pages
  after one descriptor replacement, verifies the old reader, and performs no
  writes for an unchanged publication.
- Formatting, Python lint, license headers, the Apache product dependency
  boundary (2,022 source files), and diff checks pass.
- All 11 selected public E2Es pass across the final affected runs: independent
  Iceberg commits/schema IDs/deletes/restart, fragmented text hierarchy in both
  formats, remote text/highlights/cursors, incremental generations, the wide
  typed join and plain COUNT regression, both ordered-index hydration modes,
  both signed timestamp modes, and metadata predicates above 100,000 rows.
- The 100,003-row predicate fixture exceeded its former 300-second publication
  setup deadline twice, including an isolated run. Samples identified the
  unchanged ordered-index spill and reverse-coordinate publication paths. With
  a bounded 600-second setup window, the isolated full fixture passes in
  419 seconds. Its row count and predicate/restart assertions are unchanged;
  the test does not establish a bulk-publication latency improvement.


### Per-segment cold admission, balanced bulk runs, and selective file routing

Query facades now singleflight each selected segment independently. The snapshot
mutex protects the ready map and flight publication; provider binding, manifest
navigation, failed-constructor cleanup, and worker joins happen outside it.
Waiters retain the flight and recheck current authority after completion. Teardown
fences new bindings, waits for constructors and their cleanup, then joins bound
source workers before releasing the read capability. A locked allocator protects
concurrent decoder construction without serializing provider I/O.

Ordered-index cohorts retain balanced logical merge levels in reusable 64 KiB
extents of one anonymous spill file per cohort. Extent directories scale with
physical disk quota; tail truncation returns that quota immediately. Retained logical runs no longer
consume one open descriptor each, so eight cohorts can retain the normal 32-run
merge directory within the existing 64-file cap. Encoded blocks write directly into
extents without an intermediate run copy; merges retain the existing typed codec, cancellation, compression, and
bounded parallel scheduler. Initial publication builds reverse-coordinate trees
from the file-slot/row scan order using the shared sorted page builder, avoiding
a second external sort. Incremental and legacy publications retain the sorted
reverse-tree fallback.

New text publications persist global document/field-length totals and a
copy-on-write segment-to-file tree beside compact file routes. A bounded necessary
term proof chooses a conservative set of file partitions before cache admission.
Exact terms, analyzed matches, exact term phrases, and supported Boolean clauses
can narrow the set; unknown clauses, broad sets, and exhausted optional planning
budgets use full admission. Negative clauses never establish a positive
restriction. Selected partitions seek their immutable digest prefixes in the
compact route tree rather than decoding all file routes. Cache keys distinguish
full and selected corpora and their file sets. Selected snapshots retain global
BM25 document counts, average lengths, and term frequencies, while local ordinals
and predicate membership cover only the selected files. Sealing each publication
creates private scoring state even when its physical file subset is unchanged;
reader sharing cannot carry cached term frequencies across corpus generations.
Single-segment search also uses global term frequencies for sealed collections;
its ordinary local-reader shortcut cannot substitute subset statistics. Highlights
reuse the same query selection. WAL-overlay composition keeps full admission.

These optional root fields preserve reading of existing publications; the next
rebuild upgrades their routing contract. Garbage collection traverses the new
segment-to-file tree under the existing reader-safe publication leases. The root
still contains a flat manifest-reference list: root decoding and locating selected
references remain proportional to file count, while route decoding and physical
segment admission scale with selected files. Fuzzy/pattern queries and broad
necessary sets retain the full-inventory path. Archive-scale latency, RSS, and
provider request-count qualification remain separate from deterministic regression
coverage and fixture timings.


Qualification after merging `origin/main` through `77a276cd53` (Zig 0.17.0,
Debug):

- All 332 lake integration tests pass (137 shared SQL/lake, 195 API), with no
  allocator leaks. All 52 focused ranking tests and 30 spill tests pass; the
  optimized-only ranking benchmark is skipped in Debug. The production CLI builds.
- All 11 selected real Parquet/Iceberg E2Es pass in 193.72 seconds, including
  independent Iceberg commits/schema IDs/deletes/restart, fragmented text hierarchy
  in both formats, global scoring/filter/highlight/cursor behavior, incremental
  generation and full/selected scoring transitions, both ordered hydration and
  signed timestamp modes, wide typed joins, and indexed metadata predicates.
- The final 100,003-row predicate fixture takes 121.33 seconds versus the earlier
  419-second qualification. This measures the complete fixture and includes
  main's changes; it is not an isolated spill benchmark or archive-scale claim.
- The deterministic shared-run regression writes 2,163,648 bytes versus 2,421,065
  for legacy runs. Cancellation, allocation failures, physical quota reclamation,
  and eight-cohort descriptor admission also pass.
- The native subset scoring oracle compares actual search scores with the full
  corpus, including a single visible segment. Exhaustive allocation failures
  verify owned global field names, retained older generations, private scoring
  caches, and transitions back to the complete physical collection.
- Formatting, Python syntax, license headers, both source-boundary audits, and
  diff checks pass. A final fetch confirms the merged main revision is current.

### Paged maintenance and precise segment admission

The follow-up to the shared snapshot lifecycle review merges main through
`84a0080999`. Closing an ordinary physical snapshot pin now releases that pin
without fencing the writer's shared snapshot. Query-owned read facades still
fence new bindings and join outstanding source work before releasing their
capabilities. Final snapshot destruction quiesces owned sources in both cases.
The regression covers concurrent pins and subsequent scoring without a writer
publication between queries.

Text corpus inventory version 3 stores a compact file catalog and exact segment
routes in authenticated copy-on-write page trees. The root carries the file
count and tree roots instead of an archive-sized manifest array. File identity
lookup seeks directly to its manifest reference. Snapshot refresh compares the
compact file states with the provider's pinned inventory, loads full maintenance
manifests only for changed or removed files, and applies their statistics,
collection totals, manifest routes and segment routes as deltas. Provider
inventory comparison remains proportional to the number of physical files;
unchanged files require no detailed manifest decode. Older inventories retain
compatibility readers and upgrade on their next publication.

Necessary-term selection retains exact immutable segment IDs. It no longer
widens a selective segment proof to every native segment in a Parquet file.
Selected segment routes contain their original file ordinal intervals and the
physical block references overlapping those intervals. Mapping predicates,
hydrated rows and native ordinals preserves gaps between selected segments and
deleted physical rows. Collection N, global DF and field-length totals still
belong to the complete pinned snapshot, so pruning does not change BM25 scores.
Optional proof budgets continue to fall back to full inventory admission.

Each segment's detailed admission descriptor is an independent immutable
artifact. Physical row metadata is independently admitted from section
navigation, so binding or hydrating a winner never needs the file's combined
maintenance manifest. The corpus cache routes shared physical readers through
the current generation's descriptor references. Reader-safe GC marks both paged
directories, expands the compact file catalog within its read budget, and retains
segment and physical descriptors through their maintenance manifests.

Resident join/group reduction now consumes borrowed hash matches directly into
the same typed reducer used by partitioned joins. Compact pair payloads are not
cloned into delivery pages before being merged. The reducer owns its existing
bounded spill fallback. When final groups retain all join keys, already distinct
compact results keep the streaming path instead of paying for another hash
reduction.

Validation includes the shared-pin lifecycle regression, direct lookup and
copy-on-write work counts over 2,048 file records, refresh with an unchanged
manifest deliberately unavailable, exact admission with unrelated descriptors
and payloads unavailable, sparse physical ordinal round trips, descriptor
reissue/generation isolation, and GC traversal of paged catalogs. Real Parquet
and PyIceberg E2Es exercise both fragmented files and multiple native segments
inside one physical file, including metadata filters and cold restart. Fixture
and page-count results do not substitute for representative archive-scale
latency, provider request counts, RSS or join-skew benchmarks.

Final qualification on Zig 0.17.0 Debug passes all 337 lake checks (138 shared,
199 API) without allocator leaks and builds the production CLI. Eight selected
real-file E2Es pass: fragmented files and multi-segment files in both Parquet and
Iceberg, independent Iceberg snapshot/schema/delete transitions, global remote
text scoring and cursors, incremental artifact reuse, and the wide typed join.
The 2,048-file catalog initially writes 64 pages; replacing one file changes
three pages and reads five, while two identity lookups read four pages. Formatting,
Python syntax, license headers, source boundaries, and diff checks pass.

Main integration through `1258e8cfcb` retains bounded per-file replay, durable
construction checkpoints and damaged-artifact recovery. Text DF checkpoints retain the base statistics and durable signed contribution
runs for completed files. Finalization applies the combined delta once.
Final version-3 publication reuses those completed manifests and adds paged
routes and term contribution directories. Retained-artifact recovery streams
compact routes one file at a time, validates independent descriptors and
physical bitmaps, and checks seekable packs without reopening unchanged
combined maintenance manifests. Recovery still checks retained metadata and
pack existence across the retained corpus. Ordered checkpoints retain shared
spill extents and the common 4 MiB workspace, with direct reverse construction
for the initial file and bounded COW updates for subsequent files.

Qualification after this main merge passes 342 lake checks (138 shared, 204 API)
with no allocator leaks, builds the production CLI, and passes all eight selected
real Parquet/Iceberg E2Es in 72.95 seconds. The recovery matrix additionally covers
missing independent segment and physical descriptors and preserves completed
file identities across resume. Formatting, Python syntax, license headers, the
Apache product boundary and diff checks pass. The legacy lexical boundary
analyzer stops at obsolete storage paths that moved to the embedded package;
its unchanged full audit cannot complete.


### Canonical v3 admission, reader leases and finalized join ownership

The public text query path now accepts v3 necessary-term segment proofs. Both
full admission and selective admission consume the canonical segment directory;
new publications omit the legacy full inventory, compact routing tree and
segment-to-file tree. File catalogs and combined maintenance manifests remain
for incremental updates and GC. Compatibility readers and GC marking still
recognize older publications. Segment prefix seeks have an exclusive bound,
which prevents loading a neighboring leaf just to discover the prefix ended.

Refresh validation streams canonical segment records one at a time. It never
reopens unchanged maintenance manifests. Up to eight retained-object metadata
checks overlap through shared I/O admission, with a 64 KiB allocator per check
and serialized access to the build allocator. Queue entries own their references
until joined; all outstanding tasks are canceled and joined on error. The audit
still authenticates retained metadata and checks every retained serving leaf.
Delta-only validation requires a backend retention certificate that survives
restores and invalidates on artifact loss or collection. The present backend
contract cannot supply that proof, so an unchanged root digest alone does not
suppress the audit.

Query facades expose active segment leases alongside the existing stable-pointer
borrow. Streaming search and prepared Boolean readers use leases. Four idle
bindings may remain cached; active users and compatibility borrows cannot be
evicted. Retired bindings join speculative workers outside the inventory lock,
and quiescence waits for those detached retirements. Typed value scopes retain
at most four leased segments, evict their field readers first, and remove point
cache identities before pointer reuse. Prefetch workers release navigation pins
on completion; a small join-owned descriptor also handles cancellation before
worker entry.

Aggregate providers may transfer ownership of a reducer already grouped by the
final recipe keys. Join reduction uses this contract when no upper key predicate
or aggregate FILTER adjustment remains. HAVING, ordering and result projection
consume the transferred typed state directly, including its spill fallback.
Empty global aggregates retain their single SQL result row. Predicates and
FILTER adjustments retain the existing exact merge path.

Regressions cover eviction with active and stable borrows, typed scope bounds,
point-cache pointer reuse, worker pin release before joining, empty joins,
HAVING/order/limit and predicate/FILTER fallback. The fragmented real-file HTTP
fixture checks both Parquet and Iceberg after cold restart with unrelated segment
routing leaves temporarily unavailable. This tests actual selective admission,
in addition to result and BM25 score equivalence.

Deferred typed delivery now shares ownership of the exact scoring pin instead
of reopening a corpus after ranking. Native document numbers therefore keep the
same meaning through source projection even if optional admission would choose
a different fallback on a second acquisition. Final shared release quiesces the
query facade and releases provider metadata; intermediate releases preserve
current authorization checks. Scalar identity/ordinal reads, score planning,
filter membership, sort coverage checks and sorted-segment iteration also use
scoped segment leases rather than stable compatibility borrows.

Qualification after merging `origin/main` through `d3bbb9a0c6` (Zig 0.17.0,
Debug): all 345 lake checks pass (141 shared, 204 API), with no allocator leaks;
all 43 focused reader/sort checks and 66 scorer checks pass. The production CLI
builds and all eight selected real Parquet/Iceberg HTTP E2Es pass, including both
cold-cache routing-page exclusion cases, fragmented native segments, real
Iceberg snapshot/schema/delete transitions, ranking/highlights/cursors,
incremental artifact reuse, and wide partial join delivery. Formatting, Python
syntax, changed-file license headers, the Apache product boundary (2026 sources)
and diff checks pass. Archive-scale throughput, RSS and provider-request
benchmarks remain unqualified; retained serving-closure auditing remains
corpus-wide as described above.


### Complete directory bootstrap, ordered intersections and adaptive join probes

Parallel statistics joins consume their task slot before awaiting its result.
An error therefore leaves only outstanding tasks for deferred cancellation and
joining. Frequency and summary regressions exercise a successful caller lane
alongside a failing background lane.

An optional term directory disabled by an unrepresentable key has no complete
base for incremental updates. Bootstrapping it now streams unchanged segment
summaries from the canonical serving directory, in addition to changed file
contributions. It never publishes a changed-files-only directory as a complete
membership proof. A surviving oversized key keeps the directory disabled; when
that key is removed the next complete bootstrap can restore indexed routing.
The real Parquet HTTP regression replaces an unrelated file, removes the large
key, and refreshes another unrelated file while checking the retained match.

Cold admission intersects required term memberships before acquiring readers.
Ordered tree cursors retain their authenticated ancestor path and seek forward
past entire unrelated sibling subtrees. Leapfrog intersection bounds resident
state by at most 32 term cursors and applies the 4,096 candidate limit to the
intersection, even when each term independently exceeds that limit. An 8 MiB
working/read budget and 65,536 cursor-operation budget retain the full serving
fallback if the proof is too expensive. Recursive Boolean proofs intersect known
necessary sets; unknown and negative clauses retain conservative supersets.
Global corpus totals and BM25 statistics do not change with admission.

SQL dynamic filters retain a complete exact build-key set while it fits both
4,096 distinct tuples and a 1 MiB allocation budget. Values are owned, NULL join
keys are omitted, and semantic numeric equality deduplicates tuples. Exceeding
either bound discards the entire optional set and keeps the Bloom/domain path.
Duplicates do not consume additional exact-set capacity; join multiplicities
remain in the build-side join state.

Before the first probe pull, the external SQL adapter can select a compatible
native ordered index using that sealed evidence. It encodes and deduplicates
leading-key prefixes, costs their combined authenticated cardinality, and
streams disjoint prefix ranges through one leased generation. A bounded 2 MiB
page cache shares authenticated ancestors across cost estimates and subsequent
probes; exhausted optional planning budgets retain the scan fallback. Covering values
retain typed block delivery; uncovered fields hydrate bounded physical windows.
There is no accumulated matching-row-ID list or fanout limit per key. Partial
indexes still require implication from the static query predicates. Explicit
cursor/order requests and accepted-write overlays retain their existing access
paths. Larger or incompatible builds keep scan-level filtering.

### Backend retention evidence for incremental serving closures

Retained-closure certificates are backend-owned advisory metadata keyed by the
complete scoped root identity. Publication leases, source binding, authorization
and cancellation remain independent requirements. A certificate is useful only
when the backend can attest that its immutable upload namespaces have retained
their identities; a root digest alone is insufficient.

The filesystem backend supplies namespace witnesses from directory inode,
ctime and mtime plus a durable destructive-mutation epoch. Managed deletion
advances a random epoch before removing a payload. Atomic object replacement,
directory loss and directory restoration invalidate the directory witness.
Restoration tools that modify immutable payloads in place must invalidate the
namespace epoch before doing so. Payload integrity remains checked during normal
verified reads; certificates do not replace payload checksums.

A legacy or invalid certificate triggers the existing complete serving-closure
audit. That audit snapshots namespace evidence before traversing references and
only records a certificate if all evidence remains current afterward. A fresh
build proves its newly uploaded namespace by construction. Incremental builds
inherit an audited base's evidence and add their new upload namespace. Final
publication seals pending certificates only after all indexes and publication
directory uploads have completed. Existing base namespaces are never resealed
to hide a mutation. A failed or uncommitted build cannot make a certificate into
a publication authority.

Certificates survive backend reopen. Ordinary reuse checks namespace witnesses
instead of issuing a metadata request for every retained segment pack, block,
descriptor and tree page. Cost therefore follows retained upload namespaces,
while changed artifacts are checked by ordinary authenticated build reads and
uploads. Proofs cap at 4,096 namespaces, 1 MiB serialized metadata and 64 pending
roots; exceeding the caps retains full auditing. Cloud object stores without a
provider retention guarantee retain full auditing through the same optional
capability contract. GCS and S3 retention protect versions, which is insufficient
for references addressed by their live object name. Both providers decline this
shortcut until artifact references and every reader pin an immutable generation.

Regression coverage includes durable reopen, delta inheritance, uploads after
inheritance but before sealing, deletion, restoration and corrupt certificate
fallback. Real Parquet join tests cover duplicate and NULL build keys, absent
prefixes, more than one delivery window per key, uncovered hydration, and a cold
covering join with deliberately unreadable Parquet data pages.

Validation of this refinement (Zig 0.17.0, Debug): all 346 lake integration
checks pass (141 shared and 205 API), plus 56 focused checks with one optional
benchmark skipped. The production CLI builds, and all eleven selected real
Parquet/Iceberg E2Es pass against that binary: global text behavior, incremental
artifact reuse, wide joins, the three new refresh/join cases, independent Iceberg
snapshot/schema/delete transitions, and fragmented files/segments in both
formats. The two native module-boundary audits, Apache product audit across
2,026 sources, changed-source formatting/license headers, Python syntax and diff
checks pass. Archive-scale throughput, RSS and provider-request benchmarks
remain pending.

### Ordered closure proofs, durable build runs, and large dynamic probes

Ordered row indexes now use the artifact backend's retained-closure capability,
matching native text indexes. An authenticated root carries its artifact identity
only in memory. A valid backend certificate skips the four tree walks and covering
block HEAD checks during reuse. A cold audit records witnesses before reading each
scope and remembers a certificate only after all dependencies and active-reader
checks pass. Unsupported backends continue to perform the complete audit. Deletion
or destructive restoration invalidates witnesses before mutation; ordinary leased
reads still authenticate data. Same-attempt checkpoint descendants replace their
pending advisory ancestor proof, so thousands of checkpoints cannot crowd the
final serving root out of the bounded proof directory. A pending fresh-scope
destructive epoch is checked before sealing, so build-time deletion cannot be
certified by taking a newer witness.

An initial ordered build checkpoints durable, authenticated sorted runs instead
of reconstructing the growing serving index after every source file. Equal run
levels compact by binary carry, leaving at most one run per level. Checkpoints
retain file fingerprints and stable slots, so a restart scans only unfinished
files. Cover blocks remain referenced by the runs. GC walks those run trees and
cover references, including checkpoints pinned by build authority. Final assembly
merges the bounded forest once to construct forward, reverse, tie and predicate
trees. Checkpoint forests use metadata version 6 and cannot be installed as
serving roots; version-5 workers skip these derived checkpoint candidates and
rebuild safely. Replaced or removed immutable file identities use the ordinary filtered merge rather than appending
conflicting coordinates. Serving roots omit checkpoint-only fields and keep the
existing ordered tuple encoding.

For incremental serving roots, copy-on-write admission estimates touched page
bytes per mutation batch, including repeated ancestor rewrites. It uses ordered
seeks through sorted additions and a conservative deletion estimate, compared
with a sampled streaming-rebuild estimate. Identical row counts with clustered
and scattered tuples can therefore choose different strategies. Background
streaming builders bound each authenticated page and covering block individually;
query cursors retain their cumulative read budgets. Build cancellation and lease
checks remain active throughout streaming and spill operations.

Native tuple and tie builders explicitly select encoded-byte sort mode. Keys are
already memcomparable, so normalization copies their leading bytes directly.
A variable-length radix pass distinguishes end-of-key from every byte, including
zero. Equal truncated prefixes compare borrowed full byte slices, without rebuilding
Datum rows. Spill runs, parallel run jobs, merges, and replay use the same comparator
and ordinal tie breaker. General SQL sorts keep their typed comparison semantics.

Dynamic join filters retain their small in-memory exact tuple set and move it to
a statement-owned sorted spill stream when its cardinality or memory allowance is
exceeded. The 4,096-key threshold is a representation crossover, rather than a
semantic cutoff when spill is available. Each native access candidate projects and
sorts encoded leading-key prefixes using bounded spill storage, deduplicates those
prefixes during replay, and costs the complete stream against a scan. Execution
keeps the verified B+tree path between ascending probe windows. Join multiplicity
still comes from the authoritative join state; duplicate projected prefixes do not
rescan source rows. Disk or memory admission failure abandons the entire optional
exact set and retains the Bloom/domain scan path. Cancellation and I/O failures
propagate. No partial exact set can exclude rows.

Exact NUMERIC values retain their typed payload at this boundary. A JSON-null
placeholder is never encoded as an index NULL. Same-domain values use lossless
numeric bounds; unsupported cross-domain conversion declines the optional access
path. Numeric filter masks also preserve the numeric scalar identity.

Validation covers variable-length and embedded-zero sort keys, long equal prefixes,
spill replay, complete dynamic sets above 4,096 keys, admission fallback, seventy
durable checkpoints, backend reopening, final closure proof reuse and invalidation,
and a real-Parquet multi-file join with duplicate/NULL build keys. The covering E2E
restarts after making source data pages unreadable, proving that the native index
serves both small and spilled build sets. These checks establish semantics and
bounded representations; archive-scale speedups still require workload benchmarks.

Validation for this refinement (Zig 0.17.0, Debug): all 350 lake integration
checks and 60 focused checks pass, with one optional benchmark skipped. Final
regressions also verify checkpoint-format GC marks every run and cover dependency,
and warm reverse cursors check cancellation before returning another row. All
15 selected real Parquet/Iceberg E2Es pass, including exact ordered bounds and
signed timestamp ordering. The production CLI builds. Formatting, Python syntax,
module/product boundary audits (822 embedded and 2,027 Apache sources), and diff
checks pass. No archive-scale throughput, RSS, or request-count claim is made.

### Coalesced maintenance, shared dynamic planning and cloud retention

Dense ordered refreshes filter the serving base once, then checkpoint changed
files into the durable binary merge forest already used by initial construction.
Finalization builds the serving trees once. Selective changes retain the existing
copy-on-write path. When lightweight Parquet discovery has not loaded row counts,
the strategy estimates changed rows from the retained index's per-file average.
A restart preserves stable file slots and resumes from the filtered base and completed changed-file runs; it cannot admit a construction
checkpoint as a serving index.

Global text statistics use durable signed delta runs with at most one run per
binary level. Each completed file checkpoints its manifest and those runs against
the retained base; finalization combines the contributions once. Recovery audits
and GC traverse both the base and pending runs. Final roots omit checkpoint-only
fields. Small local batches release their scratch runs after durable upload,
keeping temporary storage independent of archive size. Term-contribution directory
construction uses the same bounded batching. Exhausting that optional directory's
scratch budget disables the whole accelerator and retains exact search. Background
reverse and directory builders bound each authenticated page rather than impose a
cumulative query-sized I/O quota on an archive build. Query read admission remains
bounded separately.

Metadata publication walks sorted segment and physical-block spans linearly and
streams mutation batches into durable last-write-wins runs. It applies selective
changes in bounded COW batches or merges dense changes once. Both resident mutation
storage and local scratch usage follow a batch, rather than the full inventory.
The caller supplies the existing I/O context; publication creates no extra worker
pool.

SQL scan cursors explicitly negotiate whether they can consume a complete exact
key stream. Plain scans retain bounded Bloom/domain filters without writing an
unused exact spill stream; native index consumers can request exact spooling.
Dynamic candidates share compatible typed prefix projections through a bounded,
reference-counted cache. Planning deduplicates exact tuples before projection,
stops counting candidates once their cost exceeds the scan or current winner,
and caps probe enumeration even for absent keys. Only the selected candidate owns
the execution cursor, and its shared projection is rewound before execution.
Optional resource-admission failure retains the complete scan filter path.

GCS no longer supplies immutable-name witnesses. A locked retention policy can
retain a version while Object Versioning makes it noncurrent; checking a locked
policy and expiry does not prove that an unversioned artifact GET still resolves
to that object. The provider declines both witness creation and validation without
performing retention I/O, so old certificates also trigger complete audits. A
future provider shortcut requires generation-pinned artifact references, generation
GET/HEAD/range reads, and publication/GC proofs that carry those generations.
Checking only whether versioning is enabled at validation time would miss an
intervening configuration change. No bucket policy is created or changed by Antfly.

Provider contract: [GCS retention interactions with Object Versioning](https://docs.cloud.google.com/storage/docs/bucket-lock#considerations_with_other_features).

These changes are covered by regression tests for interrupted dense refresh,
signed contribution replay, bounded scratch reuse, optional accelerator fallback,
compatible projection reuse, linear span routing, and provider-proof reopen,
inheritance and expiry. Archive-scale latency, RSS and request-count measurements
remain required before claiming a quantified speedup.

Qualification caveat: real Parquet/Iceberg HTTP E2Es were attempted, but the
fixture rejected startup because the local volume did not meet its approximately
16 GiB free-space floor. No disk-safety setting was disabled. The final production CLI builds after reclaiming completed test link inputs.
Earlier combined builds exhausted local disk space; native integration was
therefore qualified separately. These environment failures are separate from
native real-file integration and provider-contract regression results. The
current GCS regression asserts unsupported immutable-name evidence without I/O;
the generic retained-provider fixture continues to cover valid provider witnesses.

Final qualification on Zig 0.17.0 Debug: all 359 lake integration checks pass
(142 shared and 217 API), plus 64 focused checks with one optional benchmark
skipped. No allocator leaks are reported. The object-store suite passes 83 checks
with two credentialed provider checks skipped, including the GCS retention
transport contract. The final production CLI, both native module-boundary audits,
the Apache product boundary across 2,028 sources, changed-source formatting and
license headers, and diff checks pass. The interrupted dense refresh regression
also passes in isolation. The ten selected HTTP Parquet/Iceberg E2Es could not
start under the unchanged disk-headroom safeguard; no successful E2E rerun or
archive-scale performance measurement is claimed for this qualification.

## Exact covering probes and bounded native metadata (October 2026)

Covering dynamic filters now use the same typed column conversion as Parquet
filters. NUMERIC values retain their canonical payload, including INT64 values
above 2^53, decimal strings and already-native NUMERIC cells. Equality seeks,
Bloom/domain membership and the authoritative join therefore compare the same
value. Explicit NUMERIC source contracts admit exact physical integers and decimal
strings, preserving the SQL type through footer validation and native index
builds; physical floating-point inputs remain rejected. Ordered key binding converts
external NUMERIC vectors explicitly into canonical payloads with bounded row
scratch, while native batch binding keeps strict binary validation. Native scans, vector column
evaluation and mapped batches retain full SQL type identity before coercion; lake
predicate envelopes retain exact NUMERIC lexemes and compare typed operands.
Regressions
cover decoded columnar covering blocks and independent
Parquet files, including cold queries with deliberately unreadable data pages.

Native text now publishes metadata version 10 with reader protocol 34 and a v10
binding identity. Earlier statements above about optional fields describe new
readers accepting older roots, not older readers accepting new roots. Protocol
33 workers cannot consume the paged inventory/statistics extensions and must
be fenced before interpreting them. Reconciliation replaces older publications;
the new reader and GC retain v9 recovery support, verifying the declared version
against the authenticated body before normalizing the in-memory representation.
Older GC formats continue through their historical traversal. There is no claim
that rolling back to a protocol-33 worker can serve a protocol-34 publication.

Broad text admission uses an additional authenticated compact routing tree,
ordered by immutable file identity and segment ordinal. Its pages contain score
navigation and descriptor references without physical row blocks or decoded
admissions. This eliminates the archive-wide canonical-record sort and loading
physical maps during cold corpus admission. Stable file/segment order preserves
score ties. Actual admissions remain query-scoped segment leases; physical row
maps load through the existing four-file LRU only when predicates or delivery
need them. Selective proofs still seek canonical segment identities directly.
The compact scoring navigation remains proportional to segment count and subject
to cache admission; this change does not introduce a fully virtual snapshot or
remove result, cache, or query-memory limits. GC and retained-closure audits mark
the compact routing tree alongside the canonical directories.

Term-directory construction no longer sorts a second archive-sized stream of
bucket reductions. Initial leaf order produces each bucket once, directly into a
bounded sorted page builder. Refreshes replay the ordered contribution delta,
deduplicate adjacent affected buckets before reduction, and apply bounded COW
batches. Durable leaf contribution forests continue to bound temporary disk;
optional acceleration still declines completely if those resources are exhausted.

Dynamic index costing merges the sorted exact-prefix stream with the verified
index tree. Each visited page is decoded once; fully covered subtrees use their
authenticated cardinalities. Missing domains skip together rather than performing
one root seek per key. Cost admission bounds page/candidate work and amortized
key comparisons; the former 65,536-probe cutoff is removed. A declined costing
pass never makes partial membership authoritative. The chosen execution cursor
rewinds the same projection and retains its authenticated forward path. Tests
exercise more than 65,536 interleaved missing ranges and a cold covering join with
more than 70,000 distinct build keys.

Text builds retain only compact file identities and durable manifest references
across files. Segment references, admissions and physical blocks live in a file
arena, are uploaded and checkpointed, and are released before the next file.
Changed/removed files, checkpoint recovery, collection totals, term contributions
and canonical/compact directory construction consume manifests one at a time.
The remaining archive-wide state consists of compact file references, bounded
run forests and per-field collection counters, rather than all decoded metadata.
Archive-scale throughput/RSS and provider-request benchmarks are still necessary
to quantify these improvements; deterministic tests establish correctness and
work bounds rather than a claimed speedup.


Qualification for these refinements (Zig 0.17.0, Debug): all 368 lake integration
checks pass (148 shared and 220 API), with no allocator leaks. All 39 focused
SQL batch/vector/relation checks pass, including direct and computed NUMERIC
hash joins. The production CLI builds, both native module-boundary audits pass,
and changed-source formatting, Python syntax, license headers and diff checks
pass. Twelve real-file E2Es pass: Parquet text ranking/filter/highlight/restart,
incremental generation reuse, optional term-directory bootstrap, two joins with
more than 70,000 build keys, two exact NUMERIC covering joins, real Iceberg
snapshot/schema/partition/delete transitions, and four fragmented corpus shapes
across Parquet and Iceberg. Cold covering queries succeed after source data pages
are corrupted and range caches are removed. Disk admission safeguards remain
unchanged. SQL NUMERIC comparisons retain typed residual enforcement; this
qualification does not claim a new native SQL predicate pushdown capability.

### Typed materialization, bounded broad admission, and startup navigation

Reader protocol 35 and `native-sql-aggregate-recipe-v2` fence the earlier loss of
NUMERIC type information in materialization batches. Recipe keys and measures
now carry the normalized scalar element type and NUMERIC modifier all the way
from the catalog to typed batch decoding. Both recipe identity and declaration
identity change, so a previously rounded materialization cannot satisfy a new
request or be inherited as an unchanged file contribution. This applies to
COUNT grouping as well as SUM/MIN/MAX inputs.

Standalone SQL comparisons against a direct NUMERIC column now derive native
index bounds from the bound scalar program and invocation parameters. Only
constant/parameter operands, casts, signs, and conjunctions in the exact
integer/NUMERIC comparison domain qualify. Mixed floating comparisons remain
residual. Conditions carry an exact typed operand alongside a lossless wire
encoding for tuple bounds; typed scan filtering never reconstructs that operand
through floating point. This uses the existing persistent ordered/predicate
indexes, costing, covering projection, reader leases, and spillable candidate
streams rather than introducing an independent metadata index.

Compatible Parquet integer statistics, page indexes, and equality Bloom filters
can also consume these predicates. Fractional range operands produce exact
floor/ceiling integer bounds with a typed residual. Non-finite or out-of-range
operands decline optional integer pruning, as do negative-scale column modifiers.
Text-backed decimals cannot use lexical min/max as numeric bounds. In particular,
an eight-byte string page bound must never be reinterpreted as an integer.
Annotated decimal/timestamp statistics retain the existing conservative checks.

Broad, ordinary term/match/phrase/Boolean score queries over more than 64 segments
stream the authenticated routing tree and admit at most 64 segments and 32 MiB
of encoded segment data per window. An indivisible segment is admitted alone.
The cursor carries only its identity and counts when a byte limit ends a window.
Routing decode and query scratch regions reuse capacity across pulls, including
when their parent allocator is an arena. Window snapshots use publication-wide
collection lengths and global term
frequencies. A bounded heap retains `offset + limit` winners, compares score ties
using the original corpus ordinal, and releases each window before advancing.
Final winners hydrate from physical keys under the existing leased publication.
Count-only requests sum exact counts or preserve lower-bound relations. The
cursor validates complete segment/document totals and per-file interval,
manifest, physical-map and digest continuity without retaining the inventory.
A bounded optional selection probe preserves the existing selective route for
small candidate sets.

Metadata filters, exclusion predicates, typed sorting, forward/backward cursors
and highlights now use this bounded coordinator too. The shared sort comparator
preserves exact NUMERIC and signed timestamps, null order and implicit public-ID
ties. Backward pages retain the closest preceding winners and restore canonical
output order. Compatible ordered-row index plans retain the existing global
winning-prefix seek. Other sorted requests use native doc values within each
window, rather than repeating a global ordered-index scan per window. Early
highlight planning reads only authenticated configuration/schema metadata; it
never opens the corpus merely to find highlight dependencies. Highlight fields
excluded from the public projection hydrate after final ranking. When the index
retains authenticated source, each final winner carries a compact segment-local
coordinate rather than a reader pin. Delivery reopens one immutable segment at a
time under the publication lease, checks the public document identity, and reuses
one input region for hidden source while retaining only returned snippets.
Discarded and offset-skipped candidates retain no highlight coordinates. The
shared public request/response field contract now admits the documented boolean
`store_source` option. Create-table and create-index normalization preserve true
and false, omit SDK optional nulls, and reject non-boolean values; index responses
retain the setting rather than silently dropping it.

The winner heap forwards a trusted score cutoff to later windows. Native range,
segment and posting bounds can prune before payload admission even before a local
heap fills. Equal scores remain eligible because a local document number cannot
represent a global tie boundary. Pruned results retain a lower-bound hit relation;
count-only execution never receives the cutoff. The public query parser cannot set
this internal hint. A publication-wide best-first frontier is still additional
work: current best-first navigation is within a window, and windows stream in
immutable routing order.

Profiled, overlay, multi-source composed/vector and early-source request shapes
retain their existing planners. A fully virtual snapshot for those shapes remains
additional work; this change does not claim constant navigation memory for every
search API combination. Compact source-file proof maps also
remain separate from the bounded segment window.

New text-file builds compact eight adjacent small runs when their combined input
fits 16 MiB. Optional merge work has a 64 MiB ceiling and the ordinary publication
allocator remains authoritative. A compacted artifact must fit the existing
32 MiB serving limit and avoid significant size expansion. Exhausting merge
workspace preserves the original runs. Input order and physical row keys remain
unchanged. Compaction is per changed file: immutable historical file artifacts
and their publication/GC closure are reused when a small current file arrives.
This avoids rewriting historical postings on every update. Background compaction
across source files would require a separate physical-span ownership design and
is not introduced here.

Index reconciliation and readiness attempt planning warmup once per immutable
publication, with concurrent attempts coalesced. A fixed 256-entry completion
ring bounds the memo; evicted publications may warm again. At most four publications warm
concurrently; each uses at most four shared I/O lanes, 4 MiB workspace per lane,
64 authenticated planning pages, two interior navigation levels, and a two-second
deadline. Shutdown and the caller's cancellation/deadline are propagated, and
all tasks join before the reader lease is released. Postings and source rows are
not warmed. Readiness waits for the bounded attempt; a cache miss or warming
failure does not replace ordinary source/publication proof or indefinitely block
queryability. Catalog build-progress updates do not repeat the same warmup.

`antfly_lake_navigation_warming_{attempts,failures,pages,nanoseconds}_total` report
warmup separately from query phases. Benchmark reports must distinguish a fresh
RAM/disk cache with no warming from restart-after-warmup and repeated-query cases;
removing disk entries before a restart alone does not establish an empty planning
cache after readiness. Unit/benchmark harnesses can use fresh serving-cache
instances to measure the genuinely cold path independently.

Join preaggregation retains up to four typed reducers across native batches,
subject to shared CPU/memory admission. Each canonical group key hashes to one
owner for the complete input. Stable row selections preserve source ordinals and
encoded columns; the hash is computed once per input batch. Disjoint lane states
stream directly into compact join construction/probing, eliminating duplicated
per-lane groups and the final serial rehash/merge. Source occurrence ordinals stay
separate from actual row counts in resident and spilled reducers; lane cardinality
estimates never multiply global source ordinals. Declined
scheduler submissions run inline. Borrowed source batches remain alive until all
workers join, including errors and cancellation. Lane and group budgets are
fractions of the statement allowance; no larger statement or response limit is
introduced. Canonical integer/string/Boolean/datetime group keys qualify. Floating
SUM/AVG, DISTINCT/pattern sets, noncanonical group representatives, and extrema
whose equal values can have different display representations retain ordered
reduction. Broader parallelism requires explicit representative/tie ordinals in
those typed states rather than changing observable results incidentally.

Wide external schema derivation charges parsing/runtime workspace to the statement
allocator, separately from the 1 MiB retained-layout cache slot. Cache admission
never rejects an otherwise valid wide schema: oversized JSON or retained layouts
derive directly in the bounded statement region. Cache misses retain the existing
four-build admission and authorization/catalog checks. A deterministic long-source-
URI Parquet regression covers single-table COUNT, wide join COUNT and 128 SUMs;
unit tests force both oversized JSON and retained-layout bypass, repeated misses,
slot cleanup, and compact retention of a 129-column external schema.

Earlier qualification included `origin/main` through `1a0e150bf9` and preserved
both the shared spill run store and main's presorted-run collection. That Zig 0.17.0
Debug qualification passed all 92 combined CLI/integration/boundary build steps,
374 lake integration tests (151 shared, 223 API), and both embedded boundary
audits without allocator leaks. Changed-source Zig formatting, Python syntax,
and diff checks pass.

That qualification passed 18 selected real Parquet/Iceberg E2Es against its rebuilt CLI:
remote text ranking/filter/highlight/restart, incremental artifact reuse,
optional-directory bootstrap, wide partial join delivery, two native joins with
more than 70,000 distinct build keys, two exact NUMERIC covering joins, two exact
NUMERIC aggregate materializations, the 67-file broad-window score/tie/count/
hydration comparison, two physical-type pruning regressions, real Iceberg
snapshot/schema/partition/delete transitions, and four fragmented corpus shapes.
Cold covering/materialized queries succeed with unreadable source data pages and
cleared range caches. A distinct authenticated Parquet source with unreadable
data pages verifies compatible integer pruning; eight-byte decimal strings
remain exact residual comparisons. Disk admission safeguards remain intact.
These regressions establish correctness and bounded execution contracts;
representative archive-scale throughput/RSS/provider-request measurements remain
pending.

### Current refinement qualification

The branch includes `origin/main` through `d1182ab89d`; the merge was clean.
Zig 0.17.0 Debug CLI, lake integration, bounded ranking and both native boundary
checks passed 102/102 combined build steps after that merge: 153 shared and 226
API integration tests, plus 41 shared and 28 API bounded-ranking tests, with one
benchmark skipped and no allocator leaks. Final native lake integration passed
50/50 build steps and all 382 tests (153 shared, 229 API), without leaks. Final
public option/compatibility normalization qualification passed 85/85 combined
production CLI and focused API
build steps, with all 43 table-contract regressions passing without leaks. Those
checks include both create-table paths, create-index boolean normalization,
exact large schema literals, private identity rejection, and every allocation
failure during compatibility normalization. Existing assertions now account for
current durable identities rather than requiring identity-free catalog entries.

Earlier refinement qualification also passed the public lake API roots (153
shared and 208 API tests), all ten focused schema-cache regressions, and both
focused object-store regressions. Repeated shared roots are separate test
executions, not distinct cases. The metadata-only highlight regression leaves
the corpus cache uninitialized, proving dependency planning does not acquire
readers. A final-winner regression verifies discarded and offset-skipped hits
retain no stored-highlight coordinates.

All 20 selected real Parquet/Iceberg E2Es pass against that final CLI (278 seconds). The expanded
67-file fixture compares metadata includes/exclusions, field sorts, ordered-index
prefix seeks, forward/backward cursors, hidden highlight dependencies, native
stored-source highlighting, exact counts, scores and hydration with full admission,
both before and after restart.
The wide 129-column Parquet fixture now forces a long source URI and verifies
single-table COUNT, join COUNT and 128 SUM reducers. Other selected E2Es cover
large dynamic joins, exact NUMERIC covering/materialized paths, conservative
embedded Parquet pruning, incremental reuse, optional directories, fragmented
corpora, and real Iceberg schema/partition/snapshot/delete transitions.

The final authored and resolved native module boundary audits pass, as do their
20 Python regressions. Changed-source Zig formatting, Python syntax and diff
checks pass. Cache storage follows the newly merged worktree policy. These
checks establish correctness and bounded-work contracts; they do not replace
representative archive-scale latency, RSS and provider-request measurements.

### Original physical compaction requirements (superseded by #1117)

The following records the original requirements. The bounded implementation and
its remaining scope are described in the deployment qualification plan linked
above; the statements that these features are unimplemented are historical.

Cross-source-file compaction remains unimplemented. The current serving record
owns one file's physical rank directory, so concatenating bytes across files
without changing that ownership would corrupt predicate ordinals and hydration.
The intended implementation separates maintenance file state from serving packs:

- Each serving pack owns authenticated spans identifying the original file,
  its immutable digest, physical-row manifest, file rank interval, and pack
  document interval. A source file may contribute to multiple packs. Predicate
  consumers map physical ranks through these spans; hydration uses the same
  mapping under the publication lease.
- Size-tiered merges have bounded fan-in, input bytes, output bytes and working
  memory. Small new runs accumulate separately from stable historical packs.
  Updates replace only packs containing changed spans, plus bounded compaction
  work. A small append never rebuilds an entire historical base.
- File-level DF contributions remain independent of physical packing. Serving
  term summaries and routing identify the merged packs. Refresh applies signed
  contributions exactly once and preserves the complete collection statistics.
- Maintenance records retain the metadata needed for refresh, rather than
  permanently retaining duplicate original posting payloads after compaction.
  GC marks pack payloads, span/physical manifests and statistics for every live
  reader and builder generation. Recovery checkpoints retain all unfinished
  inputs until publication commits. The reader protocol must fence this format.
- Qualification must cover selective and broad predicates, rank/select across
  delete holes and multiple source files, scores and ties against full admission,
  hidden highlight fields, sorted cursor pages, restart/replay, incremental
  append/replacement/removal, and GC while an old generation remains pinned.
  A refresh work-count test must prove that unrelated historical payloads are
  neither read nor rewritten.

The active follow-up now implements publication-wide best-first text navigation
over the existing authenticated term-directory subtree reductions, described
below. It does not require a new artifact format or a complete routing walk.

### Follow-up after PR #1063: implementation and qualification scope

PR #1063 merged as `68eece3fd6240f844cca890de36b9d1238084428`. The next
follow-up starts from that main commit, including the main changes which arrived
after the earlier `d1182ab89d` qualification. The frontier and bounded profiling/
composed-text work below are implemented and qualified in this follow-up, which
also merges main through `553e58317f60d25444a0ee80aa2cf9a20a546055` (#1113).
The historical pack and cloud-generation migrations remain pending; the PR
remains a draft while those broader workstreams are incomplete.

| Workstream | Existing behavior | Required outcome |
| --- | --- | --- |
| Global search frontier | Implemented: authenticated term reductions prioritize disjoint segment hash ranges before inventory admission. | Qualify score, tie, source, count and bounded-work contracts. |
| Historical serving compaction | Small fresh runs compact within a changed source file. | Compact across source files with authenticated physical spans, bounded maintenance work, and cheap appends. |
| Cloud retained closures | Unsupported provider evidence requires complete retained-artifact audits. | Reuse closure proofs backed by durable object generations and provider-enforced retention, without trusting mutable live names. |
| Broader bounded admission | Implemented for profiling, text components of composed/vector retrieval, pruners, rerankers and evaluation; overlays retain their existing planner. | Extend bounded ownership to overlay/early-source shapes with coherent scoring statistics and source dependencies. |

#### Global search frontier

Attach conservative query-independent term envelopes to authenticated routing
subtrees, then derive query bounds using the pinned publication's global DF,
collection lengths, BM25 configuration, and supported Boolean semantics. A
bounded frontier must identify a subtree without opening every segment below it.
Missing or unsupported bounds remain unknown/infinite and require traversal.

Keep navigation, candidate preparation, and result collection separate. Reuse
query analysis and global statistics across subtree admission. Prioritize the
highest conservative bound, feed admitted leaves through the shared native
search path, and propagate the final heap's competitive cutoff back to the
frontier. Prune only strictly lower scores; equality remains eligible until the
existing global tie comparator proves otherwise. Frontier memory pressure must
use bounded spill or a complete traversal fallback, never truncate candidates.

Preserve exact count-only execution and truthful hit-count relations when
ranking skips matches. Typed field sorts and ordered-index prefix seeks keep
their own admissible ordering contracts. Qualification must compare scores,
ties, filters, exclusions, highlights and cursor pages with full admission, and
prove that a competitive late-file subtree can be reached without admitting all
earlier windows. Include equal-bound, unknown-bound, cancellation, and frontier
pressure cases.

Implemented frontier details:

- The complete v2 term directory already carries conservative authenticated
  per-term subtree reductions. A metadata-only plan uses the native analyzer
  and Boolean lowerer, collection lengths and exact publication-wide DF. Bounds
  cover both constrained and unconstrained native lowering, including boosted
  terms' f32 addition order, before physical masks are known. The same preparation lends analyzed
  tokens and global frequencies to native scoring windows. Negative/nonfinite boosts or bounds conservatively disable
  score pruning; baseline-only and unsupported query shapes use the existing
  complete planner.
- The priority queue holds at most 512 disjoint ranges. Under pressure, a
  257-entry DFS stack completes an overflow subtree, preserving coverage and
  avoiding repeated admissions. Query scratch, syntax and navigation cache
  share an 8 MiB budget; authenticated term-tree misses have a separate 256 MiB
  read budget. Navigation failure propagates rather than returning partial
  ranked results. Optional preparation admission failures keep the established
  sequential routing path.
- Required-term and phrase-position presence follow the same lowered Boolean
  tree as scoring. A range missing a required clause is excluded before
  inventory admission even when fewer than K documents match and no cutoff
  exists. Optional clauses cannot narrow a mandatory match.
- The global cutoff excludes only strictly lower bounds. A winner's immutable
  file identity and original file rank supply the existing full-corpus tie
  order, independent of traversal and window-local native ordinals. The same
  physical rank mapping drives predicate and hydration translation. Count-only
  and typed field/cursor sorts retain their exact existing execution paths;
  skipped ranked matches yield `gte` totals.
- Profiling no longer disables bounded admission. Counters sum window work,
  capacities and collector sizes report peaks, and inconsistent plan labels
  report `mixed_window_plans`. Text components of composed/hybrid queries use
  bounded admission; their parent retains source/publication leases, fusion
  and final physical hydration. Stored-source highlights retain only final
  winning locations.
- `antfly_lake_text_frontier_*_total` metrics expose admitted segments, subtree
  reductions, pruned ranges, overflow subtrees, admission windows, summary cache
  hits and charged navigation metadata bytes, including failed preparation and
  interrupted frontiers. The refinement below includes global DF preparation
  in the cumulative navigation budget and counter. These are work
  counters, not archive-scale speedup claims.

The deterministic navigation fixture places the strong term match last among
8,192 immutable segments and checks that it is reached first with less than
2 MiB of further term-tree reads and no inventory directory. A one-entry queue
must visit every segment once at an equal score bound. A conjunction of that
common term with a rare term must admit only its three segments without a
competitive cutoff. Shared native tests compare exact scores and
winner order across analyzed, Boolean, signed and phrase shapes, including
unknown bounds. Real Parquet coverage compares full admission with bounded
profiles, metadata filters, exclusions, hybrids, hidden highlights and cursor
pages before and after restart. A direct single-index attachment additionally
asserts the `bounded_global_text_frontier` native profile label. Named and hybrid
queries retain their parent's fusion profile.

Qualification of native code at `c4897c9e94` and the final E2E fixture at
`1ef68a3f30`, on the merged main base above, uses Zig 0.17.0 on macOS arm64:

- `zig build lake-integration-test lake-text-frontier-test
  -Doptimize=ReleaseFast -j2 --summary all`: 386 integration tests and four
  focused tests pass, with no failures or leaks. The four focused tests are
  also included in the integration roots and are not four additional unique
  regressions. The deterministic frontier test covers 8,192 segments, a
  competitive late segment, required-clause absence, equal bounds, cancellation,
  and a one-entry priority queue under pressure.
- `zig build lake-integration-test -Doptimize=Debug -j2 --summary all`:
  the same 386 integration tests pass with safety checks, no failures or leaks.
- `zig build antfly -Doptimize=Debug -j2 --summary all`: the complete standalone
  server builds successfully. Real-file E2Es use a fixed copy of that binary so
  another build cannot replace it between restart assertions.
- The six selected real Parquet/Iceberg E2Es pass: broad native text windows
  with scores/ties/counts/profiles/filters/hybrids/hidden highlights and ordered
  cursor pages; independent PyIceberg schema IDs, snapshots, partitions,
  deletes and restart; and fragmented files/segments for both formats.
- Authored embedded production boundaries (824 sources), Apache product
  boundaries (2,037 sources), and resolved C API/public-package module
  boundaries (1,192/1,197 sources) pass. Changed-source formatting, Python
  syntax and `git diff --check` pass. The repository-wide license-header check
  still reports 15 files inherited unchanged from main; none is changed here.

The fixture is a correctness and work-contract regression, not an archive-scale
benchmark. At that qualification point, cross-file packing, durable provider
generation proofs, overlay and early-source bounded admission, and measured
archive-scale latency/RSS/provider request improvements remained pending. The
refinement below extends bounded admission to coordinator-owned source transforms.

#### Historical serving packs

Implement the physical-span format described above before allowing cross-file
merges. Make pack identity independent of source-file maintenance identity.
Preserve the existing logical corpus tie order across repacking; a new pack's
local document number must not become the public ranking tie breaker. Physical
predicate translation, inverse hydration, hidden stored-source highlights, and
delete-hole rank/select must all consume the same authenticated span mapping.

Use bounded size-tiered maintenance under the existing leader, source snapshot,
publication lease and shared scheduling rules. Keep stable historical packs
separate from recent runs. A source replacement/removal identifies affected
packs through indexed span ownership; unrelated historical payloads must remain
unread and unwritten. File-level signed DF contributions remain independent of
packing. Publication must atomically install matching routing, statistics,
physical maps and pack references, with crash-replay checkpoints protecting all
unfinished inputs.

Advance the reader protocol for the new representation and teach GC to retain
both supported older generations and new packs at every live reader/builder
boundary. Differential real Parquet/Iceberg tests must cover append, replacement,
removal, schema/snapshot transitions, old-reader GC, restart, partial compaction
recovery, sorted cursors and hidden highlights. Work-count tests must demonstrate
lower serving fan-out and bounded publication work for a small append.

#### Durable cloud generations and retention evidence

The optional `immutable_prefix_until` and `validate_immutable_until` callbacks
currently have no production provider implementation. In particular, GCS
generation retention cannot certify reads through a mutable live object name.
Complete auditing remains the correct fallback until a stronger artifact
reference and provider proof are available.

Persist the exact provider object generation/version in authenticated artifact
references and use it for required reads, metadata probes, verified caches and
retirement. An in-process version cache is insufficient after restart. Fence
this representation through the relevant metadata/reader versions and continue
to admit legacy references through full auditing.

A reusable certificate must bind backend identity, namespace, artifact closure,
exact protected generations and a conservative expiry. Establish from provider
evidence that those generations cannot be replaced or removed before expiry;
versioning alone does not supply that guarantee. Changes in bucket policy, live
names, authorization, retention validity, or restoration must never silently
extend the proof. Unsupported or expired evidence falls back to the existing
complete audit. Local filesystem namespace epochs retain their own semantics.

Test restart, live-name replacement, retained old generations, policy changes,
expiry, missing objects, certificate tampering, provider failures, cancellation,
cross-namespace replay and GC with pinned readers. Add provider-request counters
which prove an unchanged cloud refresh avoids per-artifact probes only while a
valid proof is present. Qualify actual provider behavior separately from mock
contract tests before enabling a production certificate implementation.

#### Broader admission and measurement gates

Move remaining request shapes onto bounded inventory ownership incrementally.
Profiling must observe the bounded execution path; overlay visibility and
multi-source/vector composition must preserve shared source/publication leases
and their exact existing ranking contracts. Audit compact file-proof maps and
repeated planning costs as file count grows. Keep existing final response-size,
statement-memory and temporary-disk safeguards.

Measure the merged baseline before making speedup claims. Use both many-small-
file and many-segment-in-one-file layouts, selective and broad terms, conjunctions
and phrases, top-K and exact counts, metadata filters, typed cursor pages, and
small appends/replacements. Report result equivalence, latency distributions,
peak RSS, provider requests/bytes, admitted inventories/readers, spill bytes,
publication reads/writes and GC work. Separate genuinely empty-cache execution,
restart after bounded readiness warming, and repeated warm queries. Record the
commit, build mode, dataset shape, cache policy and resource limits with each
measurement. Deterministic work-count regressions remain required even when
wall-clock benchmarks improve.


### PR #1112 refinement: cumulative work and coordinator transforms

The refinement merges main through `e8666a5b75` (#1115), including main's
narrow API-kernel resolver import and formatting corrections.
It corrects the frontier's read accounting: `PageStore.streaming` previously
reset the nominal cumulative allowance on every page miss. The frontier now
owns one non-replenishing 256 MiB allowance for global DF preparation,
term-directory reductions and selected routing-size reads. A separate monotonic
charged-byte counter records admitted read work, including shared-cache hits and
failed provider attempts; it is not a measurement of network transfer bytes.
A failed budget charge adds no bytes. Frontier-owned teardown records attempts,
fallbacks, cancellation and preparation failures exactly once.

A range-local cache shares each raw term summary across repeated clauses and
both canonical bound trees. Native scoring borrows the query's original analyzer
identities and pre-lowered syntax only when the scorer's complete query matches.
Predicate wrapping keeps ordinary lowering. Both constrained and unconstrained
scoring roots use the scorer's exact wrapper, preserving legacy normalization,
Boolean grouping and f32 addition order. Each selected window still owns its
visibility, predicates, iterators and scoring state. It does not re-enumerate the
publication's global term directory to rediscover the same candidates.

Admission starts with one segment so competitive cutoffs become available
quickly. An unchanged full heap grows subsequent batches geometrically to at
most 64 segments; an improving cutoff returns admission to one segment. Every
batch also respects a 32 MiB authenticated native-payload target. An indivisible
large segment is emitted alone, and an overflowing leaf is carried to the next
pull without dropping coverage. Native admission continues to use the shared
scheduler and its existing bounded parallel reader loading. Navigation counters
include admitted windows and summary-cache hits, making setup amortization and
metadata reuse observable independently of latency.

Pruners, rerankers and evaluation now use the bounded global
retrieval pool. The shared component-paging contract determines the reranker
pool; candidate evaluation retrieves its declared candidate count with offset
zero. Every child request clears coordinator-owned transforms. Source-dependent
transforms load the global pool once with all hidden input fields intact before
coordinator evaluation/reranking. Surviving hits reuse that source for final
projection and highlights instead of hydrating the final page a second time.
Pruners need only scores and avoid source hydration for rejected candidates.
Full-match evaluation for the supported text shapes disables competitive cutoffs
and verifies exact window
membership, using a same-window count when local WAND returned a lower bound.
Its byte-bounded batches grow independently of competitive cutoffs, since every
possible match must be visited.
It rejects a set larger than the declared evaluation budget before source
hydration. Its limit error has an appended stable status in both runtime error
registries and the typed public-query error contract. The query adapter and
HTTP handler preserve the public 422 `decision_limit_exceeded` response.
Exhausted cumulative navigation reads map to the existing query candidate budget
response. WAL overlays retain their existing complete planner and statistics.

Sort admission now selects physical coverage ownership from the authoritative
table binding. Remote indexes are validated by the leased native publication
reader, while local indexes continue through local observation and cold-handle
preflight. Public schema and cursor checks run for both. This avoids intermittent
`IndexNotFound` failures from preparing a nonexistent resident index for a remote
table, even when an active read schema contains only the field mappings.

Regression coverage includes monotonic and exhausted budgets with a warm shared
cache, failed-preparation accounting, range-summary reuse, size-bounded carried
batches, exact prepared scorer parity with a custom analyzer, and a provider that
rejects repeated global term preparation. The real-file differential fixture
forces a zero-score Boolean baseline for its independent reference planner and
covers pruning, hidden-field reranking, both evaluation scopes, nonzero offsets,
full-match budget rejection and restart.

Cross-source-file native serving packs and durable cloud generation certificates
remain unimplemented. Their authenticated span/reader/GC and provider generation
migrations are described above; no format flag, provider certificate, or
unsupported retention guarantee is enabled by these query-path refinements.
WAL overlay admission also remains pending the shared global-statistics and
physical-identity extension. Archive-scale latency and provider-request savings
have not yet been qualified.


#### Refinement qualification

Source at `038ad3787c`, including main `e8666a5b75`, is qualified on macOS arm64
with Zig 0.17.0. The final standalone Debug binary is copied into the worktree's
ignored cache before running real-file fixtures, so rebuilds cannot replace it
between restart assertions.

- The final combined Debug command (`antfly lake-integration-test
  lake-text-frontier-test antfly-api-public-table-http-test -j2`) completes
  all 105 build steps. Its two integration roots pass 154 and 234 tests
  (388 total), with no failures or leaks. The focused regressions overlap
  integration coverage and are not additional unique tests.
- Seven focused frontier/admission/scorer regressions pass in both Debug and
  ReleaseFast, with no failures or leaks. The sort ownership regression forces
  local observation to return cold-handle unavailability and local preflight to
  return `IndexNotFound`; remote admission still succeeds, including with a
  field-only active read schema, and rejects an invalid cursor.
- All 33 typed public HTTP contract tests pass, including the exact 422
  `decision_limit_exceeded` response. The filtered HTTP test root now imports
  its handler fixtures unconditionally, fixing previously empty selections.
- Runtime error identity checks pass in both Debug and ReleaseFast: 26 runtime
  error ABI tests, five compiled-owner failure identity tests and the frozen
  status-prefix compatibility check.
- All six real Parquet/Iceberg E2Es pass against the final Debug binary: broad
  ranked text across 67 independent files; PyIceberg snapshots/schema IDs/
  partitions/deletes/restart; and four fragmented file/segment/format cases.
  Differential broad-text coverage includes source-dependent transforms,
  count-only behavior, full-match budget rejection and profiled filtered sorts.
- Authored embedded production, Apache product and resolved C API/public-package
  boundary checks pass. Changed Zig formatting, Python fixture formatting and
  unresolved-name checks, and `git diff --check` pass.

These are correctness and bounded-work checks. They do not qualify archive-scale
latency, RSS or provider-request savings, and they do not complete the three
pending storage/visibility migrations above.

### PR #1112: dependency hydration, prepared membership and accepted-WAL windows

Transform candidate admission now plans physical dependencies before loading
source. Expression plans supply their required fields; direct-field rerankers
supply their field; highlight dependencies join that set. Free-form templates
retain the conservative complete-source path. Both paths share a 64 MiB limit
covering live hydration scratch, retained candidate JSON and transfer copies.
If final output needs additional columns, only surviving result rows load them.
Evaluation exhaustion preserves the public `decision_limit_exceeded` contract.

Immutable query preparation now includes membership and global term frequencies
for complete-admission, count and ordered windows, alongside ranked routing.
Bounded window lookahead submits at most one metadata task to the shared I/O
scheduler, reserving 8 MiB. Saturation falls back to inline required work.
Cancellation joins the worker before publication, analyzer and navigation owners
are released. This overlaps navigation with scoring; it does not preload a
second window of payload readers.

Accepted-WAL text execution builds the recent index once under a separate
64 MiB bound covering index construction and retained recent readers. Global
collection counters and term frequencies add recent rows to authenticated base
statistics exactly once. This preserves the existing immutable BM25 contract:
masked base rows remain in base scoring statistics. Physical tombstones resolve
once into compressed file/group/row sets and lower only into admitted windows.
The translation handles deleted holes, selected segment gaps and high row bits.

Recent rows score first to establish a useful cutoff. Metadata frontier bounds
use the combined collection statistics; archive payloads enter only when their
bound remains competitive. Equal-score ordering still places archive rows before
recent rows and preserves original file/rank order within the archive. Recent
metadata predicates evaluate against their accepted row images; archive
predicates retain the published native index path. Final projection and
highlights use authoritative source rows, including fields hidden from output.
The reader facade preserves the original range capability separately from its
global statistics interface. Their opaque pointers have different types and
must not be substituted for one another.

Standalone accepted text visibility now uses the same durable filesystem WAL
fallback as CDC admission and SQL. An absent cloud artifact connection no longer
silently bypasses locally acknowledged changes. Retained cut descriptors retain
both the archive publication and accepted row images across restart.

The current bounded overlay path covers eligible text queries and composed text
components. Existing fallbacks remain for unsupported early-source/hierarchy
shapes, small/legacy inventories and accepted vector/hybrid requests. Cross-file
serving compaction and durable provider-generation reference/certificate
migration remain outstanding; these changes do not claim to implement them.
Real-provider retention qualification was deferred by the operator on October
13: GCS and R2 are immediately available; a credentialed AWS S3 target is not yet
selected. No bucket retention policy or production certificate capability was
changed.

#### October 13 qualification

Source `d08a0e97bd`, including main `f3fd4d3525`, is qualified on macOS arm64
with Zig 0.17.0:

- The final Debug server/integration/public-HTTP command completes 95/95 build
  steps. Its two lake roots pass 156 and 249 tests (405 total), and all 33
  public HTTP contracts pass, with no failures or leaks.
- All 13 focused regressions pass in Debug and ReleaseFast. They cover exact
  combined-corpus/window score parity, recent-first tie ordering, private masks,
  global DF additions, recent-index byte exhaustion, physical masks across high
  row bits and selected gaps, original range-capability preservation and
  cancellation, metadata lookahead, and transform dependency planning.
- All eight real Parquet/Iceberg E2Es pass against the final Debug binary copied
  into the worktree's ignored cache. The new native-writer test uses 67 files
  written independently by PyIceberg, imports their real manifests through the
  managed catalog, accepts CDC replacements/deletes/inserts, checks bounded
  frontier selection, and verifies archive/recent predicates, hidden highlights,
  exact counts and identical retained results after restart. It belongs to the
  existing Iceberg module included in `e2e-full`.
- The 201-row wide-source fixture verifies narrow transform success, controlled
  rejection of a genuinely wide dependency and survivor-only full output across
  restart. No RSS threshold is inferred from these deterministic checks.
- The embedded production boundary (872 sources), its 20 audit tests, Apache
  product boundary (2,030 sources), changed Zig/Python formatting, unresolved
  Python-name checks and `git diff --check` pass.

These checks qualify correctness and bounded admission. They do not establish
representative archive latency or cloud request-count improvements. The storage
format migrations and broader accepted vector/hybrid shapes remain outstanding
as described above. Required cache GC ran; its 55-second guard stopped the
unresponsive uv prune after Zig/Go cleanup, preserving active build caches.

### PR #1112: accepted vector delivery and exact cloud retention primitives

Eligible accepted dense, sparse and hybrid requests now use the same late source
delivery admission as published requests. An overlay no longer forces a hybrid
text component onto whole-corpus composition. The text component can use bounded
recent/archive windows, while vector components retain their existing coherent,
exact-WAL-cut recent generation jobs. Recent vectors still require readiness;
this change does not substitute archive-only vector results during rebuilding.
Source-dependent hierarchy shapes retain their existing fallback.

The independently written 67-file Iceberg fixture now covers text, dense, sparse
and combined dense/sparse hybrid requests. Replacement vectors use different
coordinates from their archive images. Tests verify recent vector ranking,
exclusion of stale replacement vectors, indexed archive/recent filters, narrow
projection with hidden-field highlights, and identical retained results after
restart. Retained vector requests use their own vector-qualified cut: a text-only
cut that captured no recent vector declarations cannot acquire those declarations
by using a later live WAL cut. Such a request remains readiness-gated.

The provider interface now distinguishes exact-generation evidence from
current-name protection:

- S3 `GetObjectRetention` explicitly selects a non-null version and accepts only
  `COMPLIANCE` retention. Governance mode and revocable holds cannot provide this
  evidence. A contradictory response version declines evidence.
- GCS metadata explicitly selects a positive generation, checks the returned
  name/generation, and accepts only `retention.mode = Locked`. Ordinary bucket
  retention observations are not silently promoted to this stronger contract.
- Both require provider `Date`, parse expiry conservatively, and decline expired,
  missing or malformed evidence. The provider clock upper bound charges HTTP
  Date's whole-second precision and the complete request round trip. A fresh
  clock probe is available independently of per-object retention reads; callers
  must account for subsequent elapsed time and operation deadlines.
- All three GCS upload paths now return their exact generation in `PutResult`,
  including resumable uploads. Allocation-failure coverage verifies ownership.
- Explicit generation reads reject contradictory response identities. S3 retains
  the explicit selector when a compatible endpoint omits its version header.

These are read-only capabilities. They do not change retention policies, enable
cloud closure audit bypasses, or assert that R2 enforces S3 Object Lock. Existing
complete cloud closure audits remain authoritative. Protocol qualification uses
scripted transports; the two real-provider integration tests remain opt-in.

The durable publication migration remains necessary before these primitives can
produce cloud serving-closure certificates. In particular:

1. A publication must authenticate a bounded, paged artifact-to-provider-generation
   lookup, including exact generations for lookup pages themselves. Plain mutable
   sidecars or a process-local verification cache cannot provide restart safety.
2. Incremental publication must reuse sealed historical lookup roots and bound
   new records by changed uploads. Any per-attempt inventory must close before
   sealing; late uploads cannot become silently protected by its certificate.
3. Query-local readers must bind the publication's lookup before selection,
   directory loads, ranges and cache misses. Shared mutable store owners cannot
   be rebound between concurrent readers. Recent generations and retained cuts
   need their own authenticated bindings and lifetime protection.
4. GC must retain lookup pages and exact generations through publication,
   checkpoint, builder and reader authority, then retire the selected generations.
   Deleting the current name or listing only live names is insufficient.
5. A durable certificate must be authenticated by the publication, bind backend
   identity and the entire generation-qualified closure, and expire at the
   earliest witnessed retention time. Reuse requires a fresh provider-clock
   observation. Unsupported evidence, errors or expiry retain the complete audit.

Cross-source-file serving packs remain a separate unfinished format migration,
with the physical spans, ownership, tie-ordering and qualification requirements
specified above. The accepted-delivery change and provider primitives do not
implement that migration.

Qualification for source `71c4c36307` (accepted delivery `92b4e6a8a8`), still
including main `f3fd4d3525`:

- Final Debug server build: 48/48 steps; Debug lake integration: 156 shared plus
  249 API tests (405 total); public HTTP: 33 tests, all without failures/leaks.
- Provider suite: 89 tests pass in Debug and ReleaseFast; two opt-in live-provider
  tests skip. Focused lake frontier/window suite: 13 pass in both build modes.
- Final copied Debug server: all 11 Iceberg-module cases plus the wide-source
  transform dependency fixture pass (12 E2Es total, 236 seconds).
- Embedded boundary: 872 sources; Apache product boundary: 2,030 sources.
  Changed Zig/Python formatting, unresolved-name checks and diff checks pass.
- Latest fetch confirms main remains merged. Required cache GC completed Zig/Go
  cleanup and skipped active caches; the 55-second watchdog stopped uv prune.

These checks qualify the implemented delivery and provider contracts. They do
not qualify live-provider retention enforcement or complete either persisted
format migration identified above.


## Cross-file serving packs and durable cloud publications (protocol 38)

This section supersedes the earlier pending migration notes. Native text root v11
separates canonical per-source-file maintenance state from an independent packed
serving root. Roots v9/v10, including main's v10 `compacted`/`members` layout,
remain readable for recovery and retained requests. Builders advertise v11 and
refresh older publications; older workers cannot admit protocol-38 publications.
`loadRoot` retains its maintenance-reader contract, while query directory loading
resolves the authenticated serving root.

Each serving run authenticates compact source spans: original immutable file and
physical-hole manifest, original file row count/rank, packed offset and count.
Predicate translation and result/highlight hydration share these spans. Native
score collectors accept a borrowed logical tie-rank function, so equal scores and
cursor boundaries retain file-identity/original-rank order despite physical
repacking, parallel scoring or accepted-WAL composition. Conservative equal-score
block bounds cannot prune a better logical tie using a physical document number.

Serving construction has two bounded merge levels, each with eight-input fan-in,
a 16 MiB input target, a 32 MiB output cap and a 64 MiB merge workspace. Small
first-level runs feed the second level, producing historical runs with up to 64
source spans. The first workspace is released before a second-level merge starts.
Oversized optional merges split into smaller batches. A durable paged source-to-pack
ownership index locates packs touched by replacement/removal; at most seven small
open tails per level (14 total) are explicit in the root. Reader protocol 38 fences
the expanded tail representation; protocol-37 generations remain recoverable. Unchanged closed historical packs are reused
without opening their posting payloads. Small appends consolidate current tails;
source removal drops only that source's native ranges from affected packs.
Publication atomically binds serving runs, ownership and collection statistics.

Canonical file manifests and per-file signed DF contributions remain maintenance
units. GC retains their physical-hole metadata/statistics without retaining a
second copy of original postings or obsolete admission/opening artifacts. Packed
runs retain the physical manifests needed for their source spans. Retained reader
publications mark their own older serving roots before sweeping. The deterministic
regression packs 128 files into two runs, preserves both runs on append, checks
replacement/removal and logical score/cursor ordering, deletes original posting
payloads, and verifies restart and subsequent updates still work.

Cloud publications persist an immutable hash-radix generation map. Every payload
record pins its exact S3 version/GCS generation, and every map edge—including the
publication root—pins the map page's own provider generation. Incremental updates
rewrite changed radix paths. Maps use at most 128 records/leaf and 1 MiB/page;
readers have an 8 MiB cache of authenticated raw pages and decoded navigation.
The bound includes parsed arena capacity and reference ownership. Cache-hit point
lookups binary-search decoded records and copy only the selected generation;
posting range reads do not repeatedly parse map JSON. Cold lookups reclaim bounded
scratch after each range request. Query owners never rebind a shared store to another publication.
Native runtime caches charge map storage to their existing memory budgets.

Generation maps travel with durable builder checkpoints, accepted recent vector
jobs and retained text/vector/SQL cuts. Versioned opaque envelopes pin the envelope
object itself, so restart does not depend on a process-local verification cache or
mutable live names. Legacy envelopes retain their original wire identity and
verification path. R2/unversioned backends that return no usable version safely
retain ordinary content-verified reads and cannot issue generation certificates.

A cloud retention certificate binds reader protocol, backend/credentials identity,
namespace, publication generation/token, source/inventory/directory, generation-map
root/completeness, certified root set and a conservative expiry. A complete
migration audit establishes the live closure; fresh incremental uploads extend
only a valid inherited proof. Proof expiry is bounded by both payload and map-page
retention evidence. Fresh provider time with a 60-second margin is required before
skipping a closure audit. Missing, unsupported or expired evidence keeps normal
verification/auditing. The final metadata CAS resamples the lease clock after map
construction/auditing, so a long audit cannot publish using stale lease time.

`lake_indexes.cloud_generations.qualified_retention` defaults to `false`. Exact
version reads and durable maps work independently of this switch; operators must
qualify actual provider retention enforcement before enabling certificate-based
audit bypass. S3 requires exact non-null versions with COMPLIANCE retention; GCS
requires positive generations with Locked retention. Scripted wire and memory
providers qualify selection, expiry/replay checks, pagination, cancellation,
restart/live-name replacement and eventual version reclamation. These tests do
not substitute for the live S3/GCS qualification explicitly deferred by the user;
GCS/R2 availability alone does not qualify either backend's retention guarantees.

Cloud GC enumerates historical versions, including names hidden by newer versions
or delete markers, using S3 marker pairs or GCS opaque pagination. It deletes only
exact versions of artifact IDs absent from every live closure. Map pages are
reachability roots; their stale payload records do not retain orphan payloads.
Provider retention defers deletion without forgetting the authorized store locator.
Once no serving/reader/builder roots remain, bounded cleanup uses no new immutable
mark checkpoints, allowing locked bookkeeping objects to drain after expiry instead
of producing an endless chain. Provider requests and between-version work honor
cancellation. Collection progress remains durable while live roots exist.

Real Parquet/Iceberg E2Es continue to cover native filters, scoring, projections,
hidden highlights, accepted text/dense/sparse/hybrid visibility, retained tokens,
sorted pagination and cold restart. A compacted 67-file corpus can now fit a
single native window; the fixture checks native execution rather than requiring
fragmentation to force the global-frontier profile label. Archive-scale latency,
RSS and provider request benchmarks remain separate work.

The coordinator recovery adapter forwards provider-generation and retained-closure
capabilities. Readiness probes and startup warming bind the publication map before
loading its directory. A real-Parquet publication regression runs through that
adapter, checks durable checkpoint maps, reopens the packed corpus with a fresh
cloud owner, and rebuilds after a map disappears following retention expiry.
Upload consumers release provider selectors when transferring only content refs.

Compaction also exposed a small-corpus transform edge: final pagination could
shrink a reranker's candidate window before the coordinator saw it. Typed delivery
now selects the coordinator candidate window explicitly before dispatch, for both
compact and multi-window corpora. Native late-visibility paging respects the same
component window. The existing broad real-Parquet E2E requires all 31 requested
rerank candidates and checks offsets, evaluation, pruning and hidden inputs.

V11 GC traverses packed file catalogs through durable page jobs rather than
expanding a publication-sized manifest array. New records are independently
decodable, and the authenticated tree population must match the root's file count
before marking can authorize a sweep. Canonical source state retains its own
limits; serving-run counts no longer inherit the old 16,384-source-file limit.
Backend identity or namespace changes start a fresh generation map instead of
inheriting records from a different upload domain. The wrapped cloud regression
also covers identity rotation against the same physical bucket.

### Implementation qualification

Merged main base: `db86342bd8` (PR #1118). Zig 0.17.0, macOS arm64.

- The complete Debug native lake integration run passes 415 tests (156 shared
  engine and 259 server tests), with no failures or leaks. Final paged catalog
  traversal is also exercised by the targeted durable reader/GC history.
- The final focused suite passes 20 tests in Debug and ReleaseFast, including
  real-Parquet wrapped cloud publication, checkpoints, map loss recovery,
  backend identity rotation, packed-coordinate/tie/cursor correctness, original
  posting reclamation and locked-store cleanup. No failures or leaks.
- The standalone object-store suite passes 91 tests; its two real-provider
  integration tests remain intentionally disabled. GCS wire pagination and
  cancellation, exact selectors and retention proof parsing are covered.
- The complete standalone server builds. All 13 selected real Parquet/Iceberg
  E2Es pass against the final server (284.71 seconds), covering source
  append/replacement/removal/reintroduction, rare-query admission,
  metadata conjunctions and sorted cursors, accepted text/dense/sparse/hybrid
  cuts, cold restart, hidden highlights and 31-candidate score transforms.
- Authored embedded and Apache source boundaries, changed Python syntax,
  source license headers and Git whitespace checks pass.

These are functional and bounded-work qualifications. Live-provider retention
policy enforcement and representative archive-scale latency/RSS/request-count
benchmarks are deferred, as requested. Certificate-based audit bypass remains
opt-in and defaults off.

### Composed ranked pages with lower-bound cardinality

Publication traversal can prove a top-k ordered window without visiting every
matching posting, so a ranked leaf total may be `gte`. Composition must consume
that full window without requiring an archive-wide exact count. Keep unknown
cardinality separate from catalog binding and serving-token validation. Do not
sum fluctuating full/remaining lower bounds or infer exhaustion from them. A short
unknown-count page must carry exact exhaustion evidence; otherwise fail closed.
Explicit counts and bounded replacement lookups retain exactness requirements.
Version 3 continuations retain the independent identity binding; version 2 exact
continuations remain readable. Recheck exhaustion after refill before reading a
candidate from a terminal empty page. Regression coverage must include changing
bounds, full and remaining terminal counts, identity changes, legacy cursors,
masked overlays and million-row relations with bounded page work.


### Durable native cuts and local cache admission

Repeated composed searches must not exhaust a local generation-cache limit merely
because their retained capabilities remain valid. Evict idle copies under the
parent guard and exclusive per-cut lease only after a repository adapter confirms
a matching durable manifest and its authority/retention horizons. Keep active
readers, the requested generation and unpublished/undurable copies protected.
Recompute shared physical extents after eviction before granting the byte budget.
Never delete durable cursor authority to make room for a local cache. Recovery
reads the same remote generation; it cannot recapture current native rows.


### PR #1112: incremental cloud maps and compact serving navigation

Exact-generation map completeness is independent of retention certificates.
Fresh builds journal every immutable upload; durable checkpoints preserve selector
completeness without asserting a publication certificate. Complete inherited maps rewrite only
changed radix paths even when `qualified_retention` is false. Ordinary required
reads and reuse existence audits stay authoritative. Incomplete legacy/bootstrap
maps and qualified proof renewal retain a live-closure census. Its payload
verification is charged before I/O, including providers without native SHA-256
metadata, so the 512 MiB read bound covers streamed verification too. A bound
failure retains an incomplete map and ordinary required-read semantics.

Implicitly ordered window mergers always use original file identity/rank, including
baseline Boolean queries and navigation-budget fallbacks. Physical packed offsets
never decide global equal-score winners. The local tie callback precomputes a
compact file ordinal per source span during admission; its hot comparison does a
span lookup and original-rank translation without hashing file names or searching
the file table. Eligible leading `_score DESC` orders use the same conservative
global frontier as implicit score order. Equal bounds remain admissible for typed
secondary keys; backward, ascending, exact-count and distributed-statistic shapes
keep their complete traversal contracts.

Two levels of bounded size-based compaction combine eight small first-level packs
into up to 64 source spans per historical run. Only final runs publish directory,
ownership and serving-statistics entries; intermediate immutable data remain
attempt-scoped orphans for ordinary GC. Closed runs stay untouched on small
appends, while explicit per-level tails permit subsequent consolidation. Native
text metadata stays v11; reader protocol 38 protects the larger tail bound.


Refinement qualification on Zig 0.17.0/macOS ARM64 with `origin/main` at
`0bbd1807c1`: full Debug lake integration passes 424 tests (158 shared and 266
server tests), and all 23 focused contracts pass in Debug and ReleaseFast without
failures or leaks. The final Debug server/focused build passes 85/85 steps; the
optimized focused build passes 50/50 steps. The focused optimized compiler owner
peaked near 10 GB, so its declared RSS reservation now includes measured headroom
instead of requiring a second compilation. Object-store contracts, Apache source
boundaries, license headers, changed-source formatting/Python syntax and Git
whitespace checks pass. All 13 selected real Parquet/Iceberg E2Es pass in 275.89
seconds. The fragmented-file fixture has 193 files, guaranteeing an unrelated
serving descriptor even after 64-span packing. These checks establish result
contracts and bounded work; they do not claim archive-scale latency improvements
or live-provider retention qualification.

### PR #1112: checkpoint lineage, concurrent map journals, and GC-backed vacuum

Reader protocol 39 adds a resumable text checkpoint baseline and completed-file
changes. The baseline is an authenticated canonical root with its packed serving
root, rather than a list of original posting references. Checkpoints retain only
newly completed manifests and explicit removed-file identities beside that base
and the durable signed DF runs. Recovery and GC traverse the packed baseline as
maintenance metadata, while new manifests retain their pending posting payloads.
The source digest comparison against the baseline remains independent of which
files were completed before interruption. Removed identities keep recovery from
subtracting already checkpointed DF contributions twice.

Final publication updates canonical directories, term directories, collection
counters, serving ownership, and packs from the same changed-file set. Resuming a
checkpoint reuses baseline directory roots and statistics; completed new files
still enter serving compaction even though they need no further source scan.
Compaction receives the delta directly and uses ownership point seeks for affected
packs, eliminating its two full file-reference inventory reloads. Source planning
still enumerates the source inventory and compact canonical file records once;
this does not claim constant-time source discovery.

The cloud writer snapshots immutable map roots under a short bookkeeping lock.
Remote lookups and retention proofs run outside it and can overlap across shared
scheduler lanes. Flushes detach bounded batches, perform remote COW writes outside
the bookkeeping lock, and install the new root atomically. Waiting flushers use an
I/O mutex. A journal admission limit of 256 pending records applies backpressure;
the existing 64 MiB managed build allocator is shared through a locked allocator.
Publication/checkpoint barriers join active capture operations before flushing.
A failed flush disables completeness rather than publishing a partial journal as
complete. Query map caches continue to own their decoded navigation separately.

Completed cloud GC persists its authenticated paged mark set and upload floor/cutoff
in the matching lifecycle store binding. Subsequent collections retain those mark
pages as bookkeeping, without treating their stored IDs as payload roots. A new
publication can therefore remove old, unmarked generation-map records without
probing every payload or repeating a closure census. Records outside the proven
upload range remain untouched. Vacuum visits at most four radix leaves per pass,
with bounded map/mark reads, and persists its hash-order cursor in the publication
and builder checkpoints. It carries an unfinished cursor across newer GC cuts so
frequent collections cannot repeatedly starve the tail. Changed paths are COW;
older reader publications continue using their own immutable generation roots.
Interrupted optional vacuum work leaves the prior map and cursor intact. Cloud GC
work/frontier artifacts become ordinary later collection candidates; only the
current vacuum evidence pages remain rooted. Live provider retention qualification
remains deferred, as previously requested.

Qualification for these refinements on Zig 0.17.0/macOS ARM64, with latest main
`0bbd1807c1` included: the final Debug server/integration/focused build passes all
95 steps, with 427 integration executions (158 shared and 269 server) and 26
focused executions (one shared and 25 server), without failures or leaks. The
same 26 focused contracts pass in ReleaseFast. The real-Parquet scripted-provider
flow interrupts and resumes both an append and an append/removal after reclaiming
original postings, and checks exact collection/DF counts. Delta compaction also
runs with canonical file inventories unavailable. Scripted cloud tests verify
concurrent retention proofs, multi-pass vacuum, old-reader selectors, cancellation,
and completed GC evidence across restart and a later collection. All 14 selected
real Parquet/Iceberg E2Es pass in 282.74 seconds. Object-store contracts, Apache
source boundaries, license headers, changed-source formatting and Git whitespace
checks pass. No archive-scale latency or live-provider qualification is claimed.

## Paged checkpoints, reusable recent text, batched highlights and parallel map writes

Reader protocol 40 fences workers before publication of paged build checkpoints.
Completed file deltas and removed baseline identities live in authenticated COW
file trees; checkpoint roots contain only tree references/counts, the published
baseline reference and bounded signed DF runs. Legacy inline checkpoints remain
recoverable. Explicit materializing maintenance loaders expand paged checkpoints
into their existing plain manifest/group view; build recovery and GC keep the
paged representation and charge tree reads to their operation budget. New builds save their first completed file immediately, then batch
up to 64 completed files or five seconds at file boundaries. A checkpoint saves
its inventory and DF cut together. A crash can replay the unsaved tail; queries
continue to use the previous complete publication. Removing a file from an
interrupted delta prunes its pending inventory and subtracts its DF contribution
together. GC follows the pending file tree as posting owners and the baseline
as canonical serving/maintenance ownership. The 16,384-file regression keeps the
root under 16 KiB, resumes the paged inventory and verifies that replacement and
removal preserve the earlier immutable cut.

Accepted-WAL native text snapshots are now reused by the server. Cache keys bind
credential/store identity, table identity and object generation, source and
baseline snapshot, schema, index name/config and
both ends of the immutable WAL range. Four admission lanes single-flight equal
cuts. Cached snapshots retain no request metadata or authority; callers supply
current analysis/schema metadata and private visibility masks. A 256 MiB shared
heap accounts for retained snapshots, concurrent builders and retired snapshots
still pinned by requests. Each build keeps its existing 64 MiB limit. Saturation
uses a bounded private build charged to the same process resource manager;
request cancellation never invalidates another request's immutable corpus.
The cache joins the full-text residency accounting owner during lake preparation
and closes after requests have quiesced. Physical shared cells now remember their
own allocator, so private visibility cells can safely reference an immutable
corpus allocated by a different owner. Tests cover concurrent equal-cut admission,
eviction with live pins, cache pressure, different coverage cuts, reset WAL generations and legacy baseline snapshots, cancellation,
foreign allocator destruction and private masks.

Stored-source highlights hydrate final winners in physical segment/document
order using a compact permutation, while returned hits retain ranking order.
Paged winners reopen each selected immutable segment once; smaller admitted
corpora reuse their existing pin. A caller-owned stored decoder retains one
block, so winners in that block share decompression. Hidden JSON input is released
after each snippet. Native identity checks, original source projection, the
publication reader lease and cancellation remain required. Existing real-file
highlight cases cover hidden source fields, pagination, packed sources and
restart; native decoder ownership tests assert one decode across repeated rows
of the same block and release both physical and private allocation budgets.

Generation-map updates partition changed records once and prepare independent
root children through the shared I/O scheduler. At most eight child tasks are
outstanding per writer; deeper radix work stays inline. Scheduler saturation
executes required work inline. Every task is joined on success, failure or
cancellation before partition buffers or page arenas are freed. Only the
coordinated root is installed after all children succeed. The provider regression
requires two sibling writes to enter before either may finish, then verifies all
exact generation selectors under the resulting root. Scripted providers now
serialize their internal object/version maps while allowing remote-operation
hooks to overlap. No live-provider throughput or archive-scale latency result is
claimed by these operation-count and ownership regressions.

Qualification on Zig 0.17.0/macOS ARM64 with main `832c9d6659` merged: the
final Debug build passes all 95 steps, including the server, 432 lake integration
executions (159 shared and 273 server), and 31 focused executions (two shared and
29 server), without failures or leaks. The same 31 focused contracts pass in
ReleaseFast (50/50 steps). All 15 selected real Parquet/Iceberg E2Es pass on the
final server in 281.32 seconds, covering small and paged stored highlights,
accepted text/dense/sparse/hybrid cuts, incremental refresh, transforms and
restart. The reset-generation regression uses analyzed text search to verify
that identical LSNs and row keys cannot reuse older accepted text. Object-store
contracts pass 91 tests with two live-provider cases intentionally skipped.
Apache boundaries (2,036 sources), license headers, changed-source formatting
and Git whitespace checks pass. Required cache GC ran; an additional 24.5 GiB of
unused old server/test artifacts was safely evicted from this worktree, preserving
active outputs and restoring E2E disk headroom without overriding safeguards.
Live-provider retention qualification and archive-scale benchmarking remain
previously deferred; these checks establish correctness and bounded/reused work.

## Immutable accepted cuts and ordered source routing (protocol 41)

Accepted search reads now pin and validate the WAL floor and immutable tail before
looking up server-owned normalized cut state. Four descriptor lanes share a
256 MiB process-accounted budget; each cut has a 128 MiB construction/retention
bound. Cache keys bind the backend identity, table/object generation, publication,
schema, queue namespace, base coverage and immutable tail record. Hits perform
only the floor/tail reads. A later tail extends the nearest cached cut only after
checking its immutable parent record, reading and parsing the new suffix. Cuts
retain immutable earlier images through reference-counted owners, with a maximum
of eight extensions before a full rebuild. Eviction cannot invalidate active
readers. Pressure uses a bounded private build; no partial cut becomes visible.

The same owner single-flights a publication-specific compressed physical
replacement mask. It retains data, never a request context, reader lease or
provider client. Every waiter checks its own deadline; errors discard partial
mask allocations. Both text and vector execution borrow the mask while their
cut pin remains live. Retained descriptors still persist their normalized images
and remain usable after restart, independently of this optional process cache.

Native text v11 serving roots now carry a COW inverse source-route tree under
reader protocol 41. A key contains the source-file identity and original live
row rank; its value authenticates the serving segment and physical-hole manifest.
Ordered row consumers seek a predecessor after translating physical coordinates
through that manifest. This avoids enumerating all serving packs for a large
source file. Incremental compaction updates only affected spans. A one-time
metadata bootstrap upgrades older packed roots. GC, closure auditing and bounded
startup navigation warming include the new tree; older retained cuts remain
readable.

Eligible ordered text queries stream bounded batches from an index with proven
complete tuple/public-ID ordering. Each batch admits only the serving packs
containing its rows and lowers those rows into native ordinal constraints under
the same pinned statistics and source authority. A full top-K proves early stop.
Backward pages use the reverse tuple order. Incomplete ordering or an unproductive
bounded prefix returns to bounded text windows. Proved segment subsets have their
own byte/count-bounded cursor: neither a hard-coded 64-segment shortcut nor a
small percentage of the archive can force complete subset admission or widen the
selection back to unrelated packs. A small requested page does not
require full text-corpus admission. Accepted overlays and full-match evaluation
retain complete window traversal. Early-stopped totals are explicit lower bounds.

Published-only count requests whose lowered membership is exactly one positive
term (including a one-token analyzed match) use authenticated global document
frequency. Exclusions, metadata/authorization filters, private masks, accepted
rows, distributed statistics and cursor/ordering requests retain normal exact
membership execution. Profiles identify the shortcut as
`native_text_metadata_count`; these queries admit no posting readers.

Stored highlights prepare query matchers once per page and consume one borrowed
parsed source at a time. Stored-block batching remains independent of public hit
order. Regex matchers retain a stable allocator owner for lazy DFA construction;
only final snippets transfer to the result allocator.

Operators can tune `lake_indexes.text_cache.admission_window_segments` (1–64,
default 64) and `admission_window_bytes` (1–32 MiB, default 32 MiB) downward to
reduce admission granularity. A single immutable serving segment is indivisible.
These settings control execution windows, not result or candidate limits. Real
Parquet and Iceberg E2Es set the segment window to one, preserving small independent
fixtures while forcing multiple serving windows after compaction. They assert the
selected bounded plan, stored hidden-source highlights, metadata counts, ordered
forward/backward pages and retained restart results. This supersedes the earlier
67-source-file test's insufficient inference that file count guaranteed windowing.


Qualification for the protocol-41 refinements on Zig 0.17.0/macOS ARM64, with
latest main `77b50e6adb` merged and runtime code at `a180dcbd73`: all 95 Debug
build steps pass, including the server, 434 lake integration executions (160
shared and 274 server), and 33 focused executions (three shared and 30 server).
All 33 focused executions also pass in ReleaseFast (50/50 steps). Both modes
report no failures or leaks. All 15 selected real Parquet/Iceberg E2Es pass on
the final server in 272.90 seconds (31 unrelated cases deselected), including
bounded-window plan assertions after compaction, hidden stored highlights,
metadata counts, ordered forward/backward pages, accepted text/vector/hybrid
cuts and restart. Apache boundaries (2,036 sources), license headers, 69 changed
Zig files, Python formatting/name checks and Git whitespace checks pass.

Required cache GC preserved active worktrees and reached package-cache pruning;
a 55-second watchdog stopped its existing uv cache-lock wait. Only unused,
superseded outputs in this worktree were separately evicted to restore disk
headroom; the E2E disk safeguard remained enabled. Live-provider retention
qualification remains deferred and certificate enablement defaults off.
Archive-scale latency/RSS measurements remain outstanding; these regressions
establish correct results, bounded admission and reduced repeated work, not a
measured speedup for a production archive.

## Shared run owners and byte-targeted packs (protocol 42)

Cloud GC asks the backend whether version operations are available, independently
of whether bucket versioning is currently enabled. Known R2 endpoints use ordinary
name enumeration and deletion. Other S3-compatible endpoints prove API support;
explicit unsupported responses permit the unversioned path, while authorization,
transport and malformed-response errors do not downgrade it. A GC operation caches
that decision. S3 versioned collection still requires permission to list versions.
No bucket versioning or retention policy is changed by this capability check.

Normalized pending cuts now carry an immutable identity derived from their pinned
WAL head and base coverage. Retained descriptors preserve that identity. Legacy
images derive a streamed content digest without allocating another encoded suffix.
Recent-text keys and fresh search snapshot hashes include the cut identity, so a
same-generation restored queue cannot alias an earlier cut merely by reusing LSNs.

Recent text is partitioned by ordered stable-key prefix. Each immutable run owns
its allocator and native snapshot; a new cut retains matching runs directly and
rebuilds changed prefixes. This preserves the original logical key order, complete
BM25 statistics and private accepted-row visibility. Run references do not retain
ancestor cuts, so repeated updates need no periodic full rebuild to truncate an
ownership chain. The existing 256 MiB process-accounted cache budget covers run
owners, payloads and cut metadata. Cut construction and retained-run admission are
bounded separately; saturation retains the bounded private-build fallback. The
regression checks twenty successive one-prefix replacements, old-cut results and
active-reader survival after cache eviction.

Cloud generation bindings share authenticated encoded and decoded navigation
through a data-only process pool. Eight backend/credential scopes share 64 MiB,
including owner allocations; an active scope has 128 LRU page slots and may use
available capacity up to that shared ceiling. Exact artifact and provider-generation
selectors identify each page. Clients, roots, permissions, request deadlines and
reader leases remain operation-owned. Cache hits never widen a complete binding.
Pinned owners survive eviction, and pressure evicts before optional decoding or
returns an uncached authenticated result. Query, builder, recent-vector and GC
bindings can reuse these pages. Scripted-provider tests prove reuse across reader
release, isolation between scopes, pinned eviction and the pressure fallback.

One lookahead task now prepares an admitted native window as well as compact
navigation. It transfers a pinned archive source to the consuming window, where
normal WAL masks, global statistics and query authority still apply. The shared
scheduler reserves 16 MiB of planning work; actual inventory, segment and resource
admission retain their existing limits. At most one future window is prepared.
Saturation executes the required pull inline. Successful transfer, cancellation
and query exit join the task and release every unconsumed pin before publication
and source leases end. This extends the existing parallel opening inside a window
by overlapping the next window's preparation with current-window scoring.

Serving compaction now has four bounded eight-way tiers. Tiny files can share a
pack until its byte/metadata target or 4,096-source safety bound is reached, while
no merge opens more than eight posting inputs. Packs target at most 16 MiB of
input admission and 1 MiB of estimated source metadata; indivisible prior inputs
retain their established admission contract. Only small tails with less than
512 KiB of source metadata remain open for future packing. Source spans beyond the small inline
case live in authenticated pages of at most 64 spans and 64 KiB. Compact routing
stores page edges; only admitted packs expand their spans. Physical inverse seeks,
logical ties, hidden highlights, incremental ownership and stored source mapping
use the same validated coordinates. GC and reuse audits retain the span pages and
physical dependencies, with page reads charged to the operation's budget.

Reader protocol 42 fences the paged layout; a layout marker upgrades older serving
roots while retained earlier roots remain readable. The 128-file native regression
now produces one paged pack and verifies reclamation, restart, inverse routing,
append/removal/replacement and preserved prior pack bytes. Real Parquet and Iceberg
fixtures use 131 independent files and a one-segment admission window, exercising
both paged spans and multiple actual serving windows after compaction. Archive-scale
latency/RSS benchmarking and live-provider retention qualification remain deferred;
no measured archive speedup is claimed by these bounded-work regressions.

Admission planning counts span pages, descriptors and manifests as well as native
posting bytes, with a conservative fourfold allowance for decoded metadata and
coordinate maps. This cost is available from authenticated navigation alone;
planning never fetches source-span pages. New serving roots maintain the aggregate
cost incrementally. Older roots without that counter use bounded windows instead
of the small-corpus shortcut until a fresh serving layout is published.

Qualification for code head `99b054525a` (including `origin/main` at `7aa031f49d`):

- Debug server and lake suites: 95/95 build steps; 437 integration and 36 focused
  test executions passed, with no failures or leaks.
- ReleaseFast focused suite: 51/51 build steps; 36 test executions passed, with
  no failures or leaks.
- Object-store suite: 92 passed; two live-provider tests skipped. Provider code is
  unchanged from the separately qualified refinement commit `8f4e4ee042`.
- Real Parquet/Iceberg E2Es: all 15 selected tests passed (31 deselected) in
  310.93 seconds, including the 131-file paged-pack/bounded-window fixtures.
- Apache dependency boundary verified 2,037 sources; license headers, formatting
  of 69 changed Zig files, Python formatting/name checks and Git whitespace passed.

These results qualify correctness and bounded execution. Provider retention
policies and archive-scale throughput/latency/RSS remain separate qualification.

Post-qualification cleanup ran `make gc`; its Zig/Rust/Go phases completed before
its uv cache-prune phase reached the 55-second watchdog. Completed fixture output
was removed from the worktree; qualification logs remain in `/private/tmp`.


## Partition-owned accepted cuts and selective tail maintenance (protocol 43)

Normalized accepted cuts retain independently owned immutable row partitions.
Only prefixes touched by the authenticated WAL suffix are cloned and normalized;
unchanged final rows, sorted identities and text digests are shared directly.
The new cut owns its head and partition references, with no ancestor-cut chain or
periodic whole-image normalization. Existing 65,536-change and process-cache
bounds remain. Retained descriptors serialize the same flat `changes` wire view;
SQL materializes that view only when needed and clones maps before normalization.

Each row partition caches its publication-specific physical replacement mask.
Body-only updates share that mask through an independent owner; no old row
partition is retained solely to keep a mask alive. Its key filter is built only
when that partition changes. A new cut unions the
compressed masks without repeating unchanged index probes. Optional mask caches
have a 512 KiB per-partition ceiling; pressure retains the bounded direct path.
The WAL cache remains process-accounted at 256 MiB, with 128 MiB image admission
and the existing bounded construction workspace.

Recent dense/sparse indexes use at most sixteen immutable stable-prefix runs.
Content-and-recipe-keyed durable jobs reuse unchanged runs across accepted cuts;
changed runs alone invoke enrichment and rebuild native generations. Exact cloud
selectors and run declarations travel with retained snapshots. A run's expiry
must cover the requesting cut; renewal retains the existing rebuild/claim fence.
Queries rank one run at a time, release its runtime, and merge bounded candidate
windows. Winning chunk delivery can reopen the exact originating run. Empty and
legacy cuts retain their existing complete-corpus path. The real Iceberg fixture
checks unchanged run artifacts after another accepted update and then cold restart.

Shared cloud navigation coalesces up to 32 distinct cold page flights per scope.
Waiters suspend through their operation I/O context, check their own cancellation,
and retry under their own authority after a failed/canceled leader. No client,
root, deadline or lease is stored in shared flight state. Saturation and callers
without asynchronous I/O retain authenticated direct reads. Successful flights
publish into the existing bounded decoded-page cache before releasing waiters.

Compact tail descriptors identify tiers without opening historical manifests.
A new or changed input activates only tails in its own tier; promotion activates
the next tier when required. Other tiers retain their original manifests, source
pages, term contributions and ownership routes. Legacy roots initialize this
metadata during their next delta publication. The regression makes an unrelated
tail manifest unavailable during an append under a fresh upload attempt, proving
that append maintenance neither reads nor republishes it.

Retained token production and query admission share provider-generation bounds.
Exact-version tokens support the full 4,096-byte selector limit while malformed
or oversized envelopes remain rejected. GC union bindings complete fallible
allocation before acquiring shared navigation references; allocation-failure
coverage verifies that every acquired reference is released.

Protocol 43 fences the new root/retained-run fields. Live-provider retention
qualification and archive-scale latency/RSS benchmarking remain separate; these
changes make no measured archive-speedup claim.


Qualification of the protocol-43 refinement at code head `8308732aec`, including
`origin/main` at `2c8240e703`:

- Merged Debug server: 49/49 build steps succeeded. Full Debug lake qualification:
  95/95 steps; all 439 integration and 38 focused executions passed, without
  failures or leaks.
- ReleaseFast focused suite: 51/51 build steps; all 38 executions passed without
  failures or leaks.
- Real Parquet/Iceberg E2Es: eleven archive/index cases passed against the merged
  server. After correcting the checkpoint and holding catalog publication fixed
  for the reuse assertion, all four accepted-cut cases passed in 61.75 seconds
  (seven deselected), giving fifteen distinct selected cases with passing results.
  The fixture explicitly verifies that both updates use the same publication;
  background draining may otherwise legitimately move earlier rows into the
  archive. Pinned replay and restart remain covered.
- Apache dependency boundary verified 2,037 sources; license headers, formatting
  of all 73 changed Zig files, Python formatting/name checks and Git whitespace
  passed. Object-store provider code is unchanged from its separately qualified
  92-pass/two-live-provider-skip run.

Post-qualification `make gc` preserved active worktree caches and completed its
Zig/Rust/Go phases; uv cache pruning reached the 55-second watchdog and its own
process group was stopped. Completed E2E fixture output was removed; logs remain
in `/private/tmp`.


## Reusable retained state and exact recent delivery (protocol 44)

Accepted images use the ingestion layer's canonical stable-key identity, including
numeric equivalence such as negative and positive zero. Retained pending data
records its key encoding explicitly; legacy descriptors keep their original
JSON-array identities so existing tokens replay the same opaque row IDs. Upserts
and deletes use one identity function throughout warm image maintenance and cold
reconstruction.

Final unit/chunk hydration resolves the exact declared artifact for the winning
row's stable-prefix run. Equal index names in different recent runs cannot alias.
Delivery borrows an already-open matching runtime or acquires and releases one
exact runtime, retaining the query's provider selectors and authority.

Archive file maps are keyed by immutable publication and schema, independently
of the accepted WAL head. Selection, permissions, source coverage and reader
leases are still checked on every query. Recent vector preparation uses borrowed
immutable row-prefix views and a bounded ready-job metadata memo; hits avoid
copying row maps or fetching ready jobs. Entries are scoped to store, table
incarnation, recipe, archive and cut, and must outlive the entire new cursor.
Renewal and misses retain the durable claim/CAS path.

Retained tokens reference a fresh small lease envelope containing their current
reader token and full configured expiry. Envelopes share an immutable state
object and at most sixteen independently reusable row-group objects. Modern WAL
images supply cached partition digests, so unchanged rows need neither walking
nor serialization on warm queries. A changed cut rewrites only changed groups
and its compact state. Payloads expire after 24 hours; reuse requires retention
past the new envelope expiry. Expiring envelopes therefore cannot collect data
still referenced by a later envelope. State and row groups use the existing
retained-cut GC domain and bounded collector, without another reachability graph.

The process memo stores only copied artifact references, under a 4 MiB/512-entry
bound; vector job metadata has a 32 MiB/128-entry bound. Both attach to the process
resource manager, support eviction, and retain no clients or authority contexts.
A restart reads the exact durable state and groups. Cold group reads use the
shared I/O scheduler with four concurrent reads, memory reservations and an
inline fallback under saturation. Combined encoded state remains bounded by
40 MiB and 65,536 changes; provider version selectors and checksums are verified
for every object. Missing nested objects invalidate the cut consistently.

Native regressions cover canonical upsert/delete and replay, exact multi-run
hydration, zero-copy views, memo eviction/expiry, fresh lease/state reuse,
changed-group reuse, restart, legacy descriptors and collection. The real
Iceberg fixture checks fresh envelope sharing, unchanged row groups and vector
runs after a new accepted update, plus pinned replay across restart.

Protocol 44 fences the new retained envelope and key-encoding fields. Provider
retention qualification remains deferred and disabled by default. These changes
remove repeated work; archive-scale throughput, latency and RSS remain separate
benchmark qualification.


Protocol-44 qualification, including `origin/main` at `2c8240e703`:

- Debug server: 49/49 build steps; full Debug lake suites: 95/95 steps,
  442 integration and 44 focused test executions, without failures or leaks.
- ReleaseFast focused and object-store suites: 54/54 build steps; all 44 focused
  executions and 92 object-store tests passed, with two live-provider skips.
- Real Parquet/Iceberg E2Es: eleven archive/index cases passed in the full
  selection. All four accepted-cut variants passed the corrected fixture in
  55.99 seconds (seven deselected), including state/group reuse, full fresh
  cursor expiry and restart. All fifteen distinct selected cases have passing
  results. Retained groups include tombstones; vector runs include live rows,
  so the fixture asserts one changed group rather than equating those counts.
- Apache boundary verified 2,037 sources. License headers, formatting of all
  74 changed Zig files, Python formatting/name checks and Git whitespace passed.
- Refetched main remained `2c8240e703`; the branch already contains that commit.

The repository cache collector preserved this worktree's active cache and
completed its Zig/Rust/Go phases before uv pruning reached the 55-second
watchdog. Obsolete test/server/library output in this worktree was pruned to
restore the E2E suite's production disk-admission headroom. No other worktree's
active outputs were changed.


### Retention horizons, decoded cursor reuse and predicate-led admission

The next refinement retains protocol 44's immutable wire format. State memo
expiry now inherits the minimum retention scope of its row groups. A new cursor
that exceeds an unchanged group's lifetime renews that group and the root.
Concurrent root and row-group construction shares bounded (32-slot) in-process
flights; waiters recheck their own requested expiry after joining. Failure and
cancellation release construction admission. Saturation bypasses coalescing.
Every fresh cut capture continues to issue its own small envelope.

Pinned cursor pagination uses the existing resource-accounted decoded-cache
singleflight and leases, sharing decoded state and the accepted overlay map.
Its key includes store, table incarnation and the exact provider-qualified state
reference. Each page still reads its own envelope and validates permissions,
recipe, expiry, transitive retention scopes and current reader/source authority.
No request pointer, provider client or reader lease is cached. Legacy row identity
encoding stays unchanged. Cold misses retain four-way bounded group reads.

A selective required metadata conjunct can now drive pack admission before text
ranking, including when a common text term cannot fill K after filtering. The
optional planner tries native compound and column predicates, bounds candidate
cardinality to 4,096 physical rows, and seeks paged inverse source/rank routes.
It neither scans Parquet to plan admission nor expands broad predicates into
scalar IDs. Full predicate membership, global BM25, accepted-WAL masking,
residual OR/NOT, result delivery and cursor behavior remain authoritative.
Profiles identify this path as `indexed_predicate_text_admission`.

Regressions cover earlier retained groups followed by a longer cursor, cached
transitive-expiry rejection, warm sharing across independent reader envelopes,
deadline and expiration fencing, residency pressure, eight concurrent first-cut
publications, canceled waiters, failed-leader retry and flight saturation. The
real Parquet/Iceberg fragmented fixture now checks metadata-driven admission,
including restart and successful common-term queries with unrelated serving
descriptors physically removed.

Qualification on `origin/main` at `4838edd96171b020eb865e62f16e027551d02a79`:

- Final Debug server and lake suites: 95/95 build steps; all 444 integration
  and 46 focused test executions passed, without failures or leaks.
- ReleaseFast focused and object-store suites: 54/54 build steps; all 46
  focused executions and 92 object-store tests passed, with two live-provider
  skips.
- Real Parquet/Iceberg selection: 15 passed, 31 deselected in 329.00 seconds.
  The final-binary selective-admission and accepted-hybrid checks both passed
  again (two passed, nine deselected in 34.13 seconds).
- The merged checkpoint fixture uses matching declaration/artifact names and
  real distinct scoped artifact IDs. Its targeted native rerun passed.
- Apache dependency boundary verified 2,037 sources. License headers, formatting
  of all 74 changed Zig files and both Python files, Python name checks and Git
  whitespace passed.
- Final fetch confirmed the same main revision is included.

The repository cache collector ran with its bounded watchdog. Obsolete binary
variants were reclaimed only in this worktree; two stale object-store cache
manifests were invalidated and the optimized suite then passed. Other active
worktrees were preserved. Provider retention qualification remains deferred.


## Native retained payloads and compressed predicate routing (protocol 45)

Retained replay keeps query admission independent of the decoded cache's 64 MiB
residency and active-load limits. A valid legacy JSON cut which cannot use that
cache retries in a private, process-accounted lease with a bounded 256 MiB decode
workspace. Cancellation and each fresh reader envelope remain authoritative;
private leases release their complete workspace with the query. The encoded
40 MiB and 65,536-change limits remain enforced.

New retained row groups use a versioned native value payload. Container counts
allow one allocation per array/map, primitive arrays share their type tag, and
integer lanes use exact zigzag varints. Float bits, negative zero, exact numeric
tokens, heterogeneous arrays, object field order, and legacy stable-key encodings
are preserved. Metadata remains a small JSON state/envelope; v1 JSON row groups
and old descriptor tokens still replay. Prefix-group digests and unchanged-group
reuse keep their existing immutable ownership. Native WAL partition construction
now uses the shared typed-value clone instead of serializing and parsing rows.
A 125,000-integer regression row keeps the new image and native replay below
16 MiB each. Legacy replay also succeeds under an explicitly constrained 32 MiB
active-load policy, using a private bounded lease. The test avoids depending on
allocator remapping or build-specific parser peaks. This is a bounded fixture
measurement, not an archive-throughput claim.

Indexed predicate admission accepts broader native compressed block producers.
A selected physical bitmap is routed through authenticated source rank bounds
and pack spans using rank/select and range cardinality, including delete holes
and full u64 row coordinates. It never expands broad matches into scalar row-ID
lists. Optional proofs cap files, route spans, block work, reads, and memory;
near-full pack admission falls back to the existing bounded ranking windows.
The complete residual predicate and publication-wide text statistics remain in
force. Roaring intersections remove empty containers so the shared emptiness
invariant remains true after sparse or dense intersections. Point producers
retain their selective admission bound.

Publication inventories record the exact provider selectors used by their native
file identities. A refresh shares one authenticated previous inventory among
builders; eligible ordinary text refreshes derive their private file plan from
it and seek only removed/replaced native file records. Unchanged native catalogs
are inherited through immutable directory roots. Aggregate read admission covers
all changed-record seeks. Broad replacements use a bounded sequential catalog
walk instead of repeated point seeks. Native roots persist a delete-free inventory proof. Legacy roots/inventories
without that proof or version evidence, source or schema mismatches,
delete-bearing prior/current snapshots, and partial checkpoint recovery
retain full reconciliation. Compact source inventory decoding and fingerprinting
remain linear in file count; this removes the separate native catalog census,
not all source metadata work. Required closure audits remain independent of this
planning optimization and require qualified provider retention to bypass.

Regressions cover oversized legacy replay without residency, native payload
round trips and malformed counts, broad compressed routing over holes/high row
bits, a 4,096-file inventory with a two-file delta under a 256 KiB planning budget,
and a 10,000-match real-Parquet predicate compared with authoritative ranking.
Live-provider retention qualification remains deferred and disabled by default.


### Protocol 45 qualification on merged main

Qualified with `origin/main` at `c102cbeac5` (storage merge/reopen fixes, PR
#1131). The merge preserved native shared-segment allocator ownership.

- Full Debug lake build: 61/61 steps; 162 shared plus 287 server integration
  tests passed (449 total), alongside 51 focused tests.
- ReleaseFast: 54/54 steps; 51 focused tests and object-store checks passed.
  The two credentialed live-provider checks remain skipped.
- Fifteen unique real Parquet/Iceberg E2E gates qualified with both `lake` and
  `iceberg` extras and `ANTFLY_E2E_FULL_LAKE=1`: 13 unchanged gates from the full
  selection, plus both final file-layout pruning cases rerun (2/2 in 23.63s).
  The pruning fixture uses 73 = 64 + 8 + 1 source files with rare matches in two
  sources, guaranteeing an unrelated descriptor under four-tier compaction
  regardless of immutable file-hash order.
- Standalone Roaring: 38/38; native typed-value codec: 2/2. Zig formatting,
  Python formatting/naming, whitespace, Apache product boundary (2,037 source
  files), and license-header checks passed.

The retained replay regression explicitly constrains active-load admission to
32 MiB and verifies the private 256 MiB lease, preserving the default-cache replay
check without relying on allocator-specific parser peaks. New image and native
replay footprints remain below 16 MiB for the 125,000-integer fixture. These are
bounded correctness/workspace checks; archive-scale latency/RSS measurement and
real-provider retention qualification remain deferred.

### Protocol 46: paged file identities, delete-aware deltas, and shared replay groups

Publication-bound source navigation now persists authenticated file proofs and
public/private identity lookup trees. Search opens the small navigation root and
resolves reached files through bounded pages. A request retains at most 512 memo
entries within 4 MiB, a 512 KiB page cache, and bounded lookup scratch; it no longer
constructs three archive-sized maps on a cold query. Immutable decoded navigation
pages share the bounded process decode cache, so broad filters do not repeatedly
decode the same lookup pages or charge cached pages as new I/O. Fresh coverage,
source/schema/snapshot binding, cancellation, and reader authority remain mandatory
on cache hits.
Historical publications without navigation retain their existing mapping path.

File proofs use the exact qualified object selectors, data sequence, partition
metadata, inventory deletes, and prepared per-file position/equality-delete
fingerprint used by native index construction. An authenticated set reduction on
each file-tree subtree lets refresh planning skip unchanged prior records. Changed
and removed file proofs point-seek only their corresponding native maintenance
records. The publisher computes current identities once and shares them across
native builders; delete-bearing snapshots now participate in this delta path.
Legacy or incompatible proofs retain full reconciliation. Public IDs still fence
snapshot identity, so a new snapshot bulk-builds its public reverse lookup tree.
When a prior serving term directory is disabled, its replacement bootstraps
complete positive contributions from the final serving inventory, including
untouched packs. Signed deltas without a complete base cannot establish term
absence or score bounds. This recovery reads summaries, not posting payloads.
Source discovery and fresh coverage still enumerate the current snapshot; this
change does not claim constant-time Iceberg manifest discovery or replace source
validation. It removes full prior-inventory JSON decoding for native text planning
and avoids reading unchanged prior file records when reductions qualify them.

The catalog reader protocol fences source-navigation ownership. GC marks its root
and every lookup/proof page in both complete traversal and resumable publication
expansion. Directory deduplication must not suppress navigation expansion. An
old reader's navigation remains protected until release, then becomes collectible;
the one-job-budget GC regression exercises both transitions. Checkpoints retain the
same navigation as their build. Cloud publication binding/certification includes
the navigation root and its closure.
Historical certificate preimages remain readable. Missing optional prior navigation
migrates through full construction, and coordinator readiness checks its root.

Retained search cuts now compose independently decoded immutable WAL row groups.
A separate singleflight/admission coordinator prevents nested decoded-cache waits;
bounded four-group scheduling preserves cold prefetch. Keys bind exact artifact
identity, provider generation, codec version, and retention domain. Every cut still
validates expiry, incarnation, recipe, and authority before borrowing a group.
Composite residency conservatively charges all foreign buffers it keeps alive;
uncached oversized legacy groups remain private and process-accounted. Eviction
cannot invalidate either an older cut or a neighboring cut sharing its buffers.

Recent vector partition jobs keep their reusable LSN-independent identities.
Table-level enrichment status is now published only by composition under the actual
WAL LSN, archive generation, incarnation, recipe, and cut. It reports required/ready
partition counts, failures/retry timing, and the minimum covered artifact-retention
horizon. CAS ordering prevents older archive/WAL cuts or incomplete observations
from replacing ready coverage. A recipe/incarnation-specific status key prevents
cross-generation completions from competing, and a small aggregate memo avoids
status storage operations on warm ready queries.

Live-provider retention qualification remains deferred and certificate enablement
remains off by default. Qualification on merged main `0cba59bceb`:

- Debug lake integration: 168 shared-engine and 293 server tests passed (461 total).
- Focused navigation/frontier/WAL checks: 5 shared-engine and 50 server tests passed
  (55 total), including selective delete-aware reconciliation, independent shared
  row-group owners, composed-status CAS races, resumable navigation GC, and optional
  term-directory recovery with untouched historical packs.
- Production CLI build passed. All 17 selected real Parquet/Iceberg E2Es passed in
  205.15 seconds, covering snapshot/schema evolution, position/equality deletes,
  accepted text/vector cuts, restart, broad windows, metadata filters above the
  old ID-list limit, fragmented layouts, signed timestamps, and incremental refresh.
- Zig/Python formatting, whitespace, license headers, and the Apache product
  boundary passed (2,038 source files).

These checks establish correctness and bounded work for the covered fixtures;
representative archive-scale latency and RSS benchmarks remain deferred.


## Protocol-47 refinements

- Bind cloud vacuum cursors and completion to the complete GC proof, including
  mark roots at unchanged cutoffs; legacy unbound progress restarts safely.
- Persist a portable data census digest in source navigation. Identical freshly
  authorized immutable plans skip published-inventory decoding and resolve exact
  file selectors lazily. Mutable data and delete objects retain fresh validation.
  Sources independently own their native client and retained reader handle.
- Admit prefix/wildcard/regexp/fuzzy searches through bounded windows and shared
  Boolean preparation, with segment-local compressed membership and bounded top-k
  state. Unknown pattern bounds remain conservative, preserving global scores.
- Search recent dense/sparse partitions in bounded batches on the shared scheduler,
  with isolated mutable state, join-owned reservations, explicit cancelable joins,
  deterministic merging, and serial fallback for oversized partitions.

Main is merged through `f56cf1f4c7`. Live-provider retention qualification remains
deferred; certificate audit bypass remains disabled by default. These refinements
do not claim representative archive-scale latency or RSS benchmark results.


Qualification after merging main `f56cf1f4c7`:

- Debug lake integration: 169 shared-engine and 295 server tests passed (464 total).
- Focused navigation/frontier/WAL checks: 6 shared-engine and 52 server tests
  passed (58 total), including cancelable vector joins, saturated admission,
  oversized serial retries, mixed pattern scores/cursors, complete GC proof
  changes, and real Parquet hydration after the original cursor/store close.
- The default shared-owner CLI built in Debug mode. All 46 real Parquet/Iceberg
  E2Es passed in 300.06 seconds, including immutable source qualification,
  indexed metadata conjunctions, deletes, schema evolution, retained vector cuts,
  incremental refresh, projection/highlights, embedded pruning, and restart.
- The merged shared-owner adapters remain intact. Generated native error transport
  includes the branch's cloud-map, typed-JSON, and text-preparation errors; its
  pinned-compiler generation check passed.
- Zig/Python formatting, lint, whitespace, license headers, and the Apache product
  boundary passed (2,051 source files).

Live-provider retention qualification and archive-scale latency/RSS benchmarks
remain deferred; no benchmark speedup is claimed by these correctness fixtures.


### Selective pattern navigation and coalesced discovery

Pattern queries can now prove selective archive admission from the authenticated
publication vocabulary before opening serving packs. Prefix, wildcard, regular
expression, and fuzzy expansion share the scorer's whole-term matching semantics.
The Boolean navigation plan retains complete vocabulary expansions separately
from scoring: patterns still contribute their constant boost once per matching
document, rather than a score per expanded term. Materialized prefix fields and
ordinary fallback fields contribute a conservative union. Recent WAL windows
remain independently admitted, including terms absent from the archive.

Expansion examines at most 4,096 dictionary entries, retains at most 128 terms,
and gives each matcher a 2 MiB workspace. Exceeding a bound or failing allocation
returns unknown admission, preserving complete bounded-window execution. In
particular, an automaton's allocation-induced dead state cannot prove absence.
Broad patterns and exact counts still require complete membership work; this
change targets selective patterns without claiming that all pattern queries
become sublinear.

Cold Iceberg snapshot planning now uses the decoded cache's shared construction
admission and singleflight path. Readers of the same freshly authorized metadata
share one immutable manifest/delete plan, including canonical file order and
lookup metadata. Each waiter checks its own authority and cancellation. A failed
or canceled leader releases construction ownership so another reader can retry.
Fresh catalog-pointer validation remains mandatory, and a genuinely cold new
snapshot still enumerates its manifests. No stale pointer TTL or provider client
is retained in the cache.

The coordinator also treats a missing generation-map page during directory
availability checks as recoverable derived-artifact loss, allowing its existing
fresh rebuild path to run. Provider authorization and transport failures retain
their error semantics.

Regressions cover selective and absent pattern admission, broad-expansion
fallback, automaton allocation failure, coalesced cold discovery, missing cloud
map recovery, and real Parquet pattern scores/ties against complete admission.


Qualification on merged main `f56cf1f4c7` for selective pattern navigation:

- Default shared-owner Debug CLI built successfully.
- Full Debug lake integration passed 467 tests: 171 shared-engine and 296 server.
- Focused checks passed 61 tests: 8 shared-engine and 53 server. The cold-plan
  regression also passed separately with a deterministic blocked leader and a
  confirmed second flight waiter, plus cancellation on a cached hit.
- All 46 real Parquet/Iceberg E2Es passed in 309.26 seconds. The fragmented
  Parquet test compares each pattern's scores, source rows, and ties against
  independent complete admission, alongside existing projection, highlights,
  transforms, cursor, retained-cut, and restart gates.
- Pinned-compiler native error generation, Zig/Python formatting, Python lint,
  whitespace, license headers, and the Apache product boundary passed.

Provider retention qualification and archive-scale latency/RSS measurements
remain deferred. These fixture checks establish correctness and bounded work;
they do not claim an archive-scale timing improvement.


### PR #1112: intersected score navigation, vocabulary seeks, retained views and ties

Indexed metadata pack proofs now restrict the publication-wide score frontier.
The frontier owns a sorted copy of the bounded pack identities and clips each
candidate range before reading term envelopes. Row predicates remain residual
checks under the same publication. An empty proof admits no archive packs;
recent WAL rows remain independently admitted and scored with global statistics.

Regex and fuzzy expansion now use the scorer's automaton to seek to the next
viable byte prefix in the authenticated global dictionary. Dead prefixes skip
sibling dictionary pages through the existing forward cursor. Expansion keeps
the 128-match, 4096-candidate, automaton-memory and metadata-read limits; exceeding
a limit remains unknown and uses complete bounded admission. Allocation failure
cannot become an absence proof. Wildcards retain literal-prefix seeks.

Native retained row groups build independently owned partition indexes and key
filters once, borrowing the group's already decoded row values. Each decoded cut
keeps only a small composite image view and pins its groups. Unchanged groups
share row indexes, stable IDs, fingerprints and predicate syntax across cuts;
they do not flatten into a new full row map or JSON key disjunction. Legacy cuts
retain their existing decoding path. Group cleanup releases its indexes before
its row arena, and eviction cannot invalidate a cut's foreign group leases.

Serving compaction publishes an authenticated tree of minimum logical file/rank
positions per pack. Internal reductions store the minimum over each subtree.
Appends and replacements update only changed pack paths; older publications
bootstrap this navigation from manifests/spans on their next refresh. GC and
reuse verification include these pages. Ranked admission carries an immutable
score/tie cutoff through bounded lookahead. Equal-score pruning requires a
logical-position proof under the default comparator; typed secondary ordering,
recent ties, count-only and full-match evaluation retain conservative admission.
Constant-score bounds avoid unnecessary floating-point inflation; BM25 bounds
retain their rounding margin.

Regression qualification includes large dictionaries with over 4096 irrelevant
terms, differential byte-automaton seeks, filtered competitive admission,
reversed physical/logical pack order, shared retained-partition identity and
real Parquet tie/filter comparisons before and after restart. Archive-scale
latency/RSS benchmarks and live-provider retention qualification remain deferred.


Qualification on the final refinement revision: the default shared-owner CLI
built in Debug mode; lake integration passed 172 shared-engine and 296 server
tests (468 total), focused navigation/ownership checks passed 9 shared-engine
and 53 server tests (62 total), and all 46 real Parquet/Iceberg E2Es passed in
309.55 seconds. The real-file fixture compares constant-score ties and indexed
filters with independent full admission before and after restart. Native error
transport generation, Zig formatting, Python lint/formatting, whitespace,
licenses and the Apache product boundary also passed. Fresh origin/main remained
at f56cf1f4c7 and was already included in the branch.


After that qualification, origin/main advanced to 5fdb3ea392 (#1135). The branch
merged it, retaining the shared-owner inference diagnostic entry point and
regenerating the compiler-local error transport for TestRuntimeMutexHeld. On the
merged code, the Debug CLI rebuilt successfully, all 468 lake integration tests
and 62 focused tests passed, and all 46 real Parquet/Iceberg E2Es passed in 303.59
seconds. Generated error transport and the Apache source boundary checks passed;
there are no unresolved merge conflicts. Provider retention qualification and
archive-scale performance measurements remain deferred.

### Streaming admission, exact single-occurrence ceilings and snapshot reuse

Metadata predicate admission no longer needs a complete in-memory physical row
set or a flat serving-pack list. A necessary native predicate streams compressed
posting pages into inverse source routes. A bounded external merge deduplicates
pack identities into a query-owned fixed-width disk index. The score frontier
binary-seeks this index when clipping ranges. Construction has a 16 MiB resident
budget and uses the statement spill manager's disk quota; exhausted optional
planning falls back to authoritative row filtering. The legacy selective proof
remains available for other ordering paths. No result-size limit is increased.
Physical block seeks skip unrelated groups and rank ranges. Delete-bitmap caches
explicitly own their backing allocator, independently of routing scratch arenas.

Broad patterns retain range-local, constant-score admission even when the global
vocabulary cannot fit the small expansion. Prefix, wildcard, regexp and fuzzy
queries use the scorer's matcher. A range needs only one matching term witness;
unsuccessful witness work is bounded, and unknown retains the entire range.
Inventory cursors establish real pack endpoints before an unknown range is split.
Only an exhausted vocabulary proves absence. Materialized prefix fields remain
part of the conservative union. Required Boolean clauses can still route when an
optional or other required clause has unknown membership. Final membership and
scores remain the scorer's responsibility.

BM25 envelopes whose maximum frequency is one prove a fixed frequency for every
posting. Their norm minimum gives a monotone f32 ceiling under both canonical
scoring formulas. These envelopes can use logical-position tie pruning without
a rounding margin. Higher/unknown frequency envelopes and scored phrases retain
the existing margin; this is not a claim that all BM25 arithmetic is monotone in
frequency. Startup warming includes the authenticated logical tie tree.

Archive and recent dense/sparse searches enter the same bounded scheduler pass.
Each lane owns runtime pins, membership cursors, version maps and range-reader
flights. Parent immutable inventory and reader authority remain pinned until all
lanes join. Results merge in archive/run order; saturation runs on the caller,
oversized lane work retries serially, and errors/cancellation drain every owner.
Archive predicates reopen private membership cursors rather than sharing the
parent's mutable cursor. Source views can adopt their own inventory without
freeing the parent's pinned-file evidence.

Immutable Iceberg manifests have a separate decoded-cache admission coordinator
from complete snapshot plans. Fresh metadata and manifest-list selection remain
mandatory. Unchanged manifest paths/lengths reuse parsed Avro under the current
credential/backend scope; mutable attachments retain fresh reads. Inherited
sequence numbers, partitions, summaries and schema are interpreted under the new
manifest list and metadata, not cached from an older snapshot. Snapshot assembly
uses a manifest hash directory rather than repeated linear manifest searches.
The public inventory is still assembled linearly in its entries; this change
avoids repeated manifest fetch/parse and quadratic manifest lookup, and does not
claim constant-time cold planning for an arbitrarily large new snapshot.

Regressions cover external pack proofs beyond 4096 entries, compressed routing
through deleted holes, exact f32 ceilings, mixed Boolean admission, broad pattern
scores/ties over real Parquet, archive/recent overlap with cancellation and quota
pressure, source-view ownership, and immutable manifest reuse across freshly
selected snapshots and credential scopes. Live-provider retention qualification
and archive-scale latency/RSS benchmarking remain deferred.


Final qualification after merging origin/main fc4f2402ad (#1137): the Debug CLI
rebuilt, all 472 lake integration tests passed (174 shared-engine and 298 server),
all 68 focused tests passed (13 shared-engine and 55 server), and all 46 real
Parquet/Iceberg E2Es passed in 306.69 seconds. The focused suite now includes
main's bounded constant-score cursor/cancellation regressions. Pure patterns
retain their dedicated bitmap collector while mixed Boolean trees use streaming
lowering; prepared counts carry cancellation/deadlines through shared filters.
Runtime failure conflicts preserve main's published numeric identities.
Compiler-local native error generation, Zig/Python formatting and lint,
whitespace, license checks and the Apache product boundary passed (2,052 source
files). Provider retention qualification and archive-scale performance
benchmarks remain deferred.


### Lazy physical probes, shared snapshot chunks and composed scheduling

The frontier now includes negative-only Boolean baseline scores, including
boosted nested clauses admitted through a positive sibling. Regressions compare
canonical scorer scores with bounds and real Parquet ranked pages with an all-row
request that cannot establish an early competitive cutoff.

Broad metadata admission has a lazy physical-range path backed by optional
reverse-index tuple reductions. Its query-owned cache/read quota is independent
of point-membership quota; unknown/legacy/budget-limited proofs retain packs.
Small predicates retain complete compressed/spillable admission. Broad patterns
reuse compiled matchers and traverse authenticated lexical term subtrees, using
pack envelopes to avoid unrelated child reads. Exact repeated-frequency and
phrase ceilings enumerate a bounded frequency domain under both canonical
f32 scorer formulas and cache repeated envelopes. Escaped/large domains keep
conservative margins.

Iceberg snapshot plans can pin shared immutable derived manifest chunks, merge
ordered file views and regenerate only snapshot-qualified version strings.
Manifest-list inheritance inputs participate in chunk identity. Credential
changes, mutable attachments and fresh delete applicability retain their existing
contracts. Snapshot flat views and public row-ID ordering still require work
proportional to file count. Native composed retrieval overlaps independent text,
dense and sparse executions through the shared scheduler; private mutable state,
serial saturation/quota fallback, cancellation draining and declaration-order
fusion are explicit. Provider retention qualification and archive-scale latency /
RSS benchmarks remain deferred; no throughput multiplier is claimed.


Final qualification after merging origin/main `89df7aff46` (#1140): the Debug
CLI rebuilt; all 476 lake integration tests passed (175 shared-engine and 301
server), all 72 focused tests passed (14 shared-engine and 58 server), and all 46
real Parquet/Iceberg E2Es passed in 311.75 seconds. The large indexed-predicate
fixture asserts native predicate admission; the multi-pack fixture compares
boosted negative-only Boolean and repeated-term phrase pages with an all-row
request. Nested vector scheduling regressions cover serial execution under the
enclosing lane, cancellation, and enclosing workspace exhaustion. Reverse tuple
reductions use conservative bounded prefixes within the 256-byte page format;
long-key publication, legacy children, and saturated prefixes retain safe
fallbacks. Compiler-local native error generation, Zig/Python formatting and
lint, whitespace, all 100 changed source license headers, and the Apache product
boundary passed (2,053 source files). Live-provider retention qualification and
archive-scale latency/RSS benchmarks remain deferred.


### Lazy snapshot catalogs and bounded composed setup

Legacy compacted text roots now detect supported versions from authenticated
bodies in both serving and resumable GC. Composed workers borrow immutable
parent version maps, own private overrides, and retry serially if workspace
admission fails during setup. These address the two findings from the review.

Iceberg snapshot discovery keeps a persistent file-ID tree over leased manifest
chunks. Unchanged snapshots share the root; changed snapshots path-copy only
changed entries. Selective row hydration performs direct lookups and owns just
the selected file view. Full inventory and canonical ordering are deferred to
scans, with the latter shared after its first construction. Cache pressure
retains the materialized fallback. Fresh metadata, credential scope, schemas,
delete plans and reader authority remain required. Initial manifest-list work
and full-scan ordering remain proportional to their inputs.

Selective Boolean trees now keep finite bounds for constant-score match-all and
bitmap clauses. Regressions cover root compatibility/GC, source views larger
than a worker quota, persistent-catalog allocation failures and snapshot refresh,
and real Parquet mixed-Boolean score/tie/offset comparisons. Live-provider
retention qualification and archive-scale latency/RSS benchmarks remain deferred.


Final qualification with origin/main `89df7aff46`: Debug CLI built; all 479 lake
integration tests passed (176 shared-engine and 303 server), all 75 focused
tests passed (15 and 60), and all 46 real Parquet/Iceberg E2Es passed in 374.08
seconds. The E2Es used an isolated 3 GiB HFS test volume to retain production
disk-admission checks despite low host-volume headroom. The temporary volume
was removed after qualification. Snapshot refresh coverage changes the metadata
URI and verifies unchanged file-node reuse and fresh version strings. Legacy
non-snapshot planning leases remain compatible. Native error metadata, Zig and
Python formatting/lint, whitespace, all 102 changed source license headers, and
the Apache product boundary passed (2,054 source files). Provider qualification
and archive-scale latency/RSS benchmarks remain deferred; this is functional
and ownership qualification, not an archive-scale performance measurement.


### Catalog admission, cold construction, and independent delete-file indexes

Catalog and lazy-ordering allocations now evict predecessor-cache pins and retry
before falling back. Active reader pins remain valid. Cold catalogs bulk-build
one node per file through sorted manifest merging; incremental trees continue
to copy only changed paths. Allocation-failure sweeps cover the bulk builder,
and a quota-pressure regression verifies eviction and surviving reader pins.

Selected Iceberg views share independently owned delete-file indexes keyed by
strong provider identity, complete recipe/schema, credential scope and policy.
Fresh object stats precede lookup. Views apply applicability and bounds locally;
large or weakly identified files retain streaming decode. Per-loader admission
is 64 MiB, foreign index leases are capped at 256 MiB per prepared view, and cache
residency counts foreign bytes. Regression coverage checks selected-view reuse,
zero additional range reads on reuse, streaming fallback, and same-size provider
replacement. Streaming position deletes now use direct data-path lookup too.

Text-frontier sibling metadata warming uses two scheduler lanes with independent
2 MiB workspaces and 1 MiB read allowances. Optional reads total at most 8 MiB per
query. Saturation skips warming, cancellation drains workers, and all authority,
scoring and traversal reductions stay on the coordinator. A ranked-equivalence
regression exercises the parallel path. Composed benchmark timings now carry
actual completed worker execution durations, including membership preparation,
instead of timing result transfer. Component durations may overlap.

Live-provider retention qualification and archive-scale cold/warm latency and
RSS benchmarks remain deferred. The bounded algorithms and fixture read counts
establish ownership/correctness and removed work; they do not measure production
archive speedups.


Final qualification with origin/main `d0614ed7e5`: Debug CLI built; all 481 lake
integration tests passed (178 shared-engine and 303 server), all 77 focused
tests passed (17 and 60), and all 46 real Parquet/Iceberg E2Es passed in 312.91
seconds. The E2Es ran on the host volume with production disk-admission rules.
Pinned-compiler native error metadata, Zig/Python formatting and lint,
whitespace, all 104 changed source license headers, and the Apache product
boundary passed (2,060 source files). These results qualify behavior and
ownership; representative archive-scale latency/RSS and live-provider retention
qualification remain deferred.


Cold immutable Iceberg indexed reads now authenticate publication file navigation
against fresh metadata, selected snapshot/schema and credential/table/store
identity before deferring data manifests. Both native search and SQL use this
opener. Manifest-list/delete discovery remains fresh, selected file metadata is
query-owned, and full scans discover files from frozen metadata normally. Current
and retained explicit-snapshot requests accept the existing matching digest
variants without changing the publication format. A cold-restart E2E removes
data manifests after publication and exercises text, retained text and SQL point
reads, then restores manifests for the full-scan fallback.

Position-delete membership is shared as compressed 64-bit Roaring blocks. Views
borrow immutable singleton sets and merge blocks only for overlapping delete
files. Cached canonical fingerprint prefixes preserve publication IDs. Ordered
text batches prepare syntax/statistics once and share at most four immutable
pack membership bitmaps under an 8 MiB query quota, evicting or falling back on
pressure while leaving exact scoring/filtering authoritative. Cloud generation
navigation now reclaims other idle credential scopes for owner and decoded-page
admission; active readers remain pinned. Regressions cover admission pressure,
shared delete ownership/high coordinates/fingerprint compatibility, and repeated
ordered batches with bounded fallback.


Fallible decoded-cache constructors now complete before installing the payload
union tag. Missing manifest or allocation failures leave the prior empty payload
valid for cleanup; fully constructed resources retain normal destruction if a
later preparation step fails. Regression coverage retries a missing-manifest
load and verifies surviving shared snapshot leases. Synonym membership also
propagates allocation failure instead of silently dropping expansion matches,
allowing the optional ordered bitmap memo to fall back without false negatives.


Publication navigation binds read callbacks to its own retained native handle,
not the source opener's temporary handle. Native search shares its existing
reader lease; SQL acquires a lease only when it has none. Navigation authority
therefore survives source-open return without adding a second search lease.
The cold-restart test exercises this boundary during final source hydration.


Residual predicates resolve a bounded source view directly from the physical
selection's file identities before assigning scan-local file ordinals. This
works with both persistent catalogs and publication navigation, without expanding
selected row coordinates or discovering unrelated data manifests. Full scans
retain credential-scoped immutable parsed-manifest reuse under fresh selection.
The cold E2E includes ordered indexed-range plus unindexed-prefix filtering while
data manifests remain unavailable.


Final qualification with origin/main `e32171bfb1`: the Debug CLI built; all
485 lake integration tests passed (180 shared-engine and 305 server), all 81
focused tests passed (19 and 62), and all 47 real Parquet/Iceberg E2Es passed in
313.34 seconds. The strengthened cold restart E2E covers native text, retained
text, SQL point lookup, ordered indexed-range/unindexed-prefix residual filtering
with data manifests unavailable, and ordinary full scans after restoration.
Pinned-compiler error metadata, Zig/Python formatting and lint, whitespace, all
109 changed source license headers, and the Apache boundary passed (2,064 source
files). Live-provider retention qualification and representative archive-scale
latency/RSS benchmarks remain deferred; no archive speedup multiplier is claimed.


### Shared full-scan plans, ordered pattern leaves, and delete qualification

Automatic navigation admission now preserves ordinary Iceberg SQL fallback when
the native artifact store or reader pool is unavailable. Required/retained reads,
request cancellation, OOM and ambiguous mutation outcomes retain their errors.
Full scans lease one frozen flat inventory, qualified version strings and
canonical plan through the bounded decoded cache. Selective reads keep their
publication navigation and fresh metadata checks.

Ordered pack membership and scoring share bounded pattern-leaf bitmaps under the
existing four-pack/8 MiB query budget. Scoring owns private copies, so parallel
mutation and eviction cannot change a winner. Delete qualification is query-owned
and singleflight: eight bounded stat lanes populate a 16 MiB partition/sequence
index, then selected views decode and retain only candidate delete files. Serving
coverage and hydration share that evidence; build verification still freshly
checks providers before and after publication. Incomplete scopes and quota denial
preserve the authoritative fallback paths.

Regressions cover unavailable native storage through real Iceberg SQL, warm full
scans with data manifests unavailable, shared empty-plan ownership and allocation
failures, pattern reuse across ordered prefilter/scoring batches, private bitmap
mutation, cancellation and pressure, and delete partition/sequence pruning with
provider replacement and unchanged fingerprints. Representative archive-scale
latency/RSS and live-provider retention qualification remain deferred.


Final qualification after merging origin/main `a900887124` (#1153): Debug CLI
built; all 488 lake integration tests passed (183 shared-engine and 305 server),
all 84 focused tests passed (22 and 62), and all 47 real Parquet/Iceberg E2Es
passed in 620.91 seconds. Qualification covers unavailable optional native
storage, warm full scans with data manifests unavailable, shared-plan ownership
and allocation failures, pattern reuse during prefiltering and scoring, and
partition/sequence delete routing with unchanged fingerprints. Provider
replacement, equality/position sequence boundaries, cancellation, quota denial
and qualifier allocation failures pass. Compiler-local error metadata, Zig and
Python formatting/lint, whitespace, staged source licenses and the Apache
boundary passed (2,065 source files). Live-provider retention and representative
archive-scale latency/RSS qualification remain deferred. These E2E timings were
collected during builds and are functional qualification, not benchmarks.


### Optional scan admission, paged dictionaries and version-qualified deletes

The full-scan regression is addressed at both layers. Ordinary SQL scans over
new publication navigation stream canonical public-file pages and retain one
file view per lane. Disjoint digest intervals keep ordered and unordered scans
parallel under the shared scheduler; workers own their cursors, readers, version
maps and selected delete state. Flat plans remain available for callers that
need a census, with statement-owned overflow when the optional 64 MiB shared
plan cache declines admission. Overflow ownership and partial cursor creation
are allocation-failure tested.

Protocol 48 persists ordered file-slot/name/public-digest dictionaries. Selective
serving and readiness load compact roots and reached records; maintenance loads
full slot state explicitly. Copy-on-write updates retain unchanged dictionary
trees and GC follows every new page root. Old 5/6 roots remain readable and new
serving/checkpoint metadata uses 7/8. A 17,000-file regression verifies selective
lookups, old-reader stability and bounded changed-page writes.

Pattern scorer leaves borrow pinned immutable segment-local bitmaps. Cache hits
avoid complete clones and offset materialization, while mutable filter callers
keep private copies. Pins exclude entries from eviction and release when their
scoring builder joins/closes. Cold misses and cache pressure use the ordinary
filter path.

Delete routing coalesces partition/sequence suffixes before union. Pinned,
immutable catalog snapshots on version-capable providers may publish exact
provider delete selectors, with coverage/recipe digests authenticated by the
same publication. Query qualification then builds routing metadata without an
archive-wide HEAD census and reads only reached selector pages. Version-qualified
payload reads, snapshot and native reader pins preserve the selected cut.
Unversioned or mutable sources, absent catalog pins and legacy proofs retain
fresh validation. Publication verification still re-stats providers before and
after the build. Live-provider retention qualification and archive-scale latency
and RSS benchmarks remain deferred.

Each scan lane owns a small navigation cache and independent point-read scratch,
while immutable decoded pages remain shared. Per-file metadata reads retain their
individual bounds without imposing an archive-wide 64 MiB navigation ceiling.
Delete-selector pages use the same immutable decoded navigation cache. Ordered
readers also keep a bounded decoded-name memo to avoid parsing slot JSON per row;
output names remain independently owned across eviction.

Before the final main merge, the Debug CLI built and all 47 real Parquet/Iceberg
E2Es passed in
633.06 seconds; after the final provider-selector ownership cleanup, the cold
Iceberg navigation/scan regression passed again against the rebuilt CLI in
8.81 seconds. That regression hides real data manifests before cold COUNT and
unindexed-column filtering, then exercises indexed, retained and fallback reads.
The focused native suites passed 87 tests (22 shared-engine + 65 server), with no
failures or leaks. All 492 integration tests passed (184 shared-engine + 308
server), also without failures or leaks.
Formatting, lint, generated error metadata, source licenses and the Apache
product boundary passed (2,065 source files).

After merging origin/main at `f9f1582346` (#1145), the Debug CLI rebuilt,
all 87 focused native tests passed again without failures or leaks, and the
cold real-Iceberg navigation/scan E2E passed against that binary in 9.29 seconds.
Merged generated error metadata and the Apache product boundary also passed.


### Shared public ordering and bounded publication scan planning

Reader protocol 49 publishes ordered serving/checkpoint roots as versions 9/10.
Versions 5/6 and 7/8 remain readable for migration and reader-safe collection.
Every ordered index borrows the publication's authenticated public-digest-to-file
name directory. Each index keeps its own stable file slots, including retired
slots; a name lookup translates shared navigation to that index's coordinates.
The live public-file count is independent of its historical slot count. Snapshot
changes replace the shared reference without rewriting unchanged slot/name trees.
Fresh slot trees are bulk-built in slot order; hashed dictionaries use bounded
512 KiB sort runs with a four-run merge limit. Incremental edits still copy only
changed paths.

A sequential prefix of at most 65 file records identifies wide groups; small
groups pay no separate cardinality seek. For large or sufficiently dense
logical-key ties, public ordering walks that shared directory in digest order and probes exact tie membership. A small LIMIT
can stop before reading every participating file. Continuation seeks start at
the boundary digest and preserve physical row coordinates, ascending/descending
IDs, backward pages and transitions between tuples. Small or sparse ties retain
local ordering and its scoped cache; wide dense groups use lazy intersection, while sparse wide groups use
bounded participant spilling as described below. Counts choose execution strategy only;
exact membership and native rows decide results. Spill-admission fallback may still
require many directory probes, so representative archive benchmarks remain
necessary.

Publication scans can freeze at most eight file descriptors into a private
statement view, with an independent iterator preserving serial fallback after
setup failure. Unordered scans then use the existing shared row-group work queue;
single-file ordered scans split row groups into contiguous canonical ranges.
The original multi-file scan path used lazy contiguous file ranges. The rolling
row-group scheduler below replaces fixed file lanes for large publication scans
while retaining deferred descriptor/footer errors and bounded file views. Startup warming
loads compact ordered roots and selected dictionary pages under the existing
page/time budget, avoiding full dictionary materialization.

Ordinary and resumable GC now use one source-navigation dependency walker,
including delete-version proof pages. An actual bounded-sweep regression pins an
old publication, replaces its proof, verifies the page survives collection, then
releases the last reader and verifies retirement. Other regressions cover shared
dictionaries with distinct slot histories, bulk write counts, bounded sparse-tie
prefix work and continuation boundaries, small-scan allocation failures, and
real cold Iceberg scans with one/two files and many Parquet row groups.


Qualification with origin/main `f9f1582346` included: the Debug CLI built;
495 integration tests (184 shared-engine + 311 server) and 90 focused tests
(22 + 68) passed without failures or leaks. All 48 real Parquet/Iceberg E2Es
passed in 636.58 seconds. The final ordered-scan refinement passed its native
allocation/row-group regression and four ordered/cold-Iceberg E2Es against the
rebuilt CLI (28.09 seconds). Compiler-derived error metadata, formatting/lint,
source licenses, whitespace and the Apache boundary (2,065 files) passed.
Live-provider retention qualification and representative archive latency/RSS
benchmarks remain deferred; these E2E timings are functional qualification.


## Dictionary-bound pagination and rolling row-group scheduling

The public tie cache binds its slot order to the authenticated ordered-file
mapping as well as the tuple tree, publication source/snapshot and authorization
scope. Two indexes with equivalent definitions can retain different stable slot
histories, so the tuple-tree identity alone cannot authorize reuse of cached
slot numbers. Regression coverage warms one mapping and queries the other in
both directions with continuation boundaries.

Ordered data-run ancestry and dictionary ancestry are independent. Within a
checkpoint sequence, the first root establishes the complete slot/name layout;
subsequent roots authenticate and edit only the reached fingerprint slots.
Fresh data runs inherit the dictionary independently of their merge forest,
and checkpoint assembly reuses that dictionary directly. Maintenance reloads
compact roots instead of materializing/sorting all file names after each file.
The first incremental merge retains the previous layout's length when newly
admitted files extend the census. Final assembly streams the row runs normally.

Public ordering chooses between snapshot-directory intersection for dense ties
and a participant-only external sort for sparse wide ties. Participant sorting
uses 128 KiB runs and the existing bounded spill manager, resolves only group
members, and orders digest/slot keys in the requested direction. Continuations
filter digest bounds before sorting and preserve exact physical row-coordinate
seeks at an equal boundary. This bounds memory and avoids unrelated archive
membership probes; it trades up-front participant work for predictable sparse
group traversal. Small pages keep the lazy prefix when it is cheaper. Continued
traversal switches to a spilled unvisited suffix once probes exceed participant
preparation cost or the reserved metadata-read budget, finishing any current
file before switching. Spill admission
failure retains lazy traversal. A native fixture exceeds 65,000 files and 4,096 participants,
forces spilling, and checks full order plus forward/backward continuations.

Large publication scans use a rolling, query-owned row-group scheduler.
Unordered lanes share a window of at most four footer plans and claim individual
row groups by compressed size; tiny files overlap and a large file can occupy
every worker. The next window is planned after current ranges have finished and
released their borrowed plans. An independent producer preserves the unopened
parent for serial spilling fallback, and reducers release active claims on early
exit. Physical row admission belongs to the statement and survives worker
ranges, rolling windows and fallback. Waiting lanes retain immutable statement
context and I/O, independently of the producer's changing file view. Ordered
SQL delivery consumes contiguous row-group ranges as a bounded batch, joins and closes the batch, then advances to the next file.
Later descriptor/footer errors cannot displace an already delivered prefix.
Scheduler saturation executes required ordered ranges on the caller rather
than discarding an already advanced batch. The existing small-inventory path
continues to support its fixed immutable work plan.

These changes do not claim an archive-scale speedup factor. Representative
cold/warm latency, publication request counts, RSS and skewed-file throughput
measurements remain qualification work; real-provider retention qualification
remains deferred.


Validation with origin/main `f9f1582346` included: the Debug CLI built;
497 lake integration tests (185 shared-engine + 312 server) and 15 focused SQL
contracts passed without failures or leaks. The final adaptive traversal passed
four focused cache/tie regressions, including the 65,553-file fixture, small-page
prefixes, bounded promotion, spill rejection, backward cursors and a following
tuple. All 50 real Parquet/Iceberg E2Es passed in 621.84 seconds. The new Parquet
case streams 20,288 rows over pgwire in 137-row delivery batches across cold
restarts and checks filtered/grouped aggregates; cold Iceberg coverage now also
includes ten independently written files. Compiler error metadata, lint,
whitespace, source licenses and the Apache boundary passed. These timings are
functional qualification rather than archive latency benchmarks.


## Ordered quota boundaries and pipelined footer navigation

Ordered delivery reserves useful caller workspace independently of shared
scheduler availability. Worker execution batches use that lane's memory limit,
so saturation cannot assign zero workspace or run a statement-sized expression
batch inside a small lane. Terminal worker EOF is administrative completion and
does not consume a logical scan page. Exact row/page limits permit a one-row EOF
probe; any actual row or nonterminal empty response still passes ordinary quota
admission before it becomes visible.

A statement may retain one independent enumeration lookahead cursor and at most
one admitted I/O task. It advances monotonically and warms parsed, version-bound
Parquet footers for the next one-file ordered batch or four-file unordered
window while current row groups execute. Its private metadata workspace is
bounded at 16 MiB, alongside the existing bounded decoded cache. Scheduler denial
keeps required reads synchronous; speculative metadata failures disable warming
and remain deferred until required enumeration reaches the affected file.
Required progress never waits for unrelated speculative footers: caught-up
readers coalesce through the shared cache. Closing the statement cancels and
joins warming before releasing source leases.

Unordered rolling workers borrow the sealed dynamic filter and retain exact
residual membership evaluation, including projection-hidden join fields. Small
fixed work plans use the same propagation. Idle lanes wait on a native I/O
condition with deadline checks rather than polling between windows.

Ordered maintenance carries explicit sorted retirement slots. The first
checkpoint retires the planned changed/removed slots once; later checkpoints
visit only their own file slot. The full keep mask still governs streaming
merges, and legacy/direct callers can retain mask-based discovery. Dictionary
fingerprint updates and retirement planning now both avoid a full slot census
per source-file checkpoint.

Regressions cover exhausted scheduler bytes, exact EOF quotas and nonterminal
empty loops, footer lookahead admission/denial, deferred descriptor errors,
filtered small/rolling workers, concurrent claim release, and bounded retirement
iteration over a million-slot fixture. The real skewed-file Parquet flow also
checks an aggregate join with duplicate and NULL build keys after cold restarts.
Archive-scale speedup factors remain unmeasured.


Qualification with origin/main `f9f1582346`: the Debug CLI built and a full
498-test lake integration run passed (185 shared-engine + 313 server), without
failures or leaks. Six focused SQL contracts passed. After the final nonblocking
footer refinement, the native rolling-scan regression passed again, including
a fully delivered first batch followed by demand progress while speculative
navigation remained paused, cancellation/ownership checks and empty-filter
fast paths. All 50 distinct real Parquet/Iceberg E2Es qualified against the final
production implementation. E2E execution resumed after storage-headroom guards
stopped fixture startup; the successful runs covered 39, four and seven cases.
The production disk guard remained enabled. Compiler-derived error metadata,
Zig/Python formatting and lint, whitespace, source licenses and the Apache
boundary (2,065 source files) passed. Archive-scale latency/RSS and live-provider
retention qualification remain deferred.


## Adaptive reducers, overlapping work windows, and direct parallel bounds

Footer lookahead completion state belongs to its worker until joining. Required
navigation checks task ownership before reading EOF or other worker-written
fields, including when it advances faster than speculation.

Aggregate workers exempt terminal empty EOF from logical page admission, just
like ordered delivery. Nonterminal empty responses still consume page credit.
Each reducer now owns a stable, reclaiming allocator budget covering its local
state and execution scratch; source buffers remain cursor-owned under the
statement's existing physical scan limits. Adaptive admission reserves half of the
node's temporary-work allowance for I/O, starts admitted sibling workers behind
a readiness barrier, and executes one lane on the caller. At the default node
and statement budgets, four lanes receive 8 MiB each and three concurrent CPU
leases consume 24 MiB; a 16 MiB footer task can still run. Local hash states
spill within their workspace or fall back to the untouched pinned parent.

Unordered publication scans retain at most two independently owned four-file
windows. A producer transfers its planned descriptors, version pins, and work
census to a window owner while retaining the same monotonic enumeration cursor
and optional lookahead owner. Idle lanes can plan and claim the next window
while earlier claims remain active. Each window closes only after its own last
claim releases; planning failures become sticky rather than allowing another
lane to skip an already advanced source range. Ordered delivery keeps its
existing file/row-group order and deferred-error contract.

Text frontier siblings compute authenticated bounds on independent I/O lanes
and return their final bounds and counters to the coordinator. Successful work
is not reduced again by a serial traversal. Two persistent worker owners each
have a 2 MiB budget for private navigation pages, scratch, score-ceiling caches,
and compiled pattern state, alongside the coordinator's existing 8 MiB budget.
Patterns compile at most once per lane for a query. Immutable prepared syntax
and selection arrays are borrowed through joining; spilled selection clipping,
score-heap updates, and read accounting remain coordinator-owned. Optional
workspace/read admission failures retry the required bound serially; corruption
and cancellation remain errors. The existing 8 MiB parallel-read allowance
accounts for every attempted worker read, including failed attempts.

Regressions exercise exact aggregate page limits, preserved I/O headroom,
retained early-window vectors while later windows drain, concurrent Boolean and
pattern bounds, exact ties, scheduler denial, and fewer coordinator traversals.
Archive-scale throughput and latency improvements still require measurement.


Qualification with merged origin/main `464b41145f`: the final Debug server built;
all 498 native lake tests (185 shared-engine and 313 server), nine focused SQL
contracts, and 29 runtime error ABI tests passed. Native tests reported zero
failures and leaks. The final direct parallel-bound regression also passed,
including fewer coordinator traversals, Boolean conjunctions, exact ties,
reused broad-pattern compilation, and saturated-I/O fallback. All 50 real
Parquet/Iceberg E2Es passed in one run (608.00 seconds), with the production disk
guard enabled. Generated error identities, formatting, whitespace, source
licenses, and the 2,065-file Apache boundary passed. Real-provider retention and
representative archive-scale latency/RSS qualification remain deferred.


## Logical aggregate credits and typed final reduction

Native serial and parallel aggregate scans share statement-level logical credits:
positive rows consume `ceil(total_rows / statement_execution_rows)` units,
nonterminal empty pulls consume one unit each, and terminal EOF consumes none.
The statement chooses the credit size before worker workspace changes physical
pull sizes. File boundaries, Parquet row-group boundaries, worker fan-out and
response delivery size cannot multiply positive-row charges. The row ceiling
still applies independently. Backend row-page and materialized-partial quotas
retain their existing contracts. This matches native ordered delivery's logical
work policy; it does not increase configured limits.

Parallel exact reduction includes MIN/MAX for integer widths, booleans and text.
Their equal values have the same representation, so local extrema can merge in
any lane order while keeping SQL null handling. Floating extrema, dynamic values
and other types retain ordered execution: signed-zero representatives and mixed
numeric representations require a stronger tie contract before parallel use.

Final in-memory reduction packs up to 64 group keys into one scratch matrix,
resolves destination IDs once, and merges fixed-width partial vectors by column.
Count, wide integer sum, boolean and integer extrema merging allocate no reducer
snapshots. NUMERIC merges the borrowed exact accumulator directly, avoiding its
previous per-group clone. Variable-width states use precise per-group admission;
exponent gaps spill before allocation and string extrema copy only a replacement
winner into destination ownership. Durable spill encoding remains the boundary
for disk partials, with one reusable per-partial scratch arena rather than
retaining a batch of cloned states. Group ordinals and overflow checks survive
both batched and spill paths. Small spill reducers also size their active hash
partition count from workspace (two to eight), preventing nested join reducers
from retaining eight files each and exhausting merge-run headroom. The shared
64-file ceiling remains enforced. Sorted skew fallbacks reuse the existing
anonymous extent store, keeping one descriptor for all logical merge runs
instead of retaining a separate file for every run.

Ordered producers now use the same CPU workspace policy as aggregate workers,
reserving half of the shared temporary-work allowance (up to the I/O ceiling)
for navigation and prefetch. Required caller fallback remains available under
scheduler saturation. A regression holds actual ordered producer tasks while
admitting a concurrent 16 MiB I/O task; primitive partial merges are also checked
with a zero-capacity allocator. Real Parquet regressions cover serial/parallel
quota equivalence, mixed extrema and a cold-restart grouped MIN/MAX flow.

Merged main `9dd098d773` retains its explicitly pinned runtime error IDs 674 and
675; the draft branch's conflicting decision-limit detail is assigned 676.
Main's inference lifecycle changes and corresponding boundary tests are retained.
Live-provider retention and representative archive-scale latency/RSS measurement
remain deferred.


Qualification on merged main `9dd098d773`: the final Debug CLI built; all 498
native lake tests (185 shared-engine and 313 server), 22 focused SQL regressions,
and 31 runtime ABI tests passed. Native suites reported zero failures or leaks.
All 50 real Parquet/Iceberg E2Es passed in one run (614.96 seconds), including
the extended grouped extrema cold-restart flow, with production disk safeguards
enabled. Generated error parity, Zig/Python formatting and lint, whitespace,
source licenses, and the 2,066-file Apache boundary passed. Archive-scale
latency/RSS benchmarks and real-provider retention qualification remain deferred.


## GC expansion upgrades and final serving refinements

Artifact marking and dependency traversal are separate. Ordinary GC planning and
cloud publication audits retain a bounded expansion bitset beside each marked
reference. A maintenance-only canonical manifest can later be expanded for an
older reader-held serving publication, including seekable directory children.
Full expansion subsumes maintenance expansion; visiting a raw reference cannot
suppress its later traversal. Resumable collection keeps its durable expansion
keys, including a separate seekable-manifest key. Failed or incomplete traversals
never authorize a sweep.

Parquet cursors load the embedded OffsetIndex for projected chunks even without a
filter. Unfiltered scans omit the ColumnIndex bounds read. For indexed data pages,
required reads and rolling prefetch use the exact header-inclusive compressed
range. The encoded header must agree with the index length, page type, and row
count before decoding. Range and input admission precede the read. Dictionary
preludes and chunks without an OffsetIndex retain bounded header discovery.
Optional indexes that exceed navigation admission use the ordinary bounded
header path; malformed admitted indexes still fail validation.
This removes a header-probe GET for cold indexed pages larger than the probe;
small pages already fit the existing probe. The page directory remains versioned,
leased, and bounded by each consumer's allocation policy.

Exact parallel aggregation partitions in-memory partial groups by their existing
semantic key hash once there are at least 4,096 partial groups. Each admitted
reducer exclusively owns its destination hash table and merges typed state through
selection vectors. A stable owner retains reducer budgets until result shaping
consumes their batches. Final batches go directly to the existing result collector,
without a serial hash-table rebuild. Source ordinals preserve implicit ordering;
global group limits apply across all reducers. Every task joins before scratch or
source release. Low-cardinality, spilled-source, and declined-admission cases use
the existing bounded serial/spilling bridge. Reducers themselves may spill when
an owner is skewed. Floating or order-sensitive aggregate states retain their
established eligibility restrictions.

Posting scorers share query-owned immutable regex programs and wildcard navigation
plans across segments and remote routing windows. Regex execution forks private
DFA state; provider I/O never holds the compilation-cache mutex. The cache admits
at most 16 programs within 2 MiB, owns syntax bytes, and declines oversized/full
entries to normal per-consumer compilation. Deadlines and cancellation remain
authoritative. Compilation and DFA allocation failures propagate instead of
turning a valid regex into an empty result; DFA columns publish states only after
all allocations succeed. Fuzzy matching still constructs its inexpensive term
wrapper with independent execution state.

Regressions exercise both manifest traversal orders, already-marked references,
one provider GET per indexed data page, dictionary preludes, inconsistent index
lengths/ordinals, concurrent program reuse, allocation-failure cleanup, hash-owned
state lifetime, overlapping groups, winning text extrema, ordinals, and the global
group limit. The real Parquet cold-restart fixture now writes embedded page indexes
and checks a 2,048-group aggregate alongside ordered result delivery. Provider
retention qualification and representative archive latency/RSS measurements remain
deferred; request-count regression evidence is not an archive-scale speedup claim.


Qualification (2026-10-16): all 501 full native lake tests (187 shared-engine and
314 server), 24 focused SQL regressions, and 31 runtime ABI tests passed without
native failures or leaks. Final reducer sealing and four-column lookahead were
rechecked with four focused native tests and six typed SQL owner tests. All 50
real Parquet/Iceberg E2Es passed together in 639.93 seconds, with production disk
safeguards enabled. The final Debug CLI, source/license boundaries, generated
error parity, formatting, and lint checks passed. Latest fetched `origin/main`
`9dd098d773` is included. The final CLI also passed the indexed Parquet
cold-restart/grouping flow again (12.08 seconds) after the last guards.


### Decoded-page hits, global spill exchange, and physical aggregate kernels

Indexed decoded-page entries now retain validated Parquet header evidence under
an immutable object-version/range/interpretation key. Readers consult that entry
before encoded I/O or speculative prefetch and reapply their own input, decoded,
structure, row-count, directory, and cancellation checks. Dictionary dependencies
remain pinned; dictionary preludes and unindexed files retain bounded header
discovery. With encoded residency disabled, warm scans decode no pages and fetch
no indexed data-page payloads, including when asynchronous lookahead is enabled.

Final exact aggregation can transfer unreduced worker spill partition files to
global hash owners. The existing native block codec and AGS checkpoints cross the
exchange unchanged; in-memory workers export a compact checkpoint once. This
removes the serial worker-finalization and partial re-encoding bridge for compatible
fresh spill streams. All files are sealed and destination capacity is reserved
before ownership moves. After the first transfer, adoption is infallible. Source
ordinal bases, semantic key ownership, wide integer cancellation, shared scheduler
admission, disk quotas, cancellation, and sorted skew fallback are preserved.
Already consumed or incompatible partition layouts retain the established bridge.

Identity SQL column expressions now retain borrowed native vectors through encoded
cohort evaluation, projection, and selection. Aggregate kernels dispatch on the
state/vector pair and consume selections, null masks, and dictionary IDs directly.
COUNT, integer SUM/AVG, floating SUM/AVG, boolean reductions, and primitive MIN/MAX avoid
per-row Datum construction. Global unfiltered aggregation uses this path without
per-row hash probes. Floating transitions preserve source update order, compensated
sums, SQL NaN ordering, and first-occurrence signed-zero ties. Only winning text
extrema are copied. Coercions and representations without a proven physical view,
exact NUMERIC, DISTINCT, pattern sets, and filtered expression cohorts preserve
their established typed/scalar evaluation and admission paths. Temporary composed
selections are scoped to the update; retained aggregate state owns its payloads.
Computed dictionary expressions retain their existing fused dictionary kernels.

Regex and fuzzy posting traversals lease exclusive mutable DFA state from the
query-owned program cache. Up to four executions per program can retain tables
between segments; each execution has at most 256 KiB within the shared 2 MiB
query cache. Traversal and provider I/O never hold the cache mutex. Saturation uses
private execution, and cache-budget exhaustion retries private execution without
changing matching results or execution statistics. Actual allocation failures
propagate. Regex and fuzzy DFA columns publish states only after all allocations
succeed. Query teardown requires all execution leases to be returned.

Regressions cover decoded-only warm reads and stricter consumers, concurrent and
sequential pattern state reuse, saturated and exhausted execution pools, mixed
memory/disk global reducers, transfer without additional writes, ordinal offsets,
wide integer cancellation, allocation-failure cleanup, physical identity expression
ownership, selection/null/dictionary kernels, zero-allocation primitive updates,
winning text ownership, NaN ordering, and global aggregation without hash probes.
Representative archive latency/RSS benchmarks and live-provider retention
qualification remain deferred.


Qualification (2026-10-16, after merging `origin/main` `b79c65538f`): all 502
full native lake tests (188 shared-engine and 314 server), 29 focused SQL tests,
and 31 runtime ABI tests passed with zero native failures or leaks. All 50 real
Parquet/Iceberg E2Es passed against the final Debug CLI in 626.48 seconds with
production disk safeguards enabled. Generated error parity, Zig formatting,
whitespace, licenses, and the Apache boundary (2,066 source files) passed. The
private error-name table includes main's new promotion-writer fencing error.


### Bounded global state, typed expression delivery, and adjacent page envelopes

Global aggregation admits one group independently of input batch length. Fixed
primitive reducers update that state in windows of at most 256 lanes; variable
MIN/MAX admission reserves the largest possible winner. Physical descriptors are
normalized once per batch and reused for admission, semantic key hashes, exact
typed key lookup, and aggregate updates. The small-budget path avoids per-row
hash probes without bypassing exact NUMERIC, DISTINCT, pattern, spill, or error
semantics.

Computed primitive expressions deliver owned typed payloads and separate null
states through the existing batch reader contract. Their instruction DAG retains
shared subexpressions and uses shared CPU scheduling for sufficiently large
cohorts; joined output copies primitive lanes without a Datum delivery matrix.
INT16/INT32 validation applies only to selected lanes. REAL inputs and each REAL
instruction retain scalar rounding, overflow, and underflow behavior. Borrowed
retained columns expose native payload/dictionary views; portable spill blocks
decode numeric wire lanes once into bounded typed buffers, preserving borrowed
text and dictionary entries. Complex values retain their exact fallback.

Aggregate FILTER expressions form lazy cohorts. Only surviving physical lanes
evaluate aggregate inputs. Sparse cohort positions gather group IDs directly
into bounded native updates; scalar fallback observes SQL NULL elsewhere.
Keys resolve once, unrelated unfiltered inputs stay native, and
filtered-out division/coercion errors remain unevaluated. Ordered and DISTINCT
consumers receive the same logical rows and null semantics as before. Compact
computed dictionary entries retain their existing encoded dictionary path.

Failed regex/fuzzy executions are removed from the query-owned pool on lease
return, including failures caused by the parent budget rather than the child.
Their tables are reclaimed before a private retry starts; the first attempt's
reader and iterator also close before retry. Later traversals can build and reuse
a healthy bounded execution when headroom becomes available. Actual allocation
failures still propagate, and cache pressure does not change matches/statistics.

Parquet prefetch merges only adjacent explicitly planned cold ranges, with zero
gap and a 2 MiB envelope target. At most four columns plan the current page plus
one adjacent indexed page each. Filtered/pruning consumers defer future payloads,
pruned current pages are not prefetched, and decoded hits skip encoded fetches.
Each exact page cache entry pins a slice of the shared immutable response; its
cache key retains source version and credential scope. Workers resolve exact
persistent-cache entries before coalescing only missing adjacent ranges.
Admission charges a shared envelope once until its last resident alias leaves;
pinned aliases remain resident and charged. Alias metadata remains independently
bounded by the entry limit. No cache lock covers disk or provider I/O. Member flights let required
reads wait for an in-flight envelope, cancellation/close joins every owner, and
shared admission can fall back to smaller individual page jobs. Dictionary and
unindexed header discovery retain their bounded path. The real Parquet restart
fixture now also checks mixed computed FILTER cohorts and wide global counts.

These are bounded execution improvements, not an archive-scale latency claim.
Representative latency/RSS benchmarking and live cloud retention qualification
remain deferred.


Qualification at `512f16a3dd` after merging `origin/main` `ce578d9033` (2026-10-16): all 503
full native lake tests (189 shared-engine and 314 server), 43 focused SQL
tests, and 31 runtime ABI tests passed with zero native failures or leaks. All
50 real Parquet/Iceberg E2Es passed against the final merged Debug CLI in
632.59 seconds with production disk safeguards enabled. The expanded Parquet
restart fixture also passed separately. Merge coverage includes compact spill
arrays, native parameter ownership, and leased array projections. Generated error
parity, Zig/Python formatting, Python lint, whitespace, license headers, and the
Apache product boundary (2,073 source files) passed. The private error table
includes the combined branch/main names; public runtime ABI values remain pinned.

### Distinct admission and typed relational delivery

Nested mapped, selected, projected and native-column views push their final
selection to raw producers before each coercion. Excluded narrow-integer and
REAL errors remain lazy; normalized readers may expose the existing physical
callback, while coercing readers implement the selection-aware callback.

A bounded group census admits newly discovered semantic keys rather than every
input row. Page-local dictionary tuples memoize resolved representatives, with
semantic hashing and exact equality across differing tuple identities. The
census caps representatives at 4,096 and tuple storage at 1 MiB, then keeps the
ordered spill/scalar fallback. Variable-width winners reserve maximum candidate
size per group; exact NUMERIC, DISTINCT and pattern state retain their existing
specialized paths. Group quotas and source ordinals remain unchanged.

Derived primitive projections carry native or compact dictionary batches through
CTE/subquery adapters. Projection programs retain bound-query ownership and
borrowed columns remain valid until the next producer pull. Unsupported values
and expression errors use the existing scalar prefix delivery, preserving lazy
errors, LIMIT and exact typed arrays/numerics. Sparse FILTER cohorts share their
selected positions instead of allocating parent-width scatter buffers for each
aggregate. These improvements add no new SQL or cache configuration surface.

Qualification of the distinct-admission and typed-delivery refinements
(2026-10-16): all 505 full native lake tests (191 shared-engine and 314 server),
54 focused SQL tests and 31 runtime ABI tests passed with zero native failures or
leaks. All 50 real Parquet/Iceberg E2Es passed against the final Debug CLI in
618.60 seconds. The expanded Parquet restart case covers derived native
projections feeding grouped FILTER aggregates. Selection/coercion regression,
persistent-tier prefetch, shared-buffer pin/eviction accounting, distinct-key
admission under a small quota, and lazy/error-prefix/allocation-failure behavior
are covered. Zig/Python formatting, Python lint, whitespace, licenses and the
Apache product boundary (2,073 source files) passed. Current `origin/main`
`ce578d9033` is included. Archive-scale latency/RSS benchmarking and live-provider
retention qualification remain deferred.

### Mixed projection delivery and pressure-aware grouping

Derived streaming queries propagate output demand before physical scan planning.
Unused expressions and their cold scan fields are omitted while schema widths
and ordinals remain stable. Predicate and named-window input dependencies are
retained, including scalar-subquery cardinality in unused definitions; ordered,
aggregate, window, recursive, set and materialized domains conservatively retain
the positional inputs they need. External projection providers retain their
existing invocation contract. This is internal planning, with no new SQL syntax.

A derived output batch may combine native primitives, compact dictionaries and
exact scalar arrays/NUMERIC/JSON-null columns. Speculative encoded evaluation
owns a separate reclaimable arena: an expression error releases that workspace
before evaluating the existing scalar error prefix. Unsupported scalar columns
are evaluated once alongside eligible native outputs. The mixed 49-column
regression now succeeds under its existing 448 KiB working-memory limit.

Expression kernels gather validated primitive/dictionary payloads from mapped,
selected and physical-only readers directly into typed kernel lanes. Selection
is pushed before coercion; SQL null masks, narrow integer checks, REAL rounding,
nonfinite rules and scalar JSON-null/exact-NUMERIC fallbacks remain authoritative.
Identity expressions borrow exact column views as well, avoiding full-row scratch
when an aggregate consumes an array or NUMERIC field.

Grouping admits distinct new state, variable-width winner growth, replacement
vector/hash capacities and whole-column mixed-extrema/key promotions. Shared
statement headroom participates in admission. Existing fixed-width groups remain
batched above half quota; oversized censuses
or reservations split into smaller prefixes instead of deoptimizing the entire
input. Partitioned spill ingestion reuses semantic hashes, scatters bounded
256-row windows and writes the existing native column-block codec, preserving
per-partition input order, raw/partial markers and source ordinals. Order-sensitive
and specialized exact aggregate paths retain their existing fallback contracts.

Qualification of mixed projections, capacity-aware group admission and physical
expression gathers (2026-10-16): the full SQL gate passed 990 tests, with three
opt-in benchmarks skipped; all 505 native lake tests and the runtime I/O ABI
gate passed, with zero failures or leaks. Focused final spill validation,
allocation-failure, mixed cursor and named-window dependency checks also passed.
All 50 real Parquet/Iceberg E2Es passed against the final Debug CLI in 642.72
seconds. The expanded Parquet restart case checks mixed exact arrays/native
outputs and unused-expression pruning. Zig/Python formatting, Python lint,
whitespace, licenses and the Apache product boundary (2,073 sources) passed.
Current `origin/main` `ce578d9033` is included. Archive-scale latency/RSS
benchmarks and live-provider retention qualification remain deferred.


### Native scan identities, exact predicate masks and spill reduction

Contiguous Parquet/Iceberg batches carry a shared external identity span.
The batch count and on-demand row-reference accessor preserve snapshot, file,
row-group and physical ordinal authority without a reference allocation per
row. Explicit identities remain available for arbitrary inputs. Slicing, delete
masks, hydration, pagination, replay and index builders consume the same accessor;
retained builds clone the shared identity once and preserve the compact span.

Residual scan predicates dispatch once per native column. Dense integer/float
comparisons use SIMD lanes; selected dictionary entries use a fixed bounded memo.
SQL NULL, excluded lanes and signed timestamp boundaries remain exact. NUMERIC,
wide datetime bounds, mixed integer/float comparisons, nonfinite coercion and
complex values retain the established scalar boundary. Literal coercion stays
lazy when no nonnull selected lane needs it. Embedded pruning remains advisory.

Native spill ingestion sizes and encodes selected primitive/dictionary columns
without Datum row reconstruction, using the existing portable block format.
Compatible raw partition blocks feed typed grouped state atomically; partial AGS
checkpoints, specialized exact state and sorted skew fallback retain their
existing representations and order. No borrowed block survives its owning read.
Declined group censuses use independently bounded workspace and iterative
reclaim-before-retry windows. Singleton fallback needs no census. Ordinary SQL
row/cardinality errors share one prefix-delivery classification; cancellation,
resource and integrity errors remain immediate failures.

Qualification: the full SQL gate passed 997 tests (three opt-in benchmarks
skipped); native lake integration passed 509 tests and the runtime I/O ABI gate
passed, with zero failures or leaks. All 50 real Parquet/Iceberg E2Es passed
against the final Debug CLI in 635.73 seconds, including filtered compact identity
stability across cold restarts. Focused allocation-failure, scalar-predicate
parity, mixed spill validation and direct/wrapped allocator pressure checks passed.
Formatting, Python lint, license headers and the Apache boundary (2,074 sources)
passed. Latest fetched origin/main `ce578d9033` is included. Representative
archive-scale latency/RSS and live-provider retention qualification remain deferred.

### Compact physical views and adaptive checkpoint reduction

Native SQL physical views borrow retained packed validity with a bit offset and
represent contiguous row selections as ranges. Sparse/permuted selections retain
explicit coordinates. Computed primitive outputs and spill readers share this
trait; hashing, admission and reduction reuse prepared block columns. Owned replay
materializes validity once at its ownership boundary. Selected coercion, REAL
rounding and SQL NULL identity retain their existing validation contracts.

Physical normalization has independently bounded optional workspace before the
group census. Admission follows immutable parent allocators through the SQL
synchronization and spill wrappers, preserving shared statement headroom. Declined windows release that workspace and retry smaller inputs;
actual allocator failures still propagate. Direct native result streams now use
the same SQL row-error classification as derived projections, preserving valid
prefixes before invalid casts without retrying resource or control failures.

Compatible AGS v2 checkpoints restore borrowed primitive columns and merge COUNT,
integer SUM, boolean reductions and eligible extrema after one destination-key
census. String extrema copy only winning payloads. Every signature and complete
window is checked before retained mutation; declined suffixes keep their original
checkpoint and ordinal. NUMERIC, DISTINCT, pattern and order-sensitive states
retain their established exact/sorted paths. No durable format or ABI changes.

Oversized fixed-state hash partitions may repartition on a discriminating unused
hash bit. Binary children, at most eight passes, descriptor headroom and disabled
nested worker fan-out bound resource usage. Identical hashes and unsupported state
keep sorted skew reduction. All partition staging is sealed and released before
reduction, preserving workspace for projections and downstream blocking operators.
The real Parquet restart fixture covers high-cardinality nested aggregation and
pgwire delivery of a successful row before an invalid text cast.

Qualification of compact physical views, allocator-aware admission, checkpoint
columns and adaptive radix reduction (2026-10-16): the full SQL gate passed
1,003 tests (777 shared-engine and 226 server), with three opt-in benchmarks
skipped. All 510 native lake tests (194 shared-engine and 316 server) and the
runtime I/O ABI gate passed with zero failures or leaks. All 50 real
Parquet/Iceberg E2Es passed against the final Debug CLI using two test workers in
325.57 seconds. Coverage includes direct/wrapped shared-pressure admission,
packed-validity ownership, partial signatures/truncation, allocation failures,
repartition progress/skew, descriptor pressure and real restart/error-prefix
flows. The fixture retries only explicitly retryable, unadmitted index-creation
catalog conflicts. Formatting, Python lint, whitespace, staged license headers
and the Apache boundary (2,074 sources) passed. Latest fetched origin/main
`ce578d9033` is included. Representative archive-scale latency/RSS and
live-provider retention qualification remain deferred.


### Survivor projection and exact spill admission

Scan-level SQL predicates pass an exact residual/delete/dynamic-filter mask
into Parquet projection. Cursor-owned pages validate framing and retain decompressed wire
payloads, definition coordinates and dictionary IDs. Projection converts only
surviving primitive, timestamp and decimal values; strings borrow immutable page
bytes. Eligible dictionary projections retain their dictionary identities for
downstream expression/group kernels. Rejected positions remain masked SQL NULL
placeholders in the original physical coordinate range. Misaligned column pages and later cohorts reuse the
same validated framing. A partially projected vector never becomes a shared
full-page cache entry. Broad page-local cohorts, complete cache hits and
unsupported layouts retain their existing paths. Density uses the projection
page intersection, even when predicate pages have different boundaries. Retained
page allocators switch to stable cursor backing after workers join, before scoped
locking adapters leave scope.

Dynamic scan filters dispatch per primitive column and hash fixed 128-row stack
windows, avoiding width-by-row Datum arrays. Dictionary memo state is bounded to
one window. Exact NUMERIC, unsupported coercions and NaN domain bounds retain
scalar semantics with reusable lane scratch. This applies equally to Parquet and
Iceberg scans; complete index-driven dynamic joins keep their existing planner.

AGS v2 checkpoint columns now restore exact NUMERIC SUM/AVG coefficients without
Aggregate/OwnedRow reconstruction. Admission reserves the union of exponent
ranges for every destination group before mutation. Raw NUMERIC spill blocks use
the typed key census as well. Adaptive radix reduction includes exact states;
file, descriptor and shared-workspace headroom constrain additional depth, and
exact finalization leaves room for downstream delivery and sorting. DISTINCT and
order-sensitive reducers retain their established exact paths. Wire formats and
public tuning surfaces are unchanged.

Retry selections have their own optional admission budget, so an oversized view
can shrink before allocating from statement headroom. Required allocation
failures still propagate. Vector-overlap qualification waits for archive entry
and recognizes either lane-entry order. The real Parquet restart fixture also
checks sparse wide/decimal/nullable projections; existing real Iceberg coverage
exercises dictionary projections, schema evolution, partition pruning and deletes.

Ancestor quota diagnostics follow scheduler and spill allocator wrappers. Reusable
pattern executions inspect local admission and their own DFA allocation-failure
state, so an ancestor's historical quota failure cannot invalidate healthy forks
after headroom recovers. Failed executions are still discarded and reconstructed.
Hard overlay and transform-size diagnostics likewise inspect their local budget;
parent cache pressure remains retryable and can use private ownership. Cancellation
and provider errors retain their own identity even after quota pressure.

Qualification: the full SQL gate passed 1,009 tests (three opt-in benchmarks
skipped), all 515 native lake tests and the runtime I/O ABI gate passed with zero
failures or leaks. Coverage includes retry-view pressure through direct and locked
allocators, allocation-free dynamic filters, selected nullable/dictionary framing
and ownership failures, exact coefficient unions, checkpoint truncation, and
small-budget NUMERIC spill/sort execution, and WAL private fallback/headroom
recovery. The archive/recent overlap fixture
passed 10 consecutive focused runs. All 50 real Parquet/Iceberg E2Es passed in
312.11 seconds against the final Debug CLI; its SHA-256 stayed unchanged across
the run. Formatting, Python lint, whitespace, staged license headers and the
Apache boundary (2,076 sources) passed. Latest fetched origin/main `968f037c9a`
is included. Representative archive-scale latency/RSS and live-provider retention
qualification remain deferred.

### Native skew joins, encoded predicates, and fixed decimals (2026-10-17)

This revision closes the wide-key aggregation admission regression and extends
the existing native execution interfaces:

- Encoded group retries switch to direct scalar lane reads at one row. Optional
  provider physical evidence is validated one column at a time in separately
  reclaimed admission scopes. Neither selection directories nor physical-view
  arrays must fit alongside wide grouping state; real allocator failures and
  invalid provider evidence still propagate. Input ordinals and scalar spill
  behavior are preserved for both original singleton batches and retry windows.
- Ordinary SQL joins already used Grace partitioning and typed sequential spill
  blocks. The remaining inseparable-partition fallback now compacts its existing
  build/probe runs into bounded blocks, then replays a build block across a probe
  block. This replaces per-candidate linked disk records in that path. Accepted
  build matches use a disk-backed byte map buffered per build block; probe match
  flags stay bounded to one block. Residual ON predicates remain authoritative,
  including both sides of full joins. The low-level reusable HashJoin disk API
  retains its existing bucket-chain representation for callers that cannot spool
  their probe relation. No general join reordering or SQL ordering contract changes.
- SQL scan predicate evidence can borrow validated encoded Parquet pages. A
  page-level mask callback dispatches once by type/operator, uses SIMD for dense
  primitive numeric runs, and memoizes dictionary comparisons with bounded state.
  Unsupported encodings/logical interpretations still use the complete decoder.
  Nonfinite coercion failures decline before changing masks; excluded values and
  projected payloads remain lazy. Required page framing and dictionary IDs remain
  validated. Non-nullable pages no longer allocate identity-position arrays.
- Native decimal scan vectors carry i128 coefficients plus precision/scale.
  Declared NUMERIC cells enter base-10000 exact state directly, preserving display
  scale and all 38 digits. Raw/string consumers format at their boundary. Native
  coefficient ownership, slicing, result preparation, replay, deletes, index
  construction and generated source/error catalogs follow the existing contracts.

The skew regression fixture evaluates 48,000 candidate pairs using 1,204 typed
build-block decodes, including its final unmatched-build pass. It verifies residual
acceptance and both outer sides. Separate fault injection covers early close and
match-state ownership. The misaligned-page fixture now materializes 11 predicate
values and 11 payload values from 1,024 source rows, while evaluating four encoded
predicate pages. These are bounded-work regression checks, not archive-scale
latency/RSS measurements. Plain and dictionary decimal128 files are added to the
real Parquet NUMERIC seek/join, algebraic aggregate, and pruning/restart E2Es.

The final integration also carries native coefficients into canonical ordered
NUMERIC index keys. String-declared index keys, raw predicates, text sidecars and
raw string group sidecars format into caller scratch and retain fixed display
scale. Encoded predicate eligibility matches the full decoder's physical/logical
annotation rules before borrowing a page; unsupported interpretations keep the
established decoder and errors.

Latest fetched origin/main `acf50f2f60` is merged. All existing main runtime-error
wire identifiers remain pinned; the branch-only decision limit detail follows
main's last assigned detail. Reduced storage chunker owners implement main's new
model-input boundary and explicitly reject unavailable model execution rather
than flattening binary inputs.

Qualification on the merged tree: 1,016 SQL tests passed (three opt-in benchmarks
skipped), all 521 native lake tests passed, and the runtime I/O ABI gate passed
with zero failures or leaks. All 32 runtime error ABI tests and the reduced
chunker's model-input/configuration regression passed. The final shared native
rerun includes canonical decimal index keys, fixed-scale raw consumers and
unsupported Parquet annotation fallback.
All 56 real Parquet/Iceberg E2Es passed in 327.93 seconds against the final Debug
CLI with two workers; its SHA-256 remained unchanged throughout the run. Fixtures
and standalone roots were isolated under the worktree for this qualification.
This includes plain/dictionary decimal128 indexed joins, exact materializations,
pruning and restart coverage. Zig/Python formatting, Python lint, whitespace,
source/error catalog checks, license headers and the Apache product boundary
(2,282 source files) passed. Representative archive-scale latency/RSS and
live-provider retention qualification remain deferred.

### Coefficient kernels, hybrid runs and snapshot join planning (2026-10-17)

Fixed decimal128 vectors now enter exact comparison and hash kernels through a
stack-owned canonical NUMERIC view. Common comparison literals lower once to a
coefficient at the column scale. Fractional literals that cannot be represented
at that scale, huge values and specials retain exact arbitrary-precision
comparison without allocating a NUMERIC value per source row. Projection kernels
publish ordinary native boolean vectors, preserving selected/duplicate lanes,
SQL NULLs and the existing scalar error-prefix fallback. String declarations and
NUMERIC modifiers keep their existing coercion boundary.

An opt-in coefficient view follows column projection, selection and normalization
wrappers. Hash/group census scratch reuses one stable NUMERIC slot per key column;
retained keys still enter the canonical typed store. SUM/AVG update the existing
wide base-10000 accumulator directly, including FILTER cohorts. Only retained
state growth allocates; a sum can exceed i128 without narrowing. Ordered NUMERIC
index keys use the same stack view. Optional preparation remains admitted and
falls back to smaller windows under pressure.

Parquet dictionary predicates retain validated hybrid RLE/bit-packed runs and
borrow packed payload bytes, extracting IDs with at most five byte reads rather
than a per-bit loop. Highly fragmented short runs use a flat ID vector
when that is smaller. Repeated runs can reject a dense mask interval with one
write; nullable coordinates and projected survivor IDs retain their existing
interfaces. All logical dictionary IDs and framing remain validated before a
page is exposed, including truncated streams and padded final packed groups.

Join planning runs after statement snapshot capture. Up to eight eligible inputs
use bounded dynamic programming over connected left-deep inner-join orders;
only a strictly cheaper estimated plan replaces the original tree. Identity
keys and pure equality conjunctions are required. Outer, lateral, correlated,
recursive, effectful and unsupported shapes retain their existing plan. Explicit
output permutations preserve logical columns, including typed spill output,
parallel worker residual evaluation and join partial aggregation. Independent filter producers retain their evaluation
boundary. Their estimates account for AND/OR selectivity and LIMIT; ordinary
row/byte estimates remain advisory. Providers can supply estimates after their
captured conditions, and lake scans use already-owned candidate or row-group
pruning evidence without fetching footers or enumerating lazy inventories.
Absent precise evidence, selectivity and key cardinality remain costing
heuristics; no estimate limits execution or substitutes for exact matching.
Planning has its own bounded owner and declines under resource pressure.
Build-side selection first preserves a negotiated native exact-index probe when
only one side offers it. Compressed scan estimates alone must not switch away
from an available indexed/covering probe, including after a cold restart. The
metadata-only capability negotiation reuses the exact-key spooling contract.
A biased-estimate runtime fixture refuses indexed source reads until its filter
is installed; the real-file cold covering join exercises the durable path.

Lake-kernel qualification before the final startup/linking merge: all 1,022 SQL
tests passed (three opt-in benchmarks skipped), all
525 native lake tests passed, and the runtime I/O ABI gate passed with zero
failures or leaks. All 32 runtime error ABI tests passed. This includes the
biased-estimate indexed-probe fixture, both spill-worker orientations, wide-key
memory pressure, decimal allocation-failure coverage and every packed-ID width.
All 56 real Parquet/Iceberg E2Es passed in 334.09 seconds with two workers against
the final Debug CLI. Its SHA-256 remained
`99b5642c0dfa98420d3c016f8a298783a85bca7921fac14232d70be6397be423`.
The targeted covering/non-covering cold-index regression also passed in both
modes. Source/error catalogs, Zig/Python formatting, Python lint, whitespace,
license headers and the Apache product boundary (2,285 source files) passed.
Fixtures and server roots were isolated under the worktree.

Final main integration also merged origin/main `2de3e3780c` cleanly after it
advanced during qualification. The rebuilt CLI passed all 62 post-merge E2Es
in 353.60 seconds: the complete 56-case real Parquet/Iceberg suite and six
credential-rejection startup cases across standalone, metadata and data roles.
Its SHA-256 remained
`60ec441d33f83bdc6142587305a9b04bb3f04254a6f3f6d051dd79ee9cc9f501`.
The standalone runtime/linking gate passed, including the inline inference
provider fixture, production-linked ABI probe and localhost TLS fixtures.
Post-merge source/error catalog and Apache boundary checks passed. Real-provider
retention qualification and representative archive-scale latency/RSS remain
deferred.


## Native planning and nullable/decimal kernel refinements

Identity join-key lineage now crosses bounded, effect-free projection/filter
frames, including nested aliases and reordered output columns. Exact-index
capability negotiation and runtime-filter installation share the same lineage.
LIMIT/OFFSET, cardinality checks, ordering, aggregates, windows, invocation
references and computed or fallible projections/predicates remain opaque.
The filter stays borrowed until the statement closes; no source is pulled to
negotiate it. Existing join-kind rules still protect preserved outer rows.
Cold covering-index E2Es also join a filtered, renamed derived table after the
original Parquet data pages have been made unreadable.

Snapshot cursors can expose advisory column statistics: distinct/non-null counts,
population, uniqueness, integer domains, average width and up to 32 semantic
key frequencies. Captured resident Parquet metadata supplies these without
additional object requests; optional preparation declines above 512 files,
8,192 column chunks or 256 requested columns. Footer null/distinct counts now
survive inventory format v19; v18 and earlier supported inventories still read.
Absent global NDV sketches, merged per-group distinct counts and integer domains
remain estimates. Constant row groups contribute bounded heavy-key observations.
The bounded join enumerator caches metadata/capabilities once, costs equality
fanout and hot keys, row-width estimates, indexed probes and spill write/read traffic.
Build orientation compares estimated retained bytes while preserving an available
exact probe. Statistics never limit rows, prove pruning, or replace exact matches.

Nullable Parquet pages retain repeated definition runs or packed membership plus
64-lane rank checkpoints. Homogeneous present/absent pages need no per-row
coordinate storage; fragmented streams use a compact bitmap. Gathers use one
machine-word popcount instead of scanning definition bytes. Predicate masks can
skip whole null intervals and intersect repeated dictionary-ID runs with present
intervals. All framing and logical IDs are still validated before publication.

Decimal expression kernels support nested addition, subtraction, multiplication,
remainder, division and signs over native coefficients, integer columns and
constant operands. Scale alignment and arithmetic are checked. Division shares
the exact kernel's PostgreSQL display-scale rule and rounds half away using
bounded integer quotient/remainder operations. A cohort needing different
per-row division scales, wider precision/scale, special values, type modifiers
or intermediate overflow retains the exact scalar path. Owned coefficient
outputs keep their native decimal trait through projection, hashing and reduction;
scalar/wire boundaries materialize canonical limbs only for consumed cells.
Metadata precision limits are validated once per vector.

Archive-scale latency/RSS measurements and real-provider retention qualification
remain deferred. These refinements do not claim a measured archive speedup.

Qualification after merging origin/main `1b4b255aae`: all 1,024 SQL tests passed
(798 core and 226 API; three opt-in benchmarks skipped), and all 529 native lake
tests passed (211 shared and 318 API), without failures or leaks. The runtime I/O
ABI gate passed, as did all 40 runtime error/native ABI checks. Five focused
inventory compatibility, statistics, definition-rank allocation-failure and
nullable dictionary budget checks passed; a 65,536-row optional dictionary page
fits the 16 KiB decode grant without expanded definition/ordinal arrays.

The real-file suite passed 55 cases in its full run. Its new arithmetic fixture
initially used an inferred string column; retaining that coverage and adding an
explicit NUMERIC attachment over the same files fixed the fixture. The corrected
streaming/arithmetic case passed across two cold restarts in 14.07 seconds, so
all 56 Parquet/Iceberg cases are covered. All six credential-rejection startup
cases also passed after the main merge. The CLI SHA-256 stayed
`372501d99c8dd1c7506817979826c5f3dd906239a0e454a0a0e8ab5232763a26`
through these runs. Zig/Python formatting, lint, source/error catalogs, license
headers, whitespace and the Apache boundary passed. Repository cache GC completed
and preserved active build output; uv pruning timed out on the shared cache lock
without interrupting its owner. Archive benchmarks and live retention qualification
remain deferred.

## Decimal continuity and durable planner statistics

Exact coefficient batches now survive derived query projections, native joins,
retained typed stores and SUM/AVG reduction. Kernel dispatch decides capability
for all derived expressions, rather than excluding NUMERIC through the primitive
kernel's type gate. Retained finite values with at most 38 coefficient digits
use i128 coefficients and display-scale vectors; wide values and special values
promote once to the general exact representation. Hashing and candidate equality
borrow canonical stack views, and uniform-scale stores avoid a scale gather.
SQL NULL remains a separate bitmap, including mixed/duplicate selections.
Scalar cell materialization belongs to the retained store, with lazy canonical
cells cached by row under its read lock. It does not leak allocations through a
caller's reclaiming allocator. Physical hashing and exact reduction continue to
borrow coefficient views without materializing those cells.

A bounded decimal expression workspace shares pure subexpressions across the
projection forest. Constants broadcast from one lane. Scale alignment and
quotient rescaling use checked power-table multiplications. Division can retain
per-lane PostgreSQL display scales, including across further arithmetic,
subqueries, grouping and retained state; it no longer declines an entire cohort
merely because magnitudes need different scales. The optional workspace admits
at most 8 MiB (and at most one quarter of visible parent headroom). Exhausting
its local grant declines acceleration; backing allocation failure, SQL errors,
wide intermediates, constrained numeric modifiers and unsupported instructions
retain their existing behavior. Required output allocations remain outside the
optional fallback boundary.

Native publication now builds advisory planner statistics from the same
projected, delete-aware reads used by native indexes. Up to 64 primitive indexed
column paths receive per-file sketches, each with 64 HLL registers, counts,
retained-width observations, integer bounds and four weighted Misra-Gries
heavy-key counters. Sketches are fixed, versioned 224-byte records and ordered
page aggregates. They estimate distinct values and skew; they never prove
uniqueness, prune data, limit rows or replace an exact predicate.

Each column owns an immutable file-keyed tree. Refresh deletes removed/replaced
leaves, replaces completely read files, and path-copies affected summaries.
Unchanged complete trees need no source reads or file-by-file statistics census.
Replay and ordinary streaming readers both capture sketches, so oversized files
can retain streaming execution. Older publications backfill missing leaves once
using the bounded projected source/replay path. Backfill checkpoints retain
partial file trees every 64 completed files; a matching resumed build reuses
those leaves. Partial summaries are never exposed to planning. Incomplete columns
are omitted from final estimates.
Only complete file coverage enters the published compact root summary, which
SQL planning can consume without loading footers or flattening lazy inventories.
The same estimates are exposed by physical and native ordered-index cursors.

Source navigation v3 and reader protocol 50 fence these new references. All GC
modes traverse the statistics trees, and cloud generation maps/certificates bind
them through the published navigation root. Immutable metadata cache pins retain
only costing data; they do not extend mutable reader authority. Iceberg lazy
navigation and ordinary Parquet source opening both expose matching published
statistics. There is no new SQL syntax, attachment option or result format.
Archive-scale latency/RSS measurements and live-provider retention qualification
remain deferred; these changes do not claim a measured archive speedup.

Qualification of the native NUMERIC and durable-costing changes: all 1,028 SQL
checks passed (three opt-in benchmarks skipped), including exhaustive allocator
faults and retained-owner budget reclamation. The installed CLI passed all 56
real Parquet/Iceberg E2Es in 658.75 seconds with an unchanged executable hash,
including derived decimal aggregation across cold restarts.
All 300 native lake API checks, both lake scaffold groups (466 and 151),
the 213 shared focused lake checks, and the runtime I/O ABI gate passed.
The combined SQL/lake/API/ABI/CLI build exited successfully with zero test
failures or leaks. Source/error catalogs, license and Apache-boundary checks,
Python lint/format and whitespace checks also passed.

Merged origin/main `08cfbb73c1` without conflicts (`dfa59c1b79`). The
post-merge inference registry tests, runtime I/O ABI gate and CLI rebuild passed,
as did all four upstream GLiNER migration/preparation Python checks. The rebuilt
CLI passed the real Parquet decimal and Iceberg snapshot/schema/delete/restart
regressions (2 E2Es, 25.48 seconds), with an unchanged executable hash. Source
and error catalogs, formatting, licenses and the Apache boundary (2,286 sources)
also passed after the merge. Repository cache cleanup completed; uv pruning
skipped a shared cache lock rather than interrupting its owner.

### Signed statistics, fused decimal execution, and stable public navigation

Mixed-sign DECIMAL footer bounds are validated after logical annotations are
known, using signed two's-complement coefficients. Exact scale-aware comparisons
also drive decimal row-group, page-index and Iceberg file pruning. Timestamp
bounds use signed nanoseconds; missing, malformed or incompatible bounds remain
conservative. Declared scale rounding keeps raw decimal bounds residual when
the physical scale exceeds the declared scale. Static conditions and runtime
join ranges share these typed
predicates, while existing exact row filters retain result authority.

NUMERIC COUNT/MIN/MAX consume coefficient/validity vectors directly. Extrema keep
one coefficient and display scale per group and materialize canonical numeric
storage only at scalar delivery; wider or special values promote to the exact
state. Merges preserve the same representation. Pure decimal expression forests
execute through bounded, shared registers, allocating coefficient vectors only
for final outputs. Unsupported or overflowing lanes retain scalar semantics.

New publications use source navigation v4, reader protocol 51 and inventory v20.
The public file tree names files with SHA-256(source, empty snapshot, file), while
public `lake3:` row IDs carry separate full SHA-256 snapshot and file digests,
followed by fixed-width group/row coordinates. Changing snapshots therefore
changes every public ID's authority without rewriting every public file key.
Membership deltas path-copy added/removed keys; replacements reuse the same key.
Readers continue to support legacy inventories, navigation and `lake1:` IDs.
Private `lake2:` posting identities remain unchanged. Search hydration, ordered
pagination, highlight attachment and lazy/full-scan ordering negotiate the same
identity version, with separate cache entries for legacy and current ordering.
Snapshot proofs are checked before a public lookup or continuation is accepted.
Ordering cache entries carry explicit identity-version provenance independently
of their payload variant, preserving legacy coverage-proof leases. Builders
select the version before initial source planning; legacy flat callers rebuild
once before lending parallel views, avoiding a full sort per changed-file scan.

This removes the public-directory sort and rebuild from compatible refreshes.
The current inventory proof census and its sort still cover all source files;
this change does not claim total publication CPU or memory is proportional to
only the delta. Source-native incremental proof construction remains a separate
opportunity requiring trustworthy manifest lineage and delete coverage.

Qualification with merged `origin/main` at `408644a861` (merge `58b87ea765`):
1,030 SQL checks passed (804 shared, 226 server; three opt-in benchmarks skipped),
with 468/151 lake checks and 217/301 shared/server API checks passing without
failures or leaks. Final ordering refinements passed four focused publication,
legacy-coverage, navigation and costing regressions and the 300 other API cases;
the unchanged spilled-pack proof case passed in the full 301-case owner. Runtime
I/O ABI and the 63-step installed CLI build passed. All 58 real Parquet/Iceberg
E2Es passed in 356.40 seconds with two isolated workers and unchanged executable
SHA-256 `434ecd856ba6c02b8ea664676522faa338feafb528e50dc83f5b29914cc0a51d`.
The new mixed-sign statistics cases cover scalar/native arithmetic, extrema,
filters, join ranges and cold restarts for both independent writers. Packaging
passed 13 tests. Formatting, Python lint, source/error catalogs, whitespace,
license headers and the Apache boundary (2,286 sources) passed. Focused emitted
test binaries were removed; required `make gc` completed, with the busy uv cache
left alone after its bounded lock timeout.

### Source identity, normalized filters, and committed Parquet discovery

The follow-up fixes REAL dynamic-filter false negatives and makes row identity
a durable source-binding property. Both dictionary and flat native lanes use
SQL width normalization before range/Bloom checks. New source bindings select
identity version 2 once; older bindings retain version 1 through index lifecycle
changes. Scalar binding now prepares immutable decimal execution templates, and
decimal precision limits use precomputed constants.

Explicit `.parquet-manifest.json` source heads provide fresh, small commit
selection and immutable checksummed file-list chunks. Matching published native
navigation defers chunks completely for selected serving. Persistent chunk
catalogs provide lineage for incremental native proof preparation for both
Parquet commits and Iceberg. The proof census reuses unchanged hashes, path-copies
changed branches and supplies range summaries without per-refresh proof sorting
or prefix arrays. Changed delete evidence conservatively rebuilds the census.
Canonical coverage also reuses ordered file inventories.

Flat builder arrays, inventory serialization and the canonical coverage hash
remain linear in file count. Raw uncommitted prefixes still require complete
listing for freshness. These residual costs and live-provider/archive-scale
qualification are not hidden by the new preparation caches.

Qualification of the committed-Parquet and proof-census refinement passed
1,031 SQL checks (three opt-in benchmarks skipped), lake groups of 472 and 151,
and the shared API group of 222, without leaks. The server API sweep passed
301 cases; its lone outdated publication assertion expected an index build to
upgrade a legacy source identity. The corrected assertion explicitly preserves
version 1 across binding, source and navigation, and passed its focused rerun.
All 61 real Parquet/Iceberg E2Es passed in 358.89 seconds against the same final
executable, including the new plain/dictionary REAL joins, saved IDs through
publication/removal/restart, and cold committed-Parquet navigation while an old
file chunk is unavailable. Runtime I/O ABI, all 13 packaging checks, generated
SDK/frontend builds, source/error catalogs, formatting, Python lint, whitespace,
licenses and the Apache boundary passed. The branch includes origin/main
through 0f85c7907f (merge b4958dd66b). Archive-scale latency/RSS and live cloud
retention qualification remain deferred.

### Protocol 53 publication and native join delivery

The remaining static REAL filter gap now normalizes stored DOUBLE cells at the
SQL width boundary while preserving each operand's comparison domain. Encoded
physical masks decline REAL until their callback carries a SQL type descriptor;
shared typed cell comparison handles flat and dictionary fallback consistently.
Committed Parquet catalog admission is optional under pinned-reader pressure and
falls back to already-loaded chunks, matching Iceberg planning.

Publication writes a compact, versioned paged inventory descriptor referencing the
existing authenticated source file tree. Canonical data coverage is a path-copied
Merkle reduction, with cached and streaming reductions sharing one format. Strong
immutable committed-Parquet catalogs avoid flattening merely to establish data
coverage; unresolved Iceberg selectors and mutable sources retain qualification.
Consumers borrow immutable catalog rank directories. Compact mutable builder
arrays remain linear and are not described as fully delta-sized producer state.

The origin/main SQL memory changes are integrated with native physical/decimal
views, output reordering, and block-skew fallback. Native join batches expose
primitive columns, forward encoded JSON, and evaluate residuals before outer
match bookkeeping. Cold Parquet metadata uses bounded shared scheduling and
serial large-object fallback. Live-provider retention and full-archive latency/RSS
qualification remain separate from deterministic contract coverage.

Qualification after integrating origin/main's SQL memory changes passed 925
shared SQL checks (three opt-in benchmarks skipped), 226 server SQL checks,
and 224 query-contract checks, without leaks. Lake/API sweeps passed 472, 223,
and 319 checks; their only failures were the schema-prefetch fixture missing a
schema and two legacy protocol fixtures retaining the new inventory descriptor.
The corrected fixtures passed focused reruns. Frontier owners passed 26 and 70
checks, and focused native join, protocol, coverage, navigation, and REAL
parameter regressions passed. The real Parquet E2E suite was extended for static
REAL comparisons but could not be rerun: available disk remained below its
16.25 GiB admission requirement. The earlier 61-case E2E result above applies to
the prior executable, not this merged implementation.

The branch includes origin/main through `055bd5f7c9` (merge `c0c0d6e6b0`).
All 30 packaging tests completed successfully with pinned Zig 0.17 (two
platform-specific checks skipped). Source/error catalogs, Python lint and
formatting, whitespace, license headers, and the Apache boundary passed.
The final installed CLI build and version smoke check passed after reclaiming
completed task-local compiler intermediates. Required repository cache cleanup
completed; its uv prune was stopped after waiting on the existing cache lock.

### Independent public-ID ordering, complete cutoffs and batched translation

Declared string sources now consult their exact physical mapping for exclusion.
A numeric-only dynamic template can no longer unconditionally suppress a declared
string. Exact historical projection witnesses authorize the existing admitted
leader-owned rebuild; unknown drift remains manual repair debt. Native definition
revision 11 revalidates bindings and changes only affected whole-document text
hashes. Selected-field text and vector bindings remain stable; native reader
protocol 84 fences the semantic transition without changing text metadata v13.

Plain `_id` sorting reuses public-file/current-row/index-by-ID navigation through
the exact membership collector, including forward/backward cursors. Competitive
bounds carry the complete winning public ID. Better primary groups retain all
identities; only equal groups receive the inclusive public endpoint. Optional
fallback withdraws the complete-order proof and retains completed top-K work.
Stable-ID and filename translation use bounded authenticated multi-get. Coalesced
and parallel reads share the optional quota under the owner lock, preserve the
required-read reserve and retain existing scheduler/memory admission.

The [public-ID evidence](bench/lake/native-public-id-v3-observation.json) retains
all 48 exact-answer samples, independent oracle derivation, actual binary/source
identities, workload/environment provenance and full-report hashes. On the same
16,384 rows/eight files, plain `_id` drops from 16,384 decorations to 21, with 21
ordered probes. Existing complete forward/reverse and wide-tie controls stay at
21 decorations. Local Debug warm medians fall from 1,293 ms to 24 ms; timings do
not isolate attribution or qualify cloud/full-archive throughput.

Separate deterministic regressions reduce a 64-row range to one scanned row with
a complete public-ID cutoff, and charge 196,760 bytes for 64 batched lookups versus
12,592,640 for scalar seeks. Tests cover both directions, equal-primary endpoint
proofs, duplicates/misses, optional row/read exhaustion, required reserve and
allocation failures. The final CLI passes 209 lake native tests and 93 logical
projection/query tests, with two benchmark skips and no leaks. Eighteen archive/
schema E2Es and both full native Parquet/real Iceberg lifecycles pass. An actual
old-to-new executable upgrade repairs the missing local posting and retains it
through a second restart; an existing remote Parquet publication remains queryable.
Relational external schemas reject dynamic templates, so the affected native
binding migration is independently covered at the planner boundary.


## Document-type projection, typed route bounds and refresh-key reuse

The latest refinement scopes physical source exclusions and named text subfields
to the selected document type, including undeclared dynamic strings. These rules
are rebound from the authenticated public validator while ASCH epoch bytes stay
unchanged. Their semantic witness authorizes the ordinary leader-owned shadow
repair only for known historical provenance. A real old-to-new executable test
repairs both the missing beta root posting and the leaked beta alias posting,
then retains the corrected results through another restart.

Reader protocol 85 / native text metadata v14 persist typed first/last bounds in
routing records. Eligible field-sort windows reject strictly worse primary
ranges before admitting descriptors or lineage. Equal primary keys remain live
for public-ID ties; exact counts/full-match evaluation do not prune. Unknown or
invalid evidence stays eligible. Existing v13 publications remain reusable and
missing bounds retain ordinary execution. Encoders capture new bounds while the
segment reader is resident; unsorted segments add no artifact reads.

Cold lineage loads reserve complete logical read charges before submitting
joined waves of up to eight shared scheduler I/O lanes. Saturation runs required
reads on the caller; publication-owned execution keeps its serial phase contract.
Immutable cutoff copies prevent prefetch from borrowing moving winner storage.

Native fragment spaces retain optional per-row typed ordering keys under an
ordering cohort and exact input-version digest. Unchanged rows reuse keys when a
fragment is sorted again; changed/missing keys gather current ordering columns.
Deletion removes cache entries, a changed cohort starts a new tree, and older
publications without keys retain source fallback. Only the current native space
owns this refresh state; consumer coordinate views do not retain obsolete key
trees. Existing publication certificates, cloud generation maps, reader leases
and GC walk the same identity dependency closure. See the [row identity
contract](../docs/plans/lake-row-identity.md#incremental-ordering-key-state-reader-protocol-85).

The [routing/refresh observation](bench/lake/native-routing-refresh-v3-observation.json)
retains deterministic work counts and all 48 exact-answer baseline/candidate
control samples on the same frozen 16,384-row Parquet source. A 257-row fragment
refresh gathers one changed row and reuses 256 keys; deletion gathers none, while
changed-cohort and absent-cache controls gather all remaining rows. Eight lineage
pages reach eight concurrent reads, or one under saturation. Nine real-segment
routing records retain two admissions and preserve original offsets across seven
pruned records. These are bounded-work checks, not real-cloud throughput results.

All control queries retain 21 decorated candidates. Warm medians in this run are
25–27 ms versus 22–24 ms previously; concurrent native/E2E validation prevents
latency attribution, so this refinement claims no measured latency improvement.
