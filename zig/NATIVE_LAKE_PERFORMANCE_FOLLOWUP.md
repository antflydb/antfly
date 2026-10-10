# Native lake performance follow-up

Status: implemented; representative archive benchmarks remain pending. This follow-up builds on
PR #1006 and keeps native execution, public snapshot tokens, exact filters, and
reader leases. The contracts below describe the implementations and their
validation requirements.

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
