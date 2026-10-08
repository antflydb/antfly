# Native lake performance follow-up

Status: implemented; representative archive benchmarks remain pending. This follow-up builds on
PR #1006 and keeps native execution, public snapshot tokens, exact filters, and
reader leases. The contracts below describe the implementations and their
validation requirements.

## Native sparse predicate intersection

Implemented through query-owned compressed native ordinal selections. Native
sparse recipe v4 persists authenticated 1024-row physical-to-native ordinal
blocks inside the checkpoint. Each entry stores a two-byte physical offset and
four-byte native ordinal; ingestion coalesces writes with at most 64 resident
blocks. Inserts, replacements, deletes and compaction update these maps in the
same transaction. A completeness marker distinguishes missing vectors from
legacy checkpoints, which retain point lookup fallback until rebuilt. Queries
translate a physical selection with one lookup per occupied block and check
cancellation between blocks, without formatting a document key per selected row.

Selective sets (at most 4096 rows, scaled to the requested result count) use
36-byte forward locators and preserve zero/negative scores for overlapping terms;
absence of overlap is separate from score. Broader sets intersect native bitmaps
with authenticated posting-block ordinal trailers before decoding. Posting payloads remain V1; old readers ignore the extended range metadata,
and unextended blocks derive bounds without allocating decoded arrays. Exact multi-term accumulation retains a score map;
complete checkpoints select winners with a bounded top-k heap and sort only k
entries, excluding tombstones before admission. Legacy checkpoints retain the
full candidate sort and defensive identity fallback.

Persist authenticated mappings between physical file/group/row coordinates and
native sparse ordinals as part of each immutable publication. Share the physical
selection contract with dense and text readers; do not expand broad predicates
into public key arrays. Intersect selected ordinals with postings before scoring.
For highly selective predicates, choose a forward-vector scoring path using
cardinality and projected read cost. Keep mutable-index incarnation checks where
required; omit them only under an explicit immutable publication proof.

Acceptance: point and broad filters, exclusions, mixed terms, changed files,
deletes, restart, and cancellation agree with an exact reference scorer. Measure
reverse metadata reads, posting blocks decoded, scored rows, peak memory, and
provider bytes. A point predicate must not walk a common term's entire posting
list merely to resolve membership. Preserve score and tie behavior.

## Residual evaluation over narrowed physical selections

Implemented for vector predicates by retaining an indexed conjunction superset
and a separate residual IR containing only unresolved children. One pinned
Parquet cursor borrows compressed physical selections for the entire scan;
file/group/page pruning and reusable reader plans avoid reopening every 1024
rows. Only residual dependency columns are projected. Direct-column expressions
execute shared predicate leaves over page masks, preserving Boolean short
circuiting and projected document null semantics without per-row JSON objects.
Nested paths and document-ID expressions retain the shared document evaluator.
The resulting exact physical set is shared by dense and sparse membership.

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
when skewed membership defeats that estimate, retaining truthful traversal
counts and the `ordered_lake_index_then_text_postings` source. Compatible direct
relational keys stream pinned physical row
references without Parquet hydration. Safe required predicates provide tuple
bounds and equality prefixes; unsupported leaves remain native membership checks.
Forward cursors seek inclusively on the first ordered field and keep complete
boundary ties for public-ID comparison. The collector stops only after the
boundary key group, preserves exact totals, and reports matching candidates plus
`ordered_scanned_count` (all traversed physical references before membership).
Offset is supported. Backward cursors, timestamp sort domains, incompatible null
policies/collations and unproven orders retain the native doc-value fallback.
Existing scoring uses full-corpus statistics.

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
bitmap count over a segment without deletions uses rank/cardinality with no
result allocation. Compound filters retain bitmap operations and deletion masks.
Union/shift allocation failures now propagate with ownership-safe cleanup.

Fused ordered-index builds reserve spill-file capacity across the cohort: at most
eight simultaneous sorts retain four runs each, leaving room in the unchanged
64-file budget for pending writes and merge outputs. Level-aware compaction
remains in the shared sort implementation.

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

- [x] Implement native sparse ordinal intersection and selective forward scoring.
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

## Remaining work after the October 8 refinements

The checkpoint mappings, block rejection, bounded winner collection, residual-only
projection/page masks, costed ordered selection, equality prefixes and inclusive
forward seeks are implemented. Full signed timestamp ordering still needs a
versioned native datetime doc-value/cursor contract: native search currently uses
unsigned nanoseconds, while relational timestamps use signed wide integers.
Backward index traversal needs an explicit reverse cursor in the persistent tree;
a forward scan with a reversed comparator would not prove early stopping.
Multi-term sparse score accumulation still scales with matching postings; WAND or
another exact signed-bound algorithm would need a separate proof and benchmark.
No representative 50-million-row cold/warm throughput result is claimed.


Validation on 2026-10-08: merged origin/main at 272db51d52 and built the Debug
server with Zig 0.17.0. All 30 sparse tests, 319 SQL tests (three benchmark
skips), and 184 lake API tests passed. Column residual masks include exhaustive
allocation-failure checks and comparison with the shared document evaluator;
bounded sparse top-k matches the legacy collector with signed weights and deleted
winners. Block-map tests cover compaction, replacement, deletion, restart and
legacy completeness fallback. Posting tests prove unchanged V1 payloads and
legacy range-prefix decoding of extended metadata.

All 21 real Parquet/PyIceberg e2e-full tests passed together in 271.11 seconds
against the final binary. The 100,003-row two-file fixture covers nonpositive
sparse scores, point-filter native sorting with zero ordered probes, forward
cursor continuation in both sort directions, combined range/category bounds,
residual predicates, exact totals and public-ID ties. A text membership clustered
at the archive end triggers native fallback after at most 1024 ordered probes
and still returns the exact page and 1000-row total. Apache boundary validation
(1904 sources), Zig formatting, the joined public OpenAPI comparison and diff
whitespace checks passed. No cold/warm archive throughput benchmark is claimed.
