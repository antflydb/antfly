# Native lake performance follow-up

Status: implementation planned. This follow-up builds on PR #1006 and keeps the
native execution model, public snapshot tokens, exact filters, and reader leases.
The items below are not implemented by this design change.

## Native sparse predicate intersection

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

Keep the indexed superset for a partially resolved conjunction. Iterate it in
bounded file/group/row windows, project only residual expression dependencies,
and evaluate the shared compiled predicate on those rows. Produce an exact
physical selection before vector ranking. OR and exclusions require exact sets;
unsupported expressions retain the authoritative fallback. Admission and spilling
must bound scratch without imposing a matching-ID list limit.

Acceptance: indexed point plus unindexed prefix/range/Boolean residuals match full
scans for text, dense, sparse, and hybrid requests. Include nulls, deletes, and
more than 100,000 selected rows. Record decoded rows and bytes to prove a selective
conjunct narrows residual I/O. Test cancellation and allocation failures.

## Ordered lake index top-N

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

Add a Roaring range-slice/rebase operation that copies or combines containers and
masks boundary words without iterating every selected row. Use it to lower global
predicate selections into segment-local filters. Add cardinality-only kernels for
supported Boolean/count shapes so counts need not materialize result candidates.
Preserve sparse arrays, dense containers, deleted rows, and full u32 boundaries.

Acceptance: differential tests against scalar iteration for empty, sparse, dense,
overlapping, unaligned, and boundary ranges, including allocator failures. Compare
large broad-filter counts and peak allocations with the existing scalar path.

## Shared temporal predicate semantics

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

- [ ] Implement native sparse ordinal intersection and selective forward scoring.
- [ ] Evaluate vector residuals over indexed physical selections.
- [ ] Add compatible ordered-index top-N execution.
- [ ] Add bulk bitmap slice/rebase and cardinality kernels.
- [ ] Prove and push down normalized temporal predicates.
- [ ] Run focused differential/OOM/cancellation checks and real Parquet/Iceberg e2e-full.
- [ ] Record cold/warm benchmark results, including a representative large archive.

Each implementation commit should state the workload, baseline, observed resource
counts, and remaining fallback shapes. Existing 100,003-row E2E correctness results
do not establish throughput for a 50-million-row archive. Preserve build, spill,
metadata, and response budgets throughout these changes.
