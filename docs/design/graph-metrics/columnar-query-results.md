# Columnar graph query results

Graph query callers read projected values through `GraphResultNode.metricCount()`
and `metricAt(column)`. These accessors accept both compact response columns and
legacy materialized `metrics` arrays. `metricAt` returns a borrowed name and an
optional score; a missing native score remains `null` at the API boundary.

```zig
for (result.nodes) |node| {
    for (0..node.metricCount()) |column| {
        const metric = node.metricAt(column);
        // metric.name and metric.score belong to this result.
    }
}
```

The `metrics` slice remains available for legacy payload construction, but it
can be empty on a query result that contains projected values. Callers that
need an independently mutable array can use `node.cloneMetrics(allocator)` and
release its values and container after use.

## Ownership and response construction

`GraphMetricColumns` owns one name per column and dense, independent, or paged
`f64` score backing. A bounded group directory supports projections read at
different stages, including independent row mappings and mixed flat/page
backing. Optional row mappings preserve logical response order.
Nodes hold a borrowed owner pointer and a bounded row ordinal. Results retire
nodes before retiring their column owners. A shard merger retains one stable
owner per contribution; growing the result node array does not relocate scores.
Clones copy columns into independent owners, so modifying a clone cannot modify
its source. Heterogeneous legacy payloads use one value slab and interned names.

Eligible flat columns and pages transfer into the response, including their
retained-memory credit. Page allocators are retargeted to stable response backing.
Copies remain necessary for nullable legacy columns, nonprojected page columns,
or backing that exceeds dense output by more than 256 KiB. No read lease or
stack-bound budget allocator escapes.
Required output allocations and permutation validation precede node ownership
commit. Result destruction releases the transferred storage normally.

The internal graph node JSON encoder emits the existing fields directly from
the accessors. Its parser accepts only the existing wire payload, so owner
pointers and row mappings never cross the transport boundary.

## Reads and memory admission

Unlimited fused selection gathers published projections during the survivor
scan. Batches contain at most 4096 score cells. Limited selection reads new
projections only for its selected rows. Staged published reads prove local key order first. Already ordered rows use
bounded gathers directly; shuffled rows sort a compact permutation globally
before gathering and scattering bounded batches. Qualified identities remain missing locally, duplicate
keys retain independent logical rows, and seeded personalization runs once over
its complete local relation.

The native typed key encoder uses exact-sized reusable storage independent of
general graph arena growth. Store residency and the borrowing query remain
accounted; retained encoder capacity is capped between leases. On a required
memory failure the coordinator retires the failing scope and retries the span
once at the same transaction checkpoint. A second failure clears the request's
outputs and propagates the error.

## Limits

Compact output still requires response-owned score storage. Eligible backing
moves without score copies; incompatible backing promotes into dense columns.
This is a reduction in allocation and retained memory, not a zero-cost claim.
The legacy materialization helpers remain available for explicit adapters.

## Transport and shard fan-in

Canonical traversal nodes serialize accessor values directly into the existing
metric map, including explicit null scores. The encoder borrows the result until
serialization finishes and keeps only one node's document/path/evidence adapters
live at a time. Qualified document lookup retains borrowed hit descriptors; it
projects document contents in scratch capped to 256 KiB between nodes. Delivery
prepares the complete response or spool before committing headers, so malformed
late nodes cannot cause a successful partial response. Legacy and other canonical
graph shapes retain their generated adapters. Uniform remote maps decode directly into one
column owner even when map key order differs; heterogeneous or omitted maps keep
the compatibility representation. Uniform legacy clones also use compact columns.

Same-allocator owned shard contributions transfer node payloads, metric owners,
paths, accepted matches, and hydrated hits. A budgeted checkpoint per contribution
keeps source ownership intact until the append succeeds; on failure, destination
ownership rolls back and the accumulator becomes unusable except for destruction.
Transferred bytes keep their admission charge rather than being charged twice.
Different allocators use independent clones. Aggregates and overlapping statuses
still require merge computation. These are copy and allocation reductions, not
zero CPU or allocation guarantees for the complete query.


## Distributed traversal

The distributed coordinator retains one metric dictionary and growable score
pages for its execution dependencies. Canonical edge expansion also uses shared
columns rather than allocating values and names per edge. Each returned node
borrows a stable row; a nonreturned frontier candidate rolls back its last score
row immediately. Page capacity and directories share the request's retained
memory budget, and admission denial remains `GraphWorkBudgetExceeded`.

An unchanged projection transfers this storage without another allocation.
Filtering and ordering can retain a compact row mapping. If retained backing
would exceed dense output by more than 256 KiB, finalization prepares a dense
replacement before changing any borrowing nodes. Hidden dependencies disappear
from the public dictionary. Destruction and error rollback retire borrowing
nodes before backing, and successful transfer retargets page allocation to the
stable response allocator.
