# Grouped artifact sources and prior context

A `group` enrichment builds private, bounded source units from rows in one
Antfly table. Producers consume those units using `source_artifact_name`.
Grouping keys may span the table's shards. Cross-table grouping is not part of
this contract.

```json
{
  "name": "turns",
  "kind": "group",
  "source": {
    "group_by": "thread_id",
    "order_by": "ordinal",
    "segment_by": "turn_id",
    "field": "search_text",
    "filter": {"term": {"speaker": ["user", "assistant"]}},
    "max_bytes": 32768,
    "max_members": 1024,
    "window_ordinals": 128
  }
}
```

Keys are scalar strings or integers, with type-preserving identity; integer `1`
and string `"1"` belong to different groups. Ordering requires integer ordinals.
Equal ordinals use document keys to order members, and segment identity to order
prior-context candidates. Missing, null, or filtered-out members do not enter
the covering index. Membership changes remove the old projection and add the
new projection in the same primary transaction, including moves between groups.

Fixed ordinal cells bound reflow independently of total conversation length.
Within a cell and segment, units split at byte/member limits. Logical unit
identity binds the group definition, group, segment, cell, and first member key.
The private document identity also includes its birth lease: updates preserve
that identity, while deletion and recreation establish a new lifetime. A delayed
producer callback therefore cannot publish into a recreated unit. Unit
fingerprints bind ordered member content. Appending to a later cell or segment
preserves earlier units and their producer receipts. A selected member exceeding
`max_bytes` rejects its source mutation explicitly, including leader admission
before a Raft proposal. Limits are 4 MiB and 4,096 members per unit;
`window_ordinals` is at most 4,096. A fan-in partition is independently bounded
to 4 MiB, and publication has at most 128 units.

Units carry `_group_key`, `_segment_key`, member keys, ordinal range, and
fingerprints. Private source rows are excluded from public row counts, ordinary lookup, and
ordinary search. Owner controls and producer snapshots use private paths. Copy, chunk, embedding, generator, extractor, and decision
consumers retain ordinary artifact ownership, skip state, provenance, repair,
and graph lifecycle semantics. Retirement wakes carry deleted private units
through ordinary enrichment replay. Their applicable producer DAG remains
planned after the private row disappears, allowing descendants to publish
accepted absence and retract graph contributions without another model call.

## Stateful producers

A prompt-consuming producer can include earlier output from its own group:

```json
{
  "prior_context": {
    "artifact": "claims",
    "max_items": 40,
    "max_bytes": 16384,
    "render": "{{id}}: [{{kind}}] {{statement}}",
    "horizon_segments": 8
  }
}
```

The named artifact must share the producer's group root. Root arrays and an
object's `items` array provide individual items; other root objects provide one
item. Antfly selects earlier, different segments only, newest first, bounded by
items, rendered bytes, and segment horizon. The rendered context fingerprint
participates in the producer's skip-state hash. References such as `replaces`
and `retracts` remain application fields and can be mapped through ordinary
graph sources; Antfly does not rewrite their identifiers.

An original member change seeds a durable finite invalidation interval. That
interval may require multiple bounded pages, and survives owner restart.
Completing a recomputed segment never extends the interval. Producers persist a
unit context epoch, including its publication revision, rather than recursively
inheriting every prior producer's
read set, which would turn a bounded correction into whole-thread replay.

## Graph identity and provenance

Graph mappings can aggregate units onto a logical group node using existing
`nodes.source`, while their durable owner remains the individual unit:

```json
{
  "source": {
    "artifact": "claims",
    "format": "extraction_relation",
    "nodes": {
      "source": "thread/{{_doc.value._group_key}}",
      "target": "{{_item.id}}"
    },
    "edge": {
      "type": "about",
      "edge_id": "{{_doc.key}}:{{_item.id}}",
      "metadata": {"unit": "{{_doc.key}}"}
    },
    "context": {"doc_fields": ["_group_key"]}
  }
}
```

Distinct unit owners preserve contributing evidence and allow deleting one
unit without deleting another unit's contribution to the same logical group.
Resolvers and immutable artifact replay use the ordinary source-unit identity.

## Durable execution

Source-owned reverse records and invalidation outboxes commit beside canonical
rows. Relational writes prepare bounded group projections while their logical
fields are owned, then carry those projections with the typed row through
commit. Covering membership and point work directories avoid a table scan on
append. Covering rows are checked against their authoritative reverse records,
and stale work-directory hints do not block unrelated deliveries. Catalog activation uses a resumable bounded baseline; catalog removal
or replacement uses a separate durable retirement sweep, fenced by the current
catalog and range. Maintenance reads select bounded key-and-hash pages on the
leader; binary range routes and physical cursors use base64 on private JSON
boundaries. Replicated apply point-reads those keys rather than walking local
derived records. Durable definition incarnations reject delayed pages after
removal and re-addition. Baseline readiness uses schema and logical range
identity, so restarting a process does not launch a new backfill. Rebuilding
covering membership preserves authoritative member versions. Repair work does
not seed a new causal replay interval, and unchanged unit context keeps its
existing publication revision. Unit membership pointers let the retirement
sweep reclaim orphaned private records while preserving current and retiring
units; catalog mutation gates protect partially installed transfers.

Original source sequence debt is indexed separately from
coalesced latest work, so future appends do not starve an earlier visibility
barrier. `enrichments` and `full_index` wait for affected grouped work.
Each original correction owns bounded dependency rows for the partitions it
invalidates. Completion retires those rows against accepted partition revisions,
so a later source outside the correction's horizon cannot extend its barrier.
Moves scan the last published and replacement positions independently; queue
ordinals remain scheduling lower bounds. If another edit supersedes a correction
mid-scan, its persisted cursor drains before enrollment of the newest event.
Events with unpublished replacements still coalesce immediately, avoiding a
self-dependency on their newer source delivery. This preserves published
neighborhoods with bounded metadata even under repeated moves. Context invalidation
records pending partition work immediately; unit guards become visible with
the replacement snapshot in group order, keeping later producers from blocking
the replay that materializes their predecessors. Publication compares each unit's
context guard even when a source edit coalesces with context invalidation, so
unchanged source payloads still replay affected producers.
Grouped producer readiness replaces the shared enrichment replay watermark for
group-rooted DAGs; producers rooted in primary documents retain their ordinary
fence. After grouped acceptance, `full_index` captures one publication cut and
waits for its managed indexes without chasing later appends.

A group coordinator lives at a hash-distributed logical anchor. Units and
coordinator records stay together across split boundaries. Cross-shard fan-in
uses current route fences, source-owner read indices, partition revisions,
baseline readiness, and a final topology check. Replicated grouped owners activate ordered artifact
publication and completion proofs as part of the group control protocol. A stale lease or catalog cannot
publish prepared units. No model call runs under the primary writer.

Group sequencing uses a separate receiver-local producer readiness receipt.
Each receiver verifies bounded pages of applicable producer and child-scope
requirements, then records a receipt fenced by its physical root, catalog,
source observation, and outstanding work revision. Resolver, promotion, graph,
and physical index obligations retain their ordinary independent convergence;
a group receipt cannot clear those full-table obligations. This permits later
segments to proceed when their prior producer artifacts are ready. New input
or dependency work invalidates the receipt. Retired units and full table
completion reclaim it; epoch/root garbage collection handles obsolete roots.

Certified transfer protocol 16 carries document-owned group records as typed
private afterimages. Receivers atomically reconstruct covering/work indexes and
advance the portable, table-wide source revision clock. That clock survives
individual group configuration retirement. Unit timestamps travel as typed
companions. A table-wide monotone lease clock and owner-incarnation checks fence stale claims
across removal, re-addition, restore, and owner replacement. Replicas use the
same logical range identity; Lite uses its persistent root incarnation. Global
indexes are reconstructible; they do
not become a second authority. Protocol 23 on data Raft and catalog reader floor
8 prevent older replicas from silently applying a partial group operation.

Private storage ABI 27 includes grouped source admission; older owner bindings
cannot silently bypass the new validation entrypoint. Kernel owner ABI 78 adds
fixed visibility targets: each bounded wait releases its owner lease before
retrying, preserving the original replay sequence, timeout, and physical root.
Catalog transitions can therefore proceed while derived work is pending.
Ordered transaction apply commits primary resolution and durable replay debt
without awaiting derived publication. The request coordinator waits for visibility
after apply, keeping the Raft progress loop available to publish that work.
Graph endpoint cleanup also runs in bounded background jobs so its Raft apply
wait cannot block the control driver. Required-format proposals join protocol
activation within their deadline rather than interpreting a concurrent probe
as an unsupported decoder.

Retired private units enter ordinary producer replay as absent sources, so
producer tombstone receipts and graph cleanup converge before group retirement
finishes. Already accepted outputs validate their saved causal proof before
loading new prior context; an unchanged segment outside a correction's horizon
can reuse its committed result while earlier work is pending.

Private owner queries provide a bounded `readiness` diagnostic for up to sixteen
unit IDs. It reports the current/receipt work revisions, verified requirement
prefix, next blocked producer, and any rejected completion command, without
returning model inputs or outputs.

Prior-context reads keep at most `max_items` candidates and stop reading older
artifacts once they cannot affect that window. Ties across directory cells
retain the same deterministic unit and item ordering as context selection.
The 4 MiB decoded-artifact limit applies to outputs needed to establish the
window; unrelated older outputs do not consume it or require acceptance proofs.
Prior artifacts retain exact decimal and exponent literals through context
rendering, including values outside floating-point precision and range. Template
substitutions preserve their original numeric text; conditionals recognize numeric
zero, and numeric equality uses the same exact decimal comparison as constraints.

Dynamic output serialization and template-context conversion use explicit work
lists so deeply nested unused fields cannot exhaust the native call stack.
