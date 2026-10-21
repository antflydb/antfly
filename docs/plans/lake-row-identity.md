# Lake row identity and index reuse

Status: accepted decision; typed stable-key relocation implemented for Parquet and Iceberg.
Additional identity adapters and finer-grained reuse are staged below.

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
  whole visible cuts; affected-only reuse remains a later refinement.

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

The builder streams delete-aware batches into bounded native spill sorts and
publishes immutable ID-to-location and location-to-ID trees. Verified unchanged
files reuse authenticated previous row state through the source delta; changed
files are decoded. Current value fingerprints cover every declared input column
and the desired index recipe. This intentionally declines reuse for changes to
unused columns, added/deleted rows, and generated embeddings with uncaptured
remote media inputs. A subsequent value change after relocation rebases physical
indexes before resuming ordinary incremental publication.

Equivalent cuts reuse the complete native index directory. The certificate binds
the original index closure, current source cut, namespace, recipe, and both row
maps. Subsequent rewrites flatten to that original index space. A shared
publication-pinned resolver translates forward hydration and reverse predicate
membership for text, ordered rows, dense/sparse retrieval, covering values and
highlights. Algebraic state reuses only the proven identical visible cut.
Translated canonical ranks preserve default text score ties; explicit ordering
and cursors retain current source semantics. Lookup failures propagate before
heap comparisons; comparison callbacks never perform I/O.

Reader leases, local GC traversal, cloud publication certificates, startup
navigation warming, and recovery include the map and original navigation closure.
Protocol 66 fences readers that cannot interpret relocated index bindings.
Existing physical public IDs remain unchanged; the logical ID codec is internal
and does not introduce a public stable-ID promise.

The resolver has a bounded page cache and two point lookups per translation;
it does not materialize all row mappings in query memory. Mapping metadata still
scales with row count, and broad equal-score searches can require substantial
navigation. Compact dense translation pages and batched translation are future
optimizations, to be measured against index rebuild savings.

## Staged extensions

- Connect native Iceberg v3 manifest/row lineage to the shared identity contract.
  The inheritance rules are modeled and tested, but the current reader does not
  claim automatic v3 lineage reuse.
- Accept independently authenticated, snapshot-bound writer mapping proofs.
- Track versions by index input recipe and reuse unchanged subsets on row/value
  changes, without forcing a whole physical rebase.
- Enable stable logical public IDs only with complete lookup, overlay, cursor,
  filtering and compatibility coverage.

These extensions must preserve the same proof and publication boundary; none
may infer identity from file layout or silently trust a user-declared key.

## Validation

`zig/e2e/antfly/test_lake_row_identity.py` runs independent PyArrow Parquet and
PyIceberg rewrites against fresh-build oracle tables. It checks repeated
relocation, a changed value, append with shifted file slots, restart, text scores,
indexed filters, sorting, forward/backward paging, covering/highlight fields,
dense/sparse membership and SQL aggregation. The Iceberg parameter participates
in `e2e-full`. Native tests cover spill bounds, duplicate keys, proof tampering,
flattening, reader-safe GC, certificate binding, cancellation, allocation failures,
and missing derived navigation. Sparse scores use the existing per-block 8-bit
quantization tolerance; physical rebuilds need not choose identical block scales.

Further qualification should exercise Parquet and Iceberg rewrites that preserve rows, arbitrarily
reorder rows, update indexed and unindexed values, remove rows and introduce null
or duplicate keys. Verify exact search scores, metadata filters, ordered/reverse
paging, vectors, aggregates, covering fields, highlights and public IDs against a
rebuild oracle. Check restart, old-reader leases, publication failure, mapping
composition, reader-safe GC and bounded memory/spill with allocation failures.

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
