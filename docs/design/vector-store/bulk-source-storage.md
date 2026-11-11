# Bulk source storage and recoverable construction

The [offline lake builder](bulk-builder.md) uses bounded immutable exact-vector
runs, streamed native posting publication, and reservoirs collected during
partition writes. This document specifies the remaining larger source-storage
changes and their compatibility requirements. Source-backed exact reranking,
compact-ordinal partition files, and durable construction-phase manifests remain
implementation work.

## Source contracts

A materialized source stores original float32 vectors in immutable native runs.
Its durable manifest and mutation overlay provide exact-vector lookup after
publication. This supports authored embeddings and enrichment-generated vectors.

An external source-backed generation must pin the Parquet object version or
content identity, schema, vector column, row group, and physical row ordinal.
Its retained inventory and deletion qualification belong to the generation.
Source lookup must remain valid after a newer table snapshot becomes current.
Each row locator must resolve against the generation's retained source inventory.

Source-backed exact reranking applies to immutable authored vector columns.
Generated and chunked embeddings require materialized coordinates. Original
coordinates and the configured metric remain the exact-scoring contract.
Transformed coordinates belong to construction and quantized posting encoding.

The source manifest must retain access to every referenced object for the full
reader lease. A missing version, checksum mismatch, dimension mismatch, or
invalid deletion binding fails the generation read. A source-backed generation
needs an explicit retention capability before construction may select it.

## Batched reranking

Resolve candidate ordinals to file, row group, column, and row. Group requests by
physical location before reading. Decode each relevant page once under a bounded
cache. Restore candidate ordering after gathering exact coordinates. Shared
range reads retain authorization, cancellation, object version, and checksum
validation. Query admission includes compressed input, decoded pages, and output
coordinates.

Record exact rows requested, physical bytes fetched, decoded bytes, page reuse,
and source lookup latency. A vector-only local materialization can serve the
same generation when remote page amplification exceeds the configured cache and
latency allowance. Publication records the selected backing representation.

## Compact ordinal partitions

A canonical vector plane assigns one dense ordinal per accepted vector. A compact
partition contains checksummed ordinal records. Original row identity and
vector ID live in a separately authenticated directory. Duplicate identities and
vector-ID collisions must be rejected before publication.

Partition routing gathers coordinates in bounded groups, ordered by canonical
location. Reads coalesce adjacent extents and bound amplification. Full-vector
sequential spill remains useful for partitions whose canonical reads have poor
locality. The partition descriptor records its physical representation, member
count, source-plane identity, checksum, reservoir, and ordering guarantees.

Construction chooses a representation using estimated physical read and write
bytes and admitted source residency. That decision must account for repeated
passes over siblings and source-cache eviction. Validate the decision against
large corpora exceeding RAM as well as corpora fitting the source cache.

## Native posting assembly

A direct native emitter needs final parent identities, covering radii, split
ranges, leaf revisions, and vector-to-leaf mappings. Reserve topology identities
before emitting dependent leaves. Compute weighted parent statistics from
completed children. Sort compact reverse-map records externally, rejecting
identity collisions while merging. Stream the existing native containers into
atomic staging sinks and install their authenticated receipt together.

The existing private projection remains the construction implementation until
these responsibilities have a direct emitter with complete mutation and reopen
coverage. The authoritative serving format and publication fence remain shared.

## Durable phase manifests

Construction progress belongs to the fenced build attempt and grants recovery
access to immutable work. Serving authority requires the final complete native
generation and catalog publication.

A phase manifest binds the source snapshot and object versions, index recipe,
metric, transform, dimensions, algorithm version, random state, resource
allowances, completed input position, immutable source runs, partition frontier,
completed subtrees, and artifact receipts. Each referenced object is immutable
and independently authenticated. Save a manifest only after its referenced
objects are durable.

Recovery validates source and recipe bindings, reuses immutable completed work,
and starts a new private staging sink. A superseded worker retains its own
staging files. The active attempt publishes through the existing lease and
catalog compare-and-swap. Unreferenced intermediate objects enter the existing
retirement path after reader and recovery leases expire.

Checkpoints should reuse uploaded immutable source runs and completed posting
containers. The checkpoint schedule should balance bounded repeated work against
artifact upload cost. Phase receipts require a distinct private representation
that cannot satisfy a completed index declaration.

## Required verification

Exercise optional and required Parquet lists, dictionary and plain encoding,
compression, page boundaries inside vectors, missing optional columns, and
invalid coordinates. Verify original-coordinate exact scores across all metrics.

Measure the complete operation at 1M and 10M vectors. Apply updates, replacements,
and deletes to the resulting generation, force consolidation, reopen, and verify
identities and recall. Inject crashes before and after phase manifests, native
receipt installation, artifact upload, and catalog publication. Repeat with
source replacement and worker ownership changes.
