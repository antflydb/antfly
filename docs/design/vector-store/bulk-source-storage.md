# Bulk source storage and recoverable construction

The [offline lake builder](bulk-builder.md) uses bounded immutable exact-vector
runs, streamed native posting publication, and reservoirs collected during
partition writes. This document specifies the remaining larger source-storage
changes and their compatibility requirements. Bounded compact-ordinal routing
and durable source/completed-clustering receipts are implemented. Source-backed
exact reranking and a direct final posting emitter remain separate alternatives
whose adoption requires measured end-to-end benefit and their stated contracts.

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

Fresh source backends collect 16 MiB sorted input runs under input admission,
then seal the complete bulk window into bounded-size sorted runs before
publishing its receipt. Sealing uses the bounded merge below; one lifecycle
boundary can require several merge passes. Ordinary L0 run-count compaction is disabled for the
private build lifetime after that seal. In particular, close/reopen between fused source
preparation and clustering must preserve the authenticated receipt's run IDs;
normal close-time write pressure otherwise rewrites and reuploads those bytes.
The ordinary maintenance policy resumes when a later incremental builder opens
the published source. A regression pins more than eight runs, closes and reopens
the private source, and requires identical run IDs.

Before exporting the sealed source, the private builder checkpoints the durable
manifest recovery closure through the existing streaming manifest machinery.
A current run set plus a removal journal is insufficient for prompt local disk
reuse: completed bulk plans and retired metadata can still own file pins for
the old ingest generation. Manifest replay applies removals before opening the
live SSTs; this retention is an ownership/reclamation issue, not a requirement
to read removed SSTs during recovery. The explicit quiescent handoff joins owned background work and
replaces that history with the sealed run inventory. It also drains completed
bulk-plan and retired run/directory metadata ownership before file reclamation;
normal bounded retirement slices can otherwise keep a large ingest generation
pinned until backend close. Obsolete run owners are released through one
bounded-visit sweep, preserving externally pinned snapshots. The handoff then makes one cancellable pass over the eligible obsolete-file
ledger and persists its result, instead of depending on the ordinary 2 ms
maintenance slice. Delete failures fail the handoff and retain retry ownership.
Private sources use zero obsolete-file grace; reader/version and exported-
checkpoint pins still prevent premature deletion. Ordinary serving retention remains unchanged. With no external pins, closing and reopening the private source must leave only
its live physical SSTs. A 65,537-row regression flushes 32-row batches to create
2,049 ingest SSTs, checking corpus-scale metadata fan-out alongside the entry
cap, physical inventory and data reads. A read-only inventory after the final
10M handoff found exactly 153 SSTs totaling 31,551,639,070 nominal bytes, matching
the live source receipt; the pre-fix inventory retained 2,075 SSTs totaling
63,115,470,532 bytes. These are file lengths, not allocated-block measurements.

## Bounded global vector-ID uniqueness

The source producer tees native vector IDs into a private external census while
writing the initial routing partition. It sorts at most 262,144 IDs (2 MiB) per
run, checks CRC-protected ID records, and merges at most 32 runs at a time with
32 KiB input buffers. Multi-level merging rejects collisions across arbitrary
runs, including IDs zero and the maximum u64. Temporary census files never join
the published source; cancellation and errors remove every owned file.

Only a completed census marks the partition's identities verified. Its evidence
is bound to the authenticated source receipt's immutable key census. Recovery
reconstructs the same IDs from those keys. Legacy/unverified routing partitions
receive one sequential validation pass before index writes. Root completeness
and per-terminal duplicate checks remain required.

This replaces probing all previously written metadata for every terminal
region. Those random negative lookups grew with both corpus and private-run
count and retained per-run lookup indexes; a 10M qualification exceeded the
256 MiB builder budget despite successful source preparation. Clustering now
writes the already verified metadata directly. A shared private LSM cache has a
16 MiB retention target for remaining reads. Pinned handles can exceed this soft
target; their allocations still count against the hard builder quota. The
node-cache allowance
accounts for four-byte float coordinates. The runtime allocation limit stays
256 MiB; it is not raised to accommodate the corpus.

## Bounded sorted-run merging and publication

Sealing opens at most 64 input streams. Contiguous newest-first groups are merged
through the existing native SST writer. A group is one logical sorted stream,
even when the file-size limit splits it into many physical files. The next pass
opens only one file at a time from each group, validates disjoint bounds, and
concatenates its files. Group count decreases each pass independently of the
physical output-file count. Duplicate resolution preserves original precedence;
tombstones survive intermediate passes and may be dropped only in the final
merge with complete coverage.

Private exact-source and construction-projection SSTs contain at most 65,536
entries; the existing byte limit can split wide rows earlier. This bounds decoded
per-file indexes as well as active stream count. Output writers cap their initial
reservation by this entry limit, even when a merge estimates a much larger
window. Split files remain one lazily concatenated logical stream. Ordinary
serving and incremental stores keep their existing entry policy.

Original files remain pinned until atomic manifest publication. Once a pass
finishes, its unpublished intermediate files are unlinked before another pass
begins; output ownership and admission are released through the existing cleanup
path. Thus completed intermediate passes do not accumulate with merge depth.
Physical run IDs reserve a checked worst-case range covering every pass, and
maintenance I/O admission includes intermediate reads and writes. Private source
and projection builders retain filters in SST files without holding a duplicate
in-memory Bloom filter for every run. Serving and incremental defaults retain
their existing filter policy.

Native bootstrap publication admits at most 1,024 unconsolidated construction
runs to the ordered path. This is a stream-count ceiling, not a claim that those
streams fit every memory allowance: the caller's allocator still admits every
allocation. Larger fresh all-L0 snapshots are consolidated into a disjoint level
using the bounded merge. Namespace cursors then concatenate level files, leaving
one live stream per namespace. An admitted snapshot avoids this extra rewrite.

If ordered staging fails with `OutOfMemory`, its cursors and atomic temporary
output are released before one consolidation-and-restaging attempt. Cancellation
and I/O failures are propagated without retry. Checkpoint preparation and the
authority handoff occur outside that retry boundary, so a failure after `CURRENT`
may change can never repeat staging. Existing mixed leveled stores retain their
ordinary maintenance policy; a failed consolidation admission remains a normal
build failure. The shared native codecs, checksum validation and final authority
fence remain unchanged. Builder cancellation and lease checks use the transport-neutral
cancellation token throughout merge passes, with all backend work joined before
the borrowed lifecycle owner is destroyed.

These guarantees bound live stream buffers and duplicate filter residency. Run
metadata, manifests, native format limits and the caller's allocator admission
still apply; a higher input allowance does not reserve local disk or promise an
arbitrarily large corpus within a fixed memory quota.

## Construct nodes with their final parent

Fresh recursive construction reserves an internal node ID before its children.
Both external spool regions and in-memory terminal hierarchies pass that ID to
leaf construction. The final parent is encoded in each packed header immediately.
The parent record itself is emitted after its children supply weighted centroids,
member counts, ranges, and covering-radius inputs.

The old bottom-up assignment temporarily treated every leaf as a root. Root leaf
payloads contain exact vectors; attaching those leaves later changed their payload
type and triggered requantization and vector reloads. Preassigned parents remove
that intermediate representation while reusing the same leaf codecs, native
serializer, mutation store, and crash-safe final handoff. The private projection
can contain unresolved forward parent references while construction is underway;
it is never a serving or completed-clustering receipt until the whole tree is
finished and its metadata/count are committed.

A qualification control retains late parent assignment. Tests require zero parent
rewrites and zero source-vector loads during preassigned construction, then verify
links, reopened search, replacement, deletion, consolidation, and another reopen
across metrics and quantization modes.

## Compact ordinal partitions

The optional representation is adaptive within each depth-first subtree.
Production defaults retain full-vector partitions; canonical routing requires an
explicit `compact_partition_bytes` allowance. Lower write volume alone does not
establish faster complete construction on the qualification host.
Large partitions contain checksummed full-vector records. A nonterminal region
of at most 32 MiB can become an admitted canonical byte plane; descendants carry
checksummed 64-bit offsets into that plane. IDs, metadata, and coordinates remain
in the authenticated canonical record. Readers validate both checksums before
using a gathered row. Root/child reservoirs retain their existing independent
checksums and bounded vector representation.

This avoids a corpus-wide indirect gather: the complete referenced region fits
inside the builder's allocator allowance, independently of filesystem cache size.
Routing and fallback sampling read an admitted canonical region from its retained
bytes, avoiding a second file scan. Logical read/write counters distinguish this
work from physical device I/O and filesystem-cache effects.
Terminal matrices, canonical bytes, reservoirs, and native caches share the
256 MiB allocator. The region is released before visiting the next independent
subtree. `compact_partition_bytes=0` is the full-vector comparison path.
Qualification records scratch bytes, compact rows, allocator peak, elapsed time,
and reopened recall. A corpus-wide ordinal plane is not an equivalent extension;
it must separately demonstrate bounded read amplification when the source exceeds
available memory.

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

Initial publication consumes that private projection through ordered node scans
and a merge join with quantization keys. Each pass owns one node's copied values;
its cursors and transaction close before the next pass. Root quantization needs
one exceptional point lookup. This avoids probing absent range/posting keys and
redecoding run indexes for every node across overlapping construction runs. It
does not introduce another final format: existing native serializers, atomic
staging, admission and authority installation remain shared. The standalone
publication qualification selects point or ordered access with the same codecs,
reopens native authority and rejects any exact-source read. See the measured
10M comparison in [bulk-builder.md](bulk-builder.md#verification-and-measurement).

## Durable phase manifests

Implemented private source receipts bind source and recipe identity, dimensions,
algorithm, resource allowance, RNG state, immutable exact runs, root partition metadata and
reservoir, and paged file identity references. The full transformed root partition
is rebuilt from authenticated exact vectors and key chunks on restart, avoiding
an additional full-corpus upload. Completed-clustering receipts
replace the routing partition with a complete private native projection checkpoint.
The identity/input proof is retained alongside the receipt. All referenced objects
are uploaded before the fenced catalog checkpoint acknowledges the phase.

```mermaid
flowchart LR
    P[Typed Parquet and identity verification] --> S[Verified exact-source and sampler receipt]
    S --> R[Retained or reconstructed routing input]
    R --> C[Bounded recursive clustering]
    C --> T[Private completed-tree receipt]
    T --> N[Shared native publication]
    N --> F[Fenced catalog CAS]
    F --> Q[Serving generation]
    S -. Restart reconstructs routing input .-> R
    T -. Restart skips clustering .-> N
```

Recovery validates the same source/configuration bindings and opens a new private
overlay. It skips verified source preparation when a source receipt is available,
and skips clustering when the completed projection is available. Readers require
the final complete native generation and catalog publication. Construction work
has a distinct catalog field; it cannot satisfy a serving declaration and must be
absent from a completed publication. Old protocol 97–102 publications remain readable;
new receipts require protocol 103. Ordinary and resumable GC traverse the receipt's
complete dependency closure.

An unfinished older-protocol attempt is fenced from producing new artifacts; lease
renewal establishes a protocol 103 attempt. Missing immutable data, chunk-reference
pages or generation-directory pages invalidate the construction checkpoint and
trigger a fresh build. A missing logical file remains distinct from a missing
artifact that the authenticated checkpoint declared.

Work within an unfinished phase is repeated after failure. Persisting an internal
partition frontier and partially completed subtrees would shorten that interval,
but requires an additional projection/frontier consistency boundary; current
receipts never claim to resume an unfinished subtree. Checkpoint scheduling and
storage comparisons must include private upload bytes and avoided retry work.

## Required verification

Exercise optional and required Parquet lists, dictionary and plain encoding,
compression, page boundaries inside vectors, missing optional columns, and
invalid coordinates. Verify original-coordinate exact scores across all metrics.

Measure the complete operation at 1M and 10M vectors. Apply updates, replacements,
and deletes to the resulting generation, force consolidation, reopen, and verify
identities and recall. Inject crashes before and after phase manifests, native
receipt installation, artifact upload, and catalog publication. Repeat with
source replacement and worker ownership changes.

## Initial qualification decisions (before the external ID census)

The [paired complete-operation observations](../../../zig/bench/vectors/cohere_bulk_recovery_qualification.json)
qualify fused preparation and preassigned parents on the authored 1M corpus.
Three trials per variant reduce median native operation time from 204.9 to
130.5 seconds and whole-probe CPU from 233.5 to 176.4 seconds, with identical
recall at every measured width. Private projection bytes fall from 590.3 to
285.4 MB. Exact-source bytes are fully reused at final publication. The builder
allocators remain within their 256 MiB limits; process RSS is separately recorded
and does not show a consistent improvement across the shared-host trials.

Compact regions reduce logical partition writes by 19.4% and reads by 15.7%,
but the isolated observation does not establish a complete-operation time gain
and uses more private memory. They remain opt-in. The private projection's
remaining commit cost is a few seconds per 1M operation; a new final emitter
needs stronger evidence to justify duplicating the shared publication path.
Source-backed exact reranking still requires the retention, mutation-overlay and
cold-read contract above, rather than silently transferring import work to serving.

The initial 10M attempt exposed the retained-index limit described above and is
recorded as unqualified. Final corpus-scale qualification must use the external
ID census and bounded private caches.
