# Offline native lake vector builds

Fresh native Parquet lake indexes can select sampled hierarchical Lloyd or
e-means construction. The build operates on the complete source snapshot.
Configurations with offline clustering run as whole-source consumers in the
lake producer pipeline. They use the physical source provider and skip optional
logical fragment payload capture and assembly, avoiding another vector-corpus
copy ahead of the bulk scan. Stable row membership and input evidence remain
verified. Subsequent published-generation updates use the native
incremental mutation path.

## Configuration

Set the options on a dense HBC embeddings index.

```json
{
  "dense": {
    "type": "embeddings",
    "external": true,
    "dimension": 768,
    "distance_metric": "l2_squared",
    "lake_bulk_build": "emeans",
    "lake_bulk_build_max_input_bytes": 4294967296
  }
}
```

Use `lloyd` for sampled Lloyd training. E-means uses the CPU squared-L2
implementation. Both algorithms use the same reservoir selection and routing
machinery. The existing default build strategy applies when the option is
omitted.

Quantization settings remain part of the generation recipe. Build, archived
mutation, and serving reopen all reconstruct `quantization_bits` and
`quantization_refine`. Multi-bit quantization enables the required deterministic
rotation; exact reranking continues to use original coordinates. Version 4
lake writers ignored these recipe fields, so those generations reopen with
their actual one-bit, unrotated defaults. A rebuild requesting nondefault
quantization cannot seed from such a generation.

The input allowance counts vector bytes and 128 identity bytes per member. It
defaults to 512 MiB. Generation output has twice that allowance. Identity tracking uses the same
input allowance. Configured allowances also apply to subsequent updates. Set
the input allowance explicitly for a larger corpus. The shared native builder allocator
admits at most 256 MiB of live allocations across the index and exact-vector
store. Native mappings, allocator overhead, and resident file pages contribute
separately to process RSS.

## Construction

1. Decode authored Parquet `LIST<FLOAT>` and `LIST<DOUBLE>` columns into borrowed
   float32 vectors. Repetition and definition levels preserve rows across page
   boundaries and distinguish null lists from empty lists. The reader retains
   one physical page and a bounded output cohort. Zstandard pages use the shared
   native decoder's bounded Huffman prefix table: at most 2,048 two-byte entries
   replace per-literal linear symbol searches. Short stream tails retain exact
   bit-width traversal and four-stream transitions. Decoder windows use the
   admitted heap. Shared producer replay keeps
   vector values in packed float32 form. Identity evidence hashes canonical
   coordinate bits in bounded blocks, preserving its existing digest without
   issuing a hash update for every coordinate. SQL and stored-source consumers
   materialize JSON arrays when their requested result requires them.
2. Scan the immutable source with its existing cancellation, identity, enrichment,
   and file-state tracking. Sort bounded 16 MiB batches of original vectors and
   publish immutable runs directly through the existing native table-file codec.
   The shared checkpoint and exact-vector readers consume these runs. Append
   transformed vectors and identities to a private checksummed sequential spool.
3. Collect the root reservoir during source ingestion and child reservoirs while
   writing partition files. Persist each reservoir with row IDs and checksums.
   Train external routers from these small files, removing the extra corpus scan
   previously needed for sampling. Each router has at most 16 centers. Each
   reservoir holds at most 512 rows and 8 MiB of vector coordinates. Routing
   owns at most 16 reservoirs, releasing them before recursive descent.
4. Route every partition row once to its nearest center. A partition that assigns
   over three quarters of its rows to one child is routed again with fixed
   capacities. This bounds recursive progress for identical vectors and skewed
   inputs. The progress threshold always requires at least one row to leave the
   largest child, including two-row and three-row forced-spill partitions.
5. Finish small partitions in memory under an 8 MiB admission estimate covering
   vectors, identities, and per-row construction scratch. Their learned hierarchy
   uses confidence-ordered capacity assignment. Final leaves respect the index's
   leaf capacity, which defaults to 100.
6. Encode leaf payloads directly from their transformed matrices. Build weighted
   parent centroids, covering radii, split ranges, and reverse mappings through
   the existing HBC write methods. Rotate storage transactions around an 8 MiB
   staged-byte budget between complete writes. Fresh private projection batches
   use immutable bulk ingestion. Flush the private projection before native
   handoff. Construction failures discard the entire private candidate.
7. Stream the native generation into an atomic staging sink. Topology,
   centroid blocks, quantized scan blocks, and ordered vector-directory blocks
   share the established native codecs. Bootstrap point-read transactions end
   after each node. Initial publication, synchronous checkpoints, and background
   full consolidation share bounded streaming publication. Centroid writers
   retain one block, including when flattening an existing large generation.
8. Validate and install the staged receipt, publish durable `CURRENT`, and mark
   native authority. Publish the posting checkpoint and exact-vector checkpoint
   together through the existing lake artifact and generation-directory path.
   Paged chunk directories admit more than 65,536 chunks. Inline chunk arrays
   retain their separate allocation bound. Generation admission consequently
   scales beyond the former 16 GiB aggregate limit. Large source files page their
   identity-chunk references through the same authenticated ordered tree. The
   tracker buffers at most 32 references per active file. Native vector and
   sparse metadata version 5 adds this directory, while readers retain version
   4 compatibility. Deletion and garbage collection traverse the paged references.

External partition scratch is proportional to one corpus plus the partition
currently being split. Terminal construction memory and the depth-first parent
frontier remain bounded independently of the full vector matrix. The index's
ordered metadata namespace supplies cross-partition identity checks. Duplicate
IDs within a terminal partition are checked before staging its metadata.

The private candidate owns its scratch directory. Cancellation, allocation
failure, malformed scratch input, and duplicate IDs discard the candidate.
Recovery reuses completed lake checkpoints. An interrupted fresh whole-corpus
candidate is rebuilt from the admitted immutable source.

## Verification and measurement

Run the focused construction and publication regressions.

```sh
cd zig
zig build bulk-spool-test
zig build lib-vectorindex-test
zig build lake-execution-abi-integration-test
```

The quality benchmark accepts a disk-backed normalized float32 corpus, float32
queries, and one-based uint64 top-100 ground truth.

```sh
zig build bench-build-quality -Doptimize=ReleaseFast
zig-out/bin/bench_build_quality train.f32 queries.f32 truth.u64   768 spool_emeans full ./bulk-index 100 0 42
```

Select `spool_lloyd` for the paired sampled Lloyd build and `hilbert_seeded`
for the current Hilbert full-build baseline. The final argument selects the
seed. Pass `reopen` in place of `full` to repeat the extended recall sweep
against an existing generation with the disk-backed vector loader. Spooled runs share a 256 MiB allocator budget, stream their source matrix,
publish native authority, reopen it, verify tree links and member count, and
run the held-out recall sweep. Logs include construction, training, routing,
publication, and peak admitted allocation measurements.

These local corpus benchmarks cover native posting construction and reopen.
Parquet normalization, exact-vector LSM construction, remote reads, and artifact
upload are separate stages. The lake integration regression exercises the
Parquet-to-published-artifact path across multiple files and reopens its native
vector index.

## Storage and I/O decisions

Immutable exact-vector runs reuse the existing native source format, checksums,
publication manifests, batched reads, and mutation overlays. This removes the
source WAL and mutable-table phase for fresh offline builds. The authoritative
posting generation uses the existing topology, centroid, scan, and reverse-map
containers. Its final publication installs one complete durable generation.

Collecting reservoirs during partition writes changes external partition work
from a sampling read, routing read, and partition write to a routing read and
partition write, plus small reservoir files. The sampling-only corpus pass is
eliminated. Uniform replacement uses an independent deterministic random stream
per output. Balanced rerouting rebuilds reservoirs for the final membership.
Legacy callers supplying a spool without a reservoir retain the sampling scan.

The larger source-backed and ordinal-partition designs are specified in
[bulk source storage](bulk-source-storage.md). Their source-version, deletion,
read-locality, and recovery requirements apply together.

## Complete Parquet operation benchmark

The existing native execution runtime probe accepts a raw Cohere-style Parquet
file with `id` and `emb` columns. It streams that file into the filesystem
object-store fixture before starting the operation timer. It measures source identity
construction, typed decoding, exact-vector runs, clustering, artifact upload,
and publication through the native operation. It then reopens the published
artifacts and executes a vector query.

```sh
cd zig
ANTFLY_LAKE_BULK_PARQUET_FILE=/absolute/path/to/train.parquet \
ANTFLY_LAKE_BULK_DIMS=768 \
ANTFLY_LAKE_BULK_ALGORITHM=lloyd \
ANTFLY_LAKE_BULK_INPUT_BYTES=4294967296 \
zig build lake-execution-runtime-probe -Doptimize=ReleaseFast
```

To use an existing object-store source, set `ANTFLY_LAKE_BULK_PARQUET` to its
URI instead. A filesystem URI identifies a native framed object store, whose
objects are installed through `Client.putFile`, rather than a plain directory.

The timer covers the native build call, including its artifact publication and
returned catalog candidate. Initial fixture preparation and source opening,
the caller's final catalog transaction, and reopen/search are outside the timer.
The report includes whether optional logical identity evidence was published.

The operation probe preserves the authored vector coordinates and uses squared
L2. Its query checks serving and exact-vector reopening. Recall measurement
continues to use held-out queries and matching ground truth in the quality
benchmark.


## Compressed-source kernel benchmark

The native Zstandard prefix decoder shares the existing frame, dictionary,
checksum, window, and bitstream implementation. The table occupies 4 KiB per
Huffman tree and is rebuilt when entropy tables change. Valid-frame output is
checked against the standard decoder, with multiblock, truncated-frame, and
exhaustive eleven-bit-prefix regressions. Existing raw/trained dictionary
reference fixtures also exercise the shared codec.

```sh
cd zig
zig build bench-zstd -Doptimize=ReleaseFast
zig-out/bin/bench_zstd lib/compress/src/testdata/cohere_literals.zst 2000 24576 5
```

Each trial decodes 49,152,000 bytes through each implementation using equal
window allocations and checks every output. Trial order alternates. Kernel
measurements isolate compressed-source decoding; complete Parquet operation
measurements remain separate.
