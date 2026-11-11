# Offline native lake vector builds

Fresh native Parquet lake indexes can select sampled hierarchical Lloyd or
e-means construction. The build operates on the complete source snapshot.
Configurations with offline clustering run as whole-source consumers in the
lake producer pipeline. Subsequent published-generation updates use the native
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

The input allowance counts vector bytes and 128 identity bytes per member. It
defaults to 512 MiB. Generation output has twice that allowance. Set the input
allowance explicitly for a larger corpus. The shared native builder allocator
admits at most 256 MiB of live allocations across the index and exact-vector
store. Native mappings, allocator overhead, and resident file pages contribute
separately to process RSS.

## Construction

1. Scan the immutable source with its existing cancellation, identity, enrichment,
   and file-state tracking. Commit original vectors to the exact-vector LSM and
   append transformed vectors and identities to a private checksummed sequential spool.
2. Train each external partition on a uniform reservoir of up to 32 rows per
   router center, with an 8 MiB vector-sample allowance. Each router has at most
   16 centers.
3. Route every partition row once to its nearest center. A partition that assigns
   over three quarters of its rows to one child is routed again with fixed
   capacities. This bounds recursive progress for identical vectors and skewed
   inputs.
4. Finish small partitions in memory under an 8 MiB admission estimate covering
   vectors, identities, and per-row construction scratch. Their learned hierarchy
   uses confidence-ordered capacity assignment. Final leaves respect the index's
   leaf capacity, which defaults to 100.
5. Encode leaf payloads directly from their transformed matrices. Build weighted
   parent centroids, covering radii, split ranges, and reverse mappings through
   the existing HBC write methods. Rotate storage transactions around an 8 MiB
   staged-byte budget between complete writes. Flush the private projection
   before native handoff.
6. Stream the native generation into an atomic staging sink. Topology,
   centroid blocks, quantized scan blocks, and ordered vector-directory blocks
   share the established native codecs. Bootstrap point-read transactions end
   after each node. The centroid writer retains one bounded block.
7. Validate and install the staged receipt, publish durable `CURRENT`, and mark
   native authority. Publish the posting checkpoint and exact-vector checkpoint
   together through the existing lake artifact and generation-directory path.

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
