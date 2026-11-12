# Bulk builder review and qualification

This review covers the Parquet bulk-builder changes and their integration with
main through `05fdfbdd71`. It includes the vector library, private LSM source and
projection, typed Parquet reader, native publication, construction catalog,
recovery, garbage collection, lifecycle callbacks and benchmark evidence.

The confirmed findings below have implementations and regression coverage.
Bulk-builder scale qualification and the post-main integration checks passed;
the recorded source/executable hashes
separate each observation from subsequent changes. See the
[qualification report](../../../zig/bench/vectors/cohere_bulk_recovery_qualification.json),
[builder design](bulk-builder.md) and [source-storage design](bulk-source-storage.md).

## Findings addressed

| Priority | Finding and consequence | Resolution and verification |
| --- | --- | --- |
| P1 | Reused cache-clock slots did not bind their replacement key. Maps could retain entries the eviction clock no longer tracked, exhausting the builder budget. | Shared explicit occupancy/reference state, replacement-key binding including ID zero, and allocation-safe node clones. Repeated eviction in all four concrete cache classes and construction with a resident source reservation pass under the test quota. |
| P1 | Older pending reader-protocol attempts could emit construction/paged artifacts outside their contract. | Reader protocol 103 requires pending attempts to renew while retaining reads of 97–102 serving publications. Tests cover protocol boundaries, stale/expired leases, combined opening-bundle/construction serialization and final-publication rejection of construction receipts. |
| P2 | Generic cache helpers transferred ownership before all map insertions succeeded; allocation failures could leave dangling ownership or leak clones. | Reserve before mutation, make transfer explicit and unwind partial wrappers/clones. Exhaustive allocation-failure tests cover the helper classes. These helpers have no current production callers, so this is not presented as a measured production speedup. |
| P2 | Source and projection merging opened one cursor per physical input, making working memory grow with run count. | At most 64 logical streams, contiguous precedence-preserving multipass groups, and lazy concatenation of split files. Intermediate tombstones survive; run-ID/I/O reservations cover every pass. Regressions cover 257 inputs, split outputs, duplicate precedence, tombstones, cancellation, cleanup and 193 physical files in one logical stream. |
| P2 | Bounding stream count did not bound each decoded SST index. Output reservation also ignored its configured entry cap. | Private SSTs cap entries at 65,536; byte limits still split wide rows earlier. Writers reserve at most their output entry limit. A seven-entry output with a million-entry estimate succeeds under 512 KiB. Normal serving entry policy is unchanged. |
| P2 | Superseded source files remained pinned by completed bulk plans, retired stores/directories and obsolete run owners. Large sources could retain another corpus on local disk after sealing. | The quiescent handoff checkpoints metadata, drains owned retirement work, then makes a cancellable pass over eligible obsolete files and persists cleanup. Reader/checkpoint pins and retry ownership remain intact. The strengthened regression creates 2,049 ingest SSTs under 16 MiB and requires exactly two live physical SSTs after seal/reopen. The final 10M handoff has 153 SSTs / 31.55 GB nominal lengths versus 2,075 / 63.12 GB before the ownership fix. |
| P2 | Declared missing checkpoint dependencies could look like an ordinary absent logical file, preventing the intended fresh rebuild. | Missing declared data chunks, reference pages and directory pages become artifact-unavailable errors. Derived work is discarded through the fenced owner and rebuilt once. Undeclared logical-file absence remains distinct. Missing completed-tree, healthy, all-missing-vector and lease-failure cases are covered. |
| P2 | Catalog validation accepted construction metadata versions the decoder could not read. | Only supported construction metadata version 1 is accepted before persistence. The catalog regression rejects unsupported versions. |
| P2 | Benchmark qualification could accept NaN recall, duplicate/missing widths, incomplete phases or partial source reuse. It also confused a soft cache target with a hard quota and discarded completed process counters on failure. | Validate finite/count-consistent recall, the exact ordered probe sweep, phase records and full reuse. Hash actual truth payloads and raw/framed source bodies/inventory. Save raw child measurements before parsing. Thirteen harness regressions and eight timeout/evidence-checker tests pass. Lost historical counters remain unavailable rather than reconstructed. |

Relevant implementations are
[cache/runtime](../../../zig/lib/vectorindex/src/hbc_runtime.zig),
[HBC storage/publication](../../../zig/pkg/antfly-embedded/src/storage/hbc_adapter.zig),
[LSM ownership](../../../zig/pkg/antfly-embedded/src/storage/lsm_backend.zig),
[bounded merge](../../../zig/pkg/antfly-embedded/src/storage/lsm_backend/compaction.zig),
[private source](../../../zig/pkg/antfly/src/api/lake_index_native_dense.zig),
[source stage](../../../zig/pkg/antfly/src/api/lake_bulk_stage.zig),
[catalog](../../../zig/pkg/antfly-embedded/src/metadata/lake_index_catalog.zig) and
[benchmark runner](../../../zig/bench/vectors/bench_bulk_pipeline.py).

## Correctness and completeness coverage

- Typed FLOAT decoding covers PLAIN, dictionary and byte-stream-split pages,
  V1/V2 continuation, null rows/elements and page-spanning lists. DOUBLE keeps
  SQL precision; embedding validation rejects invalid/nonfinite vectors. The
  merge with main preserves the column admission owner and hydration behavior.
- Eligible authored-vector identity and source ingestion share one typed scan.
  The external ID census uses a 2 MiB sort buffer, CRC-protected runs and at most
  32 merge inputs, including IDs zero and maximum u64. Unsupported eligibility
  retains the established path; it does not silently omit rows.
- Hierarchical construction covers tiny/identical partitions, progress,
  preassigned parents, weighted centroids, carried child radii and quantization
  from known child centroids. Inner-product covering bounds remain unknown.
  Private fresh snapshots bypass the live-update node-body overlay and rotate
  bounded transactions. Global identity evidence replaces repeated negative
  metadata probes.
- Ordered namespace publication shares native serializers and mutation codecs.
  Tests cover sparse/optional values, namespace joins, source reuse, native
  reopen and metadata. Paged directories exceed the former 8,192-file ceiling;
  legacy inline roots keep their own cap.
- Adaptive staging retries only allocation failure, after partial output and
  cursors unwind. It does not retry cancellation, I/O, preparation, installation
  or CURRENT authority handoff. Constructor cancellation from main is composed
  with explicit lifecycle checks. Borrowed callbacks remain synchronous and
  owned work is joined before destruction.
- Source/sampler and completed-tree receipts bind identity, recipe, configuration
  and RNG state. They cannot serve or remain in final publication. Ordinary and
  resumable GC traverse their dependencies. Later logical deltas remain eligible,
  including the documented physical-to-logical rebase.

## Performance decisions

The large wins retained are fused Parquet/source ingestion, immutable source SST
writes without the WAL/mutable-table path, the external identity census,
preassigned parents, carried child bounds/centroids, bounded private ownership and
ordered native publication. The historical matched parent-assignment experiment
had one completed 1,400.4-second control and three optimized runs with a
123.6-second median and identical recall/scored counts. It predates the final
runtime and is not three alternating pairs; missing control process counters
remain null.

The v67 core-builder repetitions took 142.1, 140.8 and 161.2 seconds for 1M
vectors. Its complete 10M operation took 1,646.1 seconds native and 1,937.1 seconds
whole probe, with unchanged recall, including 96.83% at 16,384 probes. That version
still retained excess local source files, which motivated the final ownership
drain. These observations do not qualify that later change.

On the same frozen 10M projection, the private entry cap reduced consolidation's
publication-window allocator peak from 208.8 MB to 47.6 MB. Capped runs took
74.1/56.7 seconds versus one uncapped 81.1-second control. Initial metadata
sampling has a separate peak; this is not a 47.6 MB whole-allocator or RSS claim.
All five full immutable segment hashes matched. Adaptive direct publication
remained faster at 29.2/22.1 seconds, so unconditional consolidation was rejected.
The final binary's adaptive output check took 28.2 seconds with the same segment
hash. Larger fresh snapshots or an admitted staging OOM select consolidation.

Optional compact ordinal regions remain off: earlier I/O reductions did not
establish a complete-operation elapsed-time improvement. Clustering strategy
changes remain explicit for offline builds; ordinary incremental construction
keeps its strategy.

The final v68 runtime was qualified with one complete import per corpus after
the metadata-owner drain, separately from the earlier repetitions above. The 1M
operation took 143.499 seconds native and
197.032 seconds whole probe. The 10M operation took
1671.739 seconds native and 1853.348 seconds whole
probe. All recall and scored counts matched the preceding qualified versions.
Both imports wrote two construction checkpoints, reopened successfully and
reused all source runs without new source bytes. The 10M dense/source allocator
peaks were 214.4/43.9 MB, separately, within the unchanged
256 MiB per-owner limits. The post-handoff 10M source contained only its 153
live SSTs / 31.55 GB nominal lengths, versus 2,075 / 63.12 GB before the ownership
fix. One read-only inventory observation is recorded in the raw conditions.
These focused runs establish final-runtime correctness and scale; they do not
establish a repeatable small elapsed-time gain over the earlier repetitions.

## Main integration after scale qualification

Main through `05fdfbdd71` (#1642) was merged after the fully scaled v68
qualification. Its changes affect exhaustive/full-effort search, embedding reuse,
tokenization and ranked graph starts. Bulk construction, source handoff, native
serialization and the measured ANN mode are unchanged. The appended-test conflict
was resolved by retaining both the bulk regressions and the new full-effort oracle.

Post-merge checks passed: 19 exhaustive-recall/embedding-reuse tests, 272 vector
library tests, 94 tokenizer tests with two skips, 35 bulk tests plus nine owner
checks, and 24 native tests with one guarded S3 skip. Production and CLI checks
passed. Two tokenizer harness issues were fixed without changing tokenization or
golden expectations: the test target now sets its fixture working directory, and
the BERT allocation-failure fixture disables OS-dependent in-place growth.

One complete post-merge 1M import qualified at 149.924 seconds native and
221.547 seconds whole probe, with all recall and scored counts unchanged.
The post-merge publication check took 31.214 seconds and matched the full immutable
segment hashes. This is integration verification, not a repeated performance
comparison. The 10M result remains explicitly bound to v68; no post-merge 10M
rerun is claimed. The qualification report records both source snapshots.

## Verification and limits

Final storage verification: 639 LSM tests passed, one skipped; 24 native
integration tests passed, one guarded S3 skip; production build and CLI help
passed. The final combined build passed 110/110 steps. Earlier broad verification
also passed 24 metadata tests, 35 bulk plus nine owner checks, both upstream dense
and sparse cancellation tests, 345 embedded reader tests with one skip, 121
focused Parquet integration tests, vector-library allocation-failure coverage,
and regenerated Go SDK tests after merging main.

These are shared-workstation filesystem-object-store measurements. No own
compilation or GC overlapped the final qualification's timed children; read-only
diagnostic overlap is recorded. Earlier runs retain their own overlap and
censoring notes. Native timings exclude final external metadata CAS and
reopened recall. Phase counters are nested. A 16 MiB cache retention target is
soft; private allocator admission and process RSS are separate measurements.
No consistent RSS improvement or real-S3 throughput/fault guarantee is claimed.

Recovery resumes at phase boundaries; unfinished subtrees restart. Exact vectors
are materialized once for construction/recovery/reranking. Source-backed Parquet
reranking and a separate final-format emitter are not implemented or claimed.
Input/output admission does not reserve disk, and format/run-metadata limits
still apply to larger corpora. Larger corpora remain subject to allocator, metadata and disk admission.
