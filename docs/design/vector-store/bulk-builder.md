# Offline native lake vector builds

Fresh native Parquet lake indexes can select sampled hierarchical Lloyd or
e-means construction. The build operates on the complete source snapshot.
Configurations with offline clustering run as whole-source consumers in the
lake producer pipeline. A fresh, single authored dense index with stable row
identity tees the verifier's typed batches directly into immutable exact-vector
runs and the transformed root partition. Identity verification must finish before
these private outputs become recoverable work. No intermediate replay corpus or
second Parquet decode is required on this path. Generated embeddings, multiple
consumers, and unavailable identity evidence retain ordinary source consumption.

Fresh offline construction skips optional logical fragment payload assembly.
That decision is phase-specific: keeping bulk mode configured does not disable
fragment assembly and logical deltas for subsequent published-generation updates.
Existing physical generations can require a one-time rebase into the logical
fragment namespace when adopting that representation.

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

1. Decode authored Parquet `LIST<FLOAT>` columns into borrowed float32 vectors.
   `LIST<DOUBLE>` preserves its original precision in bounded numeric JSON arrays;
   embedding consumers perform their float32 conversion at the vector boundary.
   SQL and stored-source reads preserve doubles, including finite values outside
   the float32 range. Repetition and definition levels preserve rows across page
   boundaries and distinguish null lists from empty lists. Numeric SQL arrays
   may exceed embedding dimensions and may contain null elements. Packed float
   columns use a reader-produced NaN sentinel for null coordinates; actual
   nonfinite authored coordinates are rejected, SQL materializes the sentinel
   as null, and embedding consumers reject it. DOUBLE SQL arrays serialize null
   explicitly, without converting it into a nonfinite-number string. FLOAT pages
   and dictionaries decode directly to float32, avoiding a float64 intermediate.
   The reader retains
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
5. With an explicit compact-partition allowance, when a nonterminal partition
   fits an admitted canonical region (32 MiB in qualification),
   retain its original checked records in bounded memory. Descendants then store
   12-byte checksummed offsets instead of copying vectors and keys. Each gather
   verifies both the ordinal record and the original record. Large regions stay
   sequential full-vector partitions, so this optimization cannot turn the full
   corpus into random source reads. The region is released after its depth-first
   subtree completes. Routing and fallback sampling scan the retained canonical
   bytes directly; they do not reread that region from scratch storage. A
   zero-byte allowance retains full-vector partitions and is the production
   default. Paired qualification selects each representation explicitly.
6. Finish small partitions in memory under an 8 MiB admission estimate covering
   vectors, identities, and per-row construction scratch. Their learned hierarchy
   uses confidence-ordered capacity assignment. Final leaves respect the index's
   leaf capacity, which defaults to 100.
7. Reserve each internal node ID before descending into its children. Encode
   each leaf once from its transformed matrix with its final parent already set.
   Only the real root leaf uses the nonquantized root representation. This avoids
   temporary root payloads, later packed-node rewrites, and exact-vector reloads
   to requantize the leaves. Build weighted
   parent centroids, covering radii, split ranges, and reverse mappings through
   the existing HBC write methods. Rotate storage transactions around an 8 MiB
   staged-byte budget between complete writes. Fresh private projection batches
   use immutable bulk ingestion. Flush the private projection before native
   handoff. Construction failures discard the entire private candidate.
8. Use ordered publication directly for an admitted private projection with at
   most 1,024 construction runs. Larger snapshots, or one staging attempt that
   exhausts allocator admission, consolidate into a disjoint LSM level through
   a bounded merge with at most 64 active streams. Multi-file intermediate
   streams open their SSTs sequentially; consumed passes are removed before the
   next pass. Ordered namespace cursors then concatenate the level's files.
   The allocation-failure retry releases partial staging before consolidation;
   preparation and `CURRENT` installation are outside that retry boundary.
   Stream the native generation into an atomic staging sink. Topology,
   centroid blocks, quantized scan blocks, and ordered vector-directory blocks
   share the established native codecs. Bootstrap publication groups ordered
   node keys and merge-joins ordered quantization keys. It retains only one
   node's values and closes each pass's cursors before beginning the next pass.
   This avoids repeated absent-key probes and run-index decoding across the
   overlapping immutable construction runs. The root quantization exception
   needs one point lookup. Qualification can select the former point-read
   access strategy while sharing every serializer, so performance comparisons
   do not compare different native formats. Initial publication, synchronous checkpoints, and background
   full consolidation share bounded streaming publication. Centroid writers
   retain one block, including when flattening an existing large generation.
9. Validate and install the staged receipt, publish durable `CURRENT`, and mark
   native authority. Publish the posting checkpoint and exact-vector checkpoint
   together through the existing lake artifact and generation-directory path.
   Paged chunk directories admit more than 65,536 chunks. Inline chunk arrays
   retain their separate allocation bound. Generation admission consequently
   scales beyond the former 16 GiB aggregate limit. Large source files page their
   identity-chunk references through the same authenticated ordered tree. The
   tracker buffers at most 32 references per active file. Exact-source file
   descriptors are published in bounded batches directly into a paged tree;
   the paged generation has no 8,192-file ceiling. Legacy inline roots retain
   their separate allocation limit. Native vector and
   sparse metadata version 5 adds this directory, while readers retain version
   4 compatibility. Deletion and garbage collection traverse the paged references.

External partition scratch is proportional to one corpus plus the partition
currently being split. The worker also retains the materialized exact-vector
runs until native handoff. The source handoff also checkpoints its local
recovery manifest so superseded ingest SSTs do not retain another corpus copy
during clustering; reader and checkpoint pins remain authoritative. Peak local
storage therefore includes those exact
runs, the live routing partitions and the current split's output, plus the
private projection and native generation being emitted. Uploaded source
checkpoints share their immutable exact-vector objects with final publication;
publication does not upload another corpus copy. Filesystem object-store
qualification places those checkpoint objects on the same disk as worker
scratch, whereas remote object storage holds them separately. Input/output
allowances govern admitted work and artifacts; they do not reserve local disk
space. Terminal construction memory and the depth-first parent
frontier remain bounded independently of the full vector matrix. The index's
ordered metadata namespace supplies cross-partition identity checks. Duplicate
IDs within a terminal partition are checked before staging its metadata.

The private candidate owns its scratch directory. Cancellation, allocation
failure, malformed scratch input, and duplicate IDs discard the candidate.
Recovery uses private source and completed-clustering receipts as well as
finished index declarations. A source receipt binds the source snapshot,
configuration recipe, dimensions, algorithm, input allowance, RNG state,
immutable exact-vector runs, the small checksummed reservoir, and paged file
identities. The root partition is reconstructed from those authenticated keys and
exact vectors in original producer order after restart; it is not uploaded as a
second corpus copy. A completed-clustering receipt replaces the source-phase work with
the private native projection checkpoint. Recovery opens a fresh overlay and
reuses the verified identity/input census from the same catalog checkpoint.

The fused source handoff runs after identity workers have joined. Its checkpoint
publication must acquire the same bounded I/O capability as ordinary index jobs;
otherwise the 256 KiB immutable source chunks serialize over remote round trips.
It preserves an existing caller capability, joins every upload before returning,
and restores the prior capability on success, cancellation and failure. Disabled
I/O lanes, insufficient CPU-lane capacity, busy admission and unknown allocator headroom retain
the required-inline fallback. This changes scheduling, not the receipt format or
publication fence. A stack-owned publication scope first tries CPU admission
against the scheduler's usable CPU budget, after its I/O reserve. It never waits
for an impossible or nested CPU reservation. Source, completed-tree and final
dense checkpoints share this scope; inherited capabilities remain borrowed.
Every upload wave yields CPU admission, drains all children and restores CPU
admission before returning; scope cleanup releases it on success and failure.
Local-to-GCS qualification and its measurement limits are
recorded separately from the filesystem construction benchmark.

These receipts live in the catalog's separate `construction` field. They never
satisfy a serving declaration, and final publication rejects a nonempty
construction set. Saving uses the active lease and catalog fence. Both ordinary
and resumable GC retain the receipt, its file identities, and its authenticated
file/chunk tree. Protocol 103 carries construction receipts; protocols 97–102 remain
readable for prior serving generations and cannot carry the new work field.
An unfinished attempt carrying an older protocol must renew its publication fence
under protocol 103 before building new artifacts. The worker rejects the old
attempt rather than publishing expanded artifacts with an older reader contract.

A crash within source preparation repeats that source phase; a crash within
clustering reconstructs the root partition from durable exact vectors and repeats
clustering. A crash after the
clustering receipt reuses the completed tree. Final native serialization and
catalog publication retain their existing atomic handoff. Private phase output
has a separate allowance of three times the configured input bytes; final serving
generation output retains the twice-input allowance. Checkpoint uploads reuse
exact-vector objects in final publication.

Both fused and ordinary ingestion use the shared vector producer for missing and
null columns, numeric conversion, dimensions, and finite-coordinate validation.
The tee borrows typed float32 vectors instead of adding a second set of rules.

## Verification and measurement

Native bootstrap publication can be qualified independently from source scanning
and clustering with `zig build bench-hbc-publication -Doptimize=ReleaseFast`.
Run `zig-out/bin/bench_hbc_publication <private-projection-clone> <dims> <count>
ordered` or replace `ordered` with `point`. Each run requires a fresh disposable
clone of the same sealed projection. Both strategies share the native encoders;
only the projection access pattern changes. The tool enforces a 256 MiB allocator
limit with a 16 MiB LSM cache retention target, rejects exact-source reads, reopens native authority,
and checks member count and 100 metadata values. Its timer covers finalization;
opening the private input, metadata sampling and reopened validation are outside
the timer. A timed-out point run is a censored observation, not a completed
baseline or an exact speedup ratio.

On one frozen 10M projection with 582 immutable runs (2.688 GB), two ordered
publication observations took 33.4 and 51.2 seconds, produced identical native
manifest digests, and peaked at 214.1 MB with zero exact-source reads. The
interleaved point-read observation remained incomplete after 120 seconds measured
from the publication-start marker. Relative to the slower ordered observation,
this establishes a conservative 2.34x lower bound for this phase. Concurrent
compiler activity and changing host load make these shared-workstation
observations unsuitable as a dedicated-host throughput guarantee. They justify
the ordered access algorithm, while full import timings and recall are qualified
separately.

Run the focused construction and publication regressions. The high-dimensional
private-projection regression builds 65,536 vectors at 768 dimensions through
more than 100 immutable batch commits under a 16 MiB allocator limit, including
clean backend teardown. It catches retention across commits separately from the
normal 256 MiB runtime admission limit.

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

## Complete Parquet operation qualification

`zig/bench/vectors/prepare_raw_recall.py` computes held-out top-100 squared-L2
truth from original float32 coordinates with float64 accumulation. It never
normalizes the corpus. A training directory can contain the framed ten-file
Cohere fixture produced by `prepare_framed_cohere.py`; physical ordinals follow
lexically sorted object names. Reopened native metadata is resolved through the
authenticated file directory and pinned Parquet footers before comparison.

Build the install-only probe in ReleaseFast, then run the alternating comparison:

```sh
zig build --build-file zig/build.zig lake-execution-runtime-probe-build -Doptimize=ReleaseFast
python3 zig/bench/vectors/bench_bulk_pipeline.py \
  zig/zig-out/bin/lake-execution-runtime-probe train.parquet raw-recall \
  comparison.json --logs comparison-logs --trials 3
```

The same runner accepts a framed multi-object fixture directly. For the ten-file
10M corpus, after preparing its raw-coordinate truth:

```sh
python3 zig/bench/vectors/bench_bulk_pipeline.py \
  zig/zig-out/bin/lake-execution-runtime-probe cohere-10m-input raw-recall-10m-v2 \
  qualification-10m.json --logs qualification-10m --trials 1 \
  --variants prebound_parents --allowance 42949672960 \
  --identity-spill-allowance 8589934592
```

Framed qualification verifies the exact ordered object inventory, each envelope,
body size and body checksum against the ground-truth source list. It hashes the
query/truth payloads as well. After each child exits it revalidates the source and
truth before qualifying the already saved process measurements. These checks
are outside the child's operation and process timers; they warm filesystem data,
so the run does not establish cold-cache throughput.

The Python tools require Python 3.11 or later; truth preparation additionally
requires NumPy and PyArrow. The default three variants share the same verified ReleaseFast caller/provider
executable,
checkpoint policy, dataset and recall sweep. They compare full-vector partitions,
fused identity ingestion, and fused ingestion with preassigned parents. Add
`--variants fused_compact` to qualify the optional compact representation with
preassigned parents. Trial
order reverses between repetitions. Reports retain source/executable checksums,
host load, elapsed and process CPU time, process peak RSS, private projection size,
exact-source reuse, phase attribution, and logical partition read/write bytes,
both private vector-allocator peaks, and all recall observations. Phase timers
are nested attribution counters and must not be summed as disjoint elapsed phases. The recall
sweep always includes 512 through 8,192 probes and widens up to 131,072 if needed
to qualify 95% recall; reports retain that extra search work. Allocator peaks
are distinct from process RSS and do not total every runtime quota.

The operation timer includes identity verification, exact-vector construction,
clustering, durable phase receipts, native serialization, and an artifact
publication candidate. It excludes fixture framing, final metadata CAS, and
reopened recall. File-backed object-store timings do not establish S3 throughput
or HTTP import latency. Run these measurements without overlapping builds.

Local HBC cache limits are enforced by a shared clock implementation for node,
quantized, vector and metadata caches. Reusing a victim slot always records its
replacement key before publishing the entry. Occupancy is explicit, independent
of the full-width key, so ID zero remains cacheable and evictable. This prevents
untracked replacement entries from accumulating beyond the configured capacity.
Node cloning also releases partial ownership on allocation failure. Regressions
exercise repeated eviction in all four cache classes and construct the private
projection while reserving 6.5 MiB for the concurrently resident exact-vector
owner within the same 16 MiB test budget.

Private offline construction bypasses the cross-batch node publication overlay.
That overlay is necessary for live updates, but retaining every packed node body
until the end of an unpublished candidate makes memory grow with corpus size.
Offline node puts and deletes go directly into the rotating immutable batches;
the completed generation remains the only serving publication boundary. The
private projection retains its bounded immutable batches, leaves bulk-window
merging disabled, and disables ordinary close-time L0 pressure compaction. Its
immutable receipt and native emitter are
the next owners, so a corpus-wide rewrite during disposable-candidate teardown
would spend memory and I/O after construction has already finished. Each
completed-child result also retains its scalar covering radius alongside its
centroid. Parent construction composes those bounds directly using the same
metric-specific chord formula as storage-backed recomputation, avoiding child
node reloads solely to recover already-computed bounds. The retained child
centroids also feed the shared quantizer directly when serializing a new parent,
so internal payload construction does not reload those same children again.
Inner product and
invalid child bounds retain the existing unknown-radius semantics.

The initial root writer also collects a bounded external ID census. A 2 MiB
sort buffer and at most 32 merge inputs establish exact global uniqueness before
clustering. CRC-protected spill records, cancellation and owned-file cleanup
apply to every pass. The private source receipt retains the completed census
attestation; old/unverified root partitions receive one sequential verification
pass. Terminal builders no longer probe the growing private projection as an
identity registry. Remaining private LSM reads use a shared cache with a 16 MiB
retention target. Pinned handles can temporarily exceed that target while their
allocations remain charged to the hard 256 MiB builder quota,
and the node-cache allowance includes four-byte coordinates. The 256 MiB runtime
budget is unchanged. See [the source-storage design](bulk-source-storage.md) for
the failure that motivated this change and recovery invariants.

Private source and projection SSTs also cap entries at 65,536, with the existing
byte cap splitting wide rows earlier. Bounded merge fan-in alone does not bound
per-file index size. The entry cap uses the existing SST format and lazy
concatenation, while output reservations respect the same cap. Normal serving
and incremental entry policies remain unchanged.

On the frozen 10M projection, the entry-cap experiment reduced consolidation's
publication-window allocator peak from 208.8 MB to 47.6 MB. Two capped runs took
74.1 and 56.7 seconds versus one uncapped 81.1-second control. These shared-host
observations establish a memory benefit without an observed latency regression;
they do not establish a repeatable throughput gain or lower process RSS. The cap
produced 313 physical output files versus eight, still one concatenated level
stream. Admitted direct publication remained faster at 22.1–29.2 seconds, so
consolidation remains adaptive rather than unconditional. All five final-binary publication runs produced identical full immutable segment
SHA-256 hashes, reopened 10M vectors, matched 100 metadata samples and made zero
exact-vector loads. Whole-import qualification is recorded separately from this
phase experiment.

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
L2. Its initial query checks serving and exact-vector reopening. Set
`ANTFLY_LAKE_BULK_RECALL_DIR` to matching raw-coordinate truth to qualify
held-out recall after reopening, as the paired pipeline harness does.


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


## Final reviewed runtime qualification

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

## Core-builder repetitions before the complete metadata-owner drain

The v67 runtime included the durable metadata handoff and eligible-file pass,
before the complete metadata-owner drain. Three complete 1M imports took 142.116,
140.845 and 161.186 seconds native, with median 142.116 seconds. Whole-probe
times were 193.340, 192.073 and 223.801 seconds. All preserved recall and
scored-vector counts at every width, wrote two construction checkpoints,
reopened successfully and reused all exact-source runs. The dense allocator
peak was about 54.2 MB and the separate source peak was 39.9 MB.

The pre-handoff capped runtime below had a 146.227-second native median. These
sequential three-run groups on a shared workstation do not establish a
repeatable throughput gain from the manifest handoff. Its purpose is releasing
superseded local source generations before clustering; the regression requires
physical SST inventory to match the live sealed inventory after reopening.
The frozen final source, executables, measurements and selected raw records are
in the [qualification report](../../../zig/bench/vectors/cohere_bulk_recovery_qualification.json).

## Entry-cap qualification before the recovery-manifest handoff

The earlier capped runtime (reader protocol 103, runtime ABI 89), before the
recovery-manifest handoff, was qualified separately from the historical runs
below. Three 1M-vector imports took
146.227, 162.481 and 145.500 seconds in the native operation, with median
146.227 seconds. Whole-probe times were 203.667, 233.211 and 207.092 seconds.
All matched the uncapped control's recall and scored-vector counts at every
width, wrote both construction checkpoints, reopened the index, and reused all
exact-source runs without new source bytes. Dense allocator peaks were about
54.2 MB and the separate source peak was 39.9 MB.

The merged-main control without the private SST entry cap took 151.821 seconds
native and 229.323 seconds whole probe. One control followed by three capped
trials on a shared host does not establish a repeatable speedup. It shows no
observed median regression from the cap, whose demonstrated benefit is the
publication allocator reduction above. The earlier parent-rewrite comparison
below measures a different change and source revision.

## 10M complete-operation qualification before the bounded-merge review

The v48 ReleaseFast probe qualifies raw Cohere Large 10M: ten authored Parquet
objects, 768 dimensions, Lloyd clustering, one-bit quantization, fused source
preparation and preassigned parents. The complete native build call takes
1,817.7 seconds; the whole probe, including reopen, recall and teardown, takes
2,091.2 seconds. This is one shared-workstation scale observation, not a paired
10M throughput comparison. It includes one second of read-only profiling during
root routing, with no pauses or overlapping builds or cleanup from this task.

The build verifies the global ID census, publishes two construction receipts,
reopens all ten million members with logical identity evidence, and returns ten
query hits. It reuses all 31,562,812,645 exact-source bytes across 60 immutable
runs, writing zero new exact-source bytes at final publication. Dense allocation
peaks at 252,550,961 bytes and the separate source allocator at 140,686,900 bytes;
both remain below their respective 256 MiB limits. These are allocator counters,
not a claim that whole-process RSS is limited to 256 MiB.

Reopened recall@100 against authored float32 coordinates and float64 squared-L2
ground truth is 61.00%, 72.00%, 82.37%, 89.96%, 94.79% and 96.83% at 512,
1,024, 2,048, 4,096, 8,192 and 16,384 probes. The probe uses 100 queries, epsilon
7 and rerank multiplier 4, increasing search width until it exceeds 95% recall.
Search width is reported explicitly rather than assuming that a 1M setting
provides the same recall at 10M.

Construction emits a 2.688 GB private projection in 582 runs, performs zero
parent updates, and peaks at 93.1 MB before native publication. Clustering takes
540.3 seconds and the native checkpoint takes 46.2 seconds. Those phase timers
are nested within the operation and must not be summed with its total. Source
preparation and durable receipts remain substantial costs; the ordered publisher
does not eliminate them. The recorded qualification retains earlier allocation
failures and the terminated legacy publication observation as unqualified runs.

## Initial bulk-path comparison (before the external ID census)

This initial comparison predates the external ID census. Its later 10M run
exposed retained lookup-index growth and is recorded as unqualified; it does not
establish the final implementation's corpus-scale behavior.

The three-way comparison on raw Cohere 1M Parquet (768 dimensions, one-bit
quantization, Lloyd) uses one ReleaseFast executable, three alternating trials,
and source/completed-clustering checkpoints in every variant. The [recorded
observations](../../../zig/bench/vectors/cohere_bulk_recovery_qualification.json)
pin the executable, source snapshot, toolchain, input and ground-truth hashes,
trial order, phase attribution, allocation peaks, source reuse, and recall sweeps.

| Variant | Median native operation | Median whole-probe CPU | Private LSM projection |
| --- | ---: | ---: | ---: |
| Separate identity/source, late parents | 204.9 s | 233.5 s | 590.3 MB |
| Fused identity/source, late parents | 161.3 s | 196.3 s | 590.3 MB |
| Fused identity/source, preassigned parents | 130.5 s | 176.4 s | 285.4 MB |

The combined change reduces median operation time by 36.3%, whole-probe CPU by
24.4%, and private projection bytes by 51.6%. The final operation observations
are 128.6, 130.5 and 142.5 seconds; controls are 191.6, 205.5 and 204.9 seconds.
These are shared-workstation measurements, with recorded host load, rather than
a dedicated-host throughput guarantee. Whole-probe CPU includes fixture framing,
source opening, reopened recall and teardown; the native operation excludes those
steps and the caller's final metadata CAS.

Every variant reopens an index reporting one million members, returns logical identity,
checks both construction receipts, and reuses all 3,156,307,497 exact-source bytes
at final publication. All runs have identical recall and scored-vector counts at
each width: recall@100 is 85.00%, 93.36%, 97.58%, 98.98% and 99.16% at 512,
1,024, 2,048, 4,096 and 8,192 probes. Ground truth uses authored float32 coordinates
and float64 squared-L2 accumulation, without normalization.

Preassigned construction performs zero parent updates versus 14,248 for each
late-parent control. Its maximum reported dense allocator peak is 189.8 MB,
versus 247.6 MB for controls; the separate fused source allocator peaks at
41.0 MB. Whole-probe RSS ranges substantially and does not establish a consistent
RSS reduction. These allocator figures neither total every runtime quota nor
include all serving-query allocations.

An isolated optional compact observation takes 133.4 seconds with the same
recall, compared with the noncompact median of 130.5 seconds. Logical child
partition writes fall from 13.485 GB to 10.870 GB (19.4%); reads, including the
initial root partition, fall from 16.667 GB to 14.051 GB (15.7%). The dense
allocator peak rises to 200.9 MB. This supports keeping compact regions available
for explicit qualification, but not enabling them by default on elapsed-time
evidence. Reservoir and exact-plane I/O are outside these partition counters.

The internal fused/preassigned policies apply after selecting the offline bulk
path; `lake_bulk_build` remains opt-in. Materialized immutable exact vectors,
shared native serialization, mutation overlays and source-retention rules remain
the serving contract. The remaining private projection commit is only a few
seconds per 1M operation; this comparison does not justify a separate final-format
emitter or an implicit source-backed reranking contract.

## Historical complete-operation observations

On the authored Cohere 1M Parquet corpus (768 dimensions, 3.13 GB source), the
local filesystem operation took 788.9 seconds with standard Zstandard literal
search, then 220.4 seconds with the native prefix decoder and Lloyd, and 218.1
seconds with the prefix decoder and e-means. The optimized runs published
logical identity evidence and reopened all one million members, returning ten
query hits. These are shared-workstation observations with concurrent builds,
not a controlled speedup estimate. The raw corpus was not normalized by the
builder, and this probe checks serving rather than recall.

Five alternating decoder trials each validated 49.15 MB byte-for-byte. Median
native decoding was 0.697 seconds versus 5.446 seconds for standard decoding,
with equal window allocations. The [recorded data](../../../zig/bench/vectors/cohere_bulk_pipeline_1m.json)
includes trial order, raw timings, executable hashes, source identity, and timer
boundaries. Optimizer-only timings in the earlier quality comparison describe a
smaller portion of the import pipeline.


A separate synthetic admission regression builds three million 4d vectors from
a raw Parquet file with a 512 MiB input allowance. Its 288 MB of fixed-width
private keys exceed the former 256 MiB tracker cap. Construction, publication,
logical identity evidence, and reopen passed in 102.3 seconds. This checks scale
and configured admission; it is not a high-dimensional recall comparison.
Generate the input with `zig/bench/vectors/prepare_identity_stress.py` (requires
PyArrow), then run the operation probe with `ANTFLY_LAKE_BULK_DIMS=4` and
`ANTFLY_LAKE_BULK_INPUT_BYTES=536870912`.


## Object-store source handoff qualification

A local macOS probe against GCS exposed serial uploads at the fused source
handoff after identity workers had joined. Borrowing the existing bounded I/O
scheduler for that quiescent checkpoint reduced a 100K source-receipt phase
from 359.5 to 127.7 seconds and native construction from 599.6 to 374.7 seconds.
Both builds reused all 315.5 MB of sealed exact-vector source runs. Dense and
source allocator peaks were unchanged, and separately reopened publications
matched the fixed ten-query recall oracle exactly. These are single-pair WAN
observations on a shared workstation. The original attempt's recall was stopped;
a separate reopen qualified its retained publication, so there is no paired
complete child-wall comparison. The [GCS qualification report](../../../zig/bench/vectors/GCS_BULK_QUALIFICATION.md)
records timer boundaries, hashes, diagnostic overlap and incomplete attempts.

The same diagnostic found that established fresh-index fragment preparation can
spend substantial time on remote identity-tree lookups before vector insertion.
A 100K comparison was censored after 20 minutes in that phase. It does not supply
a completed insertion latency or justify a numerical speedup claim.

Broad uncaptured fragment preparation now merge-joins current and fragment
identity maps in verified ID order. It checks equal IDs, value evidence and row
counts, externally sorts 16-byte physical coordinates into a private translation
tree, and rejects duplicate physical coordinates by checking the reduced count.
Physical files resolve once through the authenticated dictionary. Payload tiles
then batch-seek only the local tree, rather than seeking both remote identity
maps for every tile. The existing captured-payload adoption path keeps its
verified-ID join and does not pay for this extra map.

The translation map shares the spool's 64 MiB workspace and configured spill
quota. All temporary pages remain private and are deleted when capture finishes;
they never change a publication format or enter its GC closure. Quota pressure
can still decline the optional spool and use the existing required gather path.
This converts repeated random cloud navigation into sequential remote scans plus
bounded local sorting and seeks. Read-count comparisons and remote timing must
be reported separately; neither establishes a full managed-table import rate.


The final publication scope also covers completed-tree and serving-generation
uploads. A subsequent 100K local-to-GCS operation completed and qualified in
299.9 seconds, compared with 374.7 seconds for source-checkpoint scheduling alone;
final dense upload decreased from 44.1 to 13.2 seconds. Both had the same private
allocator peaks, source reuse and ten-query recall/scored counts. These remain
single-pair WAN observations, not same-region throughput estimates.

Qualification inputs form an owned execution snapshot: executable, Python
controller/worker/validator, oracle inputs, case descriptor and any retained
reopen catalog. Probe settings are explicit rather than inherited. The probe
attests the entire source inventory and checks its admitted immutable revisions
before construction; vectorless files cannot silently alter identity work while
preserving embedding count and recall. Receipt checks bind the exact interrupted
manifest and require skipped input replay/clustering after restart. These testing
contracts are separate from production catalog CAS, Raft and power-loss recovery.

### Conditional upload recovery and request memory

Identity ingestion can publish row-key chunks before it saves a source receipt.
Those writes need the same bounded recovery behavior as final artifact uploads.
GCS implements create-only media uploads with POST and `ifGenerationMatch=0`.
The object-store caller explicitly admits replay only for that conditional media
request. HTTPx retains its ordinary method safety defaults, retry ceiling,
backoff, cancellation and request-wide deadline; it does not propagate this
admission through a redirect. Authentication and transport consume the captured
request deadline. Unconditional POST and resumable-session initiation do not
inherit the new replay admission.

A lost successful response may turn the retry into a precondition conflict.
The GCS client returns that conflict. The artifact layer then reads the existing
object under its immutable provider revision and verifies its length and digest
before accepting it. A different object at the expected content address fails
closed. Retrying a request never grants publication authority to private work.

GCS transport borrows the immutable request body through synchronous completion
of every attempt, removing a second payload-sized allocation and copy. The
owner must keep the bytes alive and immutable until the call returns. A focused
8 MiB request-setup comparison measured 8,388,855 allocated bytes with an owned
body versus 247 with the borrowed body; this excludes network execution and is
an allocation result, not an ingest throughput or RSS claim. Clustering, native
SST formats and normal database insertion are unaffected by this change.

Qualification emits fixed operation/error tags at source-read, identity spill
and chunk publication, exact-vector flush, and spool append/finish failures.
Spool failures retain the file writer's underlying error when available. The
diagnostic does not log object URLs, credentials, source values or scratch paths,
and compiles away in production. The controller treats SIGHUP and SIGTERM as
cancellation, drains its owned worker group and persists a terminal status.
SIGKILL cannot run cleanup; long suites still require a durable supervisor and
retained exit records.

The final 1M WAN attempt failed during identity ingestion and remains
unqualified. Its generic `WriteFailed` log cannot establish which operation
failed. The [failure investigation](../../../zig/bench/vectors/GCS_BULK_FAILURE_INVESTIGATION.md)
separates that historical uncertainty from reproduced recovery and lifecycle
defects. Planned range reads already skip metadata probes, so removing an
assumed extra HEAD is not a performance opportunity on that path. Further
upload batching, range coalescing or concurrency changes need attributed phase
and request measurements plus a successful publication/reopen/recall check
before another paired performance comparison.

### Qualification process ownership

The controller and worker install one cancellation scope before starting work.
SIGTERM, SIGHUP and SIGINT record the first stop request; handlers do not raise
inside process creation or teardown. Every helper command, upload and native
child acquires an owner before `Popen`. Cancellable waits poll that scope, while
teardown drains and reaps the owned process without a second signal interrupting
cleanup. Frozen-controller re-exec blocks these signals across exec and installs
handlers before unblocking them in the new image, preserving pending requests.

The controller owns the worker's process group. Native children remain in that
group, so controller escalation still reaches them if worker cleanup stalls.
The worker additionally owns and reaps its native child with `wait4`, preserving
CPU/RSS observations when available. Failed process creation is explicitly
distinguished from a started child; missing resource usage is unavailable rather
than zero. Source hashing checks cancellation between reads of a reused 1 MiB
buffer. Upload writes and captured helper output also have cancellable waits.

One reporting boundary covers admission, worker execution and post-build source
and input verification. Any exception leaves a terminal failed/cancelled report,
including post-build verification after worker exit zero. Disk sampling writes
an independent checkpoint every two seconds and finalizes on success, failure or
catchable cancellation. Reports retain minimum host free space, case-directory
nominal lengths, sampling errors and whether its thread stopped. Native `/tmp`
scratch remains outside that directory total, and host free space includes
unrelated workloads. SIGKILL cannot run finalization; the periodic checkpoint and
external supervisor's exit record remain the available evidence.

Shutdown persists the last complete observations before waiting for the sampler.
Cancelled and failed attempts skip final collection. Successful attempts request
one final scan in the sampling daemon, with a two-second cooperative deadline
and a three-second join ceiling. Filesystem calls can block beyond that deadline;
the reporting thread still persists terminal failure with
`final_sample_complete=false` and `sampler_stopped=false`. A successful case
requires completed final collection. Scan loops check cancellation and deadlines
between filesystem operations. No final scan runs in the reporting thread.

Real-GCS qualification rejects inherited storage endpoint overrides. ADC mode
also rejects `GCS_BEARER_TOKEN` and `GOOGLE_OAUTH_ACCESS_TOKEN`, which otherwise
take precedence over refreshable ADC in the native default credential resolver.
Rejections list names only. Explicit ADC-file selection and ordinary proxy
settings remain supported. The worker records its admitted credential mode and
canonical storage/upload endpoints without copying credentials into evidence.

### Bounded identity publication chunks

Fresh fused bulk construction uses a 262,037-byte identity chunk target. The
last append can add a 108-byte key, keeping every payload within the existing
256 KiB reader limit. Both 96-byte parent keys and 108-byte enrichment keys retain
their existing paged-reference representation. Oversized targets are rejected
before appending. The tracker's pending buffer and publication scratch are
charged to the native bulk owner's 256 MiB private allocator. Other tracker
callers retain their 192 KiB target.

Paged-reference publication also borrows an operation-local copy of the artifact
store with the tracker allocator. This includes page packing and backend upload
scratch without changing the shared capability's allocator. The publication
fixture checks allocator identity for every identity and reference-page upload.

The `lake-identity-publication-test` fixture publishes one million real keys for
each target/key-width pair through `FsStore`, then reads every persisted chunk
through the established paged reader and requires equal corpus count, order and
BLAKE3 digest. For 96-byte keys, chunk puts decreased from 489 to 367 and all
artifact puts from 524 to 395, with the same 519,243-byte tracker allocation peak.
For 108-byte keys, chunk puts decreased from 550 to 413 and all puts from 590 to
441; the measured peak increased from 454,327 to 552,489 bytes. Every fixture is
admitted under an 8 MiB tracker limit. These are measured artifact API counts and
filesystem allocation observations, not measured GCS HTTP counts or a WAN/full
build speedup. Raw observations are in
[`bulk_identity_publication_results.json`](../../../zig/bench/vectors/bulk_identity_publication_results.json).

A proposed 1 MiB identity chunk failed existing reader validation and was not
enabled. Raising the shared SST chunk size would change persisted-file offset
interpretation and is not part of this optimization. Additional asynchronous
publication queues need separate latency, ordering, cancellation and scheduler
admission evidence before adoption.
